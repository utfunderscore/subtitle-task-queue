use crate::multipart_upload::upload;
use amqprs::channel::{BasicPublishArguments, QueueDeclareArguments};
use amqprs::connection::{Connection, OpenConnectionArguments};
use amqprs::{BasicProperties, DELIVERY_MODE_PERSISTENT};
use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config};
use axum::extract::{DefaultBodyLimit, Multipart};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Json, Router, debug_handler};
use serde::Serialize;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

pub mod multipart_upload;
pub mod task_producer;

const QUEUE_NAME: &str = "subtitles-work-queue";

#[derive(Serialize)]
struct SuccessResponse {
    task_id: String,
    message: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    details: Option<String>,
}

/// Custom error type that can be converted into an Axum response
struct AppError {
    status: StatusCode,
    message: String,
    details: Option<String>,
}

impl AppError {
    fn with_details(
        status: StatusCode,
        message: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            details: Some(details.into()),
        }
    }

    fn internal_error(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Internal server error".to_string(),
            details: Some(format!("{:#}", err)),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = Json(ErrorResponse {
            error: self.message,
            details: self.details,
        });
        (self.status, body).into_response()
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let s3_client: Client = create_s3_client();
    let connection = create_rabbitmq_connection().await?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = Router::new()
        .route("/", post(accept_form))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(
            2500 * 1024 * 1024, /* 250mb */
        ))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
        )
        .layer(Extension(s3_client))
        .layer(Extension(connection))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn create_rabbitmq_connection() -> Result<Connection> {
    Connection::open(&OpenConnectionArguments::new(
        "localhost",
        5672,
        "admin",
        "admin",
    ))
    .await
    .context("Failed to connect to RabbitMQ")
}

#[debug_handler]
async fn accept_form(
    client: Extension<Client>,
    connection: Extension<Connection>,
    mut multipart: Multipart,
) -> Result<Json<SuccessResponse>, AppError> {
    let task_id = Uuid::new_v4();

    let path = format!("temp/{}", task_id.to_string());
    let mut writer = BufWriter::new(File::create(path.clone()).await.map_err(|e| {
        AppError::with_details(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create temporary file",
            e.to_string(),
        )
    })?);

    while let Some(mut field) = multipart.next_field().await.map_err(|e| {
        AppError::with_details(
            StatusCode::BAD_REQUEST,
            "Failed to read multipart field",
            e.to_string(),
        )
    })? {
        loop {
            match field.chunk().await {
                Ok(Some(chunk)) => {
                    writer.write(&chunk.to_vec()).await.map_err(|e| {
                        AppError::with_details(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to write file chunk",
                            e.to_string(),
                        )
                    })?;
                }
                Ok(None) => {
                    writer.flush().await.map_err(|e| {
                        AppError::with_details(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to flush file buffer",
                            e.to_string(),
                        )
                    })?;
                    break;
                }
                Err(e) => {
                    writer.flush().await.ok();
                    fs::remove_file(&path).await.ok();

                    return Err(AppError::with_details(
                        StatusCode::BAD_REQUEST,
                        "Failed to read file chunk",
                        e.to_string(),
                    ));
                }
            }
        }
    }
    drop(writer);

    info!("File upload complete, transferring to s3");
    if let Err(err) = upload(client.0, &task_id, &path).await {
        fs::remove_file(&path).await.ok();
        return Err(AppError::internal_error(err));
    }

    info!("S3 Transfer complete for {}", task_id);
    fs::remove_file(&path).await.map_err(|e| {
        AppError::with_details(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to remove temporary file",
            e.to_string(),
        )
    })?;

    info!("Removed local file... adding to work queue");
    publish_task(connection.0, task_id)
        .await
        .map_err(AppError::internal_error)?;

    info!("Task published successfully");

    Ok(Json(SuccessResponse {
        task_id: task_id.to_string(),
        message: "Task submitted successfully".to_string(),
    }))
}

async fn publish_task(connection: Connection, item_id: Uuid) -> Result<()> {
    let args = BasicPublishArguments::new("", QUEUE_NAME);

    // Open a channel
    let channel = connection
        .open_channel(None)
        .await
        .context("Failed to open RabbitMQ channel")?;

    channel
        .queue_declare(
            QueueDeclareArguments::new(QUEUE_NAME)
                .durable(true) // Make queue persistent
                .finish(),
        )
        .await
        .context("Failed to declare RabbitMQ queue")?;

    channel
        .basic_publish(
            BasicProperties::default()
                .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
                .finish(), // Persistent message
            item_id.as_bytes().to_vec(),
            args,
        )
        .await
        .context("Failed to publish message to RabbitMQ")?;

    Ok(())
}

fn create_s3_client() -> Client {
    Client::from_conf(
        Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                "rustfsadmin",
                "rustfsadmin",
                None,
                None,
                "my-provider",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url("http://localhost:9000")
            .force_path_style(true)
            .build(),
    )
}
