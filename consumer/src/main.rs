mod convert;
mod inference;
mod task_consumer;
mod audio_checker;

use crate::inference::WhisperModel;
use crate::task_consumer::AudioConsumer;
use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::{BasicConsumeArguments, BasicQosArguments, QueueDeclareArguments};
use amqprs::connection::{Connection, OpenConnectionArguments};
use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config};
use whisper_rs::{WhisperContext, WhisperContextParameters};

#[tokio::main]
async fn main() -> Result<()> {
    // Enable backtrace capture

    let ctx = WhisperContext::new_with_params(
        "ggml-large-v3-turbo.bin",
        WhisperContextParameters::default(),
    )
    .context("Failed to initialize Whisper context with model file 'ggml-large-v3-turbo-q5_0.bin'")?;

    let client = create_s3_client();

    let connection = Connection::open(&OpenConnectionArguments::new(
        "localhost",
        5672,
        "admin",
        "admin",
    ))
    .await
    .context("Failed to connect to RabbitMQ at localhost:5672")?;

    let _channel = register_task_consumer(
        &connection,
        client,
        String::from("test"),
        "subtitles-work-queue",
        ctx,
    )
    .await
    .context("Failed to register task consumer")?;

    println!("Consumer started successfully. Press Ctrl+C to stop.");

    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C signal")?;

    println!("Shutting down...");
    
    connection
        .close()
        .await
        .context("Failed to close RabbitMQ connection")?;

    println!("Consumer shut down successfully.");
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

async fn register_task_consumer(
    connection: &Connection,
    client: Client,
    bucket: String,
    queue_name: &str,
    whisper_context: WhisperContext,
) -> Result<amqprs::channel::Channel> {
    connection
        .register_callback(DefaultConnectionCallback)
        .await
        .context("Failed to register connection callback")?;

    let channel = connection
        .open_channel(None)
        .await
        .context("Failed to open AMQP channel")?;
    
    channel
        .register_callback(DefaultChannelCallback)
        .await
        .context("Failed to register channel callback")?;

    // Declare the queue (idempotent)
    let args = QueueDeclareArguments::new(queue_name)
        .durable(true)
        .finish();

    channel
        .queue_declare(args)
        .await
        .context(format!("Failed to declare queue '{}'", queue_name))?;
    
    channel
        .basic_qos(BasicQosArguments::new(0, 1, false))
        .await
        .context("Failed to set QoS settings on channel")?;

    let args = BasicConsumeArguments::new(queue_name, "")
        .manual_ack(true)
        .finish();

    channel
        .basic_consume(
            AudioConsumer::new(client, bucket, Box::new(WhisperModel::new(whisper_context)))
                .context("Failed to create AudioConsumer")?,
            args,
        )
        .await
        .context(format!("Failed to start consuming from queue '{}'", queue_name))?;
    
    Ok(channel)
}
