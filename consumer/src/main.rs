mod convert;
mod inference;
mod task_consumer;

use crate::inference::WhisperModel;
use crate::task_consumer::AudioConsumer;
use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::{BasicConsumeArguments, BasicQosArguments, QueueDeclareArguments};
use amqprs::connection::{Connection, OpenConnectionArguments};
use std::error::Error;
use whisper_rs::{WhisperContext, WhisperContextParameters};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ctx = WhisperContext::new_with_params(
        "ggml-large-v3-turbo-q5_0.bin",
        WhisperContextParameters::default(),
    )
    .expect("failed to load model");

    let connection = Connection::open(&OpenConnectionArguments::new(
        "localhost",
        5672,
        "admin",
        "admin",
    ))
    .await?;

    let connection = register_task_consumer(connection, "subtitles-task-queue", ctx).await?;

    tokio::signal::ctrl_c().await?;
    connection.close().await?;
    Ok(())
}

async fn register_task_consumer(
    connection: Connection,
    queue_name: &str,
    whisper_context: WhisperContext,
) -> Result<Connection, Box<dyn Error>> {
    connection
        .register_callback(DefaultConnectionCallback)
        .await?;

    let channel = connection.open_channel(None).await?;
    channel.register_callback(DefaultChannelCallback).await?;

    // Declare the queue (idempotent)
    let args = QueueDeclareArguments::new(queue_name)
        .durable(true)
        .finish();

    channel.queue_declare(args).await?;
    channel
        .basic_qos(BasicQosArguments::new(0, 1, false))
        .await?;

    let args = BasicConsumeArguments::new(queue_name, "")
        .manual_ack(true)
        .finish();

    channel
        .basic_consume(
            AudioConsumer::new(Box::new(WhisperModel::new(whisper_context))),
            args,
        )
        .await?;
    Ok(connection)
}
