use crate::audio_checker::is_ffmpeg_compatible;
use crate::convert::convert_to_wav;
use crate::inference::SubtitleInference;
use amqprs::channel::{BasicAckArguments, BasicNackArguments, Channel};
use amqprs::consumer::AsyncConsumer;
use amqprs::{BasicProperties, Deliver};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use std::fs;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub struct AudioConsumer {
    client: Client,
    inference: Arc<Box<dyn SubtitleInference + Send + Sync>>,
    bucket: String,
}

#[async_trait]
impl AsyncConsumer for AudioConsumer {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let delivery_tag = deliver.delivery_tag();

        println!("Processing task {}", delivery_tag);

        match self.process_task(content).await {
            Ok(_) => {
                if let Err(e) = channel
                    .basic_ack(BasicAckArguments::new(delivery_tag, false))
                    .await
                {
                    eprintln!("Failed to ACK message: {:#}", e);
                } else {
                    println!("Acknowledged task {delivery_tag} as complete");
                }
            }
            Err(e) => {
                eprintln!("Error processing task: {:#}", e);
                eprintln!("Backtrace:\n{}", e.backtrace());

                if let Err(nack_err) = channel
                    .basic_nack(BasicNackArguments::new(delivery_tag, false, true))
                    .await
                {
                    eprintln!("Failed to NACK message: {:#}", nack_err);
                }
            }
        };
    }
}

impl AudioConsumer {
    pub fn new(
        client: Client,
        bucket: String,
        inference: Box<dyn SubtitleInference + Send + Sync>,
    ) -> Result<Self> {
        fs::create_dir_all("temp").context("Failed to create temp directory")?;
        Ok(Self {
            client,
            inference: Arc::new(inference),
            bucket,
        })
    }

    pub async fn process_task(&self, content: Vec<u8>) -> Result<()> {
        let task_id =
            Uuid::from_slice(&content).context("Failed to parse task ID from message content")?;

        println!("Processing task: {}", task_id);

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(task_id.to_string())
            .send()
            .await
            .context(format!(
                "Failed to download object {} from S3 bucket '{}'",
                task_id, self.bucket
            ))?;

        let file_path = format!("temp/{}", task_id);
        let mut file = tokio::fs::File::create(&file_path).await.context(format!(
            "Failed to create temporary file at '{}'",
            file_path
        ))?;

        let mut body = resp.body;

        // Stream the body directly to the file in chunks
        while let Some(bytes) = body
            .try_next()
            .await
            .context("Failed to read chunk from S3 response body")?
        {
            file.write_all(&bytes).await.context(format!(
                "Failed to write data to temporary file '{}'",
                file_path
            ))?;
        }

        file.flush()
            .await
            .context(format!("Failed to flush temporary file '{}'", file_path))?;

        // Process the task and ensure cleanup happens regardless of success or failure
        let result = self.process_downloaded_file(&file_path, task_id).await;

        // // Clean up the temporary file after processing (success or failure)
        // if let Err(e) = tokio::fs::remove_file(&file_path).await {
        //     eprintln!("Warning: Failed to clean up temporary file '{}': {}", file_path, e);
        // }

        result
    }

    async fn process_downloaded_file(&self, file_path: &str, task_id: Uuid) -> Result<()> {
        let is_compatible = is_ffmpeg_compatible(file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to check if file is ffmpeg compatible: {}", e))?;

        if !is_compatible {
            println!("File is not ffmpeg compatible");
            return Ok(());
        }

        let output = convert_to_wav(file_path)?;
        let inference = self.inference.clone();

        let segments = tokio::task::spawn_blocking(move || {
            let task = &output;
            inference.execute(task)
        })
        .await
        .context("Failed to spawn inference task")?
        .context("Failed to execute inference")?;

        println!(
            "Successfully processed task {} with {} segments",
            task_id,
            segments.len()
        );

        Ok(())
    }
}
