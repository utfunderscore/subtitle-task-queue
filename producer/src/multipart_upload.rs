use aws_sdk_s3::Client;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use anyhow::{Context, Result, anyhow};
use std::path::Path;
use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use uuid::Uuid;

const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
const MAX_CHUNKS: u64 = 10000;
const BUCKET: &str = "test";

pub async fn upload(client: Client, path: &str) -> Result<()> {
    let key = Uuid::new_v4().to_string();

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .context("Failed to create multipart upload")?;

    let upload_id = multipart_upload_res
        .upload_id()
        .ok_or_else(|| anyhow!("Missing upload id from S3 response"))?;

    let path = Path::new(path);
    let file_size = tokio::fs::metadata(path)
        .await
        .context("Failed to read file metadata")?
        .len();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -= 1;
    }

    if file_size == 0 {
        return Err(anyhow!("File size is zero - cannot upload empty file"));
    }
    if chunk_count > MAX_CHUNKS {
        return Err(anyhow!(
            "File requires {} chunks, exceeding maximum of {} chunks. Try increasing chunk size.",
            chunk_count,
            MAX_CHUNKS
        ));
    }

    let mut upload_parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();

    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };
        let stream = ByteStream::read_from()
            .path(path)
            .offset(chunk_index * CHUNK_SIZE)
            .length(Length::Exact(this_chunk))
            .build()
            .await
            .context(format!("Failed to read chunk {} from file", chunk_index))?;

        // Chunk index needs to start at 0, but part numbers start at 1.
        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(&key)
            .bucket(BUCKET)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await
            .context(format!("Failed to upload part {} to S3", part_number))?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .context("Failed to complete multipart upload")?;

    Ok(())
}
