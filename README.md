# Distributed Subtitle Generation System

A high-performance subtitle generation system built on a distributed architecture. This project processes video and audio files through machine learning inference to produce accurate transcriptions, handling the entire pipeline from upload to final output.

## What It Does

Upload a video file through an HTTP endpoint, and the system coordinates multiple services to generate subtitles. The producer accepts your file, stores it in object storage, and queues the work. Worker nodes pull tasks from the queue, convert media to the correct format, run Whisper AI inference, and persist timestamped text segments to a database. Poll the status endpoint to track progress and retrieve your completed subtitles.

## Architecture

The system splits responsibilities across specialized services that communicate through message queues and shared storage.

**Producer Service**  
Runs an HTTP server that accepts multipart file uploads. Writes incoming data to temporary local storage, then streams it to S3-compatible object storage using multipart upload. Once the file lands in object storage, the producer publishes a task ID to RabbitMQ and updates the task status in PostgreSQL. This service never blocks on inference work.

**Consumer Service**  
Pulls task IDs from the RabbitMQ queue. Downloads the corresponding file from object storage, validates media compatibility with ffmpeg, converts to 16kHz mono WAV, and runs Whisper inference. Results get written directly to PostgreSQL as timestamped segments. Each consumer processes one task at a time with manual acknowledgment to RabbitMQ.

**Database Layer**  
Diesel ORM manages PostgreSQL schema with two tables. The `task` table tracks upload status through stages like "UploadingFile," "Processing," and "Success." The `segment` table stores inference output with foreign key relationships to tasks. Connection pooling through r2d2 handles concurrent access from multiple services.

**Storage Layer**  
RustFS provides S3-compatible object storage with a local filesystem backend. Producer uploads use the multipart API for large files. Consumers download entire objects for processing. This separation means inference failures never require re-uploading from clients.

**Message Queue**  
RabbitMQ coordinates work distribution with durable queues and persistent messages. QoS settings limit each consumer to one in-flight task. Manual acknowledgment ensures failed tasks return to the queue automatically.

## Data Flow

1. Client POSTs multipart form data to `/` endpoint
2. Producer streams upload to local temp file
3. Producer transfers file to RustFS using multipart upload
4. Producer writes task record to PostgreSQL
5. Producer publishes task ID to RabbitMQ queue
6. Consumer reads task ID from queue
7. Consumer downloads file from RustFS
8. Consumer validates media with ffmpeg probe
9. Consumer converts to WAV format
10. Consumer runs Whisper inference on converted audio
11. Consumer writes segments to PostgreSQL
12. Consumer updates task status to "Success"
13. Consumer acknowledges message to RabbitMQ
14. Client GETs `/task/{id}` to retrieve segments

## Technologies

**Core Language**  
Rust 2024 edition across all services for memory safety and concurrency

**Web Framework**  
Axum for HTTP routing with tower middleware for body limits and request tracing

**Async Runtime**  
Tokio powers all async operations including file I/O, network requests, and task spawning

**Database**  
PostgreSQL 15 with Diesel ORM for type-safe queries and migrations. R2d2 handles connection pooling.

**Message Queue**  
RabbitMQ 4.2 with amqprs client library for async AMQP operations

**Object Storage**  
RustFS for S3-compatible storage. AWS SDK for Rust handles all S3 protocol interactions.

**Machine Learning**  
whisper-rs bindings to OpenAI's Whisper C++ implementation. Runs on CUDA for GPU acceleration.

**Media Processing**  
FFmpeg through system calls for media validation and WAV conversion. Hound library reads WAV samples into i16 buffers.

**Serialization**  
Serde for JSON API responses and message queue payloads

**Error Handling**  
Anyhow provides context-aware error propagation across async boundaries

## Running Locally

Start infrastructure services:

```bash
docker compose up -d
```

This launches PostgreSQL, RabbitMQ, and RustFS. Wait for health checks to pass.

Run database migrations:

```bash
cd db
diesel migration run
```

Download the Whisper model (consumer needs this file in its directory):

```bash
cd consumer
wget https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-large-v3-turbo.bin
```

Start the producer in one terminal:

```bash
cd producer
cargo run --release
```

Start one or more consumers in separate terminals:

```bash
cd consumer
cargo run --release
```

Upload a video file:

```bash
curl -X POST http://localhost:3000/ \
  -F "file=@your-video.mp4"
```

Check status (replace `{id}` with task_id from upload response):

```bash
curl http://localhost:3000/task/{id}
```

## Project Structure

```
distributed-subtitles/
├── producer/          # HTTP API for uploads
├── consumer/          # Inference worker
├── db/                # Database schema and queries
├── common/            # Shared types
└── docker-compose.yaml
```

## Why This Approach

Separating upload handling from compute-intensive inference lets the system scale horizontally. Add more consumers without touching the producer. Queue-based coordination means the producer never waits on inference completion. Persistent messages survive consumer crashes. Storing files in object storage before processing means clients disconnect after upload rather than holding connections during inference.

The entire system runs on localhost for development but can deploy to distributed infrastructure by changing connection strings and endpoints.

## Performance Characteristics

Producer handles concurrent uploads through Axum's task spawning and buffered file writes. Multipart uploads to RustFS stream data without loading entire files into memory. Consumers run inference on GPU through CUDA bindings, completing a typical 5-minute audio file in under 30 seconds. Database connection pooling prevents query queueing under load.

Task acknowledgment happens only after segment persistence, guaranteeing exactly-once processing semantics for successful tasks.
