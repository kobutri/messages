# Messages

A high-performance, async Rust server for streaming and storing chat messages, with a benchmarking client.

---

## Project Purpose
This project demonstrates how to distribute documents, messages, or any data that is produced incrementally to consumers in a low-latency manner, while avoiding excessive buffering and allowing new clients to catch up seamlessly.

- **Low-latency streaming:** As messages are produced, they are distributed to all connected clients in real time.
- **Catch-up support:** New clients receive the full message history (persisted + in-memory buffer) before subscribing to live updates, ensuring they never miss content.
- **No over-buffering:** The system avoids unnecessary buffering, delivering data as soon as it is available.
- **Efficient locking:** When a new client connects, it temporarily locks the broadcast channel for its document, fetches persisted and buffered content, then subscribes to updates and releases the lock. This prevents missing new messages during the catch-up phase.
- **Persistence:** Messages are buffered in memory and periodically persisted to the database for durability.
- **Simple client interface:** Clients connect via a streamed HTTP response, always receiving the entire message stream from the beginning.

---

## Features
- **Async HTTP API** using [Axum](https://github.com/tokio-rs/axum)
- **PostgreSQL** storage via [SQLx](https://github.com/launchbadge/sqlx)
- **Streaming endpoints** for real-time message delivery
- **In-memory channel management** with [DashMap](https://github.com/xacrimon/dashmap)
- **Benchmark client** for load testing
- **Docker Compose** for easy database setup

## Project Structure
- `messages/` - Main server implementation
- `messages_bench/` - Benchmarking client

## Getting Started

### Prerequisites
- [Rust (nightly)](https://rustup.rs/)
- [Docker](https://www.docker.com/)

### Setup Database
```sh
docker compose -f messages/docker-compose.yml up -d
```

### Environment
Copy `.env` if needed and set `DATABASE_URL` (see sample in repo).

### Run Server
```sh
cd messages
cargo run
```

Server will listen on `0.0.0.0:3000`.

### Run Benchmarks
```sh
cd messages_bench
cargo run
```

## API Overview
- `POST /` - Create a new chat stream (with query params `freq`, `max`)
- `GET /?id=...` - Stream chat content for a given channel

## Database Migrations
Migrations are in `messages/migrations/` and managed by SQLx. They run automatically on server start.

## Benchmarking Client
The `messages_bench` benchmarking client simulates thousands of channels and clients to stress-test the server. It:
- Creates many channels, each with several simulated clients.
- Starts streams on the server and connects clients with random delays.
- Verifies that all clients receive the same message content as the original stream.
- Reports any inconsistencies, helping to validate the server's real-time streaming and catch-up guarantees.

## Dependencies
- axum
- sqlx
- tokio
- dashmap
- reqwest
- anyhow