use anyhow::{Context, anyhow};
use anyhow_http::http::StatusCode;
use anyhow_http::response::HttpJsonResult;
use anyhow_http::{OptionExt, http_error, http_error_bail};
use axum::Json;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use dotenvy_macro::dotenv;
use futures::{StreamExt, stream};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::signal;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, broadcast};
use tokio_stream::wrappers::BroadcastStream;

struct AppState {
    db: Database,
    streams: DashMap<String, Arc<Mutex<Sender<String>>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AppInspect {
    streams_total: usize,
    chats_total: usize,
    chats: HashMap<String, usize>,
    streams: HashSet<String>,
}

impl AppInspect {
    fn from_state(state: &AppState) -> Self {
        let streams_total: usize = state.streams.len();
        let mut chats_total: usize = 0;
        let read = state.db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();

        let chats = table
            .iter()
            .unwrap()
            .map(|r| {
                let r = r.unwrap();
                chats_total += r.1.value().len();
                (r.0.value().to_string(), r.1.value().len())
            })
            .collect();
        let streams = state.streams.iter().map(|r| r.key().clone()).collect();
        Self {
            streams_total,
            chats_total,
            chats,
            streams,
        }
    }
}

const TABLE: TableDefinition<String, String> = TableDefinition::new("chats");
const DATABASE: &str = dotenv!("DATABASE");

#[tokio::main]
async fn main() {
    let db = Database::create(DATABASE).expect("Failed to create database");

    let state = Arc::new(AppState {
        db,
        streams: DashMap::new(),
    });

    let app = Router::new()
        .route("/", get(connect))
        .route("/", post(create_new))
        .route("/inspect", get(inspect))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn connect(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> HttpJsonResult<impl IntoResponse> {
    let id = params.get("id").ok_or_status(StatusCode::BAD_REQUEST)?;
    let old_content = async || -> HttpJsonResult<String> {
        let read = state
            .db
            .begin_read()
            .map_err(|e| http_error!(INTERNAL_SERVER_ERROR, "Database read error: {}", e))?;
        let table = read
            .open_table(TABLE)
            .map_err(|e| http_error!(INTERNAL_SERVER_ERROR, "Failed to open table: {}", e))?;
        match table.get(id) {
            Ok(Some(content)) => Ok(content.value()),
            Ok(None) => http_error_bail!(NOT_FOUND, "Chat not found"),
            Err(e) => http_error_bail!(INTERNAL_SERVER_ERROR, "Database error: {}", e),
        }
    };
    if let Some(sender) = state.streams.get(id) {
        let sender = sender.lock().await;
        let old_content = old_content().await?;
        let stream = futures::StreamExt::chain(
            stream::once(async move { Ok(old_content) }),
            BroadcastStream::new(sender.subscribe()),
        );
        Ok(Body::from_stream(stream).into_response())
    } else {
        Ok(old_content().await?.into_response())
    }
}

async fn create_new(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    body: String,
) -> HttpJsonResult<impl IntoResponse> {
    let freq: f32 = params
        .get("freq")
        .and_then(|f| f.parse().ok())
        .unwrap_or(20.0);
    let max: usize = params.get("max").and_then(|m| m.parse().ok()).unwrap_or(50);
    let id = body;
    let stream = tokio_stream::StreamExt::throttle(
        stream::iter((1..=max).map({
            let id = id.clone();
            move |i| {
                let content = format!("{}: {}\n", i, &id);
                content
            }
        })),
        Duration::from_secs_f32(1.0 / freq),
    );

    {
        let Ok(mut write) = state.db.begin_write() else {
            http_error_bail!(INTERNAL_SERVER_ERROR, "Database write error");
        };
        write.set_durability(redb::Durability::Eventual);
        {
            let Ok(mut table) = write.open_table(TABLE) else {
                http_error_bail!(INTERNAL_SERVER_ERROR, "Failed to open table");
            };
            let Ok(_) = table.insert(id.clone(), "".to_string()) else {
                http_error_bail!(INTERNAL_SERVER_ERROR, "Failed to insert chat");
            };
        }
        if let Err(e) = write.commit() {
            http_error_bail!(INTERNAL_SERVER_ERROR, "Failed to commit transaction: {}", e);
        }
    }
    let (sender, receiver) = broadcast::channel(100);
    let sender = Arc::new(Mutex::new(sender));
    state.streams.insert(id.clone(), sender.clone());
    let return_stream = BroadcastStream::new(receiver);
    tokio::task::spawn({
        let tx = sender.clone();
        let state = state.clone();
        let id = id.clone();
        async move {
            stream
                .for_each(async |s| {
                    let tx = tx.lock().await;
                    if let Err(e) = tx.send(s.clone()) {
                        eprintln!("Error sending to stream: {}", e);
                    }
                    if let Err(e) = state
                        .db
                        .begin_write()
                        .context("failed to begin write transaction")
                        .and_then(|mut write| {
                            {
                                write.set_durability(redb::Durability::Eventual);
                                let mut table =
                                    write.open_table(TABLE).context("Failed to open table")?;
                                let new_value = {
                                    let mut v = table
                                        .get(&id)
                                        .context(format!("Failed to get chat with id {}", &id))?
                                        .ok_or(anyhow!("Chat not found"))?
                                        .value();
                                    v.push_str(&s);
                                    v
                                };
                                table
                                    .insert(&id, new_value)
                                    .context("Failed to insert chat")?;
                            }
                            write.commit().context("Failed to commit transaction")?;

                            Ok(())
                        })
                    {
                        eprintln!("Error writing to database: {}", e);
                        return;
                    }
                })
                .await;
            state.streams.remove(&id);
        }
    });
    Ok(Body::from_stream(return_stream).into_response())
}

async fn inspect(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    return Json(AppInspect::from_state(&state)).into_response();
}
