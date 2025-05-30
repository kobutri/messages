use anyhow::{Context, anyhow, bail};
use anyhow_http::http::StatusCode;
use anyhow_http::response::HttpJsonResult;
use anyhow_http::{OptionExt, http_error};
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
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::signal;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, broadcast};
use tokio_stream::wrappers::BroadcastStream;

struct AppState {
    keyspace: fjall::TransactionalKeyspace,
    db: fjall::TransactionalPartition,
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
    async fn from_state(state: Arc<AppState>) -> Self {
        let streams_total: usize = state.streams.len();
        let mut chats_total: usize = 0;
        let chats = tokio::task::spawn_blocking({
            let state = state.clone();
            move || {
                let tx = state.keyspace.read_tx();
                tx.iter(&state.db)
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| {
                        let key = String::from_utf8(k.to_vec()).unwrap();
                        let value = String::from_utf8(v.to_vec()).unwrap();
                        chats_total += value.len();
                        (key, value.len())
                    })
                    .collect::<HashMap<String, usize>>()
            }
        })
        .await
        .expect("Failed to read from database");
        let streams = state.streams.iter().map(|r| r.key().clone()).collect();
        Self {
            streams_total,
            chats_total,
            chats,
            streams,
        }
    }
}

const DATABASE: &str = dotenv!("DATABASE");

#[tokio::main]
async fn main() {
    let keyspace = fjall::Config::new(DATABASE)
        .open_transactional()
        .expect(&format!("Failed to open database: {}", DATABASE));
    let db = keyspace
        .open_partition("chats", fjall::PartitionCreateOptions::default())
        .expect("Failed to open partition 'chats'");

    let state = Arc::new(AppState {
        keyspace,
        db,
        streams: DashMap::new(),
    });

    let app = Router::new()
        .route("/", get(connect))
        .route("/", post(create_new))
        .route("/inspect", get(inspect))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    // state
    //     .db
    //     .flush_async()
    //     .await
    //     .expect("Failed to flush database");
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
        let res = tokio::task::spawn_blocking({
            let state = state.clone();
            let id = id.clone();
            move || {
                let tx = state.keyspace.read_tx();
                tx.get(&state.db, id.as_bytes())
                    .map_err(|e| http_error!(INTERNAL_SERVER_ERROR, "Database error: {}", e))
            }
        })
        .await??
        .ok_or(http_error!(NOT_FOUND, "Chat with id {} not found", id))?;
        let res = String::from_utf8(res.to_vec())
            .map_err(|_| http_error!(INTERNAL_SERVER_ERROR, "Invalid UTF-8 in chat content"))?;
        Ok(res)
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

    tokio::task::spawn_blocking({
        let state = state.clone();
        let id = id.clone();
        move || {
            state
                .db
                .insert(id.as_bytes(), "")
                .map_err(|e| http_error!(INTERNAL_SERVER_ERROR, "Database error: {}", e))
        }
    })
    .await??;
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
                    let s = s.clone();
                    let id = id.clone();
                    if let Err(e) = tokio::task::spawn_blocking({
                        let state = state.clone();
                        let s = s.clone();
                        move || -> Result<(), anyhow::Error> {
                            let mut tx = state.keyspace.write_tx()?;
                            let Some(re) = tx.get(&state.db, id.as_bytes())? else {
                                bail!("Chat with id {} not found", id);
                            };
                            let mut v = String::from_utf8(re.to_vec())?;
                            v.push_str(&s);
                            tx.insert(&state.db, id.as_bytes(), v.as_bytes());
                            tx.commit()??;
                            Ok(())
                        }
                    })
                    .await
                    {
                        eprintln!("Error writing to database: {}", e);
                        return;
                    }

                    if let Err(e) = tx.send(s.clone()) {
                        eprintln!("Error sending to stream: {}", e);
                    }
                })
                .await;
            state.streams.remove(&id);
        }
    });
    Ok(Body::from_stream(return_stream).into_response())
}

async fn inspect(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    return Json(AppInspect::from_state(state).await).into_response();
}
