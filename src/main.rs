use anyhow_http::http::StatusCode;
use anyhow_http::response::HttpJsonResult;
use anyhow_http::{OptionExt, http_error_bail};
use axum::Json;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, broadcast};
use tokio_stream::wrappers::BroadcastStream;

struct AppState {
    chats: DashMap<String, String>,
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
        let mut streams_total: usize = 0;
        let chats_total = state.chats.len();
        let chats = state
            .chats
            .iter()
            .map(|r| {
                streams_total += r.value().len();
                (r.key().clone(), r.value().len())
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

const CONNECTION_STR: &str = dotenvy_macro::dotenv!("DATABASE_URL");

#[tokio::main]
async fn main() {
    console_subscriber::init();

    let state = Arc::new(AppState {
        chats: DashMap::new(),
        streams: DashMap::new(),
    });

    let app = Router::new()
        .route("/", get(connect))
        .route("/", post(create_new))
        .route("/inspect", get(inspect))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn connect(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> HttpJsonResult<impl IntoResponse> {
    let id = params.get("id").ok_or_status(StatusCode::BAD_REQUEST)?;
    let old_content = async || -> HttpJsonResult<String> {
        if let Some(content) = state.chats.get(id) {
            Ok(content.clone())
        } else {
            http_error_bail!(NOT_FOUND, "Chat not found");
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

    state.chats.insert(id.clone(), "".to_string());
    println!("Created new chat with id: {}", &id);
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
                    if let Some(mut chat) = state.chats.get_mut(&id) {
                        chat.push_str(&s);
                    } else {
                        eprintln!("Chat with id {} not found", id);
                        return;
                    }
                })
                .await;
            state.streams.remove(&id);
            println!("Stream for {} has ended", id);
        }
    });
    Ok(Body::from_stream(return_stream).into_response())
}

async fn inspect(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    return Json(AppInspect::from_state(&state)).into_response();
}
