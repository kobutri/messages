use anyhow_http::response::HttpJsonResult;
use anyhow_http::OptionExt;
use anyhow_http::{http::StatusCode, http_error};
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use futures::{stream, StreamExt};
use sqlx::{PgPool, Pool, Postgres, migrate::MigrateDatabase};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::interval;
use tokio_stream::wrappers::BroadcastStream;

struct StreamState {
    sender: Sender<String>,
    buffer: String,
}

struct AppState {
    pool: Pool<Postgres>,
    streams: DashMap<String, Arc<Mutex<StreamState>>>,
}

const CONNECTION_STR: &str = dotenvy_macro::dotenv!("DATABASE_URL");

#[tokio::main]
async fn main() {
    console_subscriber::init();

    if !<Postgres as MigrateDatabase>::database_exists(CONNECTION_STR)
        .await
        .unwrap()
    {
        <Postgres as MigrateDatabase>::create_database(CONNECTION_STR)
            .await
            .unwrap()
    }

    let pool = PgPool::connect(CONNECTION_STR).await.unwrap();

    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let state = Arc::new(AppState {
        pool: pool,
        streams: DashMap::new(),
    });

    let app = Router::new()
        .route("/", get(connect))
        .route("/", post(create_new))
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
        let query = sqlx::query!("SELECT content from chat WHERE name = $1", id)
            .fetch_one(&state.pool)
            .await
            .map_err(|_| http_error!(NOT_FOUND))?;

        Ok(query.content.unwrap_or_default())
    };
    if let Some(stream_state) = state.streams.get(id) {
        let guard = stream_state.lock().await;
        let old_content = old_content().await?;
        let stream = futures::StreamExt::chain(
            stream::iter(vec![old_content, guard.buffer.clone()]).map(|x| Ok(x)),
            BroadcastStream::new(guard.sender.subscribe()),
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
    let row = sqlx::query!(
        "INSERT INTO chat (name, content) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET content = EXCLUDED.content RETURNING id",
        id,
        ""
    )
    .fetch_one(&state.pool)
    .await?;
    let row_id = row.id;
    println!("Created new chat with id: {}", row_id);
    let (sender, receiver) = broadcast::channel(100);
    let stream_state = Arc::new(Mutex::new(StreamState {
        sender: sender.clone(),
        buffer: String::new(),
    }));
    state.streams.insert(id.clone(), stream_state.clone());
    let return_stream = BroadcastStream::new(receiver);
    tokio::task::spawn({
        let stream_state = stream_state.clone();
        let state = state.clone();
        let id = id.clone();
        let mut stream = Box::pin(stream);
        async move {
            let (tx, mut rx) = oneshot::channel();
            let handle = tokio::task::spawn({
                let stream_state = stream_state.clone();
                let state = state.clone();
                async move {
                    let mut stop = false;
                    let mut interval = interval(Duration::from_millis(500));
                    while !stop {
                        tokio::select! {
                            _ = interval.tick() => {},
                            x = &mut rx => {
                                if let Ok(_) = x {
                                    stop = true;
                                } else {
                                    eprintln!("Stream stopped unexpectedly");
                                    break;
                                }
                            }
                        }
                        let mut guard = stream_state.lock().await;
                        if let Err(e) = sqlx::query!(
                            "UPDATE chat SET content = chat.content || $1 WHERE id = $2",
                            guard.buffer,
                            row_id
                        )
                        .execute(&state.pool)
                        .await
                        {
                            eprintln!("Failed to update chat content: {}", e);
                            break;
                        }
                        guard.buffer.clear();
                    }
                }
            });
            while let Some(s) = stream.as_mut().next().await {
                let mut stream_state = stream_state.lock().await;
                stream_state.buffer.push_str(&s);
                if stream_state.sender.send(s).is_err() {
                    break;
                }
            }
            if let Err(_) = tx.send(()) {
                eprintln!("Failed to send completion signal");
            }
            handle.await.unwrap_or_else(|e| {
                eprintln!("Stream task failed: {}", e);
            });
            state.streams.remove(&id);
        }
    });
    Ok(Body::from_stream(return_stream).into_response())
}
