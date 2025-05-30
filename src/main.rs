use anyhow_http::{http_error_bail, OptionExt};
use anyhow_http::response::HttpJsonResult;
use anyhow_http::{http::StatusCode, http_error};
use axum::body::BodyDataStream;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use futures::future::join_all;
use futures::{StreamExt, stream};
use sqlx::{Acquire, PgPool, Pool, Postgres, migrate::MigrateDatabase};
use tokio::time::timeout;
use std::future;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::task::JoinSet;
use tokio_stream::wrappers::BroadcastStream;
struct AppState {
    pool: Pool<Postgres>,
    streams: DashMap<String, Arc<Mutex<Sender<String>>>>,
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
    let sender = Arc::new(Mutex::new(sender));
    state.streams.insert(id.clone(), sender.clone());
    let return_stream = BroadcastStream::new(receiver);
    tokio::task::spawn({
        let tx = sender.clone();
        let state = state.clone();
        let id = id.clone();
        async move {
            let mut set = JoinSet::new();
            stream
                .for_each(|s| {
                    let tx = tx.clone();
                    let state = state.clone();
                    set.spawn(async move {
                        let tx = tx.lock().await;
                        if let Err(e) = tx.send(s.clone()) {
                            eprintln!("Error sending to stream: {}", e);
                        }
                        if let Err(e) = sqlx::query!(
                            "UPDATE chat SET content = chat.content || $1 WHERE id = $2",
                            s,
                            row_id
                        )
                        .execute(&state.pool)
                        .await
                        {
                            eprintln!("Error updating chat: {}", e);
                        }
                    });
                    future::ready(())
                })
                .await;
            set.join_all().await;
            state.streams.remove(&id);
            println!("Stream for {} has ended", id);
        }
    });
    Ok(Body::from_stream(return_stream).into_response())
}
