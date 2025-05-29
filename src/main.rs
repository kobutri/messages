use anyhow_http::OptionExt;
use anyhow_http::response::HttpJsonResult;
use anyhow_http::{http::StatusCode, http_error};
use async_utf8_decoder::Utf8Decoder;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt, stream};
use sqlx::{Connection, Pool, Sqlite, SqliteConnection, SqlitePool, migrate::MigrateDatabase, pool::PoolConnection, Acquire};
use std::{collections::HashMap, future, io, sync::Arc};
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;

const CONNECTION_STR: &str = "sqlite:../chats.db";

struct AppState {
    pool: Pool<Sqlite>,
    streams: DashMap<String, Arc<Mutex<Sender<String>>>>,
}

#[tokio::main]
async fn main() {
    console_subscriber::init();

    if !<Sqlite as MigrateDatabase>::database_exists(CONNECTION_STR)
        .await
        .unwrap()
    {
        <Sqlite as MigrateDatabase>::create_database(CONNECTION_STR)
            .await
            .unwrap()
    }

    let pool = SqlitePool::connect(CONNECTION_STR).await.unwrap();

    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let connection = pool.acquire().await.unwrap();

    let state = Arc::new(AppState {
        pool: pool,
        streams: DashMap::new(),
    });

    let app = Router::new()
        .route("/", get(connect))
        .route("/", post(create_new))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn connect(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> HttpJsonResult<impl IntoResponse> {
    let id = params.get("id").ok_or_status(StatusCode::BAD_REQUEST)?;
    let old_content = async || -> HttpJsonResult<String> {
        let query = sqlx::query!("SELECT content from chat WHERE name = ?", id)
            .fetch_one(&state.pool)
            .await
            .map_err(|_| http_error!(NOT_FOUND))?;

        Ok(query.content.unwrap_or_default())
    };
    if let Some(sender) = state.streams.get_mut(id) {
        let sender = sender.lock().await;
        let old_content = old_content().await?;
        let stream = stream::once(async move { Ok(old_content) })
            .chain(BroadcastStream::new(sender.subscribe()));
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
    let response = reqwest::get(format!("http://localhost:3000?freq={freq}&max={max}"))
        .await?
        .error_for_status()?;
    let reader = response
        .bytes_stream()
        .map_err(io::Error::other)
        .into_async_read();
    let stream = Utf8Decoder::new(reader);
    let sender = Arc::new(Mutex::new(broadcast::Sender::new(16)));
    let mut conn = state.pool.acquire().await?;
    let mut tx = conn.begin().await?;
    sqlx::query!("DELETE FROM chat WHERE name = ?", id).execute(&mut *tx).await?;
    let row_id = sqlx::query!("INSERT INTO chat (name, content) values (?, ?)", id, "")
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
    tx.commit().await?;

    tokio::task::spawn({
        let tx = sender.clone();
        let state = state.clone();
        let id = id.clone();
        async move {
            stream
                .filter_map(async |s| s.ok())
                .for_each(async |s| {
                    let tx = tx.lock().await;
                    if let Err(e) = sqlx::query!(
                        "UPDATE chat SET content = content || ? WHERE id = ?",
                        s,
                        row_id
                    )
                    .execute(&state.pool)
                    .await
                    {
                        eprintln!("Error updating chat: {}", e);
                        return;
                    }
                    tx.send(s);
                })
                .await;
            state.streams.remove(&id);
        }
    });
    state.streams.insert(id, sender.clone());
    let return_stream = BroadcastStream::new(sender.lock().await.subscribe());
    Ok(Body::from_stream(return_stream))
}
