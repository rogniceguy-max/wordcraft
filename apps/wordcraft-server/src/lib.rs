//! Local, durable document storage for the WordCraft browser editor.
//!
//! Snapshot writes use compare-and-swap versions. A stale client receives a conflict and cannot
//! silently replace a newer snapshot. A narrow WebSocket prototype syncs one plain-text paragraph;
//! it is not a rich-document collaboration adapter for the browser editor.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable)]

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use fs2::FileExt;
use futures_util::{SinkExt, StreamExt};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::{Any, Doc, GetString, Out, ReadTxn, StateVector, Text, Transact, Update};

const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_TITLE_CHARS: usize = 240;
const MAX_COLLAB_UPDATE_BYTES: usize = 8 * 1024 * 1024;
const MAX_COLLAB_TEXT_BYTES: usize = 8 * 1024 * 1024;
const COLLAB_TEXT_ROOT: &str = "body:main";
const COLLAB_ROOM_CAPACITY: usize = 256;

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    collab_rooms: Arc<CollaborationRooms>,
    _server_lock: Arc<DatabaseProcessLock>,
}

/// Construct the JSON API router. Static file serving is added by the binary when configured.
pub fn api_router(store: Arc<Store>) -> io::Result<Router> {
    let server_lock = Arc::new(DatabaseProcessLock::acquire(store.path())?);
    Ok(Router::new()
        .route("/api/health", get(health))
        .route("/api/documents", get(list_documents).post(create_document))
        .route("/api/documents/{id}", get(read_document).put(save_document))
        .route("/api/documents/{id}/collab", get(collaborate))
        .route("/api/documents/{id}/versions", get(list_versions))
        .route("/api/documents/{id}/restore-version", post(restore_version))
        .route("/api/documents/{id}/trash", post(trash_document))
        .route("/api/documents/{id}/untrash", post(untrash_document))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(AppState { store, collab_rooms: Arc::new(CollaborationRooms::default()), _server_lock: server_lock }))
}

struct DatabaseProcessLock {
    _file: File,
}

impl DatabaseProcessLock {
    fn acquire(database_path: &Path) -> io::Result<Self> {
        let canonical_database = std::fs::canonicalize(database_path)?;
        let mut lock_name = canonical_database.into_os_string();
        lock_name.push(".wordcraft-server.lock");
        let lock_path = PathBuf::from(lock_name);
        let file = OpenOptions::new().create(true).truncate(false).read(true).write(true).open(&lock_path)?;
        file.try_lock_exclusive().map_err(|error| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "another WordCraft server already holds the database lock for {} ({}): {error}",
                    database_path.display(),
                    lock_path.display()
                ),
            )
        })?;
        Ok(DatabaseProcessLock { _file: file })
    }
}

#[derive(Default)]
struct CollaborationRooms {
    // Live fanout is process-local; run one server instance for each SQLite database.
    rooms: Mutex<HashMap<String, Weak<CollaborationRoom>>>,
}

struct CollaborationRoom {
    id: String,
    epoch: u64,
    #[cfg(test)]
    seed_update: Vec<u8>,
    state: Mutex<CollaborationRoomState>,
    updates: broadcast::Sender<Vec<u8>>,
}

struct CollaborationRoomState {
    document: Doc,
    digests: HashSet<Vec<u8>>,
}

#[derive(Clone)]
struct StoredCollaborationUpdate {
    digest: Vec<u8>,
    bytes: Vec<u8>,
}

struct CollaborationData {
    epoch: u64,
    seed_update: Vec<u8>,
    updates: Vec<StoredCollaborationUpdate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DocumentSummary {
    pub id: String,
    pub title: String,
    pub version: u64,
    pub created_at: i64,
    pub modified_at: i64,
    pub deleted: bool,
    /// True once this document has entered its durable live-text collaboration epoch.
    /// The epoch survives disconnects and restarts, and its edits are not in version history.
    pub snapshot_locked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DocumentRecord {
    #[serde(flatten)]
    pub summary: DocumentSummary,
    pub snapshot: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VersionSummary {
    pub version: u64,
    pub title: String,
    pub created_at: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    include_deleted: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateDocument {
    title: String,
    snapshot: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SaveDocument {
    base_version: u64,
    title: String,
    snapshot: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestoreVersion {
    base_version: u64,
    version: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangeDeleted {
    base_version: u64,
}

async fn health() -> Json<Value> {
    Json(serde_json::json!({"status": "ok", "storage": "sqlite"}))
}

async fn collaborate(State(state): State<AppState>, AxumPath(id): AxumPath<String>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_COLLAB_UPDATE_BYTES)
        .max_frame_size(MAX_COLLAB_UPDATE_BYTES)
        .on_upgrade(move |socket| collaboration_socket(socket, state, id))
        .into_response()
}

async fn collaboration_socket(mut socket: WebSocket, state: AppState, id: String) {
    let store = state.store.clone();
    let rooms = state.collab_rooms.clone();
    let room_id = id.clone();
    let room_result = tokio::task::spawn_blocking(move || rooms.get_or_load(&store, &room_id)).await;
    let room = match room_result {
        Ok(Ok(room)) => room,
        Ok(Err(error)) => {
            let (code, reason) = collaboration_close_reason(&error);
            let _ = socket.send(close_message(code, &reason)).await;
            return;
        }
        Err(error) => {
            eprintln!("WordCraft collaboration room worker failed: {error}");
            let _ = socket.send(close_message(close_code::ERROR, "collaboration room could not be loaded")).await;
            return;
        }
    };

    // Subscribe before capturing the initial state. An update arriving between these steps can
    // appear both in this state and in the receiver; Yrs updates are idempotent, so no event is lost.
    let mut updates = room.updates.subscribe();
    let initial = match room.full_state() {
        Ok(update) => update,
        Err(error) => {
            let (code, reason) = collaboration_close_reason(&error);
            let _ = socket.send(close_message(code, &reason)).await;
            return;
        }
    };
    if socket.send(Message::Binary(initial.into())).await.is_err() {
        return;
    }

    let (mut sender, mut incoming) = socket.split();
    loop {
        tokio::select! {
            message = incoming.next() => {
                match message {
                    Some(Ok(Message::Binary(bytes))) => {
                        let room = room.clone();
                        let store = state.store.clone();
                        let update = bytes.to_vec();
                        let duplicate_ack = update.clone();
                        let result = tokio::task::spawn_blocking(move || room.accept_update(&store, &update)).await;
                        match result {
                            Ok(Ok(true)) => {}
                            Ok(Ok(false)) => {
                                // The update is already durable. Echo only to this socket so a
                                // reconnecting sender can drain its pending bytes without a peer
                                // rebroadcast.
                                if sender.send(Message::Binary(duplicate_ack.into())).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(error)) => {
                                let (code, reason) = collaboration_close_reason(&error);
                                let _ = sender.send(close_message(code, &reason)).await;
                                break;
                            }
                            Err(error) => {
                                eprintln!("WordCraft collaboration update worker failed: {error}");
                                let _ = sender.send(close_message(close_code::ERROR, "collaboration update could not be stored")).await;
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Text(_))) => {
                        let _ = sender.send(close_message(close_code::UNSUPPORTED, "send binary Yrs v1 updates only")).await;
                        break;
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                }
            }
            update = updates.recv() => {
                match update {
                    Ok(update) => {
                        if sender.send(Message::Binary(update.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // A lagging peer can recover from one full-state update without a cursor.
                        match room.full_state() {
                            Ok(update) => {
                                if sender.send(Message::Binary(update.into())).await.is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                let (code, reason) = collaboration_close_reason(&error);
                                let _ = sender.send(close_message(code, &reason)).await;
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

fn close_message(code: u16, reason: &str) -> Message {
    let mut bounded_reason = String::new();
    for character in reason.chars() {
        if bounded_reason.len().saturating_add(character.len_utf8()) > 120 {
            break;
        }
        bounded_reason.push(character);
    }
    Message::Close(Some(CloseFrame { code, reason: bounded_reason.into() }))
}

fn collaboration_close_reason(error: &StoreError) -> (u16, String) {
    match error {
        StoreError::NotFound | StoreError::Deleted | StoreError::UnsupportedCollaborationDocument(_) => (close_code::POLICY, error.to_string()),
        StoreError::InvalidCollaborationUpdate(_) | StoreError::InvalidCollaborationState(_) => (close_code::POLICY, error.to_string()),
        _ => {
            eprintln!("WordCraft collaboration error: {error}");
            (close_code::ERROR, "collaboration storage failed".into())
        }
    }
}

async fn list_documents(State(state): State<AppState>, Query(query): Query<ListQuery>) -> Result<Json<Vec<DocumentSummary>>, ApiError> {
    let include_deleted = query.include_deleted.unwrap_or(false);
    let documents = in_store(state.store, move |store| store.list(include_deleted)).await?;
    Ok(Json(documents))
}

async fn create_document(State(state): State<AppState>, Json(request): Json<CreateDocument>) -> Result<(StatusCode, Json<DocumentRecord>), ApiError> {
    let record = in_store(state.store, move |store| store.create(&request.title, request.snapshot)).await?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn read_document(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Result<Json<DocumentRecord>, ApiError> {
    let record = in_store(state.store, move |store| store.get(&id)).await?;
    Ok(Json(record))
}

async fn save_document(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SaveDocument>,
) -> Result<Json<DocumentRecord>, ApiError> {
    let record = in_store(state.store, move |store| store.save(&id, request.base_version, &request.title, request.snapshot)).await?;
    Ok(Json(record))
}

async fn list_versions(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Result<Json<Vec<VersionSummary>>, ApiError> {
    let versions = in_store(state.store, move |store| store.versions(&id)).await?;
    Ok(Json(versions))
}

async fn restore_version(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<RestoreVersion>,
) -> Result<Json<DocumentRecord>, ApiError> {
    let record = in_store(state.store, move |store| store.restore_version(&id, request.base_version, request.version)).await?;
    Ok(Json(record))
}

async fn trash_document(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ChangeDeleted>,
) -> Result<Json<DocumentSummary>, ApiError> {
    let summary = in_store(state.store, move |store| store.set_deleted(&id, request.base_version, true)).await?;
    Ok(Json(summary))
}

async fn untrash_document(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ChangeDeleted>,
) -> Result<Json<DocumentSummary>, ApiError> {
    let summary = in_store(state.store, move |store| store.set_deleted(&id, request.base_version, false)).await?;
    Ok(Json(summary))
}

async fn in_store<T, F>(store: Arc<Store>, operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(&store))
        .await
        .map_err(|error| {
            eprintln!("WordCraft storage worker failed: {error}");
            ApiError::Internal
        })?
        .map_err(ApiError::from)
}

#[derive(Debug, Error)]
enum StoreError {
    #[error("document not found")]
    NotFound,
    #[error("document was deleted")]
    Deleted,
    #[error("document version conflict (current version {current_version})")]
    Conflict { current_version: u64 },
    #[error("document has an active collaboration epoch; snapshot save and restore are disabled (current version {current_version})")]
    CollaborationActive { current_version: u64 },
    #[error("invalid title: {0}")]
    InvalidTitle(String),
    #[error("invalid document snapshot: {0}")]
    InvalidSnapshot(String),
    #[error("stored document is invalid: {0}")]
    Corrupt(String),
    #[error("database lock was poisoned")]
    Poisoned,
    #[error("document version is out of range")]
    VersionRange,
    #[error("real-time collaboration supports only one plain-text body paragraph: {0}")]
    UnsupportedCollaborationDocument(String),
    #[error("invalid Yrs v1 collaboration update: {0}")]
    InvalidCollaborationUpdate(String),
    #[error("stored collaboration state is invalid: {0}")]
    InvalidCollaborationState(String),
    #[error("collaboration epoch is unavailable or out of date")]
    CollaborationEpoch,
    #[error("collaboration room lock was poisoned")]
    CollaborationLock,
    #[error("collaboration room registry lock was poisoned")]
    CollaborationRegistryLock,
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("file system error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound,
    Conflict(u64),
    CollaborationActive(u64),
    Internal,
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound | StoreError::Deleted => ApiError::NotFound,
            StoreError::Conflict { current_version } => ApiError::Conflict(current_version),
            StoreError::CollaborationActive { current_version } => ApiError::CollaborationActive(current_version),
            StoreError::InvalidTitle(message) | StoreError::InvalidSnapshot(message) => ApiError::BadRequest(message),
            error => {
                eprintln!("WordCraft storage error: {error}");
                ApiError::Internal
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, serde_json::json!({"error": message})),
            ApiError::NotFound => (StatusCode::NOT_FOUND, serde_json::json!({"error": "document not found"})),
            ApiError::Conflict(version) => {
                (StatusCode::CONFLICT, serde_json::json!({"error": "document version conflict", "currentVersion": version}))
            }
            ApiError::CollaborationActive(version) => (
                StatusCode::CONFLICT,
                serde_json::json!({
                    "error": "document has an active collaboration epoch; snapshot save and restore are disabled",
                    "currentVersion": version
                }),
            ),
            ApiError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({"error": "internal storage failure"})),
        };
        (status, Json(body)).into_response()
    }
}

/// SQLite store. All writes are transactional and use FULL synchronous durability before
/// returning success. The database is the single consistency boundary for snapshots and media.
pub struct Store {
    connection: Mutex<Connection>,
    path: PathBuf,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_inner(path.as_ref()).map_err(|error| error.to_string())
    }

    fn open_inner(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let schema_version = connection.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?;
        if schema_version > 2 {
            return Err(StoreError::InvalidSnapshot(format!("database schema version {schema_version} is newer than this server")));
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL,
                version INTEGER NOT NULL CHECK(version >= 1),
                created_at INTEGER NOT NULL,
                modified_at INTEGER NOT NULL,
                deleted_at INTEGER,
                snapshot BLOB NOT NULL,
                format_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS document_versions (
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL CHECK(version >= 1),
                title TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                snapshot BLOB NOT NULL,
                format_version INTEGER NOT NULL,
                PRIMARY KEY(document_id, version),
                FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS collaboration_documents (
                document_id TEXT PRIMARY KEY NOT NULL,
                epoch INTEGER NOT NULL CHECK(epoch >= 1),
                seed_update BLOB NOT NULL,
                FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS collaboration_updates (
                document_id TEXT NOT NULL,
                epoch INTEGER NOT NULL CHECK(epoch >= 1),
                seq INTEGER NOT NULL CHECK(seq >= 1),
                digest BLOB NOT NULL CHECK(length(digest) = 32),
                update_bytes BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY(document_id, epoch, seq),
                UNIQUE(document_id, epoch, digest),
                FOREIGN KEY(document_id) REFERENCES collaboration_documents(document_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS documents_active_modified ON documents(deleted_at, modified_at DESC);
            PRAGMA user_version = 2;",
        )?;
        Ok(Store { connection: Mutex::new(connection), path: path.to_path_buf() })
    }

    /// Return the database path for deployment diagnostics.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }

    fn list(&self, include_deleted: bool) -> Result<Vec<DocumentSummary>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, title, version, created_at, modified_at, deleted_at,
                    EXISTS(SELECT 1 FROM collaboration_documents c WHERE c.document_id = documents.id)
             FROM documents WHERE deleted_at IS NULL OR ?1 = 1
             ORDER BY deleted_at IS NOT NULL, modified_at DESC, id",
        )?;
        let rows = statement.query_map([include_deleted], |row| {
            Ok(DocumentSummary {
                id: row.get(0)?,
                title: row.get(1)?,
                version: nonnegative_version(row.get(2)?),
                created_at: row.get(3)?,
                modified_at: row.get(4)?,
                deleted: row.get::<_, Option<i64>>(5)?.is_some(),
                snapshot_locked: row.get(6)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::Sqlite)
    }

    fn create(&self, title: &str, snapshot: Value) -> Result<DocumentRecord, StoreError> {
        let title = validate_title(title)?;
        let id = Uuid::new_v4().to_string();
        let created_at = now_ms();
        let snapshot = prepare_snapshot(snapshot, &title, 1)?;
        let bytes = serde_json::to_vec(&snapshot)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO documents(id, title, version, created_at, modified_at, snapshot, format_version)
             VALUES(?1, ?2, 1, ?3, ?3, ?4, ?5)",
            params![id, title, created_at, bytes, i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION)],
        )?;
        transaction.execute(
            "INSERT INTO document_versions(document_id, version, title, created_at, snapshot, format_version)
             VALUES(?1, 1, ?2, ?3, ?4, ?5)",
            params![id, title, created_at, bytes, i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION)],
        )?;
        transaction.commit()?;
        let summary = DocumentSummary { id, title, version: 1, created_at, modified_at: created_at, deleted: false, snapshot_locked: false };
        Ok(DocumentRecord { summary, snapshot })
    }

    fn get(&self, id: &str) -> Result<DocumentRecord, StoreError> {
        let connection = self.connection()?;
        let row = connection
            .query_row(
                "SELECT id, title, version, created_at, modified_at, deleted_at, snapshot,
                        EXISTS(SELECT 1 FROM collaboration_documents c WHERE c.document_id = documents.id)
                 FROM documents WHERE id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                        row.get::<_, bool>(7)?,
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if row.5.is_some() {
            return Err(StoreError::Deleted);
        }
        let snapshot = serde_json::from_slice::<Value>(&row.6).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        validate_snapshot(&snapshot).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        Ok(DocumentRecord {
            summary: DocumentSummary {
                id: row.0,
                title: row.1,
                version: nonnegative_version(row.2),
                created_at: row.3,
                modified_at: row.4,
                deleted: false,
                snapshot_locked: row.7,
            },
            snapshot,
        })
    }

    fn save(&self, id: &str, base_version: u64, title: &str, snapshot: Value) -> Result<DocumentRecord, StoreError> {
        let title = validate_title(title)?;
        let base_version = to_sql_version(base_version)?;
        let new_version = base_version.checked_add(1).ok_or(StoreError::VersionRange)?;
        let modified_at = now_ms();
        let snapshot = prepare_snapshot(snapshot, &title, new_version as u64)?;
        let bytes = serde_json::to_vec(&snapshot)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row("SELECT version, created_at, deleted_at FROM documents WHERE id = ?1", [id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?))
            })
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if current.2.is_some() {
            return Err(StoreError::Deleted);
        }
        if transaction.query_row("SELECT 1 FROM collaboration_documents WHERE document_id = ?1", [id], |_| Ok(())).optional()?.is_some() {
            return Err(StoreError::CollaborationActive { current_version: nonnegative_version(current.0) });
        }
        if current.0 != base_version {
            return Err(StoreError::Conflict { current_version: nonnegative_version(current.0) });
        }
        transaction.execute(
            "UPDATE documents SET title = ?1, version = ?2, modified_at = ?3, snapshot = ?4, format_version = ?5
             WHERE id = ?6 AND version = ?7 AND deleted_at IS NULL",
            params![title, new_version, modified_at, bytes, i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION), id, base_version],
        )?;
        transaction.execute(
            "INSERT INTO document_versions(document_id, version, title, created_at, snapshot, format_version)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, new_version, title, modified_at, bytes, i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION)],
        )?;
        transaction.commit()?;
        Ok(DocumentRecord {
            summary: DocumentSummary {
                id: id.to_string(),
                title,
                version: new_version as u64,
                created_at: current.1,
                modified_at,
                deleted: false,
                snapshot_locked: false,
            },
            snapshot,
        })
    }

    fn versions(&self, id: &str) -> Result<Vec<VersionSummary>, StoreError> {
        let connection = self.connection()?;
        let exists = connection.query_row("SELECT 1 FROM documents WHERE id = ?1", [id], |_| Ok(())).optional()?;
        if exists.is_none() {
            return Err(StoreError::NotFound);
        }
        let mut statement =
            connection.prepare("SELECT version, title, created_at FROM document_versions WHERE document_id = ?1 ORDER BY version DESC")?;
        let rows = statement
            .query_map([id], |row| Ok(VersionSummary { version: nonnegative_version(row.get(0)?), title: row.get(1)?, created_at: row.get(2)? }))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::Sqlite)
    }

    fn load_or_seed_collaboration(&self, id: &str) -> Result<CollaborationData, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = transaction
            .query_row("SELECT snapshot, deleted_at FROM documents WHERE id = ?1", [id], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<i64>>(1)?))
            })
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if row.1.is_some() {
            return Err(StoreError::Deleted);
        }

        let collaboration = transaction
            .query_row("SELECT epoch, seed_update FROM collaboration_documents WHERE document_id = ?1", [id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .optional()?;
        let (epoch, seed_update) = if let Some((epoch, seed_update)) = collaboration {
            (positive_version(epoch)?, seed_update)
        } else {
            let snapshot = serde_json::from_slice::<Value>(&row.0).map_err(|error| StoreError::Corrupt(error.to_string()))?;
            let document = wordcraft_engine::io::document_from_storage_snapshot(&snapshot).map_err(|error| StoreError::Corrupt(error.to_string()))?;
            let text = collaboration_seed_text(&document)?;
            let seed_update = make_collaboration_seed(&text)?;
            transaction
                .execute("INSERT INTO collaboration_documents(document_id, epoch, seed_update) VALUES(?1, 1, ?2)", params![id, seed_update])?;
            (1, seed_update)
        };

        let mut statement =
            transaction.prepare("SELECT digest, update_bytes FROM collaboration_updates WHERE document_id = ?1 AND epoch = ?2 ORDER BY seq")?;
        let rows = statement
            .query_map(params![id, to_sql_version(epoch)?], |row| Ok(StoredCollaborationUpdate { digest: row.get(0)?, bytes: row.get(1)? }))?;
        let updates = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for update in &updates {
            if update.digest.len() != 32 || Sha256::digest(&update.bytes).as_slice() != update.digest.as_slice() {
                return Err(StoreError::InvalidCollaborationState("update digest does not match stored bytes".into()));
            }
        }
        transaction.commit()?;
        Ok(CollaborationData { epoch, seed_update, updates })
    }

    fn append_collaboration_update(&self, id: &str, epoch: u64, digest: &[u8], update: &[u8], text: &str) -> Result<bool, StoreError> {
        if digest.len() != 32 || Sha256::digest(update).as_slice() != digest {
            return Err(StoreError::InvalidCollaborationUpdate("update digest does not match payload".into()));
        }
        let epoch = to_sql_version(epoch)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let document_row = transaction
            .query_row("SELECT deleted_at, snapshot FROM documents WHERE id = ?1", [id], |row| {
                Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if document_row.0.is_some() {
            return Err(StoreError::Deleted);
        }
        let stored_epoch = transaction
            .query_row("SELECT epoch FROM collaboration_documents WHERE document_id = ?1", [id], |row| row.get::<_, i64>(0))
            .optional()?
            .ok_or(StoreError::CollaborationEpoch)?;
        if stored_epoch != epoch {
            return Err(StoreError::CollaborationEpoch);
        }
        let already_stored = transaction
            .query_row(
                "SELECT 1 FROM collaboration_updates WHERE document_id = ?1 AND epoch = ?2 AND digest = ?3",
                params![id, epoch, digest],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if already_stored {
            transaction.commit()?;
            return Ok(false);
        }

        // Keep the current REST snapshot useful without turning every keystroke update into a
        // user-visible version. The immutable document_versions rows and their version numbers
        // remain separate from the collaboration epoch and update log.
        let snapshot = serde_json::from_slice::<Value>(&document_row.1).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let mut document = wordcraft_engine::io::document_from_storage_snapshot(&snapshot).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let _ = collaboration_seed_text(&document)?;
        document.body = vec![wordcraft_doc::para_block(wordcraft_doc::Paragraph::with_text(text, wordcraft_doc::CharProps::default()))];
        document.core.modified = wordcraft_engine::cmd::now_iso();
        let snapshot = wordcraft_engine::io::storage_snapshot(&document).map_err(StoreError::InvalidSnapshot)?;
        let snapshot_bytes = serde_json::to_vec(&snapshot)?;
        let modified_at = now_ms();
        let previous_seq = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM collaboration_updates WHERE document_id = ?1 AND epoch = ?2",
            params![id, epoch],
            |row| row.get::<_, i64>(0),
        )?;
        let seq = previous_seq.checked_add(1).ok_or(StoreError::VersionRange)?;
        transaction.execute(
            "INSERT INTO collaboration_updates(document_id, epoch, seq, digest, update_bytes, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, epoch, seq, digest, update, modified_at],
        )?;
        transaction.execute("UPDATE documents SET snapshot = ?1, modified_at = ?2 WHERE id = ?3", params![snapshot_bytes, modified_at, id])?;
        transaction.commit()?;
        Ok(true)
    }

    fn restore_version(&self, id: &str, base_version: u64, target_version: u64) -> Result<DocumentRecord, StoreError> {
        let base_version = to_sql_version(base_version)?;
        let target_version = to_sql_version(target_version)?;
        let modified_at = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row("SELECT version, created_at, deleted_at FROM documents WHERE id = ?1", [id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?))
            })
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if current.2.is_some() {
            return Err(StoreError::Deleted);
        }
        if transaction.query_row("SELECT 1 FROM collaboration_documents WHERE document_id = ?1", [id], |_| Ok(())).optional()?.is_some() {
            return Err(StoreError::CollaborationActive { current_version: nonnegative_version(current.0) });
        }
        if current.0 != base_version {
            return Err(StoreError::Conflict { current_version: nonnegative_version(current.0) });
        }
        let new_version = current.0.checked_add(1).ok_or(StoreError::VersionRange)?;
        let target = transaction
            .query_row("SELECT title, snapshot FROM document_versions WHERE document_id = ?1 AND version = ?2", params![id, target_version], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .optional()?
            .ok_or(StoreError::NotFound)?;
        let old_snapshot = serde_json::from_slice::<Value>(&target.1).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let snapshot = prepare_snapshot(old_snapshot, &target.0, new_version as u64)?;
        let bytes = serde_json::to_vec(&snapshot)?;
        transaction.execute(
            "UPDATE documents SET title = ?1, version = ?2, modified_at = ?3, snapshot = ?4, format_version = ?5
             WHERE id = ?6 AND version = ?7 AND deleted_at IS NULL",
            params![target.0, new_version, modified_at, bytes, i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION), id, base_version],
        )?;
        transaction.execute(
            "INSERT INTO document_versions(document_id, version, title, created_at, snapshot, format_version)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, new_version, target.0, modified_at, bytes, i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION)],
        )?;
        transaction.commit()?;
        Ok(DocumentRecord {
            summary: DocumentSummary {
                id: id.to_string(),
                title: target.0,
                version: new_version as u64,
                created_at: current.1,
                modified_at,
                deleted: false,
                snapshot_locked: false,
            },
            snapshot,
        })
    }

    fn set_deleted(&self, id: &str, base_version: u64, deleted: bool) -> Result<DocumentSummary, StoreError> {
        let base_version = to_sql_version(base_version)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT title, version, created_at, modified_at, deleted_at,
                        EXISTS(SELECT 1 FROM collaboration_documents c WHERE c.document_id = documents.id)
                 FROM documents WHERE id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, bool>(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::NotFound)?;
        if current.1 != base_version {
            return Err(StoreError::Conflict { current_version: nonnegative_version(current.1) });
        }
        if current.4.is_some() == deleted {
            return if deleted { Err(StoreError::Deleted) } else { Err(StoreError::NotFound) };
        }
        transaction.execute(
            "UPDATE documents SET deleted_at = ?1 WHERE id = ?2 AND version = ?3",
            params![if deleted { Some(now) } else { None }, id, base_version],
        )?;
        transaction.commit()?;
        Ok(DocumentSummary {
            id: id.to_string(),
            title: current.0,
            version: nonnegative_version(current.1),
            created_at: current.2,
            modified_at: current.3,
            deleted,
            snapshot_locked: current.5,
        })
    }
}

impl CollaborationRooms {
    fn get_or_load(&self, store: &Store, id: &str) -> Result<Arc<CollaborationRoom>, StoreError> {
        let mut rooms = self.rooms.lock().map_err(|_| StoreError::CollaborationRegistryLock)?;
        rooms.retain(|_, room| room.strong_count() > 0);
        if let Some(room) = rooms.get(id).and_then(Weak::upgrade) {
            return Ok(room);
        }

        // The registry lock makes first-load/seed one operation per document in this process.
        // The SQLite IMMEDIATE transaction ensures concurrent initializers persist one seed row.
        let data = store.load_or_seed_collaboration(id)?;
        let room = Arc::new(CollaborationRoom::from_data(id.to_string(), data)?);
        rooms.insert(id.to_string(), Arc::downgrade(&room));
        Ok(room)
    }
}

impl CollaborationRoom {
    fn from_data(id: String, data: CollaborationData) -> Result<Self, StoreError> {
        let document = rebuild_collaboration_document(&data.seed_update, &data.updates, false)?;
        let digests = data.updates.iter().map(|update| update.digest.clone()).collect();
        let (updates, _) = broadcast::channel(COLLAB_ROOM_CAPACITY);
        Ok(CollaborationRoom {
            id,
            epoch: data.epoch,
            #[cfg(test)]
            seed_update: data.seed_update,
            state: Mutex::new(CollaborationRoomState { document, digests }),
            updates,
        })
    }

    fn full_state(&self) -> Result<Vec<u8>, StoreError> {
        let state = self.state.lock().map_err(|_| StoreError::CollaborationLock)?;
        Ok(state.document.transact().encode_state_as_update_v1(&StateVector::default()))
    }

    /// Validate on a replay-built candidate, commit the update, then expose it in the room.
    /// The room mutex serializes validation, persistence, and broadcast so observers see only
    /// committed updates in a single consistent order.
    fn accept_update(&self, store: &Store, bytes: &[u8]) -> Result<bool, StoreError> {
        if bytes.is_empty() || bytes.len() > MAX_COLLAB_UPDATE_BYTES {
            return Err(StoreError::InvalidCollaborationUpdate("update is empty or exceeds the message size limit".into()));
        }
        let digest = Sha256::digest(bytes).to_vec();
        let mut state = self.state.lock().map_err(|_| StoreError::CollaborationLock)?;
        if state.digests.contains(&digest) {
            return Ok(false);
        }

        let update = StoredCollaborationUpdate { digest: digest.clone(), bytes: bytes.to_vec() };
        let current_state = state.document.transact().encode_state_as_update_v1(&StateVector::default());
        let candidate = rebuild_collaboration_document(&current_state, std::slice::from_ref(&update), true)?;

        let text = collaboration_text(&candidate)?;
        let inserted = store.append_collaboration_update(&self.id, self.epoch, &digest, bytes, &text)?;
        if !inserted {
            // This can happen if another server process stored the same payload while this room
            // was running. Refresh from the durable log and avoid a duplicate broadcast.
            let data = store.load_or_seed_collaboration(&self.id)?;
            if data.epoch != self.epoch {
                return Err(StoreError::CollaborationEpoch);
            }
            let document = rebuild_collaboration_document(&data.seed_update, &data.updates, false)?;
            state.digests = data.updates.iter().map(|entry| entry.digest.clone()).collect();
            state.document = document;
            return Ok(false);
        }

        state.document = candidate;
        state.digests.insert(digest);
        let _ = self.updates.send(bytes.to_vec());
        Ok(true)
    }
}

fn collaboration_seed_text(document: &wordcraft_doc::Document) -> Result<String, StoreError> {
    if document.settings.track_changes || document.settings.protection.is_some() {
        return Err(StoreError::UnsupportedCollaborationDocument("track changes and document protection are not supported".into()));
    }
    if !document.parts.is_empty() || !document.comments.is_empty() || !document.revisions.is_empty() || !document.sources.is_empty() {
        return Err(StoreError::UnsupportedCollaborationDocument("headers, footers, comments, revisions, and sources are not supported".into()));
    }
    if document.body.len() != 1 {
        return Err(StoreError::UnsupportedCollaborationDocument("the body must contain exactly one paragraph".into()));
    }
    let Some(paragraph) = document.body.first().and_then(|block| block.as_para()) else {
        return Err(StoreError::UnsupportedCollaborationDocument("tables are not supported".into()));
    };
    let run_len = paragraph.runs.iter().try_fold(0usize, |total, run| total.checked_add(run.len));
    let plain_runs = run_len == Some(paragraph.text.len()) && paragraph.runs.iter().all(|run| run.props.is_empty());
    if !paragraph.props.is_empty()
        || !paragraph.mark.is_empty()
        || paragraph.section.is_some()
        || !paragraph.objects.is_empty()
        || paragraph.text.contains('\n')
        || paragraph.text.contains('\r')
        || paragraph.text.chars().any(unsupported_collaboration_char)
        || !plain_runs
    {
        return Err(StoreError::UnsupportedCollaborationDocument("formatting, inline objects, and section breaks are not supported".into()));
    }
    if paragraph.text.len() > MAX_COLLAB_TEXT_BYTES {
        return Err(StoreError::UnsupportedCollaborationDocument("the body paragraph exceeds the collaboration size limit".into()));
    }
    Ok(paragraph.text.clone())
}

fn make_collaboration_seed(text: &str) -> Result<Vec<u8>, StoreError> {
    let document = Doc::new();
    let shared_text = document.get_or_insert_text(COLLAB_TEXT_ROOT);
    if !text.is_empty() {
        shared_text.push(&mut document.transact_mut(), text);
    }
    let update = document.transact().encode_state_as_update_v1(&StateVector::default());
    Ok(update)
}

fn unsupported_collaboration_char(character: char) -> bool {
    use wordcraft_doc::para::OBJ;
    character.is_control() || matches!(character, OBJ | '\u{2028}' | '\u{2029}')
}

fn rebuild_collaboration_document(seed_update: &[u8], updates: &[StoredCollaborationUpdate], incoming_update: bool) -> Result<Doc, StoreError> {
    let document = Doc::new();
    let _ = document.get_or_insert_text(COLLAB_TEXT_ROOT);
    apply_collaboration_update(&document, seed_update, incoming_update)?;
    for update in updates {
        apply_collaboration_update(&document, &update.bytes, incoming_update)?;
    }
    validate_collaboration_state(&document, incoming_update)?;
    Ok(document)
}

fn apply_collaboration_update(document: &Doc, bytes: &[u8], incoming_update: bool) -> Result<(), StoreError> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let update = Update::decode_v1(bytes).map_err(|error| error.to_string())?;
        document.transact_mut().apply_update(update).map_err(|error| error.to_string())
    }));
    let result = match result {
        Ok(result) => result,
        Err(_) => Err("Yrs rejected a malformed update during guarded validation".to_string()),
    };
    result.map_err(|error| if incoming_update { StoreError::InvalidCollaborationUpdate(error) } else { StoreError::InvalidCollaborationState(error) })
}

fn validate_collaboration_state(document: &Doc, incoming_update: bool) -> Result<(), StoreError> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let transaction = document.transact();
        let mut root = None;
        for (name, value) in transaction.root_refs() {
            match (name, value) {
                (COLLAB_TEXT_ROOT, Out::YText(text)) if root.is_none() => root = Some(text),
                _ => return Err("only the body:main text root is supported".to_string()),
            }
        }
        let Some(text) = root else {
            return Err("the body:main text root is missing".to_string());
        };
        for chunk in text.diff(&transaction, |_| ()) {
            if chunk.attributes.is_some() || !matches!(chunk.insert, Out::Any(Any::String(_))) {
                return Err("only unformatted plain text inserts are supported".to_string());
            }
        }
        let value = text.get_string(&transaction);
        if value.len() > MAX_COLLAB_TEXT_BYTES {
            return Err("the body:main text exceeds the collaboration size limit".to_string());
        }
        if value.chars().any(unsupported_collaboration_char) {
            return Err("control characters, inline objects, and paragraph or section breaks are not supported".to_string());
        }
        Ok(())
    }));
    let result = match result {
        Ok(result) => result,
        Err(_) => Err("Yrs state failed guarded plain-text validation".to_string()),
    };
    result.map_err(|error| if incoming_update { StoreError::InvalidCollaborationUpdate(error) } else { StoreError::InvalidCollaborationState(error) })
}

fn collaboration_text(document: &Doc) -> Result<String, StoreError> {
    let transaction = document.transact();
    let Some((_, Out::YText(text))) = transaction.root_refs().next() else {
        return Err(StoreError::InvalidCollaborationState("the body:main text root is missing".into()));
    };
    Ok(text.get_string(&transaction))
}

fn validate_title(title: &str) -> Result<String, StoreError> {
    let title = title.trim();
    if title.is_empty() {
        return Err(StoreError::InvalidTitle("title cannot be empty".into()));
    }
    if title.chars().count() > MAX_TITLE_CHARS {
        return Err(StoreError::InvalidTitle(format!("title exceeds {MAX_TITLE_CHARS} characters")));
    }
    Ok(title.to_string())
}

fn to_sql_version(version: u64) -> Result<i64, StoreError> {
    i64::try_from(version).map_err(|_| StoreError::VersionRange)
}

fn nonnegative_version(version: i64) -> u64 {
    version.max(0) as u64
}

fn positive_version(version: i64) -> Result<u64, StoreError> {
    u64::try_from(version).ok().filter(|version| *version > 0).ok_or(StoreError::CollaborationEpoch)
}

fn prepare_snapshot(snapshot: Value, title: &str, version: u64) -> Result<Value, StoreError> {
    let mut document = wordcraft_engine::io::document_from_storage_snapshot(&snapshot).map_err(StoreError::InvalidSnapshot)?;
    let now = wordcraft_engine::cmd::now_iso();
    document.core.title = title.to_string();
    if document.core.created.is_empty() {
        document.core.created = now.clone();
    }
    document.core.modified = now;
    document.core.revision = version.min(u64::from(u32::MAX)) as u32;
    let snapshot = wordcraft_engine::io::storage_snapshot(&document).map_err(StoreError::InvalidSnapshot)?;
    Ok(snapshot)
}

fn validate_snapshot(snapshot: &Value) -> Result<(), StoreError> {
    wordcraft_engine::io::document_from_storage_snapshot(snapshot).map(|_| ()).map_err(StoreError::InvalidSnapshot)
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::{Store, StoreError};
    use futures_util::{SinkExt, Stream, StreamExt};
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    use wordcraft_doc::{Document, StoryRef};
    use yrs::types::text::TextRef;
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, GetString, Text, Transact, Update};

    fn test_path(label: &str) -> PathBuf {
        let suffix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos();
        std::env::temp_dir().join(format!("wordcraft-server-{label}-{}-{suffix}.sqlite3", std::process::id()))
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn saves_are_versioned_conflict_safe_and_restorable() {
        let path = test_path("versions");
        {
            let store = Store::open(&path).expect("store opens");
            let original = Document::from_text("original");
            let created =
                store.create("Shared document", wordcraft_engine::io::storage_snapshot(&original).expect("snapshot")).expect("document creates");
            assert_eq!(created.summary.version, 1);

            let changed = Document::from_text("latest saved text");
            let second = store
                .save(&created.summary.id, 1, "Shared document", wordcraft_engine::io::storage_snapshot(&changed).expect("snapshot"))
                .expect("version saves");
            assert_eq!(second.summary.version, 2);

            let stale = store.save(&created.summary.id, 1, "stale client", wordcraft_engine::io::storage_snapshot(&original).expect("snapshot"));
            assert!(matches!(stale, Err(StoreError::Conflict { current_version: 2 })));

            let restored = store.restore_version(&created.summary.id, 2, 1).expect("old version restores as new");
            assert_eq!(restored.summary.version, 3);
            assert_eq!(
                wordcraft_engine::io::document_from_storage_snapshot(&restored.snapshot)
                    .expect("restored document")
                    .plain_text(wordcraft_doc::StoryRef::Body),
                "original"
            );
            assert_eq!(store.versions(&created.summary.id).expect("versions list").len(), 3);
            store.set_deleted(&created.summary.id, 3, true).expect("soft delete");
            assert!(store.list(false).expect("active list").is_empty());
            assert_eq!(store.list(true).expect("trash list").len(), 1);
            assert!(matches!(store.get(&created.summary.id), Err(StoreError::Deleted)));
            store.set_deleted(&created.summary.id, 3, false).expect("restore from trash");
            assert_eq!(store.get(&created.summary.id).expect("document restored").summary.version, 3);
        }
        cleanup(&path);
    }

    #[test]
    fn reopening_store_preserves_embedded_media() {
        let path = test_path("media");
        let record = {
            let store = Store::open(&path).expect("store opens");
            let mut document = Document::from_text("has image bytes");
            document.media.insert("image.png".into(), Arc::new(vec![0, 3, 42, 255]));
            document.passthrough.insert("custom.xml".into(), Arc::new(b"<custom/>".to_vec()));
            store.create("Image document", wordcraft_engine::io::storage_snapshot(&document).expect("snapshot")).expect("document creates")
        };
        let reopened = Store::open(&path).expect("database reopens");
        let stored = reopened.get(&record.summary.id).expect("document survives reopen");
        let document = wordcraft_engine::io::document_from_storage_snapshot(&stored.snapshot).expect("snapshot restores");
        assert_eq!(document.media.get("image.png").map(|data| data.as_slice()), Some([0, 3, 42, 255].as_slice()));
        assert_eq!(document.passthrough.get("custom.xml").map(|data| data.as_slice()), Some(b"<custom/>".as_slice()));
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn document_summary_keeps_live_text_snapshot_lock_after_store_reopen() {
        let path = test_path("collab-summary-lock");
        let id;
        {
            let store = Store::open(&path).expect("store opens");
            let created = store
                .create("One paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("draft")).expect("snapshot"))
                .expect("document creates");
            id = created.summary.id.clone();
            assert!(!created.summary.snapshot_locked);
            assert!(!store.get(&id).expect("document loads").summary.snapshot_locked);

            store.load_or_seed_collaboration(&id).expect("collaboration epoch seeds");
            assert!(store.get(&id).expect("collaborative document loads").summary.snapshot_locked);
            assert!(store.list(false).expect("document list loads")[0].snapshot_locked);
        }

        let reopened = Store::open(&path).expect("store reopens");
        let record = reopened.get(&id).expect("collaboration lock survives restart");
        assert!(record.summary.snapshot_locked);
        let response = serde_json::to_value(record).expect("document record serializes for the API");
        assert_eq!(response.get("snapshotLocked"), Some(&serde_json::json!(true)));
        assert!(reopened.list(false).expect("document list survives restart")[0].snapshot_locked);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn invalid_snapshots_and_titles_are_rejected() {
        let path = test_path("validation");
        let store = Store::open(&path).expect("store opens");
        assert!(store.create("  ", serde_json::Value::Null).is_err());
        assert!(store.create("Title", serde_json::json!({"format":"other","formatVersion":1})).is_err());
        drop(store);
        cleanup(&path);
    }

    fn collaboration_client(seed: &[u8], client_id: u64) -> (Doc, TextRef) {
        let doc = Doc::with_client_id(client_id);
        let text = doc.get_or_insert_text(super::COLLAB_TEXT_ROOT);
        let update = Update::decode_v1(seed).expect("seed update decodes");
        doc.transact_mut().apply_update(update).expect("seed update applies");
        (doc, text)
    }

    fn client_insert(seed: &[u8], client_id: u64, insertion: &str) -> Vec<u8> {
        let (doc, text) = collaboration_client(seed, client_id);
        let mut transaction = doc.transact_mut();
        text.insert(&mut transaction, "前".len() as u32, insertion);
        transaction.encode_update_v1()
    }

    fn snapshot_text(store: &Store, id: &str) -> String {
        let record = store.get(id).expect("document is stored");
        wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("stored snapshot decodes").plain_text(StoryRef::Body)
    }

    async fn next_client_binary<S>(stream: &mut S) -> Vec<u8>
    where
        S: Stream<Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("websocket message arrives before timeout")
            .expect("websocket remains open")
            .expect("websocket message is valid");
        assert!(matches!(message, ClientMessage::Binary(_)), "server message should be binary: {message:?}");
        match message {
            ClientMessage::Binary(bytes) => bytes.to_vec(),
            _ => Vec::new(),
        }
    }

    #[test]
    fn concurrent_clients_converge_and_materialize_without_advancing_snapshot_version() {
        let path = test_path("collab-concurrent");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let created = store
            .create("Collaborative paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("前後")).expect("snapshot"))
            .expect("document creates");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&store, &created.summary.id).expect("collaboration room seeds");
        let alice_update = client_insert(&room.seed_update, 101, "甲👩‍💻");
        let bob_update = client_insert(&room.seed_update, 102, "乙e\u{301}");

        let barrier = Arc::new(Barrier::new(3));
        let alice_room = room.clone();
        let alice_store = store.clone();
        let alice_barrier = barrier.clone();
        let alice = std::thread::spawn(move || {
            alice_barrier.wait();
            alice_room.accept_update(&alice_store, &alice_update)
        });
        let bob_room = room.clone();
        let bob_store = store.clone();
        let bob_barrier = barrier.clone();
        let bob = std::thread::spawn(move || {
            bob_barrier.wait();
            bob_room.accept_update(&bob_store, &bob_update)
        });
        barrier.wait();
        assert!(alice.join().expect("Alice worker joins").expect("Alice update persists"));
        assert!(bob.join().expect("Bob worker joins").expect("Bob update persists"));

        let full_state = room.full_state().expect("room encodes full state");
        let (alice_doc, alice_text) = collaboration_client(&room.seed_update, 103);
        let (bob_doc, bob_text) = collaboration_client(&room.seed_update, 104);
        for client in [&alice_doc, &bob_doc] {
            let update = Update::decode_v1(&full_state).expect("full state decodes");
            client.transact_mut().apply_update(update).expect("full state applies");
        }
        let alice_value = alice_text.get_string(&alice_doc.transact());
        let bob_value = bob_text.get_string(&bob_doc.transact());
        assert_eq!(alice_value, bob_value);
        assert!(alice_value.contains("甲👩‍💻"));
        assert!(alice_value.contains("乙e\u{301}"));

        let record = store.get(&created.summary.id).expect("document reads");
        assert_eq!(record.summary.version, 1, "collaboration epoch does not advance REST versions");
        assert_eq!(snapshot_text(&store, &created.summary.id), alice_value);
        drop(room);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn duplicate_update_is_not_persisted_or_broadcast_twice() {
        let path = test_path("collab-dedup");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Collaborative paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("前後")).expect("snapshot"))
            .expect("document creates");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&store, &created.summary.id).expect("collaboration room seeds");
        let mut receiver = room.updates.subscribe();
        let update = client_insert(&room.seed_update, 201, "甲");

        assert!(room.accept_update(&store, &update).expect("first update persists"));
        assert_eq!(receiver.try_recv().expect("first update broadcasts"), update);
        assert!(!room.accept_update(&store, &update).expect("duplicate is ignored"));
        assert!(matches!(receiver.try_recv(), Err(tokio::sync::broadcast::error::TryRecvError::Empty)));
        assert_eq!(store.load_or_seed_collaboration(&created.summary.id).expect("stored state loads").updates.len(), 1);
        assert_eq!(snapshot_text(&store, &created.summary.id), "前甲後");
        drop(room);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }

    #[tokio::test]
    async fn websocket_replays_duplicate_only_to_reconnecting_sender() {
        let path = test_path("collab-ws-duplicate");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let created = store
            .create("Collaborative paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("前後")).expect("snapshot"))
            .expect("document creates");
        let app = super::api_router(store.clone()).expect("router acquires database lock");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("server listener binds");
        let address = listener.local_addr().expect("server address is available");
        let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("server serves websocket requests") });
        let url = format!("ws://{address}/api/documents/{}/collab", created.summary.id);

        let (mut peer, _) = tokio_tungstenite::connect_async(&url).await.expect("peer connects");
        let _peer_seed = next_client_binary(&mut peer).await;
        let (mut sender, _) = tokio_tungstenite::connect_async(&url).await.expect("sender connects");
        let sender_seed = next_client_binary(&mut sender).await;
        let update = client_insert(&sender_seed, 501, "甲");
        sender.send(ClientMessage::Binary(update.clone().into())).await.expect("sender submits update");
        assert_eq!(next_client_binary(&mut sender).await, update, "unique update is broadcast to sender");
        assert_eq!(next_client_binary(&mut peer).await, update, "unique update is broadcast to peer");

        drop(sender);
        let (mut reconnected, _) = tokio_tungstenite::connect_async(&url).await.expect("sender reconnects");
        let full_state = next_client_binary(&mut reconnected).await;
        let (replayed_client, replayed_text) = collaboration_client(&full_state, 502);
        assert_eq!(replayed_text.get_string(&replayed_client.transact()), "前甲後");

        reconnected.send(ClientMessage::Binary(update.clone().into())).await.expect("reconnected sender retries pending update");
        assert_eq!(next_client_binary(&mut reconnected).await, update, "durable duplicate is echoed to sender");
        assert!(tokio::time::timeout(Duration::from_millis(150), peer.next()).await.is_err(), "a duplicate retry must not be rebroadcast to peers");
        assert_eq!(store.load_or_seed_collaboration(&created.summary.id).expect("durable log loads").updates.len(), 1);

        let _ = reconnected.close(None).await;
        let _ = peer.close(None).await;
        server.abort();
        let _ = server.await;
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn api_router_allows_only_one_live_instance_per_database() {
        let path = test_path("server-process-lock");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let first = super::api_router(store.clone()).expect("first router acquires database lock");
        let second = super::api_router(store.clone());
        assert!(second.is_err(), "second router must not share one SQLite database");
        let error = second.expect_err("second router is rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(error.to_string().contains("another WordCraft server already holds the database lock"));

        drop(first);
        let after_shutdown = super::api_router(store.clone()).expect("lock releases when router is dropped");
        drop(after_shutdown);
        drop(store);
        cleanup(&path);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".wordcraft-server.lock");
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn collaboration_log_replays_after_store_reopen() {
        let path = test_path("collab-reopen");
        let created_id;
        {
            let store = Store::open(&path).expect("store opens");
            let created = store
                .create("Collaborative paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("前後")).expect("snapshot"))
                .expect("document creates");
            created_id = created.summary.id.clone();
            let rooms = super::CollaborationRooms::default();
            let room = rooms.get_or_load(&store, &created_id).expect("collaboration room seeds");
            let update = client_insert(&room.seed_update, 301, "🧑🏽‍🚀");
            assert!(room.accept_update(&store, &update).expect("update persists"));
        }

        let reopened = Store::open(&path).expect("store reopens");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&reopened, &created_id).expect("collaboration room replays");
        let full_state = room.full_state().expect("replayed state encodes");
        let (client, text) = collaboration_client(&room.seed_update, 302);
        client.transact_mut().apply_update(Update::decode_v1(&full_state).expect("replay decodes")).expect("replay applies");
        assert_eq!(text.get_string(&client.transact()), "前🧑🏽‍🚀後");
        assert_eq!(snapshot_text(&reopened, &created_id), "前🧑🏽‍🚀後");
        assert_eq!(reopened.get(&created_id).expect("document reopens").summary.version, 1);
        drop(room);
        drop(rooms);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn unsupported_documents_are_not_seeded_and_collaboration_fences_snapshot_writes() {
        let path = test_path("collab-scope");
        let store = Store::open(&path).expect("store opens");
        let unsupported = store
            .create("Two paragraphs", wordcraft_engine::io::storage_snapshot(&Document::from_text("first\nsecond")).expect("snapshot"))
            .expect("document creates");
        assert!(matches!(store.load_or_seed_collaboration(&unsupported.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let mut line_broken = Document::new();
        line_broken.body = vec![wordcraft_doc::para_block(wordcraft_doc::Paragraph::with_text("first\nsecond", wordcraft_doc::CharProps::default()))];
        let line_broken =
            store.create("Line break", wordcraft_engine::io::storage_snapshot(&line_broken).expect("snapshot")).expect("document creates");
        assert!(matches!(store.load_or_seed_collaboration(&line_broken.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let mut tracked_changes = Document::from_text("tracked");
        tracked_changes.settings.track_changes = true;
        let tracked_changes =
            store.create("Tracked changes", wordcraft_engine::io::storage_snapshot(&tracked_changes).expect("snapshot")).expect("document creates");
        assert!(matches!(store.load_or_seed_collaboration(&tracked_changes.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let supported = store
            .create("One paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("text")).expect("snapshot"))
            .expect("document creates");
        let _room = super::CollaborationRooms::default().get_or_load(&store, &supported.summary.id).expect("collaboration room seeds");
        let stale = store.save(
            &supported.summary.id,
            supported.summary.version,
            "stale snapshot",
            wordcraft_engine::io::storage_snapshot(&Document::from_text("overwritten")).expect("snapshot"),
        );
        assert!(matches!(stale, Err(StoreError::CollaborationActive { current_version: 1 })));
        let restore = store.restore_version(&supported.summary.id, supported.summary.version, supported.summary.version);
        assert!(matches!(restore, Err(StoreError::CollaborationActive { current_version: 1 })));
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn concurrent_first_connections_share_one_persisted_seed() {
        let path = test_path("collab-seed-race");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let created = store
            .create("One paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("seed")).expect("snapshot"))
            .expect("document creates");
        let rooms = Arc::new(super::CollaborationRooms::default());
        let barrier = Arc::new(Barrier::new(3));
        let join_room = |barrier: Arc<Barrier>| {
            let store = store.clone();
            let rooms = rooms.clone();
            let id = created.summary.id.clone();
            std::thread::spawn(move || {
                barrier.wait();
                rooms.get_or_load(&store, &id).expect("room loads")
            })
        };
        let first = join_room(barrier.clone());
        let second = join_room(barrier.clone());
        barrier.wait();
        let first = first.join().expect("first room worker joins");
        let second = second.join().expect("second room worker joins");
        assert!(Arc::ptr_eq(&first, &second));
        let data = store.load_or_seed_collaboration(&created.summary.id).expect("seed loads");
        assert_eq!(data.epoch, 1);
        assert!(data.updates.is_empty());
        assert_eq!(snapshot_text(&store, &created.summary.id), "seed");
        drop(first);
        drop(second);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn malformed_and_line_break_updates_are_rejected_before_storage() {
        let path = test_path("collab-invalid");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("One paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("seed")).expect("snapshot"))
            .expect("document creates");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&store, &created.summary.id).expect("room seeds");
        let line_break = client_insert(&room.seed_update, 401, "\n");
        assert!(matches!(room.accept_update(&store, &line_break), Err(StoreError::InvalidCollaborationUpdate(_))));
        let control = client_insert(&room.seed_update, 402, "\u{0001}");
        assert!(matches!(room.accept_update(&store, &control), Err(StoreError::InvalidCollaborationUpdate(_))));
        let separator = client_insert(&room.seed_update, 403, "\u{2028}");
        assert!(matches!(room.accept_update(&store, &separator), Err(StoreError::InvalidCollaborationUpdate(_))));
        assert!(matches!(room.accept_update(&store, &[0xff]), Err(StoreError::InvalidCollaborationUpdate(_))));
        assert!(store.load_or_seed_collaboration(&created.summary.id).expect("state loads").updates.is_empty());
        assert_eq!(snapshot_text(&store, &created.summary.id), "seed");
        drop(room);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }
}
