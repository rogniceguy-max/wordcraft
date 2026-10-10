//! Local, durable document storage for the WordCraft browser editor.
//!
//! Snapshot writes use compare-and-swap versions. A stale client receives a conflict and cannot
//! silently replace a newer snapshot. This service is the persistence foundation; it does not yet
//! claim operation-based real-time collaboration.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_TITLE_CHARS: usize = 240;

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
}

/// Construct the JSON API router. Static file serving is added by the binary when configured.
pub fn api_router(store: Arc<Store>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/documents", get(list_documents).post(create_document))
        .route("/api/documents/{id}", get(read_document).put(save_document))
        .route("/api/documents/{id}/versions", get(list_versions))
        .route("/api/documents/{id}/restore-version", post(restore_version))
        .route("/api/documents/{id}/trash", post(trash_document))
        .route("/api/documents/{id}/untrash", post(untrash_document))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(AppState { store })
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
    Internal,
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound | StoreError::Deleted => ApiError::NotFound,
            StoreError::Conflict { current_version } => ApiError::Conflict(current_version),
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
        if schema_version > 1 {
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
            CREATE INDEX IF NOT EXISTS documents_active_modified ON documents(deleted_at, modified_at DESC);
            PRAGMA user_version = 1;",
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
            "SELECT id, title, version, created_at, modified_at, deleted_at
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
        let summary = DocumentSummary { id, title, version: 1, created_at, modified_at: created_at, deleted: false };
        Ok(DocumentRecord { summary, snapshot })
    }

    fn get(&self, id: &str) -> Result<DocumentRecord, StoreError> {
        let connection = self.connection()?;
        let row = connection
            .query_row(
                "SELECT id, title, version, created_at, modified_at, deleted_at, snapshot
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
            summary: DocumentSummary { id: id.to_string(), title, version: new_version as u64, created_at: current.1, modified_at, deleted: false },
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
            .query_row("SELECT title, version, created_at, modified_at, deleted_at FROM documents WHERE id = ?1", [id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, Option<i64>>(4)?))
            })
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
        })
    }
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
    use std::sync::Arc;

    use super::{Store, StoreError};
    use wordcraft_doc::Document;

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
    fn invalid_snapshots_and_titles_are_rejected() {
        let path = test_path("validation");
        let store = Store::open(&path).expect("store opens");
        assert!(store.create("  ", serde_json::Value::Null).is_err());
        assert!(store.create("Title", serde_json::json!({"format":"other","formatVersion":1})).is_err());
        drop(store);
        cleanup(&path);
    }
}
