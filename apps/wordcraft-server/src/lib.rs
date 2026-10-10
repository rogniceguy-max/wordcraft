//! Local, durable document storage for the WordCraft browser editor.
//!
//! Snapshot writes use compare-and-swap versions. A stale client receives a conflict and cannot
//! silently replace a newer snapshot. The WebSocket prototype syncs a fixed set of body and
//! supported table-cell paragraphs with plain text and selected character styles; it is not a
//! rich-document collaboration adapter.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
use yrs::block::{BLOCK_GC_REF_NUMBER, BLOCK_SKIP_REF_NUMBER, HAS_ORIGIN, HAS_PARENT_SUB, HAS_RIGHT_ORIGIN, ItemContent};
use yrs::encoding::read::Read as _;
use yrs::types::Attrs;
use yrs::updates::decoder::{Decode, Decoder, DecoderV1};
use yrs::{Any, Doc, IdSet, Out, ReadTxn, StateVector, Text, Transact, Update};

const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_TITLE_CHARS: usize = 240;
const MAX_COLLAB_UPDATE_BYTES: usize = 8 * 1024 * 1024;
const MAX_COLLAB_TEXT_BYTES: usize = 8 * 1024 * 1024;
const MAX_COLLAB_PARAGRAPHS: usize = 256;
const MAX_COLLAB_PHYSICAL_CELLS: usize = 256;
const MAX_COLLAB_TABLE_COLUMNS: usize = 63;
const COLLAB_TEXT_ROOT: &str = "body:main";
const COLLAB_ROOM_CAPACITY: usize = 256;
const MAX_PRESENCE_MESSAGE_BYTES: usize = 1024;
const MAX_PRESENCE_PARTICIPANTS: usize = 128;
const MAX_PRESENCE_STICKY_CHARS: usize = 128;
const MAX_PRESENCE_ROSTER_BYTES: usize = 64 * 1024;
const PRESENCE_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const PRESENCE_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    collab_rooms: Arc<CollaborationRooms>,
    presence_rooms: Arc<PresenceRooms>,
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
        .route("/api/documents/{id}/presence", get(presence))
        .route("/api/documents/{id}/versions", get(list_versions))
        .route("/api/documents/{id}/restore-version", post(restore_version))
        .route("/api/documents/{id}/trash", post(trash_document))
        .route("/api/documents/{id}/untrash", post(untrash_document))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(AppState {
            store,
            collab_rooms: Arc::new(CollaborationRooms::default()),
            presence_rooms: Arc::new(PresenceRooms::default()),
            _server_lock: server_lock,
        }))
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

#[derive(Default)]
struct PresenceRooms {
    rooms: Mutex<HashMap<String, Arc<PresenceRoom>>>,
}

struct PresenceRoom {
    participants: Mutex<BTreeMap<String, PresenceParticipant>>,
    updates: broadcast::Sender<String>,
}

struct PresenceParticipant {
    name: String,
    cursor: Option<PresenceCursor>,
    last_heartbeat: Instant,
}

struct PresenceSession {
    id: String,
    room: Arc<PresenceRoom>,
    updates: broadcast::Receiver<String>,
}

#[derive(Serialize)]
struct PresenceRoster {
    kind: &'static str,
    participants: Vec<PresenceParticipantView>,
}

#[derive(Serialize)]
struct PresenceParticipantView {
    id: String,
    name: String,
    cursor: Option<PresenceCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PresenceCursorPosition {
    paragraph: u16,
    sticky: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PresenceCursor {
    anchor: PresenceCursorPosition,
    focus: PresenceCursorPosition,
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum PresenceClientMessage {
    #[serde(rename = "heartbeat")]
    Heartbeat {},
    #[serde(rename = "cursor")]
    Cursor { anchor: PresenceCursorPosition, focus: PresenceCursorPosition },
}

#[derive(Serialize)]
struct PresenceSessionMessage<'a> {
    kind: &'static str,
    id: &'a str,
}

impl PresenceRoom {
    fn new() -> Self {
        let (updates, _) = broadcast::channel(MAX_PRESENCE_PARTICIPANTS * 2);
        PresenceRoom { participants: Mutex::new(BTreeMap::new()), updates }
    }

    fn add_at(&self, now: Instant) -> Result<(String, String, String), String> {
        let mut participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        if participants.len() >= MAX_PRESENCE_PARTICIPANTS {
            return Err("this document already has the maximum number of presence participants".into());
        }
        let name = (1..=MAX_PRESENCE_PARTICIPANTS)
            .map(|number| format!("Guest {number}"))
            .find(|candidate| participants.values().all(|participant| participant.name != *candidate))
            .ok_or_else(|| "this document already has the maximum number of presence participants".to_string())?;
        let id = Uuid::new_v4().to_string();
        participants.insert(id.clone(), PresenceParticipant { name: name.clone(), cursor: None, last_heartbeat: now });
        match presence_roster_json(&participants) {
            Ok(roster) => Ok((id, name, roster)),
            Err(error) => {
                participants.remove(&id);
                Err(error)
            }
        }
    }

    fn heartbeat_at(&self, id: &str, now: Instant) -> Result<bool, String> {
        let mut participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        let Some(participant) = participants.get_mut(id) else { return Ok(false) };
        participant.last_heartbeat = now;
        Ok(true)
    }

    fn set_cursor_at(&self, id: &str, cursor: PresenceCursor, now: Instant) -> Result<Option<String>, String> {
        let mut participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        let Some(participant) = participants.get_mut(id) else { return Ok(None) };
        let previous_cursor = participant.cursor.replace(cursor);
        let previous_heartbeat = participant.last_heartbeat;
        participant.last_heartbeat = now;
        match presence_roster_json(&participants) {
            Ok(roster) => Ok(Some(roster)),
            Err(error) => {
                if let Some(participant) = participants.get_mut(id) {
                    participant.cursor = previous_cursor;
                    participant.last_heartbeat = previous_heartbeat;
                }
                Err(error)
            }
        }
    }

    fn expire_at(&self, now: Instant) -> Result<Option<String>, String> {
        let mut participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        let expired: Vec<String> = participants
            .iter()
            .filter(|(_, participant)| now.saturating_duration_since(participant.last_heartbeat) >= PRESENCE_HEARTBEAT_TIMEOUT)
            .map(|(id, _)| id.clone())
            .collect();
        if expired.is_empty() {
            return Ok(None);
        }
        for id in expired {
            participants.remove(&id);
        }
        presence_roster_json(&participants).map(Some)
    }

    fn remove(&self, id: &str) -> Result<Option<String>, String> {
        let mut participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        if participants.remove(id).is_none() {
            return Ok(None);
        }
        presence_roster_json(&participants).map(Some)
    }

    fn contains(&self, id: &str) -> Result<bool, String> {
        let participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        Ok(participants.contains_key(id))
    }

    fn is_empty(&self) -> Result<bool, String> {
        let participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        Ok(participants.is_empty())
    }

    fn snapshot(&self) -> Result<String, String> {
        let participants = self.participants.lock().map_err(|_| "presence room lock was poisoned".to_string())?;
        presence_roster_json(&participants)
    }
}

impl PresenceRooms {
    fn join(&self, document_id: &str) -> Result<PresenceSession, String> {
        self.join_at(document_id, Instant::now())
    }

    fn join_at(&self, document_id: &str, now: Instant) -> Result<PresenceSession, String> {
        let mut rooms = self.rooms.lock().map_err(|_| "presence room registry lock was poisoned".to_string())?;
        let room = rooms.entry(document_id.to_string()).or_insert_with(|| Arc::new(PresenceRoom::new())).clone();
        let updates = room.updates.subscribe();
        if let Some(roster) = room.expire_at(now)? {
            let _ = room.updates.send(roster);
        }
        match room.add_at(now) {
            Ok((id, _name, roster)) => {
                let _ = room.updates.send(roster);
                Ok(PresenceSession { id, room, updates })
            }
            Err(error) => {
                if room.is_empty()? {
                    rooms.remove(document_id);
                }
                Err(error)
            }
        }
    }

    fn heartbeat_at(&self, session: &PresenceSession, now: Instant) -> Result<bool, String> {
        session.room.heartbeat_at(&session.id, now)
    }

    fn set_cursor_at(&self, session: &PresenceSession, cursor: PresenceCursor, now: Instant) -> Result<bool, String> {
        let Some(roster) = session.room.set_cursor_at(&session.id, cursor, now)? else { return Ok(false) };
        let _ = session.room.updates.send(roster);
        Ok(true)
    }

    fn leave(&self, document_id: &str, session: &PresenceSession) -> Result<(), String> {
        if let Some(roster) = session.room.remove(&session.id)? {
            let _ = session.room.updates.send(roster);
        }
        self.cleanup_if_empty(document_id, &session.room)
    }

    fn cleanup_if_empty(&self, document_id: &str, room: &Arc<PresenceRoom>) -> Result<(), String> {
        let mut rooms = self.rooms.lock().map_err(|_| "presence room registry lock was poisoned".to_string())?;
        let is_current_room = rooms.get(document_id).is_some_and(|current| Arc::ptr_eq(current, room));
        if is_current_room && room.is_empty()? {
            rooms.remove(document_id);
        }
        Ok(())
    }

    #[cfg(test)]
    fn is_empty(&self) -> Result<bool, String> {
        let rooms = self.rooms.lock().map_err(|_| "presence room registry lock was poisoned".to_string())?;
        Ok(rooms.is_empty())
    }
}

fn presence_roster_json(participants: &BTreeMap<String, PresenceParticipant>) -> Result<String, String> {
    let roster = PresenceRoster {
        kind: "roster",
        participants: participants
            .iter()
            .map(|(id, participant)| PresenceParticipantView { id: id.clone(), name: participant.name.clone(), cursor: participant.cursor.clone() })
            .collect(),
    };
    let encoded = serde_json::to_string(&roster).map_err(|error| format!("could not encode presence roster: {error}"))?;
    if encoded.len() > MAX_PRESENCE_ROSTER_BYTES {
        return Err("presence roster exceeds the 64 KiB client message limit".into());
    }
    Ok(encoded)
}

fn presence_session_json(id: &str) -> Result<String, String> {
    serde_json::to_string(&PresenceSessionMessage { kind: "session", id }).map_err(|error| format!("could not encode presence session: {error}"))
}

fn validate_presence_cursor_position(position: &PresenceCursorPosition) -> Result<(), &'static str> {
    if position.paragraph > 255 {
        return Err("cursor paragraph index must be between 0 and 255");
    }
    if !is_standard_base64_sticky(&position.sticky) {
        return Err("cursor sticky positions must be 1 to 128 standard base64 characters");
    }
    Ok(())
}

fn validate_presence_cursor(cursor: PresenceCursor) -> Result<PresenceCursor, &'static str> {
    validate_presence_cursor_position(&cursor.anchor)?;
    validate_presence_cursor_position(&cursor.focus)?;
    Ok(cursor)
}

fn is_standard_base64_sticky(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_PRESENCE_STICKY_CHARS {
        return false;
    }
    let data_len = bytes.iter().position(|byte| *byte == b'=').unwrap_or(bytes.len());
    let padding_len = bytes.len() - data_len;
    if padding_len > 2
        || bytes[..data_len].iter().any(|byte| !byte.is_ascii_alphanumeric() && *byte != b'+' && *byte != b'/')
        || bytes[data_len..].iter().any(|byte| *byte != b'=')
    {
        return false;
    }
    match padding_len {
        0 => data_len % 4 != 1,
        1 => data_len % 4 == 3,
        2 => data_len % 4 == 2,
        _ => false,
    }
}

struct CollaborationRoom {
    id: String,
    epoch: u64,
    paragraph_count: usize,
    roots: Vec<CollaborationRoot>,
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
    paragraph_count: usize,
    roots: Vec<CollaborationRoot>,
    seed_update: Vec<u8>,
    updates: Vec<StoredCollaborationUpdate>,
}

#[derive(Clone, Debug, PartialEq)]
struct CollaborationParagraph {
    text: String,
    runs: Vec<wordcraft_doc::Run>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CollaborationRoot {
    root_name: String,
    path: wordcraft_doc::Path,
}

struct CollaborationSnapshotContent {
    roots: Vec<CollaborationRoot>,
    paragraphs: Vec<CollaborationParagraph>,
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

async fn presence(State(state): State<AppState>, AxumPath(id): AxumPath<String>, ws: WebSocketUpgrade) -> Result<Response, ApiError> {
    let document_id = id.clone();
    in_store(state.store.clone(), move |store| store.get(&document_id).map(|_| ())).await?;
    Ok(ws
        .max_message_size(MAX_PRESENCE_MESSAGE_BYTES)
        .max_frame_size(MAX_PRESENCE_MESSAGE_BYTES)
        .on_upgrade(move |socket| presence_socket(socket, state, id))
        .into_response())
}

async fn presence_socket(mut socket: WebSocket, state: AppState, document_id: String) {
    let mut presence = match state.presence_rooms.join(&document_id) {
        Ok(presence) => presence,
        Err(error) => {
            let _ = socket.send(close_message(close_code::POLICY, &error)).await;
            return;
        }
    };
    let session_message = match presence_session_json(&presence.id) {
        Ok(message) => message,
        Err(error) => {
            eprintln!("WordCraft presence session encoding failed: {error}");
            let _ = state.presence_rooms.leave(&document_id, &presence);
            let _ = socket.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
            return;
        }
    };
    if socket.send(Message::Text(session_message.into())).await.is_err() {
        let _ = state.presence_rooms.leave(&document_id, &presence);
        return;
    }
    let (mut sender, mut incoming) = socket.split();
    let mut sweep = tokio::time::interval_at(tokio::time::Instant::now() + PRESENCE_SWEEP_INTERVAL, PRESENCE_SWEEP_INTERVAL);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            message = incoming.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > MAX_PRESENCE_MESSAGE_BYTES {
                            let _ = sender.send(close_message(close_code::POLICY, "presence messages are limited to 1 KiB")).await;
                            break;
                        }
                        match serde_json::from_str::<PresenceClientMessage>(text.as_str()) {
                            Ok(PresenceClientMessage::Heartbeat {}) => {
                                match state.presence_rooms.heartbeat_at(&presence, Instant::now()) {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        let _ = sender.send(close_message(close_code::POLICY, "presence session expired")).await;
                                        break;
                                    }
                                    Err(error) => {
                                        eprintln!("WordCraft presence heartbeat failed: {error}");
                                        let _ = sender.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
                                        break;
                                    }
                                }
                            }
                            Ok(PresenceClientMessage::Cursor { anchor, focus }) => {
                                let cursor = match validate_presence_cursor(PresenceCursor { anchor, focus }) {
                                    Ok(cursor) => cursor,
                                    Err(error) => {
                                        let _ = sender.send(close_message(close_code::UNSUPPORTED, error)).await;
                                        break;
                                    }
                                };
                                match state.presence_rooms.set_cursor_at(&presence, cursor, Instant::now()) {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        let _ = sender.send(close_message(close_code::POLICY, "presence session expired")).await;
                                        break;
                                    }
                                    Err(error) => {
                                        eprintln!("WordCraft presence cursor update failed: {error}");
                                        let _ = sender.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
                                        break;
                                    }
                                }
                            }
                            Err(_) => {
                                let _ = sender.send(close_message(close_code::UNSUPPORTED, "invalid presence JSON message")).await;
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        let _ = sender.send(close_message(close_code::UNSUPPORTED, "send JSON heartbeat or cursor messages only")).await;
                        break;
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                }
            }
            update = presence.updates.recv() => {
                match update {
                    Ok(roster) => {
                        if sender.send(Message::Text(roster.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        match presence.room.snapshot() {
                            Ok(roster) => {
                                if sender.send(Message::Text(roster.into())).await.is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                eprintln!("WordCraft presence roster failed: {error}");
                                let _ = sender.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = sweep.tick() => {
                match presence.room.expire_at(Instant::now()) {
                    Ok(Some(roster)) => {
                        let _ = presence.room.updates.send(roster);
                        if let Err(error) = state.presence_rooms.cleanup_if_empty(&document_id, &presence.room) {
                            eprintln!("WordCraft presence room cleanup failed: {error}");
                            let _ = sender.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("WordCraft presence timeout sweep failed: {error}");
                        let _ = sender.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
                        break;
                    }
                }
                match presence.room.contains(&presence.id) {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = sender.send(close_message(close_code::POLICY, "presence heartbeat timed out")).await;
                        break;
                    }
                    Err(error) => {
                        eprintln!("WordCraft presence membership check failed: {error}");
                        let _ = sender.send(close_message(close_code::ERROR, "presence state is unavailable")).await;
                        break;
                    }
                }
            }
        }
    }

    if let Err(error) = state.presence_rooms.leave(&document_id, &presence) {
        eprintln!("WordCraft presence participant cleanup failed: {error}");
    }
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
    #[error("real-time collaboration supports one to 256 supported body and table-cell paragraphs: {0}")]
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

        let snapshot = serde_json::from_slice::<Value>(&row.0).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let document = wordcraft_engine::io::document_from_storage_snapshot(&snapshot).map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let seed_content = collaboration_seed_content(&document)?;
        let roots = seed_content.roots.clone();
        let paragraph_count = roots.len();

        let collaboration = transaction
            .query_row("SELECT epoch, seed_update FROM collaboration_documents WHERE document_id = ?1", [id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .optional()?;
        let (epoch, seed_update) = if let Some((epoch, seed_update)) = collaboration {
            (positive_version(epoch)?, seed_update)
        } else {
            let seed_update = make_collaboration_seed(&seed_content)?;
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
        Ok(CollaborationData { epoch, paragraph_count, roots, seed_update, updates })
    }

    fn append_collaboration_update(
        &self,
        id: &str,
        epoch: u64,
        digest: &[u8],
        update: &[u8],
        roots: &[CollaborationRoot],
        paragraphs: &[CollaborationParagraph],
    ) -> Result<bool, StoreError> {
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
        let stored_content = collaboration_seed_content(&document)?;
        if stored_content.roots != roots || roots.len() != paragraphs.len() || stored_content.paragraphs.len() != paragraphs.len() {
            return Err(StoreError::InvalidCollaborationState("stored paragraph structure changed during the collaboration epoch".into()));
        }
        for (root, shared) in roots.iter().zip(paragraphs) {
            let paragraph = document
                .para_mut(wordcraft_doc::StoryRef::Body, &root.path)
                .map_err(|error| StoreError::InvalidCollaborationState(error.to_string()))?;
            paragraph.text.clone_from(&shared.text);
            paragraph.runs.clone_from(&shared.runs);
            paragraph.touch();
        }
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
        let document = rebuild_collaboration_document(&data.seed_update, &data.updates, &data.roots, false)?;
        let digests = data.updates.iter().map(|update| update.digest.clone()).collect();
        let (updates, _) = broadcast::channel(COLLAB_ROOM_CAPACITY);
        Ok(CollaborationRoom {
            id,
            epoch: data.epoch,
            paragraph_count: data.paragraph_count,
            roots: data.roots,
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
        validate_collaboration_update_attributes(bytes).map_err(StoreError::InvalidCollaborationUpdate)?;
        let digest = Sha256::digest(bytes).to_vec();
        let mut state = self.state.lock().map_err(|_| StoreError::CollaborationLock)?;
        if state.digests.contains(&digest) {
            return Ok(false);
        }

        let update = StoredCollaborationUpdate { digest: digest.clone(), bytes: bytes.to_vec() };
        let current_state = state.document.transact().encode_state_as_update_v1(&StateVector::default());
        let candidate = rebuild_collaboration_document(&current_state, std::slice::from_ref(&update), &self.roots, true)?;

        let paragraphs = collaboration_content(&candidate, &self.roots).map_err(StoreError::InvalidCollaborationUpdate)?;
        let inserted = store.append_collaboration_update(&self.id, self.epoch, &digest, bytes, &self.roots, &paragraphs)?;
        if !inserted {
            // This can happen if another server process stored the same payload while this room
            // was running. Refresh from the durable log and avoid a duplicate broadcast.
            let data = store.load_or_seed_collaboration(&self.id)?;
            if data.epoch != self.epoch || data.paragraph_count != self.paragraph_count || data.roots != self.roots {
                return Err(StoreError::CollaborationEpoch);
            }
            let document = rebuild_collaboration_document(&data.seed_update, &data.updates, &data.roots, false)?;
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

fn collaboration_seed_content(document: &wordcraft_doc::Document) -> Result<CollaborationSnapshotContent, StoreError> {
    if document.settings.track_changes || document.settings.protection.is_some() {
        return Err(StoreError::UnsupportedCollaborationDocument("track changes and document protection are not supported".into()));
    }
    if !document.parts.is_empty() || !document.comments.is_empty() || !document.revisions.is_empty() || !document.sources.is_empty() {
        return Err(StoreError::UnsupportedCollaborationDocument("headers, footers, comments, revisions, and sources are not supported".into()));
    }
    let roots = collaboration_root_manifest(document)?;
    let mut text_bytes = 0usize;
    let mut paragraphs = Vec::with_capacity(roots.len());
    for root in &roots {
        let paragraph = document
            .para(wordcraft_doc::StoryRef::Body, &root.path)
            .ok_or_else(|| StoreError::UnsupportedCollaborationDocument(format!("collaboration paragraph path {} is invalid", root.path)))?;
        let Some(next_text_bytes) = text_bytes.checked_add(paragraph.text.len()) else {
            return Err(StoreError::UnsupportedCollaborationDocument("the body text size exceeds the collaboration limit".into()));
        };
        if next_text_bytes > MAX_COLLAB_TEXT_BYTES {
            return Err(StoreError::UnsupportedCollaborationDocument("the body text exceeds the collaboration size limit".into()));
        }
        text_bytes = next_text_bytes;
        let run_len = paragraph.runs.iter().try_fold(0usize, |total, run| total.checked_add(run.len));
        let runs_cover_text = run_len == Some(paragraph.text.len());
        if !paragraph.props.is_empty()
            || !paragraph.mark.is_empty()
            || paragraph.section.is_some()
            || !paragraph.objects.is_empty()
            || paragraph.text.contains('\n')
            || paragraph.text.contains('\r')
            || paragraph.text.chars().any(unsupported_collaboration_char)
            || !runs_cover_text
        {
            return Err(StoreError::UnsupportedCollaborationDocument(
                "paragraph formatting, pending mark formatting, inline objects, and section breaks are not supported".into(),
            ));
        }
        let mut byte_offset = 0usize;
        for run in &paragraph.runs {
            if !collaboration_run_props_are_supported(&run.props) {
                return Err(StoreError::UnsupportedCollaborationDocument(
                    "only direct bold, italic, and single underline character properties are supported".into(),
                ));
            }
            let end = byte_offset
                .checked_add(run.len)
                .ok_or_else(|| StoreError::UnsupportedCollaborationDocument("run offsets exceed the paragraph size".into()))?;
            if paragraph.text.get(byte_offset..end).is_none() {
                return Err(StoreError::UnsupportedCollaborationDocument("run boundaries must align with UTF-8 character boundaries".into()));
            }
            byte_offset = end;
        }
        paragraphs.push(CollaborationParagraph { text: paragraph.text.clone(), runs: paragraph.runs.clone() });
    }
    Ok(CollaborationSnapshotContent { roots, paragraphs })
}

fn collaboration_root_manifest(document: &wordcraft_doc::Document) -> Result<Vec<CollaborationRoot>, StoreError> {
    use wordcraft_doc::props::VMerge;
    use wordcraft_doc::{Block, Path};

    let mut roots = Vec::with_capacity(document.body.len().min(MAX_COLLAB_PARAGRAPHS));
    let mut physical_cells = 0usize;
    for (block_index, block) in document.body.iter().enumerate() {
        match &**block {
            Block::Para(_) => {
                let path = Path::top(block_index);
                let root_name = collaboration_text_root(roots.len());
                push_collaboration_root(&mut roots, root_name, path)?;
            }
            Block::Table(table) => {
                if table.rows.is_empty() || table.rows.len() > MAX_COLLAB_PHYSICAL_CELLS || table.grid.len() > MAX_COLLAB_TABLE_COLUMNS {
                    return Err(StoreError::UnsupportedCollaborationDocument("table dimensions exceed the collaboration limit".into()));
                }
                for (row_index, row) in table.rows.iter().enumerate() {
                    if row.cells.is_empty() || row.cells.len() > MAX_COLLAB_TABLE_COLUMNS {
                        return Err(StoreError::UnsupportedCollaborationDocument("table row dimensions exceed the collaboration limit".into()));
                    }
                    physical_cells = physical_cells
                        .checked_add(row.cells.len())
                        .ok_or_else(|| StoreError::UnsupportedCollaborationDocument("table cell count exceeds the collaboration limit".into()))?;
                    if physical_cells > MAX_COLLAB_PHYSICAL_CELLS {
                        return Err(StoreError::UnsupportedCollaborationDocument("table cell count exceeds the collaboration limit".into()));
                    }
                    for (cell_index, cell) in row.cells.iter().enumerate() {
                        if cell.props.span != 1 || cell.props.vmerge != VMerge::None {
                            return Err(StoreError::UnsupportedCollaborationDocument("merged table cells are not supported".into()));
                        }
                        if cell.blocks.is_empty() {
                            return Err(StoreError::UnsupportedCollaborationDocument("table cells must contain at least one paragraph".into()));
                        }
                        for (paragraph_index, inner_block) in cell.blocks.iter().enumerate() {
                            if inner_block.as_para().is_none() {
                                return Err(StoreError::UnsupportedCollaborationDocument("nested tables are not supported".into()));
                            }
                            let coordinates = [block_index, row_index, cell_index, paragraph_index];
                            let mut path = Vec::with_capacity(coordinates.len());
                            for coordinate in coordinates {
                                path.push(u32::try_from(coordinate).map_err(|_| {
                                    StoreError::UnsupportedCollaborationDocument("table paragraph path exceeds the supported range".into())
                                })?);
                            }
                            let path = Path(path);
                            if path.depth() > 1 {
                                return Err(StoreError::UnsupportedCollaborationDocument("nested tables are not supported".into()));
                            }
                            let root_name = format!("body:cell-paragraph:{path}");
                            push_collaboration_root(&mut roots, root_name, path)?;
                        }
                    }
                }
            }
        }
    }
    if roots.is_empty() {
        return Err(StoreError::UnsupportedCollaborationDocument("the body must contain at least one supported paragraph".into()));
    }
    Ok(roots)
}

fn push_collaboration_root(roots: &mut Vec<CollaborationRoot>, root_name: String, path: wordcraft_doc::Path) -> Result<(), StoreError> {
    if roots.len() >= MAX_COLLAB_PARAGRAPHS {
        return Err(StoreError::UnsupportedCollaborationDocument(format!(
            "the body must contain no more than {MAX_COLLAB_PARAGRAPHS} supported paragraphs"
        )));
    }
    if path.depth() > 1 || roots.iter().any(|root| root.root_name == root_name || root.path == path) {
        return Err(StoreError::UnsupportedCollaborationDocument("collaboration paragraph topology is invalid".into()));
    }
    roots.push(CollaborationRoot { root_name, path });
    Ok(())
}

fn collaboration_run_props_are_supported(props: &wordcraft_doc::CharProps) -> bool {
    let supported_underline =
        props.underline.is_none_or(|underline| matches!(underline, wordcraft_doc::props::Underline::None | wordcraft_doc::props::Underline::Single));
    if !supported_underline {
        return false;
    }
    let mut remaining = props.clone();
    remaining.bold = None;
    remaining.italic = None;
    remaining.underline = None;
    remaining.is_empty()
}

fn collaboration_attrs(props: &wordcraft_doc::CharProps) -> Attrs {
    let mut attributes = Attrs::new();
    if let Some(value) = props.bold {
        attributes.insert("wc.bold".into(), value.into());
    }
    if let Some(value) = props.italic {
        attributes.insert("wc.italic".into(), value.into());
    }
    if let Some(value) = props.underline {
        attributes.insert("wc.underline".into(), (value == wordcraft_doc::props::Underline::Single).into());
    }
    attributes
}

fn make_collaboration_seed(content: &CollaborationSnapshotContent) -> Result<Vec<u8>, StoreError> {
    let document = Doc::new();
    let shared_texts: Vec<_> = content.roots.iter().map(|root| document.get_or_insert_text(root.root_name.as_str())).collect();
    let mut transaction = document.transact_mut();
    for (paragraph, shared_text) in content.paragraphs.iter().zip(shared_texts) {
        let mut byte_offset = 0usize;
        let mut text_index = 0u32;
        for run in &paragraph.runs {
            let end = byte_offset
                .checked_add(run.len)
                .ok_or_else(|| StoreError::UnsupportedCollaborationDocument("run offsets exceed the paragraph size".into()))?;
            let chunk = paragraph
                .text
                .get(byte_offset..end)
                .ok_or_else(|| StoreError::UnsupportedCollaborationDocument("run boundaries must align with UTF-8 character boundaries".into()))?;
            if !chunk.is_empty() {
                shared_text.insert_with_attributes(&mut transaction, text_index, chunk, collaboration_attrs(&run.props));
                let byte_len = u32::try_from(chunk.len())
                    .map_err(|_| StoreError::UnsupportedCollaborationDocument("paragraph exceeds the Yrs text index range".into()))?;
                text_index = text_index
                    .checked_add(byte_len)
                    .ok_or_else(|| StoreError::UnsupportedCollaborationDocument("paragraph exceeds the Yrs text index range".into()))?;
            }
            byte_offset = end;
        }
    }
    drop(transaction);
    let update = document.transact().encode_state_as_update_v1(&StateVector::default());
    Ok(update)
}

fn collaboration_text_root(index: usize) -> String {
    if index == 0 { COLLAB_TEXT_ROOT.to_string() } else { format!("body:paragraph:{index}") }
}

fn unsupported_collaboration_char(character: char) -> bool {
    use wordcraft_doc::para::OBJ;
    character.is_control() || matches!(character, OBJ | '\u{2028}' | '\u{2029}')
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CollaborationUpdateScanEvent {
    GarbageCollectedBlock,
    SkipBlock,
    Origin,
    RootParent,
    NestedParent,
    ParentSub,
    FormatMarker,
}

/// Yrs 0.28 keeps decoded Update blocks private, while TextRef::diff intentionally omits format
/// markers that do not cover visible text. Scan the public v1 decoder framing and delegate payload
/// decoding to Yrs so every Format item is checked, including markers at the current text end.
fn validate_collaboration_update_attributes(bytes: &[u8]) -> Result<(), String> {
    let scan = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scan_collaboration_update_v1(bytes, |_| {})));
    match scan {
        Ok(result) => result,
        Err(_) => Err("Yrs update failed guarded attribute validation".into()),
    }
}

fn scan_collaboration_update_v1<F>(bytes: &[u8], mut event: F) -> Result<(), String>
where
    F: FnMut(CollaborationUpdateScanEvent),
{
    // Run Yrs' own structural decoder before mirroring its block framing below. This keeps the
    // scanner on the exact v1 grammar and rejects malformed payloads before our inspection pass.
    let _ = Update::decode_v1(bytes).map_err(|error| format!("invalid Yrs v1 update: {error}"))?;
    let mut decoder = DecoderV1::from(bytes);
    let client_count = decoder.read_var::<u32>().map_err(|error| error.to_string())?;

    for _ in 0..client_count {
        let block_count = decoder.read_var::<u32>().map_err(|error| error.to_string())?;
        let _client = decoder.read_client().map_err(|error| error.to_string())?;
        let _first_clock = decoder.read_var::<u32>().map_err(|error| error.to_string())?;

        for _ in 0..block_count {
            let info = decoder.read_info().map_err(|error| error.to_string())?;
            if info == BLOCK_SKIP_REF_NUMBER {
                let _skip_len = decoder.read_var::<u32>().map_err(|error| error.to_string())?;
                event(CollaborationUpdateScanEvent::SkipBlock);
                continue;
            }
            if info == BLOCK_GC_REF_NUMBER {
                let _gc_len = decoder.read_len().map_err(|error| error.to_string())?;
                event(CollaborationUpdateScanEvent::GarbageCollectedBlock);
                continue;
            }

            let has_origin = info & (HAS_ORIGIN | HAS_RIGHT_ORIGIN) != 0;
            if info & HAS_ORIGIN != 0 {
                let _origin = decoder.read_left_id().map_err(|error| error.to_string())?;
                event(CollaborationUpdateScanEvent::Origin);
            }
            if info & HAS_RIGHT_ORIGIN != 0 {
                let _right_origin = decoder.read_right_id().map_err(|error| error.to_string())?;
                event(CollaborationUpdateScanEvent::Origin);
            }

            if !has_origin {
                if decoder.read_parent_info().map_err(|error| error.to_string())? {
                    let _root_name = decoder.read_string().map_err(|error| error.to_string())?;
                    event(CollaborationUpdateScanEvent::RootParent);
                } else {
                    let _parent_id = decoder.read_left_id().map_err(|error| error.to_string())?;
                    event(CollaborationUpdateScanEvent::NestedParent);
                }
                if info & HAS_PARENT_SUB != 0 {
                    let _parent_key = decoder.read_string().map_err(|error| error.to_string())?;
                    event(CollaborationUpdateScanEvent::ParentSub);
                }
            }

            if let ItemContent::Format(key, value) = ItemContent::decode(&mut decoder, info).map_err(|error| error.to_string())? {
                event(CollaborationUpdateScanEvent::FormatMarker);
                if !matches!(key.as_ref(), "wc.bold" | "wc.italic" | "wc.underline") {
                    return Err(format!("unsupported collaboration attribute {key}"));
                }
                if !matches!(value.as_ref(), Any::Bool(_) | Any::Null) {
                    return Err(format!("collaboration attribute {key} must be boolean or null"));
                }
            }
        }
    }

    let _delete_set = IdSet::decode(&mut decoder).map_err(|error| error.to_string())?;
    if !decoder.read_to_end().map_err(|error| error.to_string())?.is_empty() {
        return Err("Yrs v1 update contains trailing bytes".into());
    }
    Ok(())
}

fn rebuild_collaboration_document(
    seed_update: &[u8],
    updates: &[StoredCollaborationUpdate],
    roots: &[CollaborationRoot],
    incoming_update: bool,
) -> Result<Doc, StoreError> {
    if roots.is_empty() || roots.len() > MAX_COLLAB_PARAGRAPHS {
        return Err(StoreError::InvalidCollaborationState("collaboration root count is outside the supported range".into()));
    }
    let document = Doc::new();
    for root in roots {
        let _ = document.get_or_insert_text(root.root_name.as_str());
    }
    apply_collaboration_update(&document, seed_update, incoming_update)?;
    for update in updates {
        apply_collaboration_update(&document, &update.bytes, incoming_update)?;
    }
    validate_collaboration_state(&document, roots, incoming_update)?;
    Ok(document)
}

fn apply_collaboration_update(document: &Doc, bytes: &[u8], incoming_update: bool) -> Result<(), StoreError> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if !incoming_update {
            validate_collaboration_update_attributes(bytes)?;
        }
        let update = Update::decode_v1(bytes).map_err(|error| error.to_string())?;
        document.transact_mut().apply_update(update).map_err(|error| error.to_string())
    }));
    let result = match result {
        Ok(result) => result,
        Err(_) => Err("Yrs rejected a malformed update during guarded validation".to_string()),
    };
    result.map_err(|error| if incoming_update { StoreError::InvalidCollaborationUpdate(error) } else { StoreError::InvalidCollaborationState(error) })
}

fn validate_collaboration_state(document: &Doc, roots: &[CollaborationRoot], incoming_update: bool) -> Result<(), StoreError> {
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| collaboration_content(document, roots).map(|_| ()))) {
        Ok(result) => result,
        Err(_) => Err("Yrs state failed guarded collaboration validation".to_string()),
    };
    result.map_err(|error| if incoming_update { StoreError::InvalidCollaborationUpdate(error) } else { StoreError::InvalidCollaborationState(error) })
}

fn collaboration_content(document: &Doc, roots: &[CollaborationRoot]) -> Result<Vec<CollaborationParagraph>, String> {
    if roots.is_empty() || roots.len() > MAX_COLLAB_PARAGRAPHS {
        return Err("collaboration root count is outside the supported range".into());
    }
    let transaction = document.transact();
    let mut contents = (0..roots.len()).map(|_| None).collect::<Vec<Option<CollaborationParagraph>>>();
    let mut total_bytes = 0usize;
    for (name, value) in transaction.root_refs() {
        let index = collaboration_root_index(name, roots)?;
        if contents.get(index).is_none_or(Option::is_some) {
            return Err(format!("collaboration text root {name} is duplicated"));
        }
        let Out::YText(text) = value else {
            return Err(format!("collaboration root {name} must be a text sequence"));
        };
        let mut paragraph_text = String::new();
        let mut runs = Vec::new();
        for chunk in text.diff(&transaction, |_| ()) {
            let props = collaboration_char_props(chunk.attributes.as_deref())?;
            let inserted = match chunk.insert {
                Out::Any(Any::String(inserted)) => inserted,
                _ => return Err("only string inserts with supported character properties are allowed".into()),
            };
            let next_total = total_bytes
                .checked_add(paragraph_text.len())
                .and_then(|size| size.checked_add(inserted.len()))
                .ok_or_else(|| "body text size exceeds the collaboration limit".to_string())?;
            if next_total > MAX_COLLAB_TEXT_BYTES {
                return Err("body text exceeds the collaboration size limit".into());
            }
            paragraph_text.push_str(&inserted);
            if !inserted.is_empty() {
                runs.push(wordcraft_doc::Run { len: inserted.len(), props });
            }
        }
        if paragraph_text.chars().any(unsupported_collaboration_char) {
            return Err("control characters, inline objects, and paragraph or section breaks are not supported".into());
        }
        total_bytes = total_bytes.checked_add(paragraph_text.len()).ok_or_else(|| "body text size exceeds the collaboration limit".to_string())?;
        if total_bytes > MAX_COLLAB_TEXT_BYTES {
            return Err("body text exceeds the collaboration size limit".into());
        }
        contents[index] = Some(CollaborationParagraph { text: paragraph_text, runs });
    }
    let mut paragraphs = Vec::with_capacity(roots.len());
    for (index, content) in contents.into_iter().enumerate() {
        let root_name = roots.get(index).map(|root| root.root_name.as_str()).unwrap_or("unknown");
        let content = content.ok_or_else(|| format!("collaboration text root {root_name} is missing"))?;
        paragraphs.push(content);
    }
    Ok(paragraphs)
}

fn collaboration_root_index(name: &str, roots: &[CollaborationRoot]) -> Result<usize, String> {
    if let Some(index) = roots.iter().position(|root| root.root_name == name) {
        return Ok(index);
    }
    if let Some(index_text) = name.strip_prefix("body:paragraph:")
        && let Ok(index) = index_text.parse::<usize>()
        && index >= roots.len()
    {
        return Err(format!("collaboration text root {name} is out of range"));
    }
    Err(format!("unknown collaboration text root {name}"))
}

fn collaboration_char_props(attributes: Option<&Attrs>) -> Result<wordcraft_doc::CharProps, String> {
    let mut props = wordcraft_doc::CharProps::default();
    let Some(attributes) = attributes else {
        return Ok(props);
    };
    for (key, value) in attributes {
        let Any::Bool(value) = value else {
            return Err(format!("collaboration attribute {key} must have a boolean value"));
        };
        match key.as_ref() {
            "wc.bold" => props.bold = Some(*value),
            "wc.italic" => props.italic = Some(*value),
            "wc.underline" => {
                props.underline = Some(if *value { wordcraft_doc::props::Underline::Single } else { wordcraft_doc::props::Underline::None });
            }
            _ => return Err(format!("unsupported collaboration attribute {key}")),
        }
    }
    Ok(props)
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
    use wordcraft_doc::{
        Block, CharProps, Document, Paragraph, Run, StoryRef, Table, para_block,
        props::{Underline, VMerge},
    };
    use yrs::types::Attrs;
    use yrs::types::text::TextRef;
    use yrs::updates::decoder::Decode;
    use yrs::{Any, ClientID, Doc, GetString, Map, OffsetKind, Options, ReadTxn, StateVector, Text, Transact, Update};

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
        collaboration_client_with_offset(seed, client_id, OffsetKind::Bytes)
    }

    fn collaboration_client_with_offset(seed: &[u8], client_id: u64, offset_kind: OffsetKind) -> (Doc, TextRef) {
        let mut options = Options::with_client_id(ClientID::new(client_id));
        options.offset_kind = offset_kind;
        let doc = Doc::with_options(options);
        let text = doc.get_or_insert_text(super::COLLAB_TEXT_ROOT);
        let update = Update::decode_v1(seed).expect("seed update decodes");
        doc.transact_mut().apply_update(update).expect("seed update applies");
        (doc, text)
    }

    fn collaboration_client_with_paragraphs(seed: &[u8], client_id: u64, paragraph_count: usize) -> (Doc, Vec<TextRef>) {
        let root_names: Vec<_> = (0..paragraph_count).map(super::collaboration_text_root).collect();
        collaboration_client_with_roots(seed, client_id, &root_names)
    }

    fn collaboration_client_with_roots(seed: &[u8], client_id: u64, root_names: &[String]) -> (Doc, Vec<TextRef>) {
        let doc = Doc::with_options(Options::with_client_id(ClientID::new(client_id)));
        let texts: Vec<_> = root_names.iter().map(|name| doc.get_or_insert_text(name.as_str())).collect();
        let update = Update::decode_v1(seed).expect("seed update decodes");
        doc.transact_mut().apply_update(update).expect("seed update applies");
        (doc, texts)
    }

    fn client_insert_in_root_names(
        seed: &[u8],
        client_id: u64,
        root_names: &[String],
        root_index: usize,
        offset: u32,
        insertion: &str,
        attrs: Attrs,
    ) -> Vec<u8> {
        let (doc, texts) = collaboration_client_with_roots(seed, client_id, root_names);
        let text = texts.get(root_index).expect("root exists");
        let mut transaction = doc.transact_mut();
        text.insert_with_attributes(&mut transaction, offset, insertion, attrs);
        transaction.encode_update_v1()
    }

    fn client_insert_in_paragraph(
        seed: &[u8],
        client_id: u64,
        paragraph_count: usize,
        paragraph_index: usize,
        offset: u32,
        insertion: &str,
        attrs: Attrs,
    ) -> Vec<u8> {
        let (doc, texts) = collaboration_client_with_paragraphs(seed, client_id, paragraph_count);
        let text = texts.get(paragraph_index).expect("paragraph root exists");
        let mut transaction = doc.transact_mut();
        text.insert_with_attributes(&mut transaction, offset, insertion, attrs);
        transaction.encode_update_v1()
    }

    fn client_insert_at_root(seed: &[u8], client_id: u64, root_name: &str, insertion: &str) -> Vec<u8> {
        let doc = Doc::with_options(Options::with_client_id(ClientID::new(client_id)));
        let update = Update::decode_v1(seed).expect("seed update decodes");
        doc.transact_mut().apply_update(update).expect("seed update applies");
        let text = doc.get_or_insert_text(root_name);
        let mut transaction = doc.transact_mut();
        text.insert(&mut transaction, 0, insertion);
        transaction.encode_update_v1()
    }

    fn client_insert(seed: &[u8], client_id: u64, insertion: &str) -> Vec<u8> {
        let (doc, text) = collaboration_client(seed, client_id);
        let mut transaction = doc.transact_mut();
        text.insert(&mut transaction, "前".len() as u32, insertion);
        transaction.encode_update_v1()
    }

    fn client_insert_with_attrs(seed: &[u8], client_id: u64, insertion: &str, attrs: Attrs) -> Vec<u8> {
        let (doc, text) = collaboration_client(seed, client_id);
        let mut transaction = doc.transact_mut();
        text.insert_with_attributes(&mut transaction, 0, insertion, attrs);
        transaction.encode_update_v1()
    }

    fn client_format(seed: &[u8], client_id: u64, index: u32, len: u32, attrs: Attrs) -> Vec<u8> {
        let (doc, text) = collaboration_client(seed, client_id);
        let mut transaction = doc.transact_mut();
        text.format(&mut transaction, index, len, attrs);
        transaction.encode_update_v1()
    }

    fn text_state_update(value: &str, client_id: u64) -> Vec<u8> {
        let doc = Doc::with_options(Options::with_client_id(ClientID::new(client_id)));
        let text = doc.get_or_insert_text(super::COLLAB_TEXT_ROOT);
        let mut transaction = doc.transact_mut();
        if !value.is_empty() {
            text.insert(&mut transaction, 0, value);
        }
        transaction.encode_update_v1()
    }

    fn scan_events(bytes: &[u8]) -> Result<Vec<super::CollaborationUpdateScanEvent>, String> {
        let mut events = Vec::new();
        super::scan_collaboration_update_v1(bytes, |event| events.push(event))?;
        Ok(events)
    }

    fn one_paragraph(document: &Document) -> &Paragraph {
        document.body.first().and_then(|block| block.as_para()).expect("document has one body paragraph")
    }

    fn formatted_seed_document() -> Document {
        let mut paragraph = Paragraph::with_text("A😀B", CharProps::default());
        paragraph.runs = vec![
            Run { len: 1, props: CharProps { bold: Some(true), ..CharProps::default() } },
            Run { len: 4, props: CharProps { italic: Some(true), underline: Some(Underline::Single), ..CharProps::default() } },
            Run { len: 1, props: CharProps { bold: Some(false), italic: Some(false), underline: Some(Underline::None), ..CharProps::default() } },
        ];
        let mut document = Document::new();
        document.body = vec![para_block(paragraph)];
        document
    }

    fn multi_paragraph_seed_document() -> Document {
        let mut first = Paragraph::with_text("甲乙", CharProps::default());
        first.runs = vec![Run { len: 3, props: CharProps { bold: Some(true), ..CharProps::default() } }, Run { len: 3, props: CharProps::default() }];
        let mut second = Paragraph::with_text("A😀B", CharProps::default());
        second.runs = vec![
            Run { len: 1, props: CharProps::default() },
            Run { len: 4, props: CharProps { italic: Some(true), ..CharProps::default() } },
            Run { len: 1, props: CharProps::default() },
        ];
        let mut document = Document::new();
        document.body = vec![para_block(first), para_block(second)];
        document
    }

    fn table_cell_seed_document() -> Document {
        let mut table = Table::new(2, 2, 240.0);
        table.props.style = Some("GridTable4AccentBlue".into());
        table.rows[0].props.header = true;
        table.rows[0].cells[0].props.width = Some(120.0);
        table.rows[0].cells[0].blocks = vec![para_block(Paragraph::with_text("left", CharProps::default()))];

        let mut emphasized = Paragraph::with_text("cell", CharProps::default());
        emphasized.runs = vec![Run { len: 4, props: CharProps { bold: Some(true), ..CharProps::default() } }];
        table.rows[0].cells[1].blocks = vec![para_block(emphasized), para_block(Paragraph::with_text("second", CharProps::default()))];
        table.rows[1].cells[0].blocks = vec![para_block(Paragraph::with_text("bottom-left", CharProps::default()))];
        table.rows[1].cells[1].blocks = vec![para_block(Paragraph::with_text("bottom-right", CharProps::default()))];

        let mut document = Document::new();
        document.body = vec![
            para_block(Paragraph::with_text("before", CharProps::default())),
            Arc::new(Block::Table(table)),
            para_block(Paragraph::with_text("after", CharProps::default())),
        ];
        document
    }

    fn snapshot_text(store: &Store, id: &str) -> String {
        let record = store.get(id).expect("document is stored");
        wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("stored snapshot decodes").plain_text(StoryRef::Body)
    }

    #[test]
    fn formatted_document_seed_preserves_supported_run_properties_in_yrs() {
        let path = test_path("collab-formatted-seed");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Formatted paragraph", wordcraft_engine::io::storage_snapshot(&formatted_seed_document()).expect("snapshot"))
            .expect("document creates");
        let room = super::CollaborationRooms::default().get_or_load(&store, &created.summary.id).expect("formatted document seeds");
        let (client, text) = collaboration_client(&room.seed_update, 701);
        let transaction = client.transact();
        let diffs = text.diff(&transaction, |_| ());
        assert_eq!(diffs.len(), 3, "UTF-8 run boundaries must map to the correct UTF-16 Yrs offsets");
        assert_eq!(text.get_string(&transaction), "A😀B");

        let first = diffs.first().expect("first run exists");
        let first_attrs = first.attributes.as_deref().expect("bold run has attributes");
        assert_eq!(first_attrs.len(), 1);
        assert_eq!(first_attrs.get("wc.bold"), Some(&Any::Bool(true)));

        let emoji = diffs.get(1).expect("emoji run exists");
        let emoji_attrs = emoji.attributes.as_deref().expect("italic underline run has attributes");
        assert_eq!(emoji_attrs.len(), 2);
        assert_eq!(emoji_attrs.get("wc.italic"), Some(&Any::Bool(true)));
        assert_eq!(emoji_attrs.get("wc.underline"), Some(&Any::Bool(true)));

        let last = diffs.get(2).expect("last run exists");
        let last_attrs = last.attributes.as_deref().expect("explicit false run has attributes");
        assert_eq!(last_attrs.len(), 3);
        assert_eq!(last_attrs.get("wc.bold"), Some(&Any::Bool(false)));
        assert_eq!(last_attrs.get("wc.italic"), Some(&Any::Bool(false)));
        assert_eq!(last_attrs.get("wc.underline"), Some(&Any::Bool(false)));
        drop(transaction);
        drop(room);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn multi_paragraph_roots_seed_independent_updates_converge_and_preserve_snapshot_runs() {
        let path = test_path("collab-multi-paragraph");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Collaborative paragraphs", wordcraft_engine::io::storage_snapshot(&multi_paragraph_seed_document()).expect("snapshot"))
            .expect("document creates");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&store, &created.summary.id).expect("multi-paragraph collaboration room seeds");
        assert_eq!(room.paragraph_count, 2);
        assert_eq!(room.roots.iter().map(|root| root.root_name.as_str()).collect::<Vec<_>>(), vec!["body:main", "body:paragraph:1"]);

        let (alice, alice_texts) = collaboration_client_with_paragraphs(&room.seed_update, 901, 2);
        let (bob, bob_texts) = collaboration_client_with_paragraphs(&room.seed_update, 902, 2);
        let alice_update = client_insert_in_paragraph(&room.seed_update, 901, 2, 0, 3, "X", Attrs::from([("wc.underline".into(), true.into())]));
        let bob_update = client_insert_in_paragraph(&room.seed_update, 902, 2, 1, 1, "Y", Attrs::from([("wc.underline".into(), true.into())]));

        assert!(room.accept_update(&store, &alice_update).expect("first paragraph update persists"));
        assert!(room.accept_update(&store, &bob_update).expect("second paragraph update persists"));
        let full_state = room.full_state().expect("full multi-paragraph state encodes");
        alice.transact_mut().apply_update(Update::decode_v1(&full_state).expect("full state decodes")).expect("Alice receives merged state");
        bob.transact_mut().apply_update(Update::decode_v1(&full_state).expect("full state decodes")).expect("Bob receives merged state");

        let alice_transaction = alice.transact();
        let alice_values: Vec<_> = alice_texts.iter().map(|text| text.get_string(&alice_transaction)).collect();
        drop(alice_transaction);
        let bob_transaction = bob.transact();
        let bob_values: Vec<_> = bob_texts.iter().map(|text| text.get_string(&bob_transaction)).collect();
        drop(bob_transaction);
        assert_eq!(alice_values, vec!["甲X乙", "AY😀B"]);
        assert_eq!(bob_values, alice_values, "clients converge on both independent roots");

        let record = store.get(&created.summary.id).expect("materialized document loads");
        assert_eq!(record.summary.version, 1, "live paragraph updates do not create REST versions");
        let document = wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("snapshot decodes");
        assert_eq!(document.body.len(), 2, "materialization preserves paragraph count and order");
        assert_eq!(document.core.title, "Collaborative paragraphs", "materialization preserves document metadata");
        let first = document.body.first().and_then(|block| block.as_para()).expect("first paragraph remains in place");
        let second = document.body.get(1).and_then(|block| block.as_para()).expect("second paragraph remains in place");
        assert_eq!(first.text, "甲X乙");
        assert!(first.runs.iter().any(|run| run.props.bold == Some(true)), "first paragraph keeps seeded bold");
        assert!(first.runs.iter().any(|run| run.props.underline == Some(Underline::Single)), "first paragraph stores its new underline run");
        assert_eq!(second.text, "AY😀B");
        assert!(second.runs.iter().any(|run| run.props.italic == Some(true)), "second paragraph keeps seeded italic");
        assert!(second.runs.iter().any(|run| run.props.underline == Some(Underline::Single)), "second paragraph stores its new underline run");
        drop(room);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn table_cell_roots_seed_and_persist_updates_without_changing_topology() {
        let path = test_path("collab-table-cell");
        let store = Store::open(&path).expect("store opens");
        let seed_document = table_cell_seed_document();
        let created =
            store.create("Collaborative table", wordcraft_engine::io::storage_snapshot(&seed_document).expect("snapshot")).expect("document creates");
        let room = super::CollaborationRooms::default().get_or_load(&store, &created.summary.id).expect("table collaboration room seeds");
        assert_eq!(room.paragraph_count, 7);
        let root_names: Vec<_> = room.roots.iter().map(|root| root.root_name.clone()).collect();
        assert_eq!(
            root_names,
            vec![
                "body:main",
                "body:cell-paragraph:1/0/0/0",
                "body:cell-paragraph:1/0/1/0",
                "body:cell-paragraph:1/0/1/1",
                "body:cell-paragraph:1/1/0/0",
                "body:cell-paragraph:1/1/1/0",
                "body:paragraph:6",
            ]
        );

        let (client, texts) = collaboration_client_with_roots(&room.seed_update, 951, &root_names);
        let transaction = client.transact();
        let seeded_texts: Vec<_> = texts.iter().map(|text| text.get_string(&transaction)).collect();
        drop(transaction);
        assert_eq!(seeded_texts, vec!["before", "left", "cell", "second", "bottom-left", "bottom-right", "after"]);

        let update = client_insert_in_root_names(&room.seed_update, 952, &root_names, 3, 6, "!", Attrs::new());
        assert!(room.accept_update(&store, &update).expect("specific cell paragraph update persists"));
        let data = store.load_or_seed_collaboration(&created.summary.id).expect("update log reloads");
        assert_eq!(data.updates.len(), 1, "accepted cell update is durable");

        let record = store.get(&created.summary.id).expect("materialized table document loads");
        assert_eq!(record.summary.version, created.summary.version, "cell text updates do not create a REST version");
        assert!(record.summary.snapshot_locked);
        let before = wordcraft_engine::io::document_from_storage_snapshot(&created.snapshot).expect("initial snapshot decodes");
        let after = wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("materialized snapshot decodes");
        let before_table = before.body.get(1).and_then(|block| block.as_table()).expect("seed has a table");
        let after_table = after.body.get(1).and_then(|block| block.as_table()).expect("materialized document keeps its table");
        assert_eq!(after_table.props, before_table.props);
        assert_eq!(after_table.grid, before_table.grid);
        assert_eq!(after_table.rows.len(), before_table.rows.len());
        for (before_row, after_row) in before_table.rows.iter().zip(&after_table.rows) {
            assert_eq!(after_row.props, before_row.props);
            assert_eq!(after_row.cells.len(), before_row.cells.len());
            for (before_cell, after_cell) in before_row.cells.iter().zip(&after_row.cells) {
                assert_eq!(after_cell.props, before_cell.props);
                assert_eq!(after_cell.blocks.len(), before_cell.blocks.len());
            }
        }
        let materialized_texts: Vec<_> = after
            .para_paths(StoryRef::Body)
            .iter()
            .map(|path| after.para(StoryRef::Body, path).expect("manifest path resolves").text.clone())
            .collect();
        assert_eq!(materialized_texts, vec!["before", "left", "cell", "second!", "bottom-left", "bottom-right", "after"]);
        assert!(
            after
                .para(StoryRef::Body, &room.roots.get(2).expect("styled cell root exists").path)
                .expect("styled cell paragraph resolves")
                .runs
                .iter()
                .any(|run| run.props.bold == Some(true)),
            "unrelated cell formatting is preserved"
        );
        drop(room);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn table_cell_update_log_replays_after_store_reopen() {
        let path = test_path("collab-table-cell-reopen");
        let document_id;
        {
            let store = Store::open(&path).expect("store opens");
            let created = store
                .create("Collaborative table", wordcraft_engine::io::storage_snapshot(&table_cell_seed_document()).expect("snapshot"))
                .expect("document creates");
            document_id = created.summary.id.clone();
            let rooms = super::CollaborationRooms::default();
            let room = rooms.get_or_load(&store, &document_id).expect("table room seeds");
            let root_names: Vec<_> = room.roots.iter().map(|root| root.root_name.clone()).collect();
            let update = client_insert_in_root_names(&room.seed_update, 953, &root_names, 3, 6, "!", Attrs::new());
            assert!(room.accept_update(&store, &update).expect("cell update persists"));
        }

        let reopened = Store::open(&path).expect("store reopens");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&reopened, &document_id).expect("table update log replays");
        assert_eq!(room.paragraph_count, 7, "root manifest is recovered from the materialized snapshot");
        let root_names: Vec<_> = room.roots.iter().map(|root| root.root_name.clone()).collect();
        let (client, texts) = collaboration_client_with_roots(&room.seed_update, 954, &root_names);
        client
            .transact_mut()
            .apply_update(Update::decode_v1(&room.full_state().expect("replayed state encodes")).expect("replayed state decodes"))
            .expect("replayed state applies");
        let transaction = client.transact();
        assert_eq!(texts.get(3).expect("cell paragraph root exists").get_string(&transaction), "second!");
        drop(transaction);
        let data = reopened.load_or_seed_collaboration(&document_id).expect("durable table update reloads");
        assert_eq!(data.updates.len(), 1);
        let materialized = wordcraft_engine::io::document_from_storage_snapshot(&reopened.get(&document_id).expect("snapshot loads").snapshot)
            .expect("snapshot decodes");
        assert_eq!(
            materialized.para(StoryRef::Body, &room.roots.get(3).expect("cell root exists").path).expect("cell path resolves").text,
            "second!"
        );
        drop(room);
        drop(rooms);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn nested_merged_and_oversized_tables_are_rejected_before_epoch_seed() {
        let path = test_path("collab-table-rejections");
        let store = Store::open(&path).expect("store opens");

        let mut nested_table = Table::new(1, 1, 100.0);
        nested_table.rows[0].cells[0].blocks = vec![Arc::new(Block::Table(Table::new(1, 1, 50.0)))];
        let mut nested_document = Document::new();
        nested_document.body = vec![para_block(Paragraph::with_text("before", CharProps::default())), Arc::new(Block::Table(nested_table))];
        let nested = store
            .create("Nested table", wordcraft_engine::io::storage_snapshot(&nested_document).expect("nested snapshot"))
            .expect("nested document creates");
        assert!(matches!(store.load_or_seed_collaboration(&nested.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let mut merged_document = table_cell_seed_document();
        let Some(Block::Table(table)) = merged_document.body.get_mut(1).map(Arc::make_mut) else { panic!("fixture has a table") };
        table.rows[0].cells[0].props.span = 2;
        let merged = store
            .create("Merged table", wordcraft_engine::io::storage_snapshot(&merged_document).expect("merged snapshot"))
            .expect("merged document creates");
        assert!(matches!(store.load_or_seed_collaboration(&merged.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let mut vertical_document = table_cell_seed_document();
        let Some(Block::Table(table)) = vertical_document.body.get_mut(1).map(Arc::make_mut) else { panic!("fixture has a table") };
        table.rows[0].cells[0].props.vmerge = VMerge::Restart;
        let vertical = store
            .create("Vertically merged table", wordcraft_engine::io::storage_snapshot(&vertical_document).expect("vertical merge snapshot"))
            .expect("vertical merge document creates");
        assert!(matches!(store.load_or_seed_collaboration(&vertical.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let mut oversized_document = Document::new();
        oversized_document.body =
            vec![para_block(Paragraph::with_text("before", CharProps::default())), Arc::new(Block::Table(Table::new(5, 63, 630.0)))];
        let oversized = store
            .create("Oversized table", wordcraft_engine::io::storage_snapshot(&oversized_document).expect("oversized snapshot"))
            .expect("oversized document creates");
        assert!(matches!(store.load_or_seed_collaboration(&oversized.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let mut oversized_root_document = Document::new();
        let mut single_cell_table = Table::new(1, 1, 100.0);
        single_cell_table.rows[0].cells[0].blocks = (0..super::MAX_COLLAB_PARAGRAPHS).map(|_| para_block(Paragraph::new())).collect();
        oversized_root_document.body = vec![para_block(Paragraph::new()), Arc::new(Block::Table(single_cell_table))];
        let oversized_roots = store
            .create("Oversized root set", wordcraft_engine::io::storage_snapshot(&oversized_root_document).expect("oversized root snapshot"))
            .expect("oversized root document creates");
        assert!(matches!(store.load_or_seed_collaboration(&oversized_roots.summary.id), Err(StoreError::UnsupportedCollaborationDocument(_))));

        let connection = store.connection().expect("database connection locks");
        for id in [&nested.summary.id, &merged.summary.id, &vertical.summary.id, &oversized.summary.id, &oversized_roots.summary.id] {
            let epoch_count: i64 = connection
                .query_row("SELECT COUNT(*) FROM collaboration_documents WHERE document_id = ?1", [id], |row| row.get(0))
                .expect("collaboration epoch count reads");
            assert_eq!(epoch_count, 0, "unsupported topology is rejected before an epoch is stored");
        }
        drop(connection);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn empty_paragraph_root_preserves_its_fixed_position() {
        let path = test_path("collab-empty-paragraph-root");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create(
                "Paragraphs with a blank line",
                wordcraft_engine::io::storage_snapshot(&Document::from_text("before\n\nafter")).expect("snapshot"),
            )
            .expect("document creates");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&store, &created.summary.id).expect("room seeds all paragraph roots");
        assert_eq!(room.paragraph_count, 3);
        let update = client_insert_in_paragraph(&room.seed_update, 905, 3, 1, 0, "middle", Attrs::new());
        assert!(room.accept_update(&store, &update).expect("empty paragraph root accepts text"));

        let record = store.get(&created.summary.id).expect("materialized snapshot loads");
        let document = wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("snapshot decodes");
        assert_eq!(document.body.len(), 3, "editing an empty paragraph does not shift following paths");
        assert_eq!(document.body.first().and_then(|block| block.as_para()).expect("first paragraph exists").text, "before");
        assert_eq!(document.body.get(1).and_then(|block| block.as_para()).expect("middle paragraph exists").text, "middle");
        assert_eq!(document.body.get(2).and_then(|block| block.as_para()).expect("last paragraph exists").text, "after");
        drop(room);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn multi_paragraph_collaboration_replays_after_store_reopen() {
        let path = test_path("collab-multi-paragraph-reopen");
        let document_id;
        {
            let store = Store::open(&path).expect("store opens");
            let created = store
                .create("Two paragraphs", wordcraft_engine::io::storage_snapshot(&Document::from_text("one\ntwo")).expect("snapshot"))
                .expect("document creates");
            document_id = created.summary.id.clone();
            let rooms = super::CollaborationRooms::default();
            let room = rooms.get_or_load(&store, &document_id).expect("room seeds");
            let first_update = client_insert_in_paragraph(&room.seed_update, 911, 2, 0, 3, "X", Attrs::new());
            let second_update = client_insert_in_paragraph(&room.seed_update, 912, 2, 1, 3, "Y", Attrs::new());
            assert!(room.accept_update(&store, &first_update).expect("first root update persists"));
            assert!(room.accept_update(&store, &second_update).expect("second root update persists"));
            drop(room);
            drop(rooms);
        }

        let reopened = Store::open(&path).expect("store reopens");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&reopened, &document_id).expect("multi-paragraph update log replays");
        assert_eq!(room.paragraph_count, 2, "fixed paragraph count is recovered from the materialized snapshot");
        let (client, texts) = collaboration_client_with_paragraphs(&room.seed_update, 913, room.paragraph_count);
        client
            .transact_mut()
            .apply_update(Update::decode_v1(&room.full_state().expect("replayed state encodes")).expect("replayed state decodes"))
            .expect("replayed state applies");
        let transaction = client.transact();
        assert_eq!(texts.first().expect("first root exists").get_string(&transaction), "oneX");
        assert_eq!(texts.get(1).expect("second root exists").get_string(&transaction), "twoY");
        drop(transaction);
        let record = reopened.get(&document_id).expect("reopened materialized document loads");
        let snapshot = wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("reopened snapshot decodes");
        assert_eq!(snapshot.body.len(), 2);
        assert_eq!(snapshot.body.first().and_then(|block| block.as_para()).expect("first paragraph exists").text, "oneX");
        assert_eq!(snapshot.body.get(1).and_then(|block| block.as_para()).expect("second paragraph exists").text, "twoY");
        drop(room);
        drop(rooms);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn unknown_and_out_of_range_paragraph_roots_are_rejected_before_storage() {
        let path = test_path("collab-multi-paragraph-roots");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Two paragraphs", wordcraft_engine::io::storage_snapshot(&Document::from_text("first\nsecond")).expect("snapshot"))
            .expect("document creates");
        let rooms = super::CollaborationRooms::default();
        let room = rooms.get_or_load(&store, &created.summary.id).expect("room seeds");

        let out_of_range = client_insert_at_root(&room.seed_update, 921, "body:paragraph:2", "X");
        let error = room.accept_update(&store, &out_of_range).expect_err("third paragraph root is rejected");
        assert!(error.to_string().contains("out of range"));
        let unknown = client_insert_at_root(&room.seed_update, 922, "body:unknown", "Y");
        assert!(matches!(room.accept_update(&store, &unknown), Err(StoreError::InvalidCollaborationUpdate(_))));

        assert!(store.load_or_seed_collaboration(&created.summary.id).expect("durable state remains valid").updates.is_empty());
        let record = store.get(&created.summary.id).expect("snapshot remains available");
        let document = wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("snapshot remains valid");
        assert_eq!(document.body.len(), 2);
        assert_eq!(document.body.first().and_then(|block| block.as_para()).expect("first paragraph exists").text, "first");
        assert_eq!(document.body.get(1).and_then(|block| block.as_para()).expect("second paragraph exists").text, "second");
        drop(room);
        drop(rooms);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn remote_format_update_round_trips_into_document_runs() {
        let path = test_path("collab-formatted-update");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("A😀B")).expect("snapshot"))
            .expect("document creates");
        let room = super::CollaborationRooms::default().get_or_load(&store, &created.summary.id).expect("collaboration room seeds");
        let (client, text) = collaboration_client_with_offset(&room.seed_update, 702, OffsetKind::Utf16);
        let mut transaction = client.transact_mut();
        text.format(&mut transaction, 1, 2, Attrs::from([("wc.bold".into(), true.into()), ("wc.underline".into(), true.into())]));
        text.insert_with_attributes(&mut transaction, 3, "!", Attrs::from([("wc.italic".into(), true.into())]));
        let update = transaction.encode_update_v1();
        drop(transaction);

        assert!(room.accept_update(&store, &update).expect("formatted update persists"));
        let record = store.get(&created.summary.id).expect("document snapshot reads");
        let document = wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot).expect("formatted snapshot decodes");
        let paragraph = one_paragraph(&document);
        assert_eq!(paragraph.text, "A😀!B");
        assert_eq!(paragraph.runs.len(), 4);
        assert_eq!(paragraph.runs[0], Run { len: 1, props: CharProps::default() });
        assert_eq!(
            paragraph.runs[1],
            Run { len: 4, props: CharProps { bold: Some(true), underline: Some(Underline::Single), ..CharProps::default() } }
        );
        assert_eq!(paragraph.runs[2], Run { len: 1, props: CharProps { italic: Some(true), ..CharProps::default() } });
        assert_eq!(paragraph.runs[3], Run { len: 1, props: CharProps::default() });
        assert_eq!(record.summary.version, 1, "live formatting does not create REST versions");
        drop(room);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn formatted_collaboration_update_and_snapshot_survive_store_restart() {
        let path = test_path("collab-formatted-reopen");
        let id;
        {
            let store = Store::open(&path).expect("store opens");
            let created = store
                .create("Paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("seed")).expect("snapshot"))
                .expect("document creates");
            id = created.summary.id.clone();
            let room = super::CollaborationRooms::default().get_or_load(&store, &id).expect("room seeds");
            let update = client_insert_with_attrs(
                &room.seed_update,
                703,
                "X",
                Attrs::from([("wc.bold".into(), true.into()), ("wc.underline".into(), true.into())]),
            );
            assert!(room.accept_update(&store, &update).expect("formatted update persists"));
        }

        let reopened = Store::open(&path).expect("database reopens");
        let room = super::CollaborationRooms::default().get_or_load(&reopened, &id).expect("collaboration update log replays");
        let state = room.full_state().expect("formatted state encodes");
        let (client, text) = collaboration_client(&state, 704);
        let transaction = client.transact();
        let diffs = text.diff(&transaction, |_| ());
        assert_eq!(text.get_string(&transaction), "Xseed");
        let attributes = diffs.first().and_then(|diff| diff.attributes.as_deref()).expect("replayed formatted run has attributes");
        assert_eq!(attributes.get("wc.bold"), Some(&Any::Bool(true)));
        assert_eq!(attributes.get("wc.underline"), Some(&Any::Bool(true)));
        drop(transaction);

        let document = wordcraft_engine::io::document_from_storage_snapshot(&reopened.get(&id).expect("snapshot survives restart").snapshot)
            .expect("snapshot decodes");
        let paragraph = one_paragraph(&document);
        assert_eq!(paragraph.text, "Xseed");
        assert_eq!(
            paragraph.runs.first(),
            Some(&Run { len: 1, props: CharProps { bold: Some(true), underline: Some(Underline::Single), ..CharProps::default() } })
        );
        assert_eq!(paragraph.runs.get(1), Some(&Run { len: 4, props: CharProps::default() }));
        drop(room);
        drop(reopened);
        cleanup(&path);
    }

    #[test]
    fn unknown_or_non_boolean_collaboration_attributes_are_rejected() {
        let path = test_path("collab-bad-attributes");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("seed")).expect("snapshot"))
            .expect("document creates");
        let room = super::CollaborationRooms::default().get_or_load(&store, &created.summary.id).expect("room seeds");
        let unknown = client_insert_with_attrs(&room.seed_update, 705, "X", Attrs::from([("wc.font".into(), true.into())]));
        assert!(matches!(room.accept_update(&store, &unknown), Err(StoreError::InvalidCollaborationUpdate(_))));
        let wrong_type = client_insert_with_attrs(&room.seed_update, 706, "Y", Attrs::from([("wc.bold".into(), "yes".into())]));
        assert!(matches!(room.accept_update(&store, &wrong_type), Err(StoreError::InvalidCollaborationUpdate(_))));
        assert!(store.load_or_seed_collaboration(&created.summary.id).expect("collaboration state remains valid").updates.is_empty());
        assert_eq!(snapshot_text(&store, &created.summary.id), "seed");
        drop(room);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn update_attribute_scanner_accepts_plain_supported_and_null_format_updates() {
        let seed = text_state_update("seed", 810);
        let plain = client_insert(&seed, 811, "A");
        assert!(super::validate_collaboration_update_attributes(&plain).is_ok());

        let empty_seed = text_state_update("", 812);
        let styled_insert = client_insert_with_attrs(
            &empty_seed,
            813,
            "X",
            Attrs::from([("wc.bold".into(), true.into()), ("wc.italic".into(), false.into()), ("wc.underline".into(), true.into())]),
        );
        assert!(super::validate_collaboration_update_attributes(&styled_insert).is_ok());

        let clear_bold = client_format(&styled_insert, 814, 0, 1, Attrs::from([("wc.bold".into(), Any::Null)]));
        assert!(super::validate_collaboration_update_attributes(&clear_bold).is_ok());
    }

    #[test]
    fn terminal_unknown_zero_length_format_marker_is_rejected_before_storage() {
        let path = test_path("collab-terminal-unknown-format");
        let store = Store::open(&path).expect("store opens");
        let created = store
            .create("Paragraph", wordcraft_engine::io::storage_snapshot(&Document::from_text("seed")).expect("snapshot"))
            .expect("document creates");
        let room = super::CollaborationRooms::default().get_or_load(&store, &created.summary.id).expect("room seeds");

        let (client, text) = collaboration_client(&room.seed_update, 815);
        let mut transaction = client.transact_mut();
        text.insert_with_attributes(&mut transaction, 4, "X", Attrs::from([("wc.font".into(), "forbidden".into())]));
        let insertion = transaction.encode_update_v1();
        drop(transaction);
        let mut transaction = client.transact_mut();
        text.remove_range(&mut transaction, 4, 1);
        let deletion = transaction.encode_update_v1();
        drop(transaction);
        let merged = yrs::merge_updates_v1([&insertion, &deletion]).expect("merged update decodes");

        let (receiver, receiver_text) = collaboration_client(&room.seed_update, 816);
        receiver.transact_mut().apply_update(Update::decode_v1(&merged).expect("merged update decodes")).expect("merged update applies");
        let transaction = receiver.transact();
        assert_eq!(receiver_text.get_string(&transaction), "seed");
        assert!(
            receiver_text
                .diff(&transaction, |_| ())
                .iter()
                .all(|chunk| { chunk.attributes.as_deref().and_then(|attributes| attributes.get("wc.font")).is_none() })
        );
        drop(transaction);

        assert!(matches!(room.accept_update(&store, &merged), Err(StoreError::InvalidCollaborationUpdate(_))));
        assert!(store.load_or_seed_collaboration(&created.summary.id).expect("state remains valid").updates.is_empty());
        drop(room);
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn update_attribute_scanner_handles_skip_gc_origin_and_parent_framing() {
        let seed = text_state_update("seed", 820);
        let origin_update = client_insert(&seed, 821, "A");
        assert!(scan_events(&origin_update).expect("origin update scans").contains(&super::CollaborationUpdateScanEvent::Origin));

        let empty_seed = text_state_update("", 822);
        let root_parent_update = client_insert_with_attrs(&empty_seed, 823, "X", Attrs::from([("wc.bold".into(), true.into())]));
        assert!(scan_events(&root_parent_update).expect("root parent update scans").contains(&super::CollaborationUpdateScanEvent::RootParent));

        let map_doc = Doc::with_options(Options::with_client_id(ClientID::new(824)));
        let map = map_doc.get_or_insert_map("not-the-collaboration-root");
        let mut transaction = map_doc.transact_mut();
        map.insert(&mut transaction, "entry", "value");
        let nested_map = map.insert(&mut transaction, "child", yrs::MapPrelim::default());
        nested_map.insert(&mut transaction, "nested-entry", "nested-value");
        let map_update = transaction.encode_update_v1();
        let map_events = scan_events(&map_update).expect("map parent fields scan");
        assert!(map_events.contains(&super::CollaborationUpdateScanEvent::RootParent));
        assert!(map_events.contains(&super::CollaborationUpdateScanEvent::NestedParent));
        assert!(map_events.contains(&super::CollaborationUpdateScanEvent::ParentSub));

        let skip_doc = Doc::with_options(Options::with_client_id(ClientID::new(826)));
        let skip_text = skip_doc.get_or_insert_text(super::COLLAB_TEXT_ROOT);
        let mut transaction = skip_doc.transact_mut();
        skip_text.insert(&mut transaction, 0, "A");
        let first_clock = transaction.encode_update_v1();
        drop(transaction);
        let mut transaction = skip_doc.transact_mut();
        skip_text.insert(&mut transaction, 1, "B");
        drop(transaction);
        let mut transaction = skip_doc.transact_mut();
        skip_text.insert(&mut transaction, 2, "C");
        let third_clock = transaction.encode_update_v1();
        let skip_bytes = yrs::merge_updates_v1([&first_clock, &third_clock]).expect("gapped updates merge");
        assert!(scan_events(&skip_bytes).expect("skip block scans").contains(&super::CollaborationUpdateScanEvent::SkipBlock));

        let gc_doc = Doc::with_options(Options { client_id: ClientID::new(825), skip_gc: true, ..Options::default() });
        let gc_map = gc_doc.get_or_insert_map("gc-fixture");
        {
            let mut transaction = gc_doc.transact_mut();
            let gc_text = gc_map.insert(&mut transaction, "text", yrs::types::text::TextPrelim::default());
            gc_text.insert(&mut transaction, 0, "GC");
            gc_map.remove(&mut transaction, "text");
        }
        gc_doc.transact_mut().gc(None);
        let gc_update = gc_doc.transact().encode_state_as_update_v1(&StateVector::default());
        assert!(
            scan_events(&gc_update).expect("garbage-collection block scans").contains(&super::CollaborationUpdateScanEvent::GarbageCollectedBlock)
        );
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

    async fn next_client_text<S>(stream: &mut S) -> String
    where
        S: Stream<Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("websocket message arrives before timeout")
            .expect("websocket remains open")
            .expect("websocket message is valid");
        match message {
            ClientMessage::Text(text) => text.to_string(),
            other => panic!("server message should be JSON text: {other:?}"),
        }
    }

    async fn expect_client_closed<S>(stream: &mut S)
    where
        S: Stream<Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("server closes unsupported presence socket");
        assert!(matches!(message, None | Some(Ok(ClientMessage::Close(_))) | Some(Err(_))), "unexpected message before close: {message:?}");
    }

    async fn start_test_server(store: Arc<Store>) -> (String, tokio::task::JoinHandle<()>) {
        let app = super::api_router(store).expect("router acquires database lock");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("server listener binds");
        let address = listener.local_addr().expect("server address is available");
        let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("server serves requests") });
        (format!("ws://{address}"), server)
    }

    fn cleanup_server_test(path: &PathBuf) {
        cleanup(path);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".wordcraft-server.lock");
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn presence_heartbeat_refreshes_timeout_and_empty_rooms_are_removed() {
        let rooms = super::PresenceRooms::default();
        let start = super::Instant::now();
        let mut session = rooms.join_at("presence-doc", start).expect("first participant joins");
        let initial = session.updates.try_recv().expect("join publishes initial roster");
        let initial: serde_json::Value = serde_json::from_str(&initial).expect("roster is JSON");
        assert_eq!(initial["kind"], "roster");
        assert_eq!(initial["participants"][0]["name"], "Guest 1");
        assert!(uuid::Uuid::parse_str(initial["participants"][0]["id"].as_str().expect("participant id is a string")).is_ok());

        assert!(rooms.heartbeat_at(&session, start + Duration::from_secs(20)).expect("heartbeat refreshes participant"));
        let cursor = super::PresenceCursor {
            anchor: super::PresenceCursorPosition { paragraph: 1, sticky: "AQ==".into() },
            focus: super::PresenceCursorPosition { paragraph: 2, sticky: "Ag==".into() },
        };
        assert!(rooms.set_cursor_at(&session, cursor, start + Duration::from_secs(40)).expect("cursor refreshes participant"));
        let cursor_roster: serde_json::Value =
            serde_json::from_str(&session.updates.try_recv().expect("cursor update broadcasts roster")).expect("cursor roster is JSON");
        assert_eq!(cursor_roster["participants"][0]["cursor"]["anchor"]["paragraph"], 1);
        assert!(session.room.expire_at(start + Duration::from_secs(80)).expect("sweep succeeds").is_none());
        let expired_roster = session
            .room
            .expire_at(start + Duration::from_secs(86))
            .expect("45-second heartbeat timeout is swept")
            .expect("expired participant changes roster");
        let expired_roster: serde_json::Value = serde_json::from_str(&expired_roster).expect("expiry roster is JSON");
        assert!(expired_roster["participants"].as_array().expect("participants is an array").is_empty());
        assert!(!session.room.contains(&session.id).expect("room membership reads"));
        rooms.cleanup_if_empty("presence-doc", &session.room).expect("empty room is removed");
        assert!(rooms.is_empty().expect("presence registry reads"));
        assert!(!rooms.heartbeat_at(&session, start + Duration::from_secs(87)).expect("expired heartbeat is rejected"));
    }

    #[test]
    fn presence_room_caps_participants_at_128_and_cleans_up_on_last_leave() {
        let rooms = super::PresenceRooms::default();
        let start = super::Instant::now();
        let mut sessions = Vec::with_capacity(super::MAX_PRESENCE_PARTICIPANTS);
        for offset in 0..super::MAX_PRESENCE_PARTICIPANTS {
            sessions.push(rooms.join_at("full-room", start + Duration::from_millis(offset as u64)).expect("participant joins within limit"));
        }
        assert!(rooms.join_at("full-room", start + Duration::from_secs(1)).is_err(), "the 129th participant is refused");
        for session in &sessions {
            rooms.leave("full-room", session).expect("participant leave broadcasts and cleans up");
        }
        assert!(rooms.is_empty().expect("empty room is removed from the registry"));
    }

    #[test]
    fn presence_cursor_roster_stays_within_client_message_limit() {
        let sticky = "A".repeat(super::MAX_PRESENCE_STICKY_CHARS);
        let too_long = "A".repeat(super::MAX_PRESENCE_STICKY_CHARS + 1);
        assert!(super::is_standard_base64_sticky(&sticky), "the exact sticky limit is accepted");
        assert!(!super::is_standard_base64_sticky(&too_long), "one character over the sticky limit is rejected");

        let room = super::PresenceRoom::new();
        let now = super::Instant::now();
        for _ in 0..super::MAX_PRESENCE_PARTICIPANTS {
            let (id, _, _) = room.add_at(now).expect("participant joins within room limit");
            let cursor = super::PresenceCursor {
                anchor: super::PresenceCursorPosition { paragraph: 255, sticky: sticky.clone() },
                focus: super::PresenceCursorPosition { paragraph: 255, sticky: sticky.clone() },
            };
            let cursor = super::validate_presence_cursor(cursor).expect("maximum cursor is valid");
            room.set_cursor_at(&id, cursor, now).expect("cursor updates").expect("participant remains in room");
        }
        let roster = room.snapshot().expect("full cursor roster encodes");
        assert!(roster.len() <= super::MAX_PRESENCE_ROSTER_BYTES, "roster is {} bytes", roster.len());
    }

    #[tokio::test]
    async fn presence_websocket_roster_tracks_join_heartbeat_and_leave_without_persistence() {
        let path = test_path("presence-roster");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let created = store
            .create("Presence document", wordcraft_engine::io::storage_snapshot(&Document::from_text("unchanged")).expect("snapshot"))
            .expect("document creates");
        let version = created.summary.version;
        let (base, server) = start_test_server(store.clone()).await;
        let url = format!("{base}/api/documents/{}/presence", created.summary.id);

        let (mut alice, _) = tokio_tungstenite::connect_async(&url).await.expect("Alice joins presence room");
        let alice_session: serde_json::Value = serde_json::from_str(&next_client_text(&mut alice).await).expect("Alice session is JSON");
        assert_eq!(alice_session["kind"], "session");
        let alice_id = alice_session["id"].as_str().expect("server assigns Alice a session id").to_string();
        assert!(uuid::Uuid::parse_str(&alice_id).is_ok());
        let alice_join: serde_json::Value = serde_json::from_str(&next_client_text(&mut alice).await).expect("Alice roster is JSON");
        assert_eq!(alice_join["kind"], "roster");
        let alice_entry = alice_join["participants"].as_array().expect("roster participants array").first().expect("Alice is listed").clone();
        assert_eq!(alice_entry["id"], alice_id, "the targeted session id matches Alice's roster entry");
        assert!(alice_entry["cursor"].is_null());
        assert_eq!(alice_entry["name"], "Guest 1");

        let (mut bob, _) = tokio_tungstenite::connect_async(&url).await.expect("Bob joins presence room");
        let bob_session: serde_json::Value = serde_json::from_str(&next_client_text(&mut bob).await).expect("Bob session is JSON");
        assert_eq!(bob_session["kind"], "session");
        let bob_id = bob_session["id"].as_str().expect("server assigns Bob a session id").to_string();
        assert!(uuid::Uuid::parse_str(&bob_id).is_ok());
        assert_ne!(bob_id, alice_id, "each socket receives its own server-assigned id");
        let bob_join: serde_json::Value = serde_json::from_str(&next_client_text(&mut bob).await).expect("Bob roster is JSON");
        let alice_update: serde_json::Value = serde_json::from_str(&next_client_text(&mut alice).await).expect("Alice receives join roster");
        let bob_entries = bob_join["participants"].as_array().expect("Bob roster participant array");
        assert_eq!(bob_entries.len(), 2);
        assert_eq!(alice_update["participants"], bob_join["participants"], "both sockets receive the same fresh roster");
        assert!(bob_entries.iter().any(|participant| participant["id"] == alice_id));
        let bob_entry = bob_entries.iter().find(|participant| participant["id"] != alice_id).expect("Bob gets a distinct server id");
        assert_ne!(bob_entry["id"], alice_id);
        assert_eq!(bob_entry["id"], bob_id, "Bob's session id matches his roster entry");
        assert_eq!(bob_entry["name"], "Guest 2");

        let cursor = serde_json::json!({
            "anchor": {"paragraph": 2, "sticky": "AQID"},
            "focus": {"paragraph": 5, "sticky": "BAUG"}
        });
        bob.send(ClientMessage::Text(serde_json::json!({"kind": "cursor", "anchor": cursor["anchor"], "focus": cursor["focus"]}).to_string().into()))
            .await
            .expect("Bob cursor update sends");
        let bob_cursor_roster: serde_json::Value = serde_json::from_str(&next_client_text(&mut bob).await).expect("Bob receives cursor roster");
        let alice_cursor_roster: serde_json::Value = serde_json::from_str(&next_client_text(&mut alice).await).expect("Alice receives cursor roster");
        assert_eq!(alice_cursor_roster["participants"], bob_cursor_roster["participants"], "cursor roster fans out consistently");
        let bob_cursor = bob_cursor_roster["participants"]
            .as_array()
            .expect("cursor roster participants array")
            .iter()
            .find(|participant| participant["id"] == bob_id)
            .expect("Bob appears in cursor roster");
        assert_eq!(bob_cursor["cursor"], cursor);
        let alice_cursor = bob_cursor_roster["participants"]
            .as_array()
            .expect("cursor roster participants array")
            .iter()
            .find(|participant| participant["id"] == alice_id)
            .expect("Alice appears in cursor roster");
        assert!(alice_cursor["cursor"].is_null());

        bob.send(ClientMessage::Close(None)).await.expect("Bob closes presence socket");
        let alice_leave: serde_json::Value = serde_json::from_str(&next_client_text(&mut alice).await).expect("Alice receives leave roster");
        assert_eq!(alice_leave["participants"].as_array().expect("leave participants array").len(), 1);
        assert_eq!(alice_leave["participants"][0]["id"], alice_id);

        alice.send(ClientMessage::Text(r#"{"kind":"heartbeat"}"#.into())).await.expect("heartbeat is accepted");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), alice.next()).await.is_err(),
            "heartbeat receives no application-level response and keeps the socket open"
        );

        let stored = store.get(&created.summary.id).expect("document remains available");
        assert_eq!(stored.summary.version, version, "presence does not advance document version");
        assert!(!stored.summary.snapshot_locked, "presence does not seed a collaboration epoch");
        assert_eq!(stored.snapshot, created.snapshot, "presence does not alter the stored document snapshot");
        let (epoch_count, update_count) = {
            let connection = store.connection().expect("database connection locks");
            let epoch_count: i64 = connection
                .query_row("SELECT COUNT(*) FROM collaboration_documents WHERE document_id = ?1", [&created.summary.id], |row| row.get(0))
                .expect("collaboration epoch count reads");
            let update_count: i64 = connection
                .query_row("SELECT COUNT(*) FROM collaboration_updates WHERE document_id = ?1", [&created.summary.id], |row| row.get(0))
                .expect("collaboration update count reads");
            (epoch_count, update_count)
        };
        assert_eq!(epoch_count, 0);
        assert_eq!(update_count, 0);

        alice.send(ClientMessage::Close(None)).await.expect("Alice closes presence socket");
        server.abort();
        let _ = server.await;
        drop(store);
        cleanup_server_test(&path);
    }

    #[tokio::test]
    async fn presence_requires_an_existing_non_deleted_document() {
        let path = test_path("presence-document-check");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let created = store
            .create("Deleted document", wordcraft_engine::io::storage_snapshot(&Document::from_text("text")).expect("snapshot"))
            .expect("document creates");
        store.set_deleted(&created.summary.id, created.summary.version, true).expect("document soft-deletes");
        let (base, server) = start_test_server(store.clone()).await;

        for id in ["missing-document", created.summary.id.as_str()] {
            let url = format!("{base}/api/documents/{id}/presence");
            match tokio_tungstenite::connect_async(&url).await {
                Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
                }
                Err(error) => panic!("missing/deleted document should fail with HTTP 404: {error}"),
                Ok((mut socket, _)) => {
                    let _ = socket.close(None).await;
                    panic!("presence socket opened for missing/deleted document");
                }
            }
        }

        server.abort();
        let _ = server.await;
        drop(store);
        cleanup_server_test(&path);
    }

    #[tokio::test]
    async fn presence_rejects_malformed_unsupported_and_oversized_messages() {
        let path = test_path("presence-invalid-messages");
        let store = Arc::new(Store::open(&path).expect("store opens"));
        let created = store
            .create("Presence document", wordcraft_engine::io::storage_snapshot(&Document::from_text("text")).expect("snapshot"))
            .expect("document creates");
        let (base, server) = start_test_server(store.clone()).await;
        let url = format!("{base}/api/documents/{}/presence", created.summary.id);
        let invalid_messages = [
            ClientMessage::Text("{".into()),
            ClientMessage::Text(r#"{"kind":"cursor"}"#.into()),
            ClientMessage::Text(r#"{"kind":"heartbeat","id":"client-chosen"}"#.into()),
            ClientMessage::Text(r#"{"kind":"cursor","anchor":{"paragraph":256,"sticky":"AQ=="},"focus":{"paragraph":0,"sticky":"Ag=="}}"#.into()),
            ClientMessage::Text(r#"{"kind":"cursor","anchor":{"paragraph":-1,"sticky":"AQ=="},"focus":{"paragraph":0,"sticky":"Ag=="}}"#.into()),
            ClientMessage::Text(r#"{"kind":"cursor","anchor":{"paragraph":0,"sticky":""},"focus":{"paragraph":0,"sticky":"Ag=="}}"#.into()),
            ClientMessage::Text(r#"{"kind":"cursor","anchor":{"paragraph":0,"sticky":"AQ*="},"focus":{"paragraph":0,"sticky":"Ag=="}}"#.into()),
            ClientMessage::Text(r#"{"kind":"cursor","anchor":{"paragraph":0,"sticky":"A=Q="},"focus":{"paragraph":0,"sticky":"Ag=="}}"#.into()),
            ClientMessage::Text(
                r#"{"kind":"cursor","anchor":{"paragraph":0,"sticky":"AQ==","unknown":true},"focus":{"paragraph":0,"sticky":"Ag=="}}"#.into(),
            ),
            ClientMessage::Text(
                r#"{"kind":"cursor","anchor":{"paragraph":0,"sticky":"AQ=="},"focus":{"paragraph":0,"sticky":"Ag=="},"unknown":true}"#.into(),
            ),
            ClientMessage::Text(
                serde_json::json!({
                    "kind": "cursor",
                    "anchor": {"paragraph": 0, "sticky": "A".repeat(super::MAX_PRESENCE_STICKY_CHARS + 1)},
                    "focus": {"paragraph": 0, "sticky": "Ag=="}
                })
                .to_string()
                .into(),
            ),
            ClientMessage::Binary(vec![1, 2, 3].into()),
            ClientMessage::Text("x".repeat(super::MAX_PRESENCE_MESSAGE_BYTES + 1).into()),
        ];
        for invalid in invalid_messages {
            let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.expect("presence socket joins");
            let session: serde_json::Value = serde_json::from_str(&next_client_text(&mut socket).await).expect("session is JSON");
            assert_eq!(session["kind"], "session");
            let roster: serde_json::Value = serde_json::from_str(&next_client_text(&mut socket).await).expect("initial roster is JSON");
            assert_eq!(roster["kind"], "roster");
            socket.send(invalid).await.expect("invalid test frame sends");
            expect_client_closed(&mut socket).await;
        }

        server.abort();
        let _ = server.await;
        drop(store);
        cleanup_server_test(&path);
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
    fn oversized_or_unsupported_documents_are_not_seeded_and_collaboration_fences_snapshot_writes() {
        let path = test_path("collab-scope");
        let store = Store::open(&path).expect("store opens");
        let mut too_many_paragraphs = Document::new();
        too_many_paragraphs.body = (0..=super::MAX_COLLAB_PARAGRAPHS).map(|_| para_block(Paragraph::new())).collect();
        let unsupported = store
            .create("Too many paragraphs", wordcraft_engine::io::storage_snapshot(&too_many_paragraphs).expect("snapshot"))
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
