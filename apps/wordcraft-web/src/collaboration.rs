//! Narrow fixed-structure live text collaboration for the browser editor.
//!
//! The adapter shares a frozen sequence of plain-text body paragraphs from the main body and
//! one-level table cells. Rich structure remains outside this mode; anonymous presence and cursor
//! sharing use a separate ephemeral socket.
//! The ordinary WordCraft `Session` remains the visible editor model.

use wordcraft_doc::props::{Underline, VMerge};
use wordcraft_doc::{Block, CharProps, Document, Path, Pos, Run, StoryRef};
use wordcraft_engine::{Selection, Session};
#[cfg(any(target_arch = "wasm32", test))]
use yrs::branch::Branch;
use yrs::types::Attrs;
use yrs::types::text::YChange;
use yrs::undo::{Options as UndoOptions, UndoManager};
use yrs::updates::decoder::Decode;
#[cfg(any(target_arch = "wasm32", test))]
use yrs::updates::encoder::Encode;
use yrs::{
    Any, Assoc, Doc, GetString, IndexedSequence, OffsetKind, Options as DocOptions, Out, ReadTxn, StateVector, StickyIndex, Text, TextRef, Transact,
    Update,
};

fn undo_options() -> UndoOptions<()> {
    UndoOptions {
        capture_timeout_millis: 500,
        tracked_origins: std::collections::HashSet::new(),
        capture_transaction: None,
        timestamp: std::sync::Arc::new(undo_timestamp),
        init_undo_stack: Vec::new(),
        init_redo_stack: Vec::new(),
    }
}

#[cfg(target_arch = "wasm32")]
fn undo_timestamp() -> u64 {
    web_sys::window()
        .and_then(|window| window.performance())
        .map(|performance| performance.now().max(0.0) as u64)
        .unwrap_or_else(|| js_sys::Date::now().max(0.0) as u64)
}

#[cfg(not(target_arch = "wasm32"))]
fn undo_timestamp() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

const SHARED_TEXT: &str = "body:main";
const MAX_SHARED_PARAGRAPHS: usize = 256;
const MAX_SHARED_TEXT_BYTES: usize = 8 * 1024 * 1024;
const ATTR_BOLD: &str = "wc.bold";
const ATTR_ITALIC: &str = "wc.italic";
const ATTR_UNDERLINE: &str = "wc.underline";
const RECONNECT_INITIAL_DELAY_MS: u64 = 500;
const RECONNECT_MAX_DELAY_MS: u64 = 30_000;
#[cfg(any(target_arch = "wasm32", test))]
const PRESENCE_HEARTBEAT_INTERVAL_MS: u64 = 15_000;
#[cfg(any(target_arch = "wasm32", test))]
const PRESENCE_MAX_FRAME_BYTES: usize = 64 * 1024;
#[cfg(any(target_arch = "wasm32", test))]
const PRESENCE_MAX_PARTICIPANTS: usize = 128;
#[cfg(any(target_arch = "wasm32", test))]
const PRESENCE_MAX_STICKY_CHARS: usize = 128;
#[cfg(any(target_arch = "wasm32", test))]
const PRESENCE_MAX_CLIENT_FRAME_BYTES: usize = 1024;
#[cfg(any(target_arch = "wasm32", test))]
const PRESENCE_CURSOR_THROTTLE_MS: u64 = 100;
#[cfg(target_arch = "wasm32")]
const CONNECT_TIMEOUT_MS: u64 = 15_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconnectDecision {
    Disabled,
    Wait(u64),
    RetryNow,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ReconnectSchedule {
    enabled: bool,
    attempts: u32,
    retry_at_ms: Option<u64>,
}

impl ReconnectSchedule {
    fn start(&mut self) {
        self.enabled = true;
        self.attempts = 0;
        self.retry_at_ms = None;
    }

    fn ensure_started(&mut self) {
        if !self.enabled {
            self.start();
        }
    }

    fn pause(&mut self) {
        self.enabled = false;
        self.retry_at_ms = None;
    }

    fn connected(&mut self) {
        self.attempts = 0;
        self.retry_at_ms = None;
    }

    fn tick(&mut self, now_ms: u64) -> ReconnectDecision {
        if !self.enabled {
            return ReconnectDecision::Disabled;
        }
        let retry_at_ms = match self.retry_at_ms {
            Some(retry_at_ms) => retry_at_ms,
            None => {
                let delay_ms = Self::delay_ms(self.attempts);
                self.attempts = self.attempts.saturating_add(1);
                let retry_at_ms = now_ms.saturating_add(delay_ms);
                self.retry_at_ms = Some(retry_at_ms);
                retry_at_ms
            }
        };
        if now_ms >= retry_at_ms {
            self.retry_at_ms = None;
            ReconnectDecision::RetryNow
        } else {
            ReconnectDecision::Wait(retry_at_ms - now_ms)
        }
    }

    fn delay_ms(attempt: u32) -> u64 {
        let multiplier = 1_u64 << attempt.min(6);
        RECONNECT_INITIAL_DELAY_MS.saturating_mul(multiplier).min(RECONNECT_MAX_DELAY_MS)
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn is_current_socket_generation(event_generation: u64, current_generation: u64) -> bool {
    event_generation == current_generation
}

#[cfg(any(target_arch = "wasm32", test))]
fn is_current_presence_generation(event_generation: u64, current_generation: u64) -> bool {
    event_generation == current_generation
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PresenceHeartbeatSchedule {
    next_at_ms: Option<u64>,
}

#[cfg(any(target_arch = "wasm32", test))]
impl PresenceHeartbeatSchedule {
    fn start(&mut self, now_ms: u64) {
        self.next_at_ms = Some(now_ms);
    }

    fn pause(&mut self) {
        self.next_at_ms = None;
    }

    /// Returns true once when due. A delayed frame advances from `now`, coalescing missed ticks.
    fn due(&mut self, now_ms: u64) -> bool {
        if !self.next_at_ms.is_some_and(|next_at_ms| now_ms >= next_at_ms) {
            return false;
        }
        self.next_at_ms = Some(now_ms.saturating_add(PRESENCE_HEARTBEAT_INTERVAL_MS));
        true
    }

    fn wait_ms(&self, now_ms: u64) -> Option<u64> {
        self.next_at_ms.map(|next_at_ms| next_at_ms.saturating_sub(now_ms))
    }
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct PresenceAnchor {
    paragraph: usize,
    sticky: StickyIndex,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct PresenceCursor {
    anchor: PresenceAnchor,
    focus: PresenceAnchor,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct PresenceParticipant {
    id: String,
    name: String,
    cursor: Option<PresenceCursor>,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PresenceSessionMessage {
    kind: String,
    id: String,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PresenceRosterMessage {
    kind: String,
    participants: Vec<PresenceParticipantMessage>,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PresenceParticipantMessage {
    id: String,
    name: String,
    cursor: serde_json::Value,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PresenceCursorMessage {
    anchor: PresenceAnchorMessage,
    focus: PresenceAnchorMessage,
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PresenceAnchorMessage {
    paragraph: usize,
    sticky: String,
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_presence_session(message: &str) -> Result<String, String> {
    if message.len() > PRESENCE_MAX_CLIENT_FRAME_BYTES {
        return Err("The presence session handshake exceeded its size limit.".into());
    }
    let session: PresenceSessionMessage = serde_json::from_str(message).map_err(|error| format!("Invalid presence session: {error}"))?;
    if session.kind != "session" || !valid_presence_label(&session.id) {
        return Err("The presence server sent an invalid session identifier.".into());
    }
    Ok(session.id)
}

#[cfg(any(target_arch = "wasm32", test))]
fn presence_message_kind(message: &str) -> Result<String, String> {
    if message.len() > PRESENCE_MAX_FRAME_BYTES {
        return Err("The presence message exceeded its size limit.".into());
    }
    let value: serde_json::Value = serde_json::from_str(message).map_err(|error| format!("Invalid presence message: {error}"))?;
    value.get("kind").and_then(serde_json::Value::as_str).map(str::to_owned).ok_or_else(|| "The presence message has no kind.".into())
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_presence_roster(message: &str) -> Result<Vec<PresenceParticipant>, String> {
    if message.len() > PRESENCE_MAX_FRAME_BYTES {
        return Err("The presence roster exceeded its size limit.".into());
    }
    let roster: PresenceRosterMessage = serde_json::from_str(message).map_err(|error| format!("Invalid presence roster: {error}"))?;
    if roster.kind != "roster" {
        return Err("The presence server sent an unsupported message.".into());
    }
    if roster.participants.len() > PRESENCE_MAX_PARTICIPANTS {
        return Err("The presence roster contains too many participants.".into());
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut participants = Vec::with_capacity(roster.participants.len());
    for participant in roster.participants {
        if !valid_presence_label(&participant.id) || !valid_presence_label(&participant.name) || !ids.insert(participant.id.clone()) {
            return Err("The presence roster contains an invalid participant.".into());
        }
        let cursor = if participant.cursor.is_null() {
            None
        } else {
            let cursor: PresenceCursorMessage =
                serde_json::from_value(participant.cursor).map_err(|error| format!("The presence roster contains an invalid cursor: {error}"))?;
            Some(parse_presence_cursor(cursor)?)
        };
        participants.push(PresenceParticipant { id: participant.id, name: participant.name, cursor });
    }
    Ok(participants)
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_presence_cursor(cursor: PresenceCursorMessage) -> Result<PresenceCursor, String> {
    Ok(PresenceCursor { anchor: parse_presence_anchor(cursor.anchor)?, focus: parse_presence_anchor(cursor.focus)? })
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_presence_anchor(anchor: PresenceAnchorMessage) -> Result<PresenceAnchor, String> {
    if anchor.paragraph >= MAX_SHARED_PARAGRAPHS {
        return Err("The presence cursor points outside the fixed body paragraph range.".into());
    }
    if anchor.sticky.is_empty() || anchor.sticky.len() > PRESENCE_MAX_STICKY_CHARS {
        return Err("The presence cursor sticky position exceeded its size limit.".into());
    }
    let bytes = wordcraft_engine::cmd::insert::base64_decode(&anchor.sticky)
        .filter(|bytes| wordcraft_engine::cmd::insert::base64_encode(bytes) == anchor.sticky)
        .ok_or_else(|| "The presence cursor sticky position is not standard base64.".to_string())?;
    let sticky = StickyIndex::decode_v1(&bytes).map_err(|error| format!("Invalid presence cursor sticky position: {error}"))?;
    if sticky.encode_v1() != bytes {
        return Err("The presence cursor sticky position has trailing data.".into());
    }
    Ok(PresenceAnchor { paragraph: anchor.paragraph, sticky })
}

#[cfg(any(target_arch = "wasm32", test))]
fn valid_presence_label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

#[cfg(any(target_arch = "wasm32", test))]
fn encode_presence_cursor(selection: &RelativeSelection) -> Result<String, String> {
    let sticky = |index: &StickyIndex| wordcraft_engine::cmd::insert::base64_encode(&index.encode_v1());
    let message = serde_json::json!({
        "kind": "cursor",
        "anchor": {"paragraph": selection.anchor_paragraph, "sticky": sticky(&selection.anchor)},
        "focus": {"paragraph": selection.focus_paragraph, "sticky": sticky(&selection.focus)},
    });
    let encoded = serde_json::to_string(&message).map_err(|error| format!("Could not encode presence cursor: {error}"))?;
    if encoded.len() > PRESENCE_MAX_CLIENT_FRAME_BYTES {
        return Err("The presence cursor exceeded its size limit.".into());
    }
    Ok(encoded)
}

#[cfg(any(target_arch = "wasm32", test))]
fn presence_heartbeat_json() -> &'static str {
    r#"{"kind":"heartbeat"}"#
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PresenceCursorSchedule {
    last_sent: Option<String>,
    pending: Option<String>,
    next_at_ms: Option<u64>,
}

#[cfg(any(target_arch = "wasm32", test))]
impl PresenceCursorSchedule {
    fn queue(&mut self, payload: String, now_ms: u64) {
        if self.last_sent.as_deref() == Some(payload.as_str()) {
            self.pending = None;
            return;
        }
        if self.pending.as_deref() == Some(payload.as_str()) {
            return;
        }
        self.pending = Some(payload);
        self.next_at_ms.get_or_insert(now_ms);
    }

    fn due(&mut self, now_ms: u64) -> Option<String> {
        if !self.next_at_ms.is_some_and(|next_at_ms| now_ms >= next_at_ms) {
            return None;
        }
        let payload = self.pending.take()?;
        self.last_sent = Some(payload.clone());
        self.next_at_ms = Some(now_ms.saturating_add(PRESENCE_CURSOR_THROTTLE_MS));
        Some(payload)
    }

    fn wait_ms(&self, now_ms: u64) -> Option<u64> {
        self.pending.as_ref()?;
        self.next_at_ms.map(|next_at_ms| next_at_ms.saturating_sub(now_ms))
    }

    fn reset_for_reconnect(&mut self) {
        self.last_sent = None;
        self.pending = None;
        self.next_at_ms = None;
    }

    fn clear(&mut self) {
        self.reset_for_reconnect();
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn presence_color(id: &str) -> egui::Color32 {
    const PALETTE: [[u8; 3]; 8] = [
        [0x1c, 0x7e, 0xd6],
        [0xe6, 0x77, 0x00],
        [0x70, 0x48, 0xe8],
        [0x2f, 0x9e, 0x44],
        [0xd6, 0x33, 0x6c],
        [0x0c, 0x85, 0x99],
        [0xb0, 0x89, 0x00],
        [0xc9, 0x2a, 0x2a],
    ];
    let hash = id.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3));
    let rgb = PALETTE[(hash as usize) % PALETTE.len()];
    egui::Color32::from_rgb(rgb[0], rgb[1], rgb[2])
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Clone, Debug, PartialEq, Eq)]
enum PresenceSocketEvent {
    Open { generation: u64 },
    Message { generation: u64, text: String },
    BadMessage { generation: u64 },
    Closed { generation: u64 },
    Error { generation: u64 },
}

#[cfg(any(target_arch = "wasm32", test))]
impl PresenceSocketEvent {
    fn generation(&self) -> u64 {
        match self {
            PresenceSocketEvent::Open { generation }
            | PresenceSocketEvent::Message { generation, .. }
            | PresenceSocketEvent::BadMessage { generation }
            | PresenceSocketEvent::Closed { generation }
            | PresenceSocketEvent::Error { generation } => *generation,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct SharedParagraph {
    path: Path,
    text: String,
    runs: Vec<Run>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BodyBlockStructure {
    Paragraph,
    Table { grid_columns: usize, rows: Vec<Vec<usize>> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BodyStructureSignature(Vec<BodyBlockStructure>);

/// Validate fixed body topology and compute a compact signature before allocating paragraph paths.
fn body_structure_signature(document: &Document) -> Option<BodyStructureSignature> {
    if document.body.is_empty() {
        return None;
    }
    let mut root_count = 0usize;
    let mut cell_count = 0usize;
    for block in &document.body {
        match &**block {
            Block::Para(_) => {
                root_count = root_count.checked_add(1)?;
            }
            Block::Table(table) => {
                if table.rows.is_empty() || table.grid.len() > wordcraft_doc::table::MAX_COLS {
                    return None;
                }
                for row in &table.rows {
                    if row.cells.is_empty() || row.cells.len() > wordcraft_doc::table::MAX_COLS {
                        return None;
                    }
                    for cell in &row.cells {
                        cell_count = cell_count.checked_add(1)?;
                        if cell_count > MAX_SHARED_PARAGRAPHS || cell.props.span != 1 || cell.props.vmerge != VMerge::None || cell.blocks.is_empty() {
                            return None;
                        }
                        for cell_block in &cell.blocks {
                            match &**cell_block {
                                Block::Para(_) => root_count = root_count.checked_add(1)?,
                                Block::Table(_) => return None,
                            }
                            if root_count > MAX_SHARED_PARAGRAPHS {
                                return None;
                            }
                        }
                    }
                }
            }
        }
        if root_count > MAX_SHARED_PARAGRAPHS {
            return None;
        }
    }
    if root_count == 0 {
        return None;
    }

    let mut signature = Vec::with_capacity(document.body.len());
    for block in &document.body {
        match &**block {
            Block::Para(_) => signature.push(BodyBlockStructure::Paragraph),
            Block::Table(table) => {
                let rows = table.rows.iter().map(|row| row.cells.iter().map(|cell| cell.blocks.len()).collect()).collect();
                signature.push(BodyBlockStructure::Table { grid_columns: table.grid.len(), rows });
            }
        }
    }
    Some(BodyStructureSignature(signature))
}

/// Flatten supported top-level and one-level table-cell paragraphs in document order.
fn supported_body(document: &Document) -> Option<Vec<SharedParagraph>> {
    if !document.parts.is_empty()
        || !document.comments.is_empty()
        || !document.revisions.is_empty()
        || !document.sources.is_empty()
        || document.settings.track_changes
        || document.settings.protection.is_some()
    {
        return None;
    }
    body_structure_signature(document)?;
    let paths = document.para_paths(StoryRef::Body);
    if paths.is_empty() || paths.len() > MAX_SHARED_PARAGRAPHS || paths.iter().any(|path| path.depth() > 1) {
        return None;
    }
    let mut total_bytes = 0usize;
    let mut paragraphs = Vec::with_capacity(paths.len());
    for path in paths {
        let paragraph = document.para(StoryRef::Body, &path)?;
        let run_len = paragraph.runs.iter().try_fold(0usize, |total, run| total.checked_add(run.len));
        if paragraph.props != wordcraft_doc::props::ParaProps::default()
            || paragraph.mark != CharProps::default()
            || paragraph.section.is_some()
            || !paragraph.objects.is_empty()
            || paragraph.runs.iter().any(|run| !supported_char_props(&run.props))
            || run_len != Some(paragraph.text.len())
            || paragraph.text.chars().any(unsupported_text_char)
        {
            return None;
        }
        total_bytes = total_bytes.checked_add(paragraph.text.len())?;
        if total_bytes > MAX_SHARED_TEXT_BYTES {
            return None;
        }
        paragraphs.push(SharedParagraph { path, text: paragraph.text.clone(), runs: paragraph.runs.clone() });
    }
    Some(paragraphs)
}

#[cfg(test)]
fn supported_body_text(document: &Document) -> Option<Vec<String>> {
    supported_body(document).map(|paragraphs| paragraphs.into_iter().map(|paragraph| paragraph.text).collect())
}

fn supported_char_props(props: &CharProps) -> bool {
    let supported_underline = props.underline.is_none_or(|underline| matches!(underline, Underline::None | Underline::Single));
    let allowed = CharProps { bold: props.bold, italic: props.italic, underline: props.underline, ..CharProps::default() };
    supported_underline && *props == allowed
}

fn clear_paragraph_mark(document: &mut Document, path: &Path) -> Result<bool, String> {
    let paragraph = document.para_mut(StoryRef::Body, path).map_err(|_| scope_error())?;
    let changed = paragraph.mark != CharProps::default();
    if changed {
        paragraph.mark = CharProps::default();
        paragraph.touch();
    }
    Ok(changed)
}

fn props_for_range(document: &Document, path: &Path, start: usize, end: usize) -> Result<CharProps, String> {
    if start >= end {
        return Err("A live text insertion must contain at least one character.".into());
    }
    let Some(paragraph) = document.para(StoryRef::Body, path) else { return Err(scope_error()) };
    let mut selected_props = None;
    let mut covered = 0usize;
    for (range, props) in paragraph.run_ranges() {
        let overlap_start = range.start.max(start);
        let overlap_end = range.end.min(end);
        if overlap_start >= overlap_end {
            continue;
        }
        if selected_props.as_ref().is_some_and(|selected| selected != props) {
            return Err("A live text insertion crossed multiple character styles.".into());
        }
        selected_props = Some(props.clone());
        covered = covered.checked_add(overlap_end - overlap_start).ok_or_else(scope_error)?;
    }
    if covered != end - start {
        return Err("The inserted text could not be mapped to one supported character style.".into());
    }
    selected_props.ok_or_else(scope_error)
}

fn attrs_from_props(props: &CharProps) -> Attrs {
    let mut attributes = Attrs::new();
    if let Some(value) = props.bold {
        attributes.insert(std::sync::Arc::from(ATTR_BOLD), Any::Bool(value));
    }
    if let Some(value) = props.italic {
        attributes.insert(std::sync::Arc::from(ATTR_ITALIC), Any::Bool(value));
    }
    if let Some(value) = props.underline {
        attributes.insert(std::sync::Arc::from(ATTR_UNDERLINE), Any::Bool(value == Underline::Single));
    }
    attributes
}

fn props_from_attrs(attributes: Option<&Attrs>) -> Result<CharProps, String> {
    let mut props = CharProps::default();
    if let Some(attributes) = attributes {
        for (key, value) in attributes {
            let Any::Bool(value) = value else {
                return Err(format!("live text attribute `{key}` must be a boolean"));
            };
            match key.as_ref() {
                ATTR_BOLD => props.bold = Some(*value),
                ATTR_ITALIC => props.italic = Some(*value),
                ATTR_UNDERLINE => props.underline = Some(if *value { Underline::Single } else { Underline::None }),
                _ => return Err(format!("live text attribute `{key}` is not supported")),
            }
        }
    }
    Ok(props)
}

#[cfg(any(target_arch = "wasm32", test))]
fn selection_format_value(document: &Document, path: &Path, id: &str, start: usize, end: usize) -> Result<bool, String> {
    let Some(paragraph) = document.para(StoryRef::Body, path) else { return Err(scope_error()) };
    let mut selected_value = None;
    let mut selected_any = false;
    for (range, props) in paragraph.run_ranges() {
        if range.start >= end || range.end <= start {
            continue;
        }
        selected_any = true;
        let value = match id {
            "format.bold" => props.bold.ok_or_else(|| "The selected characters have no live bold value.".to_string())?,
            "format.italic" => props.italic.ok_or_else(|| "The selected characters have no live italic value.".to_string())?,
            "format.underline" => match props.underline {
                Some(Underline::Single) => true,
                Some(Underline::None) => false,
                Some(_) => return Err("Only single underline is supported in live text.".into()),
                None => return Err("The selected characters have no live underline value.".into()),
            },
            _ => return Err("Unsupported live formatting command.".into()),
        };
        if selected_value.is_some_and(|previous| previous != value) {
            return Err("The formatting command did not produce one value across the selection.".into());
        }
        selected_value = Some(value);
    }
    if !selected_any {
        return Err("Live formatting requires selected text.".into());
    }
    selected_value.ok_or_else(|| "Live formatting did not apply to the selection.".into())
}

fn unsupported_text_char(ch: char) -> bool {
    ch.is_control() || ch == '\u{FFFC}' || ch == '\u{2028}' || ch == '\u{2029}'
}

#[derive(Clone)]
struct RelativeSelection {
    anchor_paragraph: usize,
    focus_paragraph: usize,
    anchor: StickyIndex,
    focus: StickyIndex,
    collapsed: bool,
}

/// Yrs state and actor-scoped undo for the fixed set of body text roots.
struct LiveText {
    doc: Doc,
    texts: Vec<TextRef>,
    paths: Vec<Path>,
    structure_signature: BodyStructureSignature,
    undo: Option<UndoManager<()>>,
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
struct FullStateResult {
    catch_up: Option<Vec<u8>>,
    local_state_acknowledged: bool,
}

fn shared_text_root(paragraph_index: usize, path: &Path) -> String {
    if path.depth() > 0 {
        format!("body:cell-paragraph:{path}")
    } else if paragraph_index == 0 {
        SHARED_TEXT.to_string()
    } else {
        format!("body:paragraph:{paragraph_index}")
    }
}

fn paragraph_from_shared_text<T: ReadTxn>(text: &TextRef, path: &Path, transaction: &T) -> Result<SharedParagraph, String> {
    let mut value = String::new();
    let mut runs: Vec<Run> = Vec::new();
    for chunk in text.diff(transaction, YChange::identity) {
        let Out::Any(Any::String(chunk_text)) = chunk.insert else {
            return Err("Live text supports plain text only; embedded objects are blocked.".into());
        };
        let chunk_text = chunk_text.as_ref();
        if chunk_text.chars().any(unsupported_text_char) {
            return Err(scope_error());
        }
        let props = props_from_attrs(chunk.attributes.as_deref())?;
        if !chunk_text.is_empty() {
            if let Some(last) = runs.last_mut().filter(|last| last.props == props) {
                last.len = last.len.saturating_add(chunk_text.len());
            } else {
                runs.push(Run { len: chunk_text.len(), props });
            }
            value.push_str(chunk_text);
        }
    }
    Ok(SharedParagraph { path: path.clone(), text: value, runs })
}

impl LiveText {
    fn with_document(document: &Document) -> Result<Self, String> {
        let paragraphs = supported_body(document).ok_or_else(scope_error)?;
        let paths = paragraphs.into_iter().map(|paragraph| paragraph.path).collect();
        let structure_signature = body_structure_signature(document).ok_or_else(scope_error)?;
        Self::with_manifest(paths, structure_signature)
    }

    #[cfg(test)]
    fn with_paragraph_count(paragraph_count: usize) -> Result<Self, String> {
        let paths: Vec<Path> = (0..paragraph_count).map(Path::top).collect();
        let signature = BodyStructureSignature(vec![BodyBlockStructure::Paragraph; paragraph_count]);
        Self::with_manifest(paths, signature)
    }

    fn with_manifest(paths: Vec<Path>, structure_signature: BodyStructureSignature) -> Result<Self, String> {
        let root_count = structure_signature.0.iter().try_fold(0usize, |count, block| {
            let block_count = match block {
                BodyBlockStructure::Paragraph => 1,
                BodyBlockStructure::Table { rows, .. } => {
                    rows.iter().flatten().try_fold(0usize, |sum, cell_paragraphs| sum.checked_add(*cell_paragraphs))?
                }
            };
            count.checked_add(block_count)
        });
        if !(1..=MAX_SHARED_PARAGRAPHS).contains(&paths.len()) || root_count != Some(paths.len()) || paths.iter().any(|path| path.depth() > 1) {
            return Err(scope_error());
        }
        // Yrs Bytes mode makes StickyIndex mix UTF-8 byte lengths with internal item clocks
        // for multibyte text. UTF-16 mode keeps relative positions consistent; edit offsets
        // are converted at the adapter boundary from WordCraft's UTF-8 byte positions.
        let doc = Doc::with_options(DocOptions { offset_kind: OffsetKind::Utf16, ..DocOptions::default() });
        let texts = paths.iter().enumerate().map(|(index, path)| doc.get_or_insert_text(shared_text_root(index, path))).collect();
        Ok(LiveText { doc, texts, paths, structure_signature, undo: None })
    }

    fn paragraph_text(&self, paragraph_index: usize) -> Result<String, String> {
        let text = self.texts.get(paragraph_index).ok_or_else(scope_error)?;
        Ok(text.get_string(&self.doc.transact()))
    }

    fn body_texts(&self) -> Result<Vec<String>, String> {
        Ok(self.shared_paragraphs()?.into_iter().map(|paragraph| paragraph.text).collect())
    }

    fn shared_paragraphs(&self) -> Result<Vec<SharedParagraph>, String> {
        let transaction = self.doc.transact();
        self.texts.iter().zip(&self.paths).map(|(text, path)| paragraph_from_shared_text(text, path, &transaction)).collect()
    }

    #[cfg(any(target_arch = "wasm32", test))]
    fn resolve_presence_cursor(&self, cursor: &PresenceCursor) -> Option<(Pos, Pos)> {
        let transaction = self.doc.transact();
        let anchor = resolve_presence_anchor(&self.texts, &self.paths, &cursor.anchor, &transaction)?;
        let focus = resolve_presence_anchor(&self.texts, &self.paths, &cursor.focus, &transaction)?;
        Some((anchor, focus))
    }

    fn editor_matches_shared(&self, document: &Document) -> Result<bool, String> {
        let Some(editor_paragraphs) = supported_body(document) else { return Ok(false) };
        Ok(self.matches_manifest(document) && editor_paragraphs == self.shared_paragraphs()?)
    }

    fn matches_manifest(&self, document: &Document) -> bool {
        if body_structure_signature(document).as_ref() != Some(&self.structure_signature) {
            return false;
        }
        document.para_paths(StoryRef::Body) == self.paths
    }

    fn state_vector(&self) -> StateVector {
        self.doc.transact().state_vector()
    }

    /// Apply the server's initial full-state update. A nonempty local document is never seeded
    /// into an empty server CRDT here: doing so can duplicate text when two clients join together.
    fn apply_full_state(&mut self, session: &mut Session, bytes: &[u8]) -> Result<FullStateResult, String> {
        let server_update = Update::decode_v1(bytes).map_err(|error| format!("invalid live text update: {error}"))?;
        let server_doc = Doc::new();
        let server_texts: Vec<TextRef> =
            self.paths.iter().enumerate().map(|(index, path)| server_doc.get_or_insert_text(shared_text_root(index, path))).collect();
        server_doc.transact_mut().apply_update(server_update).map_err(|error| format!("could not read live text state: {error}"))?;
        let server_paragraphs = {
            let transaction = server_doc.transact();
            server_texts
                .iter()
                .zip(&self.paths)
                .map(|(text, path)| paragraph_from_shared_text(text, path, &transaction))
                .collect::<Result<Vec<_>, _>>()?
        };
        validate_shared_paragraphs(&server_paragraphs)?;

        let editor_paragraphs = supported_body(&session.doc).ok_or_else(scope_error)?;
        if self.body_texts()?.iter().all(String::is_empty)
            && editor_paragraphs.iter().any(|paragraph| !paragraph.text.is_empty())
            && server_paragraphs.iter().all(|paragraph| paragraph.text.is_empty())
        {
            return Err(
                "The server has no seeded live text for this document. Your editor content was kept; save or resolve the document before joining."
                    .into(),
            );
        }
        let old_anchors = if self.editor_matches_shared(&session.doc)? { self.capture_selection(session).ok() } else { None };

        let local_update = Update::decode_v1(bytes).map_err(|error| format!("invalid live text update: {error}"))?;
        self.doc.transact_mut().apply_update(local_update).map_err(|error| format!("could not apply live text state: {error}"))?;
        let merged_paragraphs = self.shared_paragraphs()?;
        validate_shared_paragraphs(&merged_paragraphs)?;
        if merged_paragraphs != editor_paragraphs || !self.editor_matches_shared(&session.doc)? {
            self.project_body(session, &merged_paragraphs, old_anchors, session.sel.clone())?;
        }
        if self.undo.is_none() {
            self.initialize_undo();
        }

        let server_state = server_doc.transact().state_vector();
        let local_state = self.state_vector();
        let local_state_acknowledged = local_state.get(&self.doc.client_id()) <= server_state.get(&self.doc.client_id());
        let catch_up = if local_state == server_state { None } else { Some(self.doc.transact().encode_state_as_update_v1(&server_state)) };
        Ok(FullStateResult { catch_up, local_state_acknowledged })
    }

    fn apply_remote_update(&mut self, session: &mut Session, bytes: &[u8]) -> Result<bool, String> {
        let update = Update::decode_v1(bytes).map_err(|error| format!("invalid live text update: {error}"))?;
        let before = supported_body(&session.doc).ok_or_else(scope_error)?;
        if !self.editor_matches_shared(&session.doc)? {
            return Err("The editor and live text are out of sync. Editing was paused to protect the document.".into());
        }
        let anchors = self.capture_selection(session)?;
        self.doc.transact_mut().apply_update(update).map_err(|error| format!("could not apply live text update: {error}"))?;
        let after = self.shared_paragraphs()?;
        validate_shared_paragraphs(&after)?;
        if after == before && self.editor_matches_shared(&session.doc)? {
            return Ok(false);
        }
        self.project_body(session, &after, Some(anchors), session.sel.clone())?;
        Ok(true)
    }

    /// Diff one paragraph's completed editor command into one Yrs delete/insert transaction.
    fn apply_local_editor_change(
        &mut self,
        session: &mut Session,
        before: &[SharedParagraph],
        paragraph_index: usize,
    ) -> Result<Option<Vec<u8>>, String> {
        if !self.matches_manifest(&session.doc) {
            return Err("The body structure changed during live collaboration; the attempted edit was reverted.".into());
        }
        let after = supported_body(&session.doc).ok_or_else(scope_error)?;
        if before.len() != self.texts.len() || after.len() != before.len() || paragraph_index >= before.len() {
            return Err("Live text paragraph structure is fixed while collaboration is active.".into());
        }
        if self.shared_paragraphs()? != before {
            return Err("The editor is catching up with live text. The attempted edit was reverted; retry when the live session is ready.".into());
        }
        if before.iter().zip(&after).enumerate().any(|(index, (old, new))| index != paragraph_index && old != new) {
            return Err("Live text edits must stay inside one body paragraph; the attempted structural edit was reverted.".into());
        }
        let before_paragraph = before.get(paragraph_index).ok_or_else(scope_error)?;
        let after_paragraph = after.get(paragraph_index).ok_or_else(scope_error)?;
        let Some((start, old_end, inserted)) = one_splice(&before_paragraph.text, &after_paragraph.text) else {
            if before_paragraph != after_paragraph {
                return Err("A live text command changed character formatting unexpectedly; the edit was reverted.".into());
            }
            session.reset_history();
            return Ok(None);
        };
        let inserted_end = start.checked_add(inserted.len()).ok_or_else(scope_error)?;
        if !before_paragraph.text.is_char_boundary(start)
            || !before_paragraph.text.is_char_boundary(old_end)
            || !after_paragraph.text.is_char_boundary(start)
            || !after_paragraph.text.is_char_boundary(inserted_end)
        {
            return Err("The text edit did not fall on a UTF-8 character boundary.".into());
        }
        let insert_attributes = if inserted.is_empty() {
            None
        } else {
            Some(attrs_from_props(&props_for_range(&session.doc, &before_paragraph.path, start, inserted_end)?))
        };
        let start_utf16 = byte_to_utf16_offset(&before_paragraph.text, start)?;
        let old_end_utf16 = byte_to_utf16_offset(&before_paragraph.text, old_end)?;
        let remove_len = old_end_utf16.saturating_sub(start_utf16);
        let prior_state = self.state_vector();
        let text = self.texts.get(paragraph_index).ok_or_else(scope_error)?;
        {
            let mut transaction = self.doc.transact_mut_with(self.doc.client_id());
            if remove_len > 0 {
                text.remove_range(&mut transaction, start_utf16, remove_len);
            }
            if let Some(attributes) = insert_attributes {
                text.insert_with_attributes(&mut transaction, start_utf16, inserted, attributes);
            }
        }
        let shared_after = self.shared_paragraphs()?;
        validate_shared_paragraphs(&shared_after)?;
        session.reset_history();
        if shared_after != after || !self.editor_matches_shared(&session.doc)? {
            self.project_body(session, &shared_after, None, session.sel.clone())?;
        }
        if shared_after == before { Ok(None) } else { Ok(Some(self.doc.transact().encode_state_as_update_v1(&prior_state))) }
    }

    fn apply_format_update(
        &mut self,
        paragraph_index: usize,
        id: &str,
        start_byte: usize,
        end_byte: usize,
        value: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        let current = self.paragraph_text(paragraph_index)?;
        if start_byte >= end_byte {
            return Err("Live formatting requires a non-empty selection.".into());
        }
        let start = byte_to_utf16_offset(&current, start_byte)?;
        let end = byte_to_utf16_offset(&current, end_byte)?;
        let attribute = match id {
            "format.bold" => ATTR_BOLD,
            "format.italic" => ATTR_ITALIC,
            "format.underline" => ATTR_UNDERLINE,
            _ => return Err("Unsupported live formatting command.".into()),
        };
        let prior_state = self.state_vector();
        let text = self.texts.get(paragraph_index).ok_or_else(scope_error)?;
        let mut attributes = Attrs::new();
        attributes.insert(std::sync::Arc::from(attribute), Any::Bool(value));
        {
            let mut transaction = self.doc.transact_mut_with(self.doc.client_id());
            text.format(&mut transaction, start, end - start, attributes);
        }
        if self.paragraph_text(paragraph_index)? != current {
            return Err("A live formatting update changed the paragraph text.".into());
        }
        if self.state_vector() == prior_state { Ok(None) } else { Ok(Some(self.doc.transact().encode_state_as_update_v1(&prior_state))) }
    }

    fn undo_or_redo(&mut self, session: &mut Session, redo: bool) -> Result<Option<Vec<u8>>, String> {
        let current = supported_body(&session.doc).ok_or_else(scope_error)?;
        if current != self.shared_paragraphs()? || !self.editor_matches_shared(&session.doc)? {
            return Err("The editor is catching up with live text. Undo was paused to protect the document.".into());
        }
        let anchors = self.capture_selection(session)?;
        let prior_shared = self.shared_paragraphs()?;
        let prior_state = self.state_vector();
        let undo = self.undo.as_mut().ok_or_else(|| "Live undo is not ready yet.".to_string())?;
        let changed = if redo { undo.redo_blocking() } else { undo.undo_blocking() };
        if !changed {
            return Ok(None);
        }
        let updated = self.shared_paragraphs()?;
        validate_shared_paragraphs(&updated)?;
        if updated != current || !self.editor_matches_shared(&session.doc)? {
            self.project_body(session, &updated, Some(anchors), session.sel.clone())?;
        }
        session.reset_history();
        if self.shared_paragraphs()? == prior_shared { Ok(None) } else { Ok(Some(self.doc.transact().encode_state_as_update_v1(&prior_state))) }
    }

    #[cfg(target_arch = "wasm32")]
    fn can_undo(&self) -> bool {
        self.undo.as_ref().is_some_and(UndoManager::can_undo)
    }

    #[cfg(target_arch = "wasm32")]
    fn can_redo(&self) -> bool {
        self.undo.as_ref().is_some_and(UndoManager::can_redo)
    }

    fn initialize_undo(&mut self) {
        let mut undo = UndoManager::<()>::with_options(undo_options());
        for text in &self.texts {
            undo.expand_scope(&self.doc, text);
        }
        undo.include_origin(self.doc.client_id());
        self.undo = Some(undo);
    }

    fn capture_selection(&self, session: &Session) -> Result<RelativeSelection, String> {
        if !self.matches_manifest(&session.doc) || supported_body(&session.doc).as_deref() != Some(self.shared_paragraphs()?.as_slice()) {
            return Err("The editor and live text are out of sync.".into());
        }
        let selection = &session.sel;
        let anchor_paragraph = selection_paragraph_index(&selection.anchor, &self.paths)?;
        let focus_paragraph = selection_paragraph_index(&selection.focus, &self.paths)?;
        let paragraphs = self.shared_paragraphs()?;
        let anchor = checked_offset(&paragraphs[anchor_paragraph].text, selection.anchor.off)?;
        let focus = checked_offset(&paragraphs[focus_paragraph].text, selection.focus.off)?;
        let transaction = self.doc.transact();
        let anchor_assoc = if selection.is_collapsed() {
            Assoc::After
        } else if selection.anchor <= selection.focus {
            Assoc::Before
        } else {
            Assoc::After
        };
        let focus_assoc = if selection.is_collapsed() || selection.anchor <= selection.focus { Assoc::After } else { Assoc::Before };
        let anchor_text = self.texts.get(anchor_paragraph).ok_or_else(scope_error)?;
        let focus_text = self.texts.get(focus_paragraph).ok_or_else(scope_error)?;
        let anchor = sticky_index(anchor_text, &transaction, anchor, anchor_assoc)?;
        let focus = if selection.is_collapsed() { anchor.clone() } else { sticky_index(focus_text, &transaction, focus, focus_assoc)? };
        Ok(RelativeSelection { anchor_paragraph, focus_paragraph, anchor, focus, collapsed: selection.is_collapsed() })
    }

    fn project_body(
        &self,
        session: &mut Session,
        paragraphs: &[SharedParagraph],
        anchors: Option<RelativeSelection>,
        fallback: Selection,
    ) -> Result<(), String> {
        validate_shared_paragraphs(paragraphs)?;
        let Some(editor_paragraphs) = supported_body(&session.doc) else { return Err(scope_error()) };
        if paragraphs.len() != self.texts.len()
            || paragraphs.iter().zip(&self.paths).any(|(paragraph, path)| &paragraph.path != path)
            || editor_paragraphs.iter().map(|paragraph| paragraph.path.clone()).collect::<Vec<_>>() != self.paths
            || body_structure_signature(&session.doc).as_ref() != Some(&self.structure_signature)
        {
            return Err(scope_error());
        }
        let resolved_selection = if let Some(anchors) = anchors {
            let transaction = self.doc.transact();
            let anchor_text = paragraphs.get(anchors.anchor_paragraph).ok_or_else(scope_error)?;
            let focus_text = paragraphs.get(anchors.focus_paragraph).ok_or_else(scope_error)?;
            let anchor = resolve_sticky(&anchors.anchor, &transaction, &anchor_text.text)?;
            let focus = if anchors.collapsed { anchor } else { resolve_sticky(&anchors.focus, &transaction, &focus_text.text)? };
            let anchor_path = self.paths.get(anchors.anchor_paragraph).ok_or_else(scope_error)?;
            let focus_path = self.paths.get(anchors.focus_paragraph).ok_or_else(scope_error)?;
            Some(Selection {
                anchor: Pos::new(StoryRef::Body, anchor_path.clone(), anchor),
                focus: Pos::new(StoryRef::Body, focus_path.clone(), focus),
            })
        } else {
            None
        };
        if self.shared_paragraphs()? != paragraphs {
            return Err("The live text body and formatting are out of sync. Editing was paused to protect the document.".into());
        }
        for (paragraph_index, shared) in paragraphs.iter().enumerate() {
            let path = self.paths.get(paragraph_index).ok_or_else(scope_error)?;
            let unchanged = session
                .doc
                .para(StoryRef::Body, path)
                .is_some_and(|paragraph| paragraph.text == shared.text && paragraph.runs == shared.runs && paragraph.mark == CharProps::default());
            if unchanged {
                continue;
            }
            let paragraph = session.doc.para_mut(StoryRef::Body, path).map_err(|error| error.to_string())?;
            let len = paragraph.len();
            paragraph.delete(0, len).map_err(|error| error.to_string())?;
            let mut offset = 0usize;
            for run in &shared.runs {
                let end = offset.checked_add(run.len).ok_or_else(scope_error)?;
                let chunk = shared.text.get(offset..end).ok_or_else(scope_error)?;
                if !chunk.is_empty() {
                    paragraph.insert_text(offset, chunk, &run.props).map_err(|error| error.to_string())?;
                }
                offset = end;
            }
            if offset != shared.text.len() {
                return Err("The live text run lengths do not match a body paragraph.".into());
            }
            paragraph.mark = CharProps::default();
            paragraph.touch();
        }
        if let Some(selection) = resolved_selection {
            session.sel = selection;
        } else {
            session.sel = Selection {
                anchor: clamp_position(paragraphs, &self.paths, &fallback.anchor)?,
                focus: clamp_position(paragraphs, &self.paths, &fallback.focus)?,
            };
        }
        session.clamp_selection();
        session.reset_history();
        session.touch();
        session.relayout();
        Ok(())
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn resolve_presence_anchor<T: ReadTxn>(texts: &[TextRef], paths: &[Path], anchor: &PresenceAnchor, transaction: &T) -> Option<Pos> {
    let text = texts.get(anchor.paragraph)?;
    let path = paths.get(anchor.paragraph)?;
    let offset = anchor.sticky.get_offset(transaction)?;
    let resolved_branch: &Branch = offset.branch.as_ref();
    let expected_branch: &Branch = <TextRef as AsRef<Branch>>::as_ref(text);
    if resolved_branch.id() != expected_branch.id() {
        return None;
    }
    let content = text.get_string(transaction);
    if usize::try_from(offset.index).ok()? > content.encode_utf16().count() {
        return None;
    }
    let byte_offset = utf16_to_byte_offset_exact(&content, offset.index)?;
    content.is_char_boundary(byte_offset).then(|| Pos::new(StoryRef::Body, path.clone(), byte_offset))
}

#[cfg(any(target_arch = "wasm32", test))]
fn utf16_to_byte_offset_exact(text: &str, utf16_offset: u32) -> Option<usize> {
    let target = usize::try_from(utf16_offset).ok()?;
    let mut units = 0usize;
    for (byte, ch) in text.char_indices() {
        if units == target {
            return Some(byte);
        }
        units = units.checked_add(ch.len_utf16())?;
        if units > target {
            return None;
        }
    }
    (units == target).then_some(text.len())
}

fn validate_shared_paragraphs(paragraphs: &[SharedParagraph]) -> Result<(), String> {
    if !(1..=MAX_SHARED_PARAGRAPHS).contains(&paragraphs.len()) {
        return Err(scope_error());
    }
    let mut total_bytes = 0usize;
    for paragraph in paragraphs {
        total_bytes =
            total_bytes.checked_add(paragraph.text.len()).ok_or_else(|| "Live text exceeds the 8 MiB combined document limit.".to_string())?;
        if paragraph.text.chars().any(unsupported_text_char) {
            return Err("This live session supports plain text only. Inline objects and control characters are not supported.".into());
        }
    }
    if total_bytes > MAX_SHARED_TEXT_BYTES {
        return Err("Live text is limited to 8 MiB across all body paragraphs. The update was rejected to protect the document.".into());
    }
    Ok(())
}

fn selection_paragraph_index(pos: &Pos, paths: &[Path]) -> Result<usize, String> {
    if pos.story != StoryRef::Body {
        return Err("Live editing supports selections in the fixed body paragraph paths only.".into());
    }
    paths.iter().position(|path| path == &pos.path).ok_or_else(|| "Live editing supports selections in the fixed body paragraph paths only.".into())
}

fn selected_paragraph_index(selection: &Selection, paths: &[Path]) -> Result<usize, String> {
    let anchor = selection_paragraph_index(&selection.anchor, paths)?;
    let focus = selection_paragraph_index(&selection.focus, paths)?;
    if anchor != focus {
        return Err("Live editing and formatting must stay inside one body paragraph.".into());
    }
    Ok(anchor)
}

fn checked_offset(text: &str, offset: usize) -> Result<u32, String> {
    if offset > text.len() || !text.is_char_boundary(offset) {
        return Err("The caret is not at a valid UTF-8 text boundary.".into());
    }
    u32::try_from(offset).map_err(|_| "The caret position is too large for live editing.".into())
}

fn sticky_index<T: ReadTxn>(text: &TextRef, transaction: &T, offset: u32, assoc: Assoc) -> Result<StickyIndex, String> {
    if text.len(transaction) == 0 {
        Ok(StickyIndex::from_type(transaction, text, assoc))
    } else {
        let current = text.get_string(transaction);
        let byte_offset = usize::try_from(offset).map_err(|_| "The caret position is too large for live text.".to_string())?;
        if byte_offset > current.len() || !current.is_char_boundary(byte_offset) {
            return Err("The caret is not at a valid UTF-8 text boundary.".into());
        }
        let sequence_offset = byte_to_utf16_offset(&current, byte_offset)?;
        if sequence_offset == text.len(transaction) && assoc == Assoc::After {
            // `StickyIndex::at` cannot produce a right-associated anchor at the end of a
            // sequence; the shared type boundary resolves to the text end and follows inserts.
            Ok(StickyIndex::from_type(transaction, text, assoc))
        } else {
            text.sticky_index(transaction, sequence_offset, assoc).ok_or_else(|| "Could not anchor the current caret in live text.".into())
        }
    }
}

fn resolve_sticky<T: ReadTxn>(index: &StickyIndex, transaction: &T, text: &str) -> Result<usize, String> {
    let offset = index.get_offset(transaction).ok_or_else(|| {
        "The live text update could not be mapped to the current selection. Editing was paused to protect the document.".to_string()
    })?;
    Ok(utf16_to_byte_offset(text, offset.index))
}

fn byte_to_utf16_offset(text: &str, byte_offset: usize) -> Result<u32, String> {
    if byte_offset > text.len() || !text.is_char_boundary(byte_offset) {
        return Err("The text position is not at a valid UTF-8 character boundary.".into());
    }
    u32::try_from(text[..byte_offset].encode_utf16().count()).map_err(|_| "The text position is too large for live editing.".into())
}

fn utf16_to_byte_offset(text: &str, utf16_offset: u32) -> usize {
    let target = utf16_offset as usize;
    let mut units = 0usize;
    for (byte, ch) in text.char_indices() {
        if units >= target {
            return byte;
        }
        let next = units.saturating_add(ch.len_utf16());
        if target < next {
            return byte;
        }
        units = next;
        if units == target {
            return byte + ch.len_utf8();
        }
    }
    text.len()
}

fn clamp_offset(text: &str, offset: usize) -> usize {
    let mut offset = offset.min(text.len());
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn clamp_position(paragraphs: &[SharedParagraph], paths: &[Path], pos: &Pos) -> Result<Pos, String> {
    let (index, offset) = selection_paragraph_index(pos, paths).map_or((0, 0), |index| (index, pos.off));
    let paragraph = paragraphs.get(index).ok_or_else(scope_error)?;
    Ok(Pos::new(StoryRef::Body, paragraph.path.clone(), clamp_offset(&paragraph.text, offset)))
}

fn one_splice<'a>(before: &str, after: &'a str) -> Option<(usize, usize, &'a str)> {
    if before == after {
        return None;
    }
    let prefix_bytes = before.bytes().zip(after.bytes()).take_while(|(left, right)| left == right).count();
    let mut prefix = prefix_bytes;
    while prefix > 0 && (!before.is_char_boundary(prefix) || !after.is_char_boundary(prefix)) {
        prefix -= 1;
    }
    let mut old_end = before.len();
    let mut new_end = after.len();
    while old_end > prefix && new_end > prefix {
        let old_char = before[..old_end].chars().next_back()?;
        let new_char = after[..new_end].chars().next_back()?;
        if old_char != new_char {
            break;
        }
        old_end -= old_char.len_utf8();
        new_end -= new_char.len_utf8();
    }
    Some((prefix, old_end, after.get(prefix..new_end)?))
}

fn scope_error() -> String {
    "Live editing supports 1–256 fixed body paragraphs in the main body and one-level table cells, with unmerged cells, plain text plus bold, italic, and single underline, and an 8 MiB combined text limit. Save or open a supported document first.".into()
}

fn restore_editor(session: &mut Session, document: Document, selection: Selection) {
    session.doc = document;
    session.sel = selection;
    session.reset_history();
    session.touch();
    session.relayout();
}

fn apply_local_text_command(
    adapter: &mut LiveText,
    session: &mut Session,
    id: &str,
    params: &serde_json::Value,
) -> Result<(serde_json::Value, Option<Vec<u8>>, bool), String> {
    let current = supported_body(&session.doc).ok_or_else(scope_error)?;
    let paragraph_index = selected_paragraph_index(&session.sel, &adapter.paths)?;
    if adapter.shared_paragraphs()? != current || !adapter.editor_matches_shared(&session.doc)? {
        return Err("The editor is catching up with live text. Wait for the update before typing.".into());
    }
    let paragraph_text = current.get(paragraph_index).ok_or_else(scope_error)?.text.as_str();
    if session.sel.is_collapsed()
        && ((id == "text.backspace" && session.sel.focus.off == 0 && paragraph_index > 0)
            || (id == "text.delete" && session.sel.focus.off == paragraph_text.len() && paragraph_index + 1 < current.len()))
    {
        return Err("Paragraph merging is disabled during live collaboration.".into());
    }
    let before_document = session.doc.clone();
    let before_selection = session.sel.clone();
    let mut command_params = params.clone();
    if id == "text.insert" {
        if let Some(text) = params.get("text").and_then(serde_json::Value::as_str)
            && text.chars().any(unsupported_text_char)
        {
            return Err("Paragraph breaks and control characters are blocked during live collaboration.".into());
        }
        if let Some(object) = command_params.as_object_mut() {
            object.insert("raw".into(), serde_json::Value::Bool(true));
        }
    }
    let result = match session.run(id, &command_params) {
        Ok(result) => result,
        Err(error) => {
            restore_editor(session, before_document, before_selection);
            return Err(error.to_string());
        }
    };
    if !adapter.matches_manifest(&session.doc) {
        restore_editor(session, before_document, before_selection);
        return Err("The body structure changed during live collaboration; the attempted edit was reverted.".into());
    }
    let paragraph_path = current.get(paragraph_index).ok_or_else(scope_error)?.path.clone();
    let paragraph_mark_cleared = match clear_paragraph_mark(&mut session.doc, &paragraph_path) {
        Ok(cleared) => cleared,
        Err(error) => {
            restore_editor(session, before_document, before_selection);
            return Err(error);
        }
    };
    let update = match adapter.apply_local_editor_change(session, &current, paragraph_index) {
        Ok(update) => update,
        Err(error) => {
            restore_editor(session, before_document, before_selection);
            return Err(error);
        }
    };
    if paragraph_mark_cleared {
        session.touch();
        session.relayout();
    }
    Ok((result, update, paragraph_mark_cleared))
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct CollaborationBridge {
    inner: std::rc::Rc<std::cell::RefCell<BridgeState>>,
}

#[cfg(target_arch = "wasm32")]
struct BridgeState {
    phase: ConnectionPhase,
    document_id: Option<String>,
    adapter: Option<LiveText>,
    socket: Option<web_sys::WebSocket>,
    socket_generation: u64,
    connection_deadline_ms: Option<u64>,
    reconnect: ReconnectSchedule,
    presence: PresenceState,
    on_open: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    on_message: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::MessageEvent)>>,
    on_close: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    on_error: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    events: std::collections::VecDeque<SocketEvent>,
    pending_updates: std::collections::VecDeque<Vec<u8>>,
    first_state_received: bool,
    epoch_documents: std::collections::BTreeSet<String>,
    status: String,
}

#[cfg(target_arch = "wasm32")]
#[derive(Default)]
struct PresenceState {
    phase: PresencePhase,
    socket: Option<web_sys::WebSocket>,
    socket_generation: u64,
    connection_deadline_ms: Option<u64>,
    reconnect: ReconnectSchedule,
    heartbeat: PresenceHeartbeatSchedule,
    cursor_schedule: PresenceCursorSchedule,
    session_id: Option<String>,
    on_open: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    on_message: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::MessageEvent)>>,
    on_close: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    on_error: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    events: std::collections::VecDeque<PresenceSocketEvent>,
    participants: Vec<PresenceParticipant>,
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PresencePhase {
    #[default]
    Inactive,
    Connecting,
    Live,
    Offline,
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionPhase {
    Inactive,
    Connecting,
    Live,
    Offline,
    Faulted,
}

#[cfg(target_arch = "wasm32")]
enum SocketEvent {
    Open { generation: u64 },
    Message { generation: u64, bytes: Vec<u8> },
    BadMessage { generation: u64, message: String },
    Closed { generation: u64 },
    Error { generation: u64 },
}

#[cfg(target_arch = "wasm32")]
impl SocketEvent {
    fn generation(&self) -> u64 {
        match self {
            SocketEvent::Open { generation }
            | SocketEvent::Message { generation, .. }
            | SocketEvent::BadMessage { generation, .. }
            | SocketEvent::Closed { generation }
            | SocketEvent::Error { generation } => *generation,
        }
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn clear_stale_document_queues<E>(events: &mut std::collections::VecDeque<E>, pending_updates: &mut std::collections::VecDeque<Vec<u8>>) {
    events.clear();
    pending_updates.clear();
}

#[cfg(target_arch = "wasm32")]
impl Default for BridgeState {
    fn default() -> Self {
        BridgeState {
            phase: ConnectionPhase::Inactive,
            document_id: None,
            adapter: None,
            socket: None,
            socket_generation: 0,
            connection_deadline_ms: None,
            reconnect: ReconnectSchedule::default(),
            presence: PresenceState::default(),
            on_open: None,
            on_message: None,
            on_close: None,
            on_error: None,
            events: std::collections::VecDeque::new(),
            pending_updates: std::collections::VecDeque::new(),
            first_state_received: false,
            epoch_documents: std::collections::BTreeSet::new(),
            status: "Live text is not connected.".into(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl PresenceState {
    fn close_socket(&mut self) {
        self.socket_generation = self.socket_generation.wrapping_add(1);
        if let Some(socket) = self.socket.take() {
            socket.set_onopen(None);
            socket.set_onmessage(None);
            socket.set_onclose(None);
            socket.set_onerror(None);
            let _ = socket.close();
        }
        self.on_open = None;
        self.on_message = None;
        self.on_close = None;
        self.on_error = None;
        self.events.clear();
    }

    fn stop(&mut self) {
        if self.phase != PresencePhase::Inactive || self.socket.is_some() {
            self.close_socket();
        } else {
            self.events.clear();
        }
        self.phase = PresencePhase::Inactive;
        self.connection_deadline_ms = None;
        self.reconnect.pause();
        self.heartbeat.pause();
        self.cursor_schedule.clear();
        self.session_id = None;
        self.participants.clear();
    }

    fn failed(&mut self) {
        self.close_socket();
        self.phase = PresencePhase::Offline;
        self.connection_deadline_ms = None;
        self.heartbeat.pause();
        self.cursor_schedule.clear();
        self.session_id = None;
        self.participants.clear();
    }
}

#[cfg(target_arch = "wasm32")]
impl CollaborationBridge {
    pub fn new() -> Self {
        CollaborationBridge { inner: std::rc::Rc::new(std::cell::RefCell::new(BridgeState::default())) }
    }

    pub fn command_handler(&self) -> Box<dyn FnMut(&mut Session, &str, &serde_json::Value) -> Option<Result<serde_json::Value, String>>> {
        let inner = self.inner.clone();
        Box::new(move |session, id, params| {
            let Ok(mut state) = inner.try_borrow_mut() else {
                return Some(Err("The live editor is busy applying a collaboration update.".into()));
            };
            state.command(session, id, params)
        })
    }

    pub fn history_handler(&self) -> Box<dyn Fn() -> Option<(bool, bool)>> {
        let inner = self.inner.clone();
        Box::new(move || {
            let state = inner.try_borrow().ok()?;
            if state.phase == ConnectionPhase::Inactive {
                return None;
            }
            let adapter = state.adapter.as_ref()?;
            Some((adapter.can_undo(), adapter.can_redo()))
        })
    }

    pub fn is_active_for(&self, document_id: Option<&str>) -> bool {
        let Ok(state) = self.inner.try_borrow() else { return false };
        state.phase != ConnectionPhase::Inactive && state.phase != ConnectionPhase::Faulted && state.document_id.as_deref() == document_id
    }

    pub fn can_save_as_copy_for(&self, document_id: Option<&str>) -> bool {
        let Ok(state) = self.inner.try_borrow() else { return false };
        matches!(state.phase, ConnectionPhase::Offline | ConnectionPhase::Faulted) && state.document_id.as_deref() == document_id
    }

    pub fn snapshot_locked_for(&self, document_id: Option<&str>) -> bool {
        let Ok(state) = self.inner.try_borrow() else { return true };
        document_id.is_some_and(|id| state.epoch_documents.contains(id))
    }

    pub fn remote_selections(&self, session: &Session) -> Vec<wordcraft_ui_egui::RemoteSelection> {
        let Ok(state) = self.inner.try_borrow() else { return Vec::new() };
        if state.phase != ConnectionPhase::Live || state.presence.phase != PresencePhase::Live {
            return Vec::new();
        }
        let (Some(self_id), Some(adapter)) = (state.presence.session_id.as_deref(), state.adapter.as_ref()) else {
            return Vec::new();
        };
        if !adapter.matches_manifest(&session.doc) {
            return Vec::new();
        }
        state
            .presence
            .participants
            .iter()
            .filter(|participant| participant.id != self_id)
            .filter_map(|participant| {
                let cursor = participant.cursor.as_ref()?;
                let (anchor, focus) = adapter.resolve_presence_cursor(cursor)?;
                Some(wordcraft_ui_egui::RemoteSelection { anchor, focus, label: participant.name.clone(), color: presence_color(&participant.id) })
            })
            .collect()
    }

    pub fn logic(&self, session: &mut Session, active_document_id: Option<&str>, ctx: &egui::Context) {
        let events = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if state.document_id.is_some() && state.document_id.as_deref() != active_document_id {
                state.close_socket();
                state.presence.stop();
                state.reconnect.pause();
                state.connection_deadline_ms = None;
                state.phase = ConnectionPhase::Inactive;
                state.document_id = None;
                state.adapter = None;
                state.first_state_received = false;
                // Queued socket events and pending CRDT bytes belong to the previous document.
                // Never replay them into a document opened later in this tab. The editor's
                // local recovery draft remains the user's recovery path for unconfirmed text.
                {
                    let BridgeState { events, pending_updates, .. } = &mut *state;
                    clear_stale_document_queues(events, pending_updates);
                }
                state.status = "Live text stopped because the active server document changed. Unconfirmed edits remain in this browser's recovery draft and cannot be replayed into another document.".into();
                session.reset_history();
            }
            state.events.drain(..).collect::<Vec<_>>()
        };

        for event in events {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if !is_current_socket_generation(event.generation(), state.socket_generation) {
                continue;
            }
            match event {
                SocketEvent::Open { .. } => {
                    if state.phase == ConnectionPhase::Connecting {
                        state.status = "Connected. Waiting for the server's live text state…".into();
                    }
                }
                SocketEvent::Message { bytes, .. } => {
                    if let Err(error) = state.apply_message(session, &bytes) {
                        state.phase = ConnectionPhase::Faulted;
                        state.connection_deadline_ms = None;
                        state.reconnect.pause();
                        state.status = error;
                        state.close_socket();
                    }
                }
                SocketEvent::BadMessage { message, .. } => {
                    state.phase = ConnectionPhase::Faulted;
                    state.connection_deadline_ms = None;
                    state.reconnect.pause();
                    state.status = message;
                    state.close_socket();
                }
                SocketEvent::Closed { .. } => {
                    if state.phase != ConnectionPhase::Inactive && state.phase != ConnectionPhase::Faulted {
                        state.connection_deadline_ms = None;
                        state.close_socket();
                        state.phase = ConnectionPhase::Offline;
                        state.status = "Live text connection closed. Automatic reconnect is active; editing stays paused until the server confirms the current state.".into();
                    }
                }
                SocketEvent::Error { .. } => {
                    if state.phase != ConnectionPhase::Inactive && state.phase != ConnectionPhase::Faulted {
                        state.connection_deadline_ms = None;
                        state.close_socket();
                        state.phase = ConnectionPhase::Offline;
                        state.status = "Live text connection failed. Automatic reconnect is active; editing stays paused until the server confirms the current state.".into();
                    }
                }
            }
            ctx.request_repaint();
        }
        self.maybe_auto_reconnect(active_document_id, ctx);
        self.update_presence(session, active_document_id, ctx);
    }

    fn maybe_auto_reconnect(&self, active_document_id: Option<&str>, ctx: &egui::Context) {
        let now_ms = monotonic_now_ms();
        let (retry_document, wait_ms) = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if state.phase == ConnectionPhase::Connecting && state.connection_deadline_ms.is_some_and(|deadline| now_ms >= deadline) {
                state.close_socket();
                state.connection_deadline_ms = None;
                state.phase = ConnectionPhase::Offline;
                state.status = "The live server did not respond in time. Automatic reconnect is active; editing stays paused.".into();
            }
            if state.phase != ConnectionPhase::Offline || state.document_id.as_deref() != active_document_id {
                (None, None)
            } else {
                match state.reconnect.tick(now_ms) {
                    ReconnectDecision::Disabled => (None, None),
                    ReconnectDecision::Wait(wait_ms) => (None, Some(wait_ms)),
                    ReconnectDecision::RetryNow => {
                        let document_id = state.document_id.clone();
                        if document_id.is_some() {
                            state.phase = ConnectionPhase::Connecting;
                            state.connection_deadline_ms = Some(now_ms.saturating_add(CONNECT_TIMEOUT_MS));
                            state.status = "Reconnecting to live text… editing stays paused until the server confirms the current state.".into();
                        }
                        (document_id, None)
                    }
                }
            }
        };
        if let Some(document_id) = retry_document {
            self.open_socket(&document_id, ctx);
        } else if let Some(wait_ms) = wait_ms {
            ctx.request_repaint_after(std::time::Duration::from_millis(wait_ms.max(1)));
        }
    }

    fn update_presence(&self, session: &Session, active_document_id: Option<&str>, ctx: &egui::Context) {
        let events = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if state.phase != ConnectionPhase::Live || state.document_id.as_deref() != active_document_id {
                state.presence.stop();
                return;
            }
            let now_ms = monotonic_now_ms();
            if state.presence.phase == PresencePhase::Connecting && state.presence.connection_deadline_ms.is_some_and(|deadline| now_ms >= deadline) {
                state.presence.failed();
            }
            state.presence.events.drain(..).collect::<Vec<_>>()
        };

        for event in events {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if !is_current_presence_generation(event.generation(), state.presence.socket_generation) {
                continue;
            }
            match event {
                PresenceSocketEvent::Open { .. } => {
                    if state.phase == ConnectionPhase::Live && state.presence.phase == PresencePhase::Connecting {
                        let now_ms = monotonic_now_ms();
                        state.presence.phase = PresencePhase::Live;
                        state.presence.connection_deadline_ms = None;
                        state.presence.reconnect.connected();
                        state.presence.heartbeat.start(now_ms);
                    }
                }
                PresenceSocketEvent::Message { text, .. } => {
                    if state.phase == ConnectionPhase::Live && state.presence.phase == PresencePhase::Live {
                        match presence_message_kind(&text).as_deref() {
                            Ok("session") if state.presence.session_id.is_none() => match parse_presence_session(&text) {
                                Ok(id) => {
                                    state.presence.session_id = Some(id);
                                    state.presence.cursor_schedule.reset_for_reconnect();
                                }
                                Err(_) => state.presence.failed(),
                            },
                            Ok("roster") if state.presence.session_id.is_some() => match parse_presence_roster(&text) {
                                Ok(participants) => state.presence.participants = participants,
                                Err(_) => state.presence.failed(),
                            },
                            _ => state.presence.failed(),
                        }
                    }
                }
                PresenceSocketEvent::BadMessage { .. } | PresenceSocketEvent::Closed { .. } | PresenceSocketEvent::Error { .. } => {
                    if state.phase == ConnectionPhase::Live && state.presence.phase != PresencePhase::Inactive {
                        state.presence.failed();
                    }
                }
            }
            ctx.request_repaint();
        }

        let (open_document, wait_ms) = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if state.phase != ConnectionPhase::Live || state.document_id.as_deref() != active_document_id {
                state.presence.stop();
                return;
            }
            let now_ms = monotonic_now_ms();
            let mut open_document = None;
            let mut wait_ms = None;
            match state.presence.phase {
                PresencePhase::Inactive => open_document = state.document_id.clone(),
                PresencePhase::Connecting => {
                    wait_ms = state.presence.connection_deadline_ms.map(|deadline| deadline.saturating_sub(now_ms));
                }
                PresencePhase::Offline => match state.presence.reconnect.tick(now_ms) {
                    ReconnectDecision::Disabled => {}
                    ReconnectDecision::Wait(wait) => wait_ms = Some(wait),
                    ReconnectDecision::RetryNow => open_document = state.document_id.clone(),
                },
                PresencePhase::Live => {
                    if state.presence.session_id.is_some()
                        && let Some(cursor) = state
                            .adapter
                            .as_ref()
                            .and_then(|adapter| adapter.capture_selection(session).ok())
                            .and_then(|selection| encode_presence_cursor(&selection).ok())
                    {
                        state.presence.cursor_schedule.queue(cursor, now_ms);
                    }
                    if let Some(cursor) = state.presence.cursor_schedule.due(now_ms) {
                        let cursor_result = state
                            .presence
                            .socket
                            .as_ref()
                            .filter(|socket| socket.ready_state() == web_sys::WebSocket::OPEN)
                            .ok_or_else(|| "The presence socket is not open.".to_string())
                            .and_then(|socket| socket.send_with_str(&cursor).map_err(browser_error));
                        if cursor_result.is_err() {
                            state.presence.failed();
                        }
                    }
                    if state.presence.phase == PresencePhase::Live && state.presence.heartbeat.due(now_ms) {
                        let heartbeat_result = state
                            .presence
                            .socket
                            .as_ref()
                            .filter(|socket| socket.ready_state() == web_sys::WebSocket::OPEN)
                            .ok_or_else(|| "The presence socket is not open.".to_string())
                            .and_then(|socket| socket.send_with_str(presence_heartbeat_json()).map_err(browser_error));
                        if heartbeat_result.is_err() {
                            state.presence.failed();
                            match state.presence.reconnect.tick(now_ms) {
                                ReconnectDecision::Wait(wait) => wait_ms = Some(wait),
                                ReconnectDecision::RetryNow => open_document = state.document_id.clone(),
                                ReconnectDecision::Disabled => {}
                            }
                        }
                    }
                    if state.presence.phase == PresencePhase::Live {
                        wait_ms =
                            [state.presence.heartbeat.wait_ms(now_ms), state.presence.cursor_schedule.wait_ms(now_ms)].into_iter().flatten().min();
                    } else if state.presence.phase == PresencePhase::Offline {
                        match state.presence.reconnect.tick(now_ms) {
                            ReconnectDecision::Wait(wait) => wait_ms = Some(wait),
                            ReconnectDecision::RetryNow => open_document = state.document_id.clone(),
                            ReconnectDecision::Disabled => {}
                        }
                    }
                }
            }
            (open_document, wait_ms)
        };

        if let Some(document_id) = open_document {
            self.open_presence_socket(&document_id, ctx);
        }
        if let Some(wait_ms) = wait_ms {
            ctx.request_repaint_after(std::time::Duration::from_millis(wait_ms.max(1)));
        }
    }

    fn open_presence_socket(&self, document_id: &str, ctx: &egui::Context) {
        use wasm_bindgen::JsCast as _;

        let generation = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if state.phase != ConnectionPhase::Live || state.document_id.as_deref() != Some(document_id) {
                return;
            }
            if matches!(state.presence.phase, PresencePhase::Connecting | PresencePhase::Live) {
                return;
            }
            if state.presence.phase == PresencePhase::Inactive {
                state.presence.reconnect.ensure_started();
            }
            state.presence.close_socket();
            state.presence.session_id = None;
            state.presence.participants.clear();
            state.presence.cursor_schedule.reset_for_reconnect();
            state.presence.phase = PresencePhase::Connecting;
            state.presence.connection_deadline_ms = Some(monotonic_now_ms().saturating_add(CONNECT_TIMEOUT_MS));
            state.presence.socket_generation
        };
        ctx.request_repaint_after(std::time::Duration::from_millis(CONNECT_TIMEOUT_MS));

        let fail = |bridge: &CollaborationBridge| {
            if let Ok(mut state) = bridge.inner.try_borrow_mut()
                && state.presence.socket_generation == generation
            {
                state.presence.failed();
            }
            ctx.request_repaint_after(std::time::Duration::from_millis(RECONNECT_INITIAL_DELAY_MS));
        };
        let Some(window) = web_sys::window() else {
            fail(self);
            return;
        };
        let location = window.location();
        let Ok(protocol) = location.protocol() else {
            fail(self);
            return;
        };
        let Ok(host) = location.host() else {
            fail(self);
            return;
        };
        let scheme = match protocol.as_str() {
            "https:" => "wss",
            "http:" => "ws",
            _ => {
                fail(self);
                return;
            }
        };
        let Some(encoded_id) = js_sys::encode_uri_component(document_id).as_string() else {
            fail(self);
            return;
        };
        let url = format!("{scheme}://{host}/api/documents/{encoded_id}/presence");
        let socket = match web_sys::WebSocket::new(&url) {
            Ok(socket) => socket,
            Err(_) => {
                fail(self);
                return;
            }
        };

        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_open = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_presence_event(&weak, PresenceSocketEvent::Open { generation }, &repaint);
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_message = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::MessageEvent)>::new(move |event: web_sys::MessageEvent| {
            let data = event.data();
            if let Some(text) = data.as_string() {
                enqueue_presence_event(&weak, PresenceSocketEvent::Message { generation, text }, &repaint);
            } else {
                enqueue_presence_event(&weak, PresenceSocketEvent::BadMessage { generation }, &repaint);
            }
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_close = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_presence_event(&weak, PresenceSocketEvent::Closed { generation }, &repaint);
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_error = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_presence_event(&weak, PresenceSocketEvent::Error { generation }, &repaint);
        });
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        if let Ok(mut state) = self.inner.try_borrow_mut() {
            if state.phase == ConnectionPhase::Live
                && state.document_id.as_deref() == Some(document_id)
                && state.presence.socket_generation == generation
                && state.presence.phase == PresencePhase::Connecting
            {
                state.presence.socket = Some(socket);
                state.presence.on_open = Some(on_open);
                state.presence.on_message = Some(on_message);
                state.presence.on_close = Some(on_close);
                state.presence.on_error = Some(on_error);
            } else {
                let _ = socket.close();
            }
        } else {
            let _ = socket.close();
        }
    }

    pub fn ui(&self, session: &mut Session, active_document_id: Option<String>, join_readiness: Result<(), String>, ctx: &egui::Context) {
        let (phase, status, current_id, snapshot_locked, auto_reconnect, presence_phase, participants) = self
            .inner
            .try_borrow()
            .map(|state| {
                (
                    state.phase,
                    state.status.clone(),
                    state.document_id.clone(),
                    active_document_id.as_deref().is_some_and(|id| state.epoch_documents.contains(id)),
                    state.reconnect.enabled,
                    state.presence.phase,
                    state.presence.participants.clone(),
                )
            })
            .unwrap_or((ConnectionPhase::Faulted, "Live text state is busy.".into(), None, false, false, PresencePhase::Inactive, Vec::new()));

        let mut join = false;
        let mut reconnect = false;
        let mut disconnect = false;
        egui::Window::new("Live text collaboration")
            .id(egui::Id::new("wordcraft_live_text_collaboration"))
            .anchor(egui::Align2::LEFT_TOP, egui::vec2(12.0, 12.0))
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                ui.label("Scope: 1–256 fixed body paragraphs, including one-level table cells with multiple paragraphs. Text supports bold, italic, and single underline; editing and formatting stay inside one paragraph. Nested tables, merged cells, and structure changes are blocked.");
                ui.small("Text and selected-format changes are durably saved as live updates. Version history does not include live edits; restore is disabled.");
                ui.separator();
                ui.label(status);
                match presence_phase {
                    PresencePhase::Inactive => {
                        ui.small("Anonymous presence is paused until the live text connection is active.");
                    }
                    PresencePhase::Connecting => {
                        ui.small("Connecting anonymous presence…");
                    }
                    PresencePhase::Live if participants.is_empty() => {
                        ui.small("Anonymous presence is connected; waiting for the participant list…");
                    }
                    PresencePhase::Live => {
                        let names = participants.iter().map(|participant| participant.name.as_str()).collect::<Vec<_>>().join(", ");
                        ui.label(format!("Anonymous participants ({}): {names}", participants.len()));
                        ui.small("These temporary names do not verify anyone’s identity.");
                    }
                    PresencePhase::Offline => {
                        ui.small("Anonymous presence is reconnecting; the participant list is temporarily unavailable.");
                    }
                }
                if snapshot_locked && phase == ConnectionPhase::Inactive {
                    ui.colored_label(egui::Color32::YELLOW, "Live text edits are not included in version history. Snapshot save and restore are unavailable for this document.");
                }
                match phase {
                    ConnectionPhase::Inactive => {
                        let enabled = active_document_id.is_some() && join_readiness.is_ok();
                        if ui.add_enabled(enabled, egui::Button::new("Join live text")).clicked() {
                            join = true;
                        }
                        if active_document_id.is_none() {
                            ui.small("Save this document to the shared workspace before joining live text.");
                        } else if let Err(reason) = &join_readiness {
                            ui.small(reason);
                        }
                    }
                    ConnectionPhase::Connecting => {
                        ui.add_enabled(false, egui::Button::new("Connecting…"));
                        if auto_reconnect && ui.button("Pause retries").clicked() {
                            disconnect = true;
                        }
                    }
                    ConnectionPhase::Live => {
                        ui.colored_label(egui::Color32::LIGHT_GREEN, "Live text and formatting updates are being persisted by the server.");
                        if ui.button("Pause connection").clicked() {
                            disconnect = true;
                        }
                    }
                    ConnectionPhase::Offline => {
                        if ui.button("Reconnect now").clicked() {
                            reconnect = true;
                        }
                        if auto_reconnect && ui.button("Pause retries").clicked() {
                            disconnect = true;
                        }
                    }
                    ConnectionPhase::Faulted => {
                        if ui.button("Reconnect").clicked() {
                            reconnect = true;
                        }
                        ui.colored_label(egui::Color32::YELLOW, "This editor is read-only after an unsupported live update. Open another document or Save As a separate copy.");
                    }
                }
            });

        if join {
            if let Some(id) = active_document_id {
                self.connect(session, id, ctx);
            }
        } else if reconnect {
            if let Some(id) = current_id {
                self.reconnect(id, ctx);
            }
        } else if disconnect {
            self.disconnect(session);
        }
    }

    fn connect(&self, session: &mut Session, document_id: String, ctx: &egui::Context) {
        if session.dirty {
            if let Ok(mut state) = self.inner.try_borrow_mut() {
                state.status =
                    "Unsaved edits or a recovered local draft are present. Save or resolve them before joining; editor content was kept.".into();
            }
            return;
        }
        if supported_body(&session.doc).is_none() {
            if let Ok(mut state) = self.inner.try_borrow_mut() {
                state.status = scope_error();
            }
            return;
        }
        if document_id.is_empty() {
            return;
        }
        let adapter = match LiveText::with_document(&session.doc) {
            Ok(adapter) => adapter,
            Err(error) => {
                if let Ok(mut state) = self.inner.try_borrow_mut() {
                    state.status = error;
                }
                return;
            }
        };
        session.reset_history();
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            if state.phase == ConnectionPhase::Inactive || state.document_id.as_deref() != Some(document_id.as_str()) {
                state.presence.stop();
                state.adapter = Some(adapter);
                state.first_state_received = false;
            }
            state.presence.reconnect.start();
            state.reconnect.start();
            state.connection_deadline_ms = None;
            state.document_id = Some(document_id.clone());
            state.phase = ConnectionPhase::Connecting;
            state.status = "Connecting to live text… automatic reconnect is enabled if the connection drops.".into();
        } else {
            return;
        }
        self.open_socket(&document_id, ctx);
    }

    fn reconnect(&self, document_id: String, ctx: &egui::Context) {
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            if state.document_id.as_deref() != Some(document_id.as_str()) || state.adapter.is_none() {
                state.status = "The live text document changed. Rejoin from the active server document.".into();
                return;
            }
            state.close_socket();
            state.presence.stop();
            state.presence.reconnect.start();
            state.reconnect.start();
            state.connection_deadline_ms = None;
            state.first_state_received = false;
            state.phase = ConnectionPhase::Connecting;
            state.status = "Reconnecting to live text…".into();
        } else {
            return;
        }
        self.open_socket(&document_id, ctx);
    }

    fn open_socket(&self, document_id: &str, ctx: &egui::Context) {
        use wasm_bindgen::JsCast as _;

        let generation = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            state.close_socket();
            state.connection_deadline_ms = Some(monotonic_now_ms().saturating_add(CONNECT_TIMEOUT_MS));
            state.socket_generation
        };
        ctx.request_repaint_after(std::time::Duration::from_millis(CONNECT_TIMEOUT_MS));

        let Some(window) = web_sys::window() else {
            self.set_error("Browser window is unavailable.");
            return;
        };
        let location = window.location();
        let Ok(protocol) = location.protocol() else {
            self.set_error("Could not read the browser protocol.");
            return;
        };
        let Ok(host) = location.host() else {
            self.set_error("Could not read the browser host.");
            return;
        };
        let scheme = match protocol.as_str() {
            "https:" => "wss",
            "http:" => "ws",
            _ => {
                self.set_error("Live text requires an HTTP or HTTPS page.");
                return;
            }
        };
        let encoded_id = js_sys::encode_uri_component(document_id).as_string();
        let Some(encoded_id) = encoded_id else {
            self.set_error("Could not encode the live document id.");
            return;
        };
        let url = format!("{scheme}://{host}/api/documents/{encoded_id}/collab");
        let socket = match web_sys::WebSocket::new(&url) {
            Ok(socket) => socket,
            Err(error) => {
                self.set_error(format!("Could not open live text connection: {}", browser_error(error)));
                return;
            }
        };
        socket.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_open = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_socket_event(&weak, SocketEvent::Open { generation }, &repaint);
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_message = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::MessageEvent)>::new(move |event: web_sys::MessageEvent| {
            use wasm_bindgen::JsCast as _;
            let data = event.data();
            match data.dyn_into::<js_sys::ArrayBuffer>() {
                Ok(buffer) => {
                    enqueue_socket_event(&weak, SocketEvent::Message { generation, bytes: js_sys::Uint8Array::new(&buffer).to_vec() }, &repaint)
                }
                Err(_) => enqueue_socket_event(
                    &weak,
                    SocketEvent::BadMessage { generation, message: "The live server sent a non-binary message; the editor was paused.".into() },
                    &repaint,
                ),
            }
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_close = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_socket_event(&weak, SocketEvent::Closed { generation }, &repaint);
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_error = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_socket_event(&weak, SocketEvent::Error { generation }, &repaint);
        });
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        if let Ok(mut state) = self.inner.try_borrow_mut() {
            if state.socket_generation == generation {
                state.socket = Some(socket);
                state.on_open = Some(on_open);
                state.on_message = Some(on_message);
                state.on_close = Some(on_close);
                state.on_error = Some(on_error);
            } else {
                let _ = socket.close();
            }
        } else {
            let _ = socket.close();
        }
    }

    fn set_error(&self, error: impl Into<String>) {
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            state.phase = ConnectionPhase::Offline;
            state.connection_deadline_ms = None;
            state.status = error.into();
        }
    }

    fn disconnect(&self, session: &mut Session) {
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            state.close_socket();
            state.presence.stop();
            state.reconnect.pause();
            state.connection_deadline_ms = None;
            state.phase = ConnectionPhase::Offline;
            state.first_state_received = false;
            state.status = "Connection paused. Reconnect before editing; live updates are durable, but version history excludes them and restore is unavailable.".into();
            session.reset_history();
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl BridgeState {
    fn command(&mut self, session: &mut Session, id: &str, params: &serde_json::Value) -> Option<Result<serde_json::Value, String>> {
        if self.phase == ConnectionPhase::Inactive {
            return None;
        }
        if id == "file.save" {
            if matches!(self.phase, ConnectionPhase::Offline | ConnectionPhase::Faulted)
                && params.get("path").and_then(serde_json::Value::as_str).is_some()
            {
                return None;
            }
            return Some(if self.phase == ConnectionPhase::Live && self.pending_updates.is_empty() {
                Ok(serde_json::json!({"saved": true, "target": "live-collaboration", "serverConfirmed": true}))
            } else if self.phase == ConnectionPhase::Live {
                Err("Live text is syncing. Wait for the server to confirm the pending update before saving.".into())
            } else {
                Err("Live text is disconnected. Reconnect before saving or editing; changes made since the last server update remain in this browser.".into())
            });
        }
        if id == "file.saveAs" {
            if matches!(self.phase, ConnectionPhase::Offline | ConnectionPhase::Faulted) {
                return None;
            }
            return Some(Err("Save As is unavailable while live text is connected. Pause the connection to create a separate server copy.".into()));
        }
        if self.phase == ConnectionPhase::Faulted && matches!(id, "file.open" | "file.new") {
            return None;
        }
        if matches!(id, "file.open" | "file.new") {
            return Some(Err(
                "Document switching is paused while live text is connected. Disconnect or reload before opening another document.".into()
            ));
        }
        if matches!(id, "file.exportPdf" | "file.exportPng") || id.starts_with("file.export") {
            return None;
        }
        if matches!(id, "edit.undo" | "edit.redo") {
            if self.phase != ConnectionPhase::Live {
                return Some(Err("Undo and redo are paused until the live text connection is ready.".into()));
            }
            return Some(self.undo_or_redo(session, id == "edit.redo"));
        }
        if matches!(id, "format.bold" | "format.italic" | "format.underline") {
            if self.phase != ConnectionPhase::Live {
                return Some(Err("Formatting is paused until the live text connection is ready. Reconnect to continue.".into()));
            }
            return Some(self.local_format_command(session, id, params));
        }
        if matches!(id, "text.insert" | "text.backspace" | "text.delete") {
            if self.phase != ConnectionPhase::Live {
                return Some(Err("Editing is paused until the live text connection is ready. Reconnect to continue.".into()));
            }
            return Some(self.local_text_command(session, id, params));
        }
        let spec = session.registry.get(id)?;
        if !spec.mutates {
            return None;
        }
        Some(Err("This live session supports plain text plus bold, italic, and single underline inside fixed body paragraphs and unmerged one-level table cells. Paste and document structure changes are blocked.".into()))
    }

    fn local_format_command(&mut self, session: &mut Session, id: &str, params: &serde_json::Value) -> Result<serde_json::Value, String> {
        if session.sel.is_collapsed() {
            return Err("Live formatting requires a non-empty selection. Pending formatting at the caret is not supported.".into());
        }
        if id == "format.underline"
            && params.get("value").and_then(serde_json::Value::as_bool) != Some(false)
            && params.get("style").and_then(serde_json::Value::as_str).is_some_and(|style| style != "single")
        {
            return Err("Live text supports single underline only.".into());
        }
        let before = supported_body(&session.doc).ok_or_else(scope_error)?;
        let Some(adapter) = self.adapter.as_ref() else {
            return Err("Live text state is unavailable. Reconnect to continue.".into());
        };
        if !adapter.matches_manifest(&session.doc) || !adapter.editor_matches_shared(&session.doc)? {
            return Err("The editor is catching up with live text. Formatting was paused to protect the document.".into());
        }
        let paragraph_index = selected_paragraph_index(&session.sel, &adapter.paths)?;
        let paragraph_path = before.get(paragraph_index).ok_or_else(scope_error)?.path.clone();
        let (start, end) = session.sel.ordered();
        if start == end {
            return Err("Live formatting supports a non-empty selection in one body paragraph only.".into());
        }
        let before_document = session.doc.clone();
        let before_selection = session.sel.clone();
        let result = match session.run(id, params) {
            Ok(result) => result,
            Err(error) => {
                session.reset_history();
                return Err(error.to_string());
            }
        };
        if self.adapter.as_ref().is_none_or(|adapter| !adapter.matches_manifest(&session.doc)) {
            session.doc = before_document;
            session.sel = before_selection;
            session.reset_history();
            session.touch();
            session.relayout();
            return Err("The body structure changed during live collaboration; the formatting was reverted.".into());
        }
        if let Err(error) = clear_paragraph_mark(&mut session.doc, &paragraph_path) {
            session.doc = before_document;
            session.sel = before_selection;
            session.reset_history();
            session.touch();
            session.relayout();
            return Err(error);
        }
        let after = supported_body(&session.doc);
        if after.as_ref().is_none_or(|after| {
            after.len() != before.len()
                || after.iter().zip(&before).any(|(current, original)| current.path != original.path)
                || after
                    .iter()
                    .zip(&before)
                    .enumerate()
                    .any(|(index, (current, original))| if index == paragraph_index { current.text != original.text } else { current != original })
        }) {
            session.doc = before_document;
            session.sel = before_selection;
            session.reset_history();
            session.touch();
            session.relayout();
            return Err("Live formatting changed the body text unexpectedly; the edit was reverted.".into());
        }
        let formatted = match selection_format_value(&session.doc, &paragraph_path, id, start.off, end.off) {
            Ok(value) => value,
            Err(error) => {
                session.doc = before_document;
                session.sel = before_selection;
                session.reset_history();
                session.touch();
                session.relayout();
                return Err(error);
            }
        };
        let adapter = self.adapter.as_mut().ok_or_else(|| "Live text state is unavailable. Reconnect to continue.".to_string())?;
        let update = match adapter.apply_format_update(paragraph_index, id, start.off, end.off, formatted) {
            Ok(update) => update,
            Err(error) => {
                session.doc = before_document;
                session.sel = before_selection;
                session.reset_history();
                session.touch();
                session.relayout();
                return Err(error);
            }
        };
        let updated = adapter.shared_paragraphs()?;
        if let Err(error) = adapter.project_body(session, &updated, None, session.sel.clone()) {
            session.doc = before_document;
            session.sel = before_selection;
            session.reset_history();
            session.touch();
            session.relayout();
            self.phase = ConnectionPhase::Faulted;
            self.status = format!("Formatting could not be projected safely: {error}");
            self.close_socket();
            return Err(self.status.clone());
        }
        session.reset_history();
        if let Some(update) = update
            && let Err(error) = self.send_update(&update)
        {
            self.phase = ConnectionPhase::Offline;
            self.status =
                format!("Connection lost while sending formatting: {error}. The edit remains in this browser; reconnect before continuing.");
        }
        if self.pending_updates.is_empty() {
            session.dirty = false;
        }
        Ok(result)
    }

    fn local_text_command(&mut self, session: &mut Session, id: &str, params: &serde_json::Value) -> Result<serde_json::Value, String> {
        let adapter = self.adapter.as_mut().ok_or_else(|| "Live text state is unavailable. Reconnect to continue.".to_string())?;
        let (result, update, _paragraph_mark_cleared) = apply_local_text_command(adapter, session, id, params)?;
        if let Some(update) = update
            && let Err(error) = self.send_update(&update)
        {
            self.phase = ConnectionPhase::Offline;
            self.status = format!("Connection lost while sending an edit: {error}. The edit remains in this browser; reconnect before typing again.");
        }
        if self.pending_updates.is_empty() {
            session.dirty = false;
        }
        Ok(result)
    }

    fn undo_or_redo(&mut self, session: &mut Session, redo: bool) -> Result<serde_json::Value, String> {
        let adapter = self.adapter.as_mut().ok_or_else(|| "Live text state is unavailable. Reconnect to continue.".to_string())?;
        let update = adapter.undo_or_redo(session, redo)?;
        if let Some(update) = update
            && let Err(error) = self.send_update(&update)
        {
            self.phase = ConnectionPhase::Offline;
            self.status =
                format!("Connection lost while sending undo history: {error}. The edit remains in this browser; reconnect before continuing.");
        }
        Ok(serde_json::json!({"collaborationUndo": !redo}))
    }

    fn apply_message(&mut self, session: &mut Session, bytes: &[u8]) -> Result<(), String> {
        let adapter = self.adapter.as_mut().ok_or_else(|| "Live text state is unavailable.".to_string())?;
        if !self.first_state_received {
            let result = adapter.apply_full_state(session, bytes)?;
            self.first_state_received = true;
            self.phase = ConnectionPhase::Live;
            self.connection_deadline_ms = None;
            self.reconnect.connected();
            self.status = "Live. Text changes are persisted by the server; version history excludes these edits.".into();
            if let Some(id) = self.document_id.clone() {
                self.epoch_documents.insert(id);
            }
            if !self.pending_updates.is_empty() {
                // State vectors cover inserted structs, but do not acknowledge delete-only ops.
                // Replay the exact pending updates after reconnect and keep them dirty until their
                // durable byte-for-byte server echoes arrive.
                session.dirty = true;
                if let Err(error) = self.resend_pending() {
                    self.phase = ConnectionPhase::Offline;
                    self.status = format!("Connected, but could not retry pending live text updates: {error}. Reconnect to retry.");
                }
            } else if result.local_state_acknowledged {
                session.dirty = false;
                self.status = "Live. The server confirmed all pending text and formatting updates. Version history excludes these edits.".into();
            } else if let Some(update) = result.catch_up {
                session.dirty = true;
                if let Err(error) = self.send_update(&update) {
                    self.phase = ConnectionPhase::Offline;
                    self.status = format!("Connected, but could not synchronize this browser's pending live text: {error}. Reconnect to retry.");
                }
            } else {
                session.dirty = false;
                self.status = "Live. The server confirmed all text and formatting updates. Version history excludes these edits.".into();
            }
        } else {
            let acknowledged = self.pending_updates.iter().position(|pending| pending.as_slice() == bytes);
            let changed = adapter.apply_remote_update(session, bytes)?;
            if let Some(index) = acknowledged {
                self.pending_updates.remove(index);
            }
            if self.pending_updates.is_empty() {
                session.dirty = false;
                self.status = "Live. The server confirmed all text and formatting updates. Version history excludes these edits.".into();
            } else {
                self.status = format!("Syncing live text… {} update(s) await server confirmation.", self.pending_updates.len());
            }
            if changed && self.pending_updates.is_empty() {
                self.status = "Live. Remote text or formatting was applied to the existing editor; all updates are durably stored. Version history excludes live edits.".into();
            }
        }
        Ok(())
    }

    fn send_update(&mut self, update: &[u8]) -> Result<(), String> {
        self.pending_updates.push_back(update.to_vec());
        self.send_pending_bytes(update)
    }

    fn resend_pending(&mut self) -> Result<(), String> {
        let updates: Vec<Vec<u8>> = self.pending_updates.iter().cloned().collect();
        for update in updates {
            self.send_pending_bytes(&update)?;
        }
        Ok(())
    }

    fn send_pending_bytes(&mut self, update: &[u8]) -> Result<(), String> {
        let socket = self.socket.as_ref().ok_or_else(|| "The live text socket is not open.".to_string())?;
        if socket.ready_state() != web_sys::WebSocket::OPEN {
            return Err("The live text socket is not open.".into());
        }
        socket.send_with_u8_array(update).map_err(browser_error)?;
        self.status = format!("Syncing live text… {} update(s) await server confirmation.", self.pending_updates.len());
        Ok(())
    }

    fn close_socket(&mut self) {
        self.socket_generation = self.socket_generation.wrapping_add(1);
        if let Some(socket) = self.socket.take() {
            socket.set_onopen(None);
            socket.set_onmessage(None);
            socket.set_onclose(None);
            socket.set_onerror(None);
            let _ = socket.close();
        }
        self.on_open = None;
        self.on_message = None;
        self.on_close = None;
        self.on_error = None;
    }
}

#[cfg(target_arch = "wasm32")]
fn enqueue_socket_event(state: &std::rc::Weak<std::cell::RefCell<BridgeState>>, event: SocketEvent, ctx: &egui::Context) {
    if let Some(state) = state.upgrade()
        && let Ok(mut state) = state.try_borrow_mut()
    {
        state.events.push_back(event);
        ctx.request_repaint();
    }
}

#[cfg(target_arch = "wasm32")]
fn enqueue_presence_event(state: &std::rc::Weak<std::cell::RefCell<BridgeState>>, event: PresenceSocketEvent, ctx: &egui::Context) {
    if let Some(state) = state.upgrade()
        && let Ok(mut state) = state.try_borrow_mut()
    {
        state.presence.events.push_back(event);
        ctx.request_repaint();
    }
}

#[cfg(target_arch = "wasm32")]
fn monotonic_now_ms() -> u64 {
    web_sys::window().and_then(|window| window.performance()).map(|performance| performance.now()).unwrap_or_else(js_sys::Date::now).max(0.0) as u64
}

#[cfg(target_arch = "wasm32")]
fn browser_error(error: wasm_bindgen::JsValue) -> String {
    error.as_string().unwrap_or_else(|| format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::{
        ATTR_BOLD, Any, Attrs, LiveText, MAX_SHARED_PARAGRAPHS, MAX_SHARED_TEXT_BYTES, PRESENCE_CURSOR_THROTTLE_MS, PRESENCE_HEARTBEAT_INTERVAL_MS,
        PRESENCE_MAX_CLIENT_FRAME_BYTES, PresenceAnchor, PresenceCursor, PresenceCursorSchedule, PresenceHeartbeatSchedule, PresenceSocketEvent,
        ReconnectDecision, ReconnectSchedule, RelativeSelection, apply_local_text_command, encode_presence_cursor, is_current_presence_generation,
        is_current_socket_generation, one_splice, parse_presence_roster, parse_presence_session, presence_color, presence_heartbeat_json,
        presence_message_kind, resolve_presence_anchor, resolve_sticky, restore_editor, selection_format_value, shared_text_root, supported_body,
        supported_body_text, validate_shared_paragraphs,
    };
    use serde_json::json;
    use wordcraft_doc::{Block, CharProps, Document, Path, Pos};
    use wordcraft_engine::{Selection, Session};
    use yrs::updates::decoder::Decode;
    use yrs::updates::encoder::Encode;
    use yrs::{Assoc, Doc, GetString, IndexedSequence, ReadTxn, StateVector, StickyIndex, Text, Transact, Update};

    #[test]
    fn automatic_reconnect_uses_exponential_backoff_and_resets_after_sync() {
        let mut schedule = ReconnectSchedule::default();
        assert_eq!(schedule.tick(100), ReconnectDecision::Disabled);

        schedule.start();
        assert_eq!(schedule.tick(100), ReconnectDecision::Wait(500));
        assert_eq!(schedule.tick(599), ReconnectDecision::Wait(1));
        assert_eq!(schedule.tick(600), ReconnectDecision::RetryNow);
        assert_eq!(schedule.tick(600), ReconnectDecision::Wait(1_000));
        assert_eq!(schedule.tick(1_600), ReconnectDecision::RetryNow);
        assert_eq!(ReconnectSchedule::delay_ms(100), 30_000);

        schedule.connected();
        assert_eq!(schedule.tick(5_000), ReconnectDecision::Wait(500));
        schedule.pause();
        assert_eq!(schedule.tick(5_500), ReconnectDecision::Disabled);
    }

    #[test]
    fn presence_retry_schedule_restarts_after_text_disconnect_pauses_it() {
        let mut schedule = ReconnectSchedule::default();
        schedule.start();
        schedule.pause();
        assert_eq!(schedule.tick(10_000), ReconnectDecision::Disabled);

        // Reopening the presence socket from Inactive calls ensure_started after the durable
        // text socket returns to Live.
        schedule.ensure_started();
        assert_eq!(schedule.tick(10_000), ReconnectDecision::Wait(500));
        assert_eq!(schedule.tick(10_500), ReconnectDecision::RetryNow);
    }

    #[test]
    fn events_from_a_closed_socket_generation_are_ignored() {
        assert!(is_current_socket_generation(8, 8));
        assert!(!is_current_socket_generation(7, 8));
    }

    #[test]
    fn presence_roster_parser_accepts_only_the_roster_contract() {
        let sticky = encoded_empty_text_sticky("presence-test");
        let message = serde_json::json!({
            "kind": "roster",
            "participants": [
                {"id": "a1", "name": "Guest 1", "cursor": null},
                {"id": "b2", "name": "访客二", "cursor": {
                    "anchor": {"paragraph": 0, "sticky": sticky},
                    "focus": {"paragraph": 1, "sticky": encoded_empty_text_sticky("presence-test-2")}
                }}
            ]
        })
        .to_string();
        let participants = parse_presence_roster(&message).expect("valid anonymous roster parses");
        assert_eq!(participants.len(), 2);
        assert_eq!(participants[0].id, "a1");
        assert_eq!(participants[0].name, "Guest 1");
        assert_eq!(participants[1].name, "访客二");
        assert!(participants[0].cursor.is_none());
        assert!(participants[1].cursor.is_some());

        assert!(parse_presence_roster(r#"{"kind":"heartbeat","participants":[]}"#).is_err());
        assert!(parse_presence_roster(r#"{"kind":"roster","participants":[],"extra":true}"#).is_err());
        assert!(parse_presence_roster(r#"{"kind":"roster","participants":[{"id":"a1","name":"Guest"}]}"#).is_err());
        assert!(parse_presence_roster(r#"{"kind":"roster","participants":[{"id":"a1","name":"Guest","cursor":null,"extra":true}]}"#).is_err());
        let duplicate = serde_json::json!({"kind":"roster","participants":[
            {"id":"a1","name":"Guest","cursor":null}, {"id":"a1","name":"Guest 2","cursor":null}
        ]})
        .to_string();
        assert!(parse_presence_roster(&duplicate).is_err());
        let bad_paragraph = serde_json::json!({"kind":"roster","participants":[{"id":"a1","name":"Guest","cursor":{
            "anchor":{"paragraph":256,"sticky":encoded_empty_text_sticky("overflow")},
            "focus":{"paragraph":0,"sticky":encoded_empty_text_sticky("overflow")}
        }}]})
        .to_string();
        assert!(parse_presence_roster(&bad_paragraph).is_err());
        let bad_base64 = r#"{"kind":"roster","participants":[{"id":"a1","name":"Guest","cursor":{"anchor":{"paragraph":0,"sticky":"AQ*="},"focus":{"paragraph":0,"sticky":"AQ=="}}}]}"#;
        assert!(parse_presence_roster(bad_base64).is_err());
    }

    #[test]
    fn presence_session_handshake_is_strict_and_bounded() {
        assert_eq!(parse_presence_session(r#"{"kind":"session","id":"session-1"}"#).as_deref(), Ok("session-1"));
        assert_eq!(presence_message_kind(r#"{"kind":"session","id":"session-1"}"#).as_deref(), Ok("session"));
        assert!(parse_presence_session(r#"{"kind":"session","id":"session-1","extra":true}"#).is_err());
        assert!(parse_presence_session(&format!(r#"{{"kind":"session","id":"{}"}}"#, "x".repeat(1100))).is_err());
        assert!(presence_message_kind(r#"{"kind":"roster","participants":[],"extra":true}"#).is_ok());
        assert!(parse_presence_roster(&format!(r#"{{"kind":"roster","participants":[],"filler":"{}"}}"#, "x".repeat(65_000))).is_err());
    }

    #[test]
    fn presence_cursor_encoding_is_small_and_uses_paragraph_local_stickies() {
        let first = encoded_empty_text_sticky("cursor-paragraph-zero");
        let second = encoded_empty_text_sticky("cursor-paragraph-one");
        let decode = |encoded: &str| {
            let bytes = wordcraft_engine::cmd::insert::base64_decode(encoded).expect("base64 output");
            StickyIndex::decode_v1(&bytes).expect("encoded Yrs sticky position")
        };
        let selection =
            RelativeSelection { anchor_paragraph: 0, focus_paragraph: 1, anchor: decode(&first), focus: decode(&second), collapsed: false };
        let encoded = encode_presence_cursor(&selection).expect("valid cursor encodes");
        assert!(encoded.len() <= PRESENCE_MAX_CLIENT_FRAME_BYTES);
        let value: serde_json::Value = serde_json::from_str(&encoded).expect("cursor JSON");
        assert_eq!(value["kind"], "cursor");
        assert_eq!(value["anchor"]["paragraph"], 0);
        assert_eq!(value["focus"]["paragraph"], 1);
        assert!(value["anchor"]["sticky"].as_str().is_some_and(|sticky| sticky.len() <= 128));
    }

    #[test]
    fn presence_cursor_schedule_coalesces_and_resends_after_reconnect() {
        let mut schedule = PresenceCursorSchedule::default();
        schedule.queue("first".into(), 100);
        assert_eq!(schedule.due(100).as_deref(), Some("first"));
        schedule.queue("intermediate".into(), 120);
        schedule.queue("latest".into(), 150);
        assert_eq!(schedule.wait_ms(150), Some(PRESENCE_CURSOR_THROTTLE_MS - 50));
        assert_eq!(schedule.due(199), None);
        assert_eq!(schedule.due(200).as_deref(), Some("latest"));
        schedule.queue("next".into(), 210);
        schedule.queue("latest".into(), 220);
        assert_eq!(schedule.due(300), None, "unchanged selections are not resent on every frame");
        schedule.reset_for_reconnect();
        schedule.queue("latest".into(), 400);
        assert_eq!(schedule.due(400).as_deref(), Some("latest"));
        schedule.clear();
        assert_eq!(schedule.wait_ms(500), None);
    }

    #[test]
    fn presence_cursor_anchor_rebases_unicode_and_rejects_the_wrong_root() {
        let live = LiveText::with_paragraph_count(2).expect("fixed roots create");
        {
            let mut transaction = live.doc.transact_mut();
            live.texts[0].insert(&mut transaction, 0, "a😀bc");
            live.texts[1].insert(&mut transaction, 0, "second");
        }
        let sticky = {
            let transaction = live.doc.transact();
            live.texts[0].sticky_index(&transaction, 3, Assoc::After).expect("Unicode anchor at paragraph zero")
        };
        let anchor = PresenceAnchor { paragraph: 0, sticky: sticky.clone() };
        {
            let mut transaction = live.doc.transact_mut();
            live.texts[0].insert(&mut transaction, 0, "z");
        }
        let transaction = live.doc.transact();
        let resolved = resolve_presence_anchor(&live.texts, &live.paths, &anchor, &transaction).expect("rebased UTF-16 anchor resolves");
        assert_eq!(resolved, Pos::body(0, 6));
        let wrong_root = PresenceAnchor { paragraph: 1, sticky: sticky.clone() };
        assert!(resolve_presence_anchor(&live.texts, &live.paths, &wrong_root, &transaction).is_none());
        let cursor = PresenceCursor { anchor, focus: PresenceAnchor { paragraph: 0, sticky } };
        assert_eq!(live.resolve_presence_cursor(&cursor).expect("both endpoints resolve").0, resolved);
    }

    #[test]
    fn remote_presence_colors_are_deterministic_and_palette_based() {
        assert_eq!(presence_color("participant-id"), presence_color("participant-id"));
        assert_ne!(presence_color("participant-id"), presence_color("another-id"));
    }

    #[test]
    fn presence_heartbeat_schedule_sends_immediately_and_coalesces_delayed_ticks() {
        let mut schedule = PresenceHeartbeatSchedule::default();
        assert!(!schedule.due(100));
        schedule.start(100);
        assert!(schedule.due(100), "the first heartbeat is sent as soon as the socket opens");
        assert_eq!(schedule.wait_ms(100), Some(PRESENCE_HEARTBEAT_INTERVAL_MS));
        assert!(!schedule.due(100 + PRESENCE_HEARTBEAT_INTERVAL_MS - 1));
        assert!(schedule.due(100_000), "a delayed browser frame sends only one heartbeat");
        assert!(!schedule.due(100_000), "missed ticks are not replayed as a burst");
        assert_eq!(presence_heartbeat_json(), r#"{"kind":"heartbeat"}"#);
        schedule.pause();
        assert!(!schedule.due(200_000));
    }

    #[test]
    fn stale_presence_socket_events_are_ignored_by_generation() {
        let events = [
            PresenceSocketEvent::Open { generation: 4 },
            PresenceSocketEvent::Message { generation: 4, text: r#"{"kind":"roster","participants":[]}"#.into() },
            PresenceSocketEvent::BadMessage { generation: 4 },
            PresenceSocketEvent::Closed { generation: 4 },
            PresenceSocketEvent::Error { generation: 4 },
        ];
        for event in events {
            assert_eq!(event.generation(), 4);
            assert!(is_current_presence_generation(event.generation(), 4));
            assert!(!is_current_presence_generation(event.generation(), 5));
        }
    }

    #[test]
    fn document_switch_discards_queued_events_and_unconfirmed_updates() {
        let mut events = std::collections::VecDeque::from([1, 2]);
        let mut pending_updates = std::collections::VecDeque::from([vec![3, 4], vec![5, 6]]);

        super::clear_stale_document_queues(&mut events, &mut pending_updates);

        assert!(events.is_empty());
        assert!(pending_updates.is_empty());
    }

    fn encoded_empty_text_sticky(root: &str) -> String {
        let doc = Doc::new();
        let text = doc.get_or_insert_text(root);
        let transaction = doc.transact();
        let sticky = StickyIndex::from_type(&transaction, &text, Assoc::After);
        wordcraft_engine::cmd::insert::base64_encode(&sticky.encode_v1())
    }

    fn seed(text: &str) -> Vec<u8> {
        seed_paragraphs(&[text])
    }

    fn seed_paragraphs(paragraphs: &[&str]) -> Vec<u8> {
        let doc = Doc::new();
        for (index, value) in paragraphs.iter().enumerate() {
            let path = Path::top(index);
            let shared = doc.get_or_insert_text(shared_text_root(index, &path));
            shared.insert(&mut doc.transact_mut(), 0, value);
        }
        doc.transact().encode_state_as_update_v1(&StateVector::default())
    }

    fn table_body_document() -> Document {
        let mut document = Document::from_text("before\nafter");
        document
            .insert_block(wordcraft_doc::StoryRef::Body, &Path::top(1), Block::Table(wordcraft_doc::Table::new(1, 2, 360.0)))
            .expect("table inserts into body");
        let first_cell_second_para = Path(vec![1, 0, 0, 1]);
        document
            .insert_block(wordcraft_doc::StoryRef::Body, &first_cell_second_para, Block::Para(wordcraft_doc::Paragraph::new()))
            .expect("second paragraph inserts in first cell");
        for (path, text) in [
            (Path::top(0), "before"),
            (Path(vec![1, 0, 0, 0]), "left"),
            (Path(vec![1, 0, 0, 1]), "cell second"),
            (Path(vec![1, 0, 1, 0]), "right"),
            (Path::top(2), "after"),
        ] {
            let paragraph = document.para_mut(wordcraft_doc::StoryRef::Body, &path).expect("manifest path resolves");
            let old_len = paragraph.len();
            paragraph.delete(0, old_len).expect("fixture paragraph clears");
            paragraph.insert_text(0, text, &CharProps::default()).expect("paragraph text inserts");
        }
        document
    }

    fn seed_document_roots(document: &Document) -> Vec<u8> {
        let paragraphs = supported_body(document).expect("supported document flattens");
        let shared = Doc::with_options(super::DocOptions { offset_kind: super::OffsetKind::Utf16, ..super::DocOptions::default() });
        let texts: Vec<_> =
            paragraphs.iter().enumerate().map(|(index, paragraph)| shared.get_or_insert_text(shared_text_root(index, &paragraph.path))).collect();
        let mut transaction = shared.transact_mut();
        for (paragraph, text) in paragraphs.iter().zip(&texts) {
            text.insert(&mut transaction, 0, &paragraph.text);
        }
        drop(transaction);
        shared.transact().encode_state_as_update_v1(&StateVector::default())
    }

    fn formatted_seed(text: &str, start: u32, len: u32, attribute: &str, value: bool) -> Vec<u8> {
        formatted_paragraph_seed(&[text], 0, start, len, attribute, value)
    }

    fn formatted_paragraph_seed(paragraphs: &[&str], paragraph_index: usize, start: u32, len: u32, attribute: &str, value: bool) -> Vec<u8> {
        let doc = Doc::with_options(super::DocOptions { offset_kind: super::OffsetKind::Utf16, ..super::DocOptions::default() });
        let shared_texts: Vec<_> = paragraphs
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let path = Path::top(index);
                doc.get_or_insert_text(shared_text_root(index, &path))
            })
            .collect();
        let mut transaction = doc.transact_mut();
        for (index, (text, shared)) in paragraphs.iter().zip(&shared_texts).enumerate() {
            shared.insert(&mut transaction, 0, text);
            if index == paragraph_index {
                let attributes = Attrs::from([(std::sync::Arc::from(attribute), Any::Bool(value))]);
                shared.format(&mut transaction, start, len, attributes);
            }
        }
        drop(transaction);
        doc.transact().encode_state_as_update_v1(&StateVector::default())
    }

    fn remote_insert_update(initial: &[u8], paragraph_count: usize, inserts: &[(usize, u32, &str)]) -> Vec<u8> {
        let remote = Doc::with_options(super::DocOptions { offset_kind: super::OffsetKind::Utf16, ..super::DocOptions::default() });
        let texts: Vec<_> = (0..paragraph_count)
            .map(|index| {
                let path = Path::top(index);
                remote.get_or_insert_text(shared_text_root(index, &path))
            })
            .collect();
        remote.transact_mut().apply_update(Update::decode_v1(initial).expect("initial state decodes")).expect("initial state applies");
        let state = remote.transact().state_vector();
        {
            let mut transaction = remote.transact_mut_with(remote.client_id());
            for (paragraph_index, offset, inserted) in inserts {
                texts[*paragraph_index].insert(&mut transaction, *offset, inserted);
            }
        }
        remote.transact().encode_state_as_update_v1(&state)
    }

    fn props_at(document: &Document, offset: usize) -> CharProps {
        props_at_paragraph(document, 0, offset)
    }

    fn props_at_paragraph(document: &Document, paragraph_index: usize, offset: usize) -> CharProps {
        let Some(Block::Para(paragraph)) = document.body.get(paragraph_index).map(|block| &**block) else { return CharProps::default() };
        paragraph.run_ranges().find(|(range, _)| range.start <= offset && offset < range.end).map(|(_, props)| props.clone()).unwrap_or_default()
    }

    fn edit(adapter: &mut LiveText, session: &mut Session, id: &str, params: serde_json::Value) -> Vec<u8> {
        let before = supported_body(&session.doc).expect("eligible body before edit");
        let paragraph_index = super::selected_paragraph_index(&session.sel, &adapter.paths).expect("selection is in one paragraph");
        session.run(id, &params).expect("editor command succeeds");
        let paragraph_path = before[paragraph_index].path.clone();
        super::clear_paragraph_mark(&mut session.doc, &paragraph_path).expect("paragraph mark remains supported");
        adapter.apply_local_editor_change(session, &before, paragraph_index).expect("local edit becomes a CRDT update").unwrap_or_default()
    }

    fn update(adapter: &mut LiveText, session: &mut Session, bytes: &[u8]) {
        adapter.apply_remote_update(session, bytes).expect("remote update applies");
    }

    fn live_for(paragraph_count: usize) -> LiveText {
        LiveText::with_paragraph_count(paragraph_count).expect("fixed-paragraph adapter initializes")
    }

    fn seeded_client(document: &Document, initial: &[u8]) -> (Session, LiveText) {
        let mut session = Session::new(document.clone());
        let mut adapter = LiveText::with_document(document).expect("fixed body manifest initializes");
        adapter.apply_full_state(&mut session, initial).expect("server state applies");
        (session, adapter)
    }

    #[test]
    fn accepts_fixed_plain_text_body_paragraphs_with_supported_character_styles() {
        assert_eq!(supported_body_text(&Document::from_text("hello")), Some(vec!["hello".into()]));
        assert_eq!(supported_body_text(&Document::from_text("one\ntwo")), Some(vec!["one".into(), "two".into()]));
        let too_many = Document::from_text(&vec![""; MAX_SHARED_PARAGRAPHS + 1].join("\n"));
        assert!(supported_body_text(&too_many).is_none());
        let mut formatted = Document::from_text("hello");
        {
            let Block::Para(paragraph) = std::sync::Arc::make_mut(&mut formatted.body[0]) else { return };
            paragraph.runs[0].props = CharProps { bold: Some(true), ..CharProps::default() };
        }
        assert_eq!(supported_body_text(&formatted), Some(vec!["hello".into()]));
        {
            let Block::Para(paragraph) = std::sync::Arc::make_mut(&mut formatted.body[0]) else { return };
            paragraph.runs[0].props.italic = Some(false);
            paragraph.runs[0].props.underline = Some(wordcraft_doc::props::Underline::Single);
        }
        assert_eq!(supported_body_text(&formatted), Some(vec!["hello".into()]));
        {
            let Block::Para(paragraph) = std::sync::Arc::make_mut(&mut formatted.body[0]) else { return };
            paragraph.runs[0].props.underline = Some(wordcraft_doc::props::Underline::Double);
        }
        assert!(supported_body_text(&formatted).is_none());
        {
            let Block::Para(paragraph) = std::sync::Arc::make_mut(&mut formatted.body[0]) else { return };
            paragraph.runs[0].props.font = Some("Aptos".into());
        }
        assert!(supported_body_text(&formatted).is_none());
    }

    #[test]
    fn fixed_manifest_flattens_one_level_table_cells_and_preserves_legacy_roots() {
        let document = table_body_document();
        let paragraphs = supported_body(&document).expect("simple body table is supported");
        let paths: Vec<_> = paragraphs.iter().map(|paragraph| paragraph.path.clone()).collect();
        assert_eq!(paths, vec![Path::top(0), Path(vec![1, 0, 0, 0]), Path(vec![1, 0, 0, 1]), Path(vec![1, 0, 1, 0]), Path::top(2),]);
        assert_eq!(shared_text_root(0, &paths[0]), "body:main");
        assert_eq!(shared_text_root(1, &paths[1]), "body:cell-paragraph:1/0/0/0");
        assert_eq!(shared_text_root(2, &paths[2]), "body:cell-paragraph:1/0/0/1");
        assert_eq!(shared_text_root(4, &paths[4]), "body:paragraph:4");
        assert_eq!(shared_text_root(1, &Path::top(1)), "body:paragraph:1");
        assert_eq!(
            supported_body_text(&document),
            Some(vec!["before", "left", "cell second", "right", "after"].into_iter().map(str::to_owned).collect())
        );
    }

    #[test]
    fn live_body_preflight_rejects_nested_merged_oversized_and_overwide_tables() {
        let mut nested = table_body_document();
        nested
            .insert_block(wordcraft_doc::StoryRef::Body, &Path(vec![1, 0, 0, 1]), Block::Table(wordcraft_doc::Table::new(1, 1, 100.0)))
            .expect("nested table is representable in the editor");
        assert!(supported_body(&nested).is_none());

        let mut spanning = table_body_document();
        spanning.table_mut(wordcraft_doc::StoryRef::Body, &Path::top(1)).expect("table exists").rows[0].cells[0].props.span = 2;
        assert!(supported_body(&spanning).is_none());

        let mut vertically_merged = table_body_document();
        vertically_merged.table_mut(wordcraft_doc::StoryRef::Body, &Path::top(1)).expect("table exists").rows[0].cells[0].props.vmerge =
            wordcraft_doc::props::VMerge::Restart;
        assert!(supported_body(&vertically_merged).is_none());

        let mut too_many_cells = Document::from_text("body");
        too_many_cells
            .insert_block(wordcraft_doc::StoryRef::Body, &Path::top(1), Block::Table(wordcraft_doc::Table::new(5, 63, 630.0)))
            .expect("large table inserts");
        assert!(supported_body(&too_many_cells).is_none());

        let mut too_wide = table_body_document();
        too_wide.table_mut(wordcraft_doc::StoryRef::Body, &Path::top(1)).expect("table exists").grid.resize(64, 1.0);
        assert!(supported_body(&too_wide).is_none());
    }

    #[test]
    fn editing_and_convergence_work_inside_a_fixed_table_cell() {
        let document = table_body_document();
        let initial = seed_document_roots(&document);
        let (mut alice_session, mut alice) = seeded_client(&document, &initial);
        let (mut bob_session, mut bob) = seeded_client(&document, &initial);

        let cell_path = Path(vec![1, 0, 0, 0]);
        alice_session.sel = Selection::caret(Pos::new(wordcraft_doc::StoryRef::Body, cell_path, "left".len()));
        let inserted = edit(&mut alice, &mut alice_session, "text.insert", json!({"text":"X", "raw":true}));
        update(&mut bob, &mut bob_session, &inserted);

        let alice_body = supported_body(&alice_session.doc).expect("Alice keeps the same topology");
        let bob_body = supported_body(&bob_session.doc).expect("Bob keeps the same topology");
        assert_eq!(alice_body, bob_body);
        assert_eq!(alice_body.get(1).map(|paragraph| paragraph.text.as_str()), Some("leftX"));
        assert_eq!(alice_body.get(2).map(|paragraph| paragraph.text.as_str()), Some("cell second"));
        assert_eq!(alice_body.get(3).map(|paragraph| paragraph.text.as_str()), Some("right"));
        assert!(alice.editor_matches_shared(&alice_session.doc).expect("cell root stays in sync"));
        assert!(bob.editor_matches_shared(&bob_session.doc).expect("remote cell root stays in sync"));
    }

    #[test]
    fn selected_cell_format_converges_between_clients() {
        let document = table_body_document();
        let initial = seed_document_roots(&document);
        let (mut alice_session, mut alice) = seeded_client(&document, &initial);
        let (mut bob_session, mut bob) = seeded_client(&document, &initial);

        let cell_path = Path(vec![1, 0, 0, 0]);
        let cell_text = "left";
        alice_session.sel = Selection {
            anchor: Pos::new(wordcraft_doc::StoryRef::Body, cell_path.clone(), 0),
            focus: Pos::new(wordcraft_doc::StoryRef::Body, cell_path.clone(), cell_text.len()),
        };
        alice_session.run("format.bold", &json!({"value":true})).expect("Alice formats the cell selection");
        super::clear_paragraph_mark(&mut alice_session.doc, &cell_path).expect("cell paragraph mark remains unformatted");
        let value = selection_format_value(&alice_session.doc, &cell_path, "format.bold", 0, cell_text.len())
            .expect("cell selection has one supported bold value");
        let format_update = alice
            .apply_format_update(1, "format.bold", 0, cell_text.len(), value)
            .expect("cell format update encodes")
            .expect("cell format update changes the shared state");
        update(&mut bob, &mut bob_session, &format_update);

        for session in [&alice_session, &bob_session] {
            let paragraph = session.doc.para(wordcraft_doc::StoryRef::Body, &cell_path).expect("formatted cell paragraph exists");
            assert!(paragraph.run_ranges().all(|(_, props)| props.bold == Some(true)));
        }
        assert!(alice.editor_matches_shared(&alice_session.doc).expect("Alice cell formatting matches shared text"));
        assert!(bob.editor_matches_shared(&bob_session.doc).expect("Bob cell formatting matches shared text"));
    }

    #[test]
    fn table_cell_selection_and_remote_cursor_keep_path_through_unicode_rebase() {
        let mut document = table_body_document();
        let cell_path = Path(vec![1, 0, 0, 1]);
        {
            let paragraph = document.para_mut(wordcraft_doc::StoryRef::Body, &cell_path).expect("cell paragraph exists");
            paragraph.delete(0, paragraph.len()).expect("old cell text clears");
            paragraph.insert_text(0, "a😀bc", &CharProps::default()).expect("Unicode cell text inserts");
        }
        let initial = seed_document_roots(&document);
        let (mut session, live) = seeded_client(&document, &initial);
        session.sel = Selection::caret(Pos::new(wordcraft_doc::StoryRef::Body, cell_path.clone(), 5));

        let selection = live.capture_selection(&session).expect("local selection captures from the cell path");
        assert_eq!(selection.anchor_paragraph, 2, "cursor paragraph is the frozen flattened ordinal");
        let cursor = PresenceCursor {
            anchor: PresenceAnchor { paragraph: selection.anchor_paragraph, sticky: selection.anchor.clone() },
            focus: PresenceAnchor { paragraph: selection.focus_paragraph, sticky: selection.focus.clone() },
        };
        let (anchor, focus) = live.resolve_presence_cursor(&cursor).expect("remote anchors map back to cell paths");
        assert_eq!(anchor, Pos::new(wordcraft_doc::StoryRef::Body, cell_path.clone(), 5));
        assert_eq!(focus, anchor);
        let encoded: serde_json::Value =
            serde_json::from_str(&encode_presence_cursor(&selection).expect("cell cursor encodes")).expect("cursor JSON parses");
        assert_eq!(encoded["anchor"]["paragraph"], 2);

        {
            let mut transaction = live.doc.transact_mut();
            live.texts[selection.anchor_paragraph].insert(&mut transaction, 0, "z");
        }
        let (rebased, _) = live.resolve_presence_cursor(&cursor).expect("cursor remains resolvable after a Unicode-adjacent insert");
        assert_eq!(rebased, Pos::new(wordcraft_doc::StoryRef::Body, cell_path, 6));
    }

    #[test]
    fn table_structure_changes_are_rejected_before_shared_roots_change() {
        let document = table_body_document();
        let initial = seed_document_roots(&document);
        let (mut session, mut live) = seeded_client(&document, &initial);
        let before = supported_body(&session.doc).expect("supported initial body");
        let shared_before = live.shared_paragraphs().expect("shared text reads");
        let original_document = session.doc.clone();
        let original_selection = session.sel.clone();

        session
            .doc
            .insert_block(wordcraft_doc::StoryRef::Body, &Path(vec![1, 0, 0, 2]), Block::Para(wordcraft_doc::Paragraph::new()))
            .expect("attempted cell paragraph insertion is representable");
        let target = live.paths.iter().position(|path| path == &Path(vec![1, 0, 0, 1])).expect("existing cell path is in manifest");
        assert!(live.apply_local_editor_change(&mut session, &before, target).is_err());
        assert_eq!(live.shared_paragraphs().expect("Yrs roots remain readable"), shared_before);
        restore_editor(&mut session, original_document, original_selection);
        assert!(live.matches_manifest(&session.doc), "the editor rollback restores the fixed path manifest");
    }

    #[test]
    fn table_structure_commands_roll_back_through_the_editor_wrapper() {
        let document = table_body_document();
        let initial = seed_document_roots(&document);
        let (mut session, mut live) = seeded_client(&document, &initial);
        let cell_path = Path(vec![1, 0, 0, 0]);
        session.sel = Selection::caret(Pos::new(wordcraft_doc::StoryRef::Body, cell_path, 2));
        let original_document = session.doc.clone();
        let original_selection = session.sel.clone();
        let shared_before = live.shared_paragraphs().expect("shared table roots read");

        let error = apply_local_text_command(&mut live, &mut session, "table.insertRowBelow", &json!({}))
            .expect_err("table structure edits are rejected during live text");

        assert!(error.contains("structure changed"), "the user receives a clear structure rejection");
        assert_eq!(session.doc, original_document, "the production wrapper restores the original table");
        assert_eq!(session.sel, original_selection, "the production wrapper restores the cell selection");
        assert_eq!(live.shared_paragraphs().expect("shared table roots remain readable"), shared_before);
        assert!(live.editor_matches_shared(&session.doc).expect("the restored editor still matches the fixed manifest"));
    }

    #[test]
    fn formatted_server_state_projects_supported_runs_into_the_existing_document() {
        let value = "前😀後";
        let initial = formatted_seed(value, 1, 2, ATTR_BOLD, true);
        let mut session = Session::new(Document::from_text(value));
        let mut adapter = live_for(1);

        adapter.apply_full_state(&mut session, &initial).expect("formatted server state applies");

        assert_eq!(supported_body_text(&session.doc), Some(vec![value.into()]));
        assert_eq!(props_at(&session.doc, "前".len()).bold, Some(true));
        assert_eq!(props_at(&session.doc, "前😀".len()).bold, None);
    }

    #[test]
    fn two_paragraph_seed_projects_formatted_runs_from_the_matching_root() {
        let paragraphs = ["前😀後", "bold here"];
        let initial = formatted_paragraph_seed(&paragraphs, 1, 0, 4, ATTR_BOLD, true);
        let mut session = Session::new(Document::from_text(&paragraphs.join("\n")));
        let mut adapter = live_for(paragraphs.len());

        adapter.apply_full_state(&mut session, &initial).expect("multi-paragraph state applies");

        assert_eq!(supported_body_text(&session.doc), Some(paragraphs.iter().map(|text| (*text).to_string()).collect()));
        assert_eq!(props_at_paragraph(&session.doc, 0, 0).bold, None);
        assert_eq!(props_at_paragraph(&session.doc, 1, 0).bold, Some(true));
        assert_eq!(props_at_paragraph(&session.doc, 1, 4).bold, None);
        assert!(adapter.editor_matches_shared(&session.doc).expect("the projected paragraphs match the roots"));
    }

    #[test]
    fn typing_inside_a_formatted_run_preserves_character_styles_for_both_clients() {
        let initial = formatted_seed("ab", 0, 2, ATTR_BOLD, true);
        let mut alice_session = Session::new(Document::from_text("ab"));
        let mut bob_session = Session::new(Document::from_text("ab"));
        let mut alice = live_for(1);
        let mut bob = live_for(1);
        alice.apply_full_state(&mut alice_session, &initial).expect("Alice receives formatted seed");
        bob.apply_full_state(&mut bob_session, &initial).expect("Bob receives formatted seed");

        alice_session.sel = Selection::caret(wordcraft_doc::Pos::body(0, 1));
        let alice_insert = edit(&mut alice, &mut alice_session, "text.insert", json!({"text":"X", "raw":true}));
        update(&mut bob, &mut bob_session, &alice_insert);

        assert_eq!(supported_body_text(&alice_session.doc), Some(vec!["aXb".into()]));
        assert_eq!(supported_body_text(&bob_session.doc), Some(vec!["aXb".into()]));
        for offset in 0..3 {
            assert_eq!(props_at(&alice_session.doc, offset).bold, Some(true));
            assert_eq!(props_at(&bob_session.doc, offset).bold, Some(true));
        }
        assert!(alice.editor_matches_shared(&alice_session.doc).expect("Alice matches shared styles"));
        assert!(bob.editor_matches_shared(&bob_session.doc).expect("Bob matches shared styles"));
    }

    #[test]
    fn concurrent_edits_in_different_paragraph_roots_converge() {
        let initial = seed_paragraphs(&["one", "two"]);
        let mut alice_session = Session::new(Document::from_text("one\ntwo"));
        let mut bob_session = Session::new(Document::from_text("one\ntwo"));
        let mut alice = live_for(2);
        let mut bob = live_for(2);
        alice.apply_full_state(&mut alice_session, &initial).expect("Alice receives both paragraph roots");
        bob.apply_full_state(&mut bob_session, &initial).expect("Bob receives both paragraph roots");

        alice_session.sel = Selection::caret(Pos::body(0, 3));
        bob_session.sel = Selection::caret(Pos::body(1, 3));
        let alice_update = edit(&mut alice, &mut alice_session, "text.insert", json!({"text":"A", "raw":true}));
        let bob_update = edit(&mut bob, &mut bob_session, "text.insert", json!({"text":"B", "raw":true}));

        update(&mut alice, &mut alice_session, &bob_update);
        update(&mut bob, &mut bob_session, &alice_update);

        let expected = Some(vec!["oneA".to_string(), "twoB".to_string()]);
        assert_eq!(supported_body_text(&alice_session.doc), expected);
        assert_eq!(supported_body_text(&bob_session.doc), expected);
        assert!(alice.editor_matches_shared(&alice_session.doc).expect("Alice matches both roots"));
        assert!(bob.editor_matches_shared(&bob_session.doc).expect("Bob matches both roots"));
    }

    #[test]
    fn remote_edit_only_invalidates_the_changed_paragraph_layout() {
        let initial = seed_paragraphs(&["stable", "target"]);
        let mut session = Session::new(Document::from_text("stable\ntarget"));
        let mut adapter = live_for(2);
        adapter.apply_full_state(&mut session, &initial).expect("both roots seed");
        let first_revision = session.doc.body.first().and_then(|block| block.as_para()).expect("first paragraph exists").rev;
        let second_revision = session.doc.body.get(1).and_then(|block| block.as_para()).expect("second paragraph exists").rev;

        let remote_update = remote_insert_update(&initial, 2, &[(1, 1, "!")]);
        update(&mut adapter, &mut session, &remote_update);

        let updated_first_revision = session.doc.body.first().and_then(|block| block.as_para()).expect("first paragraph remains").rev;
        let updated_second_revision = session.doc.body.get(1).and_then(|block| block.as_para()).expect("second paragraph remains").rev;
        assert_eq!(updated_first_revision, first_revision, "an unchanged paragraph keeps its layout cache key");
        assert!(updated_second_revision > second_revision, "the remotely edited paragraph is invalidated");
        assert_eq!(supported_body_text(&session.doc), Some(vec!["stable".into(), "t!arget".into()]));
    }

    #[test]
    fn concurrent_same_root_inserts_converge_and_actor_undo_isolated_across_roots() {
        let initial = seed_paragraphs(&["x", "y"]);
        let mut alice_session = Session::new(Document::from_text("x\ny"));
        let mut bob_session = Session::new(Document::from_text("x\ny"));
        let mut alice = live_for(2);
        let mut bob = live_for(2);
        alice.apply_full_state(&mut alice_session, &initial).expect("Alice receives both roots");
        bob.apply_full_state(&mut bob_session, &initial).expect("Bob receives both roots");

        alice_session.sel = Selection::caret(Pos::body(0, 0));
        bob_session.sel = Selection::caret(Pos::body(0, 0));
        let alice_insert = edit(&mut alice, &mut alice_session, "text.insert", json!({"text":"A", "raw":true}));
        let bob_insert = edit(&mut bob, &mut bob_session, "text.insert", json!({"text":"B", "raw":true}));
        update(&mut alice, &mut alice_session, &bob_insert);
        update(&mut bob, &mut bob_session, &alice_insert);
        assert_eq!(supported_body_text(&alice_session.doc), supported_body_text(&bob_session.doc));

        bob_session.sel = Selection::caret(Pos::body(1, 1));
        let remote_other_root = edit(&mut bob, &mut bob_session, "text.insert", json!({"text":"R", "raw":true}));
        update(&mut alice, &mut alice_session, &remote_other_root);
        assert_eq!(supported_body_text(&alice_session.doc), Some(vec![supported_body_text(&bob_session.doc).unwrap()[0].clone(), "yR".into()]));

        let undo = alice.undo_or_redo(&mut alice_session, false).expect("local actor undo succeeds").expect("Alice's insert is undoable");
        update(&mut bob, &mut bob_session, &undo);

        let alice_body = supported_body_text(&alice_session.doc).expect("Alice keeps a supported body");
        let bob_body = supported_body_text(&bob_session.doc).expect("Bob keeps a supported body");
        assert_eq!(alice_body, bob_body);
        assert!(alice_body[0].contains('B'), "Bob's same-root insert must remain after Alice undoes hers");
        assert!(!alice_body[0].contains('A'), "Alice's local insert should be undone");
        assert_eq!(alice_body[1], "yR", "undo in paragraph 0 must preserve the remote edit in paragraph 1");
    }

    #[test]
    fn formatting_a_selection_updates_only_its_paragraph_root() {
        let initial = seed_paragraphs(&["first", "bold"]);
        let mut alice_session = Session::new(Document::from_text("first\nbold"));
        let mut bob_session = Session::new(Document::from_text("first\nbold"));
        let mut alice = live_for(2);
        let mut bob = live_for(2);
        alice.apply_full_state(&mut alice_session, &initial).expect("Alice receives both roots");
        bob.apply_full_state(&mut bob_session, &initial).expect("Bob receives both roots");

        alice_session.sel = Selection { anchor: Pos::body(1, 0), focus: Pos::body(1, 4) };
        alice_session.run("format.bold", &json!({"value":true})).expect("selection formats");
        let second_path = Path::top(1);
        super::clear_paragraph_mark(&mut alice_session.doc, &second_path).expect("paragraph mark resets");
        let bold = selection_format_value(&alice_session.doc, &second_path, "format.bold", 0, 4).expect("formatted selection reads back");
        let format_update =
            alice.apply_format_update(1, "format.bold", 0, 4, bold).expect("format update encodes").expect("second-root format changes shared state");
        update(&mut bob, &mut bob_session, &format_update);

        assert_eq!(supported_body_text(&alice_session.doc), Some(vec!["first".into(), "bold".into()]));
        assert_eq!(props_at_paragraph(&alice_session.doc, 0, 0).bold, None);
        assert_eq!(props_at_paragraph(&alice_session.doc, 1, 0).bold, Some(true));
        assert_eq!(props_at_paragraph(&bob_session.doc, 1, 0).bold, Some(true));
        assert!(alice.editor_matches_shared(&alice_session.doc).expect("Alice matches the two roots"));
        assert!(bob.editor_matches_shared(&bob_session.doc).expect("Bob matches the two roots"));
    }

    #[test]
    fn unicode_selection_stays_on_its_root_and_rebases_through_remote_inserts() {
        let initial = seed_paragraphs(&["first", "前😀後"]);
        let mut session = Session::new(Document::from_text("first\n前😀後"));
        let mut local = live_for(2);
        local.apply_full_state(&mut session, &initial).expect("both roots seed");
        session.sel = Selection { anchor: Pos::body(1, "前😀".len()), focus: Pos::body(0, "fi".len()) };

        let remote_update = remote_insert_update(&initial, 2, &[(0, 0, "遠"), (1, 1, "乙")]);
        update(&mut local, &mut session, &remote_update);

        assert_eq!(supported_body_text(&session.doc), Some(vec!["遠first".into(), "前乙😀後".into()]));
        assert_eq!(session.sel.anchor.path, wordcraft_doc::Path::top(1));
        assert_eq!(session.sel.focus.path, wordcraft_doc::Path::top(0));
        assert!(session.sel.anchor.off > session.sel.focus.off, "reversed selection direction is retained");
        assert_eq!(session.sel.anchor.off, "前乙😀".len());
        assert_eq!(session.sel.focus.off, "遠fi".len());
    }

    #[test]
    fn paragraph_breaks_merges_and_cross_paragraph_deletes_are_rejected_without_editor_changes() {
        let initial = seed_paragraphs(&["one", "two"]);
        let mut session = Session::new(Document::from_text("one\ntwo"));
        let mut adapter = live_for(2);
        adapter.apply_full_state(&mut session, &initial).expect("both roots seed");

        for (selection, id, params) in [
            (Selection::caret(Pos::body(0, 1)), "text.insert", json!({"text":"\n"})),
            (Selection::caret(Pos::body(1, 0)), "text.backspace", json!({})),
            (Selection::caret(Pos::body(0, 3)), "text.delete", json!({})),
            (Selection { anchor: Pos::body(0, 0), focus: Pos::body(1, 1) }, "text.delete", json!({})),
        ] {
            session.sel = selection.clone();
            let before_doc = session.doc.clone();
            let before_selection = session.sel.clone();
            let error = apply_local_text_command(&mut adapter, &mut session, id, &params).expect_err("structural edit is blocked");
            assert!(!error.is_empty());
            assert_eq!(session.doc, before_doc, "a rejected command leaves all paragraphs unchanged");
            assert_eq!(session.sel, before_selection, "a rejected command leaves the visible selection unchanged");
            assert_eq!(adapter.body_texts().expect("shared roots remain readable"), vec!["one", "two"]);
        }
    }

    #[test]
    fn live_format_update_syncs_and_actor_undo_preserves_remote_text() {
        let initial = seed("text");
        let mut alice_session = Session::new(Document::from_text("text"));
        let mut bob_session = Session::new(Document::from_text("text"));
        let mut alice = live_for(1);
        let mut bob = live_for(1);
        alice.apply_full_state(&mut alice_session, &initial).expect("Alice receives seed");
        bob.apply_full_state(&mut bob_session, &initial).expect("Bob receives seed");

        bob_session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "text".len()));
        let bob_insert = edit(&mut bob, &mut bob_session, "text.insert", json!({"text":" B", "raw":true}));
        update(&mut alice, &mut alice_session, &bob_insert);
        alice_session.sel = Selection { anchor: wordcraft_doc::Pos::body(0, 0), focus: wordcraft_doc::Pos::body(0, "text".len()) };
        alice_session.run("format.bold", &json!({"value":true})).expect("Alice formats selection");
        let first_path = Path::top(0);
        super::clear_paragraph_mark(&mut alice_session.doc, &first_path).expect("paragraph mark remains unformatted");
        let bold = selection_format_value(&alice_session.doc, &first_path, "format.bold", 0, "text".len()).expect("selection has bold value");
        let format_update =
            alice.apply_format_update(0, "format.bold", 0, "text".len(), bold).expect("format update encodes").expect("format changes shared state");
        update(&mut bob, &mut bob_session, &format_update);
        for (id, params) in [("format.italic", json!({"value":true})), ("format.underline", json!({"value":true}))] {
            alice_session.run(id, &params).expect("Alice applies another supported character style");
            super::clear_paragraph_mark(&mut alice_session.doc, &first_path).expect("paragraph mark remains unformatted");
            let value = selection_format_value(&alice_session.doc, &first_path, id, 0, "text".len()).expect("selection has the applied style");
            let format_update =
                alice.apply_format_update(0, id, 0, "text".len(), value).expect("format update encodes").expect("style changes shared state");
            update(&mut bob, &mut bob_session, &format_update);
        }
        assert_eq!(supported_body_text(&alice_session.doc), Some(vec!["text B".into()]));
        assert_eq!(supported_body_text(&bob_session.doc), Some(vec!["text B".into()]));
        assert_eq!(props_at(&bob_session.doc, 0).bold, Some(true));
        assert_eq!(props_at(&bob_session.doc, 0).italic, Some(true));
        assert_eq!(props_at(&bob_session.doc, 0).underline, Some(wordcraft_doc::props::Underline::Single));
        assert_eq!(props_at(&bob_session.doc, "text ".len()).bold, None);

        let undo = alice.undo_or_redo(&mut alice_session, false).expect("format undo succeeds").expect("format undo emits update");
        update(&mut bob, &mut bob_session, &undo);
        assert_eq!(supported_body_text(&alice_session.doc), Some(vec!["text B".into()]));
        assert_eq!(supported_body_text(&bob_session.doc), Some(vec!["text B".into()]));
        assert_eq!(props_at(&bob_session.doc, 0), CharProps::default());
        assert_eq!(props_at(&bob_session.doc, "text ".len()), CharProps::default());

        let redo = alice.undo_or_redo(&mut alice_session, true).expect("format redo succeeds").expect("format redo emits update");
        update(&mut bob, &mut bob_session, &redo);
        assert_eq!(supported_body_text(&alice_session.doc), Some(vec!["text B".into()]));
        assert_eq!(supported_body_text(&bob_session.doc), Some(vec!["text B".into()]));
        assert_eq!(props_at(&alice_session.doc, 0).bold, Some(true));
        assert_eq!(props_at(&bob_session.doc, 0).bold, Some(true));
    }

    #[test]
    fn deleting_all_formatted_text_keeps_an_empty_supported_paragraph() {
        let initial = formatted_seed("bold", 0, 4, super::ATTR_BOLD, true);
        let mut session = Session::new(Document::from_text("bold"));
        let mut adapter = live_for(1);
        adapter.apply_full_state(&mut session, &initial).expect("formatted server state applies");
        session.sel = Selection { anchor: wordcraft_doc::Pos::body(0, 0), focus: wordcraft_doc::Pos::body(0, 4) };
        let before = supported_body(&session.doc).expect("supported body before deletion");
        session.run("text.delete", &json!({})).expect("selected text deletes");
        super::clear_paragraph_mark(&mut session.doc, &Path::top(0)).expect("paragraph mark resets");

        let update_bytes =
            adapter.apply_local_editor_change(&mut session, &before, 0).expect("deletion becomes a CRDT update").expect("delete emits update");
        assert_eq!(supported_body_text(&session.doc), Some(vec![String::new()]));
        assert!(!update_bytes.is_empty());
        assert_eq!(adapter.body_texts().expect("live text reads"), vec![String::new()]);
    }

    #[test]
    fn one_splice_respects_utf8_character_boundaries() {
        assert_eq!(one_splice("前後", "前甲後"), Some((3, 3, "甲")));
        assert_eq!(one_splice("a👩‍💻z", "a👩‍💻Xz"), Some(("a👩‍💻".len(), "a👩‍💻".len(), "X")));
        assert_eq!(one_splice("same", "same"), None);
    }

    #[test]
    fn two_editor_adapters_converge_and_local_undo_preserves_remote_text() {
        let initial = seed("前後");
        let mut alice_session = Session::new(Document::from_text("前後"));
        let mut bob_session = Session::new(Document::from_text("前後"));
        let mut alice = live_for(1);
        let mut bob = live_for(1);
        alice.apply_full_state(&mut alice_session, &initial).expect("Alice receives server state");
        bob.apply_full_state(&mut bob_session, &initial).expect("Bob receives server state");

        alice_session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前".len()));
        bob_session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前".len()));
        let alice_update = edit(&mut alice, &mut alice_session, "text.insert", json!({"text":"甲👩‍💻", "raw":true}));
        let bob_update = edit(&mut bob, &mut bob_session, "text.insert", json!({"text":"乙e\u{301}", "raw":true}));
        update(&mut alice, &mut alice_session, &bob_update);
        update(&mut bob, &mut bob_session, &alice_update);
        assert_eq!(supported_body_text(&alice_session.doc), supported_body_text(&bob_session.doc));

        let undo_update = alice.undo_or_redo(&mut alice_session, false).expect("local undo").expect("local insertion is undoable");
        update(&mut bob, &mut bob_session, &undo_update);
        assert_eq!(supported_body_text(&alice_session.doc), Some(vec!["前乙e\u{301}後".into()]));
        assert_eq!(supported_body_text(&alice_session.doc), supported_body_text(&bob_session.doc));

        let redo_update = alice.undo_or_redo(&mut alice_session, true).expect("local redo").expect("local undo is redoable");
        update(&mut bob, &mut bob_session, &redo_update);
        assert_eq!(supported_body_text(&alice_session.doc), supported_body_text(&bob_session.doc));
    }

    #[test]
    fn incoming_insert_rebases_the_existing_caret_with_a_sticky_anchor() {
        let initial = seed("前後");
        let mut session = Session::new(Document::from_text("前後"));
        let mut local = live_for(1);
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前".len()));

        let remote = Doc::new();
        let remote_text = remote.get_or_insert_text(super::SHARED_TEXT);
        let initial_doc = Doc::new();
        let initial_text = initial_doc.get_or_insert_text(super::SHARED_TEXT);
        initial_doc.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed update decodes")).expect("seed state applies");
        let state = initial_doc.transact().state_vector();
        remote.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed update decodes")).expect("seed state applies");
        {
            let mut transaction = remote.transact_mut_with(remote.client_id());
            remote_text.insert(&mut transaction, 0, "乙");
        }
        let remote_update = remote.transact().encode_state_as_update_v1(&state);
        let _ = initial_text.get_string(&initial_doc.transact());

        local.apply_remote_update(&mut session, &remote_update).expect("remote text applies");
        assert_eq!(supported_body_text(&session.doc), Some(vec!["乙前後".into()]));
        assert_eq!(session.sel.focus.off, "乙前".len());
    }

    #[test]
    fn end_caret_anchor_survives_a_remote_insert_after_emoji() {
        let value = "前😀";
        let initial = seed(value);
        let mut session = Session::new(Document::from_text(value));
        let mut local = live_for(1);
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, value.len()));

        let remote = Doc::new();
        let remote_text = remote.get_or_insert_text(super::SHARED_TEXT);
        remote.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed decodes")).expect("seed applies");
        let state = remote.transact().state_vector();
        remote_text.insert(&mut remote.transact_mut(), value.len() as u32, "乙");
        let remote_update = remote.transact().encode_state_as_update_v1(&state);

        local.apply_remote_update(&mut session, &remote_update).expect("remote end insert applies");
        assert_eq!(supported_body_text(&session.doc), Some(vec!["前😀乙".into()]));
        assert_eq!(session.sel.focus.off, "前😀乙".len());
    }

    #[test]
    fn reversed_selection_around_emoji_rebases_when_remote_text_enters_one_boundary() {
        let initial = seed("前😀後");
        let mut session = Session::new(Document::from_text("前😀後"));
        let mut local = live_for(1);
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        session.sel = Selection { anchor: wordcraft_doc::Pos::body(0, "前😀".len()), focus: wordcraft_doc::Pos::body(0, "前".len()) };

        let remote = Doc::new();
        let remote_text = remote.get_or_insert_text(super::SHARED_TEXT);
        remote.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed update decodes")).expect("remote seed applies");
        let state = remote.transact().state_vector();
        remote_text.insert(&mut remote.transact_mut_with(remote.client_id()), "前".len() as u32, "乙");
        let remote_update = remote.transact().encode_state_as_update_v1(&state);

        local.apply_remote_update(&mut session, &remote_update).expect("remote insert projects");
        assert_eq!(supported_body_text(&session.doc), Some(vec!["前乙😀後".into()]));
        assert!(session.sel.anchor.off > session.sel.focus.off, "selection direction must remain reversed");
        assert_eq!(session.sel.anchor.off, "前乙😀".len());
        assert_eq!(session.sel.focus.off, "前".len(), "the focus should remain before text inserted at its boundary");
    }

    #[test]
    fn unresolved_relative_selection_is_an_error_instead_of_offset_zero() {
        let doc = Doc::new();
        let text = doc.get_or_insert_text(super::SHARED_TEXT);
        let missing = yrs::StickyIndex::from_id(yrs::ID::new(yrs::ClientID::new(1_000_000), 0), yrs::Assoc::After);
        assert!(resolve_sticky(&missing, &doc.transact(), "emoji 😀").is_err());
        assert_eq!(text.get_string(&doc.transact()), "");
    }

    #[test]
    fn utf16_local_offsets_map_unicode_insert_and_delete_to_wordcraft_byte_positions() {
        let initial = seed("前後");
        let mut session = Session::new(Document::from_text("前後"));
        let mut peer_session = Session::new(Document::from_text("前後"));
        let mut local = live_for(1);
        let mut peer = live_for(1);
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        peer.apply_full_state(&mut peer_session, &initial).expect("peer state applies");
        assert_eq!(local.texts[0].len(&local.doc.transact()), 2, "UTF-16 mode uses one index per Han character");

        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前".len()));
        let insert = edit(&mut local, &mut session, "text.insert", json!({"text":"甲" , "raw":true}));
        assert!(!insert.is_empty(), "Unicode insertion must produce a Yrs update");
        assert_eq!(supported_body_text(&session.doc), Some(vec!["前甲後".into()]));
        update(&mut peer, &mut peer_session, &insert);

        local.undo.as_mut().expect("undo manager exists").reset();
        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前甲".len()));
        let delete = edit(&mut local, &mut session, "text.backspace", json!({}));
        assert!(!delete.is_empty(), "delete-only updates must be emitted even when the state vector does not advance");
        assert_eq!(supported_body_text(&session.doc), Some(vec!["前後".into()]));
        update(&mut peer, &mut peer_session, &delete);
        assert_eq!(supported_body_text(&peer_session.doc), Some(vec!["前後".into()]));

        let undo = local.undo_or_redo(&mut session, false).expect("delete can be undone").expect("undo emits a Yrs update");
        update(&mut peer, &mut peer_session, &undo);
        assert_eq!(supported_body_text(&session.doc), Some(vec!["前甲後".into()]));
        assert_eq!(supported_body_text(&peer_session.doc), Some(vec!["前甲後".into()]));
    }

    #[test]
    fn utf16_client_update_applies_to_a_default_bytes_mode_server_for_han_and_emoji() {
        let initial = seed("前後");
        let server = Doc::new();
        let server_text = server.get_or_insert_text(super::SHARED_TEXT);
        server.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed update decodes")).expect("server seed applies");

        let mut session = Session::new(Document::from_text("前後"));
        let mut client = live_for(1);
        client.apply_full_state(&mut session, &initial).expect("client receives server state");
        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前".len()));
        let update = edit(&mut client, &mut session, "text.insert", json!({"text":"🧑🏽‍🚀", "raw":true}));
        server
            .transact_mut()
            .apply_update(Update::decode_v1(&update).expect("client update decodes"))
            .expect("Bytes-mode server accepts the Utf16-mode client update");

        assert_eq!(server_text.get_string(&server.transact()), "前🧑🏽‍🚀後");
    }

    #[test]
    fn shared_text_limit_matches_the_server_and_rejects_oversized_local_edits() {
        let boundary = "x".repeat(MAX_SHARED_TEXT_BYTES);
        let too_large = format!("{boundary}x");
        assert!(supported_body(&Document::from_text(&boundary)).is_some());
        assert!(supported_body_text(&Document::from_text(&boundary)).is_some());
        assert!(supported_body(&Document::from_text(&too_large)).is_none());
        let combined_overflow = vec![
            super::SharedParagraph { path: Path::top(0), text: boundary.clone(), runs: Vec::new() },
            super::SharedParagraph { path: Path::top(1), text: "x".into(), runs: Vec::new() },
        ];
        assert!(validate_shared_paragraphs(&combined_overflow).is_err(), "the byte cap applies across the fixed body");
        assert!(supported_body_text(&Document::from_text(&too_large)).is_none());

        let initial = seed("small");
        let mut session = Session::new(Document::from_text("small"));
        let mut local = live_for(1);
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        let paragraph = session.doc.para_mut(wordcraft_doc::StoryRef::Body, &wordcraft_doc::Path::top(0)).expect("body paragraph exists");
        let old_len = paragraph.len();
        paragraph.delete(0, old_len).expect("old text removes");
        paragraph.insert_text(0, &too_large, &CharProps::default()).expect("oversized editor state is constructed");
        let before = supported_body(&Document::from_text("small")).expect("small seed is eligible");
        let error = local.apply_local_editor_change(&mut session, &before, 0).expect_err("oversized local edit is rejected");
        assert!(error.contains("8 MiB"));
        assert_eq!(local.body_texts().expect("live text reads"), vec!["small"], "rejected text must not enter the local CRDT");
    }
}
