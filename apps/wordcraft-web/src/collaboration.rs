//! Narrow single-paragraph live text collaboration for the browser editor.
//!
//! The adapter intentionally shares only `body:main`. Rich text, structure, and cursor presence
//! are outside this mode; the ordinary WordCraft `Session` remains the visible editor model.

use wordcraft_doc::{Block, CharProps, Document, Path, Pos, StoryRef};
use wordcraft_engine::{Selection, Session};
use yrs::undo::{Options as UndoOptions, UndoManager};
use yrs::updates::decoder::Decode;
use yrs::{
    Assoc, Doc, GetString, IndexedSequence, OffsetKind, Options as DocOptions, ReadTxn, StateVector, StickyIndex, Text, TextRef, Transact, Update,
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
const MAX_SHARED_TEXT_BYTES: usize = 8 * 1024 * 1024;

/// True only for a document whose entire body is one unformatted, plain-text paragraph.
pub fn supported_body_text(document: &Document) -> Option<String> {
    if document.body.len() != 1
        || !document.parts.is_empty()
        || !document.comments.is_empty()
        || !document.revisions.is_empty()
        || !document.sources.is_empty()
        || document.settings.track_changes
        || document.settings.protection.is_some()
    {
        return None;
    }
    let Block::Para(paragraph) = &**document.body.first()? else { return None };
    if paragraph.text.len() > MAX_SHARED_TEXT_BYTES {
        return None;
    }
    if paragraph.props != wordcraft_doc::props::ParaProps::default()
        || paragraph.mark != CharProps::default()
        || paragraph.section.is_some()
        || !paragraph.objects.is_empty()
        || paragraph.runs.iter().any(|run| run.props != CharProps::default())
        || paragraph.text.chars().any(unsupported_text_char)
    {
        return None;
    }
    Some(paragraph.text.clone())
}

fn unsupported_text_char(ch: char) -> bool {
    ch.is_control() || ch == '\u{FFFC}' || ch == '\u{2028}' || ch == '\u{2029}'
}

#[derive(Clone)]
struct RelativeSelection {
    anchor: StickyIndex,
    focus: StickyIndex,
    collapsed: bool,
}

/// Yrs state and actor-scoped undo for the one shared text sequence.
struct LiveText {
    doc: Doc,
    text: TextRef,
    undo: Option<UndoManager<()>>,
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
struct FullStateResult {
    catch_up: Option<Vec<u8>>,
    local_state_acknowledged: bool,
}

impl LiveText {
    fn new() -> Self {
        // Yrs Bytes mode makes StickyIndex mix UTF-8 byte lengths with internal item clocks
        // for multibyte text. UTF-16 mode keeps relative positions consistent; edit offsets
        // are converted at the adapter boundary from WordCraft's UTF-8 byte positions.
        let doc = Doc::with_options(DocOptions { offset_kind: OffsetKind::Utf16, ..DocOptions::default() });
        let text = doc.get_or_insert_text(SHARED_TEXT);
        LiveText { doc, text, undo: None }
    }

    fn text(&self) -> String {
        self.text.get_string(&self.doc.transact())
    }

    fn state_vector(&self) -> StateVector {
        self.doc.transact().state_vector()
    }

    /// Apply the server's initial full-state update. A nonempty local document is never seeded
    /// into an empty server CRDT here: doing so can duplicate text when two clients join together.
    fn apply_full_state(&mut self, session: &mut Session, bytes: &[u8]) -> Result<FullStateResult, String> {
        let server_update = Update::decode_v1(bytes).map_err(|error| format!("invalid live text update: {error}"))?;
        let server_doc = Doc::new();
        let server_text = server_doc.get_or_insert_text(SHARED_TEXT);
        server_doc.transact_mut().apply_update(server_update).map_err(|error| format!("could not read live text state: {error}"))?;
        let server_value = server_text.get_string(&server_doc.transact());
        validate_shared_text(&server_value)?;

        let editor_value = supported_body_text(&session.doc).ok_or_else(scope_error)?;
        let local_value = self.text();
        if local_value.is_empty() && !editor_value.is_empty() && server_value.is_empty() {
            return Err(
                "The server has no seeded live text for this document. Your editor content was kept; save or resolve the document before joining."
                    .into(),
            );
        }
        let old_anchors = if local_value == editor_value { self.capture_selection(session).ok() } else { None };

        let local_update = Update::decode_v1(bytes).map_err(|error| format!("invalid live text update: {error}"))?;
        self.doc.transact_mut().apply_update(local_update).map_err(|error| format!("could not apply live text state: {error}"))?;
        let merged_value = self.text();
        validate_shared_text(&merged_value)?;
        if merged_value != editor_value {
            self.project_text(session, &merged_value, old_anchors, session.sel.clone())?;
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
        let before = supported_body_text(&session.doc).ok_or_else(scope_error)?;
        if before != self.text() {
            return Err("The editor and live text are out of sync. Editing was paused to protect the document.".into());
        }
        let anchors = self.capture_selection(session)?;
        self.doc.transact_mut().apply_update(update).map_err(|error| format!("could not apply live text update: {error}"))?;
        let after = self.text();
        validate_shared_text(&after)?;
        if after == before {
            return Ok(false);
        }
        self.project_text(session, &after, Some(anchors), session.sel.clone())?;
        Ok(true)
    }

    /// Diff the editor's completed single-paragraph command into one Yrs delete/insert transaction.
    fn apply_local_editor_change(&mut self, session: &mut Session, before: &str) -> Result<Option<Vec<u8>>, String> {
        let after = supported_body_text(&session.doc).ok_or_else(scope_error)?;
        let shared_before = self.text();
        if shared_before != before {
            return Err("The editor is catching up with live text. The attempted edit was reverted; retry when the live session is ready.".into());
        }
        let Some((start, old_end, inserted)) = one_splice(before, &after) else {
            session.reset_history();
            return Ok(None);
        };
        if !before.is_char_boundary(start) || !before.is_char_boundary(old_end) || !after.is_char_boundary(start) {
            return Err("The text edit did not fall on a UTF-8 character boundary.".into());
        }
        let start = byte_to_utf16_offset(before, start)?;
        let old_end = byte_to_utf16_offset(before, old_end)?;
        let remove_len = old_end.saturating_sub(start);
        let prior_state = self.state_vector();
        {
            let mut transaction = self.doc.transact_mut_with(self.doc.client_id());
            if remove_len > 0 {
                self.text.remove_range(&mut transaction, start, remove_len);
            }
            if !inserted.is_empty() {
                self.text.insert(&mut transaction, start, inserted);
            }
        }
        let shared_after = self.text();
        validate_shared_text(&shared_after)?;
        session.reset_history();
        if shared_after != after {
            self.project_text(session, &shared_after, None, session.sel.clone())?;
        }
        if shared_after == before { Ok(None) } else { Ok(Some(self.doc.transact().encode_state_as_update_v1(&prior_state))) }
    }

    fn undo_or_redo(&mut self, session: &mut Session, redo: bool) -> Result<Option<Vec<u8>>, String> {
        let current = supported_body_text(&session.doc).ok_or_else(scope_error)?;
        if current != self.text() {
            return Err("The editor is catching up with live text. Undo was paused to protect the document.".into());
        }
        let anchors = self.capture_selection(session)?;
        let prior_state = self.state_vector();
        let undo = self.undo.as_mut().ok_or_else(|| "Live undo is not ready yet.".to_string())?;
        let changed = if redo { undo.redo_blocking() } else { undo.undo_blocking() };
        if !changed {
            return Ok(None);
        }
        let updated = self.text();
        validate_shared_text(&updated)?;
        if updated != current {
            self.project_text(session, &updated, Some(anchors), session.sel.clone())?;
        }
        session.reset_history();
        if updated == current { Ok(None) } else { Ok(Some(self.doc.transact().encode_state_as_update_v1(&prior_state))) }
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
        undo.expand_scope(&self.doc, &self.text);
        undo.include_origin(self.doc.client_id());
        self.undo = Some(undo);
    }

    fn capture_selection(&self, session: &Session) -> Result<RelativeSelection, String> {
        let current = self.text();
        if supported_body_text(&session.doc).as_deref() != Some(current.as_str()) {
            return Err("The editor and live text are out of sync.".into());
        }
        let selection = &session.sel;
        if selection.anchor.story != StoryRef::Body
            || selection.focus.story != StoryRef::Body
            || selection.anchor.path != Path::top(0)
            || selection.focus.path != Path::top(0)
        {
            return Err("Live editing supports a caret or selection in the single body paragraph only.".into());
        }
        let anchor = checked_offset(&current, selection.anchor.off)?;
        let focus = checked_offset(&current, selection.focus.off)?;
        let transaction = self.doc.transact();
        let anchor_assoc = if selection.is_collapsed() {
            Assoc::After
        } else if selection.anchor <= selection.focus {
            Assoc::Before
        } else {
            Assoc::After
        };
        let focus_assoc = if selection.is_collapsed() || selection.anchor <= selection.focus { Assoc::After } else { Assoc::Before };
        let anchor = sticky_index(&self.text, &transaction, anchor, anchor_assoc)?;
        let focus = if selection.is_collapsed() { anchor.clone() } else { sticky_index(&self.text, &transaction, focus, focus_assoc)? };
        Ok(RelativeSelection { anchor, focus, collapsed: selection.is_collapsed() })
    }

    fn project_text(&self, session: &mut Session, value: &str, anchors: Option<RelativeSelection>, fallback: Selection) -> Result<(), String> {
        validate_shared_text(value)?;
        if supported_body_text(&session.doc).is_none() {
            return Err(scope_error());
        }
        let resolved_selection = if let Some(anchors) = anchors {
            let transaction = self.doc.transact();
            let anchor = resolve_sticky(&anchors.anchor, &transaction, value)?;
            let focus = if anchors.collapsed { anchor } else { resolve_sticky(&anchors.focus, &transaction, value)? };
            Some(Selection { anchor: Pos::body(0, anchor), focus: Pos::body(0, focus) })
        } else {
            None
        };
        let paragraph = session.doc.para_mut(StoryRef::Body, &Path::top(0)).map_err(|error| error.to_string())?;
        let len = paragraph.len();
        paragraph.delete(0, len).map_err(|error| error.to_string())?;
        if !value.is_empty() {
            paragraph.insert_text(0, value, &CharProps::default()).map_err(|error| error.to_string())?;
        }
        paragraph.touch();
        if let Some(selection) = resolved_selection {
            session.sel = selection;
        } else {
            session.sel = Selection { anchor: clamp_offset(value, fallback.anchor.off), focus: clamp_offset(value, fallback.focus.off) };
        }
        session.clamp_selection();
        session.reset_history();
        session.touch();
        session.relayout();
        Ok(())
    }
}

fn validate_shared_text(value: &str) -> Result<(), String> {
    if value.len() > MAX_SHARED_TEXT_BYTES {
        Err("Live text is limited to 8 MiB per document. The update was rejected to protect the document.".into())
    } else if value.chars().any(unsupported_text_char) {
        Err("This live session supports plain text in one paragraph. Paragraph breaks, inline objects, and control characters are not supported."
            .into())
    } else {
        Ok(())
    }
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

fn clamp_offset(text: &str, offset: usize) -> Pos {
    let mut offset = offset.min(text.len());
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    Pos::body(0, offset)
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
    "Live editing is limited to one plain-text body paragraph up to 8 MiB. Save or open a supported text-only document first.".into()
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
    Open,
    Message(Vec<u8>),
    BadMessage(String),
    Closed,
    Error,
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

    pub fn logic(&self, session: &mut Session, active_document_id: Option<&str>, ctx: &egui::Context) {
        let events = {
            let Ok(mut state) = self.inner.try_borrow_mut() else { return };
            if state.document_id.is_some() && state.document_id.as_deref() != active_document_id {
                state.close_socket();
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
            match event {
                SocketEvent::Open => {
                    if state.phase == ConnectionPhase::Connecting {
                        state.status = "Connected. Waiting for the server's live text state…".into();
                    }
                }
                SocketEvent::Message(bytes) => {
                    if let Err(error) = state.apply_message(session, &bytes) {
                        state.phase = ConnectionPhase::Faulted;
                        state.status = error;
                        state.close_socket();
                    }
                }
                SocketEvent::BadMessage(message) => {
                    state.phase = ConnectionPhase::Faulted;
                    state.status = message;
                    state.close_socket();
                }
                SocketEvent::Closed => {
                    if state.phase != ConnectionPhase::Inactive && state.phase != ConnectionPhase::Faulted {
                        state.phase = ConnectionPhase::Offline;
                        state.status = "Live text connection closed. Reconnect to continue editing; local updates remain in this browser.".into();
                    }
                }
                SocketEvent::Error => {
                    if state.phase != ConnectionPhase::Inactive && state.phase != ConnectionPhase::Faulted {
                        state.phase = ConnectionPhase::Offline;
                        state.status = "Live text connection failed. Reconnect to continue editing; local updates remain in this browser.".into();
                    }
                }
            }
            ctx.request_repaint();
        }
    }

    pub fn ui(&self, session: &mut Session, active_document_id: Option<String>, join_readiness: Result<(), String>, ctx: &egui::Context) {
        let (phase, status, current_id, snapshot_locked) = self
            .inner
            .try_borrow()
            .map(|state| {
                (
                    state.phase,
                    state.status.clone(),
                    state.document_id.clone(),
                    active_document_id.as_deref().is_some_and(|id| state.epoch_documents.contains(id)),
                )
            })
            .unwrap_or((ConnectionPhase::Faulted, "Live text state is busy.".into(), None, false));

        let mut join = false;
        let mut reconnect = false;
        let mut disconnect = false;
        egui::Window::new("Live text collaboration")
            .id(egui::Id::new("wordcraft_live_text_collaboration"))
            .anchor(egui::Align2::LEFT_TOP, egui::vec2(12.0, 12.0))
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                ui.label("Text-only scope: one plain-text body paragraph. Formatting, paste, paragraph breaks, and document structure changes are blocked.");
                ui.small("Changes are durably saved as live text updates. Version history does not include live edits; restore is disabled. No shared cursors or presence are provided.");
                ui.separator();
                ui.label(status);
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
                    }
                    ConnectionPhase::Live => {
                        ui.colored_label(egui::Color32::LIGHT_GREEN, "Live text updates are being persisted by the server.");
                        if ui.button("Pause connection").clicked() {
                            disconnect = true;
                        }
                    }
                    ConnectionPhase::Offline | ConnectionPhase::Faulted => {
                        if ui.button("Reconnect").clicked() {
                            reconnect = true;
                        }
                        if phase == ConnectionPhase::Faulted {
                            ui.colored_label(egui::Color32::YELLOW, "This editor is read-only after an unsupported live update. Open another document or Save As a separate copy.");
                        }
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
        if supported_body_text(&session.doc).is_none() {
            if let Ok(mut state) = self.inner.try_borrow_mut() {
                state.status = scope_error();
            }
            return;
        }
        if document_id.is_empty() {
            return;
        }
        session.reset_history();
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            if state.phase == ConnectionPhase::Inactive || state.document_id.as_deref() != Some(document_id.as_str()) {
                state.adapter = Some(LiveText::new());
                state.first_state_received = false;
            }
            state.document_id = Some(document_id.clone());
            state.phase = ConnectionPhase::Connecting;
            state.status = "Connecting to live text…".into();
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
            enqueue_socket_event(&weak, SocketEvent::Open, &repaint);
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_message = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::MessageEvent)>::new(move |event: web_sys::MessageEvent| {
            use wasm_bindgen::JsCast as _;
            let data = event.data();
            match data.dyn_into::<js_sys::ArrayBuffer>() {
                Ok(buffer) => enqueue_socket_event(&weak, SocketEvent::Message(js_sys::Uint8Array::new(&buffer).to_vec()), &repaint),
                Err(_) => enqueue_socket_event(
                    &weak,
                    SocketEvent::BadMessage("The live server sent a non-binary message; the editor was paused.".into()),
                    &repaint,
                ),
            }
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_close = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_socket_event(&weak, SocketEvent::Closed, &repaint);
        });
        let weak = std::rc::Rc::downgrade(&self.inner);
        let repaint = ctx.clone();
        let on_error = wasm_bindgen::closure::Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            enqueue_socket_event(&weak, SocketEvent::Error, &repaint);
        });
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        if let Ok(mut state) = self.inner.try_borrow_mut() {
            state.close_socket();
            state.socket = Some(socket);
            state.on_open = Some(on_open);
            state.on_message = Some(on_message);
            state.on_close = Some(on_close);
            state.on_error = Some(on_error);
        }
    }

    fn set_error(&self, error: impl Into<String>) {
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            state.phase = ConnectionPhase::Offline;
            state.status = error.into();
        }
    }

    fn disconnect(&self, session: &mut Session) {
        if let Ok(mut state) = self.inner.try_borrow_mut() {
            state.close_socket();
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
        if matches!(id, "text.insert" | "text.backspace" | "text.delete") {
            if self.phase != ConnectionPhase::Live {
                return Some(Err("Editing is paused until the live text connection is ready. Reconnect to continue.".into()));
            }
            return Some(self.local_text_command(session, id, params));
        }
        let Some(spec) = session.registry.get(id) else { return None };
        if !spec.mutates {
            return None;
        }
        Some(Err("This live session is text-only. Formatting, paste, paragraph breaks, and document structure changes are blocked.".into()))
    }

    fn local_text_command(&mut self, session: &mut Session, id: &str, params: &serde_json::Value) -> Result<serde_json::Value, String> {
        let current = supported_body_text(&session.doc).ok_or_else(scope_error)?;
        let adapter = self.adapter.as_mut().ok_or_else(|| "Live text state is unavailable. Reconnect to continue.".to_string())?;
        if adapter.text() != current {
            return Err("The editor is catching up with live text. Wait for the update before typing.".into());
        }
        let before_document = session.doc.clone();
        let before_selection = session.sel.clone();
        let mut command_params = params.clone();
        if id == "text.insert" {
            if let Some(text) = params.get("text").and_then(serde_json::Value::as_str)
                && text.chars().any(unsupported_text_char)
            {
                return Err("This live session supports one plain-text paragraph. Paragraph breaks and control characters are blocked.".into());
            }
            if let Some(object) = command_params.as_object_mut() {
                object.insert("raw".into(), serde_json::Value::Bool(true));
            }
        }
        let result = match session.run(id, &command_params) {
            Ok(result) => result,
            Err(error) => {
                session.reset_history();
                return Err(error.to_string());
            }
        };
        let update = match adapter.apply_local_editor_change(session, &current) {
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
            self.status =
                "Live. Text changes are persisted by the server; version history excludes these edits. No cursor presence is shared.".into();
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
                self.status =
                    "Live. The server confirmed all pending text updates. Version history excludes these edits. No cursor presence is shared.".into();
            } else if let Some(update) = result.catch_up {
                session.dirty = true;
                if let Err(error) = self.send_update(&update) {
                    self.phase = ConnectionPhase::Offline;
                    self.status = format!("Connected, but could not synchronize this browser's pending live text: {error}. Reconnect to retry.");
                }
            } else {
                session.dirty = false;
                self.status =
                    "Live. The server confirmed all text updates. Version history excludes these edits. No cursor presence is shared.".into();
            }
        } else {
            let acknowledged = self.pending_updates.iter().position(|pending| pending.as_slice() == bytes);
            let changed = adapter.apply_remote_update(session, bytes)?;
            if let Some(index) = acknowledged {
                self.pending_updates.remove(index);
            }
            if self.pending_updates.is_empty() {
                session.dirty = false;
                self.status =
                    "Live. The server confirmed all text updates. Version history excludes these edits. No cursor presence is shared.".into();
            } else {
                self.status = format!("Syncing live text… {} update(s) await server confirmation.", self.pending_updates.len());
            }
            if changed && self.pending_updates.is_empty() {
                self.status = "Live. Remote text was applied to the existing editor; all updates are durably stored. Version history excludes live edits. No cursor presence is shared.".into();
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
fn browser_error(error: wasm_bindgen::JsValue) -> String {
    error.as_string().unwrap_or_else(|| format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::{LiveText, MAX_SHARED_TEXT_BYTES, one_splice, resolve_sticky, supported_body_text, validate_shared_text};
    use serde_json::json;
    use wordcraft_doc::{Block, CharProps, Document};
    use wordcraft_engine::{Selection, Session};
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

    #[test]
    fn document_switch_discards_queued_events_and_unconfirmed_updates() {
        let mut events = std::collections::VecDeque::from([1, 2]);
        let mut pending_updates = std::collections::VecDeque::from([vec![3, 4], vec![5, 6]]);

        super::clear_stale_document_queues(&mut events, &mut pending_updates);

        assert!(events.is_empty());
        assert!(pending_updates.is_empty());
    }

    fn seed(text: &str) -> Vec<u8> {
        let doc = Doc::new();
        let shared = doc.get_or_insert_text(super::SHARED_TEXT);
        shared.insert(&mut doc.transact_mut(), 0, text);
        doc.transact().encode_state_as_update_v1(&StateVector::default())
    }

    fn edit(adapter: &mut LiveText, session: &mut Session, id: &str, params: serde_json::Value) -> Vec<u8> {
        let before = supported_body_text(&session.doc).unwrap_or_default();
        session.run(id, &params).expect("editor command succeeds");
        adapter.apply_local_editor_change(session, &before).expect("local edit becomes a CRDT update").unwrap_or_default()
    }

    fn update(adapter: &mut LiveText, session: &mut Session, bytes: &[u8]) {
        adapter.apply_remote_update(session, bytes).expect("remote update applies");
    }

    #[test]
    fn accepts_only_one_unformatted_plain_text_body_paragraph() {
        assert_eq!(supported_body_text(&Document::from_text("hello")), Some("hello".into()));
        assert!(supported_body_text(&Document::from_text("one\ntwo")).is_none());
        let mut formatted = Document::from_text("hello");
        let Block::Para(paragraph) = std::sync::Arc::make_mut(&mut formatted.body[0]) else { return };
        paragraph.runs[0].props = CharProps { bold: Some(true), ..CharProps::default() };
        assert!(supported_body_text(&formatted).is_none());
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
        let mut alice = LiveText::new();
        let mut bob = LiveText::new();
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
        assert_eq!(supported_body_text(&alice_session.doc), Some("前乙e\u{301}後".into()));
        assert_eq!(supported_body_text(&alice_session.doc), supported_body_text(&bob_session.doc));

        let redo_update = alice.undo_or_redo(&mut alice_session, true).expect("local redo").expect("local undo is redoable");
        update(&mut bob, &mut bob_session, &redo_update);
        assert_eq!(supported_body_text(&alice_session.doc), supported_body_text(&bob_session.doc));
    }

    #[test]
    fn incoming_insert_rebases_the_existing_caret_with_a_sticky_anchor() {
        let initial = seed("前後");
        let mut session = Session::new(Document::from_text("前後"));
        let mut local = LiveText::new();
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
        assert_eq!(supported_body_text(&session.doc), Some("乙前後".into()));
        assert_eq!(session.sel.focus.off, "乙前".len());
    }

    #[test]
    fn end_caret_anchor_survives_a_remote_insert_after_emoji() {
        let value = "前😀";
        let initial = seed(value);
        let mut session = Session::new(Document::from_text(value));
        let mut local = LiveText::new();
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, value.len()));

        let remote = Doc::new();
        let remote_text = remote.get_or_insert_text(super::SHARED_TEXT);
        remote.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed decodes")).expect("seed applies");
        let state = remote.transact().state_vector();
        remote_text.insert(&mut remote.transact_mut(), value.len() as u32, "乙");
        let remote_update = remote.transact().encode_state_as_update_v1(&state);

        local.apply_remote_update(&mut session, &remote_update).expect("remote end insert applies");
        assert_eq!(supported_body_text(&session.doc), Some("前😀乙".into()));
        assert_eq!(session.sel.focus.off, "前😀乙".len());
    }

    #[test]
    fn reversed_selection_around_emoji_rebases_when_remote_text_enters_one_boundary() {
        let initial = seed("前😀後");
        let mut session = Session::new(Document::from_text("前😀後"));
        let mut local = LiveText::new();
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        session.sel = Selection { anchor: wordcraft_doc::Pos::body(0, "前😀".len()), focus: wordcraft_doc::Pos::body(0, "前".len()) };

        let remote = Doc::new();
        let remote_text = remote.get_or_insert_text(super::SHARED_TEXT);
        remote.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed update decodes")).expect("remote seed applies");
        let state = remote.transact().state_vector();
        remote_text.insert(&mut remote.transact_mut_with(remote.client_id()), "前".len() as u32, "乙");
        let remote_update = remote.transact().encode_state_as_update_v1(&state);

        local.apply_remote_update(&mut session, &remote_update).expect("remote insert projects");
        assert_eq!(supported_body_text(&session.doc), Some("前乙😀後".into()));
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
        let mut local = LiveText::new();
        let mut peer = LiveText::new();
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        peer.apply_full_state(&mut peer_session, &initial).expect("peer state applies");
        assert_eq!(local.text.len(&local.doc.transact()), 2, "UTF-16 mode uses one index per Han character");

        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前".len()));
        let insert = edit(&mut local, &mut session, "text.insert", json!({"text":"甲" , "raw":true}));
        assert!(!insert.is_empty(), "Unicode insertion must produce a Yrs update");
        assert_eq!(supported_body_text(&session.doc), Some("前甲後".into()));
        update(&mut peer, &mut peer_session, &insert);

        local.undo.as_mut().expect("undo manager exists").reset();
        session.sel = Selection::caret(wordcraft_doc::Pos::body(0, "前甲".len()));
        let delete = edit(&mut local, &mut session, "text.backspace", json!({}));
        assert!(!delete.is_empty(), "delete-only updates must be emitted even when the state vector does not advance");
        assert_eq!(supported_body_text(&session.doc), Some("前後".into()));
        update(&mut peer, &mut peer_session, &delete);
        assert_eq!(supported_body_text(&peer_session.doc), Some("前後".into()));

        let undo = local.undo_or_redo(&mut session, false).expect("delete can be undone").expect("undo emits a Yrs update");
        update(&mut peer, &mut peer_session, &undo);
        assert_eq!(supported_body_text(&session.doc), Some("前甲後".into()));
        assert_eq!(supported_body_text(&peer_session.doc), Some("前甲後".into()));
    }

    #[test]
    fn utf16_client_update_applies_to_a_default_bytes_mode_server_for_han_and_emoji() {
        let initial = seed("前後");
        let server = Doc::new();
        let server_text = server.get_or_insert_text(super::SHARED_TEXT);
        server.transact_mut().apply_update(Update::decode_v1(&initial).expect("seed update decodes")).expect("server seed applies");

        let mut session = Session::new(Document::from_text("前後"));
        let mut client = LiveText::new();
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
        assert!(validate_shared_text(&boundary).is_ok());
        assert!(supported_body_text(&Document::from_text(&boundary)).is_some());
        assert!(validate_shared_text(&too_large).is_err());
        assert!(supported_body_text(&Document::from_text(&too_large)).is_none());

        let initial = seed("small");
        let mut session = Session::new(Document::from_text("small"));
        let mut local = LiveText::new();
        local.apply_full_state(&mut session, &initial).expect("server state applies");
        let paragraph = session.doc.para_mut(wordcraft_doc::StoryRef::Body, &wordcraft_doc::Path::top(0)).expect("body paragraph exists");
        let old_len = paragraph.len();
        paragraph.delete(0, old_len).expect("old text removes");
        paragraph.insert_text(0, &too_large, &CharProps::default()).expect("oversized editor state is constructed");
        let error = local.apply_local_editor_change(&mut session, "small").expect_err("oversized local edit is rejected");
        assert!(error.contains("8 MiB"));
        assert_eq!(local.text(), "small", "rejected text must not enter the local CRDT");
    }
}
