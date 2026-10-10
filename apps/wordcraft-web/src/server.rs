use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use egui::{Align2, Context, Id, Window, vec2};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, Response, Storage};
use wordcraft_doc::Document;
use wordcraft_ui_egui::WordApp;

use crate::server_policy::{MAX_LOCAL_DRAFT_BYTES, local_draft_fits, save_base_version};

const LOCAL_DRAFT_KEY: &str = "wordcraft-unsaved-document-v1";
const LOCAL_DRAFT_DEBOUNCE_MS: f64 = 500.0;

#[derive(Clone)]
pub struct ServerBridge {
    shared: Arc<Mutex<Shared>>,
    dirty_signal: Arc<AtomicBool>,
}

#[derive(Default)]
struct Shared {
    documents: Vec<DocumentSummary>,
    active_id: Option<String>,
    base_version: u64,
    loaded_document: Option<Document>,
    status: String,
    actions: VecDeque<Action>,
    busy: bool,
    list_open: bool,
    save_as_open: bool,
    save_as_title: String,
    confirm_open: Option<String>,
    pending_open: Option<PendingOpen>,
    pending_save: Option<(DocumentRecord, u64, bool)>,
    last_seen_revision: u64,
    last_changed_at: f64,
    last_saved_revision: u64,
    last_queued_revision: Option<u64>,
    retry_after: f64,
    conflict: bool,
    local_draft: Option<LocalDraft>,
    draft_revision: Option<u64>,
    draft_changed_at: f64,
    draft_attempted_revision: Option<u64>,
    recovery_status: String,
    draft_load_blocked: bool,
    draft_stored: bool,
    draft_clear_failed: bool,
    history_open: bool,
    versions: Vec<VersionSummary>,
    restore_confirmation: Option<(String, u64)>,
}

enum Action {
    Refresh,
    Open { id: String, discard_dirty: bool, expected_revision: Option<u64> },
    ListVersions { id: String },
    RestoreVersion { id: String, target_version: u64, editor_revision: u64 },
    Save { id: Option<String>, active_at_queue: Option<String>, title: String, snapshot: Value, revision: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocumentSummary {
    id: String,
    title: String,
    version: u64,
    created_at: i64,
    modified_at: i64,
    deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocumentRecord {
    id: String,
    title: String,
    version: u64,
    created_at: i64,
    modified_at: i64,
    deleted: bool,
    snapshot: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalDraft {
    format_version: u32,
    dirty: bool,
    active_id: Option<String>,
    base_version: u64,
    local_path: Option<String>,
    snapshot: Value,
}

struct PendingOpen {
    record: DocumentRecord,
    discard_dirty: bool,
    expected_revision: Option<u64>,
    is_restore: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VersionSummary {
    version: u64,
    title: String,
    created_at: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateRequest<'a> {
    title: &'a str,
    snapshot: &'a Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SaveRequest<'a> {
    base_version: u64,
    title: &'a str,
    snapshot: &'a Value,
}

struct UiSnapshot {
    status: String,
    active: Option<(String, u64)>,
    documents: Vec<DocumentSummary>,
    list_open: bool,
    save_as_open: bool,
    save_as_title: String,
    confirm_open: Option<String>,
    conflict: bool,
    recovery_status: String,
    history_open: bool,
    versions: Vec<VersionSummary>,
    restore_confirmation: Option<(String, u64)>,
}

enum UiAction {
    Save,
    Refresh,
    Open(String),
    ConfirmOpen,
    CancelOpen,
    OpenList,
    SaveAs,
    CancelSaveAs,
    SubmitSaveAs(String),
    DownloadDocx,
    OpenHistory,
    RestoreVersion(u64),
    ConfirmRestore,
    CancelRestore,
}

impl ServerBridge {
    pub fn new() -> Self {
        let (local_draft, recovery_status, draft_load_blocked) = load_local_draft();
        let draft_stored = local_draft.is_some();
        let mut shared = Shared {
            status: "Connecting to the shared server…".into(),
            local_draft,
            recovery_status,
            draft_load_blocked,
            draft_stored,
            ..Shared::default()
        };
        shared.actions.push_back(Action::Refresh);
        ServerBridge { shared: Arc::new(Mutex::new(shared)), dirty_signal: Arc::new(AtomicBool::new(false)) }
    }

    pub fn dirty_signal(&self) -> Arc<AtomicBool> {
        self.dirty_signal.clone()
    }

    /// Attach the web server save operation to WordApp's existing Save and Save As commands.
    pub fn save_handler(&self) -> Box<dyn Fn(Option<&str>, &Document, u64)> {
        let shared = self.shared.clone();
        Box::new(move |save_as_name, document, revision| {
            let title = save_as_name.map(title_from_name).unwrap_or_else(|| {
                let title = document.core.title.trim();
                if title.is_empty() { "Untitled document".into() } else { title.to_string() }
            });
            let snapshot = match wordcraft_engine::io::storage_snapshot(document) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let mut shared = lock_shared(&shared);
                    shared.status = format!("Save failed: {error}");
                    return;
                }
            };
            let mut shared = lock_shared(&shared);
            let save_as = save_as_name.is_some();
            let create_new = save_as || shared.active_id.is_none();
            if !save_as && (shared.last_saved_revision == revision || shared.last_queued_revision == Some(revision)) {
                shared.status = if shared.last_saved_revision == revision {
                    "This revision is already saved on the server."
                } else {
                    "This revision is already being saved."
                }
                .into();
                return;
            }
            if shared.conflict && !create_new {
                shared.status = "Conflict: reload the server version or save a new copy before continuing.".into();
                return;
            }
            let id = if create_new { None } else { shared.active_id.clone() };
            let active_at_queue = shared.active_id.clone();
            shared.actions.push_back(Action::Save { id, active_at_queue, title, snapshot, revision });
            shared.last_queued_revision = Some(revision);
            shared.status = if create_new { "Creating a shared server document…".into() } else { "Saving to the shared server…".into() };
        })
    }

    /// Per-frame server sync. Edits are saved with optimistic versions, so a stale session cannot
    /// overwrite a newer server snapshot. A conflict leaves the local session untouched.
    pub fn logic(&mut self, app: &mut WordApp, ctx: &Context) {
        self.apply_local_draft(app, ctx);
        self.apply_pending_open(app);
        self.apply_pending_save(app);
        self.detach_if_local_document(app);
        self.queue_debounced_save(app, ctx);
        self.persist_local_draft(app, ctx);
        self.dispatch_one(ctx);
        self.dirty_signal.store(app.session.dirty, Ordering::Relaxed);
    }

    pub fn ui(&mut self, app: &mut WordApp, ctx: &Context) {
        let snapshot = {
            let shared = lock_shared(&self.shared);
            UiSnapshot {
                status: shared.status.clone(),
                active: shared.active_id.clone().map(|id| (id, shared.base_version)),
                documents: shared.documents.clone(),
                list_open: shared.list_open,
                save_as_open: shared.save_as_open,
                save_as_title: if shared.save_as_title.is_empty() { app.session.doc.core.title.clone() } else { shared.save_as_title.clone() },
                confirm_open: shared.confirm_open.clone(),
                conflict: shared.conflict,
                recovery_status: shared.recovery_status.clone(),
                history_open: shared.history_open,
                versions: shared.versions.clone(),
                restore_confirmation: shared.restore_confirmation.clone(),
            }
        };
        let mut view = snapshot;
        let mut actions = Vec::new();

        Window::new("Shared workspace")
            .id(Id::new("wordcraft_shared_workspace"))
            .anchor(Align2::RIGHT_TOP, vec2(-12.0, 12.0))
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                ui.label(&view.status);
                if !view.recovery_status.is_empty() {
                    ui.small(&view.recovery_status);
                }
                if let Some((id, version)) = &view.active {
                    ui.small(format!("Server document · v{version} · {id}"));
                } else {
                    ui.small("Current document is not yet saved on the server.");
                }
                ui.horizontal(|ui| {
                    if ui.button("Save to server").clicked() {
                        actions.push(UiAction::Save);
                    }
                    if ui.button("Save as new server document").clicked() {
                        actions.push(UiAction::SaveAs);
                    }
                });
                ui.horizontal(|ui| {
                    if ui.button("Shared documents").clicked() {
                        actions.push(UiAction::OpenList);
                    }
                    if view.active.is_some() && ui.button("Version history").clicked() {
                        actions.push(UiAction::OpenHistory);
                    }
                    if ui.button("Download DOCX").clicked() {
                        actions.push(UiAction::DownloadDocx);
                    }
                });
                ui.small("Downloads do not save changes to the shared server.");
                if view.conflict {
                    ui.colored_label(egui::Color32::LIGHT_RED, "The server has a newer version. Local edits are preserved.");
                }
            });

        if view.list_open {
            Window::new("Shared documents").id(Id::new("wordcraft_shared_document_list")).default_width(390.0).open(&mut view.list_open).show(
                ctx,
                |ui| {
                    if ui.button("Refresh list").clicked() {
                        actions.push(UiAction::Refresh);
                    }
                    ui.separator();
                    if view.documents.is_empty() {
                        ui.label("No shared documents are available.");
                    }
                    for document in &view.documents {
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.label(&document.title);
                                ui.small(format!("Version {} · updated {}", document.version, format_date(document.modified_at)));
                            });
                            if ui.button("Open").clicked() {
                                actions.push(UiAction::Open(document.id.clone()));
                            }
                        });
                        ui.separator();
                    }
                },
            );
        }

        if view.history_open {
            Window::new("Version history").id(Id::new("wordcraft_server_version_history")).default_width(440.0).open(&mut view.history_open).show(
                ctx,
                |ui| {
                    if ui.button("Refresh history").clicked() {
                        actions.push(UiAction::OpenHistory);
                    }
                    ui.separator();
                    if view.versions.is_empty() {
                        ui.label("No saved versions are available.");
                    }
                    let current_version = view.active.as_ref().map(|(_, version)| *version);
                    for version in &view.versions {
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.label(format!("Version {} · {}", version.version, version.title));
                                ui.small(format!("Saved {}", format_date(version.created_at)));
                            });
                            let is_current = current_version == Some(version.version);
                            if ui.add_enabled(!is_current, egui::Button::new("Restore…")).clicked() {
                                actions.push(UiAction::RestoreVersion(version.version));
                            }
                            if is_current {
                                ui.small("Current");
                            }
                        });
                        ui.separator();
                    }
                },
            );
        }

        if let Some((_, version)) = &view.restore_confirmation {
            Window::new("Restore saved version?").id(Id::new("wordcraft_confirm_version_restore")).resizable(false).collapsible(false).show(
                ctx,
                |ui| {
                    ui.label(format!("Restore version {version}? This creates a new server version."));
                    if app.session.dirty {
                        ui.colored_label(egui::Color32::LIGHT_RED, "This will replace the unsaved edits in the current editor.");
                    }
                    ui.horizontal(|ui| {
                        if ui.button("Restore version").clicked() {
                            actions.push(UiAction::ConfirmRestore);
                        }
                        if ui.button("Cancel").clicked() {
                            actions.push(UiAction::CancelRestore);
                        }
                    });
                },
            );
        }

        if view.save_as_open {
            Window::new("Save as a new server document")
                .id(Id::new("wordcraft_save_as_shared"))
                .resizable(false)
                .collapsible(false)
                .open(&mut view.save_as_open)
                .show(ctx, |ui| {
                    ui.label("Create a separate shared document from the current content.");
                    ui.text_edit_singleline(&mut view.save_as_title);
                    ui.horizontal(|ui| {
                        if ui.button("Create").clicked() {
                            actions.push(UiAction::SubmitSaveAs(view.save_as_title.clone()));
                        }
                        if ui.button("Cancel").clicked() {
                            actions.push(UiAction::CancelSaveAs);
                        }
                    });
                });
        }

        if let Some(id) = &view.confirm_open {
            Window::new("Unsaved changes").id(Id::new("wordcraft_confirm_open_shared")).resizable(false).collapsible(false).show(ctx, |ui| {
                ui.label("Opening this document will replace the current unsaved edits.");
                ui.horizontal(|ui| {
                    if ui.button("Discard edits and open").clicked() {
                        actions.push(UiAction::ConfirmOpen);
                    }
                    if ui.button("Keep editing").clicked() {
                        actions.push(UiAction::CancelOpen);
                    }
                });
                ui.small(format!("Document: {id}"));
            });
        }

        {
            let mut shared = lock_shared(&self.shared);
            shared.list_open = view.list_open;
            shared.save_as_open = view.save_as_open;
            shared.save_as_title = view.save_as_title;
            shared.history_open = view.history_open;
        }

        for action in actions {
            match action {
                UiAction::Save => {
                    let _ = app.run("file.save", serde_json::json!({}));
                }
                UiAction::Refresh => enqueue(&self.shared, Action::Refresh),
                UiAction::Open(id) => {
                    if app.session.dirty {
                        lock_shared(&self.shared).confirm_open = Some(id);
                    } else {
                        enqueue(&self.shared, Action::Open { id, discard_dirty: false, expected_revision: None });
                    }
                }
                UiAction::ConfirmOpen => {
                    let id = lock_shared(&self.shared).confirm_open.take();
                    if let Some(id) = id {
                        enqueue(&self.shared, Action::Open { id, discard_dirty: true, expected_revision: Some(app.session.rev()) });
                    }
                }
                UiAction::CancelOpen => lock_shared(&self.shared).confirm_open = None,
                UiAction::OpenList => {
                    let mut shared = lock_shared(&self.shared);
                    shared.list_open = true;
                    shared.actions.push_back(Action::Refresh);
                }
                UiAction::SaveAs => {
                    let mut shared = lock_shared(&self.shared);
                    shared.save_as_title = app.session.doc.core.title.clone();
                    shared.save_as_open = true;
                }
                UiAction::CancelSaveAs => lock_shared(&self.shared).save_as_open = false,
                UiAction::SubmitSaveAs(title) => {
                    let title = title.trim();
                    if title.is_empty() {
                        lock_shared(&self.shared).status = "A title is required to create a server document.".into();
                    } else {
                        let _ = app.run("file.save", serde_json::json!({"path": title}));
                        let mut shared = lock_shared(&self.shared);
                        shared.save_as_open = false;
                        shared.save_as_title.clear();
                    }
                }
                UiAction::DownloadDocx => download_docx(app, &self.shared),
                UiAction::OpenHistory => {
                    let mut shared = lock_shared(&self.shared);
                    if let Some(id) = shared.active_id.clone() {
                        shared.history_open = true;
                        shared.versions.clear();
                        shared.actions.push_back(Action::ListVersions { id });
                    }
                }
                UiAction::RestoreVersion(version) => {
                    let active_id = lock_shared(&self.shared).active_id.clone();
                    if let Some(id) = active_id {
                        lock_shared(&self.shared).restore_confirmation = Some((id, version));
                    }
                }
                UiAction::ConfirmRestore => {
                    let confirmation = lock_shared(&self.shared).restore_confirmation.take();
                    if let Some((id, version)) = confirmation {
                        let mut shared = lock_shared(&self.shared);
                        if shared.active_id.as_deref() == Some(id.as_str()) {
                            shared.actions.push_back(Action::RestoreVersion { id, target_version: version, editor_revision: app.session.rev() });
                            shared.status = format!("Restoring saved version {version}…");
                        } else {
                            shared.status = "The active document changed. Reopen version history before restoring.".into();
                        }
                    }
                }
                UiAction::CancelRestore => lock_shared(&self.shared).restore_confirmation = None,
            }
        }
    }

    fn apply_local_draft(&mut self, app: &mut WordApp, ctx: &Context) {
        let draft = lock_shared(&self.shared).local_draft.take();
        let Some(draft) = draft else { return };
        match wordcraft_engine::io::document_from_storage_snapshot(&draft.snapshot) {
            Ok(document) => {
                app.session.set_document(document);
                app.session.dirty = true;
                app.session.path = draft.local_path.map(std::path::PathBuf::from);
                let revision = app.session.rev();
                let now = now_ms(ctx);
                let mut shared = lock_shared(&self.shared);
                shared.active_id = draft.active_id;
                shared.base_version = draft.base_version;
                shared.loaded_document = Some(app.session.doc.clone());
                shared.last_seen_revision = revision;
                shared.last_changed_at = now;
                shared.last_saved_revision = 0;
                shared.last_queued_revision = None;
                shared.draft_revision = Some(revision);
                shared.draft_changed_at = now;
                shared.draft_attempted_revision = Some(revision);
                shared.draft_load_blocked = false;
                shared.status = "Recovered a local draft. Save it to the server when ready.".into();
                shared.recovery_status = "Recovered unsaved changes from this browser.".into();
            }
            Err(error) => {
                let mut shared = lock_shared(&self.shared);
                shared.draft_load_blocked = true;
                shared.recovery_status = format!("Local draft could not be opened and was kept: {error}");
            }
        }
    }

    fn persist_local_draft(&mut self, app: &WordApp, ctx: &Context) {
        let now = now_ms(ctx);
        let task = {
            let mut shared = lock_shared(&self.shared);
            if app.session.dirty {
                let revision = app.session.rev();
                if shared.draft_revision != Some(revision) {
                    shared.draft_revision = Some(revision);
                    shared.draft_changed_at = now;
                    shared.draft_attempted_revision = None;
                }
                if shared.draft_attempted_revision == Some(revision) {
                    return;
                }
                let elapsed = now - shared.draft_changed_at;
                if elapsed < LOCAL_DRAFT_DEBOUNCE_MS {
                    ctx.request_repaint_after(std::time::Duration::from_millis(200));
                    return;
                }
                shared.draft_attempted_revision = Some(revision);
                Some((
                    revision,
                    shared.active_id.clone(),
                    shared.base_version,
                    app.session.path.as_ref().map(|path| path.to_string_lossy().into_owned()),
                ))
            } else if shared.draft_stored && !shared.draft_load_blocked && !shared.draft_clear_failed {
                shared.draft_attempted_revision = None;
                shared.draft_revision = None;
                Some((0, None, 0, None))
            } else {
                None
            }
        };
        let Some((revision, active_id, base_version, local_path)) = task else { return };
        let storage = match browser_storage() {
            Ok(storage) => storage,
            Err(error) => {
                if revision != 0 {
                    lock_shared(&self.shared).recovery_status = format!("Local recovery unavailable: {error}");
                }
                return;
            }
        };
        if revision == 0 {
            self.clear_local_draft(&storage);
            return;
        }
        let snapshot = match wordcraft_engine::io::storage_snapshot(&app.session.doc) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                lock_shared(&self.shared).recovery_status = format!("Local recovery could not serialize this document: {error}");
                return;
            }
        };
        let draft = LocalDraft { format_version: 1, dirty: true, active_id, base_version, local_path, snapshot };
        let encoded = match serde_json::to_string(&draft) {
            Ok(encoded) => encoded,
            Err(error) => {
                lock_shared(&self.shared).recovery_status = format!("Local recovery could not serialize this document: {error}");
                return;
            }
        };
        if !local_draft_fits(encoded.len()) {
            lock_shared(&self.shared).recovery_status = format!(
                "Local recovery is unavailable because this draft exceeds the {} MiB browser-storage limit. Download a copy before leaving.",
                MAX_LOCAL_DRAFT_BYTES / (1024 * 1024)
            );
            return;
        }
        match storage.set_item(LOCAL_DRAFT_KEY, &encoded) {
            Ok(()) => {
                let mut shared = lock_shared(&self.shared);
                shared.draft_load_blocked = false;
                shared.draft_stored = true;
                shared.draft_clear_failed = false;
                shared.recovery_status = "Local recovery copy saved on this browser.".into();
            }
            Err(error) => {
                lock_shared(&self.shared).recovery_status = format!("Local recovery could not be saved: {error:?}. Download a copy before leaving.")
            }
        }
    }

    fn clear_local_draft(&mut self, storage: &Storage) {
        match storage.remove_item(LOCAL_DRAFT_KEY) {
            Ok(()) => {
                let mut shared = lock_shared(&self.shared);
                shared.draft_stored = false;
                shared.draft_clear_failed = false;
                shared.recovery_status.clear();
            }
            Err(error) => {
                let status = match storage.get_item(LOCAL_DRAFT_KEY) {
                    Ok(Some(encoded)) => match serde_json::from_str::<LocalDraft>(&encoded) {
                        Ok(mut draft) => {
                            draft.dirty = false;
                            serde_json::to_string(&draft)
                                .ok()
                                .filter(|encoded| encoded.len() <= MAX_LOCAL_DRAFT_BYTES)
                                .and_then(|encoded| storage.set_item(LOCAL_DRAFT_KEY, &encoded).ok())
                                .map(|()| "Document is saved, but browser storage kept a non-recoverable draft marker.".to_string())
                                .unwrap_or_else(|| format!("Document is saved, but the local recovery copy could not be cleared: {error:?}."))
                        }
                        Err(_) => format!("Document is saved, but an old local recovery entry could not be cleared: {error:?}."),
                    },
                    _ => format!("Document is saved, but the local recovery entry could not be cleared: {error:?}."),
                };
                let mut shared = lock_shared(&self.shared);
                shared.draft_clear_failed = true;
                if status.contains("non-recoverable draft marker") {
                    shared.draft_stored = false;
                    shared.draft_clear_failed = false;
                }
                shared.recovery_status = status;
            }
        }
    }

    fn apply_pending_open(&mut self, app: &mut WordApp) {
        let pending = lock_shared(&self.shared).pending_open.take();
        let Some(PendingOpen { record, discard_dirty, expected_revision, is_restore }) = pending else { return };
        if expected_revision.is_some_and(|revision| app.session.rev() != revision) {
            let mut shared = lock_shared(&self.shared);
            if is_restore {
                shared.conflict = true;
                shared.status = "The server version was restored, but the editor changed before it could be loaded. Local edits are preserved; reload the server version or save a new copy.".into();
            } else {
                shared.confirm_open = Some(record.id);
                shared.status = "The editor changed after you confirmed opening. Confirm again to discard those edits.".into();
            }
            return;
        }
        if app.session.dirty && !discard_dirty {
            let mut shared = lock_shared(&self.shared);
            shared.confirm_open = Some(record.id);
            shared.status = "The current document has unsaved edits. Confirm before replacing it.".into();
            return;
        }
        match wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot) {
            Ok(document) => {
                app.session.set_document(document);
                app.session.dirty = false;
                app.session.path = None;
                let mut shared = lock_shared(&self.shared);
                shared.active_id = Some(record.id);
                shared.base_version = record.version;
                shared.loaded_document = Some(app.session.doc.clone());
                shared.last_seen_revision = app.session.rev();
                shared.last_saved_revision = app.session.rev();
                shared.last_queued_revision = None;
                shared.conflict = false;
                shared.versions.clear();
                shared.history_open = false;
                shared.restore_confirmation = None;
                shared.status = format!("Opened server version {}.", record.version);
            }
            Err(error) => {
                lock_shared(&self.shared).status = format!("Could not open server document: {error}");
            }
        }
    }

    fn apply_pending_save(&mut self, app: &mut WordApp) {
        let pending = lock_shared(&self.shared).pending_save.take();
        let Some((record, revision, clear_local_path)) = pending else { return };
        if app.session.rev() != revision {
            return;
        }
        match wordcraft_engine::io::document_from_storage_snapshot(&record.snapshot) {
            Ok(saved) => {
                app.session.doc.core = saved.core;
                app.session.dirty = false;
                if clear_local_path {
                    app.session.path = None;
                }
                let mut shared = lock_shared(&self.shared);
                shared.loaded_document = Some(app.session.doc.clone());
                shared.last_saved_revision = revision;
                shared.status = format!("Saved on server · version {}.", record.version);
            }
            Err(error) => {
                lock_shared(&self.shared).status = format!("Server saved the document, but its response could not be read: {error}");
            }
        }
    }

    fn detach_if_local_document(&mut self, app: &mut WordApp) {
        let mut shared = lock_shared(&self.shared);
        if shared.active_id.is_none() || app.session.dirty {
            return;
        }
        let local_path = app.session.path.is_some();
        let replaced_document = shared.loaded_document.as_ref().is_some_and(|loaded| loaded != &app.session.doc);
        if local_path || replaced_document {
            shared.active_id = None;
            shared.base_version = 0;
            shared.loaded_document = None;
            shared.conflict = false;
            shared.last_saved_revision = 0;
            shared.versions.clear();
            shared.history_open = false;
            shared.restore_confirmation = None;
            shared.status = "Local document loaded. Save to server to create a shared copy.".into();
        }
    }

    fn queue_debounced_save(&mut self, app: &WordApp, ctx: &Context) {
        let now = now_ms(ctx);
        let revision = app.session.rev();
        let mut shared = lock_shared(&self.shared);
        if shared.last_seen_revision != revision {
            shared.last_seen_revision = revision;
            shared.last_changed_at = now;
        }
        let is_new_blank = shared.active_id.is_none() && app.session.path.is_none();
        if !app.session.dirty || shared.busy || shared.conflict || !is_new_blank && shared.active_id.is_none() {
            return;
        }
        if revision == shared.last_saved_revision || shared.last_queued_revision == Some(revision) || now < shared.retry_after {
            return;
        }
        if now - shared.last_changed_at < 900.0 {
            ctx.request_repaint_after(std::time::Duration::from_millis(250));
            return;
        }
        let create_new = shared.active_id.is_none();
        let title = if app.session.doc.core.title.trim().is_empty() { "Untitled document" } else { app.session.doc.core.title.trim() }.to_string();
        let mut snapshot_document = app.session.doc.clone();
        snapshot_document.core.title = title.clone();
        let snapshot = match wordcraft_engine::io::storage_snapshot(&snapshot_document) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                shared.status = format!("Save failed: {error}");
                return;
            }
        };
        let active_at_queue = shared.active_id.clone();
        shared.actions.push_back(Action::Save {
            id: if create_new { None } else { active_at_queue.clone() },
            active_at_queue,
            title,
            snapshot,
            revision,
        });
        shared.last_queued_revision = Some(revision);
        shared.status = "Saving changes to the server…".into();
    }

    fn dispatch_one(&mut self, ctx: &Context) {
        let (action, active_id, active_version) = {
            let mut shared = lock_shared(&self.shared);
            if shared.busy {
                return;
            }
            let action = shared.actions.pop_front();
            if action.is_some() {
                shared.busy = true;
            }
            (action, shared.active_id.clone(), shared.base_version)
        };
        let Some(action) = action else { return };
        let shared = self.shared.clone();
        let ctx = ctx.clone();
        match action {
            Action::Refresh => wasm_bindgen_futures::spawn_local(async move {
                let result = fetch_json::<Vec<DocumentSummary>>("GET", "/api/documents", None).await;
                let mut shared = lock_shared(&shared);
                shared.busy = false;
                match result {
                    Ok(documents) => {
                        shared.documents = documents;
                        if shared.status.starts_with("Offline") || shared.status.starts_with("Connecting") {
                            shared.status = "Connected to the shared server.".into();
                        }
                    }
                    Err(error) => shared.status = format!("Offline — {error}"),
                }
                drop(shared);
                ctx.request_repaint();
            }),
            Action::Open { id, discard_dirty, expected_revision } => wasm_bindgen_futures::spawn_local(async move {
                let result = fetch_json::<DocumentRecord>("GET", &format!("/api/documents/{id}"), None).await;
                let mut shared = lock_shared(&shared);
                shared.busy = false;
                match result {
                    Ok(record) => {
                        shared.pending_open = Some(PendingOpen { record, discard_dirty, expected_revision, is_restore: false });
                        shared.status = "Server document downloaded; opening it…".into();
                    }
                    Err(error) => shared.status = format!("Could not open document: {error}"),
                }
                drop(shared);
                ctx.request_repaint();
            }),
            Action::ListVersions { id } => wasm_bindgen_futures::spawn_local(async move {
                let result = fetch_json::<Vec<VersionSummary>>("GET", &format!("/api/documents/{id}/versions"), None).await;
                let mut shared = lock_shared(&shared);
                shared.busy = false;
                match result {
                    Ok(versions) if shared.active_id.as_deref() == Some(id.as_str()) => {
                        shared.versions = versions;
                        shared.status = "Version history loaded.".into();
                    }
                    Ok(_) => {}
                    Err(error) => shared.status = format!("Could not load version history: {error}"),
                }
                drop(shared);
                ctx.request_repaint();
            }),
            Action::RestoreVersion { id, target_version, editor_revision } => {
                let Some(base_version) = save_base_version(Some(&id), active_id.as_deref(), active_version) else {
                    let mut shared = lock_shared(&self.shared);
                    shared.busy = false;
                    shared.status = "The active server document changed before restore. Open its history and try again.".into();
                    ctx.request_repaint();
                    return;
                };
                wasm_bindgen_futures::spawn_local(async move {
                    let body = serde_json::json!({"baseVersion": base_version, "version": target_version});
                    let result = fetch_json::<DocumentRecord>("POST", &format!("/api/documents/{id}/restore-version"), Some(body)).await;
                    let mut shared = lock_shared(&shared);
                    shared.busy = false;
                    match result {
                        Ok(record) if shared.active_id.as_deref() == Some(id.as_str()) => {
                            shared.base_version = record.version;
                            shared.conflict = false;
                            if shared.last_seen_revision == editor_revision {
                                shared.pending_open = Some(PendingOpen {
                                    record: record.clone(),
                                    discard_dirty: true,
                                    expected_revision: Some(editor_revision),
                                    is_restore: true,
                                });
                                shared.status = format!("Restored version {target_version} as new version {}.", record.version);
                            } else {
                                shared.conflict = true;
                                shared.status = format!(
                                    "Version {target_version} was restored on the server as version {}, but the editor changed during the request. Local edits are preserved; reload the server version or save a new copy.",
                                    record.version
                                );
                            }
                            shared.actions.push_back(Action::ListVersions { id });
                        }
                        Ok(record) => {
                            shared.status = format!(
                                "Version {target_version} was restored on the server as version {}, but the active document changed. The editor was left untouched.",
                                record.version
                            );
                        }
                        Err(error) => {
                            shared.conflict = error.contains("409") || error.contains("version conflict");
                            shared.status = if shared.conflict {
                                format!("Restore conflict: {error}. The editor was left unchanged; refresh the history and try again.")
                            } else {
                                format!("Could not restore version {target_version}: {error}")
                            };
                        }
                    }
                    drop(shared);
                    ctx.request_repaint();
                });
            }
            Action::Save { id, active_at_queue, title, snapshot, revision } => {
                let source_id = id.clone();
                let source_active_id = active_at_queue.clone();
                let Some(base_version) = save_base_version(id.as_deref(), active_id.as_deref(), active_version) else {
                    let mut shared = lock_shared(&self.shared);
                    shared.busy = false;
                    if shared.last_queued_revision == Some(revision) {
                        shared.last_queued_revision = None;
                    }
                    shared.status = "The active server document changed before this save could run. Save again to apply these edits.".into();
                    ctx.request_repaint();
                    return;
                };
                wasm_bindgen_futures::spawn_local(async move {
                    let body = if id.is_some() {
                        serde_json::to_value(SaveRequest { base_version, title: &title, snapshot: &snapshot })
                    } else {
                        serde_json::to_value(CreateRequest { title: &title, snapshot: &snapshot })
                    };
                    let result = match body {
                        Ok(body) => {
                            let (method, path) = match id {
                                Some(id) => ("PUT", format!("/api/documents/{id}")),
                                None => ("POST", "/api/documents".into()),
                            };
                            fetch_json::<DocumentRecord>(method, &path, Some(body)).await
                        }
                        Err(error) => Err(error.to_string()),
                    };
                    let mut shared = lock_shared(&shared);
                    shared.busy = false;
                    match result {
                        Ok(record) => {
                            let still_active = shared.active_id == source_active_id;
                            let same_revision = shared.last_seen_revision == revision;
                            if still_active && (same_revision || source_id.is_none()) {
                                shared.active_id = Some(record.id.clone());
                                shared.base_version = record.version;
                                shared.conflict = false;
                                if same_revision {
                                    shared.pending_save = Some((record.clone(), revision, true));
                                }
                            } else if still_active {
                                shared.base_version = record.version;
                                shared.conflict = false;
                            }
                            if shared.last_queued_revision == Some(revision) {
                                shared.last_queued_revision = None;
                            }
                            shared.retry_after = 0.0;
                            shared.actions.push_back(Action::Refresh);
                            shared.status = if same_revision {
                                format!("Server accepted version {}.", record.version)
                            } else {
                                format!("Server accepted version {}, and newer local edits remain open.", record.version)
                            };
                        }
                        Err(error) => {
                            if shared.last_queued_revision == Some(revision) {
                                shared.last_queued_revision = None;
                            }
                            shared.retry_after = now_ms_raw() + 3000.0;
                            shared.conflict = error.contains("409") || error.contains("version conflict");
                            shared.status = if shared.conflict {
                                format!("Conflict: {error}. Your local edits are intact; reload or save a new copy.")
                            } else {
                                format!("Offline or save failed: {error}. Local edits are still open.")
                            };
                        }
                    }
                    drop(shared);
                    ctx.request_repaint();
                });
            }
        }
    }
}

fn lock_shared(shared: &Arc<Mutex<Shared>>) -> std::sync::MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(|error| error.into_inner())
}

fn browser_storage() -> Result<Storage, String> {
    let window = web_sys::window().ok_or_else(|| "browser window is unavailable".to_string())?;
    window.local_storage().map_err(js_error)?.ok_or_else(|| "browser local storage is disabled".to_string())
}

fn load_local_draft() -> (Option<LocalDraft>, String, bool) {
    let storage = match browser_storage() {
        Ok(storage) => storage,
        Err(error) => return (None, format!("Local recovery unavailable: {error}."), true),
    };
    let encoded = match storage.get_item(LOCAL_DRAFT_KEY) {
        Ok(Some(encoded)) => encoded,
        Ok(None) => return (None, String::new(), false),
        Err(error) => return (None, format!("Local recovery unavailable: browser storage could not be read ({error:?})."), true),
    };
    if !local_draft_fits(encoded.len()) {
        return (
            None,
            format!("A local draft exceeds the {} MiB browser-storage limit and was kept without opening.", MAX_LOCAL_DRAFT_BYTES / (1024 * 1024)),
            true,
        );
    }
    let draft = match serde_json::from_str::<LocalDraft>(&encoded) {
        Ok(draft) if draft.format_version == 1 => draft,
        Ok(_) => return (None, "A local draft uses an unsupported format and was kept.".into(), true),
        Err(error) => return (None, format!("A local draft could not be read and was kept: {error}."), true),
    };
    if !draft.dirty {
        let status = if storage.remove_item(LOCAL_DRAFT_KEY).is_ok() {
            String::new()
        } else {
            "A completed local draft marker remains in browser storage; it will not be restored.".into()
        };
        let blocked = !status.is_empty();
        return (None, status, blocked);
    }
    (Some(draft), "Unsaved local changes were found in this browser.".into(), false)
}

fn enqueue(shared: &Arc<Mutex<Shared>>, action: Action) {
    lock_shared(shared).actions.push_back(action);
}

fn title_from_name(name: &str) -> String {
    let leaf = name.rsplit(['/', '\\']).next().unwrap_or(name).trim();
    let title = std::path::Path::new(leaf).file_stem().and_then(|stem| stem.to_str()).unwrap_or(leaf).trim();
    if title.is_empty() { "Untitled document".into() } else { title.to_string() }
}

fn download_docx(app: &WordApp, shared: &Arc<Mutex<Shared>>) {
    let name = format!("{}.docx", title_from_name(&app.session.doc.core.title));
    let bytes = match wordcraft_engine::io::save_bytes(&name, &app.session.doc) {
        Ok(bytes) => bytes,
        Err(error) => {
            lock_shared(shared).status = format!("DOCX export failed: {error}");
            return;
        }
    };
    let result = app.services.download.as_ref().map(|download| download(&name, &bytes));
    match result {
        Some(Ok(())) => lock_shared(shared).status = format!("Downloaded {name}; server save status is unchanged."),
        Some(Err(error)) => lock_shared(shared).status = format!("DOCX download failed: {error}"),
        None => lock_shared(shared).status = "Browser download is unavailable.".into(),
    }
}

async fn fetch_json<T: DeserializeOwned>(method: &str, path: &str, body: Option<Value>) -> Result<T, String> {
    let window = web_sys::window().ok_or_else(|| "browser window is unavailable".to_string())?;
    let options = RequestInit::new();
    options.set_method(method);
    let has_body = body.is_some();
    let serialized_body = body.map(|value| serde_json::to_string(&value).map_err(|error| error.to_string())).transpose()?;
    if let Some(body) = serialized_body.as_deref() {
        options.set_body(&JsValue::from_str(body));
    }
    let request = Request::new_with_str_and_init(path, &options).map_err(js_error)?;
    if has_body {
        request.headers().set("Content-Type", "application/json").map_err(js_error)?;
    }
    let response = JsFuture::from(window.fetch_with_request(&request)).await.map_err(js_error)?;
    let response = response.dyn_into::<Response>().map_err(js_error)?;
    let status = response.status();
    let response_text = JsFuture::from(response.text().map_err(js_error)?).await.map_err(js_error)?;
    let response_text = response_text.as_string().unwrap_or_default();
    if !response.ok() {
        let message = serde_json::from_str::<Value>(&response_text)
            .ok()
            .and_then(|value| value.get("error").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_else(|| response_text.clone());
        return Err(format!("HTTP {status}: {message}"));
    }
    serde_json::from_str(&response_text).map_err(|error| format!("invalid server response: {error}"))
}

fn js_error(error: JsValue) -> String {
    error.as_string().unwrap_or_else(|| format!("{error:?}"))
}

fn now_ms(ctx: &Context) -> f64 {
    ctx.input(|input| input.time * 1000.0)
}

fn now_ms_raw() -> f64 {
    web_sys::window().and_then(|window| window.performance()).map(|performance| performance.now()).unwrap_or_default()
}

fn format_date(timestamp_ms: i64) -> String {
    let date = js_sys::Date::new(&JsValue::from_f64(timestamp_ms as f64));
    date.to_locale_string("zh-CN", &JsValue::undefined()).as_string().unwrap_or_else(|| timestamp_ms.to_string())
}
