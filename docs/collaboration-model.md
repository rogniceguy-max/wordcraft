# Collaboration model assessment

## Audit findings

WordCraft's `Pos` combines a story, a vector-index `Path`, and a UTF-8 byte offset. A path can
change when an earlier block or table row is inserted. Paragraph edits validate byte boundaries,
but transmitting a `Pos` as the durable address of a concurrent edit would leave it vulnerable to
structural changes.

`Session` undo/redo stores and restores whole `Document` snapshots. Once a remote edit is applied,
restoring one of those snapshots can erase that remote work. The existing server uses versioned
snapshot compare-and-swap, which rejects stale saves safely but does not merge or broadcast edits.

## Prototype result

`crates/engine/tests/collaboration_prototype.rs` remains an isolated Yrs experiment. The web editor
now also has a narrow live-text path in `apps/wordcraft-web/src/collaboration.rs`, and the Rust
server exposes `/api/documents/{id}/collab` as a binary Yrs WebSocket. It reuses the existing
WordCraft `Session`, command dispatch, document model, and renderer; it does not replace the editor
with a textarea.

The current adapter accepts exactly one body paragraph with plain text and direct bold, italic, and
single underline formatting on selected text. It rejects other blocks, paragraph formatting,
pending formatting at the paragraph mark, other character styles, inline objects, section breaks,
comments, revisions, sources, track changes, document protection, and unsupported control
characters. Formatting can be seeded from an eligible saved document and is persisted with live
updates. WordCraft positions are UTF-8 byte offsets. The browser adapter uses Yrs UTF-16 indexing
for stable relative selection anchors and converts at the boundary; tests cover Han text, emoji,
reversed selections, inserts, deletes, formatting, and actor-scoped undo/redo. The server stores a
SHA-256-deduplicated Yrs update log in SQLite, updates the materialized document snapshot in the
same transaction, and broadcasts only after commit. Before storage, it scans every raw Yrs format
item, including zero-length markers omitted from visible text diffs; this framing scanner is tied
to the pinned Yrs 0.28.0 version. Replayed exact updates receive an
acknowledgment only on the reconnecting socket. Server restarts rebuild the live state from the
seed and update log.

This is a P3/P4.2 narrow prototype, not the full collaboration product. Same-tab reconnect runs
automatically while the same document remains selected, with exponential delay from 500 ms capped
at 30 seconds and a 15-second connection timeout. Users can pause retries. Editing stays disabled
until the server's current state is received; then the client retries exact unacknowledged update
bytes and waits for durable acknowledgments. Socket generation tags discard events arriving late
from an older connection. If a browser reload recovers an unacknowledged live edit, it keeps an
immediate local snapshot and asks the user to make a separate server copy or discard it; it does not
automatically rejoin that snapshot to the CRDT. Updates that are confirmed by the server are
available from the current document after reopening. The per-document update log has no compaction,
and live edits do not create immutable version-history rows. Snapshot save and restore are blocked
after a live-text epoch is seeded. The durable epoch flag is returned as
`snapshotLocked` on document-list and document-detail responses, so the browser keeps the save and
restore warning visible after opening the document again or reloading the page. Local recovery
drafts use a per-page key; a new page context rotates a copied session-storage ID before writing,
while a same-tab reload reuses its key. The server takes an OS advisory lock so only one WordCraft
API router can serve a given canonical database path at a time.

This P4.2 slice does not provide shared presence, cursor broadcast, heartbeat timer, table-cell
editing, structural editing, or multi-paragraph collaboration. Browser-level three-tab localhost
smoke has passed for edits in both directions and actor-scoped undo. A separate two-tab smoke
confirmed that selected bold, italic, and single underline reached both editors and the materialized
server snapshot; typing inside the styled run preserved those attributes in the server snapshot.
Automatic reconnect backoff and stale-socket filtering have unit coverage, while browser outage and
recovery remain unverified. Reopening the document after a page reload also showed the durable
snapshot-save/restore fence. These browser checks ran on one Windows host; they do not verify
separate computers, IME behavior, DOCX round-trip, Mac mini startup/reboot, or the target performance
matrix.

## Integration gate

The next gate is to expand the adapter to stable paragraph and table-cell identities, broaden
character and paragraph formatting, support range operations safely, and add presence, cursor and
selection transport. It also needs a safe path to reconcile pending updates
after reload, update-log compaction, and a history model that can snapshot/restore a collaborative
epoch without letting stale sessions overwrite it. Automated multi-client acceptance should cover
same-position insertions, delete/insert races, formatting, table cells, IME composition, and server
restart before broadening the enabled command set.

Yrs updates are idempotent and commutative, so transport delivery order need not define the document
order. Native whole-document undo is reset while live text is active; actor-scoped Yrs undo handles
the supported text operations. Any adapter must validate text size and character boundaries before
calling Yrs text methods, which panic for out-of-range indexes.

Candidate references: [Yrs text offsets and formatting](https://docs.rs/yrs/latest/yrs/types/text/trait.Text.html),
[Yrs origin-scoped undo](https://docs.rs/yrs/latest/yrs/undo/struct.UndoManager.html), and
[Yrs document updates](https://docs.rs/yrs/latest/yrs/struct.TransactionMut.html).
