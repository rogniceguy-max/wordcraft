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

The current adapter accepts 1–256 fixed top-level body paragraphs with plain text and direct bold,
italic, and single underline formatting on selected text. Paragraph order and count remain fixed
for the collaboration epoch. Paragraph 0 retains the legacy `body:main` Yrs root; later paragraphs
use `body:paragraph:{index}`, with the count recovered from the materialized snapshot. This keeps
existing single-paragraph P3 epochs readable. Editing and formatting must stay within one
paragraph; paragraph insertion, deletion, merging, and other structure changes are rejected or
reverted. It also rejects tables, paragraph formatting, pending formatting at the paragraph mark,
other character styles, inline objects, section breaks, comments, revisions, sources, track changes,
document protection, and unsupported control characters. The combined text limit is 8 MiB.

Formatting can be seeded from an eligible saved document and is persisted with live updates.
WordCraft positions are UTF-8 byte offsets. The browser adapter uses Yrs UTF-16 indexing for stable
relative selection anchors and converts at the boundary; tests cover Han text, emoji, selection
mapping across roots, inserts, deletes, formatting, and actor-scoped undo/redo across paragraphs.
The server stores a SHA-256-deduplicated Yrs update log in SQLite, updates the materialized document
snapshot in the same transaction, and broadcasts only after commit. Before storage, it scans every
raw Yrs format item, including zero-length markers omitted from visible text diffs; this framing
scanner is tied to the pinned Yrs 0.28.0 version. Replayed exact updates receive an acknowledgment
only on the reconnecting socket. Server restarts rebuild the live state from the seed and update log.

This is a P3/P4.3 narrow prototype, not the full collaboration product. Same-tab reconnect runs
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

P4.3 provided no presence or cursor broadcast. P4.4 adds an independent ephemeral WebSocket at
`/api/documents/{id}/presence`. It assigns a random session ID and a temporary `Guest N` name,
broadcasts the current roster, accepts only small JSON heartbeat messages, expires sessions after
45 seconds without a heartbeat, and sends a refreshed roster on join or leave. Presence is limited
to existing, non-deleted documents and 128 concurrent participants per document. It does not write
to the document, version history, Yrs state, or update log. The client opens it only while the
durable text channel is live, sends a heartbeat every 15 seconds, retries it independently, ignores
stale socket events, and clears names when the channel stops or fails. These names only distinguish
temporary sessions; they do not authenticate anyone. Focused server/client tests cover roster
fanout, heartbeat expiry, capacity and cleanup, invalid messages, storage isolation, heartbeat
scheduling, generation filtering, and retry restart after the text channel recovers.

P4.5 extends the ephemeral channel with cursor and selection anchors. Each socket first receives its
own server-assigned session ID so the browser can omit its own cursor from the canvas. Cursor frames
carry paragraph indexes and base64-encoded Yrs sticky indexes for both selection endpoints; they do
not use fixed byte offsets and never enter the document or Yrs update log. The server strictly
validates the message shape, paragraph range, base64 value and size, and relays the latest roster.
The client coalesces updates, resolves each anchor against the expected Yrs paragraph root, then
converts Yrs UTF-16 positions to WordCraft UTF-8 positions. Unresolved or wrong-root anchors are
hidden without stopping text editing. Canvas overlays use the current frame's page geometry and
scale, remain outside page textures and local selection state, and show the temporary participant
label in a deterministic color. Tests cover Unicode rebasing, wrong-root rejection, parser bounds,
cursor fanout, throttle/coalescing, reconnect resend, and storage isolation.

P4.5 still does not support table-cell editing, paragraph structure editing, or automatic reload
rejoin. Browser-level three-tab localhost smoke has passed for edits in both directions and
actor-scoped undo. A separate two-tab smoke confirmed that selected bold, italic, and single
underline reached both editors and the materialized server snapshot; typing inside the styled run
preserved those attributes in the server snapshot. Presence and cursor overlays have not yet had
browser smoke. Automatic reconnect backoff and stale-socket filtering have unit coverage, while
browser outage and recovery remain unverified. Multi-paragraph collaboration also lacks browser
smoke. Reopening the document after a page reload showed the durable snapshot-save/restore fence.
The prior browser checks ran on one Windows host; they do not verify separate computers, IME
behavior, DOCX round-trip, Mac mini startup/reboot, or the target performance matrix.

## Integration gate

The next gate is fixed-topology table-cell text roots. Broader character and paragraph formatting
and safe range operations follow only after those mappings are tested. The collaboration path also
needs a safe way to reconcile pending updates after reload, update-log compaction, and a history
model that can snapshot/restore an epoch without letting stale sessions overwrite it. Automated
multi-client acceptance should cover same-position insertions, delete/insert races, formatting,
table cells, IME composition, and server restart before broadening the enabled command set.

Yrs updates are idempotent and commutative, so transport delivery order need not define the document
order. Native whole-document undo is reset while live text is active; actor-scoped Yrs undo handles
the supported text operations. Any adapter must validate text size and character boundaries before
calling Yrs text methods, which panic for out-of-range indexes.

Candidate references: [Yrs text offsets and formatting](https://docs.rs/yrs/latest/yrs/types/text/trait.Text.html),
[Yrs origin-scoped undo](https://docs.rs/yrs/latest/yrs/undo/struct.UndoManager.html), and
[Yrs document updates](https://docs.rs/yrs/latest/yrs/struct.TransactionMut.html).
