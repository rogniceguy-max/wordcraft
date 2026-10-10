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

`crates/engine/tests/collaboration_prototype.rs` is a deliberately isolated Yrs experiment for one
synthetic paragraph containing plain text. It exercises concurrent insert updates in opposite
delivery orders, a local undo that preserves another actor's insertion, redo convergence, and a
UTF-8 byte-offset projection into a WordCraft paragraph. The Yrs default text offset is UTF-8 bytes,
matching WordCraft's paragraph offsets. Yrs also provides text attributes and origin-filtered undo
managers, which are promising primitives for a later adapter.

This prototype is not connected to `Session`, the web editor, or the server. It does not validate
paragraph identity, selection/cursor mapping, deletes, formatting conflicts, tables, attachments,
comments, revisions, or restart persistence of collaboration state. It is evidence for a text-level
candidate only, not a real-time collaboration implementation.

## Integration gate

Before connecting the prototype, the production design still needs stable IDs for paragraphs and
table cells; a conversion that preserves `Run` attributes and inline objects; relative anchors for
WordCraft selections; durable update persistence, replay and compaction; duplicate-safe reconnect;
explicit handling of unsupported structural edits; and a web transport with presence and cursor
state. Yrs updates are idempotent and commutative, so transport delivery order need not define the
document order. Native whole-document undo must be replaced or isolated from collaborative text
edits before remote operations are applied to a `Session`. Any adapter must bounds-check offsets and
verify UTF-8 boundaries before calling Yrs text methods, which panic for out-of-range indexes.

Candidate references: [Yrs text offsets and formatting](https://docs.rs/yrs/latest/yrs/types/text/trait.Text.html),
[Yrs origin-scoped undo](https://docs.rs/yrs/latest/yrs/undo/struct.UndoManager.html), and
[Yrs document updates](https://docs.rs/yrs/latest/yrs/struct.TransactionMut.html).
