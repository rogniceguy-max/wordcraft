# WordCraft roadmap

WordCraft aims for complete feature parity with Microsoft Word, then goes further on speed, openness
and agent control. This file tracks where we are honestly. Generated numbers come from
`cargo xtask parity` (`docs/parity.md`).

## Where we are (2026-10-07)

| Measure | Value |
|---|---|
| Commands (every action, scriptable by CLI/MCP/control channel) | **389** |
| Feature catalog coverage (Word ribbon/menu features with a command) | **354 / 405 (87%)** |
| **Estimated real feature parity** (depth and fidelity, not just a command) | **~62%** |
| **Distance to alpha** | **~85% of the way; ≈20–25 h of Claude wall-clock work** |
| **Distance to 100% parity** | **≈120–150 h of Claude Opus 5.5 wall-clock work** (with parallel agents) |
| Tests | ~225 test functions (unit, round-trip, fuzz/proptest, MCP acceptance) |
| Layout speed (188-page document) | 61 ms cold, **1.4 ms** relayout after an edit |
| Code | ~44k lines of Rust in 12 crates and 3 apps |

Catalog coverage overstates parity: many commands are first versions. The real-parity estimate
weighs each area by how much of Word's behaviour it reproduces, and by how often people use it.

## Alpha

**What we mean by alpha:** someone can write, format and review real documents every day, open
the .docx files they receive and send back files Word opens cleanly, on every desktop platform
from a signed download, without losing work.

| Alpha requirement | State |
|---|---|
| Editing, formatting, styles, lists, tables, headers/footers, references, review | done (first versions, tested) |
| Pagination fidelity: wrap, row splitting, hyphenation, drop caps, line numbers, borders | done this arc |
| Comments in margin balloons, track changes | done |
| Never-crash standard (no panics, hostile-param fuzzing, panic guard) | done |
| Autosave and crash recovery | done; needs a soak test |
| **DOCX fidelity against a corpus of real-world files** (open, render, round-trip, fix) | **not started — the main alpha blocker (≈10 h)** |
| Footnotes that continue onto the next page; column balancing | missing (≈4 h) |
| Native printing (today printing goes through PDF) | missing (≈4 h) |
| Signed builds for macOS, Windows, Linux, FreeBSD and web | pipeline written; **needs the GitHub remote and release secrets from the owner** |
| Real app icon / mascot art | placeholder; **needs owner art** |

So: alpha is roughly **85% of the way**, with about **20–25 hours** of agent work left, plus two
owner actions (push to `github.com/storytold/wordcraft` and enable the release secrets, and supply
the icon art).

## Parity by area

| Area | Status | Parity |
|---|---|---|
| Typing, selection, clipboard, undo, find/replace | Solid; IME, autocorrect, smart quotes, list autoformat | 85% |
| Character & paragraph formatting | Nearly all properties; Font/Paragraph dialogs | 85% |
| Styles (gallery, pane, create/modify, style sets, themes) | Good | 75% |
| Lists (bullets, numbering, multilevel, restart, set value) | Good | 75% |
| Tables (insert, merge/split, styles, borders, header rows, sort, formula) | Good; rows split across pages; no drawn tables, no nested-table polish | 72% |
| Page layout (margins, size, orientation, columns, breaks, sections) | Good; page borders, line numbers, vertical alignment, drop caps, hyphenation; columns don't balance | 72% |
| Headers/footers, page numbers, fields | Good; first/even/odd, link to previous | 70% |
| Footnotes/endnotes | Placed and editable; long notes don't continue onto the next page | 60% |
| References (TOC, citations APA/MLA/Chicago/IEEE, bibliography, index, figures, cross-refs, TOA) | Working first versions | 60% |
| Review (spelling, grammar, thesaurus, comments, track changes, compare, protect) | Good; comment balloons in the margin; no track-changes balloons | 75% |
| Mailings (mail merge, rules, preview, envelopes, labels) | Working | 70% |
| Pictures & shapes (insert, size, crop, recolour, effects, styles, float position) | Text wraps around floats (rectangular); text boxes render their text; no tight/contour wrap, no rotation handles | 55% |
| Draw tab (ink), SmartArt, charts, 3D models, equations editor | Not started / linear equations only | 5% |
| File formats: DOCX read/write | Good (Word opens our files); charts/SmartArt/OLE dropped; untested on a real-world corpus | 75% |
| File formats: PDF, ODT, RTF, HTML, Markdown, TXT | Working | 70% |
| View modes (print, web, draft, read, focus, zoom, navigation pane) | Working | 70% |
| Backstage (new from templates, open, info, export, options) | Working; printing goes through PDF | 55% |
| Agent control (CLI, MCP, control channel, macros) | Beyond Word | 100%+ |

## Estimate to 100%

**About 120–150 hours of Claude Opus 5.5 wall-clock work** (with parallel agents), in this order:

1. **Alpha blockers** (≈20–25 h): DOCX fidelity corpus, footnote continuation, column balancing,
   native printing, autosave soak test, first signed release.
2. **Objects** (≈15 h): tight/through (contour) wrap, rotation and resize handles on the canvas,
   grouping, z-order polish, track-changes balloons and formatting revisions.
3. **Draw tab / ink** (≈15 h).
4. **Charts (own renderer) and SmartArt-style diagrams** (≈20 h).
5. **Equation editor (OMML read/write, 2D layout)** (≈15 h).
6. **Dialog depth**: every Word dialog with all its options (Font, Paragraph, Tabs, Borders and
   Shading, Page Setup, Styles, Columns, Index/TOC options, Mail Merge wizard, Options panes) (≈20 h).
7. **Accessibility (screen readers), localisation, RTL and complex scripts** (≈15 h).
8. **Long tail**: kerning/ligature options, drawn tables, nested-table polish, more templates,
   Word's less-used commands still missing from the catalog (≈10 h).

## Milestones

| # | Milestone | State |
|---|---|---|
| M0 | Skeleton + vertical slice (model, layout, render, engine, Word-style UI, CLI, MCP, web) | **done** |
| M1 | DOCX I/O | **done** (first version; corpus testing next) |
| M2 | Home tab complete | **done** |
| M3 | Insert tab | mostly done (charts/SmartArt/icons/3D missing) |
| M4 | Layout + Design tabs | **done** except column balancing |
| M5 | Tables | **done** (row splitting landed) |
| M6 | References | first version done; footnote continuation missing |
| M7 | Review | **done** (margin balloons landed) |
| M8 | View | mostly done |
| M9 | Mailings | done (first version) |
| M10 | File/Backstage | mostly done; native printing missing |
| M11 | Draw + objects | text wrap and text boxes done; ink, rotation, grouping missing |
| M12 | Formats breadth (PDF, ODT, RTF, HTML, MD, TXT) | **done** (first versions) |
| M13 | Performance budgets | on track (1.4 ms relayout) |
| M14 | 1.0 polish, packaging, signing | pipeline written; waiting on remote and secrets |

## Recently landed
- Text wraps around floating pictures and shapes; text boxes lay out their own text.
- Table rows split across pages between lines (Can't Split honoured, header rows repeat).
- Drop caps; automatic hyphenation with Word's 0.25" hyphenation zone; soft hyphens.
- Page borders measured from the page edge, line numbers, vertical alignment, column separators.
- Comment balloons in a markup area beside each page, with leader lines to their anchors.

## Next
The collaboration milestone for the WordCraft Web internal document system is tracked separately
from desktop feature parity:

| Web collaboration phase | State |
|---|---|
| P0 architecture audit | **done**; `docs/collaboration-model.md` records the positional-path and snapshot-undo hazards |
| P1 browser build and DOCX round-trip | Trunk release build and local WebGPU/WebGL2 browser startup/input/server-save smoke passed; DOCX browser round-trip and Chrome/Edge/Safari/IME matrix remain unverified |
| P2 server persistence, CAS versions and restore | implemented; workspace CI and local HTTP/browser save smoke passed, including stale-version conflict and restore; Mac restart recovery remains unverified |
| P3 live collaborative text | **narrow prototype wired**; durable Yrs WebSocket updates, one paragraph with supported text and selected-character formatting, Unicode position mapping, delete/undo isolation, exact replay ACK, restart replay, snapshot-lock persistence, per-tab recovery keys, and single-server DB lock have automated coverage; local three-tab browser smoke passed for text edits and actor undo |
| P4 rich-text/table collaboration and presence | **partial (P4.1)**; selected-text bold, italic, and single underline sync in the one-paragraph live-text slice; two-tab localhost smoke on Windows confirmed formatting in both editors and the server snapshot, and typing inside a styled run preserved its attributes; multiple paragraphs, table cells, shared cursors/presence, heartbeats, automatic reconnect, and automatic reload rejoin remain unsupported |
| P5 per-browser pure-black page display | implemented and covered by renderer/UI tests; in-browser black-theme visual review remains pending |
| P6 dated backup and restore tooling | implemented; workspace CI and local backup verify/restore smoke passed against an isolated test database; launchd scheduling remains unverified on macOS |
| P7 Mac mini deployment and browser acceptance | not run; no target Mac is connected, and scenarios A–I have not passed end to end |

These results validate the web shell, persistence API, a narrow text and selected-format collaboration slice, theme, and backup components. The local browser smoke used three tabs on one Windows host; it does not validate two separate computers, IME composition, Chrome/Edge/Safari, Mac reboot, or the performance matrix. The collaboration slice deliberately blocks unsupported edits and snapshot restore. It does not meet the final success criterion until the remaining P4 work and Mac/browser acceptance are completed.

The DOCX fidelity corpus, then footnote continuation, column balancing and native printing remain
the next desktop parity priorities.
