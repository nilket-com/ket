# UAST Inspector (`:uast`)

Status: implemented 2026-07-28.

Canonical command `:uast` (the panel shows storage truth, not the parser's
AST); `:astview` retained as a discoverable alias.

## Motivation

A live split-pane microscope showing what libaryon *actually stores* for a
document — not the parser's transient AST. Primary uses: debugging structural
diff/graft behavior, folding, sparse residency, and answering the architectural
question of whether the uast is rich enough to hang navigation off of. Before
this work, provider-specific kind vocabulary was erased at graft time; the
inspector implementation adds the compact identity channel needed to expose it.

## Two layers, because storage has two regimes

- **Raw UAST/storage view** — always available. Physical extents, virtual
  leaves, Group fan-out, residency, metric knowledge, topology. For docs at or
  above `MAX_SEMANTIC_BYTES` (8 MiB, `engine/core.rs:30`) this is the *only*
  view: no semantic pass runs, the tree stays sparse physical extents hydrated
  around the viewport (`engine/render.rs:~960`).
- **Semantic view** — available only after a structural graft
  (`engine/semantic.rs:81` → `engine/structural_graft.rs:104`). Adds
  provider-specific kinds once those are threaded through (stage 3).

The panel header states the regime explicitly instead of silently showing a
disappointing flat tree:

```
topology: sparse physical · semantic: unavailable (>8 MiB) · resident: 3/412 extents
```

## Hard rules

1. **Metadata-only traversal, zero I/O.** Snapshot construction must never
   call `read_node_bytes_sync`, request hydration, or build a whole-document
   snapshot. `apply_graft` copies source into `virtual_data` on every leaf
   (`structural_graft.rs:195`), and normal projection force-hydrates cold
   leaves (`render.rs:960` `hydrate_whole_leaf`) — the inspector must not use
   that path. Snippets are included only when bytes are already resident; cold
   nodes honestly say `cold`.
2. **Stamped node references.** A row's NodeId is valid only with its stamp
   `(BufferId, root NodeId, StateId, TopologyRev)`. The root survives grafts
   (only its descendants are replaced, `structural_graft.rs:117`); descendants
   do not. Click/jump rules:
   - Stamp matches → use NodeId directly.
   - *Source-range jump*: exact whenever StateId matches, even if TopologyRev
     advanced (grafts require byte-identical content, `validate_graft`).
   - *Inspector-node remap* (re-selecting "the same node" after a graft): a
     byte range does not uniquely identify a node — nested nodes share
     identical extents. Requires range + provider kind + ancestry path, and
     may remain ambiguous; fall back to the deepest match.
   - StateId differs → offsets are stale; reject the stale click and refresh.
     Do not silently clamp an intentional node selection.
   Never retain descendant NodeIds across graft revisions.
3. **Bounded output.** Depth limits + fold state + viewport-driven rendering.
   ~1 node per token incl. trivia (20–50k nodes for a 1k-line Rust file);
   whole-tree dumps are never rendered. `Group` fan-out padding
   (`TARGET_TREE_FANOUT=64`, `uast/topology.rs`) and trivia tokens collapsed
   by default.

## Semantic freshness

Add explicit `applied_structural_state_id` bookkeeping so the panel can
distinguish (current request tracking records only what was *asked for*):

- topology matches current source (graft applied at this StateId)
- edited topology, last-graft labels (registry drifted since graft)
- reparse pending
- flat/sparse (never grafted / over limit)
- graft failed or unavailable

This is what makes "watching the drift" (flat → grafted → edited → regrafted)
trustworthy.

## Substrate

**Not** `ReplaceDocument { virtual_path: "ast://…" }` — that makes derived UI
into ordinary (editable, language-serviced) source and risks feeding the
rendered tree back through the semantic pipeline.

Preferred: a dedicated `BufferContent::AstInspector` buffer/view kind in the
file-browser spirit (`engine/file_browser.rs`: synthetic text + parallel row
model for hit-testing), because the inspector needs interactive state (fold
set, follow-cursor mode, selection, raw/semantic mode toggle) plus the stamped
row→node mapping. Highlighting is **producer-owned spans** (the output-panel
pattern, `session.rs:3746` re-applied highlights) — we generate the rows and
know their semantics; do not re-lex our own output through a provider.

Implementation note: the first cut uses the existing stable one-root/one-leaf
output-buffer storage plus a dedicated `UastInspectorPanel` sidecar. The
sidecar owns source/view identity, mode, stamps, row mappings, and the bounded
graph snapshot; `BufferRole::OutputPanel` supplies read-only behavior and
semantic-pipeline exclusion without adding another document-shaped enum
variant. This preserves the contract above while avoiding duplicated text
projection machinery.

Refresh: per-frame staleness check in `render_frame` — same pattern as
`refresh_markdown_image_previews` (`render.rs:172`) and `recompute_all_diffs`
(`render.rs:169`). The inspector pairs with **both a source BufferId and a
source ViewId**: topology belongs to the buffer, but cursor-following and jump
targeting belong to a particular view. Full refresh identity:

```
(source BufferId, source ViewId, StateId, TopologyRev,
 source cursor byte, inspector fold/mode revision, pane geometry,
 registry inspector revision)
```

The registry revision covers residency, metric-certainty, allocation, and
topology changes that do not otherwise alter the source state stamp. Snapshot
construction remains metadata-only; the revision lets a per-frame check avoid
walking the tree when none of those inputs changed.

Implemented lifecycle: while a source view is focused, the inspector follows
that view. If it closes, another open view of the same buffer is acquired; if
none remains, the inspector degrades to buffer-only mode with cursor following
paused. If the source buffer itself closes, the inspector remains open with an
explicit detached message. `f` toggles cursor following, `Space` folds the
selected node, and `Enter` jumps to its stamped source range.

Command plumbing: `CommandSpec` in `engine/input/command_registry.rs` +
`EditorCommand` variant + `split_pane_with_new_pane_order`, per the
`:panediff` recipe (`plans/baryon/panediff-git.md`); regenerate
`snapshots/builtin-catalog.txt`.

## Provider kind ids (stage 3)

- New registry SoA column: per-provider `u16` kind id (paged, ~2 B/node
  amortized, no heap). Not `String`s — NodeIds are never reclaimed, so
  per-node heap would accumulate forever.
- `0` = no provider identity — used for synthetic groups, physical extents,
  and mutation-born unknown nodes alike. No reserved value for `Group`: it is
  already represented by the universal `SemanticKind`, and the provider
  namespace stays strictly provider-owned (a Group row renders as
  `provider_kind=0 · semantic_kind=Group · origin=synthetic`). Nonzero values
  interpreted via the buffer's active language provider (`id -> display name`
  mapping on the provider). Display strings are derived transiently for the
  bounded snapshot; they are never retained in the registry.
- Per-provider numbering, **not** a shared cross-language namespace — no
  universal enum pretending Rust `FN`, SQL `SELECT`, CSV `Field` are one
  taxonomy. Universal `SemanticKind` remains the cross-language classification
  for diffing and motions.
- New nodes created by in-place mutation get `unknown` until the next graft.
- Threading: extend `GreenNode` (`engine/language.rs`) with the u16; populate
  in providers (`rust_kind_to_semantic` site `svp/rust_provider.rs:343`,
  `sql_semantic.rs`, `csv_semantic.rs`, …); carry through `apply_graft`.
- Names/snippets derived lazily from byte ranges at snapshot time (resident
  bytes only), never stored per node.

## Memory diagnostics (cheap, high value — but bounded like everything else)

The real existing memory debt: grafted leaves hold non-reconstructible
`virtual_data`, orphaned graft generations are unlinked but never reclaimed
(`registry.rs` `disconnect_node`; `plans/baryon/object-identity.md`), and
resident-budget eviction skips data without a physical backing span. Panel
header exposes: reachable node count, allocated-node high-water mark
(`next_id`, O(1)), reachable resident bytes.

Reachable count/bytes never require an unbounded walk. The implementation uses
a viewport-sized budgeted scan. Exact values are shown only when traversal
completed within that budget; cursor-scoped or truncated results are rendered
as explicit lower bounds (`reachable >= N`, `resident >= B`). A cheap registry
revision invalidates cached headers when allocation, residency, metrics, or
topology changes, so unchanged frames do not repeat even that bounded scan.

Metric certainty is exposed explicitly: a sparse node's newline count reports
`unknown | partial | exact` — reading it must never trigger metric inflation.

## Staging

1. Metadata-only, no-I/O live UAST snapshot API in libaryon: NodeId,
   parent/depth, universal kind, byte/newline metrics, child count,
   residency/storage state, semantic freshness. Walk-time prefix-sum offsets
   (à la `UastQuery::node_bounds`).
2. Textual `:uast` split with raw/semantic modes, stamped row mapping,
   row-select → source jump, cursor → ancestor-path follow, bounded traversal.
3. Thread per-provider u16 kinds through green node → graft → registry.
4. Lazily derived resident snippets; richer SQL kind coverage. CSV remains
   row-granular (`Table` / `Row` / `Token`) deliberately: introducing a node
   per field regressed the registry's bounded-fanout and large-row behavior.
5. DOT export off the same snapshot; cursor-scoped graphical render in Baryon
   via the diff-bridge image pattern (`baryon/src/ui/diff.rs`: snapshot →
   fingerprint → worker thread → Kitty/sixel image; scoped subtree only, never
   whole-file hairball); Polariton renders the same snapshot interactively.
6. Go-to-definition afterward (Rust first, off the already-live
   `ra_ap_ide::AnalysisHost` in `svp/rust_provider.rs` — `goto_definition`
   currently unused).

Stages 1–5 are implemented. Stage 6 remains a separate navigation feature,
not part of the inspector implementation.

## Settled questions

- Command name: **`:uast`** canonical, `:astview` alias.
- `PaneViewport` payload slot: **deferred to the graphical stage (5)**. The
  textual stage rides ordinary projection plus engine-owned stamped row state.
- Polariton transport: **bounded protocol snapshot**. Polariton never
  traverses `UastRegistry` directly — that would undermine libaryon's
  headless boundary (`docs/architecture.md`).
- Source-view lifecycle: follow the focused paired view; reacquire a surviving
  view of the same buffer; degrade to buffer-only mode when only the buffer
  survives; display a detached state once the source buffer closes.
- Reachable diagnostics: viewport-budgeted traversal with explicit lower
  bounds, cached behind the registry inspector revision. No new per-root
  accounting column or unbounded scan.
