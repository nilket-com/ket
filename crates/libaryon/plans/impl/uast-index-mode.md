# `:uast index` — positional-index inspector mode

Scope: the existing UAST inspector gains an index mode for engine
development on sparse seek/splice paths. `:uast index` inspects the dynamic
root-child treap (`uast/root_line_index.rs` `RootChildLineIndex`);
`:uast index sumtree` inspects the cached fixed-fanout `SumTree` layout
index; `:treap` is a command alias for `:uast index`. **Integrated into the
existing microscope** — same panel, same stamps, same metadata-only and
bounded contracts. Deliverable is one `impl:` commit reviewed against this
plan.

## Why a mode and not just a header line

A health line (size/depth/frontier) answers "is it broken?"; the mode
answers the development questions nothing else can show:

- the lookup path for a given byte/line (which summaries a seek consulted);
- known-metric prefix boundaries as a visible frontier, not per-node marks;
- subtree byte/newline sums and where they stop being exact;
- whether an edit performed an O(log n) splice or forced a full rebuild;
- stale `NodeId → arena index` mappings and index-vs-registry drift.

## Default view: document order, not tree shape

The randomized shape is implementation noise; rank order is the honest
default. One row per entry, bounded to a viewport window around the focus
rank (same overscan/budget discipline as the node view, same
`>=N (truncated)` honesty):

```
rank  arena  node        bytes            lines      subtree(b/nl)   ✓
310   14     #42@g3      1.27M..1.31M     8101..8183 32K/640
311   7      #91@g0      1.31M..1.35M     unknown    12K/?           !
```

- `node` renders packed identity (`#slot@gN`) — stale-mapping bugs are
  visible as generation mismatches.
- `lines`/`subtree` render the `known_prefix_*` truth: `unknown`/`?` where
  scans have not reached; the frontier rank is called out in the header.
- **`✓`/`!` is a per-visible-row consistency check**: the entry's
  `byte_length` vs the registry's O(1) per-node metric for that `NodeId`
  (and generation currency via `is_current_node`). Cheap — one column read
  per visible row — and it turns index-vs-registry drift from a latent bug
  class into a visible marker. No subtree recomputation; visible rows only.

This schema is specific to the root-child treap. `SumTree` does **not**
index `NodeId`s: its leaves contain `LineMetric` values in a private
fixed-fanout arena. Its document-order rows therefore render:

```
rank  leaf  slot  bytes          doc-lines  visual-rows
310   4     54    1.27M..1.28M  8101..8102 8101..8102
```

The SumTree header checks its root summary against the owning
`CachedSumTree` totals. Do not fabricate a `NodeId`, registry-consistency
marker, or stale-generation claim for a line-metric entry.

## Cursor follow = lookup-path trace

Reuse the panel's existing follow semantics: with follow on, the source
cursor's byte drives a `location_for` style seek, and the mode highlights
the consulted path — the focus row plus, in a `links:` detail line for the
selected row, its parent/left/right arena links and the rank/summary
decisions the descent made. Selecting a row manually (follow off) shows the
same detail for that row. This is read-only tracing of the same query the
engine runs; it must call the index's pure lookup, never mutate, never
hydrate (the index is pure metadata already — keep it that way
structurally: the snapshot builder takes `&RootChildLineIndex`).

## Operation counters and stamps (the splice-vs-rebuild diagnostic)

The counters and revision belong to the registry boundary, **not to a
`RootChildLineIndex` instance**. A rebuild replaces that instance, so
instance-local `rebuilds` or `index_generation` would reset and could repeat
a stale row's stamp.

- Reuse the existing process-global
  `root_child_line_index_full_builds: AtomicU64`.
- Add a process-global monotonic splice/update counter and a process-global
  monotonic root-index revision. The revision advances at publication
  boundaries for installed-index build, effective update/reconcile, removal,
  and replacement. Conservative cross-root invalidation is acceptable;
  repeating a revision is not.
- A treap row stamp is the ordinary source stamp plus this root-index
  revision. A stale click is rejected and refreshed; no rank remap is
  attempted because an index row is a diagnostic observation, not durable
  document identity.

Header shows registry-global `splices=S rebuilds=R` — an edit loop that
should splice but rebuilds every keystroke becomes visible in two frames.
The revision joins the mode's refresh key.

The cached SumTree already has an identity tuple
`(root, StateId, total_lines, total_bytes, topology_revision)`. Its stamp
adds the existing registry-global `sum_tree_rebuilds` count so dropping and
rebuilding an otherwise identical cache cannot repeat an observation.

## Header

```
INDEX root=#7@g1 · treap · extents=412 depth=11 (log2=9) known=1.2G/2.1G ·
splices=48 rebuilds=1 · rev=49 · reachable-rows=…
```

The three health numbers (size, depth vs ideal, frontier) live here — the
monitoring answer rides along with the development mode. The sumtree
submode adapts labels (`entries=`, `levels=`, `rebuilds=`; no splice counter
— it is rebuilt as a cache, so say so rather than showing a meaningless 0).
Keep the index-mode preamble at the inspector's fixed six header lines so
existing row coordinates and viewport-budget math remain valid.

## Wiring

- `UastInspectorMode` gains `Index { kind: PositionalIndexKind }`
  (`Treap | SumTree`); `cmd_uast` parses `index` / `index sumtree`;
  `CommandSpec` for `:treap` dispatches `OpenUastInspector { mode:
  Index(Treap) }`; catalog/help regenerated.
- Keep the existing node rows and graph state untouched. Add a bounded
  index-row sidecar with an enum target (`RootExtent { NodeId, rank }` |
  `SumTreeEntry { rank, leaf, slot }`) and common source range/stamp.
  `Enter` jumps to the entry's start byte; `Space` reports that index rows
  are flat rather than entering the node-fold path; `f` keeps its existing
  follow meaning.
- Documents without the relevant index (grafted semantic trees may have no
  root-child index; small docs may have no sumtree cache) show the regime
  honestly: `index: none for this root` — never build one to display it.

The cached SumTree is owned by the text document's semantic state (or the
file-browser document for that content kind), not by `UastRegistry`.
Inspection may only clone/read an existing `CachedSumTree`; it must never
call `refresh_sum_tree`. Because inspector refresh currently precedes pane
projection (where the cache is normally built), SumTree cache
presence/identity must participate in the refresh key: an initial
`index: none` heals on the following frame after ordinary source rendering
constructs the cache.

Both index implementations expose owned, bounded diagnostic snapshots from
their defining modules. The session/UI layer must not reach into private
treap links or `TreeNode` arena storage.

## Tests

1. Document-order window correctness against a registry fixture: ranks,
   ranges, and subtree sums match an exact fold; truncation honest.
2. Lookup-path trace: for a chosen byte, the highlighted path equals the
   descent the real seek performs (instrument the pure lookup in test).
3. Consistency marker: a test-injected metric drift (registry metric
   changed without index update, via test-only access) flips `✓` to `!`
   for exactly that row.
4. Counters: an installed-index splice bumps the global splice count only;
   installation/rebuild bumps the existing build count; the registry-issued
   revision never repeats when an index instance is replaced and invalidates
   stamped rows (stale click rejected).
5. Frontier: partially-scanned fixture shows `unknown` rows and the
   header frontier; completing the scan (existing machinery) heals both.
6. No-index regimes render the honest placeholder; no index is built as a
   side effect of inspecting (assert root-index and SumTree construction
   counters unmoved). An initially absent SumTree cache appears after normal
   source projection without manual invalidation.
7. Metadata-only: existing no-hydration inspector test extended to the
   index mode.
8. SumTree rows are leaf-entry metrics in document order, have no fabricated
   `NodeId`, and their traced descent equals the production query path.

## Non-goals

- Graphical tree-shape mode (priorities/rotations are noise; revisit only
  with a concrete balance bug in hand).
- No new pane architecture, no new buffer kind, no Polariton surface.
- No changes to index behavior itself — counters and read-only snapshots
  only.

## Review checklist

- [ ] `:uast index`, `:uast index sumtree`, `:treap` alias; catalog + help.
- [ ] Bounded rank windows and non-repeating registry/cache stamps in refresh.
- [ ] Per-row consistency marker is O(visible rows), reads only.
- [ ] Treap and SumTree path traces equal their real seeks (test-proven).
- [ ] Splice/rebuild counters bump at the right sites and nowhere else.
- [ ] No-index and sumtree regimes honest; nothing constructed to display.
- [ ] SumTree rows do not invent registry-node identity.
- [ ] Existing node/graph rows and graph state remain untouched.
