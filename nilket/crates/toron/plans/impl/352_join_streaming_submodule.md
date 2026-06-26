# ADR 352 - Implementation plan: carve exec/join/streaming.rs (coupling-map gated)

Pairs with `plans/adr/352_join_streaming_submodule.md`. A pure `crates/toron` source-structure cut, **gated on the
coupling map**: carve the streaming "move now" bucket out of `exec/join.rs` (5,801 lines) into
`src/exec/join/streaming.rs` **only if** the map proves one-way dependencies; otherwise the deliverable is the
aggregate-first re-order recommendation. Behavior-neutral, exactly one carve.

## Phase 0 - the coupling map (the go/no-go; produce this BEFORE moving anything)
For every streaming symbol (`try_streaming_*`, `prepare_*`, `probe_*`, the streaming structs, the streamed-spilled/
partitioned paths, streaming free fns, streaming tests), record a row:

| symbol | kind (struct/fn/impl-method/test) | requires impl Executor? | aggregate touch (API call vs private internals) | filter/project/exchange touch (shared vs streaming-only) | residual/materialization touch | spill-API touch | test locality (local vs mixed) | **bucket** |

Bucket each into:
- **Move now** - deps are one-way calls into root/shared APIs + the 351 spill API.
- **Stay root** - shared (non-streaming join uses it too) / aggregate helpers (353) / generic filter/project/
  materialization.
- **Defer / re-scope** - move would drag aggregate internals, general dispatch, or broad test fixtures.

**Decision:**
- If aggregate folding (+ filter/project/materialization) is **mostly API calls** -> proceed with the move-now bucket.
- If streaming reaches **private aggregate internals** -> **stop; recommend aggregate-first (353)**; ADR 352's output is
  the map + the re-order rationale, not a forced carve.

## D1 - the carve (only the move-now bucket; only if map-proven)
- Create `src/exec/join/streaming.rs`; move the move-now symbols verbatim (no logic change), using a split
  `impl Executor` block for the qualifying methods; declare `mod streaming;` in the join root.
- Residuals (`match_filter`) move with streaming. Spill calls go through 351's `pub(in crate::exec::join)` API.
- **Stay-root + defer buckets do not move.** `pub(in crate::exec)` / `pub(in crate::exec::join)` only what the move
  requires; do not widen aggregate/shared helpers beyond what a one-way call needs.
- Mixed-harness streaming tests stay in the root test module (documented); only truly-local streaming tests move.
- `Executor`, `ExecutionContext`, `materialize_scalar_rows`, `pub mod exec;` reachable as before.
- **No-go branch:** if Phase 0 shows streaming needs private aggregate internals, stop before any source move. Write the
  coupling map and aggregate-first recommendation to the results doc; do not create an empty `streaming.rs`, do not add a
  fake module guard, and do not move a token partial cluster just to make the commit look like a carve.

## D2 - extend the guard (`exec_decomposition.rs`)
- **Only if D1 proceeds:** `exec_join_root_declares_streaming_module`; `exec_join_streaming_module_exists`.
- **Only if D1 proceeds:** `exec_join_root_excludes_streaming_clusters` - the moved streaming symbols are no longer in
  the join root.
- **Only if D1 proceeds:** `exec_join_root_is_bounded` ratcheted **down** below 5,801 (Phase-0 measured, fails on
  streaming drift-back).
- **Only if D1 proceeds:** extend the no-qNN scan to `exec/join/streaming.rs`; public-surface assertion unchanged.

## Tests / safety net (behavior-neutral is the whole game)
- **The sf=1 board is the tripwire:** `correctness-matrix` 22/22 + `matrix` 22/22 **bit-identical**.
- Full `--lib`, `--test architecture_test` (incl. the extended guard), `--no-fail-fast`, `toron-tpch` green.
- `check -p toron --all-targets` + `-p toron-tpch --all-targets`.
- **LSP diagnostics are advisory** - stale E0365/E0433/E0624 during the move are expected (350/351 experience); the
  authoritative signal is `scripts/cargo-cgroup check` + the architecture guard + the board commands. Ground on those,
  not the inline diagnostics.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # incl. exec_decomposition.rs
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix        # 22/22 unchanged (behavior-neutral)
scripts/cargo-cgroup run -p toron-tpch -- matrix                    # 22/22 unchanged
git diff --check
#   exec/join.rs -> join root + exec/join/streaming.rs; move-now bucket only; root ratcheted < 5,801; board bit-identical
```
- Run architecture_test AFTER fmt (the edit -> fmt -> fmt --check -> full architecture_test -> seal order).

## Results doc
`crates/toron/plans/impl/352_join_streaming_submodule_results.md`: the **three-bucket coupling map** (every streaming
symbol -> move-now / stay-root / defer); the go/no-go decision (carved vs aggregate-first recommended) + its
one-way-dependency evidence; if carved: join-root + `streaming.rs` line counts before/after + the guard threshold, the
`pub(in ...)` surface widened, which tests moved vs stayed; the behavior-neutral proof (sf=1 22/22 bit-identical + full
suite); confirmation the LSP errors seen during the move were stale (the `cargo-cgroup check` is clean). If no-go: the
results doc is the implementation artifact, records that no source split or guard extension was performed, and routes
the next ADR to aggregate-first (353).

## Risk / blast radius
- **Map-gated** - no move until the three-bucket table is produced; aggregate-first is the escape hatch if streaming
  needs private aggregate internals.
- **One-way dependency justification** - split impl only for methods whose deps are one-way into stable APIs.
- **Behavior-neutral** - pure move; sf=1 tripwire + `--no-fail-fast`.
- **LSP advisory** - ground on `cargo-cgroup check` + guard + board, not inline diagnostics.
- **No perf/logic/budget/cost-model change.**

## Seal
`impl: adr 352 join streaming submodule` (if carved), or a planning-only outcome recommending aggregate-first. Do not
commit wip drafts.
