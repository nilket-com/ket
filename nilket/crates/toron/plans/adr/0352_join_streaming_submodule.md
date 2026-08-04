# ADR 352 - join sub-split: carve exec/join/streaming.rs (only if the coupling map proves it)

## Status
Proposed.

## Context

The streaming carve deferred from 351. `exec/join.rs` is **5,801 lines** post-351; the streaming surface (the
`try_streaming_*` / `prepare_*` / `probe_*` `impl Executor` family + the streaming structs + the streamed-spilled/
partitioned paths) is the next cluster. But streaming touches `impl Executor`, aggregate folding, filter/project/
exchange, materialization, residuals, and the mixed test harness - so **this ADR is "carve streaming only if the
coupling map proves it," not a pre-commitment to move the whole `try_streaming_*` family.** Same 176/350/351 pattern,
behavior-neutral, guarded.

## Decision - coupling map first; carve the "move now" bucket only if dependencies are one-way

### Phase 0 - the coupling map, a three-bucket decision table (the go/no-go)
For every streaming symbol, classify into exactly one bucket:
- **Move now** - streaming-specific structs / free fns / `impl Executor` methods whose only dependencies are **one-way
  calls** into existing **root/shared APIs** plus the **ADR 351 spill API**. These can move to `streaming.rs`.
- **Stay root** - shared helpers also used by non-streaming join execution; aggregate helpers slated for ADR 353;
  generic filter/project/materialization helpers. These do **not** move (streaming calls them via API).
- **Defer / re-scope** - anything whose move would require dragging **aggregate implementation internals**, **general
  executor dispatch**, or **broad test fixtures** across the boundary. These stay, and may force a smaller cut.

### The go/no-go (the justification is dependency direction, not the Rust mechanism)
A split `impl Executor` block in `streaming.rs` is the right Rust mechanism, **but it is not the justification by
itself.** The justification is that the moved methods' dependencies remain **one-way calls into stable root/shared
helpers**:
- **If the map shows aggregate folding (and filter/project/materialization) is mostly just API calls** -> proceed:
  carve the "move now" bucket into `streaming.rs`.
- **If the map shows streaming needs private aggregate internals** (reaching into aggregate state/fold implementation,
  not a stable API) -> **do not force streaming.rs**; the cleaner next ADR is **aggregate-first (353)**, which gives
  streaming a stable aggregate API to call - the same reason spill-first preceded streaming. ADR 352 then becomes the
  recommendation to re-order, with the map as evidence.

### The carve (only the "move now" bucket)
- `exec/join/streaming.rs`: the move-now streaming structs + streaming-only free fns + a split `impl Executor` block for
  the qualifying `try_streaming_*` / `prepare_*` / `probe_*` methods + the streamed-spilled/partitioned paths that pass
  the one-way test. Residuals (`match_filter`) move with streaming.
- **Stay root:** aggregate fold/finalize (353), shared filter/project/exchange/materialization, non-streaming
  `impl Executor` methods. Spill via 351's API.
- Behavior-neutral pure move; no logic/perf/budget change.
- **If Phase 0 returns no-go:** do **not** create `streaming.rs`, do **not** add a fake module guard, and do **not**
  move a partial cluster just to show progress. The ADR's implementation result is the coupling map + aggregate-first
  recommendation; 353 becomes the next source-moving cut.

### The guard - extend `exec_decomposition.rs`
- **If the carve proceeds:** `exec/join` root declares `mod streaming;`; `src/exec/join/streaming.rs` exists.
- **If the carve proceeds:** the moved streaming symbols are no longer in the join root.
- **If the carve proceeds:** the `exec/join` root line count ratchets **down** below 5,801 (Phase-0 measured, fails on
  streaming drift-back).
- no qNN in `streaming.rs`; public surface (`Executor`, `ExecutionContext`, `materialize_scalar_rows`) unchanged.

## Acceptance (behavior-neutral, map-justified, one carve)
- **Either** `exec/join.rs` -> join root + `exec/join/streaming.rs` carrying **only the "move now" bucket** (the map
  proves one-way deps); **or**, if the map shows private-aggregate coupling, the ADR's deliverable is the
  **aggregate-first re-order recommendation** with the coupling-map evidence (no forced streaming carve).
- **Safety net green + unchanged:** `--lib`, `--test architecture_test`, `--no-fail-fast`, `toron-tpch`,
  **`correctness-matrix` 22/22 + `matrix` 22/22 bit-identical** (the sf=1 board is the tripwire).
- `exec_decomposition.rs` extended + green **if the carve proceeds**; if Phase 0 returns no-go, the guard is left
  unchanged and the results doc explains why no source split was performed.
- de-branded; no perf/streaming/budget/cost-model change.

## Guardrails
- **Coupling map first** - the three-bucket table is the go/no-go artifact; no symbol moves until classified.
- **Justification = one-way dependency, not the split-impl mechanism** - split impl is allowed, but only for methods
  whose deps are one-way into stable root/shared/spill APIs.
- **No big-bang** - "stay root" + "defer/re-scope" buckets stay put; if streaming can't separate without dragging
  aggregate internals / dispatch / broad fixtures, re-scope or recommend aggregate-first.
- **Behavior-neutral** - pure move; sf=1 board bit-identical; no logic edits.
- **LSP diagnostics are advisory on these carves** - stale errors (E0365/E0433/E0624 mid-move) are expected; the
  **authoritative signal is `scripts/cargo-cgroup check` + the architecture guard + the board commands**
  (`correctness-matrix` / `matrix`), per the 350/351 experience.

## Risks
- **Forcing the carve** - moving the whole `try_streaming_*` family regardless of the map; the three-bucket table + the
  one-way-dependency go/no-go prevent it (aggregate-first is the escape hatch).
- **Behavior drift** - a logic edit in the move; the sf=1 tripwire (22/22 bit-identical) + `--no-fail-fast`.
- **Stale-diagnostic misread** - treating mid-move LSP errors as real; the LSP note + grounding on `cargo-cgroup check`.
- **Test fixture drag** - mixed-harness streaming tests stay in the root test module (documented), not moved to satisfy
  a location ideal.

## Forward arc (the ratchet)
350 (`exec/join.rs`) -> 351 (`spill.rs`) -> **352 (`streaming.rs` if map-proven, else aggregate-first re-order)** ->
353 (aggregate) -> 354 (expr) -> later (context). Gemini's full structure is the end state. **Then** step 2 (the perf/
regression rail).

## Why toron-home
Pure `crates/toron` source-structure cut (exec/join.rs -> exec/join/streaming.rs + guard extension). No Wendelon /
cross-ring surface; no behavior change.

## Council provenance
Codex + Claude. Same 176/350/351 pattern, now streaming. Codex's tightenings (adopted): (1) **"streaming split only if
the coupling map proves it"** - no pre-commit to moving the whole `try_streaming_*` family; (2) the map produces a
**three-bucket decision table** (move now / stay root / defer-re-scope); (3) **split `impl Executor` is the mechanism,
one-way dependency is the justification** - if streaming needs private aggregate internals, **aggregate-first (353) is
the cleaner next ADR**; (4) the **LSP-diagnostics-are-advisory** note carried into the plan (authoritative signal =
`cargo-cgroup check` + guard + board).
