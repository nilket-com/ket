# ADR 353 - carve exec/aggregate.rs (the aggregate executor/state cluster)

## Status
Proposed.

## Context

The first **root** carve after the join sub-split. `exec.rs` root is **11,611 lines** - now the largest file. Carve the
**aggregate executor/state cluster** into `exec/aggregate.rs`, same 350/351/352 playbook: coupling map first,
behavior-neutral, `exec_decomposition.rs` extended/ratcheted, exactly one carve, ground on `cargo-cgroup check` not LSP.
The kernels are already separate (ADR 021 `src/aggregate_kernel.rs`); this is the **executor**, not the kernels.

## Decision - carve the full aggregate executor/state cluster (only if the map proves it)

### The boundary is the full cluster (Codex), not just the top methods
Move the **whole aggregate executor/state cluster**, so no aggregate-state helper is stranded in the root:
- **Global:** `bound_global_aggregate`, `fold_global_aggregate_state`, `init_global_aggregate_state`,
  `finalize_global_aggregate_state`.
- **Grouped (bound):** `bound_grouped_aggregate`, `fold_bound_grouped_aggregate_batch`,
  `finalize_bound_grouped_aggregate`.
- **Legacy/test grouped helper:** `#[cfg(test)] grouped_aggregate` moves only if it stays local to aggregate tests;
  otherwise keep it rooted/mixed-harness and document why.
- **State + group:** `AggState`, `init_bound_states`, the state init/update/finalize helpers, `GroupValue`,
  `group_value`, the group-materialization helpers.
- aggregate executor unit tests that move cleanly; mixed-harness tests stay (documented).

### The dispatch stays rooted (this is NOT a dispatch cleanup)
The root dispatch arms stay in the root. The **bound** aggregate dispatch re-points to the aggregate module API. The
legacy/unbound logical aggregate branch, including its inline global mock, stays rooted unless Phase 0 deliberately
extracts a *tiny* helper that sharpens the API. This ADR must not turn into a dispatch refactor.

### The callers are broader than streaming (Codex) - a deliberate exec::aggregate API
Three call sites fold into aggregates and all re-point to the new module via one designed surface:
- **`exec.rs`** (the root dispatch arm),
- **`exec/join.rs`** (the hash-join-into-aggregate paths),
- **`exec/join/streaming.rs`** (the `try_streaming_*` aggregate methods, via the 352 API).
Phase 0 enumerates **all** call sites and designs a deliberate `pub(in crate::exec)` `exec::aggregate` API for them -
not an incidental scatter of `pub(in ...)`.

### Phase 0 - the coupling map (three-bucket; produce BEFORE moving)
For each cluster symbol + each call site: kind / requires-impl-Executor / expr touch (API vs private internals) /
dispatch touch / kernel touch / caller (exec.rs | join.rs | streaming.rs) / test locality -> bucket:
- **Move now** - the full aggregate executor/state cluster, deps one-way into the root dispatch, `aggregate_kernel.rs`,
  and a stable expr API.
- **Stay root** - `execute_bound`/dispatch, the legacy/unbound logical aggregate branch, expr internals (354),
  scan/exchange, and any mixed-harness aggregate tests that do not move cleanly.
- **Defer / re-scope** - anything dragging dispatch or **private expr internals**.

**Decision:** expr reached via a stable API -> proceed (carve `exec/aggregate.rs`). Aggregate needs **private expr
internals** -> stop, recommend **expr-first (354)**, map as evidence; no forced carve.

## Acceptance (behavior-neutral, full-cluster, map-justified)
- **Either** `exec.rs` -> root + `exec/aggregate.rs` carrying the **full aggregate executor/state cluster**, with a
  deliberate `exec::aggregate` API the three callers (exec.rs, join.rs, streaming.rs) use; **or** the **expr-first
  re-order recommendation** with coupling-map evidence (no forced carve).
- **Dispatch stays rooted** - `execute` / `execute_bound` routing remains in the root; the legacy/unbound aggregate
  branch is not refactored except for an optional tiny helper.
- **Safety net green + unchanged:** `--lib`, `--test architecture_test`, `--no-fail-fast`, `toron-tpch`,
  **`correctness-matrix` 22/22 + `matrix` 22/22 bit-identical** (the sf=1 board is the tripwire).
- `exec_decomposition.rs` extended + green (if carved); the exec root ratcheted down below 11,611.
- de-branded; no perf/streaming/budget/cost-model change.

## Guardrails
- **Full cluster, not top methods** - `AggState`/`GroupValue`/state+group helpers move with the methods; nothing
  stranded.
- **Dispatch stays rooted** - this ADR is not a dispatch cleanup; only a tiny deliberate helper may be extracted if it
  sharpens the API. The legacy/unbound inline global mock is not forced into the aggregate module.
- **Deliberate exec::aggregate API** - one designed surface for the three callers, not an incidental `pub(in ...)`
  scatter.
- **Coupling map first / one-way dependency** - the three-bucket table is the go/no-go; private-expr-internals ->
  expr-first.
- **Behavior-neutral** - pure move; sf=1 board bit-identical; re-pointing callers is path/visibility only.
- **LSP diagnostics advisory** - stale errors expected; authoritative = `scripts/cargo-cgroup check` + guard + board.

## Risks
- **Stranded state helpers** - moving only top methods leaves `AggState`/helpers in the root; the full-cluster boundary
  prevents it.
- **Dispatch-cleanup creep** - the dispatch-stays-rooted fence + the exactly-one-carve scope bound it.
- **API scatter** - a designed `exec::aggregate` surface, not incidental visibility widening.
- **Behavior drift** - the sf=1 tripwire (22/22 bit-identical) + `--no-fail-fast`.
- **Stale-diagnostic misread** - ground on `cargo-cgroup check`, not LSP.

## Forward arc (the ratchet)
350 (`exec/join.rs`) -> 351 (`spill.rs`) -> 352 (`streaming.rs`) -> **353 (`exec/aggregate.rs`, executor; or expr-first
re-order)** -> 354 (expr) -> later (context). Gemini's full structure is the end state. **Then** step 2 (the perf/
regression rail).

## Why toron-home
Pure `crates/toron` source-structure cut (exec.rs -> exec/aggregate.rs + guard extension). No Wendelon / cross-ring
surface; no behavior change.

## Council provenance
Codex + Claude. Same 176/350/351/352 pattern, now the aggregate executor. Codex's tightenings (adopted): (1) the
boundary is the **full aggregate executor/state cluster** (`AggState`, `GroupValue`, state init/update/finalize, group
materialization, the bound aggregate methods), not just the top methods; (2) callers are **broader than streaming**
(`exec.rs`, `exec/join.rs`, `exec/join/streaming.rs`) -> a **deliberate aggregate-module API** for all three; (3)
**dispatch stays rooted** - this ADR is not a dispatch cleanup, and the legacy/unbound inline global mock is not forced
into the aggregate module. The kernels stay in `aggregate_kernel.rs` (021). Coupling map first; expr-first (354) if the
executor needs private expr internals.
