# ADR 350 - Semantic Executor Decomposition (176 pattern for exec.rs, join-first)

## Status
Proposed.

## Context

Housekeeping before the perf rail: `crates/toron/src/exec.rs` is **18,711 lines** - the binder's un-split sibling.
Apply the **ADR 176 Semantic Binder Decomposition pattern to exec.rs**: a thin bounded root + semantic submodules under
`crates/toron/src/exec/`, **with an architecture guard from day one** (`exec_decomposition.rs`, mirroring
`binder_decomposition.rs`). **Behavior-neutral only.** A **ratchet**, not a big-bang.

**This ADR creates the decomposition framework and performs exactly one carve: `join`.** "Semantic executor
decomposition" is **not** permission to opportunistically move expression, aggregate, or window code in the same
implementation. The framework + the join carve land here; every other module is a separate follow-up ADR.

### ADR granularity
Use **multiple ADRs**, not one broad ADR with many implementation phases. ADR 350 owns only the decomposition
framework and the first `join` carve. Later carves (join sub-split, aggregate, expr, window, context) each get their
own ADR because each boundary needs its own map, line-count ratchet, architecture guard update, and behavior-neutral
proof. Within ADR 350, phases are only for mapping, the one carve, and the guard.

### Grounded
- 176 precedent: `binder.rs` -> `binder/` semantic modules + `binder_decomposition.rs` guard + bounded root + extracted
  tests, behavior-neutral. Prior module decompositions: 176 (binder), 077 (data server), 078 (slab manager).
- exec.rs has **never** been decomposed; it owns the scale-arc substrate (340-349). join is the largest + riskiest
  cluster, so carving it first gives the biggest reduction and proves the guard.
- Gemini's full target (`context` / `expr` / `aggregate` / `join{,/hash,/spill,/streaming}` / `tests`) is the **end
  state**, reached by follow-up ADRs - not this cut.

## Decision - thin bounded root + `exec/join.rs`, behavior-neutral, guarded, one carve

Convert `exec.rs` into a thin root (`mod join;` + the remaining inline code) plus `src/exec/join.rs`. **No logic, perf,
streaming, or budget change** - a pure move.

### The one carve - `exec/join.rs` (the join substrate)
- `execute_hash_join` + variants; join-into-global-aggregate paths.
- in-memory join selection / materialization (`materialize_join_selection`, `build_in_memory_join_rows`).
- streamed/spilled join paths (340-349): the streaming visitors, `execute_spilled_hash_join`, the partitioned/grace
  LeftOuter route.
- spill structs + IPC page helpers (`SpilledJoinBuild`, `SpilledInnerJoinBuild`, page read/write/encrypt).
- join key / partition / budget helpers (`join_spill_index_bytes`, `join_take_index_bytes`, key-bytes, bucketing).
- join-focused unit tests - moved **with** the module (scoped `exec/join/tests.rs` or inline), not a giant relocated
  test file.

### Explicitly NOT in this ADR (follow-up cuts, no opportunistic moves)
context (`Executor`/`ExecutionContext`, the public face - last), expr (separate seam, heavy private coupling),
aggregate (global/grouped state used by join streaming - move after the join boundary settles), window, and any
big-bang `tests.rs` relocation. A reviewer should expect **only** join symbols to move; any other relocation in this
impl is out of scope.

### The guard - `tests/architecture/exec_decomposition.rs` (mirror 176)
- root declares `mod join;` and **excludes the carved join clusters** (the moved symbols are no longer in the root).
- `src/exec/join.rs` exists.
- **root line count is bounded** - the concrete ratchet (below).
- **no qNN / TPC-H query-number identifiers** in executor semantic modules (the de-branding discipline, now guarded).
- **behavior-neutral public surface:** `pub mod exec;`, `Executor`, `ExecutionContext`, `materialize_scalar_rows`
  remain reachable as before.

### The line-count ratchet (concrete)
- Current root: **18,711 lines.**
- The first guard threshold is **set below the current root** (proving the join carve happened) with **enough slack for
  module declarations / import churn**, and **close enough that join logic drifting back into `exec.rs` fails the
  guard**.
- The exact number is **Phase 0-measured after the move** (root lines post-carve + a small fixed slack), recorded in
  the guard with a comment; it **ratchets down** as each follow-up ADR carves another module.

## Acceptance (behavior-neutral, one carve, guarded)
- `exec.rs` -> thin root + `exec/join.rs`; **exactly one carve (join); pure move, no behavior change; no other module
  relocated.**
- **Safety net green + unchanged:** the script-wrapped suite (`scripts/cargo-cgroup test -p toron --lib`,
  `scripts/cargo-cgroup test -p toron --test architecture_test`, `scripts/cargo-cgroup test -p toron --no-fail-fast`,
  `scripts/cargo-cgroup test -p toron-tpch`) plus **`correctness-matrix` 22/22 + `matrix` 22/22** (the just-completed
  sf=1 board is the tripwire, bit-identical results).
- `exec_decomposition.rs` added + green; root line count reduced + bounded (below 18,711, fails on join drift-back).
- de-branded; no perf/streaming/budget/cost-model change.

## Guardrails
- **Behavior-neutral** - pure move; the sf=1 board (22/22 reach + correctness) must be bit-identical; no logic edits
  riding along.
- **Exactly one carve** - only join symbols move; expr/aggregate/window/context relocations are out of scope for this
  impl (the framework allows them; this ADR does not perform them).
- **Minimal cross-module surface** - prefer private helpers inside `join`; widen visibility (`pub(crate)`) only where
  the move requires it.
- **Guard from day one** - the structure is sealed by `exec_decomposition.rs`, not convention; the line-count threshold
  fails on drift-back.
- **Ratchet, not big-bang** - if the join boundary is worse than expected, stop and reassess before any follow-up carve.

## Risks
- **Scope creep** - "decomposition" read as permission to move more; the exactly-one-carve fence + the guard's
  carved-clusters check bound it.
- **Behavior drift** - a logic edit sneaking into the move; the sf=1 tripwire (22/22 bit-identical) + full
  `--no-fail-fast` catch it.
- **Cross-module tangle** - join coupling to aggregate/expr; Phase 0 maps the boundary and keeps shared symbols in the
  root rather than forcing a premature split.

## Forward arc (the ratchet)
350 (`exec/join.rs` + guard, one carve) -> 351 (sub-split `exec/join/spill.rs`, `exec/join/streaming.rs`) -> 352
(aggregate state/helpers) -> 353 (expr evaluation) -> later (context). Gemini's full structure is the end state. **Then**
step 2 (the perf/regression rail) on a navigable exec - the decomposition is the precursor that makes the perf + sf=10
work legible.

## Why toron-home
Pure `crates/toron` source-structure cut (exec.rs -> exec/ modules + an architecture guard). No Wendelon / cross-ring
surface; no behavior change.

## Council provenance
Codex + Gemini + Claude. 176 pattern applied to exec.rs; join-first because it owns the scale substrate; behavior-
neutral; 176-style architecture guard from day one; public surface unchanged; no perf instrumentation yet. Gemini's
full module target is the end state, reached by follow-ups (Codex's ratchet), not this cut. Operator tightenings: this
ADR **creates the framework and performs exactly one carve (join)** - no opportunistic expr/aggregate/window moves in
the same impl; and the **line-count guard is a concrete ratchet** - below the current 18,711, with churn slack,
Phase 0-measured after the move, set close enough to fail if join logic drifts back into `exec.rs`.
