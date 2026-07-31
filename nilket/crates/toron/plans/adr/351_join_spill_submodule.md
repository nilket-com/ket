# ADR 351 - join sub-split: carve exec/join/spill.rs (spill storage only)

## Status
Proposed.

## Context

The next ratchet step after ADR 350. `exec/join.rs` is **7,143 lines** - the largest single module post-350. This cut
carves **exactly one sub-module: `exec/join/spill.rs`** - the spill **storage** layer - following the same 176/350
pattern (thin parent + sub-module, `exec_decomposition.rs` extended/ratcheted, behavior-neutral). **Streaming is
explicitly deferred to the next ADR.**

### Why spill-only (Codex's narrowing)
Spill is a **clean bottom-layer** carve: `SpilledJoinBuild` / `SpilledInnerJoinBuild`, IPC page read/write,
encryption/tag, and the resident/budget helpers (`join_spill_index_bytes`) depend on nothing above them. Streaming, by
contrast, touches `impl Executor`, aggregate folding, filter/project/exchange helpers, materialization, residuals, and
the mixed test harness - so even though streaming -> spill is one-way, moving streaming in the **same** ADR creates far
wider visibility churn than a pure ratchet needs. Carving spill first **proves the spill API + shared-helper surface by
the compiler**, so the streaming split (the next ADR) starts from a settled boundary.

## Decision - carve `exec/join/spill.rs` (storage only), behavior-neutral, guarded

Convert `exec/join.rs` into a thin-er join root (`mod spill;` + the rest) plus `src/exec/join/spill.rs` holding the
spill storage. **No logic, perf, streaming, or budget change** - a pure move.

### The one carve - `exec/join/spill.rs`
- `SpilledJoinBuild`, `SpilledInnerJoinBuild`, `SpilledSemiAntiExistenceBuild` (the structs + their impls) and their
  spill row-reference structs.
- IPC page read/write, page caching, encryption/tag helpers.
- spill-index resident helpers (`join_spill_index_bytes`, `spilled_batch_join_index_bytes`,
  `semi_anti_existence_spill_index_bytes`) and spill-only constants (`JOIN_SPILL_PAGE_SIZE`,
  `JOIN_SPILL_CACHE_TARGET_BYTES`, `NEXT_JOIN_SPILL_ID`).
- spill-storage unit tests that move cleanly with the structs; if moving a test drags the mixed executor harness across
  the boundary, leave it in the existing root test module and document that choice in the results.

### Explicitly NOT in this ADR (next cuts)
- **streaming** (the `ReorientedInner`/`StreamingSemiAnti`/`StreamingLeftOuterRecombine` structs, the
  `try_execute_streamed_spilled_*` / `visit_*` / `fold_*` paths, extract/decompose) - **ADR 352**, with the spill API
  already proven.
- in-memory hash join + shared helpers (`JoinKey`, `materialize_join_selection`, ...) - stay in the join root for now.
- generic budget/materialization helpers (`budget_exhausted`, `join_output_materialization_memory_cap`,
  `JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER`) stay in the join root unless Phase 0 proves they are spill-only.
- aggregate / expr / window / context - later ADRs.

### The guard - extend `exec_decomposition.rs`
- `exec/join` root declares `mod spill;`; `src/exec/join/spill.rs` exists.
- the spill storage symbols are no longer in the join root (the carve happened).
- the `exec/join` root line count is bounded (a ratchet threshold, Phase 0-measured, fails on spill drift-back); the
  `exec.rs` root threshold unchanged unless something net-leaves it.
- no qNN in `exec/join/spill.rs`; public surface (`Executor`, `ExecutionContext`, `materialize_scalar_rows`) unchanged.

## Phase 0 - map the spill boundary + the API surface
1. **Enumerate the spill storage symbols** (structs, IPC, encryption, resident/budget) and confirm they form a clean
   bottom layer (no upward dependency on streaming/hash/aggregate).
2. **Map the spill API surface:** what streaming/hash call into spill becomes `spill.rs`'s `pub(super)` /
   `pub(in crate::exec::join)` API - minimize it; this is the surface the streaming ADR will build on.
3. **Classify shared helpers:** if a spill implementation needs `JoinKey`, `JoinKeyPlan`, `key_arrays`,
   `join_key_at`, `project_batch_columns`, or `budget_exhausted`, it should call them from the join root rather than
   pulling those shared helpers into `spill.rs`.
4. **Confirm pure move:** the carve compiles with no logic edits; only spill storage moves.
5. **Threshold:** measure the join-root + `spill.rs` line counts after the move; set the bounded `exec/join` root
   threshold below today's 7,143 with churn slack, close enough to fail on spill drift-back.

## Acceptance (behavior-neutral, one carve, guarded)
- `exec/join.rs` -> join root + `exec/join/spill.rs`; **exactly one carve (spill storage); pure move, no behavior
  change; streaming/hash/aggregate untouched.**
- **Safety net green + unchanged:** `--lib`, `--test architecture_test`, `--no-fail-fast`, `toron-tpch`, and
  **`correctness-matrix` 22/22 + `matrix` 22/22 bit-identical** (the sf=1 board is the tripwire).
- `exec_decomposition.rs` extended + green; the `exec/join` root bounded (below 7,143, fails on spill drift-back).
- de-branded; no perf/streaming/budget/cost-model change.

## Guardrails
- **Behavior-neutral** - pure move; sf=1 board bit-identical; no logic edits riding along.
- **Exactly the spill carve** - only spill storage moves; streaming/hash/aggregate/expr/context are out of scope (the
  framework allows them; this ADR performs only spill).
- **Minimal spill API surface** - `pub(in crate::exec::join)` only what streaming/hash require; this is the surface the
  streaming ADR proves against.
- **Guard ratchets** - `mod spill;` + the bounded join root sealed by `exec_decomposition.rs`; no backsliding.

## Risks
- **Scope creep into streaming** - the exactly-one-carve fence + the guard's carved-clusters check bound it; streaming
  is ADR 352.
- **Behavior drift** - a logic edit in the move; the sf=1 tripwire (22/22 bit-identical) + `--no-fail-fast` catch it.
- **Spill API too wide** - if the streaming/hash callers need a sprawling spill surface, note it (it informs whether the
  352 streaming split is as clean as hoped), but keep it `pub(in crate::exec::join)`, not `pub(crate)`.
- **Test extraction creep** - spill-focused tests may stay in the existing mixed `exec::tests` module if extracting them
  would require moving shared fixtures or broad executor tests. The production spill carve is the ratchet; test
  extraction is allowed only where local.

## Forward arc (the ratchet)
350 (`exec/join.rs`) -> **351 (`exec/join/spill.rs`, storage)** -> 352 (`exec/join/streaming.rs`, with the spill API
proven) -> 353 (aggregate) -> 354 (expr) -> later (context). Gemini's full structure is the end state. **Then** step 2
(the perf/regression rail) on a navigable exec.

## Why toron-home
Pure `crates/toron` source-structure cut (exec/join.rs -> exec/join/spill.rs + guard extension). No Wendelon /
cross-ring surface; no behavior change.

## Council provenance
Codex + Claude. Same 176/350 decomposition pattern, now within join. Codex's narrowing (adopted): **ADR 351 =
`exec/join/spill.rs` only.** Spill is the clean bottom-layer carve; streaming touches `impl Executor`, aggregate
folding, filter/project/exchange, materialization, residuals, and the mixed test harness, so it gets its own ADR (352)
with the spill API + shared-helper surface already proven by the compiler. Behavior-neutral, exactly one carve, guard
extended.
