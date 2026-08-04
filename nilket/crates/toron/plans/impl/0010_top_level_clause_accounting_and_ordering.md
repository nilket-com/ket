# Implementation Plan: ADR 010 — Top-Level SQL Clause Accounting and Deterministic Result Ordering

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/0010_top_level_clause_accounting_and_ordering.md`
**Bench-results artifact:** none — ADR 010 makes no performance
claim. Promotion evidence is digest parity (canonical + ordered)
plus stage-shape inspection against hand-built `Sort` plans, and
updated/preserved architecture guards (see "Definition of done").

## Status

Promoted to `plans/impl/` and implemented.

Built across three topic-iteration rounds and two ADR-drafting
rounds with the AI Swarm Council
(`plans/wip/{claude,codex,gemini}_*_010_*.md`). Three-way final
convergence reached; Codex iter-2 approved the promotion base,
Gemini iter-2 ratified the same contract.

## Substrate

- `crates/toron/src/sql_planner.rs` — `SqlPlanner::lower_query`
  / `SqlPlanner::lower_select` (the silent-drop sites), the
  existing `unsupported(...)` / `FrontendError::Unsupported`
  path. Most ADR 010 production code lands here.
- `crates/toron/src/ir.rs` — `LogicalPlan` enum (8 variants).
  Gains one: `Sort` + the `SortExpr` struct.
- `crates/toron/src/ir/execution.rs` — `ExecutionPlan` enum (6
  variants), `ExecutionPlan::schema()`. Gains one: `Sort` +
  `ExecutionSortKey`; `schema()` gains a `Self::Sort {
  output_schema, .. } => output_schema.clone()` arm.
- `crates/toron/src/binder.rs` — the logical→execution lowering
  path. Gains a `Sort` arm: key resolution (names + 1-based
  ordinals), non-output / alias / computed rejection,
  `OrderByExpr` modifier validation, bind-time tie-break
  appending. No new binder semantic class.
- `crates/toron/src/exec.rs` — the operator executor. `SortExec`
  lands here, near the aggregate/join helpers (Codex D7 — no
  `src/exec/sort.rs` module split unless file size later forces
  it).
- `crates/toron/src/distributed/fragment.rs` — the fragmenter,
  operating on the **bound `ExecutionPlan`**. Phase 4 teaches it
  the required post-join terminal gather-sort boundary,
  strategy-agnostic.
- `crates/toron/src/distributed/budget.rs` — `BudgetDimension`
  (7 variants incl. `Memory`). Reused unchanged.
- `crates/toron/src/error.rs` / `src/error/distributed.rs` —
  `ExecutionError::BudgetExhausted`,
  `TaskFailureKind::BudgetExhausted { dimension, requested,
  available }` (7-variant `TaskFailureKind`). Reused unchanged.
- Arrow 58 (`arrow` / `arrow-schema`; workspace pins `58.3.0`) —
  `arrow::compute::{concat_batches, lexsort_to_indices,
  SortColumn, SortOptions, take}` and `UInt32Array` indices, all
  via the existing `arrow::compute` re-export. ADR 003 §13
  canonical normalization in `src/distributed/determinism.rs`
  already imports exactly this set
  (`use arrow::compute::{SortColumn, SortOptions, concat_batches,
  lexsort_to_indices, take};`) and calls `lexsort_to_indices`
  the same way — ADR 010 reuses that import path verbatim and
  adds **no new direct dependency** (no `arrow-ord`, no
  `Cargo.toml` change).
- `crates/toron/tests/architecture_test.rs` — the ADR 003/004/008
  guard set, `g17_join_ir_surface_has_single_join_variant_and_two_strategies`,
  the `enum_variant_count` helper, `source_lines` / `manifest_dir`.
- `crates/toron/tests/sort_ordering_test.rs` — new; public SQL /
  end-to-end ordering + binder hard gates.
- `crates/toron/tests/clause_accounting_test.rs` — new; the
  no-silent-drop rejection table tests.

## Goal

Ship the ADR 010 contract: (1) `lower_query` / `lower_select`
account for every parsed top-level `Query` / `Select` field
across three buckets, enforced by an exhaustive no-`..`
destructure plus a source guard (field-level —
`Select.projection` keeps ADR 008 / 009 handling; projection-item
accounting is named deferred work, ADR §"Not in scope"); (2)
top-level `ORDER BY` /
`ORDER BY ... LIMIT n` / `ORDER BY ... OFFSET n` lower to a new
`Sort` operator with output-ordinal keys (names + 1-based
ordinals), a bind-time-appended ascending-`NULLS LAST` total-
order tie-break, and a required strategy-agnostic terminal
gather-sort distributed shape. No new wire, `TaskFailureKind`,
`BudgetDimension`, Wendelon, `JoinKind`, or `JoinStrategy`
surface. ADR 008 / 009 / M7 behavior byte-identical for every
query that does not contain a now-supported `ORDER BY` and did
not previously rely on a now-loud-rejected clause being silently
dropped.

## Implementation cautions (guardrails, not scope)

- **C1. The destructure is the guard — no `..`.** `lower_query`
  / `lower_select` bind every `Query` / `Select` field by name
  into one of three buckets.
- **C2. `Sort` sits above `Project`; keys are output ordinals.**
  Never lower `Sort` below `Project`; never push a non-output
  column through as a hidden sort column.
- **C3. Total order, always; tie-break at bind time.** The binder
  appends every remaining output ordinal as an ascending
  `NULLS LAST` tie-break key. No `SortExec`-side tie-break
  construction, no partial-sort fast path.
- **C4. Preserve binder ownership.** The planner builds
  `LogicalPlan::Sort`; the binder validates resolution, modifier
  support, schema, ordinals, and appends tie-breaks.
- **C5. Byte-identity first.** The destructure refactor regression
  gate runs before any `Sort` rule lands.
- **C6. Reuse the budget path.** `SortExec` reserves against
  `BudgetDimension::Memory`, returns
  `ExecutionError::BudgetExhausted`, maps to the existing
  `TaskFailureKind::BudgetExhausted`. No new variant / dimension.
- **C7. The fragmenter is the real risk.** The required
  strategy-agnostic post-join terminal gather-sort is gated by
  stage-shape inspection, not output digest alone.
- **C8. `OFFSET` / `LIMIT` apply after the full logical order.**
  Do not pass a `limit` to Arrow's `lexsort_to_indices` as a
  substitute — Toron truncates the sorted index vector itself.

## Phases

### Phase 0 — Audit

Locked decisions (recorded here, not open questions):

- **P0.4: default null policy is `NULLS LAST`** — for both
  ascending and descending user keys; tie-break keys are
  ascending `NULLS LAST`.
- **P0.5: float sort keys are supported** — `Float32` /
  `Float64`, ordered by Arrow 58's IEEE 754 total order; pinned
  by the `sort_float_keys_follow_arrow_total_order` gate, not by
  version string.
- **P0.7: 1-based positive integer output ordinals are
  supported** — `ORDER BY 1` resolves against the output schema
  by position; `0`, negative, out-of-range, non-integer, and
  expression-shaped ordinals reject.
- **P0.9: the post-join terminal gather-sort is required** — not
  a fork. Phase 0 pins only the exact code mechanics (stage IDs,
  exchange IDs, replacement helpers), not whether it is needed.

True audit details (resolved in Phase 0, then a `// P0.x:`
comment each):

- **P0.1:** exact `sqlparser` 0.54 `Query` field inventory + the
  three-bucket assignment; confirm the no-`..` destructure
  compiles.
- **P0.2:** exact `Select` field inventory + bucket assignment;
  enumerate the parser-metadata bucket (`select_token`,
  `top_before_distinct`, `window_before_qualify`) so the source
  guard's allow-list is exact.
- **P0.3:** exact `OrderByExpr` / `OrderBy` container field shape
  (`expr`, `asc: Option<bool>`, `nulls_first: Option<bool>`,
  `with_fill`); which modifiers are V1-supported vs rejected.
- **P0.6:** exact `LIMIT` / `OFFSET` literal AST shape; the
  `parse_non_negative_usize_literal` helper behavior and overflow
  handling.
- **P0.8:** exact `SortExec` memory estimator for the
  `BudgetDimension::Memory` reservation — proposed: per
  materialized batch `RecordBatch::get_array_memory_size()` plus
  `row_count * 4` bytes for the `UInt32Array` index buffer.
- **P0.5b / Arrow API:** confirm `lexsort_to_indices` returns a
  `UInt32Array`, and its null / float handling under the pinned
  Arrow version.

Phase 0 produces the `// P0.x:` comments, then a single commit
`plan: adr 010 top level clause accounting and ordering`.

### Phase 1 — Clause accounting (the integrity fix, lands first)

Files:
- `crates/toron/src/sql_planner.rs`
- `crates/toron/tests/architecture_test.rs`

Work items:

- **A. `lower_query` exhaustive three-bucket destructure.** Full
  destructure of `query` binding every field (no `..`). `body` →
  `lower_select`; `order_by` / `limit` / `offset` captured for
  Phase 3 (in Phase 1 they still reject); every other present
  semantic field → `FrontendError::Unsupported`.
- **B. `lower_select` exhaustive three-bucket destructure.** Same
  for `Select`: `from` / `selection` / `projection` keep current
  handling; present semantic clauses → `FrontendError::Unsupported`;
  parser-metadata fields (`select_token`, `top_before_distinct`,
  `window_before_qualify`) bound and commented as non-semantic.
  The accounting is **field-level**: `Select.projection` is in
  the semantic-lowered bucket and retains its ADR 008 / 009
  lowering unchanged — ADR 010 guarantees each *field* is
  handled, not that every *item within* `projection` is. Known
  projection-item gaps that predate ADR 010 (mixed
  aggregate / non-aggregate projection, wildcard mixed with
  explicit items) are **named deferred work**, not closed here
  (ADR §"Not in scope"); bundling projection-item accounting
  widens ADR 010 into aggregate-path territory.
- **C. `parse_non_negative_usize_literal` helper.** Shared
  `LIMIT` / `OFFSET` literal parser; rejects negative, decimal,
  parameter, and expression-shaped values. Used by Phase 3; lands
  in Phase 1 with the destructure so the captured `limit` /
  `offset` have a typed parse path.
- **D. Clause-accounting source guard.** An `architecture_test.rs`
  guard that fails if `lower_query` / `lower_select` contain a
  `..` rest pattern in their `Query` / `Select` destructure;
  allow-list explicitly names the parser-metadata fields (P0.2).
- **E. Byte-identity check (FIRST).** Every existing SQL test
  runs unchanged. This phase changes no accepted query's plan; a
  moved digest reverts the refactor.

Tests (`tests/clause_accounting_test.rs`,
`tests/architecture_test.rs`):
- `existing_sql_queries_byte_identical_after_clause_accounting`
  — **Hard gate 24**, runs first.
- `limit_without_order_by_is_unsupported` — **Hard gate 18.**
- `offset_without_order_by_is_unsupported` — **Hard gate 19.**
- `fetch_and_limit_by_are_unsupported` — **Hard gate 20.**
- `distinct_is_unsupported_not_dropped` — **Hard gate 21.**
- `group_by_having_with_are_unsupported_not_dropped` — **Hard
  gate 22.**
- `dialect_select_and_query_clauses_are_unsupported_not_dropped`
  — **Hard gate 23**, table test.
- `lower_query_and_lower_select_have_no_rest_pattern` — part of
  **Hard gate 25**.

Phase 1 commits as a self-contained correctness fix —
`feat: adr 010 phase 1 top-level clause accounting`. Promotable
on its own value before the ordering feature lands.

### Phase 2 — `Sort` IR + binder

Files:
- `crates/toron/src/ir.rs`
- `crates/toron/src/ir/execution.rs`
- `crates/toron/src/binder.rs`

Work items:

- **A. `LogicalPlan::Sort` variant.** `Sort { input:
  Box<LogicalPlan>, order_by: Vec<SortExpr>, limit:
  Option<usize>, offset: usize }`, `SortExpr { expr: Expr,
  descending: bool, nulls_first: bool }`.
- **B. `ExecutionPlan::Sort` variant.** `Sort { input:
  Box<ExecutionPlan>, order_by: Vec<ExecutionSortKey>, limit:
  Option<usize>, offset: usize, output_schema: SchemaRef }`,
  `ExecutionSortKey { ordinal: ColumnOrdinal, descending: bool,
  nulls_first: bool }` (`descending` aligns with Arrow
  `SortOptions`). Add the `Self::Sort { output_schema, .. } =>
  output_schema.clone()` arm to `ExecutionPlan::schema()`.
- **C. Binder `Sort` arm (Codex D1, D2 + iter-2 ordinals).**
  Produce a fully explicit physical key list:
  1. resolve user keys that are column names to output
     `ColumnOrdinal`s;
  2. resolve 1-based positive integer literals as output
     ordinals by position;
  3. reject aliases, computed expressions, non-output names,
     `ORDER BY 0`, negative / out-of-range / non-integer
     ordinals, and unsupported `OrderByExpr` modifiers;
  4. resolve omitted null policy to `NULLS LAST`;
  5. append implicit tie-break keys — for every output ordinal
     not in the user key set, `ExecutionSortKey { ordinal,
     descending: false, nulls_first: false }`.
  Reuse existing column-resolution and
  `FrontendError::Unsupported` machinery (C4).

Tests (`tests/sort_ordering_test.rs` + in-module `#[cfg(test)]`
for comparator/binder units):
- `sort_key_resolves_against_output_schema` — **Hard gate 8.**
- `order_by_positive_integer_resolves_output_ordinal` — **Hard
  gate 9.**
- `order_by_zero_negative_or_out_of_range_ordinal_rejects` —
  **Hard gate 10.**
- `sort_key_rejects_non_output_alias_and_computed_keys` —
  includes `SELECT a FROM t ORDER BY b` — **Hard gate 11.**
- `sort_tie_break_keys_are_appended_by_output_ordinal_at_bind_time`
  — **Hard gate 12.**
- `sort_default_null_policy_is_nulls_last` — **Hard gate 5.**
- `sort_explicit_nulls_first_last_override_default` — **Hard
  gate 6.**
- `sort_preserves_output_schema`.

Gate: `cargo check --workspace` + `cargo test -p toron --lib`.

### Phase 3 — `SortExec` + `ORDER BY` lowering

Files:
- `crates/toron/src/exec.rs`
- `crates/toron/src/sql_planner.rs`

Work items:

- **A. `ORDER BY` rewrite in `lower_query` (Codex D3).** When
  `query.order_by` is present (with `query.limit` / `offset`
  parsed via `parse_non_negative_usize_literal`), after
  `lower_select` produces the select-level plan, build
  `LogicalPlan::Sort` above an **explicitly emitted** terminal
  `LogicalPlan::Exchange { partition_count: 1, key: vec![] }`.
  Lower each `OrderByExpr` to a `SortExpr` with the resolved
  `descending` / `nulls_first` policy; `with_fill` and
  unsupported modifiers reject.
- **B. `SortExec` operator — index-first (Codex iter-2).** In
  `exec.rs`:
  1. concatenate input batches (`arrow::compute::concat_batches`);
  2. reserve against `BudgetDimension::Memory` using the P0.8
     estimate (`get_array_memory_size()` + `row_count * 4`); fail
     with `ExecutionError::BudgetExhausted` if exceeded;
  3. build `SortColumn`s from the `Vec<ExecutionSortKey>` (each
     with its `SortOptions { descending, nulls_first }`) — the
     list is already complete incl. tie-break (C3);
  4. `arrow::compute::lexsort_to_indices(&columns, None)` — pass
     `None`, not a limit (C8); same import path as ADR 003 §13
     `determinism.rs`;
  5. apply `offset` then `limit` to the returned `UInt32Array`
     index vector;
  6. `arrow::compute::take` every output column with the
     truncated indices;
  7. return one `RecordBatch`, or an empty batch with the same
     schema when the input is empty / fully offset past.

Tests (`tests/sort_ordering_test.rs`):
- `top_level_order_by_lowers_to_sort_operator` — **Hard gate 1.**
- `order_by_executes_in_expected_order` — ordered-digest parity
  vs hand-built `Sort` plan — **Hard gate 2.**
- `order_by_limit_offset_truncates_after_sort` — **Hard gate 3.**
- `order_by_with_ties_is_total_and_reproducible` — **Hard
  gate 4.**
- `sort_float_keys_follow_arrow_total_order` — `NaN`, `-0.0`,
  `0.0`, ordinary values, nulls — **Hard gate 7.**
- `canonical_digest_still_matches_for_ordered_query` — **Hard
  gate 17.**
- `sort_over_budget_fails_cleanly` — **Hard gate 26.**

Gate: `cargo test -p toron --test sort_ordering_test`.

### Phase 4 — Distributed fragmenter post-join gather-sort (the real risk)

Files:
- `crates/toron/src/distributed/fragment.rs`

Work items (Codex D4 + iter-2 — concrete, strategy-agnostic):

- **A. Post-join terminal gather stage.** The current join
  fragmenter finds the first join, builds join-producer stages,
  and emits join-consumer tasks as final-result tasks. For
  `Sort(Exchange₁(Project(Join(…))))` that is insufficient. Teach
  the fragmenter the post-join gather boundary: the join-consumer
  stage publishes shuffle output to the terminal single-partition
  exchange; that exchange fans into **one** final task carrying
  `ExecutionPlan::Sort`, whose output is `FinalResult`. An
  ordered hash-join query has exactly **one more stage** than its
  unordered baseline.
- **B. Strategy-agnostic.** The same path covers a `Broadcast`
  join consumer — SQL lowering emits the identical
  `Sort(Exchange₁(…))` shape regardless of join strategy. A
  broadcast ordered join must not be left silently wrong.
- **C. Non-join terminal gather stage.** For
  `Sort(Exchange₁(Project(Scan)))`, the terminal single-partition
  exchange gathers scan/filter/project partitions into the one
  final sort task. P0.9 confirms whether the existing `Exchange`
  fragmentation already handles this or needs a small explicit
  path.
- **D. Ordered-digest test helper.** Add the emitted-sequence
  hash helper (distinct from ADR 003 §13's pre-sort canonical
  digest) to the test support module.

Tests — **stage-shape gates primary, digest gates after** (Codex
iter-2):
- `order_by_above_hash_join_has_terminal_single_task_sort_stage`
  — one final task carrying `ExecutionPlan::Sort`, output
  `FinalResult`; one more stage than the unordered hash-join
  baseline — **Hard gate 13.**
- `post_join_gather_stage_consumes_all_join_partitions` — the
  join-consumer stage publishes shuffle output to the terminal
  gather, not independent final results — **Hard gate 14.**
- `order_by_above_broadcast_join_uses_terminal_gather_sort` —
  **Hard gate 15.**
- `ordered_digest_matches_single_process_and_distributed` —
  **Hard gate 16.**

Gate: `cargo test -p toron --test sort_ordering_test --test
distributed_process_test`.

### Phase 5 — Architecture guards + verification + promotion

Files:
- `crates/toron/tests/architecture_test.rs`
- `crates/toron/plans/adr/0010_top_level_clause_accounting_and_ordering.md`
- `crates/toron/plans/impl/0010_top_level_clause_accounting_and_ordering.md`
- `crates/toron/plans/wip/roadmap.md`

Work items:

- **A. Variant-count guard update.** Update the IR variant-count
  guard to expect `LogicalPlan` +1 and `ExecutionPlan` +1
  (`Sort`), intentionally. Confirm
  `g17_join_ir_surface_has_single_join_variant_and_two_strategies`
  stays green (it counts `Join {` — adding `Sort` does not move
  it). Confirm `TaskFailureKind` and `BudgetDimension` variant
  counts remain 7. **Hard gate 25.**
- **B. Source guard confirmation.** The no-`..` clause-accounting
  guard (Phase 1D) stays green. **Hard gate 25.**
- **C. Verification + promotion.** Run the full verification
  command block. ADR 010 produces no bench-results artifact. The
  promoted impl plan states: no new `TaskFailureKind`, no new
  `BudgetDimension`, no new wire or Wendelon surface, no bench
  artifact, and stage-shape inspection is part of promotion
  evidence.

Gate: full verification block + `cargo test --workspace
--no-fail-fast`.

## Verification commands at promotion

```bash
cargo fmt -p toron --check
cargo check --workspace
cargo test -p toron --no-fail-fast
cargo test -p toron --test architecture_test --no-fail-fast
cargo test -p toron --test sort_ordering_test --no-fail-fast
cargo test -p toron --test clause_accounting_test --no-fail-fast
cargo test -p toron --test join_kind_test --no-fail-fast
cargo test -p toron --test distributed_process_test --no-fail-fast
cargo test --workspace --no-fail-fast
git diff --check
```

## Risks + mitigations

- **R1: the `lower_query` / `lower_select` destructure refactor
  regresses existing SQL.**
  *Mitigation:* C5 + P0.1/P0.2 inventory; gate 24 runs first in
  Phase 1; a moved digest reverts the refactor.

- **R2: a silently-dropped clause was load-bearing for a query
  that "works" today.**
  *Mitigation:* intended, not a regression — such a query
  already returns wrong results. Phase 1 ships as a
  self-contained correctness commit.

- **R3: parser-metadata field mis-bucketed as a clause.** Routing
  `select_token` to "unsupported" would reject every query.
  *Mitigation:* three-bucket model; P0.2 enumerates the metadata
  bucket; the source guard's allow-list is exact.

- **R4: partial-order `ORDER BY` ships by accident.**
  *Mitigation:* C3 — tie-break appended at bind time; gate 4
  asserts ordered-digest parity across runs; gate 12 asserts the
  appending.

- **R5: `Sort` placed below `Project` / hidden sort column.**
  *Mitigation:* C2; gate 11 pins non-output / alias / computed
  rejection including `SELECT a FROM t ORDER BY b`.

- **R6: fragmenter does not produce a terminal gather-sort above
  a join — each join consumer publishes an independent final
  result.** The largest real implementation risk (C7).
  *Mitigation:* P0.9 pins the topology before code; Phase 4A/B
  is a concrete strategy-agnostic work item; gates 13–15 inspect
  **stage shape**, not just output digest — a digest-only test
  could pass with the wrong topology if the data happened to
  merge.

- **R7: `SortExec` OOMs instead of failing cleanly.**
  *Mitigation:* C6; P0.8 pins the memory estimate and
  reservation point; gate 26 asserts clean over-budget failure.

- **R8: a new `TaskFailureKind` / `BudgetDimension` sneaks in.**
  *Mitigation:* C6 — both `TaskFailureKind::BudgetExhausted` and
  `BudgetDimension::Memory` already exist; gate 25 asserts the
  seven-variant guards stay green.

- **R9: `sqlparser` `OrderByExpr` modifier silently ignored.**
  *Mitigation:* P0.3 enumerates the modifiers; Phase 3A rejects
  every unsupported modifier; gate 6 pins supported-modifier
  behavior.

- **R10: Arrow's `lexsort_to_indices` `limit` argument used as a
  shortcut, breaking `OFFSET`.**
  *Mitigation:* C8; Phase 3B step 4 passes `None`; gate 3 pins
  `offset`-then-`limit` truncation order.

- **R11: scope creep toward distributed sort / sort spill /
  `DISTINCT`.**
  *Mitigation:* ADR §"Not in scope" and §"Rejected alternatives"
  are explicit; the clause-accounting table test makes any
  half-shipped feature visible as a still-rejected clause.

## Definition of done

- Phase 0 audit complete: locked decisions confirmed, true audit
  details recorded as `// P0.x:` comments, plus the planning
  commit.
- Phases 1–5 complete: hard gates 1–26 passing.
- Phase 1 is independently reviewable as a self-contained
  correctness commit (silent drop → loud reject).
- Promotion evidence: digest parity — canonical digest (gate 17)
  and ordered digest (gates 2, 4, 16) — plus stage-shape
  inspection (gates 13–15) and updated/preserved architecture
  guards (gate 25). No bench-results artifact.
- All verification commands above green.
- Documentation: ADR 010 and impl plan land at promotion.
- Roadmap (`plans/wip/roadmap.md`) updated: ADR 010 marked
  Promoted + implemented; the post-ADR-009 A–E candidate list
  annotated as superseded by code evidence of a V1 silent-wrong-
  results defect; ADR 011 candidates reframed with the ADR 006
  Wendelon production row as the parallel-track input.

## Files touched (expected)

- `crates/toron/src/sql_planner.rs` — exhaustive three-bucket
  `lower_query` / `lower_select` destructure, clause rejection
  routing, `parse_non_negative_usize_literal`, `ORDER BY` rewrite
  with explicit terminal gather, `SortExpr` construction.
- `crates/toron/src/ir.rs` — `LogicalPlan::Sort` + `SortExpr`.
- `crates/toron/src/ir/execution.rs` — `ExecutionPlan::Sort` +
  `ExecutionSortKey`, `schema()` arm.
- `crates/toron/src/binder.rs` — `Sort` lowering arm: key
  binding (names + 1-based ordinals) + bind-time tie-break
  appending.
- `crates/toron/src/exec.rs` — `SortExec` operator (in-file, no
  module split).
- `crates/toron/src/distributed/fragment.rs` — required
  strategy-agnostic post-join terminal gather-sort stage.
- `crates/toron/tests/sort_ordering_test.rs` — new; ordering +
  binder gates.
- `crates/toron/tests/clause_accounting_test.rs` — new;
  no-silent-drop rejection table tests.
- `crates/toron/tests/distributed_process_test.rs` — distributed
  gather-sort stage-shape + parity gates.
- `crates/toron/tests/architecture_test.rs` — variant-count guard
  update + no-`..` clause-accounting source guard.
- `crates/toron/plans/adr/010_*.md`,
  `crates/toron/plans/impl/010_*.md`,
  `crates/toron/plans/wip/roadmap.md`.

## Files not expected to change

- `crates/toron/src/wire.rs`, `crates/toron/src/distributed/task.rs`,
  `crates/toron/src/error/distributed.rs`,
  `crates/toron/src/error.rs` — no new wire, `MessageKind`,
  `TaskFailureKind`, or `ExecutionError` variant (`BudgetExhausted`
  reused).
- `crates/toron/src/distributed/budget.rs` — `BudgetDimension`
  reused unchanged.
- `crates/toron/src/exec/runtime_filter.rs` — runtime-filter path
  untouched.
- `crates/toron/Cargo.toml` and the workspace `Cargo.toml` — no
  new dependency; the `arrow::compute` re-export already exposes
  every sort kernel ADR 010 needs (`src/distributed/determinism.rs`
  imports the same set today).
- `crates/toron/benches/*` — no new operator perf claim.
- Wendelon crates.

## Promotion / fallback

Promote if all 26 hard gates pass and the full verification block
is green.

If `sqlparser` 0.54's `Query` / `Select` / `OrderByExpr` shapes
differ from the P0.1–P0.3 audit assumptions, adjust the
destructure and AST-matching code; the contract does not move.

If P0.9 shows the fragmenter needs more than a narrow
strategy-agnostic post-join terminal gather-sort path — i.e. it
would require new exchange-mode or barrier surface — **narrow the
scope**: ship V1 `ORDER BY` over non-distributed plans and over
the single-stage gather, and move `ORDER BY` above a distributed
join to a named deferred extension rather than growing the
fragmenter contract. ADR 010's core value (clause accounting +
the ordering operator) does not depend on that one composition.
This is the most likely fallback to exercise — Phase 4 is the
real risk.

If P0.8 shows the existing budget reservation surface cannot
express `SortExec`'s memory estimate cleanly, pin a narrow
conservative estimate (materialized batch bytes + index buffer);
do not add a new `BudgetDimension` — the dimension is `Memory`.

If pressure for an out-of-scope shape (distributed range sort,
sort spill, `DISTINCT`, `GROUP BY`, `ORDER BY` on input columns)
appears during review, hold the boundary. Each is a named,
deferred extension; ADR 010 ships clause accounting plus the
narrow deterministic-ordering operator.
