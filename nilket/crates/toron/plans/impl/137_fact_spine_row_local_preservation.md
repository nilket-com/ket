# Impl Plan — ADR 137: Fact-Spine Preservation Through Row-Local Volume-Non-Increasing Nodes

**Migration for** `plans/adr/137_fact_spine_row_local_preservation.md`.
**Production scope:** `binder.rs` only — the two row-local arms (`Filter`, `Project`),
a PK-equality recognizer, and centralized `PlanOptimizerFacts` helpers. Tests/guards
live in the usual binder test module and architecture suite.
**Non-scope:** `Sort` (inert — its input is Exchange-wrapped from the surface); any
width/byte-from-rows model (→ 138); `ExecutionPlan`/wire/fragmenter/scheduler;
129–136 strategy logic; 136-W.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| demotion site — Filter | `LogicalPlan::Filter` arm; `composite_with_row(input_facts.row_upper_bound)` `binder.rs:380` |
| demotion site — Project | `LogicalPlan::Project` arm; `composite_with_row(...)` `binder.rs:396` |
| Sort is inert (do NOT touch for facts) | `apply_order_by_limit_tail` lowers `Sort { input: Exchange{partition_count:1} }` (`sql_planner.rs`); Exchange already → `Composite` |
| facts type / origin | `PlanOptimizerFacts` `binder.rs:157`; `PlanOrigin::{SingleBaseRelation, Composite}` `:164`; `from_table` stamps single-base `:171` |
| `Alias` already preserves | `binder.rs:353` (`let facts = input.facts;`) — leave as-is |
| byte-only decision (D3 honesty) | `choose_auto_join_strategy` `binder.rs:3889/3897`; single-base reject `:3950/3953` |
| PK columns | `TableBinding.primary_key_columns: HashSet<ColumnOrdinal>` `catalog.rs:127` |
| surface caveat | SQL rejects joins around derived tables (`sql_planner.rs:2779`); native Rune `.join` is pinned hash |
| out of scope | `Aggregate` `:413` (Composite), `Take` `:496` (116 row fact); no `Distinct` variant |

## Phases
0. **Failing gates first:** `filtered_tiny_dim_infers_broadcast` and
   `narrowing_project_tiny_dim_infers_broadcast` fail on `main` as direct
   `LogicalPlan`/binder fixtures (the two arms demote to `Composite`). Do not write
   these as SQL derived-table joins. `filtered_dim_without_facts_still_hashes` green
   before + after.
1. **Centralized helpers (D4):** on `PlanOptimizerFacts` —
   `preserve_volume_non_increasing(self) -> Self` (origin+byte+row, for Filter);
   `preserve_through_project(self, &exprs) -> Self` (column-subset test → full
   preserve, else preserve origin + row while setting byte to `None`);
   `with_pk_row_cap(self) -> Self` (row = 1, byte untouched).
2. **Wire the two arms:** Filter `:380` → `preserve_volume_non_increasing` (after the
   existing `EmptyRelation`/known-empty branch); Project `:396` →
   `preserve_through_project(&exprs)` (compute facts *after* binding exprs so the test
   sees bound nodes).
3. **PK row cap (D3):** reuse/tighten the existing point-read PK recognizer
   (`predicate_includes_primary_key_equality` family) — require
   `primary_key_columns.len() == 1`, that column, a non-null literal. Compute this
   proof from the bound predicate before `plan_filter_with_predicate_pushdown` consumes
   it (or clone deliberately), then apply `with_pk_row_cap` inside the Filter arm.
   Composite descriptor PKs are not ≤1-row proofs. **Do not** touch the byte bound or
   the decision; assert a large table still hashes.
4. **Column-subset predicate:** every output root is a distinct
   `BoundExprNode::Column` of the input (no expr/cast/literal/dup). Single helper,
   unit-tested.
5. **Guards + tests:** the five `adr137_*` guards (incl. the centralization guard — no
   Filter/Project row-local arm builds facts inline; scope it so Sort's intentional
   `composite_with_row` remains allowed) + the gate list; re-run 129–136.
6. **Verify:** `cargo test -p toron` · `--test architecture_test -- adr137` (+ re-run
   adr129..136) · `cargo check -p toron --all-targets` · `git diff --check`.

## Risk register
- **Over-preserving a non-subset Project** (byte bound on an expression project that
  grows bytes → false broadcast): the column-subset test +
  `expression_project_drops_byte_bound_and_hashes` are the backstops. Compute facts
  *after* binding exprs.
- **PK cap leaking into a byte decision** (deliver-the-name trap): D3 never shrinks
  bytes; `pk_filtered_large_table_caps_rows_but_still_hashes` pins that the strategy
  stays hash. Honesty check: reverting the row-cap line must change no strategy gate.
- **Composite-key false ≤1-row proof:** require `len() == 1`.
- **Surface mismatch:** build-side Filter/Project broadcast gates are direct
  `LogicalPlan` fixtures. SQL derived-table joins remain unsupported and native Rune
  joins are pinned hash, so don't claim those surfaces in 137.
- **Invariant rot in a future arm:** `adr137_row_local_facts_go_through_shared_helper`.
- **Known-empty regression:** Filter preserves 136 row=0; static-false stays
  `EmptyRelation`.
- **Tempting Sort fold:** leave the `Sort` arm (`:479`) as-is — preserving its facts is
  inert while surface `ORDER BY` wraps the input in a one-partition `Exchange`.

## Acceptance
Direct binder fixtures for filtered and narrowing-projected tiny dims infer broadcast
over real facts with no seeding; expression/duplicate projects preserve lineage + row
but drop bytes and hash; aggregates stay `Composite`; single-col-PK filter caps rows
to 1 yet a large table still hashes; composite-PK component filters do not cap rows;
missing facts hash; all row-local fact computation routes through the shared helpers;
five `adr137_*` + 129–136 guards green.
