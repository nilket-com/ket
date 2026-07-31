# Implementation Plan: ADR 023 — Post-Aggregate Projection

**Companion ADR:**
`plans/adr/023_post_aggregate_projection.md`

## Status

Promoted.

Six-phase incremental migration. Each phase compiles green
and keeps the existing test suite green except where ADR
023's contract changes are intentional.

The contract is **additive at the SQL surface** (aliases,
HAVING-hidden aggregates, reorder / subset projections,
existing-`Expr`-surface computed projections become accepted)
and **internally renaming** at the planner accounting layer
(`GroupedAggregateScope` → `AggregateOutputCatalog`). One IR
field is added to `LogicalPlan::Project`; no new IR variant.

Implementation rules:

> Catalog accounting moves; expression and execution semantics
> stay. ADR 021's executor-boundary discipline applies. ADR
> 022's row-update body invariance precedent applies. ADR
> 021's SoA-readiness invariants preserved (no fn pointers in
> the new catalog; no row-path dyn dispatch).

> No new IR variant. No new fragmenter recognition site. No
> new `AggregateOutputRule` variant. No new
> `StableAggregateKernel` row. No `AggState: Copy` removal. No
> `Expr` widening (scalar arithmetic deferred). No
> `AggregateFunction` widening (computed aggregate arguments
> deferred). No cross-ring engagement.

> `output_names.len() == exprs.len()` is the load-bearing IR
> invariant. A Project-construction helper, or direct-site
> `debug_assert!`, catches planner mistakes early; binder
> validates at bind time; architecture guard source-reads the
> field declaration.

## Goal

Land post-aggregate projection across `ir.rs`, `sql_planner.rs`,
`binder.rs`, `exec.rs`, `distributed/fragment.rs`, and the
architecture / test suite without:

- Adding a new IR variant.
- Adding new IR variants of `Expr`, `AggregateFunction`,
  `StableAggregateKernel`, `AggregateOutputRule`, or `AggState`.
- Removing `AggState: Copy` (ADR 022 invariant).
- Introducing row-path trait dispatch.
- Changing `ExecutionPlan` digests for queries currently accepted
  (identity-shape `Project` suppression).
- Changing the Wendelon AGG protocol surface.
- Adding a second fragmenter detector for the new plan shape.

## Substrate

### Modified files

- `crates/toron/src/ir.rs` — Phase 1.
- `crates/toron/src/binder.rs` — Phase 1.
- `crates/toron/src/exec.rs` — Phase 1 (unbound direct executor
  Project naming).
- `crates/toron/src/rune_engine.rs` — Phase 1 (one `Project`
  construction site).
- `crates/toron/src/sql_planner.rs` — Phase 2 + Phase 3 +
  Phase 4.
- `crates/toron/src/distributed/fragment.rs` — Phase 5.
- `crates/toron/tests/architecture_test.rs` — Phase 6.
- `crates/toron/tests/group_by_test.rs` — Phase 6.
- `crates/toron/tests/distributed_process_test.rs` — Phase 6.
- `crates/toron/tests/non_aggregate_projection_accounting_test.rs`
  — Phase 1 (existing destructure pattern updates).
- `crates/toron/tests/sort_ordering_test.rs` — Phase 1
  (existing destructure pattern updates).
- `crates/toron/tests/clause_accounting_test.rs` — Phase 1
  (pattern updates if needed).
- `crates/toron/tests/wendelon_storage_execution_test.rs` —
  Phase 1 (two Project construction sites).

### New files

None required.

### Code-grounded touchpoint sweep

Verified file:line references (from `crates/toron/src` and
`crates/toron/tests` at the head of the planning branch):

| Site | File:Line | Phase | Change |
|---|---|---|---|
| `LogicalPlan::Project` variant | `ir.rs:162-165` | 1 | Add `output_names: Vec<Option<String>>` |
| `Project` construction (rune) | `rune_engine.rs:198` | 1 | Pass `output_names: vec![None; exprs.len()]` |
| `Project` match (binder) | `binder.rs:104` | 1 | Destructure with `output_names`; bind into `ExecutionPlan::Project.output_schema` |
| `Project` schema-walker arm | `binder.rs:481` | 1 | `..` rest stays correct; no destructure change |
| `Project` construction (binder #1) | `binder.rs:1431` | 1 | Pass `output_names: vec![None; ...]` |
| `Project` construction (binder #2) | `binder.rs:1470` | 1 | Same |
| `Project` construction (binder #3) | `binder.rs:1531` | 1 | Same |
| `Project` construction (binder #4) | `binder.rs:1551` | 1 | Same |
| `Project` match (sql_planner suffix walk) | `sql_planner.rs:1237` | 1 | `..` rest stays correct |
| `Project` match (plan-traversal walk) | `sql_planner.rs:1516` | 1 | `..` rest stays correct |
| `Project` construction (sql_planner non-aggregate path) | `sql_planner.rs:332-336` | 1 + 3 | Pass `output_names`; aliases threaded for non-aggregate path |
| `Project` match (exec direct) | `exec.rs:224` | 1 | Use `output_names[i].as_deref()` when present; fall back to `col_i` |
| `Project` construction (exec #1, test) | `exec.rs:3926` | 1 | `output_names: vec![None; ...]` |
| `GroupedAggregateScope` struct | `sql_planner.rs:943-1000` | 2 | Rename to `AggregateOutputCatalog`; extend with HAVING discovery, final projected names |
| `GroupedAggregateScope::from_projection` | `sql_planner.rs:948-974` | 2 | Refactor into new builder shape with SELECT-then-HAVING discovery |
| `GroupedAggregateScope::resolve` | `sql_planner.rs:976-999` | 2 | Preserved initially; later resolver returns projected names only for bare aggregate SELECT outputs |
| `LoweredSelect` | `sql_planner.rs:930-934` | 2 | Field rename: `grouped_aggregate_scope` → `aggregate_output_catalog` |
| `lower_grouped_projection` | `sql_planner.rs:425-466` | 3 | Strict-order reject removed; catalog-backed projection lowering |
| Strict-order reject site #1 | `sql_planner.rs:432` | 3 | Remove |
| Strict-order reject site #2 | `sql_planner.rs:437` | 3 | Remove |
| Strict-order reject site #3 | `sql_planner.rs:450` | 3 | Remove |
| Strict-order reject site #4 | `sql_planner.rs:1101` | 3 | Remove (`projection_group_key_name`) |
| `lower_global_aggregate_projection` | `sql_planner.rs:410-423` | 3 | Refactor to share catalog-builder substrate with grouped path |
| `lower_having_predicate` | `sql_planner.rs:468-476` | 4 | Catalog resolution rejects HAVING alias references with new diagnostic |
| `lower_order_by` | `sql_planner.rs:340-371` | 4 | Catalog resolution rejects ORDER BY hidden aggregate calls with new diagnostic |
| `rewrite_grouped_aggregate_calls` | `sql_planner.rs:478-506` | 3 + 4 | Walker delegates to catalog; expression coverage matches existing `lower_expr` |
| `resolve_grouped_aggregate_reference` | `sql_planner.rs:508-525` | 4 | Returns projected name for bare aggregate SELECT outputs (alias if any, else natural) |
| `is_terminal_sort_over_grouped_aggregate` | `distributed/fragment.rs:562-632` | 5 | Peel optional `[Project]` between gather `Exchange(1)` and existing `[Filter]` |
| `TerminalSortOverAggregate` struct | `distributed/fragment.rs:519-560` | 5 | Add `Option<Vec<ExecutionExpr>>` for projection exprs; `Option<SchemaRef>` for output schema |
| `fragment_terminal_sort_over_grouped_aggregate` | `distributed/fragment.rs:634-700` | 5 | Package `[Project]` into aggregate-consumer task |
| `Project` match (fragment scan utility) | `distributed/fragment.rs:825, 860, 874, 897, 936-940, 1002-1006` | 1 | `..` rest stays correct; no destructure change |
| Test `LogicalPlan::Project` constructions | `tests/architecture_test.rs:107`, `tests/wendelon_storage_execution_test.rs:100, 328`, `tests/group_by_test.rs:2107, 2135`, `tests/sort_ordering_test.rs:155` | 1 | Add `output_names: vec![None; ...]` |
| Test `LogicalPlan::Project` destructures | `tests/non_aggregate_projection_accounting_test.rs:24, 70, 99`, `tests/sort_ordering_test.rs:53`, `tests/clause_accounting_test.rs:9` | 1 | Add `output_names` field or use `..` rest |
| Architecture guard checking scope-type | `tests/architecture_test.rs:580` | 6 | Rename `GroupedAggregateScope` → `AggregateOutputCatalog` |

## Phases

### Phase 1 — Project naming substrate

**Goal.** Add `output_names: Vec<Option<String>>` to
`LogicalPlan::Project`; update every construction and
destructure site; bind names through to
`ExecutionPlan::Project.output_schema`; enable non-aggregate
`SelectItem::ExprWithAlias`. Build green; existing behavior
preserved when all `output_names` are `None`.

**Tasks.**

1. In `ir.rs`, extend `LogicalPlan::Project`:
   ```rust
   Project {
       input: Box<LogicalPlan>,
       exprs: Vec<Expr>,
       output_names: Vec<Option<String>>,
   },
   ```

2. Update every `LogicalPlan::Project { ... }` construction
   site to pass `output_names: vec![None; exprs.len()]`.
   Verified sites listed in §Code-grounded touchpoint sweep.
   `cargo build -p toron` will catch any miss.

3. Update every `LogicalPlan::Project { ... }` destructure
   site:
   - Add `output_names` to explicit destructures.
   - Sites using `{ .. }` rest patterns are unaffected.
   Verified sites listed.

4. In `binder.rs:104`, the `Project` arm constructs
   `ExecutionPlan::Project { output_schema, .. }`. Extend
   schema construction:
   - Validate `exprs.len() == output_names.len()` before
     binding.
   - Bind all expressions as today.
   - Build the derived schema once with existing
     `schema_for_exprs(&bound_exprs)`.
   - Replace each derived field name with `output_names[i]`
     when that entry is `Some(name)`; otherwise keep the derived
     field unchanged.

5. In `exec.rs:224`, the unbound direct-executor `Project`
   path uses similar name resolution. When `output_names[i]`
   is `Some(name)`, the output column name is `name`;
   otherwise derived as today.

6. In `sql_planner.rs:318-337`, the non-aggregate `Project`
   path accepts `SelectItem::ExprWithAlias` via:
   ```rust
   SelectItem::ExprWithAlias { expr, alias } => {
       exprs.push(self.lower_expr(expr)?);
       output_names.push(Some(alias.value.clone()));
   }
   SelectItem::UnnamedExpr(sql_expr) => {
       exprs.push(self.lower_expr(sql_expr)?);
       output_names.push(None);
   }
   ```
   Other `SelectItem` variants continue to reject through the
   existing path.

7. Prefer a small helper for constructing `LogicalPlan::Project`
   with the invariant checked in one place:
   ```rust
   fn project_plan(
       input: LogicalPlan,
       exprs: Vec<Expr>,
       output_names: Vec<Option<String>>,
   ) -> LogicalPlan {
       debug_assert_eq!(
           exprs.len(),
           output_names.len(),
           "ADR 023 invariant: Project exprs/output_names length"
       );
       LogicalPlan::Project {
           input: Box::new(input),
           exprs,
           output_names,
       }
   }
   ```
   If direct struct construction remains in tests or local helper
   code, keep the same `debug_assert_eq!` immediately before it.

8. Binder length validation:
   ```rust
   if exprs.len() != output_names.len() {
       return Err(FrontendError::Unsupported(
           "internal: Project exprs/output_names length mismatch".into()
       ));
   }
   ```

9. Run `cargo build -p toron` → green. Run
   `cargo test -p toron` → green; existing tests pass.

**Definition of Phase 1 done.**
- `LogicalPlan::Project` carries `output_names`.
- All construction + destructure sites updated.
- Non-aggregate `SelectItem::ExprWithAlias` accepted.
- Binder + unbound executor route names into output schema.
- Build + tests green.

### Phase 2 — Catalog rename + builder

**Goal.** Rename `GroupedAggregateScope` →
`AggregateOutputCatalog` with no behavior change in this
phase. Extend the catalog's builder to support SELECT-then-
HAVING aggregate discovery and projected-name tracking. Phase
3 consumes the new substrate; Phase 2 is mechanical refactor
plus additive surface.

**Tasks.**

1. Rename type:
   ```rust
   struct AggregateOutputCatalog {
       entries: Vec<AggregateOutputEntry>,
       output_name_counts: HashMap<String, usize>,
       // NEW in Phase 2 (consumed in Phase 3):
       projected_names: Vec<String>,
       group_keys: Vec<GroupKey>,
   }
   ```
   `AggregateOutputEntry` replaces `GroupedAggregateEntry`;
   add `in_select: bool`, `in_having: bool`, and
   `order_by_visible_name: Option<String>` fields. The
   `order_by_visible_name` is set only for bare aggregate SELECT
   outputs, not for aggregate requirements discovered inside a
   computed SELECT expression.

2. Rename `LoweredSelect.grouped_aggregate_scope` →
   `aggregate_output_catalog: Option<AggregateOutputCatalog>`.

3. Rename `GroupedProjection.grouped_scope` → similar.

4. Update every reference site (15+ sites; `cargo build`
   catches all).

5. Builder additions (used in Phase 3):
   - `AggregateOutputCatalog::from_select_and_having(projection,
     having, group_keys)` — performs SELECT-then-HAVING
     discovery walker.
   - `resolve_for_having(&signature) → ResolveOutcome` —
     returns natural name (HAVING runs on natural Aggregate
     output schema).
   - `resolve_for_order_by(&signature) → ResolveOutcome` —
     returns projected name only when the signature is exposed by
     a bare aggregate SELECT output, else rejects.
   - `projected_name_at(ordinal) → &str` — for ORDER BY alias
     resolution.

6. Phase 2 preserves behavior: existing call sites still use
   `from_projection`; the new builder is unused. `cargo test
   -p toron` green.

**Definition of Phase 2 done.**
- Type rename complete; old name absent from source.
- Builder surface extended without behavior change.
- Build + tests green.

### Phase 3 — Projection rewrite (grouped + scalar)

**Goal.** Replace `lower_grouped_projection`'s strict-positional
shape with catalog-backed lowering; remove the four reject
sites; emit `LogicalPlan::Project` per the four-condition
predicate; preserve identity-shape for currently-accepted
queries.

**Tasks.**

1. Replace `lower_grouped_projection` body:
   - Discovery: walk projection then HAVING; collect unique
     `GroupedAggregateSignature` entries.
   - Validate: at least one aggregate signature exists in
     SELECT or HAVING (ADR 011 zero-aggregate reject
     preserved).
   - Build `Aggregate` over `[group_keys..., aggregates...]`.
   - Lower SELECT projection items through the catalog
     (group-key + aggregate-call references replaced with
     `Expr::column(natural_name)`).
   - Emit `Project` iff the four-condition predicate fires;
     otherwise return naked `Aggregate`.

2. Refactor `lower_global_aggregate_projection`
   (`sql_planner.rs:410-423`) to share the catalog-builder
   substrate. Scalar path uses the same machinery with
   `group_keys = []`.

3. Remove the four strict-positional reject sites:
   - `sql_planner.rs:432`
   - `sql_planner.rs:437`
   - `sql_planner.rs:450`
   - `sql_planner.rs:1101`
   The reject string `"GROUP BY projection must list all keys
   in order before aggregates"` must not appear anywhere in
   `sql_planner.rs` post-Phase-3.

4. Four-condition `Project` emission predicate:
   ```rust
   fn project_emission_required(
       projection: &[SelectItem],
       group_keys: &[GroupKey],
       aggregate_count: usize,
       having_only_aggregates: usize,
   ) -> bool {
       has_any_alias(projection)
           || has_any_computed_expression(projection)
           || projection_order_differs(projection, group_keys, aggregate_count)
           || having_only_aggregates > 0
   }
   ```

5. Identity-shape detection: when none of the four conditions
   fire, the plan shape collapses to `Filter? → Aggregate →
   Exchange(N) → input`, with a byte-identical bound
   `ExecutionPlan` digest to pre-ADR-023. Verify via digest test
   fixture.

6. `cargo test -p toron --test group_by_test` → expect
   currently-accepted tests to still pass; expect previously-
   rejected forms (aliases, reorder, subset, HAVING-only
   aggregates) to now succeed (Phase 4 finishes wiring).

**Definition of Phase 3 done.**
- The four reject sites are removed.
- Catalog-backed lowering emits `Project` per predicate.
- Identity-shape preservation verified.
- Build + tests green for currently-accepted queries;
  previously-rejected forms newly accept (Phase 4 completes).

### Phase 4 — HAVING / ORDER BY rules + diagnostics

**Goal.** HAVING resolves through the catalog's natural-name
bindings; HAVING alias visibility rejects with the new
diagnostic. ORDER BY resolves through projected names; hidden
ORDER BY aggregate calls reject with the new diagnostic.

**Tasks.**

1. `lower_having_predicate` (`sql_planner.rs:468-476`):
   - Uses `AggregateOutputCatalog::resolve_for_having` for
     aggregate-call references.
   - Rejects identifier references that resolve to projected
     aliases (not natural aggregate output names) with the
     diagnostic:
     ```
     HAVING does not resolve SELECT aliases; spell the aggregate explicitly
     ```

2. `lower_order_by` (`sql_planner.rs:340-371`):
   - Uses `AggregateOutputCatalog::resolve_for_order_by` for
     aggregate-call references.
   - Rejects aggregate calls whose signatures are not exposed by
     a bare aggregate SELECT output with the diagnostic:
     ```
     ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list
     ```

3. `rewrite_grouped_aggregate_calls` (`sql_planner.rs:478-506`)
   stays clause-neutral; the catalog method (HAVING vs ORDER
   BY) determines which name binding is returned.

4. `cargo test -p toron --test group_by_test` → all positive
   and negative cases from §Surface matrix green.

**Definition of Phase 4 done.**
- HAVING aggregate calls accepted whether or not they're in
  SELECT; HAVING alias references rejected.
- ORDER BY aggregate calls accepted only when exposed by a bare
  aggregate SELECT output; hidden ORDER BY aggregates rejected.
- Both new diagnostic strings present in source.

### Phase 5 — Fragmenter widening

**Goal.** `is_terminal_sort_over_grouped_aggregate` peels one
optional `[Project]` between the gather `Exchange(1)` and the
optional `[Filter]`; the aggregate-consumer task packages the
projection.

**Tasks.**

1. In `distributed/fragment.rs:519-560`, extend
   `TerminalSortOverAggregate`:
   ```rust
   struct TerminalSortOverAggregate {
       // ... existing fields ...
       projection: Option<(Vec<ExecutionExpr>, SchemaRef)>,  // NEW
   }
   ```

2. In `is_terminal_sort_over_grouped_aggregate`
   (`fragment.rs:562-632`), add the peel BEFORE the existing
   `[Filter]` peel:
   ```rust
   let mut current = input.as_ref();
   let mut projection = None;
   if let ExecutionPlan::Project { input, exprs, output_schema } = current {
       projection = Some((exprs.clone(), output_schema.clone()));
       current = input.as_ref();
   }
   let mut having = None;
   if let ExecutionPlan::Filter { input, predicate, disposition } = current {
       having = Some((predicate.clone(), *disposition));
       current = input.as_ref();
   }
   let ExecutionPlan::Aggregate { ... } = current else {
       return None;
   };
   ```

3. In `fragment_terminal_sort_over_grouped_aggregate`
   (`fragment.rs:634-700`), package the projection into the
   aggregate-consumer task. The consumer plan becomes:
   ```text
   Project? → Filter? → Aggregate → ScanInput
   ```
   where `Project?` and `Filter?` are independent optional
   peels.

4. The terminal-exchange schema is the projection schema when
   `Project` is present; otherwise the aggregate output
   schema.

5. Plan-digest preservation: queries that don't emit `Project`
   (the four-condition predicate doesn't fire) produce
   byte-identical fragment plans pre/post Phase 5.

6. `cargo test -p toron --test distributed_process_test`
   green.

**Definition of Phase 5 done.**
- Detector peels `[Project]` then `[Filter]` then `Aggregate`.
- `TerminalSortOverAggregate` carries optional projection.
- Aggregate-consumer task packages the projection before
  publishing to the terminal exchange.
- Distributed-process-test digest parity for currently-accepted
  queries.

### Phase 6 — Architecture guards + tests + bench

**Goal.** Six existing architecture guards adjust; five new
guards land; positive + negative test fixture suite covers
the §Surface matrix; distributed-process-test digest fixture
covers one composition.

**Tasks.**

1. Architecture guard adjustments
   (`tests/architecture_test.rs`):
   - `adr015_*`: detector body source-read recognizes the new
     `Project?` peel position.
   - `adr016_*`: detector + reject-string list adjustments
     for Filter-below-Project relative position.
   - `adr017_*`: clause-neutral resolver source-read
     preserved.
   - `adr018_*`: scalar branch source-read adjusts for
     catalog rename.
   - `adr021_*`: preserved.
   - `adr022_*`: preserved.

2. New architecture guards
   (`tests/architecture_test.rs`):
   - `aggregate_output_catalog_replaces_grouped_aggregate_scope`:
     source-reads `sql_planner.rs` and asserts
     `GroupedAggregateScope` is absent.
   - `adr023_strict_projection_reject_is_absent`: source-reads
     `sql_planner.rs` for the absence of the string
     `"GROUP BY projection must list all keys in order before
     aggregates"`.
   - `logical_project_carries_output_names`: source-reads
     `ir.rs` for the `output_names: Vec<Option<String>>` field
     declaration on `LogicalPlan::Project`.
   - `adr023_terminal_sort_detector_peels_project_then_filter`:
     source-reads `distributed/fragment.rs` to confirm the
     peel order (Project before Filter).
   - `adr023_diagnostics_are_preserved`: source-reads
     `sql_planner.rs` for both new diagnostic strings.

3. Add positive fixture suite to
   `tests/group_by_test.rs`:
   - Grouped aggregate alias.
   - Scalar aggregate alias.
   - Non-aggregate alias.
   - Reorder.
   - Subset.
   - Existing-`Expr`-surface computed projection
     (`SUM(x) > 10`).
   - HAVING hidden aggregate (grouped + scalar).
   - ORDER BY visible alias.
   - ORDER BY visible bare aggregate call (resolves to alias if
     any).
   - Composition: all of the above in one query.

4. Add negative fixture suite to `tests/group_by_test.rs`:
   - Hidden ORDER BY aggregate.
   - HAVING alias visibility.
   - Scalar arithmetic (existing reject preserved).
   - Computed aggregate argument (existing reject preserved).
   - DISTINCT (existing reject preserved).
   - GROUP BY over joins (existing reject preserved).
   - GROUP BY without any aggregate in SELECT or HAVING
     (existing reject preserved).

5. `tests/distributed_process_test.rs`: one composition
   fixture across 2 and 4 workers, covering alias + reorder +
   HAVING hidden aggregate + ORDER BY visible alias. Finite
   values only; digest parity is the gate.

6. Optional bench artifact: no new fixture required. Post-
   aggregate Project is a per-row transform on a small Stage 1
   output (one row per group); the existing bench rows for
   grouped aggregates measure the relevant path. If a perf
   regression appears post-implementation, capture
   `storage_execution` rows for the alias + computed-comparison
   shapes.

7. Run focused suites:
   ```sh
   cargo fmt --check
   cargo test -p toron --test architecture_test
   cargo test -p toron --test group_by_test
   cargo test -p toron --test distributed_process_test
   cargo test -p toron --test non_aggregate_projection_accounting_test
   cargo test -p toron --test sort_ordering_test
   cargo test -p toron --test clause_accounting_test
   cargo test -p toron
   ```

**Definition of Phase 6 done.**
- Five new architecture guards green.
- Existing architecture guards green after adjustment.
- Positive + negative fixture suite green.
- Distributed-process-test composition fixture green at 2 +
  4 workers.
- `cargo fmt --check` clean; `cargo test -p toron` green.

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | `ExecutionPlan` digest regression for currently-accepted queries | Identity-shape predicate (four conditions) suppresses `Project` emission. Distributed-process-test digest fixture covers; architecture guard pins the predicate site. |
| R2 | `output_names.len() != exprs.len()` invariant violation | Project-construction helper or direct-site `debug_assert!`; binder validates at bind time and returns `FrontendError`; architecture guard `logical_project_carries_output_names` source-reads the field declaration. |
| R3 | Phase 1 misses a `Project` construction or destructure site | `cargo build -p toron` catches at compile time; the touchpoint sweep enumerates 14+ sites for visual review before commit. |
| R4 | Catalog rename misses a reference site | `cargo build` catches; architecture guard asserts old name is absent. |
| R5 | The HAVING-only aggregate is not dropped by Project | Test fixture: `SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 100` expects output schema `[region]`, not `[region, sum]`. Phase 4 wiring + Phase 5 fragmenter packaging both required. |
| R6 | Hidden ORDER BY aggregate slips through | Reject-string-preservation guard for the new diagnostic. Negative fixture: `SELECT region FROM sales GROUP BY region ORDER BY SUM(amount)` rejects with the exact diagnostic. |
| R7 | HAVING alias resolution accidentally enabled | Reject-string-preservation guard. Negative fixture: `SELECT SUM(x) AS total FROM sales HAVING total > 10` rejects with the exact diagnostic. |
| R8 | Computed projection accidentally exercises non-existent arithmetic `Expr` | Existing `lower_expr` unsupported-expression reject preserved; no new diagnostic needed. Negative fixture: `SELECT SUM(x) + 1 FROM sales` rejects through the existing path. |
| R9 | Non-aggregate alias enablement (Phase 1) regresses an existing non-alias test | Existing non-aggregate projection accounting tests (`non_aggregate_projection_accounting_test.rs`) green; the change is additive at the `SelectItem::ExprWithAlias` arm, not modification of the `UnnamedExpr` arm. |
| R10 | Fragmenter detector accidentally widens to recognize unintended shapes | Architecture guard `adr023_terminal_sort_detector_peels_project_then_filter` asserts exact peel order; no more than two optional operators before `Aggregate`. |
| R11 | ORDER BY visible-aggregate rewrite returns natural name instead of projected name | Catalog method `resolve_for_order_by` returns the projected name for bare aggregate SELECT outputs; test fixture: `SELECT SUM(x) AS total ORDER BY SUM(x)` produces a sort key on `total`, asserted by binder schema inspection. |
| R12 | The four `LogicalPlan::Project` constructions in `binder.rs` (lines 1431, 1470, 1531, 1551) are NOT for SQL-planner-emitted projections; they're for direct binder use (likely aggregate-projection internal) | Phase 1 sweep treats them identically (`output_names: vec![None; ...]`); the binder's name resolution falls back to derived names for `None` entries, preserving existing behavior. |
| R13 | The rune-engine `Project` construction at `rune_engine.rs:198` is an integration-test or Rune-script path | Phase 1 passes `output_names: vec![None; ...]`; the rune surface doesn't expose alias syntax independently. |
| R14 | Phase 3 + Phase 4 + Phase 5 must bundle for HAVING-only aggregates to work end-to-end | Bundle Phase 3 + 4 + 5 as one landing commit OR keep Phases 3 + 4 in a feature branch until Phase 5 is included. Mirrors ADR 022 Phase 1+2 bundling discipline. |
| R15 | The new `output_names` field changes `LogicalPlan` serde shape; the wire-format for coordinator-internal IR transmission changes | `LogicalPlan` is coordinator-internal (ADR 003 G12 enforces). Workers receive `TaskSpec(ExecutionPlan)`, not `LogicalPlan`. ADR 003 reject-on-mismatch posture covers the single-workspace bump. |
| R16 | The distributed-process-test digest fixture asserts on byte patterns including `LogicalPlan` IR | Coordinator-internal `LogicalPlan` isn't part of any distributed digest. `ExecutionPlan::Project.output_schema` is the binder-built schema; identity-shape preservation keeps `ExecutionPlan` digests byte-identical for currently-accepted queries. |

## Test plan

Existing tests that must stay green:

- `tests/architecture_test.rs` — adjusted + 5 new guards.
- `tests/group_by_test.rs` — extended positive + negative
  suite.
- `tests/distributed_process_test.rs` — 1 new composition
  fixture.
- `tests/non_aggregate_projection_accounting_test.rs` — Phase
  1 destructure pattern updates.
- `tests/sort_ordering_test.rs` — Phase 1 destructure pattern
  updates.
- `tests/clause_accounting_test.rs` — Phase 1 pattern updates
  if needed.
- `tests/aggregate_projection_accounting_test.rs` — projection
  accounting unaffected at the SELECT-list-validation layer.
- `tests/wendelon_storage_execution_test.rs` — Phase 1
  construction-site updates.
- Binder + planner unit tests — alias / diagnostic verification.

New test files:

- None. All semantic coverage consolidated in
  `tests/group_by_test.rs`.

Bench target:

- No new bench rows required for the implementation gate. If
  post-implementation perf evidence warrants, add
  `group_alias_projection` and `group_having_only_aggregate`
  rows to `benches/storage_execution.rs`.

## Definition of done

The migration lands when:

1. `LogicalPlan::Project` carries `output_names:
   Vec<Option<String>>`; length invariant enforced.
2. `sql_planner.rs` references `AggregateOutputCatalog`; old
   name absent.
3. Four strict-positional reject sites removed; reject string
   absent from source.
4. SELECT-then-HAVING aggregate discovery operational.
5. `LogicalPlan::Project` emitted iff the four-condition
   predicate fires; otherwise omitted.
6. HAVING rewrites resolve against natural Aggregate output;
   HAVING alias visibility rejects with the new diagnostic.
7. ORDER BY rewrites resolve against projected output and bare
   aggregate SELECT outputs; hidden ORDER BY aggregate calls
   reject with the new diagnostic.
8. Non-aggregate `SelectItem::ExprWithAlias` accepted.
9. `is_terminal_sort_over_grouped_aggregate` peels optional
   `[Project]` then `[Filter]`.
10. Five new architecture guards green; six existing guards
    green after adjustment.
11. Surface matrix (positive + negative) tested in
    `group_by_test.rs`.
12. Distributed-process-test composition fixture green at 2 +
    4 workers.
13. `ExecutionPlan` digests for currently-accepted queries
    byte-identical.
14. `cargo fmt --check` clean; `cargo test -p toron` green.

## Phase ordering rationale

Phase 1 (IR field substrate) lands first as the mechanical
refactor that enables both the alias surface (non-aggregate
path can immediately accept `ExprWithAlias`) and the catalog
naming work (Phase 3 emits `Project` with non-`None`
`output_names`). Phase 2 (catalog rename) is a pure refactor
with no behavior change; can land independently and reduces
the diff size of Phase 3.

Phase 3 (projection rewrite) + Phase 4 (HAVING / ORDER BY
diagnostics) + Phase 5 (fragmenter widening) bundle as one
landing commit because:

- The HAVING-only aggregate case requires the four-condition
  predicate (Phase 3) AND the new HAVING resolver
  (Phase 4) AND the fragmenter packaging of `Project` on the
  aggregate-consumer task (Phase 5) all in place. Splitting
  leaves an intermediate broken state where HAVING-only
  aggregates either fail to drop from final output (missing
  Phase 3 / 5) or fail to resolve (missing Phase 4).

Phase 6 (guards + tests + bench) is the final correctness gate.

This matches ADR 022 Phase 1+2 bundling discipline (avoid
intermediate broken-tests states) and ADR 021 Phase ordering
(refactor before behavioral change).
