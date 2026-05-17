# ADR 032: SQL `INSERT INTO ... SELECT` — Local Read-Write Composition

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached in three substantive iterations. Iter-0
selected the topic per ADR 031's "ADR 032 candidates" list
(composition ADRs that exercise the substrate). Iter-1
settled the load-bearing shape decisions: separate IR
variants (Shape A) instead of widening the VALUES `Insert`
variant; positional source-to-target column mapping with a
bound `target_columns: Vec<ColumnOrdinal>` carrier; binder-
validated arity / type-admissibility; materialize-before-
write coordinator path that reuses ADR 030 `StorageWrite`;
strict numeric cast (ADR 025) for array coercion, not
`TRY_CAST`; runtime null-count enforcement against non-null
target fields; outer morsels do not partition the source
SELECT; local-coordinator-only execution with explicit
distributed rejection before fragmentation. Iter-2 was
promotion preparation: ratified Codex iter-1's R1-R5
precision items + added three small Claude deltas (C5
`evaluate_cast` visibility guard wording, C6 source-arity
check guard wording, C7 JOIN composition fixture elevated
to Definition-of-Done priority alongside the SUM fixture)
and pinned the distributed-reject diagnostic and telemetry
label.

**First composition ADR** after the ADR 024 → ADR 031
substrate arc (eight consecutive substrate-introducing /
substrate-downstream / substrate-replacing cycles). ADR
032 introduces no new substrate of its own — it adds two
plan-IR variants and one coordinator helper that compose
ADR 029 catalog identity, ADR 030 `StorageWrite`, ADR 031
`BoundExprProgram` (inside the bound source SELECT), ADR
025 strict cast kernels, and the existing SELECT execution
pipeline. **First zero-substrate-invariant-break cycle
since ADR 028**: ADR 029 + 030 were substrate-introducing
(additive, zero break); ADR 031 broke the byte-identical
SELECT plan-digest invariant by substrate-replacing
construction; ADR 032 composes and re-stabilizes (D10
restores the post-ADR-031 byte-identical SELECT plan-
digest baseline going forward). **First cycle to break
the Gemini iter-1/2 retraction streak** (was 6-of-6 across
ADR 025 / 026 / 028 / 029 / 030 / 031; broken at 6-of-7
because Gemini iter-1 converged directly with Codex iter-1
shape without proposing broader-scope framings that iter-
2/3 needed to retract). **First cycle to break the topic-
concession-compresses-next-cycle pattern** (was 3-of-3 at
ADR 020→021, 027→028, 029→031; broken because the ADR
031→032 transition did not generate an iter-0 alt-topic
seed). Both breaks are recorded as cycle-property
refinements rather than regressions: broader-scope framings
and seed-pattern carry-forward are **substrate-cycle
phenomena**, not universal cycle phenomena. Fourteenth
instance of the ADR 015 cycle (after 016 / 017 / 018 /
019 / 022 / 023 / 024 / 025 / 026 / 027 / 028 / 029 /
030 / 031).

## Scale boundary

**Primary:** Toron SQL planner + binder + local
`QueryCoordinator` + existing `StorageWrite` boundary.
Surface widens the statement layer in
`SqlPlanner::lower_insert` to handle non-VALUES `Query`
sources; adds `LogicalPlan::InsertSelect` +
`ExecutionPlan::InsertSelect` variants alongside (not
replacing) `LogicalPlan::Insert` + `ExecutionPlan::Insert`;
adds one coordinator helper that executes the bound source
SELECT, coerces and concatenates source batches into one
target-schema `RecordBatch`, and calls `StorageWrite::write`
once.

In scope:

- `SqlPlanner::lower_insert` accepts `Statement::Insert`
  whose source is a non-VALUES `Query`. A small private
  `LoweredInsertSource::{Values, Select}` split keeps the
  VALUES path byte-identical and routes non-VALUES
  through `lower_query`.
- New `LogicalPlan::InsertSelect { table: TableRef,
  columns: Option<Vec<String>>, source: Box<LogicalPlan> }`
  variant.
- New `ExecutionPlan::InsertSelect { legacy_table_id:
  TableId, table: TableObjectId, fingerprint:
  PhysicalDescriptorFingerprint, schema: SchemaRef,
  target_columns: Vec<ColumnOrdinal>, source:
  Box<ExecutionPlan> }` variant.
- New `Binder::bind_insert_select` + shared
  `resolve_insert_target_columns(schema, columns)`
  helper extracted from `bind_insert`.
- New `materialize_insert_select_batch(target_schema,
  target_columns, source_batches) -> Result<Option<RecordBatch>,
  ExecutionError>` private helper in
  `coordinator/write.rs`.
- `QueryCoordinator::execute_query` intercepts
  `LogicalPlan::InsertSelect` next to
  `LogicalPlan::Insert`, **before** the SELECT
  descriptor-evolution retry loop.
- `QueryCoordinator::execute_write_plan` extends to
  handle `ExecutionPlan::InsertSelect` by executing the
  source via `Executor::execute_bound(source, None)`,
  materializing through
  `materialize_insert_select_batch`, and calling the
  existing `StorageWrite::write` exactly once when the
  batch is non-empty.
- `DistributedQueryCoordinator::execute_query` rejects
  `ExecutionPlan::InsertSelect` before `fragment_plan`
  with stable diagnostic `"distributed INSERT INTO ...
  SELECT execution is not wired to a write-context-
  bearing coordinator"` (parallels ADR 030 D13 wording
  but disambiguates the DML form).
- `crate::exec::evaluate_cast` visibility widens from
  private to `pub(crate)` so the coordinator helper can
  call ADR 025's strict cast kernels for numeric
  coercion. `evaluate_try_cast` and lower-level cast
  kernels stay private.
- New `FragmentError::InsertSelectNotFragmentable`
  variant; fragmenter rejects before `contains_join` /
  `count_exchanges`.
- Distributed / server / scheduler metadata walkers
  treat `ExecutionPlan::InsertSelect` as a leaf (join
  detection returns false; exchange counting returns
  zero; scan metadata / fingerprint lookups return
  none; replacement walkers leave the variant unchanged).
  Binder optimization is the one intentional exception:
  `optimize_for_required` recurses into the source
  with `required_output = None` so existing scan-
  projection pushdown for the source SELECT survives
  (R2).
- Defensive `Executor::execute_bound` reject for any
  uncoordinated `ExecutionPlan::InsertSelect` reach.
- Telemetry labels for distributed / server / scheduler
  walker `InsertSelect` arms use `"insert_select"` (not
  `"insert"`, which ADR 030 already owns).
- 14 architecture guards (12 ADR 032 specific + 2
  composition guards verifying preserved invariants
  from ADR 030 + ADR 031).
- Two required end-to-end composition fixtures: SUM
  grouped aggregate source and INNER JOIN source.

Out of scope:

- `CREATE TABLE AS SELECT (CTAS)`.
- `INSERT ... RETURNING` clause; statement still
  returns `Ok(Vec::new())`; no row count emitted.
- `ON CONFLICT` / `UPSERT` / `INSERT OR REPLACE` /
  `INSERT IGNORE` / `REPLACE INTO` / `INSERT OVERWRITE`
  / `MERGE`.
- `DEFAULT` value expressions; `GENERATED` columns;
  named constraints; dialect-specific INSERT clauses
  (assignments, partitions, settings, format clauses,
  priorities, target aliases).
- `UPDATE` / `DELETE` (DML siblings).
- Distributed `INSERT ... SELECT` execution
  (coordinator-only in v1; future cross-ring
  distributed-write-context ADR lifts).
- Multi-batch / streaming `StorageWrite` (statement
  fails before write if source row count exceeds
  `TABLE_WRITE_MAX_ROWS`).
- Write retries after descriptor evolution
  (`InsertSelect` does not enter the SELECT
  descriptor-evolution retry loop in v1; mirrors
  ADR 030 D12).
- Read-your-writes session HLC advancement.
- New Wendelon API surface; no new wrapper or
  receipt field consumed.
- New PBC opcode or typed expression tape; no new
  `BoundExprProgram` variant; no `ExecutionExpr`
  resurrection.
- New SQL syntax beyond the bare `INSERT INTO [schema.]
  table [(col, ...)] <select-statement>` form.
- New type lattice extension; ADR 029's six-type
  lattice (`Boolean`, `Int32`, `Int64`, `Float32`,
  `Float64`, `Utf8`) is the coercion target set; ADR
  030's `coerce_insert_literal` lattice (which is
  literal-shape rather than array-shape) is unchanged.
- New error variants; `ExecutionError::InvalidExpression`
  carries the row-cap / coercion / null-count / arity
  diagnostics.
- New aggregate kernel; no fragmenter detector change
  for SELECT paths beyond the leaf-treatment shims.

## Context

ADR 029 added the DDL surface (`CREATE TABLE`); ADR 030
added the DML surface (`INSERT INTO ... VALUES`); ADR 031
replaced the recursive bound physical expression IR with
the index-based `BoundExprProgram`. The substrate arc is
complete: catalog identity, write trait, expression
substrate, statement-layer interception, and coordinator
write routing all exist on `main` and are exercised by
end-to-end tests for `CREATE TABLE; INSERT VALUES; SELECT`
chains.

The natural composition step is `INSERT INTO ... SELECT`:
write rows derived from a query rather than from inline
literals. It exercises every substrate that the ADR 024 →
031 arc shipped (bound `BoundExprProgram` evaluation for
the source SELECT, ADR 029 catalog identity for the target,
ADR 030 `StorageWrite` for the write call, ADR 025 strict
cast kernels for array coercion, ADR 028 grouped-aggregate
signature for `SUM` / `AVG` aggregate sources) in a single
SQL statement. It also resolves a long-standing test-shape
friction: end-to-end fixtures currently load test rows via
VALUES; ADR 032 lets fixtures derive rows from other
queries, including aggregate roll-ups.

The load-bearing decisions are: **how to extend the IR**
(widen the VALUES `Insert` variant vs add a separate
`InsertSelect` variant), **whether to bind the source
plan as part of `InsertSelect` or carry the unbound
`LogicalPlan` and defer**, **how to map source output
columns to target columns** (positional with a bound
ordinal carrier vs name-based at write time), **whether
source-schema nullability rejects bind-time or runtime**
(R4), **whether coordinator-resident materialization
streams or buffers** (D5 + D6), **what cast policy applies
when source and target types differ** (strict vs try-cast;
R3), **whether outer morsel partitioning affects the
source SELECT** (R5), **whether the binder optimizer may
descend into the source SELECT** (R2), and **whether
distributed execution attempts fragmentation** (D1, no).

The council converged on Shape A separate IR variants;
binder produces a bound `ExecutionPlan` source as part of
`InsertSelect`; positional mapping with
`target_columns: Vec<ColumnOrdinal>` in source-output
order; runtime null-count enforcement; full materialization
before a single `StorageWrite::write` call with a
`TABLE_WRITE_MAX_ROWS = 65_536` total-row cap; strict
ADR 025 cast for numeric coercion; outer morsels ignored
for the source SELECT (mirrors ADR 030 VALUES pattern);
binder optimizer descent allowed with one intentional
exception note; local-coordinator-only execution with
explicit distributed-coordinator + fragmenter rejection
upstream of any fragmentation analysis.

## Decision

### D1 — Local coordinator only

Local `QueryCoordinator::execute_query` intercepts
`LogicalPlan::InsertSelect` next to `LogicalPlan::Insert`,
**before** the SELECT descriptor-evolution retry loop.
Distributed `DistributedQueryCoordinator::execute_query`
rejects `ExecutionPlan::InsertSelect` **before**
`fragment_plan` with stable diagnostic `"distributed INSERT
INTO ... SELECT execution is not wired to a write-context-
bearing coordinator"`. The fragmenter has a defensive
`FragmentError::InsertSelectNotFragmentable` reject that
runs before any `contains_join` / `count_exchanges` work.

Defensive reach also lives in `Executor::execute_bound`
(uncoordinated `ExecutionPlan::InsertSelect` returns
`NotImplemented`). All four reject sites are independent
guards: the planner reaches `LogicalPlan::InsertSelect`
only through `lower_insert`; the binder reaches
`ExecutionPlan::InsertSelect` only through
`bind_insert_select`; the local coordinator intercepts
before retry; the distributed coordinator and fragmenter
reject before fragmentation. None silently accept; none
silently drop.

### D2 — Separate IR variants (Shape A)

Two new variants are added; the existing `Insert`
variants stay VALUES-only:

```rust
LogicalPlan::InsertSelect {
    table: TableRef,
    columns: Option<Vec<String>>,
    source: Box<LogicalPlan>,
}

ExecutionPlan::InsertSelect {
    legacy_table_id: TableId,
    table: TableObjectId,
    fingerprint: PhysicalDescriptorFingerprint,
    schema: SchemaRef,
    target_columns: Vec<ColumnOrdinal>,
    source: Box<ExecutionPlan>,
}
```

The existing `LogicalPlan::Insert` /
`ExecutionPlan::Insert` variants remain expression-free
literal carriers (ADR 030 D4 carry-forward). Widening the
VALUES variant to also carry a bound source plan would
have collapsed two distinct DML shapes into one — the
VALUES path has literal rows and no source plan; the
SELECT path has no literal rows and a bound source plan.
Shape A preserves the existing VALUES architecture guards
verbatim and lets the InsertSelect variant carry its own
disjoint shape.

`LogicalPlan` variant count: 11 → 12.
`ExecutionPlan` variant count: 9 → 10.

### D3 — Positional source-to-target mapping with bound `target_columns`

`ExecutionPlan::InsertSelect.target_columns: Vec<ColumnOrdinal>`
records the destination column ordinals in **source-output
order**. The schema is the full target payload schema;
`target_columns` tells the coordinator where each source
output column lands. The binder resolves the optional
target column list to ordinals; the coordinator never
re-parses the target column list.

- With no column list: `INSERT INTO dst SELECT a, b, c
  FROM src`. `target_columns = [0, 1, 2]` and source
  arity must equal target schema arity.
- With a column list: `INSERT INTO dst (b, a) SELECT x,
  y FROM src`. `target_columns = [1, 0]` and source
  arity must equal `target_columns.len()`.

Without this carrier, column reorders would require
recovering planner-only target-list state after binding.
Source output column **names** are not compared to target
column names; SQL `INSERT` column mapping is positional
after the optional target column list is resolved (Codex
iter-1 R1 + sql_planner.rs convention).

### D4 — Binder validation

`Binder::bind_insert_select` performs:

- resolve the target table through the catalog;
- call `resolve_insert_target_columns(schema, columns)`
  to translate the optional column list to
  `Vec<ColumnOrdinal>` with duplicate-name + unknown-
  column + omitted-not-null rejects;
- bind the source logical plan through the normal SELECT
  binder (`bind_plan`);
- leave the bound source as the `InsertSelect` child;
  the existing top-level `push_down_scan_projection`
  pass reaches it through the D11 `optimize_for_required`
  arm, so the source SELECT keeps scan-projection
  pushdown without a second ad hoc optimizer call;
- compare source output arity with `target_columns.len()`
  and reject mismatches with a stable diagnostic that
  references both numbers;
- validate source output type to target field type via
  `is_insert_select_coercible(source_ty, target_ty)`
  helper that mirrors ADR 025 strict-cast numeric-cell
  admissibility plus identity for Boolean and Utf8;
- emit `ExecutionPlan::InsertSelect` with the full target
  schema, the resolved `target_columns`, and the bound +
  optimized source.

The binder does NOT reject a nullable source field
feeding a non-null target field at bind time. Source-
schema nullability is a static type-system property;
actual array null-counts are runtime data. A filter like
`WHERE col IS NOT NULL` can produce a nullable-typed
source that has zero actual nulls. Runtime null-count
enforcement is D9.

The binder DOES reject omitted non-null target columns
(known from the target column list at bind time) with the
same diagnostic ADR 030 D14 emits for `bind_insert`.

`resolve_insert_target_columns` is shared with
`bind_insert` so the resolution / duplicate-check /
unknown-column / omitted-not-null discipline stays
identical across VALUES and SELECT INSERT.

### D5 — Materialize before write

The coordinator executes the bound source once via
`Executor::execute_bound(source, None)`, materializes all
source batches, enforces the total-row cap (D6), coerces
and reorders columns through `materialize_insert_select_batch`,
and then calls `StorageWrite::write` exactly once when the
result is non-empty.

`WriteRequest` still accepts one `RecordBatch` payload —
no streaming trait change in this cycle. Multi-batch or
chunked writes are deferred to a future ADR.

The coordinator never passes the outer `morsel` into the
source SELECT path. ADR 030 VALUES INSERT already ignored
`morsel` because DML statements do not partition by
scan-morsel; ADR 032 preserves that behavior (D12 /
Codex iter-1 R5). Without this rule, a top-level scan
morsel could silently write only one morsel's worth of
rows.

### D6 — Total row cap

`TABLE_WRITE_MAX_ROWS = 65_536` applies to the **total**
source row count, not per-batch. If the source produces
more rows, the statement fails before any write with
`ExecutionError::InvalidExpression` and a stable
diagnostic that references both the actual count and the
cap. The check happens after materialization (so all
source batches have already been collected) but before any
coerced-batch construction, so the failure path does not
do unnecessary cast work.

The cap matches ADR 030 D15 and Wendelon's `TABLE_WRITE_MAX_ROWS`
constant. The cap is enforced once globally per
`InsertSelect` statement; chunking into multiple
`StorageWrite::write` calls is out of scope.

### D7 — Empty source is a no-op

If the source SELECT produces zero rows, the coordinator
returns `Ok(Vec::new())` without calling
`StorageWrite::write`. `materialize_insert_select_batch`
returns `Ok(None)` for zero rows; the coordinator routing
short-circuits to `Ok(Vec::new())`.

This intentionally does not solve empty-table storage
initialization (a `INSERT INTO dst SELECT * FROM
empty_src` does not advance any descriptor metadata for
`dst`). A future ADR may add an explicit "initialize
empty table" path; ADR 032 does not.

### D8 — Strict array coercion

Numeric source arrays are coerced via the existing ADR
025 strict cast kernels through the
`crate::exec::evaluate_cast` entry point (visibility
widened to `pub(crate)`). Boolean and Utf8 require
identity type (no widening, no narrowing). Unsupported
source-type / target-type combinations fail before write
with a stable diagnostic.

`evaluate_try_cast` and the lower-level numeric cast
kernels stay private. ADR 032 explicitly does NOT use
`TRY_CAST` semantics: numeric overflow during coercion
fails the statement rather than nulling cells. This
matches user expectation that `INSERT INTO ... SELECT`
preserves data fidelity; users who want fail-to-null
semantics can spell `TRY_CAST` explicitly inside the
source SELECT.

The supported target-type lattice is ADR 030's six-type
set: `Boolean`, `Int32`, `Int64`, `Float32`, `Float64`,
`Utf8`. ADR 029 created this lattice; ADR 030 reused it
for `coerce_insert_literal`; ADR 032 reuses it for array
coercion. No new type is admissible.

### D9 — Non-null enforcement before write

After coercion and any typed-null fill for omitted
nullable target columns, the coordinator checks every
non-null target field: if the corresponding array has
`null_count() > 0`, the statement fails before
`StorageWrite::write` with a stable diagnostic referencing
the offending column name.

This is a runtime check on actual data, not a bind-time
check on schema nullability. The two are deliberately
split: omitted target columns of non-null type reject at
bind time (the writer can see the target column list and
the target schema); nullable-typed source feeding a
non-null target rejects at runtime only when actual rows
contain null cells (the writer cannot see the data until
the source executes).

### D10 — SELECT plan stability resumes

ADR 032 is additive over the post-ADR-031 SELECT plan-
digest baseline. Existing SELECT-only plan shapes and
plan digests remain byte-identical pre/post-ADR-032
because the new `InsertSelect` variants only appear under
`INSERT INTO ... SELECT` statements; SELECT-only
statements never produce them.

ADR 031 D14 broke the byte-identical SELECT plan-digest
invariant by substrate-replacing construction (`ExecutionExpr`
→ `BoundExprProgram`); ADR 032 re-stabilizes the
invariant going forward at the post-ADR-031 baseline. The
break-then-restabilize pattern is the cycle property:
substrate-replacing ADRs may break promised invariants
with explicit D-level ratification; subsequent
composition ADRs re-stabilize the new baseline.

### D11 — Binder optimizer may descend; distributed walkers treat as leaf

The "InsertSelect source is a leaf" rule applies to
**distributed-planning walkers** (fragmenter / scheduler /
server metadata helpers), where `InsertSelect` is already
rejected as coordinator-only DML. They return:

- join detection: false / none;
- exchange counting: zero;
- scan metadata / fingerprint lookup: none;
- replacement walkers: leave the plan unchanged.

The binder's `optimize_for_required` pass is the one
intentional exception. It may recurse into the source
with `required_output = None`, because every source
output column is consumed by the write mapping. This
preserves existing ADR 013 / 014 runtime-filter and
scan-projection pushdown optimization for the source
SELECT without making distributed planning see
`InsertSelect` as a fragmentable tree.

This split is enforced behaviorally and through the
architecture guard `adr032_distributed_walkers_treat_insert_select_as_leaf`
(distributed-side) plus a binder behavioral test
(source scan projection observed to narrow).

### D12 — Outer morsels do not partition DML writes

`QueryCoordinator::execute_query` already ignores `morsel`
for VALUES INSERT. `InsertSelect` preserves that behavior:
the source SELECT executes with `None` morsel inside the
`InsertSelect` coordinator path. A top-level write
statement is not a scan-morsel-partitioned operator —
treating it as one would silently write only one morsel's
worth of rows.

This pin is a defensive guardrail; the existing morsel
plumbing in the read path does not naturally affect DML,
but the explicit `None` argument prevents any future
refactor from accidentally propagating outer morsels into
the write path. Codex iter-1 R5.

### D13 — `evaluate_cast` visibility is the only cast-helper exposure

`crate::exec::evaluate_cast` visibility widens from
private to `pub(crate)` so the coordinator helper at
`crate::coordinator::write` can invoke ADR 025 strict
cast kernels for source-array coercion. No other cast-
helper visibility changes:

- `evaluate_try_cast` stays private (ADR 026 carry-
  forward; D8 forbids TRY_CAST semantics for InsertSelect
  coercion);
- lower-level numeric cast kernels stay private;
- no new public API is exposed outside the toron crate.

The change is the minimum visibility widening that lets
the new helper reuse the existing strict cast substrate
without creating a parallel cast implementation. Claude
iter-1 C5.

## Reject removal (ADR 015 cycle's fourteenth instance)

ADR 032 lifts one user-facing reject and adds three new
runtime rejects:

**Lifted:** `SqlPlanner::lower_insert` previously rejected
`INSERT` statements whose source was a non-VALUES
`Query`. The new `LoweredInsertSource::{Values, Select}`
split routes non-VALUES query bodies through
`lower_query` and emits `LogicalPlan::InsertSelect` instead
of returning an unsupported diagnostic.

**Added (binder, bind time):**

- arity mismatch between source output count and
  `target_columns.len()` with both numbers in the
  diagnostic;
- source output type not admissible for the corresponding
  target field under the ADR 032 lattice (e.g., Utf8
  source into Int64 target).

**Added (coordinator, runtime, before write):**

- total source row count > `TABLE_WRITE_MAX_ROWS`;
- non-null target field receives a coerced array with
  `null_count() > 0` (column name in diagnostic);
- numeric coercion overflow (propagated from
  `evaluate_cast`).

**Added (distributed coordinator, before fragmentation):**

- `ExecutionPlan::InsertSelect` rejected as
  coordinator-only DML.

**Added (fragmenter, before join / exchange analysis):**

- `FragmentError::InsertSelectNotFragmentable`.

The existing VALUES INSERT rejects (`RETURNING`, `ON
CONFLICT`, target alias, `REPLACE`, `OVERWRITE`,
`PARTITION`, `FORMAT`, non-literal `VALUES` expression)
remain unchanged. No reject is silently widened or
narrowed beyond the explicit lift.

## Architecture Guard Set

14 guards total: 12 ADR-032-specific + 2 composition
guards verifying ADR 030 + ADR 031 invariants survive
ADR 032.

| Guard | Pins | File anchor |
|---|---|---|
| `adr032_logical_plan_variant_count_is_12` | `LogicalPlan` 11 → 12 growth | `ir.rs` |
| `adr032_execution_plan_variant_count_is_10` | `ExecutionPlan` 9 → 10 growth | `ir/execution.rs` |
| `adr032_insert_stays_values_only` | `ExecutionPlan::Insert` still contains `rows: Vec<Vec<ScalarValue>>` with no `BoundExprProgram` or `PbcProgram` | `ir/execution.rs` |
| `adr032_insert_select_carries_target_columns` | `ExecutionPlan::InsertSelect` contains `target_columns: Vec<ColumnOrdinal>` + `source: Box<ExecutionPlan>` | `ir/execution.rs` |
| `adr032_insert_select_no_direct_bound_expr_program` | `InsertSelect` variant body has no direct `BoundExprProgram` or `PbcProgram` field | `ir/execution.rs` |
| `adr032_local_coordinator_intercepts_before_retry` | Local intercept precedes the SELECT descriptor-evolution retry loop | `coordinator.rs` |
| `adr032_distributed_coordinator_rejects_before_fragment` | Distributed reject precedes `fragment_plan` | `coordinator.rs` |
| `adr032_fragmenter_rejects_before_join_or_exchange` | `FragmentError::InsertSelectNotFragmentable` reject precedes `contains_join` / `count_exchanges` | `distributed/fragment.rs` |
| `adr032_distributed_walkers_treat_insert_select_as_leaf` | Distributed / server / scheduler metadata walkers return leaf for `InsertSelect` (binder optimizer is the documented exception) | `distributed/*`, `server/mod.rs` |
| `adr032_coordinator_uses_only_evaluate_cast` | `coordinator/write.rs` calls `evaluate_cast` and does not call `evaluate_try_cast` | `coordinator/write.rs` |
| `adr032_binder_enforces_source_target_arity` | `bind_insert_select` body references `target_columns.len()` and source arity for the arity check | `binder.rs` |
| `adr032_storage_write_trait_unchanged` | `StorageWrite::write` signature stable; single `RecordBatch` payload | `storage.rs` |

Composition guards verifying preserved invariants:

| Guard | Pins | File anchor |
|---|---|---|
| `adr030_insert_stays_expression_free` | ADR 030 D4 + ADR 031 D10 invariant survives ADR 032 | `ir/execution.rs` |
| `adr031_bound_expr_program_partial_eq_only` | ADR 031 D7 invariant survives ADR 032 | `ir/execution.rs` |

Each guard's diagnostic string names ADR 032 + the
specific decision it enforces, per the ADR 015 cycle
narrow-detector pattern.

## Required composition fixtures

Two end-to-end fixtures are required for promotion. Both
exercise the substrate arc end to end and must pass before
ADR 032 lands.

### Fixture A — Grouped aggregate source

```sql
CREATE TABLE src (id INT, amount BIGINT);
CREATE TABLE dst (id INT, total BIGINT);
INSERT INTO src VALUES (1, 100), (1, 200), (2, 50);
INSERT INTO dst (id, total)
SELECT id, SUM(amount) FROM src GROUP BY id;
SELECT id, total FROM dst ORDER BY id;
-- Expected: (1, 300), (2, 50)
```

Exercises ADR 029 catalog identity (two tables); ADR 030
VALUES write (initial seed); ADR 028 grouped-aggregate
signature substrate; ADR 031 `BoundExprProgram` aggregate
evaluation; ADR 032 InsertSelect write of aggregate
output; ADR 015-style terminal ORDER BY on the readback.

### Fixture B — INNER JOIN source

```sql
CREATE TABLE left_table (id INT, label TEXT);
CREATE TABLE right_table (id INT, score BIGINT);
CREATE TABLE dst (id INT, label TEXT, score BIGINT);
INSERT INTO left_table VALUES (1, 'a'), (2, 'b');
INSERT INTO right_table VALUES (1, 100), (2, 200);
INSERT INTO dst (id, label, score)
SELECT l.id, l.label, r.score
FROM left_table l INNER JOIN right_table r ON l.id = r.id;
SELECT id, label, score FROM dst ORDER BY id;
-- Expected: (1, 'a', 100), (2, 'b', 200)
```

Exercises ADR 008 / 009 join substrate (inner equi-join);
ADR 030 VALUES write (initial seed of both source tables);
ADR 032 InsertSelect write of join output; ADR 015-style
terminal ORDER BY on the readback. The explicit target
column list `(id, label, score)` exercises the
`target_columns` carrier (D3 / R1).

The two fixtures together validate that the substrate arc
composes across the two most distinct source-plan shapes
(grouped aggregate; inner join). One fixture alone leaves
either substrate path uncovered.

## SoA-readiness check

ADR 021's four SoA-readiness invariants preserved:

1. **Catalog stays pure metadata for kernels.** ADR 032
   adds no kernel; catalog unchanged.
2. **`#[repr(u8)]` on `StableAggregateKernel` unchanged.**
   No aggregate kernel touched.
3. **Centralized dispatch unchanged.** Coordinator helper
   reuses existing per-(source, target) cast kernels via
   `evaluate_cast`; no new dispatch site.
4. **No row-path dyn dispatch.** Coordinator helper
   operates on already-bound `ArrayRef`s; no `dyn` added.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched — ADR
  032 adds no expression substrate; existing
  `BoundExprProgram` JIT seam from ADR 031 unchanged.
- **Brainstorm 002 (WeakControlRing).** Untouched — pure
  Toron-local cycle.
- **Brainstorm 003 (Disaggregated Compute).** Touched
  but preserved — distributed coordinator gets one new
  reject arm; no fragmentation work added. Future
  distributed-write-context ADR composes with ADR 032.
- **Brainstorm 004 (Memory Arenas).** Untouched — no
  new IR carrier; the `Box<ExecutionPlan>` source field
  is the same boxing pattern used by other plan
  variants.

No seam closes.

## What Not To Do

- **Don't widen `ExecutionPlan::Insert` to carry a source
  plan** (D2 — separate IR variants; ADR 030 D4 carry-
  forward).
- **Don't carry a `BoundExprProgram` directly in
  `InsertSelect`** (D2 — InsertSelect carries a bound
  `Box<ExecutionPlan>` source, not an expression
  program).
- **Don't compare source output names to target column
  names** (D3 — positional mapping; source aliases are
  irrelevant after the optional target column list is
  resolved).
- **Don't reject nullable-typed source feeding non-null
  target at bind time** (D9 — runtime null-count
  enforcement only; ADR 032 R4).
- **Don't use `evaluate_try_cast`** in
  `coordinator/write.rs` (D8 / D13 — strict cast only;
  no TRY_CAST semantics).
- **Don't pass outer morsel into the source SELECT**
  (D12 — None argument; preserves ADR 030 VALUES
  behavior).
- **Don't add a streaming `StorageWrite::write` overload**
  (D5 + D6 — single-batch write; multi-batch deferred).
- **Don't return a row count** (D5 — `Ok(Vec::new())`;
  no RETURNING).
- **Don't extend the SELECT descriptor-evolution retry
  loop to InsertSelect** (D1 / out-of-scope — mirrors
  ADR 030 D12).
- **Don't fragment `InsertSelect`** (D1 + D11 —
  distributed reject before `fragment_plan`; walkers
  treat as leaf).
- **Don't make the binder optimizer treat the source as
  a leaf** (D11 — `optimize_for_required` descends into
  the source with `required_output = None` to preserve
  scan-projection pushdown).
- **Don't add new public cast helpers** (D13 — only
  `evaluate_cast` visibility widens; everything else
  stays private).
- **Don't introduce a `LogicalPlan::Insert →
  LogicalPlan::InsertSelect` rewrite pass** (Shape A is
  the IR shape; SQL planner emits the variant directly
  from `Statement::Insert` source body inspection).
- **Don't preserve byte-identical plan-digest output
  across VALUES INSERT → InsertSelect rewrite** — these
  are different statement shapes; no equivalence claim.
- **Don't add `blake3`** to `crates/toron/Cargo.toml`
  (ADR 030 D7 / ADR 031 D-level invariant carry-forward).
- **Don't migrate `ExecutionPlan::Insert` to
  `BoundExprProgram`** (ADR 030 D4 + ADR 031 D10 carry-
  forward).
- **Don't allow `LogicalPlan::InsertSelect` to bind to
  `Insert`** (separate variants; separate binders;
  separate executors).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | VALUES INSERT path silently regresses through `lower_insert` refactor | `LoweredInsertSource::{Values, Select}` keeps the existing VALUES validations byte-identical; existing VALUES architecture guards survive. |
| R2 | Distributed `InsertSelect` accidentally fragments | D1 + 3 independent reject sites (distributed coordinator, fragmenter, executor) + `adr032_distributed_walkers_treat_insert_select_as_leaf` guard. |
| R3 | Binder optimizer fails to descend into source SELECT, killing scan-projection pushdown for the source | D11 explicit exception; behavioral test asserts narrowed scan projection observed. |
| R4 | Source-schema nullability false-rejects legitimate filtered sources | D4 + D9 split: bind time enforces only omitted-not-null; runtime checks `null_count() > 0`. |
| R5 | Outer morsel accidentally partitions DML, silently writing only one morsel's rows | D12 `None` morsel argument; behavioral test asserts full-source write. |
| R6 | Total row cap not enforced; large source crashes writer | D6 total-row cap before coerced-batch construction; rejects with `ExecutionError::InvalidExpression`. |
| R7 | Empty source still calls writer, advancing descriptor metadata | D7 short-circuit before write call. |
| R8 | Strict cast accidentally swapped for try-cast in coordinator helper | D8 + D13 `adr032_coordinator_uses_only_evaluate_cast` guard pins presence + absence. |
| R9 | Source arity check loosens to soft warning | C6-derived `adr032_binder_enforces_source_target_arity` guard source-checks `bind_insert_select` body. |
| R10 | Target column reorder breaks because `target_columns` is dropped or reordered post-binding | D3 carrier explicit in IR; binder unit test asserts ordinals match for explicit column lists. |
| R11 | JOIN source path uncovered by promotion tests | Fixture B required for Definition of Done. |
| R12 | Grouped aggregate source path uncovered by promotion tests | Fixture A required for Definition of Done. |
| R13 | New IR variants silently widen `Box<ExecutionPlan>` reach into unrelated walkers | Distributed / server / scheduler walker leaf-treatment guard. |
| R14 | `Insert` VALUES architecture guards silently weaken via shared helper drift | `resolve_insert_target_columns` extraction shared between `bind_insert` and `bind_insert_select`; existing ADR 030 guards survive on `bind_insert` body. |
| R15 | Plan-digest invariant accidentally re-broken | D10 re-stabilization claim is verified by no SELECT-only plan-digest fixture regressing. |
| R16 | `evaluate_cast` widening leaks `evaluate_try_cast` to coordinator side | D13 limits visibility widening to `evaluate_cast` only; architecture guard pins absence of `evaluate_try_cast` in `coordinator/write.rs`. |

## Recorded dissent

**None at iter-2.**

Iter-0 had a topic-shape question (whether to emit a
single widened `Insert` variant or two separate IR
variants); Codex iter-0 + Claude iter-0 + Gemini iter-0
all converged on Shape A independently. The "Shape B
(widened Insert variant)" alternative was named and
rejected at iter-0 without surviving into iter-1.

Gemini iter-1 did NOT propose broader-scope framings
(e.g., streaming `StorageWrite`, distributed write
context in same ADR) — first composition cycle in the
post-ADR-024 arc where Gemini iter-1/2 did not generate
positions that iter-2/3 had to retract. **Gemini iter-1/2
retraction streak: broken at 6-of-7** after ADR 025 /
026 / 028 / 029 / 030 / 031 ran 6-of-6.

## Process observations

### Fastest composition convergence to date

ADR 032 took 2 substantive iterations + 1 promotion-
drafting pass = **3 total with 0 flip-flops**. Compare:

| ADR | Shape | Iters | Flip-flops |
|---|---|---|---|
| ADR 029 | Substrate-introducing (DDL) | 4 | 0 |
| ADR 030 | Substrate-introducing (DML, cross-ring) | 4 | 1 |
| ADR 031 | Substrate-replacing pure-Toron | 3 + 1 ratification | 0 |
| **ADR 032** | **Composition (pure local)** | **3 total** | **0** |

Three factors drove the speed:

1. **First composition ADR after the substrate arc.**
   ADR 024 → ADR 031 shipped catalog, write trait,
   expression substrate, statement-layer interception,
   and coordinator write routing. ADR 032 composes
   without adding substrate.
2. **Pure-Toron and pure-local.** No Wendelon-side
   discovery pass; no distributed-write-context
   threading; ADR 030 D13 distributed reject pattern
   directly mirrored.
3. **Codex iter-1 ADR absorbed Claude iter-0 C1-C4 +
   Q1-Q4 wholesale.** Claude iter-0 plan-digest re-
   stabilization (C1 → D10), strict-cast (C2 → D8),
   walker leaf-treatment (C3 → D11 with R2 refinement),
   aggregate composition fixture (C4 → Required
   Fixture A) all promoted directly with one refinement
   (R2's binder-optimizer exception). Claude iter-1
   then added three small deltas (C5 / C6 / C7) and
   ratified the rest.

### Codex iter-1 contributed the most distinctive code-evidence reasoning

R1 (target_columns carrier), R2 (binder-may-descend
exception), R4 (runtime null-check vs schema null-
check) are all non-obvious refinements that Claude
iter-0 missed. Healthy collaboration sign — the council
is not single-channel even in fast-converging composition
cycles. Codex iter-1 + Claude iter-1 + Gemini iter-2
each contributed at least one promotion-load-bearing
item.

### Two cycle-property breaks recorded

- **Gemini iter-1/2 retraction streak broken at 6-of-7.**
  First non-substrate cycle in the streak. Updated cycle
  property: broader-scope framings are a substrate-cycle
  phenomenon, not a universal one. Composition cycles
  may not provoke the same tendency.
- **Topic-concession-compresses-next-cycle streak broken
  at 3-of-3.** ADR 020 → 021, 027 → 028, 029 → 031 all
  saw iter-0 alt-topic seeds becoming the next cycle's
  topic. ADR 031 → 032 did not generate a seed pattern
  (ADR 031's "ADR 032 candidates" list was generated
  forward, not as iter-0 dissent). Updated cycle
  property: topic-concession seed pattern is also a
  substrate-cycle phenomenon.

Both breaks are recorded as cycle-property refinements,
not regressions.

### First zero-invariant-break cycle since ADR 028

ADR 029 (additive substrate, zero break), ADR 030
(additive substrate, zero break), ADR 031 (substrate-
replacing, broke plan-digest invariant per D14), ADR 032
(composition, zero break + re-stabilizes the broken
invariant per D10). Pattern: composition cycles are
zero-break by construction.

### Three Claude iter-0 / iter-1 contributions promoted

| Claude contribution | Promoted to |
|---|---|
| iter-0 C1 (plan-digest re-stabilization) | D10 |
| iter-0 C2 (strict-cast ADR 025 kernels) | D8 |
| iter-1 C5 (`evaluate_cast` visibility guard) | D13 + architecture guard |

Plus iter-0 C3 (walker leaf-treatment) was promoted to
D11 with Codex R2 refinement, and iter-0 C4 (aggregate
composition fixture) became Required Fixture A. Iter-1
C6 (source-arity guard wording) and C7 (JOIN fixture
elevation) folded directly into the architecture guard
table and Required Fixture B respectively.

## Future work

ADR 033+ candidates carry forward the ADR 029 → 032
arc:

- **Plan Arena / Flattened Plan IR.** A separate
  substrate ADR candidate once boxed-plan walker
  pressure has enough evidence. This would shift the
  `LogicalPlan` and `ExecutionPlan` recursive trees
  to an index-based IR (parallel to ADR 031's
  expression arena), consolidating the duplicated
  walker logic exposed by ADR 032's traversal-policy
  split.
- **Cross-ring distributed-write-context ADR.** Lifts
  ADR 030 D13 / ADR 032 D1 distributed reject. Threads
  tenant / session / HLC context through
  `DistributedQueryCoordinator`.
- **`CREATE TABLE AS SELECT` (CTAS).** Combines ADR
  029 DDL with ADR 032 read-write composition; one
  statement instead of two.
- **`INSERT ... RETURNING` clause.** Shifts ADR 030
  D17 / ADR 032 `Ok(Vec::new())` contract to emit row
  counts and/or projected columns.
- **`UPDATE` / `DELETE`.** DML siblings to ADR 030 /
  032; `__row_tombstone__` becomes load-bearing for
  DELETE; Wendelon-side already owns tombstone
  stamping.
- **`ON CONFLICT` / `UPSERT` / `MERGE`.** DML
  extensions; need PK / unique-constraint substrate
  first.
- **Streaming / multi-batch `StorageWrite::write`.**
  ADR 032 D5 + D6 carry-forward; future ADR lifts the
  single-batch cap for bulk loads.
- **Write retries after descriptor evolution.** ADR
  030 D12 + ADR 032 carry-forward.
- **Read-your-writes session HLC advancement.** ADR
  030 / 032 still discard `commit_hlc`.
- **Per-statement `durability` override.** ADR 030
  carry-forward.
- **Cross-ring metadata-write contract ADR.** Needed
  to lift `WendelonCatalog::create_table → AdminUnavailable`.
- **Typed expression tape / opcode interpreter.**
  Brainstorm 004's deeper realization over ADR 031's
  `BoundExprProgram`.
- **Common subexpression elimination (CSE) + literal
  deduplication.** ADR 031 D8 carry-forward.
- **Cranelift JIT compilation.** Brainstorm 001
  deeper realization over `BoundExprProgram`.
- **Logical `Expr` arena migration.** Brainstorm 004
  broader framing.
- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023 reject.
- **HAVING alias visibility ADR.** Loosens ADR 023
  reject.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **PBC partial-pushdown of computed-arg aggregates.**
  Cross-ring; workload-evidence-pressured.
- **Wendelon AGG consumer wiring.** Cross-ring; PBC
  partial-pushdown.
- **Temporal types (DATE, TIMESTAMP)** for ADR 029
  type lattice + ADR 030 / 032 coercion lattice.
- **DECIMAL** for ADR 029 type lattice + ADR 030 /
  032 coercion lattice.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).
- **Constraints** (PRIMARY KEY / FOREIGN KEY / CHECK
  / column-level UNIQUE) / DEFAULT / GENERATED
  columns.
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW`.** DDL siblings.
- **Richer Toron `Hlc` type** for read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**

## Definition of Done

- [ ] `LogicalPlan::InsertSelect { table, columns,
      source: Box<LogicalPlan> }` defined.
- [ ] `ExecutionPlan::InsertSelect { legacy_table_id,
      table, fingerprint, schema, target_columns:
      Vec<ColumnOrdinal>, source: Box<ExecutionPlan> }`
      defined.
- [ ] `LogicalPlan` variant count is 12;
      `ExecutionPlan` variant count is 10.
- [ ] `ExecutionPlan::Insert` still carries
      `rows: Vec<Vec<ScalarValue>>` with no
      `BoundExprProgram` / `PbcProgram`.
- [ ] `SqlPlanner::lower_insert` routes non-VALUES
      query bodies through `lower_query` via a private
      `LoweredInsertSource::{Values, Select}` enum
      without weakening any VALUES-only validation.
- [ ] `Binder::bind_insert_select` resolves target
      table, calls `resolve_insert_target_columns`,
      binds + optimizes source, validates arity and
      type admissibility, emits
      `ExecutionPlan::InsertSelect`.
- [ ] `resolve_insert_target_columns(schema, columns)`
      helper shared between `bind_insert` and
      `bind_insert_select`.
- [ ] `crate::exec::evaluate_cast` is `pub(crate)`;
      `evaluate_try_cast` and lower-level cast kernels
      stay private.
- [ ] `coordinator/write.rs::materialize_insert_select_batch(target_schema,
      target_columns, source_batches)` returns
      `Ok(None)` for zero rows, `Ok(Some(batch))` for
      coerced single batch, `Err(...)` for over-cap /
      coercion failure / non-null violation.
- [ ] `QueryCoordinator::execute_query` intercepts
      `LogicalPlan::InsertSelect` next to
      `LogicalPlan::Insert`, before the SELECT retry
      loop.
- [ ] `QueryCoordinator::execute_write_plan` executes
      source via `Executor::execute_bound(source, None)`
      (never passes outer morsel) and calls
      `StorageWrite::write` at most once.
- [ ] `DistributedQueryCoordinator::execute_query`
      rejects `ExecutionPlan::InsertSelect` before
      `fragment_plan` with the D1 diagnostic.
- [ ] `FragmentError::InsertSelectNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges`.
- [ ] Distributed / server / scheduler metadata
      walkers treat `InsertSelect` as a leaf; binder
      `optimize_for_required` is the documented
      exception.
- [ ] Telemetry labels for distributed / server /
      scheduler walker `InsertSelect` arms use
      `"insert_select"`.
- [ ] Total source row count over `TABLE_WRITE_MAX_ROWS`
      fails before write with stable diagnostic.
- [ ] Empty source returns `Ok(Vec::new())` without
      calling the writer.
- [ ] Nullable source values into a non-null target
      fail before write with column name in diagnostic.
- [ ] Strict numeric cast overflow fails before write
      (no TRY_CAST semantics in `coordinator/write.rs`).
- [ ] Explicit `INSERT INTO dst (b, a) SELECT x, y
      FROM src` reorders columns correctly per
      `target_columns`.
- [ ] Omitted nullable target columns are filled with
      typed NULL.
- [ ] Omitted non-null target columns fail at bind
      time.
- [ ] Source output column names are ignored; only
      position matters.
- [ ] Source SELECT scan projection narrows when
      `optimize_for_required` descends with
      `required_output = None`.
- [ ] **Fixture A (grouped aggregate source) passes
      end-to-end.**
- [ ] **Fixture B (INNER JOIN source) passes end-to-
      end.**
- [ ] 12 ADR-032-specific architecture guards pass.
- [ ] 2 composition guards verifying ADR 030 + ADR
      031 invariants survive ADR 032 pass.
- [ ] Existing VALUES INSERT tests remain green; no
      VALUES architecture guard adjusted.
- [ ] Existing SELECT-only plan-digest fixtures
      remain green (D10 re-stabilization).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-free
      preserved.
- [ ] ADR 031 `BoundExprProgram` `PartialEq`-only
      preserved.
- [ ] `StorageWrite` trait shape unchanged; single
      `RecordBatch` payload.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added (`bumpalo` /
      `typed-arena` / `slab` / `generational-arena`).
- [ ] Roadmap moves ADR 032 entry from "candidate" to
      "promoted".
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
