# ADR 033: SQL `CREATE TABLE AS SELECT` — Local DDL + DML Composition

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached in three iterations — fastest cycle in
council history alongside ADR 032. **Iter-0 produced
three-way independent topic convergence on CTAS** (first
in the council's history); no topic dissent. Iter-1
settled the load-bearing shape decisions: separate IR
variants (Shape A) instead of widening `CreateTable` or
rewriting to `CreateTable + InsertSelect`; bound CTAS
carries no pre-create table identity (no `TableId`,
`TableObjectId`, or `PhysicalDescriptorFingerprint`);
source binds before catalog mutation; coordinator creates
table before source execution (fails fast on table-exists
/ admin-unavailable); narrow v1 SQL surface (no column
list, no `IF NOT EXISTS`); schema inferred from bound
source output with ADR 029 lattice validation; empty-
source still creates queryable table via zero-row write
through a new sibling helper; ADR 032
`materialize_insert_select_batch` contract preserved
verbatim; local-coordinator-only with three defensive
distributed rejects. Iter-2 was promotion-drafting:
ratified Codex iter-1's R1-R3 precision items + 18-guard
architecture set + `ctas_test.rs` separate-file
recommendation; folded Claude iter-1 deltas (C1 sibling
helper shape, C2 ordering pins, C3 synthetic-name reject
site with Codex R2 strict no-forked-literals discipline,
C4 three required composition fixtures) + Claude iter-2
A1 strict-pin guard wording.

**Second composition ADR** after ADR 032. Continues the
"composition cycles are zero-break by construction"
pattern: zero new substrate, zero new architecture-guard
invariants broken, ADR 032 D10 SELECT plan-digest
stability re-stabilized for the post-ADR-031 baseline.
**First three-way independent iter-0 topic convergence
in the council's history.** **Second consecutive cycle
with broken Gemini iter-1/2 retraction streak** (now
6-of-8): Gemini iter-1 absorbed Codex iter-0 wholesale
on the stricter v1 surface and proposed no broader-scope
framings. Updated cycle property after two composition-
cycle data points: **broader-scope framings are a
substrate-cycle phenomenon, not a universal cycle
phenomenon.** **Second consecutive cycle with broken
topic-concession-compresses-next-cycle pattern** (now
3-of-5): ADR 032 → 033 generated no iter-0 alt-topic
seed; the seed pattern is also substrate-cycle-specific.

**Sixth consecutive cycle of the Codex correctness-
clarification pattern.** Codex iter-0 contributed seven
R-items (R1 no pre-create identity, R2 bind source before
catalog mutation, R3 no v1 column list, R4 no v1 `IF NOT
EXISTS`, R5 schema validation with name/type lattice, R6
empty source must initialize queryable table, R7 non-
atomicity acceptable if named); Codex iter-1 added three
more (R1 create-before-source-execution intentional, R2
Wendelon constants reused without forked literals, R3 no
coercion in CTAS); each surfaced from
code-evidence audit of the catalog / binder / coordinator
boundary.

**First cycle where Claude iter-0 lead defaults were
retracted four-for-four by iter-1.** My iter-0 picked
the right topic (CTAS) but four wrong v1-surface defaults
(D3 column rename list, D5 all-nullable, D7 atomicity
wording, D8 empty-source no-write). Codex iter-0 caught
the code-evidence constraints I missed. Recorded as cycle
property: composition cycles over substrate-rich
foundations (ADR 029 / 030 / 031 / 032 stack) have many
subtle code-evidence constraints that iter-0 design
sketches can miss; iter-1 code-evidence audits become
more load-bearing.

Fifteenth instance of the ADR 015 cycle (after 016 /
017 / 018 / 019 / 022 / 023 / 024 / 025 / 026 / 027 /
028 / 029 / 030 / 031 / 032).

## Scale boundary

**Primary:** Toron SQL planner + binder + local
`QueryCoordinator` + existing `Catalog::create_table` +
existing `StorageWrite` boundary. Surface widens the
statement layer in `SqlPlanner::lower_create_table` to
handle `CreateTable` with a non-empty `query` body
(routes to new `lower_create_table_as_select`); adds
`LogicalPlan::CreateTableAsSelect` +
`ExecutionPlan::CreateTableAsSelect` variants; adds one
new private helper `materialize_empty_record_batch` in
`coordinator/write.rs` (sibling to ADR 032's
`materialize_insert_select_batch`); adds a CTAS arm to
`QueryCoordinator::execute_write_plan` that calls
catalog → executor → materialization helper → writer in
that fixed order.

In scope:

- `SqlPlanner::lower_create_table` routes
  `CreateTable.query.is_some()` to new
  `lower_create_table_as_select`. The exhaustive
  `sqlparser::ast::CreateTable` destructure (no `..`)
  from ADR 029 D2 is preserved verbatim.
- New `LogicalPlan::CreateTableAsSelect { table:
  TableRef, source: Box<LogicalPlan> }` variant. No
  `columns` field (R3 rejects non-empty column lists
  at the planner; the logical IR doesn't carry what
  cannot exist).
- New `ExecutionPlan::CreateTableAsSelect { table:
  TableRef, schema: SchemaRef, source: Box<ExecutionPlan> }`
  variant. **No `TableId`, `TableObjectId`, or
  `PhysicalDescriptorFingerprint` fields** — the bound
  CTAS plan carries only metadata known before catalog
  mutation (Codex iter-0 R1).
- New `Binder::bind_create_table_as_select` that binds
  source SELECT first, then infers + validates target
  schema, then emits the bound plan. No catalog
  mutation from the binder.
- New binder helpers: `infer_ctas_schema(source_schema)`,
  `validate_ctas_column_name(name)`,
  `is_supported_ctas_column_type(data_type)`. Schema-
  inference site enforces ADR 029 lattice (six types) +
  duplicate-name reject + Wendelon storage synthetic-
  name reject.
- Wendelon synthetic name constants reused directly
  in `binder.rs` or through a neutral Toron helper;
  **no forked `__row_*__` string literals** (Codex
  iter-1 R2).
- New `materialize_empty_record_batch(schema:
  &SchemaRef) -> Result<RecordBatch, ExecutionError>`
  private helper in `coordinator/write.rs`. Reuses the
  existing `typed_null_array(_, 0, _)` helper for empty
  typed array construction.
- ADR 032's `materialize_insert_select_batch` contract
  **unchanged** — it still returns `Ok(None)` for zero
  rows; the CTAS branch handles the `None` arm by
  calling `materialize_empty_record_batch` (Claude
  iter-1 C1).
- `QueryCoordinator::execute_query` intercepts
  `LogicalPlan::CreateTableAsSelect` next to existing
  `Insert` / `InsertSelect` arms, **before** the
  SELECT descriptor-evolution retry loop.
- `QueryCoordinator::execute_write_plan` CTAS arm
  performs `catalog.create_table` → `execute_bound(source,
  None)` → `materialize_insert_select_batch` (with
  identity ordinals) → `materialize_empty_record_batch`
  if helper returned `Ok(None)` → single
  `StorageWrite::write` → `Ok(Vec::new())`.
- `DistributedQueryCoordinator::execute_query` rejects
  `ExecutionPlan::CreateTableAsSelect` before
  `fragment_plan` with stable diagnostic `"distributed
  CREATE TABLE AS SELECT execution is not wired to a
  write-context-bearing coordinator"`.
- New `FragmentError::CreateTableAsSelectNotFragmentable`
  variant; fragmenter rejects before `contains_join` /
  `count_exchanges`.
- Distributed / scheduler / server metadata walkers
  treat `CreateTableAsSelect` as a leaf (join detection
  returns false; exchange counting returns zero; scan
  metadata / fingerprint lookups return none; replacement
  walkers leave the variant unchanged). Binder
  optimization is the one intentional exception:
  `optimize_for_required` recurses into the source with
  `required_output = None` so existing scan-projection
  pushdown for the source SELECT survives (mirrors
  ADR 032 D11).
- Defensive `Executor::execute_bound` reject for any
  uncoordinated `ExecutionPlan::CreateTableAsSelect`
  reach; defensive `LogicalPlan::CreateTableAsSelect`
  reject for unbound execution.
- Telemetry labels for distributed / server / scheduler
  walker `CreateTableAsSelect` arms use
  `"create_table_as_select"` (distinct from existing
  `"catalog"` for plain `CreateTable` and
  `"insert_select"` for InsertSelect; preserves
  telemetry distinguishability).
- 18 architecture guards + 1 paired adjustment to the
  existing `architecture_guards_unchanged_plus_sort_variant`
  variant-count catch-all guard.
- New `crates/toron/tests/ctas_test.rs` test module
  with four required end-to-end fixtures (Codex iter-1
  R-fixtures + Claude iter-1 C4) plus the smaller
  reject coverage.

Out of scope:

- CTAS column rename lists (`CREATE TABLE t (a, b) AS
  SELECT x, y FROM src`). Users rename through SELECT
  aliases: `CREATE TABLE t AS SELECT x AS a, y AS b
  FROM src`. Rejected at planner with stable
  diagnostic (Codex iter-0 R3).
- CTAS explicit type declarations (`CREATE TABLE t (a
  INT, b BIGINT) AS SELECT ...`). Mixing inference
  with explicit types muddles ADR 029's DDL surface.
- `IF NOT EXISTS` for CTAS. `Catalog::create_table`
  returns a `TableBinding`, not a "created vs already
  existed" outcome — supporting `IF NOT EXISTS` could
  accidentally append SELECT rows into an existing
  table. Rejected at planner (Codex iter-0 R4). A
  future ADR may add a `CreateTableOutcome { binding,
  created }` catalog shape.
- `OR REPLACE` for CTAS. Needs `DROP TABLE` support
  first.
- `CREATE TABLE ... LIKE` and `CREATE TABLE ... CLONE`
  for CTAS. Different inference semantics.
- Catalog rollback after write failure. CTAS is non-
  atomic by design (Codex iter-0 R7): if
  `catalog.create_table` succeeds but the subsequent
  write fails, the catalog entry remains. A future
  cross-ring metadata-write contract ADR can engage
  transactional semantics.
- Distributed CTAS execution. Defers to the same future
  cross-ring distributed-write-context ADR that lifts
  ADR 030 D13 + ADR 032 D1.
- Source-to-target type coercion. The target schema is
  inferred from the source schema by construction, so
  source and target types are identical (Codex iter-1
  R3). Users who want type adjustments spell them in
  the SELECT (`CREATE TABLE dst AS SELECT CAST(x AS
  BIGINT) AS x FROM src`); CTAS itself never injects
  casts. The `evaluate_cast` `pub(crate)` visibility
  ADR 032 D13 introduced is not even consumed by the
  CTAS path.
- Streaming / multi-batch `StorageWrite`. CTAS reuses
  the ADR 032 single-batch contract.
- Write retries after descriptor evolution. CTAS does
  not enter the SELECT descriptor-evolution retry loop
  (mirrors ADR 030 D12 + ADR 032 carry-forward).
- Read-your-writes session HLC advancement. CTAS still
  discards `commit_hlc`.
- New Wendelon API surface; no new wrapper or receipt
  field consumed.
- New PBC opcode or typed expression tape; no new
  `BoundExprProgram` variant.
- Plan Arena / index-based plan IR (`bumpalo` /
  `typed-arena` / `slab` / `generational-arena`). Plan
  arena debt is real but not yet workload-pressured.
- New aggregate kernel.
- Fragmenter detector change for SELECT paths beyond
  the leaf-treatment shims.

## Context

After ADR 029 (CREATE TABLE), ADR 030 (INSERT VALUES),
ADR 031 (`BoundExprProgram` substrate), and ADR 032
(INSERT INTO ... SELECT), the local read-write composition
arc has one obvious next step: `CREATE TABLE name AS
<query>`. Every analyst pipeline that uses Toron will
write CTAS as the standard idiom for materializing roll-
ups; without CTAS, users write two statements (`CREATE
TABLE x (cols...); INSERT INTO x SELECT ...`) and must
hand-type the schema the SELECT already knows.

The load-bearing decisions are: **how the bound CTAS
plan carries (or doesn't carry) target table identity**
(R1: doesn't, because binding cannot mutate catalog),
**the ordering of source binding vs catalog mutation vs
source execution** (R2: bind source → create table →
execute source → materialize → write — fails fast on
catalog errors before source SELECT work), **what to do
with the inferred schema's nullability** (D4: verbatim
from source), **what SQL surface CTAS accepts in v1**
(R3 + R4: bare `CREATE TABLE name AS <query>` only — no
column list, no `IF NOT EXISTS`), **how to handle the
empty-source case** (R6: zero-row write to initialize
storage so the table is queryable; not the same as ADR
032 InsertSelect's empty-source-no-op), **whether to
modify ADR 032's helper or add a sibling** (Claude C1 +
Codex R6: sibling helper; preserves ADR 032 D7
contract), **whether to do any type coercion at the CTAS
layer** (R3: no — source schema IS target schema by
construction), and **where Wendelon synthetic name
literals live** (Codex iter-1 R2: imported as constants;
no forked literals).

The council converged in three iterations with zero
flip-flops. Iter-0 produced three-way independent
topic convergence (Claude / Codex / Gemini all
independently picked CTAS as lead). Iter-1 settled the
load-bearing shape via Codex's seven iter-0 R-items
absorbed wholesale by Claude/Gemini iter-1, plus three
additional Codex iter-1 R-items (create-before-source-
execution intentional, Wendelon constants imported
directly, no coercion in CTAS). Iter-2 was promotion-
drafting.

## Decision

### D1 — Topic: local CTAS

ADR 033 implements local `CREATE TABLE name AS <query>`.
Local `QueryCoordinator::execute_query` intercepts
`LogicalPlan::CreateTableAsSelect` next to existing
`Insert` / `InsertSelect` arms, before the SELECT
descriptor-evolution retry loop. Distributed
`DistributedQueryCoordinator::execute_query` rejects
`ExecutionPlan::CreateTableAsSelect` before
`fragment_plan` with stable diagnostic. Fragmenter has
defensive `FragmentError::CreateTableAsSelectNotFragmentable`
reject before `contains_join` / `count_exchanges`.
Executor has defensive `NotImplemented` arms for both
unbound and bound CTAS.

This is the second composition ADR after ADR 032. It
continues the local read-write arc without adding plan
arena, typed expression tape, streaming write API,
distributed write context, Wendelon admin API, or HLC
receipt consumption.

### D2 — Separate IR variants (Shape A)

CTAS gets its own logical and execution plan variants;
the existing `CreateTable` (ADR 029) and `InsertSelect`
(ADR 032) variants stay unchanged.

```rust
LogicalPlan::CreateTableAsSelect {
    table: TableRef,
    source: Box<LogicalPlan>,
}

ExecutionPlan::CreateTableAsSelect {
    table: TableRef,
    schema: SchemaRef,
    source: Box<ExecutionPlan>,
}
```

The execution variant deliberately does **not** carry
`TableId`, `TableObjectId`, or
`PhysicalDescriptorFingerprint`. Binding must not mutate
the catalog, and the target table does not exist until
the coordinator calls `Catalog::create_table`. The
coordinator obtains the `TableBinding` from the create
call and uses it to populate `WriteRequest`.

`LogicalPlan` variant count: 12 → 13.
`ExecutionPlan` variant count: 10 → 11.

### D3 — Narrow SQL surface (Codex iter-0 R3 + R4)

Supported v1 surface:

```sql
CREATE TABLE table_name AS <query>
```

Rejected in v1 with stable diagnostics:

- non-empty `CreateTable.columns` (CTAS column rename
  list);
- `if_not_exists == true`;
- `OR REPLACE`;
- `LIKE` / `CLONE`;
- table constraints, column definitions, options,
  storage clauses, comments, engine clauses,
  partitioning, tags, and other ADR 029 unsupported
  CREATE TABLE clauses (the exhaustive `CreateTable`
  destructure from ADR 029 D2 still owns these
  rejects).

Column renaming uses SELECT aliases:

```sql
CREATE TABLE dst AS SELECT x AS a, y AS b FROM src;
```

This avoids a confusing "column definitions mean
aliases only under CTAS" rule and preserves ADR 029's
DDL semantics.

### D4 — Schema inference from bound source output

The target table schema is inferred from the bound
source SELECT's output schema:

- output names are inherited from the source schema;
- data types are inherited from the source schema;
- nullability is inherited from the source schema.

The binder validates the inferred schema before
emitting the execution plan:

- duplicate output names reject;
- empty output names reject if reachable;
- Wendelon storage synthetic column names reject
  (`__row_hlc__` / `__row_key_id__` /
  `__row_routing_locality__` / `__row_table_object_id__`
  / `__row_tombstone__`);
- data types outside the ADR 029 table lattice reject
  (`Boolean`, `Int32`, `Int64`, `Float32`, `Float64`,
  `Utf8`).

Validation belongs in the binder, not the coordinator.
A bad source schema should never create a catalog table.

### D5 — Bind source before catalog mutation (Codex iter-0 R2)

The source SELECT must bind before any catalog
mutation. This protects obvious errors:

```sql
CREATE TABLE dst AS SELECT * FROM missing;
```

must fail without creating `dst`.

Binding source first does **not** mean executing source
first. The coordinator still performs `create_table`
before source execution to fail fast on table-exists /
admin-unavailable catalog errors before spending work
on the SELECT (Codex iter-1 R1).

The load-bearing safety invariant is source **binding**
before catalog mutation, not source **execution**
before catalog mutation. Binding is cheap; execution
can be expensive.

### D6 — Coordinator order: create → execute → materialize → write (Codex iter-1 R1)

Coordinator sequence:

1. Receive already-bound `ExecutionPlan::CreateTableAsSelect`.
2. Call `catalog.create_table(table, schema.clone(),
   false)` — `if_not_exists = false` always; CTAS
   never inherits the ADR 029 D14 surface.
3. Receive `TableBinding` (with `local_table_id`,
   `table`, `fingerprint`).
4. Execute the bound source via
   `Executor::execute_bound(source, None)` (mirrors
   ADR 030 / 032 D12 outer-morsel-`None` discipline).
5. Materialize source batches via
   `write::materialize_insert_select_batch(schema,
   &identity_target_columns, &source_batches)`
   (ADR 032 helper, identity ordinals, no coercion).
6. If helper returned `Ok(None)`, call
   `write::materialize_empty_record_batch(schema)` to
   produce a zero-row payload (Claude iter-1 C1 +
   Codex iter-0 R6).
7. Call `self.writer.write(WriteRequest { ...
   binding.local_table_id, binding.table,
   binding.fingerprint, payload ... })` once.
8. Return `Ok(Vec::new())` (no row count, no
   RETURNING).

The `create_table` call intentionally happens before
source execution. The source was already bound (so
missing source tables and invalid source expressions
already failed before catalog mutation per D5), and
table-exists / admin-unavailable failures should also
fail before source execution cost.

### D7 — Non-atomic catalog-then-write (Codex iter-0 R7)

CTAS is non-atomic. If `create_table` succeeds and a
later source execution or write fails, the catalog
entry remains. That is an explicit v1 limitation, not
something hidden behind partial rollback.

Rollback would need:

- `DROP TABLE` support in the catalog (future ADR);
- or a transactional metadata-write contract (future
  cross-ring ADR);
- or a richer cross-ring admin API (future cross-ring
  ADR).

None belongs in ADR 033.

### D8 — Empty source initializes queryable storage (Codex iter-0 R6 + Claude iter-1 C1)

ADR 032 `INSERT INTO ... SELECT` treats empty source as
a writer no-op (`materialize_insert_select_batch`
returns `Ok(None)` for zero rows; the coordinator
short-circuits to `Ok(Vec::new())` without calling the
writer). CTAS cannot do that: a created table must be
queryable.

CTAS preserves ADR 032's helper contract verbatim:

```rust
materialize_insert_select_batch(...) -> Ok(None)
```

for zero rows. The CTAS coordinator branch handles
`None` by calling a new sibling helper:

```rust
pub(super) fn materialize_empty_record_batch(
    schema: &SchemaRef,
) -> Result<RecordBatch, ExecutionError>
```

that builds a zero-row `RecordBatch` with the inferred
schema. The CTAS branch then writes the zero-row batch
through the normal `StorageWrite::write` path. This
preserves InsertSelect's empty-source no-op while
giving CTAS the different storage-initialization
semantics it needs. No modification of the ADR 032
helper.

The sibling helper reuses ADR 032's existing private
`typed_null_array(data_type, 0, column)` helper for
empty typed array construction — no new builder code
paths.

### D9 — Wendelon synthetic constants reused without forked literals (Codex iter-1 R2)

The synthetic column name reject site is the binder's
schema-inference path. The binder must either:

- import the five Wendelon synthetic constants
  directly in `binder.rs`; or
- factor a small Toron-private helper into a neutral
  module that re-exports them.

Forking the five `__row_*__` string literals across
modules (e.g., copying them from
`sql_planner.rs`'s private
`STORAGE_SYNTHETIC_COLUMN_NAMES` slice) is forbidden:
it creates silent-drift risk between two enforcement
sites. Architecture guard pins absence of forked
literals in `binder.rs` (negative-pattern enforcement
per Claude iter-2 A1).

### D10 — No coercion in CTAS (Codex iter-1 R3)

Because the target schema is inferred from the source
schema by construction (D4), source-to-target type
coercion is structurally impossible — source and target
types are identical. CTAS passes identity
`ColumnOrdinal` target columns to
`materialize_insert_select_batch` and gets:

- shared `TABLE_WRITE_MAX_ROWS` cap;
- batch concatenation;
- single `RecordBatch` write API;
- defensive non-null validation (always succeeds in
  CTAS because nullability is inherited verbatim).

Users who want type adjustments spell them in the
SELECT:

```sql
CREATE TABLE dst AS SELECT CAST(x AS BIGINT) AS x FROM src;
```

CTAS itself never injects casts. The `evaluate_cast`
`pub(crate)` visibility ADR 032 D13 introduced is not
even consumed by the CTAS path.

### D11 — Local coordinator only

CTAS follows ADR 029 / 030 / 032:

- local `QueryCoordinator` intercepts before the SELECT
  retry loop;
- distributed coordinator rejects before `fragment_plan`
  with stable diagnostic `"distributed CREATE TABLE AS
  SELECT execution is not wired to a write-context-
  bearing coordinator"` (parallels ADR 030 D13 + ADR
  032 D1 wording but disambiguates the DML form);
- fragmenter rejects via new
  `FragmentError::CreateTableAsSelectNotFragmentable`
  before join / exchange analysis;
- executor rejects uncoordinated CTAS execution with
  `NotImplemented`.

All four reject sites are independent guards (planner
reaches `LogicalPlan::CreateTableAsSelect` only via
`lower_create_table_as_select`; binder reaches
`ExecutionPlan::CreateTableAsSelect` only via
`bind_create_table_as_select`; local coordinator
intercepts before retry; distributed coordinator and
fragmenter reject before fragmentation). None silently
accept; none silently drop.

Distributed CTAS is future cross-ring distributed-
write-context work.

### D12 — Binder optimizer may descend; distributed walkers treat CTAS as leaf

Mirrors ADR 032 D11. The "CTAS source is a leaf" rule
applies to distributed-planning walkers (fragmenter /
scheduler / server metadata helpers), where
`CreateTableAsSelect` is already rejected as
coordinator-only DML. They return:

- join detection: false / none;
- exchange counting: zero;
- scan metadata / fingerprint lookup: none;
- replacement walkers: leave the plan unchanged.

The binder's `optimize_for_required` is the one
intentional exception. It recurses into the source
with `required_output = None` so existing ADR 013 /
014 runtime-filter and scan-projection pushdown
optimization for the source SELECT survive.

### D13 — Outer morsels do not partition CTAS source (ADR 030 / 032 carry-forward)

The CTAS source SELECT executes with `None` morsel
argument inside the coordinator path. Mirrors ADR 030
VALUES INSERT and ADR 032 InsertSelect carry-forward
discipline. A top-level write statement is not a scan-
morsel-partitioned operator; treating it as one would
silently write only one morsel's worth of rows.

### D14 — SELECT plan stability resumes (ADR 032 D10 carry-forward)

ADR 033 is additive over the post-ADR-032 SELECT plan-
digest baseline. Existing SELECT-only plan shapes and
plan digests remain byte-identical pre/post-ADR-033
because the new `CreateTableAsSelect` variants only
appear under `CREATE TABLE ... AS SELECT` statements.
**Composition cycles are zero-break by construction**
— second data point (ADR 032 + ADR 033).

## Reject removal (ADR 015 cycle's fifteenth instance)

ADR 033 lifts one user-facing reject and adds five new
rejects:

**Lifted:** `SqlPlanner::lower_create_table` previously
rejected `CREATE TABLE` statements with non-`None`
`query` field with diagnostic `"unsupported CREATE
TABLE clause: AS SELECT (CTAS)"`. The new
`lower_create_table_as_select` routes non-`None` queries
to CTAS lowering instead of returning the unsupported
diagnostic.

**Added (planner, lower time):**

- non-empty `CreateTable.columns` with CTAS (D3 / R3)
  rejects with stable diagnostic directing users to
  SELECT aliases;
- `if_not_exists == true` with CTAS (D3 / R4) rejects
  with stable diagnostic.

**Added (binder, bind time):**

- duplicate output names in inferred schema (D4 / R5);
- Wendelon storage synthetic names in inferred schema
  (D4 / R5 / R2 / D9);
- source output types outside ADR 029 lattice (D4 / R5).

**Added (distributed coordinator, before fragmentation):**

- `ExecutionPlan::CreateTableAsSelect` rejected as
  coordinator-only DML (D11).

**Added (fragmenter, before join / exchange analysis):**

- `FragmentError::CreateTableAsSelectNotFragmentable`
  (D11).

Existing CREATE TABLE rejects (constraints, storage
options, `OR REPLACE`, etc.) remain unchanged. No
reject is silently widened or narrowed beyond the
explicit lift.

## Architecture Guard Set

**18 ADR-033-specific guards + 1 paired adjustment to
the existing variant-count catch-all = 19 architecture
guard touchpoints.**

| Guard | Pins | File anchor |
|---|---|---|
| `adr033_logical_variant_count_is_13` | `LogicalPlan` 12 → 13 growth | `ir.rs` |
| `adr033_execution_variant_count_is_11` | `ExecutionPlan` 10 → 11 growth | `ir/execution.rs` |
| `adr033_execution_ctas_has_no_precreate_identity` | `ExecutionPlan::CreateTableAsSelect` body has `table`, `schema`, `source`, and absence of `TableId` / `TableObjectId` / `PhysicalDescriptorFingerprint` | `ir/execution.rs` |
| `adr033_planner_routes_only_query_create_table_to_ctas` | `lower_create_table` routes `query.is_some()` to `lower_create_table_as_select`; plain CREATE TABLE stays on ADR 029 path | `sql_planner.rs` |
| `adr033_planner_rejects_ctas_column_list_and_if_not_exists` | `lower_create_table_as_select` rejects non-empty `columns` + `if_not_exists` with stable diagnostics | `sql_planner.rs` |
| `adr033_binder_binds_source_before_inferring_schema` | Byte-position comparison: `self.bind_plan(source)` call precedes `source.schema()` reads in `bind_create_table_as_select` body | `binder.rs` |
| `adr033_schema_inference_rejects_synthetic_names` | Binder rejects Wendelon storage synthetic names; **absence of forked `__row_*__` literals** (Codex iter-1 R2 + Claude iter-2 A1 strict pin) | `binder.rs` |
| `adr033_schema_inference_rejects_duplicate_names` | Duplicate output names reject before catalog mutation | `binder.rs` |
| `adr033_schema_inference_uses_adr029_type_lattice` | Only six table data types admitted (`Boolean` / `Int32` / `Int64` / `Float32` / `Float64` / `Utf8`) | `binder.rs` |
| `adr033_local_coordinator_intercepts_before_retry` | Local CTAS intercept precedes SELECT retry loop | `coordinator.rs` |
| `adr033_coordinator_creates_table_before_source_execution` | Byte-position comparison: `catalog.create_table` call precedes `executor.execute_bound` call in CTAS arm | `coordinator.rs` |
| `adr033_coordinator_uses_create_table_binding_for_write` | `WriteRequest` table identity comes from `create_table`'s returned `TableBinding`, not from binder-side fields | `coordinator.rs` |
| `adr033_empty_source_ctas_writes_zero_row_batch` | CTAS handles `materialize_insert_select_batch == None` by calling `materialize_empty_record_batch` + writing zero-row batch | `coordinator.rs` |
| `adr033_distributed_coordinator_rejects_before_fragment` | Distributed CTAS reject precedes `fragment_plan` | `coordinator.rs` |
| `adr033_fragmenter_rejects_before_join_or_exchange` | `CreateTableAsSelectNotFragmentable` reject precedes `contains_join` / `count_exchanges` | `distributed/fragment.rs` |
| `adr033_distributed_walkers_treat_ctas_as_leaf` | Distributed / scheduler / server metadata walkers return leaf for CTAS | `distributed/*`, `server/mod.rs` |
| `adr033_does_not_modify_insert_select_empty_contract` | `materialize_insert_select_batch` still returns `Ok(None)` for zero rows (ADR 032 D7 invariant survives ADR 033 — Claude iter-1 C1 verification) | `coordinator/write.rs` |
| `adr033_does_not_add_plan_arena_dependency` | No `bumpalo` / `typed-arena` / `slab` / `generational-arena` added | `Cargo.toml` |

Paired adjustment to existing guard:

| Existing guard | Adjustment |
|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | `LogicalPlan` count assert bumps 12 → 13 with `CreateTableAsSelect` content assert; `ExecutionPlan` count assert bumps 10 → 11 with `CreateTableAsSelect` content assert |

Each guard's diagnostic string names ADR 033 + the
specific decision or refinement it enforces (per the
ADR 015 cycle narrow-detector pattern).

## Required composition fixtures

Four end-to-end fixtures required for promotion. All must
pass before ADR 033 lands. Live in
`crates/toron/tests/ctas_test.rs`.

### Fixture A — Grouped aggregate source

```sql
CREATE TABLE src (id INT NOT NULL, amount BIGINT NOT NULL);
INSERT INTO src VALUES (1, 100), (1, 200), (2, 50);
CREATE TABLE totals AS
  SELECT id, SUM(amount) AS total FROM src GROUP BY id;
SELECT id, total FROM totals ORDER BY id;
-- Expected: (1, 300), (2, 50)
```

Exercises ADR 028 grouped-aggregate signature + ADR
031 `BoundExprProgram` aggregate evaluation + ADR
033 schema inference (target schema has `id INT
NULL, total BIGINT NULL` — types and nullability
from source SELECT output verbatim).

### Fixture B — INNER JOIN source

```sql
CREATE TABLE left_table (id INT NOT NULL, label TEXT);
CREATE TABLE right_table (left_id INT NOT NULL, score BIGINT NOT NULL);
INSERT INTO left_table VALUES (1, 'a'), (2, 'b');
INSERT INTO right_table VALUES (1, 100), (2, 200);
CREATE TABLE joined AS
  SELECT id, label, score
  FROM left_table
  INNER JOIN right_table ON left_table.id = right_table.left_id;
SELECT id, label, score FROM joined ORDER BY id;
-- Expected: (1, 'a', 100), (2, 'b', 200)
```

Exercises ADR 008 / 009 join substrate + ADR 033
schema inference across joined output.

### Fixture C — Empty-source initialization (Codex iter-0 R6 + Claude iter-1 C1)

```sql
CREATE TABLE src (id INT NOT NULL);
CREATE TABLE empty_copy AS SELECT id FROM src WHERE id > 100;
SELECT id FROM empty_copy;
-- Expected: empty result set; query MUST SUCCEED (no "table not found")
```

The empirical verification of D8. Without C1 + the
`materialize_empty_record_batch` sibling helper, the
final `SELECT` would fail with `"table not found"`
after the empty-source `WHERE id > 100` filter
returns zero rows.

### Fixture D — Failure-before-catalog (Codex iter-1 R-fixture)

```sql
CREATE TABLE dst AS SELECT missing FROM src;
```

Expected: error message references the missing column;
the destination table `dst` must NOT be resolvable in
the catalog after the failed CTAS. Regression test
for D5 / R2 (source binding before catalog mutation).

## SoA-readiness check

ADR 021's four SoA-readiness invariants preserved:

1. **Catalog stays pure metadata for kernels.** ADR
   033 adds no kernel; catalog unchanged.
2. **`#[repr(u8)]` on `StableAggregateKernel`
   unchanged.** No aggregate kernel touched.
3. **Centralized dispatch unchanged.** CTAS uses
   existing ADR 029 `Catalog::create_table` path and
   ADR 032 materialization helper; no new dispatch
   site.
4. **No row-path dyn dispatch.** Coordinator helper
   operates on already-bound `ArrayRef`s; no `dyn`
   added.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched —
  ADR 033 adds no expression substrate.
- **Brainstorm 002 (WeakControlRing).** Untouched —
  pure Toron-local cycle.
- **Brainstorm 003 (Disaggregated Compute).** Touched
  but preserved — distributed coordinator gets one
  new reject arm; no fragmentation work added.
  Future distributed-write-context ADR composes with
  ADR 033.
- **Brainstorm 004 (Memory Arenas).** Untouched — no
  new IR carrier; the `Box<ExecutionPlan>` source
  field is the same boxing pattern used by other plan
  variants. Plan arena debt is still real but not yet
  workload-pressured.

No seam closes.

## What Not To Do

- **Don't carry pre-create table identity in
  `ExecutionPlan::CreateTableAsSelect`** (D2 / Codex
  iter-0 R1 — `TableId` / `TableObjectId` /
  `PhysicalDescriptorFingerprint` are obtained from
  `catalog.create_table` in the coordinator phase).
- **Don't bind CTAS by rewriting to `CreateTable +
  InsertSelect`** (D2 — separate IR variant preserves
  architecture-guard precision and statement-boundary
  clarity).
- **Don't widen `ExecutionPlan::CreateTable` to carry
  an optional source plan** (D2 — ADR 029 D-level
  carry-forward; the existing variant stays metadata-
  only).
- **Don't accept a CTAS column rename list in v1**
  (D3 / Codex iter-0 R3 — sqlparser's
  `CreateTable.columns` is `Vec<ColumnDef>`, not a
  clean alias list; supporting "column list means
  aliases under CTAS but typed columns elsewhere"
  muddles ADR 029's DDL surface).
- **Don't accept `IF NOT EXISTS` for CTAS in v1**
  (D3 / Codex iter-0 R4 — `Catalog::create_table`
  returns a `TableBinding`, not "created vs already
  existed"; supporting `IF NOT EXISTS` could
  accidentally append SELECT rows into an existing
  table).
- **Don't infer target nullability differently from
  source** (D4 — verbatim from source schema; the
  binder doesn't try to be clever about post-aggregate
  always-non-null output).
- **Don't mutate the catalog from the binder** (D5 /
  Codex iter-0 R2 — binder only binds + infers +
  validates; coordinator does the mutation).
- **Don't execute the source before catalog
  mutation** (D6 / Codex iter-1 R1 — table-exists /
  admin-unavailable failures should fail fast before
  spending source SELECT work).
- **Don't attempt rollback after catalog success +
  write failure** (D7 — non-atomicity is the v1
  contract; future ADR engages transactional
  metadata-write).
- **Don't use TRY_CAST or any cast at the CTAS
  layer** (D10 / Codex iter-1 R3 — source schema IS
  target schema by construction; users spell CASTs in
  SELECT).
- **Don't modify `materialize_insert_select_batch`
  for CTAS empty-source behavior** (D8 / Claude
  iter-1 C1 — it must still return `Ok(None)` for
  zero rows per ADR 032 D7; CTAS branch uses sibling
  helper `materialize_empty_record_batch`).
- **Don't pass outer morsel into the CTAS source
  SELECT** (D13 — `None` argument; preserves ADR 030 /
  032 carry-forward).
- **Don't extend the SELECT descriptor-evolution
  retry loop to CTAS** (D11 — mirrors ADR 030 D12 +
  ADR 032 carry-forward).
- **Don't fragment CTAS** (D11 / D12 — distributed
  reject before `fragment_plan`; walkers treat as
  leaf).
- **Don't fork Wendelon synthetic name literals into
  `binder.rs`** (D9 / Codex iter-1 R2 — import
  constants directly or use a neutral helper; no
  literal `"__row_hlc__"` / etc. in binder source).
- **Don't add a streaming `StorageWrite::write`
  overload** (out of scope — CTAS reuses ADR 032
  single-batch contract; bulk loads await a future
  ADR).
- **Don't return a row count** (D6 — `Ok(Vec::new())`;
  no RETURNING).
- **Don't add `blake3`** (ADR 030 D7 / ADR 031 /
  ADR 032 carry-forward invariant).
- **Don't add external allocator crates** (`bumpalo`
  / `typed-arena` / `slab` / `generational-arena` —
  ADR 031 D-level invariant carry-forward; Plan
  Arena deferred to evidence-pressured cycle).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Phantom table after write failure | Accepted v1 behavior (D7); documented non-atomicity; no rollback attempt. |
| R2 | Empty-source CTAS write behavior accidentally diverges from InsertSelect's no-op | Intentional split (D8); preserved by keeping ADR 032 helper unchanged and adding CTAS-specific empty-batch fallback (C1). |
| R3 | Schema inference accidentally creates storage-reserved columns | Binder validation against Wendelon synthetic constants (D9 / R2 / Codex iter-1 R2); architecture guard #7 pins absence of forked literals. |
| R4 | Binder grows a private fork of ADR 029 type / name rules | Wendelon constant import or neutral helper (D9); architecture guard pins absence of forked literals. |
| R5 | CTAS enters SELECT retry loop | Local coordinator intercept guard (#10). |
| R6 | Distributed path accidentally fragments CTAS source | Distributed coordinator + fragmenter rejects (#14 + #15) + walker leaf-treatment (#16). |
| R7 | Plan Arena scope creep | Deferred explicitly (out of scope + #18 architecture guard). |
| R8 | Pre-create table identity leaks into bound variant | Architecture guard #3 source-checks variant body for absence of `TableId` / `TableObjectId` / `PhysicalDescriptorFingerprint`. |
| R9 | Source execution happens before catalog mutation | Architecture guard #11 byte-position-comparison pins create-table before execute-bound. |
| R10 | WriteRequest constructed from binder-side fields instead of catalog binding | Architecture guard #12 pins `WriteRequest` uses `create_table` binding. |
| R11 | Empty-source helper drift | Architecture guard #13 pins exact mechanism (`materialize_insert_select_batch == None → materialize_empty_record_batch → write`). |
| R12 | ADR 032 `materialize_insert_select_batch` accidentally modified to support empty rows differently | Architecture guard #17 pins `Ok(None)` zero-row return. |
| R13 | Binder schema-inference path accidentally reuses `validate_omitted_insert_target_columns` (ADR 032 D4 helper) | CTAS has no omitted columns by construction; not reused. Architecture guard absence-check optional but not enforced. |
| R14 | Composition fixture coverage gaps | Four required fixtures (A SUM / B JOIN / C empty / D failure-before-catalog) cover the load-bearing cases. |
| R15 | Test file growth in `insert_test.rs` muddles ADR scope | New `crates/toron/tests/ctas_test.rs` module; Claude iter-2 A2 ratification. |

## Recorded dissent

**None at iter-2.**

Claude iter-0 had four wrong defaults (D3 column rename
list, D5 all-nullable, D7 atomicity wording, D8 empty-
source no-write) that Codex iter-0 corrected via R3, R5,
R4, R6 respectively. All four retracted cleanly at iter-1
without further reopen. Worth recording as cycle property:
composition cycles over substrate-rich foundations have
many subtle code-evidence constraints that iter-0 design
sketches can miss; iter-1 / iter-2 code-evidence audits
become more load-bearing.

Gemini iter-1 did NOT propose broader-scope framings
(no Plan Arena push, no streaming `StorageWrite` push,
no distributed-write-context push). Gemini iter-1
absorbed Codex iter-0 wholesale on the stricter v1
surface. **Second consecutive composition cycle**
(ADR 032 + ADR 033) where Gemini did not provoke the
iter-1/2 retraction pattern that the substrate cycles
(ADR 025 / 026 / 028 / 029 / 030 / 031) generated.
Stable cycle property after two data points:
**broader-scope framings are a substrate-cycle
phenomenon, not a universal cycle phenomenon.**

## Process observations

### Fastest composition convergence to date (ties ADR 032)

ADR 033 took 3 iterations / 0 flip-flops. Iter-0
produced three-way independent topic convergence (the
first in council history); iter-1 settled the load-
bearing shape; iter-2 was promotion-drafting.

| ADR | Shape | Iters | Flip-flops |
|---|---|---|---|
| ADR 029 | Substrate-introducing (DDL) | 4 | 0 |
| ADR 030 | Substrate-introducing (DML, cross-ring) | 4 | 1 |
| ADR 031 | Substrate-replacing pure-Toron | 3 + 1 ratification | 0 |
| ADR 032 | Composition (pure local) | 3 | 0 |
| **ADR 033** | **Composition (pure local + DDL+DML compose)** | **3** | **0** |

Three factors drove the speed:

1. **Three-way independent iter-0 topic convergence.**
   All three council members independently picked CTAS
   as lead for ADR 033. No topic dissent, no iter-1
   topic-reopen. First such convergence in council
   history.
2. **Pure-Toron and pure-local.** No Wendelon-side
   discovery pass; no distributed-write-context
   threading; ADR 029 catalog path + ADR 032
   materialization helper directly reused.
3. **Codex iter-0 R1-R7 absorbed wholesale by Claude
   iter-1 and Gemini iter-1.** Plus three more Codex
   iter-1 R-items refining the coordinator ordering
   and synthetic-name discipline. The council aligned
   tightly on the load-bearing shape after iter-1.

### Sixth consecutive cycle of Codex correctness-clarification pattern

Codex contributed 10 R-items total across iter-0 + iter-1
(7 + 3). Each surfaced from code-evidence audit of
catalog / binder / coordinator boundaries that Claude
and Gemini iter-0 sketches missed. Pattern is now
firmly established across ADR 024 / 025 / 026 / 028 /
029 / 030 / 031 / 032 / 033.

### First cycle where Claude iter-0 was retracted four-for-four on load-bearing defaults

My iter-0 picked the right topic but four wrong v1-
surface defaults. All four corrected by Codex iter-0.
Recorded as cycle property worth tracking:
**composition cycles over substrate-rich foundations
have more subtle code-evidence constraints than
substrate cycles**, because the substrate already
introduced the carriers, helpers, and contracts that
the composition layer must respect. Iter-0 design
sketches at the composition layer benefit from
deferring v1-surface defaults to iter-1 code-evidence
audits.

### Two cycle-property breaks confirmed (composition-specific)

- **Gemini iter-1/2 retraction streak still broken at
  6-of-8.** Second consecutive composition cycle
  (032 + 033) where Gemini did not provoke the streak.
  Updated cycle property: broader-scope framings are
  a substrate-cycle phenomenon.
- **Topic-concession-compresses-next-cycle pattern
  still broken at 3-of-5.** ADR 032 → 033 also
  generated no iter-0 alt-topic seed. Second composition-
  cycle data point confirming seed-carry-forward is
  substrate-cycle-specific.

Both cycle-property refinements are now confirmed by
two composition-cycle data points each.

### Three Claude iter-1/iter-2 contributions promoted to D-level or guard-level

| Claude contribution | Promoted to |
|---|---|
| iter-1 C1 empty-source sibling helper shape | D8 + new helper `materialize_empty_record_batch` + architecture guard #13 |
| iter-1 C4 + iter-2 fixture additions | 4 required composition fixtures (A SUM / B JOIN / C empty / D failure-before-catalog) |
| iter-2 A1 strict-pin guard wording for synthetic-name reject | Architecture guard #7 wording (negative-pattern enforcement against forked `__row_*__` literals) |

Iter-1 C2 ordering pins were absorbed into Codex iter-1
architecture guards #6 + #11. Iter-1 C3 was loosened by
Codex iter-1 R2 (strict no-forked-literals); my iter-2
A1 restored the strict pin. Iter-2 A2 (`ctas_test.rs`
adoption), A3 (helper-reuse table), A4 (variant-count
guard pairing) were folded into the impl plan.

## Future work

ADR 034+ candidates carry forward the ADR 029 → 033
arc:

- **Cross-ring distributed-write-context ADR.** Lifts
  ADR 030 D13 + ADR 032 D1 + ADR 033 D11 distributed
  rejects. Threads tenant / session / HLC context
  through `DistributedQueryCoordinator`.
- **`DROP TABLE`.** First DDL deletion ADR;
  prerequisite for `OR REPLACE` and for any rollback-
  on-write-failure CTAS extension.
- **`UPDATE` / `DELETE WHERE`.** DML siblings to ADR
  030 / 032 / 033; `__row_tombstone__` becomes load-
  bearing for DELETE; UPDATE needs row-identification
  predicate substrate.
- **`INSERT ... RETURNING` clause / `CTAS ...
  RETURNING`.** DML result-shape extension; shifts
  ADR 030 / 032 / 033 `Ok(Vec::new())` contract.
- **`ON CONFLICT` / `UPSERT` / `MERGE`.** DML
  extensions; need PK / unique-constraint substrate
  first.
- **Streaming / multi-batch `StorageWrite::write`.**
  Lifts ADR 032 D5 + D6 + ADR 033 D8 single-batch
  cap for bulk loads.
- **Write retries after descriptor evolution.** ADR
  030 D12 + ADR 032 + ADR 033 carry-forward.
- **Read-your-writes session HLC advancement.** ADR
  030 / 032 / 033 still discard `commit_hlc`.
- **Per-statement `durability` override.** ADR 030
  / 032 / 033 use wrapper default
  `DurabilityClass::QUORUM`.
- **Cross-ring metadata-write contract ADR.** Needed
  to lift `WendelonCatalog::create_table` from
  `AdminUnavailable`; also engages transactional CTAS.
- **Hidden-aggregate `ORDER BY` keys ADR.** Loosens
  ADR 023 reject.
- **HAVING alias visibility ADR.** Loosens ADR 023
  reject.
- **Non-aggregate computed `ORDER BY` ADR.** ADR 027
  D10 deferred.
- **Typed expression tape / opcode interpreter.**
  Brainstorm 004 deeper realization.
- **Common subexpression elimination (CSE) + literal
  deduplication.** ADR 031 D8 carry-forward.
- **Cranelift JIT.** Brainstorm 001 deeper
  realization.
- **Plan Arena / index-based plan IR.** Brainstorm
  004 broader framing; evidence-pressured carry-
  forward.
- **PBC partial-pushdown of computed-arg aggregates.**
  Cross-ring; workload-evidence-pressured.
- **Wendelon AGG consumer wiring.** Cross-ring.
- **Temporal types (DATE, TIMESTAMP)** for ADR 029 /
  030 / 032 / 033 type lattices.
- **DECIMAL** for ADR 029 / 030 / 032 / 033 type
  lattices.
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`DROP TABLE` / `ALTER TABLE` / `CREATE INDEX` /
  `CREATE VIEW`.** DDL siblings.
- **Constraints** (PRIMARY KEY / FOREIGN KEY / CHECK
  / column-level UNIQUE) / DEFAULT / GENERATED
  columns.
- **Richer Toron `Hlc` type** for read-your-writes.
- **Idempotency / write-retry-after-descriptor-
  evolution ADR.**
- **Kernel graduations** (`LinearRegress`,
  `ApproxQuantile`).

## Definition of Done

- [ ] `LogicalPlan::CreateTableAsSelect { table,
      source }` defined (12 → 13 variant count).
- [ ] `ExecutionPlan::CreateTableAsSelect { table,
      schema, source }` defined (10 → 11 variant
      count); **no TableId / TableObjectId /
      fingerprint fields** (D2 / Codex iter-0 R1).
- [ ] `SqlPlanner::lower_create_table` routes
      `CreateTable.query.is_some()` to
      `lower_create_table_as_select`; exhaustive
      `CreateTable` destructure (no `..`) preserved.
- [ ] `lower_create_table_as_select` rejects non-empty
      column list (D3 / R3) + `if_not_exists` (D3 /
      R4) with stable diagnostics.
- [ ] `Binder::bind_create_table_as_select` binds
      source BEFORE inferring schema (D5 / R2).
- [ ] `infer_ctas_schema` + `validate_ctas_column_name`
      + `is_supported_ctas_column_type` helpers
      present.
- [ ] Inferred schema validated against ADR 029
      lattice (six types + no duplicates + no
      synthetic names) (D4 / R5).
- [ ] Binder uses Wendelon synthetic constants
      directly or neutral helper; **no forked
      `__row_*__` literals** (D9 / Codex iter-1 R2).
- [ ] `materialize_empty_record_batch(schema)` helper
      present in `coordinator/write.rs` (D8 / Claude
      iter-1 C1).
- [ ] `materialize_insert_select_batch` unchanged —
      still returns `Ok(None)` for zero rows
      (architecture guard #17).
- [ ] `QueryCoordinator::execute_query` intercepts
      `LogicalPlan::CreateTableAsSelect` next to
      Insert / InsertSelect, before SELECT retry loop.
- [ ] `execute_write_plan` CTAS arm sequence:
      `catalog.create_table` → `execute_bound(source,
      None)` → `materialize_insert_select_batch` (Some)
      or `materialize_empty_record_batch` (None) →
      single `writer.write` → `Ok(Vec::new())` (D6 /
      Codex iter-1 R1).
- [ ] `WriteRequest` table identity from
      `create_table`'s returned `TableBinding`
      (architecture guard #12).
- [ ] `DistributedQueryCoordinator::execute_query`
      rejects CTAS before `fragment_plan` with D11
      diagnostic.
- [ ] `FragmentError::CreateTableAsSelectNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges`.
- [ ] Distributed / scheduler / server walkers treat
      CTAS as leaf; binder `optimize_for_required` is
      the documented exception (descends with `None`).
- [ ] Telemetry label `"create_table_as_select"`.
- [ ] Defensive `Executor::execute_bound` reject for
      uncoordinated CTAS.
- [ ] **All 18 ADR-033-specific architecture guards
      pass** + 1 paired adjustment to
      `architecture_guards_unchanged_plus_sort_variant`.
- [ ] **Fixture A (SUM grouped aggregate) passes
      end-to-end.**
- [ ] **Fixture B (INNER JOIN source) passes end-to-
      end.**
- [ ] **Fixture C (empty-source initialization)
      passes end-to-end — empty SELECT must SUCCEED
      on the newly-created table.**
- [ ] **Fixture D (failure-before-catalog) passes —
      missing source column fails without creating
      destination table.**
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT tests green.
- [ ] All existing SELECT-only plan-digest fixtures
      green (D14 re-stabilization).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-free
      preserved.
- [ ] ADR 031 `BoundExprProgram` `PartialEq`-only
      preserved.
- [ ] ADR 032 `materialize_insert_select_batch`
      contract (`Ok(None)` for zero rows) preserved.
- [ ] ADR 032 `evaluate_cast` `pub(crate)`
      visibility preserved (not consumed by CTAS).
- [ ] `StorageWrite` trait shape unchanged.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] Roadmap moves ADR 033 from candidate to
      promoted.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
