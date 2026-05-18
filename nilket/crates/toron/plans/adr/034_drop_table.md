# ADR 034: SQL `DROP TABLE` — Local DDL Deletion

## Status

Promoted. Three-way council convergence (Claude / Codex /
Gemini) reached in three iterations — third consecutive
composition cycle at 3-iter / 0-flip-flop cadence
(matches ADR 032 + 033). **Iter-0 produced a topic split**
(Claude + Codex on DROP TABLE; Gemini on DELETE WHERE)
that resolved decisively at iter-1 when Gemini engaged
Codex iter-0's E1 code-evidence audit and retracted the
DELETE lead. Iter-1 settled the load-bearing shape via
Codex iter-1's D1-D16 absorbed wholesale by Claude /
Gemini iter-1, plus three Claude iter-1 contributions
(C1 17th architecture guard, C2 DDL helper rename, C3
test-file naming convention) folded by iter-2. Iter-2
was promotion-drafting.

**Third composition ADR** in the post-substrate arc
(ADR 032 + 033 + 034). Continues the "composition cycles
are zero-break by construction" pattern: zero new
substrate, zero new architecture-guard invariants
broken, all ADR 029 / 030 / 031 / 032 / 033 invariants
preserved. **Composition trilogy complete** following
the substrate trilogy (ADR 029 → 030 → 031); ADR 035
sequencing commitment (D15) starts a new substrate phase
with the row-identity / predicate-delete cycle.

**Seventh consecutive cycle of the Codex correctness-
clarification pattern.** Codex iter-0 was unusually
complete (13 D-level sketches + E1 code-evidence audit
documenting `wendelon::client::TableDeleteRowsRequest`'s
row-key-id-based delete API + Toron's user-schema-only
scan boundary + ADR 030 D7's prohibition on Toron-side
synthetic column construction). The E1 audit was
decisive: it demonstrated DELETE WHERE is not a small
DML widening but a substrate cycle requiring either
row-identity discovery or a new predicate-delete storage
contract. Codex iter-1 added three R-items (D14 CTAS
cleanup composition test, D15 sequencing commitment,
D16 Plan Arena guardrail).

**Gemini iter-1/2 retraction streak restored at 7-of-9**
after the 6-of-8 plateau. Gemini iter-0 picked DELETE
WHERE with CRUD-completion framing; Gemini iter-1
explicitly retracted after engaging E1 evidence. Updated
cycle property: broader-scope framings emerge when
there is substrate ambiguity, regardless of whether the
cycle is composition or substrate-introducing. Composition
cycles 032 and 033 had no substrate ambiguity for Gemini
to engage with; ADR 034's topic question itself was
substrate-vs-composition framing, and Gemini iter-0
chose substrate without auditing the substrate boundary.

**Topic-concession-compresses-next-cycle pattern back at
4-of-6.** ADR 034 → 035 generates the row-identity /
predicate-delete substrate seed via D15. Updated cycle
property: the seed pattern returns when a cycle
explicitly defers substrate work to the next cycle.
Substrate-defer ADRs seed; pure composition cycles
don't.

**First cycle where Claude iter-0 process predictions
came true verbatim.** Claude iter-0 C5 predicted Gemini
iter-1 retraction; predicted Codex iter-1 absorption of
C2/C3/C4/Q7; predicted iter-2 promotion-drafting shape.
All three confirmed. Cycle property: iter-0 process
predictions about Gemini retraction are reliable when
there's a specific code-evidence pointer for Gemini to
engage with at iter-1.

Sixteenth instance of the ADR 015 cycle (after 016 /
017 / 018 / 019 / 022 / 023 / 024 / 025 / 026 / 027 /
028 / 029 / 030 / 031 / 032 / 033).

## Scale boundary

**Primary:** Toron SQL planner + binder + local +
distributed coordinator + existing `Catalog` trait
boundary. Surface widens the SQL planner statement
dispatcher to accept `Statement::Drop`; adds
`LogicalPlan::DropTable` + `ExecutionPlan::DropTable`
leaf variants; extends the `Catalog` trait with one
new method (`drop_table`); routes DROP through the
existing `execute_catalog_plan` helper alongside
CREATE TABLE.

In scope:

- `SqlPlanner::plan` dispatches `Statement::Drop` to
  new `lower_drop_table`. The exhaustive
  `Statement::Drop` destructure (no `..`) mirrors ADR
  029 D2 + ADR 033 D3 discipline.
- New `LogicalPlan::DropTable { name: TableRef,
  if_exists: bool }` variant.
- New `ExecutionPlan::DropTable { name: TableRef,
  if_exists: bool }` variant. **No `TableId`,
  `TableObjectId`, or `PhysicalDescriptorFingerprint`
  fields** — the bound DROP plan carries no pre-drop
  table identity (mirrors ADR 033 R1 symmetry).
- DDL name helper renamed `lower_create_table_name`
  → `lower_ddl_table_name` (Claude iter-1 C2). Both
  existing call sites (CREATE TABLE per ADR 029,
  CTAS per ADR 033) updated. DROP TABLE uses the
  renamed helper.
- Binder pass-through: `Binder::bind_plan` arm emits
  `ExecutionPlan::DropTable` directly. **No catalog
  resolution from the binder** (no `resolve_table` /
  `resolve_table_id` / `drop_table` calls). Existence
  + `IF EXISTS` semantics centralize in the catalog
  mutation.
- New `Catalog::drop_table(&self, name: &TableRef,
  if_exists: bool) -> Result<Option<TableBinding>,
  CatalogError>` trait method.
- `MockCatalog::drop_table` removes the resolved
  `TableId` from `by_id` and uses `by_ref.retain` to
  remove every alias pointing to that same id. Exact
  alias cleanup — no alias repointing, no peer-table
  removal. No `next_table_id` reuse / decrement.
- `WendelonCatalog::drop_table` returns
  `CatalogError::AdminUnavailable` for known tables
  (mirrors ADR 029 `create_table` discipline) and
  `Ok(None)` for unknown tables with `IF EXISTS`.
- `execute_catalog_plan` extended to handle both
  `CreateTable` and `DropTable`. Local
  `QueryCoordinator::execute_query` intercepts
  `LogicalPlan::DropTable` next to `CreateTable`,
  **before** the SELECT descriptor-evolution retry
  loop. Distributed
  `DistributedQueryCoordinator::execute_query` also
  routes `ExecutionPlan::DropTable` through
  `execute_catalog_plan` **before** `fragment_plan`.
- New `FragmentError::DropTableNotFragmentable`
  variant; fragmenter rejects before `contains_join`
  / `count_exchanges`.
- Distributed / scheduler / server metadata walkers
  treat `DropTable` as a leaf (join detection
  returns false; exchange counting returns zero;
  scan metadata returns none; replacement walkers
  leave the variant unchanged).
- Defensive `Executor::execute_bound` reject for
  uncoordinated `ExecutionPlan::DropTable`;
  defensive `LogicalPlan::DropTable` reject for
  unbound execution.
- Telemetry label `"drop_table"` for distributed /
  server / scheduler walker `DropTable` arms
  (Claude iter-0 C2; distinct from `"catalog"` which
  CREATE TABLE owns).
- 17 architecture guards + 1 paired adjustment to
  the existing `architecture_guards_unchanged_plus_sort_variant`
  variant-count catch-all guard.
- New test file `crates/toron/tests/drop_table_test.rs`
  with 11 required functional fixtures (planner /
  binder / catalog / coordinator / distributed /
  Wendelon / defensive-reject coverage).

Out of scope:

- SQL `DELETE WHERE`. Deferred to ADR 036 after ADR
  035 (row-identity / predicate-delete substrate
  per D15 sequencing commitment).
- `DROP VIEW` / `DROP SCHEMA` / `DROP INDEX` / `DROP
  FUNCTION`. Defer until views / schemas / indexes /
  UDFs exist.
- Multi-target DROP (`DROP TABLE a, b, c`). Reject
  in v1 with stable diagnostic.
- `CASCADE` keyword. Toron has no dependent objects
  yet. Reject in v1.
- Explicit `RESTRICT` keyword. Toron has no
  dependent objects yet; accepting `RESTRICT` would
  pre-commit dependency semantics before views /
  indexes / foreign keys exist. Reject in v1
  (Claude iter-0 + Codex iter-0 + iter-1 default).
- `PURGE` keyword. Storage reclaim is a separate
  contract. Reject in v1.
- `TEMPORARY` keyword. Toron has no temporary table
  concept. Reject in v1.
- Physical storage reclaim. ADR 034 removes catalog
  reachability only.
- In-memory `InMemoryTableStorage` map cleanup.
  Future storage-admin ADR.
- Wendelon descriptor deletion. Future cross-ring
  metadata-write contract ADR.
- Invalidation of already-bound `ExecutionPlan::Scan`
  values held by test code or distributed
  coordinator state. Storage backend behavior
  (`TableNotFound` for InMemory; whatever Wendelon
  returns) is the v1 contract for stale plans.
- Automatic rollback of failed CTAS writes. ADR 033
  D7 non-atomicity remains; DROP TABLE provides the
  manual recovery path (Claude iter-0 C1).
- `CREATE OR REPLACE TABLE`. Future ADR can lower
  to `DROP IF EXISTS + CREATE TABLE` once DROP
  exists (Claude iter-0 C1).
- Row-identity discovery substrate (Toron-side
  exposure of `__row_key_id__` hidden column).
  Future ADR 035.
- Predicate-delete storage contract (Wendelon-side
  acceptance of predicate expressions). Future ADR
  035 cross-ring.
- `StorageDeleteRows` trait. Future ADR 035.
- New SQL syntax beyond bare `DROP TABLE [IF
  EXISTS] table_name`.
- New aggregate kernel, expression variant, PBC
  opcode, or fragmenter detector change for SELECT
  paths.
- Plan Arena / index-based plan IR. Third
  consecutive composition cycle preserving Plan
  Arena as named-debt-not-yet-pressured.
- New external allocator crate (`bumpalo` /
  `typed-arena` / `slab` / `generational-arena`).
- `blake3` dependency.
- Streaming `StorageWrite` overload.
- Read-your-writes session HLC advancement.
- Per-statement durability override.

## Context

ADR 029 → 033 closed the local DDL/DML composition
loop: CREATE TABLE (029) + INSERT VALUES (030) +
INSERT SELECT (032) + CTAS (033). The four-statement
set lets users build, populate, derive, and
materialize tables in a single local coordinator. The
arc has two unaddressed gaps:

1. **No deletion primitive of any kind.** No `DROP
   TABLE`. No `DELETE WHERE`. Every CREATE / CTAS
   accretes catalog reachability with no SQL-surface
   path to remove it.
2. **No row-identification substrate.** Toron scans
   return user-schema `RecordBatch` values; synthetic
   storage columns are hidden and Toron-side
   construction is forbidden by ADR 030 D7.

These are different gaps with different fixes. The
iter-0 question was whether ADR 034 closes the first
(DDL deletion: `DROP TABLE`) or starts on the second
(row-identity substrate, which DML deletion needs).

**Codex iter-0 E1 code-evidence audit was decisive.**
Wendelon's delete API is row-key-id-based
(`wendelon::client::TableDeleteRowsRequest { row_key_ids:
Vec<Identity> }`), not predicate-based. Toron's
`StorageScan::scan` returns user-schema batches without
storage synthetic identity columns. ADR 030 D7
explicitly forbids Toron-side construction of
`__row_*__` synthetic columns. A correct `DELETE WHERE`
needs either:

- a new row-identity discovery substrate (Toron-side
  exposure of hidden column when caller is
  coordinator);
- a new predicate-delete storage contract (Wendelon-
  side acceptance of predicate);
- a scan-then-tombstone-write sequence with a new
  `StorageDeleteRows` trait.

Each is a substrate cycle in its own right. Bundling
the SQL-surface lift with substrate introduction in
one cycle would either rush substrate decisions or
stretch the cycle to 5+ iterations with multiple
flip-flops. ADR 034 = narrow DROP TABLE; ADR 035 =
row-identity / predicate-delete substrate (D15
sequencing commitment); future ADR 036 = SQL DELETE
WHERE as a narrow composition lift over the substrate.

DROP TABLE in turn unblocks three concrete deferred
items (Claude iter-0 C1):

1. ADR 029 D14 `IF NOT EXISTS` semantic complement
   (`DROP IF EXISTS`).
2. ADR 033 D7 CTAS non-atomicity manual cleanup
   (drop the phantom catalog entry after a CTAS
   write failure).
3. Future `CREATE OR REPLACE TABLE` lowering path
   (`DROP IF EXISTS x; CREATE TABLE x AS SELECT ...`
   becomes a planner-side option once DROP exists).

The load-bearing decisions are: **how the bound DROP
plan carries (or doesn't carry) table identity** (D5:
doesn't, mirroring ADR 033 R1 symmetry — catalog
resolves at mutation time), **what SQL surface DROP
accepts in v1** (D2/D3: bare `DROP TABLE [IF EXISTS]
table_name` only — no column list, no CASCADE / RESTRICT
/ PURGE / TEMPORARY / multi-name / non-table), **where
existence checks live** (D6 binder pass-through;
D7 catalog owns IF EXISTS semantics), **what alias
cleanup discipline applies to MockCatalog** (D8: exact,
no repointing), **whether storage reclaim is bundled**
(D9: no, catalog reachability only), **whether DELETE
WHERE is the same cycle or a follow-on** (D15: follow-
on after ADR 035 row-identity substrate cycle).

The council converged in three iterations with zero
flip-flops. Iter-0 produced topic split (resolved at
iter-1 via Codex E1 evidence pressure). Iter-1 settled
load-bearing shape (D1-D16). Iter-2 was promotion-
drafting.

## Decision

### D1 — Topic: narrow `DROP TABLE`

Support:

```sql
DROP TABLE sales;
DROP TABLE IF EXISTS sales;
DROP TABLE app.sales;
DROP TABLE IF EXISTS app.sales;
```

This removes catalog reachability only. It does not
delete rows by predicate, reclaim physical storage,
invalidate already-bound plans, or engage Wendelon
metadata administration.

### D2 — Strict SQL surface

The planner accepts exactly:

- `Statement::Drop`;
- `object_type == ObjectType::Table`;
- `if_exists` true or false;
- exactly one table name;
- `cascade == false`;
- `restrict == false`;
- `purge == false`;
- `temporary == false`.

Reject with stable diagnostics:

- non-table object types (`DROP VIEW`, etc.);
- multiple names (`DROP TABLE a, b`);
- `CASCADE`;
- explicit `RESTRICT`;
- `PURGE`;
- `TEMPORARY`.

Explicit `RESTRICT` is rejected in v1 even though it
maps to today's default behavior. Toron has no
dependent objects yet, so accepting it would claim
dependency semantics before views / indexes / foreign
keys exist.

### D3 — Exhaustive DROP lowering

`lower_drop_table` destructures sqlparser's DROP shape
without a `..` rest pattern:

```rust
Statement::Drop {
    object_type,
    if_exists,
    names,
    cascade,
    restrict,
    purge,
    temporary,
}
```

This mirrors ADR 029 D2 + ADR 033 D3 discipline.
sqlparser dependency upgrades should fail fast if
DROP gains fields.

### D4 — Neutral DDL table-name helper (Claude iter-1 C2)

Rename `lower_create_table_name` → `lower_ddl_table_name`.
Both existing call sites (CREATE TABLE per ADR 029,
CTAS per ADR 033) are updated to use the renamed
helper. DROP TABLE uses the same helper.

The helper preserves ADR 029's name rule:

- `table`;
- `schema.table`;
- three-part names reject.

Do not route DROP through SELECT-side
`table_ref_from_object_name`. The rename eliminates
the implicit "create-specific" reading; future
ALTER TABLE / CREATE INDEX / CREATE VIEW DDL ADRs
inherit the same neutral helper pattern.

### D5 — Leaf IR variants

Add:

```rust
LogicalPlan::DropTable {
    name: TableRef,
    if_exists: bool,
}

ExecutionPlan::DropTable {
    name: TableRef,
    if_exists: bool,
}
```

`ExecutionPlan::DropTable` carries no `TableId`,
`TableObjectId`, `PhysicalDescriptorFingerprint`, or
boxed source child. The catalog resolves current
identity at mutation time (mirrors ADR 033 R1
symmetry).

Variant counts:

- `LogicalPlan`: 13 → 14;
- `ExecutionPlan`: 11 → 12.

### D6 — Binder pass-through

The binder emits `ExecutionPlan::DropTable` directly.
It must not call:

- `resolve_table`;
- `resolve_table_id`;
- `drop_table`.

Existence and `IF EXISTS` semantics belong to the
catalog mutation operation, centralized in one place.

### D7 — Catalog API

Extend `Catalog`:

```rust
async fn drop_table(
    &self,
    name: &TableRef,
    if_exists: bool,
) -> Result<Option<TableBinding>, CatalogError>;
```

Semantics:

- existing table: remove catalog reachability and
  return `Ok(Some(binding))`;
- missing table with `if_exists == true`: return
  `Ok(None)`;
- missing table with `if_exists == false`: return
  `CatalogError::TableNotFound`;
- Wendelon known table: return
  `CatalogError::AdminUnavailable` (even with `IF
  EXISTS`);
- Wendelon unknown table with `IF EXISTS`: return
  `Ok(None)`;
- Wendelon unknown table without `IF EXISTS`:
  return `CatalogError::TableNotFound`.

No new `CatalogError` variant is needed. The
`Option<TableBinding>` return shape is forward-
friendly for future storage-reclaim ADRs that need
to know which table identity was removed.

### D8 — MockCatalog exact alias cleanup

`MockCatalog::drop_table` removes:

- the `by_id` entry for the resolved `TableId`;
- every `by_ref` alias pointing to that same
  `TableId`.

It must not:

- remove aliases for other table ids;
- repoint an unqualified alias to another schema-
  qualified table;
- reuse or decrement `next_table_id`.

Examples:

- If `app.sales` owns the unqualified `sales` alias,
  dropping `app.sales` removes both `app.sales` and
  `sales`.
- If `other.sales` exists but `sales` points to
  `app.sales`, dropping `app.sales` does not
  reassign `sales` to `other.sales`.
- If `sales` points to `app.sales`, dropping
  unqualified `sales` drops the table resolved by
  that alias and removes all aliases to that table
  id.

This is the deletion-side counterpart to ADR 029's
schema-qualified alias-no-overwrite rule.

### D9 — No storage reclaim guarantee

ADR 034 only changes catalog reachability.

Out of scope:

- physical file deletion;
- in-memory storage map cleanup;
- Wendelon descriptor deletion;
- invalidation of already-bound `ExecutionPlan::Scan`
  values;
- automatic rollback of failed CTAS writes.

Future SQL statements bind through the catalog and
fail after a drop. Already-bound plans held by test
code or distributed coordinator state are outside the
ADR 034 contract; because ADR 034 does not reclaim
storage, a stale bound scan may still read old data if
the backend still has it. Tests pin SQL rebind and
recreate behavior only (Claude iter-0 C3 refined).

Recreate-after-drop is still observable through
normal SQL because `MockCatalog` assigns a new
monotonic `TableId` to the recreated table — stale
in-memory storage data for the old id is unreachable
through the new binding.

### D10 — Coordinator routing via shared DDL helper

Extend `execute_catalog_plan` to handle both
`CreateTable` and `DropTable`. Local
`QueryCoordinator::execute_query` intercepts
`LogicalPlan::DropTable` next to `CreateTable`,
**before** the SELECT descriptor-evolution retry
loop. `DistributedQueryCoordinator::execute_query`
also routes `ExecutionPlan::DropTable` through the
same metadata helper **before** `fragment_plan`,
matching CREATE TABLE.

With `WendelonCatalog`, known-table DROP returns
`AdminUnavailable`; with `MockCatalog`, distributed
tests can exercise metadata mutation without worker
dispatch.

### D11 — Empty statement result

Successful `DROP TABLE` returns `Ok(Vec::new())`.
`DROP TABLE IF EXISTS missing` also returns
`Ok(Vec::new())`.

No row count, list of dropped tables, RETURNING
output, or dropped identity in the SQL result.

### D12 — Defensive executor + fragmenter rejects

DROP is coordinator-only DDL:

- unbound executor rejects `LogicalPlan::DropTable`
  with `ExecutionError::NotImplemented { feature:
  "unbound DROP TABLE execution" }`;
- bound executor rejects `ExecutionPlan::DropTable`
  with `ExecutionError::NotImplemented { feature:
  "DROP TABLE execution is coordinator-only" }`;
- fragmenter rejects `ExecutionPlan::DropTable`
  before `contains_join` / `count_exchanges` with
  new `FragmentError::DropTableNotFragmentable`;
- distributed / scheduler / server walkers treat
  `DropTable` as a leaf (mirrors ADR 029 / 032 /
  033 walker discipline).

### D13 — Telemetry label (Claude iter-0 C2)

`server::operator_kind_label(ExecutionPlan::DropTable
{ .. })` returns `"drop_table"`.

Do not reuse CREATE TABLE's `"catalog"` label. Per
ADR 032 / 033 telemetry-label discipline, each
statement form gets a distinct label so traces
distinguish without payload inspection.

### D14 — CTAS cleanup composition fixture

ADR 034 must include a CTAS cleanup composition test
(Claude iter-0 C1 promoted to test coverage
requirement by Codex iter-1):

1. create source table;
2. CTAS into target;
3. drop target;
4. rerun the same CTAS successfully.

This proves the manual cleanup path ADR 033 D7
named. It does not make CTAS atomic — that requires
a future transactional metadata-write ADR.

### D15 — ADR 035 sequencing commitment (Claude iter-0 C4)

ADR 035 should be the row-identity / predicate-delete
substrate cycle before SQL `DELETE WHERE`.

It must settle:

- how Toron discovers visible row key ids for SQL
  predicates;
- whether row identity travels as a privileged
  side-channel or hidden column;
- how a `StorageDeleteRows` trait maps to Wendelon's
  `TableDeleteRowsRequest`;
- local in-memory deletion behavior;
- delete-count result semantics.

Then SQL `DELETE WHERE` (ADR 036) can be a smaller
composition ADR over that substrate.

This restores the topic-concession-compresses-next-
cycle pattern at 4-of-6 — substrate-defer ADRs seed
the next cycle.

### D16 — Plan Arena remains debt, not scope

`DROP TABLE` adds one leaf plan variant and no boxed
source child. It does not materially worsen the
plan-spine debt that Plan Arena would address.

ADR 034 must not add `bumpalo`, `typed-arena`,
`slab`, `generational-arena`, `blake3`, or storage
synthetic row construction.

Plan Arena remains a future substrate ADR, not a
hidden dependency of `DROP TABLE`. Third consecutive
composition cycle preserving this discipline.

## Reject removal (ADR 015 cycle's sixteenth instance)

ADR 034 lifts the implicit DROP rejection that all
prior cycles assumed (`SqlPlanner::plan` previously
rejected `Statement::Drop` as an unsupported
statement). The new `lower_drop_table` accepts the
narrow form. The supported-statement diagnostic
updates to include `DROP TABLE`.

**Added (planner, lower time):**

- non-table object type rejects with `"unsupported
  DROP object type: only TABLE is supported"`;
- multiple names rejects with `"DROP TABLE supports
  exactly one table name"`;
- CASCADE / RESTRICT / PURGE / TEMPORARY each rejects
  with stable per-clause diagnostic.

**Added (catalog, mutation time):**

- missing table without `IF EXISTS`:
  `CatalogError::TableNotFound`;
- known Wendelon table:
  `CatalogError::AdminUnavailable`.

**Added (distributed coordinator, before fragmentation):**

- `ExecutionPlan::DropTable` routes through
  metadata-only helper; no fragmentation reject path
  needed (DROP is metadata-only, not coordinator-
  reject-only).

**Added (fragmenter, before join / exchange
analysis):**

- `FragmentError::DropTableNotFragmentable` (defensive
  reject if uncoordinated reach occurs).

Existing CREATE TABLE / INSERT / INSERT SELECT / CTAS
rejects remain unchanged. No reject is silently
widened or narrowed beyond the explicit lift.

## Architecture Guard Set

**17 ADR-034-specific guards + 1 paired adjustment to
the existing variant-count catch-all = 18 architecture
guard touchpoints.**

| # | Guard | Pins | File anchor |
|---|---|---|---|
| 1 | `adr034_sql_planner_accepts_drop_table` | Statement dispatcher contains `Statement::Drop`; unsupported diagnostic includes `DROP TABLE` | `sql_planner.rs` |
| 2 | `adr034_drop_table_destructure_is_exhaustive` | `lower_drop_table` body contains all DROP fields; no `..` | `sql_planner.rs` |
| 3 | `adr034_drop_table_reject_surface_is_narrow` | Checks `ObjectType::Table`; rejects multi-name + `cascade` + `restrict` + `purge` + `temporary` | `sql_planner.rs` |
| 4 | `adr034_drop_table_name_lowering_is_ddl_specific` | Neutral `lower_ddl_table_name` exists; CREATE + CTAS + DROP all call it; rejects three-part names | `sql_planner.rs` |
| 5 | `adr034_logical_variant_count_is_14` | `LogicalPlan` 13 → 14 growth | `ir.rs` |
| 6 | `adr034_execution_variant_count_is_12` | `ExecutionPlan` 11 → 12 growth | `ir/execution.rs` |
| 7 | `adr034_execution_drop_table_has_no_identity_or_box` | Body contains only `name` + `if_exists`; no `TableId` / `TableObjectId` / `PhysicalDescriptorFingerprint` / `Box<` | `ir/execution.rs` |
| 8 | `adr034_binder_drop_table_is_pass_through` | Binder arm emits `ExecutionPlan::DropTable`; no `.resolve_table(` / `.resolve_table_id(` / `.drop_table(` | `binder.rs` |
| 9 | `adr034_catalog_drop_table_returns_optional_binding` | Trait signature returns `Result<Option<TableBinding>, CatalogError>` | `catalog.rs` |
| 10 | `adr034_mock_catalog_drop_table_cleans_aliases` | Body removes from `by_id`; uses `by_ref.retain`; compares against dropped id; no alias repointing | `catalog.rs` |
| 11 | `adr034_wendelon_catalog_drop_table_is_admin_unavailable` | Known path returns `AdminUnavailable` with `DROP TABLE` message; unknown `if_exists` returns `Ok(None)` | `catalog.rs` |
| 12 | `adr034_coordinators_intercept_drop_table` | Local DropTable branch before retry loop; distributed DropTable branch before `fragment_plan`; `execute_catalog_plan` contains `.drop_table(` | `coordinator.rs` |
| 13 | `adr034_executor_and_fragmenter_reject_drop_table` | Executor strings present; `DropTableNotFragmentable` present; fragmenter reject before join/exchange | `exec.rs`, `distributed/fragment.rs` |
| 14 | `adr034_distributed_walkers_treat_drop_table_as_leaf` | Fragmenter / scheduler / server helper arms include `ExecutionPlan::DropTable { .. }` as leaf | `distributed/*`, `server/mod.rs` |
| 15 | `adr034_operator_label_is_drop_table` | `operator_kind_label` maps DropTable to `"drop_table"` | `server/mod.rs` |
| 16 | `adr034_does_not_add_storage_reclaim_or_plan_arena` | No `StorageReclaim` / `StorageDrop` / table-reclaim trait; no `blake3`; no arena dependencies | `storage.rs`, `Cargo.toml` |
| 17 | `adr034_catalog_trait_grows_by_exactly_one_method` (Claude iter-1 C1) | Trait contains `async fn drop_table`; negative-pattern against `truncate_table` / `replace_table` / `drop_schema` / `alter_table` / `rename_table` / `drop_table_outcome` | `catalog.rs` |

Paired adjustment:

| Existing guard | Adjustment |
|---|---|
| `architecture_guards_unchanged_plus_sort_variant` | `LogicalPlan` count assert bumps 13 → 14 with `DropTable` content assert; `ExecutionPlan` count assert bumps 11 → 12 with `DropTable` content assert |

Each guard's diagnostic string names ADR 034 + the
specific decision or refinement it enforces (per the
ADR 015 cycle narrow-detector pattern).

## Required functional fixtures

11 fixtures required for promotion. All must pass
before ADR 034 lands. Live in
`crates/toron/tests/drop_table_test.rs` (Claude iter-1
A3 file-naming convention).

1. `planner_accepts_drop_table_surface` — `DROP TABLE
   sales` + `DROP TABLE IF EXISTS app.sales`; assert
   logical plan fields.
2. `planner_rejects_unsupported_drop_table_shapes` —
   `DROP VIEW v` + `DROP TABLE a, b` + `DROP TABLE
   sales CASCADE` + `DROP TABLE sales RESTRICT` +
   `DROP TABLE sales PURGE` + `DROP TEMPORARY TABLE
   sales` + `DROP TABLE a.b.c`.
3. `binder_emits_identity_free_drop_table` — bind
   with empty `MockCatalog`; assert
   `ExecutionPlan::DropTable { name, if_exists }`
   without catalog resolution.
4. `mock_catalog_drop_table_semantics` — existing →
   `Some(binding)`; subsequent resolve fails; missing
   without `IF EXISTS` → `TableNotFound`; missing
   with `IF EXISTS` → `None`.
5. `mock_catalog_drop_table_schema_alias_cleanup_is_exact`
   — create `app.sales` + `other.sales`; verify
   unqualified `sales` points to `app.sales`; drop
   `other.sales` (unqualified `sales` still resolves
   to `app.sales`); recreate / drop `app.sales`
   (unqualified `sales` no longer resolves); do NOT
   assert alias repointing.
6. `query_coordinator_executes_drop_table` —
   create / insert / select succeeds; drop; SQL
   select fails at bind; `DROP TABLE IF EXISTS
   missing` succeeds.
7. `query_coordinator_recreate_after_drop_isolated`
   — create / insert / drop; recreate same name;
   select from recreated table returns zero rows or
   only newly inserted rows, never old rows.
8. `query_coordinator_ctas_cleanup_allows_rerun`
   (D14 fixture; promotes Claude iter-0 C1) —
   create source; CTAS target; drop target; rerun
   same CTAS; select target succeeds.
9. `distributed_coordinator_executes_drop_table_before_fragment`
   — `MockCatalog` + empty `StaticWorkerRegistry` +
   failing transport; create table; run distributed
   DROP; table no longer resolves; failing transport
   not touched.
10. `wendelon_catalog_drop_table_semantics` — known
    table → `AdminUnavailable`; unknown with `IF
    EXISTS` → success; unknown without `IF EXISTS`
    → `TableNotFound`.
11. `executor_and_fragmenter_reject_direct_drop_table_execution`
    — executor rejects bound + unbound direct reach;
    fragmenter returns `DropTableNotFragmentable`.

## SoA-readiness check

ADR 021's four SoA-readiness invariants preserved:

1. **Catalog stays pure metadata for kernels.** ADR
   034 adds one new method but no kernel; catalog
   metadata-only discipline preserved.
2. **`#[repr(u8)]` on `StableAggregateKernel`
   unchanged.** No aggregate kernel touched.
3. **Centralized dispatch unchanged.** DROP uses
   existing `execute_catalog_plan` helper; no new
   dispatch site.
4. **No row-path dyn dispatch.** Coordinator helper
   operates on catalog method call; no `dyn` added.

## Brainstorm-seam touch check

- **Brainstorm 001 (Cranelift JIT).** Untouched.
- **Brainstorm 002 (WeakControlRing).** Untouched.
- **Brainstorm 003 (Disaggregated Compute).** Touched
  but preserved — distributed coordinator gets one
  new metadata-routing arm; no fragmentation work
  added.
- **Brainstorm 004 (Memory Arenas).** Untouched —
  DROP adds one leaf variant with no boxed child.
  Third consecutive composition cycle preserving Plan
  Arena as named-debt-not-yet-pressured.

No seam closes.

## What Not To Do

- **Don't carry pre-drop table identity in
  `ExecutionPlan::DropTable`** (D5 — mirrors ADR 033
  R1 symmetry; `TableId` / `TableObjectId` /
  `PhysicalDescriptorFingerprint` come from catalog
  mutation, not bound plan).
- **Don't resolve or mutate catalog from the binder**
  (D6 — pass-through only; existence + `IF EXISTS`
  centralize in catalog).
- **Don't accept `CASCADE` / explicit `RESTRICT` /
  `PURGE` / `TEMPORARY` in v1** (D2 — Toron has no
  dependent objects, no storage-admin contract, no
  temporary-table concept).
- **Don't accept multi-target DROP** (D2 — `DROP
  TABLE a, b` rejected; one statement, one table).
- **Don't accept `DROP VIEW` / `DROP SCHEMA` / `DROP
  INDEX`** (D2 — defer until those object types
  exist).
- **Don't reuse CREATE TABLE's `"catalog"` telemetry
  label** (D13 — distinct `"drop_table"` per ADR
  032 / 033 discipline).
- **Don't fork sqlparser destructure with `..`**
  (D3 — exhaustive destructure mirrors ADR 029 D2 +
  ADR 033 D3; sqlparser upgrades fail fast).
- **Don't add `truncate_table` / `replace_table` /
  `drop_schema` / `alter_table` / `rename_table` /
  `drop_table_outcome` to the `Catalog` trait** (D7
  / guard #17 — narrow surface; one new method, no
  opportunistic peers).
- **Don't promise storage reclaim** (D9 — catalog
  reachability only; future storage-admin ADR
  engages).
- **Don't promise stale-bound-plan invalidation**
  (D9 — stale bound plans are outside the contract;
  tests pin SQL rebind behavior only).
- **Don't add automatic CTAS rollback** (D14 +
  out-of-scope — manual cleanup path via DROP only).
- **Don't add `DELETE WHERE` to this cycle** (D15
  / out-of-scope — defer to ADR 036 after ADR 035
  substrate).
- **Don't bypass `lower_ddl_table_name`** (D4 — DROP
  uses the renamed neutral DDL helper; do not route
  through SELECT-side `table_ref_from_object_name`).
- **Don't reuse / decrement `next_table_id`** (D8 —
  MockCatalog monotonic id assignment preserved;
  recreate-after-drop gets a new id).
- **Don't repoint unqualified aliases on drop** (D8
  — exact alias cleanup only; never reassign
  `sales` from dropped `app.sales` to existing
  `other.sales`).
- **Don't add `blake3`** (out-of-scope + guard #16
  invariant carry-forward from ADR 030 D7).
- **Don't add external allocator crates** (`bumpalo`
  / `typed-arena` / `slab` / `generational-arena`
  — guard #16 invariant carry-forward from ADR 031).
- **Don't bundle DROP coverage in
  `insert_test.rs`** (Claude iter-1 A3 — new file
  `crates/toron/tests/drop_table_test.rs` matches
  ADR 033 `ctas_test.rs` precedent; one statement
  form, one test file).

## Risk register

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Bound DROP carries pre-drop identity | Guard #7 source-checks variant body for absence of identity fields. |
| R2 | Binder resolves table or calls drop_table | Guard #8 negative-pattern enforcement. |
| R3 | Coordinator order swaps (DROP routed through fragment) | Guards #12 + #13 + #14 pin metadata-helper routing before fragmentation. |
| R4 | `Catalog` trait grows beyond `drop_table` | Guard #17 negative-pattern enforcement against peer DDL helpers. |
| R5 | MockCatalog repoints aliases on drop | Guard #10 positive `retain` pattern + negative assertions on repointing logic. |
| R6 | WendelonCatalog accepts known-table DROP | Guard #11 source-checks for `AdminUnavailable` with `DROP TABLE` diagnostic message. |
| R7 | Telemetry label drift (CREATE/DROP merged label) | Guard #15 source-checks `"drop_table"` literal in `operator_kind_label`. |
| R8 | DDL name helper rename incomplete (one call site missed) | Guard #4 source-checks all three call sites (CREATE / CTAS / DROP) use neutral helper. |
| R9 | sqlparser `Statement::Drop` shape change accepted silently | Guard #2 enforces exhaustive destructure with no `..`. |
| R10 | DELETE WHERE smuggled in via DROP cycle | D15 sequencing commitment explicit; D2/D9/D16 out-of-scope list explicit. |
| R11 | Storage reclaim helper added to coordinator | Guard #16 negative-pattern check. |
| R12 | Stale-bound-plan after drop behavior over-specified | D9 explicitly makes stale bound plans out of scope; test #6 pins SQL rebind/recreate behavior instead of storage behavior. |
| R13 | Existing CREATE TABLE / CTAS tests regress through DDL helper rename | Two call sites updated mechanically; existing tests verify behavior unchanged. |
| R14 | Plan Arena pressure increases | DROP adds leaf variant; no boxed child; net pressure essentially unchanged. |
| R15 | CTAS rollback test (#8) misleads users about atomicity | D14 + D7 (carried from ADR 033) explicitly document non-atomicity; test name `_cleanup_allows_rerun` doesn't suggest atomicity. |

## Recorded dissent

**None at iter-2.**

Iter-0 Gemini DELETE WHERE lead was retracted at
iter-1 after engaging Codex iter-0 E1 evidence (row-
key-id-based Wendelon delete API; Toron's user-schema
scan boundary; ADR 030 D7 synthetic-column prohibition).
Retraction was clean — no iter-2 reopening attempt.

Claude iter-0 had no wrong defaults this cycle (vs
ADR 033 where four defaults were retracted). My
iter-0 picked the right topic and ratified Codex
iter-0 wholesale, adding five distinct contributions
(C1-C5) that all promoted to either D-level decisions
(C2 → D13; C4 → D15), test coverage (C1 → D14), or
architecture guards (C1 → guard #17 via iter-1
contribution; Q7 → guard #16). Worth recording as
cycle property: composition-trilogy cycles (ADR 032
+ 033 + 034) showed declining Claude iter-0 default-
correction load — iter-0 design instincts improved as
the substrate landscape stabilized.

## Process observations

### Third 3-iter / 0-flip-flop composition cycle

ADR 032 / 033 / 034 all converged in 3 iterations
with zero flip-flops. The composition-cycle floor is
firmly established at 3 iters.

| ADR | Shape | Iters | Flip-flops |
|---|---|---|---|
| ADR 032 | Composition (pure local INSERT SELECT) | 3 | 0 |
| ADR 033 | Composition (pure local CTAS, four Claude defaults retracted) | 3 | 0 |
| **ADR 034** | **Composition (pure local DROP TABLE, Gemini DELETE retracted)** | **3** | **0** |

Three data points. The pattern is no longer
hypothesis; it's the established composition-cycle
shape.

### Topic-split-resolved-by-E1-evidence

ADR 034 was the first iter-0 topic split (Codex +
Claude on DROP TABLE vs Gemini on DELETE WHERE)
resolved decisively at iter-1 by a single code-
evidence audit. The pattern is documented: when one
council member has direct code-evidence audit
(Codex E1) and the other(s) propose substrate-
adjacent topics, iter-1 retraction is reliable.

Recorded cycle property: **iter-0 topic splits with
direct code-evidence pointers resolve at iter-1
without iter-2 reopening.**

### Seventh consecutive Codex correctness-clarification cycle

Codex iter-0 contributed 13 D-level sketches + E1
audit; Codex iter-1 added three more R-items (D14
CTAS test, D15 sequencing, D16 Plan Arena
guardrail). The pattern is now firmly established
across ADR 024 / 025 / 026 / 028 / 029 / 030 / 031 /
032 / 033 / 034.

### Composition trilogy complete

ADR 029 → 030 → 031 was the substrate trilogy (DDL +
DML + expression-IR-replacing).
ADR 032 → 033 → 034 is the composition trilogy
(INSERT SELECT + CTAS + DROP TABLE) over that
substrate.

ADR 035 (per D15 sequencing) starts a new substrate
phase with the row-identity / predicate-delete cycle.
The trilogy alternation pattern (substrate trilogy
→ composition trilogy → substrate cycle …) is worth
tracking as a potential new cycle property.

### Three Claude iter-0/1 contributions promoted to D-level

| Claude contribution | Promoted to |
|---|---|
| iter-0 C2 telemetry label `"drop_table"` | D13 |
| iter-0 C4 ADR 035 sequencing commitment | D15 |
| iter-0 C1 three-deferred-item unblock list | D14 (CTAS cleanup test) |
| iter-0 Q7 negative-pattern storage-reclaim guard | Architecture guard #16 |
| iter-1 C1 catalog-trait narrowness guard | Architecture guard #17 |
| iter-1 C2 DDL helper rename | D4 |
| iter-1 A3 test-file naming convention | Codex iter-2 impl Phase 7 + impl naming pattern codified |

Seven Claude contributions promoted. iter-0 C3
(stale-bound-plan wording) absorbed by Codex iter-1
D9. iter-0 C5 (Gemini retraction prediction) confirmed
verbatim — not a promoted item but a cycle property
data point.

### Composition-cycle property catalog after three cycles

After ADR 032 + 033 + 034:

1. **Composition cycles are zero-substrate-invariant-
   break by construction.** (3 data points)
2. **Composition cycles converge in 3 iterations.**
   (3 data points; matches the substrate-cycle
   composition-cadence hypothesis)
3. **Composition cycles preserve Plan Arena as
   named-debt.** (3 data points)
4. **Broader-scope framings are substrate-cycle
   phenomena, not universal.** (Reinforced by ADR
   034 Gemini retraction — Gemini's DELETE WHERE
   framing under-engaged the substrate audit Codex
   iter-0 performed; once Gemini engaged at iter-1,
   retraction was immediate)
5. **Topic-concession-seed pattern returns when a
   cycle explicitly defers substrate work to the
   next cycle.** ADR 034 D15 generates the seed for
   ADR 035. (1 data point in the new pattern; prior
   data points were substrate-cycle-specific)

## Future work

ADR 035+ candidates carry forward the ADR 029 → 034
arc:

- **ADR 035 — row-identity / predicate-delete
  substrate cycle** (firm commitment per D15). Five
  questions to settle (per Codex iter-0 D13 sketch
  / D15 list).
- **ADR 036 — SQL `DELETE WHERE`** (firm sequencing
  commitment after ADR 035). Composition over the
  row-identity substrate.
- **`CREATE OR REPLACE TABLE`** — lower to `DROP IF
  EXISTS x; CREATE TABLE x AS SELECT ...` once
  DROP exists. Composition over ADR 029 / 033 / 034.
- **Cross-ring distributed-write-context ADR.**
  Lifts ADR 030 D13 + ADR 032 D1 + ADR 033 D11
  distributed write rejects. ADR 034's Wendelon
  `drop_table` `AdminUnavailable` belongs to the
  metadata-write contract below.
- **`UPDATE WHERE`** — composition over ADR 035 row-
  identity substrate + new partial-write contract.
- **`INSERT ... RETURNING` / `CTAS ... RETURNING` /
  `DELETE ... RETURNING`.**
- **`ON CONFLICT` / `UPSERT` / `MERGE`** — DML
  extensions; need PK / unique-constraint substrate
  first.
- **Streaming / multi-batch `StorageWrite::write`.**
- **Cross-ring metadata-write contract ADR.** Lifts
  `WendelonCatalog::create_table` /
  `WendelonCatalog::drop_table` `AdminUnavailable`;
  unblocks transactional CTAS + atomic DROP.
- **Storage reclaim ADR.** Physical file deletion,
  in-memory storage map cleanup, Wendelon descriptor
  deletion; lifts D9 carry-forward.
- **Stale-bound-plan invalidation ADR.**
- **Read-your-writes session HLC advancement ADR.**
- **Per-statement durability override.**
- **Hidden-aggregate `ORDER BY` keys ADR.**
- **HAVING alias visibility ADR.**
- **Non-aggregate computed `ORDER BY` ADR.**
- **Typed expression tape / opcode interpreter.**
- **Plan Arena / index-based plan IR.**
- **CSE + literal deduplication.**
- **Cranelift JIT compilation.**
- **Logical `Expr` arena migration.**
- **PBC partial-pushdown of computed-arg aggregates.**
- **Temporal types (DATE, TIMESTAMP).**
- **DECIMAL.**
- **Utf8 ↔ numeric CAST + TRY_CAST.**
- **Boolean ↔ numeric CAST + TRY_CAST.**
- **`ALTER TABLE` / `CREATE INDEX` / `CREATE VIEW`
  / `DROP VIEW` / `DROP SCHEMA` / `DROP INDEX`** —
  DDL siblings using the renamed
  `lower_ddl_table_name` helper pattern from D4.
- **Constraints** (PRIMARY KEY / FOREIGN KEY /
  CHECK / column-level UNIQUE) / DEFAULT /
  GENERATED.
- **Wendelon AGG consumer wiring.**
- **Kernel graduations.**

## Definition of Done

- [ ] `LogicalPlan::DropTable { name, if_exists }`
      defined (13 → 14 variant count).
- [ ] `ExecutionPlan::DropTable { name, if_exists }`
      defined (11 → 12 variant count); **no
      TableId / TableObjectId / fingerprint fields,
      no boxed child** (D5 / R1 carry-forward from
      ADR 033).
- [ ] `lower_create_table_name` renamed to
      `lower_ddl_table_name`; both existing call
      sites (CREATE TABLE per ADR 029, CTAS per ADR
      033) updated; DROP TABLE uses the renamed
      helper (D4).
- [ ] `SqlPlanner::plan` dispatches `Statement::Drop`
      to `lower_drop_table`; supported-statement
      diagnostic includes `DROP TABLE`.
- [ ] `lower_drop_table` exhaustively destructures
      `Statement::Drop` with no `..`; rejects non-
      table object types + multi-name + CASCADE +
      RESTRICT + PURGE + TEMPORARY with stable
      diagnostics (D2 / D3).
- [ ] `Binder::bind_plan` `DropTable` arm emits
      `ExecutionPlan::DropTable` directly; no
      catalog calls in the binder arm (D6).
- [ ] `Catalog::drop_table(&self, name, if_exists)
      -> Result<Option<TableBinding>, CatalogError>`
      trait method added (D7).
- [ ] `MockCatalog::drop_table` removes from
      `by_id`; uses `by_ref.retain` to clear all
      aliases pointing to the dropped table id; does
      not repoint aliases; does not reuse
      `next_table_id` (D8).
- [ ] `WendelonCatalog::drop_table` returns
      `AdminUnavailable` for known tables (with
      `DROP TABLE` message); returns `Ok(None)` for
      unknown tables with `IF EXISTS` (D7).
- [ ] `execute_catalog_plan` handles both
      `CreateTable` and `DropTable` arms.
- [ ] Local `QueryCoordinator::execute_query`
      intercepts `LogicalPlan::DropTable` next to
      `CreateTable`, before SELECT retry loop (D10).
- [ ] `DistributedQueryCoordinator::execute_query`
      routes `ExecutionPlan::DropTable` through
      `execute_catalog_plan` before `fragment_plan`
      (D10).
- [ ] `FragmentError::DropTableNotFragmentable`
      reject fires before `contains_join` /
      `count_exchanges` (D12).
- [ ] Distributed / scheduler / server walkers
      treat `DropTable` as leaf (D12 / guard #14).
- [ ] Telemetry label `"drop_table"` (D13 / guard
      #15).
- [ ] Defensive `Executor::execute_bound` +
      `LogicalPlan::DropTable` rejects (D12).
- [ ] **All 17 ADR-034-specific architecture guards
      pass.**
- [ ] **Paired adjustment to
      `architecture_guards_unchanged_plus_sort_variant`
      passes.**
- [ ] **Fixture #1 (planner accepts) passes.**
- [ ] **Fixture #2 (planner rejects) passes.**
- [ ] **Fixture #3 (binder identity-free emit)
      passes.**
- [ ] **Fixture #4 (MockCatalog semantics) passes.**
- [ ] **Fixture #5 (alias cleanup exactness)
      passes.**
- [ ] **Fixture #6 (coordinator create/drop/rebind-
      fail) passes.**
- [ ] **Fixture #7 (recreate-after-drop isolation)
      passes.**
- [ ] **Fixture #8 (CTAS cleanup composition,
      D14) passes.**
- [ ] **Fixture #9 (distributed metadata DROP)
      passes.**
- [ ] **Fixture #10 (Wendelon semantics) passes.**
- [ ] **Fixture #11 (defensive rejects) passes.**
- [ ] All existing CREATE TABLE / VALUES INSERT /
      INSERT SELECT / CTAS tests green.
- [ ] All existing SELECT-only plan-digest fixtures
      green (composition-cycle carry-forward).
- [ ] ADR 029 catalog metadata-only preserved.
- [ ] ADR 030 `ExecutionPlan::Insert` expression-
      free preserved.
- [ ] ADR 031 `BoundExprProgram` `PartialEq`-only
      preserved.
- [ ] ADR 032 InsertSelect invariants preserved.
- [ ] ADR 033 CTAS invariants preserved (including
      `materialize_insert_select_batch` `Ok(None)`
      zero-row contract).
- [ ] `StorageWrite` trait shape unchanged.
- [ ] No `blake3` dependency added.
- [ ] No external allocator crate added.
- [ ] No storage reclaim trait / helper introduced.
- [ ] Roadmap moves ADR 034 from candidate to
      promoted; advances active section to ADR 035
      (row-identity / predicate-delete substrate
      per D15 sequencing commitment).
- [ ] `cargo fmt --check` clean.
- [ ] `cargo test -p toron` green.
