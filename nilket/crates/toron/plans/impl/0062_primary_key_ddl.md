# Impl Plan 062: Primary-Key DDL for Toron-Created Tables

**Status:** Implemented, paired with `plans/adr/0062_primary_key_ddl.md`.
Evidence: `plans/impl/0062_primary_key_ddl_evidence.md`.
**Posture:** Toron SQL/coordinator + catalog only. Exposes Wendelon's existing
PK substrate via DDL; **no new Wendelon wire/storage primitive, no worker
protocol, no DDL type-surface widening.** Six implementation phases after a
Phase 0 audit. Novel pieces: a Toron-local row-identity enum threaded
planner -> IR -> catalog, and the catalog mapping PK to
`RowIdentityPolicy::PrimaryKey`.

## Invariants carried in

- Prior ADRs remain unchanged except for explicit retargets: ADR 038's
  append-only-create guard and the CREATE-TABLE-PK reject tests now narrow to
  no-PK/PK/CTAS behavior. `crates/nilket` runtime-free; fixture worker
  unchanged; no-`*V1`.
- No `Statement::Update` / `RETURNING`; CTAS stays append-only.

## Phase 0 — Audit (verified anchors)

- `sql_planner.rs:630` `lower_create_table` destructures `primary_key` (:659)
  and **rejects it** (:766); builds `LogicalPlan::CreateTable` (:847). `:325`
  `ColumnOption::Unique { .. }` is the column-level PK/unique path. The CTAS
  create path (`lower_create_table_as_select`, ~:857) — reject PK there.
- Binder turns `LogicalPlan::CreateTable` → `ExecutionPlan::CreateTable` by
  copying lowered fields (no column resolution) → **PK validation is
  planner-owned; binder preserves.**
- `catalog.rs` `create_table_with_outcome` (ADR 061) returns
  `CreateTableOutcome { binding, created }`, hardcodes
  `RowIdentityPolicy::AppendOnly` (:1490).
  `RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(col))` is
  **already constructible from Toron** (error.rs:337, lib.rs:1232).
- ADR 038 guard pins SQL-create = `AppendOnly` — to retarget.

## Phase 1 — Toron-local identity types (ir.rs, catalog.rs)

- `pub enum CreateTableRowIdentity { AppendOnly, PrimaryKey { column: String } }`
  (SQL-IR intent) and `pub enum TableRowIdentity { AppendOnly, PrimaryKey {
  column: String, ordinal: ColumnOrdinal }, Unsupported }` (resolved binding).
  Derive the existing plan/binding traits. **No Wendelon `RowIdentityPolicy`
  in the SQL IR.**

## Phase 2 — Planner: validate + normalize PK (sql_planner.rs)

- Replace the `:766` reject with `lower_create_table_schema_and_identity(
  columns, constraints, primary_key)`: collect the column-level PK option
  (:325 path) + table-level PK (constraints / `primary_key` field) into one
  declaration; validate exactly-one-column (reject composite/duplicate), the
  column **exists**, is **`NOT NULL`** (reject missing + `NULL`+PK conflict),
  and is a **supported type** (`Int32`/`Int64`/`Utf8`; reject others). Returns
  `(SchemaRef, CreateTableRowIdentity)`. Keep `UNIQUE`-not-PK + other clause
  rejects. Reject PK in the CTAS path.

## Phase 3 — IR + binder pass-through (ir.rs, ir/execution.rs, binder.rs)

- Add `row_identity: CreateTableRowIdentity` to `LogicalPlan::CreateTable` and
  `ExecutionPlan::CreateTable` (not CTAS). Binder **clones it through**.

## Phase 4 — Catalog create accepts identity (catalog.rs)

- Thread identity into create (a structured `CreateTableRequest { name,
  schema, row_identity, if_not_exists }` or a `row_identity` param); keep
  `create_table` as the wrapper. The Wendelon-backed catalog maps `PrimaryKey
  { column }` → `RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(
  column))`, `AppendOnly` → `RowIdentityPolicy::AppendOnly` (stops hardcoding
  :1490). Preserve the ADR 061 race-safe outcome + D12 Wendelon-create-race
  (now comparing identity too).

## Phase 5 — Binding identity + IF NOT EXISTS compatibility (catalog.rs)

- Carry `TableRowIdentity` on resolved metadata (keep `primary_key_columns`).
  Extend 061's compatibility predicate to `(arrow_schema, row_identity)`:
  same-schema-same-identity → `created: false`; same-schema-different-identity
  → `SchemaMismatch`; content-hash/unknown identity → **fail closed**.

## Phase 6 — Guards + retargets + fixtures + evidence + roadmap

- 18 architecture + 3 convention + meta guards (ADR list). **Retarget** the
  ADR 038 append-only guard → "no-PK → AppendOnly; single-column PK →
  PrimaryKey; CTAS → AppendOnly"; meta scoped `…_except_retarget`.
- **Retarget** the existing CREATE-TABLE tests asserting PK is rejected → the
  narrowed accept/reject matrix.
- 16 fixtures (ADR list), incl. the end-to-end `sql_created_pk_table_insert_
  supersedes_visible_row` + the point-read fixture.
- `plans/impl/0062_primary_key_ddl_evidence.md`: `architecture_test adr062`,
  full `architecture_test`, `cargo test -p toron` (planner/binder/catalog/
  create-table suite counts), `clippy -p toron --all-targets -- -D warnings`,
  `git diff --check` (capped-linker procedure).
- Roadmap: 062 promoted; ADR 063 = `UPDATE … WHERE` over PK tables.

## Watch-items

1. **Planner-owned PK validation/normalization** (not binder); binder
   preserves only.
2. **Toron-local identity enum** — no Wendelon `RowIdentityPolicy`/descriptor
   types in `ir.rs`/`ir/execution.rs`; map only at the catalog boundary.
3. **Supported PK types = Int32/Int64/Utf8** (DDL ∩ Wendelon V1) — **do not
   widen** the DDL type surface.
4. **Explicit `NOT NULL`** — reject missing + `NULL`+PK conflict; clear
   diagnostic; standard-SQL-divergence recorded.
5. **IF NOT EXISTS compatibility includes `row_identity`** (061 composition);
   fail-closed content-hash.
6. **ADR 038 retarget** (prior-guard reconciliation — guard goes false); meta
   scoped `…_except_retarget`.
7. **UPDATE / RETURNING hard-guards remain absent**; CTAS append-only, PK in
   CTAS rejected.
8. **Insert-supersession fixture** proves the SQL-created descriptor drives
   Wendelon PK admission (not inert).
9. **No Wendelon storage/wire/worker change**; no new uniqueness logic.

## Acceptance checklist

- [x] Phase 1: `CreateTableRowIdentity` (IR) + `TableRowIdentity` (binding).
- [x] Phase 2: planner helper validates single-col / NOT-NULL / supported-type
      / exists; column + table syntax; rejects composite/dup/UNIQUE/CTAS-PK/
      nullable/unsupported-type.
- [x] Phase 3: `row_identity` on Logical + Execution CreateTable; binder
      preserves.
- [x] Phase 4: catalog create accepts identity; Wendelon maps PK →
      `PrimaryKeyDescriptorV1::single`; no-PK → AppendOnly; 061 outcome + D12
      preserved.
- [x] Phase 5: `TableRowIdentity` binding; IF-NOT-EXISTS compat compares
      identity; fail-closed content-hash.
- [x] Phase 6: 18 + 3 + meta guards; ADR 038 + CREATE-TABLE-PK test retargets;
      16 fixtures; evidence + roadmap; no UPDATE / RETURNING / CTAS-PK / DDL-
      type-widening / Wendelon-storage drift.
