# ADR 062: Primary-Key DDL for Toron-Created Tables

**Status:** Promoted (3-of-3 on PK DDL — Claude, Codex, Gemini; the PK
**type set** is 2-of-3 narrow (Codex+Claude), with Gemini's broader set
recorded as minority).
**Seed:** ADR 061's carried table-identity-policy Phase 0. UPDATE needs a
logical row-identity, but Toron-created tables are unconditionally
**append-only / no-PK** (`sql_planner.rs:766` rejects `PRIMARY KEY`;
`catalog.rs:1490` hardcodes `RowIdentityPolicy::AppendOnly`), and there is
**no atomic composite tombstone+insert** (separate `StorageWrite` /
`StorageDeleteRows`), so append-only UPDATE would be non-atomic or a cross-ring
storage ADR. PK DDL is the smaller honest substrate — it **exposes Wendelon's
already-existing PK substrate** (`PrimaryKeyDescriptorV1`,
`RowIdentityPolicy::PrimaryKey`, PK admission/supersession, LSM elision;
`RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(col))` is already
constructible from Toron) through `CREATE TABLE`, not a new storage model.
**Council:** topic-divergence (Claude UPDATE/append-only, Codex/Gemini PK DDL,
Gemini-then audit→withdrawn) resolved 3-of-3 on PK DDL once the
no-atomic-composite-mutation boundary was grounded.
**Category:** Toron compute-ring — SQL DDL surface / catalog create policy.
Coordinator-owned; no new Wendelon control/data-ring method, storage
algorithm, or worker protocol.

## Summary

`CREATE TABLE … (col … PRIMARY KEY)` and table-level `PRIMARY KEY (col)` are
rejected today. 062 accepts a **single-column, explicit-`NOT NULL`** PK and
creates a `RowIdentityPolicy::PrimaryKey` descriptor (vs the append-only
default), so the tables users create carry the logical row-identity the future
UPDATE (063) needs. The SQL IR carries a **Toron-local** identity enum;
Wendelon descriptor types stay at the catalog boundary. `IF NOT EXISTS`
compatibility (ADR 061) extends to `(schema, row_identity)`. No UPDATE /
RETURNING / CTAS-PK / composite-PK / DDL-type-widening / new storage primitive.

## Decisions (12, Promotion-Locked)

**D1 Placement.** Toron SQL/catalog ADR. No Wendelon control-ring, data-ring,
storage, or worker protocol change.

**D2 Syntax.** Accept exactly one PK declaration in schema-only `CREATE TABLE`
— column-level (`id INT NOT NULL PRIMARY KEY`) and table-level (`…, PRIMARY
KEY (id)`). Reject duplicate declarations, composite keys, named constraints,
non-PK `UNIQUE`, `FOREIGN KEY`/`CHECK`/`DEFAULT`/generated columns, and PK
syntax in CTAS. One helper handles the column-option and
table-constraint/`primary_key`-field AST shapes; pin both accepted forms +
reject forms so sqlparser AST changes fail visibly.

**D3 Explicit `NOT NULL`.** A PK column must be explicitly `NOT NULL`;
`PRIMARY KEY` does **not** imply it in 062. Reject missing-`NOT NULL` *and*
`NULL` + `PRIMARY KEY` with a stable diagnostic ("PRIMARY KEY column must be
declared NOT NULL"). An intentional V1 divergence from common SQL ergonomics
(no implicit nullability rewrite); revisit later.

**D4 Type scope (narrow — 2-of-3).** Validate the lowered Arrow type in the
planner; accept only `DataType::Int32`, `DataType::Int64`, `DataType::Utf8` —
the currently expressible Toron CREATE TABLE types that Wendelon V1 can
canonicalize. **Do not** add unsigned-int / binary / large-string / timestamp
DDL support in this ADR. *(Minority — Gemini: Wendelon's broader canonical
set; not adopted because it would widen Toron's DDL type surface, which is
separate scope.)* Descriptor validation remains storage-boundary defense.

**D5 Planner-owned lowering.** `SqlPlanner::lower_create_table` validates +
normalizes PK syntax while building the schema (it already owns the CREATE
TABLE column/option/clause surface). The binder **preserves** the normalized
identity into `ExecutionPlan::CreateTable` (it does no CREATE TABLE column
resolution).

**D6 Toron-local identity enum.** `CreateTableRowIdentity { AppendOnly,
PrimaryKey { column: String } }` on `LogicalPlan::CreateTable` and
`ExecutionPlan::CreateTable`. **Not** on CTAS. **Wendelon `RowIdentityPolicy`
does not enter the SQL IR** (ring-boundary discipline).

**D7 Catalog create identity.** The catalog create path accepts the row
identity explicitly (a structured create request — no `Option` where `None`
implicitly means append). Wendelon-backed create maps `AppendOnly` →
`RowIdentityPolicy::AppendOnly`; `PrimaryKey { column }` →
`RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(column))`. No-PK
tables remain append-only.

**D8 Resolved identity.** Resolved metadata carries a Toron-local
`TableRowIdentity { AppendOnly, PrimaryKey { column, ordinal }, Unsupported }`
(alongside `primary_key_columns` for read planning). Unknown / content-hash /
future identity classes **fail closed** for create compatibility.

**D9 `IF NOT EXISTS` compatibility (ADR 061 composition).** 061's exact-schema
compatibility extends to `(schema, row_identity)`: same schema + different
identity (append-only-vs-PK, or PK-on-different-column) → `SchemaMismatch`.
The 061 D12 Wendelon-create-race reconciliation compares identity too.

**D10 Behavior proof.** End-to-end fixture: create a PK table via Toron SQL;
insert `(id=1,'old')` then `(id=1,'new')`; observe only the superseding
visible row. The SQL-created descriptor drives Wendelon PK admission — not
inert metadata. No new uniqueness logic in Toron.

**D11 Hard out of scope.** No UPDATE, RETURNING, CTAS PK inference, composite
PKs, standalone `UNIQUE`, `ALTER TABLE`, DDL type widening, Toron uniqueness
implementation, worker-local write-back, or Wendelon storage/wire change.

**D12 Carry to ADR 063.** First UPDATE cut targets PK tables only, rejects
PK-column mutation, and continues rejecting append-only/no-PK UPDATE until an
atomic composite tombstone+insert primitive exists.

## Architecture Guards (18 + 3 convention + meta)

1. `adr062_create_table_row_identity_type_is_toron_local`
2. `adr062_logical_create_table_carries_row_identity`
3. `adr062_execution_create_table_carries_row_identity`
4. `adr062_binder_preserves_create_table_row_identity`
5. `adr062_sql_planner_accepts_column_primary_key`
6. `adr062_sql_planner_accepts_table_primary_key`
7. `adr062_sql_planner_rejects_duplicate_primary_key`
8. `adr062_sql_planner_rejects_composite_primary_key`
9. `adr062_sql_planner_requires_primary_key_not_null`
10. `adr062_sql_planner_rejects_unsupported_primary_key_types`
11. `adr062_ctas_has_no_primary_key_identity_surface`
12. `adr062_catalog_create_request_carries_row_identity`
13. `adr062_wendelon_catalog_maps_primary_key_descriptor`
14. `adr062_if_not_exists_compares_row_identity`
15. `adr062_no_pk_create_remains_append_only`
16. `adr062_update_surface_remains_absent`
17. `adr062_returning_surface_remains_absent`
18. `adr062_prior_adr038_append_only_guard_retargeted`

**Convention:** `adr062_nilket_root_stays_runtime_free`,
`adr062_fixture_worker_source_unchanged`,
`adr062_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr062_prior_adr033_to_061_guards_continue_to_pass_except_retarget`.

**Prior-guard reconciliation:** ADR 038's append-only guard is retargeted —
no-PK schema-only CREATE TABLE → append-only; PK schema-only CREATE TABLE →
primary-key descriptor; CTAS → append-only.

## Integration Fixtures (16)

`planner_accepts_column_primary_key`, `planner_accepts_table_primary_key`,
`planner_rejects_primary_key_without_not_null_column_form`,
`planner_rejects_primary_key_without_not_null_table_form`,
`planner_rejects_null_primary_key_conflict`,
`planner_rejects_composite_primary_key`,
`planner_rejects_duplicate_primary_key_declarations`,
`planner_rejects_unique_constraint`,
`planner_rejects_unsupported_primary_key_type`,
`planner_rejects_ctas_primary_key`,
`catalog_create_without_pk_remains_append_only`,
`catalog_create_with_pk_builds_primary_key_descriptor`,
`if_not_exists_same_schema_same_identity_skips`,
`if_not_exists_same_schema_different_identity_schema_mismatch`,
`sql_created_pk_table_insert_supersedes_visible_row`,
`sql_created_pk_descriptor_enables_point_read_profile`.

## Out of Scope

UPDATE, RETURNING, composite PKs, `UNIQUE` independent of PK, `ALTER TABLE ADD
PRIMARY KEY`, CTAS PK inference, widening the DDL type surface, worker-local
write-back / new distributed mutation protocol, Wendelon storage-ring changes.

## Council History

iter-0: 3-way (Claude UPDATE/append-only, Codex PK-DDL, Gemini audit). iter-1:
Gemini withdrew audit; Claude conceded append-only UPDATE (hidden-identity
contract, no upsert, non-atomic) → PK-DDL. iter-2: Codex grounded the
**no-atomic-composite-mutation** boundary (separate write/delete) — 3-of-3 on
PK DDL. iter-3: Claude conceded **planner-owned (not binder)** PK lowering +
**intersection-only** PK types. iter-4: design locked; the sole residual is
the type set — narrow (Int32/Int64/Utf8) adopted 2-of-3, Gemini's broader set
recorded as minority (would widen DDL).

## Carried to ADR 063 (`UPDATE … WHERE` over PK tables)

Coordinator-owned full-row rewrite → `table-write` of the new row with the
**same PK** (Wendelon supersession handles visibility — atomic per row, single
op); `SET` cannot modify the PK column; append-only/no-PK UPDATE rejected until
a composite primitive exists; no `RETURNING`/`FROM`/subquery-RHS/transaction/
worker-local-write-back.

## Promotion criteria met

D-set 12, 3-of-3 on PK DDL (type set 2-of-3, minority recorded); 18 + 3
convention + meta guards; ADR 038 retargeted + CREATE-TABLE-PK-reject tests
retargeted (prior-guard reconciliation); 16 fixtures; Toron SQL/catalog;
exposes the existing Wendelon PK substrate; no Wendelon wire/storage/worker
change. Migration: `plans/impl/0062_primary_key_ddl.md`.

## Conclusion

PK DDL establishes the logical row-identity contract for SQL-created tables by
exposing Wendelon's existing PK substrate — single-column, explicit-`NOT
NULL`, narrow type set, Toron-local IR identity, `IF NOT EXISTS` identity
compatibility — leaving ADR 063 a small, atomic `UPDATE … WHERE` over
PK tables.
