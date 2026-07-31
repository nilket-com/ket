# ADR 063: PK-Table `UPDATE … WHERE` via Full-Row Upsert

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** the direct consumer ADR 062's PK-DDL substrate was built to unlock.
UPDATE was deferred from 061 because Toron-created tables were append-only
(no PK) and there is **no atomic composite tombstone+insert** primitive; 062
added PK descriptors, so a full-row write with the same PK is now the native
per-row replacement operation (Wendelon PK supersession).
**Council:** 3-of-3 from iter-0 once 062 made PK-table UPDATE small. The only
fork (no-WHERE vs WHERE-required) resolved to **WHERE-required**, documented
as deliberately stricter than the existing no-WHERE DELETE.
**Category:** Toron compute-ring — SQL DML composition / coordinator mutation
path. Composition over ADR 062; **no new Wendelon wire/storage/descriptor
primitive, no worker write-back, no transaction model.**

## Summary

`UPDATE <table> SET <col> = <expr>[, …] WHERE <pred>` on **PK tables**:
the binder rewrites it as a **full-row projection over the matched rows**
(assigned columns ← `SET` exprs; unassigned columns + the **PK** pass
through), the coordinator writes those rows through the existing
`StorageWrite`, and Wendelon's existing **per-row PK supersession** provides
replacement visibility. Reuses Filter + Projection + scalar-exprs + the
INSERT-SELECT write-with-source path; adds no storage substrate.

## Decisions (17, Promotion-Locked)

**D1 Placement.** Toron SQL/planner/binder/coordinator only. No Wendelon
server, client wire, descriptor schema, storage engine, or worker protocol
change.

**D2 Accepted syntax + exhaustive destructure.** `SqlPlanner::plan` admits
`Statement::Update` and exhaustively destructures the **sqlparser 0.54.0**
field set `{ table, assignments, from, selection, returning, or }`, accepting
only `UPDATE <table> SET <col> = <scalar_expr>[, …] WHERE <predicate>`.

**D3 Parser boundary — no opportunistic upgrade.** ADR 063 targets the
committed **sqlparser 0.54.0**. A 0.62.0 upgrade was checked this round and
does **not** pass `cargo check -p toron --tests` (broad AST drift —
`ObjectNamePart`, `ValueWithSpan`, `limit_clause`, changed CREATE/INSERT/
DELETE/SELECT/Cast shapes); a parser migration is a **separate ADR**, guarded
here.

**D4 Unsupported surface.** Reject `FROM`, `RETURNING`, conflict (`or`)
clauses, target aliases, joins, table args/hints/versions/ordinality/
partitions/JSON-path/samples, tuple assignment, qualified assignment targets,
empty + duplicate assignments, subquery RHS, aggregate/window RHS, and
parser-level `OUTPUT`/`ORDER BY`/`LIMIT`. Retarget the top-level
unsupported-statement message to include UPDATE.

**D5 WHERE required, DELETE unchanged.** Missing `WHERE` rejects in the
planner — **deliberately stricter than current no-WHERE DELETE**
(`lower_delete` `selection: Option`, `sql_planner.rs:683`). 063 documents the
asymmetry, does **not** change DELETE, and leaves no-WHERE UPDATE as a future
symmetry-restoration candidate.

**D6 Logical IR (name-based).** `LogicalPlan::Update { table: TableRef,
assignments: Vec<(String, Expr)>, predicate: Expr }`. No Wendelon descriptor /
`RowIdentityPolicy` / `TableRowIdentity` in logical IR.

**D7 Binder PK-identity gate.** Require `TableRowIdentity::PrimaryKey {
column, ordinal }` (ADR 062), ordinal in `primary_key_columns`. Append-only /
unsupported identities reject before execution: "UPDATE requires a PRIMARY KEY
table".

**D8 Primary key immutable.** Reject any assignment to the PK column. UPDATE
always writes a replacement row with the **same PK** — never models PK change
as delete-old + insert-new (the two-op path 062 avoided).

**D9 Assignment targets.** Only unqualified single-identifier
`AssignmentTarget::ColumnName`; reject tuple/qualified/duplicate/empty; binder
rejects unknown + storage-synthetic columns.

**D10 Simultaneous SET semantics.** Every `SET` RHS evaluates against the
**original matched row**; `SET a = b, b = a` swaps; assignments do not cascade
through list order. (Falls out of one projection over one input row.)

**D11 Full-row source projection.** Binder builds `Project(full target
columns; SET substituted) Filter(predicate) Scan(target, all user columns,
row_identity=None)`. PK + unassigned columns pass through; UPDATE does not
request appended row identity.

**D12 Execution IR (dedicated, source-bearing).** `ExecutionPlan::Update {
local_table_id, table, fingerprint, schema, source: Box<ExecutionPlan>,
profile_hint }`. The coordinator consumes `source`; it does not reconstruct
assignment semantics. Not an `InsertSelect` variant.

**D13 Write-source materialization (INSERT-SELECT parity).** Reuse the exact
`INSERT … SELECT` target-schema coercion/nullability lattice: numeric casts
per the existing lattice; unsupported combos fail before write; NULL into a
non-nullable target fails before write; payload schema = target schema.

**D14 Coordinator execution.** `execute_write_plan_with_source` handles
UPDATE: execute `source`; **zero rows → `affected_count_batch(0)`, no
`StorageWrite`**; else materialize → `StorageWrite::write` (descriptor pins) →
`affected_count_batch(outcome.rows)`. SQL sees `Complete { row_count }` only;
no `RETURNING`.

**D15 Descriptor evolution (ADR 054).** `DescriptorEvolved` → invalidate the
stale descriptor + return the error; **no retry**.

**D16 Atomicity = per-row, not transaction.** Claim only per-row replacement
visibility via existing Wendelon PK supersession. **Not** statement-level
atomicity, rollback, locking, lost-update detection, CAS, or all-row
isolation. Concurrency resolves by existing HLC last-writer visibility.

**D17 Distributed / point-read.** The source read may run through the
existing distributed read path; the write stays **coordinator-owned**; UPDATE
is **non-fragmentable** and fragmenter + worker reject raw
`ExecutionPlan::Update`. PK-equality `WHERE` reuses the existing
`QueryProfileHint::PointRead` (062's scan-binding); no UPDATE-specific
detector.

## Architecture Guards (23 + 3 convention + meta)

`adr063_sql_planner_accepts_statement_update`,
`adr063_sql_planner_destructures_sqlparser_054_update_fields`,
`adr063_sqlparser_062_upgrade_is_not_part_of_update_surface`,
`adr063_unsupported_statement_text_lists_update`,
`adr063_update_requires_where_and_delete_semantics_unchanged`,
`adr063_update_rejects_unsupported_clauses`,
`adr063_update_rejects_target_alias_joins_and_qualified_targets`,
`adr063_update_rejects_duplicate_and_empty_assignments`,
`adr063_logical_update_shape_is_name_based`,
`adr063_execution_update_shape_is_source_bearing`,
`adr063_logical_update_has_no_wendelon_identity_types`,
`adr063_binder_requires_primary_key_row_identity`,
`adr063_binder_rejects_primary_key_assignment`,
`adr063_binder_builds_full_row_source_projection`,
`adr063_update_source_scan_does_not_request_append_identity`,
`adr063_update_reuses_existing_point_read_profile`,
`adr063_update_reuses_write_source_coercion_policy`,
`adr063_coordinator_update_uses_storage_write_not_delete`,
`adr063_descriptor_evolved_invalidates_without_retry`,
`adr063_update_remains_non_fragmentable_and_worker_rejected`,
`adr063_sql_response_is_complete_only_no_returning`,
`adr063_no_wendelon_wire_storage_or_descriptor_change`,
`adr063_no_transaction_lock_compare_swap_or_no_where_surface`.
**Convention:** `adr063_nilket_root_stays_runtime_free`,
`adr063_fixture_worker_source_unchanged`,
`adr063_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr063_prior_adr054_to_062_guards_continue_to_pass_except_retarget`.

## Prior-Guard Reconciliation

**`adr062_update_surface_remains_absent` is retargeted.** It currently
asserts `Statement::Update` / `LogicalPlan::Update` / `ExecutionPlan::Update`
are *absent*; 063 deliberately makes that surface present under the PK-table
contract. The retarget preserves the original intent narrowly — still no
`RETURNING`, no no-WHERE UPDATE, no PK mutation, no append-only UPDATE, no
Wendelon surface change, no worker write-back. (`adr062_returning_surface_
remains_absent` stays — 063 adds no RETURNING.)

## Integration Fixtures (18)

`planner_accepts_update_where`; `planner_rejects_update_without_where`;
`planner_rejects_update_from_returning_conflict_and_parser_rejects_output_order_by_limit`;
`planner_rejects_update_target_alias_join_and_qualified_assignment`;
`planner_rejects_empty_tuple_or_duplicate_assignments`;
`binder_rejects_update_on_append_only_table`;
`binder_rejects_update_on_unsupported_identity_table`;
`binder_rejects_primary_key_assignment`;
`binder_update_pk_equality_uses_point_read_profile`;
`binder_update_non_pk_predicate_uses_scan_profile`;
`update_zero_matches_returns_row_count_zero_without_write`;
`update_writes_full_replacement_row`;
`update_assignments_read_original_row_values` (`SET a=b, b=a` swap);
`update_null_for_non_nullable_column_fails_before_write`;
`update_type_coercion_matches_insert_select_policy`;
`update_descriptor_evolved_invalidates_without_retry`;
`sql_listener_update_complete_frame_carries_row_count`;
`live_wendelon_sql_pk_update_supersedes_visible_row`.

## Out of Scope

Everything in D4/D16; plus no-WHERE UPDATE, PK mutation, append-only/no-PK
UPDATE, composite-PK UPDATE, `ALTER`, the sqlparser-0.62 upgrade, worker-local
write-back, and any Wendelon wire/storage/descriptor change.

## Council History

iter-0: 3-of-3 on the spine (full-row rewrite via PK-upsert, SET-as-
projection, PK-only, SET-can't-modify-PK). iter-1: Claude conceded
WHERE-required (2-of-3) and surfaced the verified DELETE-allows-no-WHERE
asymmetry; Codex sharpened per-row-not-transaction + point-read reuse +
INSERT-SELECT coercion. iter-2: Claude affirmed simultaneous-assignment +
pinned **sqlparser 0.54.0**; Codex corrected the exact field set. iter-3/4:
Codex *tested* the 0.62 upgrade (breaks the build → out of scope, guarded) and
caught the **`adr062_update_surface_remains_absent` retarget** after the first
dual pass had treated it as unnecessary — the one prior-guard reconciliation
this ADR needs. Design locked.

## Migration

`plans/impl/063_pk_table_update.md`.
