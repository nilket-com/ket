# ADR 064: DML `RETURNING` Rows

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** ADR 063 completed the core mutation spine; the smallest user-visible
completion is *returning the affected rows* from DML — moving the mutation
result surface from `Complete { row_count }` to a result set.
**Council:** 3-of-3 from iter-0 (DML trio, CTAS out). Resolved forks: CTAS
out; **DELETE RETURNING not PK-gated** (DELETE works on append-only, `adr036`);
**DELETE partial receipts fail closed** (`DeleteRowsOutcome` is count-only);
**precompute+validate the projection before the mutation, emit only after
storage success**.
**Category:** Toron compute-ring — SQL DML result surface. **Proven-small:**
the result protocol is already bimodal (`sql_plan_returns_rows` →
`ResultBatch`/`Complete`) and each mutation already materializes the affected
rows. No Wendelon / worker / storage-request / descriptor / SQL-wire /
parser-version / admission-config change.

## Scale Boundary

ADR 064 is an exterior Toron composition change. It touches Toron's SQL
planner, logical IR, binder, execution IR, coordinator mutation execution, and
SQL listener result-shape predicate. It does not change Wendelon, worker
transport, SQL frame variants, descriptor schemas, storage request/receipt
shapes, tenant admission policy, or sqlparser version.

## Summary

Support `RETURNING` for `INSERT VALUES`, `INSERT SELECT`, `UPDATE`, and
`DELETE`. A returning mutation emits existing Arrow `ResultBatch` frames then
`Complete { row_count }`; non-returning mutations stay Complete-only.
Visibility is standard SQL — INSERT/UPDATE return post-mutation rows, DELETE
returns pre-delete rows. The coordinator **materializes + validates the
RETURNING projection before the storage call**, buffers it, and **emits it
only after storage success and receipt-count validation**. Any planning /
binding / projection / storage / descriptor-evolved / timeout / cancellation /
receipt-mismatch path emits no rows.

## Decisions (18, Promotion-Locked)

**D1 DML-only surface.** RETURNING on `INSERT VALUES`, `INSERT SELECT`,
`UPDATE`, `DELETE`. CTAS/CREATE/DROP/others remain without RETURNING.

**D2 sqlparser 0.54 boundary.** Use the existing Insert/Update/Delete
`returning` fields; **no 0.62 upgrade** here.

**D3 Standard visibility.** INSERT / INSERT-SELECT → inserted target rows
(after target-column reorder/coercion); UPDATE → post-update replacement rows;
DELETE → pre-delete candidate rows.

**D4 Toron-local returning IR.** Planner lowers sqlparser `SelectItem`s into a
parser-independent enum: `ReturningProjection::Star` or
`ReturningProjection::Exprs { exprs, output_names }`. Logical + execution
plans carry **no sqlparser AST**.

**D5 Bound returning projection.** Binder lowers to `BoundReturningProjection
{ exprs: BoundExprProgram, output_schema: SchemaRef }`; `ExecutionPlan::schema()`
reports that schema for DML-with-RETURNING.

**D6 Projection vocabulary.** `RETURNING *`, unqualified target columns,
aliases, scalar expressions over the affected row. Reject aggregate / window /
subquery, target aliases, qualified wildcards, storage-synthetic columns, and
`old.` / `new.` namespaces.

**D7 User-schema boundary.** RETURNING binds against the target **user**
schema; DELETE's appended row identity is internal (`RETURNING *` excludes it,
name lookup can't resolve it).

**D8 No new PK gate.** RETURNING follows each statement's existing table
support — INSERT and DELETE RETURNING work on **append-only and PK** tables;
UPDATE RETURNING is PK-only only because ADR 063 UPDATE is PK-only.

**D9 Precompute before mutation.** Returning rows are projected + validated
**before** `StorageWrite`/`StorageDeleteRows`; projection/type/expression
failures skip the mutation (no half-done write).

**D10 Emit only after receipt.** Buffered rows emit only after storage success
+ receipt validation. No streaming-before-commit.

**D11 Row count must not lie.** `Complete.row_count` == emitted rows ==
storage receipt rows. INSERT/UPDATE require `WriteOutcome.rows ==
returned_rows`.

**D12 DELETE exact receipt.** `DeleteRowsOutcome { requested, deleted,
already_deleted, not_found }` is count-only, so DELETE RETURNING emits the
pre-delete candidate rows **only** on the exact all-deleted receipt
(`requested == candidate_count && deleted == candidate_count &&
already_deleted == 0 && not_found == 0`); any partial/already-deleted/
not-found → internal-consistency error, no rows. Precise per-row DELETE
RETURNING under contention needs a future receipt carrying deleted identities.

**D13 Existing SQL wire only.** Current `ResultBatch` (Arrow IPC carries
schema) + `Complete`. **No** `ResultSchema` frame, message variant, or
protocol version. Empty results complete with `row_count 0`, no batches.

**D14 Coordinator-owned, non-fragmentable.** Existing distributed reads may
feed INSERT-SELECT / UPDATE / DELETE candidate discovery; RETURNING projection
+ receipt validation stay local to the coordinator. No worker-local write-back.

**D15 No Wendelon / worker change.** No Wendelon wire/descriptor/storage
primitive, worker task spec, or worker write-back.

**D16 Existing budget surface.** No RETURNING-specific memory/config knob;
temporary RETURNING batches ride the admitted query's existing resident-memory
posture (use existing task/query memory checks if needed).

**D17 Prior posture stays.** ADR 054 descriptor-evolved write/delete
invalidate-no-retry; ADR 063 UPDATE stays WHERE-required / PK-only / no-PK-
mutation / no-FROM-OR-no-WHERE-subquery-RHS / per-row-not-transactional.

**D18 CTAS unchanged.** CTAS keeps its current Complete-only success shape;
CTAS row-count cleanup is a separate future ADR.

## Architecture Guards (24 + 3 convention + meta)

`adr064_sqlparser_054_returning_surface_is_dml_only`,
`adr064_insert_values_insert_select_update_delete_accept_returning`,
`adr064_ctas_returning_remains_absent`,
`adr064_returning_projection_is_toron_local`,
`adr064_returning_does_not_carry_sqlparser_ast_in_ir`,
`adr064_execution_schema_reflects_returning_projection`,
`adr064_returning_does_not_add_sql_wire_message_or_protocol_version`,
`adr064_result_batch_arrow_ipc_remains_the_schema_carrier`,
`adr064_returning_does_not_touch_wendelon_or_worker_transport`,
`adr064_returning_projection_is_validated_before_mutation`,
`adr064_returning_rows_emit_only_after_storage_success`,
`adr064_complete_row_count_matches_storage_receipt_and_returned_rows`,
`adr064_no_new_pk_gate_for_insert_or_delete_returning`,
`adr064_update_returning_inherits_pk_update_contract`,
`adr064_delete_returning_fails_closed_on_partial_receipt`,
`adr064_delete_returning_uses_pre_delete_rows_without_identity`,
`adr064_insert_returning_uses_inserted_target_rows`,
`adr064_insert_select_returning_uses_target_shaped_payload`,
`adr064_update_returning_uses_post_update_rows`,
`adr064_returning_star_hides_storage_synthetic_columns`,
`adr064_returning_rejects_aggregate_window_subquery`,
`adr064_descriptor_evolved_still_invalidates_without_retry`,
`adr064_prior_returning_absence_guards_are_retargeted`,
`adr064_no_returning_specific_memory_or_config_knob`.
**Convention:** `adr064_nilket_root_stays_runtime_free`,
`adr064_fixture_worker_source_unchanged`,
`adr064_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr064_prior_adr030_to_063_guards_continue_to_pass_except_retarget`.

## Prior-Guard Reconciliation (the run's largest retarget set)

RETURNING was rejected in **three** places; 064 flips all three and narrows
the absence guards, **self-enforcing** (support-present + stale-reject-gone,
the 063 pattern):
- `adr063_update_rejects_unsupported_clauses` → keeps FROM/OR/no-WHERE
  rejects, no longer rejects RETURNING.
- `adr063_sql_response_is_complete_only_no_returning` → non-returning DML
  Complete-only **plus** returning DML ResultBatch.
- `adr062_returning_surface_remains_absent` → narrowed to "no CTAS RETURNING;
  no Wendelon/worker/wire/parser change".
- `adr062_update_surface_is_present_under_pk_update_contract_after_adr063` →
  drops only its old UPDATE-RETURNING-reject assertion.
- INSERT/UPDATE/DELETE RETURNING reject fixtures (`insert_test.rs`/
  `update_test.rs`/`delete_test.rs`) → support fixtures; ADR 030/032/036
  RETURNING-absence text guards → support-present.

## Integration Fixtures (24)

- `planner_accepts_insert_values_returning_columns`
- `planner_accepts_insert_select_returning_star`
- `planner_accepts_update_returning_star`
- `planner_accepts_delete_returning_columns`
- `planner_rejects_ctas_returning_or_parser_has_no_surface`
- `binder_returning_rejects_unknown_column`
- `binder_returning_rejects_storage_synthetic_column`
- `binder_returning_rejects_aggregate_window_subquery`
- `insert_values_returning_emits_inserted_rows`
- `insert_select_returning_preserves_target_reorder_and_coercion`
- `insert_returning_works_on_append_only_table`
- `update_returning_emits_post_update_values`
- `update_returning_still_rejects_append_only_table`
- `delete_returning_emits_pre_delete_values_on_append_only_table`
- `delete_returning_emits_pre_delete_values_on_pk_table`
- `delete_returning_hides_appended_row_identity`
- `delete_returning_fails_closed_on_partial_receipt`
- `returning_projection_failure_happens_before_write`
- `write_failure_emits_no_returning_rows`
- `delete_failure_emits_no_returning_rows`
- `descriptor_evolved_returning_invalidates_without_retry_and_no_rows`
- `sql_listener_dml_returning_sends_result_batches_then_complete`
- `zero_row_update_delete_returning_completes_row_count_zero`
- `non_returning_insert_update_delete_remain_complete_only`

## Out of Scope

CTAS RETURNING / CTAS row-count correction, parser upgrade, new SQL wire
frames, Wendelon changes, worker-local write-back, transaction / rollback /
lock / CAS / lost-update detection, `old.`/`new.` namespaces, qualified target
aliases, no-WHERE UPDATE, PK mutation, and precise per-row DELETE RETURNING
under concurrent tombstone races.

## Council History

iter-0: 3-of-3 on DML RETURNING / CTAS out / result-protocol reuse. iter-1:
Claude verified DELETE candidate scan already reads full rows (no extension —
conceding the iter-0 misread) + pushed **not-PK-gated**; standard visibility +
emit-after-success agreed. iter-2: Claude grounded **`DeleteRowsOutcome`
count-only → DELETE fail-closed** and corrected Gemini's residual DELETE-PK-
only; Codex sharpened **precompute+validate before the mutation** + the exact
all-deleted receipt. iter-3/4: promotion shape; the `ReturningProjection::Star`
bind-time expansion + the no-memory-knob guard locked. The run's largest
prior-guard retarget set.

## Promotion criteria met

D-set 18, 3-of-3; 24 + 3 convention + meta guards; the largest retarget set
flipped self-enforcing; 24 fixtures; Toron SQL surface; reuses the SELECT
result path + each mutation's existing row materialization; no Wendelon/wire/
worker/parser/storage-primitive change. Migration:
`plans/impl/064_dml_returning.md`.

## Conclusion

DML RETURNING completes the mutation result surface — INSERT/UPDATE return
post-mutation rows, DELETE returns pre-delete rows — with receipt-gated
emission (project+validate before, emit after success), reusing the SELECT
result path and each mutation's existing materialization. No new wire,
storage, Wendelon, worker, or config surface. The "First Query with Integrity"
DML story (SELECT/INSERT/DELETE/UPDATE/CTAS + RETURNING) is now complete.
