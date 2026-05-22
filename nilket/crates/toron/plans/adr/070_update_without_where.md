# ADR 070: Toron `UPDATE` without `WHERE` (PK-table symmetry restoration)

**Status:** Promoted (plan) — **3-of-3 (Codex, Gemini, Claude)**, the cleanest
convergence of the cycle (no substantive divergence at any iteration).
**Seed:** ADR 063 implemented `UPDATE … WHERE` on PK tables but **deliberately
required `WHERE`** to bound risk; ADR 036 (`DELETE … WHERE`) already lowers a
missing predicate as `None` (all rows). 070 restores the symmetry — an **ADR-015
cycle move** (remove one reject + mirror the existing path) — pure composition
over 063 (UPDATE), 064 (RETURNING), and 036 (DELETE no-`WHERE`).
**Category:** Toron compute-ring DML. No Wendelon / storage / wire / descriptor
change.

## Summary

Accept `UPDATE table SET col = expr [RETURNING …]` (no `WHERE`) on **primary-key
tables**, updating every visible target row. Narrow composition:
`LogicalPlan::Update.predicate` becomes `Option<Expr>`; a missing `WHERE` is
`None` (never a synthetic `WHERE true`); `None` binds to `Project → Scan` with no
`Filter`, no PBC, no appended row identity, and `QueryProfileHint::Scan`. The
`WHERE` form remains behaviorally unchanged (ADR 063, incl. PK-equality
`PointRead`). UPDATE stays PK-table-only, PK-immutable, coordinator-owned,
non-fragmentable, and per-row atomic via Wendelon PK supersession; ADR 064
RETURNING composes.

## The asymmetry (grounded)

- **`lower_update`** (`sql_planner.rs:738-741`): `predicate = selection.as_ref()
  .ok_or_else(|| unsupported("UPDATE requires a WHERE clause")).and_then(…)?` →
  `Update { predicate: Expr }` — **required**.
- **`lower_delete`** (`sql_planner.rs:837-840`): `.map(|e| self.lower_expr(e))
  .transpose()?` → `Option<Expr>` — no-`WHERE` = all rows.

070 makes UPDATE match DELETE.

## Decisions (15, Promotion-Locked)

**D1 Placement.** Toron planner, logical IR, binder, tests, docs only.
`ExecutionPlan::Update` stays source-bearing and unchanged in shape.

**D2 Accepted SQL.** `UPDATE t SET … [RETURNING …]` with and without `WHERE`. All
063/064 unsupported UPDATE surface stays rejected: `FROM`, `OR`, joins, aliases,
table args/hints/versions/ordinality/partitions/json-path/sample, tuple/qualified
targets, empty/duplicate assignments, aggregate/window/subquery RHS, parser-level
`OUTPUT`/`ORDER BY`/`LIMIT`, and the sqlparser upgrade.

**D3 IR: `LogicalPlan::Update.predicate: Expr → Option<Expr>`** (`ir.rs:260`),
mirroring `LogicalPlan::Delete`.

**D4 Absent IS absence.** A missing `WHERE` is **not** lowered as a literal-true
predicate: the `None` branch binds no `Expr::Literal`, compiles no PBC, attaches
no PBC program, creates no `ExecutionPlan::Filter`, and infers no point-read
accounting. (Avoids false PBC/profile drift; preserves the ADR 036 precedent.)

**D5 Planner lowering.** `lower_update` removes the `:740` `"UPDATE requires a
WHERE clause"` reject and lowers `selection` with the DELETE-shaped
`.map(|e| self.lower_expr(e)).transpose()?`. All other clause/target/assignment
rejects and RETURNING lowering unchanged.

**D6 Binder gates before the branch.** `bind_update` takes `Option<&Expr>`;
table resolution, PK-table validation, RETURNING binding, assignment-target
resolution, storage-synthetic rejection, duplicate-target rejection, and
PK-assignment rejection all happen **before** the `Some`/`None` source branch.

**D7 `WHERE` branch unchanged (063).** `Some(p)`: bind one boolean root, compute
`scan_profile_hint` (preserving PK-equality `PointRead`, `binder.rs:953`), attach
PBC when possible, wrap `Filter`, project the full replacement row.

**D8 No-`WHERE` branch.** `None` builds the guarded source shape:
`Project(full target cols; SET substituted)` over `Scan(target, all user cols,
row_identity = RowIdentityProjection::None, profile_hint = QueryProfileHint::Scan)`
(`scan_plan(binding, None)`, `binder.rs:923/931`) — **no `Filter`, no PBC, no
literal-true, no appended row identity**. The profile hint is the **existing
`QueryProfileHint::Scan`, not a new `FullTable` variant** (Codex iter-3).

**D9 PK contract unchanged (063).** Append-only / no-PK / unsupported-identity
tables still reject `"UPDATE requires a PRIMARY KEY table"`. SET cannot target the
PK column. No-`WHERE` changes the *candidate set*, not the identity contract.

**D10 Simultaneity.** SET expressions read the **original** scanned row and apply
simultaneously (`SET a=b, b=a` swaps for every affected row).

**D11 RETURNING (064) + zero-row.** No-`WHERE` `UPDATE … RETURNING` returns
post-update rows from the same materialized replacement payload written to
`StorageWrite`; receipt verification unchanged. Zero affected rows ⇒ no RETURNING
batches, row count 0, no write.

**D12 Coordinator path.** Reuse `execute_write_plan_with_source`
(`coordinator.rs:962`): execute `source`, materialize through the existing
UPDATE/INSERT-SELECT coercion + nullability lattice (NULL→non-nullable fails
before write), write via `StorageWrite`, report `outcome.rows`.
`DescriptorEvolved` invalidates and returns the error; no retry.

**D13 Atomicity + scale.** Per-row PK supersession, **not** statement-level
transactionality / rollback / lock / CAS / lost-update detection. A no-`WHERE`
UPDATE may materialize a full-table source on the coordinator through the
existing query/task budgeted source-execution paths; it remains
**non-fragmentable** (`ExecutionPlan::Update` worker arms stay `NotImplemented`,
`exec.rs:622/787`). Fragmentable bulk DML is a future ADR.

**D14 No operational safety surface.** No config flag, warning, confirmation
syntax, audit event, memory knob, or dialect switch for full-table UPDATE — the
063 PK-table gate is the load-bearing safety boundary.

**D15 DELETE unchanged.** ADR 036 DELETE no-`WHERE` stays behavior-equivalent; no
`"DELETE requires a WHERE clause"` diagnostic introduced.

## Prior-Guard Reconciliation (five sites — retarget, don't delete)

The reject string `"UPDATE requires a WHERE clause"` appears **3×** in the arch
test; five sites reference the no-`WHERE`/`predicate: Expr` surface and must be
retargeted (missing any breaks the build):
1. `tests/update_test.rs:67 planner_rejects_update_without_where` →
   `planner_accepts_update_without_where` (`LogicalPlan::Update`,
   `predicate.is_none()`).
2. `adr062_update_surface_is_present_under_pk_update_contract_after_adr063`
   (`:13916`) → keep UPDATE-surface + PK-contract; drop the reject-string assertion.
3. `adr063_update_requires_where_and_delete_semantics_unchanged` (`:14098`) → the
   **070 symmetry guard** (UPDATE + DELETE both lower a missing predicate to
   `None`; DELETE otherwise unchanged).
4. `adr063_logical_update_shape_is_name_based` (`:14198`) → expect `predicate:
   Option<Expr>` (retain the no-Wendelon-types invariant).
5. `adr063_no_transaction_lock_compare_swap_or_no_where_surface` (`:14438`) →
   keep the no-transaction/lock/CAS/retry/`StorageDeleteRows` assertions; drop the
   no-`WHERE`-surface assertion.
ADR 064's UPDATE-RETURNING guard keeps its meaning (now over the accepted
no-`WHERE` source).

## Architecture Guards (17 + 3 convention/meta)

`adr070_update_logical_predicate_is_optional`,
`adr070_planner_accepts_update_without_where`,
`adr070_planner_lowers_missing_update_selection_to_none`,
`adr070_update_without_where_is_not_lowered_as_true_literal`,
`adr070_update_without_where_builds_project_over_scan_without_filter`,
`adr070_update_without_where_scan_uses_no_append_identity`,
`adr070_update_without_where_profile_hint_is_scan`,
`adr070_update_where_pk_equality_point_read_profile_still_works`,
`adr070_update_without_where_still_requires_pk_table`,
`adr070_update_without_where_preserves_pk_immutability`,
`adr070_update_returning_composes_with_no_where`,
`adr070_update_remains_coordinator_owned_source_bearing_write`,
`adr070_update_remains_non_fragmentable_and_worker_rejected`,
`adr070_delete_no_where_semantics_unchanged`,
`adr070_no_transaction_lock_cas_retry_or_safety_knob`,
`adr070_no_wendelon_wire_storage_or_descriptor_change`,
`adr070_prior_adr062_to_064_guards_continue_to_pass_except_no_where_retarget`.
**Convention:** `adr070_nilket_root_stays_runtime_free`,
`adr070_fixture_worker_source_unchanged`,
`adr070_new_surface_is_brand_clean_and_has_no_v1_suffix`.

## Integration Fixtures (13)

`planner_accepts_update_without_where`(+`_returning`);
`binder_update_without_where_uses_full_scan_profile_no_filter`;
`binder_update_without_where_rejects_append_only_or_no_pk`;
`binder_update_without_where_rejects_primary_key_assignment`;
`update_all_rows_writes_full_table_replacement`;
`update_all_rows_zero_rows_table_is_no_op`;
`update_all_rows_respects_simultaneous_assignment` (`SET a=b, b=a`);
`update_all_rows_returning_emits_post_update_rows`;
`update_all_rows_type_coercion_matches_insert_select_policy`;
`update_all_rows_null_for_non_nullable_column_fails_before_write`;
`update_where_pk_equality_keeps_point_read_profile`;
`fragmenter_rejects_direct_update_without_where_fragmentation`;
existing ADR 063 `UPDATE … WHERE` + ADR 036 DELETE no-`WHERE` fixtures remain
green and behavior-equivalent.

## Out of Scope

Append-only/no-PK UPDATE; PK mutation; composite-PK; `UPDATE FROM`/joins/aliases/
tuple-targets/`OR`/`ORDER BY`/`LIMIT`; subquery/aggregate RHS; sqlparser upgrade;
write/delete trace carriers; fragmentable bulk DML; worker-local write-back;
Wendelon storage/wire changes; statement-level transactions; operational
warning/confirmation/audit policy.

## Council History

iter-0: 3 drafts, immediate convergence on `Option<Expr>` over a synthetic
literal-true. iter-1: absent-is-absence, full-scan/no-filter binding, `Scan`
profile, PK-only, RETURNING composition, coordinator-owned non-fragmentable;
Codex's dual added the precise `bind_update(Option<&Expr>)` branch +
`RowIdentityProjection::None` and the verified five-site guard reconciliation;
Claude's contributed the ADR-015 framing + per-row-atomicity honesty. iter-2:
locked the source-scannable no-predicate branch + the five-site retarget. iter-3
(Codex): resolved the last ambiguity — the profile hint is the existing
`QueryProfileHint::Scan`, not a new `FullTable`. No substantive fork at any point.

## Promotion criteria met

D-set 15; 17 + 3 convention/meta guards; 13 fixtures; pure composition (only
`Update.predicate: Option<Expr>`, no new execution machinery, no Wendelon/storage
change); every 063 invariant preserved + 064 RETURNING; the five-site guard
reconciliation specified. Migration: `plans/impl/070_update_without_where.md`.

## Conclusion

The ADR-015 cycle move: drop the `sql_planner.rs:740` reject, make
`LogicalPlan::Update.predicate` `Option<Expr>` mirroring `Delete`, and bind `None`
to a full-table unfiltered scan (`RowIdentityProjection::None`,
`QueryProfileHint::Scan`) through the existing coordinator write path —
absent-is-absence, every 063 invariant intact, RETURNING composing, the five
062/063 no-`WHERE` guards retargeted, Toron-only.
