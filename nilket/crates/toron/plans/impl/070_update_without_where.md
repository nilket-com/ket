# Impl Plan 070: Toron `UPDATE` without `WHERE` (PK-table)

**Status:** Promoted, paired with `plans/adr/070_update_without_where.md`.
**Posture:** ADR-015 cycle move — drop the planner's `WHERE`-required reject,
make `LogicalPlan::Update.predicate` `Option<Expr>` (mirroring `Delete`), bind
`None` to a full-table unfiltered scan through the **existing** coordinator write
path. Toron-only; no new execution machinery; every 063 invariant preserved.
Five phases.

## Invariants carried in

- **Absent IS absence** (no synthetic `WHERE true`, no PBC, no `Filter`, no
  appended row identity). PK-table gate + PK immutability + per-row-supersession
  atomicity + simultaneity (063); RETURNING + INSERT-SELECT coercion/nullability
  lattice + NULL→non-nullable-fails-before-write + descriptor-evolution
  (064/063); **coordinator-owned, non-fragmentable** (worker `Update` arms stay
  `NotImplemented`); ADR 036 DELETE no-`WHERE` behavior-equivalent; **no safety
  knob**; **no Wendelon/storage/wire/descriptor change**; only IR change is
  `Update.predicate: Option<Expr>`; profile hint is the existing
  `QueryProfileHint::Scan` (no new `FullTable`); `nilket` runtime-free.

## Phase 0 — Audit (verified anchors)

- `sql_planner.rs:738-741 lower_update` — the `ok_or_else("UPDATE requires a
  WHERE clause")` reject; **mirror** `lower_delete:837-840`
  `.map(lower_expr).transpose()?`.
- `ir.rs:260 LogicalPlan::Update.predicate: Expr` → `Option<Expr>` (Delete is
  already `Option`).
- `binder.rs:471 bind_update`; `scan_plan(binding, None)` (`:923`) ⇒
  `RowIdentityProjection::None` (`:931`); `scan_profile_hint` (`PointRead`
  PK-equality `:953`, else `Scan`); PK-mutation reject in this fn.
- `coordinator.rs:962 execute_write_plan_with_source` (callers `:390/419/1469`) —
  reused unchanged. `exec.rs:622/787 Update {..}` arms field-agnostic
  `NotImplemented` — unchanged (non-fragmentable).
- **Reconciliation (verified — reject string appears 3×):** `update_test.rs:67
  planner_rejects_update_without_where`; arch guards `adr062_..._present...`
  (`:13916`), `adr063_update_requires_where...` (`:14098`),
  `adr063_logical_update_shape_is_name_based` (`:14198`),
  `adr063_no_transaction_lock_compare_swap_or_no_where_surface` (`:14438`).

## Phase 1 — IR + planner

- `ir.rs`: `Update.predicate: Expr → Option<Expr>`; fix destructure sites (exec
  arms are `{ .. }`, unaffected; planner/binder are the real consumers).
- `sql_planner.rs:lower_update`: replace the `ok_or_else` reject with
  `selection.as_ref().map(|e| self.lower_expr(e)).transpose()?`. Keep the
  exhaustive `Statement::Update` destructure + all other rejects + RETURNING.

## Phase 2 — Binder (`Option<&Expr>` branch)

- `bind_update(predicate: Option<&Expr>)`. **Bind the gates first** (PK-table,
  storage-synthetic/duplicate target, PK-assignment reject) — before the branch.
- `let mut scan = scan_plan(binding.clone(), None);` then:
  - `Some(p)`: `bind_expr_program` → `require_single_boolean_root(.., "UPDATE
    WHERE")` → `scan_profile_hint(..).unwrap_or(Scan)` → `apply_scan_profile_hint`
    → optional `compile_pbc`/`attach_pbc_program` (`FilterDisposition`) →
    `ExecutionPlan::Filter{..}` as the project input (unchanged 063 path).
  - `None`: project input = `scan` (no Filter, no PBC), `QueryProfileHint::Scan`.
- `bind_expr_program(&source_exprs, &project_input.schema(), ..)` → full-row
  `Project`; return the existing `ExecutionPlan::Update` with that `source`. **Do
  not change** `ExecutionPlan::Update`, `materialize_update_select_batch`,
  `verify_write_returning_receipt`, descriptor invalidation, or worker rejection.

## Phase 3 — Coordinator / exec (reuse, no change)

- `execute_write_plan_with_source` executes `source`, materializes (coercion +
  nullability lattice), writes via `StorageWrite`; zero batches ⇒ no write,
  `affected_count_batch(0)`; whole-table source uses the existing query/task
  budgeted source-execution paths. Worker `Update` arms stay `NotImplemented`
  (non-fragmentable). No shape change.

## Phase 4 — Guard reconciliation (five sites — retarget, don't delete)

1. `update_test.rs:67` → `planner_accepts_update_without_where` (`LogicalPlan::
   Update`, `predicate.is_none()`, assignments lowered); assert
   `planner_accepts_update_where` predicate is `Some(Eq)`; add `_returning`.
2. `adr062_..._present...` (`:13916`) → drop the `"UPDATE requires a WHERE
   clause"` assertion; keep UPDATE-surface + PK-contract.
3. `adr063_update_requires_where...` (`:14098`) → 070 symmetry guard (`lower_update`
   contains `.map(|expr| self.lower_expr(expr))` + `.transpose()?`, not the reject
   string; `lower_delete` unchanged).
4. `adr063_logical_update_shape_is_name_based` (`:14198`) → `predicate:
   Option<Expr>` (retain no-Wendelon-types).
5. `adr063_no_transaction_lock...no_where_surface` (`:14438`) → keep the
   transaction/lock/CAS/retry/`StorageDeleteRows` assertions; drop the
   no-`WHERE`-surface assertion.

## Phase 5 — Guards, fixtures, evidence

- 17 architecture + 3 convention/meta guards (predicate-optional,
  accepts-no-where, lowers-none, not-true-literal,
  project-over-scan-without-filter, scan-no-append-identity,
  profile-scan, where-point-read-still-works, requires-pk, preserves-pk-
  immutability, returning-composes, coordinator-source-bearing,
  non-fragmentable-worker-rejected, delete-unchanged, no-safety-knob,
  no-Wendelon-change, the meta + convention).
- 13 fixtures in `tests/update_test.rs` (extend, don't fork). If
  `update_source_scan` assumes `Project→Filter→Scan`, add a sibling accepting
  `Project→Scan` and assert the no-filter shape.
- `plans/impl/070_update_without_where_evidence.md`: `cargo test -p toron --test
  update_test`, `architecture_test adr070`/`adr062`/`adr063`/`adr064`, full
  `cargo test -p toron`, `cargo check -p toron` feature matrix, `clippy -D
  warnings`, `git diff --check` (record any disk-limited partials).

## Watch-items

1. **Absent ≠ `WHERE true`** — the `None` branch adds no `Filter`/literal/PBC;
   guarded by source-scan.
2. **`RowIdentityProjection::None` + `Scan` profile** (`scan_plan(binding, None)`);
   the WHERE PK-equality PointRead must stay; profile hint is `Scan`, not a new
   `FullTable`.
3. **Gates before the branch** — PK-table + PK-immutability reject on both forms.
4. **Five-site reconciliation** — reject string asserted 3×; retarget all, don't
   delete the surviving invariants.
5. **Non-fragmentable** — worker `Update` arms stay `NotImplemented`; whole-table
   materialization uses the existing query/task budgeted source-execution paths.

## Acceptance checklist

- [ ] Phase 1: `Update.predicate: Option<Expr>`; `lower_update` accepts `None`
      (mirrors `lower_delete`); no synthetic true.
- [ ] Phase 2: `bind_update(Option<&Expr>)`; `None` ⇒ `Project→Scan`
      (`RowIdentityProjection::None`, `Scan` profile), no Filter/PBC; gates first.
- [ ] Phase 3: coordinator path + affected-count reused unchanged; workers reject.
- [ ] Phase 4: all five 062/063 + `update_test` sites retargeted (not deleted).
- [ ] Phase 5: 20 guards + 13 fixtures + evidence; 063 WHERE + ADR 036 DELETE
      suites unchanged.
