# Impl Plan 064: DML `RETURNING` Rows

**Status:** Implemented, paired with `plans/adr/064_dml_returning.md`.
Evidence: `plans/impl/064_dml_returning_evidence.md`.
**Posture:** Toron SQL/planner/binder/coordinator/listener. Reuses each
mutation's existing row materialization + the SELECT result path; **no new SQL
wire message, storage primitive, Wendelon/worker change, parser upgrade, or
memory/config knob.** Six phases (Phase 0 audit plus five implementation
phases). Novel piece: the returning IR + the precompute-validate-before-
mutation / emit-after-success coordinator flow.

## Invariants carried in

- ADR 030/032/036/054/062/063 unchanged **except** the retargeted RETURNING-
  absence/reject guards (the run's largest retarget set). `crates/nilket`
  runtime-free; fixture worker unchanged; no-`*V1`. CTAS unchanged.

## Phase 0 — Audit (verified anchors)

- The **three RETURNING rejects** to flip: `sql_planner.rs:730` UPDATE
  (`returning` `:718`), `:774` DELETE (`:763`), `:907` INSERT (`:863`).
- **`sql_plan_returns_rows`** (daemon.rs:981) bifurcates: `returns_rows` →
  `ResultBatch` (`:838`); else `sql_affected_row_count` → `Complete`
  (`:855/:858`).
- **DML logical IR** (ir.rs): `Delete :246`, `Update :250`, `Insert :255`,
  `InsertSelect :260` (+ execution variants) — add `returning`.
- **Materialize seams** (coordinator/write.rs): `materialize_insert_batch`,
  `materialize_insert_select_batch`, `materialize_update_select_batch`;
  DELETE's `derive_delete_candidate_plan` (coordinator.rs:1168) scans full
  schema + `RowIdentityProjection::Append` (candidate batches = full rows +
  appended identity). `DeleteRowsOutcome { requested, deleted, already_deleted,
  not_found }` count-only (storage.rs:113).
- `ResultBatch`/`Complete`/`Error` in `sql_wire.rs` — no new frame.

## Phase 1 — Planner (sql_planner.rs)

- Replace the three `if returning.is_some() { Err(unsupported(...)) }` with
  lowering `Vec<SelectItem>` → `ReturningProjection::Star` |
  `ReturningProjection::Exprs { exprs, output_names }` (parser-independent).
  Preserve all unrelated rejects (UPDATE FROM/OR/no-WHERE; DELETE USING/ORDER
  BY/LIMIT/alias/multi-target; INSERT ON CONFLICT/alias). CTAS keeps no
  RETURNING.

## Phase 2 — IR + binder (ir.rs, ir/execution.rs, binder.rs)

- Logical `ReturningProjection` (unbound enum) on Insert/InsertSelect/Update/
  Delete; bound `BoundReturningProjection { exprs: BoundExprProgram,
  output_schema: SchemaRef }` on execution variants. `ExecutionPlan::schema()`
  returns the returning output schema for DML-with-returning, else the count/
  empty schema.
- Binder binds against the **target user schema**; `Star` expands to user
  columns in target order (excludes storage-synthetic/identity); rejects
  aggregate/window/subquery, unknown, synthetic columns. **DELETE** binds the
  user schema, not the identity-appended candidate schema.

## Phase 3 — Coordinator (coordinator.rs, coordinator/write.rs)

Per verb — **materialize → project+validate RETURNING (before write) → write/
delete → verify receipt → emit only on success**:
- **INSERT VALUES:** `materialize_insert_batch` → project → write →
  `WriteOutcome.rows == returned` → emit.
- **INSERT SELECT:** `materialize_insert_select_batch` (reorder/coerce) →
  project → write → verify → emit.
- **UPDATE:** 063 source projection → `materialize_update_select_batch` →
  project (post-update) → write → verify → emit.
- **DELETE:** identity-appended candidate plan → split user columns from the
  appended identity → project RETURNING over user columns (pre-delete) →
  `StorageDeleteRows` → **require exact all-deleted receipt** (`requested ==
  candidate_count && deleted == candidate_count && already_deleted == 0 &&
  not_found == 0`) else internal-consistency error → emit.
- Zero rows → no batches + `Complete { row_count: 0 }`. ADR 054 descriptor-
  evolved → invalidate, no retry, no rows.

## Phase 4 — SQL listener (daemon.rs)

- `sql_plan_returns_rows` true for DML plans with `returning.is_some()`;
  returning DML reuses `ResultBatch` emission + row-count summing; non-
  returning DML keeps `sql_affected_row_count`.

## Phase 5 — Guards + retargets + fixtures + evidence + roadmap

- 24 architecture + 3 convention + meta guards (ADR list), incl.
  `adr064_no_returning_specific_memory_or_config_knob`. **Largest retarget
  set:** `adr063_update_rejects_unsupported_clauses`,
  `adr063_sql_response_is_complete_only_no_returning`,
  `adr062_returning_surface_remains_absent`,
  `adr062_update_surface_is_present_under_pk_update_contract_after_adr063`, the
  INSERT/UPDATE/DELETE RETURNING reject fixtures, and ADR 030/032/036
  RETURNING-absence guards → support-present + stale-reject-gone (self-
  enforcing). Meta scoped `…_except_retarget`.
- 24 fixtures (ADR list), incl. DELETE-fail-closed-on-partial-receipt + the
  append-only INSERT/DELETE RETURNING cases.
- `plans/impl/064_dml_returning_evidence.md`: `architecture_test adr064`, full
  `architecture_test`, `cargo test -p toron` (planner/binder/coordinator/
  insert/update/delete suite counts), `clippy -p toron --all-targets -- -D
  warnings`, `git diff --check` (capped-linker procedure).
- Roadmap: 064 promoted — mutation result surface complete.

## Watch-items

1. **Precompute + validate RETURNING BEFORE the mutation** (projection errors
   fail early, no half-done write); **emit only after** storage success.
2. **DELETE fail-closed** — exact all-deleted receipt (`requested ==
   candidate_count && deleted == candidate_count && already_deleted == 0 &&
   not_found == 0`); else internal-consistency error (DeleteRowsOutcome is
   count-only).
3. **Not PK-gated** — INSERT/DELETE on append-only + PK; UPDATE PK-only
   because UPDATE is.
4. **`RETURNING *` hides storage-synthetic/identity** (DELETE's appended id).
5. **Standard visibility** — POST for INSERT/UPDATE, PRE for DELETE.
6. **Reuse SELECT result path** — no new wire frame; Arrow IPC carries schema;
   **no new memory/config knob** (rows already materialized; existing budget).
7. **Toron-local returning IR** — no sqlparser AST in plans.
8. **Row count must not lie** — `Complete.row_count` == receipt == emitted.
9. **Largest retarget set** — flip three RETURNING-rejects + narrow absence
   guards; self-enforcing; meta `…_except_retarget`.
10. **No Wendelon/worker/wire/parser change; CTAS unchanged.**

## Acceptance checklist

- [x] Phase 1: flip the 3 rejects → `ReturningProjection::{Star,Exprs}`;
      preserve unrelated rejects; CTAS no-RETURNING.
- [x] Phase 2: logical + bound returning IR; `schema()` returns returning
      output schema; binder target-user-schema, `Star` expansion (hides
      synthetic), reject aggregate/window/subquery.
- [x] Phase 3: per-verb materialize → project-before-write → write/delete →
      receipt-verify (DELETE exact all-deleted) → emit-after-success; zero-row
      → Complete{0}; 054 invalidate-no-retry.
- [x] Phase 4: `sql_plan_returns_rows` true for returning DML; reuse
      ResultBatch+Complete; row_count = emitted.
- [x] Phase 5: 24+3+meta guards (incl. no-knob); largest retarget set self-
      enforcing; 24 fixtures; evidence + roadmap; no CTAS / Wendelon / worker /
      wire / parser / new-PK-gate / new-knob drift.
