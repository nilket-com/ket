# ADR 061: CTAS `IF NOT EXISTS` with Race-Safe Catalog Create Outcome

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** the SQL-surface-widening pivot after the 049–060 governance/
cancellation arc. ADR 061 candidates were audit (045 D7), `UPDATE`, and CTAS
`IF NOT EXISTS`. The lane converged 3-of-3 (Gemini withdrew audit); the lead
resolved to **CTAS `IF NOT EXISTS`** on the decisive verified fact that
Toron-created tables are **append-only / no-PK** (`catalog.rs:1443/1609`;
`adr036`; PK upsert is bootstrapped-PK-only, `pk_delete.rs`), so
`UPDATE`-by-PK-supersession can't serve the tables users create — it needs a
table-identity-policy Phase 0 (→ ADR 062). Gemini conceded UPDATE→CTAS at
iter-4 — clean 3-of-3.
**Category:** Toron compute-ring — SQL surface / coordinator DDL. Coordinator-
owned, non-fragmentable; no Wendelon server/wire change, no worker transport,
no SQL wire change, no new storage primitive.

## Summary

`CREATE TABLE IF NOT EXISTS <name> AS SELECT …` is rejected today
(`sql_planner.rs:911`). 061 accepts it. The substance is the
**race-correctness cut**, not parser polish: CTAS executes the source
**before** creating the target (`coordinator.rs:1013→1018`, currently an
unconditional `create_table(…, false)` + write), so a compatible target can
**race in** between source materialization and create. The fix is a catalog
**created-vs-existing outcome** (`CreateTableOutcome { binding, created }`) —
CTAS **writes payload rows only when it created the table**. `IF NOT EXISTS`
suppresses *row execution/write* when the target exists but **never masks
source bind/schema errors** (bind-always). Reuses
`catalog.create_table(if_not_exists)`'s existing exact-schema compatible →
binding / incompatible → `SchemaMismatch` semantics (`catalog.rs:431–460`),
and reconciles the **Wendelon-backed** create race.

## Decisions (16, Promotion-Locked)

**D1 Placement.** Toron SQL/coordinator only. No new Wendelon API, worker
protocol, SQL wire frame, Control-Ring RPC, storage primitive, or distributed
DDL protocol.

**D2 Accepted SQL.** Accept `CREATE TABLE IF NOT EXISTS [schema.]name AS
SELECT …`.

**D3 Conflict still rejected.** `CREATE OR REPLACE TABLE IF NOT EXISTS … AS
SELECT …` stays rejected with the existing mutual-exclusion diagnostic
(`sql_planner.rs:905`).

**D4 CTAS column lists still rejected.** Explicit CTAS column lists remain out
of scope; SELECT aliases remain the supported naming path.

**D5 IR flag.** `LogicalPlan::CreateTableAsSelect` and
`ExecutionPlan::CreateTableAsSelect` carry `if_not_exists: bool`.

**D6 Bind-always.** Parser/planner/binder/schema-inference run regardless of
target existence. `IF NOT EXISTS` does **not** mask malformed SQL, missing
source tables, duplicate CTAS output names, unsupported output types, or
binding errors. (Deterministic static validation — deliberately *not*
PostgreSQL's "skip the whole statement"; Toron treats a statement as one
checked unit.)

**D7 Pre-execution skip (optimization).** When `if_not_exists` is true, the
coordinator resolves the target through the normal catalog path **before**
source execution; a **compatible existing target returns empty success
without reading source rows**.

**D8 Exact compatibility.** Compatibility is **exact Arrow schema equality**
(matching schema-only `CREATE TABLE IF NOT EXISTS`); incompatible existing
target → `SchemaMismatch`.

**D9 Created outcome.** Catalog create exposes `CreateTableOutcome { binding,
created }` (`create_table_with_outcome`, or equivalent); `create_table`
remains a convenience wrapper.

**D10 Write gate (the race fix).** CTAS writes payload rows **only when
`created == true`**; `created == false` → empty success without writing.

**D11 Race reconciliation (local).** Compatible target appearing after source
execution but before create → empty success, no write. Incompatible →
`SchemaMismatch`.

**D12 Wendelon-backed create race.** Under `if_not_exists`, a **Wendelon
schema-create rejection** that may mean "target now exists remotely" must
trigger current-descriptor resolution/sync: current schema matches →
`created = false`; differs → `SchemaMismatch`; no current descriptor provable
→ surface the original admin error. (The race is at the Wendelon descriptor
layer, not only the local catalog.)

**D13 Write-failure semantics.** Unchanged when this execution creates the
table; existing/raced-in targets attempt no CTAS write.

**D14 Result shape.** Empty success — no row count, created flag, or
`RETURNING`.

**D15 Distribution.** Coordinator-owned and non-fragmentable; distributed
source reads may be used as today, but CTAS mutation/write stays in the
coordinator mutation path (no worker-local write-back).

**D16 Carried work.** `UPDATE`, mutation `RETURNING`, audit, parser
throttling, OTLP traces, typed `QUERY_CANCELLED`, Wendelon legacy
cancellation — separate candidates.

## Architecture Guards (11 + 3 convention + meta)

1. `adr061_ctas_if_not_exists_is_accepted`
2. `adr061_ctas_ir_carries_if_not_exists`
3. `adr061_or_replace_if_not_exists_remains_rejected`
4. `adr061_ctas_column_list_remains_rejected`
5. `adr061_catalog_create_exposes_created_outcome`
6. `adr061_ctas_write_is_gated_on_created_outcome`
7. `adr061_ctas_binds_source_before_existing_target_skip`
8. `adr061_ctas_uses_exact_schema_compatibility`
9. `adr061_ctas_remains_coordinator_owned_non_fragmentable`
10. `adr061_no_wendelon_wire_worker_or_sql_protocol_change`
11. `adr061_retargets_prior_ctas_if_not_exists_reject_guards`

**Convention:** `adr061_nilket_root_stays_runtime_free`,
`adr061_fixture_worker_source_unchanged`,
`adr061_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr061_prior_adr033_to_060_guards_continue_to_pass_except_retarget`.

**Prior-guard reconciliation:** ADR 033 keeps the CTAS explicit-column-list
rejection but drops the IF-NOT-EXISTS-unsupported claim; ADR 041 flips from
CTAS `IF NOT EXISTS` rejection to positive support, preserving the OR-REPLACE
conflict. The existing `ctas_if_not_exists_remains_rejected` fixture is
retargeted to the positive behavior.

## Integration Fixtures (12 required)

1. `planner_accepts_create_table_if_not_exists_as_select`
2. `planner_rejects_create_or_replace_if_not_exists_ctas`
3. `planner_still_rejects_ctas_explicit_column_list`
4. `ctas_if_not_exists_binder_surfaces_source_errors_when_target_exists`
5. `ctas_if_not_exists_compatible_existing_target_skips_source_execution`
6. `ctas_if_not_exists_incompatible_existing_target_returns_schema_mismatch`
7. `ctas_if_not_exists_absent_target_creates_and_writes_like_current_ctas`
8. `ctas_if_not_exists_compatible_raced_in_target_succeeds_without_writing`
9. `ctas_if_not_exists_incompatible_raced_in_target_returns_schema_mismatch`
10. `distributed_ctas_if_not_exists_existing_target_does_not_request_workers`
11. `catalog_create_outcome_reports_created_true_and_false`
12. `wendelon_catalog_if_not_exists_recovers_compatible_remote_create_race`

## Out of Scope

- `UPDATE` and primary-key DDL (→ ADR 062).
- `RETURNING` / row count / created flag / created-vs-existing SQL output.
- CTAS explicit column lists; schema coercion / partial compatibility.
- Worker-local CTAS write-back or new distributed mutation protocol.
- Wendelon server/client wire changes; audit subsystem.

## Council History

- iter-0: 3-way divergence — Claude `UPDATE`, Codex CTAS, Gemini audit.
- iter-1: Gemini withdrew audit; Claude conceded `UPDATE` isn't proven-small
  and endorsed CTAS; Codex held CTAS.
- iter-2: decisive fact — Toron tables are **append-only / no-PK**, so
  `UPDATE`-by-PK-supersession can't serve them (→ 062); Codex added the
  distributed-mutation finding (workers run read-only; coordinator owns the
  write).
- iter-3/4: Codex finalized D1–D16 (added D4 column-list + D12 Wendelon-create
  race); **Gemini conceded UPDATE→CTAS at iter-4 — 3-of-3.**

## Carried to ADR 062 (`UPDATE`)

Phase 0 must choose the **table-identity policy** (bootstrapped-PK-only /
append-only delete-plus-insert / add PK create-table DDL) and lock
**coordinator-owned rewrite vs worker-local write-back**, plus SET-can't-
modify-PK, strip storage-metadata, reject content-hash, no RETURNING/FROM/
subquery-RHS/transaction.

## Promotion criteria met

D-set 16, 3-of-3; 11 + 3 convention + meta guards; `adr033`/`adr041`
retargeted + the `ctas_test` fixture retargeted (prior-guard reconciliation);
12 fixtures; Toron-ring, coordinator-owned, non-fragmentable; no Wendelon
server/wire or worker-protocol change; reuses existing catalog IF-NOT-EXISTS
semantics.

## Conclusion

CTAS `IF NOT EXISTS` retires a long-carried SQL gap with a small but real
correctness cut — the race-safe `created` outcome (local *and* Wendelon-
backed) plus bind-always diagnostics — on exactly the append-only tables Toron
creates, and leaves `UPDATE` scoped and ready as ADR 062. Migration:
`plans/impl/061_ctas_if_not_exists.md`.
