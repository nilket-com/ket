# ADR 041: CREATE OR REPLACE TABLE

**Status:** Promoted
**Seed:** `b7e205a feat: adr 039 wendelon drop migration substrate` (the cross-ring DROP substrate this ADR composes against)
**Council:** Claude, Codex, Gemini (4 iterations: iter-0 split 1-vs-2, iter-1 triple-swap, iter-2 double-reversal, iter-3 final convergence)
**Category:** SQL composition ADR (over ADR 029 / 033 / 034 / 036 / 037 / 038 / 039)

## Summary

Lift the two `unsupported CREATE TABLE clause: OR REPLACE` planner rejects in `crates/toron/src/sql_planner.rs:671-672` (schema-only) and `:904-905` (CTAS). Compose the existing high-level DROP and CREATE paths into a destructive non-atomic replacement: resolve the existing target, drop it through the ADR 038/039 storage-admin path, then create the new target through the ADR 029/033 catalog path. CTAS replace materializes its SELECT source **before** any replacement drop, so a source failure leaves the existing target untouched.

The implementation adds a single `or_replace: bool` field to `LogicalPlan::CreateTable`, `LogicalPlan::CreateTableAsSelect`, and their `ExecutionPlan` counterparts. **No** `Sequence` IR, **no** new Wendelon protocol, **no** `TableSchemaReplaceRequest`, **no** cross-ring transaction substrate.

## Motivation

After three back-to-back substrate cycles (ADR 037 distributed-write-context, ADR 038 storage-admin-reclaim, ADR 039 Wendelon-DROP-migration) and one operator-surface cycle (ADR 040 unified Nilket CLI), the codebase needs a **composition cycle**: SQL surface area that exercises the cross-ring DROP substrate end-to-end. The two `OR REPLACE` rejects are the highest-evidence-pressure composition target — concrete, file:line-precise, operator-visible, and exactly the cycle shape the Prioritization philosophy favors ("narrow, evidence-driven, single-decision contracts").

`CREATE OR REPLACE TABLE` is a standard DDL idempotency idiom (operators expect "redeploy schema definitions" to work). It is also the smallest cycle that proves the ADR 039 Wendelon-DROP lift actually flows correctly when driven by higher-level SQL constructs rather than by a direct admin-RPC call.

## Decisions

**D1: Lift Schema-Only `CREATE OR REPLACE TABLE`.**
`CREATE OR REPLACE TABLE name (...)` is accepted. If the target is absent, execution is ordinary CREATE TABLE. If present, Toron drops the old table through the existing storage-admin path, removes the catalog binding, then creates the new table through the existing catalog path.

**D2: Lift CTAS `CREATE OR REPLACE TABLE ... AS SELECT`.**
`CREATE OR REPLACE TABLE name AS SELECT ...` is accepted. The SELECT source is evaluated and materialized **before** any replacement drop. If source materialization fails, the existing target remains untouched. After source materialization succeeds, Toron drops the old target if present, creates a fresh target, then writes the materialized payload. The ordering is load-bearing and is pinned by both an architecture guard and an integration fixture.

**D3: Defer CTAS `IF NOT EXISTS`.**
The `sql_planner.rs:897` reject `CTAS does not support IF NOT EXISTS in v1` stays. This is not an incidental parser-neighborhood cleanup; it is a distinct skip-source-on-existence idempotency contract belonging to a separate ADR.

**D4: `OR REPLACE` and `IF NOT EXISTS` Are Mutually Exclusive.**
If sqlparser admits both modifiers on a CREATE TABLE statement, Toron rejects with the stable diagnostic:

```text
CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS
```

This applies before either modifier's individual branch, including CTAS.

**D5: Replacement Is Fresh-Identity Creation.**
Replacement is destructive — not in-place schema evolution. The new table receives a fresh `TableObjectId`, fresh schema generation, and fresh physical descriptor fingerprint. Bound plans and storage requests carrying the old identity fail through existing descriptor/table-identity guards (preserving the ADR 029 / 033 / 038 / 039 identity invariants).

**D6: Replacement Is Non-Atomic.**
ADR 041 does not add cross-ring transactions. A failure between the old-table drop and the new-table create can leave the table absent. This failure window is documented and accepted for V1. Operators recover by re-running the `CREATE OR REPLACE` statement against current state; this is retryable DDL, not transactional snapshot replay.

**D7: Use Explicit Plan Flags, Not Sequence IR.**
Add `or_replace: bool` to:

```rust
LogicalPlan::CreateTable
LogicalPlan::CreateTableAsSelect
ExecutionPlan::CreateTable
ExecutionPlan::CreateTableAsSelect
```

**Do not** add `LogicalPlan::Sequence`, `ExecutionPlan::Sequence`, or any general multi-statement lowering/execution path. Introducing a transaction-shaped sequencer for a one-clause ADR is exactly the scope creep the Prioritization philosophy warns against.

**D8: Reuse the ADR 039 Single-Call Wendelon Admin Boundary.**
Replacement composes existing high-level operations:

1. Resolve the old binding (if any).
2. `StorageAdmin::drop_table_storage(DropTableStorageRequest { ... })`.
3. `Catalog::drop_table(name, false)`.
4. `Catalog::create_table(...)`.

Toron must not orchestrate Wendelon-internal drop stages, define a replacement proof carrier, or add `TableSchemaReplaceRequest` / `submit_table_replace` / `drop_table_with_replacement`. Each Wendelon admin operation remains a single high-level request owned internally by Wendelon — the ADR 039 D14 cross-ring single-call invariant preserved at the composition layer.

**D9: Existing `DependentIndexesPresent` Rejection Propagates.**
If the old target has dependent secondary indexes, Wendelon's existing `DependentIndexesPresent` rejection (ADR 039 D12) propagates through the replace path unchanged. Cascading secondary-index drop remains a deferred ADR 039 follow-up.

**D10: ADR 040 Toron CLI Reservations Stay Untouched.**
`nilket toron start`, `nilket toron sql`, and `nilket toron inspect` remain reserved with byte-identical diagnostic text. The Toron daemon substrate is a carried-forward ADR 042 lead candidate; ADR 041 does not spend scope on Toron daemon or public query networking.

## Architecture Guards

Added to `crates/toron/tests/architecture_test.rs`:

1. `adr041_schema_create_or_replace_no_longer_rejected`
2. `adr041_ctas_or_replace_no_longer_rejected`
3. `adr041_ctas_if_not_exists_remains_rejected`
4. `adr041_or_replace_if_not_exists_is_explicitly_rejected`
5. `adr041_no_statement_sequence_ir`
6. `adr041_no_wendelon_replace_request`
7. `adr041_replace_reuses_storage_admin_drop`
8. `adr041_ctas_replace_materializes_source_before_drop`
9. `adr041_toron_cli_reservations_preserved`
10. `adr041_replace_uses_fresh_table_object_id`

## Integration Fixtures

1. `create_or_replace_table_creates_when_absent`
2. `create_or_replace_table_replaces_existing_schema`
3. `create_or_replace_table_rejects_if_not_exists_combo`
4. `create_or_replace_table_produces_fresh_object_id`
5. `create_or_replace_table_as_select_creates_when_absent`
6. `create_or_replace_table_as_select_replaces_existing_table`
7. `ctas_or_replace_source_failure_preserves_existing_table`
8. `ctas_if_not_exists_remains_rejected`
9. `or_replace_uses_storage_admin_drop_before_catalog_removal`
10. `wendelon_dependent_index_reject_propagates_through_replace`
11. `nilket_toron_sql_start_inspect_still_reserved`

## Sub-Lift Posture

Single-plan. Unlike ADRs 037/038/039/040, ADR 041 has no Plan A / Plan B framing. Pure composition over already-shipped substrate — either both `OR REPLACE` rejects lift cleanly with the source-first CTAS ordering, or they don't. No half-state is worth shipping.

## Out of Scope (Recorded for Future ADRs)

- CTAS `IF NOT EXISTS` lift (skip-source-on-existence idempotency contract).
- Cascading secondary-index DROP on replacement.
- Atomic Wendelon-side replacement protocol (`TableSchemaReplaceRequest`).
- Cross-ring transactions / `LogicalPlan::Sequence` IR.
- Toron Compute Daemon Substrate (`nilket toron start` lift) — ADR 042 lead candidate.
- Public Toron SQL listener (`nilket toron sql` lift).
- `UPDATE WHERE` (requires PK/UNIQUE constraint substrate + partial-write substrate first).

## Council History

- **Iter 0:** Topic split. Claude → CREATE OR REPLACE; Codex + Gemini → Toron Compute Daemon Substrate. 1-vs-2.
- **Iter 1:** Triple-swap. Codex and Gemini both yielded to Claude's candidate; Claude yielded to theirs. 1-vs-2 reversed.
- **Iter 2:** Claude double-reversed back to CREATE OR REPLACE after weighing Codex iter-1's counter ("daemon without SQL service is operationally inert"). Codex iter-1 D-set ratified as spine. CTAS `IF NOT EXISTS` dispute: Codex + Claude defer, Gemini lift.
- **Iter 3 (final):** Gemini yielded on CTAS `IF NOT EXISTS`. Codex added one guard pinning the defer at source level. Topic and D-set locked. Triple convergence.

The triple-swap → double-reversal pattern is unusual but produced an exceptionally well-stress-tested D-set: every load-bearing decision was argued at least once from the opposing side before being adopted.

## Promotion Notes

This is the **first SQL composition cycle after the 037/038/039/040 substrate-and-operator-surface run**. It is also the first cycle in council history with a complete topic-swap-and-back pattern (Claude 0 = topic A; Claude 1 = topic B; Claude 2 = topic A; final = topic A). The pattern is the **discipline-by-restraint** signal working as intended: each member's iter-0 was structurally sound; council convergence selected the narrower, lower-substrate-cost cycle without anyone "winning" or "losing" their initial topic — the deferred topics carry forward as lead candidates for ADR 042+.
