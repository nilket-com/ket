# Implementation Plan: ADR 050 — Per-Tenant Memory Governance for Toron SQL

**Status:** Promoted (paired with
`plans/adr/050_per_tenant_memory_governance.md`).

## Overview

Eight-phase migration. Toron-ring substrate cycle that **reuses
the existing budget framework**. Query memory is already enforced
(`QueryBudget`/`TaskBudget.memory` →
`ExecutionError::BudgetExhausted`, `exec.rs:2387`); ADR 050 makes
its **source** per-tenant and adds a per-tenant **aggregate**
admission cap. No new enforcement engine; no `MemoryManager`
change (wire buffers, not query memory).

```
Phase 0: Audit (confirm reuse points + MemoryManager-is-wire-buffers)
Phase 1: [tenant_budgets] config + validation + omitted-default synthesis
Phase 2: Source per-query QueryBudget per-tenant at the coordinator
Phase 3: Per-tenant aggregate admission accountant (reserve / RAII release)
Phase 4: SqlErrorCode::ResourceExhausted + error mapping
Phase 5: Architecture guards
Phase 6: Integration fixtures
Phase 7: Evidence + promotion
```

## Phase 0: Audit (confirm the reuse points)

**Tasks:**

**0.1** Confirm the enforcement path is reused, not rebuilt:
`ExecutionError::BudgetExhausted { dimension:
BudgetDimension::Memory, requested, available }` (`exec.rs:2387`,
also `:2664`). Confirm the surrounding budget framework remains
unchanged: `partition_store.rs` `budget.reserve(..)` atomic reject
and `PartitionStoreError::BudgetExhausted`;
`TaskFailureKind::BudgetExhausted` (`scheduler.rs:780`).
`QueryBudget { memory_per_task }` / `TaskBudget { memory }` /
`TaskBudget::from_query_budget` (`distributed/budget.rs:65,101,110`);
coordinator `query_budget: QueryBudget` (`coordinator.rs:43`);
daemon wiring currently supplies `QueryBudget::default()` rather
than selecting a budget by tenant.

**0.2** Confirm `MemoryManager`/`BufPool` is **not** the
query-memory chokepoint (only `read_envelope` wire buffers,
`server/mod.rs:1374`). Guard 3 pins this.

**0.3** Confirm `context.tenant_id` reaches the coordinator entry
`execute_query_with_context(&self, plan, context)` (ADR 049) —
the admission chokepoint for D6.

**0.4** Confirm `SqlErrorCode` baseline = 11 variants (for the
only-new-code guard); `map_sql_query_error` is the mapping site.

**Acceptance:** reuse points confirmed; `MemoryManager`-is-wire-buffers
confirmed; `SqlErrorCode` baseline recorded.

## Phase 1: `[tenant_budgets]` config + validation

**Tasks:**

**1.1** Add `[tenant_budgets]` (D3): `[tenant_budgets.default]`
(`memory_per_task_bytes`, `aggregate_memory_bytes`) +
`[[tenant_budgets.tenants]]` entries (`tenant_id` +
per-task/aggregate). Parse into `TenantBudgetPolicy { per_tenant:
HashMap<TenantId, TenantBudget>, default: TenantBudget }`
(brand-clean).

**1.2** Validation (D3): nonzero `tenant_id`; nonzero byte
counts; `memory_per_task <= aggregate`; reject duplicate tenant
entries. Not sum-capped.

**1.3** Omitted-default synthesis (D3): if `[tenant_budgets]`
absent, synthesize a default preserving `QueryBudget::default()`
per-task memory. The synthesized default aggregate is
`max([worker].memory_budget_bytes, QueryBudget::default().memory_per_task)`
so `memory_per_task <= aggregate` still holds when an operator
configured worker memory below the current query default.
`[worker].memory_budget_bytes` stays distinct (D4).

**Acceptance:** fixture 5 (validation rejects) + optional
round-trip fixture.

## Phase 2: Source per-query QueryBudget per-tenant

**Tasks:**

**2.1** `TenantBudgetPolicy::query_budget_for(tenant_id) ->
QueryBudget` — builds a `QueryBudget` with the tenant's
`memory_per_task` (default for unlisted).

**2.2** Thread `TenantBudgetPolicy` into the coordinator
(constructor + daemon wiring from `[tenant_budgets]`).

**2.3** At `execute_query_with_context`, replace the single
`self.query_budget` with
`self.tenant_budgets.query_budget_for(context.tenant_id)` before
`TaskBudget::from_query_budget(...)` flows to the fragmenter.

**Acceptance:** fixtures 1-4 (per-tenant + default + loopback
budgets).

## Phase 3: Per-tenant aggregate admission accountant

**Tasks:**

**3.1** Add `TenantMemoryAccountant`: per-tenant in-flight
reserved `ByteCount` (shared map). `try_reserve(tenant_id, bytes)
-> Result<Reservation, _>` rejecting when `in_flight + bytes >
aggregate`; `Reservation` releases on drop (RAII).

**3.2** At `execute_query_with_context`: reserve the query's
effective `memory_per_task` (Phase 2) against `context.tenant_id`
**before** bind/execute; hold the `Reservation` for the query
lifetime; on exhaustion return early with the D8 error (no bind,
no execution).

**3.3** Ensure RAII release on all exit paths (success, error,
cancellation, early return).

**Acceptance:** fixtures 7-11 (admit within budget, reject over
budget before bind/execution, release on success + error,
one-tenant-saturated-does-not-block-another).

## Phase 4: `SqlErrorCode::ResourceExhausted` + mapping

**Tasks:**

**4.1** Add `SqlErrorCode::ResourceExhausted` (the one deliberate
wire addition, D8).

**4.2** Map `ExecutionError::BudgetExhausted { dimension: Memory }`
→ `ResourceExhausted` in `map_sql_query_error` (currently falls
to `Internal`).

**4.3** Map the D6 admission rejection → `ResourceExhausted`
(stable diagnostic `tenant memory budget exhausted`).

**4.4** Leave distributed `TaskFailureKind::BudgetExhausted`
unchanged on the distributed wire (D8), while mapping
`TaskFailureKind::BudgetExhausted { dimension: Memory, .. }` to
`ResourceExhausted` at the SQL boundary. Confirm no other
`SqlErrorCode` / `SqlMessage` change.

**Acceptance:** fixtures 6, 12, 13 (over-budget → ResourceExhausted;
distributed-task-failure still maps via TaskFailureKind;
only-new-code wire guard).

## Phase 5: Architecture guards

Add the 13 guards + 3 convention guards (ADR §Architecture
Guards) to `crates/toron/tests/architecture_test.rs`.

**Notes:**
- Guard 3 (`query_memory_governed_by_query_budget_not_memory_manager`):
  assert the per-tenant governance path uses `QueryBudget` /
  `TaskBudget.memory`, and `MemoryManager` is not the tenant
  governance mechanism.
- Guard 7/8: `ExecutionError::BudgetExhausted` + admission
  rejection map to `ResourceExhausted`.
- Guard 8 (`resource_exhausted_is_only_new_sql_error_code`):
  source-text assert `SqlErrorCode` gained exactly
  `ResourceExhausted` vs. the 11-variant baseline.
- Guard 13 (meta): ADR 040-049 guard fn names persist + sweep
  passes.

**Acceptance:** 13 + 3 pass; ADR 040-049 guards still pass.

## Phase 6: Integration fixtures

Implement the 13 required (+ 1 optional) fixtures (ADR
§Integration Fixtures). Mix: coordinator budget-source tests
(1-4), config validation (5), executor budget-exceed →
ResourceExhausted (6), aggregate accountant (7-11), distributed
task-failure mapping (12), wire-surface guard (13). Reuse the
existing `QueryBudget::test_default()` / `ByteCount::new(..)`
helpers.

**Notes:**
- Fixture 11 (`one_tenant_saturated_does_not_block_another`) is
  the noisy-neighbor witness: saturate tenant A's aggregate pool,
  prove tenant B admits + executes.

**Acceptance:** `cargo test -p toron` passes the 13 required.

## Phase 7: Evidence + promotion

**Tasks:**

**7.1** Commit atomically.
**7.2** Verify promoted docs (this file +
`plans/adr/050_per_tenant_memory_governance.md`).
**7.3** Update `crates/toron/plans/wip/roadmap.md`: Status bullet
for ADR 050; Forward sequence new `### ADR 050 promoted; ADR 051
candidates` (lead: Codex's tenant-explicit descriptor ingestion,
Toron-local); demote `### ADR 049 promoted; ADR 050 candidates`
to `(HISTORICAL)`.
**7.4** Evidence record (commands + pass counts + the one new
`SqlErrorCode` note).

**Acceptance:** ADR + impl + roadmap promoted; evidence captured.

## Risk register

| Risk | Mitigation |
|------|-----------|
| Re-implementing memory enforcement instead of reusing | Phase 0.1 pins reuse of `ExecutionError::BudgetExhausted` + `QueryBudget`/`TaskBudget`; D2 forbids `MemoryManager` instrumentation |
| Governing the wrong allocations (wire/`PooledBuf` not RecordBatch) | D2 + guard 3: govern via `QueryBudget` (where RecordBatch memory is accounted), not `BufPool` |
| Aggregate reservation leak on error/cancel path | Phase 3.1/3.3 RAII `Reservation` drop covers all exits; fixtures 9-10 assert release on success + error |
| `ResourceExhausted` wire addition seen as scope creep | D8 justifies it as the one operator-visible surface; guard 8 pins it as the *only* new code |
| Per-query budget alone fails to cap aggregate | D6 aggregate accountant is the cap; fixture 11 is the noisy-neighbor witness |
| `[tenant_budgets]` vs `[worker].memory_budget_bytes` confusion | D4: worker budget stays the node physical cap; tenant default independent; guard 1/2 pin config shape |
| Reserve full per-task budget over-reserves | Phase 3 reserves per-task budget upfront (deterministic, Phase 1); runtime high-water is Phase 2 |

## Phase totals

- 8 phases.
- 0 new crates; 0 Wendelon changes; 0 `crates/nilket` changes;
  `toron_fixture_worker.rs` unchanged; `MemoryManager` unchanged.
- Touch points: `[tenant_budgets]` config (config.rs); coordinator
  budget source + aggregate accountant (coordinator.rs);
  `SqlErrorCode::ResourceExhausted` + mapping (sql_wire.rs +
  map_sql_query_error). Reuses `QueryBudget`/`TaskBudget`/
  `ExecutionError::BudgetExhausted` unchanged.
- 13 guards + 3 convention; 13 required fixtures + 1 optional.
- One deliberate wire addition: `SqlErrorCode::ResourceExhausted`.

## Acceptance checklist

- [ ] Phase 0 confirms reuse points; `MemoryManager`-is-wire-buffers;
      11-variant `SqlErrorCode` baseline.
- [ ] Phase 1 `[tenant_budgets]` config + validation +
      omitted-default synthesis.
- [ ] Phase 2 per-query budget sourced per-tenant at coordinator.
- [ ] Phase 3 per-tenant aggregate admission (reserve / RAII
      release).
- [ ] Phase 4 `ResourceExhausted` + `BudgetExhausted` mapping;
      distributed `TaskFailureKind::BudgetExhausted` unchanged on
      the distributed wire.
- [ ] Phase 5 13 + 3 guards pass; ADR 040-049 guards pass.
- [ ] Phase 6 13 required fixtures pass (incl. noisy-neighbor
      witness).
- [ ] Phase 7 ADR + impl + roadmap promoted; evidence.

Ready for implementation.
