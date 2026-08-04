# ADR 050: Per-Tenant Memory Governance for Toron SQL

**Status:** Promoted
**Seed:** ADR 049 promotion §"ADR 050 candidates" (per-tenant
resource governance, LEAD — unblocked by ADR 049's
per-connection tenant identity).
**Council:** Claude, Codex, Gemini (iter-0 3-way: Claude + Gemini
lead per-tenant governance, Codex leads tenant-explicit
descriptor ingestion; iter-1 a live-code audit reframed the
governance *mechanism* — `MemoryManager`/`BufPool` governs
wire-message buffers, not query memory; query memory is already
enforced via `QueryBudget`/`TaskBudget.memory` +
`ExecutionError::BudgetExhausted`, sourced statically daemon-wide
— so the cut is "source the budget per-tenant + add aggregate
admission," the ADR 049 source-not-mechanism pattern; iter-2
Codex converged on the governance lead with the QueryBudget
mechanism (deferring his descriptor-ingestion lead to ADR 051)
and added the concrete config + omitted-default synthesis +
distributed-failure clarification; promoted from iter-2 with
Codex+Claude 2-of-3 convergence).
**Category:** Toron compute-ring substrate ADR — resource
isolation completing ADR 049's logical isolation. Exterior
Substrate → Toron home. Memory-only Phase 1. Zero Wendelon
production change.

## Summary

Make Toron's query-memory budget **per-tenant** and cap each
tenant's **aggregate** in-flight memory, so one admitted tenant
cannot consume the shared daemon unchecked (the noisy-neighbor
risk ADR 049's multi-tenant model introduced). Each query's
`QueryBudget.memory_per_task` is sourced from a node-local
per-tenant policy keyed by ADR 049's `tenant_id`; a per-tenant
aggregate admission accountant reserves the query's budget
against the tenant's pool at coordinator admission (RAII release)
and rejects over-budget queries before binding/execution.

The cut **reuses the existing budget enforcement** — query
memory already flows through `QueryBudget` →
`TaskBudget::from_query_budget` → executor checks
(`ExecutionError::BudgetExhausted { dimension:
BudgetDimension::Memory }`, `exec.rs:2387`) — but the coordinator
used a single daemon-wide `QueryBudget::default()` rather than a
tenant policy. ADR 050 changes the **source** to per-tenant and
adds the aggregate cap. `[worker].memory_budget_bytes` remains the
worker physical/advertised-memory setting and participates only
in omitted-policy default aggregate synthesis. The only
operator-visible wire addition is one new
`SqlErrorCode::ResourceExhausted`.

## Motivation

ADR 049 delivered logical multi-tenant isolation: SQL connections
route by mTLS-derived `tenant_id`, catalog resolution is
tenant-scoped. But resource isolation was deferred. On the
already-working create-then-query flow (a tenant creates tables
via DDL and queries them within one daemon), **noisy-neighbor is
a live availability risk the moment two tenants share a daemon** —
one tenant's heavy queries can exhaust the shared memory budget
and starve others.

Audit findings that shape the mechanism:
- **`MemoryManager`/`BufPool` is not the query-memory chokepoint.**
  Its only non-test acquire site is `read_envelope`
  (`server/mod.rs:1374`), reading wire frames. Query memory
  (RecordBatches, arrow buffers) is not `PooledBuf`.
- **Query memory is already governed and enforced** via
  `QueryBudget { memory_per_task }` → `TaskBudget { memory }`
  (`distributed/budget.rs`), checked per-task at `exec.rs:2387`
  and by the resident-memory check at `exec.rs:2664`; other
  budget dimensions already use the same `BudgetExhausted`
  framework.
- **It is sourced daemon-wide** today: the coordinator holds one
  `query_budget` (`coordinator.rs:43`), and the daemon wires it as
  `QueryBudget::default()` rather than selecting by tenant.

This is the ADR 049 pattern (substrate flows + is enforced; only
the *source* is static). ADR 050 makes the source per-tenant and
adds aggregate admission, reusing the enforcement.

## Decisions (12, Promotion-Locked)

**D1: Placement — Toron sequence.** Exterior Substrate; the
behavior changed is Toron's query-budget source + coordinator
admission. Unblocked by ADR 049 per-connection tenant identity.
No Wendelon production change.

**D2: Govern query memory through `QueryBudget`, not
`MemoryManager`.** Query memory continues to flow through
`QueryBudget`, `TaskBudget::from_query_budget`, executor budget
checks, and `BudgetDimension::Memory` failures. `MemoryManager` /
`BufPool` (wire buffers) is **not** introduced as a tenant-scoped
governance mechanism.

*(Rejected 2-of-3 Codex+Claude: Gemini's `MemoryManager::scoped(tenant_id)`
buffer-pool views with a shared per-tenant usage map. The audit
shows the `BufPool` backs wire frames + `PooledBuf`, not the
RecordBatch query memory that drives noisy-neighbor; instrumenting
it would govern the wrong allocations and miss the budget the
executor already enforces. Same caller-owned-vs-low-level-scoped
trade-off settled in ADR 047 D8 / ADR 049 D7.)*

**D3: Node-local tenant budget policy.** Add a `[tenant_budgets]`
config section with a default + per-tenant overrides:

```toml
[tenant_budgets.default]
memory_per_task_bytes = 67108864
aggregate_memory_bytes = 268435456

[[tenant_budgets.tenants]]
tenant_id = 7
memory_per_task_bytes = 16777216
aggregate_memory_bytes = 67108864
```

Validation: `tenant_id` nonzero; all byte counts nonzero;
`memory_per_task_bytes <= aggregate_memory_bytes`; duplicate
tenant entries rejected. Tenant budgets are **not** sum-capped —
operators may oversubscribe aggregates under node-local policy.

If `[tenant_budgets]` is omitted, Toron synthesizes a default
policy preserving the current per-task `QueryBudget::default()`
memory limit. The synthesized default aggregate is
`max([worker].memory_budget_bytes, QueryBudget::default().memory_per_task)`
so the generated policy is valid even when an operator configured
worker memory below the current daemon-wide query default.
Explicit policy is preferred for multi-tenant deployments.

**D4: `[worker].memory_budget_bytes` remains distinct.** It stays
the worker's physical/advertised memory + shared buffer-pool
construction input; it is not silently reinterpreted as a
per-tenant query budget. ADR 050 policy lives under
`[tenant_budgets]`.

**D5: Per-query budget selected by tenant.** At coordinator
admission, select the effective tenant budget using ADR 049's
`context.tenant_id` (`ExecutionContext` /
`TaskExecutionContext`). Build the query's `QueryBudget` from that
tenant policy before `TaskBudget::from_query_budget` reaches the
fragmenter/executor. The existing per-task enforcement then caps
each query at the tenant's per-task budget.

**D6: Per-tenant aggregate admission reserved up front.**
Per-query budgets do not bound concurrency. A per-tenant
aggregate admission accountant reserves the query's effective
`memory_per_task` against the tenant's `aggregate_memory_bytes`
on query entry; if the reservation would exceed the aggregate
cap, reject **before** binding/execution. The reservation
releases via RAII on success, error, cancellation, or early
return. Phase 1 reserves the full per-task budget up front
(runtime high-water reservation is a future refinement).

**D7: Hard reject — no queue, borrow, spill, or fairness.**
Tenant aggregate exhaustion rejects the query immediately. ADR
050 does not queue, borrow from idle tenants, spill, or implement
weighted fairness.

**D8: Failure surface — one new `SqlErrorCode::ResourceExhausted`.**
Both coordinator admission rejection (D6) and downstream
`ExecutionError::BudgetExhausted { dimension: Memory, .. }` map to
a **new** `SqlErrorCode::ResourceExhausted` with a stable
diagnostic (`tenant memory budget exhausted`). This is the
**only** SQL wire-surface addition — no new `SqlMessage` variant,
no SQL payload tenant field. Distributed task failures continue
to use the existing `TaskFailureKind::BudgetExhausted`
(`scheduler.rs:780`), unchanged on the distributed wire; at the
SQL boundary, `TaskFailureKind::BudgetExhausted { dimension:
Memory, .. }` maps to `ResourceExhausted`.

**D9: Tenant source is ADR 049 only.** Policy lookup uses only
ADR 049 admission: authenticated SQL client identity for
non-loopback SQL, configured loopback/default tenant for
loopback. No CLI `--tenant`, SQL text tenant claim,
token/JWT/OIDC/SPIFFE claim, or request-payload tenant field.

**D10: Memory-only Phase 1.** CPU quotas, rate limits, connection
caps, storage quotas, cluster-wide scheduling fairness, and
spill-policy changes are out of scope.

**D11: Metrics remain low-cardinality.** ADR 045/049 posture
preserved. A resource-exhaustion counter may use a bounded reason
label; no `tenant_id` labels or per-tenant metric families.

**D12: Descriptor discovery remains deferred.** Tenant-explicit
descriptor ingestion (Codex's Option 1, Toron-local) is the lead
ADR 051 candidate; live Wendelon descriptor sync remains a later
cross-ring design. ADR 050 changes no descriptor bootstrap,
Wendelon schema API, or catalog discovery.

**Single-plan posture; Toron-ring only.**

## Architecture Guards (13, Promotion-Locked)

1. `adr050_tenant_budget_policy_config_is_node_local`
2. `adr050_tenant_budget_config_validates_default_duplicates_and_nonzero`
3. `adr050_query_memory_governed_by_query_budget_not_memory_manager`
4. `adr050_per_query_budget_sourced_by_tenant_at_coordinator`
5. `adr050_aggregate_admission_reserves_and_releases_by_tenant`
6. `adr050_aggregate_admission_rejects_before_bind_or_execution`
7. `adr050_budget_exhaustion_maps_to_resource_exhausted`
8. `adr050_resource_exhausted_is_only_new_sql_error_code`
9. `adr050_default_and_loopback_tenants_use_default_budget`
10. `adr050_memory_only_no_cpu_rate_connection_or_storage_quotas`
11. `adr050_no_per_tenant_metric_labels`
12. `adr050_no_wendelon_production_changes`
13. `adr050_prior_adr04x_guards_continue_to_pass` (meta-guard;
    ADR 040-049 guard names persist + sweep passes)

**Post-promotion convention guards:**
- `adr050_nilket_root_remains_runtime_free`
- `adr050_fixture_worker_source_unchanged`
- `adr050_new_surface_is_brand_clean_and_has_no_v1_suffix`

## Integration Fixtures (13 required + 1 V1-evidence optional)

**Required (gates promotion):**

1. `tenant_7_query_uses_tenant_7_per_task_budget`.
2. `tenant_8_query_uses_distinct_budget_proves_not_daemon_wide`.
3. `unlisted_tenant_uses_default_budget`.
4. `loopback_tenant_uses_default_budget`.
5. `tenant_budget_config_rejects_zero_duplicate_and_per_task_over_aggregate`.
6. `query_over_per_task_budget_returns_resource_exhausted`
   (`ExecutionError::BudgetExhausted` → `ResourceExhausted`).
7. `tenant_aggregate_pool_admits_within_budget`.
8. `tenant_aggregate_pool_rejects_over_budget_before_bind_or_execution`.
9. `aggregate_reservation_released_on_successful_completion`.
10. `aggregate_reservation_released_on_error_or_cancellation`.
11. `one_tenant_saturated_does_not_block_another` — the
    noisy-neighbor witness.
12. `distributed_task_budget_exhausted_still_maps_via_task_failure_kind`.
13. `resource_exhausted_is_only_new_sql_error_code_no_new_message`
    — wire-surface guard fixture.

**Optional V1-evidence (non-gating):**
- `tenant_budget_policy_config_round_trips` — `[tenant_budgets]`
  parse + omitted-default synthesis.

## Sub-Lift Posture

**Single-plan.** Toron-ring only; reuses the existing budget
framework. The new substrate is: the per-tenant config policy,
the coordinator per-tenant budget selection, the aggregate RAII
admission accountant, and the one `ResourceExhausted` error code.

## Out of Scope

- Tenant-scoped `MemoryManager` / `BufPool` as the governance
  mechanism (D2 rejection).
- Byte-perfect heap accounting (governs query budget, not every
  allocation).
- Queuing, borrowing, fair-share, runtime high-water reservation
  (Phase 2+).
- CPU / rate / connection / storage / spill governance.
- Per-tenant metric labels.
- Descriptor ingestion or live descriptor sync (ADR 051).
- Token/JWT/OIDC/SPIFFE tenant claims.
- Any Wendelon production change.

## Council History

- **Iter-0:** Claude + Gemini lead per-tenant governance; Codex
  leads tenant-explicit descriptor ingestion.
- **Iter-1:** Claude R1 — live-code audit retracted the iter-0
  "instrument `MemoryManager`" framing (mine + Gemini's); the
  real mechanism is per-tenant `QueryBudget` source + aggregate
  admission, reusing the existing `BudgetExhausted` enforcement
  (the ADR 049 source-not-mechanism pattern). Engaged Codex's
  sequencing: governance leads (live noisy-neighbor risk on the
  working create-then-query flow); descriptor ingestion is the
  ADR 051 lead (Codex Option 1, confirmed Toron-local).
- **Iter-2 (Codex+Claude 2-of-3, promoted):** Codex converged on
  the governance lead with the QueryBudget mechanism (deferring
  his descriptor-ingestion lead to ADR 051), and added the
  concrete `[tenant_budgets]` config + omitted-default synthesis
  (D3), hard-reject (D7), and the distributed
  `TaskFailureKind::BudgetExhausted`-stays-on-the-distributed-wire
  clarification (D8).

**Rejected (2-of-3 Codex+Claude):** Gemini's
`MemoryManager::scoped(tenant_id)` buffer-pool governance —
governs wire/`PooledBuf` allocations, not the RecordBatch query
memory the executor already budgets (audit-confirmed the `BufPool`
is not the query-memory chokepoint). Mirrors the ADR 047 D8 /
ADR 049 D7 resolutions favoring reuse of existing higher-layer
enforcement over a low-level scoped-view.

## Promotion criteria met

1. D-set locked at 12 (Codex+Claude 2-of-3 convergent).
2. Guard set 13 + 3 convention.
3. Fixture set 13 required + 1 V1-evidence.
4. Single-plan, Toron-ring; no Wendelon production change.
5. Placement: Exterior Substrate → Toron home (convention).
6. No-V1-suffix convention; `toron_fixture_worker.rs` unchanged;
   `crates/nilket` runtime-free.
7. ADR 040-049 invariants preserved (meta-guard 13).
8. Reuses the existing `QueryBudget`/`TaskBudget`/`BudgetExhausted`
   enforcement; one deliberate wire addition
   (`SqlErrorCode::ResourceExhausted`).

## Conclusion

ADR 050 completes ADR 049's logical isolation with resource
isolation: per-tenant query budgets sourced from node-local
policy keyed by the mTLS-admitted tenant, plus a per-tenant
aggregate admission cap, reusing Toron's existing budget
enforcement. Memory-only, Toron-ring, no Wendelon change; the
single operator-visible surface is `SqlErrorCode::ResourceExhausted`.

The eight-phase migration is materialized in
`plans/impl/0050_per_tenant_memory_governance.md`.
