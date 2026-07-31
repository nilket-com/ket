# ADR 052: Per-Tenant Concurrency Admission and Bounded Resource Observability

**Status:** Promoted
**Seed:** ADR 051 promotion §"ADR 052 candidates" (Per-Tenant
Governance Phase 2 — concurrency caps, deferred from ADR 050 D10;
per-tenant metric labels, deferred from ADR 050 D11 / ADR 045
posture).
**Council:** Claude, Codex, Gemini (iter-0 topic split — Claude +
Gemini led Governance Phase 2, Codex led live descriptor sync;
iter-1 Codex retracted his liveness lead to 3-of-3 topic
convergence on Governance Phase 2 once the live-sync producer was
recognized as Wendelon-gated; the metrics-scope question split
on whether listener/general daemon metrics gain tenant labels;
iter-2 the listener-label question settled 2-of-3 Claude+Codex
toward aggregate listener metrics, and all three reached a unified
promotion-ready D-set).
**Category:** Toron compute-ring substrate ADR — coordinator
resource-admission revision extending the multi-tenant arc
(ADR 049 routing + ADR 050 memory governance + ADR 051 descriptor
source). Exterior Substrate → Toron home. Coordinator-scale;
zero Wendelon change; zero SQL wire change.

## Summary

Add **per-tenant query concurrency admission** at the Toron
coordinator: each tenant carries a node-local
`max_concurrent_queries` cap, reserved alongside the ADR 050
aggregate memory reservation **before bind**, and surfaced through
the existing `SqlErrorCode::ResourceExhausted`. This closes
ADR 050's deferred concurrency vector (D10): a tenant can saturate
coordinator query slots without ever hitting a memory limit.
Source-not-mechanism, again — a count reservation beside the
existing byte reservation; no new coordinator architecture.

In the same cycle, lift ADR 050's no-per-tenant-label deferral
(D11) under a **config-bounded cardinality rule**: the named
resource-governance metric families gain a `tenant_id` label whose
value is a configured tenant id or `"other"`, keeping the series
count `O(configured_tenants + 1)`. General daemon and listener
metrics remain aggregate.

Live Wendelon → Toron descriptor sync — the ADR 051 liveness gap —
is **sequenced, not abandoned**: a Wendelon-sequence descriptor
read/snapshot producer ADR first, then a later Toron consumer ADR.
It is not Toron 052.

## Motivation

ADR 049 gave each SQL connection a tenant source; ADR 050 used
that source to select a per-tenant `QueryBudget` and reserve
aggregate tenant memory before bind (`coordinator.rs:329`
`admit_query` → `TenantMemoryAccountant::try_reserve`); ADR 051
made startup descriptors tenant-explicit. ADR 050 was deliberately
**memory-only Phase 1** (D10): CPU, rate, connection, and
concurrency limits were deferred. The live noisy-neighbor vector
that remains on the working create→query flow is **query
concurrency** — a tenant opening unbounded in-flight queries
starves other tenants' admission slots with no memory pressure
required. ADR 052 closes that vector at the same admission seam.

ADR 050 also kept metrics tenant-label-free (D11), preserving the
ADR 045/049 low-cardinality posture. But a concurrency cap that
rejects with `ResourceExhausted` and gives the operator no
per-tenant visibility into *which* tenant is saturating or *how
close* tenants run to their limits is half-operable. The only real
basis for the no-label invariant is cardinality; binding the label
set to the configured-tenant list answers that on the merits, so
ADR 052 lifts the deferral for the narrow resource-governance
surface that operating the new cap requires.

## Decisions (15, Promotion-Locked)

**D1: Topic.** Multi-tenant hardening — per-tenant query
concurrency admission plus bounded per-tenant resource-governance
observability.

**D2: Placement — Toron sequence.** Exterior Substrate → Toron
home. Zero Wendelon production change; `crates/nilket` stays
runtime-free.

**D3: Scale boundary — coordinator, single daemon.** This is
cross-query admission on one Toron node. Cluster-wide tenant caps,
a distributed semaphore, global coordination, queueing, borrowing,
and fair-share scheduling are out of scope.

**D4: Config surface.** Extend the `[tenant_budgets]` default and
per-tenant entries (`RawTenantBudget` / `RawTenantBudgetEntry`)
with an optional `max_concurrent_queries`.
`normalize_tenant_budget_policy` normalizes it into a runtime
nonzero field. If present it must be `> 0`; if omitted it is
derived from the memory policy:

```text
max(1, aggregate_memory_bytes / memory_per_task_bytes)  (clamped to u32::MAX)
```

This keeps every existing ADR 050 `[tenant_budgets]` config valid.
Pinned validation diagnostic:

```text
tenant_budgets max_concurrent_queries must be greater than 0
```

**D5: Runtime policy type.** `TenantBudget`
(`distributed/budget.rs`) gains `max_concurrent_queries:
NonZeroU32`. Keep the `TenantBudgetPolicy` name for this cycle —
it already owns node-local tenant resource limits; broadening the
name is churn without value. `TenantBudgetPolicy::from_query_budget`
(the conservative bare-coordinator fallback) uses
`max_concurrent_queries = 1`.

**D6: Admission mechanism.** Add `TenantConcurrencyAccountant` and
`TenantConcurrencyReservation` beside `TenantMemoryAccountant`.
Tenant-keyed, count-based (`u32`), internally synchronized, RAII
released on drop. It does **not** overload the byte-oriented
`BudgetDimension` / `BudgetRejection`; it uses a dedicated
`TenantConcurrencyRejection { tenant_id, requested, available }`.

**D7: Admission order.** `coordinator.rs::admit_query` reserves
concurrency **then** memory before bind. `AdmittedQuery` holds both
reservations until the query completes, errors, or is cancelled.
If the memory reservation fails after the concurrency reservation
is taken, the concurrency reservation drops (RAII) before the
error returns — all-or-nothing admission.

**D8: Rejection timing.** Concurrency exhaustion rejects **before**
bind, planning, catalog access, or execution — matching ADR 050's
memory-admission posture.

**D9: SQL surface.** Reuse `SqlErrorCode::ResourceExhausted`. The
concurrency rejection maps through a **named** error variant (not
string matching) so the SQL diagnostic is testable; the daemon
error-mapping message branch (`daemon.rs:790`) distinguishes the
two causes:

```text
tenant concurrency limit exhausted        (vs ADR 050's "tenant memory budget exhausted")
```

No new `SqlErrorCode`, no new `SqlMessage` variant, no SQL payload
tenant field, no SQL syntax.

**D10: Tenant source remains ADR 049 only.** Remote SQL uses the
authenticated client-cert tenant; loopback uses
`config.node.tenant_id`. Concurrency policy never reads a tenant
from SQL text, SQL frames, CLI flags, or tokens.

**D11: Bounded resource-governance metrics.** Add `tenant_id`
labels to exactly these families:

```text
tenant_memory_bytes_used        tenant_concurrency_active
tenant_memory_bytes_limit       tenant_concurrency_limit
tenant_admission_rejections_total{resource="memory|concurrency"}
```

The label value is a configured tenant id or `"other"`. Configured
= a key in `TenantBudgetPolicy::per_tenant`; every unconfigured
tenant collapses to `"other"` (including loopback when its tenant
id is not explicitly configured), and that bucket carries the
default policy's limits. Series count is
`O(configured_tenants + 1)`. Admission-rejection totals are
maintained with relaxed atomics; current usage gauges may be
rendered from accountant snapshots or maintained with advisory
relaxed atomics. Admission must not gain a hot-path lock beyond the
accountant lock it already needs.

**D12: General daemon metrics remain aggregate.** Do **not** add
`tenant_id` labels to `listener_accepts_total`,
`listener_rejects_total`, `listener_up`, `daemon_ready`,
`daemon_uptime_seconds`, `descriptors_loaded`,
`static_workers_configured`, the readiness JSON, or SQL wire
counters. Listener-edge tenant labeling is a separate
observability design — pre-auth and failed-auth connections have
no stable tenant.

**D13: Prior-guard reconciliation.** The ADR 049/050/051
`*_no_per_tenant_metric_labels` guards — each asserting no
`tenant_id` substring in `observability.rs` — are **retargeted in
place** (continuous-refactoring + meta-guard discipline): keep the
names, change the assertions from "no `tenant_id` anywhere" to "no
*unbounded/general* tenant labels; resource-metric labels are
bounded by configured tenants plus `other`." This invariant
reversal is an explicit ADR 052 decision, not a behavior-neutral
cleanup.

**D14: Scope fence.** No CPU wall-time limits, rate limits,
listener connection caps, storage quotas, queueing, borrowing,
fair-share, priority classes, cross-worker cancellation changes,
audit subsystem, descriptor sync, reload endpoint, file watch, or
runtime descriptor ingestion.

**D15: Descriptor liveness carry-forward.** Live Wendelon → Toron
descriptor sync is sequenced, not rejected: a **Wendelon-sequence**
descriptor read/snapshot producer ADR first, then a later **Toron**
bind-miss consumer ADR over the real producer API. The consumer
must use an **in-place registry merge** (e.g.
`sync_descriptor_for_tenant`) under the registry write lock — it
must **not** swap the daemon's shared registry `Arc`, and ADR 051's
fresh `from_tenant_descriptors` constructor is **not** sufficient
for live sync (the daemon services share one
`Arc<WendelonDescriptorRegistry>`). ADR 052 adds no speculative
`DescriptorSource` trait, background sync task, or descriptor stub.

**Single-plan posture; Toron-ring only.**

## Architecture Guards (15, Promotion-Locked)

1. `adr052_tenant_budget_policy_has_concurrency_limit`
2. `adr052_concurrency_config_validates_nonzero`
3. `adr052_omitted_config_derives_bounded_concurrency_default`
4. `adr052_concurrency_accountant_is_tenant_keyed_and_raii`
5. `adr052_concurrency_admission_rejects_before_bind_or_execution`
6. `adr052_admitted_query_holds_memory_and_concurrency_reservations`
7. `adr052_concurrency_exhaustion_maps_to_resource_exhausted`
8. `adr052_resource_exhausted_reuses_existing_sql_error_code`
9. `adr052_one_tenant_saturated_does_not_block_another`
10. `adr052_resource_metrics_have_bounded_tenant_labels`
11. `adr052_general_daemon_metrics_remain_tenant_label_free`
12. `adr052_no_cpu_rate_connection_storage_queue_or_fair_share`
13. `adr052_no_wendelon_production_changes`
14. `adr052_no_descriptor_sync_stub_or_runtime_ingestion`
15. `adr052_prior_adr04x_05x_guards_continue_to_pass` (meta-guard;
    ADR 040–051 guard names persist + sweep passes)

**Convention guards:**
- `adr052_nilket_root_remains_runtime_free`
- `adr052_fixture_worker_source_unchanged`
- `adr052_new_surface_is_brand_clean_and_has_no_v1_suffix`

**Prior-guard reconciliation (continuous-refactoring; names kept,
assertions retargeted per D13):**
- `adr049_no_per_tenant_metric_labels`
- `adr050_no_per_tenant_metric_labels`
- `adr051_no_per_tenant_metric_labels`

→ each asserts no *unbounded/general* tenant labels and pins the
configured-tenant/`other` resource-metric cardinality rule.

## Integration Fixtures (14 required + 1 optional)

**Required (gates promotion):**
1. `tenant_concurrency_policy_config_round_trips`
2. `tenant_concurrency_config_rejects_zero`
3. `tenant_concurrency_omitted_config_derives_default`
4. `tenant_concurrency_limit_rejects_before_bind`
5. `tenant_concurrency_reservation_releases_on_success`
6. `tenant_concurrency_reservation_releases_on_error`
7. `one_tenant_concurrency_saturation_does_not_block_another`
   (the noisy-neighbor witness)
8. `loopback_sql_uses_default_concurrency_policy`
9. `sql_concurrency_exhaustion_returns_resource_exhausted`
10. `resource_metrics_label_configured_tenant`
11. `resource_metrics_bucket_unconfigured_tenant_as_other`
12. `resource_metrics_expose_memory_and_concurrency_limits`
13. `resource_admission_rejections_are_labeled_by_resource`
14. `general_daemon_metrics_do_not_gain_tenant_labels`

**Optional (non-gating):**
- cancellation releases the concurrency reservation — included if
  the existing harness can trigger cancellation without new
  machinery.

## Out of Scope

- CPU wall-time / rate / connection / storage / spill governance;
  queueing, borrowing, fair-share, priority classes (Phase 3+).
- Cluster-wide / distributed concurrency caps (D3 — coordinator
  scale only).
- Listener-edge tenant-labeled metrics; tenant labels on general
  daemon metrics (D12).
- Live Wendelon → Toron descriptor sync (D15 — Wendelon producer
  ADR first, then a later Toron consumer ADR).
- Runtime descriptor ingestion / reload / file watch; a
  speculative `DescriptorSource` trait.
- New SQL error code / `SqlMessage` / tenant field / syntax;
  token/JWT/OIDC/SPIFFE tenant claims.
- Any Wendelon production change.

## Council History

- **Iter-0 (topic split):** Claude + Gemini led Per-Tenant
  Governance Phase 2; Codex led live descriptor sync. All three
  flagged that the live-sync *producer* is Wendelon-owned and
  gated.
- **Iter-1:** Codex retracted his liveness topic lead → 3-of-3 on
  Governance Phase 2, sequencing live sync as a Wendelon producer
  first / Toron consumer later. Claude conceded toward Gemini and
  folded bounded per-tenant metrics into the cycle, naming the
  three-guard invariant reversal as the cost. Codex banked the
  correction that a live-sync consumer needs an in-place registry
  merge, not ADR 051's fresh constructor. The metrics *scope*
  split: Gemini labeled resource **and** listener metrics; Claude
  + Codex kept listener/general metrics aggregate.
- **Iter-2 (promoted):** the listener-label question resolved
  **2-of-3 Claude+Codex** toward aggregate listener metrics
  (pre-auth / failed-auth connections lack a stable tenant), while
  keeping Gemini's operability point for the resource metrics that
  matter to the new cap. Unified D-set; coordinator-scale
  confirmed; derived-default concurrency settled.

**Resolved (2-of-3):** Gemini's broader proposal to tenant-label
listener and general daemon metrics — kept aggregate (D12); the
bounded resource-governance labels (D11) carry the operability
intent without the listener-edge tenant-classification problem.

**Sequenced, not rejected:** live Wendelon → Toron descriptor sync
(Codex's iter-0 bind-miss design + the in-place-merge correction)
→ a Wendelon descriptor-producer ADR, then a Toron consumer ADR.

## Promotion criteria met

1. D-set locked at 15 (3-of-3 topic; metrics scope 2-of-3).
2. Guards 15 + 3 convention; ADR 049/050/051 no-label guards
   retargeted in place (D13).
3. Fixtures 14 required + 1 optional.
4. Single-plan, Toron-ring; coordinator scale; no Wendelon
   production change; no SQL wire change.
5. Placement: Exterior Substrate → Toron home
   (ring-product-placement).
6. No-V1 convention; `toron_fixture_worker.rs` unchanged;
   `crates/nilket` runtime-free.
7. ADR 040–051 invariants preserved (meta-guard 15); the
   no-per-tenant-label invariant is explicitly and narrowly
   superseded for the named resource-governance metrics (D11/D13).

## Conclusion

ADR 052 adds count-based per-tenant query admission where ADR 050
already admits memory, reusing the coordinator `admit_query` seam
and the existing `ResourceExhausted` surface, and exposes the
minimum bounded per-tenant metrics needed to operate the cap —
closing the multi-tenant arc's resource-isolation dimension on a
single coordinator. Descriptor liveness is left for the correctly
placed Wendelon-first sequence.

The migration is materialized in
`plans/impl/052_per_tenant_concurrency_admission.md`.
