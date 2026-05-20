# Impl Plan 052: Per-Tenant Concurrency Admission and Bounded Resource Observability

**Status:** Promoted, paired with
`plans/adr/052_per_tenant_concurrency_admission.md`.
**Posture:** Single plan, Toron-ring only. Coordinator-scale
admission extension + bounded resource metrics. No Wendelon
production change, no SQL wire change. Six phases.

## Invariants carried in (must still hold at the end)

- `crates/nilket` stays runtime-free.
- `crates/toron/src/bin/toron_fixture_worker.rs` source unchanged.
- New surface is brand-clean (no `Toron` prefix on protocol/helper
  names) and has no `*V1` suffix.
- ADR 049 tenant routing, ADR 050 memory budgeting, and ADR 051
  tenant-explicit startup descriptors remain behaviorally intact;
  their guards keep passing (with the D13 no-label-guard
  reconciliation).
- The only superseded invariant is the blanket no-per-tenant-label
  posture, and only for the named resource-governance metrics
  (ADR 052 D11/D13).

## Phase 0 — Audit (no code change)

Confirm the seams (verified at promotion):

- Admission: `coordinator.rs:329` `admit_query(&self, context) ->
  Result<AdmittedQuery, QueryError>` reserves memory only today via
  `tenant_budgets.query_budget_for_base(...)` +
  `tenant_memory.try_reserve(...)`; `AdmittedQuery { query_budget,
  _reservation: TenantMemoryReservation }` (`coordinator.rs:374`).
  `budget_rejection_query_error` (`:379`) maps `BudgetRejection` →
  `ExecutionError::BudgetExhausted`.
- Policy: `distributed/budget.rs:102` `TenantBudget {
  memory_per_task, aggregate_memory }`; `:117` `TenantBudgetPolicy
  { default, per_tenant }` with `omitted_config(worker_memory_budget)`
  (`:123`), `from_query_budget(query_budget)` (`:137`),
  `budget_for` (`:151`), `query_budget_for_base` (`:162`).
  Memory accountant: `TenantMemoryAccountant` (`:298`),
  `try_reserve` (`:311`), `TenantMemoryReservation` (`:347`).
- Config: `config.rs` `RawTenantBudgetPolicy` (`:291`) /
  `RawTenantBudget` (`:298`) / `RawTenantBudgetEntry` (`:304`);
  `normalize_tenant_budget_policy(raw, worker_memory_budget)`
  (`:604`); TOML `[tenant_budgets.default]` + `[[tenant_budgets.tenants]]`.
- Error mapping: `daemon.rs:771-795` maps
  `ExecutionError::BudgetExhausted { dimension: Memory }` →
  `SqlErrorCode::ResourceExhausted`, with a `code == ResourceExhausted`
  message branch emitting `"tenant memory budget exhausted"`.
- Observability: `network/observability.rs` `MetricsRegistry`
  (`:149`), `render_text` (`:182`); families
  `daemon_ready`, `listener_accepts_total`,
  `listener_rejects_total{listener,reason}`, `descriptors_loaded`,
  `static_workers_configured`.
- Locate the ADR 049/050/051 `*_no_per_tenant_metric_labels`
  guards (`architecture_test.rs:8083 / 8368 / 8673`, each
  asserting `!observability.contains("tenant_id")`).

## Phase 1 — Config + runtime policy (`config.rs`, `distributed/budget.rs`)

- `TenantBudget` gains `max_concurrent_queries: NonZeroU32` (D5).
  Update `TenantBudget::new` and all construction sites.
- `RawTenantBudget` / `RawTenantBudgetEntry` gain
  `max_concurrent_queries: Option<u32>` (D4).
- `normalize_tenant_budget_policy`: when present, reject `0` with
  the pinned diagnostic `tenant_budgets max_concurrent_queries must
  be greater than 0`; when omitted, derive
  `max(1, aggregate_memory_bytes / memory_per_task_bytes)` clamped
  to `u32::MAX` → `NonZeroU32`.
- `TenantBudgetPolicy::omitted_config` derives the same default
  from the synthesized memory policy; `from_query_budget` sets
  `max_concurrent_queries = NonZeroU32::new(1)` (D5 fallback).
- Update the example-config comment block in `config.rs` to show
  the new optional key.

## Phase 2 — Concurrency accountant (`distributed/budget.rs`)

- Add `TenantConcurrencyAccountant` (tenant-keyed, count-based,
  `Arc<Mutex<HashMap<TenantId, u32>>>` interior mutability, sibling
  to `TenantMemoryAccountant`), `try_reserve(tenant_id) ->
  Result<TenantConcurrencyReservation, TenantConcurrencyRejection>`,
  and `TenantConcurrencyReservation` (RAII decrement on drop) (D6).
- `TenantConcurrencyRejection { tenant_id, requested, available }`
  — a dedicated count rejection; do **not** reuse byte-oriented
  `BudgetDimension` / `BudgetRejection` (D6).
- Add a narrow error variant for concurrency exhaustion (e.g.
  `ExecutionError::ConcurrencyExhausted { tenant_id, requested,
  available }`) so SQL mapping is by variant, not string (D9).

## Phase 3 — Coordinator admission + error mapping (`coordinator.rs`, `daemon.rs`)

- The coordinator gains a `tenant_concurrency:
  TenantConcurrencyAccountant` field, constructed from the same
  `TenantBudgetPolicy` as `tenant_memory` (limits via
  `policy.budget_for(tenant_id).max_concurrent_queries`).
- `admit_query` (D7): reserve **concurrency first**, then memory.
  On memory failure the concurrency reservation local drops (RAII)
  before returning. `AdmittedQuery` holds both:
  `_concurrency_reservation` + `_reservation`.
- Concurrency rejection → `ExecutionError::ConcurrencyExhausted`
  via a `concurrency_rejection_query_error` analogous to
  `budget_rejection_query_error`. Rejection is before bind/plan/
  catalog/execution (D8 — `admit_query` already precedes
  `execute_bound_distributed_with_context`).
- `daemon.rs:771-795`: add the `ConcurrencyExhausted` arm →
  `ResourceExhausted`; **split the message branch** so concurrency
  emits `"tenant concurrency limit exhausted"` while memory keeps
  `"tenant memory budget exhausted"` (D9). Extend the existing
  `daemon.rs` unit tests (`:981+`) to pin both messages.

## Phase 4 — Bounded resource metrics (`network/observability.rs`)

- `MetricsRegistry` learns a bounded per-tenant resource surface
  (D11): the configured-tenant set (keys of
  `TenantBudgetPolicy::per_tenant`) is the promoted label set;
  every unconfigured tenant renders `tenant_id="other"` (including
  loopback when its tenant id is not explicitly configured).
- `render_text` emits, for each promoted tenant id and `"other"`:
  `tenant_memory_bytes_used`, `tenant_memory_bytes_limit`,
  `tenant_concurrency_active`, `tenant_concurrency_limit`, and
  `tenant_admission_rejections_total{resource="memory|concurrency"}`.
- Feed current usage gauges from accountant snapshots + policy
  limits (or advisory relaxed atomics if implementation-local
  simpler). Maintain `tenant_admission_rejections_total` with
  relaxed-atomic counters incremented on memory/concurrency
  admission failure. No hot-path lock beyond the admission
  accountant lock (D11).
- **Do not** add `tenant_id` to listener/general families (D12):
  `listener_accepts_total`, `listener_rejects_total`,
  `daemon_ready`, `daemon_uptime_seconds`, `descriptors_loaded`,
  `static_workers_configured`, readiness JSON, SQL wire counters.

## Phase 5 — Guards + fixtures + prior-guard reconciliation

- Add the 15 architecture guards + 3 convention guards (ADR doc
  list).
- **Retarget in place** `adr049/050/051_no_per_tenant_metric_labels`
  (D13): from `!observability.contains("tenant_id")` to assertions
  that (a) general/listener families carry no `tenant_id`, and
  (b) the bounded resource families use the configured-id/`other`
  rule. Keep the guard names (meta-guard / name-pin discipline).
- Add the 14 required integration fixtures + the optional
  cancellation fixture (ADR doc list). Reuse ADR 050's
  noisy-neighbor witness shape for fixture 7.

## Phase 6 — Evidence + promotion

- Evidence doc:
  `plans/impl/052_per_tenant_concurrency_admission_evidence.md`
  (commands + observed results: targeted config/budget/coordinator
  tests, `architecture_test adr052`, full `architecture_test`
  including the reconciled prior guards, `cargo test -p toron`,
  `cargo clippy -p toron --all-targets -- -D warnings` — note
  `--all-targets` so test code is linted, the gap surfaced in the
  ADR 051 review).
- Roadmap: ADR 052 Status bullet; new `### ADR 052 promoted;
  ADR 053 candidates` (lead: **Wendelon descriptor read/snapshot
  producer** in the Wendelon sequence, then a Toron bind-miss
  consumer reusing an in-place `sync_descriptor_for_tenant` merge;
  secondary: governance Phase 3 — CPU/connection/rate; OTLP /
  audit); demote `### ADR 051 promoted; ADR 052 candidates` to
  HISTORICAL; correct the stale "live sync reuses the
  `from_tenant_descriptors` seam" note to "needs an in-place merge"
  (D15 banked correction).

## Acceptance checklist

- [x] Phase 1 config + `NonZeroU32` runtime field + nonzero
      validation + derived-default; ADR 050 configs still valid.
- [x] Phase 2 concurrency accountant + reservation + dedicated
      rejection + named error variant (no `BudgetDimension` reuse).
- [x] Phase 3 admission reserves concurrency+memory before bind;
      both held by `AdmittedQuery`; failure drops the other;
      distinct `ResourceExhausted` message.
- [x] Phase 4 bounded resource metrics; general/listener metrics
      unchanged.
- [x] Phase 5 15+3 guards pass; ADR 049/050/051 no-label guards
      retargeted; ADR 040–051 guards pass.
- [x] Phase 6 14 required fixtures pass; ADR + impl + roadmap
      promoted; evidence recorded.
