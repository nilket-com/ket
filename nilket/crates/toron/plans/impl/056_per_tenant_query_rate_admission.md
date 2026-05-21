# Impl Plan 056: Per-Tenant Query-Rate Admission

**Status:** Promoted, paired with
`plans/adr/056_per_tenant_query_rate_admission.md`.
**Posture:** Single plan, Toron-ring only. Folds one rate gate into the
ADR 055 `TenantAdmissionController`; reuses the `ResourceExhausted` /
`tenant_admission_rejections_total` surfaces; no new error code, no SQL
wire / Wendelon / nilket / fixture-worker change. Eight phases, dependency-
ordered. **Default-disabled: with `max_queries_per_second = 0` (the
default), every phase is behavior-neutral vs 055.**

## Invariants carried in

- ADR 049-055 behaviorally unchanged when rate is unconfigured; 055's
  admission guards keep passing (055 no-rate-surface guard retargeted in
  place).
- `crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged;
  brand-clean / no-`*V1`. Node-local single daemon (no cluster/distributed
  rate). No background task.

## Phase 0 — Audit (anchors to verify before edits)

- `distributed/budget.rs`:
  - `TenantBudget { …, admission_weight, max_queued_queries,
    max_borrowed_memory, max_borrowed_concurrent_queries }` +
    `with_admission_limits(..)`.
  - `TenantAdmissionController` / `…Inner { policy, admission:
    Option<AdmissionPolicy>, state: Mutex<AdmissionState> }`;
    `try_admit(self: &Arc<Self>, tenant_id, memory)` — the rate gate's home.
  - `AdmissionState` (per-tenant maps, queues, deficits, `queue_full_
    rejections` / `queue_timeouts: HashMap<TenantMetricKey, u64>`);
    `TenantMetricKey::{Configured(TenantId), Other}` is the metric shape to
    mirror, not the runtime bucket key to reuse.
  - `AdmissionResource::{Memory, Concurrency}` + `TenantResourceRejection
    Counters` (`counter()` match) + `TenantAdmissionRejectionCounters`.
  - `TenantAdmissionRejection::{Memory, Concurrency, QueueDisabled,
    QueueFull, QueueTimeout, RequestTooLarge, GrantCanceled}` — add
    `RateLimited`.
- `coordinator.rs`: `admit_query` (async) → `try_admit`; `admission_
  rejection_query_error(rejection)` maps variants and increments
  `admission_rejections`. The 054 retry loop + writes run *under* the
  returned `AdmittedQuery` (one admission per top-level query).
- `config.rs`: `normalize_tenant_budget(..)`; `RawTenantBudget` /
  `RawTenantBudgetEntry`.
- `network/observability.rs`: renders `tenant_admission_rejections_total`.

## Phase 1 — Config surface (config.rs, budget.rs)

- `TenantBudget` += `max_queries_per_second: u32`, `query_burst_capacity:
  u32` (default 0). Extend the constructor; helpers `rate_enabled()` and
  `effective_burst()` (`= rate if burst==0 && rate>0`).
- `RawTenantBudget` / `RawTenantBudgetEntry` += `Option<u32>` → `unwrap_or(0)`;
  commented config template updated.
- Config tests: omitted ⇒ disabled; burst-derive; existing configs valid.

## Phase 2 — Clock seam (budget.rs, coordinator.rs, daemon.rs)

- Inject a monotonic clock into the controller (`trait MonotonicClock { fn
  now_millis(&self) -> u64 }` with `SystemClock` + `TestClock`, or
  equivalent). `TenantAdmissionController::new(..)` keeps its signature via a
  `SystemClock` default; add `with_clock(..)` for tests. 055's queue timeout
  still uses `tokio::time::timeout` in the coordinator — separate, untouched.

## Phase 3 — Token bucket + rate gate (budget.rs) — the core

```rust
struct TokenBucket { tokens: u32, last_refill_millis: u64 }
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RateBucketKey { Configured(TenantId), Other }   // shared "other"
// in AdmissionState: rate_buckets: HashMap<RateBucketKey, TokenBucket>
```

- `try_admit` gains a **first** step, *before* the `let Some(admission) =
  self.admission else {…}` branch:
  ```text
  lock state
  budget = policy.budget_for(tenant)
  if budget.rate_enabled():
      key = Configured(tenant) if policy.is_configured_tenant(tenant) else Other
      // Other uses the DEFAULT budget's rate/burst
      refill(bucket, clock.now_millis(), rate, burst)
      if bucket.tokens == 0 { return Rejected(RateLimited{tenant}) }   // no consume, no queue
      bucket.tokens -= 1                                               // consume-on-pass
  // …existing 055 owned/queue/borrow logic, unchanged…
  ```
- **Refill (remainder-preserving — watch-item #2):**
  `elapsed = now.saturating_sub(last_refill); added = (elapsed as u128 *
  rate as u128 / 1000) as u64; if added > 0 { tokens = min(burst, tokens +
  added); last_refill += added * 1000 / rate }` — **advance by consumed
  time, not to `now`.** Fresh/absent bucket starts full.
- No `Drop`, no release, no refund. The rate gate touches only
  `rate_buckets`; on reject it returns before any owned/queue interaction.

## Phase 4 — Rejection variant + coordinator mapping (budget.rs, coordinator.rs)

- `TenantAdmissionRejection::RateLimited { tenant_id }`;
  `AdmissionResource::Rate` (extend `TenantResourceRejectionCounters` with a
  `rate` atomic + `counter()` arm + `get`).
- `coordinator.rs::admission_rejection_query_error`: add the `RateLimited`
  arm → increment `admission_rejections` with `AdmissionResource::Rate` →
  `ResourceExhausted` + diagnostic
  `"tenant query rate limit exhausted for tenant {tenant_id}"`. (Immediate
  `Rejected`, never `Queued`; no await path — same shape as
  Memory/Concurrency arms.)
- 054 retry unchanged — runs inside the granted `AdmittedQuery`, never
  re-enters `try_admit`, so no token re-spend (verify + fixture).

## Phase 5 — Metrics (budget.rs, network/observability.rs) (D11)

- `tenant_admission_rejections_total{resource="rate"}` rides the existing
  bounded family (configured + `"other"`). Extend the render. No new family,
  no gauge, no table/query/object/descriptor/fingerprint/SQL-text labels.

## Phase 6 — Guards + prior-guard retarget (tests/architecture_test.rs)

- 14 architecture + 3 convention guards (ADR list).
- **Retarget** `adr055_no_rate_limit_or_connection_cap_surface` →
  `adr055_no_connection_cap_surface`. Extend ADR meta coverage through 056
  after the canonical ADR file lands, while preserving 049-055 coverage.

## Phase 7 — Fixtures (budget / config / coordinator tests)

14 required + 1 optional:

1. `default_zero_rate_preserves_current_admission_behavior`
2. `rate_flood_of_cheap_queries_rejects_without_concurrency_saturation`
3. `burst_capacity_admits_then_rejects`
4. `tokens_refill_with_injected_clock`
5. `slow_subgranularity_rate_still_accrues_over_time`
6. `rate_rejected_query_never_enters_admission_queue`
7. `token_consumed_on_pass_not_refunded_on_queue_timeout`
8. `token_consumed_on_pass_not_refunded_on_bind_error`
9. `token_consumed_on_pass_not_refunded_on_execution_error`
10. `adr054_evolved_retry_does_not_reconsume_rate_token`
11. `unconfigured_tenants_share_other_rate_bucket`
12. `configured_tenant_rate_bucket_isolated_from_other`
13. `rate_rejection_metric_uses_configured_plus_other_and_resource_rate`
14. `reads_and_writes_each_spend_one_top_level_token`

Optional: `rate_gate_works_with_055_queue_disabled`.

## Phase 8 — Evidence + roadmap

- `plans/impl/056_per_tenant_query_rate_admission_evidence.md`:
  `architecture_test adr056`, full `architecture_test`, `cargo test -p
  toron`, budget suite count, `clippy -p toron --all-targets -- -D warnings`,
  `git diff --check` (capped-linker procedure).
- Roadmap: 056 promoted (rate dimension; closes 055 D18 loop); **ADR 057 =
  connection caps (committed)** stays the next lead.

## Watch-items

1. **Rate-first placement** must be *before* the `admission` Some/None
   branch, else rate won't gate the legacy (queue-disabled) path.
2. **Refill remainder** — advance `last_refill` by *consumed* time, not to
   `now`, else repeated sub-interval attempts can prevent the next whole
   token from accruing.
   Fixture `slow_subgranularity_rate_still_accrues_over_time` pins it.
3. **Consume only on pass; never refund** — no `Drop`/release for rate.
4. **054 retry stays inside one admission** — no re-call of `try_admit`.
5. **`"other"` is one shared bucket** keyed `RateBucketKey::Other`, using
   the *default* budget's rate/burst — bounded, no per-tenant growth.
6. **Overflow** on long-idle refill: u128 intermediate + saturating, cap at
   burst (full-width `u64` time — never pack into 32 bits).
7. **Clock injection must not change 055** — the queue path still uses
   `tokio::time::timeout`; the injected clock only drives rate refill.

## Acceptance checklist

- [x] Phase 1: `TenantBudget` rate fields parse; disabled default; burst
      derive; back-compat.
- [x] Phase 2: injectable monotonic clock; 055 construction unchanged.
- [x] Phase 3: rate-first before the admission branch; consume-on-pass;
      remainder-preserving refill; shared `"other"`; no Drop/refund.
- [x] Phase 4: `RateLimited` → `AdmissionResource::Rate` → `Resource
      Exhausted`; 054 retry no re-spend.
- [x] Phase 5: existing bounded `resource="rate"` metric; no new family/gauge.
- [x] Phase 6-8: 14+3 guards, 055 guard retarget, 14 fixtures, prior guards
      pass, evidence + roadmap; no parser/listener/cluster drift.
