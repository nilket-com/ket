# Impl Plan 055: Bounded Fair Admission Queue

**Status:** Promoted, paired with
`plans/adr/0055_bounded_fair_admission_queue.md`.
**Posture:** Single plan, Toron-ring only. Extends ADR 050/052 admission;
no new error code, no SQL wire / Wendelon change, no ADR 053/054
descriptor behavior change. Ten phases, dependency-ordered. **Default-
disabled: with no `[admission]` config, every phase is behavior-neutral vs
050/052.** When `[admission]` is present, per-tenant queue/borrow fields
still default to zero, so tenants must opt in before they can queue or
borrow.

## Invariants carried in

- ADR 049 routing, 050 memory, 051 bootstrap, 052 concurrency, 053 sync,
  054 invalidation — behaviorally unchanged when admission config is
  omitted; their guards keep passing (050/052 hard-reject guards
  retargeted in place).
- `crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged;
  brand-clean / no-`*V1`.
- Node-local single daemon (scale boundary, D1). No distributed semaphore
  / cluster cap. No rate-limit or connection-cap surface (→ 056/057).

## Phase 0 — Audit (verified anchors)

- `distributed/budget.rs`: `TenantBudget { memory_per_task,
  aggregate_memory, max_concurrent_queries, max_concurrent_descriptor_syncs }`
  + constructors; `TenantBudgetPolicy { default, per_tenant }` with
  `budget_for` / `is_configured_tenant` / `configured_tenant_ids` /
  `query_budget_for`; `TenantMemoryAccountant` / `TenantConcurrencyAccountant`
  (RAII reservations); `AdmissionResource::{Memory, Concurrency}` +
  `TenantAdmissionRejectionCounters` (configured + `"other"`).
- `coordinator.rs`: `fn admit_query(&self, context) -> Result<AdmittedQuery,
  QueryError>` reserves memory + concurrency before bind, hard-rejects
  `ResourceExhausted`; `AdmittedQuery` holds the reservations; the SELECT
  path (incl. the ADR 054 retry loop) and write paths run *under* it.
- `config.rs`: `[tenant_budgets]` parse → `TenantBudgetPolicy`.
- `network/observability.rs`: `ResourceMetrics` renders the 052 bounded
  `tenant_*` families; `MetricsRegistryConfig` threads the accountants.
- Architecture guards asserting "admission is immediate hard reject /
  no queue" (the retarget targets).

## Phase 1 — Config surface (config.rs, budget.rs)

- New `[admission]` → `AdmissionPolicy { max_queued_queries,
  max_queue_wait_ms, shared_memory_bytes, shared_concurrent_queries }`;
  absent section ⇒ `None` ⇒ queue/borrow disabled (D5).
- Extend `TenantBudget` with `admission_weight: NonZeroU32` (default 1),
  `max_queued_queries: u32` (default 0), `max_borrowed_memory_bytes:
  ByteCount` (default 0), `max_borrowed_concurrent_queries: u32` (default
  0). All-zero per-tenant ⇒ that tenant never queues/borrows.
- Config tests: omitted section/fields parse to disabled defaults;
  existing `[tenant_budgets]` files remain valid (back-compat).

## Phase 2 — Shared pool + borrowed accounting (budget.rs)

- `SharedAdmissionPool { memory: ByteCount, concurrency: u32 }` capacity +
  live borrowed counters; **plain fields under the controller lock**, not
  standalone atomics (atomicity is the controller's job — D10).
- Per-tenant borrowed tracking keyed `(configured | "other")`.
- Prefer sibling rejection counters (`queue_full`, `queue_timeout`) over
  widening `AdmissionResource`, to keep 052's surface stable.

## Phase 3 — `TenantAdmissionController` (budget.rs) — the core

```rust
pub struct TenantAdmissionController {
    policy: TenantBudgetPolicy,
    admission: Option<AdmissionPolicy>,        // None ⇒ disabled
    state: std::sync::Mutex<AdmissionState>,   // synchronous; no await held
    metrics: Arc<AdmissionMetrics>,
}
struct AdmissionState {
    tenant_memory: ...,        // owned usage (owns/wraps 050 accounting)
    tenant_concurrency: ...,   // owned usage (owns/wraps 052 accounting)
    shared: SharedAdmissionPool,
    queues: HashMap<TenantKey, VecDeque<Waiter>>,
    deficits: HashMap<TenantKey, u64>,         // integer deficit RR
}
struct Waiter {
    id: WaiterId,
    tenant,
    mem,
    // requested concurrency is implicit and fixed at 1
    seq,
    queue_wait_ms,
    grant: oneshot::Sender<Grant>,
}
struct QueuedAdmission {
    id: WaiterId,
    grant: oneshot::Receiver<Grant>,
    // Owns cancellation cleanup; drop before grant removes the waiter + pumps.
}
```

- `try_admit(tenant, mem) -> Admit` returns `Granted(AdmissionReservation)`,
  `Queued(QueuedAdmission)`, or `Rejected(reason)`; the grant receiver is
  awaited by the caller **outside the lock** (D6, D11).
- `QueuedAdmission` is an RAII wait handle: it carries the stable
  controller-local `WaiterId`, removes exactly that queued waiter and pumps
  on drop before grant, and disarms cleanup once grant ownership transfers.
- **Fast path (D7):** omitted `[admission]` config uses the ADR 050/052
  legacy owned grant/reject path; with admission configured, a tenant with
  no waiters and an immediately successful owned reserve gets `Granted`.
  If the tenant has waiters → fall through to enqueue (no bypass —
  anti-starvation).
- **Reject (D8):** disabled / queue full / never-fits(owned+borrow caps) /
  zero queue wait budget → `Rejected` (`ResourceExhausted`).
- **Enqueue + pump:** push `Waiter` (FIFO), then `pump()` synchronously.
- **`pump()` (D10):** loop — pick the eligible tenant by integer weighted
  deficit RR, take its FIFO head, attempt **one atomic grant** over {owned
  mem+conc → else owned + shared-borrow within per-tenant + pool caps}; on
  success dequeue, update deficits + borrowed usage, send on the oneshot;
  stop when no head is grantable. Immediate-grant returns `Granted` in the
  same call (queue-first ≠ wait-first).
- **`AdmissionReservation` (D14):** `Drop` returns owned + borrowed to
  `state` and calls `pump()`. No skippable manual release.
- Unit tests: deterministic weighted order; atomic grant has no partial
  rollback; FIFO-within-tenant; no-bypass-when-waiters; drop pumps next.

## Phase 4 — `admit_query` rewrite (coordinator.rs)

- Replace the direct memory+concurrency reservations with one
  `controller.try_admit(...)`. On `Queued(wait)`: `tokio::time::timeout(
  queue_wait, wait.recv()).await` — **the only await; the lock is not held**
  (guard #11). Timeout/cancel drops `wait`, removing the waiter, pumping,
  and returning `ResourceExhausted` (D12). On `Granted` → proceed.
- `AdmittedQuery` now wraps the `AdmissionReservation`; the SELECT path,
  the ADR 054 retry loop, and write paths run inside it unchanged.

## Phase 5 — Descriptor-path interaction (coordinator.rs) (D15)

- Confirm bind / 053 sync / 054 invalidation run **after** grant, under the
  held reservation; a 053-throttled bind-miss returns `ResourceExhausted`
  and drops the reservation (→ `pump`).
- The ADR 054 evolved-retry loop stays **inside** one reservation and does
  **not** re-enter `try_admit` (no re-enqueue). Add the regression fixture.

## Phase 6 — Metrics (budget.rs, network/observability.rs) (D16)

- The admission controller exposes metric slots for `queue_depth` /
  `borrowed_memory_bytes` / `borrowed_concurrency` gauges plus
  `queue_timeouts_total` / `queue_full_rejections_total` counters, all
  keyed configured + `"other"` (052 model). Render alongside
  `ResourceMetrics`; thread the controller through `MetricsRegistryConfig`.
  No table/query/object/descriptor/fingerprint labels.

## Phase 7 — Guards + prior-guard retarget (tests/architecture_test.rs)

- 16 architecture + 3 convention guards (ADR list).
- **Retarget** the ADR 050/052 hard-reject guards in place: from
  "admission rejects immediately" to "admission rejects immediately **when
  queueing is unconfigured**; the opt-in queue path is allowed and pinned."
- Extend the meta-guard to `adr049_..adr054_`.

## Phase 8 — Fixtures (config/coordinator/budget tests)

16 required + 1 optional (ADR list): omitted-config-preserves-hard-reject,
queue-disabled / queue-full / never-fits / timeout / cancel,
release-grants-next-no-leak, same-tenant-FIFO-no-bypass,
integer-weighted-deficit order, shared-pool + per-tenant borrow caps,
no-cross-tenant-owned-steal, no-preemption, bind-miss-after-grant releases
admission, 054-retry-stays-in-one-reservation, bounded metrics.

## Phase 9 — Evidence + roadmap

- `plans/impl/0055_bounded_fair_admission_queue_evidence.md`:
  `architecture_test adr055`, full `architecture_test`, `cargo test -p
  toron`, `clippy -p toron --all-targets -- -D warnings`, `git diff --check`
  (capped-linker procedure per house note).
- Roadmap: 055 promoted (the 050/052 reversal); **ADR 056 = rate admission
  (committed)**, **ADR 057 = connection caps (committed)**.

## Watch-items

1. **Lock discipline.** The controller `Mutex` is synchronous; the only
   await (oneshot recv under `timeout`) is *outside* it (guard #11).
2. **No-bypass-when-waiters (D7)** is the anti-starvation core — easy to
   regress by "optimizing" the fast path. Fixture #8 + guard #4 pin it.
3. **Atomic multi-resource grant (D10)** — memory + concurrency + borrow
   decided together; never `fetch_sub` one then discover the other is full.
4. **Timeout/cancel must `pump`** — a freed slot can admit others; cleanup
   that forgets to pump deadlocks waiters.
5. **Reservation drop during the 054 retry must not release+re-admit** —
   one reservation spans the retry loop (D15).
6. **Omitted `[admission]` == 050/052 byte-for-byte** — the headline guard
   (#3) and fixture #1; zero per-tenant queue/borrow fields keep tenants
   queue/borrow-disabled after an explicit `[admission]` section appears.
   The whole reversal is inert until opt-in.

## Acceptance checklist

- [x] Phase 1: `[admission]` + per-tenant fields parse; disabled defaults;
      back-compat with existing `[tenant_budgets]`.
- [x] Phase 2-3: controller with one sync lock; explicit shared pool;
      integer weighted-deficit pump; atomic grant; RAII release pumps.
- [x] Phase 4: `admit_query` queues before bind; await outside lock;
      timeout/cancel clean + pump.
- [x] Phase 5: 053/054 unchanged; bind-miss releases; 054 retry no
      re-enqueue.
- [x] Phase 6: bounded configured+other metrics, no table/query labels.
- [x] Phase 7-9: 16+3 guards, 050/052 retarget, 16 fixtures, prior guards
      pass, evidence + 3-ADR roadmap rewrite; no rate/connection-cap drift.
