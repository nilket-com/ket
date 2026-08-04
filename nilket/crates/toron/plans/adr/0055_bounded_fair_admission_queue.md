# ADR 055: Bounded Fair Admission Queue

**Status:** Promoted
**Seed:** ADR 054 D17 — Governance Phase 3, "explicitly framed as the ADR
050 D7 / ADR 052 D3-D14 hard-reject reversal, its own cycle, not a 054
appendix."
**Council:** Claude, Codex, Gemini. iter-0 three shapes (Claude
rate-first; Codex reversal-first; Gemini all-in bundle). iter-1 Gemini
dropped QPS + the new wire error + preemption; all three converged on a
pre-bind unified queue reusing `ResourceExhausted`. iter-2 Claude conceded
the ADR number — Codex's **default-disabled** decision answers the
"weights need telemetry" objection (an opt-in feature needs no
fleet-calibrated defaults) — fixing 055 as the reversal and the 3-ADR
sequence (055/056/057). iter-3 Claude adjudicated the residual mechanism
fork on **correctness** (borrow-first atomics starve queued waiters; the
instantaneous fairness ratio is a TOCTOU race; independent atomics cannot
make an atomic multi-resource grant) toward Codex's queue-first inline
pump; Codex concurred. iter-4/5 dual adr+impl; **Gemini iter-5 conceded
the scheduler core (3-of-3): unified node-local scheduler over per-tenant
semaphores, node-local slack, integer fairness.** The lone split decision
— client connection caps — is **2-of-3 (Claude+Codex) out → ADR 057**, a
listener/auth seam, not a query-admission one.
**Category:** Toron compute-ring substrate ADR — coordinator query
admission. Exterior Substrate → Toron home. Node-local single daemon; no
Wendelon server change, no SQL wire change.

## Summary

ADR 050/052 admit a query by **immediately reserving** owned per-tenant
memory + concurrency before bind, and **hard-rejecting**
(`ResourceExhausted`) when either is full. ADR 055 reverses that posture
**as an opt-in**: when owned capacity is full, a query may **wait in a
bounded per-tenant FIFO** and be granted later from its **owned capacity
or an explicit, node-local, configured shared borrow pool**, in
**deterministic weighted-deficit order** across tenants. Borrowing is
**non-preemptive** (in-flight work is never killed) and **never consumes
another tenant's owned budget**. With no `[admission]` config, behavior
is **byte-for-byte ADR 050/052 hard reject**. Per-tenant queue/borrow
fields default to zero, so an explicit `[admission]` section still needs a
tenant-level opt-in before a tenant can queue or borrow. Query-rate token
buckets (ADR 056) and listener connection caps (ADR 057) are **out of
scope**, named follow-ups.

## Motivation

Pure hard-reject gives strong isolation but, under bursty multi-tenant
load, converts transient contention into client-visible failure + retry
storms while other tenants sit idle. The ADR 052 instrument
(`tenant_admission_rejections_total`) exists precisely to surface this.
ADR 055 lets an operator trade immediate rejection for **bounded waiting +
explicit oversubscription**, without weakening the owned-budget guarantee
that 050/052 established. The existing SELECT retry loop, the ADR 053
governed bind-miss sync, and the ADR 054 reactive invalidation all run
**under** an admission reservation and are behaviorally unchanged.

## Decisions (18, Promotion-Locked)

**D1: Placement + scale boundary.** Toron Exterior Substrate,
`QueryCoordinator` admission, **node-local single daemon**. No Wendelon
server, SQL wire, descriptor-registry, `crates/nilket` runtime, or
`toron_fixture_worker.rs` change. ADR 052 D3's rejection of cluster-wide
caps / distributed semaphore / global fair-share stands — "slack" here is
a *configured node-local pool*, never "whatever the cluster isn't using."

**D2: Scope.** Reverses the ADR 050 D7 / ADR 052 D3 immediate hard-reject
posture for **query memory + concurrency** admission via bounded queueing,
explicit shared-pool borrowing, and weighted fair grants. Nothing else.

**D3: Split.** Query-rate token buckets → **ADR 056**; listener/client
connection caps → **ADR 057** (including the pre-auth tenant-identity
question, 052 D12). No `max_queries_per_second`, token bucket,
`max_concurrent_connections`, listener counter, or connection-close
protocol surface appears in ADR 055.

**D4: Admission seam.** Queueing happens **before bind** at the existing
`admit_query` boundary. A queued entry holds only admission metadata
(tenant id, requested memory, requested concurrency = 1, enqueue order,
deadline, grant waiter) — **no** SQL string, logical/bound plan,
descriptor, storage handle, memory reservation, or concurrency
reservation.

**D5: Default = hard reject.** Omitted `[admission]` config preserves ADR
050/052 behavior exactly. Per-tenant queue/borrow fields default to zero,
so even an explicit `[admission]` section requires tenant-level opt-in
before a tenant can queue or borrow; queueing and borrowing are opt-in.

**D6: One admission controller.** `TenantAdmissionController` owns a
**single synchronous state lock** over owned usage, borrowed usage, the
shared pool, per-tenant FIFOs, per-tenant deficits, deadlines, and
cancellation. The lock is held only to enqueue / grant / cancel / time out
/ release / pump — **never across bind, descriptor sync, execution, or
grant-wait awaits**. Grant delivery is via a oneshot waiter handle awaited
outside the lock.

**D7: Fast path + no-bypass-when-waiters.** A request skips the queue only
when queueing is disabled for its tenant, **or** the tenant has **no
current waiters** and owned memory + concurrency reserve immediately. If
the tenant already has waiters, a new request **must** enqueue behind them
— the anti-starvation invariant; fresh arrivals never jump parked
requests.

**D8: Contended path.** If owned capacity can't grant now but the request
*could* fit later (owned + borrow caps), enqueue in the tenant FIFO.
Reject immediately with `ResourceExhausted` if queueing is disabled, the
queue is full, the queue wait budget is zero, or the request can **never**
fit within owned + max-borrow.

**D9: Explicit shared pool.** Borrowing draws **only** from configured
node-local `shared_memory_bytes` / `shared_concurrent_queries`, bounded by
per-tenant `max_borrowed_memory_bytes` / `max_borrowed_concurrent_queries`.
**No tenant ever consumes another tenant's owned configured budget.**

**D10: Grant scheduler.** The grant pump examines tenant queue heads by
**deterministic weighted deficit round-robin** (positive-integer
`admission_weight`, default 1), **FIFO within a tenant**. A grant may use
owned capacity, shared borrowed capacity, or both — but the memory +
concurrency (+ borrow) decision is **one atomic controller decision**, no
partial-grant rollback.

**D11: Inline pump, no background actor.** The pump runs **synchronously**
on enqueue, reservation release, timeout cleanup, and cancellation
cleanup. "Queue-first" is **not** "wait-first": a contended request that
can be served now is granted in the same call that enqueues it. No
scheduler task, poll loop, or refill thread.

**D12: Timeout / cancellation.** Backlog is bounded by the global
`[admission].max_queued_queries` and the tenant budget's
`max_queued_queries`; individual waits are bounded by
`max_queue_wait_ms`. Timeout and cancellation remove the waiter, **leak no
grant or reservation**, and **pump** (a freed slot may admit others).
Waiters have a stable controller-local identity so cleanup removes exactly
the queued entry that timed out or was canceled; the queued wait handle
owns that cleanup so task cancellation cannot strand a waiter.

**D13: No preemption.** In-flight work — including borrowed work — is never
killed or revoked to restore fairness. Fairness affects **future grants
only**. No budget-driven cancellation token reaches `Executor` /
`StorageScan`.

**D14: RAII reservation.** A granted `AdmissionReservation` releases owned
**and** borrowed resources on drop; release pumps waiters. Error paths
must not require a skippable manual release.

**D15: Descriptor isolation.** ADR 053 sync and ADR 054 invalidation are
**unchanged**. A descriptor bind-miss happens only **after** admission
grant; a 053-throttled bind-miss surfaces `ResourceExhausted` and
**releases** the admission reservation. An ADR 054 SELECT evolved-retry
stays **inside the same admission reservation** and **does not re-enqueue**.

**D16: Observability.** Queue-depth / borrowed-memory / borrowed-concurrency
gauges and queue-timeout / queue-full counters follow **ADR 052's** bounded
`configured-tenant | "other"` cardinality rule (`O(configured+1)` series).
No table / query / object / namespace / descriptor / fingerprint / SQL-text
labels. ADR 052 is the label precedent — **not** the aggregate-only ADR
050/054 metrics.

**D17: Error surface.** Queue full, queue timeout, cancellation-after-
timeout, queue-disabled, and impossible-request all surface as
`SqlErrorCode::ResourceExhausted` with diagnostic strings distinguishing
the cause. **No** new SQL error code or wire variant.

**D18: Calibration honesty.** `admission_weight` and `max_borrowed_*`
defaults ship conservative and operator-configured. ADR 056 rate telemetry
may later improve tuning; ADR 055 needs **no** fleet-calibrated defaults
because it is opt-in and default-disabled (the resolved residue of the
iter-0 evidence-first position).

**Single-plan posture; Toron-ring only.**

## Architecture Guards (16, Promotion-Locked)

1. `adr055_queue_lives_at_admit_query_before_bind`
2. `adr055_queue_entries_hold_no_plan_descriptor_or_reservation`
3. `adr055_omitted_config_preserves_hard_reject`
4. `adr055_no_queue_bypass_when_tenant_has_waiters`
5. `adr055_borrowing_uses_explicit_shared_pool_only`
6. `adr055_no_borrow_from_other_tenant_owned_budget`
7. `adr055_grants_decide_memory_concurrency_and_borrow_atomically`
8. `adr055_weighted_scheduler_uses_integer_deficit_rr`
9. `adr055_no_preemption_or_mid_query_revocation`
10. `adr055_no_background_scheduler_actor`
11. `adr055_admission_lock_not_held_across_await`
12. `adr055_no_rate_limit_or_connection_cap_surface`
13. `adr055_no_descriptor_registry_refresh_or_sync_change`
14. `adr055_no_wendelon_server_or_sql_wire_change`
15. `adr055_resource_exhausted_reused_no_new_sql_code`
16. `adr055_metrics_bounded_configured_plus_other`

**Convention guards:** `adr055_nilket_root_stays_runtime_free`,
`adr055_fixture_worker_source_unchanged`,
`adr055_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Prior-guard reconciliation:** retarget the ADR 050/052 hard-reject
guards in place — they currently assert "admission is immediate reject";
narrow them to "admission is immediate reject **when queueing is
unconfigured**," with the opt-in queue path now allowed and pinned
(continuous-refactor, as ADR 054 did for the 053 evolved-path guard). The
ADR 049-054 meta-guard persists.

## Integration Fixtures (16 required + 1 optional)

1. `omitted_admission_config_preserves_memory_and_concurrency_hard_reject`
2. `queue_disabled_for_tenant_rejects_immediately`
3. `queue_full_rejects_immediately`
4. `request_that_can_never_fit_owned_plus_borrow_rejects_immediately`
5. `queue_timeout_removes_waiter_and_returns_resource_exhausted`
6. `cancelled_queued_request_cannot_receive_a_stale_grant`
7. `release_grants_next_waiter_without_leaking_owned_or_borrowed`
8. `same_tenant_fifo_holds_new_request_cannot_bypass_existing_waiter`
9. `weighted_tenants_receive_deterministic_integer_deficit_grant_order`
10. `borrowed_memory_and_concurrency_never_exceed_shared_pool_caps`
11. `per_tenant_borrow_never_exceeds_tenant_borrow_caps`
12. `tenant_a_cannot_consume_tenant_b_owned_budget`
13. `borrowed_in_flight_query_not_preempted_when_other_tenant_arrives`
14. `bind_miss_after_grant_still_resource_exhausted_and_releases_admission`
15. `adr054_evolved_retry_stays_inside_one_admission_reservation`
16. `queue_and_borrow_metrics_are_configured_plus_other_no_table_labels`

**Optional:** `grant_pump_admits_immediately_when_capacity_exists_no_wait`.

## Out of Scope

- Query-rate / token-bucket admission (→ ADR 056).
- Listener / client connection caps at the QUIC/TLS edge (→ ADR 057),
  including the pre-auth tenant-identity question (052 D12).
- Worker-to-worker (`ToronTransport`) connection capping — risks shuffle
  deadlock; never in scope.
- Query preemption / mid-query revocation / borrowed-capacity reclaim.
- Cluster-wide caps, distributed semaphore, global fair-share (052 D3).
- Implicit "node slack" borrowing from other tenants' idle owned budget.
- New SQL error code / wire variant / DDL.
- Any Wendelon server change; any ADR 053/054 descriptor behavior change.

## Council History

- **iter-0:** Claude = rate (a new hard-reject dimension); Codex =
  reversal (queue+borrow+WFQ); Gemini = bundle (caps+queue+WFQ+QPS+metrics).
- **iter-1:** Gemini dropped QPS, preemption, and `RateLimitExceeded`; all
  three converged on a pre-bind unified queue reusing `ResourceExhausted`.
  Claude offered to yield the ADR number.
- **iter-2:** Claude conceded 055 = the reversal (Codex's default-disabled
  answers the "weights need telemetry" objection); settled the 3-ADR
  sequence, explicit pool over implicit slack, integer weights.
- **iter-3:** Claude adjudicated the residual mechanism fork on
  correctness — borrow-first + per-tenant semaphores **starve** queued
  waiters (freed capacity is grabbed by fresh arrivals; nothing wakes the
  oldest waiter); the instantaneous fairness ratio is a TOCTOU race;
  independent atomics cannot make an atomic multi-resource grant.
  Resolution: queue-first inline pump under one controller lock.
- **iter-4/5 (dual adr+impl):** Codex and Claude converged on the
  identical 18-decision controller. **Gemini iter-5 conceded the scheduler
  core (3-of-3):** unified node-local scheduler (not isolated semaphores),
  node-local slack (not cluster), integer fairness shares, no
  worker-transport caps.
- **Minority/split (recorded):** Gemini's client-edge connection cap is
  valid but lives at a different (listener/auth) seam — **split to ADR
  057** by 2-of-3 (Claude+Codex), preserving the ADR-by-ADR cadence.
  Gemini's earlier borrow-first atomics path is recorded rejected for the
  starvation, non-atomic-grant, and non-determinism reasons above.

## Promotion criteria met

1. D-set locked at 18; scheduler core 3-of-3; connection-cap split 2-of-3
   with the cadence rationale recorded.
2. Guards 16 + 3 convention; ADR 050/052 hard-reject guards retargeted;
   ADR 049-054 meta-guard.
3. Fixtures 16 required + 1 optional.
4. Toron-ring; reuses 050/052/053/054 surfaces; no Wendelon / SQL wire
   change.
5. Placement: Exterior Substrate → Toron home; node-local scale boundary.
6. Brand-clean / no-`*V1`; `toron_fixture_worker.rs` unchanged; `crates/
   nilket` runtime-free.
7. ADR 049-054 invariants preserved; the reversal is inert until opt-in
   (D5), so isolation / fail-closed semantics are unchanged by default.

## Conclusion

ADR 055 reverses the 050/052 hard-reject posture **safely**: bounded
waiting, explicit node-local oversubscription, deterministic weighted
fairness over queue heads, non-preemptive, inert-by-default. The two
adjacent governance turns are split off and named (rate → 056, connection
caps → 057). The migration is materialized in
`plans/impl/0055_bounded_fair_admission_queue.md`.
