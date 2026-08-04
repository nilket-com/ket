# ADR 056: Per-Tenant Query-Rate Admission

**Status:** Promoted (2-of-3 — Claude + Codex; Gemini minority on
placement/atomics/metric recorded)
**Seed:** ADR 055 Out-of-Scope + roadmap "ADR 056 candidates (LEAD,
committed)": the orthogonal hard-reject dimension 050/052/055 are blind to.
**Council:** iter-0 three shapes (Claude rate-first in-controller; Codex
rate after-055-grant; Gemini pre-parse + new error). iter-1 Codex moved to
rate-first in-controller (the consume-on-pass / queued-path composition
argument was decisive); Gemini conceded the error surface (reuse
`ResourceExhausted`) and config location (`TenantBudget`). iter-2 Claude +
Codex fully converged. iter-3/4/5 dual adr+impl: Claude and Codex stabilized
the identical 14-decision controller-integrated D-set; **Gemini held a
minority pre-parse / lock-free / separate-metric design**, adjudicated
against on the merits (recorded below). Promoted 2-of-3 with recorded
dissent, as ADR 055 did for its split decision.
**Category:** Toron compute-ring substrate ADR — coordinator query
admission. Exterior Substrate → Toron home. Node-local single daemon; no
Wendelon server change, no SQL wire change.

## Summary

050 governs footprint (bytes), 052 governs occupancy (concurrent queries),
055 governs *waiting* for those two. None governs **arrival rate** — so a
flood of cheap, fast queries is admitted, runs briefly, releases, and the
next is admitted, saturating the coordinator while memory and concurrency
read "fine." ADR 056 adds **rate** as a third orthogonal hard-reject
dimension: a per-tenant token bucket checked **first** inside the existing
`TenantAdmissionController::try_admit`. A token is **consumed on pass and
never refunded**; an exhausted bucket **hard-rejects** with
`ResourceExhausted` (no queue, no wait). Lazy refill from an injected
monotonic clock; **no background task**. With `max_queries_per_second = 0`
(the default) the gate is skipped — behavior is byte-for-byte today's. It
is **not** a reversal — it extends the 050/052 hard-reject posture, and its
telemetry closes the ADR 055 D18 loop (calibrating 055's WFQ weights).

## Motivation

The cheap-flood vector is real and *structurally* unguarded (provable by
construction: no Toron path limits arrival rate). This is the same class of
gap 052 closed for 050 — a resource vector the prior model can't see — and
clears the same evidence bar. Rate also produces the per-tenant arrival
signal ADR 055 D18 deferred its weight/borrow tuning to. The existing
SELECT retry loop, ADR 053 governed bind-miss sync, and ADR 054 reactive
invalidation all run **under** one admission and are behaviorally unchanged.

## Decisions (14, Promotion-Locked)

**D1 Placement.** Toron Exterior Substrate; implemented **inside
`TenantAdmissionController::try_admit`**; node-local single daemon. No
Wendelon server, SQL wire, `crates/nilket` runtime, or
`toron_fixture_worker.rs` change.

**D2 Scope.** **Top-level query arrival rate only.** No parser throttle,
byte-rate, row-rate, CPU-rate, storage quota, listener/connection cap,
worker-transport / internal-fragment cap, or cluster/global limiter.

**D3 Hard-reject dimension, not a reversal.** Rate extends the ADR 050/052
hard-reject admission posture. It introduces **no** queue/wait/borrow/
fair-share *on rate* — those stay 055's, untouched (052 D3 still holds).

**D4 Rate-first.** The token bucket is the **first** check in `try_admit`,
**before** the `admission`-disabled/enabled branch and before owned grant /
queue / borrow. So rate gates *both* the legacy (queue-disabled) and the
055-queued paths, and a rate-rejected request **never enters the 055
queue** and never gets an `AdmissionReservation`.

**D5 Consume-on-pass, never refund.** Passing the gate spends exactly one
token. It is **never** refunded — not on queue timeout, queue cancellation,
bind failure, descriptor-sync failure, execution failure, or completion.
There is **no rate `Drop`/RAII/release path** (the asymmetry vs
memory/concurrency is intentional; rate governs *arrival*, not success).

**D6 Lazy refill, no background task.** Refill is computed on access from an
**injectable monotonic clock**; production uses the real monotonic clock,
tests advance it deterministically (no `sleep`). No refill task, timer
wheel, or scheduler actor.

**D7 Controller lock over lock-free atomics.** Bucket state lives under the
controller's existing synchronous lock that already makes 055's
multi-resource grant atomic. **No** separate `TenantRateLimiter`,
`AtomicU64` packing, CAS loop, or lock-free fast path is a promoted
requirement — a token bucket is two fields that move together
(`tokens`, `last_refill`), and a lock-free fast path can be a later
optimization on profiling evidence, not part of 056's correctness contract.

**D8 Config.** `TenantBudget` gains `max_queries_per_second: u32` and
`query_burst_capacity: u32`. `max_queries_per_second = 0` ⇒ disabled
(default). `query_burst_capacity = 0` ⇒ derive burst from the rate;
effective burst is nonzero whenever rate is enabled. Rate lives in
`TenantBudget`, not `[admission]`. Existing configs stay valid.

**D9 Configured / `"other"` state.** Configured tenants get **dedicated**
buckets; **all unconfigured tenants share one `"other"` bucket** from the
default-tenant rate (unlimited if the default rate is 0). Bounded
`O(configured+1)`, sweep-free — matches the 052/055 cardinality model and
avoids the transient-`"other"`-map growth the 055 review flagged.

**D10 Error surface.** New `TenantAdmissionRejection::RateLimited {
tenant_id }` → `SqlErrorCode::ResourceExhausted` with a distinct diagnostic
(*"tenant query rate limit exhausted for tenant <id>"*). **No** new SQL
error code or wire variant; clients differentiate on the diagnostic string
(covers 429-style backoff).

**D11 Metrics.** Add `AdmissionResource::Rate` to the **existing** bounded
`tenant_admission_rejections_total{resource="rate"}` family
(configured + `"other"`). **No** separate `tenant_rate_limit_rejections_
total` family, **no** token-availability gauge, no
query/table/object/descriptor/fingerprint/SQL-text labels.

**D12 Retry isolation.** A top-level query spends **at most one** token.
ADR 053 bind-miss sync and ADR 054 descriptor-evolved retry run *inside*
the already-admitted statement and **do not re-consume** a token.

**D13 Statement parity / internal bypass.** SELECT, INSERT, INSERT SELECT,
CTAS, DELETE, CREATE, DROP each spend one token when admitted at
`admit_query`. Internal worker/shuffle/fragment tasks are **not** rate-
gated (they never enter top-level admission).

**D14 No product drift.** No Wendelon server / SQL wire / DDL change; no
`crates/nilket` runtime dep; `toron_fixture_worker.rs` unchanged;
brand-clean / no-`*V1`.

**Single-plan posture; Toron-ring only; node-local.**

## Architecture Guards (14, Promotion-Locked)

1. `adr056_rate_is_outermost_controller_gate`
2. `adr056_rate_gate_runs_before_admission_enabled_branch`
3. `adr056_rate_rejection_never_enters_queue_or_reservation`
4. `adr056_rate_token_consumed_on_pass_never_refunded`
5. `adr056_rate_has_no_reservation_or_drop_release_path`
6. `adr056_lazy_refill_uses_injected_clock_no_background_task`
7. `adr056_no_lock_free_or_separate_rate_limiter_component`
8. `adr056_default_zero_rate_preserves_current_behavior`
9. `adr056_configured_tenants_have_buckets_other_is_shared`
10. `adr056_resource_exhausted_reused_no_new_sql_code`
11. `adr056_rate_uses_existing_bounded_rejection_metric_no_new_family`
12. `adr056_no_preparse_listener_connection_or_worker_transport_rate`
13. `adr056_descriptor_retry_does_not_reconsume_rate_token`
14. `adr056_no_wendelon_sql_wire_nilket_or_fixture_worker_change`

**Convention guards:** `adr056_nilket_root_stays_runtime_free`,
`adr056_fixture_worker_source_unchanged`,
`adr056_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Prior-guard reconciliation:** retarget ADR 055's
`adr055_no_rate_limit_or_connection_cap_surface` →
`adr055_no_connection_cap_surface` (rate now legitimately exists; connection
caps still → ADR 057). ADR 049-055 meta-guard persists.

## Integration Fixtures (14 required + 1 optional)

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

**Optional:** `rate_gate_works_with_055_queue_disabled`.

## Out of Scope

- **Parser-flood protection / pre-parse shedding.** The coordinator gates a
  parsed `LogicalPlan`; SQL→plan happens upstream. Pre-parse shedding is a
  *listener/frontend* concern that needs the pre-auth tenant identity ADR
  057 owns — a future throttle *on measured evidence*, not 056.
- Listener / connection caps (→ ADR 057); worker-transport / internal-task
  rate; byte/row/CPU rate; storage quotas.
- Wait-for-token / rate queue / borrowing on rate (it's a hard reject).
- Cluster-wide / distributed rate (052 D3 node-local boundary).
- New SQL error code / wire variant; separate rate metric family; token
  gauge.
- Fractional configured rates / sub-1-per-second limits such as "30/min".
  The V1 config is whole QPS; the required remainder-preserving refill still
  applies to repeated sub-second accesses at whole-QPS rates.

## Council History

- **iter-0:** Claude rate-first in-controller; Codex rate after-055-grant;
  Gemini pre-parse + `RateLimitExceeded`.
- **iter-1:** Codex moved to rate-first in-controller — consume-on-pass +
  the queued-path composition argument were decisive (after-grant can't
  define "drop the reservation" for a request 055 *queued*). Gemini conceded
  the error surface and config location.
- **iter-2:** Claude + Codex fully converged — `admit_query` in-controller,
  shared `"other"`, controller-lock-not-atomics, integer tokens, no gauge;
  parser-flood named out-of-scope (→ post-057).
- **iter-3/4/5 (dual adr+impl):** Claude and Codex stabilized the identical
  14-decision controller-integrated D-set.
- **Minority path (Gemini, recorded):** pre-parse placement via a separate
  `TenantRateLimiter`, lock-free packed-`AtomicU64` bucket, and a separate
  `tenant_rate_limit_rejections_total` family. Adjudicated against on the
  merits: (a) "pre-parse in the coordinator" is self-contradictory — the
  coordinator already holds a parsed `LogicalPlan`; true pre-parse is
  listener/057 territory and splits the single admission seam; (b) the
  `AtomicU64` is lock-free only to *update* an existing entry — inserting a
  new tenant's bucket still needs map synchronization, so it's a lock *plus*
  atomics, more machinery than the one short controller lock; (c) packing a
  millisecond timestamp in 32 bits **wraps in ~49.7 days**, corrupting
  refill on a long-lived daemon; (d) setting `timestamp = now` per access
  loses the remainder, so repeated sub-interval attempts can prevent the
  next whole token from accruing; (e) `HashMap<TenantId, _>` for all
  tenants reopens the unbounded-`"other"` growth 055 just fixed; (f) a
  separate metric family fragments the 050/052/055/056 admission telemetry.
  The controller-lock
  design avoids all six by construction. Gemini's valid concern (parser CPU)
  is preserved as named Out-of-Scope, routed to a post-057 listener throttle
  on evidence.

## Promotion criteria met

1. D-set 14, 2-of-3 (Claude+Codex); Gemini dissent recorded with reasons.
2. Guards 14 + 3 convention; 055 no-rate-surface guard retargeted; ADR
   049-055 meta-guard.
3. Fixtures 14 + 1 optional.
4. Toron-ring; folds into the 055 controller; no Wendelon / SQL wire change.
5. Placement: Exterior Substrate → Toron home; node-local scale boundary.
6. Brand-clean / no-`*V1`; fixture worker unchanged; nilket runtime-free.
7. ADR 049-055 invariants preserved; inert when `max_queries_per_second=0`.

## Conclusion

ADR 056 closes the arrival-rate vector with a thin, default-disabled,
node-local token bucket folded into the 055 controller as the outermost
hard-reject gate — consume-on-pass, never refund, lazy refill, no
background task, controller-lock not lock-free. It reuses the existing error
and metric surfaces, and its telemetry feeds the 055 D18 tuning loop.
Migration: `plans/impl/0056_per_tenant_query_rate_admission.md`.
