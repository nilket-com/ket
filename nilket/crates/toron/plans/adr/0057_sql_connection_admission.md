# ADR 057: Two-Tier SQL Connection Admission

**Status:** Promoted (2-of-3 — Claude + Codex; Gemini minority on
ownership/atomics/reject-surface recorded)
**Seed:** ADR 056 Out-of-Scope + roadmap "ADR 057 candidates (LEAD,
committed)": per-tenant connection caps at the client listener edge, owning
the 052 D12 pre-auth tenant-identity question.
**Council:** iter-0 three shapes (Claude two-tier; Codex per-tenant-only;
Gemini per-tenant + SQL-frame reject). iter-1 crossover — Codex adopted
two-tier on the pre-auth argument; Claude conceded Tier 1 on a
handshake-timeout premise. iter-2 Codex's finding that
`QuicEndpoint::accept_connection()` awaits the handshake *unconditionally*
(so the timeout bounds fd dwell, not handshake-work rate) corrected that
premise; Claude re-converged to two-tier as a default-disabled opt-in.
iter-3 dual — Claude and Codex stabilized the identical design; **Gemini
held a minority coordinator-owned / per-bucket-atomics / SQL-frame design
with no global tier**, adjudicated against on the merits (recorded below).
Promoted 2-of-3 with recorded dissent, matching the recorded-minority
posture used in the 055/056 sequence.
**Category:** Toron compute-ring substrate ADR — SQL **listener edge**.
Exterior Substrate → Toron home. Node-local single daemon; no Wendelon
server change, no SQL wire change.

## Summary

050/052/055 govern query memory/concurrency/waiting; 056 governs query
arrival rate — all at coordinator `admit_query`, *after* a connection and
SQL frame exist. The **listener edge itself** is ungoverned: a tenant can
hold many authenticated SQL connections (fd / socket occupancy), and
unauthenticated attempts pay TLS handshake work unconditionally. ADR 057
adds two **default-disabled, node-local, hard-reject** caps at the SQL
listener: **Tier 1**, a tenant-agnostic global cap enforced at the
`quinn::Incoming` seam *before* the handshake; and **Tier 2**, a per-tenant
cap enforced *after* SQL tenant resolution. Reject is **transport
refuse/close**, never a SQL `ResourceExhausted` frame. SQL listener only —
**never** worker transport (shuffle deadlock) or observability. With both
caps unset the listener is byte-for-byte unchanged.

## Motivation

The roadmap's named threat is a tenant holding excess authenticated SQL
connections (fd/epoll/socket exhaustion) below every query-admission gate.
The 052 D12 wrinkle is real: a connection has **no stable tenant** until
the mTLS handshake yields a cert SAN (`accepted_sql_tenant`), so per-tenant
alone can't bound pre-identity work. And `accept_connection()` drives the
handshake before returning a `Connection`, so the existing idle timeout
bounds fd *dwell* but not handshake-work *rate*. Hence two tiers: a
pre-handshake global ceiling + a post-auth per-tenant cap.

## Decisions (15, Promotion-Locked)

**D1 Placement.** Toron Exterior Substrate, **SQL listener edge**. A
`SqlConnectionLimiter` constructed by the daemon and passed **only** to
`run_sql_accept_loop` — **not** a field on `DistributedQueryCoordinator`,
`TenantAdmissionController`, Wendelon, or the SQL wire layer. No
`crates/nilket` runtime / Wendelon server / SQL wire / fixture-worker
change.

**D2 Scope.** Concurrent SQL **client** connection admission on one node.
Out: worker task listener / `ToronTransport` / shuffle / internal fragments;
observability listeners; query admission/rate/queue/borrow/fairness;
parser/pre-parse throttling; IP-based throttles; cluster-wide accounting.

**D3 Two tiers.** **Tier 1** — tenant-agnostic global SQL cap at the
`quinn::Incoming` seam, bounding in-flight incoming attempts + established
SQL connections under one node-local ceiling. **Tier 2** — per-tenant cap
after tenant identity, bounding that tenant's established connections until
the connection task ends. Both disabled when unset.

**D4 Identity boundary.** Tier 2 keys on the existing SQL tenant source:
remote = mTLS client SAN via `accepted_sql_tenant`; loopback =
`SqlTenantSource::LoopbackConfig(config.node.tenant_id)`. Failed-auth /
pre-auth connections have no tenant and count **only** against Tier 1.
(JWT/OIDC/SPIFFE remains a separate carried-forward topic.)

**D5 QUIC Incoming seam.** Add a **SQL-only** helper around
`quinn::Endpoint::accept()` / `quinn::Incoming` (or split
`QuicEndpoint::accept_connection()`) so the SQL listener can: await an
incoming attempt → acquire a Tier 1 permit if enabled → `Incoming::refuse()`
when Tier 1 is full → otherwise drive the handshake holding the permit.
**Worker and observability paths keep `accept_connection()` unchanged.**
This is the one genuinely new mechanism in 057.

**D6 Config.** Two surfaces: `[sql].max_concurrent_connections` (Tier 1,
tenant-agnostic) and `TenantBudget.max_concurrent_connections` (Tier 2,
configured + `"other"`). Normalize disabled as **`Option<NonZeroU32>`**
(`None` = disabled), not "0 as a usable limit." The bare field name is
acceptable only because the table gives the scope; guards prevent worker/
observability/controller caps growing from it. Existing configs stay valid.

**D7 State ownership.** One `SqlConnectionLimiter` with a **single short
synchronous lock** over the global counter + a bounded
`HashMap<TenantConnectionKey, u32>`. **Not** `HashMap<_, AtomicUsize>`: the
map needs synchronization anyway, and a two-tier grant (check global **and**
per-tenant together, decrement both on release) is not an atomic any
independent counter buys. Lock never held across the handshake or an await.

**D8 Bounded tenant keys.** `TenantConnectionKey::{Configured(TenantId),
Other}`. Configured tenants get dedicated Tier 2 counters; all unconfigured
share one `"other"` counter under the default budget. No transient tenant
id allocates a durable entry (the 055/056 cardinality discipline).

**D9 RAII lifetime.** All accepted capacity is RAII permits. The **Tier 1**
permit is acquired before the handshake and released on refuse / handshake
failure / tenant-extract failure / Tier-2 rejection / stream-accept failure
/ connection-task exit. The **Tier 2** permit is acquired after tenant
resolution and released when the connection task exits (normal close, stream
error, cancellation, shutdown). No reaper, no background task.

**D10 Reject surface.** Transport refuse/close — **never** a SQL
`ResourceExhausted` frame (Tier 1 has no `Connection`/stream/`query_id`;
even Tier 2 sending a frame means accepting a stream = burning the resource
the cap protects). Tier 1 uses `Incoming::refuse()`, which carries **no
custom application reason**, so its diagnostic surface is **metrics/logging
only**. Tier 2 closes the QUIC connection with a stable application reason
(*"tenant sql connection limit exhausted"*) where the transport exposes one.
No new SQL error code or wire variant.

**D11 Metrics.** Tier 1 aggregate (tenant unknown): `sql_connections_active`,
`sql_connection_rejections_total`. Tier 2 bounded:
`tenant_sql_connections_active{tenant_id="7|other"}` and
`tenant_sql_connection_rejections_total{tenant_id="7|other"}`. General
listener families stay **tenant-label-free** (052 D12):
`listener_accepts_total{listener}`, `listener_rejects_total{listener,reason}`
with a bounded `connection_limit` reason. No table/query/SQL-text/IP labels.

**D12 Default-disabled compatibility.** Both caps unset ⇒ byte-for-byte
current listener behavior; existing configs valid.

**D13 Hard reject only.** No waiting for a connection slot, no borrow pool,
no WFQ, no preemption, no socket parking. Over cap ⇒ refuse or close. (055's
queue is for *query* admission — a different object.)

**D14 QUIC idle-timeout guard.** Tier 1 caps *concurrency*, not per-attempt
dwell. Keep a guard that Toron does **not** disable quinn's bounded
`max_idle_timeout` on the SQL transport, so a slow-handshake/idle flood
can't pin Tier 1's ceiling.

**D15 Prior-guard reconciliation.** Retarget
`adr055_no_connection_cap_surface` — 057 legitimately adds the SQL
connection-cap surface; the narrowed guard keeps forbidding worker-transport,
observability, and query-controller connection caps.

**Single-plan posture; Toron-ring only; node-local.**

## Architecture Guards (18, Promotion-Locked)

1. `adr057_sql_connection_limiter_lives_at_sql_accept_edge`
2. `adr057_global_cap_uses_quic_incoming_before_handshake`
3. `adr057_per_tenant_cap_checks_after_sql_tenant_resolution`
4. `adr057_loopback_sql_connection_cap_uses_config_tenant`
5. `adr057_connection_limiter_is_not_query_coordinator_or_admission_controller`
6. `adr057_connection_state_uses_one_lock_not_per_bucket_atomics`
7. `adr057_tenant_connection_keys_are_configured_plus_other`
8. `adr057_connection_permits_release_by_raii_no_reaper`
9. `adr057_tier1_refuses_incoming_without_sql_wire_error`
10. `adr057_tier2_closes_connection_without_sql_wire_error`
11. `adr057_metrics_split_global_aggregate_and_tenant_bounded`
12. `adr057_general_listener_metrics_remain_tenant_label_free`
13. `adr057_default_disabled_caps_preserve_current_behavior`
14. `adr057_no_worker_transport_or_observability_connection_cap`
15. `adr057_no_parser_query_rate_queue_borrow_or_cluster_connection_coordination`
16. `adr057_sql_quic_idle_timeout_not_disabled`
17. `adr057_no_wendelon_sql_wire_nilket_or_fixture_worker_change`
18. `adr057_prior_adr049_050_051_052_053_054_055_056_guards_continue_to_pass`

**Convention guards:** `adr057_nilket_root_stays_runtime_free`,
`adr057_fixture_worker_source_unchanged`,
`adr057_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Prior-guard reconciliation:** retarget `adr055_no_connection_cap_surface`
(D15). ADR 049-056 meta-guard persists.

## Integration Fixtures (18 required + 1 optional)

1. `default_disabled_connection_caps_preserve_current_connection_behavior`
2. `global_sql_connection_cap_refuses_excess_incoming_before_handshake`
3. `global_sql_connection_cap_releases_after_handshake_failure`
4. `remote_sql_tenant_connection_cap_closes_excess_after_san_resolution`
5. `loopback_sql_tenant_connection_cap_uses_config_tenant`
6. `unconfigured_tenants_share_other_sql_connection_cap`
7. `configured_tenant_connection_cap_isolated_from_other`
8. `connection_permits_release_after_normal_close`
9. `connection_permits_release_after_stream_accept_error`
10. `connection_permits_release_after_connection_task_cancel`
11. `tier1_connection_limit_rejection_does_not_emit_sql_frame`
12. `tier2_connection_limit_rejection_does_not_emit_sql_frame`
13. `global_connection_metrics_are_aggregate`
14. `tenant_connection_metrics_are_configured_plus_other`
15. `listener_reject_metric_records_connection_limit_without_tenant_label`
16. `worker_task_listener_ignores_connection_caps`
17. `observability_listener_ignores_connection_caps`
18. `connection_caps_do_not_call_admit_query`

**Optional:** `sql_quic_idle_timeout_remains_bounded`.

## Out of Scope

- Worker-transport / observability connection caps (deadlock / wrong seam).
- Query admission/rate/queue/borrow/fairness (050/052/055/056 own those).
- **Parser / pre-parse throttling** — 056 deferred it here, but it needs
  measured parser-CPU evidence and is a *statement-read* limit, not a
  connection cap; → a later evidence-driven ADR.
- IP-based / pre-TLS throttles (break multi-tenant proxies; no evidence).
- Cluster-wide / distributed connection accounting (052 D3 node-local).
- New SQL error code / wire variant; SQL-frame rejection for caps.

## Council History

- **iter-0:** Claude two-tier (Tier 1 as safety prerequisite); Codex
  per-tenant-only; Gemini per-tenant + SQL-frame reject.
- **iter-1:** crossover — Codex adopted two-tier; Claude conceded Tier 1 on
  a "handshake timeout bounds the pre-auth threat" premise.
- **iter-2:** Codex found `accept_connection()` drives the handshake
  *unconditionally* (timeout bounds fd dwell, not handshake-work rate) and
  that `Incoming::refuse()` is a clean pre-handshake seam — correcting the
  premise; Claude re-converged to two-tier as a default-disabled opt-in
  (the 055/056 pattern), not a speculative build.
- **iter-3 (dual):** Claude and Codex stabilized the identical design; Codex
  added `Option<NonZeroU32>` disabled, the Tier-1-refuse-has-no-reason
  correction, and the idle-timeout guard.
- **Minority path (Gemini, recorded):** tracker owned by
  `DistributedQueryCoordinator`, `HashMap<_, AtomicUsize>` per-bucket
  atomics, a SQL `ResourceExhausted` frame on reject, and **no Tier 1**.
  Adjudicated against: (a) the cap protects *listener-edge* lifetime, not
  query lifetime — coordinator ownership is the wrong layer; (b) a two-tier
  grant needs one lock over global + per-tenant, which independent atomics
  can't provide (and the map needs synchronization regardless); (c) sending
  a SQL frame requires *accepting a stream*, burning the resource the cap
  protects, and Tier 1 has no stream/`query_id` at all — a QUIC close
  code+reason gives graceful backoff without it; (d) per-tenant-only leaves
  the pre-auth handshake-work hole the roadmap assigned 057 to own. Gemini's
  fd-exhaustion motivation is *served* by Tier 1, just not by its mechanism.

## Promotion criteria met

1. D-set 15, 2-of-3 (Claude+Codex); Gemini dissent recorded with reasons.
2. Guards 18 + 3 convention; 055 connection-cap guard retargeted; ADR
   049-056 meta-guard.
3. Fixtures 18 + 1 optional.
4. Toron-ring; SQL-listener edge; no Wendelon / SQL wire change.
5. Placement: Exterior Substrate → Toron home; node-local scale boundary.
6. Brand-clean / no-`*V1`; fixture worker unchanged; nilket runtime-free.
7. ADR 049-056 invariants preserved; inert when both caps unset.

## Conclusion

ADR 057 governs the SQL listener edge with two default-disabled, node-local,
hard-reject caps: a pre-handshake global ceiling at `quinn::Incoming` and a
post-auth per-tenant cap, both reject-by-transport-close, both RAII-released
under one short lock, SQL-listener only. It owns the 052 D12 pre-auth
identity question by answering it (pre-auth ⇒ Tier 1 only). Parser-flood
protection stays the one named-and-deferred item. Migration:
`plans/impl/0057_sql_connection_admission.md`.
