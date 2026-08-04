# Impl Plan 057: Two-Tier SQL Connection Admission

**Status:** Promoted, paired with `plans/adr/0057_sql_connection_admission.md`.
**Posture:** Single plan, Toron-ring only. New listener-edge limiter; no
coordinator `admit_query` change, no SQL wire / Wendelon / nilket /
fixture-worker change. Nine phases, dependency-ordered. **Default-disabled:
with both caps unset, every phase is byte-for-byte current listener
behavior.**

## Invariants carried in

- ADR 049-056 behaviorally unchanged when both caps are unset; their guards
  keep passing (055 connection-cap guard retargeted in place).
- `crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged;
  brand-clean / no-`*V1`. Node-local single daemon; SQL listener only;
  worker/observability paths untouched.

## Phase 0 — Audit (anchors to verify before edits)

- `daemon.rs`: `run_sql_accept_loop(...)` (the SQL accept path), the
  `connection_tasks: Arc<Mutex<JoinSet<()>>>` connection-task pool, and
  `SqlTenantSource::{LoopbackConfig(TenantId), …}`. The limiter is
  constructed here and passed only to the SQL loop.
- `network/tls.rs`: `accepted_sql_tenant(...)` (mTLS SAN → tenant),
  `ListenerRole::{Sql, WorkerTask, Observability}`.
- `quic.rs` / `QuicEndpoint`: `accept_connection()` **awaits the handshake**
  before returning `quinn::Connection` — the seam to split (D5). Confirm
  quinn's `Endpoint::accept()` → `Incoming` with `refuse()` is reachable,
  and the SQL transport's `max_idle_timeout` setting (D14).
- `distributed/budget.rs`: `TenantBudget`, `TenantBudgetPolicy`
  (`budget_for` / `is_configured_tenant` / `configured_tenant_ids`).
- `config.rs`: `[sql]` section (`SqlConfig`), `RawTenantBudget` /
  `RawTenantBudgetEntry`, `normalize_*`.
- `network/observability.rs`: `ResourceMetrics` / `listener_*` rendering.
- The guard to retarget: `adr055_no_connection_cap_surface`.

## Phase 1 — Config surface (config.rs, budget.rs)

- Tier 1: `SqlConfig.max_concurrent_connections: Option<NonZeroU32>` from
  `[sql].max_concurrent_connections` (raw `u32`, `0` ⇒ `None`).
- Tier 2: `TenantBudget.max_concurrent_connections: Option<NonZeroU32>`
  from `TenantBudget` raw fields (`0`/omitted ⇒ `None`).
- Config tests: omitted/zero ⇒ `None` (disabled); existing configs valid.

## Phase 2 — QUIC Incoming seam (quic.rs) (D5)

- Add a **SQL-only** helper, e.g. `accept_sql_incoming()`, that returns the
  `quinn::Incoming` decision point (or refactors `accept_connection` so the
  SQL caller drives accept/refuse). The helper lets the SQL loop call
  `Incoming::refuse()` *before* the server drives its handshake.
- **Worker / observability keep `accept_connection()` unchanged** — a
  separate code path so the guard can prove they're untouched.

## Phase 3 — `SqlConnectionLimiter` (network or daemon module) (D7-D10)

```rust
enum TenantConnectionKey { Configured(TenantId), Other }

struct SqlConnectionState {
    global_active: u32,
    tenant_active: HashMap<TenantConnectionKey, u32>,
}
struct SqlConnectionLimiter {
    max_total: Option<NonZeroU32>,           // Tier 1
    tenant_policy: TenantBudgetPolicy,       // Tier 2 limits + configured set
    state: Mutex<SqlConnectionState>,        // one short lock, no await held
}
```

- `try_acquire_tier1(&self) -> Result<Tier1Permit, ConnectionLimitExceeded>`
  — disabled returns a no-op permit; `Err` means refuse because
  `global_active >= max_total`; otherwise inc + permit.
- `try_acquire_tier2(&self, tenant_id) -> Result<Tier2Permit,
  ConnectionLimitExceeded>` — key via `Configured|Other`, limit via
  `tenant_policy.budget_for(tenant)`; disabled returns a no-op permit, over
  cap ⇒ `Err`, otherwise inc + permit.
- **RAII permits** decrement on `Drop` (Tier 1 global; Tier 2 tenant key).
  All exit paths covered; no reaper. Disabled tiers return a no-op permit.

## Phase 4 — Wire into `run_sql_accept_loop` (daemon.rs) (D3-D5, D9)

1. Await an incoming SQL attempt via the Phase-2 helper.
2. `try_acquire_tier1()`; if `Err` → `Incoming::refuse()` + Tier 1 reject
   metric; continue. Disabled Tier 1 returns a no-op permit, not a reject.
3. Drive the handshake (holding the Tier 1 permit). On handshake failure →
   permit drops (releases).
4. Resolve tenant (`accepted_sql_tenant` / `SqlTenantSource::LoopbackConfig`).
   On failure → keep the existing tenant-rejected close/metric path; the
   Tier 1 permit drops.
5. `try_acquire_tier2(tenant)`; if `Err` → close the connection with the
   stable reason + Tier 2 reject metric; permits drop.
6. Spawn the connection task into `connection_tasks`, **moving both permits
   into the task** so they release exactly when it exits.

## Phase 5 — Reject surfaces (daemon.rs, quic.rs) (D10)

- Tier 1: `Incoming::refuse()` (no app reason available) + metric/log only.
- Tier 2: QUIC connection close with application reason *"tenant sql
  connection limit exhausted"*. **No SQL `ResourceExhausted` frame, no
  stream accepted** on either tier.

## Phase 6 — Metrics (network/observability.rs) (D11)

- Tier 1 aggregate: `sql_connections_active`, `sql_connection_rejections_total`.
- Tier 2 bounded: `tenant_sql_connections_active{tenant_id="7|other"}`,
  `tenant_sql_connection_rejections_total{tenant_id="7|other"}`.
- General `listener_rejects_total{listener,reason}` gains a bounded
  `connection_limit` reason; **no tenant label** on general listener
  families (052 D12).

## Phase 7 — Guards + reconciliation (tests/architecture_test.rs)

- 18 architecture + 3 convention guards (ADR list), incl. the
  `quinn::Incoming`-before-handshake guard, one-lock-not-atomics,
  configured+other keys, RAII-no-reaper, Tier1/Tier2-no-SQL-frame,
  worker/observability-never-capped, and `sql_quic_idle_timeout_not_disabled`.
- **Retarget** `adr055_no_connection_cap_surface` to forbid only worker/
  observability/controller connection caps. Extend the meta-guard through
  056.

## Phase 8 — Fixtures

18 required + 1 optional (ADR list).

## Phase 9 — Evidence + roadmap

- `plans/impl/0057_sql_connection_admission_evidence.md`:
  `architecture_test adr057`, full `architecture_test`, `cargo test -p
  toron`, listener/daemon suite counts, `clippy -p toron --all-targets -- -D
  warnings`, `git diff --check` (capped-linker procedure).
- Roadmap: 057 promoted; next candidates (parser/front-end throttle on
  evidence; descriptor-refresh hardening; OTLP; audit; SQL widening;
  JWT/OIDC/SPIFFE; gRPC/Arrow-Flight/Postgres frontends).

## Watch-items

1. **The `quinn::Incoming` seam is the only real new mechanism.** Keep
   worker/observability on the unchanged `accept_connection()` (guard #14);
   the SQL helper is the sole refuse-before-handshake path.
2. **Tier 1 permit lifetime** spans refuse / handshake-fail / tenant-fail /
   Tier-2-reject / stream-fail / task-exit — RAII on every path (D9).
3. **One lock, never across the handshake/await.** Acquire/refuse is a
   quick critical section; the handshake happens *after* the Tier 1 permit
   is taken but *outside* the lock.
4. **Tier 1 has no application close reason** (`Incoming::refuse()`); its
   diagnostic is metrics/log only — don't fabricate a phrase (D10).
5. **No SQL frame on reject** — never accept a stream to send an error; it
   burns the resource the cap protects.
6. **Don't disable quinn's idle timeout** (D14) — Tier 1 caps concurrency,
   not per-attempt dwell.
7. **Disabled (`None`) ⇒ byte-for-byte current behavior** — the headline
   compat guard (#13) + fixture #1.
8. **Bounded `Configured|Other` keys** — no durable entry per transient
   unconfigured tenant.

## Acceptance checklist

- [ ] Phase 1: `Option<NonZeroU32>` Tier 1 + Tier 2 config; disabled
      defaults; back-compat.
- [ ] Phase 2: SQL-only `Incoming` helper; worker/observability unchanged.
- [ ] Phase 3-4: one-lock limiter; Tier 1 at Incoming, Tier 2 post-auth;
      RAII permits moved into the connection task.
- [ ] Phase 5: Tier 1 refuse (metric-only), Tier 2 close-with-reason; no
      SQL frame either tier.
- [ ] Phase 6: split aggregate/bounded metrics; general listener
      tenant-label-free.
- [ ] Phase 7-9: 18+3 guards, 055 retarget, idle-timeout guard, 18
      fixtures, prior guards pass, evidence + roadmap; no
      worker/observability/parser/cluster drift.
