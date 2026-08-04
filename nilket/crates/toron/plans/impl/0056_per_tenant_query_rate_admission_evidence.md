# ADR 056 Per-Tenant Query-Rate Admission Evidence

Date: 2026-05-21 America/Chicago.

## Passing Gates

- `cargo fmt -p toron`
  - Result: passed.
- `cargo check -p toron --tests`
  - Result: passed.
- `cargo clippy -p toron --all-targets -- -D warnings`
  - Result: passed.
- `cargo test -p toron --test architecture_test adr056 -- --test-threads=1`
  - Result: 18 passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1`
  - Result: 536 passed.
  - Covered: ADR 056 guards plus retargeted prior ADR 050/052/054/055
    guards that now allow the promoted rate-admission surface while keeping
    their original no-CPU/connection/storage/no-separate-limiter scope.
- `cargo test -p toron distributed::budget::tests:: -- --test-threads=1`
  - Result: 33 passed.
  - Covered: default-disabled behavior, burst exhaustion, lazy refill with
    injected clock, sub-granularity remainder preservation, shared
    `"other"` bucket, configured-tenant bucket isolation, rate rejection
    before queueing, consume-on-pass/no-refund on queue timeout, and the
    existing ADR 055 queue/borrow invariants.
- `cargo test -p toron coordinator::tests:: -- --test-threads=1`
  - Result: 20 passed.
  - Covered: rate rejection mapping to `ResourceExhausted`, cheap-query
    flood rejection without concurrency saturation, bind/execution errors
    not refunding tokens, reads/writes spending one top-level token, and
    ADR 054 descriptor retry staying inside one admission.
- `cargo test -p toron --test config_test -- --test-threads=1`
  - Result: 31 passed.
  - Covered: `max_queries_per_second` / `query_burst_capacity` parsing,
    omitted zero defaults, and burst derivation.
- `cargo test -p toron --test observability_listener_test -- --test-threads=1`
  - Result: 20 passed.
  - Covered: `tenant_admission_rejections_total{resource="rate"}` on the
    existing configured-plus-`"other"` metric family and absence of a
    separate rate-limit metric family.
- `cargo test -p toron -- --test-threads=6`
  - Result: passed.
  - Covered: full Toron test suite, including local TCP/TLS listener
    fixtures. Expected negative-path TLS handshake and OpenRaft shutdown
    diagnostics were emitted while the corresponding tests passed.
- `git diff --check`
  - Result: passed.

## Requirement Evidence

- Config surface:
  - `TenantBudget` carries `max_queries_per_second` and
    `query_burst_capacity`; both default to zero.
  - `max_queries_per_second = 0` skips the gate; `query_burst_capacity = 0`
    derives burst from the rate when enabled.
- Controller:
  - The token bucket gate is the first check in
    `TenantAdmissionControllerInner::try_admit`, before the `admission`
    enabled/disabled branch.
  - Bucket state is under the existing controller lock, with
    `RateBucketKey::Configured(TenantId)` and a single shared
    `RateBucketKey::Other`.
  - Refill is lazy from an injected monotonic clock and advances by consumed
    time, preserving sub-second remainder.
  - Passing the gate consumes one token; there is no rate reservation,
    `Drop`, release, or refund path.
- Coordinator:
  - `TenantAdmissionRejection::RateLimited` increments
    `AdmissionResource::Rate` and maps to `ResourceExhausted` with the
    diagnostic `tenant query rate limit exhausted for tenant <id>`.
  - ADR 053 bind-miss sync and ADR 054 descriptor-evolved retry run under
    one top-level admission and do not re-consume a token.
- Metrics and scope:
  - Rate rejections ride the existing bounded
    `tenant_admission_rejections_total` family with `resource="rate"`.
  - No parser/listener, worker-transport, SQL wire, Wendelon server,
    `crates/nilket`, or `toron_fixture_worker.rs` drift.
