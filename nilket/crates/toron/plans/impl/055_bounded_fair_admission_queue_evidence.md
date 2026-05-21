# ADR 055 Bounded Fair Admission Queue Evidence

Date: 2026-05-21 America/Chicago.

## Passing Gates

- `cargo fmt -p toron`
  - Result: passed.
- `cargo check -p toron --tests`
  - Result: passed.
- `cargo clippy -p toron --all-targets -- -D warnings`
  - Result: passed.
- `cargo test -p toron distributed::budget::tests:: -- --test-threads=1`
  - Result: 25 passed.
  - Covered: omitted-config hard reject, queue-disabled/full/timeout,
    cancellation cleanup, release pumping, same-tenant FIFO no-bypass,
    integer weighted-deficit ordering, shared-pool caps, per-tenant borrow
    caps, borrowed-reservation release preserving owned-memory accounting,
    drained unconfigured-tenant deficit pruning, no cross-tenant
    owned-budget steal, and no borrowed-work preemption.
- `cargo test -p toron coordinator::tests:: -- --test-threads=1`
  - Result: 16 passed.
  - Covered: ADR 050/052 admission compatibility, post-grant bind-miss
    `ResourceExhausted` release, and ADR 054 descriptor-evolved retry
    staying inside one admission reservation.
- `cargo test -p toron --test config_test -- --test-threads=1`
  - Result: 30 passed.
  - Covered: `[admission]` parsing, per-tenant admission fields, disabled
    defaults, and zero `admission_weight` rejection.
- `cargo test -p toron --test observability_listener_test queue_and_borrow_metrics_are_configured_plus_other_no_table_labels -- --test-threads=1`
  - Result: 1 passed.
  - Covered: queue/borrow metrics use configured tenant labels plus
    `"other"` only, with no table/query/fingerprint labels.
- `cargo test -p toron --test architecture_test adr055 -- --test-threads=1`
  - Result: 20 passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1`
  - Result: 518 passed.
  - Rechecked after pinning the ADR 055 descriptor-interaction fixtures and
    keeping the test-only scripted transport clear of the legacy
    duplex-only source marker.
- `cargo test -p toron -- --test-threads=6`
  - Result: passed.
  - Covered: full Toron test suite, including the local TCP/TLS listener
    fixtures. Expected negative-path TLS/OpenRaft shutdown diagnostics were
    emitted while the corresponding tests passed.
- `git diff --check`
  - Result: passed.

## Requirement Evidence

- Config surface:
  - `AdmissionPolicy` plus per-tenant `admission_weight`,
    `max_queued_queries`, `max_borrowed_memory_bytes`, and
    `max_borrowed_concurrent_queries` are parsed and defaulted.
  - Omitted `[admission]` preserves ADR 050/052 hard rejection.
- Controller:
  - `TenantAdmissionController` owns one synchronous state lock over owned
    usage, borrowed usage, shared-pool counters, queues, deficits, and
    bounded metric counters.
  - `QueuedAdmission` and `AdmissionReservation` are RAII handles; timeout,
    cancellation, and reservation drop all pump waiters.
  - Grants are one controller decision over owned memory/concurrency and
    shared borrowed memory/concurrency.
- Coordinator:
  - `admit_query` runs before bind and awaits queued grants outside the
    controller lock.
  - SELECT descriptor-evolved retry remains under one admission reservation;
    writes/deletes/CTAS keep the ADR 054 no-retry behavior.
- Metrics:
  - Queue depth, borrowed memory, borrowed concurrency, queue timeouts, and
    queue-full rejections render with bounded configured-plus-`"other"`
    tenant labels only.
- Scope guards:
  - No SQL wire or Wendelon server change.
  - No rate-limit, token-bucket, connection-cap, cluster-wide semaphore, or
    preemption surface.
  - `crates/nilket` remains runtime-free; `toron_fixture_worker.rs`
    remains outside the ADR 055 surface.
