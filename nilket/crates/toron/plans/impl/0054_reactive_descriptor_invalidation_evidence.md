# ADR 054 Reactive Descriptor Invalidation Evidence

Date: 2026-05-20 America/Chicago; full-suite rerun timestamps are
2026-05-21 UTC.

All linked commands below were run with a temporary PATH-local clang
wrapper that forwards to `/usr/bin/clang` and appends
`-Wl,--threads=6`, plus `CARGO_BUILD_JOBS=1`, to avoid the uncapped
`ld.mold` parallelism that was locking up the workstation.

## Passing Gates

- `cargo check -p toron --tests`
  - Result: passed.
- `cargo test -p toron --lib descriptor_evolved -- --test-threads=1`
  - Result: 6 passed.
  - Covered: local SELECT retry, second-evolution fail-closed,
    replacement-object fail-closed, dropped-object table-not-found,
    write invalidation without retry, distributed task failure carrying
    the stale `TableId`.
- `cargo test -p toron --lib refresh_sync -- --test-threads=1`
  - Result: 1 passed.
  - Covered: post-invalidation bind-miss surfacing
    `ResourceExhausted`.
- `cargo test -p toron --lib invalidate_descriptor -- --test-threads=1`
  - Result: 2 passed.
  - Covered: registry invalidation removes by-id/payload/aliases,
    tenant scoping, idempotence.
- `cargo test -p toron --lib resolve_table_throttled_is_resource_exhausted -- --test-threads=1`
  - Result: 1 passed.
  - Covered: ADR 053 descriptor-sync budget exhaustion remains
    `ResourceExhausted`.
- `cargo test -p toron --lib concurrent_same_table_bind_misses_share_one_sync_reservation -- --test-threads=1`
  - Result: 1 passed.
  - Covered: ADR 053 single-flight waiters share the owner sync
    reservation.
- `cargo test -p toron --test insert_test -- --test-threads=1`
  - Result: 25 passed.
- `cargo test -p toron --lib -- --test-threads=1` with the local TCP
  bind fixtures skipped
  - Result: 239 passed, 26 filtered out.
  - Covered: all lib tests that do not require binding local sockets in
    the current sandbox.
- `cargo test -p toron --test observability_listener_test catalog_descriptor_refresh_metrics_are_aggregate_outcome_only -- --test-threads=1`
  - Result: 1 passed.
- `cargo test -p toron --test architecture_test adr054 -- --test-threads=1`
  - Result: 19 passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1`
  - Result: 498 passed.
  - Rechecked after retargeting the prior ADR 053 descriptor-evolved
    guard to the narrower no-direct-sync invariant.
- `cargo clippy -p toron --all-targets -- -D warnings`
  - Result: passed.
- `cargo test -p toron -- --test-threads=6`
  - Result at 2026-05-21T02:46Z (2026-05-20 America/Chicago): passed.
  - Covered: the full Toron test suite, including the previously blocked
    SQL/TLS local listener fixtures.
- `git diff --check`
  - Result: passed.

## Prior Environment-Blocked Gate

- `cargo test -p toron -- --test-threads=1`
  - Result in the current Codex sandbox: failed in local TCP listener
    fixtures with `Os { code: 1, kind: PermissionDenied, message:
    "Operation not permitted" }`.
  - Observed summary before failure: 244 passed, 21 failed, 0 ignored.
  - Failed tests were the listener/TCP fixtures that bind local sockets
    (inspect CLI ready listeners, SQL mTLS listener, worker/shuffle TCP
    boundary tests). ADR 054 unit, catalog, coordinator, storage, server,
    insert, metrics, and architecture coverage passed before those
    environment failures.
  - Setting `CODEX_NETWORK_ALLOW_LOCAL_BINDING=1` for a single failing
    fixture did not change the sandbox bind denial.
- Recheck on 2026-05-21:
  - `cargo test -p toron --lib cli::tests::inspect_exit_zero_on_ready_one_on_degraded -- --test-threads=1`
    still failed with the same `Operation not permitted` listener bind
    error.
  - `cargo check -p toron --tests`, `cargo clippy -p toron --all-targets
    -- -D warnings`, and `git diff --check` still pass on the current
    tree under the capped linker wrapper.

This earlier blocker is superseded by the 2026-05-21 full-suite rerun
with `--test-threads=6`, which completed successfully in this worktree.
