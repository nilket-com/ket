# Workstream A — Runtime Foundation and Integration Gate

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phases 0 + 1)
**Status:** First pass
**Subsystems:** `src/main.rs`, `src/node.rs`, `src/config.rs`, `src/network/server.rs`, `src/network/control_server.rs`, `src/network/data_server.rs`

---

## Problem Statement

The shipped daemon (`main.rs`) cannot form a real distributed cluster. It
does not construct a `quinn::Endpoint`, does not call `serve_quic`, does
not load TLS material from config, rejects `cluster.mode = "join"`, and
hardcodes `l2: None`. The tracing subscriber is never installed, so all
`tracing::*!` macros are no-ops. Additionally, several critical
correctness bugs (JOIN identity binding, follower disk leak, unbounded
caches) affect the existing library code.

This workstream transforms the binary from a single-node dev harness into
a real cluster process and fixes the highest-impact bugs, then gates all
future work behind a real-network integration test.

**This workstream is the prerequisite to all others.** No subsequent
workstream can be verified end-to-end until this one lands.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Config loader | `src/config.rs` | TLS fields, L2 fields, dev-key warning |
| Daemon entry | `src/main.rs` | QUIC endpoint, tracing subscriber |
| Node bootstrap | `src/node.rs` | Join-mode support, TierObserver wiring |
| Control server | `src/network/control_server.rs` | JOIN identity binding fix |
| Data server | `src/network/data_server.rs` | Idempotency cache TTL |
| QUIC server | `src/network/server.rs` | Per-peer stream concurrency bound |
| Integration tests | `tests/integration_smoke.rs` (new) | Real-network harness |
| CI | CI config (new or updated) | Per-PR gate |

---

## Ordered Implementation Steps

### A.1 — Config surface (no runtime dependencies)

| # | Step | Target | Details |
|---|------|--------|---------|
| A.1.1 | Add TLS material fields to config TOML schema | `src/config.rs` | `cert_path`, `key_path`, `ca_path` parsed into `TlsMaterial::load` inputs |
| A.1.2 | Remove `l2: None` hardcode (line ~231) | `src/config.rs` | Parse `[l2]` section from TOML into `ObjectStoreConfig` |
| A.1.3 | Add dev-key warning to `render_example_config` | `src/config.rs` | Emit comment: "WARNING: these are deterministic dev keys — replace before production" |

### A.2 — Daemon runtime wiring (depends on A.1.1)

| # | Step | Target | Details |
|---|------|--------|---------|
| A.2.1 | Install tracing subscriber | `src/main.rs` | `tracing_subscriber::fmt::init()` or equivalent; respect `RUST_LOG` |
| A.2.2 | Construct `quinn::Endpoint` from loaded TLS material | `src/main.rs`, `src/node.rs` | Build `ServerConfig` from `TlsMaterial`, bind to configured address |
| A.2.3 | Call `serve_quic` from `main.rs` | `src/main.rs` | Wire the endpoint into the existing `WendelonNode::serve_quic` path |
| A.2.4 | Support `cluster.mode = "join"` | `src/node.rs` | Remove `ErrorKind::Unsupported` gate at `node.rs:379`; construct `QuicTransportProvider` and call `bootstrap_with_provider` |

### A.3 — Critical correctness fixes (no ordering dependency on A.2)

| # | Step | Target | Details |
|---|------|--------|---------|
| A.3.1 | Fix JOIN_REQ identity binding | `src/network/control_server.rs:937` | Assert `req.node_id == remote` in `accept_join`; reject mismatch with `ErrorKind::PermissionDenied` |
| A.3.2 | Wire `ManagerTierObserver` into bootstrap | `src/node.rs` | Call `with_tier_observer` on log store builder during `bootstrap_with_provider` |
| A.3.3 | Add idempotency cache TTL sweep | `src/network/data_server.rs` | Spawn periodic task; evict entries older than `idempotency_ttl_ms`; respect `DashMap` shard locking |
| A.3.4 | Bound per-peer QUIC stream concurrency | `src/network/server.rs:423` | Add semaphore or `Arc<AtomicUsize>` per connection; reject/backpressure when limit reached |

### A.4 — Integration test harness (depends on A.2)

| # | Step | Target | Details |
|---|------|--------|---------|
| A.4.1 | Test CA + per-node cert generation helper | `tests/integration_smoke.rs` | Generate ephemeral CA + 3 node certs with correct SANs at test start |
| A.4.2 | 3-node cluster formation test | `tests/integration_smoke.rs` | Node 1 bootstraps; nodes 2+3 join via QUIC; assert all 3 in membership |
| A.4.3 | Write + read round-trip test | `tests/integration_smoke.rs` | Write to leader, read from each node, assert data matches |
| A.4.4 | Node restart test | `tests/integration_smoke.rs` | Kill node 2, restart, re-join, verify data still readable |
| A.4.5 | Malicious-join negative test | `tests/integration_smoke.rs` | Node with valid CA cert but mismatched `node_id` in JOIN payload — must be rejected |
| A.4.6 | CI job wiring | CI config | Run `cargo test --test integration_smoke` on every PR |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| 3-node cluster formation | Integration | Config -> TLS -> QUIC -> join works end-to-end |
| Write + read round-trip | Integration | Data path through real transport is functional |
| Node restart | Integration | Recovery path works with real QUIC reconnection |
| Malicious JOIN rejection | Integration (negative) | Identity binding fix (A.3.1) is enforced |
| Idempotency cache eviction | Unit | TTL sweep removes stale entries |
| Stream concurrency bound | Unit | Excess streams are rejected, not spawned |
| TierObserver wiring | Integration | Follower disk usage stabilizes after tiering (no leak) |
| L2 config parsing | Unit | `[l2]` TOML section round-trips correctly |

---

## Rollout and Migration Concerns

- **Config format change.** Adding TLS and L2 fields to the TOML schema
  is additive; existing configs without those sections continue to work
  (single-node, no TLS, no L2). Document the new fields in the example
  config.
- **`bootstrap` behavior change.** Removing the `Unsupported` gate for
  join mode changes the public API of `WendelonNode::bootstrap`. Any
  existing callers that relied on the error need updating (likely only
  tests).
- **Idempotency cache TTL.** Clients that rely on very long idempotency
  windows need the TTL to be configurable and documented.

---

## Failure Modes and Rollback

- **TLS misconfiguration.** If cert/key paths are wrong, the daemon
  should fail fast at startup with a clear error, not silently fall back
  to unencrypted. Test this explicitly.
- **JOIN identity mismatch.** The new assertion may break existing test
  setups that use mismatched node IDs. Audit all test helpers that
  construct `JoinClusterRequestWire`.
- **Stream concurrency bound too low.** A misconfigured cap could reject
  legitimate traffic. Make the limit configurable with a sensible default
  (e.g., 256 concurrent streams per peer). Log when the limit is hit.

---

## Exit Criteria

- `cargo test --test integration_smoke` passes: 3 real QUIC nodes,
  mTLS, join, write, read, restart, malicious-join rejection.
- `main.rs` boots a real networked node from TOML config alone.
- Tracing output is visible in daemon stderr.
- CI runs the integration test on every PR.

---

## Open Questions

1. **Test topology:** Should the integration harness run 3 separate OS
   processes (closer to production) or 3 in-process nodes with real
   `quinn::Endpoint` (faster, easier to debug)? The in-process approach
   is likely sufficient for CI speed, but a separate-process mode would
   catch more wiring issues (signal handling, config file loading, stderr
   output).
2. **TLS cert generation in tests:** Use `rcgen` at test time, or
   pre-generate and check in fixture certs? `rcgen` is more flexible but
   adds a dev dependency.
3. **Default stream concurrency limit:** What's a reasonable default?
   Need to understand the expected stream-per-write fan-out pattern
   before picking a number.
4. **Tracing subscriber choice:** Bare `fmt` subscriber, or wire
   `tracing-subscriber` with env filter + optional JSON output from the
   start? The latter is more work but avoids a later migration.
