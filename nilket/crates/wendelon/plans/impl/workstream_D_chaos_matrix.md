# Workstream D Chaos Matrix

Date: 2026-04-22
Author Role: Lead Architect
Workstream: `D3`
Roadmap Source: `plans/impl/workstream_D.md`

## Purpose

This is the canonical fault matrix for `D3`.

It exists to answer one question honestly: which hostile combinations are actually covered by executable evidence, and which ones are still only described in prose.

## Coverage Rules

- `live`: covered by an executable test or chaos scenario now
- `partial`: some evidence exists, but not yet as the full mixed fault the workstream wants
- `gap`: no credible automated evidence yet

## Matrix

| Scenario | Status | Harness / Source | Current Evidence | Notes |
|---|---|---|---|---|
| Leader partition and rejoin | `live` | `tests/chaos.rs` | `leader_partition_and_rejoin` | Existing quorum/failover smoke path |
| Partitioned follower restart and rejoin | `live` | `tests/chaos.rs` | `follower_partition_restart_and_rejoin` | New D3 scenario; proves the leader-majority survives a partitioned follower restart and heal |
| Follower loss during active write load | `live` | `tests/chaos.rs` | `follower_loss_under_control_write_load` | New D3 scenario; repeated writes while one follower is absent, then rejoin |
| Crash between durable quorum and finalization | `partial` | `src/network/data_server.rs` targeted regressions | `pending_finalization_replays_after_restart_when_control_branch_failed`, `duplicate_write_replays_persisted_pending_intent_after_cache_loss` | Real executable recovery evidence exists, but it is still component-scoped rather than a multi-node chaos scenario |
| Overloaded listener plus retry traffic | `live` | `tests/quic_transport.rs` | `listener_overload_sheds_excess_handshakes_while_retry_redirect_succeeds` | Real QUIC listener overload and smart-client retry now coexist in one executable scenario |
| Audit sink distress where local events are fail-closed | `live` | `tests/observability_listeners.rs` | `observability_metrics_fail_closed_when_local_audit_sink_rejects` | New D3 fail-closed operator-surface regression |
| Leader partition plus restart plus rejoin | `live` | `tests/chaos.rs` | `leader_partition_restart_and_rejoin` | Former leader crash/restart now recovers from durable committed progress instead of replaying an uncommitted WAL tail into the state machine |

## What This Means

`D3` is no longer empty. The cluster chaos catalog now covers:

- partition plus restart
- isolated leader partition plus restart plus rejoin
- follower loss under continuing write traffic
- existing failover and packet-drop smoke cases

## Reproduction

Cluster chaos scenarios replay through:

```bash
WENDELON_CHAOS_SCENARIO=<scenario> WENDELON_CHAOS_SEED=<seed> cargo test --test chaos chaos_smoke_catalog -- --nocapture
```

The supported scenario list is maintained in:

- `tests/chaos.rs`
- `tests/chaos/seeds/README.md`

The mixed listener-overload integration evidence lives in:

- `tests/quic_transport.rs`

## Exit Bar For D3

I would call `D3` complete only when:

1. every row above is at least `live` or explicitly deferred by design
2. at least one frozen regression seed exists for each multi-node chaos scenario class that has failed before
3. the matrix is the thing we update when a new hostile case is discovered, not a dead planning appendix
