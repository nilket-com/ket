# Wendelon Execution Workstream E — Bootstrap And Operations

Author: Codex
Source roadmap: `plans/gap/codex_execution_20260415.md`

## Purpose

This document decomposes Workstream E into implementation-ready packets.

Workstream E exists to move the system from a development bootstrap toward an
operable multi-node baseline with enough observability and lifecycle support to
exercise the architecture in real conditions.

## Workstream Objective

- establish configuration, cluster bootstrap, observability, lifecycle, and
  later operational hardening paths

## Entry Criteria

- Workstreams A through C are stable enough to run on real multi-node setups

## Exit Criteria

- the cluster can be brought up from configuration rather than the current dev-only path
- baseline observability and lifecycle hooks exist
- later operational hardening items have explicit plans rather than remaining undefined

## Ordering

1. E.1 config loader and node identity material
2. E.2 multi-node bootstrap and join
3. E.3 Prometheus/exported metrics baseline
4. E.4 graceful drain and lifecycle hooks
5. E.5 rolling-upgrade mini-plan and implementation
6. E.6 backup/restore mini-plan and implementation

E.3 and E.4 can run in parallel once E.1 is underway.

The E.1 identity-material slice may be pulled forward ahead of the rest of this
workstream if Workstream D needs signing keys before full M6 cluster bootstrap.

## Work Packets

### E.1 `wendelon.toml` config loader and identity material

Objective:
- replace the current argument-based bootstrap with a real config-driven node definition

Size:
- `M`

Exact files/modules expected to change:
- `src/main.rs`
- `src/node.rs`
- one configuration module under `src/`
- `tests/config_loader.rs`

Explicit invariants that must become true:
- node identity, addresses, and key material come from configuration
- startup is not limited to the current three-argument dev path
- configuration can support later lease-signing and TLS identity work

Explicit non-goals:
- rolling-upgrade compatibility
- backup/export
- multi-tenancy

Dependencies:
- A.2

Tests that must pass or be added:
- add `tests/config_loader.rs`
- config parsing and startup coverage

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M6 configuration baseline
- gap: `main.rs` is still a minimal dev bootstrap

### E.2 Multi-node bootstrap and join

Objective:
- let a cluster form and admit nodes from configuration rather than a single-node-oriented bootstrap

Size:
- `M`

Exact files/modules expected to change:
- `src/node.rs`
- configuration module under `src/`
- control-ring server module under `src/network/`
- `tests/multinode_bootstrap.rs`

Explicit invariants that must become true:
- a multi-node cluster can come up from config alone
- join behavior is explicit and testable
- bootstrap-from-peer or seed-list behavior is real rather than implied

Explicit non-goals:
- backup/restore
- rolling upgrades
- topology push subscription

Dependencies:
- E.1
- Workstream B control-plane reachability

Tests that must pass or be added:
- add `tests/multinode_bootstrap.rs`
- cluster formation coverage
- join and rejoin coverage

What mini-plan, if any, governs the change:
- none unless join semantics remain unsettled

What assessment gap or milestone item it closes:
- M6 multi-node bootstrap and join

### E.3 Prometheus/exported metrics baseline

Objective:
- provide enough exported metrics to observe the new correctness, control, data, and migration paths

Size:
- `S`

Exact files/modules expected to change:
- one metrics/exporter module under `src/`
- `src/main.rs`
- `src/consensus/monitor.rs`
- `src/network/data_server.rs`
- `src/storage/sweeper.rs`
- `tests/metrics_smoke.rs`

Explicit invariants that must become true:
- the runtime exposes a scrapeable metrics surface
- key correctness and migration counters can be observed
- metrics additions do not change control-plane or data-plane semantics

Explicit non-goals:
- dashboard authoring
- full SLO design
- backup/export metrics

Dependencies:
- none hard
- E.1 preferred for configuration-driven exporter wiring

Tests that must pass or be added:
- add `tests/metrics_smoke.rs`
- scrape endpoint coverage

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M6 observability baseline
- gap: no Prometheus exporter wiring exists today

### E.4 Graceful drain and lifecycle hooks

Objective:
- add explicit drain and shutdown behavior instead of relying on abrupt process termination

Size:
- `S`

Exact files/modules expected to change:
- `src/main.rs`
- `src/node.rs`
- `src/network/server.rs`
- `tests/graceful_shutdown.rs`

Explicit invariants that must become true:
- shutdown stops new work intake in an orderly way
- in-flight work is given a defined drain path
- lifecycle behavior is explicit rather than accidental

Explicit non-goals:
- rolling-upgrade protocol
- backup/restore
- multi-tenancy

Dependencies:
- E.1
- E.2 preferred for realistic cluster behavior

Tests that must pass or be added:
- add `tests/graceful_shutdown.rs`
- graceful stop coverage

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M6 lifecycle baseline

### E.5 Rolling-upgrade protocol

Objective:
- define and implement a version-negotiation and compatibility story for controlled upgrades

Size:
- `L`

Exact files/modules expected to change:
- `plans/ops/upgrades.md`
- `src/schema/core.rs`
- `src/network/wire.rs`
- `src/node.rs`
- upgrade compatibility tests under `tests/`

Explicit invariants that must become true:
- version skew rules are explicit
- incompatible peers do not fail ambiguously
- rollback behavior is defined for the supported window

Explicit non-goals:
- backup/restore
- multi-tenancy
- RDMA transport work

Dependencies:
- E.1
- E.2

Tests that must pass or be added:
- add an upgrade compatibility test once the protocol is settled

What mini-plan, if any, governs the change:
- create `plans/026_rolling_upgrade.md` first

What assessment gap or milestone item it closes:
- later operational hardening for upgrades

### E.6 Backup and restore baseline

Objective:
- define and implement a first credible backup/restore path for the cluster

Size:
- `L`

Exact files/modules expected to change:
- `plans/ops/backup_dr.md`
- `src/consensus/raft_storage.rs`
- `src/storage/manager.rs`
- restore/backup test coverage under `tests/`

Explicit invariants that must become true:
- backup contents and restore target are explicit
- restore produces a coherent cluster state rather than a partial node image
- operational recovery semantics are testable

Explicit non-goals:
- rolling-upgrade protocol
- multi-tenancy
- DataFusion or PGWire

Dependencies:
- E.2
- D.7 preferred if backups should include remote-tier assumptions

Tests that must pass or be added:
- add backup/restore integration coverage once the artifact format is settled

What mini-plan, if any, governs the change:
- create `plans/027_backup_dr.md` first

What assessment gap or milestone item it closes:
- later operational hardening for backup/DR
