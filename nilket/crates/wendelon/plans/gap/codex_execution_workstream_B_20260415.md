# Wendelon Execution Workstream B — Reachable Control Plane

Author: Codex
Source roadmap: `plans/gap/codex_execution_20260415.md`

## Purpose

This document decomposes Workstream B into implementation-ready packets.

Workstream B exists to make control-plane behavior externally reachable instead
of effectively in-process only.

## Workstream Objective

- stand up a real control-ring server surface for status, lease, leadership,
  epoch, and membership actions

## Entry Criteria

- Workstream A is materially complete, especially A.2 and A.4
- `plans/022_control_ring_server.md` exists and is accepted

## Exit Criteria

- control-plane RPCs are served over a real external path
- topology/status retrieval no longer speaks into an unimplemented server void
- membership-affecting actions commit through real control-plane flows

## Ordering

1. B.1 control-ring server skeleton and status path
2. B.2 lease acquisition and cluster status reachability
3. B.3 leadership transfer and epoch advance reachability
4. B.4 membership advancement through committed control actions

## Work Packets

### B.1 Control-ring server skeleton and `STATUS_REQ` / `STATUS_RESP`

Objective:
- create the server-side control-ring dispatch path and make topology/status queries work end to end

Size:
- `L`

Exact files/modules expected to change:
- `src/network/traits.rs`
- `src/node.rs`
- `src/network/router.rs`
- one new control-ring server module under `src/network/`
- `src/client/topology.rs`
- `tests/control_ring_status.rs`

Explicit invariants that must become true:
- control-plane frames are routed independently of the current data-ring-only dispatch
- `ControlRingTopologySource` succeeds against a live server
- status requests and responses use a stable wire path rather than test-only or in-process shortcuts

Explicit non-goals:
- push topology subscription
- lease signature enforcement
- migration automation

Dependencies:
- `plans/022_control_ring_server.md`
- A.2

Tests that must pass or be added:
- add `tests/control_ring_status.rs`
- verify `STATUS_REQ` / `STATUS_RESP` end to end

What mini-plan, if any, governs the change:
- `plans/022_control_ring_server.md`

What assessment gap or milestone item it closes:
- M2 control-ring server reachability
- gap: client currently expects status RPCs but no server-side listener exists

### B.2 Lease acquisition and cluster status RPC reachability

Objective:
- expose lease acquisition and rich cluster status through the real control-ring surface

Size:
- `M`

Exact files/modules expected to change:
- control-ring server module under `src/network/`
- `src/node.rs`
- `src/consensus/raft_storage.rs`
- `src/schema/core.rs`
- `tests/control_ring_rpc.rs`

Explicit invariants that must become true:
- remote callers can acquire leases through a committed control-plane path
- cluster status is externally retrievable with the expected topology/lease payload
- server-side control-plane handling does not bypass Raft for state-changing actions

Explicit non-goals:
- signed leases
- leadership transfer authentication hardening
- client retry policy work

Dependencies:
- B.1

Tests that must pass or be added:
- add or extend `tests/control_ring_rpc.rs`
- cover lease acquisition races
- cover cluster-status retrieval against a live node

What mini-plan, if any, governs the change:
- `plans/022_control_ring_server.md`

What assessment gap or milestone item it closes:
- M2 lease and status reachability

### B.3 Leadership transfer and epoch advance reachability

Objective:
- expose leadership transfer and epoch advancement over the control ring

Size:
- `M`

Exact files/modules expected to change:
- control-ring server module under `src/network/`
- `src/node.rs`
- `src/consensus/core.rs`
- `src/consensus/raft_storage.rs`
- `tests/control_ring_rpc.rs`

Explicit invariants that must become true:
- leadership transfer requests are externally callable and validated through the chosen control path
- epoch advancement is externally reachable through a real control-ring RPC
- status and epoch changes are observable after restart once Workstream A is complete

Explicit non-goals:
- automatic migration seeding itself
- rolling-upgrade compatibility
- multi-tenancy policy

Dependencies:
- B.1
- B.2 preferred first

Tests that must pass or be added:
- leadership-transfer RPC test
- epoch-advance RPC test

What mini-plan, if any, governs the change:
- `plans/022_control_ring_server.md`

What assessment gap or milestone item it closes:
- M2 leadership and epoch reachability

### B.4 Membership advancement via committed control actions

Objective:
- replace the current watch-only membership shortcut with a committed control-plane action path

Size:
- `M`

Exact files/modules expected to change:
- `src/node.rs`
- `src/consensus/monitor.rs`
- `src/consensus/raft_storage.rs`
- control-ring server module under `src/network/`
- `tests/membership_advancement.rs`

Explicit invariants that must become true:
- membership change decisions are not local-only mutations
- a partitioned minority cannot force an eviction outcome
- self-abort on local drift remains intact where intended

Explicit non-goals:
- topology push subscription
- Prometheus exporter work
- client-side retry logic

Dependencies:
- B.3
- A.2

Tests that must pass or be added:
- add `tests/membership_advancement.rs`
- cover partitioned minority behavior
- cover drift-triggered committed membership change

What mini-plan, if any, governs the change:
- `plans/022_control_ring_server.md`

What assessment gap or milestone item it closes:
- M2 committed membership action path
- gap: `WatchMembershipAdvancer` currently mutates local watch state directly
