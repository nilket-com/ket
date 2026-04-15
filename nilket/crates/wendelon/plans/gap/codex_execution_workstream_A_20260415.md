# Wendelon Execution Workstream A — Correctness Foundation

Author: Codex
Source roadmap: `plans/gap/codex_execution_20260415.md`

## Purpose

This document decomposes Workstream A into implementation-ready packets.

Workstream A exists to make the strongest published guarantees true before the
project expands more public surface area.

## Workstream Objective

- make durability, recovery, and transport identity guarantees match the plans

## Entry Criteria

- `plans/020_durability_boundary.md` exists and is accepted
- `plans/021_wal_recovery.md` exists and is accepted
- `plans/023_san_nodeid_binding.md` exists and is accepted
- the WAL hash disposition from M0 is decided

## Exit Criteria

- quorum acks mean durable acks in code, not just in documentation
- restart uses real recovery instead of the current in-memory bootstrap path
- peer identity is bound to verified certificate identity rather than a placeholder

## Ordering

1. A.1 durable quorum acknowledgements
2. A.2 live WAL recovery and bootstrap
3. A.3 WAL integrity disposition follow-through
4. A.4 SAN -> NodeId enforcement

A.4 can run in parallel with A.2 once its mini-plan is settled.

## Work Packets

### A.1 Durable quorum acknowledgements

Objective:
- make the replication path count only durable acknowledgements toward quorum

Size:
- `M`

Exact files/modules expected to change:
- `src/network/data_server.rs`
- `src/storage/core.rs`
- `src/storage/io_uring.rs`
- `src/schema/core.rs`
- `tests/replication_durability.rs`

Explicit invariants that must become true:
- no `QUORUM`-level acknowledgement is returned before the required durable flush
- leader quorum counting only advances on acknowledgements that explicitly mean durable
- if a weaker non-durable acknowledgement is retained, it cannot be mistaken for quorum durability

Explicit non-goals:
- transport-provider optimization
- batching or throughput tuning beyond what is required to preserve correctness
- RDMA work

Dependencies:
- `plans/020_durability_boundary.md`

Tests that must pass or be added:
- add `tests/replication_durability.rs`
- cover crash between append and flush on enough replicas to matter
- verify that quorum-acked writes survive restart and weaker-acked writes do not claim that guarantee

What mini-plan, if any, governs the change:
- `plans/020_durability_boundary.md`

What assessment gap or milestone item it closes:
- M1 durability semantics
- gap: durable quorum semantics are weaker in code than in plans

### A.2 Live WAL recovery and bootstrap

Objective:
- make node startup recover durable state from WAL instead of booting from an in-memory log store

Size:
- `M`

Exact files/modules expected to change:
- `src/consensus/raft_storage.rs`
- `src/node.rs`
- `src/consensus/core.rs`
- `tests/raft_recovery.rs`

Explicit invariants that must become true:
- `open()` reconstructs committed state after restart
- torn-tail handling does not leave the state machine inconsistent
- the live bootstrap path prefers durable recovery when on-disk state exists

Explicit non-goals:
- snapshot compression work
- rolling-upgrade compatibility
- backup/export flows

Dependencies:
- `plans/021_wal_recovery.md`
- A.1 strongly preferred first

Tests that must pass or be added:
- add `tests/raft_recovery.rs`
- restart after committed writes
- recovery with torn tail
- recovery with checkpoint present

What mini-plan, if any, governs the change:
- `plans/021_wal_recovery.md`

What assessment gap or milestone item it closes:
- M1 recovery semantics
- gap: bootstrap currently uses `in_memory()` and WAL replay is incomplete

### A.3 WAL integrity disposition follow-through

Objective:
- make WAL integrity rules consistent between plan and code

Size:
- `S`

Exact files/modules expected to change:
- `src/consensus/raft_storage.rs`
- `src/storage/io_uring.rs`
- `plans/consensus/raft-core.md`

Explicit invariants that must become true:
- one integrity scheme is authoritative across the relevant code and plan docs
- torn-write or bit-flip detection remains covered by tests

Explicit non-goals:
- redesign of the entire storage format
- unrelated checksum changes in other subsystems

Dependencies:
- M0 drift disposition
- A.2 if a code-path change is required

Tests that must pass or be added:
- add or extend a torn-write detection test
- ensure recovery tests still pass after the chosen integrity path is applied

What mini-plan, if any, governs the change:
- none if the M0 disposition is already explicit

What assessment gap or milestone item it closes:
- M0 drift reconciliation for blake3 vs CRC32c

### A.4 SAN -> NodeId enforcement

Objective:
- bind accepted peer identity to verified certificate identity instead of a placeholder `NodeId`

Size:
- `S`

Exact files/modules expected to change:
- `src/network/tls.rs`
- `src/network/server.rs`
- `src/network/quic_transport.rs`
- `tests/tls_identity.rs`

Explicit invariants that must become true:
- a valid chain is not enough if the peer SAN does not map to the expected `NodeId`
- accepted server-side connections carry a verified peer identity
- downstream code no longer has to treat `NodeId { id: 0 }` as normal behavior

Explicit non-goals:
- certificate hot reload
- full PKI lifecycle management
- transport metrics work

Dependencies:
- `plans/023_san_nodeid_binding.md`

Tests that must pass or be added:
- add `tests/tls_identity.rs`
- cover happy path, SAN mismatch, and missing SAN

What mini-plan, if any, governs the change:
- `plans/023_san_nodeid_binding.md`

What assessment gap or milestone item it closes:
- M1 transport identity correctness
- gap: SAN -> `NodeId` verification is not enforced today
