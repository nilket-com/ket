# Wendelon Execution Workstream C — Reachable Data Plane And Client Surface

Author: Codex
Source roadmap: `plans/gap/codex_execution_20260415.md`

## Purpose

This document decomposes Workstream C into implementation-ready packets.

Workstream C exists to expose the already-substantial data path on the wire and
make the client capable of using it without in-process shortcuts.

## Workstream Objective

- make read, write, bulk, finalization, and client behavior align with the
  externally reachable protocol

## Entry Criteria

- Workstream A is materially complete
- Workstream B has at least the status/control skeleton in place

## Exit Criteria

- `WRITE_REQ`, `READ_REQ`, and `BULK_WRITE_REQ` are reachable over HRFT
- `stream_finalization` is real, not a stub
- the client can use the wire-reachable data ring with explicit retry and topology behavior

## Ordering

1. C.1 HRFT ingress for `WRITE_REQ` / `READ_REQ`
2. C.2 bulk ingress and leader-side HLC merge
3. C.3 `stream_finalization`
4. C.4 client write/read/bulk APIs
5. C.5 topology refresh and retry policy upgrade
6. C.6 streaming query/WPBC client handling

## Work Packets

### C.1 HRFT ingress for `WRITE_REQ` / `READ_REQ`

Objective:
- route client-visible write and read requests through `handle_data_frame`

Size:
- `M`

Exact files/modules expected to change:
- `src/network/data_server.rs`
- `src/network/wire.rs`
- `src/schema/core.rs`
- `tests/data_ring_wire.rs`

Explicit invariants that must become true:
- write and read operations are no longer reachable only through in-process trait calls
- protocol error codes remain faithful to the schema contracts
- the server can decode request meta and Arrow payloads from the HRFT wire path

Explicit non-goals:
- bulk write streaming
- push topology subscription
- query streaming

Dependencies:
- Workstream A complete enough to trust durability semantics

Tests that must pass or be added:
- add `tests/data_ring_wire.rs`
- end-to-end write then read over HRFT
- error code coverage for stale epoch, not leader, and `E_SAFE_BEHIND`

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M3 base data-ring reachability
- gap: `handle_data_frame` currently routes only replicate, query, and migrate

### C.2 `BULK_WRITE_REQ` ingress and leader-side HLC merge

Objective:
- replace the bulk-write stub with a real wire path and enforce leader-side HLC merge semantics

Size:
- `M`

Exact files/modules expected to change:
- `src/network/data_server.rs`
- `src/schema/core.rs`
- `src/network/wire.rs`
- `tests/bulk_write.rs`

Explicit invariants that must become true:
- bulk writes do not require buffering the full payload in memory
- leader-side HLC merge is applied consistently to write paths that need it
- bulk writes honor the same correctness contract as the single-write path

Explicit non-goals:
- DataFusion integration
- server-side spill control
- RDMA transport work

Dependencies:
- C.1
- A.1

Tests that must pass or be added:
- add `tests/bulk_write.rs`
- large bulk-write path
- HLC merge behavior coverage

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M3 bulk ingress
- gap: `bulk_write` is currently a stub and leader-side HLC merge is not enforced

### C.3 `stream_finalization` implementation

Objective:
- replace the current stream-finalization stub with a real finalization feed

Size:
- `M`

Exact files/modules expected to change:
- `src/network/data_server.rs`
- `src/consensus/raft_storage.rs`
- `src/schema/core.rs`
- `tests/stream_finalization.rs`

Explicit invariants that must become true:
- subscribers receive finalization markers after the requested floor
- back-pressure from a slow client does not stall the state machine
- reconnect semantics remain well-defined

Explicit non-goals:
- client query streaming
- push topology subscription
- backup/export work

Dependencies:
- C.1

Tests that must pass or be added:
- add `tests/stream_finalization.rs`
- ordered delivery under sustained write load
- slow-consumer behavior

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M3 stream-finalization path
- gap: `stream_finalization` currently returns an empty channel

### C.4 Client write/read/bulk APIs

Objective:
- give the client first-class APIs for the wire-reachable data ring

Size:
- `M`

Exact files/modules expected to change:
- `src/client/core.rs`
- `src/client/topology.rs`
- `tests/client_data_api.rs`

Explicit invariants that must become true:
- the client no longer assumes query-only access to the system
- write, read, and bulk paths use the same externally reachable protocol the server now exposes
- redirect and status handling remain explicit at the client boundary

Explicit non-goals:
- DataFusion adapter
- PGWire adapter
- topology push stream

Dependencies:
- C.1
- C.2

Tests that must pass or be added:
- add `tests/client_data_api.rs`
- write/read/bulk API smoke coverage

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M4 client data-ring surface
- gap: client currently lacks comparable write/read/bulk APIs

### C.5 Topology refresh and retry policy upgrade

Objective:
- move the client beyond pull-only, one-shot redirect behavior where the chosen roadmap requires it

Size:
- `M`

Exact files/modules expected to change:
- `src/client/core.rs`
- `src/client/topology.rs`
- `tests/client_retry_topology.rs`

Explicit invariants that must become true:
- retry policy is stronger than a one-shot redirect on `NOT_LEADER` and `E_SAFE_BEHIND`
- topology refresh behavior is explicit and testable
- client behavior remains compatible with the control-ring status truth from Workstream B

Explicit non-goals:
- mandatory push subscription if the team keeps pull-plus-refresh as the accepted model
- query streaming
- migration sweeper logic

Dependencies:
- C.4
- B.1

Tests that must pass or be added:
- add `tests/client_retry_topology.rs`
- redirect retry coverage
- stale-topology refresh behavior

What mini-plan, if any, governs the change:
- none

What assessment gap or milestone item it closes:
- M4 retry and topology behavior

### C.6 Streaming query / WPBC client handling

Objective:
- align client-side query handling with the planned streaming response shape for WPBC execution

Size:
- `L`

Exact files/modules expected to change:
- `src/client/core.rs`
- `src/network/data_server.rs`
- `src/schema/wpbc.rs`
- `src/schema/core.rs`
- `tests/streaming_query_client.rs`

Explicit invariants that must become true:
- query consumers are not limited to a single response frame assumption
- response handling remains compatible with server-side WPBC execution
- large result sets have a path that does not depend on one-frame concatenation

Explicit non-goals:
- DataFusion integration
- PGWire
- full spill-to-disk policy

Dependencies:
- C.4
- C.5

Tests that must pass or be added:
- add `tests/streaming_query_client.rs`
- streaming response assembly coverage

What mini-plan, if any, governs the change:
- create a short plan first if the team wants to bundle this with server-side spill and query framing changes

What assessment gap or milestone item it closes:
- M4 streaming query surface
- gap: client and server still lean on single-frame query assumptions
