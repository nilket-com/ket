# Wendelon Gap Assessment — 2026-04-15

Author: Codex
Scope: divergence between `plans/` and the current implementation under `src/`.
Method: full read-through of the `plans/` tree, followed by source review across
the consensus, network, client, schema, and storage modules. This was a read/write
planning pass only; no Rust code changes were made.
Revision note: updated after review of `plans/gap/claude_20260415.md` to fold in
additional concrete target-level deltas.

## Overall Assessment

The codebase is ahead of the plans on internal primitives and data-path mechanics.
Many of the architectural ideas in plans 012-019 are already present in code:
schema types, WPBC execution, tiering machinery, migration ledger handling, a
unified ring, lease management, and drift monitoring all exist in substantive form.

The main gap is different from "the plans are unimplemented." The stronger statement
is this:

- Internal mechanisms are mostly there.
- Public RPC surfaces are incomplete.
- Durability and recovery guarantees are weaker than the plans claim.
- Production bootstrap, security hardening, and ops workflows are still mostly at
  the plan stage.

That means `plans/` now reads more like target architecture than accurate delivery
tracking.

## Areas Already Implemented More Than The Plans Suggest

### Schema and protocol building blocks

The schema layer is already much richer than a first pass through the plans would
imply. `src/schema/core.rs` contains substantial support for:

- finalization markers
- lease descriptors and lease acquisition messages
- replica-write and query messages
- migration and slab-manifest types
- raft log and snapshot wire structures

This is not placeholder scaffolding. The system already has many of the fixed-shape
types the later plans depend on.

### WPBC execution

`src/schema/wpbc.rs` contains a real server-side WPBC interpreter. This moves plan
015 beyond design-only status. The engine can already evaluate server-side query
programs; the remaining gap is more about response streaming and operational limits
than about the existence of computation.

### Unified ring, migration, and leases

The implementation is materially ahead in the areas covered by plans 016, 018, and
019:

- `src/network/ring.rs` implements a unified virtual-node ring.
- `src/network/data_server.rs` contains real lease management and migration-aware
  request handling.
- `src/consensus/core.rs` and `src/consensus/raft_storage.rs` contain concrete
  migration-ledger machinery.

Lazy migration is not just discussed in planning docs; significant pieces are
already present.

### Tiering and MVCC-style reads

The storage layer already supports more than the plans might lead a reader to
expect:

- `src/storage/sweeper.rs` implements a slab sweeper for demotion/tiering.
- `src/storage/core.rs` and `src/storage/manager.rs` support time-qualified
  lookups and scans.
- query and read paths already carry `as_of_hlc` style semantics.

### Transport abstraction

The transport refactor has real shape in code. `src/network/transport.rs`,
`src/network/factory.rs`, and `src/network/router.rs` give the project a usable
abstraction boundary, even though the full transport matrix from the plans is not
yet implemented.

## Major Gaps Between Plans and Code

### 1. Control Ring RPCs remain largely unimplemented

This is the single clearest structural gap.

`src/network/traits.rs` still treats control-ring metadata as provisional, and I
did not find a concrete `ControlRingServer` implementation. `src/node.rs` wires
the message router around the data ring and raft internals, but not around a full
control-plane service.

This is especially visible because the client side already expects part of that
surface: `src/client/topology.rs` has a production `ControlRingTopologySource`
that issues `STATUS_REQ` and expects `STATUS_RESP`.

Impact:

- topology/status RPCs are only partial
- lease-control workflows are incomplete
- epoch and leadership orchestration expected by the plans is not fully exposed

The plans assume a stronger control-plane surface than the code currently exports.
In practice, most of this control-plane behavior is still effectively in-process
today rather than reachable through a real server-side control-ring RPC surface.

### 2. Data-ring wire ingress is narrower than the protocol docs

`src/network/data_server.rs` decodes and handles a subset of the wire operations,
not the full client-visible protocol described in the network plans. More
specifically, `handle_data_frame` only routes replication, query, and migration
requests.

What exists:

- replication traffic
- query traffic
- migration traffic

What is still missing or stubbed on the HRFT server path:

- full client-facing write ingress
- full client-facing read ingress
- bulk-write ingress
- a real stream-finalization path

The traits and internal methods exist, but the network-facing path is not yet
fully aligned with the protocol documents.

Two of these are explicit stubs rather than merely absent behavior:

- `stream_finalization` currently returns an empty `mpsc::channel(1)`
- `bulk_write` currently returns an empty `mpsc::channel(1)`

### 3. The smart client is only partially realized

`src/client/core.rs` and `src/client/topology.rs` already support topology refresh
and query routing, but the client layer is still short of the SDK plans.

Notable gaps:

- no equivalent high-level write API
- no equivalent high-level read API for the planned data path
- no bulk-write API
- no full scan fan-out across replicas/ranges

Additional gaps sharpened by Claude's review:

- topology refresh is still pull-only rather than subscription-based
- retry behavior is still one-shot on `NOT_LEADER` and `E_SAFE_BEHIND`
- there is no streaming result consumer to match the planned WPBC response model

The current client is better described as an early query-capable client than as
the complete SDK envisioned in plan 013.

### 4. Durable quorum semantics are not yet enforced

The write-path plans describe quorum acknowledgements with durable semantics before
finalization. The code appears to advance on append completion rather than on a
forced durability boundary.

Relevant contrast:

- `src/storage/core.rs` includes `fsync` support
- the write/replication path in `src/network/data_server.rs` does not appear to
  force `fsync` before quorum/finalization decisions

This is an important gap because it is not just a missing feature. It changes the
actual failure semantics of the write path relative to what the plans promise.

### 5. Raft persistence and recovery are only partially wired

The Raft storage code has progressed, but the actual node bootstrap path still
falls short of the persistence model in the consensus plans.

Observed issues:

- `src/node.rs` still bootstraps through an in-memory log store path
- `src/consensus/raft_storage.rs` still treats WAL replay/recovery as incomplete

The result is that the code contains pieces of the durable design, but the running
node path does not yet satisfy the recovery story the plans describe.

There is also a plan/code format drift here: the code uses blake3-derived record
integrity in places where the consensus plan still specifies CRC32c.

### 6. Lazy migration is real, but not yet fully automated

This was one of the most useful corrections from Claude's document. My first pass
correctly noted that lazy migration is substantially implemented, but understated
the remaining automation gaps.

What already exists:

- migration-ledger state
- point-read and query fallback into migration pull paths
- idempotent migration ingest

What still appears missing:

- no epoch-transition hook that automatically seeds migration ledger entries when
  ownership moves from `ring(E)` to `ring(E+1)`
- no background migration sweeper driving peer migration RPCs to completion
- no tombstone-aware compaction or physical reclaim after migration completion
- `MIGRATION_INCOMPLETE` is surfaced, but the client does not appear to treat it
  as a first-class retry/backoff case

This means plan 019 is much further along than "unimplemented," but it is not yet
closed operationally.

### 7. Transport support is incomplete despite good abstractions

The transport architecture is reasonably well factored, but only a subset of the
planned providers appear to be real.

Implemented:

- QUIC transport

Missing or not substantively implemented:

- real TCP/TLS transport provider
- real RDMA provider

This leaves the transport refactor as "architecturally prepared, operationally
partial."

### 8. Security and lease authenticity are still V1-grade

The networking layer does not yet fully bind peer identity to transport-level
credentials in the way the security plans assume, and the lease/transfer path is
similarly incomplete cryptographically.

Examples:

- `src/network/server.rs` still uses placeholder peer identity behavior
- `src/network/tls.rs` still leaves SAN-to-`NodeId` verification incomplete
- `LeaseDescriptor.signature` is still zero-filled in the lease refresh path
- `TransferToken` signing/verification is defined at the schema level but not
  fully enforced in the receive path

That means mTLS and signed lease-transfer semantics exist as a direction, not yet
as fully enforced trust boundaries.

### 9. Ring/hash configuration is still static where the plans expect epoch-derived state

The hash-ring code currently uses fixed keys/constants where the plans imply that
epoch metadata should drive those parameters.

This matters because the implementation is not yet sourcing ring identity from the
same control-plane state the plans describe. It works as a V1 simplification, but
it is still a divergence from the intended architecture.

Claude also surfaced a concrete parameter drift worth reconciling: the code uses
`VNODES_PER_NODE = 64`, while the network topology plan still describes `256`.

### 10. Tiering backends and read-path behavior are still limited

The tiering story is real but incomplete.

Current state:

- demotion/tiering machinery exists
- a local filesystem style L2 exists

Still missing or limited:

- no real remote object-store backend such as S3
- L2 reads still operate in a coarse-grained way rather than with the tighter
  access patterns implied by the plans

This is enough for functional groundwork, but not enough for the target operational
model.

### 11. Bootstrap, configuration, and ops remain mostly planned

The binary entry path is still much closer to a development bootstrap than to the
ops model described under `plans/ops/`.

Examples:

- `src/main.rs` is still a small argument-based bootstrap
- `src/node.rs` is explicitly single-node oriented
- I did not find substantive implementation for multi-tenancy, quotas, RBAC,
  backup/restore, or a full observability/exporter layer

Claude's review sharpened this further with several concrete omissions that match
the current tree:

- no `wendelon.toml` style config loader
- no real multi-node join/bootstrap-from-peer flow
- no Prometheus exporter wiring
- no lifecycle/drain hooks for graceful shutdown
- no backup/DR workflow or rolling-upgrade protocol

This is one of the clearest areas where the plans are farther ahead than the code.

## Plan Reconciliation Items

Claude's assessment usefully highlighted a separate class of work: places where
the plans themselves now need updating so they match the tree.

The most important examples are:

- `VNODES_PER_NODE` differs between plan and code
- consensus WAL integrity is described as CRC32c in the plan, while current code
  uses blake3-derived integrity markers
- `plans/019_lazy_migration.md` appears older than the descendant-reconciliation
  logic already present in the migration-complete path

These are not just implementation gaps. They are documentation drift.

## What The Gap Actually Means

The important planning takeaway is that Wendelon is not blocked on fundamental
architecture. It is blocked on finishing the surfaces and guarantees that make the
architecture externally true.

In practical terms, the highest-value gap categories are:

1. control-plane RPC completion
2. full client-visible data-ring ingress
3. durable write semantics
4. real recovery on restart
5. production bootstrap and security hardening

Those items matter more than adding new internal mechanisms, because many of the
internal mechanisms already exist.

## Recommended Interpretation Of `plans/`

`plans/` should no longer be treated as a sequential implementation tracker.
Several of the numbered plans have already been partially or substantially absorbed
into the codebase, while some cross-cutting requirements outside those plans still
lag.

A more accurate next planning artifact would be a matrix with three states per
feature area:

- implemented
- partial
- still planned

That would better reflect the current tree than the implied chronology of the plan
numbers.

## Validation Notes

I also ran the test suite as part of the assessment.

- `cargo test` passes outside the sandbox
- the initial sandbox-local failure was due to `io_uring` permissions, not to a
  failing test in the codebase

So the gap assessment should be interpreted as a capability and completeness review,
not as evidence of a currently broken build.

The green test suite also should not be over-read. Several plan-level acceptance
criteria still appear to lack direct end-to-end coverage, especially around restart
recovery, streaming behavior, and some operational workflows.
