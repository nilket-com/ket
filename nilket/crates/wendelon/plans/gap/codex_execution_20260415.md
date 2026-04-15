# Wendelon Gap Closure Execution Plan — 2026-04-15

Author: Codex
Inputs:
- `plans/gap/codex_assessment_20260415.md`
- `plans/gap/claude_assessment_20260415.md`

## Purpose

This document converts the two gap assessments into an execution roadmap.

It is not a second architecture spec. The architecture already exists in
`plans/` and in the code. The job here is to decide:

- what should be implemented first
- what is blocked by what
- where a short mini-plan is still required before coding
- what "done" means for each execution wave

## How To Use This Document

Use the assessment docs as the source of scope and this document as the source
of ordering.

The intended workflow is:

1. Pick the next milestone from this roadmap.
2. If the milestone contains a "mini-plan required" item, write that short plan
   first.
3. Execute the milestone as a bounded work packet with explicit tests.
4. Update both the roadmap and the relevant plan docs when code reality changes.

Do not drive implementation directly from the assessment docs alone. They are
good at identifying gaps, but they do not fully define sequencing, dependency
order, or exit criteria.

## Execution Sizing

Use the following sizing language when turning milestone items into actual work
packets:

- `S` — one small PR or a tightly bounded change
- `M` — several related changes or a short sequence of PRs
- `L` — design-sensitive enough that it should get a short mini-plan before code

Treat `L` as "design first," not "large by default." Some items are risky because
they set contracts, not because they require a lot of code.

## Execution Principles

- Close correctness gaps before feature-surface gaps.
- Make the architecture wire-reachable before polishing the SDK.
- Prefer multi-node truth over single-node convenience when the plans describe
  distributed behavior.
- Treat plan/code drift as work, not as documentation trivia.
- Ship milestone-sized slices with tests rather than opening every gap at once.

## Mini-Plans Required Before Coding

Most gaps are implementation work, not design work. A few still need a short
execution note before code starts.

### 1. fsync-before-quorum semantics

Why this needs a mini-plan:

- The current gap is not just missing functionality.
- It is a guarantee mismatch between the write path described in `plans/` and
  the actual durability boundary in code.

The mini-plan should settle:

- exactly which append paths must force durability before ack
- whether the system uses per-write fsync, group commit, or another explicit
  durability boundary
- how the local leg and remote legs align semantically
- what crash tests prove the guarantee

### 2. WAL recovery and bootstrap contract

Why this needs a mini-plan:

- The code has partial WAL machinery, but the live bootstrap path still starts
  from an in-memory store.
- Recovery semantics interact with checkpoints, snapshots, and startup flow.

The mini-plan should settle:

- what `open()` guarantees on restart
- when replay ends and checkpoint/snapshot state takes over
- how `node.rs` chooses between bootstrap and recovery
- what compatibility assumptions hold for existing on-disk state

### 3. Control Ring server surface

Why this needs a mini-plan:

- The trait exists, but there is no server-side implementation.
- Several later milestones depend on this surface existing.

The mini-plan should settle:

- which RPCs are required in slice 1
- which service owns dispatch and routing
- how status, lease, epoch, and leadership operations are framed on the wire
- what must be reachable externally before client work continues

### 4. SAN -> NodeId verification contract

Why this needs a mini-plan:

- The security plan assumes a stronger identity binding than the code enforces.
- The implementation depends on certificate naming conventions that should be
  explicit before code lands.

The mini-plan should settle:

- where the authoritative `NodeId` is encoded in certificates
- what exact verification rule is applied at accept/dial time
- how mismatch is surfaced
- whether this blocks all peers or can roll out behind a compatibility flag

### 5. Epoch-transition auto-seeding for lazy migration

Why this needs a mini-plan:

- The ledger logic is already substantial.
- The remaining gap is mostly orchestration, but it touches epoch advancement
  and control-plane ownership.

The mini-plan should settle:

- where the seeding hook lives
- what input diff drives `SeedMigration` emission
- how idempotency works across retries/restarts
- what tests prove chained migrations remain correct

## Workstreams

This is the recommended top-level decomposition.

### Workstream A — Correctness Foundation

Goal:
- make the published durability, recovery, and identity guarantees true

Primary gaps:
- fsync-before-quorum
- WAL recovery and restart correctness
- SAN -> NodeId verification

Why first:
- everything else builds on these guarantees
- feature work on top of weaker semantics creates rework

### Workstream B — Reachable Control Plane

Goal:
- make control-plane behavior externally reachable over a real server surface

Primary gaps:
- server-side `ControlRing` implementation
- status/lease/leadership/epoch RPC reachability
- membership advancement through committed control-plane actions

Why second:
- several other features are blocked on a real control plane

### Workstream C — Reachable Data Plane And Client Surface

Goal:
- expose the implemented data-plane behavior on the wire and make the client
  capable of using it

Primary gaps:
- HRFT ingress for write/read/bulk
- stream finalization and bulk-write completion
- client write/read/bulk APIs
- topology subscribe and better retry policy
- WPBC streaming consumer

Why third:
- much of the internal data path already exists
- the missing step is end-to-end reachability, not invention

### Workstream D — Migration, Leases, And Tiering Closure

Goal:
- move the implemented migration and lease machinery from partial to complete

Primary gaps:
- auto-seeding migrations on epoch changes
- migration sweeper
- tombstone-aware reclaim
- signed leases and transfer-token verification
- L2 backend and rehydration improvements

Why fourth:
- the foundation is already there
- this work is most valuable once control-plane and data-plane reachability exist

### Workstream E — Bootstrap And Operations

Goal:
- move from development bootstrap toward a real operable cluster

Primary gaps:
- config loader
- multi-node bootstrap/join
- observability
- lifecycle hooks
- backup/restore and upgrade mechanics

Why fifth:
- some ops work can happen in parallel, but cluster bootstrap should follow
  correctness and control-plane truth

## Parallelism Notes

This roadmap is dependency-ordered, not strictly serial.

The main hard gate is M1. Before M1 lands, the project should not make stronger
durability, recovery, or identity claims than the code actually supports.

After M1:

- M2 and early observability/lifecycle work can proceed in parallel
- M3 can begin once the core durability contract is settled, even if some
  control-plane work is still in flight
- the config and identity-material slice of M6 may be pulled forward if
  Workstream D needs signing keys before the rest of the ops baseline lands
- M5 should wait until M2 through M4 are substantially in place
- M7 is intentionally later hardening work, not part of the first closure wave

## Milestones

### M0 — Alignment And Slice Contracts

Objective:
- lock the execution assumptions that would otherwise cause churn midstream

Scope:
- decide whether WAL integrity stays blake3-based or moves to CRC32c
- decide whether `VNODES_PER_NODE` remains `64` or moves to `256`
- write the mini-plans listed above
- explicitly mark which gaps are phase-1 execution targets and which are later

Suggested artifacts:
- `plans/020_durability_boundary.md`
- `plans/021_wal_recovery.md`
- `plans/022_control_ring_server.md`
- `plans/023_san_nodeid_binding.md`
- `plans/024_epoch_transition_autoseed.md`

Exit criteria:
- mini-plans written for the five design-sensitive items
- plan/code drift items assigned a disposition
- milestone ordering accepted as the working roadmap

Why this exists:
- this is a small amount of planning that reduces a large amount of rework

### M1 — Make Core Guarantees True

Objective:
- close the correctness-shaped gaps before expanding surface area

Scope:
- implement fsync-before-quorum semantics
- implement WAL replay/recovery in live bootstrap
- enforce SAN -> NodeId verification on transport identity

Dependencies:
- M0 mini-plans for durability, recovery, and SAN verification

Definition of done:
- acknowledged writes survive the crash model promised by the plans
- restart path rehydrates committed state from durable storage
- peers with valid chains but wrong SAN identity are rejected

Required tests:
- crash/restart durability test for quorum-acked writes
- node restart recovery test covering WAL replay
- transport identity test proving SAN mismatch rejection

### M2 — Stand Up The Control Ring

Objective:
- make control-plane actions externally reachable

Scope:
- implement server-side `ControlRing`
- wire `STATUS_REQ` / `STATUS_RESP`
- expose lease acquisition, leadership transfer, epoch advance, and cluster
  status through real RPC handling
- replace watch-only membership advancement assumptions with committed control
  actions where applicable

Dependencies:
- M0 control-ring mini-plan
- M1 is strongly preferred first, though parts of this can start earlier

Definition of done:
- control-plane RPCs are no longer in-process only
- topology/status retrieval works against a real server
- epoch and lease workflows have an externally reachable path

Required tests:
- end-to-end control-ring status test
- lease acquisition RPC test
- epoch-advance RPC test
- leadership-transfer RPC test

### M3 — Expose The Data Plane On The Wire

Objective:
- make the implemented data path actually reachable by clients

Scope:
- route `WRITE_REQ`, `READ_REQ`, and `BULK_WRITE_REQ` through HRFT ingress
- replace current stub behavior for `stream_finalization` and `bulk_write`
- ensure read/write/bulk flows align with the protocol docs

Dependencies:
- M1 correctness work should land first
- M2 is preferred because some client routing depends on status/topology truth

Definition of done:
- write/read/bulk operations are reachable over the network, not only through
  in-process tests
- protocol handlers exist for the client-visible message types already defined
  in schema

Required tests:
- end-to-end write over HRFT
- end-to-end read over HRFT
- end-to-end bulk-write path test
- stream-finalization test

### M4 — Complete The Client And Query Surface

Objective:
- make the client consistent with the exposed data and control plane

Scope:
- add client write/read/bulk APIs
- add topology subscription or another accepted long-lived refresh model
- upgrade retry behavior beyond one-shot redirects
- add client-side streaming result handling
- align WPBC response handling with streaming semantics

Dependencies:
- M2 and M3

Definition of done:
- the client can use the wire-reachable data plane without hidden in-process
  assumptions
- topology handling and retry behavior match the expected distributed model
- query results no longer assume single-frame responses only

Required tests:
- client redirect/retry tests
- topology refresh/subscription tests
- streaming query client test
- bulk-write client API test

### M5 — Close Lazy Migration And Lease Authenticity

Objective:
- complete the remaining automation and authenticity gaps in the migration and
  lease subsystems

Scope:
- auto-seed migrations on epoch transition
- add background migration sweeper
- add tombstone-aware reclaim/compaction
- sign leases and verify transfer tokens
- teach the client to treat `MIGRATION_INCOMPLETE` as a real retry case

Dependencies:
- M2 control plane
- M3 data-plane reachability
- M4 client improvements
- M0 mini-plan for migration auto-seeding

Definition of done:
- migration seeding does not rely on an external manual path
- migration completion can progress automatically
- lease and transfer authenticity are cryptographically enforced

Required tests:
- epoch transition emits migration seeds
- background migration drain test
- chained migration correctness test
- signed-lease validation test
- transfer-token verification test

### M6 — Multi-Node Bootstrap And Baseline Operations

Objective:
- move the system from a development binary toward a usable cluster baseline

Scope:
- add a config loader
- add multi-node bootstrap and join flow
- add Prometheus/exported metrics baseline
- add lifecycle/drain hooks
- add enough operational structure to run milestone testing on real multi-node
  setups

Dependencies:
- M1 through M3 should already be stable

Notes:
- the config and identity-material portion of this milestone can be pulled
  forward ahead of the rest of M6 if later workstreams need key material before
  full multi-node bootstrap is in place

Definition of done:
- a cluster can be configured and brought up without the current dev-only
  bootstrap path
- basic observability exists for the core flows
- shutdown/drain behavior has explicit hooks rather than abrupt termination only

Required tests:
- multi-node bootstrap test
- join/rejoin test
- graceful-drain lifecycle test
- metrics smoke test

### M7 — Tiering And Production Hardening

Objective:
- complete the most important hardening items after the core cluster behaviors
  are true

Scope:
- add a concrete remote `ObjectStore` backend
- improve L2 read behavior and rehydration
- add missing read-path metrics around tiering
- evaluate server-side WPBC spill control and program caching
- continue backup/restore and rolling-upgrade work if they remain in scope

Dependencies:
- M5 and M6

Definition of done:
- tiering is no longer limited to the local-filesystem-shaped V1 path
- operational behavior under demotion/rehydration is observable
- query execution has a path away from single-frame, all-in-memory assumptions

Required tests:
- remote object-store integration test
- rehydration test
- tiered read latency/behavior smoke test
- large-result WPBC test

## Suggested Near-Term Sequencing

If the team wants the shortest path to making the system architecturally true,
the first execution wave should be:

1. M0 mini-plans
2. M1 correctness foundation
3. M2 control ring
4. M3 HRFT data-plane ingress
5. M4 client completion
6. M5 migration and lease closure

This sequence closes the most structurally important gaps first.

## Items That Should Probably Stay Out Of The First Wave

These are real gaps, but they should not distract from the first execution wave:

- RDMA Tier 0 provider
- PGWire adapter
- DataFusion `TableProvider`
- rack/AZ anti-affinity
- full backup/DR
- rolling-upgrade protocol
- multi-tenancy

These are better treated as second-wave hardening or productization work unless
another project constraint makes them urgent.

## Recommended Work Packet Format

When handing a milestone or subtask to an implementation pass, give it in this
shape:

- objective
- size (`S`, `M`, or `L`)
- exact files/modules expected to change
- explicit invariants that must become true
- explicit non-goals
- dependencies
- tests that must pass or be added
- what mini-plan, if any, governs the change
- what assessment gap or milestone item it closes

That is a much better steering mechanism than saying "close the gaps in plan 012"
or "implement the assessment doc."

## Review Cadence

- Revisit this roadmap whenever a milestone closes or a mini-plan changes a
  contract.
- Promote any design-sensitive item to its own short plan before coding if the
  solution shape is still unsettled.
- If two milestone items collide heavily on the same files, sequence them
  explicitly rather than trying to merge them into one implementation pass.

## Exit Condition For The Gap-Closure Program

The gap-closure program should be considered complete when all of the following
are true:

- the strongest guarantees claimed in `plans/` are also true in code
- control-plane and data-plane behaviors are externally reachable over the wire
- client behavior matches the distributed protocol rather than a single-process
  approximation
- migration, leases, and tiering no longer depend on manual or placeholder paths
- bootstrap and observability are strong enough to exercise the system as a
  real multi-node cluster

At that point the remaining work is no longer "close the architecture gap." It
becomes normal product hardening and feature expansion.
