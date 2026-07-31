# ADR 001: Toron Foundation and Engine Contract

Author: AI Swarm Council (Claude / Codex / Gemini)
Primary draft: Codex

## Status

Promoted. Companion implementation plan at `plans/impl/001_toron_foundation_and_engine_contract.md`.

Council converged in `plans/wip/` after Claude / Codex / Gemini
iterations. Seed brainstorms under `plans/` have been deleted.

## Context

Nilket is a vertically integrated analytics platform. Wendelon is the
smart storage ring; Toron is the compute ring. The boundary exists for
scaling dynamics and resource isolation, not because storage and
compute are independent products. We control both ends of the
contract.

Toron starts from zero code while Wendelon already has a large active
storage substrate: PBC pushdown, row-group fetch, sparse projection,
encoded validity, page stats, FSST, CDC, schema descriptors, tenant
identity, and adaptive transport work. The first Toron decision must
therefore be both architectural and practical: define the durable
engine contract while keeping the first implementation small enough to
compile, test, and change.

The council explicitly rejects using a generic SQL-first distributed
engine as the foundation. Toron needs Wendelon locality, PBC pushdown,
query-session identity, storage-authored status mapping, tenant
context, and a Rune-first typed programming model. Adapting
DataFusion or Ballista would make those properties extension hooks
around somebody else's center of gravity.

## Decision

### 1. Build Toron greenfield

Toron is a greenfield Arrow-native compute engine. It may reuse
foundational crates and ideas, but it does not build on DataFusion or
Ballista.

Allowed reuse:

- `arrow-rs` and Arrow compute kernels.
- `sqlparser-rs` later for the SQL compatibility surface.
- Algorithms, benchmark ideas, and small implementation ideas after
  source review.

Rejected for V0:

- DataFusion logical plans, optimizer, physical execution, and
  scheduler foundations.
- Ballista scheduler / execution model.
- DataFusion-family crates that pull in the plan / optimizer /
  execution stack.
- `datafusion-functions` or similar built-in catalogs. Revisit only
  after the SQL surface exists and dependency cost is clear.

### 2. `toron-ir` is the engine contract

Rune is the primary programmable surface. SQL is a compatibility
surface. Both lower to `toron-ir`; neither is the engine truth.

`toron-ir` is the durable contract consumed by the planner,
optimizer, executor, and future worker wire. In V0 it lives as a
module inside `crates/toron`; it splits into a separate crate only
when a real consumer forces it: Baryon, Polariton, persisted
dashboard IR, or a stable coordinator-worker wire boundary.

Engine capabilities land in IR first, then surface through Rune and
SQL. The engine does not grow a SQL-only capability catalog.

### 3. Start with one crate

V0 starts with one package:

```text
crates/toron
```

Initial internal modules:

- `ir`
- `planner`
- `exec`
- `storage`
- `pbc`
- optional `server` once a process exists

Future split candidates:

- `toron-ir` when Baryon / Polariton imports IR, dashboards persist
  IR, or workers receive IR over the wire.
- `toron-wire` when coordinator / worker communication is real.
- `toron-rune` when Rune bindings are real.
- `toron-plan` when optimizer state becomes large enough to justify
  an independent compile boundary.

Empty crates are rejected. Splits happen under consumer or compile
pressure.

### 4. V0 IR vocabulary

The initial logical nodes are:

- `Scan`
- `Filter`
- `Project`
- `Aggregate`

`Project` is part of V0. Without it, the executor over-fetches
columns and the PBC / projection boundary is harder to test honestly.

The initial expression catalog is:

- column reference
- typed scalar literal
- comparison: `Eq`, `Neq`, `Lt`, `Lte`, `Gt`, `Gte`
- boolean: `And`, `Or`, `Not`
- null tests: `IsNull`, `IsNotNull`

The initial aggregate catalog is:

- `CountAll`

Every expression carries or can derive:

- result Arrow data type
- nullability
- deterministic / non-deterministic marker
- PBC pushdown eligibility

PBC translatability is a first-class property of expression planning,
not a post-hoc check. The key question is whether an expression can
lower to Wendelon PBC without changing Arrow null semantics.

### 5. Storage boundary: `StorageScan`

Toron operators do not call Wendelon internals directly. Operators
consume Arrow batches from a storage boundary.

The conceptual boundary is:

```rust
trait StorageScan {
    fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError>;
}
```

The M3 real shape is async and stream-oriented because Wendelon I/O is
async. Pure Arrow operators stay synchronous. A narrow adapter pumps
async scan batches into the sync operator pipeline. Toron does not
take a runtime dependency before M3 unless earlier implementation
proves it necessary.

`ScanRequest` must express:

- tenant id
- table object id or table reference
- snapshot / HLC read bound
- projected columns
- optional PBC predicate program
- trace context
- storage-authored status mapping
- query-profile hint analogous to ADR 062 `PointRead` / `Scan`

Implementations:

- `InMemoryStorageScan` for M1 unit fixtures and fast IR / executor
  progress.
- `WendelonStorageScan` in M3, wrapping `wendelon-client`.

Wendelon remains authoritative for schema / catalog truth. Toron
caches descriptors; it does not become a competing catalog authority.

### 6. Workspace and Wendelon dependency posture

The monorepo root `Cargo.toml` is the workspace root. M0 should make
the root workspace load with Toron.

Wendelon's nested workspace should be flattened in M0 unless the
attempt exposes a concrete blocker. Workspace exclusions are allowed
only as a temporary fallback with the blocker recorded. This is a
manifest-level migration, not a Wendelon code rewrite.

Do not extract `wendelon-net`, `wendelon-types`, `wendelon-schema`,
or `wendelon-client` before Toron has concrete pressure. The initial
PBC lowering target is Wendelon's existing `schema::pbc` API. If that
pulls in too much of Wendelon, record it as extraction pressure; do
not preemptively carve the storage tree before M2 / M3 evidence.

### 7. Milestone sequence

Each milestone must ship a runnable slice with fixture-backed tests.
Framework-only work does not count as complete.

#### M0: Monorepo scaffold

Goal: root workspace loads and Toron has a compile target.

Tasks:

- Add `crates/toron/Cargo.toml` and `src/lib.rs`.
- Keep council WIP material under `crates/toron/plans/wip/`.
- Add `plans/adr/` and `plans/impl/` at promotion.
- Add a minimal Toron smoke test.
- Flatten Wendelon's nested workspace, or record a concrete blocker
  and temporary exclusion.

Verification:

```text
cargo metadata --no-deps --format-version 1
cargo test -q -p toron
```

#### M1: Single-process IR execution

Goal: execute a hand-built logical plan over in-memory Arrow batches.

Scope:

- implement V0 IR structs / enums
- type-check the V0 expression catalog
- implement an Arrow vectorized interpreter for V0 expressions
- implement `CountAll`
- execute `Scan -> Filter -> Aggregate`

No SQL. No Rune. No Wendelon network. No distributed worker.

#### M2: PBC lowering contract

Goal: pushdown eligibility is real and testable.

Scope:

- map V0 expression nodes to Wendelon PBC opcodes
- produce deterministic PBC payloads or structured PBC programs
- record private decline reasons for non-pushable nodes
- preserve Arrow null semantics in lowering tests

Use Wendelon's existing `schema::pbc` API directly first. Shared
protocol extraction waits for evidence.

#### M3: Wendelon client adapter

Goal: replace `InMemoryStorageScan` with a real Wendelon-backed
adapter without changing the executor.

Scope:

- implement `WendelonStorageScan`
- use ADR 034 compute/storage contract surfaces:
  query-session identity, trace context, storage-authored status
  mapping, schema intent separation
- pass tenant id and PBC predicate through the adapter
- receive Arrow batches through `wendelon-client`

Tests start with in-process Wendelon fixtures, then add a live local
cluster integration test to prove the QUIC path.

#### M4: Single-process morsels

Goal: prove the executor's parallel unit before distribution.

Scope:

- introduce `MorselId`, `StageId`, and local stage DAG newtypes
- split a scan into deterministic morsels
- run morsels in one process
- merge `CountAll` results deterministically

Hard gates:

- single-thread and multi-thread runs produce identical output
- retrying a morsel is idempotent
- morsel order does not affect final result
- determinism digest matches across independent runs

#### M5: Coordinator / worker boundary

Goal: split the process after IR, execution, and storage adapter
semantics are stable.

Scope:

- introduce coordinator and worker binaries or modes
- define native messages for:
  - submit IR
  - dispatch morsel
  - stream incremental Arrow result batches
  - report storage-authored error
- use QUIC first
- no RDMA, SIW, or RXE in V0

M5 uses streaming message shape from the start, not a buffered
whole-result RPC. It needs single-stream boundedness; full shuffle
backpressure lands in M6.

Crash-only hard gate: kill a worker mid-query; the coordinator
detects the failure, redispatches idempotently, and output is
byte-identical to the non-failure run.

Workers are stateless per fragment. Coordinators hold client session
state, tenant context, cursors, and query lifecycle.

#### M6: First shuffle

Goal: add one real distributed operator after single-stage work is
boring.

Scope:

- add `GroupBy` and `Exchange`
- implement hash partition exchange over QUIC
- implement partial and final hash aggregate
- add bounded memory accounting and receive-side backpressure

Hard gates:

- distributed `GROUP BY` output is deterministic across independent
  runs
- partition order does not affect final aggregate output
- receive-side backpressure prevents unbounded buffering

Do not implement encrypted spill in the same milestone. Spill is its
own ADR because it touches local disk, crypto, cleanup, and recovery.

#### M7+: frontends and follow-ups

Order after M6 is intentionally not pinned before evidence exists.
Candidate follow-ups:

- SQL frontend: `sqlparser-rs` into `toron-ir` for a small analytical
  subset. This may land before Rune as parser pressure, but SQL
  remains the compatibility surface.
- Rune frontend: typed DataFrame API plus sandbox primitives. Rune
  remains the primary product surface.
- encrypted local spill via Wendelon crypto primitives
- Wendelon-side crate extraction driven by real M3 / M5 / M6 pressure
- second transport tier for hot-path edges
- advanced engine capabilities: sketches, regression, time-series,
  hybrid search

### 8. Testing discipline

Toron inherits Wendelon's ADR / fixture discipline from day one.

- Every IR operator lands with a fixture and hand-computed oracle.
- Every pushdown-eligible expression has an engaged fixture and a
  declined fixture.
- Every pushdown test verifies Toron and PBC semantics under Arrow
  null rules.
- Every executor optimization has a determinism gate.
- Cross-process determinism lands before additional transport tiers.
- The initial fixture corpus is Wendelon-native, not TPC-H. Sparse
  projection, encoded validity, page stats, ADR 070 all-null elision,
  and FSST matter more to Toron V0 than generic benchmark coverage.

## Rationale

### Why greenfield

Toron is specialized around a storage ring we control. Its core
advantages come from knowing Wendelon's physical layout and pushdown
capabilities. A generic SQL planner would either hide these facts or
force them into side channels. Building the engine contract ourselves
keeps the storage/compute boundary explicit.

### Why one crate first

With zero Toron code, crate splits would mostly encode guesses. A
single crate keeps refactors cheap while the IR, executor, PBC
compiler, and storage adapter are still moving. The planned module
names preserve future extraction points without making empty crates
part of the initial API.

### Why in-memory before Wendelon

The IR, type checker, executor, and Arrow kernel boundary can be
tested without Wendelon runtime topology, TLS, transport policy, or
cluster fixtures. In-memory `StorageScan` gives Toron a fast dev loop
and a stable executor contract. Wendelon integration then swaps the
storage implementation behind the same boundary.

### Why PBC lowering before Wendelon adapter

PBC pushdown is a core Toron planning property. If lowering is
bundled with the Wendelon adapter, failures become hard to attribute:
IR semantics, PBC encoding, storage routing, and Arrow stream
materialization all move at once. M2 isolates the pushdown contract.

### Why M4 before M5

Morsel determinism should be proven in one process before transport
is introduced. Otherwise M5 failures are ambiguous: scheduler bug,
retry bug, serialization bug, or network bug. M4 makes the execution
unit boring before it crosses a wire.

### Why streaming shape in M5

The first coordinator / worker protocol should not teach the system
that query results are whole buffered objects. M5 needs only
single-stream boundedness, not full shuffle backpressure. Choosing
streaming message shape now avoids a predictable protocol rewrite
when Polariton and M6 exchange arrive.

### Why no pinned-memory shuffle yet

Pinned memory, NIC registration, and allocator strategy are RDMA
work. M6's first shuffle runs over QUIC and should prove correctness,
determinism, and backpressure using ordinary Arrow buffers. RDMA-tier
memory work earns its own ADR.

## Consequences

- Toron accepts the cost of building a planner and executor instead
  of adapting a generic engine.
- Early SQL support is deferred. SQL remains important, but the first
  engine contract is handwritten IR.
- Rune is deferred past the distributed skeleton even though it is
  the primary product surface.
- Wendelon integration does not arrive until M3.
- Wendelon-side crate extraction is delayed until evidence exists.
- Full multi-tenant authorization, HLC-coordinated time travel,
  federation, visualization operators, encrypted spill, and hybrid
  vector search are all out of V0.

## Explicit Rejections

- DataFusion / Ballista as the core engine.
- SQL-first architecture.
- Direct operator access to Wendelon internals.
- Distributed runtime before single-process semantics.
- Public federation in V0 / V1.
- Protocol branching or transport-tier inspection inside operators.
- Wendelon-side crate extraction before M2 / M3 pressure proves it.
- Cranelift or any JIT in the V0 binary.
- Visualization-as-compute IR operators before Polariton has a tile
  protocol.
- Pinned-memory / RDMA allocator commitments in the first shuffle.

## Open Questions

These do not block ADR 001. They belong in the companion
implementation plan as calibration items.

1. Is the `rune` crate sandboxable enough for native-function
   allow-lists, fuel, CPU budget, memory budget, and no-filesystem /
   no-network policy?
2. What is the exact shape of the in-process Wendelon fixture harness
   for M3?
3. What is the smallest SQL subset worth adding after M6 if SQL lands
   before Rune for parser pressure?

## Promotion Follow-Up

Promote the companion implementation plan at:

```text
crates/toron/plans/impl/001_toron_foundation_and_engine_contract.md
```

Add a Toron WIP roadmap mirroring Wendelon's planning pattern after
ADR 001 and its implementation plan land.
