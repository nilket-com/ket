# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not at
implementation commits. The council operates on a block-chain
rhythm: plan → implement → plan → implement, and this file is a
snapshot of the most recent plan. Between sessions the roadmap
lags the code on purpose — an item can read "ADR N
implementation phases pending" while several phases have
already landed on `main`. That is expected. Check `git log` and
the relevant impl plan / bench-results file for actual status,
and roll roadmap edits into the next planning session along
with the forward-sequence rewrite.

## Status

- **ADR 001 (Foundation and Engine Contract)** — promoted at
  `plans/adr/001_toron_foundation_and_engine_contract.md`.
- **ADR 002 (Storage-Backed Execution Contract)** — promoted
  and implemented. Bench evidence at
  `plans/impl/002_storage_execution_contract_bench_results.md`.
- **ADR 003 (Distributed Runtime and Data Movement Contract)**
  — promoted at
  `plans/adr/003_toron_distributed_runtime_and_data_movement.md`,
  implemented. Bench evidence at
  `plans/impl/003_distributed_runtime_bench_results.md`.

V1 target is **"First Query with Integrity"** plus
**"Distributed Execution Correctness"**: correct analytical
results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage boundary,
multi-process coordinator/worker mechanics surviving a worker
SIGKILL with byte-identical normalized output. Distributed
joins, multi-tenant governance, and pipelined execution remain
post-V1.

## Prioritization philosophy

Toron is the compute ring of the Nilket stack; Wendelon is the
storage ring. We control both ends. The boundary exists for
scaling dynamics and resource isolation, not because storage
and compute are independent products.

**Toron's reason for existence is smart-storage pushdown.**
ADR 002 made that load-bearing in the runtime path. ADR 003
makes the cross-process mechanics real. Every other improvement
(executor model, advanced kernels, frontend hardening) polishes
a system that does its primary job along its primary axis.

**ADR-by-ADR cadence.** Wendelon ADR 070 pattern: narrow,
evidence-driven, single-decision contracts. ADR 003 ships
runtime mechanics only; it does not bundle distributed joins
or tenant governance even though both need ADR 003's substrate.
Bundling defers real decisions behind larger ones.

**Evidence drives scope.** The most informative artifact for
the next ADR is the previous ADR's bench results. ADR 003's
keystone fixture (SIGKILL + cross-replica determinism + budget
exhaustion across worker counts) will tell us whether ADR 004
attacks distributed joins (likely), executor model (only if
the buffered executor visibly groans post-distribution), or
governance (only if multi-tenant pressure shows up before V1
launch).

## Forward sequence

Earlier items have firmer scope; later items are direction,
not commitment.

### Next: ADR 004 planning

ADR 003's implementation makes real worker endpoints,
pull-addressed shuffle, producer-loss replay, registry-backed
attempt placement, and canonical distributed output comparison
available as substrate. ADR 004 should now choose the next
contract surface from the bench evidence and fixture gaps below,
with distributed joins still the leading candidate.

### ADR 004 candidates *(scope evidence-driven from ADR 003 bench)*

The most likely ADR 004 is **distributed joins**. Alternative
shapes exist if ADR 003 evidence redirects.

- **Distributed joins.** Most likely. Broadcast vs hash-
  partitioned, bloom filter pushdown into Wendelon, skew
  detection, join-key null semantics, zero-copy join
  materialization. Depends on ADR 003 shuffle + ADR 002
  grouped-aggregate semantics.
- **Multi-tenant resource governance** (alternative). Per-
  query / per-tenant CPU + memory + spill + stream-bytes
  budgets. Token-bucket vs scheduler-slot admission. Memory-
  rejection vs spill-trigger thresholds. Promotes if V1
  launch pressure surfaces governance before joins.
- **Executor model upgrade** (alternative). Streaming end-to-
  end, morsel parallelism, push-based execution. Promotes
  only if ADR 003's post-distribution evidence shows the
  buffered executor is the new bottleneck. ADR 002's
  pushdown + ADR 003's distribution should remove most
  filter / aggregate pressure first.
- **Rune sandbox enforcement.** V1 launch prerequisite.
  Native-function allow-list, fuel engagement (currently
  commented out at `rune_engine.rs:188`), per-tenant CPU +
  memory budgets, no filesystem / no network gating. Could
  ship as its own narrow ADR alongside ADR 004 if launch
  timing demands.

### Advanced kernel graduations *(one ADR per kernel)*

Wendelon ADR 070 narrow-decision pattern. Each promotes under
its own ADR with bench evidence.

- **`LinearRegress` stable contract.** Current implementation
  is straight OLS without condition-number guard, no centered
  formula, no rank check. Numerical hardening + stability
  across numeric types.
- **`ApproxQuantile` stable contract.** Current extraction is
  `sort + index` of KLL retained values, which discards the
  rank statistics KLL was designed to preserve. Real rank-
  based estimation + cross-impl parity with Wendelon's KLL
  substrate.
- **`TimeBucket` stable contract.** Bucket-alignment semantics
  across timezone, duration, and pre-epoch inputs.

### Brainstorms and high-level vision

Long-horizon shapes the council has noted but not committed.

- **Brainstorm 001: Pure Cranelift JIT as a First-Class Query
  Specializer.** JIT-compiling hot kernels (predicates, UDFs)
  into native machine code for zero-copy, bare-metal
  performance. Reserved for post-V1 deliberation.
- **Brainstorm 002: WeakControlRing.** In-memory Raft ring for
  HA coordinators, cluster-wide quotas, and shuffle barrier
  synchronization without the persistence tax of NVMe / WAL.
  ADR 003's `WorkerRegistry` trait + `WorkerLease` /
  `LeaseEpoch` fields are the seam.

### V1.x items *(post-V1, evidence-driven)*

Items the council has explicitly deferred from V1 ADRs.
Promoted to their own ADRs when evidence drives them.

- **Pipelined cross-stage execution.** ADR 003 ships strict
  stage barriers; pipelining adds latency wins at the cost
  of failure-recovery complexity. V1.x when the failure-
  recovery story is concrete.
- **Locality-aware fragment routing.** ADR 003 is location-
  agnostic; locality requires new Wendelon public-API
  surface and conflates runtime identity with storage
  coordination. V1.x.
- **RDMA / RoCEv2 / SIW transport tier.** M9 seeds the
  transport-tier-negotiation shape; QUIC preferred through
  V1. V1.x when transport-tier evidence justifies the
  surface.
- **Operator-internal streaming end-to-end.** ADR 003
  requires stream-shaped task output; full operator-internal
  streaming is V1.x executor work.
- **Subtree-split pushdown.** Lifts ADR 002's
  `FilterDisposition::FullyPushed | EvaluateInToron` to add
  `PartialResidual { residual_predicate }`. Requires a
  planner-side cost model.
- **Encrypted spill governance.** Spill lifecycle beyond M10
  seed: key rotation, AAD policy, crash recovery,
  observability, multi-query isolation. Seed uses
  AES-256-GCM-SIV without rotation.
- **Higher retry counts.** ADR 003 ships `max_attempts = 2`;
  future ADR raises with evidence of which failure modes
  benefit.
- **Wendelon-side crate extraction.** `wendelon-types` (PBC
  ops, identity, fingerprint), `wendelon-net` (transport
  facade), `wendelon-schema` (descriptor types). Defer until
  multi-consumer pressure (Polariton, Baryon, external Toron
  SDK) materializes.
- **Decimal numeric types.** Their own ADR; pins decimal-
  specific aggregate semantics. Deferred by ADR 002
  §"Resolved Q2".
- **Full DDL propagation.** Async invalidation broadcasts,
  multi-coordinator schema consensus, descriptor versioning
  beyond fingerprint-pin.
- **Wire-protocol cross-version compat.** ADR 003 ships
  reject-on-mismatch; rolling upgrades are V1.x.

### V2+ items

Direction, not near-term plan.

- **Replicated / disaggregated shuffle service.** ADR 003's
  per-worker partition store is the V1 substrate; a
  standalone service is V2 when evidence shows producer-
  hosted partitions are the next bottleneck.
- **Direct P2P shuffle as fast path.** V2 optimization over
  the V1 pull-addressed contract.
- **Hybrid vector search.** Vector ANN with metadata pushdown
  into Wendelon. Waits for Wendelon vector-aware placement.
- **Visualization-as-compute IR operators.** Density tiles,
  2D binning, quantile-per-pixel. Waits for Polariton's tile
  protocol.
- **External federation** (Postgres / S3 / BigQuery
  adapters). Explicit ADR 001 §"Deferrals" item — "one
  external adapter as a capability proof," never on the
  Wendelon-native hot path.

### Refinement plumbing *(opportunistic, no ordering)*

Small improvements that don't fit a single ADR.

- **Capability-table aggregate dispatch** replacing the enum
  (when the aggregate catalog grows beyond hand-listable).
- **Newtype hygiene pass** — `Symbol` / `ColumnId` instead
  of bare `String` for column refs in narrower contexts.
- **PBC catalog growth** — additional ops promoted from
  experimental to stable.

## Backlog (parked, may not return)

Ideas explored in `plans/wip/*topics*.md` that did not earn
forward placement. Kept here as a record of what the council
considered and let go.

- **New IR level below `ExecutionPlan`** (`PhysicalFragment`
  vocabulary). Considered for ADR 003 unit-of-work; rejected
  because `TaskSpec` wraps the existing bound plan without
  adding translation surface.
- **Pure spill-to-pull as V1 default shuffle.** Considered
  for max resilience; rejected because disk-write + decrypt
  latency on every shuffle is unacceptable for in-memory-
  sized queries.
- **Transport-native flow control alone as backpressure
  contract.** Considered; rejected because wire-level flow
  control can't bound application-level memory or shuffle
  bytes.
- **Schema-registry hardening as ADR 003 alternative.**
  Considered; rejected for lack of high-frequency DDL
  pressure in ADR 002 bench evidence.
- **Stable worker assignment as the determinism gate.**
  Considered; rejected because worker pools are dynamic and
  assignment parity isn't a useful user-facing contract.
- **`PhysicalDescriptor` evolution beyond fingerprint-pin**
  in ADR 002 scope. Considered; resolved by single-retry-
  then-fail behavior. Full DDL propagation parked for V1.x.

## What Not To Do Next

Enduring guardrails for upcoming work. Items tied to specific
shipped ADRs live in those ADRs' §"What Not To Do" sections;
this list is the cross-cutting set.

- **Don't adapt DataFusion / Ballista.** ADR 001 rejected;
  ADR 002 reinforced. The greenfield decision is final unless
  a future ADR cites concrete evidence reversing the surface-
  area-mismatch argument.
- **Don't bundle scope across contract surfaces.** Distributed
  joins, multi-tenant governance, executor model, and Rune
  sandbox enforcement are each their own ADR. ADR 003
  intentionally narrow; ADR 004 stays narrow.
- **Don't ungate experimental kernels by default.** ADR 002
  requires `PlannerConfig::allow_experimental_kernels =
  false`. Each kernel graduates under its own narrow ADR.
- **Don't put `Result<_, String>` in any public or module-
  crossing signature.** ADR 002 G3 enforces; ADR 003 G9
  enforces for distributed surface.
- **Don't skip descriptor-fingerprint validation on PBC
  pushdown.** Silently-wrong-results is the failure mode
  ADR 002 paid complexity to prevent.
- **Don't extract Wendelon types into a shared crate before
  V1.x consumer pressure.** ADR 001 / 002 / 003 import
  allowlist holds.
- **Don't bypass the Wendelon import allowlist.** Production
  code consumes `wendelon::schema::{pbc, *types}`,
  `wendelon_client::*`, `wendelon::network::pool::*`,
  `wendelon::storage::file_ops::*`. Forbidden:
  `wendelon::network::raft::*`, `wendelon::network::quic::*`,
  `wendelon::storage::lsm::*`, ad-hoc page readers, anything
  inside Wendelon's node / raft / transport internals.
- **Don't query Wendelon for ring placement.** ADR 003 V1 is
  location-agnostic. Locality routing is V1.x.
- **Don't initiate P2P-push from producer to consumer.**
  ADR 003 G11 enforces. Consumers always pull via
  `PullPartition`.
- **Don't use `tokio::time::sleep` for backpressure** in the
  production worker path. ADR 003 G10 enforces. The per-
  query budget envelope is the contract.
- **Don't dispatch `LogicalPlan` to workers.** ADR 003 G12
  enforces. Coordinator binds; workers receive `TaskSpec`.
- **Don't let `MorselId` cross the wire.** ADR 003 G14
  enforces. Worker-local scheduling detail only.
- **Don't introduce a new IR level below `ExecutionPlan`.**
  `TaskSpec` wraps; it doesn't transform.
- **Don't conflate task-lifecycle failures with partition-
  store wire errors.** `TaskFailureKind` is 7 variants;
  `PartitionMissing` is a wire response carrying
  `PartitionMissingReason`, not a `TaskFailed` variant.
- **Don't start Stage N+1 tasks before Stage N manifests
  are accepted.** ADR 003 G13 enforces strict barrier.
- **Don't promise byte-identical Arrow output without
  canonical normalization.** ADR 002 §"Resolved Q4" concat-
  and-reslice canonicalizes batch boundaries; ADR 003 §13
  adds a stable full-row lexicographic sort on top because
  concat-and-reslice alone does not survive distributed
  hash-aggregation / partition-arrival / map-iteration
  nondeterminism. ADR 002 callsites pass `SortPolicy::None`;
  ADR 003 distributed callsites pass `SortPolicy::FullRowLex`.
- **Don't require stable worker assignment for
  determinism.** ADR 003 output parity is the contract;
  assignment may vary across runs.
- **Don't promote a kernel without a bench-evidence
  artifact.** Wendelon ADR 070 pattern — narrow ADR + bench
  results + roadmap update, every time.
- **Don't open the next ADR with a mega-scope.** One contract
  surface per ADR; bundling defers real decisions.
- **Don't promote stabilization watch items into roadmap
  items.** Observability metrics, architectural-test guards,
  post-merge dashboards belong in runbooks and impl plans.
  Decision-enabling measurement belongs inside the owning
  ADR.
