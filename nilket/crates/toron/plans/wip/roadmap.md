# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not at
implementation commits. The council operates on a block-chain
rhythm: plan → implement → plan → implement, and this file is
a snapshot of the most recent plan. Between sessions the
roadmap lags the code on purpose — an item can read "ADR N
implementation phases pending" while several phases have
already landed on `main`. That is expected. Check `git log`
and the relevant impl plan / bench-results file for actual
status, and roll roadmap edits into the next planning session
along with the forward-sequence rewrite.

## Status

- **ADR 001 — Foundation and Engine Contract.** Promoted +
  implemented.
- **ADR 002 — Storage-Backed Execution Contract.** Promoted +
  implemented. Bench evidence at
  `plans/impl/002_storage_execution_contract_bench_results.md`.
- **ADR 003 — Distributed Runtime and Data Movement
  Contract.** Promoted + implemented. Bench evidence at
  `plans/impl/003_distributed_runtime_bench_results.md`.
- **ADR 004 — Distributed Inner Equi-Join Contract.**
  Promoted + implemented. Bench evidence at
  `plans/impl/004_distributed_inner_equi_join_bench_results.md`.

V1 target is **"First Query with Integrity"** plus
**"Distributed Execution Correctness"**: correct analytical
results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage
boundary, multi-process coordinator/worker mechanics
surviving a worker SIGKILL with byte-identical normalized
output, and the first two-input relational operator working
across hash-partitioned and bounded broadcast strategies.
Multi-tenant governance, executor model upgrade, outer
joins, sort-merge, and pipelined cross-stage execution
remain post-V1.

## Prioritization philosophy

Toron is the compute ring of the Nilket stack; Wendelon is
the storage ring. We control both ends. The boundary exists
for scaling dynamics and resource isolation, not because
storage and compute are independent products.

**Toron's reason for existence is smart-storage pushdown.**
ADR 002 made that load-bearing in the runtime path. ADR 003
made the cross-process mechanics real. ADR 004 added the
first two-input operator and finally exercises the buffered
executor under full-cardinality probe-side pressure. Every
other improvement (executor model upgrade, advanced kernels,
frontend hardening) polishes a system that does its primary
job along its primary axis.

**ADR-by-ADR cadence.** Wendelon ADR 070 pattern: narrow,
evidence-driven, single-decision contracts. ADR 003 shipped
runtime mechanics only; ADR 004 ships inner equi-join only
(no outer joins, no sort-merge, no filter pushdown). Each
narrow ADR earns its scope from the prior ADR's bench
evidence.

**Evidence drives scope.** The most informative artifact for
the next ADR is the previous ADR's bench results. ADR 004's
10M × 10M `INNER JOIN` benchmark at 8 workers will tell us
whether the buffered executor is the remaining V1 bottleneck
(promoting executor model upgrade), whether large-build
spill engagement is the throughput floor (promoting a
focused spill-policy ADR), or whether neither — at which
point ADR 005 attacks outer joins, runtime-filter pushdown,
or multi-tenant governance based on launch-timing pressure.

## Forward sequence

Earlier items have firmer scope; later items are direction,
not commitment.

### Next: ADR 005 planning

ADR 004's promotion makes the first two-input relational
operator available alongside the ADR 003 substrate. ADR 005
should choose the next contract surface from the bench
evidence and the remaining V1 launch gates.

Recorded ADR 004 evidence: 10M × 10M hash-partitioned join
completed at 8 workers, broadcast and hash digests match on
the 1M × 10M comparison fixture, and 10M × 1M worker scaling
missed the soft target. That keeps executor model /
scheduling overhead as the leading ADR 005 candidate unless
launch pressure forces a narrower surface first.

The five live candidates, in rough order of probable
selection given evidence shapes the council can already
anticipate:

#### Candidate A — Executor model upgrade

Promotes if ADR 004's 10M × 10M join bench shows the
buffered executor groans at full-cardinality probe-side
throughput. Streaming end-to-end execution, morsel
parallelism, push-based execution. Lifts every workload
that doesn't have pushdown to fall back on; particularly
joins, complex aggregates, and any future operator that
materializes full-cardinality intermediate state.

The bench evidence question: does probe-side wall-clock
scale linearly with worker count up to 8 workers, or does
it plateau? Plateau = buffered executor is the bottleneck =
executor model is ADR 005. Linear scaling = something else.

#### Candidate B — Bloom / runtime filter pushdown into Wendelon

The cross-ring contract that completes Toron's "smart-
storage pushdown" thesis for joins. Build-side stage
produces a Bloom filter (or exact `InList` for small build
sets) from observed keys; coordinator attaches the filter
to the probe-side `ScanRequest` (ADR 002 surface); Wendelon
applies the filter at the storage layer (row-group skip or
row-level reject) before bytes hit the wire.

Shape from council topics:

- Worker build → coordinator collect → Wendelon attach via
  PBC.
- False-positive tolerance: best-effort selectivity (join
  remains responsible for correctness).
- Hash seed + encoding stability for cross-replica
  determinism.
- Byte budget per filter; descriptor-fingerprint
  interaction.
- Behavior when filter cannot be pushed into Wendelon (V1.x
  fallback: filter ignored, join still correct).

Promotes if ADR 004 join bench shows selective joins are
shuffle-bytes-dominated and filter-side reduction would
help. Requires cross-ring contract surface with Wendelon
(its own ADR scope).

#### Candidate C — Outer / semi / anti joins

Pins null-fill semantics for preserved sides under
`LeftOuter` / `RightOuter` / `FullOuter` plus existence-
based filtering for `Semi` / `Anti`. Substrate exists from
ADR 004; the contract decision is null semantics and output
schema nullability adjustments.

Bench evidence question: does V1 launch require outer joins?
Most analytical SQL workloads use them (LEFT OUTER on
missing dimension lookups, FULL OUTER for diff reports). If
launch timing forces this before executor or filter
pushdown, ADR 005 promotes outer joins.

#### Candidate D — Multi-tenant resource governance

Per-query → per-tenant budgets, token-bucket admission,
scheduler-slot allocation, memory-rejection vs spill-trigger
thresholds. Promotes if V1 launch pressure surfaces multi-
tenancy before joins-adjacent perf work.

ADR 003's per-query `QueryBudget` is the natural antecedent;
the per-tenant layer wants real multi-tenant workload
evidence before its shape gets pinned.

#### Candidate E — Rune sandbox enforcement

V1 launch prerequisite. Fuel is currently disabled at
`rune_engine.rs:188`. Native-function allow-list, per-tenant
CPU + memory budgets, no-filesystem / no-network gating.
Self-contained — doesn't depend on join shape, executor
shape, or governance shape.

Could ship as a parallel narrow ADR alongside ADR 005 if
launch timing demands; otherwise sequenced ahead when V1
launch becomes load-bearing.

### Advanced kernel graduations *(one ADR per kernel)*

Wendelon ADR 070 narrow-decision pattern. Each promotes
under its own ADR with bench evidence. Inherited from
prior roadmap, unchanged:

- **`LinearRegress` stable contract.** Numerical hardening
  (condition-number guard, centered formula, rank check)
  + stability across numeric types.
- **`ApproxQuantile` stable contract.** Real rank-based
  estimation against KLL retained values + cross-impl
  parity with Wendelon's KLL substrate.
- **`TimeBucket` stable contract.** Bucket-alignment
  semantics across timezone, duration, and pre-epoch
  inputs.

### Brainstorms and high-level vision

Long-horizon shapes the council has noted but not
committed.

- **Brainstorm 001: Pure Cranelift JIT as a First-Class
  Query Specializer.** JIT-compile hot kernels (predicates,
  UDFs, hash-join probe loops) into native machine code for
  zero-copy, bare-metal performance. Reserved for post-V1
  deliberation when bench evidence quantifies the
  interpretation overhead.
- **Brainstorm 002: WeakControlRing.** In-memory Raft ring
  for HA coordinators, cluster-wide quotas, and shuffle
  barrier synchronization without the persistence tax of
  NVMe / WAL. ADR 003's `WorkerRegistry` trait +
  `WorkerLease` / `LeaseEpoch` fields are the seam.
- **Brainstorm 003: Smart-Storage Pushdown beyond
  filters.** Toron's pushdown story currently covers
  filters (ADR 002) and shuffle-key affinity (ADR 003).
  Future pushdown surfaces: aggregate partial pushdown
  (already half-Wendelon-bound), join-runtime-filter
  pushdown (ADR 005 Candidate B), top-K / sort
  pushdown, projection-with-computed-column pushdown.
  Each is its own ADR; the cumulative shape is "Toron
  binds; Wendelon executes the leaves; the wire carries
  results, not bytes."

### V1.x items *(post-V1, evidence-driven)*

Items the council has explicitly deferred from V1 ADRs.
Promoted to their own ADRs when evidence drives them.

- **Outer / semi / anti joins.** ADR 004 deferred; own
  ADR pins preserved-side null-fill semantics.
- **Non-equi joins / theta-joins.** Own ADR.
- **Sort-merge join.** Requires ordering contract (no
  ORDER BY semantics pinned in Toron yet).
- **Cross-join.** Own ADR or V1.x SQL surface.
- **Cost-based build/probe swap; cost-based strategy
  selection.** Requires Wendelon stats surface and Toron
  cost model. Will land `BuildSide` field on
  `ExecutionPlan::Join` when it engages.
- **Multi-way join planning.** Requires cost model.
- **Skew detection + mitigation.** ADR 004 ships
  observation only (soft gate). Mitigation needs heavy-
  hitter detection, salt-and-replicate, dynamic
  partition-count override. Own ADR.
- **SQL join hint syntax.** `/*+ BROADCAST(t) */` and
  similar. V1.x SQL surface.
- **`USING` clause; comma-join; `CROSS JOIN`.** V1.x SQL
  sugar.
- **Floating-point join keys.** Requires canonical-
  encoding ADR (NaN, signed zero, subnormals).
- **Intra-task build / probe pipelining.** V1.x executor
  work; depends on Candidate A outcome.
- **Hash-spill-to-memory demotion.** ADR 004 ships one-way
  spill; demotion is V1.x when bench shows churn benefits.
- **SQL-surface auto-rename of duplicate output column
  names.** ADR 004 requires explicit qualification.
- **Pipelined cross-stage execution.** ADR 003 ships
  strict stage barriers; pipelining adds latency wins at
  the cost of failure-recovery complexity. V1.x when the
  failure-recovery story is concrete.
- **Locality-aware fragment routing.** ADR 003 +
  ADR 004 are location-agnostic; locality requires new
  Wendelon public-API surface.
- **RDMA / RoCEv2 / SIW transport tier.** M9 seeds the
  transport-tier-negotiation shape; QUIC preferred
  through V1.
- **Operator-internal streaming end-to-end.** ADR 003
  requires stream-shaped task output; full operator-
  internal streaming is V1.x executor work (Candidate A
  territory).
- **Higher retry counts.** ADR 003 ships `max_attempts =
  2`; future ADR raises with evidence of which failure
  modes benefit.
- **Encrypted spill governance.** Spill lifecycle beyond
  M10 seed: key rotation, AAD policy, crash recovery,
  observability, multi-query isolation. ADR 003 +
  ADR 004 share the M10 AES-256-GCM-SIV substrate without
  key rotation.
- **Full DDL propagation.** Async invalidation broadcasts,
  multi-coordinator schema consensus, descriptor
  versioning beyond fingerprint-pin.
- **Wire-protocol cross-version compat.** ADR 003 ships
  reject-on-mismatch; rolling upgrades are V1.x.
- **Decimal numeric types.** Their own ADR; pins decimal-
  specific aggregate semantics.
- **Wendelon-side crate extraction.** `wendelon-types`
  (PBC ops, identity, fingerprint), `wendelon-net`
  (transport facade), `wendelon-schema` (descriptor
  types). Defer until multi-consumer pressure (Polariton,
  Baryon, external Toron SDK) materializes.

### V2+ items

Direction, not near-term plan.

- **Replicated / disaggregated shuffle service.** ADR 003
  per-worker partition store is the V1 substrate; a
  standalone service is V2 when evidence shows producer-
  hosted partitions are the next bottleneck.
- **Direct P2P shuffle as fast path.** V2 optimization
  over the V1 pull-addressed contract.
- **Hybrid vector search.** Vector ANN with metadata
  pushdown into Wendelon. Waits for Wendelon vector-aware
  placement.
- **Visualization-as-compute IR operators.** Density
  tiles, 2D binning, quantile-per-pixel. Waits for
  Polariton's tile protocol.
- **External federation** (Postgres / S3 / BigQuery
  adapters). Explicit ADR 001 §"Deferrals" item — "one
  external adapter as a capability proof," never on the
  Wendelon-native hot path.
- **Materialized views; incremental view maintenance.**
  Requires the executor model to support both batch and
  incremental shapes; far horizon.

### Refinement plumbing *(opportunistic, no ordering)*

Small improvements that don't fit a single ADR.

- **Capability-table aggregate dispatch** replacing the
  enum (when the aggregate catalog grows beyond hand-
  listable).
- **Newtype hygiene pass** — `Symbol` / `ColumnId` instead
  of bare `String` for column refs in narrower contexts.
- **PBC catalog growth** — additional ops promoted from
  experimental to stable.
- **ADR file renaming pass** — drop the redundant
  `toron_` prefix from `plans/adr/001-003` and
  `plans/impl/001-003` filenames (ADR 004's filename
  already follows the cleaner pattern). Mechanical sed +
  cross-reference update.

## Backlog (parked, may not return)

Ideas explored across iteration rounds in `plans/wip/`
that did not earn forward placement. Kept here as a
record of what the council considered and let go. Items
labeled by ADR cycle.

### ADR 004 cycle

- **Hash-only V1.** Considered as a narrow scope (Claude
  iter-3); rejected because star-schema workloads waste
  shuffle bytes on the small-dim side without broadcast.
  Bounded broadcast with three caps is cheap enough to
  land alongside.
- **Unbounded broadcast.** Replication-storm risk;
  rejected. The three-cap regime is non-negotiable.
- **Right = build (V1 default).** Considered as
  convention-matching with most SQL engines (Claude iter-2,
  Codex iter-3); rejected for consistency with the visual
  order of the `LogicalPlan::Join.left` field. Build =
  left across both strategies.
- **Cost-based strategy selection in V1.** Considered as
  ideal default (Gemini iter-1); rejected because Toron
  has no cost model. Programmatic strategy selection is
  V1's contract; cost-based engagement is V1.x.
- **Runtime filter (exact PBC `InList`) as ADR 004 soft
  gate.** Codex iter-1 / iter-3 middle path; rejected
  because filter-collection plumbing on the coordinator
  is its own optimization surface deserving its own ADR.
- **Bloom filter pushdown as ADR 004 hard gate.** Gemini
  iter-2 framing; rejected for the same reason —
  cross-ring contract surface deserves its own ADR.
- **Operator-state hash spill deferred to V1.x.** Codex
  iter-1 / Claude iter-2 position; rejected because V1's
  "First Query with Integrity" requires realistic large-
  build joins to succeed gracefully. Encrypted spill in
  scope.
- **Reject duplicate output column names at the binder.**
  Codex iter-2 position; rejected because it conflates
  SQL-surface naming with bound-IR ordinal semantics.
  SQL planner is the right layer for naming discipline.
- **`JoinSide { Left, Right }` tag on `ResolvedColumn`.**
  Gemini iter-3 alternative; rejected because side is
  structural (encoded in `left_keys` / `right_keys`
  field names), not metadata.
- **Separate `ExecutionPlan::HashJoin` and
  `BroadcastJoin` variants.** Claude iter-3 position
  (before broadcast came back into scope); rejected for
  `Join { strategy }` field-discriminated which scales
  cleanly to V1.x.
- **`BuildSide` enum field on `Join` in V1.** Considered;
  rejected because it would be a single-value
  `BuildSide::Left` reserved for V1.x. Convention
  forbids reservation.
- **Streaming-spill probe-side as alternative to hash-
  table spill.** Gemini topics Topic 8 alternative
  ("streaming join that spills the probe side if the
  build side fits in RAM"); rejected because probe-side
  is naturally streaming (no buffering needed) — the
  build side is the operator-local memory hot spot.
- **Bundling ADR 003 telemetry residual into ADR 004
  Phase 0.** Gemini iter-2 framing; rejected for commit
  boundary cleanliness — remains a separate `chore:` slice
  outside ADR 004 promotion.
- **Inline broadcast payload in `StartTask`.** Considered
  as zero-extra-hop optimization; rejected because it
  conflicts with ADR 003 pull-addressing and would
  introduce a wire-message-size-dependent dispatch path.
- **Auto-rename duplicate output columns at the binder
  (e.g., `lhs_x` / `rhs_x`).** Considered as
  caller-friendly default; rejected for V1 explicit-
  qualification discipline.

### ADR 003 cycle (preserved from prior roadmap)

- **New IR level below `ExecutionPlan`** (`PhysicalFragment`
  vocabulary). Considered; rejected because `TaskSpec`
  wraps the bound plan without adding translation surface.
- **Pure spill-to-pull as V1 default shuffle.** Considered
  for max resilience; rejected because disk-write +
  decrypt latency on every shuffle is unacceptable for
  in-memory-sized queries.
- **Transport-native flow control alone as backpressure
  contract.** Considered; rejected because wire-level
  flow control can't bound application-level memory or
  shuffle bytes.
- **Schema-registry hardening as ADR 003 alternative.**
  Considered; rejected for lack of high-frequency DDL
  pressure in ADR 002 bench evidence.
- **Stable worker assignment as the determinism gate.**
  Considered; rejected because worker pools are dynamic
  and assignment parity isn't a useful user-facing
  contract.

## What Not To Do Next

Enduring guardrails for upcoming work. Items tied to
specific shipped ADRs live in those ADRs' §"What Not To Do"
sections; this list is the cross-cutting set.

- **Don't adapt DataFusion / Ballista.** ADR 001 rejected;
  ADR 002 / 003 reinforced. The greenfield decision is
  final unless a future ADR cites concrete evidence
  reversing the surface-area-mismatch argument.
- **Don't bundle scope across contract surfaces.**
  Distributed joins, executor model upgrade, runtime
  filter pushdown, outer joins, multi-tenant governance,
  and Rune sandbox enforcement are each their own ADR.
  ADR 003 was intentionally narrow; ADR 004 inherits the
  discipline; ADR 005 stays narrow.
- **Don't ungate experimental kernels by default.** ADR
  002 requires `PlannerConfig::allow_experimental_kernels
  = false`. Each kernel graduates under its own narrow
  ADR.
- **Don't put `Result<_, String>` in any public or module-
  crossing signature.** ADR 002 G3 enforces; ADR 003 G9
  enforces for distributed surface; ADR 004 inherits.
- **Don't skip descriptor-fingerprint validation on PBC
  pushdown.** Silently-wrong-results is the failure mode
  ADR 002 paid complexity to prevent.
- **Don't extract Wendelon types into a shared crate
  before V1.x consumer pressure.** ADR 001 / 002 / 003
  import allowlist holds.
- **Don't bypass the Wendelon import allowlist.**
  Production code consumes `wendelon::schema::{pbc, *types}`,
  `wendelon_client::*`, `wendelon::network::pool::*`,
  `wendelon::storage::file_ops::*`. Forbidden:
  `wendelon::network::raft::*`, `wendelon::network::quic::*`,
  `wendelon::storage::lsm::*`, ad-hoc page readers,
  anything inside Wendelon's node / raft / transport
  internals.
- **Don't query Wendelon for ring placement.** ADR 003
  V1 is location-agnostic; ADR 004 inherits. Locality
  routing is V1.x.
- **Don't initiate P2P-push from producer to consumer.**
  ADR 003 G11 enforces. Consumers always pull via
  `PullPartition`.
- **Don't use `tokio::time::sleep` for backpressure** in
  the production worker path. ADR 003 G10 enforces. The
  per-query budget envelope is the contract.
- **Don't dispatch `LogicalPlan` to workers.** ADR 003
  G12 enforces. Coordinator binds; workers receive
  `TaskSpec`.
- **Don't let `MorselId` cross the wire.** ADR 003 G14
  enforces. Worker-local scheduling detail only.
- **Don't introduce a new IR level below `ExecutionPlan`.**
  `TaskSpec` wraps; it doesn't transform.
- **Don't conflate task-lifecycle failures with partition-
  store wire errors.** `TaskFailureKind` is 7 variants;
  `PartitionMissing` is a wire response carrying
  `PartitionMissingReason`, not a `TaskFailed` variant.
- **Don't start Stage N+1 tasks before Stage N manifests
  are accepted.** ADR 003 G13 enforces strict barrier;
  ADR 004 G18 generalizes for multi-upstream.
- **Don't promise byte-identical Arrow output without
  canonical normalization.** ADR 003 §13 normalization
  (concat → full-row sort → reslice → digest) is the
  contract; ADR 004 reuses unchanged.
- **Don't require stable worker assignment for
  determinism.** ADR 003 output parity is the contract.
- **Don't promote a kernel without a bench-evidence
  artifact.** Wendelon ADR 070 pattern — narrow ADR +
  bench results + roadmap update, every time.
- **Don't open the next ADR with a mega-scope.** One
  contract surface per ADR; bundling defers real
  decisions.
- **Don't promote stabilization watch items into roadmap
  items.** Observability metrics, architectural-test
  guards, post-merge dashboards belong in runbooks and
  impl plans. Decision-enabling measurement belongs
  inside the owning ADR.
- **Don't introduce non-pinned join algorithms.** ADR 004
  G16 enforces. Sort-merge, nested-loop, broadcast-right,
  cross join, semi join, anti join, outer join — each is
  its own future ADR.
- **Don't reject duplicate output column names at the
  binder.** ADR 004 architectural division puts naming
  in the SQL planner, ordinals in the bound IR.
- **Don't synthesize `Exchange` in the fragmenter.** The
  planner emits explicit `Exchange` above each join /
  aggregate input; the fragmenter never adds operators.
- **Don't allow broadcast to bypass caps.** ADR 004
  pinned three caps; over-cap fails terminal; no silent
  strategy switch.
- **Don't reserve placeholder fields, message slots, or
  PBC ops for deferred features.** Convention
  `no-premature-backward-compat.md`. The future ADR adds
  the surface when it pins the contract.
- **Don't bundle ADR 003 telemetry residual into ADR 004
  feature commits.** Separate `chore:` slice (ADR 004
  council decision 19).
