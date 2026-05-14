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
- **ADR 005 — Hash-Join Build Path Efficiency.** Promoted +
  implemented. Bench evidence at
  `plans/impl/005_hash_join_build_path_efficiency_bench_results.md`.
  The 8-worker 10M x 1M rerun moved `build_ms`
  `2581.550 ms -> 542.787 ms` with digest equality preserved.
  `framework_overhead_ms` moved proportionally, placing ADR 006 in
  the operator-adjacent band rather than runtime-plumbing
  investigation.
- **ADR 006 — Runtime Filter Pushdown.** Promoted; Toron-side
  implementation complete. Bench evidence at
  `plans/impl/006_runtime_filter_pushdown_bench_results.md`.
  Cross-ring optimization contract: advisory Bloom runtime
  filters for hash-partitioned inner equi-joins via two-wave
  execution. Pinned by ADR 005's post-promotion telemetry —
  the bytes-movement cluster (`batch_send` +
  `shuffle_pull_decode` + `output_publish` + `batch_encode` =
  558ms, 34.1%) overtook build as the largest bucket. ADR 006
  attacks the cluster at the source (Wendelon filters probe
  rows before they hit the wire) rather than moving the same
  bytes faster. Advisory invariant makes split-shipment safe:
  Toron-side evidence is captured; production Wendelon-enabled
  end-to-end evidence lands in a bench-results update once
  Wendelon-side application ships.
- **ADR 007 — Rune Sandbox Enforcement.** Promoted +
  implemented. Process-internal safety contract:
  closes the M7-recorded launch blocker (sandbox fuel disabled,
  `Context::with_default_modules()` exposing filesystem /
  network / process / environment / time / random capability)
  with `RuneSandboxConfig` carrying fuel + memory + wall-timeout
  + source-size + allow-profile, single
  `FrontendError::RuneSandbox` variant carrying a 7-label
  `RuneSandboxViolation` enum, allow-list (not deny-list)
  context construction, and conditional architecture guard for
  `Context::with_default_modules` absence. Memory enforcement
  is V1 hard requirement (Rune-native cap or helper-process
  isolation per Phase 0). Implemented with Rune 0.13
  `budget::with` + `alloc::limit::with`, `max_source_bytes`,
  yielding-path `wall_timeout`, allow-list context construction,
  and non-retryable `RuneSandbox::<label>` task-failure mapping
  if sandbox errors cross the task boundary. Selected via the iter-3 ADR 007
  topics convergence on Codex's four-path selection rule
  ("V1 exposes user-supplied Rune -> L-2 Rune sandbox").
- **ADR 008 — Outer / Semi / Anti Joins.** Promoted +
  implemented. Operator-level surface extension
  closing the analytical-SQL "join width gap" left by ADR 004
  (inner-only). `JoinKind` extends from 1 variant to 8 (`Inner`
  plus `LeftOuter` / `RightOuter` / `FullOuter` / `LeftSemi` /
  `RightSemi` / `LeftAnti` / `RightAnti`), null-fill via Arrow
  native `NullBuffer`, output-schema nullability promoted at
  bind time, hash-partitioned-only distributed execution for
  non-Inner kinds (binder rejects `Broadcast` for
  `kind != Inner`; fragmenter has belt-and-braces guard),
  runtime-filter eligibility stays Inner-only (ADR 006
  invariant preserved), single `JoinOutputRow` row-selection
  intermediate with shared `materialize_join_selection` helper,
  spill semantics match in-memory for every kind. No new
  `TaskFailureKind` / `MessageKind` / `JoinStrategy` variant;
  `LogicalPlan::Join` and `ExecutionPlan::Join` stay
  single-variant. Mixed old/new binary support explicitly not
  promised — single workspace bump, full-fleet upgrade.
  Three-way AI Swarm Council convergence (Claude / Codex /
  Gemini) at iter-3. Bench evidence landed at
  `plans/impl/008_outer_semi_anti_joins_bench_results.md`.

V1 target is **"First Query with Integrity"** plus
**"Distributed Execution Correctness"**: correct analytical
results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage
boundary, multi-process coordinator/worker mechanics
surviving a worker SIGKILL with byte-identical normalized
output, and the first two-input relational operator working
across hash-partitioned and bounded broadcast strategies.
Multi-tenant governance, executor model upgrade, sort-merge,
and pipelined cross-stage execution
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
the next ADR is the previous ADR's bench results. ADR 005's
post-promotion telemetry showed `build_ms` fell to 33.1% of
wall while the bytes-movement cluster (`batch_send` +
`shuffle_pull_decode` + `output_publish` + `batch_encode`)
rose to 34.1% — overtaking build as the largest bucket. The
user pinned **Evidence Pressure > Launch Pressure** for ADR
006, selecting Topic A (runtime filter pushdown) over
launch-driven candidates. ADR 006 attacks the bytes-movement
cluster at the source via Bloom runtime filters and two-wave
hash-partitioned execution. The conditional ADR 005 rule
already resolved the residual to operator-adjacent — no
runtime-plumbing investigation is on the table. ADR 007 (Rune
sandbox enforcement) was promoted independently of ADR 006's
bench outcome — it closes a launch-blocker on the authoring
surface that the bytes-movement evidence did not touch. ADR 008
(outer / semi / anti joins) was selected as the strongest
launch-driven candidate post-ADR-007 — the "SQL width" gap
left by ADR 004's inner-only scope was load-bearing for
analytical workloads. ADR 009 selection is the next decision:
the ADR 006 selective bench rerun and any launch-pressure
shift feed into that scoring, and the candidate ordering
below reflects the post-ADR-008 landscape.

**Scale-boundary convention (adopted iter-5 of ADR 006).**
Every ADR from ADR 006 forward includes an explicit
**Scale boundary** structural section after Status and before
Context. Scale boundaries seen so far: process-internal
(ADR 007 Rune sandbox), operator-level (ADR 004, ADR 005,
ADR 008), cluster-wide (governance, ADR 003), cross-ring
(ADR 002, ADR 006), transport (ADR 003 wire, ADR 006
`PartitionManifest`/`ScanRequest` additions),
storage-internal (Wendelon). The decomposition makes
scope-creep into adjacent boundaries visible at review
time and sharpens ADR selection: same operator at different
scales is a different contract.

## Forward sequence

Earlier items have firmer scope; later items are direction,
not commitment.

### Next: ADR 009 planning

ADR 008's contract is pinned at
`plans/adr/008_outer_semi_anti_joins.md`; the impl plan lives at
`plans/impl/008_outer_semi_anti_joins.md`; bench evidence lives at
`plans/impl/008_outer_semi_anti_joins_bench_results.md`. The next
council deliverable is ADR 009 planning.

In parallel, the ADR 006 Wendelon-side row-level application
remains pending — when Wendelon-side ships, the
`plans/impl/006_runtime_filter_pushdown_bench_results.md`
artifact gets the production end-to-end row appended as a
follow-up update.

ADR 009 selection now waits on the ADR 006 Wendelon row arrival
and any follow-up evidence from ADR 008's per-kind quick-row
matrix. Until those land, the candidate ordering below reflects
the post-ADR-008 landscape.

### ADR 009 candidates (post-ADR-008 reordered)

With L-2 (Rune sandbox) consumed by ADR 007 and operator-level
SQL width consumed by ADR 008, the remaining candidates rotate.
The candidates, in rough order of probable selection given
evidence shapes the council can already anticipate:

#### Candidate A — Multi-tenant resource governance

**Scale boundary:** cluster-wide.

Per-query to per-tenant budgets, token-bucket admission,
scheduler-slot allocation, memory-rejection vs spill-trigger
thresholds. Promotes if V1 launch pressure surfaces
multi-tenancy before joins-adjacent perf work.

ADR 003's per-query `QueryBudget` is the natural antecedent;
the per-tenant layer wants real multi-tenant workload evidence
before its shape gets pinned. ADR 007 explicitly deferred
per-tenant Rune budgets to this work (L-3 from the topics
cycle).

#### Candidate B — Cross-process wire efficiency

**Scale boundary:** transport.

The compute-ring fallback if ADR 006's Wendelon-enabled
selective bench misses or non-selective workloads dominate
post-V1. Batch-size policy, Arrow IPC encode/decode tuning,
`WireEnvelope` overhead audit, output-publish path streamlining.
No cross-ring dependency.

Promotes if ADR 006's selective bench misses its target or if
non-selective workloads dominate the V1 launch shape. Lower
leverage than ADR 006 on selective joins (moves bytes faster vs
avoids moving rejected bytes), but uniform across selective and
non-selective workloads. Phase 0 sub-bucket selection discipline
preserved if C promotes (no grab-bag wire-perf ADR).

#### Candidate C — V1.x runtime-filter extensions

**Scale boundary:** cross-ring (continuation of ADR 006).

A cluster of follow-on work to ADR 006, **gated on production
Wendelon-side row landing**:

- **Exact `InList` filters** for small build sets. Qualitatively
  precise (zero false positives), useful for small dimension
  joins. Extends the ADR 006 descriptor contract. Codex iter-1
  topics decision-matrix candidate when Wendelon row hits ≥50%
  with material FP residual.
- **Broadcast-strategy runtime filters.** ADR 006 V1 limits
  filter eligibility to hash-partitioned joins. Broadcast
  filters reuse the descriptor contract with a different
  attachment path.
- **Outer / semi / anti runtime-filter pushdown.** ADR 008
  reserved this — runtime filters on outer / anti probe sides
  would incorrectly drop rows the outer-preservation semantics
  require. A "filter + null-pad" combined operator is the
  separable design.
- **Planner cost-model opt-in.** Always-two-wave in V1;
  planner-stats opt-in once stats exist.
- **Filter eligibility extension to canonical-encoder
  additions.** When the canonical encoder grows support for
  `Date`, `Timestamp`, or `Decimal`, filter eligibility extends
  with it.

Each sub-item is plausibly its own narrow ADR. Held as topic
cards, not selected, until the production Wendelon row lands.

#### Candidate D — Worker-side Rune UDF execution

**Scale boundary:** operator-level + process-internal
(distributed sandbox).

Reserved. ADR 007 explicitly scoped row-level Rune UDFs out.
This is the substantial new ADR if a future feature needs
worker-side script execution — covers distributed sandbox +
per-worker budget interaction + the `TaskFailureKind`
reason-string mapping ADR 007 reserved as the forward seam.

#### Candidate E — `EXISTS` / `NOT EXISTS` subquery decorrelation

**Scale boundary:** frontend planner.

Reserved. ADR 008 ships the `Semi` / `Anti` operators; this
candidate adds the subquery-planner contract that detects
correlated-equi-join `EXISTS` / `NOT EXISTS` patterns and
rewrites them to the `Semi` / `Anti` operators. Substrate exists
from ADR 008; the decision matrix is which `EXISTS` shapes get
decorrelated, how the planner detects them, and how the binder
folds them. Likely follows ADR 008 implementation evidence.

#### Candidate F — Executor model upgrade

**Scale boundary:** operator-level (executor substrate).

Demoted by ADR 004 sub-attribution and re-confirmed by ADR 005
+ ADR 006 (probe + output combined < 15% of wall on selective
workloads; even smaller post-filter). Reserved for the case
where a future workload (large probe-side without filtering,
multi-way joins, complex aggregates) shows probe-side
dominance. Stays demoted.

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
  filters.** Toron's pushdown story now covers static
  predicates (ADR 002), shuffle-key affinity (ADR 003),
  and *runtime* join-key filtering (ADR 006). Future
  pushdown surfaces: aggregate partial pushdown (already
  half-Wendelon-bound), top-K / sort pushdown,
  projection-with-computed-column pushdown. Each is its
  own ADR; the cumulative shape is "Toron binds; Wendelon
  executes the leaves; the wire carries results, not
  bytes." ADR 006 is the first runtime-pushdown ADR and
  the substrate that V1.x runtime-filter extensions
  (Candidate E above) build on.

### V1.x items *(post-V1, evidence-driven)*

Items the council has explicitly deferred from V1 ADRs.
Promoted to their own ADRs when evidence drives them.

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
  work; depends on Candidate E (executor model upgrade)
  outcome.
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
  internal streaming is V1.x executor work (Candidate E —
  executor model upgrade — territory).
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

### ADR 008 cycle

- **Six-variant `JoinKind` (no `RightSemi` / `RightAnti`).**
  Considered (Claude iter-0); rejected. `sqlparser` 0.54 exposes
  both directions natively and the executor's build-left /
  probe-right machinery supports both with identical
  infrastructure (`Right*` probe-time-decided; `Left*` after-pass
  on build-side bitmap). Excluding right-side would fragment the
  SQL surface without simplifying the implementation. Codex's
  symmetry argument was load-bearing.
- **Broadcast rewrite for non-Inner kinds** (sides-swap
  `RightOuter` → `LeftOuter`, fallback `FullOuter` →
  `HashPartitioned`). Considered (Claude iter-0); rejected.
  Strategy-dependent semantics blur the operator-level scale
  boundary — the binder would accept `Broadcast` but the planner
  would rewrite it, making the "what's the strategy" question
  ambiguous. Cleaner contract: binder rejects `Broadcast` for
  `kind != Inner`; SQL planner already defaults to
  `HashPartitioned` for non-Inner kinds. A strategy-rewrite ADR
  can follow if evidence supports it.
- **`EXISTS` / `NOT EXISTS` decorrelation to `Semi` / `Anti`.**
  Considered (Claude iter-0, Gemini iter-1); rejected for ADR
  008 scope. Subquery-planner contract requires its own decision
  matrix: which patterns are decorrelated, how does the planner
  detect them, how does the binder fold them. ADR 008 ships the
  `Semi` / `Anti` operators; a later subquery-planner ADR
  (Candidate E above) can leverage them.
- **Per-kind probe loops without a row-selection intermediate.**
  Considered (Claude iter-0); rejected in favor of Codex's
  `JoinOutputRow` design. Per-kind loops duplicate null-fill code
  across paths and make the in-memory / spilled selection logic
  divergent (each kind reasoning about emission separately).
  Centralizing on `JoinOutputRow` means one selection/materializer
  contract — the spilled path emits the same selection
  representation after lookup (no spill-format change; ADR 005
  build-side-hash-page format is reused unchanged).
- **Sentinel-based null fill.** Considered as performance
  alternative; rejected. Breaks the Arrow contract, requires
  per-type sentinel choices, couples to downstream readers, and
  hidden semantics make outer-join output not interchangeable
  with inner-join output. Arrow's native `NullBuffer` is the
  canonical representation.
- **Loosen runtime-filter eligibility to outer / semi / anti.**
  Considered; rejected for ADR 008. A Bloom filter on the build
  side of a left-outer join would eliminate probe rows that
  don't match the build — but those are exactly the rows the
  outer join must preserve with null fill. `RightAnti` depends
  on emitting unmatched probe rows. A "filter + preserve"
  combined operator is the separable design; reserved as ADR 009
  Candidate C (V1.x runtime-filter extensions).
- **New `TaskFailureKind::JoinSemantics` variant.** Considered;
  rejected. No new failure class exists — the eight join kinds
  use existing build/probe/spill machinery and the existing
  budget envelope. Invalid plans surface as `FrontendError`;
  runtime resource failures map to existing
  `BudgetExhausted` / `TaskFailed`. ADR 003 7-variant guard
  stays load-bearing.
- **Second `ExecutionPlan::Join` or `LogicalPlan::Join` physical
  variant** (e.g., `ExecutionPlan::SemiJoin`). Considered;
  rejected. Per-kind physical variants duplicate the operator
  shape (two inputs, keys, strategy, schema) and weaken ADR
  004's single-variant architecture guard. The semantic
  difference belongs in `JoinKind`. `LogicalPlan::Join`
  single-variant guard added in Phase 1E pairs with the existing
  `ExecutionPlan::Join` guard.
- **Rolling coordinator/worker mixed-version semantics for
  `JoinKind`.** Considered as forward-compat affordance;
  rejected. Externally-tagged serde adds variants as new strings
  on the wire; older binaries deserialize unknown variants as
  errors. Promising rolling support would require either
  version-negotiation (cross-ring scope) or a serde fallback
  variant (changes wire semantics for every receiver). Single
  workspace bump, full-fleet upgrade matches the posture from
  prior ADRs.
- **Per-kind task-telemetry counters** (e.g.,
  `outer_rows_null_filled`). Considered; rejected. Existing
  `build_rows` / `probe_rows` / `output_rows` counters are
  sufficient for V1; kind-specific aggregates can be surfaced
  downstream of the task wire if needed. Adding telemetry fields
  is an ADR 003 task-telemetry contract change worth carrying
  only when bench evidence is insufficient.
- **Seven separate ADRs, one per kind.** Considered as
  scope-discipline alternative; rejected. The seven kinds share
  the same build-left / probe-right substrate, the same
  null-fill machinery, the same spill semantics. Splitting would
  duplicate contract framing five times without adding
  evidence-driven decisions. The scale-boundary section already
  prevents scope creep.
- **Inner-join regression gate at ±2%** (Claude iter-0 proposal)
  vs ±10% (council convergence). Considered; rejected in favor
  of ±10%. The `JoinOutputRow` refactor introduces measurable
  allocation overhead through the shared materializer; ±10%
  accommodates this without leaving the refactor with no
  guardrail. R13 mitigations are reversible if the bench shows
  >10% regression.
- **Drop IR variants if `sqlparser` regresses a spelling.**
  Considered as cleanup discipline; rejected. The serde wire
  contract and the architecture guard both depend on the IR
  variant set being stable. Fallback is to gate SQL lowering to
  whatever the current dialect parses; keep IR variants.

### ADR 007 cycle

- **Fuel-only V1 sandbox.** Considered (my iter-0 had memory
  enforcement as a V1.x reservation); rejected. Codex iter-0
  argument that a memory bomb fitting within fuel can still OOM
  the host was load-bearing. Memory enforcement is V1 hard
  requirement — Rune-native cap or helper-process isolation
  picked by Phase 0 P0.3.
- **`Context::with_default_modules()` with deny-list tests.**
  Considered (intuitive but fragile to Rune version drift);
  rejected. Allow-list discipline (`Context::new()` + explicit
  registration) inverts the failure mode — new Rune modules
  don't silently widen the attack surface; missing primitives
  break loud rather than capability silently exposed.
- **New `TaskFailureKind` variants
  (`SandboxFuelExhausted` / `SandboxMemoryExhausted`).**
  Considered (Gemini iter-0 proposal); rejected. Preserves the
  ADR 003 7-variant `TaskFailureKind` wire contract. Sandbox
  failures crossing the task wire map to existing
  `TaskFailureKind::TaskFailed { retryable: false, reason:
  "RuneSandbox::<label>" }` — the reason-string mapping is the
  right cross-wire surface.
- **Raw missing-function strings in `CapabilityDenied`.**
  Considered (my iter-0); rejected in favor of
  `CapabilityCategory` enum (`Filesystem`, `Process`, `Network`,
  `Environment`, `Time`, `Random`, `Threading`, `Unknown`). More
  robust against Rune version drift; more useful for client
  tooling. Raw module paths live in internal `tracing` debug
  logs only — never on the public enum.
- **Row-level Rune UDF execution.** Considered (Gemini iter-0
  framed sandbox failures as worker-side UDF failures); rejected
  as out of scope for ADR 007. Rune is frontend-scoped in V1.
  Worker-side execution is a substantial future ADR.
- **Subprocess sandbox with full seccomp + rlimit.** Considered
  as V1 memory-enforcement path; rejected for V1 — heavier than
  necessary. Helper-process isolation (Phase 0 P0.3 path (b))
  is lightweight enough when needed.
- **WASM runtime migration.** Considered as stronger sandboxing;
  rejected — would replace M7's authoring surface entirely. Out
  of scope; reserved for future ADR if Rune sandbox proves
  insufficient.
- **No architecture guard for `Context::with_default_modules`
  absence.** Considered as my iter-0 default; rejected in favor
  of conditional escape firing. Source-level grep guard at
  `tests/architecture_test.rs` is the right defense against
  silent regression to default-allow. Mirrors ADR 005 hard gate
  14 / ADR 006 hard gate 15 pattern.
- **Wall-timeout as primary CPU accounting.** Considered;
  rejected. Fuel is primary; wall-timeout is defense-in-depth
  belt against broken fuel implementation. Legitimate scripts
  never observe `Timeout` under normal operation — fuel
  exhaustion fires first.
- **`AllowProfile` as `Option<String>` or absent in V1.**
  Considered (single-profile means the field is redundant);
  kept as named enum with one variant
  (`AllowProfile::ToronAuthoring`). V1.x extension is then
  type-system-checked, no `RuneSandboxConfig` shape churn.
- **`execute_script(script, fuel)` compatibility shim
  preserved.** Considered (keep the existing signature, ignore
  fuel arg, add a separate `execute_script_with_config`
  method); rejected. The dead `_fuel` parameter cannot remain
  load-bearing. `execute_script(script, &config)` is the V1
  signature; a transitional wrapper is acceptable if it
  constructs a config from the fuel arg, not if it ignores it.

### ADR 006 cycle

- **Filter generation inside `execute_inner_join`.** Original
  Claude/Gemini iter-1 framing; rejected. Codex iter-2's
  stage-order-honesty correction showed that a filter built in
  the join consumer runs after both producer-side shuffles
  complete — too late to attack the bytes-movement bucket.
  The two-wave hash-partitioned shape moves filter generation
  to the build-side exchange producer.
- **Broadcast-only V1 (ADR 006), hash-partitioned filter as
  ADR 007.** Considered (Claude iter-3 option b); rejected.
  Fits one ADR cycle but doesn't move the 10M x 1M
  hash-partitioned promotion workload that produced the ADR 006
  signal — the bench artifact would have nothing to show
  against ADR 005's baseline. Half-ships the contract.
- **Split into ADR 006 (stage-order contract) + ADR 007 (filter
  implementation).** Considered (Claude iter-3 option c);
  rejected. Two ADRs for what is realistically one feature;
  the stage-order decision is meaningless without the filter
  motivating it.
- **Exact `InList` filters in V1.** Considered (Claude/Codex
  iter-2); rejected for V1 in favor of Bloom-only. Reasoning:
  the first cross-ring runtime-filter ADR should prove the
  cross-ring + stage-order contract, not multiply filter kinds.
  `InList` extends the same descriptor contract in V1.x.
- **Cost-based planner opt-in in V1.** Considered; rejected.
  Planner-stats surface doesn't yet exist. Always-two-wave in
  V1; opt-in is V1.x once stats land.
- **Topic E (cross-process wire efficiency) as ADR 006.**
  Considered (Claude iter-1 as a real candidate); demoted to
  fallback. Lower leverage than runtime filters on selective
  joins: moves bytes faster vs avoids moving rejected bytes.
  Reopens as ADR 007 if ADR 006's selective bench misses.
- **Row-group skip as required V1 enforcement.** Considered;
  rejected for required-status. Bloom membership is
  per-encoded-key; row-group min/max stats do not automatically
  prove Bloom absence for a range. Row-level reject is the
  sound V1 enforcement; row-group skip stays opportunistic and
  separately proven.
- **Runtime-plumbing investigation as ADR 006.** Possible
  branch of ADR 005's conditional rule; resolved by ADR 005
  evidence to operator-adjacent (`framework_overhead_ms` fell
  78.8% proportionally with `build_ms`'s 79.0%). Not on the
  ADR 006 table.
- **New runtime-filter telemetry columns in V1.** Considered
  (would sharpen filter-effectiveness measurement); rejected
  in favor of bench evidence only. ADR 005's columns plus the
  selective + non-selective bench rows suffice. New telemetry
  fields are an ADR 003 task-telemetry contract change;
  worth it only when bench evidence is insufficient.
- **Always-emit fragments (no eligibility check).** Considered
  for simplicity; rejected. Composite-key joins, non-canonical
  key types, and broadcast joins should not pay filter
  generation overhead. Eligibility check is single-column +
  inner + hash-partitioned + canonical-encoder-supported type.
- **`nilket-shared` as required new crate.** Considered (Codex
  iter-3 / Gemini iter-4 named it); softened to default
  recommendation only. Extending an existing Wendelon-published
  schema crate is preferred over spawning a new crate before
  multi-consumer pressure justifies it (roadmap "Wendelon-side
  crate extraction" convention). Final crate choice is Phase 1
  implementation detail, not contract-level.

### ADR 005 cycle

- **Streaming push-based executor as ADR 005.** Pre-telemetry
  working hypothesis (Volcano → push-based, morsel parallelism,
  end-to-end streaming). Rejected outright by ADR 004's
  post-promotion telemetry: probe + output combined at ~7% of
  wall; the rewrite addresses ~0% of measured bottleneck.
  Reserved for the case where a future workload shifts probe-side
  to the dominant bucket; until then, this stays demoted.
- **Sub-attribution-first investigation of the 38% residual as
  ADR 005.** Considered (Codex iter-1 framing); rejected because
  the build bucket is sharper and the conditional ADR 006 rule
  resolves the residual question for free in most cases.
- **Fast-path eligibility extending broadly in V1.** Considered
  (Gemini iter-1 — all primitive integer types in V1); rejected
  in favor of the conservative `Int64`/`Int32` default. Each
  extension carries a parity-test and logical-type-tag obligation;
  Phase 0 evidence may justify extending within this ADR.
- **Pin a specific hash table layout in the ADR.** Considered
  (open addressing vs chaining vs Swiss table). Rejected: the
  contract pins invariants, layout is Phase 0 calibration evidence.
- **Hash function change** (replacing `xxh3_64` with ahash /
  wyhash). Rejected — partition-assignment consistency requires
  `xxh3_64` parity. Operator-local hash-table choice must preserve
  the ADR 003 §5 boundary.
- **Concurrent / sharded hash-table build in V1.** Considered as
  Candidate (morsel-parallel build inside a worker); rejected.
  Adds sharded-table determinism, per-thread memory accounting,
  and allocator questions needing their own evidence. V1.x with
  its own ADR.
- **Two-band conditional ADR 006 rule** (operator-adjacent vs
  flat, no gray zone). Considered (Codex iter-1 / Gemini iter-2
  framings); rejected for the explicit three-band rule with
  numeric thresholds (≥50%, ≤20%, gray) plus the "both fail"
  reopen branch.
- **New telemetry columns to sharpen residual attribution within
  ADR 005.** Considered as ADR-005 precondition; rejected as it
  would turn ADR 005 back into attribution work. ADR 004's columns
  are sufficient; the conditional ADR 006 rule resolves the
  remaining question.
- **Mandatory flamegraph diff** (baseline vs post) in bench
  artifact. Considered (iter-3 open question); rejected — the
  dominant-hot-path note is the load-bearing evidence; flamegraph
  file reference is optional.

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
  G16 enforces. Sort-merge, nested-loop, non-equi joins,
  cross join — each is its own future ADR. Outer / semi / anti
  joins now pinned by ADR 008 (eight-variant `JoinKind`); the
  underlying hash-build / probe-right algorithm is unchanged.
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
- **Don't conflate join-build key hashing with
  exchange-partition hashing.** ADR 003 §5 partition assignment
  uses `xxh3_64(canonical_encoded_bytes)`; ADR 005's fast-path
  key representation is operator-local. Helper names must make
  the boundary explicit ("join build key" vs "exchange partition
  hash"), never a generic "fast hash" that blurs them. ADR 005
  hard gate 14 conditional architecture guard.
- **Don't let an ADR 005 fast-path encoder lose the logical-type
  tag.** `Int32(1)` and `Int64(1)` must remain distinct
  build-side hash keys. ADR 005 hard gate 7 unit-test enforced.
- **Don't promote ADR 005 on wall-clock alone.** `build_ms` is
  the primary signal; digest equality and phase attribution are
  load-bearing. ADR 005 hard gate 16.
- **Don't soften an ADR 005 `build_ms` target miss** into a
  vague "improved" narrative. The bench artifact records the
  explicit gap; the council selects accept-narrower / scope-down
  / shelve.
- **Don't add new contract surface in ADR 005.** No new
  `MessageKind`, IR variant, `BudgetDimension`, task telemetry
  field, public API, Wendelon scan contract, or architecture
  guard (default; conditional escape only). ADR 005 hard gates
  3, 12-14.
- **Don't parallelize hash-table build inside a worker in
  ADR 005.** Sharded concurrent insertion is V1.x with its own
  determinism + budget accounting work.
- **Don't bundle runtime-plumbing investigation into ADR 005.**
  Conditional ADR 006 governed by the three-band rerun rule.
- **Don't generate ADR 006 runtime filters inside
  `execute_inner_join`.** Too late in ADR 004's stage graph;
  the probe-side shuffle has already happened. Filter
  generation lives in the build-side exchange producer wave
  (ADR 006 Decision §1).
- **Don't let ADR 006's runtime-filter hash conflate with the
  exchange-partition hash.** ADR 003 §5 partition assignment
  uses `xxh3_64(canonical_encoded_bytes)`; ADR 006's
  runtime-filter hashing is operator/cross-ring scoped.
  Helper names are semantic ("runtime filter key" vs
  "exchange partition hash"). Generalizes ADR 005's pattern.
  ADR 006 hard gate 15.
- **Don't let an ADR 006 runtime filter affect join
  correctness.** Advisory always — false positives allowed,
  false negatives forbidden. If Wendelon drops the filter for
  any reason, output is byte-identical to filter-disabled.
  ADR 006 hard gate 7 (false-positive tolerance) + hard
  gate 10 (no false negatives).
- **Don't start probe-side dispatch before the ADR 006
  build-side runtime-filter barrier opens** when filters are
  enabled. ADR 006 hard gate 13 — the two-wave barrier is
  contractually load-bearing.
- **Don't deserialize ADR 006's runtime-filter descriptor as
  a Toron-private type in Wendelon.** Shared cross-ring schema
  surface only (default: extend an existing Wendelon-published
  schema crate; alternative: `nilket-shared` common module).
- **Don't multiply ADR 006 filter kinds in V1.** Bloom only.
  Exact `InList` is V1.x once the cross-ring contract is
  proven.
- **Don't add ADR 006 planner cost-model opt-in in V1.** Always
  two-wave when the filter is enabled; planner-stats opt-in
  is V1.x once stats exist.
- **Don't couple ADR 006 promotion to lockstep Wendelon-side
  landing.** Advisory invariant makes split-shipment safe.
  Toron-side ships first; Wendelon-enabled bench row arrives
  in a follow-up update.
- **Don't extend ADR 006 filter eligibility to outer / semi /
  anti / sort-merge / non-equi / broadcast joins.** Each is
  its own future ADR (V1.x). Inner + hash-partitioned only in
  V1.
- **Don't soften an ADR 006 selective-row reduction miss**
  into a vague "improved" narrative. The bench artifact
  records the explicit gap; the council selects
  accept-narrower / scope-down (narrow filter eligibility) /
  shelve.
- **Don't promote a runtime-filter ADR without a
  no-false-negative unit test.** The advisory invariant is
  load-bearing; false negatives are contract violations.
  ADR 006 hard gate 10.
- **Don't skip the non-selective guard benchmark.** The
  two-wave barrier cost is real; the guard proves it's
  bounded against ADR 005's `build_ms` baseline. ADR 006
  hard gate 19.
- **Don't skip the Scale boundary section in new ADRs**
  (convention adopted iter-5 of ADR 006). Even when the
  boundary feels obvious, naming it explicitly makes
  scope-creep into adjacent boundaries visible at review
  time. Omit only when truly N/A and say so explicitly.
- **Don't call `Context::with_default_modules()` in the Rune
  engine** (ADR 007 hard gate 19). The V1 contract is
  allow-list (`Context::new()` + explicit registration); a
  future refactor reverting to default-allow opens the
  sandbox. Source-level architecture grep guard at
  `tests/architecture_test.rs` enforces this.
- **Don't ship a fuel-only V1 Rune sandbox.** Memory
  enforcement is V1 hard requirement (ADR 007 hard gate 8). A
  memory bomb that fits within fuel can still OOM the host —
  Rune-native cap or helper-process isolation, not "deferred."
- **Don't add new `TaskFailureKind` variants for sandbox
  failures.** ADR 003 7-variant wire contract is
  architecture-guarded. Sandbox failures crossing the task wire
  map to `TaskFailureKind::TaskFailed { retryable: false,
  reason: "RuneSandbox::<label>" }`.
- **Don't let Rune sandbox failures fall through to
  `RuneRuntime`.** Use the `FrontendError::RuneSandbox`
  variant carrying the typed `RuneSandboxViolation` enum.
  `RuneRuntime` is for genuine script-logic errors only.
- **Don't expose host paths, internal VM state, or Rune
  source positions in coordinator-facing error messages**
  (ADR 007 hard gate 20). `CapabilityCategory` enum abstracts
  over raw module paths for stability and non-leakiness.
- **Don't add native functions to the Rune allow-list without
  threat-model review.** Each new function is a potential
  sandbox-escape vector. Extension is a separate narrow ADR.
- **Don't couple Rune sandbox enforcement to per-tenant
  governance.** Tenant identity is L-3 (multi-tenant
  governance) territory. ADR 007 ships per-*script* budgets;
  per-tenant aggregation is a future ADR when workload evidence
  justifies.
- **Don't treat `wall_timeout` as primary CPU accounting.**
  Fuel is primary; `tokio::time::timeout` around
  `async_complete()` is a defense-in-depth belt that fires only
  at .await yield points (Rune 0.13.4 drives the VM via a
  synchronous `vm.run()` inside `async_resume()`). Real
  preemption of a CPU-bound loop with broken fuel comes from the
  Phase 4 helper-process parent-side SIGKILL deadline, not the
  Tokio timeout. The infinite-loop hard gate (gate 7) must
  assert `FuelExhausted`, not merely `Timeout`.
- **Don't run row-level Rune UDFs in worker tasks under
  ADR 007.** Rune is frontend-scoped in V1 (script →
  `LogicalPlan` → distribution). Worker-side execution is a
  substantial future ADR.
- **Don't introduce a second `ExecutionPlan::Join` or
  `LogicalPlan::Join` physical variant** (ADR 008 hard gates
  22, 23). Per-kind physical variants duplicate the operator
  shape and weaken ADR 004's single-variant architecture guard.
  The semantic difference belongs in `JoinKind`.
- **Don't use sentinel values for null fill in outer joins.**
  Arrow's native `NullBuffer` is the canonical representation;
  sentinels break the contract and couple to downstream readers.
  ADR 008 hard gate 10.
- **Don't loosen runtime-filter eligibility beyond `Inner`.**
  ADR 006 invariant is load-bearing for outer / anti
  correctness. A Bloom filter on the build side of a left-outer
  join would drop the very rows the outer must preserve;
  `RightAnti` depends on emitting unmatched probe rows. ADR 008
  hard gate 17 enforces; a "filter + preserve" combined operator
  is the separable future design (ADR 009 Candidate C).
- **Don't allow `JoinStrategy::Broadcast` for non-Inner
  `JoinKind`.** Broadcast replicates the build side; preserved-
  side or existence variants would emit duplicate unmatched
  rows in every probe partition. ADR 008 hard gate 16 (binder
  rejection) + hard gate 18 (fragmenter defensive guard) form
  the defense-in-depth pair.
- **Don't rewrite `RightOuter` ↔ `LeftOuter` at planner time as
  a workaround for broadcast.** Strategy-dependent semantics
  blur the operator-level scale boundary. The cleaner contract
  is reject-and-defer; a strategy-rewrite ADR can follow if
  evidence supports it.
- **Don't add `EXISTS` / `NOT EXISTS` decorrelation under ADR
  008.** Subquery-planner contract; reserved as ADR 009
  Candidate E. ADR 008 ships the `Semi` / `Anti` operators; the
  subquery-planner ADR layers on top.
- **Don't promise rolling coordinator/worker mixed-version
  semantics for `JoinKind`.** Serde-tagged variants are additive
  but older binaries reject unknown variants. ADR 008 ships at a
  single workspace version bump; cluster upgrades are
  full-fleet, not rolling. Matches the posture from prior ADRs.
- **Don't drop `JoinKind` IR variants if `sqlparser` regresses
  a spelling.** Keep the IR variants; gate SQL lowering to
  whatever parses. Dropping IR variants breaks the serde wire
  contract and ADR 008 hard gate 25 (variant-count architecture
  guard).
- **Don't add per-kind task-telemetry counters for the new join
  kinds.** ADR 003 task-telemetry contract change worth carrying
  only when bench evidence is insufficient. Existing
  `build_rows` / `probe_rows` / `output_rows` counters are
  sufficient for V1; kind-specific aggregates surface downstream
  of the task wire if needed.
