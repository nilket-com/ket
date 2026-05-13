# ADR 004: Toron Distributed Inner Equi-Join Contract

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/004_distributed_inner_equi_join.md`

## Status

Promoted to `plans/adr/`. Pre-implementation; Phase 0
calibration runs first. Promotion evidence will live at
`plans/impl/004_distributed_inner_equi_join_bench_results.md`
after the join keystone fixtures pass.

Builds on five WIP iteration rounds across the council. All
three voices converged on every contract surface; iteration
artifacts retired (git holds the narrative). Gemini's
qualifier-aware column identity framing, Codex's evidence-
first scope discipline and broadcast caller-rule pin, and
Claude's explicit-pre-shuffle and hash-spill arguments are
the load-bearing antecedents.

This ADR is Toron's V1 contract for the **first two-input
relational operator**: how an `INNER` equi-join gets bound,
fragmented, dispatched, executed, and recovered across
multiple worker processes under two strategies (hash-
partitioned correctness baseline; bounded broadcast
optimization).

It does **not** decide outer / semi / anti / cross joins
(V1.x), non-equi joins (V1.x), sort-merge join (V1.x —
requires ordering contract), multi-way join planning
(V1.x — requires cost model), cost-based strategy /
build-probe selection (V1.x — requires Wendelon stats +
Toron cost model), bloom / runtime filter pushdown (own
cross-ring ADR), skew mitigation (own ADR; observation
only here), `USING` / `CROSS JOIN` / comma-join SQL surface
(V1.x), floating-point join keys (own canonical-encoding
ADR), or SQL join hints (V1.x).

## V1 contract summary

- **Join kind.** Binary `INNER` equi-join only.
  `JoinKind { Inner }` single-variant enum. V1.x outer-join
  ADR extends.
- **Strategies (two).** `JoinStrategy { HashPartitioned,
  Broadcast }`. Both pinned in V1; explicit in bound plan.
- **Strategy selection.** SQL planner defaults to
  `HashPartitioned` for `JOIN ... ON` syntax. Programmatic
  callers (Rune, fixtures, benches, external APIs) emit
  `JoinStrategy::Broadcast` when they have cardinality
  knowledge. SQL hint syntax (`/*+ BROADCAST(t) */`) and
  statistics-driven selection are V1.x.
- **Build side.** Syntactic `left` = build; syntactic
  `right` = probe for **both** strategies. For broadcast
  this is a caller-discipline contract: callers requesting
  `Broadcast` must place the small build input on the left.
  No `BuildSide` field on `ExecutionPlan::Join` in V1
  (V1.x cost-based-swap ADR adds it).
- **Column identity.** Logical `Expr::Column(String)` is
  replaced by `Expr::Column(ColumnRef)` where
  `ColumnRef = { qualifier: Option<TableRef>, name:
  ColumnName }`. Binder maintains separate scopes for left
  and right join inputs; unqualified refs ambiguous across
  both sides reject with `BindError::AmbiguousColumnRef`.
  Output schema = left's fields followed by right's
  fields; duplicate field names across sides **permitted**
  at the bound-schema level; downstream consumers
  disambiguate by ordinal; SQL planner enforces naming
  discipline at the SQL surface (top-level ambiguous refs
  fail typed at the planner, not the binder).
- **Plan surface.** `LogicalPlan::Join { left, right,
  kind, on: Vec<(Expr, Expr)>, strategy: JoinStrategy }`
  lowers to `ExecutionPlan::Join { left, right, kind,
  left_keys: Vec<ColumnOrdinal>, right_keys:
  Vec<ColumnOrdinal>, strategy, output_schema }`. Single
  bound join variant.
- **Stage DAG, hash-partitioned.** Three stages: left-
  partition (Stage K-2), right-partition (Stage K-1), join
  (Stage K, multi-upstream). The planner emits explicit
  `Exchange` operators above each join input; the
  fragmenter does not synthesize exchanges.
- **Stage DAG, broadcast.** Two stages: build-publish
  (Stage K-1, single-partition manifest from the left
  input), probe-and-join (Stage K, per-worker probe scan
  share + pulled build manifest). Build publish reuses
  `Exchange { partition_count: 1 }` (broadcast publish
  operator decision lands in Phase 0).
- **`StagePlan` substrate.** `upstream_stage:
  Option<StageId>` renamed mechanically to
  `upstream_stages: Vec<StageId>` per
  `no-premature-backward-compat.md`. No compat shim.
- **`TaskInputBinding::Scan` carries optional share.**
  `Scan { share: Option<ScanShare> }` where `ScanShare =
  { worker_index, worker_count }`. `share: None` is the
  existing full-source-table scan (ADR 003 source stages
  unchanged). `share: Some(...)` is the broadcast probe-
  scan-share path: worker `w_i` scans tablets satisfying
  `xxh3_64(tablet_id) mod worker_count == worker_index`.
  The mechanical rename of the `Scan` variant from
  fieldless to single-field is per
  `no-premature-backward-compat.md` (no compat shim;
  existing call sites become `Scan { share: None }`).
- **Wire surface.** Zero new `MessageKind` variants. Hash-
  partitioned join tasks carry two `TaskInputBinding::
  ShuffleConsumer` entries; broadcast join tasks carry one
  `Scan { share: Some(...) }` (right input probe-scan
  share) + one `ShuffleConsumer` (left input build
  manifest). Input routing — how a worker discriminates
  build from probe within a single multi-input task — is
  by **binding variant** under broadcast (`Scan` is probe,
  `ShuffleConsumer` is build) and by **`producer_stage_id`
  matching the operator's left / right subtree terminal
  `Exchange` stages** under hash-partitioned. Both
  strategies have unambiguous one-rule routing for V1; no
  explicit side tag on the binding is needed.
- **Partition assignment.** Inherited from ADR 003 §5:
  `partition_id = xxh3_64(key_values) mod partition_count`.
  Same hash function and partition count on both sides of
  hash-partitioned.
- **Broadcast cap regime (three).**
  - `broadcast_build_bytes_per_task` (default 64 MiB) — the
    materialized build payload size.
  - `broadcast_replicated_bytes_per_query` (default 1 GiB)
    — build size × consumer worker count.
  - `memory_per_task_bytes` (ADR 003 default 64 MiB).
  Admission validated at bind time (when sizes are
  statically known) and at dispatch time (against actual
  manifest `size_bytes`). Over-cap fails terminal
  `BudgetExhausted { dimension: BroadcastReplication }`. No
  silent strategy switch mid-attempt. New
  `BudgetDimension::BroadcastReplication` variant.
- **Broadcast probe scan-share.** Deterministic per-worker
  assignment: `xxh3_64(source_tablet_id) mod worker_count`.
  Required for cross-replica determinism.
- **NULL-key semantics.** `INNER` only in V1: rows with
  NULL in any join-key component dropped on both sides.
  NULL ≠ NULL. Multi-column keys match only when every
  component is non-NULL and equal to its paired component.
- **Key types.** Signed / unsigned integer widths, UTF-8
  string, boolean (both sides matching), date / timestamp
  integer representations. Floating-point rejected at
  bind time (NaN / signed-zero / canonical-encoding
  unspecified — own future ADR).
- **Build-side memory budget.** Hash table reserves against
  `QueryBudget::memory_per_task_bytes`.
- **Build-side hash spill.** When next-row reservation
  would exceed `memory_per_task_bytes`, hash table
  promotes one-way to encrypted spill using the M10
  AES-256-GCM-SIV substrate. Index is hash-bucket → page
  range (random-access, distinct from ADR 003 partition-
  store streaming index). Probe lookups read relevant
  bucket pages. LRU page cache. Spill bytes reserve
  against `QueryBudget::spill_bytes_reserved_per_query`
  (shared cap with partition-store spill). Spill-cap
  exhaustion fails terminal `BudgetExhausted { dimension:
  SpillBytes }`. Memory-cap exhaustion before spill can
  initialize fails terminal
  `BudgetExhausted { dimension: Memory }`.
- **Failure modes.** Compose on ADR 003 §8 producer-loss
  invalidation. Left- or right-producer SIGKILL
  invalidates that side's manifest and cascades to
  dependent join consumers. Broadcast-build-publish
  SIGKILL invalidates the build manifest and cascades to
  all probe consumers. Join-worker SIGKILL retries the
  join task without invalidating upstream manifests.
- **Determinism.** ADR 003 §13 canonical normalization
  (concat → stable lex full-row sort → reslice 65,536 rows
  → digest) is the output-parity contract. No operator-
  internal sort. Hash-bucket iteration order and partition
  arrival order are intrinsically non-deterministic; the
  full-row sort subsumes both.
- **Three new architecture guards G16-G18** supplementing
  ADR 002's G1-G7 and ADR 003's G8-G15. G16: no non-pinned
  join algorithms. G17: `ExecutionPlan::Join` sole join
  variant with two-variant `JoinStrategy`. G18: scheduler
  reads `upstream_stages: Vec<StageId>` (not single-Option).

## Context

ADR 001 promoted Toron's foundation (greenfield Arrow-native
engine, `StorageScan` boundary). ADR 002 promoted the
storage-backed execution contract (PBC pushdown load-
bearing, schema-bound `ExecutionPlan`, descriptor
fingerprint pin, grouped aggregate). ADR 003 promoted the
distributed runtime and data-movement contract (pull-
addressed shuffle, producer-loss replay, strict stage
barriers, per-query budgets, canonical output digests; 14
hard gates passed; SIGKILL keystone fixture green).

The remaining V1 contract surface is the first two-input
relational operator. Without joins, Toron cannot serve most
analytical workloads — star schemas, fact-dimension joins,
multi-table reports, and CTE-driven analytics all require
joining tables. ADR 003 left the substrate complete enough
to compose joins on top: shuffle for hash-partitioned
exchange, partition-store publish for broadcast, multi-
upstream stage support via the planned `Vec<StageId>`
generalization, per-query budget for memory and spill caps,
canonical normalization for output determinism.

Joins are also the **keystone evidence workload** for the
ADR 005 scope decision. Probe-side cardinality cannot be
pushed into Wendelon (no cross-ring join primitive); build-
side materialization is memory-bound; both paths exercise
the buffered executor through full-cardinality data. The
10M × 10M `INNER JOIN` benchmark answers whether the
buffered executor is the remaining V1 bottleneck or whether
ADR 002 + ADR 003 already carry V1.

### Why distributed joins is the keystone scope

Three alternative ADR 004 scopes were considered against the
ADR 003 bench evidence:

- **Multi-tenant resource governance.** Rejected: ADR 003
  bench did not surface multi-tenant pressure pre-V1; per-
  query budgets are sufficient for the next operator
  contract. Deferred to ADR 005+ if launch pressure
  surfaces.
- **Executor model upgrade.** Rejected: post-distribution
  evidence from ADR 003 is too sparse to say the buffered
  executor is the bottleneck. Joins are the workload that
  answers the question; executor ADR comes after joins
  surface the bottleneck. Deferred to ADR 005+.
- **Distributed joins** (this ADR). Selected: the missing
  two-input operator is the V1 launch gate; substrate
  composes cleanly on ADR 003; joins are the keystone
  evidence workload for the next ADR's scope.

### Why scope is intentionally narrow

ADR 004 is deliberately scoped to **inner equi-join under
two strategies**. It excludes:

- **Outer / semi / anti / cross joins.** Each pins
  preserved-side null semantics or different match cardinality.
  Own ADR.
- **Non-equi joins / theta-joins.** Different distributed
  shape — can't partition on equality of inequality
  predicates. Own ADR.
- **Sort-merge join.** Requires ordering contract Toron
  doesn't have (no ORDER BY semantics pinned, no sort
  operator). Own ADR.
- **Multi-way join planning.** Requires cost model.
- **Cost-based build / probe swap and cost-based strategy
  selection.** Require Wendelon stats surface and Toron
  cost model.
- **Bloom / runtime filter pushdown.** Cross-ring contract
  with Wendelon `ScanStream`. Own ADR when join bench
  evidence demonstrates filter-side reduction matters.
- **Skew mitigation.** Observation in V1 (soft gate);
  mitigation needs heavy-hitter detection, dynamic
  partition-count override, salt-and-replicate. Own ADR.
- **SQL surface sugar.** `USING`, `CROSS JOIN`, comma-join,
  `/*+ BROADCAST(t) */` hint syntax. V1.x SQL surface
  decisions.
- **Floating-point join keys.** Need canonical encoding
  for NaN / signed zero / subnormals. Own ADR.
- **Intra-task build / probe pipelining.** V1.x executor
  work.
- **Hash-spill-to-memory demotion.** One-way per task in
  V1; demotion is V1.x.

## Decision

### 1. Plan surface

```rust
pub enum JoinKind {
    Inner,
}

pub enum JoinStrategy {
    HashPartitioned,
    Broadcast,
}

pub enum LogicalPlan {
    // ... existing variants ...
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinKind,
        on: Vec<(Expr, Expr)>,
        strategy: JoinStrategy,
    },
}

pub enum ExecutionPlan {
    // ... existing variants ...
    Join {
        left: Box<ExecutionPlan>,
        right: Box<ExecutionPlan>,
        kind: JoinKind,
        left_keys: Vec<ColumnOrdinal>,
        right_keys: Vec<ColumnOrdinal>,
        strategy: JoinStrategy,
        output_schema: SchemaRef,
    },
}
```

`left_keys[i]` and `right_keys[i]` form the i-th equality
pair. The bound IR uses ordinals; column names are metadata
(downstream `Project` references by ordinal).

`on` is `Vec<(Expr, Expr)>`. The binder asserts each pair is
two `Expr::Column(_)` forms; computed-expression keys
(`ON a.x + 1 = b.y`) reject with
`JoinComputedKeyUnsupported`. Type-level restriction to
`ColumnRef` was considered and rejected because V1.x may
relax this constraint with expression-aware partitioning.

### 2. Column identity and name resolution

Logical column references become qualifier-aware:

```rust
pub struct TableRef(pub String);

pub struct ColumnRef {
    pub qualifier: Option<TableRef>,
    pub name: ColumnName,
}

pub enum Expr {
    Column(ColumnRef),
    // ... other variants unchanged
}
```

Existing single-input plans construct `ColumnRef
{ qualifier: None, name }`; the binder's single-input path
is unchanged in behavior.

Join binding maintains separate scopes for left and right
inputs (collected from `LogicalPlan::ScanNamed` leaves and
any SQL aliases). Resolution rules:

- **Qualified reference** (`orders.id`) — must name exactly
  one visible table or alias on one side. Unknown qualifier
  → `BindError::UnknownTableRef`.
- **Unqualified reference** (`id`) — resolves only when
  exactly one side exposes that column name. Both →
  `BindError::AmbiguousColumnRef`. Neither →
  `BindError::UnknownColumn`.
- **Pair order in `on`** — the binder normalizes pair order
  so the bound `left_keys[i]` always references the logical
  `left` input. Users writing `ON customers.id =
  orders.customer_id` (where `customers` is the right input)
  get auto-swapped.

The bound `ExecutionPlan::Join` uses `left_keys:
Vec<ColumnOrdinal>` and `right_keys: Vec<ColumnOrdinal>`,
paired by index. No `JoinSide { Left, Right }` tag on
`ResolvedColumn` — the side is structural (encoded in
`left_keys` / `right_keys` field names), not metadata.

Output schema = left's fields followed by right's fields,
in order. Field nullability preserved (no nullable
adjustment for `Inner`; outer-join ADR will adjust for
preserved-side semantics). **Duplicate field names across
sides are permitted** at the bound-schema level —
downstream operators reference by `ColumnOrdinal`, not by
name. SQL-surface ambiguity (top-level `SELECT id FROM a
JOIN b ON a.id = b.id`) is the SQL planner's concern: it
fails with `AmbiguousColumn` when the user references an
ambiguous name; explicit qualification (`SELECT a.id`)
succeeds. PostgreSQL and DuckDB use this same architectural
division.

### 3. Strategy selection in V1

`JoinStrategy` is explicit on `LogicalPlan::Join.strategy`:

- **SQL planner** lowers ordinary `JOIN ... ON` syntax to
  `JoinStrategy::HashPartitioned` (safe default).
- **Programmatic callers** (Rune programs, test fixtures,
  benches, external API consumers) can emit
  `JoinStrategy::Broadcast` when they have cardinality
  knowledge of the small build side.
- **Binder validation.** When broadcast input sizes are
  statically known (scan-stats hint), the binder rejects
  known-over-cap broadcast with
  `BroadcastCapExceededAtBind`. Otherwise, dispatch-time
  validation catches it.
- **No mid-attempt strategy switch.** Once dispatched,
  strategy is load-bearing. Cap violation at dispatch
  fails the query terminally; the next bind (with actual
  size knowledge) would naturally choose
  `HashPartitioned`. V1 surfaces the cap; doesn't silently
  switch.

V1 ships without a cost model. Statistics-driven and SQL-
hint strategy selection are V1.x.

### 4. Hash-partitioned strategy

Three-stage DAG:

```
Stage K-2: Scan(left=build) → optional Filter → Exchange(left_keys, partition_count)
                                                                              ↘
                                                                               Stage K: Join
                                                                              ↗
Stage K-1: Scan(right=probe) → optional Filter → Exchange(right_keys, partition_count)
```

Stage K has `partition_count` tasks. Each task carries two
`TaskInputBinding::ShuffleConsumer` entries in `inputs` and
`upstream_stages: vec![Stage K-2, Stage K-1]`.

Worker discriminates build vs probe by matching
`TaskSpec.inputs[i].producer_stage_id` against the bound
operator's left / right subtree terminal `Exchange` stages.
Input order in `inputs` is not load-bearing.

The bound plan contains **explicit `Exchange`** operators
above each join input. The fragmenter does not synthesize
exchanges. Missing exchanges reject typed:
`FragmentError::JoinInputNotExchanged`. Mismatched
`partition_count` or `partition_key` across the two
exchanges rejects at bind time:
`BindError::JoinPartitionCountMismatch` /
`BindError::JoinKeyExchangeMismatch`.

Same `xxh3_64` partition assignment as ADR 003 §5. Same
`partition_count` and `partition_key` shape on both sides —
colocation is structural, not configured.

### 5. Bounded broadcast strategy

Two-stage DAG:

```
Stage K-1: Scan(left=build) → optional Filter → BroadcastPublish(partition_count=1)
                                                                              ↘
                                                                               Stage K: per-worker Scan(right=probe share) + Join
```

**Build is always left.** Convention rule for callers
requesting `Broadcast`: place the input intended for
broadcast publish on the `left` side of the
`LogicalPlan::Join`. Build = left consistently across both
strategies. The binder's `BroadcastBuildSidePlacement`
check is a **structural** validation, not a stats-driven
intent inference (the binder cannot know a caller's
intended "small build input" without statistics; see
Rationale §"BroadcastBuildSidePlacement is structural").
The check: under `JoinStrategy::Broadcast`, the left
subtree must terminate in the single-partition publish
shape (`Exchange { partition_count: 1 }` or
`BroadcastPublish` per Phase 0 P0.2); right-publish shape
rejects with `BindError::BroadcastBuildSidePlacement`.

Stage K-1 publishes one logical partition (the full
materialized left input). Stage K has `worker_count` tasks
(one per assigned worker). Each consumer task carries:

- one `TaskInputBinding::Scan { share: Some(ScanShare
  { worker_index, worker_count }) }` for the right (probe)
  input — each consumer scans its share of the probe input
  directly; **no shuffle on the probe side**;
- one `TaskInputBinding::ShuffleConsumer` for the broadcast
  build manifest, with `partition_id = 0` (single-partition
  publish).

Workers identify the input role within the task by
binding variant: `Scan { share: Some(_) }` is the broadcast
probe input (right subtree); `ShuffleConsumer` is the build
input (left subtree). Architecture-guarded — see G16-G18
plus the architectural assertion that broadcast join tasks
carry **exactly one** `Scan { share: Some(_) }` and
**exactly one** `ShuffleConsumer` (no ambiguity in
discrimination).

**Probe scan-share assignment.** Deterministic per-worker
via `xxh3_64(source_tablet_id) mod worker_count`. Worker
`w_i` reads tablets whose hash maps to `i`. Required for
cross-replica determinism — assignment must be reproducible
across runs of the same query.

**Broadcast publish operator.** Phase 0 P0.2 picks between
reusing `Exchange { partition_count: 1 }` vs introducing a
narrow `BroadcastPublish` operator. The wire shape is
identical either way (single-partition manifest published
to partition store, all consumers pull via
`PartitionAddress`).

#### Cap regime

Broadcast is admissible only when **all three** caps prove
satisfied:

1. `broadcast_build_bytes_per_task` (default 64 MiB) — the
   materialized build payload size.
2. `broadcast_replicated_bytes_per_query` (default 1 GiB) —
   build size × consumer worker count.
3. `memory_per_task_bytes` (ADR 003 default 64 MiB) — each
   consumer's hash-table memory reservation.

**Admission flow.** Bind-time validation rejects
known-over-cap (when scan-stats hints are available).
Dispatch-time validation against the actual build manifest
`size_bytes` (from `PartitionEntry`) is the load-bearing
check; over-cap fails terminal with `BudgetExhausted
{ dimension: BroadcastReplication }`. No silent strategy
switch.

Broadcast build payloads are **never** inlined in
`StartTask`. They flow through the partition store like
any other shuffle artifact (architecture-guarded).

### 6. Worker-local join operator

Both strategies share the operator core:

1. **Build phase.** Pull build-side (left input). For each
   row, compute `HashKey` from `left_keys` values
   (canonical encoding + `xxh3_64`, same hash function as
   ADR 003 §5 partition assignment). NULL-key rows
   dropped (not inserted). Insert non-null-key rows into
   hash table.
2. **Probe phase.** Pull probe-side (right input). For
   each row, compute `HashKey` from `right_keys`. NULL-key
   probe rows produce no matches (skip). Look up matching
   build rows in the hash table; emit one output row per
   (build, probe) match with left's columns followed by
   right's columns.
3. **Output emit.** Stream Arrow IPC batches via
   `TaskBatch` (final-result join tasks). Row order is
   non-deterministic at the operator layer; ADR 003 §13
   canonical normalization handles it.

**Input routing rule.** Within a multi-input task, the
worker matches each `TaskInputBinding` entry in
`TaskSpec.inputs` to the build or probe role as follows:

- Under `HashPartitioned`: both inputs are
  `ShuffleConsumer`. Match `producer_stage_id` against the
  bound `Join` operator's left / right subtree terminal
  `Exchange` stages. Left-subtree `Exchange` →
  build-input; right-subtree → probe-input. Input order in
  `inputs` is not load-bearing.
- Under `Broadcast`: inputs are one `Scan { share:
  Some(_) }` and one `ShuffleConsumer`. The `Scan` is the
  probe input (right subtree); the `ShuffleConsumer` is
  the build input (left subtree). Discrimination is by
  binding variant; no `producer_stage_id` matching is
  needed for the scan side (the scan has no upstream
  stage). Architecture-asserted: broadcast join tasks
  carry exactly one of each variant.

Hash-partitioned: build pulls from left-stage shuffle
partition. Broadcast: build pulls from broadcast publish
manifest (every consumer pulls the same partition); probe
reads the per-worker tablet share described by
`ScanShare`. Same operator core; different input binding
source for the build side.

Build phase fully completes before probe phase begins.
Intra-task pipelining (probing while still building) is
V1.x.

### 7. Build-side hash spill

When the hash table's next-row reservation would exceed
`memory_per_task_bytes`:

1. **Promote.** Flush all in-memory hash buckets to
   encrypted page-aligned blocks. Switch the insert path
   to spill-write mode. **One-way per task attempt** — no
   demotion back to memory.
2. **Substrate.** AES-256-GCM-SIV encryption via the M10
   substrate (`wendelon::storage::file_ops::*`). One spill
   file per join task; `SpillId` allocated at task start;
   per-page nonce derivation from `SpillId + page_index`.
3. **Page layout.** Page size: 64 KiB (Phase 0 P0.9
   calibration). Each page holds rows for one or a
   contiguous range of hash buckets. Index:
   `HashMap<HashBucketId, PageRange>`.
4. **Probe-time access.** Bucket lookup checks in-memory
   first (pre-promotion buckets remain there until
   flushed); falls back to spill page fetch + decrypt +
   scan. LRU page cache up to `hash_spill_cache_bytes`
   (default 8 MiB per task; Phase 0 P0.9).
5. **Budget accounting.** Two separate reservations:
   - **`spill_bytes_reserved_per_query`** charges every
     byte written to encrypted pages on disk. Shared cap
     with ADR 003 partition-store streaming spill.
   - **`memory_per_task_bytes`** charges every byte of
     spill-resident in-memory state: the bucket → page-
     range index, the LRU decrypted-page cache, the
     spill-writer buffers in flight, and any per-bucket
     scratch state. The hash table's pre-promotion
     in-memory buckets (if any survive promotion) also
     count. **The 8 MiB default cache size and bucket
     index together must fit under
     `memory_per_task_bytes`** — the cache is *part of*
     the task memory budget, not in addition to it.
     Spilling does not let the task escape its memory
     cap; it lets the table grow beyond what fits in
     memory by shifting persistent state to disk while
     keeping resident bookkeeping bounded.
6. **Failure.** Spill-cap exhaustion fails terminal
   `BudgetExhausted { dimension: SpillBytes }`. Memory cap
   exhaustion (including by index + cache pressure when
   the working set evicts too aggressively) fails terminal
   `BudgetExhausted { dimension: Memory }`. Memory cap
   exhaustion before spill can initialize also fails this
   way.
7. **Cleanup.** Spill files deleted on task termination
   (complete, failed, cancelled); composes with ADR 003
   partition-store cleanup hooks.

**Why a separate index from partition-store spill.**
Partition-store reads are streaming (`PullPartition` reads
sequentially from start to end); the producer writes once,
the consumer reads once. Hash-spill reads are random-access
by bucket (probe lookups need point-access). Sharing one
streaming index would force probe to scan the full spilled
table per lookup — asymptotically O(N²) where N is build
cardinality. The encryption + page substrate is shared
(both use M10 AES-256-GCM-SIV); the lookup index is not.
This is operator-local layout, not a new distributed
protocol surface.

### 8. Failure modes

All compose on ADR 003 §8 producer-loss invalidation.

- **Hash-partitioned left producer (build) SIGKILL.**
  Invalidate left manifest; rerun build task; cascade
  invalidation to dependent join consumers; redispatch
  after rebuild. Right manifests unaffected.
- **Hash-partitioned right producer (probe) SIGKILL.**
  Symmetric.
- **Hash-partitioned both upstream workers lost.** Both
  producer stages trigger invalidation in parallel; join
  tasks wait for both via multi-upstream barrier.
- **Broadcast build-publish worker SIGKILL.** Build
  manifest invalidated; rebuild; all probe consumers
  redispatch.
- **Join-worker SIGKILL (either strategy).** Retry on
  different worker per ADR 003; upstream manifests
  unaffected.
- **Build-side memory exhaustion.** Hash spill engages
  (§7).
- **Build-side spill exhaustion.** Terminal
  `BudgetExhausted { dimension: SpillBytes }`.
- **Broadcast cap exhaustion at dispatch.** Terminal
  `BudgetExhausted { dimension: BroadcastReplication }`.

Stale partition pulls from old attempts surface as
partition-store mismatch (ADR 003 `RuntimeError::
PartitionStoreMismatch`), not as join-specific task
failure variants.

### 9. Determinism

Reuses ADR 003 §13 canonical normalization unchanged:
concat → stable lex full-row sort → reslice (65,536 rows)
→ digest. The join operator does not internally sort
within a partition — operator-internal sort would be a
fake stability promise, canonicalizing only within
partition, not across. The full-row sort at digest time is
the contract.

The new non-determinism sources within joins:

- Hash-bucket iteration order during probe.
- `FullOuter` end-of-probe emission of unmatched build
  rows (V1.x; not in ADR 004's `Inner` scope but the same
  rule applies).
- Partition arrival order at join tasks.

All subsumed by the canonical normalization. Cross-replica
determinism gate fixture: two independent multi-worker
runs of the same join query (one with build-worker
SIGKILL); both produce byte-identical normalized digests
for each strategy.

### 10. Wire surface

**Zero new `MessageKind` variants.** All join traffic uses
ADR 003 lifecycle messages (`StartTask`, `TaskStarted`,
`TaskBatch`, `TaskComplete`, `TaskFailed`, `CancelTask`,
`TaskCancelled`, `Progress`, `Heartbeat`) and data movement
messages (`PullPartition`, `PartitionData`,
`PartitionMissing`).

Join tasks validate each pulled partition's `ExchangeId`,
`PartitionId`, producer `StageId`, and `AttemptId` — same
validation as ADR 003 shuffle consumers. Mismatches surface
as `PartitionMissing { reason: AttemptStale }` on the wire
and as `RuntimeError::PartitionStoreMismatch` on the
coordinator side.

### 11. Architecture guards G16-G18

Supplementing ADR 002's G1-G7 and ADR 003's G8-G15.

- **G16. No non-pinned join algorithms.** Grep
  `src/exec/**` and `src/distributed/**` for identifiers
  matching `SortMergeJoin`, `NestedLoopJoin`, `CrossJoin`,
  `SemiJoin`, `AntiJoin`, `OuterJoin`, `LeftJoin`,
  `RightJoin`, `FullJoin`. None appear. Permitted
  identifiers: `Join`, `HashPartitioned`, `Broadcast`.
- **G17. `ExecutionPlan::Join` is the sole join variant.**
  Compile-time enum exhaustiveness. `JoinStrategy` has
  exactly two variants. Adding a third requires a new ADR.
- **G18. Multi-upstream stage barrier.** Scheduler reads
  `StagePlan::upstream_stages: Vec<StageId>`. Grep guard
  against `upstream_stage: Option` reappearing on
  `StagePlan`.

## Rationale

### Why two strategies in V1, not one

Hash-partitioned is the correctness baseline — it handles
arbitrary join cardinality and composes cleanly with ADR
003 shuffle. But star-schema workloads with a small
dimension table waste shuffle bytes on the dimension side.
Broadcast is a cap-gated optimization for those cases: the
small build publishes once, every consumer pulls it. With
explicit caps and dispatch-time validation, the failure
mode (replication storms, cluster-wide memory pressure) is
closed.

Two strategies is the smallest V1 surface that handles
both correctness and the common small-dim optimization
case. Adding sort-merge, broadcast-right, or nested-loop
in V1 would dilute the contract; those are their own future
ADRs.

### Why hash spill in scope

V1's "First Query with Integrity" promise requires joins
that work on realistic data sizes. Most analytical joins
have build-side cardinality that exceeds
`memory_per_task_bytes = 64 MiB`. Without spill, every such
join fails terminally on `BudgetExhausted { Memory }` —
which means ADR 004 wouldn't ship a usable join contract.
With encrypted spill via the M10 substrate and a random-
access bucket index, large-build joins succeed gracefully;
spill-cap exhaustion fails typed.

The spill index is operator-local (random-access by
bucket), distinct from the partition store's streaming
index. Same encryption substrate; different lookup
pattern. The contract addition is small — "build-side
overrun spills cleanly; spill-cap exhaustion fails
terminal" — and earned by realistic workload viability.

### Why explicit pre-shuffle `Exchange` in the bound plan

Three reasons hold:

1. **Determinism.** Partition count is part of the bound
   plan and gets pinned by the descriptor fingerprint.
   Implicit shuffle (fragmenter synthesizes `Exchange`)
   would make partition count a fragmenter choice — easy
   to make non-deterministic by accident.
2. **Printability.** `ExecutionPlan` debug output shows
   the actual stage structure. Readers don't have to
   mentally re-fragment to understand what runs.
3. **Fragmenter mechanicalness.** The fragmenter never
   synthesizes operators; it cuts stages at existing
   `Exchange` boundaries. Adding a new operator with
   shuffle needs doesn't change the fragmenter — only the
   planner.

### Why permit duplicate output column names

The bound IR uses `ColumnOrdinal` as identifier; column
`name` is metadata. Rejecting duplicates at the binder
forces every join over tables sharing column names (`id`,
`created_at`, `updated_at` — common) to require explicit
`Project` renames before binding. SQL-surface naming
discipline lives in the SQL planner: top-level `SELECT id
FROM a JOIN b ON a.id = b.id` fails with `AmbiguousColumn`
from the SQL planner; qualified `SELECT a.id` succeeds.
PostgreSQL and DuckDB use this same architectural
division.

### Why build = left across both strategies

Convention consistency across strategies. Hash-partitioned
V1 default is build = syntactic left (cost-based swap
V1.x). Broadcast is a caller-discipline contract: the
caller requesting broadcast must place the small input on
the left. Either way, "left = build" is the rule
downstream code can rely on. V1.x cost-based-swap ADR adds
a `BuildSide` field on `ExecutionPlan::Join` when
statistics enable the swap; until then, the field would be
a single-value `BuildSide::Left` reserved for V1.x —
convention forbids the reservation.

### Why programmatic strategy selection in V1

Without a cost model, statistics-driven strategy selection
isn't possible in V1. The contract has to pin who picks:
SQL surface defaults to safe (`HashPartitioned`);
programmatic callers opt into `Broadcast` deliberately
when they have cardinality knowledge. This matches Spark's
`broadcast()` hint pattern at the programmatic level. V1.x
adds SQL hint syntax (`/*+ BROADCAST(t) */`) and later
statistics-driven selection.

### Why no operator-internal sort

ADR 003 §13 canonical normalization handles all sources of
non-determinism uniformly. An operator-internal sort would
be a fake stability promise — only canonicalizing within
partition, not across partition. The full-row sort at
digest time is the contract.

### Why one `Join` variant with strategy field

Two alternatives were considered:

- **`HashJoin` + `BroadcastJoin` sibling variants.**
  Splits the join contract across two enum variants
  duplicating most fields; renaming when a third strategy
  lands.
- **`Join { strategy }` field-discriminated** (selected).
  Single variant; strategy is metadata. Two-variant
  `JoinStrategy` is well-formed (not the premature
  single-variant pattern forbidden by
  `no-premature-backward-compat.md`).

The single-variant shape scales cleanly to V1.x (adding
a strategy variant) without IR-level renaming.

### Why `BroadcastBuildSidePlacement` is structural

The binder cannot infer the caller's intended "small build
input" without statistics (Toron has no cost model in V1).
The check is therefore not "we detected the build was
placed on the wrong side" — it's strictly **structural**:
under `JoinStrategy::Broadcast`, the **left** subtree must
terminate in the single-partition publish shape
(`Exchange { partition_count: 1 }` or `BroadcastPublish`).
If the right subtree carries that shape instead, the
binder rejects with `BroadcastBuildSidePlacement`. The
error name reflects the **caller-discipline contract**
(build = left) rather than implying the binder can
distinguish "small" inputs from "large" ones. Callers who
want statistics-driven build selection wait for V1.x
cost-based-swap ADR.

### Why ADR 003 telemetry residual ships separately

The four ADR 003 watch counters (pulled shuffle bytes,
retry count, stale rejection count, cancellation cleanup
count) are ADR 003 substrate work, not ADR 004 contract
work. Bundling them obscures ADR 004 promotion evidence;
commit boundary cleanliness wins. Ships as its own
`chore:` slice before ADR 004 feature commits.

## Consequences

**Positive:**

- The executor question (does buffered execution carry
  full-cardinality probe?) becomes measurable; ADR 005
  scope decision gains evidence.
- Qualifier-aware columns unblock every V1.x multi-input
  operator (UNION, subquery, CTE, derived tables).
- Hash spill makes large-build joins viable on realistic
  data sizes.
- Bounded broadcast covers small-dim workloads without
  shuffle waste.
- Single `Join` variant with strategy field scales cleanly
  to V1.x strategies.
- ADR 003 substrate (shuffle, producer-loss replay, strict
  barriers, partition store, canonical normalization)
  carries the join contract without modification beyond
  the mechanical `StagePlan` rename.

**Costs:**

- `Expr::Column(String) → Expr::Column(ColumnRef)`
  refactor ripples through every plan-building site (SQL
  planner, Rune engine, tests, benches).
- Binder grows a multi-scope resolver + pair auto-swap.
- Fragmenter handles two strategy shapes (3-stage hash,
  2-stage broadcast) + multi-upstream rename.
- Scheduler strict-barrier check iterates a `Vec`.
- New `hash_spill` module with random-access bucket-page
  index.
- New `BudgetDimension::BroadcastReplication` variant.
- Build-side memory pressure is a new failure mode V1
  handles gracefully via spill (not just terminal fail).

## Promotion criteria

Bench artifact records hard / soft gates. Hard gates block
promotion; soft gates can fail with recorded justification
(Wendelon ADR 070 precedent).

### Hard gates

1. **SQL planner lowers two-table `INNER JOIN ... ON`**
   with aliases on both sides into `LogicalPlan::Join
   { kind: Inner, strategy: HashPartitioned }`. Bare `JOIN`
   (sugar for `INNER`) accepted.
2. **Programmatic broadcast.** Non-SQL plan construction
   (Rune, tests, benches) can request
   `JoinStrategy::Broadcast`.
3. **Qualifier-aware binding.** Binder accepts qualified
   refs against correct scope; rejects ambiguous
   unqualified refs typed; allows duplicate output names;
   binds key ordinals side-correctly with auto-swap on
   inverse-ordered pairs.
4. **Binder typed rejections** (combined gate, ten cases):
   non-equi `ON` (`JoinPredicateNotEqui`), key-type
   mismatch (`JoinKeyTypeMismatch`), floating-point key
   (`JoinKeyTypeUnsupported`), computed-expression key
   (`JoinComputedKeyUnsupported`), ambiguous unqualified
   column (`AmbiguousColumnRef`), unknown qualifier
   (`UnknownTableRef`), hash-partitioned partition-count
   mismatch (`JoinPartitionCountMismatch`), key-exchange
   shape mismatch (`JoinKeyExchangeMismatch`), broadcast
   build placed on right rather than left
   (`BroadcastBuildSidePlacement`), broadcast input
   statically known to exceed caps at bind
   (`BroadcastCapExceededAtBind`).
5. **Bound dispatch.** Workers receive
   `ExecutionPlan::Join` via `TaskSpec`; no
   `LogicalPlan::Join` crosses the wire (ADR 003 G12 holds;
   new fixture for join).
6. **Hash-partitioned correctness.** 10M × 1M `INNER JOIN`
   at 4 workers byte-identical to in-process baseline
   after canonical normalization. Same at 10M × 10M, 8
   workers.
7. **Multi-column keys.** 10M × 1M on a `(region,
   customer_id)` composite key partitions deterministically
   and matches correctly.
8. **NULL-key semantics.** 4-cell fixture (build-null,
   probe-null, both-null, neither-null) digest-equal to
   reference row-level join.
9. **Multi-upstream stage barrier enforced.** Join stage
   does not dispatch until both upstreams are
   `LiveAccepted`. Architecture guard G18.
10. **Hash-partitioned left-producer SIGKILL replay.**
    Build worker killed mid-shuffle after manifest
    accepted; manifest invalidated; rebuild; downstream
    join consumers redispatch; final digest byte-identical
    to no-failure baseline.
11. **Hash-partitioned right-producer SIGKILL replay.**
    Symmetric.
12. **Join-worker SIGKILL retry.** Join task retried on
    different worker; upstream manifests unaffected; final
    digest byte-identical.
13. **Broadcast under-cap correctness.** Small-build
    `INNER JOIN` with `strategy: Broadcast` byte-identical
    to hash-partitioned baseline result.
14. **Broadcast over-cap terminal failure.** Build
    manifest actual size > `broadcast_replicated_bytes_per_query`
    at dispatch fails typed `BudgetExhausted { dimension:
    BroadcastReplication }`. No silent strategy switch.
15. **Broadcast build-publish SIGKILL replay.** Build
    worker killed; manifest invalidated; rebuild; all
    consumers redispatch; final digest byte-identical.
16. **Broadcast probe scan-share determinism.** Two
    independent broadcast runs produce identical per-worker
    probe-share assignments.
17. **Build-side spill engagement.** Fixture sized to
    exceed `memory_per_task_bytes` triggers promotion to
    encrypted spill; probe completes against spilled
    buckets; final digest byte-identical to in-memory
    baseline.
18. **Build-side spill exhaustion.** Fixture sized to
    exceed `spill_bytes_reserved_per_query` fails typed
    `BudgetExhausted { dimension: SpillBytes }`
    (terminal).
19. **Cross-replica determinism.** Two independent multi-
    worker runs (one with SIGKILL) produce byte-identical
    normalized digests for **each** `JoinStrategy`.
20. **Pull-addressed shuffle on both sides.** Joins use
    `PullPartition` / `PartitionAddress`; no peer-push.
    Broadcast publishes via partition store, not inlined
    in `StartTask` (architecture-guarded).
21. **Architecture guards G16-G18 pass.**

### Soft gates

Threshold values are Phase 0 calibration targets. Recorded
but not blocking promotion.

- **Throughput scales with worker count** (hash-
  partitioned). 10M × 1M wall-clock at 2/4/8 workers;
  target ≥ 1.5× speedup per doubling.
- **Per-side shuffle bytes recorded** (hash-partitioned).
- **Broadcast replicated bytes recorded** (broadcast).
- **Build-side rows, hash peak memory, spill bytes, spill
  cache hit rate recorded.**
- **Per-query join counters recorded** (build rows, probe
  rows, matched rows).
- **Skew observation.** A 90%-skew fixture records per-
  partition row counts. No mitigation required in V1.
- **Executor evidence row.** 10M × 10M at 8 workers wall-
  clock gives the council data to decide ADR 005 scope
  (executor model upgrade vs alternatives).
- **ADR 003 single-exchange GROUP BY regression-free.**
  Existing ADR 003 fixture still passes byte-identically
  after `StagePlan` rename.

## Resolved questions

Iteration-resolved questions distilled into the V1
contract.

### Q1 — Hash-only vs hash+broadcast

**Hash-partitioned baseline + bounded broadcast.** Both
strategies pinned in V1; broadcast is cap-gated and admits
through three explicit budgets. Hash-only was considered
(narrower scope) and rejected because star-schema
workloads waste shuffle bytes on the small-dim side without
broadcast.

### Q2 — Build side designation

**Syntactic left = build for both strategies.** Hash-
partitioned: V1 default; cost-based swap is V1.x.
Broadcast: caller-discipline contract; callers requesting
broadcast must place the small build input on the left.
The `LogicalPlan::Join.left` field is the build side under
both strategies; no `BuildSide` enum in V1.

### Q3 — Operator-state hash spill in scope

**Yes, in scope.** M10 AES-256-GCM-SIV substrate; random-
access bucket-page index distinct from ADR 003 partition-
store streaming index. One-way promotion per task. Without
spill, realistic large-build joins fail terminal — V1's
"First Query with Integrity" requires graceful handling.

### Q4 — Runtime / bloom filter pushdown

**Deferred entirely.** Cross-ring contract (build-side
distinct-key collection → coordinator → Wendelon
`ScanStream` attachment) deserves its own ADR. Soft-gate
evidence here would add coordinator plumbing without a
contract decision. Future ADR when join bench shows
filter-side reduction matters.

### Q5 — Duplicate output column names

**Permitted at bound schema; SQL planner enforces SQL-
surface naming discipline.** The bound IR uses
`ColumnOrdinal` as identifier; downstream operators
disambiguate by ordinal. Top-level SQL ambiguity
(`SELECT id FROM a JOIN b ON a.id = b.id`) fails at the
SQL planner with `AmbiguousColumn`; explicit qualification
(`SELECT a.id`) succeeds.

### Q6 — Strategy selection mechanism

**Programmatic in V1.** SQL planner defaults to
`HashPartitioned`. Programmatic callers (Rune, fixtures,
benches, external APIs) emit `JoinStrategy::Broadcast`
when they have cardinality knowledge. SQL hint syntax
(`/*+ BROADCAST(t) */`) and statistics-driven selection
are V1.x.

### Q7 — ADR 003 telemetry residual bundling

**Separate `chore:` slice.** Ships before ADR 004 feature
commits; commit boundary cleanliness wins.

## Deferred (V1.x or later)

- **Outer joins** (LEFT / RIGHT / FULL OUTER). Own ADR
  pinning preserved-side null-fill semantics.
- **Semi / anti joins.** Own ADR.
- **Cross joins.** Own ADR or V1.x.
- **Non-equi joins / theta-joins / residual `ON`
  predicates.** Own ADR.
- **Sort-merge join.** Requires ordering contract (no
  ORDER BY semantics pinned in Toron yet).
- **Multi-way join planning.** Requires cost model.
- **Cost-based build / probe swap.** Requires Wendelon
  stats surface and Toron cost model. Will land
  `BuildSide` field on `ExecutionPlan::Join` when it
  engages.
- **Cost-based strategy selection.** Requires same.
- **Bloom / runtime filter pushdown.** Own cross-ring ADR
  with Wendelon. Pins false-positive rate, hash seed,
  byte budget, descriptor-fingerprint interaction,
  unpushable-filter behavior.
- **Skew mitigation.** Observation in V1; mitigation
  (heavy-hitter detection, salt-and-replicate, dynamic
  partition-count override) is own ADR.
- **`USING` clause / comma-join.** SQL surface sugar.
- **Floating-point join keys.** Own ADR pinning canonical
  encoding (NaN, signed zero, subnormals).
- **SQL join hints** (`/*+ BROADCAST(t) */`).
- **Intra-task build / probe pipelining.** V1.x executor
  work.
- **Hash-spill-to-memory demotion.** One-way per task in
  V1.
- **SQL-surface auto-rename of duplicate output column
  names.** V1 requires explicit qualification.
- **Third or further `JoinStrategy` variants.**

## Rejected alternatives

- **Hash-only V1.** Too narrow; misses common star-schema
  optimization. Bounded broadcast covers without giving
  up the hash-partitioned correctness baseline.
- **Unbounded broadcast.** Replication storms; caps are
  non-negotiable.
- **Broadcast as planner heuristic without explicit cap
  budget.** Hides cost-model absence; explicit caps +
  programmatic selection make the contract precise.
- **Inline broadcast payload in `StartTask`.** Conflicts
  with ADR 003 pull-addressing.
- **Operator-state spill deferred to V1.x.** Realistic
  large-build joins must work; spill correctness is part
  of the contract.
- **Right = build.** Conflicts with build=left consistency
  across strategies; reverses the visual order of the
  `left` / `right` plan fields.
- **Cost-based strategy selection in V1.** No cost model
  exists; user-set strategy + dispatch validation is the
  V1 contract.
- **Separate `HashJoin` and `BroadcastJoin` execution
  variants.** Single `Join` variant with strategy field
  is more compact and scales cleanly to V1.x strategies.
- **`BuildSide` enum field on `Join` in V1.** Would be a
  single-value `BuildSide::Left` reserved for V1.x.
  Convention forbids reservation.
- **Reject duplicate output column names at the binder.**
  Conflates SQL-surface naming with bound-IR ordinal
  semantics; SQL planner is the right layer.
- **`JoinSide { Left, Right }` tag on `ResolvedColumn`.**
  Side is structural (encoded in `left_keys` /
  `right_keys` field names), not metadata.
- **Bloom pushdown as soft evidence row in ADR 004.**
  Cross-ring contract; own ADR with its own bench
  evidence.
- **Sort-merge / nested-loop / cross join in V1.** Each
  is its own future ADR.

## What Not To Do

- **Do not dispatch `LogicalPlan::Join` to workers.**
  Coordinator binds; workers receive `ExecutionPlan::Join`
  via `TaskSpec` (ADR 003 G12 holds).
- **Do not bind aliases or columns on workers.** Binding
  lives in the coordinator under the pinned fingerprint.
- **Do not synthesize `Exchange` in the fragmenter.** The
  planner emits explicit `Exchange` above each `Join`
  input.
- **Do not allow non-equi `ON`.** Binder rejects with
  `JoinPredicateNotEqui`.
- **Do not allow implicit type coercion on join keys.**
  Binder rejects.
- **Do not allow floating-point join keys.** Binder
  rejects.
- **Do not allow computed-expression keys.** Binder
  rejects.
- **Do not reject duplicate output column names at the
  binder.** SQL planner is the right layer for naming
  discipline.
- **Do not push join partitions directly between
  workers.** ADR 003 G11 holds; consumers pull via
  `PullPartition`.
- **Do not inline broadcast build payloads in
  `StartTask`.** Broadcast uses partition-store publish.
- **Do not let broadcast bypass caps.** Three caps gate
  admission; over-cap fails terminal.
- **Do not silently switch broadcast to hash-partitioned
  mid-attempt.** Strategy is bound, not negotiated.
- **Do not promote a spilled hash table back to memory.**
  One-way per task attempt.
- **Do not share the hash-spill index with the partition-
  store streaming index.** Different access patterns
  (random-access vs streaming).
- **Do not introduce a new `MessageKind` variant.** Join
  reuses ADR 003 wire surface entirely.
- **Do not introduce a new IR level below
  `ExecutionPlan`.** ADR 003 §1 holds.
- **Do not require stable worker assignment for
  determinism.** ADR 003 §13 normalization is the
  contract.
- **Do not bundle ADR 003 telemetry residual into ADR 004
  feature commits.** Separate `chore:` slice.
- **Do not introduce broadcast / sort-merge / nested-loop
  *algorithm* identifiers** beyond the two pinned
  strategies (G16).
- **Do not let `MorselId` cross the wire** (ADR 003 G14
  holds).
- **Do not bundle distributed joins with tenant
  governance, Rune sandbox enforcement, or executor-model
  replacement.** ADR 003 / Wendelon ADR 070 narrow-
  decision pattern.
- **Do not raise `JoinStrategy` past two variants
  without a new ADR.** G17 enforces.

## Council decisions

Compact archival summary of the iter-1 through iter-5
convergence. Iteration history lives in git on
`plans/wip/`.

1. **ADR 004 = distributed inner equi-join.** Three-way
   topics iter-1.
2. **`JoinKind { Inner }` single variant.** Three-way.
3. **`JoinStrategy { HashPartitioned, Broadcast }`** two-
   variant.
4. **`LogicalPlan::Join` + `ExecutionPlan::Join` with
   strategy field.** Three-way (one bound variant scales
   cleanly to V1.x).
5. **Bounded broadcast with three explicit caps; no
   silent strategy switch.** Three-way iter-5.
6. **Build = left across both strategies; broadcast
   caller-discipline places small input on left.** Three-
   way iter-5 (Codex iter-final pinned the broadcast
   caller rule explicitly).
7. **Qualifier-aware `ColumnRef`; ambiguity rejected;
   duplicate output names permitted at bound; SQL planner
   enforces naming discipline.** Three-way iter-3.
8. **Multi-upstream `StagePlan::upstream_stages:
   Vec<StageId>` rename, mechanical.** Three-way.
9. **No new `MessageKind` variants.** Three-way.
10. **ADR 003 §13 canonical normalization is the
    determinism gate; no operator-internal sort.** Three-
    way.
11. **Multi-column keys, hard gate.** Three-way.
12. **NULL ≠ NULL; drop on both sides for `Inner`.**
    Three-way.
13. **Float keys rejected at bind.** Three-way (NaN /
    signed-zero / canonical-encoding need own ADR).
14. **Computed-expression keys rejected at bind.** Three-
    way.
15. **Non-equi `ON` rejected at bind.** Three-way.
16. **Build-side encrypted hash spill in scope** (M10
    substrate, random-access bucket-page index, one-way
    per task). Three-way iter-5.
17. **Runtime / bloom filter pushdown deferred entirely.**
    Three-way iter-5.
18. **Skew observation soft gate; mitigation deferred.**
    Three-way.
19. **ADR 003 telemetry residual ships as separate
    `chore:` slice.** Three-way iter-5.
20. **Strategy selection: SQL planner defaults
    `HashPartitioned`; programmatic callers can emit
    `Broadcast`. SQL hints + cost-based selection are
    V1.x.** Three-way iter-5.
21. **Broadcast probe scan-share assignment:
    `xxh3_64(source_tablet_id) mod worker_count`,
    deterministic.** Three-way iter-5.
22. **Three new architecture guards G16-G18.** Three-way.
23. **Out of scope confirmed:** outer / semi / anti / cross
    joins, non-equi joins, sort-merge, multi-way planning,
    cost-based swap / strategy selection, bloom / runtime
    filter pushdown, skew mitigation, locality routing,
    pipelined cross-stage, float keys, SQL hints, `USING`
    clause, intra-task pipelining, hash-spill demotion,
    SQL-surface auto-rename, third strategy variants.
    Three-way.
