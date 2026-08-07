# ADR 538 - Matched-envelope local/distributed timing atlas

ADR 529 deferred every timing question behind a correctness precondition: its receipt named the
identical set as "the only execution-reachable comparison candidates" and authorized "no timing,
ScanShare, scheduler, or capacity work." That precondition is now met. The sealed census at
`9945e8f73e94` records **21 `executes_identical`, 0 `executes_divergent`**, with q15 the sole
typed fragment rejection — zero correctness findings on the board for the first time since the
census began.

This ADR collects the timing evidence 529 deferred, and nothing else. It is **attribution only**,
in the shape ADR 527 established: no engine behavior change, no ScanShare implementation, no
scheduler or capacity work, no producer parallelism. Its output is a per-cell picture of where
distributed execution spends time relative to the local arm under a matched envelope, and a
predeclared fork that decides what — if anything — becomes the next substrate decision.

The inherited envelope is not re-derived. It is ADR 529's, unchanged through 535, 536 and 537,
and the atlas is worthless if it quietly drifts into a new benchmark.

## Decision

**1. Inherited scope and envelope, stated so drift is visible.**

- Tree: sealed `b39368f9` lineage, clean working tree, clean-build stamp on every timed child.
- Dataset: strict sf=8 persisted Parquet, manifest format 2, full-hash verification.
- Eligible cells: exactly **q01-q14 and q16-q22** — the 21 `executes_identical` cells. **q15 stays
  excluded** as a typed capability wall (`BroadcastProbeNotStarLinear`, never attempted), not as a
  slow or inconvenient cell.
- Envelope: both arms share the ADR 529 aggregate — 2 GiB task-memory envelope and 2 GiB spill
  reservation per query, `max_in_flight_tasks = W`, in-memory partition store.
- Topology: two workers over the production loopback TCP transport. Not an in-process shortcut,
  and not more workers.

**2. Protocol fixed before any measurement.** Every parameter below is declared here so it cannot
be chosen after seeing data:

- **Repetitions:** 7 paired runs per cell, preceded by 2 discarded warmups (one per arm).
- **Pairing:** adjacent local/distributed pairs with **alternating arm order** across repetitions,
  so drift in machine state cannot accrue to one arm.
- **Estimator — paired, not per-arm.** The primary estimators are the **median of the 7 paired
  `distributed - local` deltas** and the **median of the 7 paired `distributed / local` ratios**.
  Per-arm medians are reported as description only and never drive a verdict: taking them would
  discard exactly the drift control the adjacent-pair design pays for. The materiality baseline is
  the control-arm (local) median, reported explicitly — a pooled median shrinks by the effect it
  is meant to measure.
- **Confidence rule:** a cell is `distinguished` only when all 7 paired deltas are **strictly
  positive or strictly negative**. Any zero delta makes the cell `indistinguishable`, whatever the
  medians show. No p-value is manufactured from 7 samples.
- **Cache/warmup posture:** page cache warm from the discarded warmups. Join-spill scratch is
  cleared **before each cell's warmup pair** — not once for the whole atlas — and **not** cleared
  between that cell's 7 repetitions, so every repetition of a cell sees the same scratch posture.
- **Retry policy:** an I/O failure **invalidates the entire adjacent pair**. Both arms are retried
  once, in the same order, and the failed attempt is retained as an invalid receipt. A retried
  distributed child is **never** timed against its original local partner — that would compare
  children from different machine states and silently break the pairing. No other retry, and no
  re-run of a cell whose numbers are disliked.

**2b. Lifecycle and arm order, fixed before measurement.**

- **One process** runs the whole atlas. Per cell, workers and the coordinator are constructed
  **once before the warmups**, retained across all 7 pairs, and torn down after the cell.
- **Workers stay resident during local arms too**, so alternating pairs observe the same host
  footprint rather than a footprint that changes with the arm being timed.
- Query elapsed **excludes** worker startup, dataset verification, digesting and teardown. Each is
  recorded separately. Persistent worker startup is not a per-query substrate cost and must not be
  charged to one.
- Every child gets a **fresh query identity**, and shuffle/task state is reconciled before the next
  child so no residue crosses a boundary.
- **Starting order is fixed by cell parity**, because 7 pairs leave one arm first four times and
  that asymmetry must not correlate with anything: **odd q-numbers start local→distributed, even
  q-numbers start distributed→local.** Warmup order and measured order are recorded on every row.
- **Inter-child cleanup is explicit, awaited, and excluded from query elapsed.** Production
  completion does **not** reclaim shuffle state: `PartitionStore::remove_query` exists but has
  no production caller, and a measured child leaves its partitions resident. With workers held
  across sixteen children, an accumulating store would raise memory pressure as a cell
  progresses — drift correlated with pair index, which the paired design cannot subtract
  because it is not machine noise. The apparatus therefore reclaims each child's query
  namespace explicitly after the elapsed boundary, awaits it, and records it separately as
  inter-child cleanup cost. Retained bytes must return to the post-setup baseline before the
  next child begins. Cleanup runs for successful, failed and invalidated attempts alike, and a
  replacement pair allocates two fresh identities distinct from both identities of the pair it
  replaces.
- **Durable JSONL retains everything**: each child row, each pair row, each invalid attempt, the
  correctness facts, and the raw attribution receipts. Nothing is summarized away at write time.
- `timing_authority=true` is emitted **only** by this rail, and only when **all** of clean-build,
  full-hash dataset verification, quiescent host, unmodified protocol constants and complete cohort
  hold. Any one failing yields `timing_authority=false` and a stated reason.

**3. Every timed child keeps its correctness checks.** Digest, row count, schema fingerprint,
plan shape and clean-build stamp are verified on each timed run. A timed row that cannot show
identity is discarded as invalid, never reported as fast. Timing does not get a weaker bar than
the census that authorized it.

**4a-i. The component taxonomy, frozen before instrumentation.** One taxonomy is shared by
both arms, so a component always names the same thing on each side and contributions are
comparable:

| component | scope |
|---|---|
| `scan` | reading base relations from storage |
| `operator` | **join only** (`scope=join`): build, probe, spill |
| `shuffle` | partitioning, publication and consumption of exchange partitions |
| `dispatch` | task admission, scheduling and manifest acceptance |
| `transport` | loopback TCP encode, send, receive, decode |
| `reduction` | digesting, identity comparison and receipt processing |

These stay distinct. In particular, **no `stage` component may hide concurrent task spans
behind a single summed figure** — a stage bracketing several concurrent operators bounds
where cost sits, not what it is.

**`operator` is scoped to joins for this ADR.** The existing evidence covers join build, probe
and spill only; aggregate, filter, sort and projection are uninstrumented on both arms.
Labelling that partial coverage as generic operator time would let a fraction of execution
compete in the ≥50% leading test as though it were the whole. Non-join execution stays in the
**unattributed remainder and cannot lead a cell**. A future all-operator atlas may extend the
taxonomy on its own evidence.

**Local pairing has three states, and collapsing them is how a zero gets invented.**

- **paired** (`scan`, `operator`, `shuffle`): both arms emit a receipt; a missing local side
  is a defect. **`shuffle` is paired per cell.** q06 — the sole cell whose local plan carries
  no `Exchange` — supplies an **observed local receipt reading zero**, which is evidence, not
  a global exception.
- **declared zero-local** (`transport` only): the local arm structurally cannot incur it.
- **no local equivalent** (`dispatch`): no semantically equivalent local interval exists.
  Distributed dispatch receipts are **retained raw as evidence** and **excluded from paired
  contributions and from dominance**. Without this state the validator would either invent a
  zero or wrongly raise `MissingLocalCounterpart`. Consequently the dispatch/transport
  forward branch may be selected only from **receipt-backed transport**, or from dispatch once
  a genuine local counterpart exists — raw distributed-only dispatch time can never lead a
  cell.

**Phase counters are same-timeline intervals.** A component receipt at task scope claims a
partition of that task's wall clock, so a phase counter may contain only intervals measured on
the task's own execution thread within its wall. The fourth authoritative run falsified the
prior state: the dispatcher's decode of streamed result batches — measured on the coordinator
while the worker's wall was still running — was folded into the task's `batch_decode_micros`,
counting the transfer window at both ends (send worker-side, decode coordinator-side). Eligible
portions exceeded their own task wall on 81 nodes across ten cells (worst: q01's producer,
+921 ms), hidden wherever the coordinator-side remainder was thick enough to absorb it and
exposed by q22's thin one. Consequences: dispatcher-side decode lives in a **non-phase field**
(`dispatcher_batch_decode_micros`), retained raw and never a portion; the worker-side decode of
pulled shuffle inputs is **split out of `shuffle_pull_decode` into `batch_decode_micros`**, so
`transport` remains encode + send + decode with every interval a worker-side wall slice; and
replay verifies the partition **per node** — a node whose eligible portions exceed its own wall
is a violation, not a rounding artifact.

**`transport` is the only structural zero-local component, verified rather than assumed.**
`shuffle` was a candidate and is **refused**: twenty of the twenty-one eligible cells bind a
local plan containing an `Exchange`, which the local executor runs via `bound_exchange` as real
in-memory repartitioning (only q06 lacks one). Shuffle therefore requires a genuine local
counterpart. `dispatch` likewise gets no exception in this cut: rather than declare a zero from
the absence of a scheduler in the local path, it must either carry a local counterpart or stay
unattributed. Declaring a zero is precisely what lets an unimplemented probe claim a whole
delta, so the bar is a verified structural impossibility, not an observed absence.

**`reduction` is out of band and does not participate.** Digesting, identity comparison and
receipt processing occur after the elapsed boundary by construction, so they are recorded as
metadata (`verification_ms`) and are **excluded from arm reconciliation and from dominance**.
No in-bound zero receipt is fabricated for them; the validator must not accept a `reduction`
component among timed contributions.

**A distributed-only receipt cannot produce a paired contribution.** For every component the
atlas emits **both** a local and a distributed receipt, and the contribution is
`distributed − local`. The sole exception is a component whose local counterpart is
**explicitly defined as zero** because the local arm cannot incur it — `transport` is the
motivating case, and any such exception must be declared in this table rather than inferred
at runtime. **A missing local receipt is never treated as zero**: it withholds authority, so
an unimplemented local probe cannot silently inflate a distributed component to 100% of a
delta.

Reconciliation is per arm: a cell's admissible components for one arm, plus that arm's
unattributed remainder, equal that arm's measured elapsed.

**Per-cell distributed values are derived by stage-graph critical path.** The per-task phase
counters partition one task's wall clock and are `non_overlapping` **at task scope only**; a
stage runs several tasks concurrently, so their walls must never be summed. A per-cell figure
is admissible only as a `critical_path` derivation through the stage graph, under these rules:

1. Select **one** task path through the stage DAG, by **total wall duration**.
2. Attribute **only that selected path's** component portions.
3. **Never** take each component's maximum across tasks independently and sum those maxima.
   That recreates the cross-task overcounting the task-scope partition exists to prevent, and
   describes a task that never ran.
4. **Persist the selected stage/task path**, edge by edge, so replay can verify every edge and
   every component basis rather than trusting a derived total.

The local arm applies the same principle over its single execution path. q06's observed local
shuffle zero survives that derivation as evidence; the other twenty cells require real
exchange timing. Each eligible
component emits matching local and distributed records carrying the same scope and basis, so
every contribution is dimensional and replayable.

**4a. Component semantics, fixed before instrumentation.** A component may be compared
against a cell's median paired delta only when its receipt carries all of:

- an explicit **basis** of `non_overlapping` (the interval provably overlaps no other counted
  interval) or `critical_path` (the interval lies on a defined path through the stage graph).
  No other basis is admissible, and a **sum of concurrent task durations is not a basis** —
  it measures work, not wall clock;
- a **paired contribution**: the component's cost in the distributed arm minus its cost in the
  local arm, so it is dimensionally comparable to the paired delta it is being weighed
  against. A distributed-only absolute is not a contribution;
- the **raw receipt fields** it was derived from, retained even when the component cannot
  enter the additive breakdown.

A component failing any clause stays in the journal as evidence but is excluded from the
breakdown and from the ≥50% leading test. A journal whose finalized cells carry no component
receipts at all does not discharge this ADR and must withhold authority
(`attribution_receipts_missing`).

**4. Transport and task overhead are priced from receipts, never inferred, and only added when
additivity is real.** The distributed arm pays dispatch, TCP loopback, partition publication and
manifest acceptance that the local arm does not. These are attributed from the receipts already
emitted — stage task counts, partition bytes, shuffle byte counters, per-stage elapsed.
**No generic constant is subtracted from any cell.**

Per-stage and per-task elapsed intervals **overlap**: tasks run concurrently, so summing their
durations measures work, not wall clock, and would inflate an attribution past the delta it is
explaining. A component may therefore enter the additive breakdown **only** when the receipts
establish either non-overlapping wall-clock intervals or a defined critical path through the
stage graph. **Concurrent task durations are never summed.** Anything that cannot be placed
additively under that rule stays in the unattributed remainder, which is a finding rather than an
embarrassment.

**5. Reporting is per cell.** Per-cell local median, distributed median, delta, ratio, and the
attributed overhead breakdown. **Cells are not summed and no suite-wide winner is declared.**
A geometric mean over 21 heterogeneous cells would describe no query anyone runs. The 3,554 ms
optimization floor from the closed epoch stays frozen and **excluded** — it governed a different
question and importing it would smuggle a stopping rule into an attribution study.

**6. Environmental precondition, separate from methodology.** `nilket-local.service` is currently
active and holds UDP `127.0.0.1:7742` plus several TCP ports. A live deployment is a
timing-authority defect, not a methodology detail: it competes for CPU, page cache and ports.
Before any authoritative run: stop the service, verify 7742 is free, and record the quiescent-host
state on the receipt. Restart it afterwards if Polariton needs it. A run collected without that
record carries no timing authority.

## Acceptance

1. **Protocol compliance is visible on the rows.** Every row carries repetition index, arm order,
   envelope, worker count, transport, build stamp and dataset fingerprint. A reader can tell
   whether the declared protocol was followed without trusting the summary.
2. **Correctness held throughout.** All 21 cells show equal digest, row count and schema
   fingerprint on every timed child. Any cell that loses identity mid-atlas fails the ADR and is
   reported, not dropped.
3. **q15 is absent and labelled** as an inherited capability wall, so its absence cannot be read
   as an omission or a failure.
4. **Overhead attribution reconciles additively.** For each cell, attributed components plus the
   unattributed remainder equal the measured distributed elapsed. No component is a fitted
   constant, and no component enters the sum without a non-overlap or critical-path justification
   from its receipts. A breakdown that only reconciles by summing concurrent durations fails this
   item.
5. **Quiescent host recorded**, with the service stopped and 7742 free before the authoritative
   run.
6. **The fork is answered from the data, not narrated.** The atlas names which of the forward
   branches the evidence supports, including the branch where nothing material is found.

## Guardrails

- No engine behavior change rides this ADR: no ScanShare, no scheduler or dispatcher change, no
  producer parallelism, no store or capacity work, no plan-shape edits. If the atlas motivates
  one, it becomes its own ADR with its own gate.
- No envelope tuning to improve a number. The envelope is inherited precisely so results remain
  comparable to the census lineage.
- No cell exclusion after seeing its result, and no repetition count chosen post hoc.
- No cross-engine claim. This is Toron local against Toron distributed under one envelope; DuckDB
  and the closed epoch's ratios are not the comparison and must not be reintroduced here.
- Elapsed remains diagnostic until this ADR closes; no row from a prior census gains timing
  authority retroactively.

## Forward

The measured stage shares and transport costs decide the next substrate question. Both the branches
and the arithmetic that selects one are fixed here, so a branch cannot be argued into winning after
the numbers are in.

**Definitions, applied in this order.**

- Let **S** be the set of cells classified `distinguished` with a median paired delta **greater
  than zero** — distributed slower than local.
- **If S is empty, the no-work branch wins outright.** A board where distributed is never
  distinguishably slower authorizes no substrate work, regardless of what any breakdown suggests.
- Within a cell of S, a component is **leading** when it accounts for **at least 50%** of that
  cell's median paired delta under the additivity rule above.
- A branch **wins** when its component is leading in at least **two thirds of S, rounded up**, and
  **|S| ≥ 5**. Fewer than five distinguished slower cells is too thin a base to direct substrate
  work, and the no-work branch wins.
- If no component meets that bar, there is **no dominant component** and the no-work branch wins.

**Branches.**

- **Scan-side leading** → the next decision concerns `ScanShare` and scan reuse.
- **Dispatch/transport leading** → the next decision concerns task dispatch and the loopback
  transport, including whether an in-process path is worth its complexity.
- **A single other operator leading** → that operator earns its own attribution ADR.
- **No dominant component, S too small, or S empty** → no material substrate work is authorized,
  and the atlas closes having said so. This is a real outcome, not a failure of the study.

**Independent finding, recorded not fixed: shuffle state is never reclaimed in production.**
`PartitionStore::remove_query` exists but has **no caller anywhere in `crates/toron/src`** —
every call site is inside `partition_store.rs`'s own tests. Measured with the real harness at
sf=0.01: retained shuffle bytes are **0 at the post-setup baseline and 63,432 after a single
child**, with no reclamation on query completion. A long-lived production worker therefore
accumulates one resident namespace per query indefinitely.

This ADR changes nothing about it. The atlas's `cleanup_child` is **repetition isolation after
the response boundary** — a property of the measurement apparatus, required because workers are
held resident across sixteen children — and must not be read as describing or relying on
existing production behavior. Whether production should reclaim is a separate decision with its
own blast radius, and an attribution-only cut is the wrong place to make it.

Fragmented-plan fingerprint coverage remains an open item from ADR 537 and is not in scope here,
though a timing study that compares plans across arms is a plausible future consumer of it.
