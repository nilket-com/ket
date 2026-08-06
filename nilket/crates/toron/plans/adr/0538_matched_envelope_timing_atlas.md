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

- **Repetitions:** 7 paired runs per cell, plus 1 discarded warmup per arm per cell.
- **Pairing:** adjacent local/distributed pairs with **alternating arm order** across repetitions,
  so drift in machine state cannot accrue to one arm.
- **Estimator:** per-arm median of the 7 elapsed values. The materiality baseline is the
  **control-arm (local) median**, reported explicitly — a pooled median shrinks by the effect it
  is meant to measure.
- **Confidence rule:** a cell is `distinguished` only when all 7 paired deltas share a sign;
  otherwise `indistinguishable`, regardless of median separation. No p-value is manufactured from
  7 samples.
- **Cache/warmup posture:** page cache warm from the discarded warmup; join-spill scratch cleared
  before the run, not between repetitions.
- **Retry policy:** a transport flake surfacing as `QueryError::Io` retries once and is recorded
  on the row; no other retry, and no re-run of a cell whose numbers are disliked.

**3. Every timed child keeps its correctness checks.** Digest, row count, schema fingerprint,
plan shape and clean-build stamp are verified on each timed run. A timed row that cannot show
identity is discarded as invalid, never reported as fast. Timing does not get a weaker bar than
the census that authorized it.

**4. Transport and task overhead are priced from receipts, never inferred.** The distributed arm
pays dispatch, TCP loopback, partition publication and manifest acceptance that the local arm does
not. These are attributed from the receipts already emitted — stage task counts, partition bytes,
shuffle byte counters, per-stage elapsed. **No generic constant is subtracted from any cell.**
An unattributable remainder is reported as unattributed, which is a finding rather than an
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
4. **Overhead attribution reconciles.** For each cell, attributed components plus unattributed
   remainder equal the measured distributed elapsed. No component is a fitted constant.
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

The measured stage shares and transport costs decide the next substrate question, and the branches
are predeclared so the answer cannot be chosen to suit a preferred project:

- **Scan-side dominance** → the next decision concerns `ScanShare` and scan reuse.
- **Dispatch/transport dominance** → the next decision concerns task dispatch and the loopback
  transport, including whether an in-process path is worth its complexity.
- **A single operator dominating across cells** → that operator earns its own attribution ADR.
- **No dominant component, or overhead within noise of the local arm** → the honest outcome is
  that no material substrate work is authorized, and the atlas closes having said so.

Fragmented-plan fingerprint coverage remains an open item from ADR 537 and is not in scope here,
though a timing study that compares plans across arms is a plausible future consumer of it.
