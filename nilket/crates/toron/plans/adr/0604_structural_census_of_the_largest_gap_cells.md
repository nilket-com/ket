# ADR 604 - a structural census of the largest gap cells, Toron beside DuckDB

**Status:** PROPOSED. Successor to ADR 603, whose sealed anchor on strix
reads Toron 63.430 s against DuckDB 15.650 s (4.053×), 32.064 s above the
campaign's 31.366 s target. ADR 603's two memory panels did not
materially narrow the aggregate gap: 4.062× with DuckDB at a nominally
matched 2 GiB, and 4.037× with Toron at 32 GiB per task. They are context
here, not a replacement anchor. The anchor ranks five cells first by
absolute gap: q21 (5,846 ms), q09 (5,479), q05 (4,689), q18 (4,547), and
q01 (3,700). Together they are 24.3 s of the 47.8 s gap. This record
measures what each engine does on those five cells, so that the next
owner is chosen from evidence about structure, not from wall time alone.
It changes no engine code and retains no gain.


## Context

**What is known about these cells, and what is not.** q21 has had nine
records (ADRs 592–602) on its replay-1 build and probe. Its fifteen-term
attribution puts runtime preparation of inner 1 (981 median-scaled ms)
and the semi and anti writes (847 and 699) first, and the anchor shows
it spilling 27.2 MB at both 2 GiB and 32 GiB. For the other four cells
the records are older and Toron-only. ADR 507's node-attribution pilot
at SF8 put q09 at 95% aggregate and q18 at 65% join and 17% exchange.
ADR 441 at SF10 put q18 at 78–82% scan and spill. ADR 443's q05 gate
failed with its leading leaf split between reservoir probe and partition
spill write. ADR 538's timing atlas compared local with distributed
execution, not with DuckDB. The July re-pin's reading, that the gap is
"composition — plan shape, spill — not scan, decode, or arithmetic
kernels", was an inference from wall totals. No record has put Toron's
plan and per-operator cost beside DuckDB's for any of these five cells,
and none has split q01, the one join-free cell, into scan and decode,
expression, and aggregation.

**What exists to measure with** (harness `toron-tpch`, engine `toron`):
- `perf-run-one --query <q>` at SF8 parquet, with
  `TORON_NODE_ATTRIBUTION_PROFILE_PATH` (`exec/node_attribution.rs`):
  exclusive time, calls, and spill bytes per node kind (scan, filter,
  join, aggregate, exchange, sort, other), with an outer reconciliation
  against `execute_ms`. It aggregates by kind, not by plan node, and
  records no row flow.
- `TORON_TPCH_PARQUET_SCAN_LEDGER`: scans, batches, rows, decode
  microseconds, and raw bytes.
- The binder's `EXPLAIN ANALYZE` path (`binder/explain.rs`,
  `optimizer_analyze_schema`): the join decisions and the join order,
  with estimated and actual rows per join.
- `compact_execution_plan_shape` and the serializable `ExecutionPlan`
  (`lib.rs`, `ir/execution.rs`): the physical plan's shape and full tree.
- DuckDB 1.5.4's JSON profiler, as ADR 603's sweep uses it, which with
  its operator metrics enabled reports the operator tree with each
  operator's type, timing, and cardinality.

Nothing yet runs these together for one query, or emits them as one
artifact beside DuckDB's.


## Decision

**One diagnostic census artifact per cell, for q21, q09, q05, q18, and
q01, at the sealed anchor's coordinate. Each puts Toron's plan, join
cardinalities, per-kind cost, and scan ledger beside DuckDB's profiled
operator tree. The results then read, per cell, where the evidence
places the gap, and say so when it does not isolate one.**

*The harness change: `structural-census --query <q>`.* One command per
cell that runs at the anchor's coordinate (SF8 strict parquet, 2 GiB
task memory and spill reserve, partition parallelism 1, affinity `0-15`,
DuckDB at one thread with its default limit) and writes one JSON
artifact. Every execution is named, and each fact is attributed to the
execution that produced it.

- **Toron, T1: the `EXPLAIN ANALYZE` execution,** run locally through
  `QueryCoordinator` on `EXPLAIN ANALYZE <sql>` under the harness's
  planner configuration (the distributed variant plans differently and
  is not used). It executes the query, as a separate execution, and
  discards the result. It supplies estimated and actual rows **only for
  the joins it recognizes as a star, snowflake, galaxy, or bridge
  order**; every other join in the cell is reported by name as
  `not_emitted`. (Amended before implementation: a code read found that
  the analyze rows cover only those join orders, and a non-star join
  yields none.)
- **The plan, without an execution of its own:** the same logical plan
  bound by the explain binder (`bind_with_explain_summary`) under the
  same configuration. It supplies the plan's fingerprint, compact shape,
  and serialized tree, and the binder's join decisions and join order,
  with **estimated** rows where the binder has them and
  `estimate_unknown` otherwise. (Amended before implementation: an
  SF 0.01 smoke found the binder's join-order edges carrying no
  estimate.)
- **Toron, T2: the profiled execution,** with node attribution and the
  scan ledger. It supplies the per-kind exclusive time, calls, spill
  bytes, the outer reconciliation against its own `execute_ms`, the
  ledger's rows, batches, decode time, and bytes, and the result digest.
  The plan's compact shape and serialized tree come from the plan T2
  runs, without an execution of their own.
- **DuckDB, D0: plain `EXPLAIN`.** Plan text, with no execution.
- **DuckDB, D1: the profiled execution,** with operator metrics enabled.
  It supplies the operator tree (type, `OPERATOR_TIMING`, `CPU_TIME`, and
  cardinality per operator), the query's `LATENCY`, and the result
  digest.

T1, T2, and D1 are diagnostic executions (D0 does not execute), run in
a separated window with no timed artifact in it.

*Plan correspondence.* Neither T1 nor T2 returns its bound plan. The
explain binder's fingerprint must equal T2's `plan_fingerprint` and the
ADR 603 anchor's Toron `plan_fingerprint` for the cell before T1's rows
are combined with T2's costs. If they differ, the mismatch is named and
the join comparison for that cell is left unresolved. This
correspondence is indirect: it rests on the explain binder and T2
agreeing, not on T1's own plan, and no exact T1–T2 plan identity is
claimed. Likewise, D0's plain `EXPLAIN` is descriptive and D1's
operator tree is the executed plan. Their correspondence is checked, D1
takes precedence, and a D0 that does not correspond is labelled
unmatched. D0 and D1 are compared by `(depth, operator name)` in
pre-order. A match there is a shape-and-name match, weaker than exact
plan correspondence, since sources and predicates are not compared, and
it is labelled so. None is compared with a board median, and nothing
here touches the sealed anchor's timing.

*The anchor, bound by content.* The census reads the sealed anchor only
after verifying its `perf_board.jsonl` against the sealed SHA-256
(`d4044c40cfe517085ec5754b4de76ef647bdbaf29cd3294c325496a9249c3273`),
and records that identity. The coordinate compared with the sealed
header is a declared selection of its fields (the board's coordinate,
plus observed affinity and dataset verification), not every header
field.

*Comparability.* A cell enters the reading only if T2 passed, T2's
digest matches the anchor's Toron digest, D1's digest matches the
anchor's DuckDB digest, and D1 read back the same default memory limit
as the anchor's DuckDB rows. A cell that fails any of these is retained
in the record with its reasons named, and left out of the reading.

*Identity.* Each engine's diagnostic result is tied to its own timed
result on the sealed anchor. T2's digest is compared with ADR 603's
timed Toron digest for the cell (the anchor's rows carry an
`ArrowIpcDigest` per cell), and D1's `duckdb_result_sha256` with ADR
603's timed DuckDB digest. A cell whose diagnostic digest does not match
its own engine's timed digest is reported and left out of the reading.
Cross-engine result equivalence is **not proved by this record**. Both
engines' row counts are reported side by side, and agreement in count is
not presented as agreement in result.

*The census tables, per cell.* Only buckets the instrumentation
observes, with anything else named as unsplit or residual:
- **(a) Join structure.** Each engine's join order, as a per-join table
  from the binder's join edges. For Toron, the binder's estimated rows
  where present (`estimate_unknown` otherwise), and T1's actual rows only
  where a stable join identity ties an analyze row to a join; a join is
  `not_emitted` when T1 emitted nothing, and T1 rows that cannot be tied
  to a join are `unassigned`, never attached by position; for DuckDB, D1's
  per-operator cardinalities. Toron's rows at joins T1 does not cover are
  unknown in this record. (Amended before implementation: the
  join-input replay ledger is not used for join row flow. It records
  input concatenations, and ADR 584 found none for four of q21's five
  replays although those joins ran. A separate execution to collect
  it for materialization shapes was judged of too little value for
  this census and is not run.)
- **(b) Materialization and volume.** Rows scanned per table from each
  engine, Toron's spill from T2, and rows carried between operators where
  both engines report them.
- **(c) Cost by kind, each engine against its own denominator.** Toron:
  T2's exclusive time per node kind (scan, filter, join, aggregate,
  exchange, sort, other), which reconciles to T2's `execute_ms`, with
  the reconciliation's residual shown; within scan, the ledger's decode
  time. DuckDB: D1's `OPERATOR_TIMING` summed per operator type, grouped
  to the same kinds by a declared mapping, beside D1's `LATENCY`, with
  the signed residual `LATENCY − sum(OPERATOR_TIMING)` shown. A positive
  or negative residual is reported as the difference between the two
  measures, not as uncovered work. `CPU_TIME` is cumulative over the
  operator timings, so per-node `CPU_TIME` is not summed again; the
  root's `CPU_TIME` is recorded as a cross-check. The two
  engines' shares are laid side by side and labelled as coming from
  different denominators and instrumentation; they are not treated as
  directly equivalent. No build/probe split within joins is reported,
  because neither engine's instrumentation here isolates it, and q01's
  time is reported as scan (with decode), filter, aggregate, and other.
  Any finer split needs its own instrumentation in a later plan.
- **(d) Divergences.** The items on which the engines differ most in
  rows or in structure.

*Comparable boundaries, and the 2× triage flag.* Two nodes are
comparable only when they represent the same logical boundary: the same
set of input relations and the same predicate or grouping. At a
comparable boundary, Toron carrying at least 2× DuckDB's rows is raised
as a **triage flag**. Where DuckDB reports no rows and Toron reports
some, there is no finite ratio, and the case is named apart from the
flag. Row counts at unmatched intermediate nodes are
reported as non-comparable, and never used to classify.

*Reading, declared before the runs.* Each cell is read into one of:
- **plan shape:** the join orders differ, or a 2× flag stands at a
  comparable boundary.
- **execution path:** the plans match in order and in volume at
  comparable boundaries, and Toron's time concentrates in a kind or path
  DuckDB's tree does not show (spill, materialization, repartition,
  exchange).
- **kernel throughput, as a candidate only:** plan and volume match, and
  no execution path stands out. This reading cannot be confirmed from
  shares on different denominators; it names the cell as a candidate for
  a later controlled cut or rate measurement.
- **mixed or unresolved:** the evidence does not isolate a cause.

A cell may carry a secondary reading, named as such. The reading is not
a measurement of recoverable time, and it projects no gain.

*What the record hands forward.* For each reading, the cells assigned to
it and their anchor gaps summed. A reading spanning several cells with a
large summed gap is a candidate for an architectural plan record; a
single cell's dominant divergence is a candidate for a scoped cut. The
next plan record makes the choice, reviewed as usual.

*Procedure.* Build on bigfoot, stage and hash-verify on strix, and run
the five cells as one separated launch (ADR 595's procedure, one unit,
the cells in sequence, each with a fresh output directory and DuckDB
temp directory). The artifact's header records the coordinate, which is
checked against ADR 603's sealed header field by field. No timed
artifact runs in the same window. The target coordinate stays the
sealed anchor unless the user chooses otherwise; neither ADR 603 panel
is reinterpreted as a target here.


## Acceptance

- `structural-census` fixtured on a small in-repo query at a test scale:
  - the artifact names T1, T2, D0, and D1 and attributes each field to
    one of them;
  - T2's digest matches `perf-run-one`'s for the same query and data;
  - D1's operator tree and its timings, cardinalities, and `LATENCY`
    parse from a recorded profile;
  - each engine's operator sum and residual are computed against its own
    denominator, DuckDB's as the signed `LATENCY − sum(OPERATOR_TIMING)`
    with per-node `CPU_TIME` not summed;
  - a T1 plan whose shape or fingerprint differs from T2's leaves that
    cell's join comparison unresolved, naming the mismatch; a D0 that
    does not correspond to D1 is labelled unmatched;
  - the coordinate check refuses a header that differs from a given
    sealed header in any field, naming it.
- The operator-kind mapping from DuckDB operator types to Toron node
  kinds, declared as a table in the code and the results, with a fixture
  per mapped type, and unmapped types reported by name rather than
  folded into `other`.
- The comparable-boundary rule fixtured: a matched boundary with 2× rows
  raises the flag; an unmatched intermediate node reports its rows as
  non-comparable and raises nothing.
- On strix, five artifacts, one per cell, under the separated launch,
  each with its diagnostic digests checked against its own engine's
  ADR 603 timed digests, and a mismatching cell reported and left out.
- The results record: the four census tables per cell with the
  denominators and residuals, the reading per cell with its evidence
  (`mixed or unresolved` where it applies), the readings' summed anchor
  gaps, the statement that cross-engine result equivalence is not proved
  here, and the profiling caveat.
- The ladder green on bigfoot (Toron and toron-tpch, fmt clean) on the
  measured tree.
- Plan commit, then one implementation/results/closure commit.


## Guardrails

- No engine code changes; the harness change adds a diagnostic command
  and changes no existing timed path.
- No census number is compared with a board median or read as a saving.
  Cross-engine shares are never read as equivalent, and a kernel
  throughput reading is never read as confirmed.
- The sealed anchor stays the campaign's measure; nothing here reseals
  or replaces it.
- Build on bigfoot, run on strix, the separated launch, and one attempt.


## Risks

- Node attribution aggregates by kind, not by node. Two joins of one kind
  share a line, so per-join detail comes only from T1's and D1's
  cardinalities, and cost stays at kind level.
- If T1 cannot reuse T2's execution, the census runs Toron twice per
  cell. Both runs are diagnostic and named.
- DuckDB's operator taxonomy does not map one-to-one onto Toron's node
  kinds. The mapping is declared and fixtured, and anything unmapped is
  named.
- Many cells may read `mixed or unresolved`. That is a result, and it
  would point the next plan at instrumentation before any cut.
- q21 has nine records of attribution behind it. The census may only
  restate them; that is still worth recording beside DuckDB's tree.


## Forward

- The next plan record chooses its owner from the classes' summed gaps:
  a scoped cut, or an architectural change.
- The RSS-measurement correction remains its own record.
