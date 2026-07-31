# ADR 527 - Partition-work capability gate before a thread-scaling atlas

The single-query-task board is closed at `53f18ebc` with its 3,554 ms floor frozen. The next
doctrinal question is whether morsel-driven scheduling could become evidence-backed, but source
grounding found that the proposed Toron control does not yet mean what a 1/2/4/8-thread atlas
requires.

`TORON_PARTITION_PARALLELISM` initializes `ExecutionContext::partition_parallelism`, but its only
production consumer is `parallel_partition_plan` inside the partitioned spilled scan-build join
path (`exec/join/streaming.rs:18186`). It is a requested upper bound on route-local blocking work,
further limited by available cores and retained footprint. It does not distribute scans,
aggregates, exchanges, ordinary joins, or the query task itself. The TPC-H harness invokes one
local `Executor` with one shared `TaskBudget`; dividing that budget by the requested concurrency
would shrink the query's real memory cap rather than hold aggregate memory fixed.

ADR 527 therefore begins with a bounded **capability gate**, not a scaling sweep. It remains
attribution only: no morsel implementation, operator parallelization, distributed-plan migration,
or scheduling remedy. A true Toron/DuckDB matched-thread atlas is authorized only if the gate proves
that an existing query-wide Toron thread control actually exists.

## Contract

1. **Tree and cells:** current default-on tree; q01-q22 at strict sf=8 persisted Parquet,
   format-2 full-hash.
2. **Control name and provenance:** persist `partition_work_concurrency_requested` and provenance
   on Toron perf rows. Never label it `threads`. DuckDB's
   `TORON_TPCH_DUCKDB_THREADS` remains a real reference-engine thread pin and is not treated as an
   equivalent control.
3. **Fixed budget:** both capability arms keep memory and spill reservation at 2 GiB. No division
   by the requested concurrency is permitted because the local executor owns one shared task
   budget.
4. **Actual reach:** one O(1) execution receipt at the sole consumer records the join replay/route,
   partition count, requested concurrency, footprint-limited planned `K`, and actual partition
   tasks launched and completed. A cell with no receipt is `uncontrolled`, not `K=1`.
5. **Capability arms:** run Toron once at requested values 1 and 8 across q01-q22. Fingerprint,
   digest, row count, spill, budget, requested pin, and execution receipts must be present and
   reconciled before the gate is evaluated. These are reachability rows, not performance evidence.
6. **Historical honesty:** the closed epoch's Toron setting is `unrecorded`. Current defaults or a
   new run cannot reconstruct a past environment. The epoch coordinate is “single query task with
   unrecorded route-local partition concurrency,” while DuckDB's one-thread reference remains a
   disclosed session pin.

## Predeclared capability gate

A general q01-q22 thread-scaling atlas is expressible only if every cell has a query-wide Toron
control whose pin-1 arm reports effective concurrency 1 and whose pin-8 arm can report effective
concurrency greater than 1 without changing the fixed aggregate budget. Route-local receipts do
not satisfy that condition for cells which never execute the route.

**If any q-cell is uncontrolled, ADR 527 closes red as a general thread-scaling atlas.** The receipt
publishes the complete controlled/uncontrolled matrix and any route-local `K` facts, but no
1/2/4/8 sweep, efficiency statistic, straggler verdict, DuckDB comparison, or scheduler conclusion
is authorized. This is a mechanics red: the measurement axis is absent, not evidence for or
against morsels.

If all cells pass — contrary to the grounded one-consumer source map — a follow-up amendment must
name the real query-wide control, predeclare fixed efficiency/straggler thresholds independently
of outcome data, and only then authorize the full atlas. Pin-1 noise may define an indifference
band around fixed thresholds; it may not choose the thresholds.

## Acceptance

1. Requested pin and provenance are persisted on every Toron perf row; non-Toron rows carry null.
2. The route-local receipt is typed, backward-compatible under serde defaults, O(1) per runtime,
   and structurally tested for `K=1`, `K>1`, and budget-limited `K`.
3. Both arms retain explicit 2 GiB memory and spill budgets and share one fingerprint/digest/row
   count per cell; any identity failure stops the gate.
4. The q01-q22 capability matrix distinguishes `uncontrolled`, `controlled_sequential`, and
   `controlled_parallel`, and reconciles launched/completed tasks for every receipt.
5. Standard gates pass from the final tree. No executor behavior, default, planner, or scheduling
   policy changes.

## Guardrails

- `partition_work_concurrency_requested` is an observational name, not a promise of effective
  workers. Actual `K` is separately receipt-backed.
- Plan fingerprints cannot establish control reach: the setting is consumed after bind. Runtime
  receipts carry the decision.
- The frozen 3,554 ms floor is not consulted; this gate tests whether a scaling coordinate exists.
- The closed epoch's receipts stay canonical. No new run rewrites their missing provenance.
- A red gate does not select distributed tasks, stage parallelism, or morsels. Any substrate is a
  separately authorized architecture decision.

## Forward

- Gate green: amend ADR 527 with the query-wide control and the actual 1/2/4/8 atlas protocol.
- Gate red: record that the current local executor cannot express a general scaling atlas. Product
  demand for multicore latency, if any, must authorize a bounded execution-substrate comparison
  before a scheduler design is chosen.
