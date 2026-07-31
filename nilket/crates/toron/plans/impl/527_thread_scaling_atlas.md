# impl 527 - partition-work capability gate plan

Attribution only: persist the requested route-local partition-work limit, add one typed O(1)
receipt at its sole executor consumer, and run the fixed-budget pin-1/pin-8 q01-q22 capability
census. No thread-scaling claim, morsel code, executor remedy, or default change.

## Immutable inputs

- tree: epoch-close main (`53f18ebc` lineage), all activated knobs default on;
- dataset: strict sf=8 persisted Parquet, manifest format 2, full-hash;
- exact local substrate: one `Executor::execute_bound` invocation and one shared `TaskBudget` per
  query (`toron-tpch/src/lib.rs:4343,4658`);
- exact control reach: `ExecutionContext::partition_parallelism` has one production consumer,
  `parallel_partition_plan` in the partitioned spilled scan-build path
  (`exec/join/streaming.rs:18186`);
- fixed budgets in both arms: memory 2,147,483,648 B; spill reservation 2,147,483,648 B;
- the frozen 3,554 ms floor is not an input; capability, not materiality, is being tested.

## Step 1 - provenance and typed execution receipt

1. Add a loud harness accessor for `TORON_PARTITION_PARALLELISM`: positive integer or error;
   absent resolves through the engine default. Persist the resolved value and `explicit`/`default`
   provenance as `partition_work_concurrency_requested` on Toron perf rows; peer-engine rows carry
   null. A knob-contract test pins valid, invalid, absent, and JSON-row behavior.
2. Add a serde-defaulted `PartitionWorkConcurrencyObservation` to `TaskTelemetry`, containing:
   replay id, the partitioned streamed scan-build route, partition count, requested concurrency,
   planned `K`, resident peak, memory budget, tasks launched, and tasks completed. Emit once per
   runtime after all launched handles reconcile; the sequential fallback emits `K=1` with its
   completed work count. No per-row/per-batch clocks and no code motion.
3. Surface the typed observations in Toron perf rows. Tests pin backward-compatible telemetry
   deserialization, `K=1`, `K>1`, budget-limited `K`, launched/completed equality, and absence on an
   unrelated in-memory join. Architecture guard changes, if any, name ADR 527 precisely.

## Step 2 - seal and gates

Commit `impl: adr 527 partition-work capability receipts`, run the standard full battery with
direct exit statuses, push, and build one release binary from that immutable identity. This is an
observability seal; no executor decision or budget arithmetic changes.

## Step 3 - fixed-budget capability census

Run two Toron-only q01-q22 censuses from the sealed binary:

- arm 1: `TORON_PARTITION_PARALLELISM=1`;
- arm 8: `TORON_PARTITION_PARALLELISM=8`;
- both: explicit 2 GiB memory and explicit 2 GiB spill reservation, strict sf=8 persisted Parquet,
  full-hash, one run and no warmup, join-spill scratch cleared before each child.

For every cell require equal fingerprints, digests, row counts, and successful status across arms.
Classify from receipts only:

- `uncontrolled`: no partition-work receipt in either arm;
- `controlled_sequential`: receipts exist and maximum planned `K` is 1;
- `controlled_parallel`: pin-8 has planned `K > 1`, with launched = completed.

The receipt table records both budgets, requested pin/provenance, route, partition count, planned
`K`, resident peak, launched/completed tasks, spill, and identity facts. Execute times are
diagnostic only and carry no scaling authority.

## Step 4 - predeclared verdict and receipt

If any q-cell is `uncontrolled`, close ADR 527 red as a general thread-scaling atlas. Append the
matrix and exact controlled set here; do not run DuckDB, pins 2/4, repeated cohorts, efficiency
statistics, or straggler analysis. The sealed observational fields remain only if the standard
gates and census prove them behavior-neutral; otherwise revert them and commit a documentation-only
red receipt.

Only an all-controlled matrix may continue, and even then the plan must be amended with the real
query-wide control and fixed decision thresholds before a scaling sweep. No result from Step 3 can
silently authorize that sweep.

## Repository gates

Formatting in both crates; all three `--all-targets` checks; Toron library tests; full
`architecture_test`; complete Toron `--no-fail-fast`; full `toron-tpch`; `git diff --check`.
Direct exit statuses and full logs preserved, with the full battery repeated after any code change
made in response.
