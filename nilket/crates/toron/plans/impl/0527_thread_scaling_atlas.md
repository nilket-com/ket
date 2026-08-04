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
   host availability, planned `K`, resident peak, the derived retained-footprint budget that
   actually bounds `K`, tasks launched, and tasks completed. Emit once per runtime after all
   launched handles reconcile; the sequential fallback emits `K=1` with its completed work count.
   No per-row/per-batch clocks and no code motion.
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

## Measured outcome - capability gate RED

The observational implementation sealed and pushed as
`impl: adr 527 partition-work capability receipts` (`c54f49a4`). The receipt differs from the
initial sketch in one evidence-strengthening detail: the runtime record carries both host
availability and the derived retained-footprint budget. The 2 GiB task budget is already a perf-row
fact; recording it again as though it directly bounded `K` would have concealed two real limits.

### Seal

All repository gates returned their own direct exit status 0: both format checks; Toron library
tests (1,461 pass); full `architecture_test` (2,095 pass); the complete Toron `--no-fail-fast`
suite; the full `toron-tpch` suite; Toron telemetry, base TPC-H, and Polars-reference
`--all-targets` checks; and `git diff --check`. No architecture guard required reconciliation.

The release binary was built from `c54f49a4` (build receipt `c54f49a477fe`). The two authoritative
artifact roots are:

- `target/tpch/adr527_capability_pin1_c54f49a4`;
- `target/tpch/adr527_capability_pin8_c54f49a4`.

Both census legs returned direct exit 0. All 44 summary rows are `pass`, `full_hash`, release,
explicit 2,147,483,648 B memory, explicit 2,147,483,648 B spill reservation, and explicit requested
partition-work concurrency 1 or 8 as appropriate. Across arms every cell has one fingerprint,
digest, output schema fingerprint, output row count, and spill total. q21 alone spills
27,242,808 B in each arm; every other cell spills zero.

### Exact capability matrix

Receipt counts are runtime facts, not inferred from spill or timing. A zero in both arms therefore
means `uncontrolled`, never `K=1`.

| cell | pin-1 receipts | pin-8 receipts | identity | classification |
|---|---:|---:|---|---|
| q01 | 0 | 0 | equal | uncontrolled |
| q02 | 0 | 0 | equal | uncontrolled |
| q03 | 0 | 0 | equal | uncontrolled |
| q04 | 0 | 0 | equal | uncontrolled |
| q05 | 0 | 0 | equal | uncontrolled |
| q06 | 0 | 0 | equal | uncontrolled |
| q07 | 0 | 0 | equal | uncontrolled |
| q08 | 0 | 0 | equal | uncontrolled |
| q09 | 0 | 0 | equal | uncontrolled |
| q10 | 0 | 0 | equal | uncontrolled |
| q11 | 0 | 0 | equal | uncontrolled |
| q12 | 0 | 0 | equal | uncontrolled |
| q13 | 0 | 0 | equal | uncontrolled |
| q14 | 0 | 0 | equal | uncontrolled |
| q15 | 0 | 0 | equal | uncontrolled |
| q16 | 0 | 0 | equal | uncontrolled |
| q17 | 0 | 0 | equal | uncontrolled |
| q18 | 0 | 0 | equal | uncontrolled |
| q19 | 0 | 0 | equal | uncontrolled |
| q20 | 0 | 0 | equal | uncontrolled |
| q21 | 0 | 0 | equal | uncontrolled |
| q22 | 0 | 0 | equal | uncontrolled |

Controlled set: empty. Uncontrolled set: q01-q22.

Two non-authoritative full-hash route checks sharpen the boundary. q21 with ADR 514 compact sizing
disabled returned its historical 897,713,384 B deferred spill but no partition-work receipt: that
reservoir is a different handler. q17 with ADR 511's swap disabled emitted the older
`join_spill_observations.path = partitioned_streaming_scan_build` fact (622,544,968 B attributed)
but also no partition-work receipt: the broad spill vocabulary names a generic join0 lifecycle,
not proof that the reoriented-chain consumer ran. Source inspection and the focused executor tests
pin the distinction; neither diagnostic participates in the matrix.

### Verdict

**Close red as a general thread-scaling atlas.** The predeclared gate required every q-cell to be
controlled; all 22 are uncontrolled. Requested pin 8 changes no measured q01-q22 executor path, so
these runs have no thread-scaling, efficiency, or straggler authority. Per contract, DuckDB,
pins 2/4, repeated cohorts, efficiency calculations, and straggler analysis were not run.

The observational seal remains: it is serde-backward-compatible, O(1) per engaged runtime,
behavior-neutral under the census identity rails, and prevents future work from mistaking a
route-local request for a query-wide thread control. A future scaling atlas first needs a real
query-wide execution control (or a new scheduler), then a fresh plan with decision thresholds.
