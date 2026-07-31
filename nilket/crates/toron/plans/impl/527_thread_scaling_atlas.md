# impl 527 - thread scaling atlas plan

Attribution only: build the 1/2/4/8-thread scaling atlas for q01-q22, Toron and DuckDB at
matched pins, fixed 2 GiB aggregate memory, persisted identity facts, per-cell verdicts from
the predeclared decision tree. No morsel code, no executor remedy, no default change.

## Immutable inputs

- tree: epoch-close main (`53f18ebc` lineage), all activated knobs default on;
- dataset: strict sf=8 persisted Parquet, manifest format 2, full-hash;
- the frozen 3,554 ms floor is NOT an input — the atlas measures scaling, not materiality;
- known controls: `TORON_PARTITION_PARALLELISM` (defaults to machine parallelism — exec.rs
  `default_partition_parallelism`), `TORON_TPCH_DUCKDB_THREADS` (persisted since the
  pin-persistence fix), `TORON_TPCH_MEMORY_PER_TASK` / `TORON_TPCH_SPILL_BYTES_RESERVED`
  (per-task semantics — the atlas divides the fixed 2 GiB aggregate by the thread count);
- doctrine: reference rows are timing evidence only; perturbation gates per ADR 516 for any
  new recorder; no summed-board credit.

## Step 0 - honesty facts before any sweep

1. Record machine topology (cores, sockets/NUMA if any, governor) into the atlas preamble.
2. Pin the historical coordinate: measure what effective `TORON_PARTITION_PARALLELISM` the
   closed board actually ran with (unset env → machine default) and whether q-cell plans or
   walls at an explicit pin of that value reproduce the era's rows; state the finding either
   way — the "one-task" era language is verified, not assumed.
3. Verify plan fingerprints at pins 1/2/4/8 for all 22 cells (cheap bind-only or --runs 1
   sweep): cells whose plans vary with the pin are flagged optimizer-variant now, before the
   measured sweep.

## Step 1 - row provenance and (if needed) the partition recorder

- Persist `toron_partition_parallelism` + provenance in perf rows (the Toron side of the
  pin-persistence fix); persist CPU affinity/topology summary per row.
- Survey existing telemetry for per-partition completion times and exchange volumes (spill
  ledger, exchange receipts, chain profiles). Where partition completion is not observable,
  add a batch-scoped, reconciliation-gated partition recorder under the profiler env —
  replay/partition-scoped parents from the start (the 524 lesson), once-per-runtime
  construction (092e574a), no code motion in hot paths.
- Gates for any recorder: 5 interleaved off/on pairs on q09 and q08 at pins 1 and 8,
  absolute 3% median, two strikes close the recorder red and the atlas ships with the
  observable subset, gaps named.

## Step 2 - seal and battery

Standard cohesive seal (`impl: adr 527 thread scaling atlas`), full battery with direct exit
statuses, push, release build. Knob-contract tests for any new envs.

## Step 3 - the sweep

Per (cell, engine, pin) for pins {1, 2, 4, 8}: 5 runs + 1 warmup, medians and spread.
Toron: `TORON_PARTITION_PARALLELISM=<pin>`, `TORON_TPCH_MEMORY_PER_TASK = 2 GiB / pin`,
spill reservation likewise; DuckDB: `TORON_TPCH_DUCKDB_THREADS=<pin>`. Join-spill scratch
cleared between arms; digest identity asserted across every arm of a cell; rows carry
thread pins, affinity, binary and dataset identity, RSS, spill, plan fingerprint.

## Step 4 - the atlas and verdicts

One results document: per-cell tables (median, spread, speedup, parallel efficiency vs the
same engine's pin-1 arm, straggler max/median ratio, exchange volume, RSS, spill) at all four
pins; the optimizer-variant flag column; the machine topology record. Efficiency and
straggler bands predeclared in this document from pin-1 noise before the measured sweep runs;
each cell receives exactly one verdict from the decision tree (good scaling /
uneven-completion / balanced-but-inefficient / optimizer-variant). `chore:`-class receipt;
no remedy selected; the doctrine ledger's morsel gate is updated with the verdict counts.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
