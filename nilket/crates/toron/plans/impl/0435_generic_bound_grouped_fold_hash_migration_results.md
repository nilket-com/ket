# ADR 435 results - generic bound grouped-fold hash migration

## Verdict

ADR 435 is green. The generic bound grouped fold now has one hash-table owner across all 18 fold and
10 finalize callers. Paired `(key, states)` finalization preserves raw sorted batches, and growth is
budgeted against the complete transient rehash peak before allocation.

For q13, the mechanism moves the second article-grade wall:

- the inner 15,337,604-row / 1,500,000-group fold falls from 11,883.068 ms to 6,704.305 ms in the
  diagnostic ledger, a 43.58% reduction;
- the unprofiled ADR 356-shaped median falls from 28,288.448 ms to **22,257.249 ms**, a
  **1.271x speedup / 21.32% reduction**, with 0.01722 spread;
- the Toron/DuckDB q13 ratio falls from 20.47x to **16.11x** against the sealed 1,382.000 ms DuckDB
  median;
- fingerprint, digest, schema, 46 rows, and 9,152,992 spill bytes are invariant in every measured
  child;
- persisted strict sf=10 remains `pass@compare` under 4 GiB / no swap.

The independent route census measures 19 generic-fold cells and three bypass controls
`{q06, q14, q19}`. All 19 affected cells pass persisted sf=10 comparison. This is affected-set
correctness, not a new full-board redux.

The next q13 candidate is also measured: the new `find_bucket_in` lookup is the largest cpu-core
userspace frame at 23.83%. Per-row `Vec<GroupValue>` allocation remains deferred as ADR 435 selected
the hash-only depth; a future cut must diagnose hashing, equality, and key construction together.

## Conditions and provenance

| item | value |
|---|---|
| ADR 434 implementation baseline | `3eddbd5` |
| committed ADR 435 plan | `11d73bc` |
| measurement date | 2026-07-14 CDT |
| measured and final release binary SHA-256 | `ec4ba1ad0cc06c37173433d93a901d48492d9ed9be23aec72761a5c4d2c2948f` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet at `target/tpch/data` |
| throughput/census scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| correctness scope | same compute profile, memory non-limiting, `RuntimeMaxSec=600`; q13 additionally repeated under the 4 GiB scope below |
| survival scope | `MemoryHigh=4G`, `MemoryMax=4G`, `MemorySwapMax=0`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=600` |
| host | 12th Gen Intel Core i9-12900K; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel / perf | Linux `7.0.0-27-generic`; perf `7.0.6`; `perf_event_paranoid=1`; `kptr_restrict=1` |
| evidence root | `target/tpch/adr435/` |
| join-spill scratch | `target/tpch/join_spill`; empty before and after every accepted child |

The generic receipt and perf timings are diagnostic. The speed claim uses only the unprofiled 7+1
release rail. The final post-gate release rebuild is byte-identical to the binary used for all
accepted live receipts.

## Phase 0 - depth selection

The post-434 fingerprint was reconfirmed as `ac3784c060c3c226` in a separate warmup. One fresh
symbol-rich child then ran under `perf record --freq 99 --call-graph dwarf,16384` with zero lost
samples. It retained the sealed digest, schema, 46 rows, and 9,152,992 spill bytes.

The meaningful `cpu_core/cycles/P` event recorded approximately 160,190,416,059 cycles. The flat
profile attributed 33.74% of total cycles to `fold_bound_grouped_aggregate_batch`. Within the
source-line fold report:

| pre-migration source/frame | total cpu-core cycles |
|---|---:|
| derived `GroupValue::Ord` comparison at `aggregate.rs:1469` | 16.36% |
| BTree node search at `node.rs:396` | 7.79% |
| BTree search at `search.rs:226` | 4.00% |
| process-wide `_int_malloc` | 6.24% |

Tree lookup/comparison was therefore the dominant measured sub-cost; allocator work was materially
smaller and not all process allocator samples belonged to key construction. Phase 0 selected the
**hash-only** depth. The per-row `Vec<GroupValue>` allocation remains intact and is not claimed as
fixed.

## Mechanism and migration surface

`BoundGroupedAggregateGroups` owns parallel key/state vectors plus an open-addressed hash index. Its
state is boxed so the 28-call-site migration does not enlarge the widely held async state by the
table itself. The wrapper owns:

- `entry_or_init`, including one hash/equality route for occupied and vacant entries;
- structural key/state alignment;
- paired sorted finalization;
- current ownership, projected rehash peak, and finalize-sort scratch accounting;
- the independent `TORON_GENERIC_GROUPED_FOLD_PROFILE_DIR` receipt.

An architecture gate counts one definition plus 18 fold callers and one definition plus 10 finalize
callers, rejects a raw `BTreeMap<Vec<GroupValue>, Vec<AggState>>` or raw BTree entry API in the
aggregate/streaming owners, and requires both modules to route through the wrapper. The primitive
Int64 grouped-SUM table is unchanged.

### Paired finalization rail

`finalize_sorted` constructs borrowed `BoundGroupedAggregateEntry { key, states }` pairs and sorts
those pairs by key. Finalization never sorts keys separately or zips them against hash-order values.
The direct raw-batch test uses:

- two group columns, including nullable Int64 and UTF-8 keys;
- deliberately non-sorted input and a null group;
- COUNT ALL, COUNT, SUM, MIN, MAX, and AVG states;
- an exact order-sensitive expected `RecordBatch`.

It passes, as does the existing q13-class double-group streamed/materialized parity test. The live
19-cell comparison rail additionally verifies canonical values across the full affected surface.

### Growth-aware residency rail

The wrapper maintains current ownership incrementally, so accounting is O(1) per insertion. At a
rehash it computes the complete new key-vector, state-vector, and bucket buffers while the old table
is still resident, guards that projected peak, allocates, then publishes the replacement. Finalize
sort scratch is separately guarded and recorded.

The forced-growth fixture fills a two-entry table, sets the budget to one byte below the exact
`current + new buffers` projection, and proves:

- the guard fires before state initialization or any table mutation;
- the error is `BudgetExhausted` with producer `generic bound grouped-fold hash table`;
- existing keys/states and current ownership remain exact;
- the rejected key is absent;
- the recorded projected peak is exact;
- setting the budget to the exact projection admits the same growth;
- incremental current ownership equals an independent full recomputation before and after rehash.

The live q13 inner-fold receipt separates the three residency domains:

| quantity | bytes |
|---|---:|
| final current group-table ownership | 597,440,688 |
| maximum projected rehash peak at a growth boundary | 511,705,264 |
| finalize paired-sort scratch | 48,000,000 |
| current plus finalize scratch | 645,440,688 |

The rehash projection is lower than final current ownership because keys/states continue to be added
after the last rehash. They are intentionally not cross-summed as simultaneous maxima. The 4 GiB
q13 run below is the live cap proof.

## Independent generic-fold receipt

The receipt is off by default and independent of the ADR 433 spill profiler. When the environment
variable is absent, the wrapper has no profile state or sink and takes no diagnostic clocks. When
enabled, one atomic temp-then-rename JSON file is published per finalized group table; publication
failure is returned as `ExecutionError` and is unit-locked as loud.

The q13 diagnostic child emits two receipts: the 15.34M-row inner fold and the 1.5M-row outer fold.

| counter | inner fold | outer fold |
|---|---:|---:|
| input rows | 15,337,604 | 1,500,000 |
| groups | 1,500,000 | 46 |
| entry lookups | 15,337,604 | 1,500,000 |
| occupied / vacant | 13,837,604 / 1,500,000 | 1,499,954 / 46 |
| hash probes | 21,952,635 | 1,499,999 |
| probes per lookup | 1.431 | 1.000 |
| maximum probes | 66 | 1 |
| key allocations | 15,337,604 | 1,500,000 |
| aggregate updates | 15,337,604 | 1,500,000 |
| fold time | 6,704.305 ms | 36.623 ms |
| finalize pair-sort time | 234.104 ms | 0.003 ms |
| counter overflow | false | false |

The enabled diagnostic output is exactly invariant with the disabled measured children. Its
22,044.138 ms execute time is not used as a speed result.

## Affected-set census and correctness

Every cell was attempted once in a fresh sf=10 process with a cell-local receipt directory. All 22
reach runs passed. Receipt presence, not SQL/source inference, selected the affected set:

| cell | receipt count | route | persisted compare |
|---|---:|---|---|
| q01 | 1 | generic | pass |
| q02 | 1 | generic | pass |
| q03 | 1 | generic | pass |
| q04 | 1 | generic | pass |
| q05 | 1 | generic | pass |
| q06 | 0 | bypass control | not re-run |
| q07 | 1 | generic | pass |
| q08 | 1 | generic | pass |
| q09 | 1 | generic | pass |
| q10 | 1 | generic | pass |
| q11 | 1 | generic | pass |
| q12 | 1 | generic | pass |
| q13 | 2 | generic, nested folds | pass |
| q14 | 0 | bypass control | not re-run |
| q15 | 2 | generic, nested folds | pass |
| q16 | 2 | generic, nested folds | pass |
| q17 | 1 | generic | pass |
| q18 | 1 | generic | pass |
| q19 | 0 | specialized primitive path | not re-run |
| q20 | 1 | generic | pass |
| q21 | 1 | generic | pass |
| q22 | 1 | generic | pass |

The persisted rail is 19/19 green. The three bypass controls passed the same source-matching reach
census, but this document does not silently promote those reach rows to persisted correctness and
does not claim a new full-board redux.

## Unprofiled ADR 356 before/after

One separate warmup completed at 22,316.804 ms. All seven measured children passed and agreed on
fingerprint `ac3784c060c3c226`, the sealed digest, schema `fnv1a64:06933e705bc02cb3`, 46 rows, and
9,152,992 spill bytes:

| run | execute ms | max RSS KiB | voluntary switches |
|---:|---:|---:|---:|
| 1 | 22,097.006 | 981,844 | 23,461 |
| 2 | 22,257.249 | 982,828 | 23,456 |
| 3 | 22,061.912 | 981,916 | 23,526 |
| 4 | 22,445.267 | 982,176 | 23,492 |
| 5 | 22,200.290 | 982,324 | 23,496 |
| 6 | 22,373.177 | 982,744 | 23,472 |
| 7 | 22,269.630 | 982,388 | 23,463 |

| statistic | ADR 434 before | ADR 435 after |
|---|---:|---:|
| minimum | 28,210.956 ms | 22,061.912 ms |
| median | 28,288.448 ms | **22,257.249 ms** |
| maximum | 28,342.476 ms | 22,445.267 ms |
| spread `(max-min)/median` | 0.00465 | **0.01722** |
| Toron / sealed DuckDB q13 median | 20.47x | **16.11x** |

The applicable band is `max(0.10, 2 * max(0.00465, 0.01722)) = 0.10`. The 21.32% median reduction
exceeds the band, so the speed claim is banked without a retry.

The post-change symbolized child records approximately 127,838,478,320 cpu-core cycles, 20.20% below
the Phase-0 child. The generic BTree fold frame is absent; residual BTree frames use unrelated
`usize` maps and total well below 2%. The new `find_bucket_in` is 23.83% of cpu-core cycles, while
process-wide `_int_malloc` remains 5.80%. This names the possible next wall without folding another
mechanism into ADR 435.

## Correctness and survival

The independent persisted q13 command completed under 4 GiB / no swap:

```text
tpch_correctness query=q13 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=33216.1
```

It recorded 982,868 KiB maximum RSS, 23,496 voluntary context switches, zero filesystem-input
blocks, no swap, exit status 0, and clean join-spill scratch. The affected-set correctness rail ran
separately under the memory-non-limiting profile. No full-board survival rerun is claimed.

## Implementation iteration disclosed

Two exploratory attempts are excluded from all accepted measurements:

1. An initial live probe copied the pre-435 release binary before the cgrouped link had published its
   new artifact. Its old SHA and missing generic-fold receipt exposed the mismatch; both rows were
   invalidated before comparison.
2. The first source-matching hash prototype recomputed current ownership by scanning every existing
   key/state on every vacant insertion. The valid binary exceeded 94 s at 100% CPU and was stopped;
   that accidental O(n-squared) accounting path left one PID-named spill file, which was removed
   before the accepted rails. Current ownership is now maintained incrementally, and the forced-growth
   test independently recomputes it at the rehash boundary.

Neither exploratory timing is used in a table, ratio, or speed claim.

## Tests and verification

- `scripts/cargo-cgroup fmt -- --check` - pass;
- `scripts/cargo-cgroup check -p toron --all-targets` and
  `scripts/cargo-cgroup check -p toron-tpch --all-targets` - pass;
- `scripts/cargo-cgroup test -p toron --lib` - 1,295 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --test architecture_test` - 2,081 passed, 0 failed;
- direct mixed-state ordered batch parity, q13 double-group parity, disabled-profile, atomic receipt,
  loud receipt-I/O failure, and forced-growth guard tests - pass;
- `scripts/cargo-cgroup test -p toron --no-fail-fast` - 44 binaries, 4,663 passed, 0 failed,
  1 ignored;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast` -
  9 binaries, 311 passed, 0 failed, 2 ignored;
- `git diff --check` - clean;
- `git ls-files '*.perf.data'` - empty;
- `exec.rs` remains 7,499 lines;
- final join-spill scratch is empty.

No production profile environment variable is set by default, no query-number branding appears in
executor code, and no `.perf.data` artifact is tracked. The implementation and this results document
remain uncommitted for independent review.
