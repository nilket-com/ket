# ADR 482 implementation receipt

The m05 high-cardinality grouping path no longer gives every single-Int64 key a
separate heap `Vec`. For a bound `GROUP BY` schema of exactly one Int64 column,
the GroupTable now stores one `[GroupValue; 1]` slot per group contiguously; all
other grouping shapes retain the generic store. ADR 002's lexicographic output
sort is unchanged.

## Grounding

ADR 481's profile placed `find_bucket_in` at 28.6% self time for m05. A follow-up
`perf stat` run over the same 48M lineitem rows recorded substantially more cache
and DTLB-load misses per row for m05 than the m02 scalar control (about 15x and
10x respectively). The host's hybrid-core event accounting does not identify one
function as the source of those counters, so this supports locality as a design
constraint rather than proving a kernel-frame cause.

## Correctness and accounting

- The compact store is selected from the bound schema before the first insert and
  accepts Int64 or null keys only. Non-Int64 and multi-column groupings stay generic.
- `single_int64_store_is_contiguous_and_preserves_null_and_sorted_output` pins
  duplicate counts, null grouping, ADR 002 ordering, and resident-byte
  recomputation. The growth guard charges the compact outer buffer before allocating.
- m05's Parquet smoke result retains its prior output: 12,000,000 rows, identical
  Arrow IPC digest, identical plan fingerprint, and no spill.

## Matched release A/B (sf=8, strict Parquet, 64 MiB task / 512 MiB reserve)

The candidate was committed before its sibling baseline was built in a detached
worktree. Eight serial alternating pairs used those two saved release binaries;
raw rows are retained under `target/tpch/adr482_ab_1784611004/`.

| metric | generic baseline | compact store |
|---|---:|---:|
| median execute | 10,987.5 ms | 8,645.8 ms |
| paired median reduction | | **2,348.9 ms (21.3%)** |
| pair signs | | **8/8 faster** |

Every timed row has the same output digest, output cardinality, and plan fingerprint.
One earlier shell invocation passed its arguments as a single zsh word and printed
usage without running a child; a later live-output handle dropped from an otherwise
serial run. Neither has raw evidence and neither enters the verdict. The persisted
eight-pair set above is the sole timing evidence.

## Gates

Targeted group-table and aggregate tests pass; architecture is **2,093/2,093**.
The full engine suite passed with durable `full_engine_exit=0` under
`target/tpch/adr482_gates_1784611663/`. The armed strict-Parquet sweep passed
**22/22** (q17 at its established sf=0.02 exception; every other query at sf=0.01)
under `target/tpch/adr482_parquet_sweep_1784611750/`.
