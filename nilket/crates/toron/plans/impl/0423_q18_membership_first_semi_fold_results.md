# ADR 423 results - q18 membership-fold prerequisite

## Outcome

Phase 0 fired the ADR's explicit stop-line. The isolated sf=10 membership aggregate was killed
by the 4 GiB cgroup while the generic grouped fold was still growing an unguarded
`BTreeMap<Vec<GroupValue>, Vec<AggState>>`. Shipping membership-first at that point would have
replaced q18's typed `SpillBytes` wall with a kernel OOM, so the scheduled reorder did not land.

ADR 423 therefore pivoted, as authorized by the dual, to the single mechanism that Phase 0
proved was prerequisite: a guarded primitive single-Int64 grouped `SUM` fold. The same isolated
membership subtree now completes all 59,986,052 input rows and 15,000,000 groups under the
unchanged budget. The membership-first composition remains the next bounded cut.

This is a prerequisite seal, not a q18 board win. The board remains 17/22: q18 still reaches
the byte-identical semi outer-writer quota wall before membership execution.

## Phase 0 decision table

### Exact plan and bounded census

The persisted q18 membership subtree matched the expected primitive shape:

- projected scan schema `[l_quantity: Decimal128(15,2), l_orderkey: Int64]`;
- four-way `Exchange` on non-null Int64 ordinal 1;
- group expression is the same direct ordinal 1 column;
- one direct `SUM` over non-null Decimal128 ordinal 0;
- no scan predicate or project-filter predicate.

The O(1)-state generator census counted the pre-limit, post-`HAVING SUM(l_quantity) > 300`
membership keys directly. It did not use the outer `LIMIT 100` answer or construct the sf=10
oracle maps.

| scale | lineitem rows | distinct order keys | qualifying keys | duplicate/null keys | key payload | projected presence-set capacity |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6,001,215 | 1,500,000 | 57 | 0 / 0 | 456 B | 1,024 entries + 2,048 buckets = 16,384 B |
| 10 | 59,986,052 | 15,000,000 | 624 | 0 / 0 | 4,992 B | 1,024 entries + 2,048 buckets = 16,384 B |

The census closes the `LIMIT 100` trap: sf=10 has 624 qualifying keys even though the
published query returns at most 100 rows. It also proves that presence-set residency is not the
current wall.

### Isolated membership stop-line

The temporary `q18mem` harness executed exactly the persisted membership subtree under
`MemoryMax=4G`, `MemoryHigh=4G`, and `RuntimeMaxSec=600s`. Its progress receipt was flushed
outside process-exit telemetry.

| implementation | result | last/completed input rows | last/completed groups | cgroup/RSS | fold time |
| --- | --- | ---: | ---: | ---: | ---: |
| generic grouped fold | `Result=oom-kill`, status 15 | 39,017,472 | 9,754,994 | exactly 4,294,967,296 B cgroup peak | 13.614 s CPU before kill |
| primitive grouped SUM | pass, 624 output keys | 59,986,052 | 15,000,000 | 1,075,536 KiB max RSS | 3.111 s fold; 3.382 s execute |

The pre-cut process produced no terminal JSON, as expected for an external kill. The durable
progress file is the evidence for its last completed fold state; no final counters are
invented. Scratch was empty after both runs, and the temporary harness and diagnostic hook were
removed before the final build.

The stop-line verdict was therefore **pivot**. Presence-set microfixture work and the
membership-first committed sequence were not implemented after the higher-priority resource
safety failure became conclusive.

## The prerequisite cut

`try_streaming_scan_grouped_aggregate` now selects a local primitive representation only when
all structural facts are proved before scan I/O:

- an exchange exists and its key program exactly equals the group program;
- the group is one direct, non-null Int64 column in both the expression and input schema;
- there is exactly one aggregate and it is a direct non-null `SUM`;
- the input/output pair is Int64/Int64 or Decimal128/Decimal128 with the same scale;
- the output has exactly the non-null Int64 key plus the SUM column;
- scan-filter and project-filter predicates are absent.

Everything else declines to the unchanged generic fold. A plain scan projection remains
eligible because q18's persisted scan already projects the two required columns before the
exchange.

The admitted representation is three primitive vectors:

```text
buckets: Vec<u32>    # EMPTY = u32::MAX, seeded hash, linear probe, <= 0.5 load
keys:    Vec<i64>
totals:  Vec<i128>   # checked SUM for both Int64 and Decimal128 inputs
```

Growth is explicit and transactional. Before each `try_reserve_exact`, the executor checks the
requested old-plus-new transition footprint against the unchanged retained-footprint cap; it
then checks observed capacities and rebuilds the new buckets while the old table remains live.
There is no container implicit growth. Allocation failure, i128 accumulation overflow, u32
domain exhaustion, and Int64 finalization overflow are loud errors.

At sf=10 the completed table and largest transition are byte-derived:

| item | capacity | bytes |
| --- | ---: | ---: |
| keys | 16,777,216 x 8 B | 134,217,728 |
| totals | 16,777,216 x 16 B | 268,435,456 |
| buckets | 33,554,432 x 4 B | 134,217,728 |
| completed resident table | | **536,870,912** |
| old-plus-new growth peak | | **805,306,368** |
| unchanged retained-state cap | | **1,073,741,824** |

Final output materialization has its existing independent cap. Decimal totals move directly
into the Arrow output; Int64 totals are checked while narrowing from i128. An off-by-default,
query-neutral profile receipt records input rows, groups, observed capacities, completed bytes,
peak bytes, cap, and elapsed fold time.

## Discriminating locks

Five direct locks cover the new mechanism rather than passing through fallback:

1. compact-state arithmetic, duplicate accumulation, missing lookup, full-domain `i64::MIN`,
   exact capacities, and exact transition peak;
2. a cap one byte below the initial complete allocation, proving rejection occurs before any
   vector allocation;
3. full-row sorted digest parity with the generic fold for Decimal128, including duplicate,
   negative, and full-domain keys;
4. checked Int64 finalization, including an i128 value outside the Int64 output domain;
5. direct eligibility plus declines for nullable keys/arguments, non-SUM aggregation,
   predicates, and missing exchange.

The focused aggregate suite passed 28/28 and the full architecture suite passed 2,079/2,079.
The remaining full seal matrix is recorded below.

## Final persisted evidence

The final release binary was rebuilt after removing every temporary harness and progress hook.

At sf=1, strict q18 passed comparison in 4,549.4 ms. The real-plan receipt proves selection:

```text
input_rows=6001215
groups=1500000
entry_capacity=2097152
bucket_capacity=4194304
accounted_bytes=67108864
peak_bytes=100663296
cap=1073741824
elapsed_micros=180746
```

At sf=10, the unchanged pre-membership schedule reproduced its established wall:

| fingerprint | producer | requested | available | spill bytes | execute | max RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `d8b60e3a03bfaf79` | `streamed inner join build spill writer` | 536,899,968 | 536,870,912 | 1,091,648,144 | 137,163.7 ms | 2,200,480 KiB |

No primitive-fold receipt was created during that run: the outer writer failed before the
membership subtree opened. This is the expected execution-order proof for the deferred reorder,
not evidence that the primitive gate missed.

All twelve won-ground controls passed strict sf=10 comparison on the same final binary:

| query | elapsed ms | query | elapsed ms |
| --- | ---: | --- | ---: |
| q03 | 24,608.3 | q13 | 145,169.9 |
| q07 | 35,172.0 | q14 | 11,860.5 |
| q09 | 44,386.0 | q16 | 9,586.2 |
| q10 | 23,100.9 | q19 | 98,376.6 |
| q11 | 9,588.4 | q20 | 30,092.7 |
| q12 | 18,239.6 | q22 | 10,111.9 |

The prior forensic spill signatures remain the comparison bands; this implementation changes
neither spill accounting nor any join writer. The correctness controls additionally prove the
shared aggregate dispatcher did not widen into unrelated shapes.

## Verification

All commands used the repository cgroup wrapper. The final tree passed:

- `fmt -- --check`;
- `check -p toron --all-targets`;
- focused aggregate 28/28, spill 50/50, join 213/213, and streaming 54/54;
- required full Toron library 1,252/1,252;
- full architecture 2,079/2,079;
- `test -p toron --no-fail-fast`, including every integration binary and doc tests;
- base `toron-tpch` library/CLI/architecture/correctness/decimal/harness/probe suites;
- the same `toron-tpch` matrix with `polars-sql-reference --no-fail-fast`.

`git diff --check` is clean. `exec.rs` remains exactly at its 7,500-line ratchet. Production
source contains no query/ADR branding or temporary diagnostics, scratch was cleaned after the
seal, and no `.perf.data` file is tracked.

## Honest forward arc

The next q18 cut is the membership-first composition already designed by this ADR: execute the
now-bounded membership fold first, build the measured 624-key presence set, and drive the
preserved chain directly into the filtered outer fold without constructing the semi outer
writer. Its two-peak accounting and original-key ordinal pins remain applicable unchanged.

No q21 speedup is claimed. The primitive fold is a plausible shared throughput ingredient, but
q21 still requires profiling that separates fold time from its six probe stages. q02, q05, and
q17 remain their named correctness/diagnosis/policy threads.
