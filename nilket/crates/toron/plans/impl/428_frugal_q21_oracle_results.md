# ADR 428 results - frugal q21 oracle

## Outcome

ADR 428 is implemented. The public q21 correctness oracle no longer retains the 15-million-row
orders map or the 59,986,052-row lineitem-by-order map. It keeps the small supplier and nation
lookups, streams asserted orders and contiguous lineitem groups through a two-cursor merge, and
feeds every matched order to one shared semi/anti counter used by both the production path and
the test-only dense reference.

The canonical persisted strict q21 sf=10 comparison passed under the standard 4 GiB cgroup in
94,717.9 ms with 842,104 KiB maximum RSS. This retires `q21_sf10_oracle_residency_gap`: q21 moves
from engine-green/reach-only to `pass@compare`, and the board moves from **18/22 to 19/22
green**. ADR 427's 6 GiB comparison remains supplemental evidence and earns no board credit.

No Toron engine code, comparator behavior, non-vacuity rule, output schema, LIMIT ownership, or
other query oracle changed.

## Phase 0 evidence

### Dense residency gap

The promoted plan commit (`8515ab1`) was directly atop ADR 427's completed engine cut, with the
dense q21 oracle unchanged. The immediately preceding canonical receipt is therefore the before
measurement for this instrument-only cut:

| envelope | result | terminal correctness row | maximum RSS |
| --- | --- | --- | ---: |
| 4 GiB / 600 s | cgroup OOM after engine completion | none | cgroup limit reached |
| 6 GiB / 600 s | `pass@compare` in 120,499.1 ms | present | 5,045,848 KiB |

Source inspection grounded the 4 GiB failure in the co-resident dense oracle maps rather than
the engine. The successful 6 GiB row established semantic parity but did not satisfy the board
envelope.

### Dense targets and ordered generator shape

The retained dense implementation and the frugal implementation produced complete structural
equality at both supported parity scales:

| scale | sorted answer rows | contributing lineitem rows |
| ---: | ---: | ---: |
| 0.01 | 1 | 9 |
| 1 | 411 | 4,141 |

The generator shape remains the established 15,000,000 strictly increasing orders and
59,986,052 nondecreasing lineitems in 15,000,000 contiguous groups at sf=10. The production
oracle now asserts those properties while consuming the streams. The sf=1 structural-parity run
and the complete sf=10 4 GiB comparison both reached EOF without an ordering violation; tail
drain tests separately prove that this is not a prefix-only check.

## The cut

### Shared counter

`q21_count_order` owns the complete per-order SQL arithmetic for both implementations:

- reject an order whose status is not `F`;
- build the per-order supplier and late-supplier sets;
- consider every late lineitem row independently;
- require a resolvable Saudi-Arabian supplier;
- require another supplier on the order;
- reject the row if another supplier is late;
- checked-add both `contributing_rows` and the supplier-name count.

Multiple qualifying lineitems from the sole late Saudi supplier therefore each count, while
`receiptdate == commitdate` does not. Unknown supplier and nation references retain the dense
oracle's skip behavior. The dense reference calls this same helper, so parity tests validate
delivery through the merge rather than allowing a second arithmetic implementation to drift.

### Asserted stream merge

`Q21LineitemGroups` keeps one order's lineitems plus one-row lookahead. Equal contiguous
`l_orderkey` values form one group; a lower raw key, including a key that reappears after another
group, fails loudly. The order cursor independently requires strictly increasing unique
`o_orderkey` values.

The two-cursor merge discards unmatched orders and unmatched lineitem groups, invokes the shared
counter only for equal keys, and advances both cursors exactly once on a match. After either
cursor reaches EOF, the implementation drains the other cursor completely so a violation in
either unmatched tail is still observed. Peak oracle residency is one order's lineitems, the
small supplier/nation maps, and the answer counts.

### Dense reference ownership

The former dense implementation is now `q21_oracle_dense_from_rows` /
`strict_q21_oracle_dense` inside the internal `#[cfg(test)]` ADR 428 module. It is absent from
the shipped crate. `Q21Oracle` derives `PartialEq, Eq`, so parity compares the full sorted rows
and `contributing_rows` together. Public `strict_q21_oracle` continues to call the unchanged
public `q21_oracle_from_rows` seam, now backed by the frugal merge.

`compare_strict_q21`, its 100-row LIMIT handling, schema assertion, and non-vacuity predicate
are byte-unchanged.

## Discriminating locks

Seven internal ADR 428 tests cover:

- dense/frugal structural equality at sf=0.01 and sf=1;
- status other than `F`, sole-supplier EXISTS failure, second-late-supplier NOT-EXISTS failure,
  multiple qualifying lineitems, equality at the late-date boundary, non-Saudi rows, missing
  orders, orders without lineitems, and unknown supplier/nation references;
- contiguous equal lineitem keys forming one successful group;
- duplicate/decreasing orders and a lineitem key that reappears after another group;
- an ordering violation located only in the unmatched order tail and one located only in the
  unmatched lineitem tail;
- independent checked overflow of `contributing_rows` and a supplier count;
- the unchanged comparator rejecting an empty oracle through its non-vacuity guard.

The existing public harness q21 oracle test also remains green. Focused ADR 428 results were 7/7.

## Live results

The final release binary ran persisted strict q21 correctness under `MemoryMax=4G`,
`MemoryHigh=4G`, `TasksMax=512`, and `RuntimeMaxSec=600s`:

| scale / source | result | elapsed | max RSS | cgroup result |
| --- | --- | ---: | ---: | --- |
| 10 / persisted parquet | `pass@compare` | 94,717.9 ms | 842,104 KiB | success, exit 0 |
| 1 / memory | `pass@compare` | 4,700.3 ms | not separately measured | exit 0 |
| 0.01 / memory | `pass@compare` | 752.7 ms | not separately measured | exit 0 |

The sf=10 process completed in 1:34.75 wall time. This systemd version reported the completed
unit's `MemoryPeak` as unset, so the successful `/usr/bin/time -v` maximum-RSS receipt is the
recorded peak. The 842,104 KiB process peak is well below the normative 4 GiB cgroup limit.

## Scope and controls

The production diff is confined to `crates/toron-tpch/src/lib.rs`; there is no `crates/toron`
production change. The thirteen sf=10 controls from ADR 427, q17's oracle path, and every other
query path are untouched. The controls were not rerun because this is a q21-only correctness
instrument cut; the full Toron regression gate supplies the engine safety check.

The board is now 19/22. The remaining named threads are q17's oracle boundary-precision policy,
q02's join-output C-spec, and q05's diagnosis-first OOM.

## Verification

The normative Cargo verification gates used `scripts/cargo-cgroup`; temporary formatting and
parity-target development checks also used Cargo directly:

- `fmt -- --check` and `check -p toron-tpch --all-targets` passed;
- `toron-tpch --features polars-sql-reference --no-fail-fast` passed: library 41 passed / one
  intentional heavy ignore, CLI 15/15, architecture 12/12, correctness 176 passed / one
  intentional heavy ignore, Arrow probes 5/5, decimal probes 12/12, harness 44/44, q14 probes
  2/2, and doc tests;
- Toron architecture/source guards passed 2,079/2,079;
- the complete `toron --no-fail-fast` all-target matrix passed, including the 1,266-test library;
- the release build and live sf=0.01, sf=1, and persisted sf=10 comparisons passed;
- `git diff --check` passed, spill scratch was removed, and no `.perf.data` is tracked.

Only `toron-tpch/src/lib.rs` and this results document are changed. No temporary parity output,
debug output, or profiler artifact is shipped.
