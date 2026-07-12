# ADR 425 results - frugal q18 oracle

## Outcome

ADR 425 is implemented. The public q18 correctness oracle now validates the full generator at
sf=10 without retaining either 15-million-entry map. It keeps one measured customer-key set,
streams lineitems into asserted order groups, merges those groups with the asserted orders
stream, and resolves names for qualifying customers in a second streaming customer pass.

The canonical in-process strict q18 comparison passed under the standard 4 GiB cgroup in
226,744.2 ms with 1,256,868 KiB maximum RSS. This retires
`q18_sf10_oracle_residency_gap`: q18 moves from engine-green/reach-only to `pass@compare`, and
the board moves from **17/22 to 18/22 green**. ADR 424's 6 GiB comparison remains historical
supplemental evidence; it is not used for this result.

No Toron engine code, comparator behavior, non-vacuity rule, output schema, or LIMIT ownership
changed.

## Phase 0 evidence

### Canonical gap reproduction

The promoted-plan HEAD (`218fc0d`) was built in release mode before production code changed.
The exact persisted strict q18 correctness command was then rerun with `MemoryMax=4G`,
`MemoryHigh=4G`, `TasksMax=512`, and `RuntimeMaxSec=600s`:

| result | exit | memory peak | terminal correctness row |
| --- | ---: | ---: | --- |
| `oom-kill` | 15 (`systemd-run` rc 1) | 4,294,967,296 bytes | none |

Both stdout and stderr were empty: the process died during dense oracle construction after the
engine result, before it could emit a terminal compare row. This reproduces ADR 424's named
instrument gap at the exact board envelope rather than inferring it from the earlier 6 GiB
peak.

### Dense parity targets and generator shape

The old dense oracle established these complete structural parity targets:

| scale | sorted rows / qualifying | threshold failures | selected lineitem rows |
| ---: | ---: | ---: | ---: |
| 0.01 | 2 / 2 | 14,998 | 60,175 |
| 1 | 57 / 57 | 1,499,943 | 6,001,215 |

The bounded sf=10 generator probe recorded:

| stream | rows/groups | last order key | order violations |
| --- | ---: | ---: | ---: |
| orders | 15,000,000 rows | 60,000,000 | 0 |
| lineitems | 59,986,052 rows / 15,000,000 groups | 60,000,000 | 0 |

The lineitem grouping census found 624 sums strictly above 30,000, matching ADR 424. The
customer pass found 1,500,000 unique keys, zero duplicates, and no null-key representation
(the generator key is non-null `i64`). The standard `HashSet<i64>` ended at capacity 1,835,008;
its 12,000,000-byte key payload is recorded separately from container residency. With that set
kept live through all order and lineitem probes, the release helper peaked at 336,416 KiB RSS
and completed in 4.64 seconds. This left material headroom, so the compact-representation
stop-line did not fire.

The temporary helper was deleted before the production cut.

## The cut

### Membership and asserted streams

`strict_q18_oracle` remains the public oracle used by every correctness rail and external
integration test. Its implementation now supplies two separate customer iterators to a private
frugal row seam:

1. The first pass inserts only `c_custkey` into the measured `HashSet<i64>` and rejects a
   duplicate key loudly.
2. `Q18LineitemGroups` wraps the lineitem iterator with one-row lookahead. It checked-adds every
   raw row to `selected_lineitem_rows`, checked-adds quantities within the current contiguous
   order group, rejects a lower next group key, and returns the final group at EOF.
3. The order loop requires strictly increasing, unique `o_orderkey` values. A two-cursor merge
   discards unmatched lineitem groups or orders, consumes matched groups once, applies the
   customer-membership test, and classifies the exact `sum > 30_000` boundary.
4. After the order stream ends, all remaining lineitem groups are drained. This preserves the
   dense oracle's contract that `selected_lineitem_rows` counts every raw lineitem, including a
   group with no order.
5. Qualifying order metadata stays in a small pending type. A second customer pass retains names
   only for its distinct customer keys and errors if a previously admitted customer has no name.
6. All qualifying rows are built and sorted with the old total-price/date/order-key comparator.

All quantity and public counter increments are checked. The merge retains every qualifying row;
`compare_strict_q18` remains byte-unchanged and still applies `LIMIT 100` by taking the first 100
oracle rows.

### Dense reference ownership

The former dense implementation is renamed `q18_oracle_dense_from_rows` /
`strict_q18_oracle_dense` inside the internal `#[cfg(test)]` ADR 425 module. It remains the
structural tripwire but is absent from the shipped crate and cannot be selected by a live rail.
`Q18Oracle` now derives `PartialEq, Eq`, so parity compares the full sorted row vector and all
three counters. External `correctness_test.rs` continues to import the public frugal
`strict_q18_oracle`.

The comparator, schema checker, non-vacuity predicate, and q18 execution path are otherwise
unchanged.

## Discriminating locks

Seven internal ADR 425 tests cover:

- dense/frugal structural equality at sf=0.01 and sf=1;
- the exact-30,000 failure and 30,001 success boundaries;
- missing lineitems, missing orders, unknown customers, and the raw-lineitem counter;
- a single final group and multiple contiguous same-key rows;
- shuffled and duplicate orders, plus a lineitem key that reappears after another group;
- duplicate customer membership, a missing qualifying name in pass two, checked quantity
  overflow, and checked counter overflow;
- retaining 101 qualifying rows before the comparator's LIMIT.

The external correctness suite adds a direct empty-oracle lock proving the unchanged q18
non-vacuity guard still returns `oracle non-vacuous guard failed`. Existing mismatch, missing,
extra, duplicate, null-cell, and output-order comparator locks remain green.

Focused results were 7/7 for the internal ADR 425 module and 4/4 for the public strict-q18
integration filter.

## Live results

The final release binary ran the persisted strict q18 correctness rail under the same outer
cgroup as the Phase 0 failure:

| scale/source | result | elapsed | max RSS | cgroup result |
| --- | --- | ---: | ---: | --- |
| 10 / persisted parquet | `pass@compare` | 226,744.2 ms | 1,256,868 KiB | success, exit 0 |
| 1 / memory | `pass@compare` | 5,618.4 ms | not separately measured | exit 0 |
| 0.01 / memory | `pass@compare` | 731.2 ms | not separately measured | exit 0 |

The sf=10 process completed in 3:46.81 wall time. The cgroup stayed below 4 GiB; the retained
`/usr/bin/time` receipt is used for the successful process peak because this systemd version
reported the completed transient unit's `MemoryPeak` as unset. Phase 0's killed unit did retain
the exact 4 GiB cgroup peak.

This row supersedes the q18 residency-gap cell. The 6 GiB ADR 424 run is neither required nor
credited.

## Scope and controls

The diff has no `crates/toron` production change. The twelve sf=10 engine controls from ADR 424
were therefore not rerun, matching the plan's toron-tpch-only control rule; their execution,
plans, oracles, and data paths are untouched. q17 remains separately reach-only under
`q17_sf10_oracle_policy_gap`.

The board is now 18/22. The remaining named cells are q21 (throughput, profiling-first), q02
(join-output C-spec), q05 (diagnosis-first OOM), and q17 (oracle policy).

## Verification

All Cargo commands used `scripts/cargo-cgroup`:

- `fmt -- --check` and `check -p toron-tpch --all-targets` passed;
- base `toron-tpch`: library 34 passed / 1 intentional heavy ignore, CLI 14/14,
  toron-tpch architecture 12/12, correctness 176 passed / 1 intentional heavy ignore,
  Arrow probes 5/5, decimal probes 12/12, harness 44/44, q14 probes 2/2, and doc tests;
- the same matrix with `polars-sql-reference --no-fail-fast` passed, including the
  feature-specific CLI test (15/15);
- Toron architecture/source-scan guards passed 2,079/2,079;
- release build plus live sf=0.01, sf=1, and persisted sf=10 comparisons passed;
- `git diff --check` passed, spill scratch was removed, and no `.perf.data` is tracked.

Only `toron-tpch/src/lib.rs`, its correctness integration test, and this results document are
changed. No new debug output or temporary Phase 0 source remains.
