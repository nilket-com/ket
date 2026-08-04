# ADR 494 implementation receipt — grouping reprofile and borrowed UTF-8 probe

The post-493 ladder selected grouping, but the two leading grouping cells have
different owners. ADR 494 profiled both on the current binary before choosing a
mechanism.

## Diagnosis

At sf=8, strict Parquet, and the explicit 2 GiB task coordinate:

- **m05 is structurally over budget.** Its grouped-fold receipt reports
  `current_resident_bytes=3,377,742,008` and 384,000,000 bytes of finalize-sort
  scratch, consistent with the ~4.7 GiB process RSS. The generic streaming grouped
  path constructs `BoundGroupedAggregateGroups::new()` without installing
  `growth_budget_bytes`, so its otherwise-live guard has no policy input. This is a
  real contract gap, but repairing it requires a spill/admission design; merely
  turning on the guard would make the passing 12M-group query fail.
- **m04 has a bounded CPU owner.** Its grouped-fold receipt attributes 1,263,707 us
  to folding 47,989,007 rows into three groups, versus 8 us total finalization and
  only 58,203 bytes of resident group state. Symbol sampling also names
  `find_bucket_in`; ADR 477 had already left the remaining per-row
  `GroupValue::Utf8(String)` ownership as the priced residual.

The m05 budget repair is therefore recorded as the forward structural cut. ADR 494
does not disguise it as a performance optimization or mix it into the independent
m04 mechanism.

## Change

For exactly one UTF-8 group column, the grouped fold hashes and compares the Arrow
`&str` directly. It creates an owned `GroupValue::Utf8(String)` only when inserting
a new group. All other key shapes retain the generic owned-key path.

The borrowed hash reproduces the existing one-element `GroupValue` slice hash from
the slice length, the existing enum discriminant, and borrowed string bytes. It
introduces no Arrow datatype or IR dependency into the group-table key abstraction.
A focused test pins owned/borrowed hash identity for UTF-8 and NULL, hit behavior,
insert-only initialization, resident accounting, and sorted output.

## Matched alternating A/B

Coordinate: sf=8, strict Parquet, 2 GiB task, 2 GiB spill reserve, release. The same
binary (`build_commit=264eef400bb6-dirty`, `binary_modified_unix=1784700527`)
alternated five control/candidate pairs. `TORON_TPCH_BORROW_SINGLE_UTF8_GROUP_KEY=0`
restores owned-per-row extraction; `=1` is the default candidate.

| arm | execute (5 runs) | median | peak RSS range | spill |
|---|---|---:|---:|---:|
| owned UTF-8 probe | 2,496.4 / 2,510.7 / 2,465.2 / 2,506.8 / 2,517.0 ms | 2,506.8 ms | 22,692–23,076 KiB | 0 B |
| borrowed UTF-8 probe | 1,891.3 / 1,878.1 / 1,883.7 / 1,890.7 / 1,912.5 ms | 1,890.7 ms | 22,576–23,564 KiB | 0 B |

**Effect: -24.6% execute median, 5/5 paired wins.** All ten rows have the same
`04c9d21d87cbb802` plan fingerprint, Arrow IPC digest, three-row output, binary
identity, and zero spill. RSS is unchanged within run noise.

Raw rows and profiler artifacts are retained under `target/tpch/adr494/` and
timestamped `target/tpch/adr494_ab_{control,candidate}_*/` directories.

## Forward

m05 remains the largest absolute ladder gap and now has a grounded contract defect:
generic grouped fold can exceed task memory because its growth budget is unset. The
next design must provide a passing bounded strategy—partitioned spill, governed
admission, or another explicitly receipted policy—not simply activate the existing
guard and convert m05 into a resource-exhausted failure.
