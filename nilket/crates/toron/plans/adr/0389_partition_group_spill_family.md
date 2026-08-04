# ADR 389 - partition-group-sizing spill family (q12 canary, recursive grace by max group spill)

## Status
Proposed.

## Context

ADR 388 converged 4 cells on one producer: **`streamed inner join build partition group sizing`** - q12 (8.4 GB), q03/
q05/q10 (7.2 GB) / 512 MiB. The 378 partition-grouping hit its stop-line. The correction that matters: this is **not
"the build is 8 GB, therefore impossible"** - it is **"the active partition group is still sized by resident memory, not
by spill budget"** (`streamed_spill_partition_count(estimated_build_bytes, task_budget.memory.get())`, join.rs:2006/2145).
So recursive/deeper grace is the real candidate, with projection only as the required first proof. **q12 canary** (at
this producer pre-388); q03/q05/q10 measured coverage. No cap bump.

## Decision

### Phase 0 - projection first (required proof), then the sizing gap
1. **Projection check:** measure q12's partition-group build **retained vs key+needed columns**. Dead columns -> reuse
   the projection lever (348/363/372), re-measure. The build already projects, so expect minimal - but prove it.
2. **Confirm the sizing gap:** the group spill exceeds budget because P/G are **resident-sized, not spill-sized**.

### The explicit, testable sizing formula
```
group_spill_budget   = spill_budget - writer/page/encryption overhead reserve
target_group_count   = ceil(projected_spill_bytes / group_spill_budget)
```
**The chosen partitioning must prove `max(group_projected_spill_bytes) <= group_spill_budget`** - **not** average group
size under budget. Average-under-budget is insufficient: a hot key / skewed partition can blow a single group while the
average looks fine.

### Skew handling (max, not average)
If, after `target_group_count` sizing, a single group still exceeds `group_spill_budget` (a hot key / skewed
partition):
- **recursive split** that group (partition-the-partition) until its spill fits, OR
- if a single key's payload alone exceeds `group_spill_budget` and cannot be split, **name a skew floor** with the
  measured key/bytes.
Do not ship "bounded" if `max(group_spill) > group_spill_budget`.

### The cut (projection -> recursive grace by max -> proof/floor)
- Dead columns -> project, re-measure.
- Minimal -> recursive grace: size groups so **max group spill <= group_spill_budget**; one active group resident at a
  time; deterministic ascending-order emission with scatter/order restoration. Reuse the 349 grace / 378 grouping
  substrate.
- Floor only with proof: after projection + recursion, a single unsplittable unit forced >`group_spill_budget` by
  semantics. Total 7-8 GB is not a floor.
- Topology-named; no qNN; no cap bump.

## Acceptance
- **Projection measured first** (retained vs key columns recorded; minimal or reduced).
- **Recursive/grace sizing by max active-group spill, not average:** the implementation proves
  `max(group_projected_spill_bytes) <= group_spill_budget` (a test, not just the average), including a **skewed-key
  fixture**; a group that still exceeds is recursively split or named a skew floor.
- **q12 canary: green-or-next-wall** (partition-group-sizing gone + no downstream + persisted sf=10 correctness, OR a
  named different producer).
- **Measured coverage q03/q05/q10** - green/advanced/unchanged; claim only measured.
- **Deterministic ordered emission** - ascending groups + scatter; digest parity vs the pre-389 path (strict + dev).
- **Bounded-by-design** - `group_spill_budget` derives from `spill_budget` minus a measured overhead reserve; one active
  group resident; **no cap bump**; `spill_bytes`/`writer_peak`/resident-guard unchanged-or-better.
- **Persisted sf=10 correctness for any pass row; `--no-fail-fast` + `toron-tpch --no-fail-fast`**; de-branded; prior
  greens + q08/q19 unchanged.

## Guardrails
- **Projection first (proof), recursive grace if minimal, floor only with proof** - 7-8 GB total is not a floor.
- **Size from the spill, by MAX group spill not average** - the skew guard; recursive split or named skew floor if a
  group still exceeds.
- **Bounded-by-design** - max group spill <= group_spill_budget by construction; one active group resident; no cap bump.
- **Deterministic ordered emission** (digest parity); claim only measured coverage; full engine gates; no qNN.

## Risks
- **Skew (hot key) blows a single group** - the max-not-average proof + recursive split / named skew floor guards it.
- **Recursive partitioning re-scan cost** - more passes; bound with 385 read-once + 387/388 memory-aware patterns, or
  name a time wall.
- **q12 miss differs from q03/q05/q10** - q12-only + split, acceptable.
- **Projection already minimal** - straight to recursive grace (expected).
- **Genuine floor** - only with the post-projection + post-recursion proof.

## Forward Arc
389 (partition-group family: projection or recursive grace by max or proven floor) -> if it greens, the family's spill
is bounded by design; if it advances, the named next producer. Then q21's tiny resident wall, q07/q08/q17 time cluster,
the memory-tail. The DuckDB perf gap (greens are 11-82x) is the eventual perf arc.

## Grounding evidence (file:line)
388: q03/q05/q10 -> `streamed inner join build partition group sizing` 7.2 GB; q12 8.4 GB / 512 MiB. resident-sized P:
`streamed_spill_partition_count(estimated_build_bytes, task_budget.memory.get())` join.rs:2006/2145,
`streamed_inner_build_partition_count` streaming.rs:1850. build projects: `retained_columns` streaming.rs:855+. 349
grace / 378 grouping substrate. q12 = orders/lineitem + group (`Q12_DEV_SQL`). q12 DuckDB=133ms.
