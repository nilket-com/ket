# ADR 389 - Implementation plan: partition-group-sizing spill family (q12 canary)

Pairs with `plans/adr/389_partition_group_spill_family.md`. Bound the partition-group spill by sizing groups from the
**spill budget** (not resident), proving **max group spill <= group_spill_budget** (skew guard). Projection first;
recursive grace if minimal; floor only with proof. q12 canary; q03/q05/q10 coverage; no cap bump; full engine gates.

## Phase 0 - projection check + sizing gap (before code)
- **Projection:** measure q12's partition-group build retained columns vs key+needed columns. Dead columns -> project
  (348/363/372), re-measure spill. Record minimal-or-reduced.
- **Sizing gap:** confirm the group spill exceeds 512 MiB because P/G are resident-sized
  (`streamed_spill_partition_count(build_bytes, memory)`), not spill-sized. Record projected_spill_bytes per cell.
- **Overhead reserve:** measure the writer/page/encryption overhead per group so `group_spill_budget = spill_budget -
  reserve` is real, not a guess.

## D1 - spill-budget group sizing (max, not average)
- Compute `group_spill_budget = spill_budget - overhead_reserve`;
  `target_group_count = ceil(projected_spill_bytes / group_spill_budget)`.
- Size partitions/groups so **every** group's projected spill <= `group_spill_budget`. Process one active group at a
  time, ascending order, scatter/order restoration (deterministic emission). Reuse 349 grace / 378 grouping.
- **Skew:** if a single group still exceeds `group_spill_budget`, **recursively split** it (partition-the-partition)
  until it fits; if a single unsplittable key exceeds it, **name a skew floor** (measured key/bytes). Never ship
  "bounded" with `max(group_spill) > group_spill_budget`.
- Keep one-active-group-resident; no cap bump; topology-named; no qNN.

## Tests
- **Max-group-spill bound (skew):** a synthetic build exceeding budget as one group, **including a skewed/hot-key
  fixture**, completes with **every** active group's spill <= `group_spill_budget` (assert max, not average); a
  hot-key-larger-than-budget case triggers recursive split or the named skew floor.
- **Deterministic emission:** ascending-group + scatter output digest == the pre-389 path (strict + dev), order/schema
  identical.
- **Bounded resident:** one active group resident; `spill_bytes`/`writer_peak`/resident-guard unchanged-or-better.
- **Projection parity (if applied):** projected build round-trips identically.
- **q12/q03 correctness:** strict `pass@compare` sf=0.01/sf=1.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 0.01   # strict pass@compare
# fixed-cap sf=10 re-measure q12/q03/q05/q10: partition-group-sizing gone -> green or named next producer;
#   max group spill <= group_spill_budget; persisted sf=10 correctness for greens
git diff --check
#   projection measured; group sizing by max (skew fixture); deterministic emission; bounded resident; no cap bump;
#   q12 green-or-next-wall; q03/q05/q10 measured; --no-fail-fast + toron-tpch green; no qNN
```

## Results doc
`crates/toron/plans/impl/389_partition_group_spill_family_results.md`:
- projection check (retained vs key columns; minimal/reduced);
- the sizing formula in practice: `group_spill_budget`, `overhead_reserve`, `target_group_count`, measured
  **max group spill** per cell (proof it's <= budget, not just average);
- skew handling: recursive split count or named skew floor (with key/bytes);
- q12 green-or-next-wall + q03/q05/q10 measured (green/advanced/unchanged);
- deterministic-emission digest parity; bounded-resident + no-cap-bump confirmation;
- persisted sf=10 correctness for greens; full engine gates green; no qNN.

## Risk / blast radius
- **Skew blows a single group** -> max-not-average proof + recursive split / named skew floor.
- **Recursive re-scan cost** -> 385 read-once + 387/388 memory-aware patterns; or name a time wall.
- **q12 miss differs from q03/q05/q10** -> q12-only + split.
- **Genuine floor** -> post-projection + post-recursion proof only.
- **Regression** -> `--no-fail-fast` + `toron-tpch`.

## Seal
`impl: adr 389 partition group spill family`. Do not commit wip drafts or large `.perf.data`.
