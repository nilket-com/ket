# ADR 378 - Implementation plan: bounded partition-group spill

Pairs with `plans/adr/378_bounded_partition_group_spill.md`. Bound 377's streamed inner-build spill wall for
q03/q05/q10/q12. The recorded producer is `streamed inner join build spill writer`, so Phase 0 must discriminate
**per-writer spill** from **aggregate-live spill** before code. Projection is already spent; the durable cut sizes the
partition count `P` for per-writer spill and the group count `G` for live group spill. Not a cap bump. q03 canary;
parity + spill-peak gated.

## Phase 0 - classify + size P/G (before code)
- Confirm q03/q05/q10/q12 hit the **377 streamed inner-build spill writer** wall with projected columns already applied.
  Record each cell's current `partition_count`, largest partition-writer spill, aggregate live spill, total projected
  build spill, and over-budget bytes (q03/q05/q10 ~312 B, q12 ~5,968 B per 377).
- Classify the wall:
  - **Per-writer wall:** `streamed inner join build spill writer` from a single `SpilledInnerJoinBuild` exceeds budget.
    Grouping alone does not fix it; increase/split `P`.
  - **Aggregate-live wall:** individual partition writers fit, but the sum of active partitions live before probing
    exceeds budget. Grouping fixes it.
  - If both apply, size both.
- **Size P from the measured per-writer peak:** choose a target writer budget with headroom (for example ~400 MiB) and
  pick `P = max(memory_required_P, ceil(total_or_largest_partition_spill / target_writer_budget))`, rounded consistently
  with the existing partition-count policy and capped by the existing hard max. If `P` exceeds the cap -> stop-line
  residual, do not ship.
- **Size G from the measured aggregate-live peak:** choose a target group budget with headroom (for example ~400 MiB) and
  pick `G = ceil(total_groupable_spill / target_group_budget)`. Expect small `P`/`G` (likely around 2). If `G` is large
  enough to resemble the old per-partition scan loop -> stop-line residual, do not ship. `G == P` is acceptable when
  both are small.
- Confirm the per-group cleanup point (free/delete a group's spill before the next group) and the accounting split:
  live spill is budgeted per writer/group; cumulative bytes written remain telemetry.

## D1 - spill-budget-aware partitioning + G-pass bounded grouping
- Size the streamed inner-build partition count using both resident memory and spill budget (`P = max(memory P, spill
  P)`), preserving the existing hard cap and topology scope.
- Group the `P` partitions into G **ordered** groups. Prefer contiguous ascending partition ranges.
- For each group g in 0..G:
  - scan the build once; route rows whose bucket falls in group g into their per-partition spills (reuse 377's router +
    projected `write_build_keys`); skip rows not in group g;
  - rebuild/probe the group's partitions (377's one-partition-at-a-time within the group);
  - **free the group's spill** before the next group.
- Preserve the same observable partition order as 377: groups are processed in ascending partition order, and partitions
  within a group are probed in ascending order. Non-contiguous grouping is allowed only if it restores that order.
- Live disk spill = one group's payload; assert per-writer and live group spill stay < the spill budget. Keep budget
  producers/names correct. Cumulative `spill_bytes` telemetry may exceed one group; it must not drive the live budget
  decision.
- Topology-named; no qNN in core; no broad abstraction.

## Tests / safety net
- **Parity vs 377 single-pass:** G-pass output == single-pass output (row order, schema, join output) for the q07/q03-
  shaped inner chain (multi-batch, multi-partition, null + residual keys).
- **Per-writer bound:** with a synthetic build that exceeds budget under the old partition sizing, the spill-aware `P`
  keeps each partition writer under budget.
- **Live-group bound:** with a synthetic build that would exceed budget as one all-partition pass, the G-pass keeps live
  group spill under budget and completes.
- **Live-vs-cumulative accounting:** cumulative bytes written across groups may exceed one group, but the live budget
  decision is based on the active writer/group and cleanup releases the prior group.
- **Per-group cleanup:** a group's spill is freed before the next (no stale carryover; resident/peak assertion).
- **P/G sizing:** P and G derived from measured peaks (not hardcoded); cap/stop-line residual path is taken if P/G are
  not bounded.
- **q03 small-scale strict `pass@compare`** preserved.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut, spill path)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
# fixed-cap sf=10 family re-measure (persisted), per query q03,q05,q10,q12:
#   systemd-run --user --property=MemoryMax=4G ... target/release/toron-tpch perf --sf 10 --query q03,q05,q10,q12 \
#     --engines toron --data-source parquet --data-dir target/tpch/data ...
#   + correctness --query <green cell> --sf 10 --data-source parquet ... for any new green
git diff --check
#   wall classified (per-writer vs aggregate-live, projected); P/G from measured peaks; parity vs single-pass;
#   per-writer and live-group spill bounded; q03/q05/q10/q12 green-or-next-wall; no cap bump; --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/378_bounded_partition_group_spill_results.md`:
- Phase-0 confirmation (projected, not projection gap) + producer classification (per-writer vs aggregate-live) + each
  cell's per-writer peak / aggregate-live peak / partition_count / over-budget;
- the chosen P and G + how each was derived from the measured peaks;
- the G-pass grouping + per-group cleanup;
- parity (order/schema/output vs 377 single-pass) + per-writer / live-group spill-bound test results;
- **sf=10 family re-measure**: q03/q05/q10/q12 green or the new named wall (per cell); persisted correctness for greens;
- the bounded G-pass scan honesty (O(partitions) -> O(G)) and the live-vs-cumulative accounting split;
- confirmation no cap bump, spill peak bounded, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Wrong wall classification** - grouping alone cannot fix a per-writer spill wall; Phase 0 gates this.
- **P/G unbounded** - stop-line residual; not shipped as bounded.
- **Stale group spill** - explicit per-group free + assertion.
- **Parity drift** - G-pass vs single-pass output test.
- **New wall, not green** - honest green-or-next-wall.
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 378 bounded partition group spill`. Do not commit wip drafts or large `.perf.data`.
