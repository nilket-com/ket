# ADR 330 results - scale-atlas probe

The scale atlas is measured from the built `toron-tpch` binary under systemd cgroup limits:

```bash
scripts/cargo-cgroup build -p toron-tpch
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=1G \
  --property=RuntimeMaxSec=60s  /bin/sh -lc 'target/debug/toron-tpch run --query qNN --mode strict --sf 0.1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/debug/toron-tpch run --query qNN --mode strict --sf 1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=1G \
  --property=RuntimeMaxSec=60s  /bin/sh -lc 'target/debug/toron-tpch correctness --query qNN --sf 0.1'
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=RuntimeMaxSec=300s /bin/sh -lc 'target/debug/toron-tpch correctness --query qNN --sf 1'
```

## Summary
- Green at both scales: q01, q03, q05, q06, q10, q11, q13, q15, q16, q22.
- Original memory/cap walls: q02, q07, q08, q09, q14, q17, q18, q19, q21.
- Historical bind/cartesian walls: q02, q08, q09 at sf=1 (fixed by ADR 331).
- Execute walls: q04, q12, q20 at sf=1 (renamed by ADR 332 to `memory_budget_exhausted`).
- No time walls observed in this probe.
- No distributed-correctness divergence observed in the scale-stable correctness slice.
- q17 is reach-only at scale; its correctness oracle is fixed to sf=0.02 and is not scale-general.
- ADR 333 release re-measured q07/q14/q17/q18/q19/q21 with diagnostic retention. All six remain sf=1
  OOM-guard-gaps: systemd killed the process at 4G before the harness emitted a row.
- ADR 340 moved the q07/q21 sf=1 first wall off the ADR 335 `Join(0)` raw-output guard:
  q07 now fails at a later join-output materialization (requested 1,177,978,914), and q21 fails at the budgeted
  streamed-filter accumulator (requested 1,074,312,896). Both remain `memory_budget_exhausted@execute`.
- ADR 341 moved q21's sf=1 wall off the ADR 340 streamed-filter accumulator and onto the budgeted Exchange-output
  accumulator (requested 1,074,142,886). q07 remains at the later join-output wall.
- ADR 346 moved q17 from the `join output materialization` wall (requested 1,651,834,429) to `pass@execute` at sf=1.
  q17 correctness remains fixed at sf=0.02; no sf=1 `pass@compare` claim is made for q17.

## Reach atlas
Format: `class (stage; wall-clock / RSS)`

| query | sf=0.1 | sf=1 |
| --- | --- | --- |
| q01 | green (execute; 6.13s / 517904 KB) | green (execute; 24.93s / 2250728 KB) |
| q02 | memory (oom-kill; 6.40s / 1024 MB) | bind-fail (unsupported_cross_join; 3.6 ms / 386.7 MB) |
| q03 | green (execute; 5.71s / 643736 KB) | green (execute; 20.83s / 3364072 KB) |
| q04 | green (execute; 5.27s / 608484 KB) | execute-fail (memory_budget_exhausted; 2.08s / 2.9 GB) |
| q05 | green (execute; 6.01s / 662804 KB) | green (execute; 27.80s / 3558492 KB) |
| q06 | green (execute; 4.48s / 515456 KB) | green (execute; 9.52s / 2250096 KB) |
| q07 | green (execute; 8.78s / 798388 KB) | OOM-guard-gap (oom-kill; no row; 4.403s / 4 GB) |
| q08 | memory (oom-kill; 7.33s / 1 GB) | bind-fail (unsupported_cross_join; 59.3 ms / 2.1 GB) |
| q09 | memory (oom-kill; 6.58s / 1 GB) | bind-fail (unsupported_cross_join; 65.7 ms / 2.1 GB) |
| q10 | green (execute; 5.73s / 620428 KB) | green (execute; 22.88s / 3135540 KB) |
| q11 | green (execute; 4.28s / 384308 KB) | green (execute; 22.99s / 912076 KB) |
| q12 | green (execute; 6.26s / 806136 KB) | execute-fail (memory_budget_exhausted; 2.32s / 3.4 GB) |
| q13 | green (execute; 4.91s / 454188 KB) | green (execute; 14.42s / 1438460 KB) |
| q14 | green (execute; 18.20s / 755568 KB) | OOM-guard-gap (oom-kill; no row; 16.320s / 4 GB) |
| q15 | green (execute; 4.32s / 516784 KB) | green (execute; 9.97s / 2252740 KB) |
| q16 | green (execute; 4.09s / 379696 KB) | green (execute; 107.81s / 673536 KB) |
| q17 | green (execute; 19.15s / 759676 KB) | OOM-guard-gap (oom-kill; no row; 17.085s / 4 GB) |
| q18 | green (execute; 9.35s / 1043576 KB) | OOM-guard-gap (oom-kill; no row; 7.236s / 4 GB) |
| q19 | green (execute; 20.18s / 921560 KB) | OOM-guard-gap (oom-kill; no row; 19.189s / 4 GB) |
| q20 | green (execute; 4.64s / 522592 KB) | execute-fail (memory_budget_exhausted; 1.47s / 2.1 GB) |
| q21 | green (execute; 10.45s / 895356 KB) | OOM-guard-gap (oom-kill; no row; 4.295s / 4 GB) |
| q22 | green (execute; 4.10s / 405816 KB) | green (execute; 7.98s / 944820 KB) |

## Correctness slice
Scale-stable correctness was run only for the queries that still reached compare at sf=1.

| query | sf=0.1 | sf=1 |
| --- | --- | --- |
| q01 | pass@compare (5.75s / 519272 KB) | pass@compare (27.97s / 2250412 KB) |
| q03 | pass@compare (5.67s / 643736 KB) | pass@compare (24.29s / 3363724 KB) |
| q05 | pass@compare (6.02s / 663176 KB) | pass@compare (31.59s / 3558520 KB) |
| q06 | pass@compare (4.31s / 515464 KB) | pass@compare (10.39s / 2250528 KB) |
| q10 | pass@compare (5.47s / 619192 KB) | pass@compare (23.13s / 3137604 KB) |
| q11 | pass@compare (3.94s / 384076 KB) | pass@compare (21.04s / 910340 KB) |
| q13 | pass@compare (5.09s / 455420 KB) | pass@compare (15.19s / 1440044 KB) |
| q15 | pass@compare (4.69s / 516540 KB) | pass@compare (11.19s / 2253116 KB) |
| q16 | pass@compare (4.30s / 379484 KB) | pass@compare (104.63s / 672660 KB) |
| q22 | pass@compare (4.38s / 405108 KB) | pass@compare (9.10s / 943952 KB) |

## Interpretation
- The sf=1 board is not uniformly green. Several queries hold, but there are clear bind walls, execute walls, and
  memory walls.
- The scale arc therefore points to follow-on scale work before any broad logical-completeness branch.
- q16 remains the slowest compare-pass row at sf=1; ADR 333 shows q07/q14/q17/q18/q19/q21 still hit the cgroup
  ceiling before emitting a row; q02/q08/q09 were the original bind/cartesian family (fixed by ADR 331);
  q04/q12/q20 fail in execute with `memory_budget_exhausted`.
- q04/q12/q20's execute walls were renamed by ADR 332: the raw error is `BudgetExhausted { dimension: Memory, ... }`,
  classified as `memory_budget_exhausted` and left as `fail`, not a recognized blocker.
- q17 remains the fixed-scale correctness outlier: reach is now green at sf=1 after ADR 346, while correctness remains
  pinned to sf=0.02.

## Result
The scale atlas exists, is classified, and names the next arc from measured evidence instead of guesswork.

## Later Delta - ADR 340

ADR 340 is the first q07/q21 streaming-substrate wall-mover. It does not make either cell green, but it changes the
first sf=1 wall:

| query | pre-ADR 340 first wall | post-ADR 340 first wall |
| --- | --- | --- |
| q07 | `Join(0)` raw output materialization, requested 1,975,900,039 | later join output materialization, requested 1,177,978,914 |
| q21 | `Join(0)` raw output materialization, requested 1,975,900,039 | streamed-filter accumulator, requested 1,074,312,896 |

The atlas classification remains `memory_budget_exhausted@execute`; the producer/bytes now identify the next staged
substrate cuts.

## Later Delta - ADR 341

ADR 341 streams ADR 340's filtered batches into the Exchange before the Filter result `Vec` exists. The current
materialize-each executor still requires Exchange to return `Vec<RecordBatch>`, so this is Branch 2 from the plan:
q21 remains red, but at the next named wall.

| query | pre-ADR 341 first wall | post-ADR 341 first wall |
| --- | --- | --- |
| q07 | later join output materialization, requested 1,177,978,914 | unchanged: later join output materialization, requested 1,177,978,914 |
| q21 | streamed-filter accumulator, requested 1,074,312,896 | streamed exchange output accumulator, requested 1,074,142,886 |

The atlas classification remains `memory_budget_exhausted@execute`; ADR 342 owns the consumer streaming /
join-to-join boundary needed to move past the returned Exchange output.

## Later Delta - ADR 345

ADR 345 streams and spills the semi/anti existence-set build instead of retaining the full existence batch. q21 moves
from the ADR 344 semi existence-set wall to green at `sf=1`.

| query | pre-ADR 345 first wall | post-ADR 345 result |
| --- | --- | --- |
| q21 | `semi/anti existence-set build/spill accumulator`, requested 1,526,014,069 | `pass@execute` at sf=1; `pass@compare` at sf=1 |

This leaves the current strict sf=1 memory walls as q04, q12, q18, and q20.

## Later Delta - ADR 346

ADR 346 streams q17's correlated-scalar LeftOuter recombine path after making the filtered Join(0) visitor
spilled-capable. q17 moves from the ADR 335 output-materialization wall to green reachability at `sf=1`.

| query | pre-ADR 346 first wall | post-ADR 346 result |
| --- | --- | --- |
| q17 | `join output materialization`, requested 1,651,834,429 | `pass@execute` at sf=1 |

q17 remains a fixed-scale correctness case: its oracle is pinned to sf=0.02, so this delta updates reachability only.
The current strict sf=1 memory walls remain q04, q12, q18, and q20; q02 remains a separate
`unrecognized@execute` wall.
