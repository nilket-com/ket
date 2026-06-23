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
- Memory walls: q02, q07, q08, q09, q14, q17, q18, q19, q21.
- Bind walls: q02, q08, q09 at sf=1.
- Execute walls: q04, q12, q20 at sf=1.
- No time walls observed in this probe.
- No distributed-correctness divergence observed in the scale-stable correctness slice.
- q17 is reach-only at scale; its correctness oracle is fixed to sf=0.02 and is not scale-general.

## Reach atlas
Format: `class (stage; wall-clock / RSS)`

| query | sf=0.1 | sf=1 |
| --- | --- | --- |
| q01 | green (execute; 6.13s / 517904 KB) | green (execute; 24.93s / 2250728 KB) |
| q02 | memory (oom-kill; 6.40s / 1024 MB) | bind-fail (unsupported_cross_join; 3.6 ms / 386.7 MB) |
| q03 | green (execute; 5.71s / 643736 KB) | green (execute; 20.83s / 3364072 KB) |
| q04 | green (execute; 5.27s / 608484 KB) | execute-fail (unrecognized; 18.29s / 3090852 KB) |
| q05 | green (execute; 6.01s / 662804 KB) | green (execute; 27.80s / 3558492 KB) |
| q06 | green (execute; 4.48s / 515456 KB) | green (execute; 9.52s / 2250096 KB) |
| q07 | green (execute; 8.78s / 798388 KB) | memory (oom-kill; 18.51s / 4 GB) |
| q08 | memory (oom-kill; 7.33s / 1 GB) | bind-fail (unsupported_cross_join; 59.3 ms / 2.1 GB) |
| q09 | memory (oom-kill; 6.58s / 1 GB) | bind-fail (unsupported_cross_join; 65.7 ms / 2.1 GB) |
| q10 | green (execute; 5.73s / 620428 KB) | green (execute; 22.88s / 3135540 KB) |
| q11 | green (execute; 4.28s / 384308 KB) | green (execute; 22.99s / 912076 KB) |
| q12 | green (execute; 6.26s / 806136 KB) | execute-fail (unrecognized; 20.31s / 3667420 KB) |
| q13 | green (execute; 4.91s / 454188 KB) | green (execute; 14.42s / 1438460 KB) |
| q14 | green (execute; 18.20s / 755568 KB) | memory (oom-kill; 2m34.903s / 4 GB) |
| q15 | green (execute; 4.32s / 516784 KB) | green (execute; 9.97s / 2252740 KB) |
| q16 | green (execute; 4.09s / 379696 KB) | green (execute; 107.81s / 673536 KB) |
| q17 | green (execute; 19.15s / 759676 KB) | memory (oom-kill; 2m35.853s / 4 GB) |
| q18 | green (execute; 9.35s / 1043576 KB) | memory (oom-kill; 27.112s / 4 GB) |
| q19 | green (execute; 20.18s / 921560 KB) | memory (oom-kill; 2m42.773s / 4 GB) |
| q20 | green (execute; 4.64s / 522592 KB) | execute-fail (unrecognized; 16.54s / 2313768 KB) |
| q21 | green (execute; 10.45s / 895356 KB) | memory (oom-kill; 20.173s / 4 GB) |
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
- q16 remains the slowest compare-pass row at sf=1; q14/q17/q19/q21 hit the memory ceiling; q02/q08/q09 stop in
  bind; q04/q12/q20 fail in execute.
- q17 remains the fixed-scale outlier: reach-only at scale, correctness pinned to sf=0.02.

## Result
The scale atlas exists, is classified, and names the next arc from measured evidence instead of guesswork.
