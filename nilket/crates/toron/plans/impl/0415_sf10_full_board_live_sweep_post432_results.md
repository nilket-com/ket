# ADR 415 redux results - post-432 sf=10 full-board live sweep

## Verdict

The post-432 redux completes the live sf=10 census under the unchanged fixed 4 GiB envelope:

- **All 22 queries reach output and pass a separate persisted strict comparison.** The consolidated board is
  **22/22 green** on one release binary.
- **q05 passes twice consecutively** with fresh disk-backed scratch, satisfying ADR 415's reproducibility protocol;
  a third attempt was therefore not run.
- **All 22 plan fingerprints match their latest live green receipts.** There is no plan drift hidden by final-result
  agreement.
- **DuckDB completes all 22 reference cells** with one warmup plus seven measured fresh child processes per cell.
- Every Toron reach, correctness, and DuckDB cell ran in a fresh systemd scope. `target/tpch/join_spill` was empty
  before and after every scoped process.

This is a measurement result. It changes no engine source and makes no repeated-run Toron performance claim. Its
purpose is to prove that the ten cells red in the original ADR 415 census and the twelve original controls hold
together live on the sealed post-432 binary.

## Conditions and provenance

| item | value |
|---|---|
| board seal commit | `7488ddff01b6dd9c04f2dd9eec0223b65f022529` |
| binary SHA-256 | `cca277b41ca6bf87dc401e0bb97cbdaff896d7cf751bd4859ac5539eda7a02ac` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| DuckDB | `v1.5.4 (Variegata) 08e34c447b` |
| scope | `MemoryMax=4G`, `MemoryHigh=4G`, `MemorySwapMax=0`, `TasksMax=512` |
| reach timeout | 360 seconds per cell |
| correctness timeout | 600 seconds per cell |
| join spill scratch | `target/tpch/join_spill` on ext4; empty before and after every scope |
| data | persisted strict sf=10 Parquet |
| artifact root | `target/tpch/adr415_post432/` |

The release binary was built once from the sealed commit with the `polars-sql-reference` feature and reused for all
subprocesses. The ext4 filesystem had 117 GiB free at preflight. The evidence directory is separate from the
permanent join-spill scratch, so normal cleanup did not erase the receipts. Completed user units reported
`MemoryPeak` as unset; as in ADR 428, successful rows therefore record `/usr/bin/time -v` maximum RSS. Hard-failure
fallback to cgroup `MemoryPeak` was not needed.

## Live census

Times are one live Toron reach attempt, not medians. q05 uses attempt 1 here and carries both required attempts in
the dedicated table below. Spill is Toron's committed telemetry, not residual disk usage. Every green status is
backed by the independently scoped `pass@compare`, not by row count or digest alone.

| query | reach | Toron ms | peak RSS (KiB) | spill bytes | plan fingerprint | correctness | scratch | status |
|---|---|---:|---:|---:|---|---|---|---|
| q01 | pass | 25,180.154 | 28,380 | 0 | `395ed6eff1196fd9` | pass@compare | empty | green |
| q02 | pass | 8,500.881 | 1,447,608 | 234,983,488 | `cf586e71755bbca3` | pass@compare | empty | green |
| q03 | pass | 17,737.733 | 531,360 | 233,338,304 | `666e0ac028036f70` | pass@compare | empty | green |
| q04 | pass | 6,058.440 | 1,232,864 | 0 | `61ccf3a19b2a9dd9` | pass@compare | empty | green |
| q05 | pass x2 | 185,962.156 | 3,056,452 | 5,353,985,328 | `757d7c625669b21a` | pass@compare | empty | green |
| q06 | pass | 1,293.084 | 27,408 | 0 | `6a75fa54204b16de` | pass@compare | empty | green |
| q07 | pass | 15,586.315 | 983,940 | 134,732,480 | `f0bf3fb0ae7aef26` | pass@compare | empty | green |
| q08 | pass | 22,917.928 | 1,079,168 | 0 | `cb686ae2c1202c97` | pass@compare | empty | green |
| q09 | pass | 34,611.533 | 2,818,256 | 108,992,256 | `5fc5d571c280b8df` | pass@compare | empty | green |
| q10 | pass | 13,158.102 | 1,700,656 | 215,703,848 | `4e9ea5a292a03a47` | pass@compare | empty | green |
| q11 | pass | 5,395.668 | 1,015,720 | 166,552,400 | `02e70dc5eac79715` | pass@compare | empty | green |
| q12 | pass | 6,340.735 | 1,432,616 | 159,510,976 | `ba50c30d4b80e733` | pass@compare | empty | green |
| q13 | pass | 147,289.325 | 959,212 | 9,152,992 | `ac3784c060c3c226` | pass@compare | empty | green |
| q14 | pass | 5,138.092 | 287,756 | 36,464,000 | `2eccb7ecdb30faa6` | pass@compare | empty | green |
| q15 | pass | 5,286.319 | 104,456 | 0 | `d34599f946ebfff6` | pass@compare | empty | green |
| q16 | pass | 4,596.545 | 762,872 | 60,078,400 | `c01a7fb9e82911d0` | pass@compare | empty | green |
| q17 | pass | 59,223.399 | 1,113,912 | 961,584 | `97ca111db986c14d` | pass@compare | empty | green |
| q18 | pass | 221,677.856 | 1,236,128 | 4,380,793,312 | `d8b60e3a03bfaf79` | pass@compare | empty | green |
| q19 | pass | 81,793.830 | 272,164 | 51,471,552 | `65c3c2c0b0a99063` | pass@compare | empty | green |
| q20 | pass | 22,808.126 | 3,362,132 | 256,104,392 | `08eb04262c519cbf` | pass@compare | empty | green |
| q21 | pass | 86,353.457 | 844,984 | 2,286,556,056 | `e2e25953a6a3fefd` | pass@compare | empty | green |
| q22 | pass | 3,444.138 | 680,252 | 31,509,620 | `ffd45ab430a864ab` | pass@compare | empty | green |

The heaviest successful process-RSS controls remain q20 at 3,362,132 KiB and q05 at approximately 3,056,000 KiB.
During q05 attempt 1, a live cgroup sample reached the exact 4 GiB peak while 5.0 GiB of disk-backed spill existed;
execution continued through reclaim and completed instead of OOM. Normal completion removed that spill before the
scope's scratch check.

## Closure provenance

Every cell red in ADR 415 maps to the complete wall lineage that made this 22/22 result possible. This is a
wall-to-ADR map, not a lossy one-ADR-per-cell summary.

| cell | closure lineage |
|---|---|
| q07 / q09 | compact-index residency -> ADR 416 |
| q13 | scan-output accumulator -> ADR 417 |
| q17 | scan-output reach -> ADR 417; oracle boundary fidelity -> ADR 431 |
| q18 | inner-chain composition -> ADR 418; membership prerequisite -> ADR 423; engine completion -> ADR 424; oracle residency -> ADR 425 |
| q22 | resident page locator -> ADR 419; singleton-page probe amplification -> ADR 420 |
| q21 | scan-output reach -> ADR 417; existence-summary residency -> ADR 421; throughput diagnosis/fix -> ADRs 426-427; oracle residency -> ADR 428 |
| q11 | scale-degenerate fixture/oracle contract -> ADR 422 |
| q05 | residency attribution -> ADR 429; disk-backed spill placement -> ADR 430 |
| q02 | join-output monolith -> ADR 432 |

The twelve cells already green in ADR 415 - q01, q03, q04, q06, q08, q10, q12, q14, q15, q16, q19, and q20 -
carry no closure lineage. All twelve re-proved persisted comparison as controls.

## q05 consecutive-pass resolution

ADR 415 requires two consecutive passes with fresh scratch before q05 can be called green. Both attempts passed, so
the protocol stopped after attempt 2. CPU is `/usr/bin/time` user plus system time; the peak column is process RSS
because the completed user unit did not retain its cgroup `MemoryPeak` property.

| attempt | result | wall | CPU (user + system) | peak RSS (KiB) | spill bytes | scratch after |
|---|---|---:|---:|---:|---:|---|
| 1 | pass | 3:09.87 | 157.76 + 28.69 s | 3,056,452 | 5,353,985,328 | empty |
| 2 | pass | 3:04.65 | 155.66 + 28.70 s | 3,056,284 | 5,353,985,328 | empty |
| 3 | not run | n/a | n/a | n/a | n/a | two consecutive passes already satisfied the protocol |

The attempts have identical plan fingerprints and spill telemetry. The separate persisted q05 comparison passed in
196,908.815 ms with 3,363,460 KiB maximum RSS and empty scratch.

## Fingerprint stability reconciliation

The twelve original controls compare against the retained ADR 415 raw reach artifacts. Formerly-red cells compare
against the latest closing engine receipt; oracle-only follow-ons do not change the engine plan. Typed correctness
remains an independent gate even though every fingerprint matches.

| cell | this run | reference | source | verdict |
|---|---|---|---|---|
| q01 | `395ed6eff1196fd9` | `395ed6eff1196fd9` | ADR 415 raw reach receipt | match |
| q02 | `cf586e71755bbca3` | `cf586e71755bbca3` | ADR 432 results | match |
| q03 | `666e0ac028036f70` | `666e0ac028036f70` | ADR 415 raw reach receipt | match |
| q04 | `61ccf3a19b2a9dd9` | `61ccf3a19b2a9dd9` | ADR 415 raw reach receipt | match |
| q05 | `757d7c625669b21a` | `757d7c625669b21a` | ADR 430 results | match |
| q06 | `6a75fa54204b16de` | `6a75fa54204b16de` | ADR 415 raw reach receipt | match |
| q07 | `f0bf3fb0ae7aef26` | `f0bf3fb0ae7aef26` | ADR 416 results | match |
| q08 | `cb686ae2c1202c97` | `cb686ae2c1202c97` | ADR 415 raw reach receipt | match |
| q09 | `5fc5d571c280b8df` | `5fc5d571c280b8df` | ADR 416 results | match |
| q10 | `4e9ea5a292a03a47` | `4e9ea5a292a03a47` | ADR 415 raw reach receipt | match |
| q11 | `02e70dc5eac79715` | `02e70dc5eac79715` | ADR 422 results | match |
| q12 | `ba50c30d4b80e733` | `ba50c30d4b80e733` | ADR 415 raw reach receipt | match |
| q13 | `ac3784c060c3c226` | `ac3784c060c3c226` | ADR 417 results | match |
| q14 | `2eccb7ecdb30faa6` | `2eccb7ecdb30faa6` | ADR 415 raw reach receipt | match |
| q15 | `d34599f946ebfff6` | `d34599f946ebfff6` | ADR 415 raw reach receipt | match |
| q16 | `c01a7fb9e82911d0` | `c01a7fb9e82911d0` | ADR 415 raw reach receipt | match |
| q17 | `97ca111db986c14d` | `97ca111db986c14d` | ADR 431 results | match |
| q18 | `d8b60e3a03bfaf79` | `d8b60e3a03bfaf79` | ADR 424 results | match |
| q19 | `65c3c2c0b0a99063` | `65c3c2c0b0a99063` | ADR 415 raw reach receipt | match |
| q20 | `08eb04262c519cbf` | `08eb04262c519cbf` | ADR 415 raw reach receipt | match |
| q21 | `e2e25953a6a3fefd` | `e2e25953a6a3fefd` | ADR 427 results | match |
| q22 | `ffd45ab430a864ab` | `ffd45ab430a864ab` | ADR 420 results | match |

## DuckDB reference

Each cell ran in a fresh 4 GiB/no-swap systemd scope. Within the cell, the ADR 356 runner used one warmup and seven
measured fresh child processes. All rows passed with `flags=none`. Ratios divide the single Toron census reach by
the DuckDB median and are normalization only, not Toron repeated-run performance claims.

| query | DuckDB median ms | Toron/DuckDB | query | DuckDB median ms | Toron/DuckDB |
|---|---:|---:|---|---:|---:|
| q01 | 200 | 125.9x | q12 | 117 | 54.2x |
| q02 | 59 | 144.1x | q13 | 285 | 516.8x |
| q03 | 242 | 73.3x | q14 | 118 | 43.5x |
| q04 | 123 | 49.3x | q15 | 114 | 46.4x |
| q05 | 176 | 1,056.6x | q16 | 88 | 52.2x |
| q06 | 78 | 16.6x | q17 | 174 | 340.4x |
| q07 | 169 | 92.2x | q18 | 322 | 688.4x |
| q08 | 218 | 105.1x | q19 | 155 | 527.7x |
| q09 | 440 | 78.7x | q20 | 146 | 156.2x |
| q10 | 782 | 16.8x | q21 | 556 | 155.3x |
| q11 | 41 | 131.6x | q22 | 67 | 51.4x |

## Comparison to ADR 415

| axis | ADR 415 | post-432 redux |
|---|---|---|
| green cells | 12/22 | **22/22** |
| non-green families | four scan-output, two spilled-index, one join-output, one writer-resident, q05 OOM, q11 oracle | none |
| q05 | OOM x3 at the exact cgroup cap | pass x2 plus persisted pass@compare |
| correctness | 12 green comparisons; q11 and q17 instrument gaps | 22/22 persisted pass@compare |
| plan stability | per-cell plans not consolidated | 22/22 match latest live receipt |
| board state | fully classified, not fully green | fully green under the fixed envelope |

The established forensic controls remain exact: q03 spills 233,338,304 bytes, q10 215,703,848, q12 159,510,976,
and q19 51,471,552. q20's 3,362,132 KiB peak matches its post-415 residency-watch band. q10's 13,158 ms row
retains the post-ADR-416 improved single-session band; it is not presented as a new speed result. No formerly-green
cell regressed into a correctness, plan, spill-signature, or residency finding.

## Hygiene

- Live persisted sf=10 rows only; no matrix or correctness-cache row was used.
- Measurement-only: no Toron engine source changed.
- Fixed 4 GiB/no-swap scope; no cap, multiplier, timeout, or cgroup relaxation.
- Scratch was empty before and after every Toron reach, Toron correctness, and DuckDB cell.
- DuckDB reference: 22/22 attempted and completed.
- Evidence remains under `target/tpch/adr415_post432/`; join-spill scratch is empty after the sweep.
- No `.perf.data` is part of this change.

## Forward arc

This redux is the consolidated closing artifact: one sealed binary, 22 isolated envelopes, 22 live reaches, and 22
persisted comparisons. It is a census, not the article rail. Article-grade numbers come next from the separate
ADR 356 repeated-run method (`perf --runs 7 --warmup 1`, fresh processes, median/min/spread, stability gate) across
the Toron/DuckDB full board and the currently supported Polars subset. Any decision-worthy substrate gap exposed by
that baseline can then justify a new ADR; collecting the baseline itself does not.
