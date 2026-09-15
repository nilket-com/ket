# ADR 590 - strix as the Toron measurement host: results

Measured 2026-09-15 on strix under the configuration ADR 590 froze, with
the executable built on mini. **The baseline did not seal.** Three full
boards ran; every one passed all 264 samples and every integrity item but
one. The first was refused for a non-fresh output directory (my host
snapshots had been written into it), the second and third for the
stability gate on one cell, DuckDB q11, which threw a single sample about
22% above its band in each. A forty-sample diagnosis ran that cell
restricted to the P-cores and then to the E-cores of the hybrid
i9-14900K: under the P-core restriction it repeats to 2.8%; under the
E-core restriction it is 1.78x slower; unrestricted it spans 106-132 ms.
That supports scheduling across the two core kinds as the hypothesis for
the refusals; it does not show that the unrestricted outliers migrated,
exclude a thermal or timer contribution, or show an effect in Toron's
cells. It is enough to make placement a coordinate item, since the
P-core restriction is the one condition under which the cell repeated,
and ADR 590's freeze left placement to the scheduler. Under the record as
written that is the finding: no tuning was applied, no board is a
candidate, and the placement pin goes to a successor record that also
makes the harness record it, so a pinned board is distinguishable from
an unpinned one by its artifact.

Toron's own cells were reproducible across the three boards: the sum of
Toron medians was 64.545, 64.632, and 64.624 s (within 0.14%), the worst
Toron spread 0.078 (q16, third board), and q21 9,048.6 / 9,078.3 /
9,099.1 ms. That is a first repeatability reading for strix, stated as
such and not compared to slim's.

## Identity

| identity | value |
|---|---|
| build host | mini (i9-14900, checkout `cf498086`, rustc 1.95.0 by `rustup override`) |
| run host | strix (i9-14900K, 8 P-cores + 16 E-cores, 123 GiB) |
| source tree | `6a697bcf91ea888009b2df8335f4909242d5d62e` (= `cf498086`, the plan commit) |
| Toron/harness executable | `331dd20ddc6efaa9163d1082cdcebe0ab94d0ebace0670d6675a073c69929428` |
| DuckDB executable | `c478c0f49f6755100d55dee32de485c0f00f3729b40b62658a83b0666b684707` (v1.5.4, ADR 583's bytes) |
| build recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` (slim's, reproduced on mini) |
| dataset manifest | `fnv1a64:0fc5c06d19c5ed23`, `full_hash` on every row |
| build manifest | `target/tpch/evidence/build/adr590-strix-board.manifest` (on mini and strix, staged hashes verified on strix) |

The recipe hash agreeing with slim's proves agreement on the fields the
recipe records; the tool versions were captured separately and agree
(clang 21.1.8, mold 2.40.4, binutils 2.46, glibc 2.43, kernel
`7.0.0-31-generic` on slim, mini, and strix). Whether the same tree builds
bit-identically on two hosts is unchecked.

## The frozen configuration, verified

Verified before each attempt from the snapshot files below, item for item
against the record: default target `multi-user.target`; `gdm` inactive
and disabled, `display-manager` inactive, no `gnome-shell`; the only
sessions the launching SSH session (type `tty`, no seat) and its user
manager; user services 9-10 (49 with the desktop up); `intel_pstate`
active, governor `powersave` on every CPU, energy-performance preference
`performance` on every CPU, `no_turbo = 0`, `hwp_dynamic_boost = 0`, SMT
on, 800 MHz-5.7 GHz, THP `madvise`, default mitigations, microcode
`0x133`, `perf_event_paranoid = 2`; join-spill scratch empty; output
directory absent (attempts 2 and 3). The desktop session was terminated
without root, `gdm` immediately respawned a greeter on tty1, and only
`systemctl disable --now gdm` (operator, root) ended it; the check covers
the display manager, not only the session, for that reason.

Host record, retained on strix under `target/tpch/adr590-strix-host-record/`
(SHA-256):

| file | sha256 |
|---|---|
| `tool_versions_run_host.txt` | `d88e2dc951f15e4c64bf2f6550b699e6543a8a30c19403f4bb35f0b60981acfd` |
| `attempt1/host_config_before.txt` | `b0fcbd45ab3f6c7b29066bbe74b8c5861e2adea98f075f8e84ca4d1a08b555bf` |
| `attempt1/host_after.txt` | `1bf32a64a00f6e177366022e2138e77ef1be3757b5b72537036323ab296ac10a` |
| `attempt2/host_config_before.txt` | `579eece0b60362e40f2a4090a98a1b2a631c3620399a7f0b5a65086a8c209154` |
| `attempt2/host_after.txt` | `589310d68d63bca9bc6a4c5017f0d837522d536b5ba71274c49c942736a65aea` |
| `attempt3/host_config_before.txt` | `2be0c217a2ebe6806573112186e35740ae05ba73f7fe2cd44083db43ba52f1f9` |
| `attempt3/host_after.txt` | `71ad4129a95582637104a45e3254121fb78061c810d96b48ba38deeee7954816` |

Each attempt's `perf_stdout.log` sits beside its snapshots. Package
temperature (coretemp, `lm-sensors` is not installed) was 40-43 °C at each
start and 52 °C immediately after attempts 1 and 2; attempt 3's after-line
was taken nine minutes after the board finished (08:31:27, from the
evidence file's timestamp) because the launching shell had been cut off,
and reads 44 °C at load 0.05. Load was 0.05-0.29 at every start.

## The three boards

All three ran `perf --engines toron,duckdb --query all --mode strict --sf 8
--data-source parquet --runs 5 --warmup 1` through `toron-bench run` with
`TORON_TPCH_MEMORY_PER_TASK` and `TORON_TPCH_SPILL_BYTES_RESERVED` at
2 GiB, `TORON_PARTITION_PARALLELISM=1`, `TORON_TPCH_DUCKDB_THREADS=1`,
and a dedicated empty `TORON_JOIN_SPILL_DIR`. Every board: 264 samples
passed, 44 summaries, host quiescent at start and end, spill scratch empty
before and after, every row `full_hash` with the dataset fingerprint,
DuckDB rows at one thread from the staged executable, parallelism 1 with
an empty observation list on Toron rows. Retained on strix:

| attempt | window (CDT) | directory | authority refusal | Toron sum | DuckDB sum | ratio of sums | median ratio |
|---|---|---|---|---:|---:|---:|---:|
| 1 | 07:44-07:58 | `adr590-strix-board-rejected-output-dir-not-fresh/` | `output_directory_fresh` | 64.545 s | 15.691 s | 4.114x | 4.06x |
| 2 | 08:00-08:15 | `adr590-strix-board-rejected-cell-unstable-q11-duckdb/` | `cell_unstable:q11:duckdb` | 64.632 s | 15.692 s | 4.119x | 4.08x |
| 3 | 08:16-08:31 | `adr590-strix-board/` | `cell_unstable:q11:duckdb` | 64.624 s | 15.702 s | 4.116x | 4.08x |

Attempt 1's refusal is procedural: the host snapshots were written into
the output directory before the run, and the gate requires it fresh. The
snapshots moved to the sibling host-record directory for attempts 2 and 3.
Its cells are otherwise a complete board and are kept as a repeatability
reading.

The stability gate is `(max - min) / median <= 0.20` per cell. DuckDB q11
samples, in run order: attempt 1 `110, 115, 115, 115, 112` (0.043);
attempt 2 `109, 114, 132, 107, 127` (0.219); attempt 3 `108, 110, 132,
108, 110` (0.218). DuckDB q02 in attempt 1: `131, 134, 137, 133, 153`
(0.164, under the gate). Toron's worst spreads: 0.034 (q11), 0.034
(q21), 0.078 (q16). The sixteen DuckDB `schema_mismatch` flags are the
informational ones ADR 583's board carried.

## The diagnosis

DuckDB q11 alone, 40 measured samples after 1 warmup, same manifest and
environment, three placements (`target/tpch/adr590-diag-q11-duckdb*/`):

| placement | min | median | max | spread |
|---|---:|---:|---:|---:|
| unpinned (as the boards ran) | 106 | 111.5 | 127 | 0.188 |
| `taskset -c 0-15` (P-cores, `/sys/devices/cpu_core/cpus`) | 106 | 107.0 | 109 | 0.028 |
| `taskset -c 16-31` (E-cores, `/sys/devices/cpu_atom/cpus`) | 189 | 191.0 | 193 | 0.021 |

Under the P-core restriction the cell repeats to 2.8%; under the E-core
restriction it is 1.78x slower and equally repeatable. Unrestricted, the
106-109 band coincides with the P-core time and the higher samples lie
between the two restricted bands, which is consistent with runs that
spent part of their time on an E-core; no placement was observed, so that
reading is the supported hypothesis, not a demonstration, and a thermal
or timer contribution to the individual outliers is not excluded by it.
What the diagnosis does establish is that the cell repeats under the
P-core restriction and does not, reliably, without it. Whether Toron's
cells carry an effect is not shown either way; a 22 ms excursion would be
0.2% of q21 and 20% of q11. No configuration was changed for the
diagnosis; `taskset` was applied to the diagnostic command only. slim's i9-12900K is hybrid as well (8 P + 8 E);
ADR 583's board sealed unpinned there, so its DuckDB q11 cell (139-144 ms)
carried the same hazard and happened not to trip.

## Host-to-host, explanatory only

Attempt 3's cells against ADR 583's sealed slim table. Unsealed, no
authority, no Toron change inferred: the Toron trees differ (583's
`9e29278d…` against `6a697bcf…`, six ADRs apart) as well as the host, and
DuckDB's own ratio (0.69-0.81) is the host's contribution alone.

| cell | strix Toron (ms) | strix DuckDB (ms) | ratio | gap (ms) | Toron spread | DuckDB spread | slim 583 Toron (ms) | slim 583 DuckDB (ms) | Toron strix/slim | DuckDB strix/slim |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q21 | 9,099.1 | 2,213.0 | 4.11x | 6,886.1 | 0.012 | 0.011 | 14,956.0 | 2,970.0 | 0.608 | 0.745 |
| q09 | 7,113.0 | 1,647.0 | 4.32x | 5,466.0 | 0.010 | 0.014 | 10,211.5 | 2,192.0 | 0.697 | 0.751 |
| q05 | 5,275.8 | 593.0 | 8.90x | 4,682.8 | 0.012 | 0.005 | 7,329.0 | 861.0 | 0.720 | 0.689 |
| q18 | 6,014.5 | 1,496.0 | 4.02x | 4,518.5 | 0.006 | 0.017 | 7,691.3 | 2,024.0 | 0.782 | 0.739 |
| q01 | 4,771.0 | 962.0 | 4.96x | 3,809.0 | 0.006 | 0.030 | 6,445.4 | 1,302.0 | 0.740 | 0.739 |
| q07 | 3,961.6 | 595.0 | 6.66x | 3,366.6 | 0.010 | 0.029 | 5,507.0 | 818.0 | 0.719 | 0.727 |
| q08 | 3,252.5 | 649.0 | 5.01x | 2,603.5 | 0.005 | 0.003 | 4,555.8 | 906.0 | 0.714 | 0.716 |
| q17 | 3,112.6 | 629.0 | 4.95x | 2,483.6 | 0.063 | 0.017 | 4,178.0 | 776.0 | 0.745 | 0.811 |
| q04 | 2,789.0 | 458.0 | 6.09x | 2,331.0 | 0.025 | 0.007 | 4,248.0 | 623.0 | 0.657 | 0.735 |
| q12 | 2,067.7 | 512.0 | 4.04x | 1,555.7 | 0.011 | 0.016 | 2,797.9 | 650.0 | 0.739 | 0.788 |
| q02 | 1,488.8 | 135.0 | 11.03x | 1,353.8 | 0.018 | 0.052 | 2,024.9 | 177.0 | 0.735 | 0.763 |
| q13 | 2,492.2 | 1,197.0 | 2.08x | 1,295.2 | 0.009 | 0.007 | 3,413.3 | 1,539.0 | 0.730 | 0.778 |
| q10 | 2,378.8 | 1,132.0 | 2.10x | 1,246.8 | 0.009 | 0.019 | 3,134.8 | 1,502.0 | 0.759 | 0.754 |
| q03 | 1,885.6 | 656.0 | 2.87x | 1,229.6 | 0.012 | 0.005 | 2,702.1 | 902.0 | 0.698 | 0.727 |
| q16 | 1,381.8 | 195.0 | 7.09x | 1,186.8 | 0.078 | 0.036 | 1,947.9 | 271.0 | 0.709 | 0.720 |
| q20 | 1,654.5 | 521.0 | 3.18x | 1,133.5 | 0.018 | 0.015 | 2,277.9 | 690.0 | 0.726 | 0.755 |
| q19 | 1,753.8 | 678.0 | 2.59x | 1,075.8 | 0.013 | 0.013 | 2,367.5 | 895.0 | 0.741 | 0.758 |
| q15 | 1,229.5 | 314.0 | 3.92x | 915.5 | 0.002 | 0.010 | 1,905.3 | 429.0 | 0.645 | 0.732 |
| q11 | 924.7 | 110.0 | 8.41x | 814.7 | 0.034 | 0.218 | 1,178.5 | 140.0 | 0.785 | 0.786 |
| q22 | 656.7 | 238.0 | 2.76x | 418.7 | 0.016 | 0.034 | 871.2 | 304.0 | 0.754 | 0.783 |
| q06 | 685.5 | 367.0 | 1.87x | 318.5 | 0.014 | 0.008 | 996.3 | 494.0 | 0.688 | 0.743 |
| q14 | 635.3 | 405.0 | 1.57x | 230.3 | 0.010 | 0.010 | 844.3 | 517.0 | 0.752 | 0.783 |
peak rss max toron 8151 MiB; spill max 27242808

DuckDB runs 0.69-0.81 of its slim time on strix across the cells; Toron
0.65-0.78 outside q21, and q21 0.61, carrying ADR 586-589's cuts as well. Peak RSS
on Toron rows was q05 at 8,152 MiB (8,520 MiB on attempt 2).

## Closure

ADR 590 closes with the freeze verified, the host record retained and
hashed, three boards retained, no sealed baseline, and a finding: on this
hybrid CPU the refused cell repeats under a P-core restriction and not
without one, so thread placement is a coordinate item, with scheduling
across core kinds the supported hypothesis for the refusals. The next owner is
therefore not a query yet; the arc's next owner selection waits on a
sealed strix board. Successor: ADR 591 records the CPU affinity of the
harness and every child in the artifact header and rows, requires the
P-core set as a coordinate item for both engines, and reruns and seals the
board. Nothing else under this number.

Ladders: no code changed under this record; the plan and this record are
the only files.
