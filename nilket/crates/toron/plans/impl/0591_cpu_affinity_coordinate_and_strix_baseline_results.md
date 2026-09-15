# ADR 591 - CPU affinity as a recorded coordinate item, and strix's sealed baseline: results

Measured 2026-09-15 on strix under ADR 590's frozen configuration plus the
P-core affinity `0-15`, applied by the harness and recorded on the header
and every row, with the executable built on mini. **One board ran, every
gate passed, and it sealed with final authority**: 264 samples passed, 44
summaries stable, host quiescent at both ends, output directory fresh,
join-spill scratch empty before and after, every row `full_hash` on the
dataset fingerprint, DuckDB at one thread from the staged executable, the
parallelism pin at one with an empty observation list, and affinity `0-15`
requested, explicit, and observed on the header and all 264 rows. DuckDB
q11, the cell that refused ADR 590's boards twice, ran 107, 107, 108, 108,
108 ms. strix's canonical baseline is this board; the next owner by
absolute Toron-minus-DuckDB gap is **q21 at 6,916.9 ms**, with q09 second
at 5,518.0 ms.

## Sealed identity

| identity | value |
|---|---|
| source tree | `a962f3d80f63cd1d4ea06e8ffed624681dec3aaa` |
| commit hint | `3c9df2bc` (the harness change and its tests, before this record was folded in; the closing commit differs from it in the plan documents alone) |
| evidence tag | `evidence/a962f3d80f63-d03935eb49cc` (on the tree; fetched to slim for the canonical remote) |
| artifact SHA-256 | `d03935eb49cce8a49ef709155f715797bee03f17fdf3d331d0df6c579ee7e9c1` |
| sealed manifest SHA-256 | `19c8a91ac029fa37b90464afa10812abed1322cd42afd6ccf50d6732ceb44743` |
| harness/Toron SHA-256 | `950ebc4b46642a32e6069b0a737bb6f289a6c7e6a221e60b1b937a907bc99835` |
| DuckDB SHA-256 | `c478c0f49f6755100d55dee32de485c0f00f3729b40b62658a83b0666b684707` (v1.5.4, ADR 583's bytes) |
| build recipe SHA-256 | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` (slim's, reproduced on mini) |
| dataset manifest fingerprint | `fnv1a64:0fc5c06d19c5ed23` |
| build host / run host | mini (i9-14900) / strix (i9-14900K, 8 P + 16 E cores, 123 GiB) |
| cpu_affinity_requested / provenance / observed | `0-15` / `explicit` / `0-15` |

Evidence lives on strix under `~/work/ket/nilket/target/tpch/`:
`adr591-strix-board/` (artifact, run manifest, sealed manifest),
`evidence/archive/` (both executables), `evidence/build/adr591-strix-board.manifest`
(also on mini), and `adr591-strix-host-record/`. The seal's tag push went to
a local bare remote on strix (`~/work/ket-seal-remote.git`), since strix has
no key for the canonical remote; slim fetched the tag from strix's checkout
and pushes it with the closing commit. Tool versions agree across slim,
mini, and strix (clang 21.1.8, mold 2.40.4, binutils 2.46, glibc 2.43,
kernel `7.0.0-31-generic`); cross-host bit-reproducibility of the build
remains unchecked.

## The coordinate, verified

ADR 590's freeze item for item, from the snapshot taken at 09:37:41 CDT
before the run: `multi-user.target`, `gdm` inactive and disabled,
`display-manager` inactive, no `gnome-shell`, only the launching SSH
session and its user manager; `intel_pstate` `powersave` with EPP
`performance` on every CPU, turbo on, SMT on, 800 MHz-5.7 GHz, THP
`madvise`, default mitigations, microcode `0x133`,
`perf_event_paranoid = 2`; load 0.22, package 42 °C, spill scratch empty,
output directory absent. `/sys/devices/cpu_core/cpus` = `0-15` and
`/sys/devices/cpu_atom/cpus` = `16-31`, quoted in the snapshot beside the
requested cpulist. After the run (09:52:39, the board finished 09:52:18):
load 0.72, package 50 °C, `gdm` inactive, spill scratch empty.

Placement evidence beyond the rows' own claim, taken during the run:
the `toron-bench` wrapper (which applies nothing) showed
`Cpus_allowed_list: 0-31`; the harness process it launched showed `0-15`;
a live DuckDB child (`c478c0f4…`, pid 175096, caught at 09:50:26) showed
`0-15`. That is the inheritance chain the record describes, observed on
the processes themselves.

Host record (SHA-256):

| file | sha256 |
|---|---|
| `host_config_before.txt` | `f6d34c6183b55bae946817d19e583115ffa2eacb945957f83c5dba02aab1dd3b` |
| `host_after.txt` | `f54cf7f64c2c838cae306ffe24e7583d66ea0a82d659dd1751cbb5f867680463` |
| `tool_versions_run_host.txt` | `d88e2dc951f15e4c64bf2f6550b699e6543a8a30c19403f4bb35f0b60981acfd` |
| `perf_stdout.log` | `7703e357314b6a919d386ac2d409b3a2b06f71edbde46eed745800531bc601d1` |

## The board

`perf --engines toron,duckdb --query all --mode strict --sf 8 --data-source
parquet --runs 5 --warmup 1` through `toron-bench run`, with
`TORON_TPCH_CPU_AFFINITY=0-15`, 2 GiB task memory and spill reserve,
`TORON_PARTITION_PARALLELISM=1`, `TORON_TPCH_DUCKDB_THREADS=1`, and a
dedicated empty `TORON_JOIN_SPILL_DIR`. Aggregates: sum of Toron medians
**64.836 s**, sum of DuckDB medians **15.683 s**, ratio of summed medians
**4.134x**, median per-cell ratio **4.08x**. Worst spreads: Toron 0.092
(q06), DuckDB 0.051 (q22); every cell under the 0.20 gate. Peak RSS on
Toron rows: q05 at 8,520 MiB. The sixteen DuckDB `schema_mismatch` flags
are ADR 583's informational ones.

Cells ranked by absolute gap, with ADR 583's sealed slim table (different
host, Toron tree six ADRs older, DuckDB the same bytes) and ADR 590's
third unsealed strix board (same host, unpinned, Toron tree one commit
older) beside them. Both comparisons are explanatory: the first is
host-to-host and refused as incomparable by the gate (no affinity field on
583's board), the second is unsealed.

| cell | Toron (ms) | DuckDB (ms) | ratio | gap (ms) | Toron spread | DuckDB spread | ADR 583 slim Toron | ADR 583 slim DuckDB | ADR 590 unsealed Toron | ADR 590 unsealed DuckDB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q21 | 9,126.9 | 2,210.0 | 4.13x | 6,916.9 | 0.022 | 0.013 | 14,956.0 | 2,970.0 | 9,099.1 | 2,213.0 |
| q09 | 7,161.0 | 1,643.0 | 4.36x | 5,518.0 | 0.004 | 0.012 | 10,211.5 | 2,192.0 | 7,113.0 | 1,647.0 |
| q05 | 5,347.5 | 593.0 | 9.02x | 4,754.5 | 0.007 | 0.030 | 7,329.0 | 861.0 | 5,275.8 | 593.0 |
| q18 | 6,068.9 | 1,503.0 | 4.04x | 4,565.9 | 0.010 | 0.014 | 7,691.3 | 2,024.0 | 6,014.5 | 1,496.0 |
| q01 | 4,804.7 | 958.0 | 5.02x | 3,846.7 | 0.005 | 0.030 | 6,445.4 | 1,302.0 | 4,771.0 | 962.0 |
| q07 | 3,952.2 | 596.0 | 6.63x | 3,356.2 | 0.021 | 0.003 | 5,507.0 | 818.0 | 3,961.6 | 595.0 |
| q08 | 3,266.6 | 649.0 | 5.03x | 2,617.6 | 0.010 | 0.026 | 4,555.8 | 906.0 | 3,252.5 | 649.0 |
| q17 | 3,090.2 | 625.0 | 4.94x | 2,465.2 | 0.005 | 0.006 | 4,178.0 | 776.0 | 3,112.6 | 629.0 |
| q04 | 2,790.2 | 457.0 | 6.11x | 2,333.2 | 0.020 | 0.024 | 4,248.0 | 623.0 | 2,789.0 | 458.0 |
| q12 | 2,061.3 | 511.0 | 4.03x | 1,550.3 | 0.011 | 0.008 | 2,797.9 | 650.0 | 2,067.7 | 512.0 |
| q02 | 1,504.9 | 129.0 | 11.67x | 1,375.9 | 0.032 | 0.023 | 2,024.9 | 177.0 | 1,488.8 | 135.0 |
| q13 | 2,515.8 | 1,196.0 | 2.10x | 1,319.8 | 0.018 | 0.006 | 3,413.3 | 1,539.0 | 2,492.2 | 1,197.0 |
| q03 | 1,881.9 | 653.0 | 2.88x | 1,228.9 | 0.011 | 0.017 | 2,702.1 | 902.0 | 1,885.6 | 656.0 |
| q10 | 2,362.0 | 1,144.0 | 2.06x | 1,218.0 | 0.008 | 0.018 | 3,134.8 | 1,502.0 | 2,378.8 | 1,132.0 |
| q16 | 1,350.1 | 191.0 | 7.07x | 1,159.1 | 0.009 | 0.026 | 1,947.9 | 271.0 | 1,381.8 | 195.0 |
| q20 | 1,661.9 | 519.0 | 3.20x | 1,142.9 | 0.023 | 0.023 | 2,277.9 | 690.0 | 1,654.5 | 521.0 |
| q19 | 1,759.7 | 679.0 | 2.59x | 1,080.7 | 0.004 | 0.006 | 2,367.5 | 895.0 | 1,753.8 | 678.0 |
| q15 | 1,239.0 | 313.0 | 3.96x | 926.0 | 0.012 | 0.003 | 1,905.3 | 429.0 | 1,229.5 | 314.0 |
| q11 | 899.1 | 108.0 | 8.33x | 791.1 | 0.009 | 0.009 | 1,178.5 | 140.0 | 924.7 | 110.0 |
| q22 | 667.0 | 236.0 | 2.83x | 431.0 | 0.055 | 0.051 | 871.2 | 304.0 | 656.7 | 238.0 |
| q06 | 683.5 | 366.0 | 1.87x | 317.5 | 0.092 | 0.014 | 996.3 | 494.0 | 685.5 | 367.0 |
| q14 | 641.6 | 404.0 | 1.59x | 237.6 | 0.002 | 0.002 | 844.3 | 517.0 | 635.3 | 405.0 |

Against ADR 590's unpinned board, Toron cells moved by −2.8% to +1.6% and
DuckDB cells by −4.4% to +1.1%; the sums moved +0.3% (Toron) and −0.1%
(DuckDB). As an observation from one board: the cells that carried the
refusals repeated inside a 1% band here (q11 DuckDB 0.009, q02 DuckDB
0.023) while the aggregates stayed close to the unpinned boards'. One
board does not establish that the pin caused the narrowing, nor the
absence of a level effect. Against slim's ADR 583 board, DuckDB runs at 0.69-0.81 of
its slim time and Toron at 0.65-0.78 outside q21 (q21 0.61 carries ADR
586-589's cuts as well); the ratio of sums is 4.134x against 4.365x, which
is a statement about two hosts and two trees, not about Toron.

## Next owner

q21, by 6,916.9 ms of absolute gap, 1,398.9 ms ahead of q09. That confirms
the hypothesis ADR 590's unsealed boards offered; the selection is made on
this sealed board. Attribution on strix precedes any cut: ADR 584's
attribution and ADR 585's decomposition drivers apply, under this
coordinate, before any owner from slim's chain is assumed.

## Implementation and verification

`toron_tpch::affinity`: cpulist parse and normalize; `apply_affinity`
applies with `sched_setaffinity` on the calling thread and refuses a
readback that differs from the request, naming both cpulists;
`apply_from_environment` runs first in the synchronous `main`, before the
Tokio runtime is built, and fixes the coordinate for the process;
`PerfAttempt::json_value` stamps `cpu_affinity_requested`, its provenance,
and `cpu_affinity_observed` (the serializing thread's mask, read at each
row) on every perf row; the board header carries the same three from the
finishing thread. Gate: provenance `explicit`
(`coordinate_mismatch:cpu_affinity_requested_provenance:expected_explicit`,
the harness's form for provenance fields), header observed equal to
requested (`cpu_affinity_mismatch:header:observed`), and on every row
requested equal to the header's and observed equal to requested
(`cpu_affinity_mismatch:<query>:<engine>:requested|observed|provenance`); comparison
requires equal requested affinity and names an absent field
(`board_coordinate_absent:cpu_affinity_requested:<side>`). Tests: cpulist
round trips and refusals; an applied mask observed on a thread spawned
afterwards, on a worker of a Tokio runtime built afterwards, and on an
executed child via `/proc/self/status`; a request the host narrows (the
current mask plus the first unconfigured CPU id) refused with both lists
named while `sched_setaffinity` itself succeeds; the default coordinate
when nothing was applied; the header refusals (default provenance,
observed differing, fields absent); the row refusals (observed moved; both
fields moved to another mask and still refused by the header binding;
fields absent); comparison refusals and the equal case; every perf row
carrying the three fields.

## Review round, and the retained board under the tightened gate

Two contracts were tightened under review after the run, in the closing
commit. Only an absent `TORON_TPCH_CPU_AFFINITY` selects the default: a
present value that is empty, whitespace, malformed, or not Unicode is
refused before the runtime starts (`resolve_environment`, tested on the
environment path with every one of those cases, not only the parser). And
every row's `cpu_affinity_requested_provenance` must equal the header's:
a row whose masks agree but whose provenance is `default` or absent is
refused as `cpu_affinity_mismatch:<query>:<engine>:provenance` (one fixture
each). A `perf-board-replay --artifact PATH` verb re-evaluates a published
board under the gate the running executable carries and reports whether
the artifact's own claim still holds, so a later gate can verify earlier
retained rows without rerunning them; its test replays a header that
claims authority the gate refuses.

The retained board was re-evaluated under the tightened gate on strix by
the tightened gate's executable (`49e37530994fa57f1ee7437ba17c8073f3e8f98fdd9b7f5d12ab6c6cba8d2960`,
tree `3f2e5c03…`, which differs from the closing commit's tree in the
plan documents alone; same recipe `9af51cbc…`; manifest
`evidence/build/adr591-replay.manifest`): 264 samples, 44 summaries,
replayed `candidate_authority = true` with no reasons, the artifact's
claim agreeing with the replay, the artifact's SHA-256 unchanged at
`d03935eb…`, and every one of the 264 rows carrying provenance
`explicit`. The report is retained as
`adr591-strix-host-record/replay_under_tightened_gate.json`
(`4ecd25473315c782728be5e5b1bb1c142a07930b793bc707509ec815e7905a39`). The
sealed board, its executable, and its tag are unchanged; no board was
rerun for these fixes.

Ladder on mini (`RUST_MIN_STACK=67108864`, fmt clean), passing with one
exclusion: Toron library 1,806, architecture 2,151, integration 3,649;
toron-tpch library 231 (+4 ignored, 1 excluded), `toron-tpch` binary 96,
`toron-bench` 9, integration 261 (+2 ignored). Two host notes, neither
caused by this record: mini's
`~/.gitconfig` signs commits with a passphrase-protected key, so the
evidence tests that commit in temporary repositories ran with
`GIT_CONFIG_GLOBAL` pointed at an unsigned identity; and
`adr538_timing_atlas_driver::driver_produces_a_journal_that_replays_without_violations`
fails with SIGSEGV on mini on the baseline tree `5291dba3` as well (two of
three full runs, and alone), while it passes on slim, so it is the one
exclusion above, tracked separately as its own finding.

## Closure

ADR 591 closes with the affinity coordinate recorded and gated, strix's
baseline sealed, and q21 named as the next owner on strix. Nothing else
under this number.
