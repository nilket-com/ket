# ADR 603 - the SF8 board refreshed on strix, with two labelled memory panels: results

Measured 2026-09-29 on strix, on tree `96097b3e` (commit `e9421922`, tag
`artifact/adr603-measured`): the anchor board at ADR 591's coordinate,
sealed with final authority; the two memory panels the user asked for,
each at its own labelled coordinate; and a profiled DuckDB sweep after
the anchor and after Panel A. **The anchor reads Toron 63.430 s against
DuckDB 15.650 s, a ratio of sums of 4.053× (median per-cell 4.043×),
32.064 s above the campaign's 31.366 s target.** Panel A, DuckDB under a
nominally matched 2 GiB cap, reads 15.615 s, 4.062× against the anchor's
Toron. Panel B, Toron at 32 GiB per task, reads 63.182 s over all 22
cells, 4.037× against the anchor's DuckDB. Its q21 still spills, so its
no-spill subset covers 21 of 22 cells at 4.107×. On this board, neither
memory comparison moves the ratio of sums by as much as 0.02: Panel A
is 0.009 above the anchor's ratio, and Panel B 0.016 below it. This
record chooses no target coordinate;
that choice is the user's.

## Identity

| identity | value |
|---|---|
| measured tree | `96097b3ee59ddc9a9b4357b73debde450a415b49`, commit `e9421922` (the harness change and its tests; the closing commit differs from it in the plan documents alone) |
| executable | `aa225d15510a05ec3ffc8ba2dde628e8165d7a724fba9e7846627e3958168359` (harness and Toron), manifest `target/tpch/evidence/build/adr603-measured.manifest` |
| DuckDB | `c478c0f49f6755100d55dee32de485c0f00f3729b40b62658a83b0666b684707`, v1.5.4 (Variegata) `08e34c447b` |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580`; rustc 1.95.0 |
| dataset | `fnv1a64:0fc5c06d19c5ed23`, `full_hash` on every row |
| anchor artifact / sealed manifest | `d4044c40cfe517085ec5754b4de76ef647bdbaf29cd3294c325496a9249c3273` / `fbaf997d02758e82039d6068a6f32036fedb241ac9b34b1a567f64a33fb97abf` |
| evidence tag | `evidence/96097b3ee59d-d4044c40cfe5` (pushed to strix's local seal remote, fetched to the workstation, and pushed with the closing commit, as ADR 591's was) |
| launcher | `toron-bench` `23267312…` on strix (ADR 598's, unchanged) |
| build host / run host | bigfoot / strix |

## The procedure

Each step was launched under ADR 595's separated procedure. One SSH
session ran `launch.sh <step> <token>`, which took the before-snapshot
into the step's own directory under `adr603-strix-host-record/<step>/`,
scheduled a transient user unit 120 s out, and exited. The completion
hook's datagram reached the workstation's listener, and only then did a
read-out session take the after-snapshot into the same directory. Every
step had a fresh output directory and an empty join-spill scratch before
and after. No profiled child ran during a timed artifact, including its
warmups, or between its cells. Each step started on its own gate, after
the previous step's completion.

| step (CDT) | launched | completed | before: load, °C | after: load, °C |
|---|---|---|---|---|
| wiring smoke (q09, q18; DuckDB at 2 GiB; one run each; a sweep child each) | 08:43:19 | 08:45:41 | 0.00, 41 | 0.30, 43 |
| **anchor** (both engines, all 22, 5 runs + 1 warmup) | 08:46:34 | 09:03:02 | 0.15, 43 | 1.01, 47 |
| anchor's DuckDB sweep | 09:03:50 | 09:07:01 | 0.52, 46 | 0.69, 49 |
| Panel A (DuckDB, `memory_limit=2GiB`) | 09:07:22 | 09:12:01 | 0.54, 46 | 0.81, 46 |
| Panel A's DuckDB sweep | 09:12:24 | 09:15:35 | 0.68, 47 | 0.61, 46 |
| Panel B (Toron, 32 GiB task memory and spill reserve) | 09:15:55 | 09:29:45 | 0.51, 48 | 0.92, 44 |

Every completion record reads `result=success exit_code=exited
exit_status=0`, with its attempt token (`a620…`, `c0ff…`, `590d…`,
`fc37…`, `7f19…`, `667f…`).

The smoke verified the wiring before the full runs. DuckDB read back
`2.0 GiB` (display-derived 2,147,483,648 bytes). Each child had its own
temp directory, and none remained afterwards. Each sweep child's
`duckdb_result_sha256` equalled its timed row's. The capped artifact was
refused the anchor's authority for exactly the intended reasons
(`duckdb_memory_limit:expected_null`,
`duckdb_memory_limit_provenance:expected_default`, and the engine list).

## The anchor

`candidate_authority` true with no reasons: host quiescent at start and
end, spill scratch empty before and after, output directory fresh,
affinity `0-15` requested and observed, DuckDB at one thread from its
staged bytes, and `duckdb_memory_limit` null with provenance `default`.
264 samples and 44 summaries, every one stable under the gate's
`(max − min) / median ≤ 0.20`. Sealed through `seal-evidence` with
`final_authority` true; the harness, Toron, and DuckDB executables are
archived under `target/tpch/evidence/archive/`. DuckDB's default limit,
observed by read-back on every DuckDB child, is `98.5 GiB`, which
confirms the plan's estimate.

| query | Toron ms | DuckDB ms | ratio | gap ms | ADR 591 Toron | change |
|---|---:|---:|---:|---:|---:|---:|
| q01 | 4,657 | 957 | 4.87× | 3,700 | 4,805 | −147 |
| q02 | 1,484 | 129 | 11.51× | 1,355 | 1,505 | −21 |
| q03 | 1,880 | 658 | 2.86× | 1,222 | 1,882 | −1 |
| q04 | 2,781 | 456 | 6.10× | 2,325 | 2,790 | −9 |
| q05 | 5,286 | 597 | 8.85× | 4,689 | 5,348 | −62 |
| q06 | 687 | 366 | 1.88× | 321 | 683 | +4 |
| q07 | 3,941 | 593 | 6.65× | 3,348 | 3,952 | −12 |
| q08 | 3,261 | 647 | 5.04× | 2,614 | 3,267 | −6 |
| q09 | 7,114 | 1,635 | 4.35× | 5,479 | 7,161 | −47 |
| q10 | 2,372 | 1,130 | 2.10× | 1,242 | 2,362 | +10 |
| q11 | 906 | 108 | 8.39× | 798 | 899 | +7 |
| q12 | 2,064 | 511 | 4.04× | 1,553 | 2,061 | +2 |
| q13 | 2,490 | 1,199 | 2.08× | 1,291 | 2,516 | −25 |
| q14 | 636 | 403 | 1.58× | 233 | 642 | −6 |
| q15 | 1,234 | 312 | 3.96× | 922 | 1,239 | −5 |
| q16 | 1,341 | 190 | 7.06× | 1,151 | 1,350 | −9 |
| q17 | 3,099 | 626 | 4.95× | 2,473 | 3,090 | +9 |
| q18 | 6,039 | 1,492 | 4.05× | 4,547 | 6,069 | −30 |
| q19 | 1,769 | 677 | 2.61× | 1,092 | 1,760 | +9 |
| q20 | 1,677 | 518 | 3.24× | 1,159 | 1,662 | +15 |
| q21 | 8,057 | 2,211 | 3.64× | 5,846 | 9,127 | −1,070 |
| q22 | 654 | 235 | 2.78× | 419 | 667 | −13 |
| **sum** | **63,430** | **15,650** | **4.053×** | 47,780 | 64,836 | −1,406 |

Against ADR 591's sealed board the comparison is table to table: ADR
591's board has no DuckDB memory fields, and the gate names them
absent. q21's −1,070 ms is where ADRs 600 and 601 retained their cuts.
The other cells' changes are consistent with the variation seen on ADR
590's same-tree boards. The ranking by absolute gap is q21 (5.8 s), q09 (5.5),
q05 (4.7), q18 (4.5), q01 (3.7), q07 (3.3), q08 (2.6), and q17 (2.5).

**Spill and memory, as reported.** Toron's `spill_bytes` is zero on 21
cells. q21 spills 27,242,808 bytes on every sample. Toron's peak RSS is
labelled `above_parent_high_water` on 15 cells and `ambiguous` on seven
(q06, q13, q14, q15, q16, q19, and q22, which read at or under the
parent's high-water). The largest readings above it are q05 8,156 MiB, q18
3,956, q17 3,645, q03 3,373, q09 2,282, and q20 2,187. The anchor's DuckDB
sweep observed no spill in any cell (every `system_peak_temp_dir_size`
is 0). DuckDB's peak buffer memory in the sweep is 1,938 MiB on q09,
1,249 on q18, and at most 467 on every other cell. All 22 sweep results
match their timed cells (`matches_timed`). These are diagnostic
observations of each query at the anchor's setting, not facts about the
timed runs, whose spilling is not directly observed.

## Panel A: DuckDB under a nominally matched cap

DuckDB only, with the anchor's coordinate and
`TORON_TPCH_DUCKDB_MEMORY_LIMIT=2GiB`, and a fresh temporary directory
per child. It is **nominally matched**: DuckDB's limit is process-wide,
and Toron's is per task. Every row passed. The read-back was `2.0 GiB`
on every child. The gate refused it the anchor's authority by
construction, and for nothing else. DuckDB summed 15,615 ms against the
anchor's 15,650. No cell differed from its anchor DuckDB median by more
than 3%. Anchor Toron over capped DuckDB is 4.062×. The Panel A sweep
observed no spill in any cell at 2 GiB, the same two cells came nearest
the cap (buffer peaks of 1,938 MiB on q09 and 1,249 on q18), and all 22
results match their timed cells.

## Panel B: Toron at a larger budget

Toron only, with the anchor's coordinate except task memory and spill
reserve at 32 GiB. Every row passed, the gate refused it sealing by
construction, and the scratch was empty before and after. The status is
read per cell. **q21 spills at 32 GiB exactly as at 2 GiB:** 27,242,808
bytes on every sample, the same plan fingerprint, and an empty
observation list. Raising the budget did not prevent q21's spill in
this run. The record does not establish how the spilled branch is
chosen.
The other 21 cells read `no_spill`. q11, q14, and q19 keep their
zero-byte `in_memory` observations. The full 22-cell aggregate, q21
included and marked, is 63,182 ms, 4.037× the anchor's DuckDB. The
no-spill subset, 21 of 22 cells without q21, is 55,196 ms against
13,439, 4.107×.

Most cells moved by 1.5% or less. Three moved more. **q14 (−8.6%) and
q19 (−6.6%) ran different plans:** their plan fingerprints change with
the budget (`93da4fd9…` to `4c39c4c6…`, and `5222f906…` to `727884ce…`),
and their reported RSS values change (653 to 166 MiB, and 653 to 350).
The anchor's readings for both cells are in the `ambiguous` group, so no
peak reduction is inferred from them. The plan change is
the plan-admission sensitivity the plan warned of, read as the board's
sensitivity to this coordinate, not as a spill price. q06 (+7.7%) kept
its plan; its samples are bimodal in both runs (681–688 and 741–748 ms),
so its median moved with the split between the two groups.

## The comparison, summarized

| coordinate | Toron sum | DuckDB sum | ratio of sums |
|---|---:|---:|---:|
| anchor (sealed) | 63.430 s | 15.650 s | 4.053× |
| Panel A: DuckDB at 2 GiB (nominally matched) | 63.430 s (anchor) | 15.615 s | 4.062× |
| Panel B: Toron at 32 GiB, all 22 cells | 63.182 s | 15.650 s (anchor) | 4.037× |
| Panel B: no-spill subset, 21 of 22 | 55.196 s | 13.439 s (anchor) | 4.107× |

Computed by `memory-panels-summary` (`complete: true`) from the three
artifacts and both sweeps; the output is
`adr603-strix-host-record/memory_panels_summary.json`.

## Implementation and verification

Harness only, with no engine code. `toron-tpch/src/duckdb_memory.rs`
(new): the `TORON_TPCH_DUCKDB_MEMORY_LIMIT` coordinate (absent is the
default; present and empty, malformed, or non-Unicode is a refusal
before any query), normalization to bytes, DuckDB's display form and the
string comparison of the read-back at that precision (display-derived
bytes labelled as such), and the profiler settings and profile parser.
`main.rs`: `SET memory_limit` after the thread pin when explicit; a fresh
per-child temp directory under an explicit limit; the read-back before
`.timer on`; the result taken as the last JSON array that is not the
read-back; `duckdb_result_sha256` on every passing DuckDB row, computed
after the child exits; a successful child without a read-back, or with
one that disagrees, fails its row by name; the parent's `ru_maxrss` at
spawn and the `above_parent_high_water`/`ambiguous` label on Toron and
DuckDB rows; the coordinate on the header and DuckDB rows; and the
`duckdb-profile-sweep` and `memory-panels-summary` subcommands.
`perf_board.rs`: the anchor requires `duckdb_memory_limit` null and
provenance `default`; DuckDB rows are bound to both fields; comparability
covers both, and a board without them is named absent; the per-cell
spill status (zero bytes and no spilled path in every present
observation, with an empty list evidence of nothing); and the summary,
which publishes no aggregate when a cell is missing. (Review rounds: the
plan was amended before implementation, after a DuckDB probe on bigfoot
showed the read-back is display-rounded and that a temp-directory check
cannot observe DuckDB's spill. The implementation review added the timed
result digest and the sweep's identity check, the null-limit requirement
on the anchor, the missing-read-back failure, and null aggregates on an
incomplete summary.)

Fixtures: 12 new (the request parser, the environment refusals, the
display and read-back agreement at 2GiB, 2GB, and 1536MiB with a
disagreement, the forced-spill profile, the capped artifact refused as
the anchor, a missing limit field, DuckDB rows bound to the header, a
board without the coordinate named absent, the RSS label on equal, below,
and above, the no-spill rule including q21's shape and the in-memory
observation, the panel summary with a spilling cell and one missing
cell, and the sweep identity in each of its six states). Ladder on
bigfoot: toron-tpch library 253 (+4 ignored), binary 133, `toron-bench`
9, integration 261 (+2 ignored), no warnings, and fmt clean, with the
evidence seal fixtures run under a forwarded SSH agent as in ADR 602.
The Toron crate is unchanged from ADR 602's closing tree.

Evidence on strix under `~/work/ket/nilket/target/tpch/`:
`adr603-anchor/` (`perf_board.jsonl` `d4044c40cfe5…`, `.evidence`
`916951703e6a…`, `.evidence.sealed` `fbaf997d0275…`, `samples.jsonl`
`70645b5b84cb…`); `adr603-sweep-anchor/duckdb_profile_sweep.jsonl`
`de56739a0af5…` and its profiles; `adr603-panel-a/` (`perf_board.jsonl`
`49ea7f1e8d5b…`, `samples.jsonl` `557f7cf21fdd…`);
`adr603-sweep-panel-a/duckdb_profile_sweep.jsonl` `83be1c591a9e…`;
`adr603-panel-b/` (`perf_board.jsonl` `52e9e01c70af…`, `samples.jsonl`
`ae70e25feca2…`); `adr603-smoke/`; and `adr603-strix-host-record/`
(`launch.sh` `85761d5abde5…`, `readout.sh` `c5c944da9bf2…`,
`snapshot.sh` `76763644fdfb…`, `memory_panels_summary.json`
`7b168f7ebdbc…`, and a directory per step holding its before- and
after-snapshots, driver logs, and completion marker).

## Closure

ADR 603 closes with the anchor sealed and both panels reported at their
own coordinates. On this board, the two memory settings tested did not
materially narrow the aggregate gap. DuckDB at a nominally matched
2 GiB runs as it does at its 98.5 GiB default, with no spill observed
in its diagnostic children. Toron at a 32 GiB task budget runs within
half a percent of its 2 GiB sum. At the same time, q14 and q19 show plan
sensitivity to the budget, and q21 still spills at 32 GiB. Whether a
target coordinate other than the anchor serves the campaign is the
user's choice, made from these numbers. The next record is the one the
plan's forward section named: a structural census of the largest
absolute-gap cells (q21, q09, q05, q18, q01 by this ranking) under a
declared protocol, with Toron's plan, materialization, and operator
evidence beside DuckDB's `EXPLAIN ANALYZE`, to choose the next owner,
whether a kernel cut or an architectural change. The RSS measurement
correction remains its own record.
