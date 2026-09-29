# ADR 603 - the SF8 board refreshed on strix, with two labelled memory panels

**Status:** CLOSED. The anchor is sealed with final authority on
strix (results in
`plans/impl/0603_sf8_board_refresh_and_memory_panels_results.md`,
evidence tag `evidence/96097b3ee59d-d4044c40cfe5`): Toron 63.430 s
against DuckDB 15.650 s, 4.053×, 32.064 s above the 31.366 s target.
Panel A (DuckDB at a nominally matched 2 GiB) reads 4.062×. Panel B
(Toron at 32 GiB) reads 4.037× over all 22 cells, with q21 still
spilling, and 4.107× over its 21-cell no-spill subset. No target is
chosen here. Successor to ADR 591, whose sealed strix board is
the campaign's anchor: Toron 64.836 s against DuckDB 15.683 s summed
over q01–q22 at SF 8, one thread, 2 GiB per task. ADRs 600 and 601
retained two q21 cuts since then, and ADR 602 closed `not_established`.
The campaign set on 2026-09-29 is Toron at no more than twice DuckDB's
time on this board (a Toron sum ≤ 31.366 s on ADR 591's DuckDB sum), and
faster if it can be. The next owner is chosen by absolute recoverable
time, so the ranking has to be re-read on the tree that now exists.
When the user set the campaign, they asked that two memory comparisons
be reported beside the anchor before a target coordinate is chosen.
This record makes one board and two panels, and chooses nothing.


## Context

**What the anchor fixes.** ADR 583's board as ADR 591 sealed it on
strix: `perf --engines toron,duckdb --query all --mode strict --sf 8
--data-source parquet --runs 5 --warmup 1` through `toron-bench run`;
`TORON_TPCH_MEMORY_PER_TASK` and `TORON_TPCH_SPILL_BYTES_RESERVED` at
2 GiB; `TORON_PARTITION_PARALLELISM=1`; `TORON_TPCH_DUCKDB_THREADS=1`;
`TORON_TPCH_CPU_AFFINITY=0-15`; a dedicated empty
`TORON_JOIN_SPILL_DIR`; DuckDB 1.5.4 from its staged bytes
(`c478c0f4…`); the dataset `fnv1a64:0fc5c06d19c5ed23` with `full_hash`
on every row; the recipe `9af51cbc…`; the board gate
(`toron-tpch/src/perf_board.rs`) requiring every one of those as an
explicit coordinate.

**What the harness does not set.** `duckdb_setup_sql`
(`toron-tpch/src/main.rs:11219`) issues `SET threads=1;` and the parquet
views, and nothing else. DuckDB therefore runs under its default
`memory_limit`, 80% of physical memory, about 98 GiB on strix, and its
default temporary directory. Toron's 2 GiB is per task, with its own
spill reserve. The board does not establish equal memory budgets, and
it does not claim to.

**What the anchor's own rows already say.** The sealed board carries
`spill_bytes` and `peak_rss_kib` on every row
(`target/tpch/adr591-strix-board/samples.jsonl`). Toron's `spill_bytes`
is zero on 21 of 22 cells; q21 spills about 27.2 MB. Toron's reported
peaks reach 8,724,568 KiB on q05, 4,045,484 on q18, and 2,322,108 on
q09; DuckDB's largest are 1,154,556 on q09 and 1,092,996 on q18. Those
figures are read with a caveat. `peak_rss_kib` is the child's
`ru_maxrss` from `wait4` (`main.rs` ~12148 for DuckDB, ~12464 for the
perf child), and Linux carries `ru_maxrss` across `exec` from the forked
harness parent, so a cell whose value equals the parent's size at fork
(633 MiB on most later cells) may report that inherited value rather
than its own peak. Above it the value is the best reading of the
child's peak; at it the value is at most an upper
bound on it. Read directly: 21 of 22 Toron cells report zero spill,
and several of Toron's reported peaks exceed 2 GiB and exceed DuckDB's.
That does not settle what a matched setting would do. Toron's budget is per task and
can change plan admission as well as spilling, and q21 does spill. So
the comparisons are made directly, not inferred.


## Decision

**One board at the anchor's coordinate, sealed; then two panels, each
labelled with its own coordinate and without sealing authority; and a
DuckDB memory setting in the harness to make the first panel possible.**

*The harness change: an explicit DuckDB memory limit, recorded as a
coordinate.* `TORON_TPCH_DUCKDB_MEMORY_LIMIT`, when present, is emitted
as `SET memory_limit='<value>';` after `SET threads`, with the value
checked against DuckDB's size syntax (a number and a unit among `KiB`,
`MiB`, `GiB`, `KB`, `MB`, `GB`) and refused before any query when empty
or malformed, as ADR 591 treats the affinity. The header and every row
carry `duckdb_memory_limit` (the normalized value, or null) and its
provenance (`explicit` when set, `default` when absent). DuckDB is asked
back, in the same process, `SELECT current_setting('memory_limit')`,
and the answer is recorded as `duckdb_memory_limit_observed` on DuckDB
rows. (Amended before implementation.) DuckDB 1.5.4 reports the setting
rounded for display, one decimal of the largest binary unit, truncated:
on bigfoot the default read `98.5 GiB`, `2GiB` read `2.0 GiB`, and `2GB`
read `1.8 GiB`. So the read-back is compared with the request's expected
display at that precision, as strings, and never as exact bytes. Any byte
value parsed from it is labelled display-derived. An explicit request
whose read-back differs from its expected display is refused on the row
by name. A successful `SET` with a matching read-back supports the
requested setting only to display precision. On the anchor, the value
DuckDB returns for its default is reported as observed; the Context's
~98 GiB is an estimate until then. The board gate adds `duckdb_memory_limit_provenance =
default` to the anchor's coordinate, so a capped artifact cannot seal
as the anchor, and adds both fields to the fields two boards must share.
An older board without them, such as ADR 591's, is named as absent, as
ADR 591 named the affinity. The anchor is compared with ADR 591 in the
results document, table to table.

*The anchor.* The board above, run once on strix on this record's
measured tree, sealed through `seal-evidence` as ADR 591's was: 264
samples, 44 stable summaries (the gate's `(max − min) / median ≤ 0.20`
per cell), the integrity gate and the affinity conditions, host records
before and after, and a fresh output directory. Per cell it reports the
Toron and DuckDB medians, the ratio, the absolute gap, and the change
from ADR 591's cell. In aggregate it reports the summed-median ratio,
the median per-cell ratio, and the distance to 31.366 s. This is the
board the campaign's target is measured on until the user chooses
otherwise.

*Panel A, DuckDB at a nominally matched cap.* DuckDB only (`--engines
duckdb`), with the anchor's coordinate and `TORON_TPCH_DUCKDB_MEMORY_LIMIT=2GiB`,
a fresh, isolated, empty `temp_directory` for every measured DuckDB
child, set explicitly beside the limit (so no cell can see a prior
cell's files), and
the same runs, warmup, stability gate, and integrity checks. Its rows
are compared per cell with the anchor's Toron rows and with the anchor's
DuckDB rows. It is labelled **nominally matched**: DuckDB's limit is
process-wide and Toron's is per task, so equal numbers are not proven
equal effective budgets. The gate refuses it sealing authority by
construction (`coordinate_mismatch:duckdb_memory_limit_provenance`, and
the engine list); it is a diagnostic with its own directory and hashes.
Whether DuckDB spilled is observed, not assumed. (Amended before
implementation.) A probe on bigfoot ruled out a file check: a 40M-row
sort under a 100 MiB limit spilled, yet DuckDB had removed its files and
the temporary directory it created before exit, and
`duckdb_temporary_files()` read zero after the query. DuckDB's JSON
profiler does observe it. With `SYSTEM_PEAK_TEMP_DIR_SIZE` and
`SYSTEM_PEAK_BUFFER_MEMORY` enabled, the same sort reported 198,410,240
temp-directory bytes and a 135,622,656-byte buffer peak under 100 MiB,
and 0 and 1,086,291,968 under 8 GiB. Profiling perturbs timing, so it is
kept out of every timed child, warmups included. See the diagnostic
sweep below.

*Panel B, Toron at a verified no-spill budget.* Toron only (`--engines
toron`), with the anchor's coordinate except
`TORON_TPCH_MEMORY_PER_TASK` and `TORON_TPCH_SPILL_BYTES_RESERVED` at
32 GiB each (strix has 123 GiB; the anchor's largest reported Toron peak
is 8.3 GiB). It is compared per cell with the anchor's DuckDB rows and
with the anchor's Toron rows. A cell counts as **no-spill** only when
every sample reads `spill_bytes = 0` and every join-spill observation
present on it reports zero bytes and no spilled path. An empty
observation list is not evidence either way. ADR 591's rows show why:
q11, q14, and q19 carry an observation with `path=in_memory` and zero
bytes, while q21 spilled about 27.2 MB with an empty list. In-memory
observations are kept and reported. The status is checked cell by cell,
never inferred from the budget. A cell that spills at 32 GiB is reported
as spilling, never relabelled. Panel B always reports its full 22-cell
aggregate and ratio, with any spilling cell marked in it. When a cell
spills, it also reports a no-spill subset aggregate with its coverage
stated (how many cells, and which were left out), so no exclusion
silently changes the denominator of the requested comparison. The
panel's header names the budget; the gate refuses it sealing authority
by construction (`memory_per_task`, `spill_bytes_reserved`, the engine
list). Because a larger budget can change plan admission as well as
spilling, a Panel B cell's difference from the anchor is read as the
board's sensitivity to that coordinate, not as the price of spilling.

*Memory as reported, with its limits.* All three artifacts report
`peak_rss_kib` per cell beside the harness parent's own high-water mark,
read at the child's spawn (`getrusage(RUSAGE_SELF)`), and label each row
`above_parent_high_water` when the child's value exceeds it, and
`ambiguous` otherwise, including equal and below. A value above the
parent's high-water is the best available reading of the child's peak,
reported as such. An ambiguous value is never presented as the cell's
consumption. The relation between the parent's high-water and the
child's inherited `ru_maxrss` is not claimed as an exact floor here;
establishing it is part of the measurement record below. Exact per-cell peaks are not an
acceptance condition of this record. Correcting the measurement (for
example the child's `VmHWM` from `/proc/<pid>/status` before it is
reaped) is its own small plan/impl record, and it can precede a later
refresh.

*Order and procedure.* Build on bigfoot, stage and hash-verify on
strix, and launch each of the three under ADR 595's separated
procedure. The anchor runs first; each panel's attempt starts only after
the previous one's completion is received and its marker, logs, and
after-snapshot have been moved into that attempt's directory. One
attempt each: a refused board is retained and reported, and the record
closes on it, as ADRs 590 and 591 did.

*The DuckDB diagnostic sweep (amended before implementation).* After a
DuckDB-bearing timed artifact (the anchor, then Panel A) is complete and
its after-snapshot and, for the anchor, its seal are captured, a
separate sweep runs one profiled DuckDB child per cell. It uses the same
staged executable, query, data, thread setting, and memory setting as
that artifact, a fresh isolated `temp_directory` per child, and a
per-child profile file. It records `system_peak_temp_dir_size` and
`system_peak_buffer_memory` with the sweep's own run identity and the
child's result digest, checked against the cell's result. These are
labelled separate diagnostic observations of the cell's query at that
setting, not facts about the timed runs, whose spilling is not directly
observed. No profiled child runs during a timed artifact, including its
warmups, or between its cells; Panel A starts only after its own ADR 595
gate, which follows the anchor's sweep. Profile-derived spill and buffer
peaks are attached to the sweep's run identities, never to timed rows. A
cell
whose sweep child spills is reported as spilling at that setting in the
diagnostic, and the timed rows are left as measured.

Before the full runs, a smoke
cell pair (q09 and q18, one run, no warmup) under Panel A's setting
verifies the harness wiring, including the observed-limit read-back and
a profiled sweep child for each of the two cells. It does not stand in
for the panel.

*What the record reports, and does not decide.* For each coordinate:
the per-cell table, both aggregates, the ratio to the matching DuckDB
rows, spill per cell, and memory as above. Then the ranking by absolute
Toron-minus-DuckDB gap on the anchor, and whether that ranking changes
under either panel. The choice of the campaign's target coordinate is
the user's, after these are read; this record makes no projection and
names no owner beyond reporting the ranking.


## Acceptance

- The DuckDB memory limit: a value round-trips through parsing and
  normalization; empty, malformed, and unit-less values are refused
  before any query, on the environment-handling path itself. `SET
  memory_limit` is emitted only when the value is explicit, and the
  observed read-back is recorded on DuckDB rows. The read-back is
  compared with the expected display at DuckDB's precision: `2GiB`
  against `2.0 GiB` agrees, `2GB` against `1.8 GiB` agrees, and a
  disagreement is refused on the row by name. All three are fixtured,
  and parsed byte values are labelled display-derived.
- The no-spill rule: fixtures for a cell with `spill_bytes = 0` and an
  in-memory observation (no-spill, observation kept), a cell with spill
  bytes and an empty list (spilling), and a cell with an observation
  reporting spilled bytes (spilling).
- The memory labels: the parent's high-water recorded at each spawn,
  each row labelled `above_parent_high_water` or `ambiguous`, fixtured
  on equal, below, and above.
- The gate: the anchor requires `duckdb_memory_limit_provenance =
  default`; a capped artifact is refused as the anchor with the field
  named; the comparability check includes both new fields; a board
  without them is named as absent. Each has a fixture altering exactly
  one field.
- The DuckDB diagnostic sweep after the anchor and after Panel A, each
  after its artifact is complete and snapshotted (and, for the anchor,
  sealed): one profiled child per cell with a fresh temp directory,
  recording peak temp-directory bytes and peak buffer memory with its
  own run identity and a result digest that matches the cell's. A
  forced-spill fixture on the profile parser, and the probe's readings
  quoted.
- The anchor sealed with final authority: 264 samples, 44 stable
  summaries, `full_hash` with the dataset fingerprint on every row,
  DuckDB at one thread with no memory limit by staged hash, parallelism
  1, affinity `0-15` requested and observed; archived executables, an
  evidence tag, a sealed manifest. Or the refusal, retained.
- Panel A and Panel B complete under their stated coordinates with the
  stability gate and integrity checks; the no-spill label applied per
  cell only on zero spill and no observations; every departure named.
- The results document with the three per-cell tables, both aggregates
  for each, the distance to 31.366 s on the anchor, the ranking by
  absolute gap and its movement under the panels, spill and memory as
  reported with the memory labels, the host records and hashes, and the
  panels labelled as diagnostics at their own coordinates.
- The ladder green on bigfoot (Toron and toron-tpch, fmt clean) on the
  measured tree.
- Plan commit, then one implementation/results/closure commit carrying
  the harness change, its tests, the three runs, and the closure.


## Guardrails

- No engine code changes. The harness change is limited to the DuckDB
  memory setting, its recording and read-back, the spill signal, the
  memory labels, and the gate's fields.
- The anchor's coordinate is ADR 591's verbatim, with only the new
  field's default added. The panels never replace the anchor or seal as
  it.
- Build on bigfoot, measure on strix, the separated launch, nothing else
  on strix in each window; one attempt per artifact.
- No target coordinate chosen, no projection published.


## Risks

- The anchor may not seal. q11's DuckDB cell was the unstable one on ADR
  590's boards, and the stability gate is unchanged. A refusal is the
  result, reported, and a second attempt would need its own record.
- The DuckDB spill signal may prove unreliable; the panel then says so
  instead of claiming no spill.
- 32 GiB may still leave a Toron cell spilling. The cell is then
  reported as spilling, marked in the full 22-cell aggregate, and left
  out of the no-spill subset with that subset's coverage stated.
- Panel differences mix memory with plan admission; the record reads
  them as coordinate sensitivity, not as causes.


## Forward

- The user chooses the target coordinate from the anchor and the two
  panels.
- The RSS measurement correction, as its own small record.
- A structural census of the largest absolute-gap cells under a
  declared protocol (Toron's plan, spill, materialization, and operator
  evidence beside DuckDB's `EXPLAIN ANALYZE`), which selects the next
  owner, whether a kernel cut or an architectural change.
