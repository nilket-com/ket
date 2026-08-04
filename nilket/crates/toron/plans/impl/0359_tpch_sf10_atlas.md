# ADR 359 - Implementation plan: the TPC-H SF=10 atlas (live, fixed-cap, measure + classify, with DuckDB)

Pairs with `plans/adr/0359_tpch_sf10_atlas.md`. A **measurement + classification** cut in `crates/toron-tpch`: run all
22 at sf=10 live under the 356 rail + 358 DuckDB, fixed 4 GB, classify every wall, produce the atlas artifact. **No
engine fixes, no cap bump.** The deliverable is the wall atlas + the Toron-vs-DuckDB sf=10 board, not code changes (any
harness additions are limited to classification/rendering, not the engine).

## Phase 0 - pilot + shape verification (the gate)
- **Build/runtime split:** build the release binary first using `scripts/cargo-cgroup build -p toron-tpch --release`
  under normal build limits. The canonical sf=10 measurement then runs the already-built `target/release/toron-tpch`
  inside a real fixed 4 GB runtime cap. Do not classify a release rebuild under the 4 GB measurement cap.
- **DuckDB under cap:** make `/home/me/.duckdb/cli/latest/duckdb` visible to the capped runtime scope before the pilot
  (`TORON_TPCH_DUCKDB_BIN`, imported into the user systemd environment if using `systemd-run`, or an equivalent direct
  cgroup wrapper). `CARGO_CGROUP_BACKEND=affinity` is acceptable only for a DuckDB path smoke; it is not fixed-cap
  evidence.
- **Fixed-cap runtime wrapper:** prefer a direct user-systemd scope for the built binary:
  ```
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryHigh=4G --property=MemoryMax=4G --property=TasksMax=512 \
    --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
    target/release/toron-tpch ...
  ```
  If user systemd is unavailable, use an equivalent direct cgroup/nerdctl wrapper and record the exact command in the
  results doc. Do not substitute `scripts/cargo-cgroup ... run --release` for the canonical measurement, because it
  can rebuild inside the measurement scope and its `affinity` backend has no memory ceiling.
- **sf=10 data:** confirm the gauge generates sf=10 tables (10x); gen excluded from execute_ms; confirm 358's Parquet
  export handles sf=10 volume (file sizes, write time) for the DuckDB side.
- **Pilot run (q06/q14/q21):** run three one-query commands; `perf --query` supports one query or `all`, not a
  comma-list today.
  ```
  <fixed-4g-runtime-scope> \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 --query q06 \
      --engines toron,duckdb --output-dir target/tpch/perf_adr359_sf10_pilot_q06
  <fixed-4g-runtime-scope> \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 --query q14 \
      --engines toron,duckdb --output-dir target/tpch/perf_adr359_sf10_pilot_q14
  <fixed-4g-runtime-scope> \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 1 --warmup 0 --query q21 \
      --engines toron,duckdb --output-dir target/tpch/perf_adr359_sf10_pilot_q21
  ```
  Verify: per-query wall-clock (does all-22 fit a sane time budget?), peak RSS vs the 4 GB cap, output row volume,
  DuckDB export size/parity, stability spread. Choose full-board `--runs N --warmup K` from the pilot (up to `7/1` if
  affordable); **do not change the 4 GB cap**.
- **Decision:** only after the pilot confirms runtime/resource/output shape is sane do we run the full board. If the
  pilot itself walls, the atlas records those named pilot walls and the full run is scoped from that evidence.

## D1 - the full sf=10 board (live)
- `perf --sf 10 --mode strict --runs N --warmup K --query all --engines toron,duckdb` (release, fixed 4 GB runtime
  cap; `N/K` chosen by Phase 0) for reach + execute_ms + DuckDB.
- **Live correctness** for each cell that reaches: `correctness --query qNN --sf 10` (the answer, not just reach) -
  where an sf=10 oracle exists; note cells with no sf=10 oracle (for example q17's current correctness scale policy)
  as `no_sf10_oracle` rather than fake compare rows.
- **Never** read `matrix`/`correctness-matrix` for an sf=10 verdict (cache renders).

## D2 - classification + the atlas artifact
- Per query, record: **reach** (pass@execute) or **wall** (kind / named producer / bytes via 332); **correctness**
  (pass@compare or no-oracle); **Toron execute_ms** (median/min/spread); **DuckDB execute_ms** + flags
  (unstable/schema_mismatch); **constant-memory-held?** (vs the sf=1 streamed cells under the same 4 GB).
- Wall kinds: memory / time(cap) / spill-volume / join-orientation / exchange-or-materialization / correctness-drift.
- Any harness addition is **classification/rendering only** (e.g. an atlas-board renderer over the rail JSONL); no
  engine or cost-model change.

## Tests / safety net
- **No engine behavior change:** the sf=1 board (the recorded 356/357 baseline) is unchanged; `check`/`test -p
  toron-tpch` green; any new renderer/classifier has a unit test (wall-kind classification from a synthetic rail row).
- The atlas run itself is the evidence (not a unit test); its artifact (JSONL + the classified board) is committed
  under the results doc / a `target/tpch/...` path referenced by the results.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
# Build first; do not compile inside the 4 GB measurement cap.
scripts/cargo-cgroup build -p toron-tpch --release
# Pilot (q06/q14/q21 as separate one-query runs) then full board, sf=10, fixed 4 GB, live, with DuckDB:
<fixed-4g-runtime-scope> \
  target/release/toron-tpch perf --sf 10 --mode strict --runs N --warmup K --query all \
  --engines toron,duckdb --output-dir target/tpch/perf_adr359_sf10
git diff --check
#   pilot shape verified; all 22 live at sf=10 under fixed 4 GB; every wall classified (producer/bytes/kind);
#   Toron-vs-DuckDB board recorded with flags; no cap bump; no engine fix
```

## Results doc
`crates/toron/plans/impl/0359_tpch_sf10_atlas_results.md`: the pilot shape verification (runtime/RSS/output-volume for
q06/q14/q21); the full sf=10 atlas table (per query: reach/wall+kind+producer+bytes, correctness, Toron execute_ms,
DuckDB execute_ms+flags, constant-memory-held); the **streaming substrate constant-memory verdict** at 10x (which cells
hold under 4 GB, which break + why); the **Toron-vs-DuckDB sf=10 board** (absolute + gap vs sf=1); the named next walls
(prime suspect: upstream child materialization / exchange, ADR 349) as the forward ADR list; explicit note that 359 is
measure+classify (no fixes, no cap bump, live-not-cached).

## Risk / blast radius
- **Resource surprise** - the pilot gate verifies shape before all-22.
- **Cap bump / fix creep** - rejected; over-cap = named wall; fixes are subsequent ADRs.
- **Cached-matrix contamination** - live-only; the ADR 356 lesson.
- **DuckDB noise at sf=10** - recorded as flags, not dropped.
- **No engine behavior change** - measurement + classification only.

## Seal
`impl: adr 359 sf10 atlas`. Do not commit wip drafts.
