# ADR 415 - Implementation plan: sf=10 full-board live sweep

Pairs with `plans/adr/415_sf10_full_board_live_sweep.md`. Measurement-only closing artifact: live-probe all 22 queries
at sf=10 under the fixed scope, resolve q05, pass@compare every green, name every wall with its component payload, and
state the final board. No engine changes; new walls become follow-on ADRs.

## Preflight
```bash
# clean rebuild (stale-binary hazard) - wrapper, bounded jobs
CARGO_CGROUP_BACKEND=affinity CARGO_CGROUP_JOBS=4 \
  scripts/cargo-cgroup build -p toron-tpch --features polars-sql-reference --release
# scratch + disk hygiene (the 414 finding; already 1.3 GB re-accumulated)
du -sh /tmp/toron_join_spill; rm -rf /tmp/toron_join_spill; df -h /tmp | tail -1
# data present
ls target/tpch/data/strict/sf10/manifest.json
```

## Reach pass (one bounded attempt per cell; q05 runs separately)
```bash
mkdir -p target/tpch/adr415
for q in q01 q02 q03 q04 q06 q07 q08 q09 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22; do
  unit="toron-adr415-${q}-reach-a1"
  systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=360s \
    /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query "$q" \
      --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr415/${q}_reach.json" 2> "target/tpch/adr415/${q}_reach.err"
  rc=$?
  {
    echo "systemd_run_rc=$rc"
    systemctl --user show "$unit" \
      --property=Result --property=ExecMainCode --property=ExecMainStatus --property=MemoryPeak 2>&1 || true
  } > "target/tpch/adr415/${q}_reach.meta"
  systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
  du -sh /tmp/toron_join_spill 2>/dev/null; rm -rf /tmp/toron_join_spill   # hygiene between cells
done
```
Record per cell: status / wall / user+sys / peak RSS / spill bytes / row count; on failure the **producer + component
payload when emitted** (412 detail), or the retained systemd `Result` for a hard OOM/timeout; on plan/bind reject the
verbatim reject (classified `blocked`, never dodged). A disk-related failure is retried once after cleanup, not
recorded as a query wall. The `.meta` artifact is mandatory: `|| true` must not erase the distinction between an
engine failure, `oom-kill`, and `timeout`.

## q05 protocol (2-3 attempts, fresh scratch each)
- Use the same named-unit command and retain `.json`, `.err`, and `.meta` per attempt.
- Run at least two attempts. Stop after two passes; otherwise run the third.
- Two consecutive passes -> green (attempt variance recorded); any pass mixed with OOM/failure -> `flaky-green`;
  three OOMs -> OOM with systemd/RSS evidence + name "q05 RSS headroom" follow-on; a repeated engine failure keeps
  its named producer. A hard OOM need not have an engine component payload because the cgroup may be killed before
  telemetry flushes.
- `green` and `flaky-green` still require a persisted q05 `pass@compare`; these labels describe reach stability, not
  a correctness exemption.

## Correctness pass (every completing cell)
```bash
for q in <completing cells>; do
  unit="toron-adr415-${q}-correctness"
  systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    /usr/bin/time -v target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr415/${q}_correctness.out" 2> "target/tpch/adr415/${q}_correctness.err"
  rc=$?
  {
    echo "systemd_run_rc=$rc"
    systemctl --user show "$unit" \
      --property=Result --property=ExecMainCode --property=ExecMainStatus --property=MemoryPeak 2>&1 || true
  } > "target/tpch/adr415/${q}_correctness.meta"
  systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
  rm -rf /tmp/toron_join_spill
done
```
A completing cell without `pass@compare` is a **finding** (413's completing-but-wrong lesson), not a green. q17 is
still run at `--sf 10`; its expected `q17_sf10_oracle_policy_gap` is recorded live as `reach_only_no_oracle`, not
silently substituted with lower-scale correctness and not classified green.

## DuckDB reference pass
Run all 22 DuckDB references on the same persisted data (7 runs / 1 warmup; cheap at this scale), recording the CLI
version. The board includes `duckdb_ms` and `toron/duckdb`; Toron timeouts use a censored lower bound
(`>360000 / duckdb_ms`). A missing DuckDB row is a named reference-run gap and does not change the Toron
classification.

```bash
duckdb_bin="$(command -v duckdb)"
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  --setenv=TORON_TPCH_DUCKDB_BIN="$duckdb_bin" \
  target/release/toron-tpch perf \
    --sf 10 --mode strict --runs 7 --warmup 1 --query all --engines duckdb \
    --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/adr415/duckdb \
  > target/tpch/adr415/duckdb.out 2> target/tpch/adr415/duckdb.err
```

If the all-query command stops on one unsupported/reference failure, rerun per query so one missing row does not
erase the other 21 reference rows.

## Deliverable (results doc)
`crates/toron/plans/impl/415_sf10_full_board_live_sweep_results.md`:
- preflight record (rebuild, scratch cleared, disk headroom);
- **the final 22-row board:** per cell - status (`pass@compare` / `fail: <producer + payload>` / `timeout` /
  `oom: <systemd evidence>` / `blocked: <reject>` / `reach_only_no_oracle`), wall, peak RSS, spill, DuckDB time,
  Toron/DuckDB ratio or lower bound; all from this sweep's live runs (no cached rows);
- the q05 attempt table + resolution;
- atlas-family band comparison (q03/q04/q08/q10/q12/q19 vs their arc bands; out-of-band = named finding);
- the follow-on list: every non-green cell's named next cut + the standing backlog (speed thread, external sort,
  decode residual, q05 RSS headroom if confirmed);
- hygiene: no engine source changes, no cap/scope change, scratch cleared after, no `.perf.data` committed.

## Verification (measurement-only ADR)
```text
git status --short        # results doc (+ scripts if any) only; no engine src
scripts/cargo-cgroup fmt -- --check   # unchanged tree stays clean
git diff --check
git ls-files '*.perf.data'
```

## Risk / blast radius
- **Unknown walls** -> named with engine payload or systemd evidence; follow-on ADRs (the sweep's purpose).
- **q05 OOM confirmed** -> honest board ("6 green + q05 borderline") + named follow-on.
- **Duration** (~2-3 h worst case) -> per-cell artifacts checkpoint; a session break loses nothing.
- **Disk ghosts** -> hygiene between cells; retry-once-after-cleanup rule.
- **Band drift** -> bands not medians; only out-of-band moves are findings.

## Seal
`plan: adr 415 sf10 full board live sweep` (the dual). Results: `impl: adr 415 sf10 full board live sweep`. Do not
commit `.perf.data`.
