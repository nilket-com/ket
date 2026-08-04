# ADR 392 - Implementation plan: substrate-ceiling measurement at completing scale (q03 sf=1)

Pairs with `plans/adr/0392_substrate_ceiling_completing_scale.md`. Diagnostic-only: q03 three-way (SQL/Rune/DuckDB) at
sf=1 persisted, release, runs=7/warmup=1, medians + stability gate. Headline `Rune/DuckDB`. Reuse the 391 instrument; no
engine change.

## Phase 0 - persisted sf=1 data + instrument check
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity \
  scripts/cargo-cgroup build -p toron-tpch --release

test -f target/tpch/data/strict/sf1/manifest.json || \
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch prepare --data-source parquet --mode strict --sf 1 --data-dir target/tpch/data
# confirm the 391 instrument runs --engines rune/toron/duckdb at --sf 1 --data-source parquet
```
(Optionally prepare sf=3 for the escalation branch.)

## Phase 1 - the three-way at sf=1 persisted (release, runs=7 warmup=1)
```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr392_q03
```
- **Correctness gate (before ratios):** SQL and Rune q03 pass the oracle/order comparator at sf=1. SQL requires an
  explicit persisted correctness command because the SQL perf row is timing/reach only:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q03 --sf 1 \
    --data-source parquet --data-dir target/tpch/data
```

  Rune correctness is accepted only from an `engine="rune"` raw perf row with `phase=compare status=pass`, because the
  ADR 391 Rune perf runner invokes the q03 oracle/order comparator.
- **Stability gate:** runs=7/warmup=1; medians; if a row's spread exceeds the noise band -> unstable -> escalate to sf=3.

## Phase 2 - ratios + branches
- Compute medians: `Rune/DuckDB` (headline), `SQL/Rune`, `SQL/DuckDB` - in the results artifact, not as perf rows.
- **Rune slow (`Rune/DuckDB` large):** symbol-retaining profile (381 method, `--scope`,
  `CARGO_PROFILE_RELEASE_DEBUG=true`, `CARGO_PROFILE_RELEASE_STRIP=false`, `perf report -g`) on the Rune path; name the
  substrate stack.
- **Rune fails at sf=1:** verdict = reach-blocked; record it, do not fabricate a substrate number.
- **sf=1 unstable / fixed-overhead-dominated:** escalate to sf=3 (same method) and use that as the headline.

## Deliverable (results doc)
`crates/toron/plans/impl/0392_substrate_ceiling_completing_scale_results.md`:
- the sf=1 (and sf=3 if escalated) three-way table: engine | status | execute_ms (median/min/spread) | ratio_vs_duckdb |
  correctness (oracle/order) | producer/profile;
- the stability-gate verdict (stable at sf=1, or escalated);
- the headline `Rune/DuckDB` median + `SQL/Rune` + `SQL/DuckDB`;
- if Rune slow: the symbol-retaining profile + named substrate stack;
- if Rune failed at sf=1: the reach-blocked verdict;
- **the named next-arc decision** (reach-first / substrate precision-strikes / planner-shape) justified by the number;
- confirmation harness-only, no engine change, no cap bump, `--no-fail-fast` unchanged, no qNN, no committed perf.data.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron-tpch                       # the Rune perf path + correctness (unchanged from 391)
scripts/cargo-cgroup test -p toron --no-fail-fast             # unchanged engine
# the sf=1 three-way (runs=7/warmup=1) + correctness gate + medians/stability; ratios in the results doc
git diff --check
```

## Risk / blast radius
- **Fixed-overhead-dominated sf=1** -> stability gate + sf=3 escalation.
- **Unstable spread** -> runs=7/warmup=1 + gate; escalate, don't trust.
- **Rune fails at sf=1** -> reach-blocked verdict, no fake number.
- **Faster-because-wrong** -> oracle/order gate.
- **Harness leak into engine** -> `crates/toron/src` unchanged; split an ADR if tempted.

## Seal
`plan: adr 392 substrate ceiling completing scale` (the dual). If any harness change is needed beyond reusing 391,
it seals `impl: adr 392 ...` (harness-only). Do not commit large `.perf.data`.
