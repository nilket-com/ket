# ADR 394 - Implementation plan: Polars peer-substrate reference adapter

Pairs with `plans/adr/394_polars_peer_substrate_reference.md`. Harness-only, optional `engine=polars_sql` peer reference
via a committed stdin-JSON Python runner. Execute-only timing; `engine_version` generalized; skip-clean; value-gate, not
schema equality. No `crates/toron/src` change.

## D1 - the committed runner (`crates/toron-tpch/scripts/polars_perf_runner.py`)
- Reads **one JSON request on stdin**: `{query_id, sql, tables: {name: path}, manifest, run_index, warmup}`.
- `import polars as pl` (capture `pl.__version__`); build `SQLContext`, `register(name, pl.scan_parquet(path))` per
  table. **Import + registration happen OUTSIDE the timed region.**
- The Rust harness invokes the runner **once per warmup/measured attempt**, preserving the perf rail's fresh-child and
  raw-row semantics. The runner performs exactly one timed collection; **time only `collect()`**.
- Emits **one JSON response on stdout**: `{status, execute_ms, rows, engine_version, error_class?}`.
  - `status=ok` on success; `status=unsupported` with `error_class` (`correlated_subquery` / `interval_syntax` /
    `join_constraint` / `index_limit` / `other`) on a Polars SQL error - **non-zero is a classified result, not a
    crash**; print the JSON and exit 0 so Rust always parses a row.
- The runner does **not** compute medians/spread; Rust writes one raw `tpch_perf` row per attempt and the existing
  perf summary computes medians/min/spread/stability.
- No argv SQL; no network; no writes outside stdout.

## D2 - the Rust adapter (harness, `crates/toron-tpch`)
- New engine `polars_sql` in the perf engine set; selected only by explicit `--engines` (NOT added to `all`).
- Locate interpreter via **`TORON_TPCH_POLARS_PYTHON`**. If unset or the runner import fails ->
  `status=not_attempted(polars_absent)`, skip-clean (no harness failure).
- Spawn the runner, write the JSON request to **stdin**, read one JSON line from stdout, map to a perf row.
- Map runner `status=unsupported` onto the existing perf telemetry contract: `status=not_attempted`, `reason =
  polars_sql_unsupported_<error_class>`, plus the diagnostic message. Do **not** add `Status::Unsupported` in this ADR.
- Record **`engine_version`** (generalized field) from the response; populate it for DuckDB too (its version string).
  Keep `duckdb_version` only if a consumer needs it; otherwise migrate to `engine_version`.
- `execute_ms` = the runner's single-attempt `execute_ms` (collect-only). Parent wall/RSS may be recorded separately,
  not as `execute_ms`.

## D3 - capability sweep + value gate
- A `--engines polars_sql` run over the 22 yields the empirical capability table (status/error_class per query).
- For q03 (and the q06/q14/q19 starters), the results artifact runs a **value-compatible comparator** vs the q03 oracle
  / DuckDB: same rows, same order, values within decimal tolerance (scale 2 vs 4 expected). Fingerprint is sanity only.

## Tests / checks
- runner unit behavior: a supported query -> `status=ok` + rows; an unsupported one (e.g. q02) -> `status=unsupported`
  `error_class=correlated_subquery`, exit 0;
- Rust adapter: absent `TORON_TPCH_POLARS_PYTHON` -> `not_attempted(polars_absent)`, rail completes;
- unsupported Polars SQL maps to `not_attempted(polars_sql_unsupported_*)` and does not abort the rail;
- `--engines all` still resolves to Toron+DuckDB (a test pinning the set);
- `engine_version` present on DuckDB + Polars rows;
- q03 value-compatible comparator passes (decimal tolerance).

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --no-fail-fast              # unchanged engine
# build a local venv for the runner (setup convenience; uv, not a runtime dep):
#   uv pip install --python <venv> polars duckdb pyarrow ; export TORON_TPCH_POLARS_PYTHON=<venv>/bin/python
# explicit polars_sql run (sf1 persisted, runs=7/warmup=1):
systemd-run ... target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 \
  --query q03 --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data
# capability sweep:
#   for q in q01..q22: perf --engines polars_sql ... ; collect status/error_class
git diff --check
```

## Deliverable (results doc)
`crates/toron/plans/impl/394_polars_peer_substrate_reference_results.md`:
- the runner/adapter contract (stdin JSON, execute-only timing, env-point, skip-clean);
- the empirical **22-query capability table** (12 ok row-exact + 10 classified failures), labeled "Polars SQL subset"
  with a separate "future Polars Lazy 22" note;
- the q03/q06/q14/q19 rows: `toron | rune | polars_sql | duckdb`, execute_ms median/spread, `engine_version`, status;
- the **ratios** `Toron/DuckDB`, `Toron/Polars`, `Polars/DuckDB`, with the value-compatibility verdict for q03;
- confirmation: harness-only, no core change, `--engines all` unchanged, no committed venv/perf.data, stability gate met.

## Risk / blast radius
- **Python-boundary noise** -> execute-only timing + medians/stability; parent wall/RSS separate.
- **Absent/unsupported** -> classified rows, rail never aborts.
- **Version drift** -> `engine_version` on every row.
- **Harness leak into core** -> `crates/toron/src` unchanged; `polars_sql` harness-only, explicit `--engines`.

## Seal
`plan: adr 394 polars peer substrate reference` (the dual). Implementation (runner + adapter + results):
`impl: adr 394 polars peer substrate reference`. Do not commit the venv or `.perf.data`.
