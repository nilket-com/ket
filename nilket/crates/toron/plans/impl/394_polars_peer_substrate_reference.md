# ADR 394 - Implementation plan: Polars peer-substrate reference adapter

Pairs with `plans/adr/394_polars_peer_substrate_reference.md`. Harness-only, optional `engine=polars_sql` peer reference
via pure Rust `polars` / `polars-sql` behind a `toron-tpch` feature. Execute-only timing; `engine_version` generalized;
skip-clean; value-gate, not schema equality. No `crates/toron/src` change.

## D1 - workspace dependency shape
- Root `Cargo.toml` pins `polars` and `polars-sql` versions only.
- `crates/toron-tpch/Cargo.toml` adds feature `polars-sql-reference = ["dep:polars", "dep:polars-sql"]` and owns the
  required Polars features (`lazy`, `parquet`, `dtype-decimal`, `temporal`, `strings`, `semi_anti_join`).
- Default builds do not compile Polars. Explicit Polars runs use:
  `scripts/cargo-cgroup run -p toron-tpch --features polars-sql-reference --release -- perf ... --engines polars_sql`.

## D2 - the Rust adapter (harness, `crates/toron-tpch`)
- New engine `polars_sql` in the perf engine set; selected only by explicit `--engines` (NOT added to `all`).
- `polars_sql` uses the existing `perf-run-one` child path, preserving fresh-process raw-row semantics and parent RSS.
- Feature absent -> `status=not_attempted`, `reason=polars_absent`, no harness failure.
- Feature enabled -> register persisted Parquet tables with `SQLContext`, execute SQL, time only `collect()`, emit one
  raw `tpch_perf` row.
- The Rust `polars-sql` crate rejects comma joins, so the starter probes use Polars-only explicit-join SQL text for
  `q03`, `q14`, and `q19`; `q06` uses the canonical single-table SQL. The Polars plan fingerprint includes the actual
  SQL text Polars ran.
- Map Polars SQL errors onto the existing telemetry contract: `status=not_attempted`, `reason =
  polars_sql_unsupported_<error_class>`, plus the diagnostic message. Do **not** add `Status::Unsupported`.
- Record **`engine_version`** (generalized field) from `polars::VERSION`; populate it for DuckDB too. Keep
  `duckdb_version` for compatibility.

## D3 - capability sweep + value gate
- A `--engines polars_sql` run over the 22 yields the empirical capability table (status/error_class per query) as the
  Rust SQL dialect map expands. Do not hardcode the Python-probe 12/22 result as the Rust adapter result.
- For q03 (and the q06/q14/q19 starters), the results artifact runs a **value-compatible comparator** vs the q03 oracle
  / DuckDB: same rows, same order, values within decimal tolerance (scale 2 vs 4 expected). Fingerprint is sanity only.

## Tests / checks
- default-feature Rust adapter: explicit `polars_sql` -> `not_attempted(polars_absent)`, rail completes;
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
# compile the Polars adapter explicitly:
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
# explicit polars_sql run (sf1 persisted, runs=7/warmup=1):
systemd-run ... target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 \
  --query q03 --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data
# capability sweep as the Rust SQL dialect map expands:
#   for q in q01..q22: perf --engines polars_sql ... ; collect status/error_class
git diff --check
```

## Deliverable (results doc)
`crates/toron/plans/impl/394_polars_peer_substrate_reference_results.md`:
- the Rust adapter contract (feature-gated, execute-only timing, skip-clean);
- the empirical **capability rows** for the Rust adapter, labeled "Polars SQL subset" with a separate "future Polars
  Lazy 22" note;
- the q03/q06/q14/q19 rows: `toron | rune | polars_sql | duckdb`, execute_ms median/spread, `engine_version`, status;
- the **ratios** `Toron/DuckDB`, `Toron/Polars`, `Polars/DuckDB`, with the value-compatibility verdict for q03;
- confirmation: harness-only, no core change, `--engines all` unchanged, no committed perf.data, stability gate met.

## Risk / blast radius
- **Polars compile weight** -> feature-gated in `toron-tpch`; default builds stay light.
- **Absent/unsupported** -> classified rows, rail never aborts.
- **Version drift** -> `engine_version` on every row.
- **Harness leak into core** -> `crates/toron/src` unchanged; `polars_sql` harness-only, explicit `--engines`.

## Seal
`plan: adr 394 polars peer substrate reference` (the dual). Implementation (adapter + results):
`impl: adr 394 polars peer substrate reference`. Do not commit `.perf.data`.
