# ADR 358 results - DuckDB perf adapter

## Summary

Implemented the `toron-tpch` perf adapter plumbing for a DuckDB reference engine without changing the Toron engine:

- Perf summaries and baselines are keyed by `(engine, query)`, not query alone.
- Perf rows now carry `output_row_count`, `output_schema_fingerprint`, and `duckdb_version`.
- The harness can export the exact Toron perf fixtures to Parquet with a manifest containing table row counts, schema fingerprints, and file hashes.
- `perf --engines toron,duckdb` is supported. If `duckdb` is absent, DuckDB rows are emitted as `status=not_attempted reason=duckdb_absent`.
- DuckDB rows use a DuckDB-side `plan_fingerprint` based on SQL + manifest fingerprint, not Toron's bound-plan fingerprint.

## Evidence

Engine-aware summary:

- `summarize_perf_rows` groups by `(engine, query)`.
- `read_baseline_rows` keys baselines by `(engine, query)` and treats old baseline rows without `engine` as `toron`.
- `tpch_perf` summary output includes `engine=...`.
- Tests:
  - `perf_summary_keeps_engines_separate_for_same_query`
  - existing Toron summary regression tests updated to use the engine-aware baseline key.

Data parity export:

- Added `parquet = "58.3.0"` to `crates/toron-tpch` only.
- Export reuses `build_perf_query_spec(query, mode, sf)`, the same SQL/table helper seam used by Toron perf runs.
- Export path is query-scoped under `target/tpch/parquet/<mode>/sf<N>/<query>/...` so helper-specific table projections cannot collide.
- Manifest contains table names, row counts, schema signatures/fingerprints, file hashes, mode, sf, and manifest fingerprint.
- Test:
  - `parquet_export_manifest_matches_exact_perf_tables`

DuckDB adapter:

- `--engines` accepts `toron`, `duckdb`, `toron,duckdb`, or `all`.
- DuckDB CLI is optional and discovered via `duckdb --version` or `TORON_TPCH_DUCKDB_BIN`.
- DuckDB type parity gate checks Parquet `DESCRIBE` + `COUNT(*)` against the manifest before timing.
- Full cross-engine digest is intentionally deferred; row-count/schema sanity is recorded and compared when matching Toron rows are present.
- Tests:
  - `duckdb_absent_rows_summarize_as_not_attempted`
  - `duckdb_schema_fingerprint_normalizes_describe_types`

Local DuckDB availability:

DuckDB 1.5.4 was installed after the first implementation pass. The default `scripts/cargo-cgroup` systemd backend did
not inherit the updated PATH, so the live smoke uses the explicit binary through `TORON_TPCH_DUCKDB_BIN` and the
affinity backend:

```text
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb scripts/cargo-cgroup run -p toron-tpch -- perf --query q06 --sf 0.01 --mode strict --runs 1 --warmup 0 --engines toron,duckdb --output-dir target/tpch/perf_adr358_duckdb_smoke_affinity
```

Output:

```text
tpch_perf engine=duckdb query=q06 status=pass execute_ms_median=1.0 execute_ms_min=1.0 spread=0.000 peak_rss_kib=347224 spill_bytes=0 flags=schema_mismatch
tpch_perf engine=toron query=q06 status=pass execute_ms_median=10.0 execute_ms_min=10.0 spread=0.000 peak_rss_kib=357444 spill_bytes=0 flags=debug_build
```

The raw Toron row includes:

- `output_row_count = 1`
- `output_schema_fingerprint = fnv1a64:976008d6ff3464f1`

The raw DuckDB row includes:

- `status = pass`
- `duckdb_version = v1.5.4 (Variegata) 08e34c447b`
- `output_row_count = 1`
- `output_schema_fingerprint = fnv1a64:a79cda9e0558905c`
- `flags = ["schema_mismatch"]`

The `schema_mismatch` flag is the row-level sanity check doing its job: the output row count agrees, but DuckDB's
derived output schema fingerprint differs from Toron's for q06. Full output normalization/cross-engine digest remains a
follow-on; ADR 358 records the mismatch rather than hiding it.

Full sf=1 release board:

```text
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 1 --mode strict --runs 7 --warmup 1 --query all --engines toron,duckdb --output-dir target/tpch/perf_adr358_sf1_duckdb
```

Result:

- Toron: 22/22 `pass`, no flags.
- DuckDB: 22/22 `pass`; 18 rows flagged `unstable`; 16 rows flagged `schema_mismatch`.
- Baseline was not recorded because mixed-engine rows were not all stable and flag-free. That is correct for ADR 356/358 baseline discipline.

Selected board rows (deduped flags):

```text
duckdb q01 pass 156.0ms schema_mismatch+unstable
duckdb q06 pass 36.0ms schema_mismatch+unstable
duckdb q13 pass 178.0ms none
duckdb q16 pass 67.0ms none
duckdb q21 pass 306.0ms unstable
toron  q01 pass 2.206s none
toron  q06 pass 169.2ms none
toron  q14 pass 90.609s none
toron  q17 pass 93.103s none
toron  q21 pass 17.533s none
```

The release board is the first live Toron-vs-DuckDB sf=1 artifact for the rail. DuckDB's timing noise and schema flags
are now visible inputs for follow-on work rather than hidden adapter uncertainty.

DuckDB q06 instability was checked against raw rows to distinguish fast-query noise from setup leakage:

```text
run execute_ms parent_wall_ms
0   36.0       46.565
1   32.0       42.157
2   40.0       51.606
3   44.0       53.404
4   31.0       41.334
5   40.0       49.829
6   34.0       43.931
```

The measured query is in the 31-44 ms band while parent wall is only ~10 ms higher. That supports the conclusion that
`execute_ms` is not including Parquet export/view setup; the relative instability is the fast-query measurement floor at
sf=1. This is why the DuckDB comparison should be banked at sf=10 rather than forcing an sf=1 DuckDB baseline.

## Verification

Passed:

```text
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup fmt -- --check
git diff --check
scripts/cargo-cgroup run -p toron-tpch -- perf --query q06 --sf 0.01 --mode strict --runs 1 --warmup 0 --engines toron,duckdb --output-dir target/tpch/perf_adr358_smoke
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb scripts/cargo-cgroup run -p toron-tpch -- perf --query q06 --sf 0.01 --mode strict --runs 1 --warmup 0 --engines toron,duckdb --output-dir target/tpch/perf_adr358_duckdb_smoke_affinity
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 1 --mode strict --runs 7 --warmup 1 --query all --engines toron,duckdb --output-dir target/tpch/perf_adr358_sf1_duckdb
scripts/cargo-cgroup test -p toron-tpch perf_summary_deduplicates_row_flags
```

Not run here:

- Full Toron suite / sf=1 perf board, because this cut is harness-only and `toron-tpch` tests plus compile covered the changed surface.

## Follow-up

- Run the full `--engines toron,duckdb` sf=1 perf board to capture the first live DuckDB baseline.
- Add the full cross-engine canonical digest oracle as a separate ADR.
- Address matrix/correctness-matrix cache hygiene separately; ADR 358 does not change those commands.
