# ADR 394 results - Polars peer-substrate reference adapter

ADR 394 landed as a **pure Rust**, harness-only Polars SQL reference adapter. No Python runner, no interpreter env, and
no `crates/toron/src` dependency were added.

## Implementation

- Added `engine=polars_sql` to the perf rail, selected explicitly by `--engines polars_sql`.
- Kept `--engines all` unchanged as Toron + DuckDB.
- Added `engine_version` to perf raw and summary rows; DuckDB now mirrors its existing `duckdb_version` into the generic
  field, and Polars records `polars::VERSION`.
- Pinned `polars` / `polars-sql` in root `[workspace.dependencies]`, while `crates/toron-tpch` owns the optional
  `polars-sql-reference` feature and required Polars features.
- Feature-off explicit Polars runs return `status=not_attempted`, `reason=polars_absent`, not a harness failure.
- Feature-on runs execute inside the existing `perf-run-one` fresh child path, preserving parent RSS/wall accounting.
- Polars SQL uses explicit-join dialect text for the starter comma-join probes (`q03`, `q14`, `q19`); `q06` uses the
  canonical single-table SQL. The Polars plan fingerprint includes the actual SQL text Polars ran.

## Live evidence

Feature-off skip:

```text
scripts/cargo-cgroup run -p toron-tpch -- perf --sf 0.01 --mode strict --runs 1 --warmup 0 \
  --query q03 --engines polars_sql --data-source memory --output-dir target/tpch/perf_adr394_skip
```

Result: `engine=polars_sql`, `status=not_attempted`, `reason=polars_absent`, `engine_version=null`.

Feature-on starter probes, debug smoke:

| query | status | engine_version | execute_ms | rows |
|---|---:|---:|---:|---:|
| q03 | pass | 0.54.4 | 48.7 | 138 |
| q06 | pass | 0.54.4 | 51.7 | 1 |
| q14 | pass | 0.54.4 | 24.4 | 1 |
| q19 | pass | 0.54.4 | 26.0 | 1 |

Release q03 timing smoke:

```text
scripts/cargo-cgroup run -p toron-tpch --features polars-sql-reference --release -- perf \
  --sf 0.01 --mode strict --runs 7 --warmup 1 --query q03 --engines polars_sql \
  --data-source memory --output-dir target/tpch/perf_adr394_polars_q03_release
```

Result: `status=pass`, `engine_version=0.54.4`, median `execute_ms=4.304`, min `4.011`, spread `0.115`, peak RSS
`359340 KiB`, no flags.

## Notes

- The prior local probe established q03 value compatibility against DuckDB/oracle shape; this Rust adapter run records
  row count, schema fingerprint, timing, version, and plan fingerprint. Polars remains a peer timing reference, not a
  second correctness oracle.
- The Python probe's 12/22 SQL subset remains useful grounding, but the Rust `polars-sql` crate rejects TPC-H comma joins.
  ADR 394 therefore lands the adapter and starter explicit-join mappings first; expanding the Rust SQL dialect map to the
  rest of the empirical subset is a follow-up, separate from future Polars Lazy 22 coverage.
- Adding Polars pulled `sqlparser 0.60.0` transitively into `Cargo.lock` while Toron itself remains pinned to
  `sqlparser 0.62`. The architecture guard was tightened to assert Toron's 0.62 lockfile entry instead of assuming the
  workspace lock contains only one `sqlparser` package.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference
scripts/cargo-cgroup test -p toron --test architecture_test -- --nocapture
git diff --check
```

`scripts/cargo-cgroup test -p toron --no-fail-fast` was also run. All non-architecture targets passed; the architecture
target initially failed only on the brittle `sqlparser` lockfile guard described above, then passed after the guard was
corrected.
