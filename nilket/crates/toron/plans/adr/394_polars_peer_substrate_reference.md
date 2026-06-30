# ADR 394 - Polars peer-substrate reference adapter

## Status
Proposed.

## Context

A local probe ran the harness's q01-q22 SQL through `pl.SQLContext` on the persisted sf1 Parquet: **12/22 run,
row-exact vs DuckDB** after dialect-compatible handling; failures are the correlated-subquery family + a few dialect
issues. The decisive signal:
**q03 Polars SQL ~23 ms ~= DuckDB ~25 ms, while Toron/Rune q03 is ~1580 ms (ADR 392) - ~60x off both.** So the q03 gap is
**Toron-specific substrate/data-movement inefficiency, not "Arrow engines are slower than DuckDB."** 394 makes Polars a
standing peer-substrate measurement column.

**Polars is not "another DuckDB."** Two distinct reference questions, kept as separate columns:
- **DuckDB** - distance from a top SQL/vectorized engine.
- **Polars** - distance from a fast Rust/Arrow/lazy dataframe engine (our own family).

Polars SQL lowers into Polars expressions and runs through the Polars engine; the expression API is primary and gets
features first (https://docs.pola.rs/user-guide/sql/intro/). Polars' PDS-H benchmark is TPC-H-derived and puts Polars
streaming and DuckDB in the same broad tier (https://pola.rs/posts/benchmarks/). So the **Polars SQL subset** is the
honest near-term surface; the dataframe "Polars Lazy 22" is a separate future reference.

## Decision

Add `engine=polars_sql` as an **optional, harness-only peer-substrate reference**, implemented in Rust behind the
`toron-tpch` feature `polars-sql-reference`.

### Rust adapter contract (feature-gated, raw rows)
- **Dependencies:** root `Cargo.toml` pins `polars` / `polars-sql` versions; `crates/toron-tpch` owns the exact features
  behind `polars-sql-reference`. No `crates/toron/src` dependency.
- **Execution:** `polars_sql` runs inside the existing `perf-run-one` fresh child, like Toron/Rune. The parent still
  records child RSS/wall and the existing summary computes medians/min/spread/stability.
- **Execute-only timing:** `execute_ms` wraps only `SQLContext::execute(...).collect()`. Parquet scan registration and SQL
  lowering stay outside the timed region.
- **Feature-off behavior:** if `toron-tpch` is built without `--features polars-sql-reference`, explicit
  `--engines polars_sql` emits `status=not_attempted`, `reason=polars_absent`, not a harness failure.
- **Dialect mapping:** the Rust `polars-sql` crate rejects TPC-H comma joins. For the starter peer-reference probes,
  the adapter uses value-equivalent Polars SQL text with explicit `JOIN` syntax (`q03`, `q14`, `q19`); `q06` uses the
  canonical single-table SQL. This mapping is Polars-only and is included in the Polars plan fingerprint.

### Version metadata - generalized
Add **`engine_version`** to perf rows, populated for **both DuckDB and Polars** (and any external engine). Do not
overload `duckdb_version` for Polars; keep or remove `duckdb_version` only deliberately for compatibility.

### Engine selection - explicit, `all` unchanged
`polars_sql` is **not** silently added to `--engines all` (which stays Toron+DuckDB). It requires explicit
`--engines polars_sql` or `--engines toron,rune,polars_sql,duckdb`.

### Capability rows never abort the rail
- **Polars feature absent** -> `status=not_attempted(polars_absent)` (skip-clean, no harness failure).
- **Unsupported SQL** -> row-level classified result, **not** a harness failure. The Rust perf row must use the existing
  telemetry contract: `status=not_attempted` with a reason like `polars_sql_unsupported_correlated_subquery` /
  `polars_sql_unsupported_interval_syntax` / `polars_sql_unsupported_join_constraint` /
  `polars_sql_unsupported_index_limit`. Do not add a new perf status unless a separate telemetry ADR owns it.
- Supported-subset runs still produce stable summary rows. The 22-query capability table is **empirical**, not hardcoded;
  the Rust adapter's explicit-join coverage expands query-by-query.

### Value compatibility - a results gate, not schema equality
Decimal-scale differences are expected (Polars q03 scale 2 vs DuckDB scale 4). For q03, the results artifact runs a
**value-compatible comparator** against the q03 oracle / DuckDB (rows, order, values within decimal tolerance) - the
probe already confirmed row-exact + revenue equal to ~2 decimals. Row/schema fingerprints are sanity, not sufficient
truth.

### Report rows + ratios
Real raw rows per query - **`toron` (SQL) | `rune` | `polars_sql` | `duckdb`** - with the results artifact computing
**`Toron/DuckDB`** (distance from top vectorized), **`Toron/Polars`** (the *fixable* gap), **`Polars/DuckDB`**
(architectural context). Start with q03/q06/q14/q19; expand the empirical SQL subset from there, not as a 22/22 claim.

## Acceptance
- **`polars_sql` adapter** via pure Rust `polars` / `polars-sql`, feature-gated by `polars-sql-reference`; **skip-clean**
  (`not_attempted(polars_absent)`) when the feature is absent.
- **Execute-only `execute_ms`** (collect() only; registration/lowering excluded), recorded with `engine_version`.
- **q03 value-compatible** with the q03 oracle / DuckDB (decimal tolerance), proven by the results comparator.
- **Stable timing** under runs=7/warmup=1, medians, the 356 stability gate; unstable -> escalate, don't report.
- **Empirical capability rows** for the Rust adapter; **SQL subset != Lazy 22**, the artifact says so explicitly.
- **No 22/22 oracle claim** - Polars provides peer timing + a value cross-check; the correctness oracle stays Toron's.
- **`--engines all` unchanged**; `engine_version` populated for DuckDB + Polars.
- **Harness-only** (`crates/toron-tpch`); no `crates/toron/src` change; `--no-fail-fast` + `toron-tpch` unchanged; no
  qNN/brand in core; no committed `.perf.data`.

## Guardrails
- **Peer reference, not a second oracle** - timing + value cross-check; the oracle is Toron's.
- **Two distinct baselines** - DuckDB (vectorized) and Polars (Arrow peer) stay separate columns; never merged.
- **Execute-only timing** - Parquet registration and SQL lowering never inflate `execute_ms`.
- **Optional + non-aborting** - absent or unsupported never breaks the rail; classified rows.
- **Subset honesty** - empirical capability table; SQL subset != Lazy 22; classify every failure.
- **Value gate, not schema equality** - decimal-scale tolerance via a value comparator.

## Risks
- **Polars compile weight** - feature-gated in `toron-tpch`; default builds stay light, explicit Polars builds pay the
  dependency cost.
- **Subset over-read as full coverage** - the artifact separates the Rust SQL subset from the future Lazy 22.
- **Version drift** - `engine_version` (`polars::VERSION`) recorded on every row.
- **`polars_sql` leaking into `all` / core** - explicit `--engines` only; harness-only.
- **Faster-because-wrong** - q03 value-compatible comparator gates it.

## Forward Arc
394 turns "Toron vs DuckDB" into a triangulated diagnosis: **DuckDB baseline / Polars peer baseline / Toron+Rune current
path.** The 392/393 substrate arc then reads `Toron/Polars` as the *fixable* gap (peer-achievable) and `Polars/DuckDB` as
the *architectural* context. Follow-ups can expand the Rust SQL dialect map for the remaining SQL-subset queries, and a
later ADR can add the Polars Lazy (dataframe) 22 path for full-coverage peer timing on the queries the SQL subset can't
express (the correlated-subquery family).

## Grounding evidence (file:line / refs)
local probe: `pl.SQLContext` on `target/tpch/data/strict/sf1`, 12/22 SQL row-exact; q03 polars ~23ms vs duckdb ~25ms vs
Toron/Rune ~1580ms (ADR 392); cross-query Polars/DuckDB 0.65x-16x. harness query SQL: `perf_query_sql` lib.rs:4006.
DuckDB adapter env pattern: `TORON_TPCH_DUCKDB_BIN`. persisted surface: ADR 360. perf rail / stability gate: ADR 356.
refs: pola.rs SQL intro + PDS-H benchmark (above).
