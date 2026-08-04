# ADR 356 follow-up plan - post-432 article performance baseline

This measurement-only plan applies ADR 356's repeated-run method to the sealed post-432 sf=10 board. It produces
the article-grade throughput baseline after the separate ADR 415 correctness census and ADR 394 Polars capability
census. No engine or adapter behavior changes are admitted in this cut.

## Scope

- Toron: q01-q22.
- DuckDB: q01-q22.
- Polars SQL: only the measured capability-pass set `{q01, q03, q06, q14, q19}`.
- Persisted strict sf=10 Parquet, one release binary, seven measured fresh child processes after one discarded
  cache-conditioning warmup.
- Primary metric: each adapter's `execute_ms`; report median, minimum, maximum, and
  `(maximum - minimum) / median` spread.

Polars is a peer-timing reference, not a second correctness oracle. q03 is value-validated by its dedicated oracle
gate; q01, q06, q14, and q19 remain explicitly labeled reach+schema-only.

## Measurement envelope

Every engine/cell command runs in its own user systemd scope:

```text
MemoryHigh=infinity
MemoryMax=infinity
AllowedCPUs=0-1
CPUQuota=150%
TasksMax=512
RuntimeMaxSec=3600
```

CPUs 0-1 are SMT siblings on one P-core. This is a new memory-unbounded, compute-fixed throughput profile. It makes
the three engines comparable within this rail; its timings are not cross-comparable with ADR 415, whose scope did not
pin CPU affinity or quota. Record the host CPU, RAM, swap, kernel, frequency governor, binary SHA-256, manifest
SHA-256, DuckDB version, Polars version, and measurement window in the results.

## Run protocol

Run 49 isolated single-engine cells, never `--query all` or a combined engine list:

- 22 Toron cells;
- 22 DuckDB cells;
- 5 Polars SQL cells.

Each command uses this shape with a distinct output directory:

```text
target/release/toron-tpch perf \
  --engines <toron|duckdb|polars_sql> --query <qNN> \
  --sf 10 --mode strict --runs 7 --warmup 1 \
  --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/article_perf_post432/<engine>/<qNN>/attempt1
```

Before and after each scope, verify `target/tpch/join_spill` is empty. Retain the raw JSONL, summary JSONL, parent
stdout/stderr, and scope metadata before starting the next cell.

If the summary has `unstable` (`spread > 0.20`), retain attempt 1 and run at most one isolated attempt 2. Never average
attempts. Bank a clean retry when it passes every audit; if the retry remains unstable, report it as unstable rather
than presenting a clean number. Apply the same one-retry ceiling to a scoped failure.

## Raw-row admission audit

Before banking a median, inspect all seven measured rows, excluding the warmup:

- all engines: status pass, release profile, invariant plan fingerprint, output schema fingerprint, and output row
  count;
- Toron: invariant digest and plan fingerprint equal to the post-432 ADR 415 redux receipt;
- DuckDB: digest is null by design; no digest claim;
- Polars: digest is null by design and plan fingerprint equals the ADR 394 capability-census receipt.

Any within-sample divergence invalidates that cell's median. Do not average different plans, schemas, row counts, or
Toron digests into one timing result.

## Statistics and flags

- Median: middle of seven measured `execute_ms` rows.
- Spread: `(max - min) / median`.
- Stability: `unstable` when spread is greater than `0.20`.
- Regression band for a later comparable baseline:
  `max(0.10, 2 * max(current spread, baseline spread))`.
- Ratios: Toron median divided by DuckDB median and, for the five admitted cells, Toron median divided by Polars
  median.

This artifact establishes a new baseline, so it does not infer a repeated-run regression from ADR 415's one-shot
timings. Timing flags are recorded findings, not hard CI failures.

## Results artifact

Write `plans/impl/0356_article_perf_baseline_post432_results.md` with:

- complete provenance and exact envelope;
- a 22-row Toron/DuckDB board plus five Polars rows and validation levels;
- per-engine median, min, max, spread, peak RSS, spill bytes, and flags;
- Toron/DuckDB and Toron/Polars median ratios;
- the within-sample and external-fingerprint audit;
- retained retry provenance for every initially unstable or failed cell;
- scratch and repository hygiene.

## Acceptance

- All 49 engine/cells have a retained classification from seven measured rows and one discarded warmup, or a named
  bounded failure.
- No clean median is banked when a raw-row invariant differs or spread remains unstable after the one allowed retry.
- All admitted Toron fingerprints match the redux; all admitted Polars fingerprints match the capability census.
- Polars validation levels remain explicit and unchanged.
- No engine or adapter source changes, cap substitution, query rewrite, or dialect expansion.
- Scratch is empty after the rail; no `.perf.data` is tracked.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
git diff --check
git ls-files '*.perf.data'
git status --short
```

Commit this plan before any `impl:` results commit. The completed results remain reviewable and uncommitted until the
evidence audit approves them.
