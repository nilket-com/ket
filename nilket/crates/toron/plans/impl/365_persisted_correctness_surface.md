# ADR 365 implementation plan - persisted correctness surface

Pairs with `plans/adr/365_persisted_correctness_surface.md`.

This is a `crates/toron-tpch` harness cut. It adds persisted Parquet data-source support to the correctness command so
ADR 366 can verify sf=10 pass rows against independent oracles. It must not change `crates/toron` engine behavior.

## Phase 0 - map the correctness seam

Ground the current shape:

- `main.rs::correctness_command` parses `--query` and `--sf`, but not `--data-source` / `--data-dir`.
- `perf` already routes `--data-source parquet|persisted` through `run_perf_query_with_data_source`.
- `lib.rs::build_persisted_perf_query_spec` already builds the persisted SQL, table metadata, and `ParquetHarnessScan`.
- `check_strict_q*` functions currently build in-memory tables and then compare returned batches to per-query oracles.

Before moving code, write the seam table:

| query | strict SQL source | in-memory table builder | schema assertion | oracle comparator | persisted spec support | q17 policy? |
| --- | --- | --- | --- | --- | --- | --- |

The expected answer is that all correctness-supported queries can use the existing strict SQL + persisted table mapping.
If any query lacks persisted spec support, record it before implementing the broad path.

## D1 - CLI and data-source dispatch

Update `correctness_command` to parse:

- `--data-source` defaulting to `memory`;
- `--data-dir` defaulting to `target/tpch/data`.

Accepted sources:

- `memory`
- `parquet`
- `persisted`

Unsupported sources return usage error through `harness_error`, matching `perf`.

Memory mode keeps the current match arms and behavior. Persisted mode routes to a new correctness execution path.

## D2 - persisted correctness execution helper

Add a helper shaped like:

```rust
async fn execute_strict_query_for_correctness(
    query: &str,
    scale_factor: f64,
    data_source: &str,
    data_root: &Path,
) -> Result<Vec<RecordBatch>, StageError>
```

or equivalent.

For `memory`, use the existing table builder path.

For `parquet|persisted`, use `build_persisted_perf_query_spec(query, "strict", scale_factor, data_root)` and execute
via `execute_query_with_metadata_timed` / existing metadata-storage helper. Do not route through `run_perf_query_*`,
because correctness needs raw result batches and oracle comparison, not a digest/perf row.

Keep the compare logic per query:

- schema assertion stays query-specific;
- independent oracle stays query-specific;
- comparator stays query-specific.

The implementation can either:

- split each `check_strict_q*` into `execute` + `compare` helpers and reuse them from both data sources; or
- add a data-source-aware `check_strict_q*_with_source` wrapper for each query.

Prefer the smallest mechanical split that avoids duplicating oracle logic.

## D3 - q17 policy

q17 currently uses a special correctness scale policy. Treat this explicitly:

- If sf=10 q17 oracle comparison is valid and non-vacuous on persisted data, extend the policy with a focused test.
- If not, make q17 return a clear policy error for sf=10 and record `q17_sf10_oracle_policy_gap` for ADR 366.

Do not silently skip q17 or report a stale lower-scale correctness row as sf=10 evidence.

## Tests

Add focused tests for:

- `correctness` accepts `--data-source parquet --data-dir ...`.
- unsupported `--data-source` fails as usage.
- memory-mode q06 remains unchanged.
- persisted q06 at a small scale matches the in-memory digest/schema path or directly passes compare.
- q17 policy behavior is explicit.

If practical, add a pure unit test around the dispatch parser so the CLI does not regress.

## Live verification

Prepare/build if needed:

```bash
scripts/cargo-cgroup build -p toron-tpch --release
test -f target/tpch/data/strict/sf10/manifest.json || \
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch prepare \
      --data-source parquet --mode strict --sf 10 --data-dir target/tpch/data
```

Run persisted correctness under the fixed cap:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness \
    --query q06 --sf 10 --data-source parquet --data-dir target/tpch/data

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness \
    --query q14 --sf 10 --data-source parquet --data-dir target/tpch/data

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness \
    --query q21 --sf 10 --data-source parquet --data-dir target/tpch/data
```

Expected:

- q06: `pass@compare`.
- q14: `pass@compare`.
- q21: reaches the current named q21 wall (`semi/anti existence-set spill writer` spill budget) unless a later engine
  cut changed it; critically, it must not OOM in fixture generation.

## Required verification

```bash
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```

If any shared engine-facing execution code changes beyond the harness, also run:

```bash
scripts/cargo-cgroup test -p toron --no-fail-fast
```

## Results doc

Create `crates/toron/plans/impl/365_persisted_correctness_surface_results.md` with:

- the seam table;
- the chosen implementation shape;
- q17 policy decision;
- q06/q14/q21 live persisted correctness outputs;
- verification commands;
- confirmation that no `crates/toron/src` code changed.

## Seal

Commit as:

```text
impl: adr 365 persisted correctness surface
```

Then ADR 366 can run the sf=10 atlas with correctness provenance instead of `no_persisted_sf10_oracle` placeholders.
