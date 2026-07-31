# ADR 386 implementation plan - SF=10 post-385 mini-atlas

Pairs with `plans/adr/386_sf10_post_385_mini_atlas.md`.

This is a measurement-only cut. It refreshes the cells moved by ADRs 374-385 and ranks the next cut from current live
evidence. It must not modify `crates/toron/src` or any budget constants.

## Phase 0 - Setup and Anchors

Verify the persisted sf=10 dataset:

```bash
test -f target/tpch/data/strict/sf10/manifest.json
```

Build release outside the fixed-cap run:

```bash
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
```

Smoke anchors under the fixed 4 GiB scope:

```bash
for q in q06 q14; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch perf \
      --sf 10 --mode strict --runs 1 --warmup 0 --query "$q" \
      --engines toron \
      --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr386/${q}"
done
```

Gate:

- q14 should produce a Toron pass row and live persisted correctness should pass.
- q06 should produce a current live classification; do not assume its 366 class.
- Raw rows must include status, reason, diagnostic message, execute_ms, spill_bytes, plan_fingerprint, and engine.

## Phase 1 - Target Set

Run the focused target set:

```bash
queries=(q03 q04 q05 q06 q07 q08 q10 q12 q14 q17 q19 q21)
for q in "${queries[@]}"; do
  rm -rf "target/tpch/perf_adr386/${q}"
  mkdir -p "target/tpch/perf_adr386/${q}"
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
    timeout --signal=INT 360s \
    target/release/toron-tpch perf \
      --sf 10 --mode strict --runs 1 --warmup 0 --query "$q" \
      --engines toron,duckdb \
      --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr386/${q}"
done
```

If combined `toron,duckdb` hides one engine's row after a timeout or failure, rerun that query with separate
`--engines toron` and `--engines duckdb` invocations and record the split-run reason.

If q07/q17 need a longer time-wall sample to classify, rerun only that query with a documented timeout. Do not raise the
memory cap.

## Phase 2 - Correctness Provenance

For every target Toron pass row:

```bash
CARGO_CGROUP_BACKEND=affinity target/release/toron-tpch correctness \
  --query <query> --sf 10 --data-source parquet --data-dir target/tpch/data
```

Record `pass@compare`, oracle/policy gap, or correctness failure. A pass row without persisted correctness is not green.

## Phase 3 - Results Doc

Create `crates/toron/plans/impl/386_sf10_post_385_mini_atlas_results.md` with:

1. Setup:
   - commit,
   - dataset manifest path/fingerprint if available,
   - fixed cap,
   - timeout,
   - DuckDB version if collected.
2. Target-set table:
   - query,
   - previous class from the latest relevant ADR,
   - current Toron status/reason/producer/bytes,
   - execute_ms/spill_bytes/plan_fingerprint,
   - correctness provenance,
   - DuckDB reference if collected.
3. Changed-cell summary:
   - q19 re-confirmed or regression named;
   - q03/q05/q10/q12 current class;
   - q08 memory-tail control verdict and whether carried-forward memory-tail rows remain validated;
   - q07/q17/q21 current class.
4. Ranking:
   - next-cut candidates ordered by fresh count/leverage;
   - carried-forward memory-tail rows listed separately with provenance tags, not merged into fresh counts.

## Verification

Required for measurement-only ADR:

```bash
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
git diff --check
```

If any harness code changes become necessary, stop and either:

- keep them measurement-only and add `fmt`, `check`, `test -p toron-tpch`, and `test -p toron --no-fail-fast`; or
- split the harness/engine change into a separate ADR.

No `crates/toron/src` changes are expected.

## Seal

Commit as:

```text
plan: adr 386 sf10 post 385 mini atlas
```

Implementation/results commit, when run:

```text
impl: adr 386 sf10 post 385 mini atlas
```
