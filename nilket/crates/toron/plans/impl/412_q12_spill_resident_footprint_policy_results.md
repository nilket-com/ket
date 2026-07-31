# ADR 412 - q12 spill resident-footprint policy results

ADR 412 added a durable component split to the streamed inner spill resident-state guard, then used it to classify
q12's 67,728-byte edge before cutting policy. The cut is not a cap bump: the 64 MiB task budget, x16 retained-spill
multiplier, and 4 GiB scope are unchanged.

## Phase 0

Fresh release build with the component payload, before the policy fix:

```bash
CARGO_BUILD_JOBS=12 cargo build -p toron-tpch --release --features polars-sql-reference
rm -rf target/tpch/perf_adr412_q12_phase0
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  target/release/toron-tpch perf-run-one \
    --engine toron \
    --query q12 \
    --mode strict \
    --sf 10 \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr412_q12_phase0 \
  > target/tpch/adr412_q12_phase0.out
```

Result:

| query | status | execute_ms | producer | requested | available |
|---|---|---:|---|---:|---:|
| q12 | fail | 8,692.4 | streamed inner join build spill resident state | 1,073,809,552 | 1,073,741,824 |

Component detail:

| site | resident_index_bytes | writer_peak_bytes | page_reserve_bytes | pinned_decoded_bytes |
|---|---:|---:|---:|---:|
| write | 1,073,656,584 | 87,432 | 65,536 | 0 |

Classification: **writer scratch was a declinable retained component at the write guard**. The resident index alone
still fit the cap at this point. Pinning was not involved (`pinned_decoded_bytes=0`, site=`write`), so the stale
post-pin hypothesis is rejected for q12.

## Cut

The write guard now sheds retained writer scratch before failing:

- `release_write_scratch()` drops the retained plaintext/ref buffers and resets `writer_peak` to zero;
- `write_page_batch_inner` calls a write-site guard that sheds this scratch only if the resident total is over cap;
- the same guard is immediately retried, so a genuinely over resident index still fails with the same producer.

The component payload is kept permanently through `ExecutionError::BudgetExhausted.detail`. Normal budget errors keep
an empty detail and render as before; the streamed inner spill resident guard adds:

```text
site=<write|rebuild_index|pin_decoded_pages|finish>,
resident_index_bytes=<n>,
writer_peak_bytes=<n>,
page_reserve_bytes=<n>,
pinned_decoded_bytes=<n>
```

Discriminating tests:

```bash
cargo test -p toron inner_spill_guard_reports_component_detail_and_site --lib -- --nocapture
cargo test -p toron inner_spill_write_guard_sheds_writer_scratch_but_not_index_pressure --lib -- --nocapture
cargo test -p toron incremental_resident_guard_decisions_match_fold --lib -- --nocapture
```

All three passed. The new tests lock both sides of the policy: detailed diagnostics are present, writer scratch is
shed before failing, and a resident index plus page reserve that is genuinely over cap still fails.

## q12 Post-Fix

Post-fix probe:

```bash
rm -rf target/tpch/perf_adr412_q12_post
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  target/release/toron-tpch perf-run-one \
    --engine toron \
    --query q12 \
    --mode strict \
    --sf 10 \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr412_q12_post \
  > target/tpch/adr412_q12_post.out
```

Reproduction:

```bash
rm -rf target/tpch/perf_adr412_q12_post_repro
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  target/release/toron-tpch perf-run-one \
    --engine toron \
    --query q12 \
    --mode strict \
    --sf 10 \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr412_q12_post_repro \
  > target/tpch/adr412_q12_post_repro.out
```

| run | status | execute_ms | requested | available | detail |
|---|---|---:|---:|---:|---|
| post | fail | 8,130.9 | 1,074,443,016 | 1,073,741,824 | `site=write, resident_index_bytes=1074377480, writer_peak_bytes=0, page_reserve_bytes=65536, pinned_decoded_bytes=0` |
| repro | fail | 8,028.0 | 1,074,443,016 | 1,073,741,824 | `site=write, resident_index_bytes=1074377480, writer_peak_bytes=0, page_reserve_bytes=65536, pinned_decoded_bytes=0` |

Verdict: q12 is **not green**. ADR 412 removes the false writer-scratch edge and reproduces the remaining wall as
genuine resident-index pressure under the same guard. The next q12 mechanism is not a cap tweak; it needs to reduce or
partition the spilled-build resident index footprint.

## Controls

sf=10 persisted correctness controls:

```bash
for q in q03 q04 q08 q10 q19; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G \
    --property=MemoryHigh=4G \
    --property=TasksMax=512 \
    --property=RuntimeMaxSec=180s \
    target/release/toron-tpch correctness --query "$q" --mode strict --sf 10 \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr412_controls_sf10.out 2>> target/tpch/adr412_controls_sf10.err
done
```

| query | result |
|---|---:|
| q03 | `pass@compare`, 26,112.7 ms |
| q04 | `pass@compare`, 17,195.3 ms |
| q08 | `pass@compare`, 33,058.3 ms |
| q10 | `pass@compare`, 123,292.5 ms |
| q19 | `pass@compare`, 105,126.1 ms |

q03 sf=1 four-way control:

```bash
rm -rf target/tpch/perf_adr412_q03_sf1_fourway_duckdb
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=180s \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.local/bin/duckdb \
  target/release/toron-tpch perf \
    --sf 1 \
    --mode strict \
    --runs 7 \
    --warmup 1 \
    --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr412_q03_sf1_fourway_duckdb \
  > target/tpch/adr412_q03_sf1_fourway_duckdb.out \
  2> target/tpch/adr412_q03_sf1_fourway_duckdb.err
```

Artifact: `target/tpch/perf_adr412_q03_sf1_fourway_duckdb/latest_summary.jsonl`.

| engine | status | median_ms | spread | fingerprint | spill_bytes | flags |
|---|---|---:|---:|---|---:|---|
| toron | pass | 373.754 | 0.031 | `666e0ac028036f70` | 0 | none |
| rune | pass | 381.893 | 0.028 | `666e0ac028036f70` | 0 | none |
| polars_sql | pass | 105.397 | 0.127 | `fnv1a64:a3b5c15847833ac3` | 0 | none |
| duckdb | pass | 50.000 | 0.220 | `fnv1a64:73b142150e59739f` | 0 | `unstable,schema_mismatch` |

Toron/Rune shape closure holds (`666e0ac...`, no spill). DuckDB remains context-only for the short sf=1 row because
it is flagged unstable.

## Verification

Commands run:

```bash
cargo fmt --all --check
cargo check -p toron --lib
cargo test -p toron inner_spill_guard_reports_component_detail_and_site --lib -- --nocapture
cargo test -p toron inner_spill_write_guard_sheds_writer_scratch_but_not_index_pressure --lib -- --nocapture
cargo test -p toron incremental_resident_guard_decisions_match_fold --lib -- --nocapture
CARGO_BUILD_JOBS=12 cargo build -p toron-tpch --release --features polars-sql-reference
CARGO_BUILD_JOBS=12 cargo test -p toron --test architecture_test
CARGO_BUILD_JOBS=12 cargo test -p toron --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

Results:

- `cargo fmt --all --check`: pass
- `cargo check -p toron --lib`: pass
- focused spill guard tests: 3 passed
- `architecture_test`: 2,079 passed, 0 failed
- `toron --no-fail-fast`: pass
- `toron-tpch --features polars-sql-reference --no-fail-fast`: pass
- `git diff --check`: clean
- `git ls-files '*.perf.data'`: none

## Verdict

ADR 412 is a bounded resident-footprint policy correction and diagnostic improvement:

- the original q12 edge was correctly classified as retained writer scratch at the write guard;
- writer scratch is now shed before failing, without changing any budget cap or multiplier;
- the guard remains armed and q12's remaining blocker is reproduced as resident-index pressure with
  `writer_peak_bytes=0`;
- all completed sf=10 controls remain correct;
- q03 sf=1 shape closure and the peer-reference scorecard remain intact.

Forward: q12 needs a resident-index footprint mechanism; q05 remains the reservoir capstone.
