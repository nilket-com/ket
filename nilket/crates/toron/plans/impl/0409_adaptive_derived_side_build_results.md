# ADR 409 results - adaptive derived-side build

## Summary

ADR 409 executed ADR 408's named fork: when the terminal reoriented streamed-inner stage would build on a catastrophically
large scan side, Toron now tries to materialize the derived side under the cap, builds on it if the measured bytes fit,
and streams the scan side once. If the measured derived side does not fit, the old partitioned scan-build path remains
the fallback. No cap was bumped and there is no q03/table-specific branch.

The q03 sf=10 wall moved decisively:

| item | ADR 408 diagnostic | ADR 409 |
|---|---:|---:|
| q03 sf=10 status | time/no-row | pass |
| q03 sf=10 execute | no row in 360 s | 19,441.666 ms |
| q03 sf=10 correctness | n/a | pass@compare |
| lineitem scans | 31 starts, about 30.8 passes | 1 pass |
| lineitem rows read | 1,849,706,360 in 180 s | 59,986,052 |
| lineitem bytes read | 82,081,050,880 in 180 s | 2,661,892,096 |
| max RSS | about 1.54 GiB in ADR 408 timeout probe | 531,700 KiB |

This is the intended mechanism proof: q03 no longer rereads projected lineitem once per partition group. It reads the
leaf scan once, builds on the measured derived side, and completes under the fixed 4 GiB scope.

The first post-cut q03 run completed in 16,627.737 ms. After the fingerprint-only harness fix below, the refreshed
same-mechanism run completed in 19,441.666 ms. Both rows prove the same reach-board move and one-pass scan mechanism;
the refreshed row is the final artifact used below.

## Phase 0

The code grounding from the ADR was correct: `extract_reoriented_inner_chain` structurally makes the chain/derived side
the probe and the leaf scan side the build. Size was not part of that decision. The q03 sf=10 failure was therefore a
bad execution-side orientation for the over-cap case, not a SQL/Rune shape problem.

Final plan-proof fingerprints after the implementation:

| scale | SQL fingerprint | Rune fingerprint | plans differ? |
|---:|---|---|---|
| sf=1 | `666e0ac028036f70` | `666e0ac028036f70` | false |
| sf=10 | `666e0ac028036f70` | `666e0ac028036f70` | false |

`estimated_byte_size` is a routing hint, not plan semantics, so the TPC-H harness excludes it from the plan-fingerprint
preimage. That restores the ADR 401-408 convention: cross-scale fingerprint equality still means shape closure, while
the runtime can still use scan-size metadata for the catastrophic-route guard.

Supporting size evidence for the derived side:

```bash
duckdb -csv -c "SELECT count(*) AS rows
FROM read_parquet('target/tpch/data/strict/sf10/customer.parquet') c
JOIN read_parquet('target/tpch/data/strict/sf10/orders.parquet') o
  ON c.c_custkey=o.o_custkey
WHERE c.c_mktsegment='BUILDING'
  AND o.o_orderdate < DATE '1995-03-15';"
```

Result:

```text
rows
1461923
```

This is support, not the decision source. The actual runtime flip is still measured inside Toron: the derived side is
materialized under the cap, its hash-build bytes are checked, and the flip declines if the measured value exceeds the
budget.

Order/consumer audit: the adaptive cut is only used for the terminal inner-only reoriented stage. The consumer above q03
is grouped aggregate + sort, and the implementation restores the observable column order through the existing
`probe_columns_first` machinery. The unit test locks both the one-scan behavior and the restored output order.

## Implementation

The implementation has three parts:

- `ExecutionPlan::Scan` now has `estimated_byte_size: Option<u64>`, seeded by the binder from optimizer table facts and
  preserved through scan projection. This is used only as a catastrophic over-cap routing hint.
- The terminal reoriented stage can defer the old partitioned scan-build path and try an adaptive derived-side build.
  The catastrophic shortcut only fires when the scan estimate is greater than eight times the materialization cap. That
  is a capacity argument, not benchmark tuning: at >=8x cap, the old grouped path needs many scan passes before it can
  make progress, so one cap-guarded attempt to build the other side is the safer first move. Moderate over-cap scans
  keep the old measured path because projected metadata can be much larger than the actual filtered/materialized build.
- The adaptive path materializes the derived side under cap, checks its measured hash-build bytes, builds with the
  existing in-memory join table, and streams the leaf scan side once. `StreamMaterializedBatches` preserves already-read
  scan batches for measured over-cap decisions so the fallback remains behavior-neutral.

Runtime fallback remains intact:

- measured derived side over cap -> old partitioned scan-build path;
- non-terminal or unsupported stage -> old path;
- moderate scan-size cases -> old measured decision path;
- below-cap stages -> old materialized path.

Fork B, the one-pass partition reservoir, remains named for cases where the derived side cannot fit.

## Tests

Focused tests:

```text
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron adaptive_derived_last_stage --lib -- --nocapture
```

Result:

```text
2 passed; 0 failed
```

The tests cover:

- over-cap terminal scan-build + fitting derived side -> adaptive flip, leaf side scanned exactly once, output order
  restored;
- measured derived side over cap -> adaptive path declines and the old path remains available.

Reoriented inner-chain tests:

```text
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron reoriented_inner --lib -- --nocapture
```

Result:

```text
9 passed; 0 failed
```

The existing decision test was updated to expect `StreamMaterializedBatches` for the measured hash-over-cap branch.

## q03 sf=10 result

Command:

```bash
rm -f target/tpch/adr409_q03_sf10_scan_progress_fingerprint_fixed.jsonl \
      target/tpch/adr409_q03_sf10_probe_fingerprint_fixed.json \
      target/tpch/adr409_q03_sf10_probe_fingerprint_fixed.err

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr409_q03_sf10_scan_progress_fingerprint_fixed.jsonl \
  /usr/bin/time -v timeout -k 10s 360s \
    target/release/toron-tpch perf-run-one \
      --engine toron \
      --query q03 \
      --mode strict \
      --sf 10 \
      --run-index 0 \
      --data-source parquet \
      --data-dir target/tpch/data \
  > target/tpch/adr409_q03_sf10_probe_fingerprint_fixed.json \
  2> target/tpch/adr409_q03_sf10_probe_fingerprint_fixed.err
```

Result:

| metric | value |
|---|---:|
| status | pass |
| execute_ms | 19,441.666 |
| total_ms | 19,449.556 |
| output rows | 114,003 |
| spill bytes | 233,338,304 |
| plan fingerprint | `666e0ac028036f70` |
| max RSS | 531,212 KiB |
| wall | 23.42 s |
| user | 18.68 s |
| sys | 4.74 s |
| CPU | 100% |

Progress summary from `target/tpch/adr409_q03_sf10_scan_progress_fingerprint_fixed.jsonl`:

| table | scan starts | raw rows | output rows | raw bytes | output bytes |
|---|---:|---:|---:|---:|---:|
| customer | 3 | 4,500,000 | 900,828 | 112,245,492 | 7,628,544 |
| orders | 5 | 75,000,000 | 75,000,000 | 1,828,195,200 | 1,828,195,200 |
| lineitem | 1 | 59,986,052 | 59,986,052 | 2,661,892,096 | 2,661,892,096 |

The remaining repeated scans are on the much smaller derived-side inputs. The catastrophic repeated lineitem scan is
gone.

Correctness:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  target/release/toron-tpch correctness \
    --query q03 \
    --sf 10 \
    --mode strict \
    --data-source parquet \
    --data-dir target/tpch/data
```

Result:

```text
tpch_correctness query=q03 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=24027.0
```

## sf=1 control

Command:

```bash
rm -rf target/tpch/perf_adr409_q03_sf1_control_fingerprint_fixed

systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
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
    --output-dir target/tpch/perf_adr409_q03_sf1_control_fingerprint_fixed
```

Artifact: `target/tpch/perf_adr409_q03_sf1_control_fingerprint_fixed/latest_summary.jsonl`.

| engine | status | median | spread | fingerprint | spill bytes | flags |
|---|---|---:|---:|---|---:|---|
| toron | pass | 380.623 ms | 0.036 | `666e0ac028036f70` | 0 | none |
| rune | pass | 376.753 ms | 0.044 | `666e0ac028036f70` | 0 | none |
| polars_sql | pass | 101.759 ms | 0.122 | `fnv1a64:a3b5c15847833ac3` | 0 | none |
| duckdb | pass | 44.000 ms | 0.273 | `fnv1a64:73b142150e59739f` | 0 | `unstable,schema_mismatch` |

The important control is Toron/Rune equality plus `spill_bytes_max=0`: the adaptive shortcut does not fire on the sf=1
path, and the previously won shape/substrate ground is retained. DuckDB is context only for this short run because the
spread is unstable.

## Controls

Final persisted sf=10 controls on the same release binary:

| query | result |
|---|---|
| q04 | pass@compare, 15,508.601 ms |
| q08 | pass@compare, 29,923.262 ms |
| q19 | pass@compare, 102,279.275 ms |

## q05/q10/q12 sibling probes

The ADR requested a cheap sibling re-probe. These are generalization signals, not q03 acceptance criteria.

Commands used the same bounded shape as q03:

```bash
for q in q05 q10 q12; do
  rm -f "target/tpch/adr409_${q}_sf10_scan_progress_final.jsonl" \
        "target/tpch/adr409_${q}_sf10_probe_final.json" \
        "target/tpch/adr409_${q}_sf10_probe_final.err"
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G \
    --property=MemoryHigh=4G \
    --property=TasksMax=512 \
    --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH="target/tpch/adr409_${q}_sf10_scan_progress_final.jsonl" \
    /usr/bin/time -v timeout -k 10s 360s \
      target/release/toron-tpch perf-run-one \
        --engine toron \
        --query "$q" \
        --mode strict \
        --sf 10 \
        --run-index 0 \
        --data-source parquet \
        --data-dir target/tpch/data \
    > "target/tpch/adr409_${q}_sf10_probe_final.json" \
    2> "target/tpch/adr409_${q}_sf10_probe_final.err" || true
done
```

Outcome:

| query | status | elapsed | peak RSS | producer / reason | interpretation |
|---|---|---:|---:|---|---|
| q05 | timeout, no JSON row | 360.30 s | 3,076,416 KiB | CPU-bound repeated scan work | still a time wall |
| q10 | fail | 7.633 s | 1,894,288 KiB | `reoriented join build accumulator`, 1,776,981,960 / 1,073,741,824 | q08-style build-accumulator wall |
| q12 | fail | 9.466 s | 2,156,392 KiB | `streamed inner join build spill resident state`, 1,073,809,552 / 1,073,741,824 | spill resident-state edge |

Progress summaries:

| query | table | scan starts | raw rows | raw bytes | output rows | output bytes |
|---|---|---:|---:|---:|---:|---:|
| q05 | lineitem | 44 | 2,629,309,996 | 127,193,499,136 | 2,629,309,996 | 127,193,499,136 |
| q05 | orders | 4 | 60,000,000 | 1,216,921,728 | 60,000,000 | 1,216,921,728 |
| q05 | customer | 3 | 4,500,000 | 72,851,520 | 4,500,000 | 72,851,520 |
| q05 | supplier | 2 | 200,000 | 3,248,896 | 200,000 | 3,248,896 |
| q05 | nation | 2 | 50 | 2,144 | 50 | 2,144 |
| q05 | region | 2 | 10 | 688 | 2 | 208 |
| q10 | lineitem | 2 | 119,972,104 | 5,447,037,080 | 29,616,366 | 1,218,397,296 |
| q12 | lineitem | 1 | 59,986,052 | 1,949,334,188 | 17,140,455 | 509,691,888 |
| q12 | orders | 1 | 15,000,000 | 407,667,452 | 15,000,000 | 407,667,452 |

Sibling verdict: q03's adaptive terminal-stage flip is real but not sufficient for the whole family. q05 still has a
lineitem re-scan loop, q10 exposes the same accumulator class q08 had before ADR 403, and q12 is now back at a tiny
resident-state spill edge. These are the next named producers; they are no longer hidden behind q03's terminal
lineitem loop.

## Verification

Completed:

```text
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup fmt
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup build -p toron-tpch --release --features polars-sql-reference
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron adaptive_derived_last_stage --lib -- --nocapture
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron reoriented_inner --lib -- --nocapture
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron-tpch physical_plan_fingerprint_shape_ignores_scan_size_routing_hint --lib -- --nocapture
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron --test architecture_test --no-fail-fast
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

Final gate results:

```text
toron lib: 1220 passed; 0 failed
fingerprint-routing-hint test: 1 passed; 0 failed
architecture_test: 2079 passed; 0 failed
toron --no-fail-fast: passed
toron-tpch --features polars-sql-reference --no-fail-fast: passed
```

The full `toron` gate was rerun at `CARGO_CGROUP_JOBS=4` because the 12-job run was killed by systemd-oomd during
compilation. The machine has enough cores, but this crate's full test build is memory-bound under the 4 GiB cgroup.

## Verdict

ADR 409 lands fork A. The q03 sf=10 timeout was not an execution-model floor; it was a bad over-cap orientation that
caused repeated lineitem scans. The adaptive measured derived-side build removes that loop, keeps the sf=1 path on the
old no-spill route, and preserves the reservoir fork for derived sides that do not fit.

The sibling sweep is now done. The next cut can choose among three named producers: q05's remaining repeated lineitem
loop, q10's reoriented build accumulator wall, or q12's tiny streamed-spill resident-state edge. q03's remaining
orders/customer rescans are also visible but are no longer the reach blocker for q03.
