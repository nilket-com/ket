# ADR 393 results - Rune q03 scan-output reach

## Summary

ADR 393 removed the immediate persisted sf=3 Rune q03 `scan output accumulator` wall for the Rune-authored
`auto_join` shape. It did not produce the clean `Rune/DuckDB` substrate ratio ADR 392 wanted: after the accumulator
wall is gone, Rune q03 is time-bound in the bounded sf=3 probe.

The implementation is a topology-based executor change, not a Rune/q03 branch:

- `try_streaming_reoriented_inner_chain_grouped_aggregate` now accepts both exchange-wrapped aggregate inputs and bare
  reoriented-inner-chain aggregate inputs.
- The existing inner-chain recognizers remain the shape fence.
- Bare under-cap stage scans reuse the batches already read by the memory-aware decision, avoiding the duplicate sf=1
  scan that an estimate-then-fallback path would introduce.

## Phase 0

The ADR 391/392 fingerprint discrepancy was real and reconciled before implementation:

| source | data source / scale | Rune q03 fingerprint | explanation |
| --- | --- | --- | --- |
| ADR 391 isolation proof | in-memory sf=0.01 | `0fd3784049e71920` | lower join uses the in-memory catalog shape |
| ADR 392 perf row | persisted sf=1/sf=3 | `2b72c561c2595512` | persisted catalog/stats bind both joins as `HashPartitioned` |

The persisted shape is the relevant ADR 393 target. Its grouped aggregate input is a bare top-level inner chain:

```text
Aggregate {
  input: Join { replay_id: JoinReplayId(1), strategy: HashPartitioned, ... }
}
```

The existing SQL-shaped streaming path expected:

```text
Aggregate {
  input: Exchange { input: Join { replay_id: JoinReplayId(1), ... }, ... }
}
```

That wrapper mismatch kept the persisted Rune shape out of the existing reoriented-inner-chain grouped streaming path.

## Implementation

The entry gate now normalizes only the aggregate-input wrapper:

```text
Exchange(input) -> input
bare input      -> input
```

After that, the same existing recognizers decide whether the plan is eligible:

- `extract_reoriented_inner_chain`
- `reoriented_inner_chain_grouped_base_scan_probe_mode`
- `decompose_streaming_scan_probe_input`

For bare inputs, the stage scan-build decision is memory-aware:

| decision | behavior |
| --- | --- |
| over scan-output cap | stream the scan build through the existing streamed-scan-build runtime |
| under scan-output cap | build the runtime from the batches read by the cap decision |
| non-scan / low-budget path | use the existing materialized runtime path |

The cached under-cap branch is load-bearing for the sf=1 control: it avoids scanning a stage once for the gate and then
again for the materialized runtime.

## Results

### sf=1 control

Release, persisted Parquet, fixed 4 GB, runs=7, warmup=1:

| engine | status | phase | execute_ms median | min | max | spread | rows | peak RSS | spill | flags |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| rune | pass | compare | 1311.8 | 1298.6 | 1326.1 | 0.021 | 11620 | 1,166,448 KiB | 0 | none |

Control verdict: no small-scale regression. ADR 392's Rune sf=1 median was `1580.6 ms`; the final ADR 393 path is
faster (`1311.8 ms`) because it reuses under-cap stage batches rather than duplicating the scan.

### sf=3 canary

The old ADR 392 persisted Rune sf=3 row failed fast:

```text
engine=rune phase=execute status=fail reason=memory_budget_exhausted
producer="scan output accumulator"
requested=1,073,790,848 available=1,073,741,824
execute_ms_median=1486.4
```

After ADR 393, the same shape no longer fails at that producer. A bounded current-code probe was still active after
two minutes:

```text
adr393-rune-sf3.service active for 2min 0s
Memory=2.0G, peak=2.0G, CPU=1min 59s
command=target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 3 --data-source parquet
```

The earlier seven-run board attempted before the under-cap cache refactor also stayed active in the Rune warmup for
about nine minutes on the same over-cap streamed branch. The final under-cap cache affects sf=1/under-cap stages; it
does not change the sf=3 over-cap streaming branch.

DuckDB sf=3 reference, same persisted data, fixed 4 GB, runs=7, warmup=1:

| engine | status | execute_ms median | min | max | spread | rows | peak RSS | spill | flags |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| duckdb | pass | 93.0 | 92.0 | 95.0 | 0.032 | 34207 | 252,700 KiB | 0 | none |

## Verdict

ADR 393 is a reach wall-move:

- `scan output accumulator` is gone for persisted Rune q03 sf=3.
- Rune q03 still does not complete in the bounded probe, so there is no valid sf=3 `Rune/DuckDB` substrate ratio.
- The next measurement, when this path completes, must be labeled a **streaming-substrate** ratio: it compares Toron's
  streaming/spill memory model against DuckDB's in-memory execution at sf=3, not a pure Arrow-kernel/materialize-each
  ratio.

The next cut should profile the now-running Rune q03 sf=3 path, or choose a smaller completing scale/window, to name the
time-bound producer before further reach/perf work.

## Verification

```text
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
git diff --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib reoriented_inner_chain_generic_base_consumes_streamed_stage_runtime -- --nocapture
scripts/cargo-cgroup test -p toron-tpch adr391_rune_perf_decomposition -- --nocapture
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_PROFILE_RELEASE_DEBUG=true scripts/cargo-cgroup build -p toron-tpch --release
systemd-run ... perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 --engines rune --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr393_q03_sf1_rune_control_cached
systemd-run ... perf-run-one --engine rune --query q03 --mode strict --sf 3 --run-index 0 --data-source parquet --data-dir target/tpch/data
systemd-run ... perf --sf 3 --mode strict --runs 7 --warmup 1 --query q03 --engines duckdb --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr393_q03_sf3_duckdb_only
```
