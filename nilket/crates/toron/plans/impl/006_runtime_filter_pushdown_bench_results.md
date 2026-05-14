# ADR 006 Runtime Filter Pushdown Bench Results

**Status:** Toron-side implementation evidence and
promotion-scale Wendelon-disabled rows captured; full Phase 0
selective mock-Wendelon row captured; production Wendelon-enabled
selective evidence remains follow-up work.

**Date:** 2026-05-13

## Scope

This artifact records the evidence available with the Toron-side
implementation:

- Runtime-filter descriptor and carrier fields are implemented.
- Build-wave Bloom fragments are generated and merged by the
  scheduler.
- Probe-wave `TaskInputBinding::Scan.runtime_filter` is injected
  and reaches `ScanRequest.runtime_filter`.
- Wendelon-disabled execution remains byte-identical to
  filter-disabled in the runtime-filter integration test.
- A test-only Wendelon mock applies row-level rejects and preserves
  join output digest on a selective fixture.
- The distributed join bench harness can now run the same mock
  Wendelon application path with runtime filters enabled or disabled.
- The full Phase 0 selective fixture has been run against that
  mock Wendelon path at 100K build rows x 100M probe rows.

Production Wendelon scan-engine application remains Wendelon-side
follow-up work. The ADR 006 selective `>=50%` bytes-movement target
is demonstrated at full Phase 0 scale with Toron's mock Wendelon
path below, but is not claimed here for the production Wendelon
scan path.

## Verification

Commands run:

```text
cargo fmt -p toron --check
cargo check --workspace
cargo test -p toron
cargo test -p toron --test runtime_filter_pushdown_test
cargo test -p toron runtime_filter
cargo test -p wendelon runtime_filter
cargo test --workspace
cargo bench -p toron --bench distributed_inner_equi_join --no-run
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only --strategy=hash --runtime-filter=enabled,disabled
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only --strategy=hash --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled --probe-key-domain=32768
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=2,4 --build-rows=10000000 --probe-rows=1000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=disabled
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=8 --build-rows=10000000 --probe-rows=1000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=disabled
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=8 --build-rows=100000 --probe-rows=10000000 --probe-key-domain=10000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=8 --build-rows=100000 --probe-rows=100000000 --probe-key-domain=10000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled
git diff --check
samply record --save-only -o /tmp/adr006_runtime_filter_quick_profile.json.gz -- target/release/deps/distributed_inner_equi_join-72edfd45b70587ad --quick --distributed-inner-equi-join-only --strategy=hash --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled --probe-key-domain=32768
```

Results:

- `cargo test -p toron`: 161 unit tests, 22 architecture tests,
  5 distributed process tests, 5 hash-join fast-path parity tests,
  7 runtime-filter pushdown tests, and 5 Wendelon storage execution
  tests passed.
- `cargo test -p toron runtime_filter`: 13 filtered tests passed.
- `cargo test -p wendelon runtime_filter`: 2 shared-schema
  runtime-filter tests passed.
- `cargo test --workspace`: passed across Toron, Wendelon, and
  Wendelon client crates.
- `distributed_inner_equi_join --no-run`: bench target compiled.
- `distributed_inner_equi_join` quick selective mock-Wendelon row:
  digest-preserving, `telemetry_probe_rows` dropped from 8192 to
  2048, and `shuffle_bytes_published` dropped from 168480 to 68640
  when both Toron runtime filters and mock Wendelon filtering were
  enabled.
- `distributed_inner_equi_join` ADR 005-shaped 10M x 1M rows:
  2/4/8 worker filter-enabled and filter-disabled
  Wendelon-disabled digests all matched ADR 005's canonical digest.
- `distributed_inner_equi_join` 100K x 10M selective
  mock-Wendelon row: digest-preserving, `telemetry_probe_rows`
  dropped from 10,000,000 to 199,713 and
  `shuffle_bytes_published` dropped from 164,133,120 to 4,878,464
  when both Toron runtime filters and mock Wendelon filtering were
  enabled.
- `distributed_inner_equi_join` full Phase 0 100K x 100M
  selective mock-Wendelon row: digest-preserving,
  `telemetry_probe_rows` dropped from 100,000,000 to 1,997,130
  and `shuffle_bytes_published` dropped from 1,626,633,088 to
  34,086,656 when both Toron runtime filters and mock Wendelon
  filtering were enabled.
- `git diff --check`: clean.
- `samply record`: blocked by host kernel permissions:
  `/proc/sys/kernel/perf_event_paranoid` is `4`; `samply` requires
  `1` or lower for non-root Linux sampling.

## Phase 0 Calibration

V1 sizing is adaptive:

- Target false-positive rate: 1%.
- Optimal bit count: `m = ceil(-n * ln(p) / (ln 2)^2)`, approximately
  `9.59 * n` for `p = 0.01`.
- Hash count: `k = round((m/n) * ln 2)`, capped by the effective
  bit budget so the 16 KiB minimum does not inflate CPU cost for
  tiny build sets.
- Minimum serialized payload: 16 KiB.
- Maximum serialized payload: 1 MiB.

Unit evidence:

- `runtime_filter_generation_no_false_negatives` confirms 16 KiB
  minimum sizing and `hash_count = 7` for a small Int32 build set.
- `runtime_filter_generation_eligibility_excludes_floats`
  confirms float keys are ineligible.
- `runtime_filter_generation_eligibility_excludes_composites`
  confirms composite keys are ineligible.
- `runtime_filter_generation_preserves_type_tag` confirms Int32
  and Int64 key encodings do not collide.
- `runtime_filter_generation_fp_rate_within_target` confirms a
  deterministic fixture stays within the 1.5% noise bound.

Full Phase 0 selective mock-Wendelon evidence:

- Build rows: 100,000.
- Probe rows: 100,000,000.
- Probe key domain: 10,000,000.
- True match rows: 1,000,000.
- Filtered probe rows with mock Wendelon application: 1,997,130.
- Implied false positives: 997,130 out of 99,000,000 non-matching
  probe rows, approximately 1.01%.

The full 100M-row probe fixture from the Phase 0 design was run
with Toron's mock Wendelon application path. Production Wendelon
scan-engine application remains a separate follow-up.

## Quick Selective Mock-Wendelon Bench

Command:

```text
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only --strategy=hash --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled --probe-key-domain=32768
```

Rows:

```text
adr004_distributed_inner_equi_join,quick=true,strategy=hash_partitioned,key=single,runtime_filter=enabled,wendelon_runtime_filter=enabled,workers=2,build_rows=2048,probe_rows=8192,probe_key_domain=32768,stages=3,tasks=4,shuffle_bytes_published=68640,broadcast_replicated_bytes=0,pulled_shuffle_bytes=68640,left_shuffle_bytes=34320,right_shuffle_bytes=34320,published_shuffle_bytes=68640,task_wall_ms=1.544,shuffle_pull_decode_ms=0.441,build_ms=0.108,probe_ms=0.046,spill_ms=0.000,output_publish_ms=0.048,task_setup_ms=0.078,batch_encode_ms=0.081,batch_send_ms=0.178,batch_decode_ms=0.104,dispatch_wait_ms=2.233,telemetry_lock_ms=0.000,framework_overhead_ms=0.564,telemetry_build_rows=2048,telemetry_probe_rows=2048,matched_rows=2048,emitted_rows=6144,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=2048,digest=32d5b74f9e632c419de20a2baed0e2b23a99c2428462a769c907385ace9dee1b,wall_ms=2.584
adr004_distributed_inner_equi_join,quick=true,strategy=hash_partitioned,key=single,runtime_filter=enabled,wendelon_runtime_filter=disabled,workers=2,build_rows=2048,probe_rows=8192,probe_key_domain=32768,stages=3,tasks=4,shuffle_bytes_published=168480,broadcast_replicated_bytes=0,pulled_shuffle_bytes=168480,left_shuffle_bytes=34320,right_shuffle_bytes=134160,published_shuffle_bytes=168480,task_wall_ms=1.607,shuffle_pull_decode_ms=0.492,build_ms=0.063,probe_ms=0.128,spill_ms=0.000,output_publish_ms=0.101,task_setup_ms=0.053,batch_encode_ms=0.135,batch_send_ms=0.174,batch_decode_ms=0.070,dispatch_wait_ms=2.121,telemetry_lock_ms=0.000,framework_overhead_ms=0.461,telemetry_build_rows=2048,telemetry_probe_rows=8192,matched_rows=2048,emitted_rows=12288,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=2048,digest=32d5b74f9e632c419de20a2baed0e2b23a99c2428462a769c907385ace9dee1b,wall_ms=2.365
adr004_distributed_inner_equi_join,quick=true,strategy=hash_partitioned,key=single,runtime_filter=disabled,wendelon_runtime_filter=enabled,workers=2,build_rows=2048,probe_rows=8192,probe_key_domain=32768,stages=3,tasks=4,shuffle_bytes_published=168480,broadcast_replicated_bytes=0,pulled_shuffle_bytes=168480,left_shuffle_bytes=34320,right_shuffle_bytes=134160,published_shuffle_bytes=168480,task_wall_ms=1.739,shuffle_pull_decode_ms=0.614,build_ms=0.071,probe_ms=0.134,spill_ms=0.000,output_publish_ms=0.093,task_setup_ms=0.066,batch_encode_ms=0.090,batch_send_ms=0.199,batch_decode_ms=0.063,dispatch_wait_ms=2.318,telemetry_lock_ms=0.000,framework_overhead_ms=0.472,telemetry_build_rows=2048,telemetry_probe_rows=8192,matched_rows=2048,emitted_rows=12288,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=2048,digest=32d5b74f9e632c419de20a2baed0e2b23a99c2428462a769c907385ace9dee1b,wall_ms=2.351
adr004_distributed_inner_equi_join,quick=true,strategy=hash_partitioned,key=single,runtime_filter=disabled,wendelon_runtime_filter=disabled,workers=2,build_rows=2048,probe_rows=8192,probe_key_domain=32768,stages=3,tasks=4,shuffle_bytes_published=168480,broadcast_replicated_bytes=0,pulled_shuffle_bytes=168480,left_shuffle_bytes=34320,right_shuffle_bytes=134160,published_shuffle_bytes=168480,task_wall_ms=1.460,shuffle_pull_decode_ms=0.422,build_ms=0.054,probe_ms=0.121,spill_ms=0.000,output_publish_ms=0.101,task_setup_ms=0.050,batch_encode_ms=0.106,batch_send_ms=0.183,batch_decode_ms=0.059,dispatch_wait_ms=2.056,telemetry_lock_ms=0.000,framework_overhead_ms=0.423,telemetry_build_rows=2048,telemetry_probe_rows=8192,matched_rows=2048,emitted_rows=12288,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=2048,digest=32d5b74f9e632c419de20a2baed0e2b23a99c2428462a769c907385ace9dee1b,wall_ms=2.073
```

Readout:

- All four mode combinations produce the same digest:
  `32d5b74f9e632c419de20a2baed0e2b23a99c2428462a769c907385ace9dee1b`.
- With Toron runtime filters enabled and mock Wendelon filtering
  enabled, `telemetry_probe_rows` drops from 8192 to 2048.
- `shuffle_bytes_published` drops from 168480 to 68640, a 59.3%
  reduction on this quick selective fixture.
- With mock Wendelon filtering disabled, bytes remain unchanged.
  This is expected for the Wendelon-disabled path.

## Promotion-Scale Wendelon-Disabled Rows

Command family:

```text
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=disabled
```

Digest for every row:
`2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`.

| workers | runtime filter | shuffle bytes | build_ms | bytes cluster ms | wall_ms |
|---:|---|---:|---:|---:|---:|
| 2 | enabled | 178,752,032 | 673.282 | 1,073.903 | 2,720.792 |
| 2 | disabled | 178,752,032 | 649.091 | 1,047.521 | 2,325.867 |
| 4 | enabled | 178,753,984 | 583.770 | 790.746 | 2,221.831 |
| 4 | disabled | 178,753,984 | 585.690 | 847.895 | 1,996.426 |
| 8 | enabled | 178,758,272 | 546.053 | 801.769 | 2,240.622 |
| 8 | disabled | 178,758,272 | 539.178 | 699.797 | 1,801.679 |

Readout:

- The Wendelon-disabled gate passes: filter-enabled and
  filter-disabled rows are digest-identical at 2/4/8 workers.
- The ADR 005 canonical digest is preserved exactly.
- The 8-worker non-selective guard uses ADR 005's 8-worker
  `build_ms = 542.787` as baseline. Filter-enabled `build_ms`
  is `546.053`, a +0.6% delta, within the 10% Toron-side
  promotion guard.
- The cap-bound Bloom builder was optimized after an initial
  audit run exposed hidden coordinator wall time from building
  a 10M-entry distinct-key set. Cap-bound generation now sizes
  from non-null row count and streams inserts instead of storing
  all distinct keys.

## Larger Selective Mock-Wendelon Row

Command:

```text
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=8 --build-rows=100000 --probe-rows=10000000 --probe-key-domain=10000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled
```

| runtime filter | mock Wendelon | shuffle bytes | probe rows | bytes cluster ms | digest |
|---|---|---:|---:|---:|---|
| enabled | enabled | 4,878,464 | 199,713 | 13.574 | `12c9c73fa3420689c99785639bee71b8ea9c13bee3cb2d184d82a9d69c9bba9e` |
| enabled | disabled | 164,133,120 | 10,000,000 | 717.597 | `12c9c73fa3420689c99785639bee71b8ea9c13bee3cb2d184d82a9d69c9bba9e` |
| disabled | enabled | 164,133,120 | 10,000,000 | 527.493 | `12c9c73fa3420689c99785639bee71b8ea9c13bee3cb2d184d82a9d69c9bba9e` |
| disabled | disabled | 164,133,120 | 10,000,000 | 575.000 | `12c9c73fa3420689c99785639bee71b8ea9c13bee3cb2d184d82a9d69c9bba9e` |

Readout:

- All four mode combinations are digest-identical.
- Mock-Wendelon row rejection reduces `shuffle_bytes_published`
  by 97.0% versus filter-disabled / Wendelon-disabled.
- The bytes-movement cluster drops from 575.000 ms to 13.574 ms,
  a 97.6% reduction on this 100K x 10M selective fixture.
- This is Toron harness evidence only. Production Wendelon-side
  row-level application still needs its own follow-up evidence.

## Full Phase 0 Selective Mock-Wendelon Row

Command:

```text
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=8 --build-rows=100000 --probe-rows=100000000 --probe-key-domain=10000000 --runtime-filter=enabled,disabled --wendelon-runtime-filter=enabled,disabled
```

Rows:

```text
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,runtime_filter=enabled,wendelon_runtime_filter=enabled,workers=8,build_rows=100000,probe_rows=100000000,probe_key_domain=10000000,stages=3,tasks=10,shuffle_bytes_published=34086656,broadcast_replicated_bytes=0,pulled_shuffle_bytes=34086656,left_shuffle_bytes=1629248,right_shuffle_bytes=32457408,published_shuffle_bytes=34086656,task_wall_ms=3230.307,shuffle_pull_decode_ms=29.240,build_ms=1.794,probe_ms=33.243,spill_ms=0.000,output_publish_ms=21.262,task_setup_ms=0.157,batch_encode_ms=17.485,batch_send_ms=37.085,batch_decode_ms=14.179,dispatch_wait_ms=3251.999,telemetry_lock_ms=0.000,framework_overhead_ms=3090.041,telemetry_build_rows=100000,telemetry_probe_rows=1997130,matched_rows=1000000,emitted_rows=3097130,emitted_batches=24,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=d3e2b85525c72a8e7125221d53777bb9829c05f68999c2c1aac4e97ba316cd3f,wall_ms=3268.413
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,runtime_filter=enabled,wendelon_runtime_filter=disabled,workers=8,build_rows=100000,probe_rows=100000000,probe_key_domain=10000000,stages=3,tasks=10,shuffle_bytes_published=1626633088,broadcast_replicated_bytes=0,pulled_shuffle_bytes=1626633088,left_shuffle_bytes=1629248,right_shuffle_bytes=1625003840,published_shuffle_bytes=1626633088,task_wall_ms=14600.682,shuffle_pull_decode_ms=3900.126,build_ms=1.889,probe_ms=1510.992,spill_ms=0.000,output_publish_ms=1327.681,task_setup_ms=0.199,batch_encode_ms=1024.932,batch_send_ms=3354.116,batch_decode_ms=550.210,dispatch_wait_ms=14888.878,telemetry_lock_ms=0.000,framework_overhead_ms=3480.747,telemetry_build_rows=100000,telemetry_probe_rows=100000000,matched_rows=1000000,emitted_rows=101100000,emitted_batches=24,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=d3e2b85525c72a8e7125221d53777bb9829c05f68999c2c1aac4e97ba316cd3f,wall_ms=14956.443
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,runtime_filter=disabled,wendelon_runtime_filter=enabled,workers=8,build_rows=100000,probe_rows=100000000,probe_key_domain=10000000,stages=3,tasks=10,shuffle_bytes_published=1626633088,broadcast_replicated_bytes=0,pulled_shuffle_bytes=1626633088,left_shuffle_bytes=1629248,right_shuffle_bytes=1625003840,published_shuffle_bytes=1626633088,task_wall_ms=14599.974,shuffle_pull_decode_ms=3944.619,build_ms=1.834,probe_ms=1526.512,spill_ms=0.000,output_publish_ms=1322.632,task_setup_ms=0.195,batch_encode_ms=1002.739,batch_send_ms=3345.164,batch_decode_ms=549.554,dispatch_wait_ms=14890.085,telemetry_lock_ms=0.000,framework_overhead_ms=3456.279,telemetry_build_rows=100000,telemetry_probe_rows=100000000,matched_rows=1000000,emitted_rows=101100000,emitted_batches=24,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=d3e2b85525c72a8e7125221d53777bb9829c05f68999c2c1aac4e97ba316cd3f,wall_ms=14944.374
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,runtime_filter=disabled,wendelon_runtime_filter=disabled,workers=8,build_rows=100000,probe_rows=100000000,probe_key_domain=10000000,stages=3,tasks=10,shuffle_bytes_published=1626633088,broadcast_replicated_bytes=0,pulled_shuffle_bytes=1626633088,left_shuffle_bytes=1629248,right_shuffle_bytes=1625003840,published_shuffle_bytes=1626633088,task_wall_ms=14741.962,shuffle_pull_decode_ms=4028.066,build_ms=1.823,probe_ms=1519.544,spill_ms=0.000,output_publish_ms=1321.260,task_setup_ms=0.202,batch_encode_ms=1024.903,batch_send_ms=3345.086,batch_decode_ms=551.608,dispatch_wait_ms=15032.407,telemetry_lock_ms=0.000,framework_overhead_ms=3501.078,telemetry_build_rows=100000,telemetry_probe_rows=100000000,matched_rows=1000000,emitted_rows=101100000,emitted_batches=24,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=d3e2b85525c72a8e7125221d53777bb9829c05f68999c2c1aac4e97ba316cd3f,wall_ms=15087.404
```

Readout:

- All four mode combinations are digest-identical:
  `d3e2b85525c72a8e7125221d53777bb9829c05f68999c2c1aac4e97ba316cd3f`.
- Mock-Wendelon row rejection reduces `shuffle_bytes_published`
  by 97.9% versus filter-disabled / Wendelon-disabled.
- `telemetry_probe_rows` drops by 98.0%, from 100,000,000 to
  1,997,130.
- The bytes-movement cluster (`batch_send` +
  `shuffle_pull_decode` + `output_publish` + `batch_encode`)
  drops from 9,719.315 ms to 105.072 ms, a 98.9% reduction.
- Wall time drops from 15,087.404 ms to 3,268.413 ms, a 78.3%
  reduction in this local harness.
- Implied false-positive rate is 997,130 / 99,000,000 =
  approximately 1.01%, matching the Phase 0 calibration target.
- This is Toron harness evidence only. Production Wendelon-side
  row-level application still needs its own follow-up evidence.

## Required Production Rows

The following evidence is still required before claiming production
Wendelon-enabled completion:

- Production Wendelon scan-engine row-level application.
- Production Wendelon-enabled selective row, filter-enabled and
  filter-disabled.
- Profiler note. `samply` was attempted, but this host blocks
  non-root sampling with `/proc/sys/kernel/perf_event_paranoid = 4`;
  no profile was recorded.

## Conditional Readout

Current result is **Toron-side implementation complete with
Toron-side promotion-bar evidence captured and full Phase 0
selective mock-Wendelon evidence captured; production
Wendelon-enabled selective evidence remains incomplete**.

The missing evidence is not a correctness blocker for the Toron
contract. It is the production Wendelon-side application package
and optional exact-scale selective benchmark package.
