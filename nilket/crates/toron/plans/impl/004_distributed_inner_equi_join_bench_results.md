# ADR 004 Distributed Inner Equi-Join Bench Results

Status: promotion-scale bench evidence recorded. Hard-gate rows passed;
soft scaling target missed and is recorded below.

Date: 2026-05-13

## Final Verification

- `cargo test --workspace` passed.
- `cargo test -p toron` passed with 137 lib tests, 22
  architecture tests, 5 distributed process tests, 5 Wendelon
  storage execution tests, and doc tests.
- `cargo bench -p toron --bench distributed_inner_equi_join
  --no-run` passed.
- `cargo fmt -p toron --check` passed.
- `git diff --check` passed.

## Phase 0 Audit

- **P0.1 Fragmenter audit.** Implemented hash-partitioned
  three-stage DAG and broadcast two-stage DAG. ADR 003
  single-exchange GROUP BY remains green in `cargo test -p
  toron`.
- **P0.2 Broadcast publish operator decision.** Reused
  `Exchange { partition_count: 1 }` for broadcast publish.
- **P0.3 M10 hash-spill substrate audit.** ADR 004 reuses the
  M10 AES-256-GCM-SIV spill substrate; spill engagement and
  spill exhaustion tests pass in `cargo test -p toron`.
- **P0.4 Scheduler audit.** `StagePlan::upstream_stages:
  Vec<StageId>` is implemented and architecture-guarded by
  G18.
- **P0.5 `Expr::Column` refactor scope.** Logical column
  references use `ColumnRef`; SQL planner, binder, Rune, tests,
  and benches compile against the refactored surface.
- **P0.6 Broadcast probe scan-share rule.** Implemented
  `xxh3_64(tablet_id) mod worker_count == worker_index`;
  fragmenter and architecture tests cover the `ScanShare`
  shape.
- **P0.7 Fixture matrix.** Promotion-scale hash, broadcast, and
  composite-key rows are recorded below. NULL semantics,
  duplicate output names, SIGKILL replay, over-cap broadcast,
  spill, and determinism coverage live in `cargo test -p
  toron`.
- **P0.8 Architecture guards.** G16-G18 plus the broadcast
  input-shape and spill-substrate guards pass in
  `tests/architecture_test.rs`.
- **P0.9 Soft-gate thresholds.** Worker-count scaling target,
  broadcast caps, memory/spill defaults, and spill page/cache
  defaults are pinned in the ADR/impl pair; the scaling target
  missed on this local run and is recorded under Soft Gates.
- **P0.10 Bench-row schema.** Rows record strategy, key shape,
  worker count, input/output row counts, stage/task counts,
  shuffle bytes, broadcast replicated bytes, task-total row
  counters, executor phase timings, framework sub-attribution,
  scheduler residual counters, digest, and wall time.
  `framework_overhead_ms` is preserved as the residual after
  named in-task phases are accounted. Hash peak memory, spill
  cache hit rate, and skew partition counts are still
  observation-only soft-gate gaps.
- **P0.11 Runtime telemetry.** Stacked into ADR 004 while the
  promotion work is still staged. The four ADR 003 residual
  counters (pulled shuffle bytes, retry count, stale rejection
  count, cancellation cleanup count) plus executor/join phase
  spans are now emitted by task completions and aggregated in
  the bench harness.

## Commands

```text
cargo bench -p toron --bench distributed_inner_equi_join --no-run
cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=8 --build-rows=10000000 --probe-rows=10000000
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash,broadcast --workers=8 --build-rows=1000000 --probe-rows=10000000
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=4 --build-rows=10000000 --probe-rows=1000000 --composite-key
```

## Quick Smoke Rows

```text
adr004_distributed_inner_equi_join,quick=true,strategy=hash_partitioned,key=single,workers=2,build_rows=2048,probe_rows=8192,stages=3,tasks=4,shuffle_bytes_published=168352,broadcast_replicated_bytes=0,pulled_shuffle_bytes=168352,left_shuffle_bytes=34320,right_shuffle_bytes=134032,published_shuffle_bytes=168352,task_wall_ms=4.041,shuffle_pull_decode_ms=1.038,build_ms=0.418,probe_ms=0.567,spill_ms=0.000,output_publish_ms=0.167,task_setup_ms=0.113,batch_encode_ms=0.392,batch_send_ms=0.470,batch_decode_ms=0.231,dispatch_wait_ms=5.164,telemetry_lock_ms=0.000,framework_overhead_ms=0.876,telemetry_build_rows=2048,telemetry_probe_rows=8192,matched_rows=8192,emitted_rows=18432,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=8192,digest=0d96a56d48ff397ece0003ef5cb2a6d45782bb9f34221be544b15ad77ed4ce3b,wall_ms=5.200
adr004_distributed_inner_equi_join,quick=true,strategy=broadcast,key=single,workers=2,build_rows=2048,probe_rows=8192,stages=2,tasks=3,shuffle_bytes_published=33672,broadcast_replicated_bytes=67344,pulled_shuffle_bytes=67344,left_shuffle_bytes=67344,right_shuffle_bytes=0,published_shuffle_bytes=33672,task_wall_ms=2.430,shuffle_pull_decode_ms=0.327,build_ms=0.733,probe_ms=0.528,spill_ms=0.000,output_publish_ms=0.016,task_setup_ms=0.055,batch_encode_ms=0.154,batch_send_ms=0.233,batch_decode_ms=0.022,dispatch_wait_ms=2.854,telemetry_lock_ms=0.000,framework_overhead_ms=0.384,telemetry_build_rows=4096,telemetry_probe_rows=8192,matched_rows=8192,emitted_rows=10240,emitted_batches=3,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=8192,digest=0d96a56d48ff397ece0003ef5cb2a6d45782bb9f34221be544b15ad77ed4ce3b,wall_ms=2.866
```

## Promotion-Scale Rows

These rows are the promotion-scale correctness and scaling
evidence recorded before the telemetry stack was added. The
quick smoke rows above show the current telemetry schema; the
ADR 005 attribution rerun should regenerate the 10M-scale rows
with the same phase columns.

```text
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=2,build_rows=10000000,probe_rows=1000000,stages=3,tasks=4,shuffle_bytes_published=178752032,broadcast_replicated_bytes=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=6975.847
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=4,build_rows=10000000,probe_rows=1000000,stages=3,tasks=6,shuffle_bytes_published=178753984,broadcast_replicated_bytes=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=5963.102
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=8,build_rows=10000000,probe_rows=1000000,stages=3,tasks=10,shuffle_bytes_published=178758272,broadcast_replicated_bytes=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=5141.824
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=8,build_rows=10000000,probe_rows=10000000,stages=3,tasks=10,shuffle_bytes_published=325007744,broadcast_replicated_bytes=0,result_rows=10000000,digest=50553056941ef6af41cd824d55426863996747340f11095adb30ccb1530df2ad,wall_ms=8637.664
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=8,build_rows=1000000,probe_rows=10000000,stages=3,tasks=10,shuffle_bytes_published=178758528,broadcast_replicated_bytes=0,result_rows=10000000,digest=b2700b265437181cdc020f1ef095ab5a422c19325758d3cda6bdef0ef63ae7e8,wall_ms=2978.834
adr004_distributed_inner_equi_join,quick=false,strategy=broadcast,key=single,workers=8,build_rows=1000000,probe_rows=10000000,stages=2,tasks=9,shuffle_bytes_published=16250504,broadcast_replicated_bytes=130004032,result_rows=10000000,digest=b2700b265437181cdc020f1ef095ab5a422c19325758d3cda6bdef0ef63ae7e8,wall_ms=6465.483
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=composite,workers=4,build_rows=10000000,probe_rows=1000000,stages=3,tasks=6,shuffle_bytes_published=268131200,broadcast_replicated_bytes=0,result_rows=1000000,digest=5cfc93d2eff99e6010c36675b5388a7571dcb82ec85b80b562000b4821233f51,wall_ms=9469.548
```

## Gate Notes

- Hash-partitioned correctness gate: 10M x 1M at 4 workers
  produced the same digest as the 2- and 8-worker runs.
- 10M x 10M executor evidence row completed at 8 workers in
  8637.664 ms with 10M result rows.
- Composite-key gate: 10M x 1M on `(region, id) =
  (region, customer_id)` completed at 4 workers with 1M
  result rows.
- Broadcast under-cap correctness gate: the 1M x 10M hash
  and broadcast rows at 8 workers produced matching digest
  `b2700b265437181cdc020f1ef095ab5a422c19325758d3cda6bdef0ef63ae7e8`.
- Broadcast replicated bytes are recorded on the broadcast
  row (`130004032` bytes). Hash shuffle bytes are recorded per
  row.
- SIGKILL replay evidence lives in
  `crates/toron/tests/distributed_process_test.rs`:
  hash left/right producers, hash join worker, and broadcast
  build-publish replay all pass with byte-identical digests.
- Broadcast over-cap terminal failure evidence lives in the
  distributed TCP test and server cap-validation tests.
- Pull-addressed shuffle and no-inline broadcast payloads are
  covered by architecture tests plus distributed TCP tests.
- SQL planner, binder, NULL semantics, duplicate output names,
  spill engagement/exhaustion, and architecture guards are
  covered by `cargo test -p toron`.

## Repeat Determinism Rows

```text
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=4,build_rows=10000000,probe_rows=1000000,stages=3,tasks=6,shuffle_bytes_published=178753984,broadcast_replicated_bytes=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=6565.711
adr004_distributed_inner_equi_join,quick=false,strategy=broadcast,key=single,workers=8,build_rows=1000000,probe_rows=10000000,stages=2,tasks=9,shuffle_bytes_published=16250504,broadcast_replicated_bytes=130004032,result_rows=10000000,digest=b2700b265437181cdc020f1ef095ab5a422c19325758d3cda6bdef0ef63ae7e8,wall_ms=6727.584
```

The repeat hash row matches the 10M x 1M 4-worker digest in
the promotion matrix. The repeat broadcast row matches the
1M x 10M broadcast digest in the promotion matrix. SIGKILL
determinism remains process-test evidence rather than a bench
mode.

## Post-Promotion Telemetry Rerun

Command:

```text
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000
```

Rows:

```text
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=2,build_rows=10000000,probe_rows=1000000,stages=3,tasks=4,shuffle_bytes_published=178752032,broadcast_replicated_bytes=0,pulled_shuffle_bytes=178752032,left_shuffle_bytes=162501008,right_shuffle_bytes=16251024,published_shuffle_bytes=178752032,task_wall_ms=6920.416,shuffle_pull_decode_ms=414.586,build_ms=3316.375,probe_ms=209.738,spill_ms=0.000,output_publish_ms=147.118,framework_overhead_ms=2832.599,telemetry_build_rows=10000000,telemetry_probe_rows=1000000,matched_rows=1000000,emitted_rows=12000000,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=7041.867
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=4,build_rows=10000000,probe_rows=1000000,stages=3,tasks=6,shuffle_bytes_published=178753984,broadcast_replicated_bytes=0,pulled_shuffle_bytes=178753984,left_shuffle_bytes=162502048,right_shuffle_bytes=16251936,published_shuffle_bytes=178753984,task_wall_ms=6008.799,shuffle_pull_decode_ms=235.152,build_ms=2809.858,probe_ms=176.177,spill_ms=0.000,output_publish_ms=160.600,framework_overhead_ms=2627.012,telemetry_build_rows=10000000,telemetry_probe_rows=1000000,matched_rows=1000000,emitted_rows=12000000,emitted_batches=12,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=6080.771
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=8,build_rows=10000000,probe_rows=1000000,stages=3,tasks=10,shuffle_bytes_published=178758272,broadcast_replicated_bytes=0,pulled_shuffle_bytes=178758272,left_shuffle_bytes=162503872,right_shuffle_bytes=16254400,published_shuffle_bytes=178758272,task_wall_ms=5382.285,shuffle_pull_decode_ms=109.647,build_ms=2531.824,probe_ms=163.229,spill_ms=0.000,output_publish_ms=205.791,framework_overhead_ms=2371.794,telemetry_build_rows=10000000,telemetry_probe_rows=1000000,matched_rows=1000000,emitted_rows=12000000,emitted_batches=24,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=5414.078
```

The rerun preserves the 10M x 1M digest from the promotion
matrix across 2/4/8 workers. Wall-clock speedup remains
sub-target: 1.16x from 2 to 4 workers and 1.12x from 4 to 8
workers. Task-total attribution points at build work plus
framework overhead, not shuffle pull, spill, or output publish:
at 8 workers, build is 2531.824 ms and framework overhead is
2371.794 ms of 5382.285 ms task-total wall time. Shuffle
pull/decode drops from 414.586 ms at 2 workers to 109.647 ms
at 8 workers; spill stays at 0 bytes; retry, stale rejection,
and cancellation cleanup counters stay zero in this
non-failure bench path.

## Framework Overhead Sub-Attribution Rerun

Command:

```text
cargo bench -p toron --bench distributed_inner_equi_join -- --distributed-inner-equi-join-only --strategy=hash --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000
```

Rows:

```text
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=2,build_rows=10000000,probe_rows=1000000,stages=3,tasks=4,shuffle_bytes_published=178752032,broadcast_replicated_bytes=0,pulled_shuffle_bytes=178752032,left_shuffle_bytes=162501008,right_shuffle_bytes=16251024,published_shuffle_bytes=178752032,task_wall_ms=6818.307,shuffle_pull_decode_ms=418.884,build_ms=3199.407,probe_ms=207.333,spill_ms=0.000,output_publish_ms=146.111,task_setup_ms=0.126,batch_encode_ms=122.410,batch_send_ms=353.770,batch_decode_ms=75.123,dispatch_wait_ms=6935.714,telemetry_lock_ms=0.000,framework_overhead_ms=2370.266,telemetry_build_rows=10000000,telemetry_probe_rows=1000000,matched_rows=1000000,emitted_rows=12000000,emitted_batches=6,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=6944.653
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=4,build_rows=10000000,probe_rows=1000000,stages=3,tasks=6,shuffle_bytes_published=178753984,broadcast_replicated_bytes=0,pulled_shuffle_bytes=178753984,left_shuffle_bytes=162502048,right_shuffle_bytes=16251936,published_shuffle_bytes=178753984,task_wall_ms=6145.772,shuffle_pull_decode_ms=223.555,build_ms=2916.464,probe_ms=179.779,spill_ms=0.000,output_publish_ms=142.665,task_setup_ms=0.123,batch_encode_ms=116.309,batch_send_ms=342.274,batch_decode_ms=62.808,dispatch_wait_ms=6219.337,telemetry_lock_ms=0.000,framework_overhead_ms=2224.603,telemetry_build_rows=10000000,telemetry_probe_rows=1000000,matched_rows=1000000,emitted_rows=12000000,emitted_batches=12,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=6225.153
adr004_distributed_inner_equi_join,quick=false,strategy=hash_partitioned,key=single,workers=8,build_rows=10000000,probe_rows=1000000,stages=3,tasks=10,shuffle_bytes_published=178758272,broadcast_replicated_bytes=0,pulled_shuffle_bytes=178758272,left_shuffle_bytes=162503872,right_shuffle_bytes=16254400,published_shuffle_bytes=178758272,task_wall_ms=5416.577,shuffle_pull_decode_ms=132.865,build_ms=2581.550,probe_ms=162.982,spill_ms=0.000,output_publish_ms=201.890,task_setup_ms=0.177,batch_encode_ms=114.017,batch_send_ms=180.547,batch_decode_ms=55.647,dispatch_wait_ms=5451.250,telemetry_lock_ms=0.000,framework_overhead_ms=2042.549,telemetry_build_rows=10000000,telemetry_probe_rows=1000000,matched_rows=1000000,emitted_rows=12000000,emitted_batches=24,spill_bytes=0,retry_count=0,stale_rejection_count=0,cancellation_cleanup_count=0,result_rows=1000000,digest=2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289,wall_ms=5451.372
```

This rerun preserves the same 10M x 1M digest across 2/4/8
workers. The split keeps `framework_overhead_ms` as the
in-task residual after setup, batch encode, batch send, and
telemetry lock time are accounted. On the 8-worker row,
batch encode + send accounts for 294.564 ms and telemetry
lock is 0.000 ms, while residual framework overhead remains
2042.549 ms. Coordinator-side `dispatch_wait_ms` tracks wall
time observed around remote task dispatch and therefore is
reported separately rather than subtracted from worker
`task_wall_ms`; `batch_decode_ms` is also coordinator-side.
The sub-attribution does not make batch encode/decode/send or
telemetry locks dominant. ADR 005 should treat hash-build
work and remaining worker residual/runtime plumbing as the
active candidates; streaming probe/output remains unsupported
by this evidence. A p99 is not recorded for this sweep because
2/4/8 workers only produce 4/6/10 tasks, making p99
effectively a max-task statistic rather than a stable tail
latency signal.

## Soft Gates

- Worker scaling target missed on this local run. 10M x 1M
  hash wall-clock: 2 workers 6975.847 ms, 4 workers
  5963.102 ms, 8 workers 5141.824 ms. Speedup is 1.17x
  from 2 to 4 workers and 1.16x from 4 to 8 workers, below
  the target of at least 1.5x per doubling.
- Per-side shuffle bytes, task-total build/probe rows,
  matched rows, emitted rows, spill bytes, executor phase
  timings, and framework sub-attribution columns are emitted
  by the current bench harness.
- Hash peak memory and spill cache hit rate were not emitted
  by this bench harness.
- Skew observation was not emitted as a bench row; skew remains
  observation-only for V1.
- Retry count, stale rejection count, and cancellation cleanup
  count are emitted as scheduler residual counters. They are
  zero in the non-failure bench rows; failure behavior remains
  covered by tests.
- ADR 003 single-exchange GROUP BY stayed regression-free under
  `cargo test -p toron`.
- Executor-model signal for ADR 005: the 10M x 10M row
  completes, but scaling is sub-target even on the 10M x 1M
  fixture. This points ADR 005 toward executor model /
  scheduling overhead investigation before adding more join
  algorithms.
