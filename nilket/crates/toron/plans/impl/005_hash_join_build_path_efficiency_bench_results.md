# ADR 005 Hash-Join Build Path Efficiency Bench Results

Date: 2026-05-13

## Phase 0 Calibration

Profiler tooling:

- `perf stat ...` attempted before implementation and failed because
  `/proc/sys/kernel/perf_event_paranoid` is `4`.
- `samply 0.13.1` was installed during the implementation turn, but
  `samply record -s -o /tmp/toron_adr005_samply_smoke.json.gz true`
  failed with the same kernel permission gate.
- A non-perf stack-sampling fallback was checked: `gdb` is present,
  but attach is blocked by the container's ptrace/Yama policy.
  `strace` is present but only provides syscall tracing, not CPU hot
  path samples, so it was not used as a profiling substitute.
- No flamegraph/profile file was captured in this container.

Machine note: local Linux Codex workspace container. The profiler
permission gate prevented sampling; implementation order stayed on the
ADR-selected three items: typed fixed-width key path, inline-first row
lists with map pre-sizing, and typed build/probe loops. The private
layout choice for ADR 005 is `std::collections::HashMap` with the new
operator-local `JoinKey` and `BuildRowList` representation, leaving spill
on canonical bytes.

## Baseline

ADR 004 post-promotion baseline, 10M build rows x 1M probe rows,
hash-partitioned inner join:

| Workers | build_ms | framework_overhead_ms | wall_ms |
|---:|---:|---:|---:|
| 2 | 3199.407 | 2370.266 | 6944.653 |
| 4 | 2916.464 | 2224.603 | 6225.153 |
| 8 | 2581.550 | 2042.549 | 5451.372 |

Baseline digest:
`2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289`.

## ADR 005 Rerun

Command:

```text
cargo bench -p toron --bench distributed_inner_equi_join \
    -- --distributed-inner-equi-join-only --strategy=hash \
    --workers=2,4,8 --build-rows=10000000 --probe-rows=1000000
```

| Workers | task_wall_ms | shuffle_pull_decode_ms | build_ms | probe_ms | spill_ms | output_publish_ms | batch_encode_ms | batch_send_ms | framework_overhead_ms | wall_ms | Digest |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 2304.630 | 455.291 | 675.014 | 98.152 | 0.000 | 151.783 | 139.294 | 345.356 | 439.642 | 2436.903 | `2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289` |
| 4 | 1931.438 | 210.518 | 578.328 | 91.224 | 0.000 | 143.041 | 125.872 | 351.195 | 431.103 | 2000.714 | `2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289` |
| 8 | 1614.107 | 156.441 | 542.787 | 80.191 | 0.000 | 143.318 | 73.630 | 185.123 | 432.429 | 1637.966 | `2fe6c8d80c88662cb326bc5c840c25f7abbe0aa1c0fbaf0d52e1f21468e81289` |

All rows reported:

- `telemetry_build_rows=10000000`
- `telemetry_probe_rows=1000000`
- `matched_rows=1000000`
- `result_rows=1000000`
- `spill_bytes=0`
- `retry_count=0`
- `stale_rejection_count=0`
- `cancellation_cleanup_count=0`

## Deltas

| Workers | build_ms delta | build_ms reduction | framework_overhead_ms reduction | wall_ms reduction | baseline build rows/sec | ADR 005 build rows/sec |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | -2524.393 | 78.9% | 81.5% | 64.9% | 3,125,579 | 14,814,508 |
| 4 | -2338.136 | 80.2% | 80.6% | 67.9% | 3,428,810 | 17,291,226 |
| 8 | -2038.763 | 79.0% | 78.8% | 70.0% | 3,873,642 | 18,423,433 |

8-worker target:

- Required: `build_ms <= 1807.085 ms`.
- Actual: `542.787 ms`.
- Result: hit, `79.0%` reduction versus ADR 004 baseline.

Digest comparison: all ADR 005 rerun rows match the ADR 004 canonical
digest byte-identically.

## ADR 006 Readout

At 8 workers, `build_ms` fell `79.0%` and `framework_overhead_ms` fell
`78.8%`. The residual drop is effectively proportional to the build
drop, well above the ADR 005 `>=50%` operator-adjacent threshold.

Conditional ADR 006 band: **operator-adjacent**. ADR 006 does not need
to promote a runtime-plumbing investigation from this evidence.
