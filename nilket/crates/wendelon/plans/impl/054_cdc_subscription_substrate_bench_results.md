# ADR 054 CDC Subscription Substrate Bench Results

This file records the ADR 054 bench evidence. The entries below
exercise every named bench-matrix category with synthetic
in-process measurements, but they are not the full small /
medium / large promotion-to-shipped matrix on representative
hardware.

The harness also supports an acceptance verifier:
`--acceptance-one-way-quic-rtt-micros N`. The `N` value must be
the measured one-way QUIC RTT for the representative environment.
When supplied, the bench emits `cdc_acceptance_*` pass/fail lines
and exits nonzero if any threshold fails.

## Quick Smoke

Commands:

```sh
cargo bench --bench cdc_subscription --no-run
cargo bench --bench cdc_subscription -- --quick
```

Result:

```text
cdc_live_tail_latency rows=1024 iters=2 p50_micros=0 p95_micros=0 p99_micros=0 p99_nanos=258
cdc_live_tail_fanout subscribers=1 rows=1024 iters=2 p99_micros=0 p99_nanos=314
cdc_live_tail_fanout subscribers=10 rows=1024 iters=2 p99_micros=1 p99_nanos=1791
cdc_live_tail_fanout subscribers=100 rows=1024 iters=2 p99_micros=14 p99_nanos=14388
cdc_catchup_throughput rows=1024 iters=2 matched=1024 decoded_mib_per_sec=12308.69
cdc_ring_overflow_threshold cap_bytes=3328 rows=1024 first_overflow_at=33 evicted=992 frontier_present=true
cdc_checkpoint_cost rows=1024 iters=2 avg_micros=7309
cdc_admission_isolation rows=1024 iters=2 p99_0_subs_micros=0 p99_100_subs_micros=0 p99_0_subs_nanos=315 p99_100_subs_nanos=227 regression_percent=-27.94
cdc_schema_fingerprint_change rows=1024 iters=2 changes=16 avg_nanos=6
cdc_wire_compression rows=1024 iters=2 encoded_bytes=4616 avg_micros=20
cdc_ring_stage_finalize rows=1024 iters=2 avg_micros=4212
cdc_stage_release_drain rows=1024 iters=2 push_avg_nanos=3373 stage_avg_nanos=70 release_avg_nanos=862 drain_avg_nanos=1676
cdc_safe_emission_watermark_hold rows=1024 initial_safe_ms=50 blocked_before_heartbeat=1024 released_after_heartbeat=1024
cdc_frame_encode rows=1024 iters=2 avg_micros=21
```

## Coverage Notes

- `cdc_live_tail_latency` covers the admit -> finalization ->
  consumer receive path through the subscription registry.
- `cdc_live_tail_fanout` covers 1 / 10 / 100 subscriber fan-out
  in quick mode; the full run adds the larger fan-out case.
- `cdc_catchup_throughput` covers full-`CdcCursor` tuple window
  filtering as the synthetic catchup throughput proxy.
- `cdc_ring_overflow_threshold` verifies overflow accounting and
  the table-scoped eviction frontier used by slow-consumer
  recovery.
- `cdc_checkpoint_cost` covers visible-state digest construction
  plus checkpoint frame emit.
- `cdc_admission_isolation` compares 0 vs 100 active
  registered subscriptions in-process while timing only the
  admission-stage ring push. Fan-out cost is measured separately
  by `cdc_live_tail_fanout`.
- `cdc_schema_fingerprint_change` covers per-frame fingerprint
  compare / rebind detection cost.
- `cdc_wire_compression` confirms the CDC event path uses ADR 053
  wire-side IPC compression and records encoded bytes.
- `cdc_stage_release_drain` decomposes stage, release, and drain
  costs.
- `cdc_safe_emission_watermark_hold` exercises the skewed-locality
  watermark gate and heartbeat release shape.

## Acceptance Verifier Smoke

Command:

```sh
cargo bench --bench cdc_subscription -- --quick --acceptance-one-way-quic-rtt-micros 1000
```

Result:

```text
cdc_live_tail_latency rows=1024 iters=2 p50_micros=0 p95_micros=0 p99_micros=0 p99_nanos=602
cdc_live_tail_fanout subscribers=1 rows=1024 iters=2 p99_micros=1 p99_nanos=1090
cdc_live_tail_fanout subscribers=10 rows=1024 iters=2 p99_micros=3 p99_nanos=3792
cdc_live_tail_fanout subscribers=100 rows=1024 iters=2 p99_micros=18 p99_nanos=18495
cdc_catchup_throughput rows=1024 iters=2 matched=1024 decoded_mib_per_sec=10254.55
cdc_ring_overflow_threshold cap_bytes=3328 rows=1024 first_overflow_at=33 evicted=992 frontier_present=true
cdc_checkpoint_cost rows=1024 iters=2 avg_micros=8632
cdc_admission_isolation rows=1024 iters=2 p99_0_subs_micros=0 p99_100_subs_micros=0 p99_0_subs_nanos=437 p99_100_subs_nanos=362 regression_percent=-17.16
cdc_schema_fingerprint_change rows=1024 iters=2 changes=16 avg_nanos=12
cdc_wire_compression rows=1024 iters=2 encoded_bytes=4616 avg_micros=24
cdc_ring_stage_finalize rows=1024 iters=2 avg_micros=4180
cdc_stage_release_drain rows=1024 iters=2 push_avg_nanos=3828 stage_avg_nanos=73 release_avg_nanos=124 drain_avg_nanos=392
cdc_safe_emission_watermark_hold rows=1024 initial_safe_ms=50 blocked_before_heartbeat=1024 released_after_heartbeat=1024
cdc_frame_encode rows=1024 iters=2 avg_micros=80
cdc_acceptance_live_tail_latency rows=1024 iters=2 one_way_quic_rtt_micros=1000 threshold_nanos=5000000 observed_p99_nanos=18495 status=pass
cdc_acceptance_catchup_throughput rows=1024 iters=2 threshold_mib_per_sec=100.00 observed_mib_per_sec=10254.55 status=pass
cdc_acceptance_ring_overflow_nonblocking rows=1024 iters=2 first_overflow_at=33 evicted=992 frontier_present=true status=pass
cdc_acceptance_admission_isolation rows=1024 iters=2 threshold_percent=5.00 observed_percent=-17.16 p99_0_subs_nanos=437 p99_100_subs_nanos=362 status=pass
```

Interpretation:

- This validates the harness-side acceptance reporter and nonzero
  failure path. The `1000` microsecond RTT value is a local smoke
  placeholder, not representative deployment evidence.

## 10k Acceptance-Oriented Sample

Command:

```sh
cargo bench --bench cdc_subscription -- --rows 10000 --iters 3
```

Result:

```text
cdc_live_tail_latency rows=10000 iters=3 p50_micros=0 p95_micros=0 p99_micros=0 p99_nanos=231
cdc_live_tail_fanout subscribers=1 rows=10000 iters=3 p99_micros=0 p99_nanos=294
cdc_live_tail_fanout subscribers=10 rows=10000 iters=3 p99_micros=1 p99_nanos=1550
cdc_live_tail_fanout subscribers=100 rows=10000 iters=3 p99_micros=14 p99_nanos=14392
cdc_live_tail_fanout subscribers=1000 rows=10000 iters=3 p99_micros=152 p99_nanos=152138
cdc_catchup_throughput rows=10000 iters=3 matched=15000 decoded_mib_per_sec=11007.41
cdc_ring_overflow_threshold cap_bytes=3328 rows=10000 first_overflow_at=33 evicted=9968 frontier_present=true
cdc_checkpoint_cost rows=10000 iters=3 avg_micros=72174
cdc_admission_isolation rows=10000 iters=3 p99_0_subs_micros=0 p99_100_subs_micros=0 p99_0_subs_nanos=378 p99_100_subs_nanos=96 regression_percent=-74.60
cdc_schema_fingerprint_change rows=10000 iters=3 changes=234 avg_nanos=6
cdc_wire_compression rows=10000 iters=3 encoded_bytes=40520 avg_micros=106
cdc_ring_stage_finalize rows=10000 iters=3 avg_micros=401125
cdc_stage_release_drain rows=10000 iters=3 push_avg_nanos=4150 stage_avg_nanos=98 release_avg_nanos=41790 drain_avg_nanos=57108
cdc_safe_emission_watermark_hold rows=10000 initial_safe_ms=50 blocked_before_heartbeat=10000 released_after_heartbeat=10000
cdc_frame_encode rows=10000 iters=3 avg_micros=95
```

Interpretation:

- Catchup throughput is above the ADR 054 starting point of
  100 MiB/sec on this synthetic tuple-filter path.
- Admission-stage p99 with 100 registered subscriptions is within
  the <= 5% starting point in this in-process sample; finalization
  fan-out remains intentionally O(N) and is reported separately.
- Ring overflow does not fail or block the benchmarked admission
  loop; the table-scoped eviction frontier remains present.
- Live-tail p99 is below microsecond resolution in-process. This
  does not measure a real one-way QUIC RTT.

## Local Full Matrix Validation

Command:

```sh
cargo bench --bench cdc_subscription -- --full
```

Result:

```text
cdc_bench_matrix_case rows=1000 iters=10
cdc_live_tail_latency rows=1000 iters=10 p50_micros=0 p95_micros=0 p99_micros=0 p99_nanos=350
cdc_live_tail_fanout subscribers=1 rows=1000 iters=10 p99_micros=0 p99_nanos=361
cdc_live_tail_fanout subscribers=10 rows=1000 iters=10 p99_micros=1 p99_nanos=1930
cdc_live_tail_fanout subscribers=100 rows=1000 iters=10 p99_micros=17 p99_nanos=17884
cdc_catchup_throughput rows=1000 iters=10 matched=5000 decoded_mib_per_sec=10182.22
cdc_ring_overflow_threshold cap_bytes=3328 rows=1000 first_overflow_at=33 evicted=968 frontier_present=true
cdc_checkpoint_cost rows=1000 iters=10 avg_micros=7940
cdc_admission_isolation rows=1000 iters=10 p99_0_subs_micros=0 p99_100_subs_micros=0 p99_0_subs_nanos=522 p99_100_subs_nanos=203 regression_percent=-61.11
cdc_schema_fingerprint_change rows=1000 iters=10 changes=78 avg_nanos=7
cdc_wire_compression rows=1000 iters=10 encoded_bytes=4488 avg_micros=16
cdc_ring_stage_finalize rows=1000 iters=10 avg_micros=3841
cdc_stage_release_drain rows=1000 iters=10 push_avg_nanos=3525 stage_avg_nanos=72 release_avg_nanos=116 drain_avg_nanos=360
cdc_safe_emission_watermark_hold rows=1000 initial_safe_ms=50 blocked_before_heartbeat=1000 released_after_heartbeat=1000
cdc_frame_encode rows=1000 iters=10 avg_micros=15
cdc_bench_matrix_case rows=10000 iters=10
cdc_live_tail_latency rows=10000 iters=10 p50_micros=0 p95_micros=0 p99_micros=0 p99_nanos=304
cdc_live_tail_fanout subscribers=1 rows=10000 iters=10 p99_micros=0 p99_nanos=376
cdc_live_tail_fanout subscribers=10 rows=10000 iters=10 p99_micros=1 p99_nanos=1961
cdc_live_tail_fanout subscribers=100 rows=10000 iters=10 p99_micros=17 p99_nanos=17902
cdc_live_tail_fanout subscribers=1000 rows=10000 iters=10 p99_micros=179 p99_nanos=179205
cdc_catchup_throughput rows=10000 iters=10 matched=50000 decoded_mib_per_sec=10722.54
cdc_ring_overflow_threshold cap_bytes=3328 rows=10000 first_overflow_at=33 evicted=9968 frontier_present=true
cdc_checkpoint_cost rows=10000 iters=10 avg_micros=81468
cdc_admission_isolation rows=10000 iters=10 p99_0_subs_micros=0 p99_100_subs_micros=0 p99_0_subs_nanos=447 p99_100_subs_nanos=193 regression_percent=-56.82
cdc_schema_fingerprint_change rows=10000 iters=10 changes=780 avg_nanos=7
cdc_wire_compression rows=10000 iters=10 encoded_bytes=40520 avg_micros=102
cdc_ring_stage_finalize rows=10000 iters=10 avg_micros=38919
cdc_stage_release_drain rows=10000 iters=10 push_avg_nanos=3549 stage_avg_nanos=74 release_avg_nanos=116 drain_avg_nanos=362
cdc_safe_emission_watermark_hold rows=10000 initial_safe_ms=50 blocked_before_heartbeat=10000 released_after_heartbeat=10000
cdc_frame_encode rows=10000 iters=10 avg_micros=103
cdc_bench_matrix_case rows=100000 iters=5
cdc_live_tail_latency rows=100000 iters=5 p50_micros=0 p95_micros=0 p99_micros=0 p99_nanos=340
cdc_live_tail_fanout subscribers=1 rows=100000 iters=5 p99_micros=0 p99_nanos=399
cdc_live_tail_fanout subscribers=10 rows=100000 iters=5 p99_micros=1 p99_nanos=1991
cdc_live_tail_fanout subscribers=100 rows=100000 iters=5 p99_micros=18 p99_nanos=18028
cdc_live_tail_fanout subscribers=1000 rows=100000 iters=5 p99_micros=180 p99_nanos=180403
cdc_catchup_throughput rows=100000 iters=5 matched=250000 decoded_mib_per_sec=7568.95
cdc_ring_overflow_threshold cap_bytes=3328 rows=100000 first_overflow_at=33 evicted=99968 frontier_present=true
cdc_checkpoint_cost rows=100000 iters=5 avg_micros=830623
cdc_admission_isolation rows=100000 iters=5 p99_0_subs_micros=0 p99_100_subs_micros=0 p99_0_subs_nanos=545 p99_100_subs_nanos=224 regression_percent=-58.90
cdc_schema_fingerprint_change rows=100000 iters=5 changes=3892 avg_nanos=7
cdc_wire_compression rows=100000 iters=5 encoded_bytes=400776 avg_micros=1031
cdc_ring_stage_finalize rows=100000 iters=5 avg_micros=391724
cdc_stage_release_drain rows=100000 iters=5 push_avg_nanos=3658 stage_avg_nanos=76 release_avg_nanos=121 drain_avg_nanos=376
cdc_safe_emission_watermark_hold rows=100000 initial_safe_ms=50 blocked_before_heartbeat=100000 released_after_heartbeat=100000
cdc_frame_encode rows=100000 iters=5 avg_micros=1095
```

Interpretation:

- The local `--full` command is executable and covers the intended
  small / medium / large row-count matrix.
- All local full-matrix catchup-throughput samples are above the
  ADR 054 starting point of 100 MiB/sec.
- Admission-stage p99 with 100 registered subscriptions is within
  the <= 5% starting point in every local full-matrix case.
- This remains in-process evidence. It does not measure a real
  one-way QUIC RTT or representative deployment hardware.

## Remaining Promotion Evidence

Before declaring ADR 054 shipped, run and record the full
small / medium / large matrix on representative hardware:

```sh
cargo bench --bench cdc_subscription -- --full --acceptance-one-way-quic-rtt-micros <measured_one_way_quic_rtt_micros>
```

The preferred evidence-capture wrapper is:

```sh
scripts/bench/run_cdc_subscription_acceptance.sh <output-dir> <measured_one_way_quic_rtt_micros>
```

It writes environment metadata, the raw bench log, a validation log,
and an acceptance summary into `<output-dir>`. The script exits
nonzero unless every expected `cdc_acceptance_*` line is present with
`status=pass`.

`--full` expands to the explicit matrix below. The explicit form
is useful when an operator wants to schedule each size separately:

```sh
cargo bench --bench cdc_subscription -- --rows 1000 --iters 10 --acceptance-one-way-quic-rtt-micros <measured_one_way_quic_rtt_micros>
cargo bench --bench cdc_subscription -- --rows 10000 --iters 10 --acceptance-one-way-quic-rtt-micros <measured_one_way_quic_rtt_micros>
cargo bench --bench cdc_subscription -- --rows 100000 --iters 5 --acceptance-one-way-quic-rtt-micros <measured_one_way_quic_rtt_micros>
```

The shipped evidence must include:

- end-to-end live-tail latency under fan-out with the measured
  one-way QUIC RTT for that environment;
- decoded catchup throughput;
- ring-overflow recovery behavior;
- admission p99 regression at 100 active subscriptions.
- `cdc_acceptance_*` lines with `status=pass` for every case in
  the representative matrix.

Current local evidence is synthetic and in-process. It clears the
catchup-throughput and admission-stage isolation starting points
locally, and the verifier smoke validates pass/fail reporting, but
it does not measure a real one-way QUIC RTT.

## Representative Acceptance Template

Use this block when recording the promotion run. Do not check the
ADR 054 bench-acceptance item until every `cdc_acceptance_*` line
below reports `status=pass` for all matrix cases.

Environment:

- hardware:
- node count:
- deployment topology:
- storage medium:
- CPU governor / power profile:
- git revision:
- rustc version:
- command:
  `scripts/bench/run_cdc_subscription_acceptance.sh <output-dir> <measured_one_way_quic_rtt_micros>`
- raw bench command:
  `cargo bench --bench cdc_subscription -- --full --acceptance-one-way-quic-rtt-micros <measured_one_way_quic_rtt_micros>`

QUIC RTT measurement:

- measurement command / probe:
- measured one-way QUIC RTT micros:
- measurement notes:

Acceptance output:

```text
cdc_bench_matrix_case rows=1000 iters=10
cdc_acceptance_live_tail_latency rows=1000 ... status=pass
cdc_acceptance_catchup_throughput rows=1000 ... status=pass
cdc_acceptance_ring_overflow_nonblocking rows=1000 ... status=pass
cdc_acceptance_admission_isolation rows=1000 ... status=pass

cdc_bench_matrix_case rows=10000 iters=10
cdc_acceptance_live_tail_latency rows=10000 ... status=pass
cdc_acceptance_catchup_throughput rows=10000 ... status=pass
cdc_acceptance_ring_overflow_nonblocking rows=10000 ... status=pass
cdc_acceptance_admission_isolation rows=10000 ... status=pass

cdc_bench_matrix_case rows=100000 iters=5
cdc_acceptance_live_tail_latency rows=100000 ... status=pass
cdc_acceptance_catchup_throughput rows=100000 ... status=pass
cdc_acceptance_ring_overflow_nonblocking rows=100000 ... status=pass
cdc_acceptance_admission_isolation rows=100000 ... status=pass
```

Promotion decision:

- [ ] The representative run used a measured one-way QUIC RTT, not
      the local smoke placeholder.
- [ ] Every `cdc_acceptance_*` line reports `status=pass`.
- [ ] The wrapper summary reports `promotion_candidate=true`.
- [ ] The raw full bench output is retained with this file or in
      the linked release evidence.
- [ ] `plans/adr/054_cdc_subscription_substrate.md` bench
      acceptance is checked only after the bullets above are true.
