# ADR 053 Wire Arrow IPC Body Compression Bench Results

Status: full promotion matrix captured on 2026-05-04. ADR 053 is not
declared shipped by this file alone because the measured small-frame
latency gate needs council disposition.

Harness:

```sh
cargo bench --bench wire_arrow_ipc_body_compression -- --quick
cargo bench --bench wire_arrow_ipc_body_compression -- --full
```

The harness reports:

- `wire_arrow_ipc_size` rows for default-vs-LZ4 IPC byte ratio.
- `encode_default` and `encode_wire_lz4` for encode CPU partitioning.
- `decode_default` and `decode_wire_lz4` for decode CPU partitioning.
- RPC-shaped rows for `client_batch`, `query_response`,
  `migration_records`, and `read_response` pass-through decode-only cost.

Required full-run matrix:

- Payload sizes: 1 KiB, 4 KiB, 1 MiB, 10 MiB.
- Shapes: `narrow_int`, `wide_low_cardinality`,
  `wide_high_cardinality`, `high_entropy_binary`,
  `mixed_nullability`.
- Report encode CPU, decode CPU, payload-size ratio, and RPC-shaped
  latency partitions.

Acceptance starting points from the implementation plan:

- Decode CPU stays materially below encode CPU on representative payloads.
- Analytical-shaped Arrow batches compress to at most 60% of the
  uncompressed IPC size.
- Small-frame end-to-end latency does not regress by more than 5%.
- Storage pass-through `READ_RESP` has no wire-helper encode in the timed
  path.

## 2026-05-04 Quick Smoke

Command:

```sh
cargo bench --bench wire_arrow_ipc_body_compression -- --quick
```

Scope: release-profile smoke only. `--quick` covers 1 KiB and 4 KiB
payload targets, `narrow_int`, `wide_low_cardinality`, and
`high_entropy_binary` shapes, with two measured iterations and no warmup.
This is not the full promotion matrix.

Size ratios:

| target | shape | uncompressed bytes | compressed bytes | ratio |
| --- | --- | ---: | ---: | ---: |
| 1 KiB | narrow_int | 1,672 | 1,416 | 0.8469 |
| 1 KiB | wide_low_cardinality | 1,800 | 904 | 0.5022 |
| 1 KiB | high_entropy_binary | 1,800 | 1,864 | 1.0356 |
| 4 KiB | narrow_int | 4,744 | 3,464 | 0.7302 |
| 4 KiB | wide_low_cardinality | 5,256 | 1,096 | 0.2085 |
| 4 KiB | high_entropy_binary | 5,256 | 5,128 | 0.9756 |

CPU partition, microseconds/op:

| target | shape | encode default | encode LZ4 | decode default | decode LZ4 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 KiB | narrow_int | 5.878 | 33.958 | 14.404 | 44.867 |
| 1 KiB | wide_low_cardinality | 6.138 | 37.669 | 8.434 | 24.676 |
| 1 KiB | high_entropy_binary | 5.808 | 13.222 | 5.089 | 27.025 |
| 4 KiB | narrow_int | 5.003 | 15.629 | 6.123 | 12.521 |
| 4 KiB | wide_low_cardinality | 5.188 | 9.371 | 4.300 | 12.097 |
| 4 KiB | high_entropy_binary | 6.226 | 15.786 | 4.143 | 24.986 |

RPC-shaped partition:

| RPC | path | uncompressed bytes | compressed bytes | ratio | default encode+decode us | LZ4 encode+decode us | default transmit us at 1 Gbps | LZ4 transmit us at 1 Gbps |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TABLE_DELETE_REQ | client_batch | 1,800 | 1,416 | 0.7867 | 9.589 | 31.997 | 14.400 | 11.328 |
| TABLE_QUERY_RESP | query_response | 5,256 | 1,096 | 0.2085 | 9.778 | 42.324 | 42.048 | 8.768 |
| MIGRATE_RESP | migration_records | 5,000 | 2,312 | 0.4624 | 14.524 | 60.218 | 40.000 | 18.496 |
| READ_RESP pass-through decode-only | read_response | n/a | 1,096 | n/a | n/a | 39.218 | n/a | 8.768 |

Interpretation:

- The quick smoke confirms the density direction for compressible Arrow
  batches: 4 KiB low-cardinality payloads shrink to 20.85% of default IPC
  size, and migration-shaped rows shrink to 46.24%.
- High-entropy 1 KiB payloads can expand slightly (103.56% of default IPC),
  so the full matrix must keep the small-edge / entropy crossover visible.
- Local CPU-only encode+decode rows are slower under LZ4 for these small
  quick cases. The small-frame latency acceptance gate is therefore not
  satisfied by this smoke run alone; the full matrix must evaluate the
  combined encode, wire-transmit, and decode partition.
- Storage pass-through `READ_RESP` reports decode-only cost and
  `wire_helper_encode_in_timed_path=false`, preserving the ADR 053 boundary.

## 2026-05-04 Full Promotion Matrix

Command:

```sh
cargo bench --bench wire_arrow_ipc_body_compression -- --full
```

Scope: release-profile run over 1 KiB, 4 KiB, 1 MiB, and 10 MiB targets;
all five planned shapes; five measured iterations with one warmup pass.

Size and CPU partition:

| target | shape | uncompressed bytes | compressed bytes | ratio | encode default us | encode LZ4 us | decode default us | decode LZ4 us |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | narrow_int | 1,672 | 1,416 | 0.8469 | 4.493 | 33.823 | 2.680 | 32.853 |
| 1 KiB | wide_low_cardinality | 1,800 | 904 | 0.5022 | 3.641 | 6.988 | 3.337 | 9.809 |
| 1 KiB | wide_high_cardinality | 1,800 | 1,416 | 0.7867 | 3.520 | 10.103 | 2.975 | 13.005 |
| 1 KiB | high_entropy_binary | 1,800 | 1,864 | 1.0356 | 3.021 | 7.253 | 2.895 | 23.821 |
| 1 KiB | mixed_nullability | 1,800 | 1,672 | 0.9289 | 4.146 | 10.160 | 3.682 | 10.700 |
| 4 KiB | narrow_int | 4,744 | 3,464 | 0.7302 | 3.210 | 11.955 | 3.004 | 29.379 |
| 4 KiB | wide_low_cardinality | 5,256 | 1,096 | 0.2085 | 3.557 | 7.451 | 3.175 | 26.633 |
| 4 KiB | wide_high_cardinality | 5,256 | 1,992 | 0.3790 | 3.389 | 10.082 | 3.133 | 27.957 |
| 4 KiB | high_entropy_binary | 5,256 | 5,128 | 0.9756 | 3.636 | 9.716 | 3.469 | 27.745 |
| 4 KiB | mixed_nullability | 5,000 | 2,312 | 0.4624 | 4.395 | 12.750 | 3.944 | 28.677 |
| 1 MiB | narrow_int | 1,070,984 | 356,040 | 0.3324 | 77.821 | 780.551 | 53.007 | 1,830.911 |
| 1 MiB | wide_low_cardinality | 1,182,216 | 70,472 | 0.0596 | 104.152 | 232.579 | 70.727 | 1,229.066 |
| 1 MiB | wide_high_cardinality | 1,182,216 | 195,848 | 0.1657 | 94.180 | 612.625 | 73.608 | 1,428.806 |
| 1 MiB | high_entropy_binary | 1,182,216 | 1,114,888 | 0.9430 | 85.272 | 1,035.772 | 79.850 | 150.393 |
| 1 MiB | mixed_nullability | 1,079,688 | 194,632 | 0.1803 | 74.235 | 571.904 | 59.664 | 379.425 |
| 10 MiB | narrow_int | 10,704,776 | 3,515,976 | 0.3284 | 4,975.907 | 8,264.018 | 707.167 | 5,078.341 |
| 10 MiB | wide_low_cardinality | 11,817,480 | 698,056 | 0.0591 | 5,695.779 | 2,296.412 | 839.357 | 1,965.963 |
| 10 MiB | wide_high_cardinality | 11,817,480 | 1,954,184 | 0.1654 | 7,021.800 | 6,639.657 | 804.829 | 3,496.463 |
| 10 MiB | high_entropy_binary | 11,817,480 | 11,142,536 | 0.9429 | 1,353.531 | 5,080.751 | 835.240 | 2,074.688 |
| 10 MiB | mixed_nullability | 10,790,728 | 1,936,136 | 0.1794 | 1,984.531 | 6,222.224 | 754.967 | 3,135.912 |

RPC-shaped partition:

| RPC | path | uncompressed bytes | compressed bytes | ratio | default encode+decode us | LZ4 encode+decode us | default transmit us at 1 Gbps | LZ4 transmit us at 1 Gbps |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TABLE_DELETE_REQ | client_batch | 1,800 | 1,416 | 0.7867 | 4.782 | 10.519 | 14.400 | 11.328 |
| TABLE_QUERY_RESP | query_response | 11,817,480 | 698,056 | 0.0591 | 10,897.483 | 4,704.528 | 94,539.840 | 5,584.448 |
| MIGRATE_RESP | migration_records | 10,790,728 | 1,936,136 | 0.1794 | 2,955.842 | 9,584.155 | 86,325.824 | 15,489.088 |
| READ_RESP pass-through decode-only | read_response | n/a | 698,056 | n/a | n/a | 1,908.703 | n/a | 5,584.448 |

Interpretation:

- Analytical-shaped 1 MiB and 10 MiB batches clear the density target:
  `wide_low_cardinality`, `wide_high_cardinality`, `mixed_nullability`,
  and `narrow_int` all land below 60% of default IPC size.
- High-entropy payloads remain near default size (94-104%) and are the
  expected edge case for the pinned codec.
- `TABLE_QUERY_RESP` at the 10 MiB low-cardinality shape is a clear
  end-to-end win even before transmit savings: encode+decode drops from
  10.897 ms to 4.705 ms, and estimated 1 Gbps transmit drops from
  94.540 ms to 5.584 ms.
- `MIGRATE_RESP` pays more local CPU under LZ4 (9.584 ms vs 2.956 ms) but
  the estimated 1 Gbps transmit reduction is much larger (86.326 ms to
  15.489 ms).
- The small `TABLE_DELETE_REQ` shape misses the draft small-frame target:
  CPU+estimated 1 Gbps transmit is 19.182 us default vs 21.847 us LZ4
  (+13.9%). At 10 Gbps the local CPU cost dominates further. This is now a
  measured disposition item, not an unmeasured risk.
- Storage pass-through `READ_RESP` remains decode-only in the timed path
  and reports `wire_helper_encode_in_timed_path=false`.
