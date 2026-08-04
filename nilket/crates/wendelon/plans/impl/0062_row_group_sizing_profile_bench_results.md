# ADR 062 Row-Group Sizing Profile Bench Results

Date: 2026-05-08

Commands run:

```sh
cargo bench --bench row_group_fetch -- --quick > /tmp/row_group_fetch_062_quick.out
cargo bench --bench row_group_fetch > /tmp/row_group_fetch_062_full.out
cargo bench --bench row_group_fetch > /tmp/row_group_fetch_062_full_after_preflight.out
cargo bench --bench row_group_fetch > /tmp/row_group_fetch_062_full_final.out
cargo bench --bench row_group_fetch -- --quick > /tmp/row_group_fetch_062_quick_final.out
```

Environment:

- `rustc 1.93.0 (254b59607 2026-01-19)`
- `cargo 1.93.0 (083ac5135 2025-12-15)`
- `x86_64-unknown-linux-gnu`, Linux 6.17.0-22-generic

Scope note: the promotion disposition below uses the final full
100-iteration run at `/tmp/row_group_fetch_062_full_final.out`. That run
emitted 569 benchmark rows, including 54 ADR 062 rows. The final quick smoke
run emitted 193 rows. Earlier runs were retained as smoke / optimization
checks. The final full run is after the `Scan` writer preflight and row-group
fixed-point clone removal.

Raw ADR 062 row anchors in the final full output include
`scenario=adr062_writer_cost`, `scenario=adr062_scan_profile_directory`,
`scenario=adr062_secondary_index_point_read_selected_bytes`,
`scenario=adr062_scan_boundary`, `scenario=adr062_scan_scale_model`,
`scenario=adr062_sparse_projection_model`,
`scenario=adr062_stats_prune_model`,
`scenario=adr062_scan_compression_loss`, and
`scenario=adr062_cross_replica_determinism`.

## Target Selection

The candidate sweep records the closed candidate set
`{4_096, 8_192, 16_384, 32_768, 65_536, 131_072}` across the ADR 062 schema
fixtures. Rows below a candidate's paging-row gate record `0` row groups.

| Fixture | Target | Row groups | Modeled row-group bytes | One-entry candidate |
| --- | ---: | ---: | ---: | --- |
| 8,192 x 50 int64 | 4,096 | 2 | 1,638,400 | no |
| 8,192 x 50 int64 | 8,192 | 1 | 3,276,800 | yes |
| 8,192 x 50 int64 | 16,384 | 0 | 0 | no |
| 8,192 x 50 int64 | 32,768 | 0 | 0 | no |
| 8,192 x 50 int64 | 65,536 | 0 | 0 | no |
| 8,192 x 50 int64 | 131,072 | 0 | 0 | no |
| 16,384 x 50 int64 | 4,096 | 4 | 1,638,400 | no |
| 16,384 x 50 int64 | 8,192 | 2 | 3,276,800 | no |
| 16,384 x 50 int64 | 16,384 | 1 | 6,553,600 | yes |
| 16,384 x 50 int64 | 32,768 | 0 | 0 | no |
| 16,384 x 50 int64 | 65,536 | 0 | 0 | no |
| 16,384 x 50 int64 | 131,072 | 0 | 0 | no |
| 65,536 x 4 int64 | 4,096 | 16 | 131,072 | no |
| 65,536 x 4 int64 | 8,192 | 8 | 262,144 | no |
| 65,536 x 4 int64 | 16,384 | 4 | 524,288 | no |
| 65,536 x 4 int64 | 32,768 | 2 | 1,048,576 | no |
| 65,536 x 4 int64 | 65,536 | 1 | 2,097,152 | yes |
| 65,536 x 4 int64 | 131,072 | 0 | 0 | no |
| 65,536 x 4 mixed | 4,096 | 16 | 167,936 | no |
| 65,536 x 4 mixed | 8,192 | 8 | 335,872 | no |
| 65,536 x 4 mixed | 16,384 | 4 | 671,744 | no |
| 65,536 x 4 mixed | 32,768 | 2 | 1,343,488 | no |
| 65,536 x 4 mixed | 65,536 | 1 | 2,686,976 | yes |
| 65,536 x 4 mixed | 131,072 | 0 | 0 | no |

Disposition: `65_536` remains the selected `Scan` target. It is the smallest
candidate that makes the largest legal scan fixture a one-row-group record
while keeping modeled per-row-group bytes in MiB territory. Smaller candidates
fragment that fixture into multiple row groups; larger candidates suppress
paging on the same fixture.

## Scan Profile Actual Fixture

Actual fixture: `65,536 x 4 int64`, uncompressed row-group writer.

| Profile | Row groups | Writer us/op | Over disabled |
| --- | ---: | ---: | ---: |
| disabled | 0 | 21,780.859 | baseline |
| PointRead | 16 | 25,871.344 | +18.79% |
| Scan | 1 | 24,608.043 | +12.98% |

Disposition: writer-cost gate passes. `Scan` is below the accepted
`<= 25%` overhead gate on the largest legal representative fixture.

Directory / dense-fetch row:

| Rows | Columns | Row groups | Buffer pages | Stats | Record bytes | Directory bytes | Directory fraction | Dense p50 | Dense p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 65,536 | 4 | 1 | 18 | 6 | 6,900,104 | 1,188 | 0.0172% | 6,904,400 | 6,904,400 |

Disposition:

- dense p50 / p95 pass: both exceed 1 MiB / 4 MiB gates;
- directory-size pass: 1,188 bytes is 0.0172% of record bytes and fits within
  ADR 044's metadata probe budget.

Secondary-index point-read selected bytes:

| Profile | Selected groups | Total groups | Selected ranges | Selected bytes | Record bytes | Fraction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Scan | 1 | 1 | 1 | 533,056 | 6,900,104 | 7.73% |

Disposition: point-read selected bytes remain below the full record body. The
latency tradeoff versus `PointRead` is the intentional access-pattern tradeoff:
`PointRead` retains 16 row groups on this fixture, while `Scan` fetches one
larger row group.

## Boundary Disposition

Actual `Scan` boundary rows:

| Rows | Row groups | Directory present | Directory bytes |
| ---: | ---: | --- | ---: |
| 4,096 | 0 | no | 0 |
| 65,535 | 0 | no | 0 |
| 65,536 | 1 | yes | 1,188 |
| 65,537 | 2 | yes | 2,268 |

Disposition: one-entry `Scan` directory behavior is empirically recorded at
exact target; target-plus-one remains two row groups with no V1 tail merge.

## Scan-Scale Dense Model

ADR 062 scan-scale model rows:

| Rows | Records | Row-group rows | Record bytes | Chunk size | Dense p50 | Dense p95 | GET ranges |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16,777,216 | 256 | 65,536 | 4,194,304 | 4 KiB | 4,210,688 | 4,210,688 | 1 |
| 16,777,216 | 256 | 65,536 | 4,194,304 | 8 KiB | 4,202,496 | 4,202,496 | 1 |
| 16,777,216 | 256 | 65,536 | 4,194,304 | 16 KiB | 4,198,400 | 4,198,400 | 1 |
| 16,777,216 | 256 | 65,536 | 4,194,304 | 32 KiB | 4,196,352 | 4,196,352 | 1 |
| 16,777,216 | 256 | 65,536 | 4,194,304 | 64 KiB | 4,195,328 | 4,195,328 | 1 |
| 100,000,000 | 1,526 | 65,536 | 4,194,304 | 16 KiB | 4,198,400 | 4,198,400 | 1 |

Disposition: dense p50 / p95 and dense GET-count gates pass across the
scan-scale modeled rows. The 16 KiB ADR 061 default row remains above both
MiB-class gates.

## Sparse Projection / Stats-Prune

ADR 062 sparse projection modeled rows at 16 KiB chunks:

| Mask | Scan sparse bytes | 64 KiB baseline bytes | Reduction |
| --- | ---: | ---: | ---: |
| clustered 1% | 16,400 | 65,552 | 3.997x |
| clustered 5% | 82,000 | 327,760 | 3.997x |

Disposition: ADR 060 sparse projection non-regression passes for the clustered
scan-profile rows.

Stats-prune model:

| Predicate | PointRead survival | Scan survival | Scan / PointRead |
| --- | ---: | ---: | ---: |
| clustered broad 75% | 75% | 100% | 1.333x |
| sorted broad 75% | 75% | 100% | 1.333x |
| random uniform | 100% | 100% | 1.000x |

Disposition: representative broad sorted / clustered predicates stay within
the 1.5x survival-rate gate. Random-uniform is recorded separately as planned.

## Compression And Determinism

LZ4 compression loss at `Scan` profile:

| Shape | Single LZ4 bytes | Scan LZ4 bytes | Loss |
| --- | ---: | ---: | ---: |
| int64 | 3,658,376 | 3,659,592 | +0.0332% |
| float64 | 3,651,272 | 3,652,360 | +0.0297% |
| utf8 | 4,076,104 | 4,077,512 | +0.0345% |
| mixed | 3,484,168 | 3,485,384 | +0.0349% |

Disposition: compression loss is comfortably within ADR 056 accepted bounds.

Cross-replica determinism:

| Profile | Byte equal | Digest | Bytes |
| --- | --- | --- | ---: |
| PointRead | true | `ab1bffe15987bad180c89f62d1dce6a68c60e42df72626439a88eb7a4effeef1` | 6,924,936 |
| Scan | true | `1b3c1f94ebf215861a9037c733fb4db08d1fafdabef38b6331b273f5a304757c` | 6,900,104 |

Disposition: deterministic output passes for both profiles. PointRead keeps
ADR 056's default sizing constants and single-batch fallback behavior; the
code path remains byte-shape equivalent for PointRead fixtures, while `Scan`
uses its intentional one-entry directory branch.

## Promotion Disposition

ADR 062 is promoted with `Scan` target `65_536`.

Gate summary:

- closed profile catalog + config parsing: pass, covered by code and
  `config_loader` / `row_group_config` tests;
- exact-target one-entry directory and target-plus-one tail: pass, covered by
  `row_group_writer` tests and bench boundary rows;
- dense p50 / p95 and GET count: pass;
- sparse projection modeled non-regression: pass;
- stats-prune representative broad predicates: pass;
- secondary-index selected bytes below full record body: pass;
- writer-cost overhead at `Scan`: pass at +12.98%;
- directory bytes: pass at 0.0172%;
- compression loss: pass across integer / float / UTF-8 / mixed shapes;
- cross-replica determinism: pass for both profiles.
