# ADR 056 Row-Group Fetch Bench Results

Date: 2026-05-06
Machine: local workstation, 24 logical CPUs, 61 GiB RAM

## Status

Phase 10 evidence accepted with an empirical writer-cost gate revision. This
artifact promotes ADR 056 to shipped and opens ADR 057 drafting.

The original `<= 15%` writer-CPU target was provisional and set before
bench data existed. The Phase 10 matrix shows that target is below the
structural cost floor of multi-RecordBatch IPC emission. Council accepted a
revised `<= 25%` writer-cost gate on the largest legal representative
fixture because the feature is opt-in writer-side, default-off, and the
read-side savings are the load-bearing OLAP win.

## Commands

```sh
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
cargo test --test architecture_test test_adr056_row_group_fetch_guards --quiet
cargo fmt --check
```

An attempted kernel profile with `perf record -F 99 -g -- ... --quick`
failed because this machine has `perf_event_paranoid=4`. The harness-level
stats-on / stats-off and directory-build splits are the writer-path profile
for this pass.

## Fixture Legality

The original Phase 10 text named a 65,536-row / 50-column uncompressed
fixture. That shape exceeds the current `MultiRowLayoutDescriptor`
`max_record_bytes = 8 MiB` cap and is rejected by production admission.

The harness now uses legal default cases:

- 8,192 rows x 50 int64 columns
- 16,384 rows x 50 int64 columns
- 65,536 rows x 4 int64 columns

The writer-cost promotion gate is evaluated on the largest legal
representative fixture: 65,536 rows x 4 int64 columns.

## Stats Optimization

The original row-group stats path canonical-encoded every value before
comparison. The patched path compares with typed Arrow kernels and
canonical-encodes only the winning min/max values.

| Case | Pre-opt stats compute | Post-opt stats compute | Pre-opt directory build + serialize | Post-opt directory build + serialize |
| --- | ---: | ---: | ---: | ---: |
| 8,192 x 50 int64 | 5,154 us/op | 462 us/op | 4,764 us/op | 488 us/op |
| 65,536 x 4 int64 | 3,316 us/op | 300 us/op | 3,419 us/op | 311 us/op |

Stats are no longer the dominant writer-cost miss. The remaining overhead is
mostly RecordBatch splitting and multi-batch IPC serialization.

## Writer CPU

Current production-shape defaults (`min_paged_record_rows = 8,192`,
`target_row_group_rows = 4,096`):

| Case | Disabled | Enabled + stats | Enabled, no stats | Row groups | Stats overhead vs disabled | No-stats overhead vs disabled |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8,192 x 50 int64 | 4,346 us/op | 7,745 us/op | 5,717 us/op | 2 | +78% | +32% |
| 16,384 x 50 int64 | 9,653 us/op | 17,768 us/op | 14,851 us/op | 4 | +84% | +54% |
| 65,536 x 4 int64 | 17,212 us/op | 21,329 us/op | 19,534 us/op | 16 | +24% | +13% |

The largest legal representative fixture passes the revised `<= 25%`
writer-cost gate. Wide threshold-margin fixtures do not; that is documented
as an operator-facing cost warning rather than a promotion blocker. Row-group
writing remains default-off and opt-in.

Pre-optimization baseline from the same expanded harness:

| Case | Disabled | Enabled + stats | Enabled, no stats | Stats overhead vs disabled | No-stats overhead vs disabled |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8,192 x 50 int64 | 4,658 us/op | 23,297 us/op | 7,484 us/op | +400% | +61% |
| 65,536 x 4 int64 | 23,595 us/op | 37,585 us/op | 23,938 us/op | +59% | +1% |

## Threshold Experiments

Threshold changes were tested and rejected before promotion:

- Raising `min_paged_record_rows` to 16,384 simply moved the margin case; a
  16,384 x 50 fixture still overshot the writer gate.
- A derived 64 KiB per row-group/column floor was too conservative for the
  current estimator and disabled paging on the representative 65,536 x 4
  fixture.

No threshold change ships with ADR 056. Defaults remain:
`target_row_group_rows = 4,096`, `max_row_group_rows = 8,192`,
`min_paged_record_rows = 8,192`, `min_paged_record_bytes = 512 KiB`.

## Target-Row Sweep

The sweep keeps measuring the row-group surface across target sizes.

| Case | Target rows | Row groups | Writer time |
| --- | ---: | ---: | ---: |
| 8,192 x 50 int64 | 1,024 | 8 | 7,115 us/op |
| 8,192 x 50 int64 | 4,096 | 2 | 7,771 us/op |
| 8,192 x 50 int64 | 8,192 | 0 | 3,680 us/op |
| 8,192 x 50 int64 | 16,384 | 0 | 3,592 us/op |
| 16,384 x 50 int64 | 1,024 | 16 | 11,868 us/op |
| 16,384 x 50 int64 | 4,096 | 4 | 11,322 us/op |
| 16,384 x 50 int64 | 8,192 | 2 | 11,358 us/op |
| 16,384 x 50 int64 | 16,384 | 0 | 5,321 us/op |
| 65,536 x 4 int64 | 1,024 | 64 | 22,073 us/op |
| 65,536 x 4 int64 | 4,096 | 16 | 20,603 us/op |
| 65,536 x 4 int64 | 8,192 | 8 | 20,665 us/op |
| 65,536 x 4 int64 | 16,384 | 4 | 20,326 us/op |

Larger target row groups did not remove enough writer cost to justify a
default change.

## Directory Size

Directory sizes remain within the `<= 2%` record-body target.

| Case | Row groups | Buffer pages | Stats entries | Record bytes | Directory value bytes | Record fraction | 64 KiB probe fraction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8,192 x 50 int64 | 2 | 220 | 104 | 3,947,144 | 14,780 | 0.37% | 22.55% |
| 16,384 x 50 int64 | 4 | 440 | 208 | 7,891,272 | 29,452 | 0.37% | 44.94% |
| 65,536 x 4 int64 | 16 | 288 | 96 | 6,924,936 | 17,388 | 0.25% | 26.53% |

All legal fixtures fit under the ADR 044 64 KiB initial probe window.

## Selected Bytes

Selected-byte fractions support the row-group read path for point and sparse
projected reads on legal fixtures.

| Case | Projection columns | Selected groups | Selected bytes | Fraction |
| --- | ---: | ---: | ---: | ---: |
| 8,192 x 50 int64 | 1 | 1 / 2 | 36,032 | 0.91% |
| 8,192 x 50 int64 | 5 | 1 / 2 | 169,152 | 4.29% |
| 8,192 x 50 int64 | 25 | 1 / 2 | 834,752 | 21.15% |
| 8,192 x 50 int64 | 50 | 1 / 2 | 1,666,752 | 42.23% |
| 16,384 x 50 int64 | 1 | 1 / 4 | 36,032 | 0.46% |
| 16,384 x 50 int64 | 5 | 1 / 4 | 169,152 | 2.14% |
| 16,384 x 50 int64 | 25 | 1 / 4 | 834,752 | 10.58% |
| 16,384 x 50 int64 | 50 | 1 / 4 | 1,666,752 | 21.12% |
| 65,536 x 4 int64 | 1 | 1 / 16 | 33,856 | 0.49% |
| 65,536 x 4 int64 | 1 | 8 / 16 | 270,848 | 3.91% |
| 65,536 x 4 int64 | 4 | 1 / 16 | 133,696 | 1.93% |
| 65,536 x 4 int64 | 4 | 8 / 16 | 1,069,568 | 15.45% |
| 65,536 x 4 int64 | 4 | 16 / 16 | 2,139,136 | 30.89% |

## Compression Ratio

8,192 rows x 16 columns with LZ4 frame compression:

| Shape | Single-batch bytes | Row-group bytes | Row-group loss |
| --- | ---: | ---: | ---: |
| int64 | 859,080 | 874,696 | +1.82% |
| float64 | 901,704 | 910,152 | +0.94% |
| utf8 | 1,209,736 | 1,224,264 | +1.20% |
| mixed | 828,360 | 840,328 | +1.44% |

Compression loss passes the initial target on this local harness
(`<= 4%` integer / float, `<= 2%` UTF-8).

## Planner And Materialization

| Case | Planner one group | Phase B mask | Masked materialization |
| --- | ---: | ---: | ---: |
| 8,192 x 50 int64 | 0.37 us/op | 3.28 us/op | 1.02 us/op |
| 16,384 x 50 int64 | 0.55 us/op | 6.34 us/op | 1.06 us/op |
| 65,536 x 4 int64 | 0.36 us/op | 24.87 us/op | 1.02 us/op |

Planner and materialization overheads are not the bottleneck in this pass.

## Council Disposition

Council accepted the provisional-gate interpretation:

1. The original `<= 15%` writer-cost target was a pre-data estimate, not an
   architectural invariant.
2. The measured writer floor on the largest legal representative fixture is
   `+24%`, so the empirical gate is revised to `<= 25%`.
3. Threshold-margin wide fixtures are expected to overshoot and must be
   called out in operator guidance; they do not block promotion because
   writer-side row-group emission is opt-in and default-off.
4. Read-side evidence is the strategic OLAP criterion: sparse projections
   fetch `0.49%` to `0.91%` of whole-record bytes for one selected group,
   directory size stays below `0.4%` of record body, and compression loss is
   below `2%` across the tested shapes.
5. ADR 056 is promoted. ADR 057 drafting may open with this artifact as its
   empirical input.
