# ADR 063 Page-Level Predicate Stats Bench Results

Date: 2026-05-09

Command:

```bash
cargo bench --bench row_group_fetch -- --quick
```

This quick artifact records the ADR 063 rows emitted by
`benches/row_group_fetch.rs`. The fixture uses a `UInt8`
predicate column encoded as `DictionaryBitPacked`, with the
default PointRead / Scan row-group thresholds preserved.

## Headline Rows

| Row | Profile | Rows | Selectivity | Route | Dense predicate bytes | Narrowed predicate bytes | Reduction | Page stats bytes | Combined metadata |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| clustered | Scan | 65,536 | 5% | page_narrowed | 9,153 | 144 | 63.56x | 1,704 | 3,472 |
| clustered | Scan | 65,536 | 25% | page_narrowed | 9,152 | 0 | zero fetch | 1,704 | 3,472 |
| clustered | PointRead | 8,192 | 5% | page_narrowed | 1,145 | 144 | 7.95x | 212 | 2,212 |
| random-uniform | Scan | 65,536 | 5% | dense_fallback | 9,216 | 9,216 | 1.00x | 1,704 | 3,472 |

The Scan 5% row exceeds the >= 3x clustered predicate-fetch
plaintext-byte gate. The PointRead 5% row exceeds the >= 1.5x
Phase 0 profile-emission gate, so PointRead remains in scope.

## Metadata Envelope

Representative Scan fixture:

- `row_group_directory_bytes=760`
- `encoding_directory_bytes_before_page_stats=1008`
- `encoding_directory_bytes_after_page_stats=2712`
- `page_stats_bytes=1704`
- `combined_metadata_bytes=3472`
- `probe_64k_fit=true`
- `record_bytes=4274896`

Page stats are roughly 0.04% of record bytes on this fixture
and combined metadata stays well inside the 64 KiB initial probe.

## Classification

Scan clustered 5%:

- `page_stats=64`, `page_count=64`
- `disjoint_pages=60`
- `exact_pages=3`
- `maybe_pages=1`
- `exact_match_rows=3072`
- `selected_ranges=1`

All-Disjoint and All-Exact boundary rows both produced zero
predicate fetch bytes. Random-uniform Scan rows routed to
`dense_fallback` with no predicate-byte reduction, as expected.

## Boundary Rows

| Profile | Rows | Row groups | Directory | Encoding directory | Page stats |
| --- | ---: | ---: | --- | --- | ---: |
| PointRead | 4,096 | 0 | false | false | 0 |
| Scan | 65,535 | 0 | false | false | 0 |
| Scan | 65,536 | 1 | true | true | 64 |
| Scan | 65,537 | 2 | true | true | 64 |

## Type And Cascade Gates

Fixed-width page-stat type gate:

- included: Int8/16/32/64, UInt8/16/32/64,
  Timestamp(ms/us/ns), Date32, Date64, Boolean
- excluded: Float64, Utf8

Cascade gate:

- eligible: BitPacked, DictionaryPlain, DictionaryBitPacked
- excluded: RlePlain (page-emitting but not allow-listed),
  Delta (allow-listed but not page-emitting), Plain, Lz4Frame

## Writer Cost And Determinism

Quick writer-cost rows:

- ADR 062 Scan baseline appears in the same quick run as a
  neighboring control row; ADR 063 gates on the enabled /
  disabled rows below.
- ADR 063 Scan stats disabled:
  `micros_per_op=12008.839`, `page_stats=0`
- ADR 063 Scan stats enabled:
  `micros_per_op=19546.284`, `page_stats=64`

Cross-replica determinism:

- PointRead: `byte_equal=true`,
  digest `d7240231bf656886d394bf5605df23d1423f257eca8d019745aabb1c1193e12e`
- Scan: `byte_equal=true`,
  digest `83410c2dcab87bedc445b5407b285e42f4e7cdc5a620e10f72ecad58018dc6e4`
