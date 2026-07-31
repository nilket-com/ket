# ADR 057 Bench Results — Encoding Cascades Diagnostic Matrix

Date: 2026-05-06

Status: Phase 10 evidence collected; representative writer gate
passes after selector tightening. The 1000-hit / 100-record L2
GET-rate cost row remains a deterministic model, backed by an
end-to-end page-fast-path test.

Command:

```sh
cargo bench --bench encoding
```

Implementation state:

- descriptive `storage::encoding` naming; no deprecated acronym;
- physical IPC body compaction is implemented: RecordBatch buffer
  offsets / lengths are rewritten to compact encoded extents;
- `Dictionary(BitPacked)` is implemented for fixed-width values and
  variable-width Arrow value buffers when the sibling offset buffer
  supplies value boundaries;
- `BitPacked` is implemented for fixed-width 1 / 2 / 4 / 8-byte
  integer-like buffers, not only byte buffers;
- `Fsst` uses a deterministic byte-pair symbol table codec and `Alp`
  uses a typed Float32 / Float64 common-exponent byte codec;
- fixed-width page-capable buffers emit multiple `page_end_offsets`;
- Dictionary selection / encode uses deterministic first-encounter
  dictionary order with hash lookups on the writer hot path;
- variable-width Dictionary selection is bounded to one
  `target_simd_page_values` page; multi-page variable-width buffers use
  `Fsst` when compressible, otherwise the normal `Plain` / `Lz4Frame`
  fallback, avoiding the multi-page Dictionary writer-cost spike;
- unmatched high-entropy buffers conservatively fall back to `Plain`
  when `Plain` is enabled; `Lz4Frame` remains selectable when `Plain`
  is removed from `enabled_cascades`;
- ADR 052 secondary-index selected-row reads have an Encoding page
  fast path for non-null fixed-width projected buffers;
- recovery / tier metadata extraction materializes Encoding-aware IPC
  before reading storage synthetic columns.

## Writer CPU

| Fixture | ADR 056-only | ADR 057 enabled | Delta |
| --- | ---: | ---: | ---: |
| 8,192 rows x 8 cols | 3,245.224 us/op | 5,669.577 us/op | +74.7% |
| 16,384 rows x 8 cols | 5,678.535 us/op | 10,208.499 us/op | +79.8% |
| 65,536 rows x 4 cols | 22,224.095 us/op | 38,184.080 us/op | +71.8% |

Readout:

- The largest legal representative fixture clears the <= 75% starting
  writer gate on repeated full runs after bounding variable-width
  Dictionary selection to one SIMD page. The two post-change full runs
  measured +71.1% and +71.8%.
- Threshold-margin fixtures remain above the gate. They are useful
  diagnostics, but the ADR gate is explicitly on the largest legal
  representative fixture.
- `enabled_cascades` ablation showed multi-page variable-width
  Dictionary was the writer-cost lever. The selector now avoids that
  default path; disabling `Dictionary` no longer changes the main
  65,536 x 4 fixture bytes (3,704,520) and is within bench noise of
  the default writer time.
- Disabling `Plain` forces the conservative fallback to `Lz4Frame`:
  the same 8,192 x 8 fixture measured 4,698.411 us/op and 224,208
  bytes. That proves a denser LZ4 fallback mode exists, but it is not
  the default because the `Plain` fallback now gives enough density
  while preserving the representative writer gate.

## Reader CPU

| Fixture | ADR 056-only full materialize | ADR 057 full materialize | Delta |
| --- | ---: | ---: | ---: |
| 8,192 rows x 8 cols | 636.144 us/op | 347.201 us/op | -45.4% |
| 16,384 rows x 8 cols | 1,204.094 us/op | 733.651 us/op | -39.1% |
| 65,536 rows x 4 cols | 3,753.470 us/op | 2,049.066 us/op | -45.4% |

## Directory / Body Size

| Fixture | ADR 056-only bytes | ADR 057 bytes | Reduction | Cascades | Buffer encodings |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8,192 rows x 8 cols | 1,230,152 | 860,376 | -30.1% | 4 | 38 |
| 16,384 rows x 8 cols | 2,459,272 | 1,723,624 | -29.9% | 6 | 76 |
| 65,536 rows x 4 cols | 7,308,680 | 3,704,520 | -49.3% | 18 | 208 |

| Fixture | Row-group metadata | Encoding metadata | Combined | Fits 64 KiB probe |
| --- | ---: | ---: | ---: | --- |
| 8,192 rows x 8 cols | 3,272 | 2,880 | 6,152 | yes |
| 16,384 rows x 8 cols | 6,432 | 5,624 | 12,056 | yes |
| 65,536 rows x 4 cols | 17,080 | 15,144 | 32,224 | yes |

## Page / Point Read

| Fixture | Full-buffer decode | SIMD-page decode | Decoded values | ADR 056 bytes | Encoded full-buffer bytes | SIMD-page bytes | Reduction vs ADR 056 | Reduction vs encoded full |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8,192 rows x 8 cols | 10.054 us/op | 2.428 us/op | 4096 -> 1024 | 32,768 | 6,656 | 1,664 | 19.69x | 4.00x |
| 16,384 rows x 8 cols | 10.028 us/op | 2.566 us/op | 4096 -> 1024 | 32,768 | 7,168 | 1,792 | 18.29x | 4.00x |
| 65,536 rows x 4 cols | 11.362 us/op | 2.833 us/op | 4096 -> 1024 | 32,768 | 8,192 | 2,048 | 16.00x | 4.00x |

The ADR 057 point-read target is met when selected bytes are measured
against the ADR 056 row-group-buffer baseline: all fixtures are >= 16x.
The encoded-full-buffer diagnostic remains 4x because this V1 fixture
has four 1024-value SIMD pages per 4096-row row group.

## Additional Phase 10 Axes

- Cross-replica determinism: byte-identical outputs for all three
  fixtures in this harness.
- Mixed pre-057 / post-057 read throughput:
  474.695 us/op (8,192 x 8), 955.370 us/op (16,384 x 8),
  3,113.245 us/op (65,536 x 4).
- Cascade selection CPU:
  64 sample rows = 18.749 us/op,
  256 sample rows = 20.843 us/op,
  1024 sample rows = 28.379 us/op.
- `target_simd_page_values` sweep on 8,192 x 8:
  512 = 860,600 bytes,
  1024 = 860,376 bytes,
  2048 = 860,232 bytes.
- Typed Phase 10 compression ratios vs. ADR 043 LZ4:
  int64 BitPacked = 2.504x,
  timestamp Delta = 4.231x,
  low-cardinality UTF-8 Dictionary = 3.597x,
  high-cardinality UTF-8 Fsst = 1.514x,
  float64 Alp = 1.577x.
- L2 GET-rate model: 1000 hits across 100 records coalesce from
  1000 row-group GETs to 100 page GETs (10x).
- End-to-end L2 page-fast-path evidence:
  `encoding_secondary_index_fetches_only_selected_simd_pages` tiers an
  encrypted slab, reads three secondary-index hits through L2, proves
  the Encoding plan fetches fewer SIMD-page ranges than selected rows,
  and materializes the expected projected values without fetching the
  whole encoded buffer.
- CSB short-circuit evidence: `aggregate_csb_direct_answer_handles_count_min_and_max`
  exercises a real aggregate CSB direct answer, and
  `test_adr057_encoding_guards` pins `try_aggregate_csb_direct_answer`
  ahead of projected-read planning / Encoding planner entry in
  `execute_aggregate_local`.

## Cascade Microbench

| Cascade | Encode | Decode | Encoded bytes |
| --- | ---: | ---: | ---: |
| plain | 0.069 us/op | 0.061 us/op | 4,096 |
| lz4_frame | 0.654 us/op | 1.161 us/op | 292 |
| bit_packed | 7.895 us/op | 6.266 us/op | 1,536 |
| rle | 1.423 us/op | 0.056 us/op | 5 |
| dictionary_bitpacked | 36.990 us/op | 12.720 us/op | 1,558 |
| delta | 15.589 us/op | 13.322 us/op | 8,216 |
| fsst | 35.980 us/op | 2.134 us/op | 3,093 |
| alp | 10.329 us/op | 16.629 us/op | 28,683 |
| dictionary_utf8_bitpacked | 48.049 us/op | 11.964 us/op | 1,066 |

## Disposition

ADR 057 is materially implemented, and the expanded harness now covers
the Phase 10 axes locally. The selector tightening changes the
writer/density trade: variable-width Dictionary remains implemented and
bench-proven, but the default selector uses it only for one-page
buffers so multi-page string columns do not dominate writer CPU.

Promotion readout:

- representative writer gate passes on repeat (+71.1%, +71.8% vs.
  <= 75%);
- typed Phase 10 compression-ratio targets pass;
- full materialization is faster than ADR 056-only on all three
  fixtures;
- point-read selected-byte reduction is >= 16x against the ADR 056
  row-group-buffer baseline;
- combined metadata stays within the 64 KiB initial probe;
- cross-replica determinism passes;
- secondary-index page fast path and CSB bypass are covered by
  end-to-end tests / source guards.

Residual note: the deterministic 1000-hit / 100-record L2 GET-rate
row remains a model in the bench harness rather than a 100-record
object-store integration benchmark. The manager-level L2 test exercises
the same page-fast-path planning and materialization on an encrypted
tiered slab.
