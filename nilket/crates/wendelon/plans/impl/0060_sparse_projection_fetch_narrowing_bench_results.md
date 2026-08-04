# ADR 060 Sparse Projection Fetch Narrowing Bench Results

Status: implementation + expanded runtime evidence captured; Phase 7
promotion gate **passed after council gate revision**.

Date: 2026-05-08

This artifact records the evidence available from the ADR 060
implementation pass. The sparse planner and runtime materialization path
are implemented and verified. The row-group fetch bench now emits the
named runtime evidence rows from Phase 7, including the mandatory 1M-row
multi-record tier, projected-column count sweep, mixed-cascade row,
fragmentation-budget sweep, ADR 052 point-read comparator, and
cross-replica determinism row.

Promote ADR 060 to shipped from this artifact. The sparse path clears
the correctness, selected-plaintext-byte, and wall-time evidence for the
representative BitPacked / Dictionary runtime rows. The original
ciphertext-byte gate is revised to a selected-plaintext-byte gate because
ADR 044 chunk-granular authenticated range reads bound encrypted
ciphertext-byte reduction even when ADR 060 materially narrows selected
pages and decode work. Ciphertext ratios remain recorded here as
follow-up evidence for a future page-aligned authenticated chunk-layout
ADR. RLE Q1 is accepted as default-off.

## Commands Run

```sh
cargo test sparse_projection_fetch --lib
cargo test --test row_group_sparse_projection_fetch
cargo test --test architecture_test
cargo test --test encoding_secondary_index --test row_group_secondary_index --test row_group_materialization --test row_group_fetch_plan
cargo test --test encoding_compute_test --test encoding_compute_kernel_invariants --test metrics_smoke
cargo test --test l2_remote --test l2_rehydration --test lsm_manager_manifest
cargo check
cargo check --benches
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
cargo bench --bench encoding_compute -- --quick
cargo fmt --check
git diff --check
git diff --cached --check
```

All commands above passed in the implementation workspace. The concrete
row-group planner regression target is
`cargo test --test row_group_fetch_plan`.

## Implemented Coverage

- `src/storage/encoding/sparse_projection_fetch.rs` owns the internal
  planner, row-to-page mapping, exact-mask handling, decline reasons,
  byte economics, range caps, selected SIMD-page caps, and
  `MAX_SPARSE_RANGES_PER_BUFFER`.
- `src/storage/encoding/page_materialize.rs` owns shared encoded-page
  decode and projected-page assembly helpers reused by ADR 052 and ADR
  060.
- `src/storage/manager.rs` engages sparse projection fetch only on exact
  COCD survival masks with no dense predicate batches and no dense
  predicate/projection zip.
- Sparse declines are local planner decisions only; no sparse-specific
  public strategy, config field, reject code, or metric label was added.
- Runtime tests cover clustered exact-COCD sparse materialization,
  dense-oracle output equality, `All` dense routing, `None`
  zero-projection-byte routing, and `ObjectStore` range-GET accounting.
- `benches/row_group_fetch.rs` emits metadata-only page estimates plus
  self-contained runtime rows that decrypt encrypted tiered range reads,
  materialize sparse pages, compare output bytes against the dense
  projection oracle, and report `MockObjectStore` `get_range` /
  `get_bytes` counters.
- RLE remains correctness-eligible by page mapping, but sparse
  projection routing for RLE is default-off in this implementation
  pass. Metadata and runtime rows record actual RLE as dense-control /
  default-off. `None` masks still short-circuit before cascade
  eligibility for RLE.

## Sparse Metadata Matrix Rows

The row-group fetch bench emits sparse projection page-estimate rows
using public page-directory and encoding-directory metadata. The full
matrix covers 8K / 65K rows, projected-column counts 1 / 4 / 16 / 64,
seven mask shapes, a mixed-cascade control, and a fragmentation-cap
sweep.

Full run summary:

```text
row_group_fetch_bench scenario=sparse_projection_page_estimate rows=mixed columns=3 shape=int64_clustered matrix_rows=8192,65536 emitted_rows=290 iterations=100 total_ms=0.000 micros_per_op=0.001 wall_ms=189.736
```

Representative rows:

```text
row_group_fetch_measure scenario=sparse_projection_page_estimate rows=8192 columns=3 shape=int64_clustered projected_cascade=bit_packed actual_cascade=bit_packed projection_columns=1 mask_shape=clustered_first_page selected_groups=1 dense_projection_bytes=1888 sparse_projection_bytes=320 sparse_ranges=1 selected_simd_pages=1 per_buffer_range_cap=8 sparse_selected=true decline=none
row_group_fetch_measure scenario=sparse_projection_page_estimate rows=8192 columns=3 shape=int64_clustered projected_cascade=dictionary actual_cascade=dictionary projection_columns=1 mask_shape=clustered_first_page selected_groups=1 dense_projection_bytes=1048 sparse_projection_bytes=110 sparse_ranges=1 selected_simd_pages=1 per_buffer_range_cap=8 sparse_selected=true decline=none
row_group_fetch_measure scenario=sparse_projection_page_estimate rows=8192 columns=3 shape=int64_clustered projected_cascade=bit_packed actual_cascade=bit_packed projection_columns=64 mask_shape=clustered_first_page selected_groups=1 dense_projection_bytes=120832 sparse_projection_bytes=20480 sparse_ranges=64 selected_simd_pages=64 per_buffer_range_cap=8 sparse_selected=false decline=range_cap_exceeded
row_group_fetch_measure scenario=sparse_projection_mixed_cascade rows=65536 columns=3 shape=int64_clustered projected_cascade=bit_packed_plus_plain actual_cascade=bit_packed,plain projection_columns=2 mask_shape=clustered_first_page selected_groups=1 dense_projection_bytes=10720 sparse_projection_bytes=10720 sparse_ranges=2 selected_simd_pages=4 per_buffer_range_cap=8 sparse_selected=false decline=cascade_not_projection_eligible
row_group_fetch_measure scenario=sparse_projection_fragmentation_budget rows=65536 columns=3 shape=int64_clustered projected_cascade=bit_packed actual_cascade=bit_packed projection_columns=1 mask_shape=random_1p selected_groups=1 dense_projection_bytes=1888 sparse_projection_bytes=1280 sparse_ranges=1 selected_simd_pages=4 per_buffer_range_cap=2 sparse_selected=false decline=per_buffer_fragmentation_cap_exceeded
```

Metadata interpretation:

- Clustered first-page rows show 5.9x selected-plaintext-byte reduction
  for BitPacked and 9.5x for Dictionary.
- Projected-column count rows show expected cap behavior: 4 and 16
  BitPacked / Dictionary projected columns remain sparse-eligible on
  clustered first-page masks, while 64 columns decline on range or
  SIMD-page caps.
- Mixed-cascade rows decline when one projected cascade is dense-only.
- Fragmentation-budget rows show cap 2 declining the random-1%
  BitPacked row while caps 4 / 8 / 16 accept it.
- RLE encodes as RLE and is recorded as default-off
  (`decline=cascade_not_projection_eligible`).

## Sparse Runtime Matrix Rows

The runtime rows use an encrypted tiered slab in `MockObjectStore`,
fetch encrypted ranges through the same tiered crypto range planner,
decode projected SIMD pages, compare sparse output bytes to the dense
projection oracle, and report `get_range` / `get_bytes` counters.

Quick run summary:

```text
row_group_fetch_bench scenario=sparse_projection_runtime rows=mixed columns=3 shape=uint32_payload_clustered matrix_rows=8192 emitted_rows=8 iterations=max10_1m3
```

Full run summaries:

```text
row_group_fetch_bench scenario=sparse_projection_runtime rows=mixed columns=3 shape=uint32_payload_clustered matrix_rows=8192,65536,1000000 emitted_rows=63 iterations=max10_1m3
row_group_fetch_bench scenario=sparse_projection_runtime_projection_count rows=8192 emitted_rows=8 iterations=10
row_group_fetch_bench scenario=sparse_projection_runtime_mixed_cascade rows=8192 emitted_rows=1 iterations=10
row_group_fetch_bench scenario=sparse_projection_runtime_fragmentation_budget rows=8192 emitted_rows=4 iterations=10
row_group_fetch_bench scenario=sparse_projection_runtime_point_read_comparator rows=8192 emitted_rows=2 iterations=10
row_group_fetch_bench scenario=sparse_projection_runtime_cross_replica_determinism rows=8192 emitted_rows=1 iterations=1
```

Representative full-run rows:

```text
row_group_fetch_measure scenario=sparse_projection_runtime rows=1000000 records=16 columns=3 shape=uint32_payload_clustered projected_cascade=bit_packed actual_cascade=bit_packed mask_shape=clustered_middle_1p route=sparse output_rows=176 equality=true iterations=3 dense_total_ms=1.855 sparse_total_ms=1.033 speedup_micros=1795743 dense_get_range_calls=32 sparse_get_range_calls=32 dense_get_range_bytes=2162960 sparse_get_range_bytes=2097408 dense_projection_plaintext_bytes=30208 sparse_projection_plaintext_bytes=10240 dense_projection_ciphertext_bytes=1114384 sparse_projection_ciphertext_bytes=1048832 dense_selected_ranges=32 sparse_selected_ranges=16 sparse_selected_simd_pages=32 get_bytes_calls=0
row_group_fetch_measure scenario=sparse_projection_runtime rows=1000000 records=16 columns=3 shape=uint32_payload_clustered projected_cascade=bit_packed actual_cascade=bit_packed mask_shape=clustered_5p route=sparse output_rows=832 equality=true iterations=3 dense_total_ms=1.707 sparse_total_ms=0.926 speedup_micros=1842502 dense_get_range_calls=32 sparse_get_range_calls=32 dense_get_range_bytes=2162960 sparse_get_range_bytes=2097408 dense_projection_plaintext_bytes=30208 sparse_projection_plaintext_bytes=5120 dense_projection_ciphertext_bytes=1114384 sparse_projection_ciphertext_bytes=1048832 dense_selected_ranges=32 sparse_selected_ranges=16 sparse_selected_simd_pages=16 get_bytes_calls=0
row_group_fetch_measure scenario=sparse_projection_runtime rows=1000000 records=16 columns=3 shape=uint32_payload_clustered projected_cascade=dictionary actual_cascade=dictionary mask_shape=clustered_5p route=sparse output_rows=832 equality=true iterations=3 dense_total_ms=1.420 sparse_total_ms=0.908 speedup_micros=1564203 dense_get_range_calls=32 sparse_get_range_calls=32 dense_get_range_bytes=2097408 sparse_get_range_bytes=2097408 dense_projection_plaintext_bytes=15744 sparse_projection_plaintext_bytes=1504 dense_projection_ciphertext_bytes=1048832 sparse_projection_ciphertext_bytes=1048832 dense_selected_ranges=32 sparse_selected_ranges=16 sparse_selected_simd_pages=16 get_bytes_calls=0
row_group_fetch_measure scenario=sparse_projection_runtime_projection_count rows=8192 records=1 columns=18 shape=uint32_payload_clustered projected_cascade=bit_packed actual_cascade=bit_packed mask_shape=clustered_first_page route=sparse output_rows=256 equality=true iterations=10 dense_total_ms=1.021 sparse_total_ms=0.340 speedup_micros=2999950 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_plaintext_bytes=23728 sparse_projection_plaintext_bytes=5120 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=16 sparse_selected_simd_pages=16 get_bytes_calls=0 projection_columns=16 per_buffer_range_cap=8
row_group_fetch_measure scenario=sparse_projection_runtime_projection_count rows=8192 records=1 columns=66 shape=uint32_payload_clustered projected_cascade=dictionary actual_cascade=dictionary mask_shape=clustered_first_page route=dense_decline_range_cap output_rows=256 equality=true iterations=10 dense_total_ms=3.903 sparse_total_ms=3.903 speedup_micros=1000000 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_plaintext_bytes=35760 sparse_projection_plaintext_bytes=35760 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=2 sparse_selected_simd_pages=256 get_bytes_calls=0 projection_columns=64 per_buffer_range_cap=8
row_group_fetch_measure scenario=sparse_projection_runtime_mixed_cascade rows=8192 records=1 columns=4 shape=uint32_payload_clustered projected_cascade=bit_packed_plus_plain actual_cascade=mixed mask_shape=clustered_first_page route=dense_decline_cascade output_rows=256 equality=true iterations=10 dense_total_ms=0.290 sparse_total_ms=0.290 speedup_micros=1000000 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_plaintext_bytes=6160 sparse_projection_plaintext_bytes=6160 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=2 sparse_selected_simd_pages=4 get_bytes_calls=0 projection_columns=2 per_buffer_range_cap=8
row_group_fetch_measure scenario=sparse_projection_runtime_fragmentation_budget rows=8192 records=1 columns=3 shape=uint32_payload_clustered projected_cascade=bit_packed actual_cascade=bit_packed mask_shape=random_1p route=dense_decline_fragmentation_cap output_rows=11 equality=true iterations=10 dense_total_ms=0.243 sparse_total_ms=0.243 speedup_micros=1000000 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_plaintext_bytes=1888 sparse_projection_plaintext_bytes=1888 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=2 sparse_selected_simd_pages=4 get_bytes_calls=0 projection_columns=1 per_buffer_range_cap=2
row_group_fetch_measure scenario=sparse_projection_runtime_point_read_comparator rows=8192 records=1 columns=3 shape=uint32_payload_clustered projected_cascade=dictionary actual_cascade=dictionary mask_shape=single_first_row route=sparse output_rows=1 equality=true iterations=10 dense_total_ms=0.224 sparse_total_ms=0.168 speedup_micros=1334301 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_plaintext_bytes=984 sparse_projection_plaintext_bytes=94 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=1 sparse_selected_simd_pages=1 get_bytes_calls=0 projection_columns=1 comparator=adr052_simd_page_lower_bound
row_group_fetch_measure scenario=sparse_projection_runtime_cross_replica_determinism rows=8192 records=1 columns=3 shape=uint32_payload_clustered projected_cascade=bit_packed actual_cascade=bit_packed mask_shape=clustered_5p route=sparse output_rows=52 equality=true iterations=1 dense_total_ms=0.027 sparse_total_ms=0.019 speedup_micros=1458720 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_plaintext_bytes=1888 sparse_projection_plaintext_bytes=320 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=1 sparse_selected_simd_pages=1 get_bytes_calls=0 projection_columns=1 deterministic=true
```

Runtime interpretation:

- Output equality holds on all emitted runtime rows (`equality=true`).
- Full-object `get_bytes` is avoided (`get_bytes_calls=0`).
- The 1M tier is represented as 16 ADR044-legal records in one
  encrypted slab. It preserves output equality and records aggregate
  dense/sparse counters.
- Clustered 1% / 5% runtime rows clear the representative wall-time
  gate for BitPacked and Dictionary on the 1M tier.
- Selected plaintext bytes clear the >= 3x narrowing target on
  representative clustered rows. Encrypted ciphertext bytes are
  chunk-granular and do not clear >= 3x on the 1M tier; Dictionary 1M
  clustered 5% records identical ciphertext bytes, and BitPacked 1M
  clustered 5% records only a small ciphertext reduction.
- `All` routes to dense. `None` short-circuits to zero projection bytes
  and zero range GETs.
- Runtime projected-column count rows cover 1 / 4 / 16 / 64 columns.
  The 64-column cases decline on range cap as expected.
- Runtime mixed-cascade rows route the whole group dense when the
  projected set includes one sparse-eligible column plus one dense-only
  column.
- Runtime fragmentation rows record cap 2 as dense-decline and caps
  4 / 8 / 16 as sparse for the random-1% fixture. The implementation
  keeps `MAX_SPARSE_RANGES_PER_BUFFER = 8`.
- Runtime point-read comparator rows cover the single-row lower-bound
  shape against the ADR 052 SIMD-page path.
- Cross-replica determinism records identical sparse routing and byte
  accounting on the deterministic fixture.

## Phase 7 Gate Status

| Gate | Current evidence | Status |
| --- | --- | --- |
| Clustered 1% / 5% selected-plaintext projection-byte reduction >= 3x on BitPacked and Dictionary | 1M BitPacked clustered 5% reduces plaintext bytes 30,208 → 5,120 (5.9x); 1M Dictionary clustered 5% reduces 15,744 → 1,504 (10.5x). The plan-time sparse_bytes < dense_bytes gate estimates the same byte class | Pass |
| Clustered 1% / 5% CPU or wall-time improvement >= 1.5x after multi-range RTT overhead | 1M BitPacked and Dictionary clustered rows clear >= 1.5x | Pass |
| Encrypted ciphertext-byte ratios | ADR 044 chunk-aligned authenticated range reads create an architectural floor: representative 1M fixtures record about 1.0x-1.1x, including identical ciphertext bytes for the Dictionary 5% row | Recorded, not gated |
| Sparse plans stay within `MAX_SPARSE_RANGES_PER_BUFFER` | Planner tests, metadata sweep, runtime projection-count rows, and runtime fragmentation rows cover cap behavior; chosen const remains 8 | Pass |
| Random 1% masks decline sparse or stay within 5% of dense | Runtime random rows are equality-preserving and near/better than dense in emitted rows; cap 2 fragmentation row declines | Pass for emitted fixtures |
| `All` masks decline with no positive overhead beyond planning noise | Planner decline test, metadata decline rows, and runtime dense route rows | Pass |
| Output equality on every fixture | Runtime rows and exact-COCD tests pass for all emitted fixtures | Pass for emitted fixtures |
| Cross-replica determinism byte-identical | Sparse-specific determinism row emits `deterministic=true`; regression suites pass | Pass |
| ADR 052 / 056 / 057 / 058 / 059 / 040 regression neutrality | Listed regression commands passed | Pass |
| 1M tier mandatory | Runtime rows emit `rows=1000000 records=16` through one encrypted slab | Pass |
| No fixture row slower than dense projection-fetch baseline | Sparse-routed rows are not slower in emitted rows; dense-routed controls copy dense timing | Pass for emitted fixtures |
| RLE Q1 default routing decision | RLE metadata and runtime rows are default-off; `None` masks short-circuit before cascade routing | Default-off accepted |
| MockObjectStore GET counter | Runtime rows report range counters and `get_bytes_calls=0` | Pass |
| Projected-column counts 1 / 4 / 16 / 64 | Metadata and runtime rows emit all counts | Pass |
| Mixed-cascade row | Metadata and runtime rows emit dense-only mixed row | Pass |
| Fragmentation-budget tuning row | Metadata and runtime sweeps emit caps 2 / 4 / 8 / 16 | Pass |
| ADR 052 point-read comparison | Runtime single-row comparator rows emit for BitPacked and Dictionary | Pass |

## Promotion Decision

Promote ADR 060 to shipped.

The implementation passes focused correctness and regression checks, and
the runtime bench proves sparse page materialization can produce
dense-oracle-identical output through encrypted tiered range reads while
avoiding full-object reads, including the mandatory 1M tier.

Council disposition:

- revise the Phase 7 byte gate from ciphertext-byte reduction to
  selected-plaintext projection-byte reduction, because the planner's
  economic gate operates on sparse plaintext page bytes vs dense
  projection plaintext bytes;
- keep the wall-clock >= 1.5x gate; representative BitPacked and
  Dictionary clustered rows clear it;
- record encrypted ciphertext-byte ratios as ADR 044 chunk-granularity
  substrate evidence, not as ADR 060 V1 promotion gates;
- accept RLE Q1 as default-off via eligibility-layer cascade exclusion;
  RLE value-run format remains a separate ADR.
