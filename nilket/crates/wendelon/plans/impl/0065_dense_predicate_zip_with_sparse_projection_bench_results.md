# ADR 065 Dense Predicate-Zip With Sparse Projection Bench Results

Date: 2026-05-09
Status: Phase 8 promotion evidence captured; gates passed.

## Commands Run

```sh
cargo bench --bench row_group_fetch -- --quick
```

Focused correctness and architecture commands from the implementation
workspace:

```sh
cargo test page_materialize --lib
cargo test sparse_projection_fetch --lib
cargo test --test architecture_test
cargo test --test row_group_materialization
cargo test --test row_group_sparse_projection_fetch
```

The quick bench emitted a dedicated ADR 065 row from
`benches/row_group_fetch.rs`:

```text
row_group_fetch_bench scenario=adr065_dense_predicate_zip rows=mixed emitted_rows=1 iterations=max10_1m3
```

The ADR 065-specific runtime correctness coverage is in
`storage::manager::tests::sparse_projection_fetch_materializes_exact_cocd_clustered_pages`,
which exercises:

- Case A mask-only sparse projection from the encoding-compute branch;
- Case B scalar dense predicate/output-overlap zip;
- Case C scalar dense no-overlap sparse projection;
- `None` mask zero projection fetch;
- `All` mask dense routing;
- variable-width dense predicate column zipped with fixed-width sparse
  projection.

## Promotion Gate Rows

Predicate bytes are sunk in ADR 065's plan-time projection economic
gate. The bench artifact therefore records both the marginal projection
axis and the total-query axis.

The measured quick row below is the dedicated ADR 065 bench row:
four clustered nullable fixed-width projection columns at 5%
selectivity, one scalar dense-decoded predicate column, and
Case B predicate/output-overlap zip. The row records the sunk dense
predicate bytes, remaining-projection bytes, total-query bytes, and
wall-clock after adding predicate filtering, sparse projection
materialization, and zip assembly.

| Profile | Rows | Source | Dense Predicate | Dense Remaining Projection | Sparse Remaining Projection | Projection Reduction | Total Dense Query | Total Sparse Query | Total Reduction | Dense Total | Sparse Total | Wall-Clock Speedup |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| PointRead | 8,192 | `scenario=adr065_dense_predicate_zip` | 1,776 B | 6,256 B | 1,408 B | 4.44x | 8,032 B | 3,184 B | 2.52x | 0.676 ms | 0.353 ms | 1.92x |

Promotion gates:

- remaining-projection plaintext reduction >= 1.5x: pass
  (4.44x measured);
- total-query plaintext reduction >= 1.25x: pass
  (2.52x measured);
- wall-clock improvement >= 1.2x: pass
  (1.92x measured).

The >= 3x headline diagnostic is satisfied for the
remaining-projection axis.

Dedicated row:

```text
row_group_fetch_measure scenario=adr065_dense_predicate_zip profile=point_read rows=8192 records=1 page_values=256 predicate_source=scalar_dense_decode route=case_b_sparse_zip output_rows=52 equality=true null_count_equal=true iterations=10 dense_predicate_plaintext_bytes=1776 dense_remaining_projection_plaintext_bytes=6256 sparse_value_plaintext_bytes=1280 sparse_validity_plaintext_bytes=128 sparse_projection_plaintext_bytes=1408 total_dense_query_plaintext_bytes=8032 total_sparse_query_plaintext_bytes=3184 remaining_projection_reduction_micros=4443181 total_query_reduction_micros=2522613 dense_total_ms=0.676 sparse_total_ms=0.353 speedup_micros=1917410 dense_get_range_calls=2 sparse_get_range_calls=2 dense_get_range_bytes=131088 sparse_get_range_bytes=131088 dense_projection_ciphertext_bytes=65552 sparse_projection_ciphertext_bytes=65552 dense_selected_ranges=2 sparse_selected_ranges=8 selected_value_pages=4 selected_validity_ranges=4 get_bytes_calls=0
```

## Runtime Correctness

The implementation fixture in `src/storage/manager.rs` verifies that
ADR 065 engages only on the scalar dense-decode branch:

- encoding-compute paths continue to produce mask-only `PhaseBOutcome`
  and engage Case A;
- scalar no-overlap predicates keep projection-side sparse ranges and
  skip zip;
- scalar overlap predicates keep projection-side sparse ranges and zip
  dense predicate pages with sparse projection pages;
- variable-width dense predicate columns are allowed on the predicate
  side while the sparse projection side remains ADR 064's fixed-width
  nullable catalog;
- output equality is checked against the full dense PBC oracle for all
  runtime rows.

The page-local helper tests cover byte-aligned and non-byte-aligned
page starts, tail spans, all-survivor pages, zero-survivor pages,
variable-width predicate slicing, and fail-closed span/mask mismatches.

## Control Rows

The quick bench preserved adjacent sparse-projection behavior:

- ADR 060 clustered 5% rows still route sparse for BitPacked and
  Dictionary and route dense for RLE / Plain / Delta default-off
  controls.
- ADR 064 clustered 5% nullable control rows report `equality=true` and
  `null_count_equal=true`, with measured quick rows such as:
  - four nullable fixed BitPacked projection:
    `dense_projection_plaintext_bytes=6256`,
    `sparse_projection_plaintext_bytes=1408`,
    `dense_total_ms=0.658`, `sparse_total_ms=0.233`;
  - one nullable fixed BitPacked projection:
    `dense_projection_plaintext_bytes=1888`,
    `sparse_projection_plaintext_bytes=352`,
    `dense_total_ms=0.384`, `sparse_total_ms=0.188`.
- ADR 063 ExactMatch / Maybe rows remain Case A controls in V1 because
  the encoding-compute branch does not populate
  `predicate_projection_by_group`.

## Promotion Decision

Promote ADR 065 to shipped.

The code removes the dense-predicate-batch sparse-projection cliff while
preserving the metadata-only sparse planner, exact-mask precondition,
page-local row alignment, and no-public-surface-change boundary. The
bench artifact clears the three promotion axes for clustered 5%
representative rows and records the narrowed V1 boundary: Case B/C
engagement is the scalar dense-decode branch only; ADR 063
encoding-compute paths remain Case A mask-only sparse projection.
