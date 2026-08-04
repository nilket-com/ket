# ADR 064 Nullable Sparse Projection Bench Results

Date: 2026-05-09
Status: Phase 7 bench evidence captured; promotion gates passed.

## Commands Run

```bash
cargo fmt --all
cargo check --benches
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
```

The full run emitted 27 ADR 064 rows from `benches/row_group_fetch.rs`:

```text
row_group_fetch_bench scenario=adr064_nullable_sparse_projection rows=mixed emitted_rows=27 iterations=max10_1m3
```

The quick and full bench logs were checked for `warning`, `error`, `panicked`, and `bench failed`; no matches were present.

## Promotion Gate: Clustered 5% Nullable Sparse Rows

Sparse plaintext bytes include selected value pages plus selected validity pages. Dense plaintext bytes are the existing dense value+validity fetch baseline for the projected column.

| Profile | Rows | Cascade | Dense Plaintext | Sparse Value | Sparse Validity | Sparse Total | Byte Reduction | Dense Total | Sparse Total | Wall-Clock Speedup |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| point_read | 8,192 | BitPacked | 1,888 | 320 | 32 | 352 | 5.36x | 0.398 ms | 0.228 ms | 1.75x |
| point_read | 8,192 | Dictionary | 984 | 94 | 32 | 126 | 7.81x | 0.327 ms | 0.218 ms | 1.50x |
| scan | 65,536 | BitPacked | 90,592 | 4,160 | 416 | 4,576 | 19.80x | 5.480 ms | 2.059 ms | 2.66x |
| scan | 65,536 | Dictionary | 32,736 | 1,222 | 416 | 1,638 | 19.98x | 5.487 ms | 1.827 ms | 3.00x |
| point_read | 1,000,000 | BitPacked | 30,208 | 5,120 | 512 | 5,632 | 5.36x | 4.861 ms | 0.891 ms | 5.45x |
| point_read | 1,000,000 | Dictionary | 15,744 | 1,504 | 512 | 2,016 | 7.81x | 3.531 ms | 0.928 ms | 3.80x |

Result: every clustered 5% BitPacked and Dictionary row exceeds the ADR 064 promotion gates of at least 1.5x plaintext-byte reduction and at least 1.2x wall-clock speedup.

## Correctness And Control Rows

| Requirement | Evidence | Result |
| --- | --- | --- |
| Output equality | All 26 runtime measurement rows report `equality=true`. | Pass |
| Null-count equality | All 26 runtime measurement rows report `null_count_equal=true`. | Pass |
| `None` mask zero fetch | `route=none_zero_fetch` rows report `sparse_value_plaintext_bytes=0`, `sparse_validity_plaintext_bytes=0`, `sparse_projection_plaintext_bytes=0`, and `sparse_get_range_calls=0`. | Pass |
| `All` mask dense decline | `route=dense_decline_all` rows preserve the dense baseline and fetch no separate sparse validity pages. | Pass |
| No-null row group | `null_distribution=no_null_row_group` reports `sparse_validity_plaintext_bytes=0` and `selected_validity_ranges=0`. | Pass |
| Non-byte page boundary | `page_values=1025` rows pass for mixed nullable/non-null projection and all-null selected page cases. | Pass |
| All-null selected page | `null_distribution=all_null_selected_page` reports `null_count=256`, `equality=true`, and `null_count_equal=true`. | Pass |
| Variable-width control | `projection_shape=nullable_plus_variable_width_control` uses `route=dense_decline_variable_width`. | Pass |
| Encoded-validity decline control | Encoded validity control uses `route=dense_decline_encoded_validity` with `decline=encoded_validity_buffer_unsupported`. | Pass |
| Cross-replica determinism | `scenario=adr064_nullable_sparse_projection_determinism` reports `deterministic=true` and matching left/right output digests. | Pass |

## ADR 063 Composability

The ADR 063 composability rows split the ExactMatch and Maybe cases and compare the combined ADR 063 + ADR 064 path against either ADR alone.

| Predicate Source | Dense Predicate Bytes | Narrowed Predicate Bytes | ADR 063 Only | ADR 064 Only | Combined | Combined Strictly Better |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `adr063_exact_match` | 1,145 | 0 | 1,888 | 1,465 | 320 | true |
| `adr063_maybe` | 1,145 | 144 | 2,032 | 1,497 | 496 | true |

Result: both ExactMatch and Maybe rows satisfy the Phase 7 requirement that combined savings beat either ADR alone.

## Promotion Decision

Phase 7 passes. The bench artifact provides clustered 5% byte and wall-clock evidence, equality/null-count checks, mask-shape controls, ADR 063 composability evidence, encoded-validity decline coverage, non-byte-boundary coverage, no-null/all-null coverage, variable-width decline coverage, and cross-replica determinism evidence.

ADR 064 is ready for Phase 8 promotion updates.
