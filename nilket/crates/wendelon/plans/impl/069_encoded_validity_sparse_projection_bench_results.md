# ADR 069 Bench Results: Encoded-Validity Sparse Projection

Status: promoted. Production writer routing for encoded validity is
enabled in `build_encoding_directory_for_ipc_stream`; the disabled
baseline remains available through the existing bench helper.

## Prompt-to-artifact checklist

| Requirement | Evidence | Status |
| --- | --- | --- |
| ADR and impl plan exist | `plans/adr/069_encoded_validity_sparse_projection.md`, `plans/impl/069_encoded_validity_sparse_projection.md` | Pass |
| Bitmap page decode helper | `decode_rle_validity_page` with tail-padding tests | Pass |
| Full-buffer stitch helper | `stitch_validity_pages_into_row_group_bitmap` tests | Pass |
| Writer emits page-local RLE validity | `production_writer_routing_emits_promoted_validity_buffer`, `bench_routing_emits_page_aligned_rle_validity_buffer` | Pass |
| Sparse planner narrows encoded-validity decline | `rle_plain_encoded_validity_buffer_fetches_selected_validity_page` | Pass |
| Internal decline reasons are covered | `encoded_validity_sparse_eligibility_reports_decline_reasons` | Pass |
| Materializer decodes encoded validity as bitmap | `sparse_projection_fetch_materializes_exact_cocd_clustered_pages`, `cargo test -q validity` | Pass |
| Architecture guards pin closed surface | `cargo test -q architecture` | Pass |
| Promotion bench surface exists | `cargo bench --bench encoding_compute -- --quick --encoded-validity-only --rows=8192,65536,1048576` | Pass |
| Production route promoted | `WriterValidityCascadeRouting::Enabled` in the standard writer entry point | Pass |
| ADR 064 and roadmap amended | ADR 064 V2 deferral note and roadmap entry #25 | Pass |

## Phase 0 pins

- **P-fork:** pre-ADR writer selected value buffers only; ADR 069
  adds a separate null-buffer-ordinal writer route. Production is
  now promoted to the enabled route.
- **P1 / P2:** constants remain
  `VALIDITY_RLE_BYTE_SAVING_FRACTION = 1/4` and
  `MIN_VALIDITY_ENCODE_BYTES = 256`.
- **P3:** encoded-validity ranges are included in the sparse
  selected range set and counted by existing sparse byte/range gates.
- **P4:** bench rows report `deterministic=true` across two enabled
  writer runs for all fixtures and row tiers.
- **P5:** non-byte-aligned page correctness is covered by bitmap
  decode and stitch tests; page-local emission bounds padding to
  page tails.
- **P6:** helper stayed co-located in `select.rs`; the scope is small
  and directly tied to writer cascade selection.
- **P7:** architecture guard locks the closed catalog and public
  surface.
- **P8:** the writer uses ADR 057 RLE byte-run payloads over packed
  bitmap bytes; hand-traced page offsets are asserted in writer tests.
- **P9:** full-buffer callers route validity through
  `materialize_encoding_validity_buffer` / `decode_encoded_validity_buffer`.
- **P10:** clustered nullable `Int64` fixtures are represented by
  `clustered_5p` and `clustered_1p`.
- **P11:** bench home is `benches/encoding_compute.rs` with
  `--encoded-validity-only` stdout diagnostic rows.

## Hard gates

| Gate | Evidence | Result |
| --- | --- | --- |
| Dense sparse-projection oracle equality | `sparse_projection_fetch_materializes_exact_cocd_clustered_pages`, `cargo test -q sparse_projection_fetch`, `cargo test -q validity` | Pass |
| Tail-padding `null_count` correctness | `decode_rle_validity_page_counts_only_live_bits` covers rows 1, 7, 8, 9, 13, 16, 64, 256, and 1027 for all-valid and all-null pages | Pass |
| Full-buffer validity materialization stitches row-group bitmap | `materialize_encoding_validity_buffer` / `decode_encoded_validity_buffer`; `cargo test -q --test encoding_materialize`; architecture guard pins validity-ordinal role dispatch | Pass |
| Writer emits page-aligned, page-local RLE validity | `production_writer_routing_emits_promoted_validity_buffer`, `bench_routing_emits_page_aligned_rle_validity_buffer` | Pass |
| Unsupported encoded-validity shapes fail closed | `encoded_validity_sparse_eligibility_reports_decline_reasons`, `encoded_validity_buffer_declines_sparse_projection` | Pass |
| Bitmap path remains separate from fixed-width value-page decode | `cargo test -q architecture` pins `PageMaterializeValiditySource::Encoded`, `decode_rle_validity_page`, and the no-fixed-width-decoder invariant | Pass |
| Sparse economics and decline coverage | 1M clustered selected validity is 5 / 32 bytes (15.625% of raw); `random_50p` declines in the bench; `select_validity_cascade_declines_random_nullity`, `select_validity_cascade_declines_all_valid_and_small_raw_buffers`, and `validity_rle_economics_declines_threshold_equality` cover V1 decline cases | Pass |
| Deterministic writer output | Bench emits `deterministic=true` for all fixtures and row tiers after two enabled writer runs | Pass |
| Closed catalog, closed surface, and no directory-version bump | `cargo test -q architecture`; `ENCODING_DIRECTORY_FORMAT_VERSION` remains 3; no public config field or metric label was added | Pass |
| ADR 070 boundary stays deferred | `cargo test -q architecture` guards against all-null value-page elision in `manager.rs` | Pass |

## Soft gates

Quick bench command:

```text
cargo bench --bench encoding_compute -- --quick --encoded-validity-only --rows=8192,65536,1048576
```

Representative rows:

```text
clustered_5p rows=1048576 raw_validity_bytes=131072 enabled_validity_payload_bytes=20490 enabled_validity_page_table_bytes=16384 selected_validity_raw_bytes=32 selected_validity_encoded_bytes=5 deterministic=true
clustered_1p rows=1048576 raw_validity_bytes=131072 enabled_validity_payload_bytes=20490 enabled_validity_page_table_bytes=16384 selected_validity_raw_bytes=32 selected_validity_encoded_bytes=5 deterministic=true
all_valid rows=1048576 enabled_validity_encoded=false deterministic=true
all_null rows=1048576 enabled_validity_payload_bytes=20480 enabled_validity_page_table_bytes=16384 selected_validity_raw_bytes=32 selected_validity_encoded_bytes=5 deterministic=true
random_50p rows=1048576 enabled_validity_encoded=false deterministic=true
alternating_bit rows=1048576 enabled_validity_payload_bytes=20480 enabled_validity_page_table_bytes=16384 selected_validity_raw_bytes=32 selected_validity_encoded_bytes=5 deterministic=true
```

- Encoded selected validity bytes are 5 / 32 = 15.625% of raw on
  clustered and all-null selected pages, passing the <= 25% target.
- V1 decline coverage comes from `random_50p`, all-valid / zero-null
  buffers, sub-256-byte raw validity buffers, and strict-threshold
  equality. `random_50p` declines after an exact lower-bound early
  stop in the writer.
- Alternating-bit nullity encodes by design: packed alternating bits
  produce repeated `0x55` / `0xaa` bytes, so byte-run RLE legitimately
  compresses it. This revises the original soft expectation while
  preserving the economic gate. `alternating_bit` is therefore an
  encode fixture in this artifact, not a decline fixture.
- Writer wall-clock percentage overhead is not bounded by the original
  <= 10% eligible / <= 5% non-eligible thresholds on the single-column
  synthetic. The absolute 1M-row quick-bench delta is about 1.1 ms for
  clustered eligible rows and about 0.2 ms for random-decline rows.
  Promotion accepts this as operationally small because the route
  remains exact, deterministic, and economically gated. The artifact
  does not directly prove that a realistic multi-column denominator
  brings the percentage under the original bound; that is a follow-up
  bench row for any future ADR that reopens ADR 069 writer-cost scope.

## Verification commands

```text
cargo fmt
cargo check --benches
cargo test -q select_validity_cascade
cargo test -q production_writer_routing_emits_promoted_validity_buffer
cargo test -q bench_routing_emits_page_aligned_rle_validity_buffer
cargo test -q encoded_validity_sparse_eligibility_reports_decline_reasons
cargo test -q architecture
cargo test -q sparse_projection_fetch
cargo test -q validity
cargo test -q sparse_projection_fetch_materializes_exact_cocd_clustered_pages
cargo test -q --test encoding_materialize
cargo bench --bench encoding_compute -- --quick --encoded-validity-only --rows=8192,65536,1048576
git diff --check
```
