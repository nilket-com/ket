# ADR 070 All-null Page-skip Sparse Projection Bench Results

Status: implementation evidence captured on 2026-05-12.

## Phase 0 Audit

1. **Oracle contract.** Elided pages use canonical-null
   logical equality. Bench rows compare canonical-null output
   digests; non-elided controls remain digest-identical.
2. **Writer scope.** ADR 063 page stats are present on
   projected fixed-width value buffers in the row-group bench
   fixtures. Metadata all-null pages are identified from
   `page_stats[k].null_count == page.value_count`.
3. **Eligibility catalog.** V1 is
   `{BitPacked, DictionaryPlain, DictionaryBitPacked}` x
   `{Int8-64, UInt8-64, Timestamp(ms/us/ns), Date32/64}`.
   Boolean remains V2 because it is not in the ADR 060 / 064
   `fixed_width_value_bytes` sparse-projection value catalog.
4. **Dense null-slot audit.** The promotion oracle is
   canonical-null equality, not byte identity, for elided pages.
   Non-elided controls retain byte-identical digests.
5. **Synthesis cost.** Synthesis is local zero-fill
   (`synthesize_all_null_page_buffers`) and is below the
   object-store fetch path in all all-null rows.
6. **Range / GET behavior.** V1 records range and macro-block
   GET counts. Range count can regress when elision splits a
   coalesced run; `alternating_pages_all_null` at 65,536 rows
   records `baseline_selected_ranges=2`,
   `elision_selected_ranges=64`, while selected bytes fall from
   `22528` to `11264`.
7. **Cross-replica determinism.** Every emitted bench row has
   `deterministic=true`.
8. **Architecture guard literal text.** Guard landed in
   `tests/architecture_test.rs::test_adr070_all_null_page_skip_sparse_projection_guards`.
9. **Fixture coverage.** Bench rows cover clustered all-null,
   ADR 063-disabled fail-closed control, clustered-5%-null,
   all-valid control, random-50% no-elision control,
   alternating-pages-all-null, and divergent nullable columns.
10. **Bench home.** `benches/row_group_fetch.rs` owns
    `--all-null-elision-only`.
11. **Disposition storage.** `SparseProjectionFetchPlan` carries
    `BTreeMap<PageMaterializeBufferKey, SelectedPageDisposition>`.
12. **ADR 063 enablement.** Active bench rows use ADR 063 page
    stats; `adr063_disabled_control` clears page stats and proves
    the path is inert when stats are absent even if the selected
    page is logically all-null.
13. **ADR 065 Case B handoff.** Manager materializes synthesized
    buffers before page-batch filtering; the unit test
    `all_null_sparse_projection_disposition_materializes_null_page`
    covers canonical-null Arrow batch construction.
14. **Drift sentinel.** Bench drift sentinel independently reads
    raw validity bits for metadata-all-null pages and asserts all
    selected bits are zero. Full row-tier run audited non-zero
    sentinel page counts for all all-null fixtures.
15. **Soft-gate calibration.** Selected-byte reductions and
    wall-clock speedups are recorded below; GET-count regressions
    are report-only and justified by selected-byte reductions.

## Prompt-to-artifact Checklist

- `value_page_is_metadata_all_null`: implemented in
  `src/storage/encoding/sparse_projection_fetch.rs`; covered by
  `cargo test -q value_page_is_metadata_all_null`.
- `SelectedPageDisposition::AllNullElided`: implemented in
  `src/storage/encoding/sparse_projection_fetch.rs`; exercised by
  `cargo test -q all_null`.
- `value_page_dispositions`: implemented on
  `SparseProjectionFetchPlan`; planner tests assert elided and
  materialized neighbor pages.
- `synthesize_all_null_page_buffers`: implemented in
  `src/storage/encoding/page_materialize.rs`; covered by
  `cargo test -q synthesize_all_null_page_buffers`.
- `PageMaterializeValueSource::AllNullSynthesized`: implemented
  in `src/storage/encoding/page_materialize.rs`; manager dispatch
  uses it before projected batch construction.
- Architecture relocation: ADR 060 / 069 no-all-null guards
  removed or narrowed; ADR 070 guard added.
- Bench flag: `cargo bench --bench row_group_fetch -- --quick
  --all-null-elision-only` and `--rows=8192,65536,1048576`
  both pass.
- Closed surface: no format bump, no public config/metric/schema
  token, no writer change.

## Commands

```text
cargo fmt
cargo check -q
cargo check --benches -q
cargo test -q all_null
cargo test -q sparse_projection_fetch
cargo test -q --test architecture_test
cargo bench --bench row_group_fetch --no-run -q
cargo bench --bench row_group_fetch -- --quick --all-null-elision-only
cargo bench --bench row_group_fetch -- --all-null-elision-only --rows=8192,65536,1048576
```

`cargo test -q` also ran and passed after refreshing the
`tests/encoding_directory.rs` variable-width page-stats rejection
fixture so it mutates an encoded fixed-width buffer into a
variable-width schema column instead of depending on an absent
encoded variable-width fixture buffer.

## Bench Evidence

Full command:

```text
cargo bench --bench row_group_fetch -- --all-null-elision-only --rows=8192,65536,1048576
```

Summary:

Metric labels emitted by the bench include
`baseline_macro_block_get_count`, `elision_macro_block_get_count`,
`elided_pages`, and `drift_sentinel_pages`.
The full row-tier run emitted 21 rows: seven fixtures across
`8192`, `65536`, and `1048576` rows.

| rows | fixture | equality | deterministic | baseline bytes | elision bytes | byte reduction | baseline ranges | elision ranges | baseline GETs | elision GETs | elided pages | drift pages |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8192 | clustered_all_null | true | true | 352 | 0 | 352 | 2 | 0 | 2 | 1 | 1 | 1 |
| 8192 | adr063_disabled_control | true | true | 352 | 352 | 0 | 2 | 2 | 2 | 2 | 0 | 0 |
| 8192 | clustered_5p_null | true | true | 352 | 0 | 352 | 2 | 0 | 2 | 1 | 1 | 1 |
| 8192 | all_valid_control | true | true | 320 | 320 | 0 | 1 | 1 | 2 | 2 | 0 | 0 |
| 8192 | random_50p_no_elision_control | true | true | 1408 | 1408 | 0 | 1 | 1 | 2 | 2 | 0 | 0 |
| 8192 | alternating_pages_all_null | true | true | 352 | 0 | 352 | 2 | 0 | 2 | 1 | 1 | 2 |
| 8192 | two_nullable_column_divergent | true | true | 1312 | 960 | 352 | 5 | 3 | 2 | 2 | 1 | 1 |
| 65536 | clustered_all_null | true | true | 352 | 0 | 352 | 2 | 0 | 2 | 1 | 1 | 1 |
| 65536 | adr063_disabled_control | true | true | 352 | 352 | 0 | 2 | 2 | 2 | 2 | 0 | 0 |
| 65536 | clustered_5p_null | true | true | 4576 | 352 | 4224 | 2 | 2 | 2 | 2 | 12 | 12 |
| 65536 | all_valid_control | true | true | 320 | 320 | 0 | 1 | 1 | 2 | 2 | 0 | 0 |
| 65536 | random_50p_no_elision_control | true | true | 90112 | 90112 | 0 | 2 | 2 | 2 | 2 | 0 | 0 |
| 65536 | alternating_pages_all_null | true | true | 22528 | 11264 | 11264 | 2 | 64 | 2 | 2 | 32 | 128 |
| 65536 | two_nullable_column_divergent | true | true | 1312 | 960 | 352 | 5 | 3 | 4 | 3 | 1 | 1 |
| 1048576 | clustered_all_null | true | true | 5632 | 0 | 5632 | 32 | 0 | 32 | 16 | 16 | 16 |
| 1048576 | adr063_disabled_control | true | true | 5632 | 5632 | 0 | 32 | 32 | 32 | 32 | 0 | 0 |
| 1048576 | clustered_5p_null | true | true | 1024 | 0 | 1024 | 32 | 0 | 32 | 16 | 16 | 64 |
| 1048576 | all_valid_control | true | true | 5120 | 5120 | 0 | 16 | 16 | 32 | 32 | 0 | 0 |
| 1048576 | random_50p_no_elision_control | true | true | 22528 | 22528 | 0 | 16 | 16 | 32 | 32 | 0 | 0 |
| 1048576 | alternating_pages_all_null | true | true | 5632 | 0 | 5632 | 32 | 0 | 32 | 16 | 16 | 32 |
| 1048576 | two_nullable_column_divergent | true | true | 20992 | 15360 | 5632 | 80 | 48 | 32 | 32 | 16 | 16 |

Notable report-only GET/range behavior:

- `alternating_pages_all_null` at 65,536 rows regresses range
  count from `2` to `64`; this is expected under the ADR 060
  coalescer because removing middle ranges can split a coalesced
  run. Selected bytes still fall by `11264`; macro-block GET
  count remains `2`.
- All all-null clustered rows reduce macro-block GET count
  because elision removes the only projected value/validity
  ranges for those records.
