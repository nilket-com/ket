# ADR 002 Storage Execution Contract Bench Results

**Status:** Promotion-complete evidence.
**Bench target:** `crates/toron/benches/storage_execution.rs`
**Companion plan:** `crates/toron/plans/impl/002_toron_storage_backed_execution_contract.md`

This artifact records the executable ADR 002 evidence pass and the
completion audit for promotion.

## Evidence Model

ADR 002 requires a Wendelon-native fixture matrix, but Toron's bench
target must not import Wendelon's storage-manager internals or raw
storage readers. Promotion therefore uses a split matrix:

1. **Toron tiered bench rows** prove planner / executor / PBC /
   projection / aggregate parity across 8,192 / 65,536 /
   1,048,576 rows plus the 10,485,760 diagnostic tier, with
   `ScanRequest` instrumentation.
2. **Live Toron-to-Wendelon fixtures** prove the real
   `WendelonStorageScan` + `wendelon-client::table_query` path:
   schema install, table write, PBC + projection, declined pushdown
   fallback, grouped aggregate over storage-backed scans, descriptor
   pin mapping, and tenant isolation.
3. **Wendelon-native substrate fixtures** prove the storage-local
   mechanics ADR 002 depends on: sparse projection, page-level
   predicate stats, nullable sparse projection, encoded validity,
   all-null elision, and table-query stats pruning.

This is the promotion rule for ADR 002. A single unified Toron bench
that reaches into Wendelon's private fixture substrate would provide a
cleaner table shape but would violate the ADR's import boundary.

## Phase 0 Audit

- **P0.1 Wendelon public surface audit:** Toron production code uses
  Wendelon schema / PBC / client surfaces; no production imports of
  `wendelon::network::raft`, raw `wendelon::network::quic`, or
  `wendelon::storage::lsm` were found by source scan. The live
  integration test currently bootstraps Wendelon through public
  in-process node and simulated-transport APIs because no dedicated
  `wendelon::test_support` fixture surface exists yet; that dependency
  is test-only and does not enter Toron production code.
- **P0.2 Fingerprint availability:** `wendelon-client` now exposes
  `TableQueryOptions::physical_descriptor_fingerprint`, maps mismatch
  to `ClientError::TableDescriptorEvolved`, and Toron passes the
  pinned `ScanRequest.physical_descriptor_fingerprint` into table
  queries. Wendelon's table-query wire meta now also carries the
  physical descriptor fingerprint; server-side mismatch returns
  `ReadStatus::TABLE_DESCRIPTOR_EVOLVED`, which `wendelon-client` maps
  back to the typed descriptor-evolved error path.
- **P0.3 PBC opcode catalog:** Toron's V1 filter-pushdown op imports
  exist in `wendelon::schema::pbc`: comparisons, booleans, null tests,
  primitive literals, column push, and projection.
- **P0.4 Fixture corpus:** Current executable matrix includes the
  in-memory Toron bench shapes plus a live single-node
  Toron-to-Wendelon fixture that installs schema, writes rows through
  `wendelon-client`, reads through `WendelonStorageScan`, validates
  descriptor-evolved mapping, covers two-tenant isolation, compares a
  declined-pushdown `TimeBucket` filter against the pure in-memory
  baseline, and compares grouped aggregate output against the pure
  in-memory baseline.
  Wendelon-native substrate fixtures are bound below for sparse
  projection, page-level predicate stats, nullable sparse projection,
  encoded validity, all-null elision, and table-query stats pruning.
- **P0.5 Architecture guards:** `cargo test -p toron --test
  architecture_test` passes seven guards.
- **P0.6 Soft-gate calibration:** The in-memory bench now records the
  requested 8,192 / 65,536 / 1,048,576 row tiers plus the 10,485,760
  diagnostic tier. These rows are soft-gate calibration evidence; the
  Wendelon-native substrate rows below satisfy the native-fixture
  requirement without importing Wendelon internals into the Toron
  bench.
- **P0.7 BindingError taxonomy:** Implemented and covered by binder
  tests: unknown column, type mismatch, experimental kernel gate, and
  catalog failures.
- **P0.8 Cleanup:** Milestone-era production comments and seed test
  names were removed. The remaining seed-compatibility tests stay in
  `src/lib.rs` intentionally because they exercise private
  `#[cfg(test)]` shims for the legacy logical evaluator; relocating
  them before those shims retire would add public test-only surface
  without improving ADR 002 coverage.

## Verification Run

Commands run during this evidence pass:

```text
cargo test -p toron pbc
cargo test -p toron storage
cargo test -p toron --test architecture_test
cargo test -p toron
cargo test -p wendelon table_query_wire_contract_carries_descriptor_pin_and_pbc_only
cargo test -p wendelon table_query_request_validation_checks_descriptor_and_bytecode_len
cargo test -p wendelon query_failure_error_messages_name_terminal_query_statuses
cargo test -p wendelon table_query_dispatch_materializes_multirow_records_before_pbc
cargo test -p wendelon-client
cargo test -p toron --test wendelon_storage_execution_test
cargo check -p wendelon-client -p toron
cargo bench -p toron --bench storage_execution --no-run
cargo bench -p toron --bench storage_execution -- --quick --storage-execution-only
cargo bench -p toron --bench storage_execution -- --storage-execution-only --rows=8192,65536,1048576
cargo bench -p toron --bench storage_execution -- --storage-execution-only --rows=10485760
for test in \
  sparse_projection_fetch_materializes_exact_cocd_clustered_pages \
  encoding_predicate_page_plan_narrows_clustered_scan_pages \
  adr063_is_null_exact_mask_drives_nullable_sparse_projection \
  adr063_is_not_null_exact_mask_drives_nullable_sparse_projection \
  adr063_nullable_value_comparison_exact_mask_drives_sparse_projection \
  nullable_active_projection_column_fetches_page_validity_bitmap \
  rle_plain_encoded_validity_buffer_fetches_selected_validity_page \
  encoded_validity_detection_for_one_row_group_does_not_poison_another \
  nullable_all_null_page_elides_value_and_validity_fetches \
  nullable_all_null_elision_preserves_materialized_neighbor_pages \
  all_null_sparse_projection_disposition_materializes_null_page \
  table_query_skips_tiered_slab_when_column_stats_cannot_match; do \
  cargo test -p wendelon "$test"; \
done
```

Observed results:

```text
cargo test -p toron pbc
  6 unit tests passed; 1 architecture filter test passed; 0 failed

cargo test -p toron storage
  1 passed; 0 failed

cargo test -p toron --test architecture_test
  7 passed; 0 failed

cargo test -p toron
  45 unit tests passed; 7 architecture tests passed; 5 live storage
  execution tests passed; 0 failed

targeted Wendelon descriptor-pin wire/server tests
  all passed

cargo test -p wendelon-client
  3 passed; 0 failed

cargo test -p toron --test wendelon_storage_execution_test
  5 passed; 0 failed

cargo check -p wendelon-client -p toron
  finished successfully

cargo bench -p toron --bench storage_execution --no-run
  bench target built successfully

targeted Wendelon-native substrate fixture tests
  12 passed; 0 failed
```

The tiered in-memory bench emitted these rows:

| Case | Rows | PBC | Profile | Projection Cols | Baseline Bytes | Engaged Bytes | Speedup | Digest |
|---|---:|---|---|---:|---:|---:|---:|---|
| scan_projection | 8192 | false | Scan | 2 | 130376 | 130376 | 2.25 | `07d6302d15162ca8` |
| scan_filter | 8192 | true | Scan | 4 | 198152 | 83272 | 2.48 | `806895781d4bc85d` |
| filter_projection | 8192 | true | Scan | 2 | 100808 | 42440 | 0.94 | `8a6eb29c7a3bfdf3` |
| point_read_filter | 8192 | true | PointRead | 4 | 198152 | 1224 | 6.55 | `dbb83b1553b7b360` |
| count_all | 8192 | false | Scan | 0 | 200 | 200 | 2.25 | `b585b7acef38f2b1` |
| count_nullable | 8192 | false | Scan | 1 | 34184 | 34184 | 1.22 | `1332fc6a1cf7072d` |
| sum_nullable | 8192 | false | Scan | 1 | 34184 | 34184 | 1.07 | `1bec982d6b355b2a` |
| sum_all_null | 8192 | false | Scan | 1 | 34184 | 34184 | 1.05 | `080471c3f6f0ea45` |
| group_sum | 8192 | false | Scan | 2 | 97608 | 97608 | 0.97 | `30c3f2476114ef59` |
| group_null_key | 8192 | false | Scan | 1 | 63688 | 63688 | 0.91 | `50b794f755c6adf9` |
| scan_projection | 65536 | false | Scan | 2 | 1039304 | 1039304 | 2.50 | `984027e5524904bb` |
| scan_filter | 65536 | true | Scan | 4 | 1580168 | 671880 | 4.27 | `c3d24d34098fc178` |
| filter_projection | 65536 | true | Scan | 2 | 803272 | 341512 | 0.95 | `5bb40502bae554d2` |
| point_read_filter | 65536 | true | PointRead | 4 | 1580168 | 1224 | 18.85 | `dbb83b1553b7b360` |
| count_all | 65536 | false | Scan | 0 | 200 | 200 | 2.04 | `a53a1f68b110d75d` |
| count_nullable | 65536 | false | Scan | 1 | 270728 | 270728 | 1.10 | `47c3c4de90a807fc` |
| sum_nullable | 65536 | false | Scan | 1 | 270728 | 270728 | 1.10 | `0fab708c194588e5` |
| sum_all_null | 65536 | false | Scan | 1 | 270728 | 270728 | 0.95 | `080471c3f6f0ea45` |
| group_sum | 65536 | false | Scan | 2 | 777160 | 777160 | 1.05 | `4b2f76b13e73b8a4` |
| group_null_key | 65536 | false | Scan | 1 | 506696 | 506696 | 0.95 | `6d1c0e57eaf6e90b` |
| scan_projection | 1048576 | false | Scan | 2 | 16620488 | 16620488 | 2.32 | `937510666683822d` |
| scan_filter | 1048576 | true | Scan | 4 | 25271432 | 10808392 | 3.81 | `52e9a4146a25f098` |
| filter_projection | 1048576 | true | Scan | 2 | 12845512 | 5492360 | 1.07 | `21a0c65e4e511e11` |
| point_read_filter | 1048576 | true | PointRead | 4 | 25271432 | 1224 | 29.73 | `dbb83b1553b7b360` |
| count_all | 1048576 | false | Scan | 0 | 200 | 200 | 3.41 | `c9e4dd2b1269492f` |
| count_nullable | 1048576 | false | Scan | 1 | 4325768 | 4325768 | 1.08 | `91df1602bf972adc` |
| sum_nullable | 1048576 | false | Scan | 1 | 4325768 | 4325768 | 0.93 | `1b1c61b2c9637bc4` |
| sum_all_null | 1048576 | false | Scan | 1 | 4325768 | 4325768 | 1.00 | `080471c3f6f0ea45` |
| group_sum | 1048576 | false | Scan | 2 | 12426184 | 12426184 | 1.04 | `f6681985abd769c6` |
| group_null_key | 1048576 | false | Scan | 1 | 8100680 | 8100680 | 0.99 | `49a5fab4cc8e3d20` |
| scan_projection | 10485760 | false | Scan | 2 | 166199816 | 166199816 | 1.03 | `2ed468f5b422a07d` |
| scan_filter | 10485760 | true | Scan | 4 | 252707528 | 108117128 | 1.96 | `843c8950dcd3f555` |
| filter_projection | 10485760 | true | Scan | 2 | 128451016 | 54939464 | 1.47 | `67a9d8947a0d0949` |
| point_read_filter | 10485760 | true | PointRead | 4 | 252707528 | 1224 | 39.35 | `dbb83b1553b7b360` |
| count_all | 10485760 | false | Scan | 0 | 200 | 200 | 2.91 | `9530c3f8ee4eeafa` |
| count_nullable | 10485760 | false | Scan | 1 | 43254152 | 43254152 | 1.00 | `7ca02fcb93bd05cb` |
| sum_nullable | 10485760 | false | Scan | 1 | 43254152 | 43254152 | 1.05 | `96c6ebd870bbcc15` |
| sum_all_null | 10485760 | false | Scan | 1 | 43254152 | 43254152 | 1.00 | `080471c3f6f0ea45` |
| group_sum | 10485760 | false | Scan | 2 | 124256776 | 124256776 | 0.98 | `7cca7732486fdc9e` |
| group_null_key | 10485760 | false | Scan | 1 | 81002888 | 81002888 | 0.99 | `633aa1f487b2a661` |

Each row compares a bound no-PBC baseline against the engaged bound
path after canonical concat-and-reslice normalization at 65,536 rows.
The digest is the normalized Arrow IPC digest; mismatched digests fail
the bench harness.

## Wendelon-Native Substrate Fixture Matrix

The rows below are Wendelon-native fixture evidence, not Toron bench
rows. They bind ADR 002 to the storage substrate already shipped under
Wendelon ADRs while the live Toron fixture proves the cross-ring
`WendelonStorageScan` path.

| Substrate | Fixture | Evidence |
|---|---|---|
| ADR 060 sparse projection | `storage::manager::tests::sparse_projection_fetch_materializes_exact_cocd_clustered_pages` | Exercises L2 row-group sparse projection, validates range GETs, proves projected nullable validity fetch, and asserts selected projection bytes stay below the full payload range. |
| ADR 063 page-level predicate stats | `storage::manager::tests::encoding_predicate_page_plan_narrows_clustered_scan_pages` | Builds page-level predicate plan, observes disjoint / exact / maybe pages, and asserts page-plan bytes are materially smaller than dense-plan bytes. |
| ADR 063 table-query stats pruning | `network::data_server::tests::table_query_skips_tiered_slab_when_column_stats_cannot_match` | Runs table-query planning over tiered slab stats and proves impossible predicates avoid full L2 row ciphertext reads. |
| ADR 064 nullable sparse projection | `adr063_is_null_exact_mask_drives_nullable_sparse_projection`, `adr063_is_not_null_exact_mask_drives_nullable_sparse_projection`, `adr063_nullable_value_comparison_exact_mask_drives_sparse_projection`, `nullable_active_projection_column_fetches_page_validity_bitmap` | Proves exact masks from nullable predicates drive sparse projection and fetch the projected nullable column validity page. |
| ADR 069 encoded validity | `rle_plain_encoded_validity_buffer_fetches_selected_validity_page`, `encoded_validity_detection_for_one_row_group_does_not_poison_another` | Proves supported encoded-validity pages are selected and row-group-local encoded-validity detection does not poison neighboring row groups. |
| ADR 070 all-null page elision | `nullable_all_null_page_elides_value_and_validity_fetches`, `nullable_all_null_elision_preserves_materialized_neighbor_pages`, `all_null_sparse_projection_disposition_materializes_null_page` | Proves metadata-all-null pages elide value/validity fetches and materialize null pages without corrupting neighboring selected pages. |

## Completion Audit

Objective: implement ADR 002's storage-backed execution contract so
Toron executes bound, descriptor-pinned plans against Wendelon-backed
storage with PBC pushdown engaged when eligible, correct fallback when
declined, V1 aggregate correctness, structured errors, architecture
guards, and promotion evidence.

| Requirement | Evidence |
|---|---|
| Phase 0 pins | P0.1-P0.8 recorded above. |
| Structured errors | `crates/toron/src/error.rs`; `cargo test -p toron` error tests; architecture guard for public `Result<_, String>`. |
| IR split and binder | `crates/toron/src/ir.rs`, `crates/toron/src/ir/execution.rs`, `crates/toron/src/binder.rs`; binder unit tests cover resolved ordinals, projection union, type errors, unknown columns, profile hints, and experimental gating. |
| Catalog | `crates/toron/src/catalog.rs`; `MockCatalog` and descriptor-backed `WendelonCatalog` tests preserve table identity and physical fingerprint. |
| Runtime PBC engagement | `crates/toron/src/pbc.rs`, `crates/toron/src/binder.rs`, `crates/toron/src/exec.rs`, `crates/toron/src/storage.rs`; architecture test and bench rows observe `ScanRequest.pbc_predicate`; live Wendelon fixture returns the pushed-filter oracle. |
| Projection minimality | Binder / executor tests observe exact required-column unions; bench rows record projected-column cardinality; Wendelon-native sparse-projection fixtures prove storage-side byte narrowing. |
| GROUP BY and aggregates | Executor tests cover `CountAll`, `Count(expr)`, `Sum(expr)`, grouped sum/count, null keys, empty `COUNT(*)`, mixed/all-null groups, and all-null numeric `SUM`; live Wendelon fixture matches in-memory grouped aggregate output. |
| Descriptor fingerprint pin | `ScanRequest` requires a physical fingerprint; `WendelonStorageScan` validates local descriptors and passes the pin to `wendelon-client`; Wendelon table-query wire/server/client mapping tested; coordinator retry-once and double-fail tested. |
| Streaming boundary | `ScanStream` is stream-shaped; collection centralized in `coordinator.rs`; architecture guard enforces localization. |
| Architecture guards | `cargo test -p toron --test architecture_test` passes all seven guards. |
| Bench artifact and fixture matrix | This file records tiered Toron rows, live cross-ring fixtures, Wendelon-native substrate fixtures, hard-gate statuses, and soft-gate observations. |

## Gate Status

1. **PBC engagement observed:** Satisfied. In-memory bench rows and
   tests observe `ScanRequest.pbc_predicate` for eligible filters. A
   live single-node Wendelon fixture executes an eligible filter +
   projection through `WendelonStorageScan`, which would return the
   wrong rows if the pushed predicate were absent. The same fixture
   verifies tenant isolation for the storage request tenant id.
2. **Semantic parity:** Satisfied. The bench harness asserts
   normalized digest parity for tiered Toron shapes. The live Wendelon
   fixture checks hand-computed PBC output plus byte-identical IPC
   output against pure in-memory baselines for declined-pushdown and
   grouped-aggregate paths. Wendelon-native substrate fixtures have
   their own storage-local oracles.
3. **Declined-pushdown deterministic:** Satisfied by typed private
   `PushdownDeclineReason` and PBC decline tests that assert repeated
   compiler runs produce stable reasons.
4. **Declined-pushdown correctness:** Satisfied for the current live
   cross-ring V1 fixture. `TimeBucket` binds only with the experimental
   flag, declines PBC, attaches no PBC program to the scan, runs through
   Wendelon-backed storage plus Toron filter evaluation, and produces
   byte-identical Arrow IPC output to the pure in-memory baseline.
5. **Fingerprint-pin enforcement:** Satisfied. Toron
   `ScanRequest` requires descriptor pins, adapter checks descriptor
   identity, coordinator retries once, and `wendelon-client` exposes a
   physical-fingerprint option. Wendelon table-query wire meta carries
   the physical fingerprint and server-side mismatch reports
   `TABLE_DESCRIPTOR_EVOLVED`. The live Toron-to-Wendelon fixture now
   covers the server-side physical descriptor pin through the typed
   `ClientError::TableDescriptorEvolved` path. `MockCatalog` and
   descriptor-backed `WendelonCatalog` resolution are covered by
   catalog tests, including physical fingerprint preservation.
6. **Architecture guards:** Satisfied for the seven ADR 002 guards in
   `tests/architecture_test.rs`.
7. **Aggregate correctness:** Satisfied. `CountAll`,
   `Count(expr)`, `Sum(expr)`, grouped sum/count, null group keys, and
   all-null numeric `SUM` behavior are covered in Toron tests. Empty
   global `COUNT(*)` returns a single `0`; grouped `SUM` distinguishes
   all-null groups, mixed groups, and null-key groups. A live
   Wendelon-backed grouped aggregate fixture now matches the pure
   in-memory baseline byte-for-byte.
8. **Structured storage errors:** Satisfied. Typed error
   taxonomy, Wendelon-client origin mapping, and one live server-side
   descriptor-evolved mapping are implemented.
9. **Projection-pushdown minimality:** Satisfied. Binder tests
   and bench rows record exact required projection cardinalities.
   Wendelon-native sparse projection tests prove storage-side byte
   narrowing.
10. **Streaming boundary shape:** Satisfied for the V1 boundary guard:
    `ScanStream` is stream-shaped and collection is localized to
    `coordinator.rs`.

## Soft Gate Status

Soft gates are recorded diagnostics, not promotion blockers.

1. **Bytes-fetched reduction tracks selectivity:** Recorded in the
   tiered bench rows. `scan_filter`, `filter_projection`, and
   `point_read_filter` show lower engaged bytes when PBC engages.
2. **Wall-clock speedup:** Recorded in the tiered bench rows. Some
   in-memory projection-heavy rows do not reach the placeholder
   threshold, which is expected for a benchmark that removes network /
   storage latency. The row remains valuable as parity and byte-shape
   evidence; this does not block ADR 002 promotion.
3. **Range-count behavior:** Recorded through Wendelon-native
   substrate fixtures rather than the Toron bench because the public
   `table_query` / `WendelonStorageScan` contract does not expose
   storage-manager range counts. This preserves ADR 002's Wendelon
   import boundary.
4. **Binder-pass overhead:** Not isolated as a separate timing row in
   the bench harness. Every bench case binds before execution and the
   planner path is covered by unit tests; this remains diagnostic
   evidence only and does not block promotion.

## Unified Bench Deviations

The original Phase 8 sketch listed a single table with range count,
declined-pushdown reason, semantic digest, and cross-replica digest per
fixture. ADR 002 now uses the split evidence model instead:

- **Semantic digest:** recorded by the tiered Toron bench for each
  query shape; live Wendelon parity fixtures compare Arrow IPC bytes.
- **Declined reason:** covered by private `PushdownDeclineReason` unit
  tests; the reason remains crate-private by ADR design and is not a
  public metric label.
- **Range count:** covered by Wendelon-native substrate fixtures where
  range planning is observable without adding storage internals to
  Toron.
- **Cross-replica determinism:** deferred to ADR 003's distributed
  runtime scope. ADR 002's V1 boundary proves deterministic local
  planning / execution and descriptor-pin behavior; it does not create
  a multi-replica execution runtime.

Additional execution-path note: `ToronWorker` now binds inbound logical
plans with a catalog and dispatches the resulting `ExecutionPlan` to
`execute_bound`; the legacy logical evaluator is retained only under
`#[cfg(test)]` for seed-kernel coverage.

## Promotion Blockers

None. ADR 002 promotion uses the split evidence model recorded above.
