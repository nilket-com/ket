# Implementation Plan: ADR 013 - Runtime Filter Application at the Storage-Scan Boundary

**Companion ADR:**
`plans/adr/013_runtime_filter_application_at_the_storage_scan_boundary.md`

## Status

Promoted + implemented.

This is a real cross-crate implementation: Toron storage, Wendelon schema,
Wendelon client plumbing, Wendelon wire decode, Wendelon scan execution, and
bench evidence all participate.

## Goal

Fulfill the existing `ScanRequest.runtime_filter` contract in production
scan implementations:

- `InMemoryStorageScan` applies usable runtime filters locally.
- `WendelonStorageScan` forwards usable runtime filters to Wendelon table
  query.
- Wendelon applies runtime filters during table-query scan materialization.

The implementation must preserve the advisory invariant: an absent or
unusable runtime filter produces byte-identical results to filter-disabled
execution.

## Substrate

- `crates/wendelon/src/schema/runtime_filter.rs`
  - Already owns `RuntimeFilterDescriptor`.
  - Gains production encoder, membership, validation, and Arrow mask helper.
- `crates/toron/src/exec/runtime_filter.rs`
  - Currently owns runtime-filter generation and a private/test-facing
    membership path.
  - Generation should call the shared Wendelon encoder.
- `crates/toron/src/storage.rs`
  - `ScanRequest` already carries `runtime_filter`.
  - `InMemoryStorageScan` and `WendelonStorageScan` currently ignore it.
- `crates/wendelon/src/schema/table.rs`
  - `TableQueryRequestMeta` is the `#[repr(C)]` Pod wire struct.
  - `_pad2: [u8; 4]` sits at offset 100 after `bytecode_len`.
- `crates/wendelon/crates/wendelon-client/src/lib.rs`
  - Public smart-client table-query API and `TableQueryOptions`.
- `crates/wendelon/src/client/core.rs`
  - Lower-level `TableQueryRequest` and request validation.
- `crates/wendelon/src/network/data_server.rs`
  - HRFT table-query decode.
  - `prepare_table_query_execution_plan_with_migration`.
  - `read_projected_query_location`.
  - `read_secondary_index_query_location_group`.
  - whole-record fallback PBC execution.
- `crates/wendelon/src/storage/columnar.rs`
  - `ColumnFetchRequirement`.
  - `analyze_pbc_column_requirement`.
  - `execute_pbc_projected_record_batch`.
- `crates/toron/tests/wendelon_storage_execution_test.rs`
  - Existing in-process Wendelon node harness for CI-reachable live coverage.
- `crates/toron/plans/impl/006_runtime_filter_pushdown_bench_results.md`
  - Receives the ADR 013 Wendelon-backed selective evidence row.

## Expected files

Expected code changes:

- `crates/wendelon/src/schema/runtime_filter.rs`
- `crates/wendelon/src/schema/table.rs`
- `crates/wendelon/crates/wendelon-client/src/lib.rs`
- `crates/wendelon/src/client/core.rs`
- `crates/wendelon/src/network/data_server.rs`
- `crates/wendelon/src/storage/columnar.rs`
- `crates/toron/src/exec/runtime_filter.rs`
- `crates/toron/src/storage.rs`
- focused tests under `crates/wendelon` and `crates/toron/tests`

Expected documentation changes at implementation promotion:

- `crates/toron/plans/adr/013_runtime_filter_application_at_the_storage_scan_boundary.md`
- `crates/toron/plans/impl/013_runtime_filter_application_at_the_storage_scan_boundary.md`
- `crates/toron/plans/impl/006_runtime_filter_pushdown_bench_results.md`
- `crates/toron/plans/wip/roadmap.md`

Not expected:

- new Toron IR variants;
- new `JoinKind`, `JoinStrategy`, or scheduler barrier semantics;
- new `TaskSpec`, `TaskFailureKind`, `BudgetDimension`, or `MessageKind`;
- runtime-filter eligibility beyond hash-partitioned inner equi-joins;
- aggregate table-query runtime-filter application;
- row-group skip;
- exact `InList` filters;
- planner cost-model changes;
- `TableQueryOptions` losing `Copy`.

## Phase 0 - Audit and test scaffolding

- Confirm the current Toron generation path and identify the smallest move
  that lets generation call a Wendelon-owned encoder without changing
  descriptor semantics.
- Confirm the existing key type set and preserve every supported type.
- Confirm `TableQueryRequestMeta` layout assertions and offset tests in
  `crates/wendelon/src/schema/table.rs`.
- Rename
  `table_query_wire_contract_carries_descriptor_pin_and_pbc_only`, because
  after ADR 013 the table-query wire carries descriptor pin, PBC bytecode,
  and optional runtime-filter descriptor length.
- Confirm all table-query frame decode sites route through a common helper
  or can be made to share the new trailing split behavior.
- Confirm projected-read paths can carry a runtime-filter application object
  without broadening aggregate table-query execution.
- Confirm the secondary-index projected path uses the same projected-record
  model as primary projected reads.
- Confirm the whole-record fallback points where a full `RecordBatch` exists
  after descriptor matching and before PBC execution.
- Add failing tests before behavior changes when practical. For wire layout
  and helper functions, unit tests can come first.

## Phase 1 - Shared production encoder and membership helper

Extend `crates/wendelon/src/schema/runtime_filter.rs`.

Move or re-home the canonical runtime-filter key encoder into Wendelon
schema code. Toron generation and Wendelon scan application must call the
same encoder. Do not leave two independent encoders.

Expose production helpers with this shape:

- validate descriptor shape against expected key columns and table schema;
- encode one row's probe key into canonical runtime-filter bytes;
- test encoded-scalar membership;
- build an Arrow boolean row-survival mask for a `RecordBatch` or projected
  batch view.

Validation should reject or drop:

- key count mismatch between `key_types` and `probe_key_columns`;
- unsupported `key_encoding_version`;
- unsupported key type;
- out-of-range probe key ordinal;
- inconsistent `bit_count`, `hash_count`, `byte_budget`, or payload length;
- null or otherwise unencodable probe row keys as row-level non-matches for
  scan application. This must not hide false negatives: every encodable value
  inserted by Toron's generator must pass membership.

Test requirements:

- all supported key types preserve no-false-negative behavior for values
  inserted by Toron's generator;
- type tags remain load-bearing, for example `Int32(1)` and `Int64(1)` do
  not collide as logical keys;
- unsupported types and malformed descriptor shapes return typed
  application/drop errors;
- Toron generation tests use the shared encoder.

## Phase 2 - Toron in-memory scan application

Update `InMemoryStorageScan` in `crates/toron/src/storage.rs`.

Order:

1. clone table batch;
2. apply morsel slicing;
3. apply usable runtime-filter mask;
4. execute PBC;
5. apply explicit projection.

Fingerprint mismatches, unsupported shapes, and materialization failures
advisory-drop the filter and continue without it. Result equality to the
filter-disabled path remains the safety baseline.

Test requirements:

- filter applies after morsel slicing;
- filter applies before PBC/projection;
- advisory-drop paths return the same result as `runtime_filter = None`;
- no metrics fire when the descriptor is absent.

## Phase 3 - Table-query wire and client plumbing

Update `TableQueryRequestMeta` in `crates/wendelon/src/schema/table.rs`:

```rust
pub runtime_filter_len: u32
```

This replaces `_pad2: [u8; 4]`. The struct remains 120 bytes, and offset
assertions stay stable. Existing clients in this workspace already zero
`_pad2`, so their frames decode as `runtime_filter_len = 0` under the new
metadata interpretation.

Rename the existing wire-contract test
`table_query_wire_contract_carries_descriptor_pin_and_pbc_only` so the name no
longer claims table-query carries PBC only.

Update client request state:

- `crates/wendelon/src/client/core.rs`
  - `TableQueryRequest` gains
    `pub runtime_filter: Option<RuntimeFilterDescriptor>`.
  - validation checks descriptor serialization length when present.
- `crates/wendelon/crates/wendelon-client/src/lib.rs`
  - add `table_query_with_runtime_filter`;
  - add `table_query_stream_with_runtime_filter`;
  - existing `table_query` / `table_query_stream` delegate with `None`;
  - keep `TableQueryOptions: Copy`.

The trailing section is:

```text
[PBC bytecode][postcard RuntimeFilterDescriptor]
```

Use checked arithmetic:

```text
pbc_end = bytecode_len
runtime_filter_end = bytecode_len + runtime_filter_len
```

Hard framing errors:

- declared nonzero length with missing trailing bytes;
- overflow in length addition;
- declared split beyond trailing length.

Advisory-drop decode case:

- runtime-filter slice length is consistent, but postcard decode fails.

The server-side decoded state should preserve:

```text
Absent
Decoded(RuntimeFilterDescriptor)
DroppedBeforePlanning(RuntimeFilterDropReason)
```

The third state is load-bearing because the query still runs, while metrics
must report that a descriptor was present and dropped.

Update `WendelonStorageScan::scan` to call the new client method and pass
`request.runtime_filter.clone()`.

Test requirements:

- wire layout remains 120 bytes and offset-stable;
- the renamed wire-contract architecture test reflects descriptor pin, PBC,
  and runtime-filter length;
- client trailing bytes split exactly by `bytecode_len` and
  `runtime_filter_len`;
- old public client methods remain source-compatible and send no descriptor;
- impossible length math is a hard framing error;
- length-consistent postcard decode failure becomes
  `DroppedBeforePlanning(decode_failed)`.

## Phase 4 - Wendelon plan-time validation and materialization

Introduce a small internal runtime-filter application state for table-query
execution. Keep it local to Wendelon table-query planning/execution unless a
broader module boundary becomes clearly useful.

Plan-time work:

- carry the decoded runtime-filter state into
  `prepare_table_query_execution_plan_with_migration`;
- validate descriptor fingerprint against the table descriptor;
- validate descriptor shape, key encoding version, payload dimensions,
  supported key types, and probe key ordinals;
- add valid probe key ordinals to
  `ColumnFetchRequirement::predicate_columns`;
- carry an active runtime-filter application object into the query execution
  plan;
- preserve pre-planning drop state for metrics.

The requirement must be augmented before any `read_projected_record` call.

For `QueryProjectedExecution::Pbc`, update the `ColumnFetchRequirement`
before constructing or dispatching projected execution. The aggregate
variant is out of scope; do not broaden aggregate table-query semantics.

If a descriptor is usable and the probe key is already present in the
requirement, no extra fetch is added. If absent, augment
`predicate_columns`. If the key cannot be added, advisory-drop with
`materialization_incompatible`.

Test requirements:

- a projected PBC output that omits the runtime-filter key still fetches the
  key as a predicate column;
- the key does not appear in output;
- fingerprint mismatch drops before mask evaluation;
- unsupported shapes and out-of-range ordinals drop without changing the
  result.

## Phase 5 - Wendelon row-time mask application

Create one shared execution helper for mask application and metric/drop
behavior.

Use it from:

- `read_projected_query_location`;
- `read_secondary_index_query_location_group`;
- whole-record fallback paths.

Projected path order:

1. materialize projected batch;
2. run projected descriptor matching;
3. apply runtime-filter mask;
4. execute PBC over surviving rows.

Whole-record fallback order:

1. materialize full record batch;
2. run table descriptor matching;
3. apply runtime-filter mask;
4. execute PBC over surviving rows.

The helper records:

- descriptor-present count;
- applied count;
- advisory-dropped count;
- closed drop reason;
- input row count;
- surviving row count.

No runtime-filter metric is emitted for the `None` path. Do not use
descriptor fingerprints, table IDs, or key ordinals as metric labels.

Test requirements:

- primary projected path applies after descriptor match and before PBC;
- secondary-index projected path has identical behavior;
- whole-record fallback applies before PBC;
- applied filters reduce rows when selective;
- advisory-dropped filters produce byte-identical output to absent filters;
- row counters reflect input and survivor counts for applied filters.

## Phase 6 - End-to-end validation and benchmark evidence

Extend `crates/toron/tests/wendelon_storage_execution_test.rs`.

Required live-Wendelon coverage:

- Toron passes `request.runtime_filter` through `WendelonStorageScan`.
- Wendelon receives and applies it.
- A sparse projected table query fetches the probe key as a predicate input
  while omitting it from output.
- Filter-on and filter-off results are digest-equal.
- Selective filter-on execution observes fewer probe rows/bytes crossing the
  storage boundary.

Append the selective production row to:

```text
crates/toron/plans/impl/006_runtime_filter_pushdown_bench_results.md
```

The row must show:

- filter-on vs filter-off;
- digest equality;
- fewer probe rows and/or bytes crossing Wendelon to Toron;
- enough context to compare against ADR 006's deferred Wendelon row.

## Phase 7 - Documentation promotion update

After implementation:

- update this plan's status to "implemented";
- update the ADR status to "promoted and implemented";
- update `plans/wip/roadmap.md`;
- append or update the ADR 006 bench-results row;
- do not add a separate ADR 013 bench-results artifact unless the evidence
  no longer fits the ADR 006 artifact.

## Verification

Minimum before opening implementation for review:

```text
cargo fmt --all --check
cargo check --workspace --locked
cargo test --locked -p wendelon --no-fail-fast
cargo test --locked -p toron --test wendelon_storage_execution_test --no-fail-fast
cargo test --locked -p toron --no-fail-fast
git diff --check
```

Promotion:

```text
cargo test --locked --workspace --no-fail-fast
```

If the full workspace test suite is too slow or environment-limited, record
the exact subset run and the reason. Do not promote implementation without
the live Wendelon runtime-filter path and ADR 006 bench row.

## Definition of done

- `ScanRequest.runtime_filter` is applied or advisory-dropped by both
  production Toron scan implementations.
- Wendelon table-query wire carries runtime-filter descriptors.
- `TableQueryOptions` remains `Copy`.
- Wendelon primary projected, secondary-index projected, and whole-record
  fallback paths apply runtime filters at the required point.
- Runtime-filter probe columns can be fetched as predicate inputs without
  appearing in output.
- Shared encoder and membership helpers are production code in Wendelon
  schema.
- Coverage proving no false negatives exists for every generated key type.
- Advisory-drop paths are byte-identical to absent-filter execution.
- Metrics are bounded and descriptor-present only.
- ADR 006 bench artifact contains the Wendelon-backed selective row.
