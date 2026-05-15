# ADR 013: Runtime Filter Application at the Storage-Scan Boundary

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Gemini iter-4, promoted by Codex
**Companion implementation plan:**
`plans/impl/013_runtime_filter_application_at_the_storage_scan_boundary.md`

## Status

Promoted to `plans/adr/`. Implementation is pending.

Three-way council convergence reached across four iteration rounds. The
only material fork, augmenting Wendelon's projected fetch requirement
versus dropping when runtime-filter keys were absent, converged on
augmenting `ColumnFetchRequirement::predicate_columns`.

Load-bearing antecedents: Claude's ADR-012-cycle seed drafts and
materialization-requirement trace; Codex's contract sharpening, deeper
`read_projected_query_location` trace, request-plumbing decision, and
augment-if-absent framing adjustment; Gemini's materialization-gate
investigation, iter-2 to iter-3 concession that closed the only
architectural fork, and iter-4 consolidation basis.

ADR 013 has no independent bench artifact. Its implementation updates the
ADR 006 bench artifact with the deferred production Wendelon runtime-filter
row.

## Goal

Make `ScanRequest.runtime_filter` live at the production storage-scan
boundary.

ADR 006 already generates, merges, and attaches Bloom runtime-filter
descriptors for hash-partitioned inner equi-joins. Today those descriptors
arrive at storage scans and are ignored by both production scan
implementations. ADR 013 closes that gap for `InMemoryStorageScan` and
`WendelonStorageScan`, including Wendelon's table-query wire and scan-engine
application path.

## Scale boundary

**Primary:** cross-ring storage-scan application.

ADR 013 touches:

- Toron's `StorageScan` contract and production implementations;
- Wendelon client table-query request construction;
- Wendelon's table-query wire frame metadata and decode path;
- Wendelon's table-query projected-read and whole-record execution paths;
- shared runtime-filter key encoding and membership helpers.

Explicitly unchanged:

- runtime-filter generation, sizing, merge, and two-wave scheduler barrier;
- hash-join runtime-filter eligibility: hash-partitioned inner equi-joins
  only;
- broadcast, outer, semi, anti, non-equi, and exact `InList` runtime
  filters;
- planner cost model and stats-based runtime-filter opt-in;
- Toron IR, `JoinKind`, scheduler, fragmenter, `TaskSpec`,
  `TaskFailureKind`, `BudgetDimension`, and `MessageKind`;
- aggregate table-query runtime-filter semantics;
- row-group skip.

No slot, field, message, PBC op, or descriptor surface is reserved for the
deferred runtime-filter extensions. Future ADRs add their own surface when
they pin their own contract.

## Context

ADR 006 introduced advisory Bloom runtime filters for hash-partitioned
inner equi-joins. Toron-side production code builds fragments, merges them,
and threads `RuntimeFilterDescriptor` to `ScanRequest.runtime_filter`.

The storage-scan boundary is still inert:

- `InMemoryStorageScan` ignores `request.runtime_filter`.
- `WendelonStorageScan` ignores `request.runtime_filter` and calls
  `Client::table_query` without a descriptor.
- Wendelon's table-query wire has PBC bytecode length but no
  runtime-filter descriptor length.
- Wendelon's table-query execution does not apply a runtime-filter mask.
- The only row-rejection implementation is test-local.

This split was safe because runtime filters are advisory, but it left ADR
006's production performance claim incomplete. ADR 013 makes the descriptor
actionable at the scan boundary while preserving byte-identical fallback
behavior when the descriptor is absent or unusable.

## Decision

### 1. Production scans apply or advisory-drop

Production `StorageScan` implementations that receive a
`request.runtime_filter` must either apply a usable descriptor or
advisory-drop it.

For ADR 013, the production implementations are:

- `InMemoryStorageScan`;
- `WendelonStorageScan`.

Test-only scan fixtures may either conform or explicitly document
non-participation.

### 2. Advisory drop is safe and explicit

A runtime filter is an advisory row-rejection hint. Dropping it cannot
change the query result: the scan returns a superset, and the downstream
join filters that superset to the same answer.

This is categorically different from ADR 010 / 011 / 012 semantic drops,
which silently changed observable query semantics. An ADR 013 advisory drop
means "apply nothing." It is never legal to apply a stale, malformed,
mismatched, or partially understood descriptor.

Descriptor fingerprint validation happens before any mask evaluation.

### 3. Application order is fixed

Runtime-filter application happens:

1. after morsel/range narrowing;
2. after table descriptor verification;
3. before PBC projection;
4. before downstream row/byte accounting that observes scan output.

This order preserves the advisory invariant and makes row/byte reductions
real at the storage boundary.

### 4. In-memory scan application

`InMemoryStorageScan` applies a usable runtime filter after morsel slicing
and before PBC execution or explicit column projection.

Unsupported descriptor shapes, fingerprint mismatches, and key
materialization failures advisory-drop the descriptor and run the scan
without the filter.

### 5. Wendelon projected-read materialization

Wendelon table-query projected reads must add runtime-filter
`probe_key_columns` to `ColumnFetchRequirement::predicate_columns`.

If the probe key columns are already present, this is a no-op fast path. If
they are absent, augmenting the predicate axis is required. The key columns
are fetched for row survival only; they are not added to
`output_columns` and must not appear in the PBC output schema.

If any probe key column cannot be added, fetched, or encoded, the
descriptor is advisory-dropped with `drop_reason =
materialization_incompatible` or `unsupported_shape`.

The projected-read application sites are:

- primary projected table-query reads through `read_projected_query_location`;
- secondary-index projected table-query reads through
  `read_secondary_index_query_location_group`.

The mask is applied after projected descriptor matching and before PBC
execution over the projected batch.

Whole-record fallback paths apply the mask after table descriptor matching
and before PBC execution. Aggregate table-query execution is out of scope.

### 6. Table-query wire extension

`TableQueryRequestMeta._pad2` becomes:

```rust
pub runtime_filter_len: u32
```

The struct remains 120 bytes, and existing offsets stay stable. The trailing
section is split as:

```text
[PBC bytecode][postcard RuntimeFilterDescriptor]
```

`bytecode_len` identifies the PBC bytecode length.
`runtime_filter_len` identifies the descriptor length immediately after the
PBC bytecode.

Existing clients in this workspace already zero `_pad2`, so frames emitted
before ADR 013 naturally decode as `runtime_filter_len = 0` under the new
metadata interpretation.

Length arithmetic overflow, missing required trailing bytes, or declared
lengths exceeding the trailing section are hard framing errors. A
length-consistent descriptor slice that fails postcard decode is an
advisory drop with `drop_reason = decode_failed`.

### 7. Descriptor transport keeps options copyable

`TableQueryOptions` remains `Copy`.

Runtime filters are query payload, not durable client options. The Wendelon
client adds parallel opt-in methods such as:

- `table_query_with_runtime_filter`;
- `table_query_stream_with_runtime_filter`.

Existing `table_query` and `table_query_stream` methods delegate with
`None`. The lower-level `TableQueryRequest` carries
`Option<RuntimeFilterDescriptor>` adjacent to the PBC bytecode.

### 8. Shared membership helper and encoder

Runtime-filter key encoding and membership move into production Wendelon
schema code under `wendelon::schema::runtime_filter`.

The helper exposes:

- an encoded-scalar membership primitive for direct tests and simple row
  loops;
- an Arrow batch mask builder for scan application.

Toron runtime-filter generation and Wendelon runtime-filter application use
the same encoder. The dependency direction remains Toron to Wendelon, which
already owns `RuntimeFilterDescriptor`.

### 9. Metrics are bounded and descriptor-present only

The `None` path emits no runtime-filter metrics.

Descriptor-present scans emit bounded counters/events:

- `present`;
- `applied`;
- `advisory_dropped`;
- `drop_reason`;
- `input_rows`;
- `surviving_rows`.

The closed initial drop-reason set is:

- `decode_failed`;
- `fingerprint_mismatch`;
- `unsupported_shape`;
- `materialization_incompatible`.

Descriptor fingerprints, table IDs, and key ordinals are not metric labels.
Debug logs may include bounded detail when useful.

## Consequences

ADR 013 completes ADR 006's production application path. Selective
Wendelon-backed hash joins can reject probe rows before they cross the
Wendelon-to-Toron boundary.

The advisory-drop invariant keeps split shipment safe: a missing or unusable
filter only disables the optimization, never changes the result.

Fetching runtime-filter probe columns through `predicate_columns` preserves
sparse projected reads without corrupting output schema. The same model is
already used for PBC predicate columns: predicate inputs may be fetched
without being emitted.

## Promotion evidence

Implementation promotion requires:

1. Unit coverage for the shared production encoder and membership helper,
   including every supported key type.
2. `InMemoryStorageScan` coverage proving the filter applies after morsel
   slicing and before PBC/projection.
3. Wendelon projected table-query coverage proving a runtime-filter key can
   be fetched as a predicate column while omitted from PBC output.
4. Wendelon secondary-index projected-read coverage with the same
   semantics.
5. Wendelon whole-record fallback coverage proving application before PBC
   execution.
6. Wire-framing coverage proving `TableQueryRequestMeta` layout stability,
   exact trailing-byte splitting, hard failure for impossible length math,
   and advisory drop for length-consistent postcard decode failure.
7. Live Toron-to-Wendelon coverage using
   `crates/toron/tests/wendelon_storage_execution_test.rs`, which already
   starts an in-process Wendelon node in CI.
8. A selective Wendelon-backed filter-on versus filter-off row appended to
   `plans/impl/006_runtime_filter_pushdown_bench_results.md`, with
   digest-equal output and fewer probe rows/bytes crossing the storage
   boundary.

## What not to do

- Do not apply a descriptor whose fingerprint or shape does not match the
  scanned table.
- Do not add runtime-filter key columns to scan output.
- Do not put `RuntimeFilterDescriptor` into `TableQueryOptions`.
- Do not duplicate the runtime-filter key encoder in Toron and Wendelon.
- Do not use advisory drops to hide lack of production application in the
  promoted implementation.
- Do not extend runtime-filter eligibility beyond ADR 006's
  hash-partitioned inner equi-join contract in ADR 013.
