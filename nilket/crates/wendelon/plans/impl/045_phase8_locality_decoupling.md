# Implementation Plan: ADR 045 Phase 8 — Locality Decoupling

Companion ADR: `plans/adr/045_pk_less_append_only_tables.md`

## Why this exists

Phase 7 fixed the HLC-split correctness bug and dropped the
canonical row payload hash from the §4 identity preimage. A
post-merge review surfaced four issues that Phase 7 didn't
cover. The blocker: append-only descriptor matching
re-synthesized identities with a zero HLC, filtering append-only
rows out of query merge, reclustering, stats sweeps, and LZ4-tier
candidate detection. The architectural issue: client-side
routing previewed admission with `client_hlc`, server-side
admission used `commit_hlc`, and the resulting per-row localities
disagreed — under lease gate this returned `NOT_LEADER` hinting
the admitting node itself, burning the retry budget without
converging. Two smaller issues: `writer_node_id` was bound from
`meta.proxy_source` (zero on direct SDK writes, so the
authenticated-admitter contract was unmet); admission ran
`observe_storage_append_only_batch` as a side effect even on
client-side preview and aborted server attempts.

The fix is structural, not a retry workaround:
- AppendOnly **identity** stays HLC-derived (Phase 7 contract).
- AppendOnly **locality** decouples from identity. It becomes a
  per-request value derived from retry-stable write context.
- AppendOnly **writer_node_id** binds to the authenticated server
  node, plumbed explicitly into admission.
- AppendOnly **descriptor matching** trusts stored synthetic
  columns; it does not re-derive.
- AppendOnly **metrics** observe at the actual append-success
  point on the server, not inside admission.

## Goal

For ADR 045 V1 AppendOnly tables:

1. Locality is computed once per write request from
   `(tenant_id, table_object_id, parent_write_id)` under the
   domain tag `"table.append-locality.v1"` and is the same value
   for every row in the batch. Client preview and server
   admission produce the same locality byte-for-byte, so routing
   converges in one hop and the lease gate does not trip on
   structural client/server disagreement.
2. The `__row_routing_locality__` synthetic column carries this
   per-request locality on every row.
3. `WriterNodeId` is bound from the admitting server's
   authenticated `NodeId` (ADR 018), passed explicitly into
   admission. `meta.proxy_source` retains its proxy-routing
   semantics.
4. `RowIdentityPolicy::AppendOnly` requires
   `LocalityPolicyV1::TableSpreadProjection`. Descriptor
   validation rejects `AppendOnly + IdentityProjection` and
   `AppendOnly + ColumnProjection` for V1.
5. Descriptor-match helpers (`table_record_matches_descriptor`,
   `descriptor_matches_tiered_row`) dispatch on
   `RowIdentityPolicy`; for AppendOnly they verify schema only
   and trust the stored synthetic columns.
6. Admission is side-effect-free. Batch / row counters fire on
   the server's success path after lease gate, idempotency
   replay, and quota all clear.
7. Identity-synthesis histogram deleted (bench is the right
   measurement surface for that signal).

## Non-goals

- No change to AppendOnly identity preimage. §4 still hashes
  `(tenant, table, writer, commit_hlc, intra_batch_index)`.
- No change to PK-keyed tables.
- No new wire frame, no new node capability, no
  `NodeCapability::*` bit, no enum variant suffix.
- No content-hash dedupe, no idempotent ingest, no
  request-level dedup beyond the existing
  `parent_write_id`-keyed idempotency cache.

## Phase 8.0 — Locality preimage contract

```text
append_locality_seed = BLAKE3(
    "table.append-locality.v1"
    || tenant_id              // 16 B (8-byte LE value + 8 zero pad)
    || table_object_id        // 16 B
    || parent_write_id        // 32 B
)
```

The `u64` derived from the seed feeds the existing table-spread
machinery — `bucket = jump_consistent_hash(seed_u64,
spread_factor)`, `prefix = table_spread_prefix_hash(...)`,
`suffix = seed_u64 & suffix_mask`. The `suffix` term keeps the
low bits varying per request (good distribution within the
spread bucket) without re-introducing per-row variation.

The domain tag is versioned (`v1`) so a later ADR can rotate the
locality-derivation function without overlapping the current
keyspace.

## Phase 8.1 — Admission and helper signatures

```rust
pub fn synthesize_append_only_row_id(
    tenant_id: TenantId,
    table_object_id: TableObjectId,
    writer_node_id: WriterNodeId,
    row_hlc: HybridTimestamp,
    intra_batch_index: IntraBatchIndex,
) -> Identity;  // unchanged

pub fn append_only_request_locality(
    tenant_id: TenantId,
    table_object_id: TableObjectId,
    parent_write_id: ClientWriteId,
    spread: TableSpreadLocalityDescriptorV1,
) -> std::io::Result<Locality>;  // new

pub fn validate_table_write_request_batch(
    meta: &TableWriteRequestMeta,
    batch: &RecordBatch,
    descriptor: &TableSchemaDescriptorV1,
    writer_node_id: WriterNodeId,
    row_hlc: HybridTimestamp,
) -> Result<TableWriteAdmission, TableWriteValidationError>;
```

`bifurcate_table_write` and `pack_record_batch_by_bucket` take
`writer_node_id` as an explicit parameter. The data-server's
`table_write` computes
`writer_node_id = WriterNodeId::from_node_id(self.local_node)`
once per request and threads it through.

For AppendOnly admission, every row receives the same
`Locality`, computed once from the request via
`append_only_request_locality`. Identity stays per-row.

## Phase 8.2 — Descriptor validator extension

`validate_row_identity_policy` (or a new cross-policy check
called from `Self::validate`) rejects the following descriptor
shapes with `PBC_REJECT_DESCRIPTOR_INVALID`:

- `RowIdentityPolicy::AppendOnly` with
  `LocalityPolicyV1::IdentityProjection`.
- `RowIdentityPolicy::AppendOnly` with
  `LocalityPolicyV1::ColumnProjection(_)`.

Reason in the reject message: AppendOnly identity is HLC-derived
and `IdentityProjection` / `ColumnProjection` cannot produce
retry-stable client-side routing under the V1 contract.

## Phase 8.3 — Descriptor-match helper split

`table_record_matches_descriptor` (data_server.rs) and
`descriptor_matches_tiered_row` (manager.rs) become thin
dispatchers:

```text
match descriptor.row_identity_policy {
    RowIdentityPolicy::PrimaryKey(_) => existing PK admission re-check,
    RowIdentityPolicy::AppendOnly => schema_match_only(batch, descriptor),
}
```

For AppendOnly, the helper verifies `batch.num_rows() == 1` and
`batch.schema().as_ref() == &descriptor.arrow_schema()?`. It does
not re-derive identity or re-compute locality: the stored row's
synthetic columns are authoritative. The `zero_hlc` placeholder
and its lying comments are deleted.

## Phase 8.4 — Metric site move

- `observe_storage_append_only_batch` is removed from
  `admit_append_only_write`. The data-server `table_write` path
  calls it after lease gate passes, idempotency replay misses,
  quota clears, and `bifurcate_table_write` succeeds.
- `storage_append_only_identity_synthesis_micros` histogram +
  `observe_storage_append_only_identity_synthesis` method are
  deleted. Bench coverage stays.

## Phase 8.5 — Tests

- AppendOnly client-preview locality equals server-admission
  locality byte-for-byte across distinct
  `(client_hlc, commit_hlc)`.
- Single-locality append-only batch passes the lease gate when
  the local node is primary for that one locality.
- `WriterNodeId` from authenticated `local_node` flows into the
  identity preimage even when `meta.proxy_source == 0`.
- Descriptor validator rejects `AppendOnly + IdentityProjection`
  and `AppendOnly + ColumnProjection`.
- Append-only rows are visible to query-path descriptor
  matching (regression for the blocker), to reclustering, to
  stats sweep, and to LZ4-tier candidate detection.
- `observe_storage_append_only_batch` does not fire on
  client-preview admission or on a server attempt that fails
  lease gate / idempotency replay / quota.

## Read-pruning constraint

Switching to one locality per request must not weaken read
pruning. Stored `__row_routing_locality__` is the request-level
append locality on every row. Query scan ranges for AppendOnly
table-spread continue to cover the full set of prefixes
produced by `table_spread_prefix_ranges(tenant_id,
table_object_id, spread)`. Queries do not know
`parent_write_id`s, so they pin no narrower than the spread
prefix space — which is exactly the existing behavior. No
query-side changes are required.

## Forward-compatibility

- The locality domain tag is versioned. A future
  identity-policy variant (e.g., content-hash) can introduce
  its own locality preimage without overlapping `v1`.
- `IdentityProjection` and `ColumnProjection` for AppendOnly
  remain available for future ADRs that want to prove
  retry-stable routing with a different mechanism.
- `MultiRow` packing continues to bucket by
  `__row_routing_locality__`; with one locality per AppendOnly
  request, each batch packs into one bucket and produces one
  multi-row record (size limits still apply). No changes to the
  packer.

## Phase 8 Addendum — AppendOnly table-isolation column

Schema-only descriptor matching for AppendOnly (Phase 8's
first cut) was insufficient: same-tenant table-spread
prefixes are not globally unique, so two AppendOnly tables
with the same Arrow user schema in the same tenant could
have overlapping table-spread prefix ranges, and rows from
one descriptor would leak into the other's query results,
reclustering, stats matching, and LZ4-tier candidate
detection. PK-keyed tables avoid this through canonical PK
re-derivation under `descriptor.table_object_id`; AppendOnly
needed an equivalent durable binding.

Resolution after the Phase 8.5 projected-path follow-up:
every multi-row record (PK + AppendOnly) gains a per-row
`__row_table_object_id__: FixedSizeBinary(16)` synthetic
column, stamped at admission with the descriptor's
`table_object_id`. The matchers verify equality with the
candidate descriptor's `table_object_id`; for AppendOnly this
is the descriptor binding, and for PK it is an early
descriptor-isolation check before any whole-record PK
re-derivation.

Storage cost: 16 bytes × N rows per multi-row record before
compression. The column is a constant-per-record value, so LZ4
compresses it to dictionary overhead — effectively a fixed
small cost per record.

Compaction preservation: reclustering re-stamps from the
descriptor (the descriptor is authoritative for the rows
being reclustered); generic compaction reads the per-row
stamp from the source storage rows and propagates it
unchanged.

Public surface: `MultiRowSourceRow` and `StorageRowMetadata`
gain `table_object_id: Option<TableObjectId>`. The
decoder/encoder helpers handle present/absent symmetrically
because single-row records still carry no storage synthetic
columns. A new helper `storage_row_table_object_id_at(batch,
record_row_index)` is the matcher's read path.

Tests: same-tenant cross-descriptor isolation (two AppendOnly
tables, same Arrow schema, distinct `table_object_id`s —
matcher correctly distinguishes them).

## Phase 8.5 Addendum — Projected fast path verification

The first cut of Phase 8's matcher fix only covered the
whole-record fallback path. The L2 projected query path
(`read_projected_query_location`,
`read_aggregate_query_location`) returned projected results
without going through `table_record_matches_descriptor`, so
the AppendOnly table-isolation check was bypassed when the
column-fetch fast path applied.

Resolution:

1. New `StorageColumn::RowTableObjectId` variant. The L2 fetch
   layer can request `__row_table_object_id__` like any other
   synthetic column.
2. `augment_requirement_for_descriptor_verification(req,
   descriptor)` adds `RowTableObjectId` to the
   `ColumnFetchRequirement.storage_columns` for every
   multi-row descriptor (PK + AppendOnly). Single-row
   descriptors carry no synthetic columns at all, so the
   augmentation is skipped there.
3. `projected_descriptor_match(descriptor, projected_batch)`
   verifies the stored `__row_table_object_id__` equals
   `descriptor.table_object_id` uniformly for PK and
   AppendOnly. If the column isn't in the projected fetch
   (single-row record, or otherwise missing), the helper
   returns `Ok(false)` and the projected path returns
   `Ok(None)` so the caller falls back to the whole-record
   path that runs the trusted
   `table_record_matches_descriptor`.

Storage manager `unwrap_or(None)` swallowing of malformed
synthetic-column data was also tightened: maintenance paths
(`stats`, `reclustering`, `LZ4-tier candidate`) now propagate
errors via `?` and only treat `Ok(None)` (column absent —
single-row record) as "no table-id stamp."

### PK projected-path closure (Phase 8.5 follow-up)

The earlier Phase 8 implementation stamped
`__row_table_object_id__` on AppendOnly multi-row records
only. Descriptor-filtered PK queries on the projected path
returned without verification, leaving a pre-existing
correctness gap: the PK matcher in the whole-record path
re-derives identity from the user batch's PK column, but
the projected fast path may not include the PK column,
so the same re-derivation isn't available there.

Phase 8.5 closed this by stamping
`__row_table_object_id__` on every multi-row record (PK +
AppendOnly), and using a uniform projected verifier:

- `pack_admitted_record_batch_by_bucket` and the
  reclustering site stamp `Some(descriptor.table_object_id)`
  unconditionally for multi-row descriptors.
- The PK schema validator forbids users from declaring a
  `__row_table_object_id__` column (rejection lifted to a
  shared helper across PK and AppendOnly).
- `augment_requirement_for_descriptor_verification` and
  `projected_descriptor_match` drop the AppendOnly gate.
- `descriptor_matches_tiered_row` and
  `table_record_matches_descriptor` use the column as an
  early-fail in the PK arm before re-derivation.

Single-row PK records (which don't carry synthetic columns
at all) fall back to the whole-record path; correctness is
preserved with a performance gap on single-row PK
descriptor-filtered queries that we can revisit.

Trade-off: 16 bytes per row of stored data on the PK side,
dictionary-compressible (one value repeated across the
entire record). The constant-column buffer is ~negligible
versus the projected-path savings on PK queries that no
longer pay full PK re-derivation cost per candidate row.
