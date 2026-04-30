# Implementation Plan: ADR 052 — Secondary Indexes on Non-Clustering Columns

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/052_secondary_indexes.md`

Inherits substrate: `plans/adr/046_state_digest_substrate.md`
(`DOMAIN_SECONDARY_INDEX_ENTRY` already at
`src/hash/state_digest.rs:19`).
Inherits identity: `plans/adr/037_tenant_table_schemas.md`
(PK), `plans/adr/045_pk_less_append_only_tables.md`,
`plans/adr/049_content_hash_row_identity.md`.
Inherits supersession: `plans/adr/047_row_tombstones_append_only.md`,
`plans/adr/050_pk_delete_supersession.md`.
Inherits retention: `plans/adr/013_time_travel_queries.md`,
`plans/adr/048_per_table_retention_policy.md`.
Inherits descriptor fingerprint:
`plans/adr/051_descriptor_fingerprint_split.md`.

## Goal

Add a second access path on tables: secondary indexes keyed on
a non-`__row_key_id__` column, as a derived candidate
accelerator. Index descriptors live in a replicated catalog
outside `TableSchemaDescriptorV1`. Index segments are per-
source-slab sidecars carrying physical locator triples for
fetch and a **logical version-entry** `StateDigest` for replica
reconciliation. Query planning uses covered sidecars on
`Ready`-state indexes; uncovered slabs and `Building` /
`Dropping` indexes route to the existing scan path. Every
candidate is revalidated against row visibility, indexed-value
match, and the full PBC predicate. Index entries bind to
ADR 051's `LogicalSchemaFingerprint` so physical retunes of
the table do not invalidate sidecars.

The implementation lets secondary indexes drive:

- `SchemaUpdateEnvelopeV1` create / drop with
  `SchemaObjectKind::Index`. The `Alter` mutation is rejected
  at the schema-mutation layer with
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
- replicated `SecondaryIndexCatalog` with lifecycle states
  `{Building, Ready, Dropping}` keyed by
  `(tenant_id, index_object_id)` and indexed by
  `(tenant_id, namespace, index_name)` and by
  `(tenant_id, table_object_id)`;
- per-source-slab sidecars with sort key
  `(encoded_key, row_key_id, row_hlc, record_location,
  record_row_index)` and a header-resident `StateDigest` over
  reserved `digest.secondary_index.entry.v1`;
- async per-replica backfill builder; active-slab buffers;
  local coverage tracking; hybrid plans for partial coverage;
- mandatory candidate revalidation (segment fingerprint match;
  row-metadata equality; `lookup_as_of`; indexed-value
  recheck; PBC recheck);
- automatic sidecar regeneration on source-slab compaction /
  retention / tiering;
- minimal planner integration: equality, range, IN-list with
  hybrid fallback over `Ready`-state indexes;
- recovery, metrics, audit, runbook, architecture guards.

## Deliverables

- `IndexObjectId` newtype; `RecordRowIndex(u32)` newtype.
- `SecondaryIndexDescriptor`, `SecondaryIndexKeyDescriptor`,
  `IndexArrowTypeTag`, `IndexKeyEncoding::OrderedScalar`,
  `SecondaryIndexCatalog`,
  `SecondaryIndexState ∈ {Building, Ready, Dropping}`.
- `SchemaObjectKind::Index` create / drop in the control-ring
  state machine; `Alter` rejected with
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
- Canonical ordered scalar key encoder per ADR 052 §3
  (non-NULL only; NULL rows skip emission).
- `secondary_index_entry_element` digest helper using
  `DOMAIN_SECONDARY_INDEX_ENTRY`.
- Immutable per-source-slab sidecar segment format and local
  segment catalog.
- Async backfill worker with source-slab read-lease against
  compaction; bounded active-slab index buffer
  (`max_index_buffer_bytes`); per-replica coverage tracker.
- Two-phase replicated drop GC: `Dropping` → sidecar GC →
  catalog removal mutation.
- Coverage healing via compaction (uncovered source-slab
  output gets a fresh sidecar).
- Read path: candidate retrieval + `lookup_as_of`
  revalidation + indexed-value recheck + PBC recheck.
- Compaction / retention / tiering segment rebuild.
- Drop-index sidecar GC.
- Minimal planner integration on PBC predicate extraction
  (equality, range, IN-list) gated on `Ready`-state.
- Architectural guards: `digest.secondary_index.entry.v1` is
  the only index digest domain; index descriptors live
  outside `TableSchemaDescriptorV1`; no table
  `schema_generation` bump on index create / drop; no new
  HRFT frame family; no bump on any existing
  `format_version` (sidecars carry their own on a fresh
  artifact family); no covering /
  unique / multi-column / expression index code paths; index
  emits on every data-row write (including unchanged-value
  upserts; non-NULL rows only); tombstones emit no index
  entry; NULL rows emit no entry.
- Golden vectors covering descriptor validation, key encoding,
  digest preimage, segment round-trip, supersession through
  `lookup_as_of`, indexed-value mutation, unchanged-value
  upsert (load-bearing — without per-version emission, post-
  upsert lookup misses), NULL exclusion, retention, hybrid
  plan equivalence with full scan.
- Bench: key encoding, segment build, range scan, candidate
  validation, hybrid-plan crossover, compaction rebuild,
  digest comparison.
- Operator metrics, audit events, runbook.

## Non-goals

- No multi-column composite indexes.
- No expression / functional / covering / partial / unique /
  full-text / vector indexes.
- No NULL-included mode (V1 excludes NULL; `IS NULL` routes
  to scan).
- No float / decimal / dictionary / nested / list / struct /
  map / union / interval indexed columns.
- No locale collation / case folding / NFC / total-order
  float encoding.
- No synchronous index updates blocking write admission.
- No global ready-state replication.
- No cost-based optimizer.
- No range-sharded ring projection.
- No cross-table indexes.
- No SQL parser.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait change.
- No bump on any **existing** `format_version` (envelope,
  data-slab footer, CSB header, table descriptor). The
  per-source-slab index sidecar is a fresh artifact
  family and carries its own `format_version` in its
  header (Phase 3 §1); that field is new, not a bump on
  an existing one.
- No table descriptor field for indexes.
- No table `schema_generation` bump on index create / drop.
- No new descriptor-axis fingerprint domain.
- No separate index-axis logical fingerprint newtype.
- No new digest domain beyond
  `digest.secondary_index.entry.v1`.
- No durable digest placement on data slabs.
- No reserved enum slots, compatibility shims, or `V1` /
  `V2` Rust type suffixes.
- No anti-entropy reader / fanout (substrate only).
- No full-chain key-retirement proof.
- No append-only-style aggressive pair-GC on index sidecars.
- No CDC index-axis event surface.
- No `Alter` mutation; drop + create with new ID.
- **No visible-membership digest in V1.** ADR 052 ships the
  logical version-entry digest; visible-only reconciliation
  is a follow-up consumer.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the descriptor shape, the digest discipline, the
candidate revalidation rule, the sidecar shape, the planner
surface, the lifecycle states, the reject taxonomy, and the
architectural guards before touching admission code or
segment serialization.

Files:

- `src/schema/units.rs` (new newtypes)
- `src/schema/secondary_index.rs` (new — descriptor module)
- `src/storage/secondary_index.rs` (new — admission /
  revalidation skeleton)
- `tests/fixtures/secondary_index/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze newtypes per `newtype-units.md`:
   `IndexObjectId([u8; 16])`,
   `RecordRowIndex(u32)`,
   plus `IndexArrowTypeTag`, `IndexKeyEncoding`.

2. Freeze `SecondaryIndexDescriptor` shape per ADR 052 §1.
   Postcard-serializable. No `V1` Rust suffix.

3. Freeze `SecondaryIndexState ∈ {Building, Ready, Dropping}`
   on catalog rows. Lifecycle: absent → `Building` →
   `Ready` → `Dropping` → absent. No backwards transitions.

4. Freeze the digest preimage helper signature using the
   already-allocated domain constant:

   ```rust
   pub fn secondary_index_entry_element(
       tenant_id:                  TenantId,
       table_object_id:            TableObjectId,
       index_object_id:            IndexObjectId,
       logical_schema_fingerprint: LogicalSchemaFingerprint,
       column_ordinal:             u32,
       encoded_key:                &[u8],
       row_key_id:                 Identity,
       row_hlc:                    HybridTimestamp,
   ) -> StateDigestElement;
   ```

   Element projection uses
   `StateDigestElement::project(DOMAIN_SECONDARY_INDEX_ENTRY,
   preimage)`.

5. Freeze the candidate revalidation contract (per ADR 052
   §6):
   - segment `logical_schema_fingerprint` match;
   - candidate row fetch via
     `(slab_id, record_location, record_row_index)`;
   - row-metadata equality on
     `(__row_key_id__, __row_hlc__)`;
   - `lookup_as_of(tenant_id, row_key_id, as_of_hlc)`
     returns the same `(row_key_id, row_hlc)` and is not
     a tombstone;
   - fetched row's current indexed value equals the
     entry's `encoded_key` under the original canonical
     encoding;
   - full PBC predicate evaluates to true.

6. Freeze the sidecar segment header shape per ADR 052 §4.
   Per-source-slab binding key is
   `(tenant_id, table_object_id, index_object_id,
   logical_schema_fingerprint, source_slab_id)`.

7. Freeze the indexed Arrow type set per ADR 052 §2; helper
   `indexed_type_supported(arrow_type) -> bool`.

8. Freeze the **emission rule**: every data-row admission
   whose indexed column is non-NULL emits exactly one
   sidecar entry. NULL → no entry. Tombstone admission →
   no entry. **Unchanged-value upsert → one entry** (the
   new version's locator).

9. Freeze the reject taxonomy per ADR 052 §13:

   **Lookup / descriptor-validation rejects (`INDEX_REJECT_*`):**
   - `INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH`
   - `INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE`
   - `INDEX_REJECT_PK_COLUMN`
   - `INDEX_REJECT_SYNTHETIC_COLUMN`

   **Schema-mutation-layer reject (separate namespace):**
   - `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` —
     surfaced on the schema-update reply path; not an
     `INDEX_REJECT_*` code.

   No `INDEX_REJECT_NULL_INDEXED_VALUE_LOOKUP` (NULL is
   excluded; `IS NULL` routes to scan).
   No `INDEX_REJECT_NOT_FOUND`. Dropped / `Building` /
   `Dropping` indexes route through fingerprint mismatch.

10. Freeze the lifecycle-mutation carrier:
    `SchemaUpdateEnvelopeV1` with `SchemaObjectKind::Index`
    and four `mutation` variants:
    - `Create` — operator-initiated; admits a new
      `SecondaryIndexDescriptor` in `Building`.
    - `Drop` — operator-initiated; transitions
      `Building`/`Ready` to `Dropping`.
    - `IndexBackfillComplete` — replica-initiated;
      transitions `Building → Ready` once historical
      backfill completes locally and the replica's local
      coverage tracker reports all-slabs-covered.
    - `IndexDropGcComplete` — replica-initiated;
      acknowledges per-replica completion of the local L1/
      L2 sidecar GC (see Phase 7). The catalog row goes
      absent only after every replica has replicated its
      own `IndexDropGcComplete`.
    All four reuse the existing
    `SchemaUpdateEnvelopeV1` Raft path, audit envelope,
    and reply discipline; no parallel mutation family is
    introduced. `Alter` is rejected at the schema-
    mutation layer with
    `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.

11. Freeze the typed lookup executor surface per ADR 052
    §12:

    ```rust
    pub struct TableLookupByIndexRequest { ... }
    pub enum IndexQuery {
        PointEqual { encoded_key: Vec<u8> },
        Range      { lower: IndexBound, upper: IndexBound },
        InList     { encoded_keys: Vec<Vec<u8>> },
    }
    ```

12. Architectural guards (TODO entries; populated by later
    phases):
    - `secondary_index_entry_element` uses
      `DOMAIN_SECONDARY_INDEX_ENTRY` only.
    - Index descriptors do not live inside
      `TableSchemaDescriptorV1`.
    - Index create / drop does **not** bump table
      `schema_generation`.
    - Index entries bind to the table's
      `LogicalSchemaFingerprint`; no binding to
      `PhysicalDescriptorFingerprint`.
    - No new digest domain.
    - No `__row_*__` synthetic column added for indexes.
    - No `SecondaryIndexDescriptorV1` / `IndexEntryV1` type
      suffixes. Construct forbidden literals via
      `concat!()` per the ADR 050 / 051 architecture-test
      convention.
    - No project-name prefix on new symbols.
    - No bump on any existing `format_version`. Sidecars
      carry their own `format_version` on a fresh
      artifact family; the guard is on **existing**
      fields only.
    - No new HRFT frame family.
    - No covering-index column carriage.
    - No multi-column / composite key path.
    - No expression-evaluation hook in admission.
    - No DDL parser.
    - No `Alter` mutation accepted at the schema-mutation
      path; the rejection surfaces via
      `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`, not
      via any `INDEX_REJECT_*` code.
    - Sidecar segments are a fresh artifact family; data
      slab format unchanged.
    - Read path always invokes `lookup_as_of` and rechecks
      PBC after candidate fetch.
    - Write admission does not roll back data writes on
      index buffer failure.
    - Coverage state is per-replica; no replicated ready-
      state watermark.
    - **Index emits on every non-NULL data-row write
      (including unchanged-value upserts).** NULL rows
      emit no entry; tombstones emit no entry.
    - **Lifecycle is linear**: absent → `Building` →
      `Ready` → `Dropping` → absent. No `Ready → Building`
      transition.
    - **Planner gates on `Ready`-state only.** `Building`
      and `Dropping` descriptors are not used for query
      planning; lookups against them route through
      fingerprint mismatch.

Behavior fixtures under `tests/fixtures/secondary_index/`:

- Create on supported non-PK column succeeds; descriptor
  enters `Building`.
- Backfill completes; descriptor transitions to `Ready`.
- Drop: descriptor transitions to `Dropping`; sidecars
  GC'd; descriptor goes absent.
- Create on PK column → `INDEX_REJECT_PK_COLUMN`.
- Create on `__row_*__` synthetic column →
  `INDEX_REJECT_SYNTHETIC_COLUMN`.
- Create with unsupported Arrow type →
  `INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE`.
- `Alter` rejected at the schema-mutation path with
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
- Index create / drop does not bump table
  `schema_generation`.
- Physical descriptor retune preserves index sidecar
  usability.
- Logical descriptor mutation makes old sidecars unusable.
- Insert one row → one index entry; digest +1 element.
- **Unchanged-value upsert: new version-entry emitted;
  range scan post-upsert returns the new row's locator
  (load-bearing — without per-version emission, the
  lookup silently misses K).**
- Indexed-value-changing upsert: old entry stays; new
  entry added; range scan over old value rejects via
  step 5; range scan over new value accepts.
- Delete: no index entry emitted; range scan rejects via
  step 4 (`lookup_as_of` returns tombstone).
- Resurrection: new entry added at resurrection HLC;
  range scan at resurrection HLC accepts.
- NULL indexed value: no entry emitted; admission
  succeeds; `IS NULL` predicate routes to scan.
- Row updated from non-NULL to NULL: no new entry
  emitted; old non-NULL entry remains; range scan over old
  value rejects via step 5 (current value is NULL,
  mismatches `encoded_key`).
- Row updated from NULL to non-NULL: new non-NULL entry
  emitted at the new HLC; range over new value accepts.
- Range scan over indexed_value range with mixed visible /
  superseded / mutated candidates.
- Point-equal lookup at multiple `as_of_hlc` showing time-
  travel through indexed-value mutations.
- Lookup with stale `LogicalSchemaFingerprint` →
  `INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH`.
- Lookup against `Building` index → fingerprint mismatch
  (planner refuses; no `Building`-specific reject).
- Lookup against `Dropping` index → fingerprint mismatch.
- Index admission failure on active-slab buffer marks slab
  uncovered; data write succeeds; subsequent scan returns
  same rows as indexed plan.

Exit:

- Contract pinned; fixtures fail with `unimplemented!()`
  until later phases.
- Architectural guards in place.

## Phase 1 — Catalog: descriptor, mutations, replication, lifecycle

Files:

- `src/schema/secondary_index.rs`
- `src/schema/mod.rs`
- `src/consensus/raft_storage.rs`
- `src/consensus/core.rs`
- `src/network/control_server.rs`
- `tests/secondary_index_catalog.rs`
- `tests/control_ring_secondary_index.rs`

Work:

1. Implement `SecondaryIndexCatalog` as a Raft-replicated
   state-machine table keyed by
   `(tenant_id, index_object_id)` and indexed by
   `(tenant_id, namespace, index_name)` and
   `(tenant_id, table_object_id)`. Each row carries
   `SecondaryIndexState`.

2. Extend `apply_schema_update` to route
   `SchemaObjectKind::Index { Create | Drop }`. `Alter`
   rejects at the schema-mutation path with
   `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`. Existing
   `SchemaObjectKind::Table` path unchanged.

3. Validate the descriptor at admission per ADR 052 §1
   (referenced table exists; generation + fingerprint match;
   column exists; not synthetic; not PK; type supported;
   name unique).

4. Lifecycle transitions, driven by the four mutation
   variants (Phase 0 §10):
   - `Create` accept: row created with
     `state = Building`.
   - `IndexBackfillComplete` accept (replica-initiated
     after Phase 4 §5): transition `Building → Ready`.
     Kept minimal; monotonically advances state.
   - `Drop` accept on `Ready` row: transition `Ready →
     Dropping`.
   - `Drop` accept on `Building` row: transition `Building
     → Dropping` (in-flight backfill aborts).
   - `IndexDropGcComplete` accept (replica-initiated
     after Phase 7 GC): records that *this* replica has
     completed local GC. Catalog row goes absent only
     once **every** replica has replicated its own
     `IndexDropGcComplete`; partial confirmations leave
     the row in `Dropping`.
   - No `Ready → Building` transition.

5. **Do not** bump table `schema_generation`. Confirm with
   a targeted test: a `TableSchemaDescriptorV1`'s generation
   and both fingerprints are unchanged across an index
   create / drop pair, including the intermediate
   `Building` and `Ready` and `Dropping` states.

6. Audit events on every accepted / rejected mutation:
   tenant id, table object id, index object id, index name,
   table schema generation, logical fingerprint, column
   name, action, lifecycle transition, outcome.

Tests:

- Catalog create / drop round-trip preserves replication
  state.
- Catalog drop is idempotent.
- Lifecycle transitions are linear and CAS-protected.
- No `Ready → Building` transition.
- Index name collision within `(tenant_id, namespace)`
  rejects.
- Unsupported Arrow type rejects at admission with
  `INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE`.
- PK column rejects at admission with
  `INDEX_REJECT_PK_COLUMN`.
- Synthetic column rejects at admission with
  `INDEX_REJECT_SYNTHETIC_COLUMN`.
- `Alter` rejects with
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` at the
  schema-mutation path; not via any `INDEX_REJECT_*` code.
- Stale `table_schema_generation` rejects.
- Stale `logical_schema_fingerprint` rejects.
- Index create / drop does NOT bump table
  `schema_generation` (positive guard).
- Replica state-machine convergence under partition.

Exit:

- Replicated catalog supports the full index lifecycle;
  mutations replicate; state transitions are audited;
  table descriptor is not perturbed.

## Phase 2 — Key encoding and digest helper

Files:

- `src/storage/secondary_index.rs`
- `src/hash/state_digest.rs`
- `tests/secondary_index_key_encoding.rs`
- `tests/state_digest_invariants.rs`

Work:

1. Implement
   `encode_secondary_index_key(array, row, descriptor) ->
   Option<Vec<u8>>`. Returns `None` if the row's indexed
   column is NULL (NULL excluded from V1 index).

2. Non-NULL encoding per ADR 052 §3:
   - bool: `0x00` false, `0x01` true.
   - unsigned integer: big-endian.
   - signed integer / date / timestamp: big-endian with
     sign bit flipped.
   - UTF-8 / binary: raw bytes.

3. No sentinel byte. The encoded key bytes are the
   ordered representation directly. `IS NULL` predicates
   route to scan.

4. Reject unsupported types at descriptor validation and
   defensive encode sites.

5. Implement `secondary_index_entry_element` per Phase 0
   §4 using `DOMAIN_SECONDARY_INDEX_ENTRY`.

6. Confirm `DOMAIN_SECONDARY_INDEX_ENTRY` already lives at
   `src/hash/state_digest.rs:19`; do not reallocate.

Tests:

- Golden encoded bytes for every supported type, including
  true, false, all signed-integer boundary values, max
  unsigned, empty UTF-8, multi-byte UTF-8, empty binary.
- Lexicographic order on encoded bytes matches typed value
  order.
- Sign-bit flip golden vectors for `Int64::MIN`, `-1`,
  `0`, `1`, `Int64::MAX`.
- NULL row returns `None` (no entry, no sentinel).
- Digest golden vector for one fixed entry (preimage byte-
  for-byte).
- Physical locator changes (slab rewrite) leave the digest
  element unchanged.
- Changing `logical_schema_fingerprint` changes the digest
  element.
- Changing `column_ordinal` changes the digest element.
- Changing `index_object_id` changes the digest element.
- Changing `tenant_id` changes the digest element.
- Two version-entries for same `(K, encoded_key)` at
  different `row_hlc` produce two distinct digest
  elements (load-bearing for the version-entry digest
  framing).

Exit:

- Key encoding and digest projection are stable, golden-
  vector-pinned, NULL-handled, independently tested.

## Phase 3 — Segment format and local persistent catalog

Files:

- `src/storage/secondary_index_segment.rs` (new)
- `src/storage/secondary_index.rs`
- `src/storage/index.rs` (local persistent metadata)
- `src/storage/manager.rs`
- `tests/secondary_index_segment.rs`

Work:

1. Define immutable segment types per ADR 052 §4:

   ```rust
   pub struct SecondaryIndexSegmentHeader { ... }
   pub struct SecondaryIndexEntry {
       pub encoded_key:      Vec<u8>,
       pub row_key_id:       Identity,
       pub row_hlc:          HybridTimestamp,
       pub record_location:  RecordLocation,
       pub record_row_index: RecordRowIndex,
   }
   pub struct SecondaryIndexSegment { ... }
   ```

   Header: magic, `format_version`, tenant id, table id,
   index id, source slab id, table schema generation,
   logical fingerprint, column ordinal, type tag, entry
   count, min / max encoded key, segment `StateDigest`,
   payload hash.

2. Encode entries sorted by:
   `(encoded_key, row_key_id, row_hlc, record_location,
   record_row_index)`.

3. Local persistent index for sidecar metadata:
   `(tenant, table, index, logical_fingerprint,
   source_slab_id) → segment pointer`.

4. Sidecar storage path convention:
   `secondary_index/<tenant>/<table>/<index>/<source_slab_id>.idx`
   in L1, mirrored to L2 alongside the source slab.

5. Segment decode validation:
   - header payload hash matches;
   - entries are sorted;
   - recomputed digest equals header digest;
   - logical identity fields match the descriptor used by
     the reader.

6. Defensive: a sidecar whose
   `logical_schema_fingerprint` does not equal the current
   descriptor's is silently ignored by the planner (not a
   hard error — coverage miss).

Tests:

- Segment encode / decode round-trip preserves entry order
  and digest.
- Tampered payload hash rejects with corruption metric.
- Tampered digest rejects.
- Unsorted entries reject.
- Sidecar with matching logical fingerprint accepts after
  table physical retune.
- Sidecar with mismatched logical fingerprint is silently
  ignored.
- Sidecar storage path round-trip through `ObjectStore`.

Exit:

- Sidecars can be serialized, stored locally, loaded,
  verified, tiered.

## Phase 4 — Backfill, active-slab buffer, coverage tracking

Files:

- `src/storage/secondary_index.rs`
- `src/storage/secondary_index_backfill.rs` (new)
- `src/storage/manager.rs`
- `src/storage/core.rs`
- `src/network/data_server.rs`
- `tests/secondary_index_backfill.rs`

Work:

1. Async per-replica backfill worker:
   - On `CreateSecondaryIndex` accept (descriptor in
     `Building`), enqueue backfill for every existing
     sealed slab of the target table.
   - For each sealed slab, **acquire a read-lease on
     the source slab** to pin it against compaction. If
     compaction claims the slab during build, abort
     backfill for that slab cleanly; the rebuilt sidecar
     will be emitted by the compaction path (Phase 6
     coverage healing).
   - Iterate **every retained non-tombstone data version**
     in the source slab — not `lookup_as_of(K, now)`
     winners. This matches the steady-state admission
     rule (one entry per data-row write) applied
     retroactively to history. Skip tombstone records.
     For each retained data version, compute
     `encoded_key`; if the indexed column is NULL on that
     version, skip emission (NULL exclusion is per-
     version, not per-row). Append the surviving entry
     to a per-slab segment buffer. The result is that an
     `as_of_hlc` query reaching a covered slab finds the
     version-entry that was visible at the requested
     time, just as steady-state admission would have
     produced if the index had existed when the data
     was first written.
   - Seal segment; verify digest; release lease;
     persist sidecar metadata; mark the slab covered.
   - On per-slab failure, mark the slab uncovered with
     an error reason; surface in metrics. Do not retry
     synchronously — compaction healing covers the slab
     when its source compacts.
   - When all historical slabs are covered, submit
     `IndexBackfillComplete` mutation; replication
     transitions catalog state to `Ready`.

2. Local coverage tracker per
   `(tenant, table, index, logical_fingerprint)`:
   - `covered_slabs: HashSet<SlabId>`
   - `active_slab_buffer: Option<ActiveBuffer>`
   - `uncovered_slabs: HashMap<SlabId, BackfillError>`
   - `last_backfill_error: Option<String>`

3. Active-slab admission rule (per ADR 052 §6):
   - For every `Building`-or-`Ready` index whose
     `logical_schema_fingerprint` matches the write
     descriptor, encode the indexed value for **every
     non-NULL data row** (including unchanged-value
     upserts); append `(encoded_key, row_key_id, row_hlc,
     record_location, record_row_index)` to the active
     buffer.
   - Skip emission if the indexed column is NULL
     (encoder returns `None`).
   - Skip emission for tombstone admission entirely.
   - **Do not** check whether the indexed value is the
     same as the prior version; emit unconditionally on
     every non-NULL data-row write.
   - **Memory bound**: the active buffer is bounded by
     `max_index_buffer_bytes` (operator tunable, default
     calibrated by Phase 9 bench). On overflow, drop the
     buffer, mark the active slab uncovered, increment
     `storage_secondary_index_active_buffer_failures_total
     {reason="overflow"}`, and continue admitting data
     writes. **Do not roll back the data write.**
   - On any other buffer-append failure (encode error,
     allocation failure), mark the slab uncovered with
     the corresponding `reason` label.
   - `Dropping` indexes do not emit.

4. Slab seal:
   - Flush active buffer into an immutable sidecar.
   - Verify digest.
   - Persist sidecar metadata.
   - Transition the slab from active to covered.
   - On failure, mark the slab uncovered.

5. Recovery: replay durability log; restore active-buffer
   state; re-enqueue backfill for any uncovered or
   partially sealed slabs; resume backfill for indexes
   stuck in `Building`.

6. Coverage exposed to the planner via a thread-safe
   accessor.

Tests:

- Backfill produces one sidecar per existing sealed slab.
- **Backfill emits one entry per retained non-tombstone
  data version, not one entry per "now"-visible row.**
  Setup: row K has `col=foo` at hlc=10 and `col=bar` at
  hlc=20, both retained. Index created at hlc=30.
  Backfill emits `(foo, K, 10, locator_v1)` AND
  `(bar, K, 20, locator_v2)`. Range over `col='foo'` at
  `as_of=15` accepts (revalidation step 4 returns the
  v=10 row, step 5 matches). Range over `col='foo'` at
  `as_of=now` rejects via step 4 (newer hlc=20 row).
  **Without retained-version iteration, the as_of=15
  query silently misses K.**
- Active writes appear in the active buffer and in the
  resulting sealed sidecar.
- **Unchanged-value upsert produces a new version-entry
  in the active buffer.** Range scan post-upsert through
  the indexed plan returns the new row's locator (the
  load-bearing test for the version-entry digest
  framing).
- **Indexed-value-changing upsert produces a new entry at
  the new value.** The old entry remains; revalidation
  rejects it on subsequent reads via step 5.
- Tombstone writes produce no entry.
- NULL indexed value produces no entry.
- Update from non-NULL to NULL produces no new entry; old
  non-NULL entry stays; revalidation step 5 rejects the
  stale non-NULL candidate.
- Update from NULL to non-NULL produces a new entry at
  the new HLC.
- Buffer-append failure marks the slab uncovered and the
  data write succeeds.
- Re-running backfill is idempotent.
- Coverage tracker survives restart.
- Multiple `Ready` indexes on same table all emit per
  admission.
- A `Building` index's active-slab buffer is populated
  immediately; the planner does NOT use the index
  (Phase 5 gates on `Ready`).
- Backfill complete triggers `Building → Ready`
  transition.
- `Dropping` indexes stop emitting.

Exit:

- Local index coverage converges without making data
  writes depend on index-segment success. Lifecycle
  transitions are exercised end-to-end.

## Phase 5 — Read path: planner extraction + candidate revalidation

Files:

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- `src/storage/secondary_index.rs`
- `src/schema/pbc.rs`
- `tests/secondary_index_query.rs`

Work:

1. Implement the internal typed lookup executor per
   ADR 052 §12:
   - `PointEqual`;
   - `Range`;
   - `InList`;
   - lifecycle gate: `Ready` only. `Building` and
     `Dropping` descriptors route through fingerprint
     mismatch.

2. Add PBC predicate extraction for indexed columns:
   equality, inclusive / exclusive range, IN-list. The
   extractor walks the PBC program, identifies single-
   column constraints whose column matches a `Ready`
   index, and produces an `IndexAccessPlan`.

3. Planner choice (heuristic, V1):
   - if no `Ready` index covers the predicate → full scan;
   - if `Ready` index exists and uncovered fraction below
     threshold AND extracted predicate selectivity below
     threshold → indexed plan;
   - otherwise → full scan.
   Thresholds runbook-tunable; calibrate from Phase 9
   bench.

4. Indexed plan execution:
   - Iterate covered sidecars whose
     `(min_encoded_key, max_encoded_key)` intersects the
     predicate range.
   - Stream entries that match the predicate.
   - For each candidate, run revalidation per ADR 052 §6:
     a. `logical_schema_fingerprint` match (already
        gated by sidecar load);
     b. Fetch row via `(slab_id, record_location,
        record_row_index)`;
     c. Verify `(__row_key_id__, __row_hlc__)` match;
     d. `lookup_as_of(tenant, row_key_id, as_of_hlc)`
        returns the same `(row_key_id, row_hlc)` and
        is not a tombstone;
     e. Re-encode the fetched row's indexed value and
        verify it equals the entry's `encoded_key`;
     f. Evaluate the full PBC predicate on the fetched
        row.
   - Add scan-tail rows from uncovered slabs (PBC scan
     path).
   - Deduplicate by
     `(row_key_id, row_hlc, record_location)`.

5. Classification triple:
   - `visible_count`: passed all revalidation steps.
   - `superseded_count`: rejected at step (d).
   - `stale_indexed_value_count`: rejected at step (e).
   PBC residual rejections flow through existing PBC
   counters.

6. `read_consistency` plumb-through:
   `LinearizableLeaderRead` and `BoundedStaleness` at the
   same shape ADR 044 / 045 use.

Tests:

- Equality predicate uses index and returns same result
  set as full scan.
- Range predicate likewise.
- IN-list predicate likewise.
- Extra residual PBC predicate rechecked at step (f).
- Partial coverage scans uncovered slabs.
- **Unchanged-value upsert: indexed plan returns the new
  row's locator; without the version-entry emission rule,
  this test fails.**
- Stale candidate after indexed-value-changing upsert:
  rejected at step (e); `stale_indexed_value_count`
  increments.
- Candidate after delete: rejected at step (d);
  `superseded_count` increments.
- Candidate before delete under `as_of`: accepted.
- Candidate after resurrection: returns the new data
  version's locator.
- Stale `logical_schema_fingerprint` →
  `INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH`.
- Lookup against `Building` index → fingerprint mismatch.
- Lookup against `Dropping` index → fingerprint mismatch.
- Lookup against dropped (absent) index → fingerprint
  mismatch.
- Indexed plan and full-scan plan return identical
  results on every fixture (regression invariant).

Exit:

- Index-assisted reads are behaviorally identical to full
  scans, with the planner choosing the indexed path when
  beneficial and gating strictly on `Ready`-state.

## Phase 6 — Compaction, retention, tiering

Files:

- `src/storage/sweeper.rs`
- `src/storage/manager.rs`
- `src/storage/secondary_index.rs`
- `src/storage/index.rs`
- `tests/secondary_index_compaction.rs`

Work:

1. Source-slab compaction hook: when a slab compacts,
   rebuild the sidecar over that slab for every index in
   `Building` **or** `Ready` state. Both states emit on
   admission and both have sidecars (or pending coverage)
   to maintain. `Dropping` indexes do not get a rebuild
   — Phase 7 GC reclaims their bytes instead. Obsolete
   entries (tombstoned or overwritten rows) are naturally
   omitted from survivors; the rebuilt sidecar is purged
   of stale candidates automatically.

   **Why `Building` is included.** A `Building` index's
   backfill worker may have lost a read-lease race
   against compaction (Phase 4 §1); without compaction
   rebuilding for `Building` indexes, the output slab
   would be uncovered for that index until the backfill
   worker's next pass — racing the compactor again.
   Compaction-time emission for `Building | Ready`
   closes the race cleanly: either the backfill worker
   wins and seals the input slab's sidecar, or
   compaction wins and seals the output slab's sidecar.
   Either way the slab ends up covered without retry
   loops.

   **Coverage healing**: compaction over an *uncovered*
   source slab also emits a sidecar for the output slab.
   Compaction does not branch on input coverage state;
   it always emits a sidecar for the output (for every
   `Building`-or-`Ready` index). This makes uncovered
   slabs (e.g., from Phase 4 buffer overflow or backfill
   abort) self-healing on the natural compaction
   cadence.

2. Retention rules per ADR 048 / 050:
   - Append-only / content-hash pair-GC: rebuilt sidecar
     excludes pair-GC'd data entries.
   - PK prefix retention: rebuilt sidecar indexes the
     retained data boundary; excludes a retained
     tombstone boundary (tombstones produce no entry).
   - PK gate (`RETENTION_REJECTED_PK_DISALLOWED`) remains
     in force.

3. Older superseded PK data versions retained above the
   floor remain in sidecars; revalidation decides
   visibility at `as_of`.

4. Verify rebuilt sidecar digest before retiring the old
   sidecar. Mismatch fails closed with a corruption
   metric.

5. Tiering:
   - Slab L1 → L2 promotion moves the sidecar alongside.
   - Local persistent metadata updates atomically with
     the slab pointer.

Tests:

- Compaction removes entries for GC'd rows.
- PK tombstone boundary produces no index entry.
- PK data boundary produces one index entry.
- Resurrection above floor → sidecar contains all
  retained versions.
- Sidecar digest mismatch on rebuild forces fallback.
- Tiered slab can serve indexed candidates from L2.
- Compacted sidecar digest equals an oracle digest.
- Append-only pair-GC tests on the data axis remain
  unchanged.
- Compaction collapses unchanged-value upsert version-
  entries: pre-compaction sidecar has multiple
  version-entries for `(K, foo)`; post-compaction
  sidecar has one entry for the survivor version.
- **Compaction rebuilds for a `Building` index.** Setup:
  index in `Building`; backfill loses the read-lease
  race against compaction on slab S. Compaction emits
  the output slab's sidecar for the `Building` index
  even though its catalog row is not yet `Ready`.
  Backfill's coverage tracker observes the new sidecar
  and marks S' covered without re-running backfill on
  S. Without `Building`-state rebuild, the test fails
  (uncovered slab; backfill must re-race the
  compactor).

Exit:

- Indexes stay correct across slab lifecycle and storage
  movement.

## Phase 7 — Two-phase drop-index garbage collection

Files:

- `src/storage/secondary_index.rs`
- `src/storage/sweeper.rs`
- `src/consensus/raft_storage.rs`
- `tests/secondary_index_drop_gc.rs`

Work — **explicit three-step replicated discipline** to
prevent crash-leaks:

1. **Drop mutation accept (phase 1):** `SchemaUpdateEnvelopeV1`
   `Drop` transitions the catalog row to `Dropping`.
   Replication carries the transition. Effects on every
   replica:
   - new admissions stop emitting for the dropped
     `index_object_id`;
   - reads against the index reject via fingerprint
     mismatch;
   - in-progress backfill aborts (the lease from Phase 4
     §1 is released).

2. **Background GC pass (phase 2):** the per-replica
   sweeper scans local sidecar metadata for sidecars in
   `Dropping` state; tombstones the sidecar pointers;
   deletes sidecar bytes from L1; emits a tier-aware
   delete to L2. The pass is **idempotent and crash-safe**
   — a crash mid-GC leaves the catalog row in `Dropping`,
   and the next pass resumes. No replica advances past
   `Dropping` until its local GC completes.

3. **Per-replica `IndexDropGcComplete` confirmations
   (phase 3):** once a replica's local GC completes, it
   submits an `IndexDropGcComplete` mutation. The state
   machine records the confirmation against the
   originating replica id. The catalog row is removed
   only after *every* replica in the current cluster
   membership has replicated its own
   `IndexDropGcComplete`; partial confirmations leave
   the row in `Dropping`. A single replica's
   confirmation is **not** sufficient — that would let
   slow or partitioned replicas keep sidecar bytes after
   the descriptor went absent, reintroducing the crash-
   leak the two-phase design is meant to prevent.

4. Read path against any `Dropping` or absent index
   rejects via fingerprint mismatch.

5. Audit events at each phase: drop-mutation accept
   (with lifecycle transition), drop-GC start, drop-GC
   complete (per replica), descriptor removal.

Tests:

- Drop completes catalog mutation; descriptor in
  `Dropping`.
- Background GC removes sidecars.
- Reads during `Dropping` window reject (fingerprint
  mismatch).
- Catalog row goes absent after GC completes.
- Re-creating an index with the same name (new
  `IndexObjectId`) does not collide with the dropped
  index's GC.
- Drop on a `Building` index aborts in-flight backfill.

Exit:

- Drop semantics are clean and auditable across the
  lifecycle.

## Phase 8 — Recovery, tiered metadata convergence

Files:

- `src/consensus/raft_storage.rs`
- `src/storage/secondary_index.rs`
- `src/storage/secondary_index_segment.rs`
- `tests/secondary_index_recovery.rs`

Work:

1. Recovery from durability log:
   - Catalog state restores from Raft state machine.
   - Active-slab buffers reconstruct.
   - Sealed sidecar metadata loads from local persistent
     index.
   - Sidecar headers re-validate lazily on first read or
     during a scheduled scan; corrupt headers fail closed
     with a corruption metric and the slab is marked
     uncovered.

2. Tiered metadata convergence:
   - L2 sidecars whose digest does not match the
     catalog's expected `logical_schema_fingerprint` are
     silently ignored (planner coverage miss); not a
     hard error.
   - L1 ↔ L2 pointer convergence runs through existing
     tier-aware metadata refresh.

3. Recovery from a partial backfill: the affected slab is
   marked uncovered; backfill re-enqueues; the data write
   stream is unaffected. Indexes stuck in `Building`
   resume backfill and eventually transition to `Ready`.

Tests:

- Restart preserves index state for covered slabs.
- Restart re-enqueues backfill for uncovered slabs.
- Restart resumes backfill for `Building` indexes.
- Corrupt sidecar fails closed with metric; slab marked
  uncovered; planner falls through.
- Tiered convergence: L1 sidecar matches catalog
  descriptor; L2 sidecar with stale fingerprint silently
  ignored.

Exit:

- Recovery and tiered convergence survive index lifecycle
  failures without breaking the data path.

## Phase 9 — Bench

Files:

- `benches/secondary_index.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/052_secondary_indexes_bench_results.md` (new,
  populated when the bench runs)

Bench matrix:

1. **Key encoding cost** by supported Arrow type at 100k
   and 1M rows.

2. **Sidecar build cost** (100k and 1M rows):
   - narrow rows / wide rows;
   - low / high indexed-column cardinality.

3. **Sidecar range scan**:
   - equality;
   - narrow range (~1% selectivity);
   - wide range (~50% selectivity);
   - full scan equivalent;
   - IN-list of size 1, 10, 100.

4. **Candidate validation**:
   - all visible (no supersession or value mutation);
   - 50% superseded by data-axis tombstone;
   - 50% superseded by indexed-value mutation;
   - 50% stale by unchanged-value upsert (load-bearing
     for the version-entry framing);
   - resurrection mix.

5. **Hybrid plan crossover**:
   - 0%, 10%, 50%, 100% uncovered slabs.
   - Calibrate the threshold runbook tunables.

6. **Compaction rebuild**:
   - append-only pair-GC;
   - PK prefix retention with data boundary;
   - PK prefix retention with tombstone boundary;
   - data-only rewrite;
   - **unchanged-value-upsert collapse**: pre-compaction
     sidecar with N version-entries → post-compaction
     sidecar with 1 entry per visible row.

7. **Digest verification**:
   - segment digest recompute;
   - cross-replica point comparison cost.

8. **Bench partitioning** (mandatory):
   - Isolate **lookup cost** from **revalidation cost**
     from **PBC recheck cost** so a slow path has a
     named source.

Acceptance starting points:

- Indexed equality on fully covered slabs materially
  faster than full scan at 1% selectivity.
- Hybrid plan never slower than full scan by more than
  the calibrated threshold; otherwise planner chooses
  full scan.
- Sidecar build linear in row count.
- Candidate validation cost isolated and named per
  source.
- Digest verification effectively constant per segment.
- Compaction-rebuild cost linear in survivor count.

Bench results in
`plans/impl/052_secondary_indexes_bench_results.md`.
Quick smoke acceptable for code review; full 100k / 1M
matrix is the ADR 052 promotion-to-shipped gate.

## Phase 10 — Architectural guards, metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/audit.rs`
- `tests/metrics_smoke.rs`
- `tests/architecture_test.rs`
- `plans/ops/observability.md`
- `plans/ops/runbooks/secondary_indexes.md` (new)
- `plans/ops/runbooks/README.md`

Metrics:

```text
storage_secondary_index_create_total{tenant, table, index}
storage_secondary_index_drop_total{tenant, table, index}
storage_secondary_index_lifecycle_transitions_total{tenant, table, index, from, to}
storage_secondary_index_segments_built_total{tenant, table, index}
storage_secondary_index_segments_failed_total{tenant, table, index, reason}
storage_secondary_index_entries_indexed_total{tenant, table, index}
storage_secondary_index_active_buffer_failures_total{tenant, table, index, reason}
storage_secondary_index_query_plans_total{tenant, table, index, mode}
storage_secondary_index_candidate_rows_total{tenant, table, index}
storage_secondary_index_visible_total{tenant, table, index}
storage_secondary_index_superseded_total{tenant, table, index}
storage_secondary_index_stale_indexed_value_total{tenant, table, index}
storage_secondary_index_uncovered_slabs_total{tenant, table, index}
storage_secondary_index_backfill_seconds{tenant, table, index}
storage_secondary_index_segment_state_digest_mismatch_total{tenant, table, index}
storage_secondary_index_logical_fingerprint_mismatch_total{tenant, table, index}
storage_secondary_index_corruption_failures_total{tenant, table, index}
storage_secondary_index_drop_gc_seconds{tenant, table, index}
```

Audit:

- One audit event per accepted / rejected `Create` /
  `Drop` / `IndexBackfillComplete` schema-update with
  index payload.
- One per backfill-start, backfill-complete (with segment
  count, entry count, uncovered slab count, digest
  verification result, duration), backfill-rollback.
- One per drop-GC-complete.
- Per-row admission emissions do **not** emit audit
  events.
- Per-lookup audit not emitted.

Runbook (`secondary_indexes.md`):

- When secondary indexes are appropriate vs CSB / PBC
  fall-through.
- Reject taxonomy and operator interpretation. Lookup-time
  rejects use the `INDEX_REJECT_*` prefix; the schema-
  mutation `Alter` reject uses
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` and
  surfaces on the schema-update reply path.
- Lifecycle states and what each gates.
- Hybrid plan correctness (uncovered slabs are scanned).
- **Storage amplification on update-heavy tables**:
  unchanged-value upserts emit version-entries; sidecar
  size grows until source-slab compaction runs;
  operators should expect
  `secondary_index_entries_indexed_total` to outpace
  visible-row count and watch
  `secondary_index_compaction_rebuild` cadence.
- Indexed-value mutation: `stale_indexed_value_total` is
  expected; not a bug.
- NULL handling: V1 excludes NULL; `IS NULL` predicates
  fall through to scan. Future variant ADR may admit
  NULL via sentinel encoding.
- Composition with retention and ADR 050 PK gate.
- Composition with ADR 051 logical fingerprint binding.
- Identity-vs-Locality framing on the index axis (sidecar
  preserves single-locality model).
- Cluster-wide ready-state is per-replica in V1.

Architectural guards (extend
`tests/architecture_test.rs`):

**Convention.** Forbidden-literal checks construct needles
via `concat!(...)` per the ADR 050 / 051 architecture-test
convention.

Guards to add:

- ☐ `secondary_index_entry_element` exists and uses
  `DOMAIN_SECONDARY_INDEX_ENTRY` only.
- ☐ Index descriptors do not appear inside
  `TableSchemaDescriptorV1`.
- ☐ Index create / drop does not bump table
  `schema_generation` (positive guard).
- ☐ Index entries bind to the table's
  `LogicalSchemaFingerprint`. Construct forbidden binding
  to physical via
  `concat!("PhysicalDescriptorFingerprint", "")` and
  assert it does not appear in
  `src/storage/secondary_index*.rs` or
  `src/schema/secondary_index.rs`.
- ☐ No new digest domain. Construct forbidden needles
  via `concat!("digest.secondary_index.", X, ".v1")` for
  values of `X` other than `entry`.
- ☐ No `IndexLogicalFingerprint` newtype or
  `digest.table.index.descriptor.logical.v1` domain.
- ☐ No `__row_*__` synthetic column added for indexes.
- ☐ No `SecondaryIndexDescriptorV1` / `IndexEntryV1` /
  `IndexLocatorV1` / `SecondaryIndexCatalogV1` suffixed
  types. Construct forbidden needles via
  `concat!("SecondaryIndexDescriptor", "V1")` etc.
- ☐ No project-name prefix on new symbols
  (`WendelonIndex*`, `wendelon_index_*`). Construct
  forbidden needles via `concat!("Wendelon", "Index")`
  and `concat!("wendelon", "_index_")`.
- ☐ No bump on any existing `format_version`. The
  per-source-slab sidecar header carries its own
  `format_version` field (Phase 3 §1) on a fresh
  artifact family; pre-existing `format_version`
  constants on envelope / data-slab footer / CSB
  header / table descriptor are unchanged from `main`.
- ☐ No new HRFT frame family.
- ☐ No covering-index column carriage on
  `SecondaryIndexEntry`.
- ☐ No multi-column / composite key code path.
- ☐ No expression-evaluation hook in admission.
- ☐ No DDL parser entry point.
- ☐ `Alter` mutation rejected at the schema-mutation
  layer with `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
  Positive guard: the constant exists and is returned by
  the index `Alter` handler.
- ☐ `Alter` rejection is **not** carried by an
  `INDEX_REJECT_*` code. Construct forbidden needle via
  `concat!("INDEX_REJECT_", "ALTER")` and assert it does
  not appear in `src/` or `tests/` (the
  `INDEX_REJECT_*` family stays four codes).
- ☐ Read path always invokes `lookup_as_of` and rechecks
  PBC. Positive guard: both appear in the indexed-plan
  execution function.
- ☐ Write admission failure on index buffer does not
  roll back the data write. Positive guard: failure
  path calls `mark_slab_uncovered` and returns success
  on the data axis.
- ☐ Index emits on every data-row write (positive
  guard): no admission code path checks whether prior
  indexed value equals new indexed value before
  emitting.
- ☐ Tombstone admission path does NOT call
  `admit_index_emission`.
- ☐ NULL row admission does NOT call the entry-append
  path (positive guard: encoder returns `None`;
  admission helper short-circuits without touching the
  active buffer).
- ☐ Active-slab buffer has a `max_index_buffer_bytes`
  bound (positive guard: the buffer's overflow path
  exists and increments the
  `active_buffer_failures_total{reason="overflow"}`
  metric).
- ☐ Backfill worker acquires a read-lease on the source
  slab and releases it on seal or abort.
- ☐ Drop is two-phase replicated: `Dropping` state
  persists until `IndexDropGcComplete` is emitted by
  every replica; descriptor removal happens only after
  all-replica confirmation.
- ☐ Compaction emits a sidecar for every output slab
  regardless of input coverage state (coverage healing).
- ☐ Lifecycle transitions are linear; no
  `Ready → Building` transition. Construct forbidden
  needle via `concat!("Ready", " → ", "Building")` (or
  the equivalent enum-pair check) and assert no
  transition function admits it.
- ☐ Planner gates on `Ready`-state only. Positive guard:
  the indexed-plan executor starts with a `Ready`
  check.
- ☐ Coverage state is per-replica; no replicated ready-
  state watermark exists in `src/consensus/`.
- ☐ `RETENTION_REJECTED_PK_DISALLOWED` remains enforced
  (positive guard).
- ☐ `digest.table.descriptor.physical.v1` (ADR 051) is
  the only physical descriptor digest domain.

Exit:

- Operators can observe index lifecycle, audit creates /
  drops / backfills / drop-GCs, follow a runbook.
- Architecture is mechanically protected.

## Verification gate

Required before declaring ADR 052 shipped:

```sh
cargo test --lib secondary_index
cargo test --lib index
cargo test --lib storage::secondary_index
cargo test secondary_index --quiet
cargo test secondary_index_catalog --quiet
cargo test control_ring_secondary_index --quiet
cargo test secondary_index_key_encoding --quiet
cargo test secondary_index_segment --quiet
cargo test secondary_index_backfill --quiet
cargo test secondary_index_query --quiet
cargo test secondary_index_compaction --quiet
cargo test secondary_index_drop_gc --quiet
cargo test secondary_index_recovery --quiet
cargo test --test architecture_test --quiet
cargo test --test state_digest_invariants --quiet
cargo test --test descriptor_validation --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench secondary_index --no-run
cargo bench --bench secondary_index -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Domain-tag well-formedness (extends ADR 046 / 047 / 049
  / 050 / 051):
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output is the existing set plus exactly one entry
  exercised by ADR 052: `digest.secondary_index.entry.v1`
  (already reserved in `src/hash/state_digest.rs:19`).

- No new descriptor-axis fingerprint on the table:
  ```sh
  rg -n 'digest\.table\.descriptor\.' src tests benches | sort -u
  ```
  Output is unchanged from ADR 051's
  `digest.table.descriptor.physical.v1`.

- No `IndexLogicalFingerprint` newtype or domain:
  ```sh
  rg -n 'IndexLogicalFingerprint|digest\.table\.index\.descriptor' src tests benches
  ```
  Empty.

- No new HRFT frame family:
  ```sh
  rg -n 'TABLE_(LOOKUP_BY_INDEX|INDEX_LOOKUP|INDEX_*)_REQ' src/schema src/network
  ```
  Empty.

- No bump on any **existing** `format_version`. The
  expected diff vs `main` is exactly one new occurrence:
  the sidecar header's own `format_version` field on the
  fresh artifact family. All pre-existing
  `format_version` constants (envelope, data-slab footer,
  CSB header, table descriptor) keep their current
  values:
  ```sh
  rg -n 'format_version' src tests benches | sort -u
  ```
  Pre-existing call sites unchanged from `main`; only
  new occurrences are inside
  `src/storage/secondary_index_segment.rs` and the
  matching tests / benches for the new sidecar family.

- Lazy supersession discipline (no Tombstone kind):
  ```sh
  rg -n 'IndexEntryKind|IndexEntry::Tombstone' src tests benches
  ```
  Empty.

- No covering-index payload:
  ```sh
  rg -n 'covering|covered_columns|index_payload_columns' src tests
  ```
  Empty.

- No multi-column index path:
  ```sh
  rg -n 'composite_index|multi_column_index|indexed_columns' src tests
  ```
  Empty.

- No expression-evaluation hook:
  ```sh
  rg -n 'expression_index|functional_index|expr_eval_index' src tests
  ```
  Empty.

- No project-name prefix on index symbols:
  ```sh
  rg -n '\bWendelonIndex[A-Z]|\bwendelon_index_[a-z_]' src tests benches
  ```
  Empty.

- No version suffixes on ADR 052 Rust types:
  ```sh
  rg -n 'SecondaryIndexDescriptorV[0-9]|SecondaryIndexEntryV[0-9]|SecondaryIndexCatalogV[0-9]|IndexObjectIdV[0-9]' src tests
  ```
  Empty.

- `RETENTION_REJECTED_PK_DISALLOWED` remains enforced:
  ```sh
  rg -n 'RETENTION_REJECTED_PK_DISALLOWED' src tests
  ```
  Present.

- Index entries do **not** bind to the table's physical
  fingerprint:
  ```sh
  rg -n 'PhysicalDescriptorFingerprint' src/storage/secondary_index*.rs src/schema/secondary_index.rs
  ```
  Empty.

- Index create / drop does NOT bump table generation:
  ```sh
  rg -n 'SchemaObjectKind::Index' src/consensus/raft_storage.rs
  ```
  Present; surrounding code does not call
  `bump_schema_generation`.

- `Alter` rejection lives at the schema-mutation layer
  under `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`:
  ```sh
  rg -n 'SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED' src tests
  ```
  Present (defined as a constant; returned by the index
  `Alter` handler in `src/consensus/raft_storage.rs`).

- `Alter` rejection is **not** carried by an
  `INDEX_REJECT_*` code:
  ```sh
  rg -n 'INDEX_REJECT_ALTER' src tests
  ```
  Empty (the `INDEX_REJECT_*` family stays at four codes;
  the schema-mutation reject is a separate constant).

- Synthetic-column / PK-column index descriptor
  rejection:
  ```sh
  rg -n 'INDEX_REJECT_SYNTHETIC_COLUMN|INDEX_REJECT_PK_COLUMN' src tests
  ```
  Present and exercised.

- No `INDEX_REJECT_NULL_INDEXED_VALUE_LOOKUP` reject
  code:
  ```sh
  rg -n 'INDEX_REJECT_NULL' src tests
  ```
  Empty (NULL is excluded from the V1 index; `IS NULL`
  routes to scan; no reject needed).

- Active-slab buffer memory bound is in code:
  ```sh
  rg -n 'max_index_buffer_bytes' src tests
  ```
  Present and exercised.

- Backfill acquires a read-lease against compaction:
  ```sh
  rg -n 'source_slab_lease\|read_lease.*backfill\|backfill.*read_lease' src tests
  ```
  Present.

- Two-phase drop discipline:
  ```sh
  rg -n 'IndexDropGcComplete' src tests
  ```
  Present and exercised.

- Lifecycle gating:
  ```sh
  rg -n 'SecondaryIndexState::(Building|Ready|Dropping)' src tests
  ```
  Present and exercised. The indexed-plan executor checks
  `Ready` before proceeding.

Convention guards:

- No `#[serde(default)]` shim on
  `SecondaryIndexDescriptor` fields.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change for data slabs.
- Reject-code disposition (Phase 0 §9), create-carrier
  decision (Phase 0 §10), and emission rule (Phase 0 §8)
  are settled in code.

## File-by-file expected edits

`src/schema/units.rs`
- Add `IndexObjectId` newtype.

`src/schema/secondary_index.rs` (new)
- `SecondaryIndexDescriptor`, `SecondaryIndexKeyDescriptor`,
  `IndexArrowTypeTag`, `IndexKeyEncoding`,
  `SecondaryIndexCatalog`, `SecondaryIndexState`.
- Descriptor validation (PK rejection, synthetic-column
  rejection, type-set check, name-uniqueness check,
  fingerprint match).

`src/schema/mod.rs`
- Re-export index schema types.

`src/consensus/raft_storage.rs`
- Add index catalog to `SmState`.
- Implement `SchemaObjectKind::Index` create / drop /
  backfill-complete; reject `Alter` with
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
- Lifecycle CAS transitions.
- Audit detail.
- Positive guard: do not bump table `schema_generation`
  on index mutations.

`src/consensus/core.rs`
- Imports only if `SchemaUpdate` envelope is sufficient.

`src/network/control_server.rs`
- Wire tenant-admin handling for index schema updates if
  needed; include
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED` in the
  schema-update reply path's reason-code mapping.

`src/storage/secondary_index.rs` (new)
- Key encoding (non-NULL ordered scalar; NULL → `None`,
  no entry); entry digest helper; admission emission
  (emit on non-NULL data row / no-emit on NULL or
  tombstone); candidate revalidation; bounded active-slab
  buffer; coverage tracker.

`src/storage/secondary_index_segment.rs` (new)
- Sidecar header / entry / segment encode-decode; payload
  hash; digest verification.

`src/storage/secondary_index_backfill.rs` (new)
- Async per-replica builder; active-slab buffer; failure
  handling; lifecycle transition trigger.

`src/storage/index.rs`
- Local persistent metadata for sidecar pointers.

`src/storage/manager.rs`
- Coordinate coverage, admission integration, query
  access.

`src/network/data_server.rs`
- Planner predicate extraction; indexed-plan execution;
  classification triple; lifecycle gating.

`src/storage/sweeper.rs`
- Compaction / retention / drop-GC sidecar rebuild.

`src/storage/columnar.rs`
- Tiering integration.

`src/hash/state_digest.rs`
- Confirm `DOMAIN_SECONDARY_INDEX_ENTRY` is consumed; add
  helper if needed.

`src/metrics/registry.rs`, `src/audit.rs`
- New metrics and audit events.

`tests/architecture_test.rs`
- ADR 052 guards.

`plans/ops/observability.md`
- Document new metrics.

`plans/ops/runbooks/secondary_indexes.md` (new)
- Runbook.

`plans/ops/runbooks/README.md`
- Add index runbook to the index.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- Index metadata is a separate replicated catalog object;
  table descriptor and table generation are unchanged.
- Index create / drop carrier:
  `SchemaUpdateEnvelopeV1` with
  `SchemaObjectKind::Index`. `Alter` rejected at the
  schema-mutation path with
  `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`.
- Catalog rows carry
  `SecondaryIndexState ∈ {Building, Ready, Dropping}`.
  State gates planner use, not active-slab admission.
- One non-unique ordered scalar column per index.
- Indexed types: bool, signed/unsigned ints,
  date/timestamp, Utf8/LargeUtf8, Binary/LargeBinary.
  Float / decimal / nested deferred.
- NULL excluded from V1 index. `IS NULL` predicates fall
  through to scan. Future variant ADR may admit NULL via
  sentinel encoding when a consumer demands it.
- Index segments are per-source-slab sidecars.
- Locator tail is `(record_location, record_row_index)`
  with `record_row_index: u32`; `slab_id` implicit in the
  sidecar binding.
- Index compatibility binds to the table's
  `LogicalSchemaFingerprint`; physical retunes leave
  sidecars valid; logical mutations require drop +
  create.
- Per-segment `StateDigest` consumes
  `digest.secondary_index.entry.v1` (ADR 046
  reservation), over **logical version-entries** —
  every data-row write contributes one element.
- Indexes are derived accelerators with per-replica
  coverage; uncovered slabs scanned.
- Lazy supersession: tombstones emit no entry; every
  candidate is revalidated at read time via
  `lookup_as_of` + indexed-value recheck + PBC recheck.
- Compaction rebuilds sidecars from survivor data rows
  on source-slab compaction; obsolete entries naturally
  omitted.
- PK retention stays cluster-MVCC prefix per ADR 050;
  `RETENTION_REJECTED_PK_DISALLOWED` remains in force.
- Reject taxonomy partitioned across two namespaces:
  four `INDEX_REJECT_*` codes
  (`LOGICAL_FINGERPRINT_MISMATCH`,
  `UNSUPPORTED_COLUMN_TYPE`, `PK_COLUMN`,
  `SYNTHETIC_COLUMN`) for lookup / descriptor-validation,
  plus `SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED`
  surfaced at the schema-update reply path. The two
  surfaces are deliberately separate — different call-
  paths, different operator audiences.
- Active-slab buffer bounded by `max_index_buffer_bytes`;
  overflow drops the buffer and marks the slab uncovered.
- Backfill acquires a read-lease on each source slab to
  pin it against compaction; lease loss aborts backfill
  for that slab (compaction healing covers it).
- Two-phase replicated drop GC: catalog row holds
  `Dropping` until local GC completes on all replicas.
- Compaction over an uncovered source slab heals
  coverage by emitting a sidecar for the output slab.
- No SQL parser; reuse `SchemaUpdateEnvelopeV1`.
- No new HRFT frame family.
- Minimal planner integration: equality, range, IN-list
  with hybrid fallback over `Ready`-state indexes.
  Cost-based optimizer deferred.
- Operator metrics for lifecycle, admission, lookup,
  classification, coverage, corruption, drop-GC.

Out of Wendelon's scope (client-tool ADR territory): DDL
syntax for `CREATE INDEX … ON … (col)`; cost-based
optimizer; client-side index-aware query rewriting.

## Deferred to follow-up ADRs

Mirrors ADR 052 *Deferred* section:

1. Cost-based optimizer for index access path selection.
2. Online concurrent index backfill.
3. Composite / multi-column indexes.
4. Expression / functional indexes.
5. Covering indexes.
6. Unique-constraint enforcement.
7. Partial / filtered indexes.
8. Full-text / inverted indexes.
9. ANN / vector indexes.
10. Range-sharded ring projection.
11. Per-column structural fingerprint.
12. Anti-entropy reader / fanout consumer.
13. Full-chain key retirement using sidecar cross-slab
    evidence.
14. CDC index-axis event surface.
15. Cluster-wide ready-state replication.
16. Float / decimal / dictionary / nested-type indexed
    columns.
17. NULL-included index variant (V1 excludes NULL).
18. Visible-membership digest as a separate domain.
19. Index-assisted joins.
