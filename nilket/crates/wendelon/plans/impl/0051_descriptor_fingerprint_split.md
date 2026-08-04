# Implementation Plan: ADR 051 — Logical / Physical Descriptor Fingerprint Split

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/0051_descriptor_fingerprint_split.md`

Inherits descriptor surface:
`plans/adr/0037_tenant_table_schemas.md`
Inherits identity policies (logical preimage inputs):
`plans/adr/0045_pk_less_append_only_tables.md`,
`plans/adr/0049_content_hash_row_identity.md`
Inherits BLAKE3 / domain-tag discipline:
`plans/adr/0046_state_digest_substrate.md`
Inherits CSB header surface:
`plans/adr/0039_slab_footer_column_statistics_v1.md`,
`plans/adr/0040_aggregate_pushdown.md`
Inherits PK-delete admission discipline (regression
audit): `plans/adr/0050_pk_delete_supersession.md`
Bench reference:
`plans/impl/0046_state_digest_bench_results.md`,
`plans/impl/0049_content_hash_bench_results.md`.

## Goal

Replace `TableSchemaDescriptorV1::schema_fingerprint:
[u8; 32]` with two newtyped fingerprints —
`LogicalSchemaFingerprint` and
`PhysicalDescriptorFingerprint` — over domain-tagged
BLAKE3 preimages with the physical preimage embedding
the logical fingerprint. Migrate every existing call
site to the **logical** fingerprint (aggregate
idempotency, aggregate wire, CSB header, CSB merge
eligibility, columnar directory). CSB merge and direct-
answer paths additionally validate parsed payload
coverage (Arrow stats tag, `stat_flags`, optional
payload availability) after descriptor-level logical
identity matches. Physical fingerprint serves V1 in-
memory planning caches and the future LSM segment merge.
Reinterpret the CSB header's fingerprint slot as logical
bytes; accept the pre-launch one-time cliff for pre-ADR-
051 slabs.

The implementation lets ADR 051 settle:

- two distinct fingerprint newtypes with constructor
  discipline (`newtype-units`);
- domain-tagged BLAKE3 preimages
  (`digest.table.schema.logical.v1`,
  `digest.table.descriptor.physical.v1`);
- stacked composition (physical preimage embeds logical
  fingerprint);
- consumer routing per ADR §6;
- payload-coverage validation per ADR §7;
- reject-code rename
  (`PBC_REJECT_STATS_FINGERPRINT_MISMATCH` →
  `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`);
- pre-launch postcard schema break on the descriptor;
- pre-launch CSB header slot reinterpretation cliff;
- architectural guards on routing correctness;
- bench coverage for fingerprint compute / validate /
  CSB eligibility / payload-coverage costs;
- regression audit confirming ADR 047 / 048 / 049 / 050
  preimages stay fingerprint-free.

## Deliverables

- New module `src/hash/descriptor_fingerprint.rs`
  defining the two newtypes, the two preimage helpers,
  and the domain-tag constants.
- `TableSchemaDescriptorV1::schema_fingerprint` field
  replaced by `logical_schema_fingerprint:
  LogicalSchemaFingerprint` and
  `physical_descriptor_fingerprint:
  PhysicalDescriptorFingerprint`.
- `compute_schema_fingerprint` replaced by
  `compute_logical_schema_fingerprint`,
  `compute_physical_descriptor_fingerprint(logical)`,
  and `refresh_descriptor_fingerprints`.
- `validate()` recomputes both fingerprints and strict-
  rejects on mismatch.
- Aggregate request idempotency + wire envelope
  migrated to logical
  (`src/storage/aggregate.rs:135-136, 276-277, 1367`,
  `src/schema/aggregate.rs:339, 1367`).
- CSB compaction eligibility migrated to logical
  (`src/storage/stats.rs:1959-1963`); reject code
  renamed to `_LOGICAL_*` (`src/storage/stats.rs:391`).
- CSB header on-disk slot reinterpretation as logical
  fingerprint (`src/storage/stats.rs:71-96`).
- CSB merge / direct-answer paths gain payload-coverage
  validation (column ordinal, Arrow stats tag,
  `stat_flags`, optional payload presence).
- Columnar directory validation migrated to logical
  (`src/storage/columnar.rs:381, 1035, 2443`).
- Tiered stats cache keys updated to use logical
  fingerprint plus existing slab locator fields
  (`stats_format_version`, `stats_block_len`,
  `stats_block_hash128`).
- Regression audit confirming ADR 047 / 048 / 049 / 050
  preimages stay fingerprint-free (Phase 5).
- Architectural guards: each call site uses the
  documented fingerprint; no mixing; no third
  fingerprint domain; no `format_version` bump; no
  descriptor-physical-policy CSB rejection paths.
- Golden vectors for both preimages; cross-language
  stable.
- Bench: descriptor recompute / validate cost; CSB
  merge eligibility cost (with payload-coverage
  variants); aggregate cache lookup cost.
- Operator metrics for fingerprint-mismatch fall-
  throughs, payload-coverage fall-throughs (with reason
  label), pre-ADR-051 cliff observability, bi-
  dimensional mutation counters.

## Non-goals

- No new non-fingerprint descriptor fields (the
  fingerprint slot is split into two newtyped fields).
- No new `RowIdentityPolicy` variant.
- No new digest domain beyond the two listed.
- No new HRFT frame family.
- No new node capability bit.
- No slab footer field.
- No `format_version` / envelope / wire-frame bump.
- No `ObjectStore` trait change.
- No removal or bifurcation of `schema_generation`.
- No `physical_generation` catalog split.
- No per-node physical fingerprint variant.
- No per-column fingerprint.
- No `digest.table.row.physical.v1` allocation.
- No SQL parser work.
- No tolerant decoder for pre-ADR-051 CSB headers.
- No `V1` / `V2` Rust type suffixes; no reserved enum
  slots; no compat shims.
- No rename of `TableSchemaDescriptorV1` →
  `TableSchemaDescriptor` (separate convention-
  compliance pass).
- No broadening of schema-evolution rules.
- No `tenant_id` in either preimage.
- No `contract_version` in either preimage.
- No CSB rejection solely on descriptor physical-policy
  difference (payload coverage governs per-operation
  validity).

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock newtypes, domain tags, preimage byte layouts,
consumer routing, payload-coverage discipline, and the
reject-code disposition before touching call sites.

Files:

- `src/hash/descriptor_fingerprint.rs` (new — newtypes
  + helpers + domain constants)
- `src/schema/units.rs` (newtypes may live here; either
  location works — pick at implementation time)
- `src/schema/table.rs` (descriptor field replacement,
  helper signatures)
- `tests/fixtures/descriptor_fingerprint/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze the newtypes:
   ```rust
   #[repr(transparent)]
   #[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
   #[serde(transparent)]
   pub struct LogicalSchemaFingerprint([u8; 32]);

   #[repr(transparent)]
   #[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
   #[serde(transparent)]
   pub struct PhysicalDescriptorFingerprint([u8; 32]);

   impl LogicalSchemaFingerprint {
       pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self;
       pub fn as_bytes(&self) -> &[u8; 32];
   }
   ```
   `Debug` impls hex-encode for log readability. No
   `Pod` / `Zeroable`.

2. Freeze the domain-tag constants:
   ```rust
   pub const LOGICAL_SCHEMA_DOMAIN: &[u8] =
       b"digest.table.schema.logical.v1";
   pub const PHYSICAL_DESCRIPTOR_DOMAIN: &[u8] =
       b"digest.table.descriptor.physical.v1";
   ```

3. Freeze the preimage helper signatures:
   ```rust
   pub(crate) fn compute_logical_schema_fingerprint(
       table_object_id: &TableObjectId,
       namespace: &str,
       table_name: &str,
       arrow_schema_ipc: &[u8],
       row_identity_policy: &RowIdentityPolicy,
       evolution_policy: &SchemaEvolutionPolicyV1,
   ) -> LogicalSchemaFingerprint;

   pub(crate) fn compute_physical_descriptor_fingerprint(
       logical: LogicalSchemaFingerprint,
       locality_policy: &LocalityPolicyV1,
       record_layout_policy: &RecordLayoutPolicy,
       record_body_compression_policy: &RecordBodyCompressionPolicy,
       stats_policy: &ColumnStatsPolicyV1,
   ) -> PhysicalDescriptorFingerprint;
   ```

   No `tenant_id`, no `contract_version`, no
   `schema_generation` arguments. Helpers begin with the
   domain-tag bytes, append length-prefixed canonical
   postcard bytes per embedded struct (with fixed-width
   LE byte-count prefix for cross-language stability),
   and run BLAKE3.

4. Freeze the descriptor field replacement
   (`src/schema/table.rs:866-881`):
   ```rust
   // Removed:   pub schema_fingerprint: [u8; 32]
   pub logical_schema_fingerprint:    LogicalSchemaFingerprint,
   pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
   ```

5. Freeze the descriptor helper rewrites:
   ```rust
   impl TableSchemaDescriptorV1 {
       pub fn compute_logical_schema_fingerprint(
           &self,
       ) -> LogicalSchemaFingerprint;

       pub fn compute_physical_descriptor_fingerprint(
           &self,
           logical: LogicalSchemaFingerprint,
       ) -> PhysicalDescriptorFingerprint;

       pub fn refresh_descriptor_fingerprints(&mut self) -> std::io::Result<()>;

       pub fn validate(&self) -> Result<(), DescriptorValidationError>;
   }
   ```

6. Freeze the reject-code disposition: rename
   `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` →
   `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`
   (`src/storage/stats.rs:391`). No new reject codes.

7. Freeze the consumer-routing matrix:
   ```text
   logical:    aggregate idempotency cache, aggregate
               wire, CSB header, CSB compaction
               eligibility, columnar directory, future
               CDC cursor, future secondary index
               entries
   physical:   in-memory planning caches (layout,
               compression, stats-builder, scan-range),
               future LSM segment merge eligibility
   generation: write admission, replica mutation gate
   ```

8. Architectural guards (TODO entries; populated by
   later phases — `concat!()`-needle convention from
   ADR 050 §9).

9. Behavior fixtures under
   `tests/fixtures/descriptor_fingerprint/`:
   - **Logical-stable-under-physical-mutation**: bump
     `record_body_compression_policy` from
     `Uncompressed` to `Lz4Frame`; assert
     `logical_schema_fingerprint` unchanged,
     `physical_descriptor_fingerprint` changes.
   - **Physical-changes-on-logical-mutation**: add a
     column (admissible additive evolution); assert
     both fingerprints change. Pins stacking property.
   - **Identification-affects-logical**: two
     descriptors with identical structural fields but
     different `(table_object_id, namespace,
     table_name)`; assert both fingerprints differ.
   - **Generation-bump-without-shape-change**: same
     descriptor fields, `schema_generation` bumped;
     assert both fingerprints unchanged.
   - **Contract-version-relabel-no-change**: bump
     `contract_version` (test-only direct field
     overwrite); assert both fingerprints unchanged.
   - **Lifecycle-outside-both**: per ADR 048,
     `SetTableRetentionPolicy` does not touch the
     descriptor fingerprints.
   - **Cross-language preimage stability**: golden
     32-byte hex outputs for fixed descriptors.
   - **Stacked-implication**: hand-modify the logical
     fingerprint without recomputing physical;
     `validate()` rejects via physical mismatch
     (because physical preimage embeds the stale
     logical).
   - **Stats-policy-retune-payload-coverage**: two
     descriptors with same logical schema but different
     `stats_policy` (e.g., one with bloom-on-Y, one
     without); CSBs from each; merge eligibility
     passes (same logical), per-operation success
     depends on parsed payload (queries that need
     bloom-on-Y succeed only against CSBs that carry
     it).

10. Architecture-test guard fixtures: walk the tree and
    confirm that no remaining call site reads a
    `schema_fingerprint` field name.

Exit:

- Newtypes and helpers compile but are unused.
- Architectural guards in place, mostly TODO.
- Behavior fixtures fail with `unimplemented!()` until
  Phase 1 lands the helpers.

## Phase 1 — Newtype + preimage helper implementation

Files:

- `src/hash/descriptor_fingerprint.rs`
- `src/hash/blake3.rs` (extension; thin wrapper if
  needed)
- `tests/descriptor_fingerprint.rs` (new)

Work:

1. Implement `compute_logical_schema_fingerprint` and
   `compute_physical_descriptor_fingerprint` per the
   signatures frozen in Phase 0 §3. Both helpers begin
   with domain-tag bytes; append length-prefixed
   canonical postcard bytes per embedded struct; run
   BLAKE3; return the newtype via `from_bytes`.

2. Confirm canonical postcard ordering matches what
   `src/schema/table.rs:962` already produces today.
   Reuse, don't reinvent.

3. Newtype serde: `#[serde(transparent)]` so postcard /
   serde-json see each individual newtype as the
   underlying `[u8; 32]`. **Per-newtype** wire
   serialization is byte-for-byte equivalent to today's
   raw `[u8; 32]`. The descriptor as a whole is a pre-
   launch postcard schema break (one 32-byte field
   becomes two — see Phase 2 §10); the per-newtype
   compatibility note here is scoped to the individual
   fingerprint payload, not the descriptor envelope.

4. Cross-language preimage stability: helpers length-
   prefix every variable-length input with `u64` LE
   byte count. The 64-bit width keeps Arrow IPC and
   postcard component sizes unconstrained by the
   fingerprint format. Pin golden vectors in
   `tests/descriptor_fingerprint.rs`.

5. Defensive constructor visibility: `pub(crate) fn
   from_bytes`; external callers compare by `==` and
   read via `as_bytes()`.

Tests:

- Logical-preimage golden equals expected hex for
  fixture A.
- Physical-preimage golden equals expected hex for
  fixture A.
- Mutating any logical-preimage byte changes the
  logical fingerprint.
- Mutating any physical-only-preimage byte changes the
  physical fingerprint but not the logical.
- Stacking property: two descriptors that differ only
  in `arrow_schema_ipc` produce both different logical
  and different physical fingerprints.

Exit:

- Helpers behave per spec; golden vectors pin behavior.

## Phase 2 — Descriptor field replacement

Files:

- `src/schema/table.rs`
- `src/storage/stats.rs` (test helpers — full migration
  in Phase 4)
- `src/storage/multirow.rs` (test helpers)
- `src/storage/content_hash.rs` (test helpers, ADR 049)
- `src/storage/columnar.rs` (full migration in Phase 4)
- `src/storage/aggregate.rs` (full migration in Phase 3)
- `src/schema/aggregate.rs` (full migration in Phase 3)

Work:

1. Remove `schema_fingerprint: [u8; 32]` from
   `TableSchemaDescriptorV1`.

2. Add `logical_schema_fingerprint:
   LogicalSchemaFingerprint` and
   `physical_descriptor_fingerprint:
   PhysicalDescriptorFingerprint`.

3. Replace `compute_schema_fingerprint` with the two
   new helpers and `refresh_descriptor_fingerprints`.

4. Update every constructor call site (every
   `new_with_layout` path, test helper, fixture) to
   call `refresh_descriptor_fingerprints`.

5. Update `validate()` to recompute both fingerprints
   from the non-fingerprint fields and strict-reject on
   mismatch.

6. Migrate the seven test helpers in
   `src/storage/stats.rs:2141, 2184, 2248, 2297, 2355,
   2406, 2505`.

7. Migrate the two test helpers in
   `src/storage/multirow.rs:1044, 1106`.

8. Migrate the helper in
   `src/storage/content_hash.rs:742`.

9. Remove `SchemaFingerprint = [u8; 32]` alias from
   `src/schema/aggregate.rs` and replace with
   `LogicalSchemaFingerprint`.

10. **Postcard schema break.** Confirm that the
    descriptor's on-disk and on-wire postcard shape now
    has *two* 32-byte fields where it had one. Pre-
    launch — accept break.

Tests:

- `descriptor_validation::roundtrip`: descriptor
  serializes, deserializes, validates.
- `descriptor_validation::tampered_logical_fingerprint`:
  hand-modifying the logical fingerprint byte fails
  `validate()`.
- `descriptor_validation::tampered_physical_fingerprint`:
  hand-modifying the physical fingerprint byte fails
  `validate()`.
- `descriptor_validation::stacked_implication`: tamper
  logical without recomputing physical → physical
  mismatch rejects validate (because physical preimage
  embeds the stale logical).
- All existing descriptor tests pass after migration.

Exit:

- The descriptor type carries two newtyped
  fingerprints.
- Old `schema_fingerprint` field name is gone.

## Phase 3 — Aggregate idempotency + wire envelope migration (logical)

Files:

- `src/storage/aggregate.rs`
- `src/schema/aggregate.rs`
- `src/network/data_server.rs`
- `tests/aggregate_idempotency.rs`

Work:

1. Rename `AggregateRequest::schema_fingerprint` to
   `logical_schema_fingerprint:
   LogicalSchemaFingerprint`. Pre-launch wire rename.

2. Idempotency cache key
   (`src/storage/aggregate.rs:1367`): compare
   `request.logical_schema_fingerprint` against
   `response.logical_schema_fingerprint`. Cache key
   composition keeps `(tenant_id, table_object_id,
   schema_generation, logical_schema_fingerprint)` —
   per ADR Open Questions §1, a future ADR may relax
   this to drop generation.

3. Aggregate response composition
   (`src/storage/aggregate.rs:135-136, 276-277`):
   stamp the response with
   `descriptor.logical_schema_fingerprint`.

4. Update client-side aggregate request construction
   (`src/client/core.rs` aggregate paths, if any) to
   read `descriptor.logical_schema_fingerprint`.

5. Update `prepare_aggregate_execution_context_with_migration`
   to compare logical fingerprint.

6. Update `response_crossbind_matches` to compare
   logical fingerprint.

7. **Do not** add physical fingerprint to aggregate
   request or response.

Tests:

- `aggregate_idempotency::cache_miss_after_physical_retune_with_generation_bump`:
  perform an aggregate; mutate the descriptor's
  `record_body_compression_policy` (which advances
  `schema_generation` per ADR §5); replay the same
  aggregate; cache miss. The `logical_schema_fingerprint`
  axis is unchanged — the miss is on the
  `schema_generation` axis. Pins V1 cache-key behavior
  and documents the future-ADR relaxation site (Open
  Questions §1).
- `aggregate_idempotency::cache_miss_after_logical_change`:
  add a column (admissible additive evolution); replay
  the same aggregate; cache miss (logical fingerprint
  changes, generation also bumps; both axes invalidate).
- `aggregate_idempotency::logical_fingerprint_decoupled_from_physical_fingerprint`:
  pin that the cache key reads
  `request.logical_schema_fingerprint` and never
  consults `physical_descriptor_fingerprint`. (Pure
  positive guard against accidental physical-routing.)
- Wire round-trip preserves `logical_schema_fingerprint`
  byte-for-byte.

Exit:

- Aggregate idempotency survives physical retune;
  invalidates on logical change.

## Phase 4 — CSB header + merge eligibility + columnar directory + payload coverage (logical)

Files:

- `src/storage/stats.rs`
- `src/storage/aggregate.rs` (CSB direct-answer
  eligibility call sites)
- `src/storage/columnar.rs`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `benches/multi_row_fragments.rs`
- `tests/csb_merge_eligibility.rs`
- `tests/csb_payload_coverage.rs` (new)
- `tests/columnar_directory.rs`

Work — CSB eligibility:

1. Rename the CSB header's fingerprint field from
   `schema_fingerprint` to
   `logical_schema_fingerprint`. The on-disk byte
   layout is unchanged (32 bytes in the same slot).

2. CSB compaction eligibility check
   (`src/storage/stats.rs:1959-1963`): compare
   `logical_schema_fingerprint` of both CSBs (or
   descriptor + incoming CSB header). Mismatch falls
   through to row-level, same as today.

3. Rename `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` →
   `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`
   (`src/storage/stats.rs:391`).

4. Update `ColumnStatsDescriptorIdentity` to bind
   `logical_schema_fingerprint`. Optional rename to
   `ColumnStatsLogicalIdentity` if local naming reads
   better.

5. Update `build_column_stats_block` to stamp
   `descriptor.logical_schema_fingerprint`.

6. CSB direct-answer eligibility in aggregate
   execution: compare `(table_object_id,
   schema_generation, logical_schema_fingerprint)`.

Work — payload-coverage validation (per ADR §7):

7. Add CSB payload-coverage validation for merge and
   direct-answer paths. After descriptor-level logical
   identity matches, compare the parsed facts the
   operation will consume:

   - column ordinal;
   - Arrow stats type tag;
   - required `stat_flags` (e.g., min-present,
     max-present);
   - optional payload presence for Bloom, HLL,
     small-set, min, max.

   A stats-policy retune must not become a descriptor-
   wide physical mismatch. Older CSBs remain usable for
   always-on facts they carry; operations that require
   absent optional coverage fall through to row-level
   evaluation.

8. Pre-ADR-051 CSB cliff: confirm old slabs (with the
   old monolithic fingerprint in the slot) fall through
   to row-level on the eligibility check; instrument
   with Phase 8 metric.

Work — columnar directory:

9. `RecordBufferDirectory` (line 381 / 1035): rename
   `schema_fingerprint` field to
   `logical_schema_fingerprint`.

10. Validation site (line 2443): compare
    `directory.logical_schema_fingerprint ==
    descriptor.logical_schema_fingerprint`. Keep
    directory validation anchored in the directory's
    recorded write-time IPC schema, storage-column
    ordinals, buffer ranges, null counts, row count,
    and record-body compression — physical decode facts
    are self-describing.

11. Tiered stats cache keys
    (`src/storage/manager.rs`): use logical fingerprint
    plus the existing slab locator fields
    (`stats_format_version`, `stats_block_len`,
    `stats_block_hash128`) so descriptor physical
    retunes do not evict parsed CSBs whose bytes are
    unchanged.

Tests — CSB eligibility:

- `csb_merge_eligibility::logical_match_merges`: two
  CSBs with matching logical fingerprint and adequate
  payload coverage merge.
- `csb_merge_eligibility::logical_mismatch_falls_through`:
  two CSBs with different logical fingerprint skip
  merge, fall through to row-level.
- `csb_merge_eligibility::physical_retune_does_not_block_merge`:
  two CSBs from same logical schema but different
  compression policies merge successfully — physical
  retune does not block CSB merge.
- `csb_merge_eligibility::pre_adr_051_cliff_falls_through`:
  fixture pre-ADR-051 CSB header bytes (synthesized);
  eligibility check rejects, falls through, no panic.
- `csb_merge_eligibility::reject_code_renamed`: assert
  the new code constant exists; old name is gone.

Tests — payload coverage:

- `csb_payload_coverage::stats_policy_retune_with_adequate_coverage`:
  two CSBs from same logical schema; both carry the
  payload an aggregate needs; merge / direct-answer
  succeeds.
- `csb_payload_coverage::stats_policy_retune_with_missing_coverage`:
  one CSB lacks bloom-on-Y; aggregate that needs
  bloom-on-Y falls through to row-level for the
  partial-coverage rows; no panic.
- `csb_payload_coverage::missing_optional_payload_falls_through`:
  direct-answer for a missing optional payload falls
  through without incorrect success.

Tests — columnar:

- `columnar_directory::logical_match_passes`: directory
  built and validated under same logical fingerprint.
- `columnar_directory::logical_mismatch_rejects`:
  directory built under one logical fingerprint,
  validated against another, rejects.
- `columnar_directory::physical_retune_does_not_invalidate`:
  fixture where logical fingerprint is equal but
  physical changes; directory passes validation
  (recorded write-time facts are authoritative for its
  bytes).

Exit:

- CSB header / merge eligibility / columnar directory
  all bind logical fingerprint.
- Payload-coverage validation governs per-operation CSB
  success after descriptor-level eligibility.
- Pre-ADR-051 CSBs fall through cleanly.
- Physical retunes do not block CSB merges or
  invalidate directories when payload coverage is
  adequate.
- Tiered stats cache survives physical retunes.

## Phase 5 — Regression audit (ADR 047 / 048 / 049 / 050 preimage purity)

Files:

- `src/storage/content_hash.rs`
- `src/storage/pk_delete.rs`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `src/network/data_server.rs`
- `tests/state_digest_invariants.rs`
- `tests/descriptor_validation.rs`

Work:

1. **ADR 049 audit.** Confirm content-hash row identity
   preimages still exclude:
   - `schema_generation`
   - `logical_schema_fingerprint`
   - `physical_descriptor_fingerprint`
   - `schema_fingerprint` (unreachable; field removed)

   Add a regression fixture mutating descriptor
   fingerprint bytes (test-only direct field overwrite
   that bypasses validate); assert row identity is
   unchanged.

2. **ADR 050 audit.** Confirm PK delete admission and
   supersession paths do not read either fingerprint
   except through descriptor validation already
   performed by catalog resolution.

3. **ADR 048 audit.** Confirm `SetTableRetentionPolicy`
   mutation does not touch either descriptor
   fingerprint. Lifecycle policy lives in the
   replicated `TableLifecycleCatalog`.

4. **ADR 047 audit.** Confirm tombstone admission paths
   do not read either fingerprint.

5. Confirm `TableWriteRequestMeta`,
   `TableDeleteRowsRequestMeta`, and
   `TableQueryRequestMeta` do not gain new fingerprint
   fields.

Tests:

- `state_digest_invariants::content_hash_unchanged_across_physical_retune`:
  insert a row; retune compression; insert the same
  canonical row content; verify identical
  `__row_key_id__`.
- `state_digest_invariants::content_hash_unchanged_across_descriptor_fingerprint_mutation`:
  mutate the descriptor's fingerprint bytes (test-only
  direct field overwrite that bypasses validate);
  verify content-hash identity unchanged.
- `pk_delete::admission_independent_of_fingerprint`.
- `retention_policy::lifecycle_does_not_change_fingerprints`.

Exit:

- Content-hash and PK delete behavior unchanged across
  the split.
- Lifecycle policy stays orthogonal to fingerprints.

## Phase 6 — Architectural guards

Files:

- `tests/architecture_test.rs`

**Convention.** Forbidden-literal checks construct their
needles via `concat!(...)` so the architecture test
source itself does not contain the forbidden literal —
same convention ADR 049 (`row_currently_visible`) and
ADR 050 (`PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY`)
established.

Positive guards:

- ☐ `LogicalSchemaFingerprint` and
  `PhysicalDescriptorFingerprint` newtypes exist.
- ☐ `compute_logical_schema_fingerprint` and
  `compute_physical_descriptor_fingerprint` exist; the
  physical helper takes the logical fingerprint as its
  first argument (stacking).
- ☐ `LOGICAL_SCHEMA_DOMAIN` and
  `PHYSICAL_DESCRIPTOR_DOMAIN` constants are defined
  and used; no string literal duplication.
- ☐ `logical_schema_fingerprint` exists on descriptor,
  aggregate request / response, CSB header, and
  columnar directory surfaces.
- ☐ `physical_descriptor_fingerprint` exists on
  descriptor.
- ☐ Descriptor `validate()` recomputes both
  fingerprints.
- ☐ Aggregate idempotency cache uses logical
  fingerprint.
- ☐ CSB header serialization stamps logical
  fingerprint.
- ☐ CSB compaction eligibility compares logical
  fingerprint.
- ☐ CSB merge / direct-answer code validates parsed
  stats payload coverage after logical identity match.
- ☐ Columnar directory validation compares logical
  fingerprint.
- ☐ Columnar directory keeps write-time physical
  decode facts self-describing.
- ☐ Tiered stats cache keys include logical fingerprint
  plus slab locator fields.
- ☐ `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`
  exists in `PbcRejectCode`.
- ☐ `RETENTION_REJECTED_PK_DISALLOWED` (from ADR 048)
  remains in force.
- ☐ `schema_generation` remains the catalog sequencing
  field.

Negative guards (forbidden literals constructed via
`concat!()`):

- ☐ No `schema_fingerprint` literal in `src` / `tests` /
  `benches`. Needle: `concat!("schema_", "fingerprint")`.
- ☐ No `SchemaFingerprint` type alias. Needle:
  `concat!("Schema", "Fingerprint")`.
- ☐ No `LogicalSchemaFingerprintV[0-9]` /
  `PhysicalDescriptorFingerprintV[0-9]` /
  `DescriptorFingerprintV[0-9]` suffixed types.
- ☐ No project-name prefix
  (`WendelonLogicalSchemaFingerprint*`, etc.).
- ☐ No `physical_generation` field.
- ☐ No `digest.table.row.physical.v1` literal anywhere
  (future-ADR domain).
- ☐ No `digest.table.descriptor.physical_per_node.v1`
  literal anywhere.
- ☐ No `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` (old
  name).
- ☐ No `PBC_REJECT_STATS_PHYSICAL_FINGERPRINT_MISMATCH`
  for stats / CSB paths (routing is logical).
- ☐ No CSB eligibility guard that rejects solely
  because descriptor compression, layout, locality, or
  stats policy differs when logical fingerprint and
  parsed payload coverage are adequate.
- ☐ No `format_version` bump in CSB header or slab
  footer.
- ☐ No new HRFT frame family for descriptor
  fingerprints.
- ☐ No new node capability bit for descriptor
  fingerprints.
- ☐ No durable `StateDigest` placement on descriptor
  fingerprints (these are plain BLAKE3, not Pedersen).
- ☐ No `#[serde(default)]` on the new fingerprint
  fields.
- ☐ No lifecycle-policy field embedded in
  `TableSchemaDescriptorV1`.
- ☐ No descriptor fingerprint in content-hash row-key
  preimage helpers.
- ☐ No descriptor fingerprint in PK delete admission
  helpers.
- ☐ No `tenant_id` consumed by either preimage helper.
- ☐ No `contract_version` consumed by either preimage
  helper.

Exit:

- The split is mechanically protected against
  regression to the monolithic name, mis-routed
  consumers, identification-omission drift, payload-
  coverage shortcut, or compatibility scaffolding.

## Phase 7 — Bench

Files:

- `benches/descriptor_fingerprint.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/0051_descriptor_fingerprint_split_bench_results.md`
  (new, populated when bench runs)

Bench matrix:

1. **Descriptor fingerprint compute cost.**
   - `compute_logical_schema_fingerprint` alone.
   - `compute_physical_descriptor_fingerprint(logical)`
     alone.
   - `refresh_descriptor_fingerprints` (both,
     sequential).
   - Compare against today's single
     `compute_schema_fingerprint`.

2. **Descriptor `validate()` cost.**
   - Validate a fresh descriptor.
   - Validate after a recompute.

3. **CSB merge eligibility check cost.**
   - Match path (logical fingerprint equality, fast).
   - Mismatch path (falls through, same cost as today).
   - Pre-ADR-051 cliff path (synthesized old bytes;
     rejection cost).

4. **CSB payload-coverage check cost.**
   - Stats-policy retune with adequate parsed payload
     coverage (passes).
   - Stats-policy retune with missing optional payload
     coverage (falls through).

5. **Aggregate cache lookup cost.**
   - Cache hit (logical match).
   - Cache miss (logical mismatch).

6. **Bench shapes:**
   - Single-column PK descriptor.
   - Wide Arrow schema descriptor.
   - Multi-row append-only descriptor.
   - Content-hash descriptor (ADR 049 path).
   - Descriptors with populated stats policies.

7. **Bench partitioning** (mandatory):
   - Isolate logical-compute cost from physical-compute
     cost.
   - Isolate descriptor-recompute from descriptor-
     validate.
   - Isolate eligibility-fingerprint-compare from
     payload-coverage check.

Acceptance threshold: same order of cost as today's
monolithic hashing. Phase 7 calibrates after smoke run;
full 100k / 1M matrix is the ADR 051 promotion-to-
shipped gate (mirroring the cadence ADR 047 / 048 / 049
used).

Bench results documented in
`plans/impl/0051_descriptor_fingerprint_split_bench_results.md`.

## Phase 8 — Metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/audit.rs`
- `tests/metrics_smoke.rs`
- `plans/ops/runbooks/descriptor_fingerprint_split.md`
  (new)

Metrics:

```text
storage_csb_merge_logical_fingerprint_mismatch_total{tenant, table}
storage_csb_payload_coverage_fall_through_total{tenant, table, reason}
storage_csb_merge_pre_adr_051_cliff_total{tenant, table}
storage_aggregate_cache_logical_fingerprint_hit_total{tenant, table}
storage_aggregate_cache_logical_fingerprint_miss_total{tenant, table}
storage_descriptor_recompute_seconds{tenant, table}
storage_descriptor_validate_seconds{tenant, table}
storage_descriptor_logical_only_mutations_total{tenant, table}
storage_descriptor_physical_only_mutations_total{tenant, table}
storage_descriptor_combined_mutations_total{tenant, table}
```

The `payload_coverage_fall_through_total` metric carries
a `reason` label (`missing_bloom`, `missing_hll`,
`missing_smallset`, `missing_min_max`) so an operator
can see which optional payloads are absent across the
fleet and tune `stats_policy` accordingly.

The pre-ADR-051 cliff metric is operational visibility
into the one-time CSB cliff: rises during ramp, decays
to zero as compaction rewrites old slabs. Stuck-nonzero
after a soak window is a runbook escalation.

`storage_descriptor_logical_only_mutations_total` is expected
to stay zero under the stacked physical preimage: any logical
change should also change the physical fingerprint because the
logical fingerprint is part of the physical preimage.

The three mutation counters (logical-only, physical-
only, combined) make the bi-dimensional descriptor
change axis operator-legible — the primary
observability benefit of the split.

Audit:

- One audit event on every typed descriptor mutation —
  composition with ADR 048's `SetTableRetentionPolicy`
  audit pattern.
- The audit event includes both fingerprints pre- and
  post-mutation, so an operator can trace which axis
  changed.

Runbook (`descriptor_fingerprint_split.md`):

- What logical vs physical fingerprint means; how to
  read the audit event delta.
- Pre-ADR-051 cliff: expected one-time fall-through;
  how to interpret the cliff metric; when to consider
  it stuck.
- Aggregate cache survival across physical retune:
  operator-visible benefit; how to verify.
- CSB merge survival across compression / layout
  retune; how payload-coverage governs per-operation
  validity.
- Stats-policy retune workflow: how older CSBs lose
  optional coverage gracefully via fall-through.
- Reject-code rename for operators consuming logs.
- Composition with ADR 048 (cluster-vs-table
  retention), ADR 049 (content-hash identity), ADR 050
  (PK delete) — pointers, no rehash.

Exit:

- Operators can observe descriptor mutation activity
  along both axes, audit fingerprint deltas, observe
  payload-coverage fall-throughs, and follow a runbook.

## Verification gate

Required before declaring ADR 051 shipped:

```sh
cargo test --lib descriptor_fingerprint
cargo test --lib table
cargo test --lib stats
cargo test --lib columnar
cargo test --lib aggregate
cargo test --test architecture_test --quiet
cargo test --test descriptor_fingerprint --quiet
cargo test --test descriptor_validation --quiet
cargo test --test aggregate_idempotency --quiet
cargo test --test csb_merge_eligibility --quiet
cargo test --test csb_payload_coverage --quiet
cargo test --test columnar_directory --quiet
cargo test --test state_digest_invariants --quiet
cargo test pk_delete --quiet
cargo test retention --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench descriptor_fingerprint --no-run
cargo bench --bench descriptor_fingerprint -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Domain-tag well-formedness (extends ADR 046 / 047 /
  049 / 050):
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output equals the existing set plus exactly two new
  entries: `digest.table.schema.logical.v1` and
  `digest.table.descriptor.physical.v1`.

- No future / reserved descriptor digest domains:
  ```sh
  rg -n 'digest\.table\.(row\.physical|descriptor\.physical_per_node)\.v[0-9]' src tests benches
  ```
  Empty.

- Old field name is gone:
  ```sh
  rg -n '\bschema_fingerprint\b' src tests benches
  ```
  Empty (architecture test guard constructs the
  forbidden needle via `concat!()`).

- Old type alias gone:
  ```sh
  rg -n '\bSchemaFingerprint\b' src tests benches
  ```
  Empty.

- New field names present:
  ```sh
  rg -n '\b(logical_schema_fingerprint|physical_descriptor_fingerprint)\b' src tests benches
  ```
  Present at every documented call site.

- Reject-code rename:
  ```sh
  rg -n 'PBC_REJECT_STATS_FINGERPRINT_MISMATCH' src tests
  ```
  Empty (old name gone).
  ```sh
  rg -n 'PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH' src tests
  ```
  Present.
  ```sh
  rg -n 'PBC_REJECT_STATS_PHYSICAL_FINGERPRINT_MISMATCH' src tests
  ```
  Empty (routing is logical).

- No version suffixes on ADR 051 Rust types:
  ```sh
  rg -n '(LogicalSchemaFingerprintV[0-9]|PhysicalDescriptorFingerprintV[0-9]|DescriptorFingerprintV[0-9])' src tests
  ```
  Empty.

- No project-name prefix:
  ```sh
  rg -n '\bWendelon(LogicalSchema|PhysicalDescriptor)Fingerprint' src tests benches
  ```
  Empty.

- Aggregate, CSB, and columnar use logical:
  ```sh
  rg -n 'logical_schema_fingerprint' src/storage/aggregate.rs src/storage/stats.rs src/storage/columnar.rs src/schema/aggregate.rs tests
  ```
  Present at every documented call site.

- Physical fingerprint reserved for planning caches:
  ```sh
  rg -n 'physical_descriptor_fingerprint' src tests benches
  ```
  Present at descriptor field, planning-cache keys,
  bench fixtures. Not in CSB / aggregate / columnar
  hot paths.

- No durable StateDigest on descriptor fingerprints:
  ```sh
  rg -n 'StateDigest\b' src/hash/descriptor_fingerprint.rs src/schema/table.rs
  ```
  Empty.

- Content-hash and PK delete preimage purity:
  ```sh
  rg -n '(logical_schema_fingerprint|physical_descriptor_fingerprint).*row_key_id|row_key_id.*(logical_schema_fingerprint|physical_descriptor_fingerprint)' src tests benches
  ```
  Empty.

- No `physical_generation`:
  ```sh
  rg -n '\bphysical_generation\b' src tests benches
  ```
  Empty.

- No `tenant_id` or `contract_version` in preimage
  helpers:
  ```sh
  rg -n 'compute_logical_schema_fingerprint.*tenant_id|compute_logical_schema_fingerprint.*contract_version|compute_physical_descriptor_fingerprint.*tenant_id|compute_physical_descriptor_fingerprint.*contract_version' src tests benches
  ```
  Empty.

Convention guards:

- No `#[serde(default)]` shim on descriptor fingerprint
  fields.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- CSB header byte layout unchanged (only slot
  interpretation differs).
- Reject-code disposition (Phase 0 §6) settled in code.

## File-by-file expected edits

`src/hash/descriptor_fingerprint.rs` (new)

- Add fingerprint newtypes (or place in
  `src/schema/units.rs`).
- Add domain tags and logical / physical preimage
  helpers.

`src/schema/table.rs`

- Replace descriptor fields.
- Add logical and physical computation helpers.
- Refresh constructors, default descriptor,
  encoding/decoding, tests.
- Update schema evolution fingerprint assertions.
- Keep catalog key shape unchanged.

`src/schema/aggregate.rs`

- Replace `SchemaFingerprint` alias and aggregate
  fields with `LogicalSchemaFingerprint`.

`src/storage/stats.rs`

- Rename header field and descriptor identity.
- Stamp logical fingerprint.
- Keep header byte size stable.
- Add payload-coverage validation for merge / direct-
  answer consumers after logical identity matches.
- Rename stats fingerprint mismatch reject to the
  logical-specific name.

`src/storage/aggregate.rs`

- Compare logical fingerprint in aggregate response
  merge.
- Compare logical fingerprint in CSB direct-answer
  eligibility.
- Add payload-coverage check after eligibility.

`src/storage/columnar.rs`

- Carry logical fingerprint in `RecordBufferDirectory`.
- Keep directory validation anchored in the
  directory's recorded write-time IPC schema, storage-
  column ordinals, buffer ranges, null counts, row
  count, and record-body compression.

`src/storage/manager.rs`

- Update tiered stats cache keys to use logical
  fingerprint plus the existing slab locator fields
  (`stats_format_version`, `stats_block_len`,
  `stats_block_hash128`) so descriptor physical retunes
  do not evict parsed CSBs when the payload itself is
  unchanged.

`src/network/data_server.rs`

- Update aggregate request/response helpers and tests.
- Update descriptor test fixtures and refresh calls.

`src/storage/sweeper.rs`, `src/storage/content_hash.rs`,
`src/storage/pk_delete.rs`

- Update descriptor refresh and fingerprint field
  references.
- Preserve semantics (Phase 5 regression audit).

`benches/multi_row_fragments.rs`

- Update descriptor refresh calls and expected field
  names.

`tests/architecture_test.rs`

- Add ADR 051 positive and negative guards.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- Two newtyped descriptor fingerprints:
  `LogicalSchemaFingerprint` (over `(table_object_id,
  namespace, table_name, arrow_schema_ipc,
  row_identity_policy, evolution_policy)`) and
  `PhysicalDescriptorFingerprint` (binds logical
  fingerprint + locality + layout + compression + stats
  policy).
- Domain-tagged BLAKE3 preimages
  (`digest.table.schema.logical.v1`,
  `digest.table.descriptor.physical.v1`).
- Stacked composition: physical preimage embeds
  logical fingerprint as its first field. LSM safety
  property.
- `schema_generation` orthogonal and unchanged.
- `tenant_id` and `contract_version` excluded from
  both preimages.
- Consumer routing: every existing fingerprint
  comparison site (aggregate idempotency, aggregate
  wire, CSB header, CSB merge eligibility, columnar
  directory) → **logical**; in-memory planning caches
  → **physical**; future LSM segment merge →
  **physical**.
- CSB merge / direct-answer paths validate parsed
  payload coverage (column ordinal, Arrow stats type,
  `stat_flags`, optional payload presence) after
  descriptor-level logical identity match.
- Tiered stats cache keys use logical fingerprint +
  slab locator fields.
- `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` renamed to
  `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`.
- Pre-launch postcard schema break on the descriptor;
  pre-launch CSB header slot reinterpretation cliff.
- No new HRFT frame family; no `format_version` bump;
  no new digest domain beyond the two listed; no new
  non-fingerprint descriptor fields (the fingerprint
  slot is split into two newtyped fields); no
  `physical_generation`.
- Content-hash row identity (ADR 049) and PK delete
  admission (ADR 050) preimages stay fingerprint-free
  (Phase 5 regression audit).
- Operator metrics for cache survival, fingerprint
  mismatch fall-throughs, payload-coverage fall-
  throughs (with reason label), descriptor recompute /
  validate cost, pre-ADR-051 cliff observability,
  bi-dimensional mutation counters.

Out of Wendelon's scope (client-tool ADR territory):
SDK-side fingerprint reproduction (golden vectors are
pinned for cross-language stability).

## Deferred to follow-up ADRs

Mirrors ADR 051 *Deferred* section:

1. Rename `TableSchemaDescriptorV1` →
   `TableSchemaDescriptor` per `no-version-suffixes`.
2. Per-node physical fingerprint variation
   (`digest.table.descriptor.physical_per_node.v1`).
3. Per-column fingerprint or column-projection logical
   fingerprint.
4. `digest.table.row.physical.v1` for row-set physical
   comparison.
5. Logical-generation / physical-generation
   bifurcation if a future consumer demonstrates need.
6. Tolerant decoder for pre-ADR-051 CSB headers
   (rejected here).
7. Locality-policy recluster migration.
8. Per-column physical encoding and compression
   policies.
9. Aggregate cache key relaxation (drop generation;
   key on logical fingerprint alone).
10. Cross-tenant structural-shape fingerprint
    (table-identity-free) if a consumer emerges.
11. Secondary-index descriptor and segment metadata
    (#8).
12. CDC cursor surface (#10).
