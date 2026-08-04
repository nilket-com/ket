# Implementation Plan: ADR 050 — PK-Keyed Row Delete via Supersession

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/0050_pk_delete_supersession.md`

Inherits identity layer:
`plans/adr/037_*` (PK descriptor),
`plans/adr/0045_pk_less_append_only_tables.md`
Inherits substrate: `plans/adr/0046_state_digest_substrate.md`
Inherits CSB metadata: `plans/adr/040_*`,
`plans/adr/042_*` (`supersession_metadata`),
`plans/adr/0047_row_tombstones_append_only.md`
(`tombstone_metadata`)
Inherits retention: `plans/adr/013_*` (cluster MVCC prefix),
`plans/adr/0048_per_table_retention_policy.md` (PK gate retained)
Inherits admission-time visibility discipline:
`plans/adr/0049_content_hash_row_identity.md`
Bench reference: `plans/impl/0046_state_digest_bench_results.md`,
`plans/impl/0047_row_tombstone_bench_results.md`,
`plans/impl/0048_table_retention_bench_results.md`,
`plans/impl/0049_content_hash_bench_results.md`.

## Goal

Add PK-keyed delete admission with supersession discipline. Reuse
ADR 047 row tombstones (`__row_tombstone__`) on the existing PK
version chain. Make admission-time visible-state branching correct
for both delete and insert / upsert under ADR 046 §5 visible-row
digest semantics, with a **policy-aware visibility helper**
(`row_visible_for_admission`): PK boundary versions are still
visible; append-only / content-hash rows respect the
retention-floor predicate. Maintain ADR 042 `supersession_metadata`
and ADR 047 `tombstone_metadata` correctly across all transitions,
including resurrection (tombstone → data). Keep ADR 048's
`RETENTION_REJECTED_PK_DISALLOWED` gate in force — PK retention
stays on ADR 013 cluster-MVCC prefix retention.

The implementation lets PK-keyed delete drive:

- write admission with absorb-vs-supersede branching;
- a symmetric insert / upsert digest discipline (no add when `K`
  is already currently visible);
- composition with ADR 047 tombstones, ADR 048 retention, and
  ADR 049's policy-aware visibility helper;
- read-path supersession through the version chain at
  `latest-as-of-T`;
- CSB metadata maintenance (`tombstone_metadata` /
  `supersession_metadata`) across data, tombstone, and
  resurrection transitions;
- architectural guards and operator metrics.

## Deliverables

- PK-keyed delete admission entry on the data server, reusing the
  existing `TableDeleteRowsRequestMeta` wire surface.
- New helper `admit_pk_delete_batch` building tombstone records
  for every key currently visible.
- PK admission outcome enum + classification-count triple
  (`deleted` / `already_deleted` / `not_found`) on the response.
- **Policy-aware visibility helper** `row_visible_for_admission`
  shared between PK insert / upsert / delete and ADR 049's
  content-hash admission. ADR 049's existing
  `row_currently_visible` call sites are migrated.
- PK insert / upsert path wired to add `element(K)` to the
  visible digest only when `K` is not currently visible.
- Layout-independent tombstone marker packing: `SingleRow` PK
  tables emit multi-row tombstone-only marker frames.
- CSB metadata maintenance for all admissible transitions,
  including resurrection (tombstone → data records
  `supersession_metadata` on the tombstone slab).
- Recovery: PK tombstone-only marker frames replay; metadata
  recomputed from version chains; ambiguous chains fail closed.
- Reject-code disposition: **remove**
  `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` as unreachable.
  Keep `RETENTION_REJECTED_PK_DISALLOWED` enforced.
- Architectural guards: PK admission consults
  `row_visible_for_admission`; supersession introduces no new
  digest domain; tombstone marker shape stays ADR 047; ADR 048
  PK retention gate remains enforced.
- Golden vectors covering all transitions, time-travel reads,
  resurrection, retention boundary-version preservation,
  below-floor boundary delete.
- Bench: delete admission, upsert-no-add, supersession through
  long chains, retention boundary preservation, CSB metadata
  recompute.
- Operator metrics for delete admission outcomes, upsert
  no-digest-add count, retained-boundary-tombstone count,
  visibility-lookup latency.

## Non-goals

- No new `RowIdentityPolicy` variant.
- No predicate / range / truncate / cascade / column-level
  delete.
- No multi-table delete batches.
- No new digest domain.
- No async-only delete or async-only upsert dedup.
- No new tombstone shape or column.
- No new HRFT frame family.
- No new node capability bit.
- No slab footer field.
- No `format_version` / envelope / wire-frame bump.
- No `ObjectStore` trait change.
- No PK + content-hash combined identity work.
- No PK-value-keyed delete wire variant.
- No SQL parser work.
- No V1 / V2 Rust type suffixes; no reserved enum slots; no
  compat shims.
- No append-only-style data+tombstone pair-GC for PK chains.
- No composite-PK expansion.
- No table-specific retention override for PK rows. ADR 048's
  `RETENTION_REJECTED_PK_DISALLOWED` gate remains in force.
- No full-chain key retirement proof. Cross-slab evidence is hard
  before secondary indexes (#8); deferred.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the admission-path contract, the digest-add discipline, the
CSB metadata discipline, the retention rule (PK stays on
cluster-MVCC), the policy-aware visibility helper, and the delete
reject taxonomy before touching admission code.

Files:

- `src/schema/table.rs` (`RowIdentityPolicy::supports_row_tombstones()`
  and reject code disposition)
- `src/storage/pk_delete.rs` (new — admission helper skeleton)
- `tests/fixtures/pk_delete_supersession/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze `RowIdentityPolicy::supports_row_tombstones()` to
   include `PrimaryKey(_)`. Rename the predicate to a
   policy-neutral `supports_delete_tombstones()` if call-site
   readability benefits.

2. Freeze the admission-outcome enum:
   ```rust
   pub enum PkAdmissionOutcome {
       InsertedNewVisible        { added_digest_element: bool },
       UpsertedAlreadyVisible    { added_digest_element: bool },
       DeletedSupersedingVisible { subtracted_digest_element: bool },
       DeleteAbsorbedNotVisible  { latest_was_tombstone: bool },
   }
   ```
   The `latest_was_tombstone` flag distinguishes the
   `already_deleted` vs `not_found` count buckets.

3. Freeze the admission helper signatures:
   ```rust
   pub fn admit_pk_write_row(
       descriptor: &TableSchemaDescriptorV1,
       row: &RowView,
       admission_hlc: HybridTimestamp,
       row_index: &SlabManager,
       effective_retention: &EffectiveRetention,
       digest: &mut StateDigestAccumulator,
   ) -> Result<PkAdmissionOutcome, AdmissionError>;

   pub fn admit_pk_delete_row(
       descriptor: &TableSchemaDescriptorV1,
       key_identity: Identity,
       delete_hlc: HybridTimestamp,
       row_index: &SlabManager,
       effective_retention: &EffectiveRetention,
       digest: &mut StateDigestAccumulator,
   ) -> Result<PkAdmissionOutcome, AdmissionError>;
   ```

   The helpers take `effective_retention` for uniformity with the
   shared `row_visible_for_admission` helper signature; the PK
   dispatch arm of that helper does not consume the retention
   parameter for PK boundary versions.

4. Freeze the **policy-aware visibility helper**:

   ```rust
   pub fn row_visible_for_admission(
       descriptor: &TableSchemaDescriptorV1,
       tenant_id: TenantId,
       key_identity: Identity,
       admission_hlc: HybridTimestamp,
       effective_retention: &EffectiveRetention,
       row_index: &SlabManager,
   ) -> bool;
   ```

   The helper dispatches internally on
   `descriptor.row_identity_policy`:

   - **`AppendOnly` / `AppendOnlyContentHash`**: visible iff
     `slab.lookup(tenant, key)` is not tombstoned AND
     `row_visible_under_retention(loc.hlc, effective_retention)`.
   - **`PrimaryKey`**: visible iff
     `slab.lookup_as_of(tenant, key, admission_hlc)` exists and
     is not a tombstone. Retention-floor predicate is *not*
     applied; chain-prefix retention has already enforced
     visibility.

   ADR 049's existing `row_currently_visible` call sites are
   migrated to `row_visible_for_admission`; the
   `AppendOnly` / `AppendOnlyContentHash` dispatch arm preserves
   the exact predicate ADR 049 ships, including the
   retention-window fix.

5. Freeze the wire response classification triple:
   ```rust
   pub struct PkDeleteClassificationCounts {
       pub deleted_count:          u32,
       pub already_deleted_count:  u32,
       pub not_found_count:        u32,
   }
   ```

6. Freeze the reject-code disposition: **remove**
   `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` as
   unreachable. All current identity policies support tombstones
   after this ADR. No new `PK_DELETE_REJECTED_*` family.

7. Freeze the retention rule: ADR 048's
   `RETENTION_REJECTED_PK_DISALLOWED` validation **remains in
   force**. PK descriptors still reject `FixedWindow` /
   `Disabled` lifecycle policies. Phase 7 honors cluster-MVCC
   prefix retention.

8. Confirm current PK upsert digest behavior. Add a fixture
   under `tests/fixtures/pk_delete_supersession/upsert_double.rs`:
   - Write PK row at HLC 10, capture `digest_after_first`.
   - Upsert same PK at HLC 20 (no value change), capture
     `digest_after_second`.
   - **If `digest_after_first == digest_after_second` fails on
     `main`**, today's PK upsert blindly adds; Phase 3 is the
     rectification site. **If it passes**, the discipline is
     partially in place; Phase 3 formalizes it.

9. CSB metadata fixtures (under
   `tests/fixtures/pk_delete_supersession/csb_metadata/`):
   - data→data: `supersession_metadata` on old data slab.
   - data→tombstone: `tombstone_metadata` on old data slab.
   - tombstone→data (resurrection): **`supersession_metadata` on
     the tombstone slab**.

10. Architectural guards (TODO entries; populated by later
    phases):
    - PK admission must call `row_visible_for_admission`.
    - PK delete must produce only `__row_tombstone__`-bearing
      tombstone-only multi-row records, regardless of data-row
      layout policy.
    - No new `digest.table.row.*.v1` byte tag for supersession.
    - No `PkDeleteV1` / `PkSupersessionV1` suffixed types.
    - No project-name prefix on new symbols.
    - No `format_version` bump.
    - No new HRFT frame family.
    - No `__row_tombstone_target__` or other parallel synthetic
      column.
    - No durable `StateDigest` placement on PK paths.
    - PK descriptors with `FixedWindow` / `Disabled` lifecycle
      policies **still reject** with
      `RETENTION_REJECTED_PK_DISALLOWED`.
    - `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` is gone
      from the taxonomy.

Behavior fixtures under `tests/fixtures/pk_delete_supersession/`:

- Insert-then-delete-then-resurrect: visible digest equals a
  single insert at the end; CSB metadata correctly tracks the
  three transitions.
- Delete-then-delete on same PK: second is absorbed
  (`already_deleted_count`).
- Delete on never-existed PK: absorb (`not_found_count`).
- **Delete on a below-floor boundary data version**: the row is
  still currently visible under prefix retention; delete emits a
  tombstone normally; `deleted_count = 1`. (Regression test for
  the policy-aware visibility helper.)
- PK `SingleRow` data layout: delete writes a tombstone-only
  multi-row marker.
- PK `MultiRow` data layout: delete writes a tombstone-only
  multi-row marker.
- Boundary-version retention: chain (data, tombstone) below the
  cluster floor retains the tombstone as boundary; `latest-as-of`
  through the floor returns deleted.
- Boundary-version retention with resurrection: chain
  (data, tombstone, data) below the cluster floor retains only
  the newest data version as boundary; older versions and the
  tombstone may be pruned.
- Time-travel through tombstone: at `T < tombstone.hlc` row
  visible; at `T ≥ tombstone.hlc` row deleted; at
  `T ≥ resurrection.hlc` row visible again.
- Upsert of already-visible row: digest unchanged; physical
  record written.
- `SetTableRetentionPolicy { FixedWindow | Disabled }` against a
  PK table still rejects with `RETENTION_REJECTED_PK_DISALLOWED`.

Exit:

- Contract pinned; fixtures fail with `unimplemented!()` until
  later phases.
- Architectural guards in place.

## Phase 1 — Descriptor / wire admission, reject-code cleanup

Files:

- `src/schema/table.rs` (`RowIdentityPolicy::supports_row_tombstones()`
  + `PbcRejectCode` cleanup + retention-gate preservation)
- `src/network/data_server.rs` (`table_delete` admission)
- `tests/descriptor_validation.rs`

Work:

1. Update `RowIdentityPolicy::supports_row_tombstones()` to
   return `true` for `PrimaryKey(_)` (or rename per Phase 0 §1).

2. Remove the `table_delete` admission rejection for
   `RowIdentityPolicy::PrimaryKey`. Wire shape unchanged:
   `Vec<Identity>` row key IDs.

3. Remove `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` from
   `PbcRejectCode`. Update any test that expected it to fall
   through to a different path.

4. **Keep** `RETENTION_REJECTED_PK_DISALLOWED` in
   `SetTableRetentionPolicy` validation. PK descriptors continue
   to reject `FixedWindow` / `Disabled` mutations.

5. Preserve leader-direct behavior on the delete path:
   - Follower returns `NOT_LEADER` before target classification.
   - Leader stamps one `delete_hlc` per request.
   - Idempotency cache replay returns the cached response when
     present.

6. Resolve descriptor and (for non-PK admission paths) effective
   retention from one state snapshot.

7. Validate the delete-batch admission against the descriptor:
   - For `PrimaryKey` tables, accept and continue to Phase 2
     admission.
   - For `AppendOnly` and `AppendOnlyContentHash`, the existing
     ADR 047 admission path is unchanged.

Tests:

- `RowIdentityPolicy::supports_row_tombstones()` returns `true`
  for `PrimaryKey(_)`.
- `table_delete` on a PK table is no longer rejected pre-Phase-2.
- `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` is gone (grep
  guard in `tests/architecture_test.rs`).
- `SetTableRetentionPolicy { FixedWindow | Disabled }` against a
  PK table still rejects with `RETENTION_REJECTED_PK_DISALLOWED`.
- Follower returns `NOT_LEADER` without classification counts.
- Append-only / content-hash delete tests still pass.

Exit:

- PK descriptors enter the table-delete admission path; PK
  retention gate stays in force.

## Phase 2 — Policy-aware visibility helper + admission outcome branching

Files:

- `src/storage/pk_delete.rs`
- `src/storage/manager.rs` (helper extension)
- `src/network/data_server.rs` (admission entry)
- `tests/pk_admission.rs`

Work:

1. Implement the policy-aware visibility helper:

   ```rust
   pub fn row_visible_for_admission(
       descriptor: &TableSchemaDescriptorV1,
       tenant_id: TenantId,
       key_identity: Identity,
       admission_hlc: HybridTimestamp,
       effective_retention: &EffectiveRetention,
       row_index: &SlabManager,
   ) -> bool {
       match descriptor.row_identity_policy {
           RowIdentityPolicy::PrimaryKey(_) => {
               // PK: chain-prefix retention has already pruned
               // below-floor versions except the boundary
               // sentinel, which remains the current visible
               // row. Retention-floor predicate is NOT applied.
               row_index
                   .lookup_as_of(tenant_id, key_identity, admission_hlc)
                   .is_some_and(|loc| !loc.is_tombstone)
           }
           RowIdentityPolicy::AppendOnly
           | RowIdentityPolicy::AppendOnlyContentHash => {
               // ADR 049 predicate, including retention-window fix.
               row_index
                   .lookup(tenant_id, key_identity)
                   .is_some_and(|loc| {
                       !loc.is_tombstone
                           && row_visible_under_retention(
                               loc.hlc,
                               effective_retention,
                           )
                   })
           }
       }
   }
   ```

   Migrate ADR 049's existing `row_currently_visible` call sites
   to `row_visible_for_admission`; the
   `AppendOnly` / `AppendOnlyContentHash` dispatch arm preserves
   the exact predicate ADR 049 already ships.

2. Implement `admit_pk_write_row`:
   - Call `descriptor.canonicalize_pk(row) → __row_key_id__`.
   - Call `row_visible_for_admission(...)` with the new write's
     `commit_hlc` as `admission_hlc`.
   - If currently visible → `UpsertedAlreadyVisible {
     added_digest_element: false }`. **Do not add to digest.**
     A new physical version is still packed (Phase 4).
   - If not currently visible → `InsertedNewVisible {
     added_digest_element: true }`. Caller adds `element(K)`.

3. Implement `admit_pk_delete_row`:
   - Use the pre-derived `key_identity`.
   - Call `row_visible_for_admission(...)` with `delete_hlc` as
     `admission_hlc`. The PK dispatch arm internally calls
     `lookup_as_of(tenant, key, delete_hlc)`.
   - If currently visible →
     `DeletedSupersedingVisible { subtracted_digest_element: true }`.
     Caller subtracts `element(K)` and packs a tombstone (Phase
     4).
   - If latest is a tombstone →
     `DeleteAbsorbedNotVisible { latest_was_tombstone: true }`.
   - If no version exists at or before `delete_hlc` →
     `DeleteAbsorbedNotVisible { latest_was_tombstone: false }`.

   The two absorb cases are distinguished by an additional
   `lookup_as_of` probe (or by exposing the "found tombstone vs
   nothing" distinction from the helper's PK dispatch arm).

4. In-batch dedup: identical identities deduplicate to one
   tombstone within a single delete batch.

5. Locality recovery for the tombstone marker: inherited from
   the latest data version's location entry. Fail closed with
   `PBC_REJECT_LOCALITY_UNSUPPORTED` if not recoverable.

6. Wire response: aggregate outcomes into
   `PkDeleteClassificationCounts`.

Tests:

- Delete on visible key: `DeletedSupersedingVisible`; digest
  subtract; `deleted_count++`.
- Delete on already-tombstoned key:
  `DeleteAbsorbedNotVisible { latest_was_tombstone: true }`;
  `already_deleted_count++`.
- Delete on never-existed key:
  `DeleteAbsorbedNotVisible { latest_was_tombstone: false }`;
  `not_found_count++`.
- **Delete on below-floor boundary data version**: visibility
  helper returns `true` for PK; tombstone emitted;
  `deleted_count++`. (Regression test for the PK dispatch arm.)
- Upsert of already-visible row: `UpsertedAlreadyVisible`;
  physical write; digest unchanged.
- Upsert of previously-tombstoned key: `InsertedNewVisible`;
  data row (not tombstone); digest re-added.
- In-batch duplicate identities: count once.
- Missing locality: `PBC_REJECT_LOCALITY_UNSUPPORTED`.
- **Append-only / content-hash dispatch arm unchanged**:
  retention-floor-crossed rows return `false`; the ADR 049
  retention-window fix continues to hold.

Exit:

- Admission helper handles all four outcomes correctly under
  policy-aware visibility; in-batch dedup works; below-floor
  boundary correctness holds.

## Phase 3 — Insert / upsert digest discipline retrofit

Files:

- `src/network/data_server.rs` (PK insert / upsert entry)
- `src/storage/manager.rs`
- `src/storage/pk_delete.rs`
- `tests/state_digest_invariants.rs`

Work:

1. Phase 0 §8's fixture identifies whether digest-add is
   currently blind or visible-state-aware on `main`.

2. **If blind**: wire the insert / upsert path through
   `admit_pk_write_row`; replace the unconditional digest-add
   with the outcome's `added_digest_element` flag. Physical
   packing path unchanged.

3. **If already aware**: verify the existing visibility predicate
   matches `row_visible_for_admission`. Close any gaps.

4. State-digest invariant tests in
   `tests/state_digest_invariants.rs`:
   - "Two upserts of the same PK leave digest equal to a single
     insert."
   - "10 upserts of the same PK leave digest equal to a single
     insert."
   - "Insert + delete + insert at distinct HLCs leaves digest
     equal to a single insert (post-resurrection state)."
   - "Insert + delete leaves digest with zero contribution for
     K."
   - "Upsert over a below-floor boundary data version leaves
     digest unchanged" (no double-count when the prior visible
     version is a boundary sentinel).

5. Soak fixture: 1k PKs with mixed insert / upsert / delete /
   resurrection patterns; final visible digest equals an oracle
   digest computed over the final visible row set.

Tests:

- Pin every invariant explicitly.
- Soak comparison passes.

Exit:

- PK-keyed visible digest is coherent with §"visible row set"
  reading of ADR 046 §5 across all four operations.

## Phase 4 — Layout-independent tombstone marker packing

Files:

- `src/storage/multirow.rs`
- `src/network/data_server.rs`
- `src/storage/manager.rs`
- `src/storage/columnar.rs`

Work:

1. Extract a tombstone marker layout helper:
   ```rust
   pub fn tombstone_marker_layout(
       descriptor: &TableSchemaDescriptorV1,
   ) -> MultiRowLayoutDescriptor;
   ```
   For `MultiRow` data tables, reuse the descriptor layout. For
   `SingleRow` data tables, use a compact internal default within
   the standard multi-row caps.

2. Generalize / rename
   `pack_tombstone_multirow_source_rows_with_policy` so callers
   do not imply append-only semantics.

3. Every marker row stamps:
   - `__row_key_id__` = target PK identity.
   - `__row_routing_locality__` = inherited.
   - `__row_hlc__` = `delete_hlc`.
   - `__row_table_object_id__` = `Some(descriptor.table_object_id)`.
   - `__row_tombstone__` = true.
   - User columns: absent.

4. Update scan / recovery decoders so a PK `SingleRow` table can
   encounter a tombstone-only marker without treating it as a
   malformed user row.

5. Update column-stats / CSB builders to ignore tombstone-only
   marker records as user-column inputs.

Tests:

- PK `SingleRow` table delete writes a tombstone-only marker.
- PK `MultiRow` table delete writes a tombstone-only marker.
- Tombstone marker decode reconstructs `StorageRowMetadata`.
- User-facing scans never expose the marker row.
- Column stats / CSB builders ignore marker-only records.

Exit:

- The storage format for PK tombstones is shared with ADR 047,
  layout-independent.

## Phase 5 — Version-chain CSB metadata + visible digest transitions

Files:

- `src/storage/manager.rs`
- `src/hash/state_digest.rs`
- `src/network/data_server.rs`
- `tests/state_digest_invariants.rs`

Work:

1. Centralize version-chain transition classification at
   admission:
   ```text
   previous: none | data | tombstone
   new:      data | tombstone
   ```

2. Preserve existing metadata behavior:
   - **data → data**: `supersession_metadata` on old data slab.
   - **data → tombstone**: `tombstone_metadata` on old data slab.

3. Add **resurrection** behavior:
   - **tombstone → data**: `supersession_metadata` on the
     tombstone slab.

4. **tombstone → tombstone** is unreachable from admission.
   Recovery may observe it under corrupt chains and must fail
   closed (Phase 7).

5. **none → tombstone** is unreachable from admission.

6. PK visible-domain digest delta helper (in-memory only; no
   durable digest placement):
   ```text
   none/tombstone -> data       Some(+element(K))
   data -> data                 None
   data -> tombstone            Some(-element(K))
   tombstone -> tombstone       None  (unreachable; defensive)
   none -> tombstone            None  (unreachable; defensive)
   ```

7. Audit existing append-only / content-hash digest behavior so
   the helper does not change their semantics.

Tests:

- data → data updates supersession metadata; no digest delta.
- data → tombstone updates tombstone metadata; subtracts one
  element.
- tombstone → data updates supersession metadata on the tombstone
  slab; adds one element.
- repeated tombstone admission: blocked by §4; defensive helper
  return is `None`.
- physical-folding-of-two-PK-data-versions is *not* the visible
  digest model.

Exit:

- PK delete and resurrection maintain the same CSB metadata
  proofs as existing PK updates.
- Digest invariants are chain-aware and admission-time correct.

## Phase 6 — Read paths: point reads, scans, PBC, aggregates, CSB direct-answer

Files:

- `src/storage/manager.rs` (`scan`, `scan_as_of`,
  `scan_all_versions`, `lookup_as_of`)
- `src/storage/aggregate.rs`
- `src/schema/pbc.rs`
- `src/storage/columnar.rs` (ADR 044 fetch)
- `src/network/data_server.rs`

Work:

1. Confirm `latest-as-of-T(tenant, K)` is tombstone-aware:
   - Largest-HLC version with `hlc ≤ T` is tombstone → row
     hidden.
   - Largest-HLC version is data → return it.
   - No version `≤ T` → row absent.

2. Scan paths:
   - `scan` (latest-as-of-now): only PKs whose latest is data.
   - `scan_as_of(T)`: PKs whose latest `≤ T` is data.
   - `scan_all_versions`: includes tombstone records (advanced
     introspection).

3. Point reads: existing path already rejects
   `loc.is_tombstone`; add PK regression coverage for delete and
   resurrection.

4. PBC: predicate evaluation sees only the post-supersession
   visible row set.

5. AGG: `COUNT(*)` and `COUNT(DISTINCT __row_key_id__)` honor
   supersession.

6. CSB direct-answer eligibility (per ADR §7) is unchanged. PK
   delete and resurrection extend the set of slabs that may
   report the metadata domains.

Tests:

- PK point read after delete returns `KEY_NOT_FOUND`.
- PK point read AS OF before delete returns the old data.
- PK point read after resurrection returns the new data.
- Full scan, projected scan, PBC filter, and aggregate agree on
  row visibility.
- CSB direct answer falls through when a PK tombstone applies.
- CSB direct answer falls through when a tombstone slab was
  later superseded by data (resurrection).
- `COUNT(*)` and `COUNT(DISTINCT __row_key_id__)` agree after
  delete.

Exit:

- All user-visible read surfaces agree on PK delete semantics.

## Phase 7 — Retention, compaction, recovery, tiered backfill

Files:

- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `src/consensus/raft_storage.rs`
- `tests/state_digest_invariants.rs`
- `benches/table_retention.rs`

Work:

1. **PK retention follows ADR 013 / ADR 048 V1**: cluster MVCC
   floor only; no table-specific `FixedWindow` / `Disabled`;
   `RETENTION_REJECTED_PK_DISALLOWED` remains enforced.

2. Apply prefix retention to each PK version chain:
   - Versions with `row_hlc >= cluster_floor` survive.
   - Newest version below the floor survives as boundary
     version.
   - Older below-floor versions are eligible for pruning.

3. **Conservative path only**: tombstones participate as normal
   versions. If the boundary is a tombstone, retain it. **No
   full-chain retirement in V1** (deferred per ADR §8).

4. If a later data version exists and becomes the retained
   boundary, older data and tombstone versions below the floor
   may be pruned.

5. Compaction recomputes output metadata from survivors:
   `tombstone_metadata`, `supersession_metadata`,
   `has_row_tombstone_records`, row-HLC bounds, column stats /
   CSBs.

6. **Do not apply append-only-style data+tombstone pair-GC** to
   PK chains.

7. **Recovery** from durability log:
   - PK tombstone-only marker frames replay through the same
     path as ADR 047 tombstones.
   - Version chain reconstructs in HLC order.

8. **Recovery: ambiguous chains fail closed**:
   - Multiple tombstones without intervening data version.
   - Tombstone marker missing `__row_table_object_id__`.
   - Marker row with user columns.

9. **Tiered metadata backfill**: `tombstone_metadata` and
   `supersession_metadata` converge for pre-backfill pointers.

Tests:

- Retention: data/tombstone chain below floor retains tombstone
  boundary.
- Retention: data/tombstone/data chain below floor retains
  newest data boundary; older tombstone and data versions may be
  pruned.
- Retention: data below floor, tombstone above floor retains
  both.
- Retention: PK chain prunes by cluster floor, not table floor.
- Retention: PK + `FixedWindow` / `Disabled` policy mutation
  still rejects under ADR 048.
- Compaction: tombstone-only boundary survives L1 pruning and
  L2 rewrite.
- Compaction: output metadata reflects retained survivors.
- Recovery: restart after PK delete preserves hidden state.
- Recovery: restart after delete + resurrection preserves
  visible new data.
- Recovery: corrupt marker fixture is rejected or conservatively
  ignored with a metric.
- Tiered: PK tombstone pointer blocks CSB direct answer until
  metadata is tracked.
- Append-only pair-GC tests remain unchanged.

Exit:

- PK retention cannot resurrect deleted rows by dropping a
  tombstone too early.
- Recovery and tiered backfill survive PK delete + resurrection.
- ADR 048's two-phase enforcement and PK gate are intact.

## Phase 8 — Bench

Files:

- `benches/pk_delete_supersession.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/0050_pk_delete_supersession_bench_results.md` (new,
  populated when the bench runs)

Bench matrix:

1. **Delete admission cost** (100k and 1M rows):
   - Every key currently visible (`deleted_count` = 100%).
   - 50% absent keys (mixed
     `DeletedSupersedingVisible` / `DeleteAbsorbedNotVisible`).
   - Every key already tombstoned (`already_deleted_count` =
     100%).
   - Below-floor boundary delete mix (regression scenario for
     the policy-aware visibility helper).

2. **Insert / upsert admission cost** (100k and 1M rows):
   - Pure insert (every key new).
   - 50% upsert mix.
   - 100% upsert (no digest churn).
   - Resurrection (tombstone → data).

3. **Latest-as-of classification through long version chains**:
   - Data-only chains (1, 10, 100 versions).
   - data → tombstone chains.
   - data → tombstone → data chains.

4. **Scan / PBC / aggregate filtering**:
   - PK `SingleRow` data + tombstone marker frames.
   - PK `MultiRow` data + tombstone marker frames.

5. **Retention compaction**:
   - Conservative tombstone boundary retained.
   - Resurrection boundary retained.
   - Cluster-MVCC prefix pruning cost.
   - Metadata recompute cost.

6. **Digest transition cost**:
   - data → tombstone subtract.
   - tombstone → data add.
   - data → data no-op.

7. **Bench partitioning** (mandatory):
   - Isolate **lookup cost** from **packing cost** so a slow
     path has a named source.

Acceptance starting points (refine after one quick smoke run):

- Delete admission within 1.5× of append-only `DeleteRows`
  admission at the same row count.
- PK upsert visibility lookup does not regress baseline PK write
  throughput beyond a first-measured threshold.
- Latest-as-of classification stays the same order as ADR 047
  tombstone classification.
- PK `SingleRow` marker filtering adds no user-row decode cost.
- Retention compaction stays linear in version count.
- Digest transition helper cost stays within ADR 046
  single-element add/subtract cost.

Bench results documented in
`plans/impl/0050_pk_delete_supersession_bench_results.md`. Quick
smoke is acceptable for code review; full 100k / 1M matrix is
the ADR 050 promotion-to-shipped gate.

## Phase 9 — Architectural guards, metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/audit.rs`
- `tests/metrics_smoke.rs`
- `tests/architecture_test.rs`
- `plans/ops/runbooks/pk_delete_supersession.md` (new)

Metrics:

```text
storage_pk_delete_requests_total{tenant, table}
storage_pk_delete_rows_deleted_total{tenant, table}
storage_pk_delete_rows_already_deleted_total{tenant, table}
storage_pk_delete_rows_not_found_total{tenant, table}
storage_pk_delete_resurrections_total{tenant, table}
storage_pk_delete_tombstone_boundary_versions_retained_total{tenant, table}
storage_pk_admission_visibility_lookup_seconds{tenant, table}
storage_pk_upsert_no_digest_add_total{tenant, table}
agg_csb_short_circuit_skipped_pk_tombstone_total{tenant, table}
```

Audit:

- One audit event per PK delete request: tenant, table, schema
  generation, parent write id, delete HLC, distinct target
  count, classification counts triple, durability, finalization
  status.
- Resurrection events covered by existing table-write audit plus
  the resurrections metric.
- Per-row delete-supersede outcomes do **not** emit audit events
  (volume).

Runbook (`pk_delete_supersession.md`):

- When PK-keyed delete is appropriate vs alternative shapes.
- Reject taxonomy and operator interpretation.
- Tombstone boundary sentinels and why they may persist (V1
  conservatism).
- Resurrection: mechanism, expected metric signal, time-travel
  semantics.
- Composition with ADR 047 (AppendOnly tombstones) and ADR 048
  (cluster MVCC retention; per-table override remains gated).
- CSB fallback symptoms and metric.
- Identity-based storage API and client PK canonicalization.
- Replay safety guarantees.

Architectural guards (extend `tests/architecture_test.rs`):

**Convention.** Forbidden-literal checks must construct their needles
via `concat!(...)` (or equivalent split / format) so the architecture
test source itself does not contain the forbidden literal. This
matches the ADR 049 / ADR 047 pattern at
`tests/architecture_test.rs:1124-1133` (e.g.,
`concat!("AppendOnlyContentHash", "V1")`). The verification-gate
grep guards rely on this convention — they expect
`row_currently_visible`, `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY`,
and `PkDeleteV[0-9]`-style literals to be absent from source as
single tokens, including from the architecture test itself.

Guards to add:

- ☐ `admit_pk_write_row` and `admit_pk_delete_row` exist and are
  exercised by tests.
- ☐ Both `admit_pk_*` helpers consult
  `row_visible_for_admission`.
- ☐ The visibility helper is **policy-aware**: the PK dispatch
  arm does not apply the retention-floor predicate to boundary
  versions; the append-only / content-hash arm preserves
  ADR 049's retention-window predicate exactly.
- ☐ PK insert / upsert path does not unconditionally add the
  digest element; the add is gated on `added_digest_element`.
- ☐ PK delete records are tombstone-only multi-row records
  regardless of data-row layout.
- ☐ No `__row_tombstone_target__` or other parallel synthetic
  column. Construct the forbidden needle via
  `concat!("__row_tombstone_", "target__")` etc.
- ☐ No new `digest.table.row.*.v1` byte tag for supersession or
  PK-delete. Construct forbidden domain tags via `concat!()`.
- ☐ No `PkDeleteV1` / `PkSupersessionV1` suffixed types.
  Construct via e.g. `concat!("PkDelete", "V1")`.
- ☐ No project-name prefix on new symbols (`WendelonPkDelete*`).
  Construct via `concat!()`.
- ☐ No `format_version` bump.
- ☐ No new HRFT frame family.
- ☐ No durable `StateDigest` placement on PK paths.
- ☐ ADR 049's old helper name is **migrated, not duplicated**:
  the literal `row_currently_visible` (constructed via
  `concat!("row_currently", "_visible")`) appears nowhere in
  `src` / `tests` / `benches`.
- ☐ `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` is gone.
  Construct the forbidden needle via
  `concat!("PBC_REJECT_ROW_TOMBSTONES_", "REQUIRE_APPEND_ONLY")`.
- ☐ `RETENTION_REJECTED_PK_DISALLOWED` is **still enforced** for
  PK descriptors. (This one is a *positive* guard — the literal
  *should* exist; written as a normal string literal in the
  architecture test.)
- ☐ No new `PK_DELETE_REJECTED_*` reject family. Construct the
  forbidden needle via `concat!("PK_DELETE_", "REJECTED_")`.

Exit:

- Operators can observe PK-keyed delete activity, audit
  classification counts, follow a runbook.

## Verification gate

Required before declaring ADR 050 shipped:

```sh
cargo test --lib pk_delete
cargo test --lib table
cargo test pk_delete --quiet
cargo test pk_admission --quiet
cargo test row_tombstone --quiet
cargo test retention --quiet
cargo test storage::manager --quiet
cargo test storage::sweeper --quiet
cargo test --test architecture_test --quiet
cargo test --test state_digest_invariants --quiet
cargo test --test descriptor_validation --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench pk_delete_supersession --no-run
cargo bench --bench pk_delete_supersession -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Domain-tag well-formedness (extends ADR 046 / 047 / 049):
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output equals the existing ADR 046 / 047 / 049 set; no new
  domains added by this ADR.

- No new delete-specific or supersession-specific digest domains:
  ```sh
  rg -n 'digest\..*(pk|delete|supersession).*\.v[0-9]' src tests benches
  ```
  Empty.

- No second target column:
  ```sh
  rg -n '__row_tombstone_target__|__row_delete_target__' src tests benches
  ```
  Empty.

- Synchronous-branching admission path:
  ```sh
  rg -n 'admit_pk_write_row|admit_pk_delete_row|PkAdmissionOutcome' src tests
  ```
  Present and exercised.

- PK admission uses the policy-aware visibility helper:
  ```sh
  rg -n 'row_visible_for_admission' src/storage/pk_delete.rs src/network/data_server.rs tests
  ```
  Present and exercised.

- ADR 049's old helper name is migrated, not duplicated:
  ```sh
  rg -n 'row_currently_visible' src tests benches
  ```
  Empty (call sites migrated to `row_visible_for_admission`).
  Per the Phase 9 convention, the architecture test guard for
  this rule constructs the forbidden needle via
  `concat!("row_currently", "_visible")`, so the architecture
  test source itself does not contain the literal and does not
  self-match this grep.

- `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` is removed:
  ```sh
  rg -n 'PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY' src tests
  ```
  Empty. Same convention: the architecture test guard
  constructs the forbidden needle via
  `concat!("PBC_REJECT_ROW_TOMBSTONES_", "REQUIRE_APPEND_ONLY")`,
  so the test source does not contain the literal.

- `RETENTION_REJECTED_PK_DISALLOWED` remains enforced:
  ```sh
  rg -n 'RETENTION_REJECTED_PK_DISALLOWED' src tests
  ```
  Present in ADR 048 validation path; not removed.

- No version suffixes on ADR 050 Rust types:
  ```sh
  rg -n 'PkDeleteV[0-9]|PkSupersessionV[0-9]|PkAdmissionOutcomeV[0-9]' src tests
  ```
  Empty.

- No project-name prefix on new PK delete symbols:
  ```sh
  rg -n '\bWendelonPkDelete[A-Z]|\bwendelon_pk_delete[a-z_]' src tests benches
  ```
  Empty.

- No durable `StateDigest` placement on PK delete paths:
  ```sh
  rg -n 'StateDigest\b' src/storage/pk_delete.rs src/network/data_server.rs \
    | grep -v 'StateDigestAccumulator\|StateDigestElement\|test\|bench'
  ```
  Empty (helper signatures may reference accumulator / element
  types; no durable-state field carries a digest).

- No new table-delete frame family:
  ```sh
  rg -n 'TABLE_.*DELETE|DeleteRows' src/schema src/network
  ```
  Existing `TABLE_DELETE_REQ` / `TABLE_DELETE_RESP` and
  `TableDeleteRows*` symbols only.

Convention guards:

- No `#[serde(default)]` shim on PK delete fields.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change.
- Reject-code disposition (Phase 0 §6) and retention-gate
  preservation (Phase 0 §7) are settled in code.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- PK-keyed delete admission path on `RowIdentityPolicy::PrimaryKey`
  tables; reuses existing `TableDeleteRows` wire frame.
- Tombstone reuses ADR 047 `__row_tombstone__` synthetic column
  and multi-row tombstone-only record packing,
  layout-independent.
- Supersession via HLC-ordered version chain on the existing PK
  index; no new index structure.
- Synchronous admission-time visible-state branching for both
  delete and insert / upsert.
- **Policy-aware visibility helper** `row_visible_for_admission`
  dispatches internally by identity policy. PK boundary versions
  remain visible at admission; append-only / content-hash
  retention semantics from ADR 049 are preserved exactly.
- Visible-domain `StateDigest` element adds on insert-of-not-
  visible and subtracts on delete-of-visible; otherwise no-op.
- CSB metadata composition: `supersession_metadata` and
  `tombstone_metadata` maintained for all admissible
  transitions, including resurrection (tombstone → data records
  supersession on the tombstone slab).
- PK retention stays on ADR 013 cluster-MVCC prefix retention
  with conservative boundary-tombstone preservation. ADR 048's
  `RETENTION_REJECTED_PK_DISALLOWED` gate remains in force. No
  full-chain key retirement in V1.
- No new digest domain; reserved-tag mechanism intact.
- Wire surface stays identity-based; PK-value ergonomics out of
  scope.
- `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` removed as
  unreachable; no new `PK_DELETE_REJECTED_*` family.
- Operator metrics for classification counts, resurrections,
  retained boundary tombstones, upsert no-digest-add count,
  visibility-lookup latency.

Out of Wendelon's scope (client-tool ADR territory): DDL / DML
syntax for `DELETE FROM table WHERE pk = …`.

## Deferred to follow-up ADRs

Mirrors ADR 050 *Deferred* section:

1. Predicate-driven delete (`DELETE WHERE …`).
2. Multi-table or cross-tenant delete batches.
3. Per-column delete (column-level removal on a still-visible
   row).
4. PK + content-hash combined identity with supersession.
5. PK-value-keyed delete wire variant.
6. Per-table retention for PK tables (lift
   `RETENTION_REJECTED_PK_DISALLOWED`).
7. Full-chain key retirement proof for boundary-tombstone
   removal.
8. Anti-entropy reconciliation over PK-keyed visible digests.
9. Time-travel-aware secondary index reclaim under supersession.
10. CDC cursor surface for resurrection events.
