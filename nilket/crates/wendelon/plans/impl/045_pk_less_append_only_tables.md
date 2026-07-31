# Implementation Plan: ADR 045 — PK-less Append-Only Tables

Companion ADR: `plans/adr/045_pk_less_append_only_tables.md`

## Goal

Add append-only tenant tables whose rows have no user-declared
primary key, no dedupe behavior, and no update / delete-by-key
semantics. Row identity is synthesized from authenticated write
context — `(writer_node_id, row_hlc, intra_batch_index)` where
`row_hlc` is the leader-merged commit HLC — and carried through
stored multi-row synthetic columns. No replicated counter. No
stored writer / intra-batch columns. Phase 8.5 adds
`__row_table_object_id__` as a descriptor-isolation stamp on
multi-row records. Multi-row layout, compression, CSB, AGG,
and ADR 044 columnar fetch continue to work unchanged.

## Non-goals

- No row-level `DELETE` / `__row_tombstone__` synthetic column.
- No content-hash identity mode.
- No idempotent-ingest mode.
- No replicated table-local append counter.
- No stored write-context synthetic columns
  (`writer_node_id`, `intra_batch_index`).
- No public point-read API by HLC or identity.
- No new PBC opcode.
- No new HRFT write-frame family.
- No new node capability bit.
- No new locality policy variant.
- No new `RecordLayoutPolicy` variant.
- No ADR 044 planner changes.
- No `ObjectStore` trait change.
- No envelope `format_version` bump.
- No descriptor compatibility shim.
- No project-name-prefixed names.
- No `V1` / `V2` suffixes.
- No reserved-for-V2 enum slots.

## Phase 0 — Contract freeze and golden fixtures

Lock the descriptor surface, the identity preimage, the Toron
wire-frame extension, and the metric catalog.

Outputs:

- `RowIdentityPolicy` enum:
  ```rust
  pub enum RowIdentityPolicy {
      PrimaryKey(PrimaryKeyDescriptor),
      AppendOnly,
  }
  ```
- New newtypes in `src/schema/units.rs`:
  ```rust
  pub struct WriterNodeId([u8; 16]);   // Pod + Zeroable
  pub struct IntraBatchIndex(u32);     // Pod + Zeroable
  ```
  `IntraBatchIndex` constructor accepts `0..=u32::MAX - 1`;
  `u32::MAX` is reserved for overflow detection in the
  allocator path.
- Identity preimage contract (locked):
  ```text
  key_identity = BLAKE3(
      "table.append-row"
      || tenant_id              // 16 B
      || table_object_id        // 16 B
      || writer_node_id         // 16 B
      || row_hlc                // 16 B  (= leader-merged commit_hlc)
      || intra_batch_index_be   // u32 big-endian, 4 B
  )
  ```
  Phase 7 resolved Open question 1 (drop the canonical row
  payload hash). The BLAKE3 input layout is locked.
  `row_hlc` is the leader-merged commit HLC and equals the
  value persisted in `__row_hlc__`.
- Helper-shape contract:
  ```rust
  pub fn synthesize_append_only_row_id(
      tenant_id: TenantId,
      table_object_id: TableObjectId,
      writer_node_id: WriterNodeId,
      row_hlc: HybridTimestamp,
      intra_batch_index: IntraBatchIndex,
  ) -> Identity;
  ```
- Toron wire-frame extension: `CreateTable` gains
  `row_identity_policy: RowIdentityPolicy`. No new frame.

Golden fixtures under `tests/fixtures/append_only_v1/`:

- A 50-column `MultiRow + Lz4Frame` append-only event table.
  Cold-tier scan via ADR 044 column-fetch returns byte-
  identical results to whole-record fetch.
- Two identical rows appended in one batch — distinct
  `__row_key_id__` (different `intra_batch_index`).
- Two identical rows appended in separate batches with
  different HLCs — distinct identities.
- Two writers admitting identical payloads with the same
  HLC and `intra_batch_index = 0` — distinct identities
  (different `writer_node_id`). Documents the
  cross-writer disambiguation.
- A `TableSpreadProjection(spread_factor=4)` append-only
  table — spread input is the retry-stable request identity
  (`parent_write_id`), not synthesized `key_identity`.
- Negative fixtures proving `AppendOnly + ColumnProjection`
  and `AppendOnly + IdentityProjection` reject in V1.
- A descriptor with omitted locality — confirms the
  `TableSpreadProjection { spread_factor: 4,
  prefix_bits: 16 }` default is resolved before
  fingerprinting and produces byte-identical output to
  the explicit-spread fixture.
- Recovery fixture proving no append-counter state is
  required: a sealed slab corpus reconstructs the redb
  index from stored `__row_key_id__` / `__row_hlc__` /
  `__row_routing_locality__`; descriptor matching uses
  stored `__row_table_object_id__`.
- CSB fixture: a sealed append-only slab is direct-answer
  eligible; `supersession_metadata = tracked None`.
- Mixed-shape fixture: PK-keyed table + append-only table
  in the same recovery corpus, both rebuild correctly.
- Negative fixtures:
  - `AppendOnly + SingleRow` rejects.
  - `AppendOnly + PRIMARY KEY` rejects.
  - User column named `__row_*` rejects.
  - All reject with documented reject codes.

Exit:

- All new types and helper signatures merged behind stub
  `unimplemented!()` bodies.
- Fixtures land.

## Phase 1 — Descriptor and validation

Files:

- `src/network/control_server.rs`
- `src/schema/table.rs` (`TableSchemaDescriptor`)
- `src/schema/units.rs`
- `src/schema/mod.rs`
- table descriptor tests
- `tests/architecture_test.rs`

Work:

1. Add `WriterNodeId` and `IntraBatchIndex` newtypes per
   `newtype-units.md`. `Pod + Zeroable`.
   `IntraBatchIndex::try_new(u32)` rejects `u32::MAX`.
2. Replace `primary_key: PrimaryKeyDescriptor` with
   `row_identity_policy: RowIdentityPolicy`. Fold the
   existing `PrimaryKeyDescriptor` into the
   `RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor)`
   variant.
3. **Do not** add `Option<PrimaryKeyDescriptor>` coexistence
   field. Per `no-premature-backward-compat.md`, just
   change the type.
4. Update `SchemaFingerprint` derivation to include
   `row_identity_policy`. Existing tables re-fingerprint on
   schema-load.
5. Descriptor validators for
   `RowIdentityPolicy::AppendOnly`:
   - **Reject** `AppendOnly +
     RecordLayoutPolicy::SingleRow`.
   - **Reject** Arrow schema declaring user columns named
     `__row_hlc__`, `__row_key_id__`,
     `__row_routing_locality__`, or
     `__row_table_object_id__` (defense-in-depth).
   - **Accept** `TableSpreadProjection`.
   - **Reject** `IdentityProjection` and
     `ColumnProjection` for AppendOnly V1 because neither
     can produce retry-stable routing under HLC-derived
     identity.
   - **Default** an omitted `LocalityPolicy` to
     `TableSpreadProjection { spread_factor: 4,
     prefix_bits: 16 }` as a *create-time resolution
     rule*: the create-table path writes the resolved
     value into the persisted descriptor before
     fingerprinting. Serialized descriptors carry an
     explicit locality on the wire and at rest. Schema-
     load decode never silently fills in defaults.
   - **Accept** `MultiRow` layout policies of any
     `target_rows_per_record`.
6. Default compression follows ADR 043's layout-driven
   rule: `MultiRow` append-only tables default to
   `Lz4Frame`.
7. Extend the existing create-table request/handler with
   `row_identity_policy: RowIdentityPolicy`. Do not add a
   new HRFT frame family.
8. Toron-side: `PRIMARY KEY` + `APPEND ONLY` is a parser
   error (Toron's responsibility); storage-side validator
   defends in depth.
9. **No `TableAppendState`, no replicated counter
   initialization.** Append-only tables carry no mutable
   table state.

Tests:

- Existing PK descriptor tests pass through
  `RowIdentityPolicy::PrimaryKey`.
- `RowIdentityPolicy::AppendOnly` admitted with
  `TableSpreadProjection` and `MultiRow` layout.
- `AppendOnly + IdentityProjection` and `AppendOnly +
  ColumnProjection` rejected.
- `AppendOnly + SingleRow` rejected.
- `AppendOnly` with user `__row_*` column rejected.
- `AppendOnly` without explicit locality picks
  `TableSpreadProjection { spread_factor: 4,
  prefix_bits: 16 }` default; the resolved locality is
  written into the persisted descriptor.
- Descriptor encode / decode round-trips
  `RowIdentityPolicy::AppendOnly`.
- `SchemaFingerprint` differs between an `AppendOnly`
  descriptor and an otherwise-identical `PrimaryKey`
  descriptor.
- `PRIMARY KEY` + append-only is rejected at the
  storage-side validator.
- Architectural test: no `Option<PrimaryKeyDescriptor>`
  coexistence shim; no `#[serde(default)]` on
  `row_identity_policy`; no brand prefix; no `V1` / `V2`
  suffix on ADR 045 surfaces; no `TableAppendState` or
  `AppendOrdinal` type introduced.

Exit:

- Schema state can represent append-only tables. Writes
  still reject them until Phase 3.

## Phase 2 — Write-context identity helpers

Files:

- `src/schema/table.rs`
- `src/storage/manager.rs`
- `src/storage/index.rs`
- `src/network/data_server.rs`
- `src/hash/content.rs` (canonical row hashing — create
  if absent)

Work:

1. Implement identity synthesis per the ADR §4 preimage:
   ```rust
   pub fn synthesize_append_only_row_id(
       tenant_id: TenantId,
       table_object_id: TableObjectId,
       writer_node_id: WriterNodeId,
       row_hlc: HybridTimestamp,
       intra_batch_index: IntraBatchIndex,
   ) -> Identity;
   ```
   Output: 32-byte identity matching ADR 042's
   `__row_key_id__` slot.
2. Bind `WriterNodeId` from the authenticated mTLS node
   identity (ADR 018) of the request context. Reuse
   existing node-identity material; do not add a new
   negotiated capability or wire frame.
3. Assign zero-indexed `IntraBatchIndex` values from the
   row's position in the incoming Arrow batch. A row that
   would require `IntraBatchIndex(u32::MAX)` rejects the
   write before any slab append. Serialize the value as
   big-endian `u32` in the identity preimage.
4. The `row_hlc` passed into identity synthesis MUST be
   the leader-merged commit HLC — the same value the
   admission path persists in `__row_hlc__`. The leader
   computes `commit_hlc = hlc_driver.merge(client_hlc)`
   once per request, before admission, and forwards the
   single value through admission, packing, and storage.
   No per-row payload hashing.
5. Add a small internal audit field with row count and
   writer node identity. Do not log every generated row
   identity.
6. Compaction must never call
   `synthesize_append_only_row_id` for an existing row.
   Architectural test asserts.

Tests:

- Golden vector: fixed `(tenant, table, writer, hlc,
  intra_batch_index)` produces a known identity (locks
  the BLAKE3 preimage layout, including the big-endian
  `intra_batch_index` encoding).
- Two writers with identical payload, HLC, and intra-
  batch index produce distinct identities (different
  `writer_node_id`).
- One writer with identical payload and HLC but different
  intra-batch indexes produces distinct identities.
- Two identical payloads in separate batches produce
  distinct identities when HLC differs.
- Identical client_hlc + identical writer + same row idx
  but distinct *commit_hlc* produce distinct identities
  (HLC-split correctness regression guard, Phase 7).
- A batch requiring `IntraBatchIndex(u32::MAX)` rejects
  with the documented reject code; no partial-batch
  admission.
- Domain-tag separation: a constructed PK can never
  BLAKE3-collide with an append-only `key_identity` for
  the same `(tenant_id, table_object_id)` because the
  preimage domain tags differ.
- Architectural test: compactor source does not call
  `synthesize_append_only_row_id`.
- Architectural test: nothing in the source tree
  references `canonical_row_payload_hash` (deleted in
  Phase 7).

Exit:

- The system can synthesize append-only identities without
  any mutable per-table counter state.

## Phase 3 — Append-only write admission

Files:

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- `src/storage/multirow.rs`
- `src/hash/content.rs`
- `src/schema/table.rs`

Work:

1. Branch table-write admission on `RowIdentityPolicy`.
2. For `PrimaryKey(...)`, keep ADR 037 behavior unchanged:
   PK column presence, null rejection, canonicalization,
   duplicate-in-batch rejection.
3. For `AppendOnly`:
   - Validate Arrow batch against the descriptor's user
     schema.
   - Skip PK validation and duplicate-in-batch checks.
   - Bind `WriterNodeId`, receive the leader-merged
     `commit_hlc` from the caller (single HLC per
     request; ADR 012 monotonicity), assign
     `IntraBatchIndex` per row.
   - Derive `key_identity` per the ADR §4 preimage.
     **No payload hashing.**
   - Compute request-level locality from canonical
     `parent_write_id` under `TableSpreadProjection`.
   - Persist `__row_hlc__ = commit_hlc` — the same value
     used in identity synthesis.
4. Reject update / upsert / delete-by-key operations
   against `AppendOnly` descriptors with the existing
   schema / write reject machinery, or a narrowly-named
   reject code if the current catalog lacks one.
5. Identical payloads remain distinct rows — distinct
   identities by write context.
6. Audit metadata: `{batch_id, row_count, writer_id}` —
   not per-row identity.

Tests:

- Two identical rows in one batch produce distinct
  `key_identity` values.
- Two identical rows in separate batches produce distinct
  identities.
- `AppendOnly` write with no PK column succeeds.
- `AppendOnly` write containing user-declared ADR 042
  synthetic column names rejects at descriptor /
  write validation.
- PK-table duplicate-in-batch rejection still works.
- `AppendOnly` update / upsert / delete-by-key
  rejected.
- Locality calculation is retry-stable for
  `TableSpreadProjection`; `IdentityProjection` and
  `ColumnProjection` reject for append-only tables.
- `IntraBatchIndex` overflow rejects atomically.

Exit:

- Append-only rows are admitted and assigned identity, but
  storage packing may still be temporary until Phase 4.

## Phase 4 — Multi-row storage, recovery, and compaction

Files:

- `src/storage/multirow.rs`
- `src/storage/manager.rs`
- `src/storage/index.rs`
- `src/storage/stats.rs`
- `src/storage/sweeper.rs`
- `src/storage/ipc.rs`
- recovery tests

Work:

1. Pack append-only rows only into `MultiRow` records (hard
   requirement from ADR §2).
2. Sort append-only rows by the ADR 042 deterministic
   order, unchanged:
   ```text
   (__row_routing_locality__, __row_key_id__, __row_hlc__, original_ordinal)
   ```
   The `__row_key_id__` tie-breaker is unique with
   overwhelming probability.
3. Preserve ADR 042 bucket-pure packing.
4. Write `__row_hlc__`, `__row_key_id__`,
   `__row_routing_locality__`, and
   `__row_table_object_id__` for every multi-row row.
5. Strip all storage-only synthetic columns before
   user-visible decode (existing path; no public leakage).
6. Recovery reads `__row_key_id__`, `__row_hlc__`, and
   `__row_routing_locality__` directly from the IPC body
   to rebuild redb indexes; descriptor matching reads
   `__row_table_object_id__`. **No append-counter state
   to reconcile.**
7. Compaction preserves `__row_key_id__` and `__row_hlc__`
   exactly per row. Architectural test asserts the
   compactor never calls `synthesize_append_only_row_id`.
8. Stats and CSB writers strip storage-only synthetic
   columns before user-column folding (existing path).
9. CSB writers populate `supersession_metadata = tracked
   None` for sealed append-only slabs.

Tests:

- Encoded append-only IPC contains the ADR 042 synthetic
  storage columns plus `__row_table_object_id__` in the
  documented order.
- User-facing reads strip all synthetic columns.
- Recovery rebuilds the key / locality index without
  consulting any append-counter state — architectural
  test inspects the recovery code path for absence of
  `TableAppendState` / `AppendOrdinal` references.
- Compaction preserves key identities byte-for-byte.
- AppendOnly packing is bucket-pure with one locality per
  request; size limits may still split the request into
  multiple records in that bucket.
- Stats ignore storage-only columns.
- Mixed PK-keyed + append-only corpora recover together.
- Architectural test: compactor source does not call
  `synthesize_append_only_row_id`.

Exit:

- Append-only tables physically stored and recovered
  through the ADR 042 multi-row substrate without
  introducing any new state surface.

## Phase 5 — Read path, PBC, AGG, CSB visibility

Files:

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- `src/storage/aggregate.rs`
- `src/storage/columnar.rs`
- `src/storage/stats.rs`
- `src/schema/pbc.rs`

Work:

1. Read path branches on `RowIdentityPolicy`:
   - `PrimaryKey(...)` → existing path with supersession
     visibility (unchanged).
   - `AppendOnly` → simplified path: every row is
     visible by construction; the MVCC version-hiding loop
     is skipped.
2. CSB short-circuit logic for `AppendOnly` slabs:
   - `supersession_metadata` is unconditionally
     `tracked None`. The "fully superseded by later
     slabs" check returns `false` without inspection.
   - `tombstone_metadata` (ADR 040, slab-level
     visibility) remains applicable to table / range /
     slab lifecycle operations.
3. PBC sees only descriptor user columns. No new
   stripping rule.
4. AGG accumulator input analysis ignores storage-only
   identity columns. `count_star` / `count(*)` over
   append-only is exact (no supersession to subtract, no
   tombstones in V1).
5. ADR 044 `ColumnFetchRequirement` is unchanged.
   Synthetic columns enter the requirement only when
   storage-internal paths require them.
6. `as_of_hlc` filtering continues to use per-row
   `__row_hlc__`.
7. Reclaim loop: PK-keyed slabs continue through
   supersession-driven reclaim. Append-only slabs reclaim
   only on slab lifecycle events. No cross-contamination.

Tests:

- PBC filter / project over append-only matches whole-
  record decode baseline.
- AGG `count_star`, `min(event_time)`, and
  `max(event_time)` work over append-only tables.
- CSB direct-answer fires on a sealed append-only slab
  with no supersession check.
- `as_of_hlc` excludes rows appended after the query
  timestamp, validated against `__row_hlc__`.
- ADR 044 column-fetch over append-only `MultiRow +
  Lz4Frame` returns byte-identical results to whole-
  record fetch.
- A 5-of-50-column projection on append-only fetches
  buffer-identical bytes to the PK-keyed equivalent.
- Ordinary user projection never surfaces synthetic
  columns.
- Reclaim correctness on a mixed PK + append-only corpus.

Exit:

- Append-only tables queryable through existing scan /
  PBC / AGG / CSB paths. Supersession-axis collapse
  observable in metrics.

## Phase 6 — Observability and architectural guards

Files:

- `src/metrics/registry.rs`
- `tests/metrics_smoke.rs`
- `tests/architecture_test.rs`
- write reject-code definitions / tests

New metrics (functional segment names; brand-clean per
ADR 041 / convention):

```text
storage_append_only_batches_total{tenant}                   // server success path only (Phase 8)
storage_append_only_rows_written_total{tenant}              // server success path only (Phase 8)
storage_append_only_multirow_records_written_total{tenant}
storage_append_only_intra_batch_index_exhaustion_total
storage_append_only_supersession_metadata_skipped_total     // expected nonzero
storage_descriptor_validation_reject_total{reason}          // extends existing family
```

Phase 8 deleted `storage_append_only_identity_synthesis_micros`
— bench (`benches/append_only_tables.rs`) is the right
measurement surface for per-row identity CPU, not an always-on
production histogram.

Architectural tests:

- `RowIdentityPolicy` enum exhaustive in dispatch sites
  (no `_` catch-all).
- Compactor source does not call
  `synthesize_append_only_row_id`.
- Append-only admission does not call
  `canonical_row_payload_hash`; the symbol does not
  appear anywhere in `src/` (Phase 7 deletion guard).
- Descriptor validator rejects the documented invalid
  combos (`AppendOnly + SingleRow`, user `__row_*`
  column).
- No new node capability bit (`NodeCapability::*Append*`,
  `*Tombstone*` greps return empty).
- No new HRFT frame family added by ADR 045.
- No `format_version` bump on the ADR 045 diff scope.
- `__row_tombstone__` does not appear anywhere in ADR 045
  scope (deferred to follow-up ADR).
- `__row_append_ordinal__` does not appear anywhere
  (architectural test guards against accidental
  reintroduction from prior drafts).
- `TableAppendState`, `AppendOrdinal`, and
  `reserve_append_ordinals` symbols do not exist
  (architectural test guards against the counter
  approach being reintroduced).
- No `Option<PrimaryKeyDescriptor>` coexistence shim.
- No `V1` / `V2` suffix on ADR 045 surfaces.
- No project-name prefix on ADR 045 surfaces.
- Brand-prefix grep guards stay green.

Reject codes added if the existing catalog lacks
coverage:

- `AppendOnlyUpdateRejected` — update / upsert against
  append-only.
- `AppendOnlyDeleteByKeyRejected` — delete-by-key against
  append-only.
- `IntraBatchIndexExhausted` — `u32` intra-batch space
  full for a single write.
- `AppendOnlyRequiresMultiRow` — descriptor validation
  reject.

Tests:

- Metric-registry golden output includes the new
  families.
- `supersession_metadata_skipped_total` increments on
  every append-only sealed slab (sanity check that the
  skip path fires).
- `intra_batch_index_exhaustion_total` increments on the
  injected overflow test.
- Reject-code tests cover the append-only-invalid
  operations.
- Architectural tests fail on row-tombstone, capability
  bit, public synthetic-column leakage, or
  reintroduction of the counter approach.

Exit:

- Operators can observe append-only write volume,
  identity-synthesis health, and supersession-collapse
  adoption.

## Phase 7 — Tools, benchmarks, runbook, stabilization

Files:

- `src/bin/dump_slab_record.rs`
- `plans/ops/runbooks/adr045_append_only_tables.md`
- `plans/ops/observability.md`
- `benches/multi_row_fragments.rs` (extend)
- `benches/append_only_tables.rs` (new)

Work:

1. Extend `dump_slab_record` with a `--storage-identity`
   debug flag that surfaces the ADR 042 synthetic columns
   for debugging. Default output remains user-column-only.
2. Operator runbook covers:
   - When to declare a table `APPEND ONLY` vs PK-keyed.
   - Why duplicate payloads are preserved as distinct
     rows.
   - How to interpret the new metrics; what
     `intra_batch_index_exhaustion_total > 0` means
     (single write request larger than 4 G rows; rare).
   - That row-level delete is *not* available in V1; the
     follow-up ADR is the path. V1 retention is at the
     table / range / slab lifecycle level.
   - That ADR 044 column-fetch composes naturally — cold-
     read on append-only is the canonical 044 workload.
   - That writer-identity collisions (cryptographic
     `WriterNodeId` collision) would manifest as
     `RecordLocation` collisions in the redb index;
     observed collision is a key-management / ADR 018
     bug, not an ADR 045 bug.
3. Benchmarks:
   - **Append-only ingest throughput** (rows / s) vs PK-
     keyed baseline at matched batch sizes. Target:
     within 10% of PK-keyed throughput; identity
     synthesis is cheaper than PK canonicalization for
     most schemas (no PK column to canonicalize), so a
     win is plausible.
   - **Identical-payload append cost** — duplicate rows
     don't trigger any short-circuit; baseline cost.
   - **Payload-size sweep** at admission — rows
     held constant, payload size varied (0 / 32 / 256 /
     1024 B). Used in Phase 7 to resolve Open question 1:
     payload hashing scaled linearly with size while
     identity synthesis stayed flat, so the canonical
     row payload hash was dropped from the preimage.
   - **Identity-synthesis CPU share** — measured by the
     bench's `case=identity-synthesis` line. BLAKE3 over the
     84-byte preimage; expected trivial. Phase 8 dropped the
     production histogram (per-row timer overhead not worth
     the always-on signal).
   - **Cold-tier scan over a 50-column append-only
     `MultiRow + Lz4Frame` table** with a 5-column
     projection: bytes-fetched matches ADR 044's target
     (≤ 20% of whole-record).
   - **AGG `count(*)`** equality with raw-row count for
     append-only.
   - **AGG `min(event_time)` / `max(event_time)`** with
     CSB direct-answer on sealed append-only slabs.
   - **Recovery throughput** on a 100M-row append-only
     corpus: rebuild time vs PK-keyed baseline. Expected
     within 10%, possibly faster (no PK
     canonicalization, no counter reconciliation).
4. Stabilization watch (post-merge), mirrors ADR 042 /
   043 / 044 discipline:
   - Rows per multi-row record on append-only tables.
   - CSB direct-answer rate on append-only tables.
   - `supersession_metadata_skipped_total` is nonzero on
     every append-only sealed slab.
   - `intra_batch_index_exhaustion_total` should remain
     zero in steady state.
   - ADR 044 column-fetch metrics on append-only tenants:
     high `bytes_avoided_total`, low
     `phase_a_round_trips` per request, healthy strategy
     distribution.
   - Architectural-test enforcement preserved post-merge.

Exit criteria:

- Append-only ingest throughput within 10% of PK-keyed
  baseline (or better).
- ADR 044 column-fetch metrics on append-only match the
  Phase 11 (ADR 044) targets within bench noise.
- No regression in any existing benchmark for PK-keyed
  tables.
- Recovery on a 100M-row append-only corpus completes
  within 10% of PK-keyed baseline.

## Phase 8 — Locality decoupling, writer-identity binding,
## descriptor-match split, metric hygiene

Companion plan note: `plans/impl/045_phase8_locality_decoupling.md`.
Triggered by post-Phase 7 review findings (the four issues
the design note enumerates).

Summary of work landed in this phase:

- ADR §4a added: AppendOnly locality is computed once per
  request from `(tenant_id, table_object_id, parent_write_id)`
  under the domain tag `"table.append-locality.v1"`. All rows
  in a single write request receive the same `Locality`.
  Bifurcation across replica groups is structurally
  impossible for AppendOnly.
- `WriterNodeId` is bound from the admitting server's
  authenticated `NodeId` (`self.local_node` on the data
  server), passed explicitly into
  `validate_table_write_request_batch`,
  `pack_record_batch_by_bucket`, and `bifurcate_table_write`.
  No more `meta.proxy_source` derivation.
- Descriptor validator rejects AppendOnly +
  `LocalityPolicyV1::IdentityProjection` and AppendOnly +
  `LocalityPolicyV1::ColumnProjection`. AppendOnly V1 only
  supports `TableSpreadProjection`.
- `table_record_matches_descriptor` and
  `descriptor_matches_tiered_row` dispatch on
  `RowIdentityPolicy`. PK keeps the existing re-derivation
  check; AppendOnly verifies schema plus stored
  `__row_table_object_id__` and trusts the stored synthetic
  identity / locality columns. The `zero_hlc` placeholder is
  gone from the AppendOnly path.
- `observe_storage_append_only_batch` is no longer called
  inside admission. The data-server fires it on the success
  path only — after lease gate, idempotency replay miss,
  quota, durable quorum.
- `storage_append_only_identity_synthesis_micros` deleted.
  Bench is the right measurement surface.

Tests: see Phase 8 design note §8.5. New regression coverage:
client-preview / server-admission locality byte-identity for
AppendOnly; AppendOnly batch passes lease gate when the local
node is primary for its single locality; descriptor validator
rejects the two forbidden combinations; descriptor matchers
admit AppendOnly rows that the Phase 7 `zero_hlc` path would
have filtered out.


## Verification gate

Required before closing the implementation pass:

```sh
cargo test --lib --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench multi_row_fragments --no-run
cargo bench --bench append_only_tables --no-run
cargo bench --bench append_only_tables -- --quick
git diff --check
rustfmt --check --edition 2024
```

Grep guards (character-class self-defending):

- Brand-prefix:

  ```sh
  rg -n 'wendelo[n]_.*append|Wendelo[n].*Append|wendelo[n]_.*identit[y]' \
    src tests benches plans
  ```

  Must return empty.

- Capability-bit:

  ```sh
  rg -n 'NodeCapability::.*Append|NodeCapability::.*Tombstone|AppendOnly.*Capability' \
    src tests
  ```

  Must return empty.

- Tombstone leak (deferred to follow-up ADR):

  ```sh
  rg -n '__row_tombstone__|RowTombstone|row_tombstone' src tests benches
  ```

  Must return empty within ADR 045 scope.

- Replicated-counter leak (rejected design):

  ```sh
  rg -n 'TableAppendState|AppendOrdinal|reserve_append_ordinals|__row_append_ordinal__' \
    src tests benches
  ```

  Must return empty within ADR 045 scope.

- Coexistence-shim:

  ```sh
  rg -n 'Option<PrimaryKeyDescriptor>|primary_key:\s*Option' src tests
  ```

  Must return empty.

- Slab-format-version bump:

  ```sh
  rg -n 'slab_format_version\s*[=:]\s*[3-9]|format_version\s*[=:]\s*[3-9]' src
  ```

  Must return empty on the ADR 045 diff scope.

- Version-suffix:

  ```sh
  rg -n 'AppendOnly.*V[0-9]|WriterNodeIdV[0-9]|IntraBatchIndexV[0-9]|RowIdentityPolicyV[0-9]' \
    src tests
  ```

  Must return empty.

Convention guards green:

- No new V1/V2 suffix on ADR 045 surfaces.
- No `#[serde(default)]` shim on the descriptor refactor.
- No reserved enum slots in `RowIdentityPolicy` beyond
  the V1 set.
- No envelope `format_version` bump.
- No new HRFT frame family or reject-code family
  (narrowly-named append-only reject codes are
  extensions of the existing reject machinery, not a new
  family).
- No new node capability bit.
- No `ObjectStore` trait break.
- No stored write-context synthetic columns. The only new
  storage-only column is the Phase 8.5
  `__row_table_object_id__` descriptor-isolation stamp.

## Deferred to bench / implementation discovery / council

Items the implementer or Phase-7 benches resolve. None
block any phase from landing.

1. ~~**Whether the payload hash remains in the identity
   preimage.**~~ **Resolved (Phase 7, 2026-04):** dropped.
   Phase 7 payload-size sweep showed BLAKE3-on-payload
   dominated wide-row admission CPU (per-row gap scaled
   linearly with payload size), while identity synthesis
   stayed flat. `canonical_row_payload_hash` and the
   `storage_append_only_payload_hash_micros` metric were
   deleted; an architectural guard test asserts the
   symbol is not re-introduced.
2. **Whether `IntraBatchIndex` widens to `u64`
   preemptively.** Default V1: `u32` (4 G rows / writer
   per HLC tick). Council/policy decision.
3. **Whether CDC exposes an HLC-derived cursor.**
   Brainstorm 004 territory.
4. **Identity-collision response.** A second
   `RecordLocation` for the same `key_identity` is a
   corruption signal; V1 returns a hard error. Soft-
   recovery modes would require council-level decision.

## Deferred to follow-up ADRs

1. **Row-level tombstones / `DELETE` on append-only
   tables.** Concrete shape sketched in the ADR's
   Forward-compatibility analysis. Pulled by GDPR /
   audit-trail row-delete needs.
2. **Content-hash identity (`AppendOnlyContentHash`).**
3. **PK-keyed row delete via supersession.**
4. **Per-table retention policy.**
5. **Public CDC cursor derived from `__row_hlc__` /
   `__row_key_id__`** — brainstorm 004 / CDC ADR.
6. **Vector-form identity / locality PBC opcodes.**
7. **Append-only `SingleRow` low-latency path.**
8. **`COUNT(DISTINCT __row_key_id__)` short-circuit.**
