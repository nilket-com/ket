# Implementation Plan: ADR 045 — PK-less Append-Only Tables

Companion ADR: `plans/adr/045_pk_less_append_only_tables.md`

## Goal

Add append-only tenant tables whose rows have no user-declared
primary key, no dedupe behavior, and no update / delete-by-key
semantics. Row identity is synthesized from authenticated write
context — `(writer_node_id, row_hlc, intra_batch_index)` plus
canonical-payload-hash — and carried through the existing ADR
042 synthetic-column trio. No replicated counter. No new storage-
only column. Multi-row layout, compression, CSB, AGG, and ADR
044 columnar fetch continue to work unchanged.

## Non-goals

- No row-level `DELETE` / `__row_tombstone__` synthetic column.
- No content-hash identity mode.
- No idempotent-ingest mode.
- No replicated table-local append counter.
- No new storage-only synthetic column.
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
  row_payload_hash = BLAKE3(canonical admitted Arrow row bytes)

  key_identity = BLAKE3(
      "table.append-row"
      || tenant_id              // 16 B
      || table_object_id        // 16 B
      || writer_node_id         // 16 B
      || row_hlc                // 16 B
      || intra_batch_index_be   // u32 big-endian, 4 B
      || row_payload_hash       // 32 B
  )
  ```
  Whether the payload hash stays in V1 is the only open
  question (Open question 1 of the ADR); the BLAKE3 input
  layout is locked either way.
- Helper-shape contract:
  ```rust
  pub fn synthesize_append_only_row_id(
      tenant_id: TenantId,
      table_object_id: TableObjectId,
      writer_node_id: WriterNodeId,
      row_hlc: HybridTimestamp,
      intra_batch_index: IntraBatchIndex,
      row_payload_hash: Hash,
  ) -> RowKeyId;
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
- A `ColumnProjection(event_time)` append-only table.
- A `TableSpreadProjection(spread_factor=4)` append-only
  table — spread input is the synthesized `key_identity`.
- An `IdentityProjection` append-only table — explicit
  opt-in for write-spread-heavy streams.
- A descriptor with omitted locality — confirms the
  `TableSpreadProjection { spread_factor: 4,
  prefix_bits: 16 }` default is resolved before
  fingerprinting and produces byte-identical output to
  the explicit-spread fixture.
- Recovery fixture proving no append-counter state is
  required: a sealed slab corpus reconstructs the redb
  index from `__row_key_id__` / `__row_hlc__` /
  `__row_routing_locality__` alone.
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
     `__row_hlc__`, `__row_key_id__`, or
     `__row_routing_locality__` (defense-in-depth).
   - **Accept** all three existing locality variants
     (`TableSpreadProjection`, `ColumnProjection`,
     `IdentityProjection`).
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
- `RowIdentityPolicy::AppendOnly` admitted with all
  three locality variants and `MultiRow` layout.
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
       row_payload_hash: Hash,
   ) -> RowKeyId;
   ```
   Output: 32-byte `RowKeyId` matching ADR 042's slot.
2. Bind `WriterNodeId` from the authenticated mTLS node
   identity (ADR 018) of the request context. Reuse
   existing node-identity material; do not add a new
   negotiated capability or wire frame.
3. Assign zero-indexed `IntraBatchIndex` values from the
   row's position in the incoming Arrow batch. A row that
   would require `IntraBatchIndex(u32::MAX)` rejects the
   write before any slab append. Serialize the value as
   big-endian `u32` in the identity preimage.
4. Compute canonical row payload hashes through the
   structured Arrow row encoder — not ad hoc byte slicing.
   Canonical encoding must match ADR 037's PK
   canonicalization rules where applicable (NULL
   handling, integer width, decimal scale, timestamp
   precision) so that future content-hash mode can reuse
   the same canonicalization.
5. Add a small internal audit field with row count and
   writer node identity. Do not log every generated row
   identity.
6. Compaction must never call
   `synthesize_append_only_row_id` for an existing row.
   Architectural test asserts.

Tests:

- Golden vector: fixed `(writer, hlc, intra_batch_index,
  payload_hash)` produces a known identity (locks the
  BLAKE3 preimage layout, including the big-endian
  `intra_batch_index` encoding).
- Two writers with identical payload, HLC, and intra-
  batch index produce distinct identities (different
  `writer_node_id`).
- One writer with identical payload and HLC but different
  intra-batch indexes produces distinct identities.
- Two identical payloads in separate batches produce
  distinct identities when HLC differs.
- A batch requiring `IntraBatchIndex(u32::MAX)` rejects
  with the documented reject code; no partial-batch
  admission.
- Domain-tag separation: a constructed PK can never
  BLAKE3-collide with an append-only `key_identity` for
  the same `(tenant_id, table_object_id)` because the
  preimage domain tags differ.
- Architectural test: compactor source does not call
  `synthesize_append_only_row_id`.

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
   - Bind `WriterNodeId`, assign per-row HLCs (ADR 012
     monotonicity), assign `IntraBatchIndex` per row.
   - Compute canonical row payload hash per row.
   - Derive `key_identity` per the ADR §4 preimage.
   - Compute locality from the descriptor's existing
     `LocalityPolicy` (unchanged math; the salt source
     for `TableSpreadProjection` is now `key_identity`).
   - Assign per-row `__row_hlc__` exactly as today.
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
- Locality calculation works for all three locality
  variants on append-only tables.
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
4. Write `__row_hlc__`, `__row_key_id__`, and
   `__row_routing_locality__` for every append-only row.
   **No new synthetic column.**
5. Strip all ADR 042 synthetic columns before user-visible
   decode (existing path; no new stripping rule).
6. Recovery reads `__row_key_id__`, `__row_hlc__`, and
   `__row_routing_locality__` directly from the IPC body
   to rebuild redb indexes. **No append-counter state to
   reconcile.**
7. Compaction preserves `__row_key_id__` and `__row_hlc__`
   exactly per row. Architectural test asserts the
   compactor never calls `synthesize_append_only_row_id`.
8. Stats and CSB writers strip storage-only synthetic
   columns before user-column folding (existing path).
9. CSB writers populate `supersession_metadata = tracked
   None` for sealed append-only slabs.

Tests:

- Encoded append-only IPC contains the ADR 042 synthetic
  storage columns in the documented order — no new
  column.
- User-facing reads strip all synthetic columns.
- Recovery rebuilds the key / locality index without
  consulting any append-counter state — architectural
  test inspects the recovery code path for absence of
  `TableAppendState` / `AppendOrdinal` references.
- Compaction preserves key identities byte-for-byte.
- Bucket-pure packing holds for append-only writes
  spanning multiple locality buckets.
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
storage_append_only_batches_total{tenant}
storage_append_only_rows_written_total{tenant}
storage_append_only_multirow_records_written_total{tenant}
storage_append_only_identity_synthesis_micros               // histogram
storage_append_only_payload_hash_micros                     // histogram
storage_append_only_intra_batch_index_exhaustion_total
storage_append_only_supersession_metadata_skipped_total     // expected nonzero
storage_descriptor_validation_reject_total{reason}          // extends existing family
```

Architectural tests:

- `RowIdentityPolicy` enum exhaustive in dispatch sites
  (no `_` catch-all).
- Compactor source does not call
  `synthesize_append_only_row_id`.
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
   - **Payload-hash CPU share** — visible in
     `storage_append_only_payload_hash_micros`. If hash
     dominates ingest CPU on wide-row workloads, surface
     for council to revisit Open question 1 (drop hash
     from preimage).
   - **Identity-synthesis CPU share** — visible in
     `storage_append_only_identity_synthesis_micros`.
     BLAKE3 over the 100-byte preimage; expected
     trivial.
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
   - Identity-synthesis latency.
   - Payload-hash CPU share.
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
- No new storage-only synthetic column.

## Deferred to bench / implementation discovery / council

Items the implementer or Phase-7 benches resolve. None
block any phase from landing.

1. **Whether the payload hash remains in the identity
   preimage.** Default V1: yes. Bench-resolvable on
   wide-row workloads. Drop if BLAKE3-on-row dominates
   ingest CPU.
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
