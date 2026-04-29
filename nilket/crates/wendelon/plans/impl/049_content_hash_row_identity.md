# Implementation Plan: ADR 049 — Content-Hash Row Identity

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/049_content_hash_row_identity.md`
Inherits identity layer:
`plans/adr/045_pk_less_append_only_tables.md`
Inherits substrate: `plans/adr/046_state_digest_substrate.md`
Inherits lifecycle composition:
`plans/adr/047_row_tombstones_append_only.md`,
`plans/adr/048_per_table_retention_policy.md`
Bench reference: `plans/impl/046_state_digest_bench_results.md`,
`plans/impl/047_row_tombstone_bench_results.md`,
`plans/impl/048_table_retention_bench_results.md`.

## Goal

Add the third `RowIdentityPolicy` variant
(`AppendOnlyContentHash`) with content-derived row identity,
synchronous admission-time duplicate collapse, sparse
type-tagged canonical encoding, and visible-domain
`StateDigest` composition. Preserve existing `AppendOnly` and
`PrimaryKey` semantics unchanged.

The implementation lets content-hash identity drive:

- write admission with absorb-vs-store branching;
- canonical row encoding (a new module separate from any IPC
  code path);
- descriptor validation (allowed locality combinations;
  rejected schema mutations);
- visible-domain `StateDigest` element add / no-add at
  admission, composing with ADR 047 / 048 re-ingestion
  semantics;
- architectural guards and operator metrics.

## Deliverables

- New `RowIdentityPolicy::AppendOnlyContentHash` variant.
- New canonical row encoder module producing sparse type-
  tagged byte streams over logical Arrow values.
- Stable `column_tag` surface — assigned column IDs exposed
  through the descriptor (Phase 1 confirms whether one exists
  or adds one).
- Synchronous-collapse admission path: row-index lookup
  branching to absorb-vs-store.
- Two new descriptor-level reject codes:
  `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT` and
  `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.
- Locality-policy validation: enable
  `AppendOnlyContentHash + IdentityProjection`; reject
  `AppendOnlyContentHash + ColumnProjection`.
- Architectural guards: no raw IPC bytes as identity preimage;
  no schema generation in the preimage; no project-name
  prefix; no V1 / V2 type suffixes.
- Golden vectors covering each supported Arrow type, sparse
  encoding under nullable evolution, and the absorb-vs-store
  admission distinction.
- Bench: payload-hash isolation; row-index-lookup cost;
  `IdentityProjection` locality cost; tombstone / retention
  re-ingestion smoke.
- Operator metric:
  `storage_content_hash_duplicate_absorptions_total` per
  tenant / table.

## Non-goals

- No anti-entropy protocol.
- No durable `StateDigest` placement.
- No PK-keyed content identity.
- No PK-keyed delete semantics.
- No descriptor fingerprint split beyond what this plan
  forces.
- No SQL parser work.
- No V1 / V2 Rust type suffixes.
- No compatibility shim or reserved enum variants.
- No async-only duplicate dedup path.
- No `ColumnProjection` for content-hash tables.
- No non-NULL-default additive schema evolution.
- No decimal precision / scale evolution.
- No multiset duplicate semantics.
- No new HRFT frame family.
- No new node capability bit.
- No slab footer field.
- No `ObjectStore` trait change.
- No `format_version` bump.
- No envelope or wire-frame bump.
- No `digest.table.row.content.v1` (or similar) domain.
  ADR 046's reserved-tag mechanism is left untouched for a
  future consumer with a concrete reader.
- No tolerant decoder for "pre-content-hash" descriptor
  shapes. Tables predating `AppendOnlyContentHash` keep their
  existing variant; conversion is a future ADR's call.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the descriptor variant, canonical-encoding shape, and
admission-path contract before touching engine code.

Files:

- `src/schema/units.rs` (newtype additions)
- `src/schema/identity.rs` (or wherever `RowIdentityPolicy`
  lives) — variant placeholder
- `src/storage/content_hash.rs` (new — canonical encoder
  skeleton + identity preimage)
- `tests/fixtures/content_hash/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze the variant:
   ```rust
   pub enum RowIdentityPolicy {
       PrimaryKey(PrimaryKeyDescriptorV1),
       AppendOnly,
       AppendOnlyContentHash,
   }
   ```
   No reserved enum slot. No `V1` / `V2` type suffix.

2. Freeze the identity preimage tag and helper signature:
   ```rust
   pub const DOMAIN_CONTENT_HASH_ROW: &[u8] =
       b"table.append-content-row.v1";

   pub fn content_hash_row_key_id(
       tenant: TenantId,
       table: TableObjectId,
       canonical_row_bytes: &[u8],
   ) -> RowKeyId;
   ```

3. Freeze the canonical-encoder signature:
   ```rust
   pub fn encode_row_canonical(
       descriptor: &TableSchemaDescriptor,
       row: &RowView,
   ) -> Result<CanonicalRowBytes, CanonicalEncodeError>;
   ```
   Sparse output: concatenation of non-null
   `column_tag | type_tag | value_length | value_bytes`
   entries sorted by ascending `column_tag`.

4. Freeze `column_tag` shape. Phase 1 confirms whether the
   descriptor already exposes a stable assigned column ID.
   If yes, reuse; if no, Phase 1 adds one. Phase 0 captures
   the contract that `column_tag` is **stable across additive
   evolution and column reordering**.

5. Freeze admission-path branching:
   ```rust
   pub enum ContentHashAdmissionOutcome {
       Absorbed,
       Stored { added_digest_element: bool },
   }

   pub fn admit_content_hash_row(
       descriptor: &TableSchemaDescriptor,
       row: &RowView,
       row_index: &RowIndex,
       digest: &mut StateDigestAccumulator,
   ) -> Result<ContentHashAdmissionOutcome, AdmissionError>;
   ```

6. Freeze reject codes:
   - `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT` — raised at
     descriptor validation when a column-add mutation
     supplies a non-NULL default on a content-hash table.
   - `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION` — raised at
     descriptor validation when a decimal precision / scale
     mutation is attempted on a content-hash table.
   - `CONTENT_HASH_REJECTED_INVALID_UTF8` — raised at the
     canonical encoder if a string value fails UTF-8
     validation. Rust's `&str` invariant typically prevents
     this; the reject covers the binary / string boundary if
     any consumer hands raw bytes.

7. Architectural guards (TODO entries; populated by later
   phases):
   - No raw Arrow IPC bytes used as identity preimage.
   - No schema generation in the preimage.
   - No physical descriptor fingerprint in the preimage.
   - No physical column ordinal as `column_tag`.
   - No `digest.table.row.content.v1` byte tag in source.
   - No `AppendOnlyContentHashV1` / similar suffixed types.
   - No project-name prefix on new symbols
     (`WendelonContentHash*` / `wendelon_content_hash_*`).
   - No `format_version` bump.
   - No new HRFT frame family.

Fixtures under `tests/fixtures/content_hash/`:

- Row encoded under schema gen N; same row re-encoded under
  schema gen N+1 (with a new nullable column) → identical
  canonical bytes → identical `__row_key_id__`.
- Row written twice under a content-hash table → first write
  changes visible state and digest; second write absorbed,
  visible state and digest unchanged.
- Row tombstoned (ADR 047), then re-ingested → store path
  taken; visible state restored; digest element re-added.
- Row retention-evicted (ADR 048), then re-ingested → store
  path taken; same digest behavior.
- Column-add mutation with non-NULL default on a content-hash
  table → rejected with
  `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`.
- Decimal scale-change mutation on a content-hash table →
  rejected with `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.
- Descriptor validating
  `AppendOnlyContentHash + ColumnProjection` → rejected.
- Descriptor validating
  `AppendOnlyContentHash + IdentityProjection` → accepted.
- Per-type canonical-encoding golden vectors (booleans,
  integers, floats with NaN / -0, decimals, timestamps in
  each unit, strings under NFC, dictionary arrays).
- List / struct / map / nested types → encoder rejects.
- Extension types without local canonicalization → encoder
  rejects.

Exit:

- Contract text and fixtures exist; tests fail with
  `unimplemented!()` until later phases.
- Architectural guards in place.

## Phase 1 — Types, descriptor validation, stable column ID

Files:

- `src/schema/identity.rs`
- `src/schema/descriptor.rs`
- `src/schema/units.rs`
- `tests/descriptor_validation.rs`

Work:

1. Add `RowIdentityPolicy::AppendOnlyContentHash` to the
   variant set. Validate the descriptor.

2. **Stable column ID resolution.**
   - If the descriptor already exposes a stable assigned
     column ID per logical column, reuse it as `column_tag`.
   - If not, add `column_id: ColumnId` to the column-
     descriptor shape. `ColumnId` is a `u64` newtype
     monotonic-allocated by the descriptor at column
     creation; never reused, never changed across schema
     mutations.
   - Architectural guard: `column_tag` derivation must use
     the stable ID, not the physical ordinal.

3. Validate locality combinations on `AppendOnlyContentHash`:
   - `AppendOnlyContentHash + IdentityProjection` → accepted.
   - `AppendOnlyContentHash + ColumnProjection` → rejected.
   - `AppendOnlyContentHash + DefaultLocality` → accepted
     (whatever the cluster default is, treat as a non-
     locality-projection table).
   - `AppendOnlyContentHash + SingleRow` (if such a layout
     exists) → match the ADR 045 reject pattern if it
     applies; otherwise accepted.

4. Validate schema-mutation rejects on content-hash tables:
   - `column-add` with non-NULL default →
     `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`.
   - Decimal precision / scale change →
     `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.
   - Type change on an existing column → reject (general
     no-type-change rule; named in the reject taxonomy).

5. Ensure `schema_fingerprint` includes the
   `RowIdentityPolicy` variant choice (so adding a content-
   hash table changes the fingerprint, but does not require
   a separate descriptor field for identity-policy
   configuration beyond the variant).

Tests:

- Create-table with `AppendOnlyContentHash`: descriptor
  validates; `schema_fingerprint` reflects the variant.
- Create-table with `AppendOnlyContentHash + ColumnProjection`:
  rejected.
- Create-table with
  `AppendOnlyContentHash + IdentityProjection`: accepted.
- `column-add` with `nullable: true, default: NULL` on a
  content-hash table: accepted.
- `column-add` with `nullable: true, default: <value>` on a
  content-hash table: rejected with the typed code.
- `column-add` with `nullable: false` on a content-hash table:
  rejected with the typed code (non-NULL by definition).
- Decimal column scale change on a content-hash table:
  rejected with the typed code.
- Stable column ID is allocated on column creation; not
  reused after column drop.
- Architectural guard: `column_tag` derivation lookup-grep.

Exit:

- Descriptor variant, locality validation, and schema-
  mutation rejects are in place.
- Stable column ID surface is available for the canonical
  encoder.

## Phase 2 — Canonical row encoder

Files:

- `src/storage/content_hash.rs` (encoder body)
- `src/storage/canonical_types.rs` (per-type encoding)
- `tests/canonical_encoding.rs`

Work:

1. Implement the structured canonical encoder following §3
   of the ADR.

2. Per-type encoding:
   - Boolean: 1-byte `0x00` / `0x01`.
   - Integer (i8/i16/i32/i64/u8/u16/u32/u64): little-endian
     fixed-width.
   - Float (f32/f64): IEEE 754 LE; canonicalize NaN to
     `f32::NAN` / `f64::NAN` quiet-NaN bit pattern; map
     `-0.0 → +0.0`.
   - Decimal: precision (u8) + scale (u8) + unscaled-integer
     LE (width per declared precision).
   - Timestamp: convert to UTC (offset stripped); encode as
     i64 in declared unit.
   - Date / time / interval / duration: per-type pinned
     fixed-width logical value.
   - UTF-8 string: NFC-normalize via `unicode-normalization`
     crate; length-prefix (varint or u32 LE; pin in this
     phase); bytes follow.
   - Binary: length-prefix; bytes follow.
   - Dictionary array: decode dictionary indices to logical
     values; encode the logical value, never the dictionary
     id.
   - List / struct / map / nested: encoder rejects with
     `CanonicalEncodeError::UnsupportedType`.
   - Extension type: rejects unless local canonicalization
     pinned (none in V1).

3. Sparse encoding logic:
   - Iterate columns in `column_tag` ascending order.
   - For each non-NULL column, append
     `column_tag | type_tag | value_length | value_bytes`.
   - For NULL or absent columns, append nothing.

4. Synthetic `__row_*` columns are excluded by construction
   (the encoder takes a user-column projection of the row).

Tests:

- Per-type golden vectors (one per supported Arrow type).
- NaN canonicalization (signaling, quiet, `-NaN`,
  payload-bearing) collapses to one bit pattern.
- `+0.0` and `-0.0` encode identically.
- Decimal scale boundary cases.
- Timestamp unit conversions across ns / µs / ms / s.
- NFC normalization on strings: combining-character
  sequences canonicalize.
- Invalid UTF-8 (raw byte input) raises
  `CONTENT_HASH_REJECTED_INVALID_UTF8` if applicable.
- Dictionary array → encodes logical values.
- List / struct / map / nested → encoder rejects.
- Sparse property: encoding row R under schema gen N +
  encoding row R under schema gen N+1 (new nullable column,
  unset) produce identical canonical bytes.
- Column reordering invariance: reordering columns in the
  descriptor (without changing assigned column IDs)
  produces the same canonical bytes for the same logical
  row.

Exit:

- Canonical encoder is complete and exhaustively tested.
- Identity preimage helper is wired through to produce
  `__row_key_id__` from canonical bytes.

## Phase 3 — Write admission with synchronous collapse

Files:

- `src/network/data_server.rs` (write admission entry)
- `src/storage/manager.rs` (row-index lookup)
- `src/storage/content_hash.rs` (admission helper)

Work:

1. Branch admission on `RowIdentityPolicy::AppendOnlyContentHash`:
   ```rust
   match descriptor.row_identity_policy() {
       RowIdentityPolicy::AppendOnly => admit_append_only(...),
       RowIdentityPolicy::PrimaryKey(_) => admit_pk(...),
       RowIdentityPolicy::AppendOnlyContentHash =>
           admit_content_hash_row(...),
   }
   ```

2. Inside `admit_content_hash_row`:
   - Call `encode_row_canonical(...)` → `canonical_row_bytes`.
   - Call `content_hash_row_key_id(...)` → `K`.
   - Lookup `K` against the table's row index under current
     visible membership (excluding tombstoned and retention-
     evicted rows).
   - If `K` is currently visible: return
     `Absorbed`. **No physical record written. No
     `StateDigest` element added.** Return idempotent
     success to the client.
   - If `K` is not currently visible: store the row
     (multi-row record using the existing append-only
     packing path), add `visible_row_element(t, T, K)` to
     the visible-domain `StateDigest` accumulator, return
     `Stored { added_digest_element: true }`.

3. Preserve `AppendOnly` and `PrimaryKey` admission paths
   unchanged.

4. Wire the admission helper to record the operator metric:
   `storage_content_hash_duplicate_absorptions_total` per
   tenant / table.

Tests (lib + integration):

- First write of content `C` to a content-hash table →
  `Stored { added_digest_element: true }`; visible state and
  digest reflect the row.
- Second write of identical content `C` → `Absorbed`; visible
  state and digest unchanged.
- Tombstone the row, then re-ingest content `C` → `Stored`;
  digest element re-added.
- Retention-evict the row (force compaction with policy that
  crosses the row's HLC), then re-ingest content `C` →
  `Stored`; digest element re-added.
- Two writes with **different** canonical content (e.g.,
  one column differs) → both `Stored`; two distinct
  `__row_key_id__` values; digest reflects two elements.
- Mixed-table batch: writes to an `AppendOnly` table and a
  content-hash table interleaved; each takes its own
  admission path; cross-table state stays consistent.
- Architectural guard: duplicate admission on a content-hash
  table never reaches the `AppendOnly` blind-append path.

Exit:

- Synchronous-collapse admission is in place; tombstone and
  retention re-ingestion compose correctly; metric is
  observable.

## Phase 4 — Packing, recovery, compaction

Files:

- `src/storage/multirow.rs`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`

Work:

1. Multi-row packing preserves `__row_key_id__` and
   `__row_hlc__` byte-for-byte for content-hash rows. No new
   record-shape bits.

2. Recovery from durability log: a content-hash row's
   `__row_key_id__` is the value stored in the slab; recovery
   does **not** recompute identity from a possibly newer
   descriptor. (If a future descriptor has new columns, the
   stored identity is still correct because old rows
   physically lacked those columns and sparse encoding makes
   the absence identity-neutral.)

3. Compaction (L1 / L2) preserves `__row_key_id__` byte-for-
   byte. No re-canonicalization at compaction time.

4. ADR 047 pair-GC and ADR 048 retention reclaim compose
   without changes — they operate on `__row_key_id__` and
   `__row_hlc__`, both of which are stable for content-hash
   rows.

Tests:

- Pack a content-hash row, repack it through the multi-row
  layout, read it back: `__row_key_id__` byte-identical.
- Recovery from durability replay produces the same
  `__row_key_id__` as the original write.
- L1 → L2 compaction preserves identity; compacted slab has
  the same `__row_key_id__`s.
- ADR 047 pair-GC reclaims a content-hash row's data and
  tombstone correctly under per-table retention floors.

Exit:

- Identity is stable across packing, recovery, and
  compaction.

## Phase 5 — Reads, AGG, PBC, CSB

Files:

- `src/storage/aggregate.rs`
- `src/schema/pbc.rs`
- `src/storage/columnar.rs` (ADR 044 fetch)
- `src/storage/manager.rs` (scan)

Work:

1. Scan / read paths handle content-hash tables with no new
   shape changes; `__row_key_id__` is the row identity, same
   as PK and AppendOnly tables.

2. AGG: `COUNT(*)` and `COUNT(DISTINCT __row_key_id__)` agree
   on content-hash tables under collapse semantics (every
   physical record corresponds to a distinct logical row,
   because admission absorbed duplicates synchronously).

3. PBC: predicate evaluation on content-hash tables produces
   the same results regardless of how many times the content
   was written (collapse).

4. CSB: visibility metadata composes with content-hash
   identity unchanged. The CSB straddle gate (ADR 048) and
   tombstone visibility (ADR 047) work as before because
   they operate on `__row_hlc__` and `__row_key_id__`, both
   of which behave the same as in the existing variants.

5. Latest-as-of and time-travel reads on content-hash tables
   return rows visible at the requested HLC. Re-ingestion
   that occurred after a tombstone produces a new
   `__row_hlc__` for the re-added row; the time-travel read
   selects the appropriate physical record.

Tests:

- `COUNT(*)` and `COUNT(DISTINCT __row_key_id__)` match on
  a content-hash table after a duplicate-write sequence.
- PBC filter `column_a = X` returns the same row regardless
  of how many duplicate writes occurred.
- Latest read after tombstone-then-re-ingest returns the
  re-added row.
- Time-travel read between tombstone and re-ingest returns
  the row as visible at that point in history (or hidden if
  tombstoned).

Exit:

- Read / AGG / PBC / CSB paths handle content-hash tables
  with no surprising semantics.

## Phase 6 — Bench

Files:

- `benches/content_hash.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/049_content_hash_bench_results.md` (new,
  populated when the bench runs)

Bench matrix:

1. **Admission cost** (100k and 1M rows):
   - Content-hash admission with no duplicates (every write
     hits the store path).
   - Content-hash admission with 10% duplicates.
   - Content-hash admission with 100% duplicates.
   - Compare against ADR 045 `AppendOnly` admission baseline
     (write-context identity, no row-index lookup).

2. **Wide-row case** (load-bearing per ADR 049 §"Why this is
   next" rationale):
   - Reproduce the ADR 045 wide-row regime (the case where
     ADR 045 dropped payload hashing for CPU reasons).
   - Compare content-hash admission cost against ADR 045
     wide-row admission cost.

3. **Bench partitioning** (mandatory):
   - Isolate **payload-hash cost** (canonical encoding +
     BLAKE3) from **row-index-lookup cost** (visible-
     membership lookup) so a slow path has a named source.
   - Per-call timing breakdown reported in the bench output.

4. **`IdentityProjection` locality**:
   - Locality projection on content-hash tables vs PK-keyed
     tables vs `AppendOnly` tables (where allowed).

5. **Tombstone / retention re-ingestion smoke** for the
   absorb-vs-re-add distinction:
   - First write: content `C` → store path; digest
     element added.
   - Tombstone the row: digest element subtracted; row not
     visible.
   - Re-ingest `C`: store path; digest element re-added.
   - Re-ingest `C` again (still visible): absorb path; no
     digest change.
   - Same sequence with retention eviction in place of
     tombstone.
   - This catches bugs that confuse "known key" with
     "currently visible key" — a stale visibility check
     would silently absorb the post-tombstone re-ingest and
     drop the re-added row.

Acceptance starting points (refine after one quick smoke
run):

- Admission cost on no-duplicate content-hash writes within
  2× of ADR 045 wide-row baseline. The factor explains the
  payload-hash + lookup overhead; faster than 2× is good
  news.
- `IdentityProjection` cost on content-hash tables matches
  PK-keyed `IdentityProjection` cost within 10%.
- Duplicate-absorb path returns within one row-index lookup
  (no payload-hash recomputation cached; lookup hit short-
  circuits).
- Bench output partitions payload-hash vs lookup cost so
  regressions have a named source.

Bench results documented in
`plans/impl/049_content_hash_bench_results.md`. Quick smoke
is acceptable for code review; full 100k / 1M matrix is the
ADR 049 promotion-to-shipped gate.

## Phase 7 — Architectural guards, metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/audit.rs`
- `tests/metrics_smoke.rs`
- `tests/architecture_test.rs`
- `plans/ops/runbooks/content_hash_identity.md` (new)

Metrics:

```text
storage_content_hash_admissions_total{tenant, table, outcome=absorbed|stored}
storage_content_hash_duplicate_absorptions_total{tenant, table}
storage_content_hash_canonical_encode_failures_total{tenant, reason}
storage_content_hash_descriptor_rejects_total{tenant, reason=non_null_default|decimal_evolution|column_projection}
```

Audit:

- Descriptor mutation rejects emit audit events with the
  reject code, the table identity, the actor, and the
  attempted mutation shape.
- Admission absorptions do **not** emit audit events
  (volume); the metric is the operator-visible signal.

Runbook (`content_hash_identity.md`):

- When to use `AppendOnlyContentHash` vs `AppendOnly`
  (idempotent re-ingestion is the differentiator).
- Schema-mutation restrictions and the typed reject codes.
- How to interpret the duplicate-absorption metric (replay
  rate; idempotency hit rate).
- Operator workflow: enable, query, replay, observe.
- Composition with tombstones (ADR 047) and retention
  (ADR 048) — re-ingestion semantics.

Architectural guards (extend `tests/architecture_test.rs`):

- ☐ `RowIdentityPolicy::AppendOnlyContentHash` exists and is
  validated.
- ☐ Content-hash table never accepts a non-NULL default
  additive column.
- ☐ Content-hash table rejects `ColumnProjection`.
- ☐ Content-hash table rejects decimal precision / scale
  evolution.
- ☐ Duplicate admission on a content-hash table passes
  through the absorption path, not a blind append path. A
  regression that silently re-routes duplicates through the
  standard append path fails this guard.
- ☐ Raw Arrow IPC bytes never used as canonical row identity
  (grep guard on the canonical encoder module).
- ☐ Schema generation never enters the identity preimage
  (grep guard).
- ☐ Physical descriptor fingerprint never enters the identity
  preimage (grep guard).
- ☐ `column_tag` derivation uses the stable column ID, not
  the physical ordinal.
- ☐ No project-name prefix on new symbols
  (`WendelonContentHash*` / `wendelon_content_hash_*`).
- ☐ No V1 / V2 suffix on new Rust types
  (`AppendOnlyContentHashV[0-9]`,
  `ContentHashIdentityV[0-9]`,
  `RowIdentityPolicyV[0-9]`).
- ☐ No `digest.table.row.content.v1` byte tag in source
  (reserved-tag mechanism remains for a future ADR).

Exit:

- Operators can observe content-hash admission activity,
  audit descriptor mutations, follow a runbook for common
  operations.

## Verification gate

Required before declaring ADR 049 shipped:

```sh
cargo test --lib content_hash
cargo test --lib table
cargo test content_hash --quiet
cargo test append_only --quiet
cargo test --test architecture_test --quiet
cargo test --test state_digest_invariants --quiet
cargo test --test descriptor_validation --quiet
cargo test --test canonical_encoding --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench content_hash --no-run
cargo bench --bench content_hash -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Domain-tag well-formedness (extends ADR 046):
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output equals the ADR 046 set; no new domains added by this
  ADR.

- Identity-domain tag for content-hash:
  ```sh
  rg -n 'b"table\.append-content-row\.v1"' src tests
  ```
  Present and used in exactly one place (the canonical-
  encoder identity helper).

- No raw Arrow IPC bytes used as identity preimage:
  ```sh
  rg -n 'arrow_ipc::.*as_row_key|arrow_ipc::.*identity_preimage' src tests
  ```
  Empty.

- No schema generation in the preimage:
  ```sh
  rg -n 'schema_generation.*row_key_id|row_key_id.*schema_generation' src tests
  ```
  Empty.

- Reject codes:
  ```sh
  rg -n 'CONTENT_HASH_REJECTED_NON_NULL_DEFAULT' src tests
  rg -n 'CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION' src tests
  rg -n 'CONTENT_HASH_REJECTED_INVALID_UTF8' src tests
  ```
  Each present and exercised by tests.

- Synchronous-collapse admission path:
  ```sh
  rg -n 'duplicate_absorption|absorbed_duplicate|content_hash_duplicate' src tests
  ```
  Present and exercised by tests.

- No ColumnProjection on content-hash:
  ```sh
  rg -n 'AppendOnlyContentHash.*ColumnProjection' src tests
  ```
  Empty except tests that assert rejection.

- No version suffixes on ADR 049 Rust types:
  ```sh
  rg -n 'AppendOnlyContentHashV[0-9]|ContentHashIdentityV[0-9]|RowIdentityPolicyV[0-9]' src tests
  ```
  Empty.

- No project-name prefix on new content-hash symbols:
  ```sh
  rg -n '\bWendelonContentHash[A-Z]|\bwendelon_content_hash[a-z_]' src tests benches
  ```
  Empty.

- No `digest.table.row.content.v1` byte tag:
  ```sh
  rg -n 'b"digest\.table\.row\.content\.v[0-9]+"' src tests
  ```
  Empty (reserved-tag mechanism remains for a future ADR).

Convention guards:

- No `#[serde(default)]` shim on content-hash fields.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change beyond the descriptor variant
  selection.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- `RowIdentityPolicy::AppendOnlyContentHash` is the third
  identity variant.
- Synchronous admission-time collapse via row-index lookup;
  absorb-vs-store branching at admission.
- Sparse type-tagged canonical encoding over logical Arrow
  values; raw IPC bytes never used.
- `column_tag` is a stable assigned column ID (preferred) or
  stable logical-name hash (fallback); never the physical
  ordinal.
- NFC string normalization.
- Identity preimage:
  `BLAKE3("table.append-content-row.v1" || tenant ||
  table || canonical_row_bytes)`.
- Schema generation and descriptor fingerprint not in the
  preimage.
- Reject non-NULL default additive column-add with
  `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`.
- Reject decimal precision / scale evolution with
  `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.
- Reject `ColumnProjection`; enable `IdentityProjection`.
- Visible-domain `StateDigest` element added at admission
  store path; not added on absorb path.
- Re-ingestion after tombstone or retention takes the store
  path (key not currently visible).
- No new digest domain; reserved-tag mechanism intact.
- Operator metric:
  `storage_content_hash_duplicate_absorptions_total`.

Out of Wendelon's scope (client-tool ADR territory): DDL
syntax for declaring `AppendOnlyContentHash` tables.

## Deferred to follow-up ADRs

Mirrors ADR 049 *Deferred* section:

1. PK-keyed content identity (PK + content hash combined).
2. Per-column retention on content-hash tables.
3. `ColumnProjection` enable for content-hash tables (likely
   alongside roadmap #8 secondary indexes).
4. Non-NULL default additive evolution migration semantics.
5. Decimal precision / scale evolution migration semantics.
6. List / struct / map / nested type canonicalization.
7. Extension type canonicalization framework.
8. Physical / content row-set digest domain
   (`digest.table.row.content.v1` reserved by future ADR).
9. Column rename semantics on content-hash tables (under
   logical-name-hash `column_tag` fallback).
10. Anti-entropy reconciliation protocol over content-hash
    digests.
