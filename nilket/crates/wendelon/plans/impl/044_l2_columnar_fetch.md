# Implementation plan: ADR 044 — L2 Columnar Fetch (Sovereign Pushdown)

Companion to `plans/adr/044_l2_columnar_fetch.md`.

Phase boundaries are designed so the work ships in pieces:
phases 0–5 land the read-side machinery without touching
the L2 hot path; phase 6 picks the strategy; phase 7 wires
it into the L2 read; phases 8–12 refine and stabilize.

## Goal

Implement column-projected cold-tier fetch as a two-phase
chunk-aligned authenticated read against the existing ADR
020 / 022 / 042 / 043 substrate. No on-disk format change;
no AEAD or chunk-layout change; the IPC FlatBuffer is the
single source of truth for per-buffer offsets.

The path from the storage node's perspective:

```text
admission (ADR 038 + 039 + CSB short-circuit ADR 040 + 042)
  → for each admitted record:
      → Phase A: fetch + decrypt the metadata window;
                 parse RecordBufferDirectory.
      → planner: L2ColumnFetchConfig + projection ratio +
                 record-size threshold → whole-record vs.
                 column-projected vs. fallback(reason).
      → Phase B (column path):
                 build ColumnFetchPlan;
                 chunk-aligned Range-GET;
                 AEAD decrypt; LZ4 decompress;
                 decode_projected_record_batch.
      → Phase B (whole-record path):
                 existing read_from_l2.
  → strip ADR 042 synthetic columns;
  → PBC over projected batch;
  → AGG fold over survivors.
```

## Non-goals (V1)

- No homerolled lightweight columnar encodings.
- No mini-page or row-group granularity.
- No per-buffer page directory.
- No `RecordBufferDirectory` cache.
- No bulk-slab metadata pre-fetch.
- No provider-native filtering.
- No Parquet / SSE cold-tier fork.
- No ADR 020 AEAD / nonce / AAD / chunk-size change.
- No ADR 022 footer extension or HMAC-scope change.
- No envelope `format_version` bump.
- No new node capability bit.
- No new HRFT frame family.
- No new reject-code family.
- No new wire-protocol surface to Toron.
- No project-name-prefixed names.
- No `V1` / `V2` suffixes on new types.
- No descriptor field added at all (and therefore no
  `#[serde(default)]` shim).
- No `ObjectStore` trait break.
- No partial-slab rehydration (ADR 006 stays whole-slab).

## Phase 0 — Contract freeze and golden fixtures

Lock the type surface, the node-config surface, the metric
catalog, and the L2 fetch-strategy decision contract.

Outputs:

- New module `src/storage/columnar.rs` (Phase 1 fills it).
  Exposes `RecordBufferDirectory`,
  `TieredMultiRangeReadPlan`, `ColumnProjection`,
  `ColumnFetchRequirement`, `L2FetchStrategy`,
  `ColumnarFallbackReason`, `ProjectedRecordBatch`, plan-
  building helpers.
- New newtypes in `src/schema/units.rs`:
  - `IpcBufferOffset(u64)` — byte offset within an IPC
    record body. Validator applies the
    `max_record_bytes` 8 MiB ceiling from ADR 042 §10.
  - `IpcBufferLength(u64)` — paired with
    `IpcBufferOffset`; same 8 MiB ceiling.
    Defended-in-depth against future `max_record_bytes`
    raises.
  - `IpcBufferIndex(u32)` — Arrow IPC buffer-vector
    ordinal.
  - `RecordBodyByteOffset(u64)` and
    `RecordBodyByteLen(u64)` — record-relative plaintext
    offsets distinct from absolute slab offsets.
  - `EncryptedChunkIndex(u64)` and
    `EncryptedChunkCount(u32)` — typed chunk-domain units
    (per ADR 020). Audit existing crypto code for any raw
    `u64` chunk-index uses and tighten.
- **No `ColumnFetchPolicy` enum.** **No descriptor
  field.** Column fetch is a node-local read strategy
  (ADR 044 §6 / §15).
- Node-local config:
  ```rust
  pub struct L2ColumnFetchConfig {
      enabled: bool,
      ipc_metadata_initial_window_bytes: RecordBodyByteLen,
      max_ipc_metadata_probe_bytes:      RecordBodyByteLen,
      min_record_bytes:                  RecordBodyByteLen,
      max_selected_fraction_micros:      u32, // 0..=1_000_000
      max_ranges_per_record:             EncryptedChunkCount,
      max_parallel_range_gets:           EncryptedChunkCount,
  }
  ```
  Defaults: `enabled = false` until Phase 11 bench targets
  pass; initial probe window 64 KiB (one chunk); probe cap
  256 KiB (4 chunks); `min_record_bytes` 256 KiB; fraction
  670_000 (0.67); ranges per record 8; parallel GETs 4.
  Validation rejects an initial window larger than the
  probe cap, zero probe cap, fraction ≥ 1_000_000, zero
  `min_record_bytes`, and zero range caps.
- Helper-shape contract:
  ```rust
  pub fn parse_record_buffer_directory(
      record_id: TieredRecordId,
      record_plaintext_range: PlaintextByteRange,
      metadata_window_bytes: &[u8],
      descriptor: &TableSchemaDescriptor,
  ) -> Result<RecordBufferDirectory, ColumnFetchError>;

  pub fn plan_tiered_multi_range_read(
      ranges: &[TieredPlaintextRange],
      slab_metadata: &ParsedEncryptedFooter,
  ) -> Result<TieredMultiRangeReadPlan, CryptoError>;

  pub fn build_column_fetch_plan(
      directory: &RecordBufferDirectory,
      requirement: &ColumnFetchRequirement,
      slab_metadata: &ParsedEncryptedFooter,
  ) -> ColumnarRecordFetchPlan;

  pub fn decode_projected_record_batch(
      ipc_bytes: &[u8],
      directory: &RecordBufferDirectory,
      requirement: &ColumnFetchRequirement,
  ) -> Result<ProjectedRecordBatch, ArrowError>;
  ```
- Golden fixtures (extend ADR 043 fixture set):
  - One `MultiRow + Lz4Frame` slab with a 50-column
    schema; columnar fetch and whole-record fetch over
    the same admitted record produce **byte-identical
    logical batches** after PBC filter, projection,
    synthetic-column stripping, and AGG fold.
  - One pre-044 sealed slab (built by the ADR 043 fixture
    pipeline) reads transparently under the new column-
    projected path — bytes-on-disk byte-identical.
  - One sparse-projection fixture (5 of 50 columns).
  - One dense-projection fixture (45 of 50 columns) where
    the planner picks `Fallback(FullProjection)` or
    `Fallback(TooManyBytes)`, depending on the exact
    selected-byte ratio.
  - One small-record fixture where the planner picks
    `Fallback(SmallRecord)` regardless of projection ratio.
  - One UTF-8-heavy slab where projecting one logical
    column fetches all its constituent buffers (data +
    offsets + null buffer).
  - One synthetic-column-touching PBC fixture
    (`__row_hlc__` filter) — verify synthetic buffers
    enter the requirement union and are stripped before
    user-facing output.
  - One nested / dictionary fixture that the V1 partial
    materializer **does not** support — verifies
    `unsupported_arrow_layout` fallback returns identical
    results as whole-record fetch.

Exit:

- All new types and helper signatures agreed and merged
  behind stub `unimplemented!()` bodies.
- Fixtures land in `tests/fixtures/columnar_v1/`.
- Council confirms ADR 044 has no descriptor surface
  change and no slab-format-version bump.

## Phase 1 — Newtypes and node-local read-strategy config

Files:

- `src/schema/units.rs`
- `src/schema/mod.rs`
- `src/config.rs` (or wherever node-local config lives)
- `src/storage/columnar.rs` (new)
- `src/storage/mod.rs`

Work:

1. Add the newtypes from Phase 0 with `Pod + Zeroable`
   impls. Audit `src/crypto.rs` for any raw `u64` chunk-
   index uses and tighten through `EncryptedChunkIndex`.
2. Add `L2ColumnFetchConfig` struct and validator. The
   validator enforces
   `0 < ipc_metadata_initial_window_bytes <=
    max_ipc_metadata_probe_bytes`, `min_record_bytes > 0`,
   `max_selected_fraction_micros < 1_000_000`, and
   non-zero range caps.
3. **Do not** touch `TableSchemaDescriptor`. **Do not**
   add `column_fetch_policy` to the descriptor or its
   fingerprint. The architectural test added in Phase 9
   enforces this absence.
4. Add `ColumnProjection` and `ColumnFetchRequirement`:
   ```rust
   pub struct ColumnProjection {
       columns: SmallVec<[u32; 8]>,  // descriptor ordinals
   }

   pub struct ColumnFetchRequirement {
       predicate_columns: ColumnOrdinalSet,
       output_columns: ColumnOrdinalSet,
       aggregate_columns: ColumnOrdinalSet,
       storage_columns: StorageColumnSet, // synthetic columns
   }

   impl ColumnFetchRequirement {
       pub fn is_full(&self, schema: &Schema) -> bool;
       pub fn user_visible(&self) -> ColumnOrdinalSet;
   }
   ```
   `is_full` is evaluated against the descriptor's
   user-visible Arrow schema, not the IPC schema with ADR
   042 synthetic columns. Storage/synthetic columns never
   make a requirement "full" by themselves.
5. Add stub bodies for the Phase 0 helpers.

Tests:

- Newtype validators reject out-of-range values per the
  convention.
- `L2ColumnFetchConfig::validate` rejects fraction ≥
  1_000_000, zero probe cap, zero range caps.
- `ColumnFetchRequirement::is_full` returns true when all
  user columns are present.
- `ColumnFetchRequirement::user_visible` excludes
  synthetic columns.
- No new V1/V2 suffix or brand-prefixed name on any new
  surface (architectural-test grep).
- `TableSchemaDescriptor` fingerprint is byte-identical
  before and after this phase (architectural test
  enforces no descriptor change).

Exit:

- Type surface and node config are in place; descriptor is
  unchanged; no behavioral change yet.

## Phase 2 — Multi-range crypto planning

Files:

- `src/crypto.rs`
- `src/storage/columnar.rs`
- `tests/l2_rehydration.rs`
- `tests/l2_remote.rs`

Work:

1. Add `TieredMultiRangeReadPlan` next to the existing
   `TieredRangeReadPlan` in `src/crypto.rs`:
   ```rust
   pub struct TieredPlaintextRange {
       start: RecordBodyByteOffset,
       len:   RecordBodyByteLen,
   }
   pub struct TieredMultiRangeReadPlan { /* ... */ }
   ```
2. Input is a sorted list of plaintext record-relative
   ranges plus the already-fetched `ParsedEncryptedFooter`.
3. Convert ranges to ADR 020 chunk runs; coalesce adjacent
   or overlapping chunk runs into the minimum number of
   ciphertext byte ranges.
4. Return enough metadata to drive `l2.get_range` per
   chunk run *and* extract the requested plaintext windows
   after AEAD decrypt.
5. Refactor the existing single-range `plan_tiered_range_read`
   to be a thin wrapper over the multi-range
   implementation. No behavioral change to its callers.
6. **Do not** bypass footer-HMAC validation or per-chunk
   AEAD validation. Any failure in either path surfaces as
   `CryptoError` exactly as it does today.

Tests:

- Multi-range and single-range plans produce identical
  bytes for a one-range request (regression invariant).
- Overlapping plaintext ranges coalesce into one chunk
  run.
- Adjacent ranges within the same encrypted chunk
  coalesce.
- Disjoint plaintext ranges across separated chunks
  produce two chunk runs.
- Tampered footer HMAC fails before any DEK unwrap.
- Tampered chunk AEAD fails. **The failure does not fall
  back to whole-record fetch** — it surfaces as a
  `CryptoError` to the caller. Architectural-test guard.
- Range arithmetic rejects overflow and out-of-record
  spans.

Exit:

- The storage manager can request multiple authenticated
  plaintext spans from one tiered record without
  downloading the whole body.

## Phase 3 — IPC FlatBuffer metadata parser (Phase A)

Files:

- `src/storage/ipc.rs`
- `src/storage/columnar.rs`

Work:

1. Implement `parse_record_buffer_directory` against the
   Arrow IPC `Message` / `RecordBatch` FlatBuffers.
2. The parser reads:
   - 4-byte `metadata_length` prefix per Arrow IPC
     convention.
   - Schema message (one `Message` of type `Schema`).
   - First `RecordBatch` message header.
   - From `RecordBatch.buffers`, extract the
     `(offset, length)` vector (per-buffer ranges relative
     to the IPC body start).
   - From the IPC schema, build the `column_to_buffers`
     map (IPC schema column ordinal → buffer ordinal
     range). Descriptor/user ordinals are mapped to IPC
     ordinals before lookup; ADR 042 synthetic columns use
     their appended IPC slots.
   - Copy the descriptor schema fingerprint into
     `RecordBufferDirectory`; callers use it to reject
     cross-record/schema mixing when a slab-level or
     query-level plan spans multiple records.
3. Define `ColumnarParseError`:
   ```rust
   pub enum ColumnarParseError {
       MetadataTruncated { needed: u64, have: u64 },
       FlatBufferInvalid(String),
       SchemaMismatch { record_id: TieredRecordId },
   }
   ```
   `MetadataTruncated` carries the byte count needed so the
   caller can decide to fetch more.
4. Synthetic columns (`__row_hlc__`, `__row_key_id__`,
   `__row_routing_locality__`, future `__row_tombstone__`)
   are first-class entries in `column_to_buffers`.
5. Architectural-test guard: the parser must not allocate
   Arrow arrays. It is a pure offset-extraction routine.
   Enforced by `assert_no_alloc` in unit tests where
   feasible, otherwise by code-review checklist.

Tests:

- Parse a fixture IPC body's metadata window; assert the
  resulting `RecordBufferDirectory` matches the schema's
  buffer count and offsets.
- Round-trip: encode a known `RecordBatch`, parse its
  metadata, assert each Arrow column's buffer set matches
  the column-to-buffers map.
- Variable-length column (UTF-8) test: a single logical
  column produces 2 buffers (offsets + data); both land in
  `column_to_buffers`.
- Truncation test: feed a short metadata window; assert
  `ColumnarParseError::MetadataTruncated` with the right
  `needed` byte count.
- Synthetic-column test: an ADR 042 multi-row IPC's
  synthetic columns appear with correct ordinals in
  `column_to_buffers`.

Exit:

- Phase A is fully implementable with no L2 changes — a
  test that hands the parser a synthetic 64 KiB metadata
  window from a fixture passes.

## Phase 4 — Column-projected decode (`ProjectedRecordBatch`)

Files:

- `src/storage/ipc.rs`
- `src/storage/columnar.rs`
- `src/schema/pbc.rs` (executor entry point)

Work:

1. Implement `decode_projected_record_batch(ipc_bytes,
   directory, requirement)` returning:
   ```rust
   pub struct ProjectedRecordBatch {
       batch: RecordBatch,                    // narrow physical schema
       ordinal_mapping: ColumnOrdinalMapping, // descriptor → physical
   }
   ```
   The physical batch carries only the buffers
   `requirement` actually demands; the mapping records
   each descriptor ordinal's position in `batch` (or
   `None` if absent).
2. **Correctness must not depend on sparse IPC-body
   behavior.** The partial materializer builds a
   `ProjectedRecordBatch` from selected buffers and *must
   not* require Arrow's reader to walk a sparse plaintext
   stream as the production correctness contract. Two
   implementation paths are admissible:
   - **Preferred path:** Arrow's
     `StreamReaderBuilder::with_projection`, *if* it
     works correctly when the backing byte slice covers
     only the projected buffer ranges (with non-projected
     ranges either omitted or replaced by zero-filled
     placeholders the reader never touches). The path-
     discovery test below decides whether this path is
     production-eligible.
   - **Always-correct fallback path:** bypass
     `StreamReader` entirely for the projected path and
     build Arrow arrays directly from the decrypted,
     decompressed buffers using offsets in
     `RecordBufferDirectory`. This path is always sound
     and is the V1 production code path if the path-
     discovery test fails.
   - **Do not** fabricate dummy null arrays for missing
     user columns. Missing columns are absent from
     `batch` and resolved through `ordinal_mapping`.
     Dummy nulls would silently change AGG / PBC
     semantics if a downstream caller forgets to consult
     the mapping (e.g. `MIN(col_x)` over an all-null
     placeholder yields `NULL` instead of flagging the
     column as unavailable). An architectural unit test
     rejects dummy-null array construction.
   - Phase 0 fixtures lock the contract; Phase 4 step 1
     determines which path the implementation takes.
     Both paths must produce byte-identical results on
     the golden fixtures.
3. Strip ADR 042 synthetic columns from the user-facing
   slice of `ProjectedRecordBatch::user_visible()` even
   when they were fetched for visibility. Storage-only
   columns must never appear in PBC user-visible output
   or in any wire frame to Toron.
4. Add a PBC executor entry point that resolves
   descriptor ordinals through `ordinal_mapping` (or
   wrap the projected batch transparently inside the
   existing executor — implementer's choice as long as
   `LOAD col_x` resolves correctly).
5. Apply terminal `PROJECT` in *descriptor order*, not
   fetch order — output stability is part of ADR 035's
   contract.
6. Verify per-buffer LZ4 decompression fires only on
   projected buffers. Add a counter
   (`storage_l2_column_fetch_decompressed_buffers_total{codec}`)
   to verify in benchmarks.
7. Error path: when `requirement` references a column
   ordinal absent from the IPC schema (e.g. schema drift
   between admission and decode), surface a clear error
   distinguishable from AEAD/corruption errors.

Tests:

- **Path-discovery test (one-off):** feed
  `StreamReaderBuilder::with_projection` a backing slice
  with non-projected ranges replaced by zero-bytes.
  Compare results to whole-record decode + post-project.
  If equal: the **preferred path** is correct, register
  it as the production code path. If not equal or panic:
  the **fallback path** runs in production; the path-
  discovery test stays as a regression guard against
  silently switching back to the broken preferred path.
- **Architectural unit test: dummy-null array rejection.**
  A constructor that would build a `RecordBatch` column
  filled with nulls for an unfetched user column must
  fail loudly (compile-time `#[deprecated]` + runtime
  `unreachable!()` if anyone bypasses the deprecation).
  Verifies the §6 / Rationale "no second interpretation
  of column presence" invariant.
- Partial materialization equals whole-record decode for
  primitive fixed-width columns.
- Partial materialization equals whole-record decode for
  Utf8 / Binary columns (multi-buffer per logical
  column).
- `Lz4Frame` selected buffers decompress correctly;
  non-projected buffers have `decompressed_buffers`
  counter = 0.
- Null bitmaps are honored.
- Terminal `PROJECT` output order is descriptor-order,
  not fetch-order.
- PBC filter mask over `ProjectedRecordBatch` matches
  whole-record PBC output.
- Synthetic columns fetched for visibility filter are
  stripped before user-visible output.
- Unsupported nested / dictionary layouts return
  `unsupported_arrow_layout` error before decoding (
  Phase-6 catches this; Phase 4 fails loudly).

Exit:

- Decode side is independently shippable. Phase 7 will
  glue it to the L2 fetch path.

## Phase 5 — PBC + AGG column-requirement derivation

Files:

- `src/schema/pbc.rs` (read-only walker)
- `src/storage/columnar.rs`

Work:

1. Implement `analyze_column_requirement(program: &Program)
   -> Result<PbcColumnRequirement, PbcRejectCode>` walking
   validated bytecode.
2. Walker enumerates:
   - Every `LOAD` / `PUSH_COL` operand used before
     terminal `PROJECT` (predicate columns).
   - The terminal `PROJECT` opcode's column list (output
     columns; defaults to all user columns when absent).
   - Synthetic-column references (when an opcode touches
     `__row_hlc__`, `__row_key_id__`,
     `__row_routing_locality__`, or future
     `__row_tombstone__`) — these contribute to the
     `storage_columns` axis, never to user output.
3. Re-confirm against ADR 040's accumulator catalog that
   every accumulator declares its inputs in a walkable
   form. If any accumulator does not, add an
   accumulator-specific dispatch table here before landing
   the walker.
4. Add `analyze_aggregate_requirement(request:
   &AggregateRequest)` that walks accumulator declarations
   and emits the union of input columns referenced.
5. Build `ColumnFetchRequirement` as the union of:
   - `predicate_columns` from PBC.
   - `output_columns` from terminal `PROJECT`.
   - `aggregate_columns` from AGG accumulators.
   - `storage_columns` from visibility/HLC/identity needs.
6. Reject programs that imply a column-ordinal beyond the
   active descriptor's schema (defense-in-depth; should
   be caught earlier by ADR 036's contract validation).
7. Preserve existing PBC reject-code behavior; do not add
   a new opcode.

Tests:

- A canonical PBC program with `LOAD col_a`,
  `LOAD col_b`, `PROJECT col_a, col_c` yields a
  requirement with `predicate_columns = {col_a, col_b}`,
  `output_columns = {col_a, col_c}`.
- A program with only `PROJECT *` (or no terminal
  `PROJECT`) short-circuits `output_columns` to the full
  user schema; `is_full(descriptor_user_schema)` returns
  true.
- An AGG request with `count_star` only yields the empty
  user-column requirement (count_star needs no value
  columns); `user_visible()` is empty.
- An AGG request with `min(col_x), max(col_y)` yields
  `aggregate_columns = {col_x, col_y}`.
- A program touching `__row_hlc__` records the synthetic
  ordinal in `storage_columns`; the synthetic never
  appears in `user_visible()`.
- A program referencing an out-of-range ordinal returns
  the existing PBC reject code (defense-in-depth).

Exit:

- Storage manager can build a `ColumnFetchRequirement` at
  admission time, before any L2 fetch.

## Phase 6 — Fetch planner and `L2FetchStrategy`

Files:

- `src/storage/columnar.rs`
- `src/storage/manager.rs` (strategy branch)
- `src/metrics/registry.rs`

Work:

1. Implement `build_column_fetch_plan(directory,
   requirement, slab_metadata)` using
   `plan_tiered_multi_range_read` from Phase 2:
   - Resolve `ColumnFetchRequirement` descriptor/user
     ordinals to IPC schema ordinals, then resolve those
     IPC ordinals to buffer ordinals via
     `directory.column_to_buffers` (every physical buffer a
     column needs: validity + offsets + values + dictionary
     payloads + child buffers + compression metadata).
   - Map buffer ordinals → IPC body byte ranges via
     `directory.buffers`.
   - Translate IPC body ranges → record-relative
     plaintext ranges (offset by
     `record_plaintext_range.start`).
   - Hand the sorted plaintext ranges to
     `plan_tiered_multi_range_read` for chunk-alignment
     and coalescing.
2. Implement the strategy picker:
   ```rust
   pub fn pick_l2_fetch_strategy(
       config: &L2ColumnFetchConfig,
       descriptor: &TableSchemaDescriptor,
       record_location: &RecordLocation,
       requirement: &ColumnFetchRequirement,
       directory: Option<&RecordBufferDirectory>,
   ) -> L2FetchStrategy;
   ```
   Decision rule (ADR §6) — cheap bypasses run *before*
   Phase A so they do not pay the metadata round trip:
   - `!config.enabled`              → `Fallback(Disabled)`
   - `descriptor.record_layout_policy.is_single_row()`
                                    → `Fallback(SingleRow)`
   - `requirement.is_full(descriptor_user_schema)`
                                    → `Fallback(FullProjection)`
   - `record.length < min_record_bytes`
                                    → `Fallback(SmallRecord)`
   - rehydration active for record  → `Fallback(RehydrationAvailable)`
   - **(Phase A runs here — caller hands a directory in
     iff the probe succeeded.)**
   - directory missing (probe failed within cap) →
                                       `Fallback(MetadataProbeCap)`
   - layout unsupported by partial materializer →
                                       `Fallback(UnsupportedArrowLayout)`
   - `plan.fetched_bytes + probe_bytes > whole * fraction` →
                                       `Fallback(TooManyBytes)`
   - `plan.chunk_runs.len() > max_ranges_per_record` →
                                       `Fallback(TooManyRanges)`
   - otherwise                       → `Columnar(plan)`
   `record_layout` comes from
   `descriptor.record_layout_policy` (ADR 042). The
   `schema` argument is the descriptor's user-visible Arrow
   schema; `is_full` ignores ADR 042 synthetic columns.
3. **Authentication failures never enter
   `L2FetchStrategy`.** AEAD-verify or footer-HMAC
   failures surface as `CryptoError` to the caller; the
   strategy enum has no variant for "auth failed."
   Architectural-test guard.
4. Record planning metrics:
   - `storage_l2_column_fetch_plan_micros` histogram.
   - `storage_l2_column_fetch_selected_chunk_runs` histogram.
   - `storage_l2_column_fetch_fallback_total{reason}` counter.

Tests:

- Sparse projection picks `Columnar`.
- Dense projection picks `Fallback(TooManyBytes)`.
- Range count above cap picks `Fallback(TooManyRanges)`.
- Single-row records pick `Fallback(SingleRow)`.
- Unsupported nested / dictionary layout picks
  `Fallback(UnsupportedArrowLayout)` without changing
  result bytes.
- Full-projection picks `Fallback(FullProjection)`.
- AEAD failure during Phase A surfaces as `CryptoError`;
  no `L2FetchStrategy` value emitted (architectural test).
- Tampered footer HMAC surfaces as `CryptoError`; no
  fallback (architectural test).
- Plan total `fetched_bytes` is monotonic in requirement
  size.
- Plan never returns chunk runs that overlap.
- Plan reuses `plan_tiered_multi_range_read` (and
  thereby `plan_tiered_range_read` math) — no parallel
  chunk-math impl. Architectural-test guard.

Exit:

- Every admitted L2 record has an explicit strategy
  before bytes are fetched.
- Plans are verifiable end-to-end without any actual L2
  traffic.

## Phase 7 — L2 read path integration

Files:

- `src/storage/l2.rs`
- `src/storage/manager.rs` (`read_from_l2`,
  `read_encrypted_record_from_l2`)
- `src/storage/columnar.rs`

Work:

1. Wire the two-call planner pattern explicitly:
   - First call `pick_l2_fetch_strategy(...,
     directory=None, candidate_plan=None, ...)`.
     If it returns any cheap-bypass `Fallback(*)`, dispatch
     directly to whole-record fetch and project post-decode.
   - Otherwise run Phase A and parse the
     `RecordBufferDirectory`.
   - Build the multi-range candidate plan, then call
     `pick_l2_fetch_strategy(..., directory=Some(...),
     candidate_plan=Some(...), ...)`. Dispatch this
     second result.
2. Implement `read_projected_record_from_l2(l2, pointer,
   record_location, requirement, descriptor, config) ->
   Result<ProjectedRecordBatch, _>`:
   ```text
   Phase A: derive metadata window from
            record_location.plaintext_range, take min(
            max_ipc_metadata_probe_bytes, record.length);
            plan_tiered_multi_range_read on that prefix;
            l2.get_range; AEAD decrypt;
            parse_record_buffer_directory.
            On MetadataTruncated { needed }, grow the
            window by chunk runs and re-fetch; bound the
            growth to max_ipc_metadata_probe_bytes; if
            still truncated, return MetadataProbeCap to
            the strategy picker.

   Post-Phase-A planner: pick_l2_fetch_strategy with the
            parsed directory and candidate plan; on
            Fallback(*), delegate to existing
            read_encrypted_record_from_l2 and project
            post-decode.

   Phase B (Columnar): l2.get_range for each chunk run
            (bounded parallel via the existing async I/O
            pool, capped at max_parallel_range_gets);
            AEAD decrypt; hand the decrypted plaintext
            extracts and the directory to
            decode_projected_record_batch.
   ```
3. **No "sparse IPC body assembly."** The Phase 4
   `decode_projected_record_batch` consumes
   `(decrypted_plaintext_extracts, directory,
   requirement)` and builds Arrow arrays only for the
   buffers it asked for. Output is a
   `ProjectedRecordBatch` with `ordinal_mapping` resolved
   against the descriptor — the PBC executor then runs
   through the mapping. This shape side-steps the question
   about whether Arrow's `StreamReader` honours sparse
   plaintext (resolved by the Phase 4 path-discovery test).
4. `read_from_l2` becomes a dispatcher driven by
   `L2FetchStrategy`:
   ```rust
   fn read_from_l2(...) -> ... {
       match strategy {
           L2FetchStrategy::Columnar(plan) =>
               read_projected_record_from_l2(plan, ...),
           L2FetchStrategy::WholeRecord
           | L2FetchStrategy::Fallback(_) =>
               read_encrypted_record_from_l2(...),
       }
   }
   ```
   `WholeRecord` is produced only by the dispatcher for a
   known whole-record path where the column planner is not
   invoked (for example, an unencrypted L2 object without
   ADR 020 chunk/index metadata). `pick_l2_fetch_strategy`
   returns `Columnar(plan)` or `Fallback(reason)`.
5. The PBC and AGG handlers in `src/network/data_server.rs`
   compute `ColumnFetchRequirement` from the request once
   per slab admission and pass it down.
6. **CSB pruning is unchanged and remains upstream of the
   strategy picker.** Pruned slabs issue zero record-body
   range GETs.
7. **Rehydration stays whole-slab.** Column-projected reads
   increment existing rehydration heat counters; once
   promotion triggers, the manager fetches and installs
   the full slab as today (ADR 006).
8. **L1 reads are unchanged.** ADR 044 affects the
   tiered-L2 cold path only.

Tests (integration):

- Cold-tier scan of a `MultiRow + Lz4Frame` table with a
  5-column projection over a 50-column schema returns
  results byte-identical to whole-record fetch.
- Cold-tier scan with `L2ColumnFetchConfig::enabled = false`
  always uses whole-record fetch.
- Cold-tier scan with full projection picks
  `Fallback(FullProjection)`.
- Pre-044 sealed slab (no on-disk change) reads
  transparently under both paths.
- Mixed-shape slab with one record near `min_record_bytes`
  exercises the `SmallRecord` fallback.
- Variable-length column projection (UTF-8) returns
  correct values.
- Synthetic-column-touching predicate
  (`WHERE __row_hlc__ > X`) triggers synthetic-column
  buffer fetch and produces correct visibility output;
  the synthetic column is stripped from the response.
- Cold-tier AGG over a narrow accumulator
  (`min(col_x)`) fetches only `col_x`'s buffers.
- AGG `count_star` fetches no user value columns.
- Mixed L1 / L2 scans preserve output order and contents.
- L2 retry on Phase B preserves Phase A metadata; retry
  on Phase A re-parses metadata.
- CSB-pruned slabs issue no record-body range GETs.
- Columnar reads increment rehydration heat counters; the
  rehydration trigger still installs the whole slab.
- AEAD failure during Phase B decryption surfaces as a
  `CryptoError`, **not** as a fallback to whole-record
  fetch (architectural test).
- Cold-tier scan benchmark shows fetched-byte reduction
  matching projection ratio (within chunk-boundary
  noise).

Exit:

- Storage node serves cold-tier reads via column-projected
  fetch when the planner says so.
- Operator can flip `enabled = false` cluster-wide via
  node config and observe whole-record fetch resume on
  the next config reload — no descriptor edit, no
  fingerprint churn.

## Phase 8 — Selectivity / planner refinements

Files:

- `src/storage/columnar.rs`
- `src/storage/manager.rs`

Work:

1. Extend the cheap-bypass path from Phase 6 / Phase 7
   with cross-record Phase A batching. Records already
   classified as disabled, single-row, full projection,
   small, or rehydration-resident continue to skip Phase A
   entirely; remaining records can share coalesced
   metadata-window GETs.
2. Batch Phase A across multiple admitted records when
   their record-plaintext ranges are contiguous (or
   nearly contiguous within one chunk). One L2 GET serves
   N records' metadata.
3. Add `storage_l2_column_fetch_phase_a_skipped_total` and
   `storage_l2_column_fetch_phase_a_batched_records_total` metrics so
   Phase 12 stabilization can see the round-trip
   savings.

Tests:

- Phase A skip fires when threshold not met; metric
  reflects.
- Phase A batching coalesces N contiguous records'
  metadata into one GET.
- Skip + batch produce results byte-identical to the
  baseline path.

Exit:

- Round-trip count metric drops on workloads that hit the
  skip / batch paths.

## Phase 9 — Observability and architectural guards

Files:

- `src/metrics/registry.rs`
- `src/storage/columnar.rs` (instrumentation points)
- metric smoke tests
- `tests/architecture_test.rs`

New metrics (functional segment names; brand-clean per
ADR 041 / convention):

```text
storage_l2_column_fetch_requests_total{strategy}              // strategy ∈ {columnar, whole_record, fallback}
storage_l2_column_fetch_fallback_total{reason}                // reason ∈ ColumnarFallbackReason
storage_l2_column_fetch_range_gets_total{strategy}
storage_l2_column_fetch_probe_bytes_total
storage_l2_column_fetch_record_body_bytes_fetched_total{strategy}
storage_l2_column_fetch_record_body_bytes_avoided_total
storage_l2_column_fetch_selected_columns                       // histogram
storage_l2_column_fetch_selected_chunk_runs                    // histogram
storage_l2_column_fetch_plan_micros                            // histogram
storage_l2_column_fetch_decode_micros                          // histogram
storage_l2_column_fetch_decompressed_buffers_total{codec}
storage_l2_column_fetch_overfetch_bytes_total                  // chunk-boundary overage
storage_l2_column_fetch_phase_a_round_trips_total
storage_l2_column_fetch_phase_a_skipped_total
storage_l2_column_fetch_phase_a_batched_records_total
storage_l2_column_fetch_metadata_truncated_refetch_total
storage_l2_column_fetch_metadata_parse_error_total
storage_l2_column_fetch_auth_error_total                       // expected zero
```

Architectural tests:

- Metric segment names match convention; no brand prefix
  on protocol surface.
- Architectural-test grep guards from Phase 1 stay green.
- New metrics appear in metric-registry golden output.
- `ColumnarFallbackReason` enum exhaustiveness is
  enforced (no `_` arm in dispatch sites).
- **No descriptor field added.** Architectural-test
  asserts `TableSchemaDescriptor` is unchanged on the ADR
  044 diff.
- **No new node capability bit.** Architectural-test
  greps `NodeCapability::*` for `Column` / `L2Column` /
  `Pushdown.*Column` and expects empty.
- **No Parquet / ORC dependency** added by ADR 044.
- **No provider-native filtering identifier** (`S3Select`,
  `SelectObjectContent`) introduced.
- **No slab-format-version bump** on the ADR 044 diff
  scope.
- Partial L2 reads route exclusively through Phase 2's
  multi-range plan + crypto validation helpers; no
  parallel chunk-math impl.

Tests:

- Metrics render with all `strategy` and `reason` label
  values after a fixture cold scan.
- `phase_a_round_trips_total` increments once per Phase A
  GET (and zero on skipped path).
- `bytes_avoided_total` equals
  `whole_record_bytes - bytes_fetched_total` on the
  columnar path; per-record sanity check.
- Plan complexity histogram populates with sane values.
- Overfetch counter reflects chunk-boundary overage on a
  fixture where the projected buffer is smaller than its
  containing chunk.
- `auth_error_total` stays at zero on healthy fixtures;
  increments on injected ciphertext-tamper test (and the
  query fails as a `CryptoError`, not a fallback).
- Architectural-test grep guards return empty for
  brand-prefixed surfaces, descriptor fields, capability
  bits, Parquet, S3 Select, and slab-format-version
  bumps.

Exit:

- Phase 12 stabilization signals are all wired.

## Phase 10 — Recovery and operator tooling

Files:

- `src/storage/manager.rs` (recovery scan)
- `tools/dump_slab_record` (extend existing helper)

Work:

1. Recovery scan runs whole-record fetch unconditionally —
   it needs every record body. ADR 044 introduces no new
   recovery dependency.
2. Extend `tools/dump_slab_record` to optionally accept a
   column projection (`--project col_a,col_b`) and exercise
   the column-projected path against a real slab. Useful
   for operator post-hoc debugging.
3. Operator runbook entry:
   `plans/ops/runbooks/adr044_columnar_fetch.md`
   documenting:
   - How to flip `L2ColumnFetchConfig::enabled` on a node
     (config reload).
   - How to interpret the new metrics, including which
     `ColumnarFallbackReason` to act on.
   - When to disable cluster-wide vs. tune
     `max_selected_fraction_micros`,
     `max_ranges_per_record`,
     `max_ipc_metadata_probe_bytes`, or
     `min_record_bytes`.
   - **When access-pattern leakage justifies disabling
     columnar fetch.** Tenants whose threat model treats
     query column choice as confidential should disable
     the path via node config or run on a private
     object-store backend. ADR 022's chunk-granularity
     leakage is qualitatively the baseline; ADR 044
     makes the leakage more column-correlated. The
     runbook gives operators the decision criteria.
   - That AEAD / footer-HMAC failures are *never*
     fallback signals — they surface as `CryptoError`
     and indicate corruption, key drift, or a reader
     bug, not a tuning opportunity.

Tests:

- Recovery on a mixed-compression mixed-strategy slab
  corpus reproduces in-memory index identically.
- `dump_slab_record --project` materializes the right
  columns.

Exit:

- Operator surface is documented; no recovery regression.

## Phase 11 — Benchmarks

Files:

- `benches/multi_row_fragments.rs` (extend)
- `benches/columnar_fetch.rs` (new)

Benchmarks:

- Cold-tier scan of a 50-column `Lz4Frame` `MultiRow`
  table, projecting 1 / 5 / 25 / 50 columns. Targets:
  - 1-column projection: ≤ 5% of whole-record fetch
    bytes (best case).
  - 5-column projection: ≤ 15% of whole-record fetch
    bytes (typical analytical case).
  - 25-column projection: ≤ 60% of whole-record fetch
    bytes; planner decides per record.
  - 50-column projection: 100% (planner picks whole-
    record fetch).
- Round-trip count: 1.0–1.2 GETs per admitted record on
  scattered fixtures; ≤ 1.05 with Phase 8 batching on
  contiguous fixtures.
- PBC scan latency on column-projected fetch within 10% of
  whole-record fetch (decode CPU savings ≈ network
  savings).
- AGG `min(col_x)` over `Lz4Frame` `MultiRow`: cold-tier
  bytes drop to ≈ 1/N of full-record bytes (for N
  columns).
- Variable-length-column projection (UTF-8 column over a
  text-heavy schema): bytes-fetched matches buffer-set
  expectation.
- Synthetic-column projection (`__row_hlc__` filter)
  fetches synthetic-column buffers; bench documents the
  cost.
- Mixed-shape slab range scan: per-record planner overhead
  visible in `storage_l2_column_fetch_plan_micros` but does not
  dominate scan time.
- Pre-044 sealed slab: column-projected fetch works
  byte-identically.

Exit criteria:

- Bytes-fetched on 5-of-50-column projection ≤ 20% of
  whole-record baseline on the default analytical schema.
- PBC scan throughput within 10% of whole-record baseline
  on cold tier.
- No regression in any existing benchmark when
  `L2ColumnFetchConfig::enabled = false`.
- L2 GET-rate increase ≤ 25% on contiguous-record
  workloads (with Phase 8 batching).

## Phase 12 — Stabilization watch (post-merge)

Mirrors ADR 039 / 040 / 042 / 043 discipline.

New under 044:

1. **Strategy distribution.**
   `storage_l2_column_fetch_requests_total{strategy}`.
   Healthy ratio depends on workload; alarming signal is
   `strategy="columnar"` at 0% on a fleet that should be
   using the new path.
2. **Round-trip cost.**
   `storage_l2_column_fetch_phase_a_round_trips_total /
   storage_l2_column_fetch_requests_total{strategy="columnar"}`.
   Should hover near 1.0; > 1.5 means metadata window is
   too small (tune up to 128 KiB).
3. **Phase A skip rate.**
   `storage_l2_column_fetch_phase_a_skipped_total`. Reflects
   small-record + full-projection workloads short-
   circuiting; high rate is *good*, not a regression.
4. **Plan complexity.**
   `storage_l2_column_fetch_selected_chunk_runs`
   distribution.
   Long-tail records with many chunk runs may benefit from
   tighter coalescing (Phase 8 refinement).
5. **Overfetch ratio.**
   `storage_l2_column_fetch_overfetch_bytes_total / fetched_bytes`.
   Lower bound is chunk-boundary alignment (ADR 020 64
   KiB chunks); persistent growth means coalescing
   regressed.
6. **GET-rate cost.** L2 GETs per query before/after.
   On fleets where this dominates byte savings, operator
   may flip `L2ColumnFetchConfig::enabled = false`
   cluster-wide.
7. **Cold-tier latency.** Cold-tier read p50 / p99
   before/after on a representative tenant.
8. **Decode buffer count.**
   `storage_l2_column_fetch_decompressed_buffers_total{codec="lz4_frame"}` /
   total buffers. Ratio < 1 means upstream projection is
   working.
9. **Metadata-parse errors.**
    `storage_l2_column_fetch_metadata_parse_error_total` should
    be zero in steady state.
10. **Truncated-window refetch.**
    `storage_l2_column_fetch_metadata_truncated_refetch_total`
    should be near zero. Non-zero means
    `ipc_metadata_initial_window_bytes` (default 64 KiB)
    is undersized for the workload's typical schema; bump
    closer to `max_ipc_metadata_probe_bytes` (default
    256 KiB).
11. **Phase A skip ratio.**
    `storage_l2_column_fetch_phase_a_skipped_total /
     storage_l2_column_fetch_requests_total{strategy="columnar"}`.
    High ratio is *good* — it means cheap bypasses
    (full-projection, small-record, rehydration) are
    catching the no-win cases without paying the
    metadata round trip.
12. **Architectural-test enforcement.** New guards stay
    green:
    - No new V1/V2 suffix or brand-prefix on ADR 044
      surfaces.
    - `ColumnarFallbackReason` enum has no `_` catch-all
      in match sites (exhaustiveness preserved).
    - Phase 6 plan-building reuses Phase 2's
      `plan_tiered_multi_range_read`; no parallel
      chunk-math impl.
    - `TableSchemaDescriptor` is unchanged on the ADR 044
      diff (no descriptor field added).
    - No new node capability bit.
    - No Parquet / ORC dependency added.
    - No provider-native filtering identifier.
    - No slab-format-version bump on the ADR 044 diff
      scope.
    - Operator runbook present at the documented path.

Carryover from earlier ADRs:

- All ADR 042 + 043 watch items remain applicable. ADR
  044 adds bytes-saved-on-cold-tier as a new top-line
  signal but does not retire any earlier one.

## Verification gate

Required before closing the implementation pass:

- `cargo test --lib --quiet`
- `cargo test --test architecture_test --quiet`
- `cargo bench --bench multi_row_fragments --no-run`
- `cargo bench --bench columnar_fetch --no-run`
- `cargo bench --bench columnar_fetch -- --quick`
- `git diff --check`
- `rustfmt --check --edition 2024` on touched Rust files
- Brand-prefix grep guard (character-class self-defending):

  ```sh
  rg -n 'wendelo[n]_.*column|Wendelo[n].*Colum[n]|W[C]olum[n]|W[C]olumna[r]' \
    src tests benches plans
  ```

  Must return empty.

- Provider-native filtering / Parquet grep guard:

  ```sh
  rg -n 'Parque[t]|parque[t]|OR[C]|S3 Selec[t]|SelectObjectConten[t]' \
    src tests benches
  ```

  Must return empty (intentional scope excludes
  `plans/`).

- Slab-format-version bump grep guard:

  ```sh
  rg -n 'slab_format_version\s*[=:]\s*[3-9]|format_version\s*[=:]\s*[3-9]' src
  ```

  Must return empty on the ADR 044 diff scope.

- Capability-bit grep guard:

  ```sh
  rg -n 'NodeCapability::.*Column|ColumnarL2Fetch|L2ColumnarFetch.*Capability' \
    src tests
  ```

  Must return empty.

- Descriptor field grep guard:

  ```sh
  rg -n 'column_fetch_policy|ColumnFetchPolicy' src tests
  ```

  Must return empty (no descriptor field; this is a
  node-config-only feature).

- Convention guards green:
  - No new V1/V2 suffix on ADR 044 surfaces.
  - No `#[serde(default)]` shim added (no descriptor
    field at all).
  - No reserved enum slots in `ColumnarFallbackReason`
    beyond the V1 set.
  - No envelope `format_version` bump.
  - No new HRFT frame family or reject-code family.
  - No new node capability bit.
  - No `ObjectStore` trait break.

## Deferred to bench / implementation discovery

Items the implementer or Phase-11 benches resolve. None
block any phase from landing; each phase calls out the
specific decision its work surfaces.

1. **Initial Arrow-layout coverage in the partial
   materializer** (Phase 4). V1 scope decision: primitive
   fixed-width + Utf8/Binary only
   (`unsupported_arrow_layout` fallback covers the rest),
   or full type coverage including Struct / List /
   Dictionary / Union? Default V1 is the narrower set with
   fallback for nested / dictionary; bench output drives
   any expansion.
2. **Phase A batching window size** (Phase 8 step 2). One
   chunk? Two chunks? Workload-shape dependent; defer to
   benchmark output.
3. **Whether `RecordBufferDirectory` interns the schema
   fingerprint or compares full schemas.** Interning costs
   one hash table; full compare costs schema-size bytes
   per compare. Default is fingerprint compare via the
   existing `SchemaFingerprint` newtype.
4. **Whether the `__row_tombstone__` synthetic column
   (deferred per ADR 042 §16) gets pre-wired into
   `column_to_buffers` ahead of its ADR.** Default is no —
   wire it in when the row-level tombstone ADR opens
   (convention: no premature backward-compat).
5. **Whether to surface `bytes_avoided_total` as
   gauge-shaped or counter-shaped.** Default is the
   counter shape (the question "what bytes were saved
   this query" is fundamentally counter-shaped); the
   gauge is derivable from
   `bytes_avoided_total / requests_total` if operator
   dashboards want it.
