# Implementation Plan: ADR 062 — Row-Group Sizing Profile

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/062_row_group_sizing_profile.md`

Inherits substrate:

- `plans/adr/056_row_group_fetch_and_page_directory.md` —
  `RowGroupWriterConfig`, `RecordPageDirectory`,
  `L2FetchStrategy::RowGroup`, three-phase fetch planner,
  paging-engagement gate.
- `plans/adr/057_per_buffer_pages_and_encoding_cascades.md` —
  `EncodingDirectory`, per-buffer pages.
- `plans/adr/060_sparse_projection_fetch_narrowing.md` —
  sparse projection-fetch planner.
- `plans/adr/061_page_aligned_authenticated_chunk_layout.md` —
  closed AEAD chunk-size set + macro-block coalescing; §"V1
  deferred" defers row-group sizing to ADR 062.
- `plans/conventions/no-planning-phase-names.md`,
  `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`,
  `plans/conventions/capability-vs-feature-naming.md`.

## Status

Council-converged across Claude / Codex / Gemini after five
council iterations. Implementation phases pending; Phase 7
warehouse-scale bench evidence is the promotion-to-shipped
gate.

Implementation contract — council-converged:

- closed two-variant `RowGroupSizingProfile` enum
  (`PointRead`, `Scan`);
- deployment-wide selection via
  `RowGroupWriterConfig.profile`; no per-record selector;
- `PointRead` preserves ADR 056 V1 defaults byte-identically;
- `Scan` profile sizing (council-converged):
  - `target_row_group_rows = 65_536`;
  - `max_row_group_rows    = 131_072` (= 2 × target, slack
    headroom matching ADR 056 convention);
  - `min_paged_record_rows = 65_536` (= target, enables
    one-entry directories);
  - `min_paged_record_bytes = 1 MiB`;
- `RowGroupWriterConfig` refactored: `profile` field +
  private `sizing` + accessor methods;
  `Default::default()` byte-equivalent to today;
- operator config: `profile` + `PointRead`-only numeric
  overrides escape hatch (`Scan` + numeric overrides emits
  warning, ignores numeric values);
- one-entry `Scan` directory accepted; `PointRead` preserves
  today's `batches.len() <= 1` single-batch fallback;
- `enabled = false` stays default (ADR 062 is sizing not
  rollout);
- writer threading through accessor methods at
  `src/storage/multirow.rs` and any other call site;
- no descriptor / wire / capability / PBC / digest-domain /
  `ObjectStore` / IPC custom_metadata / metric-label change;
- no new fallback / reject / catalog variant;
- no ADR 022 / ADR 056 directory shape change;
- byte-identical projected-batch invariant pinned across the
  two profiles for the same logical input;
- bench artifact demonstrates `Scan`-profile p50 ≥ 1 MiB
  per-row-group dense GET payload + `PointRead`-profile non-
  regression + bench-validated `Scan` target row count.

## Goal

Make ADR 056 row-group sizing a deployment-wide access-pattern
declaration so deployments serving analytical scan workloads
emit row groups large enough to saturate ADR 061's macro-block
coalescer, while preserving today's OLTP-shape sizing for
deployments serving point-read workloads.

The architecture realizes this by introducing a closed
two-variant access-pattern profile and refactoring
`RowGroupWriterConfig` so the four numeric thresholds derive
from the profile:

```text
operator config: profile = "scan"
  → RowGroupWriterConfig.profile = Scan
  → RowGroupWriterConfig.sizing = scan values
  → writer reads thresholds via accessors
  → row groups emitted at Scan sizing
  → directory + AEAD encryption proceed unchanged
  → ADR 061 chunk layout + ADR 044 coalescer compose unchanged
  → reader path unchanged (directory carries actual row counts)
```

Today the writer always uses `target = 4_096`, regardless of
deployment intent. ADR 062 makes the access-pattern intent
declarable; `Scan` deployments emit larger row groups
deterministically.

## Deliverables

- `src/storage/row_group.rs`:
  - add `RowGroupSizingProfile` enum (`PointRead`, `Scan`);
  - add private `RowGroupSizingValues` struct holding the four
    numeric values;
  - refactor `RowGroupWriterConfig` to `{ enabled, profile,
    sizing (private), stats_supported_types }`;
  - add `point_read()` / `scan()` constructors;
  - add accessor methods (`target_row_group_rows()`,
    `max_row_group_rows()`, `min_paged_record_rows()`,
    `min_paged_record_bytes()`);
  - add `PointRead` + `Scan` pinned `const` sizing values.
- `src/config.rs` + `RawRowGroupWriterConfig`:
  - add `profile: Option<String>` field;
  - keep four numeric fields as `PointRead`-only overrides;
  - closed parser: `point_read` / `scan`; unknown rejects;
  - `profile = "scan"` + numeric override → emit config-load
    warning, ignore numeric values;
  - `profile = "point_read"` (or omitted) → numeric values
    apply as overrides on `PointRead` defaults.
- `src/storage/multirow.rs`:
  - replace direct field reads with accessor calls;
  - add explicit one-entry `Scan` directory disposition
    (`Scan` profile bypasses today's `batches.len() <= 1`
    single-batch fallback when paging gate engaged).
- `examples/wendelon.toml.example`:
  - document `profile = "point_read"` (default) /
    `profile = "scan"`;
  - keep numeric fields documented as `PointRead`-only
    overrides (with warning that they're ignored at `Scan`).
- `tests/architecture_test.rs`:
  - add ADR 062 lockdown guards.
- `tests/row_group_config.rs` (new or extended):
  - profile parsing + validation tests;
  - `PointRead`-only numeric override tests;
  - `Scan` + numeric override warning tests;
  - accessor tests (`PointRead` returns ADR 056 V1 defaults;
    `Scan` returns pinned constants).
- `tests/encoding_chunk_layout.rs`:
  - extend with profile-axis assertions (`Scan` profile +
    every closed ADR 061 chunk size).
- `tests/row_group_sparse_projection_fetch.rs`:
  - extension at `Scan` profile.
- `tests/encoding_secondary_index.rs`:
  - verify ADR 052 point-read selected-bytes gate at `Scan`
    profile.
- `tests/l2_remote.rs` / `tests/l2_rehydration.rs`:
  - `PointRead`-profile slabs read identically; `Scan`-profile
    slabs round-trip.
- `tests/multirow.rs` (or equivalent):
  - profile-keyed unit tests + cross-replica determinism
    property suite + boundary-fixture (exact target rows →
    one-entry `Scan` directory) + boundary-plus-one
    (target + 1 → two-entry `Scan` directory).
- `benches/row_group_fetch.rs`:
  - extend with profile axis + target-rows candidate sweep +
    schema-shape sweep + scan-scale fixtures.
- `plans/impl/062_row_group_sizing_profile_bench_results.md`:
  - checked-in evidence artifact (Phase 7 output).
- `plans/adr/056_row_group_fetch_and_page_directory.md`:
  - §"Where we are" amendment after Phase 8 promotion.
- `plans/wip/roadmap.md`:
  - planning-cadence update at promotion (not before).

## Non-goals

- No new public PBC opcodes; ADR 035 / 036 byte-identical.
- No `RecordBodyCompressionPolicy` variant.
- No new `L2FetchStrategy` variant.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
  bit / IPC `custom_metadata` key / digest domain.
- No new `EncodingRejectCode` / `EncodingFallbackReason` /
  `RowGroupFallbackReason` / `RowGroupRejectCode` /
  `EncodingComputeRejectCode` /
  `EncodingComputeFallbackReason` / `PredicateAtomOp` /
  `EncodedRowMask` / `EncodingCascadeKind` /
  `EncodingComputeEvaluatorKind` / `EncodedAggregateKind`
  variants.
- No new fields in `EncodingWriterConfig`,
  `L2EncodingFetchConfig`, `L2RowGroupFetchConfig`,
  `EncodingComputeConfig`, `L2ChunkLayoutConfig`.
- No operator-tunable `Scan`-profile knob.
- No per-record geometry-keyed selector.
- No per-cascade flag, no per-record activation knob.
- No Cargo feature gating profile selection.
- No nightly rustc.
- No paging-engagement-gate change beyond profile-derived
  threshold values.
- No descriptor surface change.
- No `ObjectStore` trait change.
- No envelope `format_version` bump.
- No retirement of the current OLTP-shape sizing.
- No migration sweeper for existing slabs.
- No Toron-visible contract change.
- No new metric label.
- No internal decline reasons leaking to public surface.
- No per-row-group profile within a single record.
- No third profile in V1.
- No tail-absorption row-slicing algorithm beyond ADR 056's
  existing `max = 2 × target` slack headroom.

## Phase 0 — Contract audit

Lock the closed surface and confirm composability before
touching code.

Files (read-only):

- `plans/adr/030_deployment_profiles_and_support_envelope.md`,
  `plans/adr/055_lsm_leveled_compaction_substrate.md`,
  `plans/adr/056_row_group_fetch_and_page_directory.md`,
  `plans/adr/061_page_aligned_authenticated_chunk_layout.md`,
  `plans/adr/062_row_group_sizing_profile.md`.
- `src/storage/row_group.rs`, `src/storage/multirow.rs`,
  `src/storage/encoding/chunk_layout.rs`, `src/config.rs`,
  `examples/wendelon.toml.example`.

Work:

1. Confirm `PointRead` / `Scan` is the council-locked naming.
2. Confirm `RowGroupWriterConfig` is deployment-wide.
3. Confirm ADR 055 LSM compaction's per-output-slab
   writer-config snapshot includes the row-group writer
   config.
4. Inventory all direct reads of `target_row_group_rows`,
   `max_row_group_rows`, `min_paged_record_rows`,
   `min_paged_record_bytes`. These all migrate to accessor
   calls.
5. Inventory config loader tests + examples that set the four
   numeric fields. Migration path: keep as `PointRead`-only
   overrides; emit warning at `Scan`.
6. Confirm composability:
   - ADR 060: per-row-group decision grain unchanged;
   - ADR 061: per-slab chunk size unchanged; bigger row
     groups give the coalescer longer contiguous chunk runs;
   - ADR 044: per-record fraction gate unchanged;
   - ADR 052: `RecordRowIndex` → row-group lookup unchanged;
     wider point-read fetch on `Scan` profile recorded as the
     intentional access-pattern tradeoff;
   - ADR 055: per-output-slab directory recompute under
     deployment-wide config; cross-replica byte-equality
     preserved.
7. Pin Phase 7 candidate matrix: `{4_096, 8_192, 16_384,
   32_768, 65_536, 131_072}`.
8. Pin Phase 7 schema-shape fixtures.

**Exit gate:** All composability checks pass on paper. Pre-062
surface inventory recorded.

## Phase 1 — Profile enum + config refactor + accessors

Add the enum, refactor `RowGroupWriterConfig`, add accessor
methods, add `PointRead` + `Scan` pinned `const` values.

Files:

- `src/storage/row_group.rs`:

  ```rust
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
  pub enum RowGroupSizingProfile {
      PointRead,
      Scan,
  }

  // PointRead (ADR 056 V1 defaults preserved)
  pub const ROW_GROUP_POINT_READ_TARGET_ROWS: u32 = 4_096;
  pub const ROW_GROUP_POINT_READ_MAX_ROWS: u32 = 8_192;
  pub const ROW_GROUP_POINT_READ_MIN_PAGED_ROWS: u64 = 8_192;
  pub const ROW_GROUP_POINT_READ_MIN_PAGED_BYTES: u64 = 512 * 1024;

  // Scan (council-converged; Phase 7 validates)
  pub const ROW_GROUP_SCAN_TARGET_ROWS: u32 = 65_536;
  pub const ROW_GROUP_SCAN_MAX_ROWS: u32 = 131_072;
  pub const ROW_GROUP_SCAN_MIN_PAGED_ROWS: u64 = 65_536;
  pub const ROW_GROUP_SCAN_MIN_PAGED_BYTES: u64 = 1024 * 1024;

  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  struct RowGroupSizingValues {
      target_row_group_rows: RowGroupRowCount,
      max_row_group_rows: RowGroupRowCount,
      min_paged_record_rows: RowCount,
      min_paged_record_bytes: RecordBodyByteLen,
  }

  pub struct RowGroupWriterConfig {
      pub enabled: bool,
      pub profile: RowGroupSizingProfile,
      sizing: RowGroupSizingValues,
      pub stats_supported_types: BTreeSet<DataType>,
  }

  impl Default for RowGroupWriterConfig {
      fn default() -> Self {
          Self {
              enabled: false,
              profile: RowGroupSizingProfile::PointRead,
              sizing: point_read_values(),
              stats_supported_types: default_stats_supported_types(),
          }
      }
  }

  impl RowGroupWriterConfig {
      pub fn point_read() -> Self { ... }
      pub fn scan() -> Self {
          Self {
              enabled: false,
              profile: RowGroupSizingProfile::Scan,
              sizing: scan_values(),
              stats_supported_types: default_stats_supported_types(),
          }
      }
      pub fn target_row_group_rows(&self) -> RowGroupRowCount {
          self.sizing.target_row_group_rows
      }
      pub fn max_row_group_rows(&self) -> RowGroupRowCount {
          self.sizing.max_row_group_rows
      }
      pub fn min_paged_record_rows(&self) -> RowCount {
          self.sizing.min_paged_record_rows
      }
      pub fn min_paged_record_bytes(&self) -> RecordBodyByteLen {
          self.sizing.min_paged_record_bytes
      }
  }
  ```

Tests (in `tests/row_group_config.rs`):

- `RowGroupWriterConfig::default()`:
  - `enabled = false`;
  - `profile = PointRead`;
  - accessors return ADR 056 V1 defaults exactly.
- `RowGroupWriterConfig::point_read()`:
  - returns
    `{ enabled: false, profile: PointRead, sizing: point_read_values, ... }`
    — byte-identical to `Default::default()`. **Constructors
    default to `enabled = false`**; operators must explicitly
    opt-in via `enabled = true` in config (or via a
    `with_enabled(true)` helper if a runtime API path
    exists). ADR 062 is sizing, not rollout.
- `RowGroupWriterConfig::scan()`:
  - returns
    `{ enabled: false, profile: Scan, sizing: scan_values, ... }`
    with `Scan` accessors returning the pinned `Scan`
    `const` values.
- exhaustive `match` enforced (architectural test).

**Exit gate:** Enum + config refactor + accessors compile;
unit tests pass; pre-062 slab byte output unchanged for
`PointRead` profile.

## Phase 2 — Config parser + escape hatch

Add `profile` field to `RawRowGroupWriterConfig`. Implement
the escape-hatch compromise.

Files:

- `src/config.rs`:

  ```rust
  #[derive(Deserialize, Default)]
  struct RawRowGroupWriterConfig {
      enabled: Option<bool>,
      profile: Option<String>,

      // PointRead-only overrides
      target_row_group_rows: Option<u32>,
      max_row_group_rows: Option<u32>,
      min_paged_record_rows: Option<u64>,
      min_paged_record_bytes: Option<u64>,

      stats_supported_types: Option<Vec<String>>,
  }

  impl RawRowGroupWriterConfig {
      fn into_runtime_config(self) -> std::io::Result<RowGroupWriterConfig> {
          let profile = match self.profile.as_deref() {
              None | Some("point_read") => RowGroupSizingProfile::PointRead,
              Some("scan") => RowGroupSizingProfile::Scan,
              Some(other) => return Err(Error::new(
                  ErrorKind::InvalidInput,
                  format!("unknown row-group sizing profile: {other}"),
              )),
          };

          let mut cfg = match profile {
              RowGroupSizingProfile::PointRead => RowGroupWriterConfig::point_read(),
              RowGroupSizingProfile::Scan => RowGroupWriterConfig::scan(),
          };
          if let Some(enabled) = self.enabled {
              cfg.enabled = enabled;
          }

          let has_numeric_override = self.target_row_group_rows.is_some()
              || self.max_row_group_rows.is_some()
              || self.min_paged_record_rows.is_some()
              || self.min_paged_record_bytes.is_some();

          match profile {
              RowGroupSizingProfile::Scan if has_numeric_override => {
                  tracing::warn!(
                      "row-group numeric overrides ignored under \
                       profile = \"scan\""
                  );
                  // numeric values not applied
              }
              RowGroupSizingProfile::PointRead if has_numeric_override => {
                  cfg.apply_point_read_numeric_overrides(
                      self.target_row_group_rows,
                      self.max_row_group_rows,
                      self.min_paged_record_rows,
                      self.min_paged_record_bytes,
                  )?;
              }
              _ => {}
          }
          Ok(cfg)
      }
  }
  ```

Tests:

- `tests/row_group_config.rs`:
  - parse `profile = "point_read"` → `PointRead`;
  - parse `profile = "scan"` → `Scan`;
  - parse unknown `profile = "xyz"` → reject with clear
    error;
  - parse missing `profile` → defaults to `PointRead`;
  - `profile = "point_read"` + numeric override → `PointRead`
    defaults with override applied;
  - `profile = "scan"` + numeric override → `Scan` pinned
    values, warning emitted, override ignored;
  - `profile = "scan"` without numeric override → no warning.

**Exit gate:** Operator config parses correctly. Existing
operator configs that set numeric fields without `profile`
work unchanged. `Scan` + numeric overrides emits warning;
numeric values ignored.

## Phase 3 — Writer threading

Wire the accessors through the writer's row-group chunking
path.

Files:

- `src/storage/multirow.rs`:
  - audit every read of `config.target_row_group_rows`,
    `config.max_row_group_rows`,
    `config.min_paged_record_rows`,
    `config.min_paged_record_bytes`;
  - replace each with the corresponding accessor call:
    - `config.target_row_group_rows()`
    - `config.max_row_group_rows()`
    - `config.min_paged_record_rows()`
    - `config.min_paged_record_bytes()`;
  - paging-engagement gate `should_page(...)` reads via
    accessors.

Tests:

- existing tests stay green at `PointRead` profile (byte-
  identical output);
- new tests in `tests/multirow.rs`:
  - `Scan`-profile input → output emits row groups at
    65,536-row sizing;
  - `PointRead`-profile input → output byte-identical to
    pre-062;
  - ADR 056 directory shape preserved at `Scan` profile;
  - cross-replica determinism: two writers with same input +
    same profile produce byte-identical output;
  - ADR 057 `EncodingDirectory` per-row-group cascade
    selection unchanged at `Scan` profile;
  - ADR 061 chunk layout + AEAD encryption byte-identical at
    `Scan` profile vs forced-`Scan`-config-override on the
    same input.

**Exit gate:** `PointRead`-profile slab output byte-identical
to pre-062. `Scan`-profile slab output produces 65,536-row
groups for records above target. No directory shape change.

## Phase 4 — One-entry `Scan` directory

Council-converged disposition: `Scan` profile emits one-entry
`RecordPageDirectory` when a record at exactly the target row
count produces 1 row group. `PointRead` profile retains
today's `batches.len() <= 1` single-batch fallback.

Files:

- `src/storage/multirow.rs`:
  - locate the row-group emission gate that returns
    single-batch when `batches.len() <= 1`;
  - branch on `config.profile`:
    - `PointRead`: preserve today's fallback;
    - `Scan`: skip the fallback when paging gate has already
      engaged; emit one-entry `RecordPageDirectory`.

Tests:

- `tests/multirow.rs`:
  - `Scan`, exact-target boundary (65,536 rows × 4 MiB) →
    emits 1 row group with one-entry directory;
  - `Scan`, target-plus-one (65,537 rows × 4 MiB) → emits 2
    row groups (65,536 + 1 — V1 slicing is exact by `target`;
    the 1-row tail group is recorded as actual V1 cost; tail-
    merging deferred to V2);
  - `Scan`, sub-target (32,768 rows × 4 MiB) → fails paging
    gate; single-batch (no directory);
  - `PointRead`, exact-`PointRead`-target boundary (4,096
    rows × 4 MiB) → fails `PointRead` paging gate
    (`min_paged_record_rows = 8_192`); single-batch (no
    directory);
  - `PointRead`, paging gate met but only 1 row group emitted
    (defensive coverage of today's fallback) → preserves
    single-batch.

**Exit gate:** Boundary disposition recorded in code +
explicitly tested. `PointRead` behavior byte-identical to
pre-062 across the boundary fixtures.

## Phase 5 — Reader composability sanity

**No reader code changes.** Reader reads the directory; the
directory carries actual row-group row counts; the reader
materializes accordingly. Verify empirically.

Tests:

- `tests/l2_remote.rs` / `tests/l2_rehydration.rs`: writer
  emits `Scan`-profile slab; reader fetches; output rows
  byte-identical to input.
- `tests/encoding_chunk_layout.rs`: `Scan`-profile slab ×
  every closed ADR 061 chunk size; output byte-identical
  across the cross product.
- `tests/row_group_sparse_projection_fetch.rs`: `Scan`-
  profile slab, clustered ADR 060 sparse fetch on
  representative selectivity (1%, 5%, 25%); selected
  plaintext projection bytes do not regress vs `PointRead`
  profile on the same logical mask.
- `tests/encoding_secondary_index.rs`: `Scan`-profile slab,
  ADR 052 point read; selected bytes < full record body on
  representative `Scan` fixture; latency tradeoff vs
  `PointRead` recorded as the intentional access-pattern
  tradeoff.

**Exit gate:** All four composability tests pass.

## Phase 6 — Architectural lockdown

Land the lockdown guards in `tests/architecture_test.rs`.

Tests:

- `RowGroupSizingProfile` is a closed two-variant enum
  (`PointRead`, `Scan`).
- No `_` catch-all in any `match` over
  `RowGroupSizingProfile`.
- `RowGroupWriterConfig` exposes `profile` (public),
  `enabled` (public), `stats_supported_types` (public);
  `sizing` is private.
- Accessor methods exist for the four sizing values.
- `RawRowGroupWriterConfig` accepts `profile` plus four
  numeric fields as `PointRead`-only overrides.
- `Scan`-profile sizing constants are pinned `const`.
- Writer reads thresholds via accessor methods, not via
  direct field access.
- One-entry `Scan` directory branch present in writer.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
  bit / IPC `custom_metadata` key / digest domain.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode.
- No new `ObjectStore` trait method.
- No new `L2FetchStrategy` / `EncodedRowMask` variant.
- No new fallback / reject / catalog variant.
- ADR 020 §2 AAD layout unchanged.
- ADR 022 §3 / §5 byte layouts unchanged.
- ADR 056 / 057 directory shapes unchanged.
- ADR 061 closed chunk-size set unchanged.
- Cross-replica byte-equal output across the two profiles.
- No Cargo feature gating profile selection.
- No nightly rustc.
- No version suffix on new types.
- No planning-phase identifier on new symbols.
- No project-name prefix.
- `RowGroupSizingProfile` lives in `src/storage/row_group.rs`.

**Exit gate:** All architectural lockdown guards pass.

## Phase 7 — Bench matrix + final `Scan` target validation

Run the full bench at both profiles across the candidate
matrix + schema-shape sweep. Validate the council-converged
`Scan` target (`65_536`). Land the evidence artifact at
`plans/impl/062_row_group_sizing_profile_bench_results.md`.

Bench fixtures:

- **`PointRead`-shape fixtures** for non-regression: pre-062
  fixtures × every closed ADR 061 chunk size; output
  byte-identical (digest verified).
- **Schema-shape sweep at `Scan` profile**:
  - `8,192 × 50 int64`;
  - `16,384 × 50 int64`;
  - `65,536 × 4 int64` (ADR 061 Phase 9 fixture);
  - representative analytical mixed-type schema.
- **Target-rows candidate sweep at `Scan` profile**:
  `{4_096, 8_192, 16_384, 32_768, 65_536, 131_072}` × every
  schema-shape fixture.
- **Scan-scale fixtures**:
  - 16M+ row runtime (matches ADR 061 Phase 9);
  - 100M+ row sampled / modeled.
- **Boundary fixtures**:
  - exact-target (`Scan` one-entry directory disposition);
  - target-plus-one (`Scan` two-entry directory);
  - sub-target above `PointRead` gate;
  - `PointRead` exact-target (single-batch fallback
    preserved).

Bench rows record:

- profile, target rows, row groups per record, row-group
  bytes;
- directory bytes, directory fraction of record body;
- dense GET count, p50 / p95 coalesced GET payload bytes;
- sparse projection ciphertext bytes, sparse projection
  wall time;
- secondary-index point-read selected bytes + latency;
- writer wall time, compression loss, materialization wall
  time;
- output equality digest;
- stats-prune row-group survival rate at representative
  predicates (sorted, clustered, random-uniform).

Acceptance gates:

- **Dense p50 / p95 GET**: `Scan` profile p50 ≥ 1 MiB and
  p95 ≥ 4 MiB on scan-scale fixtures with ADR 061 chunk
  layout.
- **Dense GET count**: `Scan` profile does not regress
  versus ADR 061 contiguous dense baseline.
- **Sparse projection survival**: clustered 1% / 5%
  fixtures retain ≥ 3× selected-ciphertext-byte reduction
  versus 64 KiB chunk baseline (ADR 061 invariant);
  wall-clock regression ≤ 5% versus `PointRead` profile.
- **Stats-prune**: row-group survival rate at `Scan` profile
  within 1.5× the `PointRead` profile on representative
  sorted/clustered predicates. Random-uniform recorded
  separately.
- **Point-read selected bytes**: < full record body on
  largest legal `Scan` fixture. Latency tradeoff vs
  `PointRead` recorded explicitly.
- **Writer cost**: ≤ 25% overhead at `Scan` profile vs
  row-group-disabled writing on the largest legal fixture
  (matches ADR 056 accepted gate).
- **Directory bytes**: ≤ 2% of record body at `Scan`
  profile, within ADR 044's metadata probe cap.
- **Compression loss**: within ADR 056 accepted bounds on
  integer / float / UTF-8 / mixed fixtures.
- **Output equality**: logical Arrow output, projected
  batches, sparse projection output, aggregate input
  batches, recovery, and rehydration all byte-identical at
  both profiles.
- **Cross-replica determinism**: two writers with the same
  profile + input produce byte-identical slab bodies.
- **One-entry directory disposition**: `Scan` boundary
  fixture row records the empirical behavior.

**Final `Scan` target row count selection**: the artifact
documents the chosen value (council-converged leading
candidate `65_536`) and the gate-pass evidence across the
candidate sweep.

**Exit gate:** Every promotion-criterion bench row passes its
gate. Final `Scan` target row count validated. Bench artifact
is the basis for the council's promotion decision.

## Phase 8 — Promotion

Promote ADR 062 from council-converged → shipped.

Files:

- `plans/impl/062_row_group_sizing_profile_bench_results.md` —
  Phase 7 evidence artifact.
- `plans/adr/062_row_group_sizing_profile.md` — Status
  amendment to "shipped" with bench evidence reference.
- `plans/adr/056_row_group_fetch_and_page_directory.md` —
  §"Where we are" amendment referencing ADR 062 profile.
- `plans/wip/roadmap.md` — planning-cadence amendment;
  ADR 062 marked shipped.

Work:

1. Pin the final `Scan` target row count.
2. Amend ADR 056's where-we-are section to distinguish
   `PointRead` and `Scan` profiles.
3. Mark ADR 062 shipped only after code, tests, and bench
   evidence land together.
4. Update the roadmap at the planning-cadence boundary.

**Exit gate:** ADR 062 promoted as an ADR 056 amendment with
no directory substrate change. All ADR 056 / 057 / 058 / 059
/ 060 / 061 focused tests stay green at both profiles.

## Verification gate

Before promotion:

```sh
cargo test --test row_group_config
cargo test --test row_group_writer
cargo test --test row_group_directory
cargo test --test row_group_fetch_plan
cargo test --test row_group_sparse_projection_fetch
cargo test --test encoding_secondary_index
cargo test --test encoding_chunk_layout
cargo test --test crypto_macro_block_coalescing
cargo test --test l2_remote
cargo test --test l2_rehydration
cargo test --test architecture_test
cargo test --test config_loader
cargo bench --bench row_group_fetch -- --quick
cargo bench --bench row_group_fetch
git diff --check
git diff --cached --check
```

## Risks and mitigations

**`Scan` profile erases row-axis selectivity on point
reads.** Mitigation: Phase 7 point-read selected-byte gate.
Latency tradeoff vs `PointRead` is recorded as the intentional
access-pattern tradeoff that the `Scan` profile name
advertises, not hidden.

**One-entry `Scan` directory regresses ADR 056 discipline.**
Mitigation: Phase 4 explicit branch + boundary fixtures.
`PointRead` profile unchanged.

**Dense GET payloads stay small despite larger row groups.**
Mitigation: Phase 7 p50 / p95 gates with ADR 061 chunk layout
enabled.

**Sparse projection regresses at `Scan` profile.**
Mitigation: ADR 060 sparse-projection matrix runs under
`Scan` profile. Sparse correctness is byte-identical; the
gate is wall-clock and selected ciphertext bytes.

**Writer overhead exceeds ADR 056 accepted gate.**
Mitigation: candidate sweep includes writer wall time;
≤ 25% gate.

**Cross-replica byte equality breaks.** Mitigation: profile
is deployment-wide; tested via byte-identical repeated
writes.

**ADR 062 leaks into public protocol surfaces.** Mitigation:
Phase 6 architecture-test guards.

**Existing operator configs break at parse time.**
Mitigation: escape-hatch compromise — `PointRead`-only
numeric overrides preserve non-breaking migration. `Scan` +
numeric overrides emits warning, ignores numeric values.
