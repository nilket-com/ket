# ADR 062: Row-Group Sizing Profile

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/062_row_group_sizing_profile.md`

## Status

Council-converged across Claude / Codex / Gemini after five
council iterations. Implementation phases pending; Phase 7
warehouse-scale bench evidence is the promotion-to-shipped
gate.

Amends ADR 056 (Row-Group Fetch and Layered Page Directory)
with a closed two-variant `RowGroupSizingProfile { PointRead,
Scan }` catalog and deployment-wide profile selection. Does
not change ADR 056's directory shape, fetch planner, reject
taxonomy, secondary-index mapping, LSM compaction discipline,
or descriptor / wire / capability surface.

A standalone ADR rather than an inline ADR 056 amendment
because the profile introduces a new closed variant catalog,
its own bench-driven promotion gate, and its own architectural
lockdown guards.

## Context

ADR 061 (page-aligned authenticated chunks + macro-block read
coalescing) shipped the AEAD/network two-layer decoupling. Its
§"V1 deferred" explicitly punted:

> **Row-group sizing recommendations.** ADR 056 governs row-
> group sizing; ADR 061 surfaces but doesn't mandate that
> warehouse profiles use 1+ MiB compressed row groups. Separate
> ADR 056 amendment if council wants.

ADR 061 §"What not to do" then made the framing precise:

> **Don't treat warehouse throughput as a chunk-size question.**
> It's a coalescer + row-group-sizing question; warehouse-
> ladder AEAD chunks are the wrong layer.

ADR 062 answers that deferred question.

### What ADR 056 ships today

Existing `RowGroupWriterConfig` defaults
(`src/storage/row_group.rs:43-46`):

```text
ROW_GROUP_TARGET_ROWS_DEFAULT          = 4_096
ROW_GROUP_MAX_ROWS_DEFAULT             = 8_192   (= 2 × target)
ROW_GROUP_MIN_PAGED_RECORD_ROWS_DEFAULT = 8_192
ROW_GROUP_MIN_PAGED_RECORD_BYTES_DEFAULT = 512 KiB
```

The `max = 2 × target` slack-headroom convention is already in
ADR 056 — the `Scan` profile preserves it.

These defaults were tuned for the OLTP-shape mix that governed
ADR 056's Phase 10 bench fixture — the access pattern this ADR
calls `PointRead`. They are *not* the right tuning for the
analytics-scan fixtures ADR 061's Phase 9 introduced — 16M+
row runtime, 100M+ row sampled, 65,536 rows per record at
4 MiB plaintext per record.

### Why row-group sizing is the right layer

Three forces converge on row-group sizing as the load-bearing
knob for `Scan` throughput:

**1. ADR 061 already settled the chunk layer.** Small AEAD
chunks underneath, large coalesced GETs above. The remaining
question is whether the row-group layer above the chunk layer
produces contiguous projection ranges large enough to feed the
coalescer multi-MiB GETs.

**2. With ADR 056's 4K-row default, achieving 1+ MiB
compressed row groups requires exceptionally wide schemas.**
Typical analytical schemas (4–50 columns) compress 4K rows to
~50–250 KiB per row group — below ADR 061's p50 ≥ 1 MiB GET
target. ADR 061's Phase 9 worked around this by dense-fetching
the whole record body (~4 MiB), sidestepping the per-row-group
sizing question rather than answering it.

**3. Bigger row groups proportionally shrink the
`RecordPageDirectory` IPC footprint.** ADR 056 Phase 10 already
showed directory size can consume 22–26% of ADR 044's 64 KiB
initial probe. A 16× larger row group means ~1/16th as many
`RowGroupDirectoryEntry` rows; `Scan`-shape records benefit
doubly — smaller directory, faster Phase A metadata fetch +
larger contiguous projection ranges, more macro-block-
coalescer-friendly Phase C dense fetches.

### Why deployment-wide profile beats per-record geometry-keyed selection

The council considered both shapes across iterations and
converged on deployment-wide selection:

- **Operator-declared intent should be honored uniformly.** A
  `Scan` deployment that silently uses `PointRead` sizing on a
  10K-row record because it missed a 65K-row floor is worse
  than a `Scan` deployment that uniformly uses `Scan` sizing.
  Per-record geometry-keyed selection makes scan economics
  conditional on every record passing both floors;
  deployment-wide makes scan economics an access-pattern
  declaration.
- **Cross-replica determinism is cleaner.** Deployment-wide
  selection: every replica uses the same profile by
  construction; slab-byte equality holds trivially. Geometry-
  keyed selection relies on byte-identical geometry computation
  across replicas, which is true today but adds a determinism
  surface.
- **Bench evidence is clearer.** One profile → one shape per
  deployment, no record-by-record variance to explain. A
  2-axis matrix (profile × fixture) rather than a 3-axis
  matrix (profile × geometry × fixture).
- **The operator declaration is exactly what should be
  declared.** `profile = "scan"` says "I want analytical scan
  economics on this deployment"; `profile = "point_read"` says
  "I want point-read economics." The deployment-wide question
  becomes trivial: which access pattern does this deployment
  serve?

Mixed-workload concerns (deployments with both OLTP-shape and
analytics-shape records) are deferrable to V2 with a per-
record selector if real operational evidence appears.

### Why `PointRead` / `Scan` naming

The names describe what the profile optimizes for (access
pattern), not the deployment hardware envelope:

- `PointRead`: small row groups → fine pruning grain →
  minimal bytes per record lookup (ADR 052 secondary-index
  point reads, OLTP-shape access).
- `Scan`: large row groups → MiB-class contiguous projection
  ranges → ADR 061 macro-block coalescer economics (analytical
  scans, bulk projection).

ADR 030 vocabulary (`Commodity / Premium / Hardened`)
describes hardware support envelopes. ADR 062's profile is
about access-pattern shape — orthogonal concern. Reusing ADR
030 names would conflate two distinct layers.

### Why this is an ADR 056 amendment, not a substrate change

ADR 062 changes:

1. The mapping from operator-declared access-pattern profile
   to the numeric sizing values used by the writer.
2. The `RowGroupWriterConfig` operator surface: a profile
   field replaces direct numeric-field exposure (numeric
   fields stay in `RawRowGroupWriterConfig` as
   `PointRead`-only overrides).
3. The bench fixture surface (profile axis).
4. Architectural lockdown guards pinning the closed two-
   variant profile catalog.

ADR 062 does **not** change:

- ADR 056 `RecordPageDirectory` shape.
- ADR 056 three-phase fetch planner.
- ADR 056 `L2FetchStrategy::RowGroup` four-leg dispatch shape.
- ADR 056 reject taxonomy / fallback-reason taxonomy.
- ADR 052 secondary-index mapping or sidecar shape.
- ADR 055 LSM compaction discipline.
- ADR 057 / ADR 058 / ADR 059 / ADR 060 / ADR 061 surfaces.
- Any wire / descriptor / capability / PBC / digest-domain /
  `ObjectStore` / IPC `custom_metadata` / metric-label surface.

### Constraints carry forward unchanged

ADR 003, ADR 016, ADR 019, ADR 020, ADR 022, ADR 029, ADR 030,
ADR 035, ADR 036, ADR 037, ADR 039, ADR 040, ADR 042, ADR 043,
ADR 044, ADR 052, ADR 053, ADR 054, ADR 055, ADR 056, ADR 057,
ADR 058, ADR 059, ADR 060, ADR 061.

## Decision

ADR 062 ships:

1. **Closed two-variant `RowGroupSizingProfile` catalog**:
   `PointRead` (today's tuning preserved byte-identically),
   `Scan` (lifted target / max / paging thresholds; pinned
   `const`).

2. **Deployment-wide selection** via
   `RowGroupWriterConfig.profile`. The profile is operator-
   declared in node-local config, replicated identically
   across deployment replicas. **No per-record selector, no
   geometry-keyed auto-tuning** in V1.

3. **Profile-derived sizing values** stored internally in
   `RowGroupWriterConfig`; accessor methods expose them to the
   writer. Operators select the access-pattern profile string;
   the runtime stores derived numbers.

4. **`PointRead` profile** (today's ADR 056 V1 defaults
   preserved byte-identically):

   ```text
   target_row_group_rows  = 4_096
   max_row_group_rows     = 8_192    (= 2 × target)
   min_paged_record_rows  = 8_192
   min_paged_record_bytes = 512 KiB
   ```

5. **`Scan` profile** (Phase 7 bench validates; council-
   converged values):

   ```text
   target_row_group_rows  = 65_536
   max_row_group_rows     = 131_072  (= 2 × target, slack headroom)
   min_paged_record_rows  = 65_536   (= target)
   min_paged_record_bytes = 1 MiB
   ```

   `max = 2 × target` matches ADR 056's existing convention
   (today's `(target = 4_096, max = 8_192)` defaults are
   already the same 2× shape). V1 row slicing is exact: every
   group is `target` rows except possibly a final tail group
   of remaining rows; `max` is validation/policy headroom and
   does not trigger tail-merging in V1. A 65,537-row `Scan`
   record therefore produces 2 row groups (65,536 + 1), not 1
   absorbed group. Tail-merging is V2 territory (see V1
   deferred).

6. **Operator config exposure** (escape-hatch compromise):

   ```toml
   [storage.row_group_writer]
   enabled = true                # default false
   profile = "scan"              # default "point_read"
   # numeric fields below are PointRead-only overrides;
   # ignored with a config-load warning when profile = "scan"
   target_row_group_rows = 8_192
   max_row_group_rows = 16_384
   min_paged_record_rows = 16_384
   min_paged_record_bytes = 1024
   ```

   Closed parser maps `"point_read"` / `"scan"` to enum
   variants. Unknown profile values reject at config-parse
   time. Numeric fields are accepted but soft-deprecated:
   - `profile = "point_read"` (or omitted): numeric fields
     apply as overrides on the `PointRead` defaults;
   - `profile = "scan"`: numeric fields are ignored with a
     config-load-time warning.

   Non-breaking migration path for existing operator configs.

7. **One-entry `Scan` directory accepted.** When a `Scan`-
   profile record at exactly the target row count produces 1
   row group, the writer emits a one-entry
   `RecordPageDirectory` rather than today's
   `batches.len() <= 1` single-batch fallback. Rationale:
   `Scan` records still benefit from ADR 057 per-buffer pages
   and ADR 060 sparse projection metadata even at one row
   group; bypassing the directory silently would forfeit those
   wins. **`PointRead` profile retains today's single-batch
   fallback unchanged.**

8. **`enabled = false` stays default.** ADR 062 is sizing,
   not rollout; flipping default-on is a separate operational
   ADR.

9. **No new descriptor field**, no new wire `MsgType`, no new
   `FRAME_FLAG_*`, no new `NodeCapability` bit, no new IPC
   `custom_metadata` key, no new digest domain, no new
   `ObjectStore` trait method, no new PBC opcode, no new
   metric label.

10. **No new `RowGroupRejectCode` / `RowGroupFallbackReason` /
    `EncodingRejectCode` / `EncodingFallbackReason` /
    `EncodingComputeRejectCode` /
    `EncodingComputeFallbackReason` / `L2FetchStrategy` /
    `EncodedRowMask` variant.**

11. **No envelope `format_version` bump.** ADR 022 byte
    layout unchanged; ADR 056 directory shape unchanged.

The slab-format byte layout, AAD shape, nonce derivation, key
hierarchy, footer HMAC scope, ADR 022 footer layout, ADR 056
directory shape, ADR 057 cascade catalog, and ADR 061 closed
chunk-size set are all unchanged.

## Architectural inextricability

Three design choices physically inseparable in V1.

**1. Profile is a closed two-variant access-pattern
declaration with profile-derived internal sizing.** Open-
catalog operator-defined profiles violate the closed-catalog
principle. Per-record geometry-keyed selection is V2
territory. A descriptor-policy variant
(`RecordBodyCompressionPolicy::ScanProfile`) makes a physical
deployment knob look like logical schema policy (ADR 057
§"Why node-local writer config" framing).

**2. Operator config exposes profile + `PointRead`-only
overrides.** Exposing four independent numeric integers under
the `Scan` profile creates a tuning grab bag that drifts away
from the bench-pinned profile. `Scan` must be a closed
bench-backed shape; `PointRead` gets the soft-deprecate
escape hatch for non-breaking migration.

**3. `Scan` profile uses ADR 056's existing slack-headroom
convention.** `max = 2 × target` matches today's `PointRead`
default (`8_192 = 2 × 4_096`). Both `max = target` and
`max = 2 × target` produce identical V1 row-group output
because slicing is purely by `target` and `max` is validation
headroom; the 2× choice is forward-compatible with V2 tail-
merging without changing the operator-visible shape, while
preserving consistency with ADR 056's existing pair.

## Identity-vs-Locality framing

ADR 062 is identity-neutral and locality-neutral. Row-group
sizing is per-record physical storage shape, downstream of
admission and orthogonal to row identity, locality projection,
record location, record row index, or visible row set.

Cross-replica determinism is preserved by **deployment-wide
profile selection**: every replica in a deployment uses the
same profile; the writer's row-group sizing is a
deterministic function of (input record, profile-derived
numeric values). Slab-byte equality across replicas holds.

There is no separate "row-group-sizing plane." Profile
selection + sizing application live in the same storage-write
surface as ADR 056 row groups, ADR 057 encoding cascades, and
ADR 061 chunk layout. Unified Ring rule applies.

## V1 scope

ADR 062 ships:

### 1. `RowGroupSizingProfile` enum

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowGroupSizingProfile {
    PointRead,
    Scan,
}
```

Closed catalog, exhaustive `match` enforced by an
architectural-test guard. No `_` catch-all match arm
permitted.

### 2. `RowGroupWriterConfig` shape

```rust
pub struct RowGroupWriterConfig {
    pub enabled: bool,
    pub profile: RowGroupSizingProfile,
    sizing: RowGroupSizingValues,        // private; profile-derived
    pub stats_supported_types: BTreeSet<DataType>,
}
```

`Default::default()` returns
`{ enabled: false, profile: PointRead, sizing: point_read_values, ... }` —
byte-equivalent to today's ADR 056 default behavior.

Accessor methods (`target_row_group_rows()`,
`max_row_group_rows()`, `min_paged_record_rows()`,
`min_paged_record_bytes()`) expose the profile-derived sizing
values to the writer.

### 3. Profile-derived sizing constants

```rust
// PointRead (preserves ADR 056 V1 defaults)
pub const ROW_GROUP_POINT_READ_TARGET_ROWS: u32 = 4_096;
pub const ROW_GROUP_POINT_READ_MAX_ROWS: u32 = 8_192;
pub const ROW_GROUP_POINT_READ_MIN_PAGED_ROWS: u64 = 8_192;
pub const ROW_GROUP_POINT_READ_MIN_PAGED_BYTES: u64 = 512 * 1024;

// Scan (Phase 7 bench validates; council-converged)
pub const ROW_GROUP_SCAN_TARGET_ROWS: u32 = 65_536;
pub const ROW_GROUP_SCAN_MAX_ROWS: u32 = 131_072;
pub const ROW_GROUP_SCAN_MIN_PAGED_ROWS: u64 = 65_536;
pub const ROW_GROUP_SCAN_MIN_PAGED_BYTES: u64 = 1024 * 1024;
```

### 4. Cross-replica determinism

The profile is in deployment-wide node-local config (matching
ADR 056 / 057 / 061's node-local-writer-config pattern).
Every replica writing the same logical record uses the same
profile and applies the same profile-derived numeric values.
Slab-byte equality holds.

ADR 055 LSM compaction recompute discipline carries forward:
each output slab's writer config snapshot includes the
row-group writer config; profile + sizing are deterministic
given the snapshot; cross-replica byte-equality survives
compaction.

### 5. Composability with ADR 060 / ADR 061 / ADR 044 / ADR 052 / ADR 055

**ADR 060 sparse projection fetch**: bigger row groups → more
pages per group, but ADR 060's per-row-group decision grain +
per-buffer / per-row-group byte-fraction gate scales with
row-group size. Sparse fetch composes unchanged.

**ADR 061 page-aligned auth chunks**: orthogonal layer. A
65,536-row `Scan`-profile row group spanning many ADR 061
chunks coalesces to one multi-MiB GET via ADR 044's
`TieredMultiRangeReadPlan`, satisfying ADR 061's p50 ≥ 1 MiB
target.

**ADR 044 column-fetch**: per-record fraction gate at 67%
with buffer-end tail ≤ 1 chunk. Bigger row groups don't
change the per-record fraction calculation.

**ADR 052 secondary indexes**: `RecordRowIndex` → row group
via directory lookup. Wider point-read fetch on `Scan`
profile (65K-row group decompresses to extract one record) —
this is exactly the access-pattern tradeoff the `Scan` name
advertises. Phase 7 gate: point reads must still fetch <
full record body on the largest legal `Scan` fixture.

**ADR 055 LSM compaction**: per-output-slab directory
recompute under deployment-wide config; profile is
deterministic across compaction boundaries.

### 6. One-entry `Scan` directory

When a `Scan`-profile record at exactly
`target_row_group_rows = 65_536` rows produces 1 row group,
the writer emits a one-entry `RecordPageDirectory`. Same
`RowGroupDirectoryEntry` shape; no directory schema change.

`PointRead` profile retains today's single-batch fallback
when `batches.len() <= 1`. (`PointRead`
`min_paged_record_rows = 8_192` and `target = 4_096` make
this a non-issue in practice — sub-8K-row records are
pre-paging-gate.)

The boundary where this matters at `Scan` profile:
- 65,535-row record: below `min_paged_record_rows`, stays
  single-batch (no directory).
- 65,536-row record: at the gate, paged with one row group +
  one-entry directory.
- 131,072-row record: paged with two row groups.

### 7. Bench fixture mandate

The bench artifact must include:

- **`PointRead`-shape fixtures** for non-regression: pre-062
  fixtures × every closed ADR 061 chunk size; output
  byte-identical (digest verified).
- **`Scan`-shape fixtures**:
  - ADR 061 Phase 9 fixture (65,536 rows × 4 MiB);
  - 16M+ row runtime;
  - 100M+ row sampled.
- **Target-rows candidate sweep at `Scan` profile**:
  `{4_096, 8_192, 16_384, 32_768, 65_536, 131_072}` — final
  `Scan` target value bench-validated.
- **Schema-shape sweep**:
  - `8,192 × 50 int64`;
  - `16,384 × 50 int64`;
  - `65,536 × 4 int64` (ADR 061 Phase 9);
  - representative mixed-type analytical schema.
- **Boundary fixtures**: exact-target-rows (one-entry `Scan`
  directory disposition), target-plus-one (two-entry `Scan`
  directory).
- **p50 / p95 coalesced GET payload size**: `Scan` profile
  per-row-group dense fetch p50 ≥ 1 MiB and p95 ≥ 4 MiB on
  scan-scale fixtures.
- **Stats-prune effectiveness**: row-group survival rate at
  `Scan` profile within 1.5× the `PointRead` profile on
  representative sorted/clustered predicates. Random-uniform
  recorded separately (intrinsically weak).
- **ADR 060 sparse projection fetch** at `Scan` profile:
  selected plaintext projection bytes do not regress vs
  `PointRead` profile; wall-clock regression ≤ 5%.
- **ADR 052 secondary-index point read** at `Scan` profile:
  selected bytes < full record body on largest legal `Scan`
  fixture; latency tradeoff vs `PointRead` recorded
  explicitly.
- **Writer cost** at `Scan` profile: writer wall time within
  ≤ 25% of row-group-disabled writing on the largest legal
  fixture (matches ADR 056's accepted gate).
- **Directory size** at `Scan` profile: directory bytes
  ≤ 2% of record body, within ADR 044's metadata probe cap.
- **Compression loss** at `Scan` profile: within ADR 056's
  accepted bounds.
- **Cross-replica determinism row** verifying byte-identical
  output across two writers running the same profile + input.

### 8. Reject-taxonomy discipline

Closed-catalog discipline applies recursively. ADR 062 adds
**zero** new variants to:

- `EncodingRejectCode`, `EncodingFallbackReason`,
  `RowGroupFallbackReason`, `RowGroupRejectCode`,
  `EncodingComputeRejectCode`,
  `EncodingComputeFallbackReason`, `EncodedRowMask`,
  `L2FetchStrategy`.

Bad operator config values (unknown profile name) reject at
config-parse time via `RawRowGroupWriterConfig` validation.
Numeric fields with `profile = "scan"` emit a config-load
warning and are ignored.

### 9. Architectural lockdown

Incremental guards in `tests/architecture_test.rs`:

- `RowGroupSizingProfile` is a closed two-variant enum
  (`PointRead`, `Scan`). No `_` catch-all in any `match` over
  `RowGroupSizingProfile`.
- `RowGroupWriterConfig` exposes `profile` (public),
  `enabled` (public), `stats_supported_types` (public);
  `sizing` is private.
- Accessor methods exist for the four sizing values.
- `RawRowGroupWriterConfig` accepts `profile` plus four
  numeric fields as `PointRead`-only overrides; `Scan` +
  numeric fields combo emits warning + uses `Scan` values.
- `Scan`-profile sizing constants are pinned `const`.
- Writer call sites read target / max / paging values via
  `config.<accessor>()` methods, not via direct field access.
- One-entry `Scan` directory branch present in writer.
- No new wire `MsgType` / `FRAME_FLAG_*` / `NodeCapability`
  bit / IPC `custom_metadata` key / digest domain.
- No new descriptor field / fingerprint surface change.
- No new public PBC opcode addition.
- No new `ObjectStore` trait method.
- No new `L2FetchStrategy` / `EncodedRowMask` / catalog
  variant.
- ADR 020 §2 AAD layout unchanged (48 B).
- ADR 022 §3 / §5 byte layouts unchanged.
- ADR 056 / 057 directory shapes unchanged.
- ADR 061 closed chunk-size set unchanged.
- Cross-replica byte-equal output across the two profiles
  for the same logical record geometry.
- No Cargo feature gating profile selection.
- No nightly rustc.
- No version suffix on new types.
- No planning-phase identifier on new symbols.
- No project-name prefix.
- `RowGroupSizingProfile` lives in `src/storage/row_group.rs`.

## V1 deferred (with explicit V2 promotion paths)

ADR 062 deliberately does **not** ship:

- **A third profile between `PointRead` and `Scan`.** **V2
  promotion path:** if post-V1 evidence shows a measurable
  wall-clock or GET-economy gap on the geometry range between
  the two profiles, V2 introduces a third variant with a
  bench-defended access-pattern label.
- **Per-record geometry-keyed selector.** **V2 promotion
  path:** if operational evidence shows mixed-shape
  deployments where deployment-wide profile selection
  systematically misses a workload class, V2 introduces a
  per-record selector with its own determinism story.
- **Operator-tunable `Scan`-profile sizing values.** Pinned
  `const` after Phase 7 bench. **V2 promotion path:** if
  deployment-shape geometry materially differs from
  bench-pinned values.
- **Operator overrides for the `Scan` profile.** Strict for
  `Scan` in V1; the `PointRead`-only escape hatch is a
  non-breaking-migration affordance, not a tuning surface.
- **Per-row-group profile within a record.** Per-record
  uniform in V1.
- **Profile-aware ADR 060 sparse / dense routing changes.**
  ADR 060 V1 routing unchanged.
- **Tail-absorption row-slicing algorithm change** beyond
  `2 × target` slack-headroom.
- **Descriptor-policy activation
  (`RecordBodyCompressionPolicy::ScanProfile` etc.).** Same
  ADR 057 §"Why node-local writer config" reasoning. **No V2
  path** — physical writer policy, not logical schema policy.
- **Migration sweeper for existing slabs.** ADR 055 LSM
  compaction picks up the new profile on natural recompacts.
- **Profile-keyed metrics / labels.** Bench evidence at
  `cargo bench` time is sufficient.
- **Flipping `enabled = true` default.** Separate operational
  ADR.

## Council convergence path

Five iterations across Claude / Codex / Gemini converged on
the design above. Two questions drove the most council
oscillation:

**Selection grain (deployment-wide vs per-record geometry-
keyed)**: oscillated across iterations 1–3 with each council
member voting both ways. Iter-3 surfaced the strongest
arguments for deployment-wide selection: operator-declared
intent honored uniformly, cleaner cross-replica determinism
story, 2-axis bench matrix vs 3-axis, mixed-workload concerns
deferrable to V2. Iter-4 locked deployment-wide. The
geometry-keyed alternative remains in §"Rejected alternatives"
for V2 reference if mixed-workload evidence appears.

**Profile naming**: iterations 1–4 ran through several
candidate pairings (`Default / Warehouse`, `Standard /
Warehouse`, `Commodity / Warehouse`). User input in iter-5
settled on `PointRead / Scan` — names that describe what the
profile optimizes for (access pattern) rather than the
deployment hardware envelope (which is ADR 030 territory).

Iter-5 also locked the remaining sizing decisions: warehouse
target `65_536` (council unanimity), max `131_072` matching
ADR 056's existing `max = 2 × target` slack-headroom
convention, `min_paged_record_rows = target` to enable
one-entry `Scan` directories, `min_paged_record_bytes = 1 MiB`,
escape-hatch compromise for non-breaking operator-config
migration, `enabled = false` default carried forward from
ADR 056.

## Rejected alternatives

- **Per-record geometry-keyed selector.** Strongest arguments
  preserved for V2 reference: mixed-deployment workloads
  handled cleanly; no operator decision required for sub-
  `Scan`-floor records on `Scan` nodes. Outweighed by
  operator-intent honoring and cross-replica determinism
  cleanliness in V1.
- **Open-catalog operator-defined profiles.** Closed-catalog
  discipline.
- **Three-profile catalog in V1.** No bench evidence yet
  distinguishes a third workload.
- **Direct operator access to numeric fields under the `Scan`
  profile.** Tuning grab bag for `Scan` claims.
- **Per-table descriptor-policy variant.** ADR 057 §"Why
  node-local writer config" framing.
- **Selector keyed off table-name / schema pattern.**
  Conflates physical with logical.
- **Selector keyed off rolling traffic statistics.** Non-
  deterministic.
- **Lifting `min_paged_record_*` thresholds globally** in V1.
  Profile-derived; `PointRead` profile preserves today's
  thresholds byte-identically.
- **A new `L2FetchStrategy::ScanRowGroup` variant.** Profile
  is internal writer state, not a fetch-strategy shape.
- **Pre-pinning `Scan` target before Phase 7 bench.** Phase 7
  validates; council-converged leading candidate `65_536`.
- **`max_row_group_rows = target_row_group_rows`.** ADR 056's
  existing convention pairs `(target, 2 × target)`; `Scan`
  profile preserves it for shape consistency. Both
  `max = target` and `max = 2 × target` produce identical V1
  row-group output (slicing is by target; max is validation
  headroom). The 2× choice is forward-compatible with V2
  tail-merging without changing the operator-visible shape.
- **`min_paged_record_rows = 2 × target_row_group_rows`** for
  `Scan` profile. Would forbid one-row-group `Scan` records
  from gaining ADR 057 / 060 metadata.
- **Strict mutual-exclusion config exposure.** Would break
  existing operator configs at parse time. The escape-hatch
  compromise (`PointRead`-only overrides) preserves
  non-breaking migration.
- **Forbidding one-entry `Scan` directories.** Forfeits ADR
  057 / 060 leverage on one-row-group `Scan` records.
- **Naming: ADR 030 vocabulary
  (`Commodity / Premium / Hardened`)** for ADR 062 variants.
  Conflates ADR 062's access-pattern shape with ADR 030's
  hardware support envelope.

## What not to do

- **Don't promote `RowGroupSizingProfile` to a per-record
  selector in V1.**
- **Don't add a third variant without bench-driven V2
  evidence.**
- **Don't expose direct numeric overrides for the `Scan`
  profile.**
- **Don't change ADR 056 directory shape.**
- **Don't change ADR 022 footer layout.**
- **Don't add a new `RowGroupRejectCode` /
  `RowGroupFallbackReason` / `EncodingRejectCode` /
  `EncodingFallbackReason` variant.**
- **Don't add a new `L2FetchStrategy` variant.**
- **Don't add a new descriptor field.**
- **Don't add a new wire frame, capability bit, IPC metadata
  key, or digest domain.**
- **Don't bundle ADR 062 with page-level min/max stats / RLE
  value-run / FSST evaluator / sparse predicate-column
  fetch.**
- **Don't conflate profile selection with the paging-
  engagement gate.** Paging engagement is the upstream
  question; profile selects the threshold values.
- **Don't reach for a per-row-group profile.**
- **Don't conflate row-group sizing with chunk size.** ADR
  061 sets per-slab chunk size; ADR 062 sets per-record
  row-group sizing.
- **Don't introduce planning-phase identifiers
  (`Iter1Profile`, `WipProfile`, etc.).**
- **Don't introduce version suffixes
  (`RowGroupSizingProfileV1`, etc.).**
- **Don't treat the `Scan` profile as a license to also lift
  ADR 061 chunk size or ADR 060 fragmentation caps.**
- **Don't leave the `Scan` target as a recommended range.**
  The final ADR pins exactly one `Scan` target.
- **Don't change the `max = 2 × target` slack-headroom
  convention without an explicit ADR.**
- **Don't break existing operator configs at parse time.**
  `PointRead`-only escape hatch preserves migration path.
- **Don't reuse ADR 030 vocabulary
  (`Commodity / Premium / Hardened`) for ADR 062 profile
  variants.** ADR 030 is hardware support envelope; ADR 062
  is access-pattern shape. Different layers.

## Convergence summary

```text
ADR 056 row-group fetch + page directory  (analytics-scale row groups)
  → ADR 057 per-buffer pages + cascades   (CPU-vector pages)
  → ADR 058 substrate COCD                (compute over encoded)
  → ADR 059 native COCD kernels           (encoded compute fast)
  → ADR 060 sparse projection fetch       (sub-row-group page selection)
  → ADR 061 page-aligned AEAD chunks      (sub-record ciphertext savings
    + macro-block coalescing                via small AEAD chunks)
  → ADR 062 row-group sizing profile      (deployment-wide access-pattern
    PointRead/Scan (this ADR)               profile; PointRead preserves
                                            ADR 056 V1; Scan emits 65K-row
                                            groups for ADR 061 macro-
                                            block coalescing; ADR 056
                                            amendment, no directory
                                            shape change)  ← here
  → next Toron-adjacent follow-up
    (page-level min/max stats; predicate-fetch chunk narrowing;
     RLE value-run format; sparse predicate-column fetch;
     dense predicate-column zip with sparse projection;
     nullable sparse projection; Fsst evaluator;
     ADR 061 V2 — variable buffer-aligned chunks via encrypted
       directory, gated on real ADR 044 over-fetch evidence)
  → capacity-tier compression / serverless / EC  (rest of #19 bundle)
```

## Promotion criteria

ADR 062 promoted from council-converged → shipped when:

1. `RowGroupSizingProfile` enum + accessor methods + profile
   constants land in `src/storage/row_group.rs`.
2. `RowGroupWriterConfig` refactored to expose `profile` plus
   accessors; `sizing` stored privately.
3. `RawRowGroupWriterConfig` parses `profile = "point_read" |
   "scan"`; numeric fields accepted as `PointRead`-only
   overrides with warning when `Scan` + numeric overrides
   combine.
4. Writer call sites at `src/storage/multirow.rs` consult the
   accessors.
5. **One-entry `Scan` directory disposition recorded** (V1:
   `Scan` profile emits one-entry directory; `PointRead`
   profile retains today's `batches.len() <= 1` fallback).
6. Bench artifact at
   `plans/impl/062_row_group_sizing_profile_bench_results.md`
   demonstrates:
   - p50 ≥ 1 MiB and p95 ≥ 4 MiB `Scan`-profile per-row-group
     dense GET on scan-scale fixtures;
   - `PointRead`-profile non-regression: byte-identical
     output to pre-062 slabs;
   - target-rows candidate sweep
     `{4_096, 8_192, 16_384, 32_768, 65_536, 131_072}` records
     every fixture × candidate cross-product;
   - stats-prune row-group survival rate at `Scan` profile
     within 1.5× `PointRead` profile on representative
     sorted/clustered predicates;
   - ADR 060 sparse projection fetch non-regression at `Scan`
     profile;
   - ADR 052 secondary-index point-read selected bytes <
     full record body on largest legal `Scan` fixture;
   - writer-cost wall-time ≤ 25% overhead at `Scan` profile;
   - directory bytes ≤ 2% of record body at `Scan` profile;
   - compression loss within ADR 056 accepted bounds at
     `Scan` profile;
   - cross-replica byte-equal output across the two profiles
     for the same logical input;
   - boundary-fixture row at exact `Scan` target verifying
     one-entry directory disposition empirically.
7. **Final `Scan` target row count pinned** — council-
   converged leading candidate `65_536`; bench validates.
8. ADR 020 / 022 / 044 / 052 / 055 / 056 / 057 / 058 / 059 /
   060 / 061 focused tests stay green at both profiles.
9. Architectural lockdown guards land per §"Architectural
   lockdown".
10. ADR 056 §"Where we are" text updated to reference ADR 062
    profile catalog + `Scan` defaults.
