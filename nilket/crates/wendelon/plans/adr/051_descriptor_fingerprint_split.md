# ADR 051: Logical / Physical Descriptor Fingerprint Split

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/051_descriptor_fingerprint_split.md`

## Status

Accepted

## Context

`TableSchemaDescriptorV1` (ADR 037) carries a single 32-byte
`schema_fingerprint` computed over the postcard-serialized
descriptor with the fingerprint slot zeroed (`src/schema/table.rs:866-881`,
helper at `src/schema/table.rs:962`). The digest has no domain
tag and conflates several concerns:

- descriptor self-consistency during encode / decode;
- aggregate request and response cross-binding;
- column-stats and CSB cross-binding;
- cache invalidation for descriptor-derived execution state;
- proof that row-identity, user schema, locality, layout,
  compression, and stats policy all match one exact
  descriptor image.

That conservative shape was load-bearing while ADRs 042–050
were settling. It is now the wrong abstraction for the next
roadmap segment.

Two distinct concerns are entangled in the single fingerprint:

- **Logical schema identity** — what a row of this table
  *is* semantically: identification (`table_object_id`,
  namespace, table name), the Arrow schema (column set,
  types, nullability), `RowIdentityPolicy` (defines
  `__row_key_id__` derivation), `SchemaEvolutionPolicyV1`
  (governs admissible mutations).
- **Physical descriptor shape** — how rows of this table
  are *stored*: `LocalityPolicyV1` (partition / routing),
  `RecordLayoutPolicy` (SingleRow vs MultiRow packing),
  `RecordBodyCompressionPolicy` (codec choice),
  `ColumnStatsPolicyV1` (bloom / HLL / small-set
  coverage).

Three pressures motivate splitting them now:

- **#8 secondary indexes** will bind index entries to the
  logical row shape. A physical retune (compression off →
  on) should not invalidate index segments.
- **#10 CDC** will bind cursors to the logical event
  stream. A layout policy bump should not invalidate
  consumer cursors.
- **#11 LSM compaction** is the only access-pattern ADR
  that legitimately wants physical-shape gating —
  segments with different layout / compression cannot be
  merged without re-encoding. **And** the LSM compactor
  must not merge segments with different logical schemas,
  even when physical-only fields happen to match (additive
  evolution can leave physical-only fields unchanged while
  logical fingerprint differs). Stacking the physical
  preimage on top of the logical fingerprint enforces this
  safety property by construction.

Today's consumers (verified by survey) and their post-split
routing:

| Consumer | Site | Today | ADR 051 |
|---|---|---|---|
| Aggregate idempotency cache | `src/storage/aggregate.rs:1367` | `schema_fingerprint` | **Logical** |
| Aggregate response wire | `src/storage/aggregate.rs:135-136, 276-277` | `schema_fingerprint` | **Logical** |
| CSB compaction eligibility | `src/storage/stats.rs:1959-1963`, reject at 391 | `schema_fingerprint` | **Logical** + payload coverage |
| CSB header on-disk fingerprint slot | `src/storage/stats.rs:71-96` | `schema_fingerprint` | **Logical** |
| Columnar directory validation | `src/storage/columnar.rs:381, 1035, 2443` | `schema_fingerprint` | **Logical** |
| Write admission stale-schema gate | `src/schema/table.rs:590-597` | `schema_generation` | Unchanged |
| Replica mutation gate | `src/consensus/raft_storage.rs:3606-3609` | `schema_generation` | Unchanged |

Routing principle: every existing fingerprint comparison
site cares about logical row meaning. Physical retunes
change byte encoding, not the facts being expressed.
Routing those sites to physical would over-conservatively
block valid CSB merges and columnar reuses across
compression / layout retunes. The physical fingerprint
exists in V1 for in-memory planning caches and as the
imminent dependency of LSM (#11).

The constraints from `no-premature-backward-compat.md`,
`newtype-units.md`, `no-version-suffixes.md`,
`no-project-name-in-protocols.md`, and ADR 046 §7 all
hold.

The roadmap places this at #7, immediately after PK delete
(ADR 050), before secondary indexes (#8), CDC (#10), and
LSM (#11). Each inherits a settled fingerprint shape from
this ADR.

## Non-goals (V1)

- **No new `RowIdentityPolicy` variant.** Identity policy
  is a logical-fingerprint input; this ADR routes inputs.
- **No new non-fingerprint descriptor fields.** The
  fingerprint slot is restructured (one `[u8; 32]`
  field becomes two newtyped fields); all 12 non-
  fingerprint fields keep their shapes.
- **No new on-disk shape.** The CSB header's 32-byte
  fingerprint slot is byte-layout-identical; only its
  preimage interpretation changes. No `format_version`
  bump.
- **No new wire frame family** and no envelope
  `format_version` bump.
- **No new HRFT frame family.**
- **No new node capability bit.**
- **No `ObjectStore` trait change.**
- **No removal of `schema_generation`.** Generation orders
  mutations; fingerprints disambiguate which axis changed.
- **No `physical_generation` catalog split.** Catalog
  sequencing remains uni-dimensional; descriptor identity
  is bi-dimensional via fingerprints.
- **No per-node physical fingerprint variant.** V1 cluster
  shape is uniform across replicas; per-node compression
  / encoding is a future-ADR concern.
- **No per-column fingerprint.** Column-aware locality and
  per-column projection (deferred per ADR 049) reuse the
  descriptor-level physical fingerprint when they ship.
- **No `StateDigest` durable placement on descriptor
  fingerprints.** Descriptor fingerprints are plain
  BLAKE3 identity hashes, not Pedersen commitments. ADR
  046's substrate is for visible-row state.
- **No `digest.table.row.physical.v1` allocation.** Row-
  set physical comparison is the consumer ADR 050 §9
  reserved for a future ADR; ADR 051 does not allocate it.
- **No reserved enum slots, compatibility shims, or `V1`
  / `V2` Rust type suffixes for any new ADR 051 type.**
- **No SQL parser work.**
- **No new reject-code family.** Existing
  `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` is renamed (§7);
  no new ones added.
- **No rename of `TableSchemaDescriptorV1` →
  `TableSchemaDescriptor`.** Separate convention-
  compliance pass with broader blast radius; deferred.
- **ADR 048 lifecycle policy state stays outside both
  fingerprints and outside the descriptor.** Lifecycle
  lives in the replicated `TableLifecycleCatalog`.
- **Schema-evolution rules are unchanged.** ADR 051
  changes fingerprint accounting and cache invalidation
  semantics, not what evolutions are legal.
- **No `tenant_id` in either preimage.** Catalog /
  request cross-bind, not a descriptor field.
- **No `contract_version` in either preimage.** Descriptor-
  format metadata; a future incompatible bump gets its
  own domain tag.
- **No CSB eligibility check that rejects solely on
  descriptor physical-policy difference.** When logical
  matches, CSB usability depends on parsed payload
  coverage (§7).

## Decision

### 1. Two fingerprint newtypes

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
// Same shape for PhysicalDescriptorFingerprint.
```

Both are opaque 32-byte BLAKE3 outputs. `from_bytes` is
`pub(crate)` so only preimage helpers and crate-internal
deserialization paths mint values; `as_bytes` is `pub` for
read access. `Debug` impls hex-encode for logs and audit
events. No `Pod` / `Zeroable` — constructor discipline
over transmutation convenience.

The asymmetric type names reflect the conceptual asymmetry:
logical fingerprint is *of the schema*; physical
fingerprint is *of the descriptor* (the schema plus its
physical shape).

Per `newtype-units.md`, raw `[u8; 32]` no longer crosses
module boundaries for descriptor-fingerprint purposes; the
existing `schema_fingerprint: [u8; 32]` field is removed
(§4).

### 2. Domain-tagged preimages

Logical fingerprint preimage:

```text
"digest.table.schema.logical.v1"
  || table_object_id             (16 bytes)
  || namespace                   (length-prefixed UTF-8)
  || table_name                  (length-prefixed UTF-8)
  || arrow_schema_ipc            (length-prefixed)
  || row_identity_policy         (canonical postcard, length-prefixed)
  || evolution_policy            (canonical postcard, length-prefixed)
```

Physical fingerprint preimage (stacked — see §3):

```text
"digest.table.descriptor.physical.v1"
  || logical_schema_fingerprint  (32 bytes)
  || locality_policy             (canonical postcard, length-prefixed)
  || record_layout_policy        (canonical postcard, length-prefixed)
  || record_body_compression_policy (canonical postcard, length-prefixed)
  || stats_policy                (canonical postcard, length-prefixed)
```

Both helpers length-prefix every variable-length component
with a fixed-width `u64` little-endian byte count before
feeding it to BLAKE3. The 64-bit width intentionally keeps
Arrow IPC and postcard component sizes unconstrained by the
fingerprint format. Canonical postcard bytes behind the
length prefix make golden vectors stable and cross-language
reproducible.

Excluded from both preimages:

- `schema_generation`: temporal ordering, not row meaning.
  A no-op generation bump produces the same fingerprints.
- `tenant_id`: catalog / request cross-bind, not a
  descriptor field. The catalog tuple `(tenant_id,
  table_object_id, schema_generation)` is the instance
  identity at every wire surface.
- `contract_version`: descriptor-format metadata, not
  table shape. A future incompatible bump gets its own
  domain tag (e.g., `digest.table.schema.logical.v2`)
  rather than depending on `contract_version` inside the
  V1 preimage.
- Lifecycle policy state: lives outside the descriptor
  per ADR 048.

`(table_object_id, namespace, table_name)` are in the
logical preimage so that descriptor self-validation
catches corruption that swaps table identity fields
between descriptors. The logical fingerprint is table-
bound, not just structurally equivalent across tenants.
Cross-table structural cache sharing has no V1 consumer
and can ship as a separate "structural-shape" fingerprint
in a future ADR if a consumer emerges.

### 3. Stacked preimages

Physical preimage embeds the logical fingerprint as its
first data byte-string:

```text
logical_schema_fingerprint  =  H_logical(table_id || ns || name
                                        || arrow || identity || evolution)
physical_descriptor_fingerprint
                           =  H_physical(logical_schema_fingerprint
                                        || locality || layout
                                        || compression || stats)
```

Consequences:

- **Logical changed ⇒ physical changed automatically.**
- **Physical changed ⇏ logical changed.**
- **Physical fingerprint match is a strict refinement of
  logical fingerprint match.**

The load-bearing case for stacking is **roadmap #11 LSM
compaction**: two segments may exist at different
`schema_generation` values with identical physical-only
fields but different Arrow schemas (additive evolution).
Orthogonal preimages would pass a physical-only check and
allow the merge; stacking blocks it by construction.

Stacking also gives the descriptor's `validate()` path a
useful safety property: hand-tampering the logical
fingerprint without recomputing physical fails validation
via either logical OR physical mismatch (the stale
logical no longer feeds the recomputed physical).

### 4. Descriptor field replacement

The descriptor's single fingerprint field is replaced by
two:

```rust
// Before:
pub struct TableSchemaDescriptorV1 {
    // ...12 fields...
    pub schema_fingerprint: [u8; 32],
}

// After:
pub struct TableSchemaDescriptorV1 {
    // ...12 unchanged fields...
    pub logical_schema_fingerprint:    LogicalSchemaFingerprint,
    pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
}
```

`compute_schema_fingerprint` is replaced by:

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

`refresh_descriptor_fingerprints` computes logical first,
then physical with the freshly computed logical embedded.
`validate` recomputes both and strict-rejects on mismatch
— same shape ADRs 049 and 050 use.

### 5. `schema_generation` is unchanged

`schema_generation: u64` continues to bump on any
descriptor mutation, logical or physical. Generation is
the temporal / ordering primitive; fingerprints are the
structural primitives.

`TableSchemaCatalogV1` remains keyed by `(tenant_id,
table_object_id, schema_generation)`. The write-admission
`expected_generation` gate (`src/schema/table.rs:590-597`)
and the replica mutation gate
(`src/consensus/raft_storage.rs:3606-3609`) keep using
generation, not fingerprint.

A physical-only descriptor change advances
`schema_generation` while leaving
`logical_schema_fingerprint` unchanged — that is the
operational signal the split makes legible.

### 6. Consumer routing

V1 logical-fingerprint consumers:

- **Aggregate request idempotency cache key.**
  `src/storage/aggregate.rs:1367` — cache hit survives
  compression / layout retune. V1 cache key:
  `(tenant_id, table_object_id, schema_generation,
  logical_schema_fingerprint)`.
- **Aggregate response wire envelope.**
  `src/storage/aggregate.rs:135-136, 276-277`.
- **CSB compaction eligibility.**
  `src/storage/stats.rs:1959-1963` — descriptor-level
  logical match gates eligibility; payload-coverage
  validation governs which operations succeed (§7).
- **CSB header on-disk fingerprint slot.**
  `src/storage/stats.rs:71-96` — slot stores the logical
  fingerprint.
- **Columnar directory validation.**
  `src/storage/columnar.rs:381, 1035, 2443` — directory
  carries write-time physical details self-describingly;
  validation needs the column-decode contract (logical).
- **(Future) Secondary index entry binding (#8).**
- **(Future) CDC cursor binding (#10).**
- **(Future) Anti-entropy on logical row state.**

V1 physical-fingerprint consumers (in-memory planning
caches whose output depends on physical shape):

- Layout packer selection.
- Compression planner.
- Stats-builder planner.
- Scan-range planner under `locality_policy`.

Physical fingerprint primary future consumer:

- **(Future) LSM segment merge eligibility (#11).**
  Stacking ensures cross-logical merges are blocked by
  construction.

Generation consumers (no change): write admission stale-
schema gate, replica mutation gate, client-side query /
delete request validation (`src/client/core.rs:1366,
1735`).

Aggregates do not gain a physical fingerprint on the wire.
Table query and write request metadata do not gain a
fingerprint at all in this ADR.

### 7. CSB payload-coverage validation

Routing CSB to logical fingerprint is correct in V1
*because* CSB merge and direct-answer paths validate
parsed payload coverage at consumption time, not
descriptor-wide physical equality.

The descriptor-level logical fingerprint is the
**eligibility gate**: a CSB whose stamped logical
fingerprint matches the requested descriptor is
structurally compatible (column ordinals, types,
nullability agree). The **payload-coverage check** runs
after the eligibility gate:

- **Column ordinal** — does this CSB carry stats for the
  column the operation targets?
- **Arrow stats type tag** — does the carried stat type
  match what the operation needs (count / min / max /
  bloom / HLL / small-set)?
- **`stat_flags`** — required flags (e.g., min-present,
  max-present) match.
- **Optional payload availability** — for Bloom, HLL,
  small-set, min, max: is the payload actually present
  in this CSB?

A `stats_policy` retune changes the descriptor's
*physical* fingerprint without touching its logical
fingerprint. CSBs written under the old policy retain
their old optional coverage; CSBs written under the new
policy get the new coverage. Both are descriptor-level
eligible; payload coverage decides per-operation
success:

- **Always-on facts (count, basic min/max, nullability)**
  remain usable across stats-policy retunes.
- **Optional facts (bloom, HLL, small-set)** are usable
  only when the parsed CSB carries the payload. A CSB
  written before bloom-on-column-Y was added falls
  through to row-level for bloom-on-Y queries; a CSB
  written after satisfies them.

This makes logical routing correct, not just cleaner. A
descriptor-wide physical-fingerprint check would have
been over-conservative — rejecting CSBs whose merge or
direct-answer was actually valid (e.g., compression
retune doesn't affect any stat).

The reject path for descriptor-level logical mismatch is
unchanged: fall through to row-level.

### 8. CSB header on-disk slot reinterpretation

The CSB header's 32-byte fingerprint slot
(`src/storage/stats.rs:71-96`) keeps the same byte-layout
slot but now stores the **logical** fingerprint computed
under the new preimage. The header size and CSB frame
family do not change.

Slabs written by code prior to ADR 051 contain bytes
under that slot that *will not* match the logical
fingerprint recomputed from any current descriptor —
different domain tag, different preimage subset. The
compatibility consequence is a one-time cliff: pre-ADR-
051 CSBs fail the logical-fingerprint eligibility check
and fall through to row-level (the existing safe-
fallback path the mismatch reject already drives). They
are not corrupt; they are simply permanently ineligible
for direct CSB merge with post-ADR-051 CSBs. A
subsequent compaction rewrites them with the new
fingerprint.

Pre-launch acceptable per `no-premature-backward-compat`.
No `format_version` bump (slot byte-layout unchanged).

### 9. Reject-code disposition

`PBC_REJECT_STATS_FINGERPRINT_MISMATCH`
(`src/storage/stats.rs:391`) is renamed to
`PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH` to match
its routing. The reject path is unchanged — CSB merge
eligibility still rejects on mismatch and falls through
to row-level computation.

Pre-launch wire rename — no compat shim — consistent with
how ADR 050 removed
`PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY`.

No new reject-code family. Logical fingerprint mismatch
on aggregate cache lookup is a cache miss, not a reject.

### 10. Composition with existing ADRs

- **ADR 037 (`tenant_table_schemas`)**: descriptor
  remains postcard-serializable; one `[u8; 32]` field
  becomes two newtype-wrapped `[u8; 32]` fields. Pre-
  launch postcard schema break.
- **ADR 045 / 049 (identity policies)**:
  `RowIdentityPolicy` hashes into the logical preimage.
  ADR 049 §3 settled that content-hash row identity
  preimage is fingerprint-free; ADR 051 does not change
  that.
- **ADR 046 (`StateDigest` substrate)**: descriptor
  fingerprints use plain BLAKE3, not Pedersen. New tags
  `digest.table.schema.logical.v1` and
  `digest.table.descriptor.physical.v1` extend the
  existing domain-tag taxonomy.
- **ADR 047 / 050 (tombstones, PK delete)**: no
  descriptor field added; admission paths read
  fingerprints only through descriptor validation.
- **ADR 048 (per-table retention)**: lifecycle policy
  outside the descriptor, outside both fingerprints.
  `RETENTION_REJECTED_PK_DISALLOWED` remains in force.

### 11. Schema evolution rules unchanged

ADR 051 does not broaden what schema evolution may alter.
Existing rules stand:

- `row_identity_policy` cannot change in V1.
- `locality_policy` cannot change in V1, even though it
  is classified as physical. It is physical-but-immutable
  until a recluster story opens it.
- `evolution_policy` cannot change in V1.
- Record layout and record-body compression changes
  remain allowed where existing ADRs allow them.
- Stats-policy changes remain physical.
- Content-hash tables keep ADR 049's nullable-addition
  and decimal evolution restrictions.

The split changes fingerprints and cache invalidation
semantics. It does not silently make hard physical
migrations legal.

### 12. Identity-vs-Locality framing

Per `plans/philosophy.md`, the project distinguishes
Identity (what something *is*) from Locality (where /
how it lives). The logical / physical split applies that
framing at the descriptor level:

- **Logical fingerprint** = identity (what a row IS
  semantically: this table, these columns, this identity
  policy, this evolution discipline).
- **Physical fingerprint** = locality (where / how a row
  LIVES: this partitioning, this layout, this
  compression, this stats coverage).

Stacking encodes the natural lattice — locality is *of an
identity*; physical fingerprint is *of a logical schema*.
Identification fields (`table_object_id`, namespace,
name) sit *inside* the identity layer because they bind
which table this identity describes. `tenant_id` sits
above the descriptor in the catalog-instance layer.

## Rationale

**Why split now.** Roadmap #7 deliberately sequences this
*before* #8 / #10 / #11. Settling fingerprint shape now
means each access-pattern ADR consumes the right side
without the council relitigating "should this index entry
rebuild on compression retune?" each time. Cumulative
rework minimization, per the roadmap's "software
composition" prioritization principle.

**Why route every V1 consumer to logical.** All existing
fingerprint comparison sites care about logical row
meaning: aggregate idempotency over logical rows; CSB
facts over logical columns; columnar directory's column-
decode contract is logical. Physical / compression /
layout / stats-coverage retunes change *byte encoding* of
these facts, not the facts themselves. Routing those
sites to physical would over-conservatively block valid
CSB merges and columnar reuses across compression
retunes.

**Why payload-coverage validation makes logical CSB
routing correct.** Without the payload-coverage check, a
stats-policy retune would create CSBs that pass logical
descriptor identity but fail at the operation level
(bloom requested, no bloom carried). The §7 discipline —
descriptor-level logical match plus parsed-payload
coverage check — gives the right semantics: structurally
compatible CSBs are always considered; operations that
need missing optional coverage fall through. This is
strictly better than physical routing, which would also
reject CSBs whose operation was actually valid.

**Why include identification in logical.** Descriptor
self-validation must catch corruption that swaps
`table_object_id`, namespace, or table name. The
"structural-equivalence-across-tenants" property has no
V1 consumer; table-bound identity is the immediate
benefit.

**Why exclude `tenant_id`.** Catalog / request cross-
bind, not a descriptor field. The catalog tuple
`(tenant_id, table_object_id, schema_generation)` is the
instance identity at every wire surface.

**Why exclude `contract_version`.** Descriptor-format
metadata, not table shape. A future incompatible bump
gets its own domain tag rather than depending on V1
fingerprint comparison.

**Why exclude `schema_generation`.** A no-op generation
bump should leave fingerprints stable. Generation
captures order; fingerprint captures shape.

**Why stacked over orthogonal.** Future LSM (#11) needs
strict-physical-match that also catches logical mismatch
under additive evolution. Stacking gives this for free;
orthogonal would require LSM to compare both
fingerprints. Cost of stacking: one 32-byte preamble in
the physical preimage. Negligible.

**Why no new non-fingerprint descriptor fields.** Per
`no-premature-backward-compat`, ADR 051 settles the
*organization* of an existing field (splitting the
single fingerprint slot into two newtyped fields), not
the addition of new non-fingerprint fields. Future
ADRs that need new physical shape add fields and bump
the physical preimage with a new domain tag if needed.

**Why pre-launch CSB cliff is acceptable.** Operationally,
post-ADR-051 reads of pre-ADR-051 CSBs fall through to
row-level — no incorrect answers, no panics, just
degraded fast-path. A tolerant decoder would preserve a
code path with no production lifecycle.

**Why no `physical_generation`.** Catalog sequencing is
uni-dimensional; descriptor identity is bi-dimensional
via fingerprints. A generation split would touch envelope
semantics, client pinning, historical descriptor
selection, and migration UX — all real questions but
none required for #8 / #10 / #11.

**Why long asymmetric field names.** The asymmetry
encodes the conceptual difference: logical fingerprint
is *of the schema*; physical fingerprint is *of the
descriptor*. Long names are slightly verbose but
unambiguous in greps and call-site reads — important for
the architecture-test guards.

**Why rename `PBC_REJECT_STATS_FINGERPRINT_MISMATCH`.**
Pre-launch, an inaccurate name is a small but real
maintenance tax. The new name matches the routing.

**Why locality is physical.** Locality determines
placement and scan ranges, not the semantic value of a
row. It remains immutable in V1 because changing it
safely needs a recluster migration.

**Why newtypes.** The `newtype-units` convention forbids
raw hash-shaped quantities across module boundaries.
Logical and physical fingerprints are same-width but not
interchangeable.

## Consequences

- **Downstream ADRs (#8 / #10 / #11) inherit a settled
  fingerprint shape.** Index entries / CDC cursors / CSB
  / columnar → logical; LSM segment merge / planning
  caches → physical. No re-relitigation per ADR.
- **Aggregate idempotency cache key is decoupled from
  descriptor-wide physical fingerprint.** The
  `logical_schema_fingerprint` axis of the V1 cache key
  `(tenant_id, table_object_id, schema_generation,
  logical_schema_fingerprint)` is independent of
  physical retunes. V1 still misses on physical retunes
  via the `schema_generation` axis (every typed
  descriptor mutation bumps generation per §5);
  cross-generation cache hits on physical-only
  mutations are a follow-up ADR (Open Questions §1)
  that drops generation from the key once operator
  demand is demonstrated.
- **CSB merges survive physical retune.** Same logical
  schema → CSBs are descriptor-eligible; payload coverage
  governs per-operation success.
- **Columnar directories survive physical retune.** Each
  directory carries its write-time physical policy;
  current descriptor only needs logical agreement.
- **CSB merge / direct-answer code must validate parsed
  stats payload coverage** instead of relying on
  descriptor-level physical equality.
- **Descriptor self-validation catches identity-field
  tampering.** `table_object_id` / namespace / table
  name in the logical preimage gate corruption.
- **Physical-only descriptor changes can be observed as
  generation changes with stable logical fingerprint** —
  the bi-dimensional change axis becomes operator-
  legible.
- **One-time CSB cliff at pre-/post-ADR-051 boundary.**
  Falls through to row-level until compaction rewrites.
- **Two fingerprints to compute on every descriptor
  materialization.** Marginal CPU cost; both are 32-byte
  BLAKE3 outputs.
- **Postcard schema for `TableSchemaDescriptorV1`
  changes** — one `[u8; 32]` field becomes two newtyped
  `[u8; 32]` fields. Pre-launch break.
- **`PBC_REJECT_STATS_FINGERPRINT_MISMATCH` is renamed**
  to `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`.
- **Descriptor `validate()` recomputes and compares both
  fingerprints.** Strictly more validation surface than
  today.
- **No on-disk slab format change** beyond the CSB header
  slot reinterpretation.

## Rejected alternatives

- **Single fingerprint, mask at consumer site.** Each
  consumer derives the relevant subset before hashing.
  Rejected: every call site needs to know which fields
  matter, drift is inevitable, the whole point of
  fingerprinting is centralized preimage discipline.
- **Three-fingerprint split** (logical / physical-shared
  / physical-per-node). Rejected: V1 has no per-node
  physical variation. A future ADR can extend the
  physical preimage with a
  `digest.table.descriptor.physical_per_node.v1` domain
  when needed.
- **Per-column fingerprint.** Rejected: column-aware
  locality and column projection are deferred per ADR
  049; descriptor-level physical fingerprint serves V1
  consumers.
- **Orthogonal (unstacked) preimages.** Rejected per §3.
  Future LSM safety is the load-bearing case.
- **Route CSB / columnar directory to physical
  fingerprint.** Rejected: over-conservative — would
  reject valid merges / reuses across compression
  retunes. The §7 payload-coverage discipline gives the
  correct semantics with logical routing.
- **Stamp physical fingerprint into CSB header alongside
  logical.** Rejected: no V1 consumer reads it; logical
  is the right binding; payload coverage handles per-
  operation variation.
- **Reject CSB merge solely on descriptor physical-
  policy difference.** Rejected: over-conservative; §7
  payload-coverage check is the right gate.
- **Exclude identification fields from preimages.**
  Rejected: descriptor self-validation needs identity-
  field tampering detection; cross-tenant structural
  sharing has no V1 consumer.
- **Include `contract_version` in either preimage.**
  Rejected: descriptor-format metadata; future
  incompatible bumps get their own domain tag.
- **Include `tenant_id` in either preimage.** Rejected:
  catalog / request cross-bind.
- **Keep `schema_fingerprint` as an alias for one of the
  new fingerprints.** Rejected per
  `no-premature-backward-compat`.
- **Bump CSB header `format_version` and write a
  tolerant decoder.** Rejected per
  `no-premature-backward-compat`. Fall-through is the
  safe failure mode.
- **Include `schema_generation` in either preimage.**
  Rejected: a no-op generation bump should leave
  fingerprints stable.
- **Allocate `digest.table.row.physical.v1` here.**
  Rejected: that's a row-set comparison digest, not a
  descriptor digest; consumer (anti-entropy on physical
  row sets) is speculative for V1. Defer.
- **Defer the split to inside ADR #8 (secondary
  indexes).** Rejected: bundling fingerprint design into
  the secondary index ADR makes both ADRs larger,
  defers the descriptor cleanup, and mixes "what does an
  index entry bind to?" with "what is a descriptor?".
  Roadmap #7 sequencing exists exactly to avoid this.
- **Bundle `TableSchemaDescriptorV1` →
  `TableSchemaDescriptor` rename.** Rejected: separate
  convention-compliance pass with broader blast radius.
- **Add a logical-generation / physical-generation
  pair.** Rejected: no V1 consumer.
- **Introduce `physical_generation` catalog split.**
  Rejected: pulls in envelope semantics, client
  pinning, historical descriptor selection, migration
  UX.
- **Implement `Pod` / `Zeroable` for the fingerprint
  newtypes.** Rejected: bytemuck transmutation bypasses
  constructor discipline.
- **Short field names** (`logical_fingerprint` /
  `physical_fingerprint`). Rejected: long asymmetric
  form encodes the conceptual distinction and improves
  grep clarity.

## Resolved questions

These are the V1 decisions converged across Codex,
Claude, and Gemini. The implementation plan inherits
them.

1. Two fingerprints, both newtyped:
   `LogicalSchemaFingerprint` and
   `PhysicalDescriptorFingerprint`.
2. Stacked: physical preimage embeds logical fingerprint
   (LSM-safety).
3. Domain tags: `digest.table.schema.logical.v1` and
   `digest.table.descriptor.physical.v1`.
4. Logical preimage: `(table_object_id, namespace,
   table_name, arrow_schema_ipc, row_identity_policy,
   evolution_policy)`.
5. Physical preimage: `(logical_schema_fingerprint,
   locality_policy, record_layout_policy,
   record_body_compression_policy, stats_policy)`.
6. `tenant_id` excluded from both preimages (catalog
   cross-bind).
7. `contract_version` excluded from both preimages
   (descriptor-format metadata).
8. `schema_generation` excluded from both preimages
   (temporal ordering).
9. Lifecycle policy state outside both fingerprints (per
   ADR 048).
10. Schema-evolution rules unchanged.
11. `schema_fingerprint: [u8; 32]` is replaced by two
    newtyped fields; pre-launch postcard schema break.
12. CSB header on-disk slot stores **logical**
    fingerprint; pre-launch cliff.
13. CSB compaction eligibility uses **logical**
    fingerprint; payload-coverage validation governs
    per-operation success.
14. Columnar directory validation uses **logical**
    fingerprint.
15. Aggregate idempotency cache + wire envelope use
    **logical** fingerprint; cache key keeps
    `schema_generation`.
16. `PBC_REJECT_STATS_FINGERPRINT_MISMATCH` renamed to
    `PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH`.
17. No new non-fingerprint descriptor fields (the single
    fingerprint slot is split into two newtyped fields);
    no new HRFT frame; no new digest domain beyond the
    two listed; no `format_version` bump.
18. No `physical_generation`.
19. Content-hash row identity (ADR 049) and PK delete
    admission (ADR 050) preimages stay fingerprint-free.
20. Defer `TableSchemaDescriptorV1` →
    `TableSchemaDescriptor` rename.
21. Field names: long asymmetric form
    (`logical_schema_fingerprint`,
    `physical_descriptor_fingerprint`); type names
    match.
22. Constructor discipline: `pub(crate) fn from_bytes`,
    `pub fn as_bytes(&self) -> &[u8; 32]`, no `Pod` /
    `Zeroable`.
23. Identity-vs-Locality framing at the descriptor level
    (§12).
24. CSB merge / direct-answer paths validate parsed
    payload coverage (column ordinal, Arrow stats type,
    `stat_flags`, optional payload presence) after
    descriptor-level logical match (§7).

## Open questions

V1 picks defaults; each can be revisited in a follow-up
ADR without on-disk surface change. Two operational
calibrations remain, both pinned to specific phases of
the impl plan:

1. **Aggregate cache key relaxation.** V1 keeps
   `(tenant_id, table_object_id, schema_generation,
   logical_schema_fingerprint)`. A future ADR can relax
   the key to drop `schema_generation` if operator
   demand emerges for cross-generation cache hits on
   physical-only mutations.
2. **Bench acceptance threshold.** "Same order of cost
   as monolithic hashing" framing. A numeric tightening
   (e.g., 1.2× for descriptor recompute) can be set
   after Phase 7 smoke-run measurement.

## Forward-compatibility analysis

**Roadmap #8 secondary indexes.** Index entries bind to
logical fingerprint; index segments compare logical
fingerprints for membership compatibility. Physical
retune does not invalidate index segments — exactly the
property the split delivers.

**Roadmap #10 CDC.** Cursor checkpoints bind to logical
fingerprint plus HLC plus a state-digest reconciliation
point. Layout / compression mutations do not break
consumer cursors.

**Roadmap #11 LSM.** Compaction merge eligibility is
gated on physical fingerprint agreement. Stacking
ensures cross-logical merges are blocked even when
physical-only fields happen to match.

**Future per-node physical variation.** Allocates a
distinct `digest.table.descriptor.physical_per_node.v1`
domain tag and a parallel newtype.

**Future column-projection / column-aware locality.**
Adds descriptor fields; rolls into the physical
preimage.

**Future descriptor type rename.** Fingerprint domain
tags do not bake the type name in; the rename is
preimage-neutral.

**Future `contract_version` bump.** A future
incompatible descriptor format gets its own domain tag
(e.g., `digest.table.schema.logical.v2`) and a parallel
newtype.

**Future anti-entropy reconciliation.** Logical
fingerprint is the natural cluster-wide reconciliation
anchor for visible-row state; physical fingerprint is
the natural per-replica reconciliation anchor for
storage layout.

**Future locality recluster.** Locality is physical and
immutable in V1. A future recluster ADR can allow
locality changes by defining the migration proof and
range-index transition.

**Future cross-tenant structural cache.**
Identification-inclusion makes logical fingerprints
table-bound; cross-tenant structural sharing requires a
separate "structural-shape" fingerprint (no V1 consumer;
deferred).

## Deferred to follow-up ADRs

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
   (rejected here; reconsiderable post-launch if cliff
   is operationally painful).
7. Locality-policy recluster migration.
8. Per-column physical encoding and compression
   policies.
9. Aggregate cache key relaxation (drop generation; key
   on logical fingerprint alone).
10. Cross-tenant structural-shape fingerprint
    (table-identity-free) if a consumer emerges.
11. Secondary-index descriptor and segment metadata
    (#8).
12. CDC cursor surface (#10).

## References

- **ADR 037** — `tenant_table_schemas`; defines
  `TableSchemaDescriptorV1` and the original single-
  fingerprint shape.
- **ADR 040** — aggregate pushdown; the request /
  response cross-bind that moves to logical fingerprint.
- **ADR 042** — analytical multi-row fragments; CSB
  metadata composition origin.
- **ADR 043** — Arrow IPC body compression; physical-
  shape composition input.
- **ADR 044** — L2 columnar fetch; columnar directory
  consumer that moves to logical fingerprint.
- **ADR 045** — PK-less append-only tables;
  `RowIdentityPolicy` variant that hashes into the
  logical preimage.
- **ADR 046** — `StateDigest` substrate; §7 deferral
  test governs new digest domain allocation.
- **ADR 047** — row-level tombstones; admission paths
  read fingerprints only through descriptor validation.
- **ADR 048** — per-table retention; lifecycle policy
  outside both fingerprints.
- **ADR 049** — content-hash identity; row-key preimage
  stays fingerprint-free; `RowIdentityPolicy` variant
  hashes into the logical preimage.
- **ADR 050** — PK delete via supersession;
  `digest.table.row.physical.v1` reservation deferred to
  follow-up.
- `plans/conventions/newtype-units.md` — fingerprint
  newtypes.
- `plans/conventions/no-version-suffixes.md` — domain-
  tag `.v1` bytes are wire-protocol envelopes; new Rust
  types take no `V1` suffix.
- `plans/conventions/no-premature-backward-compat.md`
  — pre-launch CSB cliff over tolerant decoder;
  postcard schema break over compat shim.
- `plans/conventions/no-project-name-in-protocols.md`
  — domain tags carry no project name.
- `plans/conventions/capability-vs-feature-naming.md`.
- `plans/philosophy.md` — Identity-vs-Locality framing
  inherited (§12).
