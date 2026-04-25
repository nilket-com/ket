# ADR 038: Column-Aware Locality V1 (Z-Order)

## Status

Accepted

## Context

ADR 037 made typed tenant tables real and reserved
`LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1)` as the slot
for column-aware clustering. The current implementation parses that variant
and rejects it. ADR 038 closes the gap: a tenant declares a small set of
clustering columns, and the storage engine maps row writes to ring positions
that preserve range locality on those columns.

Brainstorm 001 framed the direction (Identity vs. Locality, salted spread via
jump-consistent hash, the OLAP scan thesis). ADR 037 already implemented the
salted-spread half via `TableSpreadProjection`. ADR 038 fills the
column-aware-locality half.

The seven design surfaces the post-037 roadmap names for ADR 038 are:

1. SFC bit layout
2. Supported clustering column types
3. Time bucketing
4. Salt-spread interaction
5. Migration when clustering columns change
6. Compaction interaction
7. Toron planner range pruning

Each gets an explicit decision below.

## Decision

Wendelon ships column-aware locality V1 as a Z-order projection over a bounded
set of declared clustering columns, with the descriptor as the authoritative
metadata carrier and no changes to `SlabRecordHeader`.

### Scope and compatibility

1. **`ColumnProjection` becomes supported.**
   `LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1)` no longer
   rejects at descriptor validation. Existing `IdentityProjection` and
   `TableSpreadProjection` tables are unchanged.

2. **`ProjectionFunctionV1::Hilbert` is reserved but not supported in V1.**
   Descriptors declaring `Hilbert` validate-and-reject with
   `WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`. A future Hilbert ADR
   (number assigned at opening) lifts that rejection.

3. **`ProjectionFunctionV1::RandomHash` keeps its meaning** — column-hash
   spread, no range semantics. It is a valid `ColumnProjection` choice but the
   planner does not derive prefix arcs from clustering predicates against it.

### Dimensionality and types

4. **Hard cap: `columns.len() ∈ 1..=6`.**
   `> 6` fails descriptor validation with `WPBC_REJECT_CLUSTERING_TOO_WIDE`.
   No silent truncation. No implicit secondary lex sort. The reason is
   query-time prefix decomposition: a d-dimensional range predicate fragments
   into Morton arcs at a rate that grows superlinearly with `d`. The
   `2^d` orthant count is the textbook intuition (d=6 → 64 octants), but
   real decompositions can fragment further depending on predicate shape
   and bit widths — the contractual guarantee is the planner's 1024-arc
   budget plus full-arc fallback (decision 17), not a tight math bound.

5. **`columns.len() == 1` is allowed but uses range projection, not curve
   machinery.** A single-dimensional Z-order is a no-op interleave and the
   planner can treat clustering-on-one-column as a sorted projection. This
   keeps the descriptor ergonomic for "cluster by event_time" without forcing
   tenants into multi-dim contortions.

6. **Supported clustering column types (V1 allowlist):**
   - signed integers (`Int8`/`Int16`/`Int32`/`Int64`)
   - unsigned integers (`UInt8`/`UInt16`/`UInt32`/`UInt64`)
   - `Timestamp(Millisecond, _)`, normalized to UTC milliseconds (matches ADR
     037 §6 PK canonicalization)
   - `Utf8` and `LargeUtf8` with descriptor-declared prefix byte width: the
     first N bytes are interpreted as a big-endian unsigned integer for
     ordering purposes
   - `Binary` and `LargeBinary` with descriptor-declared prefix byte width,
     same big-endian-unsigned interpretation

   Rejected in V1:
   - floats (NaN ordering, signed-zero, denormals — separate ADR if needed)
   - decimals (locality-relevant ordering depends on scale; deferred)
   - nullable clustering columns (rejected with `WPBC_REJECT_CLUSTERING_NULLABLE`)
   - nested types (`Struct`, `List`, `Map`)

   String prefix clustering is included to support common warehouse patterns
   (`region = 'eu-west-1'`, `tenant_id = 'acme'`). Without it, every tenant
   pre-computes integer surrogates, which throws the ergonomic win away.

7. **No descriptor-level time bucketing in V1.** A tenant who wants
   "cluster by event_hour" pre-computes an `event_hour` column and clusters on
   that. Embedding bucket sizes in the descriptor (hour, day, week) would
   couple the locality format to a bucket-policy enum that is not yet stable
   and creates a migration path every time a tenant changes bucket
   granularity. Pre-computed bucket columns inherit ADR 037's additive schema
   evolution rules without special-casing.

### Bit layout

8. **Per-column declared bit widths.** The descriptor states each clustering
   column's contribution to the SFC suffix in bits. The validator enforces:

   ```
   sum(column_widths_bits) + prefix_bits ≤ 64
   ```

   This is more explicit than `floor(suffix_bits / dimensions)`. A
   `tenant_id` clustering column does not need 12 bits; an
   `event_time_seconds_since_epoch_truncated` column might need 24. Letting
   the descriptor say so removes guesswork and matches the codebase's
   physics-grade discipline of making invariants visible.

9. **Locality word layout (high → low, MSB-first):**

   ```
   high                                                              low
   ┌────────────────┬─────────────────────────────────────┬──────────┐
   │ table_prefix   │       z_order_interleave            │ entropy  │
   │ (prefix_bits)  │     (sum(column_widths_bits))       │  tail    │
   └────────────────┴─────────────────────────────────────┴──────────┘
   ```

   Where:
   ```
   bucket          = jump_consistent_hash(key_identity, spread_factor)
   table_prefix    = high prefix_bits of
                     hash64("wendelon.table-locality.v1" ||
                            tenant_id || table_object_id || bucket)
   z_order_suffix  = MSB-first bit interleave of
                     normalized(col_0), normalized(col_1), ...
                     where col_i contributes column_widths_bits[i] bits
   entropy_tail    = remaining low bits filled from key_identity
   locality        = table_prefix | z_order_suffix | entropy_tail
   ```

10. **Bit-interleave order is MSB-first per column.**
    The high bit of the highest-priority clustering column lands in the high
    bit of `z_order_suffix`. This makes coarse predicates ("year = 2026") map
    to coarse locality prefixes, which is what the planner needs to derive
    contiguous arcs.

11. **Salt-before-SFC, fixed in V1.** The salted spread bucket lives in
    the prefix bits, above the SFC suffix. Matches ADR 037's
    `TableSpreadProjection` semantics: the table is spread across
    `spread_factor` arcs, and *within* each arc rows are clustered by curve.
    Putting curve bits above salt would give ring-global clustering at the
    cost of letting hot tables collapse onto a single arc — the failure
    mode ADR 037 deliberately avoided. V1 hard-codes this; no
    `salt_position` field on the descriptor.

### Metadata location and per-record layout

12. **The descriptor is authoritative; `SlabRecordHeader` is unchanged.**
    `ColumnLocalityDescriptorV1` already carries `columns`,
    `projection: ProjectionFunctionV1`, `spread_factor`, `prefix_bits`. ADR
    038 reshapes `columns` into `Vec<ClusteringColumnSpecV1>` so per-column
    `width_bits` is declared. No new bits in `SlabRecordHeader`, no rename
    of `_pad_align`.

13. **Per-scan dispatch reads the descriptor once.** The planner resolves
    `(tenant_id, table_object_id, schema_generation) → descriptor`,
    monomorphizes the encode/decompose path for the descriptor's
    `(projection, n_dims)` pair, and runs the scan. There is no per-record
    branch and therefore no need for a per-record byte.

### Encoding

14. **Default Z-order encoder is portable shift-and-mask.** Hacker's Delight
    §7-2 magic-constants bit-interleave. Works on every Wendelon-supported
    architecture (x86_64, aarch64, including ARM-based cloud instances).

15. **BMI2 `_pdep_u64` is a runtime-detected fast path.** Selected at startup
    via `is_x86_feature_detected!("bmi2")`, never assumed. Differential tests
    require BMI2 and portable paths to produce byte-identical Locality values
    on the same input.

### Range pruning

16. **Predicate → inclusive Z-range → prefix arcs.** Given a predicate
    `clustering_col_i ∈ [lo_i, hi_i]` for each clustering column, the planner:
    (a) interleaves `lo_i` MSB-first into the suffix layout to form `lo_z`,
    (b) interleaves `hi_i` to form `hi_z`,
    (c) decomposes `[lo_z, hi_z]` into contiguous prefix arcs using the
    standard Morton-range subdivision (LITMAX/BIGMIN, Tropf & Herzog 1981),
    capped at the 1024-arc budget,
    (d) for each unique table_prefix derived from the descriptor's
    `spread_factor` arcs, issues a range scan over `(table_prefix |
    arc_range)`.

17. **Range decomposition has a hard admission budget.** Default cap: 1024
    output arcs per scan. Overflow falls back to the table's full
    `spread_factor` arcs and lets WPBC do the filtering. The cap is a
    `wendelon-server` config value, not a per-query knob, so behavior is
    predictable across Toron planners. Surfaced metric:
    `wpbc_range_decomposition_overflow_total`.

### Compaction

18. **Compaction inputs must share descriptor identity.**
    Two slabs may compact into one only when their
    `(table_object_id, schema_generation, locality_policy)` agree. The
    locality policy is descriptor-identified, so this falls out of ADR 037's
    `schema_generation` discipline; ADR 038 makes the invariant explicit
    because Z-order monotonicity within a slab depends on it.

19. **Compaction output is monotonic in Locality within a slab.** Existing
    compaction already sorts on storage row identity; under
    `ColumnProjection`, sorting on Locality preserves curve-locality of the
    output. This is implementation-internal — not a wire-format change — but
    must be stated as an invariant so future compaction work doesn't
    regress it.

### Migration

20. **Clustering-policy changes are explicit, audited, offline rewrites.**
    Inherits ADR 037 §20. Adds: the rewrite tool reads existing rows,
    re-encodes Locality under the new descriptor, and writes new slab
    records under a new `schema_generation`. Old rows remain readable under
    the old `schema_generation` until a separate purge step. The audit
    record names old and new descriptor fingerprints. Online migration
    (concurrent writes during rewrite) is out of scope for V1.

21. **`spread_factor` increase is a soft migration; everything else is a
    full rewrite.** Under jump-consistent hash, adding one bucket migrates
    exactly `1/new_factor` of rows; the rewrite tool can skip records that
    already hash to the original bucket. **Decreasing `spread_factor`,
    changing `width_bits`, changing the column set, or changing the
    projection function (e.g., Z-order → Hilbert under a future ADR) all
    require a full row-by-row rewrite** because their effect on Locality
    is non-monotonic. The audit record fires in both cases; the cost
    difference is in the rewrite tool's input set, not in correctness.

## Descriptor shape

```rust
struct ColumnLocalityDescriptorV1 {
    columns: Vec<ClusteringColumnSpecV1>,   // 1..=6, validated
    projection: ProjectionFunctionV1,        // ZOrder (V1) or Hilbert (rejected)
    spread_factor: u16,                      // 1..=256, default 4
    prefix_bits: u8,                         // 1..=63, default 16
}

struct ClusteringColumnSpecV1 {
    name: String,                            // must resolve in arrow_schema
    width_bits: u8,                          // 1..=64, contribution to SFC suffix
    string_prefix_bytes: Option<u8>,         // required for Utf8/Binary; None otherwise
}

enum ProjectionFunctionV1 {
    RandomHash,
    ZOrder,
    Hilbert,                                  // parsed; rejected as WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED
}
```

Validation invariants:
- `columns.len() ∈ 1..=6`
- `sum(c.width_bits for c in columns) + prefix_bits ≤ 64`
- For `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary` columns,
  `string_prefix_bytes.is_some() && string_prefix_bytes * 8 == width_bits`
- For numeric/timestamp columns, `string_prefix_bytes.is_none()`
- `prefix_bits ∈ 1..=63`
- `spread_factor ∈ 1..=256`

## Physical layout (256-bit Identity → 64-bit Locality)

The full storage representation spans two tiers: a **256-bit content-addressed
Identity** (the BLAKE3 hash of the row preimage) and a **64-bit Locality**
(the ring address derived from Identity and the clustering columns).
Identity is invariant for the lifetime of the row; Locality is computed
once at write time and baked into the slab record (ADR 037 §20).

### Tier 1 — Identity (256 bits, BLAKE3 content hash)

```
preimage = "wendelon.table-row.v1"   ──── domain tag (ADR 037 §4)
         ‖ tenant_id                  ──── TenantId (8 B / 64-bit)
         ‖ table_object_id            ──── TableObjectId (16 B / 128-bit)
         ‖ canonical_pk_bytes         ──── PK type-specific (ADR 037 §6)

key_identity = BLAKE3(preimage)

  bit 255                                                            bit 0
  ┌────────────────────────────────────────────────────────────────────┐
  │                      key_identity (256 bits)                        │
  │              full content-addressed hash, never reused              │
  └────────────────────────────────────────────────────────────────────┘
```

Identity sits in the `(tenant_id, key_identity)` storage row key. It never
appears in the user-facing schema. PK lookup from the tenant API resolves
to `key_identity` via the same BLAKE3 preimage.

### Tier 2 — Locality (64 bits, ring address)

```
bucket          = jump_consistent_hash(key_identity, spread_factor)
                  ──> bucket ∈ [0, spread_factor)

table_prefix    = high prefix_bits of
                  hash64("wendelon.table-locality.v1"
                         ‖ tenant_id
                         ‖ table_object_id
                         ‖ bucket)

z_order_suffix  = MSB-first bit interleave of normalized clustering coords,
                  per descriptor.column_widths_bits

entropy_tail    = low (64 - prefix_bits - sum(widths_bits)) bits of
                  key_identity   ── filler when widths don't saturate

locality        = (table_prefix    << (64 - prefix_bits))
                | (z_order_suffix  << entropy_tail_bits)
                |  entropy_tail
```

```
  bit 63                                                          bit 0
  ┌─────────────┬──────────────────────────────────┬──────────────┐
  │table_prefix │         z_order_suffix           │ entropy_tail │
  │ prefix_bits │     sum(column_widths_bits)      │  remainder   │
  │ default 16  │      varies per descriptor       │              │
  └─────────────┴──────────────────────────────────┴──────────────┘
        ▲                       ▲                        ▲
        │                       │                        │
        │                       │                        └─ low bits of
        │                       │                           key_identity
        │                       │                           (Tier 1)
        │                       │
        │                       └─ Tier 3:
        │                          MSB-first bit interleave
        │                          of normalized clustering coords
        │
        └─ salted table prefix:
           bucket = jump_consistent_hash(key_identity, spread_factor)
           ──> hash64(domain_tag ‖ tenant_id ‖ table_object_id ‖ bucket)
           ──> high prefix_bits
```

Salt-before-SFC: a hot table is spread across `spread_factor` disjoint
`table_prefix` arcs, each internally clustered by the SFC. A range query
visits each of the ≤ `spread_factor` arcs and the LITMAX/BIGMIN-decomposed
sub-ranges within each.

### Tier 3 — clustering columns into the z-order suffix

The descriptor declares per-column bit widths. Interleaving proceeds
**level by level, MSB-first per column, in declared column order**.
Level 0 is the topmost level; at each level, every column that still has
a bit at that level emits one bit, in declared order. Smaller-width
columns drop out of higher (lower-index) levels.

```
level 0 (topmost):    MSB of c0, c1, c2, ..., c_{D-1}      (D bits, all emit)
level 1:              next bit of c0, c1, ...              (skip exhausted)
...
level max(widths)-1:  only the column with the largest width still emits
```

This places coarse predicates ("year = 2026") in coarse Locality prefixes,
which is what the planner needs to derive contiguous arcs.

### Worked example

Descriptor:

```
columns       = [region (UInt8), event_hour (UInt32), customer_id (UInt32)]
widths_bits   = [4,              24,                   20]   sum = 48
prefix_bits   = 16    (default)
spread_factor = 4     (default)

16 + 48 = 64    no entropy_tail in this example
```

Locality bit allocation:

```
  bit 63                      48  47                                      0
  ┌────────────────────────────┬─────────────────────────────────────────┐
  │     table_prefix (16b)     │          z_order_suffix (48b)           │
  │  salted via spread_factor=4│  MSB-first interleave of                │
  │                            │  [region, event_hour, customer_id]      │
  └────────────────────────────┴─────────────────────────────────────────┘
```

Suffix interleave structure (48 bits):

```
levels  0..3   (4 levels):  region + event_hour + customer_id all emit
                            → 3 bits/level × 4 levels = 12 bits
                            (region's 4 bits exhaust at level 3)

levels  4..19  (16 levels): event_hour + customer_id emit
                            → 2 bits/level × 16 levels = 32 bits
                            (customer_id's 20 bits exhaust at level 19)

levels 20..23  (4 levels):  only event_hour emits
                            → 1 bit/level × 4 levels = 4 bits
                            (event_hour's 24 bits exhaust at level 23)

total = 12 + 32 + 4 = 48 bits  ✓
```

A query like `region = 'eu' AND event_hour BETWEEN h_lo AND h_hi`
fixes a 4-bit prefix on `region`, constrains a 24-bit interval on
`event_hour`, and leaves `customer_id` free. The planner derives the
inclusive `[lo_z, hi_z]` interval on the suffix, decomposes it into a
small number of contiguous Morton arcs (LITMAX/BIGMIN), then fans out
across the table's 4 `spread_factor` prefix arcs. Total scan target is
bounded by the 1024-arc planner budget; the query plan does not touch
the rest of the ring, and the descriptor reads happen once per scan,
not once per record.

## Rationale

**Why Z-order only in V1.** Hilbert and Z-order share the descriptor, the
type system, and the dimensionality cap. They differ in the encode/decompose
algorithms — substantially. Hilbert's range-to-prefix decomposition is not
LITMAX/BIGMIN; it is the inverse Hilbert mapping over candidate octants
with axis-rotation bookkeeping. That is a real implementation surface
with its own correctness, performance, and platform testing requirements.
Pinning ADR 038 to Z-order keeps the surface honest: descriptor + encode
+ range-decompose + planner + compaction + migration are all small. A
future Hilbert ADR layers Hilbert in by adding one variant to
`ProjectionFunctionV1` and one alternative path in encode/decompose; the
descriptor, validator, range-budget, salt-spread, and migration discipline
established here all carry over unchanged.

**Why descriptor-authoritative metadata.** Per-record metadata is paid on
every row. Per-table metadata is paid once per scan. The SFC choice is a
table property, not a row property, so the per-row cost is pure overhead.
ADR 037 §15 made this trade explicitly for `schema_generation`; the same
trade applies to projection function. The "fast branch in the planner"
framing assumes per-row dispatch in the read path; the actual hot path is
WPBC kernel execution over a `RecordBatch`, where the SFC choice has already
been resolved into the scan strategy. Adding a per-record byte to enable a
branch we don't take is a Chesterton's-fence reversal of ADR 037.

**Why fail-fast at 6.** The newtype discipline of this codebase rests on
making intent explicit. Silent truncation creates a divergence between
declared intent ("cluster on these 8 columns") and physical layout
("clustered on 6, slab-lex-sorted on 2"). The user's stated values of unifying
storage and physics-grade rigor argue against the silent path. Users who hit
the cap genuinely have a design choice to make — most-selective 6 — and
making that choice explicit is the right answer.

**Why per-column declared widths.** A descriptor that says "6 columns, evenly
divided into 8 bits each" is wrong for almost every real workload. Clustering
columns have very different cardinalities and selectivities. Declaring widths
explicitly lets the descriptor encode the workload's actual shape, makes
range-pruning math exact, and creates a migration trigger when the workload
shifts (a width change is a descriptor rewrite by design).

**Why string prefix clustering.** The realistic OLAP shape is
`tenant_id = 'acme' AND event_time BETWEEN ...`. Forcing every tenant to
maintain an integer surrogate for string clustering columns is a tax on the
ergonomic surface that the descriptor can absorb at near-zero cost. Declared
prefix bytes turn strings into deterministic unsigned integers for ordering
purposes, with the same semantics as the integer types — no collation, no
locale, no surprises.

**Why no time bucketing in the descriptor.** A `BucketSize` enum coupled to
locality is a permanent commitment. Pre-computed bucket columns are an
explicit, evolvable choice the tenant owns. The cost is an extra column in
the table; the benefit is the locality format does not grow a parallel time
ontology that cannot then be removed.

## Consequences (the negatives)

- **Hilbert remains unsupported until a future Hilbert ADR.** Tenants
  who want tighter bounding boxes for high-dimensional analytical
  queries must wait. Mitigated by Z-order being adequate for the common
  (≤4-dim) case.
- Descriptor validation grows: widths, type support, string-prefix constraints,
  nullability, and width overflow all need stable rejects.
- Tenants must choose clustering columns carefully when they hit six dimensions.
- Bad `width_bits` choices become physical layout mistakes and require rewrite
  migrations to fix.
- String prefix clustering is useful but irreversible without rewrite if the
  prefix is too short.

## Rejected alternatives

- **Header-byte metadata overload (`SlabRecordHeader._pad_align` or top bits of `prefix_bits`).**
  Rejected. Projection kind is table metadata, and planners branch once per scan after descriptor lookup.
  The strongest peer argument for header placement is recovery without a hot descriptor cache. The counter: Wendelon already cannot serve a record whose descriptor has not been loaded — `schema_generation` drives column resolution, type validation, and WPBC compilation. Adding the projection function to the per-record header would let us decode the locality bits without the descriptor, but we still couldn't interpret the row. The recovery story is incomplete either way; the right place to harden it is descriptor-replication and warm-start, not header duplication. If a future ADR proves header placement necessary, naming the field (don't anonymously overload `_pad_align`) is the right discipline; ADR 038 records that as the fallback shape rather than the V1 path.
- **Salt-after-SFC (apply jump-consistent hash to the encoded suffix rather than to the prefix bucket selector).** Rejected. Hashing the encoded SFC scrambles adjacent ring positions for adjacent clustering coords — i.e., it destroys the clustering the SFC just computed. The intuition behind "spread hot bounding boxes" can be partially recovered by raising `spread_factor` (more arcs per table, each still internally clustered), which is the salt-before-SFC mechanism we already ship.
- **Reserving `SaltPositionV1::AfterSfcBits` in V1.** Rejected as premature compatibility. Salt-before-SFC is the only V1 behavior, and a future salt strategy can arrive as a descriptor-generation change.
- **Bundling Hilbert into ADR 038.** Rejected for scope. Hilbert shares the descriptor but not the range-decomposition algorithm. It deserves its own ADR.
- **Graceful truncation past six clustering columns.** Rejected because it lies about physical layout. A tenant declaring eight columns should receive a structured DDL error and a remediation hint, not six real dimensions plus two local-sort dimensions.
- **Implicit secondary local lexicographic sort.** Rejected for V1. It does not help ring pruning and creates slab-local behavior that query planning cannot reason about globally.
- **Even-width division across dimensions.** Rejected because real clustering columns have different cardinalities and selectivities.
- **Descriptor-native time bucketing.** Rejected for V1; use explicit bucket columns.
- **Float and decimal clustering columns.** Rejected for V1 because ordering, NaN, signed zero, and scale semantics need a separate decision.
- **Dedicated `RangeProjection` in V1.** Rejected for now. A 1D `ColumnProjection` is operationally a range projection without creating a parallel descriptor.
- **Time-pinned locality versions (e.g., `LocalityV2` with new layout).** Rejected: descriptor-versioned policy via `schema_generation` already handles policy evolution; a parallel locality-format version creates redundant evolution surfaces.

## Implementation staging

- **Phase 0: Contract freeze and reject codes**
- **Phase 1: Descriptor validation**
- **Phase 2: Column normalization**
- **Phase 3: Z-order encoder, portable + BMI2**
- **Phase 4: Locality encoding**
- **Phase 5: Planner range decomposition with 1024-arc budget**
- **Phase 6: Compaction invariant**
- **Phase 7: Reclustering migration tool**
- **Phase 8: Metrics and bench gates**
- **Phase 8.5: `SlabRecordHeader` invariant regression guard**
- **Phase 9: Enable `ColumnProjection` writes**
- **Phase 10: Docs and finalization, post-037 roadmap update**
- **Phase 11: Recovery descriptor cache discipline note**

## Deferred Decisions

1. **Default range-decomposition cap (1024 arcs)** — tunable knob; revisit after first benches under realistic predicate shapes.
2. **Online migration protocol** — V1 is offline-rewrite-only; a dual-write window protocol is its own ADR if any tenant demands non-stop reclustering.
3. **Audit-record schema for clustering-policy changes** — inherits ADR 025's tamper-evident chain; payload defined when migration tool ships.
4. **Descriptor warm-start guarantee for recovery** — inherits ADR 032; ADR 038 introduces no new lazy-load semantics.
5. **`WPBC_REJECT_RANGE_DECOMPOSITION_OVERFLOW` exposure to Toron** — metric-only by default; reject code reserved in Appendix A for opt-in via request-options struct if Toron ever needs the explicit diagnostic.

## Appendix A: WPBC reject codes

- `WPBC_REJECT_CLUSTERING_COLUMN_UNKNOWN`
- `WPBC_REJECT_CLUSTERING_EMPTY`
- `WPBC_REJECT_CLUSTERING_NULLABLE`
- `WPBC_REJECT_CLUSTERING_TOO_WIDE`
- `WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED`
- `WPBC_REJECT_CLUSTERING_WIDTH_INVALID`
- `WPBC_REJECT_CLUSTERING_WIDTH_OVERFLOW`
- `WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED`
- `WPBC_REJECT_RANGE_DECOMPOSITION_OVERFLOW` (optional explicit diagnostic; default runtime behavior is fallback plus metric)

## Appendix B: Header metadata fallback (non-recommended)

This is not the recommendation; it is the fallback layout if a future ADR reverses the descriptor-authoritative decision.

```text
byte 0:
  bits 0..1: index kind
    0b00 = Z-order
    0b01 = Hilbert
    0b10 = RandomHash
    0b11 = reserved, reject
  bit 2: secondary_lex_sort_present, MUST be 0 in ADR 038
  bits 3..5: reserved, MUST be 0
  bits 6..7: locality_meta_version, MUST be 0 for V1

byte 1: dimension_count, valid 1..=6
byte 2: prefix_bits, valid 1..=63
byte 3: spread_factor_minus_1, decoded as 1..=256
```

## References

- ADR 005: Decoupling Compute via Pushdown Bytecode (WPBC)
- ADR 029: Identity vs. Locality
- ADR 032: Schema Evolution and Schema-Ring Contract
- ADR 034: Toron-Wendelon Integration Contract
- ADR 035: Vectorized WPBC Executor
- ADR 036: WPBC Pushdown Contract
- ADR 037: Tenant Table Schemas, Row Identity, Table-Scoped Locality — this ADR fills its `ColumnProjection` slot.
- Future Hilbert ADR (number assigned at opening): Column-Aware Locality V2, Hilbert variant.
- `plans/brainstorms/001_olap_locality_projections.md` — direction seed.
- `plans/schema/compute_ast.md` — WPBC V1 spec.
