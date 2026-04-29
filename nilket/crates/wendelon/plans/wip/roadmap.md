# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward. Updated when
the tip moves: each ADR that ships shifts the chain by one.

## Where we are

ADR 049 (Content-Hash Row Identity) just promoted. Adds
`RowIdentityPolicy::AppendOnlyContentHash` — the third
identity variant, deriving `__row_key_id__` from canonical
row content rather than write context. Synchronous admission-
time collapse via row-index lookup keeps ADR 046 §5 visible-
state semantics intact: duplicate writes are absorbed
idempotently with no `StateDigest` change; the absorb-vs-store
distinction is over **current visible membership**, not "key
ever existed," so re-ingestion after an ADR 047 tombstone or
ADR 048 retention eviction takes the store path and reinstates
the row. Sparse type-tagged canonical encoding over logical
Arrow values (NFC strings, NaN / -0 normalized, decimals as
precision + scale + unscaled-integer LE, dictionary arrays
canonicalized by decoded values) makes additive nullable
schema evolution identity-neutral. `column_tag` is a stable
assigned column ID (preferred) or stable logical-name hash
(fallback); physical ordinal rejected. Schema generation and
descriptor fingerprint excluded from the identity preimage.
V1 rejects non-NULL-default additive column-add with
`CONTENT_HASH_REJECTED_NON_NULL_DEFAULT` and decimal precision
/ scale evolution with `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`;
operators wanting either case use `AppendOnly` or wait for a
follow-up. `IdentityProjection` enabled (content-hash is the
first append-only policy where retry-stable identity allows
this); `ColumnProjection` deferred to a follow-up alongside
secondary indexes (roadmap #8). Visible-domain `StateDigest`
auto-content-sensitive via the existing visible-row preimage;
no new digest domain. See
`plans/adr/049_content_hash_row_identity.md` and
`plans/impl/049_content_hash_row_identity.md`. Implementation
phases pending.

ADR 048 (Per-Table Retention Policy) is the immediate
predecessor — replicated `TableLifecycleCatalog` keyed by
`(tenant_id, table_object_id)`; three modes (`InheritCluster`
/ `Disabled` / `FixedWindow`); two-phase enforcement (read-
time logical filter plus hard `as_of < floor` reject;
compaction-time physical eviction pinned to
`compaction_start_hlc`); pair-GC composes with ADR 047 under
the table floor. The topology gate was closed alongside
ADR 048 with **Outcome B** (opaque `NodeId` retained;
brainstorm 015 status header carries the closure). Splits to
brainstorm 018 (FROST-WAL authority) and brainstorm 019
(Sybil-resistance threat model).

ADR 047 (Row-Level Tombstones on Append-Only Tables) is the
lifecycle algebra ADR 048 composes against (pair-GC under
the table floor). ADR 046 (`StateDigest` substrate) supplies
the algebra. ADR 045 (PK-Less Append-Only Tables) supplies
the identity layer that ADR 049 complements with content-hash
identity; `__row_table_object_id__` is the ownership resolver
retention reclaim uses to look up the effective floor.

The implementation chain through ADR 049 is mostly built but
not fully bench-shipped. ADR 046 substrate cleared the cut-2
promotion bench gate (see
`plans/impl/046_state_digest_bench_results.md`). ADR 047,
ADR 048, and ADR 049 have quick-smoke or pending bench
artifacts; full 100k / 1M matrices remain the shipped-
verification bar before each is declared shipped.

## Prioritization philosophy

Wendelon is being sequenced as a complete-system launch rather
than as an incrementally exposed service. That changes the
roadmap constraint. In a live product, near-term user pain,
production evidence, and delivery commitments would rightly
pull high-impact access features forward. In this pre-launch
phase, those signals inform the design, but they do not have to
dictate the order of construction.

The ordering principle for this stretch is **software
composition**. The goal is to **minimize cumulative rework
across the whole tree before launch**: choose the order that
lets later ADRs build on settled foundations instead of
revisiting earlier work because a later ADR changed a core
shape. A feature wants to come *earlier* when it settles a
foundational type or invariant (`Identity`, `NodeId`, slab
footer, descriptor fingerprint) that many later features build
on. A feature wants to come *later* when it is a consumer or
refinement of those surfaces and composes cleanly once they are
stable.

This does not make secondary indexes, CDC, or LSM compaction
less important. They remain central warehouse capabilities.
The sequencing claim is narrower: they should consume the
algebraic substrate (`StateDigest`), lifecycle layer
(tombstones, retention), topology decision, and identity /
descriptor cleanup rather than force those foundations to be
retrofitted afterward. The trade is real — substantial council
bandwidth goes to foundation work before the first access-
pattern ADR opens — but it buys a cleaner final system.

## Forward sequence (council-converged)

Decided across multiple iterations between Claude, Codex, and
Gemini.

### Algebraic state foundation

1. **`StateDigest` substrate** *(brainstorm 014, narrowed.)*
   ADR 046 promoted — see
   `plans/adr/046_state_digest_substrate.md` and
   `plans/impl/046_state_digest_substrate.md`. Substrate-only
   surface: `StateDigest` newtype, `StateDigestAccumulator`,
   `StateDigestElement`, domain-separated projection (hash-to-
   scalar over Ristretto255), V1 helper for `table.row.visible`
   only. No durable storage placement; consumer ADRs commit
   placement when they ship. Substrate implementation cleared
   the cut-2 promotion bench gate; consumer-ADR-driven
   follow-on tasks remain (durable placement, anti-entropy
   reader). Brainstorm 014 marked harvested.

### Lifecycle (validates the algebra)

2. **Row-level tombstones on append-only.** ADR 047 promoted
   — see `plans/adr/047_row_tombstones_append_only.md` and
   `plans/impl/047_row_tombstones_append_only.md`. Tombstones
   share the target's `__row_key_id__`; one new synthetic
   column (`__row_tombstone__`); `is_tombstone` bit on index
   entries; tombstone-only multi-row records (no user
   columns); reuse ADR 040 `tombstone_metadata`; one footer
   bit (`has_row_tombstone_records`, subject to Phase-0
   audit; correctness-equivalent fallback if no clean slot
   exists); visible-domain `StateDigest` subtraction with no
   durable placement. First non-speculative validation of
   ADR 046 `subtract_element` against real storage state.
   Implementation in place; full 100k / 1M bench matrix is the
   remaining shipped-verification gate.

3. **Per-table retention policy.** ADR 048 promoted — see
   `plans/adr/048_per_table_retention_policy.md` and
   `plans/impl/048_per_table_retention_policy.md`.
   Replicated `TableLifecycleCatalog` keyed by
   `(tenant_id, table_object_id)`; three modes
   (`InheritCluster` / `Disabled` /
   `FixedWindow(RetentionWindowMillis)`); typed
   `SetTableRetentionPolicy` mutation with monotonic
   `policy_generation` CAS — no DDL parser in Wendelon.
   Two-phase: read-time logical filter + hard `as_of <
   floor` reject; compaction-time physical eviction
   pinned to `compaction_start_hlc`. ADR 047 pair-GC
   composes against the table floor; ambiguous ownership
   retains. Visible-domain `StateDigest` reversal
   (subtract on data eviction, add on tombstone eviction)
   — no new digest domain. PK-keyed table-specific
   retention deferred to roadmap #6. Implementation in
   place; full 100k / 1M bench matrix is the remaining
   shipped-verification gate.

### Topology gate (closed)

4. **Algebraic topology** *(brainstorm 015 — closed.)* Closed
   2026-04-29 with **Outcome B**: opaque `NodeId` is
   sufficient; Wendelon binds node signing identity through
   epoch membership state when a real verifier exists, rather
   than reshaping `NodeId` itself. The closure is durable but
   not permanent — future ADRs that need cryptographic node
   identity must cite the rework budget recorded in the
   brainstorm 015 status header. Splits: brainstorm 018
   (FROST-WAL authority) and brainstorm 019 (Sybil-resistance
   threat model). Both are placeholders, off the active queue.
   Anti-entropy remains deferred until identity / descriptor
   cleanup (#5–#7) settles.

### Identity / descriptor cleanup before access patterns

5. **Content-hash identity (`AppendOnlyContentHash`).** ADR 049
   promoted — see
   `plans/adr/049_content_hash_row_identity.md` and
   `plans/impl/049_content_hash_row_identity.md`. Third
   `RowIdentityPolicy` variant; sparse type-tagged canonical
   encoding (NFC strings; NaN / -0 normalized; stable column-
   ID `column_tag`); domain-tagged BLAKE3 preimage excluding
   schema generation; synchronous admission-time collapse
   keeping ADR 046 §5 visible-state semantics intact;
   `IdentityProjection` enabled, `ColumnProjection` deferred;
   non-NULL-default and decimal-scale-evolution rejected with
   typed reject codes. Implementation phases pending.

6. **PK-keyed row-delete via supersession.** Tombstone hides
   earlier versions of the same key under supersession
   discipline. Reuses the row-tombstone shape but layers
   supersession on top. *3–4 iterations.*

7. **Logical / physical descriptor fingerprint split.** Splits
   logical schema identity from physical descriptor shape
   before access-pattern ADRs accrete onto the fingerprint.
   *2–3 iterations.*

### Access patterns

8. **Secondary indexes on non-clustering columns.** Index-entry
   triple `(slab_id, record_location, record_row_index)`.
   Index segments carry `StateDigest` from day one — replica
   reconciliation is O(1) point comparison. The ADR
   distinguishes physical locator entries from logical index
   membership. *5–7 iterations.*

9. **Wire-frame compression (transport-side IPC).** Lands
   before CDC so streaming surfaces inherit transport-side
   compression instead of inventing a stream-local contract.
   *2–3 iterations.*

10. **CDC + triggers** *(brainstorm 004.)* Append-only is the
    canonical CDC sink. Cursor surface decision lands here
    (HLC-derived? `__row_key_id__`-derived? tuple?); consumers
    verify cursor checkpoints against partition `StateDigest`
    points algebraically. *Large, multi-ADR.*

11. **LSM-style leveled compaction.** The compactor reads
    multi-row segments, drops rows matched by tombstones,
    generates new secondary index segments, updates
    `StateDigest` points. Append-only segments are LSM-clean by
    construction. *Large, multi-ADR.*

12. **Vector embeddings + vector-form identity/locality PBC
    opcodes** *(brainstorm 002.)* ANN as a first-class index
    family, composing on the secondary-index substrate (#8) and
    LSM (#11). *Large, multi-ADR.*

### Encoding / compute / durability

13. **Encoding, fetch, and distributed compute refinements.**
    Bundle: LCE V2 / V3 (brainstorm 011 — homerolled SIMD
    encodings, COCD); mini-page row-group fetch (refines
    ADR 044 from buffer to row-group level); per-buffer page
    directory; zstd capacity-tier codec + `CompressionLevel`
    newtype; serverless PBC L2 (brainstorm 010); L1 inline
    erasure coding (brainstorm 003). *Multi-quarter, several
    ADRs.*

### Refinement plumbing (opportunistic)

14. **Small ADRs, no ordering constraint.** AGG follow-ups
    (float canonical, Decimal SUM/AVG, GROUP BY *(medium-
    large)*, string/binary quantiles, exact COUNT(DISTINCT),
    `COUNT(DISTINCT __row_key_id__)` shortcut, CSB visibility-
    metadata consolidation). Hilbert locality variant.
    Append-only `SingleRow` low-latency path. `IntraBatchIndex`
    widening to u64. Provider-native multi-range API in
    `ObjectStore`. `RecordBufferDirectory` cache.

## What not to do next

- **Don't add bytes to the slab footer without first deciding
  what they mean.** Footer size and HMAC coverage are
  deliberately constrained; existing locator/hash space and
  projected-read integrity assumptions punish ill-specified
  additions. The `StateDigest` ADR settles meaning before
  placement.
- **Don't bundle the topology implementation into the
  `StateDigest` ADR.** They are independent algebra layers;
  bundling produces a mega-ADR that defers a real decision.
- **Don't pre-allocate `__row_tombstone__`,
  `AppendOnlyContentHash`, FROST-signature, or any other
  follow-up shape into the `StateDigest` ADR's surface.** Per
  `no-premature-backward-compat.md`, each follow-up lands its
  own surface.
- **Don't open secondary indexes before the substrate /
  lifecycle / topology / identity / descriptor work above
  lands.** If the council reverses direction, record the new
  sequence in this roadmap before opening the secondary-index
  ADR; otherwise stay on the converged ordering.
- **Don't promote stabilization watch items into roadmap
  items.** Stabilization watch — observability metrics,
  architectural-test guards, post-merge dashboards — lives in
  runbooks and tests, not as a roadmap feature. Decision-
  enabling measurement (e.g., the `StateDigest` cold-path
  benchmark) belongs inside the owning ADR.
- **Don't re-open the topology gate without citing the rework
  budget recorded in brainstorm 015's closure status.**
  Speculative threat models, aesthetic preferences, and the
  desire to reuse curve machinery are not sufficient grounds.
  A re-open requires concrete verifier requirements or
  operational evidence that opaque-handle plus membership-
  bound signing forces more rework than a `NodeId`-shape
  migration.

## Convergence summary

```text
StateDigest substrate
  → tombstones (validate algebra @ deletion)
  → retention (validate algebra @ compaction)
  → topology gate CLOSED — Outcome B (opaque NodeId)
  → content-hash identity (ADR 049)
  → PK-keyed row delete (next: roadmap #6)
  → descriptor fingerprint split
  → secondary indexes
  → wire-frame compression
  → CDC
  → LSM
  → vector embeddings
  → encoding / fetch / distributed compute
  → refinement plumbing
```

Design tip is past ADR 049 with content-hash identity
*promoted* (design accepted; impl phases pending). Next ADR
is PK-keyed row delete via supersession (#6); reuses the
row-tombstone shape (ADR 047) layered with supersession
discipline. ADR 046 substrate cleared its bench gate; ADR
047 / 048 / 049 have quick-smoke or pending bench artifacts
and need full matrices before declaring shipped.
