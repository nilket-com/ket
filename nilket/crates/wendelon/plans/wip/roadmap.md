# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward. Updated when
the tip moves: each ADR that ships shifts the chain by one.

## Where we are

ADR 050 (PK-Keyed Row Delete via Supersession) just promoted.
First-class delete operation for `RowIdentityPolicy::PrimaryKey`
tables: `TableDeleteRows` accepts PK descriptors and emits a
tombstone version under the target's `__row_key_id__` when the
key is currently visible at `delete_hlc`. Tombstones reuse
ADR 047's `__row_tombstone__` synthetic column and tombstone-
only multi-row record packing **layout-independently** —
`SingleRow` PK tables emit multi-row tombstone-only frames so
no fake user-column values pollute stats / PBC / aggregates.
Supersession is emergent from HLC-ordered version chains:
`latest-as-of-T(K) = argmax_v(v.hlc | v.hlc ≤ T)`, with
tombstones as first-class versions. Resurrection is allowed —
a later data write supersedes a tombstone and re-adds the
visible-digest element. Synchronous admission-time visible-
state branching across four outcomes
(`InsertedNewVisible` / `UpsertedAlreadyVisible` /
`DeletedSupersedingVisible` / `DeleteAbsorbedNotVisible`)
keeps ADR 046 §5 visible-state semantics intact across both
delete *and* the symmetric insert / upsert digest discipline
(no `+element(K)` on upsert of an already-visible PK; no
`-element(K)` on delete of a not-visible PK). The visibility
predicate is identity-policy-aware via
`row_visible_for_admission`: append-only / content-hash use
ADR 049's retention-floor predicate; PK uses chain-prefix-
retention semantics where below-floor boundary versions remain
visible. CSB metadata composition extends to all admissible
transitions including the new resurrection case (tombstone →
data records `supersession_metadata` on the tombstone slab).
Wire response classification triple
(`deleted_count` / `already_deleted_count` / `not_found_count`)
makes idempotent absorb operator-readable. PK retention stays
on ADR 013 cluster-MVCC prefix retention with conservative
boundary-tombstone preservation; ADR 048's
`RETENTION_REJECTED_PK_DISALLOWED` gate remains in force, and
no full-chain key retirement in V1 (cross-slab evidence proof
deferred until secondary indexes #8 surface it).
`PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` removed as
unreachable; no new `PK_DELETE_REJECTED_*` family. No new
on-disk surface, no new HRFT frame family, no new digest
domain, no `format_version` bump. See
`plans/adr/050_pk_delete_supersession.md` and
`plans/impl/050_pk_delete_supersession.md`. Implementation
phases pending.

ADR 049 (Content-Hash Row Identity) is the immediate
predecessor — `RowIdentityPolicy::AppendOnlyContentHash`
deriving `__row_key_id__` from canonical row content, with
synchronous admission-time collapse and the `row_currently_visible`
helper that ADR 050 generalizes to `row_visible_for_admission`.
Sparse type-tagged canonical encoding makes additive nullable
schema evolution identity-neutral; non-NULL-default and
decimal-scale-evolution rejected with typed reject codes;
`IdentityProjection` enabled, `ColumnProjection` deferred. Full
100k / 1M promotion bench matrix cleared on 2026-04-29.

ADR 048 (Per-Table Retention Policy) — replicated
`TableLifecycleCatalog`; three modes (`InheritCluster` /
`Disabled` / `FixedWindow`); two-phase enforcement (read-time
logical filter plus hard `as_of < floor` reject; compaction-
time physical eviction). PK-table retention deferral remains
live (ADR 050 honored; not lifted). The topology gate was
closed alongside ADR 048 with **Outcome B** (opaque `NodeId`
retained); splits to brainstorm 018 (FROST-WAL authority) and
brainstorm 019 (Sybil-resistance threat model).

ADR 047 (Row-Level Tombstones on Append-Only Tables) is the
tombstone shape ADR 050 reuses on the PK chain. ADR 046
(`StateDigest` substrate) supplies the algebra. ADR 045 (PK-
Less Append-Only Tables) supplies the identity layer ADR 049
complements with content-hash identity and ADR 050 complements
with delete semantics; `__row_table_object_id__` is the
ownership resolver retention reclaim uses to look up the
effective floor. ADR 042 supplies `supersession_metadata`,
which ADR 050 §7 composes against for the resurrection case.

The implementation chain through ADR 049 is built and bench-
shipped. ADR 046 substrate cleared the cut-2 promotion bench
gate (see `plans/impl/046_state_digest_bench_results.md`).
ADR 047, ADR 048, and ADR 049 cleared their full 100k / 1M
promotion matrices on 2026-04-29; see the corresponding
bench-result files under `plans/impl/`. ADR 050 design is
accepted; implementation phases (Phase 0 contract freeze
through Phase 9 metrics / runbook / arch guards) and full
100k / 1M bench matrix remain ahead.

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
   Implementation in place; full 100k / 1M bench matrix
   cleared on 2026-04-29.

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
   retention remains deferred (ADR 050 explicitly kept
   `RETENTION_REJECTED_PK_DISALLOWED` in force; lift is a
   follow-up after ADR 050 alongside the full-chain
   key-retirement proof). Implementation in place; full 100k /
   1M bench matrix cleared on 2026-04-29.

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
   typed reject codes. Implementation in place; full 100k /
   1M bench matrix cleared on 2026-04-29.

6. **PK-keyed row-delete via supersession.** ADR 050 promoted —
   see `plans/adr/050_pk_delete_supersession.md` and
   `plans/impl/050_pk_delete_supersession.md`. PK delete reuses
   `TableDeleteRows` (engine boundary identity-based);
   tombstones reuse ADR 047 `__row_tombstone__` and multi-row
   tombstone-only record packing layout-independently;
   supersession via HLC-ordered version chain (no new index
   structure); synchronous admission-time visible-state
   branching for both delete and insert / upsert; policy-aware
   `row_visible_for_admission` (PK boundary versions remain
   visible at admission; append-only / content-hash retention
   semantics from ADR 049 preserved); CSB metadata composition
   for the resurrection case (tombstone → data records
   supersession on tombstone slab); PK retention stays on ADR
   013 cluster-MVCC prefix retention (ADR 048 PK gate remains
   in force); no full-chain key retirement in V1; conservative
   boundary-tombstone preservation; classification count
   triple on the wire response;
   `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` removed.
   Implementation phases pending.

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
  → PK-keyed row delete via supersession (ADR 050)
  → descriptor fingerprint split (next: roadmap #7)
  → secondary indexes
  → wire-frame compression
  → CDC
  → LSM
  → vector embeddings
  → encoding / fetch / distributed compute
  → refinement plumbing
```

Design tip is past ADR 050 with PK delete via supersession
*promoted* (design accepted; implementation phases pending).
Next ADR is the logical / physical descriptor fingerprint
split (#7), which inherits a settled PK-delete supersession
story from ADR 050. ADR 046 substrate cleared its bench gate;
ADR 047 / 048 / 049 cleared full 100k / 1M promotion matrices
on 2026-04-29; ADR 050 implementation phases (Phase 0
contract freeze through Phase 9 metrics / runbook / arch
guards) and full 100k / 1M bench matrix remain ahead.
