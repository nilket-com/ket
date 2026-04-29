# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward. Updated when
the tip moves: each ADR that ships shifts the chain by one.

## Where we are

ADR 048 (Per-Table Retention Policy) just promoted. Per-
`(tenant_id, table_object_id)` retention lives in a new
replicated `TableLifecycleCatalog` in the control-ring
state, never on the descriptor; reads decode at their
requested schema generation but resolve retention from the
always-latest catalog. Three modes: `InheritCluster`,
`Disabled`, `FixedWindow(RetentionWindowMillis)` (1 minute
to 100 years). Two-phase enforcement: a Phase A logical
filter at read time (`floor <= __row_hlc__ <= as_of_hlc`)
plus hard `RETENTION_FLOOR_EXCEEDED` admission reject for
`as_of < floor`, and Phase B physical eviction at
compaction pinned to `compaction_start_hlc`. ADR 047 pair-
GC composes against the table-specific floor; ambiguous
ownership during reclaim retains the row. CSB direct
answers are gated by per-slab retention completeness
(above-floor allowed; below-floor zero; straddle falls
back to row-level filter). PK-keyed table-specific
overrides reject (`RETENTION_REJECTED_PK_DISALLOWED`) and
remain on the cluster MVCC path until roadmap #6.
Visible-domain `StateDigest` reversal — second production
consumer of ADR 046 `subtract_element`, monotonic and
compaction-driven, completing the lifecycle validation
pass before the topology gate. See
`plans/adr/048_per_table_retention_policy.md` and
`plans/impl/048_per_table_retention_policy.md`.
Implementation pending (Phases 0–8).

ADR 047 (Row-Level Tombstones on Append-Only Tables) is
the immediate predecessor — the lifecycle algebra
ADR 048 composes against (pair-GC under the table floor).
ADR 046 (`StateDigest` substrate) supplies the algebra.
ADR 045 (PK-Less Append-Only Tables) supplies the
identity layer; `__row_table_object_id__` is the
ownership resolver retention reclaim uses to look up the
effective floor.

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
   placement when they ship. Phase 0–6 impl pending.

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
   Phases 0–10 impl pending.

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
   retention deferred to roadmap #6. Phases 0–8 impl
   pending.

### Topology gate

4. **Algebraic topology** *(brainstorm 015.)* With the algebra
   validated by two storage-state consumers, the council
   decides whether to commit the topology layer. Three possible
   outputs: implement immediately (becomes the implementation
   ADR; candidate implementation surfaces include curve-derived
   `NodeId`, proof of partition custody, Sybil-resistant
   verifiable VNode generation, topology-vs-transport split,
   and FROST-signed Raft WAL — the gate decides which couple to
   topology vs. live in their own authority layer); defer with a
   concrete rework budget every later distributed ADR cites;
   close the brainstorm. *2–3 iterations as a gate; 5–8 if it
   rolls into implementation.*

### Identity / descriptor cleanup before access patterns

5. **Content-hash identity (`AppendOnlyContentHash`).** Third
   `RowIdentityPolicy` variant. Canonical Arrow row encoding
   spec (NULL handling, integer width, decimal scale, timestamp
   precision, NaN equivalence); position on identical-payload-
   collapses-vs-rejects. *4–6 iterations.*

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

## Convergence summary

```text
StateDigest substrate
  → tombstones (validate algebra @ deletion)
  → retention (validate algebra @ compaction)
  → topology gate
  → content-hash identity
  → PK-keyed row delete
  → descriptor fingerprint split
  → secondary indexes
  → wire-frame compression
  → CDC
  → LSM
  → vector embeddings
  → encoding / fetch / distributed compute
  → refinement plumbing
```

Implementation tip is now past ADR 048: the lifecycle
validation pass is complete on the design side. Next is the
topology gate. ADR 046 / 047 / 048 implementation phases
remain pending; bench cold-path Ristretto255 against
existing multi-row / append-only slab batches early so the
storage-placement decision remains evidence-based for
consumer ADRs.
