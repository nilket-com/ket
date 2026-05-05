# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

**Update cadence.** The roadmap is rewritten at planning
sessions, not at implementation commits. The council operates on
a block-chain rhythm: plan → implement → plan → implement, and
this file is a snapshot of the most recent plan. Between
sessions the roadmap lags the code on purpose — an item can read
"implementation phases pending" while the implementation is
already merged on `main`, or "full 100k / 1M bench matrix
remains ahead" while a smoke run has already landed. That is
expected, not a discrepancy. Don't open a session by flagging
the gap as if it were news; check `git log` and the relevant
impl plan / bench-results file for actual status, and roll
roadmap edits into the next planning session along with the
forward-sequence rewrite.

## Where we are

ADR 055 (LSM-Style Leveled Compaction Substrate) just
promoted. Closes the leveled-merge / manifest / pin /
elision-floor / digest-invariance shape question for
physical compaction. Substrate-only scope: size-tiered
fallback, subcompaction, bloom filters, cross-replica
deduplication, manual-trigger RPC, and L4+ extension are
explicit follow-up ADRs. Per-locality compactor on the
locality leader; four levels (`L0..L3`); HLC-disjoint
within each level (HLC is the data-ring slab seek key
per ADR 045 / 050 / 052; partitioning on the seek key
keeps binary-search-on-manifest cost constant). Internal
`CompactionMergeKey = (row_hlc, row_key_id,
record_row_index)` is storage-local and never crosses
into CDC schema. Per-locality manifest mirror is local
and rebuildable, but mutations ride a new
`MutationKind::CompactionCommit` on the existing
locality control ring (ADR 016) — readers see an atomic
epoch boundary and followers receive the mutation for
failover-safe synchronization. Compaction is
**deterministic** so each replica reproduces the same
logical output set from its local input slabs;
cross-replica logical equality holds via determinism
(visible-state digest matches), physical bytes stay
local, replication traffic stays bounded to manifest
mutations. **The elision floor IS ADR 048's retention
floor** — no separate compaction floor, CDC floor, or
operator override. Two reachability contracts would
diverge under operational drift; one floor, one
contract. Elision rules are inherited verbatim from the
existing single-slab primitives lifted to a streaming
multi-slab merger: append-only / content-hash
data+tombstone pair-GC (ADR 047 / 049), PK prefix
pruning with **boundary-sentinel retention** (ADR 050
§7), migrated-range tombstone reclaim, fail-closed on
ambiguous table ownership. Sidecar rebuild composes via
ADR 052 §9 — every output slab gets a freshly-built
sidecar, **replace-by-new** (never in-place rewrite),
digest-verified before old sidecars retire. CDC catchup
acquires manifest-snapshot iteration pins at open;
those pins survive concurrent compaction commits. Live-
tail handoff reads ADR 054's in-memory ring and does
not pin slabs. **No `relocated_reads` shim** — pins
make old and new manifest epochs independently
readable; sidecars rebuild during compaction so cached
`RecordLocation` references in old sidecars retire with
the old sidecars. Subscriptions below retention floor
reject with the existing
`CDC_REJECT_BELOW_RETENTION_FLOOR`. **No
`safe_emission_hlc` coupling** — that watermark gates
emission from the in-memory live-tail ring (upstream of
any compaction decision); by the time a version-entry
reaches a slab eligible for compaction, it's been
finalized, emitted (or skipped past) the live-tail
ring, and the ring entry has dropped. The roadmap's
"compaction must not advance below cursor_high_water
when sidecars are being rewritten in-place" is
satisfied by **not rewriting sidecars in place**; the
"ring overflow rejects cleanly" branch is the existing
`CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP` (ADR 054).
Visible-state `StateDigest` invariance audited
unconditionally at every commit via cold rebuild over
input/output *manifest views localized to touched
slabs* — O(input + output bytes), not O(table); ADR
046's monoidal digest makes the localized comparison
equivalent to whole-table comparison. Cold rebuild over
views catches both elision bugs (drops a visible row)
*and* materialization bugs (synthesizes a visible row);
streaming `visible_contrib(elision_set) == identity`
only catches elision and was rejected as the V1 install
gate. Per-version-entry digest
(`digest.secondary_index.entry.v1`, ADR 052) updated
algebraically on the elision stream; sampled cold-
rebuild verification at
`lsm_digest_verify_sample_rate`. Mismatch aborts the
commit, emits `LsmInvariantViolation`, pauses
compaction on the affected `(locality, table)` scope
until manual investigation — page-grade. Three V1
reject codes (`ManifestRace`,
`SidecarDigestMismatch`, `VisibleDigestInvariance`),
discriminants start at 1 so a zero-init reject can't
decode as valid; closed taxonomy. Bounded `lsm_*`
metrics with `{level, result, kind}` enumerations; no
tenant / table / subscription / slab_id labels. V1
defaults: `max_l0_slab_count = 4`,
`lsm_l0_max_age_millis = 5_000`,
`lsm_l1_slab_size_target_bytes = 64 MiB`,
`lsm_size_ratio = 10`, `max_l1_slab_count = 4`,
`lsm_picker_tick_millis = 100`,
`lsm_gc_tick_millis = 1_000`,
`lsm_compaction_concurrency_per_locality = 1` —
yielding L1 ≤ 256 MiB, L2 ≤ 2.56 GiB, L3 ≤ 25.6 GiB
per locality. L3 has no next level in V1. Active slab
is never a compaction input. Pause/resume via existing
node-local maintenance config, not a new wire
protocol. No new wire `MsgType`, no new `FRAME_FLAG_*`,
no new `NodeCapability` bit, no Cargo feature, no new
digest domain, no `ObjectStore` trait change, no on-
disk format change beyond the slab-manifest extension
carried by `CompactionCommit`. No `StorageTier` field
on the LSM substrate — existing tier sweeper handles
L1=local / L2=tiered moves orthogonally. See
`plans/adr/055_lsm_leveled_compaction_substrate.md`
and `plans/impl/055_lsm_leveled_compaction_substrate.md`.
Implementation phases pending; full small / medium /
large bench evidence is the promotion-to-shipped gate.

ADR 054 (CDC Subscription Substrate) is the immediate
predecessor and the binding ADR 055 inherits.
Identity-axis logical cursor `(row_hlc, row_key_id,
record_row_index)` matching the data-ring slab seek
key; in-memory per-locality `CdcLiveTailRing` with
admission-time staging gated by ADR 003
`FinalizationCommitted`; catchup over a safe global
cut `snapshot_hlc = min(locality_finalized_hlc(l))`
using full-tuple `CdcCursor` bounds; safe-emission
watermark gates live-tail emission; locality-scoped
heartbeat (one worker per locality, one marker per
quiet interval); visible-domain `StateDigest`
checkpoints; 8-code `CDC_REJECT_*` taxonomy; bounded
`cdc_*` metrics; `Live` and `SnapshotAtOpen`
subscription bounds. See
`plans/adr/054_cdc_subscription_substrate.md` and
`plans/impl/054_cdc_subscription_substrate.md`.
Initial implementation phases landed; full bench
evidence remains the promotion-to-shipped gate; smoke
artifact at
`plans/impl/054_cdc_subscription_substrate_bench_results.md`.

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

This does not make LSM compaction, vector embeddings, or
encoding refinements less important. They remain central
warehouse capabilities. The sequencing claim is narrower: they
should consume the algebraic substrate (`StateDigest`),
lifecycle layer (tombstones, retention), topology decision,
identity / descriptor cleanup, access-pattern surface
(secondary indexes), wire-compression policy, and CDC substrate
rather than force those foundations to be retrofitted
afterward.

## Forward sequence (council-converged)

Decided across multiple iterations between Claude, Codex, and
Gemini. Steps 1–11 are promoted (in `plans/adr/`); the
forward shape begins at #12.

1. **`StateDigest` substrate** — ADR 046 promoted.
2. **Row-level tombstones on append-only** — ADR 047 promoted.
3. **Per-table retention policy** — ADR 048 promoted.
4. **Algebraic topology gate** — closed 2026-04-29 with
   Outcome B (opaque `NodeId`); see brainstorm 015 closure.
   Splits to brainstorm 018 (FROST-WAL authority) and
   brainstorm 019 (Sybil-resistance threat model), both
   off the active queue.
5. **Content-hash identity (`AppendOnlyContentHash`)** —
   ADR 049 promoted.
6. **PK-keyed row-delete via supersession** — ADR 050
   promoted.
7. **Logical / physical descriptor fingerprint split** —
   ADR 051 promoted.
8. **Secondary indexes on non-clustering columns** —
   ADR 052 promoted.
9. **Wire-side Arrow IPC body compression** — ADR 053
   promoted.
10. **CDC subscription substrate** — ADR 054 promoted.
    Substrate-only; triggers, PBC filter pushdown,
    webhooks, and continuous historical-plus-live queries
    are explicit follow-up ADRs.
11. **LSM-style leveled compaction substrate** — ADR 055
    promoted. Substrate-only; size-tiered fallback,
    subcompaction, bloom filters, cross-replica
    deduplication, manual-trigger RPC, L4+ extension,
    and direct already-tiered compaction are explicit
    follow-up ADRs (each consumes the substrate). See
    *Where we are* above.

12. **CDC follow-up ADRs (consume ADR 054 substrate).**
    Independent; each ships its own contract surface:
    - **PBC filter pushdown on subscriptions.** Trailing-
      section bytecode on `CDC_SUBSCRIBE_REQ`; new
      `CDC_WATERMARK` MsgType for low-selectivity streams
      (every emitted event advances the cursor at
      substrate, but watermarks become load-bearing once
      filters can drop events without advancing); filter
      reject taxonomy; budget accounting; no-match
      checkpoint advancement. Reuses ADR 036 PBC validator
      family.
    - **Continuous historical-plus-live queries.** Either
      composes externally (L2 read at HLC=T plus CDC
      subscription with cursor matching T) or introduces a
      dedicated streaming `QUERY_REQ` / `QUERY_RESP` shape.
    - **Post-commit PBC triggers.** New `TriggerCatalog`
      (mirroring ADR 052 `SecondaryIndexCatalog` shape),
      `SchemaObjectKind::Trigger`, trigger worker that runs
      PBC over CDC events and appends to descriptor-bound
      append-only sinks; sink-write idempotency; indirect
      cycle detection; pause-resume operator surface.
    - **Synchronous PBC triggers at admission.**
      Admission-time PBC hook with its own CPU budget and
      failure-domain rules; deterministic PBC requirement.
    - **Webhook worker + `__webhook_events__` system table
      (or equivalent).** Internal HTTP retry / DLQ
      semantics; webhook worker is a CDC consumer.
    - **Server-side durable cursor commit (consumer-group
      semantics).** New `CDC_CURSOR_COMMIT_REQ` / `_RESP`
      MsgType pair; substrate cursor unchanged.
    - **Exactly-once delivery contract.** Server-side
      cursor commit + per-frame ack RPC.
    - **Cross-replica cursor handoff (zero-reconnect
      failover).** Server-side durable cursor + replica
      synchronization.
    - **Multi-table / tenant-firehose subscription scope.**
      Cursor extends to carry `TableObjectId` explicitly;
      V1's two-field scope is forward-compatible.
    - **Before-image CDC payloads.** Adds `before_*`
      columns to the CDC event row schema; substrate
      cursor / digest domain unchanged.
    - **Event-history digest domain
      (`digest.cdc.event.v1`).** Adds prefix-membership
      proof when a consumer (e.g., downstream secondary-
      index replicator) needs it independent of visible
      state.
    - **Consumer-friendly higher-level `CdcEventKind`
      view.** Layered on top of substrate version-entries;
      derives insert / update / delete / resurrection
      semantics from row-axis synthetic columns.
    - **Per-table CDC opt-in / opt-out** (and per-table
      retention extension beyond source-table retention).
    *Each is its own ADR; ordering opportunistic.*

13. **Vector embeddings + vector-form identity / locality
    PBC opcodes** *(brainstorm 002.)* ANN as a first-class
    index family, composing on the secondary-index
    substrate (#8) and LSM (#11). *Large, multi-ADR.*

14. **Encoding, fetch, and distributed compute
    refinements.** Bundle: LCE V2 / V3 (brainstorm 011 —
    homerolled SIMD encodings, COCD); mini-page row-group
    fetch (refines ADR 044 from buffer to row-group
    level); per-buffer page directory; zstd capacity-tier
    codec + `CompressionLevel` newtype; serverless PBC L2
    (brainstorm 010); L1 inline erasure coding
    (brainstorm 003). *Multi-quarter, several ADRs.*

15. **Refinement plumbing (opportunistic, no ordering
    constraint).** AGG follow-ups (float canonical,
    Decimal SUM/AVG, GROUP BY *(medium-large)*,
    string/binary quantiles, exact COUNT(DISTINCT),
    `COUNT(DISTINCT __row_key_id__)` shortcut, CSB
    visibility-metadata consolidation). Hilbert locality
    variant. Append-only `SingleRow` low-latency path.
    `IntraBatchIndex` widening to u64. Provider-native
    multi-range API in `ObjectStore`.
    `RecordBufferDirectory` cache.

## What not to do next

- **Don't bundle the CDC follow-up ADRs into one
  mega-ADR.** Each consumer of the ADR 054 substrate
  (filters, triggers, webhooks, continuous queries,
  durable cursor commit, multi-table firehose, before-
  images, event-history digest) has its own contract
  surface; bundling defers real decisions behind a larger
  one. Land them independently against the substrate.
- **Don't introduce a separate compaction floor.** ADR
  055 §"Architectural inextricability" pins the elision
  floor as identical to ADR 048's retention floor. A
  parallel parameter would diverge under operational
  drift and create two reachability contracts.
  Operator-tunable retention is the lever; "compact
  more aggressively than retention permits" is not a
  shape V1 supports.
- **Don't add an LSM `relocated_reads` shim.** ADR 055
  §7 commits to manifest-snapshot pins as the sole MVCC
  mechanism; cached `RecordLocation` references retire
  with the old sidecars (rebuilt out-of-place during
  compaction). Reintroducing per-row relocation rows
  for LSM would add chain-management complexity without
  improving CDC catchup or iterator safety.
- **Don't replace the cold-rebuild visible-domain audit
  with a streaming-only check.** ADR 055 §6 / §9 commit
  to localized cold rebuild over input/output slab
  views as the V1 install gate — equivalent O(touched
  bytes) cost, strictly stronger bug-catching (catches
  materialization, not just elision). Streaming
  `visible_contrib(elision_set) == identity` is a debug
  / bench tool, not the gate.
- **Don't widen `CompactionMergeKey` with locality /
  tenant / table prefix.** Compaction is per-locality
  by construction; table ownership is read from row-
  axis synthetic columns at retention-floor lookup, not
  from the merge key. The narrow `(HLC, RowKeyId,
  RecordRowIndex)` matches the data-ring slab seek key
  exactly.
- **Don't put `StorageTier` on the LSM substrate.**
  Tier moves are an orthogonal sweeper concern. Coupling
  them would force the substrate to coordinate with the
  sweeper without buying capability.
- **Don't bundle the LSM follow-up ADRs.** Size-tiered
  picker, subcompaction, bloom filters, cross-replica
  deduplication, manual-trigger RPC, L4+ extension, and
  direct already-tiered compaction each consume the ADR
  055 substrate independently. Bundling defers real
  decisions behind a larger one.
- **Don't promote stabilization watch items into roadmap
  items.** Stabilization watch — observability metrics,
  architectural-test guards, post-merge dashboards — lives
  in runbooks and tests, not as a roadmap feature.
  Decision-enabling measurement (e.g., the CDC watermark
  hold-time bench at multiple heartbeat cadences) belongs
  inside the owning ADR.
- **Don't re-open the topology gate without citing the
  rework budget recorded in brainstorm 015's closure
  status.** Speculative threat models, aesthetic
  preferences, and the desire to reuse curve machinery
  are not sufficient grounds. A re-open requires concrete
  verifier requirements or operational evidence that
  opaque-handle plus membership-bound signing forces more
  rework than a `NodeId`-shape migration.
- **Don't add a `CdcEventKind` enum to the substrate.**
  Consumer-friendly insert / update / delete / resurrection
  semantics layer on top in a future higher-level view ADR
  (#12 sub-bullet). The substrate stays storage-axis-
  aligned with the version-entry grain.
- **Don't expose `LocalityId` on the CDC cursor.** The
  identity-axis cursor is the load-bearing decision; the
  safe-emission watermark plus heartbeat contract is what
  makes global cursor monotonicity work without it.

## Convergence summary

```text
StateDigest substrate                         (ADR 046)
  → tombstones (validate algebra @ deletion)  (ADR 047)
  → retention (validate algebra @ compaction) (ADR 048)
  → topology gate CLOSED — Outcome B (opaque NodeId)
  → content-hash identity                     (ADR 049)
  → PK-keyed row delete via supersession      (ADR 050)
  → descriptor fingerprint split              (ADR 051)
  → secondary indexes                         (ADR 052)
  → wire-side Arrow IPC body compression      (ADR 053)
  → CDC subscription substrate                (ADR 054)
  → LSM-style leveled compaction substrate    (ADR 055)  ← here
  → CDC follow-up ADRs (filters, triggers, webhooks, …)
  → vector embeddings
  → encoding / fetch / distributed compute
  → refinement plumbing
```

Design tip is past ADR 055 with the LSM compaction
substrate *promoted* (design accepted; implementation
phases pending). The post-CDC / post-LSM cluster (#12)
is opportunistic — a CDC consumer ADR (filters,
triggers, webhooks, durable cursor commit, multi-table
firehose, before-images, event-history digest) can land
in any order, since each consumes the ADR 054 substrate
independently and none blocks the others on shape. The
council's lean on next pickup: a CDC consumer ADR if
the team has bandwidth for surface design (each is
independently scoped); vector embeddings (#13) if the
team prefers to settle the next large algebraic
substrate (ANN as a sidecar family on top of #8 and
#11) before the CDC consumer cluster.
