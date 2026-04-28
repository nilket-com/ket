# Roadmap: from 045 forward (transient nav doc)

Author: Claude (AI Swarm Council)
Scope: transient navigation — replaces `roadmap-post-044.md`,
retires whenever `plans/wip/` is cleaned up again.
Written after ADRs 035, 036, 037, 038, 039, 040, 041, 042,
043, 044, 045 shipped.

## Where we are

The warehouse story now spans eleven shipped ADRs:

- **ADR 035** — vectorized predicate-bytecode (PBC) executor.
- **ADR 036** — PBC as the versioned Toron→Wendelon
  predicate-pushdown contract.
- **ADR 037** — typed tenant tables, PK-derived row identity,
  `TableSpreadProjection`.
- **ADR 038** — column-aware locality V1 (Z-order),
  descriptor-authoritative.
- **ADR 039** — slab-footer column statistics V1.
- **ADR 040** — aggregate pushdown protocol (AGG); CSB
  short-circuit with `tombstone_metadata`.
- **ADR 041** — project-name decoupling from internal
  protocols and types.
- **ADR 042** — analytical multi-row slab fragments.
- **ADR 043** — Arrow IPC body compression (LZ4_FRAME).
- **ADR 044** — L2 columnar fetch (sovereign pushdown).
- **ADR 045** — PK-less append-only tables.
  `RowIdentityPolicy` replaces the mandatory PK field on
  `TableSchemaDescriptor`; new `AppendOnly` variant
  synthesizes `__row_key_id__` from authenticated write
  context — `BLAKE3("table.append-row" || tenant_id ||
  table_object_id || writer_node_id || row_hlc ||
  intra_batch_index_be || row_payload_hash)` — with no
  table-local replicated counter and no new synthetic
  storage column. Append-only tables hard-require
  `MultiRow` layout (composes with ADR 043's
  layout-driven `Lz4Frame` default and ADR 044's
  column-projected fetch). Sealed append-only slabs
  populate `supersession_metadata = tracked None`,
  collapsing the supersession axis for the CSB short-
  circuit. Row-level tombstones / `__row_tombstone__`
  remain explicitly deferred to a follow-up ADR.

What ADR 045 commits the platform to (this commit is doc
promotion; the runtime targets below land phase-by-phase
per `plans/impl/045_pk_less_append_only_tables.md`):

- **Coordination-free identity for streams.** Append-only
  ingest uses information the write path already has
  (authenticated `WriterNodeId` from ADR 018, per-row
  HLC from ADR 012, intra-batch position). No
  per-table replicated counter. Identity is
  deterministic by replicated apply order and recovery
  is mechanical — `__row_key_id__` is read directly
  from the synthetic-column trio and the redb identity
  index rebuilds without PK canonicalization.
- **Identical-payload preservation.** Two heartbeats
  with identical bytes at distinct write contexts are
  two rows. The `(writer_node_id, row_hlc,
  intra_batch_index)` tuple in the identity preimage
  guarantees this; the payload hash binds identity to
  row bytes for diagnostics, not dedupe.
- **Full ADR 042/043/044 substrate composition.** Append-
  only tables ship on the multi-row + LZ4 + column-
  projected-fetch path with no special-case code.
  Append-only is the canonical workload ADR 044's
  planner thresholds were tuned for; cold-tier scans
  inherit ADR 044's bytes-fetched targets.
- **No new authentication, no new wire frame, no new
  capability bit, no envelope `format_version` bump.**
  Append-only is descriptor-driven; mixed PK / append-
  only fleets serve both shapes identically. The
  `CreateTable` wire shape extends with
  `row_identity_policy` and the storage path branches
  on it; no other surface changes.
- **Descriptor fingerprint break, no compat shim.**
  `primary_key: PrimaryKeyDescriptor` is folded into
  `RowIdentityPolicy::PrimaryKey(...)`. Per
  `no-premature-backward-compat.md`, the type just
  changes; existing tables re-fingerprint on schema-
  load. No `Option<PrimaryKeyDescriptor>` coexistence
  field, no `#[serde(default)]` shim.

What 045 explicitly did *not* do, and why it matters for
the roadmap:

- **No row-level tombstones / `__row_tombstone__`
  column.** The shape is sketched in 045's "Deferred
  Shape: Row Tombstones" section so the follow-up ADR
  has a concrete starting point — `__row_tombstone__:
  Bool` synthetic column, `has_tombstones` ADR 022
  footer bit so ADR 044 can skip the buffer on
  steady-state slabs, `DeleteRows { row_key_ids }`
  request shape, scan-local visibility set,
  retention-floor compaction GC. None of this lands in
  V1. Until the follow-up ADR ships, append-only
  retention is at the table / range / slab lifecycle
  level (ADR 042 §1).
- **No content-hash identity (`AppendOnlyContentHash`).**
  Idempotent ingest with deterministic dedupe-on-write
  is its own design surface (canonicalization spec,
  dedupe-semantics contract). Pulled by exactly-once
  / dedupe-on-write workloads.
- **No replicated `AppendOrdinal` counter.** Rejected
  for the per-table serialization point + recovery
  reconciliation cost. CDC cursor benefit is
  recoverable from `(__row_hlc__, __row_key_id__)` if
  a future CDC ADR needs it.
- **No public point-read API by HLC or
  `intra_batch_index`.** Storage-internal in V1; CDC
  ADR (brainstorm 004) owns whatever cursor surface
  appears.
- **No new storage-only synthetic column.** ADR 042's
  trio is sufficient.
- **No `AppendOrderProjection` locality variant.**
  `ColumnProjection` over an event-time column already
  covers time-locality.
- **No append-only `SingleRow` fast path.** Hard-
  rejected by the descriptor validator. Future workload
  pull may reopen.

## The near map

```
                                          YOU ARE HERE
                                              ▼
     ┌──────────────────────────────────────────────────────────────┐
     │ 035 / 036 / 037 / 038 / 039 / 040 / 041 / 042 / 043 / 044 / 045 │ ← shipped
     └────────────────────────┬─────────────────────────────────────┘
                              │
                  ┌───────────┼────────────────┐
                  │           │                │
                  ▼           ▼                ▼
           stabilization    ADR 046         row-tombstones
           (1–2 weeks)     secondary         on append-only
                            indexes         (sketched in 045)
                                │                │
                                ▼                ▼
                   ┌────────────┴────────────┴───────────────┐
                   │            post-046 menu                │
                   │ ordered by compound value                │
                   └────────────┬────────────────────────────┘
                                ▼
                content-hash identity → per-table retention →
                  AGG follow-ups → wire-frame compression →
                  mini-page row-group fetch → Hilbert →
                  LSM leveled compaction → LCE V2 / V3 /
                  brainstorms
```

ADR numbering: 045 shipped at the position previously
labeled "045 (proposed) — PK-less append-only tables."
046 (secondary indexes) stays at its post-044 number.
The append-only row-delete follow-up gains a tentative
047 slot (numbered at opening); content-hash identity
follows it (or runs in parallel — independent design
surfaces).

## Immediate (1–2 weeks post-runtime-rollout)

Once the impl plan's phases land and append-only ingest
is rolling out on the fleet, let 045 settle. New watch
items the path introduces, plus carryovers from earlier
ADRs.

### New under 045

1. **Append-only adoption.**
   `storage_append_only_batches_total{tenant}` per
   tenant. Initial fleet rollout is opt-in via
   `APPEND ONLY` clause; adoption rate over time
   signals workload pull.
2. **Ingest throughput vs. PK-keyed baseline.**
   `storage_append_only_rows_written_total /
    storage_typed_table_rows_written_total` per tenant
   on tables of comparable shape. Target from impl
   Phase 7: within 10% of PK-keyed baseline; a *win*
   is plausible because identity synthesis is cheaper
   than PK canonicalization for many schemas. Bench
   evidence drives whether to keep the payload hash in
   the identity preimage (Open question 1).
3. **Payload-hash CPU share.**
   `storage_append_only_payload_hash_micros`
   distribution. If BLAKE3-on-row dominates ingest CPU
   on representative wide-row workloads, surface for
   council to revisit dropping the hash from the
   preimage.
4. **Identity-synthesis CPU.**
   `storage_append_only_identity_synthesis_micros`
   distribution. BLAKE3 over the ~100-byte preimage;
   expected trivial. Long tail = a writer with an
   unusual `IntraBatchIndex` shape; investigate.
5. **`IntraBatchIndex` exhaustion.**
   `storage_append_only_intra_batch_index_exhaustion_total`
   should remain zero in steady state. Non-zero =
   single write request larger than 4 G rows; rare,
   but the metric existing tells operators where to
   look. If sustained nonzero, escalate to the `u64`
   widening decision (Open question 2).
6. **Supersession-skip count.**
   `storage_append_only_supersession_metadata_skipped_total`
   should be nonzero on every append-only sealed slab.
   Zero = the writer is incorrectly populating
   `supersession_metadata` for append-only; regression
   bug.
7. **CSB direct-answer rate on append-only.**
   `agg_csb_short_circuit_total{policy="append_only"} /
    agg_request_total{policy="append_only"}`. Append-
   only is the workload that benefits *most* from CSB
   pruning; the ratio should run materially higher
   than on PK-keyed tables once stat-prunable
   predicates land at scale.
8. **Append-only ADR 044 column-fetch metrics.**
   On append-only tenants,
   `storage_l2_column_fetch_record_body_bytes_avoided_total`
   should track ADR 044's Phase-11 targets within
   bench noise. Append-only is the canonical 044
   workload — narrow projections over wide schemas
   should drive `bytes_avoided_total` high and
   `phase_a_round_trips` near 1.0.
9. **Recovery throughput.** Time-to-recover on a
   100M-row append-only corpus vs. PK-keyed
   baseline. Expected within 10% (or faster — no PK
   canonicalization, no counter reconciliation).
10. **Recovered key-identity coverage.** A cross-check
    metric: post-recovery redb-index size on append-
    only tables should equal the row count read from
    raw slab bytes. Drift indicates a recovery bug or
    a `__row_key_id__` write-path bug.
11. **Architectural-test enforcement.** New guards
    landed in 045 stay green:
    - No new node capability bit
      (`NodeCapability::*Append*` /
      `NodeCapability::*Tombstone*` greps empty).
    - No descriptor field added beyond the
      `primary_key` → `row_identity_policy` rename
      (architectural-test asserts no
      `Option<PrimaryKeyDescriptor>` coexistence
      shim).
    - No slab-format-version bump.
    - No `__row_tombstone__` introduced (deferred to
      follow-up ADR).
    - No `__row_append_ordinal__` /
      `TableAppendState` / `AppendOrdinal` /
      `reserve_append_ordinals` symbols
      (architectural-test guards against the
      rejected counter approach being reintroduced).
    - No `_` catch-all match arm for
      `RowIdentityPolicy`.
    - Compactor source does not call
      `synthesize_append_only_row_id`.
    - No project-name-prefixed surfaces.
12. **Operator opt-out rate.** Tenants flipping
    declared tables from `APPEND ONLY` back to
    PK-keyed (or vice versa) is an operational
    signal worth tracking — high rate suggests the
    table-shape default isn't matching the tenant's
    workload pull.
13. **Tenant ciphertext quota deltas.** Tenants on
    append-only tables should see *no* quota change
    from ADR 045 alone (it's an identity-policy and
    write-admission change; on-disk shape is
    unchanged from PK-keyed `MultiRow + Lz4Frame`).
    A delta is a metric / accounting bug.

### Carryover from 044

Still relevant; append-only is the canonical 044
workload, so 044's Phase-12 watch items are *the*
047-era observability signal:

- Strategy distribution
  (`storage_l2_column_fetch_requests_total{strategy}`).
- Fallback-reason distribution
  (`storage_l2_column_fetch_fallback_total{reason}`).
- Bytes saved on cold reads
  (`storage_l2_column_fetch_record_body_bytes_avoided_total`).
- Phase A round-trip count, skip ratio, truncated-
  window refetch.
- Auth-error rate (must stay zero).
- L2 GET-rate cost.
- Cold-tier latency p50 / p99.
- Architectural-test guards from 044.
- Operator opt-out rate for `L2ColumnFetchConfig::enabled`.

### Carryover from 043

- Compression density
  (`storage_compression_ratio{codec="lz4_frame"}`).
- Decode CPU on admitted reads (should *fall* on
  column-projected reads).
- Encode CPU on writes — append-only writes carry
  the same encode cost as PK-keyed multi-row writes;
  no delta.
- Convergence backlog (uncompressed-resident-bytes
  decay).
- Decode error rate (must stay zero).
- Stored ciphertext quota deltas.
- Architectural-test guards from 043.

### Carryover from 042

- Layout selection sanity
  (`storage_multirow_rows_per_record` distribution).
  Append-only tables overwhelmingly land in `MultiRow`
  by hard requirement; the distribution should reflect
  ingest batching not policy selection.
- Compaction load
  (`storage_multirow_recompact_total` rate vs. write
  rate).
- Migration backlog drain
  (`storage_multirow_uncompacted_row_records_total`).
- Supersession-tracking adoption
  (`agg_csb_short_circuit_skipped_supersession_metadata_untracked_total`).
- Recovery correctness on mixed
  PK / append-only / mixed-compression slab corpora.
- Architectural-test guards from 042.

### Carryover from 040 / 041

- AGG leverage and CSB short-circuit ratio.
  Append-only's permanent visibility-completeness
  should drive a *step-change* improvement in
  short-circuit hit rate on stat-prunable
  predicates.
- Capability fallback (must remain zero).
- Numeric overflow and KLL state size.
- TieredSlab `tombstone_metadata` adoption
  (unchanged by 045 — that field is for slab-level
  visibility, not row-level delete).
- Architectural guard against project-name
  references.

### Carryover from 038 / 039

- Stats prune ratio. Pair with the new
  `storage_append_only_*` metrics: append-only
  slabs are the densest CSB-prune target because of
  the supersession axis collapse.
- CSB size and decrypt latency.
- Locality clustering effectiveness.
- Width-budget pressure on declared clustering
  columns.

## ADR 046 (proposed) — Secondary indexes on non-clustering columns

**What:** Declared secondary indexes mapping
`column_value → (slab, record_location,
record_row_index)`. Composes naturally with 042's
multi-row records and 044's column-projected fetch —
secondary indexes can point at a specific row inside a
multi-row record without changing the multi-row format,
and the fetch from the indexed slab uses ADR 044's
column-projected path.

**Composition with 045:**

- Append-only tables are prime secondary-index targets.
  Wide schemas, no PK, scan-heavy lookups by user
  predicate columns. ADR 046's index entries point at
  `(slab_id, record_location, record_row_index)` and
  resolve to a column-projected fetch under 044.
- The index-entry triple shape is identical for
  PK-keyed and append-only sources; ADR 046 doesn't
  need an append-only special case beyond
  acknowledging the supersession axis collapse on
  append-only sources (no version-hiding to
  reconcile).
- ADR 046 partially fills the "no PK point reads on
  append-only" consequence ADR 045 explicitly
  accepted: a secondary index on an event-id user
  column gives append-only tables a non-PK point-
  lookup path.

**Why next (was 046 in post-044, stays at 046):** With
clustering (038), stats (039), aggregates (040),
multi-row (042), LZ4 compression (043), L2 column
fetch (044), and append-only (045) shipped, predicates
not covered by clustering are the remaining slow path.
Secondary indexes are the right tool of last resort —
their cost is real (write amplification, separate
index management), so they're opt-in per column per
table. No Toron pull has surfaced yet; pulled when
measured.

**Design surface:** Same five points the post-044
roadmap listed (storage shape, granularity, sync vs.
async maintenance, planner integration, tombstone /
supersession composition) plus one new one specific
to 045:

6. **Append-only composition.** Index entries on
   append-only tables never need version-hiding (no
   supersession), but they *will* need
   tombstone-aware visibility once the row-tombstone
   follow-up ADR ships. Sequencing matters: the
   secondary-index ADR can land before or after the
   row-tombstone ADR; if before, it adds a
   forward-compat note about how index entries
   compose with `__row_tombstone__`.

Size estimate: medium-large. Council session: 4–6
iterations.

## Immediate follow-up to 045: row-level tombstones

The "Deferred Shape: Row Tombstones" section of ADR 045
sketches the follow-up shape:

- Append `__row_tombstone__: Bool` synthetic column
  to `AppendOnly` storage IPC schemas (gated on the
  identity policy).
- Add `has_tombstones: bool` reserved bit in the ADR
  022 footer so ADR 044 can skip the tombstone buffer
  on steady-state slabs.
- Add `DeleteRows { row_key_ids: Vec<RowKeyId> }`
  request shape (extends existing write frame).
- Per-tenant `TombstoneRetentionFloor` config.
- Visibility filter: build a per-scan
  `HashSet<RowKeyId>` from rows where
  `__row_tombstone__ = true`; drop both tombstone
  markers and matching originals.
- Compaction GC drops tombstone / original pairs once
  HLC is below the retention floor.

Pulled by GDPR / audit-trail row-delete needs. Likely
lands shortly after 046 (secondary indexes) — the two
ADRs compose at the visibility-filter layer (a
secondary-index lookup against a tombstoned row should
return the tombstoned status, not the original). If
GDPR / compliance pressure outpaces secondary-index
demand, this ADR moves up the stack.

Size estimate: medium. Council session: 3–5 iterations.

## Post-046 menu (ordered by compound value)

### Row-level tombstones on append-only

Sketched above; first follow-up to 045 by natural
composition.

### Content-hash identity (`AppendOnlyContentHash`)

A third variant of `RowIdentityPolicy` for idempotent
ingest with deterministic dedupe-on-write. Needs:

- A canonical Arrow row encoding spec (covering
  NULL handling, integer width, decimal scale,
  timestamp precision, NaN equivalence).
- A position on whether identical-payload duplicates
  collapse silently or surface as a writer-visible
  reject.
- A migration path? Probably "no migration; new
  tables opt in" — existing append-only tables don't
  switch identity policy mid-flight.

Pulled by exactly-once / dedupe-on-write workloads
(CDC sinks with at-most-once semantics, idempotent
event pipelines).

Size estimate: medium-large. Council session: 4–6
iterations.

### PK-keyed row delete via supersession

Different semantics from append-only delete (the
tombstone hides earlier versions of the *same key*,
possibly across multiple slabs, under supersession
discipline). Reuses 045's row-tombstone follow-up
shape but layers supersession on top.

Pulled by tenants who need both PK semantics and
row-delete capability. Sized around the row-tombstone
follow-up — they share most machinery.

Size estimate: medium.

### Per-table retention policy

Pairs naturally with append-only: `RETENTION = '90
DAYS'` drops rows where `__row_hlc__ < now - 90d` at
compaction time. ADR 042's single-coherent-compaction
primitive carries it.

Size estimate: small-medium. Pulled by tenant
retention needs.

### AGG follow-ups (multiple, individually small)

ADR 040 deferred several aggregate-protocol
extensions. Each is its own ADR when a workload
pulls:

- Float canonical encoding (resolves ADR 039 §15's
  deferral; unblocks `sum(float)` / `avg(float)`).
- Decimal SUM/AVG (pinned `Decimal256` overflow and
  scale contract).
- Grouped aggregation (`GROUP BY`) — large,
  multi-iteration ADR.
- String/Binary quantiles.
- Exact `COUNT(DISTINCT)`.
- CSB visibility metadata consolidation (unify
  `tombstone_metadata` and `supersession_metadata`
  into a single struct on the TieredSlab pointer).
- `COUNT(DISTINCT __row_key_id__)` short-circuit on
  append-only — trivially `count(*)`. Tiny.

Most are independent and can land in any order.

### Wire-frame compression (transport-side IPC)

Storage-resident only today; control-server schema
fetch and data-server raw-batch responses still ship
default-options Arrow IPC. Future transport ADR
under the ADR 011 transport family.

Size estimate: small-medium.

### Mini-page row-group fetch (sub-record row pruning)

ADR 044 V1 fetch granularity is the Arrow buffer
(≈column) level. Future ADR adds row-group
granularity. Pulled when post-044 metrics show
selective predicates account for a meaningful
fraction of cold-read latency. Multi-quarter design
surface for the homerolled-encoding path
(brainstorm 011 V2 sub-piece).

### Vector-form identity / locality PBC opcodes

ADR 042 retired the scalar `LOAD_IDENTITY` /
`ROUTE_BY_LOCALITY`. Future ADR introduces vector-
form opcodes that consume `__row_key_id__` and
`__row_routing_locality__` as Arrow inputs. Works
identically on PK-keyed and append-only sources.

Pulled by Toron planner needing per-row
identity / locality inside a predicate.

Size estimate: small-medium.

### Per-buffer page directory for O(1) point-read decompression

Brainstorm 011 V2 sub-piece. May land independently
if point-read amplification metrics on `Lz4Frame`
tables surface real cost.

### Compression-level tunable / zstd capacity-tier codec

ADR 043 deferred. Pulled by L2 capacity-cost
evidence dominating L1 read-bandwidth.

### Per-column compression policy

Lands inside LCE V2 (brainstorm 011), not as an
extension to ADR 043.

### Provider-specific batch / multi-range API in `ObjectStore`

ADR 044 deferred. Pulled by cross-provider bench
evidence showing meaningful win on richer multi-
range shapes.

### `RecordBufferDirectory` cache

ADR 044 deferred. Pulled by repeat-cold-read
evidence.

## Hilbert curve (deferred)

ADR 038 left `ProjectionFunctionV1::Hilbert` parsed-
but-rejected. Future Hilbert ADR lifts the rejection
when locality refinement matters more than warehouse-
infrastructure progress.

## LSM-style leveled compaction (deferred)

Forward-compat analyses through 042 / 043 / 044 / 045
all confirm: nothing in the substrate hostility-tests
LSM. Append-only segments in particular are LSM-clean
by construction (no overlapping keys, no version
chains). LSM hits append-only first when the time
comes — easiest case. ADR opens when measured
write-amp pressure justifies it.

## Longer-range brainstorms (multi-ADR streams)

Each is its own council session when pulled by real
workload demand.

- **Brainstorm 002 — Vector embeddings as first-class.**
  ANN search integrated rather than sidecar. Composes
  with 038's locality, 039's stats, 040's AGG, 042's
  multi-row layout, 043's per-buffer LZ4, 044's
  column-projected fetch, and 045's append-only
  shape (vector ingest pipelines are append-only by
  construction).

- **Brainstorm 004 — Real-time CDC and triggers.**
  Native streaming from the Raft WAL. ADR 045
  promotes brainstorm 004 from "future work" to
  "natural next problem": append-only is now the
  canonical CDC sink shape, with a stable
  `__row_key_id__` surviving WAL serialization.
  When brainstorm 004 opens, the cursor question
  (HLC-derived? `__row_key_id__`-derived?
  `(hlc, key_id)` tuple?) is the first thing the
  ADR settles. ADR 045's open question 3 explicitly
  defers this to brainstorm 004.

- **Brainstorm 010 — Serverless PBC L2.** Cold-
  storage compute. Substrate is in place since
  ADR 044; append-only adds nothing structural here
  (Lambda execution sees the same chunk-aligned
  authenticated range-read primitive regardless of
  identity policy).

- **Brainstorm 011 — Lightweight Columnar Encodings
  (LCE).** V1.5 (Arrow IPC LZ4 body compression)
  shipped as ADR 043. V2 (homerolled SIMD encodings)
  and V3 (compute-over-compressed-data) remain the
  long-term scope. Pulled when post-044 metrics show
  the remaining bandwidth / memory headroom is
  worth the multi-quarter design surface.

## Status at a glance

| ADR | Scope | Status |
|---|---|---|
| 035 | Vectorized PBC executor | ✓ shipped |
| 036 | PBC pushdown contract | ✓ shipped |
| 037 | Tenant table schemas, row identity, `TableSpreadProjection` | ✓ shipped |
| 038 | Column-aware locality V1, Z-order | ✓ shipped |
| 039 | Slab-footer column statistics V1 | ✓ shipped |
| 040 | Aggregate pushdown protocol | ✓ shipped |
| 041 | Project-name decoupling refactor | ✓ shipped |
| 042 | Analytical multi-row slab fragments + supersession metadata | ✓ shipped |
| 043 | Arrow IPC body compression (LZ4) | ✓ shipped |
| 044 | L2 columnar fetch (sovereign pushdown) | ✓ shipped |
| 045 | PK-less append-only tables | ✓ shipped |
| 046 (proposed) | Secondary indexes on non-clustering columns | next pickup |
| 047+ (candidate, unordered) | Row-level tombstones on append-only | sketched in 045's deferred section |
| 047+ (candidate, unordered) | Content-hash identity (`AppendOnlyContentHash`) | deferred — pull from idempotent-ingest workload |
| 047+ (candidate, unordered) | PK-keyed row delete via supersession | deferred — pulled by PK-keyed delete demand |
| 047+ (candidate, unordered) | Per-table retention policy | deferred — append-only's natural companion |
| 047+ (candidate, unordered) | Wire-frame compression (transport-side IPC) | deferred — pull from network egress cost |
| 047+ (candidate, unordered) | Mini-page row-group fetch (sub-record row pruning) | deferred — pull from selective-predicate latency |
| 047+ (candidate, unordered) | Vector-form identity / locality PBC opcodes | deferred — pull from workload |
| 047+ (candidate, unordered) | Float canonical encoding | deferred — pull from workload |
| 047+ (candidate, unordered) | Decimal SUM/AVG | deferred — pull from workload |
| 047+ (candidate, unordered) | Grouped aggregation | deferred — pull from workload |
| 047+ (candidate, unordered) | String/Binary quantiles | deferred — pull from workload |
| 047+ (candidate, unordered) | Exact `COUNT(DISTINCT)` | deferred — pull from workload |
| 047+ (candidate, unordered) | `COUNT(DISTINCT __row_key_id__)` short-circuit on append-only | deferred — small AGG follow-up |
| 047+ (candidate, unordered) | CSB visibility metadata consolidation | deferred — small |
| 047+ (candidate, unordered) | Auto-triggered single→multi migration | deferred — pull from ops noise |
| 047+ (candidate, unordered) | zstd capacity-tier codec + `CompressionLevel` newtype | deferred — pull from L2 capacity cost |
| 047+ (candidate, unordered) | Per-buffer page directory for O(1) point-read decompression | deferred — pull from point-read amplification |
| 047+ (candidate, unordered) | Provider-native multi-range API in `ObjectStore` | deferred — pull from cross-provider bench |
| 047+ (candidate, unordered) | `RecordBufferDirectory` cache | deferred — pull from repeat-cold-read evidence |
| 047+ (candidate, unordered) | Logical / physical descriptor fingerprint split | deferred — pull from physical-only field accumulation |
| 047+ (candidate, unordered) | Append-only `SingleRow` low-latency path | deferred — pull from low-latency stream workload |
| N (future) | Column-Aware Locality V2, Hilbert variant | deferred behind AGG follow-ups |
| N (future) | LSM-style leveled compaction | deferred behind write-amp pressure |
| N (brainstorm 002) | Vector embeddings first-class | deferred, multi-ADR |
| N (brainstorm 004) | CDC + triggers | deferred, multi-ADR — ADR 045 makes this the natural next problem |
| N (brainstorm 010) | Serverless PBC L2 (node-pushdown) | deferred, multi-ADR |
| N (brainstorm 011) | LCE V2/V3 (custom SIMD encodings + COCD) | deferred, multi-ADR — V1.5 shipped as ADR 043 |

ADR numbers 047+ are candidate ordering; actual numbering
happens at opening.

## What to not do next

Carryovers from the 044-era roadmap, plus 045-specific
additions.

### From 044

- **Don't open the LSM ADR speculatively.** Wait for
  measured write-amp pressure. Append-only segments are
  LSM-clean by construction; LSM ADR will hit them
  first when it opens.
- **Don't enable `MultiRow` cluster-wide on PK-keyed
  tables.** It's a per-table opt-in for a reason
  (point-heavy tables pay the amplification tax).
  Append-only is the *exception* — multi-row is hard-
  required there.
- **Don't auto-trigger `SingleRow → MultiRow`
  migration** for PK-keyed tables. ADR 042 §13 stays
  operator-driven only.
- **Don't reintroduce scalar identity/locality PBC
  opcodes.** ADR 042 retired them; future
  identity/locality predicates use vector-form opcodes.
- **Don't open every brainstorm at once.** Each is its
  own council session.
- **Don't promote Hilbert ahead of AGG follow-ups,
  046, or row-tombstones.** Locality refinement, not
  warehouse infrastructure.
- **Don't enable `Lz4Frame` on point-heavy `SingleRow`
  workloads cluster-wide.**
- **Don't auto-flip compression policy.** Operator
  feedback is the path.
- **Don't pre-allocate `Zstd` / `Snappy` / `Vortex`
  enum slots.**
- **Don't enforce `max_record_bytes` against
  compressed bytes.**
- **Don't add a `ColumnFetchPolicy` descriptor field**
  (rejected in 044).
- **Don't break the `ObjectStore` trait for native
  multi-range APIs in V1.**
- **Don't add a `RecordBufferDirectory` cache
  speculatively.**
- **Don't fabricate dummy null arrays** for unfetched
  columns in column-projected fetch.
- **Don't let AEAD / footer-HMAC failures fall back
  to whole-record fetch.**
- **Don't auto-tune `min_record_bytes` per query.**
- **Don't pre-allocate fallback-reason enum slots
  beyond the V1 set.**
- **Don't open Brainstorm 010 (serverless PBC L2)
  before ADR 044 stabilization metrics are in.**
- **Don't bundle column-fetch into ADR 046's scope
  opportunistically.**

### New under 045

- **Don't reintroduce the replicated table-local
  counter design.** ADR 045 rejected
  `TableAppendState` / `reserve_append_ordinals` for
  the per-table serialization point and recovery
  reconciliation cost. Architectural test guards
  against the symbols reappearing. If a future ADR
  needs a per-table monotonic, it argues for it on
  its own merits — not as a tactical add-on.
- **Don't auto-default `RowIdentityPolicy` based on
  schema shape.** The descriptor declaration is the
  authoritative signal; auto-classifying tables as
  append-only based on the absence of a PK column is
  a hidden-magic anti-pattern.
- **Don't bundle row-tombstones into ADR 046**
  (secondary indexes) opportunistically. The follow-
  up ADR for row-tombstones is its own design surface
  with its own retention / visibility / compaction-GC
  contract. ADR 046 composes with row-tombstones via
  documented interfaces; it doesn't ship them.
- **Don't expose `__row_key_id__` /
  `intra_batch_index` as user-visible columns.** The
  CDC ADR (brainstorm 004) owns whatever cursor
  surface is appropriate; pre-baking it into ADR 045
  or 046 forecloses CDC design choices.
- **Don't auto-derive append-only locality from
  schema heuristics** (e.g. "this table has an
  `event_time` column → silently pick
  `ColumnProjection(event_time)`"). The descriptor
  declares; the validator resolves; whatever
  default ADR 045 ultimately ships is the only
  default.
- **Don't pre-allocate `__row_tombstone__`,
  `__row_append_ordinal__`,
  `AppendOnlyContentHash`, or any other follow-up
  shape in ADR 045's surface.** Convention violation
  per `no-premature-backward-compat.md`. Each
  follow-up ADR lands its own surface.
- **Don't promote `IntraBatchIndex` overflow to a
  regular operating mode.** The exhaustion metric
  exists for visibility into a pathological writer
  shape; if a workload genuinely needs > 4 G rows
  per request, the conversation is "widen to `u64`,"
  not "add a wraparound code path."
- **Don't auto-flip `RowIdentityPolicy` based on
  retention pressure.** A tenant deciding append-
  only isn't matching their workload re-creates the
  table with a different policy; the platform does
  not silently migrate identity policy under the
  tenant.
- **Don't add a node capability bit for append-only.**
  Mixed PK / append-only fleets serve both shapes;
  nothing to negotiate.
- **Don't promote ADR 045's open questions to ADR
  scope before bench / impl-discovery / council /
  follow-on-ADR resolution.** Of the five surviving:
  #1 is bench-resolvable in Phase 7; #2 is
  council/policy; #3 defers to brainstorm 004; #4
  reopens on workload pull; #5 is implementer
  discovery in Phase 2. None are contract-level.

ADR 045 is meant to compose, not expand. The roadmap
beyond it is workload-pull, not feature-push.
