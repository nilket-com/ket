# ADR 050: PK-Keyed Row Delete via Supersession

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/0050_pk_delete_supersession.md`

## Status

Accepted

## Context

PK-keyed tables (ADR 037 + ADR 045) have identity-by-key:
`__row_key_id__` is derived from the canonical PK preimage and is
stable across versions. Multiple writes to the same PK form an MVCC
**version chain** keyed on `__row_key_id__`, ordered by
`__row_hlc__`. `latest-as-of-T(K)` reads pick the version with the
largest HLC `≤ T`.

ADR 042 added `supersession_metadata`: the slab-side fact required
for CSB direct-answer soundness when a newer data version hides an
older one. ADR 047 added row tombstones for `AppendOnly` and the
parallel `tombstone_metadata` fact. ADR 049 introduced synchronous
admission-time visibility branching with retention-floor awareness
via `row_currently_visible`.

ADR 048's V1 rule disallows per-table retention overrides for PK
tables (`RETENTION_REJECTED_PK_DISALLOWED`). PK rows continue to
follow ADR 013 cluster-MVCC prefix retention.

PK-keyed tables today reach insert, upsert, and time-travel reads.
**Delete is missing** — `TableDeleteRows` rejects PK descriptors
because ADR 047 deliberately scoped tombstones to append-only.

Three pressures shape this surface:

- **Operator-visible delete.** Idempotent, time-travel-aware,
  retention-aware, replay-safe.
- **Visible-state digest coherence.** ADR 046 §5's visible-row
  digest is over the visible row set. Pre-existing PK upsert paths
  may be adding `element(K)` blindly; left unfixed, repeated
  upserts inflate the digest. Delete cannot ship coherently
  without the symmetric insert / upsert discipline (§6).
- **CSB direct-answer soundness.** Both `tombstone_metadata` and
  `supersession_metadata` must be maintained across the new
  resurrection case (data → tombstone → data); without it, CSB
  short-circuits could return stale answers.

The roadmap places this at #6, immediately after content-hash
identity, before descriptor fingerprint split (#7), secondary
indexes (#8), and CDC (#10). Each inherits a settled supersession
story from this ADR.

The constraint from ADR 046 §7 (no new digest domain without a
consumer), `no-premature-backward-compat.md`, and
`no-version-suffixes.md` all hold.

## Non-goals (V1)

- **No new `RowIdentityPolicy` variant.** Delete is an operation,
  not an identity policy.
- **No predicate / range / truncate / cascade / column-level
  delete.** V1 is identity-batch delete.
- **No multi-table delete.** One `(tenant_id, table_object_id)`
  per request.
- **No new digest domain.** Visible-row digest auto-tracks
  supersession.
- **No async-only supersession path.** §4 commits to synchronous
  admission-time branching.
- **No new tombstone column.** Reuses ADR 047 `__row_tombstone__`.
- **No new HRFT frame family.** Reuses
  `TABLE_DELETE_REQ` / `TABLE_DELETE_RESP`.
- **No new node capability bit.**
- **No slab footer field.** ADR 047's `has_row_tombstone_records`
  covers PK tombstones too.
- **No `format_version`, envelope, or wire-frame bump.**
- **No `ObjectStore` trait change.**
- **No PK + content-hash combined identity.** ADR 049 *Deferred*
  §1 remains deferred.
- **No SQL parser work.** Wendelon does not parse DDL / DML.
- **No PK-by-PK-value wire variant.** §1 keeps the engine
  boundary identity-based.
- **No table-specific retention override for PK rows.** ADR 048's
  `RETENTION_REJECTED_PK_DISALLOWED` gate stays in force.
- **No full-chain key-retirement proof in V1.** Boundary
  tombstones may persist; retirement requires cross-slab evidence
  that secondary indexes (#8) are the natural place to surface.
- **No append-only-style data+tombstone pair-GC for PK chains.**
  PK retention is prefix-with-boundary-sentinel.
- **No composite-PK expansion.**
- **No reserved enum slots, compatibility shims, or V1/V2 type
  suffixes.**
- **No new `PK_DELETE_REJECTED_*` reject-code family.**

## Decision

### 1. PK delete reuses `TableDeleteRows`

The data-ring delete operation is unchanged at the wire level:

```rust
pub struct TableDeleteRowsRequestMeta {
    pub parent_write_id: ClientWriteId,
    pub client_hlc: HybridTimestamp,
    pub expected_epoch: u64,
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub schema_generation: u64,
    pub row_count: u32,
    pub durability: DurabilityLevel,
    pub await_finalization: u8,
}
```

The payload is `Vec<Identity>` — 32-byte `__row_key_id__` per row.
For a `PrimaryKey` table, each identity is the descriptor-bound
PK-derived `__row_key_id__`. Clients (Toron, SDK) compute the
canonical PK preimage to derive identity. The engine boundary is
**identity-based**, matching the existing hot point-read path.
PK-value-keyed ergonomics is a higher-layer concern; pulling it
into the storage boundary would duplicate validation before the
SQL / client contract exists.

### 2. A PK delete is a tombstone version in the PK chain

For `RowIdentityPolicy::PrimaryKey`, `DeleteRows` emits a tombstone
row under the target key identity when the key is currently
visible at `delete_hlc`:

```text
__row_key_id__              = PK-derived target key identity
__row_hlc__                 = delete_hlc (leader-merged commit HLC)
__row_routing_locality__    = inherited from latest data version
__row_table_object_id__     = descriptor.table_object_id
__row_tombstone__           = true
```

The tombstone has no user columns. It participates in the same
`(tenant_id, key_identity)` MVCC chain as data versions.

Visibility is latest-version-wins:

```text
latest <= as_of is data       -> visible
latest <= as_of is tombstone  -> hidden
no version <= as_of           -> not visible
```

This is **supersession**, not append-only event semantics. The
tombstone supersedes older data versions for reads at or after
`delete_hlc`. A later data write of the same PK supersedes the
tombstone and resurrects the logical row.

### 3. Layout-independent tombstone marker storage

PK tables may use `RecordLayoutPolicy::SingleRow` or
`RecordLayoutPolicy::MultiRow` for *data* rows. **Tombstone
markers always use ADR 047's tombstone-only multi-row record
shape**, regardless of the data-row layout.

Reasons:

- Tombstones have no user payload; a single-row data layout would
  force fabricating non-null user-column values that PBC,
  aggregates, column stats, and CSB inputs would have to filter.
- The existing multi-row synthetic columns already encode every
  field a tombstone needs.
- Scans and recovery already recognize tombstone-only records
  (ADR 047 path); PK delete avoids a parallel single-row format.

`routing_locality` is inherited from the current latest data row.
If the data row exists but its routing locality cannot be
recovered, delete admission fails closed with
`PBC_REJECT_LOCALITY_UNSUPPORTED`.

### 4. Synchronous admission-time visible-state branching with policy-aware visibility

For every PK-keyed write — insert, upsert, **or** delete —
admission performs a row-index lookup against current visible
membership and branches across four outcomes:

| Operation | K currently visible? | Outcome | Storage | Digest |
|---|---|---|---|---|
| Insert / upsert | No | `InsertedNewVisible` | data row | `+element(K)` |
| Insert / upsert | Yes | `UpsertedAlreadyVisible` | data row | no change |
| Delete | Yes | `DeletedSupersedingVisible` | tombstone | `-element(K)` |
| Delete | No | `DeleteAbsorbedNotVisible` | (none) | no change |

"Currently visible" is **identity-policy-aware**, expressed
through the policy-dispatched helper `row_visible_for_admission`:

```rust
pub fn row_visible_for_admission(
    descriptor: &TableSchemaDescriptorV1,
    tenant_id: TenantId,
    key_identity: Identity,
    admission_hlc: HybridTimestamp,
    effective_retention: &EffectiveRetention,
    row_index: &SlabManager,
) -> bool;
```

The helper dispatches by `RowIdentityPolicy`:

- For **`AppendOnly`** and **`AppendOnlyContentHash`**: visible
  iff the latest location at admission is not tombstoned AND
  `row_visible_under_retention(location.hlc, effective_retention)`
  is true. (ADR 049's existing predicate.)
- For **`PrimaryKey`**: visible iff
  `slab.lookup_as_of(tenant, key, admission_hlc)` is data (not a
  tombstone). Cluster-MVCC prefix retention preserves the newest
  below-floor version as a boundary sentinel that **is still
  currently visible**, so the retention-floor predicate is *not*
  applied at admission for PK rows.

A naïve port of ADR 049's helper that applied
`row_visible_under_retention` to PK boundary versions would
falsely mark a still-current row as not-visible — a delete on it
would silently absorb instead of producing the right tombstone.
The policy-aware helper is the load-bearing fix.

The lookup happens **synchronously at admission**. Async fold-up
would over- or under-count between admission and compaction,
breaking ADR 046 §5.

In-batch dedup: identical identities within one delete request
deduplicate to one tombstone (mirrors ADR 049's content-hash
in-batch absorb). **Tombstone-over-tombstone is unreachable from
admission**; recovery that observes such a chain fails closed or
keeps the earliest tombstone with a corruption metric.

The internal outcome enum:

```rust
pub enum PkAdmissionOutcome {
    InsertedNewVisible        { added_digest_element: bool },
    UpsertedAlreadyVisible    { added_digest_element: bool },
    DeletedSupersedingVisible { subtracted_digest_element: bool },
    DeleteAbsorbedNotVisible  { latest_was_tombstone: bool },
}
```

The `latest_was_tombstone` flag distinguishes
`already_deleted_count` from `not_found_count` in §5.

### 5. Wire response: classification counts

`TableDeleteRowsResponseMeta` returns three operator-readable
counts per request:

```text
deleted_count          // currently-visible data → tombstone emitted
already_deleted_count  // latest is tombstone → no emit
not_found_count        // no version at or before delete_hlc → no emit
```

`not_found_count` does **not** include a PK boundary version
below the cluster floor; under ADR 013 prefix retention that
boundary version is still the current sentinel and reaches the
`deleted_count` path normally.

### 6. PK insert / upsert digest discipline (symmetric counterpart)

ADR 050 is the first ADR to wire PK-keyed writes into the visible
digest. The discipline:

- An insert / upsert that finds `K` not currently visible adds
  `element(K)`.
- An insert / upsert that finds `K` currently visible does **not**
  add `element(K)`.
- Both cases physically write a new version record; only the
  digest accounting differs.

Without this, two upserts of the same PK push the digest to
`2 * element(K)` — no operationally meaningful interpretation
under ADR 046 §5. The delete path needs the visibility predicate;
the predicate's correctness for delete depends on it also gating
the insert / upsert digest add. Splitting them across ADRs leaves
PK-keyed digest accounting incoherent in the interim.

The implementation plan's Phase 0 includes a diagnostic fixture
that pins whether current `main` upsert is blind-add or
visibility-aware. Either way the ADR is correct; only Phase 3's
migration flavor differs.

### 7. CSB metadata composition under data ↔ tombstone transitions

ADR 042 `supersession_metadata` and ADR 047 `tombstone_metadata`
must be maintained across all admissible transitions:

| Previous latest | New latest | Metadata recorded on |
|---|---|---|
| Data | Data | `supersession_metadata` on old data slab (existing ADR 042) |
| Data | Tombstone | `tombstone_metadata` on old data slab (mirrors ADR 047) |
| Tombstone | Data (resurrection) | **`supersession_metadata` on the tombstone slab** (new) |
| None | Data | no slab metadata change (first version) |
| None / Tombstone | Tombstone | unreachable from admission (§4) |

Resurrection is the new case: a data write supersedes a prior
tombstone, so the tombstone slab records supersession metadata,
just as a data slab does when a later data version hides it.

CSB direct-answer eligibility is unchanged from ADR 042 / 047:

```text
seal_hlc <= as_of_hlc
AND tombstone_metadata is tracked
AND no tombstone at or before as_of_hlc
AND supersession_metadata is tracked
AND no supersession at or before as_of_hlc
AND retention-complete for the requested floor
```

If either domain is untracked or indicates a relevant change, the
aggregate path falls back to row-level filtering. PK delete and
resurrection extend the set of slabs that may report these
domains; the eligibility check itself does not change.

### 8. PK retention stays cluster-MVCC prefix retention

ADR 050 keeps ADR 048's V1 boundary for PK tables. PK rows use
the ADR 013 cluster MVCC retention floor; `FixedWindow` /
`Disabled` overrides remain rejected with
`RETENTION_REJECTED_PK_DISALLOWED`.

For each PK version chain:

- All versions with `row_hlc >= cluster_floor` are retained.
- The newest version below `cluster_floor` is retained as the
  **boundary version**.
- Older below-floor versions are eligible for pruning.

Tombstones participate as normal versions. If the boundary
version is a tombstone, retain it as the boundary sentinel — it
prevents older data versions on another slab or in L2 from
reappearing after the floor crosses the delete HLC.

This is deliberately more conservative than append-only pair-GC.
PK chains can contain many data versions and resurrection
sequences; removing a local data+tombstone pair is not a sound
proof that no older physical version exists elsewhere. A future
ADR may define a full-chain key-retirement proof, likely after
secondary indexes (#8) settle the cross-slab evidence model.

ADR 050 deliberately keeps lifecycle changes narrow: it settles
delete and supersession, not per-table retention. ADR 048's PK
retention deferral remains live; a future ADR may revisit it once
the delete model has been exercised in production and the
full-chain key-retirement proof is implementable.

### 9. `StateDigest` composition

Visible-row digest preimage is unchanged from ADR 046:

```text
visible_row_element(t, T, k) := scalar(BLAKE3(
    "digest.table.row.visible.v1"
    || t || T || k
)) * G
```

Visible-domain delta rules for PK chains:

```text
none/tombstone -> data       +element(K)
data -> data                 0
data -> tombstone            -element(K)
tombstone -> tombstone       unreachable from admission
none -> tombstone            unreachable from admission
```

Compaction and retention apply visible-domain deltas only when
the logical latest-visible membership changes. Folding every
physical data version as `+element(K)` would over-count upserts —
the §6 hazard. Boundary-version retention preserves visible state
across the floor by construction; no digest delta at
retention-pruning time under V1's conservative path.

A future ADR may allocate `digest.table.row.physical.v1` for
physical row-set comparison via ADR 046's reserved-tag mechanism.
This ADR does not allocate that domain.

### 10. Composition with ADR 047 / ADR 048 / ADR 049

- **ADR 047**: PK delete tombstones reuse `__row_tombstone__` and
  the multi-row tombstone-only record packing. Supersession (§2 /
  §4) is the qualitatively new behavior; ADR 047 has no version
  chain. ADR 047 pair-GC under per-table retention does not apply
  to PK chains.
- **ADR 048**: PK retention stays on cluster-MVCC prefix
  retention; `RETENTION_REJECTED_PK_DISALLOWED` gate remains in
  force.
- **ADR 049**: Out of V1 scope for combined identity. ADR 049's
  `row_currently_visible` helper is generalized to
  `row_visible_for_admission` with policy-aware dispatch (§4);
  the append-only / content-hash arm preserves ADR 049's
  predicate exactly, including the retention-window fix.
  Content-hash tables continue to use ADR 047 tombstones
  directly.

### 11. Time-travel reads through PK tombstones

`as-of T` resolution on PK-keyed tables, after this ADR:

- The version chain of `K` is scanned for the version with the
  largest `hlc ≤ T`.
- If that version is a row → return the row.
- If that version is a tombstone → `K` is deleted at `T`; the row
  is hidden from the read result.
- If no version has `hlc ≤ T` → `K` does not exist at `T`.

Same shape ADR 045 established for upsert version chains, with
tombstones now first-class versions.

### 12. Reject-code taxonomy

ADR 050 reuses or repurposes the existing reject taxonomy. **No
new `PK_DELETE_REJECTED_*` family.**

- `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` is **removed as
  unreachable** — every current identity policy supports
  tombstone markers after this ADR.
- `PBC_REJECT_LOCALITY_UNSUPPORTED` remains the fail-closed code
  when a visible delete target exists but its routing locality
  cannot be recovered.
- `PBC_REJECT_PK_NULL` remains for future PK-value delete
  wrappers; under V1's identity-based boundary, null PKs are
  client-side canonicalization failures.
- `RETENTION_REJECTED_PK_DISALLOWED` (ADR 048) remains in force —
  PK descriptors still reject `FixedWindow` / `Disabled`
  lifecycle policies.

There is no "key not found" reject. Missing and already-deleted
keys are successful absorb outcomes represented by §5 counts.

## Rationale

**Why no new `RowIdentityPolicy` variant.** Delete is an
operation, not an identity policy. A new variant would force
every PK table to opt in to "delete-enabled" shape with no
operational meaning.

**Why synchronous admission-time visibility branching.** ADR 046
§5's visible-row digest must reflect the visible row set. Async
dedup creates incoherent intermediate state. Same load-bearing
rationale ADR 049 §2 ships.

**Why policy-aware visibility helper.** Append-only / content-hash
"row visible" means "row exists, not tombstoned, above retention
floor." PK "row visible at admission" means "version chain has a
latest entry that is not a tombstone." Prefix retention has
already pruned the chain; below-floor boundary sentinels remain
visible. Forcing one helper to work for both invites silent bugs;
policy dispatch (one helper, two semantic arms) is the right
shape.

**Why the symmetric insert / upsert discipline lands here.**
Without it, PK-keyed digest accounting is incoherent. Splitting
produces a window where the digest does not reflect visible state
on PK tables — exactly the kind of intermediate state ADR 046 §7
deferral test rejects.

**Why reuse ADR 047 tombstone shape.** No new on-disk surface;
the column, packing, footer bit, and recovery path are all
shipped. The new behavior is admission and read-side
supersession.

**Why supersession through HLC ordering rather than a hide
marker.** The version chain is already HLC-ordered. A separate
hide marker needs either a new index structure or a new column on
tombstones to point at superseded versions. HLC ordering is the
cheapest mechanism that gives correct semantics.

**Why layout-independent tombstone marker frames.** A single-row
data layout would force fabricating user-column values that PBC,
aggregates, column stats, and CSB inputs would have to filter
out.

**Why CSB metadata maintenance becomes more subtle.**
Resurrection (tombstone → data) is novel relative to ADR 047's
append-only model. Recording `supersession_metadata` on the
tombstone slab is the symmetric move; without it, CSB direct
answers could short-circuit incorrectly.

**Why resurrection is allowed.** Existing PK writes are upserts.
Rejecting writes after a delete would introduce a new table-level
lifecycle state and a new user-visible command distinction. A
later data version superseding a tombstone is the natural MVCC
interpretation.

**Why no PK table-specific retention in V1.** ADR 048
deliberately kept PK rows on the cluster MVCC path. ADR 050
settles delete semantics but does not also prove the per-table
retention and full key-retirement model for PK chains. Mixing two
lifecycle changes in one ADR is the scope risk we minimize
against. Keeping the ADR 013 prefix rule lets the delete model
ship narrow, get exercised in production, and inform a follow-up
retention ADR.

**Why retain tombstone boundary versions.** Dropping a PK
tombstone too early can allow older data versions that still
exist on another slab or tier to become visible again. The
conservative prefix rule PK retention already uses is applied
symmetrically to data and tombstone versions.

**Why no full-chain key-retirement proof in V1.** The proof needs
cross-slab evidence — every L1 slab, every L2 pointer, and (after
roadmap #8) every secondary index segment — to be enumerated and
locked atomically. Without secondary indexes, the proof's
completeness is hard to argue. Defer to a follow-up.

**Why no PK-value delete frame.** The engine routes hot reads and
deletes by `Identity`. PK identity derivation is deterministic.
A PK-value wrapper is API ergonomics; pulling it into the storage
boundary duplicates validation before the SQL / client contract
exists.

**Why idempotent absorption.** Replaying a delete-stream from
cold storage or an external bus must not become an error storm.
Same operator-experience principle as ADR 049 §2.

**Why no predicate-driven delete in V1.** Different evaluation
shape (PBC scan vs index lookup); orthogonal scope.

**Why remove the append-only tombstone reject.** After this ADR,
all current row-identity policies support tombstone markers.
Keeping `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` would
preserve a dead branch and an inaccurate name. Future policies
that do not support tombstones can allocate a policy-neutral
reject when they exist.

## Consequences

- **Operator-visible delete on PK-keyed tables.** Idempotent;
  composes with retention and CSB; participates in time-travel.
- **PK-keyed visible-digest accounting becomes load-bearing.**
  First ADR to wire PK writes into the visible digest.
  Pre-existing blind-add upsert traffic, if any, is rectified in
  the impl plan's Phase 3.
- **Admission lookup cost on every PK write.** Marginal cost over
  the existing PK preimage hash.
- **Replay safety.** PK write streams are first-class idempotent.
- **Single-row PK tables gain a scan / recovery branch** that
  skips tombstone-only multi-row marker records.
- **CSB metadata maintenance is more subtle.** Three transitions
  each maintain their domain; the eligibility check is unchanged.
- **Boundary tombstones may persist** until full-chain retirement
  is defined. Deliberate storage-amplification trade.
- **`row_visible_for_admission` becomes the shared admission
  visibility boundary** and is explicitly policy-aware.
  Append-only / content-hash semantics are preserved; PK boundary
  versions are correctly visible at admission.
- **`PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` removed.**
  Unreachable after this ADR.
- **`RETENTION_REJECTED_PK_DISALLOWED` remains in force.** PK
  descriptors still reject `FixedWindow` / `Disabled` lifecycle
  policies.
- **No on-disk surface change.** Reuses every shipped fact.
- **CDC inherits a settled delete shape (#10).** Delete events
  surface as `__row_tombstone__`-bearing records.
- **Secondary indexes (#8) inherit settled supersession.** Index
  entries hidden when the latest version `≤ T` is a tombstone.
- **ADR 047 / 048 unchanged.** AppendOnly tombstones and PK
  retention rules preserved.

## Rejected alternatives

- **New tombstone column distinct from ADR 047.** Rejected.
  ADR 047 shape covers the on-disk side; supersession is admission
  / read-side, not on-disk.
- **New digest domain.** Rejected per ADR 046 §7 deferral test.
- **Async fold-up at compaction.** Rejected per ADR 049 §2.
- **Hard-error on delete-of-not-visible key.** Rejected. Defeats
  idempotent re-delivery.
- **Hide-marker as a separate index structure.** Rejected. HLC
  ordering on the existing version chain gives correct semantics
  at zero new index surface.
- **PK-value-keyed delete wire frame in V1.** Rejected. Engine
  boundary stays identity-based.
- **Single-row tombstone records on `SingleRow` PK tables.**
  Rejected. Forces fabricating user-column values.
- **Visible-digest add on every PK upsert (no synchronous
  visibility check).** Rejected. Inflates monotonically.
- **Bundle PK delete with predicate delete.** Rejected. Different
  evaluation shape (PBC scan vs index lookup).
- **Wait for descriptor fingerprint split (#7).** Rejected. The
  dependency direction is the other way: descriptor fingerprint
  split inherits a settled supersession story from this ADR.
- **Append-only-style data+tombstone pair-GC for PK chains.**
  Rejected. PK chains can have multiple data versions interleaved
  with tombstones; pair removal loses information.
- **Drop tombstone boundary versions aggressively without a
  proof.** Rejected. Risks resurrecting older data versions.
- **Lift ADR 048's `RETENTION_REJECTED_PK_DISALLOWED` in
  ADR 050.** Rejected. PK table-specific retention needs separate
  retained-boundary, digest-reversal, and policy-extension rules
  that this ADR does not establish.
- **Ship full-chain key retirement in V1.** Rejected. Cross-slab
  evidence proof is cleaner after secondary indexes (#8) define
  their visibility and reclaim model.
- **Single non-policy-aware visibility helper.** Rejected.
  ADR 049's predicate is wrong for PK boundary versions; the
  policy-aware `row_visible_for_admission` is the correct shape.
- **New `PK_DELETE_REJECTED_*` reject family.** Rejected.
  Delete-of-missing and delete-of-already-deleted are successful
  absorb outcomes; unsupported-tombstone rejection is now
  unreachable.
- **Keep `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` after
  this ADR.** Rejected. Every current identity policy supports
  tombstones; the code is dead.

## Resolved questions

These are the V1 decisions converged across Codex, Claude, and
Gemini. The implementation plan inherits them.

1. No new `RowIdentityPolicy` variant.
2. PK delete reuses `TableDeleteRows` wire frame; engine boundary
   identity-based.
3. Tombstone shape reuses ADR 047 `__row_tombstone__`.
4. Tombstone marker storage is layout-independent.
5. Synchronous admission-time visible-state branching for both
   delete and insert / upsert.
6. Visibility predicate is identity-policy-aware via
   `row_visible_for_admission(descriptor, tenant_id,
   key_identity, admission_hlc, effective_retention,
   row_index)`. Append-only / content-hash use the retention-floor
   predicate; PK uses chain-prefix-retention semantics (boundary
   versions are visible).
7. Wire response classification counts: deleted /
   already_deleted / not_found.
8. CSB metadata composition: data→data (supersession);
   data→tombstone (tombstone); tombstone→data (supersession on
   tombstone slab).
9. PK retention stays on ADR 013 cluster-MVCC prefix retention;
   ADR 048's `RETENTION_REJECTED_PK_DISALLOWED` gate remains in
   force.
10. No full-chain key retirement in V1; boundary tombstones
    persist until a follow-up ADR defines the cross-slab evidence
    proof.
11. No new digest domain; visible-row digest auto-tracks
    supersession.
12. Composition with ADR 047 / 048 / 049 unchanged at the on-disk
    level.
13. Idempotent absorption on delete-of-not-visible (with
    `latest_was_tombstone` distinction).
14. `PBC_REJECT_ROW_TOMBSTONES_REQUIRE_APPEND_ONLY` removed as
    unreachable.
15. No new `PK_DELETE_REJECTED_*` reject-code family.

## Open questions

V1 picks defaults; each can be revisited in a follow-up ADR
without on-disk surface change. Two implementation-discovery
items remain, both pinned to specific phases of the impl plan:

1. **Existing PK upsert digest behavior on `main`.** Phase 0
   diagnostic fixture pins whether the current path is blind-add
   or already visibility-aware. ADR text holds in either case;
   only Phase 3's migration flavor differs (no-op wiring vs
   retrofit-with-rectification).
2. **Bench-shape deltas.** Phase 8 isolates admission-lookup cost
   on delete vs insert / upsert vs the existing PK admission
   baseline. Acceptance thresholds refine after a smoke run;
   promotion-to-shipped requires the full 100k / 1M matrix.

## Forward-compatibility analysis

**Roadmap #7 descriptor fingerprint split.** Logical fingerprint
changes don't change PK identity (canonical preimage is
fingerprint-independent), so PK delete works identically across
fingerprint boundaries. Resurrection across a fingerprint change
is well-defined.

**Roadmap #8 secondary indexes.** Index entries for `K` are
hidden under supersession when the latest version `≤ T` is a
tombstone. Index reclaim follows the same conservative rule as
the data side; full-chain key retirement (when defined) becomes
the trigger for both data and index reclaim.

**Roadmap #10 CDC.** Delete events are version-chain end-of-life
records. Resurrection events surface as cursor-visible "data
after tombstone" sequences. Re-ingestion is naturally idempotent.

**Roadmap #11 LSM compaction.** Compaction folds tombstones into
the version chain at retention boundaries; the boundary-version
rule prevents older versions from re-appearing through compaction
reordering.

**Future per-table retention for PK tables.** A follow-up ADR may
lift `RETENTION_REJECTED_PK_DISALLOWED` once the delete model has
been exercised. Likely paired with the full-chain key-retirement
proof.

**Future predicate-driven delete.** Composes by adding a PBC scan
that produces `__row_key_id__` values, then the same admission
path runs over them.

**Future PK + content-hash combined identity.** Inherits a
settled supersession story from this ADR.

**Future PK-value-keyed wire frame.** API ergonomics ADR; reuses
the engine's identity-based admission.

## Deferred to follow-up ADRs

1. Predicate-driven delete (`DELETE WHERE …`).
2. Multi-table or cross-tenant delete batches.
3. Per-column delete (column-level removal on a still-visible
   row).
4. PK + content-hash combined identity with supersession.
5. PK-value-keyed delete wire variant.
6. Per-table retention for PK tables (lift
   `RETENTION_REJECTED_PK_DISALLOWED`).
7. Full-chain key retirement proof for boundary-tombstone
   removal.
8. Anti-entropy reconciliation over PK-keyed visible digests.
9. Time-travel-aware secondary index reclaim under supersession.
10. CDC cursor surface for resurrection events.

## References

- **ADR 013** — cluster MVCC retention (the V1 PK retention rule
  inherited by §8).
- **ADR 037** — primary key descriptor.
- **ADR 040** — `tombstone_metadata` slab-side fact (origin).
- **ADR 042** — analytical multi-row fragments and
  `supersession_metadata` (the slab-side fact §7 composes
  against).
- **ADR 045** — `AppendOnly` write-context identity; the PK
  counterpart this ADR's delete semantics complement.
- **ADR 046** — `StateDigest` substrate; §5 visibility semantics
  drive §4's synchronous-branching commitment.
- **ADR 047** — row-level tombstones; §2 / §3 reuse
  `__row_tombstone__` and the multi-row tombstone-only record
  packing; §10 composition.
- **ADR 048** — per-table retention; §8 honors V1 PK gate.
- **ADR 049** — content-hash identity; §4's synchronous branching
  inherits ADR 049 §2's load-bearing lookup discipline; ADR 049's
  `row_currently_visible` helper is generalized to
  `row_visible_for_admission` with policy-aware dispatch.
- `plans/conventions/no-premature-backward-compat.md`.
- `plans/conventions/newtype-units.md`.
- `plans/conventions/no-version-suffixes.md`.
- `plans/conventions/capability-vs-feature-naming.md`.
- `plans/philosophy.md` — Identity-vs-Locality framing inherited.
