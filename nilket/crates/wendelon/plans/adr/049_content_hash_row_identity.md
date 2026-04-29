# ADR 049: Content-Hash Row Identity

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/049_content_hash_row_identity.md`

## Status

Accepted

## Context

ADR 045 established `AppendOnly` row identity from write
context:

```text
__row_key_id__ = BLAKE3(
    "table.append-row.v1"
    || tenant_id_slot
    || table_object_id
    || writer_node_id
    || row_hlc
    || intra_batch_index
)
```

That deliberately avoided hashing wide row payloads on the
hot path. The trade was that two separate writes of the same
logical row content produce different identities, because the
write context (`writer_node_id`, `row_hlc`,
`intra_batch_index`) differs between them. Idempotent
re-ingestion is therefore not available under `AppendOnly`.

ADR 046 added the `StateDigest` substrate. ADR 047 and ADR 048
validated visible-domain digest reversal under tombstones and
retention. But **physical / content row-set equality** —
"do these two replicas hold the same logical rows?" — remains
unavailable because no ADR has pinned canonical full-row
material. Anti-entropy reconciliation will eventually want to
ask this question of replicas; secondary indexes (roadmap #8)
will want a content-stable `__row_key_id__` for index segment
reconciliation; CDC (roadmap #10) will want cursor checkpoints
that compare content equality across replays.

The lifecycle and substrate ADRs are now shipped as design.
Content-hash identity is the right next surface to settle
before:

- **Roadmap #6 PK-keyed delete via supersession.** Supersession
  semantics layer cleanly over a content-equality model;
  mixing supersession into an unsettled identity surface is
  risky.
- **Roadmap #7 logical / physical descriptor fingerprint
  split.** The split is informed by what "row content" means;
  this ADR pins that meaning so #7 inherits a settled answer.
- **Roadmap #8 secondary indexes.** Index segments composing
  on `StateDigest` benefit from a content-stable
  `__row_key_id__`.

Three pressures shape the design.

- **Idempotent re-ingestion.** The motivating use case: replay
  a stream of records from cold storage, an external bus, or
  a re-run of a batch job, and have the table converge to the
  same logical state regardless of how many times each record
  is delivered. Counts and aggregates must see one row per
  logical fact.
- **Visible-state semantics under ADR 046 §5.** The visible
  digest reflects the visible set of rows, not the physical
  write history. Duplicate writes must not change the visible
  digest. This forces synchronous duplicate handling at
  admission, not asynchronous fold-up at compaction.
- **Schema evolution stability.** Adding an optional column
  to a content-hash table must not change row identity for
  existing rows. The canonical encoding's nullable handling
  carries this load.

The constraint from ADR 045 still holds: synthetic `__row_*`
columns are not part of user-visible row content. The
constraint from ADR 046 still holds: the substrate is
domain-separated and `StateDigest` placement is consumer-ADR
territory.

## Non-goals (V1)

- **No anti-entropy protocol.** The marquee `StateDigest`
  consumer stays deferred per the topology-gate closure
  rework budget (brainstorm 015 status header).
- **No durable `StateDigest` placement.** ADR 046 §7 holds:
  visible digest is computable on demand; not persisted to
  footer, pointer, catalog, local index, durability log, or
  wire frame.
- **No PK-keyed content identity.** PK-keyed tables already
  have identity-by-key; mixing content-hashing into the PK
  path is a follow-up if ever.
- **No PK-keyed delete semantics.** Roadmap #6.
- **No descriptor fingerprint split** beyond what this ADR
  forces. Roadmap #7.
- **No SQL parser work.** The engine surface is the typed
  descriptor mutation; client tools (Toron etc.) own DDL
  parsing per the no-DDL-parser convention from ADR 048.
- **No V1 / V2 Rust type suffixes.** Per
  `no-version-suffixes.md`. The on-the-wire domain tag
  `.v1` is allowed because it is an actual wire identifier.
- **No compatibility shim or reserved enum variants.** Per
  `no-premature-backward-compat.md`.
- **No `ColumnProjection` for content-hash tables in V1.**
  `IdentityProjection` is enabled; `ColumnProjection` defers
  to a follow-up alongside secondary indexes (roadmap #8).
- **No non-NULL-default additive schema evolution for
  content-hash tables in V1.** The descriptor validator
  rejects a `column-add` mutation that supplies a non-NULL
  default with `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`.
- **No decimal precision / scale evolution for content-hash
  tables in V1.** Same discipline as non-NULL defaults
  (§5). The descriptor validator rejects scale-change
  mutations with `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.
- **No multiset duplicate semantics.** Identical canonical
  payloads collapse to a single logical row; they do not
  remain distinct facts.
- **No async-only duplicate dedup path.** §2 commits to
  synchronous absorption at admission; an async-fold-at-
  compaction implementation would violate ADR 046 §5.
- **No raw Arrow IPC bytes as identity preimage.** §3
  commits to a canonical type-tagged byte stream over
  logical Arrow values.
- **No schema generation in the identity preimage.** Binds
  identity to the table's evolution history; destroys
  idempotency across schema changes. §4.
- **No physical descriptor fingerprint in the identity
  preimage.** Same rationale.
- **No physical column ordinal as `column_tag`.** Additive
  evolution and column reordering shift it. §3.
- **No new HRFT frame family.**
- **No new node capability bit.**
- **No slab footer field.**
- **No `ObjectStore` trait change.**
- **No `format_version` bump.**
- **No envelope or wire-frame bump.**
- **No physical / content row-set digest domain.** A future
  ADR may allocate one via ADR 046's reserved-tag mechanism
  if a concrete reader emerges.

## Decision

### 1. New `RowIdentityPolicy` variant

```rust
pub enum RowIdentityPolicy {
    PrimaryKey(PrimaryKeyDescriptorV1),
    AppendOnly,
    AppendOnlyContentHash,
}
```

`AppendOnlyContentHash` is the third variant. The existing two
remain unchanged. This is a logical-schema change; the
descriptor variant determines admission semantics, identity
preimage, and locality validation as in ADR 045.

The variant name is final. No reserved enum slot is allocated
ahead of additional variants per `no-premature-backward-
compat.md`.

### 2. Synchronous admission-time collapse for duplicate content

For every write to a `RowIdentityPolicy::AppendOnlyContentHash`
table, admission computes the canonical bytes (§3) and the
content-hash identity (§4):

```text
K = BLAKE3(
    "table.append-content-row.v1"
    || tenant_id_slot
    || table_object_id
    || canonical_row_bytes
)
```

Admission then looks up `K` against the table's row index
under current visible membership. Two paths:

- **Absorb path.** If `K` is currently visible (a row with
  that `__row_key_id__` exists and is not tombstoned or
  retention-evicted), the write is **absorbed**: no new
  physical record is written, no `StateDigest` element is
  added, the operation returns idempotent success.
- **Re-add / store path.** If `K` is not currently visible
  (never existed, was tombstoned via ADR 047, or was
  retention-evicted via ADR 048), the row is stored and
  element(`K`) is added to the visible-domain `StateDigest`.

The two paths implement **collapse semantics**: identical
payloads represent the same logical row; counts and aggregates
see one row per logical fact regardless of how many times the
content is written.

The lookup happens **synchronously at admission**, not
asynchronously at compaction. This is load-bearing. ADR 046
§5's visible-domain digest is over the visible row set, not
the physical write history; an async-fold-at-compaction
implementation would over-count the digest between admission
and compaction and violate visible-state semantics intermittent
with every duplicate write. ADR 048 already accepts transient
*inter-replica* digest divergence between replicas at different
compaction states; layering *intra-replica* admission /
compaction divergence on top would compound the spurious-
divergence problem and make anti-entropy reconciliation
substantially harder.

The admission lookup is **not a regression** — it is the
feature. Content-hash identity exists because operators want
to replay records without doubling counts. The lookup is the
mechanism that makes idempotent re-ingestion correct.

Implementation framing: the admission path is closest to
"PK-with-collapse-instead-of-supersession." It can share row-
index machinery with PK-keyed tables (the lookup is `(tenant,
table, K) → row visibility`), but it must **not** inherit
PK-style latest-version supersession semantics. Current visible
membership of `K` absorbs; absence of `K` (whatever the cause)
admits a new row and adds the digest element.

### 3. Canonical row encoding: sparse type-tagged byte stream

The canonical bytes that feed §4's preimage are produced by a
deterministic encoder over **logical Arrow values**, never
over raw Arrow IPC bytes. IPC encoding choices, dictionary
layout, padding, and batch boundaries are not row identity.
The encoder lives in a new module separate from any IPC code
path.

The canonical stream is a concatenation of **sparse non-null
column entries**:

```text
canonical_row_bytes := concat(non_null_entries_sorted_by_column_tag)

non_null_entry := column_tag | type_tag | value_length | value_bytes
```

`column_tag` is a stable assigned column ID (preferred — used
when the descriptor already exposes one) or a stable logical-
name hash (council fallback). Physical column ordinal is
**rejected** because additive evolution and column reordering
shift it.

Per-type encoding rules (each fixtured with golden vectors in
the implementation plan):

- **Synthetic `__row_*` columns**: excluded from the canonical
  stream entirely.
- **NULL-valued nullable column**: produces no bytes (sparse
  encoding; see §5 for why).
- **Boolean**: `0x00` (false) or `0x01` (true).
- **Signed / unsigned integer**: little-endian fixed-width per
  declared type.
- **Float**: IEEE 754, with NaN canonicalized to a single bit
  pattern and `-0.0 → +0.0`.
- **Decimal**: precision + scale + unscaled-integer LE.
- **Timestamp**: convert to UTC; encoded as i64 in declared
  unit (ns / µs / ms / s).
- **Date / time / interval / duration**: pinned per type
  (logical value, fixed-width).
- **UTF-8 string**: NFC-normalized, length-prefixed.
- **Binary**: length-prefixed bytes.
- **Dictionary array**: canonicalize by **decoded logical
  values**, never dictionary ids.
- **List / struct / map / nested**: **rejected** in V1.
- **Extension type**: rejected unless local canonicalization
  is pinned.

NFC string normalization is the canonical form for V1.
Invalid-UTF-8 input is rejected at the binary / string
boundary if the type system does not already prevent it.

### 4. Identity preimage

The content-hash identity preimage is domain-separated from
ADR 045 `AppendOnly` identity:

```text
__row_key_id__ = BLAKE3(
    "table.append-content-row.v1"
    || tenant_id_slot       // 16 B
    || table_object_id      // 16 B
    || canonical_row_bytes
)
```

The domain tag `"table.append-content-row.v1"` is an on-the-
wire identifier; the `.v1` suffix is allowed per
`no-version-suffixes.md`'s wire-protocol carve-out. The Rust
type `RowIdentityPolicy::AppendOnlyContentHash` carries no
suffix.

**Schema generation does not enter the preimage.** Including
either schema generation or the physical descriptor
fingerprint binds identity to the table's evolution history,
which destroys idempotency across schema changes. A row's
identity is a function of its canonical content, full stop.

### 5. Schema evolution stability

Sparse encoding from §3 makes additive evolution of
**nullable** columns identity-neutral:

- A NULL-valued nullable column produces no bytes.
- An existing row that physically lacks a newly-added nullable
  column is encoded with no bytes for that column.
- A new row that explicitly sets the new column to NULL is
  encoded with no bytes for that column.
- Both produce the same canonical bytes; their identities
  match.

This makes additive nullable evolution a no-op for identity.
Re-ingesting a logical fact through a wider schema (with
unset / NULL new columns) hits the absorb path in §2 because
the identity is unchanged.

A useful corollary: **submitted-column hashing and full-
logical-column hashing resolve to the same byte stream** for
nullable omitted columns, because nulls are omitted in both
cases. The "does the encoder hash submitted columns or all
descriptor columns?" question stops mattering for nullable
evolution.

Two schema mutations are **rejected** for content-hash tables
in V1:

- **Non-NULL-default additive column-add.** A column added
  with a non-NULL default introduces a semantic mismatch:
  - An existing row physically lacks the column → sparse
    encoding produces no bytes for it → identity stable.
  - A new row written with the column equal to `<default>` →
    sparse encoding produces bytes for it → identity differs.
  - Two writes of the same logical fact produce different
    identities under the new schema.
  V1 rejects this case at the descriptor mutation with
  `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`. A coherent
  alternative — *materialize the default into canonical bytes
  for old rows at read time* — is rejected because it ties
  row identity to descriptor-time default semantics,
  complicates recovery from older physical rows, and pulls
  descriptor-fingerprint-split decisions (roadmap #7) into
  this ADR's scope.
- **Decimal precision / scale evolution.** A scale change
  produces different canonical bytes for the same apparent
  decimal value (the unscaled-integer encoding differs). V1
  rejects scale-change mutations at the descriptor with
  `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`, parallel to the
  non-NULL-default reject. A future ADR can pin migration
  semantics if operational evidence shows the case is needed.

Operators who need either case use `AppendOnly` (write-context
identity) instead, or wait for a follow-up ADR.

### 6. Locality: enable `IdentityProjection`, defer `ColumnProjection`

ADR 045 §"No new locality policy variant" rejected
`AppendOnly + IdentityProjection` and `AppendOnly +
ColumnProjection` because HLC-derived identity is not retry-
stable from the payload. **Content-hash identity removes that
constraint** — the identity is fully a function of canonical
row bytes; it is retry-stable.

V1 enables one of the two:

- **`IdentityProjection`**: enabled. The projection is from
  `__row_key_id__` to a Locality value; deterministic and
  retry-stable.
- **`ColumnProjection`**: **rejected** for content-hash tables
  in V1. The combination would compose without violating
  identity stability (the projected column is part of
  canonical row bytes, hence part of the identity), but
  enabling it adds another descriptor-validator combination
  and another lookup path before roadmap #7 settles
  descriptor fingerprints. The follow-up that enables
  `ColumnProjection` for content-hash tables can land
  alongside secondary indexes (roadmap #8), where column-
  projection-aware indexes become operationally useful.

### 7. `StateDigest` composition: visible-domain auto-content-sensitive

Visible-row digest preimage remains as ADR 046 defined it:

```text
visible_row_element(t, T, k) := scalar(BLAKE3(
    "digest.table.row.visible.v1"
    || t                  // tenant_id slot
    || T                  // table_object_id
    || k                  // __row_key_id__
)) * G
```

For `AppendOnlyContentHash` tables, `__row_key_id__` is
content-derived (§4), so the visible-row element automatically
becomes content-sensitive. No new digest domain is introduced.

This composition is correct **only under §2's synchronous-
collapse admission**. Under async dedup, admission would add
element(`K`) blindly per duplicate write, over-counting the
digest until compaction folded duplicates and recomputed the
digest. The synchronous absorb-vs-store decision keeps the
digest invariant with the visible state at all times.

A future ADR may introduce a separate physical / content row-
set digest domain (e.g., `digest.table.row.content.v1`) if a
concrete consumer needs to compare row contents independent of
identity slots. ADR 046's reserved-tag mechanism handles that
allocation. **This ADR does not allocate that domain.**

### 8. Composition with ADR 047 and ADR 048: re-ingestion semantics

Tombstone composition (ADR 047). A tombstone for `K` subtracts
element(`K`) from the visible digest at admission. After the
tombstone, `K` is no longer visible. A subsequent re-ingest of
the same canonical content takes §2's **store path** (key not
currently visible → store + add element). The system gracefully
handles "delete then re-import" patterns without requiring
operator intervention.

Retention composition (ADR 048). A retention-evicted row for
`K` subtracts element(`K`) at compaction; `K` is no longer
visible. A subsequent re-ingest after retention eviction takes
the same **store path** as the tombstone case. Idempotent
re-ingestion across retention windows works correctly.

The absorb-vs-store distinction is precise: absorbing requires
`K` to be **currently visible**, not merely **known to have
existed**. A bug that confuses "known key" with "currently
visible key" — for example, a stale visibility check — would
silently drop a re-ingest after delete or eviction. The
implementation plan's Phase 6 bench surfaces both paths and
catches this class of regression.

PK-keyed tables (ADR 037 + ADR 045) are unaffected by this
ADR. PK-keyed identity is `__row_key_id__` derived from PK
columns; supersession governs version chains. Content-hash is
a strictly separate identity policy.

## Rationale

**Why a new `RowIdentityPolicy` variant rather than extending
`AppendOnly`.** ADR 045 carefully sized `AppendOnly` around
write-context identity to keep the hot path free of payload
hashing. Content-hash identity inherently requires payload
hashing; bundling it under `AppendOnly` would force every
existing `AppendOnly` table to absorb the cost or carry a
hidden mode bit. A separate variant lets operators opt in
explicitly and lets the engine's admission path branch on a
typed descriptor variant rather than a runtime flag.

**Why synchronous absorption rather than async dedup.** The
visible-domain digest from ADR 046 §5 is over the visible row
set. Duplicate writes do not change the visible set, so they
must not change the digest. Async dedup would over-count the
digest until compaction; that breaks the visible-state
invariant intermittently with every duplicate write and
substantially complicates anti-entropy reconciliation. The
synchronous lookup is the cost of the feature — not a
regression.

**Why sparse type-tagged encoding rather than a positional
schema-driven layout.** Sparse encoding makes additive
nullable evolution identity-neutral by construction. A
positional layout (one entry per column in physical order)
would either change identity when a column is added or
require trailing-NULL-trimming heuristics that handle column
reordering poorly. Sparse encoding handles both cases
uniformly and removes the "submitted vs full" ambiguity.

**Why reject non-NULL defaults rather than materialize them.**
Materialize-defaults-at-read is a coherent alternative but
ties row identity to descriptor-time default semantics:
identity becomes a function of "the descriptor when the row
was read" rather than "the row's content." This complicates
recovery from older physical rows (which descriptor's
defaults?), pulls descriptor-fingerprint-split decisions
(roadmap #7) into this ADR's scope, and creates a class of
identity bugs around descriptor mutations. V1 picks the
narrower discipline: reject the case so identity stays a
function of facts physically present (or explicitly NULL).

**Why reject decimal precision / scale evolution.** Same
failure mode as non-NULL defaults: scale changes produce
different canonical bytes for the same apparent value. Solving
this needs explicit migration semantics, not an implicit
encoder rule. V1 keeps the discipline parallel.

**Why enable `IdentityProjection` but defer
`ColumnProjection`.** Content-hash identity is the first
policy where `ColumnProjection` could compose without
violating identity stability. But enabling it adds another
descriptor-validator combination and another lookup path
before descriptor-fingerprint-split (roadmap #7) settles the
shape of valid `(RowIdentityPolicy, LocalityPolicy)`
combinations. One new locality combination per ADR is the
lower-risk shape. The follow-up lands with secondary indexes
(roadmap #8), where column-projection-aware indexes become
operationally useful.

**Why no schema generation or descriptor fingerprint in the
preimage.** Either would bind row identity to the table's
evolution history. A row's identity must be a function of its
content, not a function of which schema generation observed
it. This is the same principle that drives §5's sparse
encoding and the rejection of materialize-defaults-at-read.

**Why no new digest domain.** ADR 046's visible-row digest
preimage already takes `__row_key_id__` as input; making
`__row_key_id__` content-derived makes the existing visible-
row element automatically content-sensitive for content-hash
tables. Allocating a new domain (e.g.,
`digest.table.row.content.v1`) before a concrete consumer
exists would fail ADR 046 §7's deferral test. The reserved-
tag mechanism is available to a future ADR with a real reader.

**Why a stable column ID rather than ordinal or name hash.**
Physical ordinals shift under additive evolution and column
reordering, breaking identity stability. Logical-name hashes
are usable but bind identity to the column's *current* name,
making renames an identity event. A stable assigned column ID
(generated at column creation, never reused, never changed)
keeps identity stable across both reordering and rename. The
descriptor must expose such an ID; the impl plan checks
whether one already exists or adds one.

## Consequences

- **Admission lookup cost on content-hash tables.** Every
  write performs a row-index lookup. Wide-row case is the
  load-bearing benchmark — ADR 045 dropped payload hashing
  specifically for CPU reasons; the bench (impl plan Phase
  6) reproduces ADR 045's wide-row regime to compare costs
  directly.
- **Idempotent re-ingestion is a first-class operation.**
  Operators can replay record streams without inflating
  counts or aggregates. Re-ingestion across tombstones and
  retention windows reinstates rows.
- **Restricted schema mutations on content-hash tables.**
  Non-NULL defaults and decimal scale changes are rejected at
  the descriptor mutation. Operators who need either are
  steered toward `AppendOnly` or wait for a follow-up ADR.
- **`IdentityProjection` becomes valid for an append-only
  policy for the first time.** Per-row distribution is
  available without the ADR 045 HLC-derived-identity
  constraint.
- **Row-index machinery is shared with PK-keyed tables.**
  Implementation parity at low extra cost; the absorb-vs-
  store distinction is the only new behavior on the lookup
  path.
- **No on-disk shape change.** No descriptor field beyond the
  variant, no slab footer field, no envelope bump, no wire-
  frame bump.
- **Stable column ID surface required.** Whether the
  descriptor already exposes one or this ADR adds one is
  pinned by the impl plan's Phase 1.
- **Future PK-keyed delete (roadmap #6) inherits a settled
  identity surface.** Supersession discipline can layer over
  collapse semantics without re-litigating what "the same
  row" means.

## Rejected alternatives

- **Async dedup at compaction (multiset until folded).**
  Rejected. Violates ADR 046 §5 visible-state semantics
  between admission and compaction; compounds anti-entropy
  spurious divergence. §2.
- **Multiset duplicate semantics (no collapse).** Rejected.
  Defeats the content-as-identity premise; would require
  another discriminator (HLC, sequence) entering identity,
  which collapses back into write-context-identity territory
  already covered by `AppendOnly`.
- **Reject duplicates as admission errors.** Rejected. Forces
  upstream conflict handling for what should be the
  idempotent re-ingestion use case. Operators replaying a
  record stream want absorption, not a reject storm.
- **Raw Arrow IPC bytes as identity preimage.** Rejected. IPC
  encoding choices, dictionary layout, padding, and batch
  boundaries are not row identity. Two transports of the same
  logical row could produce different IPC bytes; identity
  must not depend on transport.
- **Schema generation in the identity preimage.** Rejected.
  Binds identity to the table's evolution history; destroys
  idempotency across schema changes.
- **Physical descriptor fingerprint in the identity
  preimage.** Rejected. Same rationale.
- **Physical column ordinal as `column_tag`.** Rejected.
  Additive evolution and column reordering shift it; breaks
  identity stability.
- **Materialize non-NULL defaults at read time.** Rejected.
  Coherent but ties identity to descriptor-time default
  semantics; complicates recovery from older physical rows;
  pulls descriptor-fingerprint-split decisions (roadmap #7)
  into this ADR's scope.
- **Allow `ColumnProjection` for content-hash tables in V1.**
  Rejected for V1. Adds another descriptor-validator
  combination before descriptor-fingerprint-split settles
  valid combinations; defers to a follow-up alongside
  secondary indexes.
- **Allow non-NULL-default additive column-add.** Rejected
  for V1. Breaks identity stability under sparse encoding.
- **Allow decimal precision / scale evolution.** Rejected
  for V1. Same failure mode as non-NULL defaults.
- **NFKC / NFKD / NFD as the string normalization form.**
  Rejected. NFC is the council pick — most common form for
  canonical comparison; matches the principle of least
  surprise for operators familiar with Unicode equivalence.
- **Allocate `digest.table.row.content.v1` now.** Rejected.
  No concrete reader; fails ADR 046 §7's deferral test.
  Reserved-tag mechanism handles future allocation if a
  reader emerges.
- **Bundle content-hash identity into the existing
  `AppendOnly` variant with a flag.** Rejected. Forces every
  `AppendOnly` table to absorb the payload-hash cost or
  carry a hidden runtime mode bit; the typed variant keeps
  the engine's admission branch a clean compile-time
  decision.

## Resolved questions

These are V1 decisions converged across Codex, Claude, and
Gemini. The implementation plan inherits them.

1. **Variant name: `AppendOnlyContentHash`.** Final.
2. **Synchronous admission-time collapse, not async dedup.**
   §2.
3. **Sparse type-tagged canonical encoding, not positional
   layout, not raw IPC bytes.** §3.
4. **Stable assigned column ID (preferred) or stable logical-
   name hash (fallback) as `column_tag`. Physical ordinal
   rejected.** §3.
5. **NFC string normalization.** Council unanimity.
6. **Schema generation and descriptor fingerprint excluded
   from the identity preimage.** §4.
7. **Sparse encoding makes additive nullable evolution
   identity-neutral.** §5.
8. **Non-NULL default additive column-add rejected with
   `CONTENT_HASH_REJECTED_NON_NULL_DEFAULT`.** §5.
9. **Decimal precision / scale evolution rejected with
   `CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION`.** §5.
10. **`IdentityProjection` enabled.** §6.
11. **`ColumnProjection` rejected in V1.** §6.
12. **Visible-row digest auto-content-sensitive via existing
    preimage; no new digest domain.** §7.
13. **Composition with ADR 047 / 048: re-ingestion after
    tombstone or retention eviction takes the store path.**
    §8.

## Open questions

V1 picks defaults; each can be revisited in a follow-up ADR
without on-disk surface change.

1. **Stable column ID surface.** If the descriptor already
   exposes a stable assigned column ID, the ADR uses it. If
   not, the impl plan's Phase 1 adds one. The exact shape
   (UUID, monotonic u64, content-derived) is pinned during
   Phase 1.
2. **Invalid-UTF-8 handling at the binary / string boundary.**
   The encoder rejects invalid UTF-8 at admission with a typed
   error; the exact reject code is named during Phase 1.
3. **Bench-shape deltas.** The bench in Phase 6 isolates
   payload-hash cost from row-index-lookup / duplicate-
   absorption cost. The acceptance thresholds refine after a
   first smoke run; promotion-to-shipped requires the full
   100k / 1M matrix.

## Forward-compatibility analysis

**Anti-entropy reconciliation (deferred).** Content-hash
identity is the substrate that lets anti-entropy compare
row sets across replicas by `StateDigest` point comparison.
The future anti-entropy ADR composes against the visible-
domain digest semantics this ADR establishes; no surface
change needed in this ADR.

**PK-keyed delete via supersession (roadmap #6).** PK-keyed
delete inherits a settled content-equality model from this
ADR. Supersession layers over collapse — both are admission-
time semantics over the same row-index machinery — without
re-litigating identity meaning.

**Logical / physical descriptor fingerprint split (roadmap
#7).** This ADR pins what "row content" means via the
canonical encoder. Roadmap #7's split inherits that
definition; the logical fingerprint is what identity depends
on, the physical fingerprint is everything else.

**Secondary indexes (roadmap #8).** Index segments composing
on `StateDigest` benefit from a content-stable
`__row_key_id__`. The follow-up that enables `ColumnProjection`
for content-hash tables lands here — column-projection-aware
indexes become operationally useful at the secondary-index
ADR.

**CDC (roadmap #10).** CDC consumers can verify cursor
checkpoints against partition `StateDigest` points
algebraically, with content equality as the underlying
semantic. Re-ingestion of historical data through CDC is
naturally idempotent under collapse.

**LSM-style leveled compaction (roadmap #11).** Compaction
preserves `__row_key_id__` byte-for-byte; identity is stable
across compaction boundaries by construction.

**Future schema-migration ADR.** A follow-up may pin migration
semantics for non-NULL defaults and decimal evolution on
content-hash tables. The reject codes
`CONTENT_HASH_REJECTED_NON_NULL_DEFAULT` and
`CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION` are the operator-
visible signal that a migration ADR is needed.

**Future physical / content row-set digest domain.** If a
concrete consumer (audit, content-set anti-entropy) emerges,
the future ADR allocates `digest.table.row.content.v1` (or
similar) via ADR 046's reserved-tag mechanism. This ADR does
not pre-allocate.

## Deferred to follow-up ADRs

1. PK-keyed content identity (PK + content hash combined).
2. Per-column retention on content-hash tables.
3. `ColumnProjection` enable for content-hash tables.
4. Non-NULL default additive evolution migration semantics.
5. Decimal precision / scale evolution migration semantics.
6. List / struct / map / nested type canonicalization.
7. Extension type canonicalization framework.
8. Physical / content row-set digest domain
   (`digest.table.row.content.v1` reserved by future ADR).
9. Column rename semantics on content-hash tables (under
   logical-name-hash `column_tag` fallback).
10. Anti-entropy reconciliation protocol over content-hash
    digests.

## References

- **ADR 037** — primary key descriptor (the original identity
  policy).
- **ADR 045** — `AppendOnly` write-context identity (the
  variant this ADR complements).
- **ADR 046** — `StateDigest` substrate; §5 visibility
  semantics drive the synchronous-collapse commitment in §2.
- **ADR 047** — row-level tombstones; tombstone composition
  with content-hash re-ingestion (§8).
- **ADR 048** — per-table retention; retention composition
  with content-hash re-ingestion (§8); no-DDL-parser
  convention inherited.
- **Brainstorm 015** status header (closed Outcome B
  2026-04-29) — topology-gate context; opaque `NodeId`
  sufficient, no cryptographic node identity required for
  this ADR.
- `plans/conventions/no-premature-backward-compat.md` —
  rejection of compatibility shims and reserved enum slots.
- `plans/conventions/newtype-units.md` — newtypes for
  `__row_key_id__`, `column_tag`, and other quantities.
- `plans/conventions/no-version-suffixes.md` — Rust types
  unsuffixed; on-the-wire domain tag `.v1` allowed.
- `plans/conventions/capability-vs-feature-naming.md`.
- `plans/philosophy.md` — Identity-vs-Locality framing
  inherited.
