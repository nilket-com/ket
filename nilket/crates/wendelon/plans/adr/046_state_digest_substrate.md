# ADR 046: StateDigest Substrate

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan: `plans/impl/046_state_digest_substrate.md`
Companion brainstorm: `plans/brainstorms/014_algebraic_record_identity_ecmh.md`

## Status

Accepted

## Context

The post-045 roadmap moves the algebraic state substrate ahead
of secondary indexes, tombstones, retention, CDC, and LSM
compaction. The reason is composition: those later features all
create, delete, move, reconcile, or derive state. If each one
invents its own checksum, reconciliation, or digest model, the
final system will accumulate incompatible correctness
instruments.

Brainstorm 014 proposes elliptic-curve multiset hashing (ECMH):
map each state element to a Ristretto255 point and add the
points to get a commutative state identity. The attractive
property is simple:

```text
Digest(A) + Digest(B) == Digest(A union B)
Digest(A union B) - Digest(B) == Digest(A)
```

That property composes naturally with tombstones, retention,
secondary-index segments, and future anti-entropy. It also
changes the shape of later ADRs. Secondary indexes should
consume a settled state-digest substrate rather than becoming
the accidental owner of Wendelon's reconciliation semantics.

This ADR is deliberately narrower than the full Brainstorm 014
vision. It does **not** replace record `Identity`, does **not**
change routing topology, does **not** add a cross-replica
anti-entropy protocol, and does **not** write a point into any
durable surface. The first design problem is what a digest
means, how elements are domain-separated, and whether the cold
path can afford the math.

## Non-goals (V1)

- **No anti-entropy gossip protocol.** Substrate exists so that
  protocol can be designed; not part of this ADR.
- **No `Identity` replacement.** Existing 32-byte BLAKE3
  `Identity` stays.
- **No hot-path elliptic-curve math.** Curve operations are
  cold-path only.
- **No FROST signing, no routing topology change, no `NodeId`
  shape change.** Brainstorm 015 territory.
- **No durable on-disk presence.** No slab-footer field, no
  sidecar block, no envelope `format_version` bump, no
  `TieredSlab` / `TieredSlabPointer` / catalog field, no wire
  frame. Storage placement is deferred to consumer ADRs.
- **No new HRFT frame family, no new node capability bit, no
  new reject-code family, no `ObjectStore` trait change.**
- **No new synthetic column.** `__row_*` set is unchanged.
- **No descriptor change.** `TableSchemaDescriptor` is
  unchanged.
- **No physical-row, tombstone, retention, or secondary-index
  preimage helper.** Domain names are reserved (§4); preimage
  helpers belong to the owning consumer ADR. Only
  `visible_row_element` is implemented in V1.
- **No commitment to deletion policy** (subtract-on-delete vs
  event-history). The substrate supports add+subtract; the
  row-tombstone ADR picks the policy.
- **No `format_version` bump.**
- **No project-name-prefixed names. No `V1` / `V2` Rust-type
  suffixes.** Domain-tag bytes carry a `.v1` suffix because
  domain tags are wire-format-shaped contracts where versioning
  is appropriate (consistent with ADR 045 §4a's
  `"table.append-locality.v1"`).
- **No reserved enum slots, compatibility shims, or pre-
  allocated fields for follow-up ADRs.**

## Decision

### 1. `StateDigest` is a separate algebraic state identity

Wendelon adds a new `StateDigest` abstraction for homomorphic
state accumulation. **Not a replacement for `Identity`.**

- `Identity` remains the 32-byte BLAKE3 storage coordinate used
  for rows, keys, local indexes, and existing protocol surfaces.
- `StateDigest` is a 32-byte compressed Ristretto255 point that
  summarizes a multiset of state elements.
- `StateDigestAccumulator` is the mutable cold-path builder.
  It holds the uncompressed Ristretto255 point internally,
  supports add / subtract / merge operations, and emits a
  canonical compressed `StateDigest` via `finish()`.
- `StateDigestElement` is a projected-point wrapper.
  Construction requires a domain (no untagged elements).

```rust
#[repr(transparent)]
pub struct StateDigest([u8; 32]);  // Pod + Zeroable

pub struct StateDigestAccumulator { /* opaque */ }
pub struct StateDigestElement { /* opaque, contains a curve point */ }
pub struct StateDigestDomain(&'static [u8]);
```

The empty digest is the compressed Ristretto255 identity point.
A deserialized `StateDigest` must decompress successfully as a
Ristretto255 point or the reader rejects it.

### 2. Use Ristretto255, not LtHash

The production substrate uses Ristretto255 over Curve25519 via
the `curve25519-dalek` crate.

Rationale:

- Ristretto255 gives a dense 32-byte serialized point, matching
  Wendelon's storage-density bias and 256-bit identity posture.
- LtHash-style accumulators are much faster but materially
  larger (kilobytes, not bytes), which makes them the wrong
  default for slab, index-segment, and control-ring metadata.
- The expensive work is cold-path work. L1 ingestion and
  request admission continue to compute only BLAKE3 identities.

The implementation must still benchmark Ristretto255 projection
and addition against L2-seal-shaped batches. If the cold path
cannot hide the cost, the ADR returns to council before
ratification. LtHash is the documented benchmark-only baseline,
not a production alternative.

### 3. Project elements with explicit domain separation

Every element added to a `StateDigest` is projected from:

```text
StateDigestElementPoint =
    Scalar::from_bytes_mod_order(
        BLAKE3-256(
            "state-digest.element"
            || domain_tag_len_be_u32
            || domain_tag
            || element_preimage_len_be_u64
            || element_preimage
        )
    ) * RISTRETTO_BASEPOINT_POINT
```

This intentionally chooses the hash-to-scalar shape from
Brainstorm 014 rather than a direct hash-to-curve projection.
The hot path can continue producing BLAKE3-derived identity
material, while the cold path performs batched scalar
multiplication during sealing, verification, or future
reconciliation. Hash-to-scalar composes with dalek's
`vartime_multiscalar_mul` (or equivalent batched primitive),
which is structurally faster than per-element hash-to-curve at
slab-seal cadence.

`RistrettoPoint::from_uniform_bytes(BLAKE3-XOF-512(...))` is a
documented fallback only if security review rejects hash-to-
scalar for this use. The modulo bias from
`Scalar::from_bytes_mod_order` (~3.5 bits in a ~252.5-bit field)
is irrelevant for digest use — the algebraic property
(commutative, associative, subtraction-correct) holds either
way. Bias would matter only for cryptographic schemes
(signatures, key derivation) Wendelon does not use this
primitive for. Length-prefixing prevents tag/preimage boundary
ambiguity.

The domain tag is load-bearing. Two elements with identical
preimage bytes in different domains must produce different
points.

### 4. Define semantic digest domains, but do not pre-allocate future surfaces

This ADR defines the semantic domains that later ADRs consume.
The semantic names stay unversioned in discussion; the byte tags
carry `.v1` because they are golden-vector inputs that may
require explicit rotation.

| Semantic domain | Domain-tag bytes | Owner |
| --- | --- | --- |
| `table.row.visible` | `digest.table.row.visible.v1` | ADR 046 helper |
| `table.row.physical` | `digest.table.row.physical.v1` | canonical row-fact consumer |
| `table.row.tombstone` | `digest.table.row.tombstone.v1` | row-tombstone ADR |
| `secondary_index.entry` | `digest.secondary_index.entry.v1` | secondary-index ADR |
| `retention.gc` | `digest.retention.gc.v1` | retention ADR |

Defining these domain names is not permission to add their
storage fields early. The code exposes domain-separated
projection helpers without adding descriptor fields, slab
fields, enum variants, wire tags, or absent/present slots for
follow-up ADRs. Versioned domain bytes (`.v1` suffix) are
allowed because the bytes feed golden vectors and may need
explicit rotation; versioned Rust type names (`StateDigestV1`)
are not, per `no-version-suffixes.md`.

For ADR 046's concrete helper, `table.row.visible` is the only
row-semantic domain with a pinned current preimage:

```text
visible_row_preimage =
    tenant_id              // 16 B
    || table_object_id     // 16 B
    || row_key_id          // 32 B (= __row_key_id__)
```

This proves logical row membership only. It does not prove
payload equality. A physical row-set digest must include
content material in its element preimage; this ADR deliberately
does not invent canonical full-row encoding ahead of the
content-hash identity ADR.

A second plausible model surfaced during council iteration:
keep monotone physical, tombstone, and retention event-history
digests, then derive visible state by subtracting lifecycle
domains at query time. That model is promising, but ADR 046
should not freeze it. Different domain tags do not cancel into
ordinary visible membership by themselves, and the tombstone /
retention ADRs need to decide whether they want direct
visible-membership subtraction, separate event-history evidence,
or both.

`StateDigestDomain` is exposed as a `&'static [u8]` wrapper
rather than a Rust enum. Adding new domains in consumer ADRs is
a pure addition — no enum migration, no exhaustive-match revisit
at the substrate layer. Domain separation is preserved because
the projection function uses the byte tag, not a Rust enum tag.

### 5. Compaction and reclustering invariants

A `StateDigest` over a set of elements **must be independent of**:

- row order;
- Arrow record-batch boundaries;
- multi-row record grouping;
- slab grouping;
- compaction pass boundaries.

The digest for a set of rows is the same whether rows are
sealed in one record or many, in one slab or many, as long as
the same domain and same element preimages are accumulated
*exactly once*.

**The caller owns set semantics.** Adding the same element
twice produces a different multiset digest — that is correct
multiset behavior, not a dedupe layer. Idempotent computation
is therefore a caller invariant: build from the canonical final
element set, not from retry attempts or side-effect counters.

Element preimages depend only on logical content (tenant ID,
table object ID, row key ID for the V1 visible helper). Nothing
about slab ID, record location, record row index, locality
bucket, physical packing order, or compression codec enters the
preimage. Compaction can rearrange any of those without
changing element points.

### 6. Deletion algebra is available, but tombstone semantics land later

`StateDigestAccumulator` supports `subtract_element` and
`subtract_digest` symmetric to `add_element` / `merge_digest`.
That gives consumer ADRs the algebra they need for row
tombstones and retention.

**This ADR does not dictate deletion policy.** The substrate
provides `add` and `subtract`. The row-tombstone ADR (roadmap
#2) picks among:

- subtraction from a single visible-state digest (monotone
  physical/tombstone/retention domains, visible derived as
  `physical - tombstone - retention`); or
- additions to a separate `tombstone` domain digest with
  visible-state computed at consumer-side comparison time; or
- both, with subtraction at write time and tombstone-domain
  accumulation at compaction time.

This ADR does not define `__row_tombstone__`, delete request
shapes, retention floors, visible-state query rules, or
compaction GC. Those surfaces belong to the lifecycle ADRs that
follow.

### 7. No durable storage placement in V1

ADR 046 does **not** write `StateDigest` into:

- the ADR 022 fixed slab footer;
- the ADR 039 CSB footer locator;
- a new sidecar block;
- `TieredSlab` / `TieredSlabPointer` / catalog metadata;
- table descriptors;
- wire frames or HRFT envelopes.

This is a decision, not an omission. The current ADR creates
the algebraic substrate, golden vectors, invariants, and cold-
path benchmarks. Durable placement waits until a consumer needs
it: tombstones / retention for lifecycle state, secondary
indexes for index segments, or a later anti-entropy ADR for
cross-replica comparison.

The sidecar-block placement proposed during council iteration
remains a good consumer-ADR candidate. It should be evaluated
when a durable digest has a concrete reader, authentication
boundary, and invalidation rule. ADR 046 only needs to make
that later choice possible.

Reasons:

- ADR 022's fixed footer and HMAC coverage are deliberately
  constrained.
- ADR 039 already spends the former 32-byte footer suffix on
  the CSB locator.
- ADR 044 depends on footer integrity and avoids duplicate
  footer-side metadata.
- A persisted digest without a consumer is pre-allocated
  format surface, which `no-premature-backward-compat.md`
  forbids.

### 8. Boundary for secondary indexes

The future secondary-index ADR may consume `StateDigest` for
segment equality, rebuild verification, and replica
reconciliation. It must not redefine element projection, group
arithmetic, or domain-separation rules.

The index ADR still owns:

- the `secondary_index.entry` element preimage shape (whether
  it covers physical locator entries, logical membership keyed
  by `row_key_id`, or both);
- index-segment storage placement;
- visibility composition with row tombstones (when those land).

ADR 046 supplies the algebra; the secondary-indexes ADR
supplies the meaning.

## Rationale

**Why not put the point in the footer now.** The footer is the
wrong first move. A footer point sounds small, but it has to
answer all of these questions before it is meaningful: physical
row-set state or logical visible state; payload equality or
row-identity equality; tombstones as additions, subtractions,
or separate digest; behavior under compaction when physical
locators change; authentication without changing ADR 022 / 039
/ 044 invariants. Putting bytes in the footer before answering
those questions makes the footer layout the owner of semantics
it cannot express.

**Why hash-to-scalar over hash-to-curve.** Composition with
batched scalar multiplication. The cold path can accumulate via
`vartime_multiscalar_mul(scalars, [G; n])` (or equivalent
batched primitives) instead of N independent point operations.
The hot path stays BLAKE3-only and produces material the cold
path can reduce-and-multiply efficiently. Hash-to-curve via
`from_uniform_bytes` removes modulo bias but loses the batched-
multiplication composition; for digest-only use, the bias is
irrelevant.

**Why define multiple domains now but only implement one.**
The substrate has to prevent accidental digest aliasing —
visible-row membership and secondary-index-entry are both sums
of 32-byte row-ish identifiers but they do not mean the same
thing. Domain tag is the type boundary at the hash layer.
Reserving the namespace documents the boundary; implementing
only `visible` keeps the substrate surface bounded to what V1
council can validate.

**Why not implement physical-row equality now.** Exact payload
equality requires canonical full-row material. ADR 045
intentionally removed payload hashing from append-only identity;
the roadmap puts content-hash identity at #5 (its own ADR with
the canonical-encoding spec). Smuggling canonical-row encoding
into the substrate ADR would either pre-empt that ADR or commit
to an encoding the substrate doesn't need.

**Why not freeze tombstone and retention domains now.**
Tombstone and retention digests can mean at least two different
things: visible-membership subtraction or monotone lifecycle-
event history. Both are useful, but they answer different
questions. ADR 046 exposes the algebra and domain-separation
rules; the lifecycle ADRs must own the semantic choice,
preimage bytes, storage placement, and compaction rules.

**Why `StateDigestDomain(&'static [u8])` and not a Rust enum.**
An enum forces every domain consumer to bump the substrate's
enum and revisit every dispatch site. A `&'static [u8]` wrapper
lets each consumer ADR add named domain-tag constants without
enum migration or exhaustive-match churn. Domain separation is
preserved because the projection function uses the byte tag,
not a Rust enum tag.

**Why Ristretto255 over LtHash.** Density. The CPU trade favors
LtHash but only on the cold path, where Wendelon has spare
cycles. Phase 4 bench is the load-bearing artifact; LtHash is
the benchmark-only baseline (not a production alternative).

## Consequences (the negatives)

- **New `curve25519-dalek` direct dependency.** Already present
  transitively via `ed25519-dalek`; first-class for ADR 046.
- **Cryptographic primitive outside the FIPS validation
  boundary.** Same posture as BLAKE3 content identity.
- **Misuse is possible.** Adding the same element twice
  produces a different multiset digest. Callers must build from
  canonical element sets, not retry side effects.
- **Physical row-set equality remains unavailable until a
  consumer pins canonical full-row element material.**
- **Cold-path CPU spike at L2 seal** when consumers integrate.
  Phase 4 bench measures it; runbook explains expectations.
- **No durable presence in V1.** Substrate is "code that
  consumers can call but nothing yet does." Deliberate. First
  user-observable effect ships with the row-tombstone ADR.
- **Identity preimage and digest preimage share field bytes.**
  Domain-tag prefix is the only thing keeping ADR 045 identity
  preimage `BLAKE3("table.append-row" || ...)` apart from this
  ADR's `BLAKE3("digest.table.row.visible.v1" || ...)`.
  Architectural test enforces.
- **Batched scalar accumulation becomes an explicit
  implementation concern.** If the scalar path cannot satisfy
  cold-path budgets at slab-seal cadence, council revisits
  projection before any durable consumer depends on it.

## Rejected alternatives

- **Hash-to-curve via `from_uniform_bytes` as the primary
  projection.** Loses batched-multiplication composition. Kept
  as documented fallback if security review rejects
  hash-to-scalar.
- **LtHash as V1 primitive.** Density argument. Benchmark-only
  baseline.
- **Hash-to-curve at admission.** Hot-path curve math.
- **Single-domain digest (no domain separation).** Visible /
  physical / index are not interchangeable.
- **Implementing physical / tombstone / retention preimages in
  V1 substrate.** Their preimages belong to owning ADRs.
- **Monotone-domains commitment with derived visible.** One
  valid lifecycle model; substrate shouldn't pre-pick.
- **Multi-domain `StateDigestAccumulator`.** Consumers compose.
- **`StateDigestDomain` as a Rust enum.** `&'static [u8]`
  wrapper is more flexible across consumer ADRs.
- **Durable on-disk presence in V1** (footer, sidecar, advisory
  catalog field, descriptor field). Pre-allocates format
  surface per `no-premature-backward-compat.md`.
- **Project-name-prefixed names; `V1` / `V2` Rust-type
  suffixes.** Conventions.

## Open questions

These defer to bench / security review / council. None are
contract-level.

1. **Hash-to-scalar security review.** Hash-to-scalar is the
   V1 projection (§3) on architectural composition grounds.
   Security review confirms the modulo bias is acceptable for
   digest-only use (no signature / KDF exposure). If review
   rejects, fall back to
   `RistrettoPoint::from_uniform_bytes(BLAKE3-XOF-512(...))`.
   Default expectation: review approves.
2. **Cold-path budget threshold.** Phase 4 bench reports
   throughput. Council ratification sets an explicit Ristretto
   throughput threshold before the substrate implementation is
   declared shipped. Brainstorm 014's 45–60 K
   accumulations/sec/core is the prior expectation; target is
   10k rows ≤ 100 ms and 100k rows ≤ 1 s.
3. **Lifecycle model.** Should consumer ADRs (row-tombstones,
   retention) express deletion as direct visible-membership
   subtraction, monotone event-history digests, or paired
   digests? The row-tombstone ADR makes the call. The
   substrate's algebra is sufficient for any of the three.
4. **Physical row-domain timing.** If a workload pulls
   physical-row-set anti-entropy before the content-hash
   identity ADR ships, the substrate ADR may need to grow a
   canonical row-fact encoding section. Default: physical-
   domain implementation belongs to its consumer ADR, not this
   one.

## Forward-compatibility analysis

**Anti-entropy gossip protocol (future ADR).** Consumes
`StateDigest` over the wire; decides slab / partition /
replica-group exchange shape, integrity story, and failure-mode
behavior.

**Row-tombstone ADR.** Adds elements to `table.row.tombstone`
domain or subtracts from `table.row.visible`, its choice. May
add an in-memory partition-level accumulator; storage placement
is its decision.

**Per-table retention ADR.** Adds elements to `retention.gc`
domain at compaction-time when rows cross the retention floor.

**Topology gate / brainstorm 015 ADR.** Independent of this
ADR's storage-state algebra. May share the curve dependency.
The algebra is reusable; topology preimage and digest preimage
are independently domain-tagged.

**Content-hash identity ADR.** Adds the third
`RowIdentityPolicy` variant. Identity preimage gains a content-
hash component; digest visible preimage is unchanged. May
provide canonical row encoding a future physical-row digest
helper needs.

**Secondary-indexes ADR.** §8 boundary statement.

**LSM compaction ADR.** Uses §5 invariants as level-merge
correctness instruments.

**Vector embeddings ADR.** May introduce a `vector.partition`
digest domain if pulled.

**Future `table.row.content` domain.** Workload-pulled.

## Deferred to follow-up ADRs

1. Anti-entropy gossip protocol.
2. Storage placement of slab / partition digests.
3. `Identity` replacement (brainstorm 014's longer ambition).
4. `secondary_index.entry` preimage and helper.
5. `table.row.tombstone` preimage and helper.
6. `retention.gc` preimage and helper.
7. `table.row.physical` preimage and helper.
8. `vector.partition` domain.
9. FROST-signed slab/partition digests.

## References

- **Brainstorm 014** — source proposal.
- **ADR 022 / 039 / 044** — slab footer / CSB locator / fetch
  integrity constraints driving §7 deferral.
- **ADR 042 / 045** — synthetic-column trio carrying preimage
  components; `"table.append-locality.v1"` tagging precedent.
- `plans/conventions/{newtype-units,no-premature-backward-compat,no-version-suffixes,no-project-name-in-protocols}.md`.
