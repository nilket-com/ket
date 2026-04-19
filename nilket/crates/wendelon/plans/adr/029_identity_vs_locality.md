# ADR 029: Identity vs Locality — Two Projections of a Single Ring

## Status
Proposed

## Context
Wendelon committed early to the Unified Ring topology (ADR 002): every node and every record resolves to a coordinate in one global hash space. The original implementation picked **HighwayHash-64** as the hash primitive and threaded the resulting `u64` through three distinct roles at once:

1. **Routing** — the position on the ring that selects which node stores a record.
2. **Storage lookup** — the key used by `LocalSlabStore` / `SlabManager` indices to retrieve a record.
3. **Network wire identity** — the field carried in HRFT request/response frames so server and client agree on which record is being read or written.

Collapsing three roles into one 64-bit field kept the ring elegant but is unsafe at data-warehouse scale. By the birthday bound, a 64-bit hash reaches 50% collision probability after roughly `1.2 * 2^32` samples, about 5 billion. A single tenant at tens of billions of rows is therefore already well past the point where silent collisions are merely theoretical, and the aggregate multi-tenant footprint makes the current shape indefensible. The storage-lookup role is the unforgiving one: when two keys collide on the index, the newest record silently hides the other, and neither the server (`WendelonDataServer::read`) nor the client (`client::core::read`) currently verifies that the retrieved record's key matches the requested key. ADR 019's own rejection of random 96-bit GCM nonces acknowledges that 64 bits of entropy is insufficient for cryptographic uniqueness guarantees at our record counts; we should not quietly accept less for data-identity.

ADR 022 already hints at the resolution: the L2 slab footer is a 128-bit (`blake3(key)[0..16]`) index with linear-scan collision fallback. That fix, however, was local to L2 — L1 indices, in-memory maps, HRFT frames, and the Raft log still carried 64-bit hashes. A uniform resolution across the stack is required.

The pivot must not be allowed to re-introduce the "Control Plane" / "Data Plane" split the project has rejected. Preserving the single-ring mental model while restoring storage safety is the forcing constraint.

## Decision
Wendelon treats every record as having **two projections of a single identity**, distinguished by type:

1. **`Identity`** — a 256-bit BLAKE3 hash, stored as `Identity([u8; 32])` in `src/schema/units.rs`. This is the record's "True Name." All storage-layer decisions (slab indices, dedup, collision resolution, L2 lookups, write-path idempotency, audit correlation) **MUST** key on `Identity`.

2. **`Locality`** — a 64-bit projection, stored as `Locality(u64)`. Defined as the first 8 bytes of `Identity` and reachable via `Locality::from(identity)`. All ring-placement decisions (vnode assignment, primary/replica selection, range queries, ownership transfer on epoch transitions) **MUST** key on `Locality`.

The projection is one-way: `Identity → Locality` is defined; `Locality → Identity` is not. Any code path that tries to invert the projection is structurally broken.

The two types are distinct Rust newtypes with non-overlapping `From`/`Into` impls. Passing the wrong one is a compile error.

### Invariants
- **(I-1) Unified Ring.** There is exactly one ring. Identity and Locality are two views of the same underlying hash, not two rings.
- **(I-2) Storage authority.** No storage-layer read path returns a record without verifying the full 32-byte `Identity` matches the request. A `Locality`-only match is never sufficient.
- **(I-3) Routing economy.** The routing table and every RPC frame carrying a routing decision carry `Locality` only. `Identity` never appears in the ring topology's own bookkeeping (though it appears in request payloads that the ring routes).
- **(I-4) Projection purity.** `Locality::from(identity)` is the only sanctioned projection. Direct byte-slicing of `Identity` outside `src/schema/units.rs` is an architectural violation; the boundary is enforced first by field privacy and second by review/tests.
- **(I-5) Routing collisions are harmless.** Two distinct `Identity` values projecting to the same `Locality` share a home on the ring; the storage engine differentiates them at retrieval.
- **(I-6) Storage collisions are impossible by construction.** At 256 bits (2²⁵⁶ ≈ 10⁷⁷), a BLAKE3 collision is less likely than a spontaneous hardware-level integrity failure the system is already required to tolerate. Collision handling in the storage layer remains code-present (linear scan on equal `Locality`) to uphold invariant I-2, but the branch is exercised by routing collisions, not by Identity collisions.

### Primitive and size choices
- **Hash function: BLAKE3.** Chosen over HighwayHash-256 for (a) SIMD-parallel throughput that matches or beats HighwayHash on modern hardware, (b) cryptographic collision resistance for the storage-identity role, (c) industry-standard 256-bit output aligning with SHA-256 / git's future, (d) unified primitive for both storage identity and any future content-addressable use cases.
- **Identity width: 256 bits.** Rejected 128-bit for "sound sleep" / scale-invariance: the memory difference (16 GB vs 32 GB of raw key bytes per billion records in a `DashMap<Identity, _>`) is dwarfed by the record values themselves, and 256 bits removes any future pressure to re-widen the coordinate system.
- **Locality width: 64 bits.** Retained because (a) ring math and vnode placement are already tuned for `u64`, (b) routing collisions are harmless per I-5, (c) HRFT request meta frames stay compact.
- **Build-profile stability.** Record `Identity` is format-stable across build profiles. The FIPS build keeps BLAKE3 outside the validated module boundary for record identity / content addressing, matching ADR 026 Decision §3's explicit statement that "`blake3` for content addressing is outside the module boundary"; only the audit chain and CMVP-bound cryptographic operations switch primitives by profile.

### Newtype discipline
Both `Identity` and `Locality` are `#[repr(transparent)]` with `Pod + Zeroable` to preserve zero-copy (`bytemuck`) compatibility on the wire. They sit in `src/schema/units.rs` alongside the rest of the coordinate units (`SlabId`, `LogIndex`, `Epoch`, `TenantId`). This generalizes: every fundamental quantity in Wendelon is a newtype, not a raw primitive. The compiler's type system is the enforcement mechanism for the Identity/Locality invariants; architectural tests are the second line of defense.

## Rationale
- **Preserves the Unified Ring.** The ring is still one hash space. What changes is that the hash space is 256 bits wide, of which routing cares about 64 bits. This is a *precision* split, not a *topology* split — no "plane" is introduced, and ADR 002 stands unamended.
- **Fixes the storage safety gap without cross-cutting breakage.** Because `Locality` retains its `u64` width, W3's Raft ring math and routing tables need not change shape. The refactor surface is confined to (a) `src/schema/units.rs` (already landed), (b) the storage indices and wire frames that previously used `u64` as a storage key, (c) the record-hash helper and vnode-placement hash helper, both of which move to BLAKE3-derived implementations while preserving the `u64` topology surface.
- **Aligns the W1 storage seam with the L2 ADR 022 format.** Both tiers now operate on high-entropy identities; L2's 128-bit-prefix index becomes a `Locality`-like projection of the same 256-bit Identity rather than a separate hash family.
- **Physics-of-the-model framing.** Routing is a question of *where* (locality); storage is a question of *what* (identity). Conflating the two for a single `u64` is the same category error as conflating position and momentum. Splitting them at the type level lets the compiler prevent cross-axis mistakes that a regex-based architectural test would only catch opportunistically.
- **Forward-compatible with content-addressable storage.** If Wendelon ever adopts content-addressed dedup or immutability modes, BLAKE3-256 is already the industry standard, and the `Identity` type is ready without schema migration.

## Consequences (The "Negatives" / Rejected Ideas)
- **On-disk and on-wire format break.** `SlabRecordHeader` grows from 8 bytes of key-hash to 32 bytes. `RecordLocation` widens. HRFT `ReadRequestMeta` / `WriteRequestMeta` gain 24 bytes in the key-identity field. All Arrow schemas using `UInt64` for key-hash move to `FixedSizeBinary(32)` (or `FixedSizeBinary(8)` where `Locality` is the correct field). There is no pre-W1 persistent data to migrate, so this is accepted as a one-time format break, not a migration problem; the break lands before W1's acceptance closes.
- **Memory overhead in the L1 index.** `DashMap<Identity, Vec<RecordLocation>>` at one billion entries costs on the order of tens of GB just for the keys. We accept this; a data warehouse that cannot afford 32-bytes-per-key for its hot index is not the workload we are serving.
- **Hashing throughput trade-off.** BLAKE3 is fast but is not free relative to HighwayHash-64. For the write path this is amortized against network + fsync + Raft costs and is not on the critical-path CPU budget. For WPBC pushdown paths that do per-row hashing, the cost is real and budgeted as part of W8's perf acceptance.
- **Rejected: keep HighwayHash, widen to HighwayHash-128.** Rejected because (a) it does not get us to "sound sleep" — still birthday-bounded around 2⁶⁴ records for multi-tenant scale, (b) it forecloses the future content-addressable-storage option that wants a cryptographic hash, (c) it preserves a non-cryptographic dependency that BLAKE3 subsumes for both record identity and vnode placement.
- **Rejected: retain `u64` key-hash in L1 indices and add a parallel key-bytes verification column.** Rejected because it re-encodes the failure mode at a higher layer (verification becomes fallible / forgettable at every call site), and it leaves the wire frames ambiguous about which field is authoritative. The type-level split forces the question at the API boundary.
- **Rejected: store the raw key bytes as the storage identity.** Rejected for variable-size keys (slab layout, DashMap cost), tenant-PII exposure in indices, and because hashing gives a uniform, projection-friendly coordinate system that raw bytes cannot.
- **Rejected: 128-bit Identity.** Considered and rejected in favor of 256 bits; see "Primitive and size choices" above.
- **Architectural test surface grows.** `tests/architecture_test.rs` now owns an additional responsibility: assert that the routing topology (ring tables, replica selection) compiles only against `Locality`, and that any storage-layer lookup compiles only against `Identity`. Codex, Claude, and any future contributor should expect the build to fail noisily on cross-use.
- **Documentation drag.** Every workstream doc W1-W10 inherits an "Identity/Locality impact" section. This ADR alone does not discharge that drag; see the Open Questions below.

## Open Questions
- **Arrow representation of Identity in WPBC payloads.** `FixedSizeBinary(32)` is the natural choice, but whether WPBC opcodes that currently index by `UInt64` (range filters, dedup operators) should operate on the raw 32-byte buffer or on a lexicographic-ordered `Locality`-then-`Identity` composite is a W8 decision. Tracked under W8's open items.
- **Audit canonical schema hash identity.** ADR 025's hash chain currently references records by 64-bit hash; whether to widen to 256-bit Identity or keep 64-bit Locality for audit-log compactness (with the record payload carrying the full Identity) is a W9 decision. Tracked under W9's open items.
- **Tenant routing projection.** When a `TenantId` is combined with a key to produce an Identity (ADR 021's namespace-isolation guarantee), whether the API expression is `blake3(tenant_domain || tenant_id || key_bytes)` or `blake3_keyed(tenant_derived_key, key_bytes)` is a W5 decision. The output bytes stay BLAKE3-256 either way; the choice is about domain-separation expression, not build-profile variation.
- **Salt / keyed hashing for anti-abuse.** Tenant-derived keyed mode is domain separation, not secrecy, because `tenant_id` is public. Whether routing should add a cluster-secret salt to prevent adversarial tenants from engineering locality hotspots is deferred to a future ADR once tenant threat models are concrete in W5.
