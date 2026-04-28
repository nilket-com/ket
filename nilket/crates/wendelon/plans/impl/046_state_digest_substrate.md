# Implementation Plan: ADR 046 — StateDigest Substrate

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/046_state_digest_substrate.md`
Companion brainstorm: `plans/brainstorms/014_algebraic_record_identity_ecmh.md`
Cut-2 bench evidence: `plans/impl/046_state_digest_bench_results.md`

## Goal

Implement the algebraic `StateDigest` substrate needed by the
post-045 roadmap without changing ingestion, slab layout,
descriptors, control-ring metadata, or network protocols.

The deliverable is a small, well-tested hash-layer module:

- 32-byte compressed Ristretto255 `StateDigest`;
- cold-path `StateDigestAccumulator`;
- domain-separated element projection;
- batched scalar accumulation for L2-shaped workloads;
- visible-row helper for current table rows;
- golden vectors and algebraic invariant tests;
- L2-seal-shaped benchmark over existing row-identity batches.

This plan intentionally does not persist digests, integrate with
slab seal, or wire any consumer. Durable placement and consumer
integration belong to the first consumer ADR that needs them.

## Non-goals

- No slab-footer change. No sidecar block. No CSB locator change.
- No `TieredSlab` / `TieredSlabPointer` / `TieredSlabObjectMetadata`
  field.
- No `TableSchemaDescriptor` field.
- No wire-frame change. No new HRFT family.
- No new node capability bit.
- No `ObjectStore` trait change.
- No replacement of `Identity`.
- No routing topology / `NodeId` change. No FROST work.
- No anti-entropy gossip protocol.
- No row-tombstone, retention, secondary-index, CDC, or LSM
  behavior. Substrate only.
- No hot-path curve math anywhere in `src/network/data_server.rs`
  or `src/storage/manager.rs`.
- No compatibility shim, reserved field, or `V1` / `V2` Rust-
  type suffix.

## Phase 0 — Contract freeze and dependency audit

Lock the substrate contract before touching production code.

Files:

- `Cargo.toml`
- `tests/fixtures/state_digest/` (new)

Work:

1. Add `curve25519-dalek` as a *direct* dependency. Already
   present transitively via `ed25519-dalek`; ADR 046 should not
   rely on a transitive for a first-class state primitive. Pin
   a version compatible with Wendelon's other crypto crates.
   Architectural review for any new transitive surface. The
   selected dependency disables crate defaults and enables only
   `alloc` plus `precomputed-tables`, keeping the runtime feature
   surface explicit under both default and FIPS workspace builds.
2. Confirm chosen crate features compile under default and FIPS
   feature sets. Ristretto255 is outside the FIPS validation
   boundary, like BLAKE3 content identity; document any feature
   gating if needed.
3. Freeze public type names:
   - `StateDigest`
   - `StateDigestAccumulator`
   - `StateDigestDomain`
   - `StateDigestElement`
4. Freeze domain tags as byte strings, not serialized enum
   variants. Semantic names remain unversioned in docs; the
   byte tags carry `.v1` because they feed golden vectors:

   | Semantic domain | Domain-tag bytes | Owner |
   | --- | --- | --- |
   | `table.row.visible` | `digest.table.row.visible.v1` | this ADR |
   | `table.row.physical` | `digest.table.row.physical.v1` | canonical row-fact consumer |
   | `table.row.tombstone` | `digest.table.row.tombstone.v1` | row-tombstone ADR |
   | `secondary_index.entry` | `digest.secondary_index.entry.v1` | secondary-index ADR |
   | `retention.gc` | `digest.retention.gc.v1` | retention ADR |

   Only `table.row.visible` ships with a preimage helper in V1.
   Others are reserved namespace per ADR §4 boundary statement.
5. **Lock projection function to hash-to-scalar** per ADR §3:
   ```text
   point = Scalar::from_bytes_mod_order(
       BLAKE3-256(
           "state-digest.element"
           || domain_tag_len_be_u32
           || domain_tag
           || preimage_len_be_u64
           || preimage
       )
   ) * RISTRETTO_BASEPOINT_POINT
   ```
   `RistrettoPoint::from_uniform_bytes(BLAKE3-XOF-512(...))` is
   documented as fallback only if security review rejects
   hash-to-scalar. The Phase 4 bench measures throughput
   against acceptance criteria; it does not pick between
   candidates unless security review forces a re-bench. The
   production implementation may use dalek's precomputed
   `RISTRETTO_BASEPOINT_TABLE`, which is the same fixed
   basepoint as `RISTRETTO_BASEPOINT_POINT` with faster scalar
   multiplication.
6. Produce a short local bench note covering 10k / 100k / 1M
   element batches, including a batched scalar multiplication
   path and an LtHash baseline for scale comparison. LtHash is
   benchmark-only in ADR 046, not a production alternative.

Golden fixtures under `tests/fixtures/state_digest/`:

- Empty digest (Ristretto255 identity-point byte sequence).
- Single visible-row element with a pinned `(tenant, table,
  row_key_id)` tuple → expected 32-byte point.
- Two visible-row elements in `(A, B)` and `(B, A)` order →
  same digest (commutativity).
- Split: `Digest(A) + Digest(B) == Digest({A, B})`.
- Subtraction: `Digest({A, B}) - Digest(B) == Digest(A)`.
- Cross-domain non-collision: identical `(tenant, table,
  row_key_id)` under `digest.table.row.visible.v1` vs
  `digest.table.row.physical.v1` → different points
  (regression guard for domain separation).
- Identity-vs-digest preimage non-collision: ADR 045 identity
  preimage `"table.append-row" || ...` vs ADR 046 digest
  preimage `"digest.table.row.visible.v1" || ...` over the
  same logical fields → different scalars (architectural
  regression guard).

Exit:

- Dependency choice compiles under all feature sets.
- Type names and domain-tag bytes are frozen.
- Projection function is locked (hash-to-scalar).
- Golden fixtures exist and are referenced by tests (which
  fail with `unimplemented!()` until Phase 1).

## Phase 1 — Hash-layer module: types, primitives, batched accumulation

Files:

- `src/hash/state_digest.rs` (new)
- `src/hash/mod.rs`
- `src/schema/units.rs`

Work:

1. Add `StateDigest` as a transparent 32-byte newtype:
   ```rust
   #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Pod, Zeroable)]
   #[repr(transparent)]
   pub struct StateDigest([u8; 32]);
   ```
   Required derives also include `Serialize`, `Deserialize` for
   future wire/disk use at consumer call sites.
2. Constructors / accessors:
   ```rust
   impl StateDigest {
       pub const ZERO: Self;        // compressed Ristretto identity point
       pub const fn new(bytes: [u8; 32]) -> Self;
       pub const fn as_bytes(&self) -> &[u8; 32];
       pub const fn into_bytes(self) -> [u8; 32];
       pub fn decompress(self) -> Result<RistrettoPoint, StateDigestError>;
   }
   ```
   `ZERO` is the compressed Ristretto identity point, not
   `[0; 32]` unless the selected implementation proves those
   bytes are identical.
3. `StateDigestAccumulator`:
   ```rust
   pub struct StateDigestAccumulator {
       point: RistrettoPoint,  // accumulated uncompressed
   }

   impl StateDigestAccumulator {
       pub fn new() -> Self;
       pub fn from_digest(digest: StateDigest) -> Result<Self, StateDigestError>;
       pub fn add_element(&mut self, element: StateDigestElement);
       pub fn subtract_element(&mut self, element: StateDigestElement);
       pub fn merge_digest(&mut self, digest: StateDigest) -> Result<(), StateDigestError>;
       pub fn subtract_digest(&mut self, digest: StateDigest) -> Result<(), StateDigestError>;
       pub fn finish(self) -> StateDigest;
   }
   ```
4. `StateDigestElement` and `StateDigestElement::project`:
   ```rust
   pub struct StateDigestElement {
       point: RistrettoPoint,
   }

   impl StateDigestElement {
       pub fn project(domain: StateDigestDomain, preimage: &[u8]) -> Self;
   }
   ```
5. `StateDigestDomain`:
   ```rust
   #[repr(transparent)]
   pub struct StateDigestDomain(pub &'static [u8]);
   ```
   Expose constants for current semantic tags. Call sites must
   use named constants, not inline `StateDigestDomain(b"...")`
   construction (architectural test in Phase 3 enforces).
6. `StateDigestError`:
   ```rust
   pub enum StateDigestError {
       InvalidPoint,
       MalformedDomainTag,
   }
   ```
7. **Batched accumulation helper.** Cold-path consumers (L2
   sealer, future anti-entropy) must avoid a multiply-per-row
   loop where possible. Expose:
   ```rust
   pub fn accumulate_projected_scalars(
       scalars: impl IntoIterator<Item = [u8; 32]>,
   ) -> StateDigest;
   ```
   Internally folds the reduced scalars in scalar space and
   multiplies once by dalek's precomputed Ristretto basepoint
   table:
   ```text
   Σ(s_i * G) == (Σs_i) * G
   ```
   This is equivalent to a multiscalar multiplication where
   every point is the same basepoint, but avoids a
   multiply-per-row loop. The public per-element API stays
   ergonomic for small consumers; the batched helper proves
   L2-seal viability at scale.

Tests:

- `StateDigest::ZERO == StateDigest::new([0; 32])` (verifies
  Ristretto255 identity-point byte representation).
- `StateDigest::ZERO.decompress()` succeeds.
- A 32-byte buffer that does not decompress to a valid
  Ristretto255 point returns `InvalidPoint`.
- `accumulator.from_digest(d).finish() == d`.
- Phase 0 golden vectors pass.
- **Batched equivalence:** `accumulate_projected_scalars(xs)`
  equals an element-by-element accumulation of the same `xs`
  for any input batch. Critical correctness invariant for
  Phase 4 bench composition.
- Domain separation golden tests pass.
- Visible-row preimage tests assert the 16-byte tenant slot is
  `TenantId`'s current 8-byte little-endian value followed by
  8 zero bytes; widening `TenantId` requires an explicit
  substrate contract update.

Exit:

- `cargo test --lib state_digest` passes.
- Module is reachable from `src/hash/`. No production storage
  path imports it.

## Phase 2 — Visible-row helper

Files:

- `src/hash/state_digest.rs`
- `src/schema/units.rs`
- `tests/state_digest_invariants.rs` (new)

Work:

1. Implement the only V1 row-semantic helper per ADR §4:
   ```rust
   pub fn visible_row_element(
       tenant_id: TenantId,
       table_object_id: TableObjectId,
       row_key_id: Identity,  // = __row_key_id__
   ) -> StateDigestElement {
       let mut preimage = [0u8; 64];
       preimage[..8].copy_from_slice(&tenant_id.get().to_le_bytes());
       // bytes 8..16 stay zero; this is the canonical 16-byte tenant slot
       preimage[16..32].copy_from_slice(table_object_id.as_bytes());
       preimage[32..64].copy_from_slice(row_key_id.as_bytes());
       StateDigestElement::project(DOMAIN_TABLE_ROW_VISIBLE, &preimage)
   }
   ```
2. **Do not add helpers for physical / tombstone / retention /
   index-entry domains.** Their preimages belong to their
   owning ADRs.

Tests:

- Two rows with the same `row_key_id` in different tables
  produce different elements.
- Two rows with the same `row_key_id` in different tenants
  produce different elements.
- Visible digest does not change when the same rows are
  accumulated in different orders.
- Split / merge visible digest equality over the same row set.
- Cross-domain: same logical bytes under
  `digest.table.row.visible.v1` vs `digest.table.row.physical.v1`
  → different elements (architectural regression guard).
- Append-only `__row_key_id__` values from ADR 045 fixtures
  feed `visible_row_element` and the digest matches the
  Phase 0 golden vector.

Exit:

- Existing row-identity types feed the digest substrate without
  any storage-layer change.

## Phase 3 — Algebraic invariant suite + architectural tests

Files:

- `tests/state_digest_invariants.rs`
- `tests/architecture_test.rs`

Algebraic invariants (against deterministic 1 / 2 / 10k row
sets, including intentionally repeated `row_key_id`s for
multiset-behavior checks):

- **Commutativity:** `Digest(rows) == Digest(shuffle(rows))`.
- **Associativity / split:** `Digest(A) + Digest(B) == Digest(A ∪ B)`.
- **Subtraction:** `Digest(A ∪ B) - Digest(B) == Digest(A)`.
- **Multiset semantics:** `Digest(A, A) ≠ Digest(A)` and
  `Digest(A, A) - Digest(A) == Digest(A)`.
- **Domain isolation:** `project(VISIBLE, x) ≠ project(PHYSICAL, x)`.
- **Batched equivalence:** `accumulate_projected_scalars(xs) ==
  Σ project(visible, xᵢ).finish()` for any input batch.

Architectural tests (added to `tests/architecture_test.rs`):

- **Hot-path curve isolation:**
  ```sh
  rg -n 'use\s+curve25519_dalek' src/network src/storage
  ```
  Returns empty (substrate is reachable from
  `src/hash/state_digest.rs` only; admission, request routing,
  L1 ingestion stay clean).
- **Every `StateDigestDomain` construction in `src/` is a
  module constant** in `src/hash/state_digest.rs`, not an
  inline call-site byte string.
- No `__row_tombstone__` column introduced by ADR 046.
- No `AppendOnlyContentHash` variant introduced.
- No `NodeCapability::*StateDigest*` / `*ECMH*` symbol.
- No slab `format_version` bump on the ADR 046 diff scope.
- No `StateDigest` field added to `TieredSlab`,
  `TieredSlabPointer`, `TieredSlabObjectMetadata`,
  `TableSchemaDescriptor`, or any L2 footer struct.
- No `V1` / `V2` suffix on ADR 046 surfaces.
- No project-name prefix on ADR 046 surfaces.
- **Identity-vs-digest preimage isolation:** no source file
  contains both `b"table.append-row"` and
  `b"digest.table.row."` (regression guard for accidental
  preimage tangling).

Exit:

- Algebraic properties verified independently of storage layout.
- Hot-path curve isolation enforced by tests, not just
  convention.
- Architectural tests prove ADR 046 stayed a substrate ADR.

## Phase 4 — Cold-path benchmark with LtHash baseline

Cut-2 status: complete. Results are recorded in
`plans/impl/046_state_digest_bench_results.md`.

Files:

- `benches/state_digest.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `src/hash/lthash_baseline.rs` (`#[cfg(test)]` only —
  benchmark-comparison reference, *not* shipped in production
  builds)

Work:

1. Implement an LtHash baseline at the same domain-tag and
   preimage shape as Ristretto255. Accumulates 16-bit modular
   sums into a 1024-element state array (~2 KB). Reference
   only; `cfg(test)`. The manual bench carries its own copy of
   the tiny baseline because Cargo bench targets do not compile
   the library with `cfg(test)`.
2. Add `[[bench]]` for `state_digest`. Use the repo's manual
   benchmark style (`Instant`, `black_box`, stdout
   measurements), consistent with `benches/multi_row_fragments.rs`.
3. Pre-generate `Identity` value batches to isolate curve cost
   from row synthesis.
4. Bench shapes:
   - 10k visible-row elements;
   - 100k visible-row elements;
   - 1M visible-row elements;
   - **Batched accumulation via `accumulate_projected_scalars`**
     at the same sizes (this is the hot-cold-path cost story);
   - Per-element accumulation for comparison;
   - Split / merge over 16 partitions;
   - Subtraction over 10% removed rows;
   - **LtHash baseline** at the same sizes (storage-vs-CPU
     tradeoff evidence).
5. Report:
   - elements/sec/core (Ristretto per-element vs batched vs
     LtHash);
   - ns/element for projection vs accumulation;
   - time to digest a 64 MiB-shaped slab at representative row
     widths;
   - compressed-digest validation cost (`finish()` overhead).
6. Acceptance criteria for council promotion:
   - **Minimum:** 45–60 K projected elements/sec/core on target
     hardware (brainstorm 014's prior expectation).
   - **Target:** 10k elements ≤ 100 ms; 100k elements ≤ 1 s;
     batched path ≥ 2× per-element throughput.
   - Compression cost on `finish()`: < 5 µs.
   - If neither path clears the minimum, council pauses
     promotion and re-opens projection / batching / primitive
     choice.
7. **Bench results document** captures the Ristretto throughput
   numbers, the LtHash storage-vs-CPU tradeoff, and the
   recommendation to council. If hash-to-scalar is disqualified
   by security review post-bench, the impl plan and ADR §3
   rewrite to use `from_uniform_bytes` and the bench re-runs
   against the same input batches.

Exit:

- Bench results documented; ADR §3 throughput acceptance
  criteria satisfied (or council pause triggered).

## Phase 5 — Optional cold-path smoke test

Optional. Skip if it pulls storage-format changes into ADR 046
scope.

Cut-3 status: complete as a test-only integration smoke in
`tests/state_digest_invariants.rs`. The test packs the same
append-only logical rows under different `MultiRowLayoutDescriptor`
grouping boundaries, extracts existing stored `__row_key_id__`
metadata from decoded multi-row frames, and proves the visible-row
digest is unchanged. No production storage helper, durable field, or
sealer integration was added.

Files:

- `tests/state_digest_invariants.rs`
- `src/storage/multirow.rs` (test-only helper, gated
  `#[cfg(test)]`)

Work:

1. Add a test-only helper that extracts visible-row elements
   from already-packed multi-row source rows.
2. Compute a digest over packed rows in memory.
3. Repack the same rows into different multi-row groupings and
   prove the visible digest is unchanged. Validates ADR §5
   compaction invariants against real multirow packing
   machinery.

Strict rules:

- No production sealer field.
- No on-disk write.
- No `TieredSlabObjectMetadata` field.
- No control-ring entry.

Exit:

- Substrate proven against current multi-row machinery without
  any durable surface.

## Phase 6 — Promotion: security review + throughput ratification

Cut-2 status: complete for the substrate. Hash-to-scalar review and
throughput ratification are recorded in
`plans/impl/046_state_digest_bench_results.md`. Consumer-facing
storage, protocol, lifecycle, and index semantics remain deferred.

Files:

- `src/hash/state_digest.rs`
- `plans/adr/046_state_digest_substrate.md`
- `plans/impl/046_state_digest_substrate.md` (this file)
- `plans/ops/runbooks/state_digest_substrate.md` (new)
- `plans/ops/observability.md` (extend)

Work:

1. **Security review on hash-to-scalar.** Confirm the modulo
   bias is acceptable for digest-only use (no signature / KDF
   exposure). Default expectation: approved. If rejected,
   rewrite ADR §3 and Phase 0 to use `from_uniform_bytes`,
   re-run the bench, and re-promote.
2. Confirm benchmark satisfies §2 acceptance criteria for
   hash-to-scalar.
3. Confirm storage-placement decision remains "no durable
   placement in V1."
4. Confirm no future-surface pre-allocation slipped in:
   - No tombstone column.
   - No content-hash identity variant.
   - No topology key.
   - No secondary-index digest field.
   - No footer / sidecar / control-ring storage.
5. Confirm lifecycle semantics are still deferred:
   - No decision on subtract-on-delete vs tombstone event-
     history (ADR §6 / OQ#3).
   - No retention digest preimage.
   - No physical row-fact encoding.
6. Runbook: short orientation. What `StateDigest` is. What it
   isn't (yet — no anti-entropy until follow-up ADR). How to
   read the bench results when consumers turn on metrics.
7. Update roadmap reference if ADR numbering changes.

## Verification gate

Required before declaring the substrate shipped:

```sh
cargo test --lib state_digest
cargo test --test state_digest_invariants
cargo test --test architecture_test --quiet
cargo bench --bench state_digest --no-run
cargo bench --bench state_digest -- --quick
git diff --check
rustfmt --check --edition 2024
```

Grep guards:

- Domain-tag well-formedness:
  ```sh
  rg -n 'b"digest\.[a-z_.]+\.v[0-9]+"' src tests benches | sort -u
  ```
  Output matches the constants in `src/hash/state_digest.rs`.

- Hot-path curve-crate isolation:
  ```sh
  rg -n 'use\s+curve25519_dalek' src/network src/storage
  ```
  Returns empty.

- No reserved enum slots / future-surface pre-allocation:
  ```sh
  rg -n '(?:TableRowVisible|TableRowPhysical|SecondaryIndexEntry|RetentionGc)\s*=>' src/hash
  ```
  Returns empty (domains are constants, not enum arms).

- No version suffixes on Rust types:
  ```sh
  rg -n 'StateDigestV[0-9]|StateDigestDomainV[0-9]|StateDigestAccumulatorV[0-9]|StateDigestElementV[0-9]' src tests
  ```
  Returns empty.

- No project-name prefix:
  ```sh
  rg -n 'wendelo[n]_.*digest|Wendelo[n].*Digest|wendelo[n]_.*ecmh' src tests benches plans
  ```
  Returns empty.

- No `format_version` bump on the ADR 046 diff scope:
  ```sh
  rg -n 'format_version\s*[=:]\s*[3-9]' src
  ```
  Empty.

- No `StateDigest` field on storage / catalog structs:
  ```sh
  rg -n 'StateDigest\b' src/storage src/network src/control_ring
  ```
  Empty (substrate reachable from these modules only at
  consumer-ADR time).

Convention guards:

- No new V1/V2 suffix on ADR 046 surfaces.
- No `#[serde(default)]` shim.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family.
- No new node capability bit.
- No `ObjectStore` trait break.
- No new storage-only synthetic column.
- No on-disk shape change.

## Deferred to bench / security review / council

Mirrors the ADR open-questions list:

1. **Hash-to-scalar security review.** Phase 6 ratifies; default
   expected: approved. If rejected, fall back to
   `from_uniform_bytes`.
2. **Cold-path budget threshold.** Phase 4 reports; council
   ratifies before the substrate implementation is declared
   shipped.
3. **Lifecycle model** (subtract-on-delete vs event-history):
   row-tombstone ADR's call.
4. **Physical row-domain timing:** consumer-pulled.
5. **LtHash pivot if Phase 4 disqualifies Ristretto255.** ADR
   and plan rewritten.
6. **Element-scalar caching in slab metadata.** No V1
   commitment.

## Deferred to follow-up ADRs

Mirrors ADR Deferred section. Substrate consumers come later:
anti-entropy gossip protocol, row-tombstone, retention,
secondary-indexes, CDC, LSM, vector embeddings, content-hash
identity.
