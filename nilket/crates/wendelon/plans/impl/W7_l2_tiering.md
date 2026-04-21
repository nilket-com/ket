# W7 — L2 Tiering

**Effort band:** L (8–16 eng-weeks)
**Predecessors:** W4 (envelope encryption), W6 (write path)
**Folds:** `plans/014_lazy_tiering_sweeper.md`, `plans/017_autonomous_eviction.md`, `plans/019_lazy_migration.md`, `plans/025_l2_rehydration.md`

End of W7, slabs land on NVMe, get lazily uploaded to L2 under the envelope-encryption hierarchy, get rehydrated on demand, and get reclaimed across all four reclaim tracks when tenants or slabs are deleted. The `ObjectStore` surface is consumed through a provider-neutral OpenDAL contract, validated initially against RustFS and kept portable to B2, GCS, S3-compatible, or other providers without rewriting the storage path.

## 1. ADRs consumed

- **ADR 006 (Tiered Storage and Lazy Migration).** Lazy-migration sweeper; hot-on-L1, cold-to-L2 lifecycle; rehydrate-on-miss.
- **ADR 014 (rustfs Test Backend).** rustfs is the integration-test substrate for the provider-neutral `ObjectStore` trait.
- **ADR 022 (L2 Slab Format and Footer-Indexed Range Reads).** Slab byte layout is identical on L1 and L2; footer indexes slabs for range-read pushdown.
- **ADR 023 (L2 Upload Lifecycle).** `Uploading → Uploaded → Committed` state machine; multipart threshold; commit-time tenant-quota accounting.
- **ADR 024 (L2 Orphan Cleanup and Remote Reclaim).** Four reclaim tracks (A: orphan multipart; B: orphan slab; C: tenant-delete; D: archival-tier) with leader-only reclaim authority and rate-limited DELETEs.
- **ADR 029 (Identity vs Locality).** ADR 022's 128-bit key-prefix index in the L2 slab footer is reclassified as a compact `Locality`-class projection of the 256-bit record `Identity`; linear-scan collision handling at read time then compares full `Identity` bytes to resolve the (now-rarer) collision branch.

## 2. Deliverables

1. **`ObjectStore` trait stabilized** with the surface in ADR 022 (`put`, `get`, `head`, `list`, `delete`, `multipart_*`, `abort_multipart`, `list_multipart_uploads`, `list_parts`). Implementations: OpenDAL-backed remote providers, with RustFS as one conformance target rather than the sole shape of L2.
2. **Lazy-migration sweeper** driving L1→L2 transitions per ADR 006 (folded from `014_lazy_tiering_sweeper.md`, `019_lazy_migration.md`).
3. **Autonomous eviction** from L1 under memory/disk pressure (folded from `017_autonomous_eviction.md`), gated by ADR 001 governance boundary.
4. **Slab format** with the ADR 022 footer, byte-identical on L1 and L2. Footer index enables range reads without full-slab GET.
5. **L2 upload lifecycle** per ADR 023 — multipart threshold, `(tenant_id → total_bytes)` counter increment at `Uploaded → Committed`, abort on failure.
6. **L2 rehydration** on L1 miss (folded from `025_l2_rehydration.md`), with threshold policy and hot-cache eviction.
7. **Four reclaim tracks** (ADR 024) with leader-only reclaim authority and per-tenant rate budgets; an archival-tier track is stubbed and documented as post-pilot.

## 3. Acceptance criteria (ADR-referential)

- (ADR 006) A write-then-read-then-age-then-read sequence exercises L1-only, L1+L2, and L2-only reads with byte-identical results.
- (ADR 014) the shared `ObjectStore` conformance suite passes against RustFS and stays portable to additional OpenDAL-backed providers; no single backend defines the contract.
- (ADR 022) A range read of a slab on L2 fetches at most the footer plus the requested byte range — verified by a mocked-ObjectStore call-count assertion.
- (ADR 023 §4) After disk loss on a node, the reconciliation-via-`list_multipart_uploads` sweep correctly resumes or aborts every pre-loss upload.
- (ADR 023 quota race) Commit-time accounting is exclusively at `Uploaded → Committed`; no other transition touches the tenant byte counter.
- (ADR 024) The leader does not issue a reclaim DELETE while a reclaim from the previous leader is still in flight; handoff is idempotent (re-scan is cheap).
- (ADR 024) DELETE rate stays under the per-prefix provider limit even under a full-tenant delete; rate-limit budget is exposed via W9 metrics.

## 4. Explicit non-goals

- Archival-tier reclamation (Glacier-class). **Post-pilot.**
- Server-side query pushdown against L2 range reads. **W8.**
- FIPS-compliant envelope for L2 (the primitive is W4's; W7 consumes).

## 5. Open items

- **ADR 022 Open Question — footer size budget.** Land a default and a variance-of-slab-size tolerance.
- **ADR 023 Open Question — multipart threshold.** Set a default against pilot network profile; document how to tune.
- **ADR 024 Open Question — reclaim-scan cadence under full-tenant delete.** Measure against pilot workload.

## 6. Test harness tier

- **Unit:** slab footer encode/decode; reclaim track state machines.
- **Property:** commit-time-accounting-only invariant; slab-byte-identical-across-tiers.
- **Integration:** full L1↔L2 round-trip against rustfs and against at least one additional OpenDAL-backed cloud account.
- **Simulator:** W10's mocked `ObjectStore` exercises disk-loss, leader churn, and partial-multipart scenarios.

## 7. Phasing (internal)

- **W7.a (M):** `ObjectStore` trait + provider-neutral remote backend + conformance harness (initially RustFS) + slab format.
- **W7.b (M):** Upload lifecycle + sweeper + rehydration.
- **W7.c (M):** Four reclaim tracks + leader-only authority.

## 8. Identity/Locality impact (ADR 029 addendum)

- **Slab footer index (ADR 022).** The footer's 128-bit (`blake3(key)[0..16]`) index is reframed as a projection of the 256-bit `Identity`; retained at 128 bits for footer compactness. Naming: `locator` (current) or a dedicated `IdentityPrefix` newtype in `src/schema/units.rs` — close at W7 kickoff. The footer *must* store enough of the Identity to permit byte-exact verification at read time; options:
  - **(a)** Store full 32-byte Identity per index entry (footer grows; simplest).
  - **(b)** Store 16-byte prefix in the index + full Identity inline with the record body, verified on read (current ADR 022 shape, adjusted).
  - Default to (b); (a) is a fallback if (b)'s reader cost is measured as material.
- **Byte-identical L1/L2 slab bodies.** The slab body carries the full 32-byte `Identity` per record header; same bytes on disk as on L2 object. ADR 022's "byte-identical across tiers" invariant holds.
- **Routing of rehydration.** L2 rehydration request carries `Identity` (what to fetch) + `Locality` (which node should cache it); no ambiguity about which projection is in play.
- **Reclaim tracks.** Track C (tenant-delete) benefits directly from W5's tenant-scoped Identity construction: every `Identity` in the tenant's key space is reachable and no cross-tenant orphan looks like a deletion candidate.
- **Range reads under pushdown.** WPBC range filters from W8 pass a `LocalityRange` (for primary routing) plus a `FixedSizeBinary(32)` match predicate on Identity (for intra-slab filtering). The footer index delivers candidate records by Locality prefix; the read path verifies by Identity before returning.
- **Object-store key layout.** L2 object keys may embed `tenant_id/{slab_id}` (already ADR 024 shape) and are unaffected by the Identity pivot; Identity lives inside the slab body and footer, never in the object-store key.
