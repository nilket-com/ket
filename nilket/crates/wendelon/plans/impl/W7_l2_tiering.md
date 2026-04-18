# W7 — L2 Tiering

**Effort band:** L (8–16 eng-weeks)
**Predecessors:** W4 (envelope encryption), W6 (write path)
**Folds:** `plans/014_lazy_tiering_sweeper.md`, `plans/017_autonomous_eviction.md`, `plans/019_lazy_migration.md`, `plans/025_l2_rehydration.md`

End of W7, slabs land on NVMe, get lazily uploaded to L2 under the envelope-encryption hierarchy, get rehydrated on demand, and get reclaimed across all four reclaim tracks when tenants or slabs are deleted. The `ObjectStore` surface is consumed by real S3/GCS/Azure backends in production and by `rustfs` in integration tests.

## 1. ADRs consumed

- **ADR 006 (Tiered Storage and Lazy Migration).** Lazy-migration sweeper; hot-on-L1, cold-to-L2 lifecycle; rehydrate-on-miss.
- **ADR 014 (rustfs Test Backend).** rustfs implements the canonical `ObjectStore` trait and is the integration-test substrate.
- **ADR 022 (L2 Slab Format and Footer-Indexed Range Reads).** Slab byte layout is identical on L1 and L2; footer indexes slabs for range-read pushdown.
- **ADR 023 (L2 Upload Lifecycle).** `Uploading → Uploaded → Committed` state machine; multipart threshold; commit-time tenant-quota accounting.
- **ADR 024 (L2 Orphan Cleanup and Remote Reclaim).** Four reclaim tracks (A: orphan multipart; B: orphan slab; C: tenant-delete; D: archival-tier) with leader-only reclaim authority and rate-limited DELETEs.

## 2. Deliverables

1. **`ObjectStore` trait stabilized** with the surface in ADR 022 (`put`, `get`, `head`, `list`, `delete`, `multipart_*`, `abort_multipart`, `list_multipart_uploads`, `list_parts`). Implementations: S3, GCS, Azure, rustfs.
2. **Lazy-migration sweeper** driving L1→L2 transitions per ADR 006 (folded from `014_lazy_tiering_sweeper.md`, `019_lazy_migration.md`).
3. **Autonomous eviction** from L1 under memory/disk pressure (folded from `017_autonomous_eviction.md`), gated by ADR 001 governance boundary.
4. **Slab format** with the ADR 022 footer, byte-identical on L1 and L2. Footer index enables range reads without full-slab GET.
5. **L2 upload lifecycle** per ADR 023 — multipart threshold, `(tenant_id → total_bytes)` counter increment at `Uploaded → Committed`, abort on failure.
6. **L2 rehydration** on L1 miss (folded from `025_l2_rehydration.md`), with threshold policy and hot-cache eviction.
7. **Four reclaim tracks** (ADR 024) with leader-only reclaim authority and per-tenant rate budgets; an archival-tier track is stubbed and documented as post-pilot.

## 3. Acceptance criteria (ADR-referential)

- (ADR 006) A write-then-read-then-age-then-read sequence exercises L1-only, L1+L2, and L2-only reads with byte-identical results.
- (ADR 014) rustfs implements the full `ObjectStore` surface; integration tests against rustfs match integration tests against a real cloud-backed test account.
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
- **Integration:** full L1↔L2 round-trip against rustfs and against a real cloud-backed test account.
- **Simulator:** W10's mocked `ObjectStore` exercises disk-loss, leader churn, and partial-multipart scenarios.

## 7. Phasing (internal)

- **W7.a (M):** `ObjectStore` trait + rustfs backend + slab format.
- **W7.b (M):** Upload lifecycle + sweeper + rehydration.
- **W7.c (M):** Four reclaim tracks + leader-only authority.
