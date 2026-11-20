# ADR 0117 - A cleanup debt retires only when no copy can still be indexed

## Status
Accepted, 2026-11-20 (Codex 01a1f7a9, 01a1f7ad, 01a1f7b7): the narrow repair of DC-276.

**The scope:** cleanup-debt retirement only. No positive receipt, settlement or SQL enablement follows from it. It cannot authorize a physical-absence receipt.

## Context
**DC-276, reproduced:**
1. **The schedule:** a ParentDecisions child persist records its intent and owner, appends **durable** bytes, and then fails its row publication. Its parent is refused.
2. **The store is restart-required:** the publication is orphaned, and only appends and reads are fenced.
3. **Activation still runs.** It retires the intent and owner into exactly `RetiredIntent` debt.
4. **The ordinary debt sweep then retires that debt.** `retire_debt` counts only `RECORDS_TABLE` copies, and the failed write has none.
5. **An ordinary reopen recovers the bytes:** a copy whose stored row digest equals the refused child's manifest digest is indexed again, with no intent, owner or debt.

**A second route (control C3):** a store that holds a tiered slab skips the open-time durable rebuild, because tier verification is pending (`open.rs:1241`). Ready later clears that pending set **without** a rebuild (`tier_authority.rs:1609`). A recovered local copy is then indexed in memory but absent from `RECORDS_TABLE`, and the sweep retires its debt.

**The missing-slab experiment did not show such a route:** the slab it removed held an indexed row that no table integrity fence accounted for, so that open refused (`UnaccountedDurableVersions`). `open.rs` permits a typed missing row accounted for by its table's integrity fence, so the experiment excludes only the fixture it exercised.

## Decision
**The sweep retires a debt only when all of these hold, besides `retire_debt`'s own exact-debt, base, owner and `RECORDS_TABLE` checks in its write transaction:**
1. **The debt-absence authority is held.** `SlabManager::try_debt_absence_authority` read-holds `maintenance_publication_gate` without waiting, so a contended writer means skip. Under that guard it requires that this open's recovery is complete:
   - no deferred open;
   - no unresolved LSM install;
   - not awaiting the cluster;
   - index authority available;
   - **no outstanding orphaned index publication.**

   Each read is passive: no readiness call, authority start or tier scope.
2. **The version is absent from the in-memory index:** the passive `version_in_memory_index` on `key_index`, by tenant, key and HLC. A prefix match keeps the debt.

**The order and the guards:**
- **Order:** the producer stripe (DC-271, `RetiredIntent` only), then the authority. Neither waits; a busy one skips the entry.
- **Ownership:** both guards move into the retirement's database job (`retire_debt_holding`), so they last until its commit or abort even if the awaiting future is cancelled.
- **The skip:** a skipped entry changes nothing, and the cursor advances.

**The claim, bounded:**
- **Same-version parent-intent producers:** the producer marks its orphan, and rolls back its memory entry, before it releases the stripe.
- **Relocations by compaction, LSM install and GC disposal:** these publish under the gate's **write** guard, so the check sees the old copy or the new one, never neither.
- **Not covered, and remaining census obligations:** tier-pointer publication and refresh, retention reclaim, migration, and any gate site taken only for reading.

**Liveness:**
- while an orphaned publication is outstanding, no debt retires on that store. Certain single-append orphans reconcile in-process and decrement the counter; the reproduced failed publication required a reopen;
- an ordinary reopen recovers the copy;
- certified cleanup erases it by rewrite;
- the sweep then retires the debt.

**Certification continues throughout.**

**A test-only seam:** `NodeBootstrapConfig::hold_cleanup_at_start_for_test` holds cleanup and the sweep from a node's start. The test helper `force_seal_active_for_test` now covers its next slab id with the durable high-water, as every product allocation does.

## Consequences
- **An outstanding orphaned publication** vetoes all debt retirement on that store while it remains outstanding: until its in-process reconciliation, where single-append evidence allows one, or a reopen.
- **On a store whose durable mirror lags memory** (the skipped-rebuild route), debt waits until cleanup erases the in-memory copy.

## Acceptance
- **C1:** `debt_before_append::a_failed_publication_keeps_cleanup_evidence_until_its_copy_is_gone`. RED on `c386911a4`.
  - Under restart-required, the sweep keeps the exact debt.
  - After a same-incarnation reopen, the digest-validated copy and the exact debt both exist.
  - Cleanup erases the copy, and then the debt retires.
  - A second reopen holds neither.
- **C2:** `an_indexed_refused_version_keeps_its_debt_through_the_sweep` (a passing control).
- **C3:** `offload::dc276_a_tiered_stores_skipped_rebuild_keeps_the_debt_of_a_copy_only_in_memory`. RED on `c386911a4`.
  - The sweep keeps the debt while the copy is only in memory.
  - Cleanup then erases the copy, and the debt retires.
- **C4:** `storage::manager::tests::an_orphaned_publication_refuses_the_debt_absence_authority_until_reopen` (an ordinary append's failed publication, which has no in-process reconciliation).
- **Deferred recovery:** `the_debt_absence_authority_waits_for_a_deferred_open_to_finish`. The authority is refused before deferred loading starts and while it is held, and granted once it finishes.
- **The DC-271 and DC-272 tests** still pass.
