# ADR 0111 - A first tier never publishes a source that maintenance retired

## Status
Accepted for implementation, 2026-11-18. The plan was cleared by Codex 01a1ef3b, after source and evidence reviews 01a1ef09, 01a1ef17 and 01a1ef2f. Corrected under 01a1ef52: numbered 0111, the next Wendelon ADR on the published base; the retention admission moved inside the policed attempt; the claim carries its slab; unreadable-index controls added. Base: `6272e3a6`. Not delivered.

## Context
- **The defect (DC-262), demonstrated through ordinary selection** (one-node fixture):
  1. L1 retention (`prune_versions_for_retention`) replaces a sealed slab S. Its manifest run becomes `Retired`, the survivors move, the expired keys leave `key_index`, and **S stays in `state.sealed`**: in a bound store a retired source is terminal only through GC;
  2. the ordinary `sweep_once` then uploads and publishes S's original bytes as a first tier, with no acknowledged offsets.
- **At the tested floors** that generation prepares Observe. A later floor offers only a Rewrite of the copies that are indexed elsewhere, while its replacement and a Live control retire.
- **Links:** this is the suspected producer of the repeat retention work in one live point. The attribution to its specific slabs is inferred, not traced. DC-260 shares the producer, with a different consequence.
- **Why today's checks miss it:**
  - `tier_one_slab` held only the autonomous-action permit across its awaits;
  - its pre-submit `confirm_tier_candidate_quiescent` checked "sealed, quiescent, not tiered" but not the manifest run;
  - no op row exists between `read_sealed_raw` and `allocate_ops`;
  - the maintenance finishers' guards cannot see an outstanding first-tier proposal;
  - `tier_authority.pending` holds committed pointers awaiting verification, not proposals.
- **The invariants this decision establishes:**
  - **I1:** **a first tier published by this path** never names a slab whose manifest run is known non-Live. It holds from a clean start; historical pointers are not repaired;
  - **I2:** no maintenance decision makes a sealed S non-Live while an unresolved first tier of this store's owner names S.

## Decision

### D1. First-tier admission (`sweeper.rs`)
1. **A listing filter.** The tiering loop of `sweep_once` skips a candidate whose manifest run is **known** non-Live (`Retired` or `DeletePending`) before anything is read (`SweepReport::skipped_retired_source`). **A missing legacy run is eligible.** `list_sealed_slabs` is unchanged.
2. **The claim.** `SlabManager::try_claim_first_tier_source` is a non-blocking `try_write_owned` on S's per-slab rewrite claim, taken **before the read**.
   - Busy means skip the slab this pass, before reading (`skipped_source_busy`).
   - The claim, a `FirstTierClaim`, **names its slab**. `admit_first_tier_source` and `tier_one_admitted_slab` both refuse a claim for another slab. `tier_one_admitted_slab` holds it through the upload, the verification, `propose_tier_slab` and the idempotent apply fallback.
   - `tier_one_slab` is kept, test-only, as claim + admit + tier.
3. **Re-validation under the claim, before the read** (`admit_first_tier_source`):
   - S is sealed and quiescent, and not tiered;
   - its run is not known non-Live (`FirstTierSourceRetired`);
   - **no unresolved first tier of this owner already names S** (`FirstTierAlreadyPending`). That also refuses a redundant new upload after an earlier attempt was cancelled before it recorded its ambiguous-cleanup marker.

   A refusal reads and uploads nothing (`skipped_source_not_admitted`, logged at debug).
4. **The existing pre-submit `confirm_tier_candidate_quiescent`** also refuses a known non-Live run, and its existing definitely-unsubmitted cleanup applies.
5. **Unchanged:** the crypto check, `propose_tier_slab`, and the certainty-based upload lanes.

### D2. Sealed-maintenance admission: one claimed entry
- **The helper:** `admit_sealed_slab_for_maintenance(claims, S) -> SealedMaintenanceAdmission`, one of `Claimed`, `NotEligible`, `FirstTierPending`, or `Deferred(io::Error)`.
  - It runs ADR 0097's revalidation first. Not sealed, or a known non-Live run, gives `NotEligible`; a missing legacy run stays eligible.
  - Then the first-tier fence. **It sits under S's claim and before any read or effect**, the full-prune path's delete-pending pin and secondary-index reconciliation included.
- **Callers:** `claim_sealed_slab_for_maintenance` maps every non-`Claimed` outcome to its existing `None` ("nothing to do for this candidate"). That covers table drop, legacy prune, tombstone reclaim, certified cleanup, and `recluster_one_sealed_slab`, which the public `recluster_sealed_table_layout` reaches. A `Deferred` is logged at debug, once per candidate.
- **RetentionPrune keeps the failure-policy gate first.** Inside the policed attempt, so only after `maintenance_policy.execute` has admitted the candidate, it claims with `try_claim_sealed_slab_for_maintenance` and admits under the claim:
  - a busy claim gives `SealedSlabScan::ClaimBusy`;
  - a pending first tier gives `FirstTierPending`;
  - an unreadable op index gives `FirstTierUnknown`;
  - `NotEligible` keeps its previous `Unclaimed` scan.
- **What those three outcomes record.** Each maps to a new `SuccessDisposition::UnclaimedFor(reason)`. That records exactly what `Unclaimed` does (the reservation is released; no success, no cooldown, no failure) and emits **exactly one** attempt metric, its reason (`skipped_claim_busy`, `skipped_first_tier_pending` or `skipped_first_tier_unknown`). The pass does not count these against its slab budget (`SealedSlabScan::spends_budget`), and nothing is read or mutated.
- **A candidate the failure policy refuses** (backed off, quarantined, cooling, disabled) performs **no** op-index read and no scan.
- **The finishers are unchanged,** including the without-replacement finisher's unrelated silent no-op (DC-263, separate).

### D3. LSM admission: one async check per claimed input set, in all three modes
- **Where:** `refuse_lsm_plan_over_a_pending_first_tier(&plan)` runs right after `revalidate_lsm_plan_after_claims(&plan)` in each mode, under the plan's claims, **before outputs are prepared, the maintenance gate or any lock**:
  - local `run_lsm_compaction_once_inner`;
  - ordinary ring `run_lsm_compaction_once_inner_via_control_ring`;
  - certified `claim_lsm_for_certified_elision`.
- **How:** it selects the plan's **sealed** inputs under a brief state lock, released before the op-index read; the claims keep the set stable. **Tiered inputs are unaffected.** A pending first tier gives `CompactionFailure::FirstTierPending` (retryable, mapped like `CounterMaintenanceSkipped`). A read failure is `IoError`, a deferral.
- **Unchanged:** the synchronous manifest helper and `lsm_input_authority_locked`, which recovery and GC also use.

### D4. The reciprocal order: the existing fence, unchanged
- **The existing fence:** a first tier is refused store-wide while an LSM install intent is **orphaned**. Tier-authority readiness returns `LsmInstallUnresolved` when `has_orphaned_lsm_intents()`, which tests `!in_flight`, not every active install. The new path meets it in `read_sealed_raw`, under the first-tier claim.
- **An active install over the same input is excluded by claims.** A ring LSM job acquires its claims in the outer job function and holds them across `submit_op`. Its `LsmIntentOwner` is a local of the inner install function.
- **When the outcome is unknown:**
  1. the owner drops as that inner function returns, and its `Drop` sets `in_flight = false` (orphaned);
  2. only then does the outer function's error return release the claims.

  So a tier that next obtains S's claim always meets the orphaned scope.
- **The runtime scope exists before the cancellable index transaction:** `register_lsm_install_intent` creates it under `lsm_intent_scope_gate`.
- **After reopen,** the linked durable install intent reconstructs the fence.
- **Release** comes through recovery's resolved handoff (`finish_applied_install`, or the install's rejection).
- **The global fence is not extended.**

### D5. The op-index read
- **The method:** `PersistentLocalIndex::first_tier_row_names(owner, slabs)` reads the op table in order. It decodes one row at a time and stops at the first `SlabCommand::Tier` with `replaces == None` for exactly the owner (node and incarnation) and a slab in the set. A row that does not decode fails the read.
- **The manager wrapper** `first_tier_pending(slabs)`:
  - no persistent op index, or a store not bound to a control node: there is no first-tier producer, so `false`;
  - a bound store whose owner cannot be read: an error, never a silent `false`.

### D6. When the fence clears
- **Only `hand_off_op_with` deletes the row,** on:
  - durable delivery (`op_effect_is_durable`; S has then left `state.sealed`);
  - a durable rejection or verified-Cancel handoff;
  - the superseded handoff (`hand_off_superseded_tier_op`).
- **Why the superseded release is safe:** it needs, under one fresh lease, a final Applied decision for the current owner, already superseded, with exact local convergence. That is either the committed newer pointer, or a retirement in force with no local rows. After that, **this op** cannot later publish S's original generation. A new first-tier attempt still goes through D1 and the normal authority rules.
- **Never a release:** `ViewOutcome::None`, an ambiguous or failed Cancel, received-but-unapplied, Applied-not-durable (including a swallowed observer error), not current owner, acked-through, or conflict.
- **A row committed by an abandoned `allocate_ops`** is never submitted: recovery only views, cancels and hands off. It holds the fence until recovery cancels it.
- **The invariant this relies on:** while maintenance holds S's claim, no new live first-tier submission for S can originate. The sole `propose_tier_slab(…, None)` runs under S's claim (D1).

### D7. Lock order, census and residuals
- **The order:** a first tier takes claim(S), then (inside propose) the index write lock, then submit, then the apply observer: lease → `tier_publication_lock(S)` → `cleanup_claim(S)` → `persistent_index_gate` (read) → `lsm_intent_scope_gate` (read) → state. Maintenance takes claim(S) and the D2/D3 check, then gate write → tier lock → state.
- **The census** covers every rewrite-claim acquisition site in the tree (32, three of them test-only) and is recorded with the implementation evidence. No site is reached while its task holds the gate, a tier lock, a lease, `cleanup_claim` or an index gate. The index worker runs closures that take no async locks.
- **No wall-clock bound is claimed.**
- **Residuals:** while a first tier holds S, these may wait on S:
  - legacy prune, table drop, certified cleanup and backfill readers;
  - local GC marking (which holds earlier claims);
  - op-driver-awaited GC and install tasks;
  - **an LSM plan with sorted claims, which holds lower-id claims while waiting on S.**

  The independent-slab progress shown applies to a RetentionPrune pass when the other slab's claim is free.

### D8. Not changed
- historical pointers and existing stranded objects;
- DC-228 scheduling;
- DC-263;
- `list_sealed_slabs`, `lsm_input_authority_locked`, recovery and GC helpers;
- remote cleanup.

## Acceptance
1. **The regression suite** (`storage/sweeper/tests/retired_source_tier_tests.rs`, test-only) runs on a one-node ring of real requests through `RaftStateMachine::apply`, with the real observer, `op_view` and `verify_receipt`. **It is not a three-daemon Raft run.** Its declared fault seams act only on a Tier or Compaction `submit_op` after allocation.
   - **All nine baseline failures pass with their invariant assertions unchanged:**
     - retention-first;
     - a tier paused before linearization (S skipped, and the independent slab pruned in the same pass);
     - stale selection;
     - an undecided first tier, then a pass, then the late decision (S published **Live**);
     - the same across a restart;
     - a full prune, retention-first and paused;
     - a local LSM job, and a ring LSM job.
   - **Controls that pass:** verified Cancel; a pre-submit quiescence refusal (the unsubmitted upload removed, no op); the reciprocal store-wide refusal; **the certified LSM mode deferring** (`FirstTierPending`; manager tests).
   - **Fail-closed and policy controls that pass:**
     - one undecodable op row makes the shared first-tier read an error, and the sealed-maintenance admission `Deferred` (manager tests);
     - RetentionPrune with that row defers: nothing compacted, S Live, the expired row still indexed;
     - a local LSM job with that row fails with a **retryable** `IoError`, with no output run, no install intent, the manifest epoch unchanged and S Live;
     - **a candidate the policy refuses (neutral cooldown) performs no op-index read and scans nothing.**
   - **The boundary:** one test proves the shared read error. The caller-level tests prove the deferral mapping for RetentionPrune and the local LSM mode. The other compaction callers share `claim_sealed_slab_for_maintenance`, and the ring and certified LSM modes share `refuse_lsm_plan_over_a_pending_first_tier`.
2. **The existing offload modes and local/unbound workflows** stay green.
3. **Gate:** fmt and both architecture targets, then focused tests. The library, the 2 MiB stack check and the three-daemon offload/restart run follow under their own clearance.
