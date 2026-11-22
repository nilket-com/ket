# ADR 0124 - A certified worker is credited its aged rows from its own verified receipt

## Status
Proposed (local), 2026-11-22.
- **Per Codex:**
  - 01a204ad: DC-291 registered; GO for a counting-authority design.
  - 01a204b0: GO to record this plan, with four obligations: binding, replay, read order, deterministic controls.
  - 01a204bc: GO for the implementation, with clarifications folded in below: the controls' wording; D2's snapshot epoch; a shared submit path; heap cost; and the C5–C8 and D3 controls.
  - 01a204d8: HOLD for one D2 boundary, an install already in flight at entry; corrected by an unfenced entry observation (D2 step 1) and its control.
- **Implementation:** the paired `impl: adr 0124` commit.
- **Base:** origin/main `15512473d`.
- **Unchanged:** DC-289's sealed delivery gap and the Section 8 and 1 GiB holds.

## Context
- **ADR 0122 D4** records a certified tier job's aged rows (`wendelon_storage_table_retention_l2_rows_pruned_total`, the Section 8 series) from `record_aged_l2_if_applied`. That function re-derives "Applied" from the ring's **retained** outcome (`TierSnapshotLease::op_applied_for_current_owner`) and accepts that "an outcome no longer retained records nothing".
- **DC-291:** the store's own op driver acknowledges the worker's op as soon as it is handed off (`storage/op_driver.rs`, `ack_outcomes` over `load_ack_pending`). A worker delayed between acceptance and its count, by a WAL stall or a claim wait, therefore completes correctly yet records nothing.
  - **Demonstrated 3/3:** S1 held after acceptance; ring outcomes `[3,4]` → `[]` during the hold; the count reads `applied false`; tally 2, not 3; the survivor readable; normal serialization.
- **The evidence the count needs is in the worker's hands and then dropped.**
  - `verify_receipt` (`storage/sweeper.rs`) accepts a receipt only when its `OpKey` (owner node and incarnation, op_seq) and digest are the ticket's, and turns `Recorded(Applied { index })` into `VerifiedReceipt::Applied { index }`.
  - This holds on both production paths: `client_write` on a leader, and the owner-signed `owner_round` (a certified Tier or Retire first passes `require_submission_role`).
  - `submit_tier_proposal` and `submit_tier_retirement` match `Applied { .. }`, hand the op off, and return `()`.

## Decision
### Scope
- Only the aged-row credit of certified tier jobs (ADR 0704 / 0122) changes. The aged-L1 (certified LSM) credit is unchanged.
- **Unchanged:**
  - no format, wire, snapshot, WAL or durable-state change;
  - acknowledgement, the op window and its eviction, and the handoff run exactly as now; nothing delays or holds an ack.

### D1. The credit: the op's own handoff, the scope its command names, the job's own rows
- **One shared submit path.** `submit_tier_proposal_handed_off` and `submit_tier_retirement_handed_off` (`storage/sweeper.rs`) are the submit, verify and handoff code that `submit_tier_proposal` and `submit_tier_retirement` always ran.
  - On `VerifiedReceipt::Applied { index }` followed by a successful `hand_off_op_if_complete`, they return an `AppliedHandoff { key, digest, index }`, built only there from the submitted ticket (private fields, not `Clone`).
  - The ordinary functions are thin wrappers that drop it. Every caller keeps its result and its rejection, ambiguity and error paths.
- **The scope** `CreditScope` is read from the submitted ticket's **own command** before submission (`CreditScope::of_ticket`):
  - its key and digest;
  - the command's `SlabRef` and kind (`Tier` or `Retire`);
  - its certified provenance's op;
  - for a `Tier`, the candidate as a 32-byte blake3 digest of its URI.

  An uncertified command has no scope.
- **The credit** `CertifiedAgedCredit` is minted by `CertifiedAgedCredit::mint(scope, handoff, job.slab, kind, aged)` on the live certified job path only (`propose_certified_rewrite` / `propose_certified_retirement`). It carries the scope, the applied index and the job's own aged rows, with data and tombstone counts kept distinct. It is not `Clone`, `Copy` or serde.
- **Minting refuses (no credit) unless all of these hold:**
  - the handoff is the scope's op (key and digest);
  - the command is certified by that very op;
  - the command names the job's own slab and kind under that slab's owner;
  - a `Tier` has a candidate;
  - the rows are non-empty.
- **A refused credit records nothing and changes no result:** the delivered step returns exactly as before. A refusal never becomes an error or a retryable submission.
- **The credit is consumed by value** by the one count call (`record_aged_l2`). A second count of the same job is not expressible.

### D2. The count: read order and the decision point
`record_aged_l2(credit)` makes one conservative observation, with no local lock held across the lease:
1. **Entry: an unfenced starting epoch** (`unfenced_snapshot_epoch`).
   - It is read under `snapshot_epoch_gate`, which an install holds while it advances the epoch and raises its fence, so the epoch and the fence state are one observation. The gate is released at once, never across an await.
   - If an install is in flight, **nothing is credited** and the count returns at once.
   - This closes Codex 01a204d8's interleaving: an install advances E and raises its fence first; a count captured at E inside it would otherwise see E unfenced at its decision once that fence is released.
2. **Local sample** of the slab (the job still holds its slab-rewrite exclusive claim): the digest of `tier_pointer(slab)`'s URI, and whether the slab is retired here. Each is read under its own lock, released before step 3.
3. **One canonical lease:** this store's owner; whether the credit's owner is current; the lease state's last applied index; the slab's committed tier (its URI digest and its certified provenance's op); and whether its retirement is in force.
4. **Local re-sample,** as in step 2.
5. **The decision** (`credit_earned`). It credits once iff all of:
   - at the decision, `snapshot_epoch_is_current(epoch)`: the entry epoch is unchanged and no snapshot install fence is in flight;
   - both local samples show the effect installed (a `Tier`: the pointer's digest is the candidate; a `Retire`: no pointer and retired here) and are equal;
   - this store is the credit's owner, the owner is current, and the applied index is at least the credit's;
   - the op's own effect is in force: a `Tier`'s committed generation is the candidate **and** its certified provenance op is the credit's key; a `Retire`'s slab has no committed tier and its retirement is in force (unique to this op by D3).

**Limits of the observation:**
- It does not claim to detect every transient transition.
- An install in flight at entry is caught by the entry observation. An install starting after it is caught by the epoch, an install in flight at the decision by its fence, and a local change by the re-sample.
- Local state is never treated as part of the canonical snapshot.
- Normal deliveries earn: observer-first, and both the retained and the acknowledged outcome.

### D3. Uniqueness: what can write a slab's committed tier or terminal state
**Production writers (audit at `15512473d`):** only ring apply (`consensus/raft_storage/slab_op_apply.rs`, `apply_valid_command` and `mark_removed`).
- **Tier** inserts the record. `validate_provenance` requires a certified Tier's `provenance.op == key`, its owner the slab's and `replaces == source`.
- **Retire** removes the record and calls `mark_removed`. A certified Retire's provenance op must be its own key and its slab the command's.
- **`removed` only grows**, and a Tier or claim on a removed slab is rejected with `SlabRejection::Removed`.
- **The only other writers are compiled under test:** `seed_tiered_slab_for_test` and `remove_tiered_slab_for_test` in `raft_storage.rs`.

**Whole-state paths:**
- **Snapshot install** replaces the state with one derived from the same applied log, so `removed` and provenance are preserved and monotone.
- **Logical-backup restore and member replacement** touch no tier record directly. They run under a new incarnation, so an old credit fails "owner current".

**Hence:**
- **Tier:** a later replacement carries its own provenance op, `≠ credit.key`. A later retirement leaves no committed tier. Either way, no credit.
- **Retire:** at most one Retire of (owner, slab) is ever Applied. A credit names its command's slab, so retiring A never satisfies B, nor credits an unrelated operation.
- **C9** pins the writer set.

### D4. Replay and single spend
- **Only the live job that allocated the op can mint.** Allocation takes the op's `OpOwnership` before its rows are visible, and the worker carries it while it submits. The handoff value exists only on the shared submit path, and only the certified job paths keep it.
- **Recovery cannot mint.** `recover_abandoned_tier_op` acquires ownership (`try_acquire_ops`) and never calls `submit_op`: it uses `op_view`, `cancel_op` and `deliver_applied_tier_op`. It holds no job rows and no handoff.
- **No other submitter can mint.** Every other `submit_op` caller (reservations, LSM, GC) submits other commands. A replayed recorded Applied returned anywhere else mints nothing.
- **The worker submits once.** An error, ambiguous or rejected answer, or handoff failure returns before any handoff value exists.
- **Zero-credit cases are unchanged:** cut before submit, crash or cut after acceptance, recovery delivery, cancellation, rejection, ambiguity, superseded effect, stale incarnation.
- **Observer-first** is credited once, by its worker.

### D5. The revised D4 contract (replaces ADR 0122 D4's retained-outcome rule for L2)
> A certified tier job's aged rows are recorded once, by the live worker that submitted its op, from that op's own verified Applied handoff and the command it names, when this store is the op's owner, one coherent state that has applied the op's entry shows the owner current and that op's own effect in force (for a Tier, the committed generation's certified provenance names the op and its candidate is the pointer; for a Retire, that slab's retirement), this store's local view shows the effect installed before and after that read, and the snapshot epoch observed with no install in flight at entry is unchanged, still with no install in flight, at the decision. Otherwise nothing is recorded: an undercount, never an inflation. Acknowledgement or eviction of the op's outcome has no effect on the count.

### D6. Cost
- **Memory:** constant per live certified job. One `CertifiedAgedCredit`, **256 bytes inline, no heap**: the candidate is held as a 32-byte digest, not an owned URI. Measured by `size_of` in `a_credit_is_a_constant_inline_value`. One job runs per store at a time.
- **Nothing durable:** no durable state, no per-row or per-op ledger, no delayed acknowledgement.
- **Reads:** one lease read per count, as before, plus two local samples and two epoch reads.

## Controls
Fixture: ADR 0122's `aged_l2()` (S2 retired, S1 rewritten to survivor n), with the node's own sweeper and op driver running.
- **C1 (Tier) and C3 (Retire): an acknowledged outcome.**
  - All of this store's background acknowledgements are held while armed, from before any step, by a per-store test hold that empties the op driver's ack batch.
  - The worker is held at `AfterAcceptance`.
  - The exact target op is identified (Tier: `tiered_slab(S1).certified.op`; Retire: the ring's retained certified retirement of S2).
  - The production `committer.ack_outcomes` acknowledges every pending op through the target. The ring prunes outcomes by the acknowledged prefix, and all must be accepted.
  - The target's outcome is checked **absent** from the ring before release.
  - The released worker delivers the identified op; the tally rises by exactly 1 (S1) or 2 (S2).
  - Tier: local = committed = candidate, holding n alone; n is readable everywhere. Retire: S2 retired, locally and committed.
- **C2 (Tier) and C4 (Retire): a retained outcome.** The same hold, with no explicit ack. The target's outcome is checked **present** before release and after the count. +1 or +2, once.
- **C5, the decision.**
  - **Unit** (`the_decision_credits_only_the_ops_own_installed_effect`): Tier and Retire each credit. Each of these credits nothing:
    - the same visible effect with the epoch not current (a changed epoch or an active fence);
    - this store not the owner, or no store owner;
    - the owner not current;
    - the entry not applied, or no applied index;
    - not installed before, or not installed after;
    - for a Tier: a later op's generation, another candidate, an uncertified generation, or no committed tier;
    - for a Retire: still tiered, or not retired.
  - **Real store** (`only_its_own_effect_earns_a_count`): after S2's retirement and S1's rewrite, the production count credits nothing for:
    - a Retire credit for S1 (the same owner's other slab, still tiered);
    - a Tier credit for an op the committed provenance does not name;
    - another candidate;
    - an entry not applied here;
    - a Tier credit for retired S2.

    Each op's own credit then earns its rows exactly once.
  - **Decision point** (`a_changed_snapshot_epoch_at_the_decision_earns_nothing`, `an_active_snapshot_fence_at_the_decision_earns_nothing`): S1's count is held after its first local sample (`CertifiedTierCut::BeforeCreditDecision`, a hold only). The epoch is advanced, or an install fence raised. The visible replacement is unchanged and the worker's result is `Rewritten` as before, yet nothing is credited. The survivor is read.
  - **Entry** (`a_fence_active_at_entry_and_released_before_the_decision_earns_nothing`, Codex 01a204d8):
    - S1's count is held at entry (`CertifiedTierCut::BeforeCreditEntry`, a hold only), with the candidate visible. An install fence is raised there with no epoch advance.
    - After release, the test awaits either the decision hold, where it releases the fence before the decision, still with no epoch advance, or the worker's completion. So an early refusal cannot hang it.
    - Nothing is credited. The worker returns `Rewritten`, the epoch is unchanged, and the survivor is read.
    - Before the entry observation existed, this control credited (tally 3 vs 2: `a124r-red.log`).
- **C6, the mint** (`a_credit_mints_only_for_its_own_op_slab_and_kind`). Each of these gives no credit:
  - another op's handoff, or another digest;
  - a command certified by another op;
  - another slab than the job's (a Retire of B for a job on A);
  - a Retire command for a Tier job, or the reverse;
  - a job slab of another owner;
  - a Tier without a candidate;
  - no rows.
- **C7 and C8:** the existing controls, unchanged and passing:
  - an uncommitted rewrite cut after upload earns nothing;
  - an unsubmitted retirement earns nothing;
  - observer-first is credited once;
  - a reopen replays the log and earns nothing (stale incarnation);
  - cancellation and recovery after acceptance earn nothing (`an_effect_its_worker_never_finished_earns_nothing`).
- **C9:** `only_ring_apply_writes_a_committed_tier_or_terminal_set` (architecture) pins the writer set of D3. The credit's and the handoff's constructors are private to their modules, by plain Rust visibility.

**Baselines,** recorded before the implementation at plan `375ab3f48` + `8992317b8`: C1 and C3 failed at their count (2 vs 3, 0 vs 2); C2 and C4 passed.

## Rationale
- The retained outcome is the wrong authority for a *live* worker's credit: the same store's own acknowledger removes it on a timescale unrelated to the work.
- The worker's own verified handoff, the command it names and the committed provenance together identify the effect exactly. No pointer equality alone, no durable ledger, and no change to acknowledgement are needed.

## Consequences
- The Section 8 series no longer reads a correct, slowed certified prune as absent.
- A crash between acceptance and the count still earns nothing (an undercount, by design).
- A snapshot install or fence observed around the count earns nothing for that job (a conservative undercount).

## Open Questions
- Whether a bounded wait for `last_applied ≥ applied_index` is worth adding. Not proposed: the condition yields zero instead.
