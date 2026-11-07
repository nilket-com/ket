# ADR 0697: a slab is named by its owner and store incarnation, and every terminal transition is arbitrated by the control ring

Status: accepted plan r4, 2026-11-03 (Claude). Codex plan clearance 01a1a289 of the reviewed draft (sha256 7584bc792adebd1c4eba65d1b64e588d154fba444d318f4e637ccf395f699a6b); this file differs from it only in this header and the title. The plan lands on main with its reviewed implementation, not alone. It consolidates designs r2, r3, r3b, r4 and r4 Addendum A into one ADR, per Codex 01a19f3d ("one cohesive ADR").

r4 applies Codex 01a1a25a (review of r3), and only that:
- §5.3–5.4: the replay and history lookup comes before current-owner admission; an owner failure is a non-outcome refusal.
- §6.1: phase-specific guards (claim stage versus after Complete); P4 recovers from the Applied Complete plus its exact postimage, not from an Ack; a rejected claim restores its exact recorded pre-state; local reader and GC safety checks are kept.
- The H1 label, and the defect count in §1.
- §9.3: the guards for supersession and for a crash after Complete.

r3 applied Codex 01a1a239 (review of r2):
- §5.2–5.6: the window bound, with AckPending retained until the prefix covers it, workflow span reservation, and `Refused(OutsideWindow)` that is not recorded.
- §6.1: the T1 order is restored, with the claim authorising the unlink and Complete as the terminal acknowledgement; the P3 quarantine evidence; the restored delete-pending state.
- §3: the RegisterIncarnation admission rule. §4: `reacquire_exposed`. §7: the completed seal and reset, and the real deployment limit.
- §9.3: the R1 discriminator.

r2 folded in the Codex 2026-11-03 rulings:
- 01a1a21a-23a6: r4 architecture accepted; HOLD R1–R3.
- 01a1a21a-c0d5: A1 and A3 accepted in direction.
- 01a1a21a-ea3f: DC-161 accepted as scoped.

What changed from r1:
- §5 is rewritten: the op-sequence prefix (R1), the Ack-pending handoff (R2), and the Cancel and conflict digest contract (R3).
- §6 is rewritten: the transition → local-effect authority matrix and the recognised partial states.
- §4 adds that Burning is permanent and that exposure is a typed capability.
- §8 and §9 add the DC-161 acceptance shape and the R1–R3 tests.

The settled items (§2–§4 identity, H ordering, T1, exact intervals) are not reopened.

Fixes:
- DC-157: the GC-vs-tier intent wedge.
- DC-160: cross-owner slab identity.
- DC-160a: the allocator is not durable.
- DC-161: a deposed leader's GC is orphaned. It resolves once leadership is regained (§6), with a stated availability limit. It is a pre-release format change (operator rule: format changes OK; resets need the operator's go).

## 1. Context (demonstrated)

All four are RED on the fix tip 9d691fc8 and on main 9e7e3ad1; the evidence is sealed on bigfoot `~/work/reviews-logs/`.

- **DC-157.** The §8 release smoke wedges both arms. A TierSlab and an LSM GC commit for the same slab both apply. The GC intent stays Prepared, and the `TierAuthorityLsmUnresolved` fence never clears.
- **DC-160.** `dc160_tiering_one_data_nodes_slab_leaves_anothers_same_id_slab_untouched`: node B's unrelated slab 0 took node A's tier pointer.
- **DC-160a.** `dc160a_reserved_slab_ids_are_not_reissued_after_reopen`: reserved ids 1–4 are issued again after a clean reopen. This is the local reservation form only.
- **DC-161 (scoped; accepted 01a1a21a-ea3f).** `dc161_gc_after_leadership_loss_leaves_no_unresolvable_intent`: a deposed leader's GC gets ForwardToLeader, mapped to a generic error. The intent and the fence remain. This is a confirmed availability defect: the mapping and the orphan are proven. Not measured: the production pause/election window, its frequency, and any data loss.

Root cause, in one line: the control SM keys slab state by a bare `SlabId`, but a slab is a node-local physical object (identity r2). The local allocator does not survive reopen. Nothing arbitrates tiering against GC before one side does irreversible local work.

## 2. Identity (settled: Codex 01a19f20, 01a19f3d)

- `SlabRef { node: NodeId, incarnation: StoreIncarnation, slab: SlabId }`. A bare `SlabId` is a local index within one store only. Everything that crosses the control ring carries a `SlabRef`.
- `StoreIdentity { incarnation (random 128-bit), format_version }` is written once, by `initialize_new_store`, in the same redb transaction as H, and fsynced (file plus parent directory) before any slab file exists. `open_existing` never initialises. An absent, empty or mismatched identity is a typed format error.
- **Durable allocator high-water H** (an exclusive bound). H is committed before any id or range below it is returned or used: active rotation, LSM reservations, sidecar and retention replacements, bootstrap/quarantine advances, and recovery-created active slabs. Arithmetic is checked, and exhaustion is refused. A failed or ambiguous persist exposes no id.
- **Open check.** Compute F, the own-store evidence floor. H < F fails closed: no max(H, F) repair. Foreign-owner or foreign-incarnation records never raise F. Under `SlabRef`, `committed_id_floor` (`tier_authority.rs:2057`) counts own-owner pointers only.
- **Bootstrap collision** (`tier_authority.rs:2446`): the cross-owner case is removed by construction. The same-owner restart-staging case is re-derived under §4 and gets its own test.

## 3. Registration (direction settled; semantics from Addendum A3)

- `RegisterIncarnation { node, incarnation, supersedes: Option<incarnation> }` has its own admission rule, because a new incarnation cannot already be current:
  - `node` must equal the node of the leader that appended the entry;
  - accept when the node has no registration and `supersedes == None`, or when `supersedes == Some(registry.current(node))` and `incarnation` was never registered;
  - an exact replay of an accepted registration returns its original receipt (kept per node, outside the per-owner op window) and changes nothing;
  - anything else is `StaleSupersede` or `UnknownIncarnation`.

  A retired incarnation never becomes current again. A receipt is not current authority.
- **Every new effect** of any other slab-keyed command, including `CancelOp` and `AckOutcomes`, requires `owner == registry.current(owner.node)`. A replay of an existing receipt is answered before this check and has no effect (§5.3). `OwnerRef` has a private constructor in `SlabManager`, and the SM requires `owner.node` to equal the leader that appended the entry.
- Replacement is an explicit operator procedure: a fresh store, a new identity, `supersedes`. Whether re-seeding repopulates a replaced store is a premise still to demonstrate; until then it is a stated limit.
- `max_seen` and id ≥ H are discrepancy detection only. There is no anti-rollback or clone fencing claim. The unsupported-use contract stays explicit.

## 4. Terminal arbitration (T1 settled; T2 per Addendum A1)

- The SM keeps, per owner, `tiered`, `claimed: SlabRef → job`, and `removed` (exact coalesced intervals, with no inferred watermark). These three are pairwise disjoint, and that is validated on snapshot encode, decode and install.
- **T1.** Claim `r` for job `j`, so that a TierSlab of `r` is rejected. The local irreversible step runs only after the claim is definitely accepted, with no lock held across the Raft await. Complete moves `(r, j)` from claimed to removed. The paths that use it:
  - GC mark → remove;
  - legacy compaction release (`compaction.rs:1536`);
  - retention reclaim / table drop;
  - an unused reservation without positive Unsubmitted evidence.
- **RetireTieredSlab** moves `r` from tiered to removed in its single accepted entry.
- **Not terminal:** L1 unlink after tiering, rehydration eviction, and row-subset or retention rewrite (a replacement TierSlab with the same `SlabRef`).
- **T2 (A1, accepted in direction).**
  - The transaction that advances H for a reservation writes `ReservedId { r, Unsubmitted }`.
  - `expose_reserved_id` is the only writer of `Exposed`. It runs under `tier_publication_lock(r)` plus the maintenance gate before any intent, staging record or proposal names `r`.
  - **Typed capability.** It returns an `ExposedSlab` token (a private constructor), and every proposal builder that takes a reserved id requires that token, so skipping exposure does not compile. The arch census is supplementary evidence only.
  - **Retries and restart:** `reacquire_exposed(r)` rebuilds a matching `ExposedSlab` from a durable `Exposed` row only. It performs no transition, and it refuses `Burning`, absent and `Unsubmitted` rows. The irreversible `Unsubmitted → Exposed` transition runs once, never on every retry.
  - **Burn.** `Unsubmitted → Burning` happens under the same lock and persists through a crash. The row is deleted only once `BurnReserved` is definitely accepted, so `r` is then in the SM's removed set.
  - **Burning is permanent.** `expose_reserved_id` succeeds only from a present `Unsubmitted` row. It refuses `Burning`, an absent row (a burned or never-reserved id) and `Exposed` with a typed error. No path, including the T1 fallback, revives a burning or burned id, and the SM's removed set rejects any later claim or tier of it.
  - **No row is not proof of unused.** An id with no row, or with `Exposed`, takes T1.
  - Behavioural tests cover the real burn/exposure race and a crash at each transition.
- **T3.** Durable `PendingTerminal` rows are re-driven on reopen. Each producer checks the backlog bound **before** it inserts. Completion and ack always proceed. There is a typed `TerminalBacklog` refusal. No fixed memory bound is claimed for the removed intervals.

## 5. Typed outcomes and operation evidence (A2 with R1–R3)

### 5.1 Responses

`WendelonResponse::Rejected(SlabRejection)` is distinct from `Error(String)`. The `SlabRejection` reasons fall into three classes:
- **Definite non-application of this op:** `Tiered`, `Claimed`, `ClaimedByOther`, `Removed`, `NotTiered`, `NotClaimed`, `GenerationMismatch`, `MixedOwner`, `StaleSupersede`, `UnknownIncarnation`, `Cancelled`.
- **Anomaly:** `OpConflict`.
- **Ambiguous:** `OutcomeUnavailable`.

There are also non-outcome refusals, `Refused(OutsideWindow)` and `Refused(NotCurrentOwner)` (§5.3). Each records nothing, is not an outcome, and gives no cleanup authority.

### 5.2 Operation identity and the op-sequence allocator (R1)

- **The key.** `OpKey = (owner, op_seq)`, where `owner = (node, incarnation)`. This change was accepted in 01a1a239.
  - `op_seq` is one per-owner sequence across **all** operations of every kind. It is never reset per job.
  - The canonical command digest covers the transition kind, `job_id` and every authority-bearing payload field.
- **Allocation.**
  - The local redb keeps `next_op_seq`. It is advanced in the same transaction that writes the operation's local intent, so every allocated sequence has a durable local record from the moment it exists.
  - No sequence is reused after a crash. The arithmetic is checked, and exhaustion is refused.
- **Workflows reserve their whole span.** A workflow with several canonical ops allocates all of their sequences, with their digests, in its one intent transaction:
  - T1 claim then complete;
  - GC mark then remove;
  - tier first then a later retire, when that retire is part of the same job.

  Its draining ops therefore never need a fresh sequence. If an earlier op of the workflow is definitely rejected, recovery Cancels each later reserved sequence with its recorded digest.
- **Retries and new incarnations.** A network retry reuses the OpKey and the digest. A changed payload is a new op, with a new intent and a new sequence. A new incarnation starts its own sequence space at 1.

### 5.3 SM evidence, the window, and replay exclusion (R1)

Per owner, the SM keeps:
- `acked_through: u64`, the acknowledged contiguous prefix, initially 0;
- `outcomes: BTreeMap<op_seq, { digest, outcome: Applied{index} | Rejected{reason, index}, acked: bool }>`, for sequences in the window `(acked_through, acked_through + W]`. W is a fixed protocol constant, carried in the format version.

**Admission order.** This applies to every op-keyed command (originals and `CancelOp`). The historical lookup comes first; current-owner admission applies only to a **new** effect:

1. `op_seq ≤ acked_through` (in the named owner's table, current or retired) → `Rejected(OutcomeUnavailable)`. Nothing is applied or recorded.
2. An entry exists with the same digest → return its recorded outcome unchanged. This happens even when the owner has since been superseded: the receipt answers only "what happened to this op". It has zero effects and confers no current authority; it cannot reactivate a retired incarnation.
3. An entry exists with a different digest → `Rejected(OpConflict)`. Nothing is recorded or changed.
4. `op_seq > acked_through + W` → **`Refused(OutsideWindow)`**. Nothing is applied or recorded. A correct owner never sends one (§5.6), so the proposer treats it as an anomaly, fences, and keeps the intent.
5. **Missing key inside the window: owner admission.** `owner.node` must equal the node of the leader that appended the entry, and `owner` must equal `registry.current(owner.node)`. A failure is **`Refused(NotCurrentOwner)`**: nothing is applied or recorded, it is not an outcome, and it gives no cleanup authority. A retired owner's table is frozen at no more than W entries and never grows.
6. Otherwise the op is new. Validate the current state (§4), then record Applied or the definite Rejected.

`AckOutcomes` mutates, so it requires owner admission (rule 5's checks) **first**, before marking anything. `RegisterIncarnation` has its own rule (§3).

**Bounded cardinality.** Only rule 6 inserts, and only inside the window, so a live owner holds at most W outcomes. That holds under any mix of distinct out-of-window originals and Cancels: rules 1–5 never insert. An old receipt answers only "what happened to this op" and never confers current authority.

**Advancing the prefix.** `AckOutcomes { owner, seqs }`:
- marks those entries `acked`;
- returns Success for sequences at or below `acked_through`;
- rejects, as `NotDecided`, sequences inside the window that have no entry;
- refuses sequences outside the window as rule 4 does.

`acked_through` then advances over the maximal contiguous run of acked entries, and pruning happens only on that advance. It never crosses an absent, unacked or undecided sequence, and it is never inferred from the largest completed sequence.

**What always drains when the window is full:** Cancel of a sequence inside the window, Ack, and replay of existing outcomes. None of these inserts outside the window. There is no bypass for fresh sequences.

### 5.4 CancelOp and the digest contract (R3)

- `CancelOp { owner, target_seq, target_digest }`. `target_digest` is the canonical digest of the **original command**, taken from the local intent. It is never a hash of the CancelOp envelope.
- Evaluation follows the §5.3 admission order, using `(target_seq, target_digest)`:
  - at or below the prefix → `OutcomeUnavailable`;
  - an entry with the same digest → its recorded outcome, unchanged (original before Cancel returns Applied), even after supersession;
  - an entry with a different digest → `OpConflict` (an anomaly);
  - outside the window → `Refused(OutsideWindow)`, with nothing recorded;
  - no entry, but owner admission fails → `Refused(NotCurrentOwner)`, with nothing recorded and no cleanup authority;
  - no entry, with owner admission passed → record `Rejected{Cancelled}` against `target_digest` and return it (Cancel before the original). A later arrival of the original then returns `Cancelled`.
- The command is idempotent.

### 5.5 Proposer and recovery mapping

A receipt is used only if its owner, op_seq and digest equal the local intent's. Otherwise it is an anomaly: fenced, with no cleanup.

| Receipt | Meaning | Local authority |
|---|---|---|
| `Applied` | this op's transition happened | the §6.1 row for this op's transition kind only |
| `Rejected(definite reason)`, including `Cancelled` | this op applied nothing | undo this op's own preparation only, including its fresh candidate object if no committed pointer names it |
| `Rejected(OpConflict)` | the same key may already have applied under another digest | **anomaly: fence, no cleanup** |
| `Rejected(OutcomeUnavailable)` | the op was pruned, or this is a stale duplicate | ambiguous: fence, no cleanup |
| `Refused(OutsideWindow)`, `Refused(NotCurrentOwner)` | not admitted; no outcome was recorded by this request | anomaly: fence, no cleanup, keep the intent |
| `Error`, ForwardToLeader, transport | unknown | ambiguous: fence, no cleanup; retry the same OpKey and digest when allowed |

This replaces the `Ok(_)` mapping at `sweeper.rs:333/:359`. Every production mapper inspects the typed response before it reports success or releases token authority. Old committed objects are never deleted (OPERATOR-DECISION-NO-L2-DELETES).

### 5.6 Local handoff and admission (R2, and R1's bound)

**Choice: `AckPending` is retained until the canonical prefix covers it.**

Local per-op state in redb is exactly one of:
- `Intent { op_seq, digest, transition, pre, post, phase }`;
- `AckPending { op_seq, acked: bool }`;
- absent, meaning the sequence is at or below `known_acked_through`.

`known_acked_through` is a local redb value: the canonical prefix as last read from the SM after a barrier (§6 step 1). It only increases. A stale value is smaller, which refuses more and never admits more.

**Handoff.**
- When an op's local effects are durable, one redb transaction deletes the intent and writes `AckPending { op_seq, acked: false }`. A definite rejection does the same after its own undo.
- When an `AckOutcomes` covering the op is definitely accepted, the row becomes `acked: true`.
- Rows are deleted only when a barrier read shows `acked_through ≥ op_seq`. In the same transaction, `known_acked_through` advances to that canonical value.

**Invariant.** Every sequence in `(known_acked_through, next_op_seq)` has exactly one local row. So the local row count is the allocated span, and it can no longer understate what the SM retains.

**Admission.** A producer allocates `k` sequences (its whole workflow span, §5.2) only if:

`(next_op_seq − 1 + k) − known_acked_through ≤ W`

This check runs **before** any sequence is allocated or any pending row is written, for every producer. Otherwise the producer refuses with a typed `TerminalBacklog`, and status names the oldest unresolved sequence that is holding the prefix. Because the SM's window is measured from the canonical `acked_through ≥ known_acked_through`, every sequence the owner allocates is inside the SM's window.

**Draining.** Recovery, Cancel of an allocated sequence, Ack, and the reserved later ops of an admitted workflow all use sequences that are already allocated, so they always proceed when the window is full.

Crash and loss cases:
- **A crash between the handoff and the Ack, or a lost Ack response:** reopen enumerates the `AckPending` rows and re-proposes the Ack. The Ack is idempotent.
- **A crash after the Ack, before the prefix read:** the next barrier read deletes the covered rows.
- **A duplicate original after the prefix advanced:** `OutcomeUnavailable`, which applies nothing.

## 6. Recovery (r4 §2–§3 with the R1–R3 corrections)

On reopen and on each readiness retry, for each local `Intent` and `AckPending` row:

1. **Barrier.** Obtain a linearizable read-index, wait until the local SM's applied index reaches it, and then read the owner's outcome entry, `acked_through`, and the relevant claim/tier/removed state **in one SM read**. A locally sampled commit index, an observer callback, or the barrier alone is not an outcome oracle.
2. `AckPending` → propose the Ack (§5.6).
3. `Intent`, with the outcome taken from the SM's entry for its `op_seq` (§5.5 checks key, digest and owner first):
   - **Applied:** look up the §6.1 row for this op's **transition kind and current local phase**, and re-check that the current qualified claim and state still match before any irreversible step. Then finish, hand off, and Ack.
   - **Definite Rejected:** undo this op's preparation, hand off, Ack.
   - **No entry, above the prefix:** propose `CancelOp` with the intent's original digest, then re-read after a new barrier. An ambiguous Cancel result stays fenced and is retried later.
   - **OpConflict, OutcomeUnavailable, a receipt mismatch, or an unrecognised local phase:** a fenced anomaly with typed status. Nothing is unlinked or cleared.
4. **Leader-only availability limit.** Only the current leader proposes (`should_propose`, `LeaderGatedLsmGcDriver`). A non-leader with an unresolved row stays fenced, with typed status `AwaitingLeadership`, until it leads. Recovery is not claimed to be immediate on every follower. Immediate non-leader recovery would need a forwarding/outcome protocol that this ADR does not supply.

### 6.1 Transition → local-effect authority

Each transition kind authorises only the local effect in its own row. **The T1 order is preserved:**
1. The claim is definitely Applied, and only that authorises the irreversible local work.
2. Complete is the terminal acknowledgement, proposed only after the local postimage, including its quarantine evidence, is durable.
3. Complete's Applied authorises only the post-terminal cleanup listed for it.

Each op has its own durable per-op record: its own `Intent` while undecided, then `AckPending`. Both ops' sequences are reserved by the workflow's single intent transaction (§5.2), and each later op's intent row carries its own phase.

| Op transition | Applied authorises | Recognised local phases (resumable; exact identities) | Rejected undoes |
|---|---|---|---|
| **GC mark / T1 claim** (the claim) | (i) persist the exact delete-pending run state in the manifest, if it is not already durable; (ii) the quarantine follow-up for exactly the intent's input file identity (length, digest, revision). **Claim-stage guard:** the exact current qualified claim `claimed[r] == j` and current owner, plus the existing local read-pin, eligibility, local exclusion and GC grace checks | P0: original present and identity-exact, run durably delete-pending; P1: quarantine entry and original both present, both identity-exact; P2: quarantine entry only (identity-exact), run still present; P3: quarantine entry present and identity-exact, original absent, run dropped from the manifest. P3 then proposes Complete | the run is restored to its **exact recorded pre-state** from the intent (which may be a retired run, never resurrected into a different state), including the in-memory pin and any persisted delete-pending marker. Then Cancel the reserved Complete sequence |
| **GC remove / T1 Complete** | only purging the quarantine entry for this exact identity, then the handoff. **Post-Complete guard:** the exact Applied Complete receipt for this key and digest, plus `r ∈ removed` for the current owner and current authority. A live claim is **not** required: Complete has ended it | P3c: Complete Applied, quarantine entry present and identity-exact, original absent, run dropped (before the purge); P4: Complete Applied, the quarantine entry purged with its durable purge record, original absent, run dropped (before the handoff). Both resume from the Applied receipt and the exact durable phase; an Ack is not required | fenced anomaly: a Complete of an accepted claim must not be rejected. Nothing is touched |
| T1 for legacy compaction release and retention reclaim | as the two rows above, for that path's own file identity and manifest row | as above | as above |
| LSM install | publishing exactly the intent's outputs | **Categories only:** Prepared with outputs written; OutputsPublished. Slice-5 source review must enumerate each output's exact identity and every supported intermediate filesystem and index state, each with a test | unlink only this intent's own uncommitted outputs, identity-checked (`lsm_driver.rs:1524–1556`) |
| TierSlab first / replacement | installing exactly the intent's pointer generation in the local mirror | staged with the exact URI and hash; installed with the exact generation | delete only this op's fresh candidate object, if no committed pointer names it |
| RetireTieredSlab | removing the local pointer mirror for this exact generation, and this slab's leftover cleanup | pointer present at the exact generation; pointer gone with the retired-id mirror written | nothing |
| BurnReserved | deleting the `Burning` row | Burning | fenced anomaly (a burn should not be rejected); the row stays Burning |
| RegisterIncarnation | nothing local; it gates later ops | n/a | fenced: the store cannot propose |

Rules that apply to every row:
- Unexpected file loss cannot impersonate completion. P3 and P3c require the identity-exact quarantine evidence. P4 requires the Applied Complete receipt plus the durable purge record for this exact identity. "The original is absent" alone is never enough.
- The intent becomes `AckPending` only after the phase's local effects are durable; the Ack never precedes local completion.
- Any phase not listed is fenced, and so is an identity mismatch in any phase. Nothing is unlinked in either case.
- Guards are **phase-specific**, read from the coherent SM read. Claim-stage irreversible work requires the live qualified claim and current owner. Post-Complete cleanup requires the Applied Complete receipt and the matching removed state. The order is unchanged: claim → local work → Complete.
- Canonical arbitration never replaces the local safety checks: read pins, eligibility, local exclusion and GC grace still gate every delete.

Other notes:
- The SM state, not observer events, is the oracle, so snapshot installs that skip callbacks lose nothing.
- **DC-161.** A role preflight runs before the sequence is allocated and before any irreversible local work. A leadership loss after submission resolves through Applied or Cancelled once leadership is regained, never by clearing on ForwardToLeader or on another generic error from a later attempt.

## 7. Format and existing stores

- The WAL, snapshot and redb encodings get a version bump. Old encodings are refused at open with a typed error, and there is no migration that guesses.
- **Done (operator go, 2026-11-03).** All §8 harness smoke stores, including the DC-157 wedged stores of both arms, were sealed and then deleted, so the next smoke starts on fresh stores.
  - The archive is at bigfoot `~/work/reviews-logs/s7-smoke-stores-sealed-20261103/`: `blocks.tar.zst`, sha256 7e893c546106b22f7914b8c0814722537230ca794e06a1d7701116d5e557509e, with per-file `FILES.sha256`, `TREE.txt`, `SHA256SUMS` and a README.
  - It was verified by extracting to tmpfs: every file and the tree matched.
  - The smoke logs and metrics stay sealed in `~/work/reviews-logs/s7-measure-verify-20261102/`.
  - No other reset, intent deletion or unlink is authorised by this ADR.
- **Deployment limit.** The format bump is mandatory, and it makes **every** store written by an earlier build unopenable by this build, whether or not that store ever tiered. There is no upgrade path: such a store must be re-created, as with any pre-release format change. Before release this is acceptable (pre-release format changes are allowed; any reset needs the operator's go). This ADR does not claim that no such stores are deployed.

## 8. Implementation order (each slice: RED first, two-tier gate, Codex source review)

1. `StoreIdentity`, durable H and the open check (DC-160a GREEN).
2. `SlabRef` through commands, SM state, snapshots, observers, local mirrors and L2 key prefixes, plus `RegisterIncarnation` and the owner checks (DC-160 GREEN; B's read asserts exactly `b"node-b-row"`, and A's bytes, A's pointer and B's mirrors stay distinguishable).
3. Typed `Rejected`, the op-sequence allocator, per-owner `outcomes` with `acked_through`, `CancelOp`, `AckOutcomes`, local `AckPending`, and the fixed mappers (the §9.3 tests).
4. T1 claim/complete for GC and the local-only terminal paths, T2 `ReservedId` with the `ExposedSlab` capability, and the T3 backlog (DC-157 prevention GREEN; the existing containment tests stay unchanged).
5. Recovery (§6) with every §6.1 row and every crash point on a real `ControlCluster`, including a snapshot install between apply and reopen (DC-157 recovery GREEN; DC-161 GREEN in the §9.2 shape).
6. The §8 release smoke re-run on fresh stores (both arms), after the operator's disposition of the old stores; then the §8 campaign.

## 9. Acceptance predicates

### 9.1 Demonstrated defects

1. The DC-160a, DC-160 and DC-157 REDs are GREEN, with test text unchanged except the strengthened DC-160 read.
2. The original DC-161 RED and its sealed evidence are preserved as history (its desired state is stronger than the leader-only policy; see 9.2).
3. Old-format stores are refused at open with the typed error.
4. The §8 smoke completes on fresh stores without `TierAuthorityLsmUnresolved`.

### 9.2 DC-161 acceptance shape (per 01a1a21a-ea3f)

On a real ring:
- node 1 starts GC;
- node 2 is elected;
- assert node 1 is **fenced** with `AwaitingLeadership` and the intent is retained;
- elect node 1 again;
- assert that it resolves through exact Applied or Cancelled evidence, the intent is gone, the Ack is committed, and readiness is restored.

Variants:
- (a) the op was accepted before leadership was lost, and the response was lost: it resolves as Applied with the GC follow-up;
- (b) the op was never appended: it resolves as Cancelled;
- (c) a later attempt sees ForwardToLeader or a generic error: there is **no** clearing on that observation alone.

### 9.3 Outcome protocol (R1–R3) and A4 guards

- **R1 discriminator:**
  - keep seq 1 unresolved;
  - repeatedly complete and Ack later sequences until the span reaches W;
  - assert that the **next allocation is refused** with `TerminalBacklog`, even after those sequences' Acks are accepted;
  - send repeated distinct out-of-window originals and Cancels, and assert the SM outcome cardinality stays ≤ W and nothing is recorded;
  - close seq 1 with Cancel, Ack it, read the prefix, and assert admission resumes.
- A full window still drains: Cancel of an in-window hole, Ack, replay of existing outcomes, and a reserved workflow Complete all succeed.
- A rejected claim Cancels its reserved Complete sequence.
- Each GC phase P0–P4 resumes on reopen. Losing the original file without quarantine evidence fences and does not complete. The delete-pending state is restored on a rejected claim.
- `reacquire_exposed` after a restart rebuilds the capability from `Exposed` only, and refuses `Burning`, absent and `Unsubmitted` rows.
- **Supersession guard:** accept A under I, supersede I with J, retry A → the original receipt, with zero state and zero cleanup effects. A mismatched digest stays `OpConflict`. A genuinely new op for the old owner → `Refused(NotCurrentOwner)`, with nothing recorded or mutated. Cancel follows the same three cases.
- **Crash after Complete:** a crash after Complete applies but before the quarantine purge, and a crash after the purge but before the handoff or Ack. Both finish through the post-Complete guard (P3c, P4), with no inference from missing files.
- A rejected claim of a retired run restores it to retired, not live.
- A duplicate after Ack returns `OutcomeUnavailable` and applies nothing: live, after a snapshot install, and after a restart.
- Out-of-order completion: seq 3 is acked before seq 2. The prefix does not advance past 1 until 2 is acked; then it advances to 3 and prunes.
- An unresolved hole holds the prefix, and the backlog bound refuses new work. Recovery, Cancel and Ack still drain, and the hole closes through Cancel.
- No sequence is reused after a crash between allocation and use (allocation is in the intent's transaction).
- Crash after the handoff but before the Ack, and a lost Ack response: the Ack is re-driven, with no replay and no leak.
- Original before Cancel returns Applied. Cancel before the original returns Cancelled, and the late original then returns Cancelled. A Cancel whose target digest differs is `OpConflict`, an anomaly with no cleanup.
- Same OpKey with a different digest returns `OpConflict`: fenced, nothing cleared or deleted.
- An accepted-op retry after replacement or retirement, after a snapshot install or restart, and after incarnation supersession.
- An ambiguous response followed by a retry with the same key and digest.
- Cleanup touches only its own op.
- The burn/exposure race under the lock. A crash in `Burning`, followed by an exposure attempt, is refused permanently.
- The backlog bound is checked before insertion, for every producer.
- An old registration replayed after supersession returns its original receipt, and current stays unchanged.
- Each §6.1 recognised phase resumes; an unlisted phase or an identity mismatch fences without unlinking.

### 9.4 Structure and gates

- There is no bare-`SlabId` field in any control-ring command, SM state or snapshot type (an arch test).
- Proposal builders that take a reserved id require `ExposedSlab` (by type; the arch census is supplementary).
- The full suite, clippy and `cargo fmt --check` pass at the checkpoint.

## 10. Not claimed

- No anti-rollback or clone protection.
- No shared logical slabs (identity option II).
- No L2 deletion of retired incarnations' objects (to be registered).
- No measurement of DC-161's production reachability.
- No immediate recovery on a non-leader: an unresolved store waits for leadership (§6 step 4).
