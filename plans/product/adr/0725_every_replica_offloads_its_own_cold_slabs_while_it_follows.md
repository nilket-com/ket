# ADR 0725: every replica offloads its own cold slabs while it follows

Status: accepted, 2026-11-13 (Claude, Codex). Requirement from the operator 2026-11-13, relayed by Codex 01a1d2f0: every replica must tier and reclaim its eligible cold data on day one, not only the leader. Design accepted by Codex 01a1d2fa and 01a1d2fe; source and corrections reviewed through 01a1d366 (`reviews/all-replica-tiering-20261113/`). **Delivered with a FAILED shared checkpoint (4580 of 4584), disclosed below; not a passing gate.**

## Why
- A node tiers only while it is the Raft leader. A follower uploads nothing and keeps every sealed slab on local disk for as long as it follows, so two of three nodes need disks sized for the whole dataset.
- The cause is at three layers, not one guard:
  1. **Apply.** The state machine admits a slab operation only when its owner's node is the leader that appended the entry (`admit_slab_owner`, `SlabRefusal::NotAppendingLeader`). That is how ADR 0697 knows an operation is its owner's.
  2. **Transport.** `RaftCommitter` calls `client_write` and `ensure_linearizable` directly, which work only on the leader.
  3. **Callers.** The sweeper, the operation driver and four other sites ask `should_propose` (local leader) and stand down.
- Everything else is already per owner: slab identity and object keys carry the owner's node and incarnation, operation windows and receipts are keyed by owner, and the apply observer acts only on the local store's own slabs.

## Decision
ParentDecisions mode only. Today's authority is unchanged in bytes, frames and behaviour.

1. **A slab operation is its owner's by its owner's signature.** A ParentDecisions store signs each operation with its incarnation's decision key (the existing signer binding). The three kinds are three signed bodies under three domains: Submit (the operation sequence and the exact command), Cancel (the key and the original digest) and Ack (the owner and the exact batch). A Submit signature is not a Cancel of the same operation; an Ack signature covers its whole batch. The signature binds the publication itself (slab, object URI, payload hash, `replaces`), not a node number.
2. **Apply verifies before anything else.** `WendelonRequest::OwnerOp` is verified against the key bound to the owner's exact incarnation before any lookup or effect, and before the pre-retirement hook. Apply reaches the operation only as the verified value. Then the existing rules run unchanged: history lookup (so a signed retry learns its recorded outcome and is never a new effect), current incarnation, window, validation.
3. **What a follower may originate.** For a new effect, a slab's first tier pointer with no certificate, a Cancel, and an Ack are admitted whoever appended the entry. Every other command (a replacing or certified Tier, Retire, Compaction, GcRemove, Claim, Complete, BurnReserved) still requires its owner to be the appending leader, exactly as today.
4. **A bare slab operation is refused in this mode** (`OwnerOpUnsigned`), with no lookup and no effect, whoever appended it. The leader signs its own operations too: one authority rule in the mode. An owner-signed operation on a Today store is refused (`OwnerOpNotInMode`).
5. **The route.** A follower's operation goes to the leader on the existing forwarded-proposal channel (`ForwardedDecision::OwnerOp`). The leader checks only that the owner's node is the authenticated sender and appends the signed value unchanged. The answer carries the state machine's own receipt, acknowledgement or typed refusal.
6. **Scheduling.** `TierCommitter::should_propose` keeps its meaning (local leader) at every existing call site. A separate `may_offload` is asked at exactly two places:
   - the sweeper: a follower runs reconciliation (as before), fresh tiering of its own sealed slabs, and the drop of its rehydrated copies (local only). It skips the supersession backfill, plaintext rewrite, re-wrap, retention rewrite and tombstoned-range reclaim.
   - the operation driver: a follower resolves, hands off and acknowledges its own first publications, and then lifts the reopen fence by the existing rule (no tier or retire intent of any kind is left). Every other intent keeps waiting, keeps that fence raised, and is still reported as awaiting leadership.
7. **What a receipt means.** `submit_op` returns after the ring decided the operation and this node applied through that entry. It does not say the local effect is durable: an apply observer's failure is logged, not returned. Handoff, acknowledgement and cleanup stay with `hand_off_op_if_complete`, which checks canonical authority and durable effect and otherwise leaves the intent for the driver and reconciliation.
8. **Unknown is not refused.** Only two answers prove nothing was submitted: no leader known before sending, and the leader's refusal before it called `client_write` (it does not lead, or admission refused). A transport error, a deadline, any error after `client_write`, and a late local apply are unknown outcomes: the ticket, the upload and the reservation are kept. One bound (10 s) covers an attempt from proposing to the local apply; nothing is detached.
9. **Registration.** In this mode the committer does not register an incarnation itself. It reads that the owner is its node's current incarnation with its key bound (the node's maintenance does both through the proposal path) and otherwise allocates nothing.

10. **A rejected unknown outcome does not fence its slab for good.** An unknown outcome leaves a standalone ambiguity record for the uploaded URI, which fences the slab and until now resolved only when that exact URI became the slab's pointer. When the operation instead ends Rejected (the owner's Cancel is recorded and the operation handed off), that URI can never be published, and the record never resolved: every later pass was refused. The cleanup pass now also resolves such a record when the durable rejected-candidate row for exactly that slab and URI exists, read in the same transaction. That row is written only by the rejected handoff, for an operation with a terminal Rejected decision (from its verified receipt at submission, or from the ring's record at recovery), and it outlives the acknowledgement that prunes the receipt. Only an Unresolved record qualifies, never one recorded as not submitted; without the row nothing changes; a provisional that an old-object intent owns is refused; nothing is deleted from the object store and the candidate row stays. The removal is idempotent: with the row present and the record already gone it reports the ambiguity resolved, so a pass whose earlier removal committed without its memory being updated still converges. The row is matched by slab and URI, which is enough because every upload attempt writes a fresh URI; this is not a claim that a URI identifies an object in general. After resolution the upload is no longer unresolved-cleanup debt and those gauges fall; they are not a total of retained objects, which the rejected-candidate table keeps. The rule is not specific to followers: the same record was left behind on a leader (source reading; not demonstrated on main).

## What stays leader-only
Explicitly, with their guards untouched: LSM compaction and its certified tier and LSM steps (`LeaderGatedLsmCompactionDriver`), LSM GC (`LeaderGatedLsmGcDriver`), certified cleanup of tiered slabs (`CertifiedTierRefusal::NotLeader`), the table-drop L2 phase, and the sweeper's rewrite, re-wrap, retention and reclaim passes. On a follower, local space is reclaimed by offload; compaction, GC, retention pruning and cleanup of its already-tiered objects still wait for leadership. A leader-only intent that holds an owner's window still holds it while that owner follows.

## Algebra
- An operation's identity is (owner, sequence, digest); owner is (node, store incarnation). The signer bound to a body is exactly (owner.node, owner.incarnation). A key narrower than that identity is not used anywhere: the signature covers the whole body and the signer.
- Authority for a new effect = verified signature of the current incarnation AND (the command is in the follower surface OR the owner's node appended the entry).
- Local unlink authority is unchanged: the committed pointer for this store's current incarnation, read under the canonical lease and the publication lock. With (1) and (2) a new pointer for an owner cannot be committed without that owner's signature over it, so no separate local proposal check is added. A snapshot is trusted as a snapshot, as before.

## Compatibility
Pre-release: no conversion, no reset. Older artifacts are refused at a boundary before they are replayed.
- ParentDecisions control-log and snapshot formats 17 → 18. A format-17 log may hold bare slab operations that applied; under the new rule they would replay as refusals, so a format-17 store is refused whole. Store marker `parent-decisions-v9` → `v10`.
- ParentDecisions frame envelope 10 → 11, checked before any body is decoded.
- `WendelonRequest::OwnerOp`, `ForwardedDecision::OwnerOp`, the two `ProposalResponse` variants and four `SlabRefusal` variants are appended; no existing tag moves. Every format-17 body is unchanged (pinned corpus).
- Today's formats (15, 15) and envelope (1) are unchanged.

## Consequences
- Remote storage holds one object set per replica: roughly three sets for three equivalent replicas (an estimate, not an exact multiple for every workload). Objects are not shared across owners; that is a separate, later optimization.
- Three nodes upload concurrently, each under its own rate limit, and each verifies its own upload with a whole-object read.
- A follower's operation costs one forwarded round trip and a local apply wait.
- The retention pass's refetch with any window (DC-228) and the per-record read cost (DC-223) now apply on every replica. Neither is addressed here.

## Precondition this ADR does not establish
- To offload, a node encrypts the object under the tenant's active KEK. A tenant KEK version is created on one node, wrapped under that node's root KEK, and replicated in that wrapped form; a member can unwrap it only if it has a compatible root.
- Members whose root KEK providers were initialized independently (each generates its own random root, as the development provider and the three-daemon harness's local-sealed providers do) cannot unwrap it. Such a follower fails before it uploads anything, with a sweeper warning per pass and no readiness reason. Found by this ADR's first focused run, in which all five three-node offload tests failed for that reason (`reviews/all-replica-tiering-20261113/REQ-05.md`).
- The three-node tests of this ADR therefore supply a compatible unwrapping capability themselves: one fresh development root per test cluster, enrolled into each member before it first opens (test-only). **This ADR neither provisions nor validates that capability for real daemon members, and does not turn an unprovisioned deployment into a supported installation.**
- Registered as DC-230, a day-one release blocker before any real-process all-replica run. Its effect on earlier leader-only behaviour after a leader change, and on anything else that needs the tenant KEK on a non-installing node, is known from source only, not traced or demonstrated.

## What this does not do
- It is not Byzantine agreement and adds no protection against rollback. Signer binding rests on the existing trust map.
- It does not make a follower compact, garbage-collect, prune for retention or clean certified tiered slabs.
- It does not demonstrate the real-process three-host workflow. ADR 0723's read-back failure is unresolved and separate.

## Acceptance
"Offloaded" means a committed pointer, the slab out of the sealed set, and **its file gone from the node's own directory** (the pointer is installed before the file is unlinked, so the file is the fact checked), read before any read of the tiered slab so a later rehydrated copy is not mistaken for a file never reclaimed.

In process, three ParentDecisions nodes on one shared object store, with one common development root KEK supplied by the test (see the precondition above):
- **All replicas.** Each node seals a slab holding the same rows; each, the two followers included, ends offloaded with its own object under its own owner prefix and its pointer committed under its own owner. Each node's own store returns exactly the frames it stored. Followers' acknowledgements are accepted and their intents drain. No election completed in the interval: every node's term and known leader are equal before and after.
- **Observer failure.** A follower whose apply observer fails once on its own committed pointer still ends offloaded with its intent handed off.
- **Unanswered Submit (before commit).** With the follower's Submit held at the leader, the slab stays sealed, on disk and unpublished; the follower resolves the operation and publishes; the late Submit changes nothing.
- **Leader change.** The leader is cut off from both other nodes while it holds a follower's Submit. Both remaining nodes are observed following a new leader in a later term, and only then is the held Submit released, at a node with no quorum. At every observation the slab is on disk or has a committed pointer; it ends offloaded under the new leader and read back exactly.
- **Restart.** A follower stops with its Submit pending, the leader commits it, and the reopened follower (same incarnation) completes the offload, and its tier authority, fenced at reopen by the pending Tier, reports ready again while it still follows.

Committer boundary: the real slab manager, sweeper, operation driver and `RaftCommitter` of a node that follows (a real Raft follower), with a ParentDecisions state machine applied as another node's leader would append it. The channel between them is the fixture.
- **A follower's pass.** Every sealed slab is uploaded, committed and its file removed; the driver hands off and acknowledges; nothing is cancelled; the store is ready.
- **A receipt is not completion.** The observer fails and the pass is stopped after the entry applied and before its own post-commit step: the pointer is decided, the file is still on disk, the intent remains, the ring's record is unacknowledged, and a driver pass run at that moment neither hands off nor acknowledges. After the pass completes the local effect the operation is still unacknowledged; only the next driver pass hands it off and has its acknowledgement accepted.
- **Applied, answer lost.** The Submit commits and its answer is turned into an unknown outcome at the channel. The pass reports an unknown outcome (not a proof of no submission) and deletes nothing. The same ticket resolves to its recorded Applied outcome with no Cancel and no second Submit; the live object is intact; a reopen fence over that intent lifts while the node follows.
- **Rejected after an unknown outcome.** A Submit that never reaches the log: the slab is fenced by its ambiguity record and stays fenced and local while the outcome is unknown, through a whole pass. After the driver's Cancel and rejected handoff the rejected-candidate row exists and the record is still there; the next pass resolves it, publishes the slab with a second operation under another URI, and removes the file. The first upload is still in the store and still listed as a rejected candidate.
- **Removal committed, caller lost.** The durable removal is made directly while the manager's memory still holds the record; the next pass clears the fence and the offload continues.
- **What the removal refuses** (index): no rejected-candidate row, with or without a record, and the same URI under another slab: nothing; a provisional owned by an old-object intent: refused, rows unchanged; a not-submitted record under the rejected URI: left alone; the candidate row never touched.
- **A leader-only intent.** The same recovery with an unresolved Retire intent: the first publication resolves, the Retire is neither sent nor dropped, the store reports awaiting leadership, and under that report the reopen fence is still raised.

State machine: a follower's signed first publication applies under another leader; a bare operation is refused unsigned; Today refuses an owner operation and keeps its own rule; a wrong key, a wrong signer and an unbound incarnation are refused before any effect; a Submit signature presented as a Cancel and a widened Ack batch are refused; a follower's replacing Tier, Retire and Claim are refused `NotAppendingLeader` and the pre-apply hook sees no retirement in them; a signed retry answers its record under any leader; a follower cancels its own undecided operation; a follower's window refuses operation W+1 until its own acknowledgement advances it, admits more than W in all, and is still bounded by an unacknowledged earlier operation.
Admission: an owner operation is admitted only from its owner's node. Formats: the format-18 corpus pins the three new bodies and every format-17 body unchanged; 17, envelope 10 and marker v9 are refused.

Mutant controls, each expected to fail the named tests and no others of this ADR's state-machine set:
- the follower surface removed from admission (always the appending leader): the positive follower tests fail.
- the surface widened to every command (always the owner's node): the forbidden-command test fails.
- the bare-operation refusal removed: the unsigned-refusal test fails.

## Limits
- "Committed, answer lost" is produced at the committer's channel, not by dropping a frame on the wire.
- After an unknown outcome that was never committed, the first attempt ends Cancelled and a second operation publishes a second object. The first is recorded durably as a rejected candidate and is not deleted: this ADR adds no object collection.
- The first two executions of the three-node tests failed and are kept: all five without a common root (the precondition above), then two of five with it. The unanswered-Submit failure was this permanent fence, confirmed by one logged diagnostic execution (`reviews/all-replica-tiering-20261113/`); the leader-change failure was the test holding the leader's handler across the close that waits for it, which says nothing either way about a real handler during a close.
- Reopening between the rejected handoff and the resolving pass is not exercised; both records are durable and the pass re-reads them.
- The window test runs at the state machine with W + 3 signed operations; no test uploads more than W real slabs.
- One in-process run per test is not a rate.

## Validation record
Every execution is listed, failures included. Logs are on bigfoot under `~/work/reviews-logs/`; identities and seals are copied in `reviews/all-replica-tiering-20261113/`.
- **Pin generation**: the corpus test alone, one intended failure naming exactly the three new bodies.
- **Focused 1** (tree c97de09f): FAILED 105 of 110. All five three-node offload tests failed for want of a common root KEK (the precondition above, DC-230). Architecture test 207 of 207. Mutants M1 to M3 (the follower surface removed; widened; the bare refusal removed) and M4 (the fence release removed) each failed exactly their declared tests.
- **Five three-node tests** with the test-supplied common root (tree 9d5d6ce5): FAILED 3 of 5. All replicas, observer failure and restart passed. The unanswered Submit never resolved; the leader-change test hung in the close it had itself blocked.
- **Diagnostic** of the unanswered Submit with logs: FAILED as before, and showed the cause: the Cancel and rejected handoff had happened, and the standalone ambiguity record fenced the slab on every later pass (decision 10).
- **Focused on the correction** (tree dc0e6585): the two formerly failing three-node tests passed; the post-commit retry control passed; a selection of existing cleanup tests showed no pre-existing test failing; one new control of mine failed on an invalid construction (it tried to overwrite an index row). Mutant M5 (the new manager branch removed): two discriminating kills, plus that same broken control.
- **Eight committer-boundary tests** (tree c2b0a34b): 8 of 8.
- **Shared checkpoint, once** (tree c2b0a34b): **FAILED, 4584 run, 4580 passed, 4 failed, 22 skipped** (log sha256 236d09f0…). The five three-node offload tests passed in it.
  1. `slab_op_apply` `in_parent_decisions_mode_only_a_certified_retirement_applies`: an existing test applying bare operations to a ParentDecisions state, now refused by decision 4.
  2. `mutation_visibility_architecture_test`: a pin on the last request variant, now `OwnerOp`.
  3. `certified_lsm_elides_the_obsolete_versions_on_the_leader`: the test built its own committer without the owner authority.
  4. `three_node_workflow` `three_nodes_keep_every_acknowledged_row_through_tiering_restart_and_failover`: after "leader 3 stopped, 1 took over", row 11 was never acknowledged; last error a dial of stopped node 3.
- **After the checkpoint** (tree 0a0b1317, test files only): 1 to 3 corrected and run once: the reducer test (now signed, with its three assertions and one deliberate bare refusal), the two touched certified three-node tests and a third that shares the corrected committer, and `mutation_visibility_architecture_test` 44 of 44, all passed. **No shared checkpoint was run on this tree.** The product code is the checkpoint's; three test files and one added observation test differ.
- **Failure 4 is not resolved and not waived.** By source the route is the write coordinator's redirect to the row's lineage holder (`coordinator.rs` `build_claim`), which this ADR does not touch: a row whose primary is stopped is redirected to it on every attempt and cannot commit until it returns. The same test failed the same way, on the same row and stopped leader, in ADR 0718's first gate, before this ADR. An added observation test prints this fixture's placement: row 11's ring primary is node 3 (rows 4, 6, 7, 9, 10 and 11 are node 3's; none of rows 1 to 12 is node 2's). That is placement recomputed, not a record of what any node answered in either failed execution. It is DC-224, an open availability obligation. Not measured: whether followers tiering changes how often node 3 is the leader that this workflow stops.
- One execution of each test is not a rate.

