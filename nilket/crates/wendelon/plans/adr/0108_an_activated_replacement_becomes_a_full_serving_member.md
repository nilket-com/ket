# ADR 0108 - An activated replacement becomes a full serving member

## Status
Accepted for implementation, 2026-11-16 (member replacement gate 4). Design direction accepted by Codex 01a1e36a (review draft r2, with the receipt-phase rule folded in below). The prerequisite DC-248 fix landed first (3a187d711, Codex 01a1e3a5). Not delivered.

## Context
- **The role cannot change today.**
  - The vote page's identity (`RaftIdentity { instance, origin, role }`) is written once: `write_identity` refuses any other identity (raft_storage.rs:1182-1214).
  - A full boot refuses a ControlOnly page twice (node.rs:1471-1476; instance_startup.rs:131-144).
  - `run_control_only` runs until a stop signal and exits (cli.rs:849-872).
  - Nothing turns elections on for a ControlOnly instance (control_only_node.rs:114-115).
  - No startup check ties a full node's store incarnation to applied state.
- **The fence never ends.** `preparing(N)` (instances.rs:182) refuses slot N's leases, takeovers and Reserves, and nothing clears it.
- **Leases name only a slot.**
  - `admit_lease_grant` stamps whichever incarnation is current (decisions.rs:1280).
  - `validate_lease_batch_target` checks only the mTLS slot (control_server.rs:5541).
  - So a returning old process I would renew as J once the fence ended (ADR 0105 D3's deferred item).
- **Index receipts name only a slot.** `drop_gc_completed_by: BTreeSet<u32>` (schema/secondary_index.rs:314) is admitted by actor slot (control_server.rs:2227-2237), and a held receipt skips the declaration cut. Under ParentDecisions no query reads a secondary index (data_server.rs:5745), but readiness and drop collection still count slots.
- **The time-ordering identity is the slot.** `HybridTimestamp.node_id` is the slot, in memory only (hlc.rs). Commits are bounded by `commit_hlc ≤ stamp + 500` and `> lineage.floor_ms`, where `floor_ms = ring clock + 500` at each grant or takeover (decisions.rs:1329, 1525, 2179-2185, 2426).
- **Mode scope.**
  - ParentDecisions only: lease batches and LEASES_REQ (control_server.rs:4515; raft_storage.rs:11075), and the identity page (raft_storage.rs:1190).
  - Shared by both modes: `SecondaryIndexCatalogRow` and `IndexBackfillComplete`/`IndexDropGcComplete`.
  - Not encoded: the readiness answer (`ReadinessSnapshot`, observability.rs:53), so adding fields to it is not a format change.
- **DC-248** (landed 3a187d711): a buffer seals only under this process's declaration cut, and every process makes its own cut. It covers the Today table write and replicate paths, which hold the admission guard. ParentDecisions child and repair appends are this ADR's census item (Decision D5).

## Decision

### D1. Activation, its claim and its cut
- **The command.** `DecisionCommand::ActivateReplacement { proof: Signed<ActivationBody> }`, signed by slot N's key bound to (N, J).
  - Body: `{ token, r_new, birth, incarnation: J, recovered: (gid_R, K), through: A, certified: C_A }`.
- **J's procedure** uses one view V, read once, at applied index A:
  1. C_A is V's current certificate. J's active base must equal C_A, or the step is Pending: ADR 0107's activate-then-revalidate path runs first. Activation never validates a base against old chunks.
  2. The owed set is fixed by V. It is the children Accepted in V (applied ≤ A) that name N by recorded ancestry, from V's retained bodies only. It is bounded and never re-sampled.
  3. Every child in that set must be held durably at J. Otherwise J repairs, and the step ends Pending; a later step reads a new view and a new A.
- **Reducer checks:**
  - Done{r_new}; r_new not retired and a voter in every configuration;
  - the birth matches; `promoted == J`; J current; (N, J) bound;
  - `recovered == (gid_R, K)` exactly;
  - **K ≤ A < this entry's log index**, and **gid_R ≤ C_A ≤ the current certified gid**.
  - An exact retry answers its historical success, as a reducer answer only. Otherwise the command is refused: another r_new, J or birth; not recovered; or a different activation already recorded.
- **Effect.**
  - `Replacement.activated = Some(Activation { incarnation: J, through: A, certified: C_A })`.
  - `preparing(N)` becomes "Done, not retired, not activated".
  - A promises only the children applied ≤ A that name N, at base C_A.
- **Automatic.** The preparation proposes as soon as the procedure succeeds. No operator step.

### D2. Writes after A
Children accepted in (A, the activation entry] were refused by NotServing and are owed to N. A Full J repairs them through the full member's existing paths: on read (reader.rs repair_child) and in the certification build (ADR 0107). Later children replicate to J directly.

### D3. Start-up authority and the handoff
- **Current authority.** `replacement_start_authority(state, identity, page_birth, J)` requires, in applied state:
  - a replacement of N that is Done{r_new == identity.instance}, with `activated.incarnation == promoted == J`;
  - `incarnations[N].current == J`;
  - r_new not retired, a voter in every configuration, and not the old instance of an open replacement;
  - the birth equals the page birth; (N, J) is bound.

  A record somewhere in history is not enough; supersession of J or R refuses (`ReplacementNotCurrent`). The reducer's historical exact-retry answer is a separate thing.
- **Where it is checked.** In `construct`, for an identity with origin Replacement and role Full, after `establish_identity` and BEFORE `WendelonRaft::new`. It reads `recovered_state`, which comes from `log_store.recovered_state_machine()`. On refusal, no election-enabled Raft, request publication, maintenance or lease duty has started.
- **The page writer.** `activate_identity(expected)`:
  - holds `inner.write()`, the lock the vote, birth and identity writers hold;
  - re-reads the page and requires `identity == Some(expected)` with role ControlOnly;
  - writes `PageHead { birth: page.birth, identity: Some(expected with role Full) }` with the latest `guard.vote`.

  `write_identity` stays write-once.
- **The handoff** (`run_control_only`):
  1. The preparation reports `Activated` when its own applied state holds the activation. `run_control_only` selects between that signal and the stop signal.
  2. Arm the shutdown watchdog. It stays armed through the handoff and the runtime's bounded teardown; `run_daemon`'s existing policy disarms it only after `runtime.shutdown_timeout(10 s)`, before returning the exit code.
  3. Shut ControlOnly down up to, but not including, owner close: the preparation (awaited), the index release, the lifecycle, listeners and servers, `raft.shutdown()`, and the checkpoint emitter (awaited). Each step must succeed.
  4. Re-check: `log_store.recovered_state_machine()` must show `replacement_start_authority` holding. This catches a durable cut that lags; it is not a race with the emitter.
  5. `activate_identity(expected)`.
  6. `owner.close(..)` must succeed.
  7. Exit with the restart-request status `ACTIVATION_RESTART_EXIT` (75, `EX_TEMPFAIL`). It is distinct from success (0), failure (1), configuration errors (2) and an unclean termination (86). No Full start runs in this process. Its supervisor starts it again once it is dead, and the page selects the ordinary Full startup with the same R and J, including the current-authority check and unclean-recovery validation. No incarnation, token or activation statement is minted. (Superseded: a same-process `WendelonNode::bootstrap(cfg)`; see As built, run 3.)
  - Any failure at steps 3 to 6 exits 1 and never boots Full.
  - A stop signal before or during steps 2 to 6 completes the ControlOnly shutdown and exits 0, never 75; the stop is read once, after the handoff ended. The next start follows the page.
  - A stop that arrives after that read but before the exit still sees 75. A service manager does not restart a unit it is stopping, whatever the status (systemd: `systemctl stop`). No self-spawn supervisor is introduced.
  - The supervisor contract (restart on 75 under `Restart=on-failure`, rate limits, a hand-run process, `nilket local`) is in `plans/ops/configuration.md`, "Running under systemd".
- **Crash points:**

  | Crash | Next start |
  |---|---|
  | H1: after commit, before the page write | ControlOnly; the handoff runs again |
  | H2: after the page write | Full, with the authority check before `Raft::new`. The previous stop was unclean, so full restore validation runs (tiered references, storage-index recovery, `entry_is_stored` on every certified base entry, then `complete_base_recovery` and `mark_recovery_complete`) |
  | H3: J or R superseded | Full start refused |

  DC-247 stays open.

### D4. Incarnation-bound leases and index receipts (ParentDecisions only)
**Leases.**
- `LeaseBatchGrant` and `AcquireLeasesRequestHeader` gain `holder_incarnation`. The follower sends its owner's incarnation; the leader sends its own.
- `validate_lease_batch_target` and `admit_lease_grant` refuse unless the carried incarnation is current for the slot, and, on renewal or regrant, equals `lineage.incarnation` (`HolderNotCurrent`, answered LeaseDenied). Nothing is stamped from "current".
- Trust model: an honest or stale old process. OP-3 (a hostile old host holding N's key) is out of scope.

**Index receipts.**
- **The command.** A new signed decision command `IndexReceipt { tenant, index_object_id, phase: Backfilled | DropCollected, signer: Signer { node, incarnation } }`. It is verified against the key bound to exactly (node, incarnation), and that incarnation must be current.
- **Phase is part of authority and storage.**
  - **Storage.** `DecisionState.index_receipts: BTreeMap<(TenantId, IndexObjectId), IndexReceipts { backfilled: BTreeSet<(u32, StoreIncarnation)>, drop_collected: BTreeSet<(u32, StoreIncarnation)> }>`.
  - **Admission by catalog state.**
    - A `Backfilled` receipt is admitted only while the index is Building or Ready.
    - A `DropCollected` receipt is admitted only while it is Dropping.
    - A receipt for an absent or foreign index is refused.
  - **Transitions.** Each transition counts only its own phase, at every voter slot's current incarnation:
    - Building → Ready (promote-only) needs all current `Backfilled` receipts;
    - removal of a Dropping index needs all current `DropCollected` receipts.
  - **Entering Dropping clears the Backfilled set**, as the shared catalog clears its receipt set today. A late `Backfilled` receipt never counts for the drop.
  - **Removing the index removes its entry.**
- **The shared surfaces.** Under ParentDecisions, the shared mutations `IndexBackfillComplete`/`IndexDropGcComplete` are refused at admission and at apply, and `drop_gc_completed_by` is not written. Today keeps the slot-keyed row, mutations and readiness exactly as now.
- **Readers.** The lifecycle and index status read through one mode-scoped accessor.
- **The census.** List every state-changing submission a full node makes, and mark each incarnation-bound or slot-only. Any other slot-only one is reported before code. Drop reclaim and migrations are refused under ParentDecisions.

### D5. Index rebuild
- **ControlOnly.** ControlOnly repair writes no index buffer (descriptors `&[]`, as base ingest does).
- **The Full J.** J holds no receipt at first, so the lifecycle makes its cut and backfills every historical slab. Under DC-248, a buffer without that process's cut is never persisted. J's `Backfilled` receipt is signed by (N, J).
- **Census (PD child/repair admission order, Codex 01a1e399).** Show whether ParentDecisions child and repair appends hold the admission guard across descriptor observation and append, as the Today paths do. If they do not, either bring them under it, or state why a cut cannot fall between descriptor observation and append for them, before relying on the cut for J.
- **Serving is separate from index readiness.** Under ParentDecisions no read depends on an index. Index status reports gaps.

### D6. What becomes available after activation and the Full start
| Duty | Before | After |
|---|---|---|
| Raft voter | yes | yes |
| Raft leadership | no | eligible |
| Certification report | ControlOnly driver | data-server path |
| Certified cleanup | no | yes |
| Child write or replicate | NotServing | accepted |
| Reads and queries | NOT_LEADER | served at incarnation J |
| Leases | fenced | eligible, incarnation-bound |
| Coordination | none | only under lineages J holds. None at activation; ranges do not rebalance to a returning member (ADR 0729) |
| Upload of J's cold slabs | no | yes |
| Secondary indexes | none | rebuilt by the lifecycle; receipt (N, J) |
| I's objects and pointers | protected | protected; reclaim deferred |

Old-instance exclusion is unchanged, and D4 adds the refusal of leases and receipts from I.

### D7. Time-ordering uniqueness (ADR 0105 D6)
- **Existing floors are enough.** J coordinates only under lineages whose incarnation is J. Each was created by a grant or takeover after promotion, so its floor is that stamp's ring clock + 500.
- **Every earlier commit in the range is at or below the floor.** It was reserved under an earlier lineage (`lineage_open` refuses those after the change), so it has a stamp ≤ the change's stamp and `commit_hlc ≤ stamp + 500 ≤ floor`. J's commits are > floor.
- **J as a replica or reader** mints no commit time.
- **The census.** Any use of a full timestamp as an identifier across keys or ranges voids this argument and is reported before code.

### D8. Formats (pre-release, in place)
- ParentDecisions WAL/snapshot 27 → 28, marker v17 → v18 (v17 retired), ParentDecisions Raft frame 20 → 21, golden `parent_decisions_v28.txt`.
- The lease batch header and grant gain `holder_incarnation`; they are ParentDecisions-only. The ParentDecisions mode check before decode stays (control_server.rs:4524).
- The shared catalog row and mutations are byte-unchanged in both modes.
- `ReplacementReportWire.activated`: `replace status` prints `; activated through A at gid C_A`.
- The readiness JSON gains `raft_instance` and `store_incarnation`.
- Today's WAL/snapshot (21) and frame are unchanged.

## Control map (in-process; RED first)
1. **Activation admission:**
   - before RecoveryProved;
   - a foreign J, r_new or birth;
   - not promoted;
   - A < K;
   - A ≥ the entry index;
   - C_A outside [gid_R, current];
   - a different second activation: each refused;
   - an exact retry → its historical success.
2. **The cut.** A child accepted after V is not in V's set; views are never mixed; C_A ≠ the active base gives Pending, then activation and revalidation.
3. **Crash points.** H1 and H2 each resume to Full with one activation. H3: a superseded J or R refuses its Full start. A forged Full page with no activation refuses.
4. **Handoff ordering:**
   - Full boots only after every ControlOnly part is closed;
   - a close failure exits without Full;
   - a stop during the handoff exits without Full, and the next start resumes;
   - the watchdog is disarmed before Full.
5. **Leases:**
   - J is refused while preparing and granted after activation;
   - an old I is refused after activation;
   - a wrong-incarnation renewal is refused.
6. **Coverage after A.** A row acknowledged between A and the activation's apply is read at J exactly; a later row replicates to J without repair.
7. **Time ordering.** J's first commit in a range taken over from I lies above I's commits there.
8. **Index on J.**
   - Recovered rows sit in J's reopened active slab; a Full write and a forced rotation come before the first lifecycle tick.
   - The real indexed answers are checked against J's store: complete, or the full-scan fallback, never a suffix. The same holds after a restart.
   - Mutation lookups through `table_lookup_by_index` on J's store (ParentDecisions queries do not use it).
9. **Receipts and phases:**
   - an old I's receipt is refused;
   - readiness needs (N, J);
   - all current Backfilled → Dropping with no DropCollected yet: the index is NOT removed;
   - a late Backfilled never counts for the drop; only all current DropCollected removes the index;
   - a receipt for an absent index is refused;
   - the entry is removed with the index;
   - `IndexBackfillComplete` is refused under ParentDecisions;
   - ControlOnly repair writes no buffer.
10. **A Full J after the transition** (controlled floor): reports through the data server; a generation commits and J activates it; certified cleanup runs on J; leadership moves to R and the cluster commits under it.
11. **The old instance after activation:** its Raft traffic, submissions, leases, receipts and serving are refused.
12. **Today unchanged:** the v21 goldens; Today lease and index flows.
13. **Mutants:**
    - a lease stamped from "current";
    - a start-up check accepting any historical activation;
    - the start-up check after `Raft::new`;
    - a moving owed set;
    - slot-keyed ParentDecisions receipts;
    - phase-blind receipt counting.

## Owning scenario (one real three-daemon execution, about 5 min, RuntimeMaxSec 480)
`three_daemons_replace_a_lost_follower_with_a_control_only_instance`, following the product's behaviour:
1. **Unchanged through recovery:** the loss, the blank refusal, begin, provision, Done, rows 5 to 8, promotion and recovery. Activation follows automatically.
2. **Observe `activated through A at gid C_A`,** with A ≥ K.
   - The owned ControlOnly process (pid and start time read before the activation) exits, within `HANDOFF_EXIT_BOUND` (60 s), with exactly status 75. Any other status, a signal or a timeout fails, never a reason to restart it and go on.
   - Reaped, its page is Full. The test, as the declared supervisor, starts it exactly once from the same directory, executable and configuration: an expected supervised handoff, not an operator repair or a test of systemd wiring. The new process identity differs.
   - Readiness shows `raft_instance == 2147483648`, `store_incarnation ==` the promoted J, no degraded reason, and a leader.
   - `leader_hint` is never treated as proof.
3. **Direct query.** A query sent straight to node 3, with no forwarding, retry or fallback, returns exactly rows 1 to 8, served there.
4. **Row 11 and a restart.**
   - Row 11 commits, and its child names (N, J). After a stop, the stopped-store inspection shows row 11 held at J.
   - One Full restart (H2).
   - Readiness again proves J.
   - A direct query returns exactly the row set {1..8, 11}.
5. **The old-instance steps,** unchanged. A lease assertion counts only if a request was observed; zero requests is unexercised, not a pass.
6. **What leaves this workflow.** ADR 0107's real-time certification phase leaves it. It is carried by the sealed run pd-replace-20261116T063455Z, and a Full J's certification is control 10.

## Consequences
- A replaced slot serves again without operator sequencing, and with no time window between the record and serving beyond J's own steps.
- Every replacement's first Full start runs the full restore validation, because the ControlOnly stop is unclean (DC-247).
- ParentDecisions index receipts move to decision state. Under ParentDecisions, the shared catalog's receipt set is unused.
- Ranges stay with the survivors until a later grant or takeover.

## Not decided here
- Reclaim of I's objects.
- OP-3 key rotation.
- Rebalancing ranges to J.
- Address change.
- Cancel or abandon.
- DC-243 step 2.
- DST.
- §8 (FAILED, unwaived).
- DC-244 to DC-247 (DC-247 open).
- Revalidation of legacy index segments.

## Amended during implementation (census CENSUS-87, Codex 01a1e3af)
- **D5, ParentDecisions admission order (DC-250).** Child writes and replicas, and child repair (including certification's build and the replacement's owed repair), took no index admission guard. A declaration cut could fall between `plan_child`'s reading of the descriptors and the append, and between the durable append and the buffer append. The fix: the plan is an `AdmittedPlan`, made only by taking the store's admission read guard before the descriptors are read. Its guard moves with the parent lock into the spawned persistence work, so a cancelled caller releases neither before that work ends. Repair takes it after its fetch. Lock order everywhere: the admission guard, then the parent lock.
- **D4, `Abort`.** Unchanged (Codex): a nonterminal abort must match the historical coordinator, reservation and lineage exactly. When that coordinator is an old I that is no longer current, `lineage_open` is false and the abort concludes `LineageClosed`, the outcome Resolve imposes anyway. It cannot abort a J-owned claim.
- **D4, clock samples.** Old-process clock samples are filed under slot N in the leader's memory. Recorded only: an availability and clock-admission concern, source-only and unmeasured.
- **D7.** The per-range argument depends on the ParentDecisions PrimaryKey gate (`child_writes.rs` `row_versions_in_order` refuses AppendOnly, the one cross-key use of a full HLC as an identifier) and on the inactive CDC path. It is not a claim that HLCs are unique across keys or ranges.

## As built
- **Activation.**
  - `ActivationBody` and `DecisionCommand::ActivateReplacement` are recorded once by `record_activation`, as `Replacement.activated`. `preparing(slot)` ends at activation.
  - Refusals: `ActivationNotAuthorized`, `ActivationConflict`, `ActivationCutOutOfRange`, `ActivationCertificateOutOfRange`.
  - Forwarded as `ForwardedDecision::ActivateReplacement`, for the sender's own slot.
- **The procedure** (`RecoveryDriver::activation`) runs after the delta repair and any pending report. It takes one state read: applied index A, certificate C_A, the owed set through A, and any recorded activation. C_A must be the active base. Every owed child must be a Complete intent of the recorded content. The result is a signed `ActivateReplacement`, or `Activated` once applied here.
- **The signal.** The preparation stores its `activated` watch with `send_replace`. A plain `send` dropped the value when no receiver was subscribed yet; an in-process restart that activated before the caller subscribed never saw it.
- **Start authority.** `decisions::replacement_start_authority` reads current facts only. `construct` calls it for an origin-Replacement identity right after `establish_identity` and before `WendelonRaft::new`, on `recovered_state`.
- **The page.** `WendelonLogStore::activate_identity(expected)` writes under the page lock, keeping the birth and latest vote.
- **Checkpoint flush.** `CheckpointCoordinator::flush_now` writes the latest checkpoint before the handoff reads its recovered state.
- **The handoff.** `ControlOnlyNode::hand_over`, in this order: preparation stop, index release, servers drained and shut, Raft shut down, emitter stopped, checkpoint flushed, authority on `log_store.recovered_state_machine()`, `activate_identity`, owner close.
- **The CLI.** `run_control_only` returns the daemon's exit. The stop signal is registered once for the daemon's life (`spawn_stop_watch`). `activation_exit` maps the handoff's result and the stop read after it: failure 1, stopped 0, completed 75. The watchdog stays armed through the handoff and the runtime's bounded teardown, as `run_daemon` already treats every daemon exit. (As first built, `ControlOnlyEnd::HandedOver` disarmed it and started Full in the same process; superseded after run 3.)
- **Serving by ancestry.** The read path (`reader.rs` accepted children) and the repair source (`repair.rs`) count a child by the recorded-ancestry rule (ADR 0107 C2), not by exact plan membership. Without it a Full J could not read the children it recovered for I: the first in-process read was refused "a child's plan excludes this store", QUERY_EXECUTION_FAILED. The write path stays exact.
- **Leases.** `LeaseBatchGrant.holder_incarnation`; `AcquireLeasesRequestHeader.holder_incarnation`, 16 bytes little-endian, so the header grows from 40 to 56 bytes. `admit_lease_grant` and `validate_lease_batch_target` use the carried store (`HolderNotCurrent`).
- **Index receipts.**
  - `DecisionCommand::IndexReceipt`, `IndexReceiptBody`, `IndexReceiptPhase`.
  - `DecisionState.index_receipts`, holding `IndexReceipts { backfilled, drop_collected }`. Entering Dropping clears the backfill set (`index_entered_dropping`). The catalog changes go through `promote_to_ready` and `remove_dropped` in `decisions::apply`.
  - Refusals: `IndexReceiptNotCurrent`, `IndexReceiptUnknownIndex`, `IndexReceiptWrongPhase`.
  - Slots are resolved through `instances::slot_in(decisions, ..)`: inside `reduce`, `state.decisions` is out of `state`, and `slot_of` then misreads a replacement's instance.
  - The lifecycle signs and proposes receipts under ParentDecisions (`IndexReceiptSubmitter`). The shared mutations are refused under ParentDecisions at admission and at apply.
- **ControlOnly repair** writes no index buffer (`RepairContext.index_buffers = false`, `AdmittedPlan::without_index_buffers`).
- **Readiness** JSON gains `raft_instance` and `store_incarnation`. `replace status` gains `; activated through A at gid C`.
- **Formats.** ParentDecisions WAL/snapshot 28, marker v18 (v17 retired), ParentDecisions Raft frame 21, golden `parent_decisions_v28.txt`. Moved against 27: the decision state, `acquire_leases_0` and `promoted_replacement`. Added: `activate_replacement`, `index_receipt` and `activated_replacement`.
- **Test seams.**
  - `ACTIVATION_ENABLED` (in-process replacements activate only where a test enables it; the ADR 0104 to 0107 controls keep their ControlOnly member; the product always activates).
  - `PAUSE_BEFORE_ACTIVATION`.
  - `PAUSE_BEFORE_INDEX_BUFFER`.
  - `AdmittedPlan::for_test`.
  - `control_artifact_paths_for_test`.
  - The cluster harness's `leader()` maps a Raft instance to its slot.
- **Controls.**
  - Reducer:
    - `an_activation_is_recorded_once_for_the_current_recovered_store` (1);
    - `an_activation_is_signed_by_the_promoted_store_and_refused_once_superseded` (1);
    - `a_replaced_slots_lease_carries_its_store_and_waits_for_its_activation` (5);
    - `index_receipts_are_bound_to_the_current_store_and_to_their_phase` (9);
    - `a_replacement_starts_full_only_under_its_current_activation` (3/H3).
  - Storage, DC-250:
    - `a_child_planned_before_an_index_is_not_lost_to_its_cut` (window 1);
    - `a_cut_between_a_childs_append_and_its_index_entry_loses_no_row` (window 2);
    - `a_cancelled_child_write_keeps_the_cut_out_until_its_work_ends`.
  - Cluster:
    - `an_activated_replacement_hands_over_and_serves_as_a_full_member` (1, H2, 6, 10: a generation commits on the Full replacement's report; leadership moves to its instance and the cluster commits);
    - `a_row_after_the_activation_cut_is_served_and_a_stop_before_the_rewrite_resumes` (2, H1, 6);
    - `a_superseded_replacement_refuses_its_handoff_and_keeps_its_page` (H3 at the handoff);
    - `a_parent_decisions_index_is_ready_and_dropped_on_every_current_stores_receipt` (9, cluster);
    - `a_full_replacement_indexes_its_recovered_rows_under_its_own_receipt` (8).
- **Mutants:** a lease stamped from current (5 RED); a start check without its current-store and serving checks (3 RED); phase-blind receipts with no clear at Dropping (9 RED); a guard that blocks nothing (DC-250 windows 1 and 2 and cancellation RED); a guard held only by the outer caller (cancellation RED).
- **Not built as a dedicated control:**
  - 7: the per-range time ordering is composed of the ADR 0729 floor control (`a_successors_claim_is_reserved_only_above_its_generations_floor`) and promotion's `LineagesStillHeld` (no lineage at (N, I) when J is promoted).
  - 11: the old instance after activation is covered by the lease reducer control and ADR 0104's Raft exclusion.
- **The owning scenario.** It is rebased on automatic activation: status, readiness as itself, a direct query, row 11 held, the Full page, a Full restart. ADR 0107's real-time certification phase leaves it. Launcher `replacement-launch.sh` (285639e1…, `RuntimeMaxSec` 480), unchanged.
- **Limits.**
  - Index segments persisted before DC-248's fix are not revalidated.
  - DC-247 stays open: every replacement's first Full start validates fully.
  - The old instance's clock samples are recorded only.
  - Rebalancing ranges to J is not done.
- **Corrections after source review** (Codex 01a1e41e):
  - **R1.** An activation's recorded answer is bound to its recovery. `record_activation` compares the signed (recovered gid, k) with the recorded recovery before any historical answer, then checks current authority only for a first activation. Control `an_activation_retry_names_its_recorded_recovery_exactly`: a replay changing only k, or only the gid, is refused with state unchanged, and the exact retry keeps its historical answer after supersession. RED on the earlier code.
  - **R2.** Receipt completeness needs every voter mapped to a slot. An unknown mapping refuses the receipt before anything is recorded (`IndexReceiptMembershipUnknown`). Control `an_index_receipt_is_refused_while_a_voter_maps_to_no_slot`. RED on the earlier code.
  - **R3.** The real-process oracles:
    - `direct_query_exact` checks the decoded rows of the ONE direct answer (test-support `table_query_rows_at_for_test`), within `PROBE_BOUND` and the workflow's deadline;
    - `serving_as` refuses an answer observed after its bound, and requires the owned process identity to be unchanged.

    Unit control `a_direct_answer_passes_only_exact_ok_and_in_time`: OK with a missing row, empty, another value, a redirect, a refusal, no answer, and a match after the bound all fail.
  - **R4.** The handoff returns an unexpected preparation-task join failure and a checkpoint-emitter end other than its own requested cancellation; no role rewrite or Full start follows. Control `a_failed_preparation_task_ends_the_handoff_without_a_rewrite` (seam `PANIC_AFTER_ACTIVATION`). RED on the earlier code.
  - **Not exercised by any control:** a real stop signal during the CLI handoff and a close failure in the CLI path (CLI code). Their exit mapping is unit-tested (`an_activated_member_exits_for_its_restart_only_when_its_handoff_completed`). The owning run exercises only the normal CLI handoff.
- **Correction after the FAILED owning run** pd-replace-20261116T114842Z (seal 5ea4a404…, Codex 01a1e439).
  - **The failure.** The handoff read J through `owner_ref` on the ControlOnly recovery storage. That storage is never bound to a control node, and `owner_ref` answers an unbound store differently by build: node 1 under `cfg(test)`, an error in the product. The real daemon's handoff failed at once, with no rewrite and no Full start; every in-process handoff had passed.
  - **The fix.** J is read from the recovery store's own index (`persistent_index().store_incarnation()`). A missing storage or index stays fail-closed, and the durable authority check still compares this J with the recorded activation, the promotion and the current registry. The ControlOnly store is not bound to make `owner_ref` succeed.
  - **The pin.** The in-process handoff control asserts the recovery storage is unbound before `hand_over`. That pins the fixture precondition; it is not a RED, because the cfg(test) fallback lets the old code pass.
  - **DC-251** (the fallback itself) stays open.
- **Correction after the second FAILED owning run** pd-replace-20261116T120229Z (seal 1f6c6202…, Codex 01a1e448, option A). Built as impl 2ab9115c (tree 560552ac, delta r5 9dd07610…), it FAILED the third owning run and is superseded below. It is kept here as history; its code is not in this implementation.
  - **The failure.** The handoff completed, then the Full start's bind failed with `Address already in use`. The configured transport binds `advertise_address` twice, as a QUIC (UDP) endpoint shared with the outbound provider and as a TCP+TLS listener. Quinn frees the UDP socket only when its driver ends, after the last `Endpoint` handle is gone. Which bind failed, and whether a handle leaked or the start raced the driver, was not established.
  - **Dropping the transport handles.** `hand_over` now drops every transport-holding handle (the Raft handle, the provider, the preparation, the state machine, the lifecycle) after the shutdowns. It keeps the log store, the checkpoints and the owner.
  - **The bounded release wait.** Then, before the checkpoint flush, the authority check and the role rewrite, `transport_release::await_transport_release` waits within ONE declared `TRANSPORT_RELEASE_BUDGET` (10 s, inside the shutdown watchdog). It waits until the ACTUAL bound UDP and TCP addresses, captured from the listeners at construction (`bound_transport`), bind again with the bootstrap's semantics (std UDP as Quinn binds; Tokio TCP).
    - The UDP probe is held until the TCP probe succeeds, then both are released: an observation, not a reservation.
    - Only "address in use" is observed again; another bind error fails at once.
    - A success after the deadline is refused.
    - Expiry names the protocol and address, and leaves the page unchanged.
    - The Full start's bind stays one attempt, and no reuse option is set.
    - An in-process (simulated) member bound nothing and owes no release.
  - **Controls:**
    - real transport through `configured_transport`: `a_held_quic_provider_keeps_its_udp_address_until_dropped` (a closed endpoint with a provider clone held: UDP refused within the bound; dropped: released and bound again by the real transport);
    - `a_held_tcp_listener_keeps_its_address_until_dropped`;
    - `a_release_is_refused_late_and_fails_at_once_on_another_error`;
    - handoff: `an_address_still_bound_ends_the_handoff_before_the_rewrite` (seam `TEST_BOUND_TRANSPORT`; the page stays ControlOnly).
- **The third FAILED owning run** pd-replace-20261116T122959Z (seal d487a079…, candidate 2ab9115c), and the supervised restart (Codex 01a1e463, option D).
  - **The failure.** Raft shut down cleanly. At the end of the QUIC close grace the endpoint still counted one open connection, and the UDP address was still bound at the 10 s release deadline: `TransportRelease: UDP 127.0.0.1:44895 is still in use after the release budget`. The handoff failed closed, with no rewrite and no Full start.
  - **What is not established:** whether a longer drain would have finished, and which reference or connection held the socket. DC-252 stays open.
  - **The contract change.** Activation stays automatic, but the ControlOnly process completes its storage handoff and exits 75. A supervisor starts Full once that process is dead (D3 step 7), and process exit frees its sockets. Same-PID continuation is superseded.
  - **Removed.** The in-process release wait and its controls are removed from the product path, with no unused retry mechanism left behind. The real-QUIC ownership controls are archived in delta r5 (`adr0108-delta-r5.patch`, reviews), not kept as tests of a removed path.
  - **What process death does not prove.** It solves socket lifetime, not that writers stopped. That stays the cooperative shutdown, the checkpoint flush, the durable authority recheck, the serialized rewrite and the owner close, under the watchdog until exit.
  - **Controls:**
    - `an_activated_member_exits_for_its_restart_only_when_its_handoff_completed` (CLI unit: completed 75, stopped 0, failed 1 with or without a stop; 75 distinct from 0, 1, 2 and 86);
    - `a_replacement_end_counts_only_as_a_restart_request_in_time` (workflow oracle, Codex 01a1e471 R1/R2): from its first start, the replacement's end is reaped once and kept only as 75; it is never polled again; another code, 0 or a signal is refused; a 75 first observed after its bound is refused, its time read after the end is observed, never at the poll's start (Codex 01a1e47a); there is no supervised start before a kept end, and only one after;
    - the in-process handoff controls, unchanged: H1 resume, H3 superseded refusal, R4 join failure, and the Full start after `hand_over`, which is the next start's ordinary path.
