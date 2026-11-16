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
  2. Arm the shutdown watchdog, for the ControlOnly part only.
  3. Shut ControlOnly down up to, but not including, owner close: the preparation (awaited), the index release, the lifecycle, listeners and servers, `raft.shutdown()`, and the checkpoint emitter (awaited). Each step must succeed.
  4. Re-check: `log_store.recovered_state_machine()` must show `replacement_start_authority` holding. This catches a durable cut that lags; it is not a race with the emitter.
  5. `activate_identity(expected)`.
  6. `owner.close(..)` must succeed.
  7. Disarm the watchdog, then `WendelonNode::bootstrap(cfg)` and the ordinary full daemon loop.
  - Any failure at steps 3 to 6 exits nonzero and never boots Full.
  - A stop signal during steps 2 to 6 completes the ControlOnly shutdown and exits 0. The next start follows the page.
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
   - The same owned process (pid and start time) logs the handoff.
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
