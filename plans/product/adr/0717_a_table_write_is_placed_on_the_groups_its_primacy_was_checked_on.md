# ADR 0717: a table write is placed on the groups its primacy was checked on

Status: proposed, 2026-11-11 (Claude, Codex). Registered as DC-211 by ADR 0713; read-only trace bounded (Codex 01a1ca34, `reviews/dc211/CODEX-NEXT.txt`); the operator's go for a bounded RED and correction (2026-11-11); RED and conditional correction cleared with precision requirements (Codex 01a1cb04, `reviews/dc211/CODEX-RED-DESIGN-GO.txt`). Source cleared (Codex 01a1cb1c, `reviews/dc211/CODEX-SOURCE-CLEAR.txt`). Implementation lands with this plan. Review record: `reviews/dc211/` (`TRACE.md`, `DOWNSTREAM-CHECK.md`, `RED.txt`, `impl/`). This ADR is the as-built consolidation of those; where they differ, this text and the later ruling govern.

## Why
- **A Today table write or delete checked its primacy on one list of replica groups and placed its work on another.** The caller computes the groups and passes them to the shared helper `write_table_bifurcated` (`network/data_server.rs`). With the lease gate on, the helper refuses NOT_LEADER unless this node is first in every one of them. Later, after the orphan-witness recovery, the pending lookup, the lease gate and the tenant policy check, each of which awaits, the helper computed the groups a second time from the routing and migration state as it then stood, overwrote the checked list with the result, and built the quorum, the local append and the replica legs from it. The second list was checked only for being empty.
- **Nothing after that point checks the coordinator's authority for the range.** Read independently before the reproduction was built (`reviews/dc211/DOWNSTREAM-CHECK.md`): the replica handler `handle_table_replicate` validates mode, sizes, the node's degraded state, drain admission and the payload's counts and hashes, and never reads the frame's epoch or compares the sender with a ring, a lease or a holder; its ingress authorizes a data node, not that node's authority for the range; finalization requires local Raft leadership; the Finalize apply arm validates nothing of the kind. Not read: the slab below its append calls, and the transport handshake.
- **So a change of the routing view between the two computations was accepted.** The write was placed on, reached durable quorum on and was answered OK for groups in which this node was not first, the check the lease gate configures having been made on a list that placed nothing.
- **Evidence, by level.**
  - *One deterministic two-node reproduction through the frame handler: demonstrated* (`reviews/dc211/RED.txt`). Two real nodes on the loopback transport, the peer serving the replica legs; the coordinator with the lease gate on and a valid signed lease of its own; a test router that answers "coordinator first" for the caller's placement lookup and "peer first" from the next lookup on. On unchanged production code the write (not a last attempt) was answered OK / COMMITTED with the row durable on both stores, and the delete was answered OK with the row a tombstone on both stores; the recorded lookups were [coordinator first, peer first] in both. The two controls passed.
  - *What the reproduction is:* a controlled change of the router's view. It is not a membership or migration transition end to end, and no such transition was run.
  - *Not established:* a lost, duplicated or wrongly visible row. The work landed on the newer view's replica set. What is shown is acceptance under a placement the configured check was never applied to; the harm argued in the trace is loss of coordinator exclusivity, from source.

## Decision
**The groups a Today table write or delete is placed on are computed and put to the node's configured placement check in one synchronous step, and the work is built from that step's result and nothing else. With the lease gate enabled, a node that is not first in every group it would place the work on refuses NOT_LEADER before it submits anything.**

1. **The type** (`network/data_server/write_replication.rs`, module `table_attempt`).
   - `LocallyAdmittedPlacement`: the groups, private to the module, read through `groups()`. Nothing outside the module can construct one or replace its groups.
   - `TablePlacement`: `Admitted(LocallyAdmittedPlacement)`, `NoGroups`, `NotFirst`.
   - `WendelonDataServer::place_table_attempt`, the one constructor. It takes the attempt's `&NothingSubmitted`, the children and the migration ledger; computes the groups with `table_batch_replica_groups`; answers `NoGroups` for an empty list whatever the check would say (`all` over no groups is true, and an empty placement is never admitted); answers `NotFirst` when the lease gate is on and this node is not first in every group; otherwise `Admitted`. It is synchronous. `None` is a failed computation, as before.
   - **The name says what it proves.** The groups passed this node's configured local check when they were computed. With the lease gate off that check is not configured and every non-empty placement is admitted, as before: the value is not a proof of primacy. Nor does it say the routing view is still the same once the work is under way.
2. **The helper** calls it where the second computation was, with the same inputs, after the same awaits. `NotFirst` is answered through `refused_before_submission` while `NothingSubmitted` is held; `NoGroups` and a failed computation are answered exactly as before. The quorum trackers, the local groups and the replica legs are built from `placement.groups()`. Between the call and `begin_submission` nothing is awaited and nothing is computed again.
3. **The early check stays.** The caller's groups still feed the refusal that precedes the orphan-witness recovery. The parameter is no longer assigned to; it is named `groups_at_entry` and is read by that check alone.
4. **One predicate.** `first_in_every_group` is the check at the helper's entry, in readmission after a lease-gate wait, and in `place_table_attempt`. Readmission is otherwise unchanged: its own groups are still computed, checked and dropped.
5. **Unchanged:** the KV write, ParentDecisions mode, every wire format, the replica handler, the lease gate, the wait, the records and the state machine.

## Return sites
The helper gains one return site: NOT_LEADER from `place_table_attempt`, a refusal before submission (routing state; nothing dispatched; the same class as ADR 0716's R3). ADR 0716's R11 (no groups after the second computation, INTERNAL_ERROR, preserved retention) is now the `NoGroups` arm, unchanged in answer and retention. For a table delete the new refusal therefore leaves no entry in the delete's cache and no durable record, by ADR 0716's rule.

## Limits
- **This is the local check applied to the right list. It is not a fence.** A change of the routing view after `place_table_attempt` returns is not seen: not in the synchronous lines before the legs are spawned, and not while the work is in flight. Nothing downstream would refuse it.
- **The receiver still validates no authority.** `handle_table_replicate` accepts a well-formed batch from any data node and never reads the frame's epoch. Registered as DC-220, a source-supported candidate, not a demonstrated data-loss defect. No receiver-side check, epoch comparison or topology design is part of this ADR.
- **The KV write has the same shape and is not changed.** `write_bifurcated` checks primacy on one router read and places on a second. Registered as DC-219, untraced.
- **Per-child router reads** within one computation may see different ring states; unchanged.
- **The check is primacy in the routing view, as before**: the first entry of each group, with a transfer's owner first where a transfer covers the partition. It is not a lease check on the placed groups and not a comparison with the ring's epoch.
- **Data-level damage from the old behaviour is not established**, so this ADR does not claim to have removed any.
- This is no readiness claim. Section 8 stays FAILED and parked; nothing here bears on it.

## Acceptance (finite)
Tests are in `network::data_server::tests::checked_placement`: two real nodes on the loopback transport (`build_two_node_fixture`, the existing two-node fixture with its router and the leader's lease gate as parameters), the coordinator's data server over a router whose view changes at a chosen lookup and then stays, and which records the view every lookup was given. A request is one frame through the frame handler. No sleep and no new production hook.
1. **The RED, now green.** `a_write_whose_placement_changed_after_its_primacy_check_is_refused_unsubmitted`: gate on, a valid lease, not a last attempt. The lookups are exactly [coordinator first, peer first]. The answer is NOT_LEADER; no record of the row exists on either store; the coordinator holds no durable outcome and neither node holds pending work for the identity. On unchanged production code it failed with OK / COMMITTED and the row on both stores.
2. **The delete.** `a_delete_whose_placement_changed_after_its_primacy_check_is_refused_unsubmitted`: a row written under the stable view exists on both stores; the delete's lookups are [coordinator first, peer first]; NOT_LEADER; the row is live on both stores; nothing in the delete's cache, no durable outcome, no pending work. On unchanged production code it failed with OK and a tombstone on both stores.
3. **The control.** `a_write_under_one_stable_view_is_accepted_on_both_stores`: gate on, the view never changes: OK / COMMITTED, the row on both stores, and exactly two lookups, both "coordinator first". This fixes the lookup count the other tests rely on.
4. **The gate off.** `with_the_lease_gate_off_a_changed_placement_does_not_refuse`: the same change of view is accepted, as before.
5. The ADR 0713 and ADR 0716 controls and the existing two-node tests pass unchanged. Focused runs, source review, then one checkpoint at the tip. No stress campaign, no multi-node campaign and no memory run.
6. **A failed focused run is part of the record.** The data-server test set run in one process in parallel (339 tests) failed one: `secondary_index_coverage_converges_while_the_lifecycle_driver_runs`, a table write that returned `LsmInstallUnresolved`. It was not rerun and is not attributed; registered as DC-221. A passing checkpoint is another sample, not its exoneration.

## Not established
- A real membership or migration transition producing the change of view, before or after this change.
- Any production occurrence; behaviour under load.
- The transfer variant (a partitioned child whose transfer is seeded between the computations): the test router gives one view to both placement methods; whether shipped writes produce partitioned children is not established.
- Anything about a change of view after the placement step.
- The KV write.

## Registered separately, not changed here
- **DC-219 (open, registered, untraced).** The KV write's second, unchecked router read.
- **DC-220 (open, registered, from source, not validated).** The table replica handler validates no authority of the sender for the range and never reads the frame's epoch.
- **DC-221 (open, registered, not attributed).** The failed focused run above.
- **DC-213, DC-215, DC-216, DC-217, DC-218, DC-201** are untouched.
