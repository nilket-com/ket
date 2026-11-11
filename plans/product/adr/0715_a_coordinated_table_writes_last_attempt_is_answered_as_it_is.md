# ADR 0715: a coordinated table write's last attempt is answered as it is

Status: proposed, 2026-11-11 (Claude, Codex). Registered as DC-212 by ADR 0714; next step and bounds set (Codex 01a1c9bd, `reviews/dc212/CODEX-NEXT.txt`); option B cleared with scope and limits (Codex 01a1c9ca, `reviews/dc212/CODEX-DESIGN-GO.txt`). Source review pending. Implementation lands with this plan. Review record: `reviews/dc212/` (`TRACE.md`, `RED.txt`, `RED-2.txt`, `impl/`). This ADR is the as-built consolidation of those; where they differ, this text and the later ruling govern.

## Why
- **In ParentDecisions mode the server's last-attempt rule invented an operation outcome.** `finalize_table_write_attempt` (`network/data_server/mutation_protocol.rs`) acted whenever a request carried `attempts_remaining == 1` and the answer's status was retryable. It never read the answer's own knowledge: anything but FINALIZATION_PENDING became WRITE_ATTEMPTS_EXHAUSTED + KNOWN_NOT_COMMITTED + ATTEMPT_EXHAUSTION, and that was written to the node's local `mutation_outcomes` store before the answer went out.
- **A redirect from a Reserved parent is not a refusal.** The coordinator answers NOT_LEADER + INDETERMINATE for a Reserved parent whose recorded coordinator is another node. That coordinator can still finalize it Accepted. The rule turned the redirect into "known not committed" and stored it. A caller told "known not committed" may submit a new identity while the original still commits.
- **The client could not correct it.** The public client's loop returns any answer it does not retry before it consults its own record of unresolved attempts; WRITE_ATTEMPTS_EXHAUSTED is such an answer. So the server's rewrite bypassed the client's accumulator, for this attempt and for every earlier one of the call.
- **Evidence, by level** (`reviews/dc212/TRACE.md`):
  - *Wire level: demonstrated.* A request with `attempts_remaining = 1`, sent to a node that is not the recorded coordinator of a Reserved parent, was answered KNOWN_NOT_COMMITTED and the row was stored at that node; the original was then released and the same parent was Accepted and read on every node (`RED.txt`, `RED-2.txt`).
  - *Public client: demonstrated, with two fixture preconditions.* The identity is already in flight from another call (a resubmission while the first call is outstanding), and the node the second call enters refuses its first two attempts at admission. The attempts, their targets and `attempts_remaining` were the client's own. The two BACKPRESSURED answers were not captured: they are inferred from three streams reaching that node while its admission was full for the first two, and from the client loop. A single call on its own cannot reach the Reserved redirect on its last attempt when the reservation is its own.
  - *A single call (S2): from source, plus a scripted client control; not reproduced end to end.* An earlier attempt reserves and answers WRITE_STALLED; the last attempt is refused at admission (BACKPRESSURED) and was rewritten to KNOWN_NOT_COMMITTED by a node that had not looked the identity up; the stalled attempt's Finalize can then commit. No cluster run shows this sequence.
- **The stored row was not an authority in this mode.** No production path of a ParentDecisions node reads `mutation_outcomes`; the trace's control showed a stored "known not committed" for an identity that then committed.
- ParentDecisions is opt-in and not shipped. Nothing here is a statement about the shipped mode.

## Decision
**In ParentDecisions mode the server's last-attempt rule does nothing for a table write. The coordinator's or the admission's answer goes out unchanged, on the last attempt as on any other, and no mutation outcome is stored for it: no row at all, not an indeterminate one either.**

1. **One guard, in the rule itself.** `finalize_table_write_attempt` returns the answer it was given when the node's mode is not Today, before it reads `attempts_remaining` or the status. The predicate is the one `table_write_answering` already uses to choose the coordinator. All three callers reach the rule through that one function, so all three obey it:
   - the admission refusal (BACKPRESSURED) in `serve_table_write`;
   - the early (deadline) answer in `serve_table_write_stream`;
   - the write's own result in `handle_table_write`.
2. **Why a guard and not a type.** The admission refusal is built before any mode branch, by code both modes share, and the early answer and the write's result arrive as the same response type both modes use. A type that kept a ParentDecisions answer out of the Today rule would have to be constructed at those shared points, by a check of the same mode field: a wrapper any caller can build, which prevents nothing. Carrying a real distinction would mean threading it through `table_write_answering` and the response plumbing of both modes. The ruling accepts one explicit guard in that case.
3. **Recording and replay.** The parent record in the decision log is the only durable outcome of a ParentDecisions table write. Only a recorded Accepted answers COMMITTED and only a recorded Refused answers KNOWN_NOT_COMMITTED (unchanged). A retry of the identity is answered by lookup, which is already the replay. Attempt exhaustion is a client-side summary of the answers a call received; it is never a server-side fact and is never stored.
4. **The Absent redirect is conservatively INDETERMINATE.** The coordinator marks every redirect INDETERMINATE, whether the lookup found a Reserved record or none. A last attempt redirected with no record is therefore answered NOT_LEADER + INDETERMINATE, where it used to be rewritten to KNOWN_NOT_COMMITTED. That is a loss of precision, not of truth. The coordinator is not changed to tell the two redirects apart.
5. **ADR 0714's ownership is unchanged.** The early answer still passes through the rule inside the response half of the non-cancelling join in `serve_table_write_stream`; the remainder is still polled to its end by the same task.
6. **Unchanged:** the client loop, Today mode (rewrite and record as before), the delete's rule (`finalize_table_delete_attempt`), the KV write, every wire layout, the records and the state machine.

## Behaviour that differs on the wire
- In this mode `attempts_remaining = 1` no longer asks the server to invent an operation outcome. NOT_LEADER and BACKPRESSURED remain attempt responses; the caller owns exhaustion. A sender that is not the client and that sets the field must apply the exhaustion rule itself. The only in-tree sender is the client, which already does: it accumulates unresolved attempts and reports WRITE_ATTEMPTS_EXHAUSTED with INDETERMINATE if any attempt was unresolved, else KNOWN_NOT_COMMITTED.
- Through the client: a call that ends on a redirect now reports WRITE_ATTEMPTS_EXHAUSTED + INDETERMINATE (the redirect carries INDETERMINATE). A call that ends on BACKPRESSURED reports INDETERMINATE if an earlier attempt was unresolved, else KNOWN_NOT_COMMITTED.
- A caller that treated "exhausted, known not committed" as licence to send a new identity will now see INDETERMINATE after a redirect on its last attempt, and must retry the same identity instead.

## Limits
- **An all-BACKPRESSURED history is definite only about this call's attempts, for a fresh identity** (no unresolved prior submission and no concurrent one). **It is not proof that the identity cannot commit.**
- **DC-215 (registered, not fixed): the residual cross-call client knowledge limitation.** A second call's accumulator has never seen the first call's uncertainty. A second call of an identity that a first call still holds Reserved, answered BACKPRESSURED on all of its attempts, reports KNOWN_NOT_COMMITTED while the first call can still commit. From source; the public-client fixture with the admission kept full for the third attempt is that sequence. Not run as such.
- **Option B does not make every operation-level KNOWN_NOT_COMMITTED truthful across resubmissions.** It removes the server's invention; it does not give a client knowledge of other calls.
- **Existing local outcome rows are not cleared.** A node that answered under the old rule keeps its rows until they expire. Their production readers stay unreachable in this mode (the replay paths are after the mode return; MUTATION_OUTCOME_REQ and the pin request are outside the served set). If outcome resolution is ever opened in this mode it must answer from the parent record, not from that store.
- ParentDecisions stays opt-in. This is no readiness claim.

## DC-214, for this path only
ADR 0714 left open that `handle_table_write` awaits the last-attempt rule after the coordinator returns, and that the rule's `record_mutation_outcome` (a store lock and a database write) is outside the deadline's timer. On the ParentDecisions table-write path the rule now awaits nothing: it returns before any persistence, at all three call sites. That removes this specific mechanism on this path, from source. It is not a universal bound on the wire answer by the deadline, and Today mode and the delete are untouched. DC-214 is not closed by this.

## No mutation outcome is written: the call sites checked
From source, for a table write served by a ParentDecisions node. The only writer of the store is `record_mutation_outcome` (plus the pin request's handler).
- `finalize_table_write_attempt`: guarded, at its three call sites in `write_replication.rs`.
- `table_write_answering`: its two record sites, and its calls of `acquire_table_mutation_permit`, `write_table_bifurcated` (two record sites, two calls of `complete_pending_finalization`) and the lease gate (`lease_gate_clocks_expired`), are all after the mode return to `coordinate_table_write`.
- `coordinator.rs`, `child_writes.rs`, `write_deadline.rs`: no reference to the store or to any function above.
- The pin request and MUTATION_OUTCOME_REQ: refused by the mode's served set.
- Not part of this statement: the delete (`table_delete`'s record sites are after its own mode return, but its last-attempt rule is not guarded, see DC-216), the KV write (`write_bifurcated`), and the background pending-finalization reconciler, whose records come only from Today paths and peer import.

## Acceptance (finite)
Tests are in `node::parent_decisions_cluster_tests::attempt_exhaustion`. The cluster controls use three real nodes, each write one request frame on a stream into the node's published service. "No row" means every node's own store, read as it stands, holds nothing for the identity.
1. **Wire level (was RED).** `a_last_attempt_redirected_from_a_reserved_parent_stays_indeterminate_and_records_no_refusal`: the last attempt is answered NOT_LEADER + INDETERMINATE, stop NONE, hint the coordinator; no row; released, the original is Accepted, the row reads everywhere; still no row.
2. **Public client (was RED).** `the_public_client_is_not_told_known_not_committed_for_a_parent_another_call_holds_reserved`: the second call returns WRITE_ATTEMPTS_EXHAUSTED + INDETERMINATE, hint the coordinator, after three streams; no row; the first call then commits. The first two answers are inferred, as above.
3. **Absent redirect.** `a_last_attempt_redirected_with_no_record_stays_indeterminate_and_reserves_nothing`: NOT_LEADER + INDETERMINATE; no parent record; no row; a later call of the identity at its coordinator commits.
4. **The authoritative definite negative.** `a_recorded_refused_parent_answers_known_not_committed_whatever_attempts_remain`: a parent refused by its coordinator's signed abort answers OK + KNOWN_NOT_COMMITTED at another node and at its coordinator with one attempt remaining, and with three; no row.
5. **S2, two sides, not a reproduction.**
   - Server: `a_last_attempt_refused_at_admission_is_answered_backpressured_and_unrecorded`: BACKPRESSURED, no knowledge claimed, stop NONE; no parent record; no row.
   - Client, scripted answers and no server code: `the_client_reports_indeterminate_when_a_stalled_attempt_is_followed_by_backpressure_to_exhaustion` (WRITE_STALLED, BACKPRESSURED, BACKPRESSURED gives WRITE_ATTEMPTS_EXHAUSTED + INDETERMINATE) and `a_fresh_identity_answered_backpressured_on_every_attempt_is_reported_known_not_committed` (scoped to a fresh identity).
6. **Today mode unchanged.** The existing pins pass unchanged: `final_attempt_exhaustion_is_a_durable_two_axis_outcome` (rewrite, record, reopen) and the lease gate's `each_reason_recovers_inside_the_bound_and_is_refused_at_it` (the rule over LEASE_EXPIRED through the wire entry, recorded).
7. ADR 0714's tests and the ADR 0709 and ADR 0710 controls pass unchanged.
8. **`three_node_workflow` keeps a write's identity across exhausted calls.** Its writer used to send a new identity on any "known not committed" and fail on any other exhausted answer. Now WRITE_ATTEMPTS_EXHAUSTED with either knowledge value (INDETERMINATE or KNOWN_NOT_COMMITTED) goes again with the same write id, within the same poll bound, and the record of that id decides. A new identity is sent only for OK + KNOWN_NOT_COMMITTED, which in this mode comes from a recorded Refused parent. Reason: a call summarizes only its own attempts, so a later all-BACKPRESSURED call of an identity that an earlier call left unresolved would otherwise change identity while the first can still commit (DC-215). No other status is retried, the bound is unchanged and a retry is never an acknowledgement. Its classification test `only_a_committed_answer_acknowledges_a_row` pins the four cases. Test code only; this fixture policy does not fix DC-215 in the public client.
9. Focused runs, source review, then one checkpoint at the tip. No stress campaign and no memory run.

## Not established
- S2 end to end: no run has an attempt reserve and stall, a last attempt refused at admission, and the stalled Finalize then commit.
- The two BACKPRESSURED answers of the public-client sequence, as captured responses.
- Lineage-movement sequences that could send a client to a third node.
- Whether anything outside the crate reads or copies `mutation_outcomes.redb`.
- A bound on the wire answer of an ordinary result by the deadline, beyond the removal described under DC-214.
- Behaviour of these controls under parallel load. DC-213 records existing parallel-only failures of this test module and stays open.
- How often a workload ends a call on a redirect. `three_node_workflow`'s writer now asks again with the same write id in that case; whether any of its runs takes that arm was not observed.

## Registered separately, not changed here
- **DC-213 (open).** Parallel-only failures of the cluster test module, with their existing rates.
- **DC-214 (open).** The deadline gap after the coordinator, less the mechanism removed above.
- **DC-215 (open).** The cross-call client knowledge limitation, as stated under the limits.
- **DC-216 (open, from source, not traced, not run).** The delete's rule, `finalize_table_delete_attempt`, has the same shape and is not guarded. One entry was read: a TABLE_DELETE_REQ is in this mode's served set, and its admission refusal (BACKPRESSURED) passes through that rule. Which statuses the coordinated delete itself brings to it was not traced.
