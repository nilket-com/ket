# ADR 0716: a table delete refused before it submits anything leaves no outcome behind

Status: proposed, 2026-11-11 (Claude, Codex). Registered as DC-210 by ADR 0713; trace and RED bounded (Codex 01a1c9fc, `reviews/dc210/CODEX-NEXT.txt`); a narrowed correction cleared (Codex 01a1ca05, `reviews/dc210/CODEX-DESIGN-GO.txt`). Source review pending. Implementation lands with this plan. Review record: `reviews/dc210/` (`TRACE.md`, `RED.txt`, `RED-2.txt`, `impl/`). This ADR is the as-built consolidation of those; where they differ, this text and the later ruling govern. The ruling narrows section 7 of the trace: the three-way retention cleanup proposed there is not done.

## Why
- **A Today table delete kept a transient refusal as the outcome of its identity.** After the shared helper `write_table_bifurcated` returned, `table_delete` (`network/data_server.rs`) put the answer in its in-memory map `table_delete_idempotency` and wrote it to the durable `mutation_outcomes` store, whatever the answer was. Both are read at the top of the next attempt, before that attempt's clocks and before every gate. A delete refused LEASE_EXPIRED at the lease gate, with nothing submitted and the row still readable, was therefore answered with the same refusal on every later attempt of the identity, without reaching a gate, after the lease was back.
- **Each layer did it alone.** The in-memory map has no removal and no expiry: the entry lives as long as the process. The durable record answers after a restart until its TTL ends, and then turns into WRITE_OPERATION_EXPIRED / INDETERMINATE, permanently for a UUIDv7 identity. The caller's only way out was a new identity.
- **The write and the delete differ.** A table write keeps nothing of its own after the helper returns: what a write keeps is what the helper and the last-attempt rule record (a committed or pending finalization, a clock expiry, an exhaustion). A write refused before it submitted anything on an attempt that is not its last leaves no trace, and its retry runs the gates again. ADR 0713's wait relies on that. The delete adds a second cache and a second record on top of the same helper because its answer carries three counts planned before the helper runs, and a replay must return the first execution's counts. That reason holds for an answer that may have submitted work. It does not hold for a refusal that submitted none.
- **Evidence, by level** (`reviews/dc210/TRACE.md`):
  - *Two deterministic single-node REDs through the frame handler: demonstrated.* A TABLE_DELETE_REQ with `attempts_remaining = 3` meets an expired lease, waits out the (shortened) bound and is refused LEASE_EXPIRED; a renewal arrives; a delete of another row with a fresh identity and the same client HLC and epoch is admitted without waiting; the retry of the first identity with `attempts_remaining = 2` was answered LEASE_EXPIRED with zero passes of the gate and the row still readable. One RED in the same process (the in-memory map answers), one across a real shutdown and reopen (the durable record answers alone). Each failed identically twice (`RED.txt`, `RED-2.txt`).
  - *Not run:* the public client's three-attempt sequence, any load, any multi-node sequence. Whether a production delete has met this is not established.
- The last-attempt rule cannot explain the REDs: both attempts are non-final.

## Decision
**The shared helper says, as a type carried with its answer, whether the answer is a refusal made before this attempt submitted any work. For such a refusal the Today table delete keeps nothing: no entry in its in-memory map and no durable record. For every other answer the delete does exactly what it did. The table write discards the distinction.**

1. **The type** (`network/data_server/write_replication.rs`, module `table_attempt`).
   - `TableAttemptAnswer`: the response, the trailing marker and an `AttemptRetention`, all three private to the module. It is what `write_table_bifurcated` and the lease gate now return.
   - `AttemptRetention` has two values. `RefusedBeforeSubmission`: a refusal made before this attempt submitted any work. `PreserveExistingRetention`: everything else.
   - `NothingSubmitted`: a value with no content, neither `Clone` nor `Copy`, made once where the helper begins (`at_attempt_start`) and given up (`begin_submission`) immediately before the helper spawns the local durable append and the replica legs. That point is the submission boundary. It is after the restamp, the tenant policy check and the second computation of the replica groups, none of which dispatches anything.
   - Two constructors. `refused_before_submission` takes a `&NothingSubmitted`, a status and the request, and builds the refusal response itself. `preserving` takes any response as it stands. There is no other way to make the value, no way to change its retention, and no way to move a refusal's retention onto another response. What the type guarantees is exact and limited: Rust prevents reuse of *this* consumed token after the boundary. `at_attempt_start` is callable within the data server module, so the type does not make minting a replacement token impossible; that the helper's entry is the single production construction site is verified by source, not by the compiler.
2. **Provenance originates at the producer.** A refusal is made in one place, `WendelonDataServer::refused_before_submission`, called by the helper's own refusals and by `lease_gate_refusal`, each handing over the attempt's `NothingSubmitted`. The lease gate's functions take that reference as a parameter, so the gate's answers carry their retention out through `lease_gate_exit_unadmitted`, readmission and `admit_at_table_lease_gate` unchanged. Nothing downstream reads a status, the gate's wait outcome or a metric to decide it. After the boundary the compiler rejects a refusal: the value it needs has been moved.
3. **The delete's tail** (`table_delete`). It takes the answer apart and matches on the retention. `RefusedBeforeSubmission`: nothing is inserted and nothing is recorded; the response goes out as before. `PreserveExistingRetention`: the insertion into `table_delete_idempotency` and the `record_mutation_outcome` call, as they were.
4. **The write** (`table_write_answering`) calls `discarding_retention` on the answer and continues with the same pair as before.
5. **When in doubt, preserve.** A return site is a refusal before submission only where the source shows both that nothing was submitted and that what was missing is state that changes. Every other site keeps the caller's existing retention.

## Every return site of the helper
Line numbers are of this change. `DS` = `network/data_server.rs`, `WR` = `network/data_server/write_replication.rs`. R numbers are those of the trace, section 2; two of its groupings are corrected here: a replay out of the lease gate (R9) is not one recording class with the helper's own pending replay (R5), and R10 and R11 come after the restamp.

| Site | Where | Retention | Reason |
|---|---|---|---|
| R1 durable replay at entry | DS:8868 | preserve | The answer is an outcome recorded for the identity. |
| R2 clocks expired before durable work | DS:8889 | preserve | A recorded expiry of the operation (KNOWN_NOT_COMMITTED), not a refusal of the attempt. |
| R3 not primary for all groups: NOT_LEADER | DS:8904 | **refused** | Before the boundary; routing state. |
| R4 orphan-witness barrier not recovered: NOT_LEADER | DS:8915 | **refused** | Before the boundary; recovery state of the node. |
| R5 pending finalization found: `replay_write_outcome` | DS:8933 | preserve | A pending replay: FINALIZATION_PENDING, a redirect with status NOT_LEADER, or a reconciled DUPLICATE. Work of the identity was submitted earlier. |
| R6 gate, first pass, invalid lease | WR:1278 via `lease_gate_refusal` | **refused** | The gate precedes all work of the attempt. |
| R7 gate exit, step 3: the refusal the exit was reached with (LEASE_EXPIRED at the cap, on a closed watch or on recheck; INVALID_LEASE_SIGNATURE; EPOCH_STALE; NOT_LEADER; INTERNAL_ERROR when readmission finds no replica groups) | WR:1251 via `lease_gate_refusal` | **refused** | As R6. Reached only after the exit's replays and clock check found nothing. Not a claim that every status here is temporary (the INTERNAL_ERROR arises when `table_batch_replica_groups` returns none: malformed placement or transfer, or no replica resolved); the claim is that this attempt submitted nothing and the answer is not an outcome of the operation, so re-evaluating a retry without a stored outcome is safe. |
| R8 gate exit, step 2: the caller's clocks | WR:1183 (`lease_gate_clocks_expired`) | preserve | As R2. |
| R9a gate exit, step 1: durable replay | WR:1234 | preserve | As R1. |
| R9b gate exit, step 1: pending replay | WR:1205 (`lease_gate_pending_replay`), returned at the exit | preserve | As R5; finalized or not, reconciled or not. |
| R9c readmission: durable replay | WR:1401 | preserve | As R1. |
| R9d readmission: pending replay | WR:1205, returned by readmission | preserve | As R5. |
| R10 tenant policy: TENANT_UNAVAILABLE, TENANT_SUSPENDED, QUOTA_EXCEEDED | DS:8975 | **refused** | Before the boundary; no reservation is held on this path; tenant and quota state change. |
| R11 no replica groups after the second computation: INTERNAL_ERROR | DS:8987 | preserve | Before the boundary, but not shown to be transient (an empty group list follows from an empty child list; not the same branch as R7's none), and no run reaches it. The explicit conservative exclusion: it stays sticky, so not every answer before submission is now uncached. |
| the submission boundary | DS:9011 | | `NothingSubmitted` is given up here. |
| R12 expiry in the durable-quorum wait (INDETERMINATE) | DS:9096 | preserve | Dispatched. |
| R13 local append failed or its task failed: fenced response | DS:9126, DS:9135 | preserve | Dispatched; remote legs may hold the tombstone. Its NOT_LEADER has the status of R3 and R4 and is not one of them. |
| R14 quorum not reached: INTERNAL_ERROR | DS:9163 | preserve | Dispatched. |
| R17 pending intent not persisted: committed now, or fenced | DS:9221, DS:9236 | preserve | Dispatched. |
| R15 committed, R16 finalization pending | DS:9285 | preserve | Dispatched. |

Five sites answer with a refusal (R3, R4, R6, R7, R10; R7 covers every status the exit is reached with), all through one function. The other fourteen rows, sixteen return points, preserve. An `Err` from the helper is not an answer: it leaves the delete before its tail, as before.

## Deliberately preserved
- The delete's answers outside the helper (row count, leader, epoch, its own clock and permit expiry, the degraded fence, descriptor and packing refusals, the no-op) are untouched. The no-op is still cached and recorded.
- A committed delete is cached and recorded by the tail as before, on top of the record the helper wrote. A replay returns the first counts.
- Clock expiries, post-dispatch expiry, a pending finalization, a pending or durable replay and every failure after dispatch are cached and recorded by the tail as before, including the cases the trace questions (a sticky FINALIZATION_PENDING, a replayed record written again with a new TTL).
- The response of a refused delete is byte for byte what it was, counts included.
- The table write's behaviour, the lease gate's order of decisions, its wait and its counter are unchanged. The gate's functions gained one parameter and their answer one field.
- The last-attempt rule of the delete (`finalize_table_delete_attempt`) is unchanged: a refusal on the last attempt is still rewritten to WRITE_ATTEMPTS_EXHAUSTED and recorded there.
- So the scope of the change is exact: the delete's tail no longer stores the selected refusals made before submission on attempts that are not the last. The last-attempt rule may still record an exhaustion for the identity.

## Limits
- **Existing stores keep their old records.** Nothing is repaired and nothing is deleted. A node that recorded a transient refusal under the old code answers with it until the record's TTL ends, and with WRITE_OPERATION_EXPIRED after that. This change only stops new entries of that kind.
- **A refusal still carries the planned counts.** The counts of a refused delete are the plan made before the helper ran. They are not affected rows and not a successful delete. The public client reads counts only on OK and DUPLICATE. Registered as DC-217 (in `reviews/defect-candidates-register-20261024/REGISTER.md`, from the RED log and source), not changed here. The helper's own expiry records carry the planned counts as well.
- **Pending and stale-record concerns are registered as source candidates, not solved.** DC-218 (registered there as source candidates, not validated): the delete's own map answers a retry with the FINALIZATION_PENDING it stored for the life of the process, even after the reconciler has committed the delete; the tail can write FINALIZATION_PENDING over a record the reconciler has just written; and a durable replay can carry first counts that differ from the current attempt's plan, which the tail then stores under the current plan's counts. From source, none run. A lookup of the pending store by identity before the delete plans is deferred with them.
- **The in-memory map still has no eviction.**
- **Not every sticky refusal is removed.** R11 keeps its retention. The delete's refusals before the helper were never kept.
- DC-211 (routing, the reject code of a post-wait EPOCH_STALE), DC-216 (the delete's last-attempt rule in ParentDecisions mode) and the client contract are untouched. ParentDecisions paths are not reached by this code.
- This is no readiness claim. Section 8 stays FAILED and parked; nothing here bears on it.

## Acceptance (finite)
Tests are in `network::data_server::tests::lease_wait`, on one real single-node Today server; a delete is one TABLE_DELETE_REQ frame through the frame handler.
1. **The two REDs, now green.** `a_table_delete_refused_at_the_lease_gate_succeeds_when_retried_after_renewal` and `a_table_delete_refused_at_the_lease_gate_succeeds_when_retried_after_a_restart`. After the first refusal neither `table_delete_idempotency` nor the durable store holds the identity, each read as it stands (and again after the reopen). The retry under the same identity runs the gate, answers OK with counts [1, 0, 0], and the row is a tombstone.
2. **A committed delete replays its first counts and writes no second tombstone.** In the first test a third attempt is answered from memory byte for byte, with no pass of the gate and the tombstone's commit HLC unchanged. In the second, after a second restart, the durable record alone answers DUPLICATE with [1, 0, 0] and the first commit HLC; a new identity for the same row then answers [0, 1, 0], which is what a new plan would have answered. Existing pins that pass unchanged: `table_delete_emits_tombstone_and_replays_counts` and `mutation_dedup_entry_counts_grow_for_new_operations_not_replays` (replay from memory, first counts), `warm_mutation_caches_isolate_tenants_and_ignore_same_tenant_ttl_padding` (the same, with the commit HLC and marker), `other_tenant_pending_preserves_ordinary_first_error_order` (a seeded durable record answers a delete with the stored counts and creates no warm entry).
3. **Preserved branches.**
   - `the_lease_gate_exit_marks_only_its_own_refusal_as_refused_before_submission`: the exit called directly. Its own refusal carries `RefusedBeforeSubmission` and records nothing; expired clocks, a seeded pending finalization and a recorded outcome carry `PreserveExistingRetention`. The pending replay may answer FINALIZATION_PENDING or a reconciled DUPLICATE; the test accepts both and in its fixture it reconciled. The recorded outcome is a LEASE_EXPIRED, the status of the gate's own refusal, so the distinction is shown not to follow the status.
   - `a_table_delete_ended_by_its_callers_deadline_at_the_gate_keeps_its_expiry`: a delete whose deadline ends its wait is answered WRITE_DEADLINE_EXCEEDED, both layers keep it, and the retry after renewal is answered with it and runs no gate.
   - No existing test drives a delete through the helper's own clock expiry, a post-dispatch expiry, a pending finalization or a failure after dispatch. The existing controls of those branches are of the write (`post_dispatch_expiry_resolves_and_replays_as_one_indeterminate_operation_after_restart`, `same_tenant_pending_replay_keeps_original_retryable_outcome`) and prove nothing about the delete's counts. For the delete those branches rest on the source: their arm of the tail is the old code, unchanged.
4. **The write and the lease gate.** The write-path controls and the ADR 0713 controls pass. One of them, `readmission_judges_an_elapsed_deadline_before_any_refusal_it_computed`, calls readmission directly: its call gained the new argument and reads the response through the new accessor; its assertions are unchanged.
5. Focused runs, source review, then one checkpoint at the tip. No stress campaign, no client-loop or multi-node campaign and no memory run.

## Not established
- The public client's three attempts against a refused delete, before or after this change.
- Any production occurrence; behaviour under load; any multi-node sequence.
- A refusal at R3, R4, R6 or R10 reaching the delete's tail in a run: the REDs reach R7 at the cap. The other producers share its one constructor and are classified from source.
- The delete through R2, R12, R13, R14, R16 or R17 in a run.
- Whether Toron submits a stopped delete again under the same operation id.
- A stored outcome masked by the delete's leader and epoch checks, which precede its replays (from source, as in the trace).

## Registered separately, not changed here
- **DC-211 (open).** Routing and the reject code of a post-wait EPOCH_STALE on a delete.
- **DC-216 (open).** The delete's last-attempt rule in ParentDecisions mode.
- **DC-217 (open, registered).** A refused or expired delete answers and records planned counts.
- **DC-218 (open, registered, from source, not validated).** The sticky FINALIZATION_PENDING of a delete, the tail's record over a reconciled one, replayed counts that differ from the current plan, and the deferred lookup of the pending store by identity.
