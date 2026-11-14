# ADR 0732: a recorded refusal is never answered or accepted as success

Status: accepted as a design, 2026-11-14 (Claude, Codex 01a1d895). Defect DC-238 (release-blocking). Records: `reviews/dc238-20261114/`.

## Why
- A mutation answer carries a status and, beside it, what is known about the commit (`MutationCommitKnowledge`: UNSPECIFIED 0, COMMITTED 1, KNOWN_NOT_COMMITTED 2, INDETERMINATE 3). In the ParentDecisions mode a parent that the decision log recorded as Refused is answered with status `OK` and knowledge `KNOWN_NOT_COMMITTED`: table and key-value writes in `coordinator.rs` `settled_response`, table deletes in `deletes.rs`. The pair contradicts itself.
- The public client builds a success receipt from status `OK` or `DUPLICATE` alone, in four places (key-value write, counter add, table write, table delete), and none reads the knowledge. So a caller is handed a receipt for an operation the cluster durably recorded as not done.
- Observed, real three-daemon run pd-offload-20261114T051324Z: a row's write was acknowledged after twelve offers of one identity during a control leadership change, and a query answered as complete did not contain it. The leading explanation is this defect (the write recorded refused, the refusal delivered as a receipt); the retained evidence for that is a byte search of the logs, not a decode, and the successful answer's bytes were not logged. The source defect stands without it.
- RED on unchanged code (`red1.log`): through a scripted transport, the public `table_write` and `table_delete` each return `Ok(receipt)` for an answer of `OK` + `KNOWN_NOT_COMMITTED`. The existing in-process test `a_recorded_refused_parent_answers_known_not_committed_whatever_attempts_remain` obtains exactly that pair from a real node for a recorded Refused parent, and asserts it.

## Decision
### Producers
1. A new status `WRITE_REFUSED = 26` ("write_refused"), appended; no existing number changes. It means: this operation's recorded outcome is a refusal; it did not and will not commit under this identity.
2. A recorded Refused parent is answered `WRITE_REFUSED` with knowledge `KNOWN_NOT_COMMITTED`, stop reason NONE and the echoed identity: `Settled::KnownNotCommitted` (table and key-value writes) and `DeleteSettled::KnownNotCommitted` (deletes). Nothing else about those answers changes.
3. One wire rule in the node: an answer whose status is `OK` or `DUPLICATE` goes out only with knowledge `COMMITTED`. The four frame builders of public mutation answers (key-value write, table write, table delete, counter add) pass the answer through one function; a success without a commit is a defect in the node and is sent as `INTERNAL_ERROR` with the outcome unknown, and logged, never as the success it was not shown to be. The counter add, which today leaves the knowledge unset, stamps `COMMITTED` at the point its fenced fetch-add has returned an applied result (original or replay). The implementation first writes down, from source, why that return is the add's commit point. There is no counter exception to Decision 5: if that cannot be established the work stops and the gap is reported; the guard is not bypassed, `COMMITTED` is not inferred from `OK`, and no fixture is stamped to make a test pass.
4. Unchanged: the reducer, what is refused and why, lineage and takeover policy, outcome retention, idempotency (a refused identity stays refused; it is not reopened, not replayed under a fresh identity, not turned into success by retry), every durable format.

### Consumers
5. One function in the client decides whether an answer is a committed success; every receipt is built only from its result. It returns committed (and whether a duplicate) only for status `OK` or `DUPLICATE` WITH knowledge `COMMITTED`.
6. Every other pairing is an error that carries the operation identity and exactly the knowledge the node sent, never a stronger one:
   - `WRITE_REFUSED` with KNOWN_NOT_COMMITTED, the one valid refusal → `MutationStopped { operation_id, status: WRITE_REFUSED, reason, knowledge }`, for table write, table delete, key-value write and counter add alike.
   - `WRITE_REFUSED` with COMMITTED, INDETERMINATE, UNSPECIFIED or an unknown knowledge value is not a valid refusal because of its status: it is `MutationAnswerInconsistent` (below).
   - `OK` or `DUPLICATE` with KNOWN_NOT_COMMITTED (what an unchanged node sends today) → the same `MutationStopped` with status `WRITE_REFUSED` and knowledge KNOWN_NOT_COMMITTED: the node said it did not commit, and the client does not repeat the success label.
   - `OK` or `DUPLICATE` with UNSPECIFIED, INDETERMINATE or an unknown knowledge value → a new error `MutationAnswerInconsistent { operation_id, status, knowledge }` carrying the raw status and raw knowledge received. Its effective outcome is UNKNOWN: the client claims neither commit nor non-commit, and it is never a reason to start a fresh identity; the caller may ask for the recorded outcome or repeat the same identity and request.
7. The client's retry classifier treats `WRITE_REFUSED` as final (no retry, no exhaustion rewrite): it is an answer about the identity, not about the attempt. An inconsistent answer is not retried by the client and is never summarized as known-not-committed at exhaustion.
8. The verdict is one small pure function over (status, knowledge) in the `wendelon` crate's schema layer, which both the node-side constructor rule and the SDK use (the SDK depends on `wendelon`, never the reverse). `integration/compute.rs` `map_write_status_for_compute` stays what it is, a mapper of a STATUS carried by an error (`ClientError::WriteFailed`), not a consumer of a wire answer; it is not given a knowledge it does not have. What is guaranteed there: `WRITE_REFUSED`, and both new error shapes, map to an error kind, never to `StorageStatusKind::Success`.

### Also found by the census (a separate subfinding under DC-238)
9. `finalize_table_delete_attempt` (`mutation_protocol.rs`) has no mode check, unlike `finalize_table_write_attempt`, whose comment says a ParentDecisions node "cannot tell from a redirect or an admission refusal whether the identity is reserved elsewhere". So in ParentDecisions a retryable answer to a delete's last attempt is rewritten to `WRITE_ATTEMPTS_EXHAUSTED` + `KNOWN_NOT_COMMITTED` and recorded: unknown turned into known-not-committed. Decision: the same mode check as the write's: in ParentDecisions the answer goes out as it is and no local terminal outcome is recorded. Today-mode behaviour is unchanged and keeps a control. Read from source; no test reaches it today; its RED is written first.

## Census (non-test code at a8e865458)
Producers of `OK` / `DUPLICATE`:
| Site | Mode | Knowledge today | After |
|---|---|---|---|
| coordinator.rs `settled_response`, Committed (table, key-value) | ParentDecisions | COMMITTED | same |
| coordinator.rs `settled_response`, KnownNotCommitted | ParentDecisions | KNOWN_NOT_COMMITTED with OK | `WRITE_REFUSED` |
| deletes.rs Committed (Accepted, Noop) | ParentDecisions | COMMITTED | same |
| deletes.rs KnownNotCommitted | ParentDecisions | KNOWN_NOT_COMMITTED with OK | `WRITE_REFUSED` |
| data_server.rs `committed_response_for`; delete and write noop answers | Today | COMMITTED | same |
| `duplicate_response_for_outcome`; duplicate after reconciling a pending record | Today | COMMITTED (inherited) | same |
| mutation_protocol.rs durable replays: DUPLICATE only when the stored knowledge is COMMITTED, else the stored status | Today | stored | same |
| `resolve_mutation_outcome` (stored status and knowledge as they are) | both | stored | same; its consumer already reads knowledge |
| counter.rs add: OK, or DUPLICATE on replay | both | UNSPECIFIED | COMMITTED, under Decision 3's condition |
Every other KNOWN_NOT_COMMITTED stamp already accompanies a non-OK status.

Consumers:
| Site | Reads knowledge today | After |
|---|---|---|
| SDK `write` → `WriteReceipt` | no | the one function |
| SDK `counter_fetch_add` → `CounterAddReceipt` | no (checks the delta only) | the one function plus its delta check; no exception |
| SDK `table_write` → `TableWriteReceipt` | no | the one function |
| SDK `table_delete` → `TableDeleteRowsReceipt` | no | the one function |
| SDK `resolve_table_mutation` | yes | unchanged |
| client core `retry_write_response`, `attempt_unresolved`, exhaustion helpers | partly | `WRITE_REFUSED` final; rest unchanged |
| `integration/compute.rs` `map_write_status_for_compute` (status of an error; not a wire-answer consumer) | no | unchanged in role; the new status and the new errors map to error kinds |
| Toron's coordinator (compares knowledge already) | yes | read in implementation; changed only if it accepts OK without COMMITTED |

Tests that encode the old pair and change with it: `a_recorded_refused_parent_answers_known_not_committed_whatever_attempts_remain` (asserts `OK`), `a_new_leader_with_one_voter_down_still_decides` (loops while `OK` + KNOWN_NOT_COMMITTED), and the four in-process tests that assert only the knowledge of a refused answer gain the status assertion. No test's assertion is weakened; fixtures that script `OK` answers state COMMITTED explicitly where they mean a commit.

## Compatibility
- No version negotiation exists on the client data path, and none is added. Both mixed pairings fail safe by construction: an unchanged client that receives status 26 maps an unknown status to `WriteFailed { status }` (an error; it loses the knowledge but reports no success); a changed client that receives today's `OK` + KNOWN_NOT_COMMITTED refuses it under Decision 6.
- A changed client against an unchanged node's counter add (OK, knowledge unset) reports `MutationAnswerInconsistent`, outcome unknown, where it reported a receipt before. Stated, not hidden: node and client are released together; pre-release there is no deployed mix.
- Response sizes and layouts are unchanged. The write-outcome metric names only statuses 0 to 14 and counts every later one, this one included, as `unknown`; that is left as it is.

## Algebra
- An answer's meaning is the pair (status, knowledge). Success is the set {OK, DUPLICATE} × {COMMITTED} and nothing else; the producer rule and the consumer function are the two halves of the same predicate, each enforced at one place.
- A receipt type is constructed only from that function's result, so "receipt for a non-commit" has no construction path in the client.
- The client never strengthens knowledge: the knowledge in an error is the one received, except that an unknown value is reported as received and classed as unknown.

## Limits, stated
- This corrects how a refusal is answered and accepted. It does not change when a reserved write is refused: a write whose coordinator's lineage closes during a leadership change is still refused, and the caller, now told so truthfully, must issue a new operation. Whether that refusal is too eager is a separate question (availability), not examined here.
- It does not prove what happened to the row in the observed run.
- A caller holding an old client keeps the defect against an old node.

## Acceptance
1. The two REDs turn GREEN unchanged: public `table_write` and `table_delete`, answered `OK` + KNOWN_NOT_COMMITTED, return `MutationStopped` with the sent identity and KNOWN_NOT_COMMITTED. Added: the same for the key-value `write`; `WRITE_REFUSED` answers map the same way; `OK`/`DUPLICATE` with each of UNSPECIFIED, INDETERMINATE and an unknown value → `MutationAnswerInconsistent` with the identity and the received value; positive controls for OK + COMMITTED and DUPLICATE + COMMITTED on all four receipt builders.
2. Producer: a recorded Refused parent, obtained from a real in-process cluster, is answered `WRITE_REFUSED` + KNOWN_NOT_COMMITTED for a table write, a key-value write and a table delete (the delete case is new: no test reaches `DeleteSettled::KnownNotCommitted` today), on the first answer and on a re-ask at another node; an Accepted parent still answers `OK` + COMMITTED and a re-ask the same.
3. Architecture test: in the node, the four public mutation answer frame builders apply the wire rule and the two refusal producers use `WRITE_REFUSED`; in the SDK, no success status is read directly and each of the four receipt types is constructed once, in the arm guarded by the one function's verdict.
4. Decision 9: RED then GREEN: a ParentDecisions delete's last attempt, answered retryably, goes out unchanged and leaves no recorded local outcome; a Today-mode control keeps the rewrite.
4b. Scripted controls for the refusal status: `WRITE_REFUSED` + KNOWN_NOT_COMMITTED → `MutationStopped`; `WRITE_REFUSED` with each of COMMITTED, INDETERMINATE, UNSPECIFIED, unknown → `MutationAnswerInconsistent`; one request on the wire in every case (no retry).
5. One combined checkpoint over the changed packages (`wendelon` and `wendelon-client`), and the real three-daemon workflow once, on the final tree. With this change a refused row makes the harness's write step fail at the refusal instead of at the later read: that is still a FAILED workflow, reported for disposition; the harness is not changed to retry under a new identity.

## Stop points
Plan review → REDs for 2 and 4 → implementation → source review → the whole-crate run and the real-daemon run on the agreed final tree (with or without ADR 0731 composed, as ruled) → delivery ruling.
