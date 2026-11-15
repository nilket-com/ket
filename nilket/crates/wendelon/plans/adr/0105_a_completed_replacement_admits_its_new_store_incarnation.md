# ADR 0105 - A completed replacement admits its new store incarnation

## Status
Accepted for implementation, 2026-11-15 (member replacement gate 2; Codex 01a1e0d2, choice P1). Not delivered. Gates 3 and 4 stay closed: the replacement registers and binds its new store, and still serves, signs client work, leads and coordinates nothing.

## Context
- ADR 0104 replaces a lost voter of slot N with a new Raft instance r_new that runs ControlOnly. Its Done record keeps slot N's data unavailable, and leaves the store incarnation, the signer and readiness to later gates.
- Slot N's registered store incarnation is still I, the lost store's. Under ParentDecisions (PD), `RegisterIncarnation` admits only a first registration or its exact retry. An established incarnation is never replaced (`raft_storage.rs` PD branch of `RegisterIncarnation`), and admission always sends `supersedes: None`. So the replacement's store J can never become slot N's current store.
- Everything that may act for slot N is judged against the current incarnation:
  - reports, no-ops, takeovers, members, acks, lineage use, lease renewal and regrant (`lineage.incarnation`);
  - slab owner operations (`admit_slab_owner`);
  - child repair;
  - reads (`ReadAuthority`).

  `DecisionState.signers` is keyed by (node, incarnation) and never pruned, and evidence verifies against the key bound to its exact incarnation.
- **Trust model (unchanged from 0104):** operator-issued node credentials; honest participants under crash, restart, partition and stale-state faults.

## Decision

### D1. `PromoteIncarnation`: one replacement admits one new store
- **Request.** A new PD-only request `PromoteIncarnation { token, r_new, birth, expected: I, incarnation: J }`. It is forwarded like `RegisterIncarnation`. Its node is the authenticated sender (ADR 0103).
- **Apply admits it only when all of these hold:**
  - `replacements[token]` exists, its slot is the sender, and its phase is `Done { r_new }`;
  - `instances[r_new]` has kind `Replacement { token }` and this birth;
  - `incarnations[N].current == Some(expected)`;
  - J is neither accepted nor retired for N;
  - **P1:** no lineage is held at (N, expected).
- **Effect, in one apply:**
  - current = J;
  - `expected` joins `retired`;
  - (J, Some(expected)) joins `accepted`;
  - the replacement records `promoted = Some(Promotion { expected, incarnation: J })`.

  The registry observer event is emitted as for a registration.
- **Retries and refusals** (each refusal changes nothing):
  - An exact retry of the recorded promotion answers Success and changes nothing. That answer is historical. It grants no renewed authority if a later replacement has since superseded J.
  - The same token with any other J, expected or birth is `PromotionConflict`.
  - Before Done: `ReplacementNotDone`.
  - A current that differs from expected: `StaleSupersede`.
  - J already known: `RetiredIncarnation`.
  - P1 unmet: `LineagesStillHeld`.
  - Under Today: refused.
- **P1 is an apply-time refusal, not a prune.** ADR 0729 takeover moves (N, I)'s lineages to survivors first. A shape where takeover cannot (`UnsupportedShape`) leaves the replacement waiting; that is a supported-shape limit, not a claim for every RF or layout. No hand-back to N is required.
- **Other paths unchanged.** Ordinary `RegisterIncarnation` (PD first-only, Today supersede) and `BindSigner` rules are unchanged on every other path.

### D2. Signer
After promotion, the existing `BindSigner` binds (N, J) to slot N's configured key: the trust maps are unchanged, and the same node may bind its key under a new incarnation. (N, I) stays bound, so retained history keeps verifying. The operator provisions slot N's `decision_signing_secret` on the replacement, the same configuration slot N always had. No credential enrollment change.

### D3. Preparation fence (Done to activation)
- **The predicate.** `preparing(N)` holds while a replacement of slot N is Done and its r_new is not retired. Gate 4's activation will end it; until gate 4 exists, it does not end.
- **Refused at the reducer for slot N while it holds:**
  - lease grants, renewals and regrants with holder N (`AcquireLease` and `AcquireLeases`);
  - `TakeOverLineage` with successor N;
  - a `Reserve` coordinated by N.
- **Still admitted:** resolution of prior work (`Finalize`, `Resolve`, `Abort`), exact historical retries, and this ADR's own preparation (`PromoteIncarnation`, `BindSigner`).
- **Why.** The old process may return after Done and before promotion, with I still current. Without the fence it could keep renewing I's leases, so P1 would never clear. The fence makes its lineages lapse to survivors' takeover.
- **Deferred to gate 4, recorded here.** A lease request names only a slot, and `admit_lease_grant` stamps the holder slot's current incarnation. Gate 4 must bind lease and serving requests to the caller's incarnation, so that J and a lingering old I are told apart. It must not stamp whichever incarnation is current onto a slot-only request.

### D4. ControlOnly preparation (feasibility F1 passed)
- **J's origin.** J is read from the replacement's own store index (`ControlOnlyNode::store_incarnation`), never from the Raft store's birth or the directory owner. The index is opened once under the node's owner and held until shutdown; no slab manager is built.
- **Creating the index.** It is created only when no promotion is recorded for this replacement, and only in a directory holding nothing but first-bootstrap artifacts (`startup::interrupted_bootstrap_artifact`) and an enrolled root. Its incarnation is minted in the transaction that writes it.
- **Refusals.** A missing index once promoted is `ReplacementStoreLost`; any other content is `ReplacementStoreUnexplained`. A damaged index refuses as at every open. Nothing is minted on a restart after promotion. A local J different from the promoted one stops preparation (`ReplacementStoreNotPromoted`).
- **The task.** One preparation task in the ControlOnly node. It waits until its own applied state shows Done for its instance, finds its token by r_new, reads J, and proposes in this order, each only while applied state lacks it:
  1. `PromoteIncarnation`;
  2. `BindSigner`.

  It forwards through a client-side `DecisionProposals` and runs no other maintenance step. Elections stay disabled, the router stays Raft-only, the role stays ControlOnly, and no lease, takeover, generation, certification, sweep or data handler exists.
- **Crash at each step resumes.** The same J is read again; each proposal is skipped once applied state holds it.

### D5. The old process after promotion
- **Refused:** every submission bearing I (D1's context list), and the D3 fence before promotion.
- **Kept:** (N, I) signer bindings and proofs, retained claims and terminal records, and I's tier pointers and objects. I's objects have no deletion path, and their reclaim is deferred.
- **OP-2, reads.** A read takes the leader's cut K and waits until K is applied locally (`reader.rs`). Only then does `ReadAuthority` check that its incarnation is current and that applied ≥ K. The excluded old process receives no entries after the swap, so a cut taken after promotion is never applied there. If it were applied, the incarnation check would refuse. A read linearized before promotion is not invalid. Disposition: not demonstrated as a defect. The guards are retained, and a regression control is added.
- **Named gap OP-3.** The old process holds slot N's same signing key. Fencing is by incarnation, not by key, which is sufficient under the declared trust model and not against a hostile old host. Per-slot key rotation is not in this ADR.

### D6. Effects, stated
- **Certification.** It was already blocked while N could not report. After promotion its target is restaged with (N, J) as a member, and it stays blocked until gate 3.
- **Readiness.** ADR 0104 keeps the catalog's promote-only semantics. Indexes already Ready stay Ready, and slot-keyed receipts (index `drop_gc_completed_by`, drop-reclaim participants, the backup per-node source check) are NOT incarnation-specific yet. Gate 2's safety comes from the replacement not serving. Gates 3 and 4 must discharge those receipts before it serves.
- **Takeover.** The replacement never asks to take over. Survivors' takeover proceeds without waiting on it.
- **HLC identity.** ADR 0104 left "uniqueness against a lingering old process" to gate 2. Gate 2 authors no data commits, so this obligation moves to gate 4, before J coordinates or commits data.

### D7. Compatibility (pre-release; old PD stores refused, not migrated)
- PD WAL/snapshot format 25 → 26. The mode marker moves with it.
- New `WendelonRequest::PromoteIncarnation`, `ForwardedDecision::PromoteIncarnation`, the `Replacement.promoted` field and their refusals.
- The replacement report wire gains the promotion, so `replace status` prints it.
- Goldens are regenerated. Today is unchanged.

## Control map (per semantic boundary)
| # | Boundary | Oracle |
|---|---|---|
| 1 | not Done | Begun, Reserved, or no record: `ReplacementNotDone`; nothing changes |
| 2 | binding | another birth, r_new, token or slot, or a sender other than N: refused; nothing changes |
| 3 | exact retry | Success with no change, also after J is superseded later |
| 4 | conflicting retry | another J or expected under a promoted token: `PromotionConflict` |
| 5 | registry | stale expected: `StaleSupersede`; a known J: `RetiredIncarnation` |
| 6 | P1 | a lineage at (N, I): `LineagesStillHeld`; admitted once takeover moved it |
| 7 | generic rules | PD `RegisterIncarnation` with `supersedes` or another incarnation is still refused; Today supersede is unchanged |
| 8 | retained history | (N, I) evidence verifies after promotion; a new (N, I) report, no-op, ack or lease renewal is refused |
| 9 | preparation fence | after Done, before promotion: a holder-N grant, renewal or regrant, a takeover to N and a Reserve coordinated by N are refused; `Finalize`, `Resolve` and `Abort` of prior work are admitted; survivors' leases are unaffected |
| 10 | OP-2 | the old process, back after promotion, serves no read |
| 11 | process | ControlOnly promotes and binds with slot N's key, and stays ControlOnly through restarts after J's creation, after promotion and after binding, with the same J every time |
| 12 | lost store | index lost after promotion: `ReplacementStoreLost`; nothing is minted |
| 13 | survivors | exact reads and committing writes; the certification target restages and does not commit without J's report |

**Owning daemon scenario (proposed, not launched).** Extend the single-host real three-daemon replacement test:
- after Done, `replace status` shows the promotion of J;
- the replacement restarts still ControlOnly with the same J;
- the old process, back on its old disk, is refused at the fence and serves no read;
- survivors' reads stay exact.

One execution, through the reviewed launcher.

## Interfaces for gates 3 and 4 (notes; not approved)
- **Gate 3 (J's data).** `accepted (J, Some(I))` authorizes a succession and copy relationship only. It is not proof that J holds I's data, and not permission to count I's acknowledgements as J's. Gate 3 owes:
  - durable positive coverage and provenance for the active certified base and every accepted uncertified child naming slot N;
  - the catch-up cut and delta handoff;
  - J's report, equal at the current gid, as the certification barrier;
  - a disposition for each slot-keyed receipt;
  - restricted slab storage under ControlOnly.

  I's pointers, history and objects stay protected, and reclaim stays deferred.
- **Gate 4 (activation).** A committed activation bound to this exact r_new, birth and J, after gate 3's barrier. Then a new narrow page writer moves the role from ControlOnly to Full only (`write_identity` never rewrites an identity). Recovery must work on both sides of the commit / page-write boundary. A stale or foreign activation is refused. Gate 4 also carries:
  - D3's caller-incarnation binding for leases and serving;
  - D6's HLC obligation;
  - the full-start acceptance of a recovered, non-fresh directory.

  Its query oracle reaches N directly, without forwarding or retry, and establishes the serving store's identity; `leader_hint` alone is not that proof.

## Consequences
- After Done, slot N is fenced from leases, takeovers and coordination until gate 4. Its data stays unavailable, as under 0104.
- A layout whose takeover is `UnsupportedShape` cannot promote.
- J's store index exists on the replacement before it holds any data.
- I's objects are kept in the object store, and so are their pointer, claim, generation and history references; none is dropped or reclaimed here (correction after source review: kept, not established as unreferenced).

## Not decided here
- Gates 3 and 4.
- Reclaim of I's objects.
- Per-slot key rotation.
- Cancel or abandon.
- Address change.
- DC-243 step 2.

## As built
- **Request and reducer.** `WendelonRequest::PromoteIncarnation` (appended), `ForwardedDecision::PromoteIncarnation` (the node is the sender), and `decisions::promote_incarnation`. The token, instance, birth and slot checks are `InstanceState::promotion_authority`. The registry rules, then P1 (counting lineages at (N, expected)), are applied before any change. The registry observer event is emitted as for a registration. Refusals are new `SlabRefusal` variants: `ReplacementNotDone`, `PromotionNotAuthorized` (another slot, instance or birth before any promotion), `PromotionConflict` and `LineagesStillHeld { count }`. `StaleSupersede` and `RetiredIncarnation` are reused.
- **Record.** `Replacement.promoted: Option<Promotion { expected, incarnation }>`.
- **Fence.** `InstanceState::preparing(slot)`: a Done replacement of the slot whose r_new is not retired. It is checked:
  - in `apply_lease_grant`, before admission (grant, renewal and regrant, single or batched: `LeaseDenied`);
  - in `take_over`, after the signature (`SlotBeingReplaced`);
  - in `reserve`, after the recorded-parent answer and the backup hold (`SlotBeingReplaced`).

  `Finalize`, `Resolve`, `Abort` and `RecordNoop` are unchanged.
- **Containment (D4 amended).** Besides its Raft handler, a control-only member's router holds one typed rejection handler for data-plane requests (see the DC-246 correction below). It serves, stores and acknowledges nothing.
- **Preparation.** `node::replacement_preparation::Preparation`, one task per control-only node, ticking every 200 ms. It forwards each proposal straight to the leader's address in the applied membership, within the decision exchange deadline (5 s). It builds no transport factory, epoch watch or `DecisionProposals`. A refusal or failure is logged and the next tick reads applied state again. A fail-closed condition (store lost, unexplained or not the promoted one, or a key mismatch) ends the task; the member keeps its Raft obligations. The key is the verifying key of the slot's configured `decision_signing_secret`.
- **Feasibility F1** (one execution, 3.146 s). An owned redb is released only at the owner's close, so the index is opened once and held for the node's lifetime. A second open in the same process waited on its own handle; that run hung until it was killed.
- **Report.** `ReplacementReportWire.promoted`. `replace status` prints `done <r_new>; promoted store <32 hex>`.
- **Formats.** PD WAL/snapshot 26, marker v16 (v15 retired), PD Raft frame 19 (a version-18 peer would grant a preparing slot's leases), golden `parent_decisions_v26.txt` (no body moved; `promote_incarnation` and `promoted_replacement` added).
- **RED.** The seven reducer controls ran against a stub that refused every promotion: six failed as designed, and the unchanged-registration control passed. The in-process process test was written after the preparation code; it has no RED of its own.
- **Corrections after source review** (Codex 01a1e11e):
  - **R1.** A first promotion also needs r_new to serve its slot at apply time: it is not retired, it is a voter in every configuration of the applied membership, and it is not the old instance of an open replacement. Otherwise `PromotionStale`. An exact retry of a recorded promotion still answers its historical success first. RED: the stale first promotion succeeded; GREEN after the check.
  - **R2.** The leader keeps a promotion's refusal as `ProposalOutcome::Promotion { refused }`; ordinary registration keeps its `Incarnation { accepted }` answer. The preparation task re-proposes its first promotion unchanged (the same intent) on a wait or an unknown outcome: no leader, indeterminate, an unhealthy leader clock, `LineagesStillHeld`, `StaleSupersede`, or a success not yet applied here. It ends on any other refusal. RED: a stale replacement re-proposed 296 times in 60 s; GREEN: one proposal, then stop.
- **DC-246 correction (Codex 01a1e13f).** The owning run pd-replace-20261115T215756Z FAILED before any gate-2 assertion. The control-only member dropped a client's `TABLE_WRITE_REQ` unanswered, because its router had no data handler, and the client saw `early eof`. The control-only router now installs `network::data_server::not_serving::NotServing`, a rejection-only `DataRingServer`. It holds no store and runs no duty, and answers each family in its own schema:
  - **Table write and delete:** `NOT_LEADER`, outcome `INDETERMINATE`, no hint, the write id echoed, no position, commit clock or finalization.
  - **Table query:** `NOT_LEADER`, no hint, no rows, stream end.
  - **Child write and replicate:** `ChildWriteResponse::Refused`.
  - **Child fetch:** `ChildFetchResponse::Refused`.
  - **Pending witness:** `INTERNAL_ERROR`, no witnesses, as a degraded node answers. Its caller reads any non-OK as a failed pull, not an empty inventory.
  - **Anything else, or a malformed request:** an error, as before.

  Node-to-node families still require an authenticated data node. So ControlOnly now holds a typed rejection handler, but still no data-serving node.
  - RED: the in-process control's table write got `early eof`.
  - GREEN: every family is answered as above.
  - The client's existing handling of that write answer: the same identity is sent again; when every attempt gets it, the client ends with its own `WRITE_ATTEMPTS_EXHAUSTED` + `INDETERMINATE`.
- **Daemon oracle hooks** (`test-support` only, called by no product code):
  - `Client::table_query_at_for_test`: one table query to exactly one node;
  - `stored_raft_role_for_test`: a stopped store's instance and role, from its vote page.

  The status report also carries the key bound to the promoted store (`; bound key <64 hex>`).
