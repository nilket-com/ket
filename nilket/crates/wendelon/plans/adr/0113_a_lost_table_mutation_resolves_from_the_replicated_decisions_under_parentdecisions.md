# ADR 0113 - A lost table mutation resolves from the replicated decisions under ParentDecisions

## Status
Proposed, 2026-11-19. The design was ruled by Codex 01a1f396, after RESOLVE-01. This is the third unit of the cutover to ParentDecisions as the sole serving decision authority, after product ADR 0738 and wendelon ADR 0112. Its scope is the opt-in ParentDecisions table/SQL contract. The default-authority cutover is a separate decision.

## Context
**The public path** (RESOLVE-01, on fresh local profiles, through the public SQL client): a committed INSERT and DELETE, and a never-used id.

| | Today | ParentDecisions |
|---|---|---|
| INSERT | resolves `Present`, Committed | the resolve fails: `StorageError: … early eof` |
| DELETE | resolves `Present`, Committed, counts `[2,0,0]` | the same `early eof` |
| never-used id | `Unknown` | the same `early eof` |

**Why ParentDecisions fails:**
- **The gate:** it refuses `MUTATION_OUTCOME_REQ` and `MUTATION_OUTCOME_PIN_REQ` before any handler and writes no response, so the stream drops.
- **Nothing to read locally:** under ParentDecisions nothing is written to Today's node-local outcome store.

**What a caller keeps after losing a reply:** only its authenticated tenant and the operation's query id (`resolve_mutation(query_id)`; wire `MutationOutcomeRequestMeta { operation_id, tenant_id }`). It has no table and no request digest.

**Toron mints one id per statement,** and every DML statement writes one table. Two tables under one id happen only when a caller reuses an id:
- **Today:** a reuse on another table answers OK and writes nothing (a silent replay, registered as DC-266).
- **ParentDecisions:** it executes, recording a second parent under one id, because `ParentKey` includes the table namespace.

**CTAS** is refused at its provision under ParentDecisions, before its pin, and stays so. Pin and acknowledge are deferred to their own identity and lifetime design.

## Decision

### D1. One table operation identity per tenant
- **The scope:** for the table/SQL contract, `(tenant, client UUID, sequence)` names at most one table. The TTL and padding are not identity.
- **A Reserve or RecordNoop** whose identity is retained under another table answers `Conflict` (`WRITE_KEY_CONFLICT`).
  - The check is in `lookup`, so the reducer, the network lookup and a coordinator's pre-check agree.
  - It applies atomically at apply: of two concurrent offers on two tables, exactly one parent exists, and the other answers conflict with no effect.
  - The first table's receipt is unchanged.
- **Same-parent digest conflict and replay** are preserved.
- **The KV namespace (ADR 0699) stays independent:** it neither conflicts with a table identity nor answers a table resolve.

### D2. One derived index
**The index:** `DecisionState.table_operations: (tenant, uuid, seq) → table`, holding exactly the retained TABLE keys.
- **Maintenance:**
  - updated with each key insert (Reserve, RecordNoop);
  - retired with its key at the floor;
  - **never serialized**, and rebuilt from `keys` on every decode;
  - `validate` checks it equals what the keys imply;
  - a state holding one identity under two tables refuses (fail closed).
- **Cost:**
  - lookup and admission use the index (O(log n)); there is no map walk;
  - per retained table key, about 48 bytes of entry plus B-tree overhead, roughly 64-80 bytes;
  - it is bounded by the retained keys, as `keys` is.

### D3. A linearizable resolve at any node, with the public request unchanged
**How a node resolves:**
1. it takes the leader's committed cut (the existing `DECISION_CUT` exchange, a compact index);
2. it waits until its own state is applied through that cut;
3. it reads the applied decisions by the identity alone (`resolve_table_operation`).

**No claim is forwarded.** Each node reads its own applied state at or beyond the leader's barrier. A call answers as of that point: two calls, or two nodes asked at different instants, can observe a later decision (a pending parent that has since been decided).

**Key presence first, then the replicated `retirement_cutoff()`** for an absent valid UUIDv7:

| applied state | resolution | status | knowledge | kind | counts |
|---|---|---|---|---|---|
| Accepted | PRESENT | OK, `dec`, commit HLC | COMMITTED | WRITE / DELETE | a delete's frozen counts |
| Noop | PRESENT | OK, `dec`; no commit HLC (a noop has none) | COMMITTED | DELETE | its counts |
| Refused | PRESENT | WRITE_REFUSED | KNOWN_NOT_COMMITTED | WRITE, or `DELETE_UNCOUNTED` (new value 2) for a delete | none: the SDK reports `None` |
| Reserved, at any cutoff | PRESENT | FINALIZATION_PENDING | INDETERMINATE | WRITE, or `DELETE_UNCOUNTED` for a delete | none: the SDK reports `None` (it may still delete) |
| no key, issued above the cutoff | UNKNOWN | WRITE_OPERATION_UNKNOWN | — | — | — |
| no key, at or below the cutoff (retired, even with its body retained; or never seen) | EXPIRED | WRITE_OPERATION_EXPIRED | — | — | — |
| not a UUIDv7 | INVALID_REQUEST (new value 4) | WRITE_IDENTITY_INVALID | — | — | — |
| no cut, not applied in time, or no state | UNAVAILABLE (new value 3) | WRITE_STALLED | INDETERMINATE | — | — |

- **The canonical id is echoed.**
- **A failed cut or apply never becomes UNKNOWN, EXPIRED or not-committed.**
- **The public client:**
  - counts UNAVAILABLE as an error from that node (`TimedOut`), so UNKNOWN is never answered while any node is unavailable;
  - refuses INVALID_REQUEST as invalid input;
  - turns any other unrecognized value into an error, as today (fail closed on older clients);
  - keeps "first PRESENT wins". Each node's answer is linearizable at its own barrier.
  - **reports a delete's counts only for a counted `DELETE`** (`MutationOutcomeResponseMeta::delete_counts`). A pending or refused delete is `DELETE_UNCOUNTED` and gives `None`. A committed delete's or noop's zeros are counts. An older SDK, which counts only `DELETE`, also gives `None` for it. Today still sends `DELETE` with its counts, including those of its pending finalizations, unchanged.
- **Bounded at both ends:**
  - **the server:** one budget (`PD_RESOLVE_READ_BUDGET`, 3 s) covers the cut, the apply through it, and the read of the applied state under its lock. Past the budget the answer is UNAVAILABLE.
    - **At the leader,** its own linearizable barrier is dropped where it stands.
    - **At a follower,** only its wait for the leader's reply is dropped. The leader's cut (`handle_decision_cut`, `cut_as_leader`) runs on to its own end.
    - **Two reads at different instants** may answer differently: each is linearizable at its own cut.
  - **the client:** the request carries no caller deadline, so one whole-resolve budget (`MUTATION_RESOLVE_BUDGET`, 10 s) covers every node asked. It is the shared client's, so it bounds a resolve on Today too. It allows three UNAVAILABLE answers at 3 s each. A node that withholds its answer ends the call with a `TimedOut` error and no outcome. Replies already received keep their precedence.
- **The frame layout is unchanged.**

### D4. Pin and acknowledge
- **Under ParentDecisions,** a pin or acknowledge answers `UNSUPPORTED` (new pin status 3) after write authorization. An unknown action stays `INVALID_ACTION`.
- **The client** maps `UNSUPPORTED` to `ErrorKind::Unsupported`.
- **The retention semantics** (holding a key's retirement until acknowledged) are deferred, with CTAS.

### D5. Formats: a clean pre-release bump, ParentDecisions only
**Why:** apply behaviour changes (D1), so a v19 store and its peers are incompatible.

**The new versions:**
- marker `parent-decisions-v20` (v19 retired; refused by both authorities, bytes kept, readable by its own binary);
- WAL and snapshot format 30;
- Raft/decision envelope 23.

**Unchanged:**
- **the serialized bodies:** the format-30 corpus equals format 29;
- **Today:** its versions and its outcome store.

**What versions which wire:**
- **The resolve** uses the existing public `MUTATION_OUTCOME_REQ`. Its meta is unchanged and its new status values fail closed on old clients.
- **The DECISION_CUT forward is unchanged** (it carries `HRFT_ENVELOPE_VERSION`). A mixed-version cluster is refused at the Raft envelope (`check_raft_frame`, `check_raft_response`, `validate_frame_version`) and by the marker. No forwarded lookup changes shape.

## Rationale
- A caller holds only the tenant and the id. Only a tenant-wide identity makes a namespace-free answer unambiguous, and it turns a reuse into an explicit conflict instead of a second parent.
- The cut-then-apply read reuses the linearizable read the delete path already uses. No claim crosses the wire to extract a receipt.
- Key presence before the cutoff keeps a recorded pending parent pending, and never promises a commit for an id it cannot see.

## Consequences (The "Negatives")
- **A caller that reused an id on another table** now gets a conflict, where Today silently replays.
- **ParentDecisions expiry is the cluster's floor/cutoff,** not the request's TTL.
- **Existing v19 ParentDecisions stores need their old binary.**
- **CTAS and pin-based retention remain unsupported** under ParentDecisions.

## Acceptance
**The public path:** `local_profile_public_mutation_outcomes_resolve_on_both_authorities` (live, both authorities):
- committed INSERT and DELETE (with counts) and a never-used id;
- under ParentDecisions, a reuse on another table is refused, writes 0 rows there, and the id still resolves to the first statement;
- RED on `498cf4c37` at the INSERT's resolve.

**Three voters** (`parent_decisions_cluster_tests::resolve`):
- every outcome, alike from every node: write, delete with counts, noop, pending, refused, unknown, expired, invalid;
- after a leader change and after a full restart;
- UNAVAILABLE without a quorum, also for a fresh id;
- the collision in both orders, and two concurrent offers on two tables (exactly one parent, the loser's table untouched);
- pin and acknowledge typed.
- **a pending, then refused, DELETE** resolves with the exact status and knowledge and **no counts** at the SDK's conversion. A committed delete keeps `[1,0,0]` and a noop `[0,0,1]`. RED before the fix: `Some([0,0,0])`.
- **a withheld answer** (a node that receives the resolve and never answers, and one whose connect never completes) ends the public call at its budget, with `TimedOut` and no outcome;
- **a leader without its quorum** answers UNAVAILABLE within the server budget.
- **a held state lock** after a successful cut and apply answers UNAVAILABLE within the server budget, and the receipt once released.

**The reducer:**
- the identity is tenant-wide across tables, for Reserve and RecordNoop, in both orders;
- replay and digest conflict are preserved;
- another tenant and KV are independent;
- the resolve's key-presence and cutoff order;
- the index retired at the floor, rebuilt on decode, failing closed on duplicates.

**Formats:** marker v20 written; v19 refused with bytes kept; envelope 22 refused; the format-30 corpus.

**At delivery:**
- the real three-daemon process test, which resolves its delete statement through every node and a never-used id;
- the daemon-stack lane;
- cross-binary on disposable fixtures: a v19 profile from `498cf4c37` is refused byte-preserving and reopens with `498cf4c37`.
