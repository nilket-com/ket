# ADR 0709: a remote decision RPC has a deadline

Status: proposed, 2026-11-10 (Claude, Codex). Direction and semantics cleared (Codex 01a1c6a0). Implementation lands with this plan. Closes DC-206 once its implementation and evidence are reviewed. Review record: `reviews/dc195-layout-collision/` (DC-206 entries, `BRIEF-DC206-RED.md`, `CODEX-DC206-NEXT.txt`).

## Why
- **DC-206 (availability; fails closed):** `DecisionProposals::round_trip` (`network/decision_proposals.rs`) carries a follower's decision RPC to the leader: a forwarded proposal, a lookup or a read cut. Its doc says "one bounded request and response", but it has no application deadline. Resolving or connecting, opening the stream, writing, finishing and reading the answer are all unbounded. A leader that never answers holds the caller for as long as the connection lives. QUIC's 300 s idle timeout is not a ceiling: keep-alives and other traffic on the shared connection can keep it alive.
- **RED on main 2fff28742 (dc206-r1):**
  - Setup: `a_follower_coordinated_write_answers_unknown_while_the_leader_holds_its_reserve`. The leader holds the forwarded Reserve of a write coordinated at a follower; a handshake proves the real request is waiting at the leader.
  - Result: the write gave no answer within the stated 10 s bound. After the release, the same attempt committed.
- **Callers (census).** `round_trip` is reached through `forward` (`propose`), `lookup` and `cut`. Twelve sites, by their outer bound today:
  - **None:** the write coordinator's lookup, Reserve and Finalize (`coordinator.rs`; it does not read the client's deadline); the delete lookup (`deletes.rs`); the KV read cut (`reader.rs`; KV requests carry no deadline); the maintenance base-validation cut (`certification.rs`; not under ADR 0707's stop).
  - **Optional:** the delete evaluation's cut, Reserve, RecordNoop and Finalize (`bounded(deadline, …)`, unbounded when the deadline is absent); the table query cut (`StatementBound`).
  - **Fixed:** the clock probe's cut (2 s).
  - **The ADR 0707 stop only:** maintenance's forwarded proposals.

  Every site already maps an `Indeterminate` answer, or an io error, to a correct typed result: a stalled write or delete (INDETERMINATE), BACKPRESSURED for a read, a failed validation retried next tick, or an ignored maintenance answer re-derived next tick. None maps it to NotFound, KnownNotCommitted, a rollback or a new identity.

## Decision
**One deadline bounds each remote decision RPC attempt: `DECISION_RPC_DEADLINE` = 5 s, monotonic, set before the first asynchronous step (resolution and connection) and never reset between stages. On expiry the attempt ends with `io::ErrorKind::TimedOut`, which the existing wrappers turn into `Indeterminate`. That is an outcome unknown: never evidence that the request was not delivered, refused or not committed.**

1. **Scope.**
   - **Covered:** `round_trip`, and only it: the follower side of `forward`, `lookup` and `cut`.
   - **Not covered:** local waits (the leader's own `propose_as` and `client_write`, `ensure_linearizable` in `cut_as_leader` and `lookup_as_leader`), and the leader-side handlers of remote requests. A timeout at the follower does not cancel the leader's admitted work, which may still commit.
   - **Shorter limits still apply:** a caller's shorter outer deadline (`bounded`, `StatementBound`, the 2 s clock probe) and ADR 0707's maintenance stop keep their effect, because they wrap the call.
2. **Semantics.**
   - An expired attempt is `TimedOut` and becomes `ProposalResponse::Indeterminate`, `LookupResponse::Indeterminate` or `CutResponse::Indeterminate` through the existing mapping. Elapsed time never fabricates `NotLeader`.
   - A Reserve or Finalize that times out is outcome unknown, even if the leader commits it later. A retry keeps the parent key and request digest and acts on the authoritative record. A retry after Absent rebuilds the claim (fresh commit HLC and signature); the first recorded claim wins, as for racing attempts today.
   - No timeout-triggered rollback, intent deletion, new identity, `KnownNotCommitted` or assumed absence.
   - Read callers map the uncertainty to BACKPRESSURED or a retry: never NotFound, never a stale cut.
3. **Cancellation.**
   - The deadline drops only this RPC's own stream. The stream is per RPC; the shared connection, cached by the factory, stays usable. Under QUIC, dropping the stream finishes its send side and sends STOP_SENDING on its receive side; neither touches the connection.
   - A cancelled dial changes nothing: the factory inserts a connection into its cache only after the dial completes.
   - No detached timeout task and no automatic transport-level retry.
   - Because a stream's credit stays used until the leader's handler ends, the deadline also covers `open_bi`. Repeated timeouts against a stuck leader then cannot block without bound waiting for stream credit.
4. **The bound, 5 s.** It is a design value, not a measured latency guarantee:
   - It sits above the 2 s clock-admission wait and the 2 s probe, so an answer delayed only by that admission still arrives.
   - It sits below the 15 s lifecycle drain bound, so a follower's base-validation cut, now bounded, cannot alone exhaust a drain.
   - It is one named production constant.
   - It promises no whole-write or shutdown time: an operation makes several calls and has other waits.

## Acceptance (finite)
1. **The RED passes:** while the leader holds the follower's forwarded Reserve, past the reached handshake, the write answers WRITE_STALLED with INDETERMINATE within its bound, before the release.
2. **No false non-commit and no duplicate logical write:**
   - **Reserve:** after the timeout, the leader commits the held Reserve late. The same original identity, retried, reaches the one recorded outcome: one parent, one committed write, rows read back exactly.
   - **Finalize:** the same, with the hold on the forwarded Finalize.
   - **Normal path:** a control where the leader answers in time is unchanged.
3. **A helper-level control:** the deadline covers the whole transport sequence, connect included, with a peer that accepts but never answers. A forwarded lookup and a cut each end `Indeterminate` within the bound, and the shared connection serves the next RPC.
4. **The focused set**, including `node::parent_decisions_cluster_tests`, `three_node_workflow`, `decision_proposals`, `deletes`, `reader` and `architecture_test`, passes; then one checkpoint at the tip.

## Not established
- Bounds for the local waits listed in Decision 1 and for the leader's handlers. The drain stays bounded only by ADR 0706's 15 s abort.
- DC-207: the coordinator's child writes to replicas (`send_child_frame`) have the same unbounded pattern. It is registered separately.
- A table write still ignores the client's `deadline_remaining_millis`. That is unchanged here.
- DC-201, §8, and the other registered limits.
