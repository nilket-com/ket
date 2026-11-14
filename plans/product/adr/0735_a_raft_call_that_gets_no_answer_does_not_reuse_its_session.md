# ADR 0735: a Raft call that gets no answer does not reuse its session

Status: accepted for implementation, 2026-11-14. Not delivered; the plan stays off main until its reviewed implementation follows. Defect DC-237, which this narrows and does not close.

## Why
- A node's Raft client for a peer (`WendelonRaftNetwork`) keeps one session to it and reuses it for every call unless the session reports itself closed. Beneath it, the dial provider production gives it is the shared `TransportProviderFactory`, which has a cache of its own and likewise hands back a cached session unless it reports closed. Nothing a failed or cancelled call does clears either cache.
- A session whose peer has died without a word does not report itself closed until its transport gives up on it (the configured idle limit is 300 s). openraft bounds each call (the heartbeat interval) and drops it at the bound. So a call on such a session is dropped unanswered, and the next call is made on the same session.
- Demonstrated on unchanged code, through the real factory over a scripted lowest provider (`raft1.log`, one execution): with a first session that is not closed and never answers, the first AppendEntries was seen waiting on it and was dropped at its 250 ms budget; the next call was made on the same session and was dropped too, with one dial in all at the lowest provider (answered: none; dials 1; sessions used [0, 0]). The control, a session that answers, served three calls on one dial.
- Context, not attribution: in the three-host serving-node-loss run of ADR 0734's record, the new leader's AppendEntries to the killed node missed their 250 ms budget from the kill to the end of the run, the node's return on the same address included; the returned node logged no sign of hearing its leader and campaigned. No session state was retained for that run. This ADR does not claim that was the cause, nor that the scenario passes with it.

## Decision
1. **A call without a protocol answer makes its session suspect.** The Raft client keeps a mark beside its cached session. The mark is set before the call can first suspend on that session, and cleared only when a complete response frame has been read, passed the envelope and message-type check, and decoded. It is plain state of the client, which a call borrows exclusively, so a call that is dropped at its caller's budget leaves it set, and so does a call that returns a transport, framing, envelope or decode error.
2. **A valid response keeps the session**, whatever it says: a response that decodes to a Raft error (a rejection, a forward-to-leader) is communication, and the session is reused as now. Error mapping to openraft (`Unreachable` for transport failures, `RemoteError` for logical ones) is unchanged, and so is peer authentication.
3. **The next call does not select a suspect session, at either layer.** Before it uses a session, a client whose mark is set drops its own cached session and tells its dial provider to discard THAT session, then asks the provider for a session as it does today. One unanswered or failed call is enough; there is no second-failure threshold.
4. **`TransportProvider::discard(target, session)`**, a new method with a default that does nothing, for providers that cache no sessions (the QUIC, simulated and test providers). A provider that caches sessions MUST implement it: after `discard(target, s)` returns, that provider no longer hands out `s` for `target`. `TransportProviderFactory` implements it: under its cache mutex, with no wait inside, it removes the entry for `target` only when that entry is the very session passed in (pointer identity).
5. **Exactly that session, and nothing else.** A caller holding an old session cannot remove a newer one another task has cached: the next dial then simply returns the newer one, with no further handshake. Nothing is closed: the suspect session is not shut, its other streams are not disturbed, no endpoint is touched; holders of it keep their handle and finish or fail by themselves. Another client that privately holds the same suspect session is not repaired by this discard; it needs its own unanswered call to notice.
6. **Scope.** The Raft call path (`append_entries`, `vote`, `install_snapshot`, all through the one `call`) and the factory's `discard`. The stale comment in `raft_network.rs` that calls its per-peer cache "the whole story" is corrected. No data-plane call site is changed.

## The cases, precisely
| How a call on session S ended | Mark after | Next call |
|---|---|---|
| Response read, envelope and type valid, decoded (success body) | clear | reuses S |
| Same, body is a Raft error | clear | reuses S |
| Transport error at connect, open, write, finish or read | set | drops S here, `discard(target, S)`, asks the provider |
| Response read but wrong envelope, wrong type, or undecodable | set | the same |
| Dropped by its caller before it returned (openraft's budget) | set | the same |
| No session could be obtained at all (dial failed) | set, nothing cached | asks the provider |
`discard` at the factory: cache holds S → removed; cache holds another session T → untouched, and the next dial returns T; cache empty → nothing.

## What does not change
openraft's timeouts and the consensus timing profile; QUIC idle and keep-alive values; what `is_closed` means; the factory's negotiation, dialing and its existing closed-session replacement; any wire or durable format; any public setting; ADR 0734's lease renewal; every data-plane caller of the factory.

## Limits and costs, stated
- It promises an OPPORTUNITY to recover, not completion. A single slow call discards a healthy session; the next call then pays a handshake, and where a handshake does not fit the caller's budget, repeated cancel-and-redial can prolong an outage after the peer is healthy again. No timeout, threshold, dial task or profile is changed to prevent that; the declared profile and the one real-host scenario are the evidence that follows.
- A discarded session is not closed: it lives until its holders drop it or its transport ends it.
- Remaining exposure, recorded and NOT addressed: the same factory cache serves the node's data-plane callers of `get_connection` (16 call sites at 847c10fe5: data_server.rs 8, control_server.rs 4, child_writes.rs 1, lease_batch.rs 1, repair.rs 1, decision_proposals.rs 1) and the client library's. They carry no per-call mark; none has a demonstrated failure of this kind. ADR 0734 bounds how long a lease renewal waits on such a session; it does not replace the session.
- It says nothing about what a node does once it can hear its leader again: the returning node's higher term and shorter log, its `CertifiedBaseUnvalidated` readiness reason and its recovery-integrity warning in the retained run are separate and unexplained.

## Algebra
- Per client: state (session?, suspect). `call` begins: if suspect ∧ session = S then discard(S) at the provider and session := none; then suspect := true, BEFORE anything that can wait (obtaining a session included); obtain a session; … ; on a valid decoded response suspect := false. Every exit other than that one leaves suspect = true, including no exit and a failed dial. Nothing clears it on drop.
- Factory `discard(target, S)`: cache[target] := none iff cache[target] ≡ S. It can remove at most the session named; it never adds, closes or replaces.
- Reuse is preserved exactly when the last call on the session produced a valid protocol response.

## Acceptance
1. The RED (`session_recovery_tests::the_call_after_an_unanswered_one_reaches_a_new_session`), unchanged, passes: answered, two dials at the lowest provider, sessions [0, 1]. The control still shows three answered calls on one dial.
2. A call that returns a transport error (the session's `open_bi` fails) is followed by a call on a new session.
3. A valid response whose body is a Raft error keeps the session: the next call uses it, no new dial.
4. Factory: `discard` with the cached session removes it; `discard` with a stale session leaves a newer cached one in place; and the next `connect` after a stale discard returns that newer session with no further dial.
5. Existing raft_network and factory tests pass unchanged.
Checks: the focused selection (these tests, the factory's tests, the in-process cluster modules that exercise elections and restarts: `primary_down`, `lease_renewal`); then ONE whole-crate run; then ONE reviewed three-host serving-node-loss execution at the unchanged 250 ms consensus base and 2000 ms query profile as the real-daemon record for a transport-path change, reported whatever it does. No new profile, cap or run beyond those.
Cost: product delta about 40 lines (consensus/raft_network.rs, network/transport.rs, network/factory.rs); tests about 150 lines beside the 300 already written; build about 8 minutes; focused tests minutes; whole crate about 13 minutes; the three-host run about 15 minutes.

## Stop points
Plan review → implementation (the retained RED first) → source review → the whole-crate run and the one three-host execution → delivery ruling.
