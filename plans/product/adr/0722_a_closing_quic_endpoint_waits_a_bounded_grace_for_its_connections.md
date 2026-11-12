# ADR 0722: a closing QUIC endpoint waits a bounded grace for its connections

Status: proposed, 2026-11-12 (Claude, Codex). Finding DC-227 and its diagnostic reviewed by Codex 01a1d1b9, 01a1d1bd, 01a1d1c2, 01a1d1c8 (`reviews/pd-daemon-20261112/`). Implementation lands with this plan.

## Why
- A node's orderly shutdown ends its QUIC server by closing the endpoint and then awaiting `endpoint.wait_idle()` with no bound (`QuicServerHandle::shutdown`). `wait_idle` returns when every connection on the endpoint has finished closing and draining; it exists to give peers the chance to learn of the close.
- **With peers that have gone, that wait can outlast the shutdown watchdog.** Observed with three real daemons shut down one after another (ADR 0721's smoke, executions 2 and 3): the first two nodes spent 2.3 s and 3.0 s there; the last node, with no peer left, was still there when the daemon's own watchdog ended the process at 35 s with exit status 86. An instrumented execution showed every earlier step of that node's shutdown done in under a millisecond with nothing in flight, and the wait begun and never finished.
- This step waits on parties outside the process. The steps before it either are bounded (the lifecycle drain, the node's quiescence wait, the storage close) or are unbounded awaits that completed promptly in every observed shutdown (`handles.shutdown()` and the server task joins); this ADR does not bound those.
- Not established: why the library's close takes that long toward an unreachable peer, or which connections were outstanding. This ADR bounds the wait; it does not explain it.

## Decision
1. **The endpoint is closed first, unconditionally, and only the wait is bounded.** After the close, shutdown waits for the endpoint's connections to finish closing and draining for at most `QUIC_ENDPOINT_CLOSE_GRACE`, 2 seconds, then goes on to the node's storage close.
2. **The grace is an administrative shutdown policy**, a named constant, not a value derived from any network and not a setting. In the observations a reachable peer needed 0.1 s.
3. **Running out of grace is logged as a warning and changes no result.** The handle returns what its server task returned: an earlier failure is still returned, and success is still only the server task's. Nothing treats the end of the grace as success or as failure.
4. **It does not mark the store unclean.** Whether a peer has been told of a close is not a durable-storage obligation. A clean record still depends, unchanged, on every lifecycle permit, the bounded quiescence wait and the storage close succeeding; none of them is inferred from this wait.
5. Unchanged: the order of shutdown steps, every other bound, the TCP+TLS server's shutdown, the watchdog.

## What the bound guarantees
- Exactly this: the helper returns within the grace. It does not release other references to the endpoint, and a connection still closing is left to the endpoint's own timers or to process exit.
- **Local work is not judged by it.** A server task that completes normally does so after its own quiescence wait; a server task that ended with an error proves nothing about accepted work, and its error is returned unchanged. The node's bounded quiescence wait precedes the transport shutdown. This ADR adds no inference from the grace expiring.
- A peer that is alive but slower than the grace may learn of the close from its own timers, not from a received close frame.

## Acceptance
- **The regression comparison is the real-process one.** On the tree without this change, ADR 0721's three-daemon smoke failed at the last node's stop in two executions (`reviews/pd-daemon-20261112/smoke-run2/` uninstrumented, `smoke-run3-diag/` instrumented and naming the wait), both sealed. On the composed tree with this change the same smoke, uninstrumented and unchanged, must pass: the last node exits 0.
- Real QUIC, in process, a client reaching a server through a datagram relay: with the peer reachable, the close finishes inside the grace and the helper reports that it did.
- The server handle returns its server task's result through the close: an earlier error is preserved and a success stays a success; afterwards a connection attempt with a valid client configuration is refused as the endpoint stopping (and the same attempt is accepted on an open endpoint).
- One checkpoint gate on the composed candidate, shared with ADR 0721. **Its outcome is the next section: it failed.**

## Evidence that did not hold
- A deterministic in-process construction of the old wait was attempted once and **failed to reproduce it**: an established connection whose peer had been silent for three seconds (relay cut, 100 ms keep-alives) finished closing in 92 ms, so the bounded helper had nothing to cut short. That test was removed, with its failure recorded here; the relay is kept only for the reachable-peer control.
- That one construction not reproducing the wait does not show established connections could not have been involved in the daemon observation, and it establishes nothing about connections that had not finished a handshake or about other holders of the endpoint. **Which connection state kept the daemon's wait pending is unexplained.** What is located is the wait itself.

## Checkpoint: FAILED, and what followed
- **The one shared checkpoint for ADR 0722 and ADR 0721 FAILED, 4554 of 4556**, on the composed candidate before a last correction (`reviews/pd-daemon-20261112/adr0722-0721-gate1.log`, sha256 d156256a…):
  - `adr0093_quic_config_construction_is_confined_to_the_tls_choke_point`: a real rule violation in ADR 0722's unit test, which built a Quinn client configuration outside `network/tls.rs`. Corrected: the existing client constructor in `network/tls.rs` was extracted, unchanged and in the same order, into `quic_client_config_from_rustls`, which `client_config` now calls and the test uses. On the delivered code the rule's own test and the unit test pass (architecture_test 206 of 206 and the handle-result test).
  - `authority_encoded_live_heap_and_process_resident_assay`: DC-167, an open parallel-only candidate (900 bytes of a process-wide live-heap counter not returned; the same two numbers as on 2026-11-05, when it measured 0 of 500 quiet and 8 of 1,500 under load on that day's tree). **Unresolved and unattributed.** Neither ADR touches that test or the code it measures; that does not exonerate them.
- **The uninstrumented three-daemon smoke passed once**, on the candidate before that behaviour-preserving extraction (`reviews/pd-daemon-20261112/smoke-run4-accept/`, seal 33035333…). **It was not rerun on the delivered tip. No passing full checkpoint exists for the delivered tip.**
- **This exact candidate is delivered under a recorded, one-off known-failure exception** (Codex 01a1d1f6). It is not a general policy, not an exoneration, not a "flake" label and not a green gate.
- Earlier failures stay recorded: smoke executions 2 and 3 (the wait this ADR bounds), and the removed in-process construction above.

## Limits
- No test below the process level distinguishes the old behaviour from the new: the unit and relay tests check the helper's contract and the handle's result, not the long wait.
- DC-227 is closed by this ADR only for the wait it names. Not addressed: `handles.shutdown()` and the server task joins, which remain unbounded awaits that completed immediately in every observed shutdown; the cost of the close toward a peer that is merely slow; Today-mode process tests.
- Not addressed: member replacement, backup and restore, DC-223, DC-224, DC-225, DC-226, section 8 (FAILED, unwaived).
