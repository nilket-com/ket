# ADR 0710: a child write RPC has a deadline

Status: proposed, 2026-11-10 (Claude, Codex). Direction and shape cleared (Codex 01a1c700). Implementation lands with this plan. Closes DC-207 once its implementation and evidence are reviewed. Review record: `reviews/dc195-layout-collision/` (DC-207 entries, `CODEX-DC207.txt`).

## Why
- **DC-207 (availability; fails closed).** `send_child_frame` (`network/data_server/child_writes.rs`) carries a write's rows to a remote node. It has the pattern ADR 0709 removed from `round_trip`: resolving or connecting, opening the stream, writing the metadata and rows, finishing and reading the reply, with no application deadline. Two callers nest:
  - **Coordinator → holder** (`coordinator.rs`, `TABLE_CHILD_WRITE_REQ`). The claim's children are sent one after another, each awaited before the next. One silent holder therefore stalls the write and every later child.
  - **Holder → plan members** (`fan_out_child`, `TABLE_CHILD_REPLICATE_REQ`). A `JoinSet` is drained to the end, so one silent replica holds the holder's reply even when enough acknowledgements have already arrived.
- **What a failed send means today.** For both callers it is "no acknowledgement from that peer". The coordinator collects the acknowledgements it receives, and the reducer judges quorum at Finalize. Finalize answers `PendingEvidence` when acknowledgements do not reach quorum.

## Decision
**One monotonic deadline bounds each remote child RPC attempt, selected explicitly by request kind: `CHILD_REPLICATE_DEADLINE` = 5 s for a holder's replicate request to a plan member, and `CHILD_WRITE_DEADLINE` = 10 s for a coordinator's write request to a holder. The deadline is set before the first asynchronous step and never reset. Expiry is `io::ErrorKind::TimedOut` and counts as no received acknowledgement from that peer, exactly as a transport error does today.**

1. **Selection.**
   - The caller names the budget by request kind. No message type falls back to a default budget.
   - A shorter enclosing deadline keeps its effect.
   - No retries and no detached work: the holder's `JoinSet` owns its sends and drops them when it ends.
2. **Outcome semantics.**
   - A timeout contributes no acknowledgement from that RPC. It never shows that rows are absent, unwritten or rolled back: the peer may have stored the child.
   - Acknowledgements already received from other peers stay available to Finalize.
   - A timeout alone never becomes a terminal `Refused` or `KnownNotCommitted`. Finalize without quorum answers `PendingEvidence` (outcome unknown), and only a recorded terminal result warrants a terminal answer.
   - A retry keeps the parent key and request digest and acts on the recorded claim. A late stored copy follows the existing recovery and cleanup rules.
3. **Cancellation.** Only the request's own stream and future are dropped; the shared connection is not. A dial cancelled before it completes is never cached (as in ADR 0709).
4. **The budgets are design values, not guarantees.**
   - **Why the outer budget is larger.** The holder's work includes receiving and decoding the request, `await_applied` (nominally up to 2 s), local persistence and locking, its own fan-out (bounded by the replicate budget) and the reply. Persistence and locks have no demonstrated hard bound. So the outer budget being larger than the inner one gives headroom, but cannot guarantee that a healthy holder's answer reaches the coordinator in time.
   - **Size.** The receiver refuses child rows over `MAX_CHILD_ROWS_BYTES` (64 MiB). The sender is bounded by rows per child (`MAX_ROWS_PER_CHILD`), not by bytes. A maximum-size child within the 5 s replicate budget needs at least 12.8 MiB/s of payload throughput before any other latency. This is not a supported maximum-size performance claim.
   - **No whole-write deadline.** Children remain sequential, so a write's worst case is the sum of its per-child waits (`MAX_CHILDREN` = 64). A local holder bypasses the outer RPC, and its local write can still wait. No client-latency claim follows.
5. **Out of scope:** sending a coordinator's children concurrently, and returning at quorum from the holder's fan-out. Both change task and permit ownership and how acknowledgements are collected.

## Acceptance (finite)
1. **A held remote holder.**
   - Setup: a handshake proves the coordinator's child write reached the holder, which never answers.
   - The coordinator returns a typed, bounded result that is not "known not committed".
   - After the release, a retry of the same parent reaches its single authoritative outcome, with exact rows on every node.
   - Fails on main (deterministic RED).
2. **A held replica.**
   - With other valid acknowledgements sufficient for quorum (the holder's included), the write commits after the replica's deadline from the evidence collected. This distinguishes bounding the fan-out from discarding the other peers' acknowledgements.
   - With insufficient evidence, the answer is `PendingEvidence` (outcome unknown), never a terminal refusal. Existing refusal controls are reused where they genuinely cover this.
3. **An ordinary response control**, plus a helper-level check that the deadline covers connection and transfer. Timer origins are correct: no assertion that the full budget must elapse after arrival.
4. **The focused controls pass**, followed by source review and one checkpoint at the tip. No load campaign.

## Not established
- Bounds for the holder's local persistence and locks, and for a local holder's write.
- Whole-write latency, concurrency between children, and early quorum return.
- The table write's ignored client deadline, the local Raft waits (ADR 0709), DC-201, §8, and the other registered limits.
