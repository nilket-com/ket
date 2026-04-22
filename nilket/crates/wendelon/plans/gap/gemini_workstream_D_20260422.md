# Wendelon Workstream D: Storage and Stellarator Correctness (WS1)

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026
**Reference:** Mapping to WS1 in `plans/gap/execution_20260422.md`

## 1. Architectural Stance & Opinion

Throughput means nothing if we strand quorum-durable data. The Stellarator bifurcated write model is Wendelon's crown jewel for write performance, but its current implementation contains a fatal flaw: if local pending-intent persistence fails, it degrades to a "best-effort" recovery state. In a 99.999% system, "best effort" is a synonym for "data loss."

My stance is absolute: **Correctness precedes latency, and explicit state is better than implicit logs.** We must refactor the Stellarator write path to use a strictly defined state machine. Local and remote durability must be genuinely concurrent, and if we cannot guarantee deterministic recovery of a write, we must fail it immediately.

## 2. Scope & Deliverables

### 2.1 The Explicit Write-Outcome State Machine
*   **The Mandate:** Adopt an explicit Enum for write outcomes (`DataBranchPending`, `DurableQuorumReached`, `FinalizationCommitted`, `FinalizationRecoverablePending`, `FinalizationTerminalFailure`). Every write must transition cleanly through these states, emitting traces and metrics at each step.
*   **Exit Criteria:** No write operation can return a generic or ambiguous error; every failure maps directly to a defined terminal state with documented client retry semantics.

### 2.2 Mandatory Intent Persistence & Fencing
*   **The Mandate:** Eliminate the warning-only fallback for pending-intent persistence. If a node cannot durably record its intent to finalize a write, it cannot safely participate in quorum.
*   **Implementation:** If `pending_finalizations.upsert()` fails, the node must immediately return `FinalizationTerminalFailure` and, depending on the error severity, fence itself from leader duties.
*   **Exit Criteria:** A fault-injection test fails the local disk during a write; the system correctly rejects the write without leaving orphaned quorum data.

### 2.3 True Concurrent Durability
*   **The Mandate:** The current code serializes the local durable append before spawning remote replica fan-out tasks. This defeats the purpose of the bifurcated model.
*   **Implementation:** Refactor the write pipeline to spawn remote replication tasks *concurrently* with the local `io_uring` submission.
*   **Exit Criteria:** Write latency is bound strictly by the slowest required quorum node, not the sum of local plus remote latencies.

## 3. Milestones & Gating

*   **Phase 0 (Foundation):** The write-outcome enum is defined and merged.
*   **Phase 1 (Correctness Locked):** Mandatory intent persistence and true concurrency are merged.
*   **Phase 2 (Scale & Survive):** Idempotency caches and restart replay logic are fully aligned with the new state machine.

## 4. Open Questions

1.  **Leader Fencing Thresholds:** If the local disk fails an `upsert`, should the node immediately step down as Raft leader, or just fail that specific write? (Recommendation: If the disk is failing local writes, it's highly likely to fail WAL appends soon. Immediate step-down is the safest path).
2.  **Client Retry Semantics:** Should `FinalizationRecoverablePending` be exposed directly to Toron (the compute tier) so it can choose to wait, or should we normalize it into a standard HTTP 503 / Retry-After response?
3.  **Orphaned Remote Data:** If a concurrent write fails locally and we abort, how aggressively do we garbage collect the data we just successfully fanned out to remote replicas? Does this rely entirely on the background sweeper?