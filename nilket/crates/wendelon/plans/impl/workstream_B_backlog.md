# Workstream B PR Backlog

Date: 2026-04-22
Author Role: Lead Architect
Parent Plan: `plans/impl/workstream_B.md`
Primary ADR: `plans/adr/003_stellarator_bifurcated_writes.md`
Execution Pack: `plans/impl/pr_B1_execution.md`

## Purpose

Turn Workstream B into reviewable pull requests that close the Stellarator correctness gap without mixing too many semantic changes into one diff.

This backlog is intentionally code-facing. Each item names:

- merge order
- primary files
- what is allowed in the PR
- what must not be bundled into the PR
- required tests

## Backlog Rules

1. Do not combine write-semantics changes with `io_uring` topology changes. That belongs to Workstream C.
2. Keep public wire-contract changes out of the early PRs unless there is no other path.
3. Every PR that changes write semantics must land tests in the same series.
4. Outcome-state names from ADR `003` are the source of truth. Do not invent parallel status vocabularies.
5. Metrics needed to prove the new behavior may land in the same PR, but broad observability rollout stays with Workstream D.

## Recommended Merge Order

1. `B1` Explicit write-outcome state model
2. `B2` Mandatory pending-intent durability boundary
3. `B3` Concurrent durability legs
4. `B4` Strict quorum aggregation
5. `B5` Replay and idempotency unification
6. `B6` Leader degrade or fence policy

The first PR should be `B1`. It creates the shared vocabulary the later PRs need.

## PR B1: Explicit Write-Outcome State Model

### Goal

Introduce one internal state model for bifurcated writes so later correctness fixes can target named states instead of inferred control flow.

### Primary Files

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- a new internal module if needed for the enum and transition helpers

### Allowed Changes

- add the explicit outcome enum from ADR `003`
- thread the enum through internal write-path helpers
- add a transition table or equivalent invariant comments/tests
- keep the existing external response shape if possible

### Do Not Bundle

- concurrent local and remote execution changes
- leader fencing
- large retry or replay rewrites

### Required Tests

- unit tests for legal and illegal state transitions
- adapt existing write-path tests in `src/network/data_server.rs` so they assert explicit states where possible
- keep existing regression coverage green:
  - `duplicate_write_replays_finalization_marker_and_emits_one_audit_record`
  - `pending_finalization_replays_after_restart_when_control_branch_failed`
  - `write_counts_only_durable_quorum_acks_across_replica_crash_boundary`

### Merge Criteria

- one explicit state model exists in code
- write-path branches are no longer described only by comments or booleans
- no external protocol churn is introduced without clear need

### Main Review Risk

This PR can sprawl into a rewrite. Keep it structural and semantic, not behavioral beyond the new state plumbing.

## PR B2: Mandatory Pending-Intent Durability Boundary

### Goal

Remove the best-effort branch that currently allows recoverability claims without durable pending-finalization intent.

### Primary Files

- `src/network/pending_finalizations.rs`
- `src/network/data_server.rs`
- `src/metrics/registry.rs` if a narrow failure metric is added

### Allowed Changes

- make `PendingFinalizationStore::upsert()` failure terminate the recoverable branch
- return a structured retriable error instead of continuing optimistically
- record enough local context for operator diagnosis

### Do Not Bundle

- concurrency refactors
- quorum aggregation changes
- leader step-down behavior beyond what is strictly required for this failure path

### Required Tests

- injected or fault-seam test for `upsert()` failure
- restart test proving no false recoverability is claimed when the intent was never durably stored
- regression on:
  - `pending_finalization_replays_after_restart_when_control_branch_failed`

### Merge Criteria

- no success-like path remains after pending-intent durability fails
- the code cannot claim recoverable-pending semantics unless the intent is durably stored

### Main Review Risk

It is easy to leave a secondary fallback or logging-only branch alive. Review should specifically search for every `upsert()` failure path.

## PR B3: Concurrent Durability Legs

### Goal

Start the leader-local durable append and remote replica fan-out concurrently when the leader is in the replica set.

### Primary Files

- `src/network/data_server.rs`
- `src/storage/manager.rs`
- any small helper surface needed for durable-append futures

### Allowed Changes

- split the current serial flow into concurrent local and remote legs
- keep the new logic expressed in terms of the `B1` state model
- preserve existing audit and finalization hooks

### Do Not Bundle

- executor topology changes
- post-auth admission control work
- full replay overhaul

### Required Tests

- latency or ordering test proving local append and remote fan-out can overlap
- mixed local-success and remote-timeout tests
- durable quorum tests still passing under the new concurrency model

### Merge Criteria

- the steady-state write path no longer serializes local durable append before remote fan-out
- concurrent execution is visible in code structure and test behavior

### Main Review Risk

This PR can accidentally blur write semantics and runtime scheduling. Keep the executor model unchanged; only make the two logical legs concurrent.

## PR B4: Strict Quorum Aggregation

### Goal

Make quorum computation explicit and reject pure-remote quorum when the leader is itself a replica-set member.

### Primary Files

- `src/network/data_server.rs`
- tests in `src/network/data_server.rs`

### Allowed Changes

- centralize durable-quorum counting
- require the leader-local durable leg when the leader is a replica
- preserve the append-only replica boundary already covered by tests

### Do Not Bundle

- replay unification
- leader fencing logic
- broad metrics work

### Required Tests

- strengthen or extend:
  - `write_counts_only_durable_quorum_acks_across_replica_crash_boundary`
- add explicit cases where remote quorum exists but local durable append does not

### Merge Criteria

- quorum logic is explicit and reviewable
- pure-remote quorum cannot silently succeed when the leader stores data

### Main Review Risk

Reviewers need to verify that every quorum-success path flows through the same aggregator rather than one helper being fixed while another old path survives.

## PR B5: Replay And Idempotency Unification

### Goal

Make in-process retry, duplicate requests, and restart replay converge on the same validation rules and terminal-state handling.

### Primary Files

- `src/network/data_server.rs`
- `src/network/pending_finalizations.rs`
- `src/client/core.rs` only if a narrow retry interpretation needs to be aligned

### Allowed Changes

- route retry and replay checks through the explicit outcome-state model
- centralize duplicate-write handling
- remove divergent terminal-state interpretation between live and restarted paths

### Do Not Bundle

- leader fencing
- new transport retry policy
- client-facing API redesign

### Required Tests

- duplicate retry during recoverable-pending state
- restart replay closing pending work to a legal terminal state
- no duplicate finalization or duplicate audit materialization on replay
- regression on:
  - `duplicate_write_replays_finalization_marker_and_emits_one_audit_record`
  - `pending_finalization_replays_after_restart_when_control_branch_failed`

### Merge Criteria

- retries and replays share the same state validation logic
- the same write cannot land in different terminal meanings solely because the process restarted

### Main Review Risk

Retry and replay code often hides in separate helper paths. Review should look for duplicated outcome interpretation after the refactor.

## PR B6: Leader Degrade Or Fence Policy

### Goal

Define and implement what the leader does after integrity-threatening local persistence failures.

### Primary Files

- `src/network/data_server.rs`
- `src/node.rs` if degraded status is surfaced there
- the smallest consensus/control integration surface needed to step down or fence leadership

### Allowed Changes

- detect local persistence distrust conditions
- fail active writes cleanly
- mark degraded state and, if supported by the current control surface, step down or fence leadership

### Do Not Bundle

- broad operator automation
- generic health-framework rewrites
- unrelated consensus improvements

### Required Tests

- injected local I/O failure leading to write failure
- degraded-state visibility test
- step-down or fenced-leader behavior test if that hook lands in the same PR

### Merge Criteria

- integrity-threatening local persistence failure has a deterministic behavior
- the leader does not continue acting healthy after its local durable leg becomes untrustworthy

### Main Review Risk

This PR can grow into an open-ended availability-policy debate. Keep it strictly on the branch where local persistence trust is broken.

## Hand-Off To Workstream D

Workstream D should shadow this backlog with:

- outcome-state counters or gauges if needed
- pending-finalization backlog and replay metrics
- fault-injection reports tied to `B2-B6`

## Recommended First PR

Start with `B1`.

Reason:

- it is the smallest high-value semantic change
- it de-risks every later B PR
- it creates the vocabulary needed to review `B2-B6` cleanly
