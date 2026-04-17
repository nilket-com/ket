# Codex Workstream F: Resource Control, Fault Injection, And Scale Hardening

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Prevent predictable overload failures and begin proving system behavior under crash, restart, and partition fault models.

This workstream combines two concerns that became clearer in the teammate planning pass:
- immediate stability requires bounded resource usage
- credibility requires deterministic fault-injection coverage on the real runtime path

## In Scope

- Bound unbounded in-memory state.
  - idempotency and similar registries/caches
- Overload signaling and admission control.
- Memory budgets for large write/read/tiering flows.
- Reduce whole-buffer handling where it is a major stability risk.
  - cold L2 reads
  - snapshot movement
  - large query/materialization paths
- Deterministic fault-injection coverage.
  - partition tests
  - restart/crash tests
  - tier-pivot fault coverage
- Soak and restart/load validation for the real runtime path.

## Out Of Scope For This Pass

- Pure throughput optimization not tied to correctness or stability.
- Long-tail hardware tuning such as NUMA/CPU affinity unless required by discovered bottlenecks.
- Full benchmark program beyond what is needed to validate bounded behavior.

## Primary Deliverables

- The cluster degrades predictably under pressure instead of trending toward OOM or uncontrolled growth.
- Core correctness properties are tested under deterministic faults, not just clean runs.
- Large-object handling becomes more bounded and less dependent on whole-buffer assumptions.

## Key Interfaces And Dependencies

- Depends on Workstream A because the fault model must cover the real runtime path.
- Depends on Workstreams D and E to make durability and tiering behavior real enough to stress meaningfully.
- Builds on Workstream B for the first obvious resource-bound wins.
- Feeds Workstream G by making operational overload surfaces explicit.

## First-Pass Sequencing

1. Catalog current unbounded state and classify by immediate vs broader-fix candidates.
2. Land first-pass overload signaling and admission boundaries.
3. Reduce the most dangerous whole-buffer flows.
4. Stand up deterministic crash/partition harnesses.
5. Add soak/load validation around the real runtime path.

## Validation Shape

- Negative tests for overload/admission behavior.
- Deterministic partition and restart tests for core invariants.
- Soak-style validation showing no routine runaway memory growth.
- Restart/load tests validating bounded behavior rather than peak performance.

## Exit Criteria

- Sustained runtime no longer depends on known unbounded growth paths.
- Deterministic crash/partition validation exists for the highest-value cluster invariants.
- Large-object handling is bounded enough to avoid obvious stability cliffs on the pilot path.

## Open Questions

- Should the first deterministic fault harness use an existing simulator approach, a process-level chaos harness, or both in stages?
- Which whole-buffer paths are pilot blockers versus performance follow-ups?
- How broad should the first admission-control layer be before it starts interfering with useful bring-up and debugging work?
