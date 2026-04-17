# Codex Workstream A: Production Path Bring-Up And Integration Gate

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Make the stock daemon a real, testable cluster process and establish the real-network integration gate that validates later work.

This workstream exists because the current repository has meaningful distributed-systems internals, but the shipped runtime path still does not exercise them as a production node. Until this is fixed, other workstreams risk validating library paths instead of the actual product path.

## In Scope

- Public config/runtime wiring for production-relevant fields.
  - TLS material loading.
  - L2 config loading, even if full L2 closure lands later.
- `main.rs` runtime assembly.
  - construct real `quinn::Endpoint` instances
  - call `serve_quic()`
  - support public `cluster.join` startup
  - install minimal tracing/logging for bring-up
- Replace single-node-only public startup behavior with real distributed startup.
- Real-network integration harness.
  - multi-node TLS cluster formation
  - join
  - write/read
  - restart behavior
- Standard verification integration.
  - preferably CI-driven

## Out Of Scope For This Pass

- Full security-floor closure.
- Full durability lifecycle closure.
- Full tiered-storage correctness closure.
- Large-scale performance tuning.

## Primary Deliverables

- Public daemon startup path can boot a real QUIC/TLS node from config.
- Public join path is supported through standard startup configuration.
- A reusable integration smoke test exists for the real transport path.
- The smoke test is part of normal verification rather than ad hoc manual testing.

## Key Interfaces And Dependencies

- Upstream dependency: none. This is the program entry workstream.
- Downstream consumers:
  - Workstream B uses this path to validate rapid fixes.
  - Workstreams C through G depend on this path to prove their runtime effects.
- Likely touched areas in future execution:
  - `src/main.rs`
  - `src/config.rs`
  - `src/node.rs`
  - transport/bootstrap test harness under `tests/`

## First-Pass Sequencing

1. Extend config loading to stop dropping required runtime fields.
2. Wire daemon startup to create real transport endpoints and serve them.
3. Enable public join-mode startup on the stock path.
4. Add minimal tracing/logging needed for bring-up debugging.
5. Build a small real-network smoke test.
6. Put the smoke test into standard verification.

## Validation Shape

- Three-node TLS cluster forms from config.
- A node can join through the public runtime path.
- Write and read succeed over the real transport surface.
- Restart of at least one node is covered by the gate.

## Exit Criteria

- The stock daemon forms a three-node TLS cluster from config.
- The real-network integration gate exercises join, write, read, and restart.
- Later workstreams can rely on this gate as the baseline runtime validator.

## Open Questions

- Should the first real-network gate use separate daemon processes or in-process nodes with real QUIC endpoints?
- Should L2 config loading land here if the gate initially runs without L2 enabled, or should that wait for Workstream E?
- What is the minimum tracing/logging install needed to debug bring-up without prematurely designing the full observability stack?
