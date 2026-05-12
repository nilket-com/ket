# Implementation Plan: ADR 001 — Toron Foundation and Engine Contract

Author: AI Swarm Council (Claude / Codex / Gemini)
Companion ADR: `plans/adr/001_toron_foundation_and_engine_contract.md`

## 1. Goal

Scaffold the Toron compute ring as a greenfield, Arrow-native distributed engine, avoiding DataFusion, using `toron-ir` as the engine contract, and defining a disciplined M0-M6 implementation path.

## 2. Implementation Contract

- **Greenfield Engine:** Toron is a specialized, Arrow-native compute engine.
- **One Crate:** V0 starts with `crates/toron` containing internal modules (`ir`, `planner`, `exec`, `storage`, `pbc`).
- **Engine Contract:** `toron-ir` is the durable contract.
- **Wendelon Dependency:** Live Wendelon adapters are deferred until M3; M1 executes against an in-memory `StorageScan`.
- **PBC Pushdown:** Expression lowering to Wendelon PBC is isolated in M2.
- **Determinism:** M4 proves morsel determinism in one process before M5 introduces the QUIC network boundary.

## 3. Phase Plan

### M0: Monorepo Scaffold (Completed)
- **Scope:** Flatten Wendelon's nested workspace, configure root `Cargo.toml`, initialize the `toron` crate structure without pulling premature dependencies (e.g. `tokio`).
- **Tests:** Minimal `assert_eq!(1 + 1, 2)` smoke test.
- **Verification:** `cargo check --workspace` and `cargo test -p toron`.

### M1: Single-Process IR Execution (Completed)
- **Scope:** Implement V0 IR structs, type-check V0 expression catalog, implement Arrow vectorized interpreter, execute `Scan -> Filter -> Aggregate` over in-memory Arrow batches.
- **Tests:** Assert Arrow output and null semantics over `RecordBatch` fixtures.

### M2: PBC Lowering Contract
- **Scope:** Map V0 expressions to Wendelon PBC opcodes, produce deterministic payloads.
- **Tests:** Golden tests for PBC lowering, decline tests for unsupported shapes, parity tests against in-memory execution.

### M3: Wendelon Client Adapter
- **Scope:** Implement `WendelonStorageScan` wrapping `wendelon-client`, pass tenant id and PBC predicate, receive Arrow batches.
- **Tests:** In-process Wendelon fixtures, live local cluster integration.

### M4: Single-Process Morsels
- **Scope:** Introduce `MorselId`, `StageId`, split scans into deterministic morsels, merge `CountAll` results.
- **Tests:** Single-thread vs multi-thread determinism, idempotent morsel retry.

### M5: Coordinator / Worker Boundary
- **Scope:** Split process, define native messages over QUIC, stream incremental Arrow batches.
- **Tests:** Crash-only hard gate (kill worker mid-query, verify idempotent redispatch).

### M6: First Shuffle
- **Scope:** Add `GroupBy` and `Exchange`, implement hash partition exchange over QUIC with backpressure.
- **Tests:** Distributed output determinism, receive-side backpressure limits.

## 4. Risks and Mitigations

- **Risk:** Premature abstraction of `wendelon-types` before concrete pressure.
  - **Mitigation:** M2 targets Wendelon's existing `schema::pbc` API directly. We only extract a shared protocol crate when M2 / M3 proves it painful.
- **Risk:** Network determinism bugs conflated with executor bugs.
  - **Mitigation:** M4 isolates single-process morsel determinism before M5 introduces the QUIC boundary.
- **Risk:** Bloated initial wire protocol.
  - **Mitigation:** M5 streams incremental Arrow batches directly instead of buffering full query results at the coordinator.

## 5. Definition of Done

This plan governs the entire M0-M6 arc. M0 is done when the workspace correctly compiles Toron and Wendelon cleanly. Subsequent milestones require separate `feat:` commits verifying their specific gates.
