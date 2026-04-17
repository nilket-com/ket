# Workstream A: Integration Gate & Production Path Bring-Up
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Make the stock daemon a real, testable cluster process and establish an end-to-end testing gate. A subsystem is not complete until it is exercised through the shipped `main.rs` daemon path using TOML configuration.

## Scope & Tasks

### 1. Public Config & Daemon Wiring
*   **Target Files:** `src/config.rs`, `src/main.rs`, `src/node.rs`
*   **Tasks:**
    *   Wire `config.rs` to load TLS material (`cert_path`, `key_path`, `ca_path`) and L2 configs from TOML, removing hardcoded disabled states (e.g., `l2: None`).
    *   Wire `main.rs` to construct a real `quinn::Endpoint` based on the loaded TLS material.
    *   Call `serve_quic()` in `main.rs` before blocking on quiescence.
    *   Support public `cluster.mode = "join"` startup by removing the `ErrorKind::Unsupported` gate in `node.rs`.
    *   Initialize minimal tracing in `main.rs` for bring-up visibility.

### 2. Integration Smoke Test Gate
*   **Target Files:** `tests/integration_smoke.rs` (new), CI Configuration
*   **Tasks:**
    *   Create a real-network integration harness with 3 nodes using real QUIC endpoints and mTLS.
    *   Exercise JOIN, WRITE, READ, and graceful node restart through the real transport path.
    *   Add this integration test to standard CI verification (must pass on every PR).

## Exit Criteria
*   The stock daemon can form a three-node TLS cluster from a configuration file.
*   The integration test exercises join, write, read, and restart through the real transport path and passes in CI.

---
## Open Questions
*   How should we structure the CI environment to ensure real QUIC endpoints don't conflict on port binding when running parallel test suites?