# Workstream B: Rapid Correctness Fixes
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Land small, high-leverage fixes that close obvious correctness, resource, and security holes quickly before undertaking larger structural work.

## Scope & Tasks

### 1. Rogue-Node Admission Fix
*   **Target Files:** `src/network/control_server.rs`, `src/network/server.rs`, `tests/`
*   **Tasks:**
    *   Modify the `JOIN_REQ` handler to reject the payload's `node_id` and strictly use the authenticated `remote_id` extracted from the peer's mTLS certificate SAN.
    *   Write a negative test simulating a malicious node attempting to join as a different `node_id` than its certificate specifies.

### 2. Tier Observer Wiring (Disk Leak Fix)
*   **Target Files:** `src/node.rs`
*   **Tasks:**
    *   Attach `ManagerTierObserver` in the `WendelonStateMachine` bootstrap.
    *   Ensure followers correctly `unlink` L1 slab files after the Raft commit for `TierSlab` succeeds, closing the silent disk leak.

### 3. Dev-Secret Hygiene
*   **Target Files:** `src/config.rs`
*   **Tasks:**
    *   Add explicit warnings and sharper isolation for deterministic dev signing material in operator-facing flows, particularly in `render_example_config`.

## Exit Criteria
*   Rogue node joins are demonstrably rejected by test.
*   Followers correctly delete L1 files upon successful tiering.
*   Clear warnings are generated for dev secrets.

---
## Open Questions
*   Are there any other silent disk leaks (e.g., temporary files from mid-upload crashes) that should be bundled into this rapid fix phase, or should those remain in Workstream E?