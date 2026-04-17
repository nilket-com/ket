# Workstream C: Security Floor
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Establish the minimum security and compliance posture required for a serious enterprise pilot deployment, specifically focusing on data at rest and privileged access.

## Scope & Tasks

### 1. Encryption at Rest (ADR & Implementation)
*   **Target Files:** `plans/security/001_encryption_at_rest.md` (new), `src/storage/core.rs`, `src/consensus/raft_storage.rs`, `src/storage/l2.rs`
*   **Tasks:**
    *   Write an Architecture Decision Record (ADR) for an envelope encryption scheme and external KMS integration.
    *   Implement encryption for L1 slabs, WAL pages, snapshots, and L2 objects using AES-GCM or the chosen algorithm.
    *   Inject encryption context appropriately (e.g., into the `io_uring` append path).

### 2. Control Plane Authorization
*   **Target Files:** `src/network/control_server.rs`, `src/network/tls.rs`
*   **Tasks:**
    *   Distinguish operator authority from data-node authority based on certificate claims (OID extensions or distinct CAs).
    *   Enforce these boundaries on Control Ring RPCs (e.g., `advance_epoch`, `transfer_leadership`).

### 3. Audit & Key Lifecycle
*   **Target Files:** `src/network/control_server.rs`, `src/network/tls.rs`, `src/network/lease_signing.rs`
*   **Tasks:**
    *   Add an append-only audit stream (e.g., NDJSON) for privileged admin actions (lease grants, membership changes).
    *   Introduce basic key and certificate rotation support (e.g., file watcher or SIGHUP reload) without requiring full cluster restarts.
    *   Add key-id fields to `LeaseDescriptor` for Ed25519 overlap windows.

## Exit Criteria
*   All data at rest (WAL, L1, L2, snapshots) is encrypted with KMS integration.
*   Operator actions are isolated via RBAC and logged to a structured audit stream.
*   Certificates can be reloaded dynamically.

---
## Open Questions
*   Which encryption algorithm should be standardized (AES-256-GCM vs XChaCha20-Poly1305)?
*   What are the performance implications of encryption on the `io_uring` + `O_DIRECT` alignment?
*   Should the authorization model use OID extensions in X.509 certs, a separate operator CA, or token-based auth for Control Ring RPCs?