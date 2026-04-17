# Workstream H: Multi-Tenancy & Policy Expansion
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Expand from pilot-grade security into broader commercial isolation features, providing strict multi-tenancy and resource segregation. (To be executed after initial pilot blockers are cleared).

## Scope & Tasks

### 1. Multi-Tenancy ADR
*   **Target Files:** `plans/ops/multi_tenancy.md`
*   **Tasks:**
    *   Write an ADR detailing namespace schema, routing rules, quota enforcement, and key isolation boundaries.

### 2. Isolation & Quotas
*   **Target Files:** `src/schema/`, `src/network/router.rs`, `src/storage/manager.rs`
*   **Tasks:**
    *   Implement namespace-prefixed schema and ring routing.
    *   Implement per-tenant lease grants in the Control Ring.
    *   Enforce per-tenant storage capacity and throughput quotas.
    *   Implement per-tenant L2 bucket routing.

### 3. Key Isolation
*   **Target Files:** `src/storage/core.rs`, `src/storage/l2.rs`
*   **Tasks:**
    *   Extend Workstream C encryption to support per-tenant encryption keys wired to the KMS.

## Exit Criteria
*   Multiple tenants on the same cluster cannot read each other's data.
*   Quota breaches return clear limit errors rather than impacting adjacent tenants.
*   Data at rest uses tenant-specific encryption keys.

---
## Open Questions
*   How should we model the performance penalty of managing multiple distinct KMS encryption contexts per I/O batch for different tenants?