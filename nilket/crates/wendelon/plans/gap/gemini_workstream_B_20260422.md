# Wendelon Workstream B: Toron Compute Tier & Ecosystem Integration (WS8)

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026
**Reference:** Mapping to WS8 in `plans/gap/execution_20260422.md`

## 1. Architectural Stance & Opinion

Wendelon is an incredibly fast, highly engineered storage island. But enterprises do not buy storage islands; they buy Data Warehouses. The compute tier (Toron) is the actual product surface that users, BI tools, and data engineers will interact with.

If Wendelon's Arrow-over-QUIC APIs, dual-CA identity model, and OTLP traces do not map seamlessly to Toron's SQL gateway, JDBC drivers, and SSO integration, the entire project fails at the pilot stage. This workstream is dedicated to ensuring that the boundary between Wendelon (Storage) and Toron (Compute) is formally contracted, rigidly tested, and fully observable. We must treat Toron as our most important, most demanding customer.

## 2. Scope & Deliverables

This workstream ensures that Wendelon is a good citizen in the broader enterprise data ecosystem before Gate 4 (Pilot Authorization).

### 2.1 The Compute/Storage Operational Contract
*   **The Mandate:** We must author and enforce `ops/toron_integration.md`. This is a binding contract between the Wendelon and Toron engineering teams. It dictates how Toron translates a SQL `SELECT` into Wendelon Migration Reads, how Toron handles Wendelon's transport backpressure, and how Toron caches metadata.
*   **Identity Mapping:** Toron handles enterprise SSO/SAML. Wendelon uses mTLS OIDs. We must build and document the exact translation layer where Toron exchanges a SAML assertion for a short-lived Wendelon mTLS certificate representing the executing user.
*   **Exit Criteria:** An end-to-end CI test suite that boots Toron + Wendelon, authenticates via a mock IdP, executes distributed SQL aggregations, and verifies the correct data is returned over Arrow IPC.

### 2.2 Trace & Audit Stitching
*   **The Mandate:** When a user runs a slow query in Looker, the DBA needs to trace it from the Toron SQL parser all the way down to the Wendelon `io_uring` slab read. 
*   **Implementation:** Toron must inject W3C Trace Context into the QUIC headers of every Wendelon RPC. Wendelon must attach these trace IDs to its own OTLP spans and, crucially, to the newly added `data.read` and `data.write` audit events (coordinating with WS6).
*   **Exit Criteria:** A single Datadog/Jaeger dashboard showing a unified waterfall trace crossing the Toron/Wendelon boundary, correlated with the tamper-evident audit log.

### 2.3 ELT / Ingest Ecosystem Patterns
*   **The Mandate:** Data must get into Wendelon efficiently. Forcing customers to write custom Rust QUIC clients to load data is unacceptable. We must provide standard ingestion pathways.
*   **Implementation:** We will develop and publish reference architectures for Fivetran (destination connector), Airbyte, and a dbt adapter targeting the Toron/Wendelon stack.
*   **Exit Criteria:** At least two working, documented ELT pipelines demonstrated loading TPC-H datasets into Wendelon prior to the regulated pilot.

## 3. Milestones & Gating

*   **Phase 1 (Correctness Locked):** Cross-team API freeze between Toron and Wendelon. Trace context headers implemented in the QUIC transport.
*   **Phase 2 (Scale & Survive):** Toron is able to successfully respond to Wendelon's QUIC admission control backpressure without crashing the SQL client.
*   **Phase 3 (Procurement Ready):** Identity mapping (SAML to mTLS) is implemented and passes security review.
*   **Phase 4 (Pilot Authorization):** Full Toron+Wendelon end-to-end integration tests are green. ELT reference pipelines are published.

## 4. Open Questions

1.  **Predicate Pushdown Security:** As Toron pushes compute (predicates, aggregations) down into Wendelon (via WASM or a compute AST), how do we sandbox that execution? Does the compute run under the tenant's exact mTLS identity, and does Wendelon need its own query-planning admission control?
2.  **Quota Visibility:** Wendelon tracks tenant storage and IOPS quotas. How does Toron surface these quotas to the end-user synchronously during a SQL query, especially if a query is about to be killed by Wendelon for quota violation?
3.  **Schema Evolution:** Who owns the schema registry? If Toron executes an `ALTER TABLE`, how is that serialized into Wendelon's Raft state machine safely without blocking ongoing distributed writes? (Ties into the placeholder `SchemaUpdate` RPC that WS1/WS6 needs to freeze).