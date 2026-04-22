# Wendelon Workstream E: Observability, Validation, and SLO Evidence (WS3)

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026
**Reference:** Mapping to WS3 in `plans/gap/execution_20260422.md`

## 1. Architectural Stance & Opinion

An unobservable system is an un-operable system. We have made a massive investment in OTLP and OpenMetrics, but we are missing critical instrumentation on the most complex, failure-prone parts of our hot paths. We cannot commit to 99.999% availability SLAs if we cannot see our `io_uring` queue depths, our pending finalization backlogs, or our lazy migration proxy overhead.

Furthermore, we cannot claim to be a resilient distributed database if our chaos testing consists of three basic smoke scenarios. **Validation must be exhaustive, automated, and adversarial.**

## 2. Scope & Deliverables

### 2.1 Complete Hot-Path Instrumentation
*   **The Mandate:** Add first-class metrics for pending-finalization backlogs, replay attempts/failures, replica fan-out latency, quorum acquisition latency, and `io_uring` SQ/CQ depths.
*   **Trace Stitching:** Ensure W3C trace contexts are propagated across the Toron/Wendelon boundary, and specifically trace the bifurcated write branch splits.
*   **Exit Criteria:** All required metrics are actively emitting in the CI Program Scorecard, with corresponding alerts and dashboards drafted.

### 2.2 Adversarial Validation & Chaos Matrix
*   **The Mandate:** Expand the `tests/chaos.rs` suite from 3 smoke scenarios to a comprehensive 30+ scenario matrix.
*   **Scenarios:** Must include nemesis combinations (e.g., network partition + disk fill, leader flap + cert rotation, topology change + massive analytical proxy reads).
*   **Linearizability:** Implement or integrate a linearizability checker (e.g., Jepsen-style harness) to prove read/write consistency under failure.
*   **Exit Criteria:** The chaos matrix runs nightly. A 1,000-iteration fault-injection soak test passes with zero stranded writes or consistency violations.

### 2.3 SLO Promotion
*   **The Mandate:** We currently have SLOs marked as "TARGET (unvalidated)". We cannot enter a regulated pilot with unvalidated targets.
*   **Exit Criteria:** Execute a multi-week soak test that demonstrably meets all latency and availability targets. Only then do we promote the SLOs to "COMMITTED" in our operator documentation.

## 3. Milestones & Gating

*   **Phase 1 (Correctness Locked):** Chaos matrix expanded to 20+ scenarios. Missing metrics implemented.
*   **Phase 2 (Scale & Survive):** Linearizability checker active. 1,000-iteration soak test passing.
*   **Phase 3 (Procurement Ready):** Multi-week soak complete. SLOs formally promoted to COMMITTED.

## 4. Open Questions

1.  **Cardinality Limits:** As we add more granular metrics (e.g., per-tenant backpressure queue depths), how do we enforce cardinality limits to prevent Wendelon from overwhelming the customer's Prometheus/Datadog infrastructure?
2.  **Linearizability Harness:** Do we invest in writing our own internal linearizability checker integrated tightly with our simulator, or do we bite the bullet and write a standard Clojure Jepsen test suite? (Recommendation: Jepsen has industry credibility. If we want to prove our claims to Fortune 500 architects, a passing Jepsen report is invaluable).
3.  **Trace Sampling:** 100% tracing of all distributed writes will be too expensive. How do we implement intelligent tail-based sampling so we only keep traces of writes that exceeded our P99 latency SLA or failed?