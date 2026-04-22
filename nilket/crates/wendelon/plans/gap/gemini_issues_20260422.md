# Wendelon Unresolved Issues & Friction Points

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026
**Context:** A synthesis of the remaining disagreements and open friction points after cross-reviewing the architectural answers from Claude, Codex, and myself.

---

While the Phase 0 planning process has successfully locked the vast majority of our architectural semantics, a few critical friction points remain unresolved across our respective execution plans. These items require a final, binding Council decision before we exit Phase 0.

### 1. DR Replication Posture for Pilot (Cross-Region Active Replication)
*   **The Conflict:** Claude asserts that a "Cold Backup + Restore" strategy is sufficient to satisfy a 4-hour RTO for the first regulated pilot. Codex and Gemini strongly disagree, asserting that cold restore is too weak for a serious regulated target, and that asynchronous cross-region replication or a warm standby posture is mandatory by the regulated pilot/GA horizon.
*   **The Impact:** If Claude is right, we save months of engineering on replication topologies. If Codex/Gemini are right, launching a regulated pilot with only cold backups risks an immediate procurement rejection when the customer audits our RTO capabilities under duress.

### 2. Break-glass Issuance Exceptions (Dual-Auth)
*   **The Conflict:** Claude asserts that break-glass certificate issuance *always* requires dual-authorization, arguing that issuance is the load-bearing event regardless of the certificate's scope. Codex argues for pragmatism, stating that a narrow, diagnostic-only break-glass certificate may remain single-auth provided it is heavily audited.
*   **The Impact:** A strict dual-auth requirement for *all* break-glass scenarios may unacceptably inflate mean-time-to-resolution (MTTR) during 3 AM diagnostic incidents when a secondary operator is unavailable. However, relaxing it risks a compliance finding if an auditor determines the diagnostic scope is too broad.

### 3. Cert-Rotation Service Consensus Substrate
*   **The Conflict:** Claude mandates reusing `openraft` for the cert-rotation service to avoid inventing a new HA design. Codex warns against the operational cost of managing a second, independent Raft control plane and suggests falling back to a simpler HA design if `openraft` threatens the delivery schedule.
*   **The Impact:** Forcing `openraft` into the cert-service guarantees strong consistency but doubles our consensus-layer operational burden. A simpler HA design (e.g., leader-election via a shared database lock) might be faster to ship but risks split-brain issuance. 

### 4. Autonomous Healing: K8s Operator vs. Fallback Automation
*   **The Conflict:** Codex states that the first-party `wendelon-operator` is not strictly mandatory for Gate 3, and that a tightly specified external automation contract is an acceptable fallback for the first customer-facing pilot. Gemini's original workstream strongly mandated the native K8s Operator for "Level 2 autonomous healing," arguing that relying on external automation invalidates our 99.999% availability claims.
*   **The Impact:** Can we market a "highly available" database in Pilot if the customer is responsible for writing the bash scripts that replace dead nodes? 

### 5. Ecosystem Integration: Maintained ELT Connector vs. Reference Patterns
*   **The Conflict:** Claude advocates for shipping reference patterns *plus* at least one officially maintained connector (e.g., a dbt adapter) for Gate 4 to prove the ingestion pathway. Codex asserts that tested reference patterns are sufficient, and that a maintained connector is merely "desirable."
*   **The Impact:** Engineering owns the reference patterns, but maintaining a dbt/Fivetran connector is a perpetual burden. However, handing a Fortune 500 data engineering team a "reference pattern" instead of a working connector may stall the pilot before data is ever loaded.