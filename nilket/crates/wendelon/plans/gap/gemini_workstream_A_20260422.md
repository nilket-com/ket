# Wendelon Workstream A: Closed-Loop Healing, DR, and Operator Tooling (WS7)

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026
**Reference:** Mapping to WS7 in `plans/gap/execution_20260422.md`

## 1. Architectural Stance & Opinion

We have to stop pretending that "mathematical node eviction" is a complete high-availability story. Evicting a dead node from the Raft quorum keeps the cluster *alive*, but it leaves the data **degraded**. If a second node fails while an operator is asleep, we lose data. 

For a 99.999% availability claim to survive a Fortune 500 procurement review, Wendelon must provide **Level 2 Autonomous Healing**—the physical, synchronous replacement of dead infrastructure. We can no longer push this responsibility entirely onto the customer's generic autoscaling groups. 

Similarly, Disaster Recovery (DR) in an enterprise distributed database is not "tar up the WAL and copy it to S3." DR requires cluster-wide consistent cuts, choreography for remote-tier validation, and rigorously tested RTO/RPO metrics.

## 2. Scope & Deliverables

This workstream is responsible for closing the operational safety gap before Gate 4 (Pilot Authorization).

### 2.1 First-Party Kubernetes Operator (`wendelon-operator`)
*   **The Mandate:** We will ship a native K8s Operator. When the Wendelon Control Ring evicts a node, the Operator must catch the metric/event, immediately provision a replacement pod, coordinate with the cert-rotation service (WS5) to issue an identity, and join the new node to the ring.
*   **Fallback Sidecar:** For non-K8s bare-metal/EC2 deployments, we will ship a sidecar daemon that executes customer-provided shell hooks to achieve the exact same lifecycle loop.
*   **Exit Criteria:** A chaos test kills a node; the cluster evicts it, provisions a new one, and returns to full replication factor without human intervention within 5 minutes.

### 2.2 Cluster-Level Consistent Backup & PITR
*   **The Mandate:** Single-node snapshots are insufficient. We must implement a Raft-coordinated snapshot command that halts garbage collection, records the exact log index across all nodes, stabilizes L2 (S3) pointers, and writes a unified cluster manifest.
*   **Encryption:** The backup manifest and artifacts must be encrypted via a dedicated Backup DEK, wrapped by the Root KEK (coordinating with WS4).
*   **Exit Criteria:** A verified, cross-region restore drill that successfully stands up a duplicate cluster from a backup manifest, with zero data loss for committed writes.

### 2.3 The "No Tribal Knowledge" Runbook Library
*   **The Mandate:** Code is only as good as its operating manual. We will author markdown runbooks for every single paging alert we define in the observability stack. 
*   **Topics:** `RaftNoLeader`, `PendingFinalizationBacklog`, `AuditSinkFailing`, `ClusterRestore`, `ManualNodeReplacement` (for when automation fails).
*   **Exit Criteria:** The ultimate Gate 4 validation: an engineering team that has never worked on Wendelon must successfully tear down and restore a cluster using *only* these runbooks.

## 3. Milestones & Gating

*   **Phase 1 (Correctness Locked):** Draft architecture for the K8s Operator and consistent-cut backup protocol.
*   **Phase 2 (Scale & Survive):** Operator MVP ships. Backup protocol implemented in core Raft loop.
*   **Phase 3 (Procurement Ready):** Operator is stress-tested in chaos CI. DR drill successfully executed by the core team. Runbooks drafted.
*   **Phase 4 (Pilot Authorization):** The "Runbook-Only" teardown and restore drill passes with an external team.

## 4. Open Questions

1.  **Backup Storage Provider:** Do we mandate S3/Azure Blob for the cluster backup manifest, or do we attempt to write the backup directly to the existing L2 tier? (Recommendation: Keep backups logically and physically separate from the L2 data tier to prevent a single bucket corruption from destroying both live data and backups).
2.  **Operator Scope:** Does the `wendelon-operator` also manage Toron compute nodes, or is it strictly bounded to the Wendelon storage tier?
3.  **Cross-Region Active Replication:** Will our DR strategy eventually require asynchronous active replication to a hot standby cluster, or is restoring from cold object-storage backups sufficient for our 4-hour RTO target?