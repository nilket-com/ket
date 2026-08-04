# ADR 001: Boundary of Autonomous Self-Healing vs. Infrastructure Provisioning

## Status
Accepted

## Context
At 3 AM, a hardware failure (e.g., motherboard death, catastrophic network partition) causes a Data Node to silently disappear from the cluster. If Wendelon requires a human operator or an external Kafka-driven orchestration queue to manually reassign hash ranges, the cluster will run degraded (or partitioned) until morning, violating our strict SLOs. 

We considered making Wendelon a "Level 2" orchestrator—meaning the Raft Leader would hold cloud credentials (AWS IAM) to actively provision new EC2/Spot instances to replace dead nodes.

## Decision
Wendelon will implement **Level 1 Autonomous Eviction and Mathematical Healing**. 

The Control Ring (Raft Leader) will monitor node heartbeats. If a node fails to heartbeat for a configurable threshold (e.g., 5 minutes), the Leader will autonomously propose a `CONFIG` epoch transition (`E` -> `E+1`) that evicts the dead node. 

Wendelon **WILL NOT** implement cloud provisioning logic (API calls to AWS/GCP/K8s) to physically replace the hardware.

## Rationale
- **Mathematical Safety:** Shrinking the consistent hash ring instantly reassigns the dead node's ranges to surviving nodes. Reads and writes immediately route to healthy replicas. The cluster stays 100% available without human intervention.
- **Separation of Concerns:** Provisioning hardware is the domain of infrastructure (e.g., Kubernetes StatefulSets or AWS Auto Scaling Groups). Embedding cloud SDKs into a high-performance database violates the "Dumb but Fast" mandate and creates a massive, irrelevant maintenance burden.
- **Synergy:** When the infrastructure layer eventually spins up a replacement node hours later, Wendelon's existing "Lazy Migration" protocol (`epoch_transitions.md`) will naturally and safely absorb it back into the ring.

## Consequences (The "Negatives" / Rejected Ideas)
- **The "Not My Job" Risk:** We explicitly reject the idea of Wendelon doing nothing and waiting for an external orchestrator to fix the ring. This forces Wendelon to bear the complexity of autonomous Raft configuration changes.
- **Temporary Degradation:** Evicting a node mathematically heals the routing, but it temporarily reduces the replication factor of the affected ranges until a new physical node joins the cluster. We accept this temporary drop in durability in exchange for immediate availability.
- **Cloud-Native Rejection:** We explicitly reject building EC2/K8s API integrations into Wendelon. If an operator wants "Level 2" auto-replacement, they must wire Wendelon's observability metrics (Prometheus) to an external autoscaler. We will not maintain cloud SDKs inside the database binary.