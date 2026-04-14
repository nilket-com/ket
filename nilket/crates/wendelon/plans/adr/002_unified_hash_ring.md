# ADR 002: The Unified Hash Ring Topology

## Status
Accepted

## Context
Wendelon operates with two distinct logical roles: the Control Ring (Raft consensus) and the Data Ring (bulk storage and routing). Initially, these were conceived as separate "planes," which implies distinct topological structures and addressing mechanisms. This creates friction for small-scale deployments (e.g., single-node developer instances or 3-node QA clusters) where nodes must dynamically assume both roles.

## Decision
We will map every node, regardless of its role, into a **Single Unified Hash Space**. Every node receives a unique `NodeId` that places it on one global consistent hash ring. Routing to specific roles involves filtering this unified ring for nodes with the required capability (`control` or `data`).

## Rationale
- **Simplicity of Scale:** A unified ring allows the same mathematical routing and discovery logic to run a massive, dedicated production cluster (where nodes have exclusive roles) and a tiny, converged dev cluster (where nodes have both roles).
- **Uniform Addressing:** A node is always addressed by its position on the ring, making cluster observability and topological reasoning mathematically consistent.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Distinct Planes:** We reject maintaining separate data structures for the Control Ring and Data Ring.
- **Routing Overhead:** Routing now requires an extra filtering step (e.g., "find the next 3 nodes on the ring *that have the data capability*"). We accept this minuscule CPU overhead in exchange for topological simplicity.