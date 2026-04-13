# Wendelon Topology: The Unified Ring

This document outlines the topology of the Wendelon cluster. The fundamental architectural shift from distinct "planes" to a unified hash space topology brings conceptual simplicity while preserving the logical separation of control and data responsibilities.

## 1. Unified Hash Space

In Wendelon, every node, regardless of its role (control or data), is assigned a unique `node_id`. This `node_id` determines the node's position on a single, global consistent hash ring.

By mapping all nodes into the same hash space:
- We create a unified topology view that simplifies cluster management, discovery, and debugging.
- We simplify small-scale deployments (e.g., single-node, 3-node, or 5-node clusters for QA or starter environments). In these environments, nodes can dynamically assume both control and data roles while residing on the same logical ring.
- We maintain mathematical consistency in how nodes are addressed, routed to, and monitored.

## 2. Logical Separation on the Unified Ring

While all nodes exist on the unified ring, their responsibilities are still strictly partitioned into two logical roles:

### The Control Ring (Formerly Control Plane)
- Handles cluster configuration, node membership, and epoch tracking.
- Driven by the Raft consensus protocol.
- Only nodes with the "control" role participate in the Control Ring.
- Even if a node has both roles, its Control Ring operations execute in an isolated thread pool (executor) to prevent I/O starvation.

### The Data Ring (Formerly Data Plane)
- Handles bulk read/write data routing and storage.
- Operates via Consistent Hashing against a fixed, immutable cluster snapshot (`ring(E)`).
- Queries are routed to nodes based on their position in the Data Ring subset of the unified hash space.

## 3. Deployment Flexibility

The unified ring topology enables significant flexibility:
- **Dedicated Topology (Production):** Distinct physical nodes are assigned exclusive "control" or "data" roles. The unified ring simply maps them together for addressing, but they operate entirely independently.
- **Converged Topology (Dev/QA/Starter):** Nodes are assigned both roles. Because they are on a unified ring, routing and discovery logic remains identical, allowing the same codebase to effortlessly run in resource-constrained environments.

## 4. Virtual Nodes (Vnodes)

To ensure an even distribution of data and minimize the impact of a single node failure, Wendelon uses Virtual Nodes (Vnodes).
- **Vnode Count:** By default, each physical Data Node is assigned **256 vnodes**. This can be tuned via cluster configuration, but must be identical across the cluster for fairness.
- **Assignment:** A vnode's position on the ring is deterministically derived: `VnodePosition = Hash(NodeId || VnodeIndex)`.
- **Control Ring Exemption:** Control-only nodes do not use vnodes. They occupy exactly one position on the ring (`Hash(NodeId)`).

## 5. Addressing and Routing (The Ring Walk)

The fundamental routing algorithm for the Data Ring relies on traversing the unified ring to find the correct replica nodes for a given key.

1. **Hash the Key:** `KeyHash = Hash(NamespaceID || RawKey)`.
2. **Binary Search:** The client (or proxying node) performs a binary search on the cached, sorted array of all `ring(E)` positions to find the first position strictly greater than `KeyHash`. This is the **Primary Vnode**.
3. **The Ring Walk:** To satisfy a Replication Factor (RF) of `N`, the router walks clockwise around the ring starting from the Primary Vnode.
4. **Physical Distinctness:** The router collects the underlying `NodeId` of each vnode it visits. It skips vnodes that map to a `NodeId` it has already collected, and skips nodes that lack the `data` capability.
5. **Completion:** The walk stops once exactly `N` distinct physical Data Nodes are collected. The first node collected is the Primary (leaseholder candidate); the remaining `N-1` are Secondary Replicas.
