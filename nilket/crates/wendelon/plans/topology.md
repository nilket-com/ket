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

## 4. Addressing and Routing

- The `node_id` (typically a hash) places the node uniformly on the unified ring.
- Routing to the Control Ring involves filtering the unified ring for nodes with the `control` capability.
- Routing to the Data Ring involves hashing the object key and walking the unified ring to find the next available node(s) with the `data` capability.
