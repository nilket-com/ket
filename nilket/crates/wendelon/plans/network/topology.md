# Moved from plans/topology.md

(This file was copied into plans/network/topology.md during a planned refactor.)

# Wendelon Topology: The Unified Ring

This document outlines the unified ring topology: every node has a NodeId and occupies a position in a single consistent hash ring. The ring unifies addressing for control and data responsibilities.

- Unified hash space simplifies dev/QA and converged topologies.
- Control Ring nodes are a subset of the unified ring filtered by capability.
- Data Ring uses ring(E) snapshots for deterministic routing.

