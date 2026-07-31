# Brainstorm 003: Disaggregated Compute Roles & Unified Ring Algebra

**Status:** Exploratory / Mission Critical

## The Core Tension: Elasticity vs. Data Locality
Currently, Toron (stateless SQL compute) and Wendelon (stateful consensus/storage) operate as a hyper-converged, symmetric system. Every node is both a data custodian and a compute worker. This provides excellent data locality (pushing predicates and aggregates directly to the disks holding the data) but fundamentally couples the scaling vectors: you cannot scale compute without also scaling durable storage and consensus overhead.

Modern cloud analytics demands elasticity—spinning up compute resources in minutes rather than waiting hours for stateful rebalancing. This brainstorm explores how to introduce specialized compute roles while maintaining the elegance of a single executable and a unified ring topology.

## Vision 1: Hardware-Optimized Asymmetry (NVMe vs. DDR5)
Instead of treating all nodes as identical commodities, we can introduce role-based specialization within the single binary:
*   **Wendelon (Stateful) Nodes:** Hardware profile optimized for NVMe and high IOPS. These nodes hold the L1 slabs, participate in Raft consensus, and execute the "push-down" Toron tasks (scans, local aggregates, local filters).
*   **Toron (Compute-Only) Nodes:** Hardware profile optimized for massive DDR5 RAM and CPU throughput. These nodes hold zero durable state, do not participate in Raft, but are fully capable of executing memory-intensive Toron tasks (global hash joins, large memory-bound sorts, and complex shuffles).

## Vision 2: The "Weak Ring / Strong Ring" Algebra
If both node types share the exact same ring algebra, we can model their participation differently based on their stateful durability:
*   **Strong Ring (Wendelon):** Nodes with full "width" in the ring. They are authoritative custodians of partition hash ranges.
*   **Weak Ring (Toron-Only):** Nodes that occupy the ring but act as "Weak References." They can receive routed compute tasks based on consistent hashing (e.g., for partitioned joins), but they are not authoritative for data durability. If a Weak Ring node drops, the compute fails over instantly without triggering a Raft membership change or data rebalancing.

## Vision 3: The Data Broker / Toron Worker Topology
Similar to Wendelon's L1 (NVMe) / L2 (Object Storage) split, we can apply a tiering concept to Toron execution:
*   A "normal" hyper-converged node receives a query and acts as the **Data Broker** (Coordinator).
*   It realizes the query requires massive parallelism (e.g., an enormous join).
*   It delegates the heavy memory tasks to a fleet of specialized **ToronWorker** nodes.
*   These ToronWorker nodes are managed as an elastic compute tier, analogous to how L2 object storage is managed as an elastic storage tier.

## Required Evolution Paths
To make this real without breaking the current architecture, we need to explore:
1.  **`NodeRole` Expansion:** Introducing `NodeRole::ComputeOnly` or `NodeRole::Stateless` to the `NodeRoleSet` bitmask.
2.  **Ring Routing Updates:** Ensuring `RingRouter` understands how to route durable writes strictly to `NodeRole::Data` nodes, while the `DistributedScheduler` can dispatch tasks to the expanded compute pool.
3.  **Lease & Capability Gossip:** Compute-only nodes still need to gossip their existence and memory capacities via the epoch mechanism so coordinators know they exist.

*Note: This is a foundational brainstorm. Further exploration into the precise wire protocols and scheduler modifications is required.*