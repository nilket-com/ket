# Brainstorm 012: System Visibility and Introspection (Information Schema)

## The Premise

Wendelon is designed for "First-Person Shooter" performance, but as the complexity of the transport (QUIC vs. RDMA/UCX) and the warehouse (L1/L2 tiering, multi-row compression) grows, the system must not remain a "black box." Operators and automated control planes need a standard, high-performance way to query the internal state of the cluster.

This brainstorm proposes a **Virtual System Schema** (similar to `information_schema` or `sys` in traditional databases) that exposes Wendelon's internals as read-only Arrow tables.

## The "Sovereign Query" for System State

In Wendelon, queries are executed via **PBC (Pushdown Bytecode)** and returned as **Arrow IPC**. We should use this exact same mechanism for system introspection. 

A query like `SELECT active_protocol, rtt_micros FROM system.transports WHERE peer_node_id = 'n-02'` should be handled by the same execution engine as a data query, but instead of scanning an L1 slab, the "source" is a memory-materialized Arrow batch from the node's internal state.

## Proposed Virtual Tables

### 1. `system.nodes`
Topology and health visibility.
- `node_id`: (n-01, n-02, etc.)
- `role`: (LEADER, FOLLOWER, LEARNER)
- `status`: (ONLINE, DEGRADED, DRAINING)
- `protocol_version`: (u16)
- `hardware_profile`: (String describing the detected environment)

### 2. `system.transports`
Deep visibility into the network fabric (the UCX/RDMA story).
- `peer_node_id`: Target node.
- `active_protocol`: (QUIC, iWARP, RoCEv2, IB, mlx5/UCX).
- `transport_family`: (TCP/UDP/InfiniBand).
- `rtt_micros`: Current measured round-trip time.
- `encryption`: (AEAD, TLS, None/Trusted-Fabric).
- `zero_copy_enabled`: (Boolean).

### 3. `system.leases`
Real-time view of the data-ring fencing (ADR 003).
- `tenant_id`: (UUID)
- `range_start`, `range_end`: (u64)
- `holder_node_id`: Current primary for this range.
- `expiry_hlc`: When the lease expires.

### 4. `system.storage_slabs`
Visibility into the L1/L2 "Warehouse" state.
- `tenant_id`, `table_id`, `slab_id`.
- `tier`: (L1-NVMe, L2-S3, Migrating).
- `uncompressed_bytes`, `compressed_bytes`: Monitoring ADR 043 effectiveness.
- `codec`: (LZ4, None).
- `tombstone_count`: Visibility into compaction backlog.

### 5. `system.audit_events` (ADR 025)
Standardized access to security and data events.
- `timestamp_hlc`: (HybridTimestamp)
- `actor_identity`: (Identity OID)
- `event_type`: (data.read, auth.failure, etc.)
- `status`: (OK, DENIED, ERROR)

### 6. `information_schema.tables` & `columns`
Metadata discovery for Toron and other clients.
- Standard relational metadata derived from `TableSchemaDescriptor`.

## Implementation Strategy: The "System Provider"

Instead of creating a new RPC family for "management," we extend the `READ_REQ` handler:

1.  **Routing:** If the `TableId` or `Schema` names a virtual system table, the request is routed to a `SystemTableProvider`.
2.  **Materialization:** The provider gathers state from the relevant manager (e.g., `TransportManager`, `AuditManager`).
3.  **Arrow Conversion:** The state is packed into an Arrow `RecordBatch`. 
4.  **Bytecode Execution:** The `PBC` program provided by the user is executed **against the materialized system batch**.
5.  **Return:** The filtered/aggregated results are returned as standard `READ_RESP` Arrow IPC.

## Why this is FPS-Friendly

*   **Zero-Copy Audit:** Querying the audit log doesn't require a slow "export to CSV" or "JSON parse" loop. It stays in Arrow throughout the entire pipeline.
*   **Metric Convergence:** This replaces many "ad-hoc" metrics with queryable tables. Instead of 1,000 Prometheus counters for every possible transport permutation, we expose a single table that can be sliced by any dimension.
*   **Control Plane Symmetry:** Toron can use the same code path to check "Is Node X healthy?" as it uses to fetch data.

## Open Questions

*   **Global vs. Local:** Does a system query only see the local node's state, or should the leader be able to "fan-out" a system query to the entire cluster? (Likely start with Local-only, then move to "Clustered System Views").
*   **Privilege Level:** How do we restrict `system.audit_events` or `system.transports` to administrative identities while allowing `information_schema` for general users?
*   **Performance Isolation:** How do we ensure a heavy query against a system table doesn't "stutter" the node's primary data path? (Limit the materialization to small, fixed batches).
