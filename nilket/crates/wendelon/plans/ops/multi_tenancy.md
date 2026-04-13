# Multi-tenancy & Quotas

Wendelon is designed to back massive platforms (Polariton/Baryon) which may host thousands of disparate organizations.

## 1. Logical Isolation (Namespaces)
- The unified hash ring is partitioned logically by **Namespace** (Tenant).
- `KeyHash` derivation: `Hash(NamespaceID || RawKey)`. This ensures tenant data is globally distributed across the ring, preventing hot spots if one tenant has massive data volume.

## 2. Role-Based Access Control (RBAC)
- Client write/read operations must carry an authorization token (e.g., JWT).
- The token is verified against the Raft-managed RBAC policies to ensure the client has `WRITE` or `READ` access to the target Namespace.

## 3. Resource Quotas
- To prevent noisy-neighbor problems, the Control Ring tracks quota metrics per Namespace.
- **IOPS Quotas:** The WPBC and Data Ring executors apply token-bucket rate limiting based on the `NamespaceID`.
- **Storage Quotas:** The Background Sweeper aggregates `total_bytes` from `SlabManifests` per Namespace. If a tenant exceeds their storage limit, the Data Nodes will return `QUOTA_EXCEEDED` for new writes until space is freed or the quota is raised via the Control Ring.