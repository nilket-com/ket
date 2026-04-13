# Backup, DR, and Legal Hold

While Wendelon achieves durability via replication and Tiered Storage, enterprise environments require explicit Disaster Recovery mechanisms.

## 1. L2 as the Ultimate Backup
- Because cold data naturally migrates to L2 (Object Storage) via the Background Sweeper, S3/GCS effectively acts as a continuous, native backup.
- **Cross-Region Replication:** Bucket-level replication (e.g., AWS CRR) handles catastrophic regional failures without Wendelon's active involvement.

## 2. Raft WAL Shipping
- The Control Ring's WAL (containing epoch transitions, namespaces, and schema registries) must be periodically shipped to L2.
- In a total cluster wipe, a new cluster can bootstrap by replaying the Raft WAL from L2, recovering the exact `ring(E)` mapping pointing to the data slabs already residing in L2.

## 3. Legal Hold & Immutable Slabs
- For compliance, operators can flag specific namespaces with a "Legal Hold".
- Slabs belonging to these namespaces are marked as `Immutable` in their `SlabManifest`. The Background Sweeper is forbidden from applying `E_safe` compaction (Garbage Collection) to these slabs, ensuring every historical version of a key is retained indefinitely.