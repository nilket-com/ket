---

Control Ring — unified node ordering

The system models every node (data-only, control-only, or hybrid) as a numeric NodeId that occupies a single unified ring topology — the "control ring". This simplifies small/topological test and demo deployments (1,3,5 nodes) and makes node placement and deterministic routing uniform across roles. Key points:

- NodeId is a cluster-scoped numeric handle used for bytemuck NodeId and for ring ordering. Address resolution remains out-of-band via epoch_entry_var_schema() (voter/learner address lists).
- The control ring and data ring are conceptually the same 1D hash space; placement decisions (shard owners, control responsibilities) are derived from the same ring via virtual node slots when desired.
- The ring mapping (hash algorithm, secret key if applicable, and ring width) is a one-time cluster initialization parameter and is immutable for the life of the cluster. Changing the ring's hashing semantics requires full cluster rebootstrap (re-hash and redistribute all data).
- Cluster metadata (persisted in RaftEpochEntry variable metadata — see epoch_entry_var_schema meta_keys/meta_values) MUST include keys: "hash_algo", "hash_key_hex" (if keyed), and "ring_bits" (64 or 128). Implementations SHOULD refuse operation if these fields are missing or inconsistent.

Hashing Algorithm Selection

Design goals:
- Provide a secure, fast default that resists simple adversarial input while remaining performant for high-throughput internal routing.
- Offer opt-in alternatives for specialized environments (maximum speed for trusted environments, or full cryptographic hashing for public-facing workloads).
- Make the choice immutable at cluster init to avoid subtle rebalancing bugs. The system exposes a one-time initialization API that records (hash_algo, ring_bits, key) into the initial epoch config.

Supported algorithms (user-visible names):

Algorithm | Type | Approx Speed (GB/s) | Security Level | Best Use Case
---|---:|---:|---|---
rapidhash | Non-crypto | ~40–60+ | Statistical (Excellent) | Internal-only test clusters, dev/TUI tooling where speed matters and inputs are trusted
xxh3 | Non-crypto | ~30–50 | Statistical (Good) | General purpose, backwards-compatible fast hashing in trusted contexts
highwayhash | Strong non-crypto (keyed) | ~10–15 | Adversarial-resistant (best-in-class non-crypto) | Default for distributed DB consistent hashing — strong blurring against collision attacks
blake3 | Cryptographic (keyed) | ~2–10+ | Cryptographic (full) | Public APIs, untrusted inputs, or when cryptographic resistance is required

Recommendation and defaults
- Default: keyed HighwayHash (128-bit internal state; by default expose a 64-bit ring value derived from the keyed 128-bit output via XOR-folding). HighwayHash offers strong adversarial resistance at competitive throughput and is an excellent tradeoff for consistent hashing in distributed systems.
- Optional: For maximum throughput in fully trusted environments, Rapidhash or XXH3 may be chosen to reduce CPU pressure. These are NOT recommended for public-facing workloads.
- Optional: For maximal cryptographic guarantees (at higher cost), BLAKE3 keyed mode may be selected with a 128-bit ring width.

Implementation notes
- Ring width: default ring_bits = 64. For blake3 or use-cases requiring huge namespace, ring_bits=128 may be selected. The chosen ring width is stored in cluster metadata and determines how hash outputs are reduced to ring positions.
- Key management: for keyed algorithms (HighwayHash, HighwayHash variants, BLAKE3 keyed), a per-cluster random key is generated at init and stored in the initial RaftEpochEntry metadata (meta_keys/meta_values). The key MUST be present on every node; a node unable to access the key should refuse to join the cluster.
- Versioning: store an algorithm identifier and version in cluster metadata (e.g., "highwayhash-v1"). Future algorithm upgrades must be handled as a cluster-wide rebootstrap.
- Determinism: when deriving ring positions, use canonical byte-ordering of NodeId and any virtual node index. Document the exact canonicalization in client SDKs to ensure identical hashing across language bindings.

Migration and safety
