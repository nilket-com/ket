# Read Flow & Consistency Levels

The Data Ring supports three distinct consistency levels for reads. The client specifies the desired level in the `ReadRequestMeta.consistency` field.

## 1. LINEARIZABLE (Strong Consistency)
Guarantees the client sees the most recent committed write for a key.
**Path:**
1. **Lease Validation:** The receiving node MUST hold a valid Per-Range Lease for the key's hash range. If not, it rejects with `LEASE_EXPIRED` (or proxies to the current leaseholder).
2. **Causality Check:** If the client provided a `min_hlc`, the node waits until its local `E_safe` watermark >= `min_hlc`.
3. **Index Lookup:** The node queries the local Storage Engine index.
4. **Migration Fallback:** If the index misses AND the node recently acquired this range in an Epoch Transition (`E-1` -> `E`), the node fires an asynchronous "Migration Read" proxy request to the old owner from Epoch `E-1`.
5. **Return:** The value is returned to the client and (if a migration read) asynchronously appended to the local storage.

## 2. SERIALIZABLE (Leader-Stale Consistency)
Guarantees the read is served from the authoritative primary, but does not strictly guarantee it is the *absolute latest* write (e.g., a newer write might be in-flight but not yet finalized).
**Path:**
1. **Lease Validation:** The receiving node checks if it is the primary, but does *not* strictly require the lease to be active at this exact microsecond.
2. **Index Lookup:** Immediately queries the local Storage Engine and returns the data. No waiting for `E_safe` boundaries.
3. **Use Case:** High-throughput analytics where seeing data from 50ms ago is perfectly acceptable, but reading from a completely un-synchronized replica is not.

## 3. STALE (Eventual Consistency)
Read from any available replica holding the data.
**Path:**
1. The receiving node (which may just be a secondary replica in the hash ring) skips all lease checks.
2. It queries its local Storage Engine index.
3. If the data exists, it returns it immediately.
4. **Use Case:** Massively parallel map-reduce jobs where total data availability outweighs strict freshness.