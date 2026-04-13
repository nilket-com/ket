# Node Lifecycle: Startup & Shutdown

Wendelon nodes must adhere to a strict boot sequence to avoid split-brain scenarios or returning stale data during crash recovery.

## 1. Startup Sequence

1. **Load Configuration:** Read `wendelon.toml` and evaluate `WENDELON_*` environment variables. Validate mTLS certificates.
2. **Storage Mount & WAL Replay:** 
   - Open the Raft WAL in `data_dir` and rebuild the `openraft` state machine.
   - Scan the `SlabManifest`s to identify all `Active` and `Sealed` slabs on local NVMe.
   - Sequentially scan the `SlabRecordHeader`s in all local slabs to rebuild the in-memory `DashMap`/ART index (`KeyHash -> SlabId, Offset, Length`).
3. **Initialize Network Endpoints:** Bind the `quinn` sockets on `:4001` and `:4002`. Begin listening for incoming QUIC connections, but immediately block/queue incoming Data Ring requests.
4. **Join the Control Ring:** Connect to the Raft cluster (or bootstrap a Genesis Epoch if configuring `E=1`).
5. **Lease Acquisition:** Based on the observed `ring(E)`, calculate the node's assigned vnodes/hash ranges. Issue `ACQUIRE_LEASE_REQ` to the Raft Leader for all assigned ranges.
6. **Open the Floodgates:** Once the baseline leases are acquired, unblock the Data Ring endpoint. The node is now actively routing and serving reads/writes.

## 2. Graceful Shutdown Sequence

Triggered by `SIGTERM` or `SIGINT`.
1. **Stop Accepting Traffic:** The Data Ring endpoint stops accepting new QUIC connections and returns `INTERNAL_ERROR` or `NOT_LEADER` for new requests on existing streams, gracefully draining in-flight requests.
2. **Release Leases:** The node proactively notifies the Raft Leader that it is relinquishing all Per-Range Leases, allowing the Leader to immediately grant them to other nodes without waiting for the timeout.
3. **Raft Step-Down:** If this node is the Raft Leader, it triggers an immediate `TRANSFER_LEADERSHIP_REQ` to a healthy follower.
4. **Flush Storage:** `fsync` all Active slabs. Ensure the `io_uring` CQE queue is completely drained. Flush the `SlabManifest` updates to the WAL.
5. **Exit:** Terminate the async runtime.

## 3. Crash Recovery

If a node is terminated ungracefully (e.g., `SIGKILL` or hardware power loss), the in-memory index is lost and the WAL may contain unapplied entries. 
For detailed recovery invariants and ordering, see `plans/consensus/raft-core.md` section 6 ("Recovery invariants and ordering").