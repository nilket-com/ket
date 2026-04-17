# Wendelon Gap Analysis Report

Date: 2026-04-17

Repository reviewed:
- `src/consensus/*`
- `src/storage/*`
- `src/network/*`
- `src/node.rs`
- `src/metrics/*`
- `tests/*`
- `plans/*` documents relevant to recovery, tiering, upgrades, observability, and security

Target question:
How far is `wendelon` from being a commercially viable, enterprise-ready distributed storage layer for mission-critical data?

Short answer:
It is still a prototype. Several core mechanisms are real and promising, especially around WAL durability, quorum write semantics, and basic Raft membership handling, but the shipped daemon is not yet a fully wired distributed node, L2 tiering is not production-integrated, snapshot/compaction durability is unfinished, and the security/compliance story is materially below Fortune 500 requirements.

## 1. Durability and Data Integrity

Assessment:
Crash recovery is materially better than a toy system, but it is not yet complete enough for long-lived enterprise operation. The WAL path has real durability barriers and corruption handling. The steady-state lifecycle around snapshots, purging, compaction, and background integrity verification is still incomplete.

What is implemented well:

- Raft WAL appends are checksummed and fsynced before completion. `append()` serializes each entry, waits for every write CQE, then issues an `fsync` before notifying openraft (`src/consensus/raft_storage.rs:569-675`).
- Vote persistence is also durable: `save_vote()` writes the artifact and fsyncs it (`src/consensus/raft_storage.rs:516-563`).
- Recovery semantics are sensible for the WAL:
  - load vote state and optional snapshot (`src/consensus/raft_storage.rs:279-336`, `1405-1460`)
  - truncate torn tail at EOF (`src/consensus/raft_storage.rs:1489-1697`)
  - fail loudly on interior corruption (`src/consensus/raft_storage.rs:1699-1709`, `2094-2121`)
- Recovery tests cover the important first-wave cases:
  - normal restart (`src/consensus/raft_storage.rs:2040-2063`)
  - torn tail truncation (`src/consensus/raft_storage.rs:2065-2091`)
  - interior corruption hard-fail (`src/consensus/raft_storage.rs:2093-2121`)
  - checkpoint replay (`src/consensus/raft_storage.rs:2123-2150`)
  - snapshot-plus-tail recovery (`src/consensus/raft_storage.rs:2152-2181`)
- The data-path durability boundary is real on the local storage side. `append_durable()` appends, fsyncs the slab, and only then publishes into the manager index (`src/storage/manager.rs:263-282`). The write path uses that durable append (`src/network/data_server.rs:2321-2339`).
- Slab records carry per-record checksums, and slab reopen walks the file and verifies them while rebuilding the in-memory index (`src/storage/core.rs:230-306`, `467-539`).

Critical gaps:

- Snapshot persistence is unfinished. The code can load `snapshot.wnd` on recovery (`src/consensus/raft_storage.rs:1405-1460`), but the live snapshot builder only stores bytes in memory (`src/consensus/raft_storage.rs:1309-1335`) and `install_snapshot()` only updates in-memory state (`src/consensus/raft_storage.rs:1008-1025`). I found no live path that writes `snapshot.wnd`. That means the snapshot lifecycle exists on paper and in tests, not as a production steady-state durability mechanism.
- Checkpoint parsing exists, but checkpoint emission does not. Recovery understands `REC_CHECKPOINT` (`src/consensus/raft_storage.rs:90`, `1589-1632`, `1808-1822`), but I found no production path that writes those records. Without live checkpoints, restart-time clamping of commit / `E_safe` depends on tests rather than runtime behavior.
- Raft log truncation and purging are not durable. `truncate()` and `purge()` only mutate the in-memory cache and explicitly defer WAL rewrites/compaction (`src/consensus/raft_storage.rs:677-708`). In practice that means WAL growth is unbounded and log compaction is not durably realized on disk.
- Bit-rot detection is opportunistic, not operational. There are checksums on WAL records, slab records, and tiered slab payloads, but I found no background scrubber, no periodic offline verifier, and no repair workflow. Enterprise storage needs scheduled integrity scans and operator-visible repair actions, not just detection during replay or read.
- Local slab recovery treats a bad checksum as “stop here” (`src/storage/core.rs:517-523`). That is appropriate for a torn trailing write, but it is not a full interior-corruption strategy with block quarantine, explicit bad-sector reporting, or automatic replica repair.
- Startup time scales with on-disk data. `SlabManager::open()` scans every slab file and `recover_slab()` walks each file record-by-record to rebuild indexes (`src/storage/manager.rs:324-387`, `src/storage/core.rs:467-539`). That will become a serious recovery-time issue once nodes hold large local histories.

Direct answers:

- How robust is crash recovery?
  - Good for WAL torn tails and interior corruption detection.
  - Incomplete for steady-state clustered operation because snapshots, checkpoints, and durable log compaction are not finished.
- Is corruption possible during sudden power loss or kernel panic?
  - The WAL and durable append path are reasonably protected.
  - Data/metadata divergence is still possible during slab compaction and tiering transitions because those flows are not transactionally journaled end-to-end.
- Are checksums, bit-rot detection, and background scrubbing implemented?
  - Checksums: yes.
  - Bit-rot detection: only opportunistically on replay/read.
  - Background scrubbing: no implementation found.
- How does the system handle split-brain and partitions?
  - Control-plane quorum behavior is delegated to openraft, and the minority-eviction guard is sensible. `minority_watchdog_cannot_force_local_only_eviction` passes (`tests/membership_advancement.rs:214-286`).
  - There is still no external fencing, no explicit quorum-loss recovery workflow, and no operator-grade partition diagnostics.

## 2. Object Storage Tiering (S3/GCS)

Assessment:
This is the least production-ready major subsystem. The core building blocks exist, but the feature is not actually integrated into the shipped node path, the crash semantics are not transactionally safe, and cold-tier lifecycle management is incomplete.

What is implemented well:

- The OpenDAL-backed S3-compatible client is straightforward and clean (`src/storage/l2.rs:196-247`).
- The sweeper verifies object integrity by round-tripping the uploaded blob and comparing hashes before it commits the pivot (`src/storage/sweeper.rs:263-323`).
- Rehydration writes to a temp file, fsyncs, and renames before opening the local copy (`src/storage/manager.rs:1252-1307`).
- There is test coverage for remote put/get/delete, retry on transient put failure, tier metadata persistence, and rehydration (`tests/l2_s3.rs:352-426`, `tests/l2_rehydration.rs:121-182`).

Critical gaps:

- The stock daemon does not wire L2 tiering into production bootstrap.
  - `WendelonNode::bootstrap_with_provider()` builds the slab manager with an optional `l2`, but it only spawns the migration sweeper, not the storage tiering sweeper (`src/node.rs:427-435`, `638-642`).
  - In repo search, `SlabSweeper::new()` appears only in storage-focused tests, not in production bootstrap.
  - `src/config.rs` hardcodes `l2: None` when building `NodeBootstrapConfig` from `wendelon.toml` (`src/config.rs:219-233`), so the stock daemon cannot even be configured for S3/GCS.
- The object-store interface is too minimal for scale. It is intentionally only `put/get/delete`; range GETs and multipart uploads are explicitly deferred (`src/storage/l2.rs:10-16`, `55-80`). That conflicts with the operations plan, which already assumes multipart upload (`plans/ops/storage_tiering.md:50-60`).
- The sweeper policy in code is much simpler than the documented architecture. The code tiers every sealed slab above `min_slab_bytes` (`src/storage/sweeper.rs:203-229`). The plan describes capacity- and temperature-driven tiering/promotion (`plans/ops/storage_tiering.md:44-60`).
- Tiering is not transactionally safe across L1 and L2.
  - The upload happens before the Raft `TierSlab` commit (`src/storage/sweeper.rs:269-310`).
  - If the process crashes after `put()` succeeds but before the Raft commit lands, the object is orphaned in S3.
  - There is no manifest of “uploaded but not yet committed” objects and no orphan-GC mechanism.
- Followers can pay the full upload cost and still fail the commit. `RaftCommitter` simply wraps `raft.client_write(...)` (`src/storage/sweeper.rs:151-165`), but `tier_one_slab()` uploads before it calls that committer (`src/storage/sweeper.rs:272-310`). On a follower, that means wasted uploads and potential object leaks.
- Normal L2 object deletion is missing. The only normal call to `object_store.delete()` in production code is best-effort cleanup on a round-trip hash mismatch (`src/storage/sweeper.rs:283-288`). I found no lifecycle path that deletes cold objects after remote compaction, retention expiry, or migration cleanup.
- Tombstone compaction only reclaims sealed local slabs. `compact_tombstoned_sealed_slabs()` iterates only `state.sealed` (`src/storage/manager.rs:968-999`). The operations plan says old slabs on S3 should also be reclaimable (`plans/ops/storage_tiering.md:58-60`), but that is not implemented.
- Crash safety during local compaction is weak. The replacement slab is created and fsynced, then in-memory maps are updated, then the old file is deleted (`src/storage/manager.rs:1062-1149`, `1174-1196`). If the process dies in the middle, restart rescans both on-disk slab files (`src/storage/manager.rs:324-387`, `src/storage/core.rs:467-539`), which can re-surface stale data or duplicate versions.
- Tiered-slab reconciliation is incomplete on followers and restart.
  - `apply_tier_slab()` inserts the slab into `state.tiered` before it unlinks the local file (`src/storage/manager.rs:634-655`).
  - If unlink fails, the function returns error after the state mutation, and subsequent retries short-circuit on `state.tiered.contains_key(...)` (`src/storage/manager.rs:638-653`).
  - In production bootstrap, I found no attachment of `ManagerTierObserver` to the recovered state machine; the node just uses `log_store.recovered_state_machine()` as-is (`src/node.rs:437-441`, `522-556`). That means follower-side local cleanup and restart-time reconciliation are not fully wired.
- L2 reads are whole-object reads. `read_from_l2()` downloads the full slab blob, validates the entire hash, and then extracts one record (`src/storage/manager.rs:1199-1232`, `1333-1349`). Large slabs will create avoidable memory and network spikes.

Direct answers:

- Is migration from hot local storage to object storage transactionally safe?
  - No. The local and remote sides can diverge because upload, Raft commit, and local unlink are not wrapped in one transactional protocol.
- What happens if the system crashes midway through an S3 upload or local compaction?
  - Mid-upload: retry may succeed later, but partial remote state is not durably tracked.
  - After successful upload but before commit: orphan object leak.
  - Mid-compaction: both old and replacement slabs can exist on restart, and the manager rebuilds state from whatever files are still present.
- Is there risk of orphaned objects in S3 or local disk leaks?
  - Yes on both fronts.
  - S3 orphan risk is real because there is no pending-upload journal or remote GC.
  - Local leak risk is real because tier cleanup and compaction are not crash-atomic.

## 3. High Availability and Operations

Assessment:
The control-plane library code is ahead of the daemon wiring. Membership change, protocol negotiation, and graceful drain mechanisms exist, but the shipped binary is not yet a real multi-node service process.

What is implemented well:

- Membership changes follow a reasonable sequence: `add_learner`, then `change_membership`, then visible epoch advancement (`src/network/control_server.rs:937-1003`).
- Visible epoch advancement is tied to migration seeding and reconciliation (`src/network/control_server.rs:534-631`).
- Rolling-protocol negotiation exists and is tested (`src/network/wire.rs:104-144`, `tests/upgrade_compatibility.rs:148-190`).
- Graceful drain hooks exist at the listener and request lifecycle layer (`src/network/server.rs:83-201`, `src/node.rs:675-689`, `700-710`).
- The minority partition eviction guard is a real strength. The code refuses self-destructive quorum loss and the test coverage is good (`tests/membership_advancement.rs:214-286`).

Critical gaps:

- The stock daemon is not a true distributed node yet.
  - `main()` loads config and calls `WendelonNode::bootstrap(cfg)` but never constructs a `quinn::Endpoint`, never builds TLS configs, and never calls `serve_quic()` (`src/main.rs:54-97`).
  - `bootstrap()` explicitly rejects `ClusterBootstrapConfig::Join` unless the caller uses the lower-level `bootstrap_with_provider()` (`src/node.rs:379-386`).
  - The default provider is `NoDialTransportProvider`, which fails every outbound connection attempt (`src/node.rs:996-1017`).
  - Result: the shipped binary is not a real cluster process; it is effectively a single-node bootstrap harness unless an external caller wires networking manually.
- openraft configuration is almost entirely defaulted. Only `cluster_name` is set before validation (`src/node.rs:477-490`). For an enterprise deployment I would expect explicit election, heartbeat, snapshot, compaction, and storage-retention tuning with clear operational defaults.
- Dynamic replication configuration is not implemented beyond global RF and membership-driven migrations.
  - RF is a single bootstrap-wide integer (`src/node.rs:105-107`).
  - I found no capacity-aware placement, hot-range rebalancing, or policy-driven replica redistribution independent of node add/remove events.
- Rolling upgrades are only partially operationalized.
  - Version-window negotiation exists, but real-network mixed-version testing does not.
  - I found no runtime feature gating, no staged activation of new on-disk formats, and no rollback guard beyond protocol overlap checks.
- Backup/restore is only a node-local baseline. The code can export control artifacts and local slabs and restore into a fresh directory, but there is no cluster-wide consistent backup orchestration, no restore fencing workflow, and no cross-version restore plan suitable for production DR.
- Observability is far below enterprise SRE expectations.
  - The metrics registry contains only counters and gauges, not latency histograms or dimensional labels (`src/metrics/registry.rs:40-75`, `76-255`).
  - The ops plan explicitly calls for OpenTelemetry spans and latency histograms (`plans/ops/observability.md:5-16`), but I found no OTLP/OpenTelemetry implementation.
  - `main()` never installs a tracing subscriber, so the many `tracing::warn!` / `tracing::info!` calls in the codebase are not meaningfully surfaced by the stock daemon (`src/main.rs:24-97`).
  - The metrics exporter is a tiny plaintext HTTP server with no auth or TLS (`src/metrics/exporter.rs:15-73`).

Direct answers:

- How does the system handle node additions, removals, and rolling upgrades?
  - Library-level: reasonably, with openraft membership changes and protocol negotiation.
  - Product-level: incompletely, because the daemon runtime is not fully wired for real QUIC/TLS cluster operation.
- Is there dynamic replication configuration and rebalancing?
  - Only in a limited sense. Membership changes seed migrations, but there is no policy engine for capacity, hot-range balancing, or dynamic RF.
- Are observability facilities sufficient for enterprise operations?
  - No. Metrics are basic, distributed tracing is absent, and stock logging wiring is effectively absent.

## 4. Performance and Concurrency

Assessment:
There are some sensible low-level choices here, but the current implementation will not scale predictably under large slabs, high write rates, or large cold-tier footprints. Memory is not globally budgeted, and several critical paths still operate on full-object buffers.

Strengths:

- The `io_uring` submission queue is bounded to `2 * depth`, which prevents that specific queue from growing without limit (`src/storage/io_uring.rs:202-213`).
- The transport connection factory does not hold its `Mutex<HashMap<...>>` across `await`, so it avoids one common async lock bug (`src/network/factory.rs:31-41`, `90-166`).
- Query/migration page responses have explicit record and byte caps (`src/network/data_server.rs:340-349`, `353-360`).

Critical gaps:

- `io_uring` is single-threaded per spawned ring and has an acknowledged idle-to-burst scheduling limitation while blocked in `submit_and_wait()` (`src/storage/io_uring.rs:223-233`). That is acceptable for early bring-up, not yet for a high-throughput storage fabric with multiple NVMe devices or NUMA-sensitive workloads.
- The write path serializes the full Arrow `RecordBatch` into one `Vec<u8>` before local durable append and remote fan-out (`src/network/data_server.rs:2289-2294`). Under heavy write load, that becomes a real allocator and memcpy hotspot.
- The idempotency cache is unbounded in practice.
  - The schema contract says entries expire after `idempotency_ttl_ms` or piggy-backed acknowledgement (`src/schema/core.rs:50-57`).
  - The implementation just probes and inserts into a `DashMap` with no eviction path (`src/network/data_server.rs:2233-2239`, `2413-2427`).
  - That is a memory leak under sustained client churn.
- L2 hot-path memory behavior is poor:
  - tiering uploads the full slab into memory and downloads it back for verification (`src/storage/sweeper.rs:269-282`)
  - L2 reads download the full slab to fetch one record (`src/storage/manager.rs:1202-1232`)
  - rehydration keeps a second in-memory copy while writing the local file (`src/storage/manager.rs:1252-1307`)
- There is no global memory budget, no tenant quota system, and no general backpressure layer. The server file explicitly calls out “Backpressure / per-tenant quotas” as not implemented (`src/network/server.rs:48-53`).
- Restart/recovery cost grows with data volume because slabs are rescanned to rebuild indexes (`src/storage/manager.rs:324-387`, `src/storage/core.rs:467-539`).

Direct answers:

- Are there obvious bottlenecks or lock-contention risks?
  - Yes. Full-batch serialization, whole-slab L2 transfers, slab-scan recovery, and the single `io_uring` thread are the biggest immediate bottlenecks.
  - The `DashMap` and `RwLock` usage is not obviously incorrect, but it has not been paired with memory bounds or high-cardinality eviction strategies.
- Is memory allocation bounded?
  - Partially in a few local places.
  - System-wide: no. The current design is still vulnerable to OOM or severe allocator pressure under large slabs and heavy write/cold-read traffic.

## 5. Security and Compliance

Assessment:
The in-flight crypto primitives are a decent start. The enterprise security posture as a whole is not ready. The biggest issues are missing encryption at rest, missing production TLS/runtime wiring, and an admission-control flaw in the join path.

What is implemented well:

- mTLS config builders exist for both server and client (`src/network/tls.rs:117-153`).
- Inbound QUIC peer identity is bound to the validated certificate SAN. The server extracts `remote_id` from the peer cert before constructing the connection object (`src/network/server.rs:372-384`, `src/network/tls.rs:173-248`).
- There is targeted SAN-parsing test coverage in `tests/tls_identity.rs:118-165`.
- Lease and transfer control paths use signed descriptors/tokens rather than trusting plain metadata alone (`src/network/control_server.rs:797-853` and related signing modules).

Critical gaps:

- There is no encryption at rest for slabs, WAL, snapshots, or object storage.
  - The local slab append path writes raw frame bytes into the slab file (`src/storage/core.rs:249-272`).
  - The WAL snapshot builder and install path operate on plaintext encoded state (`src/consensus/raft_storage.rs:1309-1335`, `1008-1025`).
  - `RemoteObjectStoreConfig` has endpoint and credential fields, but no SSE-KMS, client-side envelope encryption, KEK/DEK metadata, or KMS integration (`src/storage/l2.rs:99-119`, `204-247`).
  - This directly contradicts the security plan, which requires envelope encryption and external KMS integration (`plans/network/security.md:9-16`).
- The shipped daemon does not wire the TLS transport into runtime operation. `main()` never constructs `TlsMaterial`, never creates `quinn::Endpoint`, and never starts `serve_quic()` (`src/main.rs:54-97`, `src/node.rs:672-689`).
- Rogue-node admission is not properly bound to authenticated identity.
  - `handle_control_frame()` receives the authenticated `remote: NodeId` (`src/network/control_server.rs:1075-1089`).
  - `JOIN_REQ` handling ignores that authenticated identity and trusts `req.node_id` from the payload (`src/network/control_server.rs:445-470`, `906-937`).
  - `accept_join()` then adds that claimed node id into Raft membership and the visible epoch (`src/network/control_server.rs:937-1003`).
  - A node with any CA-trusted certificate can therefore request admission for an arbitrary `node_id` / address pair. Even if later dial-back fails, that is still a serious admission-control and membership-poisoning bug.
- I found no certificate hot reload, revocation checking, or rotation orchestration. `tls.rs` explicitly lists hot cert reload as not here yet (`src/network/tls.rs:36-38`).
- I found no authorization/RBAC layer for client-facing data access, no audit-log pipeline, and no compliance-grade key management hooks.
- The metrics endpoint is unauthenticated plaintext HTTP (`src/metrics/exporter.rs:15-73`).

Direct answers:

- Are intra-cluster communications properly authenticated to prevent rogue nodes from joining?
  - Partially.
  - The transport authenticates certificate chains and the inbound server extracts a SAN-bound node identity.
  - The join control path then fails to bind the claimed `req.node_id` to that authenticated transport identity. That is a real security gap and a P0 fix.
- Is data encrypted at rest locally and in S3?
  - No.

## 6. Verdict and Roadmap

### Overall Readiness Score

28 / 100

Why this low:

- The repository contains meaningful real systems work, not just stubs.
- But several enterprise-critical surfaces are still either library-only, test-only, or operationally incomplete:
  - the stock daemon is not actually a fully wired cluster node
  - object storage tiering is not production-integrated
  - snapshot/compaction durability is unfinished
  - at-rest encryption is absent
  - observability is far below enterprise expectations
  - the join path has a real admission-control flaw

Brutal verdict:
This is not safe yet for mission-critical or compliance-sensitive data. It is a promising prototype and a reasonable internal engineering platform, but not a commercially viable enterprise storage layer in its current form.

### Critical Blockers (P0)

1. Make the runtime actually distributed.
   - Wire `main()` and config loading to real TLS material, real `quinn::Endpoint` construction, `QuicTransportProvider`, `serve_quic()`, and supported join-mode startup.
   - Add real-network integration tests for QUIC + mTLS + join/restart flows.

2. Finish Raft durability lifecycle.
   - Persist `snapshot.wnd` in the live path.
   - Emit and consume real recovery checkpoints.
   - Implement durable WAL compaction / purge, not just in-memory cache mutation.

3. Make L1 to L2 transitions crash-atomic.
   - Add a durable manifest/journal for pending uploads and committed pivots.
   - Prevent follower uploads before leader authorization.
   - Reconcile tiered slabs correctly on followers and after restart.
   - Add orphan-object GC and remote object reclamation.

4. Fix security posture.
   - Bind `JOIN_REQ` identity to the authenticated transport peer.
   - Implement encryption at rest for local slabs, WAL, snapshots, and L2 objects with KMS-backed key management.
   - Add certificate rotation / reload operational support.

5. Add real memory bounds and backpressure.
   - Evict or age out the idempotency cache.
   - Add request-level memory budgets, slab-size limits that do not require full-blob reads, and upload/download streaming.
   - Add admission control / quotas.

6. Build an enterprise operations surface.
   - Install structured logging/tracing in the daemon.
   - Add latency histograms, high-cardinality-safe dimensions, and OTEL export.
   - Secure the metrics plane.

### Architectural Risks

- The most concerning design risk is state split across different persistence domains without a unifying transactional manifest. Local files, Raft state, and L2 objects can diverge today during tiering and compaction.
- The “whole slab as one object” L2 model is unlikely to scale. Range GET, multipart upload, chunk manifests, and remote compaction metadata will likely require a structural redesign before very large historical datasets are practical.
- Startup and recovery cost currently scale with raw slab footprint because indexes are rebuilt by scanning all files. At enterprise size, that will create poor restart SLOs and painful rolling upgrades.
- Too many critical behaviors are only proven in isolated tests or optional wiring rather than in the stock daemon composition. That is a maturity signal: the architecture is advancing faster than the production assembly.

### Recommended Roadmap

Phase 1: Production node foundation

- Make the daemon start a real authenticated network endpoint.
- Support join/bootstrap from config without custom embedding code.
- Add end-to-end QUIC/TLS/membership/restart tests.

Phase 2: Durability closure

- Persist snapshots and checkpoints.
- Implement durable WAL purge/compaction.
- Add scrubber jobs and operator-visible corruption handling.

Phase 3: Tiering closure

- Wire the storage sweeper into node bootstrap.
- Expose L2 config in the daemon.
- Add streaming multipart upload, range GET, pending-upload journals, orphan GC, and remote reclaim.

Phase 4: Security and compliance

- Add at-rest encryption and KMS integration.
- Fix join admission binding.
- Add audit-grade logging, certificate rotation, and policy controls.

Phase 5: SRE and scale hardening

- Add OTEL traces, histograms, and secure metrics.
- Add backpressure, quotas, and memory budgets.
- Benchmark restart time, large-slab cold reads, and quorum write saturation under fault injection.

### Evidence Notes

Targeted verification runs completed during this review:

- `cargo test normal_restart_recovers_vote_wal_and_state_machine`
- `cargo test interior_corruption_fails_startup_loudly`
- `cargo test sweeper_persists_remote_uri_shape_in_tier_metadata`
- `cargo test minority_watchdog_cannot_force_local_only_eviction`
- `cargo test supported_protocol_window_negotiates_and_joined_node_communicates`

Those passing tests increase confidence in the implemented slices. They do not close the larger product gaps above, especially around runtime wiring, L2 crash semantics, security admission, and enterprise operations.
