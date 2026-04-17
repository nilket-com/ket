# Wendelon Consolidated Assessment

Date: 2026-04-17

Inputs reviewed:
- `plans/eval/codex_eval_20260417.md`
- `plans/eval/claude_eval_20260417.md`
- `plans/eval/gemini_eval_20260417.md`

Targeted code verification performed in:
- `src/main.rs`
- `src/node.rs`
- `src/config.rs`
- `src/consensus/raft_storage.rs`
- `src/network/control_server.rs`
- `src/network/server.rs`
- `src/network/data_server.rs`
- `src/storage/manager.rs`
- `src/storage/sweeper.rs`
- `src/storage/core.rs`

## Executive Summary

My original Codex assessment was directionally correct, but Claude and Gemini both surfaced issues that deserve stronger weighting.

The core engine is real: WAL fsync discipline exists, quorum writes wait for durable replica acks, recovery paths are substantive, and several control-plane mechanisms are more than stubs. But the deployable product surface is still well short of enterprise readiness. The shipped daemon does not currently wire up a real QUIC/TLS node, `cluster.join` is still rejected by the public bootstrap path, L2 tiering is not production-integrated, follower-side tier cleanup is not wired, snapshot persistence is incomplete, encryption at rest is absent, and there is still no multi-tenant authz/audit posture.

Bottom line: this remains a credible prototype with several production-grade subsystems, not a production-grade storage product.

## What I Changed After Reading Teammate Reviews

### Points where Claude changed my weighting

- Claude is right that the runtime/bootstrap gap is not just an ops rough edge. It is a top-tier product blocker. `main.rs` only loads config, optionally starts metrics, calls `WendelonNode::bootstrap`, then waits for `ctrl_c` (`src/main.rs:54-97`). It does not build TLS material, does not construct a `quinn::Endpoint`, and does not call `serve_quic()`.
- Claude is also right that the public bootstrap surface still does not support a real join flow. `WendelonNode::bootstrap()` explicitly rejects `ClusterBootstrapConfig::Join` and tells the caller to use `bootstrap_with_provider` until transport wiring exists (`src/node.rs:379-386`).
- Claude's compliance emphasis is correct. Missing encryption at rest, missing RBAC/authz, missing audit logs, and missing real-transport integration tests are all pilot blockers for a Fortune 500 buyer, not "later hardening."

### Points where Gemini improved the assessment

- Gemini correctly identified a follower-side L1 cleanup gap as a concrete priority, not a theoretical one. `WendelonStateMachine` supports `with_tier_observer()` (`src/consensus/raft_storage.rs:876-883`), and `ManagerTierObserver` exists (`src/storage/sweeper.rs:440-460`), but production bootstrap uses `log_store.recovered_state_machine().await?` directly and never attaches the observer (`src/node.rs:437-441`). That means follower-side `TierSlab` applies do not automatically unlink local L1 files.
- Gemini is also right that the current object-tier read path is too expensive for scale because it still downloads whole slab blobs on L2 reads.

### Points where I do not adopt teammate conclusions as written

- I do not accept Gemini's framing that missing group-commit fsync is currently a correctness blocker for acknowledged writes. The current durable path is conservative rather than lax:
  - `LocalSlabStore::fsync()` is a real durability barrier (`src/storage/core.rs:187-210`).
  - `SlabManager::append_durable()` waits for that fsync before publishing the manager index (`src/storage/manager.rs:263-281`).
  - The leader write path waits for a durable quorum before finalizing (`src/network/data_server.rs:2320-2380`).
  Missing group commit is still a significant latency and throughput issue, but the current evidence supports "performance tax" more than "acknowledged-write loss."
- I also do not move to Gemini's readiness number. A system whose stock daemon does not yet bring up a real multi-node transport surface is not 65% enterprise-ready.

## Detailed Findings

## 1. Durability And Data Integrity

What is solid:

- WAL appends and vote persistence are durably fsynced before completion.
- The local data path uses a real durable boundary. `append_durable()` only publishes reads after fsync.
- Recovery coverage for torn tails, checkpoint replay, and corruption detection is materially better than toy quality.
- Record-level checksums exist in slabs and are validated during recovery.

What remains materially incomplete:

- Live snapshot persistence is still unfinished. The code can read `snapshot.wnd` on recovery, but the live snapshot builder only keeps bytes in memory (`src/consensus/raft_storage.rs:1309-1335`), and `install_snapshot()` also only updates in-memory state (`src/consensus/raft_storage.rs:1008-1025`). I still found no live persistence path that writes `snapshot.wnd`.
- Checkpoint consumption exists, but checkpoint emission still appears absent in the runtime path. That keeps restart-time safety dependent on tests and recovery scaffolding rather than on steady-state operation.
- WAL truncation and purge are not durably realized on disk.
- There is still no background scrubber, no proactive bit-rot verification pass, and no replica-repair workflow for detected corruption.
- Partition, split-brain, and deterministic fault-injection testing remain underdeveloped relative to the claims being made about correctness.
- Snapshot install is still `Cursor<Vec<u8>>` based, which makes large snapshot application RAM-unfriendly.

Assessment:

This is no longer a hand-wavy durability story. The write-ahead and quorum durability mechanics are real. But the full lifecycle is not done until snapshots, checkpoints, purge/compaction, scrub, and partition validation are closed.

## 2. Object Storage Tiering

This remains the least production-integrated major subsystem.

What is real:

- The OpenDAL-backed L2 client exists and is not a stub.
- The sweeper verifies L2 round-trip hashes before pivot.
- Rehydration uses temp-file plus fsync plus rename semantics.

Critical gaps:

- The stock daemon still cannot be configured for L2 from TOML because `load_node_bootstrap_config()` hardcodes `l2: None` (`src/config.rs:219-233`).
- Production bootstrap still does not spawn the storage tiering sweeper. In `bootstrap_with_provider()`, I only see the migration sweeper being started (`src/node.rs:638-642`), not `SlabSweeper`.
- Follower-side local cleanup is not wired. The state machine supports a tier observer, but node bootstrap does not attach `ManagerTierObserver` before handing the state machine to Raft (`src/node.rs:437-441`, `src/consensus/raft_storage.rs:876-883`, `src/storage/sweeper.rs:440-460`).
- `apply_tier_slab()` mutates in-memory tier state before unlinking the local file (`src/storage/manager.rs:634-655`). If unlink fails, retries can short-circuit because the slab already appears tiered.
- Upload still happens before the Raft pivot commit (`src/storage/sweeper.rs:264-310`). Without a pending-upload manifest or orphan reconciliation, remote leaks are a normal crash mode.
- The current leader-side defensive cleanup path in the sweeper is not enough to fix the follower problem. It only repairs the local node that ran the sweep after commit, not every replica that later applies the same `TierSlab`.
- L2 reads still pull whole slab blobs for point lookups.
- There is still no clear lifecycle for remote object reclamation, remote compaction cleanup, or orphan-GC.

Assessment:

Claude was right that L2 is not merely incomplete; it is not production-integrated. Gemini was right to elevate the follower leak. My original assessment stands, but this section should be treated as a P0/P1 closure area, not as a later optimization pass.

## 3. Runtime, High Availability, And Operations

This is the biggest place where Claude's review changed the weighting.

What is encouraging:

- Membership change logic, protocol negotiation, graceful drain hooks, and some backup/restore mechanics exist in library form.
- The control-plane implementation is materially ahead of where it was in the previous review cycle.

What still blocks a real deployment:

- The stock daemon is not yet a real networked cluster process. `main.rs` never wires QUIC/TLS runtime startup (`src/main.rs:54-97`).
- The public bootstrap path still rejects `cluster.join` (`src/node.rs:379-386`).
- The node builder still uses `NoDialTransportProvider` on the default path (`src/node.rs:386`, `996-1017`), which confirms that the out-of-process transport story is not the one exercised by the shipped binary.
- Repo search found no real-transport `quinn::Endpoint` tests under `tests/`; the existing coverage is mostly loopback/in-process.
- `main.rs` does not install a tracing subscriber, so the current `tracing::*` calls do not translate into a usable production observability surface.
- Observability remains basic: counters/gauges exist, but I still found no OTLP/OpenTelemetry path, no histograms, and no hardened metrics/auth surface.
- There is still no operator CLI, no clearly separated health/readiness probe surface, and no mature rebalancer.

Assessment:

The control-plane library code is ahead of the deployable daemon. That distinction matters. For an enterprise buyer, "the components exist" is not enough if the shipping binary still cannot form and operate a real TLS cluster.

## 4. Performance And Concurrency

What remains strong:

- `io_uring` plus `O_DIRECT` is a serious foundation.
- The quorum race in the write path is structured sensibly.
- Some local queueing bounds exist.

What is still too loose:

- The idempotency cache is still effectively unbounded. The schema promises expiry semantics, but the implementation only inserts and probes (`src/network/data_server.rs:2233-2239`, `2413-2427`).
- Whole-batch Arrow serialization on the write path remains a large allocator and copy hotspot.
- Whole-object L2 transfers remain the largest immediate cold-read cost problem.
- There is still no global admission control, no quota system, and no general backpressure layer.
- Several in-memory maps remain unbounded enough that long uptimes under high-cardinality traffic will become a stability problem.
- Recovery time still scales with slab footprint because indexes are rebuilt by rescanning files.

Assessment:

The performance story has a promising low-level substrate and a weak systems-bounding story. Missing group commit, missing backpressure, and missing memory caps should be treated as scale blockers, but not confused with the more immediate runtime and security blockers.

## 5. Security And Compliance

This is the second place where Claude's severity weighting is correct.

What is good:

- The transport identity model is conceptually sound. QUIC peers are mTLS-authenticated and SAN-bound to node IDs on the server side (`src/network/server.rs:372-384`).
- Lease and transfer flows use signed artifacts rather than trusting mutable metadata alone.

What remains unacceptable for enterprise deployment:

- There is still no encryption at rest for slabs, WAL artifacts, snapshots, or L2 objects.
- The join path still fails to bind authenticated transport identity to requested membership identity:
  - `handle_control_frame()` receives the authenticated `remote: NodeId` (`src/network/control_server.rs:1077-1088`).
  - `JOIN_REQ` handling ignores that authenticated identity and instead validates only payload fields (`src/network/control_server.rs:445-463`, `906-935`).
  - `accept_join()` then uses `req.node_id` to add membership (`src/network/control_server.rs:937-980`).
  This is a real admission-control bug.
- I still found no RBAC or client-facing authorization layer, no multi-tenant namespace enforcement, and no audit-log stream.
- There is still no hot certificate reload or key rotation path.
- The metrics plane is still plaintext and unauthenticated.
- Claude's point about deterministic dev signing material in example config is real. `render_example_config()` injects `dev_lease_signing_secret(EXAMPLE_NODE_ID)` into the starter config (`src/config.rs:35-48`), and `dev_lease_signing_secret()` is derived from a predictable string template (`src/network/lease_signing.rs:79-80`). It should be addressed before wider operator use, even if it is not the single highest blocker.

Assessment:

The crypto primitives are better than nothing, but the security posture as a product remains far below enterprise minimums. Missing at-rest encryption and missing authz/audit are enough by themselves to block regulated deployments.

## Consolidated Readiness Score

### 33 / 100

Why this number:

- Higher than my original 28 because Claude and Gemini correctly emphasize that several core subsystems are more real than "prototype" sometimes implies.
- Far below Gemini's 65 because the shipped daemon still does not assemble those subsystems into a deployable, test-proven, multi-node product.
- Still below Claude's low-40s because the runtime/bootstrap gap, L2 integration gap, and security/compliance floor are severe enough that I do not think a Fortune 500 pilot is close yet.

Interpretation:

- Engine quality: promising.
- Product assembly: incomplete.
- Enterprise defensibility: not yet close.

## Consolidated P0 Blockers

1. Make the shipped daemon a real cluster node.
   - Load TLS material from config.
   - Construct real `quinn::Endpoint` instances.
   - Start `serve_quic()`.
   - Support `cluster.join` on the public bootstrap path.

2. Establish the minimum enterprise security floor.
   - Implement encryption at rest for L1, WAL, snapshots, and L2.
   - Bind `JOIN_REQ` identity to the authenticated peer identity.
   - Add at least basic control-plane authz and an audit stream.

3. Close tiering integration and follower cleanup.
   - Expose L2 config in TOML.
   - Spawn the real tiering sweeper in bootstrap.
   - Attach `ManagerTierObserver` in production bootstrap.
   - Add orphan-object reconciliation and remote reclaim.

4. Finish the durability lifecycle.
   - Persist live snapshots.
   - Emit real checkpoints.
   - Implement durable WAL compaction/purge.
   - Add background scrub and corruption-handling flows.

5. Prove the actual runtime path.
   - Add real QUIC/TLS integration tests.
   - Add partition/fault-injection coverage.
   - Validate restart, join, failover, and backup/restore over the real transport surface.

6. Add hard memory bounds and backpressure.
   - Evict or age out idempotency state.
   - Bound in-flight request memory.
   - Introduce overload signaling and admission control.

## Suggested Planning Order

For the next execution-planning stage, I would sequence work like this:

1. Runtime wiring plus real-transport test harness.
2. Join admission fix plus at-rest encryption design.
3. L2 bootstrap wiring plus follower cleanup plus orphan reconciliation.
4. Snapshot/checkpoint/compaction closure.
5. Backpressure, quotas, and observability hardening.
6. RBAC, audit, and broader compliance work.

This order preserves the most important dependency: first make the actual shipped node real and testable, then close the security and storage-integrity holes on the path that operators will actually run.
