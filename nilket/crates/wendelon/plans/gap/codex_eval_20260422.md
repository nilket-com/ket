# Wendelon Code-Level Production Readiness Gap Report

Date: 2026-04-22

Scope reviewed:
- `src/`
- `src/consensus/`
- `src/network/`
- `src/storage/`
- `tests/`

## Verdict

Wendelon is not yet Fortune 500 production-ready at the code level, and it does not currently justify a 99.999% reliability claim.

The codebase has real substance: the unified-ring intent is visible, `openraft` is genuinely wired in, the slab manager waits for durability before publishing visibility, and there is restart-recovery machinery for bifurcated writes. But several design claims are only partially implemented, and a few core hot paths are materially weaker than the architecture language suggests:

- storage is not actually running one `io_uring` thread per NVMe device
- the Stellarator bifurcated write path serializes local durability before remote fan-out
- read and write request validation clones the full replicated state machine
- finalization recovery becomes best-effort when the local pending-intent store fails
- the QUIC server lacks admission control and transport tuning expected for hostile or very large fleets
- autonomous healing stops at mathematical eviction; node replacement remains explicitly external and accepted as a temporary durability drop
- off-box audit shipping is not a compliance-grade substitute for the local hash-chained archive, and the Kafka path is optional behind a cargo feature
- tests are smoke-level compared with the Jepsen-style and simulator-ladder claims
- backup/DR is still baseline node-image restore rather than a cluster-level recovery story
- production crypto custody is still a dev placeholder

My conclusion is: this is a credible advanced prototype with several production-oriented mechanisms, but not a finished high-scale warehouse runtime.

## Pilot-Phase Readiness Score

If "pilot phase ready" means a controlled internal pilot or a design-partner pilot on trusted infrastructure with active operator oversight, the current state is approximately **62/100** complete.

If "pilot phase ready" means a customer-facing pilot with real SLA expectations, multi-tenant exposure, and low tolerance for manual recovery, the current state is approximately **48/100** complete.

Area-by-area scoring:

- Core architecture and Raft integration: **72/100**
- Storage engine and durability path: **60/100**
- QUIC transport and hot-path efficiency: **54/100**
- Stellarator bifurcated write robustness: **52/100**
- Testing and fault-injection depth: **46/100**
- Observability and operability: **44/100**
- Security and production custody posture: **30/100**

Why the score is above 50 for an internal pilot:

- the major subsystems are implemented, not just sketched
- durability ordering is mostly thoughtful
- restart-recovery machinery exists for bifurcated writes
- the codebase is coherent enough to run meaningful workload trials

Why the score is still well below production-pilot confidence:

- the shared `io_uring` bottleneck materially weakens the storage scaling story
- local durable append is serialized before remote fan-out in the Stellarator path
- request admission clones full replicated state
- pending-finalization recovery becomes best-effort when local intent persistence fails
- QUIC admission control and per-tenant backpressure are missing
- chaos/simulator/FIPS coverage is much narrower than the design language suggests
- production key-custody is not implemented; `dev-insecure` is still the only provider

Cross-review note: after reading `plans/gap/claude_20260422.md` and `plans/gap/gemini_20260422.md` and then checking the corresponding repo docs, I would keep the same headline percentages, but the broader system evidence clusters near the lower end of the customer-facing score because Wendelon explicitly accepts degraded durability until external node replacement, the backup/DR docs are still node-image scoped, and off-box audit shipping is not the authoritative compliance archive.

## What Matches The Design Claims

There is some real alignment with the stated architecture:

- I did not find an actual separate control-plane process or separate membership universe. The code mostly follows a unified transport and shared replicated metadata model.
- The slab durability path is careful about visibility ordering. `append_durable()` does not publish the indexed location until durability succeeds and rolls back on errors.
- The pending-finalization mechanism is real, not hand-waved, and restart replay exists when the intent record is successfully persisted.
- The `openraft` integration is not cosmetic. The code persists WAL/vote/snapshot state and the targeted control-ring audit test passed.

Those strengths are real. The gaps below are about scale, recovery hardening, and operational safety.

## Guardrail Cross-Check: "No Artificial Control/Data Plane Split"

I did not find evidence of a hard architectural split in the implementation. However, the automated guardrail in `tests/architecture_test.rs` is much weaker than the philosophy document implies.

`tests/architecture_test.rs:134-156`

```rust
#[test]
fn test_forbidden_word_count() {
    let forbidden_word = "plane";
    let max_allowed = 2;
    let mut total_occurrences = 0;
    ...
    assert!(
        total_occurrences <= max_allowed,
        "The word '{}' appeared {} times, which exceeds the limit of {}.",
        forbidden_word,
        total_occurrences,
        max_allowed
    );
}
```

This test does not verify any of the things that would matter operationally:

- one membership graph
- one epoch authority
- no second listener topology that can diverge from the main ring
- no duplicated routing truth between control and data code

So the codebase mostly appears unified, but the test suite does not actually prove the guardrail.

## Critical Gaps

### 1. Storage topology does not match the "one io_uring thread per NVMe" claim

`src/storage/io_uring.rs:3-5`

```rust
//! One instance of [`IoRingHandle`] maps one-to-one to a single OS
//! thread and a single `io_uring::IoUring` instance. In production
//! Wendelon spawns one such pair per physical NVMe device.
```

`src/node.rs:970-996`

```rust
// 2. io_uring thread for slab + WAL I/O.
let io = spawn_io_thread(cfg.io_depth)?;
...
let log_store =
    WendelonLogStore::open(&wal_path, &vote_path, &snapshot_path, io.clone()).await?;
```

Assessment:

- The implementation creates one shared `io_uring` thread per node instance and then shares it between slab I/O and the Raft WAL.
- That is not "one ring per physical NVMe device".
- On a busy leader, large slab appends, fsync/fdatasync, and Raft durability work all compete in one queue and one submission thread.

Why this blocks production-readiness:

- Raft commit latency and slab write latency are directly coupled.
- A single queue/thread becomes a tail-latency choke point under mixed data and metadata pressure.
- The code-level claim overstates the actual hardware parallelism available.

Related evidence:

`src/storage/file_ops.rs:141-183`

```rust
async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
    let path = path.to_path_buf();
    spawn_blocking_io(move || {
        let mut options = std::fs::OpenOptions::new();
        ...
        options.open(path).map(FileHandle::new)
    })
    .await
}
```

`src/storage/file_ops.rs:241-305`

```rust
async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
    let path = path.to_path_buf();
    spawn_blocking_io(move || std::fs::read(path)).await
}
...
async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
    let from = from.to_path_buf();
    let to = to.to_path_buf();
    spawn_blocking_io(move || std::fs::rename(from, to)).await
}
```

The storage layer is only partially `io_uring`-driven. Important file operations still run through blocking worker threads.

### 2. The Stellarator write path serializes local durability before remote quorum fan-out

`src/network/data_server.rs:3328-3379`

```rust
let local_is_replica = replicas.iter().any(|n| n.id == self.local_node.id);
if local_is_replica {
    match self
        .slab
        .append_durable(
            meta.tenant_id,
            bifurcated.key_identity,
            commit_hlc,
            bifurcated.frame_bytes.as_slice(),
        )
        .await
    {
        Ok(_) => durable_acks += 1,
        Err(e) => return Err(e),
    }
}

let mut join: JoinSet<std::io::Result<ReplicaAckLevel>> = JoinSet::new();
for target in replicas.iter().copied() {
    ...
    join.spawn(async move {
        Self::replicate_to_remote(factory, pool, target, replica_meta, bytes).await
    });
}
```

Assessment:

- The comment says "local append+flush ... and concurrent fan-out".
- The code actually waits for the leader-local durable append to complete before any remote replication tasks are spawned.
- That means the first remote durable ack cannot even begin until local append+flush returns.

Why this matters:

- It inflates write latency by one local durability barrier on the leader.
- It reduces the throughput advantage expected from a bifurcated data-first quorum model.
- It increases the impact of local disk stalls on cluster-wide write latency.

For a system selling a high-scale write architecture, this is a material hot-path mismatch.

### 3. Finalization recovery degrades to best-effort when pending intent persistence fails

`src/network/data_server.rs:3402-3447`

```rust
let pending_persisted = match self.pending_finalizations.upsert(pending.clone()).await {
    Ok(()) => true,
    Err(err) => {
        tracing::warn!(
            pending_id = %format_bytes_hex(pending.id.as_bytes()),
            error = %err,
            "failed to persist pending finalization; continuing without restart recovery"
        );
        false
    }
};

match self.resolve_pending_finalization(&pending).await {
    ...
    Err(err) => {
        ...
        if pending_persisted {
            self.idempotency.insert(
                key,
                CachedWriteOutcome {
                    response,
                    finalization: None,
                    pending_finalization_id: Some(pending.id),
                },
            );
            self.pending_finalization_wakeup.notify_one();
        }
        Ok((response, None))
    }
}
```

Assessment:

- The code explicitly continues after failing to persist the recovery intent.
- If the control-ring finalization then fails, recovery bookkeeping is only installed when `pending_persisted == true`.
- In the failed-persist branch, the system can return a pending/error outcome after the data branch reached durable quorum but without a guaranteed restart-replay record.

Why this blocks a 99.999% claim:

- This is exactly the class of partial-failure that a bifurcated write model must nail.
- The current behavior is survivable in many cases, but it is not fully crash-resilient.
- A storage or local metadata hiccup in the wrong window can strand quorum-durable data without deterministic finalization replay.

### 4. Hot-path request validation clones the full replicated state machine

`src/network/data_server.rs:3222-3224`

```rust
let sm = self.state_machine.state_snapshot().await;
let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
```

`src/network/data_server.rs:3458-3460`

```rust
let sm = self.state_machine.state_snapshot().await;
let current_epoch = sm.e_safe_watermarks.keys().copied().max().unwrap_or(0);
```

`src/consensus/raft_storage.rs:1973-1978`

```rust
/// Test / caller inspection hook - return a cloned snapshot
/// of the applied state without driving a full snapshot
/// build round-trip.
pub async fn state_snapshot(&self) -> SmState {
    self.inner.read().await.state.clone()
}
```

`src/consensus/raft_storage.rs:1372-1432`

```rust
pub struct SmState {
    ...
    pub epoch_history: BTreeMap<u64, EpochConfig>,
    pub leases: HashMap<u64, crate::schema::LeaseDescriptor>,
    pub e_safe_watermarks: HashMap<u64, HybridTimestamp>,
    pub tiered_slabs: HashMap<SlabId, TieredSlab>,
    pub migrations: HashMap<u64, MigrationLedgerEntry>,
    pub tombstones: Vec<MigratedRangeTombstone>,
    pub tenants: HashMap<crate::schema::TenantId, TenantRecord>,
    pub tenant_keysets: HashMap<crate::schema::TenantId, TenantKeyset>,
}
```

Assessment:

- Both write and read paths clone the entire `SmState` just to derive the current epoch.
- `SmState` contains multiple growing maps and vectors.
- This is O(replicated metadata size) memory traffic on every request that hits these paths.

Why this is serious:

- It puts avoidable pressure on allocator, cache, and `tokio` scheduling.
- It creates lock hold and clone amplification as tenant count, migration ledger size, and tiered slab maps grow.
- It is the opposite of a production-ready hot path.

This should be replaced with a narrow immutable watch/cache of the specific fields required for request admission, not a full state clone.

### 5. QUIC server has no admission control, quotas, or bounded task fan-out

`src/network/server.rs:48-53`

```rust
//! ## What is not here yet
//!
//! * Unidirectional stream handling (needed for
//!   [`crate::schema::MsgType::STREAM_FIN_EVT`] push).
//! * 0-RTT or datagram paths.
//! * Backpressure / per-tenant quotas.
```

`src/network/server.rs:350-370`

```rust
incoming = endpoint.accept() => {
    let Some(incoming) = incoming else {
        break;
    };
    ...
    tokio::spawn(async move {
        ...
        if let Err(e) =
            serve_quic_connection(incoming, service, pool, lifecycle, cluster_domain).await
        {
            let _ = e;
        }
    });
}
```

`src/network/server.rs:531-538`

```rust
let _stream_guard = lifecycle.track_work();
tokio::spawn(async move {
    let _stream_guard = _stream_guard;
    let _ = handle_stream(stream, service, pool, context).await;
});
```

Assessment:

- The code says quotas/backpressure are not there yet, and the implementation confirms it.
- The listener spawns a task per connection and then another task per stream.
- I did not find transport-level admission controls in this path.

Why this blocks production-readiness:

- A hostile or simply very busy tenant can drive scheduler, memory, and buffering collapse.
- Without explicit per-tenant or per-peer bounds, one noisy peer can degrade unrelated traffic.
- This is not acceptable for shared Fortune 500 infrastructure.

### 6. The advertised "QUIC on top of io_uring + GSO" path is not actually present in code

`src/network/transport.rs:69-73`

```rust
/// Tier 1 - QUIC on top of `io_uring` + GSO. The universal
/// baseline: always available on hosts that can run Wendelon
/// at all, and the channel that carries the capability
/// handshake even when an upgrade to Tier 0 is about to happen.
```

`src/network/quic_transport.rs:179-185`

```rust
pub fn endpoint_from_tls(
    material: &TlsMaterial,
    bind_addr: SocketAddr,
) -> std::io::Result<quinn::Endpoint> {
    let mut endpoint = quinn::Endpoint::server(server_config(material)?, bind_addr)?;
    endpoint.set_default_client_config(client_config(material)?);
    Ok(endpoint)
}
```

Assessment:

- The code creates a default `quinn::Endpoint`.
- I did not find `TransportConfig` tuning, GSO/GRO handling, or any explicit `io_uring` integration in the QUIC transport path.
- So the transport tier name and comments currently overstate the implementation.

Why this matters:

- A production claim about a transport hot path needs to be true in the code, not just in naming.
- If the intended high-scale path is not configured here, then benchmark results and operational expectations are likely optimistic.

### 7. Framing and buffer reuse still rely on copies plus a global mutexed pool

`src/network/core.rs:143-160`

```rust
async fn read_section<R: AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    pool: &Arc<BufPool>,
    len: u32,
) -> std::io::Result<Option<PooledBuf>> {
    let mut buf = pool.acquire_at_least(len as usize);
    {
        let vec = buf.as_mut_vec();
        vec.clear();
        if len > 0 {
            vec.reserve(len as usize);
            let n = (&mut *reader).take(len as u64).read_to_end(vec).await?;
            if n != len as usize {
                return Err(Error::new(ErrorKind::UnexpectedEof, "early eof"));
            }
        }
    }
    Ok(Some(buf))
}
```

`src/network/pool.rs:38-42`

```rust
pub struct BufPool {
    bufs: Mutex<Vec<Vec<u8>>>,
    max_retained: usize,
    max_cap_bytes: usize,
}
```

Assessment:

- The framing path still copies bytes into pooled `Vec<u8>` buffers.
- Reuse is better than repeated allocation, but the free list is protected by one `std::sync::Mutex`.
- That is not a zero-copy or lock-minimized transport hot path.

Why this matters:

- At high connection and stream counts this pool can become a contention point.
- Copy-heavy framing erodes the performance story advertised around the network path.

## Significant Gaps

### 8. Release builds abort on panic, and production paths still contain `expect(...)` / poisoned-lock abort sites

`Cargo.toml:57-62`

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true
```

`src/storage/io_uring.rs:339-370`

```rust
let buf_ref = buf
    .as_ref()
    .expect("io_uring write requests must carry an aligned buffer");
...
unsafe {
    ring.submission()
        .push(&entry)
        .expect("SQ push: checked !is_full() above");
}
```

`src/storage/manager.rs:384-387`

```rust
self.locality_index
    .write()
    .expect("locality index lock poisoned")
```

Assessment:

- In release, a panic aborts the process.
- Some of these `expect(...)` calls may be truly unreachable, but several poisoned-lock and queue-state assumptions remain in live code.
- That means a single unexpected invariant breach becomes a process kill, not an isolated request failure.

Why this matters:

- 99.999% systems aggressively convert recoverable faults into explicit errors, fencing, or self-healing behaviors.
- Abort-on-panic plus production `expect(...)` sites materially raise the blast radius of rare failures.

### 9. Some storage hot paths still carry scaffold-grade comments and avoidable copies

`src/storage/core.rs:766-769`

```rust
// Fold header + frame into one contiguous slice for hashing.
// The extra copy is fine for scaffold grade; a streaming
// SHA-256 helper with two `update` calls would eliminate it.
let mut buf = Vec::with_capacity(header_bytes.len() + frame.len());
```

Assessment:

- The code explicitly acknowledges that this is scaffold-grade.
- That is not a correctness bug on its own, but it is a clear signal that at least some storage hot-path work is not yet finalized.

### 10. The state machine still contains acknowledged incomplete features

`src/consensus/raft_storage.rs:2770-2774`

```rust
WendelonRequest::SchemaUpdate(_bytes) => {
    // Schema registry is not modelled in SmState yet;
    // accepting the write keeps the log replayable and
    // lets the control-ring feature land incrementally.
    (WendelonResponse::Success, None)
}
```

`src/network/traits.rs:12-19`

```rust
//  CONTROL-RING META - placeholder (non-wire) structs.
//
//  `TransferLeadershipRequest/ResponseMeta` and
//  `ClusterStatusResponseMeta` are still plain-Rust stubs - the
//  control-ring wire layout for those flows is not yet frozen.
```

`src/client/topology.rs:22-28`

```rust
//! [`crate::network::traits::ClusterStatusResponseMeta`] is an
//! in-process placeholder whose wire layout is explicitly not yet
//! frozen. Rather than promote it to a [`bytemuck::Pod`] prematurely,
//! we ship a postcard-encoded projection here ...
```

Assessment:

- Parts of the control-ring API and schema replication story are still marked as placeholder or incremental.
- That is not consistent with "production-ready" if wire compatibility and schema correctness are supposed to be stable.

## Observability Gaps

The metrics surface is not deep enough for a 99.999% runtime.

`src/metrics/registry.rs:422-444`

```rust
render_histogram(
    &mut out,
    "wendelon_group_commit_window_seconds",
    "Time writes spent coalescing in the group-commit window before fdatasync.",
    &self.group_commit_window_seconds.snapshot(),
);
render_histogram(
    &mut out,
    "wendelon_l1_read_latency_seconds",
    "Storage-side latency for reads served from local L1 slabs.",
    &self.l1_read_latency_seconds.snapshot(),
);
render_histogram(
    &mut out,
    "wendelon_l2_upload_latency_seconds",
    "Latency for uploading a tiered slab object into L2 storage.",
    &self.l2_upload_latency_seconds.snapshot(),
);
```

This is useful, but I did not find first-class exported metrics for several critical surfaces after searching `src/metrics`:

- pending-finalization backlog, replay attempts, replay latency, replay failures
- replica fan-out latency and quorum-acquisition latency
- `io_uring` SQ/CQ depth, submission lag, CQE lag, submit failures
- WAL append latency and fsync/fdatasync latency
- active connection count, active stream count, task backlog, rejected streams
- lock contention or queue-depth indicators on major shared structures

Without those metrics, you do not have the telemetry needed to operate a five-nines distributed storage system.

## Test Coverage Gaps

### 11. Chaos coverage is smoke-level, not Jepsen-level

`tests/chaos.rs:32-36`

```rust
const CHAOS_SMOKE_SCENARIOS: &[&str] = &[
    "leader_partition_and_rejoin",
    "rolling_kill",
    "packet_drop_single_stream",
];
```

Assessment:

- These are useful smoke scenarios.
- They are not enough to justify a claim of Jepsen-style chaos hardening.
- I did not find broad nemesis combinations, long-running schedule exploration, or linearizability checking infrastructure at the same level as the claim implies.

### 12. Simulator ladder coverage is currently one named scenario

`tests/simulator_ladder.rs:162-164`

```rust
fn supported_scenario(name: &str) -> bool {
    matches!(name, "coverage_ladder")
}
```

Assessment:

- This is a good start, but it is not a broad ladder matrix.
- One named ladder is not enough to demonstrate coverage across upgrade, migration, lease churn, or transport degradation envelopes.

### 13. The FIPS "sweep" is a binary symbol/string check, not a full compliance validation story

`ci/check_fips_symbol_sweep.sh:23-45`

```bash
echo "Building FIPS release artifact with CC=${CC} CXX=${CXX}"
cargo build --release --no-default-features --features fips --bin wendelon
...
if nm "${binary}" | rg -i 'aesgcmsiv|AESIV|GCM_SIV'; then
  ...
fi
...
if strings "${binary}" | rg -F 'blake3-256'; then
  ...
fi
```

Assessment:

- This check is valuable as a regression guard.
- It is not equivalent to a deep cryptographic validation program.
- The user-facing claim should be described as a symbol/string sweep, not a complete FIPS assurance story.

## Cross-Review Additions

This section folds in findings from `plans/gap/claude_20260422.md` and `plans/gap/gemini_20260422.md` that I was able to corroborate directly in repo docs/code. These are broader operational and product-surface gaps than the `src/` hot-path audit above, but they materially reinforce the same readiness verdict.

### Built-in healing stops at eviction; physical replacement is explicitly external

`plans/adr/001_autonomous_healing_boundary.md:12-17`

```md
Wendelon will implement **Level 1 Autonomous Eviction and Mathematical Healing**.
...
Wendelon **WILL NOT** implement cloud provisioning logic (API calls to AWS/GCP/K8s) to physically replace the hardware.
```

`plans/adr/001_autonomous_healing_boundary.md:23-26`

```md
- **Temporary Degradation:** Evicting a node mathematically heals the routing, but it temporarily reduces the replication factor of the affected ranges until a new physical node joins the cluster.
- **Cloud-Native Rejection:** ... If an operator wants "Level 2" auto-replacement, they must wire Wendelon's observability metrics (Prometheus) to an external autoscaler.
```

Assessment:

- The architecture deliberately stops at routing-level healing and accepts a temporary durability drop afterward.
- That choice is coherent, but it materially weakens any five-nines claim unless the deployment story includes a first-party replacement operator or a tightly-specified external replacement contract.
- This amplifies the code-level concerns around write-path latency and recovery windows rather than offsetting them.

### Kafka audit shipping is optional and intentionally omits chain-replay fields

`plans/ops/audit.md:15-19`

```md
- The local file at `<data_dir>/audit.ndjson` is always enabled. `Kafka` and `Syslog` are additive shipping sinks.
- The local NDJSON file is the **only** sink that carries the per-node chain fields required for replay and tamper detection.
- `Kafka` and `Syslog` are off-box projections for distribution and alerting. They are not sufficient to reconstruct the hash chain.
```

`plans/ops/audit.md:64-72`

```md
Notably absent from Kafka:
- `sequence_number`
- `prev_hash`
- `record_hash`
- `schema_version`
- `hash_algorithm_tag`
...
Kafka is therefore a distribution format, not a chain-replay format.
```

`src/node.rs:538-543`

```rust
#[cfg(not(feature = "audit-kafka"))]
{
    return Err(Error::new(
        ErrorKind::Unsupported,
        "audit.kafka requires the audit-kafka cargo feature",
    ));
}
```

Assessment:

- Gemini and Claude were right to call this out. The off-box SIEM path is intentionally non-authoritative, and Kafka is not available in the default binary unless the feature is enabled at build time.
- For enterprise audit/compliance positioning, that means the local NDJSON chain remains the evidence archive and must itself be retained and shipped correctly.
- This is not a core consensus bug, but it is a real product-readiness limitation.

### Backup/DR remains baseline node-image restore, not cluster-wide recovery

`plans/ops/backup_dr.md:5-10`

```md
The baseline backup unit is a full node image rooted at one data directory:

- Control Ring artifacts: `control.wal`, `vote.wnd`, `snapshot.wnd` when present
- local Data Ring slab files: active and sealed `slab_<id>.wnd`
- a manifest that records the recovered Control Ring position, local slab ids,
  and any tiered-slab assumptions
```

`plans/ops/backup_dr.md:31-37`

```md
- the authoritative pointer lives in the Control Ring state
- the backup manifest records those pointers explicitly
- the backup does not inline remote-tier payload bodies

Operators therefore still need durable object-store policy outside Wendelon
for tiered data, including cross-region replication if the deployment requires
it.
```

`plans/ops/backup_dr.md:48-53`

```md
- scheduled export and retention management
- multi-node cluster replacement choreography
- remote-tier payload validation during restore
- legal-hold specific backup policy
```

Assessment:

- Claude's DR concern is well-founded. The documented baseline is still node-image restore plus external object-store guarantees.
- Multi-node replacement choreography and restore validation are explicitly deferred.
- That is acceptable for an engineering pilot, but not for a serious 99.999% reliability claim.

### Lazy migration explicitly accepts proxy-hop p99 spikes and still lacks corresponding QoS evidence

`plans/adr/006_tiered_storage_lazy_migration.md:10-20`

```md
2. **Lazy Migration:** During an epoch transition, data is **NOT** eagerly pushed to new owners. Migration is entirely lazy, triggered by on-demand "Migration Reads" for hot data, and a heavily throttled Background Sweeper for cold data.
...
- **Latency Spikes on Migration:** Because data moves lazily, the very first read of a key after a topology change will incur a "proxy hop" latency penalty as the new owner fetches it from the old owner.
- **Complex Sweeper Logic:** The Background Sweeper becomes a critical, highly complex state machine responsible for both Tiering (uploading to S3) and Migration (pulling from old owners).
```

Assessment:

- Gemini's "thundering herd" concern is directionally correct, and the ADR itself already accepts the p99 spike tradeoff.
- In the current codebase that risk is not counterbalanced by strong evidence of proxy-read QoS controls, listener admission controls, or topology-change stress coverage.
- This is one of the places where the architectural choice is coherent but the operational hardening is still behind it.

## Hardcoded Values And Embedded Policy That Need More Justification

These are not all bugs, but they are signs the system still has bench-derived policy embedded in code rather than robust adaptive control:

- `src/node.rs:968` creates `BufPool::new(256, 1024 * 1024)` as a hardcoded pool shape.
- `src/storage/io_uring.rs:27-28` sets `DEFAULT_RING_DEPTH: u32 = 1024`.
- `tests/architecture_test.rs:136-138` uses a hardcoded `"plane"` occurrence cap of `2` as an architectural guardrail.
- `tests/chaos.rs:30-36` hardcodes one default seed and three smoke scenarios.

Those defaults may be reasonable, but the current code does not show an adaptive or capacity-aware control loop around them.

## Security And Compliance Blockers

### Root KEK custody is still a dev-only placeholder

`src/crypto.rs:104-107`

```rust
pub enum RootKekProviderConfig {
    #[default]
    DevInsecure,
}
```

`src/config.rs:177-183`

```rust
impl Default for RawCryptoConfig {
    fn default() -> Self {
        Self {
            root_kek_provider: RootKekProviderConfig::default(),
            ...
        }
    }
}
```

`examples/wendelon.toml.example:39-47`

```toml
# Optional W4 root-KEK bootstrap selector. `dev-insecure` is the
# current dev/test placeholder. It persists a local plaintext Root
# KEK at `<data_dir>/.wendelon/dev-root-kek.bin` so restarted dev
# nodes can still unwrap prior Tenant KEKs, and is not suitable for
# a production custody boundary.
```

Assessment:

- The code explicitly says the only implemented provider is not suitable for production custody.
- That alone blocks any credible Fortune 500 production-readiness claim.

## Net Assessment By Area

### io-uring and storage

- Real implementation exists, but the node-level topology does not match the per-device claim.
- Mixed slab + WAL work on one shared ring thread is a bottleneck.
- Some important storage operations are still blocking-thread based.
- A few scaffold-grade optimizations are still deferred.

### QUIC transport hot path

- The implementation is functionally real but under-tuned.
- The tier naming overstates actual `io_uring`/GSO implementation.
- There is no admission control or tenant-aware backpressure.
- Framing remains copy-heavy and centrally mutexed.

### openraft / control ring

- The integration is genuine, not fake.
- The main code-level concerns are state-machine maturity, placeholder APIs, and request-path state cloning.
- I did not find a smoking-gun consensus correctness bug in the audited paths, but the operational hardening level is not yet where it needs to be.

### Stellarator bifurcated write model

- The architecture is present and the code handles the happy path plus some failure recovery.
- The main blockers are:
  - local durable append is serialized before remote fan-out
  - finalization recovery becomes best-effort when local intent persistence fails
  - observability around the recovery path is not deep enough

### Day-2 operations, audit, and DR

- The architecture intentionally stops built-in healing at eviction and accepts a temporary durability drop until infrastructure replaces the node.
- The documented backup/restore baseline is still node-image scoped and leaves cluster choreography and remote-tier validation deferred.
- Kafka and syslog audit shipping are convenience projections; the local NDJSON chain remains the authoritative tamper-evident archive.

## What Must Change Before A 99.999% Claim Is Defensible

1. Split storage execution by device or durability class, with independent `io_uring` workers and queue-depth telemetry.
2. Make bifurcated writes actually concurrent across local and remote durability legs, or document and justify the serialized design explicitly.
3. Make pending-finalization intent persistence mandatory for the recoverable branch, not a warning-only best-effort path.
4. Remove full `SmState` cloning from request admission paths.
5. Add admission control, stream limits, and per-tenant quotas to the QUIC listener.
6. Implement or remove the `io_uring + GSO` transport claim.
7. Replace live-path `expect(...)` / poisoned-lock aborts with recoverable error handling where feasible.
8. Freeze the control-ring wire contracts and finish modeling schema updates in durable state.
9. Add production crypto custody providers and make secure custody the default.
10. Expand chaos, simulator, and recovery testing to match the architectural claims.
11. Add metrics for quorum timing, pending finalization, WAL/fsync latency, transport saturation, and queue depth.
12. Provide a first-party or tightly-specified synchronous node-replacement path if five-nines availability is a real target.
13. Make the authoritative audit-retention story explicit: either ship Kafka with chain fields by default or clearly position local NDJSON plus off-box archival as the compliance archive.
14. Ship a cluster-level DR and restore choreography, not just node-image restore guidance.
15. Add explicit QoS/backpressure evidence for lazy-migration proxy reads during topology changes.

## Targeted Tests Run During This Audit

These targeted checks passed during the review:

- `cargo test --test architecture_test --quiet`
- `cargo test --test quic_transport quic_provider_dials_and_verifies_peer_identity --quiet`
- `cargo test --test control_ring_rpc advance_epoch_commit_emits_raft_audit_once_across_restart --quiet`
- `cargo test --test chaos chaos_smoke_catalog --quiet`

Those passing results are good signs, but they do not materially change the production-readiness verdict because the main blockers are architectural hot-path behavior, incomplete hardening, and insufficient fault-coverage depth.
