# ADR 015: Segmented WAL and Snapshot Lifecycle

## Status
Proposed

## Context
ADR 007 chose `openraft` as the consensus framework and declared that Wendelon owns "the physical durability layer, fsync ordering, and on-disk layout." It did not specify what that layer looks like. This ADR fills the gap: the WAL's physical form, how it compacts, when snapshots fire, how snapshots are transported, and how they're compressed.

Forces in play:
- `openraft`'s callback contract requires `purge`, snapshot-build, and snapshot-install hooks that we implement.
- The WAL cannot grow unbounded; compaction must drop log below the last committed snapshot without fighting kernel and filesystem behavior.
- Snapshot install and build must work for state machines in the tens of gigabytes from day one; whole-buffer designs will OOM the node.
- Compaction behavior must compose cleanly with per-segment encryption key rotation (see ADR 020).

Derived from `plans/gap/claude_answers_20260417.md` §3.

## Decision

1. **Segmented WAL.** The WAL is a sequence of fixed-size segment files. Default segment size is 128 MiB. Each segment is named by its starting log index. Segments are append-only; once closed, they are immutable until deletion.

2. **Drop-segment compaction.** Compaction is "delete segments entirely below the last-committed snapshot boundary." We do not hole-punch inside segments and do not rewrite segments in place.

3. **Parent-directory fsync on namespace operations.** Segment creation, rotation (atomic rename of a staging file into the active name), and deletion each require an fsync of the containing directory after the corresponding file operation. Without it, Linux ext4/xfs can lose a newly created segment file on crash or resurrect a just-deleted one, defeating segment durability. Directory fsync is cheap (one syscall per namespace transition) and mandatory.

4. **Hybrid snapshot trigger.** A snapshot fires when *either* of the following crosses its threshold, whichever first:
   - Applied log entries since last snapshot ≥ `snapshot_log_count_threshold`.
   - Wall-clock time since last snapshot ≥ `snapshot_time_threshold`.
   Both thresholds are TOML-configurable. Production defaults are tuned so worst-case replay from the most recent snapshot is bounded to ~30 seconds on reference hardware; see Open Questions.

5. **Snapshot transport — leader→follower streaming.** Snapshots are streamed over the consensus transport as fixed-size 4 MiB frames, 4 KiB-aligned for O_DIRECT. The final frame is a manifest that lists each preceding frame's hash and ordinal. The receiver accumulates frames in order, hashes each as it arrives, and on receipt of the manifest frame validates that every observed hash matches the manifest before committing the snapshot. No random-access assumption: if the transport is lost mid-stream, the partial snapshot is discarded and install restarts. Snapshots written to object storage (see ADR 022) are a separate format with a random-access footer; the consensus-transport wire format does not depend on random-access.

6. **Snapshot compression.** `zstd` with dictionary training against representative state-machine snapshots. Compression level 3 for live snapshot build during steady-state operation; level 9 for background recompression of archived snapshots. Compression runs *above* encryption — encrypted ciphertext is incompressible.

7. **Checkpoints at every snapshot boundary plus periodic.** Checkpoints are emitted both at snapshot boundaries (always) and periodically (every 1000 applied entries or every 5 seconds, whichever first). Snapshots provide logical commit points; periodic checkpoints bound recovery work when snapshots are rare.

## Rationale

- **Segments align with filesystem reality.** `fallocate` hole-punch behavior is inconsistent across ext4/xfs/btrfs and interacts badly with O_DIRECT. Segment deletion is a portable `unlink` that every filesystem implements correctly.
- **Drop-segment compaction avoids write amplification.** Rewrite-in-place doubles write cost and couples compaction to state. Drop-segment writes zero bytes during compaction.
- **Fixed-size segments enable per-segment key rotation.** A DEK is scoped to one segment; segment rotation is the natural key-rotation boundary. Hole-punch and rewrite fight this.
- **Hybrid snapshot triggers cover two failure modes.** Log-count alone misses idle clusters whose log accumulates slowly but still needs replay bounds. Time alone misses chatty clusters that accumulate thousands of entries between ticks. Either triggers correctly in either regime.
- **4 MiB chunks balance RTT and memory.** 4 MiB is large enough that snapshot install isn't RTT-bound on a 10 Gbps link; small enough that a 16-chunk pipeline fits in 64 MiB of per-peer memory.
- **Manifest-at-end, not at start, lets the writer stream without seeking back.** Manifest writes appear at the tail, not rewritten into a header.
- **zstd + dictionary training beats lz4 by 3-5× on structured Raft state.** zstd decode is fast enough that compression CPU is not the bottleneck on modern cores.
- **Periodic checkpoints bound recovery when snapshots are sparse.** A cluster tuned for infrequent snapshots otherwise pays full-snapshot replay on every restart.

## Consequences (The "Negatives")

- **Minimum log waste ≈ segment size.** A 128 MiB segment that holds one entry past the snapshot boundary cannot be deleted until the next snapshot advances past it. Tuning the segment size against the snapshot cadence is a real cost.
- **Dictionary retraining on schema evolution.** When the state-machine schema changes meaningfully, the trained dictionary becomes stale and must be retrained, meaning one snapshot cycle runs at worse compression.
- **Manifest-at-end means the receiver must buffer hash state, not raw bytes.** The receiver holds per-frame hashes in memory until the manifest arrives to cross-check. This is ≤ 32 bytes × (snapshot_size / 4 MiB), trivial even for 100 GB snapshots.
- **Mid-stream failure restarts the install.** There is no random-access recovery on the wire protocol. A dropped connection mid-install discards the partial snapshot and restarts; this is the same cost as any consensus-transport reconnect.
- **Rejected alternatives explicitly:**
  - *Hole-punch compaction* — rejected for O_DIRECT interaction bugs and per-filesystem divergence.
  - *Rewrite-in-place compaction* — rejected for write amplification and coupling to state schema.
  - *Semantic snapshot boundaries* (state-machine-aware section markers) — rejected because they couple the snapshot wire format to state-machine schema and force wire-format migrations on every state evolution.
  - *lz4 compression* — rejected for substantially worse compression ratio on structured data; CPU cost of zstd decode is not a bottleneck.
  - *Time-only or count-only snapshot trigger* — rejected for the symmetric failure modes discussed above.

## Open Questions

All three items below are benchmark-bound and must be closed before this ADR moves from Proposed to Accepted. Each has a concrete measurement it depends on; none can be resolved by further discussion.

- **Exact segment size.** 128 MiB is a reasoned default but not benchmarked. Smaller segments reduce waste but increase file-open overhead during recovery; larger segments inverse. *Resolves via:* a recovery-time benchmark across {32, 64, 128, 256, 512} MiB segments on the reference fleet, measuring open/scan cost and post-compaction waste under representative write rates.
- **Snapshot threshold defaults.** The 30-second replay target fixes the shape; the concrete `snapshot_log_count_threshold` and `snapshot_time_threshold` values depend on measured per-entry replay cost on reference hardware. *Resolves via:* a replay-time benchmark that measures per-entry apply cost on the reference state machine, then back-solves the thresholds for a 30 s ceiling. Ship the measured defaults alongside this ADR's acceptance.
- **Compression ratio on real state-machine data.** The dictionary-trained zstd claim rests on generic structured-data benchmarks. *Resolves via:* a compression benchmark on ten representative captured state-machine snapshots from a non-trivial cluster, reporting ratio and throughput at levels 3 and 9 with and without a trained dictionary. If the measured improvement falls below 2×, revisit the lz4-rejection rationale.

## References
- ADR 007: Raft Implementation Strategy (extends)
- `plans/consensus/raft-core.md`
- `plans/gap/claude_answers_20260417.md` §3
