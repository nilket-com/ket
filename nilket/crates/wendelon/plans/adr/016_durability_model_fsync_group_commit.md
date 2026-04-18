# ADR 016: Durability Model — Explicit fsync Group Commit

## Status
Proposed

## Context
Wendelon must provide a precise durability contract: when an RPC acknowledges a write, the bytes are durable on media and will survive a power loss. That contract interacts with three realities:

1. **Linux O_DIRECT alone does not guarantee media durability.** O_DIRECT bypasses the page cache but does not force the drive's write-back cache to flush. Without an explicit `fsync`/`fdatasync`, a sudden power loss can lose acknowledged writes.
2. **Power-Loss-Protection (PLP) NVMe is heterogeneous.** PLP guarantees vary across vendors, firmware revisions, and drive models. A fleet-wide "trust the hardware" bet loses its correctness story the moment an un-PLP drive enters the deployment envelope.
3. **fsync is expensive.** Per-write fsync collapses throughput. The classical answer — group commit — amortizes one fsync across many concurrent writes.

ADR 007 (openraft) declared that Wendelon owns durability. This ADR specifies how.

## Decision

1. **Explicit `fsync`/`fdatasync` at consensus-ack boundaries.** Every Raft log write that produces an ack to the proposer has its containing WAL segment `fdatasync`ed before the ack is sent. `fdatasync` is sufficient for in-place appends to segment files of known size; `fsync` is used on paths where metadata must also be durable (segment creation, rotation, truncation).

2. **Parent-directory fsync on file namespace changes.** Any operation that changes the filesystem namespace — segment creation, rename-into-place during rotation, deletion during compaction, snapshot file creation and deletion — is followed by an `fsync` of the parent directory before the operation is considered durable. Without directory fsync, Linux ext4/xfs can lose a just-created file or resurrect a just-deleted one across crash, even when the file's own data was synced. This is a mandatory part of the durability contract, not an optimization.

3. **Adaptive group-commit window.** Concurrent fsyncs coalesce into one call. The window is adaptive:
   - Floor: 200 µs (low-load latency).
   - Ceiling: 2 ms (p99 cap).
   - Shape: `window = clamp(inflight_writes × per_write_cost_estimate, 200µs, 2ms)`.
   Under low load the window collapses to the floor — single-writer latency is bounded. Under high load the window expands toward the ceiling — throughput scales with coalescing.

4. **Immediate flush on administrative barriers.** Membership changes, epoch transitions, lease commits, backup-barrier commits, and any admin-initiated "flush now" control RPC bypass the group-commit window and fsync immediately. Administrative operations must have bounded latency regardless of data-plane load.

5. **No PLP assumptions anywhere in the durability contract.** The durability story is the same on PLP NVMe, non-PLP NVMe, SAS SSDs, and rotational media. Hardware that is faster benefits from reduced fsync latency; hardware that is slower pays the cost; no path to correctness relies on a drive's PLP capacitor.

## Rationale

- **Correctness is non-negotiable; throughput is recoverable.** The cost of a PLP-assumption bug is data loss on acknowledged writes under a rare but real failure mode (un-PLP drive + power loss). The cost of "always fsync" is throughput, and group commit largely recovers it.
- **Adaptive windows dominate fixed windows.** A fixed 500 µs window punishes low-load latency and under-coalesces high-load throughput. The clamp-based shape keeps both regimes in their own sweet spot without operator tuning.
- **Administrative barriers are rare and latency-sensitive.** Forcing them through the adaptive coalescer adds up to 2 ms to a membership change that an operator is watching; bypassing costs nothing in steady state.
- **fdatasync over fsync where safe.** Segment files have stable size once created; fdatasync skips the metadata flush that fsync forces. One fewer syscall round per commit at the cost of being precise about when file metadata actually changes (segment rotation).

## Consequences (The "Negatives")

- **Single-writer latency floor ≥ 200 µs.** A workload with one uncoalesced writer pays the floor on every commit. This is by design — the floor is the coalescing window — but it is a real number that operators should know.
- **CPU cost of fsync syscalls.** fsync is not free. Under heavy write load the fsync thread becomes a real CPU consumer. Addressed architecturally by keeping the fsync path off the RPC handler thread.
- **Scaling-factor calibration is benchmark-derived.** The `per_write_cost_estimate` that drives window expansion depends on drive model and filesystem. Defaults ship; operators may need to tune for non-reference hardware.
- **PLP hardware doesn't "go faster."** Operators with PLP-equipped fleets may expect to disable fsync for throughput wins; this ADR says no. The win is realized indirectly: PLP drives complete fsync faster, so the coalescer collapses faster under load.
- **Rejected alternatives explicitly:**
  - *Trust-PLP durability* — rejected; heterogeneous fleets, correctness-first.
  - *Fixed group-commit window* — rejected; either too slow at low load or too shallow at high load.
  - *No group commit (per-write fsync)* — rejected; throughput collapse.
  - *Background flush with periodic fsync* — rejected; violates "durable before ack" contract.

## Open Questions

Benchmark-bound; must be closed before this ADR moves from Proposed to Accepted.

- **Exact scaling-factor constants inside the clamp.** The shape (`clamp(inflight × per_write_cost, floor, ceiling)`) is settled; the `per_write_cost_estimate` value and whether it should itself be a rolling measurement or a compile-time constant are benchmark-resolvable. *Resolves via:* a group-commit sweep on the reference fleet measuring p50/p99 commit latency and sustained write throughput at fixed `per_write_cost_estimate` values of {25, 50, 100, 200} µs, and at rolling-measurement configurations. Pick the configuration that holds p99 ≤ 2 ms at 90% of the peak sustained throughput. Ship measured defaults with this ADR's acceptance.

## References
- ADR 007: Raft Implementation Strategy
- ADR 015: Segmented WAL and Snapshot Lifecycle (fsync granularity)
- ADR 009: io_uring Threading Model (fsync queue placement)
