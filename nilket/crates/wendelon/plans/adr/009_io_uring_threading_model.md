# ADR 009: `io_uring` Execution Topology

## Status
Accepted (amended 2026-04-22)

## Context
In `ADR 008`, Wendelon committed to `tokio` plus the low-level `io-uring` crate rather than a thread-per-core runtime such as `tokio-uring`. The original version of this ADR chose a simple statement: one `io_uring` instance and one dedicated blocking I/O thread per physical NVMe device.

That statement captured the eventual scale shape but overcommitted in two ways:

- it assumed topology-aware per-device mapping as an immediate invariant rather than a staged capability
- it did not distinguish the WAL durability path from the slab durability path

The code-level review made the real problem explicit: the immediate architectural mismatch is not "not enough rings per NVMe" in the abstract, but that WAL durability and slab durability were coupled through shared execution resources. The hardware strategy is now clearer:

- commodity hardware is the default deployment profile
- premium multi-device NVMe hardware must still be supportable on day 1
- day-2 work is topology-aware automation and saturation tuning, not basic compatibility

## Decision

Wendelon will use an `io_uring` execution topology organized by **durability class first** and **device topology second**.

1. **The WAL durability path is isolated from the slab durability path.**
   The Raft WAL must not share its primary `io_uring` execution worker with steady-state slab I/O.

2. **At minimum, Wendelon provides:**
   - one dedicated WAL `io_uring` worker
   - one or more dedicated slab `io_uring` workers

3. **Premium hardware is supported on day 1 through explicit configuration.**
   Multi-device and higher-core-count deployments must be supportable through documented configuration for:
   - WAL worker placement
   - slab worker count
   - queue depths
   - device affinity where known

   Day-1 support means correct behavior and a documented deployment path. It does not require automatic topology discovery or benchmark-backed saturation claims yet.

4. **Per-device slab worker mapping is a supported optimization path, not the baseline contract.**
   Where hardware topology is known and the deployment benefits from it, slab workers may map one-to-one or many-to-one onto physical devices. That mapping may initially be operator-configured before it becomes automatically derived.

5. **Remaining blocking metadata operations must not interfere with WAL latency.**
   If certain file-system operations still require blocking threads temporarily, they must be isolated so they cannot stall the WAL durability path.

6. **The runtime must export direct execution-topology metrics.**
   At minimum:
   - submission-queue depth
   - completion-queue depth
   - WAL append latency
   - WAL flush latency
   - slab append latency
   - worker saturation / queueing indicators

## Rationale

- **Durability class is the real first-order separation.** The WAL exists to preserve cluster coordination correctness. Slab I/O exists to carry the data path. Sharing them defeats the purpose of the architecture even on a single fast device.
- **Commodity default and premium support can coexist.** A small deployment can run with a simple configuration while a larger deployment still gets an explicit, supported path onto better hardware.
- **Topology-aware automation is valuable but not the initial truth boundary.** The critical requirement is that better hardware is supportable without redesign. Automatic placement can come after the separation model is correct.
- **WAL latency is the canary.** If the WAL path cannot be insulated from slab pressure, Raft tail latency becomes a hidden function of data-path pressure, which is exactly the coupling this ADR is meant to avoid.
- **Metrics are part of the architecture here.** Without direct visibility into queue depth and worker saturation, operators cannot tell whether the topology is working as intended.

## Consequences (The "Negatives" / Rejected Ideas)

- **The execution model is more complex than a single global ring.** Accepted because a single global ring makes the most important interference pattern unavoidable.
- **Configuration surface increases.** Operators may need to specify worker counts, queue depths, and device affinity before auto-tuning lands. Accepted because explicit config is cheaper than pretending the system is topology-agnostic.
- **Commodity deployments may underutilize very high-end hardware initially.** Accepted because support on day 1 is about correctness and deployability, not instant saturation.
- **Rejected alternative: one global `io_uring` instance for the entire node.** Rejected because it couples WAL and slab durability too tightly.
- **Rejected alternative: one `io_uring` per Tokio worker.** Rejected because it complicates synchronization and pollutes the async scheduler with disk-submission concerns.
- **Rejected alternative: "one ring per physical NVMe device" as the only valid topology.** Rejected because it is too rigid for the current support envelope and says more than the system can honestly guarantee today.

## References
- ADR 008: Dependency Selection and Async Runtime Model
- ADR 015: Segmented WAL and Snapshot Lifecycle
- ADR 016: Durability Model — Explicit fsync Group Commit
