# Brainstorm 008: External Compute Lifecycle Orchestration

**Status:** Exploratory / Future Side Quest

## The Premise

Toron and Wendelon should remain unable to directly spawn, terminate, resize, or
otherwise control EC2 instances. Cloud lifecycle authority belongs to an external
actuator.

That does not mean Toron should be silent. A compute system can expose precise,
bounded, authenticated state and intent so a separate actuator can make capacity
decisions without scraping logs, reverse-engineering scheduler pressure, or
coupling cloud credentials into the query engine.

The design question is: if Kubernetes is not assumed, what communication surfaces
does Toron need so a containerd/systemd/EC2 actuator can safely add, drain, and
remove compute capacity?

## Non-Goals

- Toron does not call EC2, Auto Scaling, Kubernetes, Terraform, or cloud vendor
  APIs.
- Wendelon does not call EC2, Auto Scaling, Kubernetes, Terraform, or cloud
  vendor APIs.
- Storage durability, Raft membership, and table ownership are not hidden behind
  a generic "node lifecycle" command.
- Scale hints are not commands. They are observable intent with TTLs.
- No YAML control-plane dependency is implied by this brainstorm.

## Core Split: Authority vs. Evidence

The external actuator has authority over instances and containers.

Toron has authority over query admission, fragment scheduling, worker cordon,
worker drain, and safe task shutdown.

Wendelon has authority over storage, lease, durability, retention, and consensus
safety.

The actuator should never need to infer query safety from cloud state alone. It
should ask Toron whether compute can be drained and ask Wendelon whether a node
is safe to remove from any durable role.

## Passive Surfaces

Passive surfaces are read-only. They should be enough for an external actuator
to understand current pressure and safety without issuing commands.

### Toron Status

- Worker registry: identity, endpoint, protocol version, build fingerprint.
- Worker role and capabilities: coordinator, compute-only, storage-adjacent
  compute, memory class, supported execution features.
- Health and readiness: live, ready, degraded, draining, cordoned.
- Scheduler state: active fragments, queued fragments, pending exchanges,
  blocked dispatches, retry counts, cancel counts.
- Admission pressure: queue depth, wait time, rejection count, saturation reason,
  tenant-scoped budget pressure.
- Resource pressure: memory reservations, spill pressure, CPU saturation if
  available, task count, blocked spawn count.
- Query lifecycle: in-flight query count, oldest query age, draining query count.
- Coordinator-to-worker latency and recent failure rate.

### Wendelon Safety Signals

- Raft role, membership status, lease ownership, and leadership transfer state.
- Durable write pressure and quorum health.
- Table-query pressure when Wendelon is serving pushdown scans.
- Storage locality and L1/L2 residency hints.
- Retention, compaction, and migration backlog when removal would be expensive.
- Node replacement or restore state.

These signals can live in existing observability surfaces first, then become
system tables once the system introspection path is rich enough.

## Active Toron Surfaces

Active surfaces are commands into Toron. They affect query/task lifecycle, not
cloud lifecycle.

### Worker Registration

Compute workers need a bounded registration path:

- worker identity and tenant authorization posture
- advertised protocol versions
- execution capabilities
- memory and concurrency budget
- optional locality tags
- heartbeat or lease TTL

Registration is not durable storage membership. Compute-only workers should be
cheap to add and cheap to lose.

### Cordon

`cordon_worker` stops new fragment assignment to a worker while allowing existing
work to continue.

Useful reasons:

- external actuator wants to terminate or replace an instance
- worker is unhealthy but still finishing useful work
- rolling upgrade
- autoscaler is testing shrink capacity

### Drain

`drain_worker` asks Toron to move a worker toward zero in-flight work.

The response should report:

- accepted or rejected
- current in-flight count
- blocking query ids or bounded opaque handles
- estimated remaining drain window if known
- whether cancellation is required to meet the requested deadline

Drain is the most important handoff. The actuator can terminate only after Toron
reports that the worker is empty or after policy chooses forced termination.

### Evict

`evict_worker` marks a worker unavailable after missed heartbeats or after an
external termination event.

Eviction should:

- prevent new scheduling
- fail or retry affected fragments through existing cancellation/retry paths
- avoid pretending that any durable Wendelon role has been removed

### Weight or Capacity Update

`set_worker_weight` or `update_worker_capacity` lets an actuator bias scheduling
without killing or recreating a worker.

This can support:

- partially saturated hosts
- mixed instance classes
- thermal or noisy-neighbor degradation
- pre-drain traffic reduction

## Scale Hint Surface

Toron can emit scale hints. A scale hint is evidence, not authority.

Sketch:

```text
ScaleHint {
  scope: global | tenant | workload_class,
  direction: add | remove | hold,
  reason:
    admission_pressure
    queue_latency
    memory_pressure
    idle_capacity
    drain_requested
    worker_failure_rate
    protocol_skew,
  magnitude: u32,
  ttl_ms: u64,
  observed_at_hlc,
  sequence,
}
```

Important properties:

- TTL-bounded so stale pressure cannot endlessly spawn machines.
- Monotonic sequence so an actuator can dedupe and order hints.
- Reason-coded so policy can distinguish "add memory-heavy workers" from
  "replace unhealthy workers."
- Signed or served over mutually authenticated transport.
- Never interpreted as "start EC2 instance now" inside Toron.

## Actuator Responsibilities

The external actuator is allowed to:

- start instances or containers
- stop instances or containers
- choose instance families
- enforce spend limits
- talk to containerd, systemd, EC2, or another provider
- map Toron scale hints into concrete capacity changes
- decide when to ignore hints

The actuator should be expected to call Toron for cordon/drain and Wendelon for
durable-role safety before terminating anything that may carry state.

## Minimal Protocol Shape

Start with boring primitives:

1. Read Toron status and metrics.
2. Read Wendelon safety status and metrics.
3. Register compute worker.
4. Cordon worker.
5. Drain worker.
6. Evict worker.
7. Publish or poll Toron scale hints.

This is enough to support a small containerd-backed actuator without requiring
Kubernetes concepts to leak into Toron.

## Failure Modes to Design For

- Actuator misses a scale hint.
- Actuator replays an old scale hint.
- Worker is terminated before drain completes.
- Worker registers with stale protocol version.
- Worker is alive but overloaded and cannot accept more fragments.
- Coordinator dies while a drain is in progress.
- Wendelon says a node is not safe to remove even though Toron is fully drained.
- Toron says compute pressure is high while cloud budget policy refuses scale-out.

These should all be explicit states, not implicit log archaeology.

## Open Questions

- Should scale hints be an event stream, a polled system table, or both?
- Should Toron expose per-tenant hints, or should tenant-level policy stay
  entirely inside the actuator?
- What is the smallest identity model for compute-only workers that does not
  weaken worker-task authentication?
- Can the weak-ring idea from Brainstorm 003 carry compute-only placement without
  introducing durable membership churn?
- Should Wendelon expose a single "safe_to_terminate" aggregate, or is that too
  lossy for storage safety?
- How much of this belongs in `nilket inspect` before any active lifecycle
  command exists?

## Possible Promotion Path

1. Passive status only: expose worker registry, readiness, and scheduler pressure.
2. Compute worker registration and heartbeat TTL.
3. Cordon/drain/evict commands with no cloud integration.
4. Scale hint event or table.
5. External actuator prototype using containerd/systemd.
6. Optional EC2 adapter outside Toron/Wendelon.

