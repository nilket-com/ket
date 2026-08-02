# Wendelon ADR 080: Local-Profile Lease-Renewal Posture

**Status:** Proposed 2026-08-02. **Migration:**
`plans/impl/080_local_profile_lease_renewal_posture.md`.
**Category:** Operational correction discovered by the second overnight `nilket local` dogfood
soak. This record preserves per-range fencing while removing a serious-deployment renewal cadence
from an explicitly single-node convenience profile.

## Evidence that forced the decision

ADR 079 quieted CDC heartbeat audit noise and exposed the next periodic writer. During the
2026-08-02 overnight run, Wendelon recovered an aged snapshot at log index 209,999, replayed to
211,550, and remained healthy for more than eleven hours. The log then advanced from 211,550 to
254,999: **43,449 entries in 40,731 seconds = 1.0667 entries/second**. Snapshots landed every 5,000
entries, approximately every 78 minutes.

The rate decomposes exactly:

- one `AcquireLease` renewal per second for the one primary range;
- two CDC boundary-locality heartbeats every 30 seconds (`2 / 30 = 0.0667/second`);
- only startup-boundary rounding beyond those named writers.

Source inspection confirms the first term. `LEASE_TTL_MS` is 3,000 ms, renewal is hardcoded to
`TTL / 3`, and `refresh_leases_once` commits a newly signed `WendelonRequest::AcquireLease` for every
owned primary range on every tick. The state machine overwrites the same holder/range with a later
`lease_start`, `lease_expiry`, and signature. This is a legitimate replicated fencing transition,
not a duplicate no-op and not an audit-classification error.

The same soak proves ADR 079: the configured 30-second CDC cadence is active, heartbeat-shaped
`data.write_finalized` audit growth is zero, and the residual audit chain roots remain within the
one-per-minute ADR 025 budget. ADR 080 therefore fixes an independent writer; it does not reopen or
weaken ADR 079.

## Safety contract that remains authoritative

A data node may coordinate writes or serve a lease-gated read only while it holds a signed lease
for the exact locality range and epoch. A lease is HLC-bounded, committed by the Raft leader, and
invalid when its epoch is superseded or its expiry is reached. Extending the expiry changes
authoritative fencing state and must remain a Raft commit.

Lease TTL also bounds how long a dead holder can block a different successor in the same epoch.
That cost is real in a multi-node deployment. A slower CDC heartbeat delays a watermark; a longer
lease can delay failover. The local-profile choice below is safe only in its stated topology and is
not a general performance default.

## Decisions

**D1 — Lease TTL becomes one explicit node setting; renewal cadence remains derived.** Add
nonzero `[tuning].data_ring_lease_ttl_millis`. Omission preserves the ordinary **3,000 ms** value.
Renewal cadence is always derived as `TTL / 3`; there is no independently configurable renewal
interval that could exceed or silently diverge from the lease authority's expiry window. Parsing
must reject values that cannot be represented by the fixed-width wire TTL or cannot produce a
nonzero renewal interval.

**D2 — One setting feeds both grant paths.** The configured TTL is carried through
`NodeBootstrapConfig` to:

- the local lease manager that signs and commits self-owned renewals; and
- `WendelonControlServer`, which grants remote `AcquireLease` requests.

Those paths currently share one constant and must continue sharing one authority after the value
becomes configurable. No role-specific shadow default, second environment variable, or direct
constant use may split them.

**D3 — The generated local profile writes 90,000 ms visibly.** `nilket local` writes
`data_ring_lease_ttl_millis = 90000` in its generated Wendelon `[tuning]` table. The resulting
30-second renewal cadence keeps a valid lease warm while reducing the known lease writer by about
96.7%. Existing retained profiles are not silently rewritten; their owner adds the line explicitly.

The trade is narrow and topology-bound. `nilket local` has one voter/data node, one supervised
profile owner, and no different successor to fence during ordinary operation. A restart with the
same holder may renew the same range immediately. If the local profile ever admits a second node or
distinct successor identity, its 90-second value must be reconsidered in a new record before that
topology ships. The serious-deployment default remains 3 seconds.

**D4 — Idle-soak evidence bounds the Raft substrate, not only known symptoms.** Extend local-idle
soak evidence with:

- total Raft entries applied during the measured window;
- successful lease-grant/renewal delta;
- committed CDC-heartbeat delta;
- primary-range count and configured lease TTL/derived renewal interval; and
- a residual count after subtracting every evaluator-owned or named periodic writer.

After the readiness baseline and a bounded observation-race allowance, the residual must be zero.
The evaluator also enforces cadence-derived maxima for leases and CDC heartbeats. A synthetic
unexpected-Raft-writer fixture must fail even when audit growth remains within budget. Audit chain
roots remain part of the independent audit budget; they are not Raft entries and must not be used to
explain Raft growth.

**D5 — Lease-on-demand is the recorded destination candidate, not smuggled into this cut.** A future
profile may hold no lease while idle, coalesce concurrent first-use acquisition, acquire on the
first lease-requiring operation, and let authority lapse after inactivity. That posture can make
idle lease appends literally zero, but it changes request latency, retry/refusal semantics,
acquisition stampede control, and the meaning of an always-ready data gate. It requires its own
evidence and decision. ADR 080 ships the smaller explicit-TTL floor without foreclosing it.

**D6 — Snapshot and audit policies do not change.** The 5,000-entry snapshot trigger accurately
reported the writer and remains unchanged. Raising it would hide amplification rather than remove
its cause. Lease renewals remain authoritative Raft evidence; this record neither invents audit
materialization for them nor suppresses any existing audit event.

## Expected local steady state

For today's one-range, two-boundary-locality profile:

- lease renewals: `1 / 30 seconds`;
- CDC heartbeats: `2 / 30 seconds`;
- total named idle Raft rate: `0.1 entries/second`;
- a 5,000-entry snapshot: approximately every 13.9 hours rather than every 78 minutes.

This is a bounded reduction, not a claim that the local deployment has reached zero-write idle.
Lease-on-demand is the only named path here to literal zero lease renewal.

## Guards and controls

- Config tests pin omission to 3,000 ms, accept the explicit local 90,000 ms value, and reject zero,
  overflow, and values that derive a zero renewal interval.
- A local-profile rendering guard requires the visible setting and proves a synthetic drifted
  renderer fails.
- An architecture guard forbids `LEASE_TTL_MS` from being passed directly to either runtime grant
  path and requires both paths to receive the same configured field.
- Deterministic scheduler tests prove exactly one immediate seed, no immediately-ready duplicate
  timer renewal, delayed missed-tick behavior, and an epoch change renewing promptly without an
  adjacent stale timer renewal.
- The soak evaluator has positive controls for excessive lease rate, excessive total Raft growth,
  and an unknown residual writer, alongside ADR 079's audit/WARN controls.
- Multi-node lease tests retain the ordinary 3-second default and prove a conflicting holder cannot
  acquire before expiry while the same holder may renew.

Every new guard cites ADR 080 in its failure message and includes a fixture that makes it fail.

## Rejected and deferred

- **Raise the snapshot threshold:** rejected; it hides the writer while retaining WAL and SSD cost.
- **Skip Raft for same-holder renewals:** rejected; expiry extension and its signature are fencing
  authority, not cache state.
- **Grant an indefinite local lease:** rejected; it makes topology evolution and abnormal ownership
  mistakes fail open.
- **Make 90 seconds the ordinary default:** rejected; it can impose real same-epoch failover delay
  in serious deployments.
- **Configure TTL and renewal independently:** rejected; unsafe combinations become representable.
- **Special-case `node_count == 1` inside Wendelon:** rejected; the role runtime consumes explicit
  configuration and does not infer a deployment brand from current membership.
- **Renew only after observed activity:** deferred with lease-on-demand; both require request-path
  acquisition/retry semantics and evidence beyond cadence tuning.
- **Rotate or truncate the control WAL/audit chain:** rejected in this cut; neither fixes the writer.

## Consequences

The local profile remains instantly ready and continuously fenced, but its safety-bearing renewal
cost becomes visible and proportionate to a one-node workstation deployment. Serious deployments
retain their current fencing window. The soak contract graduates from checking the last discovered
noise source to checking the append-only substrate itself, so the next unknown periodic writer is a
red run rather than another morning's subtraction exercise.
