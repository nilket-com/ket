# Wendelon ADR 079: Idle Deployment Write Posture

**Status:** Proposed. **Migration:** `plans/impl/079_idle_deployment_write_posture.md`.
**Category:** Operational correction discovered by the first overnight `nilket local` dogfood
soak. This record preserves ADR 054's CDC progress contract and ADR 025's authoritative audit
chain while removing accidental write amplification from an otherwise idle local deployment.

## Evidence that forced the decision

The first overnight Polariton + Nilket session remained healthy: queries, reconnects, and charts
continued to work. Its logs and disk state nevertheless exposed two idle-path costs:

- the CDC locality heartbeat commits a no-op `TenantFinalization` every second for every quiet
  locality, so a one-locality node advances the Raft log continuously while no user is active;
- every heartbeat is currently materialized as `data.write_finalized`, even though it carries
  tenant zero, zero charged bytes, zero records, and no data mutation.

The live local profile measured on 2026-08-01 contained **89,554** records and **132,358,933 bytes**
in `audit.ndjson`; **88,792 records (99.1%)** were `data.write_finalized` heartbeat materializations.
Snapshots around log indexes 115k and 120k corroborated the control-log growth. This is not a data
loss or availability failure. It is an idle-write-posture failure found only by living with the
system overnight.

## Prior contracts that remain authoritative

- **ADR 054:** every source locality advances its finalized HLC at a bounded cadence, even when
  cold. The heartbeat remains unconditional; disabling it is not an option. The ordinary default
  remains 1,000 ms.
- **ADR 025:** every serious node keeps one local, append-only, tamper-evident NDJSON audit chain.
  Archival is additive. Local rotation or deletion is not smuggled into this cut.
- **ADR 532:** generated local-role configuration is a teaching artifact. Local smart defaults are
  explicit in that file, not hidden in `NodeBootstrapConfig::default()`.

## Decisions

**D1 — Heartbeat cadence becomes an explicit node setting; the ordinary default stays 1 second.**
`[tuning].cdc_locality_heartbeat_interval_millis` is a nonzero duration carried through
`NodeBootstrapConfig` into the existing heartbeat supervisor. Omission means ADR 054's 1,000 ms.
The generated `nilket local` Wendelon config writes **30,000 ms** visibly. At one quiet locality,
that reduces heartbeat commits by about 96.7% while preserving bounded watermark progress. A user
who chooses a different cadence sees and owns the CDC-latency trade-off.

**D2 — A CDC locality heartbeat is control-plane progress, not a data-access write.** The heartbeat
remains authoritative in Raft and still advances CDC frontiers. It does **not** materialize as
`AuditOperation::DataWriteFinalized` / `AuditSchema::DataAccess`, because no data was accessed or
mutated. The audit materialization tracker counts emitted audit events, not arbitrary Raft entries,
so this exclusion creates no false lag.

This decision **partially supersedes ADR 025 Decision 5** at one narrow boundary. ADR 025's phrase
"other Raft-committed state transitions" continues to require audit for transitions with operator,
security, tenant, schema, lifecycle, or data meaning. A deterministic periodic liveness no-op whose
only effect is advancing an internal CDC watermark is not an auditable operation merely because it
travels through Raft. Its replicated log entry remains the operational evidence of that progress.

**D3 — Heartbeat recognition is exact and shared.** Construction and recognition use one canonical
module-level contract. Recognition requires the full marker shape: tenant zero, zero charged bytes,
zero records, the heartbeat identity domain projected to the marker's locality, and the heartbeat
payload hash recomputed from locality + epoch + commit HLC. No broad predicate such as
`tenant_id == 0 && record_count == 0`, no message matching, and no request-position inference may
suppress an audit event. A near-miss remains a normal finalized write and is audited.

**D4 — Idle progress is observable without reading audit noise.** A bounded-label metric counts
committed CDC locality heartbeats. The metric is the direct numerator for idle-write-rate evidence;
audit bytes are no longer used as an accidental heartbeat counter. Existing Raft and snapshot
metrics remain the authority for total control-log behavior.

**D5 — Idle behavior joins the soak contract.** Wendelon's soak tooling gains a local-idle mode
using the existing evidence vocabulary and artifact names (`soak_config.json`, `samples.jsonl`,
`summary.json`). It records at least heartbeat deltas, audit byte/record deltas by operation, process
warnings, and deployment health. The overnight acceptance is:

- committed heartbeat rate is bounded by configured cadence and locality count;
- heartbeat-shaped `data.write_finalized` audit growth is zero;
- audit materialization lag remains zero;
- the deployment remains healthy and queryable;
- no idle-path WARN repeats.

Fast tests use a short/controlled clock; the real overnight remains product evidence, not a unit
test disguised as one.

**D6 — The authoritative local audit chain does not rotate in this cut.** Rotation sounded like the
obvious disk bound, but it changes ADR 025's verification model. Today `LocalAuditSink` recovers one
chain from `audit.ndjson`, `replay_audit_chain` accepts one path, backup/restore compares that chain,
and the operator runbook explicitly says the local baseline is not rotated merely because archive
shipping exists. Retained segment ordering, cross-segment verification, restart recovery, signed
trust anchors after deleting old segments, WORM policy, and fail-closed behavior require a separate
security ADR. Removing the 99.1% misclassification first gives that future decision honest data.

## Why this shape

Slowing heartbeat commits alone would reduce both Raft and audit growth but preserve a category
error: a no-op watermark marker would still claim to be a data write. Rotating the file alone would
hide the amplification while weakening the evidence contract. This decision fixes both causes at
their ownership boundaries: configuration controls progress cadence; the audit classifier records
only actual auditable operations.

The 30-second local value is a profile choice, not a product-wide CDC default and not credit toward
Toron's scratch-database promotion gates. Serious deployments that need low-latency CDC retain the
one-second default unless their operator explicitly changes it.

## Guards and controls

- Config parsing rejects zero and round-trips the ordinary default plus the explicit local value.
- An architecture guard requires the generated local profile to spell the cadence and forbids the
  node bootstrap path from reinstating the hardcoded constant.
- The heartbeat classifier has positive and negative controls in the same module: exact heartbeats
  are recognized; mutations of every discriminating field are not.
- The Raft audit tests prove an exact heartbeat emits no event, a near-miss emits
  `data.write_finalized`, and tracker lag stays zero in both paths.
- The soak evidence parser has a synthetic noisy-idle fixture proving it detects forbidden WARN and
  heartbeat-audit growth; a guard that cannot fail is not evidence.

Every guard failure cites ADR 079 and explains whether the boundary being crossed is CDC cadence,
heartbeat identity, or audit authority.

## Rejected and deferred

- **Disable heartbeats locally:** rejected; it violates ADR 054 and can stall a CDC watermark.
- **Make 30 seconds the global default:** rejected; a convenience profile does not silently weaken
  serious-deployment CDC latency.
- **Sample or globally suppress zero-row finalizations:** rejected; legitimate operations may be
  empty, and ADR 025's serious minimum remains 100%.
- **Infer heartbeat from tenant/locality alone:** rejected as an audit-evasion aperture.
- **Rotate/truncate `audit.ndjson` now:** deferred to a security ADR with a multi-segment verifier and
  explicit retention/trust-anchor semantics.
- **Repair dormant grafted CI workflows here:** deferred to the separate monorepo CI follow-up; this
  cut may add the idle command to the soak scripts but does not relocate workflow files.
