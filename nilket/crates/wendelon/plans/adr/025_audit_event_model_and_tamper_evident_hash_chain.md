# ADR 025: Audit Event Model and Tamper-Evident Hash Chain

## Status
Proposed

## Context
Wendelon targets regulated-industry deployments. Every admin action, every break-glass use, every tenant-lifecycle mutation, and every authentication failure must be attributable, structured, shippable to a SIEM, and tamper-evident. Audit is not observability — observability is for operators diagnosing the cluster; audit is for security/compliance reviewers reconstructing the record of who did what.

Forces in play:
- ADR 018 already emits audit records for break-glass actions and role-bound RPCs. ADR 021 already requires every `TenantAdmin` mutation to emit an audit record. Audit is therefore already a cross-cutting concern; it needs a canonical schema before the call sites proliferate.
- A structured event stream must exist from day one. Shipping only a local file first means every later sink integration (SIEM, syslog, Kafka) touches every call site.
- Tamper evidence must be cryptographic, not just "we took a daily backup." Regulators accept a hash chain with exposed head; they do not accept "trust our backup process."
- The chain primitives must come from ADR 019 (blake3 for the chain hash; signature primitives for the periodic signed root). Rolling a bespoke hash construction for audit is unnecessary and dangerous.
- Retrofitting tamper evidence is expensive — every existing audit record becomes un-chained, which is indistinguishable from tampering. Day-one hash chain is cheap; day-two hash chain is an operator-facing migration.

## Decision

1. **Canonical structured audit event schema from day one.** Every audit record is a typed structured event with a fixed field set. Free-form "log messages" are not audit records.

   Required fields on every event:
   - `event_id` — ULID, monotonic, process-local.
   - `timestamp_ns` — wall-clock nanoseconds at emission (observational only; ordering comes from the hash chain, not the timestamp).
   - `sequence_number` — u64, strictly monotonic per emitting node, never reused across process restarts (persisted across restart via the local audit store).
   - `node_id` — emitting node's ring position.
   - `tenant_id` — the target tenant of the action, or `null` for cluster-level operations (ADR 021 §2).
   - `actor` — structured: `{cert_subject, cert_key_id, role, actor_tenant_id | null}`; sourced exclusively from the authenticated mTLS peer (ADR 018).
   - `operation` — string identifier from a closed enum (e.g., `tenant.create`, `tenant.delete`, `membership.add_node`, `key.rotate.tenant_kek`, `auth.handshake_failed`, `break_glass.used`).
   - `parameters` — typed parameter map specific to the operation. Required fields per operation are fixed at the schema level.
   - `outcome` — `Allowed | Denied | Error | BreakGlass`. `BreakGlass` is a distinct outcome, not a flag on `Allowed`.
   - `authority` — `RaftLog | NodeLocal`. Identifies which store is the primary durable record for this event (see decision §5).
   - `raft_log_index` — u64 Raft log index when `authority = RaftLog`; `null` when `authority = NodeLocal`.
   - `prev_hash` — 32 bytes, `hash_func(previous_record_canonical_bytes)` (blake3 by default, SHA-256 in FIPS build; see decision §3).
   - `schema_version` — u16, starts at 1. Schema evolutions are additive; breaking changes bump the version and gate on a Raft-committed migration.

   Canonical serialization for hashing is a fixed-width CBOR encoding with canonical ordering (RFC 8949 §4.2). The serialization that is hashed is not necessarily the serialization that is emitted to the sink; sinks may project to NDJSON for human readability as long as the canonical form remains reproducible.

2. **`AuditSink` trait from day one with three initial implementations.** The trait makes every audit call site sink-agnostic:
   - **`LocalNdjson`** (default) — newline-delimited JSON to a local rotated file. Always enabled; the per-node chain is maintained on this file.
   - **`Syslog`** — AF_UNIX socket, RFC 5424 structured data. For deployments that ship audit via host-level syslog to a SIEM.
   - **`Kafka`** — feature-flagged (`audit-kafka`). For deployments that stream audit directly to a Kafka topic consumed by a SIEM.

   The `LocalNdjson` sink is never disabled. Other sinks are additive: `Syslog` and `Kafka` receive the same event stream the local file receives. Failure semantics depend on the event's `authority` (decision §5), not on which sink is writing — see §7. Off-box sink (`Syslog` / `Kafka`) failure never blocks the emitting RPC; failures are themselves audited on the next successful emission.

   What the `LocalNdjson` sink is and is not: it is the per-node durable store of the local hash chain (the materialization for `RaftLog`-authority events, the authoritative record for `NodeLocal`-authority events). It is not a claimed cluster-wide authority. The authoritative store for each event is determined by `authority` in the record itself, per §5.

3. **Per-record hash chain with exposed head.** Each record carries `prev_hash = hash_func(canonical_bytes(previous_record))`. The first record on a node's local local chain has `prev_hash = hash_func("wendelon-audit-genesis-v1" || node_id)` — a deterministic genesis tag, not a zero value. The `hash_func` is `blake3` by default, but is switched to `SHA-256` under the FIPS build profile (per ADR 026).

   The chain head — `(sequence_number, hash_func(canonical_bytes(latest_record)), hash_algorithm_tag)` — is exposed via a dedicated control RPC: `AuditChainHead.get(node_id) -> ChainHead`. Auditors snapshot heads from each node on a cadence they choose; any gap, rewrite, or divergence from the snapshotted head is detectable. The head RPC is available to `operator` and `read_only_observer` roles (ADR 018).

   Chains are per-node, not cluster-wide. A cluster-wide chain would require every audit emission to acquire Raft consensus, which collapses throughput for events that are already locally durable. Per-node chains with a cross-node roll-up (decision §4) give tamper evidence without serializing emissions.

4. **Periodic signed-root roll-up.** At a configurable cadence (default 60 seconds), every node emits a `chain_root` event containing `(node_id, sequence_number, chain_head_hash, signature)`. The signature is produced using the node's identity key (the mTLS certificate's signing key) via an AEAD-agnostic signature primitive from the aws-lc-rs backend (ADR 019). The signed root is itself an audit record and is chained like any other.

   The roll-up gives auditors a cross-node anchor: a snapshot of all signed roots at time T is a cryptographic attestation that no earlier record on any node has been altered without detection. Roll-up cadence is a tuning knob; faster means tighter detection window, slower means less overhead. 60 seconds is the default because it bounds "how far back can an attacker rewrite" to a minute for the lifetime of the cluster.

5. **Two authorities, one materialization model.** Audit events have two possible primary durable stores, and the per-node local chain is always a materialization (not always the authority). Cluster-wide atomicity across the Raft log and the local file is not claimed; the authoritative record lives in exactly one place per event and the node-local chain replays forward from that authority.

   - **`authority = RaftLog` (state-machine mutations).** Every Raft-committed state-machine mutation that produces an audit event (tenant lifecycle, membership change, KEK rotation) emits the audit record as part of the same Raft log entry that applies the mutation. **The Raft log is the authoritative ledger** for these events. Each node, as it applies the entry locally, materializes the event into its own per-node chain with that node's next `sequence_number` and `prev_hash`. The materialization is *per-node* precisely because the chain is per-node (decision §3); the cross-node cluster ordering is recovered from `raft_log_index`, which every node agrees on.
   - **`authority = NodeLocal` (non-Raft events).** Handshake failures, RPC denials, OCSP/CRL validation results, break-glass actions on non-state-machine RPCs, and any event that is not a state-machine transition are written directly to the node-local chain via the `AuditSink` interface. **The local chain is the authoritative record** for these events; there is no upstream log to replay from.

   Materialization rule for Raft-authority events: on apply, the node appends the event to its local chain. If the local append fails but the Raft commit succeeded, the Raft log retains the record — the node's chain is **behind** its authoritative state, and the replay-forward mechanism catches it up on the next apply cycle or on restart. A Raft-committed event cannot be lost by a local chain failure; it can only be lagged.

   What this gives: every state-machine transition that committed has a matching authoritative audit record (in the Raft log) by construction. Every node's local materialization of that event is eventually consistent with the Raft log. Non-Raft events have their own per-node authority and their own fail-closed semantics (decision §7). The "did we forget to audit that?" failure mode is eliminated for Raft-authority events without claiming cross-authority atomicity.

6. **Retention and rotation.** The local NDJSON file rotates at 1 GiB or 24 hours, whichever first. Retained local copies are preserved per the cluster's retention configuration (default 90 days; tunable). Rotation preserves the chain across file boundaries: the first record of a new file carries `prev_hash` of the last record in the previous file.

   Off-box shipping (`Syslog`, `Kafka`) is the long-term retention path. The local file is the short-term chain store and the fall-back if an off-box sink is unavailable.

7. **Failure semantics vary by authority.** The fail-closed rule applies where the local chain is the authoritative store; the materialization-lag rule applies where the Raft log is the authority:

   - **`authority = NodeLocal` events** (handshake failures, RPC denials, break-glass on non-state-machine RPCs): if the local NDJSON write fails, the emitting RPC **fails closed** (`AuditUnavailable`; the operation is rejected). The local file is the only durable record for these events; losing it is losing the event, which is unacceptable. An auth-related operation that cannot be audited has not happened.
   - **`authority = RaftLog` events** (state-machine mutations): a failed local append does **not** reject the operation. The Raft commit itself is the durable record; the local chain is a materialization that will catch up via replay-forward. A node whose materialization is lagging exposes the lag via a `/ready` field (ADR 027) and a metric (`audit_materialization_lag_entries`); a node cannot mark itself fully ready while its materialization lag is non-zero.
   - If an off-box sink fails (syslog, Kafka), the RPC **succeeds and the failure is itself audited** on the next successful emission. Off-box sinks are best-effort shipping, not primary storage.

   "Audit-first" is the correct posture: an operation that cannot be audited has not happened.

## Rationale

- **Structured schema from day one prevents migration hell.** Free-form logs tempt every call site into its own format. Once shipped, you cannot ask an operator to re-migrate their audit archive to a new structure; the best you can do is add a new version and carry both forever. Pay the structural cost on day one.
- **AuditSink trait matches the shape of the problem.** Three sinks cover the pilot's real integration space (local file for forensics, syslog for host-level SIEMs, Kafka for streaming SIEMs). A trait makes adding a fourth sink later zero-churn at call sites.
- **Per-node chains with signed roll-up beats cluster-wide chains on cost and on correctness.** Cluster-wide chains require consensus on every audit emission, which either collapses throughput or drops audit records that never made consensus. Per-node chains give the same tamper-evidence property (at the per-node granularity) and the signed roll-up gives the cross-node anchor that auditors actually need.
- **blake3 is the right chain primitive.** Fast on modern CPUs, parallelizable, cryptographically conservative, already a dependency for content-addressed paths. Picking SHA-3 or SHA-256 adds a second hash primitive with no correctness upside.
- **Two authorities with explicit materialization beats a false atomicity claim.** An alternative framing — "the Raft commit and the local chain append are atomic" — is not implementable: they are two different durability domains. The chosen model makes the Raft log authoritative for state-machine mutations and the local chain a per-node materialization, with replay-forward as the catch-up mechanism. Non-Raft events live entirely in the local chain and carry fail-closed semantics where it matters. The split is honest and the record is never lost: it is either authoritative where written, or replayable from where it is.
- **Fail-closed on local NDJSON (for node-local authority events) matches the regulatory posture.** A compliance story that reads "most auth failures are audited" is not a compliance story. For events whose only authoritative record is the local chain, audit-or-not-operate is the correct rule.

## Consequences (The "Negatives")

- **Schema ownership is a real burden.** A canonical schema means every new audited operation adds a typed parameter entry. Schema drift is prevented by making the schema a code artifact (enum + typed parameters) that the compiler enforces at call sites, but it still requires discipline.
- **Fail-closed on local write (for node-local authority events) is an operational sharp edge.** A full disk, a misconfigured file permission, or a rotated-out NDJSON file that the audit thread hasn't yet reopened will fail affected operations (auth checks, break-glass on non-state-machine RPCs). State-machine mutations are not affected — they ride the Raft log and their local materialization lags rather than blocks. Mitigated by admission-control disk-reservation for the audit path and by operator-visible metrics on audit-write latency, queue depth, and materialization lag.
- **Per-node materialization lag is a state operators must watch.** When a node's local chain is behind the Raft log, it exposes `audit_materialization_lag_entries` (ADR 027) and marks itself not-fully-ready on `/ready`. A cluster-wide dashboard on this metric is part of the default observability surface.
- **Per-node chains place the cross-node anchor in the roll-up cadence.** A sophisticated attacker with local root on a single node can rewrite history between roll-ups. The 60-second cadence caps this to a 1-minute window; faster cadence tightens it at the cost of event volume. Hardware-attested append-only storage (TPM-sealed) is noted as future work but not in this ADR.
- **Kafka-sink feature flag carries an operational cost.** Feature-flagging a sink avoids bloating the default binary's dependency graph but means some deployments rebuild to enable it. Accepted because Kafka's client library footprint is substantial and not every deployment wants it.
- **Canonical CBOR serialization for hashing is subtle.** CBOR canonical form is specified, but library-level canonicality must be verified — not just trusted. Mitigated by a canonicality round-trip test that hashes a wide random corpus and verifies stability across library minor versions.
- **Rejected alternatives explicitly:**
  - *Local-file-only audit* — rejected; every SIEM integration later requires touching every call site.
  - *Tamper evidence deferred to post-pilot* — rejected; retrofit cost (un-chained legacy records) is indistinguishable from tampering.
  - *Blockchain-anchored audit* — rejected (an earlier review position); external-anchor latency and operational cost are disproportionate to the threat model, and regulators accept cryptographic hash chains with exposed heads.
  - *Cluster-wide single-chain audit via Raft* — rejected; consensus on every audit emission is either a throughput collapse or a dropped-event hazard.
  - *Free-form log-line audit* — rejected; every SIEM then parses free form, poorly.

## Open Questions

- **Schema versioning and migration.** How a `schema_version = 2` is introduced alongside existing `schema_version = 1` records, and whether sinks must handle both. *Resolves via:* a follow-up schema-evolution note before acceptance; the rule is likely "additive-only within a major version; breaking changes bump the major and gate on an operator-approved migration."
- **Operator cert key use for signed roots.** Whether the node cert's mTLS signing key is the same key used for audit signed-root signatures, or a separate signing key is provisioned. The security properties differ (compromising the mTLS key currently compromises both). *Resolves via:* a security note accompanying ADR 025 acceptance.
- **Break-glass event parameter surface.** Break-glass operations are audited, but the `parameters` field for a `break_glass.used` record needs concrete fields (invoking certificate serial, operator name binding if available, reason string). *Resolves via:* a follow-up schema addendum; ADR 018's break-glass model is the upstream authority.

## References
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (principal identity source; break-glass marker)
- ADR 019: Cipher Selection and FIPS Posture (blake3 + signature primitives via aws-lc-rs)
- ADR 020: L2 Envelope Encryption and DEK Lifecycle (key rotation events consumer)
- ADR 021: Tenant-Aware Architecture from Day One (`TenantAdmin` mutations always emit audit records)
- ADR 026: FIPS 140-3 Certification Target (planned — audit signature primitives ride the same module boundary)
- ADR 027: Observability Posture (planned — audit is not observability; trace/metric/audit are three distinct streams)
