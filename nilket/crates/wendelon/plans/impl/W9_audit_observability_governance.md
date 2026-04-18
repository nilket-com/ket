# W9 — Audit, Observability & Governance

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W2 (transport/identity), W3 (Raft), W4 (crypto primitives for chain hash), W5 (tenant events)
**Folds:** none

End of W9, every auditable event is emitted to the canonical schema, linked into a tamper-evident per-node hash chain with a periodic signed root, surfaced through the three-listener observability topology, and bounded by the autonomous-healing governance boundary from ADR 001.

## 1. ADRs consumed

- **ADR 001 (Autonomous Healing Boundary).** The cluster's autonomous-action scope: what the system may do without operator input (rebalancing, rehydration, DEK rotation, orphan cleanup), and what requires an operator (tenant delete confirm, break-glass, cross-CA promotion).
- **ADR 025 (Audit Event Model and Tamper-Evident Hash Chain).** Canonical schema, `AuditSink` trait, per-node hash chain, periodic signed-root rollup, two-authority model (RaftLog vs NodeLocal), materialization-lag semantics.
- **ADR 027 (Observability Posture).** Three-listener topology (Health / Readiness / Metrics), OTLP trace + metric catalog, recording rules, alerts, authenticated scrape.

## 2. Deliverables

1. **Canonical audit schema** landed as a typed structured event. Free-form audit lines are rejected by the `AuditSink` trait.
2. **`AuditSink` trait** with ADR 025's three initial implementations: always-on `LocalNdjson`, additive `Syslog`, and feature-flagged `Kafka`. The sink surface is transport/storage for the emitted record; it is distinct from the event's `authority`.
3. **Two-authority audit model** landed exactly as ADR 025 defines it: `authority = RaftLog | NodeLocal` is a field on each event, `RaftLog` remains authoritative for state-machine mutations, and the per-node local chain is the materialization for `RaftLog` events and the authoritative store for `NodeLocal` events.
4. **Per-node hash chain** using ADR 019's chain hash primitive (`blake3` in default build, build-profile-parameterized for FIPS).
5. **Signed-root rollup** with the signature primitive selected per ADR 019.
6. **Materialization-lag semantics** documented and test-verified: a `RaftLog`-authority event's materialization on each node is observable and bounded.
7. **Observability topology** — Health plain TCP, Readiness mTLS, Metrics mTLS on distinct listeners (ties back to W2).
8. **Metric catalog** landed per `plans/ops/observability.md` with OTLP export, Prometheus recording rules for p99 series (`job:<metric>:p99_5m`), and the alert set referenced off the recording rules.
9. **Governance boundary enforcement** — autonomous-action surface per ADR 001; any action outside the boundary requires an operator signal (presented via OPS-CA cert from W2).

## 3. Acceptance criteria (ADR-referential)

- (ADR 001) The autonomous-action inventory is enumerated in code (not prose); every auto-executed action maps to a named surface. A new auto-action is a compile-time surface change, not a config flip.
- (ADR 025) Every state-machine mutation from W3, W5, W6, W7 produces exactly one audit record. Duplicate-suppression is verified by an end-to-end test.
- (ADR 025) The per-node hash chain is unbroken across any crash within W1's durability contract. Torn-append tests do not produce a phantom chain break.
- (ADR 025) Signed-root rollup is verifiable offline given the public key; a tampered audit line is detectable by chain replay.
- (ADR 025 two-authority) RaftLog and NodeLocal events are distinguishable in the on-disk format; failure semantics per §7 are exercised by chaos scenarios (W10).
- (ADR 027) The three listeners are distinct ports with distinct TLS posture. Health is scrapable without a client cert; Readiness and Metrics are not.
- (ADR 027) Recording rules produce `job:<metric>:p99_5m` series for every SLO metric named in the alert set; alerts reference the recording-rule series (no inline `histogram_quantile` in alert expressions).
- (ADR 027) A metrics scrape with a missing or invalid client cert is rejected at TLS handshake.

## 4. Explicit non-goals

- Full end-to-end audit chain under FIPS build profile — **W10** parameterizes the chain primitive and runs the chain-under-FIPS test.
- Operator signal UX (CLI / web). Pilot ships with the RPC surface only.

## 5. Open items

- **ADR 001 Open Question — rate-limit on autonomous actions.** Document the per-minute ceiling and back-pressure behavior.
- **ADR 027 Open Question — observability CA choice (share with DATA/OPS or distinct).** Commercial decision, out of workstream scope.

## 6. Test harness tier

- **Unit:** audit-schema encode/decode; hash-chain link correctness.
- **Property:** exactly-one-audit-per-mutation; chain-break-detection; authority-vs-sink semantics (changing sink mix does not change `authority`).
- **Integration:** three-listener topology against a real Prometheus scrape; recording-rule output against a golden data set.
- **Simulator:** W10 exercises chain behavior under crash and partition scenarios.

## 7. Phasing (internal)

- **W9.a (S):** canonical schema + `AuditSink` trait + sink implementations + authority discriminator.
- **W9.b (M):** per-node chain + signed root + materialization lag.
- **W9.c (M):** three-listener topology + metric catalog + recording rules + alerts.
- **W9.d (S):** governance-boundary surface + autonomous-action inventory.
