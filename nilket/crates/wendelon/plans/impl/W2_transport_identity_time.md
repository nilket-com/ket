# W2 — Transport, Identity, Time

**Effort band:** L (8–16 eng-weeks)
**Predecessors:** W1 (AsyncFileOps, redb for key material)
**Folds:** `plans/023_san_nodeid_binding.md`

End of W2, three nodes can stand up, handshake over QUIC, assert mTLS identity, and agree on a bounded-drift clock. Every later workstream that names "tenant_id" or "actor_identity" lives downstream of W2.

## 1. ADRs consumed

- **ADR 004 (Zero-Copy Transport).** `quinn`-based QUIC transport with zero-copy ingress/egress; framing is the W1/W2 boundary.
- **ADR 010 (Serialization Tiers).** Wire formats split by tier: bytemuck POD for hot paths, `serde` for Control-Ring, schema evolution rules.
- **ADR 011 (Auto-Adaptive Transport).** Transport selection (QUIC / fallback) is auto-adaptive per-peer with health-signal feedback.
- **ADR 012 (Bounded Clock Drift HLC).** Hybrid logical clock with bounded physical-clock drift; out-of-bound drift is a recovery event, not a silent monotonicity violation.
- **ADR 018 (Dual-CA mTLS Identity and OID-Bound Authorization).** Two trust roots (DATA CA, OPS CA); SAN → node_id binding; OID-bound role claims; break-glass marker.

## 2. Deliverables

1. **QUIC transport stack** (quinn), bench-verified zero-copy for hot-path reads, with full mTLS client-auth on every connection.
2. **Serialization layer** — POD hot path and `serde` Control-Ring with schema-version gates tied to wire-format evolution.
3. **Auto-adaptive transport switch** with peer health signals feeding back into selection.
4. **HLC driver** with a configurable drift bound, emitting a `clock_drift_exceeded` event when exceeded; downstream consumers (Raft, audit) subscribe.
5. **Dual-CA identity.** SAN template codified; node_id ↔ SAN binding enforced on every accept and connect; OPS-CA-signed cert presentation enables break-glass marker on audit records.
6. **TLS listener topology.** Three listeners (Health plain TCP, Readiness mTLS, Metrics mTLS) match `plans/ops/observability.md` — no TLS client-auth overload on a single port.

## 3. Acceptance criteria (ADR-referential)

- (ADR 004) Zero-copy for inbound slab range-read is measurable: no per-byte allocation in the hot path, confirmed by allocator-counter test.
- (ADR 010) Schema evolution: a node running wire-format version N+1 inter-operates with version N on Control-Ring messages for one rolling-upgrade window. Data-Ring messages are not cross-version.
- (ADR 011) Transport fallback triggers within the SLO window documented in ADR 011 when QUIC is unreachable.
- (ADR 012) The HLC never emits a timestamp that moves backwards on the same node; drift-exceeded events are audited (ADR 025 emission).
- (ADR 018) A cert signed by DATA CA cannot invoke any OPS-CA-gated operation, and vice versa, enforced at the authorization layer and verified by a negative-auth test matrix.
- (ADR 018) Break-glass marker appears on every audit record emitted during a session authenticated with the break-glass role.
- (legacy 023) Canonical SAN template is loaded from config; a mis-SAN cert is rejected at accept and connect time with a structured error.

## 4. Explicit non-goals

- `tenant_id` routing, per-tenant quotas, dispatcher pairing rules. **W5.**
- Raft consensus on membership/cluster config. **W3.**
- Metrics contents and scrape endpoint semantics beyond listener topology. **W9.**

## 5. Open items

- **ADR 011 Open Question — fallback transport choice.** TCP+TLS vs. alternative QUIC impl; close during W2 phasing.
- **ADR 012 Open Question — default drift bound.** Land a production default and document the measurement procedure for tuning.

## 6. Test harness tier

- **Unit:** per-codec POD/serde round-trip, HLC monotonicity, SAN parse.
- **Property:** HLC + drift bound invariants; SAN-to-node_id binding.
- **Integration:** three-node bring-up; negative-auth matrix across CA pairs.
- **Simulator:** once W10 lands, transport + clock failure modes run under turmoil.

## 7. Phasing (internal)

- **W2.a (M):** QUIC transport + serialization tiers.
- **W2.b (M):** Dual-CA mTLS + listener topology.
- **W2.c (S):** HLC + auto-adaptive transport.
