# W6 — Write Path

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W3 (Raft), W4 (crypto), W5 (tenant plumbing)
**Folds:** `plans/012_stellarator_write_path.md`, `plans/013_smart_client_sdk.md`

End of W6, a client SDK can submit a write, have it bifurcated per ADR 003's contract, committed through Raft, encrypted under W4, accounted against tenant quota from W5, durably landed on W1's WAL, and acknowledged with the correct durability class.

## 1. ADRs consumed

- **ADR 003 (Stellarator Bifurcated Writes).** The write path bifurcates data and control at a defined boundary; data flows through the bulk path, control flows through Raft; the bifurcation point has a documented consistency model.
- **ADR 029 (Identity vs Locality).** Write-path idempotency, quorum tracking, and audit correlation all key on `Identity`. Routing at the bifurcation boundary keys on `Locality`. The bifurcation is a pure Identity→Locality projection — no parallel hash path.

## 2. Deliverables

1. **Bifurcated write path** landed against W3's Raft state machine and W1's WAL. The bifurcation point is a single named function; every code path that produces a durable mutation flows through it.
2. **Client SDK** — a thin `wendelon-client` crate with write/read APIs, QUIC connect, mTLS presentation, durability-class selection, retry semantics on transient failure (folded from `013_smart_client_sdk.md`).
3. **Tenant-scoped write accounting.** Every write increments the tenant's in-flight byte counter (W5 quota), decrements on commit or abort, and the accounting is consistent under concurrent writes.
4. **Durability-class surface** — `LEADER`, `NONE`, or the richer per-ADR-016 class matrix — exposed through the SDK and honored end-to-end.
5. **Write-path audit emission.** Every state-machine mutation emits an audit record to W9's sink (stub until W9 connects it to the chain).

## 3. Acceptance criteria (ADR-referential)

- (ADR 003) The bifurcation contract is a single test suite: every documented invariant is either a property test or an integration test; no invariant lives only in prose.
- (ADR 003) Under a network partition that isolates the leader, no write acknowledged with leader-durability is lost after recovery. Simulator scenario in W10.
- SDK write at the quota edge produces a structured quota-exceeded error without client-side retry loops.
- Every durable write produces exactly one audit record; duplicate writes (idempotent retry) produce one audit record, not two.

## 4. Explicit non-goals

- Server-side query pushdown (WPBC). **W8.**
- L2 write path (slab uploads). **W7.**
- Time-travel write semantics. **W8.**

## 5. Open items

- **ADR 003 Open Question — bifurcation-point error semantics.** What happens when the data branch succeeds and the control branch fails? Close by landing an explicit recovery contract before acceptance.

## 6. Test harness tier

- **Unit:** SDK encoder/decoder round-trip; bifurcation state machine.
- **Property:** write-accounting consistency under concurrent writers; idempotent-retry audit emission.
- **Integration:** end-to-end SDK → cluster write with quota and audit assertions.
- **Simulator:** W10 chaos scenarios covering partition + leader churn during write.

## 7. Phasing (internal)

- **W6.a (S):** bifurcation state machine + Raft integration.
- **W6.b (M):** SDK + durability-class surface.
- **W6.c (S):** tenant-accounting wiring + audit emission.

## 8. Identity/Locality impact (ADR 029 addendum)

- **Idempotency key.** Every write carries an `Identity` computed from `(tenant_id, key_bytes)` per W5's construction. The Raft state machine tracks applied-Identity to make retries idempotent; duplicate-suppression (the "exactly one audit record" invariant in §3) is Identity-keyed, not hash-keyed.
- **SDK surface.** `wendelon-client` SDK hashes `(tenant_id, key_bytes)` client-side using the ADR 029 / W5-stable BLAKE3 formulation to produce `Identity`; server receives Identity + key_bytes on write (server verifies consistency to defend against tampered clients) and Identity on read for direct lookup. Read-by-logical-key APIs are client-side sugar over the same deterministic identity function.
- **Bifurcation boundary.** At the bifurcation point (ADR 003), data branch receives `(Identity, Locality, payload)`; control branch (Raft) receives `(Identity, tenant_id, idempotency_key, durability_class)`. Both branches see the same Identity; neither sees a `u64 key_hash`.
- **Routing.** `Locality = Locality::from(identity)` once at the SDK/ingress boundary; downstream fanout reuses the same value. The projection is never re-derived from Identity at each hop (expensive and error-prone).
- **Audit emission.** Audit records for write mutations carry full `Identity` (ADR 025 schema). W9 addendum specifies the field width.
- **Quota accounting.** Byte counts only; Identity does not appear in quota state (consistent with W5 addendum).
