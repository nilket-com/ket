# W3 — Consensus & Ring

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W2 (transport + identity), W1 (WAL, redb)
**Folds:** `plans/016_unified_hash_ring.md`, `plans/018_per_range_leases.md`, `plans/022_control_ring_server.md`, `plans/024_epoch_transition_autoseed.md`

End of W3, a tenant-less Control-Ring can commit and apply cluster-config changes through Raft, compute per-key placement from a unified hash ring with epoch transitions, and issue per-range leases to the correct owner. W3 provides the state-machine surface that W5, W6, and W9 hang their mutations off.

## 1. ADRs consumed

- **ADR 002 (Unified Hash Ring).** Single ring with bounded vnode count; epoch transitions are first-class events (not incremental membership diffs).
- **ADR 007 (Raft Implementation Strategy).** `openraft`-based integration with Wendelon owning durability (W1). State-machine surface is the extension point for later workstreams.
- **ADR 029 (Identity vs Locality).** W3 is the workstream where the invariants I-1, I-3, I-5 are enforced at the topology layer: the ring keyspace is exactly `Locality` (u64), and the Raft state machine keys records by `Identity` (32 bytes).

## 2. Deliverables

1. **`openraft` integration** over W1's WAL/redb storage. Raft log-entry → state-machine-apply path is the sole mutation surface.
2. **Unified hash ring** with explicit epoch transitions and autoseed on cold-start (folded from `024_epoch_transition_autoseed.md`). Ring state is Raft-replicated.
3. **Per-range leases** — leader-per-range ownership model with lease renewal and handoff (folded from `018_per_range_leases.md`). Leases are time-bounded and tied to the HLC from W2.
4. **Control-Ring RPC surface** — `status`, `lease_acquire`, `lease_release`, `epoch_advance`, membership ops (folded from `022_control_ring_server.md`). Every RPC authenticated via W2's dual-CA.
5. **Cluster config mutations** committed through Raft, applied deterministically on every node.

## 3. Acceptance criteria (ADR-referential)

- (ADR 002) An epoch transition reliably redistributes ownership without dropping in-flight work; the event is auditable (ADR 025 emission).
- (ADR 002) Vnode count is bounded; ring-rebalance cost is bounded per epoch transition.
- (ADR 007) Raft log durability is owned by Wendelon (W1's durability contract); `openraft`'s internal storage trait is implemented over W1.
- (ADR 007) Leader election converges within the SLO documented in `plans/consensus/raft-core.md`.
- Control-Ring RPCs reject unauthenticated callers; OPS-CA callers cannot invoke DATA-CA-gated Control-Ring ops (ADR 018 cross-check).
- Autoseed on empty-cluster cold start produces a deterministic initial ring; two nodes cold-starting the same config reach the same ring state.

## 4. Explicit non-goals

- Tenant-aware state machine (TenantAdmin ops, per-tenant quota accounting). **W5.**
- Write-path bifurcation against the Raft state machine. **W6.**
- L2 ring integration (slab placement, rehydration). **W7.**
- Audit emission on Raft commit. **W9** (but the hook lands in W3 with a stub sink).

## 5. Open items

- **ADR 002 Open Question — autoseed determinism under partial bring-up.** Property test confirming two-of-three cold-start reaches the same ring as three-of-three.
- **ADR 007 Open Question — snapshot install bandwidth shape.** Close against W1's snapshot streaming work.

## 6. Test harness tier

- **Unit:** ring vnode math; lease state machine.
- **Property:** Raft commit → state-machine apply determinism; ring epoch transitions preserve total-order.
- **Integration:** three-node cluster, kill-leader, recover, resume commits.
- **Simulator:** once W10 lands, Raft under turmoil + Elle linearizability check.

## 7. Phasing (internal)

- **W3.a (S):** `openraft` over W1 storage.
- **W3.b (M):** Unified ring + epoch transitions + autoseed.
- **W3.c (S):** Control-Ring RPC + per-range leases.

## 8. Identity/Locality impact (ADR 029 addendum)

- **Ring math is `Locality`-only.** Vnode hashing, owner selection, epoch-transition range math, and lease ownership all key on `Locality`. No Raft-replicated ring structure carries `Identity`; per-record Identity appears in the *payloads* the ring routes, not in the ring's own bookkeeping (invariant I-3).
- **Raft state machine keys `Identity`.** State-machine mutations that touch records (idempotency, applied-Identity tracking, tombstone placement) carry `Identity`. Raft log entry encoding: `(Locality, Identity, payload)` where Locality is the routing hint and Identity is the storage key. Verifying that ring ownership (via Locality) matches the applying node on apply is a state-machine invariant.
- **Epoch transitions operate on `LocalityRange`.** Ownership handoff between epochs is framed as transferring a `LocalityRange` from old owner to new. `LocalityRange` already exists in `src/schema/units.rs`.
- **Routing collisions are tolerated.** Per invariant I-5, two distinct Identities projecting to the same Locality land on the same node. Raft state machine handles this by keying the per-record state by full `Identity`, so the shared Locality is invisible at the state-machine layer — only the ring sees it.
- **Autoseed determinism.** Initial ring seed keys `Locality` space uniformly across vnodes; no Identity bits enter ring seeding.
- **Acceptance addition.** Property test: for any ring snapshot and any `Identity`, `owner_of(Locality::from(identity)) == owner_of_locality(Locality::from(identity))` holds — i.e., owner selection is a pure function of Locality.
