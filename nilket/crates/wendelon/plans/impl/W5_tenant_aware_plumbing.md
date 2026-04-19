# W5 — Tenant-Aware Plumbing

**Effort band:** M (4–8 eng-weeks)
**Predecessors:** W2 (identity, dual-CA), W3 (Raft state-machine surface)
**Folds:** none

End of W5, every RPC carries an authenticated `actor_identity` and an authenticated `target_tenant_id`; dispatcher pairing rules enforce who-can-act-on-what; `TenantAdmin` mutations commit through Raft with two-layer quota enforcement and bounded overshoot. The default-tenant-as-real-tenant bootstrap path is exercised on cold start.

## 1. ADRs consumed

- **ADR 021 (Tenant-Aware Architecture from Day One).** Every durable structure carries a `tenant_id`; dispatcher pairs `actor_identity` × `target_tenant_id`; two-layer quota (data-path soft-reject + Raft authoritative accounting + reconciliation sweep); tenant lifecycle state machine.
- **ADR 029 (Identity vs Locality).** Consumed as a construction constraint: tenant isolation must be a property of the Identity-production path, not an afterthought. W5 resolves how `tenant_id` enters the hash input that produces per-record `Identity` so that two tenants with the same key bytes yield different Identities.

## 2. Deliverables

1. **Tenant_id on every durable structure.** WAL records, redb keys, L1 slabs, L2 object keys all carry the tenant binding. Schema migration for this is a pilot-breaking change; no backward compatibility is kept.
2. **Dispatcher pairing layer.** Every RPC handler receives `(actor_identity, target_tenant_id)`; pairing rules (which actor may act on which tenant) are authoritative at the dispatcher, not delegated to individual handlers.
3. **`TenantAdmin` Raft ops** — `tenant_create`, `tenant_delete`, `tenant_update_quota`, `tenant_rotate_kek`. Every op emits an audit record (ADR 025 emission, stubbed here, fully wired in W9).
4. **Two-layer quota** — data-path soft-reject with a per-node cached limit; Raft-authoritative accounting with a reconciliation sweep; bounded-overshoot guarantee expressed as a per-tenant max-concurrent-in-flight × mean-write-size.
5. **Tenant lifecycle state machine** — `Creating → Active → Suspended → Deleting → Deleted` with legal transition matrix.
6. **Default-tenant bootstrap.** Cold start creates the default tenant as a real tenant (same code path as any other), not as a special-case hardcoded ID.

## 3. Acceptance criteria (ADR-referential)

- (ADR 021 §2) A cross-tenant request (actor from tenant A targeting tenant B, without explicit cross-tenant authorization) is rejected at the dispatcher with a structured error before reaching any handler. Negative test matrix enumerates every actor-role × target-tenant pairing.
- (ADR 021 §3) `TenantAdmin` mutations are exclusively Raft-committed; no node-local tenant mutation path exists.
- (ADR 021 §4) Quota overshoot is bounded: for a tenant at-limit, the system never accepts more than `max_concurrent × mean_write_size` bytes beyond the limit. Verified by a stress test with N concurrent writers at the quota edge.
- (ADR 021 §5) A `tenant_delete` eventually removes every durable structure keyed to that tenant; orphan-detection post-delete finds zero residual bytes (ties to W7's orphan cleanup).
- (ADR 021 §6) Default tenant is exercised by the same audit, quota, and lifecycle code paths as any other tenant.

## 4. Explicit non-goals

- Per-tenant KEK creation lifecycle under W4's three-level hierarchy. **W5 consumes W4's Tenant KEK surface; key material operations are W4.**
- L2 orphan cleanup implementation. **W7.**
- Full audit wiring (W5 emits to a stub sink; W9 connects the sink to the audit chain).

## 5. Open items

- **ADR 021 Open Question — quota reconciliation cadence.** Land a default and document the staleness window vs. overshoot tradeoff.
- **ADR 021 Open Question — dispatcher rule-change propagation.** The rule table is Raft-committed; document the at-most-one-revision-lag property.

## 6. Test harness tier

- **Unit:** pairing-rule evaluation; quota arithmetic.
- **Property:** tenant lifecycle legal-transition-only; quota-overshoot bound.
- **Integration:** cross-tenant negative-auth matrix; tenant create/delete on a running cluster.
- **Simulator:** W10 exercises quota edge under network partition + Raft leader churn.

## 7. Phasing (internal)

- **W5.a (S):** tenant_id schema propagation.
- **W5.b (M):** dispatcher pairing + TenantAdmin Raft ops.
- **W5.c (S):** two-layer quota + reconciliation.

## 8. Identity/Locality impact (ADR 029 addendum)

- **Tenant-scoped Identity construction.** `Identity` is produced from `(tenant_id, key_bytes)`, not from `key_bytes` alone, under a single BLAKE3-256 formulation used in every build profile. Two candidate API expressions remain acceptable:
  - **(a) Plain-input prefix.** `Identity = blake3(tenant_domain || tenant_id_bytes || key_bytes)`. Simple and explicit.
  - **(b) Keyed mode.** `Identity = blake3_keyed(tenant_derived_key, key_bytes)`. Equivalent trust model when the derived key comes only from public tenant identity; the advantage is domain separation by API construction, not secrecy.
  - Decision locked before W5.a implementation begins, but the result must be byte-for-byte stable across build profiles for the same logical key. Closes the ADR 029 Open Question "Tenant routing projection."
- **Locality inherits tenant scoping by consequence.** Same tenant produces scoped Locality distribution; cross-tenant Locality collisions are still "fat buckets" (invariant I-5) and still land on the same node, but no two tenants ever produce the same *Identity* for the same key.
- **Tenant-delete and Identity garbage.** `tenant_delete` (ADR 021 §5) must reclaim every durable structure keyed to the tenant's Identity space. Because Identity is tenant-scoped by construction, no record from another tenant ever aliases into the deletion set — the reclaim sweep can iterate by `tenant_id` and know every reachable Identity is in scope.
- **Dispatcher pairing receives Identity indirectly.** `(actor_identity, target_tenant_id)` remains the pairing key at the dispatcher. Record `Identity` appears in the *request* the dispatcher forwards, not in the pairing rule itself. Terminology guard from W2's addendum applies.
- **Quota accounting is unaffected.** Two-layer quota (ADR 021 §4) counts bytes, not records, and carries neither Identity nor Locality in its state.
- **Open item added.** Whether W5 ships formulation (a) or (b), and whether a future cluster-secret salt is needed to harden locality-hotspot abuse. The salt question is separate from tenant namespace separation because `tenant_id` is public.
