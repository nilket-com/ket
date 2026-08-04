# Multi-tenancy & Quotas

> **Status note.** This document has been rewritten to reflect the Wave 2 and Wave 3 ADRs. The prior revision's positions are superseded as follows:
>
> | Prior §  | Prior position | Superseding ADR(s) | Outcome |
> |----------|----------------|--------------------|---------|
> | §1 Logical Isolation | Shared ring with `Hash(NamespaceID \|\| RawKey)` routing | [ADR 021](../adr/0021_tenant_aware_architecture_day_one.md) | Superseded: per-tenant first-class keyspaces with `tenant_id` entering the hash input; logical structures (compaction, quotas, KEK bindings) are separate data structures keyed by `tenant_id` rather than prefixes on a shared keyspace. |
> | §2 RBAC | JWT bearer tokens against Raft-managed RBAC policies | [ADR 018](../adr/0018_dual_ca_mtls_identity_authorization.md), [ADR 021](../adr/0021_tenant_aware_architecture_day_one.md) | Superseded: identity is the client certificate's OID extension; bearer tokens are not a primary credential. |
> | §3 Resource Quotas | Token-bucket IOPS + storage-byte aggregation with `QUOTA_EXCEEDED` rejection | [ADR 021](../adr/0021_tenant_aware_architecture_day_one.md) | Retained in shape; aligned to the best-effort-at-margin contract (admission-time soft-reject, Raft-authoritative accounting, periodic reconciliation). |
>
> The rewrite below is the canonical operational guidance. Wave 2 (ADRs 018/019/020) and Wave 3 (ADR 021) are the authoritative architectural sources.

Wendelon backs massive platforms (Polariton/Baryon) that may host thousands of disparate organizations. Multi-tenancy is a day-one property of the architecture, not a retrofit — every durable structure carries `tenant_id`, even when the cluster is provisioned single-tenant (see §7 Default Tenant Bootstrap).

---

## 1. Logical Isolation — Per-Tenant First-Class Keyspaces

Each tenant owns an independent *logical* keyspace. Keys in tenant A cannot collide with keys in tenant B; every logical structure above the ring (compaction queues, quota counters, Tenant KEK bindings, audit streams) is keyed by `tenant_id` as a first-class dimension, not by a prefix on a shared keyspace.

**Day-one routing function** (on the unified ring from [ADR 002](../adr/0002_unified_hash_ring.md)):

```
place(tenant_id, key) = nodes_with_data_capability(Hash(tenant_id || key))
```

This is a refinement of the unified ring, not a second ring. `tenant_id` enters the hash input so a tenant's keys distribute across the ring. The logical isolation — separate compaction queues, separate quota counters, separate KEK bindings — happens in the data structures *above* the ring placement.

**Isolation scope at day one** is logical and schema-level:

- **Namespace** — key collisions across tenants are impossible; routing cannot cross tenants.
- **Compaction scheduling** — each tenant has its own compaction queue; one tenant's backlog cannot serialize behind another's.
- **Key-hotspot placement** — each tenant's hot keys distribute within that tenant's hash subspace, not amplified across the fleet as a whole.
- **Encryption context** — each tenant's AEAD AAD (ADR 020 §2) and Tenant KEK (ADR 020 §3) are independent.
- **Offboarding** — dropping a tenant is dropping its keyspace plus destroying its Tenant KEK. No shared-namespace scan; no residual bytes under a prefix that nothing reads.

**Isolation scope does not extend to physical resource isolation at day one.** Tenants still share node CPU, disk bandwidth, network, and cache memory on the same physical fleet. Operators who need physical isolation for specific tenants use the staged topology work in §5.

---

## 2. Identity and Authorization

Identity and authorization come from the mTLS handshake; see [ADR 018](../adr/0018_dual_ca_mtls_identity_authorization.md) for the full model. This section covers the tenant-specific pieces.

### 2.1 Actor vs. Target Tenant

Two distinct questions, two distinct sources:

- **Actor identity** — "who is calling?" — is the cert's OID payload (`role`, `scopes`, `tenant_id`, `key_id`). The handshake cryptographically binds this to the connection. Actor identity is never taken from a request field.
- **Target tenant** — "which tenant is this operation for?" — is an authenticated request argument, `target_tenant_id`, on every mutating RPC. It is a typed field, not a free-form header.

### 2.2 Dispatcher Pairing Rules

Enforced at the RPC dispatcher before handler code runs:

| Cert's `tenant_id` | Authorized targets |
|--------------------|-------------------|
| Non-null (tenant-scoped principal) | `target_tenant_id` **must equal** the cert's `tenant_id`; mismatch = hard authorization failure. |
| Null (cross-tenant principal: `operator`, `break_glass_admin`, or cross-tenant `data_node`) | Any tenant; the role determines which operations are permitted. |

RPCs that do not address a single tenant (cluster-status, membership changes) omit `target_tenant_id`. The dispatcher treats the absent field as an explicit declaration, not a default.

### 2.3 Role → Operation Matrix (Tenant Surface)

| Role | Data-Ring reads/writes | Cluster status / metrics / audit tail | `TenantAdmin` reads | `TenantAdmin` mutations |
|------|--------------------------|-----------------------|---------------------|--------------------------|
| `data_node` (tenant-scoped) | own tenant only | — | — | — |
| `data_node` (cross-tenant, `tenant_id = null`) | any tenant (replication/membership) | — | — | — |
| `operator` | any tenant | yes | any tenant | any tenant |
| `read_only_observer` | — | yes (ADR 018 §3) | metadata-only (ADR 021 §5) | — |
| `break_glass_admin` | any tenant | yes | any tenant | any tenant (audit-tagged) |

`read_only_observer` is deliberately scoped to operational visibility — cluster status, metrics, audit tail, and tenant *metadata*. It has no Data-Ring read access and cannot read tenant secrets or payloads. The role exists for monitoring and compliance-evidence collection, not for data inspection.

Bearer tokens are not a primary credential anywhere in the stack.

---

## 3. Resource Quotas

Quotas are enforced in two layers. The design is **best-effort at the margin, structurally convergent over the reconciliation interval** (ADR 021 §4).

### 3.0 Quota Units (closes ADR 021 Open Question 2)

The storage quota unit is **stored ciphertext bytes per logical replica** — that is, the bytes that actually occupy disk after any pre-encryption compression and after encryption overhead (per-chunk authenticator tags per ADR 020 §2), counted once per logical copy of the data rather than multiplied by replication factor. Concretely:

- **Included.** Slab payload ciphertext, per-chunk 16-byte AEAD tags, slab footer bytes.
- **Excluded from the tenant counter.** Replication-factor multiplicity (a replication-factor-3 tenant is not charged 3× for the same slab), WAL bytes, snapshot bytes, audit-trail bytes, cluster-metadata bytes.
- **Reported by.** The slab manifest's authoritative ciphertext size at the moment the slab-creating Raft commit lands. No post-hoc recomputation from disk; the manifest is the source of truth.

Why this unit:

- **It reflects what the cluster must actually hold.** Raw plaintext bytes over-count (compression shrinks them) and allocation bytes over-count (dedupe, padding). Stored ciphertext is the honest cost.
- **It is the value the slab manifest already records.** Every existing accounting path already knows this number; no new measurement is introduced.
- **Replication-factor independence is the correct semantic.** A tenant does not choose to pay 3× for running on a 3-replica cluster; replication is an operator-level durability choice, not a tenant-level consumption choice. Operators sizing a fleet budget for total disk should multiply the aggregate tenant quota by the replication factor, not charge each tenant individually.

Bandwidth and request-rate quotas are out of scope for this section; they are handled by the ADR 018 §4 per-identity rate limiter.

### 3.1 Data-Path Admission (Soft-Reject)

Every data server caches the `(tenant_id → total_bytes, limit)` view locally. At admission time:

1. Look up the cached view for the request's `target_tenant_id`.
2. If `cached_total_bytes ≥ limit`, reject immediately with `QUOTA_EXCEEDED` (the client receives current usage and configured limit in the error body).
3. Otherwise, admit the write.

Rejection is at admission — before bytes are durable. The data server does not accept-then-fail after durability.

### 3.2 Raft State Machine Authoritative Accounting

The Raft state machine holds the authoritative `(tenant_id → total_bytes)` counter.

- Every commit that persists a slab (L1 flush, L2 upload finalize) updates the counter atomically with the slab record.
- Commits succeed even when they lift a tenant above its limit; the state machine records the new total truthfully. The authoritative counter is always correct; the question is how stale the cache is.

### 3.3 Reconciliation Sweep

A low-priority sweep (default: every 30 s; tunable) publishes the authoritative Raft counter to each replica's data-path cache. Once the cache reflects an over-limit tenant, admission-time soft-reject kicks in and further writes are rejected.

### 3.4 Bounded Overshoot Window

Between the moment a write crosses the limit and the next reconciliation, already-in-flight writes across replicas can commit. The overshoot is bounded by:

```
worst_case_overshoot ≈ concurrent_ingress_rate × reconciliation_interval
```

Operators sizing for a tight quota ceiling should:
- Reduce the reconciliation interval (trades CPU for tighter overshoot).
- Set the configured limit with explicit headroom for the expected overshoot.
- Use the reservation mechanism (post-pilot) if the overshoot bound is insufficient.

### 3.5 IOPS and Rate Limiting

Per-identity rate limiting is handled by the ADR 018 §4 rate-limiter, keyed on `(key_id, role)` after mTLS succeeds. The prior document's token-bucket framing is correct in shape; the implementation is the identity-keyed post-auth path, not a tenant-prefix lookup.

### 3.6 Error Surface

| Condition | Error | Client recovery |
|-----------|-------|-----------------|
| Cached counter shows over-limit | `QUOTA_EXCEEDED` at admission | Free space, request quota increase, or retry after reconciliation. |
| Authenticated principal targets a different tenant | `AUTHORIZATION_FAILED` | Check cert OID vs. request target. |
| Tenant in `suspended` state, write | `TENANT_SUSPENDED` | Operator-contact path. |
| Tenant in `deleting` / `deleted` state | `TENANT_UNAVAILABLE` | Terminal; no recovery. |

---

## 4. Tenant Lifecycle

### 4.1 States

| State | Writes | Reads | KEK | Quota counter |
|-------|--------|-------|-----|---------------|
| `active` | allowed | allowed | present, cacheable | active |
| `suspended` | rejected | allowed | present, cacheable | active |
| `deleting` | rejected | rejected | being destroyed | frozen |
| `deleted` | rejected | rejected | destroyed | zero |

Transitions are Raft-committed. `deleting → deleted` is one-way. Re-activating a deleted tenant is not supported; the Tenant KEK is destroyed and the keyspace reclaimed. "Restore" for a deleted tenant is a backup/restore concern, not a lifecycle operation.

### 4.2 `TenantAdmin` RPC Surface

All operations take `target_tenant_id` as an explicit argument (see §2.1). All mutations are Raft-committed and audit-logged.

| RPC | Required role | Raft-committed | Audit |
|-----|---------------|----------------|-------|
| `create_tenant(target_tenant_id, initial_quota, initial_rf)` | `operator` | yes | yes |
| `update_tenant_quota(target_tenant_id, new_quota)` | `operator` | yes | yes |
| `suspend_tenant(target_tenant_id)` | `operator` | yes | yes |
| `resume_tenant(target_tenant_id)` | `operator` | yes | yes |
| `delete_tenant(target_tenant_id)` | `operator` | yes | yes (destructive marker) |
| `get_tenant(target_tenant_id)` | `operator`, `read_only_observer` | no (linearizable read) | optional |
| `list_tenants()` | `operator`, `read_only_observer` | no | optional |

`break_glass_admin` inherits all `operator` authority; every action emits an audit record with the `break_glass = true` marker (ADR 018 §3).

### 4.3 Cache Invalidation on State Transitions

- `suspend_tenant` → Control-Ring message invalidates the data-path admission cache for that tenant on every replica.
- `resume_tenant` → same channel; cache entries rebuilt on next admission.
- `delete_tenant` → invalidates admission cache, drains the Tenant KEK cache (ADR 020), and initiates keyspace reclamation through the L2 compaction machinery (ADR 024 planned).

---

## 5. Per-Tenant Topology Staging (Post-Pilot)

Physical isolation beyond the logical scope of §1 is delivered in three phases. The schedule is operational, not committed here; the architectural decision (ADR 021 §8) is that these phases are the path.

### Phase 1 — Per-Tenant Replication Factor

- **Change surface.** Placement only. A tenant can declare a replication factor higher or lower than the cluster default.
- **Why first.** Smallest structural change; exercises only the ring-placement path.
- **Open item.** Whether the change is expressed at `create_tenant` time only or updatable via `TenantAdmin`.

### Phase 2 — Per-Tenant L2 Bucket Routing

- **Change surface.** Storage lifecycle. A tenant's L2 writes route to a tenant-scoped bucket (or bucket prefix) rather than the cluster-default bucket.
- **Why second.** Couples to the L2 lifecycle machinery (ADRs 022–024 planned); must land after those.
- **Why meaningful.** Deletion recovery, provider-side access control, per-tenant billing at the provider, and compliance residency are all unlocked by tenant-scoped storage endpoints.

### Phase 3 — Per-Tenant KEK Isolation

- **Change surface.** Crypto. A tenant can be backed by its own Root KEK in the KMS (not just a per-tenant Tenant KEK wrapped by a shared Root KEK).
- **Why last.** Deepest change — crosses KMS IAM, key-hierarchy rotation, and the ADR 020 cache.
- **Relationship to day-one state.** ADR 020 already gives per-tenant *Tenant KEKs*. Phase 3 extends to per-tenant *Root KEKs* for tenants that require fully separate cryptographic roots (e.g., compliance isolation).

Each phase ships independently, is validated independently, and is reversible. Shipping them together couples three large changes to first-day tenant onboarding, which is why they are staged.

---

## 6. KMS Fan-Out Cost Model

KMS unwrap cost scales with tenant cardinality under a cold cache. The cluster-level cost formula for cold-cache cross-tenant traffic:

```
cost_per_batch ≈ (distinct_tenants_in_batch × KMS_unwrap_p99) ÷ cache_amortization_factor
```

where `cache_amortization_factor` grows with the Tenant KEK cache hit rate (ADR 020 §4).

**Structural mitigation: batch-by-tenant ingress.** Writes are batched at the data-server admission path *by `target_tenant_id`* so each batch's crypto cost is one unwrap per tenant, not N. This is structural, not best-effort — the admission path does not mix tenants within a write batch.

**Cache sizing guidance.** ADR 020 §4 sizes the two caches differently:

- **Tenant KEK cache** — bounded by the admission-control memory budget under LRU, not by a fixed entry count. TTLs: 15-min idle, 60-min absolute. For clusters expecting many active tenants, the tuning knob is the cache's memory budget, not an entry-count ceiling.
- **Slab DEK cache** — default 10,000 entries, tunable, at ~80 bytes per entry (≤1 MiB total).

Operators with very-many-tenants (≥ thousands) deployments should monitor the Tenant KEK cache hit rate in the observability stack (ADR 027 planned) and raise the memory budget if cold-unwrap rate becomes a hot-path cost. Warm entries are free at steady state.

**Worst cases operators should recognize:**

| Scenario | Cost shape | Mitigation |
|----------|-----------|-------------|
| Cold-start with many tenants | N KMS unwraps before cache warms | Pre-warming on node start; staggered per tenant. |
| Broad cross-tenant read burst | One unwrap per distinct tenant | Batch-by-tenant does not help reads across many tenants; cache hit rate does. |
| Tenant KEK rotation | Per-tenant re-wrap of all slab DEKs | Rate-limit rotation; schedule during low-ingress windows. |

Sizing recommendations for specific KMS providers live with the KMS-integration design note (ADR 020 Open Question: KMS backend abstraction).

---

## 7. Default Tenant Bootstrap

The single-tenant pilot provisions a concrete default tenant on first boot. The default tenant is a real record in the Raft state machine with a real Tenant KEK, a real OID claim on the default client certificate, and a real quota counter. Code paths never take a "no tenant" branch; schema never stores a null `tenant_id`.

The orchestration mechanism (cluster-bootstrap RPC vs. well-known state-machine migration vs. operator-issued `create_tenant`) is tracked as an open item in [`plans/ops/lifecycle.md`](lifecycle.md). The architectural invariant here is that the default tenant exists as a real record before any Data-Ring RPC is served — not how it gets there.

---

## 8. Operator Checklist (Cluster Bring-Up)

1. Confirm the cluster's Node CA and Operator CA are provisioned per ADR 018.
2. Confirm aws-lc-rs crypto provider is enabled per ADR 019.
3. Confirm Tenant KEK cache budget is sized per §6 guidance.
4. Bootstrap the default tenant (see §7).
5. Issue at least one `operator`-role cert via the Operator CA.
6. Verify `TenantAdmin.list_tenants()` returns the default tenant.
7. Set tenant quota for the default tenant via `update_tenant_quota`.
8. Run admission + reconciliation smoke test: write-to-limit, confirm `QUOTA_EXCEEDED` after cache converges.

---

## References

- [ADR 002 — Unified Hash Ring Topology](../adr/0002_unified_hash_ring.md)
- [ADR 018 — Dual-CA mTLS Identity and OID-Bound Authorization](../adr/0018_dual_ca_mtls_identity_authorization.md)
- [ADR 019 — Cipher Selection and FIPS Posture](../adr/0019_cipher_selection_and_fips_posture.md)
- [ADR 020 — L2 Envelope Encryption and DEK Lifecycle](../adr/0020_l2_envelope_encryption_and_dek_lifecycle.md)
- [ADR 021 — Tenant-Aware Architecture from Day One](../adr/0021_tenant_aware_architecture_day_one.md)
- `plans/ops/lifecycle.md` — default-tenant bootstrap mechanism
