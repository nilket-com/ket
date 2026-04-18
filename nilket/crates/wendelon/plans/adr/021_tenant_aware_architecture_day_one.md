# ADR 021: Tenant-Aware Architecture from Day One

## Status
Proposed

## Context
Wendelon will back platforms (Polariton/Baryon) that host many disparate organizations. An earlier operational position was to defer multi-tenancy until post-pilot and run the pilot single-tenant. Review rounds reversed that position: retrofitting multi-tenancy into a shipped storage architecture is how storage products acquire permanent scar tissue. `tenant_id` participates in every durable schema decision — keyspace, ring placement, encryption context, audit record. A field that is durable in on-disk artifacts cannot be retrofitted without a full-fleet migration.

Forces in play:
- **ADR 018** binds tenant identity into the client certificate's OID extension. The authoritative `tenant_id` source is cryptographic, not a request header or URL prefix.
- **ADR 020** places a per-tenant Tenant KEK in the Raft state machine. Tenant identity therefore already participates in the key hierarchy; the wrapping layer needs a first-class tenant to attach to.
- The prior draft at `plans/ops/multi_tenancy.md` carried two positions that do not survive the converged architecture: (a) §1 derives routing from `Hash(NamespaceID || RawKey)` on a shared ring — prefix-sharing couples tenants operationally (compaction, hot keys, placement); (b) §2 uses JWT-bound RBAC — bearer tokens were explicitly rejected as a primary credential by ADR 018.
- Pilot concurrency is one tenant enabled, but every durable structure must still carry the field. A "default tenant" is still a tenant; the schema never loses the column.
- Quota enforcement has a race problem: data-path-only caches let a concurrent burst defeat the quota; state-machine-only pushes every ingress through consensus. Neither position on its own is acceptable.

## Decision

1. **Per-tenant first-class keyspaces.** Each tenant owns an independent *logical* keyspace — independent key-to-node placement inputs, independent compaction queues, independent encryption context, independent lifecycle, independent quota accounting. Keyspaces are not prefixes on a shared ring.

   Isolation scope at day one is logical and schema-level, not physical. Specifically, per-tenant keyspaces give day-one isolation of:
   - **Namespace.** Keys in tenant A cannot collide with keys in tenant B; routing and lookup cannot cross tenants.
   - **Compaction scheduling.** Each tenant has its own compaction queue; one tenant's backlog does not serialize behind another's.
   - **Key-hotspot placement.** Each tenant's hot keys distribute within that tenant's keyspace, not across the fleet as a whole.
   - **Encryption context and offboarding.** Each tenant's AAD and Tenant KEK are independent, and dropping a tenant is dropping its keyspace plus destroying its Tenant KEK — no scan of a shared namespace, no residual bytes under a prefix that nothing reads.

   Isolation scope does **not** extend to physical resource isolation at day one. Tenants still share node CPU, disk bandwidth, network, and cache memory on the same physical fleet. Tenants that want dedicated physical placement use the staged per-tenant topology work in decision §8 (per-tenant replication factor → per-tenant L2 bucket → per-tenant KEK isolation). The promise here is that the logical structure makes those physical-isolation phases possible; it does not provide them on its own.

   Routing on the unified ring (ADR 002): the day-one routing function is `place(tenant_id, key) = nodes_with_data_capability(Hash(tenant_id || key))`. `tenant_id` enters the hash input so a tenant's keys distribute across the ring; the *logical* keyspaces (compaction queues, quota counters, KEK bindings, etc.) are separate data structures keyed by `tenant_id`, not derived from the ring-position hash. This is a refinement of the prior `plans/ops/multi_tenancy.md` §1, not a contradiction of ADR 002: the ring is still one, the separation is in the logical structures above it.

2. **Actor identity and target-tenant identity are distinct, both cryptographically bound.** The design separates two questions that collapsing into one field does not answer correctly:
   - **Actor identity** — "who is calling?" — is taken exclusively from the mTLS client certificate's OID extension (ADR 018). The handshake binds the actor's role and, if the cert carries one, an actor-scoped `tenant_id`. Actors with `tenant_id = null` (typically `operator`, `break_glass_admin`, and cross-tenant `data_node` principals under ADR 018) have cluster-wide or multi-tenant authority. Actor identity is never taken from a request field.
   - **Target tenant** — "which tenant is this operation for?" — is an authenticated RPC argument. Every mutating RPC carries a `target_tenant_id` in a typed request field. The dispatcher pairs actor and target before entering handler code; authorization is a function of the pair, not either alone.

   Pairing rules enforced at the dispatcher:
   - If the actor's cert binds a non-null `tenant_id`, `target_tenant_id` must equal it. A mismatch is a hard authorization failure. This catches a tenant-scoped client attempting to address a different tenant.
   - If the actor's cert binds `tenant_id = null`, the actor is a cross-tenant principal and the dispatcher checks the role (ADR 018) against the operation's required role. `operator` may address any tenant for tenant-admin operations; `data_node` with `tenant_id = null` may address any tenant for replication and membership traffic; `read_only_observer` may read metadata for any tenant but cannot mutate.
   - RPCs that do not address a single tenant (cluster-status, membership changes) omit `target_tenant_id` entirely. The dispatcher treats the absent field as an explicit declaration, not a default.

   The implications compose:
   - A tenant-scoped client cannot address another tenant by spoofing a request field: the cert-vs-request pairing fails.
   - Cross-tenant principals (operators, replication traffic) have a coherent way to name the tenant they are acting on without burying it in free-form headers.
   - Tenant identity is still propagated to every durable structure: the Raft log record, the audit event (ADR 025 planned), and the AEAD AAD of L2 chunks (ADR 020 §2, field `tenant_id`). The durable structures record the *target* tenant, which is what quota accounting and encryption scope need.
   - Encryption backstops **tenant-context mismatches**, not arbitrary cross-tenant reads. If any layer — the dispatcher, the cache, the AAD construction — feeds the decrypt path a `tenant_id` that does not match the slab's stored context, the AEAD fails and the read aborts. This catches mis-routed reads, stale cache entries, and construction bugs. It does **not** catch a buggy authorization path that correctly passes the matching tenant-B context to a request that should have been denied; that failure mode is covered by the dispatcher's cert-vs-target pairing (decision §2), not by the cipher.

3. **Two-layer policy enforcement: data path (fast) and Raft state machine (authoritative).**
   - **Data path** enforces *instantaneous* limits at admission time: rate limits, per-request authorization, the cached quota view for soft-reject. This is the fast path; it runs without a consensus round-trip.
   - **Raft state machine** enforces *durable* limits: storage quota accounting, Tenant KEK isolation, tenant lifecycle state (active/suspended/deleted). This is the authoritative path; its decisions are replicated and audited.

   Both layers exist because both failure modes exist. Data-path-only lets a race across replicas defeat a durable quota. State-machine-only adds consensus latency to every ingress. Splitting the responsibilities is the only path that survives both.

4. **Quota enforcement is best-effort at the margin, structurally convergent over the sweep interval.** The design deliberately does not offer strict over-quota rejection across every possible concurrent ingress, because doing so would require a Raft round-trip on every write. The contract is:
   - **Admission-time soft-reject (fast path).** The data server consults a cached quota view at admission. If the cache shows the tenant is already over quota, the request is rejected with an explicit `QUOTA_EXCEEDED` error before any bytes land. Never OOM; never silent drop.
   - **Authoritative accounting (slow path).** The Raft state machine holds the authoritative `(tenant_id → total_bytes)` counter. Every write that commits a slab (L1 flush or L2 upload finalize) updates the counter as part of the same Raft commit that records the slab. The commit succeeds even if it lifts the counter above the configured limit; the state machine records the new total truthfully.
   - **Periodic reconciliation sweep.** A low-priority sweep publishes the authoritative Raft counter to the data-path cache on every replica. Reconciliation catches cache drift from missed invalidations, failed writes that didn't decrement, or concurrent ingress across nodes under a stale cache. Once the cache reflects the over-limit state, the admission-time soft-reject kicks in and further writes are rejected.
   - **Race window (explicit).** Between the moment a write crosses the quota boundary and the next reconciliation, further writes already in flight across replicas can commit. The cluster can therefore exceed quota by a bounded overshoot equal to roughly `(concurrent_ingress_rate × reconciliation_interval)`. Clients see this as a brief window of accepted writes past the configured limit, followed by `QUOTA_EXCEEDED` thereafter. Tighter overshoot bounds require a reservation mechanism (pre-debit a write-size reservation before admission, release on commit failure) which is deferred to post-pilot work.

   Rejection on quota, when it happens, is at admission: the client sees `QUOTA_EXCEEDED` with the current usage and the configured limit, before any bytes are durable. The design does not accept-then-fail after durability — the mechanism by which a cluster lands over the limit is additional commits that were already admitted under a stale-but-still-under-limit cache, not post-durability rejection.

5. **`TenantAdmin` RPC surface, deny-by-default, every operation Raft-committed.** Tenant lifecycle is not config-file-driven (config reloads are eventually consistent; tenant state is authoritative). It is not external-control-plane-only (locks operators into a specific deployment topology). It is a first-class RPC surface on the authenticated cluster boundary. Every `TenantAdmin` operation is a cross-tenant RPC: the actor is an `operator` (or `break_glass_admin`) with `tenant_id = null` in the cert, and the `target_tenant_id` is an authenticated request argument per decision §2.

   Minimum `TenantAdmin` operation set (each takes `target_tenant_id` as an explicit argument):
   - `create_tenant(target_tenant_id, initial_quota, initial_rf)` — provisions the keyspace, mints the Tenant KEK (ADR 020), records the tenant record in the state machine.
   - `update_tenant_quota(target_tenant_id, new_quota)` — Raft-committed quota change.
   - `suspend_tenant(target_tenant_id)` — marks tenant non-writable; reads may still serve depending on policy. Cache invalidation is explicit.
   - `resume_tenant(target_tenant_id)` — reverses suspend.
   - `delete_tenant(target_tenant_id)` — initiates keyspace drop and Tenant KEK destruction. Destructive; requires operator role (ADR 018) and leaves an audit record.
   - `get_tenant(target_tenant_id)` / `list_tenants()` — read-only state machine queries.

   Every mutation is a Raft-committed state machine transition. Every mutation emits an audit record (ADR 025 planned). Every mutation is authorized against the caller's role: only `operator` or `break_glass_admin` (ADR 018) may mutate tenant state; `read_only_observer` may read tenant metadata but not secrets. The `TenantAdmin` interface itself is not accessible via `data_node` role.

6. **Tenant lifecycle states are a small fixed set.**

   | State | Writes | Reads | KEK | Storage charged against quota |
   |-------|--------|-------|-----|-------------------------------|
   | `active` | allowed | allowed | present, cacheable | yes |
   | `suspended` | rejected | allowed | present, cacheable | yes |
   | `deleting` | rejected | rejected | being destroyed | frozen |
   | `deleted` | rejected | rejected | destroyed | zero |

   State transitions are Raft-committed and one-way for `deleting → deleted`. Re-activating a deleted tenant is not a supported operation; the Tenant KEK is destroyed and the keyspace reclaimed. This is a deliberate simplification: "restore" semantics for a deleted tenant are a backup/restore concern (ADR-unassigned for pilot), not a lifecycle operation.

7. **Default tenant is a tenant.** The single-tenant pilot ships with a default `tenant_id` that is not an absence of a tenant — it is a concrete tenant record in the state machine, bound to a real Tenant KEK and a real certificate OID claim. Code paths never take a "no tenant" branch. Schema never stores a null `tenant_id`. Operators bringing up a single-tenant cluster get a pre-provisioned default tenant by the cluster bootstrap.

8. **Per-tenant topology staging is post-pilot work, scoped but not scheduled here.** The converged answer scoped three phases of per-tenant topology isolation: per-tenant replication factor (Phase 1), per-tenant L2 bucket routing (Phase 2), per-tenant KEK isolation (Phase 3). Tenant KEK isolation is already day-one (ADR 020); these phases extend isolation further. The phase definitions belong in the rewrite of `plans/ops/multi_tenancy.md`; this ADR fixes the decision that per-tenant topology is a supported future path, not the schedule.

## Rationale

- **Per-tenant keyspaces beat shared-ring prefixes on every axis except a trivial one.** The only virtue of a shared ring with `Hash(tenant || key)` is uniform distribution across the cluster without per-tenant placement logic — and Wendelon already runs ring placement as a first-class concern, so per-tenant placement costs nothing. Against that we gain: compaction isolation, quota accounting that does not require a scan, crypto isolation that composes with the per-tenant KEK layer already chosen, and clean offboarding (drop the keyspace, destroy the KEK, done).
- **Certificate-bound `tenant_id` is the minimum viable integrity.** Every alternative — header, URL prefix, cookie, query param, authenticated-but-independent JWT — introduces an opportunity for the authenticated principal's tenant claim to diverge from the transport identity. mTLS OID binding collapses that to one source of truth that is cryptographically attested by the Operator CA.
- **Two-layer policy enforcement is not redundancy, it is different responsibilities.** The data-path cache handles per-request latency; the Raft state machine handles cross-replica consistency. Collapsing them into one layer forces a choice between consensus-latency on every write and race-exploitable durable counters. Neither is acceptable for a storage system sold on correctness.
- **Soft-reject with reconciliation is the right contract for the pilot.** Strict per-write quota enforcement would require a Raft round-trip on every admission; that cost is unacceptable at the write-path latency budget. The chosen contract — bounded overshoot equal to the ingress-rate × reconciliation-interval, followed by strict rejection once the cache catches up — is honest about what the fast path can deliver and leaves the reservation mechanism as a deliberate post-pilot option when a tighter bound is needed. Without reconciliation, counter drift accumulates silently and over-quota is discovered only at the next full count; with it, convergence is bounded and visible to operators.
- **`TenantAdmin` as Raft-committed RPC beats every alternative framing.** Config-file provisioning loses audit trail and atomicity; external-control-plane-only surrenders tenant state to a system that is not part of the correctness envelope; raw state-machine manipulation lacks authorization and rate-limiting. RPC + Raft + ADR-018 roles gives all three properties with one interface.
- **Default tenant as a real tenant eliminates an entire class of bugs.** A "no tenant" branch in any code path is a bug waiting to become a breaking change when the second tenant is provisioned. Forcing the single-tenant pilot through the full tenant-aware code path means the path is exercised from day one.
- **Staging post-pilot topology work leaves room for measurement.** Per-tenant replication factor, per-tenant L2 bucket, and per-tenant Root-KEK all change operational surface area meaningfully. Shipping all three with the pilot couples three large changes to first-day tenant onboarding. The decision here is that the path exists; the schedule is operational.

## Consequences (The "Negatives")

- **Every on-disk and in-flight structure carries the target `tenant_id`.** Raft log entries, WAL segments, redb values (ADR 017), audit records (ADR 025 planned), L2 slab AAD (ADR 020 §2), and mutating RPC request schemas (as a typed `target_tenant_id` field per decision §2, not a free-form header). The actor's tenant (when non-null) is separately carried by the peer certificate — it is not re-encoded in the RPC frame. A single-tenant pilot pays the schema cost for zero structural benefit. Accepted because retrofitting this field later is vastly worse than paying 16 bytes per record on day one.
- **Quota enforcement has a reconciliation layer and a cache-invalidation layer.** Two more moving parts in the ingress path. Mitigated by the sweep being low-priority and the cache-invalidation messages being structurally simple (tenant, new-value). The cost is real but bounded.
- **`TenantAdmin` is a critical RPC surface.** Every operation gates on ADR 018 role checks and emits an audit record. A bug in this surface can corrupt tenant state for the cluster. Mitigated by the state machine being the authoritative layer — `TenantAdmin` proposes; state machine disposes.
- **Per-tenant keyspace adds a `tenant_id` dimension to ring placement and to the logical data structures above the ring.** Ring placement consumes `tenant_id` as part of the hash input (decision §1), changing ring-balance properties compared to a single global keyspace. Logical structures (compaction queues, quota counters, Tenant KEK bindings) gain a `tenant_id` dimension. ADR 002 defines the unified node ring; the per-tenant logical layering here is a refinement above it, not a contradiction of it. Ring re-balance under very-many-tenants load is a post-pilot measurement item.
- **Deleting a tenant is irreversible once past `deleting`.** The destructive Tenant-KEK destruction at delete time means a delete cannot be undone by a "restore tenant" RPC. Accepted because the alternative — reversible deletes — requires retaining key material indefinitely, which directly contradicts the "clean offboarding" property that is the main operational win of per-tenant keyspaces.
- **KMS fan-out grows linearly with tenant cardinality under a cold cache.** Without the Tenant KEK cache (ADR 020), a cold-read burst across N tenants is N KMS unwraps. Mitigated by the cache and by batching writes by tenant at ingress so each batch is one unwrap, not N. Operators sizing for a fleet with many tenants must know the formula: `cost_per_batch = (distinct_tenants_in_batch × KMS_unwrap_p99) ÷ cache_amortization_factor`. Batch-by-tenant ingress is structural, not best-effort.
- **Rejected alternatives explicitly:**
  - *Deferred multi-tenancy* — rejected; retrofit migration cost exceeds schema cost on day one.
  - *Shared keyspace with tenant-salted hashing* (`Hash(tenant_id || key)` on one ring) — rejected; couples compaction, hot keys, and placement across tenants. Supersedes `plans/ops/multi_tenancy.md` §1's position.
  - *JWT-bound RBAC* — rejected; ADR 018 rejected bearer tokens as primary credentials. Supersedes `plans/ops/multi_tenancy.md` §2's position.
  - *Config-file tenant provisioning* — rejected; tenant state is authoritative and must be atomically mutable, which config reloads are not.
  - *External-control-plane-only tenant management* — rejected; forces a specific deployment topology and surrenders tenant state to a system outside the consensus boundary.
  - *One Tenant KEK shared across tenants ("cluster KEK")* — rejected in ADR 020 for different reasons; noted here because some deployments ask for it to reduce KMS cost. Cost-reduction belongs in the cache and batch-by-tenant structure, not in a weakened key hierarchy.

## Open Questions

All items below must be closed before this ADR moves from Proposed to Accepted.

- **Cross-tenant copy and cross-tenant reads.** Whether any RPC accepts a principal authenticated as tenant A and operates on data owned by tenant B, and under what role. The default answer is "no" and there is no such RPC in the pilot surface. Whether a privileged `operator` RPC can ever cross tenants (e.g., for data migration) is deferred. *Resolves via:* a follow-up scope note before acceptance, either confirming no-cross-tenant-anywhere or defining the narrow privileged RPC and its audit surface. Nothing in Wave 3 implementation depends on this answer.
- **Quota units and schema.** *Resolved:* the storage quota unit is **stored ciphertext bytes per logical replica** (slab payload ciphertext + per-chunk AEAD tags + slab footer, sourced from the slab manifest at commit, not multiplied by replication factor). See `plans/ops/multi_tenancy.md` §3.0 for the full definition, inclusions, and exclusions. Bandwidth and request-rate quotas remain outside this ADR's scope; they compose through the rate-limiter (ADR 018 §4).
- **Default-tenant bootstrap mechanism.** Whether the default tenant is provisioned by the cluster-bootstrap RPC, by a well-known migration in the state machine, or by an operator-issued `create_tenant` during first-boot is an orchestration question. *Resolves via:* a follow-up note in `plans/ops/lifecycle.md` before acceptance; the ADR-level invariant is that the default tenant exists as a real record, not how it gets there.
- **`plans/ops/multi_tenancy.md` rewrite.** *Resolved:* The rewrite is complete with explicit supersession markers for the JWT/RBAC and shared-ring models.

## References
- ADR 002: Unified Hash Ring Topology (per-tenant keyspaces compose onto the same unified ring)
- ADR 017: Persistent Local Index on redb (tenant-scoped values)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization (`tenant_id` source of truth)
- ADR 019: Cipher Selection and FIPS Posture (crypto primitives for tenant-bound AAD)
- ADR 020: L2 Envelope Encryption and DEK Lifecycle (Tenant KEK consumer)
- ADR 025: Audit Event Model (planned — tenant-lifecycle audit records)
- `plans/ops/multi_tenancy.md` (§1 and §2 superseded by this ADR)
