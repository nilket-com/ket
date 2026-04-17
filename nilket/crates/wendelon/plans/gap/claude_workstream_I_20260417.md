# Workstream I — Multi-Tenancy

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 9)
**Status:** First pass
**Subsystems:** Schema, ring routing, leases, storage, crypto — cross-cutting
**Depends on:** Workstream D (encryption / per-tenant keys), Workstream E (authorization / RBAC)

---

## Problem Statement

The system has no concept of tenancy. All data shares a single namespace,
a single set of encryption keys, a single L2 bucket, and a single
authorization domain. The plans document `multi_tenancy.md` exists but
there is no implementation. Retrofitting multi-tenancy is a design-level
change that cascades into:

- **Schema:** Namespace-prefixed keys and metadata.
- **Ring routing:** Per-namespace shard placement.
- **Leases:** Per-namespace lease grants.
- **Storage:** Per-tenant L2 bucket routing.
- **Crypto:** Per-tenant encryption keys (KEK isolation).
- **Quotas:** Per-tenant storage and throughput limits.
- **Audit:** Per-tenant audit trails.

This is the most architecturally complex workstream and the long pole in
the overall plan.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Namespace scheme | `src/schema/core.rs`, `src/network/data_server.rs` | Prefix routing |
| Ring routing | `src/network/control_server.rs`, ring module | Per-namespace placement |
| Lease grants | `src/network/control_server.rs`, lease modules | Namespace-scoped |
| L2 routing | `src/storage/l2.rs`, `src/storage/sweeper.rs` | Per-tenant bucket |
| Key isolation | `src/crypto/` (Workstream D) | KEK per tenant |
| Quotas | New `src/quota/` or extension of data server | Enforcement point |
| Config | `src/config.rs` | Tenant definitions |

---

## Ordered Implementation Steps

### I.0 — ADR (blocks all implementation)

| # | Step | Details |
|---|------|---------|
| I.0.1 | Write ADR: multi-tenancy design | Namespace scheme (prefix vs. separate keyspace). Routing model (logical partitions within the same ring vs. separate rings per tenant). Quota enforcement point (admission control at data server vs. Raft-side). Tenant lifecycle (create, suspend, delete, crypto-erase). Interaction with existing single-tenant Raft state machine. |
| I.0.2 | Review ADR with architect | This is the highest-stakes design decision in the project. Get it right before writing code. |

### I.1 — Namespace-Prefixed Schema

| # | Step | Target | Details |
|---|------|--------|---------|
| I.1.1 | Add `tenant_id` to key schema | `src/schema/core.rs` | All keys are prefixed with a tenant identifier. Existing single-tenant data uses a default tenant. |
| I.1.2 | Namespace-aware write path | `src/network/data_server.rs` | Extract tenant from request, prefix key, route to correct slab. |
| I.1.3 | Namespace-aware read/query path | `src/network/data_server.rs` | Scope reads to the requesting tenant's namespace. Never return cross-tenant data. |

### I.2 — Per-Namespace Ring Routing

| # | Step | Target | Details |
|---|------|--------|---------|
| I.2.1 | Tenant-aware shard placement | Ring routing module | Hash(tenant_id + key) for placement, or separate placement policies per tenant. |
| I.2.2 | Per-namespace lease grants | `src/network/control_server.rs` | Leases scoped to (tenant_id, range). A tenant's lease cannot authorize access to another tenant's data. |

### I.3 — Per-Tenant Storage Isolation

| # | Step | Target | Details |
|---|------|--------|---------|
| I.3.1 | Per-tenant L2 bucket routing | `src/storage/l2.rs`, config | Map tenant_id → L2 bucket/prefix. Different tenants can use different S3 buckets for data sovereignty. |
| I.3.2 | Per-tenant encryption keys | `src/crypto/` (Workstream D) | Each tenant gets its own KEK. Tenant offboarding → crypto-erase by deleting the tenant's KEK. |

### I.4 — Quotas

| # | Step | Target | Details |
|---|------|--------|---------|
| I.4.1 | Storage quota enforcement | `src/network/data_server.rs` | Track bytes per tenant. Reject writes that would exceed the tenant's storage quota. |
| I.4.2 | Throughput quota enforcement | `src/network/data_server.rs` | Rate-limit writes/reads per tenant. Return clear error (not OOM or timeout) when limit is hit. |
| I.4.3 | Quota configuration | `src/config.rs` | Per-tenant quota definitions in TOML. Updatable at runtime (via control RPC or config reload). |

### I.5 — Integration Tests

| # | Step | Target | Details |
|---|------|--------|---------|
| I.5.1 | Two-tenant isolation test | `tests/` | Tenant A writes data. Tenant B cannot read it. Verify at the data path level. |
| I.5.2 | Quota enforcement test | `tests/` | Tenant A exceeds storage quota. Write is rejected with clear error. Tenant B is unaffected. |
| I.5.3 | Crypto-erase test | `tests/` | Delete tenant A's KEK. Tenant A's data is unrecoverable. Tenant B's data is unaffected. |
| I.5.4 | Cross-tenant L2 isolation test | `tests/` | Tenant A's data is in bucket-A. Tenant B's data is in bucket-B. Verify no cross-contamination. |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| Two-tenant data isolation | Integration | Namespacing works, no cross-tenant leakage |
| Storage quota rejection | Integration | Quota enforcement returns clear error |
| Throughput quota rate-limit | Integration | Rate limit triggers, other tenants unaffected |
| Crypto-erase tenant | Integration | KEK deletion renders data unrecoverable |
| Per-tenant L2 bucket | Integration | Data lands in correct bucket |

---

## Rollout and Migration Concerns

- **Existing single-tenant data.** All data written before multi-tenancy
  needs to be assigned to a default tenant. The namespace prefix must be
  backward-compatible (default tenant = no prefix or a well-known
  prefix). This is the trickiest migration concern.
- **Raft state machine schema change.** Adding tenant awareness to the
  state machine may require a state machine schema migration. Plan for
  this in the ADR.
- **Quota accounting bootstrap.** When multi-tenancy is first enabled on
  an existing cluster, per-tenant byte counts need to be computed from
  existing data. This may require a one-time scan.
- **Gradual rollout.** Consider a "single-tenant mode" (default, no
  overhead) and a "multi-tenant mode" (enabled via config) to allow
  clusters to opt in when ready.

---

## Failure Modes and Rollback

- **Tenant namespace collision.** If two tenants get the same prefix
  (misconfiguration), data leaks across tenants. Enforce uniqueness at
  tenant creation time.
- **Quota accounting drift.** If the byte counter drifts from actual
  usage (due to compaction, deletion, or crash), tenants may be
  over/under-charged. Periodic reconciliation scan needed.
- **Rollback to single-tenant.** If multi-tenancy is enabled and then
  rolled back, data with tenant prefixes will be orphaned. Rolling back
  requires a migration to strip prefixes. Document this risk.

---

## Exit Criteria

- Two tenants on the same cluster cannot read each other's data.
- Quota breach returns a clear error, not OOM.
- Crypto-erase of one tenant does not affect other tenants.
- Per-tenant L2 bucket routing works.

---

## Open Questions

1. **Namespace scheme:** Key prefix (simple, but every key gets longer)
   vs. separate keyspace/slab-per-tenant (cleaner isolation, more
   complex routing)? The choice affects every downstream component.
2. **Tenant identity propagation:** How does the system know which tenant
   a request belongs to? Extracted from the client cert (OID extension)?
   From a request header? From the connection's authenticated identity?
   Ties into Workstream E authorization model.
3. **Quota enforcement point:** At the data server (admission control) or
   at the Raft state machine (consensus-level enforcement)? Data-server
   enforcement is faster but can drift; Raft enforcement is authoritative
   but adds latency.
4. **Tenant lifecycle management:** How are tenants created, suspended,
   and deleted? Via control RPCs? Via config file? Via an external
   control plane? This affects the operational surface (Workstream F CLI).
5. **Existing multi_tenancy.md plan alignment:** The existing
   `plans/multi_tenancy.md` document may have design decisions that
   should be honored or explicitly superseded. Review it during ADR
   drafting.
6. **Per-tenant replication factor?** Can different tenants have different
   RF? This adds significant complexity to the ring routing but may be
   required for cost optimization (small tenants get RF=1, critical
   tenants get RF=5).
