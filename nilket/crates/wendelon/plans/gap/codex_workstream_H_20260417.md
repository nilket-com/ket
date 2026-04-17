# Codex Workstream H: Multi-Tenancy And Policy Expansion

Date: 2026-04-17

Derived from:
- `plans/gap/codex_execution_20260417.md`

## Objective

Extend the system from pilot-grade security and authorization into broader tenant-isolation and policy-enforcement capabilities suitable for wider commercial deployment.

This workstream is intentionally separated from pilot-readiness so the near-term roadmap is not blocked by the full long tail of tenant isolation, namespace policy, and quota modeling.

## In Scope

- Namespace and policy model.
- Per-tenant quotas.
- Per-tenant routing and storage policy.
- Per-tenant key isolation.
- Broader RBAC expansion beyond the baseline control-plane model from Workstream C.

## Out Of Scope For This Pass

- Narrow pilot-readiness items already covered by earlier workstreams.
- Purely aspirational long-term commercial packaging not grounded in concrete product surfaces.
- Tenant-facing product UX beyond the storage/control-plane implementation needs.

## Primary Deliverables

- Clear tenant isolation model.
- Enforced quota and policy behavior.
- Product path toward broader commercial deployment beyond a narrow early pilot.

## Key Interfaces And Dependencies

- Depends on Workstream C for baseline authz and encryption/key-management foundations.
- Depends on Workstream E for tenant-aware storage-policy surfaces.
- Depends on Workstream G for operator control of tenant-aware policy surfaces.
- Should not destabilize the narrower pilot path while it is still being closed.

## First-Pass Sequencing

1. Define namespace and policy model.
2. Define tenant-aware key and storage policy boundaries.
3. Land quota and routing/storage controls.
4. Expand RBAC beyond the baseline control-plane floor.
5. Add tenant-isolation validation and operational surfaces.

## Validation Shape

- Tenant A cannot access Tenant B data or policy surface.
- Quota violations are enforced with explicit product behavior.
- Tenant-aware routing and key behavior are testable and observable.

## Exit Criteria

- Tenant isolation and quota behavior are enforced by the product.
- Per-tenant policy model exists in implementation, not only in plans.
- The broader commercial-isolation story is credible beyond a single narrow pilot tenant.

## Open Questions

- Is multi-tenancy required for the first real pilot, or should this remain explicitly post-pilot?
- How much of tenant policy should be enforced in the control plane versus the data path?
- Should per-tenant L2 routing and per-tenant key isolation land together, or can they be staged safely?
