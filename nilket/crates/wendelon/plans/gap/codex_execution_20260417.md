# Wendelon Skeletal Execution Plan

Date: 2026-04-17

Source documents:
- `plans/gap/codex_assessment_20260417.md`
- `plans/gap/claude_execution_20260417.md`
- `plans/gap/gemini_execution_20260417.md`

Planning posture:
- This remains a high-level execution scaffold.
- The purpose of this iteration is to improve sequencing, workstream boundaries, and validation shape, not to expand every phase into a detailed task list yet.

## Objective

Close the gap between the current repository state and a commercially viable, production-ready Wendelon product.

Success means more than "the code exists." It means:
- the shipped daemon can form and operate a real cluster
- correctness claims hold on the real runtime path, not just in isolated tests
- security and compliance minimums are met
- durability and tiering are closed under restart and failure
- the product has operator surfaces, verification coverage, and bounded failure modes consistent with production use

## Execution Principles

1. Integration gate first.
   - The highest-leverage addition from the teammate plans is an explicit real-network gate early in the program. Most later work should be validated through that gate.

2. Shipped daemon path over library completeness.
   - A subsystem is not "done" until the stock runtime path can exercise it.

3. Fix the footgun, then harden.
   - Small correctness and security defects should not wait behind large design programs.

4. ADR first for design-heavy changes.
   - Encryption at rest, authz/RBAC, and any breaking storage-interface change should get a short design pass before broad implementation.

5. Small, independently verifiable increments.
   - Future detailed plans should prefer narrow slices that either pass or fail a clear validation step.

6. Correctness and security before major performance work.
   - Performance hardening matters, but not ahead of runtime assembly, admission safety, durability closure, and L2 correctness.

## Program Structure

This iteration adopts two useful structural ideas from the teammate plans:
- an explicit Phase 0 integration gate
- an explicit early "rapid correctness fixes" phase before broader platform hardening

It also separates pilot-readiness work from broader multi-tenancy expansion so the near-term roadmap stays commercially focused.

## Phase 0. Integration Gate And Production Path Bring-Up

Goal:
- Make the stock daemon a real, testable cluster process.

Why first:
- This was the clearest improvement in Claude's execution plan. Without a real-network gate, later work risks repeating the same "library path is real, shipped path is not" failure mode.

Scope:
- Extend the public config path to load production-relevant fields rather than dropping them on the floor.
  - TLS material fields.
  - L2 config fields, even if full tiering closure lands later.
- Wire `main.rs` to:
  - load TLS material
  - construct real `quinn::Endpoint` instances
  - call `serve_quic()`
  - support public `cluster.join` startup
  - initialize minimal tracing/logging so bring-up failures are visible
- Replace the current "single-node bootstrap only" public runtime behavior with a real distributed startup path.
- Create a real-network integration harness.
  - Acceptable forms:
    - separate daemon processes, or
    - in-process nodes using real QUIC endpoints and mTLS
- Put that harness into standard verification, ideally CI-driven.

Primary outputs:
- A daemon that can boot as a real QUIC/TLS node from config.
- A basic integration smoke test proving cluster formation and data path viability.

Exit criteria:
- The stock daemon can form a three-node TLS cluster from config.
- The integration gate exercises join, write, read, and restart through the real transport path.
- The gate is run as part of normal verification, not as a one-off manual test.

## Phase 1. Rapid Correctness Fixes

Goal:
- Land the small, high-leverage fixes that close obvious correctness and security holes quickly.

Why here:
- This is the best "fix the footgun, then harden" idea from the teammate plans. Some defects are too small and too important to bury inside broader later phases.

Scope:
- Join admission binding.
  - Adopt Gemini's stricter framing here: the join path should bind membership identity to the authenticated transport peer identity, not trust a payload-supplied node id.
- Tier observer wiring.
  - Attach `ManagerTierObserver` in production bootstrap so follower-side L1 cleanup is real.
- Dev-secret hygiene.
  - Add explicit warnings or sharper isolation for deterministic dev signing material in example/operator-facing flows.
- Immediate resource-bounding fixes where the implementation is already obvious.
  - idempotency TTL/eviction
  - basic per-peer or per-connection concurrency caps where missing

Primary outputs:
- The most obvious admission and follower-cleanup defects are closed.
- Early memory and operator footguns are reduced before deeper phases begin.

Exit criteria:
- Each fix lands with a targeted test.
- Join admission and tier-observer fixes are also covered by the Phase 0 integration gate where applicable.

## Phase 2. Security Floor

Goal:
- Establish the minimum security posture required for a serious pilot.

Scope:
- Short ADR/design pass for:
  - encryption at rest
  - control-plane authorization model
  - key/cert rotation expectations
- Implement encryption at rest for:
  - L1 slabs
  - WAL
  - snapshots
  - L2 objects
- Introduce baseline control-plane authorization boundaries.
  - Claude's distinction between operator authority and data-node authority is worth preserving here.
- Add an audit stream for privileged or operationally sensitive actions.
- Add initial key/cert lifecycle support.
  - certificate reload/rotation direction
  - signing-key rotation direction

Primary outputs:
- No pilot-blocking at-rest crypto gap.
- No pilot-blocking "any trusted node can do operator actions" gap.
- Admin-sensitive activity is auditable.

Exit criteria:
- Sensitive persisted data is encrypted at rest.
- Privileged control operations are authorization-aware.
- Admin actions generate auditable records.

## Phase 3. Durability Lifecycle Closure

Goal:
- Finish the control/storage lifecycle so durability claims hold in steady state, not only during recovery tests.

Scope:
- Live snapshot persistence.
- Runtime checkpoint emission.
- Durable WAL compaction/purge.
- Background scrub and corruption reporting/repair hooks.
- Large snapshot handling strategy.
  - Claude's explicit snapshot chunking concern is worth preserving.

Primary outputs:
- Recovery artifacts are produced in normal operation.
- Long-lived nodes have a real compaction lifecycle.
- Integrity verification is not purely opportunistic.

Exit criteria:
- Snapshot/checkpoint artifacts are emitted during steady-state runtime.
- WAL growth is bounded by a real persistence lifecycle.
- Restart and recovery paths are validated against those steady-state artifacts.

## Phase 4. Tiered Storage Closure

Goal:
- Turn L2 from an implemented subsystem into a production-integrated one.

Scope:
- Enable L2 configuration from the public config path.
- Start the real storage tiering sweeper in production bootstrap.
- Preserve follower convergence with correct local cleanup behavior.
- Add pending-upload tracking and orphan reconciliation.
- Add remote reclaim/object lifecycle management.
- Improve L2 read/write mechanics.
  - Gemini's range-read point is valid.
  - Claude's note that range GET likely implies a breaking `ObjectStore` trait change is also valid and should shape planning.
- Close crash and restart convergence gaps for tier pivots and compaction.

Primary outputs:
- L2 can be enabled from config and exercised through the shipped daemon.
- Tier pivots converge across leader, follower, restart, and failure cases.
- Remote leak behavior is managed rather than accepted.

Exit criteria:
- Tiering works end to end on real nodes.
- Follower disk leak is closed.
- Crash/retry paths converge without routine orphan accumulation.

## Phase 5. Resource Control, Fault Injection, And Scale Hardening

Goal:
- Prevent predictable overload failures and start proving behavior under fault.

Why this phase changed:
- The teammate plans improved this section in two ways:
  - Claude split out deterministic fault-injection and long-running correctness proving.
  - Gemini pushed concrete resource-bounding fixes earlier.
  This revision combines those into one high-level hardening phase.

Scope:
- Bound unbounded in-memory state.
  - idempotency
  - other caches/registries that currently grow without real lifecycle control
- Add overload signaling and admission control.
- Add memory budgets for large write/read/tiering flows.
- Reduce whole-buffer handling where it is a major stability risk.
  - cold L2 reads
  - snapshot movement
  - large query/materialization paths
- Build deterministic fault-injection coverage.
  - partition tests
  - restart/crash tests
  - tier-pivot fault coverage
- Add soak and restart/load validation appropriate to the real runtime path.

Primary outputs:
- The system degrades predictably under pressure.
- Core correctness properties are exercised under injected faults, not just clean runs.

Exit criteria:
- Sustained load no longer relies on unbounded maps or unchecked buffering.
- Deterministic partition/crash tests exist for core cluster invariants.
- Soak-style validation exists for the real runtime path.

## Phase 6. Operations, Observability, And Pilot Readiness

Goal:
- Make the product operable by an SRE or operator who did not build it.

Scope:
- Metrics hardening.
  - histograms
  - dimensional discipline
  - secure exposure
- Distributed tracing / OTEL.
- Health/readiness/drain surfaces.
- Operator CLI and admin workflows.
  - backup
  - restore
  - cluster status
  - leadership transfer
  - drain
- Backup/restore maturity and runbook closure.
- Runtime tuning defaults and documented operational rationale.
- Deployment/runbook/upgrade/rollback procedures.

Primary outputs:
- Operators can observe, diagnose, and control the system without custom Rust code.
- Pilot deployment has enough operational scaffolding to be supportable.

Exit criteria:
- The product is operable by someone other than the implementation team.
- Pilot deployment has documented procedures, instrumentation, and control surfaces.

## Phase 7. Multi-Tenancy And Policy Expansion

Goal:
- Expand from pilot-grade security/authorization into broader commercial isolation features.

Why separate:
- Claude's execution plan explicitly broke out multi-tenancy as a late program area. That is useful. It keeps the pilot path from being blocked by the full long-tail of per-tenant isolation work, while still acknowledging that the architecture must eventually support it.

Scope:
- Namespace and policy model.
- Per-tenant quotas.
- Per-tenant routing/storage policy.
- Per-tenant key isolation.
- Broader RBAC expansion beyond the minimum control-plane model from Phase 2.

Primary outputs:
- Clear tenant isolation model.
- Product path toward broader commercial deployment beyond a narrow early pilot.

Exit criteria:
- Tenant isolation and quota behavior are enforced by the product, not just planned on paper.

## Dependency Shape

Hard dependencies:
- Phase 0 gates almost everything else.
- Phase 1 should start as soon as Phase 0 work makes validation practical; some items can land during Phase 0, but the gate should validate them.
- Phase 2 depends on Phase 0 and benefits from Phase 1.
- Phase 3 and Phase 4 both depend on Phase 0 and should coordinate on shared persistence/recovery invariants.
- Phase 5 depends on the real runtime path and on earlier phases making the critical correctness boundaries real.
- Phase 6 can start in partial design form earlier, but full pilot-readiness closure depends on the earlier phases.
- Phase 7 depends on the security and storage foundations from earlier phases.

Parallelizable areas after Phase 0:
- Phase 2 and Phase 3 can progress in parallel if storage-format and key-management boundaries are made explicit.
- Phase 4 can overlap with Phase 3, but only with clear ownership around recovery semantics.
- Parts of Phase 6 can begin early, especially observability bootstrap work.

## Suggested Milestones

1. Integration-gate milestone
   - Real-network smoke test exists and runs as standard verification.

2. Real-daemon milestone
   - Stock daemon forms a real three-node TLS cluster from config.

3. Fast-fix milestone
   - Join admission bug and follower tier-observer gap are closed.

4. Security-floor milestone
   - At-rest encryption, baseline authz, and audit stream exist.

5. Recovery-and-tiering milestone
   - Snapshots/checkpoints/compaction are steady-state features and L2 is production-integrated.

6. Stability milestone
   - Backpressure, memory bounds, and deterministic fault coverage exist.

7. Pilot-readiness milestone
   - Observability, operator tooling, and rollout docs are sufficient for controlled production use.

## Deliverables For Later Planning Passes

The next detailed iterations should probably split this scaffold into dedicated plans:

1. Phase 0 transport/runtime integration plan.
2. Phase 1 rapid correctness fixes plan.
3. Phase 2 security floor and encryption-at-rest plan.
4. Phase 3 durability lifecycle plan.
5. Phase 4 tiered storage closure plan.
6. Phase 5 resource-control and fault-injection plan.
7. Phase 6 operations and pilot-readiness plan.
8. Phase 7 multi-tenancy expansion plan.

Each follow-on plan should include:
- exact problem statement
- subsystem boundaries / ownership
- ordered implementation steps
- validation and test plan
- rollout/migration concerns
- failure modes and rollback strategy

## Immediate Recommendation For The Next Planning Pass

Start with a combined detailed plan for Phase 0 and the Phase 1 fast fixes:
- public config/runtime wiring
- QUIC/TLS bootstrap and join
- real-network integration gate
- join identity binding
- production `TierObserver` wiring
- minimal tracing needed to debug bring-up

That is still the narrowest high-leverage next step. It turns the rest of the roadmap from library analysis into product-path execution, while also closing the two smallest high-severity defects immediately adjacent to that path.
