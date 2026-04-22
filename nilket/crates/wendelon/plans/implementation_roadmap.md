# Wendelon Implementation Roadmap — Outline

**Status:** ACCEPTED OUTLINE — workstream docs to follow under `plans/impl/`.

This document converts the 28 accepted/proposed ADRs into ordered, reviewable implementation workstreams. It is not itself an ADR and does not re-open architectural decisions; it only sequences them. Acceptance criteria for each workstream are lifted from the corresponding ADRs' acceptance-bar / Open-Questions sections.

The `.rs` source tree is the authoritative specification of Wendelon's behavior. ADRs and plans like this one are ephemeral scaffolding — they exist to align the next edit; once the code lands, the code is the spec. Treat this roadmap accordingly: it is guidance for the next set of edits, not a durable contract.

## Ground rules

- **Every workstream's "done" is ADR-referential, with room for implementation discovery.** If something isn't in an ADR at kickoff, that isn't a veto — implementation routinely surfaces questions an ADR couldn't anticipate. The rule is to *close the loop*: either amend the ADR or open a new one before the workstream's acceptance bar, not to enforce blind allegiance to the ADR as-written.
- **Workstreams parallelize where dependencies permit.** The dependency graph in §2 is the authority; ordering inside a workstream is up to the workstream owner.
- **Every workstream closes with a chaos-or-simulator-validated acceptance.** W1 and W10 produce the validation harnesses; every other workstream's acceptance criteria are expressed in terms of scenarios run under them.
- **No feature flags for correctness.** If a feature's code is in the binary and not gated behind a build profile (FIPS), it is exercised by the test harnesses. Dead code is not shipped.

## 1. Workstream list

| # | Workstream | ADRs consumed | Predecessors | Rough effort |
|---|------------|----------------|--------------|--------------|
| W1 | Foundation & Storage Seam | 008, 009, 015, 016, 017, 028 (AsyncFileOps extraction only) | — | L |
| W2 | Transport, Identity, Time | 004, 010, 011, 012, 018 | W1 | L |
| W3 | Consensus & Ring | 002, 007 | W2 | M |
| W4 | Crypto & Envelope Encryption | 019, 020 | W1 | M |
| W5 | Tenant-Aware Plumbing | 021 | W2, W3 | M |
| W6 | Write Path | 003 | W3, W4, W5 | M |
| W7 | L2 Tiering | 006, 014, 022, 023, 024 | W4, W6 | L |
| W8 | Query Surface | 005, 013 | W6, W7 | M |
| W9 | Audit, Observability & Governance | 001, 025, 027 | W2, W3, W4, W5 | M |
| W10 | FIPS Profile + Simulator/Chaos Productionization | 026, 028 (beyond foundation) | all prior | M |

Effort bands: S ≈ 2-4 eng-weeks, M ≈ 4-8 eng-weeks, L ≈ 8-16 eng-weeks. These are outlines, not commitments.

## 2. Dependency graph (ASCII)

```
        W1 ── W2 ── W3 ────────────┐
         │                          │
         ├── W4 ──────┐             │
         │            │             │
         │            ├── W5 ───────┤
         │            │             │
         │            └── W6 ── W7 ─┤
         │                          │
         │                   W8 ────┤
         │                          │
         ├── W9 (consumes W2..W5 as they land) ─┐
         │                                      │
         └── W10 (productionizes W1's harness, adds FIPS profile, runs after all prior) ──── MVP cut
```

Hot paths:
- **W1 → W4 → W7** (storage seam → envelope encryption → L2) is the longest critical-path chain for the Data-Ring.
- **W1 → W2 → W3 → W5** is the critical path for Control-Ring tenant admin.
- **W9** can start after W5 (needs tenant events to audit) and runs in parallel with W6/W7/W8.
- **W10** is the closeout — its acceptance is the pilot cut criterion.

## 3. Per-workstream outline

Each stream gets its own doc under `plans/impl/W{N}_<name>.md` once this outline is approved. Each doc has the same shape:

1. **ADRs consumed** — with a one-line summary per ADR of what this workstream is implementing from it.
2. **Predecessors** — which workstreams must be substantially complete before this one can start meaningful work (vs. be parallel-startable).
3. **Deliverables** — concrete, testable, reviewable. No "design X"; always "implement X with test T proving Y."
4. **Acceptance criteria** — verbatim or near-verbatim from the consumed ADRs' acceptance-bar / Open-Questions sections. Every item traces back to an ADR paragraph.
5. **Explicit non-goals** — what this workstream does *not* deliver, to prevent scope creep.
6. **Open items** — ADR Open Questions that remain live and need resolution during this workstream (not re-opening, just closing out the last mile).
7. **Test harness tier** — which of W1's / W10's harnesses validate this workstream (unit, property, integration, simulator, chaos).
8. **Effort band and rough phasing** — internal S/M/L sub-tiers, no calendar commitments.

Below is a one-line sketch per workstream; full docs land after this outline is approved.

### W1 — Foundation & Storage Seam
Land the `AsyncFileOps` trait over the existing `IoRingHandle`/MPSC/dedicated-thread architecture (ADR 028 §2). Bring WAL + snapshot lifecycle (ADR 015), durability model with fsync/group-commit (ADR 016), and redb local index (ADR 017) to production-grade with simulator replayability. Choose and pin dependency set (ADR 008).

### W2 — Transport, Identity, Time
Zero-copy QUIC transport (ADR 004), auto-adaptive transport switching (ADR 011), serialization-tier wire formats (ADR 010), bounded-clock-drift HLC (ADR 012), dual-CA mTLS identity with OID-bound authorization (ADR 018). At the end of this stream the cluster can stand up three nodes, handshake, and assert who is who.

### W3 — Consensus & Ring
Unified hash ring with epoch transitions (ADR 002). `openraft` integration with Raft state machine surface for subsequent streams' commits (ADR 007). By the end of W3 a tenant-less Control-Ring can commit and apply cluster-config changes.

### W4 — Crypto & Envelope Encryption
aws-lc-rs backend with ADR 019's two-branch cipher decision tree (SIV or injective-nonce GCM). Three-level DEK/KEK hierarchy + rotation (ADR 020). Nonce injectivity and per-DEK data-bound tests ship with acceptance.

### W5 — Tenant-Aware Plumbing
Every RPC carries `actor_identity` (from mTLS) and `target_tenant_id` (authenticated request field), with dispatcher pairing rules (ADR 021 §2). `TenantAdmin` ops on Raft. Two-layer quota (data-path soft-reject + Raft authoritative accounting + reconciliation sweep) with bounded-overshoot guarantee. Tenant lifecycle state machine. Default-tenant-as-real-tenant bootstrap.

### W6 — Write Path
Stellarator bifurcated writes (ADR 003) against W3's Raft layer and W1's WAL/redb path. Write-path acceptance ties back to W9's audit emission for state-machine mutations.

### W7 — L2 Tiering
Lazy-migration sweeper (ADR 006). RustFS test backend (ADR 014). Slab format with footer for indexed range reads (ADR 022). Upload lifecycle with durability semantics (ADR 023). Orphan cleanup via remote reclaim (ADR 024). Envelope-encrypted from W4 end-to-end.

### W8 — Query Surface
Server-side WPBC pushdown execution (ADR 005). Time-travel queries with MVCC (ADR 013). Fuzz surface (WPBC opcodes) ties to W10.

### W9 — Audit, Observability & Governance
Canonical audit schema + AuditSink trait + per-node hash chain + signed-root rollup + two-authority model + materialization-lag semantics (ADR 025). Three-listener observability with OTLP, metric catalog, recording rules, alerts (ADR 027). Autonomous-healing governance boundary (ADR 001) — what the cluster may act on without operator input.

### W10 — FIPS Profile + Simulator/Chaos Productionization
Two build profiles with compile-out of non-validated primitives (ADR 026). Audit chain hash primitive parameterized by build profile. Simulator network layer (turmoil-style) and `ObjectStore` mock (ADR 028 §3-5). Elle + monotonic correctness checkers (ADR 028 §6). Coverage ladder (10k / 1M / 100M) wired to CI (ADR 028 §7). Current chaos smoke slice plus rolling-upgrade / backup-restore scenarios, with the kernel-backed disk-full / clock-skew catalog explicitly deferred (ADR 028 §8). Seed registry with retention policy and a scheduled advisory report (per `plans/ops/testing.md` §7).

## 4. What this outline does NOT cover

- **Calendar commitments.** Effort bands are sizing, not schedule.
- **Staffing.** Who runs each workstream is outside this doc.
- **Feature flags for business-logic toggles.** None are envisioned; every code path in the binary is exercised by the harnesses.
- **Commercial/licensing decisions.** FIPS lab procurement (ADR 026 Open Question) and observability CA choice (ADR 027 Open Question) are outside this roadmap.
- **Documentation surface for end users.** Operator docs (`plans/ops/*.md`) are already the ADR exit criteria; end-user API docs are a separate track.

## 5. Promotion of Proposed ADRs to Accepted

ADRs 001-014 are already `Accepted`. ADRs 015-028 remain `Proposed`. The promotion rule tied to this roadmap applies to the still-`Proposed` ADR set:

- A `Proposed` ADR moves to `Accepted` when its consuming workstream closes its acceptance bar, with all ADR-level Open Questions either resolved in text or converted into workstream open items.
- ADRs whose acceptance bar is entirely harness-dependent (e.g., ADR 019's injectivity tests) move with W1 or W10 delivery, not with their code landing.

## 6. Disposal of prior planning material

The `plans/gap/*_20260417.md` convergence docs that fed the ADRs have been **deleted**. Git history is the archive; keeping them on disk invites re-opening resolved questions and poisons AI context with superseded reasoning. Any surviving reference-list entries pointing at `plans/gap/` should be stripped on sight.

The existing `plans/012-027_*.md` feature plans overlap with several workstreams (e.g., `020_durability_boundary.md` → W1+W6; `026_rolling_upgrade.md` → W10). These are **folded** into the corresponding `plans/impl/W{N}_<name>.md` docs as those are drafted: the salient content lands inside the workstream doc, and the original `plans/0XX_*.md` file is removed. No parallel-track feature plans survive alongside the workstream docs — one authoritative location per topic.

## 7. Steering decisions (recorded)

Resolved by the user at outline acceptance:

1. **Workstream decomposition — 10 streams.** The number emerged organically from three independent agents converging on the same slicing; not changing it.
2. **Scope discipline — ADR-referential with room for discovery.** See Ground rules; implementation-surfaced scope is fine as long as the loop closes via ADR amendment or a new ADR before the workstream's acceptance bar.
3. **Disposal of `plans/gap/*`.** Deleted, not archived. See §6.
4. **Workstream doc location — `plans/impl/W{N}_<name>.md`.** Fresh namespace, separate from the legacy `plans/0XX_*.md` feature plans.
5. **Existing `plans/012-027_*.md` feature plans — folded into workstream docs.** See §6.

## 8. Next step

Fill out `plans/impl/W{N}_<name>.md` for each workstream (10 docs). Each stub follows the §3 shape. The ten stubs can be drafted in parallel (no inter-dependencies among the docs themselves — only among the work they describe). As each workstream doc lands, the corresponding legacy `plans/0XX_*.md` feature plan is folded in and removed.
