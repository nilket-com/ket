# Wendelon Unresolved Issues — Claude's List

**Author / Role:** Claude, as Lead Architect
**Date:** 2026-04-22
**Source:** Current state after commodity-default architectural reframing.
**Purpose:** The Phase-0 Design-Lock meeting agenda. Current-state snapshot; resolved items not retained.

## Architectural frame

**Day 1 deployment:** cheap VPS without an HSM. Commodity hardware; $5/mo-class infrastructure. This is what customers run on at first ship.

**Day 2 deployment (the planning destination):** full Fortune 500 — regulated pilot with HSM-backed custody, CMVP certification, warm-standby DR, K8s Operator, full compliance pack. This is the ultimate target the program is chartered for.

**Day 1 architecture supports the Day 2 deployment from the first commit.** One product, one binary, deployment-profile-configurable. Root KEK custody via cloud KMS or TPM-sealed is the Day 1 baseline; PKCS#11 HSM is a Day 2 deployment option on the same trait. FIPS build profile is a Day 2 compile flag, not a shortcut-around. The unified-ring philosophy applies to deployment scale: no fork between tiers; nothing in the Day 1 codebase precludes the Day 2 deployment.

Items previously treated as Phase-0 critical (CMVP lab engagement, HSM procurement, `rustls-aws-lc-rs` maturity) move from the Day-1-calendar to Day-2-customer-triggered. They are fully committed as destinations; they are not Day-1 schedule drivers.

## Where the council stands

Architect-review process has substantively closed under the reframing. One residual architect-level item. The bulk of remaining work is non-architect-resolvable: procurement, legal, operations staffing, customer identification, cross-team joint specifications.

---

## 1. Architect-level residuals

### 1.1 BYOK definitional scope under commodity default — simplified

The reframing changes this item's shape substantially. Under commodity default, cloud KMS is the *native* custody mode for customers who want their keys in their own cloud account. BYOK stops being a separate "feature" and becomes the standard cloud-KMS deployment.

- **What remains:** marketing language for regulated-uplift pilots. If we say "regulated-ready multi-tenant," does the customer expect cryptographic isolation beyond BYOK (i.e., per-tenant Root KEKs)? That's a Codex-minority question about the uplift, not the default.
- **Resolution path:** Compliance + GTM own the regulated-uplift collateral language. Not an architect decision.

---

## 2. External-dependency contingencies

Known direction; external factor controls timing. Reframed for commodity-default.

- **Shamir library (`vsss-rs`) crypto audit.** Commission at Phase 0 Week 2. Escrow ceremony is still valuable for commodity-default with operator age-keys / YubiKeys as seal medium (~$50 × 5 custodians).
- **IANA PEN registration timing.** ADR 018 freezes OID pending assignment. Still Phase 0 critical.
- **Toron team engineering calendar.** Cross-team joint work.
- **Cloud-KMS vendor sandbox access.** AWS / GCP / Azure KMS sandboxes needed for provider integration testing. Phase 0.

**Deferred from Phase 0 critical path** (was previously listed here):
- ~~CMVP lab selection~~ — deferred to regulated-customer identification. Not a Phase-0 calendar driver.
- ~~`rustls-aws-lc-rs` upstream maturity~~ — tracked for regulated-uplift; not commodity-default gating.
- ~~aws-lc-rs CMVP pinned-version status~~ — same.
- ~~HSM procurement budget~~ — triggered by regulated-uplift customer, not Phase-1 default.

---

## 3. Customer-dependent interpretation

Right answer varies by customer. Trigger is customer identification.

- **WORM governance vs compliance mode.** For regulated-uplift customers with S3 Object Lock. Customer-attorney input.
- **Compliance-pack sector triggers.** SOC 2, PCI-DSS, HIPAA, FedRAMP are regulated-uplift deliverables and sector-conditional within that uplift.
- **Cloud-KMS choice.** Customer picks AWS / GCP / Azure at deploy time; all three ship at Gate 3.
- **Maintained ELT connector choice.** Depends on first-customer ingestion path.

---

## 4. Owner-assignment gaps

Shape agreed; operational ownership not yet named.

- **Async-replication workstream owner.** Regulated-uplift feature. Storage/runtime lead is default; name when regulated-uplift calendar activates.
- **Runbook quality ownership post-handoff.** Named operations-team reviewers per runbook; quarterly review cadence.
- **Design-Lock log maintenance.** Operational ownership not named.
- **Ceremony-host operational role.** Escrow ceremony for commodity-default (YubiKey-sealed shares) and regulated-uplift (HSM-sealed shares) both need an operational owner.

---

## 5. Technical specifications still unspecified

Numbers and mechanisms to pin down before implementation.

- **TPM-sealed provider implementation details.** tpm2-tools integration path; PCR state selection; re-seal policy on kernel updates. New item under the reframing; affects the commodity-default critical path.
- **Orphan-reclaim TTL for failed remote writes.** Specific number not agreed.
- **Pending-intent-store fencing threshold.** 30-second fence window proposed; runtime lead owns.
- **Cert-service deployment-profile config schema.** Single-node-default vs HA-Raft upgrade is a config flag; concrete schema not drafted.
- **Background finalization worker scope.** Thread model, queue depths.
- **Trace-sampling baseline rate default.** 1-5% range agreed.
- **Commodity-default retention floor.** Tier-keyed (90d `none`, 2555d `financial`/`healthcare`). Does a commodity-default tenant get 90d or a different floor?

---

## 6. "Out of scope" requiring customer-facing documentation

Two documentation tracks now, reflecting the reframing:

**Commodity-default track:**
- **TPM-sealed operator runbook.** How to set up TPM sealing on common VPSes (Hetzner, Linode, DO, OVH). New.
- **Cloud-KMS-from-VPS operator runbook.** IAM setup when running on non-AWS VPS with AWS KMS. New.
- **Escrow ceremony with YubiKey-sealed shares.** Non-HSM ceremony protocol. New.

**Regulated-uplift track (existing):**
- mlock memory-dump Security Policy statement.
- Per-tenant Root KEK compensating-control language.
- Backup cross-region customer runbook for BYOK.
- FIPS posture statement, CMVP validation evidence packet.

---

## 7. Cross-workstream joint decisions

Two or more TLs needed.

- **Per-user cert issuance scale** [WS-C + Toron] — throughput target validation.
- **Audit trace correlation** [WS-D + Toron] — field spec.
- **SchemaUpdate wire format** [WS-codex WS1 + Toron + WS-D].
- **Compute-AST operator set** [Gemini WS-B + WS-C + WPBC].
- **Quota visibility via QUIC trailers** [WS-D + WS-codex WS2 + Toron].
- **DR audit-chain continuity across cluster rebuild** [WS-D + WS-G].

---

## 8. Silently unaddressed

- **Tenant-cert architecture ADR 018 extension.** Phase-2 amendment scoping.
- **Post-CMVP-certificate boundary-changing PR policy.** Regulated-uplift; must exist before any CMVP certificate issues.
- **Jepsen test catalog enumeration.** Property list.

---

## 9. Items dropped by the reframing

For traceability, previously-listed items no longer relevant:

- **HSM topology per-node vs shared** — HSM is regulated-uplift-only; baseline deployment has no HSM. Dispute vanishes.
- **Compliance pack NIST 800-53 summary** — regulated-uplift-only deliverable; commodity-default does not ship a compliance pack. Codex's minority position is moot for the default tier.
- **FIPS transport Posture A/B decision gate** — deferred from Phase 1 Month 6 to regulated-customer-identification.

These resurrect if a regulated-uplift customer is identified, at which point they belong in that uplift's separate scope document.

---

## 10. Prioritization

**Phase-0 architect meeting agenda.** Effectively nothing. §1.1 is a compliance+GTM item, not architect. The reframing closed the architect-level work.

**Phase-0 operational kickoffs.** IANA PEN, cloud-KMS sandboxes, Shamir audit. No CMVP lab, no HSM procurement.

**Pre-customer-identification prep.** §3 triggers at first customer; applies to whichever tier they're in.

**Staffing window.** §4 owner-assignment; fills as operations team staffs up.

**Pre-implementation spec pinning.** §5 technical specs; TPM-sealed provider details are new critical-path work.

**Documentation track.** §6 customer docs; two parallel tracks (commodity-default + regulated-uplift).

**Cross-team joint sessions.** §7 as dependent workstreams ramp.

**Background resolution.** §8 as adjacent work makes them relevant.

The commodity-default reframing materially compresses Phase-0 and Phase-1. CMVP lab engagement was the longest-calendar dependency; removing it from the critical path buys 6-12 months. HSM procurement was the most expensive line item; removing it until customer-triggered saves the bulk of Phase-1 hardware spend.
