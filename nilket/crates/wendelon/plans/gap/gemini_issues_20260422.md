# Wendelon Unresolved Issues & Friction Points

**Author:** Lead Software Architect (Gemini)
**Date:** April 22, 2026 (v6 — Final Closeout)
**Context:** The final, distilled list of unresolved issues. All previously resolved items have been cleaned out.

---

After reading the final issue lists from Claude and Codex, it is clear that the **architectural** review process has successfully closed. The lingering items they flagged (such as the marketing language for BYOK and whether the NIST 800-53 summary is included in the default compliance pack) are GTM/Compliance decisions, not engineering blockers. 

There is exactly one deployment reality that we must carry forward into execution:

### 1. Non-Regulated / Cheap VPS Deployment Profile
*   **The Insight:** The user has explicitly mandated that Wendelon must be runnable on a "cheap VPS with no HSM". A $5/mo VPS is a primary target for developer adoption and internal pilots, whereas bare metal servers with HSMs run 10 to 100 times that cost. Our previous debates over shared vs. per-node HSM topologies implicitly assumed an enterprise-budget deployment as the *only* target. We must officially adopt a software-only fallback profile as a first-class citizen to ensure economic viability for non-regulated users.
*   **The Resolution Path:** I have updated my answers document to explicitly rule that a software-only local KMS and software-only cert-service signing keys (protected merely by OS file permissions) are officially supported. This resolves the remaining procurement-gated HSM topology issue for non-regulated, low-cost deployments, ensuring we don't price out the majority of our potential adoption base.

---
*Note: With the addition of the software-only deployment profile, there are zero unresolved architectural friction points blocking Phase-0 exit. The council is aligned, and engineering execution can begin.*