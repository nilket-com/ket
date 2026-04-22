# Wendelon Unresolved Issues — Codex

Date: 2026-04-22
Author Role: Lead Architect
Sources Reviewed:
- `plans/gap/claude_answers_20260422.md`
- `plans/gap/codex_answers_20260422.md`
- `plans/gap/gemini_answers_20260422.md`

## Framing

This file excludes questions where the three answer documents have effectively converged. It lists only the material issues that are still unresolved because the answers disagree, the milestone is still ambiguous, or external/customer input is still required.

Where useful, I give the resolution I would lock if the council forced the decision now.

## Tier 1 — Design-Lock Required

1. **BYOK scope for the first regulated pilot**

`codex_answers` treats productized BYOK as a Gate 4 requirement. `claude_answers` and `gemini_answers` both defer full productization and allow a manual customer-KMS compensating path.

This is still unresolved because it changes what “regulated pilot” means. My recommendation remains: if the pilot is marketed as regulated-ready and multi-tenant, productized BYOK belongs in Gate 4. If we cannot afford that, then we should explicitly narrow the pilot claim to Wendelon-managed keys and stop implying broader crypto-custody parity.

2. **KMS provider and PKCS#11 shipping cadence**

`codex_answers` and `gemini_answers` align on AWS KMS at Gate 3, GCP/Azure at Gate 4, and PKCS#11 only when a target customer justifies it. `claude_answers` still pushes a more aggressive plan with GCP/Azure targeting Gate 3 and PKCS#11 at Gate 4.

This needs one locked milestone table. My recommendation: AWS KMS at Gate 3, GCP/Azure at Gate 4, PKCS#11 only for a named regulated customer or else Gate 5. Schedule optimism here is expensive.

3. **Cert-service HSM topology**

`codex_answers` and `gemini_answers` prefer a shared HSM cluster for v1. `claude_answers` prefers per-node HSMs with clustering semantics handled by the HSM vendor.

This is a real architecture and procurement decision, not just an implementation detail. My recommendation: shared HSM cluster for v1, per-node HSM only when procurement and operator maturity justify the extra ceremony.

4. **Cert-service scope for tenant and end-user certificates**

`codex_answers` and `gemini_answers` keep v1 narrow: node, operator, and short-lived query/session credentials, but no general tenant CA. `claude_answers` has moved toward a universal cert-service posture for v1.

This remains unresolved because it affects trust boundaries, CRL scale, and ADR 018 follow-on work. My recommendation: keep v1 narrow. Query/session issuance belongs there; a general tenant application CA does not.

5. **Regulated-pilot DR posture and the 4-hour RTO claim**

`codex_answers` and `gemini_answers` both say cold restore alone is too weak for a serious regulated target and call for warm standby or async cross-region replication by the regulated-pilot/GA horizon. `claude_answers` still allows cold restore at Gate 4 with cluster-size tiering.

This is unresolved because it changes both architecture and commercial promises. My recommendation: do not promise a 4-hour regulated RTO without warm standby or async replication unless the pilot contract hard-caps cluster size and data volume.

6. **Gate 3 restore and operator automation bar**

`codex_answers` requires cluster-wide restore capability before a customer-facing pilot. `claude_answers` allows a documented, rehearsed cluster-scoped manual sequence at Gate 3 and pushes automated tooling to Gate 4. The operator requirement itself is also split: Codex allows a validated automation contract; Claude wants the first-party operator for Kubernetes pilots.

This still needs a single pilot bar. My recommendation: Gate 3 requires a rehearsed cluster-scoped restore runbook and scripted validation, but not necessarily a full first-party operator. The operator should be preferred, not universally mandatory.

7. **Jepsen milestone**

`codex_answers` and `gemini_answers` require Jepsen before the regulated-pilot bar. `claude_answers` argues Jepsen is procurement-visible, but elsewhere still leaves room for it to slip to the regulated-GA horizon.

This has to be locked because it drives test investment now. My recommendation: in-house checker first, Jepsen before the first regulated pilot, not after.

8. **FIPS transport boundary fallback posture**

`codex_answers` and `gemini_answers` carry an explicit fallback posture around `rustls-aws-lc-rs`. `claude_answers` prefers a full migration and treats a hybrid posture as a documentation hazard.

This is unresolved because it affects release risk and certification language. My recommendation: keep the fallback posture in the release plan. Betting the schedule on one upstream path is not disciplined.

9. **Minimum compliance artifact set for the first regulated pilot**

`codex_answers` and `gemini_answers` emphasize an evidence packet, SBOM, signing guidance, FIPS posture statement, and security/control mapping summary. `claude_answers` additionally makes SOC 2 scoping, PCI applicability, HIPAA mapping, and NIST 800-53 mapping mandatory at Gate 4.

This needs a single customer-facing minimum. My recommendation: lock the product artifact bundle at the smaller set, and treat sector/framework mappings as customer- or sector-specific overlays unless a named pilot explicitly requires them.

## Tier 2 — Should Be Locked Before Gate 3 Exit

10. **Identity integration breadth for regulated pilots**

`codex_answers` requires one major IdP plus a generic SAML path by Gate 4. `claude_answers` pushes for two concrete IdP implementations plus generic SAML. `gemini_answers` does not materially narrow this.

My recommendation: one production-quality major IdP plus generic SAML for Gate 4. A second first-class IdP should be customer-driven, not unconditional scope.

11. **Ecosystem compatibility bar**

`codex_answers` says prove only the officially supported drivers/endpoints and do not imply generic JDBC/ODBC coverage; one maintained connector is desirable but not mandatory. `claude_answers` wants the top three client paths proven and at least one maintained connector.

This is unresolved because it changes pilot promises and staffing. My recommendation: explicitly name the supported drivers for the pilot and avoid a blanket compatibility claim. Require one maintained connector only if it is tied to an actual pilot ingestion path.

12. **Default retention and WORM policy outside regulated tiers**

`codex_answers` scopes the seven-year default to regulated/compliance tiers and keeps governance mode as the general default. `claude_answers` goes further, pushing a 2555-day default across the product with opt-down, and a more tier-specific compliance-mode posture.

This is still a product-policy issue, not just a documentation detail. My recommendation: keep seven-year retention and stronger immutability defaults scoped to regulated/compliance tiers. Do not impose regulated defaults on non-regulated tenants by accident.

13. **Exact zeroization evidence package**

All three answers agree on static-path proof and `io_uring` reuse/unmap testing, but they still differ on how strong the runtime memory test needs to be. `codex_answers` wants a deterministic harness, `claude_answers` demotes the memory grep to advisory on target platforms, and `gemini_answers` is skeptical of runtime memory-grep style evidence entirely.

This should be closed with the CMVP lab, not by further internal debate. My recommendation: static-path proof plus controlled target-platform harness plus `io_uring` lifecycle tests. Do not rely on a broad whole-process grep as the primary evidence.

## Tier 3 — Empirical or Customer-Driven, But Still Open

14. **BufPool sharding commitment**

`codex_answers` still leans benchmark-first after the larger runtime refactors. `claude_answers` and `gemini_answers` are more convinced that the global mutexed pool will become the next bottleneck and want sharded designs queued early.

This is open, but it is an empirical decision rather than a council doctrine issue. My recommendation: benchmark immediately after the `io_uring` split and pre-stage the sharded design so it can land without another sprint if the contention shows up, which it probably will.

15. **Pilot messaging around restore posture and cluster-size limits**

The answers are closer than before, but the documents still do not align on whether Gate 4 can honestly support cold-restore-only for small clusters while claiming a regulated pilot.

This is partly technical and partly commercial. My recommendation: if cold restore remains the only path, publish an explicit size-bounded RTO table and stop using a flat cross-customer recovery claim.

16. **Who decides the final connector and BI-client commitments**

The documents still disagree on whether this is primarily an engineering scope question or a business-development one.

This is unresolved because the wrong owner will create fake commitments. My recommendation: architecture names the minimum technical bar, but product/GTM must own the final list of promised integrations before Gate 3 collateral is published.
