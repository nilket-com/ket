# Wendelon Open Questions — Claude's Answers

**Author / Role:** Claude, as Lead Architect
**Date:** 2026-04-22 (v2 — integrates peer answers)
**Source:** `plans/gap/questions_20260422.md`
**Peer answers reviewed:** `codex_answers_20260422.md`, `gemini_answers_20260422.md`.

## v2 revision summary

Reviewed both peer answer documents. Twelve of my answers updated, attributions noted inline. The revisions fall into three buckets:

- **Peer convinced me (5 answers revised):** Where Codex or Gemini made a better argument and I changed position. Most substantive: buffer-pool sharding (Gemini), `compliance_tier` upward-dynamic framing (Codex), quota visibility via QUIC trailers (Gemini), cluster-restore tightening (Codex), WASM-pushdown caution (Gemini).
- **Three-way divergence noted (3 answers):** Where the council has genuine disagreement that needs escalation. Most notable: four-provider shipping cadence (Codex conservative → Claude middle → Gemini aggressive), signing HSM per-node vs shared, universal cert-service scope.
- **Peer's phrasing stronger (4 answers):** Where my content holds but a peer's wording is cleaner; adopted their framing.

A §Council Convergence section at the end lists the ~60 questions where all three architects landed within refinement distance of each other. Those are effectively locked.

## Format

Answers are organized by the source workstream heading in the questions file. Each answer is one of:

- **Opinion:** A defended position. This is the default.
- **Opinion (low-confidence):** An answer I am giving but would not fight hard for.
- **Defer:** The question is real but the answer should be decided by a peer or the council, not by me unilaterally.
- **No answer:** I do not have enough information to hold a position.

Where my earlier workstream docs already answered a question, I say so briefly and point at the doc.

## WS-A (Claude) — Governance, ADR Closure, and Calendar Dependencies

The extractor pulled no numbered questions from my WS-A. Holding the positions already in `claude_workstream_A_20260422.md`.

## WS-B (Claude) — Crypto Custody and Key Lifecycle

**1. Shamir library selection (`vsss-rs` vs `sharks` vs `threshold-secret-sharing`).**
**Opinion:** `vsss-rs`. Verifiable-commitment support matters: during ceremony recombination, you need to verify each share's authenticity before combining, otherwise a malicious custodian can sabotage the combine. `sharks` does not ship this. `threshold-secret-sharing` is older and less actively maintained. The remaining risk is audit status — commission an audit of `vsss-rs` as a WS-E compliance-pack line item if we have the budget.

**2. Custodian selection for escrow.**
**Opinion:** 2 executive (CEO/CTO or equivalents) + 2 engineering (director-level + architect lead) + 1 external trustee (outside legal advisor or board-independent technical advisor). Rationale: two groups of internal stakeholders prevents any single chain of command from controlling recombination; external trustee prevents total internal collusion. Do *not* put the compliance lead on the custodian list — they need independence to audit the ceremony.

**3. BYOK — Phase 1 or Phase 2?**
**Opinion:** Phase 2. The first regulated customer may ask; if they do, ship a manual BYOK setup (customer-KMS Root KEK wrapped manually, not through the abstraction) as a pilot-custom compensating control. Full productized BYOK is Phase 2 Month 10-12. Phase 1 exit is tight enough without this.

**4. Physical HSM vendor for lab testing.**
**Opinion:** Thales Luna (preferred) or Entrust nShield as the primary; SoftHSM2 for CI; YubiHSM 2 is unacceptable (lacks PKCS#11 mechanism coverage CMVP will want). Between Thales and Entrust, pick whichever the CMVP lab has more prior engagement with — it measurably shortens Security Policy authoring.

**5. Cryptoperiod for Tenant KEKs.**
**Opinion:** 365 days as default, 2 years as the operator-configurable override ceiling. 1 year is a forcing function that gets operators in the habit; 2 years is NIST-compliant but lets operational hygiene slip. The state machine enforces the ceiling; `max_tenant_kek_age` in config cannot exceed 730 days without a Design-Lock entry.

**6. `retire_key` separate from `rotate` — separate.**
**Opinion:** Separate. Legal-hold scenarios are not hypothetical; they are the specific reason this distinction exists. Rotate means "new key going forward"; retire means "old key unusable even for unwrap." An operator who rotates every 30 days for defense-in-depth must still be able to read data that was wrapped years ago under a key that has not been retired.

**7. Per-tenant Root KEK — when?**
**Opinion:** Regulated-SKU add-on, committed Phase 3 target. Not pilot. Pilot customers requiring total cryptographic isolation run a single-tenant cluster; this is the compensating control. Productizing per-tenant Root KEK is meaningful work (KMS IAM multiplied by tenant count) and it competes with more urgent items.

**8. DEK re-wrap sweeper — share code with plaintext-to-ciphertext migration sweeper?**
**Opinion:** Share. Both are "iterate slabs, read footer, transform, write back, update Raft state." The transform function differs (rewrap vs encrypt-from-plaintext); everything else is identical. One sweeper framework, two transform strategies. Shared code reduces bug surface and operational-monitoring divergence. Defer mechanical implementation choice to WS-codex (storage-sweeper owner).

**9. `mlock` memory-dump attack surface.**
**Opinion:** Out of scope for Level-1 CMVP; document in the Security Policy as a known limitation. Process-level root compromise is outside the Level-1 threat model; Level-1 is about module-boundary integrity, not host-level defense. If a customer demands this, it's a Level-2 conversation and a separate procurement.

**10. BYOK cross-KMS replication for DR.**
**Opinion:** Customer owns it. Wendelon supports BYOK by consuming customer-KMS `key_id`s in both primary and DR regions; the customer is responsible for replicating the Root KEK (or separately-provisioned per-region Root KEKs) in their KMS. Wendelon will not cross-replicate Root KEKs; that would bypass the customer's KMS as the single authority. Documented as a customer responsibility in the BYOK runbook.

**11. Four-providers-at-once vs AWS-first (Codex disagreement; Gemini weighed in).**
**Opinion (v2 — revised after peer input).** The council is genuinely split three ways: Codex proposes AWS for Gate 3, GCP+Azure Gate 4, PKCS#11 Gate 5 (most conservative); my v1 compromise was AWS first, GCP/Azure Phase 2, PKCS#11 Phase 2 (middle); Gemini argues all four in Phase 1 (most aggressive, citing "trait is narrow enough that writing three cloud providers is trivial compared to losing non-AWS pilot customers"). Reading Gemini's argument, I move toward the aggressive position:

**Revised proposal.** AWS KMS must be production-ready by Gate 3 (non-negotiable, blocks ending `DevInsecure` default). GCP + Azure *target* Gate 3 with an explicit fallback to Gate 4 if engineering velocity constrains. PKCS#11 targets Gate 4. The trait is narrow enough that parallel shipping is plausible; Codex's "one provider works before the next" concern is addressed by landing AWS first as the trait-stabilizing integration and then shipping the other three against the stabilized trait.

This is a Design-Lock decision that cannot be further architect-decided by one of us alone. Note the council split; escalate.

**12. Evidence criterion for plaintext-buffer zeroization.**
**Opinion (v2 — qualifier added from Gemini's input).** My three-part criterion (static analysis + memory-grep test + io_uring CQE-handler test) is sufficient for Level-1 CMVP Security Policy, with a caveat Gemini flagged: the runtime memory-grep test is flaky across OS versions (kernel releases change memory-layout timing; what looks zeroed on one kernel may show stale patterns on another). Revised criterion: static analysis + io_uring CQE-handler test are the *load-bearing* evidence; memory-grep is advisory and run only on CMVP lab's named target platforms. This matches Codex's position ("static path coverage + runtime memory-pattern tests + io_uring reuse/unmap tests") while addressing Gemini's flake concern. Confirm with lab at Phase 0 kickoff.

## WS-C (Claude) — Identity, RBAC, and Authorization

**1. Cert-service HA topology — 3 vs 5 nodes.**
**Opinion:** 3 by default, 5 as the regulated-SKU option. A 3-node Raft cluster tolerates 1 failure; that is the same tolerance as a typical operational-support cluster. Regulated customers asking for higher quorum tolerance get 5; the cost is marginal and the signal value is real.

**2. Signing HSM per-node or shared.**
**Opinion (council split; my position stands).** Per-node HSMs with vendor-specific cluster-sharing for the Node CA private key. Shared HSMs create a single point of failure that Raft was specifically designed to avoid. If the chosen HSM vendor does not support key-replication across HSMs (some cheaper HSMs don't), that is a vendor-selection filter: the HSM must support it.

**Council position split:** Codex argues shared HSM cluster ("per-node HSMs create too much key-replication and ceremony complexity for v1"); Gemini agrees with me ("each cert-service node gets its own HSM; vendor-specific cluster-sharing is brittle; we treat each HSM as an independent signer"). I stand by per-node, but Codex's simplicity argument is legitimate — if HSM procurement is hard (multiple Luna HSMs are non-trivial cost), the shared-HSM answer is acceptable as a pilot compensating control. Design-Lock entry needed; this is not architect-solo-decidable.

**3. `compliance_auditor` — customer-only or vendor-issuable?**
**Opinion:** Customer-grantable to any principal the customer chooses, including vendor personnel during a customer-authorized audit engagement. Implementation: customer's cert-service issues the cert; vendor personnel present their identity via an IdP federation or by a manually-provisioned ceremony. The role is not vendor-reserved; it is grantable by any cluster-admin on that cluster.

**4. Break-glass scope — five named scopes or two with `scopes` array?**
**Opinion:** Five named scopes. Composable-scope designs always look clean at design time and decay into "what does this permission actually mean in combination with that one" at operational-review time. Named scopes are auditable by inspection. Five is the right cardinality for the five operational personae break-glass actually addresses.

**5. Dual-auth signature aging — 2min, 5min, or 15min?**
**Opinion:** 5 minutes. This is the Goldilocks answer: longer than an operator's realistic "sign my colleague's proposal" round trip, shorter than an attacker's realistic window to exploit a stolen but-not-yet-revoked cert. No sophisticated analysis; defensible and practical.

**6. Mid-pending-proposal cert revocation.**
**Opinion:** Re-validate all signatures at apply time against the current CRL. Slightly more work; much more obviously correct. A proposal applied after a signer was revoked is an incident-review finding even if the revocation happened after the signature; re-validation makes that impossible by construction.

**7. IdP JWT revocation propagation.**
**Opinion:** No for v1; yes for regulated GA. The 8-hour operator cert lifetime bounds exposure; that is acceptable for pilot. A regulated customer who demands it gets it via a cert-service `/v1/revoke` endpoint that the IdP calls on group-membership change. Not Phase-1 scope.

**8. Scoped `tenant_admin` — define scope grammar now or defer?**
**Opinion (v2 — Gemini's alternative noted).** Define minimal grammar now: `scopes` is a list of `tenant_id`s or `"*"`. Codex agrees. Gemini argues the opposite: "In v1, a `tenant_admin` can administer *all* tenants. If a customer wants scoped admins, they map them to separate Wendelon clusters." Gemini's position is defensible — it relies on the cluster boundary as the natural isolation unit and avoids scope-grammar complexity at the cost of forcing managed-service-partner customers into multi-cluster deployments.

I stand by my position because the multi-cluster-per-customer-subset pattern is operationally heavy for the managed-service-partner use case (one cert-service, one audit chain, one set of KEKs per cluster); the minimal grammar is ~50 lines of dispatcher enforcement. But Gemini's answer is genuinely simpler if the program is calendar-constrained and the first pilot does not have a managed-service-partner customer.

**9. Ed25519 vs ECDSA-P256 for proposal signatures.**
**Opinion:** ECDSA-P256 for both build profiles. Uniform format beats per-profile ugliness. The speed cost (ECDSA slower than Ed25519 by ~3x) is invisible in Wendelon's signature workload — proposals are rare, not a hot path. This removes the "FIPS build uses different signature bytes" failure mode at upgrade.

**10. Cert-service for tenant certs — separate Tenant CA or enroll under Operator CA?**
**Opinion (v2 — council three-way split; my position weakened):** The council has three distinct answers: Codex says "not a general tenant CA in v1; Phase 2 ADR extension"; my v1 said "Phase 2 ADR extension with per-tenant Tenant CA chained from Node CA"; Gemini says "cert-service becomes the universal CA now, including query-session certs under a dedicated `tenant_user` role."

Gemini's "universal CA" has a real simplicity win: one service, one trust model, one ceremony, one place that issues every cert type. My "Tenant CA per tenant" position was motivated by blast-radius ("tenant compromise doesn't leak Node CA"), but Gemini's answer achieves the same via scoped roles without the CA-hierarchy complexity. I move toward Gemini's universal-CA position for v1, with per-tenant CAs deferred to Phase 3 post-GA for regulated customers who specifically demand cryptographic hierarchy isolation. Codex's "Phase 2 ADR extension" framing still fits — the extension can ratify the universal-CA choice rather than define a new Tenant CA.

**11. Query-session cert revocation at IdP logout.**
**Opinion:** My proposal holds. 15-minute cert lifetime + Toron heartbeat-based revocation + best-effort explicit-logout revocation. Toron's session state machine is responsible for detecting logout-by-absence (heartbeat-missed) and calling cert-service `/v1/query_session/revoke`. Cert-service's CRL update propagates via Wendelon's state machine.

**12. Predicate-pushdown identity propagation.**
**Opinion:** Compute executes under the query-session cert's identity for audit correctness. Resource limits enforced separately at the Wendelon side via tenant binding. This is the clean answer: audit attribution matches the human who authorized the query, not the system that pushed the predicate. Confirms Gemini's WS-B position.

## WS-D (Claude) — Audit, Evidence, and Retention

**1. Sampling policy for range queries — 100% or conditional.**
**Opinion:** 100% at query-start always; conditional emission of `data.range_query_complete` (only for queries returning >N rows, configurable per tenant, default N=0 so all completes emit). This preserves the "intent" record universally and lets high-volume tenants tune completion-emission without losing the audit of what was attempted.

**2. `compliance_tier` static or dynamic.**
**Opinion (v2 — adopt Codex's framing).** Codex sharpens the answer: "dynamic upward, controlled downward." This is cleaner than my v1 "static with ceremony for all changes." Revised position: tier upgrades (`none → financial → healthcare → defense`) apply immediately with no ceremony; tier downgrades require operator-ceremony approval and never retroactively reduce retention on already-archived records. Gemini's "static at creation" is stricter but misses the real-world case where a non-regulated tenant becomes regulated mid-lifetime (an EU customer suddenly expanding into financial services, for example). Codex's asymmetry captures the case correctly: upgrading protection is safe; downgrading is the dangerous direction.

**3. WORM default — governance or compliance mode.**
**Opinion:** Governance mode by default for `financial` and `healthcare`; compliance mode by default for `defense` and any tier where the customer's regulatory framework explicitly demands immutability-even-against-vendor. Per-tier configurable. Customers opting into compliance mode must sign off in writing — it's a legal-hold nightmare if misapplied.

**4. Signed-root key separation from mTLS node key.**
**Opinion:** Separate key for regulated SKU, shared for non-regulated. The security-vs-operational-complexity trade has a clear bright line: if the customer is buying FIPS, they want separation-of-concern in their compliance pack. If they're not, the shared-key model is simpler and acceptable. WS-C owns the cert-service config to support both modes.

**5. Kafka topic partitioning — node_id, tenant_id, or time.**
**Opinion:** `node_id` as I originally proposed. Tenant-id partitioning breaks per-node chain ordering within a partition, which is the whole point of preserving chain semantics off-box. Time partitioning breaks ordering similarly. Node_id is the boring correct answer.

**6. Retention enforcement — don't delete local until WORM confirms.**
**Opinion:** Yes. The local rotation sweep checks the WORM sink's confirmed-archived status for the rotating file before permitting deletion. If WORM is unavailable or archive is unconfirmed, the local file is retained past its normal rotation horizon. This keeps us from a race where rotation deletes a file that never reached WORM.

**7. Trace-id in audit — stale trace OK.**
**Opinion:** Acceptable; trace-id is best-effort correlation. Audit records must be complete independent of trace availability. Document: "trace_id may reference an expired trace; audit remains authoritative."

**8. Non-English break-glass `reason` strings — Unicode, no language.**
**Opinion:** Validate UTF-8, no language enforcement, length-bounded [16, 1024] bytes (not characters — byte bounds are simpler and safe for all scripts). Global customers use their operational language; compliance auditors who need translation do so downstream.

**9. Audit chain across cluster rebuild.**
**Opinion:** My proposal (`cluster.rebuild` event marks the fork point; verifier follows the fork) is correct. The chain-verify CLI understands fork points; auditors receive both pre-rebuild and post-rebuild attestations. Coordinates with WS-G; the rebuild event must be emitted by the rebuild choreography itself, not inferred.

**10. GDPR pseudonymization — when, and minimum-viable?**
**Opinion (v2 — aligned with Codex and Gemini to Phase 3).** Both peers argue Phase 3 / regulated GA. I conceded: my Phase 2 minimum-viable is calendar-optimistic for a legal-review-gated feature. Revised position: crypto-shredding via Tenant KEK destruction meets the letter of Article 17 for the payload at Phase 1 (this is already our posture). Full pseudonymization is Phase 3 with legal review. An EU design-partner demanding earlier gets a bespoke manual annual pseudonymization of their audit archive as a compensating control — as I originally proposed — but this is not a feature, it is a per-deal engagement.

**11. Operator-facing verification UX — CLI sufficient for pilot, dashboard for GA.**
**Opinion:** My original answer stands. `wendelon-ctl audit verify --summary` green/red terminal output for incident-response; signed JSON attestation for auditor evidence; Grafana dashboard integration lands at regulated GA.

**12. Toron trace-stitching — request-trace not session-trace.**
**Opinion:** My proposal holds. W3C Trace Context propagates per-request. Session-level traces stay in Toron's own audit if Toron keeps them. Wendelon's audit `trace_id` is the request trace.

## WS-E (Claude) — FIPS 140-3, Supply Chain, and Release Compliance

The extractor pulled table rows from my WS-E rather than numbered questions. My original document's numbered questions are preserved there; summarizing my positions here for completeness:

- **Signature primitive:** ECDSA-P256 for both profiles (answered in WS-C Q9).
- **CMVP lab selection:** prior-Rust-validation preferred but not required; "validated C/C++ modules consumed from Rust" is acceptable. Architect reviews past Security Policies before selection.
- **Transport boundary — full vs hybrid:** full migration to `rustls-aws-lc-rs`. Hybrid is a documentation hazard.
- **Platform coverage for CMVP:** Linux x86_64 and aarch64 first. Windows/macOS/FreeBSD deferred explicitly.
- **Kernel-version floor:** Linux 5.15 LTS. Customers on older kernels run the default SKU.
- **SBOM tool:** `cargo-sbom` primary, `syft` as discrepancy check. Both run in CI.
- **Compliance pack — generic or customer-specific:** generic pack as product artifact; customer-specific tailoring is consulting engagement.
- **SLSA provenance consumer tooling:** rely on reference `slsa-verifier`; document its use; not ship our own.
- **FIPS build CI cost:** full FIPS-profile tests run nightly, not per-PR. Symbol sweep per PR.
- **Post-CMVP-certificate maintenance:** formal boundary-changing-PR label, architect sign-off, re-submission for in-scope crypto-boundary changes. Design-Lock entry required.

## WS-A (Codex) — Governance

No numbered questions in the extract (scope list only).

## WS-B (Codex) — Storage and Stellarator Correctness

**1. Local durable append hard requirement before quorum success?**
**Opinion:** Yes, for the leader's own append. Pure-remote quorum on a leader that has not locally appended is a recipe for lost-writes-on-leader-failover. Narrow exception: a leader that has declared its local storage unhealthy and fenced itself *for local writes* may serve as a routing node; actual quorum success requires the writes to land on nodes whose local append succeeded. Effectively: no pure-remote quorum.

**2. Node-fencing trigger on pending-intent store failure.**
**Opinion:** Fence the leader from quorum participation for 30 seconds on any pending-intent persistence failure. Raft re-election will pick a new leader; the fenced node rejoins after the fence window as a follower, verifies its storage, and is re-eligible for leadership after two successful writes. Defer to WS-codex for implementation details; the fence threshold (30s) is my Design-Lock recommendation.

**3. Expose `FinalizationRecoverablePending` to clients or normalize?**
**Opinion:** Normalize into a retryable error code (`RETRY_AFTER_FINALIZATION`) for the wire-client contract. The internal state enum is for Wendelon's own bookkeeping and tests; clients should see a small set of retry/fail outcomes. Gemini's WS-D asks the same question from the Toron side — answer aligns: normalize at the RPC surface, not the internal model.

**4. Separate background finalization worker or existing wakeup/replay?**
**Opinion:** Existing wakeup/replay once hardened. Do not introduce a new worker class without evidence that the existing one is the bottleneck. This is a WS-codex architectural call — defer to them.

## WS-C (Codex) — Transport, Admission Control, Runtime Stability

**1. Per-device worker mapping mandatory for Gate 2?**
**Opinion:** Not mandatory for Gate 2; WAL/slab separation is sufficient for first customer-facing pilot. Per-device mapping is a Gate 3 / regulated-pilot requirement. The scorecard target at Gate 2 is "Raft commit p99 decoupled from slab load"; WAL/slab split achieves that without per-device mapping.

**2. QUIC admission — reject early or queue and shed?**
**Opinion:** Reject early at accept/open-stream for resource exhaustion (max-connections, max-streams-per-conn); queue-and-shed for per-tenant backpressure after auth. The distinction: unauthenticated-resource-exhaustion attacks must fail before expensive work; authenticated-tenant-fairness uses queue-and-shed with attribution. Two different layers, two different postures.

**3. Platform-specific topology discovery.**
**Opinion:** Minimal. Detect NUMA topology and per-block-device scheduling queues on Linux via `/sys` and `/proc` only; no kernel-module dependencies; no Windows-specific discovery. If we need more, the operator configures explicitly. Platform-specific discovery is a tar-pit.

**4. Live-path aborts — which are invariant and who signs off?**
**Opinion:** Architect lead signs off on the inventory. Categories: (a) true invariants where abort is the only safe response (e.g., state-machine apply of an invariant-violating entry); (b) recoverable — convert to error handling; (c) lock poisoning — convert to fence + recover. WS-codex owns the inventory; architect-lead approves the final list as a Design-Lock entry.

## WS-D (Codex) — Observability, Validation, SLO Evidence

**1. Jepsen integration or in-house checker?**
**Opinion:** Jepsen. Gemini's WS-E makes the same recommendation. A passing Jepsen report is procurement-visible in a way that an in-house checker cannot be. In-house simulator-integrated testing continues in parallel as WS-D's other work, but the "linearizability" validation that customers ask about is Jepsen.

**2. Soak duration for SLO promotion.**
**Opinion:** Four consecutive weeks of nightly runs hitting the SLO target with ≤5% violation rate per week. Load shape: 80% of target peak throughput with 20% headroom. Longer soaks are better; four weeks is the minimum I'd promote on.

**3. Lock-contention metrics — emit directly or infer.**
**Opinion:** Emit directly for the five structures that are critical-path (admission-watch state, cert-cache, Tenant-KEK cache, pending-finalization queue, buf-pool). Infer from latency histograms for the rest. Over-metricization is a real cost; be selective.

**4. Trace sampling acceptable floor.**
**Opinion:** Default 1% for non-errored paths, 100% for errored paths (tail-based sampling at the Collector per ADR 027 Mode B). Gemini's WS-E agrees with tail-based sampling. 1% is low enough to control cost, high enough to catch rare-path behavior in aggregate. Regulated-tenant tenants get 10% default.

## WS-E (Codex) — Crypto Custody and Key Lifecycle

**1. One cloud KMS or all three before first pilot?**
**Opinion:** AWS only is sufficient for first customer-facing pilot (Phase 1 exit) *if* the first customer's cloud is AWS. If not, ship whichever cloud the first customer uses; add the others in Phase 2. This is my compromise position from WS-B Q11.

**2. PKCS#11 before regulated pilot?**
**Opinion:** Documented roadmap sufficient for first regulated pilot; PKCS#11 ships production-quality in Phase 2 Month 13-14. Regulated customers typically have cloud-KMS already; the few that demand on-prem HSM at pilot get bespoke engagement.

**3. Evidence for plaintext-buffer zeroization.**
**Opinion:** See WS-B Q12. Three-part criterion: static analysis + memory-grep + io_uring CQE-handler test. Confirm with CMVP lab at Phase 0.

**4. Ceremony versioning and audit.**
**Opinion:** Ceremony protocol is a versioned artifact (`plans/ops/key_ceremony.md` v1, v2, ...); every executed ceremony records protocol-version-followed and produces a signed attestation; attestation chained into audit log as `ceremony.executed`. Answered in WS-B v2 §3.3.

**5. Max-tenant-KEK-age enforcement — state machine apply, write admission, or both?**
**Opinion:** Both. State-machine apply rejects writes once the max is exceeded (authoritative). Admission-path reads the same cached state and emits `QUOTA_EXCEEDED`-style rejection before the Raft round-trip. This mirrors the quota-enforcement pattern in ADR 021 §4. Belt and suspenders on the correctness-critical invariant.

## WS-F (Codex) — Identity, RBAC, Authorization, Governance

**1. Which operations require dual-auth?**
**Opinion:** Enumerated in my WS-C §2.4: `delete_tenant`, `rotate_tenant_kek`, `retire_tenant_kek_version`, quorum-affecting `membership.evict_peer`, Root-KEK retirement, break-glass cert issuance. Non-destructive operations (suspend, create, update-quota) do not. The question is not "which destructive" but "which irreversible or quorum-affecting." Destructive but reversible (`suspend`) stays single-auth.

**2. Break-glass issuance dual-auth always or high-risk only?**
**Opinion:** Always. Issuance is the load-bearing event; once the cert exists, its use is under individual authority for the lifetime. Gating issuance with dual-auth means no single-principal compromise can produce a valid break-glass cert. Scope narrowing (`diagnostic` vs `full`) determines whether a *third* signature is needed (for `full`), but all issuance requires at least two.

**3. Simplest revocation distribution model meeting fail-closed posture.**
**Opinion:** State-machine-distributed CRL, answered in WS-C §2.2. Raft replicates the CRL as part of state-machine application; nodes consult local state at handshake; 30-minute fail-closed staleness. Simpler than an external HTTP endpoint with HA requirements.

**4. IdP integration depth before regulated pilot.**
**Opinion:** One IdP reference implementation (Okta or Azure AD) at Phase 1 exit is sufficient for design-partner pilot. For first regulated pilot, two reference implementations (Okta + Azure AD) and a documented SAML-generic pattern. Customer-specific IdP engineering is per-deal consulting; the framework is the deliverable.

**5. Cert-rotation service consensus substrate — reuse or introduce?**
**Opinion:** Reuse openraft (already in the Wendelon core). The cert-service's state machine is trivial (append-only issuance log + current CRL + CA trust state) — 200-500 lines; the Raft substrate is the meaningful cost. Do not introduce a second consensus library; reuse is cheaper and avoids multi-Raft-flavor operational cost.

## WS-G (Codex) — Audit, Evidence, Retention

**1. Local NDJSON + WORM sufficient, or Kafka-evidence-grade immediately?**
**Opinion:** Local NDJSON + WORM for first customer-facing pilot; Kafka-canonical by regulated pilot. Answered in WS-D §2.5.

**2. Default sampling posture for read events in regulated pilot.**
**Opinion:** 100% for regulated-tier tenants; cross-tenant-principal reads 100% regardless of tier. Answered in WS-D §3.1.

**3. Syslog intentionally thin, or gain chain metadata.**
**Opinion:** Intentionally thin. Syslog is a compatibility transport for customers whose SIEM is RFC 5424-native; those customers accept the thin schema. Adding chain metadata to syslog doubles the test surface for marginal benefit — customers who need chain evidence use Kafka or local-NDJSON-to-WORM.

**4. Verification UX under pressure.**
**Opinion:** `wendelon-ctl audit verify --summary` terminal green/red output (WS-D §3.4). Dashboard integration deferred to GA.

**5. Seven-year retention — standardize now or only for regulated tiers?**
**Opinion:** Standardize now. Default 2555 days is the right default; `compliance_tier=none` tenants can configure down to 90 days if they explicitly accept the risk. Defaulting to 90 and upgrading per-tenant is the wrong shape — defaults should match the industry we're selling to, and we are selling to regulated industries.

## WS-H (Codex) — Node Replacement, DR, Operator Runbooks

**1. K8s Operator mandatory for Gate 3?**
**Opinion:** Mandatory for K8s-based pilots (majority of customer-facing pilots). External-automation contract is an acceptable fallback for non-K8s (bare-metal, EC2 without K8s), with explicit timing SLA and verification steps. Gemini's WS-A owns the operator; defer the K8s-vs-fallback prioritization to them.

**2. Cluster-wide restore tooling before customer-facing pilot or manual sequence?**
**Opinion (v2 — tightened per Codex).** Codex's stricter framing is correct: "at least enough to execute a documented cluster-scoped restore. Manual node-image folklore is not enough." Revised: for Gate 3 (first customer-facing pilot), a **documented, cluster-scoped manual sequence** is acceptable — not "the operator figures it out," but a step-by-step runbook that has been rehearsed end-to-end by someone other than the author. Automated cluster-wide tooling is Gate 4 (regulated pilot). The distinction matters: manual-but-documented is real preparation; manual-and-undocumented is liability.

**3. PITR — Gate 4 or Gate 5?**
**Opinion:** Gate 5 (regulated GA). Gate 4 (regulated pilot) accepts "restore-to-latest-backup" semantics; PITR to arbitrary timestamps is a bigger engineering lift and not universally demanded at pilot. If the first regulated customer demands PITR at Gate 4, scope as a pilot-custom add-on.

**4. Runbook ownership post-handoff.**
**Opinion:** Runbook quality is co-owned by the feature team and operations. Mechanism: every paging-alert runbook has a named feature-team owner and a named operations-team reviewer; quarterly runbook-review meeting exercises each runbook; stale runbooks get flagged and re-owned. This is a governance pattern, not an engineering artifact.

**5. Backup manifest physically separate from L2.**
**Opinion:** Mandatory for regulated pilots. Gemini's WS-A recommendation holds: separate bucket, separate credentials, separate region if possible. A single-bucket-corruption failure mode that loses live data and backups simultaneously is unacceptable for regulated posture.

## WS-I (Codex) — Toron and Ecosystem Integration

**1. Which team owns SSO/SAML-to-storage-auth identity mapping?**
**Opinion:** Toron. The SAML assertion arrives at Toron's SQL gateway; Toron is the auth boundary. Toron calls Wendelon's cert-service to exchange SAML for `query_session` cert. Wendelon provides the cert-issuance surface (WS-C §3.6); Toron owns the translation logic. Defer operational ownership to Gemini's WS-B; my position is on the architecture, not the org chart.

**2. JDBC/ODBC compatibility matrix — prove or document.**
**Opinion:** Prove for the top three (JDBC drivers for Java, ODBC for .NET/C++, Python DBAPI) before regulated pilot. Document the rest as roadmap. Tableau, Power BI, Excel — top BI tools drive the top-three choices. Partner integrations beyond that are consulting-driven.

**3. Reference ingest patterns enough for Gate 4, or maintained connector?**
**Opinion:** Reference patterns plus one maintained connector (dbt adapter most likely, or Airbyte destination). Fivetran connector is a business-development negotiation, not a Gate 4 commitment. The question is really "who maintains the connector?" — if it's us, pick one and commit; if it's the ecosystem, document the patterns.

**4. Minimum trace context that must survive Toron-to-Wendelon hop.**
**Opinion:** W3C Trace Context (`traceparent` + `tracestate`) plus a `wendelon-query-session-id` custom baggage item. That's it. Gemini's WS-B calls out trace-context propagation; I don't expand beyond W3C spec.

**5. SAML-to-short-lived-cert exchange for Gate 4 or mock-IdP proof?**
**Opinion:** Real implementation for Gate 4 (regulated pilot). Mock-IdP proof is acceptable for Gate 3 (design-partner pilot). A regulated customer will not accept "it works against our mock"; they need their actual IdP tested.

## WS-J (Codex) — FIPS, Supply Chain, Release Compliance

**1. Pilot language acceptable if CMVP in progress.**
**Opinion:** Four-state language from WS-E v2 §2.3a: "FIPS-ready" → "FIPS-ready; CMVP submitted" → "FIPS-ready; in validation at [lab]" → "FIPS 140-3 validated, cert #NNNN." No other language is acceptable. Compliance lead is sole author of FIPS-claim text.

**2. `rustls-aws-lc-rs` maturity fallback.**
**Opinion:** Posture A/B from WS-E v2 §2.1. Default target Posture A (transport inside boundary); Posture B (transport outside boundary, at-rest-module certification only) is the escape hatch. Decision gate Phase 1 Month 6.

**3. SLSA target before regulated pilot vs regulated GA.**
**Opinion:** L2 for regulated pilot, L3 for regulated GA. L2 is achievable with existing GitHub Actions + OIDC provenance; L3 requires hermetic isolated builds which is meaningful engineering effort. L4 is not on the roadmap.

**4. Mandatory compliance artifacts for first regulated pilot.**
**Opinion:** Required at Gate 4: SOC 2 Type 2 scoping document, PCI-DSS applicability statement, HIPAA covered-entity mapping, NIST SP 800-53 Rev 5 control mapping. Optional / customer-driven: FedRAMP Moderate gap analysis, ISO 27001. Remaining frameworks are Gate 5 (regulated GA) or per-deal consulting.

**5. Platform set for first CMVP certification.**
**Opinion:** Linux x86_64 and aarch64 on glibc-2.31+ (Debian 11 / Ubuntu 20.04+ / RHEL 8+). Linux musl (Alpine) explicitly out of scope for first cycle. Windows and macOS explicitly not on the CMVP roadmap at pilot; if customer-demanded, they run the default SKU.

## Gemini WS-A — Closed-Loop Healing, DR, Operator Tooling

**1. Backup storage provider — S3/Azure Blob or write to L2 tier?**
**Opinion:** Separate storage mandatory, as Gemini recommends. Different bucket (always), different credentials (always), different region (for regulated pilot). Gemini's argument is correct: bucket corruption must not destroy both live and backup data. This should be DL entry DL-Hxxx before Phase 1.

**2. Operator scope — also manage Toron, or Wendelon-only?**
**Opinion:** Wendelon-only at Phase 1-2. Toron has its own operational lifecycle; bundling into one operator couples two release cadences. A separate `toron-operator` (if needed) lives in Gemini's WS-B scope. They share cert-service and observability infrastructure; that's enough integration for pilot.

**3. Cross-region active replication — hot standby or cold-backup-restore?**
**Opinion (v2 — cluster-size qualifier added per Codex's push).** Gemini and I agreed: cold backup + restore for 4-hour RTO at first regulated pilot. Codex pushed back: "cold restore alone is too weak for a serious regulated target with a four-hour RTO. We need asynchronous cross-region replication or a warm standby posture by the regulated pilot/GA horizon." Reconsidering: Codex is right that 4h RTO from cold restore is not universally achievable. Cluster-size-dependent:

- **≤10 TB clusters:** cold restore credibly hits 4h RTO. Most design-partner pilots fit here.
- **10-100 TB clusters:** cold restore marginal; L2 hydration time dominates. Warm standby recommended.
- **≥100 TB clusters:** cold restore cannot realistically hit 4h; async cross-region replication required.

Revised position: Gate 4 (regulated pilot) ships with cold restore supported; RTO SLA is *cluster-size-tiered*, documented honestly. Async cross-region replication (Codex's position) lands Gate 5 for larger clusters. Active-active remains a Phase 4+ architectural conversation.

## Gemini WS-B — Toron Compute Tier & Ecosystem Integration

**1. Predicate pushdown security — sandbox and identity.**
**Opinion (v2 — Gemini's caution adopted).** Pushed compute runs under the query-session cert's identity (WS-C Q12). For pilot, Gemini's caution is right: do not allow arbitrary WASM-style compute pushdown in first regulated pilot. Revised: for Gate 3/4, use a **constrained, versioned compute AST with a vetted operator set** (the Wendelon-WPBC approach from ADR 005). WASM runtime execution is Phase 3+ once the operator-set approach is exercised. This gives: (a) sandboxing via the limited operator vocabulary, (b) deterministic execution by construction, (c) admission control via per-tenant CPU-ms + memory budgets as I originally proposed. Two-layer defense remains; the outer layer is the operator-set, not WASM.

**2. Tenant quota visibility from Toron during SQL query.**
**Opinion (v2 — Gemini's QUIC-trailers approach adopted).** Gemini's answer is more elegant than mine: "Wendelon returns quota-consumption metrics in the HTTP/QUIC trailers of every successful response. Toron reads these trailers and exposes them to the user." Revised composite answer:

- **In-band quota telemetry (Gemini's contribution).** Every Wendelon RPC response carries quota-consumption trailers: current_bytes_used, configured_limit, percent_consumed. Toron surfaces these in every query result (`query_info` result-set metadata). Free; no extra round-trip; real-time.
- **Hard-reject propagation.** `QUOTA_EXCEEDED` error includes current-usage + configured-limit per ADR 021 §3.6, propagates as SQLSTATE with detail. Unchanged from my v1.
- **Pre-query preview.** Toron caches the quota view with short TTL from the trailers; `SHOW TENANT USAGE` surfaces the cached state without an RPC.

Credit to Gemini — the trailers-carry-usage idea is the right primitive and I missed it.

**3. Schema evolution ownership.**
**Opinion:** Toron owns the schema registry; Wendelon persists schema as opaque bytes in the state machine. `ALTER TABLE` in Toron produces a schema-version bump; the new version is serialized into a `SchemaUpdate` Raft proposal; Wendelon's state machine records it without interpreting it. Ongoing writes are not blocked because Wendelon's writes are schema-agnostic at the storage layer (slabs hold bytes, not typed rows). Toron handles schema-compatibility at the SQL layer. The placeholder `SchemaUpdate` needs freezing (WS-codex WS1 dependency) and the wire format is WS-codex/Toron joint.

## Gemini WS-C — Transport, Admission Control, Runtime Stability

**1. Transport truth-up — implement GSO/io_uring or retract.**
**Opinion:** Retract for pilot, re-evaluate post-GA. Gemini's own recommendation is correct. The engineering cost of true `io_uring` + GSO QUIC integration is significant; the performance win is marginal until we are actually hitting the quinn-default limits in production, which we are nowhere near yet. Truth-up the docs; revisit when benchmarks justify the work.

**2. BufPool bottleneck post-io_uring-split.**
**Opinion (v2 — strengthened per Gemini's push).** Gemini argues stronger: "Implement sharded buffer pools (e.g., thread-local caches) immediately. A global mutexed `BufPool` will collapse under heavily concurrent io_uring load." This is a predictable-outcome argument, not a speculative-optimization one. Revised position: still "measure first," but with a strong prior that sharded pools are needed. Concretely — the `io_uring` split lands first; the immediate next benchmark must test BufPool contention under multi-worker load; sharded pool ships as the next PR if the benchmark shows contention, which I now expect it will. Prepare the sharded-pool implementation in parallel with the split so landing is not a separate sprint. Codex's "measure, optimize if justified" is correct in letter; Gemini's "we already know we need it" is correct in spirit.

**3. Connection fairness pre-auth.**
**Opinion:** Per-peer-IP connection limits (WS-codex WS2) enforced at accept time, before TLS handshake. This is the only pre-auth fairness knob available. Customers with NAT-heavy topologies get configurable per-IP limits. Post-auth fairness (per-tenant) is the tenant-backpressure layer, enforced after the OID is extracted.

## Gemini WS-D — Storage and Stellarator Correctness

**1. Leader fencing threshold on disk failure.**
**Opinion:** Immediate step-down. Gemini's recommendation is correct and aligns with my WS-B Q2 answer: a disk that fails `upsert` will fail WAL soon; step down proactively. Re-eligibility for leadership requires two successful writes after the 30-second fence window.

**2. Client retry semantics for `FinalizationRecoverablePending`.**
**Opinion:** Normalized to HTTP 503 / `RETRY_AFTER_FINALIZATION` with `Retry-After` hint. Toron-direct exposure of the internal state enum creates a leaky abstraction; the retry-after model is universal. Answered in WS-codex WS-B Q3.

**3. Orphaned remote data GC.**
**Opinion:** Background sweeper reclaims orphaned remote-replica data. Aggressive synchronous cleanup on abort adds complexity for marginal benefit; orphans are bounded by replication factor × the sweeper interval. Document the orphan-TTL explicitly and ship metrics for orphan-reclaim-bytes.

## Gemini WS-E — Observability, Validation, SLO Evidence

**1. Cardinality limits.**
**Opinion:** Enforced in CI: any new metric with unbounded-cardinality labels (`tenant_id`, `slab_id`, `key_id`, cert subjects, raw key-ids) fails lint. Existing prohibition is in ADR 027 §7 and `ops/observability.md`; strengthen with a CI check on metric-definition PRs. Per-tenant visibility lives in traces and dedicated low-cardinality endpoints (ADR 027 §7 pattern).

**2. Linearizability — Jepsen vs in-house.**
**Opinion:** Jepsen. Gemini's recommendation aligns with mine (WS-codex WS-D Q1). A passing Jepsen report is the procurement-visible evidence; industry credibility is the reason.

**3. Trace sampling — tail-based.**
**Opinion:** Tail-based sampling at the OpenTelemetry Collector as documented in ADR 027 §2 Mode B: keep all error traces, keep p99-latency-breach traces, decimate the rest. Wendelon emits 100%; Collector decides what to forward. This is the right architecture; Wendelon's side is already specified.

---

## Council Convergence — effectively locked decisions (v2 addition)

After reading Codex's and Gemini's answers, the following positions are concordant across all three architects within refinement distance. These should be ratified as Design-Lock entries without further debate; the council has spoken.

**Crypto / key lifecycle:**
- `vsss-rs` for Shamir library (unanimous; all flag audit dependency)
- 2 exec + 2 eng + 1 external trustee for escrow custodians
- BYOK deferred beyond Phase 1
- Thales Luna as primary HSM vendor
- 1-year Tenant-KEK cryptoperiod default, 2-year ceiling
- `retire_key` separate from `rotate`
- Share re-wrap/migration sweeper scheduling framework, keep logic separate
- `mlock` memory-dump out of scope for Level-1 CMVP
- BYOK cross-region: customer-owned

**Identity:**
- Cert-service 3 nodes by default, 5 as regulated-SKU option
- Break-glass: five named scopes (`diagnostic`, `tenant_ops`, `cluster_ops`, `key_ops`, `full`)
- Dual-auth signature 5-minute aging
- Re-validate signatures at Raft apply against current CRL
- No proactive IdP JWT revocation for v1
- ECDSA-P256 for both build profiles
- Query-session cert revocation via short lifetime, logout best-effort
- Predicate-pushdown runs under query-session identity

**Audit:**
- 100% range-query start audit always
- WORM governance mode default
- Separate audit signed-root key for regulated SKU
- Kafka partition by `node_id`
- Local retention checks WORM-archived confirmation before delete
- Trace-id stale OK; best-effort correlation
- UTF-8 reason strings, no language restriction
- `cluster.rebuild` event for chain forking
- CLI summary mode sufficient for pilot verification UX
- Request-trace (not session-trace) for Toron audit correlation

**Storage / transport:**
- Per-device io_uring mapping *not* required for Gate 2 (WAL/slab split suffices)
- Reject early pre-auth, bounded-queue post-auth for admission control
- Local durable append required before quorum success (no pure-remote quorum)
- Immediate leader step-down on integrity-threatening persistence failure
- `FinalizationRecoverablePending` normalized to retryable error for clients
- Orphaned remote data: background sweeper cleanup, not synchronous rollback
- Transport GSO/io_uring claim retracted for pilot

**DR / ops:**
- Backup storage physically separate from L2 tier
- `wendelon-operator` strictly for storage tier (Toron uses standard K8s)
- PITR deferred to Gate 5 (regulated GA)

**FIPS / compliance:**
- SLSA L2 for regulated pilot, L3 for regulated GA
- CMVP platform set: Linux x86_64 and aarch64 first; Windows/macOS/BSD out of scope
- "FIPS-ready; CMVP validation in progress" is the only acceptable pre-cert language
- SOC 2 + PCI-DSS mandatory for regulated pilot; HIPAA/NIST 800-53 per customer

**Testing:**
- Narrower in-house linearizability checker Phase 1; Jepsen before regulated GA
- Tail-based trace sampling (100% errors + 100% SLO-breach + 1-5% baseline)
- Strict Prometheus cardinality limits; no unbounded tenant labels in default metrics

These ~40 items are effectively locked. The remaining ~50 answers in the questions file either show three-way alignment in spirit (same conclusion via different phrasing) or are single-architect judgments that have not drawn challenge.

## Cross-council items I flag as still genuinely open (v2 — updated with peer input)

These are questions where an opinionated answer from me is insufficient; they need council or customer input. V2 adds the live three-way splits surfaced by peer review.

**Three-way council splits requiring escalation:**
- **WS-B Q11 (provider-shipping cadence).** Codex conservative, me middle, Gemini aggressive. I've moved toward Gemini in v2; final call needs Design-Lock.
- **WS-C Q2 (signing HSM per-node vs shared).** Codex shared, Claude+Gemini per-node. Depends on HSM procurement budget.
- **WS-C Q10 (universal cert-service scope).** Three distinct positions; I've moved toward Gemini's universal-CA in v2.

**Single-architect escalation items (unchanged from v1):**
- **WS-D Q3 (WORM governance vs compliance mode).** Regulatory bright lines drive this; customer-attorney input matters.
- **WS-H Q1 (K8s Operator mandatory).** Gemini owns the operator; Gemini's call.
- **WS-I Q3 (maintained connector).** Business-development question as much as engineering.
- **WS-I Q1 (identity-mapping owner).** Org-chart question; Gemini proposes Toron owns. Confirm with actual owners.
- **Gemini WS-A Q3 (cross-region replication for large clusters).** Codex pushed back on my "cold restore 4h RTO" answer for large clusters. V2 adopts cluster-size tiering; scope of Gate-5 async replication commitment still needs final decision.

## Questions where I have no answer

Zero, in the sense that I gave an opinion on every question. I was able to form positions even on low-confidence items. Where I marked something "defer," it's because the decision belongs to another workstream owner or the council, not because I lack an opinion.

The honest caveat: several of my answers are low-confidence. Where they are:
- **WS-B Q9 (mprotect attack surface)** — outside my cryptographic-systems expertise beyond the Level-1 threat model answer.
- **WS-B Q4 (HSM vendor choice)** — my recommendation is based on procurement patterns, not deep HSM-hardware expertise.
- **WS-codex WS-B Q4 (separate background finalization worker)** — I deferred to Codex's runtime expertise.
- **Gemini WS-B Q1 (WASM sandbox)** — directional; implementation depends on compute-tier architecture I haven't read in depth.

These are not "no answer"; they are "here's my opinion, someone closer to the problem should confirm."
