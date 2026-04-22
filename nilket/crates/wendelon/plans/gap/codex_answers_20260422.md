# Wendelon Open Questions — Codex Answers

Date: 2026-04-22
Author Role: Lead Architect
Source: `plans/gap/questions_20260422.md`

## Framing

These answers are intended to drive design-lock entries and workstream execution. They are opinionated on purpose.

Rules I used while answering:

- pick a direction unless the repo genuinely does not support a responsible choice
- prefer solutions that reduce ambiguity at Gate 3 and Gate 4
- optimize for operability and correctness over elegance
- where a question spans multiple workstreams, answer it once and keep the other sections consistent

## Workstream A — Governance, ADR Closure, and Calendar Dependencies
### Source: `claude_workstream_A_20260422.md`

No explicit open question was preserved in the aggregate under this heading.

## Workstream B — Crypto Custody and Key Lifecycle
### Source: `claude_workstream_B_20260422.md`

1. **Shamir library selection**
   Answer: choose `vsss-rs` provisionally. It is the best fit for a serious custody path because verifiable commitments are worth the extra complexity. Do not let this block Phase 1 provider work: if the crypto review finds an issue, swap the escrow implementation behind a narrow internal interface.

2. **Custodian selection for escrow**
   Answer: use a mixed `2 executive + 2 engineering/security + 1 external trustee/legal` model. All-employee custody is too insular; all-executive custody is too operationally weak.

3. **Is BYOK required for Phase 1 or Phase 2?**
   Answer: BYOK is not required for the first customer-facing pilot at Gate 3. It becomes a Gate 4 requirement for regulated Fortune 500 pilots.

4. **Physical HSM vendor for lab testing**
   Answer: default to Thales Luna for the first physical HSM validation target. It is the most procurement-realistic and least likely to surprise us on PKCS#11 mechanism coverage.

5. **Cryptoperiod for Tenant KEKs**
   Answer: default to `365 days`, permit explicit override to `730 days` for customers with a documented policy exception. One year is the right forcing function.

6. **Is `retire_key` separate from `rotate`?**
   Answer: yes, separate. Rotation without immediate retirement is required for legal-hold, phased cutover, and rollback-safe operations.

7. **Per-tenant Root KEK — when?**
   Answer: not Gate 3 and not Gate 4. Defer to regulated GA / post-GA staging. For customers who need that isolation earlier, the compensating control is a dedicated cluster.

8. **DEK re-wrap and plaintext-to-ciphertext migration sweeper sharing**
   Answer: share the scanning/scheduling framework, keep the state machines separate. Shared mechanics, separate business logic.

9. **Memory-dump attack surface beyond zeroization**
   Answer: `mlock + zeroize + bounded lifetime` is enough for the regulated pilot. Do not add `mprotect`/lazy-unlock in this phase; it is too intrusive for the current runtime and not the highest-risk gap.

10. **BYOK cross-KMS replication for DR ownership**
   Answer: the customer owns KMS presence in every DR region they expect to restore into. Wendelon should not replicate customer key material on their behalf.

11. **Primary-plan vs fallback-plan for provider shipping**
   Answer: hybrid plan. AWS KMS must be production-ready for Gate 3. GCP KMS and Azure Key Vault must be ready by Gate 4. PKCS#11 can trail to Gate 5 unless a design partner requires it sooner.

12. **Evidence criterion for "plaintext buffers adequately zeroized"**
   Answer: the minimum acceptable evidence is:
   - static coverage proof that all plaintext slab buffers go through the zeroizing type
   - deterministic runtime memory-pattern test in a controlled harness after drop/release
   - dedicated io_uring completion-path test proving zero before reuse/unmap
   Do not rely on a generic whole-process memory grep as the primary proof. If a lab later wants more, we add more, but this is enough for internal signoff and should be confirmed at lab kickoff.

## Workstream C — Identity, RBAC, and Authorization
### Source: `claude_workstream_C_20260422.md`

1. **Cert-service HA topology**
   Answer: `3` nodes by default. Offer `5` as a regulated-SKU deployment option, not the default.

2. **Signing HSM per-node or shared**
   Answer: shared HSM cluster by default. Per-node HSMs create too much key-replication and ceremony complexity for v1.

3. **Is `compliance_auditor` a customer role or vendor role?**
   Answer: customer role by default. Vendor-held auditor access must be customer-issued, explicit, and exceptional, not built into the role model as a standing entitlement.

4. **Break-glass scope narrowing — is five the right set?**
   Answer: yes. Keep the five named scopes: `diagnostic`, `tenant_ops`, `cluster_ops`, `key_ops`, `full`.

5. **Dual-auth signature aging**
   Answer: `5 minutes`.

6. **Revoked operator cert mid-pending-proposal**
   Answer: the proposal does not apply. Re-validate all signatures against current revocation state at apply time.

7. **IdP integration: JWT revocation propagation**
   Answer: not mandatory for Gate 3. For Gate 4, support IdP-triggered revocation hooks where available, but keep short-lived certs as the baseline control.

8. **Scoped `tenant_admin` grammar now or later**
   Answer: define the minimal grammar now. At minimum support:
   - all tenants
   - explicit tenant allowlist
   Do not defer this entirely.

9. **Ed25519 vs ECDSA for proposal signatures**
   Answer: use `ECDSA-P256` for both builds. The consistency is worth more than the Ed25519 speed win here.

10. **Should the cert-service manage tenant certs too?**
   Answer: not as a general tenant CA in v1. It should manage node, operator, and short-lived query/session credentials. A broader tenant-cert model needs a Phase 2 ADR extension.

11. **Query-session cert revocation at IdP logout**
   Answer: use short lifetime as the primary control. Explicit logout-triggered revocation is best-effort, not a correctness dependency.

12. **Predicate-pushdown identity propagation**
   Answer: compute pushdown must execute under the query/session identity for audit attribution, with resource and admission control enforced server-side by Wendelon.

## Workstream D — Audit, Evidence, and Retention
### Source: `claude_workstream_D_20260422.md`

1. **Sampling policy for range queries**
   Answer: `100%` query-start always. For regulated tenants, `100%` completion as well. For non-regulated tiers, completion records may be size-thresholded or sampled, but the start record stays mandatory.

2. **Is `compliance_tier` static or dynamic?**
   Answer: dynamic upward, controlled downward. A tenant may move to a stricter tier through an audited workflow. Relaxing a tier requires explicit approval and should never silently reduce retained evidence.

3. **WORM default: governance or compliance mode?**
   Answer: governance mode by default, compliance mode opt-in per customer/tier.

4. **Signed-root key separation from mTLS node key**
   Answer: separate keys for regulated SKU. Shared key material is not acceptable there.

5. **Kafka topic partitioning**
   Answer: partition by `node_id`.

6. **Retention enforcement layer**
   Answer: local retention sweep must check confirmed archival before deleting local authoritative files.

7. **Trace-id in audit vs shorter observability retention**
   Answer: acceptable. `trace_id` is best-effort correlation, not the evidence source of truth.

8. **Non-English reason strings in break-glass**
   Answer: support UTF-8, no language restriction, bounded length.

9. **Audit archive migration during cluster rebuild**
   Answer: emit an explicit `cluster.rebuild`/fork marker and teach verification tooling to follow the chain transition.

10. **Pseudonymization for GDPR — when?**
   Answer: Gate 5 / regulated GA by default. If an EU design partner forces it earlier, ship a narrow minimum-viable path under explicit scope, not a rushed general solution.

11. **Operator-facing verification UX**
   Answer: CLI summary mode is enough for pilot. Dashboard integration can wait for GA.

12. **Toron trace correlation — request trace or session trace?**
   Answer: request trace. Session-level correlation belongs on the Toron side.

## Workstream E — FIPS 140-3, Supply Chain, and Release Compliance
### Source: `claude_workstream_E_20260422.md`

No explicit open question was preserved in the aggregate under this heading. The actionable FIPS/compliance questions appear under Workstream J below.

## Workstream A: Governance, ADR Closure, And Calendar Dependencies
### Source: `codex_workstream_A_20260422.md`

1. **Which ADRs beyond 018/020/025/026 are truly blocking for the first customer-facing pilot?**
   Answer: ADR `021` is also blocking. ADRs `019`, `027`, and `029` should be accepted in Phase 0 if possible, but only `021` joins the hard blocking set for Gate 3.

2. **Must the review group sign every accepted ADR?**
   Answer: every blocking ADR, yes. Low-risk ADRs may be delegated only after the blocking set is closed.

3. **Canonical home for the design-lock decision log**
   Answer: keep it in `plans/gap/design_lock_20260422.md`, not in the ADR directory. ADRs hold architecture decisions; the design-lock log holds narrower program decisions.

4. **How aggressively do we gate Phase 1 on external calendar items that are started but not yet completed?**
   Answer: do not block Phase 1 on completion of CMVP/IANA/vendor processes. Block only on them being formally initiated and owned.

5. **Hard one-ADR-per-week cadence or batching?**
   Answer: one per week for blocking ADRs, then batching is acceptable.

## Workstream B: Storage And Stellarator Correctness
### Source: `codex_workstream_B_20260422.md`

1. **Is local durable append a hard requirement before quorum success?**
   Answer: yes, if the local leader is a member of the replica set. Do not allow pure-remote quorum in that case.

2. **What node-fencing trigger is acceptable when the pending-intent store fails?**
   Answer: immediate write failure always. Immediate leader step-down/fencing on integrity-threatening errors (`EIO`, corruption, sync failure). For obviously transient resource errors, fail the request and step down after a short repeated-error threshold. Default post-step-down fence window: `30s`, with a local storage self-check required before the node becomes leadership-eligible again.

3. **Expose `FinalizationRecoverablePending` directly or normalize it?**
   Answer: normalize it into a stable retryable client error with `Retry-After` or equivalent metadata. Keep the richer state internally and for trusted upstreams like Toron.

4. **Separate background finalization worker or existing wakeup/replay path?**
   Answer: use a dedicated background finalization/replay worker. The current wakeup path can be reused internally, but the behavior must become an explicit subsystem.

## Workstream C: Transport, Admission Control, And Runtime Stability
### Source: `codex_workstream_C_20260422.md`

1. **Is per-device worker mapping mandatory for Gate 2?**
   Answer: no. WAL/slab separation is mandatory for Gate 2. Per-device mapping is a later scale optimization unless the target hardware/customer demands it.

2. **Reject early or queue and shed later?**
   Answer: both. Reject early on global pre-auth saturation, then use bounded authenticated queues with shedding for post-auth fairness.

3. **How much platform-specific topology discovery do we own?**
   Answer: minimal Linux-focused discovery plus explicit config overrides. Do not build a full hardware-inventory subsystem.

4. **Which live-path aborts are truly invariant failures, and who signs off?**
   Answer: only memory-safety-adjacent impossible states and post-validation internal invariants. Sign-off required from the runtime lead and architecture owner; security lead joins for auth/crypto paths.

## Workstream D: Observability, Validation, And SLO Evidence
### Source: `codex_workstream_D_20260422.md`

1. **Jepsen or narrower in-house checker first?**
   Answer: narrower in-house checker first for speed, Jepsen before regulated pilot for external credibility.

2. **Exact soak duration and load shape before `COMMITTED` SLO**
   Answer: minimum of four consecutive weekly soaks, each with at least `24 hours` at representative mixed load, plus one `72-hour` release-candidate soak. Representative load means roughly `60/40` read/write, `80%` of target peak throughput, and injected failover/restart churn during each run.

3. **Which lock-contention metrics emit directly?**
   Answer: emit direct blocked-time/queue-wait metrics only for the top shared hot-path structures: admission queues, buffer pool, pending-finalization queue, KEK cache, and cert/CRL state. Infer the rest from latency and queue behavior.

4. **How much trace sampling is acceptable?**
   Answer: `100%` for errors and finalization/replay failures, tail-sampled slow-path traces above the target threshold, and `1-5%` baseline success sampling.

## Workstream E: Crypto Custody And Key Lifecycle
### Source: `codex_workstream_E_20260422.md`

1. **Is one cloud KMS provider enough for Gate 3?**
   Answer: yes for Gate 3, no for Gate 4. AWS-only is acceptable only if the first design partner is on AWS and we are explicit about that limitation.

2. **Do we need PKCS#11 before regulated pilot?**
   Answer: no, not categorically. We need a documented plan and interface by Gate 4, and implementation if a target customer requires on-prem/HSM posture.

3. **What evidence proves plaintext buffers are adequately zeroized?**
   Answer: static path coverage + deterministic runtime memory-pattern tests in a controlled harness + io_uring reuse/unmap tests. That is the internal bar.

4. **How do we version and audit escrow ceremonies?**
   Answer: treat ceremonies as versioned operational artifacts with signed minutes, participant list, canary-unwrap proof, and explicit supersession records.

5. **Should maximum tenant-KEK age be enforced at state-machine apply, write admission, or both?**
   Answer: both is ideal; write admission enforces immediately, state-machine/state reconciliation prevents policy drift.

## Workstream F: Identity, RBAC, Authorization, And Governance
### Source: `codex_workstream_F_20260422.md`

1. **Which operations absolutely require dual-auth?**
   Answer: tenant deletion, key retirement/destruction, all break-glass issuance, and quorum/durability-affecting membership changes.

2. **Should break-glass issuance itself require dual-auth?**
   Answer: yes, always. Scope only changes whether we later add stricter approval for `full`; it does not change the baseline rule that no single principal may mint a break-glass cert alone.

3. **What revocation distribution model is simplest while meeting fail-closed posture?**
   Answer: state-machine-distributed CRL data if we can keep implementation disciplined. It minimizes external dependencies.

4. **How far must IdP integration go before the first regulated pilot?**
   Answer: enough to prove the authoritative mapping and revocation model with at least one major IdP and one generic SAML path. Do not try to solve every IdP before Gate 4.

5. **Does the cert-rotation service need its own consensus substrate?**
   Answer: do not introduce a second consensus technology. Reuse `openraft` or the cluster's existing trust-state replication model for CA/CRL state, and keep the cert service state machine narrow. A second independent control plane is the wrong trade here.

## Workstream G: Audit, Evidence, And Retention
### Source: `codex_workstream_G_20260422.md`

1. **Is local NDJSON plus WORM enough for the first customer-facing pilot?**
   Answer: yes. That is the cleanest Gate 3 answer.

2. **Default sampling posture for read events in a regulated pilot**
   Answer: `100%` for regulated tenants and cross-tenant principals. Sampled reads are for non-regulated tiers only.

3. **Should syslog remain thin?**
   Answer: yes. Keep syslog intentionally thin. Do not turn it into a second canonical evidence path.

4. **How should verification results be presented to operators?**
   Answer: terminal-friendly CLI summary first, signed JSON attestation for auditors second.

5. **Do we standardize a seven-year default now, or only for regulated tiers?**
   Answer: standardize it for regulated/compliance tiers now. Do not force seven years on non-regulated internal tenants by default.

## Workstream H: Node Replacement, DR, And Operator Runbooks
### Source: `codex_workstream_H_20260422.md`

1. **Is the first-party Kubernetes Operator mandatory for Gate 3?**
   Answer: not strictly, but the fallback must be a tightly specified and validated automation contract. For any shared serious pilot, the operator is the right answer.

2. **Do we require cluster-wide restore tooling before customer-facing pilot?**
   Answer: yes, at least enough to execute a documented cluster-scoped restore. Manual node-image folklore is not enough.

3. **Is PITR a Gate 4 or Gate 5 requirement?**
   Answer: Gate 5 by default.

4. **Who owns ongoing runbook quality once features hand off to operations?**
   Answer: operations owns currency; feature owners own technical correctness until formal handoff. Both signatures are required on critical runbook updates.

5. **Must the backup manifest live in storage physically separate from the live L2 tier?**
   Answer: yes for regulated pilots. Separate failure domains are mandatory there.

## Workstream I: Toron And Ecosystem Integration
### Source: `codex_workstream_I_20260422.md`

1. **Which team owns the authoritative SSO/SAML to storage-auth mapping?**
   Answer: the Toron team owns the front-door identity translation, Wendelon owns the storage enforcement contract. The mapping contract is jointly versioned.

2. **How much of JDBC/ODBC compatibility must be proven before regulated pilot?**
   Answer: prove the officially supported drivers/endpoints for the pilot, document the rest as unsupported or roadmap. Do not claim generic compatibility.

3. **Are reference ingest patterns enough for Gate 4?**
   Answer: yes, if they are tested end to end. One maintained connector is desirable but not mandatory.

4. **Minimum trace context that must survive the Toron to Wendelon hop**
   Answer: W3C trace id, span/parent relationship, tenant identity, and request classification metadata sufficient for audit/latency attribution.

5. **Concrete SAML-to-short-lived-storage-credential exchange for Gate 4, or mock-IdP proof enough?**
   Answer: Gate 4 requires the real exchange for at least one supported identity path. Mock-IdP alone is not enough.

## Workstream J: FIPS, Supply Chain, And Release Compliance
### Source: `codex_workstream_J_20260422.md`

1. **Acceptable pilot language if CMVP is in progress**
   Answer: "FIPS-ready build; CMVP validation in progress; not yet FIPS-certified." Nothing stronger.

2. **Is `rustls-aws-lc-rs` mature enough, or do we need fallback posture?**
   Answer: plan for it as the primary path, but carry an explicit fallback posture in the release plan. Do not bet the entire schedule on upstream perfection.

3. **Minimum SLSA target before regulated pilot vs regulated GA**
   Answer: regulated pilot requires signed artifacts plus a documented SLSA path, effectively L2-ish posture. Regulated GA should target SLSA L3.

4. **Which compliance artifacts are mandatory for the first regulated pilot?**
   Answer: pilot evidence packet, SBOM, signing verification guidance, FIPS posture statement, and security architecture/control mapping summary. The fuller compliance pack can mature toward GA.

5. **What platform set do we certify first?**
   Answer: Linux `x86_64` and `aarch64` only. Explicitly out of scope initially: Windows, macOS, and non-Linux server platforms.

## Wendelon Workstream A: Closed-Loop Healing, DR, and Operator Tooling (WS7)
### Source: `gemini_workstream_A_20260422.md`

1. **Backup Storage Provider**
   Answer: backups must live separately from the live L2 tier. Use the same cloud provider if needed, but not the same logical bucket/account boundary.

2. **Operator Scope**
   Answer: the `wendelon-operator` is strictly for the Wendelon storage tier. Do not make it responsible for Toron compute nodes.

3. **Cross-Region Active Replication vs cold restore**
   Answer: cold restore alone is too weak for a serious regulated target with a four-hour RTO. We need asynchronous cross-region replication or a warm standby posture by the regulated pilot/GA horizon.

## Wendelon Workstream B: Toron Compute Tier & Ecosystem Integration (WS8)
### Source: `gemini_workstream_B_20260422.md`

1. **Predicate Pushdown Security**
   Answer: do not allow arbitrary WASM-style compute pushdown in the first regulated pilot. Use a constrained, versioned compute AST or vetted operator set. Execution runs under the query/session identity and is admission-controlled by Wendelon.

2. **Quota Visibility**
   Answer: Toron should surface storage quota as a first-class query error condition. Wendelon remains the source of truth and should return structured quota-violation codes plus remaining-quota metadata when cheap enough; Toron may cache snapshots for UX, but it must propagate storage-authored quota outcomes.

3. **Schema Evolution**
   Answer: Toron owns schema intent; Wendelon owns durable schema state. `ALTER TABLE` must become a versioned proposal serialized through Wendelon's Raft state, not an out-of-band side effect. Wendelon does not need to understand full SQL semantics, but it does need to durably sequence schema-version transitions.

## Wendelon Workstream C: Transport, Admission Control & Runtime Stability (WS2)
### Source: `gemini_workstream_C_20260422.md`

1. **Transport Truth-Up**
   Answer: retract for the pilot. Do not spend immediate schedule on GSO/GRO + io_uring QUIC integration unless benchmarking later proves it is the next gating issue.

2. **Buffer Pooling**
   Answer: do not rewrite immediately. Measure contention after the bigger runtime refactors land; shard only if the data says it matters.

3. **Connection Fairness before auth**
   Answer: enforce coarse IP/global token buckets pre-auth, then shift to tenant fairness post-auth. Pre-auth tenant fairness is impossible by definition.

## Wendelon Workstream D: Storage and Stellarator Correctness (WS1)
### Source: `gemini_workstream_D_20260422.md`

1. **Leader Fencing Thresholds**
   Answer: if the local persistence path fails with integrity-threatening errors, step down immediately. If it is clearly transient resource pressure, fail the request and step down on a short threshold.

2. **Client Retry Semantics**
   Answer: normalize into a stable retryable error with backoff guidance. Do not expose raw internal state enums to general clients.

3. **Orphaned Remote Data**
   Answer: mark it for asynchronous cleanup through the sweeper/tombstone path. Do not synchronously block the failed write on remote cleanup.

## Wendelon Workstream E: Observability, Validation, and SLO Evidence (WS3)
### Source: `gemini_workstream_E_20260422.md`

1. **Cardinality Limits**
   Answer: no unbounded per-tenant labels in default exported metrics. Use sampled logs, debug endpoints, or top-N views for tenant-specific detail.

2. **Linearizability Harness**
   Answer: build the narrower in-house checker first, then run Jepsen before the regulated pilot bar. The two are complementary, not mutually exclusive.

3. **Trace Sampling**
   Answer: tail-based sampling in the collector:
   - keep `100%` errors
   - keep `100%` writes/finalization failures and replays
   - keep traces over the target latency threshold
   - sample a low baseline of successful requests
