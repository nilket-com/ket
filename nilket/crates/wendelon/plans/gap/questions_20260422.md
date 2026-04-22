# Consolidated Open Questions

## Workstream A — Governance, ADR Closure, and Calendar Dependencies
### Source: `claude_workstream_A_20260422.md`

This workstream is therefore positioned as the program's *foundation*, not its *administration*. Governance here is load-bearing architecture: the decisions made or deferred in Phase 0 permanently shape what Phase 1-5 can deliver.

## Workstream B — Crypto Custody and Key Lifecycle
### Source: `claude_workstream_B_20260422.md`

1. **Shamir library selection.** `vsss-rs` vs `sharks` vs `threshold-secret-sharing` — each has trade-offs. Audit status, API ergonomics, dependency weight, constant-time guarantees. I lean `vsss-rs` for its verifiable-commitment support but want the security architect's read.

2. **Custodian selection for escrow.** Five custodians across what organizational scope? All Wendelon employees? Board-level? Mixed (two executive, two engineering, one external trustee)? This is a governance decision that the architect role should propose but not unilaterally decide. I propose: 2 executive, 2 engineering director/architect, 1 external trustee (e.g., a legal advisor).

3. **Is BYOK required for Phase 1 or can it defer to Phase 2?** BYOK adds customer-trust surface. I proposed Month 8 (late Phase 1). Argument for Phase 2: fewer things at Phase 1 exit. Argument for Phase 1: first regulated customer likely asks for it.

4. **Physical HSM vendor for lab testing.** Thales Luna vs Entrust nShield vs YubiHSM 2 (low-end). Procurement-driven choice, but architect cares: PKCS#11 mechanism coverage varies, and Security Policy quality depends on us testing against one that the CMVP lab has familiarity with.

5. **Cryptoperiod for Tenant KEKs.** NIST SP 800-57 Part 1 Rev 5 Table 1 suggests 2 years for symmetric KEK cryptoperiod. I proposed 365-day default `max_tenant_kek_age`. Is 2 years the better default for regulated? Or is 1 year a forcing function worth paying for? I lean 1 year as default with per-tenant override to 2 years allowed.

6. **Is `retire_key` a separate operation from `rotate`, or are they combined?** I proposed separate. The case for combined: operators will almost always rotate-and-retire together, and two-step operations are error-prone. The case for separate: legal-hold scenarios need rotation without retirement. I lean separate; looking for counter-argument.

7. **Per-tenant Root KEK (Phase 3 of ADR 021) — when?** The ADR defers to post-pilot. But a financial-services customer with extreme isolation needs may ask at regulated pilot. Do we commit a Phase-2 schedule, defer to post-GA, or ship as a regulated-SKU add-on?

8. **DEK re-wrap on plaintext slabs still existing.** The plaintext-to-ciphertext migration sweeper (ADR 020 §2.7) and the DEK re-wrap sweeper are architecturally similar but logically separate. Should they share code, or stay separate for clarity? I lean share; want feedback from the WS-codex side whose `storage sweeper` this sits next to.

9. **Memory-dump attack surface.** Plaintext buffer zeroization addresses one attack surface. The other is the `mlock`-protected KEK cache pages: if an attacker has process-level access they can still read `mlock`-resident memory. Do we add `mprotect` + lazy-unlock, or accept as out-of-scope for Level-1 CMVP?

10. **BYOK cross-KMS replication for DR.** If a customer brings their own KMS, the cross-region DR path requires either (a) customer-KMS in both regions, or (b) Wendelon managing cross-region KEK replication. Who owns which? This crosses into WS7 territory — worth flagging here.

11. **Primary-plan vs fallback-plan for provider shipping (new, from Codex disagreement).** Four-at-once vs AWS-first. I've put both in §2.1 with the primary being my position; the program must Design-Lock one. This is the single most contested decision in WS-B and it affects Phase 1 exit content materially.

12. **Evidence criterion for "plaintext buffers adequately zeroized" (adopted from Codex's open question).** What concrete evidence satisfies the CMVP Security Policy? I propose: (a) static analysis pass asserting every slab-plaintext allocation path goes through `ZeroizingAlignedBuf`, (b) runtime test that dumps process memory after a drop and greps for known-plaintext-patterns, (c) io_uring CQE-handler test verifying buffer zero after munmap. Is this enough, or does the lab demand a formal specification?

These should be answered by Design-Lock entries or ADR 020 addenda before Phase 1 Week 4 (when the production escrow ceremony executes).

## Workstream C — Identity, RBAC, and Authorization
### Source: `claude_workstream_C_20260422.md`

1. **Cert-service HA topology.** 3 nodes or 5? 3 is cheaper and sufficient for most customers; 5 survives two simultaneous failures. I lean 3 as default with 5 as a regulated-SKU option. Open to the opposite.

2. **Signing HSM per-node or shared.** If HSM-backed signing, does each cert-service node have its own HSM (with key replication, which HSMs vary in supporting) or do they share an HSM cluster? I lean "each node has its own HSM, with Node CA key generated in-HSM and exported to peers via vendor-specific cluster-sharing protocols." This pushes HSM complexity onto the HSM vendor, which is where it belongs.

3. **Is `compliance_auditor` a customer-operator role or a vendor role?** Today I've scoped it as a role the customer grants to their internal auditors. External (third-party) auditors on customer engagements would be the customer's problem. But regulated customers sometimes want the *vendor* (Wendelon) to provide read-only auditor access for compliance verification. Should `compliance_auditor` role be issuable to vendor-held certs?

4. **Break-glass scope narrowing — is five the right set?** I proposed `diagnostic`, `tenant_ops`, `cluster_ops`, `key_ops`, `full`. Alternative: two scopes (`narrow`, `full`) with finer control in OID `scopes` array. Five named scopes is friendlier; fewer scopes is more composable. I lean five.

5. **Dual-auth signature aging.** I proposed 5-minute max signature age. Argument for longer (15 min): accommodates slower cosigner response. Argument for shorter (2 min): narrower replay window. I lean 5 minutes as balance.

6. **What happens when an operator's cert is revoked mid-pending-proposal?** A pending proposal with operator A's signature, operator A's cert revoked, operator B cosigns — does it apply? I lean "no, pending proposals re-validate all signatures at apply time against the current CRL." This is slightly more work but more obviously correct.

7. **IdP integration: JWT revocation propagation.** If a user is removed from an IdP group, their active JWT remains valid until expiry. Operator certs issued during that window remain valid until cert expiry (8 hours). Do we add a revocation hook (IdP signals cert-service, cert-service CRL-revokes)? I lean no for v1 — the 8-hour window is bounded — but regulated customers may demand revocation propagation.

8. **`tenant_admin` role and cross-tenant visibility.** A `tenant_admin` cert has `tenant_id = null` in the OID (cross-tenant authority). This matches ADR 021 §2 dispatcher pairing rules. But a customer may want a scoped `tenant_admin` — operator who can admin only tenants X and Y. This requires `scopes` field usage; I've left the scope grammar unspecified. Design question: do we define the scope grammar now or defer?

9. **Ed25519 vs ECDSA for proposal signatures.** ADR 018 does not specify. Ed25519 is faster and simpler; ECDSA is FIPS-approved. I lean Ed25519 for non-FIPS SKU and ECDSA-P256 for FIPS SKU. This makes proposal-signature format build-profile-dependent, which is ugly. Alternative: ECDSA-P256 for both; accept the speed cost. I lean the ugly option (both); looking for counter-argument.

10. **Should the cert-service manage tenant certs too?** ADR 018 says tenant-scoped clients are `data_node` role. Tenant applications authenticating into Wendelon use... what? Node CA? A third CA? Today this is hand-waved. If cert-service becomes the universal cert authority, do tenants get their own CA (Tenant CA per tenant) or do they enroll against the Operator CA? This is a big architectural question deferred by ADR 018. Gemini's Toron work (§3.6) makes this more pressing: the `query_session` cert issuance is a per-user case of the tenant-cert design. Propose: treat tenant-cert design as a Phase-2 ADR extension to ADR 018, informed by Toron integration.

11. **Query-session cert revocation at IdP logout (new from Gemini).** Gemini proposes Toron notifies cert-service on logout. How does Toron itself know about logouts in the common cases (e.g., session timeout with no explicit logout event)? Propose: query-session cert's short lifetime (15 min) + Toron heartbeat-based revocation is the mechanism; explicit logouts are best-effort additional revocation.

12. **Predicate-pushdown identity propagation (new from Gemini's WS-B open question).** Gemini asks: "Does the compute run under the tenant's exact mTLS identity?" This connects to WS-C: if Wendelon performs compute pushdown on behalf of a Toron session, does the compute execution inherit the query-session cert's identity, or execute under the Wendelon-system identity? I propose: compute executes under the query-session cert's identity for audit-attribution correctness; resource limits are enforced at the Wendelon side using the tenant binding. Needs cross-workstream alignment with Toron (Gemini's WS-B).

The last three questions want council-level answers before Phase 2. Tenant-cert architecture and Toron-originated session identity are the next layer of identity design after operator identity is sound.

## Workstream D — Audit, Evidence, and Retention
### Source: `claude_workstream_D_20260422.md`

1. **Sampling policy for range queries.** I proposed 100%. A tenant with millions of small range queries a day may push back. Alternative: 100% at query-start, emit `data.range_query_complete` only for queries that returned >N rows. I lean against: the start record is the one that captures intent; skipping completes loses the "how much did they see" signal.

2. **Is `compliance_tier` a static tenant attribute or dynamic?** I proposed static (set at tenant creation). Case for dynamic: a tenant upgrades from non-regulated to regulated, retention floor snaps up. Case for static: avoids retention-policy-applied-retroactively complications. I lean static with a documented workflow for migrating a tenant to a higher tier (involves operator ceremony).

3. **WORM default: governance or compliance mode?** I proposed governance. Some regulatory frameworks (e.g., SEC 17a-4 strict interpretation) demand compliance mode. Default per compliance tier: governance for `financial`, compliance for `defense`? I lean per-tier configurable with governance as fallback.

4. **Signed-root key separation from mTLS node key.** ADR 025 Open Question. I argued for separate key (regulated SKU). Case for shared: one HSM, one ceremony. Case for separate: compromising the mTLS key does not compromise audit. I lean separate. Deferred to WS-C (identity workstream) since the cert-service owns signing keys — want their read.

5. **Kafka topic partitioning.** I proposed by `node_id`. Alternative: by `tenant_id` (groups a tenant's audit together but breaks cross-node chain ordering). Alternative: by event time (simpler but breaks ordering). I lean node_id; tenant-id partitioning would require maintaining per-tenant chain state per-node, which we deliberately don't do.

6. **Retention enforcement at which layer?** The rotation sweep deletes old NDJSON files; the WORM sink's object-lock handles WORM retention. They should agree but don't automatically. Design question: does the retention sweep check the sink's confirmed-archived status before deleting local? I lean yes — don't delete local until WORM confirms archived.

7. **Trace-id in audit: privacy considerations.** A trace-id links an audit record to observability data. If observability data is retained shorter than audit data (typical — metrics 30d, traces 7d), an audit record pointing to an expired trace is stale. Acceptable? I lean yes; trace-id is best-effort correlation.

8. **What about non-English reason strings in break-glass?** `parameters.reason` is a free-text string. Length-bounded to [16, 1024]. Does it support Unicode? (I lean yes, validate UTF-8.) Does it require a pre-specified language? (I lean no.) These small decisions matter for global customers.

9. **Audit archive migration during cluster rebuild.** WS-G's DR plan rebuilds clusters; what happens to the audit chain? The node_id changes on rebuild; chain continuity is per-node-id; post-rebuild the chain "forks." I propose: a `cluster.rebuild` audit event marks the fork point, and verifier tools know to follow the fork. Needs coordination with WS-G.

10. **Pseudonymization for GDPR — when?** I deferred to Phase 3. A design-partner customer in EU jurisdiction may demand it earlier. What's the minimum viable pseudonymization (e.g., hash `tenant_id` with a cluster-internal salt on events older than a threshold) vs. the right pseudonymization (full PII redaction with legal review)? I lean minimum-viable is shippable in Phase 2 but commits us to the simple model; right-thing needs Phase 3 legal engagement.

11. **Operator-facing verification UX (adopted from Codex's open question).** Signed JSON attestation is for auditors. Operators under incident pressure need a terminal-friendly summary. I added a `--summary` mode in §3.4 that produces a green/red report. Is that sufficient, or should we add a dashboard integration (grafana panel showing chain continuity and latest verification result)? I lean on the CLI for pilot; dashboard for GA.

12. **Trace-stitching for Toron-originated audits (cross-ref with Gemini's WS-B).** Gemini's Toron integration proposes Toron injects W3C Trace Context into QUIC headers, which Wendelon attaches to audit `data.read`/`data.write` events. My §3.7 already has `trace_id` field; alignment with Gemini is clean. The remaining question: does the audit record's `trace_id` correlate to Toron's *session* trace or the Toron-to-Wendelon *request* trace? I propose request-trace (W3C Trace Context propagates one trace_id); session-trace stays in Toron's own audit if they keep one.

The first three questions matter most for Phase 1 scope; the rest can be locked in Phase 2 design-lock entries.

## Workstream E — FIPS 140-3, Supply Chain, and Release Compliance
### Source: `claude_workstream_E_20260422.md`

| 5 | Lab kickoff meeting. Platform list shared. Security Policy outline drafted. |
| 5 | SBOM tooling prototyped (`cargo-sbom` evaluation). |
| 6 | Signed-artifact infrastructure prototyped (cosign). |

## Workstream A: Governance, ADR Closure, And Calendar Dependencies
### Source: `codex_workstream_A_20260422.md`

- promote load-bearing ADRs to `Accepted`
- establish the design-lock decision log
- file the IANA PEN request
- kick off CMVP lab engagement
- start KMS vendor/legal onboarding
- establish the CI program scorecard and benchmark baseline artifacts

## Workstream B: Storage And Stellarator Correctness
### Source: `codex_workstream_B_20260422.md`

- Is local durable append a hard requirement before any quorum success can be reported, or can the leader permit pure-remote quorum in narrowly defined cases?
- What exact node-fencing trigger is acceptable when the pending-intent store fails?
- Should `FinalizationRecoverablePending` be exposed to clients directly, or normalized into a retryable error code?
- Do we require a separate background finalization worker model, or is the existing wakeup/replay path sufficient once hardened?

## Workstream C: Transport, Admission Control, And Runtime Stability
### Source: `codex_workstream_C_20260422.md`

- Is per-device worker mapping mandatory for Gate 2, or is WAL/slab separation enough for the first customer-facing pilot?
- Should QUIC admission control reject early at accept/open-stream time, or queue and shed later with better attribution?
- How much platform-specific topology discovery are we willing to own in the runtime?
- Which live-path aborts are truly invariant failures, and who signs off on that list?

## Workstream D: Observability, Validation, And SLO Evidence
### Source: `codex_workstream_D_20260422.md`

- Do we integrate with Jepsen directly, or build a narrower in-house checker first?
- What exact soak duration and load shape are required before promoting an SLO to `COMMITTED`?
- Which lock-contention metrics are worth emitting directly versus inferring through latency histograms?
- How much trace sampling is acceptable before we lose confidence in rare failure-path observability?

## Workstream E: Crypto Custody And Key Lifecycle
### Source: `codex_workstream_E_20260422.md`

- Is one cloud KMS provider enough for Gate 3, or do we require all three major clouds before the first customer-facing pilot?
- Do we need PKCS#11 support before regulated pilot, or is a documented roadmap sufficient?
- What exact evidence do we require to say plaintext buffers are adequately zeroized?
- How do we version and audit escrow ceremonies so they are operationally repeatable rather than person-dependent?
- Should maximum tenant-KEK age be enforced at state-machine apply, write admission, or both?

## Workstream F: Identity, RBAC, Authorization, And Governance
### Source: `codex_workstream_F_20260422.md`

- Which operations absolutely require dual-auth, and which can remain single-auth with strong audit?
- Should break-glass issuance itself require dual-auth, or only its high-risk uses?
- What revocation distribution model is simplest while still meeting the fail-closed posture?
- How far do we need to go on IdP integration before the first regulated pilot?
- Does the cert-rotation service need its own consensus substrate, or can we keep HA without introducing a second full distributed control plane?

## Workstream G: Audit, Evidence, And Retention
### Source: `codex_workstream_G_20260422.md`

- For the first customer-facing pilot, is local NDJSON plus WORM archive enough, or do we need evidence-grade Kafka immediately?
- What is the default sampling posture for read events in a regulated pilot?
- Do we want syslog to remain intentionally thin, or should it also gain chain metadata?
- How do we present verification results so operators can actually use them under pressure?
- Do we standardize a seven-year default now, or only for regulated tenant/compliance tiers?

## Workstream H: Node Replacement, DR, And Operator Runbooks
### Source: `codex_workstream_H_20260422.md`

- Is the first-party Kubernetes Operator mandatory for Gate 3, or can a tightly specified external automation contract satisfy the first customer-facing pilot?
- Do we require cluster-wide restore tooling before customer-facing pilot, or can the first pilot use a documented manual sequence?
- Is PITR a Gate 4 requirement or a Gate 5 requirement?
- Who owns ongoing runbook quality once the feature teams hand off to operations?
- Must the backup manifest live in storage that is physically separate from the live L2 tier for regulated pilots?

## Workstream I: Toron And Ecosystem Integration
### Source: `codex_workstream_I_20260422.md`

- Which team owns the authoritative identity mapping from SSO/SAML into storage authorization?
- How much of the JDBC/ODBC compatibility matrix must be proven before regulated pilot versus documented as roadmap?
- Are reference ingest patterns enough for Gate 4, or do we need at least one maintained connector implementation?
- What is the minimum trace context that must survive the Toron to Wendelon hop?
- Do we require a concrete SAML-to-short-lived-storage-credential exchange for Gate 4, or is a narrower mock-IdP proof enough at first?

## Workstream J: FIPS, Supply Chain, And Release Compliance
### Source: `codex_workstream_J_20260422.md`

- What exact pilot language is acceptable if CMVP is in progress but not complete?
- Is `rustls-aws-lc-rs` mature enough for the target regulated-pilot window, or do we need a fallback posture in the merged release plan?
- What minimum SLSA target do we require before regulated pilot versus regulated GA?
- Which customer-facing compliance artifacts are mandatory for the first regulated pilot, and which can wait for GA?
- What platform set do we certify first, and which platforms are explicitly out of scope until a later validation cycle?

## Wendelon Workstream A: Closed-Loop Healing, DR, and Operator Tooling (WS7)
### Source: `gemini_workstream_A_20260422.md`

1.  **Backup Storage Provider:** Do we mandate S3/Azure Blob for the cluster backup manifest, or do we attempt to write the backup directly to the existing L2 tier? (Recommendation: Keep backups logically and physically separate from the L2 data tier to prevent a single bucket corruption from destroying both live data and backups).
2.  **Operator Scope:** Does the `wendelon-operator` also manage Toron compute nodes, or is it strictly bounded to the Wendelon storage tier?
3.  **Cross-Region Active Replication:** Will our DR strategy eventually require asynchronous active replication to a hot standby cluster, or is restoring from cold object-storage backups sufficient for our 4-hour RTO target?

## Wendelon Workstream B: Toron Compute Tier & Ecosystem Integration (WS8)
### Source: `gemini_workstream_B_20260422.md`

1.  **Predicate Pushdown Security:** As Toron pushes compute (predicates, aggregations) down into Wendelon (via WASM or a compute AST), how do we sandbox that execution? Does the compute run under the tenant's exact mTLS identity, and does Wendelon need its own query-planning admission control?
2.  **Quota Visibility:** Wendelon tracks tenant storage and IOPS quotas. How does Toron surface these quotas to the end-user synchronously during a SQL query, especially if a query is about to be killed by Wendelon for quota violation?
3.  **Schema Evolution:** Who owns the schema registry? If Toron executes an `ALTER TABLE`, how is that serialized into Wendelon's Raft state machine safely without blocking ongoing distributed writes? (Ties into the placeholder `SchemaUpdate` RPC that WS1/WS6 needs to freeze).

## Wendelon Workstream C: Transport, Admission Control & Runtime Stability (WS2)
### Source: `gemini_workstream_C_20260422.md`

1.  **Transport Truth-Up:** Do we spend the engineering capital to actually implement GSO/GRO and `io_uring` QUIC integration now, or do we simply retract the claim from the docs and rely on `quinn`'s default performance for the pilot? (Recommendation: Retract for pilot, re-evaluate post-GA).
2.  **Buffer Pooling:** Is the global mutexed `BufPool` going to become the next bottleneck once `io_uring` is split? Should we move to a thread-local or sharded allocation scheme immediately?
3.  **Connection Fairness:** In a multi-tenant environment, how do we guarantee fair connection establishment if the QUIC listener is saturated before the TLS handshake even authenticates the tenant?

## Wendelon Workstream D: Storage and Stellarator Correctness (WS1)
### Source: `gemini_workstream_D_20260422.md`

1.  **Leader Fencing Thresholds:** If the local disk fails an `upsert`, should the node immediately step down as Raft leader, or just fail that specific write? (Recommendation: If the disk is failing local writes, it's highly likely to fail WAL appends soon. Immediate step-down is the safest path).
2.  **Client Retry Semantics:** Should `FinalizationRecoverablePending` be exposed directly to Toron (the compute tier) so it can choose to wait, or should we normalize it into a standard HTTP 503 / Retry-After response?
3.  **Orphaned Remote Data:** If a concurrent write fails locally and we abort, how aggressively do we garbage collect the data we just successfully fanned out to remote replicas? Does this rely entirely on the background sweeper?

## Wendelon Workstream E: Observability, Validation, and SLO Evidence (WS3)
### Source: `gemini_workstream_E_20260422.md`

1.  **Cardinality Limits:** As we add more granular metrics (e.g., per-tenant backpressure queue depths), how do we enforce cardinality limits to prevent Wendelon from overwhelming the customer's Prometheus/Datadog infrastructure?
2.  **Linearizability Harness:** Do we invest in writing our own internal linearizability checker integrated tightly with our simulator, or do we bite the bullet and write a standard Clojure Jepsen test suite? (Recommendation: Jepsen has industry credibility. If we want to prove our claims to Fortune 500 architects, a passing Jepsen report is invaluable).
3.  **Trace Sampling:** 100% tracing of all distributed writes will be too expensive. How do we implement intelligent tail-based sampling so we only keep traces of writes that exceeded our P99 latency SLA or failed?

