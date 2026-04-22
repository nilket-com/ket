# Workstream C — Identity, RBAC, and Authorization

**Merged-plan anchor:** WS5
**Author / Role:** Claude, as Lead Architect on Workstream C
**Version:** v2 — integrates peer review of `codex_workstream_F_20260422.md` (same-scope peer) and `gemini_workstream_B_20260422.md` (Toron integration surfaces an additional requirement on per-user cert issuance). Codex and Claude are broadly aligned; Gemini adds a new requirement.
**Phase alignment:** Design Phase 0 (after ADR 018 Accepted, Week 2); implementation Phases 1-2; IdP integration Phase 2.
**Gap items closed:** R-6, R-7, R-8 (freeze), R-13, R-14, R-16.
**Depends on:** WS-A (ADR 018 acceptance, IANA PEN). Feeds: WS-B (dual-auth on rotate RPC), WS-D (audit actor identity binding), WS-G (operator-cert issuance for K8s Operator), WS-E (Security Policy identity surface), WS-Toron (per-user cert issuance — new requirement, see §3.6).

---

## 1. Why this workstream exists

ADR 018 is a strong identity model on paper: dual-CA, cryptographically-bound OID roles, short-lifetime certs, revocation split by cert-side. The gaps that block regulated-industry adoption are (a) the OID prefix is still "TBD at ADR acceptance," (b) the cert-rotation service has no HA design, (c) the CRL distribution endpoint is an Open Question, (d) the `operator` role is wildly overpowered for Fortune-500 separation-of-duties controls, (e) destructive operations accept a single signature, and (f) break-glass has no workflow beyond "the Operator CA signs it."

Every one of these is a procurement finding. This workstream closes them.

## 2. Strategic position (opinionated)

### 2.1 Cert-rotation service is a standalone HA binary with its own Raft

**Position:** ship `wendelon-cert-service` as a separate binary with its own 3- or 5-node Raft quorum, deployed adjacent to (but not inside) the Wendelon cluster.

**Why.** A cert-rotation service embedded in Wendelon creates a deadlock: if Wendelon's internal certs expire and the rotation service runs on Wendelon, you cannot bootstrap. HashiCorp Vault demonstrates the pattern: certificate authorities run as their own HA service. The alternative — running the cert service on an entirely external cluster — adds an external-dependency procurement item and moves Wendelon's security posture off-product.

The service owns: Node CA private key (HSM-backed), Operator CA public-trust (Operator CA private key lives off-path per ADR 018 §1), cert-issuance log, CRL. It speaks mTLS to Wendelon nodes and to operators. Its own certs are self-signed at bootstrap with a published ceremony.

### 2.2 CRL distribution via Wendelon state machine (not an external HTTP endpoint)

ADR 018 Open Question: "Whether the CRL is served from the Operator CA directly, replicated to an HTTP endpoint that nodes can reach, or distributed via the cluster's own state machine is deferred."

**Position:** state-machine-distributed. Specifically, the CRL is a Raft-committed blob that every Wendelon node replicates as part of state-machine application. Nodes check the CRL from local state at mTLS handshake time; the 30-minute fail-closed staleness ceiling applies to the local cached copy, but because the CRL is replicated via Raft, it is updated at every commit anyway.

**Why.**

1. No external HA dependency. An external HTTP CRL endpoint becomes a single point of failure or requires its own HA topology.
2. Revocation latency is Raft-commit latency (milliseconds), not CRL-pull-interval (5 minutes in the ADR default).
3. Fail-closed becomes automatic: a node whose Raft state is not converging is already rejecting writes; adding "and rejecting client certs" is consistent.

**Trade-off.** The Operator CA must be able to push CRL updates into the Wendelon state machine. This is a cross-service interaction: the cert-rotation service holds the Operator CA trust; Wendelon's state machine holds the CRL. Mechanism: cert-service submits a `UpdateOperatorCRL` Raft proposal signed by the Operator CA; Wendelon state machine validates the signature and commits.

### 2.3 The `operator` role splits into exactly four, no more, no fewer

**Position:** `tenant_admin`, `cluster_admin`, `backup_admin`, `compliance_auditor`. Plus the existing `data_node`, `read_only_observer`, `break_glass_admin`. Seven roles total. No fifth split in v1.

**Why four, not five or six.**

- `tenant_admin` — TenantAdmin RPC surface: create/update/suspend/resume/delete tenant, update quota. This is the most frequently exercised operator persona.
- `cluster_admin` — membership changes, epoch advances, lease grants, leadership transfer, drain. Infrastructure-level operations; does not touch tenant data.
- `backup_admin` — backup/restore, key rotation (including `TenantAdmin.rotate_tenant_kek`). Separate because backup admins often have HSM/KMS permissions that tenant admins should not.
- `compliance_auditor` — read audit tail, run `wendelon-ctl audit verify`, read `/ready` and `/metrics`. Distinct from `read_only_observer` (which keeps its existing metadata-read scope). This is the role you give to an external auditor; they can verify the audit chain without being able to read payload data or tenant-secret metadata.

**Why not more.** Role proliferation is its own attack surface: more roles means more combinations to test and more opportunities for privilege creep. Four covers the Fortune-500 separation-of-duties matrix cleanly. A fifth role (e.g., `security_admin` for cert rotation) feels tempting but overlaps with `backup_admin` and `cluster_admin` enough that you'd spend review cycles debating which one owns which RPC. Four roles maps cleanly; stop there.

**Why not three.** Collapsing `compliance_auditor` into `read_only_observer` breaks the audit independence requirement — an observer with metric access can infer payload patterns, which is exactly the thing a compliance auditor must be able to read without.

### 2.4 Dual-authorization enforced at Raft state-machine apply, not dispatcher

**Position:** M-of-N cert signatures on destructive Raft proposals, validated at state-machine apply, not at the RPC dispatcher.

**Why.**

1. The dispatcher is a good enforcement point for *who can initiate*; the state machine is the enforcement point for *who can commit*. A dual-auth check at dispatcher is bypassable if someone finds a path that commits without going through the dispatcher (a bug we cannot yet prove does not exist). A dual-auth check at state-machine apply is unbypassable: the Raft log is the commit contract.
2. Raft already has a signature-validation surface (proposals are signed by the proposer's cert). Adding "two signatures required for destructive ops" extends existing code, not a new enforcement layer.
3. The incident-time workflow is: operator A initiates the RPC, which creates a *pending* proposal visible to other operators; operator B signs; proposal applies. This is the right shape for a two-person-control workflow and matches how bank transaction approvals already work.

**Destructive operations requiring dual-auth:**
- `delete_tenant`
- `rotate_tenant_kek` + `retire_tenant_kek_version`
- `membership.evict_peer` *when eviction would drop the cluster below quorum*
- Break-glass cert issuance (special case: see §3.4)
- Root KEK retirement (WS-B §3.1 `retire_key`)

Non-destructive operations do not require dual-auth. `suspend_tenant` is reversible; `create_tenant` is creative, not destructive. We are deliberate about where the friction lives.

### 2.5 Break-glass is dual-authorized AND incident-ticket-bound AND time-boxed

**Position:** issuing a break-glass certificate requires (a) two operator signatures, (b) a non-empty incident-ticket-id in the cert OID payload, (c) a max-lifetime of 1 hour (already in ADR 018), and (d) an expressed operation scope that narrows authority within that hour.

**Why.**

Today's ADR 018 break-glass cert grants *unrestricted* authority for up to 1 hour. That is the nuclear option. It's appropriate for the nuclear option; it is not appropriate to issue it as casually as the current ADR implies ("Operator-CA-issued; requires Operator-CA ceremony").

**Mechanism.**

- Issuance requires two operator signatures (the Operator CA ceremony holds *one* cert; a second operator cert co-signs the issuance request).
- `incident_ticket_id` is a required field in the break-glass cert's OID payload. Format enforced: `[A-Z]{2,8}-[0-9]{1,10}`. Non-empty.
- `scope` is a new field in the OID payload, narrowing authority within the break-glass hour: `full`, `tenant_ops`, `cluster_ops`, `key_ops`, `diagnostic`. Default to the narrowest scope that covers the incident; `full` requires a third signature and an architect-lead sign-off.
- Every RPC action under a break-glass cert emits an audit record with `break_glass = true` and the `incident_ticket_id`.

**Trade-off.** This adds workflow friction to the already-friction-laden break-glass path. For a regulated customer, that friction is a feature. The customer's security officer wants evidence that break-glass is rare, audited, and traceable to an incident. We give them that evidence by construction.

## 3. Design — the components

### 3.1 OID payload (post-PEN-freeze)

Once IANA PEN lands, the frozen OID payload (CBOR-encoded, stored in the critical X.509 extension):

```cbor
{
  "schema_version": 1,        // u8
  "role": string,              // one of: "data_node", "operator_tenant_admin",
                               //   "operator_cluster_admin", "operator_backup_admin",
                               //   "read_only_observer", "compliance_auditor",
                               //   "break_glass_admin"
  "scopes": [string],          // operation-specific scopes; may be empty
  "tenant_id": string | null,  // actor tenant binding (ADR 021 §2)
  "key_id": bytes,             // BLAKE3-256 of the cert's public key
  "issued_at": u64,            // Unix seconds
  "not_after": u64,            // Unix seconds; must not exceed role's max-lifetime
  "incident_ticket_id": string | null,  // required for break_glass_admin
  "break_glass_scope": string | null    // one of: full, tenant_ops, cluster_ops,
                                        //   key_ops, diagnostic (break_glass only)
}
```

**Frozen at ADR 018 acceptance.** Any schema change is a `schema_version = 2` bump, additive only.

Note the role enum has expanded: `operator` collapses to three sub-roles. `break_glass_admin` becomes the seventh role. The seven roles are exhaustive; a handshake producing an unknown role-string is rejected.

### 3.2 `wendelon-cert-service` architecture

**Topology.**
- 3- or 5-node Raft cluster (customer choice).
- Each node runs `wendelon-cert-service` binary.
- Node CA private key is HSM-backed (PKCS#11); same HSM or a separate HSM from Wendelon's KMS HSM.
- Operator CA trust is embedded; Operator CA private key lives off-path (hardware token, offline signing workstation — customer-owned).

**API surface.**
- `POST /v1/nodes/issue` — issue a 24h node cert. Auth: mTLS from Wendelon node with existing node cert (or from bootstrap token on first issuance).
- `POST /v1/observers/issue` — issue a 24h observer cert. Auth: IdP-proxied with group-membership check.
- `POST /v1/operators/issue` — issue an 8h operator cert. Auth: IdP-proxied with group-membership check; requires two-factor challenge.
- `POST /v1/break_glass/issue` — issue a 1h break-glass cert. Auth: Operator-CA-signed request + second operator co-signature + incident-ticket-id + scope.
- `GET /v1/crl` — current CRL. Public-read.
- `POST /v1/revoke` — revoke a cert. Auth: operator or Operator-CA.

**HA properties.**
- Raft leader is the signer; followers replicate the issuance log.
- Signing HSM is attached to the leader; failover re-attaches on leader election.
- Certificate issuance log is immutable (append-only). Used for audit.

**Not dependent on Wendelon itself.** This is the critical property. A Wendelon cluster whose certs have expired can still reach the cert-service to renew. Cert-service runs on independent infrastructure.

### 3.3 IdP integration

IdP mapping determines *which humans hold which cert types*. Two patterns:

**Pattern 1: IdP-gated self-service (preferred for operators).**
- Operator logs into IdP (Okta, Azure AD, Google Workspace, SAML-compatible provider).
- IdP returns a JWT with group memberships.
- Operator presents the JWT to cert-service `/v1/operators/issue`.
- Cert-service verifies the JWT signature, extracts groups, maps groups to roles via config:
  ```yaml
  idp_role_map:
    "wendelon-tenant-admins":   "operator_tenant_admin"
    "wendelon-cluster-admins":  "operator_cluster_admin"
    "wendelon-backup-admins":   "operator_backup_admin"
    "wendelon-compliance":      "compliance_auditor"
    "wendelon-observers":       "read_only_observer"
  ```
- Cert-service issues an 8h operator cert with the mapped role.
- Human revocation = remove from IdP group; next cert issuance fails.

**Pattern 2: Ceremony-gated (break-glass only).**
- Not IdP-mediated. Requires physical presence in a ceremony room, Operator CA HSM, second operator co-signature.
- Deliberately high-friction; break-glass is not a normal operation.

**Architect position.** Pattern 1 for all operational roles. Pattern 2 only for break-glass. Do not add more ceremonies; ceremonies that are rare enough to matter become unfamiliar enough to fail.

### 3.6 Per-user cert issuance for Toron integration (new from Gemini's WS-B)

Gemini's Toron integration plan identifies a concrete requirement my original plan deferred: when Toron (compute tier) receives a SQL query authenticated via SAML/SSO, Toron must translate that end-user's identity into a Wendelon mTLS cert so Wendelon can correctly attribute the data access in audit. Gemini phrases it: *"Toron exchanges a SAML assertion for a short-lived Wendelon mTLS certificate representing the executing user."*

This changes the cert-rotation service's throughput requirement materially. Operator cert issuance is "dozens per day"; per-user cert issuance is potentially "thousands per minute" (one per concurrent SQL session).

**Design response:**

1. **Scale the service.** Cert-rotation service must handle per-user-session issuance rate. Benchmarking target: 1000 issuances/second sustained, 5000 issuances/second burst. At Ed25519 signing this is ~1 CPU core; at ECDSA-P256 (FIPS) it's ~3-4 CPU cores. Either is achievable on a 3-5-node service.

2. **New role: `query_session`.** Issued to per-user-SAML-session certs. Very-short-lifetime (15 minutes). Scope: read-only; data-access audit records attribute to the cert's IdP-identity via the `operator_human_binding` field (currently in break-glass schema; extend to general cert OID for query-session certs).

3. **SAML-to-mTLS translation flow owned by Toron, enforced by cert-service.** Toron validates the SAML assertion, requests a query-session cert from cert-service via a dedicated endpoint `/v1/query_session/issue`, presents its own Toron-system cert plus the validated user identity. Cert-service issues the short-lived cert with the user's IdP identity in the OID payload.

4. **Revocation cascade.** When a user logs out of SSO, Toron notifies cert-service via `/v1/query_session/revoke`. Cert-service CRL-revokes; Wendelon picks up the revocation through state-machine CRL distribution.

**Trade-offs.** This doubles the complexity of the cert-service but avoids the alternative ("tokens all the way down") which we rejected in ADR 018. The ~15-minute cert lifetime bounds exposure from compromised tokens without requiring real-time revocation.

**Phase alignment.** This feature is required for WS-Toron (Gemini's WS-B) Gate 4. It should ship in Phase 2, coupled with Toron integration. It is not a Phase 1 dependency for the regulated pilot unless the first pilot customer uses Toron.

### 3.4 Dual-authorization mechanism in Raft state machine

**Proposal structure:**
```rust
pub enum WendelonRequest {
    TenantCreate { ... },
    TenantDelete { target_tenant_id, signatures: Vec<ProposalSignature> },
    RotateTenantKek { target_tenant_id, signatures: Vec<ProposalSignature> },
    // ... etc
}

pub struct ProposalSignature {
    pub signer_key_id: [u8; 32],  // BLAKE3-256 of signer's cert public key
    pub signer_role: String,       // resolved from the cert's OID at signing time
    pub proposal_hash: [u8; 32],   // BLAKE3-256 of canonical proposal bytes
    pub signature: Vec<u8>,        // Ed25519 signature over proposal_hash
    pub signed_at_ns: u64,         // nanosecond timestamp
}
```

**State machine apply logic:**

```
on apply(WendelonRequest::TenantDelete { target_tenant_id, signatures }):
    if len(signatures) < 2:
        reject (InsufficientSignatures)
    if distinct_signer_key_ids(signatures).len() < 2:
        reject (DuplicateSignatures)
    for sig in signatures:
        if not verify_signature(sig):
            reject (InvalidSignature)
        if sig.signer_role not in allowed_roles_for_tenant_delete:
            reject (UnauthorizedSigner)
        if abs(now_ns - sig.signed_at_ns) > MAX_SIGNATURE_AGE_NS (= 5 min):
            reject (StaleSignature)
    // All checks passed; apply the mutation.
    drop_tenant_keyspace(target_tenant_id)
    destroy_tenant_kek(target_tenant_id)
    emit_audit("tenant.delete", {signers: [...], target_tenant_id})
```

The enforcement lives in the state machine apply, not the dispatcher. A proposal that bypasses the dispatcher cannot bypass apply.

**Collecting the second signature.**

Workflow: operator A submits the RPC; dispatcher validates A's signature and forwards the pending proposal to the state machine as `ProposalPendingCosign`; operators B, C, D see the pending proposal via a new RPC `TenantAdmin.list_pending_proposals(); operator B signs; dispatcher appends the signature; state machine applies when signature count + distinctness thresholds met.

**Timeout.** A pending proposal not cosigned within 15 minutes auto-expires. Emits `proposal.expired` audit event.

### 3.5 Break-glass issuance workflow

Full flow:

1. Incident declared. Incident ticket opened in the operator's ticketing system (Jira, ServiceNow, etc.).
2. Operator A goes to the Operator-CA ceremony room. Presents their operator cert.
3. Operator A connects to the Operator-CA HSM (physical token or offline signing workstation).
4. Operator A fills out break-glass issuance request: target principal (usually the operator's own IdP identity), incident_ticket_id, break_glass_scope (default `diagnostic`, escalate with justification).
5. Operator B joins the ceremony, presents their operator cert. Cross-signs the request.
6. Operator-CA HSM signs the break-glass cert.
7. Cert is delivered to the operator's laptop over a dedicated out-of-band channel (physical USB, short-lived secure messaging session). Never via the Wendelon Data-Ring.
8. Operator uses the break-glass cert for up to 1 hour. Every action is audited with `break_glass = true` and the `incident_ticket_id`.
9. After 1 hour, cert expires. Cannot be reissued against the same incident ticket without a fresh ceremony (enforced: the incident_ticket_id is recorded in the cert-service's issuance log; duplicate issuance requests within 24h require a third signature).

**Scope narrowing.**
- `diagnostic` — read-only. Access to `/ready`, `/metrics`, audit tail, `state_snapshot`. Cannot mutate.
- `tenant_ops` — tenant admin operations only. Cannot touch cluster membership or keys.
- `cluster_ops` — membership, epoch, leadership. Cannot touch tenants or keys.
- `key_ops` — KEK rotation, KEK retirement. Cannot touch tenants or cluster membership.
- `full` — unrestricted (today's default). Requires three signatures and architect sign-off.

## 4. Work plan (Phase-aligned)

### Phase 0 (Weeks 2-6, after ADR 018 Accepted)

| Week | Deliverable |
|---|---|
| 2 | ADR 018 accepted with frozen-on-PEN OID; role enum expanded to 7 roles. |
| 3 | `wendelon-cert-service` architecture doc committed. |
| 4 | Cert-service Raft-plus-signing-HSM prototype scaffolding. |
| 5 | IdP mapping config format frozen. |
| 6 | Dual-auth Raft-proposal structure merged. State-machine validation logic in place. |

### Phase 1 (Months 2-10)

| Month | Deliverable |
|---|---|
| 2 | `wendelon-cert-service` at alpha quality: issues node certs against a SoftHSM2 backend. |
| 3 | IANA PEN lands. OID frozen. Cert-service begins signing under the frozen OID. |
| 3 | Role split lands. Dispatcher declarations updated; `tests/rbac_enforcement.rs` covers every role × every RPC. |
| 4 | Cert-service at production quality. Deployed in test fleet. Failover drill: kill leader, observe re-issuance continues. |
| 4 | CRL distribution via state-machine lands. Cert-service submits `UpdateOperatorCRL` proposals. |
| 5 | Dual-authorization landed on `delete_tenant` and `rotate_tenant_kek`. Integration tests. |
| 5 | Pending-cosign RPC surface. Operator-A-submits / operator-B-signs workflow. |
| 6 | IdP integration: Okta + Azure AD reference implementations. |
| 7 | Break-glass issuance workflow landed. Scope narrowing enforced. |
| 7 | Dual-auth expanded to `membership.evict_peer` and Root-KEK retirement. |
| 8 | IdP integration: SAML generic. Cert-service documented for customer operators. |
| 9 | Break-glass workflow exercised in chaos-harness (simulate incident response). |
| 10 | Phase 1 exit: cert-service deployed, dual-auth live on all destructive ops, break-glass workflow documented, CRL latency measured. |

### Phase 2 (Months 9-15)

Hardening, third-party red-team of the authorization path, customer-facing docs, compliance pack integration.

## 5. Interfaces with other workstreams

- **WS-A (Governance):** depends on ADR 018 Accepted (Week 2) and IANA PEN assignment.
- **WS-B (Crypto):** provides dual-auth mechanism. `rotate_tenant_kek` waits for WS-C dual-auth.
- **WS-D (Audit):** audit records carry the actor identity from the mTLS peer (already in ADR 018). No change needed, but the break-glass parameter schema (§3.5) must align with ADR 025 break-glass event schema.
- **WS-G (Gemini's WS7 / Day-2):** K8s Operator uses cert-service to issue operator certs; `wendelon-operator` service account maps to a `cluster_admin` cert.
- **WS-E (FIPS):** identity-related primitives (Ed25519 signatures, X.509 parsing) must run inside the FIPS module boundary in the regulated SKU.

## 6. Exit criteria

1. OID frozen in ADR 018 and in the codebase. Every cert issued under the frozen OID.
2. `wendelon-cert-service` ships as a separate binary with HA Raft deployment.
3. CRL distribution via Wendelon state machine; measured revocation-to-rejection latency under 10s.
4. Seven roles (4 split operator sub-roles + 3 existing) enforced at dispatcher for every RPC.
5. Dual-authorization live on `delete_tenant`, `rotate_tenant_kek`, `retire_tenant_kek_version`, quorum-affecting `membership.evict_peer`, Root-KEK retirement.
6. Break-glass workflow: two-signature ceremony, incident_ticket_id enforced, scope narrowing supported. Exercised in at least one chaos-simulated incident.
7. IdP integration for at least Okta, Azure AD, SAML-generic. Documentation for partner onboarding.
8. Red-team audit (Gate 2, WS-A coordination) passed or findings accepted with compensating controls.
9. Scorecard row "Security / auth-custody review" green.

## 7. Risks within this workstream

- **IANA PEN assignment delayed beyond Phase 1.** Mitigation: ADR 018 has frozen-on-assignment clause; cert-service refuses to sign until freeze. Phase 1 work proceeds against a placeholder OID prefix with an explicit migration plan.
- **`wendelon-cert-service` Raft complexity.** Standing up a Raft cluster for a cert service duplicates effort. Mitigation: reuse the openraft integration pattern Wendelon already has; cert-service's state machine is trivial (append-only issuance log + current CRL + current CA public trust). Should be ~2000 lines.
- **Dual-auth creates operational deadlock during incidents.** Mitigation: break-glass path retains single-authorization with audit tagging; dual-auth is the default path, not the only path. Operational documentation is explicit about when to use each.
- **IdP integration churn as customers ask for more providers.** Mitigation: cert-service's IdP adapter is a trait; adding a new provider is a crate, not a core change.
- **Operator training.** Dual-auth, pending-cosign workflow, break-glass ceremony are all new operational skills. Mitigation: runbook docs; quarterly training exercises; chaos-harness scenario for incident response.
- **Dispatcher vs state-machine enforcement divergence.** We enforce at both layers (dispatcher for fast failure, state machine for correctness). If they drift — e.g., dispatcher allows a role that state machine rejects — we get accepted-but-fail-at-apply behavior. Mitigation: single source of truth for role×operation matrix; both layers read from it; architecture test enforces.

## 8. Points of agreement and divergence with Codex's WS-F

**Agreement:**
- Role split is before serious pilot use, not after.
- Four-way split: `tenant_admin`, `cluster_admin`, `backup_admin`, `compliance_auditor`.
- Dual-auth is enforced at proposal validation (state machine apply), not just API edge.
- Break-glass remains possible, but "loud, narrow, auditable."
- Cert lifecycle is a first-class system component.

**Refinement from Codex:**
- "Which operations absolutely require dual-auth, and which can remain single-auth with strong audit?" — my §2.4 enumerated list answers this with specific operations; Codex's framing made me re-check the list for completeness.
- "Simpler CRL is better than clever CRL" (Codex's bias) — I adopt this; state-machine-distributed CRL is simpler than the external-HTTP alternative and satisfies the fail-closed posture.

**Consistent positions (both arrived independently):**
- OID freeze immediately upon PEN assignment.
- Break-glass requires incident-ticket reference and narrow scope.
- IdP mapping does not need to be the first milestone but does need a real plan before regulated pilot.

**Additions from Gemini's WS-B (Toron integration):**
- Per-user `query_session` certs for Toron-originated SQL queries (new in §3.6).
- Cert-service throughput requirement raised 100x from operator-rate to user-session-rate.

## 9. Opinionated Constraints (framing adopted from Codex)

- Do not ship regulated-pilot posture with the current `operator` role.
- Do not push destructive-op protections down into runbook convention; enforce them in the system.
- Do not let break-glass become a general-purpose bypass that everyone quietly depends on.
- (Added by Claude:) Do not accept any cert-service design that creates a deadlock with the Wendelon cluster it certifies.
- (Added by Claude:) Do not allow tenant-app authentication to go through a path that bypasses the dispatcher's cert-vs-target pairing.

## 10. Open questions

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
