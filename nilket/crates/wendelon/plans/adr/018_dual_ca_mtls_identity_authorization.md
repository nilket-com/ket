# ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization

## Status
Proposed

## Context
Wendelon must decide how a node proves who it is, how an operator proves who they are, how either's authority is scoped, and how either's authority is revoked. The choice ripples through every mutating RPC, the audit trail, the rate limiter, and the tenancy model.

Forces in play:
- The existing `plans/ops/multi_tenancy.md` draft carried a JWT-based RBAC model. Bearer tokens have an independent rotation lifecycle, are not cryptographically bound to the transport session, and expand the attack surface across every RPC that accepts them. ADR 021 supersedes this model; ADR 018 is its identity foundation.
- mTLS is already required by the data-plane transport (ADR 004, ADR 011). Every connection already terminates a client certificate. Reusing that primitive for authorization avoids a second credential type.
- Revoking an operator's authority in an incident must be fast and atomic. A role-by-role revocation within a shared CA becomes a CRL hunt; a whole-CA revocation is one action.
- Four distinct principals appear in the pilot operational model: data nodes replicating and voting, operators running incident response, observers pulling metrics and audit tails, and break-glass administrators recovering from incidents.

Derived from `plans/gap/claude_answers_20260417.md` §6.

## Decision

1. **Two trust roots.** The cluster maintains two independent certificate authorities:
   - **Node CA** issues certificates for data nodes and read-only observers. Its private key lives on the cluster's operational path.
   - **Operator CA** issues certificates for operators and break-glass administrators. Its private key is held off-path (hardware token, HSM, or offline signing workstation) and signs only on demand.
   Nodes trust both CAs; clients (operators, observers) trust only the Node CA (they need to validate node identities, not sign). Role-level revocation is implemented as CA-level revocation: revoking every operator in an incident is a single Operator-CA rotation.

2. **OID-bound claim payload.** Each certificate carries a custom X.509 extension (OID: TBD at ADR acceptance; registered under an IANA Private Enterprise Number) whose value is a CBOR-encoded payload:
   ```cbor
   {
     "role": "data_node" | "operator" | "read_only_observer" | "break_glass_admin",
     "scopes": [<string>, ...],
     "tenant_id": <string or null>,
     "key_id": <bytes>
   }
   ```
   The OID extension is marked critical. Certificates without it are rejected at handshake.

3. **Four pilot roles with deny-by-default RBAC.** Every mutating RPC declares its required role and scope. The handshake binds the role; the RPC dispatcher checks the binding before entering handler code. No role implies another.
   - `data_node` — replicate, vote, serve client reads and writes. No admin RPCs.
   - `operator` — all `data_node` permissions plus membership changes, epoch advances, lease grants, leadership transfer, drain, backup/restore, tenant admin.
   - `read_only_observer` — read-only access to cluster status, metrics, and audit tail. No mutating RPCs.
   - `break_glass_admin` — unrestricted. Operator-CA-issued. Maximum certificate lifetime 1 hour. Every action emits an audit record with an explicit `break_glass = true` marker.

4. **Rate-limiting composes per-IP pre-auth and per-identity post-auth.** Connections first hit per-source-IP counters for handshake-failure and malformed-frame protection. Once mTLS succeeds and the OID payload is validated, the connection graduates to per-identity counters keyed on `(key_id, role)`. Per-IP alone fails under NAT and cannot defend against authenticated adversaries; per-identity alone cannot defend the handshake itself.

5. **Short certificate lifetimes; revocation split by cert role.**

   Lifetimes (all rotated by the cluster's own certificate service, except break-glass which is Operator-CA-issued):
   - Node certificates: 24 hours.
   - Read-only observer certificates: 24 hours.
   - Operator certificates: 8 hours.
   - Break-glass certificates: 1 hour maximum.

   Revocation enforcement differs by which side of the mTLS handshake the certificate appears on:

   - **Node (server) certificates — presented by nodes during any inbound connection.** OCSP stapling is preferred: the node staples its own OCSP response to the TLS handshake; the connecting peer validates freshness without a separate OCSP round-trip. CRL is the fallback when OCSP stapling is unavailable (e.g., node unable to reach its CA's OCSP responder).
   - **Client certificates — operator, observer, and break-glass certs presented to nodes.** OCSP stapling is not reliable for client certs (the must-staple extension is rarely honored on client side). Nodes enforce revocation in one of two ways, in this priority order:
     1. **Short-window CRL pull.** Each node fetches the Operator CA's CRL on a short interval (default 5 minutes) and caches it. Client handshakes validate against the cached CRL. CRL pull failures keep using the previous cached CRL up to a hard staleness limit (default 30 minutes); past that limit, client authentication fails closed.
     2. **OCSP direct-query fallback.** If the Operator CA exposes an OCSP responder, nodes may query it directly per handshake as an alternative to CRL caching. This is a deployment option, not the default — it adds handshake latency and a hard dependency on responder availability.

   The short lifetime is the primary defense. Revocation is the secondary defense for the case where an operator credential must be killed faster than its 8-hour expiry.

6. **Rate-limiting and RBAC enforcement boundary is the RPC dispatcher.** Authorization checks live in a single dispatcher layer, not inside handlers. A handler that is entered has already had its role and scope validated. This makes audit and red-team analysis tractable: there is exactly one enforcement point per RPC.

7. **No bearer tokens as primary credentials.** Bearer tokens may appear inside signed, cert-authenticated control envelopes (for example, a short-lived capability token minted for a delegated operation), but they are never the primary authentication primitive. Every inbound RPC must have a valid mTLS peer.

## Rationale

- **Two CAs is the minimum that gives incident-time atomicity.** One CA requires revoking operators role-by-role; three or more CAs multiply key ceremonies without buying separation. The data-plane path and the administrative path are the two axes that matter, so two CAs is the right cardinality.
- **OID claims beat session tokens on binding strength.** A CBOR payload inside the certificate is signed by the CA at issuance; the mTLS handshake cryptographically binds the connection to the certificate. Tampering requires compromising the CA, not just stealing a token. Tokens also introduce a lifecycle problem (rotation, revocation, storage) that certificates already solve.
- **Critical extension semantics prevent silent downgrade.** Marking the OID critical means a client that doesn't understand the extension must reject the certificate instead of accepting it unclaimed. This protects against older clients being tricked into accepting unverified roles.
- **Four roles map cleanly to pilot operational reality.** Fewer merges responsibilities that should be separable (e.g., the operator and the node); more invents distinctions the pilot doesn't exercise. Adding roles later uses the same OID/CA mechanism without a protocol break.
- **Short lifetimes beat long-lifetime CRLs.** A 24-hour certificate cannot be abused for more than 24 hours even if the revocation infrastructure fails entirely. This lets us operate with a simpler revocation story in the default case and use OCSP/CRL as a secondary defense, not a primary one.
- **Enforcement-at-dispatcher beats enforcement-in-handler.** A single enforcement layer is auditable; per-handler checks drift, become inconsistent, and eventually miss an RPC.

## Consequences (The "Negatives")

- **Two CA key ceremonies.** Operating two trust roots doubles the PKI ceremony surface: two root keys to protect, two rotation schedules to run, two CA audits. We accept this for the revocation atomicity it buys.
- **Operator-CA key-handling is operator-heavy.** Because the Operator CA signs off-path, issuing an operator certificate requires an operator-ceremony step. Mitigated by making the issuance a batch operation (daily or on-demand) rather than per-connection.
- **Short certificate lifetimes require a rotation service.** Every 24 hours (nodes) or 8 hours (operators) a fresh certificate must be issued and distributed. This is a service Wendelon itself runs; it becomes a critical dependency. The service is simple (sign a CSR, publish the cert) but must itself be highly available.
- **The OID extension is a custom extension, not a standard one.** Clients written outside Wendelon cannot interpret our OID without our schema. We accept this because the alternative — using a standard extension like SAN — mis-types the payload and overloads a field with a non-name value.
- **Break-glass certificates are a correctness risk.** An issued break-glass cert has unrestricted authority for up to 1 hour. Mitigated by: (a) the short lifetime, (b) mandatory audit tagging on every action, (c) the requirement that break-glass issuance itself is an Operator-CA ceremony with its own audit trail.
- **Rejected alternatives explicitly:**
  - *Single CA with role OID separation* — rejected for the CRL-hunt-on-incident problem.
  - *Bearer tokens (JWT or similar) as primary authentication* — rejected for lifecycle, rotation, and transport-binding weaknesses.
  - *Per-role-per-CA (one CA per role)* — rejected for PKI ceremony blowup without a corresponding separation win.
  - *Long-lived certificates with aggressive CRL/OCSP* — rejected because it moves the correctness bar onto the revocation service being always available, which is a strictly worse failure mode than a short-lived certificate expiring on its own.

## Open Questions

- **OID assignment.** The OID prefix requires a registered IANA Private Enterprise Number (PEN) for Wendelon (or the owning organization). *Resolves via:* PEN registration before ADR acceptance; the full OID is fixed at acceptance and recorded in a follow-up edit to this ADR.
- **Certificate rotation service placement.** The service that issues 24-hour node certificates must itself be highly available and cannot depend on the cluster it certifies in a way that deadlocks. *Resolves via:* a follow-up design note in `plans/network/security.md` before ADR acceptance.
- **OCSP responder topology (node-cert stapling path).** OCSP stapling on node certificates requires the node to reach a responder associated with the Node CA to obtain a fresh stapled response before its cache expires. Whether the responder is co-located with the CA, replicated, or delegated to an external service is deferred. *Resolves via:* design note accompanying ADR 018 acceptance.
- **CRL distribution endpoint for Operator CA.** The client-cert revocation path depends on every node being able to pull a fresh Operator-CA CRL on a 5-minute cadence with graceful degradation to a 30-minute staleness ceiling. Whether the CRL is served from the Operator CA directly, replicated to an HTTP endpoint that nodes can reach, or distributed via the cluster's own state machine is deferred. *Resolves via:* design note accompanying ADR 018 acceptance. The default-fail-closed behavior after 30 minutes is load-bearing and must survive the design choice.

## References
- ADR 004: Zero-copy Transport (transport-layer mTLS context)
- ADR 011: Auto Adaptive Transport
- ADR 021: Tenant-Aware Architecture from Day One (planned — consumes this ADR's identity model)
- ADR 025: Audit Event Model and Tamper-Evident Hash Chain (planned — consumes the break-glass marker)
- `plans/network/security.md`
- `plans/ops/multi_tenancy.md` (§2 superseded by ADR 021)
- `plans/gap/claude_answers_20260417.md` §6
