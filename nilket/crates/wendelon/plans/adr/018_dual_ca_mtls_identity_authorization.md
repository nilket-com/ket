# ADR 018: Dual-CA mTLS Identity And OID-Bound Authorization

## Status
Proposed (amended 2026-04-22)

## Context
Wendelon needs a single, transport-bound identity model for:

- node-to-node replication and control traffic
- operator access
- read-only observation
- break-glass recovery
- short-lived query-session access from the compute layer

The architecture already rejected bearer tokens as the primary trust primitive. The planning cycle then closed several additional questions that the earlier ADR draft left too open:

- the cert-service scope needs a hard boundary
- query-session credentials must be first-class but must not quietly turn Wendelon into a universal tenant-application CA
- revocation distribution cannot depend on fragile ad hoc HTTP side channels
- the protocol must support both simple operation and stricter policy overlays without redesign

## Decision

1. **Wendelon uses two trust roots.**
   - **Node CA** issues certificates for:
     - `data_node`
     - `read_only_observer`
     - short-lived `query_session` credentials
   - **Operator CA** issues certificates for:
     - `operator`
     - `break_glass_admin`

2. **All certificates carry a critical OID-bound claim payload.**
   The claim payload is the authoritative source for:
   - role
   - scopes
   - tenant binding, when applicable
   - key identity

   Certificates without the critical extension are rejected.

3. **The supported role surface is small and explicit.**
   Wendelon recognizes:
   - `data_node`
   - `operator`
   - `read_only_observer`
   - `break_glass_admin`
   - `query_session`

   Role checks happen at the dispatcher boundary, not inside handlers.

4. **The cert-service scope is narrow by design.**
   The Wendelon certificate service may issue and rotate certificates for:
   - nodes
   - operators
   - observers
   - break-glass credentials
   - short-lived query-session credentials

   It does **not** act as a general tenant-application CA for arbitrary long-lived customer certificates.

5. **`query_session` credentials are short-lived and purpose-specific.**
   They exist to authorize Wendelon RPCs for user-originated query activity. They are:
   - tenant-bound
   - short-lived
   - non-administrative
   - unsuitable as general-purpose tenant identity outside the Wendelon/Toron boundary

6. **Client-cert revocation state is distributed through Wendelon's own replicated control state.**
   Revocation must not rely on every node discovering fresh operator-side revocation state from a best-effort external HTTP endpoint. The control ring distributes the revocation material needed for fail-closed behavior.

7. **Policy is layered above identity.**
   The certificate and role model is the stable protocol layer. Approval policy is a deployment-layer overlay:
   - simpler deployments may permit audited single-operator execution
   - stricter deployments may require dual-authorization or wider approval workflows for selected operations

   The important design rule is that the identity model and RPC surface must support both without protocol redesign.

8. **Break-glass is explicit and auditable.**
   Break-glass credentials:
   - are short-lived
   - carry explicit scope
   - are audited distinctly from normal operator activity
   - remain exceptional credentials, not a routine admin path

9. **Bearer tokens are never the primary credential.**
   Every inbound RPC is bound to mTLS identity. Any auxiliary token or capability must sit inside a cert-authenticated flow, not replace it.

## Rationale

- **Transport-bound identity is the cleanest trust model.** It eliminates a second primary credential type and keeps authentication attached to the connection itself.
- **Two roots are the right level of separation.** Node/runtime trust and human/operator trust are meaningfully different without exploding the PKI surface into per-role roots.
- **A narrow cert-service boundary prevents accidental CA sprawl.** Query-session issuance is useful; becoming a universal customer PKI is not.
- **Replicated revocation state is more believable than out-of-band polling as the primary control.** Identity and authorization are already control-ring concerns.
- **Policy layering keeps the protocol stable.** The system can support a commodity deployment and a stricter enterprise deployment without changing the role model itself.

## Consequences (The "Negatives")

- **Two CA ceremonies still exist.** That is accepted in exchange for cleaner separation between node/runtime and operator/admin trust.
- **The cert-service becomes a real control-plane component.** It must be reliable enough to support short-lived credentials and rotation.
- **Short-lived query-session credentials add issuance pressure.** Accepted because they are the cleanest way to bind compute-originated user identity to storage RPCs.
- **Rejected alternative: single CA for every actor.** Rejected because it weakens separation and incident response.
- **Rejected alternative: universal tenant-application CA in v1.** Rejected because it would expand Wendelon into a broader PKI system than the product needs.
- **Rejected alternative: bearer tokens as the primary identity primitive.** Rejected because they are weaker at transport binding and lifecycle hygiene.

## Open Questions

- **OID assignment.** The exact OID prefix still depends on PEN registration, but that is an identifier-registration task, not a blocker to the architectural model itself.
- **Exact issuance topology for the cert-service.** The service shape and HA packaging are implementation decisions built on top of this role and scope model.
- **Exact break-glass policy defaults.** Scope TTLs, approval windows, and approval counts are design-lock items layered on top of this identity model.

## References
- ADR 011: Transport Support Strategy (QUIC First, Multi-Transport Later)
- ADR 021: Tenant-Aware Architecture from Day One
- ADR 025: Audit Event Model and Tamper-Evident Hash Chain
- ADR 031: Production Key Custody Provider Stack
