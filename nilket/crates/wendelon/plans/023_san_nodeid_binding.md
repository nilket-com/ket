# Plan: SAN to NodeId Binding for mTLS Identity

## Summary
This plan freezes the identity contract that maps a node certificate to a
Wendelon `NodeId`.

The recommended decision is:

- Use a canonical DNS SAN format for node identity in the first execution wave:
  `node-<id>.<cluster_domain>`.
- The dial side's `ServerNameResolver` returns exactly that DNS name for a
  target `NodeId`.
- The accept side extracts the peer leaf certificate's DNS SAN set, finds the
  canonical node SAN, parses the numeric id, and installs that value as the
  verified `remote_id`.
- Membership authorization remains a higher-layer concern; TLS identity binding
  proves who the peer is, not whether it is currently entitled to every action.

## Motivation
Today the code stops one step short of the security plan:

- `src/network/tls.rs` explicitly says SAN extraction and `NodeId` verification
  are not implemented.
- `src/network/quic_transport.rs` already assumes the dial side can derive SNI
  from `NodeId`.
- `src/network/server.rs` still fills accepted connections with
  `NodeId { id: 0 }`.

That means the transport is encrypted and certificate-chained, but the process
does not yet have a trusted peer identity. This plan makes the identity rule
concrete and implementable.

## Goals
- Freeze one canonical node-certificate naming rule for the first execution wave.
- Make client-side SNI generation and server-side peer-id extraction consistent.
- Avoid blocking future join flows on overly strict membership checks at the TLS
  layer.

## Non-Goals
- Full PKI lifecycle automation.
- Hot cert reload.
- Revocation lists or OCSP.
- Multi-tenant or end-user identity models.

## Must Settle
1. **Canonical certificate naming**
   The first-wave contract is a DNS SAN:
   `node-<id>.<cluster_domain>`.
   Example: `node-42.cluster.local`.

2. **Dial-side rule**
   `ServerNameResolver(target.id)` must return the canonical DNS SAN for that
   target. Client-side TLS verification already checks that the presented server
   cert matches the supplied SNI.

3. **Accept-side rule**
   After a successful mTLS handshake, the server extracts the peer leaf
   certificate's DNS SANs, finds the canonical node SAN, parses the numeric id,
   and sets `TransportConnection::remote_id()` from that value.

4. **Failure rule**
   Reject the connection when:
   - no canonical node SAN exists
   - more than one canonical node SAN is present
   - the SAN cannot be parsed into a numeric `NodeId`

5. **Authorization boundary**
   TLS establishes identity, not full authorization. A peer with a valid node
   certificate may still be rejected later by membership or RPC-level policy.

## Implementation
1. **Freeze the naming template**
   Add `cluster_domain` as config input in later ops work and define the
   canonical `node-<id>.<cluster_domain>` template as the source of truth.

2. **Use the same template on both sides**
   The dial side already has the right abstraction (`ServerNameResolver`).
   Implement the matching accept-side SAN parser in the TLS or server layer.

3. **Populate verified peer identity**
   Replace the current placeholder `NodeId { id: 0 }` in accepted server-side
   QUIC connections with the parsed, verified `NodeId`.

4. **Defer richer SAN forms**
   URI SANs, custom OIDs, or SPIFFE-style identities may be added later if
   operational needs demand them. They are not required for the first execution
   wave.

## Risks and Mitigations
- **Risk: overly clever certificate formats**
  - **Mitigation:** use one simple DNS SAN rule first. It matches current rustls
    SNI verification naturally and is easy to reason about.

- **Risk: blocking future join flows**
  - **Mitigation:** keep TLS identity binding separate from membership
    authorization. A peer may present a valid node identity before the cluster
    has fully admitted it.

- **Risk: drift between client-side resolver and server-side parser**
  - **Mitigation:** both use the same canonical template, not two unrelated
    naming rules.

## Timeline
- M0: approve the canonical SAN template.
- Workstream A.4: implement client/accept-side identity binding.
- Workstream E: load `cluster_domain` and related key material from config.

## References
- `plans/gap/codex_execution_20260415.md`
- `plans/gap/codex_execution_workstream_A_20260415.md`
- `plans/network/security.md`
- `src/network/tls.rs`
- `src/network/server.rs`
- `src/network/quic_transport.rs`
