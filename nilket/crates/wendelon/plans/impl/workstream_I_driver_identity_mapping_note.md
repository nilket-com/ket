# Workstream I Driver And Identity Mapping Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `I7`

## Purpose

Define the minimal reference posture for compute clients, drivers, and identity handoff without committing Wendelon to a connector marketplace.

## Driver Boundary

Wendelon does not expose SQL, JDBC, ODBC, or BI semantics directly.

The generic model is:

- compute client owns SQL/user protocol
- compute client maps user/session identity to a Wendelon `query_session`
- Wendelon serves storage RPCs and enforces storage authorization
- Wendelon reports storage-authored errors and quota outcomes

Toron is expected to be the first compute client using this pattern.

## Identity Mapping

| Source identity | Wendelon-facing identity | Notes |
|---|---|---|
| Human or application session in compute layer | short-lived `query_session` certificate | Tenant-bound and purpose-specific |
| Compute service operator | Wendelon operator/cluster role only when performing admin actions | Not used for ordinary user query data access |
| BI/JDBC/ODBC user | Compute-layer session identity | Wendelon sees only the tenant-bound storage credential |

## SSO And SAML Posture

Wendelon does not terminate SAML, OIDC, or enterprise SSO for query users in this contract.

The compute client owns upstream identity integration. Wendelon receives a storage-facing `query_session` credential with stable tenant attribution.

## Ingest And ELT Posture

Bulk ingest tools should integrate through a compute client or Wendelon client path that can obtain a tenant-bound storage credential.

Unsupported for this contract:

- arbitrary long-lived tenant application certificates
- direct end-user certificates issued by Wendelon
- opaque bearer tokens at the Wendelon storage RPC boundary

## Future Extension Points

- richer compute-client SDK helpers
- connector-specific examples
- stronger query-session issuance front end once the cert service has an RPC/API surface
