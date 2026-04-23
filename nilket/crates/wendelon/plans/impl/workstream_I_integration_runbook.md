# Workstream I Integration Runbook

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `I6`
First Consumer: `Toron`

## Purpose

Provide the operator and implementer path for bringing a compute client onto Wendelon.

This runbook is written for Toron as the first consumer, but every step uses generic Wendelon compute-client terminology.

## Preconditions

- Wendelon cluster is running with the F3 certificate issuance policy available.
- The compute client has a tenant identifier and a key pair for the storage-facing session credential.
- The compute client can propagate W3C `traceparent`.
- The compute client can submit schema intent as `SchemaUpdateEnvelopeV1` bytes.

## Integration Steps

1. Register or select the target Wendelon tenant.
2. Request a short-lived `query_session` credential for that tenant.
3. Attach the resulting mTLS client credential to Wendelon storage RPCs.
4. Include W3C `traceparent` on user-originated request paths where cross-system tracing is required.
5. For storage-visible schema changes, submit a `SchemaUpdateEnvelopeV1` through the control-ring path.
6. Treat Wendelon quota, authorization, retention, and route-refresh statuses as storage-authored.
7. Preserve Wendelon audit attribution by keeping the `query_session` credential tenant-bound and scope-bound.

## Operational Checks

After integration, verify:

- the issued credential has role `query_session`
- the credential carries `tenant_id`, not `null`
- the credential scope includes `query_session`
- schema updates round-trip through `SchemaUpdateEnvelopeV1`
- quota failures surface as `quota_exceeded`, not generic compute errors
- trace IDs are visible on both compute and storage spans
- audit records attribute reads/writes to the `query_session` actor and tenant

## Failure Handling

| Failure | Action |
|---|---|
| Credential TTL rejected | Reduce requested TTL to `15m` or lower |
| Credential has no tenant binding | Refuse to use it for user-originated storage access |
| `traceparent` rejected | Generate a valid W3C version `00` trace context |
| `quota_exceeded` | Surface the storage-authored quota failure; do not retry unchanged |
| route refresh status | Refresh Wendelon topology/epoch and retry according to client retry policy |
| backpressure status | Retry after bounded backoff |
| schema generation mismatch | Refresh storage-visible schema generation before resubmitting intent |

## Current Toron State

`../toron` currently contains only a minimal Rust binary and brainstorm docs. There is no Toron API to call yet.

The supported evidence at this point is therefore Wendelon-side:

- generic contract module
- unit tests
- mock compute-client contract tests
- this runbook
