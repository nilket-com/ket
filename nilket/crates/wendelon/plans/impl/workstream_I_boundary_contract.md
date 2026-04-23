# Workstream I Boundary Contract

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `I1`
First Consumer: `Toron`
Code Surface: `src/integration/compute.rs`
Client Surface: `crates/wendelon-client/src/lib.rs`

## Purpose

Freeze the Wendelon-owned side of the compute/storage boundary without baking Toron naming into Wendelon's public contract.

Toron is the first intended consumer. The contract language is deliberately generic:

- `compute client`
- `query_session`
- `SchemaUpdateEnvelopeV1`
- storage-authored quota and error outcomes
- W3C trace context

This keeps Wendelon usable by Toron now without making future compute layers inherit Toron-specific API names.

## Contract Version

- Current version: `COMPUTE_STORAGE_CONTRACT_VERSION = 1`
- Code owner: `Wendelon`
- Contract module: `integration::compute`
- Client crate re-export: `wendelon-client`
- ADR authority: ADR `034`, ADR `032`, ADR `018`

Version `1` covers:

- short-lived storage-facing identity handoff
- durable schema-update envelope encoding
- storage status mapping for compute callers
- optional quota metadata trailers
- W3C trace-context validation
- audit-attribution checks for user-originated storage access

## Ownership Boundary

| Surface | Owner | Contract |
|---|---|---|
| SQL parsing and semantic planning | compute client | Wendelon does not parse SQL or own user-facing query semantics |
| User/session lifecycle | compute client | Wendelon receives only the storage-facing identity and request context |
| Storage authorization | Wendelon | Runtime admission uses the `query_session` actor claims |
| Durable schema sequencing | Wendelon | Compute may originate intent; Wendelon commits ordered schema state |
| Quota outcomes | Wendelon | Compute may display/cache outcomes but must not reinterpret them |
| Trace context | shared | Compute sends W3C context; Wendelon validates and propagates correlation |
| Audit attribution | Wendelon | Storage audit records use Wendelon actor identity and tenant binding |

## Identity Handoff

The compute client asks the Wendelon cert-service policy layer for a short-lived `query_session` credential.

The Wendelon-side contract is:

- role: `query_session`
- trust domain: data
- scope: `query_session`
- tenant binding: required
- maximum TTL: `15m`
- actor-claims payload: the F2 four-key CBOR shape

This credential is not a general application certificate and not a customer PKI surface.

## Schema Boundary

Compute clients submit storage-visible schema intent as `SchemaUpdateEnvelopeV1` bytes.

The envelope carries:

- `contract_version`
- `origin`
- `tenant_id`
- object kind, namespace, and name
- expected and requested generations
- typed `SchemaIntentId`
- mutation kind
- opaque schema payload bytes

Wendelon is the durable sequencing authority. A compute client may create intent but may not mutate durable schema state out of band.

## Error And Quota Boundary

Wendelon maps wire-level `WriteStatus` and `ReadStatus` into generic compute-facing status categories in `StorageStatusKind`.

The SDK-level `ClientError::storage_status_kind()` exposes the same generic classification so compute clients do not have to switch directly on raw wire status values.

Important rules:

- quota failures are storage-authored
- tenant suspension/unavailability is storage-authored
- route refresh and backpressure are retryable categories
- `PROXY_LIMIT` maps to route refresh because it usually means the caller followed stale routing or proxy state until the hop cap fired
- invalid lease signatures map to terminal storage error, not authorization failure, because they are treated as storage-integrity or stale-lease failures rather than caller identity denials
- query invalid-program, budget, and retention failures are terminal unless the caller changes the request or policy changes
- success does not imply quota metadata is always present

Quota metadata, when returned, uses `StorageQuotaMetadataV1` and must match the request tenant.

## Trace And Audit Boundary

Compute-originated requests must carry W3C `traceparent` if cross-system tracing is expected.

Wendelon validates:

- version `00`
- lowercase hex trace ID
- lowercase hex span ID
- non-zero trace and span IDs
- bounded `tracestate`

`traceparent` validation is strict for v1. `tracestate` validation is intentionally non-strict: Wendelon bounds length and rejects control characters, then treats the value as pass-through metadata owned by the tracing ecosystem.

Audit attribution requires a tenant-bound `query_session` actor with the `query_session` scope. Wendelon rejects cluster-wide or wrong-tenant query-session actors for user-originated storage access.

## Non-Goals

- no Toron-specific Rust dependency in Wendelon
- no Wendelon SQL front end
- no generic tenant application CA
- no arbitrary in-storage compute
- no claim that `../toron` implements this contract yet
