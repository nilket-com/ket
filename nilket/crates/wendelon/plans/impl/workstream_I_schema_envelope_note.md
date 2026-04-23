# Workstream I Schema Envelope Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `I3`
Code Surface: `src/integration/compute.rs`

## Purpose

Define the first durable schema-intent envelope for compute clients.

The envelope is generic even though Toron is the first consumer. This avoids putting Toron-specific terms into Wendelon's durable schema contract.

## Envelope

`SchemaUpdateEnvelopeV1` is encoded with `postcard` and then carried by the existing control-ring `SchemaUpdate(Vec<u8>)` request.

Required fields:

| Field | Meaning |
|---|---|
| `contract_version` | Must equal `COMPUTE_STORAGE_CONTRACT_VERSION` |
| `origin` | `compute_client` or `wendelon_operator` |
| `tenant_id` | Tenant whose schema state is changing |
| `object.kind` | `namespace`, `table`, `index`, or `view` |
| `object.namespace` | Stable namespace identifier |
| `object.name` | Stable object identifier |
| `expected_generation` | Prior generation expected by the caller, encoded as `SchemaGeneration` |
| `requested_generation` | Requested next generation, encoded as `SchemaGeneration` |
| `intent_id` | 16-byte idempotency/correlation identifier from the caller, encoded as `SchemaIntentId` |
| `mutation` | `create`, `alter`, or `drop` |
| `payload` | Opaque schema payload bytes |

## Validation

Wendelon validates the envelope before encoding and after decoding:

- supported contract version
- bounded namespace and object names
- requested generation strictly advances expected generation
- non-empty payload
- payload size no greater than `1 MiB`

Generation skips are allowed in v1. This lets a compute client batch or compact multiple semantic DDL steps into one storage-visible generation advance. Wendelon only enforces monotonic advance at this boundary.

The `intent_id` length is enforced by type: callers construct `SchemaIntentId([u8; 16])` rather than passing a length-checked byte vector.

Namespace is intentionally non-empty. Root-level objects must use the explicit `.` namespace sentinel; an empty namespace is invalid so root behavior cannot appear accidentally.

## Authority Rule

The envelope is intent, not durable state by itself.

Durability starts only when Wendelon commits the `SchemaUpdate` through the control ring. A compute client may cache its own planning state, but the Wendelon committed generation is the storage-side authority.

## Current Limitation

The existing state-machine request is still `SchemaUpdate(Vec<u8>)`. Workstream I now gives those bytes a versioned Wendelon-owned shape; a later slice can replace the raw byte variant with a typed enum once upgrade compatibility is planned.
