# Workstream F Claim/OID Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `F2`

## Purpose

Freeze the pilot actor-certificate claim surface so dependent systems can integrate against a stable contract instead of reverse-engineering the parser.

## OID

- Actor-claims extension OID: `1.3.6.1.4.1.55555.1.1`
- This is still a placeholder PEN-backed OID until the production assignment lands.
- The payload shape is frozen now even though the final PEN is not.

## Frozen Payload Shape

The extension value is a CBOR map with exactly four keys:

- `role`: text
- `scopes`: array of text
- `tenant_id`: text or `null`
- `key_id`: bytes

The runtime now rejects:

- missing fields
- unknown fields
- duplicate fields
- empty `key_id`
- any map-shape drift away from the four-key contract

There is no explicit version field in the pilot contract. The current versionless four-key map is the frozen `v1` payload.

## Semantics

- `role` carries the Wendelon actor role string.
- `scopes` is always present, even when the effective scope set is narrow.
- `tenant_id` is always present.
  - `null` means cluster-wide identity.
  - text means the actor is tenant-bound and runtime policy must treat it as restricted to that tenant.
- `key_id` is an opaque certificate key-identity blob, not a human-readable label.

## Current Role Strings

The frozen role strings at this stage are:

- `data_node`
- `operator`
- `tenant_admin`
- `cluster_admin`
- `backup_admin`
- `compliance_auditor`
- `read_only_observer`
- `break_glass_admin`
- `query_session`

`operator` remains a compatibility role for now, but the claim string itself is still part of the frozen surface until a later migration deliberately removes it.

## Forward-Change Rule

Future claim-surface changes are not allowed to silently extend this map.

If the payload needs to change, it must happen through an explicit migration such as:

- a new OID
- a deliberately versioned successor payload
- a tightly-scoped compatibility window with published retirement steps

## Evidence

- `src/network/tls.rs`
- `tests/tls_identity.rs`
- `network::tls::tests::actor_claims_v1_*`
