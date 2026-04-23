# Workstream F Cert-Service Deployment Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `F3`

## Purpose

Define the baseline Wendelon cert-service boundary without turning Wendelon into a general-purpose PKI product.

## What Landed In F3

The code now has a real certificate-issuance policy baseline in `src/network/cert_issuance_policy.rs`.

That baseline fixes:

- the supported certificate classes
- which CA signs each class
- the maximum TTL for each class
- the fixed actor-claims payload carried on every issued leaf
- the explicit refusal to act as a general tenant-application CA

This slice does **not** yet land an HTTP or RPC front end for issuance. It lands the policy and leaf-shape engine that a future `wendelon-cert-service` binary will use.

## Narrow Scope Boundary

Supported leaf classes:

- node certs
- operator certs
  - `operator`
  - `tenant_admin`
  - `cluster_admin`
  - `backup_admin`
  - `compliance_auditor`
- `read_only_observer`
- `break_glass_admin`
- short-lived `query_session`

Explicitly out of scope:

- arbitrary tenant application certs
- long-lived end-user certificates
- becoming a universal customer CA

## CA Split

- `Node CA`
  - `data_node`
  - `read_only_observer`
  - `query_session`
- `Operator CA`
  - `operator`
  - `tenant_admin`
  - `cluster_admin`
  - `backup_admin`
  - `compliance_auditor`
  - `break_glass_admin`

## Baseline TTLs

- node: `24h`
- operator and sub-roles: `8h`
- observer: `24h`
- break-glass: `15m`
- query-session: `15m`

## Claim Semantics

Every issued leaf still uses the frozen F2 claim shape:

- `role`
- `scopes`
- `tenant_id`
- `key_id`

Baseline scope defaults:

- node: `["cluster"]`
- operator and sub-roles: `["cluster"]`
- observer: `["cluster"]`
- break-glass: caller must provide an explicit named scope; F6 later narrows this to the fixed break-glass scope taxonomy plus incident metadata
- query-session: `["query_session"]`

Baseline tenant binding:

- cluster-wide leaves encode `tenant_id = null`
- `query_session` encodes the concrete tenant binding

## Deployment Posture

The intended pilot posture remains:

- separate cert-service component, not embedded in the Wendelon data/control rings
- commodity default may run a single cert-service instance with software-backed signing keys and ordinary backup/restart procedures
- HA packaging is a later deployment choice, not part of the F3 baseline

The important no-deadlock property is architectural: certificate issuance/renewal must not depend on the Wendelon cluster whose certificates are being refreshed.

## Evidence

- `src/network/cert_issuance_policy.rs`
- `network::cert_issuance_policy::tests::*`
- `src/network/tls.rs`
