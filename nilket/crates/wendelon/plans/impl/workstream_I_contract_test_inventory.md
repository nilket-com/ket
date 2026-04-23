# Workstream I Contract-Test Inventory

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `I6`
Code Surface: `src/integration/compute.rs`
Test Surface: `tests/compute_integration_contract.rs`

## Purpose

Record the current contract tests for the compute/storage boundary.

The tests use generic compute-client naming. Toron can adopt the same contract once its repo grows a real API.

## Test Inventory

| Contract | Evidence | Status |
|---|---|---|
| Query-session handoff is tenant-bound and short-lived | `tests/compute_integration_contract.rs::mock_compute_client_gets_tenant_bound_query_session_handoff` | `live` |
| Schema intent is versioned and round-trips through the Wendelon envelope | `tests/compute_integration_contract.rs::compute_schema_intent_roundtrips_as_versioned_storage_envelope` | `live` |
| Schema envelope rejects unsupported versions and oversized payloads | `tests/compute_integration_contract.rs::schema_envelope_rejects_bad_versions_and_oversized_payloads` | `live` |
| Schema intent ID shape is enforced by type | `tests/compute_integration_contract.rs::schema_intent_id_type_enforces_sixteen_byte_shape` | `live` |
| Quota failures are storage-authored and terminal to unchanged requests | `tests/compute_integration_contract.rs::storage_status_mapping_keeps_quota_and_retry_semantics_distinct` | `live` |
| Quota metadata validates tenant and usage bounds | `tests/compute_integration_contract.rs::storage_quota_metadata_validates_tenant_and_usage_bounds` | `live` |
| Trace context uses W3C `traceparent` validation | `tests/compute_integration_contract.rs::compute_trace_context_must_be_valid_w3c_traceparent` | `live` |
| Audit attribution requires tenant-bound `query_session` identity | `tests/compute_integration_contract.rs::audit_attribution_rejects_wrong_tenant_query_session` | `live` |
| Client SDK exposes generic storage-status classification, including I/O terminal errors | `tests/compute_integration_contract.rs::client_error_exposes_generic_storage_status_classification` | `live` |

## Non-Goals

- These tests do not exercise `../toron`.
- These tests do not prove a deployed compute service can integrate yet.
- These tests intentionally validate the Wendelon-owned boundary before Toron has a real client implementation.
