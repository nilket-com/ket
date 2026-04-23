# Workstream G Audit Verify Usage Guide

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `G4`

## Purpose

Document the baseline verifier for the local NDJSON evidence path.

The current baseline command is:

```text
wendelon audit verify <path/to/audit.ndjson>
```

## What It Verifies

The verifier checks the local chain for:

- JSON parseability of each non-empty line
- sequence continuity
- `prev_hash` continuity
- `record_hash` correctness against the canonical event payload
- non-decreasing `raft_log_index` on `raft_log` authority events
- signed-root payload validity for `audit.chain_root`
- signed-root anchor correctness against the surrounding chain

## Output

The command prints a JSON report to stdout.

Important fields:

- `total_lines`
- `parsed_records`
- `malformed_records`
- `node_local_records`
- `raft_log_records`
- `signed_root_records`
- `signed_roots_verified`
- `last_materialized_raft_log_index`
- `issues`

## Exit Status

- `0`
  - verification completed and no issues were found
- `1`
  - verification completed but issues were found in the evidence
- `2`
  - the command itself failed, for example because the file could not be read

## Current Boundary

This verifier is for the baseline local NDJSON chain only.

It currently expects the input file to begin at genesis:

- the first persisted record is expected to have `sequence_number = 1`
- a partial export or concatenated subset that starts later will report `non_genesis_sequence`

It does not yet:

- cross-check multiple nodes
- verify reduced Kafka distribution projections
- verify archive/WORM objects
- emit an external auditor attestation package

Canonical Kafka payloads exported one-record-per-line as `PersistedAuditRecord` NDJSON can be checked with the same command, but that is still validating the local-chain record shape, not a separate Kafka-specific evidence bundle.
