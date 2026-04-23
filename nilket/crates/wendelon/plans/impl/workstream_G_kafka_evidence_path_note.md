# Workstream G Kafka Evidence Path Note

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `G6`

## Purpose

Document the optional Kafka audit mode that carries the canonical local-chain record instead of the reduced event body.

## Runtime Contract

Kafka is still not the baseline evidence path.

The baseline remains:

- local `audit.ndjson`
- plus the archive mirror when `[audit.archive]` is configured

Kafka now has two explicit payload modes:

- `distribution`
  - default
  - publishes `AuditEvent`
  - suitable for SIEM or stream processing
  - not sufficient for chain replay
- `canonical`
  - optional
  - publishes the exact `PersistedAuditRecord` that was durably written to the local chain
  - carries `sequence_number`, `prev_hash`, `record_hash`, `hash_algorithm_tag`, and `schema_version`
  - can be exported one-record-per-line and verified with the baseline verifier

## Config Surface

```toml
[audit.kafka]
brokers = "kafka-1.internal:9092,kafka-2.internal:9092"
topic = "wendelon.audit"
payload_mode = "canonical"   # or "distribution"
client_id = "wendelon-node-01"
```

## Important Boundary

Canonical Kafka does not make Kafka the local authority.

What it does:

- preserves the same chain record shape off-box
- lets downstream consumers verify chain continuity for what Kafka actually received

What it does not do:

- change the fact that Kafka shipping is additive
- fail closed on Kafka sink outage
- replace the local NDJSON baseline or the archive mirror for serious operator retention

If Kafka publishing fails, Wendelon continues after the local emission and records `audit.sink_failure` in the local chain on the next successful local audit emit. That means canonical Kafka is independently verifiable for delivered records, but it is not the guaranteed always-complete baseline archive.

## Operator Guidance

- use `distribution` unless you explicitly need off-box chain verification
- use `canonical` when a downstream warehouse or SIEM needs the chain fields
- do not market Kafka as the sole compliance archive unless your operational process also accepts the additive-shipping failure model
- if you export canonical Kafka messages to NDJSON, the baseline verifier can validate them
