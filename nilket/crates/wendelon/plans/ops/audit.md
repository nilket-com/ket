# Audit

Operator-facing audit guidance for Wendelon. This document covers the **sink shapes that exist today**, how they differ, and which sink an operator should treat as the source of truth for a given use case. Architectural intent lives in [ADR 025](../adr/025_audit_event_model_and_tamper_evident_hash_chain.md).

## 1. Sink Posture

Wendelon emits the same audit event stream to up to three sinks, but the sinks are **not schema-identical**:

| Sink | Config | Payload shape | Primary use |
|------|--------|---------------|-------------|
| Local NDJSON | always on | `PersistedAuditRecord` JSON per line | Tamper-evident local chain, replay, forensics |
| Kafka | `[audit.kafka]` | `AuditEvent` JSON per message | SIEM / stream ingestion |
| Syslog | `[audit.syslog]` | RFC 5424 envelope plus JSON parameters payload | Host-integrated audit shipping |

Rules:
- The local file at `<data_dir>/audit.ndjson` is always enabled. `Kafka` and `Syslog` are additive shipping sinks.
- The local NDJSON file is the **only** sink that carries the per-node chain fields required for replay and tamper detection.
- `Kafka` and `Syslog` are off-box projections for distribution and alerting. They are not sufficient to reconstruct the hash chain.
- Off-box sink failure does not block the audited operation. The failure is appended to the local chain as `audit.sink_failure` on the next successful local emission.

## 2. Local NDJSON

Each line in `<data_dir>/audit.ndjson` is a `PersistedAuditRecord`. This is the operator-facing on-disk form.

Fields:
- Chain fields: `event_id`, `timestamp_ns`, `sequence_number`, `schema_version`, `hash_algorithm_tag`, `prev_hash`, `record_hash`
- Event fields: `authority`, `outcome`, `operation`, `event_type`, `node_id`, `tenant_id`, `record_identity`, `raft_log_index`, `message`, `parameters`
- Actor fields: `actor`, `actor_role`, `actor_trust_domain`, `actor_key_id`, `break_glass`

`actor` is itself a structured object when present:
- `cert_subject`
- `role`
- `trust_domain`
- `key_id`
- `actor_tenant_id`
- `node_id`
- `break_glass`
- `scopes`

This is the only sink shape that supports:
- `replay_audit_chain`
- signed-root verification against a local chain head
- cross-file chain continuity via `prev_hash`

If an operator or auditor needs tamper-evidence, this file is the authoritative input.

## 3. Kafka

When `[audit.kafka]` is configured, Kafka receives a JSON-serialized `AuditEvent` per message.

Fields:
- `authority`
- `operation`
- `event_type`
- `node_id`
- `tenant_id`
- `record_identity`
- `actor`
- `outcome`
- `raft_log_index`
- `message`
- `parameters`

Notably absent from Kafka:
- `sequence_number`
- `prev_hash`
- `record_hash`
- `schema_version`
- `hash_algorithm_tag`
- denormalized actor projection fields such as `actor_role` and `break_glass`

Kafka is therefore a distribution format, not a chain-replay format.

## 4. Syslog

When `[audit.syslog]` is configured, syslog receives an RFC 5424 message. The current structured-data surface is intentionally small:

- `operation`
- `authority`
- `outcome`
- `actor_role`

The free-text message is:
- the audit event `message`
- followed by a JSON serialization of `parameters`

Implications:
- Syslog is the least schema-rich sink.
- It is appropriate for host-level forwarding into an existing SIEM pipeline that already expects RFC 5424.
- Consumers that need full actor detail, record identity, or tamper-evident chain metadata should not treat syslog as the primary audit archive.

## 5. Consumer Guidance

Use the sink that matches the question being asked:

| Question | Recommended sink |
|----------|------------------|
| “Can I replay and verify the audit chain?” | Local NDJSON |
| “Can I forward audit into a SIEM with minimal local retention?” | Kafka or Syslog, plus local NDJSON retained per policy |
| “Do I need a single normalized downstream schema?” | Normalize downstream from sink-specific shapes; do not assume wire identity across sinks |
| “Which sink is the minimum archive for compliance evidence?” | Local NDJSON |

Recommended posture:
- Treat Local NDJSON as the compliance / forensics archive.
- Treat Kafka as the richest off-box integration surface.
- Treat Syslog as a compatibility transport, not a canonical schema.

## 6. Why The Shapes Differ

The asymmetry is intentional in the current implementation:
- The local sink owns the per-node tamper-evident chain and therefore carries chain metadata.
- Kafka ships the typed event body without local-chain projection fields.
- Syslog preserves compatibility with RFC 5424-first deployments rather than embedding the full local record shape.

This means downstream consumers must either:
- ingest only one sink and live with that sink's shape, or
- normalize the three shapes into a deployment-specific warehouse schema.

What operators must **not** assume:
- that Kafka is a byte-for-byte copy of the local NDJSON line
- that syslog contains the same field set as Kafka
- that off-box sinks are enough to replay the local chain

## 7. Configuration Pointers

Example config:

```toml
[audit.syslog]
socket_path = "/run/systemd/journal/syslog"

[audit.kafka]
brokers = "kafka-1.internal:9092,kafka-2.internal:9092"
topic = "wendelon.audit"
client_id = "wendelon-node-01"
```

The additive-shipping model is:
- local file always on
- `Syslog` optional
- `Kafka` optional and feature-gated

## References

- [ADR 025](../adr/025_audit_event_model_and_tamper_evident_hash_chain.md)
- [Observability](./observability.md)
- [Configuration Schema](./configuration.md)
