# Audit

Operator-facing audit guidance for Wendelon. This document covers the **sink shapes that exist today**, how they differ, and which sink an operator should treat as the source of truth for a given use case. Architectural intent lives in [ADR 025](../adr/025_audit_event_model_and_tamper_evident_hash_chain.md).

## 1. Sink Posture

Wendelon emits the same audit event stream to up to three sinks, but the sinks are **not schema-identical**:

| Sink | Config | Payload shape | Primary use |
|------|--------|---------------|-------------|
| Local NDJSON | always on | `PersistedAuditRecord` JSON per line | Tamper-evident local chain, replay, forensics |
| Archive mirror | `[audit.archive]` | segmented `PersistedAuditRecord` NDJSON plus manifest sidecars | Long-horizon archive / WORM handoff |
| Kafka | `[audit.kafka]` | `AuditEvent` or canonical `PersistedAuditRecord`, depending on `payload_mode` | SIEM / stream ingestion, optional off-box chain verification |
| Syslog | `[audit.syslog]` | RFC 5424 envelope plus JSON parameters payload | Host-integrated audit shipping |

Rules:
- The local file at `<data_dir>/audit.ndjson` is always enabled. `Kafka` and `Syslog` are additive shipping sinks.
- When configured, the archive mirror copies the same persisted chain shape into segmented off-box-facing files. It is the preferred long-horizon retention path on top of the local baseline.
- The local NDJSON file is the **only** sink that carries the per-node chain fields required for replay and tamper detection.
- `Kafka` is off-box shipping. In the default `distribution` mode it is not sufficient to reconstruct the hash chain. In optional `canonical` mode it carries the exact persisted local-chain record for each delivered message.
- Off-box sink failure does not block the audited operation. The failure is appended to the local chain as `audit.sink_failure` on the next successful local emission.

## 1a. Authority Versus Materialization

Wendelon distinguishes:

- `authority`
  - where the event originates
- `authority_model`
  - whether the local chain is itself authoritative or is materializing a committed authoritative source

Current values:

- `node_local_authoritative`
- `raft_authoritative_materialized`

Interpretation:

- `node_local_authoritative`
  - the event originates on this node
  - the local NDJSON chain is the authoritative baseline evidence
- `raft_authoritative_materialized`
  - the event originates from a committed state-machine mutation
  - the committed Raft log is authoritative
  - the local NDJSON chain is the required serious-deployment materialization

## 1b. Data-Access Coverage And Pilot Sampling

Current live data-access audit coverage is:

- `data.write`
  - dispatcher-visible `requested`, `denied`, and `completed` forms
- `data.write_finalized`
  - Raft-authoritative durable-finalization evidence
- `data.read`
  - dispatcher-visible `denied` and `completed` forms
- `data.range_query`
  - dispatcher-visible `denied` and `completed` forms

Current pilot sampling policy is explicit:

- no sampling on the serious minimum set above
- effective rate is `100%` for every live `data.write`, `data.write_finalized`, `data.read`, and `data.range_query` event form

This is intentionally blunt. Wendelon does not yet have tenant compliance tiers or honest retention math for selectively dropping read-side evidence, so the current implementation emits the minimum serious set every time instead of implying a policy through omission.

## 2. Local NDJSON

Each line in `<data_dir>/audit.ndjson` is a `PersistedAuditRecord`. This is the operator-facing on-disk form.

Fields:
- Chain fields: `event_id`, `timestamp_ns`, `sequence_number`, `schema_version`, `hash_algorithm_tag`, `prev_hash`, `record_hash`
- Event fields: `authority`, `authority_model`, `outcome`, `operation`, `event_type`, `node_id`, `tenant_id`, `record_identity`, `raft_log_index`, `message`, `schema`, `parameters`
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

The typed `schema` object is the minimum serious machine-readable contract for:

- `administrative_control`
- `local_security`
- `data_access`

If an operator or auditor needs tamper-evidence, this file is the authoritative input.

## 3. Kafka

When `[audit.kafka]` is configured, Kafka payload shape is selected by `payload_mode`.

Default mode:

- `payload_mode = "distribution"`
- Kafka receives a JSON-serialized `AuditEvent` per message

Fields:
- `authority`
- `authority_model`
- `operation`
- `event_type`
- `node_id`
- `tenant_id`
- `record_identity`
- `actor`
- `outcome`
- `raft_log_index`
- `message`
- `schema`
- `parameters`

Notably absent from Kafka in `distribution` mode:
- `sequence_number`
- `prev_hash`
- `record_hash`
- `schema_version`
- `hash_algorithm_tag`
- denormalized actor projection fields such as `actor_role` and `break_glass`

`distribution` mode is therefore a distribution format, not a chain-replay format.

Optional canonical mode:

- `payload_mode = "canonical"`
- Kafka receives the exact `PersistedAuditRecord` written to the local chain
- this includes:
  - `sequence_number`
  - `schema_version`
  - `hash_algorithm_tag`
  - `prev_hash`
  - `record_hash`

Implications of canonical mode:

- delivered Kafka messages can be exported one-record-per-line and verified with `wendelon audit verify`
- Kafka still remains additive shipping; outage does not block the audited operation
- Kafka is therefore independently verifiable for delivered records, but it is not the guaranteed baseline archive

## 4. Archive Mirror

When `[audit.archive]` is configured, Wendelon mirrors the local persisted chain into segmented archive files under the configured directory.

Finalized files are named:

```text
audit_<node_id>_<first_sequence>_<last_sequence>.ndjson
```

Each finalized segment also gets:

```text
audit_<node_id>_<first_sequence>_<last_sequence>.ndjson.manifest.json
```

The manifest records:

- sequence bounds
- first `prev_hash`
- last `record_hash`
- requested `lock_mode`
- requested `retain_days`
- computed `retain_until_unix_seconds`

Current runtime posture:

- finalized archive segments are never reopened
- finalized archive segments are marked read-only when `lock_mode != "none"`
- runtime archive mirroring is best-effort; if the archive path fails, local authority continues and archive backlog is only in-memory until recovery or restart

Interpretation:

- the archive mirror is the long-horizon retention path
- true WORM or object-lock immutability still depends on the underlying storage platform
- local read-only bits are only a compatibility hint, not compliance-grade immutability

## 5. Syslog

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

## 6. Consumer Guidance

Use the sink that matches the question being asked:

| Question | Recommended sink |
|----------|------------------|
| “Can I replay and verify the audit chain?” | Local NDJSON |
| “Can I forward audit into a SIEM with minimal local retention?” | Kafka or Syslog, plus local NDJSON retained per policy |
| “Can an off-box consumer replay delivered chain records from Kafka?” | Kafka with `payload_mode = "canonical"` |
| “Do I need a single normalized downstream schema?” | Normalize downstream from sink-specific shapes; do not assume wire identity across sinks |
| “Which sink is the minimum archive for compliance evidence?” | Local NDJSON plus archive mirror |

Recommended posture:
- Treat Local NDJSON as the compliance / forensics archive.
- Treat the archive mirror as the preferred long-horizon retention copy of the local chain.
- Treat Kafka `distribution` mode as the richest off-box integration surface.
- Treat Kafka `canonical` mode as the optional off-box chain-verification path, not the baseline archive.
- Treat Syslog as a compatibility transport, not a canonical schema.

## 7. Why The Shapes Differ

The asymmetry is intentional in the current implementation:
- The local sink owns the per-node tamper-evident chain and therefore carries chain metadata.
- The archive mirror preserves that same chain shape for segmented retention.
- Kafka `distribution` mode ships the typed event body without local-chain projection fields.
- Kafka `canonical` mode ships the exact persisted local-chain record.
- Syslog preserves compatibility with RFC 5424-first deployments rather than embedding the full local record shape.

This means downstream consumers must either:
- ingest only one sink and live with that sink's shape, or
- normalize the three shapes into a deployment-specific warehouse schema.

What operators must **not** assume:
- that Kafka is always a byte-for-byte copy of the local NDJSON line
- that syslog contains the same field set as Kafka
- that off-box sinks are enough to replay the local chain

## 8. Configuration Pointers

Example config:

```toml
[audit.archive]
directory = "/mnt/worm/wendelon-audit/node-01"
max_segment_bytes = 8388608
lock_mode = "governance"
retain_days = 2555

[audit.syslog]
socket_path = "/run/systemd/journal/syslog"

[audit.kafka]
brokers = "kafka-1.internal:9092,kafka-2.internal:9092"
topic = "wendelon.audit"
payload_mode = "distribution"   # or "canonical"
client_id = "wendelon-node-01"
```

The additive-shipping model is:
- local file always on
- archive mirror optional but preferred for long-horizon evidence retention
- `Syslog` optional
- `Kafka` optional and feature-gated; `payload_mode = "distribution"` by default

## 9. Baseline Verification

The baseline local-chain verifier is:

```text
wendelon audit verify <path/to/audit.ndjson>
```

It prints a JSON report and uses exit status as the quick operator signal:

- `0` = verified cleanly
- `1` = verification completed but issues were found
- `2` = command failure (for example unreadable file)

## 10. Operator Duties For Archive Retention

- use a distinct archive directory per node
- back the archive directory with WORM/object-lock-capable storage if immutability matters
- monitor `wendelon_audit_archive_pending_records`
- monitor `wendelon_audit_sink_failure_total{sink="archive",...}`
- do not delete or rotate the local `audit.ndjson` baseline purely because archive config exists
- do not confuse Kafka/Syslog projection success with archive success

## References

- [ADR 025](../adr/025_audit_event_model_and_tamper_evident_hash_chain.md)
- [Observability](./observability.md)
- [Configuration Schema](./configuration.md)
- [Workstream G Audit Verify Usage Guide](../impl/workstream_G_audit_verify_usage.md)
- [Workstream G Archive And Retention Guide](../impl/workstream_G_archive_retention_guide.md)
- [Workstream G Kafka Evidence Path Note](../impl/workstream_G_kafka_evidence_path_note.md)
