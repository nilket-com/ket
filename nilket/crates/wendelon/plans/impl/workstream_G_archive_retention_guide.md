# Workstream G Archive And Retention Guide

Date: 2026-04-23
Author Role: Lead Architect
Workstream: `G5`

## Purpose

Document the baseline archive layer that sits on top of the authoritative local NDJSON chain.

## Runtime Contract

When `[audit.archive]` is configured, Wendelon mirrors the persisted local audit chain into a segmented archive directory.

Important properties:

- the archived payload is the same `PersistedAuditRecord` NDJSON shape as the local chain
- archive files are segmented and finalized as immutable `.ndjson` objects
- each finalized segment gets a sidecar `*.manifest.json`
- the local chain remains the authoritative per-node baseline
- archive mirroring is best-effort during runtime; if the archive path is unavailable, local evidence continues and archive backlog accumulates locally in memory until the sink recovers or the process restarts

## Config Surface

```toml
[audit.archive]
directory = "/mnt/worm/wendelon-audit/node-01"
max_segment_bytes = 8388608
lock_mode = "governance"   # "none", "governance", or "compliance"
retain_days = 2555
```

Fields:

- `directory`
  - archive root for this node
  - should be node-specific, not shared by multiple nodes
- `max_segment_bytes`
  - rollover threshold for finalized archive segments
- `lock_mode`
  - requested downstream retention posture for finalized segments
- `retain_days`
  - requested downstream retention horizon for finalized segments

## Segment Shape

Finalized archive segments are named:

```text
audit_<node_id>_<first_sequence>_<last_sequence>.ndjson
```

The corresponding manifest is:

```text
audit_<node_id>_<first_sequence>_<last_sequence>.ndjson.manifest.json
```

The manifest carries:

- node id
- first and last sequence number
- first `prev_hash`
- last `record_hash`
- hash algorithm tag
- requested `lock_mode`
- requested `retain_days`
- computed `retain_until_unix_seconds`

## WORM Boundary

`lock_mode` does not magically create immutability on an arbitrary filesystem.

What Wendelon does today:

- writes finalized segments once
- never reopens finalized segments for append
- marks finalized files read-only when `lock_mode != "none"`
- emits manifest metadata that an operator or downstream archival agent can map onto object-lock / WORM policy

What the operator or storage platform must still provide:

- a WORM-capable or object-lock-capable target if true immutability is required
- retention enforcement on the archive store itself

## Operator Duties

- point `directory` at storage intended for long-horizon evidence retention
- use a distinct archive directory per node
- if immutability matters, back the directory with a WORM/object-lock workflow instead of assuming local read-only bits are sufficient
- monitor:
  - `wendelon_audit_archive_pending_records`
  - `wendelon_audit_sink_failure_total{sink="archive",...}`
  - `wendelon_audit_archived_segments_total`
- do not treat Kafka or Syslog as the archive replacement; they are still projection sinks
- do not prune or rotate the local `audit.ndjson` baseline based only on archive configuration; verify archive success first

## Current Limits

- no external archive attestation package yet
- no archive re-verifier yet; use the local verifier on concatenated segments if needed
- archive backlog is in-memory only today, so prolonged archive outages still require operator reconciliation from the local chain
