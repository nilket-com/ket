# CDC Subscription Substrate Runbook

Date: 2026-05-04
Owner: `H6`

## Scope

ADR 054 adds the CDC subscription substrate: CDC cursors, typed wire frames,
the in-memory live-tail ring, catchup/reject taxonomy, and bounded `cdc_*`
metrics. V1 CDC is a substrate contract, not a durable event log and not a
trigger/webhook system.

## Primary Metrics

- `wendelon_cdc_subscriptions_total{result}`
  - allowed `result`: `opened`, `closed`, `rejected`, `cancelled`
- `wendelon_cdc_events_emitted_total{source}`
  - allowed `source`: `ring`, `catchup`
- `wendelon_cdc_event_bytes_emitted_total{source}`
- `wendelon_cdc_checkpoints_emitted_total{source}`
- `wendelon_cdc_rejects_total{code}`
  - allowed `code`: `below_retention_floor`,
    `ring_overflow_during_catchup`,
    `schema_fingerprint_incompatible`,
    `checkpoint_digest_mismatch`, `unknown_table`,
    `tenant_auth_denied`, `table_not_replicated_here`,
    `internal_engine_error`
- `wendelon_cdc_subscription_lag_micros{source}`
- `wendelon_cdc_ring_overflow_total`
- `wendelon_cdc_catchup_to_live_handoff_micros`
- `wendelon_cdc_staged_entries_in_ring`
- `wendelon_cdc_pending_finalization_evicted_total`

Do not add tenant, table, peer, subscription id, schema fingerprint, or error
string labels to CDC metrics. Those are intentionally excluded from the V1
metric surface.

## Triage

`ring_overflow_during_catchup` means the in-memory live-tail ring could not
bridge the catchup-to-live handoff. Increase
`max_cdc_live_tail_bytes_per_locality`, reduce write pressure, or have the
consumer reconnect from a cursor still covered by retention.

`below_retention_floor` means the requested cursor is older than the table's
ADR 048 retention floor. `CdcCursor::MIN` is the retained-history sentinel and
clamps to the current floor; any other below-floor cursor must re-bootstrap.

Rising `wendelon_cdc_subscription_lag_micros{source="ring"}` with low
admission rate points at a stalled finalization frontier. Confirm the
locality-scoped heartbeat worker is running on every locality leader at
`cdc_locality_heartbeat_interval_millis` cadence. Heartbeats are locality
scoped, not per table: one quiet-locality marker advances
`locality_finalized_hlc(l)` for every table whose source-locality set includes
that locality.

`checkpoint_digest_mismatch` means the consumer's resume digest disagrees with
`digest.table.row.visible.v1` at the requested cursor. Treat it as a consumer
state divergence signal; re-bootstrap visible state before resuming.

## Recovery Notes

CDC subscriptions are long-lived streams. Consumers should persist their last
acknowledged `CdcCursor` and reconnect on stream close or typed reject. V1 does
not promise exactly-once delivery; consumers must apply idempotently by
`(row_hlc, row_key_id, record_row_index)`.

## Promotion Evidence

ADR 054 is not promotion-complete until the CDC acceptance matrix runs on
representative hardware with a measured one-way QUIC RTT. Use:

```sh
scripts/bench/run_cdc_subscription_acceptance.sh <output-dir> <measured_one_way_quic_rtt_micros>
```

The wrapper captures environment metadata, raw bench output, validation status,
and an acceptance summary. Treat the run as usable promotion evidence only when
`acceptance_summary.txt` reports `promotion_candidate=true` and every
`cdc_acceptance_*` line reports `status=pass`. Do not use the local smoke
placeholder RTT as representative evidence.
