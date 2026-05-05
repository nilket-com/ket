# CDC Subscription Fixtures

ADR 054 fixture manifest. The current integration tests build these
fixtures programmatically so cursor, digest, and frame-layout checks stay
close to the public Rust APIs instead of drifting as static binary blobs.

## Cases

- `cursor_lex_order`: same-HLC rows ordered by `(row_hlc, row_key_id,
  record_row_index)`.
- `same_hlc_suffix_catchup`: subscribe from `(T, k5, 0)` and include only
  `(T, k7, 0)` and `(T, k9, 0)`.
- `event_schema`: CDC frame Arrow schema prepends row-axis synthetic
  columns and preserves user columns.
- `tombstone_event_schema`: tombstone events carry row-axis metadata,
  set `__row_tombstone__`, and null user columns.
- `live_tail_release`: staged entries are invisible until finalization
  release.
- `terminal_failure_discard`: terminal-failure entries are discarded before
  fan-out.
- `open_time_ring_continuity`: a non-empty ring whose head cursor exceeds
  the snapshot ceiling rejects with `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`.
- `checkpoint_visible_digest`: checkpoint digest is the order-independent
  `digest.table.row.visible.v1` value at the checkpoint cursor.
- `reject_taxonomy`: all eight `CDC_REJECT_*` variants round-trip as typed
  terminal `CDC_REJECT` frames.

## Owners

- Schema/cursor cases: `tests/cdc_cursor_ordering.rs`,
  `tests/cdc_frame_schema.rs`.
- Live-tail/finalization cases: `tests/cdc_live_tail.rs`,
  `tests/cdc_admission_stage.rs`, `tests/cdc_finalization_release.rs`.
- Wire/reject cases: `tests/cdc_frame_round_trip.rs`,
  `tests/cdc_subscribe_open.rs`.
- Catchup/checkpoint/backpressure cases: `tests/cdc_catchup.rs`,
  `tests/cdc_checkpoint.rs`, `tests/cdc_backpressure.rs`.
