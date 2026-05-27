# Impl Plan — ADR 118: Stream-Shaped Storage Boundary (Execution-Pipelined Table Query)

**Migration for** `plans/adr/118_stream_shaped_storage_boundary.md`. **Cross-ring scope:** Wendelon
(`network/data_server.rs` stream handler + execution producer; `client/core.rs` `StreamingQueryResult`
terminal-metadata mode), `wendelon-client` (`table_query_stream*` returns the trailer), Toron (`storage.rs` lazy
`ScanStream` bridge). **Non-scope:** no Toron IR/SQL/Rune/PBC; **no `QueryResponseMeta` widening**; no row-level
laziness; no 115/116/117 semantics change. **The execution-producer refactor (Phase 3) is the dominant cost; the
named fallback if it proves too large is a transport-only ADR or the lifecycle pivot, not shipping A as "streaming."**

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Materialize-then-respond (refactor) | `handle_table_query` (`data_server.rs:3873`); `QueryExecutionPlan { response, batches }` |
| Streaming model + all-or-nothing reason | `handle_query_stream` (`:5121`); `encode_query_stream_batches_with_deadline` (`:13793`) |
| Meta (NOT widened) + continuation framing | `QueryResponseMeta { status, value_hlc, rows, … }`; continuation frames use `FrameHeader.arrow_len`; `FRAME_FLAG_STREAM_END` (`schema/core.rs:594`) |
| Cursor (terminal-metadata opt-in) | `StreamingQueryResult::next_batch` (`client/core.rs`) — currently rejects continuation metadata |
| Client stream facade | `wendelon-client` `table_query_stream_with_runtime_filter*` |
| Toron de-collect | `WendelonStorageScan::scan` (`toron storage.rs:853`) — `collect_all` → lazy `ScanStream` |
| Cancellation / row-limit (reuse) | 059/060 attempt hook + `ActiveTableQueryRegistry`; 116 `row_limit_rows` |

## Phase 1 — Terminal-metadata stream contract (Wendelon protocol, no widening)

Provisional-first / continuation-Arrow / terminal-meta + `STREAM_END`, distinguished by **frame position + the
`STREAM_END` flag**, reusing `QueryResponseMeta` unchanged (D3 — do not widen; keep its fixed-size/offset guards
intact). Add a **table-query terminal-metadata cursor mode** to `StreamingQueryResult` (D4): generic streams keep
rejecting continuation metadata; table-query streams accept the authoritative terminal frame. Include the zero-row
success case: terminal metadata with `OK`, `rows = 0`, no Arrow payload.

## Phase 2 — Wendelon table-query stream handler

Route `TABLE_QUERY_REQ` through a stream handler (mirror `handle_query_stream`): emit provisional-first + continuation
+ terminal frames. The terminal frame carries final `status`/`rows`/`value_hlc`. Pre-first-frame rejects and
zero-row successes may use a single terminal metadata frame with `STREAM_END` and no Arrow.

## Phase 3 — Server execution producer (dominant cost, B)

Refactor `handle_table_query` + `plan_query_execution_for_ranges_*` from "return `QueryExecutionPlan { batches: Vec }`"
into a **batch producer** that scans/PBC+runtime-filter/encodes/emits incrementally, accumulating `rows`/`value_hlc`:
- **check cancellation (059/060 token) + 116 `row_limit_rows` between batch/row-group yields** → on cancel, stop
  scanning (do **not** read remaining row groups) and, if the stream is still writable, emit the terminal frame
  with `QUERY_CANCELLED`; if the downstream has already dropped the stream, record the stream-drop/cancel outcome
  and stop without trying to finish the response; on row-limit, stop at `n` and emit terminal (116 exactness
  preserved);
- on mid-stream error → terminal frame with the error status when possible; transport write failure is recorded as
  stream drop and returned as transport error (D8);
- **secondary-index path: same producer contract (D7)** — pipeline both full-scan and secondary-index, or guard one
  as an explicit deferred fallback (parity preferred so the cost bound is access-path-independent).

## Phase 4 — Toron lazy `ScanStream` bridge (A)

`WendelonStorageScan::scan` → `table_query_stream_with_runtime_filter*` (116 `row_limit` options unchanged); wrap
`next_batch` into a lazy `ScanStream`. A non-OK terminal (or transport break) → `ScanStream` error (→ task fail →
query cancel). Keep the 060 attempt-hook + descriptor/runtime-filter/row-identity/trace. `materialize_scan_stream*`
unchanged — dropping it after a bounded consumer has enough rows cancels/stops the server and need not drain the
terminal metadata.

## Phase 5 — Compatibility wrappers (D10)

Public collecting `table_query*` client APIs become thin wrappers over the stream: drain + fold the terminal frame;
return the result only if terminal status is OK; a terminal error **discards partial batches and returns an error**.

## Phase 6 — Tests + guards

Behavior (per ADR §gates): `table_query_resp_streams_with_terminal_metadata`,
`empty_successful_table_query_can_emit_terminal_metadata_only`,
`terminal_metadata_totals_match_legacy_materialized_response`, **`cancelled_consumer_stops_before_all_row_groups_scanned`**
(the cost-bound proof — scan-counter/cancelled-mid-scan outcome), `bare_limit_three_bounds_remote_scan_without_row_limit_pushdown`,
`table_query_row_limit_still_stops_at_n`, `terminal_error_after_data_fails_toron_query`,
`terminal_error_after_data_fails_client_collection`, `secondary_index_table_query_streaming_parity`, regressions
(aggregate/row-identity, 059/060, 115/117). Guards: `adr118_table_query_req_uses_stream_handler`,
`adr118_table_query_resp_has_terminal_metadata_contract`, `adr118_streaming_query_result_terminal_mode_is_opt_in`,
`adr118_query_response_meta_not_widened`, `adr118_handle_table_query_does_not_concat_all_batches`,
`adr118_wendelon_table_query_checks_cancellation_during_production`, `adr118_toron_storage_uses_table_query_stream_api`
(retarget the storage guard), `adr118_attempt_hook_survives_stream_path`, `adr118_no_ir_sql_rune_pbc_change`.

## Phase 7 — Verification

```sh
cargo fmt
cargo test -p wendelon                          # stream handler + producer + terminal trailer + cancel-mid-scan
cargo test -p toron --test architecture_test adr118
cargo test -p toron --test architecture_test adr116
cargo test -p toron --test wendelon_storage_execution_test
cargo test -p toron
cargo check -p toron --all-targets
git diff --check
```

## Acceptance

`TABLE_QUERY_RESP` is a multi-frame stream with authoritative terminal metadata (no `QueryResponseMeta` widening;
opt-in cursor mode); Wendelon produces incrementally and **stops on cancellation/row-limit before scanning all
qualifying row groups** (observable + tested, full-scan and secondary-index); Toron consumes lazily via `ScanStream`;
terminal-error-after-data fails the query without partial success (collecting wrappers too); 116 `row_limit` +
aggregate/row-identity/runtime-filter/trace + 059/060 + 115/117 unchanged; no Toron IR/SQL/Rune/PBC change; the
`adr118_*` guards hold; `cargo test -p wendelon` + full Toron + `--all-targets` green.
