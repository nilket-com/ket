# ADR 118: Stream-Shaped Storage Boundary (Execution-Pipelined Table Query)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — Codex retracted his iter-0 A-only lean
(transport-de-collect alone under-delivers the `LIMIT 3` remote-cost goal); all three land **A+B**. **Category:
Interior/Substrate cross-ring boundary** — Wendelon table-query server + protocol + `wendelon-client` + Toron
storage bridge. **Predecessors:** 059/060 (downstream cancellation), 116 (`row_limit` pushdown — 118 makes the
*general* cancel/early-stop bound remote cost too), 117 (recursive fragmenter — pipelining builds on it).
**Migration:**
`plans/impl/118_stream_shaped_storage_boundary.md`.

## Scale boundary

This ADR is the **single table-query storage boundary**: one Wendelon `TABLE_QUERY_REQ` producing a stream consumed
by one Toron `ScanStream`. It bounds remote work for that scan when the consumer cancels, drops, or satisfies a
bounded operator. It does **not** make Toron stages consume upstream exchanges before stage completion, introduce
cross-stage pipelining, add a new logical/physical operator, or promise row-level / disk-page-level laziness inside
Wendelon. Those are follow-ons.

## Doctrine — the storage boundary streams; the terminal frame carries final status

116 made cheap bare `LIMIT` semantically possible; 118 makes the boundary **physically match** — a Toron consumer
that stops early lets Wendelon **stop work before fully materializing the table query** (the named V1 "stream-shaped
storage boundary", roadmap:928; the foundation pipelining needs, roadmap:1433). The obstacle is **not** failure
handling (Toron fails a stream cleanly: `ScanStream` error → task fail → global cancel, no partial success) — it's
the **metadata path**: `encode_query_stream_batches_with_deadline` (`data_server.rs:13793`) pre-materializes so the
first-frame `QueryResponseMeta` can carry final `status`/`value_hlc`/`rows`, because *"continuations have no status
surface."* The resolution:

> **The first frame starts the stream; the terminal frame finalizes it.** First `TABLE_QUERY_RESP` frame =
> provisional `QueryResponseMeta` (`status = OK` "stream opened"; `rows`/`value_hlc` provisional) + first Arrow;
> continuation frames = Arrow only (the `FrameHeader.arrow_len` convention already exists); **terminal frame =
> authoritative final `QueryResponseMeta` + `FRAME_FLAG_STREAM_END`.** Partial-output-then-terminal-status is the
> defined contract. Empty successful results and pre-first-frame rejections may be a single terminal metadata frame
> with no Arrow. **No `QueryResponseMeta` widening** — provisional-vs-terminal is distinguished by frame position +
> `STREAM_END` + an opt-in cursor mode.

## Decisions (promotion-locked)

**D1 — Multi-frame `TABLE_QUERY_RESP`.** A successful table query may emit multiple frames on one stream:
provisional first (meta + Arrow), continuation (Arrow only), terminal (final meta + `STREAM_END`, possibly no
Arrow). Empty successful results may emit only the terminal metadata frame. Mirror `handle_query_stream` (`:5121`)
framing, but with table-query terminal metadata.

**D2 — Terminal metadata is authoritative.** Final `status`/`rows`/`value_hlc` come from the **terminal** frame, not
the first. The first frame's `status = OK` means "stream opened"; its `rows`/`value_hlc` are provisional and must
not be reported as final totals.

**D3 — No `QueryResponseMeta` widening (Codex D10 / Gemini).** Distinguish provisional-first vs authoritative-terminal
by frame position + `FRAME_FLAG_STREAM_END` + the opt-in cursor mode (D4) — not a new/widened struct. (Contrast
116's deliberate meta widening; here position carries the distinction.)

**D4 — Terminal-metadata is opt-in cursor behavior.** The generic `StreamingQueryResult` continuation rule stays
**strict by default** (rejects metadata on a continuation frame); table-query streams **opt into** a terminal-metadata
mode. Do not globally allow metadata-bearing continuation frames for every `QUERY_RESP` stream.

**D5 — Wendelon table-query execution becomes a producer.** `handle_table_query` (`:3873`) + the
`plan_query_execution_for_ranges_*` chain move from "build all `plan.batches`, then respond" to an **incremental
producer** that scans / applies PBC+runtime-filter / encodes / emits batches while accumulating final
`rows`/`value_hlc` for the terminal frame.

**D6 — Cancellation + row-limit checked during production.** The producer checks the 059/060 cancellation token and
the 116 `row_limit_rows` **between batch/row-group yields** — so a cancelled/dropped downstream consumer **stops
Wendelon before scanning all qualifying row groups** (the cost bound). `row_limit` stays exact (stop at `n`, emit
terminal).

**D7 — Full-scan and secondary-index parity.** The secondary-index table-query path uses the **same** streaming
producer contract (pipeline both) — otherwise the cost-bound claim depends on access path. (If one path can't
pipeline in V1, it's an explicitly-guarded deferred fallback, but the recommendation is parity.)

**D8 — Partial-output-then-terminal-error contract.** Once data is on the wire, a discovered terminal status
(`QUERY_CANCELLED`/`QUERY_BUDGET_EXCEEDED`/descriptor evolution/…) is emitted as the **terminal frame's status**
when the stream is still writable; Toron treats an observed non-OK terminal (or transport break) as a `ScanStream`
error → task fail → query cancel; previously emitted batches are **not** a successful result. A bounded consumer
that has already satisfied its cardinality may intentionally drop/cancel before reading terminal metadata; that is
the early-stop success path, not partial success after an observed terminal error.

**D9 — Toron consumes lazily.** `WendelonStorageScan::scan` calls `table_query_stream_with_runtime_filter*` and
wraps `StreamingQueryResult::next_batch` into a lazy `ScanStream` (no `collect_all`, no one-frame concat).
Descriptor pins, runtime filters, row identity, trace, 116 `row_limit`, and the 060 attempt-hook stay intact;
`materialize_scan_stream*` (coordinator) stays the consumer — **dropping it now cancels/stops the server**.

**D10 — Compatibility wrappers.** Public collecting `table_query*` client APIs remain as wrappers over the stream:
they return the result only if the terminal metadata is OK; a terminal error **discards partial batches and returns
an error**.

**D11 — Scope boundary.** No Toron IR/SQL/Rune/PBC/distributed-top-k change; **no row-level laziness** (V1 yields per
batch/row-group, not per disk page — name the granularity); descriptor/runtime-filter/row-identity/trace/row-limit
semantics unchanged.

## Behavior gates

- `table_query_resp_streams_with_terminal_metadata` — multi-batch query → provisional first (meta+Arrow) +
  continuation Arrow frames + terminal meta frame with `STREAM_END`.
- `empty_successful_table_query_can_emit_terminal_metadata_only` — zero-row success does not require a dummy Arrow
  frame just to open the stream.
- `terminal_metadata_totals_match_legacy_materialized_response` — final `rows`/`value_hlc` equal the old
  single-response totals for the same fixture.
- **`cancelled_consumer_stops_before_all_row_groups_scanned`** — the headline cost bound: a consumer that drops mid-
  stream makes Wendelon stop before scanning all qualifying row groups (observable via a cancelled-mid-scan
  outcome / scan counter).
- `bare_limit_three_bounds_remote_scan_without_row_limit_pushdown` — executor early-stop/drop bounds remote work
  even with `row_limit` pushdown disabled; the success path is cancellation/drop after the bounded consumer has
  enough rows, not trailer drainage.
- `table_query_row_limit_still_stops_at_n` (116 exactness preserved).
- `terminal_error_after_data_fails_toron_query` + `terminal_error_after_data_fails_client_collection` (D8/D10 —
  partial-then-error, no partial success).
- `secondary_index_table_query_streaming_parity` (D7).
- Regressions: aggregate/row-identity table queries, 059/060 cancellation, 115/117 ordered top-k, all unchanged.

## Architecture guards

- `adr118_table_query_req_uses_stream_handler`; `adr118_table_query_resp_has_terminal_metadata_contract`;
  `adr118_streaming_query_result_terminal_mode_is_opt_in` (D4 — generic continuation-metadata still rejected by
  default); `adr118_query_response_meta_not_widened` (D3).
- `adr118_handle_table_query_does_not_concat_all_batches`;
  `adr118_wendelon_table_query_checks_cancellation_during_production` (cancel checks inside the producer loop, not
  only before/after planning).
- `adr118_toron_storage_uses_table_query_stream_api` (retarget the existing `.table_query_with_runtime_filter(`
  storage guard); `adr118_attempt_hook_survives_stream_path` (060).
- `adr118_no_ir_sql_rune_pbc_change`.
- **Recipe:** `cargo check -p toron --all-targets` + `cargo test -p wendelon` + `architecture_test adr118/adr116` +
  `wendelon_storage_execution_test` + full `-p toron`.

## Council history

3-of-3 on the topic; **scope converged to A+B after Codex retracted his iter-0 A-only lean** (it under-delivers the
`LIMIT 3` cost goal). The "scary" part resolved by synthesis: Gemini grounded the *failure* path (Toron fails a
stream cleanly), Codex grounded the *metadata* path (first-frame final fields can't be known under pipelining),
Claude collapsed them into the **terminal-trailer status contract**. Both forks I surfaced resolved: the trailer
**reuses `QueryResponseMeta` by frame position (no widening)** + an **opt-in cursor mode** (Codex D4/D10, Gemini),
and **secondary-index parity** (pipeline both, Codex D7). The live risk is implementation *size* (the producer
refactor), not direction — the named fallback if it proves too large is a deliberately-titled transport-only ADR or
the logical-type lifecycle pivot, **not** pretending transport-only A is the stream-shaped boundary.

## What to do next

Row-group-lazy-from-disk scan (if V1 yields per-batch not per-page); **pipelined cross-stage execution** / the
executor-model upgrade — now unblocked by *both* the recursive fragmenter (117) and the stream-shaped boundary
(118); joins frontier; logical-type lifecycle; declared-additive types.

## Promotion criteria

`TABLE_QUERY_RESP` is a table-query-specific multi-frame stream with authoritative **terminal** metadata (no
`QueryResponseMeta` widening; opt-in cursor mode); Wendelon produces table-query batches incrementally and **stops
on cancellation/row-limit before full materialization** (the cost bound — observable + tested across full-scan and
secondary-index paths); Toron consumes lazily via `ScanStream`; terminal errors after data fail the query without
partial success; 116 `row_limit` + aggregate/row-identity/runtime-filter/trace + 059/060 + 115/117 unchanged; no
Toron IR/SQL/Rune/PBC change; the `adr118_*` guards hold; `cargo test -p wendelon` + full Toron suite +
`cargo check -p toron --all-targets` green. Migration: `plans/impl/118_stream_shaped_storage_boundary.md`.
