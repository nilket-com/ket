# Impl Plan 074: Table-Query Trace Continuation (Producer)

**Status:** Implemented 2026-05-22; evidence recorded in
`plans/impl/0074_table_query_trace_continuation_evidence.md`. Paired with
`plans/adr/0074_table_query_trace_continuation.md`.
**Posture:** Producer-only Wendelon ADR — carry Toron's trace context through
`TABLE_QUERY_REQ` via a fixed-binary tail on `TableQueryRequestMeta`, and have
the Wendelon data server emit a sanitized `wendelon.table.query` span that adopts
Toron's parent through the **ADR 068 `otel.*` seam**. **No Toron production
change** (the Toron population is a cross-referenced consumer ADR — 072/073
precedent). Behavior-equivalent when the carrier is absent (the unchanged
Wendelon OTLP fixtures are the regression net). Four phases.

## Invariants carried in

- `Absent` carrier ⇒ **exact current behavior** (raw `data_ring.table_query.
  request` span unchanged; OTLP HTTP/gRPC fixtures pass unchanged). Wendelon
  telemetry disabled ⇒ no continuation regardless. `Unsampled` is authoritative
  (no root-sample). Malformed ⇒ fail-open unsampled, **never** reject the query.
  No new `observability::telemetry` seam; no `ComputeTraceContextV1` on the wire;
  no Control-Ring/SQL-wire/audit/cancellation/OTLP-metrics/non-table-query drift;
  **no Toron production change**; `crates/nilket` runtime-free; new surface
  brand-clean (no `*V1`).

## Phase 0 — Audit (verified anchors)

- **`TableQueryRequestMeta`** (`schema/table.rs:294`) is a fixed Pod, **136 B**;
  `const _: () = assert!(size_of == 136)` at `:320`, with offset asserts in the
  `:4122` test. `request_id: TableQueryRequestId` (the 073 cancellation field)
  is the current tail at offset 120.
- **`TableQueryOptions`** (`wendelon-client/lib.rs:157`) is the typed facade with
  `Default`; no trace field today. Built from scratch in Toron's
  `WendelonStorageScan::scan` (`toron/src/storage.rs:785`) — the consumer ADR's
  surface, not touched here.
- **Wendelon already emits data-ring spans** (`data_server.rs:2804+`) carrying
  raw `tenant_id`/`table_object_id`/hex ids — why D7/D9 require a *sanitized*
  linked span and leave the raw span unlinked.
- **The ADR 068 seam** (`observability/src/telemetry/mod.rs`): reserved
  `otel.{trace_id,span_id,parent_span_id,sampled}` fields, adopted + stripped,
  `otel.sampled` authoritative. **`blake3`** is already a direct Wendelon dep.

## Phase 1 — Wire carrier (schema + client)

- `schema/table.rs`: widen `TableQueryRequestMeta` tail with `trace_id:[u8;16]`,
  `trace_parent_span_id:[u8;8]`, `trace_flags:u8`, `_trace_pad:[u8;7]`. Update
  `assert!(size_of == 136)` → **168** and add `offset_of` pins (136/152/160);
  extend the `:4122` layout test. Flags `0x01` present, `0x02` sampled.
- `wendelon-client/lib.rs`: add `TableQueryTraceContext { Absent, Unsampled,
  Sampled { trace_id, parent_span_id } }`; `TableQueryOptions.trace_context`
  (default `Absent`); encode it into the meta tail (Absent⇒zero flags;
  Unsampled⇒present, sampled=0; Sampled⇒present+sampled+ids).
- Tests: default-absent; absent/unsampled/sampled encode+roundtrip; pinned
  size/offsets; sampled-zero-ids and unknown-flags decode as fail-open unsampled.

## Phase 2 — Server decode + carrier interpretation (fail-open)

- `data_server::handle_table_query`: decode the trace tail into a server-side
  three-state value. Apply D4 validation: present+sampled with zero `trace_id`/
  `parent_span_id` or unknown flags ⇒ **treat as unsampled**, continue the query
  unchanged (never error). `Absent` ⇒ existing path untouched.

## Phase 3 — Sanitized linked span + local-span preservation (D5–D10)

- For `Sampled`: derive the child span id (`blake3("wendelon.table_query.span.v1"
  ‖ trace_id ‖ parent_span_id ‖ request_id)[0..8]`, nonzero); start a sanitized
  `wendelon.table.query` span setting `otel.{trace_id,parent_span_id,span_id,
  sampled=true}` + bounded classes/magnitudes (D9) — **no raw ids**. Hold the
  linked span across the request so its lifetime covers the operation, but do
  **not** make it the current span while running the existing raw local span;
  record sanitized fields on the linked span directly.
- **Preserve the raw local span (D7):** keep `data_ring.table_query.request`
  exactly as today as a Wendelon-local, **unlinked** span. **Sibling, not nested
  (D8):** ensure the raw span is not created while `wendelon.table.query` is the
  current span (construct the linked span in a scope that does not enclose the
  raw span), so in-process parenting can't pull raw ids into Toron's trace.
- `Unsampled`: no linked span; do not root-sample the raw local span for this
  request. `Absent`: raw span as today (Wendelon's own sampling).

## Phase 4 — Guards, fixtures, evidence

- The ADR's 16 + 3 convention/meta guards in `wendelon/tests/architecture_test.rs`
  (incl. carrier-fixed-binary, offsets-pinned, span-id-derived-nonzero,
  unsampled-bypasses-root-sampling, sanitized-linked-span + local-raw-preserved,
  local-raw-never-nested, no-raw-ids-in-linked-span, malformed-fail-open,
  no-new-seam, absent-preserves-OTLP-fixtures, **no-Toron-production-change**,
  no-`ComputeTraceContextV1`-on-wire).
- 16 fixtures (ADR list) in `wendelon/tests/table_query_telemetry.rs`, anchored by
  **absent-context-preserves-the-unchanged-OTLP-HTTP/gRPC-fixtures**.
- `plans/impl/0074_table_query_trace_continuation_evidence.md`:
  `architecture_test adr074`, full `architecture_test`, `cargo test -p
  observability -p wendelon`, the unchanged `otlp_http_export`/`otlp_grpc_export`
  fixtures, `cargo check` Wendelon feature matrix, `clippy … -D warnings`,
  `git diff --check`.

## Watch-items

1. **Behavior-equivalence when absent is the whole risk** — the `otlp_http_
   export`/`otlp_grpc_export` fixtures stay **unchanged** and pass (the 067/073
   regression-net lesson).
2. **Sibling-not-nested** is the one real subtlety — the linked span must not
   enclose the raw span, or raw ids leak into Toron's trace.
3. **Producer-only** — do not touch `crates/toron`; the population is the
   cross-referenced Toron consumer ADR.
4. **Fail-open** — malformed/zero/unknown carriers never reject `TABLE_QUERY_REQ`.
5. **Fixed-binary, brand-clean** — pinned offsets; no string/sidecar/`*V1`/
   `ComputeTraceContextV1`.

## Acceptance checklist

- [ ] Phase 1: meta widened to 168 B (offsets pinned, asserts updated);
      `TableQueryTraceContext` + `TableQueryOptions.trace_context` (default Absent)
      encode/roundtrip; fail-open decode.
- [ ] Phase 2: server decodes three-state; malformed ⇒ unsampled, query served.
- [ ] Phase 3: sanitized linked span (otel.* + bounded fields, no raw ids);
      raw local span preserved + sibling-not-nested; unsampled suppresses.
- [ ] Phase 4: guards + fixtures + evidence; absent-context OTLP fixtures unchanged;
      no Toron production change.
- [ ] Cross-reference: Toron consumer ADR populates the carrier from `ScanRequest`.
