# Impl Plan 058: Bounded SQL Request Ingress

**Status:** Promoted, paired with `plans/adr/0058_bounded_sql_request_ingress.md`.
**Posture:** Single plan, Toron-ring only. A bounded server-inbound SQL
request reader + a pre-plan statement-byte check; no new SQL wire variant /
protocol, no Wendelon / worker-transport / observability / nilket change.
Seven phases, dependency-ordered. **Default-on safety floor** with generous
equal defaults — closes the defect while avoiding routine-traffic behavior
changes.

## Invariants carried in

- ADR 043 length-prefixed-postcard wire shape unchanged; ADR 049-057
  behavior unchanged. Outbound result batches never clamped.
- `crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged;
  brand-clean / no-`*V1`. Node-local; SQL frontend only.

## Phase 0 — Audit (verified anchors)

- `sql_wire.rs:108 read_frame` reads the 4-byte length → `len`, then
  **`:115 vec![0u8; len]` before decode** — the allocation defect.
  `:96 write_frame` is outbound — leave unchanged.
- `daemon.rs:691 handle_sql_connection` → `:697 read_sql_frame` → on
  `Execute`, `:714 SqlPlanner::new()` / `:715 planner.plan(&request.statement)`
  — the plan-before-cap site (before the statement timeout / admission).
- `config.rs:210 SqlConfig` / `:406 RawSqlConfig` (currently `listener_bind`,
  `statement_timeout_seconds`) — the `[sql]` surface to extend.
- `network/observability.rs:582 RejectReason` (bounded family; gained
  `ConnectionLimit` in 057) + `increment_reject(listener, reason)` — the
  metric home.
- `SqlErrorCode::ResourceExhausted` (the statement-reject surface).

## Phase 1 — Config surface (config.rs, sql_wire.rs constants)

- `SqlConfig` += `max_request_frame_bytes: NonZeroU32` and
  `max_statement_bytes: NonZeroU32`. `RawSqlConfig` += `Option<u32>` each.
- Constants: `SQL_REQUEST_FRAME_BYTES_HARD_CEILING` =
  `SQL_STATEMENT_BYTES_HARD_CEILING` = `64 * 1024 * 1024`; defaults
  `4 * 1024 * 1024` each.
- Normalize (D4/D5/D6): omitted ⇒ default; `0` ⇒ **config error** (not
  disabled); value > hard ceiling ⇒ error; `max_statement_bytes` >
  `max_request_frame_bytes` ⇒ error. Commented `[sql]` template updated.
- Config tests: defaults nonzero + equal; `0` rejected; over-ceiling
  rejected; `statement > frame` rejected; existing configs still valid.

## Phase 2 — Bounded server request reader (sql_wire.rs) (D2/D3)

- Add a bounded helper, for example
  `read_request_frame_bounded<R>(reader, max_request_frame_bytes)`. It
  returns `io::Result<Result<SqlFrame, RequestFrameTooLarge>>`, reads the
  4-byte prefix, returns the too-large signal *before* `vec![0u8; len]` when
  `len > max_request_frame_bytes`, and otherwise reads + postcard-decodes as
  today.
- Keep generic `read_frame` / `write_frame` for client/test round-trips and
  outbound result batches — **not** clamped (D2/guard #10/#11).

## Phase 3 — Wire into the SQL connection task (daemon.rs) (D7/D8/D9)

- `handle_sql_connection` uses
  `read_request_frame_bounded(..., config.max_request_frame_bytes)`. On
  too-large: **close the stream/connection**,
  `increment_reject(Sql, RequestFrameTooLarge)`, return — **no decode, no
  `query_id`, no SQL frame** (D7/D9).
- On a decoded `Execute`: **before** `SqlPlanner::plan`, if
  `request.statement.len() > config.max_statement_bytes` → send
  `SqlErrorCode::ResourceExhausted` for the decoded `query_id` +
  `increment_reject(Sql, StatementTooLarge)`; **do not plan** (D8/D9).
- `Cancel` is not statement-capped (D10). Inbound `ResultBatch`/`Complete`/
  `Error` keep the existing invalid-message path after decode, but all went
  through the request-frame cap pre-decode.

## Phase 4 — Reject reasons / metrics (observability.rs) (D12)

- Extend `RejectReason` with `RequestFrameTooLarge` + `StatementTooLarge`
  (bounded family; rendered as `request_frame_too_large` /
  `statement_too_large` on the existing
  `listener_rejects_total{listener,reason}`). No SQL text / IP / table /
  `query_id` / per-tenant / histogram.

## Phase 5 — Timeout-honesty cleanup (daemon.rs, docs/tests) (D11)

- Audit any wording/test implying `statement_timeout` covers parse/planning;
  correct it. The statement-byte cap is the pre-plan guard; the timeout
  remains execution-scoped.

## Phase 6 — Guards + reconciliation (tests/architecture_test.rs)

- 16 architecture + 3 convention guards (ADR list), incl.
  length-check-before-alloc, statement-check-before-planner, default-on +
  hard ceilings + `statement ≤ frame` + equal defaults, the `query_id`-keyed
  dual reject surface, server-inbound-only / result-batch-shape-unchanged,
  no-new-wire-variant, bounded text-free metrics.
- ADR 043 postcard guard unchanged; meta-guard extended through 057.

## Phase 7 — Fixtures + evidence + roadmap

- 12 fixtures (ADR list), incl. loopback + authenticated-remote happy paths
  below caps, oversized-frame-before-alloc, oversized-statement →
  `ResourceExhausted`-without-plan, `0`/over-ceiling/`statement>frame`
  config rejections, `Cancel` not statement-capped, result batches unclamped.
- `plans/impl/0058_bounded_sql_request_ingress_evidence.md`:
  `architecture_test adr058`, full `architecture_test`,
  `cargo test -p toron`, sql_listener + config suite counts,
  `clippy -p toron --all-targets -- -D warnings`, `git diff --check`
  (capped-linker procedure).
- Roadmap: 058 promoted; **ADR 059 = Distributed Query Cancellation
  Completeness** (committed next); audit / parser-throttle / pre-TLS carried
  forward.

## Watch-items

1. **Order is the fix:** the length-prefix check must precede
   `vec![0u8; len]` (guard #1) and the statement-len check must precede
   `SqlPlanner::plan` (guard #2). These two orderings are the whole ADR.
2. **Server-inbound only:** never route `write_frame` / result-batch reads
   through the bounded reader — a global cap would silently truncate large
   results (guard #10/#11).
3. **`query_id`-keyed reject surface:** pre-decode ⇒ transport close (no
   frame); post-decode ⇒ `ResourceExhausted`. Don't synthesize a `query_id`
   to send a frame for the frame-cap case.
4. **Generous equal defaults (D6):** statement default == frame default ==
   4 MiB to avoid clipping routine bulk `INSERT VALUES`; a tight
   statement/parser limit stays evidence-gated future work.
5. **`0` is a config error, not "disabled"** — a safety floor isn't
   silently turn-off-able.

## Acceptance checklist

- [ ] Phase 1: `[sql]` caps parse; nonzero defaults equal at 4 MiB; `0` /
      over-ceiling / `statement>frame` rejected; back-compat.
- [ ] Phase 2-3: bounded reader checks length before alloc; statement check
      before plan; dual reject surface by `query_id` presence.
- [ ] Phase 4: bounded reject reasons; text-free.
- [ ] Phase 5: no wording claims the timeout covers planning.
- [ ] Phase 6-7: 16+3 guards, ADR 043 + 049-057 preserved, 12 fixtures,
      evidence + roadmap; no cancel/parser-rate/connection-cap drift; result
      batches unclamped.
