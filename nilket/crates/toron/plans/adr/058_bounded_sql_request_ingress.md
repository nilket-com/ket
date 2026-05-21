# ADR 058: Bounded SQL Request Ingress

**Status:** Promoted (2-of-3 — Claude + Codex; Gemini proposed a different
ADR (audit subsystem), carried forward)
**Seed:** Code-evidence defect surfaced during ADR 058 selection:
`sql_wire.rs::read_frame` allocates an inbound `u32`-declared payload before
decode, and `handle_sql_connection` plans an `Execute.statement` before the
statement timeout / admission. The roadmap's deferred "parser/front-end"
concern, answered the least-speculative way.
**Council:** iter-0 three directions (Claude: cancellation, pending audit;
Codex: bounded ingress, from code; Gemini: audit subsystem). The Claude/
Codex pair crossed over three times as evidence resolved — both candidates
(ingress, cancellation) proved real verified defects — and converged in
iter-3/4 on **ingress first** by severity + cadence: ingress is an *acute*
instant-allocation OOM vector and fully scoped; cancellation is real but
*bounded* (wasted compute until tasks complete) and larger (own round →
ADR 059). Gemini held the audit-subsystem candidate throughout (recorded
below). Promoted 2-of-3, as ADR 055/056/057 did.
**Category:** Toron compute-ring substrate ADR — SQL **frontend ingress**.
Exterior Substrate → Toron home. Node-local; no Wendelon server change, no
SQL wire variant / protocol change.

## Summary

`sql_wire.rs::read_frame` reads a 4-byte length prefix, converts the full
`u32` to `usize`, and does `vec![0u8; len]` **before** decoding — so an
authenticated client can declare a multi-gigabyte frame and force the
allocation before any gate. Separately, `handle_sql_connection` runs
`SqlPlanner::plan(&request.statement)` **before** the statement timeout and
before query admission, so an arbitrarily large statement spends parser/
planner CPU unbounded. ADR 058 adds two **default-on safety floors** for
**server inbound SQL requests**: a `max_request_frame_bytes` checked on the
length prefix *before allocation*, and a `max_statement_bytes` checked
*before `SqlPlanner::plan`*. This is a memory/CPU **safety floor**, not a
fairness knob — so, unlike ADR 055/056/057, it is **default-on**. It is
**server-inbound only** (outbound result batches are never clamped).

## Motivation

057 bounded *how many* SQL connections; the immediate post-057 frontend gap
is *how big a request* before we trust / allocate for / plan it. The
allocation is upstream of every existing gate: ADR 050's per-task memory
budget governs *execution*, not the frontend read; ADR 056's query-rate
admission doesn't bound statement size or parser work; ADR 057's connection
cap bounds count, not bytes per request. The fix is a byte boundary — and it
answers the long-deferred parser-CPU worry the least-speculative way
(bound bytes before parser work; do **not** invent a parser-QPS scheduler
without measured pressure).

## Decisions (14, Promotion-Locked)

**D1 Placement.** Toron SQL frontend, inside the SQL connection task. The
bounded request reader is used by `handle_sql_connection`. Worker transport,
observability listeners, Wendelon, and `crates/nilket` untouched.

**D2 Server-inbound-only helper.** Add a bounded server request reader (e.g.
`read_request_frame_bounded(reader, max_request_frame_bytes)`). Keep the
generic `read_frame` / `write_frame` shape for client/test round-trips and
**outbound result batches** — those are never clamped.

**D3 Check before allocation.** The bounded reader inspects the length
prefix and rejects an over-limit frame **before** allocating a `Vec`, pooled
buffer, or decoded payload. This ordering is the load-bearing fix.

**D4 Default-on config.** Add `[sql].max_request_frame_bytes` and
`[sql].max_statement_bytes`. Omitted ⇒ defaults. `0` is **invalid** (a
config error), not "disabled" — disabling a safety floor must not be a
routine posture. This intentionally differs from ADR 055/056/057, which
were governance knobs (default-disabled); 058 is a **safety floor**
(default-on).

**D5 Hard ceilings + ordering constraint.** Configured values must be
nonzero and ≤ a compile-time hard ceiling:
`SQL_REQUEST_FRAME_BYTES_HARD_CEILING` =
`SQL_STATEMENT_BYTES_HARD_CEILING` = 64 MiB. Also require
`max_statement_bytes ≤ max_request_frame_bytes` (a statement can't exceed
the frame that carries it).

**D6 Generous equal defaults.** `max_request_frame_bytes` =
`max_statement_bytes` = 4 MiB. The **equal** defaults are deliberate: a
default-on safety floor should avoid rejecting routine legitimate traffic, so
the statement cap defaults as generous as the frame cap and does **not**
become a tight parser-governance limit that clips ordinary bulk
`INSERT VALUES (…), (…), …` or large `IN (…)` statements. Operators may set a
tighter statement cap explicitly, or raise both caps within the hard ceiling
for intentionally large SQL text; tight statement/parser-work governance
stays evidence-gated future work. (This resolves the council's
safety-floor-vs-behavior-change pressure-test.)

**D7 Frame reject surface.** Declared request-frame length >
`max_request_frame_bytes` ⇒ close the stream/connection + record a bounded
metric/log reason. **Do not** decode, synthesize a `query_id`, or emit a SQL
error frame (no trusted identity yet).

**D8 Statement reject surface.** A decoded `Execute.statement` >
`max_statement_bytes` ⇒ return `SqlErrorCode::ResourceExhausted` for the
decoded `query_id`; **do not** call `SqlPlanner::plan`.

**D9 Query-id rule.** The reject surface follows decoded identity: **no
decoded `query_id` ⇒ transport close; decoded `query_id` ⇒ existing SQL
error frame.** (Same logic as 057's Tier1/Tier2 split.)

**D10 Inbound message treatment.** `Cancel` is **not** statement-capped.
Inbound `ResultBatch` / `Complete` / `Error` keep the existing
invalid-client-message path *after* decode — but **every** inbound frame is
subject to the request-frame cap *before* decode.

**D11 Timeout honesty.** `statement_timeout_seconds` must not be described
as a parse/planning timeout — it wraps execution *after* planning. 058's
statement-byte cap is the pre-plan guard. A parser wall-time
timeout/rate-limit stays evidence-gated future work.

**D12 Metrics.** Reuse the bounded listener reject family with reasons
`request_frame_too_large` and `statement_too_large`. No SQL text, IP, table,
`query_id`, raw-tenant explosion, or histogram.

**D13 Prior-guard reconciliation.** ADR 043's length-prefixed-postcard wire
guard stays true; 058 adds server-inbound bounded-reader guards **without**
changing SQL wire variants, protocol version, the generic writer, or the
result-batch shape. ADR 049-057 guards keep passing.

**D14 Out of scope.** Distributed cancellation (→ ADR 059); audit sinks;
parser-QPS/token buckets; pre-TLS/IP throttles; prepared statements; SQL
surface widening; worker-transport / observability-listener / Wendelon /
`crates/nilket` changes.

**Single-decision posture; Toron-ring only; node-local.**

## Architecture Guards (16, Promotion-Locked)

1. `adr058_sql_request_reader_checks_length_before_allocation`
2. `adr058_statement_limit_runs_before_sql_planner`
3. `adr058_request_limits_are_default_on_nonzero_safety_floor`
4. `adr058_request_limits_have_hard_ceilings`
5. `adr058_statement_cap_cannot_exceed_request_frame_cap`
6. `adr058_default_statement_cap_matches_request_cap`
7. `adr058_frame_reject_has_no_sql_error_before_query_id`
8. `adr058_statement_reject_uses_existing_resource_exhausted`
9. `adr058_cancel_frames_are_not_statement_capped`
10. `adr058_request_frame_limit_is_server_inbound_only`
11. `adr058_result_batch_frames_keep_existing_wire_shape`
12. `adr058_no_new_sql_wire_variant_or_protocol_version`
13. `adr058_no_parser_rate_queue_connection_or_cancel_drift`
14. `adr058_request_ingress_metrics_are_bounded_and_text_free`
15. `adr058_no_worker_transport_observability_wendelon_or_nilket_change`
16. `adr058_prior_adr043_and_adr049_through_adr057_guards_continue_to_pass`

**Convention guards:** `adr058_nilket_root_stays_runtime_free`,
`adr058_fixture_worker_source_unchanged`,
`adr058_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Prior-guard reconciliation:** ADR 043's length-prefixed-postcard guard
unchanged (D13); ADR 049-057 meta-guard persists.

## Integration Fixtures (12 required)

1. `oversized_declared_request_length_rejected_before_allocation_or_decode`
2. `oversized_decoded_execute_statement_returns_resource_exhausted_without_planning`
3. `default_config_applies_nonzero_request_and_statement_caps`
4. `default_statement_cap_equals_default_request_frame_cap`
5. `explicit_zero_cap_is_rejected_by_config`
6. `value_over_hard_ceiling_is_rejected`
7. `statement_cap_greater_than_request_frame_cap_is_rejected`
8. `normal_loopback_sql_below_both_caps_executes`
9. `normal_authenticated_remote_sql_below_both_caps_executes`
10. `cancel_below_request_frame_cap_is_not_statement_capped`
11. `server_result_batches_not_constrained_by_inbound_request_cap`
12. `request_ingress_metrics_use_bounded_reasons_and_omit_sql_text`

## Out of Scope

- **Distributed query cancellation** — real and verified (timeout drops the
  coordinator future without `CancelTask` propagation; no worker active-task
  registry), but bounded and larger → **ADR 059** with a focused round.
- **Audit subsystem** (045 D7) — compliance feature, not a code-evidence
  defect; needs its own sequence (shared-crate extraction, sink/custody
  policy, fail-open/closed). Carried forward.
- **Parser-rate throttle** — byte ceilings first; a parser-QPS scheduler
  only on measured parser-CPU pressure.
- **Pre-TLS / IP throttle** — 057 Tier 1 bounds pre-handshake concurrency
  without breaking multi-tenant proxies. Deferred.
- New SQL wire variant / protocol version; result-frame cap; SQL surface
  widening; prepared statements.

## Council History

- **iter-0:** Claude cancellation (pending audit); Codex bounded ingress
  (from code); Gemini audit subsystem.
- **iter-1:** Claude withdrew cancellation (hypothesis) for Codex's verified
  ingress defect.
- **iter-2:** Codex ran the worker-cancel audit Claude requested → it
  verified a *larger* cancellation gap; Codex pivoted to cancellation;
  Claude converged.
- **iter-3:** Codex retracted to ingress on scope/cadence; Claude converged
  after correcting an iter-2 severity over-statement (the cancellation leak
  is *bounded* wasted compute; ingress is the *acute* OOM vector) and
  pressure-tested the statement-cap default (must not clip bulk SQL).
- **iter-4:** Codex's final D-set adopted equal generous defaults + hard
  ceilings + the `statement ≤ frame` constraint, resolving Claude's
  pressure-test. 058 = ingress; 059 = cancellation.
- **Minority (Gemini, recorded):** the **audit subsystem** as 058. Not
  adopted — it is a compliance *feature* with no evidence pressure, not
  primary-axis, and a multi-sink shared-crate extraction that outranks
  neither an acute frontend OOM vector (058) nor a distributed resource leak
  (059). Preserved verbatim as a carried-forward 045-D7 candidate, not the
  evidence-pick.

## Promotion criteria met

1. D-set 14, 2-of-3 (Claude+Codex); Gemini's alternative recorded + carried.
2. Guards 16 + 3 convention; ADR 043 + 049-057 guards preserved.
3. Fixtures 12 required.
4. Toron-ring; SQL frontend; no Wendelon / SQL wire-variant change.
5. Placement: Exterior Substrate → Toron home; node-local.
6. Brand-clean / no-`*V1`; fixture worker unchanged; nilket runtime-free.
7. Default-on **safety floor** with generous equal defaults — closes the
   defect while avoiding routine-traffic behavior changes.

## Conclusion

ADR 058 closes the acute frontend ingress defect with two default-on byte
ceilings — frame-cap-before-allocation and statement-cap-before-plan — as a
safety floor (generous equal defaults, hard-ceilinged, server-inbound only),
with a `query_id`-keyed dual reject surface and no new SQL wire surface. It
is the narrow, verified post-057 completion: 057 bounded connection count;
058 bounds accepted request bytes before allocation and planning.
Distributed query cancellation is sequenced next as ADR 059. Migration:
`plans/impl/058_bounded_sql_request_ingress.md`.
