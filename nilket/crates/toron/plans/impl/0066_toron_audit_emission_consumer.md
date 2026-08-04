# Impl Plan 066: Toron Audit Emission Consumer

**Status:** Promoted, paired with
`plans/adr/0066_toron_audit_emission_consumer.md`.
**Posture:** Toron becomes the second `observability::audit` consumer (mirroring
Wendelon's 065 wiring). Default-disabled `[audit]`; typed `ToronAuditEvent`; a
Toron `AuditFormat` (own genesis ⇒ chain distinct from Wendelon's);
**fail-open** async emitter (no fail-closed path — that's the deferred
`audit.fail_closed` mode). **No substrate / Wendelon / SQL-wire / telemetry
change.** Six phases.

## Invariants carried in

- Disabled audit ⇒ **exact no-op** (no chain/worker/metrics/readiness/reject
  delta). `observability::audit` unchanged (async lives in Toron). No SQL wire
  frame/error-code change. `crates/nilket` runtime-free; fixture worker
  unchanged; no-`*V1`; no Wendelon change; no OTLP.

## Phase 0 — Audit (verified anchors)

- **Node-id seam (mirror Wendelon):** `wendelon/src/audit.rs`
  `parse_node_numeric(raw) -> u32 { NodeId::parse_wire(raw)?.id }` (:49),
  `shared_node_id(n) -> AuditNodeId { AuditNodeId::new(n.format_wire(), n.id) }`
  (:53). Toron mirrors with a `toron-n-<u32>` parser → `AuditNodeId::new(wire,
  numeric)`; validation **rejects bare-numeric / UUID** inputs and requires
  `parse(node_id) == node_numeric_id`.
- **SQL edge sites:** `SqlConnectionLimiter` (`max_concurrent_connections`,
  daemon.rs:272) — connection-limit refusal; `accepted_sql_tenant`
  (network/tls.rs) — tenant/identity rejection; frame/statement caps in the SQL
  frame read; protocol mismatch / unexpected client frame in the handshake.
  **Query-outcome** after `execute_query_with_context` (daemon.rs:791).
- **Metrics home:** `network/observability.rs` aggregate registry.
- **Config:** `[audit]` is a new section (config.rs has `[node]`/`[sql]`/
  `[admission]`/`[observability]`); no audit config today.

## Phase 1 — Cargo + config

- Toron `Cargo.toml`: `observability` → `features=["http","audit"]` (no kafka).
  Add `AuditConfig` under `[audit]`, **default-disabled**. Enabled ⇒ validate:
  local chain path, queue capacity > 0, `node_id="toron-n-<u32>"` +
  `node_numeric_id`, **parse-consistency** (`toron-n-<u32>` parses to the
  numeric; reject bare-numeric/UUID); sinks reuse the shared 065 config shapes.

## Phase 2 — `crates/toron/src/audit.rs`

- Typed `ToronAuditEvent` + operation / schema / actor / statement-class /
  outcome-class / SQL-error-code-class / duration-bucket / row-count-bucket
  enums → `observability::audit::AuditEvent` at the boundary (no shared Toron
  trait). `toron_audit_format()` → Toron-owned genesis / chain-root / signature
  tags + schema version + the `toron-n-<u32>` node parser ⇒ **a Toron chain
  distinct from Wendelon's**. `NoopAuditEmitter` for disabled.

## Phase 3 — Fail-open emitter

- `AuditEmitter`: **Noop** (disabled); **async** bounded `tokio::sync::mpsc`
  worker owning the shared `AuditSink`. **All producer hooks `try_send`** —
  queue-full / closed-worker / sink-write-error never alter SQL/listener/query
  behavior. Ordinary drops → `toron_audit_dropped_total`; **security-edge drops
  also → `toron_audit_security_event_unrecorded_total`**. (No fail-closed
  path.)

## Phase 4 — Hook points

- **Lifecycle:** startup, shutdown, sink_failure.
- **SQL edge-denials** (before the existing close/refuse, **no SQL frame
  added**): connection-limit (SqlConnectionLimiter), tenant/identity
  (accepted_sql_tenant), request-frame cap, statement cap, protocol mismatch,
  unexpected client frame.
- **Query-outcome** after `execute_query_with_context` (:791): completed /
  planned-error / execution-error / cancelled / timed-out / disconnect-aborted
  — class + bucket fields, **no SQL text / table / rows**.

## Phase 5 — Metrics + readiness

- Aggregate-only counters via `network/observability.rs`:
  `toron_audit_events_total`, `toron_audit_dropped_total`,
  `toron_audit_sink_errors_total`, `toron_audit_security_event_unrecorded_total`
  — labeled event-class / outcome / drop-reason / sink-kind only.
- Readiness: disabled ⇒ none; enabled ⇒ degrade only on local-chain open /
  persistent write failure (not best-effort drops).

## Phase 6 — Guards + retargets + fixtures + evidence + roadmap

- 21 + 3 convention + meta guards (ADR list), incl.
  `adr066_audit_node_wire_string_rejects_bare_numeric_and_uuid_inputs`,
  `adr066_all_first_cut_audit_emission_is_try_send_fail_open`,
  `adr066_security_event_drops_have_dedicated_aggregate_metric`,
  `adr066_no_sql_wire_protocol_or_error_code_change`,
  `adr066_observability_telemetry_remains_reserved_only`.
- **Two retargets:** (a) 065 `toron audit-not-enabled / http-only` →
  audit-feature allowed; meta `…_except_audit_feature_retarget`; (b) **roadmap
  fail-closed candidate text → fail-open + deferred `audit.fail_closed`**,
  recorded as planning-document reconciliation rather than architecture-guarded
  because roadmap prose is not a code invariant.
- 18 fixtures (ADR list). Evidence
  (`plans/impl/0066_toron_audit_emission_consumer_evidence.md`):
  `architecture_test adr066`, full `architecture_test`, `cargo test -p toron -p
  observability`, the `audit`-feature build, `clippy … -D warnings`, `git diff
  --check` (capped-linker).
- Roadmap: 066 promoted; `audit.fail_closed` + `observability::telemetry`/OTLP
  the next reserved facets.

## Watch-items

1. **Fail-open everywhere** — `try_send`; security drops → the distinct loud
   counter; no path blocks SQL/admission. (Deferred `audit.fail_closed`.)
2. **Self-consistent node-id** — `toron-n-<u32>` parses to `node_numeric_id`;
   reject bare-numeric/UUID; Toron chain distinct from Wendelon (own genesis).
3. **No PII / SQL text / plan-fingerprint** — class + bucket fields only.
4. **Disabled = exact no-op**; async in Toron (substrate stays sync).
5. **No SQL wire / Wendelon / telemetry change**; Kafka not a Toron default.
6. **Two retargets** — 065 audit-feature code guard + roadmap
   fail-closed-text planning reconciliation.

## Acceptance checklist

- [ ] Phase 1: `audit` feature (no kafka); `[audit]` default-disabled; node-id
      parse-consistency (reject bare-numeric/UUID).
- [ ] Phase 2: `toron/src/audit.rs` typed event layer; `toron_audit_format()`
      (own genesis ⇒ distinct chain); Noop emitter.
- [ ] Phase 3: fail-open async emitter (`try_send`); ordinary + security drop
      counters; no fail-closed path.
- [ ] Phase 4: lifecycle + edge-denials (no SQL frame) + query-outcome after
      :791 (class/bucket, no SQL text).
- [ ] Phase 5: aggregate-only counters; narrow readiness.
- [ ] Phase 6: 21+3+meta guards; two retargets; 18 fixtures; evidence;
      roadmap.
