# ADR 066: Toron Audit Emission Consumer

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** ADR 065 established the shared `observability::audit` substrate but
deliberately stopped there; Toron has no audit config / payload / emission. 066
is the consumer (the 065→066 producer/consumer split, like 062→063 / 073→060).
Toron emits durable, hash-chained, privacy-preserving audit evidence for
sensitive node activity — default-disabled, reusing the shared substrate
unchanged.
**Category:** Toron compute-ring — consumer of `observability::audit`. No
substrate, Wendelon, SQL-wire, worker-transport, or telemetry/OTLP change.

## The authority fork — resolved (fail-open + loud metric)

The run's most-contested decision. Resolution: **fail-open emission for all
first-cut events + a dedicated loud counter for missed security evidence**
(`toron_audit_security_event_unrecorded_total`). The principle: **audit is
evidence, not the authorization gate.** A denial already rejects (fail-closed
is moot for it); blocking a successful admission on audit IO is a deliberate
availability tradeoff that must be an explicit gate, not a side effect of
adding the audit stream. **Recorded deferral:** durable-capture / wait-for-
evidence is a valid follow-on as an explicit **`audit.fail_closed`** mode with
its own ADR, threat model, timeout budget, client + readiness semantics, and a
fixture proving the accept loop cannot be held hostage by audit IO.

*(Council path: Claude proposed fail-open (iter-0), over-corrected to
fail-closed-for-denials (iter-1) and withdrew it (iter-2); Codex held the
fail-open reasoning throughout; Gemini converged from wait-for-evidence
(iter-2). The argument resolved it, not the count.)*

## Decisions (17, Promotion-Locked — Codex iter-4 D1–D17)

**D1 Consumer only.** Toron enables + consumes `observability::audit`; the
shared crate stays the **synchronous** substrate, Toron owns async policy. No
substrate / Wendelon / SQL-wire / worker change.

**D2 Default-disabled.** Omitted `[audit]` / `enabled = false` ⇒ no chain file,
no emitter worker, no audit metrics, no readiness effect, no listener/query
behavior change.

**D3 No Kafka default.** Toron's `observability` dep enables `http` + `audit`,
**not** `kafka`; projection sinks use only features compiled into the shared
crate.

**D4 Self-consistent audit node identity.** Enabled audit requires
`node_id = "toron-n-<u32>"` and `node_numeric_id = <u32>`; config validation
**parses the wire string and requires it to match the numeric id**. Bare
numeric strings, UUID truncation, and UUID hashing are **rejected**.

**D5 Toron-owned profile.** Toron defines its own `AuditFormat` tags, schema
version, node parser, operation strings, and schema vocabulary ⇒ a Toron chain
**distinct from Wendelon's even on the same host**.

**D6 Typed local event layer.** `crates/toron/src/audit.rs` owns
`ToronAuditEvent` + typed operation/schema/actor/class enums, converting to
`observability::audit::AuditEvent` at the boundary (no shared Toron trait).

**D7 Narrow taxonomy.** Lifecycle + SQL edge-denial + SQL query-outcome only.

**D8 Edge-denial events.** Connection-limit refusal, tenant/identity rejection
after handshake, request-frame cap, statement cap, protocol mismatch,
unexpected client frame.

**D9 Query-outcome events.** Emitted after the outcome is known: completed,
planned-error, execution-error, cancelled, timed-out, disconnect-aborted.

**D10 Privacy boundary.** No raw SQL / params / result rows / Arrow IPC / table
names / descriptor payloads / descriptor schemas / cert material / SAN strings
/ peer subjects / free-form error strings / **plan fingerprints**.

**D11 Bounded query fields.** tenant id, session id, query id, statement
**class**, outcome **class**, SQL-error-code **class**, duration **bucket**,
row-count **bucket** (`zero|one|small|medium|large`) — not exact counts (audit
is not result telemetry).

**D12 Fail-open emitter.** Enabled audit uses a bounded `tokio::sync::mpsc`
worker; producers use non-blocking **`try_send`** only.

**D13 Drop accounting.** Ordinary drops → `toron_audit_dropped_total`;
security-relevant edge-event drops **also** →
`toron_audit_security_event_unrecorded_total`.

**D14 Narrow readiness.** Disabled ⇒ none. Enabled ⇒ degrade only on
local-chain open failure / persistent local-chain write failure; queue pressure
+ best-effort drops do not affect readiness.

**D15 Aggregate-only metrics.** Label only by fixed event-class / outcome /
drop-reason / sink-kind — never tenant / query / session / table / descriptor /
SQL text / peer identity / cert material / error string.

**D16 No client protocol drift.** Audit adds no SQL message / error code /
protocol version / frame field / client-visible diagnostic; existing transport
close/refuse shapes intact.

**D17 Prior-guard + roadmap reconciliation.** ADR 065's "Toron audit not
enabled / HTTP-only dependency" guards are **retargeted** to allow the audit
consumer while preserving "no shared-substrate mutation". The **roadmap's
ADR-066-candidate fail-closed text is retargeted to fail-open + deferred
`audit.fail_closed`**.

## Architecture Guards (21 + 3 convention + meta)

`adr066_toron_enables_observability_audit_without_kafka_default`,
`adr066_audit_config_is_default_disabled`,
`adr066_audit_config_requires_explicit_node_identity_when_enabled`,
`adr066_audit_node_wire_string_parses_to_configured_numeric_id`,
`adr066_audit_node_wire_string_rejects_bare_numeric_and_uuid_inputs`,
`adr066_toron_audit_format_is_consumer_provided`,
`adr066_toron_payload_is_typed_and_toron_owned`,
`adr066_toron_audit_operations_are_bounded_strings`,
`adr066_no_raw_sql_pii_descriptor_schema_or_plan_fingerprint_in_audit`,
`adr066_query_payload_uses_statement_class_not_raw_sql`,
`adr066_query_payload_uses_row_count_bucket_not_exact_count`,
`adr066_sql_edge_denials_emit_bounded_audit_events`,
`adr066_query_outcomes_emit_after_execution_resolution`,
`adr066_all_first_cut_audit_emission_is_try_send_fail_open`,
`adr066_security_event_drops_have_dedicated_aggregate_metric`,
`adr066_async_policy_lives_in_toron_not_observability_audit`,
`adr066_audit_metrics_are_aggregate_only`,
`adr066_audit_readiness_degrades_only_on_local_chain_failure`,
`adr066_no_sql_wire_protocol_or_error_code_change`,
`adr066_no_wendelon_runtime_or_protocol_change`,
`adr066_observability_telemetry_remains_reserved_only`.
**Convention:** `adr066_nilket_root_stays_runtime_free`,
`adr066_fixture_worker_source_unchanged`,
`adr066_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr066_prior_adr045_to_065_guards_continue_to_pass_except_audit_feature_retarget`.

## Prior-Guard / Roadmap Reconciliation

Two retargets, one code-guarded: (1) the **065 audit-feature retarget** —
065's "Toron consumes `observability` with `http` only / audit-not-enabled"
guards flip to "audit feature allowed" (audit-present + stale-http-only-claim
gone); (2) the **roadmap fail-closed-text retarget** — the 065-era ADR-066
candidate text (authored describing fail-closed-for-security) flips to
fail-open + deferred `audit.fail_closed`. The roadmap text retarget is recorded
as planning-document reconciliation rather than architecture-guarded, because
roadmap prose is not a code invariant. Meta scoped
`…_except_audit_feature_retarget`.

## Integration Fixtures (18)

audit-disabled-no-op (no chain/worker/metrics); enabled-requires-`toron-n-<u32>`-
+-matching-numeric; node-id-rejects-bare-numeric-and-uuid; enabled-writes-
startup-record; verify-CLI-accepts-Toron-chain; tenant-reject-without-peer-
identity; connection-limit / oversized-frame / oversized-statement / protocol /
unexpected-frame audited without SQL-frame/payload/text; query-success records
statement-class+duration-bucket+row-count-bucket; planning/execution-error
records code-class without error-string; timeout/cancel/disconnect record
bounded outcomes; queue-full drops query-event (query still completes);
queue-full drops security-event (+ security-unrecorded metric); local-chain-
open-failure degrades ready when enabled; metrics have no tenant/query/table/
peer/error labels.

## Out of Scope (→ later)

`audit.fail_closed` / wait-for-evidence (deferred high-assurance mode); worker/
observability-listener audit; per-statement data-access audit; stable plan
fingerprints; exact row telemetry; external signed-root / audit query API /
retention policy; `observability::telemetry` / OTLP.

## Council History

iter-0: 3-of-3 topic (065 consumer); authority fork open. iter-1 (dual): Codex
fail-open+metric; Gemini wait-for-evidence; Claude conceded to fail-closed-for-
identity/authz (over-correction). iter-2: Claude withdrew it (audit is
evidence-not-gate; denials don't benefit from blocking) → fail-open+loud-metric;
statement-class-not-fingerprint + row-count-bucket; Gemini conceded from
wait-for-evidence. iter-3: consolidated on Codex's D-set. iter-4: dual; Codex
hardened node-id validation (reject bare-numeric/UUID) + added the roadmap-
fail-closed-text retarget; Gemini aligned. 3-of-3.

## Promotion criteria met

D-set 17, 3-of-3; 21 + 3 convention + meta guards; 065 audit-feature retarget
self-enforcing, roadmap fail-closed-text retarget recorded as planning-document
reconciliation; behavior-preserving when disabled (exact no-op); reuses the 065
substrate unchanged (async lives in Toron); no SQL-wire / Wendelon / telemetry
drift. Migration:
`plans/impl/0066_toron_audit_emission_consumer.md`.

## Conclusion

066 makes Toron the second `observability::audit` consumer — a default-disabled,
privacy-preserving, fail-open audit emitter (lifecycle + edge-denial + query-
outcome) over its own chain, with missed security evidence made loud rather
than blocking. It completes the production-readiness arc opened by 065;
durable-capture (`audit.fail_closed`) and `observability::telemetry` / OTLP are
the next reserved facets.
