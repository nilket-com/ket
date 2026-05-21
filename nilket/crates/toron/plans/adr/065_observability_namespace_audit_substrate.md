# ADR 065: `crates/observability` Namespace and Audit Substrate

**Status:** Promoted (3-of-3 — Claude, Codex, Gemini)
**Seed:** ADR 045 D7 — the oldest substantive deferral, unblocked now that the
DML surface is complete. Wendelon has a mature crypto-chained audit
(`wendelon/src/audit.rs`, 3248 lines, `AUDIT_SCHEMA_VERSION = 4`); Toron has
none. 065 establishes the shared `observability` namespace, migrates the ADR
048 HTTP helper into it, and extracts Wendelon's audit **mechanics** beside it
— behavior-preserving. **Toron audit emission is ADR 066.**
**Category:** Interior shared substrate — `crates/observability` (Toron-
aggregator sequence; the 047 `tls-pki` / 048 `observability-http` precedent).

## Why one ADR (the namespace-boundary decision)

This intentionally bundles "migrate 048 HTTP" + "extract Wendelon audit"
because the decision *is* creating `crates/observability` as a real module
boundary, which needs both proofs: an existing substrate (048 HTTP) retargets
into `observability::http` without drift, **and** a second substrate (Wendelon
audit) lands beside it as `observability::audit` without cross-module coupling
or Toron audit behavior. Splitting them would leave the flat
`observability-http` crate as contrary evidence against the new layout.

## Naming — `observability`, not `telemetry`

Telemetry is *emitted operational signal*; audit is *durable evidence* — audit
must not sit under `telemetry`. The umbrella is `observability`; a future
OTLP/export surface nests as **`observability::telemetry`** (telemetry ⊂
observability). This is also why it's a namespace crate, not a flat
`observability-audit` sibling: it homes `http` + `audit` now and `telemetry`
later. (Distinct from the existing *execution*-telemetry types
`SchedulerTelemetry`/`TaskTelemetry`/`metrics/telemetry.rs`, which are internal
per-task collection.)

## Summary

Create **`crates/observability`** (`default = []`) with non-default
feature-gated modules `observability::http` and `observability::audit`. Move
ADR 048's HTTP helpers into `observability::http` and **remove the flat
`observability-http` crate**. Extract Wendelon's audit **mechanics** (envelope,
hash-chain, `AuditSink`, sink configs, NDJSON sink, archive/syslog/kafka sinks,
verify/replay, signed chain-root) into `observability::audit`; Wendelon's
**typed operations/schema stay ring-owned** as a payload that serializes
through the shared envelope — **byte-equivalent**. No Toron emission, no async
policy, no schema bump.

## Decisions (14, Promotion-Locked)

**D1 Structured namespace.** `crates/observability`, `default = []`,
feature-gated `http` and `audit` modules. Single Interior-Substrate crate.

**D2 Precise feature invariant.** Cargo features unify per-package in a build
graph, so the invariant is: HTTP-only builds work (`cargo check -p
observability --no-default-features --features http`), audit-only builds work
(`… --features audit`), `default = []`, and the `http`/`audit` source modules
**do not reference each other** — *not* "each consumer compiles only its
module."

**D3 ADR 048 HTTP retarget.** Move `crates/observability-http` source +
substrate tests into `crates/observability/src/http/` (+ `tests/`); preserve
parser/writer behavior, bounded header/body limits, small public helper
surface, public helper names, caller-owned timeout posture; retarget all live
consumers and the ADR 048/071 guards.

**D4 Remove the flat HTTP runtime crate.** No compatibility `observability-http`
package remains (one substrate, one filesystem home) unless the ADR records a
temporary exception (it doesn't).

**D5 Shared audit mechanics.** `observability::audit` owns canonical envelope
mechanics, chain hashing, NDJSON local chain sink, archive/syslog/Kafka
projection sinks, sink configs, `AuditSink`, composite-sink failure tracking,
chain replay, verification, and signed chain-root helpers.

**D6 Wendelon payload stays ring-owned.** Wendelon keeps `AuditOperation`,
`AdministrativeControlSchema`, `LocalSecuritySchema`, `DataAccessSchema`,
`AuditSchema`, and `AuditActor` conversion from `ActorIdentity` as
Wendelon-owned typed payload/adapters. **The shared crate must not import
Wendelon `NodeId`, `TenantId`, `Identity`, `ActorIdentity`, metrics, or
clock-drift subscriber traits.**

**D7 Generic typed payload seam.** No free-form operation strings; no shared
`ToronPayload` in 065. The shared envelope is generic over a typed per-ring
payload; Toron's is introduced by ADR 066.

**D8 Consumer-provided genesis tag.** The shared chain code accepts a genesis
tag parameter; Wendelon passes `b"wendelon-audit-genesis-v1"`; Toron may pass
its own later.

**D9 Byte-equivalence.** No change to Wendelon's `AUDIT_SCHEMA_VERSION = 4`,
hash algorithm tags (`blake3-256` / `sha256`), chain-root tag
`wendelon-audit-chain-root-v1`, root signature tag `ed25519`, persisted JSON
field order, verifier issue codes, or report shape. Pinned by Wendelon's
existing audit tests + a pre-extraction golden.

**D10 No Toron audit emission.** No Toron `[audit]` config, audit feature, SQL/
query-outcome/edge-denial/lifecycle hook, async audit task, or audit metric.

**D11 No async policy in the substrate.** `observability::audit` exposes
synchronous mechanics matching Wendelon's current posture. Bounded MPSC /
`try_send` / dropped-event counters / fail-open / authoritative-fail-closed
split are ADR 066 consumer policy.

**D12 Dependency posture.** `observability` depends on no Toron/Wendelon/Quinn/
Raft/SQL-wire/TLS-policy/router types. Kafka feature-gated, off by default.
`crates/nilket` runtime-free; `toron` picks up no audit deps (HTTP retarget
only).

**D13 Local chain authority.** Archive/syslog/Kafka are projections; the local
NDJSON hash chain remains the authoritative node-local substrate for Wendelon's
current events.

**D14 ADR 066 carry.** Toron Audit Emission Consumer over `observability::
audit`: `ToronPayload`, `[audit]` config, event taxonomy, async policy,
fail-open/authority split, aggregate-only metrics.

## Architecture Guards (25 + 3 convention + meta)

`adr065_observability_crate_exists_under_workspace_glob`,
`adr065_observability_default_features_are_empty`,
`adr065_observability_exposes_http_and_audit_modules`,
`adr065_observability_builds_with_only_http_feature`,
`adr065_observability_builds_with_only_audit_feature`,
`adr065_observability_http_does_not_reference_audit`,
`adr065_observability_audit_does_not_reference_http`,
`adr065_observability_http_retargets_adr048_substrate`,
`adr065_observability_http_preserves_limits_and_timeout_boundary`,
`adr065_observability_http_preserves_small_public_surface`,
`adr065_flat_observability_http_runtime_crate_removed`,
`adr065_observability_has_no_toron_or_wendelon_dependency`,
`adr065_observability_has_no_quinn_raft_sql_wire_tls_or_router_dependency`,
`adr065_wendelon_consumes_observability_audit`,
`adr065_wendelon_payload_remains_ring_owned_and_typed`,
`adr065_wendelon_audit_serialization_is_preserved`,
`adr065_wendelon_audit_verify_cli_uses_shared_verifier`,
`adr065_local_ndjson_chain_sink_is_in_observability_audit`,
`adr065_audit_sink_trait_is_in_observability_audit`,
`adr065_genesis_tag_is_consumer_provided_parameter`,
`adr065_kafka_sink_remains_feature_gated`,
`adr065_no_toron_audit_config_or_emission_yet`,
`adr065_no_async_try_send_policy_in_observability_audit`,
`adr065_audit_schema_version_is_preserved`,
`adr065_observability_namespace_reserves_future_telemetry_facet`.
**Convention:** `adr065_nilket_root_stays_runtime_free`,
`adr065_fixture_worker_source_unchanged`,
`adr065_new_surface_is_brand_clean_and_has_no_v1_suffix`.
**Meta:** `adr065_prior_adr045_to_064_guards_continue_to_pass_except_adr048_retarget`.

## Prior-Guard Reconciliation

The **ADR 048 (and 071) guards naming `crates/observability-http` are
retargeted** to `observability::http` (package → module). The meta-guard is
named honestly (`…_except_adr048_retarget`) — 048's old package guard cannot
continue unchanged. Self-enforcing (module-path guard present + flat-crate-gone
guard).

## Integration Fixtures

**048 retarget (behavior-neutral):** request-header-terminator, request
over-limit reject, request-line version parsing, missing-parts reject,
titlecase response headers, content-length body read, chunked-response reject,
response-body over-limit reject, GET writer with host+close. **Audit
substrate:** local-chain append/replay, chain-head-advances-with-prev-hash,
verifier-rejects-tampered/reordered, signed-chain-root-verifies, archive-rolls-
at-segment, syslog-canonical-record, composite-sink-bounds-failures,
audit-builds-without-http. **Wendelon preservation:** pre-extraction-chain-
verifies-after, startup-record-byte-equivalent, ready/metrics-materialize,
local-security-denial-preserved, chain-root-rollup-signs-head, audit-metrics-
increment-through-adapter, audit-bundle-passes. **Toron negative:** no-`[audit]`-
until-066, sql-emits-no-audit, manifest-enables-`http`-only (no audit/kafka).

## Out of Scope (→ ADR 066 / later)

Toron audit emission/config/metrics; query-outcome audit; async emitter +
fail-open/authority split; `observability::telemetry` / OTLP export (reserved
facet); external signed roots beyond Wendelon's current chain-root behavior.

## Council History

iter-0: 3-of-3 topic; scope spread (Claude extract+066 / Codex substrate+
bootstrap / Gemini big-bang). iter-1: Codex + Gemini retracted to extraction-
only + byte-equivalent. iter-2: Claude conceded (byte-equivalent over
leave-untouched) and **folded in the `observability::{http, audit}` namespace**;
Codex accepted it + sharpened the **precise feature invariant** + **flat-crate-
removal** + the namespace-boundary justification. iter-3: Claude reserved
`observability::telemetry`. iter-4/5: dual; Codex grounded the full byte-
equivalence constant set (`blake3-256`/`sha256`, `chain-root-v1`, `ed25519`,
JSON field order, verifier codes) + the Wendelon-type-exclusion. Gemini iter-4
aligned on the namespace/extraction outcome; Claude + Codex tightened the
ring-owned payload cut. Locked.

## Promotion criteria met

D-set 14, 3-of-3; 25 + 3 convention + meta guards; the **adr048 retarget**
(prior-guard reconciliation, meta named `…_except_adr048_retarget`); behavior-
preserving (048 semantics + Wendelon audit byte-equivalence pinned); interior
shared substrate (047/048 precedent); no Toron emission / async / Wendelon-type
leakage / new crate-deps. Migration:
`plans/impl/065_observability_namespace_audit_substrate.md`.

## Conclusion

065 establishes `crates/observability` as the structured home for Nilket's
shared visibility primitives — migrating the 048 HTTP helper into
`observability::http` and extracting Wendelon's crypto-chained audit into
`observability::audit` byte-equivalently — giving both rings one audit
implementation and reserving `observability::telemetry` for future OTLP. Toron
audit emission is the ADR 066 consumer.
