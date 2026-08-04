# Impl Plan 065: `crates/observability` namespace + audit substrate

**Status:** Implemented, paired with
`plans/adr/0065_observability_namespace_audit_substrate.md`.
Evidence: `plans/impl/0065_observability_namespace_audit_substrate_evidence.md`.
**Posture:** Establish `crates/observability` (interior shared substrate);
migrate ADR 048 HTTP → `observability::http` (remove the flat crate); extract
Wendelon's audit **mechanics** → `observability::audit` **byte-equivalently**.
**No Toron emission, no async policy, no schema bump.** The only behavioral
risk surface is Wendelon's audit serialization — fully pinned by byte-
equivalence fixtures. Seven phases.

## Invariants carried in

- Wendelon audit output (records, chain, verify-CLI, rollup, readiness,
  metrics) **unchanged**; ADR 048 HTTP semantics unchanged; only the adr048
  (+071) crate/path guards retargeted. `crates/nilket` runtime-free; fixture
  worker unchanged; no-`*V1`; no Toron audit runtime deps.

## Phase 0 — Audit (verified anchors)

- **048 crate:** `crates/observability-http` (`tokio` `io-util` only).
  Consumers: wendelon `cli.rs`; toron `network/observability.rs`,
  `network/observability_http.rs`, `cli.rs`, `network/mod.rs`.
- **Wendelon audit:** `crates/wendelon/src/audit.rs` (**3248 lines**),
  `AUDIT_SCHEMA_VERSION = 4` (:25), `AUDIT_GENESIS_TAG = b"wendelon-audit-
  genesis-v1"` (:26). Consumed by wendelon `config/cli/node/sweeper/metrics/
  observability/crypto/cert_issuance_policy/tls/control_server/data_server`.
- **The cut.** *Shared mechanics → `observability::audit`:* `AuditSinksConfig`/
  `ArchiveAuditConfig`/`SyslogAuditConfig`/`KafkaAuditConfig`, `AuditAuthority`,
  `AuditOutcome`, `AuditProjectionSinkKind`, `AuditEvidenceDisposition`,
  consumer-provided `AuditFormat`, generic `AuditEvent` envelope + generic
  `AuditActor`, `AuditSink` trait, chaining (prev-hash, format-provided
  constants), NDJSON sink, archive/syslog/kafka sinks, replay + verify +
  signed chain-root helpers, composite-sink failure tracking.
  *Wendelon-owned (stays; typed payload through the envelope):*
  `AuditOperation`, `AdministrativeControlSchema`/`LocalSecuritySchema`/
  `DataAccessSchema`/`AuditSchema`, `AuditActor::from_actor_identity`
  (`ActorIdentity`).
- **Byte-equivalence constants to preserve** (D9): `AUDIT_SCHEMA_VERSION=4`,
  hash tags `blake3-256`/`sha256`, chain-root tag `wendelon-audit-chain-root-v1`,
  root-sig tag `ed25519`, persisted JSON field order, verifier issue codes,
  report shape.

## Phase 1 — Create `crates/observability`

- New crate; `default = []`; features `http`, `audit`, and optional
  `kafka` (off-default). Archive/syslog stay under `audit` because they are
  existing Wendelon audit behavior, not new user-visible Cargo feature gates.
  `lib.rs`:
  `#[cfg(feature="http")] pub mod http;` / `#[cfg(feature="audit")] pub mod
  audit;`. **`audit` must not require `http`.** Auto-included by `crates/*`.

## Phase 2 — Migrate ADR 048 → `observability::http`; remove flat crate

- Move `observability-http` source + tests into `crates/observability/src/
  http/` (+ `tests/`); preserve parser/writer, bounded limits, helper names,
  small public surface, caller-timeout. Update consumers (`observability_http::… →
  observability::http::…`) to `observability` `features=["http"]`. **Delete
  `crates/observability-http`.**

## Phase 3 — Extract audit mechanics → `observability::audit`

- Move the shared-mechanics set (Phase 0). Chaining, signed-root emission,
  syslog formatting, verification, and replay take an **`AuditFormat` /
  consumer-profile parameter** (no `wendelon-…` constants in the shared
  crate). For Wendelon this profile carries schema version 4, the genesis tag,
  chain-root tag, signature tag, canonical operation labels, syslog
  identifiers, and the ring's node-id codec. The `AuditEvent` envelope is
  generic over `P: AuditPayload`; operation/schema data stay typed at the
  consumer boundary, with no free-form operation-string escape hatch in the
  Wendelon adapter. **The shared crate imports none of:** Wendelon
  `NodeId`/`TenantId`/`Identity`/`ActorIdentity`, metrics, clock-drift
  subscriber traits (D6).

## Phase 4 — Wendelon first consumer (byte-equivalent)

- Wendelon keeps `AuditOperation`/`*Schema`/`AuditActor::from_actor_identity`
  as a **`WendelonPayload: AuditPayload`** — payload serialization moves
  **verbatim** (relocated, not rewritten). Wendelon provides the `AuditFormat`
  carrying `b"wendelon-audit-genesis-v1"`, `AUDIT_SCHEMA_VERSION=4`,
  `wendelon-audit-chain-root-v1`, `ed25519`, the existing operation labels,
  syslog identifiers, and node-id conversion. Update wendelon
  `crate::audit::…` imports → `observability::audit::…` (mechanics) + local
  `WendelonPayload` (schema); preserve the metrics adapter and keep the
  existing Wendelon `audit-kafka` Cargo feature name as the consumer-facing
  feature that enables `observability/kafka`. **Serialized envelope+payload
  bytes identical** pre/post; the D9 constants unchanged.

## Phase 5 — Toron (HTTP retarget only)

- Toron gets `observability` `features=["http"]` only. **No `audit` feature,
  no `[audit]` config, no emission.** Add the Toron negative guards/fixtures.

## Phase 6 — Guards + retargets + fixtures + evidence

- The ADR guard set, incl. **per-feature `cargo check`** (`--no-default-
  features --features http` / `… audit`), **no-cross-module-reference**
  (audit↛http, http↛audit), **ADR 048 small-public-surface preserved**,
  **flat-crate-removed**, **consumer-audit-format-param**,
  **Wendelon-payload-ring-owned**,
  byte-equivalence, mechanics-only, no-Toron-emission, no-async-substrate,
  no-Wendelon-type-leak, schema-preserved, `::telemetry`-reserved.
- **Retarget the adr048 (+071) `observability-http` crate/path guards** →
  `observability::http`; meta `…_except_adr048_retarget` (named honestly);
  self-enforcing (module-path present + flat-crate-gone).
- Fixtures (ADR list): 048-retarget (after_retarget), audit-substrate,
  **Wendelon byte-equivalence** (incl. `verify_cli_accepts_pre_extraction_
  fixture`, `chain_root_rollup_signs_head`, `metrics_increment_through_
  adapter`), Toron-negative.
- `plans/impl/0065_observability_namespace_audit_substrate_evidence.md`:
  `architecture_test adr065`, full `architecture_test` (toron + wendelon),
  `cargo test -p observability -p wendelon -p toron`, the per-feature `cargo
  check`s, `clippy … -D warnings`, `git diff --check` (capped-linker).

## Phase 7 — Roadmap

- 065 promoted — `observability` namespace established (`http` migrated,
  `audit` extracted byte-equivalently, `::telemetry` reserved); ADR 066 = Toron
  audit emission consumer.

## Watch-items

1. **Byte-equivalence is the whole risk** — preserve the full D9 constant set
   (schema-v4, `blake3-256`/`sha256`, `chain-root-v1`, `ed25519`, JSON field
   order, verifier codes); payload serialization moves **verbatim**; pin with
   existing Wendelon audit tests + a pre-extraction golden. **No schema bump.**
2. **Audit format = consumer param** (not shared Wendelon constants): genesis
   tag, schema version, chain-root/signature tags, canonical operation labels,
   syslog identifiers, and node-id codec.
3. **No Wendelon-type leakage** into the shared crate (NodeId/TenantId/Identity/
   ActorIdentity/metrics/clock-drift) — D6 guarded.
4. **Precise feature invariant** — per-feature `cargo check` + no cross-module
   references; `default=[]`; `audit ⊀ http`.
5. **Remove the flat `observability-http` crate** — one home.
6. **adr048 (+071) retarget** — the only prior-guard change; meta named
   `…_except_adr048_retarget`.
7. **Mechanics shared; Wendelon schema/operations ring-owned typed payload.**
8. **No Toron emission/config/async/metrics** (→ 066); `::telemetry` reserved
   (no OTLP here).

## Acceptance checklist

- [x] Phase 1: `crates/observability`, `default=[]`, feature-gated http/audit
      (audit ⊀ http).
- [x] Phase 2: 048 → `observability::http`; consumers migrated; flat crate
      removed; semantics preserved.
- [x] Phase 3: audit mechanics → `observability::audit`; consumer
      `AuditFormat` param; generic `AuditPayload` envelope; no Wendelon-type
      imports.
- [x] Phase 4: Wendelon `WendelonPayload` (verbatim) first consumer;
      **byte-equivalent** (D9 constants; metrics adapter; no bump).
- [x] Phase 5: Toron `http` only; no audit feature/config/emission.
- [x] Phase 6: guard set incl. per-feature cargo-check, no-cross-reference,
      flat-crate-removed, audit-format-param, no-Wendelon-type-leak; adr048
      retarget; byte-equivalence fixtures; evidence.
- [x] Phase 7: roadmap; 066 carry.
