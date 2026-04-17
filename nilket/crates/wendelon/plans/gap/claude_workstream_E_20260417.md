# Workstream E — Security and Compliance

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 5)
**Status:** First pass
**Subsystems:** `src/network/tls.rs`, `src/network/control_server.rs`, `src/network/server.rs`, `src/network/lease_signing.rs`, `src/metrics/exporter.rs`
**Depends on:** Workstream A (integration gate); ADR for authorization model must precede authz implementation

---

## Problem Statement

Beyond encryption at rest (Workstream D), the security and compliance
posture has several gaps that would fail a Fortune 500 security review:

1. **No authorization on Control Ring RPCs.** Any node with a valid mTLS
   cert can invoke `advance_epoch`, `transfer_leadership`,
   `acquire_lease`. No distinction between operator and data-node roles.
2. **No audit log.** Admin actions pass through Raft but produce no
   consumable audit stream for SIEM integration.
3. **No hot cert rotation.** `TlsMaterial::load` is one-shot. Cert
   renewal requires a full cluster restart.
4. **No Ed25519 key rotation.** Trusted signer map is frozen at boot. No
   key-id field for overlap during rotation.
5. **No FIPS mode.** rustls uses `ring`, not a FIPS-validated crypto
   backend.
6. **Unauthenticated metrics endpoint.** Plaintext HTTP, no auth, no
   request-size cap.
7. **No rate limiting.** Malformed HRFT frame floods just close
   connections — no ban list or rate counter.
8. **No secret redaction** in tracing spans.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Authorization model | `src/network/control_server.rs`, new `src/auth/` | ADR-first |
| Audit log | New `src/audit/` or extension of existing Raft apply path | |
| TLS lifecycle | `src/network/tls.rs` | Hot reload |
| Key rotation | `src/network/lease_signing.rs`, `src/network/transfer_signing.rs` | Key-id field |
| FIPS | `Cargo.toml`, `src/network/tls.rs` | Feature flag |
| Metrics hardening | `src/metrics/exporter.rs` | Auth, TLS, request limits |
| Rate limiting | `src/network/server.rs` | Per-source tracking |
| Secret redaction | Tracing layer configuration | |

---

## Ordered Implementation Steps

### E.0 — Authorization ADR (blocks E.1)

| # | Step | Details |
|---|------|---------|
| E.0.1 | Write ADR: authorization model | Options: (a) certificate OID extension encoding role (operator vs. data-node vs. read-only), (b) separate operator CA distinct from data-node CA, (c) token-based authz alongside mTLS. Evaluate operational ergonomics of each — cert OID is simplest but requires re-issuance on role change; separate CA is clean but doubles cert management; tokens are flexible but add a new credential type. |
| E.0.2 | Review ADR with architect | |

### E.1 — Control Ring Authorization

| # | Step | Target | Details |
|---|------|--------|---------|
| E.1.1 | Extract role from peer identity | `src/network/tls.rs` or `src/auth/` | Based on ADR decision: parse OID, check issuer CA, or validate token. |
| E.1.2 | Enforce role checks on control handlers | `src/network/control_server.rs` | `advance_epoch`, `transfer_leadership`, `acquire_lease`, `join` — each requires a minimum role level. Data-node certs can replicate data but cannot perform admin operations. |
| E.1.3 | Test: data-node cert rejected for admin RPC | `tests/` | A node with a data-node identity cannot invoke `transfer_leadership`. |
| E.1.4 | Test: operator cert accepted for admin RPC | `tests/` | An operator identity can invoke all admin RPCs. |

### E.2 — Audit Log

| # | Step | Target | Details |
|---|------|--------|---------|
| E.2.1 | Define audit event schema | `src/audit/` (new) | Structured NDJSON. Fields: timestamp, node_id, raft_log_index, event_type, actor (node/operator identity), detail. |
| E.2.2 | Emit audit events from Raft apply path | `src/consensus/raft_storage.rs` or state machine apply | On lease grant, membership change, epoch advance, leadership transfer, backup export. |
| E.2.3 | Audit log writer | `src/audit/` | Append-only file per node. Rotate on size threshold. Optional: forward to syslog/SIEM endpoint. |
| E.2.4 | Test: audit log captures membership change | `tests/` | Add a node, verify audit log contains the event with correct fields. |

### E.3 — Certificate Lifecycle

| # | Step | Target | Details |
|---|------|--------|---------|
| E.3.1 | Hot cert reload via file watcher or SIGHUP | `src/network/tls.rs` | Replace the one-shot `TlsMaterial::load` with a reloadable `Arc<ArcSwap<TlsMaterial>>`. On signal or file change, reload and swap. quinn's `ServerConfig` supports runtime cert updates via `Arc<dyn ServerCryptoProvider>`. |
| E.3.2 | Test: cert rotation without restart | `tests/` | Start node with cert A, rotate to cert B while running, verify new connections use cert B. |

### E.4 — Ed25519 Key Rotation

| # | Step | Target | Details |
|---|------|--------|---------|
| E.4.1 | Add `key_id` field to `LeaseDescriptor` and `TransferToken` | `src/network/lease_signing.rs`, `src/network/transfer_signing.rs` | Include key_id in signed payload so verifiers know which key to check. |
| E.4.2 | Support multiple active keys in trusted-signer map | `src/config.rs`, signing modules | Map `(node_id, key_id) -> public_key`. Allow overlap window during rotation. |
| E.4.3 | Key rotation procedure | Documentation | Document the steps: add new key to config, deploy, wait for all nodes to see new key, remove old key. |

### E.5 — FIPS Mode

| # | Step | Target | Details |
|---|------|--------|---------|
| E.5.1 | Add `fips` feature flag to `Cargo.toml` | `Cargo.toml` | When enabled, use `aws-lc-rs` as the rustls crypto provider instead of `ring`. |
| E.5.2 | Conditional crypto provider selection | `src/network/tls.rs` | `#[cfg(feature = "fips")]` to select provider. |
| E.5.3 | CI job: build with `--features fips` | CI config | Ensure FIPS build compiles and tests pass. |

### E.6 — Hardening

| # | Step | Target | Details |
|---|------|--------|---------|
| E.6.1 | Metrics endpoint auth + TLS | `src/metrics/exporter.rs` | Add optional basic auth or mTLS. Add request-size cap and timeout. |
| E.6.2 | Rate limiting for malformed frames | `src/network/server.rs` | Per-source-IP counter for failed handshakes and malformed frames. Temporary ban after threshold. |
| E.6.3 | Secret redaction in tracing | Tracing config | Add a tracing layer that redacts fields marked as sensitive (payload contents, key material). |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| Data-node cert rejected for admin RPC | Integration | Authorization enforcement |
| Operator cert accepted for admin RPC | Integration | Authorization allows legitimate access |
| Audit log captures membership change | Integration | Audit pipeline end-to-end |
| Cert rotation without restart | Integration | Hot reload works |
| Key rotation overlap window | Unit | Old and new keys both valid during transition |
| FIPS build compiles | CI | Feature flag doesn't break the build |
| Rate limit bans flood source | Unit | Malformed-frame counter triggers ban |

---

## Rollout and Migration Concerns

- **Authorization rollout.** Enabling authz on an existing cluster
  requires all nodes to have the new cert/role configuration before
  enforcement begins. Recommend a "log-only" mode first (log unauthorized
  attempts but don't reject) for a burn-in period.
- **Audit log storage.** NDJSON files will grow. Need rotation policy and
  guidance on retention (how long, where to ship). This is operational
  guidance, not code.
- **FIPS build dependency.** `aws-lc-rs` requires cmake and a C compiler.
  CI must have these available for the FIPS build job.

---

## Failure Modes and Rollback

- **Authorization misconfiguration.** If all nodes are configured as
  data-node role, no one can perform admin operations. The "log-only"
  mode and a clear error message ("no operator identity found") mitigate
  this. Emergency bypass: a CLI flag to disable authz (clearly marked as
  insecure, for recovery only).
- **Hot cert reload with invalid cert.** If the new cert file is corrupt,
  the reload should fail gracefully (keep the old cert, log error). Never
  leave the node with no valid cert.
- **Audit log disk full.** If the audit log partition fills up, the node
  should not crash. Configurable behavior: drop audit events (log
  warning), or enter read-only mode (strict compliance).

---

## Exit Criteria

- An operator cert can perform admin RPCs; a data-node cert cannot.
- Audit log captures all membership changes and lease grants.
- Cert rotation succeeds without cluster restart.
- FIPS build compiles and passes tests.
- Metrics endpoint is not accessible without auth.

---

## Open Questions

1. **Authorization mechanism:** Certificate OID extension vs. separate CA
   vs. token-based? This is the most impactful design decision in this
   workstream. Needs ADR.
2. **Audit log format and destination:** NDJSON file is the minimum. Should
   we also support direct syslog/SIEM forwarding from the start, or defer
   that?
3. **Audit log tamper-evidence:** Should audit records include a hash
   chain (each record includes hash of previous) for tamper detection?
   Adds complexity but strengthens compliance posture.
4. **FIPS certification scope:** Is the goal just "builds with a
   FIPS-validated crypto module" or full FIPS 140-3 certification of the
   Wendelon binary? The former is a code change; the latter is a
   multi-month lab process.
5. **Rate limiting granularity:** Per-IP, per-node-id, or per-connection?
   Per-IP is simplest but doesn't help if a rogue node has a valid cert
   and a single IP.
