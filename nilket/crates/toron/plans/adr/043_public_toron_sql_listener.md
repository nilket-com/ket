# ADR 043: Public Toron SQL Listener

**Status:** Promoted
**Seed:** ADR 042 reservation pattern for `nilket toron sql`; ADR 042 daemon substrate (loopback worker daemon, shared `Arc<WendelonDescriptorRegistry>`, `READY <worker-task-endpoint>` line); ADR 040 reservation-lift discipline.
**Council:** Claude, Codex, Gemini (4 iterations: iter-0 topic-shape split survey, iter-1 Codex spine + Claude four-retraction convergence, iter-2 Claude two retractions + 2-of-3 resolutions, iter-3 final triple ratification)
**Category:** Substrate ADR (operator-surface continuation; lifts the second ADR 040 reservation after ADR 042 lifted `start`)

## Summary

Lift the ADR 040 `nilket toron sql` reservation. Add `crates/toron/src/sql_wire.rs` (custom framed product types: `SqlFrame` / `SqlMessage::{Execute, Cancel, ResultBatch, Complete, Error}` — length-prefixed postcard via the ADR 039 `wire` crate, no `*V1` suffix). Extend `crates/toron/src/config.rs` with `[sql]` and `[coordinator]` sections; non-loopback SQL bind addresses fail validation at config-load time with a stable diagnostic mirroring ADR 042 D9. Extend `crates/toron/src/daemon.rs` to bind a loopback QUIC SQL listener, close ADR 042's empty-provider execution gap through the D18 Plan A / Plan B Wendelon access path, construct a `DistributedQueryCoordinator` with `StaticWorkerRegistry` from config, and route incoming SQL through coordinator → QUIC task dispatch → worker → Arrow IPC result frames. Wire `crates/toron/src/cli.rs`'s `sql` arm to a one-shot client (`nilket toron sql --endpoint <addr> "<statement>"`) that dials the daemon over QUIC, sends one `Execute` frame, receives result frames, renders Arrow batches as a pretty table or `OK rows=<n>` shape for non-SELECT. Emit `READY <worker-task-endpoint>` byte-identical to ADR 042, append a second `SQL_READY <sql-endpoint>` line. Reserve `nilket toron inspect` byte-identically.

**No** remote mTLS / token-auth policy. **No** non-loopback SQL listener. **No** non-loopback static worker endpoint. **No** multi-tenant client claims. **No** prepared statements / transactions / persistent sessions / pipelining. **No** Wendelon descriptor discovery / refresh. **No** observability listeners. **No** gRPC / Arrow Flight / Postgres wire frontend. **No** `WendelonNode` in-process. **No** `SELECT without FROM` SQL-surface widening. Those are follow-up ADRs.

## Motivation

ADR 042 lifted the first ADR 040 reservation (`start`) into a loopback worker daemon, but the daemon has no operator-visible service consumer — every SELECT against Wendelon currently goes through the in-process test path, not through any wire surface. ADR 043 lifts the second reservation (`sql`) into a real SQL listener, making the ADR 042 daemon useful as more than a task-wire smoke test target.

Critically, the SQL path goes through `DistributedQueryCoordinator` rather than a local-coordinator bypass. This makes the ADR 042 task listener **load-bearing**: a SQL `SELECT` causes the daemon to dispatch a task to itself (via `include_self_worker = true`) or to a sibling loopback worker, exercising the same multi-process execution mechanics the V1 target text requires. ADR 042 shipped the wire substrate; ADR 043 makes that wire substrate observable through the operator-facing SQL surface.

ADR 043 also closes the ADR 042 known gap flagged in code review (`providers: HashMap::new()` couldn't actually execute Wendelon-backed scans). Real `wendelon_client::Client::connect_quic` (Plan A) or a sanctioned in-process Wendelon test harness with a named QUIC provider follow-up (Plan B) is selected per the Phase 0 audit.

## Decisions (20, Promotion-Locked)

**D1: Lift only `nilket toron sql`.**
`nilket toron sql <statement...>` becomes a real client. `nilket toron inspect` remains reserved with **byte-identical ADR 040 diagnostic text**. Root `nilket sql` remains rejected by the nilket root dispatcher (ADR 040 D3 / guard 4 preserved).

**D2: The CLI is a client, not an in-process query runner.**
`nilket toron sql` constructs a Tokio runtime inside Toron, dials a Toron SQL endpoint, sends a framed SQL wire request, receives Arrow IPC result frames or structured error frames, renders stdout, and exits. It must not construct `QueryCoordinator`, `DistributedQueryCoordinator`, Wendelon clients, catalogs, storage adapters, or descriptor registries in the nilket process.

**D3: Endpoint selection stays in Toron.**
Supported client endpoint selection (precedence in order: flag > env > compiled default matching the generated config):
- `nilket toron sql --endpoint <socket-addr> <statement...>`
- `TORON_SQL_ENDPOINT=<socket-addr> nilket toron sql <statement...>`
- default `127.0.0.1:7742` (from the generated example config; bare socket endpoints use the default QUIC transport)

Parsing stays in `crates/toron/src/cli.rs`; `crates/nilket` stays runtime-free.

**D4: Add `[sql]` config section; preserve ADR 042 backward compatibility.**
Missing `[sql]` section means **worker-only ADR 042 behavior unchanged**. The generated example config includes `[sql]` enabled:

```toml
[sql]
transport = "quic"
listener_bind = "127.0.0.1:7742"
statement_timeout_seconds = 60
```

Tests bind `127.0.0.1:0` and read the resolved address from `SQL_READY` line or the daemon handle.

**D5: SQL listener is loopback-only.**
Config validation rejects non-loopback SQL bind addresses with the stable diagnostic:

```text
toron sql listener requires authenticated transport for non-loopback bind address
```

**No inert fallback.** No hostname resolution. Same security posture as ADR 042's task listener (D9).

**D6: Preserve ADR 042 task-listener security posture.**
ADR 043 must not weaken ADR 042. QUIC is the default task transport for ADR 043's SQL-driven distributed query path. ADR 042's plain `TcpTransport` remains a loopback-only legacy/test compatibility seam for pre-ADR043 worker-wire fixtures, not the forward default. Non-loopback worker task endpoints and remote-auth policy are deferred to ADR 044.

**D7: Define a Toron SQL wire module — custom framed, postcard-encoded, Arrow IPC payloads.**
Add `crates/toron/src/sql_wire.rs` with no `*V1` suffix. Length-prefixed postcard frames ride on QUIC streams and mirror Toron's existing task wire payload style:

```rust
pub struct SqlFrame {
    pub protocol_version: SqlProtocolVersion,
    pub query_id: QueryId,
    pub message: SqlMessage,
}

pub enum SqlMessage {
    Execute(ExecuteRequest),
    Cancel(CancelRequest),
    ResultBatch(ResultBatch),
    Complete(Complete),
    Error(SqlError),
}
```

`ResultBatch` carries Arrow IPC stream bytes (via `serde_bytes` or equivalent); never stringified rows. `Complete` carries `row_count: u64` for DDL/DML statements that don't return data.

`CancelRequest` carries the target `query_id: QueryId`; the enclosing `SqlFrame::query_id` remains the frame/query correlation id. Stable error codes include `SyntaxError`, `PlanningError`, `BindingError`, `CatalogError`, `StorageError`, `TenantMismatch`, `StatementTimeout`, `Cancelled`, `CancellationUnsupported`, `NoLiveWorker`, and `Internal`.

**D8: CLI text rendering is stable but not the wire contract.**
The daemon returns Arrow IPC frames. The CLI renders those batches with a stable table formatter (likely `arrow-cast::pretty::print_batches` for SELECT; `OK rows=<n>` or `OK` shape for non-SELECT). The renderer lives in Toron, not nilket root. **The wire contract is Arrow IPC + structured frames; the stdout rendering is a CLI concern.**

**D9: Server-stamped tenant and session.**
V1 SQL requests do **not** accept arbitrary client tenant claims. The daemon stamps `tenant_id` from `[node].tenant_id` and creates a per-statement `session_id` and `query_id`. Multi-tenant routing requires authenticated transport with tenant claims and is deferred to ADR 045+.

**D10: Query admission lives in the daemon and uses one registry.**
The SQL listener constructs or owns a daemon-local query service that uses the same ADR 042 Wendelon-backed adapters and the **same single `Arc<WendelonDescriptorRegistry>`**. No second registry. No fixture storage. No Wendelon server internals. Extends ADR 042 D7 invariant.

**D11: Use `DistributedQueryCoordinator` for daemon SQL.**
ADR 043 exercises the compute daemon shape, not a local coordinator bypass. The daemon builds a `StaticWorkerRegistry` from:

- the self worker endpoint resolved by ADR 042 startup, when `include_self_worker = true` (default);
- optional loopback static workers from `[[coordinator.static_workers]]`.

This makes the ADR 042 task listener **load-bearing** for SQL: a SQL `SELECT` causes the daemon to dispatch a task to itself or a sibling worker via the Toron task wire protocol. ADR 043's default dispatch transport is QUIC; plain TCP remains compatibility-only.

**D12: Add `[coordinator]` config section — meaningful only now.**
ADR 042 deferred `[coordinator]` because a static registry was inert without query admission. ADR 043 makes it meaningful:

```toml
[coordinator]
include_self_worker = true

[[coordinator.static_workers]]
worker_id = "00000000-0000-0000-0000-000000000043"
endpoint = "127.0.0.1:9101"
```

Non-loopback static worker endpoints remain rejected at validation time until authenticated remote QUIC policy exists (ADR 044+). Gossip, membership, and placement remain out of scope.

**D13: Wendelon access becomes execution-capable for SQL-enabled daemon mode.**
ADR 042's empty-provider Wendelon client is acceptable for worker-wire tests (`CancelTask` etc.) but cannot execute table scans. ADR 043 must not leave SQL execution on `providers: HashMap::new()`: Plan A introduces production Wendelon client construction via `wendelon_client::Client::connect_quic` (or equivalent), driven from `[storage.wendelon]` cert/key/ca/cluster_domain config; Plan B introduces a sanctioned in-process Wendelon harness with a named QUIC provider follow-up. **Closes the ADR 042 known gap.**

Direct daemon unit tests may retain a static no-dial client seam for wire-only tests, but any SQL execution fixture claiming real table execution must run with a Wendelon-capable client or sanctioned Wendelon test harness.

**D14: Descriptor bootstrap remains startup-only.**
No Wendelon descriptor discovery / list-descriptors RPC / refresh loop / watch loop. SQL executes against the ADR 042 `ToronDescriptorBootstrap` registry loaded at startup. Stale descriptors fail through existing descriptor evolution checks.

**D15: Cancellation is protocol-shape + best-effort in V1.**
The wire includes `Cancel(CancelRequest { query_id: <target> })`. The daemon records in-flight query ids and routes cancellation through whatever scheduler/worker cancel seams exist. If fine-grained cancellation is not available through all execution paths, the daemon returns a stable `SqlErrorCode::Cancelled` or `SqlErrorCode::CancellationUnsupported`. Strong cross-worker in-flight cancellation is a follow-up runtime ADR.

**D16: Readiness line compatibility.**
Preserve ADR 042's first stdout readiness line exactly:

```text
READY <worker-task-endpoint>
```

If `[sql]` binds, emit a **second stdout line**:

```text
SQL_READY <sql-endpoint>
```

External supervisors parsing the ADR 042 line continue to work unchanged.

**D17: One statement per CLI invocation.**
ADR 043 supports one SQL statement per `nilket toron sql` invocation. No multi-statement sessions, prepared statements, pipelining, or transaction state on the CLI path. The wire types admit extension to persistent sessions later (extra `SqlMessage` variants), but V1 is one-shot.

**D18: Wendelon transport provider Plan A / Plan B.**
Codex D13 mandates a QUIC-capable Wendelon access path. The exact provider shape depends on `wendelon_client` production-readiness:

- **Plan A (preferred):** real `QuicIoUring` provider wired from `[storage.wendelon]` TLS material; SQL fixture executes end-to-end against a real Wendelon test cluster.
- **Plan B (correctness-preserving fallback):** if `QuicIoUring` provider construction has unresolved seams that would force ADR 043 beyond its 4-5 iteration budget, ship a sanctioned in-process Wendelon test harness for loopback SQL fixtures. Defer QuicIoUring wiring to a named follow-up cycle.

Decision rule lives in Phase 0 of the impl plan: audit `wendelon_client::Client::connect_quic` (or equivalent constructor) for production-readiness; if mature, Plan A; if seams remain, Plan B + a named follow-up ADR.

**D19: SQL wire encoding reuses ADR 039 `wire` crate conventions.**
ADR 039's `TableSchemaDropRequest::V1` wire types use a shared `wire::encode` / `wire::decode` postcard-based encoding. ADR 043's `SqlFrame` and related types use the **same encoding crate and conventions** — no new serialization framework introduced.

**D20: DDL/DML result shape pinned.**
- `SELECT ...` → daemon emits `SqlMessage::ResultBatch` frames (one or more), then `SqlMessage::Complete { row_count }`.
- `CREATE TABLE`, `DROP TABLE`, `CREATE TABLE AS SELECT`, `CREATE OR REPLACE TABLE`, `CREATE TABLE IF NOT EXISTS` → daemon emits `SqlMessage::Complete { row_count }` (with `row_count = 0` for DDL).
- `INSERT INTO ... VALUES`, `INSERT INTO ... SELECT`, `DELETE FROM ... WHERE ...` → daemon emits `SqlMessage::Complete { row_count }` with `row_count = <affected rows>`.
- CLI renders `Complete { row_count: 0 }` as `OK`; `Complete { row_count: n > 0 }` as `OK rows=<n>`; result batches as a pretty table followed by `(<n> rows)` footer.

Deterministic across all statement types. No statement-specific prose in multiple places.

## Architecture Guards (21, Promotion-Locked)

Adopt the converged 21-guard set:

1. `adr043_toron_sql_is_not_reserved`
2. `adr043_toron_inspect_remains_reserved`
3. `adr043_nilket_root_sql_stays_rejected`
4. `adr043_nilket_root_stays_runtime_free`
5. `adr043_toron_cli_sql_is_client_only`
6. `adr043_sql_listener_config_surface_exists`
7. `adr043_sql_listener_is_loopback_only`
8. `adr043_task_transport_is_quic_by_default_and_loopback_only`
9. `adr043_sql_wire_uses_length_prefixed_postcard`
10. `adr043_sql_wire_uses_arrow_ipc_not_row_strings`
11. `adr043_sql_wire_types_have_no_v1_suffix`
12. `adr043_no_grpc_protobuf_or_arrow_flight`
13. `adr043_no_remote_sql_auth_policy`
14. `adr043_daemon_uses_single_descriptor_registry_for_sql`
15. `adr043_sql_uses_distributed_coordinator_not_local_bypass`
16. `adr043_static_worker_registry_config_is_coordinator_scoped`
17. `adr043_no_wendelon_descriptor_refresh`
18. `adr043_wendelon_client_has_real_provider_mode_for_sql`
19. `adr043_no_fixture_storage_in_production_sql`
20. `adr043_sql_ready_does_not_mutate_adr042_ready_line`
21. **`adr043_sql_wire_uses_existing_wire_crate`** — `sql_wire.rs` reuses the shared `wire::encode` / `wire::decode` conventions from ADR 039.

Post-promotion convention guard:

- **`adr043_new_sql_protocol_names_are_brand_clean`** — ADR 043's new SQL/QUIC protocol, wire, and config type identifiers do not introduce brand-prefixed `ToronSql*`, `ToronQuic*`, or similar project-name protocol names. Operator command names (`nilket toron sql`, `TORON_SQL_ENDPOINT`, readiness text) remain product surface, not internal protocol identifiers.

## Integration Fixtures (14 required + 4 optional V1-evidence)

Adopt Codex iter-1's fixture set with one addition (DDL/DML result-shape coverage from D20).

**Required (gates promotion):**

1. `nilket_toron_sql_requires_statement` — missing statement → non-zero exit + stable diagnostic.
2. `nilket_toron_sql_dials_endpoint_from_flag` — `--endpoint` overrides env + config.
3. `nilket_toron_sql_dials_endpoint_from_env` — `TORON_SQL_ENDPOINT` overrides config.
4. `nilket_toron_sql_reports_connection_failure` — unreachable endpoint → stable error message + non-zero exit.
5. `nilket_toron_inspect_still_reserved` — byte-identical ADR 040 diagnostic continuity.
6. `nilket_root_sql_still_rejected` — `nilket sql ...` rejected by nilket root.
7. `toron_sql_listener_rejects_non_loopback_bind` — config with non-loopback SQL bind fails validation.
8. `toron_daemon_prints_sql_ready_endpoint` — daemon emits both `READY <worker>` and `SQL_READY <sql>` on stdout.
9. `toron_sql_wire_round_trips_arrow_ipc_batch` — daemon-side wire encoding round-trips through `wire::encode` / `wire::decode`.
10. `toron_sql_syntax_error_emits_stable_error_code` — malformed SQL → `SqlError { code: SyntaxError, ... }`.
11. **`toron_sql_select_from_bootstrapped_descriptor_executes_over_wire`** — daemon with `descriptor_bootstrap_path` + sanctioned Wendelon test harness; SQL `SELECT * FROM <bootstrapped_table>` over the QUIC SQL listener returns Arrow batches matching expected rows. **Wire-correctness witness.**
12. `toron_sql_uses_self_worker_registry_entry` — daemon with `include_self_worker = true`; SQL submission dispatches a task back to the self worker; observable via tracing.
13. `toron_daemon_without_sql_section_preserves_worker_only_start` — ADR 042 backward compat: config missing `[sql]` boots a worker-only daemon successfully.
14. **`toron_sql_complete_frame_carries_stable_row_count`** — submit CREATE TABLE, INSERT, DELETE, DROP TABLE; daemon emits `Complete { row_count }` with stable values; CLI renders `OK rows=<n>` / `OK` shape per D20.

**Optional V1-evidence (non-gating, post-promotion):**

- `toron_sql_insert_then_select_against_wendelon` — full INSERT → SELECT round-trip against real Wendelon connectivity (Plan A only; gated on Phase 0 audit).
- `toron_sql_cancel_request_is_accepted` — `Cancel { query_id }` is accepted and emits stable `Cancelled` or `CancellationUnsupported` error frame.
- `toron_sql_static_loopback_worker_dispatches_remote_task` — two-daemon test: `[[coordinator.static_workers]]` entry routes a task to a sibling loopback worker daemon over QUIC.
- `toron_sql_worker_sigkill_retry_digest_is_canonical` — the V1 target signal, now reachable through SQL submission. Same post-promotion bucket as ADR 042's V1-evidence fixtures.

## Sub-Lift Posture

**Plan A / Plan B (D18) — single-axis sub-lift.** Decision lives in Phase 0 audit of `wendelon_client::Client::connect_quic` production-readiness. The rest of ADR 043 ships unchanged either way; only fixture 11's witness mechanism differs (real Wendelon test cluster under Plan A vs sanctioned in-process Wendelon test harness under Plan B).

## Out of Scope (Recorded for Future ADRs)

- **Authenticated remote QUIC policy** — remote bind admission, peer identity, and tenant claims for both worker and SQL listeners. **ADR 044 lead candidate.**
- **mTLS / token auth policy for SQL listener** — bundled with ADR 044.
- **Non-loopback SQL listener** — gated on mTLS.
- **Non-loopback task listener** — preserved from ADR 042/043.
- **Wendelon descriptor discovery / refresh** — needs Wendelon-side list-descriptors RPC; cross-ring substrate ADR.
- **Multi-tenant routing** — needs authenticated transport carrying tenant claims; ADR 045+.
- **Prepared statements / parameterized queries** — future composition ADR.
- **Multi-statement transactions** — substantial future cycle constrained by ADR 037.
- **Persistent SQL connections / pipelined statements** — wire admits extension; V1 CLI is one-shot.
- **gRPC / Arrow Flight protocol frontend** — future ADR can wrap `SqlFrame` without changing daemon behavior.
- **Postgres wire protocol frontend** — same future-frontend story.
- **`nilket toron inspect` real wiring** — ADR 040 reservation; orthogonal cycle.
- **Toron daemon observability listeners** — shares TLS material story with public SQL listener; bundles with ADR 044.
- **Wendelon SQL branch** — `nilket wendelon sql` rejection preserved by Wendelon's parser (ADR 040 invariant).
- **Root `nilket sql`** — rejected by nilket root dispatcher (ADR 040 D3).
- **SELECT without FROM** — unrelated SQL-surface widening; not a witness fixture anchor.

## Council History

- **Iter 0:** Topic shape decision needed. Claude proposed Shape C explicitly (loopback-only SQL listener). Codex proposed same topic + scope. Gemini proposed gRPC + bundled mTLS. Iter-0 surveyed three shapes (bundled mega-ADR, transport-first, SQL-first).
- **Iter 1:** Convergence on Shape C. Codex iter-1 produced the sharpest D-set (17 decisions including the critical D11 "use DistributedQueryCoordinator, not local bypass" and the `SELECT 1` rejection). Claude retracted four iter-0 positions (READY format, wire frame names, transport-providers-optional, in-flight cancellation as gating). Gemini yielded on gRPC + bundled mTLS; retained two narrow disagreements (READY format, `[sql]` default state).
- **Iter 2:** Claude retracts two iter-1 positions (persistent connection lifecycle → one-shot; `SELECT 1`-anchored witness → table-backed). Gemini's two iter-1 disagreements (READY format, `[sql]` default state) resolved 2-of-3 in favor of Codex/Claude convergence.
- **Iter 3:** Final triple ratification. Codex retracts two iter-2 trial positions (`no Cancel` frame and single-buffered `Result`) in favor of the converged `Cancel` protocol-shape + best-effort semantics and `ResultBatch` + `Complete` response stream. D-set locked at 20. Guard set locked at 21. Fixture set locked at 14 required + 4 V1-evidence optional. Ready to promote.

## Conclusion

ADR 043 ships:

- one `nilket toron sql <statement>` lifts the second ADR 040 reservation (after `start` in ADR 042; `inspect` remains)
- loopback-only QUIC SQL listener — real substrate, never inert; production-shaped `SqlFrame` wire protocol
- `nilket toron sql` is a client; daemon is the server; nilket root stays runtime-free
- `inspect` byte-identically reserved (continuity invariant; third consecutive reservation preserved)
- single shared `Arc<WendelonDescriptorRegistry>` across catalog/scan/write/delete/admin (extends ADR 042 D7 invariant)
- `DistributedQueryCoordinator` path exercised end-to-end over QUIC task dispatch; ADR 042 task listener becomes load-bearing for SQL
- ADR 042 Wendelon execution gap closed by the D18 Plan A / Plan B access path
- `[coordinator]` static worker registry config (now meaningful)
- structured error codes; no `*V1` suffix; same encoding crate as ADR 039 wire types
- `READY <worker>` preserved byte-identical; `SQL_READY <sql>` added as second line (backward compat)
- one-shot CLI client; persistent sessions left for future ADR
- 21 promoted architecture guards + 1 convention guard + 14 required integration fixtures + 4 V1-evidence optional
- Plan A / Plan B sub-lift on Wendelon QUIC provider maturity (Phase 0 audit decides)

Authentication, multi-tenant routing, descriptor refresh, observability listeners, prepared statements, transactions, gRPC / Postgres wire frontends, and `inspect` wiring all remain follow-up ADRs with clear scope boundaries.
