# ADR 042: Toron Compute Daemon Substrate

**Status:** Promoted
**Seed:** ADR 040 reservation pattern for `nilket toron start`; ADR 041 council carry-forward (Codex iter-0 D-set + Gemini iter-0 framing + Claude iter-1 A-set refinements)
**Council:** Claude, Codex, Gemini (3 iterations: iter-0 alignment with three open scope decisions, iter-1 Plan A/B retraction + descriptor-bootstrap correction, iter-2 final convergence)
**Category:** Substrate ADR (operator-surface continuation + cross-ring substrate composition)

## Summary

Lift the ADR 040 `nilket toron start` reservation. Add a Toron-owned `crates/toron/src/config.rs` (the `ToronBootstrapConfig` shape + `ToronDescriptorBootstrap` product type + loaders) and `crates/toron/src/daemon.rs` (the `ToronDaemon` lifecycle). Wire `crates/toron/src/cli.rs`'s `start` arm to a synchronous runner that constructs a Tokio runtime and delegates to async daemon startup. Bind the existing `ToronWorker::handle_connection` task wire protocol to loopback addresses only (`127.0.0.1`, `::1`); reject non-loopback bind addresses at config-validation time with a stable diagnostic. Emit `READY <bound_endpoint>` to stdout after binding (matches the existing fixture-worker contract). Reserve `nilket toron sql` and `nilket toron inspect` byte-identically.

**No** public SQL listener. **No** observability listeners. **No** mTLS task transport. **No** Wendelon descriptor discovery / refresh. **No** static coordinator registry config. **No** `WendelonNode` in-process. Those are follow-up ADRs.

## Motivation

After three back-to-back substrate cycles (ADRs 037/038/039), one operator-surface cycle (ADR 040), and one SQL composition cycle (ADR 041), the codebase needs a production Toron daemon. The V1 target text in the roadmap explicitly requires multi-process coordinator/worker mechanics surviving a worker SIGKILL with byte-identical normalized output — this is impossible while `nilket toron start` remains a reservation diagnostic and the only multi-process worker shape lives in the fixture binary.

ADR 042 is the **smallest cycle** that lifts the reservation while preserving the three substrate-class concerns that justify deferring `nilket toron sql` to a separate ADR: authenticated remote transport (its own ADR 043), public SQL framing (its own ADR), and descriptor discovery (its own ADR). The cycle ships a **loopback worker daemon** — real substrate, never inert — that future cycles compose against.

## Decisions

**D1: Lift only `nilket toron start`.**
`nilket toron start <path/to/toron.toml>` becomes a real daemon start. `nilket toron sql ...` and `nilket toron inspect ...` remain reserved with **byte-identical** ADR 040 diagnostic text. Missing config is a Toron usage error: `toron start requires <path/to/toron.toml>`.

**D2: Toron owns the async runtime internally.**
`toron::cli::run` stays synchronous at the library boundary. The `start` branch constructs a Tokio runtime inside Toron, mirroring Wendelon's ADR 040 runner pattern. `crates/nilket` stays runtime-free (no direct `tokio` / `clap` / `tracing` dependencies).

**D3: Toron config helpers and example-config surface.**
Add `crates/toron/src/config.rs` with:
- `load_toron_bootstrap_config(path) -> Result<ToronBootstrapConfig, ToronConfigError>`
- `render_example_config() -> String`
- `write_example_config(path) -> Result<(), ToronConfigError>` (refuses overwrite, mirroring Wendelon)

Toron's CLI also accepts `nilket toron --print-example-config` and `nilket toron --write-example-config <path>`. Parsing stays inside Toron; `crates/nilket` does not gain config grammar.

**D4: `ToronBootstrapConfig` is role-scoped and worker-first.**

```rust
pub struct ToronBootstrapConfig {
    pub node: ToronNodeConfig,             // worker_id, tenant_id, drain_timeout_seconds
    pub worker: ToronWorkerConfig,         // task_listener_bind, advertised_endpoint, memory_budget_bytes
    pub storage: ToronStorageConfig,       // Wendelon client bootstrap (exact fields locked at Phase 0 audit)
    pub catalog: ToronCatalogConfig,       // optional descriptor_bootstrap_path
    pub observability: ToronObservabilityConfig,  // tracing level + format
}
```

**No `[coordinator]` section.** Static worker registry entries are inert without a query-admitting coordinator service; they defer to the ADR 043+ that wires the public SQL service. Tests can construct a `StaticWorkerRegistry` in test code from the daemon's published endpoint without operator-facing config.

**D5: Descriptor bootstrap is a Toron product type with explicit `table_id`s.**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronDescriptorBootstrap {
    pub tables: Vec<ToronDescriptorBootstrapTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronDescriptorBootstrapTable {
    pub table_id: u64,
    pub descriptor: wendelon_client::TableSchemaDescriptor,
}
```

Serialized as JSON for operator-readability and test-fixture readability. **No `*V1` suffix on the type name** (follows ADR 039 user directive carried forward). `WendelonDescriptorRegistry::from_descriptors` requires `(TableId, TableSchemaDescriptor)` pairs; a raw `Vec<TableSchemaDescriptor>` would make table identity order-dependent.

**D6: Descriptor bootstrap is startup-only.**
If `[catalog].descriptor_bootstrap_path` is present, the daemon loads the JSON file exactly once during startup. If absent, the registry starts empty. **No Wendelon list-descriptors RPC, background refresh, or watch loops** are introduced. Existing stale-descriptor guards remain the failure mode.

**D7: One descriptor registry per daemon.**
Daemon assembly constructs exactly one `Arc<WendelonDescriptorRegistry>` and clones it into:
- `WendelonCatalog::with_client_and_registry`
- `WendelonStorageScan::with_descriptor_registry`
- `WendelonStorageWrite::with_descriptor_registry`
- `WendelonStorageDeleteRows::with_descriptor_registry`
- `WendelonStorageAdmin::new`

Preserves the ADR 038/039 registry-convergence invariant at process startup.

**D8: Worker task listener is in scope; public SQL listener is out.**
The daemon binds a task listener and serves `ToronWorker::handle_connection`. **No** `ToronQueryService`, SQL text framing, Arrow result framing, query admission, tenant/session binding, cancellation, or public listener auth. That is ADR 043+.

**D9: Existing TCP task listener is loopback-only.**
The daemon binds the existing unauthenticated TCP task protocol only to loopback IP socket addresses (`127.0.0.1`, `::1`, or any address for which `IpAddr::is_loopback()` is true). Hostname resolution in `task_listener_bind` is out of scope; non-loopback bind addresses fail startup with the stable diagnostic:

```text
toron worker listener requires authenticated transport for non-loopback bind address
```

**No inert no-listener fallback.** A daemon that accepts no work is not a meaningful lift of `toron start`.

**D10: Authenticated remote task transport is a follow-up ADR.**
ADR 042 preserves the `ToronTransport` seam closed to loopback. A future ADR adds `TlsToronTransport` and a TLS-accepting worker listener without changing daemon lifecycle shape or `ToronBootstrapConfig` shape (mTLS material lives in a new optional `[worker.tls]` section added by that ADR).

**D11: Deterministic daemon handle.**
`ToronDaemon::start(config) -> Result<ToronDaemon, ToronDaemonError>`. The handle exposes `handle() -> &ToronDaemonHandle` (carrying the bound `worker_endpoint`) and `shutdown() -> impl Future<Output = Result<(), ToronDaemonError>>`. Tests construct daemons directly on `127.0.0.1:0` and shut them down without `tokio::signal::ctrl_c`. The CLI path blocks on Ctrl-C and then calls `shutdown()`.

Shutdown semantics: stop accepting new connections, signal the listener task to exit, wait for spawned connection tasks up to `drain_timeout_seconds` (default 30s), force-abort remaining tasks after timeout. Fine-grained in-flight task cancellation is deferred.

**D12: Machine-readable readiness line on stdout.**
After a successful bind, `nilket toron start` prints to **stdout**:

```text
READY <worker-task-endpoint>
```

Human-readable startup details continue through tracing on stderr. This preserves the existing fixture-worker contract (which already emits `READY` on stdout) and gives tests / supervisors a non-HTTP readiness signal while health listeners remain out of scope.

**D13: Tracing-only observability.**
Toron daemon installs a process-global tracing subscriber from `[observability]` (Pretty format default; INFO level default; `RUST_LOG` override). **No** health, ready, metrics, OTLP, or mTLS observability listeners.

**D14: Fixture worker stays test-only and source-unchanged.**
`crates/toron/src/bin/toron_fixture_worker.rs` remains a test binary; its source is **not modified by ADR 042**. Preserves the byte-identical refactor-invariant chain from ADRs 035-041. Production daemon modules in `crates/toron/src/daemon.rs` and `crates/toron/src/config.rs` must not import `FixtureStorageScan`, fixture table names, or `toron_fixture_worker`. The production daemon may reuse `ToronWorker` (the production worker type), not the fixture binary.

**D15: Toron start uses Wendelon client APIs, not Wendelon server internals.**
The daemon may construct a `wendelon_client::Client` and Toron's Wendelon-backed adapters. It must **not** construct `WendelonNode`, import Wendelon raft storage, open Wendelon slab managers, or host Wendelon state-machine internals.

## Architecture Guards (15)

Added to `crates/toron/tests/architecture_test.rs`:

1. `adr042_toron_start_is_not_reserved`
2. `adr042_toron_sql_and_inspect_remain_reserved`
3. `adr042_nilket_root_stays_runtime_free`
4. `adr042_toron_cli_owns_runtime_not_nilket`
5. `adr042_toron_config_surface_exists`
6. `adr042_descriptor_bootstrap_is_product_type_with_table_ids`
7. `adr042_descriptor_bootstrap_has_no_v1_type_suffix`
8. `adr042_daemon_uses_single_descriptor_registry`
9. `adr042_descriptor_bootstrap_is_startup_only`
10. `adr042_unauthenticated_task_listener_is_loopback_only`
11. `adr042_no_public_query_service`
12. `adr042_no_observability_listeners`
13. `adr042_no_fixture_storage_in_production_daemon`
14. `adr042_no_wendelon_node_in_toron_start`
15. `adr042_fixture_worker_source_remains_isolated`

## Integration Fixtures (13 required + 2 V1-evidence optional)

Required:

1. `nilket_toron_start_requires_config_path`
2. `nilket_toron_start_rejects_missing_config_file`
3. `nilket_toron_start_rejects_non_loopback_bind`
4. `nilket_toron_sql_and_inspect_still_reserved`
5. `nilket_toron_print_example_config`
6. `nilket_toron_write_example_config_refuses_overwrite`
7. `toron_example_config_loads`
8. `toron_descriptor_bootstrap_requires_table_ids`
9. `toron_descriptor_bootstrap_loads_registry`
10. `toron_daemon_binds_loopback_ephemeral_port`
11. `toron_daemon_prints_ready_endpoint`
12. `toron_daemon_accepts_cancel_task_over_loopback` *(wire-correctness witness — proves listener + worker loop without fixture storage)*
13. `toron_daemon_shutdown_stops_accepting_connections`

Optional V1-evidence (non-gating; post-promotion):

- `toron_started_worker_executes_no_scan_task_over_loopback`
- `toron_started_worker_sigkill_retry_digest_is_canonical` — the V1 target signal (multi-process SIGKILL byte-identical-retry-digest).

## Sub-Lift Posture

**Single-plan.** No Plan A / Plan B framing. Loopback-only is the substrate floor; non-loopback bind addresses fail at config validation. Plan B framing (initialize-but-bind-no-listener) was rejected 2-of-3 in iter-1 (Codex + Claude) and settled by user acclamation in iter-2. A daemon that accepts no work is not a meaningful lift of `toron start`.

## Out of Scope (Recorded for Future ADRs)

- Public SQL listener (`nilket toron sql` wiring) — **ADR 043 lead candidate**.
- Authenticated remote task transport (`TlsToronTransport`).
- Wendelon descriptor discovery / refresh.
- Observability listeners (health / ready / metrics endpoints).
- Static coordinator registry config in `toron.toml`.
- `nilket toron inspect` wiring.
- Worker membership / placement / gossip ("weak control ring").
- Running `WendelonNode` in-process.
- `nilket standalone` / local two-child orchestration.
- Fine-grained in-flight task cancellation on daemon shutdown.
- `*V1`-suffixed type names (per ADR 039 no-V1-suffix convention).

## Council History

- **Iter 0:** Three independent drafts. All three proposed Toron Compute Daemon Substrate as the topic (already pre-aligned from ADR 041 carry-forward). Codex framed loopback-only as preferred; Claude resolved three named-open scope decisions explicitly (descriptor bootstrap A4b, observability A6b, public SQL listener deferred); Gemini matched on most but retained an inert no-listener Plan B.
- **Iter 1:** Codex held loopback-only, added the load-bearing descriptor-bootstrap-product-type correction (D5 with explicit `table_id`). Claude retracted Plan A/B framing and `[coordinator]` config in favor of Codex's positions. Gemini retained inert no-listener Plan B and `[coordinator]` config — minority positions.
- **Iter 2 (final):** Claude retracted two iter-1 positions in favor of Codex iter-1 (READY stream → stdout; descriptor bootstrap shape → product type with table_ids). Gemini's minority positions resolved by user acclamation in favor of Codex/Claude. D-set locked at 15. Guard set locked at 15. Fixture set locked at 13 required + 2 V1-evidence optional. Triple convergence.

## Promotion Notes

ADR 042 is the **first ADR that lifts an ADR 040 reservation**. It establishes the discipline for future reservation lifts (`sql` → ADR 043; `inspect` → ADR 044+): each lift produces a real substrate, not an inert one; each lift preserves the byte-identical diagnostic text for the still-reserved siblings; each lift uses a single-plan posture rather than Plan A / Plan B framing (substrate cycle discipline does not apply because the reservation itself is binary — either lifted with real substrate, or not).

The ten-phase migration is materialized in `plans/impl/042_toron_compute_daemon_substrate.md`.
