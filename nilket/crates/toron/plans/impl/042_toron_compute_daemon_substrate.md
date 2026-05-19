# Implementation Plan: ADR 042 Toron Compute Daemon Substrate

**Status:** Promoted (paired with `plans/adr/042_toron_compute_daemon_substrate.md`)

## Implementation Thesis

Lift `nilket toron start` to a real loopback worker daemon. Add `crates/toron/src/config.rs` (the `ToronBootstrapConfig` shape + `ToronDescriptorBootstrap` product type + loaders) and `crates/toron/src/daemon.rs` (the `ToronDaemon` lifecycle). Wire `crates/toron/src/cli.rs`'s `start` arm to a synchronous runner that constructs a Tokio runtime and delegates to async daemon startup. Bind `ToronWorker::handle_connection` to loopback only; reject non-loopback bind addresses at config-validation time with a stable diagnostic. Emit `READY <bound_endpoint>` to stdout after binding. No public SQL listener, no observability listeners, no mTLS — those are future ADRs.

## Phase 0: Audit and Pin Existing Surfaces

Before implementation, capture evidence for the load-bearing assumptions.

1. **`ToronWorker::handle_connection` auth state:** confirm no auth / peer-identity gate exists. Record file:line evidence (motivates D9 loopback-only).
2. **`TableSchemaDescriptor` serde derives:** confirm `Serialize` + `Deserialize` are present on `wendelon_client::TableSchemaDescriptor`. If missing, add them in Phase 3.
3. **Adapter constructors:** confirm the signatures match daemon assembly:
   - `WendelonCatalog::with_client_and_registry(client, registry)`
   - `WendelonStorageScan::with_descriptor_registry(client, registry)`
   - `WendelonStorageWrite::with_descriptor_registry(client, registry)`
   - `WendelonStorageDeleteRows::with_descriptor_registry(client, registry)`
   - `WendelonStorageAdmin::new(client, registry)`
4. **`wendelon_client::Client::connect(...)` signature:** lock the exact `[storage.wendelon]` TLS field set in `ToronStorageConfig` against the client constructor.
5. **Fixture worker isolation:** confirm `crates/toron/src/bin/toron_fixture_worker.rs` is fixture-only and can remain source-unchanged.
6. **`WendelonDescriptorRegistry::from_descriptors` signature:** confirm it accepts `(TableId, TableSchemaDescriptor)` pairs (motivates D5's product-type shape).

Evidence captured here goes in the implementation notes alongside the promoted ADR.

## Phase 1: Dependencies and Module Skeleton

Update `crates/toron/Cargo.toml` only if missing:

- `toml = "0.8"` (match Wendelon's config loader)
- `serde_json = "1.0"` (match Wendelon's JSON helper surface)
- `tracing = "0.1"`
- `tracing-subscriber = { version = "0.3", features = ["env-filter"] }`

Add module exports in `crates/toron/src/lib.rs`:

```rust
pub mod config;
pub mod daemon;
```

**Do not** touch `crates/nilket/Cargo.toml` (preserves ADR 040 root-runtime-free invariant).

## Phase 2: `crates/toron/src/config.rs` — Config Loader

Define the product type tree (`ToronBootstrapConfig` + section types). Public functions:

- `load_toron_bootstrap_config(path) -> Result<ToronBootstrapConfig, ToronConfigError>`
- `render_example_config() -> String` (includes commented-out `# descriptor_bootstrap_path = "..."` hint)
- `write_example_config(path) -> Result<(), ToronConfigError>` (refuses overwrite)

Validation rules:

- Empty/missing Wendelon storage bootstrap → fail closed.
- `memory_budget_bytes == 0` → fail closed.
- `task_listener_bind` must parse as `SocketAddr`.
- **Non-loopback `task_listener_bind` → fail closed** with the literal diagnostic:
  ```text
  toron worker listener requires authenticated transport for non-loopback bind address
  ```
  Loopback check: `addr.ip().is_loopback()`.

## Phase 3: Descriptor Bootstrap Product Type

In `config.rs` (or a new `descriptor_bootstrap.rs` module):

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

**Do not** add `ToronDescriptorBootstrapV1` or any other `*V1`-suffixed type (per ADR 039 user directive carried forward).

Public function:

```rust
pub fn load_descriptor_bootstrap(
    path: impl AsRef<Path>,
) -> Result<Vec<(TableId, wendelon_client::TableSchemaDescriptor)>, ToronConfigError>;
```

Validation:
- Deserialize as `ToronDescriptorBootstrap` from JSON.
- Reject duplicate `table_id` values.
- Reject `table_id == 0` (reserved sentinel; check existing Toron conventions during Phase 0).
- Reject duplicate `(namespace, table_name)` at registry-build via `WendelonDescriptorRegistry::from_descriptors`.
- Preserve descriptor physical fingerprints exactly.

## Phase 4: `crates/toron/src/daemon.rs` — Daemon Assembly

Define the daemon surface:

```rust
pub struct ToronDaemon { /* handle, services, shutdown_tx, listener_task, drain_timeout */ }

pub struct ToronDaemonHandle {
    pub worker_endpoint: SocketAddr,
}

struct ToronDaemonServices {
    catalog: Arc<WendelonCatalog>,
    scanner: Arc<WendelonStorageScan>,
    writer: Arc<WendelonStorageWrite>,
    deleter: Arc<WendelonStorageDeleteRows>,
    admin: Arc<WendelonStorageAdmin>,
}

impl ToronDaemon {
    pub async fn start(config: ToronBootstrapConfig) -> Result<Self, ToronDaemonError>;
    pub fn handle(&self) -> &ToronDaemonHandle;
    pub async fn shutdown(self) -> Result<(), ToronDaemonError>;
}
```

Startup sequence (`ToronDaemon::start`):

1. **Install tracing subscriber** from `config.observability`. Idempotent across daemon restarts.
2. **Build descriptor pairs:** if `config.catalog.descriptor_bootstrap_path` is `Some`, call `load_descriptor_bootstrap(path)?`. Otherwise empty `Vec`.
3. **Construct exactly one `Arc<WendelonDescriptorRegistry>`** via `WendelonDescriptorRegistry::from_descriptors(pairs)`. Emit `tracing::info!("descriptor_registry_constructed")` exactly once.
4. **Construct one `Arc<wendelon_client::Client>`** from `config.storage.wendelon`.
5. **Construct Wendelon-backed adapters** using `Arc::clone(&registry)` for each:
   - `WendelonCatalog::with_client_and_registry(Arc::clone(&client), Arc::clone(&registry))`
   - `WendelonStorageScan::with_descriptor_registry(Arc::clone(&client), Arc::clone(&registry))`
   - `WendelonStorageWrite::with_descriptor_registry(Arc::clone(&client), Arc::clone(&registry))`
   - `WendelonStorageDeleteRows::with_descriptor_registry(Arc::clone(&client), Arc::clone(&registry))`
   - `WendelonStorageAdmin::new(Arc::clone(&client), Arc::clone(&registry))`
   Store these adapters in the daemon services bundle even though ADR 042
   has no public query surface yet; this keeps the registry-convergence
   invariant concrete without constructing throwaway adapters only for
   source-read guards.
6. **Construct `ToronWorker`** via `ToronWorker::with_partition_store_and_endpoint(...)` using the production scan adapter, `MemoryManager`, `InMemoryPartitionStore`, the configured `WorkerId`, and the advertised endpoint.
7. **Bind the TCP listener** at `config.worker.task_listener_bind` via `tokio::net::TcpListener::bind(...)`. Capture `listener.local_addr()?` as the resolved `worker_endpoint`.
8. **Spawn the accept loop** via `tokio::spawn(run_accept_loop(worker, listener, shutdown_rx, connection_tasks))`.
9. **Return** the `ToronDaemon`.

**Do not** construct a `DistributedQueryCoordinator` — ADR 042 has no query admission surface.

`run_accept_loop`:

```rust
async fn run_accept_loop(
    worker: Arc<ToronWorker>,
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    connection_tasks: Arc<Mutex<JoinSet<()>>>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            accept = listener.accept() => match accept {
                Ok((stream, _peer)) => {
                    let worker = Arc::clone(&worker);
                    connection_tasks.lock().await.spawn(async move {
                        if let Err(err) = worker.handle_connection(stream).await {
                            tracing::warn!(error = %err, "toron worker connection error");
                        }
                    });
                }
                Err(err) => tracing::error!(error = %err, "toron listener accept failed"),
            },
        }
    }
}
```

`ToronDaemon::shutdown`:

1. Send `()` on `shutdown_tx`.
2. Wait for the listener task to stop accepting new connections.
3. Drain the tracked connection `JoinSet` until it is empty or
   `drain_timeout` expires.
4. On timeout-exceeded, abort remaining connection tasks and return
   `ToronDaemonError::DrainTimeoutExceeded`.

## Phase 5: Synchronous CLI Runner

In `daemon.rs`:

```rust
pub fn run_start(config_path: PathBuf) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to initialize Toron runtime: {err}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(run_start_async(config_path))
}

async fn run_start_async(config_path: PathBuf) -> ExitCode {
    let config = match crate::config::load_toron_bootstrap_config(&config_path) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("failed to load {}: {err}", config_path.display());
            return ExitCode::from(2);
        }
    };
    let daemon = match ToronDaemon::start(config).await {
        Ok(d) => d,
        Err(err) => {
            eprintln!("toron daemon bootstrap failed: {err}");
            return ExitCode::from(1);
        }
    };

    // READY line on stdout, after bind and before blocking on Ctrl-C.
    println!("READY {}", daemon.handle().worker_endpoint);

    tokio::signal::ctrl_c().await.ok();
    eprintln!("toron: draining");
    match daemon.shutdown().await {
        Ok(()) => {
            eprintln!("toron: shutdown complete");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("toron: shutdown failed: {err}");
            ExitCode::from(1)
        }
    }
}
```

## Phase 6: Wire `crates/toron/src/cli.rs`

Replace the `start` reservation arm and add config-helper arms. Preserve `sql` and `inspect` reservations byte-identically.

Current shape:

```rust
[verb] if verb == "start" => reserved("start"),
```

New shape:

```rust
[verb, config_path] if verb == "start" => {
    crate::daemon::run_start(PathBuf::from(config_path))
}
[verb] if verb == "start" => {
    eprintln!("toron start requires <path/to/toron.toml>");
    ExitCode::from(2)
}
[flag] if flag == "--print-example-config" => {
    print!("{}", crate::config::render_example_config());
    ExitCode::SUCCESS
}
[flag, path] if flag == "--write-example-config" => {
    match crate::config::write_example_config(Path::new(path)) {
        Ok(()) => {
            eprintln!("toron: wrote starter config to {path}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("failed to write starter config to {path}: {err}");
            ExitCode::from(2)
        }
    }
}
```

`sql` and `inspect` arms are **untouched**:

```rust
[verb] if verb == "sql" => reserved("sql"),
[verb, rest @ ..] if verb == "sql" && !rest.is_empty() => reserved("sql"),
[verb] if verb == "inspect" => reserved("inspect"),
[verb, _rest @ ..] if verb == "inspect" => reserved("inspect"),
```

The `reserved` helper and its diagnostic constant (`"is reserved; production execution is not wired in ADR 040"`) stay byte-identical.

## Phase 7: Architecture Guards (15)

Add to `crates/toron/tests/architecture_test.rs`:

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

Forbidden-token sets per guard:

- **Guard 11 (no public query service):** `ToronQueryService`, `ExecuteSqlRequest`, `SqlListener`, `QueryListener`, `ArrowResultService`.
- **Guard 12 (no observability listeners):** `observability::spawn`, `health_listener`, `ready_listener`, `metrics_listener`, `TlsAcceptor`.
- **Guard 13 (no fixture storage):** `toron_fixture_worker`, `FixtureStorageScan`, fixture table names from the fixture worker.
- **Guard 14 (no Wendelon node):** `WendelonNode`, `raft_storage`, `SlabManager`, `data_server`.

For guards 8 and 9, use narrow source-block extraction around the daemon startup sequence (brace-block technique from ADR 038/041 guards).

## Phase 8: Integration Fixtures (13 required)

### `crates/toron/tests/toron_config_test.rs` (new)

- `toron_example_config_loads`
- `toron_write_example_config_refuses_overwrite`
- `toron_config_rejects_missing_wendelon_bootstrap`
- `toron_config_rejects_zero_memory_budget`
- `toron_descriptor_bootstrap_requires_table_ids` — JSON with raw descriptors (no `table_id`) fails to deserialize.
- `toron_descriptor_bootstrap_rejects_duplicate_table_ids`
- `toron_descriptor_bootstrap_loads_registry`

### `crates/nilket/tests/cli_integration.rs` (additions)

- `nilket_toron_start_requires_config_path`
- `nilket_toron_start_rejects_missing_config_file`
- `nilket_toron_start_rejects_non_loopback_bind`
- `nilket_toron_sql_and_inspect_still_reserved` (continuity check)
- `nilket_toron_print_example_config`
- `nilket_toron_write_example_config_refuses_overwrite`

### `crates/toron/tests/toron_daemon_test.rs` (new)

- `toron_daemon_binds_loopback_ephemeral_port`
- `toron_daemon_prints_ready_endpoint` — spawn subprocess; parse `READY <addr>\n` from **stdout**.
- `toron_daemon_accepts_cancel_task_over_loopback` — open TCP connection; send `CancelTask` wire frame; observe response. **Wire-correctness witness** that does not pull fixture storage.
- `toron_daemon_shutdown_stops_accepting_connections`

### Optional V1-evidence fixtures (post-promotion; non-gating)

- `toron_started_worker_executes_no_scan_task_over_loopback`
- `toron_started_worker_sigkill_retry_digest_is_canonical` — the V1 target signal.

## Phase 9: Verification

```text
cargo fmt --all
cargo check -p toron -p nilket
cargo test -p toron --test architecture_test adr042
cargo test -p toron --test toron_config_test
cargo test -p toron --test toron_daemon_test
cargo test -p toron --test distributed_process_test
cargo test -p nilket
git diff --check
```

Broader sweep (when feasible):

```text
cargo test -p toron
cargo clippy --workspace -- -D warnings
```

## Phase 10: Evidence Record

Before promotion / merge, affirm:

- [ ] `nilket toron start <toron.toml>` binds a loopback worker listener, emits `READY <bind_addr>` to stdout, and accepts task RPCs over the production wire protocol.
- [ ] Non-loopback bind addresses are rejected at config load with the D9 stable diagnostic.
- [ ] `nilket toron sql` and `nilket toron inspect` remain reserved with **byte-identical** ADR 040 text.
- [ ] Daemon constructs exactly one shared `Arc<WendelonDescriptorRegistry>` and uses it across catalog/scan/write/delete/admin.
- [ ] Descriptor bootstrap loads from JSON file (via `ToronDescriptorBootstrap` product type) when path configured; daemon starts cleanly with empty registry when absent.
- [ ] `toron_fixture_worker.rs` source is **unchanged**.
- [ ] No `ToronQueryService` / public SQL listener introduced.
- [ ] No observability listeners introduced.
- [ ] No `tokio::signal::ctrl_c` in test harnesses.
- [ ] All 15 architecture guards green; all 13 required integration fixtures green.

## Acceptance Checklist

- [ ] Phase 0 audit recorded.
- [ ] Phase 1 dependencies + module skeleton.
- [ ] Phase 2 `config.rs` with loopback validation.
- [ ] Phase 3 `ToronDescriptorBootstrap` product type.
- [ ] Phase 4 `ToronDaemon::start` / `shutdown`.
- [ ] Phase 5 synchronous CLI runner; `READY` on stdout; tracing on stderr.
- [ ] Phase 6 `cli.rs` wired; reservations untouched.
- [ ] Phase 7 fifteen architecture guards green.
- [ ] Phase 8 thirteen required integration fixtures green.
- [ ] Phase 9 verification commands run.
- [ ] Phase 10 evidence record complete.

## Non-Goals

- Public SQL listener (`nilket toron sql` wiring).
- Authenticated remote task transport (`TlsToronTransport`).
- Wendelon descriptor discovery / refresh.
- Observability listeners.
- Static coordinator registry config in `toron.toml`.
- `nilket toron inspect` wiring.
- Worker membership / placement / gossip.
- Running `WendelonNode` in-process.
- `nilket standalone`.
- Modifying `toron_fixture_worker.rs`.
- Fine-grained in-flight task cancellation on shutdown.
- `*V1`-suffixed type names.
