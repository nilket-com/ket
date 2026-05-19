# Implementation Plan: ADR 043 Public Toron SQL Listener

**Status:** Promoted (paired with `plans/adr/043_public_toron_sql_listener.md`)

## Implementation Thesis

Lift `nilket toron sql` to a real loopback QUIC SQL listener. Add `crates/toron/src/sql_wire.rs` (wire frame types) and extend `crates/toron/src/config.rs` (`[sql]`, `[coordinator]`) and `crates/toron/src/daemon.rs` (QUIC SQL listener; execution-capable Wendelon access path; `DistributedQueryCoordinator` wiring). Wire `crates/toron/src/cli.rs`'s `sql` arm to a synchronous client runner. Bind the SQL listener to loopback only; reject non-loopback bind addresses at config validation. Emit `READY <worker>` byte-identical to ADR 042 plus a new `SQL_READY <sql>` line. SQL path goes through `DistributedQueryCoordinator`, exercising the ADR 042 task listener as load-bearing substrate. QUIC is the default transport for ADR 043 SQL ingress and SQL-driven task dispatch; plain TCP remains ADR 042 compatibility/test substrate, not the forward default.

## Phase 0: Audit and Plan A / Plan B Selection

Before implementation, capture evidence for the load-bearing assumptions and resolve the Plan A vs Plan B decision.

1. **Toron QUIC listener/transport seam.** Audit whether Toron can reuse an existing workspace QUIC stream abstraction without importing Wendelon server internals. ADR 043 may reuse sanctioned shared transport helpers, but must not couple the Toron SQL listener to `WendelonNode` or Wendelon raft/server modules.
2. **`wendelon_client::Client::connect_quic` (or equivalent) production readiness.** Read the constructor surface. Confirm it accepts `cert_path` / `key_path` / `ca_path` / `cluster_domain` and dials a real Wendelon cluster. **Decision rule:**
   - **Plan A:** constructor is mature; ADR 043 wires it directly. Fixture 11 uses a real Wendelon test cluster harness.
   - **Plan B:** constructor has unresolved seams (incomplete TLS plumbing, missing tier registration, etc.); ADR 043 ships a sanctioned in-process Wendelon test harness for fixture 11. Defer QuicIoUring wiring to a named follow-up ADR.
3. **`DistributedQueryCoordinator` construction surface.** Confirm it accepts the daemon's existing Wendelon-backed adapters + a `StaticWorkerRegistry`. Read `crates/toron/src/coordinator.rs:33-50` (struct + impl).
4. **`StaticWorkerRegistry` construction.** Confirm it accepts a `Vec<(WorkerId, WorkerEndpoint)>` and supports `include_self_worker` semantics (i.e., the coordinator's own worker endpoint can be registered).
5. **`ToronWorker::handle_connection` task wire protocol.** Confirm task dispatch from coordinator → worker → result flows through this loop unchanged over the QUIC task transport. ADR 042's `toron_daemon_accepts_cancel_task_over_loopback` proves the payload surface works; ADR 043 changes the default transport posture.
6. **Arrow IPC stream format support.** Confirm `arrow::ipc::writer::StreamWriter` / `arrow::ipc::reader::StreamReader` are available in the workspace dependency tree. If not, add `arrow-ipc`.
7. **`wire::encode` / `wire::decode` surface.** Confirm the shared encoding helpers from ADR 039 are exposed through a sanctioned workspace crate/module that Toron may use. If the only current implementation is Wendelon-private, extract or re-export the helpers through an approved public boundary; do not import Wendelon internals from Toron.
8. **`SELECT * FROM <bootstrapped_table>` end-to-end.** Phase 0 spike: load a `descriptor_bootstrap_path` with one table; submit `SELECT *` through the coordinator (bypassing the SQL listener for the spike); confirm Arrow batches return. This is the witness path that fixture 11 will exercise via the full SQL wire in Phase 8.

Plan A/B decision and Phase 0 evidence are recorded in the implementation evidence before the feature merge.

## Phase 1: Dependencies and Module Skeleton

Update `crates/toron/Cargo.toml` only if missing:

- `arrow-ipc = "..."` (workspace version)
- `arrow-cast = { version = "...", features = ["prettyprint"] }` (for CLI rendering)
- QUIC dependencies or sanctioned shared workspace transport helpers needed for Toron's loopback QUIC listener/client.
- Reuse existing `postcard`, `serde`, `serde_bytes`, `tokio`, `tracing` deps.

Add module exports in `crates/toron/src/lib.rs`:

```rust
pub mod sql_wire;
// existing: pub mod config; pub mod daemon; ...
```

**Do not** touch `crates/nilket/Cargo.toml` (preserves ADR 040 root-runtime-free invariant; guard 4 still passes).

## Phase 2: `crates/toron/src/sql_wire.rs` — Wire Frame Types

Define the protocol product/sum types. No `*V1` suffix.

```rust
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::distributed::identity::QueryId;
use crate::storage::TenantId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlFrame {
    pub protocol_version: SqlProtocolVersion,
    pub query_id: QueryId,
    pub message: SqlMessage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SqlProtocolVersion {
    Current,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlMessage {
    Execute(ExecuteRequest),
    Cancel(CancelRequest),
    ResultBatch(ResultBatch),
    Complete(Complete),
    Error(SqlError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub statement: String,
    pub statement_timeout_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub query_id: QueryId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultBatch {
    pub batch_index: u32,
    pub arrow_ipc_bytes: ByteBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Complete {
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlError {
    pub code: SqlErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SqlErrorCode {
    SyntaxError,
    PlanningError,
    BindingError,
    CatalogError,
    StorageError,
    TenantMismatch,
    StatementTimeout,
    Cancelled,
    CancellationUnsupported,
    NoLiveWorker,
    Internal,
}
```

Public helpers for length-prefixed framing (reuse `wire::encode` / `wire::decode` from ADR 039):

```rust
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &SqlFrame) -> io::Result<()>;
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<SqlFrame>;
```

Length prefix: u32 little-endian (matches existing Toron task wire convention).

## Phase 3: `crates/toron/src/config.rs` — `[sql]` and `[coordinator]` Sections

Extend `ToronBootstrapConfig`:

```rust
pub struct ToronBootstrapConfig {
    pub node: ToronNodeConfig,
    pub worker: ToronWorkerConfig,
    pub storage: ToronStorageConfig,
    pub catalog: ToronCatalogConfig,
    pub observability: ToronObservabilityConfig,
    pub sql: Option<SqlConfig>,        // NEW
    pub coordinator: CoordinatorConfig, // NEW
}

pub struct SqlConfig {
    pub transport: SqlTransport,
    pub listener_bind: SocketAddr,
    pub statement_timeout: Duration,
}

pub enum SqlTransport {
    Quic,
}

pub struct CoordinatorConfig {
    pub include_self_worker: bool,        // default true
    pub static_workers: Vec<StaticWorkerConfig>,
}

pub struct StaticWorkerConfig {
    pub worker_id: WorkerId,
    pub endpoint: SocketAddr,
}
```

Validation rules:

- If `[sql]` is present, `transport` defaults to `quic`; ADR 043 has no TCP SQL listener mode.
- If `[sql]` is present, `listener_bind` must parse as `SocketAddr` AND `.ip().is_loopback()`. Non-loopback bind fails with the literal D5 diagnostic:
  ```text
  toron sql listener requires authenticated transport for non-loopback bind address
  ```
- `[coordinator].static_workers[*].endpoint` must `.ip().is_loopback()` (preserves D12 — non-loopback static workers deferred to authenticated task transport ADR).
- `[sql].statement_timeout_seconds == 0` → fail closed.

Extend `render_example_config` to include `[sql]` enabled and `[coordinator]` with `include_self_worker = true`:

```toml
[sql]
transport = "quic"
listener_bind = "127.0.0.1:7742"
statement_timeout_seconds = 60

[coordinator]
include_self_worker = true
# [[coordinator.static_workers]]
# worker_id = "..."
# endpoint = "127.0.0.1:7743"
```

Missing `[sql]` section → `sql: None`; daemon binds worker-only (preserves ADR 042 backward compat per fixture 13).

Missing `[coordinator]` section → default `CoordinatorConfig { include_self_worker: true, static_workers: Vec::new() }`. Backward compat.

## Phase 4: `crates/toron/src/daemon.rs` — QUIC SQL Listener + Wendelon Access Path + Coordinator

### 4a. Execution-capable Wendelon access path (Plan A or Plan B per Phase 0)

**Plan A:** replace `providers: HashMap::new()` with real provider registration:

```rust
fn build_wendelon_client(
    config: &ToronWendelonStorageConfig,
    client_uuid: [u8; 16],
) -> Result<wendelon_client::Client, ToronDaemonError> {
    let quic_provider: Arc<dyn TransportProvider> = Arc::new(
        wendelon_client::quic_io_uring::QuicIoUringProvider::new(
            // cert/key/ca paths from config; cluster_domain
        )?,
    );
    let providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::from([
        (TransportTier::QuicIoUring, quic_provider),
    ]);
    // ... rest unchanged from ADR 042 ...
}
```

**Plan B:** ship a sanctioned in-process test provider:

```rust
let test_provider: Arc<dyn TransportProvider> = Arc::new(
    wendelon_client::test_harness::InProcessProvider::new(/* test harness handle */),
);
```

Plan B production builds **must** fail closed if the test provider is the only registered provider — guard 18 source-asserts that ADR 043 production binaries reach real provider construction, not just the test harness.

### 4b. `DistributedQueryCoordinator` wiring

```rust
struct ToronDaemonServices {
    catalog: Arc<WendelonCatalog>,
    scanner: Arc<WendelonStorageScan>,
    writer: Arc<WendelonStorageWrite>,
    deleter: Arc<WendelonStorageDeleteRows>,
    admin: Arc<WendelonStorageAdmin>,
    coordinator: Arc<DistributedQueryCoordinator</* ... */>>, // NEW
}
```

Coordinator construction:

1. Build a `StaticWorkerRegistry`:
   - If `config.coordinator.include_self_worker`, register the daemon's self-worker entry using the resolved QUIC worker endpoint.
   - For each `config.coordinator.static_workers[*]`, register with the configured `worker_id` + `endpoint`.
2. Construct `DistributedQueryCoordinator::new(catalog, scanner, writer, deleter, admin, static_worker_registry, ...)`.
3. Store in `ToronDaemonServices`.

### 4c. QUIC SQL listener accept loop

```rust
if let Some(sql_config) = &config.sql {
    let sql_endpoint = bind_toron_sql_quic_endpoint(sql_config).await?;
    // ... spawn run_sql_accept_loop ...
} else {
    sql_endpoint = None;
}
```

`run_sql_accept_loop`:

```rust
async fn run_sql_accept_loop(
    coordinator: Arc<DistributedQueryCoordinator<...>>,
    daemon_tenant_id: TenantId,
    sql_config: SqlConfig,
    endpoint: QuicEndpoint,
    mut shutdown_rx: oneshot::Receiver<()>,
    sql_connection_tasks: Arc<Mutex<JoinSet<()>>>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            accept = endpoint.accept_bi() => match accept {
                Ok(stream) => {
                    let coordinator = Arc::clone(&coordinator);
                    sql_connection_tasks.lock().await.spawn(async move {
                        if let Err(err) = handle_sql_connection(stream, coordinator, daemon_tenant_id, sql_config.statement_timeout).await {
                            tracing::warn!(error = %err, "toron sql connection error");
                        }
                    });
                }
                Err(err) => {
                    tracing::error!(error = %err, "toron sql quic listener accept failed");
                    break;
                }
            },
        }
    }
}
```

`handle_sql_connection` (one-shot per D17):

1. `read_frame(stream)` → `SqlFrame`.
2. If the message is `Cancel(req)`, route `req.query_id` through the best-effort cancellation path and return `Cancelled` or `CancellationUnsupported`.
3. If the message is not `Execute(req)`, emit `SqlErrorCode::Internal` or a narrow protocol-error mapping if one is introduced locally.
4. Stamp `tenant_id` from `daemon_tenant_id` (D9 server-stamped).
5. Parse `req.statement` via `SqlPlanner::plan_sql_text(&req.statement)?` → `LogicalPlan`.
6. Bind via `Binder` → `ExecutionPlan`.
7. Execute via `coordinator.execute_query(&plan, statement_timeout).await?` → `Vec<RecordBatch>`.
8. Encode batches as Arrow IPC stream (`arrow::ipc::writer::StreamWriter`):
   - For each batch (or for the combined buffered stream — Gemini's iter-1 buffered position; D20-compatible), emit `SqlMessage::ResultBatch { batch_index, arrow_ipc_bytes }`.
9. Emit `SqlMessage::Complete { row_count }`.
10. On error at any step, emit `SqlMessage::Error { code, message }` mapped per the stable error code table.
11. Close connection (one-shot).

Error mapping:

| Internal error | `SqlErrorCode` |
|---|---|
| `SqlPlanner` parse error | `SyntaxError` |
| `SqlPlanner` lowering error | `PlanningError` |
| `Binder` error | `BindingError` |
| `CatalogError` | `CatalogError` |
| `StorageError` | `StorageError` |
| Tenant mismatch | `TenantMismatch` |
| `tokio::time::timeout` exceeded | `StatementTimeout` |
| Cancelled by `Cancel` frame | `Cancelled` |
| Cancellation routed but coordinator doesn't support it | `CancellationUnsupported` |
| No live worker / empty worker registry | `NoLiveWorker` |
| Anything else | `Internal` |

### 4d. `SQL_READY` line emission

After both listeners bind (worker first, sql second):

```rust
println!("READY {}", worker_endpoint);
if let Some(sql_endpoint) = sql_endpoint {
    println!("SQL_READY {sql_endpoint}");
}
```

Preserves ADR 042's first line byte-identical (guard 20).

### 4e. Shutdown extension

`ToronDaemon::shutdown` already drains worker connection tasks; extend it to also drain SQL connection tasks the same way. Same `drain_timeout` applies.

## Phase 5: `crates/toron/src/cli.rs` — SQL Client Arm

Replace the `sql` reservation arms with a real client runner. Preserve `inspect` reservation byte-identically.

Current shape (per the ADR 042 review):

```rust
[verb] if verb == "sql" => reserved("sql"),
[verb, rest @ ..] if verb == "sql" && !rest.is_empty() => reserved("sql"),
```

New shape:

```rust
[verb] if verb == "sql" => {
    eprintln!("toron sql requires <statement...>");
    ExitCode::from(2)
}
[verb, rest @ ..] if verb == "sql" && !rest.is_empty() => {
    let statement = rest.join(" ");
    crate::cli::sql_client::run_sql_client(statement, /* endpoint resolution */)
}
```

`inspect` arms untouched (byte-identical reservation; guard 2 source-asserts):

```rust
[verb] if verb == "inspect" => reserved("inspect"),
[verb, _rest @ ..] if verb == "inspect" => reserved("inspect"),
```

### Add `crates/toron/src/cli/sql_client.rs` (or sibling module)

The following is shape-oriented pseudocode; production code should convert I/O, decode, handshake, and render failures into stable stderr diagnostics and non-zero exit codes instead of relying on raw `?` propagation.

```rust
pub fn run_sql_client(statement: String, endpoint_override: Option<SocketAddr>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to initialize Toron runtime: {err}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(run_sql_client_async(statement, endpoint_override))
}

async fn run_sql_client_async(statement: String, endpoint_override: Option<SocketAddr>) -> ExitCode {
    // 1. Resolve endpoint (flag > env > default).
    let endpoint = resolve_endpoint(endpoint_override);

    // 2. Dial daemon over QUIC.
    let mut stream = match QuicEndpoint::client()?.connect_stream(endpoint).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("failed to connect to toron sql endpoint {endpoint}: {err}");
            return ExitCode::from(2);
        }
    };

    // 3. Send Execute frame.
    let frame = SqlFrame {
        protocol_version: SqlProtocolVersion::Current,
        query_id: QueryId::new(),
        message: SqlMessage::Execute(ExecuteRequest {
            statement,
            statement_timeout_seconds: None,
        }),
    };
    write_frame(&mut stream, &frame).await?;

    // 4. Read response frames; collect ResultBatch arrow IPC bytes; render on Complete; print error on Error.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut row_count: Option<u64> = None;
    let mut error: Option<SqlError> = None;
    loop {
        match read_frame(&mut stream).await? {
            SqlFrame { message: SqlMessage::ResultBatch(batch), .. } => {
                let reader = StreamReader::try_new(&batch.arrow_ipc_bytes[..], None)?;
                for record in reader { batches.push(record?); }
            }
            SqlFrame { message: SqlMessage::Complete(complete), .. } => {
                row_count = Some(complete.row_count);
                break;
            }
            SqlFrame { message: SqlMessage::Error(err), .. } => {
                error = Some(err);
                break;
            }
            _ => continue,
        }
    }

    // 5. Render.
    if let Some(err) = error {
        eprintln!("toron sql error [{:?}]: {}", err.code, err.message);
        return ExitCode::from(1);
    }
    if batches.is_empty() {
        match row_count {
            Some(0) => println!("OK"),
            Some(n) => println!("OK rows={n}"),
            None => println!("OK"),
        }
    } else {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let pretty = arrow_cast::pretty::pretty_format_batches(&batches)?;
        println!("{pretty}");
        println!("({total_rows} rows)");
    }
    ExitCode::SUCCESS
}

fn resolve_endpoint(flag_override: Option<SocketAddr>) -> SocketAddr {
    // precedence: --endpoint flag > TORON_SQL_ENDPOINT env > 127.0.0.1:7742 default.
    // Bare socket addresses use ADR 043's default QUIC transport.
    flag_override
        .or_else(|| std::env::var("TORON_SQL_ENDPOINT").ok()
            .and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| "127.0.0.1:7742".parse().unwrap())
}
```

CLI flag parsing for `--endpoint`: add to the `sql` arm match in `cli.rs`. If `--endpoint <addr>` appears as the first two tokens after `sql`, strip them and pass as `endpoint_override`.

## Phase 6: Architecture Guards

Add the 21 promoted guards to `crates/toron/tests/architecture_test.rs`, plus the ADR 043 convention guard for brand-clean new protocol/type identifiers. Forbidden-token sets per guard:

- **Guard 5 (`adr043_toron_cli_sql_is_client_only`):** `crates/nilket/src/main.rs` and `crates/toron/src/cli.rs` SQL arm do not import `DistributedQueryCoordinator`, `QueryCoordinator`, `WendelonCatalog`, `WendelonStorageScan`, `WendelonStorageWrite`, `WendelonStorageDeleteRows`, `WendelonStorageAdmin`, `WendelonDescriptorRegistry`.
- **Guard 8 (`adr043_task_transport_is_quic_by_default_and_loopback_only`):** ADR 043 SQL and task-dispatch paths use the QUIC transport by default; any remaining `TcpTransport` use is confined to ADR 042 compatibility tests/fixtures and not the production SQL path.
- **Guard 9 (`adr043_sql_wire_uses_length_prefixed_postcard`):** `sql_wire.rs` contains `postcard` usage AND length-prefix framing (`u32` LE prefix or equivalent).
- **Guard 10 (`adr043_sql_wire_uses_arrow_ipc_not_row_strings`):** `ResultBatch` carries `arrow_ipc_bytes: ByteBuf` (or `Vec<u8>` with `#[serde(with = "serde_bytes")]`); no `Vec<String>` row format.
- **Guard 12 (`adr043_no_grpc_protobuf_or_arrow_flight`):** Toron source contains no `tonic`, `prost`, `arrow_flight` imports.
- **Guard 13 (`adr043_no_remote_sql_auth_policy`):** Toron SQL source contains no token-auth helpers, client tenant-claim admission, or non-loopback remote-auth policy. QUIC protocol TLS material is allowed; remote authorization policy remains ADR 044.
- **Guard 14 (`adr043_daemon_uses_single_descriptor_registry_for_sql`):** Daemon source contains exactly one `WendelonDescriptorRegistry::from_descriptors` call; SQL coordinator construction uses `Arc::clone(&registry)`.
- **Guard 15 (`adr043_sql_uses_distributed_coordinator_not_local_bypass`):** Daemon source's SQL handler constructs / uses `DistributedQueryCoordinator`, not `QueryCoordinator` (local-only).
- **Guard 17 (`adr043_no_wendelon_descriptor_refresh`):** Daemon source contains no `list_descriptors`, `refresh_descriptor`, `watch_descriptor`, `refresh_topology` symbols (extends ADR 042 guard 9).
- **Guard 18 (`adr043_wendelon_client_has_real_provider_mode_for_sql`):** `build_wendelon_client` (or equivalent) does NOT contain `providers: HashMap::new()` for the SQL-enabled production path; either real provider registration (Plan A) or sanctioned test harness (Plan B with documented test-only gating).
- **Guard 20 (`adr043_sql_ready_does_not_mutate_adr042_ready_line`):** Daemon source emits the literal `println!("READY {}` (worker line) followed by optional `println!("SQL_READY {}` — separate print calls, not a single combined line.
- **Guard 21 (`adr043_sql_wire_uses_existing_wire_crate`):** `sql_wire.rs` imports / uses `wire::encode` / `wire::decode` or equivalent shared helpers from a sanctioned ADR 039 wire boundary, not a Wendelon-private module.
- **Convention guard (`adr043_new_sql_protocol_names_are_brand_clean`):** New ADR 043 protocol, wire, and config identifiers avoid `ToronSql*`, `ToronQuic*`, `ToronCoordinatorConfig`, `ToronStaticWorker`, `ToronWendelonTlsConfig`, and related raw-config prefixes; project names remain only in operator-facing command/env/readiness text.

Prefer source-read assertions over broad semantic greps. For guards 14, 15, 18, 20, use narrow source-block extraction around the daemon startup sequence (brace-block technique from ADRs 038/041/042).

## Phase 7: Integration Fixtures

Add the 14 required fixtures across existing + new test files.

### `crates/toron/tests/toron_sql_wire_test.rs` (new)

- `toron_sql_wire_round_trips_arrow_ipc_batch` — construct `SqlFrame` with a `ResultBatch` carrying real Arrow IPC bytes; round-trip through `write_frame` / `read_frame`; assert byte-identical reconstruction.

### `crates/toron/tests/toron_sql_listener_test.rs` (new)

- `toron_sql_listener_rejects_non_loopback_bind` — config with non-loopback SQL bind fails validation with D5 diagnostic.
- `toron_daemon_prints_sql_ready_endpoint` — spawn `nilket toron start` as subprocess with `[sql]` enabled; parse both `READY <worker>` and `SQL_READY <sql>` from stdout.
- `toron_daemon_without_sql_section_preserves_worker_only_start` — config missing `[sql]`; daemon binds worker only; no `SQL_READY` line emitted. ADR 042 backward compat.
- `toron_sql_select_from_bootstrapped_descriptor_executes_over_wire` — daemon with `descriptor_bootstrap_path` (one table) + sanctioned Wendelon harness; submit `SELECT * FROM <table>` via the QUIC SQL listener + `SqlFrame`; assert Arrow batches deserialize and match expected rows. **Wire-correctness witness.**
- `toron_sql_syntax_error_emits_stable_error_code` — submit malformed SQL; observe `SqlError { code: SyntaxError, ... }`.
- `toron_sql_uses_self_worker_registry_entry` — daemon with `include_self_worker = true`; submit SQL; observe via tracing that a task dispatches to the self-worker endpoint.
- `toron_sql_complete_frame_carries_stable_row_count` — submit `CREATE TABLE`, `INSERT INTO ... VALUES (1)`, `DELETE FROM ... WHERE id = 1`, `DROP TABLE`; observe `Complete { row_count }` for each; CLI renders `OK rows=<n>` / `OK` shape per D20.

### `crates/nilket/tests/cli_integration.rs` (additions)

- `nilket_toron_sql_requires_statement` — `nilket toron sql` (no args) exits non-zero with `requires <statement...>`.
- `nilket_toron_sql_dials_endpoint_from_flag` — `nilket toron sql --endpoint 127.0.0.1:<port> "SELECT * FROM ..."` against a known daemon endpoint; succeeds.
- `nilket_toron_sql_dials_endpoint_from_env` — `TORON_SQL_ENDPOINT=127.0.0.1:<port> nilket toron sql "..."`; succeeds.
- `nilket_toron_sql_reports_connection_failure` — unreachable endpoint; exit non-zero with stable diagnostic.
- `nilket_toron_inspect_still_reserved` — byte-identical ADR 040 diagnostic continuity.
- `nilket_root_sql_still_rejected` — `nilket sql ...` rejected at nilket root (ADR 040 invariant; guard 3).

### Optional V1-evidence fixtures (non-gating, post-promotion)

- `toron_sql_insert_then_select_against_wendelon` — full INSERT → SELECT round-trip (Plan A only; gated on Phase 0 audit).
- `toron_sql_cancel_request_is_accepted` — `Cancel { query_id }` accepted; emits stable `Cancelled` / `CancellationUnsupported` error frame.
- `toron_sql_static_loopback_worker_dispatches_remote_task` — two-daemon test with `[[coordinator.static_workers]]`.
- `toron_sql_worker_sigkill_retry_digest_is_canonical` — V1 target signal via SQL submission.

## Phase 8: Verification

```text
cargo fmt --all
cargo check -p toron -p nilket
cargo test -p toron --test architecture_test adr043
cargo test -p toron --test toron_sql_wire_test
cargo test -p toron --test toron_sql_listener_test
cargo test -p toron --test toron_config_test
cargo test -p toron --test toron_daemon_test
cargo test -p nilket
git diff --check
```

Broader sweep (when feasible):

```text
cargo test -p toron
cargo clippy --workspace -- -D warnings
```

## Phase 9: Evidence Record

Before promotion / merge, affirm:

- [ ] Phase 0 audit recorded; Plan A or Plan B selected with rationale.
- [ ] `nilket toron sql "SELECT * FROM <bootstrapped_table>"` against a daemon with `descriptor_bootstrap_path` returns expected Arrow batches end-to-end.
- [ ] `nilket toron sql` against an unreachable endpoint emits stable error and exits non-zero.
- [ ] `nilket toron inspect` remains reserved with byte-identical ADR 040 text (continuity invariant; third consecutive reservation preserved through ADRs 040 → 042 → 043).
- [ ] Daemon emits `READY <worker>` byte-identical to ADR 042 + new `SQL_READY <sql>` line when `[sql]` configured.
- [ ] `[sql]` section absent → daemon boots in worker-only mode (ADR 042 compat).
- [ ] Daemon constructs exactly one shared `Arc<WendelonDescriptorRegistry>` (extends ADR 042 D7 invariant).
- [ ] SQL path routes through `DistributedQueryCoordinator`, not a local bypass.
- [ ] Real Wendelon transport providers wired (Plan A) or sanctioned test harness with named provider follow-up (Plan B).
- [ ] No `tonic`, `prost`, `arrow_flight`, token-auth, client tenant-claim admission, or non-loopback remote-auth policy in Toron source.
- [ ] No `*V1` suffix on `SqlFrame` / `SqlMessage` / `SqlErrorCode` / etc.
- [ ] `toron_fixture_worker.rs` source unchanged (extends the ADR 035-043 byte-identical refactor-invariant chain to eight preserved cycles).
- [ ] All 21 promoted architecture guards plus the ADR 043 convention guard are green; all 14 required integration fixtures green.

## Acceptance Checklist

- [ ] Phase 0 audit recorded; Plan A/B chosen.
- [ ] Phase 1 dependencies + module skeleton in place.
- [ ] Phase 2 `sql_wire.rs` shipped; frames round-trip through `wire::encode` / `wire::decode`.
- [ ] Phase 3 `[sql]` + `[coordinator]` config sections; loopback validation; example config updated.
- [ ] Phase 4a execution-capable Wendelon access path wired (Plan A or Plan B).
- [ ] Phase 4b `DistributedQueryCoordinator` constructed inside daemon with `StaticWorkerRegistry` from config.
- [ ] Phase 4c SQL listener accept loop bound; one-shot connection handler wired through coordinator.
- [ ] Phase 4d `SQL_READY` line emitted after worker `READY` line.
- [ ] Phase 4e shutdown drains SQL connection tasks.
- [ ] Phase 5 `cli.rs` SQL arm replaced; `sql_client` module shipped; `--endpoint` + `TORON_SQL_ENDPOINT` + default precedence.
- [ ] Phase 6 twenty-one architecture guards added and green.
- [ ] Phase 7 fourteen required integration fixtures added and green.
- [ ] Phase 8 verification commands run; evidence captured.
- [ ] Phase 9 evidence record complete.

## Non-Goals

- Remote authenticated Toron listener policy (ADR 044).
- mTLS / token auth policy for SQL listener.
- Non-loopback SQL listener.
- Non-loopback task listener.
- Wendelon descriptor discovery / refresh.
- Multi-tenant client claims.
- Prepared statements / parameterized queries.
- Multi-statement transactions.
- Persistent SQL connections / pipelined statements.
- gRPC / Arrow Flight / Postgres wire protocol frontends.
- `nilket toron inspect` real wiring.
- Toron daemon observability listeners.
- `nilket wendelon sql` lift.
- Root `nilket sql`.
- `SELECT without FROM` SQL-surface widening.
- Modifying `toron_fixture_worker.rs`.
- Fine-grained in-flight cancellation across worker boundaries.
- `*V1`-suffixed type names.

## Risk Register

| Risk | Mitigation |
|---|---|
| Plan A transport provider construction has unresolved seams | Phase 0 audit; Plan B fallback with sanctioned test harness. |
| `SELECT 1`-style queries used as witness fixtures | ADR 043 negative scope + Codex iter-1 explicit rejection; fixture 11 anchored on table-backed query via `descriptor_bootstrap_path`. |
| SQL listener accidentally constructed in nilket process | Guard 5 source-asserts no coordinator/storage adapter imports in nilket. |
| Wire encoding diverges from ADR 039 conventions | D19 + guard 21 source-asserts `wire::encode` / `wire::decode` reuse. |
| `READY <worker>` line format breaks under SQL-enabled daemon | D16 + guard 20: separate `println!` calls; ADR 042 first line byte-identical. |
| Multiple descriptor registries constructed | D10 + guard 14 (extends ADR 042 D7 invariant). |
| Local bypass replaces distributed coordinator | D11 + guard 15. |
| gRPC / Arrow Flight dependency creep | Guard 12 forbids `tonic` / `prost` / `arrow_flight` imports. |
| Remote-auth scope creep | Guard 13 forbids token-auth helpers, client tenant-claim admission, and non-loopback remote-auth policy; QUIC protocol TLS material is allowed. |
| Cancellation semantics overpromised | D15 protocol-shape + best-effort; CancellationUnsupported is an acceptable outcome. |
| SQL listener takes scope from `nilket toron inspect` | D1 + guard 2 byte-identical inspect reservation. |
| `toron_fixture_worker.rs` regressed by daemon SQL changes | Guard from ADR 042 (`adr042_fixture_worker_source_remains_isolated`) still passes; extends invariant chain. |
