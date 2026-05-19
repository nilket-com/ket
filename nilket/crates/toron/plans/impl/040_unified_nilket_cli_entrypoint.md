# Implementation Plan: ADR 040 Unified Nilket CLI Entrypoint

**Status:** Promoted (paired with `plans/adr/040_unified_nilket_cli_entrypoint.md`)

## Implementation Thesis

Implement `nilket` as a synchronous two-branch router. The root binary owns no command grammar and no async runtime. Each role crate parses its own arguments and constructs its own runtime if it needs one.

```text
nilket wendelon [WENDELON_ARGS...]
nilket toron    [TORON_ARGS...]
nilket --version
nilket --help
```

## Phase 0: Downstream-Tooling Audit (Plan A vs Plan B)

Before code changes, grep the repository for invocations of the existing daemon binary:

- `cargo run --bin wendelon`
- `cargo run -p wendelon`
- `target/release/wendelon`, `target/debug/wendelon`
- `bin/wendelon`, `./wendelon`, `wendelon-node`, `wendelon_node`

Inspect every `*.sh`, `Dockerfile*`, `*.yaml`, `*.yml`, `Justfile`, `Makefile`, and `.github/workflows/*` that could exec the daemon. Also `README*.md`, `docs/`, and any runbook directories.

**Decision rule:**
- **No external references found → Plan A.** Remove the legacy `wendelon` operator binary path. `nilket wendelon ...` is the only operator invocation path.
- **References found → Plan B.** Keep `crates/wendelon/src/main.rs` as a five-line shim that calls `wendelon::cli::run("wendelon", std::env::args().skip(1))`. Add a deprecation paragraph to the ADR-040 release notes; plan removal in a follow-up cycle.

Record the audit outcome and the chosen plan as a one-paragraph note appended to `plans/adr/040_unified_nilket_cli_entrypoint.md` before merge.

## Phase 1: Extract Wendelon CLI

1. Create `crates/wendelon/src/cli.rs`.
2. Move argument parsing and per-subcommand execution from `crates/wendelon/src/main.rs` into the new module. Preserve current Wendelon command semantics exactly:
   ```text
   <program> <path/to/wendelon.toml>           # start daemon
   <program> audit verify <path/to/audit.ndjson>
   <program> --print-example-config
   <program> --write-example-config <path/to/wendelon.toml>
   ```
3. Expose a synchronous public runner:
   ```rust
   use std::process::ExitCode;

   pub fn run<I>(program: &str, args: I) -> ExitCode
   where
       I: IntoIterator<Item = String>;
   ```
   `program` is the conceptual `argv[0]` (e.g. `"nilket wendelon"` from the multiplexer, `"wendelon"` from the legacy shim under Plan B). Use it in help text.
4. For daemon startup, construct the Tokio runtime *inside* Wendelon's runner:
   ```rust
   fn run_start(config_path: PathBuf) -> ExitCode {
       let runtime = match tokio::runtime::Builder::new_multi_thread()
           .enable_all()
           .build()
       {
           Ok(rt) => rt,
           Err(err) => {
               eprintln!("failed to initialize Wendelon runtime: {err}");
               return ExitCode::from(2);
           }
       };
       runtime.block_on(daemon_start(config_path))
   }
   ```
   `daemon_start` is the existing async startup body moved out of `main.rs`. Synchronous subcommands (audit verify, print/write config) run without a runtime.
5. Export the module from `crates/wendelon/src/lib.rs`:
   ```rust
   pub mod cli;
   ```
6. **Plan A:** delete `crates/wendelon/src/main.rs` if the package still has a default legacy binary entrypoint. If implementation introduces an explicit `[[bin]]` for that legacy path, delete that target too.
   **Plan B:** reduce `crates/wendelon/src/main.rs` to:
   ```rust
   use std::process::ExitCode;

   fn main() -> ExitCode {
       wendelon::cli::run("wendelon", std::env::args().skip(1))
   }
   ```

## Phase 2: Add Toron CLI

1. Create `crates/toron/src/cli.rs`.
2. Export it from `crates/toron/src/lib.rs`:
   ```rust
   pub mod cli;
   ```
3. Expose the same synchronous runner signature:
   ```rust
   pub fn run<I>(program: &str, args: I) -> std::process::ExitCode
   where
       I: IntoIterator<Item = String>;
   ```
4. Declare the role's subcommand surface. Toron may use `clap` internally or hand-roll a simple match. Surface to declare:
   - `start` → reserved diagnostic.
   - `sql <statement...>` → reserved diagnostic.
   - `inspect [target]` → reserved diagnostic.
   - `--help` / `-h` → role-scoped help text.
   - `--version` / `-V` → role-scoped version.
5. Each reserved verb emits a stable diagnostic on stderr and exits non-zero:
   ```text
   toron sql is reserved; production SQL execution is not wired in ADR 040
   ```
   Use the same form for `start` and `inspect`. The Phase 5 fixture pins this contract.
6. Do **not** call `crates/toron/src/bin/toron_fixture_worker.rs`. Do **not** import any `wendelon::` storage / consensus / state-machine construct.

## Phase 3: Add `crates/nilket`

1. Create `crates/nilket/Cargo.toml`:
   ```toml
   [package]
   name = "nilket"
   version = "0.1.0"     # or version.workspace = true if the workspace already supports it
   edition = "2024"
   publish = false

   [[bin]]
   name = "nilket"
   path = "src/main.rs"

   [dependencies]
   toron    = { path = "../toron" }
   wendelon = { path = "../wendelon" }
   ```
   No `clap`. No `tokio`. No `tracing`. No `anyhow`.
2. Create `crates/nilket/src/main.rs`:
   ```rust
   use std::process::ExitCode;

   fn main() -> ExitCode {
       let mut args = std::env::args();
       let program = args.next().unwrap_or_else(|| "nilket".to_string());

       match args.next().as_deref() {
           Some("--version") | Some("-V") => {
               println!("nilket {}", env!("CARGO_PKG_VERSION"));
               ExitCode::SUCCESS
           }
           Some("--help") | Some("-h") => {
               print_root_usage(&program);
               ExitCode::SUCCESS
           }
           Some("wendelon") => wendelon::cli::run("nilket wendelon", args),
           Some("toron")    => toron::cli::run("nilket toron", args),
           Some(other) => {
               eprintln!("unknown nilket command: {other}");
               print_root_usage(&program);
               ExitCode::from(2)
           }
           None => {
               print_root_usage(&program);
               ExitCode::from(2)
           }
       }
   }

   fn print_root_usage(program: &str) {
       eprintln!(
           "Usage: {program} <COMMAND>\n\n\
            Commands:\n  \
              wendelon    Start or manage a storage node\n  \
              toron       Start or manage a compute node"
       );
   }
   ```
3. The root binary must not mention `sql`, `inspect`, `audit`, `config`, or `start`.
4. Confirm the root workspace already includes `crates/nilket`. The current root `Cargo.toml` uses the `crates/*` workspace member glob, so no member-list edit should be needed unless the workspace layout changes.

## Phase 4: Architecture Guards

Add the following source-read tests to `crates/toron/tests/architecture_test.rs`, following the ADR 039 guard style. All are pure file-read assertions.

1. `adr040_nilket_is_leaf_workspace_package` — `crates/nilket/Cargo.toml` declares `wendelon` and `toron` path deps; no other workspace crate depends on `nilket`.
2. `adr040_nilket_root_has_no_role_runtime_dependencies` — `crates/nilket/Cargo.toml` does not list `clap`, `tokio`, or `tracing` as direct dependencies in `[dependencies]` or `[dev-dependencies]`.
3. `adr040_nilket_delegates_to_two_roles` — `crates/nilket/src/main.rs` contains both `Some("wendelon")` and `Some("toron")` match arms and calls `wendelon::cli::run(` and `toron::cli::run(`.
4. `adr040_no_extra_root_commands` — `crates/nilket/src/main.rs` does not contain root match arms for `Some("query")`, `Some("sql")`, `Some("inspect")`, `Some("diag")`, `Some("standalone")`, or `Some("local")`.
5. `adr040_wendelon_cli_module_exists` — `crates/wendelon/src/cli.rs` exists and contains `pub fn run(`.
6. `adr040_toron_cli_module_exists` — `crates/toron/src/cli.rs` exists and contains `pub fn run(`.
7. `adr040_wendelon_cli_does_not_import_toron_runtime` — `crates/wendelon/src/cli.rs` contains no `use toron::` lines.
8. `adr040_toron_cli_does_not_import_wendelon_runtime` — `crates/toron/src/cli.rs` contains no `use wendelon::` lines pointing at `WendelonNode`, state-machine constructors, slab managers, or storage-admin stage types.
9. `adr040_wendelon_binary_delegates_to_cli_runner` — If `crates/wendelon/src/main.rs` exists (Plan B), assert it calls `wendelon::cli::run`. Skip cleanly if absent (Plan A).

## Phase 5: Integration Fixtures

Add `crates/nilket/tests/cli_integration.rs`. Use `Command::new(env!("CARGO_BIN_EXE_nilket"))` for the lowest-dependency path.

1. `nilket_no_args_exits_nonzero_with_usage` — `nilket` exits non-zero; stderr contains both `wendelon` and `toron`.
2. `nilket_unknown_root_command_exits_nonzero_with_usage` — `nilket frobnicate` exits non-zero; stderr contains both valid command names.
3. `nilket_version_prints_single_coordinate` — `nilket --version` exits zero; stdout matches `^nilket [0-9].*$`.
4. `nilket_root_sql_is_rejected` — `nilket sql "SELECT 1"` exits non-zero (root rejection, does not reach Toron).
5. `nilket_toron_sql_reaches_toron_branch` — `nilket toron sql "SELECT 1"` exits non-zero with Toron's reservation diagnostic.
6. `nilket_wendelon_sql_is_rejected_by_wendelon` — `nilket wendelon sql "SELECT 1"` reaches Wendelon's parser and is rejected as an invalid Wendelon subcommand.
7. `nilket_wendelon_example_config_matches_existing_behavior` — `nilket wendelon --print-example-config` matches byte-for-byte the old binary's equivalent output. Under Plan B, also assert the old binary produces the same output.
8. `nilket_wendelon_audit_verify_round_trips` — `nilket wendelon audit verify <good>` exits zero; `nilket wendelon audit verify <bad>` exits non-zero with a stable diagnostic.

## Phase 6: Removal & Release Notes

- **Plan A:** Delete `crates/wendelon/src/main.rs` if it still represents the legacy operator binary path. Delete any explicit legacy `[[bin]]` target if one was introduced during refactor. Note the removal in the ADR-040 release notes.
- **Plan B:** Keep the shim. Add a one-line deprecation message to its `--help` output ("This binary is deprecated; use `nilket wendelon` instead.") and to the release notes. File a follow-up issue for removal in the next cycle.

## Phase 7: Verification

Targeted run (minimum):

```text
cargo fmt --all
cargo check -p nilket
cargo check -p wendelon
cargo check -p toron
cargo test -p toron --test architecture_test adr040
cargo test -p nilket
cargo test -p wendelon
```

Full sweep (when project capacity allows):

```text
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

If the full sweep is too expensive in this cycle, record the skipped scope and the targeted-test evidence in the implementation notes.

## Acceptance Checklist

- [ ] Phase 0 audit recorded; Plan A or Plan B chosen and documented in the promoted ADR.
- [ ] `crates/wendelon/src/cli.rs` exists; `cli::run` wired; existing subcommand semantics preserved byte-for-byte.
- [ ] `crates/toron/src/cli.rs` exists; reserved verbs emit stable diagnostics.
- [ ] `crates/nilket/` added to the workspace; `Cargo.toml` has zero arg-parsing or async-runtime deps; `main.rs` is the hand-rolled match.
- [ ] All 9 architecture guards added and green.
- [ ] All 8 integration fixtures added and green.
- [ ] Old `wendelon` binary either deleted (Plan A) or reduced to a 5-line shim (Plan B).
- [ ] ADR-040 release-notes paragraph written (audit outcome + Plan A/B choice + deprecation timeline if Plan B).

## Non-Goals

- Top-level `nilket sql`, `nilket query`, `nilket inspect`, or `nilket diag`.
- `nilket standalone` or `nilket local`.
- `clap`, `tokio`, or `tracing` in `crates/nilket`.
- Forced workspace-wide `version.workspace = true` migration.
- Production Toron SQL execution wiring.
- Promotion of `toron_fixture_worker` into a daemon.
- Any duplication of Wendelon daemon startup logic in `crates/nilket`.
