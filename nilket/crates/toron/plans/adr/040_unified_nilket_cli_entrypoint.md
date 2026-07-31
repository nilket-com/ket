# ADR 040: Unified Nilket CLI Entrypoint

**Status:** Promoted
**Seed:** `f7574e0 plan: brainstorm 005 unified nilket cli`
**Council:** Claude, Codex, Gemini (iter 0 through final iter 3, full convergence)
**Category:** Operator-surface ADR (first of this kind; introduces the product entrypoint layer)

## Summary

Add a new workspace crate `crates/nilket` whose single binary, `nilket`, is the unified operator-facing entrypoint for the Nilket distributed database. The binary is a synchronous router with a strict two-noun root taxonomy:

```text
nilket wendelon [WENDELON_ARGS...]
nilket toron    [TORON_ARGS...]
nilket --version
nilket --help
```

It owns no command grammar and no async runtime. Each role crate parses its own arguments via a `cli::run(program, args)` library runner and constructs its own runtime if it needs one. Wendelon's existing daemon entrypoint is extracted into `wendelon::cli`; Toron gains a new `toron::cli` module whose subcommands return stable reservation diagnostics.

The CLI taxonomy now mirrors the ADR 039 disjoint-hardware ring boundary at the OS-process layer: one product surface, two disjoint process roles, never both inside one process.

## Motivation

Through ADRs 037 / 038 / 039 the codebase converged on a strict Toron/Wendelon ring boundary with disjoint hardware and a single-call cross-ring admin pattern (ADR 039 D14). Operators currently invoke only the `wendelon` daemon binary; there is no Toron production binary. Brainstorm 005 raised the question of what the eventual product surface looks like: do we ship two binaries forever, one fused binary, or a unified entrypoint that dispatches?

ADR 040 picks the unified-entrypoint shape, but with a sharp constraint that distinguishes it from a typical "monolith multiplexer": **the multiplexer does not absorb the parser surface of the roles it routes to**. It parses exactly one token (the role noun) and delegates the rest. This preserves the disjoint-role boundary at the parser layer as cleanly as it is preserved at the process layer.

## Decisions

**D1: `crates/nilket` Is a Leaf Aggregator.**
A new workspace crate `crates/nilket` is added. It depends on `wendelon` and `toron`. No other workspace crate depends on `nilket`. Its only artifact is the `nilket` binary.

**D2: One Product Binary, Two Process Roles.**
The `nilket` binary is the authoritative operator-facing product entrypoint. Each invocation selects exactly one role and dispatches into that role's runner. A single `nilket` invocation is a single process running a single role — ADR 039's disjoint-hardware boundary is preserved verbatim at the OS-process layer. There is no "fused" mode that runs both runtimes in one process.

**D3: Strict Two-Noun Root Taxonomy.**
`nilket` accepts exactly two valid first arguments: `wendelon` and `toron`. There are no top-level client verbs (no `query`, `sql`, `inspect`), no top-level `diag` namespace, no `standalone` or `local` convenience. Unknown first arguments print root usage and exit non-zero.

**D4: No Shared Runtime in the Root Binary.**
`crates/nilket` does not directly depend on `clap`, `tokio`, `tracing`, or any other argument-parser or async-runtime framework. The multiplexer is a synchronous router. Role crates own their runtime semantics internally — Wendelon's `cli::run` constructs a Tokio runtime for daemon startup; synchronous helpers (audit verify, config rendering) run without one.

**D5: Hand-Rolled Delegated Parsing.**
`crates/nilket/src/main.rs` is a pure dispatcher. It intercepts `--version` / `-V` and `--help` / `-h` at the root, then matches `args.next()` on `Some("wendelon")` or `Some("toron")` and calls the role's runner, propagating the returned `ExitCode`. Role crates own their own help text, version flags, error formatting, and subcommand structure.

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
```

**D6: Library-Runner Extraction in Both Role Crates.**
Both role crates expose a synchronous public runner in a `cli` module:

```rust
pub fn run<I>(program: &str, args: I) -> std::process::ExitCode
where
    I: IntoIterator<Item = String>;
```

Wendelon's runner is a real extraction from the existing `crates/wendelon/src/main.rs`: the `Start`, `PrintExampleConfig`, `WriteExampleConfig`, and `AuditVerify` subcommands move into `crates/wendelon/src/cli.rs`. Toron's runner is a fresh module that declares `start`, `sql`, and `inspect` subcommands and returns stable diagnostics. The `program` parameter is the conceptual `argv[0]` (e.g. `"nilket wendelon"` from the multiplexer, `"wendelon"` from a legacy binary shim under Plan B) and is used in help text so users see the invocation they actually typed.

**D7: Role-Scoped Client Verbs.**
SQL belongs to Toron (compute layer), not to Nilket and not to Wendelon. `nilket toron sql ...` is grammatical. `nilket sql ...` is rejected at the root. `nilket wendelon sql ...` is rejected by Wendelon's parser as an unknown Wendelon subcommand. The audit verifier remains under Wendelon: `nilket wendelon audit verify ...`. No top-level `diag` namespace is introduced.

**D8: Role-Scoped Reservation Pattern.**
Verbs that are not yet fully implemented (`nilket toron start`, `nilket toron sql`, `nilket toron inspect`) are parsed by Toron's CLI and return a stable diagnostic of the form `"toron <verb> is reserved; production execution is not wired in ADR 040"` with a non-zero exit code. The namespace is claimed; the implementation is deferred to a future ADR. Wendelon verbs are real and wired (no reservation diagnostics on the Wendelon side).

**D9: Single Version Coordinate as Packaging Claim.**
`nilket --version` prints exactly one version string identifying the unified product. The multiplexer intercepts `--version` / `-V` before the first-arg match. This ADR does not mandate `version.workspace = true` for every workspace crate; release engineering can normalize manifests in a separate cycle.

**D10: Plan A / Plan B Implementation Phasing.**
Implementation begins with a Phase 0 audit of downstream tooling. If no CI script, Dockerfile, or deploy manifest invokes the existing `wendelon` binary directly, **Plan A** applies: remove the legacy `wendelon` operator binary path, and `nilket wendelon ...` is the only operator invocation path. If downstream invocations exist, **Plan B** applies: keep `crates/wendelon/src/main.rs` as a five-line shim that calls `wendelon::cli::run("wendelon", std::env::args().skip(1))` for one transition cycle, with a deprecation note in release notes.

**D11: Source-Read Architecture Guards as Primary Boundary.**
Boundary enforcement is via source-read tests that inspect Cargo manifests and `cli.rs` files. No runtime process-global role flag is introduced. Source-read guards alone suffice; the operator-surface layer adds no defense-in-depth runtime checks beyond what the role crates already enforce internally. This is the same enforcement posture used in ADR 039.

## Architecture Guards

Required in `crates/toron/tests/architecture_test.rs`:

1. `adr040_nilket_is_leaf_workspace_package` — `crates/nilket/Cargo.toml` declares `wendelon` and `toron` as path deps; no other workspace crate depends on `nilket`.
2. `adr040_nilket_root_has_no_role_runtime_dependencies` — `crates/nilket/Cargo.toml` does not list `clap`, `tokio`, or `tracing` as direct dependencies.
3. `adr040_nilket_delegates_to_two_roles` — `crates/nilket/src/main.rs` contains both `Some("wendelon")` and `Some("toron")` match arms and calls `wendelon::cli::run(` and `toron::cli::run(`.
4. `adr040_no_extra_root_commands` — `crates/nilket/src/main.rs` does not contain root match arms for `Some("query")`, `Some("sql")`, `Some("inspect")`, `Some("diag")`, `Some("standalone")`, or `Some("local")`.
5. `adr040_wendelon_cli_module_exists` — `crates/wendelon/src/cli.rs` exists and exports `pub fn run`.
6. `adr040_toron_cli_module_exists` — `crates/toron/src/cli.rs` exists and exports `pub fn run`.
7. `adr040_wendelon_cli_does_not_import_toron_runtime` — `crates/wendelon/src/cli.rs` contains no `use toron::` lines.
8. `adr040_toron_cli_does_not_import_wendelon_runtime` — `crates/toron/src/cli.rs` contains no `use wendelon::` lines pointing at `WendelonNode`, state-machine internals, slab managers, or storage-admin stage types.
9. `adr040_wendelon_binary_delegates_to_cli_runner` — If `crates/wendelon/src/main.rs` exists (Plan B), it calls `wendelon::cli::run`; skips cleanly if the file is absent (Plan A).

## Integration Fixtures

Required in `crates/nilket/tests/cli_integration.rs`:

1. `nilket_no_args_exits_nonzero_with_usage`
2. `nilket_unknown_root_command_exits_nonzero_with_usage`
3. `nilket_version_prints_single_coordinate`
4. `nilket_root_sql_is_rejected`
5. `nilket_toron_sql_reaches_toron_branch`
6. `nilket_wendelon_sql_is_rejected_by_wendelon`
7. `nilket_wendelon_example_config_matches_existing_behavior`
8. `nilket_wendelon_audit_verify_round_trips`

## Out of Scope (Recorded for Future ADRs)

- Real Toron SQL execution wiring (`nilket toron sql` returns a reservation diagnostic; future ADR wires the real client).
- Real Toron daemon startup (`nilket toron start` returns a reservation diagnostic).
- Toron inspect tooling.
- Top-level `nilket diag` cluster-wide diagnostics.
- `nilket standalone` / `nilket local` orchestration. If introduced later, must preserve the Brainstorm 005 invariant: local convenience starts separate child processes, never both role runtimes inside one process.
- Workspace-wide `version.workspace = true` manifest migration.

## Council History

- **Iter 0:** Three independent drafts. Claude proposed `<noun> <verb>` taxonomy with top-level client verbs (`nilket query`, `nilket inspect`). Codex proposed a `clap`-driven multiplexer. Gemini proposed structural leaf-crate aggregation.
- **Iter 1:** Convergence on library-runner extraction + reservation pattern. Outstanding: hand-rolled vs `clap` for the multiplexer; placement of audit verifier; whether to force workspace version inheritance.
- **Iter 2:** User-directed simplification: strict two-noun root taxonomy; delegated parsing (no `clap` in `nilket`); no top-level client verbs; SQL belongs strictly to Toron. Claude iter-2 + Gemini iter-3 + Codex iter-2 fully converged on this shape. Codex P2/P3/P4 ratified (no runtime role panic, no forced version inheritance, audit under Wendelon).
- **Iter 3 (final):** Closed two cosmetic open questions (root `--version` interception; minimal multi-line root help). Absorbed Codex iter-3 D4 sharpening: no shared async runtime in the root binary. Final D-set: 11 decisions; 9 architecture guards; 8 integration fixtures. Promoted.

## Promotion Notes

This is the **first operator-surface ADR** in the project. Prior ADRs are either substrate (037/038/039 cross-ring infrastructure), composition (036 SQL DELETE), or component-internal (the rest). ADR 040 introduces the category and establishes its discipline: source-read guards, role-scoped reservation, leaf-crate aggregation, no shared runtime, and explicit Plan A / Plan B audit phasing.

**Implementation audit outcome:** Phase 0 found repo-local references that still build, package, or verify the `wendelon` binary, including `crates/wendelon/ci/check_fips_symbol_sweep.sh`, `crates/wendelon/ci/generate_regulated_release_evidence.sh`, `crates/wendelon/ci/verify_regulated_release_evidence.sh`, and compliance/runbook docs. ADR 040 therefore ships **Plan B**: keep the legacy `wendelon` binary as a shim delegating to `wendelon::cli::run`, while `nilket wendelon ...` becomes the authoritative operator path.
