# Implementation Plan: ADR 007 — Rune Sandbox Enforcement

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:** `plans/adr/0007_rune_sandbox_enforcement.md`
**Bench-results artifact:** N/A — ADR 007 is a safety contract;
Phase 0 audit suffices.

## Inherits substrate

- ADR 001–006 promoted contracts; all hard gates and architecture
  guards G1-G18 must continue to pass byte-identically.
- M7 ("Rune Authoring Surface") shipped the authoring API at
  `plans/impl/0007_rune_authoring_surface.md`. That plan recorded
  sandbox-fuel as "disabled in V0; to be re-enabled in V1." ADR 007
  closes that gap.
- `crates/toron/src/rune_engine.rs` (~231 lines) — the entire
  sandbox surface lives here. Specifically:
  - Line 178: `Context::with_default_modules()` →
    `Context::new()` + minimal installs.
  - Line 193: `_fuel: u64` → consumed via
    `&RuneSandboxConfig`.
  - Line 197: `Source::new` → pre-compile
    `max_source_bytes` check.
  - Line 216: `// vm.set_fuel(Some(_fuel));` → uncomment + wire to
    `config.fuel_budget`.
  - Lines 222-226: `execution.async_complete().await` → wrap in
    `tokio::time::timeout(config.wall_timeout, ...)`.
- `crates/toron/src/error.rs` (wherever `FrontendError` is defined)
  — single new `RuneSandbox { kind, message }` variant carrying the
  `RuneSandboxViolation` enum.
- `crates/toron/src/lib.rs:1080-1091` — M7 backward-compat test
  fixture; the caller updates to construct a `RuneSandboxConfig`.
- `crates/toron/src/error/distributed.rs` — `TaskFailureKind` 7-variant
  shape preserved (ADR 003 architecture guard).
- `crates/toron/Cargo.toml` — currently depends on `rune = "0.13"`.
- `crates/wendelon/plans/conventions/` — semantic names,
  newtype-units, no-premature-backward-compat,
  no-planning-phase-names.
- `crates/toron/scripts/bench/enable_profiling.sh` — Phase 0 P0.7
  reuses this to bootstrap profiler permissions
  (`perf_event_paranoid`, Yama ptrace). The profile target is
  Rune-specific (samply against a Rune test/bench), **not**
  `run_runtime_filter_perft.sh --samply` — that script profiles
  the distributed-join path and is the wrong substrate for ADR
  007 calibration.

## Goal

After ADR 007, user-supplied Rune scripts cannot escape the Toron
process kernel into host capabilities (filesystem, network, process,
environment, time, random, threading), cannot consume unbounded VM
time, cannot consume unbounded memory, and cannot DoS the parser with
unbounded source input. Sandbox failures surface as typed
`FrontendError::RuneSandbox` variants with stable, non-leaky
coordinator-facing messages.

Rune is no longer a launch blocker.

## Scale boundary

**Primary:** process-internal (frontend process hosting the Rune
engine / admission path). ADR 007 is fully self-contained inside
`crates/toron/src/rune_engine.rs` plus its test surface plus a
small `FrontendError` taxonomy extension plus an architecture-test
grep guard.

**Secondary:** none. No cross-ring contract, no scheduler change,
no transport change, no operator-level change.

## Baseline (current state)

`crates/toron/src/rune_engine.rs` as of `main`:

- **Line 178:** `Context::with_default_modules()` — loads all
  default Rune modules including (depending on Rune version) io,
  process, environment, network, time. **No allow-list
  enforcement.**
- **Line 193:** `_fuel: u64` — parameter accepted but unused
  (the `_` prefix marks it dead-input).
- **Line 197:** `Source::new(script, ...)` — accepts arbitrarily
  large `script` without pre-check.
- **Line 216:** `// vm.set_fuel(Some(_fuel));` — fuel commented
  out. **No CPU budget enforcement.**
- **Lines 222-226:** `execution.async_complete().await` — no
  outer wall-time bound.
- **`FrontendError` enum:** `RuneCompile { message }` +
  `RuneRuntime { message }` — no typed taxonomy for sandbox
  violations.

The Toron-specific module at lines 145-169 is correctly explicit
(allow-list discipline at the Toron-module layer). ADR 007 extends
this pattern to the **base context**.

## Implementation contract

- Introduce `RuneSandboxConfig` carrying `fuel_budget`,
  `memory_budget`, `wall_timeout`, `max_source_bytes`, and
  `allow_profile`. `execute_script` consumes the config (the
  existing `_fuel` parameter stops being dead input).
- Replace `Context::with_default_modules()` at line 178 with
  `Context::new()` + `install_minimal_primitives(profile)` +
  existing Toron-module install. The minimal primitive set is
  Phase 0 P0.2-derived against M7 fixtures.
- Re-enable VM-budget enforcement at line 216 via the Phase 0
  P0.1-confirmed Rune 0.13 API.
- **Enforce memory bounds as V1 hard requirement** (Codex iter-0
  framing). Phase 0 P0.3 picks Rune-native cap or helper-process
  isolation. No fuel-only V1 ship.
- **Enforce `max_source_bytes` pre-compile** before `Source::new`
  to prevent parser DoS.
- **Wall-timeout outer safety belt** via `tokio::time::timeout`
  around `execution.async_complete().await`. Maps to
  `RuneSandboxViolation::Timeout`, never `RuneRuntime`. **Catches
  yielding paths only:** Rune 0.13.4 drives the VM via a
  synchronous `vm.run()` inside `async_resume()`, so a CPU-bound
  loop with broken fuel is not preempted by
  `tokio::time::timeout`. Real preemption of broken-fuel CPU loops
  comes from the Phase 4 path-(b) parent-side SIGKILL deadline on
  the helper process.
- Extend `FrontendError` with a single `RuneSandbox { kind:
  RuneSandboxViolation, message: String }` variant carrying the
  7-label enum.
- `CapabilityDenied { capability: CapabilityCategory }` uses the
  category enum (`Filesystem`, `Process`, `Network`, `Environment`,
  `Time`, `Random`, `Threading`, `Unknown`), not raw strings. Raw
  Rune module paths are logged via internal `tracing` only — never
  on the public enum surface.
- Sandbox failures crossing the task wire map to existing
  `TaskFailureKind::TaskFailed { retryable: false, reason:
  "RuneSandbox::<label>" }` — preserves ADR 003 7-variant guard.
- Add an architecture-test grep guard proving `rune_engine.rs`
  does not call `Context::with_default_modules()` (conditional
  escape from no-new-guard default fires).
- M7 test fixtures (`crates/toron/src/lib.rs:1080+`) must produce
  byte-identical `LogicalPlan` output under the new context.
- All ADR 002 / 003 / 004 / 005 / 006 hard gates pass
  byte-identically. Architecture guards G1-G18 unchanged plus the
  new `architecture_guard_rune_context_is_allow_listed`.
- No new `MessageKind`, IR variant, `BudgetDimension`, task
  telemetry field, `TaskFailureKind` variant, or Wendelon scan
  contract change.

## Phase plan

### Phase 0 — Audit + calibration

No production code. Outputs recorded inline in this plan (the
council can promote them into a brief
`plans/impl/007_rune_sandbox_audit.md` if a permanent record is
useful; no formal bench-results artifact required).

- **P0.1: Rune 0.13 budget API audit.** Confirm the exact Rune 0.13
  API for instruction budgeting. `Vm::set_fuel` was the old
  spelling and remains in the commented line at
  `rune_engine.rs:216`; the current Rune version may have moved
  it. Output: the exact implementation hook for Phase 3.
- **P0.2: Default-module inventory + M7 fixture dependency.**
  Enumerate what `Context::with_default_modules()` installs for
  Rune 0.13. Audit each module for capability (filesystem,
  network, process, environment, time, random, task spawning).
  Cross-reference against M7 fixture usage
  (`crates/toron/src/lib.rs:1080+`). Output:
  - `(module, capability, action)` table.
  - Minimum primitive set the V1 allow-list installs.
  - Any M7-used module with forbidden capability triggers
    council readout — likely the fixture needs rewriting, not
    the allow-list extending.
- **P0.3: Memory enforcement path decision.** Audit Rune 0.13's
  `Vm` API for a stable per-execution allocation limiter.
  Output: one of:
  - **(a)** Rune-native cap surface confirmed; record the API.
  - **(b)** Rune-native cap unavailable; commit to helper-process
    isolation with OS memory limit (`prlimit` / `setrlimit` /
    Linux cgroup). The helper-process protocol is local +
    private + script + config + result-or-failure
    serialization.

  **The "fuel-only, defer memory to V1.x" path is rejected**
  (Codex iter-0 framing). Phase 4 cannot start until P0.3 picks
  (a) or (b).
- **P0.4: Budget defaults calibration.** Measure M7 fixture
  consumption under permissive budgets:
  - `fuel_budget`: V1 default ≥10× the largest observed M7
    fixture consumption. Council seed: 1,000,000 ops; P0.4
    measures.
  - `memory_budget`: V1 default similarly headroom-padded against
    M7-observed allocation peak.
  - `max_source_bytes`: ~64 KiB unless P0.2 reveals M7 fixtures
    use larger sources.
  - `wall_timeout`: 5 seconds default — generous enough to catch
    fuel implementation bugs without making infinite-loop tests
    slow in CI. P0.4 may tune.
- **P0.5: Stable label snapshot.** Pin the wire-reason label
  strings exactly:
  `RuneSandbox::FuelExhausted`,
  `RuneSandbox::MemoryExhausted`,
  `RuneSandbox::CapabilityDenied`,
  `RuneSandbox::SourceTooLarge`,
  `RuneSandbox::Timeout`,
  `RuneSandbox::InvalidEntrypoint`,
  `RuneSandbox::InternalSandboxError`.
  These become test assertion targets in Phase 5.
- **P0.6: Capability negative-test catalog.** Enumerate
  forbidden-capability negative tests for Phase 2:
  - filesystem read / write
  - process spawn
  - network access (any `std::net::*`)
  - environment access (`std::env::var`)
  - stdin / stdout / stderr access
  - host clock (`std::time::*`)
  - random (any host RNG)
  - thread / task spawning
  Each gets a test asserting `CapabilityDenied { CapabilityCategory }`
  is returned with the correct category.
- **P0.7: Rune-specific profiler-enabled overhead measurement.**
  **First ADR cycle to actually execute a profiler note** for
  ADR-007-relevant code paths. Note (Codex review): the existing
  `crates/toron/scripts/bench/run_runtime_filter_perft.sh
  --samply` profiles the distributed inner join path, not
  `RuneEngine` — a passing profile from that harness does not
  exercise the M7 authoring surface. P0.7 needs a
  **Rune-specific** profile target.
  Workflow:
  ```
  crates/toron/scripts/bench/enable_profiling.sh --apply
  # Profile a Rune M7 fixture under the new minimal context.
  # Two viable options:
  #   (a) Direct samply on the existing M7 test binary:
  samply record -o /tmp/toron_adr007_samply.json.gz -- \
    cargo test -p toron --release rune_authoring_surface_builds_count_plan -- --nocapture
  #   (b) A small dedicated Rune bench (Phase 0 may add one in
  #       crates/toron/benches/rune_sandbox.rs if cargo-test
  #       wraparound noise dominates the M7 fixture work).
  crates/toron/scripts/bench/enable_profiling.sh --revert
  ```
  Output: per-script wall-time vs current implementation (target
  ≤10% overhead per ADR hard gate 22), profile file reference,
  dominant hot path note **scoped to `RuneEngine::execute_script`
  + downstream allow-list / fuel / context construction** (not
  distributed-join code). Carries forward zero "permissions
  blocked" caveat from prior ADRs.

### Phase 1 — Sandbox config + failure taxonomy

Files:
- `crates/toron/src/rune_engine.rs` — introduce
  `RuneSandboxConfig` + `RuneSandboxViolation` enum +
  `CapabilityCategory` enum.
- `crates/toron/src/error.rs` — add `RuneSandbox` variant
  carrying the kind enum.
- `crates/toron/tests/rune_sandbox_test.rs` (new) — config
  default tests, source-size enforcement, taxonomy round-trip.

Work:

- **A. `RuneSandboxConfig` struct.**

  ```rust
  pub struct RuneSandboxConfig {
      pub fuel_budget: u64,
      pub memory_budget: u64,
      pub wall_timeout: Duration,
      pub max_source_bytes: usize,
      pub allow_profile: AllowProfile,
  }

  pub enum AllowProfile {
      ToronAuthoring,
  }
  ```

  V1 default constructor (`RuneSandboxConfig::v1_default()`)
  derives values from Phase 0 P0.4 calibration.

- **B. `RuneSandboxViolation` enum + `CapabilityCategory` enum +
  `FrontendError::RuneSandbox` variant.**

  ```rust
  pub enum RuneSandboxViolation {
      FuelExhausted { fuel_consumed: u64, fuel_budget: u64 },
      MemoryExhausted { bytes_used: u64, bytes_budget: u64 },
      CapabilityDenied { capability: CapabilityCategory },
      SourceTooLarge { bytes: usize, budget: usize },
      Timeout { elapsed: Duration },
      InvalidEntrypoint { detail: String },
      InternalSandboxError { detail: String },
  }

  pub enum CapabilityCategory {
      Filesystem,
      Process,
      Network,
      Environment,
      Time,
      Random,
      Threading,
      Unknown,
  }

  FrontendError::RuneSandbox {
      kind: RuneSandboxViolation,
      message: String,
  }
  ```

  Stable wire-reason label string per kind (P0.5 pinning).
  `Display` impl produces coordinator-facing message — stable,
  non-leaky.

- **C. `execute_script` signature update.** Replace
  `execute_script(script, _fuel)` with
  `execute_script(script, &config)` at `rune_engine.rs:190`. The
  existing caller at `lib.rs:1091` updates to construct a
  `RuneSandboxConfig`. A transitional `execute_script(script,
  fuel)` compatibility wrapper is acceptable if it constructs a
  config from the fuel arg and the V1 defaults for the other
  fields — but the dead `_fuel` parameter cannot remain.

- **D. `max_source_bytes` enforcement.** Before `Source::new` at
  `rune_engine.rs:197`, check `script.len() <=
  config.max_source_bytes`. On overflow, return
  `FrontendError::RuneSandbox { kind:
  RuneSandboxViolation::SourceTooLarge { bytes: script.len(),
  budget: config.max_source_bytes }, .. }`.

Tests (in `rune_sandbox_test.rs`):

- `rune_sandbox_config_v1_default_matches_phase_0` — verify
  production defaults match Phase 0 P0.4 pinning.
- `source_too_large_rejected_before_compile` — script bigger
  than `max_source_bytes` returns `SourceTooLarge` without
  invoking `Source::new`. **Hard gate 9.**
- `rune_sandbox_violation_taxonomy_round_trips` — serialize +
  deserialize each variant; assert stable wire labels.
- `capability_category_round_trips` — every variant including
  `Unknown` round-trips with a stable wire label.

Gate:

```bash
cargo test -p toron rune_sandbox_config
```

### Phase 2 — Allow-list context + architecture guard

Files:
- `crates/toron/src/rune_engine.rs` — replace
  `Context::with_default_modules()` with `Context::new()` +
  explicit registrations.
- `crates/toron/tests/architecture_test.rs` — source-level grep
  guard for `with_default_modules` absence.
- `crates/toron/tests/rune_sandbox_test.rs` — capability negative
  tests.

Work:

- **A. Replace context construction.** At `rune_engine.rs:178`:

  ```rust
  let mut context = Context::new();
  install_minimal_primitives(&mut context, &config.allow_profile)
      .map_err(rune_compile_error)?;
  context.install(create_toron_module().map_err(rune_compile_error)?)
      .map_err(rune_compile_error)?;
  ```

  `install_minimal_primitives` is a new private helper.
  Profile-keyed so V1.x can add stricter / looser profiles
  without function signature churn. V1 ships only
  `AllowProfile::ToronAuthoring`. Phase 0 P0.2 derives the
  minimal-primitive set against M7 fixtures.

- **B. Map sandbox violations to category enum.** When the Rune
  VM raises a missing-function error (script called something
  not in the allow-list), translate to `FrontendError::RuneSandbox
  { kind: RuneSandboxViolation::CapabilityDenied { capability:
  <CapabilityCategory> }, .. }`. The category mapping is a
  private helper that classifies missing functions by their Rune
  module path (e.g., `std::fs::*` → `Filesystem`, `std::net::*` →
  `Network`); unknown paths produce `Unknown` on the public enum.
  The raw Rune module path is captured only in an internal
  `tracing::debug!` line (operator-side diagnostics), never
  attached to the public `CapabilityCategory` variant — keeping
  hard gate 17 (non-leaky coordinator-facing surface) honest.

- **C. Architecture guard.** Add a grep guard in
  `tests/architecture_test.rs`:
  `architecture_guard_rune_context_is_allow_listed` — asserts
  `crates/toron/src/rune_engine.rs` does not contain
  `Context::with_default_modules`. ADR 007 hard gate 19.

Tests (capability negatives — Phase 0 P0.6 catalog; each asserts
the correct `CapabilityCategory`):

- `sandbox_blocks_filesystem_read` — **Hard gate 10.**
- `sandbox_blocks_filesystem_write`.
- `sandbox_blocks_process_spawn` — **Hard gate 11.**
- `sandbox_blocks_network_access` — **Hard gate 12.**
- `sandbox_blocks_environment_access` — **Hard gate 13.**
- `sandbox_blocks_time_access` — **Hard gate 14.**
- `sandbox_blocks_random_access` — **Hard gate 14.**
- `sandbox_blocks_thread_spawn`.
- `sandbox_blocks_stdin_read`.
- `allow_list_arithmetic_works`.
- `allow_list_toron_module_works` — all 5 Toron module functions
  + associated functions remain callable. **Hard gate 6.**
- `deterministic_execution_preserved` — two runs of the same
  allowed script produce byte-identical `LogicalPlan`. **Hard
  gate 16.**
- `architecture_guard_rune_context_is_allow_listed` — grep
  guard. **Hard gate 19.**

Gate:

```bash
cargo test -p toron rune_sandbox
cargo test -p toron --test architecture_test
```

### Phase 3 — Fuel enforcement

Files:
- `crates/toron/src/rune_engine.rs`.

Work:

- **A. Wire fuel via Phase 0 P0.1 API.** At `rune_engine.rs:216`:

  ```rust
  vm.set_fuel(Some(config.fuel_budget));
  // or the Phase 0 P0.1-confirmed Rune 0.13 equivalent
  ```

  Map Rune-VM fuel-exhausted errors to
  `RuneSandboxViolation::FuelExhausted { fuel_consumed,
  fuel_budget }`. A fuel-exhausted execution must not produce a
  partial `LogicalPlan`.

- **B. Wall-timeout (yielding paths only).** Wrap
  `execution.async_complete().await` (lines 222-226) in
  `tokio::time::timeout(config.wall_timeout, ...)`. Map elapsed
  to `RuneSandboxViolation::Timeout { elapsed }`. **Limitation
  (Codex review):** Rune 0.13's `async_complete()` enters
  `async_resume()` which calls `vm.run()` synchronously until a
  yield (`rune-0.13.4/src/runtime/vm_execution.rs:209`). A
  CPU-bound Rune loop with broken fuel won't be preempted by
  Tokio timeout under path (a). The timeout reliably catches
  cooperative-yield paths and (under path b) helper-process
  IPC stalls. Path (b) provides the real broken-fuel backstop:
  parent SIGKILLs the helper after `wall_timeout`. Phase 0 P0.3
  carries this tradeoff explicitly.

- **C. Update `rune_runtime_error` helper** to distinguish fuel
  exhaustion + timeout from genuine script-logic errors before
  flattening. Genuine script-logic errors stay in `RuneRuntime`.

Tests:

- `infinite_loop_exhausts_fuel` — `loop { }` script returns
  `FuelExhausted` with consumed/budget reported. **Hard gate 7.**
  Wall-clock-bounded test with assertive timeout so a broken
  fuel implementation can't hang CI.
- `recursive_script_exhausts_fuel` — deeply recursive script
  hits fuel exhaustion before stack overflow (or stack overflow
  maps cleanly to a sandbox error, never panics).
- `wall_timeout_fires_as_typed_timeout_on_yielding_script` —
  defense-in-depth test for the **yielding** path: fixture script
  awaits a long host async sleep (registered as an allow-listed
  capability for the test) so the VM actually reaches a yield
  point. Configure unreasonably high fuel + low wall timeout;
  assert `Timeout` fires, not `FuelExhausted` and not
  `RuneRuntime`. Documented in the test body: this confirms only
  the yielding-path belt; CPU-loop preemption is covered by the
  Phase 4 helper-process SIGKILL test, not by
  `tokio::time::timeout`. **Hard gate 15.**

Gate:

```bash
cargo test -p toron rune_sandbox_fuel
```

### Phase 4 — Memory enforcement (path picked by Phase 0 P0.3)

**Files (path (a), Rune-native cap):**
- `crates/toron/src/rune_engine.rs`.

**Files (path (b), helper-process isolation):**
- `crates/toron/src/rune_engine.rs` — parent-side helper
  protocol.
- `crates/toron/src/rune_engine/helper.rs` (new) — helper-side
  entry point.
- `crates/toron/src/bin/rune_sandbox_helper.rs` (new) — helper-
  process binary entry point. (Alternative: `src/main.rs` argv
  branch under a feature flag. Phase 4 picks.)

Work (path (a)):

- Wire Rune-native memory cap to `config.memory_budget`. Map
  exhaustion to `RuneSandboxViolation::MemoryExhausted { bytes_used,
  bytes_budget }`.

Work (path (b)):

- Helper-process protocol: parent serializes `(script, config)`
  via pipe or short-lived file; helper applies `setrlimit` for
  memory (and CPU); helper executes script in-process; helper
  serializes result (`LogicalPlan` or `RuneSandboxViolation`)
  back to parent.
- OS-side memory limit picked via `setrlimit(RLIMIT_AS)` on
  Linux. Abnormal helper exit due to OS memory limit (signal 9 /
  SIGKILL from OOM killer) maps to `MemoryExhausted`, **not**
  `InternalSandboxError` (load-bearing — failure mode is
  predictable; taxonomy must reflect it).
- **Parent-side wall-timeout SIGKILL** (path (b)'s broken-fuel
  backstop). Parent process tracks helper-process start time;
  if helper hasn't returned within `config.wall_timeout`, parent
  sends SIGKILL. Helper abnormal-exit from this signal maps to
  `RuneSandboxViolation::Timeout { elapsed }` — distinguishable
  from `MemoryExhausted` by exit reason (parent recorded the
  SIGKILL it initiated). This is the **real** defense-in-depth
  against a broken fuel implementation under path (b); Tokio
  timeout alone cannot preempt the helper's synchronous VM.
- Protocol stays local + private to Toron; no wire surface
  exposure.

Tests:

- `memory_bomb_terminates_with_typed_error` — script allocating
  a growing `Vec` exceeds budget; returns `MemoryExhausted` with
  bytes-used/budget reported. **Hard gate 8.** Wall-clock-bounded
  test.
- `memory_budget_default_matches_phase_0`.
- (Path (b) only)
  `helper_process_oom_kill_maps_to_memory_exhausted` — explicit
  test that SIGKILL-from-OOM doesn't surface as
  `InternalSandboxError`.

Gate:

```bash
cargo test -p toron rune_sandbox_memory
```

### Phase 5 — Error propagation + distributed wire stability

Files:
- `crates/toron/src/error.rs` (if `FrontendError ↔ TaskFailureKind`
  mapping lives here).
- `crates/toron/src/server/mod.rs` only if Rune execution paths
  cross task dispatch in the current code (audit reveals).
- `crates/toron/tests/architecture_test.rs` — `TaskFailureKind`
  7-variant guard (existing; just confirm it holds).

Work:

- **A. Audit cross-wire paths.** Does any current code path map a
  Rune execution failure into a `TaskFailureKind`? If yes,
  Phase 5 implements the reason-string mapping
  (`TaskFailureKind::TaskFailed { retryable: false, reason:
  "RuneSandbox::<label>" }`). If no, Phase 5 is empty (Rune
  failures stay in `FrontendError`).
- **B. Confirm `TaskFailureKind` shape unchanged.** Run the
  existing architecture guard (no new variants added).
- **C. Non-retryable semantics.** Sandbox failures are
  deterministic per script — retrying produces the same failure.
  Map MUST set `retryable: false`.

Tests:

- `task_failure_taxonomy_has_seven_variants` — existing guard,
  reconfirmed. **Hard gate 18.**
- `rune_sandbox_failure_is_terminal_not_retryable` — if any
  wire-crossing path exists, assert mapping produces
  `retryable: false`.

Gate:

```bash
cargo test -p toron task_failure_taxonomy_has_seven_variants
cargo test -p toron rune_sandbox_failure_is_terminal
```

### Phase 6 — Verification + promotion

Files:
- Optional Phase 0 audit promotion to
  `plans/impl/007_rune_sandbox_audit.md` if council wants a
  permanent record. Otherwise Phase 0 outputs live inline in
  this plan's audit section.

Work:

- Run the full hard-gate suite:
  ```
  cargo fmt -p toron --check
  cargo check --workspace
  cargo test -p toron
  cargo test -p toron --test architecture_test
  cargo test -p toron --test distributed_process_test
  cargo test -p toron --test wendelon_storage_execution_test
  cargo test -p toron --test runtime_filter_pushdown_test
  cargo test -p toron --test hash_join_fast_path_parity_test
  cargo test -p toron --test rune_sandbox_test
  cargo test --workspace
  cargo bench -p toron --bench distributed_inner_equi_join --no-run
  cargo bench -p toron --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only
  git diff --check
  ```
  Notes:
  - `rune_sandbox_test` is the new Phase 1–5 test file.
  - Architecture guards G1-G18 must pass unchanged plus the new
    `architecture_guard_rune_context_is_allow_listed`
    (hard gate 19) and the existing
    `task_failure_taxonomy_has_seven_variants` (hard gate 18).
  - All prior ADRs' hard gates must pass byte-identically
    (hard gate 4).
  - M7 fixtures embedded in `cargo test -p toron`
    (`lib.rs:1080+`) cover the backward-compat gate (hard gate 5).
  - `distributed_inner_equi_join --quick` confirms non-Rune paths
    have zero regression (hard gate 21).
- **Phase 0 P0.7 profile capture (first ADR cycle to execute
  this).** Capture `samply` profile of an M7-class script under
  the new context via the Rune-specific profiling target
  (P0.7's `cargo test rune_authoring_surface_builds_count_plan`
  command, or a dedicated `crates/toron/benches/rune_sandbox.rs`
  if Phase 0 audit reveals cargo-test wraparound dominates).
  **Note:** do not substitute the ADR 006
  `run_runtime_filter_perft.sh --samply` workflow — that
  profiles distributed-join paths, not Rune. Record: profiler
  command, machine note, dominant hot path scoped to
  `RuneEngine::execute_script` + allow-list / fuel / context
  construction, per-script wall-time vs current implementation
  (hard gate 22 target: ≤10% overhead).

Suggested commit slices:

1. `chore: adr 007 prep — phase 0 rune sandbox audit`
2. `plan: adr 007 rune sandbox enforcement`
3. `feat: adr 007 phase 1 — rune sandbox config + frontend error
   taxonomy + source-size guard`
4. `feat: adr 007 phase 2 — rune allow-list context +
   architecture guard + capability negative tests`
5. `feat: adr 007 phase 3 — rune fuel enforcement + wall-timeout
   outer belt`
6. `feat: adr 007 phase 4 — rune memory enforcement` (path (a)
   Rune-native or path (b) helper-process per Phase 0)
7. `feat: adr 007 phase 5 — sandbox-failure distributed wire
   mapping` (may be empty/no-op if Phase 5 audit reveals no
   cross-wire path)
8. `feat: adr 007 phase 6 — verification + promotion`

`feat:` vs `fix:` note: Phase 3 (fuel) and Phase 4 (memory)
could fairly be `fix:` since both close M7-recorded gaps. Use
`feat:` because the typed taxonomy + config struct are genuinely
new contract surface.

If hard gates pass, promote. If any of gates 7-15 fail (a
forbidden capability isn't actually blocked, or fuel/memory/
timeout doesn't fire), block on debugging — that's a real
sandbox escape and ADR 007 doesn't ship.

## Files touched

Implementation:

- `crates/toron/src/rune_engine.rs` — `RuneSandboxConfig` +
  `RuneSandboxViolation` + `CapabilityCategory` +
  `Context::new()` + `install_minimal_primitives(profile)`
  helper; fuel re-enablement at line 216; wall-timeout outer
  belt; `max_source_bytes` pre-compile guard; sandbox-violation
  + fuel-exhausted + timeout error translation;
  `execute_script(script, &config)` signature update.
- `crates/toron/src/error.rs` (wherever `FrontendError` lives)
  — one new `RuneSandbox { kind, message }` variant.
- `crates/toron/src/lib.rs:1091` — update test fixture caller to
  construct a `RuneSandboxConfig`.

Tests:

- `crates/toron/tests/rune_sandbox_test.rs` (new) — Phase 1
  through Phase 5 tests.
- `crates/toron/tests/architecture_test.rs` (modified) — new
  `architecture_guard_rune_context_is_allow_listed` grep guard
  (hard gate 19); existing
  `task_failure_taxonomy_has_seven_variants` reconfirmed (hard
  gate 18).

Phase 4 conditional (Phase 0 P0.3 path (b) — helper-process):

- `crates/toron/src/rune_engine/helper.rs` (new) — helper-side
  protocol.
- `crates/toron/src/bin/rune_sandbox_helper.rs` (new) — helper
  binary entry point. (Or `src/main.rs` argv branch; structural
  choice TBD at Phase 4.)

Optional Phase 0 audit (if promoted as a permanent record):

- `crates/toron/plans/impl/007_rune_sandbox_audit.md` (new).

**Not expected:**
- No Wendelon files.
- No `distributed/fragment.rs`, `distributed/scheduler.rs`,
  `distributed/task.rs` changes.
- No storage / scan files.
- No join executor changes outside incidental imports.
- No `MessageKind`, IR variant, `BudgetDimension`, task
  telemetry field, or `TaskFailureKind` variant changes (per
  ADR 003 7-variant guard).

## Out of scope (V1.x or later)

- Per-tenant fuel / memory budgets (L-3 Multi-tenant governance
  ADR).
- Row-level Rune UDF execution (worker-side script execution —
  substantial new ADR).
- WASM runtime migration.
- Time / random access in scripts.
- New native-function allow-list additions (each is a separate
  ADR with threat-model review).
- Subprocess sandbox via full seccomp + rlimit (helper-process
  isolation as path (b) is lighter and sufficient).
- JIT / Cranelift specialization (Brainstorm 001).
- Coordinator-side fairness across submissions.

## Risks and mitigations

- **R1: Rune version drift.** A future Rune upgrade changes the
  default-module set, breaks the minimal-primitive registration
  API, or introduces a new sandbox-escape vector.
  *Mitigation:* allow-list is enforced by `Context::new()` — new
  default modules don't get auto-loaded. Capability negative
  tests (P0.6) catch regressions on Rune upgrade. Rune version
  bumps go through threat-model review per the "no new native
  functions without review" rule.

- **R2: M7 fixture breakage.** The minimal-primitive context
  excludes a primitive the M7 fixtures actually need.
  *Mitigation:* Phase 0 P0.2 audits the fixtures first; the
  allow-list is derived from observed usage, not guessed.
  `m7_test_fixtures_unchanged` is a hard gate via existing
  `cargo test -p toron`.

- **R3: Capability blocked but not detected.** A forbidden
  capability is registered through a path the negative tests
  don't cover.
  *Mitigation:* P0.2 audits the full default-module set; every
  default module either appears on the allow-list (with
  rationale) or appears in the negative-test catalog. No module
  is "implicitly excluded."

- **R4: Fuel-exhaustion error variant doesn't match Rune VM
  output.** The Rune VM's fuel-exhausted error type changes
  between versions or doesn't map cleanly to a typed variant.
  *Mitigation:* Phase 0 P0.1 confirms the mapping against the
  Rune version we use. A thin adapter layer translates VM errors
  to `RuneSandboxViolation` variants; the adapter is testable
  in isolation.

- **R5: Memory cap not exposed by Rune VM.** Phase 0 P0.3
  determines Rune doesn't expose a stable memory-cap surface.
  *Mitigation:* Phase 0 P0.3 commits to helper-process isolation
  (path b) before Phase 4 starts. **Fuel-only fallback is
  rejected** (Codex iter-0 framing) — memory bomb that fits
  within fuel can still OOM host.

- **R6: Default-module audit incomplete.** Phase 0 P0.2 misses a
  default-module capability and ships V1 with the capability
  quietly available.
  *Mitigation:* `Context::new()` (no defaults) means if P0.2
  misses something, test fixtures break loud rather than
  capability silently available. Allow-list failure mode is
  "test fails," not "capability silently exposed."

- **R7: Error messages leak host paths.** Sandbox-violation
  messages include Rune source line numbers or host paths that
  an attacker could use to map the host environment.
  *Mitigation:* hard gate 20 — error messages are stable and
  non-leaky. `CapabilityCategory` enum abstracts over raw Rune
  paths. Phase 2 tests assert error message format doesn't
  include host-derivable strings.

- **R8: Fuel-budget too low breaks legitimate scripts.** The V1
  default is set lower than some legitimate user scripts need,
  causing false-positive fuel exhaustion.
  *Mitigation:* Phase 0 P0.4 derives the default from measured
  M7-fixture consumption with 10× headroom. The fuel budget is
  per-call configurable; the default is for the common case.
  Future authoring-surface ADRs can raise it with justification.

- **R9: Backward-compat test surface incomplete.** M7's test
  fixtures don't exercise every Rune primitive that real user
  scripts use, and Phase 0 derives an allow-list that's too
  narrow.
  *Mitigation:* M7 fixtures are the current authoring contract —
  anything not exercised by them isn't part of the V1 contract.
  Real user scripts needing primitives outside the V1 allow-list
  are future authoring-surface ADRs, not ADR 007 oversights.

- **R10: Capability-category misclassification.** A missing
  function gets mapped to the wrong `CapabilityCategory` (e.g.,
  a network-related function classified as `Unknown`).
  *Mitigation:* P0.2 audit enumerates the default modules; the
  classification helper has explicit per-module mappings. The
  `Unknown` variant catches unclassified paths on the public
  surface; the raw module path is recorded only in internal
  `tracing` debug logs so operators can diagnose drift without
  the path leaking to the coordinator-facing taxonomy.

- **R11: Helper-process protocol fragility** (path (b) only). The
  IPC channel between parent and helper has race conditions or
  message-format ambiguity.
  *Mitigation:* protocol stays local + private (no wire
  exposure); short-lived (one helper per script, no long-running
  state); strict serialization (Phase 4 picks the format).
  `helper_process_oom_kill_maps_to_memory_exhausted` test covers
  the most likely abnormal-exit class.

- **R12: Wall-timeout masks broken fuel implementation.** Fuel
  enforcement silently fails and `wall_timeout` covers the
  symptom; CI passes but the underlying issue is the missing
  fuel.
  *Mitigation:* `wall_timeout_fires_as_typed_timeout_on_yielding_script`
  is a defense-in-depth test — it explicitly verifies timeout
  fires only when fuel doesn't, and only on the yielding path.
  The infinite-loop hard gate (gate 7) asserts `FuelExhausted`,
  not merely `Timeout`. If gate 7 fails but gate 15 passes, the
  fuel implementation is broken. CPU-bound preemption (broken
  fuel + non-yielding script) is covered separately by the
  Phase 4 helper-process SIGKILL test — `tokio::time::timeout`
  cannot mask that path because it does not fire on a
  synchronous `vm.run()`.

## Definition of done

- Phase 0 audit lands: Rune 0.13 budget API confirmed (P0.1),
  default-module inventory + M7 fixture dependency cross-
  referenced (P0.2), memory enforcement path chosen (P0.3 —
  Rune-native cap or helper-process), budget defaults calibrated
  (P0.4), stable wire-reason labels pinned (P0.5), capability
  negative-test catalog enumerated (P0.6),
  profiler-enabled overhead measurement captured (P0.7 — first
  ADR cycle to actually execute this).
- `RuneSandboxConfig` struct lands with all five fields
  (`fuel_budget`, `memory_budget`, `wall_timeout`,
  `max_source_bytes`, `allow_profile`).
- `Context::new()` + `install_minimal_primitives(profile)` +
  Toron module installed; `Context::with_default_modules()`
  removed; architecture grep guard confirms.
- Fuel enforcement re-enabled at `rune_engine.rs:216`.
- Wall-timeout outer belt wired via `tokio::time::timeout`.
- `max_source_bytes` pre-compile guard wired before
  `Source::new`.
- **Memory enforcement landed as V1 hard requirement** (path
  (a) Rune-native or path (b) helper-process per Phase 0 P0.3).
- `FrontendError` extended with single `RuneSandbox { kind,
  message }` variant carrying the 7-label `RuneSandboxViolation`
  enum + `CapabilityCategory` enum.
- `execute_script(script, &config)` consumes the config; no
  dead-input `_fuel` parameter remains.
- M7 test fixtures pass byte-identically under the new context.
- All capability negative tests pass (filesystem read/write,
  process spawn, network access, environment access, time
  access, random access, thread spawn, stdin) with correct
  `CapabilityCategory` returned.
- Infinite-loop script terminates with `FuelExhausted` within
  bounded wall time.
- Memory-bomb script terminates with `MemoryExhausted`; host
  process remains healthy.
- Oversized-source script rejected with `SourceTooLarge` before
  compile.
- Wall-timeout fires as `Timeout` (defense-in-depth test).
- `TaskFailureKind` 7-variant shape preserved (existing guard
  reconfirmed).
- Sandbox failures crossing the task wire (if any current path)
  map to `TaskFailureKind::TaskFailed { retryable: false,
  reason: "RuneSandbox::<label>" }`.
- Every ADR 002 / 003 / 004 / 005 / 006 hard gate passes
  byte-identically. Architecture guards G1-G18 unchanged plus
  the new `architecture_guard_rune_context_is_allow_listed`.
- Per-script wall-time overhead within 10% of current
  implementation (Phase 0 P0.7 profile measurement).
- Zero regression on non-Rune query paths (distributed bench
  quick smoke confirms).
- Coordinator-facing error messages are stable and non-leaky
  (no host paths, no VM source positions, no internal state).
- Roadmap updated to record ADR 007 promotion + reframe ADR
  008 candidates per the resolved Rune-in-V1 path.
