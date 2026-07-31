# ADR 007: Toron Rune Sandbox Enforcement

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/007_rune_sandbox_enforcement.md`

## Status

Promoted to `plans/adr/`. Implementation pending. No bench-results
artifact required — ADR 007 is a safety contract, not a performance
ADR; Phase 0 audit suffices.

Builds on three WIP iteration rounds across the council
(`plans/wip/{claude,codex,gemini}_topics_007.md`,
`plans/wip/{claude,codex,gemini}_{adr,impl}_007.md`). Three-way
final convergence reached by iter-2; iteration artifacts retired
(git holds the narrative). Codex's `RuneSandboxConfig` 5-field
struct, "memory enforcement as V1 hard requirement" framing, and
the 7-label `RuneSandboxViolation` taxonomy with stable
wire-reason labels; Gemini's brevity discipline and the non-Rune
regression hard-gate framing; Claude's `CapabilityDenied` category
enum, conditional architecture guard for `Context::with_default_modules`
absence, and three-way alignment summary are the load-bearing
antecedents.

This ADR is a **narrow safety contract** that closes the launch
blocker recorded in M7's authoring-surface impl plan
(`plans/impl/007_rune_authoring_surface.md:42` — "Sandbox Fuel:
Fuel is currently disabled in V0 due to dependency version
constraints; to be re-enabled in V1"). Every ADR 002 / 003 /
004 / 005 / 006 hard gate and architecture guard G1-G18 must
continue to pass byte-identically.

Selected by the user via the iter-3 ADR 007 topics convergence
(L-2 path of Codex's four-path rule: "V1 exposes user-supplied
Rune → ADR 007 = Rune sandbox enforcement"). The path was chosen
over launch-driven L-1 (outer joins) and over the still-pending
evidence-driven RF-* runtime-filter extensions.

It does **not** decide: row-level Rune UDF execution semantics
(out of scope — Rune is frontend-scoped in V1; worker-side UDFs
are a substantial future ADR), JIT / Cranelift / native-code
specialization (Brainstorm 001), outer / semi / anti joins (next
candidate ADR 008 per the roadmap), multi-tenant per-tenant Rune
budgets (L-3 multi-tenant governance ADR when workload evidence
lands), V1.x runtime-filter extensions (still gated on production
Wendelon row), new `TaskFailureKind` wire variants (ADR 003
7-variant guard preserved), or any cross-ring contract change.

## Scale boundary

**Primary: process-internal** — the sandbox constrains Rune
compilation and VM execution inside the Toron process hosting the
Rune engine at `crates/toron/src/rune_engine.rs`.

**Secondary: frontend / admission boundary only** — today's Rune
engine lowers a script into a `LogicalPlan` *before* distributed
tasks exist. The same sandbox posture must apply if future Rune
execution moves into worker-side UDF kernels, but that future
feature is a new ADR.

**Explicit non-boundaries:** no Wendelon contract, no cross-ring
descriptor, no distributed stage-order change, no new scheduler
surface, no new wire envelope, no relational operator semantics,
no `TaskFailureKind` variant change (ADR 003 7-variant guard
preserved).

ADR 007 is the **second ADR** drafted under the scale-boundary
structural convention adopted in ADR 006, and the first with a
fully self-contained primary boundary (no secondary cross-ring or
transport touches like ADR 006 had). The orthogonality is
load-bearing — process-internal is the one scale boundary not
touched by ADR 002–006, so zero scope overlap and zero
architectural conflict.

## V1 contract summary

- **Decision shape.** Replace
  `Context::with_default_modules()` with an explicit allow-list,
  re-enable fuel enforcement, enforce memory bounds (Rune-native
  cap or helper-process isolation per Phase 0 audit), add
  `max_source_bytes` pre-compile guard, add `wall_timeout` outer
  safety belt, pin the typed sandbox-violation taxonomy.
  User-supplied Rune cannot reach filesystem / network / process
  / environment / clock / random / thread-spawning capabilities;
  cannot consume unbounded CPU; cannot consume unbounded memory;
  cannot OOM the host process; cannot DoS the parser with
  unbounded source input.

- **`RuneSandboxConfig` struct.** Single carrier for the sandbox
  knobs:

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

  V1 ships one allow profile (`ToronAuthoring`); the field exists
  so V1.x can add stricter / looser profiles without contract
  churn. Field defaults are derived from Phase 0 calibration
  against M7 fixtures (P0.4).

- **Minimal context (allow-list, not deny-list).** Replace
  `Context::with_default_modules()` at `rune_engine.rs:178` with
  `Context::new()` + explicit registration of:
  - `create_toron_module()` (existing — `col`, `lit`,
    `time_bucket`, `linear_regress`, `approx_quantile` + associated
    functions on `RuneDataFrame` / `RuneExpr` / `ToronContext`).
  - The minimal Rune primitive set needed for the M7 DataFrame
    API. Specific list pinned by Phase 0 audit (P0.2) against M7
    test fixtures.
  - **Nothing else.** No `io`, `fs`, `process`, `net`, `env`,
    `time`, `random`, thread/task spawning, or any host-IO
    capability.

- **Native-function allow-list.** Pinned at runtime by the
  registration call sequence (matches existing
  `create_toron_module()` pattern). The V1 set is the **5
  functions + associated functions** in `create_toron_module()`;
  extension is a separate narrow ADR with its own threat-model
  review.

- **Fuel enforcement re-enabled.** At `rune_engine.rs:216`, the
  commented `vm.set_fuel(Some(_fuel))` becomes the active fuel
  hook. Phase 0 P0.1 confirms the exact Rune 0.13 API (the call
  may have moved away from `Vm::set_fuel`). Default
  `fuel_budget` pinned by Phase 0 P0.4 with M7-fixture headroom.

- **Memory enforcement as V1 hard requirement.** Every script
  receives a finite memory budget. A fuel-only sandbox is **not
  shippable** for V1 Rune exposure — a memory bomb that fits
  within fuel can still OOM the host. Phase 0 P0.3 chooses one of
  two paths before implementation proceeds:
  - **(a) Rune-native per-execution allocation limiter** if Rune
    0.13 exposes a stable cap surface; or
  - **(b) Local helper-process isolation** with OS memory limit
    (`setrlimit(RLIMIT_AS)` on Linux). The protocol is local and
    private to Toron; serializes only script + config + result
    (`LogicalPlan` or `RuneSandboxViolation`); abnormal helper
    exit due to OS memory limit maps to `MemoryExhausted`, not
    `InternalSandboxError`.

- **`max_source_bytes` pre-compile guard.** Script length is
  checked **before** `Source::new` at `rune_engine.rs:197`.
  Default ~64 KiB (Phase 0 P0.4 tunes). Prevents parser DoS via
  unbounded source input.

- **`wall_timeout` (catches yielding paths only).**
  `tokio::time::timeout` wraps `execution.async_complete().await`
  and fires as `RuneSandbox::Timeout` (not `RuneRuntime`). **Real
  limitation:** Rune 0.13's `async_complete()` enters
  `async_resume()` which calls `vm.run()` *synchronously* until
  a halt / await / yield (see
  `rune-0.13.4/src/runtime/vm_execution.rs:209`). The Tokio
  timeout only gets control at a cooperative yield point — a
  fully CPU-bound Rune loop with broken fuel will **not** be
  preempted. `wall_timeout` reliably catches:
  - cooperative-yield paths (genuine async script logic, if any);
  - helper-process IPC stalls (path b);
  - script paths that yield to the runtime between operations.

  **It does not reliably catch a fuel-broken CPU loop.** The
  true broken-fuel backstop is path (b) helper-process
  isolation — the parent can SIGKILL the helper after a
  deadline. Path (a) Rune-native cap has weaker
  defense-in-depth here; if fuel breaks under path (a), the
  symptom is a hung host thread, not a typed `Timeout`. Phase 0
  P0.3 path choice carries this tradeoff explicitly. Default
  5 seconds (Phase 0 P0.4 tunes).

- **Typed sandbox failure taxonomy.** Single
  `FrontendError::RuneSandbox { kind: RuneSandboxViolation,
  message: String }` variant, with `RuneSandboxViolation` enum
  having seven labels:

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
  ```

  Stable wire-reason labels (Phase 0 P0.5 pinning):
  `RuneSandbox::FuelExhausted`, `RuneSandbox::MemoryExhausted`,
  `RuneSandbox::CapabilityDenied`, `RuneSandbox::SourceTooLarge`,
  `RuneSandbox::Timeout`, `RuneSandbox::InvalidEntrypoint`,
  `RuneSandbox::InternalSandboxError`.

- **No `TaskFailureKind` change (ADR 003 7-variant guard
  preserved).** Rune execution lives in the frontend, before
  distributed task dispatch — sandbox failures normally surface
  as `FrontendError::RuneSandbox` at script-submission time, not
  as worker-task failures. **If a sandbox failure does cross the
  task wire** (a future code path the audit reveals or that
  emerges later), it maps to:

  ```rust
  TaskFailureKind::TaskFailed {
      retryable: false,
      reason: "RuneSandbox::<stable-label>".to_string(),
  }
  ```

  Non-retryable by construction — a script that exhausted fuel /
  memory / hit a forbidden capability will do so on retry too.

- **`execute_script` signature update.** Replace
  `execute_script(script, _fuel)` (with the dead `_fuel`
  parameter) with `execute_script(script, &config)` consuming
  `&RuneSandboxConfig`. The existing caller at
  `crates/toron/src/lib.rs:1091` updates to construct a config.
  A transitional `execute_script(script, fuel)` compatibility
  wrapper is acceptable if it constructs a config from the fuel
  arg and a default config for the other fields — but the dead
  input cannot remain.

- **Conditional architecture guard** at
  `tests/architecture_test.rs` proves `rune_engine.rs` does
  **not** call `Context::with_default_modules()`. The conditional
  escape from the no-new-guard default fires here. Same pattern
  as ADR 005 hard gate 14 / ADR 006 hard gate 15.

- **No new wire surface, no new `MessageKind`, IR variant,
  `BudgetDimension`, task telemetry field, or Wendelon scan
  contract change.** ADR 007 surfaces are private to
  `crates/toron/src/rune_engine.rs` (and a new test file plus
  the `architecture_test.rs` guard).

- **No bench evidence requirement.** Doesn't move the bench
  needle; pure safety surface. Hard gates are correctness +
  behavior + capability tests.

- **No regression on non-Rune query paths.** Queries that don't
  invoke the Rune engine show zero perf or correctness
  regression. Sandbox initialization is cold on non-Rune paths.

- **Per-script wall-time overhead within 10%** of current
  implementation on M7 fixtures. Phase 0 P0.7 reuses
  `enable_profiling.sh` to bootstrap profiler permissions, but the
  profile target is Rune-specific (e.g., `samply record -- cargo
  test rune_authoring_surface_builds_count_plan` or a dedicated
  Rune micro-bench). `run_runtime_filter_perft.sh --samply`
  profiles the distributed-join path and is **not** a substitute.

- **Backward compatibility (M7 contract preserved).** The user
  authoring surface (`col`, `lit`, `filter`, `project`,
  `aggregate`, `count`, etc.) is byte-identically supported.
  Scripts that worked under M7 continue to work unless they
  relied on now-blocked capabilities (filesystem / network /
  process / env / clock / random), which were never part of the
  authoring contract.

## Context

M7 ("Rune Authoring Surface") shipped Toron's primary user-facing
authoring API — a typed DataFrame builder in Rune that lowers to
`toron-ir`. The surface lives at `crates/toron/src/rune_engine.rs`
(~231 lines). User scripts call `engine.execute_script(&script,
fuel)` and receive a `LogicalPlan` that flows through the rest of
the pipeline (binder → executor → workers).

M7 explicitly deferred sandbox enforcement to V1:

> **Sandbox Fuel:** Fuel is currently disabled in V0 due to
> dependency version constraints; to be re-enabled in V1.
> — `plans/impl/007_rune_authoring_surface.md:42`

The current state has four concrete gaps the ADR closes:

1. **Fuel disabled.** Line 216:
   `// vm.set_fuel(Some(_fuel));` — the `_fuel` parameter is
   plumbed through `execute_script` but ignored. A script with
   `loop { }` runs forever.

2. **Default modules loaded.** Line 178:
   `Context::with_default_modules()` — loads **all** of Rune's
   default modules (filesystem, process, environment, time, and
   network depending on Rune version). The M7 plan called for
   minimal context registration; the implementation never got
   there.

3. **No typed failure taxonomy.** Runtime errors flatten through
   `FrontendError::RuneRuntime { message: String }`. Fuel
   exhaustion, sandbox violation, and logic errors are
   indistinguishable to the coordinator and tooling.

4. **No source-size guard.** `Source::new` accepts arbitrarily
   large scripts; an unbounded source string can OOM the parser
   before any other enforcement fires.

The combination means **shipping V1 with user-supplied Rune
exposed unsandboxed is a launch blocker**. The user has pinned
ADR 007 as L-2 (Rune sandbox enforcement) on the iter-3 topics
selection rule's "V1 exposes user-supplied Rune" path.

The Rune module registration in `create_toron_module()` (lines
145-169) already demonstrates the right pattern — explicit
`m.function([name], func).build()` for each allowed entry.
Extending that pattern to the **base context** (not just the
Toron module) closes the gap.

## Decision

### 1. Sandbox configuration (`RuneSandboxConfig`)

Introduce `RuneSandboxConfig` as the single carrier for sandbox
knobs (see V1 contract summary for shape). Tests, callers, and
admission code construct configs explicitly; `RuneEngine::new()`
may keep a production default, but tests and stricter fixtures
must be able to override.

`execute_script(script, _fuel)` becomes `execute_script(script,
&config)`. The dead `_fuel` parameter cannot remain load-bearing.

### 2. Default-deny allow-list context

Replace `Context::with_default_modules()` at line 178 with
`Context::new()` and install only:

- `create_toron_module()` (existing).
- A minimal Rune primitive set (the smallest set of Rune std
  modules needed to actually build a `LogicalPlan`). Phase 0
  audits the M7 test fixtures to determine the minimum set;
  council expectation: arithmetic primitives, basic
  comparison/logic, basic collection literals. **Excluded by
  default:** any module with filesystem, process, network,
  environment, time, randomness, or thread-spawning capability.

The exclusion is **enforced by absence** (allow-list discipline):
unregistered functions are not callable. The Rune VM raises a
typed error when a script calls an unregistered function; this
naturally produces `CapabilityDenied { capability: <category> }`.

The capability category enum (`Filesystem`, `Process`, `Network`,
`Environment`, `Time`, `Random`, `Threading`, `Unknown`) abstracts
over Rune's specific module/function naming so the error surface
is stable across Rune version drift.

### 3. Fuel enforcement

Uncomment the existing call at `rune_engine.rs:216` (or the
equivalent Rune 0.13 API per Phase 0 P0.1):

```rust
vm.set_fuel(Some(config.fuel_budget));
```

Rename `_fuel` → `fuel` at line 193 (now consumed via config).
Map Rune-VM fuel-exhausted errors to
`RuneSandboxViolation::FuelExhausted { fuel_consumed, fuel_budget }`.

A fuel-exhausted execution **must not produce a partial
`LogicalPlan`** — the script's incomplete state is discarded.

### 4. Memory enforcement (V1 hard requirement)

Every script receives a finite memory budget. **Fuel-only V1
sandbox is rejected.** A memory bomb that fits within fuel can
still OOM the host process.

Phase 0 P0.3 chooses one path:

- **(a) Rune-native per-execution allocation limiter** if Rune
  0.13 exposes one. Map exhaustion to
  `RuneSandboxViolation::MemoryExhausted`.
- **(b) Helper-process isolation** if (a) is unavailable. The
  Toron process forks a short-lived helper with OS memory limit
  (`setrlimit(RLIMIT_AS)` on Linux); the helper executes the
  script; results (or `RuneSandboxViolation`) flow back over a
  local pipe/socket. Abnormal helper exit due to OS memory limit
  (SIGKILL from OOM killer) maps to `MemoryExhausted`, **not**
  `InternalSandboxError` (load-bearing — the failure mode is
  predictable, taxonomy must reflect it).

Path (a) is preferred for cycle-fit; (b) is acceptable if forced.
Phase 0 cannot skip this decision before Phase 4 starts.

### 5. Source-size guard

Before `Source::new` at `rune_engine.rs:197`, check
`script.len() <= config.max_source_bytes`. On overflow, return
`FrontendError::RuneSandbox { kind:
RuneSandboxViolation::SourceTooLarge { bytes, budget }, .. }`
without invoking the parser. Prevents parser-DoS via unbounded
source input.

### 6. Wall-timeout outer safety belt

Wrap `execution.async_complete().await` in
`tokio::time::timeout(config.wall_timeout, ...)`. Elapsed maps to
`RuneSandboxViolation::Timeout { elapsed }`, **not**
`RuneRuntime` flattening. The wall-timeout is defense-in-depth
against a broken fuel implementation; under normal operation,
fuel exhaustion fires first.

**Limitation (verified against Rune 0.13.4
`vm_execution.rs:209`):** `async_complete()` calls
`async_resume()` which drives the VM via a synchronous
`vm.run()`. `tokio::time::timeout` therefore only fires at
.await yield points (host async functions); it cannot preempt a
pure CPU-bound Rune loop. The real broken-fuel backstop is the
Phase 4 path-(b) parent-side SIGKILL deadline on the helper
process, not `tokio::time::timeout` on path (a). The wall-timeout
remains the right surface contract because it (i) catches the
yielding paths cheaply, (ii) gives a typed `Timeout` shape
distinct from `FuelExhausted`, and (iii) is the surface the
helper-side SIGKILL also translates into.

### 7. Typed failure taxonomy (`FrontendError::RuneSandbox`)

Extend `FrontendError` with a single new variant carrying an
enum kind:

```rust
FrontendError::RuneSandbox {
    kind: RuneSandboxViolation,
    message: String,
}
```

(See V1 contract summary for full enum shape.) The existing
`RuneCompile` and `RuneRuntime` variants stay (distinguishing
compile errors and genuine script-logic errors from sandbox
violations). Stable wire-reason labels per kind enable
cross-wire mapping without new `TaskFailureKind` variants.

### 8. Coordinator-facing surface + distributed wire mapping

Rune execution happens at script-submission time in the
frontend, **before** any distributed task is dispatched. Sandbox
failures normally appear as `FrontendError::RuneSandbox` at the
submission entry point.

**If a sandbox failure does cross the task wire** (e.g., a
future code path runs Rune inside a worker task, or a current
path the audit reveals does so), it maps to:

```rust
TaskFailureKind::TaskFailed {
    retryable: false,
    reason: "RuneSandbox::<stable-label>".to_string(),
}
```

The ADR 003 7-variant `TaskFailureKind` shape stays unchanged
(architecture-guarded). Gemini iter-0 proposed new variants
(`SandboxFuelExhausted` / `SandboxMemoryExhausted`) — **rejected**
in favor of the reason-string mapping, which preserves the wire
contract.

**Coordinator response** to any sandbox failure: return the
typed error to the submitting client. The error message is
**stable and non-leaky**: it tells the client which budget /
capability / source-size was involved, never internal VM state,
host paths, or Rune source positions.

## Hard gates

ADR 007 is acceptable only if all of these hold:

### Correctness preservation

1. `cargo test -p toron` passes.
2. `cargo test --workspace` passes.
3. Existing architecture guards G1-G18 pass unchanged.
4. All ADR 002 / 003 / 004 / 005 / 006 hard gates pass
   byte-identically.
5. M7 test fixtures (`crates/toron/src/lib.rs:1080+`) produce
   byte-identical `LogicalPlan` output under the new minimal
   context.
6. The Toron module's 5 functions + associated functions remain
   callable end-to-end through `execute_script`.

### Sandbox enforcement

7. **Infinite-loop script terminates with
   `RuneSandbox::FuelExhausted`.** A `loop { }` script does not
   hang; terminates within fuel-budget-bounded time. Host process
   remains healthy.
8. **Memory-bomb script terminates with
   `RuneSandbox::MemoryExhausted`.** A script attempting unbounded
   allocation terminates with the typed error, **not an OOM kill
   of the host**. Enforcement path (Rune-native cap vs
   helper-process isolation) is chosen by Phase 0 P0.3; both
   paths satisfy this gate.
9. **Oversized source rejected pre-compile with
   `RuneSandbox::SourceTooLarge`.** A script exceeding
   `max_source_bytes` fails before `Source::new`.
10. **Filesystem access blocked with
    `RuneSandbox::CapabilityDenied { Filesystem }`.** Script
    attempting filesystem read/write — the VM rejects before the
    host syscall.
11. **Process access blocked with
    `RuneSandbox::CapabilityDenied { Process }`.** Script
    attempting process spawn — no process is spawned.
12. **Network access blocked with
    `RuneSandbox::CapabilityDenied { Network }`.** Script
    attempting any network primitive — no socket is opened.
13. **Environment access blocked with
    `RuneSandbox::CapabilityDenied { Environment }`.** Script
    attempting `std::env::var` — no env var is read.
14. **Time / randomness access blocked with
    `RuneSandbox::CapabilityDenied { Time }` /
    `RuneSandbox::CapabilityDenied { Random }`.** Determinism
    gate; no host clock, no host RNG.
15. **Wall-timeout fires as `RuneSandbox::Timeout` on yielding
    paths.** A script that exceeds `wall_timeout` *and is at or
    near a yield point* (cooperative async path, helper-process
    IPC stall under path b, etc.) returns `Timeout`, never
    `RuneRuntime` flattening. **Known limitation:** a fully
    CPU-bound Rune loop with broken fuel under path (a)
    Rune-native cap is *not* reliably preempted by Tokio
    timeout — see V1 contract summary. Phase 0 P0.3 path choice
    determines whether wall-timeout is a real broken-fuel
    backstop (path b: parent SIGKILLs the helper) or a soft
    belt (path a: only effective at yield points).
16. **Deterministic execution preserved.** Two runs of the same
    allowed script produce byte-identical `LogicalPlan`.

### Surface stability

17. **No new wire surface.** No `MessageKind`, IR variant,
    `BudgetDimension`, task telemetry field, or Wendelon scan
    contract change. Semantic identifier names per
    `no-planning-phase-names.md`.
18. **`TaskFailureKind` 7-variant shape preserved** (ADR 003
    architecture guard). Sandbox failures crossing the task
    wire map to `TaskFailureKind::TaskFailed { retryable:
    false, reason: "RuneSandbox::<label>" }`.
19. **Architecture guard for `Context::with_default_modules`
    absence.** A `tests/architecture_test.rs` grep guard asserts
    `rune_engine.rs` does not call
    `Context::with_default_modules()`. Defense against silent
    regression of the allow-list discipline. Conditional escape
    from the no-new-guard default fires here (same pattern as
    ADR 005 hard gate 14, ADR 006 hard gate 15).
20. **Coordinator-facing errors are stable and non-leaky.**
    Error messages do not include internal VM state, Rune
    source positions, host file paths, or anything that could
    aid a sandbox escape attempt. Category enum
    (`CapabilityCategory`) abstracts over Rune's specific
    module/function naming for stability across version drift.
21. **No regression on non-Rune query paths.** Non-Rune queries
    (SQL, direct binder, distributed joins) show zero perf or
    correctness regression. Sandbox init is cold on non-Rune
    paths.

### Performance

22. **Per-script wall-time overhead within 10% of current
    implementation.** Sandbox enforcement (fuel + allow-list +
    source-size + memory cap + wall-timeout) does not add more
    than 10% wall-time overhead to M7 fixture scripts. Phase 0
    P0.7 measures via the perft tooling
    (`enable_profiling.sh` + `samply`). **First ADR cycle to
    actually execute a profiler note** — the recurring
    "permissions blocked" caveat from ADR 005 / 006 bench
    artifacts is now closed.

## Rationale

### Why allow-list rather than deny-list

The Rune `Context::with_default_modules()` set is large and
varies between Rune versions. Deny-listing specific modules is
fragile: a future Rune upgrade could introduce a new default
module with a capability we don't yet know to deny. Allow-list
enforcement (`Context::new()` + explicit installs) inverts the
default — the script can only do what's been explicitly
registered. New Rune modules don't silently widen the attack
surface.

### Why memory enforcement is V1 hard, not V1.x

A memory-bomb script that fits within the fuel budget can still
OOM the host process. Fuel alone is necessary but not sufficient.
Memory enforcement is part of the V1 contract; the implementation
chooses Rune-native cap (preferred) or helper-process isolation
(forced), but not "deferred." This is the load-bearing
correction Codex iter-0 contributed to the cycle — my iter-0 had
memory as a V1.x reservation; the council convergence reversed
it.

### Why category-enum `CapabilityDenied`

Raw missing-function strings (e.g., `std::fs::read`) are fragile
to Rune version drift — function naming and module paths can
change. A category enum (`Filesystem`, `Process`, `Network`,
`Environment`, `Time`, `Random`, `Threading`, `Unknown`) is
stable for client tooling and human readability; the `Unknown`
variant accommodates unanticipated capability categories without
leaking implementation detail. Raw Rune module paths are captured
only in internal `tracing` debug logs (operator-side), never on
the public `CapabilityCategory` surface — keeping the
coordinator-facing taxonomy non-leaky per hard gate 17.

### Why no `TaskFailureKind` variant

Rune execution happens at script-submission time, in the
frontend. By the time a distributed task is dispatched, the
script has already produced a `LogicalPlan`; the VM is no longer
running. Adding a `TaskFailureKind::RuneFailure` variant would
imply Rune runs inside a worker task, which it doesn't in V1.
`FrontendError` is the right surface; reason-string mapping
covers the cross-wire case if a future path triggers it. ADR 003
7-variant guard stays intact.

### Why `wall_timeout` exists despite its limitation

Fuel is the primary CPU accounting mechanism. The wall-timeout
catches yielding paths (cooperative async, helper-process IPC
stalls, script paths that release control between operations).

**It does not reliably preempt a CPU-bound Rune VM** under path
(a) Rune-native cap: `async_complete()` runs the VM
synchronously inside `async_resume()` until a yield, so Tokio
gets no chance to fire the timeout. The honest broken-fuel
backstop is path (b) helper-process isolation — the parent can
SIGKILL the helper after `wall_timeout` elapses.

The contract still advertises `Timeout` because (i) it's the
right typed surface for genuine yield-bounded timeouts, (ii) it
captures helper-process stalls under path (b), and (iii) it's
load-bearing for the safety story under path (b). Phase 0 P0.3
makes the path tradeoff visible: choosing path (a) accepts
weaker broken-fuel resilience; path (b) gets the parent-side
kill switch.

### Why `max_source_bytes` is necessary

Without a pre-compile source-size check, an unbounded script
string can OOM the Rune parser before any other enforcement
fires. The 64 KiB default is generous for legitimate authoring
scripts (M7 fixtures are well under 1 KiB) and bounded enough
to prevent DoS.

### Why one allow profile in V1

`AllowProfile::ToronAuthoring` is the only V1 profile. The
field exists on the struct so V1.x can add stricter (test
sandbox) or looser (debugging override) profiles without
breaking the `RuneSandboxConfig` shape — extension is
type-system-checked.

### Why architecture guard for `with_default_modules` absence

ADR 005 hard gate 14 and ADR 006 hard gate 15 established the
conditional architecture-guard pattern: by default no new
guard, but if a specific drift is plausible from the
implementation shape, a source-level grep guard prevents it.
The `Context::with_default_modules()` call is exactly that
shape — a future refactor could re-introduce it silently,
opening the sandbox. The grep guard fails loud.

### Why no bench evidence requirement

ADR 005 and ADR 006 were perf ADRs requiring bench-results
artifacts. ADR 007 is a safety ADR — the win is "user-supplied
Rune can't escape" not "X-second wall-time reduction." Phase 0
audit + hard-gate test pass is the promotion evidence; no
formal `_bench_results.md` artifact required.

### Why Phase 0 P0.7 will execute a profiler note this cycle

ADR 005 and ADR 006 bench artifacts carried a "samply / perf /
gdb attach blocked by `/proc/sys/kernel/perf_event_paranoid=4`
and Yama ptrace policy" caveat. The post-ADR-006 perft tooling
landed `crates/toron/scripts/bench/enable_profiling.sh`, which
bootstraps the kernel + Yama policies that were blocking the
profiler. ADR 007 P0.7 reuses **that helper**, not the
distributed-join workflow that ships next to it
(`run_runtime_filter_perft.sh --samply` profiles the
runtime-filter operator, not the Rune engine). The Rune profile
target is a dedicated Rune-only run (e.g., `samply record --
cargo test rune_authoring_surface_builds_count_plan` or a small
Rune micro-bench).
ADR 007 is the first ADR cycle where Phase 0 calibration can
actually use a profiler — Phase 0 P0.7 makes the profiler
note a non-optional deliverable.

## What Not To Do

- **Do not generate the V1 sandbox via `Context::with_default_modules()`.**
  Allow-list only.
- **Do not deny-list specific Rune modules.** Allow-list is the
  contract; deny-list is fragile to Rune version drift.
- **Do not ship a fuel-only V1 sandbox.** Memory enforcement is
  load-bearing.
- **Do not add a `TaskFailureKind::RuneFailure` variant.** Rune
  is frontend-scoped; sandbox failures surface as
  `FrontendError`. Reason-string mapping covers wire-crossing.
- **Do not let fuel exhaustion be a soft warning.** It's a typed
  terminal error.
- **Do not let sandbox failures fall through to `RuneRuntime`.**
  Use the typed variants. `RuneRuntime` is for genuine
  script-logic errors.
- **Do not add new native functions to the allow-list without
  threat-model review.** Each new function is a potential
  sandbox-escape vector. Extension is a separate narrow ADR.
- **Do not expose VM internals in error messages.**
  Coordinator-facing errors are stable and non-leaky.
- **Do not couple ADR 007 to per-tenant governance.** Tenant
  identity is L-3 territory. ADR 007 enforces per-*script*
  budgets only.
- **Do not let `max_source_bytes` be checked after `Source::new`.**
  Pre-compile guard is the contract; post-parse rejection is
  too late.
- **Do not let `wall_timeout` masquerade as fuel exhaustion.**
  The infinite-loop hard gate must assert `FuelExhausted`, not
  merely `Timeout`. Wall-timeout is defense-in-depth.
- **Do not skip the M7-fixture backward-compat test.** The
  authoring surface is contract; M7 fixtures must pass
  byte-identically.
- **Do not skip Phase 0 P0.7 profiler capture.** First ADR
  cycle to execute it; carries the "no permissions blocked
  carry-forward" expectation.

## Rejected and Deferred

### Row-level Rune UDF execution

Out of scope. Rune is frontend-scoped in V1 (script →
`LogicalPlan` → distribution). Worker-side UDF execution is a
substantial future ADR covering distributed sandbox +
per-worker budget interaction + cross-task scheduling. Gemini
iter-0 framed sandbox failures as worker-side UDF failures with
new `TaskFailureKind` variants; rejected because Rune is not
in the worker-task hot path today.

### New `TaskFailureKind` variants

Rejected (Gemini iter-0 proposal). Preserves the ADR 003
7-variant `TaskFailureKind` wire contract (architecture
guarded). Reason-string mapping is the right cross-wire
surface.

### Fuel-only V1 sandbox

Rejected (Codex iter-0 framing). Memory enforcement is V1 hard
— Rune-native cap or helper-process isolation, but not
"deferred."

### Per-tenant Rune budgets

Deferred to L-3 (multi-tenant governance ADR). ADR 007 ships
per-script budgets; per-tenant aggregation comes when
multi-tenant workload evidence justifies it.

### WASM sandbox migration

Considered: replacing Rune with a WASM runtime for stronger
sandboxing. Rejected — out of scope for ADR 007 (would replace
M7's authoring surface entirely). Reserved for a future ADR if
the Rune sandbox proves insufficient.

### Subprocess sandbox via fork + seccomp + rlimit (fuller
isolation)

Considered as the V1 memory-enforcement path: each script runs
in a forked subprocess with full Linux seccomp + rlimit
isolation. Rejected for V1 — substantially heavier than
necessary; helper-process isolation (path b in §4) is
lightweight enough when needed. seccomp-bpf attack-surface
narrowing remains a future ADR if threat model evolves.

### Time / randomness access

Deferred. Rune scripts in V1 are pure plan-construction; they
don't need wall time or randomness. If a future authoring
feature legitimately needs deterministic time (e.g., a `now()`
literal that resolves at plan-bind time), that's a separate
authoring-surface ADR with its own determinism contract.

### Allow-list extension to additional native functions

Each new function is a separate threat-model review. Out of
scope for ADR 007; covered by future authoring-surface ADRs
under their own narrow contracts.

### Distributed Rune execution (worker-side)

Reserved. Currently Rune runs frontend-only. If a future feature
needs worker-side script execution, that's a substantial new
ADR covering distributed sandbox + per-worker budget
interaction. The `TaskFailureKind` mapping reserved in §8 is
the forward seam.

### JIT / Cranelift specialization

Reserved (Brainstorm 001). Performance-driven, not safety-
driven. Out of scope for ADR 007.

## Council decisions

Three-way final convergence at iter-2. Iteration artifacts
retired.

1. **ADR 007 = Rune sandbox enforcement** (L-2 from the topics
   cycle). User pinned via the "V1 exposes user-supplied Rune"
   path of Codex's four-path selection rule.
2. **Scope: process-internal scale boundary, no secondary
   boundaries** (no cross-ring, no scheduler, no transport, no
   operator-level). Three-way.
3. **`RuneSandboxConfig` struct as the sandbox carrier** with
   five fields: `fuel_budget`, `memory_budget`, `wall_timeout`,
   `max_source_bytes`, `allow_profile`. Three-way (Codex iter-0
   structure; Claude iter-1 adoption; Gemini iter-4 adoption).
4. **Allow-list (default-deny), not deny-list.** Three-way.
5. **`Context::new()` + explicit registration** replaces
   `Context::with_default_modules()` at `rune_engine.rs:178`.
   Three-way.
6. **Fuel re-enabled** at `rune_engine.rs:216` via Phase 0
   P0.1's confirmed API. Three-way.
7. **Memory enforcement as V1 hard requirement.** Rune-native
   cap preferred (path a); helper-process isolation acceptable
   if forced (path b). Fuel-only V1 rejected. Three-way (Codex
   iter-0 framing).
8. **`max_source_bytes` pre-compile guard** before
   `Source::new`. Three-way.
9. **`wall_timeout` defense-in-depth outer belt** via
   `tokio::time::timeout`, scoped to yielding paths only (Rune
   0.13.4 `vm.run()` is synchronous, so CPU-loop preemption
   comes from the Phase 4 helper-process SIGKILL deadline, not
   `tokio::time::timeout`). Three-way.
10. **Single `FrontendError::RuneSandbox { kind, message }`
    variant** carrying the 7-label `RuneSandboxViolation`
    enum. Three-way (Codex iter-0 shape).
11. **No new `TaskFailureKind` variants.** Rune is
    frontend-scoped; cross-wire mapping uses existing
    `TaskFailureKind::TaskFailed { retryable: false, reason:
    "RuneSandbox::<label>" }`. Preserves ADR 003 7-variant
    guard. Three-way (Gemini iter-0 proposed new variants —
    explicitly rejected).
12. **Stable wire-reason labels** pinned (Codex iter-0 P0.5,
    three-way): `RuneSandbox::FuelExhausted`,
    `RuneSandbox::MemoryExhausted`,
    `RuneSandbox::CapabilityDenied`,
    `RuneSandbox::SourceTooLarge`,
    `RuneSandbox::Timeout`,
    `RuneSandbox::InvalidEntrypoint`,
    `RuneSandbox::InternalSandboxError`.
13. **`CapabilityDenied { capability: CapabilityCategory }`**
    with category enum, not raw strings. Three-way iter-2
    (Claude iter-1 lean, Codex iter-2 explicit endorsement,
    Gemini iter-4 implicit).
14. **Conditional architecture guard fires** — source-level
    grep in `tests/architecture_test.rs` proves
    `rune_engine.rs` doesn't call
    `Context::with_default_modules()`. Three-way (Codex iter-0
    mandate; Claude iter-1 adoption; Gemini iter-4 implicit).
15. **Non-Rune query paths show zero regression** — hard gate.
    Three-way (Gemini iter-0 framing, adopted).
16. **Per-script wall-time overhead within 10%** of current
    implementation. Phase 0 P0.7 measures via perft tooling.
17. **Coordinator-facing error messages are stable and
    non-leaky** — no host paths, no VM source positions, no
    internal state. Three-way.
18. **Determinism preserved** — two runs of the same allowed
    script produce byte-identical `LogicalPlan`. Three-way.
19. **Scale-boundary structural ADR section carries forward**
    from ADR 006 convention. ADR 007 declares process-internal
    primary, no secondary, no cross-ring / scheduler /
    transport / operator-level touches. Standing convention.
20. **Phase 0 P0.7 will actually capture a profiler note** —
    first ADR cycle after the perft tooling story landed. No
    "permissions blocked" carry-forward caveat.
21. **Per-tenant Rune budgets deferred to L-3** (multi-tenant
    governance). ADR 007 pins per-script safety.
22. **Row-level Rune UDF execution out of scope.** Rune is
    frontend-scoped today; worker-side UDF execution is a
    substantial future ADR. Three-way (rejects Gemini iter-0's
    UDF framing).
