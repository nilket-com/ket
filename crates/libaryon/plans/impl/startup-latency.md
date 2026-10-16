# Startup latency tests

Scope: regression tests for startup time at two levels, the headless engine
(`EngineHandle::spawn` to first usable `Viewport`, in libaryon) and the terminal
head (exec to first painted frame and first reflected key, a baryon pty test).
Black box only: no production instrumentation. Converged with Codex over chatd
(conversation `startup-latency-plan`); rounds 1 and 2 of its review are folded
in and their answers are in section 9. Deliverable is one `impl:` commit
reviewed against this plan.

## 1. Goal

Catch startup regressions before users do, with a test that is cheap to run, hard to make flaky, and
says WHICH part of startup got slower. Two products, two levels:

- **libaryon** (headless engine): time from `EngineHandle::spawn(config)` to the first usable `Viewport`.
  This is what polariton pays when it embeds the engine.
- **baryon** (terminal head): time from process exec to the first painted frame, and to the first
  keystroke being reflected. This is what a user pays.

Non-goals: throughput, steady-state redraw latency, input latency after startup, absolute
benchmarking of the host (the tests assert regressions, not speed records), and huge-file stress
(200 GiB stays in `ci/huge-file-check.sh`, see 9.9).

## 2. Ground truth (read from the tree)

- `crates/baryon/src/app.rs::run` builds `EngineConfig` from settings, sets `initial_file` and
  `init_script = default_init_path()`, calls `EngineHandle::spawn`, then enables raw mode, enters the
  alternate screen, builds the ratatui terminal and runs the frontend loop.
- `EngineConfig` (`libaryon/src/engine/host.rs`) already names the startup inputs: `initial_file`,
  `init_script` (Rune; "library embedders must opt in"), `persistent_history` (reads the history file),
  `initial_viewport` (sets session geometry; it does NOT request a render).
- **The engine does not render at startup on its own.** `engine/runtime.rs` renders after init and the
  initial file load only when `init_report.needs_render || startup_subscriber_render`. Otherwise the
  first frame answers the head's first command: baryon sends `EditorCommand::Resize(w, h)` first thing
  (`baryon/src/ui/core.rs`, `run`), and `examples/headless.rs` does the same.
- Precedent: `libaryon/tests/quit_latency_test.rs` isolates `XDG_STATE_HOME`/`HOME`, seeds 1,000 history
  lines, runs 7 samples in-process and asserts the median is under a fixed limit (100 ms).
- Every nonempty file opens through `svp::ingest::ingest_svp_file` (1 MiB cold extents). `ci/huge-file-check.sh`
  builds a 200 GiB sparse fixture and runs `sparse_200_gib`; `plans/baryon/huge_file_scaling.md` still states
  the 80-200 GiB objective that first-viewport time not depend on file size (not yet met, section 10).
- `scripts/cargo-ctrl` has a controlled CPU lane for timing tests, but it only runs `--workspace --lib`
  tests selected by exact name from `ci/controlled-tests.txt`. An integration-test file such as
  `quit_latency_test.rs` runs in the general (80% CPU, parallel) lane.
- `crates/baryon` has no dev-dependencies and only `tests/architecture_test.rs`.

## 3. Milestones

| Id | Event | Where measured |
| --- | --- | --- |
| M0 | process exec | harness (spawn time) |
| M1 | `main` entered | optional trace |
| M2 | `EngineHandle::spawn` returns | libaryon test / optional trace |
| M2r | `EditorCommand::Resize(100, 30)` submitted (timestamp recorded, so queue delay is visible) | libaryon test |
| M3 | first USABLE projection after the resize: focused pane and layout present, geometry 100x30 (not "non-empty": a no-file viewport is valid with no document text) | libaryon test |
| M3t | first viewport whose focused pane carries document text (file scenarios only) | libaryon test, diagnostic |
| M4 | a COMPLETE first frame observed: PTY output is accumulated and parsed until a whole frame is visible; one `read()` can split a frame | baryon pty test |
| M5 | a known key is reflected as screen state, not merely received | baryon pty test (interactive) |

Reported: M2-M3 (engine ready: spawn returned, resize submitted, first usable viewport), M3-M3t
(first text), M0-M4 (first paint), M4-M5 (first input). Black box first: no new production
instrumentation in the first cut.

## 4. Tests

### 4.1 libaryon: `startup_latency_test` (in-process)

`EngineHandle::spawn(config)`, take the viewport receiver, submit `EditorCommand::Resize(100, 30)`, wait
for the first usable viewport, then (file scenarios) for the first viewport with document text; record
the timings and shut down. Each wait has a bounded deadline (30 s) and a missed deadline FAILS the sample
rather than recording a time. N samples per scenario (N = 9), report min / median / p90, assert on the
median. Environment isolated like the quit test, plus `DISPLAY` and `WAYLAND_DISPLAY` removed so
clipboard initialisation cannot add variance. Sidecar and cache state isolated per sample.

Scenarios with an init script (S2, S4) can render before the resize. Their scripts must not request a
render, or the test drains and identifies pre-resize projections; otherwise the first viewport is not
provably resize-caused without instrumentation.

### 4.2 baryon: `startup_latency_pty_test` (process, unix only)

Run the real `baryon` binary (`CARGO_BIN_EXE_baryon`) under a pty of fixed size (120x40) with
`TERM=xterm-256color`, measure M0-M4 by the first read that contains the alternate-screen-enter sequence
plus first content, then send a key and measure M4-M5. Kill the child after measuring. Skipped on
Windows (no pty harness) and on macOS until a lane exists.

### 4.3 Scenarios (both tests where they apply)

| Id | Scenario | Why |
| --- | --- | --- |
| S0 | no file, no init script, no history | floor |
| S1 | small file (10 KiB Rust) | syntax load, language detection |
| S2 | production-like config: non-rendering init script, 1,000 history lines | what users have |
| S3 | sparse files of 1 MiB, 1 GiB, 16 GiB (64 GiB where the controlled machine can afford it) | **scaling characterization**, NOT a gate (see 5 and 9.3): open cost is linear in extents |
| S4 | large non-rendering init script (say 2,000 lines of Rune) | Rune compile cost |
| S5 | saved-session / checkpoint restore (`spawn_saved`) | a distinct startup path |

S3 fixtures follow `ci/huge-file-check.sh`: `truncate` to size, write a `HEAD` marker at 0 and a tail
marker at the end, and refuse to run if the filesystem did not preserve sparsity. Each sample shuts the
engine down as soon as it has measured: post-open background work is out of scope (see 11).

## 5. What is asserted

Absolute thresholds are fragile across hosts, so assert in this order of preference:

1. **Relative, machine-independent:** S2 within a bounded factor of S0 (config cost is a ratio, not a number).
   The S3 per-GiB slope is REPORTED, not asserted, until the linear open cost is accepted or fixed.
2. **Generous absolute budgets,** set to catch roughly a 3x regression, asserted on the MEDIAN OF 9 and only
   in the quiet (controlled) lane or the dedicated script, never in the shared parallel lane. Keyed by
   profile (`cfg!(debug_assertions)`): the test profile is debug. Override with an environment variable
   per budget; never silently skip. Min and p90 are diagnostics (a min-only gate hides regressions that
   hit most launches).
3. **Trend record:** every run prints one JSON line (scenario, milestone, samples, min/median/p90, git
   describe, profile) so a log scraper can chart it. No file is written by default.

## 6. Where it runs (decided: B for libaryon, C for the pty test)

The controlled lane only takes `--lib` tests by exact name. Options:

- **A.** Put the libaryon test in `src/` as a unit test and list it in `ci/controlled-tests.txt`.
- **B.** Extend `cargo-ctrl` so the manifest can name integration targets (`--test startup_latency_test`).
- **C.** Run both as a dedicated script (`ci/startup-latency-check.sh`, like `ci/huge-file-check.sh`), on
  the quiet machine, outside the parallel lane, with the release binary.

**B** for the libaryon test and **C** for the pty test (9.7).

## 7. Dependencies and architecture constraints

The pty test needs a pty crate as a `baryon` dev-dependency. `tests/architecture_test.rs` must still pass;
ADR 0005 keeps analytics out of the terminal head, which a dev-only test dependency does not touch. Use
`nix` (9.7).

## 8. Questions asked

Round 1 (readiness, instrumentation, scale invariance, cold versus warm, statistics, coverage, lane and
dependency) and round 2 (startup render, the 200 GiB scenario, post-open memory, the 1 MiB anomaly) are
answered in section 9.

## 9. Decisions

### Round 1

1. **Readiness.** Headline milestones stay M2-M3 and M0-M4. M3 is the first usable projection. M4 parses
   accumulated PTY output; M5 asserts screen state after a known key. (Without a terminal-emulator crate,
   M5 can type a unique marker and look for it contiguous in the stream.) *Amended in 9.8: the startup
   render is conditional, so M3 is resize-driven.*
2. **Instrumentation.** Black box first: per-sample milestone timings plus scenario and profile metadata.
   Add an opt-in phase trace only if results leave a regression unexplained.
3. **Scale invariance is false today.** `svp::ingest::ingest_svp_file` creates one node per 1 MiB extent
   (`COLD_INGEST_EXTENT_SIZE`) and reserves and populates vectors for all of them: 1 GiB is 1,024 extents,
   the 200 GiB fixture about 205,000. The line scan is asynchronous, but topology construction scales with
   file size. The existing `sparse_200_gib_warm_index_open_stays_extent_bounded` bounds the extent count, not
   time. S3 is therefore a characterization scenario that records the per-extent slope; whether to fix the
   linear cost is a separate decision. Isolate sidecar and cache state between samples. *Confirmed by the
   baseline: about 1 ms per GiB to first frame (section 10).*
4. **Cold versus warm.** Not controllable without root or a controlled machine. Results are labelled
   "process-start, warm"; S3 sizes are sampled interleaved under the same cache and index conditions.
5. **Statistics.** Median of 9 asserted in the quiet lane; min and p90 reported.
6. **Coverage.** Add S5 (saved session). S2 includes the production theme and settings and a non-rendering
   init script. Parser and provider registry construction happen in `EngineHandle::spawn`, so S1 measures
   them. Terminal setup belongs to baryon M0-M4 only. Git refresh is reconciled after the startup render and
   file-browser indexing is a separate scenario only if opening that panel is in scope; both are out of
   scope here.
7. **Lane and dependency.** B for the libaryon test (extend `cargo-ctrl` so the manifest can name an
   integration target, keeping it off the parallel lane without a unit-test module). C for the pty test.
   No pty crate is in the workspace manifests; use a small unix-only dev-dependency (`nix` is already in
   the lockfile, not yet a direct workspace dependency) and cfg-gate the test module for unix.

### Round 2

8. **The first frame is resize-driven.** Round 1 assumed the engine always renders after init and the
   initial file load; it renders only when init or a startup subscriber asks. `initial_viewport` sets
   geometry but requests nothing. A probe that only waited for a viewport saw one only when an async
   refresh happened to arrive, and otherwise waited out its deadline. M2-M3 is therefore "spawn returned,
   `Resize(100, 30)` submitted, first usable viewport", which is also what baryon does.
9. **200 GiB is supported, but not a latency scenario.** The 200 GiB fixture exercises the current SVP open
   path, not a pre-virtualization legacy one, and `huge_file_scaling.md` still states the 80-200 GiB
   objective. It stays a Linux stress/integration check in `ci/huge-file-check.sh`. The latency loop stops at
   16 GiB (64 GiB where the controlled machine can afford it): that gives the slope without routinely
   building 200,000 extents.
10. **Post-open memory growth is a bug to attribute, not behaviour to encode.** Opening allocates O(extents)
    topology, `leaf_ids`, the metric-scan queue and set, and the root-child index up front; the background
    reader queues newline metrics and does not retain the 1 MiB buffers it reads. Continued growth after
    open (section 10) must be explained before it is accepted. The latency test shuts down after measuring
    and must not depend on it. Tracked in 11.
11. **The 1 MiB first-text anomaly is not a size threshold.** All nonempty files take `ingest_svp_file`.
    Suspect the initial viewport hydration racing its own low-priority scan; check fixture contents and
    sidecar state first. Tracked in 11.

## 10. Baseline

Development machine, release build of `examples/startup_probe.rs` (throwaway, not committed), warm cache,
`Resize(100, 30)` after spawn, 3-5 samples, typical values. Each run under a 4 GiB memory cap. Peak RSS for
open-and-measure is 28-46 MB at every size.

| Scenario | spawn returns | first viewport | first text |
| --- | --- | --- | --- |
| S0 no file | 0.4 ms | 2.5 ms | n/a |
| S1 10 KiB `.rs` | 0.4 ms | 5.5 ms | 8.5 ms |
| sparse 1 MiB | 0.4 ms | 3.8 ms | 50 ms (anomaly, 9.11) |
| sparse 64 MiB | 0.4 ms | 2.6 ms | 10 ms |
| sparse 256 MiB | 0.5 ms | 3.2 ms | 12.5 ms |
| sparse 1 GiB | 0.5 ms | 4 ms | 11.5 ms |
| sparse 4 GiB | 0.4 ms | 6 ms | 12.4 ms |
| sparse 16 GiB | 0.5 ms | 20 ms | 23 ms |
| sparse 64 GiB | 0.6 ms | 79 ms | 87 ms |
| sparse 200 GiB (stress only) | 0.5 ms | 208 ms | 221 ms |

First viewport grows about 1 ms per GiB above 4 GiB: open cost is linear in extents (9.3).

Memory with the engine kept alive after open (background line scan running): plateaus at 73 MB (1 GiB),
454 MB (16 GiB), 930 MB (64 GiB) and 1.26 GB (200 GiB, at about 17 s, when the scan drains). This is the
probe's own backlog, not engine retention: the probe stopped reading viewports after first text, and the
viewport channel is unbounded. Draining viewports during the hold keeps 64 GiB flat at 48 MB. Each queued
viewport is about 1 MB because the fixture has no newline and the pane carries the first 1 MiB of that
line as one token; the engine emits about 90 viewports a second while the scan runs. An earlier version of
the probe that never sent `Resize` held the engine open up to 120 s per sample and was OOM-killed at 59 GB
anonymous RSS (input not recorded); the same backlog is the likely cause.

## 11. Follow-ups outside this plan

- **Post-open memory growth (9.10): attributed, section 10.** Registry stats stayed flat (resident 1 MiB,
  `malloc_trim` freed 3.5 MB of 937 MB); the growth was queued viewports. It leaves three follow-ups:
  clip long lines horizontally in projection, deliver viewports latest-wins instead of through an
  unbounded channel, then measure whether scan notifications emit frames for an unchanged projection.
- **1 MiB first-text anomaly (9.11).** Explain it before the S3 numbers are used as a reference.
- **`huge_file_scaling.md`** now records that its first-viewport objective is not yet met (9.3).
