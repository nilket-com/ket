# Test parallelism policy

Status: policy, repo-wide (every crate in the nilket workspace, the ket root
crates, `ci/`, `scripts/`, `nilket/scripts/` and the ladder driver).

**Default.** cargo-cgroup runs one test thread per CPU of its cpuset (ADR
0694, which ended ADR 0673's adoption hold by operator decision on 2026-10-30,
before the PD register closed). Open register entries stay open: a failure
the default exposes is registered and then fixed or measured, never
serialised, retried to green or reverted around. A stress run sets an
explicit test count chosen for what it stresses; there is no 1024-thread
stress lane or posture (ADR 0695).

Operator rule, 2026-10-20: no test runs alone unless it blocks one core. This
document is the canonical home of that rule. `ci/README.md`
("Test concurrency"), `nilket/.config/nextest.toml`, `ci/controlled-tests.txt`
and `nilket/crates/wendelon/plans/ops/testing.md` point here.

## 1. Default: cpuset CPU count; stress sets an explicit count

- `nilket/scripts/cargo-cgroup` defaults to the CPU count of
  `CARGO_CGROUP_CPUS`, each CPU counted once (for example 8 in a ladder lane,
  12 for the default `0-11`); a cpuset it cannot count is refused.
  An explicit `CARGO_CGROUP_RUST_TEST_THREADS` override, any positive count,
  still wins.
- Stress sets its own explicit count, chosen for what it stresses (the
  ladder's in-process stress steps use 12 and 8). The cpuset, CPU quota, nice
  and ionice keep the host reachable, not the thread count: an eight-CPU lane
  consumes at most eight CPUs whatever the count. There is no dedicated
  1024-thread stress lane (ADR 0695); earlier 1024 runs, which exposed ring,
  counter, sidecar and JOIN defects, stay in the register as history.
- The cgroup limits were measured for 1024 concurrent tests and stay as
  headroom over the default (see `cargo-cgroup --help`).
  Measured on slim, 2026-10-20, eight CPUs: the Wendelon lib suite peaked at
  21-23 GiB and about 3600 tasks, with up to 3649 open files in one process.
  - Per invocation: `MemoryHigh=28G`, `MemoryMax=32G`, `TasksMax=16384`, and
    `LimitNOFILE` at min(caller hard limit, user manager `DefaultLimitNOFILE`).
    A systemd user scope otherwise starts at a soft limit of 1024 open files,
    which 1024 concurrent tests exhaust at once.
  - Shared: every invocation runs in one slice, `cargo-cgroup.slice`, whose
    `MemoryMax` bounds all concurrent sessions together. The slice max is
    min(`CARGO_CGROUP_SLICE_MEMORY_MAX`, default 96G,
    `CARGO_CGROUP_SLICE_MEMORY_PERCENT`, default 60%, of MemTotal); each
    invocation's `MemoryMax` is min(32G, slice max), and its `MemoryHigh` is at
    most its `MemoryMax`. On slim (61 GiB) the slice is about 36 GiB, so two
    sessions together cannot OOM the host.
  - The slice limit is never lowered under a running session. With no
    cargo-cgroup unit active, a starting session sets it to its own value;
    while units are active, a starting session only raises it. A failed unit
    listing counts as active, and a slice without a numeric limit while units
    run is refused rather than run under. A per-user lock in
    `/run/user/<uid>` (never `TMPDIR`, which differs per ladder lane) makes
    the check, the set and the unit start atomic; without that directory the
    wrapper refuses to start. The ladder's 80% share
    (three 32G lanes in bigfoot's 96G slice, 123 GiB) therefore arrives as a
    set or a raise, never a lower; the ladder refuses a host where 96G does
    not fit. A session that finds a higher limit already in place runs under
    it until the slice is idle again.
  - The nerdctl backend has no shared cap: its limits are per container only.
  - A timeout or signal that stops the wrapper stops its unit too (a killed
    `systemd-run` client otherwise leaves the unit running): the wrapper
    stops and reaps the client first, so the unit cannot appear after the
    stop, then stops the unit twice. `RuntimeMaxSec`
    (`CARGO_CGROUP_RUNTIME_MAX_SEC`, default one day, the ladder sets each
    step's budget plus 90 s) is the backstop if the wrapper itself is killed.
    A unit stopped by `RuntimeMaxSec` is reported on stderr and the wrapper
    exits 124.
  - **Soak drivers must set `CARGO_CGROUP_RUNTIME_MAX_SEC`** above their run
    time when launched through cargo-cgroup: the one-day default would stop
    a 24 h or 72 h soak (ADR 0629 Stage C) partway.
  - Raising parallelism without re-checking these limits is not allowed, and
    the architecture test pins the values.
- nextest already runs tests across processes concurrently; its test-groups
  are the narrow serialization mechanism there.

## 2. Serial execution: only noise-sensitive tests, only in small lanes

Operator, 2026-10-20: the main place where running one test at a time may be
relevant is the dedicated lanes for noise-sensitive tests, but one thread must
never block eight cores.

1. **The only legitimate serial case is a noise-sensitive test:** a timing,
   benchmark or perf-gate test, or a clock-drift test that is genuinely about
   timing. A test that fails under concurrency for any other reason is a
   defect candidate (section 4), and a genuine shared-resource conflict
   (one port, one global, one file) is fixed with narrow locking or isolation
   inside the tests that share it, never with serialization of the run. Such a
   lock names the resource it guards and carries the reason in a comment
   beside it; it is not listed in the allowlist, because it serializes only
   the tests that share that resource.
2. **It runs in a dedicated lane sized to what it uses.** A test that runs on
   one thread gets a 1-CPU lane, or 2 CPUs for headroom; never an 8-CPU lane.
   The other CPUs keep running parallel work at the same time. Examples:
   `scripts/cargo-ctrl`'s controlled lane (2 CPUs, `ci/controlled-tests.txt`)
   and `ci/windows-test-lanes.ps1`, its Windows counterpart. **UNVERIFIED:**
   the Windows lane script is parse-checked on Linux only and has not yet run
   on a Windows host.
3. **It is marked explicitly:** an entry in `ci/controlled-tests.txt`, a
   nextest `test-group`, or an `--exact` invocation of that one test, with the
   reason written beside it.
4. **It is allowlisted.** Any serial setting in the pinned scope (section 3)
   has an entry in `nilket/.config/serial-allowlist.txt` giving its scope and
   reason. The scope is `lane:1` or `lane:2`, and the line must declare a
   cpuset of at most that many CPUs (`taskset -c`, `CARGO_CGROUP_CPUS=`,
   `/affinity`); or `test-group`, a nextest group limit or `threads-required`
   in a `nextest.toml`, which limits its group while every other test keeps
   running; or `doc`, a Rust comment or `#[ignore]` reason that documents how
   to run one noise-sensitive measurement and executes nothing. An entry without a matching
   occurrence is an error, and so is an occurrence without an entry or a lane
   larger than its scope.

An `--exact` run of one named test needs no thread setting at all, so it
carries none.

## 3. No blanket serial settings in gates

The wrapper, ladder steps, local gates and CI scripts never pass a blanket
serial setting:
no one-thread `--test-threads`, `RUST_TEST_THREADS`, `NEXTEST_TEST_THREADS` or
`test_threads`, no one-job `-j`/`--jobs`/`CARGO_BUILD_JOBS`, no nextest
`threads-required`, and none of these through a variable whose every
assignment is 1, as a two-element argument (`"--test-threads", "1"`), or in
Cargo's `[env]` table form (`{ value = "1" }`). A step that needs a different count passes a higher one
deliberately. The pinned scope is `ci/`, `scripts/`, `nilket/scripts/`
(including the checked-in ladder template), `nilket/.config/`, every
`.cargo/config*`, every `.github/workflows/*.yml`, every `Makefile` and
`justfile`, every `Containerfile` and `Dockerfile`, every `nextest.toml`,
every `*.sh`, `*.ps1`, `*.py` and `*.rs`, and every executable file without an
extension. A Rust doc comment may document a noise-sensitive invocation
(`doc` scope), but a line inside a doctest code block is code.

Every ladder step runs under `timeout --kill-after=60 <budget>` with a budget
of about three times its expected time, so a hang (for example the futex wait
`distributed_process_test` once showed) ends with rc 124 or 137 instead of
holding the run.

## 4. A parallel-only failure is a defect candidate

A failure that appears only under concurrency is a defect candidate, never a
flake, and it is never fixed by widening serialization. Reproduce it under load
(nextest `--stress-count` plus repeated in-process libtest runs) and fix the
product or the test's isolation. See "Test concurrency" in `ci/README.md` for
the reproduction commands.

## 5. Banned terminology

The phrase made of "single" and "thread" (any case, joined by nothing or by any
run of non-alphanumeric characters such as spaces, NBSP, dashes, underscores,
`/` or `.`, with any suffix) is forbidden everywhere in
the repo: code, identifiers, comments, docs, plans and scripts. It hides what is
meant. Say the precise thing instead:

- "one worker thread", "one thread" or "a `current_thread` tokio runtime" for
  an execution model;
- "serial (reason: ...)" for a test that runs alone;
- "one core" or "pinned to one core" for a measurement;
- "one-thread reference (`SET threads=1`)" for a benchmark reference engine.

Historical result documents are reworded the same way without changing any
measured fact. A third-party quote or vendored file that genuinely needs the
phrase must be allowlisted explicitly in the architecture test; the expected
count of such entries is zero. A tracked file that is not UTF-8 fails the scan
unless its extension is a listed binary format (arrow, jpeg, parquet, png,
sqlite, ttf).

## 6. Enforcement

`nilket/crates/nilket/tests/test_parallelism_policy.rs` pins, each with a
probe or negative control:

- the cargo-cgroup defaults: one test thread per cpuset CPU, a refused
  uncountable cpuset, any explicit positive count (the override still wins). It also pins the
  slice, its 96G and 60% defaults, 28G/32G per invocation, 16384 tasks, the
  open-file minimum and the one-day `RuntimeMaxSec`; it also runs
  `nilket/scripts/tests/test-cargo-cgroup-systemd-env.sh`, which checks the
  values the systemd backend passes on several host sizes, that a big session
  running keeps a small session from lowering the slice limit, and that a
  signal stops the unit;
- no serial setting in the pinned scope outside the exact allowlist, and every
  allowlisted one in its declared scope (a lane of at most 1 or 2 CPUs, a
  nextest group, or a Rust doc line), and no scope that could allowlist a
  serial wrapper default;
- the banned phrase appears nowhere in tracked files (exact zero), and no
  unreadable file is skipped silently;
- the checked-in ladder and supplement templates carry no serial step outside
  a lane of at most 2 CPUs, and every step has a timeout budget; the
  supplement runs plan-commit steps with the audited commit's wrapper.

The ladder driver itself lives outside the repo
(`reviews/ladder-driver-v<N>/run-ladder.sh.template`). The repo carries a
byte-identical copy at `nilket/scripts/ladder/run-ladder.sh.template`, which
the pin scans; the ladder audit checks that the driver's `SHA256SUMS` entry
equals the SHA-256 of that copy at the audited commit.

## 7. Known open defects

Parallel-only defect candidates found when the default moved to 1024 test
threads, tracked one by one in
`nilket/plans/ops/parallel_defect_candidates.md`. Each open entry blocks
only its own closure (ADR 0694); none is handled by serialization.

- PD-01 to PD-09: Wendelon lib tests (process-global metric deltas, timing
  windows, a fork ENOMEM, a `DataDirectoryBusy` on reopen).
- PD-10 to PD-12: Wendelon integration binaries `stream_finalization`,
  `multinode_bootstrap` (including a SIGABRT with no result line) and `chaos`.
- PD-13: the `distributed_process_test` futex hang, unexplained and not
  reproduced since.

- PD-14 to PD-19: additional expiry, progress, tiering, shutdown and WAL
  evidence candidates reconciled in the register.
- PD-20: counter write-key gate reclamation, deterministically reproduced;
  focused repair evidence and full adoption status are tracked in the register.
- PD-21: replica operation gate reclamation, deterministically reproduced;
  logical admission repair passed focused stress and load controls; final
  stacked adoption acceptance remains pending.
