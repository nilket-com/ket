# Rust dependency advisory gate (G1)

This evaluator checks **both** `Cargo.lock` and `nilket/Cargo.lock` at an exact
Git commit. It does not build, update locks, scan non-Rust packages, or claim
whole-application coverage. No timer, gate staging, or activation is installed
by these files. G2 source is described below; adoption remains separately reviewed.

Python 3.11+ and Git are required. Obtain the reviewed Linux x86_64
cargo-audit **0.22.2** asset from `policy.json`, verify its archive SHA256, and
extract `cargo-audit` to a private tools directory. The evaluator independently
verifies the executable SHA256, version, and reviewed configuration; other
platforms/builds require an explicit reviewed policy amendment. Never replace
these hashes to make a local unknown binary pass.

Run from any directory; RUN must be a new directory outside the checkout:

```sh
python ci/advisories/gate.py --repo /path/to/checkout --commit FULL_SHA \
  --tool /path/to/pinned/cargo-audit --run /path/to/new-run --mode live
```

Exit codes are **0 PASS**, **1 FAIL** (unpermitted vulnerabilities), **2 HOLD**
(incomplete coverage, invalid policy, acquisition/execution/schema error).
Only PASS is clearance. Both locks are checked even when the first has
findings. Execution errors never become a clean report. G1 must run before
staging and freshly before FF/push once adopted by a reviewed gate inventory.
A newer database is new input; it never means rerunning passing Rust tests.
Existing vulnerabilities are not silently exempted by adding this gate.

`receipt.json` records source commit/tree, lock hashes, pinned tool/config/script
hashes, exact argv/return codes, database identity/archive hash, evaluation UTC,
raw report hashes, full findings (including package source and checksum), and verdict.
Audit and live-fetch commands use only PATH plus empty run-owned HOME, CARGO_HOME
and TMPDIR; ambient Cargo, RustSec and Git environment overrides are excluded,
and the effective environment is recorded. Raw reports and stderr are kept.
Database `last-commit`/`last-updated` in cargo-audit's `--no-fetch` reports may be
null: the wrapper separately certifies clean Git d before and after evaluation,
and archives the database, including Git metadata. Live acquisition uses an
owned new checkout and successful upstream fetch; no stale-cache fallback.
Yanked crates are explicitly **NOT CHECKED**. Informational advisories warn.

Replaying a retained database requires its unpacked **clean Git checkout**:

```sh
python ci/advisories/gate.py --repo /path/to/checkout --commit FULL_SHA \
  --tool /path/to/pinned/cargo-audit --run /path/to/new-replay --mode replay \
  --database /path/to/archive/advisory-db --at 2026-10-23T00:00:00Z
```

Replay passes `--stale`, is labeled replay, and makes no freshness claim. The
source checkout's two lockfiles must exactly match the requested Git commit.
For historical commits predating the allowlist, the explicit
`--historical-empty-allowlist` flag records an empty policy in replay only;
live mode always requires the Git-pinned allowlist. This bootstrap behavior
requires reviewer acceptance; it never treats missing current policy as PASS.

Allowlist `ci/advisory-allowlist.toml` has `entries = []`, or `[[entries]]` with
exact `id`, `crate`, nonempty `reason`, `reviewer`, UTC `approved_at` and
`expires_at`. No glob, duplicates, unknown fields, future approval or span over
30 days is accepted. Authorization holds only at approved_at <= T < expires_at.
Expired entries authorize nothing; unused expired entries warn. Exceptions need
individual review, not native cargo-audit `--ignore` or a severity filter.

Controls (no network or chatd messages):

```sh
TMPDIR=/path/to/owned-control-tmp ADVISORY_TEST_TOOL=/path/to/pinned/cargo-audit python -B ci/advisories/test_gate.py
```

The real-tool controls use a visibly synthetic advisory database and fixture
locks. They prove findings-exit authorization, no ambient HOME ignores, and
whole-lock optional/dev/target package coverage. Full unfiltered production DB
RED/Cut-C-target-removal evidence is retained separately; unrelated remaining
findings still FAIL overall. Mocked fetch failures are labeled controls.

## Daily watch (G2; templates inactive until reviewed adoption)

`watch.py` performs one bounded scan of a freshly fetched `origin/main` in an
owned source cache/checkout. It calls the same G1 evaluator in live mode.
Successful coverage advances `last_success` whether verdict is PASS or FAIL;
HOLD/fetch/parser/crash/timeout failures preserve existing exposure and all
pending outbox rows. A fresh DB commit alone never creates another incident.

State and all deliveries use one atomic state file under a cross-process
writer lock. Each transition gets its own sequence/UUID, with per-recipient
UUIDv5 keys stored in the same commit before any network action. Network calls
occur outside the short writer lock. Restart, an uncertain send, or concurrent
drainers use those exact stored keys. Lookup precedes submission; every network call checks the remaining delivery budget; a message
ID is recorded separately from recipient acknowledgement. Reading a message
never acknowledges it on the recipient's behalf. Later bounded invocations
retry unfinished deliveries and refresh ACK state, not recreate alerts.

OPEN means present, with permission state tracked separately. Reintroduction,
new affected versions/sources, and loss of authorization create distinct events.
Resolution and health outage/recovery are also recorded. An outage is deduped
until recovery, preserving the first reason and adding later handler notes. A full source/database fetch failure never scans a cached ref.
The checker, failure handler, and delivery receipt writers share the state lock;
orphan temporary state writes cannot block later atomic commits.

The watch uses the dedicated chatd sender **advisory-watch** for both recipients.
chatd rejects sender==recipient, so using codex as sender would never deliver
the codex row. Activation requires registering advisory-watch as a sender-only
participant (no delivery endpoint) in the reviewed chatd configuration and
verifying the existing claude/codex request delivery routes. This code does not
edit or restart chatd. Production activation is blocked until that prerequisite
and an actual TEST delivery/ACK rehearsal have passed.

A config with test_mode=true routes EVERY exposure/health/recovery event to
advisory-watch-TEST with TEST/old-snapshot prefix, including the real health CLI.
A stub chatctl path can exercise the whole handler without sending real messages.
The default/example has test_mode=false. Never point a control at production
state; its TEST state and stub must be isolated.

Artifact retention defaults to the newest seven run directories (retain_runs,
bounded1..30). State, incident history, and pending outbox rows are never removed.
The owned shallow source cache is garbage-collected with bounded packing
resources after a confirmed fetch; retained checkouts use independent copied
objects, so cache pruning cannot break them. Older run artifacts can expire
even while a notification is unacknowledged; the durable event payload retains
its source/database/first-seen facts. No product checkout is removed.

The user units in `systemd/` are **templates, not installed or enabled**. Their
paths target the operator's main checkout after publication. Review and copy a
config based on `watch-config.example.json` to
`~/.config/nilket/advisory-watch.json`, acquire the approved tool, and review
adoption separately. The scanner never calls `enable-linger`. With linger off,
the timer depends on the user manager being active. Persistent catch-up occurs
when that manager next starts, not necessarily on boot. Daily delay is at most
24h + 15min jitter + the bounded run, conditional on manager/network/timer
availability. A disabled/never-fired checker cannot report its own absence.
The next invocation detects >26h since success; G1 receipts separately report
watch state age/unknown status as a warning (`--watch-state` can specify it).

The checker is `Type=oneshot` with `TimeoutStartSec=15min`, stop bound30s and
control-group kill; OnFailure invokes a separately bounded2min health handler.
It persists the health incident and outbox even if the checker died before it
could. This assumes the state storage and user manager remain available; it
cannot alert during a host outage. Units impose owned slice/CPU/memory/task caps.

Additional controls (set TMPDIR to an owned directory):

```sh
python -B ci/advisories/test_watch.py
```

These mock every chatd send, use TEST-labeled fixture incidents, and cover lost
receipts, recipient failure, recurrence, expiry, missed scans, failed origin
fetches, serialized writers and interrupted state writes. Runtime timeout and
timer controls are retained separately as owned transient-unit evidence.
No production timer or alert stream is activated by running the unit tests.
