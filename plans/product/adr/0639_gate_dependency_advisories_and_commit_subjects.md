# ADR 0639: gate dependency advisories and commit subjects (follows adr 0164)

Status: accepted design, 2026-10-23. Repository delivery and dependency policy.

## Context

Dependency exposure must be evaluated from both complete recorded lockfiles,
without dropping optional, development or target-specific packages. A daily
watch must distinguish an actual finding from an unavailable checker and must
preserve incident and delivery identities through crashes.

Architectural work needs a durable repository decision before implementation.
External contracts do not replace an ADR. New history uses a plan followed
by its matching implementations in first-parent ancestry. Existing published
history is preserved until the separately approved history-cleanup procedure.

## Commit decisions and preflight

Subjects use one of these forms:

- plan: adr NNNN <summary>
- impl: adr NNNN <implementation summary>
- fix: <summary>
- test: <summary>
- docs: adr NNNN <summary> for prose bound to an existing ADR
- docs: <summary> for other prose

Messages contain only a subject, with no body. New ADR numbers are repository-wide
starting at 0630; existing documents keep their numbers.

Plan and implementation are for architecture. Fix and test are for patches
without architecture; docs is for prose that is neither a plan nor a run
record. Raw reports, receipts and timelines belong in sealed external packages.
A single short human before/after summary may accompany an implementation;
separate evidence-only commits are refused. ADRs state decisions, contracts
and acceptance rather than raw execution results.

Subjects have no scope or parentheses. The first summary character is lowercase
or a digit; the rest is printable ASCII. Product nouns and internal capitals
are allowed. Delivery cut and incident labels and unsupported prefixes are
refused. Both author and committer must be Joel Bondurant <em@joelbondurant.com>.

History preflight checks every commit reachable in origin/main..candidate,
including commits introduced through a merge. Both endpoints resolve to exact
commit ids; origin/main must be an ancestor. Missing history or malformed
metadata refuses the check. Each implementation requires an earlier plan with the same ADR number in
first-parent ancestry. The ADR file must exist at that plan commit. Multiple
implementations and interleaved lanes are allowed, with different slice
summaries. A plan adds or amends exactly one ADR Markdown file and nothing
else. The document must also exist at the implementation commit. A plan
reachable only through a second parent cannot authorize an implementation.
Numbered docs require their ADR document at that commit.

One ADR is one decision; new stages or mechanisms require successor ADRs,
whose first line cites the predecessor. Programs have short plans/roadmap
indexes. A new ADR file requires a repository-wide unique number >=0630.
A sixth first-parent implementation fails; a plan leaving its ADR over400
lines fails. Reviewed count exceptions and exact path/blob document exceptions
require reasons and reviewers in candidate-pinned
ci/commit-subject-allowlist.json. Existing oversized ADRs are frozen by exact
blob identity; changing their bytes needs another reviewed entry. Missing
policy grants no exceptions, and malformed policy is HOLD.

The preflight entry point runs this history check before advisory evaluation.
Failure stops before database fetch or evaluation. Gate drivers adopt the
entry point separately under review; adding it does not rewrite or invalidate
an already running candidate. Architecture tests run on the plan before
the implementation is committed.

## Advisory evaluation inputs

One evaluation binds the exact candidate, SHA256 of Cargo.lock and
nilket/Cargo.lock, a clean RustSec database at an exact Git commit, candidate
allowlist bytes, pinned tool/config/script policy, UTC time and live/replay
mode. Scope is these two Rust lockfiles, without a claim about non-Rust,
vendored, system or build-host packages.

The tool is cargo-audit0.22.2 with a reviewed binary SHA256 and unfiltered
configuration. It runs from a neutral owned directory with isolated tool
environment and explicit configuration, once per lockfile, using JSON,
no-fetch, the owned database and no-yanked. There are no native ignore,
severity, target or OS filters. No lock generation, update, build or lockfile
write occurs; the gate checks lock bytes before and after.

Yanked registry status is NOT CHECKED. Informational unmaintained, unsound and
notice records are retained as nonblocking warnings.

### Verdict and reproducibility

An active vulnerability is nonwithdrawn and affects the package version.
Raw findings and authorization remain separate.

HOLD means an input, tool, fetch, lock coverage, configuration, schema or
allowlist failure; HOLD never means a clean evaluation. FAIL means an active
vulnerability lacks authorization. PASS means completed evaluation with all
active vulnerabilities authorized or absent.

Tool exit0 with zero vulnerabilities and exit1 with findings both flow to
authorization. Other exits, signals, timeouts and report/exit mismatches mean
HOLD. Both lockfiles are attempted after findings or an error.

Live evaluation requires a confirmed upstream fetch at the recorded time;
failure cannot fall back to cache. Tool live-staleness refusal stays enabled.
Replay uses a clean archived database, explicit stale mode and pinned time,
and makes no freshness claim. Identical candidate/database/allowlist/policy/
time inputs reproduce the verdict.

### Allowlist

The candidate's ci/advisory-allowlist.toml starts empty. Each entry names an
exact advisory id and crate, nonempty reason/reviewer and UTC approval/expiry.
Authorization is approved_at <= time < expires_at. Expiry is later than
approval and at most30 days later. Unknown fields, wildcards, missing fields,
future approvals and duplicate keys make the whole input HOLD. Expired
entries authorize nothing; unused expired entries produce a warning.

Each key applies across both lockfiles and all versions of the crate. A
runtime exposure cannot borrow an unrelated build-only exception. Exceptions
are individually reviewed changes backed by code-path reasons; raw reports
remain intact and a wrong crate is never authorized.

### Placement and receipts

Adopted advisory preflight runs before staging and again with a fresh live
fetch immediately before fast-forward/push. FAIL or HOLD blocks publication.
A new database is a new evaluation input, without invalidating Rust test
receipts or justifying test reruns.

Receipts retain candidate/source/lock hashes, database/fetch identity, mode,
policy, time, raw reports and hashes, classifications, authorization and
verdict, with yanked explicitly unchecked. Historical replay and controls
use TEST/old-snapshot labels, never a current-exposure claim.

## Daily advisory watch

The watch freshly fetches origin/main, confirms that exact source and
evaluates both lockfiles live. Failed fetching never scans a cached ref.
It never edits source, fixes dependencies or pushes.

An incident key is advisory id, crate and lockfile. Presence determines OPEN;
authorization is independent, so expiry cannot hide a finding. Reintroduction
increments generation. New presence, affected-version changes and permission
loss alert; database churn alone does not. Resolution is recorded and sent as
information. Fetch, missing input, parse/tool/crash and missed-run errors
create durable health incidents, deduplicated until recovery while preserving
the first reason.

One serialized writer atomically persists state, monotonic event sequence,
event UUID and per-recipient outbox rows by fsync/rename and directory fsync.
Idempotency keys derive from event UUID and recipient and are durable before
sending. Repeated observations create no new logical incident; uncertain
delivery reuses the same key. Canonical message id, stored/submitted and
acknowledged are distinct. One recipient's success cannot erase another's
pending row.

Both claude and codex receive chatd requests in advisory-watch. The dedicated
sender advisory-watch requires separate registration because chatd prohibits
self-send. Controls and historical inputs route only to advisory-watch-TEST
with TEST/old-snapshot prefix. Stub transport controls send no production
alerts; actual delivery/ACK routes require rehearsal before activation.

### Runtime and availability

Owned user systemd templates provide a daily timer with at most15 minutes
random delay and persistent catch-up. The checker is oneshot, bounded by
TimeoutStartSec15min, stop30s and control-group kill, with resource caps in its
owned slice. OnFailure invokes a separately bounded2min health handler that
persists an incident and outbox after checker timeout/crash.

With user manager, network and enabled timer available, detection is within
24 hours plus jitter and runtime. Linger is an operator decision. Persistent
catch-up waits for the manager; no unconditional boot guarantee exists.
A disabled timer cannot detect itself. A later invocation reports last
success older than26 hours; advisory preflight reports age or unknown status
as a warning, not HOLD. No independent always-on watchdog is implied.

Run artifacts are bounded (default7, configurable1..30); durable incidents,
event payloads and pending deliveries are retained. Owned source-cache
pruning cannot break retained independent checkouts.

## Acceptance

History controls cover accepted plan/implementation, fix/test/numbered docs and empty
ranges; refused scopes, prefixes, capital first summary characters, delivery/incident labels,
parentheses, identity mismatch, missing history, orphan implementation,
size/count limits, exact reviewed exceptions, duplicate new ADR numbers,
message bodies, non-plan files in a plan and second-parent-only plans.
Interleaved plans and multiple implementations are positive controls. Refused history must prevent
the advisory evaluator from starting.

Advisory controls cover both full lockfiles, optional/dev/target-only
findings, raw report identity, pin mismatches, malformed/empty databases,
live fetch refusal, isolated ambient ignores, exit/report mismatches,
allowlist keys/time/expiry and unchanged lock bytes.

Watch controls cover recurrence, expiry/renewal, version changes, resolution,
fetch failure without stale scans, missed runs, repeated outage/recovery,
independent recipients, crash boundaries around sends/receipts, key reuse,
serialized writers and retained pending rows. Isolated actual systemd
controls prove checker/child timeout, empty cgroup and bounded health handler.
Historical reports remain FAIL when residual vulnerabilities remain.

## Delivery and follow-on

This advisory plan and implementation are prepared as a pair. Existing reviewed advisory
evaluator/watch files retain their bytes when the unpublished stack is
consolidated; the history linter and preflight entry point are new code.
Execution evidence stays outside Git.

Templates remain dormant pending publication and separately reviewed sender
registration, actual TEST delivery/ACK, the operator's linger decision and
gate-driver adoption. Trust policy and source-level advisory remediation get
ADR0640 and ADR0641 plan/implementation pairs. Pure lockfile patches may use
fix subjects. Frozen remediation commits and their running gate stay intact.
