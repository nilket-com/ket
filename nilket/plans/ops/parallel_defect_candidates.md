# Parallel-only defect candidates

Status: open register. Policy: `nilket/plans/ops/test_parallelism.md` (section
4: a parallel-only failure is a defect candidate, never a flake, and never
fixed by serialization; section 7: these block the adoption gate).

Found 2026-10-20 on slim when cargo-cgroup's default moved to 1024 test
threads (CPUS 0-7, quota 800%, another session sharing CPUs 0-11). Every test
below passed when run alone by exact name (`-- --exact`, 5/5 for the lib tests
and 3/3 for the integration tests), so concurrency is the trigger. A
hypothesis below is unverified until a control confirms it.

Repro, lib (PD-01 to PD-09):

```bash
TMPDIR=$CARGO_TARGET_DIR/tmp CARGO_CGROUP_CPUS=0-7 CARGO_CGROUP_CPU_QUOTA=800% \
  nilket/scripts/cargo-cgroup test --locked -p wendelon --lib --no-fail-fast
```

Repro, integration (PD-10 to PD-12): the same with `--test <binary>`.

Rates are failures out of full runs with the open-file limit fixed: two lib
runs (one with the memory and task caps lifted for measurement, one at the
final defaults) and four runs of each integration binary. A third, earlier lib run with a
1024 open-file soft limit failed 484 tests on EMFILE and is not counted.

| ID | Test | Rate | Symptom | Hypothesis (unverified) | Status |
|---|---|---|---|---|---|
| PD-01 | `storage::maintenance_policy::tests::three_deterministic_failures_quarantine_and_only_requeue_clears_it` | 2/2 | metric delta 6 or 9, expected 3 | asserts a delta on a process-global counter that concurrent tests also increment | `233760152`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-02 | `storage::manager::tests::a_compaction_tick_is_policed_and_its_success_is_recorded` | 2/2 | metric delta 3 or 6, expected 1 | same as PD-01 | `233760152`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-03 | `network::data_server::tests::table_write_seals_secondary_index_active_buffer_after_slab_rotation` | 2/2 | `[SlabId(0), SlabId(1)]`, expected `[SlabId(0)]` | none yet | `611dede80`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-04 | `consensus::raft_storage::recovery_tests::checkpoint_coordinator_flushes_on_timer` | 1/2 | checkpoint `None` after the timer | timer deadline under CPU contention | `7c7686ffa`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-05 | `storage::manager::tests::append_durable_coalesces_concurrent_fdatasyncs` | 1/2 | 2 fdatasyncs, expected 1 | the 20 ms group-commit window misses the second append under contention | `4b1866b23`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-06 | `network::data_server::tests::no_match_full_scan_progresses_per_examined_location` | 1/2 | `ReadStatus(17)`, expected `ReadStatus(1)` | none yet | `ab32e6e91`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-07 | `storage::mutation_visibility::root2::provisioning::tests::owner_log::a_forked_child_cannot_extend_a_dropped_owner_lock` | 1/2 | fork fails with `ENOMEM` | unverified; two competing hypotheses: (a) stack address space reserved by many test threads under heuristic overcommit; (b) a memory-cgroup charge failure at fork (the unit's MemoryMax/MemoryHigh, or the shared slice), which the kernel reports as ENOMEM | `d048ba810`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-08 | `network::data_server::tests::table_write_populates_building_secondary_index_active_buffer` | 1/2 | `NotFound` on a file | none yet | `611dede80` + `df86421f6`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-09 | `network::control_server::tests::maintenance_control_is_node_local_authenticated_and_durable_across_reopen` | 1/2 | `DataDirectoryBusy` on reopen | a forking lib test's child briefly holding the inherited flock (the F-OWN-1 fork window); needs a control | `0453d30e8`: implemented; exact focused receipt below; loaded/final adoption pending |
| PD-10 | `stream_finalization` (`stream_finalization_replays_after_floor_and_stays_ordered_under_write_load`) | 4/4 before oracle repair | replayed sequence differs | write-only expected list omitted other valid Finalize entries; historical unexpected entry identities not recovered | `dd936a775`: full committed-prefix oracle plus real heartbeat controls; focused stress passed, final candidate gate pending |
| PD-11 | `multinode_bootstrap` (binary) | 4/4 before scoped repair | join rejection, epoch wait timeout, or SIGABRT without result | completed self-JOIN retry after leader handoff reproduced separately | `e0edea2a0`: guarded completed-state replay; focused stress passed; incomplete epoch, replacement timeout and abort remain separate/open |
| PD-12 | `chaos::chaos_smoke_catalog` | 2/4 before fix | timed out waiting for leader 1 (current leader 2) | fixture assumed founding-node leadership | `e508e8a4d`: elected-leader fixture; focused stress passed, final candidate gate pending |
| PD-13 | `toron` `distributed_process_test` (binary) | 0/25 since | historically two concurrent tests parked every process in a futex wait that never returned | unexplained; the serial nextest group was removed so the binary keeps being tested, and ladder v5 stresses it | open |


## Additional candidates PD-14–19 (reconciled 2026-10-21)

These entries reconcile the implementation history with the original register.
A fix commit alone does not close a candidate: required concurrency receipts
and final stack acceptance remain pending. Source/evidence inventory:
`reviews/readiness-uring-snapshot-20261021/REPORT.md`.

| ID | Candidate / observed failure | Recorded context | Implementation / remaining evidence |
|---|---|---|---|
| PD-14 | `expired_table_outcome_keeps_warm_cache_replay_but_cold_replay_is_indeterminate`: premature EXPIRED | 1/5 full-lib runs at 1024 threads | `919f4170f`: maximum supported fixture TTL and deliberate hook-driven expiry; closure receipts pending |
| PD-15 | `full_scan_completes_far_beyond_the_former_lifetime_cap`: QUERY_STALLED | 1/1 loaded run at 24 threads | `ad3812629`: explicit scan-test stall budget and per-location progress assertions; product stall-default decision and closure evidence remain separate |
| PD-16 | `single_node_bootstrap_starts_background_tiering_when_l2_is_configured`: L1 file remains | 1/3 diagnostic full-lib runs at 1024 threads | `d73b8b651`: wait for actual unlink rather than earlier tier-pointer installation; closure receipts pending |
| PD-17 | `post_dispatch_expiry_resolves_and_replays_as_one_indeterminate_operation_after_restart`: KNOWN_NOT_COMMITTED instead of INDETERMINATE | 1/3 diagnostic full-lib runs at 1024 threads | `e434342c7`: hold append after dispatch through caller expiry; closure receipts pending |
| PD-18 | `shutdown_lease_node`: fixture shutdown timeout | Four simultaneous failures in one of nine full-lib runs at 1024 threads; five with additional disk load | `6abc3b464`: fixture hang detector above component shutdown budgets; closure receipts pending |
| PD-19 | Restart reports foreign WAL record / duplicate index | Exact failing test and measured rate not recovered; unknown | `28f4dbaf7` independently reproduces and repairs queued bare-fd reuse; connection to this restart failure is unconfirmed. The separate enter-error P1 below does not establish causality. Retained restart evidence and closure remain required. |

Existing-entry corrections from source history: PD-08 has both fixture-lifecycle
isolation (`611dede80`) and same-path sidecar publication (`df86421f6`) fixes.
PD-09's fork-window hypothesis was refuted by `0453d30e8` (5/5 immediate reopen
failures without fork); joined shutdown addresses retained ownership. PD-07's
`d048ba810` removes the fork mechanisms without identifying which kernel ENOMEM
cause occurred. These observations do not substitute for closure receipts.

Sidecar deletion/retirement ordering and orphan staging-file recovery now have
separate implementations and focused receipts below. Neither is closed by the
publication UUID or io_uring fixes alone; full adoption evidence remains pending.
See the snapshot report and `SIDECAR-STRIPE-ASSIGNMENT.md`.

The leader-replay test
`network::data_server::tests::fresh_leader_recovers_peer_witness_before_duplicate_write_replays`
passed in both lib runs at 1024 threads; it stays a candidate from earlier
reports, outside this register.

Closing an entry needs its fix commit and a stress result at the same
concurrency (nextest `--stress-count` and repeated in-process libtest runs),
recorded here.

## Additional P1: io_uring enter-error lifetime (2026-10-21)

Found in the independent review of fd-retention fix `28f4dbaf7`. The enter-error
path predates that fix: `submit_and_wait` errors called `drain_with_error`, which
returned or freed buffers before outstanding kernel requests had completed.
An `EINTR` while waiting is not cancellation or a CQE. This finding is separate
from the suspected PD-19 cause; neither review establishes PD-19 causality.

Implementation in `storage/io_uring.rs` and `storage/io_uring/lifetime.rs`:

- Retry `EINTR`, `EAGAIN` and `EBUSY`, reaping available CQEs and retaining all
  other requests, files, owners and permits.
- On a terminal enter error, stop admission and attempt bounded synchronous
  cancellation. Only a request's actual CQE authorizes releasing its resources;
  even a successful cancel-all return is not treated as a per-request proof.
- Without a CQE, retain the allocation, file, owner and permit until process
  exit, including on worker unwind. Wake the waiter with an error and no
  buffer. For unresolved writes, retained write permits prevent a false clean
  shutdown or LOCK release. The existing shutdown budget and process watchdog
  apply once shutdown begins; terminal ring failure does not initiate shutdown.
  Older kernels without synchronous cancellation take this same safe path.

Regression evidence: inject `EINTR` after submitting a real socket read while
its peer has supplied no bytes. The old handler returned `-EINTR` (red); the
fixed handler waits for the real read completion. Additional controls cover
resource-pressure errors, real cancellation, unsupported cancellation, a
successful cancellation return without a CQE, worker unwind, and a receiver
waker that panics while other kernel requests remain pending. The retention
controls use child processes so intentional quarantine does not leak resources
into the rest of the test suite; they do not serialize concurrent tests.

Status: implemented in the parallel-defects worktree; focused validation and
stress receipts accompany the review package. This does not close the v5
adoption gate or replace the pending full ladder and audit.

Local validation (CPU bounded; no added serial settings): all 12 io_uring
tests passed in each of 30 nextest stress iterations (360 executions), and
io_uring + file_ops + ownership passed 10 in-process runs at 1024 test threads
(28 per run, 280 executions). Architecture tests passed 180 + 42. Raw logs,
the patch and source hashes are retained in
`reviews/codex-uring-p1-fix-20261021-1/`. No ignored tests were added.

Independent Astra source review approved the scoped lifetime fix with no
must-fix correctness findings; its report is retained as `astra-review.json`
in the package above. Three optional coverage additions now assert known-CQE
reclamation without cancellation support, terminal EIO through the real worker
with queued requests and subsequent admission refusal, and explicit close
failure/no clean-shutdown record with a quarantined write permit. These tests
strengthen coverage; they do not change the production implementation.

Followup receipt: `reviews/codex-uring-p1-followup-20261021-1/review.json`
records 15 io_uring tests across 30 nextest iterations (450 executions), and
31 io_uring/file-operations/ownership tests across 10 in-process runs at 1024
threads (310 executions), all passing. These are retained focused receipts;
the full v5 ladder/audit and candidate closure remain pending.

PD-12 focused receipt (2026-10-21): signed fix `e508e8a4d`, reviewed chaos
source hash `c01703296d512c0729f550697bd9e805e437116435626e265c84615fd87869dd`.
The complete five-test chaos binary passed 30 nextest repetitions (150
executions) and 10 in-process repetitions at 1024 threads (50 executions).
Receipts: `reviews/readiness-chaos-focused-stress-20261021/summary.json`.
A new real-majority-failover control exercises nonfounder leadership; its
initial election-only setup failure is retained in the firstpass evidence.
This is focused concurrent evidence, not a full-suite loaded reproduction
or a completed v5 adoption gate. The signed enter-error P1 checkpoint is
`58eb2d68a`; neither commit establishes PD-19 causality.

PD-10 focused receipt (2026-10-21): signed test repair `dd936a775` leaves
product behavior unchanged. It injects real heartbeat finalizations before
replay and after subscription, derives all expected receipts from independent
raw committed entries through the final write, and compares the entire marker
vector with strict ordering and exactly-once write/heartbeat checks. Collection
has a count cap and correctness hang detectors. Both stream tests passed 30
nextest repetitions (60 executions) and 10 in-process repetitions at 1024
threads (20 executions). Receipts:
`reviews/readiness-pd10-focused-stress-20261021/summary.json`.

Sidecar ordering followup: signed `4b7a17ae0` orders delete and replaced-slab
retirement against same-key publication. A deterministic missing-guard RED
control reached unlink while publication was parked; the fresh GREEN executable
passed. All five publication tests passed 30 nextest repetitions (150 executions)
and 10 in-process repetitions at 1024 threads (50 executions); catalog and
architecture checks passed. Receipts:
`reviews/readiness-sidecar-focused-stress-20261021/summary.json`. A stale RED
artifact reused by the first GREEN attempt was rejected as invalid provenance;
package artifacts were invalidated and actual fresh GREEN execution verified.
The repair preserves restored stored-path behavior. Logical late resurrection,
blocking publication work after cancellation, and read/publication consistency
remain separate unresolved questions; this is ordering evidence only. Startup
orphan recovery has its own scoped implementation and receipt below.

PD-19 evidence recovery: the stopped bigfoot hunt has 40 historical runs at
intended source `df86421f6`, before both io_uring fixes. One retention reopen
reported a WAL checksum mismatch; the implicated segment was not retained.
This is not a current-source rate or causal proof. See
`reviews/readiness-focused-reviews-20261021/pd19-hunt/REPORT.md` for exact
provenance limits and all 22 failing test names. Neither old failures nor its
one passing run close the current candidates.

Orphan sidecar recovery receipt (2026-10-21): signed `c5b9db554` performs
startup-only cleanup under the directory owner, before producers start. It
recognizes only the publisher's UUID-v4 temporary spelling at the exact sidecar
directory depth; descriptor-relative traversal does not follow child symlinks.
Committed catalogs/segments, legacy temporary names, unrelated files and
malformed paths remain intact. Cleanup retains ownership and the write permit
through cancellation, and fsyncs a changed leaf even after a later sweep error.
This assumes an operator-trusted root and cooperative ownership; it does not
claim protection against arbitrary hostile filesystem mutation.

Independent Astra production review and Sol regression review passed. Actual
publisher interruption, repeated reopen, ownership cancellation, symlink and
partial-failure controls are included. Architecture passed 180/180; the exact
11-test selection passed 30 nextest repetitions (330 executions) and 10
in-process repetitions at explicit 1024 threads (110 executions). Receipts:
`reviews/readiness-sidecar-orphans-stress-r4-20261021/summary.json`. The initial
179/180 architecture failure is preserved; adding the new production module to
the pinned module census restored the guard without weakening it. The tests'
child-process guard bounds execution and reaps children on cancellation. These
are focused checks, without additional background suite load; full-stack
concurrency adoption and lifecycle/read-coherence followups remain pending.

PD-11 completed-state retry receipt (2026-10-21): signed `e0edea2a0` repairs
the reproduced self-ID rejection when an already joined voter becomes leader
and receives its matching request through a follower. The authenticated request
passes ordinary framing, shape and protocol checks; the self path then requires
a fresh quorum/applied-state fence and a coherent completed membership/epoch
proof before returning success without JOIN mutation. Joint/incomplete state,
raw field mismatches, incompatible protocol, unauthorized requests and stale
former leaders remain refused. This is current-state idempotence; no historical
operation ID or actor-key identity is invented.

The exact primary RED reached reason-5 self-ID rejection with completed epoch3.
GREEN preserves both retry acceptance and the original pending bootstrap's
success. Three pure proof tests and the nine-test bootstrap firstpass passed;
all nine bootstrap tests then passed 30 nextest repetitions (270 executions)
and 10 in-process repetitions at explicit 1024 threads (90 executions). All
180 architecture checks passed. Receipts:
`reviews/readiness-pd11-self-replay-r5-stress-20261021/summary.json`.

Two earlier incomplete-stage controls failed while establishing leadership,
before invoking the retry; those failures are retained. Diagnostics exposed
a candidate behind its surviving voter's log. The reviewed setup now partitions
in stages and waits for the joining voter to apply a fresh incumbent storage
prefix before the second cut, checking the incumbent's core leadership/vote
remains stable. A proposed cached-metrics bound was rejected in source review
before execution. These changes preserve the original timeout and election
settings. Focused passes do not close the independent incomplete-epoch handoff,
replacement follower epoch timeout, unexplained SIGABRT, full-suite loaded
evidence or final v5 adoption gate.

PD-19 capture prerequisite: signed `290131659` retains the exact retention
fixture on reopen error before temporary-directory cleanup and optionally copies
root artifacts to an explicitly configured persistent capture directory. It
records per-file pre/copy/post BLAKE3 hashes, identity/namespace checks and the
expected raw entries; partial errors retain the original directory and original
error. This is a checked per-file copy, without an atomic-snapshot or global
writer-quiescence claim. All three forced-error controls passed. Receipts:
`reviews/readiness-pd19-capture-focused-20261021/summary.json`. No current-source
retention hunt or PD-19 causal closure follows from those helper controls.

## PD-20: counter write-key gate reclamation (2026-10-21)

The historical dropped-caller counter test failed its idle gate-count assertion
5/40 times in the old hunt. A separate current-source deterministic regression
now proves an idle gate leak: Tokio unlock can synchronously wake a terminal
waiter before destroying the old guard's mutex Arc. Both destructors observe
extra strong references and skip reclamation, leaving a map-only entry. The
test uses the real mutex, a bounded waker handshake and a second OS thread;
phase, exact reference counts and one wake are verified before the strict idle
map assertion. Sole-owner and after-owner-destruction controls pass. This proves
the mechanism without assigning it to the historical failure unconditionally.

Signed repair `14bfbbb0c` reserves explicit logical admission tickets before
waiting.
Queued cancellation and acquired guards retire one ticket each; unlock runs
outside the map lock, then the final logical participant removes the matching
entry. Mutex reference counts no longer determine liveness. Nine reclamation
controls and 17 write-key/crash-cut tests passed; the exact 26-test selection
passed 30 nextest repetitions (780 executions) and 10 in-process repetitions at
explicit 1024 threads (260 executions). Receipts:
`reviews/readiness-counter-gate-logical-stress-20261021/summary.json`.

The initial architecture result was 179/180 because its canonical-key map guard
pinned the old direct mutex field type. The reviewed correction follows the
new alias and retains the exact canonical-key map and mutex checks. The failed
receipt remains; the fresh complete architecture run passed 180/180. Combined
receipt: `reviews/readiness-counter-gate-logical-r2-architecture-20261021/summary.json`.
These focused checks have no extra background suite load and do not close final v5
adoption. The analogous replica `reclaim_gate2` pattern was subsequently
reproduced and repaired separately as PD-21 below. Duplicate admission or wrong
counter values have not been established. The initial source-only audit remains
`reviews/readiness-counter-replica-gate-source-audit-20261021.json`.

PD-01–09 focused reconciliation (2026-10-21): all implementing commits above
are ancestors of the signed integration stack, and all nine candidate test
bodies plus the five inspected mechanism helpers match their fix postimages.
The exact nine original candidates passed a firstpass at eight threads, 30
nextest repetitions (270 executions), and 10 in-process repetitions at explicit
1024 threads (90 executions). Frozen metadata reused the existing executable;
no build, source edit or retry was needed. Receipts:
`reviews/readiness-pd01-09-focused-20261021/summary.json`; implementation and
remaining-prerequisite matrix:
`reviews/readiness-pd01-09-reconcile-20261021.json`. No extra background suite
load was present, so these receipts do not replace loaded closure or final v5
acceptance. Original table hypotheses remain historical where later source
notes above refute or narrow them.

Read-stall diagnostic (2026-10-21): the exact row-limit, whole-record cancelled
consumer and typed midstream-error tests passed 18 baseline executions and 39
executions under a recorded background nextest run of the remaining Wendelon
library. Source and executable hashes stayed fixed. The background was
intentionally terminated after foreground completion; it is not a full-suite
pass. Receipt: `reviews/readiness-read-stall-execution-r2-20261021/summary.json`.
This lower-width process load does not recreate the old wide in-process hunt
or establish why the historical failures disappeared. No stall budget or
production timeout was changed.

## PD-21: replica operation gate reclamation (2026-10-21)

Three deterministic regressions reproduce idle replica gate retention: terminal
rollback inside owner unlock, assigned-waiter cancellation inside unlock, and
assigned-waiter cancellation after owner release. Each retains one map-only
mutex. Three controls pass, including actual tentative/rollback/commit pending
retention. Historical RED uses a manual owner with production drop ordering
and a real rollback waiter; it is not a full owner RPC reproduction. No wrong
counter value or duplicate mutation is established. Original source, executable
and receipts: `reviews/readiness-counter-replica-red-20261021/summary.json`.

Signed repair `e95ddbe06` replaces mutex reference-count cleanup with logical
admission tickets reserved before waiting. Unlock runs outside map locks; final
retirement checks pending state freshly under the admission-map shard, retaining
gates for pending tentative data. Queued and assigned cancellation retire their
tickets. The test pause helper also holds a counted admission. Regressions retain
the original pending-to-empty handoff and add a waiter creating pending during
unlock, which guards against a stale pending check. Independent source review:
`reviews/readiness-replica-gate-repair-review-20261021-r2.json`.

Firstpass: 141 passed and one existing measurement ignore; architecture: 180
passed. The exact 26-test selection passed 30 nextest repetitions (780 executions),
10 in-process repetitions at explicit 1024 threads (260), three baseline runs
(78), and three runs overlapping recorded background library load (78). Source
and executable hashes remained unchanged. The background run was deliberately
stopped after foreground completion and is not a full-suite pass. Receipts:
`reviews/readiness-counter-replica-green-20261021/summary.json`. Focused repair
evidence does not close final stacked v5 adoption or establish storage append
cancellation atomicity.

PD-11 forwarding prerequisite (2026-10-21): signed integration `0194ccc31`
repairs a separate reproduced routing gap. When epoch 2 lacks promoted leader 3,
a follower retry previously failed before entering that leader's JOIN handler;
a direct-ingress control reached it and safely refused incomplete JOIN. The
JOIN-only fallback now uses the applied uniform voter membership's trusted
address, rechecks the leader hint, and preserves the normal epoch route and
authentication errors. It grants no JOIN acceptance or recovery mutation.

The first stress candidate failed once on completed handoff repetition 27,
before forwarding: survivor log tail 147 exceeded the joining candidate's 146.
That failure, source and executable remain retained. The reviewed fixture
revision extends existing staged partition and applied-prefix checks to both
handoff cuts, preserving completion/bootstrap assertions and all deadlines.
Final focused receipts: 13 firstpass, 180 nextest stress executions, 60
in-process executions at explicit 1024 threads, four baseline and eight loaded
controls, all passed with zero selected ignores or retries. Earlier architecture
180/180 is reused on unchanged production/guard bytes. Background load was
deliberately stopped, not a full-suite pass. Evidence:
`reviews/readiness-pd11-forwarding-20261021/r2/summary.json`; independent audit:
`reviews/readiness-next-cut-independent-evidence-audit-20261021.json`.
Incomplete epoch recovery, replacement timeout, historical abort, startup
lifecycle cleanup and final stacked v5 adoption remain separate/open. State
nonmutation checks observe the follower plus shared promotion-call count,
not direct leader durable-state closure.

PD-16 focused load evidence (2026-10-21): the actual-unlink regression passed
one firstpass, three baseline runs, 30 nextest repetitions, ten in-process runs
at explicit 1024 threads and three loaded runs, with zero selected ignores or
retries. Receipt: `reviews/readiness-pd16-focused-20261021/summary.json`.
The frozen prior executable's node/tiering/unlink sources match integration
`e92a84dd7`; three replica files differ. Later startup changes are outside this
receipt, which is not a full current-stack pass. Loaded
checks sampled only background-nextest descendants (minimum 2/7/7), with sparse
sampling because the foreground test takes about 0.1–0.25 seconds. Background
runs were deliberately stopped. An initial ancestry-sampler setup failure
stopped before loaded execution; only previously unrun loaded samples followed.
Final stacked v5 acceptance remains pending.

PD-11 startup ownership prerequisite (2026-10-21): signed `b704a10f3` owns
the five existing background task handles and transports during post-publish
cluster initialization/JOIN through visibility. Failure or cancellation drains
admission synchronously, retains tracked cleanup and prevents directory closure
until actual joins finish. Successful startup transfers the same handles back.
Typed incomplete errors retain their original kind and cause; observer timeout
does not cancel cleanup. Earlier startup prefixes and later node construction
remain outside this guard. Provider-only probes make no socket-closure claim.

The original failed/cancelled JOIN controls each retained five live tasks,
open write admission and a busy directory before explicit fixture teardown.
The reviewed repair passed 23 firstpass checks, 180 architecture checks, four
existing multi-node JOIN regressions and 506 repeated/control executions
(330 nextest, 110 in-process at explicit 1024 threads, 33 baseline, 33 loaded).
Recorded background load was deliberately stopped, not a full-suite pass.
Evidence: `reviews/readiness-pd11-startup-green-20261021/summary.json`; final
audit: `reviews/readiness-pd11-startup-final-independent-evidence-audit-20261021.json`.

The WAL5 compatibility prerequisite is signed `063217be6`: a test-only frozen
27-variant spine and 29 immutable fixture samples, checked against production
replay and suffix refusal. All 28 selected compatibility controls and 180
architecture checks passed. Receipt: `reviews/readiness-pd11-conditional-20261021/summary.json`.
The samples do not freeze every nested payload shape; changed historical leaves
must be forked before a successor decoder is activated. No format bump or
conditional recovery command is emitted. Incomplete-epoch recovery, cluster
command-support gating and final stacked v5 acceptance remain open.

## Review candidates PD-22–25 (2026-10-22)

Independent source review found these pre-existing candidates. None has a RED
execution or confirmed measured rate yet. They are registered for investigation,
not claimed as Cut A closures or regressions. Source reviews:
`reviews/claude-review-sidecar-20261022/review.md` and
`reviews/claude-review-join-startup-20261022/review.md`.

| ID | Review name | Candidate and required first proof | State / next cut |
|---|---|---|---|
| PD-22 | S1 | Sidecar file identity omits the logical schema fingerprint present in the catalog key. Confirm or refute that an object id changes on every fingerprint change; if not, reproduce new publication followed by old-row retirement unlinking the live file. A missing file currently loads as `Ok(None)`, so this may silently lose index data. | HIGH; identity narrower than catalog identity is held for proof. RED first, highest-priority cut after A; no parallel-load reproduction yet. |
| PD-23 | S2 | Active-buffer sealing may publish after GC retires its source slab because sealing has no source-slab lease/claim. Reproduce a publish/retire overlap and inspect the catalog row and coverage for the retired slab. | Unconfirmed; RED first after PD-22; no parallel-load reproduction yet. |
| PD-24 | S3 | Catalog failure after sidecar rename removes the new file while an old row may still name that path. Inject the catalog failure after a same-path replacement and prove whether the old row loses its file. | Unconfirmed; RED first after PD-22; no parallel-load reproduction yet. |
| PD-25 | J1 | Startup ownership guard currently spans only the initialization/JOIN block. Earlier producer/transport setup and later epoch reconcile, tenant seeding and LSM-worker construction failures may leave writes admitted, serving transports, detached tasks and LOCK held. Inject failures at both prefixes and observe actual closure. | Open, source-confirmed scope gap; separate Cut B contract or later cut. No parallel-load RED yet. |

Every confirmed candidate needs its RED before repair. PD-22 through PD-25
block the default flip until each is repaired after a RED or refuted with
evidence; parallel-load reproduction is an additional classification, not a
condition for blocking. The target flip uses the cpuset CPU count plus an
explicit 1024 stress lane. Cut A retains default 1 and runs the v4 gate.

Additional source-review notes, not yet demonstrated defects: forwarded JOIN
has no hop limit; the counter-key `locks` map does not shrink; the test-only
`CounterReplica` harness can remove gates with queued waiters; leader and replica
gate-drop invariant failures use different release-build policies. Record a
separate RED and scope before promoting any note to an implementation change.

PD-25 oracle note: `startup_ownership_tests.rs` computes `service_retained`
without asserting it. The later startup-prefix REDs must assert actual resource
closure, retaining a real-socket control alongside provider fixtures.

#### Cut C PD22 review notes: normalized reference checks and post-commit cleanup

The db3d24a62 repair deliberately keeps reference census fail-closed when any
other catalog row has an invalid relative-safe path. That malformed metadata is
outside the ordinary production writer contract; refusing the unlink exposes it
without assuming how a future restored-path normalizer may interpret it. This
can block delete/retire until the malformed legacy row is repaired. Follow-up:
classify and surface malformed restored paths in the legacy census, then consider
skipping proven non-aliasing entries with explicit diagnostics (review N1).

Old-path unlink or parent-fsync failure after a successful replacement catalog
commit is returned as an error. The new row is already durable; callers may retry
an idempotent publish. This intentionally reports uncompleted cleanup/durability
work rather than silently certifying it. It can leave an unreferenced legacy file
until an operator/census repair. Follow-up: distinguish publication success from
post-commit reclamation debt in a typed outcome if that debt can be scheduled and
observed durably (review N2). Neither note blocks the jointly reviewed source.


### Security incident follow-up: eMudhra trust roots and starter signing keys

- Advisory received by browser root programs: 2026-10-17 18:05 UTC ([Chrome primary report](https://blog.google/security/protecting-chrome-and-android-users-from-compromised-emudhra-intermediate-ca-certificates/)).
- Nilket exposure identified: 2026-10-22, bundled PostgreSQL TLS roots retained emSign ECC Root CA - G3. [RUSTSEC-2026-0381](https://rustsec.org/advisories/RUSTSEC-2026-0381) / [0383](https://rustsec.org/advisories/RUSTSEC-2026-0383) require roots/root-certs >=1.0.10.
- Cut C remediation prepared: both lockfiles update the two root bundles to 1.0.10, root absence regression; operator-facing starter keys use independent OS randomness with `allow_dev_keys=false` and consistent enrollment. Publication/fast-forward time: PENDING Cut C acceptance, not yet landed. Compute time-to-remediate from advisory, exposure detection, and actual publication separately.
- Lockfile metadata also reconciles existing key-guard serde_json/zeroize dependency edges in the root workspace (no package version change); polariton adds explicit dev dependencies for the two root bundle regression checks.
- Follow-up OPEN: gate deterministic `NodeBootstrapConfig::dev()` and `dev_*_signing_secret` behind test support after a complete integration-fixture caller/feature audit. This incident patch preserves the explicit in-code API; starter TOML never opts into those keys. Avoid globally forcing unrelated test-support behavior or dropping integration test names during that migration.


#### Cut C startup matrix pre-hook failure (OPEN, classification pending)

On signed B0 source `7fd0f944a`, in-process explicit1024 run4 failed after
three passing runs: `pd25_every_adopted_prefix_err_cancel_panic_releases_real_tls_socket_and_reboots`
did not reach `set_pending_finalization_reconciler` / Cancel within30s.
The other13 cases passed. Separate nextest30x14 passed. Receipt:
`/home/me/work/reviews/cut-c-b0-green-20261022/inprocess-04.log`.

The fixture discarded an early constructor outcome at this branch, so that
receipt alone cannot classify a production refusal, stalled progress, or a
fixture resource assumption. Diagnostic revision retains the original typed
error/join outcome and last checkpoint, including early completion racing the
notification. Reproduce under the same resources/concurrency; no timeout
widening, serialization, retry-to-green, or B0 stress acceptance while open.

The diagnostic reproduced on observation14 with exact `AddrInUse` (OS98),
`last_site=None`: refusal precedes all constructor fault hooks. Controlled
UDP occupancy reproduces the TCP-only reservation defect while startup
correctly fences/releases the owner. The fixture now binds both actual
listeners with `:0` and records the live TCP address before any injection;
there is no release/rebind gap and no bootstrap retry. The original run4's
exact early error was discarded and cannot be retrospectively recovered;
its attribution is consistent/probable, while diagnostic run14 is captured.
Closure remains PENDING the corrected full1024 stress cycle and review.

Healthy shutdown of a retaining provider does not yet withdraw the published
registration (pre-existing behavior; failed-startup withdrawal is repaired by
B0). Track a separate healthy-shutdown registration retirement change; current
lifecycle draining refuses new calls, but stale registrations may retain
service/storage aliases until the provider network is dropped.

#### Cut C slow-withdrawal control: observer-interference (captured)

R2 nextest iteration1 and in-process iteration1 both failed the panic half of
`pd25_slow_partial_publish_withdrawal_keeps_lock_but_not_trusted_workers`:
the active `Raft::is_initialized()` external request did not answer within the
unchanged5s observation bound. Original receipts remain in
`/home/me/work/reviews/cut-c-b0-r2-20261022/final/`.

The pinned-observer control reproduced on diagnosticR3 observation3:
`/home/me/work/reviews/cut-c-b0-slow-diagnostic-r3-20261022/full-003.log`.
BEFORE cancelling that observer, all registered handles were finished,
current cleanup stage was `provider withdrawal join`, and Raft metrics were
`Shutdown` / `Err(Stopped)`. That stage is written after the actual awaited
Raft shutdown, every task/factory join, lifecycle quiescence and transport
shutdown. Cancelling ONLY the probe left that completed stage unchanged,
with `foreign_released=false`. Trusted cleanup was already independent of
foreign work; the active API query was an invalid shutdown-completion oracle.
Claude confirmed the captured classification in durable chatd
`01a1651c-1e50-7440-9c75-e0bb218d1649`.

The repaired control waits on that passive producer stage, then requires the
finished handles and Shutdown metrics before releasing the foreign job. It
keeps the post-cleanup Raft API check after the owned cleanup joins and before
fixture teardown. The forever-withdrawal child uses the same passive ordering
proof. No bound increase, bootstrap retry or serialization is used. The original
STOPs and diagnostic failures are retained; GREEN closure remains PENDING a
complete clean nextest30 and in-process10 cycle at explicit1024 concurrency.
