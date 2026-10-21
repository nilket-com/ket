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
| PD-01 | `storage::maintenance_policy::tests::three_deterministic_failures_quarantine_and_only_requeue_clears_it` | 2/2 | metric delta 6 or 9, expected 3 | asserts a delta on a process-global counter that concurrent tests also increment | open |
| PD-02 | `storage::manager::tests::a_compaction_tick_is_policed_and_its_success_is_recorded` | 2/2 | metric delta 3 or 6, expected 1 | same as PD-01 | open |
| PD-03 | `network::data_server::tests::table_write_seals_secondary_index_active_buffer_after_slab_rotation` | 2/2 | `[SlabId(0), SlabId(1)]`, expected `[SlabId(0)]` | none yet | open |
| PD-04 | `consensus::raft_storage::recovery_tests::checkpoint_coordinator_flushes_on_timer` | 1/2 | checkpoint `None` after the timer | timer deadline under CPU contention | open |
| PD-05 | `storage::manager::tests::append_durable_coalesces_concurrent_fdatasyncs` | 1/2 | 2 fdatasyncs, expected 1 | the 20 ms group-commit window misses the second append under contention | open |
| PD-06 | `network::data_server::tests::no_match_full_scan_progresses_per_examined_location` | 1/2 | `ReadStatus(17)`, expected `ReadStatus(1)` | none yet | open |
| PD-07 | `storage::mutation_visibility::root2::provisioning::tests::owner_log::a_forked_child_cannot_extend_a_dropped_owner_lock` | 1/2 | fork fails with `ENOMEM` | unverified; two competing hypotheses: (a) stack address space reserved by many test threads under heuristic overcommit; (b) a memory-cgroup charge failure at fork (the unit's MemoryMax/MemoryHigh, or the shared slice), which the kernel reports as ENOMEM | open |
| PD-08 | `network::data_server::tests::table_write_populates_building_secondary_index_active_buffer` | 1/2 | `NotFound` on a file | none yet | open |
| PD-09 | `network::control_server::tests::maintenance_control_is_node_local_authenticated_and_durable_across_reopen` | 1/2 | `DataDirectoryBusy` on reopen | a forking lib test's child briefly holding the inherited flock (the F-OWN-1 fork window); needs a control | open |
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
adoption. The analogous replica `reclaim_gate2` source pattern is a separate
unproven followup: retained idle entries are plausible, while duplicate
admission or wrong counter values have not been established. Its audit and
proposed deterministic control are recorded in
`reviews/readiness-counter-replica-gate-source-audit-20261021.json`.
