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
| PD-10 | `stream_finalization` (`stream_finalization_replays_after_floor_and_stays_ordered_under_write_load`) | 4/4 | replayed sequence differs | none yet | open |
| PD-11 | `multinode_bootstrap` (binary) | 4/4 | 1-4 of 5 tests fail (join rejected by seed with a leader hint; epoch wait timeout), or the binary aborts with SIGABRT and prints no result line | none yet | open |
| PD-12 | `chaos::chaos_smoke_catalog` | 2/4 | timed out waiting for leader 1 (current leader 2) | none yet | open |
| PD-13 | `toron` `distributed_process_test` (binary) | 0/25 since | historically two concurrent tests parked every process in a futex wait that never returned | unexplained; the serial nextest group was removed so the binary keeps being tested, and ladder v5 stresses it | open |

The leader-replay test
`network::data_server::tests::fresh_leader_recovers_peer_witness_before_duplicate_write_replays`
passed in both lib runs at 1024 threads; it stays a candidate from earlier
reports, outside this register.

Closing an entry needs its fix commit and a stress result at the same
concurrency (nextest `--stress-count` and repeated in-process libtest runs),
recorded here.
