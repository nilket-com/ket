# ADR 430 - Spill placement / cache control

## Context

The sf=10 strict TPC-H board is 19/22 green. ADR 429 established that q05 dies during
reservoir write because its spill lands on memory-backed `tmpfs` (`/tmp`): about 1.329 GB becomes
charged cgroup `shmem` that displaces reclaimable Parquet file cache to the exact 4 GiB cap, while
anonymous memory stays flat at about 2.95 GB and selected engine ownership is about 124 MB.
Placing large join spill on a disk-backed filesystem makes those pages reclaimable file cache
instead of non-swappable shmem.

On the measured host `/tmp` is `tmpfs`, `/var/tmp` and the repository `target` tree are `ext4`, and
swap is absent. Join spill defaults to `temp_dir()/toron_join_spill` and is already overridable via
`ExecutionContext::with_join_spill_store`. Both TPC-H execution-context sites use
`ExecutionContext::local_test()` and therefore inherit the tmpfs default. The harness already
depends on `wendelon`, whose `BlockingFileOps` is public, so it can use the existing builder without
new Toron plumbing.

ADR 429 did not retain historical swap/reclaim state. The idea that ADR 413 completed only because
swap or other reclaim headroom existed remains an unverified hypothesis and is not load-bearing.

## Decision

Land two separately validated placement controls:

1. **Engine environment override.** Add `default_join_spill_dir()` in the non-ratcheted semantic
   join module `exec/join/spill_placement.rs`. Resolution precedence is
   `with_join_spill_store` builder > non-empty `TORON_JOIN_SPILL_DIR` > the legacy
   `temp_dir()/toron_join_spill` default. Empty environment values are treated as unset and
   non-Unicode paths are preserved through `OsString` and `PathBuf`. The lookup is injectable in
   tests; tests do not mutate the process-global environment.
2. **Explicit TPC-H placement.** Centralize both harness context constructions and configure
   `target/tpch/join_spill` with `with_join_spill_store`. This is permanent runtime scratch on the
   measured disk-backed `target` filesystem. `target/tpch/adr430` is evidence-only, so scratch
   cleanup cannot erase receipts.

The harness override bypasses the environment-selected default. A green q05 therefore proves
disk-backed placement, while selector tests independently prove the environment contract.

There is no automatic tmpfs detection or relocation. Detection can identify a bad default but
cannot choose a universally safe alternative across portability, permissions, persistence,
quota, and crash-residue policy. Detect-and-warn and a broader deployment-owned placement policy
remain named follow-ons.

`exec.rs` replaces its existing default expression with one fully qualified helper call. This is
a one-line behavior-changing delegation with no line-count growth. The new helper is reconciled in
the ADR 176 new-file allowlist.

## Acceptance

- **Live mechanism proof first:** with only the permanent harness builder wiring present, q05 sf=10
  runs under the unchanged 4 GiB / 600-second envelope from fresh disk scratch.
- **Board move:** q05 is green only when a bounded persisted `perf-run-one` completes without OOM
  and the separate persisted correctness rail reports `pass@compare`. Five rows and the historical
  plan fingerprint alone are insufficient; ADR 413 retained a completing but wrong rejected
  attempt that only the correctness rail caught.
- **Placement receipt:** the ADR 429 off-by-default profiler on the fixed path shows shmem near zero,
  ordinary file cache replacing tmpfs shmem, and flat anonymous memory. The claim is reclaimable
  forward progress, not lower peak memory; a successful run may still touch the exact 4 GiB cap.
- **Precedence:** injected selector tests lock non-empty environment over legacy, empty as unset,
  unset as legacy, and a non-Unicode round trip. A pure context test locks builder over the
  selected default.
- **Controls:** ordinary contexts retain the legacy path when builder and environment are absent.
  The harness intentionally changes placement for all spill-bearing queries, so q03, q10, and q19
  retain persisted `pass@compare` results and leave scratch empty after normal completion.
- **Scope:** unchanged caps and budgets; no query branding; full Toron and TPC-H gates; `exec.rs`
  ratchet unchanged except the one delegating line; no tracked `.perf.data`.

If disk-backed q05 still cannot complete inside 600 seconds, the board remains 19/22 and ADR 430
records the precise residual without weakening the envelope or raising the cap. If it completes
and compares correctly, the board moves to 20/22.

## Guardrails and risks

- Relocation removes the stale-tmpfs RAM failure mode but moves crash residue to persistent disk.
  Canonical runs start with fresh scratch, verify cleanup after normal completion, and record any
  residue after abnormal termination honestly.
- The environment surface selects a caller-owned location; it does not certify that location as
  disk-backed, writable, private, or sufficiently provisioned.
- Disk-backed probe rereads may exceed the 600-second envelope. The live proof decides that before
  the engine environment policy is added.
- The measured `target` filesystem has enough room for the historical roughly 5 GiB q05 spill, but
  free space is preflight evidence rather than a permanent guarantee.

## Forward arc

- q17 remains an oracle boundary-precision cut. Before lifting its sf=0.02 pin, compare the sf=10
  structural boundary census as well as the final scalar.
- q02 remains the join-output materialization C-spec rock and stays last.
- Deferred: tmpfs detect-and-warn and a deployment-owned universal spill-placement policy.

