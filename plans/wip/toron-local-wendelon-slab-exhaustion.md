# Local Wendelon bootstrap exhausts file descriptors after empty-slab growth

Status: handoff to the Toron / Wendelon team, 2026-08-14.

## Incident

The local Nilket workspace at
`/home/me/.local/share/nilket/local` can no longer start. The failure is not a
Polariton readiness race: Wendelon bootstrap exhausts the process file-descriptor
limit while recovering the local slab directory.

The local systemd service is configured with `LimitNOFILE=524288`; raising the
limit is therefore not a meaningful fix. Each attempted start peaks at 8.4 GiB,
runs for roughly 10 seconds, and exits with:

```
wendelon: bootstrap failed: Too many open files (os error 24)
nilket local: Wendelon exited before readiness: exit status: 1
```

`polariton uat/test33.rn` consequently reports that the local workspace is
starting, because it encounters Nilket's transient startup/lock state while the
service is crash-looping.

## State observed

At investigation time, the local Wendelon data directory contained:

| measure | observed value |
| --- | ---: |
| slab files | 668,700 |
| zero-byte slab files | 668,693 |
| total profile footprint | 191 MiB |
| first slab mtime | 2026-08-12 22:38:27 CDT |
| last slab mtime | 2026-08-14 04:13:47 CDT |
| configured `storage.slab_max_bytes` | 64 MiB |
| service `LimitNOFILE` | 524,288 |

The non-empty slab files are only seven small files (4–28 KiB). The volume is
therefore not explained by normal 64 MiB append rotation. The directory has a
regular high-ID sequence of mostly empty `slab_<id>.wnd` files, consistent with
repeated maintenance lifecycle activity rather than user data volume.

### Current observation, 2026-08-15

The figures above are retained as the original 2026-08-14 report. They no longer
describe the directory on disk, and the difference is large enough to state
plainly rather than silently supersede:

| measure | as reported 2026-08-14 | observed 2026-08-15 |
| --- | ---: | ---: |
| slab files | 668,700 | 38,929 |
| zero-byte slab files | 668,693 | 38,223 |
| non-empty slab files | 7 | 706 |
| slab mtime range | 08-12 22:38 → 08-14 04:13 | 08-07 15:13 → 08-08 00:45 |

Every artifact in the profile — `snapshot.wnd`, the control WAL segment,
`audit.ndjson`, `pending_finalizations.redb`, and the newest slab — carries the
same 2026-08-08 00:45 mtime, so nothing has written to this profile since then
and it cannot have produced the 08-12 → 08-14 activity above. It is also the only
large slab directory under `~/.local/share/nilket`; the sibling profiles hold one
to five slab files each. Whether the original report measured a different profile
or a state since replaced is not established, and this note does not guess.

The empty-slab producer is nonetheless corroborated here: 38,223 of 38,929 files
are zero-byte, and the tail ids advance by exactly two (`0x1301d`, `0x1301f`,
`0x13021`, `0x13023`), which is the reserve-an-output-then-advance-the-active
signature the Wendelon-side record identifies.

**The profile is preserved, not reset.** It is retained as the evidence for
Wendelon ADR 0085 — its snapshot is intact but shaped for a pre-ADR-0548 state
machine, and resetting would destroy the only artifact of that format gap. The
service is stopped.

## Why startup fails

`SlabManager::open` currently scans every `slab_<hex>.wnd` file, opens every one,
recovers its local index, makes the highest numbered slab active, and retains all
others as sealed history. This is documented and implemented in
`crates/wendelon/src/storage/manager.rs` around `SlabManager::open` and
`discover_slab_ids`.

With 668k files, that recovery strategy crosses the 524,288 FD limit before node
readiness. Its cost also explains the 8.4 GiB per-attempt memory peak. Restarting
the service only repeats the scan and failure.

## Suspected producer: empty active slabs around LSM compaction

This is the strongest code-level lead, not yet a proven root cause:

- Default `LsmCompactionService` ticks every 100 ms
  (`crates/wendelon/src/storage/lsm.rs` / `lsm_worker.rs`).
- LSM output installation calls `advance_active_after_lsm_outputs`
  (`crates/wendelon/src/storage/manager/lsm_driver.rs`).
- That function opens a new active slab after installed output IDs, seals the
  former active slab, and advances `next_slab_id`.
- The local directory contains almost exclusively zero-byte slabs while
  `storage.slab_max_bytes` is the normal 64 MiB.

The expected failure shape is that compaction repeatedly creates an empty active
slab, a later compaction seals it, and no manifest/GC path retires the empty
non-output slab. Please confirm or reject this with instrumentation around:

1. active-slab creation and sealing;
2. LSM output reservation/install;
3. manifest admission of the resulting IDs; and
4. GC eligibility/deletion for zero-byte, non-manifest sealed slabs.

## Requested outcome

1. Find and stop the empty-slab producer; no maintenance path should leave an
   unbounded sequence of empty local files.
2. Add a regression that runs compaction over a tiny local workload for enough
   ticks to prove slab-file count stays bounded and restart remains possible.
3. Make bootstrap recovery bounded by the authoritative manifest/index where
   possible, or otherwise fail early with a diagnostic that reports slab count
   and recommends a safe recovery path rather than consuming all FDs and memory.
4. Define a safe repair for already-affected non-development profiles. Do not
   infer that deleting zero-byte files is safe: the highest-numbered file may be
   the active slab and the manifest/index relationship must be checked first.

## Local workaround

For this disposable development profile, the safe practical recovery is:

```sh
systemctl --user stop nilket-local.service
nilket local --data-dir /home/me/.local/share/nilket/local --reset
systemctl --user start nilket-local.service
```

`nilket local --reset` removes the marked local profile's `config`, `pki`,
`data`, and `logs`, while retaining its lock inode. It is appropriate here only
because the few local tables can be recreated from scripts. It is not a repair
for a non-disposable workspace.
