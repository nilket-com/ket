# ADR 430 - Spill placement / cache control: results

Pairs with `plans/adr/0430_spill_placement_cache_control.md` and its implementation plan. ADR 430
restores q05 reclaimable forward progress under the unchanged envelope and moves the verified
sf=10 strict board from **19/22 to 20/22**.

## Outcome

ADR 429's named mechanism was correct. The TPC-H harness now places join spill under the
disk-backed `target/tpch/join_spill` cache root instead of inheriting `/tmp` tmpfs. With that single
placement change, q05 completes inside 600 seconds, produces the historical five-row result and
plan fingerprint, passes the independent persisted correctness oracle, and cleans all normal-path
scratch.

The engine also accepts a non-empty `TORON_JOIN_SPILL_DIR` as its default spill directory. The
existing `with_join_spill_store` builder remains the final and highest-precedence override. No
automatic mount detection or relocation was added, and contexts with neither override retain the
legacy `temp_dir()/toron_join_spill` path.

## Phase 0 - harness-only mechanism proof

The promoted plan commit was `7cd19c025dcc766f9e4ee11ede6e8dcf8c787b16`. Before adding the
engine selector, only the two harness context sites were replaced by a centralized constructor
that applies `BlockingFileOps` and `target/tpch/join_spill`. The retained harness-only patch is 38
lines and the release binary SHA-256 was
`65e8d3b6efa6ebe5ae0280a113294f3736e47da04b4ce5bb4f2168d326ae0b2d`.

Preflight recorded:

- `/tmp` on tmpfs and the repository/`target` tree on ext4;
- no active swap;
- 117 GiB free on the target filesystem before the canonical run;
- fresh, separate `target/tpch/join_spill` runtime scratch and `target/tpch/adr430` evidence.

The unchanged cgroup envelope was `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512`, and
`RuntimeMaxSec=600s`.

| rail | result | engine/compare wall | outer wall | max RSS fallback | scratch after |
| --- | --- | ---: | ---: | ---: | ---: |
| persisted `perf-run-one` | pass, 5 rows, 5,353,985,328 spill bytes | 259,114.7 ms | 4:24.48 | 3,050,404 KiB | 0 bytes |
| persisted correctness | `phase=compare status=pass` | 255,917.9 ms | 4:16.05 | 3,358,404 KiB | 0 bytes |

The performance row retained plan fingerprint `757d7c625669b21a`, output schema fingerprint
`fnv1a64:3f31e3a6be8148ff`, and the same Arrow IPC digest used by the prior green. The correctness
rail, rather than those structural identifiers alone, is the board-move authority.

The local user systemd instance returned `Result=success`, `ExecMainCode=0`, and
`ExecMainStatus=0` for both runs but did not retain `MemoryPeak`; both records label it `[not set]`
instead of inventing a cgroup peak. `/usr/bin/time` maximum RSS is retained as the available peak
fallback. The later live residency receipt supplies in-cgroup category evidence.

## Implementation

The shipped cut is narrow:

- `toron-tpch` has one `tpch_execution_context` helper used by both execution sites. It attaches
  telemetry and overrides join spill with `BlockingFileOps` plus
  `PathBuf::from(CACHE_ROOT).join("join_spill")`.
- `exec/join/spill_placement.rs` resolves `TORON_JOIN_SPILL_DIR` through an injected lookup. A
  non-empty `OsString` is preserved exactly; empty and missing values use the legacy default.
- `exec.rs` changes one line and no line count: the legacy expression delegates to the helper.
- The builder remains unchanged and overwrites the selected default after construction, giving the
  locked precedence builder > environment > legacy.
- The ADR 176 allowlist includes the new semantic join module. There is no q05/TPC-H engine branch,
  cap change, mount probe, filesystem side effect during selection, or new Toron export.

Five engine unit tests lock non-empty environment selection, empty-as-unset, missing-as-legacy,
Unix non-Unicode preservation, and builder precedence. A TPC-H architecture guard locks both
context sites to the shared cache-root placement helper. Tests inject selector input and never
mutate the process-global environment.

## Fixed-path residency receipt

The final release binary reran q05 with ADR 429's off-by-default profiler enabled while spill
remained under the disk-backed harness root. It completed in 191,116.7 ms engine time / 3:15.73
outer wall, returned the same output/digest/fingerprint and 5,353,985,328 total telemetry spill
bytes, exited 0, and left scratch empty. `/usr/bin/time` maximum RSS was 3,051,144 KiB.

The schema-1 terminal receipt is sequence 3,670, `status=complete`, after 234,324 reservoir build
batches and 59,986,052 build rows. It records 5,270,315,856 reservoir spill bytes.

| cgroup field | reservoir admission | terminal | delta |
| --- | ---: | ---: | ---: |
| `memory.current` | 4,251,672,576 | 4,294,848,512 | +43,175,936 |
| process RSS | 2,962,194,432 | 3,126,194,176 | +163,999,744 |
| anon | 2,945,085,440 | 3,109,019,648 | +163,934,208 |
| file | 1,297,358,848 | 1,164,906,496 | -132,452,352 |
| shmem | **0** | **0** | **0** |
| kernel | 7,053,312 | 19,570,688 | +12,517,376 |
| slab | 450,496 | 12,656,144 | +12,205,648 |

The literal pre-run expectation that anon would be flat across the whole receipt was too strong:
the terminal sample is after reservoir probing/folding and has 536,822,344 bytes of decoded writer
cache payload, so anon and RSS are about 164 MB above reservoir admission. This is bounded
post-write cache residency, not spill-sized growth. The discriminating placement result is that
`shmem` remains exactly zero while 5.27 GB is written, ordinary file cache remains reclaimable,
and direct reclaim advances without killing the query. `pgscan_direct` grows from 1,296,360 pages
at admission to 4,100,422 at terminal.

This contrasts with ADR 429's last tmpfs receipt at the same 4,294,848,512-byte live charge:
`shmem=1,328,922,624`, ordinary file cache only 643,072 bytes, and an OOM before reservoir finish.
ADR 430 reaches terminal and correct output at essentially the same charged-memory frontier because
the spill-backed pages can now be evicted. The result is **reclaimable forward progress restored**,
not a claim of lower peak memory or universal headroom.

## Focused controls

Final release, sf=10, persisted strict correctness, disk-backed harness path:

| query | result | elapsed | max RSS fallback | scratch after |
| --- | --- | ---: | ---: | ---: |
| q03 | `pass@compare` | 24,615.0 ms | 768,980 KiB | 0 bytes |
| q10 | `pass@compare` | 22,440.3 ms | 1,696,584 KiB | 0 bytes |
| q19 | `pass@compare` | 97,942.5 ms | 848,192 KiB | 0 bytes |

Each cgroup unit exited successfully. The controls prove semantic parity and normal cleanup for the
shared placement change; they do not claim that disk and tmpfs have identical performance.

## Verification and scope

Final green gates:

- `scripts/cargo-cgroup fmt -- --check`;
- `check -p toron --all-targets` and `check -p toron-tpch --all-targets`;
- Toron library: 1,282 passed, 0 failed;
- Toron architecture: 2,079 passed, 0 failed;
- full Toron `--no-fail-fast`: every unit, integration, and doc-test binary green;
- TPC-H architecture: 13 passed, 0 failed;
- TPC-H with `polars-sql-reference --no-fail-fast`: every unit, integration, and doc-test binary
  green, with only the pre-existing documented ignores;
- `git diff --check` clean;
- no tracked `.perf.data`;
- `target/tpch/join_spill` empty after the final normal run.

Crash residue remains possible after process death because spill now lives on persistent disk. The
canonical protocol removes scratch before each run, reports abnormal residue, and verifies cleanup
after normal completion. The environment-selected path remains caller-owned and is not certified
as disk-backed, private, writable, or sufficiently provisioned.

## Verdict

ADR 430 greens q05 under the unchanged 4 GiB / 600-second envelope. The verified board is now
**20/22**. q17 remains the oracle boundary-precision cut and q02 remains the join-output C-spec
rock.

