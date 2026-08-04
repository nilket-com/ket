# ADR 375 results - spill page write buffer reuse

ADR 375 reused the spill page plaintext buffer for `SpilledInnerJoinBuild::write_page_batch`. It is a
named-site perf cut, not a new query green: the q07 sample still times out, but the profile no longer points first at
resident accounting or fresh IPC encode allocation in the page writer.

## Phase 0 site and ownership boundary

The site came from the ADR 374 q07 profile. `perf report --stdio -g -i target/tpch/profiles/adr374_q07.perf.data`
showed the hot page-write path in `SpilledInnerJoinBuild::write_page_batch`, with `shmem_file_write_iter`, allocator,
memmove, Arrow take, and partition-key work nearby.

The write ownership boundary resolved to the planned primary path:

- `encode_record_batch_ipc_into(&mut Vec<u8>, batch)` writes the IPC plaintext into a reusable builder scratch buffer.
- `write_join_spill_plaintext_page_returning_buffer` still owns the mutable `Vec<u8>` while sealing in place.
- `write_plain_all_returning_buffer` returns that same sealed buffer to the builder after the file write.
- The lower `write_plain_all` slice clone remains a named residual: `AsyncFileOps::write_plain_at` still consumes a
  `Vec`, so each partial write still uses `bytes[written_total..].to_vec()`.

This cut removes the repeated IPC/plaintext encode allocation in the inner spill page writer. It does not claim to
remove every per-page write allocation.

## Implementation

- Added `scratch_plaintext: Vec<u8>` and `scratch_refs: Vec<(JoinKey, u32)>` to `SpilledInnerJoinBuild`.
- Changed `write_page_batch` to:
  - reuse `scratch_refs` for keyed row references;
  - encode IPC plaintext into `scratch_plaintext`;
  - pass the buffer through the seal/write helper and receive it back;
  - clear and store the returned buffer for the next page.
- Added `encode_record_batch_ipc_into` and kept `encode_record_batch_ipc` as the fresh-buffer wrapper.
- Added the return-buffer write helpers without changing page format, `spill_bytes`, `writer_peak`, budget producer
  names, or caps.

## Tests

New focused tests:

- `encode_record_batch_ipc_into_matches_fresh_and_reuses_scratch`
- `inner_spill_page_write_reuses_scratch_and_round_trips`

They cover plaintext equality against the fresh encoder, A-then-B scratch reuse with no stale-byte bleed, real spill
write/read round-trip, key lookup preservation, and unchanged `spill_bytes` / `writer_peak` accounting.

## Re-profile

Method matched ADR 373/374: release binary with debug symbols, fixed 4 GB `systemd-run --scope`, `perf record -F 99
--call-graph dwarf -D 60000`, 120s bounded q07 sample, Inferno-rendered SVG. The timeout exit was expected for the
bounded sample window.

Artifacts remain uncommitted under `target/tpch/profiles/`:

| artifact | path |
| --- | --- |
| perf data | `target/tpch/profiles/adr375_q07.perf.data` |
| perf script | `target/tpch/profiles/adr375_q07.perf.script` |
| folded stacks | `target/tpch/profiles/adr375_q07.folded` |
| SVG | `target/tpch/profiles/adr375_q07.svg` |

Before/after profile summary:

| stack / symbol | ADR 374 q07 | ADR 375 q07 |
| --- | ---: | ---: |
| `SpilledInnerJoinBuild::write_page_batch` | 9.44% | 6.91% |
| `shmem_file_write_iter` | about 9.94% | about 7.10% |
| `__memmove_avx_unaligned_erms` | 6.77% | 5.03% |
| `_int_malloc` | 5.69% | 2.57% |
| `partition_bucket` | 5.60% | 8.16% |
| `encode_partition_key_value` | 3.62% | 2.52% |

The named page-writer share dropped materially, and `encode_record_batch_ipc` no longer appears in the searched top
report. The next hot stack is now partition/key churn around `partition_bucket` and `encode_partition_key_value`, with
the residual write-path clone / shmem I/O and Arrow take still visible. q07 did not green in this ADR; that was not the
acceptance condition.

## Verification

All required gates passed:

```bash
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib encode_record_batch_ipc_into -- --nocapture
scripts/cargo-cgroup test -p toron --lib inner_spill_page_write_reuses_scratch_and_round_trips -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false \
  CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
git diff --check
```

The release binary was verified to contain debug info and symbols for profiling.

No cap was raised. Budget producer names and spill accounting are unchanged. No query-number references were added
under `crates/toron/src`.

## Forward

ADR 375 removed the fresh IPC plaintext allocation from the named page writer. The next evidence-backed perf target from
the q07 profile is partition/key churn in the streamed inner-chain spill path. The residual lower write clone is also
now explicitly named, but it is a separate file-ops ownership cut rather than part of this ADR.
