# ADR 383 - Implementation plan: q07 partition/take/copy (378 G-pass re-work)

Pairs with `plans/adr/0383_partition_take_copy.md`. Cut q07's 378 G-pass re-work: **lever 1 = active-group-only partition
take** (clean, spill-peak-safe); **lever 2 = bucket-array cache across G passes** (only if measured resident-safe). q07
is the canary, with q03/q05/q10 as controls if the shared partitioned streamed-inner helper moves. No cap bump; 378
no-peak guard.

## Phase 0 - measure the re-work (before code)
- For q07 sf=10: **G**, **P**, rows scanned per group, **bucket-computation count**, **partition-take count**.
- For lever 2: **rows x bucket_repr_bytes** (cache resident cost) + **scan-order stability** across the G build re-scans
  (is the build source scan deterministic in order per pass?).
- Confirm the all-P-take-then-filter waste at `probe_partitioned_streamed_inner_scan_build` (streaming.rs:3696/3703/3706).

## D1 - lever 1: active-group-only partition take (first)
- Add an active-group variant of `partition_streaming_join_batch_with_buckets` (or filter in-loop before `take`) that
  builds/takes **only** partitions in `[partition_start, partition_end)` - skip rows whose bucket is outside the active
  group **before** materializing.
- Partition-take count drops ~G x -> the active group's share; `take_bytes`/`__memmove` share drops.
- Spill-peak-safe (strictly fewer takes). Partition assignment + ordered output unchanged.

## D2 - lever 2: bucket-array cache across G passes (only if resident-safe + scan-order-stable)
- If Phase 0 proves scan-order stability: compute the per-row bucket once, store in a **compact** array
  (`rows x bucket_repr_bytes`, e.g. packed u32/u16), reuse across the G passes -> bucket precompute G x rows -> rows.
- **Budget the cache as a named `partition bucket cache` resident term** - it must fit within the existing budget (no
  cap bump). If `rows x bucket_repr_bytes` does not fit safely -> a disk/sidecar bucket store, or **name the stop-line**
  (no cache).
- **Never** hold the build to skip the re-scan (re-introduces the 378 peak) - stop-line.

## Tests / safety net
- **q07 correctness:** strict `pass@compare` sf=0.01/sf=1; persisted sf=10 if it advances/greens.
- **Partition/output parity:** active-group-only take produces the same partition assignment + ordered join output as
  the all-P-then-filter path (synthetic multi-batch, multi-partition, multi-group).
- **Bucket-cache parity (if shipped):** cached buckets == recompute for every row across passes; scan-order-stability
  asserted.
- **378 no-peak guard:** `spill_bytes`/`writer_peak`/resident-guard unchanged-or-better; the bucket cache (if shipped)
  is a bounded named resident term, asserted under budget.
- **Shared-helper controls:** q03/q05/q10 oracle-scale correctness remains green if the implementation changes the
  shared partitioned streamed-inner path; their sf=10 wall class is recorded as unchanged, improved, or named.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01   # strict pass@compare
# q03/q05/q10 correctness controls if the shared helper changes.
# symbol-retaining re-profile (381 method) q07 sf=10: partition-take count + take_bytes/__memmove + partition_bucket_*
#   share before/after; spill_bytes/writer_peak/guard no-regress; persisted sf=10 correctness if advanced/green
git diff --check
#   counts (G/P/rows/bucket/take) before+after; lever1 shipped; lever2 only if resident-safe+scan-order-proven;
#   378 no-peak guard; q07 parity; no q03 regression; no cap bump; --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/0383_partition_take_copy_results.md`:
- Phase-0 counts (G, P, rows/group, bucket-computation count, partition-take count) before/after;
- lever 1 (active-group-only take) + the partition-take-count drop + `take_bytes`/`__memmove` re-profile;
- lever 2 decision: bucket cache (with `rows x bucket_repr_bytes` resident + scan-order proof + budget) OR named
  sidecar/stop-line;
- 378 no-peak guard (`spill_bytes`/`writer_peak`/resident-guard) + the named bucket-cache resident term if shipped;
- q07 correctness parity + re-profile share drop OR named floor;
- q03/q05/q10 controls if the shared helper changes;
- confirmation no cap bump, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **Bucket cache blows resident** -> measured `rows x bucket_repr_bytes`; compact/sidecar/stop-line; named term.
- **Scan order unstable** -> no cross-pass cache; lever 1 stands.
- **Re-introducing 378 peak** -> lever 1 takes fewer; cache is named resident, not held build.
- **Inherent join-output take** -> named floor.
- **Regression** -> `--no-fail-fast`.

## Seal
`impl: adr 383 partition take copy` (or `plan: adr 383 ...` if the outcome is lever-1-plus-named-floor with no cache).
Do not commit wip drafts or large `.perf.data`.
