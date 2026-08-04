# ADR 376 - Implementation plan: q07 partition-bucket re-encode churn

Pairs with `plans/adr/0376_partition_bucket_churn.md`. Cut the partition/key churn named by ADR 375's q07 re-profile:
**A** reuse the per-call hash scratch in `partition_key_hash`; **B** (conditional) eliminate the per-partition-pass
bucket re-encode in `select_join_partition_batch`. Strict Phase-0 fork + stop line. Profile-based; no green required.

## Phase 0 - the fork (before code)
- Confirm via `perf report --stdio -g -i target/tpch/profiles/adr375_q07.perf.data` that `partition_bucket`/
  `encode_partition_key_value` dominate and the caller is the partitioned-spill re-scan (`select_join_partition_batch`
  under `for partition in 0..partition_count`, join.rs:2019), not the one-pass exchange partitioner (exec.rs:708/1150).
- **A check:** is threading a `&mut Vec<u8>` scratch through `partition_bucket`/`partition_key_hash` mechanically local
  and parity-safe? (free functions; the scratch can be caller-owned and cleared per row). If yes -> always-in.
- **B check:** the re-scan shape - is the batch **held** across partition passes (hoist a bucket array once) or
  **re-read from spill per partition** (needs a per-row bucket cache or spill-time bucket store with clear row
  identity)? Is the row-order/key alignment obvious and testable?
- **B null-key check:** for non-null-safe joins, `select_join_partition_batch` currently does
  `join_key_bytes(...).is_some()` and then `partition_bucket(...)`, which can encode the same key twice. If B ships,
  design the precompute as `Option<usize>` per row: `None` for rows with null keys (unless retained unkeyed rows route
  to partition 0), `Some(bucket)` for keyed/null-safe rows. Do not leave the old null-check encode beside the cached
  bucket encode.
- **Stop line:** if B requires reshaping spill reads or binding cached buckets to page/row identity beyond the named
  partitioned path -> record B residual, ship A, reassess parallelism.

## D1 - A: scratch reuse (always-in if clean)
- Add a caller-owned `&mut Vec<u8>` scratch; `partition_bucket(key_arrays, row, partition_count, &mut scratch)` clears +
  reuses it in `partition_key_hash` instead of `Vec::new()` per call. Update call sites (exec.rs:708/1150,
  join.rs:2152, the partitioned path) to pass a reused scratch. The exchange call-site updates are signature plumbing
  for the shared helper only; they must keep the existing one-pass exchange algorithm and are not part of the perf
  claim.
- Hash output unchanged (same bytes -> same `XxHash3_64`), so bucket assignment is bit-identical.

## D2 - B: bucket precompute/cache (only past the fork)
- **Held batch:** compute the bucket array once before the partition loop; `select_join_partition_batch` filters by the
  precomputed bucket (`bucket[row] == partition`) instead of recomputing. Represent the cached value as
  `Option<usize>` so the non-null-safe null-key decision and bucket hash come from one key encoding pass.
- **Re-read:** a per-row bucket cache keyed by stable row identity, computed on first read; or store the bucket at
  spill-write time and read it back. Only if alignment is obvious + testable; else residual.
- Assert the computation count drops rows x partitions -> rows (counter or profile).

## Tests / safety net
- **Load-bearing parity:** precomputed/reused bucket **==** the recompute for every row, across **nulls, decimal,
  string, date-ish keys, and multiple partition counts** - through the real partitioned path (not constructed buckets).
- **A scratch reuse:** the reused-scratch hash equals the fresh-`Vec::new()` hash for the same row (bit-identical).
- **Partition assignment unchanged:** same rows land in the same partitions as before the cut (end-to-end on a synthetic
  partitioned spill).
- **No hidden double-encode:** for a non-null-safe synthetic path, prove the cached `Option<usize>` matches the old
  `join_key_bytes` + `partition_bucket` decisions without retaining both encodes in the implementation.
- **q07 small-scale strict parity** preserved.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch
# re-profile q07 (373 method) inside systemd-run --scope, debug symbols:
#   ... perf record -F 99 --call-graph dwarf -D 60000 -o ...adr376_q07.perf.data -- timeout 120s \
#       target/release/toron-tpch perf --sf 10 --query q07 --data-source parquet ...
#   perf report --stdio -g -i ...adr376_q07.perf.data -> partition_bucket/encode share dropped; name next stack
git diff --check
#   A: per-call scratch gone; B: rows x partitions -> rows (or residual w/ stop-line reason); parity across key types +
#   partition counts; profile share dropped + next stack named; no cap bump; --no-fail-fast green; no qNN
```

## Results doc
`crates/toron/plans/impl/0376_partition_bucket_churn_results.md`:
- the Phase-0 fork verdict (A clean? B clean or residual + the stop-line reason);
- A implemented (scratch reuse) + the bit-identical-hash test;
- B implemented (held-hoist / re-read-cache) with the rows x partitions -> rows count drop, OR B residual + reason;
- if B shipped, the null-key discriminator outcome (cached `Option<usize>`, no hidden `join_key_bytes` double encode);
- the load-bearing bucket-parity test results (nulls, decimal/string/date keys, multiple partition counts);
- **before/after re-profile** for q07: partition_bucket/encode_partition_key_value share before/after + the next hot
  stack; whether q07 greened (incidental);
- **the parallelism reassessment**: is the cleaned q07 profile now mostly real spill/read/write/Arrow work?
- confirmation no cap bump, partition semantics unchanged, `--no-fail-fast` green, no qNN.

## Risk / blast radius
- **B alignment** - re-read order vs cached bucket; stop line + parity test; ship A only if unclear.
- **A call-site churn** - threading the scratch param through call sites; the bit-identical-hash test gates it.
- **Over-claim** - A/B success separated; overall = profile share.
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 376 partition bucket churn`. Do not commit wip drafts or large `.perf.data`.
