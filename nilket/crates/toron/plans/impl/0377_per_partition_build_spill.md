# ADR 377 - Implementation plan: per-partition build spill (eliminate the whole-build re-scan)

Pairs with `plans/adr/0377_per_partition_build_spill.md`. Replace the whole-build-per-partition re-scan in
`probe_partitioned_streamed_inner_scan_build` (streaming.rs:6135) with a **one-pass per-partition spill write + read
each partition once**. q07 canary. Map both sides + bound file handles in Phase 0. Parity + profile gated; no
parallelism, no cap bump.

## Phase 0 - map both sides + bound N (before code)
- **Build:** confirm via the post-376 q07 profile that the dominant residual is the build re-scan/scan-read under
  `probe_partitioned_streamed_inner_scan_build` (6135, the 6163 loop), not bucket churn.
- **Probe:** read the path - is the probe **held once**, **streamed once per partition**, or **re-scanned per
  partition**? Decide: route probe partitions too (one-pass) OR build-only claim + named probe residual.
- **N (partition count):** `streamed_inner_build_partition_count` (1107) / `streamed_spill_partition_count` - what is N
  at sf=10 for q07, and its max? Decide writer strategy: N concurrent writers only if N is provably small; else a
  **lazy-open / small writer pool** so open-file count is bounded.
- **Spill-byte peak:** the current path writes/probes/cleans one partition at a time. A one-pass router can make all
  partition spill bytes live on disk at once. Measure/predict that peak under the unchanged spill budget; if it would
  exceed budget, choose a bounded partition-group strategy or report a named spill-volume wall.
- **Routing surface:** record the exact old order: scan -> optional filter -> optional exchange repartition -> join
  partition selector -> retained-column projection/write. The new router must bucket the same post-filter/post-exchange
  rows with `partition_build_keys`; `write_build_keys` are only for the projected spilled payload.
- Record the both-sides map, N/file-handle decision, spill-byte peak decision, and routing surface in the results doc.

## D1 - one-pass per-partition build spill
- Scan the build **once**; after the same filter/exchange stages the old path used, route each row by its
  `partition_build_keys` bucket into its partition's spill (per-partition files or one file with per-partition extents),
  **preserving per-partition append order = the old select-per-partition scan order**.
- Process partitions: for each partition, read **only its spill once**, build its index, probe, free - one partition
  resident at a time.
- Probe per the Phase-0 decision (routed one-pass, or per-partition streamed with the residual named).
- Bound file handles per the N decision (lazy/pool if needed). Keep `spill_bytes`/budget/producer accounting correct.
- Preserve the unchanged spill-byte budget. If all partitions must be present before probing and that peak exceeds the
  budget, either group partitions so peak spill remains bounded or accept/report the named spill-volume wall.
- Topology-named; no qNN in core; no broad spill abstraction unless Phase 0 proved it better.

## Tests / safety net
- **Row-order parity:** per-partition spill route order within each partition **==** the old select-per-partition scan
  order (so join output order is identical) - a focused test on a multi-batch, multi-partition synthetic build.
- **Routing/key parity:** the router buckets the same post-filter/post-exchange rows using the same
  `partition_build_keys` as the old selector; projection / `write_build_keys` cannot affect bucket assignment.
- **Join-output parity:** materialized/re-scan path vs per-partition-spill path produce **identical** join output for
  the q07-shaped inner chain (multi-batch, multi-partition, null + residual keys).
- **One-pass assertion:** the build is scanned once (counter), not N times.
- **Resident bound:** only one partition's index/spill resident; file-handle count bounded (assert the writer
  strategy).
- **Spill-budget assertion:** unchanged budget/producers; peak spill reservation is bounded, or the next wall is named
  as `spill_budget_exhausted`.
- **q07 small-scale strict parity** preserved.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut, spill path)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01   # strict pass@compare
# re-profile q07 (373 method) inside systemd-run --scope, debug symbols -> build re-scan residual dropped; next stack
# q07 persisted sf=10 correctness if reachable; family measure:
#   ... perf --sf 10 --query q07,q03,q05,q10,q12 --engines toron --data-source parquet --data-dir target/tpch/data ...
git diff --check
#   both sides mapped; one-pass build (counter); routing/key + row-order + join-output parity; N/file-handles bounded;
#   spill-byte peak bounded-or-named; profile residual dropped + next stack; q07 pass@compare; family measured; no
#   parallelism/cap bump; --no-fail-fast; no qNN
```

## Results doc
`crates/toron/plans/impl/0377_per_partition_build_spill_results.md`:
- the both-sides map (build re-scan confirmed; probe held/streamed/re-scanned + routed-or-residual);
- the N / file-handle decision (writer strategy, bound);
- the spill-byte peak decision (all partitions at once vs partition groups vs named spill wall);
- the routing surface decision (post-filter/post-exchange, pre-projection; `partition_build_keys` vs `write_build_keys`);
- the one-pass + read-each-once implementation;
- routing/key + row-order + join-output parity test results; one-pass assertion;
- **before/after re-profile** for q07: build re-scan/bucket residual before/after + next hot stack; q07 pass@compare;
- **q03/q05/q10/q12 measured family impact** (coverage, not promised);
- **the parallelism reassessment**: is the cleaned q07 profile now mostly real spill/read/write/Arrow work?
- confirmation no parallelism, no cap bump, no broad abstraction (or why narrow was worse), `--no-fail-fast` green, no
  qNN.

## Risk / blast radius
- **Silent probe re-scan** - Phase 0 maps it; route or narrow+name.
- **File-handle blow-up** - bounded writer strategy (lazy/pool) at high N.
- **Spill-byte peak regression** - all partition files live at once can exceed the old one-partition peak; measure and
  bound or name the wall.
- **Wrong routing key surface** - use the same post-filter/post-exchange rows and `partition_build_keys`, not projected
  write ordinals.
- **Row-order drift** - order-parity test.
- **Broad-abstraction creep** - narrow unless proven.
- **Regression** - `--no-fail-fast` required.

## Seal
`impl: adr 377 per partition build spill`. Do not commit wip drafts or large `.perf.data`.
