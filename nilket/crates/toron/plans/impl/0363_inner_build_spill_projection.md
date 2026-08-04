# ADR 363 - Implementation plan: inner-build spill projection (q21)

Pairs with `plans/adr/0363_inner_build_spill_projection.md`. A `crates/toron` engine cut: measure q21's streamed
inner-build spill by producer/page/column; if it writes dead columns, route it through the existing
`write_projected_batch_chunks`; else classify the 1,104 B as page/layout or named-irreducible. Unchanged 512 MiB
budget; no cap-bump; engine-cut seal gate (`--no-fail-fast`).

## Phase 0 - prove dead columns (the go/no-go)
- Env-gated trace of q21's spill (the `streamed inner join build spill writer`): page count, per-page bytes, **the
  written column set**, rows, compression. Total 536,872,016 / budget 536,870,912.
- Record which `write_streaming_scan_inner_build_batches` branch is responsible: exchange/repartitioned scan or direct
  scan. The fix may stay branch-scoped only if the measurement says so; otherwise handle both branches through the same
  projected-writer path.
- Determine the **output-needed columns** for q21's streamed inner build (group/aggregate args + join/exchange keys
  the downstream consumes) vs the **written columns**.
- **Decision:** dead columns present -> column projection (route through `write_projected_batch_chunks`). No dead
  columns -> classify page/layout (reducible only with a real lever) or **irreducible** (named wall + evidence).
- Record the per-lever classification + the column sets in the results doc.

## D1 - the projection (if dead columns)
- Route q21's streamed inner-build spill through `SpilledInnerJoinBuild::write_projected_batch_chunks` (spill.rs:474)
  with `retained_columns` = the output-needed set + `projected_build_keys` (mirroring the LeftSemi path at
  streaming.rs:2285).
- Remap build keys to the projected ordinal space, construct the `JoinKeyPlan` against the projected schema, and
  reconstruct sparse full-schema chunks on read for ordinal compatibility. The reoriented inner runtime must emit the
  same observable build/probe schema and column order as the un-projected path.
- No new inline spill writer; reuse the existing mechanism. Topology-named; no q21/qNN in core.
- If no dead columns: no source move; the results doc records the page/layout or irreducible verdict.

## Tests / safety net
- **Strict correctness:** q21 `pass@compare` at sf=0.01 and sf=1 (the strict oracle).
- **Dev parity (not the command):** a focused test - the projected-spill inner-build path output == the un-projected
  path output in **dev (Float64)** mode at sf=0.01 (digest/batch equality).
- **Projection correctness:** a synthetic spilled inner build with a wide build + narrow output -> projected spill
  yields the same matches/answer + preserved order as the full-column spill.
- **Reoriented-inner runtime coverage:** a test must exercise the projected spill through the same reoriented inner
  runtime branch Phase 0 names (exchange branch, direct branch, or both) and prove streamed-with-projection ==
  streamed-without-projection, including key remapping, full observable schema, row order, and column order.
- **No cap-bump:** `spill_bytes_reserved_per_query` (512 MiB) unchanged (assert).
- **Bounded:** the projected spill's byte count < 512 MiB at sf=10 (or the named irreducible wall).
- **De-branding guard:** no q21/qNN in core executor.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # MANDATORY (engine cut, ADR 361 lesson)
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01   # strict
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 1      # strict
# dev parity is the focused unit test above, NOT `correctness --mode dev`.
# sf=10, fixed 4 GB, persisted surface, live: q21's spill_budget_exhausted must be GONE (green) or named irreducible:
#   systemd-run --user --property=MemoryMax=4G --setenv=TORON_TPCH_DUCKDB_BIN=<duckdb> ... \
#     target/release/toron-tpch perf --sf 10 --mode strict --runs N --warmup K --query q21 --engines toron \
#     --data-source parquet --data-dir target/tpch/data --output-dir target/tpch/perf_adr363
git diff --check
#   q21 spill measured by producer/page/column; projected (if dead cols) under unchanged 512 MiB; green or named
#   irreducible; strict + dev parity; --no-fail-fast green; no q21 in core
```

## Results doc
`crates/toron/plans/impl/0363_inner_build_spill_projection_results.md`: the Phase-0 spill breakdown (producer/page/
column/rows/compression) + the responsible branch + the written-vs-output-needed column sets + the lever
classification; the cut (projected spill via `write_projected_batch_chunks`, including key remap/schema restoration,
or the named page/layout/irreducible verdict); q21 sf=10 before/after (spill bytes; green or named wall via 332);
strict `pass@compare` (sf=0.01/sf=1) + the dev parity-test result; confirmation 512 MiB unchanged, `--no-fail-fast`
green, no q21 in core.

## Risk / blast radius
- **No dead columns** - classify page/layout or irreducible with evidence; no forced green.
- **Projection correctness** - strict + dev parity + the synthetic projected-spill test.
- **Cap-bump** - rejected; 512 MiB unchanged.
- **Engine-cut regression** - `--no-fail-fast` mandatory (the 361 lesson); the empty-batch/schema family already
  green-guarded.
- **Reuse, not new path** - `write_projected_batch_chunks`; no new inline spill writer.

## Seal
`impl: adr 363 inner build spill projection`. Do not commit wip drafts.
