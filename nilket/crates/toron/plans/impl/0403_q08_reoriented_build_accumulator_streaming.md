# ADR 403 - Implementation plan: q08 reoriented join build accumulator (diagnostic + first streaming-completion cut)

Pairs with `plans/adr/0403_q08_reoriented_build_accumulator_streaming.md`. q08 is the fast-failing, execution-side entry
into Thread 3. It hits `reoriented join build accumulator` (`exec/join/streaming.rs:8229`) because its overflowing build
misses the existing streamed scan-build runtime: either the call site chose the materialized entry point, or the
streamed entry point's eligibility gate rejected the build shape (`streaming.rs:8036`,
`stream_scan_build && decompose_streaming_scan_probe_input(stage.build).is_some()`). Diagnose why, then either extend
the existing streamed scan-build routing/eligibility path (shape-fenced) or seal the diagnostic for the bigger
derived-build-spilling cut. No q08 branch.

## Phase 0 - classify q08's build side (before any code)
1. **Reproduce + locate the stage.** Run q08 sf=10 under the ADR 398 scope and capture which join replay/stage raises
   the accumulator wall (task telemetry / replay id at `streaming.rs:8261-8264`, and the `budget_exhausted` site).
2. **Runtime entry point.** Record whether the failing stage went through
   `prepare_reoriented_inner_join_runtime_with_streamed_scan_build` (`stream_scan_build=true`) or the plain
   `prepare_reoriented_inner_join_runtime` / `prepare_reoriented_inner_join_runtime_from_build_batches` materialized
   path (`stream_scan_build=false`). If streaming was not requested, the first candidate is a call-site decision miss.
3. **Scan-eligible vs derived.** For that stage's `stage.build`, evaluate `decompose_streaming_scan_probe_input`:
   - returns `Some` shape, but streaming was not requested -> **(A1) call-site miss**;
   - returns `Some` shape and streaming was requested, but a downstream fence still materialized it -> **(A2)
     eligibility/fence miss**;
   - returns `None` because the build is a derived/intermediate join output -> **(B) derived-build**.
   Dump the build sub-plan shape (scan/exchange/filter/project chain vs join-under-build).
4. **Orientation check.** Confirm the overflowing side is the genuinely-smaller build, not an upstream build-side
   selection picking the larger side ("reoriented" already flipped once).
5. **Fence census.** Record what `decompose_streaming_scan_probe_input` accepts/rejects and which of q08's builds are
   scan-eligible vs derived.
Record all of this in the results doc **before** touching code.

## Phase 1 - the fork (Phase 0 decides which)
- **(A1) Call-site miss:** extend the existing reoriented scan-build decision/routing so the scan-decomposable build
  requests the streamed runtime under a generic plan-shape / measured-size condition. Do not bypass the existing
  `PartitionedScan`/`Spilled` runtime and do not add a q08/table branch.
- **(A2) Eligibility miss:** extend the streamed scan-build path so the scan/exchange-shaped build streams
  (`PartitionedScan`/`Spilled`) instead of falling to `prepare_reoriented_inner_join_runtime_from_build_batches`.
  Loosen the `decompose_streaming_scan_probe_input` fence by a **plan-shape property only**, generic and shape-fenced;
  reuse the existing runtime - no parallel spill path, no q08/table branch.
- **(B) Derived-build:** stop-line. Seal Phase 0 as the spec for the intermediate/derived join-build spilling ADR
  (the bigger execution-model cut). Do **not** add a q08-only spill path or bump the budget.

## Tests (behavior-neutral where code changes)
- If (A1/A2): a streamed-scan-build routing/eligibility test for the newly-accepted build shape (drives
  `PartitionedScan`/`Spilled`, not the materialized fallback), shape-fenced so an ineligible/derived build still
  materializes (discriminating);
- streamed-path invariants unchanged (existing `exec/join/streaming.rs` tests green);
- q08 persisted correctness `pass@compare` if it completes; if it advances, the new wall is asserted/reproduced;
- q19/q04 sibling shape + correctness unchanged (q19 keeps its working spill);
- if (B): no core-code test (diagnostic only); the results doc carries the reproduction + spec.

## Phase 2 - re-measure (q08 driver, q19/q04 controls, fast windows only)
```bash
# q08 driver + q19/q04 controls at sf=10, ADR 398 scope (4 GB cgroup, ~1 GiB task budget)
for q in q08 q19 q04; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
    target/release/toron-tpch perf --sf 10 --mode strict --runs 3 --warmup 1 --query "$q" \
      --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
      --output-dir "target/tpch/perf_adr403_${q}_sf10"
done
```
- Primary (A1/A2): q08 off `reoriented join build accumulator` - pass@compare, or a **named + reproduced** next wall.
- Controls: q19/q04 stay green (no regression; q19 still spills). q03/q05/q10/q12 only if cheap.
- Record the reach-board move (before wall -> after) + build-side classification.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib                            # streamed-path + new eligibility test
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (execution/core cut)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast              # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q08 --sf 0.01   # if (A) completes
git diff --check
find . -name '*.perf.data' -print
```
Note: build the large `toron` crate under a bounded job count (ADR 398/402 saw systemd-oomd at 12 rustc jobs; use
`CARGO_CGROUP_JOBS=4`).

## Deliverable (results doc)
`crates/toron/plans/impl/0403_q08_reoriented_build_accumulator_streaming_results.md`:
- Phase 0: failing replay/stage, runtime entry point, `decompose_streaming_scan_probe_input` verdict (scan-eligible vs
  derived), orientation finding, fence census - before code;
- **(A1/A2)** the shape-fenced routing/eligibility extension (the plan-shape property loosened, why it stays generic,
  why derived builds still materialize) + q08 reach-board move (wall -> pass or named next wall) + q19/q04 no-regress;
- **or (B)** the sealed diagnostic + the named derived-build-spilling follow-on ADR;
- confirmation: no cap bump, no q08 branch, full gates green, controls unregressed, scope honesty (first brick, not the
  >360s time wall).

## Risk / blast radius
- **Derived-build** -> (B) stop-line; scopes the bigger cut, not a failure.
- **Routing/fence loosening breaks a streamed-path invariant** -> shape-fence tightly; existing streamed-path tests +
  q19/q04 controls + q08 correctness gate it.
- **Wrong build-side orientation** -> Phase 0 flags a cheaper upstream planner fix.
- **New wall after advancing** -> accepted only if named + reproduced.
- **Slow sf=10 windows** -> keep controls minimal (q19/q04); q08 fails fast (~1.23s) so the driver loop stays cheap.

## Seal
`plan: adr 403 q08 reoriented build accumulator streaming` (the dual). Implementation (extension or diagnostic +
evidence + re-measure): `impl: adr 403 q08 reoriented build accumulator streaming`. Do not commit `.perf.data`.
