# ADR 410 - Implementation plan: q10 reoriented build accumulator completion

Pairs with `plans/adr/410_q10_reoriented_build_accumulator_completion.md`. q10-led fast-wall completion: classify q10's
`reoriented join build accumulator` wall (7.6 s, 1.78 GB / 1 GiB), route the over-cap materialized build through the
existing spill machinery if Phase 0 proves that bounded, assess q12's 66 KB resident-state edge as a fold-in only if
trivial, and defer q05 to 411. **Operator stop-line: if q10's real fix is the one-pass reservoir, seal 410 as
diagnostic/fork evidence - 411 owns the reservoir.**

## Phase 0 - classify both walls (all probes fail in seconds; before any fix)
1. **q10 failing stage** (bounded probe with hooks):
   ```bash
   systemd-run --user --same-dir --wait --collect --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr410_q10_sf10_scan_progress.jsonl \
     --setenv=TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr410_q10_sf10_join_profiles.jsonl \
     /usr/bin/time -v timeout -k 10s 60s \
       target/release/toron-tpch perf-run-one --engine toron --query q10 --mode strict --sf 10 \
         --run-index 0 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr410_q10_sf10_probe.json 2> target/tpch/adr410_q10_sf10_probe.err || true
   ```
   Record: which join/replay walls; is its build derived or scan-shaped; terminal or non-terminal in the reoriented
   chain; why 409's flip did not absorb it (non-terminal / other side unfittable / metadata under the 8x shortcut);
   requested vs cap.
2. **Spill-writer reuse check (code):** `SpilledInnerJoinBuild::write_batch_chunks` already ingests `RecordBatch`
   input, and `ReorientedInnerJoinBuild::Spilled` already has a probe arm. Confirm the narrow plumbing: write each
   materialized build batch to spill **before** `concat_or_empty`, derive the same key plan from build/probe schemas,
   and preserve `projection`/`probe_columns_first` semantics. Bounded = existing writer + existing probe arm + no
   concat-first resident spike; new probe machinery or reservoir-shaped behavior = stop-line evidence.
3. **q12 overshoot classification (code + probe):** read `exec/join/spill.rs:940`'s resident-state check + eviction
   policy; classify the 67,728-byte overshoot as accounting (unshed-able bytes counted), eviction-ordering (check
   before evict), or genuine shortfall. **Fold-in only if a small accounting/eviction correction.**
4. Record all classifications in the results doc before code.

## Phase 1 - bounded completion (only if Phase 0 clears the stop-line)
- **q10:** at the accumulator wall sites (`streaming.rs:8770`/`:8786`), route the over-cap materialized build into the
  existing spill path instead of failing - generic, size-keyed, no q10/table branch; below-cap builds byte-unchanged.
  Preserve the memory guard semantics (the spilled build's resident budget still applies) and probe-side behavior per
  the Phase 0 reuse check. Do not concatenate the build batches before spilling; the point is to remove the accumulator
  resident spike, not to create the giant build batch and then write it elsewhere.
- **q12 (only if folded):** the named accounting/eviction correction at `spill.rs:940`, separately implemented and
  tested - never blended into q10's routing change. No budget raise.
- **Stop-line:** reservoir-shaped requirements -> no code; seal diagnostic + fork evidence for 411.

## Tests (discriminating)
- **q10 routing:** over-cap materialized build -> spilled build (assert the mode; no accumulator wall); below-cap ->
  unchanged in-memory accumulator; scan-shaped builds -> unchanged 403/409 paths;
- **spilled derived build correctness:** join output equality vs the in-memory path on a synthetic over-cap derived
  build (all consumed join kinds of the seam);
- **q12 (if folded):** the corrected accounting/eviction order makes the previously-66 KB-over q12 state compute under
  the existing budget; a genuinely-over state still fails (the correction is accounting, not a cap bump);
- carry the join suite + 409 adaptive tests + chain tests green.

## Phase 2 - re-measure
```bash
# driver: q10 sf=10 (mechanism proof via hooks, then rail + correctness if it completes)
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr410_q10_sf10_scan_progress_post.jsonl \
  /usr/bin/time -v timeout -k 10s 360s \
    target/release/toron-tpch perf-run-one --engine toron --query q10 --mode strict --sf 10 \
      --run-index 0 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr410_q10_sf10_post.json 2> target/tpch/adr410_q10_sf10_post.err || true
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch correctness --query q10 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data
# q12 same shape if folded
# won ground: q03 sf=10 re-probe (one lineitem pass held), q03 sf=1 four-way, controls
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr410_q03_sf1_control
for q in q03 q04 q08 q19; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data
done
```
- Primary: q10 pass@compare or named + reproduced next wall; mechanism proof (build batches written to the existing
  spill path before concat, no accumulator wall) + total-I/O sanity from the progress hook (no re-scan pathology
  reintroduced).
- Controls: q03 both scales in band (fp `666e0ac028036f70` at sf=1, one lineitem pass at sf=10); q08 keeps its 403
  streamed path; q19's spill byte-sane; q04 green.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (core executor cut); JOBS=4 if oomd
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast   # REQUIRED (full crate)
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)
`crates/toron/plans/impl/410_q10_reoriented_build_accumulator_completion_results.md`:
- Phase 0: q10 stage classification (derived/scan, terminal/non-terminal, why-not-409), spill-writer reuse verdict,
  q12 overshoot classification - before code;
- **either** the bounded routing fix (+ q12 correction if folded, separately evidenced) with q10's reach-board move,
  mechanism proof, and controls, **or** the sealed diagnostic + fork evidence naming 411 as the reservoir cut;
- confirmation: no cap bump, no qNN branch, spill machinery reused (no second path), full gates green, no `.perf.data`,
  scope honesty (q05 deferred; next wall named if one appears).

## Risk / blast radius
- **Reservoir-shaped fix** -> operator stop-line; diagnostic seals, 411 owns it.
- **Spilled derived build needs new probe machinery** -> Phase 0 reuse check; bounded only if narrow plumbing.
- **Concat-first spill preserves the spike** -> rejected; write batch chunks directly or stop-line.
- **Next wall behind the accumulator** -> reach-board honesty.
- **q12 hides a mechanism** -> fold-in bar; own cut.
- **q08/q19 shared-seam regression** -> controls + full gate.
- **Spill-volume pathology** (probe re-reads) -> progress-hook I/O sanity in acceptance.

## Seal
`plan: adr 410 q10 reoriented build accumulator completion` (the dual). Implementation (routing fix or diagnostic +
evidence): `impl: adr 410 q10 reoriented build accumulator completion`. Do not commit `.perf.data`.
