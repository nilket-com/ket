# ADR 427 - Implementation plan: q21 partition-at-a-time replay 1

Pairs with `plans/adr/427_q21_partition_at_a_time_replay1.md`. Restructure replay 1 in the q21
chain owner as a partition-at-a-time grace hash join: one source scan, one index build per
partition, streamed probe pages, under the live semi/anti-summary baseline with full owner-peak
accounting. Owner-scoped, reusing generic reservoir components; no global `PartitionedScan`
change. Stop-lines: max-partition index does not fit under the summary baseline (skew floor);
the drain requires a global executor change rather than owner scheduling.

## Phase 0 - measure before code (the de-risking gate)

1. **Reproduce the 426 before** on the promoted-plan HEAD: q21 sf=10 under 4 GiB / 600 s with the
   landed seven-stage receipt enabled; confirm replay 1 ~99.99%, ~232 re-scan probe calls.
2. **Skew-floor measurement (decisive):** at sf=10 under the completed owner baseline (the
   809,500,672-byte summary pair **plus every other retained inner runtime**), measure each
   orders partition's compact-index bytes and decoded build-page bytes. Record whether the
   largest full active-partition peak - current owner baseline, persistent reservoir state,
   index, pinned build pages, one probe page, bounded output, and fold state - fits under the
   unchanged cap. If not, **stop and seal as the named skew wall**; recursive sub-partitioning is
   a follow-on mechanism, not part of 427.
3. **Build-page-reread baseline:** confirm the current reservoir probe re-decodes build pages per
   call (the 420-class risk the drain must avoid), so the after-measurement has a before.
4. **Replay-0 output volume:** record the buffered probe-side row/byte volume at sf=10, the
   build-reservoir bytes, aggregate reservoir disk volume, and all persistent
   handle/page-metadata residency; confirm the probe side can stream per page rather than
   materializing. Preserve ADR 413's landed quota contract: each partition writer stays under
   the unchanged writer quota; do not invent an aggregate 512 MiB cap across all writers.
5. **Combined-pass feasibility:** record that today's `rebuild_index_from_pages` followed by
   `try_pin_decoded_pages` reads the build pages twice. Confirm the full active partition fits
   while pages are retained, because 427 requires a one-pass rebuild-and-pin seam rather than
   weakening the build-page-reread lock.
6. Record everything before code.

## Phase 1 - the cut (q21 chain owner, generic components)

1. **Deferred-drain admission** in `try_streaming_inner_prefix_semi_anti_scan_probe_base`: after
   the first replay-0 output and with both summaries built (baseline known), admit the
   partition-at-a-time path; a **zero-I/O pre-commit** decline (baseline leaves no headroom for
   even one partition) falls back to today's path before any scan/write.
2. **Buffer both sides once:**
   - write the orders build once into partition reservoirs
     (`build_partitioned_streaming_scan_inner_build_reservoir`, one scan);
   - buffer replay-0 output into a **spill-backed probe reservoir**, partitioned by the probe key.
3. **Partition-at-a-time drain:** for each partition `p`:
   - use a narrow combined rebuild-and-pin helper to build `p`'s compact index and retain its
     decoded build pages in **one page pass**; do not compose `rebuild_index_from_pages` with
     `try_pin_decoded_pages`, which would read them twice;
   - **stream `p`'s probe reservoir pages individually** against the index and pinned pages - **no
     `read_all_batch_pages`, no `concat_or_empty`, no partition-sized probe vector**; emit output
     batches through replay 2, semi, anti, and the fold before reading the next probe page;
   - drop `p`'s index and decoded build pages; advance.
4. **Full owner-peak guard:** before each partition's index build, guard
   `summaries + other inner runtimes + persistent build/probe reservoir state + this partition's
   index + pinned build pages + one probe page + bounded current outputs + fold state <= cap`,
   with each component named in the failure detail (the 424 detail-string discipline). The guard
   lives in the owner. Immediately before the combined rebuild-and-pin pass, refresh or explicitly
   pass the owner's **current** baseline into the active spill guard; the baseline captured when
   the file was created is insufficient after probe-reservoir metadata and fold state grow.
   Capacity-based accounting is used for owner containers, and outputs are not retained across
   probe pages. Recheck the full owner sum at every growth boundary: each reservoir write, each
   pinned build page/index-growth step, probe page/output materialization, and after each fold
   update while the active index/pages remain live. The combined helper transfers decoded pages
   into pinned storage and clears duplicate cache residency before crediting it; a pre-partition
   estimate is not the only guard.
5. **Committed-arm cleanup:** post-commit failure (index build, probe, guard crossing) is a
   labeled error after cleaning every created reservoir/partition file; no silent fallback.
6. **Receipt extension:** keep the same seven public stages and attribute both probe-reservoir
   writes and the deferred index/probe work to replay 1 without double-counting logical input or
   output rows. Add atomic per-partition and per-probe-page milestones (partition index, index
   builds, build/probe page reads, pages drained), flushed on the existing temp-then-rename
   cadence so a kill mid-drain leaves parseable evidence.
7. No `exec.rs` change; no comparator/schema/guard/summary change; other stages byte-identical.

## Tests (discriminating)

- **Correctness parity:** the restructured replay 1 yields the same q21 answer as the pre-cut
  path - full-row parity at sf=0.01/sf=1 against the unchanged oracle; the drive output digest
  unchanged.
- **One-scan / one-index-per-partition lock:** a q21-shaped chain fixture records exactly one
  orders source scan and exactly one index build per partition across many probe pages (receipt
  counters), where the pre-cut path records N.
- **Streaming-probe lock:** the drain makes no `read_all_batch_pages` / `concat_or_empty` /
  partition-sized probe allocation; a hook/counter proves per-page probe streaming.
- **Build-page-reread lock:** each partition's build pages are decoded once per drain; a counter
  proves the combined rebuild-and-pin pass reads each page once and probing performs no further
  build-page decode (420 class closed).
- **Full-peak guard lock:** a fixture where summaries + one-partition index + probe page +
  other runtime/reservoir state + pinned build pages + outputs + fold crosses the cap fails
  loudly with each component named; each component alone fits. A baseline-refresh lock grows
  the probe reservoir after file creation and proves the active partition guard charges that
  later residency.
- **Skew-floor lock:** a synthetic skewed partition whose full active peak cannot fit under the
  baseline triggers the named labeled error after cleanup, not a silent re-scan; recursive
  sub-partitioning is not implemented in this seal.
- **Committed-arm locks:** zero-I/O pre-commit decline falls back cleanly (no scan/write);
  post-commit failure is labeled and leaves empty reservoir scratch.
- **Receipt-durability lock:** a simulated kill after K drained partitions/pages leaves one
  parseable snapshot at that milestone; replay-1 logical row counters are not doubled by its
  buffer and drain phases.
- Carry the streaming/join/spill suites green.

## Phase 2 - re-measure (426 receipt before/after)

```bash
scripts/cargo-cgroup build -p toron-tpch --release
mkdir -p target/tpch/adr427
# canonical board envelope: 4 GiB / 600 s, receipt enabled
unit="toron-adr427-q21"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/env TORON_CHAIN_STAGE_PROFILE_PATH=target/tpch/adr427/q21_stages.json \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr427/q21.json 2> target/tpch/adr427/q21.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=CPUUsageNSec --property=MemoryPeak; } > target/tpch/adr427/q21.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill
# if it reaches output under the gate: the pass@compare board proof
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  target/release/toron-tpch correctness --query q21 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr427/q21_correctness.json 2>&1
rm -rf /tmp/toron_join_spill
```
- **Mechanism proof:** the receipt shows replay 1 collapsed from ~99.99% to a small term, one
  orders scan, one index per partition, and the deferred-drain milestones; name any
  newly-dominant stage.
- **Outcome:** q21 `pass@compare` under 4 GiB + 600 s -> **19/22**; or the named/reproduced next
  wall (skew or a stage behind) with the collapse still proven. Over-gate completion is
  diagnostic-only, no board credit.
- **Controls:** rerun the twelve established sf=10 controls
  (q03/q07/q09/q10/q11/q12/q13/q14/q16/q19/q20/q22) plus q18 under independent 4 GiB caps with
  the receipt environment unset. This core owner/scheduling cut does not reduce the control net
  to one spill-bearing sample.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # FULL, after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (engine cut)
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/427_q21_partition_at_a_time_replay1_results.md`:
- Phase 0: the 426 before, the skew-floor max-partition measurement under the live baseline, the
  build-page-reread baseline, the replay-0 buffer volume - before code;
- the cut: deferred-drain admission, both-sides reservoir, partition-at-a-time one-index drain,
  streamed probe pages, full owner-peak guard, committed cleanup, receipt extension;
- the before/after receipt split (replay 1 collapse; one scan / one index per partition; drain
  milestones); q21 outcome (pass@compare 19/22, or the named next wall);
- locks: why each discriminates (streaming-probe, build-page-reread, full-peak, skew, committed);
- controls untouched; `exec.rs` ratchet untouched; no `.perf.data`; scope honesty (q17/q02/q05).

## Risk / blast radius

- **Skew wall** -> Phase 0 measures it; seal with the named wall and carry recursive
  sub-partitioning as the next mechanism, never silently re-scan.
- **Stage behind replay 1** -> receipt re-measures; accepted named outcome.
- **Build-page re-decode** -> build-page-reread lock (420 class).
- **Owner-peak under-count** -> full-peak guard names every component; two-component crossing
  lock.
- **Committed-arm leak** -> post-commit cleanup + scratch check + durable receipt.
- **Global perturbation** -> owner-scoped; resident-shortcut stages untouched; controls confirm.

## Seal

`plan: adr 427 q21 partition at a time replay1` (the dual). Implementation:
`impl: adr 427 q21 partition at a time replay1`. Do not commit `.perf.data`.
