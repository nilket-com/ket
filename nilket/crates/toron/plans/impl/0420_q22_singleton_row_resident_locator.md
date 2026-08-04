# ADR 420 - Implementation plan: q22 singleton-row resident locator

Pairs with `plans/adr/0420_q22_singleton_row_resident_locator.md`. One bounded value
reinterpretation: for all-singleton primitive builds, the compact locator's existing u32 value
becomes a build-row ordinal (locator-wide mode as storage variants), making the probe resident
and eliminating the 704-915 GiB page-decode amplification. Page writes, guard, accounting,
telemetry, and both 419 entry layouts stay byte-identical. Stop-lines: q22 not all-singleton at
HEAD; typed build-map rework.

## Phase 0 - record before the cut

1. **Baseline + census at current HEAD** (415 `.meta` protocol; scratch-clean; build release
   first):
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr420
   unit="toron-adr420-q22-pre"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q22 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr420/q22_pre.json 2> target/tpch/adr420/q22_pre.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
     > target/tpch/adr420/q22_pre.meta; systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
   Expect the 419 outcome (timeout at the registered boundary, no Toron diagnostic, clean
   scratch). With temporary instrumentation (removed before the cut) record: singleton count
   (expect 1,500,000), multi-row count (expect 0), max row ordinal, baseline page reads /
   decrypted bytes / cache hits/misses through the boundary, and the unchanged locator
   accounting (50,331,648; 8 B Int32 / 16 B Int64 entries). **If multi-row count != 0: stop and
   re-scope.**
2. **Ownership re-proof at HEAD:** the three production `SpilledJoinBuild::write` call sites,
   u32 construction from `0..left_batch.num_rows()`, the resident batch through each probe, and
   bitmap-based match completion (`append_join_matches_for_kind`). File:line anchors in the ADR.
3. Record everything before code.

## Phase 1 - the cut

1. **Storage variants:** extend `JoinSpillPageLocatorStorage` with `Int32Rows` / `Int64Rows`
   using the same `CompactJoinSpillPageLocator<K>` entry structs (value = build-row ordinal).
   No mode bool, no per-entry tag; a row-mode locator has no page arm to misuse.
2. **Selection:** in `SpilledJoinBuild::write_inner`, compose the 419 key-plan fence with the
   metadata-only `build_rows.values().all(|rows| rows.len() == 1)` check, once, before locator
   construction. Primitive + all-singleton -> Rows; primitive + any multi-row -> Pages;
   Generic/composite/null-safe -> Encoded. Row ordinals are already u32 by the three helpers'
   existing build-row conversion fence; Pages variants retain 419's page-index truncation fence.
3. **Insert seam:** at the two flush sites (`spill.rs:495-497`, `:530-532`, where
   `current_entries` still holds each key's row list), Rows variants insert `rows[0]`, Pages
   variants insert `page_index`. Page payload, encoding, assignment, and build-row order
   untouched.
4. **Allocation-free lookup:** replace the `Vec<u32>` return of `SpilledJoinBuild::lookup`
   (`:586-605`) with `NoMatch | OneRow(u32) | Rows(Vec<u32>)` + a concrete iterator enum,
   consumed unchanged by the three probe loops (`matches.into_iter()`). Do not use
   `Box<dyn Iterator>`: that would allocate once per singleton probe. Page-decode arm keeps
   `Rows`; the resident arm is structurally scalar. Row-mode lookup never calls `read_page`.
5. **Observability:** add `#[cfg(test)] page_read_count` to `read_page` (the 419
   `legacy_insert_count` pattern) for the amplification lock.
6. Budget, guard site, producer, writer peak/page reserve terms, cache, cleanup,
   truncation/malformed-key fences, and capacity accounting: byte-identical to 419.

## Tests (eligibility-discriminating from the start)

- **Row-mode selection + amplification lock:** a production-helper all-singleton fixture selects
  row mode before the first insert, answers present/missing/anti probes correctly, and finishes
  with `page_read_count == 0`.
- **Mixed page-mode fixture:** any multi-row entry -> whole-locator page mode; `page_read_count
  > 0`; duplicate build-row order preserved across page boundaries.
- **LeftAnti + residual parity** on the row-mode path (build-match completion, not just paired
  output) against the legacy/encoded twin.
- **Structural allocation-free lock:** the resident arm has no singleton-`Vec` variant and its
  iterator is a concrete enum, not a boxed trait object (type-level, plus lookup/iterator unit
  tests).
- **419 value locks re-asserted exactly:** Int32 8 B / Int64 16 B entry sizes, capacity
  accounting arithmetic, direct-reuse negative, one-page-per-key pinning lock.
- **Fallback fences:** Generic, composite, null-safe -> Encoded; upstream build-row and page-index
  u32 fences remain armed; malformed encoded keys still fail loudly.
- **Unchanged-surface locks:** spill bytes/telemetry, writer peak, reserve, guard-armed producer,
  cleanup on write/finish/probe error.
- Carry the spill/join/streaming suites green.

## Phase 2 - re-measure

```bash
# First build/run with temporary release instrumentation that records the real q22 locator mode,
# probe-side read_page calls, decrypted bytes, and cache hits/misses. This hook is evidence only.
scripts/cargo-cgroup build -p toron-tpch --release
unit="toron-adr420-q22-profile"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one \
    --engine toron --query q22 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr420/q22_profile.json 2> target/tpch/adr420/q22_profile.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr420/q22_profile.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill

# Record zero probe-side reads, then remove every temporary hook, rebuild, and run the final
# hook-free binary. The final run must not rely on test-only or env-gated observability.
scripts/cargo-cgroup build -p toron-tpch --release
unit="toron-adr420-q22-post"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one \
    --engine toron --query q22 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr420/q22_post.json 2> target/tpch/adr420/q22_post.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr420/q22_post.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill

# Only after final q22_post reaches output: the primary independent pass@compare proof.
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q22 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr420/q22_correctness.out 2> target/tpch/adr420/q22_correctness.err
rm -rf /tmp/toron_join_spill

mkdir -p target/tpch/adr420/walls target/tpch/adr420/controls target/tpch/adr420/control_perf
# Do-not-disturb walls: separate artifacts with exact producers/bytes.
for q in q21 q18; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    /usr/bin/time -v target/release/toron-tpch perf-run-one \
      --engine toron --query "$q" --mode strict --sf 10 \
      --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr420/walls/${q}.json" 2> "target/tpch/adr420/walls/${q}.err"
  rm -rf /tmp/toron_join_spill
done

# Spill-bearing greens: persisted strict correctness.
for q in q03 q07 q09 q10 q12 q13 q14 q16 q19 q20; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    /usr/bin/time -v target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr420/controls/${q}.out" 2> "target/tpch/adr420/controls/${q}.err"
  rm -rf /tmp/toron_join_spill
done

# The separate reach leg proves the exact forensic spill signatures; correctness output does not
# carry spill telemetry. /usr/bin/time also retains the q20 residency watch.
for q in q03 q07 q09 q10 q12 q13 q14 q16 q19 q20; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    /usr/bin/time -v target/release/toron-tpch perf-run-one \
      --engine toron --query "$q" --mode strict --sf 10 \
      --data-source parquet --data-dir target/tpch/data \
    > "target/tpch/adr420/control_perf/${q}.json" \
    2> "target/tpch/adr420/control_perf/${q}.err"
  rm -rf /tmp/toron_join_spill
done
```
- q22 mechanism proof: the temporary real-shape profile records row mode and exactly zero
  probe-side page reads; all instrumentation is then removed. The rebuilt hook-free binary
  supplies execute time, peak RSS, and pass@compare, or the named/reproduced next wall.
- Controls: forensic bands (q03 233,338,304 / q07 134,732,480 / q09 108,992,256 /
  q10 215,703,848 / q12 159,510,976 / q13 9,152,992 / q14 36,464,000 / q16 60,078,400 /
  q19 51,471,552 / q20 256,104,392 + q20 residency watch). Walls: q21 1,073,742,536 /
  1,073,741,824 and q18 536,899,968 / 536,870,912 with exact producers.

## Verification

```text
scripts/cargo-cgroup fmt -p toron -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron --lib                         # REQUIRED full library gate
scripts/cargo-cgroup test -p toron --test architecture_test        # FULL, after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut)
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/0420_q22_singleton_row_resident_locator_results.md`:
- Phase 0: census (singleton/multi-row counts, max ordinal), ownership re-proof, baseline
  page-read/decrypt/cache/probe observability, unchanged 419 accounting - before code;
- the cut: variants, selection seam, insert seam, lookup/iterator enums; confirmation that page
  bytes, telemetry, guard, producer, and both entry layouts are byte-identical;
- q22 outcome: temporary real-shape zero-page-read mechanism proof, instrumentation-removal
  hygiene, and the hook-free final pass@compare/timing, or the named/reproduced next wall;
- locks: why each is discriminating; the re-asserted 419 value locks;
- controls + do-not-disturb walls; the unchanged O(unique keys) asymptote statement; the named
  separable follow-ons (page-write elision for fully-resident builds; per-key specialization if
  a mixed driver appears);
- full gates; no cap/guard/producer/page-format change; no `.perf.data`; scope honesty (q21
  representation ADR next; q18, q02, q05, q11/q17 remain named).

## Risk / blast radius

- **Wall behind the wall** -> reach honesty; named next wall accepted.
- **Shared-writer regressions** -> mixed/fallback fixtures + the ten controls; only the locator
  value meaning and lookup return type change.
- **Allocation regression at the boundary** -> structural lock (no singleton-Vec arm and no
  boxed iterator).
- **Census drift** -> Phase 0 stop-line.
- **Probe-loop iterator mismatch** -> the enum exposes `into_iter()`; the three loops compile
  unchanged (`--all-targets` + suites).

## Seal

`plan: adr 420 q22 singleton row resident locator` (the dual). Implementation:
`impl: adr 420 q22 singleton row resident locator`. Do not commit `.perf.data`.
