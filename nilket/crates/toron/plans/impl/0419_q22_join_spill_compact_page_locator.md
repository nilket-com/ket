# ADR 419 - Implementation plan: q22 join-spill compact page locator

Pairs with `plans/adr/0419_q22_join_spill_compact_page_locator.md`. One bounded representation
cut: replace `SpilledJoinBuild`'s legacy encoded-key page index with a compact primitive page
locator behind an explicit key-plan seam, chosen between two candidate representations by
measured footprint AND lookup cost. q21 = classification only; q18 = carried `SpillBytes` row.
Stop-lines: typed-build-rows rework, or a key-in-two-pages discovery.

## Phase 0 - measure before choosing (before production behavior changes)

Temporary test-only candidate prototypes and instrumentation are allowed here because neither
candidate structure exists yet. Record the selection evidence, then remove the losing prototype and
all temporary hooks before Phase 1 is sealed.

1. **Reproduce q22's wall at current HEAD** (415 `.meta` protocol; scratch-clean). Build the
   release harness from the reviewed tree immediately before the probe:
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr419
   unit="toron-adr419-q22-pre"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q22 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr419/q22_pre.json 2> target/tpch/adr419/q22_pre.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
     > target/tpch/adr419/q22_pre.meta; systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
   Record: producer `join spill writer resident state`, request/available bytes against the
   reconstruction (1,500,000 x (64 + 9 + 8) + writer peak + page reserve = 121,631,060 /
   67,108,864), key plan (single Int64), null-safe flag (false), unique keys, page count, refs
   per key, writer peak, page reserve.
2. **Pin the invariant + blast radius:** verify one-page-per-key on the artifact (every legacy
   `Vec<usize>` length-1); grep/instrument which board queries ride `SpilledJoinBuild::write`.
   A key in two pages on any supported path -> stop-line (design a compact page-ref list; no
   silent page drop).
3. **Two-row representation selection** (focused fixture, temporary test-only versions of the
   proposed production structures, q22-shaped
   1.5M-key / 15M-probe present-missing mix; not a speed ADR):
   - open-addressed locator: completed resident bytes from bucket + entry capacities
     (projected ~48 MiB) + representative lookup timing;
   - sorted locator vec: `capacity x 16 B` (projected ~24 MB) + the same probe sequence
     (~21 comparisons x ~15M probes - measured, not assumed away).
   Selection rule: smallest representation with material sf=10 headroom AND no material probe
   regression vs the other candidate; both healthy -> sorted vector (larger margin, later
   asymptote). Record the table before the cut.
4. **q21 classification (no code):** crossing position (~batch 30,837 x 1,024 of 60M rows) and
   completed-shape projection: input rows, unique keys, refs per key, overflow capacity, pages,
   projected final accounted bytes. Feeds q21's own representation ADR.
5. **q18:** carry the `SpillBytes` row (536,899,968 / 536,870,912) unchanged.
6. Record everything before code.

## Phase 1 - the cut

1. **Explicit key-plan seam:** reuse
   `JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys)` inside each
   of the three legacy spilled-hash helpers before they construct
   `HashMap<Vec<u8>, Vec<u32>>`, and pass the derived plan into `SpilledJoinBuild::write`. The
   build map and page encoding stay as-is (typed `JoinBuildRows` rework = stop-line).
2. **Compact locator in `SpilledJoinBuild`:** selected only for single Int32/Int64,
   non-null-safe plans, before the first insertion; Generic/composite/null-safe keep the encoded
   `HashMap` path. Encoded keys decode through a plan-specific **checked** decoder (tag + exact
   length); malformed keys fail loudly. Never infer eligibility from byte length.
3. **Selected representation** (per Phase 0):
   - sorted vec: `{ key, page_index: u32 }` entries (16 B Int64 / 8 B Int32); preallocate from
     `build_rows.len()`; append during the single write pass; sort after the consumed map
     iterator drops (no second full-size vector); strict post-finish ordering; binary search
     returns exactly one page or none;
   - open-addressed: 414-style buckets + scalar u32 page value (NOT the 24-byte ref-list entry);
     bucket/entry capacity accounting.
4. **Accounting + guard:** resident accounting is capacity-based (locator capacity x entry size,
   observed), plus the unchanged writer peak and page reserve; the 64 MiB budget, guard site,
   and producer string are untouched. Lookup hands the original encoded key to page decoding -
   payload matching and build-row order unchanged.
5. Cleanup on write/finish/probe error paths unchanged and re-verified.

## Tests (eligibility-discriminating from the start)

- **Selection lock:** the production helper selects the compact locator before the first
  insertion on a primitive non-null-safe plan; legacy insertion count is zero on the compact
  path.
- **Wall-and-win lock:** a 1.5M-shaped (or mathematically equivalent) low-cap fixture - legacy
  twin fails the resident guard, compact path clears with real recorded headroom.
- **Direct-reuse negative lock:** the 4,194,304 x 4 + 2,097,152 x 24 = 67,108,864 arithmetic
  pinned (a ref-list-entry locator cannot clear the same fixture).
- **One-page-per-key pinning lock:** multi-page fixture on the actual writer; every legacy
  locator slot length-1; compact lookup returns exactly one page or none; covers the oversized
  single-entry page edge.
- **Probe parity:** present keys, absent keys, duplicate build rows within one key, page
  boundaries, preserved build-row order - compact vs legacy on identical builds.
- **Accounting lock:** accounted bytes == computed bytes, capacity-based, on grown and
  shrunk-to-fit states.
- **Fallback fences:** Generic, composite, and null-safe plans stay on the encoded path
  (selection lock inverted).
- **Truncation + malformed-key locks:** page index outside u32 rejected; wrong tag / wrong
  length encoded key fails loudly, never treated as absent.
- **Guard-armed lock:** a fixture still trips the unchanged producer past the cap on the compact
  path.
- **Cleanup lock:** scratch empty after write/finish/probe error.
- Carry the streaming/join/spill/reservoir suites green.

## Phase 2 - re-measure

```bash
# Rebuild after Phase 1; every post-cut row below must use this binary.
scripts/cargo-cgroup build -p toron-tpch --release

# q22 driver; record pass or the next named wall.
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  target/release/toron-tpch perf-run-one --engine toron --query q22 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr419/q22_post.json 2> target/tpch/adr419/q22_post.err
rm -rf /tmp/toron_join_spill

# Run only after q22_post reaches output; this is the primary pass@compare proof.
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  target/release/toron-tpch correctness --query q22 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr419/q22_correctness.out 2> target/tpch/adr419/q22_correctness.err
rm -rf /tmp/toron_join_spill

# do-not-disturb: q21 and q18 walls reproduce with EXACT producers/bytes
for q in q21 q18; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch perf-run-one --engine toron --query "$q" --mode strict --sf 10 \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr419/walls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
# controls: spill-bearing greens
for q in q03 q07 q09 q10 q12 q13 q14 q16 q19 q20; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr419/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
```
- q22 mechanism proof: the resident-state producer gone (or the named next wall byte-recorded);
  selected-representation completed capacity/computed bytes + writer/page terms recorded;
  post-cut q22 timing vs pre-cut (the lookup-cost fence, observed in production shape).
- Controls: bands (q03 233,338,304 / q07 134,732,480 / q09 108,992,256 / q10 215,703,848 /
  q12 159,510,976 / q13 9,152,992 / q14 36,464,000 / q16 60,078,400 / q19 51,471,552 /
  q20 256,104,392 + residency watch).
- q21: `semi/anti existence-set spill resident state` at 1,073,742,536 / 1,073,741,824
  unchanged; q18: `streamed inner join build spill writer` at 536,899,968 / 536,870,912
  unchanged.

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

`crates/toron/plans/impl/0419_q22_join_spill_compact_page_locator_results.md`:
- Phase 0: wall reconstruction verified on the artifact; one-page-per-key pin + blast-radius
  list; the two-row selection table and the chosen representation with its reasons;
- q21 completed-shape projection (the sibling ADR's opening evidence) and the carried q18 row;
- the cut: seam, decoder, representation, accounting; confirmation that build map, page
  encoding, cap, guard, producer, and fallback are unchanged;
- q22 outcome: pass@compare with mechanism proof and timing, or the named/byte-reproduced next
  wall;
- locks: why each is discriminating; the direct-reuse negative arithmetic;
- controls + walls do-not-disturb; the O(unique keys) asymptote statement (open-addressed
  ~sf=15 / sorted ~sf=30 at q22's shape) and the spilled/sharded-index forward pointer;
- full gates; no cap/guard/producer change; no `.perf.data`; scope honesty (q21, q18, q02, q05,
  q11/q17 remain named).

## Risk / blast radius

- **Lookup latency at 15M probes** -> the two-row selection fence; open-addressed fallback.
- **Wall behind the wall** -> reach honesty; named next wall is an accepted primary outcome.
- **Shared-writer regressions** (other `SpilledJoinBuild` riders) -> Phase 0 blast-radius list +
  probe-parity locks + spill-bearing controls.
- **Key-encoding drift** -> checked decoder + malformed-key lock.
- **capacity != len surprises** -> capacity-based accounting observed in the results doc.
- **Seam wants a typed-build-rows rework** -> stop-line; name the follow-on, keep 419 a locator
  cut.

## Seal

`plan: adr 419 q22 join spill compact page locator` (the dual). Implementation:
`impl: adr 419 q22 join spill compact page locator`. Do not commit `.perf.data`.
