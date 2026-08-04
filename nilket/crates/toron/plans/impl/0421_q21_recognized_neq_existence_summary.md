# ADR 421 - Implementation plan: q21 recognized-Neq existence summary

Pairs with `plans/adr/0421_q21_recognized_neq_existence_summary.md`. One bounded mechanism: for
q21's chain-owned semi/anti stages whose residual is a structural non-null Int64 `Neq` between
one preserved and one existence column, replace the spilled row-ref existence build with a
resident `{key, representative} + Multi bit` summary - zero pages at build and probe. Separate
summaries per stage; combined live/peak accounting enforced at the chain owner. Stop-lines:
insufficient combined sf=10 headroom, or combined-peak enforcement requiring a broad accounting
rework -> compare shared-key dual summary / sharded build instead.

## Phase 0 - verify the premise (before any code)

1. **Bound residual dump** (temporary/off-by-default, removed before the cut): for both stages
   of the persisted sf=10 plan (`e2e25953a6a3fefd`), record the residual program root shape,
   operand node kinds, ordinals, resolved types, and nullability. Expect
   `Binary { op: Neq, Column(preserved l_suppkey), Column(existence l_suppkey) }` in some
   operand order; expect non-null Int64 on both sides and Int64 non-null-safe keys.
2. **Census re-verify at HEAD** (Parquet, persisted fixture):
   semi 59,986,052 rows / 15,000,000 keys (Single 2,143,942 / Multi 12,856,058);
   anti 37,929,348 rows / 13,753,474 keys (Single 3,372,250 / Multi 10,381,224);
   max 7 distinct values/key; zero null keys/values. Drift -> record and re-derive capacities.
3. **Combined-envelope measurement:** live inner-runtime resident bytes at the moment the
   second summary builds; preserved probe volume; projected dynamic growth peaks (bucket
   rehash, entry realloc, bitset growth) for the streaming build. Record the projected
   completed pair (809,500,672) against the 1 GiB retained-footprint cap and the measured
   inner-runtime term. Identify the exact runtime methods that will export that live term; a
   Phase-0 measurement constant is evidence, not an implementation accounting input.
4. **Wall reproduction** (415 `.meta` protocol):
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr421
   unit="toron-adr421-q21-pre"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr421/q21_pre.json 2> target/tpch/adr421/q21_pre.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
     > target/tpch/adr421/q21_pre.meta; systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
   Expect `semi/anti existence-set spill resident state` at 1,073,742,536 / 1,073,741,824.
5. **Stop-lines:** recognized shape absent at HEAD; sf=10 combined headroom not material; or
   combined-peak enforcement needs an accounting rework. Record all before code.

## Phase 1 - the cut

1. **Recognizer** `recognized_neq_existence_summary_columns` (private): single root that is
   `Binary { op: Neq }`; both children direct `Column` nodes (no cast/wrapper/literal/nesting),
   either operand order; classify child ordinals against the preserved width
   (`retained_existence_columns_for_semi_anti_stage` split precedent); exactly one child per
   side; range-check the existence-local ordinal after subtraction; cross-check resolved
   columns against side schemas; require non-null Int64 on both value columns and Int64
   non-null-safe keys. All pre-I/O.
2. **Chain-owned atomic preflight:** immediately after chain extraction/projection and before
   preparing inner runtimes or opening any scan/file,
   `try_streaming_inner_prefix_semi_anti_grouped_aggregate` recognizes **both** stages and binds
   their typed summary metadata. Both pass and the owner selects the two-summary path, or either
   rejection selects the existing two-spilled path for both stages. The generic
   `prepare_streaming_semi_anti_runtime` is NOT taught to self-select (other recognizers call it),
   and this cut has no mixed summary/spilled mode or late second-stage eligibility decision.
   Unrecognized shapes keep `SpilledSemiAntiExistenceBuild` byte-identically.
3. **Summary structure** (420-locator lineage): `{ key: i64, representative: i64 }` 16-byte
   entries, u32 buckets at 0.5 max load, one `Multi` bit per entry slot; capacity-based
   accounting exported as (buckets, entries, bitset, transient reservation). Insert: new key ->
   entry with representative; existing key, different value -> set Multi. No sentinel, no
   narrowing, no per-entry tag. Do not copy 420's pre-sized constructor: that writer knows
   `build_rows.len()`, while this streaming writer does not know unique-key cardinality pre-I/O.
4. **Build:** stream the projected `[l_orderkey, l_suppkey]` batches into the summary; commit
   at first insert (no fallback/rescan; loud `Err` after cleanup). Build semi first, then anti;
   a chain-owned ledger runs before every explicit bucket/entry/bitset growth: live inner
   runtimes + completed semi summary + current anti state + old/new transient reservation vs the
   retained-footprint cap. Grow to named power-of-two targets, observe actual capacities, and
   update the ledger before insertion; no `Vec::push`, reserve, rehash, or bitset helper may
   allocate implicitly outside that transaction. Use current runtime accounting for the inner
   baseline, never the Phase-0 measurement as a constant. Allocation/accounting failure after
   commit is a loud cleaned error.
5. **Probe:** key + preserved value column -> matched bitmap; missing -> false; Single(v) ->
   `v != probe`; Multi -> true. Zero page I/O and no per-key candidate/page allocation; the
   existing per-batch key grouping, matched bitmap, and output-selection allocations remain.
   LeftSemi/LeftAnti selection consumes the bitmap unchanged.
6. **Cleanup:** second-stage preparation failure after commitment cleans the first summary and
   all created runtimes; every error exit leaves scratch empty.
7. **Shrink-at-finish:** implement only if Phase 0 proves an in-place/no-double-allocation
   contraction; otherwise omit. Exact arithmetic pinned in the ADR (semi 28,657,608 / anti
   48,757,832 saved; ~1,000,898,648 transient if reallocating). Never credited against the
   ~sf=11 ceiling.

## Tests (eligibility-discriminating from the start)

- **Selection lock:** chain-owned preflight selects both summaries before any scan/file I/O,
  proven for both operand orders; the spilled build's create path is not reached. A fixture with
  one admitted and one rejected stage selects the unchanged two-spilled path before I/O.
- **Reject-matrix lock:** same-side columns, out-of-range ordinal, cast/wrapper child,
  multi-root, equi op, nullable preserved column, nullable existence column, non-Int64 key or
  value, membership-only -> each falls back to the spilled build with zero summary I/O.
- **Parity matrix vs the spilled twin,** LeftSemi AND LeftAnti: missing key, one repeated
  value, two distinct values, same-supplier probe, different-supplier probe.
- **Zero-page lock:** summary path completes with zero existence page opens/reads (test
  counter, the 419/420 pattern); the spilled twin's counter is nonzero on the same fixture.
- **Accounting lock:** accounted == computed for buckets/entries/bitset across growth; a
  low-cap guard-armed fixture trips the summary's own resident check loudly; test-only growth
  counters and before/after capacity snapshots prove each boundary reserves the old+new peak
  before allocation and that insertion itself does not change capacity.
- **Combined-accounting lock:** a two-runtime fixture where each summary fits alone but the
  owner-level combined check fails - two independent guards cannot pass it.
- **Cleanup locks:** second-stage failure after commitment leaves no first-summary residue and
  empty scratch; probe-error exit likewise.
- Carry the spill/join/streaming suites green; the unrecognized-shape fallback keeps
  `SpilledSemiAntiExistenceBuild`'s existing locks meaningful.

## Phase 2 - re-measure

```bash
scripts/cargo-cgroup build -p toron-tpch --release
# q21 driver on the final binary
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q21 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr421/q21_post.json 2> target/tpch/adr421/q21_post.err
rm -rf /tmp/toron_join_spill
# only after q21_post reaches output - the primary pass@compare proof:
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q21 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr421/q21_correctness.json 2>&1
rm -rf /tmp/toron_join_spill
# do-not-disturb wall
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  target/release/toron-tpch perf-run-one --engine toron --query q18 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  >> target/tpch/adr421/walls.out 2>&1
rm -rf /tmp/toron_join_spill
# eleven spill-bearing green controls (q22 joins the net)
for q in q03 q07 q09 q10 q12 q13 q14 q16 q19 q20 q22; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr421/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
```
- q21 mechanism proof: existence resident-state producer gone; final task evidence records both
  summary selections, rows/keys/Multi counts, completed/peak accounted bytes (including inner
  runtimes), structurally page-free mode, and max RSS; the test counter is the direct zero-page
  proof.
- Controls: forensic bands (q03 233,338,304 / q07 134,732,480 / q09 108,992,256 /
  q10 215,703,848 / q12 159,510,976 / q13 9,152,992 / q14 36,464,000 / q16 60,078,400 /
  q19 51,471,552 / q20 256,104,392 / q22 31,509,620 + q20 residency watch). q18 wall exact:
  `streamed inner join build spill writer` 536,899,968 / 536,870,912.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # FULL, after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut)
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/0421_q21_recognized_neq_existence_summary_results.md`:
- Phase 0: residual dump, census re-verify, combined-envelope measurement, wall reproduction -
  before code;
- the cut: recognizer, chain-owned selection, summary structure, owner-level combined
  accounting, cleanup; shrink included/omitted with the measured reason;
- q21 outcome: pass@compare with zero-page mechanism proof, completed/peak bytes, and timing -
  or the named/byte-reproduced next wall;
- locks: why each is discriminating (the combined-accounting lock especially);
- controls + q18 wall; the ~sf=11 power-of-two ceiling recorded separately from the completed
  pair; the sharded follow-on named with its now-2.5x-smaller payload;
- full gates; no cap/guard/producer change on the fallback path; no membership fold-in; no
  `.perf.data`; scope honesty (q18, q02, q05, q11/q17 remain named).

## Risk / blast radius

- **Combined envelope tighter than projected** (inner runtimes, growth transients) -> Phase 0
  measurement + the stop-line; never weaken accounting.
- **Recognizer over-trigger** -> chain-owned selection + reject-matrix lock.
- **Nullable/three-valued drift** -> nullability fenced at recognition; the reject matrix pins
  it.
- **A wall behind the wall** -> reach honesty; probe volume measured in Phase 0.
- **Shrink transient worse than steady-state win** -> pinned arithmetic; in-place proof required
  or omitted.

## Seal

`plan: adr 421 q21 recognized neq existence summary` (the dual). Implementation:
`impl: adr 421 q21 recognized neq existence summary`. Do not commit `.perf.data`.
