# ADR 423 - Implementation plan: q18 membership-first semi fold

Pairs with `plans/adr/0423_q18_membership_first_semi_fold.md`. One bounded scheduling cut on the
admitted q18 chain: execute the membership subtree first, hold the post-HAVING keys in a
one-shot budgeted `Int64PresenceSet`, stream completed chain batches through a
membership-filtered fold arm, and never construct the semi's outer projected spill. Stop-lines:
membership-fold cgroup OOM (topic pivots to guarded/primitive grouped-fold work); presence-set
headroom insufficient at measured cardinality; container-refactor pressure.

## Phase 0 - the decision table (before the production cut)

1. **Exact plan probe:** persisted sf=10 q18 fingerprint (`d8b60e3a03bfaf79` as of 421-era
   walls); one Int64 non-null LeftSemi key per side, `null_safe_keys == false`, one-stage
   replay-1 chain, terminal metadata shortcut. Mismatch -> stop/re-scope.
2. **Membership census** (bounded generator pass - never `strict_q18_oracle(10)`, never inferred
   from a timed run): `tpchgen` emits an order's lineitems contiguously and its sparse order-key
   function is monotonic. Keep only the current key and checked quantity sum, assert every next
   key is greater, and count sums strictly above 300. Record pre-limit post-HAVING key rows at
   sf=1 and sf=10, duplicate/null counts (expect zero), projected output-batch bytes, and set
   capacities/accounted bytes. The existing oracle's large order/quantity hash maps are not a
   bounded census. Insufficient retained headroom for the two peaks -> stop/re-scope Fork C.
3. **Isolated membership subtree at sf=10** under the persisted cgroup (415 `.meta` protocol):
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr423
   unit="toron-adr423-q18-membership"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/env TORON_GROUPED_FOLD_PROGRESS_PATH=target/tpch/adr423/q18_membership.progress \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q18mem \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr423/q18_membership.json 2> target/tpch/adr423/q18_membership.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
       --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr423/q18_membership.meta
   systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
   (`q18mem` = a temporary/off-by-default harness attempt that executes exactly the persisted
   membership subtree; removed before the cut.) Its temporary grouped-fold diagnostic must
   durably checkpoint scan-open plus input-row/group progress so an externally killed run does
   not depend on process-exit telemetry. Record elapsed, peak RSS, terminal status, completed
   groups on completion, or the last durable progress on timeout. **Completion / typed budget
   error / in-gate timeout -> continue. Kernel or cgroup OOM -> STOP: the topic pivots.** A
   timeout must be distinguishable from a hang and leave no scratch.
4. **Set microfixture:** requested vs actual `try_reserve_exact` capacities and the
   construction peak on the real allocator. Equality is not assumed; any observed capacity
   that removes planned headroom -> fix accounting before the cut.
5. **Baseline wall reproduction:** q18 sf=10 still fails at `SpillBytes`
   536,899,968 / 536,870,912.
6. Record everything before the production cut.

## Phase 1 - the cut

1. **`Int64PresenceSet`** (local to the chain owner; 421 lineage, one-shot):
   `entries: Vec<i64>`, `buckets: Vec<u32>` (EMPTY sentinel), seeded hasher, linear probe, 0.5
   max load. Construction: `row_upper_bound = sum(num_rows)`; zero -> no allocation
   (short-circuit path); else `entry_capacity = max(1024, bound).next_power_of_two()` (checked;
   the target must preserve `u32::MAX` as EMPTY), buckets = 2x. Guard the requested total;
   allocate entries with `try_reserve_exact`; guard observed entry capacity; allocate and
   resize buckets while entries remain live; guard the final observed total; insert with zero
   capacity changes. `try_reserve_exact` capacity equality is never assumed. Duplicates find
   the existing entry. Presence-only lookup. **No generic container refactor** - stop-line.
2. **Direct-filter fences** added to the 418 admission (pre-I/O): one key per side, identical
   non-null Int64 types both schemas, `match_filter: None`, non-null-safe; the fence matrix
   rejects to the unchanged paths.
3. **Committed sequence** exactly as the ADR pins (steps 2-8): commit -> membership execution
   (errors return with zero preserved scans) -> empty-set short-circuit (typed empty grouped
   result, no set, no preserved prep/scan) -> set build under the **construction peak**
   (batch bytes + requested allocation) -> drop batches -> prepare terminal runtime with
   **set bytes as the live owner baseline** (threaded into preparation/drive guards
   pre-allocation) -> drive into the new `MembershipFilteredGroupedFold` sink arm -> finalize.
   Make that threading concrete at the q18-exclusive
   `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` owner: carry the baseline into
   terminal and base runtime preparation, the build/probe reservoir helpers it invokes, and
   each `SpilledInnerJoinBuild` resident guard it creates. Those guards compare
   `baseline + local resident` against the original retained-footprint cap. Do not lower the
   task-memory value used by materialization/partition heuristics; all other callers pass a
   zero baseline.
4. **The sink arm:** synchronous; compiles its probe against the ORIGINAL
   `semi_output_schema` / `left_keys` (never the projected spill `key_plan` at
   `streaming.rs:7300-7310`); retains matching rows once (LeftSemi duplicate suppression);
   calls the existing grouped fold. The semi outer `SpilledInnerJoinBuild` is never created on
   this path; `GroupedFold`/`ProjectedSpill` arms and the materialized branch stay
   byte-identical.
5. **Accounting + telemetry:** two peaks with a membership-first-specific producer label
   against the unchanged retained-footprint cap; established replay/counter meanings preserved;
   a narrow test counter (or owner-specific hook) on the semi outer writer for the
   no-outer-spill lock. Add an off-by-default durable scheduler receipt (generic mechanism name,
   no query/ADR branding) that flushes `selected/committed`, `membership_execute_started`, and
   `outer_writer_constructed=false` before entering membership execution, then appends
   membership/set/chain milestones if reached. This is the accepted-timeout mechanism proof;
   completion and typed returns still use the ordinary counters.
6. **Cleanup:** membership error pre-set = nothing to clean; post-commit failures (lost
   terminal stage, guard crossing, drive error) are labeled errors with exhaustive
   runtime/spill cleanup, 418-style.

## Tests (eligibility-discriminating from the start)

- **Direct-`Some` + parity:** the exact chain returns `Some`; mixed-type/full-row digest parity
  vs a shape-fenced materialized twin, including duplicate membership keys and boundary
  (exactly-300) groups excluded by strict `>`.
- **Order lock:** a membership storage error opens the membership table and ZERO preserved
  tables; scratch empty.
- **Empty-set lock:** zero membership rows -> schema-correct empty grouped output, zero
  preserved scan requests, zero set allocation.
- **No-outer-spill lock:** the admitted path records zero semi-outer-writer calls while the
  materialized/non-chain arm still exercises that writer; inner-stage spill stays legitimate.
- **Presence-set locks:** 8 B/4 B layouts; requested and observed capacity accounting without
  assuming equality; duplicate idempotence; no growth after reservation; u32-domain rejection;
  presence-only lookup.
- **Two-peak guard locks:** construction guard fails with batches live where the set alone
  would fit; combined set+runtime guard fails where each alone fits; both produce the labeled
  Memory error, never fallback.
- **Fence matrix:** nullable / null-safe / non-primitive / composite / missing-metadata /
  replay-2 / longer-chain -> `None` with zero scan requests.
- **Committed-error locks:** lost terminal stage and post-commit guard crossings are labeled
  errors after cleanup; no late `None`.
- Carry the streaming/join/spill/aggregate suites green.

## Phase 2 - re-measure

```bash
scripts/cargo-cgroup build -p toron-tpch --release
# q18 driver on the final binary
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/env TORON_MEMBERSHIP_FIRST_PROFILE_PATH=target/tpch/adr423/q18_scheduler.profile \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q18 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr423/q18_post.json 2> target/tpch/adr423/q18_post.err
rm -rf /tmp/toron_join_spill
# only after q18_post reaches output - the primary pass@compare proof:
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q18 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr423/q18_correctness.json 2>&1
rm -rf /tmp/toron_join_spill
# twelve board-green controls
for q in q03 q07 q09 q10 q11 q12 q13 q14 q16 q19 q20 q22; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr423/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
```
- Mechanism proof on completion/typed return: membership scan starts > 0; semi outer projected
  writes == 0; the `SpillBytes` producer gone. On external timeout, use the flushed scheduler
  receipt plus the direct scan-order/no-writer locks; do not report unavailable final counters.
  Then `pass@compare`, or the measured membership-fold wall (typed producer with completed
  counts, or in-gate timeout with elapsed, peak RSS, and last durable progress).
- Controls with forensic bands (q03 233,338,304 / q07 134,732,480 / q09 108,992,256 /
  q10 215,703,848 / q11 166,552,400 / q12 159,510,976 / q13 9,152,992 / q14 36,464,000 /
  q16 60,078,400 / q19 51,471,552 / q20 256,104,392 / q22 31,509,620 + q20 residency watch).

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron aggregate --lib
scripts/cargo-cgroup test -p toron spill --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron --lib                         # REQUIRED full lib leg
scripts/cargo-cgroup test -p toron --test architecture_test        # FULL, after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut)
scripts/cargo-cgroup test -p toron-tpch                            # REQUIRED base feature set
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/0423_q18_membership_first_semi_fold_results.md`:
- Phase 0: the decision table filled - plan probe, sf=1/sf=10 census (pre-limit cardinality),
  isolated membership run (elapsed/RSS/terminal plus completed groups or last durable progress),
  set microfixture, baseline wall - before production code; the stop-line verdicts stated
  explicitly;
- the cut: presence set, fences, committed sequence, sink arm, two-peak accounting, cleanup;
- q18 outcome: pass@compare with the ordinary counter proof, a typed membership-fold wall with
  its completed counters, or a timed membership-fold wall with the durable scheduler receipt
  and last progress; the `SpillBytes` producer's absence recorded without inventing killed-
  process counters;
- locks: why each is discriminating (the two-peak guards and no-outer-spill counter
  especially);
- controls; telemetry-meaning confirmation; the named follow-ons (primitive grouped-fold
  specialization; q21 profiling before any speedup claim); scope honesty (q21/q02/q05/q17
  remain named).

## Risk / blast radius

- **Membership fold OOMs in Phase 0** -> stop-line; topic pivots; no code lands.
- **Membership fold walls typed/timeout** -> named advance; the follow-on is measured.
- **Cardinality/headroom miss** -> Fork C re-scope; no membership spill invented.
- **Ordinal drift in the filter** -> original-key pin + parity locks.
- **Chain-drive wall behind the removed writer** -> reach honesty; 418 proved the chain to
  512 MiB of output already.
- **Shared-arm regressions** -> untouched-arm locks + the twelve controls.

## Seal

`plan: adr 423 q18 membership first semi fold` (the dual). Implementation:
`impl: adr 423 q18 membership first semi fold`. Do not commit `.perf.data`.
