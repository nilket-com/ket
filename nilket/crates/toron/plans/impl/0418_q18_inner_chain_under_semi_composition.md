# ADR 418 - Implementation plan: q18 inner-chain-under-semi composition

Pairs with `plans/adr/0418_q18_inner_chain_under_semi_composition.md`. One bounded composition:
admit a reoriented-inner-chain preserved side into the projected grouped LeftSemi fold through an
awaitable, chain-local two-arm sink. Gate = shape fence + non-consuming terminal metadata
shortcut, then unconditional projected spill. Stop-line: if the seam needs an executor-wide
callback/pull surface, seal as C-spec evidence; no partial rewrite.

## Phase 0 - persisted re-probe (before any code)

1. **Reproduce the wall at current HEAD** (fast: ~7 s to wall; scratch-clean; 415 `.meta`
   protocol):
   ```bash
   mkdir -p target/tpch/adr418
   unit="toron-adr418-q18-pre"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=360s \
     --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH="target/tpch/adr418/q18_scan_progress.jsonl" \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q18 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr418/q18_pre.json 2> target/tpch/adr418/q18_pre.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
     > target/tpch/adr418/q18_pre.meta; systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
2. **Persisted-path artifacts** (temporary/off-by-default bound-plan capture, removed before the
   cut, as in 417): re-capture the plan fingerprint (`d8b60e3a03bfaf79` as of 417) and the
   preserved-side `estimated_byte_size` values (417-era: customer 1,999,516,544 / orders
   12,315,016,448 / lineitem 68,489,092,352). Prove **on the artifact** that
   `reoriented_terminal_scan_build_estimated_over_cap` fires for the replay-1 terminal stage
   under the reproduced 64 MiB task budget (cap 1 GiB, threshold 8 GiB) and that the path is
   morsel-free. If metadata is absent or the shortcut does not fire: **stop, redesign the gate**
   in a named follow-on - do not proceed with a default-spill.
3. **Membership-side shape:** record the grouped lineitem membership plan (shape, projection,
   candidate recognizer, estimated group count) - it has never executed at sf=10; its wall status
   is unknown and must be recorded as such, not assumed green.
4. Record everything before code.

## Phase 1 - the cut

1. **Extract the completed-chain driver + `ReorientedChainOutputSink`** (private to
   `exec/join/streaming.rs`): a two-arm enum sink with an async `emit(batch)` -
   `GroupedFold` (calls the existing synchronous fold; returns ready) and `ProjectedSpill`
   (awaits `write_projected_batch_chunks` with the outer `retained_columns`, remapped semi keys,
   projected schema, `JoinKeyPlan`). The driver's await reaches every completed-output route:
   ordinary suffix helper, scan-probe replay-0, generic-base, partitioned-scan reservoir, and
   adaptive terminal flip. A terminal stage drains once per materialized output chunk; it may not
   collect one probe batch first because join fanout makes that collection unbounded. The exact
   q18 alias has one replay-1 suffix stage and therefore needs no intermediate-stage output vector.
   Existing vectors between nonterminal stages remain only in the behavior-preserving
   `GroupedFold` arm; no vector accumulates completed q18 output. The existing reoriented
   grouped-chain consumer migrates onto `GroupedFold` with behavior identical (its lib locks + the
   spill-bearing controls are the net).
2. **Chain-preserved alias in `try_streaming_projected_left_semi_grouped_aggregate`:**
   - fences (all pre-I/O): existing outer fences (LeftSemi, `match_filter: None`,
     exchange/group order rule, projection benefit) + `extract_reoriented_inner_chain` on the
     preserved exchange input yielding **exactly one replay-1 stage over replay 0** + terminal
     metadata shortcut + projection/key-plan/schema/sink checks. Longer chains stay unadmitted.
     Any failure -> `None`, vanilla runs, zero scans consumed;
   - **commit point:** after the first chain scan or spill write, the arm returns `Some(output)`
     or `Err`-after-cleanup only - never a late `None` (which would silently rescan
     customer/orders/lineitem through vanilla). Internal adaptive-to-partitioned fallback stays
     inside the committed arm under the scan-count locks;
   - drive the chain into `ProjectedSpill`, `finish` the spilled build, then execute the
     membership side and enter `fold_projected_streamed_left_semi_grouped_aggregate` unchanged;
   - **cleanup on every error/internal decline**, including membership failure after preserved
     pages were written (no leaked `/tmp/toron_join_spill` scratch).
3. **Exchange accounting (explicit grounded decision):** extraction begins beneath the
   preserved-side `Exchange`; either prove eliding it is behavior-neutral for this consumer (the
   spill is indexed by the semi key; the fold applies its own output repartition with
   `partition_count`/`exchange_key`) and write that proof into the results doc, or replay the
   exchange once in the driver. The multi-partition parity fixture makes this non-vacuous.
4. **Projection seeding:** chain-stage retained sets seeded backward from the outer
   `retained_columns` so no outer-required field is pruned before the sink; final batches written
   through the same full-semi-schema-to-projected-schema mapping as the materialized-left loop.
5. Telemetry: build/spill phases and counters recorded as in the existing projected spill arm.

## Tests (eligibility-discriminating from the start)

- **Primary lock:** direct recognizer invocation requires `Some(output)` on an exact one-stage
  replay-1-under-LeftSemi chain fixture (spill telemetry alone is not discriminating - the chain
  runtime or membership can spill while the composition misses).
- **Parity:** materialized-vs-streamed digest with mixed-type retained columns and distinct
  values per field, duplicate membership keys, unmatched keys, duplicate preserved rows (LeftSemi
  once-semantics), multi-partition preserved exchange; shape-fenced materialized twin.
- **Wall:** a small-cap fixture where the **preserved chain itself** exceeds the materialization
  cap - vanilla fails with the accumulator error, the direct recognizer succeeds.
- **Negative metadata fence:** missing/below-threshold terminal `estimated_byte_size` returns
  `None` **before any scan telemetry changes** (proves the non-consuming property, not just the
  variant).
- **Scan counts:** customer, orders, preserved lineitem, membership lineitem asserted separately;
  internal fallback cannot introduce a rescan loop.
- **Late-error cleanup:** membership failure after preserved pages written -> no spill scratch
  remains.
- **Recognizer fences:** a valid but longer reoriented chain and one malformed topology outside
  the admitted shape both take the vanilla arm without scan telemetry changes.
- **ScanRequest parity:** all scans via `bound_scan_request`;
  `bound_scan_request_carries_every_bound_scan_field` stays the field lock.
- Carry the streaming/join/spill/reservoir suites green (grouped-consumer neutrality).

## Phase 2 - re-measure

```bash
# q18 driver (hooks on for scan-count/streamed proof), then persisted correctness if it completes:
#   - pass@compare sf=10 (600s window), or
#   - a named + byte-reproduced next wall (expected candidate: the membership aggregate)
# controls: spill-bearing greens + q13 (417's fresh green, shares the spilled left-outer arm)
for q in q03 q07 q09 q10 q12 q13 q14 q16 q19 q20; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr418/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
```
- Mechanism proof: progress hooks show the preserved lineitem streaming (no scan-output
  accumulator), scan counts sane per table, spill signature present for the preserved build.
- Controls pass@compare with forensic bands (q03 233,338,304 / q07 134,732,480 / q09 108,992,256 /
  q10 215,703,848 / q12 159,510,976 / q14 36,464,000 / q16 60,078,400 / q19 51,471,552 /
  q20 256,104,392 + residency watch); q13 band from the 417 results doc (9,152,992).
- Outcome honesty: if the membership wall reproduces, record producer + bytes as the named next
  wall; no scope expansion in this seal.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron streaming --lib
scripts/cargo-cgroup test -p toron join --lib
scripts/cargo-cgroup test -p toron --test architecture_test        # FULL, after fmt
scripts/cargo-cgroup test -p toron --no-fail-fast                   # REQUIRED (executor cut)
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Deliverable (results doc)

`crates/toron/plans/impl/0418_q18_inner_chain_under_semi_composition_results.md`:
- Phase 0: fingerprint + metadata values + shortcut-fires proof + membership-side shape, recorded
  before code;
- the cut: sink extraction summary (exits unified, adaptive drain), alias fences, commit point,
  exchange-accounting decision with its proof, cleanup paths;
- q18 outcome: pass@compare sf=10 with mechanism proof, **or** the named/byte-reproduced next
  wall; scan counts; spill signature;
- locks: why each is eligibility-discriminating (the 417 review standard);
- controls + forensic bands; confirmation: no cap/scope change, ScanRequest parity held, no
  scan-spill-without-puller, grouped consumers behavior-neutral, full gates, no `.perf.data`,
  scope honesty (q21/q22, q02, q05, q11 remain the named families).

## Risk / blast radius

- **Membership wall behind the preserved wall** -> accepted named-wall outcome; no widening.
- **Sink extraction regresses grouped consumers** (four orchestration routes plus the shared
  suffix exit, adaptive sharpest) -> lib locks +
  q07/q09/q12/q16/q20 controls; `GroupedFold` preserves behavior, not just results.
- **Late-`None` rescan pathology** -> commit-point discipline + separate per-table scan counts.
- **Spill scratch leak on error paths** -> cleanup fixture; scratch-clean between runs.
- **Exchange elision wrong** -> explicit grounded decision + multi-partition parity.
- **Seam needs an executor-wide surface** -> C-spec stop-line; seal evidence, no partial rewrite.

## Seal

`plan: adr 418 q18 inner chain under semi composition` (the dual). Implementation:
`impl: adr 418 q18 inner chain under semi composition`. Do not commit `.perf.data`.
