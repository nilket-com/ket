# ADR 424 - Implementation plan: q18 membership-first composition

Pairs with `plans/adr/0424_q18_membership_first_composition.md`. Executes the promoted 423
design with measured parameters: membership subtree first (rides 423's guarded primitive fold),
one-shot `Int64PresenceSet` (16 KB requested; observed allocator footprint measured in Phase 0),
membership-filtered fold arm, the semi outer projected spill never constructed. Four cycle
pins: drive-time lower-bound honesty; measured batch bytes in the construction guard; 424 owns
the scheduler receipt; baseline threading never shrinks TaskBudget. Stop-line unchanged from
423: container-refactor pressure keeps the set local.

## Phase 0 - re-verification (before code)

1. **Wall + fingerprint reproduction** (415 `.meta` protocol):
   ```bash
   scripts/cargo-cgroup build -p toron-tpch --release
   mkdir -p target/tpch/adr424
   unit="toron-adr424-q18-pre"
   systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
     --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
     --property=RuntimeMaxSec=600s \
     /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q18 \
       --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
     > target/tpch/adr424/q18_pre.json 2> target/tpch/adr424/q18_pre.err
   { echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result --property=MemoryPeak; } \
     > target/tpch/adr424/q18_pre.meta; systemctl --user reset-failed "$unit" 2>/dev/null || true
   rm -rf /tmp/toron_join_spill
   ```
   Expect `streamed inner join build spill writer` 536,899,968 / 536,870,912,
   `d8b60e3a03bfaf79`.
2. **Census + exact-subtree re-run at HEAD:** the bounded generator census re-proves 57 / 624
   keys and zero duplicates/nulls. Separately, reintroduce the temporary/off-by-default
   `q18mem`-style harness from 423 to execute exactly the persisted membership subtree at sf=1
   and sf=10 and record its returned schema, row count, batch count, and
   `sum(record_batch_memory_bytes(batch))`. The 456/4,992-byte logical key payloads are not
   substitutes for full Arrow batch capacity. Remove the harness before the production cut.
3. **Set microfixture:** requested vs observed `try_reserve_exact` capacities and the
   construction peak across requested and observed allocation states on the real allocator.
4. **Primitive-fold selection confirmation:** both exact-subtree runs still select 423's
   guarded fold; retain their query-neutral receipts as Phase-0 evidence.
5. Record everything before code. No open decision gates the topic; the 423 OOM stop-line is
   spent.

## Phase 1 - the cut

1. **`Int64PresenceSet`** (local to the chain owner; one-shot, guard-first; 423 plan step 1
   verbatim): `entries: Vec<i64>` / `buckets: Vec<u32>` (EMPTY = `u32::MAX`), seeded hasher,
   linear probe, 0.5 max load; `row_upper_bound = checked sum(num_rows)`, zero -> no allocation;
   checked `next_power_of_two` target preserving the EMPTY sentinel; guard requested
   entries+buckets -> `try_reserve_exact` entries -> guard observed entries + requested buckets
   **before** allocating buckets -> `try_reserve_exact` buckets while entries live -> guard the
   final observed entries+buckets -> insert with zero capacity changes; duplicate-idempotent;
   presence-only lookup. Requested and observed capacities are recorded independently and
   equality is never assumed. No generic container refactor (stop-line).
2. **Direct-filter fences** on the 418 admission (pre-I/O): one key per side, identical
   non-null Int64 both schemas, `match_filter: None`, non-null-safe; fence matrix rejects to
   the unchanged paths.
3. **Scheduler receipt** (424-owned, off-by-default env path, query-neutral name): flush
   `selected/committed` + `outer_writer_constructed=false` at commit, `membership_execute_
   started` before `execute_bound(right)`, then membership/set/chain milestones as reached.
   Durable (flushed, not process-exit); the accepted-timeout mechanism proof.
4. **Committed sequence** (ADR steps 2-8): commit -> membership (errors return, zero preserved
   scans) -> empty-set short-circuit (fold one typed empty `semi_output_schema` batch to
   initialize group types; no runtime preparation) -> set build under the construction peak
   (**measured batch bytes** + each requested/observed allocation state) -> drop batches only
   after the final capacity check -> terminal runtime with set bytes as the live owner baseline
   -> drive -> finalize. **Baseline threading (pin 4):** carried into
   the q18-exclusive `try_streaming_reoriented_inner_chain_scan_probe_base_to_sink` owner -
   both runtime preparations, its reservoir helpers, partitioned loaded-build checks, and every
   `SpilledInnerJoinBuild` resident guard it creates - as
   `baseline + local_resident vs unchanged retained-footprint cap`. The task-memory value for
   materialization/partition/cache/quota decisions is untouched; all other callers pass zero.
   Resident-guard detail names the baseline separately from local index/writer/page/loaded
   components so a crossing is byte-auditable.
5. **`MembershipFilteredGroupedFold` sink arm:** synchronous; probe compiled against the
   ORIGINAL `semi_output_schema` / `left_keys` (never the projected `key_plan` at
   `streaming.rs:7300-7310`); LeftSemi duplicate suppression; repartition survivors using the
   aggregate's original `partition_count` / `exchange_key` exactly like
   `fold_projected_left_semi_refs`, then feed each non-empty partition to the existing grouped
   fold. The semi outer writer is never constructed; a narrow owner-specific **construct**
   counter backs the no-outer-spill lock. `GroupedFold`/`ProjectedSpill` arms and the
   materialized branch stay byte-identical.
6. **Cleanup:** 423 plan step 6 verbatim (pre-set membership errors clean nothing; post-commit
   failures are labeled errors with exhaustive runtime/spill cleanup).

## Tests - the full 423 lock set, unchanged, plus receipt locks

- Direct-`Some` + mixed-type/full-row parity (duplicate membership keys; boundary exactly-300
  exclusion under strict `>`).
- Order lock: membership storage error opens the membership table, zero preserved tables,
  empty scratch.
- Empty-set lock: typed empty grouped output, zero preserved scans, zero set allocation.
- No-outer-spill lock: zero owner construct-counter calls on the admitted path; the
  materialized arm still constructs/exercises the writer; inner-stage spill legitimate.
- Presence-set locks: layouts (8 B / 4 B), requested-and-observed capacity accounting without
  assuming equality, the observed-entry + requested-bucket pre-allocation guard, duplicate
  idempotence, no growth after reservation, u32-domain rejection.
- Two-peak guard locks: construction guard fails with batches live where the set alone fits;
  combined set+runtime guard fails where each fits alone; labeled Memory errors, never
  fallback.
- Heuristic-invariance lock: the same stage produces the same materialization/partition/cache
  decision with a zero or nonzero owner baseline; only the resident guard's requested bytes
  increase by exactly the baseline.
- Fence matrix: nullable / null-safe / non-primitive / composite / missing-metadata /
  replay-2 / longer-chain -> `None` with zero scan requests.
- Committed-error locks: lost terminal stage and post-commit crossings are labeled errors
  after cleanup; no late `None`.
- **Receipt locks:** milestones flush in order and before membership execution; off by
  default; query-neutral; a simulated post-commit kill leaves a parseable receipt ending at
  the last reached milestone.
- Carry the aggregate/spill/join/streaming suites green.

## Phase 2 - re-measure

```bash
scripts/cargo-cgroup build -p toron-tpch --release
# q18 driver on the final binary (the live run settles drive time; >= ~137 s expected)
unit="toron-adr424-q18-post"
systemd-run --user --unit="$unit" --same-dir --wait --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/env TORON_MEMBERSHIP_SCHEDULE_RECEIPT_PATH=target/tpch/adr424/q18_receipt.jsonl \
  /usr/bin/time -v target/release/toron-tpch perf-run-one --engine toron --query q18 \
    --mode strict --sf 10 --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr424/q18_post.json 2> target/tpch/adr424/q18_post.err
{ echo "systemd_run_rc=$?"; systemctl --user show "$unit" --property=Result \
    --property=ExecMainStatus --property=MemoryPeak; } > target/tpch/adr424/q18_post.meta
systemctl --user reset-failed "$unit" 2>/dev/null || true
rm -rf /tmp/toron_join_spill
```

The receipt is permanent and off by default; no hook is removed. **Only if `q18_post` reached
output**, run the hook-free `pass@compare` proof:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --property=RuntimeMaxSec=600s \
  /usr/bin/time -v target/release/toron-tpch correctness --query q18 --sf 10 --mode strict \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr424/q18_correctness.json 2>&1
rm -rf /tmp/toron_join_spill
```

Run the twelve board-green controls after either accepted q18 outcome:

```bash
# twelve board-green controls
for q in q03 q07 q09 q10 q11 q12 q13 q14 q16 q19 q20 q22; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
    --property=RuntimeMaxSec=600s \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data \
    >> target/tpch/adr424/controls.out 2>&1
  rm -rf /tmp/toron_join_spill
done
```
- Mechanism proof on completion/typed return: membership scan starts > 0; zero owner construct
  calls; the `SpillBytes` producer absent. On external timeout, use only the flushed receipt's
  reached milestones plus the direct admission/no-writer locks; do not report unavailable
  final counters.
- Then `pass@compare` (18/22), **or** the named next wall: a 600 s timeout seals with the
  receipt's last milestone as the honest terminal (per pin 1, this is a legitimate outcome).
- Controls with the established forensic bands (q03 233,338,304 / q07 134,732,480 /
  q09 108,992,256 / q10 215,703,848 / q11 166,552,400 / q12 159,510,976 / q13 9,152,992 /
  q14 36,464,000 / q16 60,078,400 / q19 51,471,552 / q20 256,104,392 / q22 31,509,620 +
  q20 residency watch).

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

`crates/toron/plans/impl/0424_q18_membership_first_composition_results.md`:
- Phase 0: wall/fingerprint reproduction, census with measured batch bytes, set microfixture,
  fold-selection confirmation - before code;
- the cut: sequence, set, receipt, baseline threading (with the untouched-heuristics
  confirmation), sink arm, cleanup;
- q18 outcome: pass@compare with the mechanism proof and the measured drive time - or the
  named wall with the receipt's last milestone; the `SpillBytes` producer's absence either way;
- locks (the two-peak guards, no-outer-spill counter, and receipt-order locks especially);
- controls; telemetry-meaning confirmation; scope honesty (q21 profiling before any speedup
  claim; q02/q05/q17 named).

## Risk / blast radius

- **Drive time exceeds the gate** -> named wall with receipt proof; feeds the q21-family
  throughput evidence.
- **Two-peak guards on real numbers** -> locks stay; the 16 KB request is small, but observed
  capacities—not the request—decide.
- **Ordinal drift** -> original-key pin + parity.
- **Receipt overhead** -> off-by-default, milestone-flush only.
- **Shared-arm regressions** -> untouched-arm locks + twelve controls.

## Seal

`plan: adr 424 q18 membership first composition` (the dual). Implementation:
`impl: adr 424 q18 membership first composition`. Do not commit `.perf.data`.
