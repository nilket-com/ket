# ADR 435 - Implementation plan: generic bound grouped-fold hash migration

Pairs with `plans/adr/0435_generic_bound_grouped_fold_hash_migration.md`. Migrate the generic bound
grouped fold from a `BTreeMap<Vec<GroupValue>, Vec<AggState>>` to an O(1) hash table behind a
`BoundGroupedAggregateGroups` wrapper, across all 18 fold + 10 finalize sites, with **paired (key,
state) finalization** and **growth-aware residency** as the two non-negotiable rails. Candidate second
speed win on q13 (28.3 s, fold = 42.85% of the probe phase); the affected-set correctness fence + q13
356 before/after decide the claim.

## Phase 0 - generic-fold receipt + affected-set enumeration

1. Add a **generic aggregate receipt**, env-gated and **independent of ADR 433's
   `SpilledInnerJoinBuild` profiler** (that profiler only fires on the spill path; non-spilled
   generic-fold cells must not be invisible). It hangs off the generic fold/finalize, not the spill
   build.
2. It splits the post-434 fold cost into **allocation / BTreeMap compare-lookup / group_value eval /
   aggregate update** (counters + coarse phase clocks + sampling; **no per-row timers**) and records,
   per query, whether it routes through the generic fold at sf=10 — producing the **empirically
   affected generic-fold cell set**.
3. **Disabled** (env unset): construct no sink and no counter state; locked parity.

## Phase 1 - `BoundGroupedAggregateGroups` wrapper migration

1. Introduce `BoundGroupedAggregateGroups` owning an O(1) hash table (`HashMap`/`hashbrown` raw entry)
   keyed by the group values, exposing: `entry_or_init`, aggregate-state access, `resident_bytes`
   (Phase 2), and `finalize_sorted`.
2. **Migrate all 18 fold + 10 finalize sites** (`aggregate.rs:545/681/696/706/1299/1311/1406`;
   `streaming.rs:7227/7357/7998/8016/8026/…` and the finalize sites) to the wrapper. No raw `BTreeMap`
   group table remains outside the wrapper; an architecture gate asserts this.
3. **Rail 1 — paired (key, state) finalize:** `finalize_sorted` materializes `(key, states)` pairs and
   sorts the **pairs** (key and its states travel together), reproducing the prior BTreeMap emission
   order. Never sort a key list and zip it against `values()`.
4. **Depth (Phase-0-selected):** if lookup dominates, keep the `Vec<GroupValue>` key; if allocation
   dominates, add a columnar group-id path or a reused key buffer to remove the per-row allocation.
5. Box any new held state if the async body grows (433 stack-overflow lesson); reconcile any new file in
   the ADR 176 allowlist.

## Phase 2 - growth-aware residency

1. Re-derive `grouped_fold_resident_bytes` (`streaming.rs:1073`) for the hash table as three distinct
   quantities: **current resident bytes**, **projected rehash peak** (next-capacity bucket+entry bytes
   held simultaneously with the current table during rehash), and **finalize-sort scratch**.
2. On budgeted replay paths the wrapper **guards before allocation**: before a growth/rehash it checks
   the projected peak against the task budget; if it would exceed, it returns a valid fallback/error
   (named producer), never a transient over-allocation. Post-growth accounting alone is insufficient.
3. Update the 7+ guarded replay sites (`streaming.rs:12116`–`:12398`) to consult the growth-aware
   accounting.

## Tests

- **Order-sensitive batch parity (rail 1):** raw output batches byte-identical pre/post on
  representative generic-fold shapes — multi-key, null group keys, mixed aggregate kinds (COUNT/SUM/
  MIN/MAX/AVG), and the q13 double-group — proving no aggregate misassociation.
- **Forced-growth residency (rail 2):** a fixture that forces a rehash asserts the pre-allocation guard
  computes the projected peak; under a budget that cannot fit the peak, the fold yields the valid
  fallback/error state (peak math proven, no over-allocation).
- **Affected-set enumeration:** the generic receipt records fold routing for spilled and non-spilled
  grouped cells; disabled → no sink/state.
- **Architecture gate:** no raw `BTreeMap` group table survives outside the wrapper; all fold/finalize
  sites route through it.
- Existing aggregate, streaming, join, architecture, Toron, and TPC-H suites remain green; goldens and
  the deep-recursion lib test pass.

## Phase 3 - live proof and the speed claim

1. **Fold split (diagnostic):** re-run q13 with the generic receipt; confirm which sub-cost dominated
   pre-migration and that it collapses post-migration.
2. **Correctness:** `pass@compare` on the Phase-0-enumerated affected-set cells + full suite/goldens; q13
   fingerprint `ac3784c060c3c226`, digest, schema, 46 rows, 9,152,992 spill bytes unchanged.
3. **Speed (unprofiled 356-shaped):** q13 under `AllowedCPUs=0-1`, `CPUQuota=150%`,
   `MemoryHigh/MemoryMax=infinity`, `TasksMax=512`, `RuntimeMaxSec=3600`, 7 measured + 1 warmup, per-row
   invariance, `spread ≤ 0.20`, one-retry; **before 28.3 s → after** with band
   `max(0.10, 2 × max(spreads))`.
4. **Survival + residency:** q13 `pass@compare` under 4 GiB / no-swap; the growth-aware
   `grouped_fold_resident_bytes` holds within cap **as measured**; board stays 22/22.

## Results document

`plans/impl/0435_generic_bound_grouped_fold_hash_migration_results.md` records the Phase-0 fold split +
affected-set enumeration, the migration surface (28 sites), rail-1 batch parity, rail-2 forced-growth
peak/fallback proof, affected-set `pass@compare`, the unprofiled 356 before/after with the band, the
4 GiB q13 rail, measured residency, and the full gate battery. If the fold no longer dominates q13,
name its next wall with evidence.

## Verification

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

## Seal

Plan dual: `plan: adr 435 generic bound grouped fold hash migration`. Implementation seal:
`impl: adr 435 generic bound grouped fold hash migration`. Do not commit `.perf.data`.

