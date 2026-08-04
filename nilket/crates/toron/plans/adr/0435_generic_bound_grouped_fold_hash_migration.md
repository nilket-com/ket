# ADR 435 - Generic bound grouped-fold hash migration (throughput)

## Context

**ADR 434 named it:** after the decoded-page pin took q13 from 134.5 s → 28.3 s, the diagnostic ledger
puts `fold_bound_grouped_aggregate_batch` at **11,883 ms = 42.85%** of the 27,730 ms probe phase. q13
is now **28.3 s / 20.47x DuckDB**.

**Scope is the shared GENERIC bound grouped fold — not every GROUP BY.** The specialized **primitive
Int64 grouped-SUM** path (`aggregate.rs:6+`) already bypasses this fold; the affected set is
**empirical**, enumerated live in Phase 0, not assumed.

**Surface: a broad core-path migration.** The generic fold reaches **18 fold call sites** and **10
finalize call sites** (excluding definitions) across `aggregate.rs` and `streaming.rs`. This is a
core-path migration, not a few-caller swap — the group table must be owned by one abstraction so all 28
sites share identical lookup, alignment, and finalization.

**q13's fold shape (grounded).** Inner `GROUP BY c_custkey, COUNT(o_orderkey)` folds the LEFT OUTER
output — **15,337,604 rows into ~1,500,000 groups** — then a tiny outer `GROUP BY c_count` (46 rows).
The inner fold is the cost.

**Why it is slow — hypotheses, measured in Phase 0.** `fold_bound_grouped_aggregate_batch` keys a
**`BTreeMap<Vec<GroupValue>, Vec<AggState>>`** (`aggregate.rs:1313`): per row it allocates a
`Vec<GroupValue>` key and does an O(log n) `entry` lookup with `Vec` comparisons. The specific
comparison/allocation counts are hypotheses until Phase 0 measures them.

**Correctness is canonicalized — digest does NOT prove order.** TPC-H `pass@compare` uses
`SortPolicy::FullRowLex` (`determinism.rs:18/79`), which sorts rows before digesting. The real hazard
is **aggregate misassociation**: a sorted key list zipped against hash-order `values()` pairs `key[i]`
with the wrong state. The correctness rail is **ordered (key, state) parity** plus canonical value
digests.

**Residency assumes BTree overhead.** `grouped_fold_resident_bytes` (`streaming.rs:1073`) feeds 7+
guarded replay sites (`:12116`–`:12398`) and models BTree node overhead. A hash table changes residency
across three **distinct** quantities — current resident bytes, projected rehash peak, and finalize-sort
scratch — and a rehash **transiently peaks** before the old table frees.

## Decision — a group-table abstraction across the core path, with paired ordering and growth-aware residency

**Phase 0 — measure the fold split + enumerate the affected set (independent receipt).** A **generic
aggregate receipt**, off-by-default and **independent of ADR 433's `SpilledInnerJoinBuild` profiler**
(the 433 profiler is spill-path-only, so non-spilled generic-fold cells would be invisible). It splits
the post-434 fold into **allocation / BTreeMap compare-lookup / `group_value` evaluation / aggregate
update** and enumerates the **empirically affected generic-fold cell set** (spilled *and* non-spilled).
No per-row timers (they perturb the 15.3M-row hot path); counters + coarse phase clocks + sampling.
**Disabled, it constructs no sink or state.**

**Phase 1 — a `BoundGroupedAggregateGroups` abstraction (broad core-path migration).** One wrapper owns
the group table across all **18 fold + 10 finalize** sites, replacing `BTreeMap<Vec<GroupValue>,
Vec<AggState>>` with an O(1) hash table internally and exposing lookup, key/state alignment, and
finalization.
- **Rail 1 — paired (key, state) finalization (non-negotiable):** finalize emits a sorted
  **`(key, states)` view** — the key and its states sort **together**, never a sorted key list zipped
  against hash-order values. This reproduces the prior BTreeMap emission order exactly, so raw output
  batches are identical.
- **Depth (Phase-0-selected):** hash table only if lookup dominates; additionally eliminate the per-row
  `Vec<GroupValue>` (columnar group-id per batch / raw-entry with a reused key buffer) only if
  allocation dominates.

**Phase 2 — growth-aware residency (non-negotiable rail 2).** Re-derive `grouped_fold_resident_bytes`
for the hash table as three distinct quantities: current resident bytes, **projected rehash peak**, and
finalize-sort scratch. On budgeted replay paths the wrapper **guards and records growth BEFORE
allocation** — post-growth accounting cannot protect against a transient rehash peak — and produces a
valid fallback/error state if the projected peak exceeds budget. Whether the cap still holds is a
**measured acceptance result**, not an assumption.

## Acceptance

- **Rail 1 proven:** direct **order-sensitive batch parity tests** on the generic fold (raw output
  batches byte-identical pre/post across multi-key, null-key, and mixed-aggregate shapes — proving no
  misassociation), plus canonical `pass@compare`. Full `--no-fail-fast` suite + goldens green.
- **Rail 2 proven:** a **forced-growth test** that triggers a rehash, asserts the pre-allocation guard
  computes the projected peak, and on a budget that cannot fit the peak yields the valid fallback/error
  state (no transient over-allocation). Guarded replay paths stay within cap **as measured**.
- **Affected-set correctness, not a full-board redux:** `pass@compare` on the Phase-0-enumerated
  generic-fold cell set (via the independent receipt) **plus** the full unit/integration/doc suite,
  goldens, and **q13's 4 GiB / no-swap rail**. A full 22-cell sf=10 live redux is claimed only if run.
- **q13 speed (primary), unprofiled 356-shaped:** q13 under the exact 356 profile, 7 measured + 1
  warmup, per-row invariance, `spread ≤ 0.20`, one-retry; **before 28.3 s → after** with band
  `max(0.10, 2 × max(spreads))`.
- **Bonus (not a gate):** affected-set cells may speed up; report as bonus 356 before/after if run.
- **Discipline:** `exec.rs` ratchet; box new held state if the async body grows (433 lesson); the
  generic receipt off-by-default with locked parity; no query branding.

## Guardrails and risks

- The migration touches 28 core sites; the wrapper's single ownership is what keeps alignment correct —
  an architecture gate should assert no raw `BTreeMap` group table survives outside the wrapper.
- The transient rehash peak is the residency trap; the guard is **pre-allocation**, proven by the
  forced-growth test, not inferred from steady-state bytes.
- If Phase 0 shows allocation (not lookup) dominates, hash-swap alone underdelivers — depth is
  Phase-0-selected for that reason.

## Forks

- **A — scope:** shared **generic** bound grouped fold (converged); primitive Int64 SUM untouched;
  affected set empirical.
- **B — depth (Phase-0-selected):** hash-table-only vs hash-table + per-row-allocation elimination, by
  the measured allocation-vs-lookup split.

## Recommendation

**A = shared generic fold, B = Phase-0-selected**, anchored on the two non-negotiable rails — **paired
(key, state) finalization** and **growth-aware (pre-allocation) residency**. Smallest cut: the wrapper
migration with paired sorted finalize; go deeper on allocation only if Phase 0 shows it dominates. q13
is the headline; the affected-set cells are a bonus the article gets from one shared cut.

## Forward arc

q13's 28.3 s → after is the second article data point and the first shared-path win. If the fold no
longer dominates q13, re-profile for its next wall. q17/q19 are diagnosed on their own numbers; the
primitive Int64 SUM path and the deferred O(1) LRU remain separate threads.

