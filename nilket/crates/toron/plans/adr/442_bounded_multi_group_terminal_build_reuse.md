# ADR 442 - Bounded multi-group terminal-build reuse under the unchanged spill reservation

## Context

**441 selected the target.** The q18 membership-chain terminal build **rescans its 59,986,052-row input
once per group — 22 times — routing 1,319,693,144 rows** (~92.7 s routing + ~6.7 s scan of a 221 s
root, 5/5 stable). Cumulative spill written is one copy (terminal role 3,289,145,168 bytes); the waste
is scan+routing CPU.

**The invariant stays.** Groups bound **peak active on-disk spill** to
`streamed_inner_build_group_spill_budget = 3/4 × task_budget.spill_bytes` = **384 MiB** under the
unchanged **512 MiB** per-query reservation (`distributed/budget.rs:97`). This cut does **not** touch
`TaskBudget.spill_bytes` or `choose_streaming_scan_inner_build_partitioning` — raising the reservation
would re-shape grouped geometry board-wide and conflate budget tuning with the mechanism win. A strict
one-pass reservoir requires a separately authorized disk-envelope policy and a like-for-like baseline —
**named deferred, not bundled**.

**The slack being recovered.** The chooser packs groups from estimator totals
(`estimate_streaming_scan_inner_build_partition_bytes`), which measure projected hash/Arrow
**residency**, not serialized disk bytes, and run conservatively high against the 3.289 GB actual.
`ceil(3,289,145,168 / 402,653,184) = 9` is the **aggregate lower bound on pass count** — it does not
prove nine contiguous packs fit under partition skew. **Acceptance measures k; no pass-count is
predicted here.**

## Decision — merge contiguous chooser groups per pass, calibrated by actual serialized bytes

**Mechanism: whole-group packing.** Passes merge **contiguous original chooser groups** — never
arbitrary partition ranges — so the floor and every fallback boundary are the exact original group
boundaries:

1. **Pass 1 = chooser group 0, byte-identical geometry**, carrying the chooser's per-partition totals
   (no re-estimation). During the write, record **actual serialized bytes per partition**.
2. **Calibrated merging.** Later passes admit the longest contiguous run of remaining original groups
   whose projected serialized bytes fit **90% of `ACTIVE_BUDGET`**. Projection uses checked integer
   arithmetic and the **maximum observed actual/estimator ratio** (not an average density). The fixed
   10% safety margin is locked before measurement. A zero estimate, arithmetic overflow, or missing
   calibration selects the **one-group suffix fallback** for the remainder.
3. **Suffix fallback semantics.** Fallback never reruns completed work: it **resumes the untouched
   suffix one original group at a time** — the exact sealed geometry for the remaining groups.
4. **Hard pre-write peak enforcement.** Today the writer detects budget excess only **after** a page is
   written (`write_page_batch_inner`, `spill.rs:1674-1685`). This cut adds a **shared pass-budget
   reservation** checked after IPC encoding/encryption determines the exact ciphertext length and
   **before `write_plain_at`**: a rejected page cleans up the pack **without the pack ever exceeding
   384 MiB on disk**, and the remainder resumes via the suffix fallback.
5. **Real aggregate writer ownership guarded.** `pack_size × (page reserve + scratch estimate)`
   undercounts metadata and retained scratch. The guard **reuses/extends
   `partition_replay_reservoir_bytes` (`streaming.rs:1050`) over `owner_retained_resident_bytes`
   (`spill.rs:2443`)** for the pack's live writers, with a **projected pre-growth check before each
   batch allocation**.
6. **Probe/delete per pass** — the existing intermediate-read + reservoir-probe flow at pack width;
   partition-at-a-time probing keeps index residency unchanged; each pass's groups are cleaned up
   before the next pass.

## Correctness

- Whole-group packing preserves per-partition page content exactly — same partition function, same
  rows, same pages; only which original groups co-exist changes. The one-scan parity lock extends
  across the pass boundary.
- Cumulative spill telemetry is predeclarable **only for zero-retry runs** (a rejected page implies a
  cleaned-up partial pack, i.e. physical writes above the logical volume). Hence the cohort rule below.
- Cleanup on every exit: pack-create, write, pre-write rejection, probe, and success paths reclaim all
  live pack partitions.

## Acceptance

- **Measured-k gate (zero-retry cohorts only).** Every accepted child has **stable k and identical pass
  boundaries with zero pre-write rejections/retries**; terminal routed rows are then exactly
  **k × 59,986,052** with **k reported and k < 22**. A child with a retry is rejected from the cohort
  (physical writes would exceed the pinned logical telemetry and routed rows would not be exact).
- **The wall shrinks — ADR 356 dynamic band.** Baseline **222,182.319 ms, spread 0.0639**; band =
  `max(0.10, 2 × max(0.0639, current_spread))`, **floor 12.78%**. Unprofiled 356-shaped strict sf=10
  q18, 1 warmup + 7 measured; ≤0.20 stability; one retained retry (cohort-level, per the 356 rule);
  never average. Sub-band → honest-red → revise/remove.
- **Peak-active proof:** per-pass actual active spill ≤ 384 MiB, enforced pre-write and reported
  per pass.
- **Output + telemetry invariance:** fingerprint `d8b60e3a03bfaf79`, Arrow digest, schema, root 624 /
  external 100 rows, **cumulative spill 4,380,793,312** — all seven runs.
- **Row-flow identity** via the 441 oracle rail: 624 / 59,986,052 / 4,368 / 624 / 100.
- **Ledger schema versioned.** The 441 profiler schema is bumped to record **pass widths (in original
  groups), predicted vs actual pass bytes, peak active spill, and retry counts**; the corroborative
  five-child reread shows `terminal_build.partition_spill_write` rows = k × 59,986,052 and falling
  absolute micros/share.
- **4 GiB/no-swap survival** with packing active: persisted strict sf=10 q18 `pass@compare`, scratch
  fresh/empty before and after, RSS vs the 1,231,652 KiB baseline.
- **Fallback equivalence:** packing disabled/unsafe ⇒ the original 22-group geometry runs
  byte-identically (test-pinned); post-pass-1 fallback resumes the suffix one group at a time.
- **No budget change:** `TaskBudget.spill_bytes` and the chooser untouched — guard-pinned.

## Guardrails and risks

- **The 384 MiB bound is now enforced before disk write**, not observed after; estimator slack is
  recovered only to the extent actual serialized density allows — if calibration finds no slack
  (k ≈ 22), the honest result is a sub-band red, and the margin is not loosened to force a k.
- **Skew honesty** — the aggregate lower bound (9) is not a target; contiguous whole-group packing
  under skew yields whatever k it yields, and acceptance reports it.
- **Aggregate writer ownership** is the survival exposure; the reservoir-bytes accountant + pre-growth
  check is the mitigation, tight-budget tested.
- **Strict one-pass** stays out pending its own disk-envelope authorization.

## Forward arc

If the band clears, a 441-profiler reread re-ranks q18's next producer (the 73 s terminal spill read is
the natural suspect). **q05 grouped-reservoir diagnosis** is the next large wall. The q19-family owner
shares the per-group rescan shape (small measured cost) — deferred.

## Deferred (named)

- **Strict one-pass reservoir** under an explicitly authorized disk-envelope policy + like-for-like
  baseline.
- **Generalize whole-group packing to the q19-family partitioned scan-build owner.**
- **q05 diagnosis; q17 remeasurement; q19 re-diagnosis + 438-profiler widening; Cut A (439 mirror).**
