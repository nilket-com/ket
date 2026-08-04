# ADR 442 impl - whole-group terminal-build packing (unchanged spill reservation)

Mechanism cut on the membership-chain terminal path. `TaskBudget.spill_bytes` and
`choose_streaming_scan_inner_build_partitioning` untouched. Original group boundaries are the floor,
the fallback unit, and the resume unit.

## The change (membership-chain terminal phase, `streaming.rs:11580-11760`)

1. **Carry chooser outputs.** Keep `partition_count`, `group_count`, `partition_group_bounds`, and the
   chooser's per-partition estimate totals. No re-estimation, no extra scan.
2. **Pass 1 = chooser group 0 (byte-identical geometry) + calibration.** During the write, surface
   **actual serialized bytes per partition** from the writers (`spill_bytes` is already tracked per
   build; expose per-partition).
3. **Whole-group packing loop.** Maintain a cursor over original groups. Each pass admits the longest
   contiguous run of remaining groups with
   `projected_bytes ≤ ACTIVE_BUDGET × 90 / 100`, using checked `u128` intermediates. Projection uses
   per-group estimator totals multiplied by the **maximum observed actual/estimator ratio** so far.
   The 10% margin is fixed before measurement. Zero estimate, overflow, or missing calibration ⇒
   **one-group suffix fallback** (cursor advances one original group per pass, sealed geometry).
4. **Shared pass-budget reservation (pre-write enforcement).** Extend the spill write path so the pass
   owns a shared reservation: in `write_page_batch_inner` (`spill.rs:1630`), after
   `encode_record_batch_ipc_into` + encryption determine the exact **ciphertext length** and **before
   `write_plain_at`**, the page reserves `ciphertext_len` against the pass budget. Rejection ⇒ the page
   is **not written**; the pack cleans up; the cursor resumes the untouched suffix one original group
   at a time. The existing post-write writer check (`spill.rs:1676-1685`) remains as the per-writer
   backstop. Threading: the reservation handle is passed only on the membership-chain terminal path;
   all other writer call sites are unchanged (`None`).
5. **Aggregate ownership guard.** Before each pass and **before each batch allocation** (projected
   pre-growth check), guard
   `partition_replay_reservoir_bytes(&pack_builds, capacity)` (`streaming.rs:1050`, summing
   `owner_retained_resident_bytes`, `spill.rs:2443`) against the resident cap via the membership-owner
   guard. This accounts metadata + retained scratch, not a `pack_size × constant` estimate.
6. **Probe/delete per pass** — existing flow at pack width; each pass's groups cleaned before the next.
7. **Runtime observation** records: pass count k, pass boundaries (original-group runs), per-pass
   predicted/actual bytes, peak active spill, retry/rejection count, and the packing/fallback decision.

## Ledger schema (441 profiler, versioned)

Bump `MembershipChainProfile` schema (2): add **pass widths in original groups, predicted vs actual
per-pass bytes, peak active spill per pass, and retry counts**. Off-by-default behavior, atomic
publication, and disabled parity unchanged.

## Tests (`-p toron --lib` + integration)

- **Pack parity:** packed run vs per-group run — identical per-partition page row multisets and probe
  results across a multi-group pass boundary.
- **Whole-group boundaries:** packs always start/end on `partition_group_bounds` boundaries; no
  arbitrary partition ranges (assert on the recorded pass boundaries).
- **Calibration math:** max-ratio (not average) projection; checked-arithmetic overflow, zero estimate,
  and missing calibration each select the one-group suffix fallback; floor = one original group.
- **Suffix-fallback semantics:** after pass 1 completes and packing is rejected, execution resumes at
  group 1 (not group 0) one group at a time; completed work is never rerun.
- **Pre-write reservation:** a pass budget sized to reject the nth page ⇒ the page is not written
  (file length unchanged at rejection), the pack cleans up, and the suffix fallback completes with the
  identical result tuple; active spill never exceeds the budget at any observation point.
- **Aggregate ownership guard:** tight resident budget fails loudly at the projected pre-growth check
  with the existing producer taxonomy; all created writers reclaimed.
- **Zero-retry accounting:** a clean packed run reports retry_count 0 and cumulative spill exactly
  4,380,793,312-shaped (test-scale analogue); a run with one rejection reports retry_count 1 and is
  flagged non-cohort.
- **Chooser untouched:** existing 3/4-budget sizing tests unchanged; guard pins that
  `TaskBudget.spill_bytes` and the chooser are unmodified.

## Architecture guard

Slice the membership-chain terminal owner: assert the packing loop merges whole chooser groups
(uses `partition_group_bounds`), the pre-write reservation precedes `write_plain_at` on this path, the
aggregate guard uses the reservoir-bytes accountant, the one-group suffix fallback exists, and no query
literal appears.

## Speed rail (the seal gate) — ADR 356 dynamic band

- Baseline **222,182.319 ms, spread 0.0639** → band floor **12.78%**.
- Unprofiled 356-shaped strict sf=10 q18: 1 warmup + 7 measured; ≤0.20 stability; one retained retry
  (cohort-level, 356 rule); never average. Seal iff the median execute wall clears the band. Sub-band →
  honest-red → revise/remove.

## Cohort acceptance (zero-retry rule)

Accepted speed/ledger children must show **stable k, identical pass boundaries, and zero pre-write
rejections/retries** — otherwise routed rows ≠ k × 59,986,052 exactly and physical writes exceed the
pinned logical telemetry. A retrying child is disclosed and excluded; systematic retries are a red
finding, not a cohort-selection knob.

## Invariance + row flow (seven runs)

Fingerprint `d8b60e3a03bfaf79`, Arrow digest, schema, root 624 / external 100, **cumulative spill
4,380,793,312** — invariant across all 7 (zero-retry). Oracle rail identity 624 / 59,986,052 / 4,368 /
624 / 100. Routed rows = **measured k × 59,986,052, k < 22**.

## Corroboration + survival

- **441-ledger reread (corroborative only):** 5 fresh schema-2 children; routed rows k × 59,986,052;
  `terminal_build.partition_spill_write` absolute micros and share fall; per-pass peak active ≤
  384 MiB; residual ≤ 10%.
- **Survival:** persisted strict sf=10 q18 `pass@compare` under 4 GiB/no-swap with packing active;
  scratch fresh/empty before and after; RSS vs 1,231,652 KiB baseline.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```text
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

`exec.rs` 7,499; join-root ratchet holds; box new held state if the terminal async body grows.

## Implementation contingencies

- Measured k near 22 (no recoverable slack) or k < 22 but sub-band wall (terminal read dominates) ⇒
  honest-red receipt, revise/remove; never loosen the fixed 10% margin or the max-ratio rule to force a
  k.
- If the pre-write reservation cannot thread cleanly to `write_page_batch_inner` without touching
  non-terminal call sites, stop and revise — the reservation must not perturb other spill writers.
- If aggregate ownership cannot be guarded at useful pack widths under the survival envelope, the
  one-group floor preserves correctness; revise rather than weaken the guard.
