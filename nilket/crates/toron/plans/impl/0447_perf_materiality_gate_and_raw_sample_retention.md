# impl 447 - perf materiality gate and raw sample retention

Paired with `adr/0447_perf_materiality_gate_and_raw_sample_retention.md`. Rail-wide,
additive; the regression rail's behavior is unchanged and pinned by test. Lands in
`toron-tpch`.

## Seams

| seam | file:line | role |
|---|---|---|
| the band expression | `crates/toron-tpch/src/main.rs:2485` | `REGRESSION_THRESHOLD.max(2.0 * spread_ratio.max(baseline_spread))` |
| the alarm it feeds | `main.rs:2486-2488` | one-sided: `(execute_median - baseline_median)/baseline_median > band` → `slower_than_baseline` |
| the constant | `main.rs:2452` | `const REGRESSION_THRESHOLD: f64 = 0.10;` |
| the summary emitter | `main.rs:2493+` | `tpch_perf_summary` json — where raw samples must land |
| existing alarm test | `main.rs:2800` | `perf_summary_flags_same_plan_slowdown_beyond_noise_band` — the behavior-neutrality pin |
| per-run measurement | `main.rs:1865` `run_duckdb_perf_child` / the Toron perf child | one warmup + seven measured children (`impl/356_...:47,174`) |
| **the interleave blocker** | `main.rs:2228-2231` `run_perf_child_args` | re-execs `std::env::current_exe()` — the parent's identity fixes every child's binary |
| the override precedent | `main.rs:956` `duckdb_binary()` | `TORON_TPCH_DUCKDB_BIN`; **no Toron equivalent exists** |
| the misuse to supersede | `adr/443:12`, `adr/444:96` | band cited as a materiality gate / 0.2386 envelope |

## Grounding steps before the diff

1. **Where are the seven per-run rows today?** `impl/356_...:174` records "one warmup
   row and exactly seven measured rows with run indices 0-6", so the rows exist at
   measurement time and are aggregated away before the receipt. Find the aggregation
   point. **If they are already emitted per-run and only the receipt drops them, this
   cut is receipt-side only and much smaller.** Ground before scoping.
2. **Does anything else consume `execute_ms_spread_ratio`?** `main.rs:2481` reads it
   from the baseline json. Confirm the field's other readers before touching its
   meaning.
3. **Is `band` referenced anywhere but the alarm?** If so, the separation is wider than
   a rename.
4. **Can the harness spawn a non-`current_exe()` child at all?** `run_perf_child_args`
   (`:2228-2231`) passes the parent's `args` to a re-exec of itself, so the child must
   accept the same CLI. An override binary must be a **`toron-tpch`** binary built from
   the other tree — not a `toron` binary. Confirm the comparison tree still builds a
   CLI-compatible `toron-tpch`. **If the CLIs have diverged, paired execution is
   impossible; record it and stop.**

## Cut

**1. Retain raw samples (additive).** Emit per-run `execute_ms` with run index in the
`tpch_perf_summary` json alongside existing fields. Existing keys, values, and the
regression flags stay byte-identical.

**2. Rename to say what it is.** `band` → `regression_alarm_band` at `main.rs:2485`,
scoped to the `slower_than_baseline` check. No arithmetic change.

**3. Add the materiality estimator, exactly as the ADR predeclares.** Pure function
over paired differences `dᵢ = A_i − B_i`:

```rust
/// Hodges–Lehmann point estimate + two-sided 95% Wilcoxon signed-rank interval.
/// Deterministic: no seed, no sampling. Walsh averages are (d_i + d_j)/2 for i <= j.
pub fn paired_median_shift(diffs: &[f64]) -> Result<PairedShift, String>;
pub struct PairedShift { point: f64, lower: f64, upper: f64, pairs: usize }

/// Minimum effect size worth building. Engineering policy, NOT a noise threshold.
/// Coincides numerically with REGRESSION_THRESHOLD by accident; they are unrelated
/// and must never be unified.
const MATERIALITY_FLOOR: f64 = 0.10;
```

`n = 10` pairs. Decision is the ADR's dual criterion — `lower > 0.0 || upper < 0.0`
(detectable) **and** `point.abs() >= MATERIALITY_FLOOR * baseline_median` (material).
**Not** wired into the regression rail; it gates nothing in this cut.

**4. Add the Toron binary override — the paired design does not exist without it.**
Add a `TORON_TPCH_TORON_BIN` env override mirroring `duckdb_binary()` (`:956`),
defaulting to `current_exe()` so every existing invocation is unchanged. Grounding step
4 decides viability; if it fails, **stop and record the contingency** rather than
degrading to two unpaired sweeps.

**5. Nothing else.** No re-judgement of 443/444/446. No mechanism. No threshold change
to the regression alarm.

## Tests

**Behavior-neutrality (load-bearing):**

- `perf_summary_flags_same_plan_slowdown_beyond_noise_band` (`main.rs:2800`) stays green
  **unmodified** — proof the rename/separation did not move the alarm.
- `adr447_regression_alarm_band_is_unchanged_by_raw_retention` — same inputs, same
  `slower_than_baseline` outcome with raw samples present. Fails if the arithmetic drifted.

**Retention:**

- `adr447_perf_summary_retains_raw_per_run_samples` — assert seven raw `execute_ms`
  values with run indices 0-6 survive into the summary, and that min/median/max/spread
  remain derivable from them and match the existing fields. This is the test that makes
  the cut worth shipping.

**Estimator (unit, fixture-driven, deterministic — no seeds):**

- `adr447_estimator_detects_a_known_median_difference` — 10 pairs, planted 12% median
  difference with realistic noise; interval excludes zero **and** the HL point estimate
  lands within tolerance of 12%. **The current band calls 12% red; this asserts the
  estimator calls it detectable and material.** The discriminating test — fails under
  "reuse the regression band as the gate".
- `adr447_estimator_rejects_a_null_difference` — no planted difference; interval
  includes zero.
- `adr447_estimator_separates_detectable_from_material` — planted 2% difference with
  tiny noise: interval excludes zero (detectable) but point estimate < 10%
  (immaterial). **Pins the dual criterion**; fails if the two collapse into one.
- `adr447_estimator_is_not_range_based` — two fixtures with identical range but
  different dispersion produce different intervals. Fails if `(max-min)/median` returns.
- `adr447_estimator_is_deterministic` — same input, repeated calls, byte-identical
  output. Pins the no-seed property HL/Wilcoxon was chosen for.
- `adr447_estimator_rejects_fewer_than_six_pairs` — n < 6 cannot reach α = 0.05
  two-sided; explicit error, not a vacuous interval.

**Harness override:**

- `adr447_perf_child_honors_a_binary_override` — spawns a child from a path other than
  `current_exe()`. **Fails today**; it is the test proving the paired design is
  executable at all.
- `adr447_perf_child_defaults_to_current_exe` — override unset → unchanged behavior.

**Architecture guard:**

- `adr447_materiality_floor_is_not_the_regression_threshold` — assert
  `MATERIALITY_FLOOR` and `REGRESSION_THRESHOLD` are **distinct declarations**, that the
  materiality estimator never reads `REGRESSION_THRESHOLD`, and that the alarm band is
  referenced only from the `slower_than_baseline` path. The two constants share the
  value `0.10` and nothing else; this guard is what stops a DRY cleanup from re-merging
  the separation this ADR exists to make.

Avoid the string `monotonic` in any test or source name (contains `tonic`; trips
`adr043_no_grpc_protobuf_or_arrow_flight`, which source-scans `src` and `tests`). Use
"strictly increasing".

## Evidence to produce

- One re-run **q05 cohort on the post-446 tree** with raw per-run samples and recorded
  order. Report raw samples, median, robust dispersion, and the range statistic side by
  side, so the gap between them is on the record.
- State plainly whether the left skew (min 9.82% below median, max 2.11% above)
  reproduces post-446, and whether dispersion lands near 443's 0.0839 or 444's 0.0204
  IQR/median. **Both outcomes are results.** No mechanism claim either way.
- Do **not** re-judge 443/444/446. The receipt records the new rule as forward-only.

Clear `target/tpch/join_spill` before any sweep — post-430 the gauge spills there, not
`/tmp`; stale scratch breaks sf=10 runs.

## Seal gate (council.md, via scripts/cargo-cgroup, after fmt)

```
scripts/cargo-cgroup fmt  -p toron -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
```

The cut lands in `toron-tpch`, so those legs are load-bearing — run the FULL set after
`fmt` regardless. Record gates green only after observing them green, never from a
prior session's summary.
