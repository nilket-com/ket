# ADR 447 - perf materiality gate and raw sample retention

Separate the regression alarm from the materiality gate, retain raw per-run perf
samples, and predeclare a paired materiality estimator. Rail-wide. No mechanism is
proposed, selected, or claimed.

## Context

ADR 446 banked this arc's most important number: a **94.79%** task-spill reduction
(5.35 GB → 278.8 MB) produced **no measurable wall change** — 7.75% on one sample,
inside q05's noise. Toron went 270.7x → 249.7x DuckDB. Its receipt states the
consequence: the result "does not validate a volume-proportional spill-time model and
must not be used to claim that the reservoir was the wall."

ADRs 443, 444, and 446 are three honest reds, and all three were adjudicated by one
number — the **23.86%** band. That number has four independent defects, any one of
which disqualifies it for the job it was given.

### The band is a regression alarm, not a materiality gate

`crates/toron-tpch/src/main.rs:2452-2487`:

```rust
const REGRESSION_THRESHOLD: f64 = 0.10;
let band = REGRESSION_THRESHOLD.max(2.0 * spread_ratio.max(baseline_spread));
if baseline_median > 0.0 && (execute_median - baseline_median) / baseline_median > band {
    push_summary_flag(&mut flags, "slower_than_baseline");
}
```

This is a **one-sided alarm**: it fires when the current median is *worse* than
baseline by more than the band. Its purpose is to suppress false "this got slower"
wolf-cries on noisy cells, and for that purpose it is reasonable and stays.

ADR 443 repurposed it as a gate on **improvements** (`adr/443:12`): "any future
mechanism faces a `max(0.10, 2 × max(0.1193, …))` ≥ **23.86%** band". ADR 444 lowered
its mechanism envelope to that same 0.2386 (`adr/444:96`).

**That is a category error.** A false-alarm threshold is not a detectability
threshold. Whether a 12% improvement is real is a question of statistical power —
answerable with enough samples and a paired design — and is unrelated to how
conservatively a wolf-cry suppressor is tuned. The arc's reds mean "smaller than the
alarm threshold", not "not real".

### Three further defects in the same expression

1. **Range statistic.** `spread = (max − min) / median` is set by whichever single
   sample is most extreme; no other observation can move it.
2. **Grows with n.** Expected range ≈ 2.33σ (n=5), 2.70σ (n=7), 3.7σ (n=20). Same
   noise, wider band — **more careful measurement makes every mechanism harder to
   prove**. Unfalsifiable by construction.
3. **Ratchet.** `spread_ratio.max(baseline_spread)` takes the max across the current
   *and* historical cohorts, so a tight cohort today cannot lower the band. q05's
   0.1193 permanently floors it at 23.86% until the baseline itself is re-run.

### q05's 0.1193 is one fast run, and the variance is not stationary

q05's retained row (`impl/356_...:100`, n=7 per `:47`): min 159,634.479 / median
177,026.577 / max 180,758.808 — min sits **9.82% below** the median while max is only
**2.11% above**. q05 is the most left-skewed Toron cell (+7.72); every cell except q17
(+4.56) is right-skewed or symmetric, the normal shape where interference makes runs
*slower*.

The tempting explanation is falsified: q18 spills 30.7 GB across its 7-run cell with
skew **+0.65**, while q17 spills 6.7 MB and is left-skewed at **+4.56**. Spill volume
does not predict the skew. (This is the ADR 444 drift story again; it failed there and
fails here. Do not resurrect it without a mechanism.)

Dispersion is also not stationary — two five-child q05 cohorts on the same host, days
apart: ADR 443 IQR/median **0.0839**, ADR 444 **0.0204**. A **4x** difference, robust
statistic to robust statistic. No single cohort can fix a durable band.

### The stakes, and the honest counterweight

Under the old rule, two of the arc's three reds turn on whether one run in a
2026-07-07 cohort happened to be fast. This cuts both ways and the record must say so:
a smaller band would also have licensed a producer that 446's null suggests would not
have paid off. **A wrong-but-strict gate accidentally prevented a wrong mechanism.**
The claim is not "the band was too strict" — it is that the band does not measure what
it was asked to measure, so its reds and its greens are equally uninterpretable.

### The evidence to settle it does not exist

The 356 receipt retains only **min / median / max** per cell (`:94-100`). The raw seven
samples are gone, so no robust statistic, stationarity test, or run-order analysis can
be run against existing evidence. **Retention is the actual gap.**

## Decision — three separations, rail-wide

**1. The regression alarm keeps its job and loses the other one.** The `band` at
`main.rs:2485` is unchanged arithmetically and continues to drive
`slower_than_baseline`. It is renamed to say what it is. This ADR records that it is
**not** a materiality gate and must not be cited as one; ADR 443's `:12` framing and
444's 0.2386 envelope are **superseded, not retroactively re-judged**.

**2. Raw per-run samples and run order are retained.** The perf receipt gains raw
`execute_ms` per run index alongside the existing min/median/max/spread. Additive;
existing fields and the regression rail are byte-identical.

**3. Materiality gets its own statistic, fully predeclared.**

*Design.* Paired, systematically interleaved: children run `A,B,A,B,…` in strict
alternation from a single sweep, run index recorded with every sample. Systematic
rather than randomized so the schedule reproduces from the receipt alone; pairing is by
adjacent index (`2k` with `2k+1`), which is what cancels common-mode drift within a
pair.

*Sample count.* **n = 10 pairs (20 measured children)** plus one discarded warmup,
matching the rail's convention (`impl/356_...:47`). n ≥ 6 is the floor at which a
two-sided Wilcoxon signed-rank test can reach α = 0.05 at all (min attainable
p = 2/2⁶ = 0.031); 10 buys margin against q05's observed IQR/median of 0.0204–0.0839.
At ~177 s/run this is ~1 hour per cell.

*Estimator.* **Hodges–Lehmann** on paired differences `dᵢ = A_i − B_i`: point estimate
`median{ (dᵢ + dⱼ)/2 : i ≤ j }` over all Walsh averages. Distribution-free, robust to
the fast-outlier shape q05 exhibits, paired, and **fully deterministic** — no seed,
unlike a bootstrap. Reproducibility from the receipt is the deciding property.

*Confidence procedure.* Two-sided **95% Wilcoxon signed-rank interval** on the same
Walsh averages. Deterministic, exact at n = 10, paired with the point estimate by
construction.

*Decision threshold — two criteria, deliberately separate.* This is the split the band
conflated:

1. **Detectable** — the 95% interval excludes zero. A statistics question.
2. **Material** — the HL point estimate is **≥ 10%** of the baseline median. An
   engineering-policy question, predeclared, independent of noise, and set by the
   operator per the project's longstanding minimum effect-size convention.

A candidate must clear **both**. Detectable-but-immaterial is a real finding (a genuine
but too-small effect); material-but-undetectable means take more pairs. Neither is a
red for the mechanism itself.

*Implementation contingency.* The Toron perf child re-execs
`std::env::current_exe()` (`main.rs:2229`, `:2300`), so the parent's identity fixes
every child's binary; `TORON_TPCH_DUCKDB_BIN` exists (`:956`) but there is **no Toron
equivalent**. A/B interleaving therefore requires a binary override mirroring that
precedent. **This cut adds it.** If it proves unavailable — including if the two trees'
`toron-tpch` CLIs have diverged, since the child receives the parent's args — the
paired design is impossible and this ADR records that rather than degrading to two
unpaired sweeps, which reintroduce the cross-cohort drift pairing exists to cancel.

**Rejected — re-judge the arc's reds.** 443/444/446 were decided honestly under a
predeclared rule. A better ruler does not retroactively make `write_ipc_encode` a
mechanism. The reds stand; the forward rule changes.

**Rejected — a q05-scoped fix.** `main.rs:2485` gates all 22 cells and the ADR 356 rail
that publishes them; q02 (0.1799) and DuckDB q11 (0.3333) imply 36% and 67% bands. The
rail is the honest unit.

## Acceptance

- Raw per-run `execute_ms` and run index retained for every cell; existing
  min/median/max/spread/flags byte-identical.
- The `slower_than_baseline` rail behaves **identically** — pinned by a test proving
  the rename and separation are behavior-neutral.
- The estimator is implemented exactly as predeclared (HL point estimate + 95% Wilcoxon
  interval, n = 10, systematic `A,B,A,B,…`, dual threshold) and unit-tested against
  fixtures with a planted median difference — including a case the current band calls
  red and the estimator calls detectable, and a case both reject.
- A test proves a perf child can spawn from a path other than `current_exe()`. **Without
  it there is no paired design**; if it cannot be added, the receipt records the
  contingency and substitutes nothing.
- One re-run q05 cohort on the **post-446** tree with retained raw samples and recorded
  order, reported as characterization — **not** a re-judgement of 443/444.
- No mechanism is proposed, selected, or claimed.

## Guardrails and risks

- **The two 0.10s are not the same number.** `REGRESSION_THRESHOLD = 0.10` (alarm
  false-positive floor) and the materiality floor (10% of baseline median) coincide
  numerically and mean different things. They must be **distinct named constants**, and
  a guard must assert the materiality estimator never reads `REGRESSION_THRESHOLD`. A
  future DRY cleanup collapsing them would silently re-merge exactly what this ADR
  separates.
- **Do not break the regression rail.** It is the only thing protecting the board from
  silent slowdowns. Its behavior is pinned by test, not inspection.
- **Do not re-open sealed reds.** 443/444/446 stand. Nothing here is retroactive.
- **This is measurement about measurement.** The council has spent 443, 444, and 446 on
  instruments — three cuts, three reds, no throughput. That is the strongest argument
  against this cut and it belongs in the record. The counter is 446: a mechanism aimed
  by this ruler predicted nothing, so a fourth mechanism against the current band is the
  more expensive mistake.
- **A better estimator does not make q05 fast.** Toron is 249.7x DuckDB with the spill
  essentially gone. This cut makes the next question answerable; it answers nothing.
- **Non-stationarity may defeat any threshold.** If dispersion genuinely varies 4x
  between cohorts, the honest end state is a paired design with no historical constant
  at all. This cut must not pretend a single new number is durable.

## Forward arc

**q05's wall regime** — with raw samples retained, the fast-mode question becomes
answerable and the 4x cohort-to-cohort dispersion swing becomes diagnosable.
Bimodality is a diagnostic **outcome**, never a presumed mechanism; nothing here
presumes q05 has a fast mode, and "it does not" is equally a result. This would finally
define a term named in two forward arcs (`adr/443:132-133`, `adr/444:163`) with no
defining paragraph in the corpus.

**Chain reorientation** is **demoted, not deferred** — justified by DuckDB's zero-spill
206 MB profile, but 446 removed 94.79% of the spill and moved nothing.

**Whether any cell has a usable materiality gate today** is worth knowing once the
estimator exists: q02 (0.1799) and DuckDB q11 (0.3333) imply 36% and 67% bands.

## Deferred (named)

- Query-budget realism — `memory_per_task: 64 MiB` reaching production via
  `impl Default for QueryBudget { fn default() -> Self { Self::test_default() } }`
  (`distributed/budget.rs:95,107-110`).
- Chain reorientation (demoted).
- `MAX_FILTER_BYTES` cap raise (1 MiB → ~2.7 MiB).
- A size-based build-side choice for spill-eligible joins — ADR 132's comparator never
  runs for non-broadcast pairs (`adr/132:3-6`).
- q17 remeasurement — itself left-skewed (+4.56), the same anomaly.
- q18 strict one-pass; q21 residual; q19 re-diagnosis + 438-profiler widening; q13 fresh
  diagnosis; Cut A (439 mirror); generic O(1) LRU rewrite.
