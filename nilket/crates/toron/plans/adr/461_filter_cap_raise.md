# ADR 461 - raise MAX_FILTER_BYTES: de-clamp the runtime filter at measured scales

Raise the runtime-filter Bloom cap from 1 MiB to 4 MiB so the filter reaches its
designed ~1% FPR at the arc's measured key counts, then measure it: gate zero at the
(64 MiB, 2 GiB, sf=8) cell tests ADR 460's ~3.9M-row projection, and the paired
cohort re-verdicts the bridge on the smaller reservoir. The successor cut 460
promoted, executed under the standing apparatus.

## Context — the clamp is the FPR

- The filter sizes at 9.585 bits/key (~1% FPR target, `TARGET_FP_RATE_BP`) but clamps
  the payload to `MAX_FILTER_BYTES = 1 MiB` (`runtime_filter.rs:22,432`). Above
  ~874k distinct keys (`cap_distinct_threshold`), bits/key fall and FPR rises —
  q05's base side carries ~1.82M keys at sf=8 (wants ~2.18 MiB) and ~2.63M at sf=10
  (wants ~3.15 MiB), squarely in the clamped region; 446 measured the consequence as
  ~17.3% FPR at its cell.
- 460's ledger-derived projection at sf=8: ON reservoir 11,742,021 rows ≈ 3.44M true
  matches + ~8.3M false positives; at ~1% FPR the reservoir drops to ≈ 3.9M rows.
- 4 MiB (not 446's literal ~2.7 MiB) because the cap should clear the measured range
  with headroom, stay a power of two, and keep one clamp semantics; the sizing
  formula still allocates only the optimal bytes below the cap, so smaller filters
  are unchanged.

## Decision — three rails

**Rail 1 — the raise, guarded.** `MAX_FILTER_BYTES` 1 MiB → 4 MiB. A discriminating
unit test pins that a key count in the previously-clamped band (e.g. 2.63M) now
receives ≥ 9 bits/key (fails under the 1 MiB cap), and that sub-threshold sizing is
byte-identical to before (no drift for small filters). Filter payload memory remains
bounded by the cap; no accounting surface changes.

**Rail 2 — gate zero measures the projection.** At the (64 MiB, 2 GiB, sf=8) cell:
OFF must equal the scale-parametric oracle unchanged; ON must be strictly less, and
the receipt records the observed ON count against the predeclared ≈3.9M estimate.
The estimate missing is a result about FPR transfer, not a defect — unless ON fails
identity checks entirely, which seals as a defect report.

**Rail 3 — paired re-verdict at the cell.** Full standing protocol (release, single
detached parent, durable journal, fingerprint- and budget-locked manifest, 20 runs /
1 warmup, `BudgetExhausted` halts, 10%-of-OFF-median floor, mandatory receipt
order), fresh output directory — the cap change is a format-adjacent epoch for the
ON arm, so pre-461 cohorts are cited, never ranked against.

Predeclared outcomes: ON reservoir shrinks toward the projection and the verdict
(material or not) seals as the bridge's standing at ~design FPR; ON reservoir does
not shrink → the FPR model mis-transfers, sealed as a diagnosis input; gate-zero
identity failure → defect report, no cohort.

## Acceptance

- Constant raised; discriminating sizing test green (and proven to fail under the
  old cap); full architecture_test and engine suite green (engine cut).
- q05 correctness green at a committing cell post-change.
- Gate zero receipt with observed-vs-projected ON count; cohort receipt in mandatory
  order with explicit coordinates; 21/21 journal; no `BudgetExhausted`.
- No sealed verdict re-judged; no cross-epoch ranking.

## Guardrails and risks

- **Board-wide but bounded**: every filter above ~874k keys grows (≤4 MiB); FPR
  falls everywhere the clamp bound. The engine suite is the containment for
  unrelated consumers.
- **Bigger filters cost build/probe cycles**: the paired cohort prices exactly this
  trade at the measured cell — that is its job.
- **The projection is an estimate** (446 FPR figures transferring); gate zero is the
  measurement.
- **Do not re-judge sealed verdicts.** 443–460 stand.

## Forward arc

Verdict lands → the bridge's story at design FPR is complete, and the article
narrative (446 mechanism → 458 format → 461 FPR) has its third act. Behind it: the
engine-side `Default` retarget, ADR 132's comparator, q17 remeasurement, q21
residual, q19 re-diagnosis + 438 widening, q13, Cut A (439 mirror), and the generic
O(1) LRU rewrite remain queued.
