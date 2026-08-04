# ADR 461 implementation receipt

The cap raise landed, every gate is green, and the paired verdict at the (64 MiB,
2 GiB, sf=8) cell is the arc's strongest: **material, HL OFF−ON 4,822 ms, 95%
interval [4,647, 4,988], 18.9% of the OFF median** — 1.89× the floor. Gate zero
missed 460's ~3.9M projection in the predeclared way, and the miss is itself the
finding: the 446 FPR figure did not transfer, and the two measurements now bracket
the cell's true-match count.

## Rail 1 — the raise, guarded

- `MAX_FILTER_BYTES` 1 MiB → 4 MiB with the sizing rationale in place; smaller
  filters allocate identically (proven), the 16 KiB minimum untouched.
- `adr461_bloom_cap_covers_measured_key_counts_without_small_filter_drift` pins ≥9
  bits/key at 2.63M keys (fails under the old cap) and sub-threshold byte-identity.
- Gates: runtime_filter suite 23/23, architecture_test 2,091/0, full engine suite
  green, q05 sf=8 oracle correctness **pass** (34.1 s, explicit coordinates).

## Rail 2 — gate zero vs the projection

- Identities pass: OFF 47,989,007 == the scale-parametric oracle; ON strictly less.
- **Observed ON reservoir: 7,692,229 rows** (from 11,742,021 pre-raise — a 34.5%
  reduction). The 460 projection was ≈3.9M, built on the assumption that 446's
  ~17.3% FPR transferred to this cell. It did not; per the predeclaration this is a
  result about FPR transfer, not a defect.
- What the two measurements jointly say (T = true matches, unknown; 47,989,007
  probed rows): pre-raise 11,742,021 = T + f₁·48.0M; post-raise 7,692,229 = T +
  f₂·48.0M. If the raised cap reached its ~1% design FPR (the filter's ~1.8–2.2 MiB
  optimum now fits), then **T ≈ 7.21M** and the pre-raise FPR was ≈9.4% — half of
  446's cell-specific figure, which measured a different scale, budget, and era.
  The alternative (T ≈ 3.44M and post-raise FPR ≈ 8.9%) would require the raised
  filter to miss its design point by 9×; the sizing test makes that implausible but
  the receipt records both readings — separating them would take a T oracle (an
  exact-set run), noted as available future work, not assumed.
- Page shape holds: ON 946 pages / 7.69M rows (~8.1k rows/page), 108 MB — pages
  track rows, byte density at parity.

## Rail 3 — the paired verdict (mandatory order, within-cell)

- 21/21 journal rows, zero `BudgetExhausted`, explicit coordinates locked in the
  manifest; fresh directory (the cap is an ON-arm epoch; no cross-epoch ranking).
- 10/10 pairs positive; **HL 4,822 ms [4,647, 4,988]** — detectable.
- OFF median 25,563 ms; floor 2,556 ms; point = 1.89× floor: **material**.
- The pooled `unstable` flag is the standing two-distribution artifact; the paired
  estimator is the instrument.

## What this seals

At design-FPR filter sizing, the bridge's verdict at the measured cell strengthens
(17.7% → 18.9% of a faster OFF baseline, the tightest interval of the arc). The
bridge's three-act story — 446 mechanism, 458 page format, 461 FPR — closes with the
mechanism material at every post-format measurement, and the FPR-transfer lesson is
recorded: FPR is cell-specific; projections across cells carry it as an assumption,
never a constant.
