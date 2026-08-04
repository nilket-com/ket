# ADR 472 implementation receipt

The unknown-size guard landed, the engaged set collapsed to exactly the winning
cells, the predeclared rule fired, and the **default is on**. A sentinel also
caught a second, unrelated consequence of ADR 471 — a plan-epoch at q05 sf=8 from
the new footer statistics — which is recorded here rather than left to surface
later.

## The guard

- `PlanOptimizerFacts::filtered_without_estimate`, set in the binder's `Filter` arm
  via the shared helper `mark_filtered_without_estimate()` (the ADR 137 design
  guard caught an inline literal on the first attempt; routed properly rather than
  weakening the guard), preserved through project, false on every other
  constructor.
- `hash_join_decision_with_placement` declines when either side carries it —
  declining is the pre-464 placement, never a correctness risk.
- Pinned by `adr472_unknown_size_side_declines_the_swap` (same shape swaps without
  the marker — the discriminating half) and the real-path
  `adr472_q19_binds_natural_with_flag_on`.

## Census and prices (post-guard epoch)

Engaged set at sf=1 collapsed from six cells to **three — exactly the material
winners**; q14, q17, q19, q20 now decline. Digest equality at all 44 census
children and all three gates.

| cell (sf=1) | HL OFF−ON | 95% interval | rel. | verdict |
|---|---:|---:|---:|---|
| q02 | +111.7 ms | [93.4, 123.5] | +17.7% | material win |
| q10 | +260.4 ms | [255.7, 263.7] | +34.6% | material win |
| q11 | +113.7 ms | [96.2, 131.4] | +23.1% | material win |

**Rule applied** ("default on iff no engaged cell prices a material regression"):
no regression, three material wins → **default on**, operator off-switch retained.

## The q05 sf=8 plan epoch (sentinel finding, ADR 471's doing, not the swap's)

The sentinel showed q05 sf=8's fingerprint moving `757d7c62…` → `bedd2eeb…`. The
plan-distinctness gate then **refused** a swap cohort there because both arms
produce `bedd2eeb…`: the swap does not engage at that cell. The mover is ADR 471's
parquet-footer column ranges — real selectivity statistics changing the chosen
plan, flag-independent.

- Digest unchanged (correctness intact); ON-arm execute ~21.6–21.7 s with spill
  174,962,832 bytes, versus the pre-471 epoch's ~1.69 GB spill at this cell.
- **Epoch recorded**: q05 sf=8 figures from ADR 451/456/457/458/461 belong to the
  pre-statistics plan and are not comparable to post-471 measurements. No
  cross-epoch ranking is made here; the numbers above are new-epoch context only.
- That the statistics work moved the flagship cell's plan is a finding in its own
  right and the natural next measurement (a fresh paired filter-ablation cohort at
  the new plan would re-price the 446 bridge in this epoch).

## Gates

Binder 635/0; architecture 2,093/0 (adr137 satisfied by the shared helper; the
adr176 binder-root bound bumped 6,050 → 6,070 with ADR 472 attribution per the
guard's own rule); toron-tpch adr469 knob test green with the new default;
**armed 22-query parquet correctness sweep 22/22 with the default on**; full engine
suite green.
