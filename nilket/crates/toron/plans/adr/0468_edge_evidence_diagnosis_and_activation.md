# ADR 468 - the "lost dimension" diagnosis, the stability gate fix, and activation

Diagnose why a star dimension's edge failed to enter the order evidence under the
armed flag, fix the proven cause, and activate the smaller-build placement under ADR
467's Rail-3 evidence. The diagnosis falsified both prior framings and landed on a
third: the ADR 193 cross-order stability gate compares placement-inclusive
fingerprints, and placement is order-dependent by 467's own contract.

## The mechanism (pinned by the fixture before the fix)

- Falsified: star extraction (logical, flag-independent) and evidence assembly
  (declines whole stars loudly; cannot drop one dimension).
- Proven: `bind_local_star_actuation`'s `strategy_authorized` requires
  `fingerprint_match` — equality of `(strategy, placement)` fingerprints across the
  declared and selected orders. Smaller-build placement varies with the order (which
  side is smaller depends on what has accumulated), so the top-level 3-dimension
  star declined, recursion descended, and an inner 2-dimension sub-star actuated:
  the "lost dimension" of the 467 receipt. The code's own ADR 193 comment already
  disclaims the fingerprint as an authorization gate; for uniform summaries it
  still gated.

## Decision — three rails

**Rail 1 — strategy-component stability.** Cross-order stability compares the
strategy component of the fingerprint only; placement is per-order information the
fingerprint records (467's contract, applied to the gate that still contradicted
it). Proven by the fixture: `uniform_hash_star_reorders_on_robust_win` actuates all
three dimensions with the flag on, and the 12-test flag-on surface collapses to
three legitimate re-pins.

**Rail 2 — receipts keep their causes.** `HashSmallerBuild` gains a `fallback:
HashByDefaultReason` field: swapping the build must not erase why the join hashed
(the carried-input-cause test's intent, preserved and strengthened; explain renders
the fallback as the reason detail). The three re-pins assert the new contract,
including the carried cause riding the swap receipt.

**Rail 3 — activation.** `hash_smaller_build_placement` defaults on (operator
off-switch retained), evidenced per 467 Rail 3 on armed sources: q17 strict parquet
(the 465 fixture), the full 22-query parquet correctness sweep, the q05 sf=8
gate-zero sentinel (identity must hold; a plan-fingerprint change is recorded as a
measurement epoch), binder + architecture + full engine suites.

## Acceptance

- The fixture actuates 3 dimensions flag-on; binder suite green flag-on; the
  strategy-component comparison and the fallback field pinned by tests.
- All Rail 3 evidence green; any q05 fingerprint drift recorded as an epoch note,
  never ranked across; sealed verdicts stand.
- Guards evolve only by their own mechanisms.

## Guardrails and risks

- **Placement remains unpriced by order costing** (467's named follow-on): orders
  are chosen blind to placement effects; the soft-margin machinery bounds the harm.
- **The 466 pin stays load-bearing** (placement/rewrite contract).
- **Do not re-judge sealed verdicts.**

## Forward arc

Placement-aware order costing; pricing an engaged swap cell with the paired
instrument. Behind them: q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A
(439 mirror), the O(1) LRU rewrite, and the budget-derivation gauge remain queued.
