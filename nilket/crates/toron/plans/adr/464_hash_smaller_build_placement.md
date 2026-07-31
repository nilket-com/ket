# ADR 464 - smaller-build placement for spill-eligible hash joins

Extend ADR 132's smaller-build dominance from broadcast-eligible pairs to the hash
fallback: an auto-strategy **inner** hash join whose sides both carry byte facts
builds the materially smaller side via the existing `SwappedRightBuild` swap+restore,
instead of always-left. Build size drives memory and spill for hash joins exactly as
it drove broadcast size — the comparator was simply never consulted there.

## Context

- 132 sealed the comparator (`right_build_materially_smaller`, the
  `AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR` margin) for the case where both sides are
  *broadcast-viable*; every hash decision leaves `hash_join_decision(reason)` with
  `placement: Natural` (`binder/join.rs:1095`), so a hash join can build — and at
  capacity, spill — the provably bigger side. The queue has carried this since 132.
- The machinery is already strategy-agnostic where it matters: `SwappedRightBuild`
  is consumed by the binder's operand swap and restore-`Project` (`binder.rs:3092,
  3199`), proven for inner joins by the broadcast path since 130.
- Byte facts (`byte_upper_bound` hard, `estimated_byte_size` estimated) live on
  `PlanOptimizerFacts` un-thresholded; `build_byte_candidate` filters them through
  the broadcast threshold, so the hash comparator needs a sibling accessor with no
  threshold, not a formula change.

## Decision

1. **Scope: `JoinStrategyIntent::Auto`, `JoinKind::Inner`, hash fallback only.**
   Pinned strategies, crosses, and non-inner joins are untouched (swap legality for
   non-inner is exactly the asymmetry the broadcast rule already refuses via
   `BroadcastPlacementRule::Never`; the hash extension inherits that boundary).
2. **Comparator reuse, not a new rule**: un-thresholded byte candidates for both
   sides; swap only when `right_build_materially_smaller` under the same dominance
   margin; missing facts on either side → today's `Natural` (no guessing).
3. **A new decision reason** (`HashSmallerBuild { build_side, build_bytes,
   rejected_side_bytes, source }`-shaped, matching the broadcast reasons) so
   explain/cockpit receipts say why placement changed; exhaustive matches updated
   where the compiler demands.
4. **Discriminating tests**: a binder test with right facts materially smaller must
   produce `(HashPartitioned, SwappedRightBuild)` — fails under always-left; a
   mirror test at margin-not-cleared and facts-missing pinning `Natural`; explain
   surface test for the new reason.
5. **Board evidence**: full correctness matrix green (the swap must be
   output-invariant); the q05 sf=8 committed cell re-checked for identity (gate-zero
   row counts/digests) — a plan-fingerprint change is expected wherever placements
   flip and is recorded as an epoch note, never ranked across.

## Acceptance

- Decision table: dominance-cleared → swap; margin-not-cleared / missing-facts /
  pinned / non-inner → unchanged. Each row pinned by a test that fails under the
  wrong branch.
- Full architecture_test and engine suite green; correctness matrix green; explain
  renders the new reason; adr176-family allowlists via their mechanism if tripped.
- q05 sf=8 gate-zero identities re-verified post-change; fingerprint drift recorded.
- No wall/materiality claims — placement changes create measurement epochs; pricing
  any specific swap is future paired-instrument work if wanted.

## Guardrails and risks

- **Estimates can be wrong.** The dominance margin (not bare `<`) is the standing
  protection; a mis-swap is a performance risk, never a correctness one (the
  restore-project preserves output shape, proven by the matrix).
- **JoinKind guard surfaces**: decision-reason variants ripple into exhaustive
  matches and source-scanning guards; run the FULL architecture_test, not
  `--all-targets` alone.
- **Fingerprint churn is real**: any baseline comparing pre/post plans flags
  `plan_changed` — correct behavior, noted in the receipt.
- **Do not re-judge sealed verdicts.**

## Forward arc

Pricing a flipped placement with the paired instrument (a cell where the swap
engages) is available future work. Behind it: q17 remeasurement, q21 residual, q19
re-diagnosis + 438 widening, q13, Cut A (439 mirror), the O(1) LRU rewrite, and the
budget-derivation gauge remain queued.
