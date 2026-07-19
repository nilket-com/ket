# ADR 467 - join-order × placement: hash-tier placement joins the fingerprint

Decide how order search reasons about order-dependent placements, and turn ADR 466's
default-off capability on. The decision: the hash tier's uniformity contract becomes
**strategy-uniformity with placement carried per-dimension in the ADR 193
fingerprint** — the exact relaxation ADR 192 already made for the broadcast tier.
Placement-after-selection is rejected because it would mutate the winner past the
selected-preflight validity gate that ADR 193 built.

## The design decision, grounded

- Today's hash tier is placement-strict: `uniform_star_order_strategy` and the 193
  fingerprint validation accept only `(HashPartitioned, Natural)`; a hash edge with
  `SwappedRightBuild` is an *invalid combination* and declines the order. This is
  ADR-181-era conservatism from when hash had exactly one placement — ADR 466 gave
  it a second, correctly plumbed one.
- **Precedent**: ADR 192 relaxed the broadcast tier to accept mixed placement at
  authenticated positions (composite builds Natural, leaf scans Swapped). The
  `StrategyFingerprint` already records `(strategy, placement)` per dimension, so
  the per-order reasoning surface exists; the hash tier simply never used it.
- **Rejected alternative** (placement after selection): running the comparator only
  on the durable winner keeps preflights uniform but makes the executed plan
  diverge from the fingerprint that authorized it — the exact post-hoc mutation the
  selected-preflight validity gate forbids. Not an option while that gate is the
  contract.
- Under the new contract, per-order placement variance is not a violation — it is
  information the fingerprint carries, per order, exactly as it does for broadcast
  snowflakes.

## Decision — three rails

**Rail 1 — recognizer and fingerprint extension.** The hash arms of
`uniform_star_order_strategy` and `uniform_snowflake_order_strategy` accept any
placement (strategy-uniform); `order_strategy_fingerprint`'s validation accepts
`(HashPartitioned, SwappedRightBuild)` as valid, recording it per dimension.
Receipts stay honest — the placement each edge resolved to is already what the
fingerprint publishes.

**Rail 2 — the 15 tests re-pinned to the new contract.** Each failing test is
updated to assert the *new* invariant, not merely silenced: strategy uniformity
where it asserted placement uniformity, and — where the comparator now engages —
the expected `SwappedRightBuild` with its `HashSmallerBuild` receipt. At least one
test pins the new fingerprint content for a hash order with mixed placements
(would fail under the old strict validation), and one pins that an
unauthenticated `Broadcast + Natural` still declines (192's rule is untouched).

**Rails 2/3 — executed deviation, discovered mid-cut.** Flipping the default
surfaced a third subsystem contract beyond the recognizers: **star/snowflake
extraction does not see through the swap-restore `Project`** that wraps a swapped
hash join. Observed as real shape regressions, not stale pins — a 3-dimension star
losing a dimension (`uniform_hash_star_reorders_on_robust_win`: declared order
`[c,p]`, `s` gone) and snowflake receipt counts collapsing to zero. Extraction
surgery is its own cut; this one does not smuggle it. The default therefore stays
**off**, with the extractor gap named as the activation gate and the failing
fixtures recorded. Rail 1's relaxation lands (inert until an emitter runs, correct
under the decided contract) and is pinned by a direct recognizer unit test — mixed
hash placements are one uniform tier; unauthenticated `Broadcast + Natural` still
declines.

**Rail 3 (original, now contingent) — flag default flips on, evidenced armed.** `hash_smaller_build_placement`
defaults to true; the 466 flag stays as an operator off-switch. Evidence per the
465 rule: q17 strict parquet (the falsifying fixture), the full 22-query parquet
sweep, the q05 sf=8 sentinel (fingerprint drift recorded as an epoch note if q05's
placements flip), binder + join-order suites, full architecture_test and engine
suite.

## Acceptance

- Hash orders with size-skewed dimensions actuate instead of declining; the
  fingerprint records their per-dimension placements; receipts name
  `hash_smaller_build` where it fired.
- 192's broadcast authentication rule byte-unchanged; flat-star broadcast
  strictness unchanged.
- All Rail 3 evidence green. If q05's plan fingerprint moves, the epoch is recorded
  and no cross-epoch ranking is made; sealed verdicts stand.
- The adr176/adr138-family guards evolve only by their own mechanisms.

## Guardrails and risks

- **Cost model neutrality**: order selection's cost comparison does not yet price
  placement; a swapped build changes real cost. This cut carries placement as
  information, not as a cost input — pricing it is named follow-on, and the
  soft-margin machinery bounds the harm of order choices made blind to it.
- **The 466 pin stays load-bearing**: placement and rewrite remain one contract;
  nothing here touches the rewrite.
- **Do not re-judge sealed verdicts.**

## Forward arc

Placement-aware order costing is the named successor once a gauge can price a
swap. Behind it: q21 residual, q19 re-diagnosis + 438 widening, q13, Cut A (439
mirror), the O(1) LRU rewrite, the budget-derivation gauge, and pricing an engaged
swap cell remain queued.
