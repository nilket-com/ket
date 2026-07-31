# ADR 467 implementation receipt

The design decision is made and landed; activation surfaced a third blocker and is
deliberately withheld. Production behavior remains byte-identical to post-466.

## The decision, implemented

Hash-tier uniformity is now **strategy uniformity**: both uniform recognizers
(`uniform_star_order_strategy`, `uniform_snowflake_order_strategy`) accept any hash
placement, and both fingerprint validations (`order_strategy_fingerprint`,
`preserved_outer_order_strategy_fingerprint`) record `(HashPartitioned,
SwappedRightBuild)` as valid per-dimension information — ADR 192's broadcast
relaxation, applied to the hash tier, with the rejected alternative
(placement-after-selection) ruled out because it would mutate the winner past ADR
193's selected-preflight validity gate.

Pinned by `adr467_hash_tier_uniformity_is_strategy_uniform_with_placement_in_fingerprint`:
mixed hash placements form one uniform tier (fails under the pre-467 strict
recognizer), and unauthenticated `Broadcast + Natural` still declines — 192's
authentication rule untouched. The adr181 recognizer guard updated per its own
mechanism to pin the new shape (including the absence of the old placement match).

## The third blocker, named and evidenced

Flipping the default exposed shape regressions in **star/snowflake extraction**:
the swap-restore `Project` wrapping a swapped hash join is opaque to the extractor
— a 3-dimension star lost a dimension (`uniform_hash_star_reorders_on_robust_win`:
declared order `[c,p]` with `s` gone) and snowflake actuation receipts collapsed to
zero. These are recognition regressions, not stale pins, and extraction surgery is
its own cut. The default therefore stays **off**; the flag's doc names the
extractor gap as the activation gate.

## Gates

- Binder suite 633/0 (all join-order behavior unchanged under the off default; the
  new recognizer pin green); full architecture_test 2,093/0; q17 parquet sentinel
  green; full engine suite green.
- No sealed verdict re-judged; no epoch created (no production placement changed).

## What remains

The activation chain is now fully explicit: **star extractor sees through the
swap-restore Project** → flip the default under Rail 3's armed evidence → then
placement-aware order costing as capability matures. Each step has its fixtures
waiting.
