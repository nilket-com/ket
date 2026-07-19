# ADR 464 implementation receipt

The smaller-build comparator now runs for auto inner hash joins: every hash fallback
in `choose_auto_join_strategy` flows through `hash_join_decision_with_placement`,
which swaps to `SwappedRightBuild` exactly when both sides carry byte facts and the
right clears ADR 132's dominance margin. Everything else — pinned strategies,
non-inner kinds, missing facts, inside-margin pairs — is byte-identical to before.

## Shape

- `unthresholded_build_byte_candidate`: the raw byte facts (hard preferred over
  estimated; Composite estimated-only; Unknown none) — the broadcast threshold stays
  a broadcast admission concern.
- Comparator and margin reused verbatim (`right_build_materially_smaller`,
  `AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR = 2`); no new rule, no new constant.
- New decision reason `HashSmallerBuild { build_bytes, rejected_side_bytes }`,
  rendered by explain (`hash_smaller_build`, build-side "right", both byte columns)
  — receipts say why placement changed.
- The swap machinery is untouched: the existing operand swap + restore-`Project`
  consumes the placement, strategy-agnostic as 130 built it.

## Decision table, each row pinned by a test that fails under the wrong branch

| case | outcome | test |
|---|---|---|
| both facts, right materially smaller | `SwappedRightBuild` + `HashSmallerBuild` | `adr464_hash_fallback_builds_materially_smaller_right_side` |
| both facts, inside margin (1.5×) | `Natural`, reason unchanged | `adr464_hash_fallback_keeps_left_build_inside_dominance_margin` |
| one side missing facts | `Natural` | `adr464_hash_fallback_without_both_facts_keeps_natural_placement` |

## Gates and board evidence

- Full architecture_test **2,093/0** (the adr132 single-helper/hysteresis guards
  pass unchanged — the comparator remains one helper, consulted from one more
  place); full engine suite green; workspace `check --all-targets` clean.
- Live correctness sweep, all 22 queries, strict memory source: **22/22 pass** —
  the swap is output-invariant everywhere it engages.
- q05 sf=8 gate-zero recheck on the rebuilt release binary: plan fingerprint
  **unchanged** (`757d7c625669b21a`), digests identical, reservoir identities
  identical (ON 7,692,229 / OFF 47,989,007 — exactly 461's numbers). The standing
  fixtures did not flip placement, so no measurement epoch lands on them; the
  fingerprint-drift note the plan reserved is not needed.
- No wall/materiality claims. Pricing a flipped placement with the paired
  instrument remains available future work at a cell where the swap engages.
