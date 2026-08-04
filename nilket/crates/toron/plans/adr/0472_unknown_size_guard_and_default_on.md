# ADR 472 - unknown-size guard: don't swap what you can't size, then default on

Close ADR 471's residual with the conservative guard rather than a selectivity
chase: a side carrying an unestimated filter has **unknown** size, and the
smaller-build comparator must decline rather than compare a stale bound against it.
Then re-price the engaged set and apply 471's predeclared rule to the default.

## The fork, decided

- 471 left two candidate closures. **Selectivity coverage** (decimal range tag
  end-to-end, string equality NDV) is unbounded in scope — each new predicate
  family is another estimator, and q19 mixes Decimal128 range with string
  categoricals — and it converts a *safety* problem into an *accuracy* project.
- **The guard** is correctness-shaped and finite: it does not need to know how big
  the filtered side is, only that it does not know. Where the estimate exists (471's
  scaled comparison), behavior is unchanged; where it doesn't, the comparator
  declines — which is exactly today's default-off behavior for that join, not a new
  risk. Coverage remains available later as pure upside: every filter that gains an
  estimate re-enables the swap on its own merits.

## Decision

1. **Facts signal**: `PlanOptimizerFacts` gains `filtered_without_estimate: bool`,
   set in the binder's `Filter` arm exactly where a filter produces no
   `estimated_row_count` (`binder.rs:1129`), cleared when an estimate exists, and
   preserved through volume-non-increasing/project transforms like other facts.
   Joins produce composite facts and do not inherit it.
2. **Guard**: `hash_join_decision_with_placement` declines the swap when *either*
   side is `filtered_without_estimate` — no comparison, natural placement, the
   fallback reason unchanged. Where both sides are sizable, 471's scaled comparison
   is untouched.
3. **Re-price and decide**: census + price the engaged set (protocol runs, no new
   ADRs), then apply 471's rule verbatim — **default on iff no engaged cell prices
   a material regression** — and record the decision in this ADR's receipt either
   way.

## Acceptance

- A binder test pins the guard: filtered-without-estimate side + otherwise
  swap-worthy sizes → `Natural` (fails without the guard); 471's estimated-filter
  case still scales and still keeps natural; 464's unfiltered case still swaps.
- q19 binds natural with the flag on (real-path probe, the residual fixture).
- Census regenerated; all engaged cells priced; the receipt states the rule before
  the table and the decision after.
- Standing gates green; if the default flips on, q05/q17 sentinels and the armed
  22-query parquet sweep must be green in the same receipt.

## Guardrails

- **Declining is never a correctness risk** — it is the pre-464 placement.
- **No new estimators in this cut**; coverage is deliberately deferred, and its
  future arrival strictly widens where the swap may fire.
- **The 465 armed-evidence rule applies** to census and prices.

## Forward

Selectivity coverage (decimal tag, string NDV) becomes an optimization cut whose
value is now measurable: each covered predicate family moves cells from
"declined-unknown" into the priced set. Placement-aware order costing remains
queued behind it.
