# ADR 471 - filter-aware comparison bytes for the smaller-build swap

Diagnose the 469/470 regressions and fix the comparator's blindness: it compared
byte *bounds* that filters never discount, so a tightly-filtered side looked huge
and the swap built the genuinely larger side. Comparison now scales each side's
byte basis by its row-estimate discount before applying the unchanged 2× margin,
then the engaged set is re-censused, re-priced, and the default re-decided on the
new table.

## The diagnosis (pinned empirically)

- q14's single join (`lineitem ⋈ part`, tight `l_shipdate` filter on lineitem):
  flag-on swaps to build part; natural builds the *filtered* lineitem. Receipt
  probe confirms the swap fires; the facts spine confirms why — `Filter` facts
  update only `estimated_row_count` (NDV/range selectivity), never the byte fields
  (`binder.rs:1028-1070,700,734`), and the comparator prefers the hard byte bound.
  A bound is valid as a bound and wrong as a size proxy above a filter.
- The 469 table reads perfectly under this mechanism: the regressing cells (q14,
  q19) have tight filters on the swapped-away side; the winning cells (q02, q11)
  compare unfiltered relations where the bound is the size.

## Decision

1. **Comparison bytes**: for each side, if `estimated_row_count` and
   `row_upper_bound` are both present and the estimate is smaller, scale the byte
   basis by `estimate / row_upper_bound` (integer arithmetic, floor, min 1 byte);
   otherwise the basis is unchanged. The comparator (`right_build_materially_
   smaller`, same margin) runs on comparison bytes. Broadcast admission is
   untouched — bounds remain bounds where a bound is what safety needs.
2. **Regression fixture as test**: a q14-shaped binder test (filtered big left,
   unfiltered small right, flag on) pins `Natural`; the unfiltered adr464 swap
   case still swaps. The lib probe test converts to asserting q14 binds natural
   with the flag on.
3. **Re-census + re-price + re-decide**: the engagement census re-runs (the
   engaged set is expected to shrink to the genuinely-comparable cells); every
   engaged cell is re-priced under the standing 469 protocol (protocol runs, no
   new ADRs); the default flips on only if the new table has no material
   regression — that decision lands in this ADR's receipt either way.

## Acceptance

- q14 binds natural flag-on (fixture + probe); q02/q11-shaped unfiltered swaps
  unchanged; decision-table tests green.
- Census artifact regenerated; all engaged cells priced; receipt carries the new
  table and the default decision with its rule stated before the prices are read.
  Predeclared rule: **default on iff no engaged cell prices a material
  regression**; otherwise default stays off and the residual mechanism is named.
- Standing gates green; sealed verdicts stand; no cross-table ranking with the
  470-era prices (different comparator epoch).

## Guardrails

- **Scaling uses only facts already in the spine** — no new estimator; a missing
  estimate means an unscaled bound, never a guess.
- **Bound semantics preserved for admission**: only the comparison path scales.
- **The 465 armed-evidence rule applies**: censuses and prices run on parquet.

## Forward

If the default flips on, placement-aware order costing inherits a working
comparison basis. Queue otherwise unchanged.
