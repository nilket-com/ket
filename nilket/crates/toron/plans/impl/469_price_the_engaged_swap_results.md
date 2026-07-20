# ADR 469 implementation receipt

Census first, cohort second, and the swap's first admissible price is a **detectable
small regression**: at the (q17, sf=1, 64 MiB, 2 GiB) cell the smaller-build swap
costs 2.4% of the OFF median — immaterial under the sealed 10% rule, but the
interval excludes zero on the wrong side. The instrument measured our own capability
as ruthlessly as it measured the 446 bridge; that is the receipt's headline.

## Rail 1 — the knob

`TORON_TPCH_HASH_SMALLER_BUILD` (unset → engine default; `0`/`1` explicit; garbage
loud), every gauge bind routed through `gauge_planner_config()` so it cannot be
bypassed, recorded in child rows with provenance, contract pinned by
`adr469_hash_smaller_build_knob_contract`.

## Rail 2 — the engagement census (44 children, sf=1, strict parquet)

- **Engaged set: q02, q11, q14, q17, q19, q20** — six of 22 queries change plan
  fingerprint under the flag.
- **Digest equality on every engaged query** — 468's output invariance re-proven at
  every cell where the swap fires, not just where it was first tested.
- Zero failures. Artifact: `target/tpch/adr469_census/census.jsonl`.

## Rail 3 — the generalized paired rail and the price

- `--paired-env/--paired-on/--paired-off` generalize the paired driver; defaults
  byte-preserve the 449 filter rail (its five fixtures still pass). The manifest
  locks the switch triple with all standing coordinates.
- **Plan-distinctness gate** (the non-reservoir analog of gate zero): arms must
  differ in plan fingerprint and agree in digest, in-run, before any timed child —
  pinned by a four-case test (same-plan refuses; digest-mismatch refuses; failures
  refuse). At the cell: plans `63f7c2d559471990` vs `97ca111db986c14d`, digests
  equal, passed.
- The cohort (release, 20 runs + warmup, durable journal, explicit coordinates,
  zero `BudgetExhausted`, no flags): **10/10 pairs negative**, HL OFF−ON
  **−47.4 ms**, 95% interval **[−90.8, −27.7] ms** — detectable regression;
  |point| = 2.4% of the 1,965.8 ms OFF median, far under the 196.6 ms floor —
  **immaterial** by the sealed dual criterion.

## What this hands the next decision

At the one priced cell, the swap's estimate-driven choice does not pay: q17's
engaged join swaps toward a "smaller" build whose benefit is exceeded by the swap's
own costs at these sizes. This is precisely the evidence shape the named follow-on
(placement-aware order costing — pricing the swap decision itself, e.g. a minimum
absolute-size floor below which swapping cannot win) exists to consume. The default
stays as 468 set it — the regression is immaterial under the sealed rule and this
is one cell of six — but the first price is negative, and the five unpriced engaged
cells (q02, q11, q14, q19, q20) are now each one cheap cohort away. Flipping the
default is a decision for that fuller picture, not this receipt.
