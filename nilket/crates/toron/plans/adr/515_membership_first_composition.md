# ADR 515 - Membership-first composition: commute the pinned semi to its key source

q18 is the board's largest surviving gap: 18.5-20.0 s against DuckDB's 1.8 s. The grounding
pinned the cause as pure plan shape. Toron binds the left-deep spine
`(customer ⋈ orders) ⋈ lineitem` and applies the author-pinned membership `LeftSemi` (the
`o_orderkey IN (grouped HAVING body)` of ADR 322) **at the root, last**:

| fact | value |
|---|---:|
| join node (all of it pre-semi) | 12,972.1 ms, 989,777,032 B spill |
| exchange on pre-semi volume | 3,451.5 ms |
| membership body fold (`scan_grouped`, stays either way) | 2,552.3 ms |
| qualifying orderkeys | **501 of 12,000,000 (0.004%)** |
| post-semi lineitem volume | **3,507 of 47,989,007 rows** |
| peak RSS | 10.77 GiB |

The reference execution runs the semi first, onto orders, and every downstream build is
post-membership tiny. This is the third instance of the arc's one recurring defect family —
selective reducers applied above instead of below (q17's join0 in ADR 511, q17's scalar
aggregate in ADR 512, q18's membership here) — and it is ADR 423's forward arc verbatim: "the
membership-first composition remains the next bounded cut."

## The transformation, framed narrowly

Commute an **existing, author-pinned `LeftSemi`** through an **Inner-join spine** to the single
source branch of its **complete correlation-key tuple**. This is not general join reordering: the
spine's join order is untouched, the membership subtree is untouched, and the only change is
where the already-planned semi applies.

The soundness argument is the classical pushdown identity, stated once and pinned by fixtures: a
plain-equality `LeftSemi` is a row-level predicate on the source branch's columns (membership of
the key tuple in the body's key set). A row-level predicate on one branch's columns commutes with
any Inner-join spine — filtering the source before fanout is identical to filtering the composite
afterward, for every key distribution. Duplicates cannot multiply (a semi emits each source row
at most once either way); null keys fail plain equality on both sides of the commute; missing
keys never match on either side. The fixtures prove each case rather than trusting the argument.

## Decision

1. **Recognizer, declining by default**, at the binder's Join arm on the direct bind path:
   - the root is `LeftSemi`, plain equality (`null_safe_keys = false`), strategy pinned by the
     author (the ADR 322 membership shape);
   - the membership subtree is **independent of every outer relation** except through the
     equality tuple: a self-contained subplan over a conservative whitelist (scan, filter,
     project, alias, distinct, aggregate, exchange), or decline;
   - every semi key on the outer side is a plain column tracing **unambiguously to exactly one
     leaf branch** of the spine (exclusive scope ownership), and all keys trace to the **same**
     branch, or decline;
   - every join crossed on the path from the root to that branch is `Inner` with plain equality —
     outer, anti, cross, null-safe, or ambiguous-lineage shapes decline;
   - the rewrite moves the **exact membership subtree and its author pin** onto the source
     branch — no cloning, no re-execution, no strategy change: `LeftSemi(branch, body)` with the
     same `on` tuple and the same pinned intent, spine above unchanged.
2. **Termination without oscillation** (ADR 512's lesson, applied structurally): the rewritten
   root is an Inner join, so the semi arm does not re-enter for it; the relocated semi's outer
   side is a single branch, not a spine, so the recognizer declines it on re-entry. One commute
   per semi, then ordinary binding — including the existing graph-order passes over the now-tiny
   spine, which see the rewritten tree normally.
3. **Typed, latched control**: `PlannerConfig::membership_first_composition`, compiled default
   off at the seal; gauge env `TORON_TPCH_MEMBERSHIP_FIRST` with the loud contract; rows carry
   value and provenance. The rewrite changes plan fingerprints, so the cohort runs under the
   existing plan-distinct gate.

## Evidence gates before the cohort

- Bind receipts on the q18 shape: the semi binds below the spine on the orders branch with its
  pin intact; knob off reproduces today's root-semi plan byte-for-byte.
- Direct volume evidence at the exact coordinate, both arms: the membership output remains
  **501 keys**, the downstream orders/lineitem volume collapses to **3,507 rows**, the
  989,777,032 B spill is eliminated (route facts and node spill zero on the engaged join), and
  digest, schema, and row count are identical. Peak RSS must collapse with the volume.
- The census determines the true engaged set across q01-q22 and m00-m10; q18 is the motivator,
  not the claim. Every fingerprint-changed cell runs its strict correctness rail in both arms;
  q19 stays the standing sentinel.

## Acceptance

- Equivalence fixtures, both arms identical, proving filter-before-fanout equals
  filter-after-composite: duplicate source keys (fanout above the semi), duplicate body keys,
  null keys on either side, keys present in the body but absent from the source and vice versa,
  and a multi-key tuple; plus the amplification pin — a source row matching several spine rows
  survives or dies atomically in both shapes.
- Decline pins, plan byte-identical with the knob on: an outer join on the path; an anti or
  cross join on the path; null-safe keys at the semi or on the path; a key tuple split across
  two branches; a membership body referencing an outer relation; a non-pinned semi shape the
  recognizer does not own.
- Pin preservation: the moved semi carries `PinnedByAuthor` and the identical membership subtree
  (structural equality, single receipt) — no clone, no second execution of the body.
- Paired verdict on q18 at the fixed 3,554 ms floor, predeclared estimator, plan-distinct gate;
  activation per ADR 472's rule.

## Guardrails

- No join-order change, no estimator, no new join kind, no placement change; ADRs 511-514
  surfaces are untouched. The commute is tree surgery on one recognized shape.
- The recognizer's whitelist and single-branch requirement fail closed; anything unproven leaves
  the plan byte-identical.
- The membership body's own cost (~2.5 s fold) is explicitly not claimed; the ADR's envelope is
  the pre-semi join, exchange, aggregate-input, and RSS waste.

## Risks

- Other pinned membership shapes across the board may engage; the census enumerates them and
  their rails run before measurement.
- The relocated semi's build (the 501-row body output) is trivially small, but the body still
  runs before the spine consumes orders — a scheduling dependency the diagnostic should show as
  the body fold preceding a now-fast spine, not overlapping waste.
- The spine's graph-order passes see a differently-shaped tree; their own receipts must stay
  reconciled (the census's fingerprint-per-cell discipline catches any surprise).

## Forward

- If activated, q18's remaining envelope is the membership body fold itself (~2.5 s over 48 M
  rows) — a `scan_grouped` premise the ADR 508 compute-fold ranking already names.
- q21's direct-route cache residual stays parked until it presents a narrower mechanism than
  general cache tuning.
- The reducer-below meta-pattern now has three instances; if a fourth appears, a survey ADR
  hunting the family systematically (rather than cell-by-cell) becomes worth its ceremony.
