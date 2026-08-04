# ADR 526 - Compact composite-key build lifecycle (q09), Phase-0-first

ADR 525 priced the partsupp lifecycle with authority: E = prep 1,485.119 + replay-2 probe
1,918.675 + cleanup 729.717 = **4,133.511 ms**, clearing the 3,554 ms floor by **579.511 ms**.
The retained footprint decomposes exactly:

| component | bytes | share |
|---|---:|---:|
| Arrow build batch | 204,800,288 | 20.5% |
| generic hash slots/bookkeeping (7,340,032 x 80) | 587,202,560 | 58.9% |
| heap-allocated encoded keys (6,400,000 x 32) | 204,800,000 | 20.5% |
| duplicate-row overflow | ~0 | — |
| total | 996,802,848 | |

~79.5% is index representation — primarily a layout problem. But layout-only economics are
**precarious**: scaling q09's own in-profile Int64 orders lifecycle to 6.4M rows at the same
probe count projects an optimistic 717.526 ms survivor, a 3,415.985 ms saving — **138 ms below
the floor** — and a two-Int64 key must not be assumed cheaper than that single-Int64 proxy.
The successor's budget is the margin, not the envelope: the whole replacement lifecycle must
project **below 579.511 ms** for the saving to clear the floor. Payload retention is likely
what decides the verdict; layout alone currently projects below materiality.

## Phase 0 - one coherent representation, exact data, predeclared kill

A throwaway prototype — no mechanism code, removed after measurement — implements exactly one
candidate representation and measures the full replacement lifecycle on the real inputs at the
exact coordinate:

1. **fixed-width two-key index**: exactly two Int64 columns — q09's
   (ps_partkey, ps_suppkey) composite — stored as fixed-width pairs, with no per-key
   `Vec<u8>` allocation anywhere;
2. **batched construction and probing** (batch-granular staging, no per-row dispatch);
3. **payload retention**: after index construction retain only the payload columns the chain
   consumes downstream, releasing redundant key columns from the retained batch. For q09 the
   derived set is exactly `ps_supplycost`: `ps_partkey` and `ps_suppkey` serve lookup only,
   while the downstream key values come from lineitem. The prototype pins that fact, while the
   eventual mechanism derives the set from the chain's required-output metadata rather than
   hard-coding q09;
4. **unchanged duplicate/null/equality semantics** — duplicates chain exactly as today, a
   NULL key matches nothing, equality is bit-exact with the generic path;
5. **byte-identical generic fallback** outside the eligible shape (prototype demonstrates
   the boundary; the mechanism would gate on it).

Inputs are exact: the real 6,400,000-row partsupp build columns and the real 2,610,723-entry
probe-key stream (the chain's suffix-1 output keys), preserving its 23,436 outer call/batch
boundaries and row order. Before timing counts, the prototype must match both the generic
path's flow counters and the ordered `ps_supplycost` output digest — match count alone cannot
license a probe that skipped payload gathering.

Each of five repetitions runs in a fresh child process. The authority statistic is the median
of the five **whole replacement lifecycle totals**; component medians are explanatory only.
The timed regions are E-equivalent:

- construction includes persisted partsupp scan/decode, key extraction and packing, table
  allocation/insertion, payload projection, and release of redundant build buffers;
- probing includes per-batch key extraction/packing, lookup, duplicate expansion, and ordered
  payload gathering under the original 23,436-boundary schedule;
- teardown drops the last owners of the compact index and retained payload — not cloned Arcs
  whose buffers remain owned by the harness.

Any unavoidable harness/integration difference is a named positive ledger line. Source
acquisition, output materialization, allocator ownership, key packing, or dispatch may not be
silently placed outside the comparison.

**Predeclared kill: if the prototype cannot project total prep + probe + teardown below
579.511 ms, ADR 526 closes red without mechanism code.** If it clears, the ADR proceeds as
one lifecycle cut priced against E.

## Decision (contingent on Phase 0)

1. One lifecycle mechanism behind a declining gate: a plain-equality suffix build with exactly
   two Int64 key columns may commit the compact representation — fixed-width pair index,
   batched construction/probe, payload-trimmed retention. Arrow field nullability does not
   decline the rule: under plain equality a NULL in either key component is skipped, exactly as
   today. Null-safe joins and every other key shape retain the generic path byte-identically,
   as does the knob-off arm.
2. Typed, latched control: `PlannerConfig::compact_composite_build` (plumbed to the runtime
   build decision), compiled default off at the seal; gauge env
   `TORON_TPCH_COMPACT_COMPOSITE_BUILD`, loud "0"/"1" contract, row fields with provenance.
3. Plans do not change — the cut is a runtime representation choice. The paired cohort runs
   under an **identity gate** (fingerprints, digests, and execution route equal across arms)
   plus an explicit build-representation receipt distinguishing them. Both route and
   representation facts must be serialized in every arm's perf row; ADR 525 proved that a
   profiler-off row cannot be treated as if an absent route field were evidence. Activation is
   per ADR 472's rule.

## Acceptance

1. Phase 0 as above, with red-close authority.
2. **Structural battery**: eligibility engages on the two-Int64 composite, including nullable
   Arrow fields under plain equality; declines pinned (null-safe equality, variable-width or
   non-Int64 keys, mixed-width pairs, arity other than two, single-key shapes — the existing
   specialized path owns those — and knob off). Each decline is byte-identical to the generic
   plan and route; engagement is visible via the build receipt, not the plan.
3. **Semantic battery**: duplicate keys with multi-row chains, NULL keys on both sides,
   empty build, all-duplicates build, equality edge cases at integer boundaries — both arms
   identical results and flow counters.
4. **Census**: q01-q22 + m00-m10 both arms — every fingerprint AND digest identical (the cut
   may not change any plan); the ADR 525 instrument witnesses q09's new lifecycle (prep,
   replay-2 parent, cleanup, retained bytes) in the on-arm; engaged cells enumerated by build
   receipts; strict correctness rails for q09 and the q19 sentinel in both arms.
5. **Paired authority**: q09 cohort, 20 runs / 10 adjacent pairs, identity gate, floor
   3,554 ms, Hodges-Lehmann + 95% Wilcoxon from the persisted journal; both-way verdicts;
   regression-check cohorts for any other cell whose build receipts show engagement.

## Guardrails

- The eligibility gate fails closed; ineligible shapes and the off arm run the generic path
  byte-identically.
- No planner changes; ADR 511-525 surfaces untouched; the ADR 525 instrument is the witness,
  not a casualty — its clocks must reconcile on the new lifecycle too.
- The 64-row-window observation from ADR 524 remains quarantined; if the prototype's batched
  probing resembles it, the resemblance is earned by Phase 0 measurement, not by the
  confound.

## Risks

- The kill arithmetic is tight by construction (579.511 ms for a full lifecycle) — a red
  Phase 0 is a priced, likely outcome and costs no mechanism code.
- Payload-retention trimming must not break downstream column consumption; the chain's
  consumed-column set is derived, not assumed, and the semantic battery covers it.
- Teardown inside E is order-sensitive; the cohort, not E arithmetic, is the activation
  authority.

## Forward

- On activation: q09's residual re-ranks (48M suffix-1 lookup 568 ms, prep/cleanup shares of
  the other builds); the board's next tier is the shared-fold family.
- On red: q09 is parked with both the attribution and the representation verdict recorded;
  the board moves to the shared-fold family.
