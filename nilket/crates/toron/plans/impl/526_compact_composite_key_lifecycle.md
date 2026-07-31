# impl 526 - compact composite-key lifecycle plan

Phase-0-first: a throwaway prototype must project the entire replacement lifecycle — fixed-
width two-key index construction, batched probing, payload-trimmed retention, teardown —
below **579.511 ms** on exact data before any mechanism code exists. Layout alone projects
138 ms below materiality; payload retention likely decides.

## Immutable inputs

- tree: post-525 main (`e5b74501` lineage), all activated knobs default on;
- exact coordinate: strict sf=8 persisted Parquet (format 2, full-hash), budgets 2 GiB /
  2 GiB explicit; fixed floor 3,554 ms;
- ADR 525 receipts (immutable): E = 1,485.119 + 1,918.675 + 729.717 = 4,133.511 ms; margin
  579.511 ms; five-arm E bracket [4,088.985, 4,147.156]; partition residuals 6 us / 8 us;
- footprint decomposition (operator, exact): Arrow batch 204,800,288 B; generic hash
  slots/bookkeeping 587,202,560 B (7,340,032 x 80); heap-encoded keys 204,800,000 B
  (6,400,000 x 32); duplicate overflow ~0; index share ~79.5%;
- the precarious proxy (operator): q09's Int64 orders lifecycle scaled to 6.4M rows at equal
  probe count → 717.526 ms optimistic survivor → 3,415.985 ms saving, 138 ms below floor;
- flow facts: build 6,400,000 rows; probes 2,610,723; emitted 2,610,723; multiplicity ~1.

## Phase 0 - throwaway prototype on exact data

Protocol:

1. Extract exact inputs from the sf=8 dataset: the partsupp build columns and the
   2,610,723-entry (partkey, suppkey) probe-key stream the chain feeds suffix-2 (derived the
   same way the chain derives it; validated against the ADR 516 flow counters).
2. Prototype one representation — fixed-width two-Int64 key index (no per-key `Vec<u8>`),
   batched construction and probing, payload retention trimmed to the chain's consumed
   columns with redundant key columns released — with duplicate/null/equality semantics
   identical to the generic path.
3. Verify match-count identity against the generic path's counters; then time construction,
   the full probe pass, and teardown at the coordinate (same allocator and runtime context
   class as the engine; five repetitions, median per component).
4. Ledger any cost the prototype cannot see (integration overhead it omits) as a stated
   line, not a silent zero.

**Predeclared kill: projected prep + probe + teardown ≥ 579.511 ms closes ADR 526 red with
the component table; the prototype is removed either way; no mechanism code exists before the
verdict.** Results land in this document before Step 1.

## Step 1 - the mechanism and control (contingent)

`PlannerConfig::compact_composite_build` plumbed to the runtime build decision, compiled
default **false**; gauge env `TORON_TPCH_COMPACT_COMPOSITE_BUILD`, loud "0"/"1" contract, row
fields with provenance, knob-contract test. Eligibility: composite keys that are exactly
tuples of fixed-width integer columns; everything else (and knob-off) runs the generic path
byte-identically. The build receipt records which representation engaged.

## Step 2 - tests

Structural: eligibility + per-gate declines (variable-width, non-integer, single-key,
knob-off) with byte-identical plans and routes; engagement via build receipt. Semantic:
duplicates with multi-row chains, NULL keys both sides, empty and all-duplicate builds,
integer-boundary equality — identical results and flow counters both arms. ADR 525
instrument reconciles on the new lifecycle. Architecture reconciliations named in the
receipt.

## Step 3 - census and rails

Both arms, q01-q22 + m00-m10: every fingerprint AND digest identical (no plan may change);
engaged set from build receipts; ADR 525 clocks witness the new q09 lifecycle (prep,
replay-2 parent, cleanup, retained bytes); strict correctness q09 + q19 sentinel both arms.

## Steps 4-7 - seal, gates, cohort, receipt

Standard protocol: cohesive seal, full battery with direct exit statuses, push, release
build; q09 paired cohort (20 runs / 10 adjacent pairs, identity gate + engagement receipts,
floor 3,554 ms, Hodges-Lehmann + 95% Wilcoxon); regression cohorts for other engaged cells;
activation per ADR 472's rule with default flip, knob retarget, full gates, default-arm
smokes; both-way verdicts priced against E.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
