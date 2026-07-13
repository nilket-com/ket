# ADR 431 — q17 sf=10 oracle boundary fidelity

## Context

The TPC-H board is 20/22 green. ADR 417 cleared q17's scan-output wall, so the engine reaches one
row at sf=10 in about 57 seconds and 1.06 GiB RSS. Correctness remains pinned to sf=0.02 by
`q17_sf10_oracle_policy_gap` even though the existing oracle is already O(eligible-parts). The open
question is exact `Decimal128(20,6)` fidelity at the correlated
`l_quantity < 0.2 * AVG(l_quantity per part)` boundary.

The engine exposes only the final scalar. Final-scalar equality can mask compensating selection
differences, so lifting the pin requires a structural Toron-versus-oracle census.

## Decision

Run two harness-only diagnostic queries through the ordinary execute path before changing the pin:

1. Per-part aggregates over eligible Brand#23/MED BOX parts:
   `p_partkey, SUM(l_quantity), COUNT(*), AVG(l_quantity)`.
2. Rows selected by q17's exact correlated predicate, projected as the stable contribution multiset
   `p_partkey, l_orderkey, l_suppkey, l_quantity, l_extendedprice`.

Aggregation and selection are separate because Toron deliberately rejects an aggregate nested in an
aggregate `CASE` argument. This cut adds no engine instrumentation or q17-specific execution code.

Compare two typed structures:

- The common Toron-versus-rounded-oracle structure contains the sorted per-part aggregates and sorted
  selected-row contribution multiset. These are compared exactly. FNV-1a summaries are evidence only,
  never the correctness gate.
- The oracle-only boundary receipt classifies every eligible row against both the exact rational
  boundary (`5 * quantity_raw * count` versus `quantity_sum_raw`) and the current Decimal path
  (scale-6 AVG, scale-7 threshold). It records below/equal/above counts and membership differences.

The census has three outcomes:

1. Toron exactly equals the rounded oracle and rounded membership equals rational membership: lift
   the pin.
2. Toron equals the rounded oracle but rounded and rational membership differ: record a Decimal
   contract decision; do not label it an engine defect or lift the pin.
3. Toron differs from the rounded oracle: record the implementation/oracle divergence; do not lift
   the pin.

On outcome 1, extract a pure shared q17 correctness-scale policy. No explicit scale retains the 0.02
default; explicit 0.02 and 10 are accepted; other values, including the vacuous global default, are
rejected. Route the CLI, data-source correctness entry, and memory correctness route through it.

## Acceptance

- The typed census structures compare exactly and the rational-versus-rounded membership difference
  is zero. Hashes are retained only as summaries.
- A live persisted sf=10 strict q17 run completes under the unchanged 4 GiB / 600 second envelope.
- Persisted sf=10 correctness reaches `pass@compare` with exact `Decimal128(20,6)` schema and value.
- Pure scale-policy tests lock absent → 0.02, explicit 0.02/10 → accepted, and all other scales →
  rejected.
- Full engine and `toron-tpch` gates pass; no engine branding or `.perf.data` is added.

Outcome 2 or 3 is a diagnosis-only seal and leaves the board at 20/22. Outcome 1 moves it to 21/22.

## Forward arc

q02 remains the final execution-model cell: join-output streaming/spill for its 1.738 GB monolithic
join output.
