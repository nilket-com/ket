# ADR 513 - Reachable-key reduction for semi/anti membership inputs

q21 is the board's second-largest surviving gap: 16.3-17.9 s against DuckDB's 2.8 s, carrying
897,713,384 B of spill that no join-spill route commits — it is reservoir spill inside the
`inner_prefix_semi_anti_grouped` handler. The post-512 grounding falsified the adjacent
hypothesis: the EXISTS semi and NOT-EXISTS anti are `PinnedByAuthor`
(`JoinStrategyPinSource::Direct` from the frontend lowering), so build-side placement never
reaches them, and their probe replays cost only 224 ms combined. The envelope sits elsewhere:

| owner | ms | notes |
|---|---:|---|
| `deferred_replay` envelope | 9,899.1 | all 897.7 MB spill; children: reservoir_buffer 4,164.2, inner_replay_1 4,619.5 (compact-index rebuild over spilled reservoir pages), inner_replay_2 446.5, semi 120.0, anti 103.7, fold 10.4, reservoir_finish 367.9 |
| `runtime_prep` | 5,728.7 | builds the semi/anti `RecognizedNeqSummary` runtimes by scanning the membership inputs: l2 = all 47,989,007 lineitem rows, l3 = the receiptdate>commitdate half |
| base_scan + inner_replay_0 | 2,200.7 | the l1 stream and first prefix replay |

The membership inputs are lineitem self-aliases correlated on `l_orderkey`, and the probe side's
key domain is bounded upstream: `l1.l_orderkey` equi-joins `o_orderkey` under
`o_orderstatus = 'F'` — a deterministic filtered base relation. ADR 512's superset theorem
extends verbatim to both membership polarities: a membership row whose complete correlation-key
tuple falls outside the probe-side domain can never match any probe row, so removing it changes
no EXISTS outcome and — because it could only have eliminated probe rows sharing its key, which
do not exist — no NOT-EXISTS outcome either.

## Decision

1. **Mechanism, and only the mechanism**: restrict the l2/l3-class membership inputs with a
   `LeftSemi` against a proven superset of the outer correlation-key domain. The recognizer
   engages a `LeftSemi` or `LeftAnti` join whose keys are plain equality
   (`null_safe_keys = false`) when ADR 512's derivation walk traces **every** key of the
   correlation tuple, jointly, to one shared deterministic filtered base-relation chain in the
   **probe-side** plan. The reduction semi joins on the **complete correlation-key tuple**; all
   residual membership predicates (`l_suppkey <>` match filters, `receiptdate > commitdate`
   locals) remain on the membership rows untouched. Partial tuple coverage declines.
2. **The route is a consequence, not a change.** The deferred-versus-direct selection —
   `partition_at_a_time_replay_spec` (streaming.rs:14500): deferred iff suffix runtime 1's build
   is `PartitionedScan` with `partition_count > 1`, unloaded, Int64 keys — is **not modified**.
   If reduced membership summaries lower the resident baseline enough that runtime 1's build
   loads, the handler takes its existing direct route and the 898 MB reservoir disappears as a
   consequence. The receipt must attribute savings causally: mechanism effect (membership rows,
   summary bytes, runtime_prep time) separately from route effect (variant selected, reservoir
   bytes, deferred stages present or absent).
3. **Typed, latched control**: `PlannerConfig::membership_input_reduction`, compiled default off
   at the seal; gauge env `TORON_TPCH_MEMBERSHIP_INPUT_REDUCTION` with the loud contract; rows
   carry value and provenance. Measurement on q21 at the immutable coordinate under the
   plan-distinct gate and the inherited 3,554 ms floor; activation per ADR 472's rule.

## Phase 0 — pin before the mechanism lands

- The deferred/direct condition, cited exactly (done at plan time; re-verified in the receipt):
  `partition_at_a_time_replay_spec`, streaming.rs:14500.
- Suffix runtime 1's build input and `estimated_build_bytes`, and whether the reduced resident
  baseline crosses the existing loaded-build admission **without changing it** — the route-flip
  question, answered from residency receipts, not assumed.
- The projected resident charge and runtime-build (summary) shape after reduction: membership
  input rows, summary bytes, and headroom against the 2 GiB budget in both variants.
- The no-flip floor path, predeclared honestly: if the route does not flip, the mechanism's
  reachable savings is bounded by the `runtime_prep` share (roughly half of 5.7 s at the ~2x
  membership cut σ_F(orders) provides) plus second-order effects — plausibly **below** the
  3,554 ms floor. That outcome closes `detectable, immaterial` per the verdict table; the floor
  is not renegotiated, and the route flip is not forced to rescue it.

## Acceptance

- Equivalence battery extending ADR 512's, for both polarities: keys present/absent from the
  probe domain, null keys both sides, duplicate keys, the complete-tuple boundary (a composite
  correlation where only one key traces → decline), and residual-predicate preservation — a
  NOT-EXISTS fixture where dropping a residual would change results pins that residuals ride the
  membership side untouched.
- Decline pins: null-safe keys, partial tuple trace, unfiltered source, knob off — all
  byte-identical plans.
- Engagement evidence on q21: membership scans shrink to the σ_F(orders)-bounded subset; the
  semi/anti summary builds shrink accordingly; digest and row count identical in both arms.
- Census diff across q01-q22 and m00-m10 with per-arm fingerprints, routes, admissions, digests;
  every changed cell correctness-green in both arms; q19 remains the standing sentinel.
- Paired verdict against the fixed floor from the predeclared estimator, with the diagnostic
  separately reporting mechanism metrics and route metrics per arm.

## Guardrails

- The recognizer declines by default; no estimator, no cost model, no route-condition edit, no
  pin removal, no placement change — ADR 512's placement work is untouched.
- The reduction never rewrites residual predicates and never engages a partial key tuple.
- The handler's stage contract and vocabularies are unchanged; if the direct variant activates,
  it is the existing predeclared variant observed, not a new lifecycle.

## Risks

- Double-scan of the bounding relation (σ_F(orders) is ~half of orders) — priced by the census;
  the reduction semi itself gets ADR 512's smaller-build placement for free when bounds admit.
- The membership cut is only ~2x on q21 (unlike q17's ~1000x); if the route does not flip, the
  cohort may close below the floor. That is an honest close, and the receipt's causal split
  tells the successor exactly which lever remains.
- Engagement beyond q21 (any EXISTS/NOT-EXISTS with a traceable probe-side bound) is
  hypothesis-only; the census is authoritative.

## Forward

- If the route does not flip: the reservoir's `inner_replay_1` compact-index rebuild (4.6 s) is
  the next bounded envelope, and the ADR 510 red pinned what does not work there.
- Selectivity coverage (ADR 472 forward) would sharpen every source-derivation admission.
- q02/q20 scalar-reduction coverage and `LeftAnti` placement (moot for q21, possibly not
  elsewhere) remain named follow-ons.
