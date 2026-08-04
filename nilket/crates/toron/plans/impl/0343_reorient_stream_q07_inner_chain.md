# ADR 343 - Implementation plan: reorient + stream the q07 inner-join chain

Pairs with `plans/adr/0343_reorient_stream_q07_inner_chain.md`. An engine cut in `crates/toron` (exec.rs): a bounded,
**executor-local** streaming helper for q07's matched all-Inner chain that **builds the small dimension** and **streams
the fact/intermediate as the probe**. q07-first; **q21 strictly out of scope** (semi/anti fenced). Green-or-next-wall;
no cost-model / global `execute_stream` claim.

## Phase 0 - the gate (before the cut)
1. **Swap safety:** confirm q07's downstream joins are all `Inner` (342 says yes) and identify each join's small
   (dimension) vs large (intermediate) side. Inner swap preserves the result *set*; output order is restored in D3.
2. **Orientation hook + the executor-local decision (load-bearing):** find where physical-left/build is chosen for
   q07's HashPartitioned joins (cost model / `probe_linearity_fence`, join_order.rs). **Confirm the reorientation can be
   made executor-local** in the matched streaming helper (choose small=build, fact=probe at execution) **without**
   changing the bound plan or general `execute_bound`. If orientation can only change globally in the cost model,
   **stop and re-scope** (gate 1) - do not risk green-cell re-planning.
3. **Probe-stream feasibility:** confirm the fact/intermediate probe can be pulled incrementally (340/341 visitor
   machinery) and probed batch-by-batch against the held small build, **without concatenating** the probe. Confirm the
   emitted intermediate can continue as the **next join's probe stream** (with exchange repartitioning applied
   batch-by-batch), not as a re-accumulated build side (gate 2).

## D1 - the executor-local reorient + stream helper (q07 matched shape)
- Detect q07's matched all-Inner `Join(i) <- Exchange <- ...` shape before `execute_bound`-materializing both children.
- **Build the small dimension** into the hash table (held, small); **pull the fact/intermediate probe** as a stream
  (visitor/chunk), probe each batch against the build, emit matched batches as the **next join's probe stream** (or into
  the grouped aggregate at the end of the chain) - **never concatenate the probe or re-accumulate the intermediate as a
  build-side `Vec`** (gate 2).
- The orientation choice is **local to this helper** (gate 1); the bound plan + fallback path are unchanged.

## D2 - no hidden materialization (gate 2)
- The reoriented probe must be consumed incrementally. A test must prove the helper **does not** concatenate the
  streamed probe into a `Vec` before joining (else inert). Assert via the streamed path (peak / a probe-batch counter /
  the absence of a full-probe `concat_or_empty`).
- The streamed output of each reoriented join must be consumed as the next reoriented join's probe stream (with
  exchange repartitioning batch-by-batch) rather than collected as the next build side. A test should distinguish
  "no full probe concat" from "hidden output accumulator"; both are failures for this cut.

## D3 - projection/order restore (gate 3)
- Swapping physical build/probe changes the natural output column/row order. The helper must **restore the materialized
  plan's observable column order and row order** (swap+restore), so q07's result is byte-identical to the materialized
  path at sf=0.01.

## Tests (`toron` lib + `toron-tpch`)
- **Reorient+stream parity (Float64 + Decimal):** a synthetic matched all-Inner chain -> the reoriented streaming
  result equals the materialized result, **column order + row order + values**, both modes (gate 3 + fold order).
- **No-hidden-materialization (gate 2):** the streamed probe is pulled incrementally, not concatenated (assert the
  probe is not fully materialized).
- **No-hidden-intermediate-accumulator (gate 2):** the output of one reoriented join is not accumulated as the next
  build-side `Vec`; it is consumed by the next reoriented join as probe chunks.
- **Executor-local (gate 1):** the bound plan / orientation for non-matching shapes is unchanged; the reorientation is
  only in the matched helper.
- **Shape fence:** non-matching (non-inner, semi/anti, no-exchange) keep the existing path + orientation.
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# Run the target/release probes under the same 4G memory + timeout wrapper used by the scale atlas.
target/release/toron-tpch run --query q07 --mode strict --sf 1
target/release/toron-tpch correctness --query q07 --sf 1
# q07 @ sf=1 (4G): pass@execute (target) + pass@compare; OR a named next wall (332 retention), measured producer.
# q21 @ sf=1: unchanged (out of scope, semi/anti fenced).
```
- q07 green, or the next named wall via 332. Update the ADR 330 atlas q07 cell.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q14/q19 stay green, **same plans** (the cost-model guard).
- q21/q17/q12/q18/q04/q20 unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01   # order + values preserved
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q07 sf=1 run + correctness -> pass@execute/pass@compare, or named next wall
#   green cells unchanged plans
```

## Results doc
`crates/toron/plans/impl/0343_reorient_stream_q07_inner_chain_results.md`: the Phase-0 orientation hook + the
executor-local decision (bound plan unchanged); the reorient+stream helper; q07 sf=1 before/after (build-side wall ->
green or named next wall via 332); the Float64/Decimal column+row-order parity result; sf=0.01 q07 pass@compare;
the green-cell regression check (unchanged plans); q21 noted out-of-scope; the ADR 330 atlas q07 update.

## Risk / blast radius
- **Executor-local (gate 1)** - orientation in the matched helper, not the cost model; green cells regression-checked.
- **No hidden materialization (gate 2)** - the probe streams; a concatenated probe or a re-accumulated intermediate is
  inert.
- **Projection/order (gate 3)** - swap+restore observable column+row order; sf=0.01 parity proves it.
- **q07-only** - q21 semi/anti fenced; not in this cut.

## Seal
`impl: adr 343 reorient stream q07 inner chain`. Do not commit wip drafts.
