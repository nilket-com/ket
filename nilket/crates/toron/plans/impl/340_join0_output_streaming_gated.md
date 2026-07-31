# ADR 340 - Implementation plan: Join(0) output streaming (selectivity-gated first substrate cut)

Pairs with `plans/adr/340_join0_output_streaming_gated.md`. An engine cut in `crates/toron` (exec.rs): stream
`Join(0)` output into its Filter (filter-on-the-fly, accumulate filtered), so q07/q21's 1.98 GB raw-output
materialization wall moves to the next boundary. **Wall-mover, not a pass claim. Gated:** Phase 0 decides
proceed-vs-pivot before any cut lands.

## Phase 0 - the hard selectivity gate (decision point, before D1)
- Instrument (temporary, env-gated, removed before seal - the ADR 336/339 pattern) the `Join(0) -> Filter` path at
  sf=1: raw `Join(0)` output bytes (already ~1.98 GB) and the **filtered output bytes** (post-filter). Release, 4G cap.
  This is not available as a passive log on the current path: the ADR 335 output guard fires before the Filter runs.
  The Phase 0 probe must temporarily chunk-materialize `Join(0)` output under an env gate, apply the Filter, record
  post-filter rows/bytes, and then be removed before seal. Do not weaken the production output guard except inside the
  temporary probe.
- **Branch (record the decision in the results doc):**
  1. filtered **materially smaller** than raw -> **proceed to D1**.
  2. filtered **~= raw** -> **stop; do not implement D1.** Re-plan: 341 (exchange streaming) becomes the first
     substrate cut; re-cut this ADR. The results doc records the inert-cut finding + the pivot.
  3. filtered **fits unexpectedly** -> proceed to D1, but expect q07/q21 to move farther; document the **actual** next
     wall (not assumed to be the filtered output).

## D1 - stream Filter <- Join(0) (only if gate branch 1 or 3)
- In the Filter arm (exec.rs:819), **before** `execute_bound(Join) -> Vec`, detect `Filter <- InnerJoin` and take the
  streaming path: build the join selection, loop **contiguous selection chunks** (reuse 338's chunk-materialize from
  `try_streaming_collapsing_global_aggregate` / `materialize_join_selection`), **filter each chunk**, **accumulate the
  filtered rows** into the returned `Vec`. The raw output never fully materializes.
- Budget the filtered accumulator itself. If the post-filter `Vec` is still too large, it must fail cleanly with a
  named `memory_budget_exhausted@execute` diagnostic (the first boundary after the streamed filter), not silently OOM
  while appending filtered chunks.
- Preserve selection order across chunks (within-partition order - the 338 invariant).
- The detection must precede `execute_bound(Join)` (else inert). Non-matching shapes (non-inner join, no filter, etc.)
  keep the existing path.
- Reuse the 338 chunk machinery; this is **accumulate-into-Vec** (non-terminal consumer), not 338's fold-into-aggregate.

## Tests (`toron` lib)
- **Filter-stream parity (Float64 + Decimal):** a synthetic `Filter <- Join` over more rows than the chunk threshold
  -> chunked filter+accumulate equals full materialize-then-filter, **both** modes (order preservation; Float64 breaks
  on reorder).
- **Filtered accumulator budget:** a synthetic filtered result that remains over the output cap emits the named
  `memory_budget_exhausted@execute` diagnostic from the filtered accumulator, proving the cut did not reintroduce a
  silent OOM wall.
- **Detection-before-execute:** the streaming path replaces `execute_bound(Join)` for the matched shape (not inert).
- **Shape fence:** non-inner join / no-filter / non-stateless-filter shapes keep the existing path.
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1) - verify the wall moved
```
scripts/cargo-cgroup build -p toron-tpch --release
# q07, q21 @ sf=1 (4G): still memory_budget_exhausted, but the retained diagnostic_message producer/bytes must be the
#   first boundary AFTER the streamed filter (Join(0) output guard no longer the producer), matching 339's prediction
#   (Exchange / Join(1)); if branch 3, document the actual next wall (may be q07/q21 moving farther).
```
- Verify via the ADR 332 retained `diagnostic_message`: producer moved off `Join(0)` output materialization.
- If the producer is the filtered accumulator, verify its bytes are the measured post-filter bytes and are materially
  below the raw 1.98 GB wall; if the filtered accumulator fits, verify the diagnostic names the later Exchange/Join
  boundary instead.
- Update the ADR 330 atlas: q07/q21 sf=1 producer changes from `Join(0)` output guard to the named next boundary.
- q14/q19 unchanged; q17/q12/q18/q04/q20 unchanged.

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
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01   # order preserved
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q07/q21 sf=1: wall moved off Join(0) output (retained diagnostic names the next boundary)
```

## Results doc
`crates/toron/plans/impl/340_join0_output_streaming_gated_results.md`: the Phase-0 selectivity measurement + the gate
branch taken; (if proceed) the streaming Filter<-Join cut; q07/q21 sf=1 before/after producer + bytes (the wall moved,
via 332 retention, including whether the first post-filter boundary is the filtered accumulator or a later
Exchange/Join boundary); the Float64/Decimal parity result; sf=0.01 q07/q21 pass@compare; the actual next wall named;
(if branch 2) the inert-cut finding + the pivot to 341; the ADR 330 atlas update.

## Risk / blast radius
- **Gate first** - measure filtered bytes + decide proceed-vs-pivot before D1; do not ship an inert cut.
- **Order, not associativity** - the Float64/Decimal parity test is the correctness gate.
- **Detection-before-execute** - the streaming path must precede `execute_bound(Join)`.
- **Acceptance is wall-moved-to-next-boundary** (via 332 retention), not q07/q21 green; the diagnostic names the first
  boundary after the streamed filter, wherever it lands.
- **No silent filtered accumulator** - if filtered output is still above budget, it must be the named diagnostic
  producer, not an untracked allocation.
- **Scope** - the `Filter <- Join(0)` boundary only; Exchange/join-to-join/semi-anti/q17 untouched.

## Seal
`impl: adr 340 join0 output streaming gated`. Do not commit wip drafts.
