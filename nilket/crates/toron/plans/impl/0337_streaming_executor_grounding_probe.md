# ADR 337 - Implementation plan: streaming executor grounding probe

Pairs with `plans/adr/0337_streaming_executor_grounding_probe.md`. A **measurement probe** in `crates/toron` /
`toron-tpch` - no engine cut. Maps the materialization boundaries, measures which dominates the sf=1 memory family,
and names **one** first streaming conversion with **evidence-backed expected coverage for ADR 338**. Built binary,
release, cgroup-capped, instrumentation removed before seal. **No streaming implementation; no pass claim.**

## Phase 0 - plan-shape map (the segment, including intermediate nodes)
- For each memory cell (q07/q14/q17/q19/q21 + q18/q12/q04/q20), record the bound operator tree from the failing node
  up to its collapsing consumer. For the 5 output-cells, name the **collapsing-pipeline segment**:
  `Join-output -> [Filter/Project/Expr ...] -> Aggregate(group-by | scalar)`. The intermediate Filter/Project nodes
  are part of the segment only if the bound plan keeps them on that path. q07's derived-table SQL introduces
  candidate date/nation filter work and a `volume` expression before the GROUP BY, but Phase 0 records the measured
  bound shape rather than assuming where those nodes land.
- Confirm the aggregate **collapses** (group cardinality << input rows): q07 nation x year, q21 s_name, q14/q17/q19
  scalar (1 row).
- For q18/q12/q04/q20 record the input/spill boundary (already named by ADR 336) - they are coverage candidates only
  if the chosen cut also bounds retained inputs.

## Phase 0 - measure the failing-node retained inputs (don't infer)
- Instrument (temporary, env-gated, removed before seal - the ADR 334/336 pattern) the **failing join node's
  immediate retained left/right batches** via `get_array_memory_size` - the *actual* intermediates it holds, which
  for a late multi-way join are prior-join results, not base tables.
- Record per cell the **peak split**: retained inputs (held) vs the output that streaming would remove. Run release,
  4G cgroup cap, sf=1, summary-not-dump.
- The decisive datum: with the output streamed away, do the **measured** retained inputs sit under 4 G? Classify each
  of the 5 as **expected-pass under output-streaming** (q07/q21 likely) or **expected-partial / needs probe-side
  streaming** (q14/q17/q19 likely, since they spill).

## Phase 0 - conversion scope (bounded vs substrate verdict)
- Determine whether the collapsing-pipeline segment can stream via a **narrow** path - e.g. an `execute_stream`
  variant where the aggregate pulls join output batches through the pipelineable nodes and folds them - **without**
  rewriting the whole `execute_bound` dispatch, the join build side, or the sink.
- Note what each option touches (operators, signatures) so ADR 338's size is known. This is the
  **bounded-vs-substrate** verdict, with evidence.

## Deliverable (the results doc - the probe IS the deliverable)
`crates/toron/plans/impl/0337_streaming_executor_grounding_probe_results.md`:
- The plan-shape map per cell (the collapsing-pipeline segment + intermediate nodes; the input/sink boundary for the
  rest).
- The per-cell peak split (measured failing-node retained inputs vs streamed output), release/4G/sf=1.
- The named **first cut** (the segment + operators) + **expected coverage for ADR 338** (cell -> expected-pass /
  expected-partial, each with its peak-split evidence) + the **reason it is first**.
- The **bounded-vs-substrate verdict** with what ADR 338 will touch.
- Exchange confirmed on/off the single-node sf=1 path.
- Explicit no-overclaim line: 337 measures + predicts; **no cell is moved by this ADR.**

## Tests
- A probe - no engine assertion, no behavioral change. Existing `toron`/`toron-tpch` suites stay green (unchanged).
  Any instrumentation is temporary + env-gated + removed before seal (`git diff --check` clean).

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
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check        # instrumentation removed; no engine change
# probe evidence (release, cgroup capped, recorded - not a CI gate):
scripts/cargo-cgroup build -p toron-tpch --release
#   per-cell failing-node retained-input + output peak split @ sf=1 (q07/q14/q17/q19/q21 + q18/q12/q04/q20)
```

## Risk / blast radius
- **Probe only** - map + measure + name + predict; no streaming code, no pass claim.
- **Measure, don't infer** - failing-node retained inputs, not base-table size.
- **Hypothesis confirmed per cell** - the collapsing-pipeline segment shape (and intermediate nodes) is measured, not
  assumed.
- **No engine change** - sf=0.01 + the observable memory board unchanged.

## Seal
`impl: adr 337 streaming executor grounding probe`. Do not commit wip drafts.
