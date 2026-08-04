# ADR 337 - streaming executor grounding probe

## Status
Proposed.

## Context

The scale arc's inflection. ADRs 330-336 made the entire sf=1 memory family **observable** (every cell a named
`memory_budget_exhausted` row); the remaining wall is **fit**, and it is the `Vec<RecordBatch>`
materialize-each-operator model. This ADR is a **grounding probe, not a rewrite:** map the materialization
boundaries, measure which dominates the sf=1 memory family, and name **one** first streaming conversion with
evidence + **expected** cell coverage for the implementing ADR (338). It makes nothing pass - it produces the
evidence that sizes the first cut.

### Grounded - every boundary is a full materialization
`execute_bound` (exec.rs:752) recursively materializes each child to a full `Vec<RecordBatch>` before operating:
filter/project/repeat/aggregate/sort/take/exchange do `execute_bound(input) -> Vec` (exec.rs:808/824/866/880/907/944/1958);
join does `execute_bound(left/right) -> Vec` (exec.rs:960-961). Candidate streaming boundaries:
1. **join output -> collapsing pipeline -> aggregate** (the 5 output-cells q07/q14/q17/q19/q21, ADR 335).
2. **join inputs** (q18/q12, ADR 336 - retained concat'd inputs).
3. **final sink** (top-level `execute_bound` result `Vec`).
4. **exchange** (distributed; the sf=1 reach path is single-node - confirm off-path in Phase 0).

## Decision - map + measure the boundaries, name the first cut + its expected coverage (no rewrite)

### Hypothesis (to prove in Phase 0, not assume): a collapsing-pipeline segment
The leading first cut is **not** a single join->aggregate edge. The 5 output-cells feed a *collapsing aggregate*
(q07/q21 group-by; q14/q17/q19 scalar) and may do so **through intermediate pipelineable nodes**. q07's derived-table
SQL introduces candidate filter/project work (date/nation predicates and
`volume = l_extendedprice * (1 - l_discount)`) before the outer GROUP BY, but the bound plan may push some of that
work earlier or shape it differently. So the streamable segment is the measured
`Join-output -> [Filter/Project/Expr]* -> collapsing Aggregate`: if every node in it consumes batches lazily and the
aggregate folds into a small group state, the join output never fully materializes. Phase 0 confirms the **actual**
segment shape per cell (any intermediate nodes on that path are part of the cut).

### Phase 0 - measure (the deliverable)
1. **Plan-shape map:** for each memory cell (q07/q14/q17/q19/q21 + q18/q12/q04/q20) record the operator tree and the
   dominant materialization boundary, including the **intermediate nodes** between the failing join and its collapsing
   consumer (the collapsing-pipeline segment), or the input/sink boundary for the others.
2. **Failing-node retained inputs (measure, don't infer):** instrument the **failing join node's immediate retained
   inputs** - the actual left/right batches it holds, which for a late multi-way join are large **intermediate**
   batches, not base tables. Do **not** infer fit from base-table size (lineitem fitting under 4 G says nothing about
   a late join's retained intermediate). Record the peak split: retained inputs vs the streamed-away output.
3. **Conversion scope (bounded vs substrate):** can the collapsing-pipeline segment stream via a **narrow**
   `execute_stream`-style path (the consumer pulls join batches through the pipelineable nodes, folding into the
   aggregate) **without** rewriting the whole `execute_bound` dispatch, the join build, or the sink? Or does the
   `Vec`-per-operator model force a broad substrate change? This is the verdict the probe returns.
4. **First cut + expected coverage:** name the boundary + operators, and the **evidence-backed expected coverage** -
   for which cells does removing the streamed output (given the measured retained inputs) drop the peak under 4 G?
   q07/q21 (in-memory build) may need only output-streaming; q14/q17/q19 (spill) may need probe-side streaming too -
   measure, classify each as expected-pass / expected-partial under the first cut.

## Acceptance - a sized, named first cut (no rewrite, no pass claim)
- **A named first conversion** (the collapsing-pipeline segment, confirmed per cell) with the **per-cell peak split**
  (retained inputs vs output) and **expected coverage for ADR 338** (which cells it should move to pass, which need
  more), each backed by the measured peak split. **Not** a claim that the probe makes any cell pass.
- **The bounded-vs-substrate verdict:** can one narrow streaming segment move the sf=1 wall, or is the executor a
  larger substrate investment - with evidence.
- **A reason the chosen cut is first** (highest expected coverage / cleanest / fewest operators touched).
- **No engine rewrite, no implementation** in this ADR; the cut is ADR 338. sf=0.01 + the observable memory board
  unchanged.

## Risks
- **Hypothesis, not finding** - the collapsing-pipeline shape (and its intermediate nodes) is confirmed per cell in
  Phase 0; the cut follows the measured tree, not the assumed edge.
- **Measure retained inputs at the failing node** - not base-table inference; a late multi-way join can retain a
  large intermediate even when its base tables fit.
- **No overclaim** - 337 outputs *expected* coverage for 338, evidence-backed; it does not move cells.
- **Probe discipline** - map + measure + name; no streaming implementation here (ADR 282 build/run discipline,
  summary-not-dump, instrumentation removed before seal).

## Forward arc
ADR 338 = the first streaming cut (the named collapsing-pipeline segment), implemented + its *actual* coverage
measured against 337's expected coverage. If one bounded segment moves several cells, the streaming arc proceeds
boundary-by-boundary (the failure-atlas cadence). If the verdict is "broad substrate," 337's evidence reframes the
arc honestly (and logical completeness becomes the legitimate near-term pivot while the substrate is planned).

## Why toron-home
Engine grounding in `crates/toron` (exec.rs `execute_bound` + the join/aggregate boundary); driven via `toron-tpch`
reach at sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Operator's call: open streaming, but with a grounding probe, not a rewrite. Codex's
tightenings folded: treat join->aggregate as a **hypothesis** (the real cut is a collapsing **pipeline**
Join->Project/Filter/Expr->Aggregate); **measure the failing join node's immediate retained inputs**, not base-table
inference (late multi-way joins retain large intermediates); a **no-overclaim gate** - 337 produces evidence-backed
**expected** coverage for 338, not a pass claim. Measure-don't-predict, applied to the biggest scale lever.
