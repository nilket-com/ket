# ADR 339 - streaming substrate sizing probe (q07/q21)

## Status
Proposed.

## Context

The scale arc continues, but the next move is a **sizing/design probe, not a substrate rewrite.** ADR 338 proved the
streaming primitive on the *direct* segment (q14/q19: Join -> global aggregate). q07/q21 are different: a **deep join
pipeline with exchanges feeding a grouped aggregate**, with the wall at `Join(0)` (~1.975 GB output) far below the
collapse. This ADR maps the minimal streaming interface, names the **first executable boundary**, and produces a
**staged plan with expected cell movement** - so the first *implementation* (ADR 340) is the smallest real substrate
cut, not "stream everything."

### Grounded (ADR 337 + executor mechanics)
- q07/q21: `Sort -> Exchange -> Project -> Aggregate(grouped) -> Exchange -> ... -> Join(i) -> Exchange -> ... ->
  Join(0)` (q21 adds `LeftAnti`/`LeftSemi` joins). Failing node `Join(0)`; the collapse is several joins + exchanges
  *above* it.
- Every boundary materializes: exchange (`bound_exchange`, exec.rs:2115, post `execute_bound`), each join
  (`execute_bound(left/right) -> Vec`), the grouped aggregate (`bound_grouped_aggregate`, 1964).
- A hash join **holds its build side**, **iterates its probe side** (`execute_hash_join`, 2928). So streamability turns
  on which side the large intermediate lands on.

## Decision - produce the build/probe map (the gate), name the first boundary, stage the plan

### Gate 1 (load-bearing): the per-join build/probe map
For q07/q21, the decisive measurement is whether the large fact/intermediate stays on the **probe** side through the
chain:
- **If probe-side throughout** -> ADR 340 can be a **bounded probe-side pipeline cut** (the fact streams through joins
  against held small builds, into a folding grouped aggregate).
- **If any large intermediate becomes a build side** -> ADR 340 must **first fix or route around that boundary**
  (e.g. re-orient the join where semantically safe, or stream that build) - otherwise streaming just **moves the
  wall** to that build. q21's semi/anti joins are fenced: no blind side swap across non-inner semantics.

### Gate 2: exchange behavior
- **Pipeable exchange** (partition + forward without materializing) -> a **staged substrate** (join cuts compose over
  a streaming exchange).
- **Materializing exchange** -> the first cut may need **exchange streaming before more join work** (the exchange is
  the foundational boundary). Existing execution already materializes; the probe must distinguish "currently
  materializes" from "locally pipeable if implemented."

### The deliverable table (explicit - this is what keeps 340 from being a guess)
Per query, **one row per join** in q07/q21's pipeline:

| query | stable_join_id | join_kind | strategy | build_side | probe_side | build_retained_bytes | probe_retained_bytes | output_estimate_bytes | downstream_consumer | exchange_before/after | streamable_as-is? | gate_verdict |

The bytes must be measured at the actual sf=1 bound/optimized boundary from the retained child batches
(`get_array_memory_size` or equivalent). Base-table footprint and query-text inference are not sufficient: ADR 337
already proved they mis-size the failing node.

Plus the exchange rows (currently materializing vs locally pipeable) and the grouped-aggregate fold feasibility:
does ADR 338's init/fold/finalize extend to grouped state, and what are the observed group cardinality + state bytes
for q07 nation/year and q21 supplier-name groups?

## Acceptance - a sized design (no implementation, no pass claim)
- **The build/probe table** (above) for q07/q21, with observed retained bytes - the Gate-1 evidence.
- **The q17 control map**, with the same table shape but explicitly non-driving.
- **The exchange verdict** (pipeable vs materializing) - the Gate-2 evidence.
- **The first executable boundary named**, classified by which gate-branch it falls in (bounded probe-pipeline cut /
  fix-a-build-boundary-first / exchange-streaming-first), and **whether it moves q07/q21 without broad executor
  replacement**.
- **A staged plan** (the substrate cut sequence) with **expected cell movement per stage** (337's expected-coverage
  discipline).
- **The minimal interface** named (extend-338's chunk-fold vs a new probe-side stream), with what each stage touches
  so ADR 340's size is known.
- **No implementation, no rewrite, no pass claim** - 339 designs + sizes.

## Scope
- **q07/q21 first** - shared ~1.975 GB output wall, clearest join-to-join/exchange candidates.
- **q17 control/secondary** - map it, but its correlated-scalar branch (Join LeftOuter + scalar recombine, ADR 337)
  may need different handling and must **not** drive the q07/q21 design.
- **Explicit non-goals:** q12/q18 retained-input walls; q04/q20 spill-writer walls.

## Risks
- **The map is the gate, not background** - if the build/probe table is skipped or assumed, ADR 340 becomes a guess
  and streaming may just move the wall.
- **Build-side intermediate** - if found, the first cut changes shape (fix that boundary first); the table must catch
  it.
- **Exchange materialization** - if the exchange materializes, it may be the foundational first cut, before join work.
- **Probe discipline** - measure + design; no streaming implementation here (ADR 282 build/run, summary-not-dump,
  any instrumentation removed before seal).

## Forward arc
ADR 340 = the smallest real substrate cut named here (probe-side pipeline / build-boundary fix / exchange streaming),
implemented + its actual coverage measured against 339's expected. The substrate then proceeds boundary-by-boundary
(the failure-atlas cadence) until q07/q21 (and then q17) move, or until a stage's expected-vs-actual reframes the arc.

## Why toron-home
Engine grounding/design in `crates/toron` (exec.rs join pipeline / exchange / grouped aggregate); driven via
`toron-tpch` reach at sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Operator's call: continue scale, but a substrate **sizing probe**, not a heroic
implementation. The **build/probe map is Phase 0's gate** (probe-side-throughout -> bounded pipeline cut;
build-side-intermediate -> fix that boundary first), with **exchange behavior the second gate** (pipeable -> staged;
materializing -> exchange streaming first), and an **explicit per-join table** (stable join id, join kind/strategy,
build/probe side bytes, output estimate, downstream consumer, exchange adjacency, gate verdict) as the deliverable
that keeps ADR 340 from guessing. q17 is mapped as a control, but cannot widen the q07/q21 first cut.
Measure-don't-predict, applied to the substrate.
