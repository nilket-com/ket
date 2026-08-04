# ADR 357 - q08/q09 reoriented-inner-chain generalization

## Status
Proposed.

## Context

ADR 356's fresh-process perf rail exposed a live sf=1 gap that the cached matrix hid: strict q08 and q09 failed at
`join output materialization` with a ~1.98 GB requested output against the 1 GiB effective output cap. The existing
matrix/correctness-matrix commands rendered cached JSONL rows and did not prove a live sf=1 board.

The failing shapes are not new logical features and not keyless joins. They are keyed multi-way inner-join chains under
a grouped aggregate, but the streaming helper added in the scale arc was too narrow: it matched the q07-style topology
only. q08/q09 need the same shape-named primitive generalized to a different inner-chain orientation:

- the replay ids are not fixed to the q07 chain;
- the growing intermediate can appear on either side of the next join;
- the grouped aggregate may sit above `Project(Filter(Join))` or `Project(Join)`;
- the base `Join(0)` may be filtered or bare;
- when the physical probe/build sides are swapped, the output columns must still match the bound plan's observable
  schema.

This is a real engine behavior change. It must not ride in ADR 356, whose contract was measurement-only.

## Decision

Generalize the existing reoriented-inner-chain streaming path by topology rather than by a TPC-H query shape:

- infer a consecutive all-Inner / HashPartitioned chain down to base `Join(0)` instead of hard-coding replay ids;
- follow the growing streamed intermediate whether it is the left or right child at each step;
- preserve optional lower and upper filters, including the no-upper-filter `Project(Join)` case;
- stream a filtered or bare base `Join(0)`;
- carry enough side-order metadata to restore the original bound-plan column order during materialization;
- keep the matcher shape-named and de-branded in `crates/toron/src` (no q08/q09 identifiers or comments).

The guardrail is the same one used throughout the streaming substrate: widen only the proven inner-chain topology. Do
not change the optimizer/cost model, do not introduce a general execute-stream rewrite, and do not broaden semi/anti or
outer-join semantics.

## Acceptance

- Live release strict q08 and q09 at sf=1 pass after the fix.
- q08/q09 correctness remains green at small scale, and the sf=1 perf rail includes both as passing rows.
- The widened matcher has discriminating unit coverage:
  - variable replay-id chain, not q07-only;
  - growing chain on the original right side;
  - optional top filter and bare `Join(0)`;
  - build-first/probe-first output column restoration.
- Existing streaming cells remain green: q07, q14, q17, q18, q19, q21.
- No query-number branding in `crates/toron/src`; docs may name q08/q09 because the ADR is TPC-H-scoped.
- ADR 356 remains the rail-only commit; ADR 357 owns this behavior change and the first clean all-22 sf=1 baseline.

## Guardrails

- **No cost-model change.** This is executor-local streaming shape recognition.
- **No query-specific symbols.** Core code names the shape, not TPC-H query numbers.
- **No hidden schema reorder.** If the streamed probe is the original right side, materialization restores the bound
  plan's original left-then-right schema.
- **No matrix-cache proof.** Live release `run` / `perf` evidence is required for sf=1.

## Forward

After ADR 357, ADR 356's perf rail has a clean all-22 sf=1 baseline. The next perf-road item is matrix hygiene:
distinguish cache-rendered status from live scale verification, or route scale claims through the perf rail explicitly.
