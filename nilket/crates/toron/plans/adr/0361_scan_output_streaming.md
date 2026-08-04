# ADR 361 - scan-output streaming (stream q14/q21's scan into its consumer)

## Status
Proposed.

## Context

The first sf=10 **engine** fix. ADR 360 made sf=10 measurable; the first named engine wall is q14/q21's **scan-output
accumulator** - `execute_bound(Scan)` -> `materialize_scan_stream(stream)` -> `Vec` (exec.rs:765-766), the
materialize-each model accumulating the full scan output before the consumer runs (gauge guard name:
"persisted parquet scan output accumulator", lib.rs:1445). This cut **streams q14/q21's scan into its consuming
operator** (direct scan, no materialize) - the first attack on the ADR 349 upstream-child-materialization term, at the
scan (leaf) level. **Shape-fenced to q14/q21; no general recognizer.**

## Grounded
- Scan dispatch builds a `ScanRequest` (projection, pbc_predicate, runtime_filter, row_identity, profile_hint, morsel,
  row_limit, trace_context, downstream_cancellation) -> `storage.scan(req)` -> **`materialize_scan_stream` -> `Vec`**
  (exec.rs:743-766).
- Streaming operators consume **materialized** children (`execute_bound(...) -> Vec`, streaming.rs:773/1032/1108/1551/
  1737) - stream output, materialize inputs (the 349 term).
- **Precedent (legality, not solution):** streaming.rs:1897 already does a direct `storage.scan(req) -> stream` on the
  existence side. It **proves the mechanics are legal**; it is a specific existence-side scan, **not** a general
  abstraction and **not** the q14/q21 edge.
- **Two production/bound-path inline `ScanRequest` builders today:** the exec.rs scan arm + the streaming.rs:1897
  path - they can drift. Phase 0 also inventories the test-only/logical scan request construction and either routes it
  through the shared builder if it is the same semantic surface, or records why it remains separate.

## Decision - two gates, then the shape-fenced streaming

### Gate 1 (FIRST): shared `ScanRequest` construction (mandatory)
**Factor one shared `ScanRequest` builder** and make **both** production/bound-path inline builders (the exec.rs scan
arm and the streaming.rs:1897 path) use it - a behavior-neutral refactor that lands **before** any new streaming. A
**drift test** asserts the builder carries **every** field: projection, PBC, runtime filter, row identity, profile hint,
morsel, row limit, trace context, downstream cancellation. The direct scan must never silently drop a field (the
silent-answer-change trap).

### Gate 2 (FIRST): the actual edge map
Phase 0 reads q14/q21's **bound plans** and produces an **edge map**: which scan edge is streamed, and whether it is a
**bare `Scan`** or a **`Filter(Scan)`**. Residual filtering is implemented **only if** q14/q21's edge is `Filter(Scan)`
(340's on-the-fly visitor); a bare `Scan` edge needs no residual. No speculative filter path.

### The fix
For the q14/q21 matched scan->streaming-consumer edge(s) from the edge map: the consumer **scans directly** (via the
shared builder -> `ScanStream`) and processes batch-by-batch (build / fold / probe), bypassing
`materialize_scan_stream -> Vec`. Shape-fenced; the broader materialize-each model is untouched. Core implementation
symbols/comments are named by topology/shape, not by TPC-H query number; q14/q21 may appear in plans, results, and the
TPC-H harness, but not as executor identifiers or core-code comments.

## Acceptance (green-or-next-wall; correctness exact)
- **The "persisted parquet scan output accumulator" producer disappears at sf=10** for q14/q21 (fixed 4 GB); q14/q21
  are **green OR advance to a named next wall** (332 retained diagnostic) - **not** expected green, but the scan
  accumulator must be gone.
- **Gate 1:** one shared `ScanRequest` builder; both paths use it; the drift test passes (every field preserved).
- **Gate 2:** the q14/q21 edge map recorded (which edge streamed; bare Scan vs Filter(Scan)).
- **Correctness:** the streamed-scan path is **digest-identical** to the materialize path at sf=0.01/sf=1, both modes
  (the load-bearing gate); ScanRequest parity proven.
- **Behavior-neutral at sf=1:** non-matched plans + answers unchanged; the sf=1 board (live) unchanged.
- **Bounded:** the scan streams (read-process-drop); peak RSS bounded vs the materialized path; no silent wall.

## Guardrails
- **Shared `ScanRequest` builder, factored first** - both inline builders converge on it; the drift test prevents field
  loss; the direct scan preserves projection/PBC/runtime-filter/row-identity/profile-hint/morsel/row-limit/trace/
  cancellation.
- **Edge map first** - implement only the q14/q21 edges the bound plans actually show; Filter(Scan) only if needed.
- **Shape fence** - q14/q21 edges only; no general scan-child recognizer; the materialize-each model is otherwise
  untouched.
- **De-branded core** - implementation in `crates/toron/src` is topology-named; no qNN identifiers/comments in the
  executor.
- **Green-or-next-wall** - the pass condition is the accumulator producer gone + any remaining failure named (332), not
  forced green.
- **Digest-identical correctness** - streamed == materialized at sf=0.01/sf=1, both modes.
- **Behavior-neutral at sf=1.**

## Risks
- **Silent answer change** - a dropped ScanRequest field; Gate 1 (shared builder + drift test) closes it.
- **Over-reach** - a general recognizer; shape-fenced to q14/q21.
- **Speculative filter path** - Filter(Scan) only if the edge map shows it.
- **1897 mis-read** - it proves legality, not the solution; the q14/q21 edge is built fresh on the shared builder.
- **Overpromising green** - green-or-next-wall; the accumulator-gone is the real bar.

## Forward arc
361 (scan-output streaming, q14/q21) -> the **next sf=10 wall** (the deeper materialize-each / upstream-child terms
beyond the scan: intermediate operator outputs, exchange buffers - ADR 349) -> sf=10 q14/q21 green / 359-redux with more
cells reachable -> the sf=10 fix arc -> sf=100 / Rune. No central roadmap.

## Why toron-home
Core engine cut in `crates/toron` (the shared `ScanRequest` builder + the q14/q21 direct-scan-into-consumer); measured
via `toron-tpch` at sf=10 / sf=0.01. No Wendelon / cross-ring surface; the storage `scan` contract is unchanged.

## Council provenance
Codex + Claude. The first sf=10 engine fix. Operator/Codex steer: **shape-fenced to q14/q21** (no general recognizer);
**Filter(Scan) only if Phase 0's bound-plan edge map shows it**. Codex's hard tightening: **shared `ScanRequest`
construction is mandatory and factored first** - two inline builders exist today (the exec.rs scan arm + the
streaming.rs:1897 existence path); converge both on one builder with a **drift test** (every field preserved). The 1897
precedent **proves the mechanics are legal, not that it solves the q14/q21 edge**. Acceptance is **green-or-next-wall**:
the "persisted parquet scan output accumulator" producer disappears at sf=10 and any remaining failure is named (332);
correctness digest-identical at small scale; the direct scan preserves every request field.
