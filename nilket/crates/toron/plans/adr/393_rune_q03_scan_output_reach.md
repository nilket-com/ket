# ADR 393 - Rune q03 scan-output reach unblock

## Status
Proposed.

## Context

ADR 391 added a real `engine="rune"` perf row for q03. ADR 392 tried to use that row to measure the substrate ceiling
against DuckDB, but the measurement is still reach-blocked:

| scale | result |
| --- | --- |
| sf=1 | SQL and Rune complete and are stable; DuckDB is unstable/noisy at 37 ms median |
| sf=3 | DuckDB is stable at 93 ms median; Rune fails at `scan output accumulator`, 1,073,790,848 / 1,073,741,824 |

The directional sf=1 read is useful but not decisive: SQL/Rune is stable at 1.92x, while Rune/DuckDB is only a noisy
42.72x estimate. To turn that into a trustworthy substrate number, Rune q03 must complete at sf=3, where DuckDB is above
the noise floor.

This ADR is the reach unblock: diagnose why the Rune-authored q03 `auto_join` shape materializes a scan output, then
apply the existing scan-streaming substrate to that measured shape. It is not a performance optimization and it is not a
Rune-script rewrite.

## Decision

Implement a diagnostic-first scan-output reach fix for the Rune-authored q03 shape at persisted sf=3.

### Phase 0 - bound-plan producer table before code

Bind the SQL and Rune q03 logical plans through the same catalog/config and record the current physical shape:

| path | scale | plan fingerprint | chain extractor | stage/base/probe | materialized scan edge | consumer | estimate vs cap | why existing streaming missed | producer after | verdict |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sql | sf=1/sf=3 | measured | measured | measured | n/a/censored | measured | measured | baseline/control | n/a | control |
| rune | sf=1 | reconcile `0fd3784049e71920` isolation vs `2b72c561c2595512` perf | measured | measured | none today | measured | under cap/materialized | expected materialized | pass | small-scale control |
| rune | sf=3 | reconcile `0fd3784049e71920` isolation vs `2b72c561c2595512` perf | measured | measured | producer: `scan output accumulator` | measured | over cap | Phase 0 | re-measure | canary |

For the Rune sf=3 row, name exactly:

- why ADR 391's isolation fingerprint (`0fd3784049e71920`) and the perf-row fingerprint (`2b72c561c2595512`) differ:
  scale-gated physical path, isolation-vs-perf bind drift, or stale recorded evidence;
- which table/scan edge materializes into the accumulator;
- whether `extract_reoriented_inner_chain` matches or declines the Rune-authored shape;
- if it matches, which stage/base/probe edge is not routed through the existing 390 generic-base streaming path;
- whether `decompose_streaming_scan_probe_input` can decompose the edge;
- whether the memory-aware scan-output gate estimates the edge over cap;
- every `ScanRequest` field required by the edge (projection, PBC predicate, runtime filter, row identity, morsel, row
  limit, trace/cancellation).

The implementation follows this table. No code moves before the miss is named.

### The cut - topology-generic, not frontend-branded

If Phase 0 shows a recognizable execution-plan topology, widen the existing scan-streaming path for that topology:

- reuse `bound_scan_request` / direct `storage.scan`;
- reuse the 387-390 memory-aware gate so sf=1 stays materialized where it fits;
- route the streamed scan into the existing reoriented/inner-chain consumer instead of `materialize_scan_stream -> Vec`;
- preserve deterministic output order and the q03 oracle/order comparator.

The executor must not branch on "Rune", q03, engine name, or harness state. The shape can be motivated by Rune q03, but
the core change is topology-based and de-branded. `RUNE_Q3_SCRIPT` stays stable unless Phase 0 proves the script itself is
wrong; changing it to mimic SQL would collapse the ADR 391 isolation and does not answer the substrate question.

### Tight success definition

393's target success is:

1. Rune q03 persisted sf=3 reaches `phase=compare status=pass`.
2. The scan-output producer is gone.
3. A 392-redux Rune/DuckDB sf=3 ratio can be interpreted.

If the scan-output wall is removed but Rune advances to a named downstream producer, that is an honest wall move, not the
substrate measurement. The result document must say the substrate remains blocked and name the next reach producer.

If Rune passes and a ratio is produced, label it precisely: it is a **streaming-substrate** number, not a pure
materialize/kernel number. At sf=3, Toron may stream or spill earlier under its scan-output cap while DuckDB may still fit
the reference plan in memory. That memory-model difference is a real substrate cost and relevant to sf=10, but the
follow-up profile must decompose kernel time vs streaming/spill time before naming precision strikes.

## Acceptance

- **Phase 0 producer table recorded** for SQL/Rune q03, with the Rune sf=3 materialized scan edge and miss class named.
- **Rune q03 sf=3 canary:** persisted Parquet, fixed 4 GB:
  - preferred: `engine="rune"` reaches `phase=compare status=pass`; OR
  - accepted wall move: `scan output accumulator` gone and a named downstream producer recorded.
- **If Rune sf=3 passes:** rerun the 392 denominator pair (`--engines rune,duckdb`, runs=7/warmup=1) and record the
  stable `Rune/DuckDB` median ratio as a streaming-substrate ratio. If the ratio is large, name the next
  substrate-profile ADR and require it to separate kernel/materialization cost from streaming/spill cost.
- **Correctness:** Rune pass rows require the q03 oracle/order comparator. SQL correctness remains the persisted
  correctness command, not a perf digest inference.
- **sf=1 no-regression control:** Rune q03 sf=1 remains on the materialized path or is proven not slower outside the
  noise band; SQL/Rune small-scale ratio is not worsened by an unconditional streamed path.
- **Shape fence:** non-matching reoriented/inner-chain tests stay materialized; no broad "stream all scans" recognizer.
- **ScanRequest parity:** no dropped projection/PBC/runtime-filter/row-identity/morsel/limit/trace/cancellation fields.
- **No cap bump; no budget constant changes.**
- **Full gates:** `fmt`, `check -p toron --all-targets`, `test -p toron-tpch`, `test -p toron --no-fail-fast`, plus the
  measured sf=3 perf/correctness commands.
- **No qNN / no frontend branding in `crates/toron/src`**; large `.perf.data` uncommitted.

## Guardrails

- **Reach unblock, not speed claim:** q03 becoming faster is incidental; 393 exists to make Rune q03 complete at sf=3 or
  name the next reach wall.
- **Do not rewrite Rune q03 to dodge the wall:** the logical-shape probe must stay meaningful.
- **Topology-based core change:** no `engine == rune`, q03, or harness-specific branch in executor code.
- **Memory-aware path selection:** sf=1/small-scale must not pay the streamed path when materialization fits.
- **No cached evidence:** use live perf/correctness rows, not matrix/correctness-matrix.
- **Wall-move honesty:** scan-output gone is not green unless the oracle/order comparator passes.

## Risks

- **Rune q03 shape is not the same family as SQL q03 after 390:** then 393 may be a Rune-specific topology cut, but still
  de-branded and shape-fenced.
- **Scan-output removal exposes another reach wall:** acceptable; record the named producer and keep the substrate
  measurement blocked.
- **Small-scale regression:** sf=1 control and memory-aware gate catch unconditional streaming.
- **Over-broad recognizer:** shape-fence tests prevent unrelated plans from changing.
- **Ratio remains unstable:** if Rune reaches but either row is unstable, repeat/escalate according to the 356 rail rather
  than reporting a false substrate number.

## Forward Arc

If 393 makes Rune q03 pass at sf=3, immediately run the 392-redux table and interpret `Rune/DuckDB`:

- small ratio -> reach was the main blocker; continue reach-first for sf=10;
- large ratio -> profile the Rune path with the 381 method, decompose kernel/materialization vs streaming/spill time, and
  then start the right substrate precision strikes;
- large SQL/Rune after the fix -> planner/logical-shape remains a parallel lever.

If 393 wall-moves instead of passing, the next ADR is the named reach producer. The substrate ceiling remains unmeasured
until Rune q03 produces a correct row at a stable DuckDB scale.

## Grounding evidence

ADR 391/392 fingerprint tension to reconcile: ADR 391 isolation proof recorded Rune q03 `0fd3784049e71920`, while
ADR 392 perf rows recorded `2b72c561c2595512`. ADR 392: sf=1 SQL/Rune stable 1.92x; sf=1 DuckDB unstable; sf=3 DuckDB
stable 93 ms; sf=3 Rune fails `scan output accumulator` 1,073,790,848 / 1,073,741,824. Rune q03 script:
`RUNE_Q3_SCRIPT` in `crates/toron-tpch/src/lib.rs`, `auto_join` calls. Existing scan-streaming substrate:
`extract_reoriented_inner_chain`, `reoriented_inner_stage_scan_build_exceeds_scan_output_cap`,
`prepare_reoriented_inner_join_runtime_with_streamed_scan_build`,
`probe_reoriented_inner_join_streaming`, and ADR 390's generic-base streamed runtime consumption in
`crates/toron/src/exec/join/streaming.rs`.
