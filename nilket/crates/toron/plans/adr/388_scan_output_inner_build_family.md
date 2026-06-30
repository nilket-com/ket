# ADR 388 - scan-output streaming for the multi-table inner-build family (q03/q05/q10)

## Status
Proposed.

## Context

ADR 386 ranked the next repeated producer; 387 cleared q08 (bare Exchange(Join(0)) base) to time. **q03/q05/q10 remain
`scan output accumulator` walls with a distinct miss** - they are **multi-table joins with a spilled inner build**
(q03: customer/orders/lineitem; q05: 6-table; q10: 4-table), each showing `spill_bytes=1,434,880,384` (1.43 GiB) behind
the scan-output wall (386). Their materializing scan is a **deeper edge** than 387's base classification covered.
Diagnostic-first; q03 canary; reuse 387's memory-aware gate. The focus is **scan-output for the multi-table inner-build
family - not a promised green**: a wall-move to the named downstream producer (likely the 1.43 GiB spill-volume) is an
expected, useful result.

## Decision

### Phase 0 - producer table for q03/q05/q10 (NOT q08 reuse)
| query | current producer | canary/control | consumer shape | miss class | producer after | spill before -> after | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q03 | `scan output accumulator` | canary | multi-table join + spilled inner build + group | (Phase 0) | (re-measure) | 1.43 GiB -> ? | canary |
| q05 | `scan output accumulator` | coverage | 6-table join + spilled inner build + group | (Phase 0) | (re-measure) | 1.43 GiB -> ? | covered-if-shared |
| q10 | `scan output accumulator` | coverage | 4-table join + spilled inner build + group | (Phase 0) | (re-measure) | 1.43 GiB -> ? | covered-if-shared |

For q03: **which scan materializes** into the accumulator (which table, where in the chain); **which consumer** receives
it (the spilled inner build / the multi-table join); whether a streamed recognizer **matched but left this edge
materialized** or **missed the shape**; persisted-vs-lower-scale plan difference; the `ScanRequest` fields the edge needs.
For the memory-aware selector, record the estimated scan-output bytes, the cap, and the selected path for the sf=10
canary and the sf=1 control. This proves the gate streamed because the edge was memory-bound, not because the shape was
unconditionally routed to the slower path.

### The cut
- Extend the scan-streaming (387 reoriented-inner mechanism + `bound_scan_request`/`storage.scan`) to q03's measured
  scan edge, into its multi-table-join/spilled-inner consumer, bypassing `materialize_scan_stream -> Vec`.
- **Reuse the 387 memory-aware gate** so **sf=1/small-scale stays on the cheaper materialized path**; stream only when
  memory-bound at sf=10.
- Shape-fenced to the measured q03/q05/q10 family; no general recognizer. Topology-named; no qNN.

### Tight green (369/387) + characterized wall-move
- **green:** scan-output gone AND no downstream producer AND live persisted sf=10 correctness.
- **advanced:** scan-output gone, a named downstream producer appears - **record the downstream producer's
  `spill_bytes` before and after** for each wall-moved row. This proves whether removing scan-output exposes the **same
  1.43 GiB producer unchanged**, **increases it**, or **changes the spill shape** - which tells us whether the next cut
  is the 1.43 GiB spill-volume family or a different spill-budget term.
- **unchanged:** same scan-output producer remains.

## Acceptance
- **Phase 0 producer table recorded** (q03/q05/q10 miss diagnosed, not q08-assumed).
- **q03 canary:** green (scan-output + downstream gone + persisted correctness) OR advanced (named downstream) OR
  unchanged with the measured reason.
- **q05/q10 re-measured live**, classified green/advanced/unchanged; claim only measured cells.
- **Spill bytes before/after for every wall-moved row** - same/increased/changed-shape, with the producer named (the
  characterization of the next cut).
- **Memory-aware gate:** sf=1/small-scale stays materialized; a sf=1 control proves no small-scale perf regression.
  Record estimated bytes, cap, and selected path for the sf=10 canary and sf=1 control.
- **Correctness:** persisted sf=10 `pass@compare` for any new green; small-scale strict/dev parity (column order,
  residual/filter, multi-table join + group); ScanRequest parity (no dropped fields).
- **No cap bump; `--no-fail-fast` + `toron-tpch --no-fail-fast` required** (engine cut); de-branded; prior greens
  (q04/q06/q14/q19 + carried) unchanged; q08's wall-moved state unchanged.

## Guardrails
- **Diagnostic-first; q03/q05/q10 miss diagnosed, not q08-assumed.**
- **Name/focus is the family, not a green** - wall-move to a named producer is success.
- **Spill before/after for wall-moves** - characterize the exposed producer (same/increased/changed), don't just label
  "spill-volume".
- **Memory-aware gate** - small-scale stays materialized; stream only when memory-bound.
- **Tight green; shape-fenced; ScanRequest parity; no cap bump; full engine gate; de-branded; no qNN.**

## Risks
- **q03 miss differs from q05/q10** - then 388 is q03-only + a split table; acceptable.
- **Wall-move exposes a different spill-budget term** (not the 1.43 GiB) - the before/after spill bytes catch it and
  name the real next producer.
- **Persisted-plan wrapper drift** - shared `bound_scan_request` + parity tests.
- **Small-scale perf regression** - memory-aware gate + sf=1 control.

## Forward Arc
388 names whether the q03/q05/q10 family advances to the **same 1.43 GiB spill-volume** (-> a spill-volume family cut,
reuse 348/363/372 projection or 378 grouping) or a **different spill-budget term** (-> that producer). Then q21's tiny
resident wall, q12's 8.4 GiB partition-group residual, q07/q08 time cluster. The 386 ranking + live re-measure + the
before/after spill characterization drive the order.

## Grounding evidence (file:line)
386: q03/q05/q10 `scan output accumulator` ~49 KiB over 1 GiB, `spill_bytes=1,434,880,384` behind the wall. 387 base:
`reoriented_inner_chain_grouped_base_scan_probe_mode` streaming.rs:756, memory-aware gate
`reoriented_inner_stage_scan_build_exceeds_scan_output_cap` 4002/6611. q03 = customer/orders/lineitem + group
(`Q3_DEV_SQL` lib.rs). shared builder: `bound_scan_request`. q03 DuckDB=269ms.
