# shared-fold family grounding at 1d901bb2 — verdict: false family

The operator's grounding contract for the last multi-cell board thread, executed with existing
telemetry only (node attribution + the generic grouped-fold profile dir; no new probe was
needed, so the contract's throwaway-probe clause went unused). Fresh instrumented runs at the
exact coordinate, sealed `1d901bb2` binary.

## Invocation ledger

| tenant | route label | envelope | fold-kernel time | kernel implementation | input rows → groups | key shape | aggregates |
|---|---|---:|---:|---|---|---|---|
| q18 body | `scan_grouped` | 2,388 ms | inside envelope, unsplit | **`PrimitiveInt64Groups`** (flat SoA: `Vec<u32>` buckets / `Vec<i64>` keys / `Vec<i128>` totals) | 47,989,007 → ~12.0M | single Int64 (`l_orderkey`) | SUM(Decimal128) |
| q18 outer | `uncovered_generic_fold` | 2,091 ms | **222 us** (group-expr 3 us, finalize 86 us) | `BoundGroupedAggregateGroups` (generic) | 3,507 → 501 (5 group columns) | multi-column | SUM |
| q13 preagg | `scan_grouped` | 3,040 ms | **2,270 ms** fold + 136 ms finalize | `BoundGroupedAggregateGroups` (generic) | 11,869,667 → 799,982 | single Int64 (`o_custkey`) | COUNT |
| q20 reduced | `uncovered_generic_fold` | **14 ms** | — | — | ~517K post-518 | — | — |

Supporting facts: q18 span 7.946 s (aggregate node 4.48, join 1.40, exchange 1.39); q13 span
3.183 s (aggregate 3.05); q20 span 2.109 s (scan-dominated, 1.10). q13's preagg profile:
15,542,506 hash probes over 11.87M lookups, 11 growths, 220.7 MB resident, max chain 33.
q18's outer profile: 3,507 lookups, 4,136 probes, 501 groups, 242.9 KB resident.

## Source resolution (the key question, answered in code, not labels)

- `uncovered_generic_fold` is not a handler: it is the **default label** for any aggregate
  invocation that never set a route (`invocation.route.unwrap_or("uncovered_generic_fold")`,
  node_attribution.rs:2392). The label means uninstrumented, not generic-kernel.
- The generic fold converges: `scan_grouped` streaming, the join-runtime fold sites, and the
  plain bound-aggregate path all call `fold_bound_grouped_aggregate_batch` →
  `BoundGroupedAggregateGroups` (exec/aggregate.rs:2050; group_table.rs).
- **q18's body fold does not use that kernel.** Its shape (single Int64 key, Decimal128 SUM)
  commits `PrimitiveInt64Groups` — a separate flat SoA implementation (aggregate.rs:276),
  default-on since the ADR 494/495 era — confirmed empirically: the 48M-row body fold wrote
  no generic-profile artifact while the 3,507-row outer fold did.
- q18's outer envelope is ~100% non-fold: 2,091 ms charged to the invocation against 222 us
  of kernel time — handler-side input pull/materialization on the commuted spine, exactly the
  contamination the grounding contract's prior predicted.

## Classification and the arithmetic kill

Per the contract, gains cannot be summed across cells; q18 is the only possible authorizing
cell, and only if one mechanism genuinely improves both of its folds. The two q18 folds share
**no kernel** (primitive SoA vs generic table) and the outer fold has **no kernel cost to
remove** (222 us). The q18 shared removable envelope reachable by one mechanism without
query-specific logic is therefore ~0.2 ms — below the 3,554 ms floor by four orders of
magnitude. Per the predeclared rule, **the family closes immediately**.

Verdict: **a false family — the same label over unrelated costs.** The five-second "fold
family" decomposes into: a primitive-kernel envelope (q18 body, 2.4 s, sub-floor), a
materialization envelope mislabeled as a fold (q18 outer, 2.1 s, sub-floor, not folding), an
independent generic-kernel cell (q13, 2.27 s kernel, sub-floor, cannot subsidize), and a
vacant label (q20, 14 ms). The operator's prior — that the outer 2.28 s contained non-fold
work and the true shared kernel envelope sat materially below 5.03 s — is confirmed in the
strongest form: the shared envelope is not merely below 5.03 s, it is effectively zero.

## Named, non-actionable observations for the record

- q13's preagg (single-Int64 COUNT) runs on the generic kernel although a primitive Int64
  count path exists; whatever declines it (the residual filter shape or the COUNT-argument
  form) is a sub-floor coverage note (~1-1.5 s ceiling on a 3.0 s envelope), independent to
  q13, and does not authorize a cut under the floor discipline.
- q18's outer 2.09 s materialization envelope is real cost with a real owner (the commuted
  spine's uncovered handler path), but it is a materialization thread, not a fold thread, and
  it is sub-floor alone.

## Board consequence

With this family closed, every remaining named opportunity on the board is below the fixed
3,554 ms floor per cell: q21 parked (9.9 s gap, cache-bound residual), q05 parked (48M-touch
family closed at 522/523), q09 parked (lifecycle bound proven unreachable at 526), fold
family closed here. The floor has exhausted this era of the board.

Artifacts: `target/tpch/fold_ground_{q18,q13,q20}_1d901bb2/`,
`target/tpch/foldprof_{q18,q13}_1d901bb2/`, profile JSONs preserved in the session job
directory.
