# ADR 402 - Grouped-aggregate exchange placement (q03)

## Context

401 closed the fixable q03 SQL/Rune planner-shape delta (side-local filter placement) and closed the same-machine
gap from `2.92x` to `1.78x`. It named one remaining structural difference as a stop-line: SQL binds
`Aggregate(input=Exchange(parts=4, keys=group_keys, input=Join))` while Rune aggregates **directly over the final
join** with no exchange. 402 closes (or diagnoses and pivots off) that last q03 shape residual.

**Grounded premise (not "add a cost model"):** the delta is a partition-count *choice*, not a different operator.
SQL grouped-aggregate lowering unconditionally wraps the aggregate input in a hash exchange with a constant partition
count (`sql_planner.rs:3974`/`:6091`, `DEFAULT_AGGREGATE_PARTITION_COUNT = DEFAULT_JOIN_PARTITION_COUNT = 4`,
`sql_planner.rs:58-60`). Rune's `aggregate()` builder emits the `Aggregate` node directly over its input with no
exchange (`rune_engine.rs:393-401`). The binder binds each node faithfully (`binder.rs:1288-1318`,
`bind_bound_aggregate`). There is **no two-phase (partial/final) aggregate** in the engine today.

The hash exchange exists so each group's rows co-locate in one partition, letting each partition aggregate its groups
independently **without** a partial/final combine. On an aggregate input that is **structurally proven** either
single-partition or already hash-co-located by the group keys, that repartition buys nothing and is pure overhead; on a
genuinely multi-partition input without that co-location proof, removing it is unsafe absent a two-phase aggregate
(groups would split across partitions and never recombine). The local gauge running in one process is not, by itself,
that proof: 402 needs a plan/fragment partitioning proof, or it must stop.

## Decision

### Phase 0 - strict stop-line (before any fix)
Two gates; **both** must pass to proceed to Phase 1. Either failing seals a diagnostic and pivots to Thread 3
(execution-model / streaming-completion).

1. **Materiality.** Measure whether the `parts=4` pre-aggregate exchange is a material fraction of the q03 SQL/Rune
   gap. Probe `partition_count=1` as a lower-bound on exchange cost (it still may execute repartition work), and, if
   feasible, a true throwaway elision that removes the exchange entirely on the measured q03 path. Run q03 sf=1
   perf same-machine and compare medians to current `parts=4`. **If true elision moves the SQL median within the
   runs=7 noise band, the exchange is not the residual -> stop-line, pivot to Thread 3** (the residual is
   substrate/execution, not this placement). A `parts=1` improvement is evidence for materiality, not shape closure.
2. **Elidability.** Prove the collapse is correctness-neutral: there is a **sound, structural co-location property**
   (single-partition, or hash partitioning by columns/equi-key classes covered by the aggregate group columns; never a
   cardinality/row/byte estimate) that gates the collapse, and the genuinely-distributed/multi-partition
   grouped-aggregate path is left untouched. Phase 0 must first census the available facts; current binder optimizer
   facts are row/byte/origin facts, not partitioning proof. **If a correct elision requires adding a new
   partitioning-provenance fact, that fact must be minimal, behavior-neutral, and distributed-test-gated; if it requires
   a real partial/final two-phase aggregate design, that is a mechanism, not a placement tweak -> stop-line, pivot to
   Thread 3.**

Record both measurements + the classification in the results doc **before** code.

### Phase 1 - the rule (only if both Phase 0 gates pass)
A **generic pre-aggregate exchange-placement rule**: remove a hash `Exchange(parts=N, keys)` feeding an `Aggregate`
(or `Distinct`) **only when the exchange input is provably co-located for the group keys**. Generic; no q03 branch. A
`partition_count=1` rewrite is acceptable only if the executor proves it is a no-op/bypass for this shape; otherwise it
remains a materiality probe, because `Exchange(parts=1, keys)` still evaluates keys and repartitions today. Candidate
loci (Phase 1 picks the one where the co-location proof is cleanly and soundly available):
- **binder normalization** - collapse `Aggregate(input=Exchange(co-located input))`; covers grouped-aggregate
  **and** the sibling `Distinct` exchange (`binder.rs:1279`) uniformly, and does not fire for Rune (no exchange to
  collapse), so Rune is unchanged by construction;
- **SQL grouped-aggregate lowering** (`sql_planner.rs:3974`/`:6091`) - skip the exchange when the input is
  co-located (grouped-aggregate only; `Distinct` would need the same edit).

The gate keys on a **partitioning/co-location proof**, never on cardinality. If the proof is unavailable/unsound ->
stop-line.

### Stop-line (either Phase 0 gate fails)
Seal a diagnostic (named delta + why: exchange immaterial, or two-phase required) and pivot to the Thread 3 big-rock
ADR. No two-phase aggregate mechanism and no cost model are built under 402.

## Acceptance
- **Phase 0 recorded:** the materiality delta (parts=4 vs parts=1, same-machine) and the elidability classification
  (co-location-safe vs needs-two-phase), before code.
- **Either** the generic placement rule **or** a sealed diagnostic (stop-line) with the named reason.
- **Shape closure (not only speedup):** SQL's bound q03 plan drops the pre-aggregate exchange so it matches Rune's
  direct-aggregate shape on that aspect (fingerprint/shape convergence); Rune's fingerprint stays
  `666e0ac028036f70`. A `parts=1` exchange is not banked as closure unless the executor bypasses it and the proof is
  recorded.
- **Distributed-aggregate correctness gate:** a test proving a genuinely multi-partition grouped-aggregate input
  **keeps** the hash exchange unless co-location is structurally proven; existing distributed
  grouped-aggregate architecture coverage stays green.
- **q03 SQL/Rune improvement:** SQL q03 sf=1 absolute median improves beyond the runs=7 band **and** `SQL/Rune` closes
  further below `1.78x` toward 1x (both Toron rows stable). **Rune unchanged (control).**
- **No sibling regression** - q05/q10 (grouped aggregates) correctness + shape unchanged or improved; if the rule is
  the binder-normalization locus, `Distinct` shape checked too.
- **Full engine gate:** `--no-fail-fast` both crates; no cap bump; de-branded; no committed `.perf.data`.
- **Hard escape hatch:** if Phase 0 fails either gate (exchange immaterial or two-phase required), the diagnostic +
  pivot to Thread 3 **is** the accepted outcome - no shape-only cut is banked.
- **Scope honesty:** closes the q03 grouped-aggregate exchange residual on the single-node path; **not** an sf=10
  heavy-cell fix, **not** a two-phase aggregate, **not** a cost model.

## Guardrails
- **Placement policy, not cost model** - the rule keys on a structural co-location proof, never cardinality; if it
  needs cardinality, stop-line.
- **`parts=1` is only a probe unless proven no-op** - shape closure means no pre-aggregate repartition work, not merely
  a one-bucket exchange.
- **Distributed correctness is the gate** - never collapse the hash exchange for a multi-partition input absent a
  two-phase combine.
- **Rune is the control** - SQL-side effect only; do not add or alter Rune's aggregate shape (the binder locus is
  unchanged-by-construction; verify it).
- **Generic rule** - no q03 branch; applies to all grouped aggregates (and `Distinct` if binder-normalized).
- **Materiality first** - do not ship a shape-only cut if the exchange is within the noise band; escape to Thread 3.

## Risks
- **Exchange immaterial** -> Phase 0 gate 1 catches it; stop-line, Thread 3.
- **Elision needs two-phase aggregate** -> Phase 0 gate 2 catches it; stop-line, Thread 3 (that mechanism is the lever
  the Polars streaming datapoint points at).
- **Over-collapse breaks distributed correctness** -> the co-location proof must be sound; the multi-partition
  keep-the-exchange test is the tripwire.
- **Shape closure without perf** -> not banked; acceptance requires the median improvement.
- **Sibling regression** -> q05/q10 + `Distinct` shape checks; full gate.

## Forward Arc
If 402 lands, q03's SQL/Rune shape is fully closed (401 filter placement + 402 aggregate placement) and the remaining
gap is pure substrate/execution -> Thread 3. If 402 escapes, the diagnostic feeds Thread 3 directly: a partial/final
two-phase aggregate and/or streaming-completion is the real lever, and this placement policy folds into that arc.
Either way the next big rock is the **execution-model / streaming-completion** change for the >360s sf=10 heavy cells,
informed by the 401 Polars-streaming peer datapoint (q03 ~102 ms, 6.66x under Toron SQL). Article-grade throughout.

## Grounding evidence (file:line)
SQL pre-aggregate exchange: `sql_planner.rs:3974`, `:6091`; `DEFAULT_AGGREGATE_PARTITION_COUNT` =
`DEFAULT_JOIN_PARTITION_COUNT` = 4 at `sql_planner.rs:58-60`, whose own comment flags it as a placeholder ("grouped
SQL reuses the join shuffle fanout until aggregate fanout has separate evidence") - i.e. the engine already records
that this fanout is unconsidered, which is exactly the residual 402 targets. Rune direct aggregate, no exchange:
`rune_engine.rs:393-401`. Binder binds `Aggregate` over its input faithfully: `binder.rs:1288-1318`
(`bind_bound_aggregate` at 1286/1318). Sibling `Distinct` pre-aggregate exchange with the same constant:
`binder.rs:1256-1286` (`DEFAULT_DISTINCT_PARTITION_COUNT`, `binder.rs:1279`). Current `PlanOptimizerFacts` preserve
row/byte/origin estimates through `exchange()` (`binder.rs:614-741`) and do not themselves prove partitioning, so Phase
0 must not infer co-location from those facts. No two-phase aggregate exists (engine grep). 401 residual +
fingerprints: q03 SQL `d1d275a0a8181d73`, Rune `666e0ac028036f70`, SQL/Rune `1.78x` same-machine (401 results). Gauge
reachability path is local/single-process (`execute_query_with_tables`), but that is not a distributed partitioning
proof.
