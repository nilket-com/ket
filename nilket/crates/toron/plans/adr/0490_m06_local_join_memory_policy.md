# ADR 490 - m06 local-join memory policy: raw-budget coordinate before admission

ADR 489 deliberately installed the distributed safety rail before anything can emit a
local (non-exchanged) hash join. This cut decides the **memory policy** that must be
true before the binder may read that local-only signal. It does not wire the reader or
change any join shape.

The policy is intentionally narrow: validate one observed m06 cell under an explicit
task-budget coordinate, then let a successor decide whether the guarded local shape is
worth admitting there. It is not a production-default change and it does not relabel
the coordinate as a general deployment recommendation.

## Grounded boundary

m06 at sf=8 is:

```sql
SELECT COUNT(l_orderkey)
FROM lineitem INNER JOIN orders ON l_orderkey = o_orderkey
```

The smaller-build placement is already active: the binder emits
`swapped_right_build hash_smaller_build`, so the 12M-row `orders` side builds. The
executor's actual direct-build guard is not the physical 86.7 MiB exchange spill and
not the retained spill cap. It is exactly:

```text
estimated_build_hash_bytes = rows * (112 + key_width)
                           = 12,000,000 * (112 + 8)
                           = 1,440,000,000 bytes
```

for m06's one `Int64` key. Both `try_create_streamed_spilled_inner_build` and
`build_in_memory_join_rows` compare that estimate with the **raw** task-memory budget.
The existing `JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER` therefore cannot
admit m06 by itself: its 64 MiB coordinate yields a 1 GiB retained cap, still below the
1.44 GB estimate, and the direct in-memory guard does not use that cap anyway.

## Decision: one explicit raw-budget coordinate

Use `(memory_per_task = 2 GiB, spill_bytes_reserved = 2 GiB, sf = 8)` for the first
local-admission cell. Two GiB is the smallest clean power-of-two above the measured
1.44 GB raw guard, leaving 707,483,648 bytes (about 0.66 GiB) of estimator headroom.
Both coordinates are explicit and retained in every raw and summary row by the existing
gauge provenance contract.

This chooses candidate (a) from ADR 489: larger, coordinate-stamped raw task memory.
Candidate (b), retained-cap admission, is rejected for this cell because it cannot
cross the executor's raw-budget gate. No global `QueryBudget` default, operator policy,
or retained-cap semantics change in this ADR.

### Bind-time visibility is explicit, not inferred

The binder does not otherwise receive `TaskBudget`, so it cannot infer the raw executor
budget from a physical byte fact or from the spill reserve. The direct single-node gauge
therefore supplies an optional `PlannerConfig.local_hash_join_memory_per_task` from the
same `TORON_TPCH_MEMORY_PER_TASK` value used to construct its `ExecutionContext`. The
field defaults to `None`, is a policy input rather than an admission reader, and has no
effect until the successor consumes it. Distributed coordinator binds retain neither the
single-partition proof nor a local admission path.

## Rail before binder admission

The local-shape reader stays absent until the following rail exists and is green on the
candidate implementation:

1. **Exact admission evidence.** The binder-facing predicate must use the same
   row-count-plus-key-width estimate as `estimated_build_hash_bytes`, not
   `PlanOptimizerFacts.byte_upper_bound` or observed spill bytes. Its unit test pins
   m06's 12M Int64 calculation, rejects 64 MiB, and accepts 2 GiB. The estimator must
   be shared with, or byte-for-byte proved equivalent to, the executor guard so a bind
   cannot promise residency that execution declines.
2. **Armed fresh-process row.** The standard `toron-tpch perf` parent/child rail runs
   m06 at the explicit coordinate, not bare `perf-run-one`: the parent records
   `peak_rss_kib` via `wait4`, alongside digest, spill, plan fingerprint, and both
   budget provenances. A direct child is insufficient because it reports no peak RSS.
3. **Policy-fired proof.** The candidate row must contain an explicit local-admission
   receipt (or an equally structural plan proof) showing the no-exchange local shape
   was chosen. `spill_bytes == 0` alone is not proof: an unrelated plan change could
   also remove spill.
4. **Safety judgment.** The row must pass with a non-null peak RSS under the normal
   12 GiB cgroup ceiling, no `BudgetExhausted`/OOM, and the exact baseline digest and
   one-row output. Peak RSS is recorded rather than compared to the 2 GiB task knob:
   task memory is a join-admission estimate, while process RSS also includes scan and
   runtime allocations. The receipt reports the absolute peak and the 2 GiB coordinate;
   it must not pretend one is a bound on the other.

The existing ADR 489 rails remain mandatory on top of this one: distributed planning
clears `single_partition_execution`, and a three-worker exchange-free hash join fails
loudly with `JoinInputNotExchanged`.

## Baseline recorded before any local admission

On current exchanged m06 at this exact coordinate, one fresh-process parent row passed:

| coordinate | digest / rows | peak RSS | spill | execute |
|---|---:|---:|---:|---:|
| 2 GiB task, 2 GiB reserve, sf=8 | standing digest / 1 | 477,476 KiB | 88,559,536 B | 20,453.8 ms |

It deliberately does **not** demonstrate the policy firing: current code still emits
the exchanged `HashPartitioned` shape (`144a330ac4a17555`). It proves the harness can
capture the residency/digest evidence at the chosen coordinate before a new local
shape exists.

## Predeclared implementation outcomes

- **Admitted and safe:** exact 2 GiB admission proof, local shape proof, zero spill,
  digest/row equality, non-null RSS, and no memory failure. Only then price it with
  matched A/B under settled load.
- **Not admitted:** the shared estimate or runtime guard still declines at 2 GiB. Keep
  the reader off and diagnose the mismatch; do not raise the coordinate ad hoc.
- **Memory failure / excessive residency:** halt as policy-inconclusive. The recorded
  peak and cgroup result determine whether a separately designed larger coordinate is
  justified; no automatic raise.
- **Digest mismatch or distributed-rail failure:** correctness failure. Revert the
  candidate and retain the exchanged plan.

## Forward

The next increment adds the shared admission-estimate rail and the fresh-process
policy-proof surface before it reads `single_partition_execution`. The direct gauge
also carries its exact raw budget to the binder as an unused policy input. Only after
those are green may it emit the no-exchange local shape, already protected by ADR 489's
coordinator and fragmenter rails.
