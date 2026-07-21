# ADR 487 - execute_hash_join shape investigation (read-only; re-scopes ADR 486)

The investigation ADR 486's re-scoping called for: trace live callers of
`execute_hash_join`, record the accepted shape, whether it is non-exchanged, its
distributed semantics, and a real query that exercises it. The result **dissolves the
"new local-hash plan shape + distributed gate" framing**: the in-memory join is the
same `ExecutionPlan::Join` node, and spill-vs-in-memory is a runtime build-size engage
threshold, not a plan shape. No code shipped.

## 1. The accepted physical-plan shape

`execute_hash_join` (`join.rs:196`) is the single hash-join entry, called from the
plain `ExecutionPlan::Join` arm (`exec.rs:1032`) which materializes both inputs. It
tries the streamed-spilled sub-paths first (`try_execute_streamed_spilled_*`), and
falls through to an in-memory-or-spill build (`join.rs:131`) when they decline. The
join-into-global-aggregate variant (`execute_hash_join_into_global_aggregate`,
`join.rs:462`) — **m06's actual path** — routes through the same engage function.
**There is no separate in-memory plan node.** The design's premise that a new shape
must be introduced is false; in-memory and spilled are two runtime outcomes of one
`Join` node.

## 2. Non-exchanged?

The spill-vs-in-memory choice is **decoupled** from exchange. Exchange presence is a
bind-time partitioning concern: `validate_strategy_shape` requires a `HashPartitioned`
join to carry its exchanges (ADR 486 probe 2's `JoinKeyExchangeMismatch` fired at
bind, not execution). The in-memory-vs-spill decision happens later, inside
`execute_hash_join`, on build size. So "route m06 in-memory" is **not** "remove the
exchange" — the two are orthogonal, which is why the exchange probes missed.

## 3. Engage rule and its distributed semantics

`try_create_streamed_spilled_inner_build` (`join.rs:2437`) declines
(`return Ok(None)` → in-memory) when
`estimated_build_hash_bytes <= task_budget.memory.get()` — the **raw** task budget —
and engages the spill otherwise. The estimate is
`rows × (112 + key_width)` (`join.rs:3624`): ~120 bytes/row for an 8-byte key, a ~15×
padding for hash-table overhead. **Distributed semantics are unchanged by this
threshold** — the plan shape, exchange, and partitioning are identical whether the
build spills or stays in memory; only the spill *timing* differs. The distributed
negative test ADR 486 designed is therefore **not the relevant rail** for this lever:
there is no non-exchanged shape and no cross-worker-match risk. The real risk is
**OOM** — building a too-large table in memory.

## 4. A real query exercising the in-memory path

q03, q05, q10 at sf=1 run their joins in-memory (spill=0), confirming the fallthrough
is live. m06 does not: at task budgets 64 MiB → 2 GiB it still spills (~86 MB), so its
`estimated_build_hash_bytes` exceeds 2 GiB. With the `rows × 120` formula that implies
a ~48M-row build — the **lineitem side**, not the 12M-row orders side. (Higher budget
does improve m06 ~25%: 20.5 s → 15.4 s at ≥512 MiB, via reduced spill pressure, but
never eliminates the spill.)

## The re-scoped m06 lever (for the next cut, not shipped here)

m06 spills because it builds the ~48M-row side and its padded estimate dwarfs any
budget. The levers are all **build-size**, none plan-shape or distributed:

- **(a) Build the smaller side.** If m06 built the 12M-row orders side, the estimate
  (~1.44 GB) would fit a large budget and go in-memory. This is a join-order /
  build-side-selection question, adjacent to the ADR 464-472 smaller-build work — and
  it is the SQLite-parity lever (SQLite probed the small side's PK index).
- **(b) Engage against the retained cap, not the raw budget.** The in-memory build can
  hold up to the 16x retained cap; the engage threshold compares against the raw
  budget, spilling builds that would fit. Aligning them routes medium builds in-memory
  — with OOM as the tradeoff the estimate padding exists to prevent.
- **(c) Right-size the 15x estimate padding** if it over-charges real hash-table cost.

Each trades against OOM, and each is measured by the ladder + a matched A/B — the
established discipline. **None needs a distributed-correctness gate.** ADR 486's design
(new strategy + distributed negative test) is superseded by this finding for the m06
case; a genuinely non-exchanged local strategy remains a separate, larger question if
ever needed, but m06 does not require it.

## Disposition

Read-only investigation; tree clean; m06 at baseline. The deliverable is this record.
The next cut is a build-side/threshold change under OOM discipline — starting with (a),
the smaller-build selection, which is both the safest and the SQLite-parity lever —
not the local-hash-plan-shape cut ADR 486 scoped.
