# ADR 499 - local grouped-aggregate exchange bypass: diagnose m04+m05, then cut redundant serial repartition

ADR 498 grounded m04's gap and, in doing so, falsified the assumption that motivated it. Direct
measurement put parquet decode at **286 ms** and the grouped fold at **421 ms** of a **1,932 ms**
wall — so neither the ADR 494 decode-vs-fold fork owns m04's time. Most of the residual did, and
Phase 1's exchange instrument then measured it: an unconditional partitioned exchange the generic
local route never needed owns **~854 ms (43% of execute)**, structurally redundant. This ADR seals
the staged gate — diagnose first, then a narrowly-admitted cut — and fixes the scope so the
diagnostic cannot drift into implementation.

## Grounded boundary

The **generic** local streaming grouped-aggregate route inserts a partitioned exchange for every
grouped SQL aggregate it handles, using the join-derived default fanout of 4 (documented as lacking
aggregate-specific evidence). In that route it: (1) hashes the group key, (2) builds per-partition
row-index vectors, (3) Arrow-`take`s **every column** into new partitioned batches, then (4)
**serial-folds** each partition into one **shared** group table. The merge is serial — there is no
per-partition parallel fold and no later merge — so the exchange supplies **zero parallelism at any
cardinality**. m04 (`GROUP BY l_returnflag`, Utf8 key) falls to this generic route; its observed
46,865 → 93,730 fold-call ratio is 2 of the 4 partitions non-empty per source batch.

**m05 is not on this route.** ADR 495's primitive Int64 grouped-count path is selected *before* the
generic path (`try_streaming_scan_grouped_aggregate`, aggregate.rs:552→582): it reads the exchange
as a **bind-time precondition** (an exchange must exist and match the group key) but then scans keys
directly into `PrimitiveInt64Groups` (`execute_primitive_int64_count_grouped_scan`, aggregate.rs:768)
and **never calls `repartition_exchange_batch`**. So m05 already pays no take-materialization; it is
a *control*, not a second rung. Only queries that fall to the generic route (non-primitive group
keys such as m04's Utf8) carry this cost. The exchange remains semantically load-bearing for
**distributed** partition ownership; this is a local generic-route observation only.

## The gate (staged)

**Phase 1 — diagnose m04 (required rung) + m05 (control), no behavior change.** An
exchange-attribution instrument splits the generic exchange into key-routing / index-construction /
take-materialization (reporting columns and bytes copied), and reconciles per query:

```
parquet_decode + exchange(routing+index+take) + fold + finalization + residual  ≈  execute
```

**Phase 1 result (complete; instrument rail +1.6% median, digest/3-rows/zero-spill invariant).**
Validated m04 paired receipt (median of two pairs): decode ~160 ms, **exchange routing/index
~757 ms + Arrow take ~94 ms ≈ 854 ms (43% of execute)**, fold ~452 ms, residual ~496 ms. m05
control confirmed on ADR 495's primitive Int64 path — `exchange_micros: 0`, 12M rows, identical
digest, zero spill: it never enters the generic route.

Two findings re-specify the Phase 2 trigger (see amendment below):
- The exchange did **not** close the whole residual — ~496 ms remains, a *separate* cost (key-eval /
  projection / framework), not evidence against the exchange.
- The dominant exchange term is **hash routing + index construction (~757 ms), not the take
  (~94 ms)**, and it is **doubly redundant**: the exchange hashes 48M keys to partition them, then
  the partitions serial-merge into one shared table where the fold **re-hashes** to group. Repartition
  is **not pipelined** with the fold — the loop completes repartition before folding each partition —
  so removing it maps directly to a wall reduction.

**Phase 2 trigger (amended).** Authorize the bypass when the generic local exchange is **large,
measured, and structurally redundant** — *not* on residual-closure. Residual-closure was a proxy for
"is the exchange the target"; the receipt gives stronger authorization: 43%-of-wall magnitude plus
structural redundancy (redundant hash-route feeding a serial re-hash fold, not pipelined). The
~496 ms residual stays **named** and becomes the **next decomposition target after** the bypass
result — it is a separate target, never a veto on deleting known-redundant work.

**Phase 2 — cut.** Build a **route-shaped** local bypass: skip the exchange when the route is a
generic local streaming grouped aggregate folding into a shared single group table. The admission is
a **structural fact of the route**, not an NDV/cardinality estimate and not a planner-wide exchange
removal. Distributed partition ownership is untouched; ADR 495's primitive m05 path is independent
and unchanged.

## Acceptance rail (Phase 2)

- Explicit **bypass receipt** carrying the admission/decline reason — proof the guard *fired*, not
  just that the clock moved (the verify-the-decision-fired rail).
- m04 `digest` and output cardinality (3 rows) unchanged; spill stays zero.
- **In the same paired runs, the exchange bucket collapses to ~0 AND execute drops materially** —
  the ~854 ms is not pipelined, so a bucket that collapses without a matching wall drop is a
  confound to explain, not the cut. The ~496 ms residual should remain ~unchanged (it is separate
  work); it stays named as the next target.
- m05 is untouched by construction (it never entered the generic route); its digest, 12M-row
  cardinality, and zero spill must remain identical as a **regression control**, not a target.

## Scope and non-goals

- No NDV/cardinality heuristic; the guard is route-shape only.
- No planner-wide change; distributed exchange semantics untouched.
- ADR 494's borrowed-UTF-8 question is closed as secondary — decode (286 ms) and fold (421 ms) are
  both minor; the borrowed on/off pairs retire that fork without gating this ADR.
- **No m05 improvement is claimed.** m05 already avoids the exchange via the primitive path; it
  appears here only as a control.

## Forward

This is a **generic-route cut** — removing a large, measured, structurally-redundant serial
repartition-and-re-hash stage from the generic local grouped-aggregate path. Its reach beyond m04 is
a **hypothesis to prove, not assert**: whether it moves the wider generic grouped-aggregate family
(non-primitive or multi-key group keys) must be shown with *another generic grouped workload*, since
m05 — the obvious second candidate — is already off this route. After the bypass result, the named
**~496 ms m04 residual** (key-eval / projection / framework) is the next decomposition target. The
guard is written against today's serial fold; if the generic local route ever gains a genuine
per-partition parallel fold, the admission must be revisited then, with evidence.
