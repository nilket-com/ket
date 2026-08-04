# ADR 432 - q02 join-output wall: attribute, then move

## Context

The sf=10 strict TPC-H board is 21/22 green. **q02 is the sole remaining cell** — pure execution
residency. ADR 284 landed q02's multi-table-inner correlated-scalar reachability and ADR 292 its
strict oracle (`strict_q02_oracle`, `compare_strict_q02`); q02 reaches output and answers correctly
at small scale. q02 is TPC-H Q2 (min-cost supplier): a 5-table EUROPE join (`part, supplier,
partsupp, nation, region`, filtered `p_size=15`, `p_type LIKE '%BRASS'`, `r_name='EUROPE'`), a
correlated `MIN(ps_supplycost)` subquery over a 4-table inner, the equality `ps_supplycost = MIN(...)`,
and a final `ORDER BY s_acctbal DESC, n_name, s_name, p_partkey` (`Q2_DEV_SQL`,
`toron-tpch/src/lib.rs:308`; strict aliases it).

**What the 415 sweep established — and what it did not.** q02 fails at **join-output
materialization**: the generic payload reports the total estimated output as
**`requested=1,738,000,000` bytes** against the 1,073,741,824 (1 GiB) cap, remedy "join-output
streaming/spill" (`plans/impl/415_...results.md:81`). But that payload carries **only the total
byte estimate** — it **lacks join identity, selection rows, inputs, and any byte decomposition**
(`materialize_join_selection`, `exec/join.rs`). So **which** join produces the 1.738 GB, and **how**
those bytes decompose, is **unattributed** — not a finding that the producer is the correlated-scalar
recombine. The iter-0 identity claim was a hypothesis; this ADR names the wall before moving it.

**The structural hypothesis (to be confirmed by Phase 0, not assumed).** The current producer of the
1.738 GB is **necessarily a join**. The likely plan is terminal
`Sort <- [Project] <- Filter <- LeftOuter`, where **`Sort` is a downstream materialization barrier**:
it collects its entire child into a `Vec` via `execute_bound(input)` before `execute_sort`
(`exec.rs:986`). The Sort barrier may be what **prevents streaming the producing join through its
`ps_supplycost = MIN(...)` filter**. It is not itself the current 1.738 GB producer; if the filtered
survivors later overflow the Sort, that is a **distinct sort-materialization wall**. The retained
sf=10 DuckDB receipt shows **4,667 result rows** — a small survivor set (grounded), but **not "one
supplier per part"**: MIN-cost **ties are required**, so uniqueness is not an invariant.

**The landed precedent to reuse (grounded).** The closest primitive is **ADR 346**, not 340 alone:
Toron already recognizes `Aggregate(global) <- Filter <- LeftOuter` and performs chunked
recombine/filtering in `try_streaming_correlated_scalar_left_outer_global_aggregate`
(`streaming.rs:3873`), plus ADR 340's filtered-survivor accumulator (339 sizing, 341 exchange, 361/367
scan-output). q02's missing consumer is the **terminal `Sort` variant** of that recombine — a
shape-fenced extension, not a new engine and not a generic join-output spill.

## Decision

**Phase 0 first (capture → attribute → conditional probe), then a shape-fenced wall-mover selected by
what Phase 0 names.** No streaming code lands until the wall is attributed.

1. **Capture** the exact bound plan and matching fingerprint from the failing **persisted sf=10**
   invocation (ADR 417 artifact discipline) — the ground truth of q02's shape.
2. **Attribute** the 1.738 GB to its exact producing join and immediate consumer via a **persisted
   Phase-0 receipt from an env-gated sf=10 diagnostic, removed before seal** — recording join
   identity/kind, parent path, inputs, selection rows, and a byte decomposition. This is a one-run
   diagnostic receipt, **not** a permanent enrichment of every generic join error (that broader
   attribution surface is a named follow-on, not this cut).
3. **Only then**, if the producing join's filter is adjacent/reachable, run an **env-gated sf=10
   chunk probe** for raw-versus-post-filter bytes (sf=1 is calibration only; the load-bearing
   selectivity measurement is sf=10). If an earlier outer or scalar-branch join owns the wall,
   post-`MIN` selectivity cannot select the mechanism — do not measure it.

**Then the wall-mover, chosen by what Phase 0 names:**

- **(a) raw adjacent output large, filtered survivors fit** → **chunk-recombine/filter and accumulate
  projected survivors** feeding the Sort — extend the ADR 346 recombine primitive + 340 accumulator to
  the terminal-`Sort` consumer (the **conditional lean**, *if* Phase 0 confirms the terminal
  correlated-scalar `Sort <- Filter <- LeftOuter` shape). A shape-fenced terminal-sort-input
  extension.
- **(b) filtered survivors exceed the Sort's budget** → spill the survivor / sort-input stream, or
  externalize the Sort. This is a **distinct sort-materialization wall**, named honestly as new;
  `SpilledInnerJoinBuild` spills **build ownership**, so it is not the vehicle here.
- **(c) the wall belongs to an earlier join** → stream/spill that named join boundary instead; q02's
  terminal shape is not the mechanism.

## Acceptance

- **Named next wall OR sf=10 persisted `pass@compare`** — no one-cut green commitment. As 339→340: the
  headline is that the 1.738 GB materialization is removed and the true next boundary is **named with
  producer + bytes**; green lands only if the survivor path fits, proven live.
- **Attribution:** the Phase-0 sf=10 receipt names the producing join, its inputs, selection rows, and
  byte decomposition — replacing the total-only generic payload for this diagnosis. The diagnostic and
  probe are removed before seal.
- **Order:** `ORDER BY` is established by the **final Sort** with the **exact q02 comparator and all
  MIN-cost ties preserved**. Chunk traversal preserves only the recombine primitive's
  selection-order parity; sorted order is the Sort's job, not "preserved across chunks."
- **Budget honesty (per 340):** the survivor/sort-input accumulator is budget-accounted and emits a
  named `memory_budget_exhausted@execute` if still too large — no silent unaccounted wall replacing
  the raw one.
- **Correctness:** small-scale `pass@compare` throughout, plus **persisted sf=10 `pass@compare`
  whenever execution reaches output**. A cap bump stays doctrine-rejected. Full Toron and TPC-H
  gates; `exec.rs` ratchet respected.

If the wall-mover removes the 1.738 GB materialization but survivors overflow the Sort, the board
stays 21/22 and this ADR records the precise named sort-materialization residual without weakening
the envelope or raising the cap. If execution reaches output and compares correctly, the board moves
to 22/22.

## Guardrails and risks

- The Phase-0 identity is a hypothesis until the sf=10 receipt names the producing join; the
  terminal-Sort-input lean is conditional on that shape. If an earlier join owns the wall, mechanism
  (c) applies and the conditional probe is not run.
- Removing the join-output materialization can expose the Sort barrier as the next wall; that is a
  distinct, honestly-named residual, not a regression.
- Disk-backed spill residue (per ADR 430) lands under `target/tpch/join_spill`; canonical runs start
  from fresh scratch and verify cleanup after normal completion.

## Forward arc

- Clearing q02 (however many cuts) closes the sf=10 22/22 arc.
- Deferred: a permanent producer-attributed generic join-error surface (this cut uses a one-run
  diagnostic receipt only).
- Beyond the board: the substrate comparison work (Rune/Polars/DuckDB, article-grade perf) is the
  standing thread.
