# ADR 436 - Generic grouped-fold hash-probe throughput diagnosis (q13's next wall)

## Context

**ADR 435 named it.** After the grouped-fold hash migration took q13 from 28.3 s → 22.3 s, the
post-change callgraph puts `find_bucket_in` (`aggregate/group_table.rs:432`) at **23.83%** of cpu-core
cycles; q13 is 22.3 s / 16.11x DuckDB.

**Equality is the leading hypothesis (from the 435 callgraph) — but only one kind diagnoses hashing.**
The 23.83% already splits **9.18 + 8.69 = 17.87 points into equality frames**, leaving **5.53 points**
in the residual lookup body. Equality has two kinds:
- **matching equality** — the successful `[GroupValue]` compare confirming a row's own group; ~15.3M of
  these are **unavoidable** grouping cost, not a hash defect;
- **collision-mismatch equality** — a compare against an *occupied, different* key during probing;
  **only this diagnoses hash quality.**
Separating the two is the whole reason 436 exists before 437.

**The probe accounting is incomplete.** The 435 receipt's **21,952,635 probes / max 66** are
**primary-lookup only** — they exclude vacant placement, rehash reinsertion (12 growths; `grow()`
re-probes via `find_bucket_in`, `group_table.rs:325/380`), and those phases' hash/equality work. `max
66` is a reason to measure clustering, not a finding.

**The hasher is `ahash::RandomState`, fresh per table instance** (`RandomState::new()` at
`group_table.rs:61`, per `BoundGroupedAggregateGroups` construction) — so it varies across fresh
children (and across q13's two folds). Probe clustering is seed-dependent; a single child cannot
separate a stable distribution from an unlucky seed.

## Decision — perf names the category; a REQUIRED ledger measures the phase × collision geometry

Diagnosis-only (429/433 discipline). All profiling is **diagnostic overhead**, not perturbation-free.

**Phase 0 — external perf across 5 fresh children.** Establishes **sampled cycle-category** stability
across seeds (equality vs hash vs residual body) and the rough share. perf **cannot** measure
p99/p99.9/tail probe stability — only the ledger can.

**Phase 1 — REQUIRED parity-locked ledger.** Since exact phase-specific probe distributions are an
acceptance requirement and perf cannot produce them, the ledger is inevitable, not conditional. Model
**phases × costs**:
- **Phases:** primary-occupied, primary-vacant, vacant-placement, rehash.
- **Per-phase metrics:** hash calls, bucket visits, equality invocations, **matching equalities**,
  **collision mismatches**, and a **probe histogram**.
- **Key construction / allocation** (the per-row `Vec<GroupValue>`, deferred by 435) is a **separate
  upstream line** — measured on its own, **not** part of the 23.83% `find_bucket_in` frame.
- **Histogram that can prove p99.9:** exact bounded bins + one overflow bucket, **no stored samples or
  per-row timers** (bounded overhead). If overflow reaches **0.1% of operations, widen and rerun** —
  otherwise p99.9 is only *bounded*, not *measured*.
- **Geometry (locked):** **per-growth-epoch histograms plus a final occupied-cluster run-length
  histogram.** A pooled tail cannot tell poor hashing from normal linear-probing at a given load; the
  geometry does.

**Exact operation reconciliation — q13's INNER fold only (outer fold kept separate, never pooled).**
The ledger must satisfy, as self-checks:

| invariant | value / rule |
|---|---|
| total calls = total hash computations | **18,933,731** (primary 15,337,603 + vacant-placement 1,500,000 + rehash 2,096,128) |
| primary `find_bucket_in` calls (first lookup takes the empty-table shortcut) | 15,337,603 |
| primary-occupied matching equalities | **exactly one per occupied lookup** |
| primary-vacant, vacant-placement, rehash matching equalities | **zero** |
| collision mismatches (per phase) | **equality invocations − matching equalities** |
| vacant-placement & rehash equality invocations | **bucket visits − calls** (the terminal empty bucket performs no equality) |
| outer fold (46 groups) | counted separately, never pooled into the inner-fold domains |

**Phase 2 — name the dominant producer** with the matching-vs-collision equality split and the per-load
geometry, stable across all 5 children. No cut in 436.

## Stability rule (locked before measurement)

- **N = 5** fresh children. **Per-child results are the gate; pooled results are volume summaries only.**
- **Same dominant sampled cost in all five children.**
- **`(max − min) / median ≤ 0.25`** across the five children for **normalized p99, p99.9, and the
  predeclared tail rates.** If the median is zero, **require all five values to be zero.**
- **Report `max`, but never gate on it** — an extreme statistic is not a stability signal.

## Mechanism discriminator (for ADR 437, not 436)

- **collision mismatches dominate** → hash quality / load factor / probe scheme, **or a stored
  fingerprint** to skip mismatched full compares;
- **matching equality dominates** (collisions low) → the cost is unavoidable grouping work → a **cheaper
  canonical / compact key representation or group ID** (a fingerprint pre-check does *not* help the
  successful compare);
- **hash computation dominates** → cached / reused hash, especially **hoisted across the second lookup**
  (primary → placement);
- **upstream key construction dominates** → allocation reuse / columnar-key cut.

Evidence chain: **perf names the cycle category → the required ledger explains its operation phase and
collision geometry → ADR 437 receives one justified mechanism.**

## Acceptance

- **Named dominant producer** for `find_bucket_in`'s 23.83%, backed by the exact operation reconciliation
  (18,933,731 across the four phases), the **matching-vs-collision equality split**, per-phase probe
  histograms (exact bins + overflow < 0.1%), and the per-growth-epoch + final run-length geometry —
  **stable across all 5 children** by `(max−min)/median ≤ 0.25` on normalized p99/p99.9/tail, not one
  seed's `max`.
- **Key construction accounted separately** upstream, not inside the find_bucket_in frame.
- **No engine behavior change, no mechanism, no speed claim**; the ledger is off-by-default with **locked
  output/telemetry parity when disabled**; artifacts under `target/tpch/adr436/`.
- **Composes with the baseline** — perf under the exact 356 profile; profiling timing is diagnostic
  overhead, never a speed number; the 356 baseline is not rewritten. `exec.rs` ratchet; box new held
  state if the async body grows; no query branding.

## Guardrails and risks

- The reconciliation self-checks are the ledger's correctness proof; a domain that does not reconcile to
  18,933,731 (or violates the per-phase matching/collision identities) invalidates the receipt.
- The overflow-under-0.1% rule is load-bearing: without it p99.9 is bounded, not measured.
- Seed dependence is real (`RandomState` per table instance); the 5-child gate is what turns a `max 66`
  anecdote into a stable finding — or refutes it.

## Forward arc

Third diagnosis in the q13 thread (433 → 434 pin → 435 fold → 436 probe); ADR 437 builds the one
mechanism the discriminator selects. q17 and q19 remain **independent** diagnoses on their own numbers.
The deferred O(1) LRU and the primitive Int64 path stay separate threads.
