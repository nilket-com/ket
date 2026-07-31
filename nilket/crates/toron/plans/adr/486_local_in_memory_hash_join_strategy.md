# ADR 486 - local in-memory hash join: strategy design

ADR 485 established m06's strategy gap: an 88 MB build is too big to broadcast
(16 MB ceiling, ADR 138-pinned) and too "large" for the only alternative
(hash-partitioned, which always repartitions and spills **both** sides), yet fits
comfortably in the 1 GiB retained memory cap. This ADR **designs** the missing
strategy — a local in-memory hash join — its admission rule, and its distributed
interaction. It is a design deliverable; implementation is a deliberate follow-on
gated on the correctness question below, not shipped here.

## The strategy

Build the entire build side into **one** in-memory hash table (no partitioning, no
spill); stream the probe side through it. This is the single-node local hash join
every row-and-columnar engine has, and **the executor machinery already exists** —
`build_in_memory_join_rows` / `probe_in_memory_join_rows` (`join.rs:1300,1387`) and
`ReorientedInnerJoinBuild::InMemory`. The gap is purely in **admission**: m06 binds
`HashPartitioned` + `Exchange`, which routes to the always-spilling exchange path
(`SpilledInnerJoinBuild`) and never reaches the in-memory build. Nothing new needs to
be built in the executor; the binder must be able to *emit a non-exchanged local
join* for the admitted case.

## Admission rule (all must hold)

1. **Inner equi-join** — the shape the in-memory build/probe supports.
2. **Build fits the retained cap** — `build_bytes ≤ memory_cap × 16`
   (`JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER`), the same cap the spilled
   path already honors. Note the direct path currently passes the *raw* task budget
   as its cap (`join.rs:367`); admitting m06's 88 MB (> 64 MiB raw, ≪ 1 GiB retained)
   requires the local join to use the retained cap. Deliberate, and stated.
3. **Build exceeds the broadcast ceiling** (> 16 MB) — below it, broadcast already
   wins and this strategy adds nothing.
4. **Probe is scan-linear** — the same linearity fence the broadcast/hash decision
   already computes, so the probe can stream without materialization.
5. **Single-partition execution** — the distributed-correctness gate (next section).

## The distributed-correctness crux (the load-bearing risk)

A local, non-exchanged hash join is correct **only when the join executes on a single
partition/worker**. In genuine distributed execution, omitting the repartition
exchange leaves each worker with only its shard of the probe and a local build →
**silently missed cross-worker matches** (a wrong answer, not a crash). This is the
one way this strategy can be catastrophically wrong.

The problem: **the binder chooses join strategy, but distribution is decided
downstream by the fragmenter** — the binder does not cleanly know whether a given
join will run single-partition. So the admission gate cannot simply be a bind-time
boolean. Two candidate resolutions, to be decided in the implementation cut with
evidence, not presumed here:

- **(A) Conservative single-node-only admission.** Admit the local join only when the
  execution context is provably non-distributed (single worker / single partition
  fragment). The gauge and single-node deployments — where m06 lives — qualify;
  distributed plans keep the exchange. Smallest blast radius, provably safe, but
  helps only single-node.
- **(B) Fragmenter-cooperative admission.** The binder marks the join
  local-eligible; the fragmenter converts it to local only in fragments it keeps
  single-partition, and falls back to the exchange when it distributes. Larger blast
  radius (binder↔fragmenter contract), but generalizes.

The design **leans (A)** as the first cut: it is provably correct, unlocks m06 and
every single-node deployment, and defers the distributed contract to a later cut that
can prove the fragmenter path. Shipping (B) first would put a wrong-answer risk behind
a performance change — the opposite of the discipline the arc has held.

## Blast radius

- **Avoid a third `JoinStrategy` IR variant if possible** — a new variant trips ~6
  source-scanning guards (JoinKind-variant-guard-checklist) and ripples through the
  fragmenter and every strategy match. Prefer expressing the local join as
  `HashPartitioned` with a single-partition / no-exchange marker that routes to the
  direct in-memory path, or the minimal representation the implementation finds
  cleanest — decided there, grounded.
- Correctness gate is non-negotiable: the armed 22-query parquet sweep plus a
  distributed-execution test that would **fail** if a local join were chosen for a
  multi-worker fragment. Absent that failing-under-distribution test, the cut does
  not ship.

## Acceptance (for the implementation follow-on, not this design)

- Local in-memory hash join admitted for m06's shape single-node; build uses the
  retained cap; no spill, no repartition.
- A distributed-correctness test proves the local join is **never** chosen for a
  distributed fragment (the wrong-answer guard).
- m06 priced via matched-build A/B under settled load (479 method); the prize is the
  ~34% partition/shuffle cost ADR 483 mapped, so a large win is plausible but the A/B
  decides.
- No sealed verdict re-judged; broadcast and hash-partitioned paths unchanged for
  their current admission.

## Forward

Implementation is the next cut, option (A) first. If it lands, m06's ~3× gap to
SQLite's indexed nested-loop is directly addressable (SQLite's advantage was exactly
avoiding the build materialization + shuffle). Option (B) — distributed local-join
cooperation — is a later generalization with its own correctness proof. This ADR is
the grounded design those cuts build on.
