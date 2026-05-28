# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) — git holds the
archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not implementation
commits. The roadmap lags the code on purpose — check `git log` and the
relevant impl plan for actual status. Per-ADR decisions live in
`plans/adr/NNN_*.md` + each ADR's §"What to do next"; this file holds only
the forward view + the active line.

## Status

ADRs 001–124 promoted (implemented unless a `feat` is still pending — check
`git log`). Per-ADR detail in `plans/adr/NNN_*.md`.

## V1 target

"First Query with Integrity" plus "Distributed Execution Correctness":
correct analytical results against a Wendelon-native cluster with pushdown
maximized, identity flow real, stream-shaped storage boundary, multi-process
coordinator/worker mechanics surviving a worker SIGKILL with byte-identical
normalized output, and the first two-input relational operator working across
hash-partitioned and bounded broadcast strategies. Multi-tenant governance,
executor model upgrade, sort-merge, output-side join streaming, and full
end-to-end natural backpressure remain post-V1.

## Prioritization philosophy

Toron is the compute ring of Nilket; Wendelon is the storage ring. We control
both. The boundary exists for scaling dynamics and resource isolation, not
because storage and compute are independent products.

- **Toron's reason for existence is smart-storage pushdown.** Every cut
  polishes a system doing its primary job along its primary axis.
- **Evidence drives scope.** The most informative artifact for the next ADR
  is the previous ADR's bench results or a code-evidence silent-drop /
  inert-feature defect surfaced during selection.
- **Minimize strategic rework.** Two narrow ADR shapes otherwise equivalent
  — prefer the one preserving plausible long-term seams.
- **Ring-product placement** (`plans/conventions/ring-product-placement.md`):
  Interior / shared substrate stays in this Toron sequence; Exterior
  ring-specific products return home to their own ring.
- **Complete cut, not granular slice.** Fold adjacent scope; defer only at
  genuine mechanism/topic boundaries.

## Forward sequence

### ADR 124 promoted; ADR 125 candidates

ADR 124 promoted **bounded live-partition backpressure** — the producer-side
repair of 123's deliberately-dropped natural-backpressure boundary. Pacing
semaphore additive over the existing `LivePartition.chunks` replay ledger
(`LIVE_BACKLOG_DEPTH = 4`); async `PartitionStore::append_live` via
`async_trait`; `farthest_released_index` invariant against retry-replay
permit double-release; sealed materialization preserved (Codex A locked after
Gemini conceded "no sealed materialization" on `scheduler.rs:856-885`'s
no-restart grounding). **Honesty boundary:** bounds unread backlog per
partition, NOT total live shuffle memory — producer-side `partition_batches`
retention at `server/mod.rs:470-471` preserved in V1 as a named follow-on.
Twelfth grounding-correction in the 119→124 arc.

**124 follow-ons:** `partition_batches` retention removal (constant-memory
live shuffle from claim to truth); executor build/probe reorder (end-to-end
natural backpressure for joins); configurable `LIVE_BACKLOG_DEPTH` via
`QueryBudget`; consumer-pull-side bounded channel.

**123 follow-ons (still-deferred):** output-side joins-as-streamable-producers;
multi-producer fan-in + remote live transport (fragmenter parallelism +
cross-worker live reads); mixed streamable+sealed multi-upstream (A2);
Rune-authored broadcast join + SQL hint (broadcast execution exists
end-to-end at `fragment.rs:473`; only the frontend bridge is missing).

**Identity tier:** logical-type lifecycle (`DROP`/`ALTER LOGICAL TYPE` —
cross-ring DROP + referential integrity, bounded 090 re-open);
declared-additive types (`SUM(Money) → Money` via a logical-type property —
narrows the 113 deferral); PK/FK-derived identity (blocked on FK support at
`sql_planner.rs:663`).

**Frontend:** qualified wildcards (`t.*` + mixed — planner↔binder
restructuring or a `Wildcard` IR variant); qualified DML RETURNING; DML
target aliases / qualified assignment targets; SQL joins around derived
tables / GROUP BY over joins; Rune sort/limit; SIMILAR TO / RLIKE / REGEXP.

**Distributed / execution:** streamable Sort/Aggregate coverage (120 V2 —
top-k bounded buffers + partial+merge); row-group-lazy-from-disk scan (118
follow-on — per disk page laziness); cost-based broadcast strategy selection
(needs cardinality estimates not in IR today).

**Optimizer / catalog:** general project-fusion beyond 105's adjacent-identity
case (evidence-gated); further `information_schema` relations (constraints /
indexes / PK-FK / views — each gated on the harder modeling pieces);
Wendelon `*V1`/`*v1` de-version sweep (Wendelon-home chore, placement
follows code-locality).

**Long-horizon:** subquery/`UNNEST`/row-value `IN`; `CHECK`/`DEFAULT`;
`ALTER`/composite-PK/`UNIQUE`; pre-op intent audit; fragmentable bulk DML;
JWT/OIDC/SPIFFE; gRPC/Arrow Flight/Postgres frontends.
