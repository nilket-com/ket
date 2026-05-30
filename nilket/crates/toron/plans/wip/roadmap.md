# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) — git holds the
archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not implementation
commits. The roadmap lags the code on purpose — check `git log` and the
relevant impl plan for actual status. Per-ADR decisions live in
`plans/adr/NNN_*.md` + each ADR's §"What to do next"; this file holds only
the forward view + the active line.

## Status

ADRs 001–139 promoted (implemented unless a `feat` is still pending — check
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

### ADR 139 promoted; ADR 140 candidates

ADR 139 promoted **Source-Aware Broadcast-Cap Hash Replay** — the keystone that retires ADR
138's named availability residual (an estimated broadcast whose *actual* encoded bytes exceed
the runtime cap fails the query where hash would have completed). The runtime caps a broadcast
build against actual encoded Arrow IPC payload bytes (`PartitionPayload::size_bytes`), failing
with a typed `BudgetExhausted(BroadcastReplication)` — never wrong rows — so an over-cap
broadcast is a **resource-admission miss, not a query meaning error**. 139 converts it into a
**one-shot, coordinator-owned, whole-query hash replay**: the coordinator already runs a
`!batch_sent`-gated whole-query retry for descriptor evolution (`coordinator.rs:836`), so the
replay is the same loop with a force-hash policy on the second attempt — no in-flight subtree
re-fragmentation (broadcast and hash are disjoint fragment topologies). **Who degrades depends
on how the broadcast was authored** (the operator's Rune/SQL steer): `JoinStrategyIntent::Pinned`
gains a `source: {Direct, SqlHint, Rune}` provenance (it previously erased authoring). **Auto**
(138 estimate / 136 hard) and **SQL `/*+ BROADCAST */`** broadcasts **replay → hash** (protect
the naive user); **Rune `broadcast_join`** and **direct/programmatic** pins **stay terminal**
(absolute control / explicit command — fail-loud). This synthesizes the fail-loud-for-authored-intent
discipline (kept on Rune + Direct, the prescriptive/programmatic surfaces) with the
pragmatic degrade (SQL, the compatibility surface) — [[rune_is_primary_interface]]. A plan with
*any* terminal pinned broadcast does not replay (keeps the replay attempt provably
broadcast-free; mixed-source per-join replay is the follow-on). Trigger is **`BroadcastReplication`
only** — `ShuffleBytes` is excluded because `shuffle_bytes_published_per_task` is a per-task
*total* and a single-producer broadcast build's hash version publishes the same total against the
same budget (hash replay can't relieve it; replication is what hash eliminates). Replay preserves
the external `QueryId` under a fresh stage/exchange id namespace (not `AttemptId` — the scheduler
rewrites attempt ids) so attempt-0 published partitions can't collide; one-shot; reads only
(`!batch_sent`-gated; mutation paths out); demoted SQL broadcasts install hash exchanges; the
estimate-driven case returns the **forced-hash digest** (byte-identical — the V1 bar).
Consensus: one-shot replay mechanism 3-of-3; source-aware D4 policy operator + Codex + Claude
(after three grounded concessions — iter-0 tractability, iter-4 Direct-pins + ShuffleBytes).
Headline gate: `estimated_broadcast_over_cap_replays_as_hash_and_matches_digest` (Rune+SQL).

**ADR 140 lead candidates:** **targeted mixed-source per-join replay** (join identity
bind→fragment→failure — closes 139's mixed-plan SQL-protection gap and removes the
whole-query-replay bluntness); **length-carrying logical-type refinement** (promotes
`Utf8`/`Binary` from a 138 *estimate* to a *hard* `byte_upper_bound` — shrinks the estimate
surface; likely a Toron+Wendelon pair); **`Take`/`LIMIT` single-base lineage** (129-retarget so
row-capped dims broadcast); generic `ShuffleBytes` recovery via multi-producer fan-out; the
**136-W** Wendelon hard-facts producer (Wendelon-home); remote-live multi-producer fan-in;
multi-column / histogram selectivity; join reordering. Evidence-gate the next pick on the 139 feat.

### ADR 138 promoted; ADR 139 candidates (HISTORICAL)

ADR 138 promoted **Estimate-Tolerant Per-Row Materialized-Size Model** — the cut where the
council, on an operator steer, traded the spine's "hard upper bound only" purism for an
honest estimate. 137 left a row-bounded relation with `row_upper_bound` set but
`byte_upper_bound` untouched, so a *string-bearing tiny dimension* still hashed: the chooser
is byte-only and nothing converts rows to bytes, and there is no hard length signal for
`Utf8`/`Binary` (refinements are value-only; the descriptor carries no per-column width). The
grounded reframe: the runtime caps a broadcast build against **actual encoded Arrow IPC
payload bytes** (`PartitionPayload::size_bytes`), failing with a typed `BudgetExhausted` —
never wrong rows — and already tolerates an imprecise planner (`cap/4` = 16 MiB ceiling vs a
1 MiB default threshold vs 64 MiB runtime cap → ~64× headroom *by design*). So undercount is
an **availability** concern, not a correctness one, and a conservative over-count estimate
beats refusing to broadcast every string dimension forever. **D1:** a new
`estimated_byte_size` channel beside the **untouched** hard `byte_upper_bound`; **138 never
writes the hard field** — Toron computes *estimates*, only the storage ring (136/136-W) issues
hard byte facts (the cross-ring [[logical_identity_receipt_doctrine]] cut). Fixed-width is an
exact-*ish* estimate, string/binary a looser one — **both** in `estimated_byte_size`, after
Codex grounded that Toron cannot prove the encoded-IPC byte unit the runtime actually meters.
**D2:** over-count biased, validated `estimated_byte_size ≥ encode_record_batch(..).len()` at
`R ∈ {1,8,1000}` (the real unit, framing included); any nested/dict column ⇒ estimate `None`.
**D3:** chooser prefers hard (`broadcast_inferred`), falls back to estimate
(`broadcast_estimated`) only when hard is absent — and reads byte-typed fields only, so 137's
chooser-byte-only guard stays green. **D4:** distinct `BroadcastEstimated` receipt → EXPLAIN
labels estimated vs hard (134). **D5:** binder-local; derive at `from_table` (descriptor rows)
+ post-137-PK-cap, carried through 137's row-local helpers; default threshold **unchanged**.
**`Take` dropped (grounded):** `take` returns `Composite`, and the chooser only reads build
bytes for `SingleBaseRelation`, so `LIMIT n` cannot broadcast without a 129-retargeting
lineage decision — a named follow-on, not an accidental side effect (corrected an earlier
"fold 116" framing). **Named residual:** until ADR 139, a gross (>headroom) under-estimate of a
genuinely-large input fails the query where hash would have succeeded (availability, not wrong
rows). Genuine 3-of-3 + operator on B′ after two iter-3 promotion blockers were ground shut
(Take lineage; encoded-IPC byte unit → fixed-width demoted to the estimate channel). Headline
gate (Rune+SQL): `string_bearing_tiny_dim_infers_broadcast_by_estimate` from a hard row source
(fails on `main`); seven `adr138_*` guards + 129–137 regressions incl. the two 137 honesty
guards un-retargeted.

**ADR 139 lead candidates:** **runtime broadcast-build → hash fallback** (the keystone the
138 reframe named — catch the build-side `BudgetExhausted` and degrade *that join* to hash
instead of failing the query; retires 138's availability residual *and* independently hardens
every existing hard-bound broadcast against a mis-set cap; distributed-runtime ADR —
fragmenter/server/scheduler) — strong continuity, my lean; a **length-carrying logical-type
refinement** (would promote `Utf8`/`Binary` width from estimate to a *hard* bound); **`Take`/
`LIMIT` single-base lineage** (129-retarget so limited dims broadcast); the **136-W** Wendelon
hard-facts producer (Wendelon-home); remote-live multi-producer fan-in; multi-column /
histogram selectivity; join reordering. Evidence-gate the next pick on the 138 feat.

### ADR 137 promoted; ADR 138 candidates (HISTORICAL)

ADR 137 promoted **Fact-Spine Preservation Through Row-Local Volume-Non-Increasing
Nodes** — the natural IQ step now that 136 made base facts real in production. 136 let
`byte_upper_bound`/`row_upper_bound` flow, but the binder threw them away the moment a
relation was wrapped: `Filter` (`binder.rs:380`) and `Project` (`:396`) each replaced
the child facts with `composite_with_row(...)` (`PlanOrigin::Composite`, byte `None`),
so the join chooser's single-base + byte-bound requirement (`:3950/3953`, `:3889/3897`)
rejected the build → every build-side `Filter`/`Project` shape silently demoted a tiny
dimension to hash. Yet a filter only *removes* rows and a column-subset project only
*drops* columns — neither grows the relation, so the hard upper bounds stay valid and
the origin stays single-base. **D1:** `Filter` preserves the full single-base spine
(origin+byte+row).
**D2:** `Project` preserves byte facts **only** for a pure distinct-column
subset/permutation; an expression/cast/literal/dup project keeps row + single-base
lineage but **drops byte** (it may grow bytes); aggregating/expanding stays `Composite`.
**D3:** single-column PK equality (`primary_key_columns.len()==1`, that column, non-null
literal) caps `row_upper_bound=1` but **never shrinks byte** — the chooser is byte-only,
so a PK-filtered large table **caps rows yet still hashes**; gated honestly as *caps
rows, still hashes* (the [[feedback_cut_must_deliver_the_name]] discipline — no name the
mechanism can't deliver). **D4:** one guarded place — `preserve_volume_non_increasing` /
`preserve_through_project` / `with_pk_row_cap` on `PlanOptimizerFacts`, so the invariant
can't silently rot. **D5:** binder-local — no new fact unit, width model, or
substrate/wire/fragmenter/scheduler change; EXPLAIN (134) is the validation surface.
**`Sort` considered and dropped (grounded):** it is the third `composite_with_row` site
(`:479`) and volume-non-increasing, so folding it looked free — but every surface
`ORDER BY` lowers as `Sort { input: Exchange{partition_count:1} }`
(`apply_order_by_limit_tail`), and the one-partition `Exchange` already resets origin to
`Composite` *below* the Sort, so Sort preservation is **inert** for SQL/Rune; revisit
only if a direct-IR un-exchanged `Sort` becomes reachable (Codex's caught caveat,
confirmed against `sql_planner.rs`). Genuine 3-of-3 on B′ (Gemini withdrew the
overclaimed `pk_filtered_large_table_infers_broadcast` gate, conceding the byte-only
grounding). Headline binder gate: `filtered_tiny_dim_infers_broadcast` (fails on
`main`); five `adr137_*` guards + 129–136 regressions.

**ADR 138 lead candidates:** **hard per-row materialized-width model** (the direct
138 follow-on — converts a row cap into a `byte_upper_bound` so a PK-filtered/known-small
row from a large table actually broadcasts; bounded-width schemas: fixed Arrow types
always, `Utf8`/`Binary` via a refinement max-length, else keep input byte bound) — the
natural completion of 137's honestly-deferred half; the **136-W** Wendelon hard-facts
producer (the other half of the 136 pair, Wendelon-home); remote-live multi-producer
fan-in (complete 135's live path for pinned-Rune broadcast); richer cost units;
multi-column / range / histogram selectivity; join reordering. Evidence-gate the next
pick on the 137 feat.

### ADR 136 promoted; ADR 137 candidates (HISTORICAL)

ADR 136 promoted **Wendelon-Backed Hard Table-Size Facts (Toron consumer + default
flip)** — the "data data data" cut that makes the 129–135 CBO spine fire on real
production tables instead of test-only `seed_optimizer_facts`. Today
`from_descriptor_with_logical_types` ends at `optimizer_facts:
TableOptimizerFacts::default()` (`catalog.rs:204`) → every Wendelon-built catalog is
blind → hash. 136 lets hard table-size facts ride a **descriptor-adjacent sidecar**
(keyed by `(tenant, table_object_id, schema_generation, physical_fingerprint,
manifest_epoch)`; `TableSchemaDescriptor` untouched) into `TableOptimizerFacts` via
facts-aware constructor/registry/bootstrap/sync paths; descriptor-only paths keep
defaulting unknown. **Cross-ring pair:** this council promotes **136-T** (the Toron
consumer + flip); the **136-W** producer (table-identity-keyed hard-facts ledger) is
named as a precise contract and handed to Wendelon's sequence — Wendelon owns its
producer; Toron holds no stats cache. Hard bounds only: unknown/absent/mismatched →
hash; producer-stale facts arrive as absent/unknown. Overcount suppresses broadcast
(safe); **undercount is a correctness bug**. Two load-bearing safety calls: the byte
fact is a
**materialized** build-footprint upper bound (named `materialized_byte_upper_bound`),
never compressed storage `size_bytes` (which would undercount decompressed Arrow
size → wrong broadcast) — Claude's correction; and descriptor refresh must update
`optimizer_facts` **even when descriptor identity is unchanged** (facts track
`manifest_epoch`, a tiny dim grows without a schema change) — Codex's correction.
`PlannerConfig::default()` flips to `Some(1 MiB)` (clamped by the 16 MiB cap
ceiling) — **inert until 136-W ships** (no facts → do-no-harm hash), so 136-T lands
first and is fixture-testable now. Known-empty normalizes to both `row_upper_bound`
0 and materialized bytes 0, so it broadcasts below cap while staying distinct from
unknown (folds ADR 095). EXPLAIN (134) is the validation surface.
Quorum: operator + Claude + Codex; Gemini held selectivity (→ 137 lead). Headline
gate: `star_query_infers_broadcast_without_seed_optimizer_facts` (SQL + Rune).

**ADR 137 lead candidates:** **fact-spine selectivity propagation through filters**
(Gemini — row-local nodes preserve upper bounds; PK-equality `col = literal` →
`row_upper_bound` 1, but broadcast still needs a separately proved hard byte cap
under the current byte-first chooser; `fact ⋈ (tiny_dim WHERE …)` infers broadcast)
— the natural IQ step now that base facts flow; the **136-W** Wendelon producer ADR
(the other half of the pair); remote-live
multi-producer fan-in (complete 135's live path for pinned-Rune broadcast); richer
cost units; join reordering. Evidence-gate the next pick on the 136 feat.

### ADR 129 promoted (HISTORICAL)

ADR 129 promoted **Optimizer Fact Spine for Tiny Broadcast Inference** — the
roadmap pivot's first step toward a cost-based optimizer. Builds a conservative
**fact spine** (hard `byte`/`row` upper bounds on `TableBinding`, propagated to
bound subplans) and spends it on one fenced rule: unhinted INNER joins whose
**syntactic-left** input is a single base relation with a hard byte bound below an
effective dark-launched threshold infer `JoinStrategy::Broadcast`; everything
else stays hash. Seam is **binding-time** (grounded: `SqlPlanner` is a
catalog-free unit struct; the binder owns catalog + `PlannerConfig`) via a single
`choose_join_strategy(...) -> {strategy, reason}` helper (the spine-honesty gate —
a 2nd CBO rule is a new arm, not a new branch). `LogicalPlan::Join` gains
`JoinStrategyIntent::{Auto, Pinned}`; `ExecutionPlan::Join` stays concrete; `Auto`
never survives binding. Fork A exchange placement (pinned stays planner-shaped,
auto shaped in binder) as **named temporary debt**. Row facts from `Take`/`LIMIT
0`/`EmptyRelation` folded into the spine but **carried, not scheduled on** (no
fragmenter/dispatch change). Config dark-launched `None`; the effective threshold
is capped by a static conservative broadcast-cap ceiling until actual
`QueryBudget` reaches binding, so production inference stays inert until a real
Wendelon-facts producer ADR plus knob enablement. Genuine 3-of-3 after three
grounding corrections (planner→binder placement; hard-bounds over `confidence`;
no-hash-hint precedence gate — ADR 128 only added `BROADCAST`). Retargets
122/125/128 join guards; do-no-harm inference, fail-loud hints unchanged.

**ADR 130 lead candidates** (the CBO arc now has a spine to extend): right-side
build selection + output-order preservation (the real `fact JOIN dim`
star-schema win — dim on the right); Wendelon-backed live table-size facts
(storage-ring producer ADR — flips 129's config to default-on); unify exchange
insertion (pays the Fork-A debt once a pinned-reshaping rule earns it);
selectivity propagation; fold ADR 116 take / ADR 095 empty-relation into the
general fact model; zero-worker / coordinator-local placement; optimizer
`EXPLAIN` over the reason receipts. Evidence-gate the pick on the 129 feat.

### ADR 128 promoted (HISTORICAL)

ADR 128 promoted **SQL broadcast join hint authoring** — closes the broadcast
authoring story (125 did Rune; this does SQL). `SELECT /*+ BROADCAST(alias) */
… INNER JOIN …` lowers to the existing `JoinStrategy::Broadcast` via a strict
raw-text recognizer over `OptimizerHint.text`; build-side resolution through
121's `plan_qualifiers`; no-silent-fallback reject of malformed / right-side /
non-INNER / ambiguous / composite-left / unused hints; EXISTS stays hash. **No
dialect/parser/substrate change** — `GenericDialect` already captures comment
hints (`generic.rs:288`); the cut replaces `lower_select`'s blanket hint reject
(`:2560`) only. Behavioral 3-of-3 with the promoted mechanics incorporating
three sqlparser-layer grounding corrections (wrong crate version 0.59→0.62;
hints are raw text not parsed `Expr`; `GenericDialect` already captures →
Claude's `ToronDialect` was unnecessary and conceded iter-2). Retargets
`adr125_no_sql_hint_authoring_yet` →
`adr128_sql_broadcast_hint_authoring_is_narrow`; adds a
`generic_dialect_remains` guard memorializing the no-dialect-change correction.

**ADR 129 lead candidates** (no strong continuity pull — broadcast surface now
whole, live-shuffle memory chain has one remaining link): chunk-granular sealed
manifest (the last live-shuffle memory link — true 1-copy, consumer-`pull`-API
break); executor build/probe reorder → streamable joins (the big distributed
arc); non-INNER broadcast widening (per-kind binder/fragmenter proofs);
frontend (qualified `t.*`, joins over derived tables). Evidence-gate the pick
on whatever the 128 feat surfaces.

### ADR 127 promoted (HISTORICAL)

ADR 127 promoted **Live-chunk release after seal** — the second link of the
live-shuffle memory chain, collapsing the 2-copy steady state ADR 126 left.
Grounded 3-of-3 (Claude final pass, Codex iter-1, Gemini iter-1) on: a
lock-coupled `active_readers` count on `LivePartition` (no count for a
fallback-only `pull_live`); a stream-owned reader guard with dual duty (decrement
once on both stream-end paths **and** preserve 124's unsealed-abnormal-drop
cancellation); idempotent `release_live_chunks_if_idle` (drop `live.chunks` +
the 124 append-side `live.reservations`, never the 126 sealed reservation)
triggered at seal-if-idle and last-reader-end; and the **correctness spine** —
`pull_live` falls back to the sealed payload from `inner.partitions` when
`sealed && chunks` empty (bypassing pacing, yielding once for index 0, erroring
on missing-sealed rather than silent EOF). This also closes a latent
silent-empty-read bug at `partition_store.rs:733`. **Topic was an operator
steer to B over the peers' iter-0 lean to C (SQL broadcast hints)** —
conditioned on the fallback, exactly the B-blocker Codex named; surfaced not
papered. Named for the common-case reclaim (one resident copy after
release/drain), NOT constant memory (worst case — reader mid-stream at seal —
is inherent; literal-constant is the deferred chunk-granular manifest). No
scheduler/consumer/wire change. 15th grounding-correction arc note: the final
promotion corrected the shorthand "3-of-3" wording against the actual peer
files — convergence was substantive, not a same-number iteration claim.

### ADR 126 promoted (HISTORICAL)

ADR 126 promoted **Live-shuffle seal correction** — the first link of ADR
125's deferred "live shuffle memory chain." Genuine 3-of-3 (Fork A) after
Codex's iter-0 *proved* (throwaway Arrow IPC test) a latent multi-chunk
data-loss bug: `seal_live`'s chunk fallback byte-concatenated N framed IPC
streams, and the single `StreamReader` stops at the first EOS → only the
first chunk's rows survive (invisible because the one covering test used
synthetic non-IPC bytes). The cut makes `live.chunks` the **single source**
seal derives from via streaming Arrow IPC reassembly (decode→write→drop per
chunk); drops `partition_batches` decoded retention + the live producer's
redundant `publish_task_output` + `TaskBatch` transmission (general publish
path preserved for non-live producers); carries `schema: Option<SchemaRef>`
on `LivePartition` so empty partitions seal as schema-bearing IPC; reserves
the sealed payload atomically (no partial publish on budget exhaustion);
manifest-derived telemetry; **retargets the one 124 guard**
(`adr124_seal_live_materializes_from_chunks` → reassembly invariant). Named
"correction," NOT "single-pass/constant-memory" — encode count stays 2,
resident copies stay 2 (chunks stay resident for in-flight `pull_live` +
retry-from-zero). 14th grounding-correction in the 119→126 arc (Claude's
"3→1 copy" headline + "no 124 guard inverts" both corrected; Gemini conceded
Fork B→A on grounded schema-availability). Substrate behavior change; no new
IR/opcode/wire type.

**Live shuffle memory chain — remaining link** (post-127):
chunk-granular sealed manifest (true constant-memory / single-copy:
multi-entry-per-partition manifest OR streaming sealed assembly — the only
path to "1 copy" without retaining one sealed partition payload in the store).

**Distributed execution arc:** executor build/probe reorder (pairs with
124 for end-to-end natural backpressure); output-side
joins-as-streamable-producers (widen `streamable_unary_plan` for `Join` +
restructure executor); multi-producer fan-in + remote live transport
(Codex's three-substrate framing); configurable `LIVE_BACKLOG_DEPTH` via
`QueryBudget`.

**Broadcast continuation:** non-INNER broadcast widening (binder + fragmenter
per-kind correctness proofs); cost-based strategy selection (needs cardinality
estimates not in IR today).

**Identity tier:** logical-type lifecycle (`DROP`/`ALTER LOGICAL TYPE` —
cross-ring DROP + referential integrity, bounded 090 re-open);
declared-additive types (`SUM(Money) → Money` via logical-type property);
PK/FK-derived identity (blocked on FK support at `sql_planner.rs:663`).

**Frontend:** qualified wildcards (`t.*` + mixed — planner↔binder
restructuring or `Wildcard` IR variant); qualified DML RETURNING; DML
target aliases / qualified assignment targets; SQL joins around derived
tables / GROUP BY over joins; Rune sort/limit; SIMILAR TO / RLIKE / REGEXP.

**Distributed (other):** streamable Sort/Aggregate coverage (120 V2 —
top-k bounded buffers + partial+merge); row-group-lazy-from-disk scan
(118 follow-on — per disk page laziness).

**Optimizer / catalog:** general project-fusion beyond 105's
adjacent-identity case (evidence-gated); further `information_schema`
relations (constraints / indexes / PK-FK / views); Wendelon `*V1`/`*v1`
de-version sweep (Wendelon-home chore).

**Long-horizon:** subquery/`UNNEST`/row-value `IN`; `CHECK`/`DEFAULT`;
`ALTER`/composite-PK/`UNIQUE`; pre-op intent audit; fragmentable bulk DML;
JWT/OIDC/SPIFFE; gRPC/Arrow Flight/Postgres frontends.
