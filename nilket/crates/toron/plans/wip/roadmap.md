# Toron Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) — git holds the
archaeology, this file looks forward.

**Update cadence.** Rewritten at planning sessions, not implementation
commits. The roadmap lags the code on purpose — check `git log` and the
relevant impl plan for actual status. Per-ADR decisions live in
`plans/adr/NNN_*.md` + each ADR's §"What to do next"; this file holds only
the forward view + the active line.

## Status

ADRs 001–145 promoted (implemented unless a `feat` is still pending — check
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

### ADR 145 promoted; ADR 146 candidates

ADR 145 promoted **SQL Derived Tables in Joins (+ Projected Build-Side Byte
Estimation)** — completes the SQL filtered-build authoring surface 143/144 opened,
and unlocks **build-side column pruning**. SQL rejected a derived table as a join
factor (`sql_planner.rs:2778, 2792`), so a user couldn't author the projected
filtered-dim shape Rune builds directly — `Join(fact, Alias(Project(Filter(Scan))))`
— that prunes the build to the join's needed columns. **The load-bearing council
correction: this is NOT a frontend-only cut.** All three iter-1 drafts (Claude
included) asserted the column-pruning win falls out of lifting the reject; Claude's
iter-2 checked the byte path and showed it does not — `estimated_byte_size` is
derived at the **Filter** on the *full* schema (`binder.rs:614`), and a pure-subset
`Project` preserves that full-width estimate **unchanged** (`preserve_through_project`,
`binder.rs:235`), and the chooser reads the stored fact, not the projected schema. So
the cut is **two coupled halves**: **D1 (frontend)** admit v1 derived tables as join
factors in all-inner chains (reuse `validate_derived_select_shape`; no new IR); **D2
(binder)** re-derive `estimated_byte_size` at a pure-subset project from the projected
`output_schema` (`derive_estimated_byte_size` is `SingleBaseRelation`-gated,
`min`-combining, schema-width → a **monotonic estimate shrink**, estimate-channel
only, never the hard `byte_upper_bound`). D2 is a **latent over-estimate fix that
travels with the ADR** — every pure-subset project today (Rune `.select()` included)
carries inflated full-width bytes. **Honesty gate** (`sql_join_projected_derived_dim_broadcasts_where_full_column_hashes`):
the projected derived dim broadcasts where the full-column 143 form hashes under
identical 144 facts — **fails on `main`, and fails if either half is reverted**. The
headline works via **fact propagation** (the selective predicate lives inside the
derived table → inner Filter reads scan facts → D2 carries the reduced byte estimate
up through Project + Alias); no through-`Alias` fact lookup added; 143 pushdown +
all-inner fence preserved. Reconciles the guards D2 reopens (143 no-longer-blanket-
derived-reject; 137 still owns conservative hard byte; 138 still estimate-only).
Genuine 3-of-3 (D1 from iter-1; D2 — Claude's iter-2 byte-path catch — carried by
the merged final plan). No chooser, executor, storage, or Wendelon change beyond
the named binder estimate refinement.

**ADR 146 lead candidates:** **136-W hard table-size producer** (light the dark
hard channel — live `row_upper_bound`/`byte_upper_bound` are `None`,
`catalog.rs::descriptor_optimizer_facts_from_storage`; a Wendelon producer of hard
table-size bounds the 129/136 spine already consumes); **hard zero-row folds /
hard-from-exact** (complete-small-set membership absence + 144 out-of-range →
*hard* 0-row → prune/short-circuit, fold ADR 095; needs the freshness doctrine,
narrowed per the 145 cycle to provable-empty since Exact NDV gives no hard
non-empty bound); **KLL/quantile range precision** (skew-aware 144 follow-on; a new
maintained producer, evidence-gate on a measured min/max-uniformity miss);
**float range selectivity** (the one excluded type Toron *has* — producer float-tag
+ order-preserving IEEE encode + continuous consumer); **temporal types in Toron**
(the big unlock behind 144's blocked date/timestamp range consumer — Toron's
`ScalarValue` is `{Bool,Int32,Int64,Float32,Float64,Utf8}`; a multi-ADR feature,
could start with `Date32`); 140 A1; multi-column/correlated selectivity. **Forward
arc:** `... → 145 column pruning → (hard channel: 136-W / hard-from-exact) →
KLL/quantile → Theta-sketch join-key intersection → join selectivity → join
reordering` (Theta stays gated on both a set-sketch substrate *and* a
join-reordering consumer, neither of which exists). Evidence-gate the next pick on
the 145 feat.

### ADR 144 promoted; ADR 145 candidates (HISTORICAL)

ADR 144 promoted **Labeled Range Selectivity from Maintained Min/Max** — the second
stats-backed selectivity class (the first since equality NDV), attacking range
predicates (`< <= > >=`, `BETWEEN`) that got a blunt default (the 141 guard
`range_filter_does_not_use_ndv_selectivity` pinned it). Wendelon already maintains
**always-on** per-column min/max + null/non-null counts
(`build_always_on_column_stats_block`, `stats.rs:638`) — *always-on*, unlike the
*opt-in* HLL/small-set NDV the 141→143 path rides, so range selectivity **fires
broadly day-one**. v1 is **min/max** (KLL/quantile skew-precision is the named
follow-on, `sketch.rs` is query-aggregate-only). A cross-ring cut mirroring 142:
Wendelon adds `column_range: Vec<ColumnRangeFact>` (additive on the 142
`TABLE_OPTIMIZER_FACTS_GET` blob); the range scalar tag/decode contract is
schema-owned and re-exported through `wendelon-client` (no Toron import of Wendelon
storage stats), and mixed-version postcard compatibility must be explicit rather
than assumed from `serde(default)`. Toron maps the fact and the binder derives a
**labeled approximate** uniform selectivity into the **estimate channel only**
(`estimated_row_count`, never `row_upper_bound`/`byte_upper_bound`, never elides a
scan), feeding 138's byte estimate → broadcast. **The load-bearing grounding:** live
facts map `row_upper_bound` to **`None`** (`catalog.rs::descriptor_optimizer_facts_from_storage`),
so the fact **carries its own `non_null_count`** and the estimate derives from it —
estimating from `row_upper_bound` would have shipped **inert** (the 141/142
build-but-dark lesson; Codex's iter-2 catch, verified, over Claude's iter-1
`base_rows`). **Discrete-domain interpolation** `ceil(non_null_count · overlap /
(max−min+1))`; **`BETWEEN` is a real band** (intersect same-column bounds, tightest
[low,high], contradictory → 0; over a stacked-`min`); **estimate from min/max — not
NDV** (D8 retargets the range guard; NDV stays equality-only); **full-contributor
coverage or absent** (reuse 142's fence — a partial/truncated merge narrows the
range → under-estimate → over-broadcast, the aggressive direction); **141
no-independence** (single tightest estimate across equality + range, no product).
**v1 consumer decodes `Int32`/`Int64` only** (Toron's literal surface);
unsigned/date/timestamp ride the sidecar fail-open; float/decimal/utf8/binary/bool
fail open. No default-policy flip, no hard bound, no scan elision. Genuine 3-of-3
(min/max-v1 from iter-1; `non_null_count` inertness fix + band + discrete interp
converged Codex+Gemini, conceded by Claude on verified grounding). Headline gate:
`range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate` (fails on
`main`).

**ADR 145 lead candidates:** **KLL/quantile precision** (skew-aware range
selectivity where min/max uniformity is too coarse — a *new maintained* producer,
evidence-gated on 144's feat showing uniformity-miss cases); **SQL
derived-tables-in-joins** (the Toron-local frontend completion 143/144 deferred —
lift the `FROM fact JOIN (SELECT … WHERE …) d` reject, machinery exists at
`lower_derived_table:3624`, spine survives `Alias`, `binder.rs:560`); **date/timestamp
+ unsigned range consumer** (decode the tags 144's producer already emits — a small
consumer-only extension); **hard-from-exact** (freshness/coverage doctrine letting a
fresh complete `Exact`/full-coverage min/max write a hard bound — would let 141/144
provable-empty become hard); **stats default-policy flip** (widen NDV coverage past
opt-in); **Theta-sketch join-key intersection** (the join-cardinality arc; heaviest);
140 A1; multi-column/correlated selectivity. **Forward arc:** `NDV equality (141) →
142 producer → 143 SQL → 144 min/max range → KLL/quantile → Theta-sketch join-key
intersection → join selectivity → join reordering`. Evidence-gate the next pick on
the 144 feat.

### ADR 143 promoted; ADR 144 candidates (HISTORICAL)

ADR 143 promoted **SQL Inner-Join Side-Local Filter Pushdown** — the
secondary-frontend completion of 141's headline, worth doing now that 142 made the
NDV facts live. SQL lowered `WHERE` *above* the join (`Filter(Join(...))`,
`sql_planner.rs:2782–2802`), so the chooser (`binder.rs:1420`, bottom-up, sees only
children) was handed the **unfiltered** dimension → 138 over-estimates → hash, where
Rune (`Join(..., Filter(dim))` by construction, `rune_engine.rs:302`) broadcasts.
143 splits the top-level `WHERE` conjuncts and pushes each **side-local** one onto
its **tightest single base input** during `lower_select`, reusing
`split_top_level_and` + `collect_scope_refs` (+ a new `ScopeRefs.inner` flag) +
`plan_qualifiers` (recurses through `Filter`), emitting existing
`LogicalPlan::Filter` nodes — **no binder/chooser/executor/replay/Wendelon change**.
**Placement resolved an iter-1 cross-over to the SQL frontend on grounding** (a
binder LP rewrite would reimplement the `SqlExpr` classifiers on logical `Expr`, and
its frontend-agnostic generality is illusory — Rune builds the filtered shape
directly, so SQL is the sole `Filter(Join)` producer; a binder phase is the right
refactor only when a second such frontend lands). **The load-bearing correction: the
all-inner-chain fence** — a *per-join* push (push at an inner boundary, skip at an
outer one) and an *upfront* push (wrap the owning factor before the chain is built)
are **both unsound** when an outer join sits *above* a pushed input
(`a INNER JOIN b RIGHT OUTER JOIN c WHERE b.x=1` → ∅ correctly, `{id, NULL}` if `b`
is pre-filtered, because `b` is on the RIGHT join's null-producing side); so pushdown
activates **only when every FROM join lowers to `Inner`**, else the full `WHERE`
stays above (byte-identical to `main`). **Tightest single input incl. the leftmost
base** — broadcast-relevant, not probe-only (the chooser evaluates a left build
candidate, `binder.rs:4268`, so a dim authored left broadcasts only when its leftmost
filter is pushed). Pushdown is **decoupled from NDV estimability** (push every
supported side-local predicate; non-estimable ones still earn runtime scan
selectivity); **residual preservation through the unchanged `lower_where`** (EXISTS
verbatim), pinned by a pushed-vs-above-join digest-equivalence gate. Retargets
`adr141_no_sql_join_filter_pushdown_in_this_cut` → six `adr143_*` guards. Genuine
3-of-3: topic + placement converged by iter-2; the fence correction (Claude, against
a per-join/upfront framing) adopted by **both** peers from a data-grounded
counterexample by iter-3; Codex's leftmost-is-broadcast-relevant refinement folded.
Headline gate: `sql_equality_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate`
(fails on `main`).

**ADR 144 lead candidates:** **quantile/range selectivity** — the *second* stats
producer (inequality selectivity via Wendelon KLL + 138 byte hardening; a new
maintained-producer extension; the named arc-next, now gated on 143's feat
evidence); **SQL derived-table-in-join** (lift the `FROM fact JOIN (SELECT … WHERE …)
d` reject, `sql_planner.rs:2778, 2784` — the other filtered-build-side authoring,
143's nearest neighbor); **mixed-chain path-aware pushdown** (143's named follow-on —
push a conjunct iff its target base is on the preserved side of every join above it,
widening past the all-inner fence); **stats default-policy flip** (a storage-cost
product call to widen NDV coverage past opt-in — would make 142/143 fire on more
tables); **hard-from-exact** (a freshness/coverage doctrine letting a fresh complete
small-set `Exact` write a hard bound); 140's A1 (join-identified replay);
multi-column stats. **Forward arc (named, not next):** `NDV equality selectivity
(141) → 142 producer → 143 SQL spends it → quantile/range → Theta-sketch join-key
intersection → join selectivity → join reordering` (HLL is single-column NDV, *not*
intersections — Theta-sketches are the likely family for join-key intersection
facts). Evidence-gate the next pick on the 143 feat.

### ADR 142 promoted; ADR 143 candidates (HISTORICAL)

ADR 142 promoted **Wendelon NDV Optimizer Fact Producer** — the **cross-ring activation cut** that
makes the built-but-dark 129→141 CBO consumer live for covered opt-in tables. 141 landed the Toron NDV consumer,
but live descriptor sync emitted no facts (`column_ndv: Vec::new()`); 142 has **Wendelon produce**
table-level per-column NDV from its already-maintained **opt-in HLL/small-set** stats and **Toron
fetch** it during descriptor sync into `DescriptorOptimizerFacts.column_ndv`. **Wendelon-owned fact
type** (`TableOptimizerFacts { …identity…, column_ndv: Vec<ColumnNdvFact> }`,
`NdvFact = Exact{count} | Approx{estimate, rel_err_ppm}`; no Toron import — Toron maps it). **A
separate, fail-open facts RPC** (`TABLE_OPTIMIZER_FACTS_GET` ~`0x46/0x47`, *not* folded into
descriptor get/list, which stay length-checked descriptor-only): fetch failure / unsupported / stale
identity / no coverage all degrade to descriptor-only sync — the endpoint is **advisory, never a
planning availability dependency**, and Toron caches no Wendelon stats. **Full live-contributor
coverage or absent (D3):** a table-level NDV requires every visible contributor — **active tail +
sealed L1 + tiered runs** — to carry compatible stats; otherwise the column is unknown, and the
producer **does not scan active data at sync time** to manufacture a fact (sparse rollout is the
right trade). **Merge raw HLL registers** (`HllRegisters::merge`, estimate from merged), not
per-contributor `DistinctCount` scalars; complete small-set union → `Exact`; quantize
`std_err`→`rel_err_ppm` at the boundary (141 chose integer error to keep facts `Eq`). **No
default-policy flip, no `TableSchemaDescriptor` mutation, no hard-bound write; the 141 consumer is
unchanged** — NDV stays labeled-approximate in the estimate channel. **Risk vocabulary corrected:**
the aggressive (over-broadcast) risk is an *under-estimated row count from an inflated NDV*
(HLL inflation / skew / stale / hot literal), bounded by 139/140 replay — a partial-subset merge
*under*-counts NDV → *over*-estimates rows → conservative; so all-contributors-or-absent is a
**fact-honesty** fence, not over-broadcast prevention (corrects the iter-1/Gemini framing). Genuine
3-of-3 on Codex's final iter-2; Gemini's reversed coverage-fence rationale surfaced + corrected.
Headline gate: `rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed` (fails on `main`).

**ADR 143 lead candidates:** **SQL join-filter pushdown** (side-local `WHERE` to the build / joins
around filtered derived tables — unlocks the SQL form of 141's headline, the secondary frontend; now
worth doing since 142 makes facts real); **stats default-policy decision** (a storage-cost product
call to widen NDV coverage beyond opt-in columns — would make 142 fire on more tables);
**quantile/range selectivity** (the *second* stats producer — inequality selectivity + 138 byte
hardening; KLL is a query aggregate, a new maintained-producer extension); **hard-from-exact** (a
stats-freshness/coverage doctrine letting a fresh complete small-set `Exact` write a hard bound);
140's A1 (exact join-identified replay); multi-column stats. **Forward arc (named, not next):** HLL
is for single-column NDV, *not* intersections — **Theta-sketches** are the likely Wendelon family
for join-key intersection facts → `NDV equality selectivity (141) → 142 producer → quantile/range →
Theta-sketch join-key intersection → join selectivity → join reordering`. Evidence-gate the next
pick on the 142 feat.

### ADR 141 promoted; ADR 142 candidates (HISTORICAL)

ADR 141 promoted **Labeled NDV Equality Selectivity Estimates** — the first stats-backed CBO cut,
attacking the *root* of the 138/139/140 arc (blunt estimates → over-cap broadcasts) instead of the
recovery symptom. Toron consumes a labeled **approximate** per-column distinct-count (NDV)
optimizer fact — `DistinctCountFact { Exact(u64), Approx { estimate, rel_err_ppm } }` (integer error
so it derives `Eq`) — riding the descriptor-adjacent fact sidecar (136 pattern: serialized
`DescriptorOptimizerFacts` gets `column_ndv` entries, normalized into a `TableOptimizerFacts`
`BTreeMap`, and then carried onto `ExecutionPlan::Scan` beside `primary_key_columns`). A `Filter`
proving `col = <literal>` over a single-base scan derives
`estimated_row_count = 0` for a hard zero-row base, else `max(1, ceil(base_rows / max(1, NDV(col))))`
into a **new estimate-only channel**
(`PlanOptimizerFacts.estimated_row_count`, never the hard `row_upper_bound` — even for `Exact`;
hard-from-exact needs a freshness/coverage doctrine, deferred), which feeds 138's
`estimated_byte_size` — so a common star-schema **build-side filtered dimension**
(`customers.filter(region == "north")` in Rune/direct IR; 137 leaves it at full dim size, 138 then
estimates too big) gets a realistic filtered estimate and broadcasts. **The runtime stays honest:**
approximate ≠ hard (guarded); equality-only (ranges need quantiles — the
*second* stats cut); top-level `AND` takes the **single most-selective factor** and stacked filters
take `min` (no independence product); a skewed-key under-estimate that over-caps is recovered by
139/140 replay (the net that licenses spending an approximate estimate). **Two grounded premises:**
Wendelon's HLL/small-set distinct-counts are **opt-in persisted CSB stats, not always-on**
(`ColumnStatsPolicyV1` defaults empty) → 141 is a 136-style **consumer-now / producer-named** pair
(141-T inert until 141-W, fixture-testable now); and SQL lowers `WHERE` **after** the join
(`sql_planner.rs:3693`) so the dim build side is unfiltered → the headline is **Rune + direct-IR**,
SQL join-filter pushdown a named follow-on ([[rune_is_primary_interface]]). Genuine 3-of-3 (no
divergence); Codex's `rel_err_ppm`-keeps-`Eq` and stacked-filter-`min` refinements folded. Headline
gate: `rune_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate` (fails on `main`).

**ADR 142 lead candidates:** **141-W** — the Wendelon NDV producer (expose table-identity-keyed
per-column NDV adjacent to descriptor sync, merging persisted CSB HLL across row-groups, preferring
a complete small-set as `Exact`; decide the opt-in-vs-default-stats-policy question) — the
cross-ring half that makes 141 fire in production; **SQL join-filter pushdown** (side-local `WHERE`
to the build / joins around filtered derived tables — unlocks the SQL headline 141 deferred);
**quantile/range selectivity** (the *second* stats cut — inequality selectivity + 138 byte-estimate
hardening; KLL is a query aggregate, not maintained, so a new Wendelon producer extension);
hard-from-exact (a stats-freshness/coverage doctrine letting a fresh exact small-set write a hard
bound); 140's A1 (exact join-identified replay); multi-column stats. **Forward arc (named, not
next):** **Theta-sketch join-key intersection** — HLL gives union/cardinality but *not* intersection
(inclusion-exclusion compounds error), so join cardinality (`|fact ⋈ dim|`) needs Theta-sketches;
a future Wendelon producer maintains per-join-key Theta-sketches → labeled-approximate join
selectivity → the line **NDV equality selectivity (141) → quantile/range selectivity →
Theta-sketch join-key intersection → join selectivity → join reordering**. Evidence-gate the next
pick on the 141 feat.

### ADR 140 promoted; ADR 141 candidates (HISTORICAL)

ADR 140 promoted **Failed-Task-Scoped Broadcast Replay** — sharpens ADR 139's blunt recovery.
139 made an over-cap broadcast an availability event recoverable by a one-shot whole-query hash
replay, but its `force_hash_joins` was **global**: one over-cap build demoted *every* replayable
broadcast in the query, and a Rune/Direct pin *anywhere* vetoed replay entirely — degrading the
common ADR 135 broadcast-star (lose dim1/dim3 to recover dim2). 140 gives each join a stable
**`JoinReplayId`** (the existing deterministic bind-time `join_index`, carried on
`ExecutionPlan::Join` with a reduced replay-eligibility tag, preserved through fragmentation),
replaces `force_hash_joins: bool` with `force_hash_join_ids: BTreeSet<JoinReplayId>`, and on a
`BudgetExhausted(BroadcastReplication)` failure collects the **replayable broadcast id *set* in
the failed task** from that execution metadata and replays forcing only that set to hash — healthy
broadcasts in other tasks/stages stay broadcast, and a SQL/Auto broadcast replays even when a Rune
pin lives elsewhere (the named 139 mixed-source gap). Source eligibility per 139 (Auto/SqlHint
replayable; Rune/Direct terminal) is now keyed by id, replacing 139's global terminal-pin veto,
without carrying full `JoinStrategyIntent` into execution IR. **The corrected trap:** *not*
"single-broadcast-or-terminal"
(both Gemini's and Claude's iter-1 floated it) — a star co-locates all dim builds in one consumer
task (`fragment_broadcast_join_plan`), so >1 broadcast → terminal where 139 *succeeds*; the policy
is **demote the set**, never worse than 139. **Honest scope:** A2 demotes the failed task's whole
replayable set — a single-task star still demotes all its dims (= 139 for the pure star), because
the worker's coarse failure carries no per-build id; per-dim-in-star precision is the **A1
follow-on** (a worker failure-hint + per-build cap validation — a wire/payload change the operator
scoped out: "small, Toron-local"). Every 139 fence preserved (one-shot, `!batch_sent`, fresh
non-`AttemptId` namespace, `BroadcastReplication`-only, mutation paths out, coordinator-owned, 138
split untouched). Consensus: A2 (Claude + Gemini + operator's "small, Toron-local" steer); Codex's
stronger join-*identified* A1 surfaced transparently as the follow-on, not folded. Headline gate:
`mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune` (fails on `main`).

**ADR 141 lead candidates:** **stats-backed optimizer facts (operator-confirmed next direction,
selectivity-first).** Wendelon is *not* absolute-facts-only — it has **KLL approximate quantiles**
(a query aggregate) and **maintained per-column row-group stats (HLL cardinality, bloom,
small-set)**. A 136-W-style cross-ring pair exposes these as **labeled-approximate** optimizer
facts Toron consumes in the estimate/selectivity channels — *never* as hard `byte_upper_bound`
receipts (channel honesty: approximate ≠ hard). **Lead with selectivity** — HLL cardinality is
already maintained per-column, so equality/filter/join selectivity is the best-grounded first
stats ADR (the real CBO substrate Gemini has wanted since 137, → eventual join reordering);
per-column byte/length quantiles are a *second* cut (a new producer extension — not maintained
today). This attacks the *root* of the 138/139/140 arc (bad estimates → over-caps) rather than
the recovery symptom. Also queued: **ADR 140's A1** (exact join-identified replay via a worker
failure-hint — `ShuffleConsumer { broadcast_join_id }` + per-build cap validation, for single-task
star per-dim precision); generic `ShuffleBytes` fan-out recovery; `Take`/`LIMIT` single-base
lineage. Evidence-gate the next pick on the 140 feat.

### ADR 139 promoted; ADR 140 candidates (HISTORICAL)

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
