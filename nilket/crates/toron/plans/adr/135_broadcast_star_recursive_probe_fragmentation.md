# ADR 135: Broadcast-Star Recursive Probe Fragmentation

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** The first
multi-way distributed-join cut: lift ADR 131's nested-join probe fence for **one
shape** — a left-deep INNER broadcast-star chain `fact ⋈ dim1 ⋈ dim2 …` (auto
builds are tiny single-base relations; pinned builds are already-authored
one-partition broadcast builds; the shared probe is the fact scan) — by
flattening it into **one scan-share probe stage fed by N one-partition broadcast
build upstreams**, reusing ADR 123's multi-upstream consumer fan-in. Delivers the
concrete star-schema win and unblocks the Rune-prescribed multi-way broadcast that
is fenced to hash today. NOT a general recursive fragmenter (deferred). No new
IR/stage/wire type.

**Category:** Toron interior — distributed fragmentation (substrate).
**Predecessors:** 123 (multi-upstream consumers / `TaskSpec.inputs: Vec<TaskInputBinding>`
— reused, not extended), 130 (right-build swap + restoring `Project`), 131
(scan-linear probe fence — widened here for the star shape only), 132 (per-link
dominance), 134 (EXPLAIN renders per-link receipts).
**Migration:** `plans/impl/135_broadcast_star_recursive_probe_fragmentation.md`.

## Doctrine — flatten the star, don't generalize the fragmenter
> 131 fenced nested-join probes to hash because `fragment_broadcast_join_plan`
> embeds the probe bare and `replace_first_join` only rewrites the first join. The
> star-schema win needs exactly one nested shape: a left-deep chain of tiny
> broadcast builds over a single shared fact scan. Grounding says the *stage
> topology already exists* — `TaskSpec.inputs` is a `Vec<TaskInputBinding>` and ADR
> 123 already runs multi-upstream consumers — so the cut is (a) a narrow binder
> eligibility widening for the star probe and (b) a fragmenter flattening of the
> left-deep chain into one scan-share probe stage + N build-publish upstreams. The
> fact is **scanned once** and probed against each broadcast build in declared
> order; the flatten is a physical regrouping that is **output-equivalent** to the
> unflattened plan (and to hash). Every other nested probe shape stays fenced —
> this earns one shape with a substrate proof, not a general claim.

## Decisions

**D1 — Binder star-eligibility (narrow widening of 131).** Widen
`broadcast_probe_is_scan_linear` (`binder.rs:3960`) so a join's probe may be a
**left-deep INNER broadcast chain**: after row-local `Filter`/`Project` wrappers, an
`ExecutionPlan::Join { strategy: Broadcast, left = one-partition build Exchange,
right = recursively eligible }`, bottoming at a scan-linear leaf (the fact). For
auto-selected links, each build already passed 129/130/132 eligibility (tiny
single-base + hard bytes) and 132 dominance applies per link. For pinned links,
the predicate accepts the already-authored broadcast shape and relies on existing
validation, caps, and fragmenter errors to stay fail-loud rather than re-proving
table facts. Every other nested-join probe (hash/non-INNER/aggregate/sort/take/
limited-scan/exchange/system) stays `ProbeNotScanLinear` → hash. Single seam
preserved (`choose_join_strategy`).

**D2 — No new IR node.** The eligible star is a left-deep chain of
`ExecutionPlan::Join{Broadcast}` in the bound plan. No `LogicalPlan`/`ExecutionPlan`/
`StagePlan`/`JoinStrategy` variant; 123–134 IR unchanged.

**D3 — Fragmenter flattening (the substrate cut).** Add a shape-gated recursive
collector in `distributed/fragment.rs`: recognize the left-deep all-broadcast-build
chain bottoming at one shared fact scan; allocate one build-publish stage per dim
(each a one-partition Exchange), rewrite each build exchange to `shuffle_input_scan(…)`,
and emit **one scan-share probe stage** (the fact, scanned once) whose
`TaskSpec.inputs` = one `Scan { share: Some(ScanShare) }` + N `ShuffleConsumer`s
(reusing 123 fan-in). Existing two-way broadcast behavior is preserved; the nested
flatten is shape-gated and must not change the public plan/stage model. No
intermediate joined result is published.

**D4 — Composed declared-output-order (correctness spine).** Each link keeps 130's
restoring `Project`; the flatten **composes** them so the flattened output is
column-and-digest-identical to the unflattened plan and to hash. The flatten is
output-equivalent regrouping, not re-derivation. This is the load-bearing invariant
— the TCP digest gate pins it.

**D5 — Executor build/probe order + fail-loud pinned.** The shared fact scan probes
each broadcast build in declared chain order; `execute_hash_join` still builds from
the broadcast/left side per link; each build stays one-partition. A **pinned**
broadcast over an *unsupported* nested probe (not the eligible star shape)
**rejects fail-loud** — never silently embeds an unfragmented nested exchange inside
a consumer task (Codex).

**D6 — EXPLAIN: N receipts.** Each star link emits its own `broadcast_inferred`
receipt with a deterministic `join_index` (133/134 unchanged); EXPLAIN of a 3-way
star shows N rows, one per logical join.

**D7 — Scope fence + honest substrate caveat.** Left-deep INNER, already-broadcast
build links, shared scan-linear fact probe only. Auto builds are tiny single-base;
pinned builds are authored broadcast builds and remain capped/fail-loud.
**Correctness baseline uses sealed multi-upstream**: ADR 123's same-worker *live*
fast path applies only when build producers co-locate; cross-worker builds fall
back to sealed manifests — acceptable for 135, remote-live fan-in deferred (Codex).
Deferred: general recursive fragmentation; right-deep/bushy trees; mixed
broadcast/hash chains (non-eligible link → that boundary hashes); output-side
join-as-streamable-producer; Wendelon facts auto-trigger (the cross-ring
facts-flip). No Wendelon / wire / scheduler-shape change.

## Behavior gates
- `three_way_broadcast_star_flattens_to_single_probe_stage` — `fact ⋈ dim1 ⋈ dim2`
  (tiny single-base dims) → one scan-share probe stage + 2 one-partition build
  upstreams; **fails on `main`** (131 fences it to hash). Headline.
- `three_way_broadcast_star_digest_matches_hash_over_tcp` — flattened star output
  byte+column-identical to the hash plan (D4 — the load-bearing gate).
- `rune_nested_broadcast_join_fragments_to_multi_build_probe_stage` — a Rune-authored
  `dim2.broadcast_join(dim1.broadcast_join(fact,on1),on2)` produces 2 build stages +
  one scan-share consumer with 2 shuffle inputs (the Rune-prescriptive win).
- `nested_hash_probe_still_stays_hash` — probe containing a HashPartitioned join →
  `ProbeNotScanLinear`/hash (131 fence preserved).
- `nested_aggregate_sort_take_or_limited_probe_still_stays_hash` — 131 global/limited
  fences intact.
- `pinned_broadcast_unsupported_nested_probe_rejects` — fail-loud, no silent embed (D5).
- `star_explain_emits_one_receipt_per_link` — EXPLAIN of the 3-way star → N
  `broadcast_inferred` rows by `join_index` (134).
- Regressions: 123 multi-upstream, 129 left-build, 130 right-build+order, 131
  scan-linear two-way, 132 dominance, 134 EXPLAIN, 125/128 — all green.

## Architecture guards
- `adr135_star_flatten_lives_in_fragmenter_not_new_stage_topology` — flatten in
  `fragment.rs`; no new `StagePlan`/`TaskInputBinding`/wire shape (reuses 123 fan-in).
- `adr135_no_new_logical_or_execution_plan_variant` (D2).
- `adr135_star_eligibility_is_narrow_left_deep_broadcast_only` — binder predicate
  admits only the star shape; every other nested-join probe still hashes.
- `adr135_final_probe_stage_has_one_scan_share` — exactly one scan-share input for
  the fact spine + N broadcast-build `ShuffleConsumer`s.
- `adr135_star_preserves_per_link_restoring_projection` (D4 order spine).
- `adr135_no_wendelon_or_wire_change`.
- **Retarget** (not delete): 131's nested-probe hash gates → "probe containing
  non-broadcast/global/limited join stays hash"; the fragmenter input-shape guard
  `broadcast_join_fragmenter_input_shape_is_architecture_guarded` → "one scan-share
  plus one or more broadcast-build `ShuffleConsumer`s." 123/129–134 guards green.

## Non-goals
General recursive fragmentation; right-deep/bushy broadcast trees; mixed
broadcast/hash chains; remote-live multi-producer fan-in; output-side
join-as-producer; Wendelon-backed auto-trigger (own cross-ring pair); selectivity.
No new IR/stage/wire type.

## Council history
iter-0: Gemini led general multi-way (new stage topology); Claude led facts-flip
then moved to multi-way on grounding (Toron-local + Rune-prescriptive-aligned);
Codex scoped it to **Broadcast-Star** with the key correction that multi-upstream
topology already exists (123 — `TaskSpec.inputs: Vec`). iter-1: Claude converged on
Codex's scope, verified the 123/seam grounding, added the composed declared-output-
order spine (D4); Codex aligned + added fail-loud-pinned (D5) and the
same-worker-live→sealed caveat (D7); Gemini conceded the general-fragmenter scope
and adopted both. Genuine 3-of-3 on the narrow star cut; Gemini's general fragmenter
is the deferred follow-on ([[feedback_grep_existing_pass_before_placement_fork]] —
the topology pass already existed, shrinking the cut).

## Follow-ons
General recursive fragmentation (arbitrary nested joins); right-deep/bushy broadcast
trees; remote-live multi-producer fan-in; the cross-ring facts-flip (Wendelon
producer → Toron consume+flip) to auto-trigger star broadcast in production (EXPLAIN
validates it); selectivity.

## Promotion criteria
A 3-way left-deep broadcast star flattens to one scan-share probe stage + N
one-partition builds (fails on `main`); flattened output digest+column-identical to
hash over TCP; non-star/hash/global/limited/mixed probes stay hash (131 intact);
pinned-unsupported nested probe rejects fail-loud; EXPLAIN emits N per-link receipts;
no new IR/stage/wire type; reuses 123 fan-in; the six `adr135_*` guards + the
retargeted 131/fragmenter-shape guards + 123/129–134 guards green; `cargo test -p
toron`, `--test architecture_test -- adr135`, `cargo check -p toron --all-targets`,
`git diff --check` all green.
