# ADR 202: N-Fact Galaxy Actuation

**Status:** Promoted. **Migration:** `plans/impl/202_n_fact_galaxy_actuation.md`.

The graph arc actuated the *bounded* cases (2-fact galaxy / 3-relation bridge) and 200/201 instrumented
them. ADR 202 takes the first **breadth** step: actuate the **n-fact galaxy** (a conformed dimension
shared by 3+ facts) — re-root to the cost-optimal fact the model already names, reusing the snowflake
staging the bounded cut already proved. Scoped to the galaxy (multi-hop bridge chains deferred). Gated,
as always, by a distributed fragmentation proof.

## Grounding (verified against the code)

- **The observe layer is already n-fact-general — only actuation is 2-fact-bounded.** This is the
  load-bearing reframe (3-of-3) that makes the cut an *actuation* cut, not an evidence cut:
  - `analyze_galaxy` claims a conformed dimension shared by `incident.len() >= 2` distinct facts (not
    `== 2`) and captures **all** of them in `incident_relations`. A 3-fact galaxy is recognized today.
  - `galaxy_order_recommendation` loops every fact in `incident_relations`, costs each fact-to-dimension
    edge, and `argmin`s — the cheapest of *n* roots. The cockpit already reports `action=would_reorder`
    to the cost-optimal root for an n-fact galaxy, and the 201 verdict log captures the `MissedWin`.
  - **Only `reroot_galaxy_join_tree` is 2-fact-bounded:** it pattern-matches the exact shape
    `Join(Join(F1, D), F2)` with all-leaf children; a 3-fact galaxy `Join(Join(Join(F1,D),F2),F3)` fails
    the inner `logical_plan_contains_join` leaf check → returns `None` → `try_bind_graph_actuation`
    **declines** → observe-only.
- **So the new capability is exactly one thing: the n-fact re-root.** Everything else is reused —
  recognition, the n-fact recommendation, the soft-margin gate, the no-op-recorder scratch recognition,
  the selected-preflight decline, the 178/186 restoration, recursion termination, and the 200/201
  `GalaxyRoot` decision+verdict accrual (an n-fact reorder records there with no new decision/verdict
  kind).
- **A re-rooted n-fact galaxy is a (deeper) snowflake — but fragmentation must be PROVEN, not assumed
  (the 193/198/199 discipline).** The 2-fact re-root produced a snowflake the fragmenter stages; the
  n-fact re-root produces a deeper left-deep snowflake. Whether it **fragments + digest-matches** is the
  load-bearing Phase-0 gate. If it hits a fragmenter cliff, the cut **re-scopes to observe-only** —
  exactly today's behavior — plus a guard pinning the decline. This is how the cut honors the council's
  observe-first instinct: actuation ships only if the distributed proof clears.

## Decision

**D1 — Generalize the re-root to n facts.** Re-signature `reroot_galaxy_join_tree(plan, pivot,
preferred_root)`: walk the left-deep inner-join galaxy chain `Join(Join(…Join(F1,D),F2)…,Fn)`, confirm
the conformed dimension `D == pivot`, and collect every fact leaf with its **canonical** fact↔D join
keys — the innermost `Join(F1, D)` gives `(on.left, on.right)`; each outer `Join(composite, Fi)` gives
`(on.right, on.left)` (Fi is the right, D sits in the left composite). Rebuild the chain left-deep with
`preferred_root = Fk` as the base driver: `Join(Join(…Join(Join(Fk,D),Fj1)…,Fjm)`, the remaining facts in
their original order, each `on` re-oriented by position (the same `(l,r)→(r,l)` flip the 2-fact path
uses; conditions are logical and re-resolved at bind). Return `None` for any non-galaxy shape (a fact
whose join does not reference `pivot`, a non-inner/non-leaf node) → decline. The 2-fact case is the
`n=2` instance; the existing 2-fact galaxy suite must stay **byte-identical**.

**D2 — Phase 0 is the life-or-death falsification gate.** Before any actuation code: a distributed
3-fact galaxy fixture proves the hand-written re-rooted query (cost-optimal fact driving) **executes
distributed and digest-matches** the AST-order baseline. PASS gates the cut; a fragmenter cliff re-scopes
to observe-only-confirm + a decline guard (today's behavior, now pinned). The 193/198/199 discipline.

**D3 — Actuate through the existing gated seam.** `try_bind_graph_actuation` passes the recommendation's
`pivot` + `preferred_root` to the generalized re-root and binds the re-rooted snowflake through the
existing staging path; the soft-margin gate (190/191), selected-preflight decline (193/198), 178/186
restoration, recursion termination (the re-rooted galaxy's own recommendation is `KeptAlreadyOptimal`),
and 200/201 accrual all carry over unchanged. No new actuator kind, decision taxonomy, verdict machinery,
fragmenter, executor, or coordinator surface — expect zero new lines in the accrual/gate paths.

**D4 — Scope to the galaxy.** Multi-hop *bridge* chains (`F ⋈ B1 ⋈ B2 ⋈ D`, length > 1) are a distinct
recognizer + reroot — deferred. n-fact galaxy is the clean first breadth step because its observe layer
is already complete.

**D5 — Boundaries / purity.** Toron binder + the existing snowflake staging. Side-effect-equivalent to
the 2-fact path generalized. No new fragmenter capability, executor kernel, scheduler, storage, Wendelon,
PBC, wire, or grammar change.

## Guards reconciled (discipline #4)

Retarget `adr198_galaxy_reroot_is_bounded_two_fact` → `adr202_galaxy_reroot_is_bounded_n_fact`: the
re-root handles a left-deep conformed-galaxy chain of any n (still no general permutation / `Cascades` /
search; rejects non-conformed / bushy / non-inner shapes → `None`). Add
`adr202_n_fact_reroot_reuses_gated_seam` (driven by `try_bind_graph_actuation` with the recommendation's
pivot/preferred_root; no new fragmenter/executor symbols) and `adr202_galaxy_actuation_is_binder_only`.
The 200/201 `GalaxyRoot` decision/verdict guards stay byte-identical (no new telemetry surface).

## Honesty gate

`three_fact_galaxy_reroots_to_cheapest_root_and_executes_distributed`: a reachable 3-fact galaxy whose
FROM root is not the cost-optimal fact **executes distributed** with the cheapest fact driving,
**digest-matches** the AST-order baseline (the 198 proof shape), and emits `action=reordered;
applied_root={Fk}` + a `galaxy_root` reordered decision (and, under sampled execution, a `galaxy_root`
verdict). Must **fail on `main`** (the re-root declines today → observe-only).

**Discriminators:** `three_fact_galaxy_already_optimal_kept` (FROM root already cheapest → kept, no
spurious re-root — proves the cost decides, not a constant); `n_fact_galaxy_reroot_preserves_output_schema`
(178/186 restoration on the deeper tree); `n_fact_galaxy_declines_on_fragmenter_cliff` (the Phase-0
fallback, if reachable). **Paired regression — the 2-fact galaxy suite + `galaxy_reroot_query_digest…`
byte-identical** (the generalized re-root must not change the n=2 path), and the 194–201 suites
byte-identical where no actuation fires.

## Consensus (transparent)

**3-of-3 (Claude, Codex, Gemini).** The topic landed as n-fact / multi-hop breadth (Codex's long-standing
push, deferrable until 201 closed the telemetry prerequisite); the operator scoped it to **galaxy
actuation**. The iter-0 round split on actuate-vs-observe-only (Gemini/Codex framed an observe-only
evidence cut); my grounding — that the observe layer (recognition + the n-fact recommendation) is
**already shipped**, so a pure observe cut is near-no-op and the honest next rung is the re-root — was
adopted 3-of-3 by iter-1 (both peers credit the reframe). The actuate-vs-observe tension is resolved not
by overriding the observe-first discipline but by **routing it through Phase 0**: actuation ships only if
the distributed fragmentation proof clears; if it falsifies, the cut *is* the observe-only-confirm +
decline guard. All three converged on: D1 generalize the reroot, D2 Phase-0 life-or-death gate, D3 reuse
the gated seam + 200/201 telemetry unchanged, D4 galaxy-only.

## Forward arc

`201 graph verdict accrual → 202 n-fact galaxy actuation (this) → multi-hop bridge chains → cross-shape
joint actuation → memo only when receipts show local decisions fighting`.

## Deferred (named)

Multi-hop bridge chains (length > 1) ordering + actuation; cross-shape joint actuation; general
join-order search / memo / Cascades; catalog PK/FK structural classification; outer joins; durable
optimizer evidence.
