# ADR 203: Multi-Hop Bridge Chain Ordering Evidence

**Status:** Promoted. **Migration:** `plans/impl/0203_bridge_chain_ordering_evidence.md`.

The graph arc mastered single-pivot stars (galaxy, n-fact) and the single-hop bridge. ADR 203 opens the
**path** topology: a multi-hop bridge chain `F ⋈ B1 ⋈ B2 ⋈ D`, where each junction's M:N fan-out
**compounds** into the next. Deliberately **observe-only**: recognize the chain, recommend a path order,
and measure its runtime truth — but never re-root. The chain re-root is the named follow-on, scoped by
these receipts.

## Grounding (verified against the code) — why observe-only, and the exact shape

This cut is the *opposite scope call from 202*, and the grounding is why. In 202 the observe layer was
already n-fact-general, so it was an actuation cut. Here it is not:

- **Recognition substrate is partially there; the path is not assembled.** `analyze_bridge` (called at
  `binder.rs:257` during binding, via `set_observed_bridge`) iterates **every** relation as a potential
  bridge and pushes a `BridgeLink` per qualifying junction — so a chain `F ⋈ B1 ⋈ B2 ⋈ D` can already
  surface **two** links today: `link1 = {fact:F, bridge:B1, dim:B2}` and `link2 = {fact:B1, bridge:B2,
  dim:D}`. But they are two *disconnected* receipts, and the vector is relation-set ordered, not path
  ordered; nothing names the path. The links chain where **`link_{i+1}.fact == link_i.bridge`** — here
  `link2.fact = B1 = link1.bridge`. (Verified by tracing `analyze_bridge`: a link's `fact` is its
  incoming-edge probe, its `bridge` is the relation itself; the successor bridge's fact is the
  predecessor bridge.)
- **The ordering model is single-hop; the path cost is new.** `bridge_order_recommendation` reads
  `links.first()` only (root F vs D for one junction). A chain is a **path-cost** question: the cheaper
  driver-end is the one whose fan-out compounds least along `F→B1→B2→D` vs `D→B2→B1→F`. That compounded
  cost is the genuinely new piece, and it cannot be assumed correct — which is exactly why this is
  observe-only: the **measured verdict** (D3) is the reality check before any re-root is built.
- **Recognition runs during binding, not EXPLAIN.** The single-bridge evidence is set on `render_trace`
  at bind time (`set_observed_bridge`), which is what lets the 188/201 *sampled* path judge it without
  EXPLAIN. The chain recognizer must do the same (set `render_trace.bridge_chain_order` during binding),
  or D3 cannot judge it under normal execution. "Observe-only" means *no plan change*, not *EXPLAIN-only*.

## Decision

**D1 — Recognize a bounded 2-hop chain (dark, at bind time).** `analyze_bridge_chain(edges, decisions)
-> Option<BridgeChainEvidence>` (a separate recognizer, composing — not modifying — `analyze_bridge`):
run `analyze_bridge`, then stitch its relation-set-ordered links into a single ordered path on
`link_{i+1}.fact == link_i.bridge`. Accept **exactly one 2-hop chain** (two links stitching end-to-end,
all distinct relations); return `None` for a single junction, branching (a relation appearing as
`bridge` in two links), a cycle, or > 2 hops. Set on `render_trace.bridge_chain_order` during binding
(beside the single-bridge `set_observed_bridge` at `binder.rs:257`); the single-bridge `bridge_order`
still sets too (additive — a chain query also carries its per-junction bridge rows). Normal binding
byte-identical.

**D2 — Path ordering recommendation (observe-only).** A compounded path cost over the per-hop edge
estimates that **discriminates direction**: cost the chain driven from each end (`F`-first vs `D`-first)
by the running fan-out, and recommend the cheaper driver-end. The per-edge estimate is a pair-join, so
the running compound is approximated — **a defensible heuristic, explicitly not claimed optimal,
deliberately tested by D3**. `declared_end` = the AST FROM end; `preferred_end` = the cheaper; `action =
Reordered` if they differ; conservative `None` on any missing edge estimate (mirroring
`bridge_order_recommendation`). Row-only (197/181). Emit a `join_order_bridge_chain` cockpit row (ordered
path, ends, per-hop costs, action). **No actuation — plan + results byte-identical.**

**D3 — Measured chain verdict (the proof).** Under EXPLAIN ANALYZE and the 188 sampled path, substitute
the 195/196 measured per-edge fan-out into the path scorer and emit a graph verdict
(`Vindicated`/`MissedWin`/`Indeterminate`) for the chain order — reusing `measured_graph_order_verdict`
and adding a chain arm to `measured_verdicts_from_summaries` (the real 201 sampled-verdict seam; *not* a
`compute_join_order_verdicts` helper, which does not exist). **The verdict cost is compounded, not an
endcap.** D2's recommendation is a cheap *entry-hop* heuristic that is deliberately blind to the middle
hop `B1⋈B2` (a common term that doesn't discriminate direction); the verdict is the *falsifier* of that
heuristic, so it folds the middle in — the declared end's compounded cost is the PEAK over its *measured*
entry + *measured* middle, against the un-run end's *estimated* peak (the 196 measured-ran-vs-estimated-
counterfactual shape). A middle that explodes past estimate flips `Vindicated → MissedWin` — exactly the
blind spot the entry-only heuristic would miss. The 201 `record_verdict` loop and the 189/191 advisory
then pick the chain verdict up for free.

**D4 — Telemetry union.** Extend `star_order_runtime_replay_ids` to union the chain's per-hop edge
targets (the 195/196 pattern), so the sampled path collects the chain's observations.

**D5 — Observational invariant / boundaries.** No re-root, no actuator, no new decision kind. The chain
is judged through the existing sampled-verdict path; no actuation commit is introduced. Execution plans +
EXPLAIN tree receipts byte-identical to `main`. Binder + explain + the existing verdict log. No
fragmenter, executor, storage, Wendelon, PBC, wire, or grammar change.

## Honesty gate

`two_hop_bridge_chain_recommends_cheaper_driver_end`: a reachable `F ⋈ B1 ⋈ B2 ⋈ D` whose AST driver-end
is not the cheaper one emits a `join_order_bridge_chain` row naming **both hops** and the preferred end,
with plan + results byte-identical. Must **fail on `main`** (single-link recommendation only — no chain
row).

**Load-bearing discriminator — `bridge_chain_recognizer_distinguishes_chain_from_single_junction`:** a
plain single bridge `F ⋈ B ⋈ D` emits the 199 `join_order_bridge` row but **no** chain row — proving the
path-stitch decides, not merely "two bridge links present." Plus `balanced_chain_kept` (symmetric fan-out
→ no spurious reorder) and the measured partner `chain_verdict_vindicates_or_faults_recommended_order`
under sampled execution. Paired regression: the 194–202 EXPLAIN + execution suites byte-identical.

## Consensus (transparent)

**3-of-3 (Claude, Codex, Gemini)** on **observe-only chain evidence, not actuation**, on the same
grounding: the recognition substrate is partially present (`analyze_bridge` already returns a `Vec` of
links) but the **path-ordering cost model is new**, so the falsification-first discipline genuinely
applies here (unlike 202, where the n-fact star cost was already general). Codex frames the decline-only
/ boundary-result fallback if the chain isn't reachable; Gemini specifies the path-cost + measured-verdict
shape. Two corrections grounded into this promotion: the chain-stitch predicate is `link_{i+1}.fact ==
link_i.bridge` (verified against `analyze_bridge`'s output; a peer draft's `link_i.dim == link_{i+1}.fact`
does not hold), and the sampled-verdict seam is `measured_verdicts_from_summaries` (a peer draft's
`compute_join_order_verdicts` does not exist). Bind-time recognition is 3-of-3. The chain *re-root*
(actuation) is the unanimous named follow-on.

## Forward arc

`202 n-fact galaxy actuation → 203 multi-hop bridge chain ordering evidence (this) → bridge chain
actuation (the path re-root, gated by a Phase-0 fragmentation proof) → cross-shape joint actuation →
memo only when receipts show local decisions fighting`.

## Deferred (named)

Bridge chain actuation (the path re-root); chains length > 2 / branching / bushy bridges; cross-shape
joint actuation (the 201 `graph_actuated` XOR-gate boundary); general join-order search / memo / Cascades;
catalog PK/FK structural classification; outer joins; durable optimizer evidence.
