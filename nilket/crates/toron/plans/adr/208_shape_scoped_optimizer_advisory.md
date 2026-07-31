# ADR 208: Shape-Scoped Optimizer Advisory

**Status:** Promoted. **Migration:** `plans/impl/208_shape_scoped_optimizer_advisory.md`.
**Category:** Toron optimizer policy — a guardrail cut on the 189/191 soft-margin advisory, sequenced
deliberately before the Wendelon rf>1 counter-replication cut amplifies 207's evidence to true cluster
history. No Wendelon, executor, fragmenter, storage, PBC, wire, or grammar change.

## Why this cut exists

The 189 advisory watches the optimizer's measured verdicts and recommends loosening or tightening the
join-order soft margin; 191 lets an operator opt in to applying that recommendation. When the advisory was
built, the verdict log held only tree-shape evidence (star/snowflake). The 195–206 graph arc then began
accruing `GalaxyRoot` / `BridgeRoot` / `BridgeChainRoot` verdicts into the same log — and the advisory kept
summing **everything**: `recommend_soft_margin` (join_order.rs:112-129) matches only on `row.verdict`,
never `row.kind`. Graph and chain receipts steer a recommendation about tree ordering. 207 then made the
evidence cluster-shared and restart-surviving, raising the stakes: under `ApplyAdvisory` on an rf=1
cluster, another coordinator's bridge-chain history can tune this coordinator's tree margin.

The intuition for the fix's shape: one thermostat currently serves two rooms. Filtering the thermostat's
*sensor* to room A (the peers' first draft) doesn't fix the building — room B's vents still take orders
from a thermostat that no longer feels room B. The grounded surprise of this cycle is that the **knob is
shared at the application site too**: the binder stores one advisory-resolved step (binder.rs:394) and the
galaxy/bridge/chain thin-margin gate reads it (join_order.rs:2496, `GraphActuationResult::KeptThinMargin`)
alongside the two tree sites (:3070, :3223). Input-only filtering would not seal the leak; it would
**reverse** it — tree evidence silently steering graph actuation with graph outcomes excluded from their
own feedback loop. So this ADR scopes **both ends**: tree evidence → tree recommendation → tree sites;
graph actuation keeps the configured base margin until a graph-family advisory exists.

**Honesty about urgency (correcting the iter-1 framing):** there is no race against accumulating history.
207's durable matrix is kind-labeled by construction (35 `{kind}/{verdict}` keys), so history accumulates
kind-separated regardless of this bug, and the scoped advisory retroactively re-reads old history cleanly.
The motivations are: (a) the advisory recommendation is simply **wrong today** wherever graph queries run —
and has been, locally, since 200/201; (b) sequencing coherence — cut-8b (rf>1 counter replication) is the
amplifier that turns 207's evidence into real cluster history, and the guardrail should be standing before
the amplifier switches on.

## Grounding (verified against the code)

- `recommend_soft_margin` sums all kinds: the match at join_order.rs:112-129 reads `row.verdict` only.
  Both consumers feed it the **whole** snapshot: `effective_join_order_soft_margin_step` (:186) and
  `margin_advisory_report` (:205).
- The resolved step is stored once at binder construction —
  `join_order_soft_margin_step: effective_join_order_soft_margin_step(config)` (binder.rs:394) — and read
  by three margin gates: the graph thin-margin arm (join_order.rs:2496) and the two tree star-order sites
  (:3070, :3223). Under `ApplyAdvisory` the advisory-resolved step governs **both shape families** today.
- The verdict kinds split cleanly: tree = `StarCandidate`, `FlatStar`, `SnowflakeRoot`, `SnowflakeBranch`;
  graph = `GalaxyRoot`, `BridgeRoot`, `BridgeChainRoot` (decision_log.rs:8-19).
- **rf>1 reality (the operator's table-stakes steer, grounded):** Wendelon rejects `COUNTER_ADD` under
  `replication_factor() > 1` fail-closed (`COUNTER_REPLICATION_UNSUPPORTED`, data_server.rs:7728) — on a
  standard rf=3 cluster, 207's durable lane latches off at first flush and evidence is local-pending only.
  This ADR's "cluster history feeds the advisory" story holds on rf=1 today; the boundary it builds is
  what makes the rf>1 amplifier safe to switch on. See D7.

## Decisions (promotion-locked)

**D1 — Tree advisory evidence scope.** The soft-margin advisory consumes only tree join-order verdict
kinds (`StarCandidate`, `FlatStar`, `SnowflakeRoot`, `SnowflakeBranch`), filtered by an explicit helper
beside `recommend_soft_margin` — the policy lives with the consumer, not in the evidence store.
`recommend_soft_margin` itself stays verdict-class math over a provided slice.

**D2 — Scope input AND application.** Binder construction resolves **two** margin steps:
`tree_join_order_soft_margin_step` (configured base under `Manual`; scoped-advisory step under
`ApplyAdvisory`) and `graph_join_order_soft_margin_step` (**always** the configured base in this ADR).
Tree sites (:3070, :3223) read the tree step; the galaxy/bridge/chain thin-margin gate (:2496) reads the
graph step. Graph actuation behavior is identical under `Manual` and `ApplyAdvisory` — explicit field
names, not a mode check at the read site, so the invariant is visible in review.

**D3 — Runtime evidence stays complete.** The durable/local verdict matrix is untouched — all 35
kind-labeled keys, no migration, reset, compaction, or deletion.
`information_schema.optimizer_runtime_verdicts` continues to report every kind. Scope is a read policy,
never a write policy.

**D4 — The advisory report names its boundary.** `information_schema.optimizer_margin_advisory` appends
two tail columns: `evidence_scope` (`"tree_join_order"`) and `excluded_sample_size` (the excluded
graph/root-path rows that would otherwise enter the sample math — `MissedWin + OverEager + Vindicated`,
mirroring the advisory's own sample definition). Existing columns keep order and semantics; `applied_step`
remains the tree-applied step (the report is about the tree advisory).

**D5 — 191 discipline preserved.** `Manual` is byte-identical for both families. `ApplyAdvisory` resolves
once at bind entry, synchronously, from the scoped snapshot; no storage awaits, no evidence flushes from
the advisory path. The 191 resolve-once guard is retargeted to pin the scoped helper.

**D6 — No graph-family tuning yet.** This ADR creates no graph-specific recommendation and no per-kind
margins. A future graph-family advisory — fed by graph kinds, governing the graph gate — is the named
follow-on, after the graph knobs worth tuning are identified. D2's base-step graph behavior is exactly
the pre-191 Manual semantics: not advisory-tunable, not degraded.

**D7 — Explicit rf>1 honesty.** This is a Toron policy boundary; it does **not** fix 207's
replicated-cluster darkness. Durable cluster-shared evidence currently exists only where Wendelon accepts
counter writes (rf=1). The next cut in sequence is **Wendelon cut-8b** (rf>1 counter replication —
peer-identity witness, ring-driven participant set, reconcile to recorded peers; transport legs already
landed behind `#[allow(dead_code)]`, data_server.rs:3863+), promoted in the Wendelon sequence, with no
Toron optimizer work in between. 208 is the guardrail; cut-8b is the amplifier.

## Honesty gate

`graph_or_chain_verdicts_do_not_drive_soft_margin_advisory`: seed only graph/chain `MissedWin` evidence at
or above `JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE` under `ApplyAdvisory`; the effective tree step must stay
the configured base. **Fails on `main`** (the unscoped sum loosens the margin).

**Load-bearing discriminators:**
- `apply_advisory_does_not_steer_graph_actuation` — seed tree evidence that loosens the tree advisory;
  a graph actuation near the margin boundary must gate on the **base** step. This is the test that fails
  under the rejected input-only design (it locks D2, not just D1 — the iter-1/iter-2 peer-draft
  discriminator).
- `tree_verdicts_still_drive_soft_margin_advisory` — tree evidence still loosens under `ApplyAdvisory`.
- `optimizer_margin_advisory_reports_scope_and_exclusions` — appended tail columns, stable prefix.
- `optimizer_runtime_verdicts_still_reports_all_kinds` — the complete matrix stays visible (D3).
- `manual_mode_remains_byte_identical_for_both_families` — D5's boundary.

## Architecture guards (promotion-locked)

- `adr208_soft_margin_advisory_filters_to_tree_verdict_kinds`
- `adr208_tree_and_graph_margin_steps_are_separate`
- `adr208_graph_margin_gate_reads_base_step_under_apply_advisory`
- `adr208_runtime_verdict_report_remains_complete`
- `adr208_margin_advisory_report_appends_scope_columns`
- `adr208_apply_advisory_remains_resolve_once_no_storage_await`
- `adr208_no_wendelon_counter_replication_claim` (no Wendelon production change; the cut must not claim
  to deliver rf>1 durability)

**Reconciled prior guards (named retargets):** ADR 189's "advisory reuses the 188 verdict log" guard
retargets to pin the **scoped** helper consuming the snapshot (raw whole-snapshot consumption by the
advisory becomes the violation). ADR 191's resolve-once guard additionally pins the two-step binder split
and rejects evidence reads/awaits inside the scoped path.

## Rejected alternatives

**Input filtering only** — leaves graph actuation reading the tree-resolved step; swaps graph→tree
contamination for invisible tree→graph contamination (the iter-1 peer drafts; superseded on grounding).
**Per-kind / graph advisory now** — the graph knobs worth tuning aren't named yet; boundary first.
**Folding Wendelon rf>1 replication into 208** — wrong ring ownership; it gets its own design cycle (and
the 075 impl doc explicitly requests one for the witness rework).
**Outer-join completion now** — independent of the leak; deferred behind cut-8b, anchors pinned.

## Consensus (transparent)

Topic 3-of-3 (Codex proposed; Gemini voted; Claude conceded from outer-join and verified the leak
line-by-line). Design: the **end-to-end scoping (D2) is the grounded 2-of-3** — Claude's iter-1 correction
from binder.rs:394 + join_order.rs:2496, adopted by Codex iter-2 ("Claude's D2′ is load-bearing"); Gemini's
latest draft predates the correction and carried input-only filtering — superseded by grounding both peers
can verify, surfaced here per council discipline rather than papered over. The urgency framing was
corrected in-cycle (kind-labeled matrix ⇒ no history poisoning; Codex's guardrail-before-amplifier is the
honest motivation, adopted). The rf=3 steer reshaped the forward arc: cut-8b next, nothing in between.

## Forward arc

`207 durable evidence telemetry → 208 shape-scoped advisory (this) → Wendelon cut-8b rf>1 counter
replication (next, no Toron optimizer work in between; transport pre-landed, witness index→identity rework
is the design crux) → 209 outer-join completion (anchors pinned in the 207/208 iter-0s) → evidence-ordered
optimizer bets (graph-family advisory; drive-from-middle; peak-vs-sum; memo — when durable receipts show
local decisions fighting)`.

## Deferred (named)

Graph-family advisory (graph kinds → graph knobs); per-kind margin policies; persisted tuned margins
(premature until the scoped advisory proves out); counter reset/decay; Wendelon cut-8b (next cycle,
Wendelon sequence); outer-join completion (209); caller-owned write_id exactly-once flush; multi-region
joint actuation (still unreachable).
