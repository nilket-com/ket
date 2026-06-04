# Brainstorm 010: Two UX Layers Over One Fact Spine — SQL auto-safe, Rune observable-steerable

**Status:** Exploratory / Future Side Quest

Seeded by an operator musing (pacing back and forth) on balancing "automagical idiot-proofing
SQL" against "giving power users the steering wheel with all the gauges visible in Rune" — two
very different UXs within one system. The frame: **one optimizer truth model (the fact spine),
two UX layers over it.** SQL stays declarative-and-automatic; Rune is the cockpit. Not two
optimizers — one optimizer, two interfaces to the same receipts, two default risk postures.

## The key realization: this is already the system's grain

This is not a new principle to adopt — it is the **retroactive name for a through-line the
council has been building since ADR 139.** Naming it just makes the latent doctrine explicit so
future forks resolve consistently. Grounded:

- **ADR 139/140 source-aware replay *is* the two-UX doctrine, shipped.** An Auto or SQL
  `/*+ BROADCAST */` over-cap broadcast **replays → hash** (protect the naive user); a **Rune**
  `broadcast_join` or direct pin stays **terminal / fail-loud**. Implemented as
  `JoinStrategyIntent::Pinned { source: JoinStrategyPinSource }` (`ir.rs:880`):
  `Pinned { .. } => BroadcastReplayEligibility::Terminal` with `SqlHint` special-cased to remain
  replayable (`binder.rs:5104–5108`). The *pin changes strategy; the cap still fires; replay-
  eligibility is keyed by source.*
- **ADR 138's receipts are already gauges.** `JoinStrategyDecisionReason::BroadcastInferred`
  (hard) vs `BroadcastEstimated` (estimate) (`binder.rs:460/466`) — the estimate-vs-hard
  confidence label.
- **The Rune-observability surface has been deliberately *reserved*.** `optimizer_explain_schema`
  (`binder.rs:5680`) is kept join-decision-only, guarded by **both** `adr133_no_public_explain_surface`
  *and* `adr154_no_public_filter_explain_surface` (`optimizer.rs:343, 2580`); 154/155/156/157 each
  carry a `no_public_filter_explain_surface` fence. Those guards aren't caution for its own sake —
  they have been **implicitly holding the richer filter-level observability back from the SQL
  surface**, exactly the cockpit this doc describes. The measurement *exists* (157 produces "uniform
  said 100, observed 5000, crossed the boundary"); it has just never been exposed to SQL.
- **[[rune_is_primary_interface]]:** Rune prescriptive / power-user-first; SQL secondary
  compatibility. This doc is the UX-policy expression of that.

## The doctrine: override strategy, not truth labeling

Manual control (Rune/SQL hints) can override **strategy** — pin hash, force broadcast, tighten a
budget. It can never override **truth labeling**. If a fact is approximate, partial, stale,
coverage-gated, or estimate-channel-only, the interface shows that plainly; if a forced broadcast
blows the cap, it fails or replays per the established rules — never silently pretends the estimate
was hard. This is already the `Pinned{source}` mechanism: the pin sets the strategy, the cap and
replay-eligibility are unchanged. It is the join-chooser instance of [[logical_identity_receipt_doctrine]]
("magic may enforce only when it has a receipt") and the channel-honesty discipline (approximate ≠
hard).

## The sharp tension: SQL is correctness-safe but performance-cliff-opaque

The automatic SQL layer is honest about *correctness* (replay protects rows) but **opaque about
performance cliffs.** A SQL user is one missing stat away — no top-k, stale coverage, an unmeasured
range-skew (157) — from a silent ~90× over-broadcast → replay tax. Rows stay correct; the tax is
invisible. That trade is acceptable **only if the escape hatch is real:** **Rune is the diagnostic
surface for SQL's opaque cases too.** "This query is slow → drop to Rune → read the gauges → it
broadcasted on a uniform estimate that missed by 90×." The doctrine states it plainly: *SQL trades
observability for simplicity; Rune is where you go to find out why.*

## One truth, two defaults (not two optimizers)

The two layers read the **same receipts** but may legitimately differ on **default risk posture**:
SQL defaults to fail-open / degrade-gracefully; Rune defaults to fail-loud on explicit pins. Same
truth model, per-source policy — which the system already has. The cockpit gauges Rune surfaces:
the chosen strategy + its reason receipt (Estimated/Inferred), the fact provenance + confidence/
coverage/freshness label, replay risk, the alternate strategy's cost, and explicit steering ("show
me why this broadcasted," "pin hash here," "try this with a tighter budget," "inspect the
range-distribution fact for `amount`").

## The discipline that keeps it from becoming a monolith

The trap is building "the optimizer cockpit" as one big observability subsystem ADR. **Don't** —
the operator's "I don't have clear ideas yet" instinct is correct. Make it a **per-fact placement
rule, accreted one fact at a time.** Every new optimizer fact ships:
1. its **spend in the estimate channel** (SQL benefits automatically, no user knowledge required);
2. a **Rune-visible receipt** with an explicit confidence / coverage / freshness / error-bound label;
3. **no new public SQL `EXPLAIN` row** (the `no_public_filter_explain_surface` fence holds).
The cockpit is the **sum of these receipts**, never a separate subsystem. 154→157 already follow leg
(1) and leg (3); leg (2) — the deliberate Rune receipt surface — is the only net-new mechanism, and
it can be introduced once (a thin Rune-only `optimizer_trace`/inspection API) and then *fed* by each
subsequent fact, not rebuilt.

## How it lands on the live arc

- **157 (range-skew evidence)** is the next instance: it *produces a gauge reading*. Per this
  principle, that reading belongs in the Rune trail, **not** SQL `EXPLAIN` — which is precisely why
  157 keeps it out of public EXPLAIN. The principle *explains* the guard rather than fighting it.
- **155/156 (heavy-hitter)** already spend exact frequencies in the estimate channel (SQL auto) with
  coverage/exactness labels on the fact (the Rune-visible truth) — leg (1)+(2) latent in the fact
  shape, leg (3) held.
- The deferred **`SET TIME ZONE` / REPL client** and any future EXPLAIN-expansion forks resolve under
  this rule: persistent session + cockpit inspection are **Rune-surface**, not SQL-grammar, work.

## Open questions for the council

1. **The one net-new mechanism:** a thin Rune-only optimizer-observability API (read the per-plan
   reason receipts + fact provenance/confidence) — is that a single small ADR that subsequent facts
   feed, or does each fact grow its own surface? (Lean: introduce once, feed thereafter.)
2. **Confidence vocabulary:** what's the minimal label set a receipt carries — `{exact | approx(err)
   | partial-coverage | stale | estimate-only}` — and is it uniform across NDV / min-max / top-k /
   quantile facts? (Uniform is the whole point of "one truth model.")
3. **The escape-hatch UX:** when a SQL query hits a perf cliff, what's the actual "drop to Rune"
   path — re-author, or a Rune wrapper that inspects a SQL plan's receipts?
4. **Steering scope:** which overrides are exposed (strategy pin, budget, fact opt-out) and which
   stay engine-only — and the fail-loud-vs-degrade default per override.
5. **Does SQL ever get a *reading* gauge** (e.g. a `WARN: broadcast on estimate that may replay`),
   or is observability strictly Rune? (Lean: SQL stays silent-automatic; warnings are a Rune affordance.)

## Why hold it as principle, not feature (now)

This connects ~18 ADRs and is the right *lens* for resolving UX-placement forks, but it is **not a
cut to schedule against the CBO spine yet** — the gauges accrete naturally as the fact arc (157 →
KLL → Theta → reordering) proceeds. Capture it, apply it as a placement rule, and let the cockpit
emerge. The one deliberate ADR it implies (the Rune observability API, OQ1) is a clean future
breadth cut alongside [[rune_is_primary_interface]] and the CONVERT/units arc.
