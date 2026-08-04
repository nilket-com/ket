# ADR 522 - Reachable-key reduction for amplified join branches

q05's grounding at `75ecd13a` found two owners, both volume: `runtime_prep` spends 2.86 s
building a suffix hash table over all 47,989,007 lineitem rows to serve 1,820,547 date-filtered
orders probes, and the chain then materializes 7,283,512 full-width rows that the supplier join
cuts to 292,167 (4%) and the region filter — reaching the chain only at its last stage — cuts
to 58,990. The selective subtree (supplier ⋈ nation ⋈ region('ASIA'), 16,042 of 80,000
suppliers) is data-counted to bound lineitem at 9,623,900 of 47,989,007 rows (~1/5). The fold
is immaterial; q05 is not a grouped-fold cell.

The remedy fork was arbitrated: reachable-key reduction first, as its own ADR, because it
attacks **both** owners (the 48M-row build and the 7.28M-row exchanged intermediate), while
reorientation alone addresses mainly the below-floor 2.86 s build and could trade cheap 1.82M
probing for 48M hash probes. Build orientation is re-grounded **after** this cut on the reduced
shape, where the placement question may disappear or change materially. Combining them would
mix two safety arguments and destroy causal attribution.

The mechanism family is ADR 512/518's superset theorem, at the join-graph level rather than a
correlated scalar: a branch B inner-joined on key k into a subtree S that carries deterministic
filters may be reduced to B ⋉ (k-keys of filtered S), because every B row the downstream inner
joins retain has its key in that set — the source is a superset of the surviving keys by
construction. For q05: `lineitem ⋉ (s_suppkey of supplier ⋈ nation ⋈ region('ASIA'))` before
the suffix build. Expected: build 47,989,007 → ~9,623,900 rows; suffix-1 emissions 7,283,512 →
~1.46M; exchange volume with it. The `c_nationkey = s_nationkey` cycle edge is untouched — the
reduction is a pure row filter on the branch.

## The economics and the honest ledger, before measurement

The removable components priced from the grounding are the build prep (2.86 s scaling with
build rows), the suffix-1 emission/materialization share, and the exchange share — plausibly
clearing the 3,554 ms floor together but not proven. Against them, ADR 513's ledger discipline
prices what the reduction **adds**:

1. **Source acquisition**: the reduction clones supplier ⋈ nation ⋈ region work the query also
   performs for its own output (n_name) — a genuine duplicated acquisition, small (80,000 + 25
   + 1 input rows → 16,042 keys) but priced, not assumed free, and recorded as physically
   shared or executed twice.
2. **The reduction's own probes**: the semi must test all 47,989,007 lineitem rows against the
   16,042-key build — 48M added hash probes, priced.
3. Any residual cloned work the bound shape reveals.

Phase 0 carries red-close authority on the net of these: a SQL twin (derived-table join against
`SELECT DISTINCT s_suppkey FROM supplier, nation, region WHERE ... r_name='ASIA'`) pays all
three costs inherently — it executes the clone, the 48M key matches, and the original joins —
so its measured delta is the honest attainable net, not a generous ceiling. Five interleaved
pairs at the exact coordinate; if the median twin delta does not clear 3,554 ms, ADR 522 closes
red with the per-component ledger. A dataset twin (pre-filtered lineitem) is the fallback only
if the twin shape is grammar-unreachable, and would be labeled the generous ceiling it is.

## Decision

1. A binder rule behind a declining gate: for an inner-join chain branch B joined on a
   plain-equality key into a filtered subtree S — deterministic filters only, the same
   filtered-chain evidence class as ADR 512 — insert `B ⋉ distinct-keys(filtered S)` on the
   branch before runtime build placement. The predicate is a pure row filter: no multiplicity
   change on retained rows, no key/intent/exchange/schema changes elsewhere, moved-not-cloned
   semantics for nothing (the source subtree is new work, priced above).
2. Declines fail closed: non-deterministic source filters, non-equality edges, outer/anti
   contexts, and unfiltered subtrees leave the plan byte-identical.
3. Typed, latched control: `PlannerConfig::join_branch_key_reduction`, compiled default off at
   the seal; gauge env `TORON_TPCH_JOIN_BRANCH_KEY_REDUCTION`, loud "0"/"1" contract, row
   fields with provenance; plan-distinct cohorts; activation per ADR 472's rule.

## Acceptance

1. **Phase 0** as specified above, with authority to close red.
2. **Structural battery**: the q05 shape engages (semi below the branch, source = the filtered
   dimension key subtree); single insertion, no residual copy; spine keys, intents, exchanges,
   and schemas unchanged except the branch semi; declines pinned (unfiltered subtree,
   non-deterministic filter, non-equality edge, outer context); knob-off byte-identity.
3. **Semantic battery**: NULL branch keys, duplicate branch rows and duplicate key matches,
   empty and all-filtered sources, and the cycle-edge case (a second equality constraining the
   same branch) — both arms identical results.
4. **Census**: q01-q22 + m00-m10 both arms, digests identical; engagement evidence for q05 —
   suffix build 47,989,007 → ~9.6M rows, suffix-1 emissions 7,283,512 → ~1.46M (ADR 516
   instrument vocabulary); fingerprint-changed cells run strict correctness both arms; q19
   sentinel. Other filtered-dimension cells (q07/q08/q09 class) may engage; each gets at least
   a regression-check cohort.
5. **Paired authority**: q05 cohort, 20 runs / 10 adjacent pairs, plan-distinct, floor
   3,554 ms, Hodges-Lehmann + 95% Wilcoxon from the persisted journal; both-way verdicts.

## Guardrails

- The gate fails closed everywhere; the knob-off arm is byte-identical.
- No executor changes; the inserted semi is ordinary machinery for placement/admission.
- ADR 511-521 surfaces untouched; interaction is census-adjudicated.

## Risks

- The net may close red at Phase 0 — the honest ledger outcome the plan prices in advance.
- Broad engagement across filtered-dimension cells could surface a regression; the census and
  per-cell regression cohorts arbitrate before any flip.
- The reduction changes the shape build orientation would be grounded on; the follow-on
  re-grounding is sequenced after this ADR by design.

## Forward

- Re-ground q05 build orientation on the reduced shape (the deferred second mechanism).
- If a future cell needs a cost-arbitrated (sometimes-worse) variant of this rule, that is a
  conditional-rewrite ledger entry for the cascades tripwires, not a gate to weaken here.
