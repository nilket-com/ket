# ADR 514 - Representation-aware sizing for the streamed scan-build partitioning choice

The q21 grounding traced the cell's 9.9 s / 898 MB deferred envelope to a single mispriced
decision. Suffix runtime 1's build is `σ(o_orderstatus='F')(orders)` projected to one Int64 key:
5,845,938 rows that materialize as a compact index — 46,837,968 B of decoded pages, a
134,217,728 B peak index allocation per partition, 23.4 MB pinned. The partitioning choice never
sees that representation: `streamed_inner_build_partition_count` prices the build with the shared
admission model at 112 + 8 = 120 B/row — 701,512,560 B — against a 536,870,912 B threshold
(`spill_budget / 4`; the 16x memory leg is loose), selects `partition_count = 2`, and thereby
selects the partition-at-a-time lifecycle: 30,343,541 probe rows buffered through an
873,395,520 B reservoir, a 4.6 s compact-index rebuild, the whole deferred envelope. The route
condition itself is sound in structure; it is priced in the wrong physical representation.

This is materially different from ADR 513's dead end: the measured footprint fits the unchanged
threshold, the direct variant already exists in the same predeclared stage vocabulary, and no
selectivity machinery, route-condition edit, or new theorem is involved. It is also deliberately
NOT an edit to `HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES`: that constant is the single source of truth
binder admission and executor fallback share (ir.rs:19), and it stays untouched. The cut is a
scoped second opinion at exactly one decision.

## Decision

1. **Eligibility before pricing, from the committed shape.** The representation-aware estimate
   applies only when the build provably takes the compact-index representation the executor would
   commit: a single build key whose `JoinKeyPlan` is compact (`Int32`/`Int64`), plain equality
   (`null_safe_keys = false`). Anything else — generic or composite keys, null-safe shapes, any
   uncertainty in representation selection — retains the legacy 120 B/row estimate unchanged,
   fail-closed.
2. **A checked upper bound over every simultaneously resident owner** at the counterfactual
   `partition_count = 1`: decoded page bytes (the measured batch bytes the estimator already
   sums), plus the index allocation at power-of-two slot capacity times the 32 B slot (the model
   the observed 134,217,728 B peak decodes exactly), plus per-row key/row references, plus write
   scratch. Not the final 46.8 MB index alone and not the 23.4 MB pinned peak — those are
   deferred-route artifacts (incremental per-partition pinning); the direct route holds pages,
   full index, and references at once, and the pricing proof must say so.
3. **A predeclared safety factor of 1.25**, applied to the summed bound, with saturating
   arithmetic; any overflow fails closed to the legacy estimate. The projected q21 numbers are
   stated now, before implementation: bound ≈ 46.8 MB pages + 268.4 MB index (8,388,608 slots x
   32 B) + ~93.6 MB references ≈ 409 MB; x1.25 ≈ 511 MB against the 536.9 MB threshold — a fit
   with roughly 5% margin, not an order-of-magnitude one. If Phase 0's owner-complete
   measurement pushes the conservative bound over the threshold, this ADR closes red; the factor
   is not negotiated down to rescue it.
4. **Typed, latched control at the route entry** (ADR 510's pattern — this is an executor
   decision, not a binder one): `TORON_COMPACT_BUILD_PARTITION_SIZING`, explicit "0"/"1", absence
   selects the compiled default (off at the seal), latched once above the route, never reread
   below it.
5. **A named paired gate for a plan-identical ablation.** Both arms bind the same plan
   fingerprint — the switch changes execution only — so the plan-distinct gate cannot arbitrate.
   Following ADR 510's precedent, the harness gains a `route-distinct` gate requiring identical
   plan fingerprint, digest, schema, and row count, and opposite route facts: partition_count
   2 versus 1, deferred stage vocabulary versus direct, reservoir spill bytes nonzero versus
   zero. Missing, stale, or same-arm evidence fails closed.

## Phase 0 — exact-coordinate evidence before any measurement

From the current tree with the chain-stage instrument (no mechanism code):

- Verify every component of the pricing model from receipts at the exact coordinate: slot bytes
  and power-of-two growth (the 134,217,728 B peak), decoded page bytes, reference bytes, write
  scratch — and compose the owner-complete `partition_count = 1` projection with the safety
  factor. State the fit or miss against 536,870,912 B plainly.
- Pin the direct variant's residency guards on the projected footprint (the existing
  `guard_membership_owner_resident` arithmetic) so the flip cannot admit an over-budget resident
  set: route flip, then guard violation, is a red, not a win.
- Residency, route, correctness, and cleanup evidence gates precede any performance row:
  strict q21 correctness at its supported coordinate in both arms, the route facts per arm, the
  residency peaks per arm, and empty join-spill scratch after each child.

## Acceptance

- Unit coverage of the sizing fork: eligible Int64/Int32 single-key shapes price with the
  owner-complete bound; generic, composite, and null-safe shapes retain the legacy estimate
  byte-for-byte; overflow saturates and falls back closed.
- Threshold-edge tests: bounds straddling 536,870,912 B on both sides of the safety factor,
  pinning `partition_count` 1 versus 2 exactly at the predeclared edge.
- The census diff across q01-q22 and m00-m10: fingerprints must be identical in both arms on
  every cell (this switch changes no plans); every cell whose route facts change runs its strict
  correctness rail in both arms; q19 stays the standing sentinel.
- The `route-distinct` gate passes before the first cohort child; the diagnostic pair reports the
  causal split — sizing decision, partition count, reservoir bytes, deferred/direct stage set,
  residency peaks — per arm.
- Paired verdict on q21 at the fixed 3,554 ms floor from the predeclared estimator; activation
  per ADR 472's rule.

## Guardrails

- `HASH_JOIN_BUILD_ROW_OVERHEAD_BYTES` and every binder-admission consumer of it are untouched;
  the second opinion exists at exactly one call site (the streamed scan-build partitioning
  choice), and an architecture guard pins that scope.
- The route condition, stage vocabularies, and lifecycle variants are unchanged; a flip selects
  the existing direct variant.
- Fail-closed is the default posture: any unproven eligibility or arithmetic uncertainty yields
  the legacy estimate and today's behavior byte-for-byte.

## Risks

- The 5% projected margin is thin. Phase 0's owner-complete measurement is the honest gate; a
  red there costs two doc commits, as ADR 513 did.
- Other cells' route facts may flip where compact builds were mispriced the same way — the
  census enumerates them, and their correctness rails run before measurement.
- The deferred route's estimate pre-scan means today's build input is scanned twice; the direct
  route scans it once inside the build. The diagnostic's causal split must not credit that
  saving to the reservoir removal.

## Forward

- If the margin holds and q21 flips: the remaining q21 envelope becomes runtime_prep (5.7 s
  summaries) and the suffix probe chain — re-rankable against the board afterward.
- If Phase 0 closes red on the owner-complete bound: the deeper question is the spill-writer
  target leg (`spill_budget / 4`) pricing resident builds that never write a spill artifact —
  a route-condition design question for a future ADR, out of this one's scope by declaration.
- The same mispricing may gate other compact-eligible builds across the board; the census is
  the discovery instrument.
