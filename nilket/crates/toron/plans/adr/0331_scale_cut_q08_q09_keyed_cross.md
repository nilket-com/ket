# ADR 331 - scale-cut #1: q08/q09/q02 scale-dependent keyless broadcast-Cross

## Status
Proposed.

## Context

The scale arc's first cut, from ADR 330's atlas. q08/q09 **OOM at sf=0.1** and **bind-fail
(`unsupported_cross_join`) at sf=1**, yet are green at sf=0.01. The first two framings of this ADR were **both
overturned by grounding** - which is the measure-don't-predict rule earning its keep:
1. *Rejected:* a harness FROM-reorder (canonical-dodge - rewrites the gauge).
2. *Overturned:* "static keyless comma-join the binder builds left-deep" - **false.** The grounding probe shows
   q08 binds **all-`Inner`, keyed, with no raw Cross at sf=0.01** (all join summaries `Inner left_keys=1
   right_keys=1`). There is no static cartesian to re-associate, which is exactly why the `reassociate_cross_child`
   experiment could not land - it was chasing a Cross that does not exist at the probe scale.

## Phase 1 - grounding (done; the probe corrected the diagnosis)
- **sf=0.01, default broadcast cap:** the cost optimizer keys *every* join (all-`Inner`). No Cross. (probe:
  `test_print_original_q8_plan`.)
- **sf=1, default cap:** bind-fail `unsupported_cross_join` = "CROSS JOIN requires a broadcast-eligible build side
  under the broadcast cap" (join.rs:769).
- **sf=1, *relaxed* cap (`u64::MAX/4`):** the bind *succeeds* but produces a **keyless broadcast-Cross** (this is
  what Codex-lite's relaxed-cap probe saw and tried to reassociate).
- **Therefore:** the Cross is **scale-dependent and optimizer-produced.** At sf=1 the cost-based join-order /
  broadcast optimizer (join_order.rs, gated by `effective_join_order_policy` / soft-margins / seeded stats) chooses
  to **broadcast a relation over a non-key edge** - a keyless broadcast-Cross - instead of the keyed all-`Inner`
  plan that is valid at sf=0.01. The default cap then rejects its oversized build. The bug is the **decision**, not
  a static join shape; `reassociate_cross_child` treats the symptom.

## Phase 1b - the remaining grounding (next step, before Phase 2)
Pinpoint **which** scale-dependent decision introduces the keyless broadcast-Cross: the galaxy/bridge/chain reroot
(`reroot_galaxy_join_tree` / `reverse_bridge_chain_join_tree`) or the broadcast strategy selection. Record the
exact decision + the non-key edge it picks; the Phase-2 hook is whatever that trace names.

**Method (paid for over three passes - use it, do not re-learn it):**
- **Minimal synthetic repro, not the 8-way query.** Build a 3-4 table comma-join with one non-key edge plus seeded
  stats that tip the optimizer to broadcast it (large fact + small dimension on a non-shared key). This reproduces
  "scale-dependent broadcast over a non-key edge" in a fast build with a tiny trace - the full q08/q09 catalog
  cost ~4-min builds for no extra signal.
- **Summarize, never dump.** Print one line - the `GraphShape`, the reroot/broadcast call that fired, the edge.
  `{:#?}`-ing the whole `OptimizerExplainRenderTrace` stack-overflows (SIGABRT); that was a probe bug, not a bind
  bug. Run trace tests on a 64 MB thread stack.
- **`scripts/cargo-cgroup` only, never bare `cargo`** (bare cargo hangs/crashes the machine).
- **Repro validity gate (the trap to avoid):** the fixture is valid ONLY if it **bind-fails under the *default*
  cap** with `unsupported_cross_join` - q08/q09's sf=1 signature. A *passing* keyed-broadcast bridge (e.g. the
  selective-bridge fixture) is the **wrong shape** and proves nothing. The non-key edge must be real: a big fact
  `F` plus `A`, `B` that share no key with each other but both key to `F` (`F.a = A.k`, `F.b = B.k`); confirm the
  keyless Cross *appears* under a relaxed cap (the sf=1-relaxed observation).
- **Sufficiency clause:** the same fixture must bind as keyed/all-`Inner` at the small/default config and only
  become a keyless Cross when scale tips it. A predicateless always-Cross cartesian is the wrong mechanism even
  if it hits the error string.

**Branches already eliminated (do not reopen):** (1) harness FROM-reorder - rejected canonical-dodge; (2) static
comma-join re-association - overturned (q08 is all-`Inner` at sf=0.01, nothing to reassociate); (3) heavy
full-query trace probe - thrashed on the mechanics above. The open set is now a single bounded unknown: the one
optimizer call that selects the non-key broadcast edge.

## Decision - keep q08/q09 keyed at scale (fix the optimizer decision, not the symptom)

### Phase 2 - the fix (IMPLEMENTED in `outer_join_demotion.rs`)
Phase 1b narrowed the decision site: the lifted `(A x B) join F` comma-equi **Cross child reaches broadcast
strategy/admission un-reassociated at scale**, so admission sees a keyless Cross and (default cap) rejects it. The
fix **reassociates the lifted Cross child into keyed joins before strategy/admission** - when both Cross children
have equi-keys to the other relation - with passthrough project/filter handling and a **nested-Cross-equi guard** so
early reassociation does not strand a true child key (this protects q05). User-visible output order is preserved
(the mediated `SELECT *` pins `A, B, F`).

Reconcile vs the earlier framing: this **is** comma-join re-association - but the *correct* kind
(reassociate-before-admission, guarded). What was overturned was the *naive sf=0.01* attempt
(`reassociate_cross_child`), which had no Cross to act on at the probe scale and lacked the guard - not
re-association as such. Still **not** a harness reorder (the rejected dodge) and **not** a cap-bump (the
relaxed-cap trap).

## Acceptance - the official query stays keyed at scale
- **q08/q09 bind keyed (all-`Inner`, no keyless Cross) at sf=1**, on the **official** query (FROM order unchanged),
  under the **default** broadcast cap - asserted via a keyed-plan check that runs at **both sf=0.01 and sf=1**
  (sf=0.01 already passes; sf=1 is the new bar).
- **q02 (free beneficiary - same mechanism, same fix):** the reassociation also keys q02's `(A x B) join F` at
  scale - q02 moved `bind-fail(unsupported_cross_join)@sf=1` -> `pass@execute` (17.0s / 1.05 GB, keyed, not a
  cartesian). Guarded by `strict_q02_stays_keyed_at_small_and_scaled` (same `assert_no_cross_join` canary as
  q08/q09; revert the reassociation -> q02 re-crosses at sf=1). Update the ADR 330 atlas: q02 sf=1 bind-fail ->
  pass@execute. So this cut clears **3** cartesian cells, not 2.
- **Runtime:** sf=1 off `unsupported_cross_join@bind` -> `pass@execute` (under caps), or an honest deeper measured
  wall named for the next cut.
- **sf=0.01 boards unchanged** (q08/q09 still keyed `pass@execute` + `pass@compare`); reach 22 / correctness 22
  hold. Full `-p toron --no-fail-fast` + `architecture_test`.
- No regression on the shared join-order/broadcast path (other queries' plans unchanged where already keyed).

## Risks
- **Reassociate before admission, guarded** - the fix keys the lifted Cross child *before* broadcast admission; the
  abandoned `reassociate_cross_child` failed only because it acted at sf=0.01 (no Cross there) and without the
  nested-Cross-equi guard. The guard is load-bearing (it preserves q05's child keys) - the shared join path needs
  the full suite + `architecture_test` as the net.
- **No cap-bump** - relaxing the broadcast cap *admits* the cartesian (the relaxed-cap probe is exactly the trap).
- **No gauge rewrite** - official q08/q09 unchanged.
- **Phase 1b first** - the sf=1 trace names the decision (reroot vs broadcast selection); don't patch blind (the
  prior blocker came from patching without the sf=1 decision in hand).
- **Shared optimizer path** - full suite + `architecture_test`; the change must be a no-op where the keyed plan is
  already chosen.

## Forward arc
q08/q09 keyed at scale -> 2 cartesian cells clear honestly. q02 (correlated scalar recombine) separate;
q04/q12/q20 (ground the `unrecognized` execute-fails); q07/q14/q17/q18/q19/q21 (cost-bound/streaming memory). The
finding generalizes: several memory walls may be the *same* scale-dependent broadcast-over-non-key-edge decision -
Phase 1b's trace will say.

## Why toron-home
A `crates/toron` cost-optimizer cut (join_order.rs / broadcast admission). The cells flip in `toron-tpch`'s scale
atlas with the official queries unchanged. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Codex-lite + Claude. Two framings overturned by grounding: the harness reorder (rejected dodge)
and the static comma-join re-association (the probe shows all-`Inner` at sf=0.01 - no static Cross). The corrected
diagnosis: a **scale-dependent cost-optimizer decision** broadcasts over a non-key edge at sf=1, producing the
keyless Cross the default cap rejects. Phase 1b grounds which decision; Phase 2 fixes it (keep the keyed plan), not
the symptom. Measure-don't-predict, twice over - the cut is only as good as the bound-plan evidence.
