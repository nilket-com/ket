# ADR 331 - Implementation plan: q08/q09/q02 scale-dependent keyless broadcast-Cross

Pairs with `plans/adr/0331_scale_cut_q08_q09_keyed_cross.md`. A `crates/toron` cost-optimizer cut
(join_order.rs / broadcast admission). **The Cross is scale-dependent and optimizer-produced** - not a static
comma-join shape (overturned) and not the harness order (rejected dodge). Official q08/q09 SQL unchanged; no
cap-bump. Built binary, capped.

## Phase 1 - grounding (DONE - the probe corrected the framing)
- sf=0.01, default cap: q08 binds **all-`Inner`, no Cross** (probe `test_print_original_q8_plan`: 7x `Inner
  left_keys=1 right_keys=1`).
- sf=1, default cap: bind-fail `unsupported_cross_join`. sf=1, relaxed cap: a **keyless broadcast-Cross** appears.
- Conclusion: the sf=1 cost optimizer broadcasts over a non-key edge -> keyless Cross. The
  `reassociate_cross_child` experiment chases that symptom and cannot land (nothing to reassociate at sf=0.01); it
  should be set aside, not committed.

## Phase 1b - pinpoint the decision (REQUIRED before Phase 2; no blind patch)
**Method (banked from three passes - one fast run, not a fourth thrash):**
- **Minimal repro, not the 8-way query.** A synthetic 3-4 table comma-join with one non-key edge + seeded stats
  (large fact, small dimension on a non-shared key) that tips the broadcast. Reproduces the failure in seconds;
  the full q08/q09 catalog cost ~4-min builds for no extra signal.
- **Repro validity gate:** valid ONLY if it **bind-fails under the default cap** with `unsupported_cross_join`
  (q08/q09's sf=1 signature). A *passing* keyed-broadcast bridge (the selective-bridge fixture) is the wrong shape.
  Need a real non-key edge: big fact `F` + `A`,`B` sharing no key with each other but both keyed to `F`
  (`F.a = A.k`, `F.b = B.k`); confirm the keyless Cross appears under a relaxed cap.
- **Sufficiency clause:** the fixture must still bind keyed/all-`Inner` at the small/default config and only turn
  into a keyless Cross when scale tips it. A predicateless always-Cross cartesian is the wrong mechanism even if
  it hits the error string.
- Capture via `bind_with_explain_trace` (binder.rs:248) - already `pub(crate)`; the trace type opens are reasonable
  to keep if the grounding lands.
- **Summarize, never `{:#?}` the render trace** (it stack-overflows -> SIGABRT). Print one line: `GraphShape` +
  the reroot/broadcast call that fired + the non-key edge. Run on a 64 MB thread stack.
- **`scripts/cargo-cgroup` only** (bare `cargo` hangs the machine).
- Record: which `GraphShape`; whether `reroot_galaxy_join_tree` / `reverse_bridge_chain_join_tree` fires; the
  broadcast decision + the non-key edge. **Write the named decision into the results doc before editing the
  optimizer.**

**Eliminated (do not reopen):** harness reorder (rejected dodge); static comma-join re-association (overturned -
all-`Inner` at sf=0.01); heavy full-query probe (thrashed on builds/stack/bare-cargo).

## Phase 2 - keep the keyed plan at scale (IMPLEMENTED in `outer_join_demotion.rs`)
- Decision site (Phase 1b): the lifted `(A x B) join F` comma-equi **Cross child reaches broadcast
  strategy/admission un-reassociated at scale** -> admission sees a keyless Cross -> default-cap reject.
- Fix: **reassociate the lifted Cross child into keyed joins before strategy/admission** when both Cross children
  have equi-keys to the other relation; passthrough project/filter handling; a **nested-Cross-equi guard** so early
  reassociation doesn't strand a true child key (preserves q05); user-visible output order preserved.
- This is the *correct* reassociation (before admission, guarded) - not the overturned naive sf=0.01 attempt
  (`reassociate_cross_child`). No cap-bump, no q08/q09 SQL change.
- Reconcile any join/optimizer architecture guard the change trips.

## Tests
- **Keyed-plan canaries (sf=0.01 + sf=1, `assert_no_cross_join`)** on the official **q08, q09, and q02**:
  `strict_q0{8,9,2}_stays_keyed_at_small_and_scaled`. All three bind with no keyless Cross at both scales; revert
  the reassociation -> they re-cross at sf=1 (discriminating). q02 is the free beneficiary (same `(A x B) join F`
  mechanism), guarded by the same canary as q08/q09.
- **Engine unit** (join_order / broadcast tests): a join graph where the scale-dependent decision would broadcast a
  non-key edge keeps the keyed plan; a legitimately keyable/broadcastable case is unchanged (no-op).
- **Architecture:** full `architecture_test`.
- sf=0.01 reach/correctness stay green.

## Phase 3 - re-measure (built binary, capped, OFFICIAL queries, DEFAULT cap)
```
target/debug/toron-tpch run --query q08 --mode strict --sf {0.1,1}
target/debug/toron-tpch run --query q09 --mode strict --sf {0.1,1}
```
- sf=0.1 no OOM; sf=1 off `unsupported_cross_join@bind` -> `pass@execute` or an honest deeper wall. Update the
  ADR 330 atlas cells. q08/q09 SQL unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) - FULL toron
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup build -p toron-tpch
#   target/debug/toron-tpch run --query q08 --mode strict --sf {0.1,1}   (capped, official, default cap)
#   target/debug/toron-tpch run --query q09 --mode strict --sf {0.1,1}
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix   # reach/correctness 22/22 hold
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check    # q08/q09 SQL constants must NOT appear
```

## Results doc
`crates/toron/plans/impl/0331_scale_cut_q08_q09_keyed_cross_results.md`: Phase-1 grounding (all-`Inner`@sf=0.01,
keyless-broadcast-Cross@sf=1-relaxed-cap); Phase-1b named decision (reroot vs broadcast selection + the non-key
edge); Phase-2 fix; the sf=1 keyed-plan assertion (before/after); new scale cells; confirmation q08/q09 SQL
unchanged + sf=0.01 boards hold.

## Risk / blast radius
- **Cause not symptom** - fix the optimizer decision; the `reassociate_cross_child` experiment is the wrong layer.
- **No cap-bump / no gauge rewrite** - default cap, official queries.
- **Phase 1b first** - the sf=1 trace names the decision; no blind patch.
- **Shared optimizer path** - full `--no-fail-fast` + `architecture_test`; no-op where the plan is already keyed.

## Seal
`impl: adr 331 scale cut q08 q09 keyed cross`. Do not commit wip drafts.
