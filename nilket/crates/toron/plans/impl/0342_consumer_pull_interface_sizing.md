# ADR 342 - Implementation plan: consumer-pull interface sizing pass

Pairs with `plans/adr/0342_consumer_pull_interface_sizing.md`. A **measurement + design probe** in `crates/toron` /
`toron-tpch` - **no engine cut**. Sizes the consumer-pull boundary (`Join(1) <- Exchange <- Filter <- Join(0)`):
the `Join(1)+` build/probe map (structural), the bounded-vs-general verdict, the interface design, and q21's
green-or-next-wall coverage - so the implementing ADR's size and acceptance are known. No pass claim.

## Phase 0 - the Join(1)+ build/probe map (STRUCTURAL, not sf=1-measured)
- sf=1 dies at the Exchange-output wall (ADR 341) **before** `Join(1)` runs, so `Join(1)+` cannot be sf=1-measured.
  Map from the **bound plan shape + sf=0.01 traces** (the partition strategy, which child is build vs probe at each
  downstream join). **Label every `Join(1)+` row `structural`** in the table; do not present a structural side as an
  sf=1 byte measurement (the 337/339 discipline).
- Record per downstream join (q07 and q21): `stable_join_id`, join_kind, strategy, build_side, probe_side, **evidence =
  structural**, and whether the large intermediate stays probe-side (pullable) or becomes a build side (held).
- q21's semi/anti joins (`LeftSemi`/`LeftAnti`) are noted + fenced (no reordering).

## Phase 0 - the consumer-pull interface design
- Inspect `bound_exchange` (exec.rs:2362) -> `Join(1)` consumption (`execute_bound(Exchange) -> Vec`, then
  `execute_bound(left/right)` at exec.rs:1000-1001 before the hash join). Design the minimal **consumer-pull**
  abstraction: `Join(1)` pulls Exchange partitions one at a time (a partition iterator / pull-based child), no
  returned `Vec`. Record what it touches: the `Join <- Exchange` arm only, or the `execute_bound` signature broadly.

## Phase 0 - the bounded-vs-general gate (the verdict)
- **Bounded:** consumer-pull introduced only for `Join(1) <- Exchange <- Filter <- Join(0)` (a special-case path like
  340/341), the rest of `execute_bound` untouched.
- **General:** the boundary forces `execute_bound` itself to become pull-based (`execute_stream` everywhere) - the
  substrate's core rewrite, a larger staged arc.
- State the verdict **with evidence** (what the special-case would touch vs what a general interface would require).

## Phase 0 - q21 coverage: green-or-next-wall
- Predict: if `Join(1)` pulls + the downstream **consumes-and-drops** through to the grouped collapse (probe-side all
  the way), q21 greens. If it **re-materializes one operator later** (Join(1) output / a post-Join accumulator), the
  wall moves there - name the predicted next producer the implementing ADR must budget+name. Structural prediction;
  the implementing ADR verifies expected-vs-actual.

## Deliverable (the results doc - the probe IS the deliverable)
`crates/toron/plans/impl/0342_consumer_pull_interface_sizing_results.md`:
- The `Join(1)+` build/probe map (q07/q21), **every datum labeled structural vs measured**.
- The **bounded-vs-general verdict** + evidence (what each touches).
- The **consumer-pull interface design** (minimal shape + surface).
- **q21 green-or-next-wall** prediction + the predicted next producer if not green; q07 compose prediction.
- Explicit no-overclaim line: 342 sizes + designs; **no cell moved**; consumer-pull is not assumed to be green.

## Tests
- A probe - no engine assertion, no behavioral change. Existing suites stay green. Any temporary instrumentation is
  env-gated + removed before seal (`git diff --check` clean).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron-tpch
git diff --check        # no engine change; instrumentation removed
# structural evidence: sf=0.01 plan-shape traces for q07/q21 Join(1)+ (the build/probe sides)
```

## Risk / blast radius
- **Structural vs measured** - `Join(1)+` is unreachable at sf=1; label its sides structural, never sf=1 bytes.
- **No green-by-assumption** - q21 coverage is green-or-next-wall; consumer-pull may re-materialize one operator later.
- **Bounded-vs-general is the deliverable** - the verdict bounds (or re-scopes) the next, largest cut.
- **Probe only** - size + design; no streaming implementation; no cell moved.

## Seal
`impl: adr 342 consumer pull interface sizing`. Do not commit wip drafts.
