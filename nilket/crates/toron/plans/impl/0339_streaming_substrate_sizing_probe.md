# ADR 339 - Implementation plan: streaming substrate sizing probe (q07/q21)

Pairs with `plans/adr/0339_streaming_substrate_sizing_probe.md`. A **measurement + design probe** in `crates/toron` /
`toron-tpch` - no engine cut. Produces the build/probe map (Gate 1), the exchange verdict (Gate 2), the first
executable boundary, and a staged plan with expected cell movement, so ADR 340 is the smallest real substrate cut.
**No streaming implementation; no pass claim.**

## Phase 0 - Gate 1: the per-join build/probe map (the deliverable)
- Instrument (temporary, env-gated, removed before seal - the ADR 334/336/337 pattern) each hash join in q07/q21's
  sf=1 bound/optimized pipeline to record, per stable join id: `join_kind`, `strategy`, which child is the **build**
  side and which is the **probe** side, and the **observed retained bytes** of each actual child batch set (via
  `get_array_memory_size`, the ADR 336 measurement style). Base-table size and query-text inference are explicitly
  rejected; ADR 337 proved those mis-size the failing node.
- Run q07/q21 strict @ sf=1 (release, 4G cgroup cap, summary-not-dump). Emit the table:

  | query | stable_join_id | join_kind | strategy | build_side | probe_side | build_retained_bytes | probe_retained_bytes | output_estimate_bytes | downstream_consumer | exchange_before/after | streamable_as-is? | gate_verdict |

- Emit the same table for q17 as a **control only**. Its correlated-scalar branch may validate or falsify generality,
  but q17 does not widen ADR 340's first q07/q21 cut.

- **The gate verdict:** does the large fact/intermediate stay on the **probe** side through the whole chain?
  - **Yes** -> ADR 340 = bounded probe-side pipeline cut.
  - **No (a large intermediate is a build side)** -> ADR 340 = fix/route that boundary first (note which
    `stable_join_id`), else streaming moves the wall. Any side re-orientation must be semantics-fenced; q21's
    `LeftSemi`/`LeftAnti` joins are not blindly swappable.

## Phase 0 - Gate 2: exchange behavior
- For each exchange on the path, record that the current executor **materializes** its input (`bound_exchange`,
  exec.rs:2115), then determine whether the exchange is **locally pipeable** (partition + forward batches) as an
  implementation cut. Record per exchange.
- **Verdict:** pipeable -> staged substrate (join cuts compose over streaming exchange); materializing -> exchange
  streaming may be the foundational first cut before join work.

## Phase 0 - grouped-fold feasibility + the minimal interface
- Can ADR 338's `init/fold/finalize` (`try_streaming_collapsing_global_aggregate`, exec.rs:1680/1534/1613/1619) extend
  to a **grouped** aggregate? Measure observed group cardinality + grouped-state bytes for q07 nation/year and q21
  supplier-name groups; only then decide reuse vs new helper.
- Name the **minimal streaming interface** the substrate needs (a probe-side batch stream across join -> exchange ->
  join -> grouped-aggregate). Does it extend 338's chunk-fold, or require a new `execute_stream`-style path? Record
  what each option touches (operators, signatures) so ADR 340's size is known.

## Phase 0 - first boundary + staged plan
- Name the **first executable boundary** (classified by gate-branch) and **whether it moves q07/q21 without broad
  executor replacement**.
- Produce the **staged plan**: the substrate cut sequence (boundary by boundary) with **expected cell movement per
  stage** (which of q07/q21 - and later q17 - each stage is expected to move).

## Deliverable (the results doc - the probe IS the deliverable)
`crates/toron/plans/impl/0339_streaming_substrate_sizing_probe_results.md`:
- The **build/probe table** per query/join (Gate 1) with observed retained bytes + the gate verdict.
- The **q17 control table**, explicitly marked non-driving.
- The **exchange verdict** per exchange (Gate 2): currently materializing vs locally pipeable.
- The **grouped-fold feasibility** (extend-338 vs new) + the **minimal interface** named, with what each stage touches.
- The **first executable boundary** + gate-branch + whether it moves q07/q21 without broad replacement.
- The **staged plan** + **expected cell movement** per stage.
- q17 mapped as control (its correlated-scalar branch noted, not driving the design).
- Explicit no-overclaim line: 339 measures + designs; **no cell is moved by this ADR.**

## Tests
- A probe - no engine assertion, no behavioral change. Existing `toron`/`toron-tpch` suites stay green. Instrumentation
  temporary + env-gated + removed before seal (`git diff --check` clean).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check        # instrumentation removed; no engine change
# probe evidence (release, cgroup capped, recorded - not a CI gate):
scripts/cargo-cgroup build -p toron-tpch --release
#   per-join build/probe + retained bytes for q07/q21 @ sf=1; q17 control; exchange verdict
```

## Risk / blast radius
- **Gate, not background** - the build/probe table is the deliverable; without it ADR 340 guesses and may move the wall.
- **Measure, don't infer** - observed retained bytes per join side, not base-table size (the ADR 337 lesson).
- **Probe only** - map + design + stage; no streaming code, no pass claim.
- **No engine change** - sf=0.01 + the observable memory board unchanged.

## Seal
`impl: adr 339 streaming substrate sizing probe`. Do not commit wip drafts.
