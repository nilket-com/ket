# ADR 466 - hash-swap plumbing: honor the placement, then reinstate 464

Diagnose and fix why the hash-strategy `SwappedRightBuild` broke q17 (465's
falsification), then reinstate ADR 464's comparator with invariance evidence on a
source that arms it. The diagnosis is complete by inspection and will be pinned by
test before the fix.

## The mechanism (grounded, to be pinned)

- The operand swap (`std::mem::swap` of plans/keys) exists only inside the
  `JoinStrategy::Broadcast` placement match (`binder.rs:3092-3098`). The
  `HashPartitioned` arm exchanges both sides and **never consults placement**.
- `restore_declared_join_order_project` keys on placement alone
  (`binder.rs:3199`): with 464's decision returning `SwappedRightBuild` under hash,
  the restore ran for a swap that never happened — a column-order scramble that
  survives schema-width checks (left/right widths swap-symmetric in q17's shape)
  and starves the downstream correlated aggregate: `avg_yearly` NULL.
- This also explains 465's second falsification: constraining *where* the
  comparator fires cannot help when the *rewrite* ignores the placement it emits.

## Decision — three rails, strictly ordered

**Rail 1 — pin the defect.** A binder unit test binds an inner join with byte facts
that force `(HashPartitioned, SwappedRightBuild)` (the 464 decision) and asserts the
physical plan actually swapped: build side is the smaller relation's plan AND the
restore project's input schema equals the swapped physical schema. This test must
fail on the pre-fix plumbing (restore without swap) — the discriminating pin.

**Rail 2 — the fix.** The hash arm honors placement exactly as broadcast does:
swap operands, keys, and resolved keys **before** building the two exchanges (both
sides still exchange at the default partition count — hash symmetry makes the swap
purely a build/probe role change). `validate_strategy_shape` and the mirrored-kind
mapping already accept inner swaps.

**Rail 3 — reinstate 464, evidenced on an armed source.** Executed with a deviation
discovered mid-cut and folded in: with honest plumbing, the comparator still broke
15 join-order tests — the star/snowflake preflights **require placement uniformity
across candidate orders**, and a size-sensitive placement varies by order, by
design. That is a subsystem contract, not stale pins. The comparator therefore
ships fully plumbed and pinned but **default-off** behind
`PlannerConfig::hash_smaller_build_placement`; flipping the default belongs to a
join-order design cut that owns relaxing (or scoping) the uniformity invariant.
Under the default, production behavior is byte-identical to the post-revert state.
The armed-source evidence below validates the default-off production
configuration; the flagged tests validate the capability:

- q17 strict parquet correctness at its special scale — the 465 fixture — green.
- The full 22-query correctness sweep on **parquet** (byte facts present, swap
  armed) at the default scale, with any query needing a special scale run at it.
- The 464 binder decision-table tests restored and green; the new Rail-1 plumbing
  test green post-fix.
- q05 sf=8 gate-zero identity recheck (fingerprint/digests/counts) as the standing
  fixture sentinel.

## Acceptance

- Rail 1 test demonstrably fails pre-fix (recorded), passes post-fix.
- All Rail 3 evidence green; full architecture_test and engine suite green.
- The receipt records the mechanism, the pin, and the armed-source sweep — the
  465-banked rule applied, not just cited.
- No wall/materiality claims; placements that newly flip on parquet sources create
  epochs only where they engage, and the standing fixtures are re-checked, not
  assumed.

## Guardrails and risks

- **Fix before reinstatement.** Re-applying 464 without Rail 1+2 would re-ship the
  falsified state; order is mandatory.
- **The restore stays placement-keyed** — after the fix, placement and rewrite
  agree by construction; a divergence recreates exactly this bug, which is what the
  Rail-1 pin guards.
- **Sweep cost is real but bounded** (small scales, release binary).
- **Do not re-judge sealed verdicts**; 464's falsification record stands as
  history, amended only by pointer to this cut.

## Forward arc

With the plumbing honest, pricing a cell where the hash swap engages becomes
available paired-instrument work. Behind it: q21 residual, q19 re-diagnosis + 438
widening, q13, Cut A (439 mirror), the O(1) LRU rewrite, and the budget-derivation
gauge remain queued.
