# ADR 466 implementation receipt

The 465 falsification is fully explained, pinned, and fixed; the 464 capability is
reinstated as a default-off, fully-plumbed flag — because execution surfaced a
second, legitimate blocker: the join-order preflights' placement-uniformity
invariant.

## The mechanism, pinned

- Confirmed by inspection and by test: the operand swap lived only in the broadcast
  placement arm; the hash arm never consulted placement, while
  `restore_declared_join_order_project` keys on placement alone. 464's decision
  emitted `SwappedRightBuild`; the rewrite ignored it; the restore ran anyway —
  column order scrambled past width checks, and q17's correlated aggregate starved
  to NULL.
- `adr466_hash_swapped_placement_swaps_physical_build_and_restores_order` pins the
  contract (physical build side, restored schema, both-side exchanges) and **failed
  on the pre-fix plumbing exactly at the missing swap** (recorded), passing after.

## The fix

The hash arm now honors `SwappedRightBuild` — operands, keys, and resolved keys
swap before the exchanges, mirroring the broadcast arm. Placement and rewrite are
one contract by construction.

## The second blocker, and the deviation it forced

With honest plumbing, the comparator still failed 15 join-order tests — the
star/snowflake preflights **require placement uniformity across candidate orders**
(`declared_and_selected_preflights_share_uniform_strategy` is the contract's own
test), and a size-sensitive placement varies by order, by design. Not stale pins: a
subsystem invariant. The comparator therefore ships behind
`PlannerConfig::hash_smaller_build_placement`, **default off**; the 464
decision-table tests and the 466 pin opt in via the flag. Flipping the default is
owned by a join-order design cut that must decide how order search reasons about
order-dependent placements.

## Evidence (armed source, per the 465 rule)

- q17 strict parquet (the falsifying fixture): **pass**.
- Full 22-query correctness sweep on **parquet** (byte facts present): **22/22
  pass** under the production default.
- q05 sf=8 sentinel: plan fingerprint `757d7c625669b21a`, digest and identities
  unchanged.
- Binder suite 632/0 (join-order invariants restored); full architecture_test
  2,093/0 (the adr176 binder-root bound bumped 6,030 → 6,050 with ADR 466
  attribution — a real rewrite-pass change per the guard's own rule); full engine
  suite green; workspace check clean.

## What this seals

The plumbing defect class is dead (the pin guards the contract), the capability is
preserved and testable rather than reverted-and-lost, and production behavior is
byte-identical to the post-465 state. The join-order × placement design cut is the
named path to turning the capability on.
