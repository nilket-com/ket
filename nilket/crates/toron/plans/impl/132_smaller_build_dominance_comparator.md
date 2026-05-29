# Impl Plan — ADR 132: Smaller-Build Dominance Comparator

**Migration for** `plans/adr/132_smaller_build_dominance_comparator.md`.
**Scope:** `crates/toron/src/binder.rs` (comparator + named constant +
`BroadcastInferred.rejected_side_bytes`), `crates/toron/src/lib.rs` (TCP digest
parity gate), `crates/toron/tests/architecture/rune_frontend.rs` (guards under
the `architecture_test` harness).
**Non-scope:** executor, fragmenter, scheduler, wire, Wendelon, config,
`PlanOrigin`, `JoinStrategy`, `ExecutionPlan::Join`, 130 swap/projection, 131 predicate.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| both-eligible left-wins (the parked tie) | `binder.rs:3822-3847` (left arm returns first) |
| `left_build_bytes`/`right_build_bytes` computed | `binder.rs:3805-3820` |
| `BroadcastInferred { build_side, build_bytes, threshold }` | `binder.rs:207-211` |
| `BroadcastBuildSide` / `JoinPhysicalPlacement` enums | `binder.rs:199` / `:193` (use `BroadcastBuildSide`, NOT `JoinBuildSide`) |
| tie test to retarget | `both_sides_tiny_prefers_left_build` `binder.rs:7509` (seeds 1024/1024; asserts `["id","customer_id","id","region"]`) |
| `BroadcastInferred` construction sites | `binder.rs:3827`, `:3840` (+ test asserts `:7848`, `:7874`) |
| `ByteCount(pub u64)` | `distributed/budget.rs:17` |
| hard byte facts | `TableOptimizerFacts` (`catalog.rs:79-82`) → `PlanOptimizerFacts` |

## Phase 0 — failing gate first
`both_sides_eligible_right_materially_smaller_prefers_right_build` must **fail on
`main`** (returns left today). `both_sides_tiny_prefers_left_build` (retargeted to
equal-keeps-left) passes before+after.

## Phase 1 — constant + comparator (exact u128)
```rust
const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR: u64 = 2;
fn right_build_materially_smaller(left_bytes: ByteCount, right_bytes: ByteCount) -> bool {
    u128::from(right_bytes.get()) * u128::from(AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR)
        <= u128::from(left_bytes.get())
}
```
(Codex's `u128` form — exact, no overflow/saturation edge.)

## Phase 2 — restructure selection (don't touch 3805-3820 or the hash ladder)
Replace the two sequential broadcast arms (`3822-3847`) with a viability match;
the `(None,None)` arm **falls through to the unchanged 131 hash-reason ladder**
(`ProbeNotScanLinear` → origin → missing → above-threshold):
```rust
let left_viable  = left_build_bytes.filter(|_| right_input.probe_scan_linear);
let right_viable = right_build_bytes.filter(|_| left_input.probe_scan_linear);
let chosen = match (left_viable, right_viable) {
    (Some(l), Some(r)) if right_build_materially_smaller(l, r) =>
        Some((BroadcastBuildSide::Right, JoinPhysicalPlacement::SwappedRightBuild, r, Some(l))),
    (Some(l), Some(r)) =>
        Some((BroadcastBuildSide::Left,  JoinPhysicalPlacement::Natural,           l, Some(r))),
    (Some(l), None) =>
        Some((BroadcastBuildSide::Left,  JoinPhysicalPlacement::Natural,           l, None)),
    (None, Some(r)) =>
        Some((BroadcastBuildSide::Right, JoinPhysicalPlacement::SwappedRightBuild, r, None)),
    (None, None) => None,
};
if let Some((build_side, placement, build_bytes, rejected_side_bytes)) = chosen {
    return JoinStrategyDecision { strategy: JoinStrategy::Broadcast, placement,
        reason: JoinStrategyDecisionReason::BroadcastInferred {
            build_side, build_bytes, threshold, rejected_side_bytes } };
}
// unchanged 131 ladder below
```

## Phase 3 — receipt field
Add `rejected_side_bytes: Option<ByteCount>` to `BroadcastInferred`
(`binder.rs:207`). Update the two non-test construction sites (produced by the
Phase-2 match) and the two test constructions (`:7848`, `:7874` →
`rejected_side_bytes: None` for the existing single-eligible cases). **Confirm
`JoinStrategyDecisionReason` is binder-private (not wire-serialized)** — grep
wire/sql_wire; the `adr132_no_…_wire` + existing 130/131 no-wire guards back this.

## Phase 4 — guards + tests
Add the six `adr132_*` guards + the behavior gates (Phase-0 + near-tie + equal +
sole-eligible + the `lib.rs` TCP digest parity `dominance_right_build_byte_identical_to_hash_over_tcp`).
**Retarget** `both_sides_tiny_prefers_left_build` to prove the margin (equal →
left, assert `rejected_side_bytes == Some(1024)`); retarget any 130 assertion
pinning "left always wins when both eligible" → "left on tie/near-tie, right on
material dominance."

## Phase 5 — verification
`cargo test -p toron` (the new gates + digest parity) · `cargo test -p toron
--test architecture_test -- adr129 adr130 adr131 adr132` (filters **after `--`**) ·
`cargo check -p toron --all-targets` (E0063 backstop for the new `BroadcastInferred`
field — [[feedback_verify_all_targets_on_new_ir_field]]) · `git diff --check`.

## Risk register
- **New struct field → E0063** at every `BroadcastInferred` site; `--all-targets`
  is the backstop.
- **Overflow:** use `u128::from` (exact), not `u64` `saturating_mul`.
- **Reason-ladder regression:** `(None,None)` must fall through to the unchanged
  131 ladder; `ProbeNotScanLinear` stays reachable (131 gate covers it — keep green).
- **Wrong enum name:** use `BroadcastBuildSide` (grounded `binder.rs:199`), not
  `JoinBuildSide`.
- **Margin value is the knob:** 2× is the call; named constant = one-line change.
- **Guard inversion as deletion:** retarget the 130 tie guard explicitly (#4).
- **Wire leak:** keep `rejected_side_bytes` binder-private; no-wire guard covers it.

## Acceptance
Both-eligible material-right-win → right-build; near-tie/equal → left; sole-eligible
→ `None` rejected bytes; right-build output byte-identical to hash; build
single-base+hard-byte+scan-linear-probe intact; 129/130/131/125/128 green; no
executor/fragmenter/scheduler/wire/Wendelon/PlanOrigin/config change; six `adr132_*`
guards + retargeted tie coverage + 129/130/131 guards green.
