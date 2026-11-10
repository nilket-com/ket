# ADR 0708: a refused generation commit changes nothing

Status: proposed, 2026-11-10 (Claude, Codex). Shape agreed (Codex 01a1c60a: a separate small fix that checks the complete freed sum and the retained-byte subtraction before any removal, with a control showing the refusal leaves the state identical). Implementation lands with this plan. Closes DC-205 once its implementation and evidence are reviewed. Review record: `reviews/dc195-layout-collision/` (DC-205 entries).

## Why
- **DC-205 (contract breach; found by the ADR 0707 replay census, not observed in a run).** The decision reducer's contract is that every refusal leaves state unchanged (`consensus/decisions.rs`, `apply`), and replay arguments, such as ADR 0707's, rely on refusals having no effect.
- **The breach:** `commit_generation` (`consensus/decisions/generations.rs`) removed the covered bodies and no-ops, summing their bytes with unchecked additions, and only then subtracted the sum from `retained_claim_bytes` with `checked_sub(…).ok_or(CounterOverflow)?`. Because `apply` writes the decision state back on a refusal, a `CounterOverflow` refusal left the cut erased while the generation stayed uncommitted.
- **Reach:** only when the retained-bytes invariant (`DecisionState::validate`: retained bytes equal the sum over bodies and no-ops) is already broken. The effect would be deterministic across replicas.
- **Census of the other writes of `retained_claim_bytes`:** Reserve and RecordNoop check (`checked_add`, the retained bound, log order) before committing anything. The two other `CounterOverflow` sites (`commit_hlc_not_ahead`, `fresh_clock`) are pure checks that run before any mutation. `commit_generation` was the one exception.

## Decision
**`commit_generation` computes the freed bytes from the covered lists with checked addition, and checks the subtraction, before it removes anything; only then does it commit the removals, the new retained total, the certificate and the cleared pending target.**

- Its result is unchanged when the invariant holds.
- A refusal now changes nothing.
- No format, wire or command change.

## Acceptance (finite)
1. **A refused commit changes nothing:**
   - With every member's report recorded and the retained-bytes invariant broken on purpose (retained one byte short of what the cut frees), `CommitGeneration` is refused `CounterOverflow`, and the decision state equals its value before the command.
   - With the invariant restored, the same command commits and erases the cut, and `validate` passes.
   - Fails on today's code: the refusal removes the cut's body.
2. **The existing decision and generation tests pass** (`consensus::decisions`), with the focused set and one checkpoint at the tip.

## Not established
- A structural guarantee that every reducer arm checks before it mutates. The census above covers the byte-accounting sites only; the contract stays a per-arm discipline.
- DC-201, §8, the ADR 0707 drain limits, and the other registered limits (unchanged).
