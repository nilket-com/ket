# ADR 302 — Implementation plan: Decimal add/sub operand alignment (mixed precision/scale)

Pairs with `plans/adr/0302_decimal_add_sub_operand_alignment.md`. Engine cut (`crates/toron`, shared binder
surface). Anchors are current-tree. Binder-typing only — **no exec change** (Arrow numeric handles it).

## Phase 0 — confirm the Arrow grounding
Confirm before editing (Codex's iter-1 grounding):
- `bind_arithmetic` (`arithmetic.rs:1001`) rejects mixed-(p,s) Decimal `+`/`-` at `:1196`; same-(p,s) arm `:1162`
  = `Decimal128(min(38,p+1), s)`.
- The executor routes Decimal `+`/`-` to `arrow_arith::numeric` `add`/`sub` (not `evaluate_arithmetic`), and those
  rescale differing-(p,s) operands to `max(s)` with the Hive-rule output. A quick `arrow_decimal_probe` check that
  Arrow `sub(Decimal128(32,4), Decimal128(31,4))` yields `Decimal128(33,4)` de-risks the whole cut.

## D1 — `decimal_add_sub_output_type` + generalize the block (`arithmetic.rs`)
```rust
fn decimal_add_sub_output_type(p1: u8, s1: i8, p2: u8, s2: i8) -> Option<DataType> {
    if s1 < 0 || s2 < 0 { return None; }
    let scale = s1.max(s2);
    let int_digits = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
    if int_digits < 0 { return None; }
    let precision = (int_digits + i16::from(scale) + 1).min(38);   // +1 carry, cap (matches Arrow)
    if !(1..=38).contains(&precision) { return None; }
    Some(DataType::Decimal128(precision as u8, scale))
}
```
- **Replace** the same-(p,s) `+`/`-` block (`:1162`) with: `if matches!(op, Add|Sub) && both Decimal128` →
  `decimal_add_sub_output_type(p1,s1,p2,s2)` → `append_binary(op, left, right, output)` (operands **unchanged** —
  no casts). `None` → fall through to the existing reject (a genuinely invalid shape).
- Confirm the same-(p,s) case still yields `Decimal128(min(38,p+1), s)` (subsumed: `int_digits = p-s`,
  `precision = (p-s)+s+1 = p+1`, capped — identical to ADR 266).
- Do **not** touch `decimal_comparison_common_type` / `append_comparison_cast` / `reconcile_*` (no rename).

## D2 — exec: none
Arrow `numeric::add`/`sub` already handle the differing-(p,s) operands + stamp the same output type. No new
`evaluate_*` arm. **Verify-first proves this**: if q09 strict walls at *execute* (not bind), the grounding is
wrong → fall back to binder-inserted operand casts (the iter-0 path); the live run is the check.

## Tests
- **Decimal behavior probes** (`crates/toron-tpch/tests/decimal_probe.rs` + `arrow_decimal_probe.rs`, exercising the
  Toron binder/executor through the harness test surface):
  - `Decimal128(32,4) - Decimal128(31,4)` (q09's shape) binds → `Decimal128(33,4)` and **executes** the correct
    value; a different-scale pair (`Decimal128(15,2) + Decimal128(15,4)` → scale 4) binds + executes.
  - `arrow_decimal_probe`: Arrow's `sub`/`add` output type **equals** `decimal_add_sub_output_type` for these
    pairs (pin the binder↔executor agreement).
  - same-(p,s) `+`/`-` unchanged (no regression on ADR 266); Int/Float arithmetic unchanged.
  - revert the generalized block → mixed-(p,s) rejects at `:1196` (discriminating).
- **Architecture:** if any guard pins the same-(p,s)-only `+`/`-` shape or the reject string, reconcile it; run
  the full `architecture_test`.
- **Gauge** (`toron-tpch`): verify-first re-measure q09 strict.

## Phase 3 — verify-first re-measure q09
```
target/debug/toron-tpch run --query q09 --mode strict --sf 0.01 --allow-blocked
```
- `pass@execute` (keyed at sf=0.01) → flip the q09 harness row to dev+strict green; reach → 14. Confirm keyed
  (not cartesian).
- a further wall → record the precise reason (next ADR); no broad classifier.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared binder cut)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```
Plus empirical: the live q09 strict (+ dev) runs.

## Results doc
`crates/toron/plans/impl/0302_decimal_add_sub_operand_alignment_results.md`: the `decimal_add_sub_output_type` rule
+ the Arrow grounding (binder type == Arrow stamp); q09 strict's new row (green → reach 14, or a wall); same-(p,s)
no-regression; confirmation prior reach/correctness rows unchanged.

## Risk / blast radius
- **`bind_arithmetic` is shared** — the generalized block must fire only for Decimal `+`/`-`; `÷`/multiply/Int/Float
  untouched. Full `--no-fail-fast` is the net.
- **Binder type must match Arrow's runtime** — the `arrow_decimal_probe` pins it (else the bound `(p,s)` drifts
  from the executor's stamp).
- **No exec change is the bet** — verify-first (q09 execute) confirms; cast fallback if Arrow doesn't handle it.
- **Preserve the cap** (no fail-loud this cut) — consistent with the same-(p,s) arm + Arrow.

## Seal
`impl: adr 302 decimal add sub operand alignment`. Do not commit wip drafts.
