# ADR 344 - Implementation plan: q21 semi/anti extension

Pairs with `plans/adr/344_q21_semi_anti_extension.md`. A `crates/toron` (exec.rs) cut: q21's inner prefix reuses 343's
reoriented-inner-chain primitive; the semi/anti suffix is a **measurement-gated** build-side cut (likely spill/budget the
existence-set + apply the non-equi residual). **Green-or-named-wall, strong prior named.** Evidence provenance is
explicit; structural-only cannot justify a green-target.

## Phase 0 - prove the semi/anti sides (the side-proving table)
- For each q21 LeftSemi/LeftAnti join, produce:

  | join_id | kind | preserved_side | preserved_source | preserved_bytes | existence_side | existence_source | existence_bytes | verdict |

- `preserved_source` / `existence_source` are one of: **sf=1 trace** (actual, if reached), **synthetic isolated-operator
  probe** (the semi/anti join run in isolation on sf=1-sized inputs - actual bytes, synthetic inputs), **sf=0.01
  scaled** (inference), **structural-only** (plan shape, no bytes). Label every byte; never present structural as sf=1.
- **The rule:** structural-only sizes the next probe / names the wall; a **green-target needs sf=1 trace or synthetic
  isolated-operator probe** showing the existence-set is small. Default to the **synthetic isolated-operator probe** to
  get real existence-set bytes at sf=1 scale (since the natural sf=1 boundary is unreached).
- Confirm the inner prefix `Join(1-2)` matches the 343 primitive (small build, streamed probe, output-order restore).
  Do **not** assume the current ADR 343 full-chain detector matches q21: it is fenced to the all-Inner 1-4 chain. If
  needed, factor a prefix-capable inner-chain visitor while preserving the existing ADR 343 full-chain path unchanged.

## The gate (cut shape follows the table)
- **Both sides large** -> spill/budget the existence-set build (reuse 336), stream the preserved side as the probe,
  apply the match_filter residual per batch. Output: a **named** existence-set build/spill wall (332).
- **Existence-set small (only on sf=1-trace / synthetic-probe)** -> 343-style reorient (build small existence-set,
  stream preserved, semi/anti match + residual) -> may green q21.

## D1 - the cut (per the gate)
- Inner prefix: reuse 343's reoriented-inner-chain primitive. This may require factoring a prefix-capable visitor out of
  the current full-chain helper; the existing ADR 343 full-chain path must stay byte-identical and shape-fenced.
- Semi/anti suffix (strong-prior path): for `Join(3)`/`Join(4)`, **spill/budget the big existence-set build** (reuse
  `execute_spilled_hash_join` / `guard_spill_retained_entry_memory`), stream the preserved side, apply the
  `match_filter` non-equi residuals per probe batch: LeftSemi applies `l2.l_suppkey <> l1.l_suppkey` and emits
  preserved-where-match; LeftAnti applies `l3.l_suppkey <> l1.l_suppkey AND l3 late` and emits
  preserved-where-no-match. Preserved side stays the output (no semantic flip).
- Any existence-set build/spill over budget -> a named producer (e.g. `semi/anti existence-set build/spill
  accumulator`, exact name to the impl), not a silent OOM.

## D2 - residual correctness (load-bearing)
- The non-equi `match_filter` must be applied in the streamed/spilled probe, per batch, for both semi and anti:
  `l2.l_suppkey <> l1.l_suppkey` for EXISTS and `l3.l_suppkey <> l1.l_suppkey AND l3 late` for NOT EXISTS. A key-only
  semi/anti silently wrong-answers q21 - the ADR 329 oracle (EXISTS + NOT-EXISTS both ways) is the correctness gate.

## Tests (`toron` lib + `toron-tpch`)
- **Semi/anti residual correctness:** a synthetic LeftSemi/LeftAnti with a non-equi residual -> streamed/spilled probe
  result equals the materialized result, including the residual (key-match AND `<>`); the ADR 329-style controls
  (qualify / NOT-EXISTS-fails / EXISTS-fails) hold.
- **Existence-set spill/budget:** an over-budget existence-set build emits the named diagnostic, not a silent OOM
  (336 lesson).
- **Inner-prefix reuse:** q21's inner prefix takes the 343 primitive; the existing ADR 343 full-chain path remains
  unchanged.
- **No semantic flip + order:** preserved side stays the output; q21 column/row order matches the materialized plan
  (sf=0.01 parity).
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# q21 @ sf=1 (4G): green (only if the gate's measurement justified it), OR a named semi/anti existence-set
#   build/spill wall (332 retention: producer + bytes), not a silent OOM.
```
- q21 green or the named wall via 332; update the ADR 330 atlas q21 cell.
- **Green-cell regression:** q01/q03/q05/q06/q10/q13/q16/q22 + q07/q14/q19 stay green, unchanged plans.
- q17/q12/q18/q04/q20 unchanged.

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
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01   # residual + order preserved
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q21 sf=1 -> green or named existence-set build/spill wall; green cells unchanged
```

## Results doc
`crates/toron/plans/impl/344_q21_semi_anti_extension_results.md`: the Phase-0 side-proving table (preserved/existence
sources + bytes for every row); the gate verdict (spill vs reorient); the cut; q21 sf=1 before/after (green or named
wall via 332); the residual-correctness result (329 controls); sf=0.01 q21 pass@compare; the green-cell regression
check; the ADR 330 atlas q21 update.

## Risk / blast radius
- **Evidence provenance** - the side-specific source columns + the structural-only-can't-green rule guard against
  claiming unreached sf=1 bytes.
- **Residual** - the non-equi match_filter must ride the probe (ADR 329 gate); not key-only.
- **No silent wall** - existence-set build/spill named (332).
- **Scope** - q21 only; inner prefix is 343 primitive reuse; semi/anti is the new piece; no cost-model change.

## Seal
`impl: adr 344 q21 semi anti extension`. Do not commit wip drafts.
