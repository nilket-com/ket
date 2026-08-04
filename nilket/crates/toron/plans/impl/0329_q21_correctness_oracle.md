# ADR 329 - Implementation plan: q21 correctness oracle (series closeout)

Pairs with `plans/adr/0329_q21_correctness_oracle.md`. Oracle + mandatory discrimination synthetic in `toron-tpch`,
plus the canonical CLI wiring. Verify-first. Strict-only canonical row. Closes the board: **correctness 22 / reach
22**.

## Phase 0 - settle vacuity + the anti-join discrimination
1. **Contributing-row count at sf=0.01:** count qualifying `l1` rows (late `S`, order has another supplier, no
   other late supplier, status-F, SAUDI). Not output rows and not deduplicated order/supplier pairs. Record it;
   thin/0 expected -> synthetic also covers vacuity. Measure, do not predict.
2. **Precision/schema:** `numwait` is `Int64` (COUNT(*)), `s_name` is `Utf8`.

## D1 - the oracle (`crates/toron-tpch/src/lib.rs`)
- Pattern: `q11_oracle`/`q16_oracle`/`check_strict_q15`/`check_strict_q19`.
- Traverse `SupplierGenerator`, `OrderGenerator`, `LineItemGenerator`, `NationGenerator`.
- Per `l_orderkey`: supplier set + late-supplier set. Lineitem `l1` contributes iff late `l1` AND (some other
  supplier on the order) AND (no other late supplier). Restrict to status-F orders + SAUDI suppliers
  (`s_nationkey = n_nationkey`, `n_name = 'SAUDI ARABIA'`).
- `numwait[s_name]` = COUNT of contributing `l1` rows; `ORDER BY numwait DESC, s_name`, `LIMIT 100`.
- Comparator `compare_strict_q21` + `check_strict_q21`: assert the `(s_name Utf8, numwait Int64)` schema; reject
  nulls slot-by-slot; compare by stable `s_name` identity; assert the `numwait DESC, s_name` order.

## D2 - the discrimination synthetic (mandatory)
- A synthetic fixture asserting each case:
  - **QUALIFY:** SAUDI `S1` late on a status-F order with on-time `S2` -> counted.
  - **NOT EXISTS fails:** `S2` also late on that order -> `S1` excluded.
  - **EXISTS fails:** single-supplier order (`S1` only, late) -> `S1` excluded.
  - **l1 on-time:** `S1` not late -> excluded.
  - **wrong nation/status:** non-SAUDI / non-F -> excluded.
- The test asserts `numwait` equals only the qualifying contributions and that each control is excluded - proving
  the EXISTS + NOT-EXISTS anti-join both ways (the ADR 323 keyed semi/anti residual).

## D3 - canonical wiring (mandatory)
- Add `"q21" => check_strict_q21(sf).await` to `main.rs` `correctness_command`.
- Confirm `correctness-matrix` (derives from `ALL_QUERY_IDS`) shows `q21 strict=pass@compare` once the row writes.

## Tests (`correctness_test.rs`)
- **q21 oracle** - the traversal; the `(s_name, numwait)` schema; null reject; `numwait DESC, s_name` order; stable
  identity.
- **Contributing-row vacuity** - assert >= 1 contributing `l1` row (live or synthetic).
- **Anti-join both ways** - the QUALIFY case counted; each control (NOT-EXISTS-fails, EXISTS-fails, on-time,
  wrong-nation/status) excluded (discriminating).
- **Expected pass** - q21 strict = `pass@compare` (or an honest red with a named reason if Phase 0 finds a bug).

## Phase 3 - the canonical board (NOT optional)
```
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
```
- `correctness --query q21` -> a real `phase=compare status=pass` row; `correctness-matrix` -> `q21 strict=pass@compare`
  (count **22**); reach 22/22; other cells unchanged. **The board is fully green.**

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0329_q21_correctness_oracle_results.md`: the oracle; the Phase-0 contributing `l1`-row count
(live + synthetic); the QUALIFY + four controls (anti-join both ways); the `(s_name, numwait)` schema; q21 strict
`pass@compare` (count 22) confirmed on the **canonical matrix**; the board now fully green (correctness 22 / reach
22); reach + other cells unchanged.

## Risk / blast radius
- **EXISTS/NOT-EXISTS discrimination** - the synthetic controls prove the anti-join both ways; range/late boundaries
  explicit.
- **Vacuity** - contributing `l1` rows, not output; synthetic mandatory.
- **Canonical wiring** - `correctness --query q21` + `correctness-matrix` part of the seal.
- **ORDER/ties/LIMIT** - deterministic `numwait DESC, s_name`; keep LIMIT 100.

## Seal
`impl: adr 329 q21 correctness oracle`. Do not commit wip drafts.
