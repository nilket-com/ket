# ADR 327 — Implementation plan: q18 correctness oracle

Pairs with `plans/adr/0327_q18_correctness_oracle.md`. Oracle (+ mandatory synthetic if thin) in `toron-tpch`, plus
the **canonical CLI wiring**. Verify-first. Strict-only canonical row.

## Phase 0 — settle the three open questions (before pinning)
1. **Non-vacuity (the crux):** measure q18's live strict row count at sf=0.01 (`run --query q18 --mode strict`, or a
   draft oracle). Record it. If 0 / trivially thin → the `> 300` filter isn't proven → **mandatory synthetic**
   (D2).
2. **Types/precision:** confirm strict `SUM(l_quantity)` type (Decimal vs Int) + `o_totalprice` `Decimal(15,2)`;
   mirror exactly.
3. **ORDER/LIMIT/ties:** confirm `o_totalprice DESC, o_orderdate` order; whether LIMIT 100 binds at sf=0.01; and the
   intra-tie behavior (compare tie rows by stable identity, not exact position).

## D1 — the oracle (`crates/toron-tpch/src/lib.rs`)
- Independent traversal (pattern: `q11_oracle`/`q16_oracle`/`strict_q15_oracle`; precision helpers
  `avg_decimal_raw`/`decimal_division_raw`):
  - IN set: per `l_orderkey`, `SUM(l_quantity) > 300`.
  - `customer ⋈ orders ⋈ lineitem`; keep `o_orderkey ∈ IN set`, `c_custkey = o_custkey`.
  - `GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)`, `SUM(l_quantity)` at the engine's
    precision.
  - `ORDER BY o_totalprice DESC, o_orderdate`, then `LIMIT 100`.
- **Comparator** (`compare_strict_q18` + `check_strict_q18`, mirroring `compare_strict_q15`/`check_strict_q15`):
  assert the strict q18 schema first; reject NULLs slot-by-slot; compare rows by a **stable identity key**
  (`o_orderkey`) so tie rows aren't brittle; assert the ordering on the distinct-key prefix.

## D2 — non-vacuity + synthetic (if Phase 0 shows thin)
- If live sf=0.01 doesn't prove the filter, add a mandatory synthetic fixture with:
  - a **passing** order: lineitems summing `> 300` → must appear.
  - a **control** order: sum `≤ 300` → must be excluded.
  - (if LIMIT isn't naturally exercised) enough qualifying rows to test `LIMIT 100`.
- The synthetic test must assert both inclusion and exclusion (the membership filter both ways).

## D3 — canonical wiring (the q15 lesson — mandatory)
- Add `"q18" => check_strict_q18(sf).await` to `main.rs` `correctness_command`.
- Confirm `correctness-matrix` (derives from `ALL_QUERY_IDS`) shows `q18 strict=pass@compare` once the row is
  written.

## Tests (`correctness_test.rs`)
- **q18 oracle** — the traversal; schema + slot null reject; stable-identity row compare; ORDER prefix + LIMIT.
- **Non-vacuity** — assert the proven subset is non-empty (live count, or the synthetic's passing order).
- **Membership both ways** — the synthetic's passing order included, control order excluded (discriminating).
- **Expected pass** — q18 strict = `pass@compare` (or an honest red with a named reason if Phase 0 finds a bug).

## Phase 3 — the canonical board (NOT optional — run these)
```
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q18
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
```
- `correctness --query q18` → a real `phase=compare status=pass` row; `correctness-matrix` → `q18 strict=pass@compare`
  (count **20**); reach 22/22; other cells unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q18
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/0327_q18_correctness_oracle_results.md`: the oracle; the Phase-0 live-count finding +
whether the synthetic was needed (with the passing/control orders); types/precision; q18 strict `pass@compare`
(count 20) confirmed on the **canonical matrix** (not just unit tests); reach + other cells unchanged.

## Risk / blast radius
- **Vacuity** — mandatory synthetic (passing-in + control-out) if the live set is thin; the non-vacuity assertion.
- **Canonical wiring** — `correctness --query q18` + `correctness-matrix` are part of the seal (the q15 miss).
- **ORDER/LIMIT/ties** — stable-identity compare; exercise LIMIT via the synthetic if needed.
- **Type/precision** — mirror exactly.

## Seal
`impl: adr 327 q18 correctness oracle`. Do not commit wip drafts.
