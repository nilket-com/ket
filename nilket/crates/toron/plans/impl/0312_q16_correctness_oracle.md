# ADR 312 — Implementation plan: q16 correctness oracle

Pairs with `plans/adr/0312_q16_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine change.
A single-pass-per-relation independent traversal; the rail's first `COUNT(DISTINCT)`.

## Phase 0 — measure schema + non-vacuity
1. **Schema:** run strict q16 at sf=0.01, capture `batches[0].schema()` → `p_brand` Utf8, `p_type` Utf8, `p_size`
   **Int32**, `supplier_cnt` **Int64**. Assert it.
2. **Non-vacuity:** measure # groups; **≥1 group where the distinct supplier count < its kept-partsupp-row count**
   (proves COUNT(DISTINCT) dedup happens live); the filters prune live. **Assert `excluded_suppliers == 0`** at
   sf=0.01 → the anti-join + LIKE are synthetic-only (mandatory synthetics below). Pre-promotion grounding saw 296
   groups, 1,196 kept partsupp rows, 1 dedup group, 1,701 pruned parts, and 0 excluded suppliers; record the live
   implementation numbers in the results doc instead of hard-coding these probe values.

## D1 — `strict_q16_oracle` (`crates/toron-tpch/src/lib.rs`)
```rust
pub struct Q16Oracle {
    pub groups: BTreeMap<(String, String, i32), HashSet<i64>>, // (p_brand,p_type,p_size) -> distinct ps_suppkey
    pub excluded_suppliers: usize,
    pub kept_partsupp_rows: usize,
    pub dedup_groups: usize,
    pub pruned_parts: usize,
}
```
- **Excluded set:** `SupplierGenerator` → `HashSet<i64>` of `s_suppkey` where the positional matcher
  `like_customer_complaints(s_comment)` holds (reuse/generalize `q13_comment_matches_pattern` — "Customer" then
  "Complaints" after; consider a shared `like_two_segment(s, a, b)`). `excluded_suppliers = set.len()`.
- **part lookup:** `PartGenerator` → `HashMap<i64,(String,String,i32)>` keeping only `p_brand != "Brand#45"`,
  `!p_type.starts_with("MEDIUM POLISHED")`, `[49,14,23,45,19,3,36,9].contains(&p_size)`. Track `pruned_parts`.
- **partsupp:** `PartSuppGenerator` → for each row with a kept part and `!excluded.contains(&ps_suppkey)`:
  `groups.entry(key).or_default().insert(ps_suppkey)`; `kept_partsupp_rows += 1`. Keep temporary per-group row
  counts or equivalent so `dedup_groups` can be computed before returning the oracle.

## D2 — `check_strict_q16` + `compare_strict_q16` + wiring
Mirror `check_strict_q22`:
1. `execute_query_with_tables(STRICT_Q16_SQL, q16_tables(true, sf))` (reuse ADR 310's `q16_tables`); `StageError`
   → classified row.
2. `assert_strict_q16_schema(&batches)` — `Utf8/Utf8/Int32/Int64`; drift → `Compare`/`Fail`.
3. Non-vacuity: `!groups.is_empty()`, `kept_partsupp_rows > 0`, `dedup_groups > 0`, `pruned_parts > 0`;
   `excluded_suppliers == 0` records why the synthetic carries the anti-join branch at default scale.
4. `compare_strict_q16(&batches, &oracle)`:
   - extract emitted `(p_brand, p_type, p_size, supplier_cnt)`; `is_null` on every cell → `Err`;
   - **bijection by `(p_brand, p_type, p_size)`**: unique; counts equal; `supplier_cnt == set.len()` (`Int64Array`);
   - **order:** adjacent rows respect `(supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC)`.
- `main.rs`: route `correctness --query q16` → `check_strict_q16(sf)` (strict-only; mode-agnostic).
- `correctness_matrix`: q16 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q16_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread if deep).
- `strict_q16_oracle_is_non_vacuous_at_default_scale` — groups present; kept rows present; ≥1 group with real
  distinct dedup; filters prune at least one part; `excluded_suppliers == 0` is asserted and documented.
- `strict_q16_output_schema_is_asserted` — accept `Utf8/Utf8/Int32/Int64`; reject drift.
- **`q16_count_distinct_dedups_supplier_per_group`** (synthetic) — one `ps_suppkey` supplying two parts in the same
  `(p_brand,p_type,p_size)` group → `supplier_cnt = 1`, not 2 (the marquee; naive COUNT(*) would fail it).
- **`q16_not_in_excludes_complaint_supplier`** (synthetic) — a supplier with `%Customer%Complaints%` → its
  `s_suppkey` excluded from every group's distinct count (the not-live anti-join branch).
- `q16_customer_complaints_matcher_is_positional` (synthetic) — accept `"…Customer…Complaints…"`; reject
  `"Complaints…Customer"`, bare `"Customer"`, bare `"Complaints"`.
- `q16_correctness_mismatch_is_fail_at_compare` (wrong supplier_cnt); `q16_missing_extra_or_duplicate_group_is_fail`;
  `q16_null_cell_is_fail`; `q16_four_key_order_violation_is_fail`.
- Existing correctness (q01-10/12/13/14/17/20/22) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q16
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
The live `correctness --query q16` command is the empirical pass row.

## Results doc
`crates/toron/plans/impl/0312_q16_correctness_oracle_results.md`: the measured schema (Utf8/Utf8/Int32/Int64);
sf=0.01 non-vacuity (# groups, kept rows, live dedup proof, pruned parts, `excluded_suppliers == 0`); q16
`strict=pass@compare`; boards level (17/17); confirmation prior rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (lying green): the COUNT-DISTINCT dedup,
  the anti-join exclusion (synthetic), the positional matcher, the mismatch/missing/dup/null/4-key-order tests, and
  the Phase-0 non-vacuity + schema pin are the defenses.
- **COUNT(DISTINCT)** — per-group `HashSet`; naive `COUNT(*)` over-counts (pin the dedup synthetic + the live proof).
- **Anti-join not live at sf=0.01** — synthetic-mandatory; the recorded `excluded_suppliers == 0` makes that honest.
- **Positional LIKE** — Customer-before-Complaints; pin the reject.

## Seal
`impl: adr 312 q16 correctness oracle`. Do not commit wip drafts.
