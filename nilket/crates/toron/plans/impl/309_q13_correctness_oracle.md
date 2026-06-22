# ADR 309 — Implementation plan: q13 correctness oracle

Pairs with `plans/adr/309_q13_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine change.

## Phase 0 — confirm grounded facts
- Schema: `c_count` Int64, `custdist` Int64. Assert it (drift -> fail).
- Non-vacuity: measure sf=0.01 live counters and record them in the results doc:
  `total_customers`, `qualifying_orders`, `comment_excluded_orders`, `zero_count_customers`,
  `positive_count_customers`, `no_order_customers`, and `only_excluded_order_customers`.
- Accept the default scale only if `total_customers > 0`, `groups.len() > 1`, `zero_count_customers > 0`,
  `positive_count_customers > 0`, `qualifying_orders > 0`, and `comment_excluded_orders > 0`.
  `only_excluded_order_customers > 0` is the strongest live proof of the residual-created zero-count case; if it
  is 0, record that fact and keep the mandatory synthetic for that sub-case.

## D1 — `strict_q13_oracle` (`crates/toron-tpch/src/lib.rs`)
Implement the traversal:
```rust
pub struct Q13Oracle {
	pub groups: Vec<(i64, i64)>,
	pub total_customers: usize,
	pub qualifying_orders: usize,
	pub comment_excluded_orders: usize, // orders matching %special%requests% (residual exercised live)
	pub zero_count_customers: usize,
	pub positive_count_customers: usize,
	pub no_order_customers: usize,
	pub only_excluded_order_customers: usize,
}
```
- Define `q13_comment_matches_pattern(comment: &str) -> bool` to match `%special%requests%` sequentially:
	```rust
	if let Some(idx) = comment.find("special") {
		comment[idx + 7..].contains("requests")
	} else {
		false
	}
	```
- Collect the customer keys first. Traverse `OrderGenerator`: ignore orders whose `o_custkey` is not in the
  customer set, matching the left side of the SQL join. Track total orders by customer. If
  `!q13_comment_matches_pattern(&order.o_comment)`, increment count in `order_counts: HashMap<i64, i64>` by
  `order.o_custkey` and increment `qualifying_orders`; else increment `comment_excluded_orders`.
- Traverse `CustomerGenerator`: lookup order count for `customer.c_custkey` (defaulting to `0`). Increment
  customer count in `group_counts: BTreeMap<i64, i64>` keyed by `c_count`; populate zero/positive, no-order, and
  only-excluded counters.
- Sort `group_counts` by `custdist DESC, c_count DESC` and store in `groups` vector.

## D2 — `check_strict_q13` + `compare_strict_q13` + wiring
Mirror `check_strict_q07`:
1. `execute_query_with_tables(STRICT_Q13_SQL, q13_tables(true, sf))`.
2. `assert_strict_q13_schema(&batches)` — the 2 columns at `Int64/Int64`; drift -> `Compare`/`Fail`.
3. Non-vacuity: `oracle.total_customers > 0`, `oracle.groups.len() > 1`,
   `oracle.zero_count_customers > 0`, `oracle.positive_count_customers > 0`,
   `oracle.qualifying_orders > 0`, and `oracle.comment_excluded_orders > 0`.
4. `compare_strict_q13(&batches, &oracle)`:
	- extract emitted `(c_count, custdist)`; `is_null` on every cell -> `Err`;
	- **bijection by `c_count`**: unique; row counts equal; exact `c_count` and `custdist`;
	- **order**: emitted rows descending by `(custdist, c_count)`.
- `main.rs`: route `correctness --query q13` -> `check_strict_q13(sf)`.
- `correctness_matrix`: q13 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q13_matches_independent_oracle` - `pass@compare` at sf=0.01.
- `strict_q13_oracle_is_non_vacuous_at_default_scale` - total customers, multiple groups, zero-count customers,
  positive-count customers, qualifying orders, and comment-excluded orders.
- `strict_q13_output_schema_is_asserted` - accept `Int64/Int64`; reject drift.
- **`q13_comment_pattern_matching_is_exact`** - sequential matching: accept `"special requests"`, `"special other
  requests"`, `"specialrequests"`; **reject `"requests special"`**, bare `"special"`, bare `"requests"`.
- **`q13_outer_join_preservation_is_pinned`** - both null-extension sub-cases land in the `0` group: a customer
  with **no orders** AND a customer whose **only order is a `special…requests`** (residual + null-extension
  combine).
- `q13_count_o_orderkey_counts_only_qualifying_orders` - a customer with one qualifying and one excluded order
  has `c_count = 1`.
- `q13_correctness_mismatch_is_fail_at_compare`, `q13_missing_extra_or_duplicate_group_is_fail`, `q13_null_cell_is_fail`, `q13_order_violation_is_fail`.

## Seal gate
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q13
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/309_q13_correctness_oracle_results.md` detailing the schema, sf=0.01 non-vacuity, q13 `strict=pass@compare`, and matrix confirmations.
