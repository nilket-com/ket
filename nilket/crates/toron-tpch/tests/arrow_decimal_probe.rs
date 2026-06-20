//! ADR 263 verify-first grounding (asserting): Arrow's Decimal128 kernels carry Q6's ops and pin the
//! precision/scale rules the binder must encode — comparison requires EXACT `(precision, scale)`,
//! `Decimal128(15,2) * Decimal128(15,2)` widens to `Decimal128(31,4)`, and `aggregate::sum` carries the
//! raw `i128` at the input scale (the caller reattaches `(p,s)` — what the Toron executor does). Pure
//! Arrow probe; the Toron integration is verified end-to-end via the harness gauge.

use arrow::array::{Array, ArrayRef, Decimal128Array, Scalar};
use arrow::compute::kernels::{aggregate, cmp, numeric};
use arrow::datatypes::DataType;

fn dec(values: Vec<i128>, precision: u8, scale: i8) -> Decimal128Array {
	Decimal128Array::from(values)
		.with_precision_and_scale(precision, scale)
		.expect("decimal column")
}

/// Comparison binds only when precision AND scale match exactly — the rule the binder's literal
/// coercion exists to satisfy (it rescales/retypes the literal to the column's `(p,s)`).
#[test]
fn arrow_decimal128_comparison_requires_exact_precision_and_scale() {
	// l_discount(15,2) = {0.06, 0.05, 0.08}
	let discount = dec(vec![6, 5, 8], 15, 2);

	// SAME (precision, scale): l_discount < 0.07 binds and is correct.
	let lit_same = Scalar::new(dec(vec![7], 15, 2));
	let lt_same = cmp::lt(&discount, &lit_same).expect("same-(p,s) comparison binds");
	let lt_same: Vec<bool> = (0..lt_same.len()).map(|i| lt_same.value(i)).collect();
	assert_eq!(
		lt_same,
		vec![true, true, false],
		"0.06<0.07, 0.05<0.07, 0.08<0.07"
	);

	// DIFFERENT precision (15,2 vs 3,2): Arrow errors — no implicit widening.
	let lit_diff_precision = Scalar::new(dec(vec![7], 3, 2));
	assert!(
		cmp::lt(&discount, &lit_diff_precision).is_err(),
		"Arrow cmp requires matching precision"
	);

	// DIFFERENT scale (2 vs 3): Arrow errors — this is why the binder must rescale literals.
	let lit_diff_scale = Scalar::new(dec(vec![70], 15, 3));
	assert!(
		cmp::lt(&discount, &lit_diff_scale).is_err(),
		"Arrow cmp requires matching scale"
	);
}

/// The binder-inserted Decimal comparison cast relies on Arrow's Decimal128-to-Decimal128 cast to widen
/// scale exactly. Scaling up multiplies the raw value and preserves NULLs. Overflow/range protection is
/// Toron's binder responsibility, pinned by `decimal_comparison_precision_overflow_rejects_at_bind`.
#[test]
fn arrow_decimal128_cast_widens_scale_exactly() {
	let source = Decimal128Array::from(vec![Some(12_345_i128), None])
		.with_precision_and_scale(15, 2)
		.expect("decimal source");
	let casted = arrow::compute::cast(&source, &DataType::Decimal128(20, 4))
		.expect("scale-widening decimal cast");
	assert_eq!(casted.data_type(), &DataType::Decimal128(20, 4));
	let casted = casted
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("casted decimal");
	assert_eq!(casted.value(0), 1_234_500, "123.45 -> 123.4500");
	assert!(casted.is_null(1), "NULL survives Decimal cast");
}

/// `*` widens to `Decimal128(31,4)` and `SUM` carries that type — the result-typing the binder encodes
/// (`p1+p2+1, s1+s2`) and the executor's SUM finalize reattaches.
#[test]
fn arrow_decimal128_mul_widens_and_sum_carries() {
	let price = dec(vec![10000, 20000, 30000], 15, 2); // 100.00, 200.00, 300.00
	let discount = dec(vec![6, 5, 8], 15, 2); // 0.06, 0.05, 0.08

	// (15,2)*(15,2) -> Decimal128(31,4): precision p1+p2+1, scale s1+s2.
	let product: ArrayRef = numeric::mul(&price, &discount).expect("decimal mul");
	assert_eq!(
		product.data_type(),
		&DataType::Decimal128(31, 4),
		"mul widens precision to p1+p2+1 and scale to s1+s2"
	);
	let product = product
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("product is Decimal128");
	let raw: Vec<i128> = (0..product.len()).map(|i| product.value(i)).collect();
	assert_eq!(
		raw,
		vec![60_000, 100_000, 240_000],
		"100.00*0.06, 200.00*0.05, 300.00*0.08 at scale 4"
	);

	// aggregate::sum carries the raw i128 at the product's scale; the caller reattaches (p,s) — exactly
	// what the Toron executor's SUM finalize does — preserving Decimal128(31,4).
	let summed = aggregate::sum(product).expect("sum over a non-empty column");
	assert_eq!(summed, 400_000, "60000+100000+240000 = 40.0000 at scale 4");
	assert_eq!(
		dec(vec![summed], 31, 4).data_type(),
		&DataType::Decimal128(31, 4),
		"the summed revenue carries the product's (precision, scale)"
	);
}

/// ADR 266: `+`/`-` on equal-`(p,s)` decimals carry the scale and add one precision digit for carry — the
/// rule the binder encodes (`Decimal(p,s) ± Decimal(p,s) → Decimal128(min(38, p+1), s)`). Unlike `*`, the
/// scale is unchanged. q01's `1 - l_discount` is this shape (after the literal coerces to the column's
/// `(15,2)`).
#[test]
fn arrow_decimal128_add_sub_carries_scale_plus_one_precision() {
	let a = dec(vec![10000, 5], 15, 2); // 100.00, 0.05
	let b = dec(vec![100, 5], 15, 2); //   1.00, 0.05

	let sum: ArrayRef = numeric::add(&a, &b).expect("decimal add");
	assert_eq!(
		sum.data_type(),
		&DataType::Decimal128(16, 2),
		"add carries scale, +1 precision for carry"
	);
	let sum = sum
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("sum is Decimal128");
	assert_eq!(
		(0..sum.len()).map(|i| sum.value(i)).collect::<Vec<_>>(),
		vec![10100, 10],
		"100.00+1.00=101.00, 0.05+0.05=0.10 at scale 2"
	);

	let diff: ArrayRef = numeric::sub(&a, &b).expect("decimal sub");
	assert_eq!(
		diff.data_type(),
		&DataType::Decimal128(16, 2),
		"sub carries scale, +1 precision"
	);
	let diff = diff
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("diff is Decimal128");
	assert_eq!(
		(0..diff.len()).map(|i| diff.value(i)).collect::<Vec<_>>(),
		vec![9900, 0],
		"100.00-1.00=99.00, 0.05-0.05=0.00 at scale 2"
	);
}
