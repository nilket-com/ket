//! ADR 261: TPC-H Reachability Harness — the library half (CLI lives in `main.rs`).
//!
//! Dev mode generates the harness `LINEITEM` subset from the pure-Rust `tpchgen` row structs,
//! converts `TPCHDecimal → Float64` and `TPCHDate → Date32` into workspace-Arrow, loads them through a
//! local [`StorageScan`], and runs Toron-authored reachability SQL single-node through `plan → bind →
//! execute`. A `pass` here means *reachability* (the shape ran and produced a digest), not TPC-H
//! correctness — dev data is synthetic and explicitly NOT a TPC-H result.

pub mod telemetry;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
	Array, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tpchgen::generators::{
	CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
	RegionGenerator, SupplierGenerator,
};

use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::distributed::budget::ByteCount;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::task::RowCount;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::TableId;
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

use telemetry::{Attempt, CorrectnessAttempt, Phase, Status, reason_for_error, status_for_reason};

/// Default cache / output root — always under `target/`, never a tracked path.
pub const CACHE_ROOT: &str = "target/tpch";

/// The 22 TPC-H query IDs (the reachability inventory).
pub const ALL_QUERY_IDS: [&str; 22] = [
	"q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10", "q11", "q12", "q13",
	"q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
];

/// The Toron-authored Q6 dev proxy. Two deliberate, visible departures from strict TPC-H: `Float64`
/// columns/literals replace `DECIMAL`, and the upper date bound is a literal date rather than
/// `DATE '1994-01-01' + INTERVAL '1' YEAR` (Toron's `Date32 ± Interval` is still narrow). NOT the
/// official Q6.
pub const Q6_DEV_SQL: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
	FROM lineitem \
	WHERE l_shipdate >= DATE '1994-01-01' \
	  AND l_shipdate <  DATE '1995-01-01' \
	  AND l_discount BETWEEN 0.05 AND 0.07 \
	  AND l_quantity < 24.0";

/// ADR 262: the strict Q6 query — the same Q6 *shape* with literal date bounds, run against the
/// official-*typed* `Decimal128(15,2)` fixture. The numeric literals (`0.05`, `0.07`, `24`) lower to
/// `Float64`/integer (Toron has no Decimal `ScalarValue`), so `l_discount BETWEEN 0.05 AND 0.07`
/// compares `Decimal128` against `Float64` — the measured bind-time blocker. NOT a TPC-H result.
pub const STRICT_Q6_SQL: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
	FROM lineitem \
	WHERE l_shipdate >= DATE '1994-01-01' \
	  AND l_shipdate <  DATE '1995-01-01' \
	  AND l_discount BETWEEN 0.05 AND 0.07 \
	  AND l_quantity < 24";

/// ADR 266 — dev Q1: the `Float64` proxy of TPC-H Q1's pricing summary. A literal cutoff date
/// (`DATE '1998-09-02'`) replaces the official `DATE '1998-12-01' - INTERVAL '90' DAY` so date arithmetic
/// doesn't mask the real wall. Full Q1 currently stops at PLAN on `ambiguous aggregate projection`
/// (multiple same-function aggregates — ADR 267), the same wall in dev and strict. The decimal `+`/`-`
/// this query also needs is verified separately by the focused decimal probe, not via this attempt.
pub const Q1_DEV_SQL: &str = "SELECT l_returnflag, l_linestatus, \
	SUM(l_quantity) AS sum_qty, \
	SUM(l_extendedprice) AS sum_base_price, \
	SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price, \
	SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge, \
	AVG(l_quantity) AS avg_qty, \
	AVG(l_extendedprice) AS avg_price, \
	AVG(l_discount) AS avg_disc, \
	COUNT(*) AS count_order \
	FROM lineitem \
	WHERE l_shipdate <= DATE '1998-09-02' \
	GROUP BY l_returnflag, l_linestatus \
	ORDER BY l_returnflag, l_linestatus";

/// strict Q1: the official-*typed* `Decimal128(15,2)` fixture + Q1 shape with the literal cutoff date.
/// Every arithmetic piece now binds and executes — `1 - l_discount` / `1 + l_tax` coerce to `Decimal128`
/// and `+`/`-` bind (266), `*` and `SUM` carry `Decimal128` (263), and `AVG(Decimal)` finalizes to a
/// widened `Decimal128(p+4,s+4)` (268) — so strict Q1 reaches execute. NOT a validated TPC-H result.
pub const STRICT_Q1_SQL: &str = "SELECT l_returnflag, l_linestatus, \
	SUM(l_quantity) AS sum_qty, \
	SUM(l_extendedprice) AS sum_base_price, \
	SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
	SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
	AVG(l_quantity) AS avg_qty, \
	AVG(l_extendedprice) AS avg_price, \
	AVG(l_discount) AS avg_disc, \
	COUNT(*) AS count_order \
	FROM lineitem \
	WHERE l_shipdate <= DATE '1998-09-02' \
	GROUP BY l_returnflag, l_linestatus \
	ORDER BY l_returnflag, l_linestatus";

/// ADR 269 — tpch.q14: the decimal ratio query, official-shaped (comma-join + literal date bounds). It
/// stacks a comma equi-join, a CASE over the `disc_price` chain, a Decimal-literal `*`, and a
/// Decimal ÷ Decimal — the reachability attempt measures whichever it walls on first. Same text in both
/// modes; only the fixture column types differ (the q01/q06 dev/strict discipline).
pub const Q14_DEV_SQL: &str = "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' \
		THEN l_extendedprice * (1 - l_discount) ELSE 0 END) \
	/ SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
	FROM lineitem, part \
	WHERE l_partkey = p_partkey \
	  AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'";

/// strict q14 — identical shape to the dev proxy; the `lineitem` fixture carries official
/// `Decimal128(15,2)` types instead of `Float64`.
pub const STRICT_Q14_SQL: &str = Q14_DEV_SQL;

/// Q3 Shipping Priority reachability SQL: tpchgen-2.0.2's Q3 text with the `:1`/`:2`
/// params bound to literals (`'BUILDING'`, `DATE '1995-03-15'`) and no `LIMIT` (the
/// bundled text carries none). Same text both modes; only `lineitem` types differ.
pub const Q3_DEV_SQL: &str = "SELECT l_orderkey, \
	SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
	o_orderdate, o_shippriority \
	FROM customer, orders, lineitem \
	WHERE c_mktsegment = 'BUILDING' \
	  AND c_custkey = o_custkey \
	  AND l_orderkey = o_orderkey \
	  AND o_orderdate < DATE '1995-03-15' \
	  AND l_shipdate > DATE '1995-03-15' \
	GROUP BY l_orderkey, o_orderdate, o_shippriority \
	ORDER BY revenue DESC, o_orderdate";

/// strict q03 — identical shape; the `lineitem` fixture carries official `Decimal128(15,2)` types.
pub const STRICT_Q3_SQL: &str = Q3_DEV_SQL;

/// ADR 274 — tpch.q10 (Returned Item Reporting): the first 4-table query, tpchgen-2.0.2's exact Q10 text with
/// `:1` → `DATE '1993-10-01'`, the interval upper bound literalized to `DATE '1994-01-01'`, and `ORDER BY
/// revenue DESC` only (no tiebreak, no LIMIT — the bundled text carries none). Groups by `c_acctbal`, a
/// numeric key. Same text both modes; only `lineitem`/`customer` types differ.
pub const Q10_DEV_SQL: &str = "SELECT c_custkey, c_name, \
	SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
	c_acctbal, n_name, c_address, c_phone, c_comment \
	FROM customer, orders, lineitem, nation \
	WHERE c_custkey = o_custkey \
	  AND l_orderkey = o_orderkey \
	  AND o_orderdate >= DATE '1993-10-01' \
	  AND o_orderdate < DATE '1994-01-01' \
	  AND l_returnflag = 'R' \
	  AND c_nationkey = n_nationkey \
	GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
	ORDER BY revenue DESC";

/// strict q10 — identical shape; `lineitem`/`customer` fixtures carry official `Decimal128(15,2)` types
/// (`c_acctbal` becomes a `Decimal128` GROUP BY key).
pub const STRICT_Q10_SQL: &str = Q10_DEV_SQL;

/// ADR 277 — tpch.q05 (Local Supplier Volume): the first 6-table query, tpchgen-2.0.2's exact Q5 with `:1` →
/// `'ASIA'`, `:2` → `'1994-01-01'`, the interval upper bound literalized to `DATE '1995-01-01'`, and `ORDER BY
/// revenue DESC` only (no LIMIT). The first cyclic join graph — the supplier edge carries multiple equalities
/// (`l_suppkey = s_suppkey` plus `c_nationkey = s_nationkey`). Same text both modes; only `lineitem` types
/// differ.
pub const Q5_DEV_SQL: &str = "SELECT n_name, \
	SUM(l_extendedprice * (1 - l_discount)) AS revenue \
	FROM customer, orders, lineitem, supplier, nation, region \
	WHERE c_custkey = o_custkey \
	  AND l_orderkey = o_orderkey \
	  AND l_suppkey = s_suppkey \
	  AND c_nationkey = s_nationkey \
	  AND s_nationkey = n_nationkey \
	  AND n_regionkey = r_regionkey \
	  AND r_name = 'ASIA' \
	  AND o_orderdate >= DATE '1994-01-01' \
	  AND o_orderdate < DATE '1995-01-01' \
	GROUP BY n_name \
	ORDER BY revenue DESC";

/// strict q05 — identical shape; the `lineitem` fixture carries official `Decimal128(15,2)` types.
pub const STRICT_Q5_SQL: &str = Q5_DEV_SQL;

/// The dev `LINEITEM` schema: the q01/q03/q06/q14 reachability subset. Money/quantity/discount/tax are
/// `Float64`, ship date is `Date32`, string flags stay `Utf8`, and join keys are `Int64`.
fn dev_lineitem_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("l_quantity", DataType::Float64, false),
		Field::new("l_extendedprice", DataType::Float64, false),
		Field::new("l_discount", DataType::Float64, false),
		Field::new("l_shipdate", DataType::Date32, false),
		// ADR 266: q01 columns (GROUP BY keys + tax) — Utf8 flags, Float64 tax in the dev proxy.
		Field::new("l_returnflag", DataType::Utf8, false),
		Field::new("l_linestatus", DataType::Utf8, false),
		Field::new("l_tax", DataType::Float64, false),
		// Append join keys after earlier query columns so q01/q06 positions and digests stay stable.
		Field::new("l_partkey", DataType::Int64, false),
		Field::new("l_orderkey", DataType::Int64, false),
		// ADR 277: q05 supplier join key, appended (existing queries don't reference it).
		Field::new("l_suppkey", DataType::Int64, false),
	]))
}

fn dev_lineitem_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	// Append-only (PK-less) identity needs the multi-row record layout explicitly.
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"lineitem",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("dev lineitem descriptor")
}

/// Generate the dev `LINEITEM` reachability subset at `scale_factor` from `tpchgen` rows into workspace-Arrow.
pub fn generate_dev_lineitem(scale_factor: f64) -> RecordBatch {
	let generator = LineItemGenerator::new(scale_factor, 1, 1);
	let mut quantity = Vec::new();
	let mut extendedprice = Vec::new();
	let mut discount = Vec::new();
	let mut shipdate = Vec::new();
	let mut returnflag = Vec::new();
	let mut linestatus = Vec::new();
	let mut tax = Vec::new();
	let mut partkey = Vec::new();
	let mut orderkey = Vec::new();
	let mut suppkey = Vec::new();
	for item in generator.iter() {
		quantity.push(item.l_quantity as f64);
		extendedprice.push(item.l_extendedprice.as_f64());
		discount.push(item.l_discount.as_f64());
		shipdate.push(item.l_shipdate.to_unix_epoch());
		returnflag.push(item.l_returnflag.to_string());
		linestatus.push(item.l_linestatus.to_string());
		tax.push(item.l_tax.as_f64());
		partkey.push(item.l_partkey);
		orderkey.push(item.l_orderkey);
		suppkey.push(item.l_suppkey);
	}
	RecordBatch::try_new(
		dev_lineitem_schema(),
		vec![
			Arc::new(Float64Array::from(quantity)),
			Arc::new(Float64Array::from(extendedprice)),
			Arc::new(Float64Array::from(discount)),
			Arc::new(Date32Array::from(shipdate)),
			Arc::new(StringArray::from(returnflag)),
			Arc::new(StringArray::from(linestatus)),
			Arc::new(Float64Array::from(tax)),
			Arc::new(Int64Array::from(partkey)),
			Arc::new(Int64Array::from(orderkey)),
			Arc::new(Int64Array::from(suppkey)),
		],
	)
	.expect("dev lineitem batch")
}

/// ADR 262: the strict `LINEITEM` schema — official physical types: `Decimal128(15,2)` for
/// money/quantity/discount, `Date32` for ship date. Same columns as dev, different *types* (the gap
/// this cut measures).
fn strict_lineitem_schema() -> SchemaRef {
	let decimal = |name: &str| Field::new(name, DataType::Decimal128(15, 2), false);
	Arc::new(Schema::new(vec![
		decimal("l_quantity"),
		decimal("l_extendedprice"),
		decimal("l_discount"),
		Field::new("l_shipdate", DataType::Date32, false),
		// ADR 266: q01 columns (GROUP BY keys + tax) — Utf8 flags, official Decimal128(15,2) tax.
		Field::new("l_returnflag", DataType::Utf8, false),
		Field::new("l_linestatus", DataType::Utf8, false),
		decimal("l_tax"),
		// Append join keys after earlier query columns so q01/q06 positions and digests stay stable.
		Field::new("l_partkey", DataType::Int64, false),
		Field::new("l_orderkey", DataType::Int64, false),
		// ADR 277: q05 supplier join key, appended (existing queries don't reference it).
		Field::new("l_suppkey", DataType::Int64, false),
	]))
}

fn strict_lineitem_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([2; 16]),
		1,
		"default",
		"lineitem",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("strict lineitem descriptor")
}

/// Generate the strict `LINEITEM` reachability subset at `scale_factor` as official `Decimal128(15,2)` Arrow.
/// `TPCHDecimal::into_inner()` is already the scale-2 raw value; whole-number quantity is scaled by 100.
pub fn generate_strict_lineitem(scale_factor: f64) -> RecordBatch {
	let generator = LineItemGenerator::new(scale_factor, 1, 1);
	let mut quantity = Vec::new();
	let mut extendedprice = Vec::new();
	let mut discount = Vec::new();
	let mut shipdate = Vec::new();
	let mut returnflag = Vec::new();
	let mut linestatus = Vec::new();
	let mut tax = Vec::new();
	let mut partkey = Vec::new();
	let mut orderkey = Vec::new();
	let mut suppkey = Vec::new();
	for item in generator.iter() {
		quantity.push(item.l_quantity as i128 * 100);
		extendedprice.push(item.l_extendedprice.into_inner() as i128);
		discount.push(item.l_discount.into_inner() as i128);
		shipdate.push(item.l_shipdate.to_unix_epoch());
		returnflag.push(item.l_returnflag.to_string());
		linestatus.push(item.l_linestatus.to_string());
		tax.push(item.l_tax.into_inner() as i128);
		partkey.push(item.l_partkey);
		orderkey.push(item.l_orderkey);
		suppkey.push(item.l_suppkey);
	}
	let decimal = |values: Vec<i128>| {
		Arc::new(
			Decimal128Array::from(values)
				.with_precision_and_scale(15, 2)
				.expect("decimal128 column"),
		) as arrow::array::ArrayRef
	};
	RecordBatch::try_new(
		strict_lineitem_schema(),
		vec![
			decimal(quantity),
			decimal(extendedprice),
			decimal(discount),
			Arc::new(Date32Array::from(shipdate)),
			Arc::new(StringArray::from(returnflag)),
			Arc::new(StringArray::from(linestatus)),
			decimal(tax),
			Arc::new(Int64Array::from(partkey)),
			Arc::new(Int64Array::from(orderkey)),
			Arc::new(Int64Array::from(suppkey)),
		],
	)
	.expect("strict lineitem batch")
}

/// The q14 `PART` fixture: the join key + the type string q14's CASE matches. Same in dev and strict (no
/// decimals). Generated from `tpchgen` so partkeys align with the generated `lineitem`.
fn part_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("p_partkey", DataType::Int64, false),
		Field::new("p_type", DataType::Utf8, false),
	]))
}

fn part_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([14; 16]),
		1,
		"default",
		"part",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("part descriptor")
}

/// Generate the q14-needed `PART` columns (`p_partkey`, `p_type`) at `scale_factor`.
pub fn generate_part(scale_factor: f64) -> RecordBatch {
	let generator = PartGenerator::new(scale_factor, 1, 1);
	let mut partkey = Vec::new();
	let mut part_type = Vec::new();
	for item in generator.iter() {
		partkey.push(item.p_partkey);
		part_type.push(item.p_type.to_string());
	}
	RecordBatch::try_new(
		part_schema(),
		vec![
			Arc::new(Int64Array::from(partkey)),
			Arc::new(StringArray::from(part_type)),
		],
	)
	.expect("part batch")
}

/// The `CUSTOMER` fixture for q03/q10. Mode-sensitive: `c_acctbal` is `Float64` (dev) / `Decimal128(15,2)`
/// (strict), like the `lineitem` money columns; every other column is mode-independent. q03 uses only
/// `c_custkey`/`c_mktsegment`; ADR 274 appended the rest for q10 (so q03's positions don't move).
fn customer_schema(strict: bool) -> SchemaRef {
	let acctbal = if strict {
		Field::new("c_acctbal", DataType::Decimal128(15, 2), false)
	} else {
		Field::new("c_acctbal", DataType::Float64, false)
	};
	Arc::new(Schema::new(vec![
		Field::new("c_custkey", DataType::Int64, false),
		Field::new("c_mktsegment", DataType::Utf8, false),
		// ADR 274: q10 columns, appended so q03's c_custkey/c_mktsegment positions don't move.
		Field::new("c_name", DataType::Utf8, false),
		acctbal,
		Field::new("c_address", DataType::Utf8, false),
		Field::new("c_phone", DataType::Utf8, false),
		Field::new("c_comment", DataType::Utf8, false),
		Field::new("c_nationkey", DataType::Int64, false),
	]))
}

fn customer_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([15; 16]),
		1,
		"default",
		"customer",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("customer descriptor")
}

/// Generate `CUSTOMER` at `scale_factor`. `c_acctbal` is `Float64` (dev) or scale-2 `Decimal128(15,2)`
/// (strict) — `TPCHDecimal::into_inner()` is the scale-2 raw, like the `lineitem` money columns.
pub fn generate_customer(strict: bool, scale_factor: f64) -> RecordBatch {
	let generator = CustomerGenerator::new(scale_factor, 1, 1);
	let mut custkey = Vec::new();
	let mut mktsegment = Vec::new();
	let mut name = Vec::new();
	let mut acctbal_dev = Vec::new();
	let mut acctbal_strict = Vec::new();
	let mut address = Vec::new();
	let mut phone = Vec::new();
	let mut comment = Vec::new();
	let mut nationkey = Vec::new();
	for item in generator.iter() {
		custkey.push(item.c_custkey);
		mktsegment.push(item.c_mktsegment.to_string());
		name.push(item.c_name.to_string());
		if strict {
			acctbal_strict.push(item.c_acctbal.into_inner() as i128);
		} else {
			acctbal_dev.push(item.c_acctbal.as_f64());
		}
		address.push(item.c_address.to_string());
		phone.push(item.c_phone.to_string());
		comment.push(item.c_comment.to_string());
		nationkey.push(item.c_nationkey);
	}
	let acctbal: arrow::array::ArrayRef = if strict {
		Arc::new(
			Decimal128Array::from(acctbal_strict)
				.with_precision_and_scale(15, 2)
				.expect("c_acctbal decimal column"),
		)
	} else {
		Arc::new(Float64Array::from(acctbal_dev))
	};
	RecordBatch::try_new(
		customer_schema(strict),
		vec![
			Arc::new(Int64Array::from(custkey)),
			Arc::new(StringArray::from(mktsegment)),
			Arc::new(StringArray::from(name)),
			acctbal,
			Arc::new(StringArray::from(address)),
			Arc::new(StringArray::from(phone)),
			Arc::new(StringArray::from(comment)),
			Arc::new(Int64Array::from(nationkey)),
		],
	)
	.expect("customer batch")
}

/// The q03 `ORDERS` fixture: the two join keys + the date filter + the group key. No decimals (q03 ignores
/// `o_totalprice`) → identical in dev and strict.
fn orders_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("o_orderkey", DataType::Int64, false),
		Field::new("o_custkey", DataType::Int64, false),
		Field::new("o_orderdate", DataType::Date32, false),
		Field::new("o_shippriority", DataType::Int32, false),
	]))
}

fn orders_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([16; 16]),
		1,
		"default",
		"orders",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("orders descriptor")
}

/// Generate the q03-needed `ORDERS` columns (`o_orderkey`, `o_custkey`, `o_orderdate`, `o_shippriority`) at
/// `scale_factor`. `o_orderdate` converts `TPCHDate → Date32` like `l_shipdate`.
pub fn generate_orders(scale_factor: f64) -> RecordBatch {
	let generator = OrderGenerator::new(scale_factor, 1, 1);
	let mut orderkey = Vec::new();
	let mut custkey = Vec::new();
	let mut orderdate = Vec::new();
	let mut shippriority = Vec::new();
	for item in generator.iter() {
		orderkey.push(item.o_orderkey);
		custkey.push(item.o_custkey);
		orderdate.push(item.o_orderdate.to_unix_epoch());
		shippriority.push(item.o_shippriority);
	}
	RecordBatch::try_new(
		orders_schema(),
		vec![
			Arc::new(Int64Array::from(orderkey)),
			Arc::new(Int64Array::from(custkey)),
			Arc::new(Date32Array::from(orderdate)),
			Arc::new(Int32Array::from(shippriority)),
		],
	)
	.expect("orders batch")
}

/// The q10 `NATION` fixture: the join key + the grouped name. A fixed 25-row lookup; no decimals → identical
/// in dev and strict (like `part`).
fn nation_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("n_nationkey", DataType::Int64, false),
		Field::new("n_name", DataType::Utf8, false),
		Field::new("n_regionkey", DataType::Int64, false),
	]))
}

fn nation_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([17; 16]),
		1,
		"default",
		"nation",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("nation descriptor")
}

/// Generate the q10-needed `NATION` columns (`n_nationkey`, `n_name`, `n_regionkey`).
pub fn generate_nation(scale_factor: f64) -> RecordBatch {
	let generator = NationGenerator::new(scale_factor, 1, 1);
	let mut nationkey = Vec::new();
	let mut name = Vec::new();
	let mut regionkey = Vec::new();
	for item in generator.iter() {
		nationkey.push(item.n_nationkey);
		name.push(item.n_name.to_string());
		regionkey.push(item.n_regionkey);
	}
	RecordBatch::try_new(
		nation_schema(),
		vec![
			Arc::new(Int64Array::from(nationkey)),
			Arc::new(StringArray::from(name)),
			Arc::new(Int64Array::from(regionkey)),
		],
	)
	.expect("nation batch")
}

/// The q05 `SUPPLIER` fixture: the two join keys q05 needs (`s_suppkey`, `s_nationkey`). No decimals → identical
/// in dev and strict (like `nation`/`part`; q05 ignores `s_acctbal`).
fn supplier_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("s_suppkey", DataType::Int64, false),
		Field::new("s_nationkey", DataType::Int64, false),
	]))
}

fn supplier_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([19; 16]),
		1,
		"default",
		"supplier",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("supplier descriptor")
}

/// Generate the q05-needed `SUPPLIER` columns (`s_suppkey`, `s_nationkey`) at `scale_factor`.
pub fn generate_supplier(scale_factor: f64) -> RecordBatch {
	let generator = SupplierGenerator::new(scale_factor, 1, 1);
	let mut suppkey = Vec::new();
	let mut nationkey = Vec::new();
	for item in generator.iter() {
		suppkey.push(item.s_suppkey);
		nationkey.push(item.s_nationkey);
	}
	RecordBatch::try_new(
		supplier_schema(),
		vec![
			Arc::new(Int64Array::from(suppkey)),
			Arc::new(Int64Array::from(nationkey)),
		],
	)
	.expect("supplier batch")
}

/// The q05 `REGION` fixture: the join key + the name filter column. A fixed 5-row lookup; no decimals →
/// identical in dev and strict.
fn region_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("r_regionkey", DataType::Int64, false),
		Field::new("r_name", DataType::Utf8, false),
	]))
}

fn region_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([21; 16]),
		1,
		"default",
		"region",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("region descriptor")
}

/// Generate the q05-needed `REGION` columns (`r_regionkey`, `r_name`).
pub fn generate_region(scale_factor: f64) -> RecordBatch {
	let generator = RegionGenerator::new(scale_factor, 1, 1);
	let mut regionkey = Vec::new();
	let mut name = Vec::new();
	for item in generator.iter() {
		regionkey.push(item.r_regionkey);
		name.push(item.r_name.to_string());
	}
	RecordBatch::try_new(
		region_schema(),
		vec![
			Arc::new(Int64Array::from(regionkey)),
			Arc::new(StringArray::from(name)),
		],
	)
	.expect("region batch")
}

/// Honest optimizer facts for a fixture batch: its real row count and in-memory byte size, so join
/// planning sees a true (tiny) build side rather than an unbounded one (ADR 269).
fn facts_for_batch(batch: &RecordBatch) -> TableOptimizerFacts {
	TableOptimizerFacts {
		row_upper_bound: Some(RowCount::new(batch.num_rows() as u64)),
		byte_upper_bound: Some(ByteCount::new(batch.get_array_memory_size() as u64)),
		..Default::default()
	}
}

/// The load seam: an in-memory [`StorageScan`] over generated batches, mirroring the test `FixtureScan`
/// (honor a pushed PBC predicate, then a projection). This is the one place the harness reaches into
/// Toron's storage trait.
pub struct HarnessScan {
	pub batches: HashMap<TableId, RecordBatch>,
}

#[async_trait]
impl StorageScan for HarnessScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;

		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}

		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}

		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}

/// Run a Q6 variant single-node over a pre-built fixture, recording the deepest phase reached. A stage
/// error is classified against the recognized-blocker allowlist: a recognized reason is `blocked`,
/// anything else is `fail`. Shared by the dev (Float64) and strict (Decimal128) runs.
/// A harness table: id, name (for fact seeding), descriptor, data, and optimizer facts.
pub struct HarnessTable {
	pub id: TableId,
	pub name: &'static str,
	pub descriptor: TableSchemaDescriptor,
	pub batch: RecordBatch,
	pub facts: TableOptimizerFacts,
}

/// One-table wrapper (q01/q06): registers `TableId(1)` with default optimizer facts, preserving the
/// original single-table behavior.
async fn run_query(
	query_id: &str,
	mode: &str,
	scale_factor: f64,
	sql: &str,
	batch: RecordBatch,
	descriptor: TableSchemaDescriptor,
) -> Attempt {
	run_query_with_tables(
		query_id,
		mode,
		scale_factor,
		sql,
		vec![HarnessTable {
			id: TableId(1),
			name: "lineitem",
			descriptor,
			batch,
			facts: TableOptimizerFacts::default(),
		}],
	)
	.await
}

/// A stage error from the shared execution path: the deepest phase reached and its classified reason.
struct StageError {
	phase: Phase,
	reason: String,
}

/// The shared Load→Plan→Bind→Execute substrate for both the reachability and correctness rails: register
/// every descriptor, seed its optimizer facts, assemble storage, then plan/bind/execute, returning the
/// result batches or the deepest-phase stage error. Reachability adds a digest; correctness adds the oracle
/// compare — neither forks the execution path (ADR 273).
async fn execute_query_with_tables(
	sql: &str,
	tables: Vec<HarnessTable>,
) -> Result<Vec<RecordBatch>, StageError> {
	let mut catalog = MockCatalog::new();
	let mut batches = HashMap::new();
	for table in tables {
		if let Err(error) = catalog.register_descriptor(table.id, table.descriptor) {
			return Err(StageError {
				phase: Phase::Load,
				reason: reason_for_error(&format!("{error:?}")).to_string(),
			});
		}
		if let Err(error) = catalog.seed_optimizer_facts(&TableRef::new(table.name), table.facts) {
			return Err(StageError {
				phase: Phase::Load,
				reason: reason_for_error(&format!("{error:?}")).to_string(),
			});
		}
		batches.insert(table.id, table.batch);
	}
	let storage = Arc::new(HarnessScan { batches });

	let logical = SqlPlanner::new().plan(sql).map_err(|error| StageError {
		phase: Phase::Plan,
		reason: reason_for_error(&error.to_string()).to_string(),
	})?;
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.map_err(|error| StageError {
			phase: Phase::Bind,
			reason: reason_for_error(&format!("{error:?}")).to_string(),
		})?;
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.map_err(|error| StageError {
			phase: Phase::Execute,
			reason: reason_for_error(&format!("{error:?}")).to_string(),
		})
}

/// Run a query single-node over N pre-built fixtures (the reachability rail): the shared execute path, then a
/// canonical digest. A stage error is classified against the recognized-blocker allowlist.
async fn run_query_with_tables(
	query_id: &str,
	mode: &str,
	scale_factor: f64,
	sql: &str,
	tables: Vec<HarnessTable>,
) -> Attempt {
	let start = Instant::now();
	let attempt =
		|phase: Phase, status: Status, reason: Option<String>, digest: Option<String>| Attempt {
			query: query_id.to_string(),
			scale_factor,
			mode: mode.to_string(),
			phase,
			status,
			reason,
			elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
			digest,
		};

	let batches = match execute_query_with_tables(sql, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason }) => {
			return attempt(phase, status_for_reason(&reason), Some(reason), None);
		}
	};

	let digest = match canonical_arrow_ipc_digest(&batches, SortPolicy::FullRowLex) {
		Ok(digest) => format!("{digest:?}"),
		Err(error) => {
			let reason = reason_for_error(&format!("{error:?}"));
			return attempt(
				Phase::Execute,
				status_for_reason(reason),
				Some(reason.to_string()),
				None,
			);
		}
	};

	attempt(Phase::Execute, Status::Pass, None, Some(digest))
}

/// Dev Q6: the `Float64` proxy. A `pass` means the shape reached `execute` (reachability), not TPC-H
/// correctness.
pub async fn run_dev_q6(scale_factor: f64) -> Attempt {
	let schema = dev_lineitem_schema();
	run_query(
		"q06",
		"dev",
		scale_factor,
		Q6_DEV_SQL,
		generate_dev_lineitem(scale_factor),
		dev_lineitem_descriptor(schema.as_ref()),
	)
	.await
}

/// ADR 266 — dev Q1: the `Float64` proxy reaching execute (reachability, not TPC-H correctness).
pub async fn run_dev_q1(scale_factor: f64) -> Attempt {
	let schema = dev_lineitem_schema();
	run_query(
		"q01",
		"dev",
		scale_factor,
		Q1_DEV_SQL,
		generate_dev_lineitem(scale_factor),
		dev_lineitem_descriptor(schema.as_ref()),
	)
	.await
}

/// strict Q1: official-typed `Decimal128(15,2)` + Q1 shape. Reaches execute end-to-end — ADR 267 cleared
/// the multiple-same-function aggregate projection wall, and ADRs 263/266/268 cover the Decimal `*`/`SUM`,
/// `+`/`-`, and `AVG` arithmetic. The focused decimal probes pin the individual arithmetic slices.
pub async fn run_strict_q1(scale_factor: f64) -> Attempt {
	let schema = strict_lineitem_schema();
	run_query(
		"q01",
		"strict",
		scale_factor,
		STRICT_Q1_SQL,
		generate_strict_lineitem(scale_factor),
		strict_lineitem_descriptor(schema.as_ref()),
	)
	.await
}

/// ADR 263 — Strict Q6: the official-*typed* `Decimal128(15,2)` fixture + Q6 shape. Grounded result:
/// `phase=execute status=pass` — the Decimal ScalarValue slice coerces the comparison literals at bind,
/// widens `*` to `Decimal128(31,4)`, and carries `SUM` through. "strict" here = official physical types
/// + shape reaching execute, NOT official provenance or answer correctness — NOT a TPC-H result.
pub async fn run_strict_q6(scale_factor: f64) -> Attempt {
	let schema = strict_lineitem_schema();
	run_query(
		"q06",
		"strict",
		scale_factor,
		STRICT_Q6_SQL,
		generate_strict_lineitem(scale_factor),
		strict_lineitem_descriptor(schema.as_ref()),
	)
	.await
}

/// ADR 273: the independent strict-q06 oracle — `Σ (l_extendedprice_raw × l_discount_raw)` (scale-4 i128)
/// over the rows the strict-q06 filters select, by traversing `LineItemGenerator` directly (NOT Toron's
/// loaded batch). Returns the expected raw sum (`None` = `SUM(empty)` → NULL) and the matched-row count (a
/// zero count would let a threshold bug masquerade as a vacuous pass). Raw `Decimal128(15,2)` thresholds:
/// discount `0.05..=0.07` = `5..=7`; quantity `< 24` = `*100 < 2400`; shipdate `[1994-01-01, 1995-01-01)` =
/// `Date32` day counts `8766..9131`.
pub fn strict_q6_oracle(scale_factor: f64) -> (Option<i128>, usize) {
	let generator = LineItemGenerator::new(scale_factor, 1, 1);
	let mut sum: i128 = 0;
	let mut matched = 0usize;
	for item in generator.iter() {
		if let Some(revenue) = q6_row_revenue(
			item.l_extendedprice.into_inner() as i128,
			item.l_discount.into_inner() as i128,
			item.l_quantity as i128 * 100,
			item.l_shipdate.to_unix_epoch(),
		) {
			sum += revenue;
			matched += 1;
		}
	}
	if matched == 0 {
		(None, 0)
	} else {
		(Some(sum), matched)
	}
}

/// ADR 273: the per-row q06 contribution — `Some(extendedprice_raw × discount_raw)` (scale 4) when the row
/// matches the strict-q06 filters, else `None`. Split out so the scaled-`i128` arithmetic and the raw bounds
/// (discount `5..=7`, quantity `*100 < 2400`, shipdate day-count `8766..9131`) are testable on crafted values.
pub fn q6_row_revenue(
	extendedprice_raw: i128,
	discount_raw: i128,
	quantity_scaled: i128,
	shipdate_days: i32,
) -> Option<i128> {
	if (5..=7).contains(&discount_raw)
		&& quantity_scaled < 2400
		&& (8766..9131).contains(&shipdate_days)
	{
		Some(extendedprice_raw * discount_raw)
	} else {
		None
	}
}

/// Compare Toron's strict-q06 executor output to the oracle's raw scale-4 `i128`. Requires one column, ≤ 1
/// row, and exact type `Decimal128(31,4)`; compares the raw value and nullness (never a formatted string, so a
/// future Decimal-widening change can't silently pass). `Ok(())` = match; `Err(reason)` = mismatch.
pub fn compare_strict_q6(batches: &[RecordBatch], expected: Option<i128>) -> Result<(), String> {
	let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
	if total_rows > 1 {
		return Err(format!("expected <= 1 output row, got {total_rows}"));
	}
	let Some(batch) = batches.iter().find(|batch| batch.num_rows() == 1) else {
		// No output row — treat as SUM(empty) = NULL.
		return match expected {
			None => Ok(()),
			Some(value) => Err(format!("expected raw {value}, got an empty result")),
		};
	};
	if batch.num_columns() != 1 {
		return Err(format!(
			"expected 1 output column, got {}",
			batch.num_columns()
		));
	}
	let column = batch.column(0);
	if column.data_type() != &DataType::Decimal128(31, 4) {
		return Err(format!(
			"expected Decimal128(31,4), got {:?}",
			column.data_type()
		));
	}
	let array = column
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.ok_or_else(|| "output column is not Decimal128".to_string())?;
	let actual = if array.is_null(0) {
		None
	} else {
		Some(array.value(0))
	};
	if actual == expected {
		Ok(())
	} else {
		Err(format!(
			"raw mismatch: expected {expected:?}, got {actual:?}"
		))
	}
}

/// ADR 273 — strict q06 correctness check: execute strict q06 over the shared substrate, then compare the
/// single `Decimal128(31,4)` output against the independent oracle. A mismatch is `fail@compare` (no "wrong
/// answer" blocker); a query that can't plan/bind/execute carries its deepest phase + reachability-style
/// reason. The oracle must select ≥ 1 row (else a vacuous `0 == 0` would mask a threshold bug).
pub async fn check_strict_q6(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let lineitem = generate_strict_lineitem(scale_factor);
	let tables = vec![HarnessTable {
		id: TableId(1),
		name: "lineitem",
		descriptor: strict_lineitem_descriptor(strict_lineitem_schema().as_ref()),
		facts: facts_for_batch(&lineitem),
		batch: lineitem,
	}];
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q06".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q6_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	let (expected, matched) = strict_q6_oracle(scale_factor);
	if matched == 0 {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some("oracle matched zero rows at this scale".to_string()),
		);
	}
	match compare_strict_q6(&batches, expected) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

/// ADR 276: one strict-q01 group's raw `i128` accumulators (the SUM columns) + the `i64` row count. The AVG
/// columns are finalized from these via [`avg_decimal_raw`]; `sum_discount` feeds `avg_disc` (q01 has no
/// `SUM(discount)` output column of its own).
#[derive(Default)]
pub struct Q1Group {
	pub sum_qty: i128,
	pub sum_base_price: i128,
	pub sum_disc_price: i128,
	pub sum_charge: i128,
	pub sum_discount: i128,
	pub count: i64,
}

/// ADR 276: the independent strict-q01 oracle — traverse `LineItemGenerator` directly, filter
/// `l_shipdate <= DATE '1998-09-02'` (day-count 10471), and accumulate raw scale-correct `i128` per
/// `(l_returnflag, l_linestatus)` group. The `BTreeMap` is sorted, matching q01's `ORDER BY` ASC/ASC.
/// Quantity raws are `l_quantity * 100` (scale 2), mirroring the strict fixture; `disc_price` is scale 4
/// (`extprice * (1 - disc)`) and `charge` is scale 6 (`disc_price * (1 + tax)`).
pub fn strict_q1_oracle(scale_factor: f64) -> BTreeMap<(String, String), Q1Group> {
	let generator = LineItemGenerator::new(scale_factor, 1, 1);
	let mut groups: BTreeMap<(String, String), Q1Group> = BTreeMap::new();
	for item in generator.iter() {
		if item.l_shipdate.to_unix_epoch() > 10471 {
			continue;
		}
		let qty_raw = item.l_quantity as i128 * 100;
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let disc_raw = item.l_discount.into_inner() as i128;
		let tax_raw = item.l_tax.into_inner() as i128;
		let group = groups
			.entry((item.l_returnflag.to_string(), item.l_linestatus.to_string()))
			.or_default();
		group.sum_qty += qty_raw;
		group.sum_base_price += extprice_raw;
		group.sum_discount += disc_raw;
		group.sum_disc_price += extprice_raw * (100 - disc_raw);
		group.sum_charge += extprice_raw * (100 - disc_raw) * (100 + tax_raw);
		group.count += 1;
	}
	groups
}

/// Mirror `exec.rs::decimal_fits_precision`: a `Decimal128(precision, _)` value must satisfy
/// `|value| < 10^precision`.
fn decimal_fits_precision(value: i128, precision: u8) -> bool {
	match 10_i128.checked_pow(u32::from(precision)) {
		Some(limit) => value.unsigned_abs() < limit.unsigned_abs(),
		None => true,
	}
}

/// ADR 276: replicate `exec.rs::avg_decimal_finalize` bit-for-bit for the q01 averages — rescale the raw sum
/// to the output scale, divide by the count, round **half away from zero** (`scaled.signum()` direction), and
/// enforce the output precision fit (the executor's final guard). Fallible like the executor's finalize: a
/// zero count, an output scale below the input scale, checked-arithmetic overflow, or a result that does not
/// fit `out_precision` returns `Err` (→ `fail@compare`) rather than a harness panic.
pub fn avg_decimal_raw(
	total: i128,
	count: i64,
	input_scale: i8,
	out_scale: i8,
	out_precision: u8,
) -> Result<i128, String> {
	if count == 0 {
		return Err("avg over zero rows".to_string());
	}
	let scale_delta = i32::from(out_scale) - i32::from(input_scale);
	if scale_delta < 0 {
		return Err("avg output scale below input scale".to_string());
	}
	let multiplier = 10_i128
		.checked_pow(scale_delta as u32)
		.ok_or_else(|| "avg scale overflow".to_string())?;
	let scaled = total
		.checked_mul(multiplier)
		.ok_or_else(|| "avg scale overflow".to_string())?;
	let count = i128::from(count);
	let mut quotient = scaled / count;
	let remainder = scaled % count;
	if remainder.unsigned_abs() * 2 >= count.unsigned_abs() {
		quotient = quotient
			.checked_add(scaled.signum())
			.ok_or_else(|| "avg rounding overflow".to_string())?;
	}
	if !decimal_fits_precision(quotient, out_precision) {
		return Err(format!(
			"avg result {quotient} does not fit output precision {out_precision}"
		));
	}
	Ok(quotient)
}

/// ADR 276: the strict-q01 output schema (name, exact type) — asserted before any value comparison so a
/// silent scale drift can't make the oracle compare at the wrong place. Read from the executor's actual
/// output, not derived (`sum_disc_price` is `Decimal128(32,4)`, not the `(31,4)` an on-paper derivation gives).
fn strict_q1_expected_schema() -> [(&'static str, DataType); 10] {
	[
		("l_returnflag", DataType::Utf8),
		("l_linestatus", DataType::Utf8),
		("sum_qty", DataType::Decimal128(15, 2)),
		("sum_base_price", DataType::Decimal128(15, 2)),
		("sum_disc_price", DataType::Decimal128(32, 4)),
		("sum_charge", DataType::Decimal128(38, 6)),
		("avg_qty", DataType::Decimal128(19, 6)),
		("avg_price", DataType::Decimal128(19, 6)),
		("avg_disc", DataType::Decimal128(19, 6)),
		("count_order", DataType::Int64),
	]
}

/// Assert strict q01's actual output schema matches [`strict_q1_expected_schema`] (column count, names, exact
/// `(p,s)`). A drift is a compare failure — never a wrong-scale value comparison.
pub fn assert_strict_q1_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q1_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q01 produced no output batch".to_string());
	};
	let schema = first.schema();
	if schema.fields().len() != expected.len() {
		return Err(format!(
			"expected {} columns, got {}",
			expected.len(),
			schema.fields().len()
		));
	}
	for (idx, (name, data_type)) in expected.iter().enumerate() {
		let field = schema.field(idx);
		if field.name() != name {
			return Err(format!(
				"column {idx}: expected name {name:?}, got {:?}",
				field.name()
			));
		}
		if field.data_type() != data_type {
			return Err(format!(
				"column {idx} ({name}): expected {data_type:?}, got {:?}",
				field.data_type()
			));
		}
	}
	Ok(())
}

/// Compare strict q01's output to the independent oracle: assert the schema, then align output rows to the
/// sorted oracle groups by key (verifying the ASC/ASC order), and assert each cell's raw value exactly (sums
/// direct; averages via [`avg_decimal_raw`]; `count_order` as `i64`). Any mismatch → `Err` (`fail@compare`).
pub fn compare_strict_q1(
	batches: &[RecordBatch],
	oracle: &BTreeMap<(String, String), Q1Group>,
) -> Result<(), String> {
	assert_strict_q1_schema(batches)?;

	struct OutRow {
		returnflag: String,
		linestatus: String,
		aggregates: [i128; 7],
		count: i64,
	}
	let mut rows: Vec<OutRow> = Vec::new();
	for batch in batches {
		let utf8 = |idx: usize| -> Result<&StringArray, String> {
			batch
				.column(idx)
				.as_any()
				.downcast_ref::<StringArray>()
				.ok_or_else(|| format!("column {idx} is not Utf8"))
		};
		let dec = |idx: usize| -> Result<&Decimal128Array, String> {
			batch
				.column(idx)
				.as_any()
				.downcast_ref::<Decimal128Array>()
				.ok_or_else(|| format!("column {idx} is not Decimal128"))
		};
		let returnflag = utf8(0)?;
		let linestatus = utf8(1)?;
		let decimals = [
			dec(2)?,
			dec(3)?,
			dec(4)?,
			dec(5)?,
			dec(6)?,
			dec(7)?,
			dec(8)?,
		];
		let count = batch
			.column(9)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or_else(|| "column 9 (count_order) is not Int64".to_string())?;
		for row in 0..batch.num_rows() {
			// Every q01 output cell over a non-empty group is non-null; a NULL key/aggregate/count is an
			// anomaly, not a value to compare through its (garbage) slot — return it as fail@compare.
			let schema = strict_q1_expected_schema();
			if returnflag.is_null(row) {
				return Err(format!(
					"column {} is unexpectedly NULL at row {row}",
					schema[0].0
				));
			}
			if linestatus.is_null(row) {
				return Err(format!(
					"column {} is unexpectedly NULL at row {row}",
					schema[1].0
				));
			}
			for (idx, decimal) in decimals.iter().enumerate() {
				if decimal.is_null(row) {
					return Err(format!(
						"column {} is unexpectedly NULL at row {row}",
						schema[idx + 2].0
					));
				}
			}
			if count.is_null(row) {
				return Err(format!(
					"column {} is unexpectedly NULL at row {row}",
					schema[9].0
				));
			}
			rows.push(OutRow {
				returnflag: returnflag.value(row).to_string(),
				linestatus: linestatus.value(row).to_string(),
				aggregates: std::array::from_fn(|i| decimals[i].value(row)),
				count: count.value(row),
			});
		}
	}

	if rows.len() != oracle.len() {
		return Err(format!(
			"expected {} groups, got {} output rows",
			oracle.len(),
			rows.len()
		));
	}
	for (row, ((returnflag, linestatus), group)) in rows.iter().zip(oracle.iter()) {
		if &row.returnflag != returnflag || &row.linestatus != linestatus {
			return Err(format!(
				"row order/key mismatch: output ({:?},{:?}) vs oracle ({returnflag:?},{linestatus:?})",
				row.returnflag, row.linestatus
			));
		}
		let expected = [
			("sum_qty", group.sum_qty),
			("sum_base_price", group.sum_base_price),
			("sum_disc_price", group.sum_disc_price),
			("sum_charge", group.sum_charge),
			(
				"avg_qty",
				avg_decimal_raw(group.sum_qty, group.count, 2, 6, 19)?,
			),
			(
				"avg_price",
				avg_decimal_raw(group.sum_base_price, group.count, 2, 6, 19)?,
			),
			(
				"avg_disc",
				avg_decimal_raw(group.sum_discount, group.count, 2, 6, 19)?,
			),
		];
		for (idx, (column, want)) in expected.iter().enumerate() {
			if row.aggregates[idx] != *want {
				return Err(format!(
					"group ({returnflag:?},{linestatus:?}) {column}: expected {want}, got {}",
					row.aggregates[idx]
				));
			}
		}
		if row.count != group.count {
			return Err(format!(
				"group ({returnflag:?},{linestatus:?}) count_order: expected {}, got {}",
				group.count, row.count
			));
		}
	}
	Ok(())
}

/// ADR 276 — the strict q01 correctness check: execute strict q01, assert its schema, guard non-vacuity
/// (≥ 1 selected input row AND > 1 group — genuinely multi-row), then compare every cell to the independent
/// oracle. A mismatch is `fail@compare`; a query that can't execute carries its deepest phase + reason.
pub async fn check_strict_q1(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let lineitem = generate_strict_lineitem(scale_factor);
	let tables = vec![HarnessTable {
		id: TableId(1),
		name: "lineitem",
		descriptor: strict_lineitem_descriptor(strict_lineitem_schema().as_ref()),
		facts: facts_for_batch(&lineitem),
		batch: lineitem,
	}];
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q01".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q1_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	let oracle = strict_q1_oracle(scale_factor);
	let selected_rows: i64 = oracle.values().map(|group| group.count).sum();
	if selected_rows < 1 || oracle.len() <= 1 {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!(
				"oracle non-vacuous guard failed: selected_rows={selected_rows} groups={}",
				oracle.len()
			)),
		);
	}
	match compare_strict_q1(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

/// Validate an explicit `--sf` (scale-factor) value: it must be a **finite, strictly-positive** number.
/// Malformed (unparseable), zero, negative, NaN, and infinite values are *usage* errors — the CLI maps the
/// `Err` to a harness error (exit 2, no telemetry row), per ADR 261's "a bad arg is a harness error, not a
/// query `blocked`/`fail`" boundary. Keeps degenerate scales out of `reachability.jsonl`.
pub fn parse_scale_factor(raw: &str) -> Result<f64, String> {
	match raw.parse::<f64>() {
		Ok(sf) if sf.is_finite() && sf > 0.0 => Ok(sf),
		_ => Err(format!(
			"--sf must be a finite, positive number (got {raw:?})"
		)),
	}
}

/// ADR 269 — dev q14: the decimal ratio query over the `Float64` proxy `lineitem` + `part`. A reachability
/// attempt — records whichever wall q14 hits first.
pub async fn run_dev_q14(scale_factor: f64) -> Attempt {
	let lineitem = generate_dev_lineitem(scale_factor);
	let part = generate_part(scale_factor);
	run_query_with_tables(
		"q14",
		"dev",
		scale_factor,
		Q14_DEV_SQL,
		vec![
			HarnessTable {
				id: TableId(1),
				name: "lineitem",
				descriptor: dev_lineitem_descriptor(dev_lineitem_schema().as_ref()),
				facts: facts_for_batch(&lineitem),
				batch: lineitem,
			},
			HarnessTable {
				id: TableId(2),
				name: "part",
				descriptor: part_descriptor(part_schema().as_ref()),
				facts: facts_for_batch(&part),
				batch: part,
			},
		],
	)
	.await
}

/// ADR 269 — strict q14: the same shape with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q14(scale_factor: f64) -> Attempt {
	let lineitem = generate_strict_lineitem(scale_factor);
	let part = generate_part(scale_factor);
	run_query_with_tables(
		"q14",
		"strict",
		scale_factor,
		STRICT_Q14_SQL,
		vec![
			HarnessTable {
				id: TableId(1),
				name: "lineitem",
				descriptor: strict_lineitem_descriptor(strict_lineitem_schema().as_ref()),
				facts: facts_for_batch(&lineitem),
				batch: lineitem,
			},
			HarnessTable {
				id: TableId(2),
				name: "part",
				descriptor: part_descriptor(part_schema().as_ref()),
				facts: facts_for_batch(&part),
				batch: part,
			},
		],
	)
	.await
}

/// Run q03 over the `Float64` proxy `lineitem`.
pub async fn run_dev_q3(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q03",
		"dev",
		scale_factor,
		Q3_DEV_SQL,
		q03_tables(false, scale_factor),
	)
	.await
}

/// Run q03 with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q3(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q03",
		"strict",
		scale_factor,
		STRICT_Q3_SQL,
		q03_tables(true, scale_factor),
	)
	.await
}

/// The three q03 harness tables. `lineitem` and `customer` are mode-sensitive (`Float64` vs `Decimal128` —
/// customer for `c_acctbal`, ADR 274); `orders` carries no decimals and is identical in both modes.
fn q03_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let customer = generate_customer(strict, scale_factor);
	let orders = generate_orders(scale_factor);
	let lineitem_descriptor = if strict {
		strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
	} else {
		dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
	};
	vec![
		HarnessTable {
			id: TableId(1),
			name: "lineitem",
			descriptor: lineitem_descriptor,
			facts: facts_for_batch(&lineitem),
			batch: lineitem,
		},
		HarnessTable {
			id: TableId(2),
			name: "customer",
			descriptor: customer_descriptor(customer_schema(strict).as_ref()),
			facts: facts_for_batch(&customer),
			batch: customer,
		},
		HarnessTable {
			id: TableId(3),
			name: "orders",
			descriptor: orders_descriptor(orders_schema().as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
	]
}

/// ADR 274 — dev q10: the first 4-table query (`customer`/`orders`/`lineitem`/`nation`). A reachability
/// attempt — records whichever wall q10 hits first.
pub async fn run_dev_q10(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q10",
		"dev",
		scale_factor,
		Q10_DEV_SQL,
		q10_tables(false, scale_factor),
	)
	.await
}

/// ADR 274 — strict q10: the same shape with official `Decimal128(15,2)` types — `c_acctbal` becomes a
/// `Decimal128` GROUP BY key (the candidate wall).
pub async fn run_strict_q10(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q10",
		"strict",
		scale_factor,
		STRICT_Q10_SQL,
		q10_tables(true, scale_factor),
	)
	.await
}

/// The four q10 harness tables: q03's lineitem/customer/orders + `nation`.
fn q10_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let mut tables = q03_tables(strict, scale_factor);
	let nation = generate_nation(scale_factor);
	tables.push(HarnessTable {
		id: TableId(4),
		name: "nation",
		descriptor: nation_descriptor(nation_schema().as_ref()),
		facts: facts_for_batch(&nation),
		batch: nation,
	});
	tables
}

/// ADR 277 — dev q05: the first 6-table query (customer/orders/lineitem/supplier/nation/region). A
/// reachability attempt — records whichever wall q05 hits first.
pub async fn run_dev_q05(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q05",
		"dev",
		scale_factor,
		Q5_DEV_SQL,
		q05_tables(false, scale_factor),
	)
	.await
}

/// ADR 277 — strict q05: the same shape with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q05(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q05",
		"strict",
		scale_factor,
		STRICT_Q5_SQL,
		q05_tables(true, scale_factor),
	)
	.await
}

/// The six q05 harness tables: q10's lineitem/customer/orders/nation + `supplier` + `region`.
fn q05_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let mut tables = q10_tables(strict, scale_factor);
	let supplier = generate_supplier(scale_factor);
	tables.push(HarnessTable {
		id: TableId(5),
		name: "supplier",
		descriptor: supplier_descriptor(supplier_schema().as_ref()),
		facts: facts_for_batch(&supplier),
		batch: supplier,
	});
	let region = generate_region(scale_factor);
	tables.push(HarnessTable {
		id: TableId(6),
		name: "region",
		descriptor: region_descriptor(region_schema().as_ref()),
		facts: facts_for_batch(&region),
		batch: region,
	});
	tables
}

/// The 22-query reachability *inventory* — static capability (does the harness implement an attempt?),
/// NOT the last measured result. Measured `blocked`/`fail`/`pass` rows come only from the JSONL-backed
/// [`matrix`].
pub fn status_rows() -> Vec<String> {
	ALL_QUERY_IDS
		.iter()
		.map(|id| {
			let state =
				if *id == "q06"
					|| *id == "q01" || *id == "q14"
					|| *id == "q03" || *id == "q10"
					|| *id == "q05"
				{
					"implemented"
				} else {
					"not_attempted"
				};
			format!("{id} dev={state} strict={state}")
		})
		.collect()
}

/// Regenerate the query × mode reachability matrix from `target/tpch/reachability.jsonl` — the latest
/// recorded outcome per (query, mode). This is the only place measured `blocked`/`fail` rows live (the
/// `status` inventory never encodes a measured result). An absent/unreadable log → the static inventory.
pub fn matrix() -> Vec<String> {
	let log = format!("{CACHE_ROOT}/reachability.jsonl");
	let contents = std::fs::read_to_string(&log).unwrap_or_default();
	if contents.trim().is_empty() {
		return status_rows();
	}
	matrix_from_log(&contents)
}

/// The pure matrix renderer over a `reachability.jsonl` body — the latest outcome per (query, mode).
/// Split out so it is testable without the shared cache file.
pub fn matrix_from_log(contents: &str) -> Vec<String> {
	let mut latest: HashMap<(String, String), String> = HashMap::new();
	for line in contents.lines() {
		let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
			continue;
		};
		let query = row["query"].as_str().unwrap_or("?").to_string();
		let mode = row["mode"].as_str().unwrap_or("?").to_string();
		let phase = row["phase"].as_str().unwrap_or("?");
		let status = row["status"].as_str().unwrap_or("?");
		let cell = match status {
			"blocked" | "fail" => {
				format!(
					"{status}({}@{phase})",
					row["reason"].as_str().unwrap_or("?")
				)
			}
			_ => format!("{status}@{phase}"),
		};
		latest.insert((query, mode), cell);
	}
	ALL_QUERY_IDS
		.iter()
		.map(|id| {
			let cell = |mode: &str| {
				latest
					.get(&(id.to_string(), mode.to_string()))
					.cloned()
					.unwrap_or_else(|| "not_run".to_string())
			};
			format!("{id} dev={} strict={}", cell("dev"), cell("strict"))
		})
		.collect()
}

/// ADR 273: regenerate the strict correctness matrix from `target/tpch/correctness.jsonl` — a rail separate
/// from the reachability matrix. The latest answer-check per query; an unchecked query renders `unchecked`.
pub fn correctness_matrix() -> Vec<String> {
	let log = format!("{CACHE_ROOT}/correctness.jsonl");
	let contents = std::fs::read_to_string(&log).unwrap_or_default();
	correctness_matrix_from_log(&contents)
}

/// The pure correctness-matrix renderer over a `correctness.jsonl` body (strict-only). Split out so it is
/// testable without the shared cache file, and so a reachability row cannot leak in (the `suite` is checked).
pub fn correctness_matrix_from_log(contents: &str) -> Vec<String> {
	let mut latest: HashMap<String, String> = HashMap::new();
	for line in contents.lines() {
		let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
			continue;
		};
		if row["suite"].as_str() != Some("tpch_correctness") {
			continue;
		}
		let query = row["query"].as_str().unwrap_or("?").to_string();
		let phase = row["phase"].as_str().unwrap_or("?");
		let status = row["status"].as_str().unwrap_or("?");
		let cell = match status {
			"fail" => format!("fail({}@{phase})", row["reason"].as_str().unwrap_or("?")),
			_ => format!("{status}@{phase}"),
		};
		latest.insert(query, cell);
	}
	ALL_QUERY_IDS
		.iter()
		.map(|id| {
			let cell = latest
				.get(*id)
				.cloned()
				.unwrap_or_else(|| "unchecked".to_string());
			format!("{id} strict={cell}")
		})
		.collect()
}

#[cfg(test)]
mod adr275_decimal_join_probe {
	use super::*;

	// ADR 275 review (Finding 1): encode_partition_key_value now supports Decimal128, and the EXPLICIT
	// `JOIN ... ON` gate (binder::join::validate_join_key_types) rejects only floats — not Decimal128 — so an
	// explicit Decimal equi-join is enabled by this cut as a side effect. (The comma-join recognizer is a
	// separate path that still gates Decimal via cross_equi_join_key_type_supported.) This probe pins the
	// measured behaviour end-to-end; if it ever regresses to a wall, the panic records the actual reason.
	#[tokio::test]
	async fn explicit_decimal_equi_join_executes() {
		fn schema(value_col: &str) -> SchemaRef {
			Arc::new(Schema::new(vec![
				Field::new("k", DataType::Decimal128(15, 2), false),
				Field::new(value_col, DataType::Int64, false),
			]))
		}
		fn keys() -> Decimal128Array {
			Decimal128Array::from(vec![100_i128, 250])
				.with_precision_and_scale(15, 2)
				.expect("decimal keys")
		}
		fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
			TableSchemaDescriptor::new_with_layout(
				TableObjectId::new([id; 16]),
				1,
				"default",
				name,
				schema,
				RowIdentityPolicy::AppendOnly,
				LocalityPolicy::default_table_spread(),
				RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
				SchemaEvolutionPolicy::AdditiveOnly,
			)
			.expect("descriptor")
		}
		let left = RecordBatch::try_new(
			schema("av"),
			vec![Arc::new(keys()), Arc::new(Int64Array::from(vec![1_i64, 2]))],
		)
		.expect("left batch");
		let right = RecordBatch::try_new(
			schema("bv"),
			vec![
				Arc::new(keys()),
				Arc::new(Int64Array::from(vec![10_i64, 20])),
			],
		)
		.expect("right batch");
		let tables = vec![
			HarnessTable {
				id: TableId(1),
				name: "a",
				descriptor: descriptor(21, "a", schema("av").as_ref()),
				facts: facts_for_batch(&left),
				batch: left,
			},
			HarnessTable {
				id: TableId(2),
				name: "b",
				descriptor: descriptor(22, "b", schema("bv").as_ref()),
				facts: facts_for_batch(&right),
				batch: right,
			},
		];
		match execute_query_with_tables("SELECT a.av, b.bv FROM a JOIN b ON a.k = b.k", tables)
			.await
		{
			Ok(batches) => {
				let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
				assert_eq!(rows, 2, "both Decimal keys (100, 250) match across a and b");
			}
			Err(StageError { phase, reason }) => {
				panic!("explicit Decimal equi-join walled at {phase:?}: {reason}");
			}
		}
	}
}
