//! ADR 261: TPC-H Reachability Harness — the library half (CLI lives in `main.rs`).
//!
//! Dev mode generates the harness `LINEITEM` subset from the pure-Rust `tpchgen` row structs,
//! converts `TPCHDecimal → Float64` and `TPCHDate → Date32` into workspace-Arrow, loads them through a
//! local [`StorageScan`], and runs Toron-authored reachability SQL single-node through `plan → bind →
//! execute`. A `pass` here means *reachability* (the shape ran and produced a digest), not TPC-H
//! correctness — dev data is synthetic and explicitly NOT a TPC-H result.

mod adr444_profile_eval;
pub mod telemetry;

pub use adr444_profile_eval::{
	ADR444_ABSOLUTE_SPREAD_LIMIT, ADR444_CHILDREN, ADR444_MECHANISM_ENVELOPE,
	ADR444_MEDIAN_SEPARATION, ADR444_SHARE_SPREAD_LIMIT, Adr444ChildSample, Adr444Evaluation,
	Adr444Gate, Adr444VarianceEvaluation, Adr444VarianceVerdict, evaluate_adr444_cohort,
	evaluate_adr444_variance,
};

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::array::{
	Array, ArrayRef, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
	StringArray, new_null_array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use tpchgen::generators::{
	CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
	PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

use toron::binder::{
	PlannerConfig, bind, bind_with_explain_summary, bind_with_local_join_admissions,
};
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::distributed::budget::{ByteCount, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::task::{JoinBuildProfile, JoinSpillObservation, RowCount, TaskTelemetry};
use toron::error::StorageError;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	BoundExprNode, BoundExprProgram, ExecutionPlan, JoinKind, LogicalPlan, TableId,
	decimal_division_output_type,
};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	RuntimeFilterDescriptor, SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	build_runtime_filter_mask_for_batch, pbc::Program,
};

use telemetry::{
	Attempt, CorrectnessAttempt, PerfAttempt, PerfPhaseTimings, Phase, Status, reason_for_error,
	status_for_reason,
};

/// Default cache / output root — always under `target/`, never a tracked path.
pub const CACHE_ROOT: &str = "target/tpch";

const TPCH_PERSISTED_CHUNK_ROWS: usize = 65_536;
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

/// ADR 473: the primitive ladder — six shapes that isolate one cost each, so
/// composite results have a denominator. All read `lineitem` except `m06`.
/// ADR 473: the primitive ladder's members, in cost order. `m01` is retained but
/// is NOT a scan floor — engines answer `COUNT(1)` from parquet metadata.
pub const PRIMITIVE_LADDER_IDS: [&str; 11] = [
	"m00", "m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10",
];

/// What each primitive isolates, for the ladder receipt.
pub fn primitive_ladder_intent(id: &str) -> &'static str {
	match id {
		"m00" => "scan proof (sum of literal; no metadata shortcut)",
		"m01" => "count shortcut candidate (NOT a scan floor)",
		"m02" => "one decimal column decode + sum",
		"m03" => "predicate + second column (selectivity)",
		"m04" => "low-cardinality grouping (3 groups)",
		"m05" => "high-cardinality grouping (~12M groups at sf=8)",
		"m06" => "one equi-join with orders",
		"m07" => "decimal column decode + null-check (no arithmetic)",
		"m08" => "Int64 column decode + null-check (no arithmetic)",
		"m09" => "Int64 decode + integer arithmetic",
		"m10" => "two-column decimal decode + arithmetic (per-row dispatch width)",
		_ => "unknown",
	}
}

pub const M00_SQL: &str = "SELECT SUM(1) AS scanned_rows FROM lineitem";
pub const M01_SQL: &str = "SELECT COUNT(1) AS row_count FROM lineitem";
pub const M02_SQL: &str = "SELECT SUM(l_quantity) AS total_quantity FROM lineitem";
pub const M03_SQL: &str = "SELECT SUM(l_extendedprice) AS total_price FROM lineitem \
	WHERE l_shipdate >= DATE '1995-09-01'";
pub const M04_SQL: &str = "SELECT l_returnflag, COUNT(1) AS row_count FROM lineitem \
	GROUP BY l_returnflag";
pub const M05_SQL: &str = "SELECT l_orderkey, COUNT(1) AS row_count FROM lineitem \
	GROUP BY l_orderkey";
pub const M07_SQL: &str = "SELECT COUNT(l_quantity) AS decoded FROM lineitem";
pub const M08_SQL: &str = "SELECT COUNT(l_orderkey) AS decoded FROM lineitem";
pub const M09_SQL: &str = "SELECT SUM(l_orderkey) AS total FROM lineitem";
pub const M10_SQL: &str = "SELECT SUM(l_extendedprice * l_discount) AS total FROM lineitem";

pub const M06_SQL: &str = "SELECT COUNT(l_orderkey) AS row_count FROM lineitem \
	INNER JOIN orders ON l_orderkey = o_orderkey";

/// strict q14 — identical shape to the dev proxy; the `lineitem` fixture carries official
/// `Decimal128(15,2)` types instead of `Float64`.
pub const STRICT_Q14_SQL: &str = Q14_DEV_SQL;

/// ADR 281 — tpch.q17 (Small-Quantity-Order Revenue): tpchgen-2.0.2's exact Q17 with `:1` → `'Brand#23'`,
/// `:2` → `'MED BOX'`, no LIMIT. The official UNqualified correlated scalar subquery
/// `(SELECT 0.2 * AVG(l_quantity) ... WHERE l_partkey = p_partkey)` is kept verbatim — it is the measured
/// frontier (an unqualified outer ref, with the aggregate wrapped in `0.2 *` arithmetic beyond ADR 260's bare
/// correlated aggregate scalar). Not pre-qualified, not pre-decorrelated.
pub const Q17_DEV_SQL: &str = "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly \
	FROM lineitem, part \
	WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' \
	  AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";

/// strict q17 — start identical to the dev text; if strict walls only on harness literal typing (`7.0`/`0.2`)
/// before the intended correlated-scalar wall, a narrow strict variant is introduced + documented as a harness
/// deviation in the results doc (ADR 281).
pub const STRICT_Q17_SQL: &str = Q17_DEV_SQL;

fn q17_eligible_partkeys(scale_factor: f64) -> Vec<i64> {
	PartGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter(|item| {
			item.p_brand.to_string() == "Brand#23" && item.p_container.to_string() == "MED BOX"
		})
		.map(|item| item.p_partkey)
		.collect()
}

fn q17_census_partkey_predicate(column: &str, partkeys: &[i64]) -> Result<String, String> {
	if partkeys.is_empty() {
		return Err("q17 census found no eligible Brand#23/MED BOX parts".to_string());
	}
	// The SQL frontend intentionally caps each literal IN list at 32 items. Compose bounded lists with
	// OR so the predicate remains one pushed scan filter rather than rereading lineitem per chunk.
	Ok(partkeys
		.chunks(32)
		.map(|chunk| {
			format!(
				"{column} IN ({})",
				chunk
					.iter()
					.map(i64::to_string)
					.collect::<Vec<_>>()
					.join(",")
			)
		})
		.collect::<Vec<_>>()
		.join(" OR "))
}

/// At sf=10 the generic join-shaped diagnostic falls outside ADR 417's q17 streaming recognizer and
/// would materialize the unfiltered lineitem scan. Materialize only the tiny eligible-key set in the
/// harness SQL so the ordinary scan predicate removes non-eligible rows before aggregation.
fn strict_q17_census_agg_sql(partkeys: &[i64]) -> Result<String, String> {
	let eligible = q17_census_partkey_predicate("l_partkey", partkeys)?;
	Ok(format!(
		"SELECT l_partkey AS p_partkey, \
		 SUM(l_quantity) AS quantity_sum, COUNT(*) AS quantity_count, AVG(l_quantity) AS quantity_avg \
		 FROM lineitem WHERE ({eligible}) GROUP BY l_partkey"
	))
}

/// Recompute AVG inside Toron, then compare every eligible lineitem row against Toron's emitted Decimal
/// average. Both scans carry the same bounded eligible-key predicate; no oracle threshold enters the SQL.
fn strict_q17_census_rows_sql(partkeys: &[i64]) -> Result<String, String> {
	let inner_eligible = q17_census_partkey_predicate("l_partkey", partkeys)?;
	let outer_eligible = q17_census_partkey_predicate("lineitem.l_partkey", partkeys)?;
	Ok(format!(
		"WITH part_avg AS ( \
		 SELECT l_partkey AS avg_partkey, AVG(l_quantity) AS avg_quantity \
		 FROM lineitem WHERE ({inner_eligible}) GROUP BY l_partkey \
		 ) SELECT lineitem.l_partkey AS p_partkey, l_orderkey, l_suppkey, l_quantity, l_extendedprice \
		 FROM lineitem, part_avg \
		 WHERE lineitem.l_partkey = part_avg.avg_partkey \
		 AND ({outer_eligible}) \
		 AND lineitem.l_quantity < 0.2 * part_avg.avg_quantity"
	))
}

/// ADR 315 — tpch.q18 (Large Volume Customer): official-shaped q18 with `:1` -> `300` and `LIMIT 100`.
/// The grouped `IN` subquery body is intentionally kept verbatim; this reachability attempt measures the
/// first wall rather than pre-solving the membership-subquery shape.
pub const Q18_DEV_SQL: &str = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) \
	FROM customer, orders, lineitem \
	WHERE o_orderkey IN ( \
	  SELECT l_orderkey \
	  FROM lineitem \
	  GROUP BY l_orderkey \
	  HAVING SUM(l_quantity) > 300 \
	) \
	  AND c_custkey = o_custkey \
	  AND o_orderkey = l_orderkey \
	GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
	ORDER BY o_totalprice DESC, o_orderdate \
	LIMIT 100";

/// strict q18 — identical SQL shape; strict mode carries official Decimal128 lineitem quantity.
pub const STRICT_Q18_SQL: &str = Q18_DEV_SQL;

/// ADR 316/319 — tpch.q19 (Discounted Revenue): official-shaped q19 with the three OR branches inlined.
/// The harness keeps a keyed-plan guard so the OR-branch extraction cannot regress into a default-scale
/// cartesian.
pub const Q19_DEV_SQL: &str = "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue \
	FROM lineitem, part \
	WHERE (p_partkey = l_partkey \
	    AND p_brand = 'Brand#12' \
	    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
	    AND l_quantity >= 1 \
	    AND l_quantity <= 11 \
	    AND p_size BETWEEN 1 AND 5 \
	    AND l_shipmode IN ('AIR', 'AIR REG') \
	    AND l_shipinstruct = 'DELIVER IN PERSON') \
	   OR (p_partkey = l_partkey \
	    AND p_brand = 'Brand#23' \
	    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
	    AND l_quantity >= 10 \
	    AND l_quantity <= 20 \
	    AND p_size BETWEEN 1 AND 10 \
	    AND l_shipmode IN ('AIR', 'AIR REG') \
	    AND l_shipinstruct = 'DELIVER IN PERSON') \
	   OR (p_partkey = l_partkey \
	    AND p_brand = 'Brand#34' \
	    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
	    AND l_quantity >= 20 \
	    AND l_quantity <= 30 \
	    AND p_size BETWEEN 1 AND 15 \
	    AND l_shipmode IN ('AIR', 'AIR REG') \
	    AND l_shipinstruct = 'DELIVER IN PERSON')";

/// strict q19 — identical SQL shape; strict mode carries official Decimal128 lineitem money/quantity types.
pub const STRICT_Q19_SQL: &str = Q19_DEV_SQL;

/// ADR 317 — tpch.q21 (Supplier Who Kept Orders Waiting): official-shaped q21 with `:1` -> `'SAUDI ARABIA'`
/// and `LIMIT 100` preserved. The query carries a correlated `EXISTS` and correlated `NOT EXISTS` over
/// lineitem self-aliases; this harness row measures that surface without pre-solving it.
pub const Q21_DEV_SQL: &str = "SELECT s_name, COUNT(*) AS numwait \
	FROM supplier, lineitem l1, orders, nation \
	WHERE s_suppkey = l1.l_suppkey \
	  AND o_orderkey = l1.l_orderkey \
	  AND o_orderstatus = 'F' \
	  AND l1.l_receiptdate > l1.l_commitdate \
	  AND EXISTS ( \
	    SELECT * \
	    FROM lineitem l2 \
	    WHERE l2.l_orderkey = l1.l_orderkey \
	      AND l2.l_suppkey <> l1.l_suppkey \
	  ) \
	  AND NOT EXISTS ( \
	    SELECT * \
	    FROM lineitem l3 \
	    WHERE l3.l_orderkey = l1.l_orderkey \
	      AND l3.l_suppkey <> l1.l_suppkey \
	      AND l3.l_receiptdate > l3.l_commitdate \
	  ) \
	  AND s_nationkey = n_nationkey \
	  AND n_name = 'SAUDI ARABIA' \
	GROUP BY s_name \
	ORDER BY numwait DESC, s_name \
	LIMIT 100";

/// strict q21 — identical SQL shape; q21 projects only `s_name` and `COUNT(*)`, so there is no strict-only
/// Decimal output surface.
pub const STRICT_Q21_SQL: &str = Q21_DEV_SQL;

/// ADR 284 — tpch.q02 (Minimum Cost Supplier): tpchgen-2.0.2's exact Q2 with `:1` → `15`, `:2` →
/// `BRASS`, `:3` → `EUROPE`, and no LIMIT. The correlated scalar `MIN(ps_supplycost)` has a multi-table
/// comma-join inner input (`partsupp, supplier, nation, region`) and is the measured frontier.
pub const Q2_DEV_SQL: &str = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, \
	s_address, s_phone, s_comment \
	FROM part, supplier, partsupp, nation, region \
	WHERE p_partkey = ps_partkey \
	  AND s_suppkey = ps_suppkey \
	  AND p_size = 15 \
	  AND p_type LIKE '%BRASS' \
	  AND s_nationkey = n_nationkey \
	  AND n_regionkey = r_regionkey \
	  AND r_name = 'EUROPE' \
	  AND ps_supplycost = ( \
	    SELECT MIN(ps_supplycost) \
	    FROM partsupp, supplier, nation, region \
	    WHERE p_partkey = ps_partkey \
	      AND s_suppkey = ps_suppkey \
	      AND s_nationkey = n_nationkey \
	      AND n_regionkey = r_regionkey \
	      AND r_name = 'EUROPE' \
	  ) \
	ORDER BY s_acctbal DESC, n_name, s_name, p_partkey";

/// strict q02 — identical shape; `supplier.s_acctbal` and `partsupp.ps_supplycost` carry official
/// `Decimal128(15,2)` types.
pub const STRICT_Q2_SQL: &str = Q2_DEV_SQL;

/// ADR 285 — tpch.q20 (Potential Part Promotion): nested membership with a correlated aggregate scalar inside
/// the membership body. The official interval upper bound is literalized (`DATE '1995-01-01'`) to keep runtime
/// interval arithmetic out of this reachability row.
pub const Q20_DEV_SQL: &str = "SELECT s_name, s_address \
	FROM supplier, nation \
	WHERE s_suppkey IN ( \
	  SELECT ps_suppkey \
	  FROM partsupp \
	  WHERE ps_partkey IN ( \
	    SELECT p_partkey \
	    FROM part \
	    WHERE p_name LIKE 'forest%' \
	  ) \
	    AND ps_availqty > ( \
	      SELECT 0.5 * SUM(l_quantity) \
	      FROM lineitem \
	      WHERE l_partkey = ps_partkey \
	        AND l_suppkey = ps_suppkey \
	        AND l_shipdate >= DATE '1994-01-01' \
	        AND l_shipdate < DATE '1995-01-01' \
	    ) \
	) \
	  AND s_nationkey = n_nationkey \
	  AND n_name = 'CANADA' \
	ORDER BY s_name";

/// strict q20 — identical SQL shape; strict mode carries official Decimal128 lineitem quantities.
pub const STRICT_Q20_SQL: &str = Q20_DEV_SQL;

/// ADR 307/308 - tpch.q13 (Customer Distribution): official-shaped q13 with the fixed
/// `%special%requests%` comment pattern and no LIMIT.
pub const Q13_DEV_SQL: &str = "SELECT c_count, COUNT(*) AS custdist \
	FROM ( \
	  SELECT c_custkey, COUNT(o_orderkey) AS c_count \
	  FROM customer LEFT OUTER JOIN orders \
	    ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' \
	  GROUP BY c_custkey \
	) AS c_orders \
	GROUP BY c_count \
	ORDER BY custdist DESC, c_count DESC";

/// strict q13 - identical SQL shape; q13 has no strict-only decimal surface.
pub const STRICT_Q13_SQL: &str = Q13_DEV_SQL;

const Q11_SQL_TEMPLATE: &str = "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value \
	FROM partsupp, supplier, nation \
	WHERE ps_suppkey = s_suppkey \
	  AND s_nationkey = n_nationkey \
	  AND n_name = 'GERMANY' \
	GROUP BY ps_partkey \
	HAVING SUM(ps_supplycost * ps_availqty) > ( \
	  SELECT SUM(ps_supplycost * ps_availqty) * [FRACTION] \
	  FROM partsupp, supplier, nation \
	  WHERE ps_suppkey = s_suppkey \
	    AND s_nationkey = n_nationkey \
	    AND n_name = 'GERMANY' \
	) \
	ORDER BY value DESC";

/// The exact q11 scale contract shared by SQL rendering and the independent oracle.
///
/// Fields are deliberately private: the only way to construct a value is the bit-exact lookup,
/// so the scale numerator and rendered decimal literal cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Q11Scale {
	hundredths_n: i128,
	fraction_sql: &'static str,
}

impl Q11Scale {
	pub fn try_from_f64(scale_factor: f64) -> Result<Self, String> {
		let scale = match scale_factor.to_bits() {
			bits if bits == 0.01_f64.to_bits() => Self {
				hundredths_n: 1,
				fraction_sql: "0.01",
			},
			bits if bits == 1.0_f64.to_bits() => Self {
				hundredths_n: 100,
				fraction_sql: "0.0001",
			},
			bits if bits == 10.0_f64.to_bits() => Self {
				hundredths_n: 1_000,
				fraction_sql: "0.00001",
			},
			_ => {
				return Err(format!(
					"q11 requires an exactly supported scale (0.01, 1, or 10); got {scale_factor:?}"
				));
			}
		};
		Ok(scale)
	}

	pub fn hundredths_n(self) -> i128 {
		self.hundredths_n
	}

	pub fn fraction_sql(self) -> &'static str {
		self.fraction_sql
	}
}

/// Render q11 from the same exact scale value consumed by the oracle.
pub fn q11_sql(scale: Q11Scale) -> String {
	debug_assert_eq!(Q11_SQL_TEMPLATE.matches("[FRACTION]").count(), 1);
	Q11_SQL_TEMPLATE.replace("[FRACTION]", scale.fraction_sql())
}

fn validate_query_scale(query: &str, scale_factor: f64) -> Result<(), String> {
	if query == "q11" {
		Q11Scale::try_from_f64(scale_factor)?;
	}
	Ok(())
}

/// ADR 314 - tpch.q11 (Important Stock Identification): the sf=1 golden text with `:1` ->
/// `GERMANY`, `:2` -> `0.0001`, and no LIMIT. Runtime q11 paths use [`q11_sql`].
pub const Q11_DEV_SQL: &str = "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value \
	FROM partsupp, supplier, nation \
	WHERE ps_suppkey = s_suppkey \
	  AND s_nationkey = n_nationkey \
	  AND n_name = 'GERMANY' \
	GROUP BY ps_partkey \
	HAVING SUM(ps_supplycost * ps_availqty) > ( \
	  SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 \
	  FROM partsupp, supplier, nation \
	  WHERE ps_suppkey = s_suppkey \
	    AND s_nationkey = n_nationkey \
	    AND n_name = 'GERMANY' \
	) \
	ORDER BY value DESC";

/// strict q11 sf=1 golden text; strict mode carries official Decimal128 partsupp supply cost.
pub const STRICT_Q11_SQL: &str = Q11_DEV_SQL;

/// ADR 313 - tpch.q15 (Top Supplier): official-shaped q15 with `:1` -> `DATE '1996-01-01'`.
/// The `revenue` CTE is intentionally referenced twice: once as an outer `FROM` source and again as the source
/// of an uncorrelated aggregate scalar subquery.
pub const Q15_DEV_SQL: &str = "WITH revenue AS ( \
	  SELECT l_suppkey AS supplier_no, \
	         SUM(l_extendedprice * (1 - l_discount)) AS total_revenue \
	  FROM lineitem \
	  WHERE l_shipdate >= DATE '1996-01-01' \
	    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH \
	  GROUP BY l_suppkey \
	) \
	SELECT s_suppkey, s_name, s_address, s_phone, total_revenue \
	FROM supplier, revenue \
	WHERE s_suppkey = supplier_no \
	  AND total_revenue = (SELECT MAX(total_revenue) FROM revenue) \
	ORDER BY s_suppkey";

/// strict q15 - identical SQL shape; strict mode carries official Decimal128 lineitem pricing columns.
pub const STRICT_Q15_SQL: &str = Q15_DEV_SQL;

/// ADR 310 - tpch.q16 (Parts/Supplier Relationship): official-shaped q16 with `:1` -> `Brand#45`,
/// `:2` -> `MEDIUM POLISHED`, `:3` -> `(49, 14, 23, 45, 19, 3, 36, 9)`, and no LIMIT.
pub const Q16_DEV_SQL: &str = "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt \
	FROM partsupp, part \
	WHERE p_partkey = ps_partkey \
	  AND p_brand <> 'Brand#45' \
	  AND p_type NOT LIKE 'MEDIUM POLISHED%' \
	  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) \
	  AND ps_suppkey NOT IN ( \
	    SELECT s_suppkey \
	    FROM supplier \
	    WHERE s_comment LIKE '%Customer%Complaints%' \
	  ) \
	GROUP BY p_brand, p_type, p_size \
	ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";

/// strict q16 - identical SQL shape; q16 projects no Decimal values.
pub const STRICT_Q16_SQL: &str = Q16_DEV_SQL;

/// ADR 304/305 - tpch.q22 (Global Sales Opportunity): official-shaped q22 with `:1` country codes
/// inlined and no LIMIT. ADR 305's SUBSTRING support makes this a reachability pass in both modes.
pub const Q22_DEV_SQL: &str = "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal \
	FROM ( \
	  SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal \
	  FROM customer \
	  WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17') \
	    AND c_acctbal > ( \
	      SELECT AVG(c_acctbal) \
	      FROM customer \
	      WHERE c_acctbal > 0.00 \
	        AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17') \
	    ) \
	    AND NOT EXISTS ( \
	      SELECT * \
	      FROM orders \
	      WHERE o_custkey = c_custkey \
	    ) \
	) AS custsale \
	GROUP BY cntrycode \
	ORDER BY cntrycode";

/// strict q22 - identical SQL shape; strict mode carries official Decimal128 customer account balances.
pub const STRICT_Q22_SQL: &str = Q22_DEV_SQL;

/// ADR 297 — tpch.q08 (National Market Share): tpchgen-2.0.2's Q8 with `:1` → `'BRAZIL'`,
/// `:2` → `'AMERICA'`, `:3` → `'ECONOMY ANODIZED STEEL'`, and no LIMIT. The derived body has eight
/// logical relation factors, but `nation n1` and `nation n2` are SQL aliases over one physical `nation`
/// fixture.
pub const Q8_DEV_SQL: &str = "SELECT o_year, \
	SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share \
	FROM ( \
	  SELECT EXTRACT(year FROM o_orderdate) AS o_year, \
	         l_extendedprice * (1 - l_discount) AS volume, \
	         n2.n_name AS nation \
	  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region \
	  WHERE p_partkey = l_partkey \
	    AND s_suppkey = l_suppkey \
	    AND l_orderkey = o_orderkey \
	    AND o_custkey = c_custkey \
	    AND c_nationkey = n1.n_nationkey \
	    AND n1.n_regionkey = r_regionkey \
	    AND r_name = 'AMERICA' \
	    AND s_nationkey = n2.n_nationkey \
	    AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
	    AND p_type = 'ECONOMY ANODIZED STEEL' \
	) AS all_nations \
	GROUP BY o_year \
	ORDER BY o_year";

/// strict q08 — identical shape; strict mode carries official Decimal128 lineitem pricing columns.
pub const STRICT_Q8_SQL: &str = Q8_DEV_SQL;

/// ADR 301 — tpch.q09 (Product Type Profit Measure): tpchgen-2.0.2's Q9 with `:1` → `'green'`
/// and no LIMIT. q09 is a reachability attempt over a six-table derived comma join plus strict profit
/// arithmetic `(revenue - supply cost)`.
pub const Q9_DEV_SQL: &str = "SELECT nation, o_year, SUM(amount) AS sum_profit \
	FROM ( \
	  SELECT n_name AS nation, \
	         EXTRACT(year FROM o_orderdate) AS o_year, \
	         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount \
	  FROM part, supplier, lineitem, partsupp, orders, nation \
	  WHERE s_suppkey = l_suppkey \
	    AND ps_suppkey = l_suppkey \
	    AND ps_partkey = l_partkey \
	    AND p_partkey = l_partkey \
	    AND o_orderkey = l_orderkey \
	    AND s_nationkey = n_nationkey \
	    AND p_name LIKE '%green%' \
	) AS profit \
	GROUP BY nation, o_year \
	ORDER BY nation, o_year DESC";

/// strict q09 — identical shape; strict mode carries official Decimal128 lineitem/partsupp numeric columns.
pub const STRICT_Q9_SQL: &str = Q9_DEV_SQL;

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

/// ADR 391 — hand-authored Rune q03 shape. Rune still binds through the normal binder; this script only
/// bypasses SQL parsing/lowering and pins the logical join shape we want to compare against SQL q03.
pub const RUNE_Q3_SCRIPT: &str = r#"
pub fn main(ctx) {
    let customer = ctx.table("customer").alias("c")
        .filter(qcol("c", "c_mktsegment").eq(lit_str("BUILDING")));
    let orders = ctx.table("orders").alias("o")
        .filter(qcol("o", "o_orderdate").lt(lit_date("1995-03-15")));
    let lineitem = ctx.table("lineitem").alias("l")
        .filter(qcol("l", "l_shipdate").gt(lit_date("1995-03-15")));

    customer
        .auto_join(
            orders,
            qcol("c", "c_custkey").eq(qcol("o", "o_custkey")),
        )
        .auto_join(
            lineitem,
            qcol("o", "o_orderkey").eq(qcol("l", "l_orderkey")),
        )
        .aggregate(
            [
                qcol("l", "l_orderkey"),
                qcol("o", "o_orderdate"),
                qcol("o", "o_shippriority"),
            ],
            [
                sum(qcol("l", "l_extendedprice").mul(lit(1).sub(qcol("l", "l_discount")))),
            ],
        )
        .project_named([
            col("l_orderkey").named("l_orderkey"),
            col("sum").named("revenue"),
            col("o_orderdate").named("o_orderdate"),
            col("o_shippriority").named("o_shippriority"),
        ])
        .sort([col("revenue").desc(), col("o_orderdate").asc()])
}
"#;

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

/// ADR 293 — tpch.q07 (Volume Shipping): the self-join reachability probe. tpchgen-2.0.2's exact Q7
/// shape with `:1` → `'FRANCE'`, `:2` → `'GERMANY'`, and no LIMIT. The query intentionally references
/// `nation` twice via SQL aliases (`nation n1`, `nation n2`) while the harness registers one physical
/// `nation` table.
pub const Q7_DEV_SQL: &str = "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue \
	FROM ( \
	  SELECT n1.n_name AS supp_nation, \
	         n2.n_name AS cust_nation, \
	         EXTRACT(year FROM l_shipdate) AS l_year, \
	         l_extendedprice * (1 - l_discount) AS volume \
	  FROM supplier, lineitem, orders, customer, nation n1, nation n2 \
	  WHERE s_suppkey = l_suppkey \
	    AND o_orderkey = l_orderkey \
	    AND c_custkey = o_custkey \
	    AND s_nationkey = n1.n_nationkey \
	    AND c_nationkey = n2.n_nationkey \
	    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') \
	      OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) \
	    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
	) AS shipping \
	GROUP BY supp_nation, cust_nation, l_year \
	ORDER BY supp_nation, cust_nation, l_year";

/// strict q07 — identical shape; the `lineitem` fixture carries official `Decimal128(15,2)` types.
pub const STRICT_Q7_SQL: &str = Q7_DEV_SQL;

/// ADR 278 — tpch.q04 (Order Priority Checking): the gauge's first correlated-subquery query, tpchgen-2.0.2's
/// exact Q4 with `:1` → `'1993-07-01'`. The `+ INTERVAL '3' MONTH` bound is **kept official** (a `Date32 +
/// CalendarMonthInterval` expression — both operands literal, so it may constant-fold at bind) and the
/// `EXISTS (… l_orderkey = o_orderkey …)` is unqualified — q04 carries both new classes, and the measured
/// first wall (plan/bind/execute) decides the next ADR. Same text both modes; only `lineitem` types differ.
pub const Q4_DEV_SQL: &str = "SELECT o_orderpriority, COUNT(*) AS order_count \
	FROM orders \
	WHERE o_orderdate >= DATE '1993-07-01' \
	  AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH \
	  AND EXISTS ( \
	    SELECT * FROM lineitem \
	    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate \
	  ) \
	GROUP BY o_orderpriority \
	ORDER BY o_orderpriority";

/// strict q04 — identical shape; the `lineitem` fixture carries official `Decimal128(15,2)` types (q04 uses no
/// `lineitem` decimals, so the modes differ only incidentally).
pub const STRICT_Q4_SQL: &str = Q4_DEV_SQL;

/// ADR 290 — tpch.q12 (Shipping Modes and Order Priority): tpchgen-2.0.2's Q12 with `:1` → `'MAIL'`,
/// `:2` → `'SHIP'`, `:3` → `DATE '1994-01-01'`, and the interval upper bound literalized to
/// `DATE '1995-01-01'`. Same text both modes; q12 references only shared key/date/string columns and integer
/// CASE literals.
pub const Q12_DEV_SQL: &str = "SELECT l_shipmode, \
	SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, \
	SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count \
	FROM orders, lineitem \
	WHERE o_orderkey = l_orderkey \
	  AND l_shipmode IN ('MAIL', 'SHIP') \
	  AND l_commitdate < l_receiptdate \
	  AND l_shipdate < l_commitdate \
	  AND l_receiptdate >= DATE '1994-01-01' \
	  AND l_receiptdate < DATE '1995-01-01' \
	GROUP BY l_shipmode \
	ORDER BY l_shipmode";

/// strict q12 — identical shape; q12 uses no mode-specific Decimal columns.
pub const STRICT_Q12_SQL: &str = Q12_DEV_SQL;

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
		// ADR 278: q04 EXISTS-subquery dates, appended.
		Field::new("l_commitdate", DataType::Date32, false),
		Field::new("l_receiptdate", DataType::Date32, false),
		// ADR 290: q12 ship-mode filter/group key, appended.
		Field::new("l_shipmode", DataType::Utf8, false),
		// ADR 316: q19 ship-instruction branch filter, appended.
		Field::new("l_shipinstruct", DataType::Utf8, false),
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
	let mut commitdate = Vec::new();
	let mut receiptdate = Vec::new();
	let mut shipmode = Vec::new();
	let mut shipinstruct = Vec::new();
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
		commitdate.push(item.l_commitdate.to_unix_epoch());
		receiptdate.push(item.l_receiptdate.to_unix_epoch());
		shipmode.push(item.l_shipmode.to_string());
		shipinstruct.push(item.l_shipinstruct.to_string());
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
			Arc::new(Date32Array::from(commitdate)),
			Arc::new(Date32Array::from(receiptdate)),
			Arc::new(StringArray::from(shipmode)),
			Arc::new(StringArray::from(shipinstruct)),
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
		// ADR 278: q04 EXISTS-subquery dates, appended.
		Field::new("l_commitdate", DataType::Date32, false),
		Field::new("l_receiptdate", DataType::Date32, false),
		// ADR 290: q12 ship-mode filter/group key, appended.
		Field::new("l_shipmode", DataType::Utf8, false),
		// ADR 316: q19 ship-instruction branch filter, appended.
		Field::new("l_shipinstruct", DataType::Utf8, false),
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
///
/// Use [`strict_lineitem_row_count`] when only the source cardinality is needed;
/// materializing this complete batch solely to call `num_rows()` is needlessly expensive
/// at large scales.
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
	let mut commitdate = Vec::new();
	let mut receiptdate = Vec::new();
	let mut shipmode = Vec::new();
	let mut shipinstruct = Vec::new();
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
		commitdate.push(item.l_commitdate.to_unix_epoch());
		receiptdate.push(item.l_receiptdate.to_unix_epoch());
		shipmode.push(item.l_shipmode.to_string());
		shipinstruct.push(item.l_shipinstruct.to_string());
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
			Arc::new(Date32Array::from(commitdate)),
			Arc::new(Date32Array::from(receiptdate)),
			Arc::new(StringArray::from(shipmode)),
			Arc::new(StringArray::from(shipinstruct)),
		],
	)
	.expect("strict lineitem batch")
}

/// Exact strict `LINEITEM` source cardinality without materializing its Arrow columns.
///
/// This traverses the same strict generator as [`generate_strict_lineitem`], so it is
/// scale-parametric and remains the authoritative q05 gate-zero OFF expectation.
pub fn strict_lineitem_row_count(scale_factor: f64) -> u64 {
	LineItemGenerator::new(scale_factor, 1, 1).iter().count() as u64
}

/// The q14 `PART` fixture: the join key + the type string q14's CASE matches. Same in dev and strict (no
/// decimals). Generated from `tpchgen` so partkeys align with the generated `lineitem`.
fn part_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("p_partkey", DataType::Int64, false),
		Field::new("p_type", DataType::Utf8, false),
		// ADR 281: q17 filters (p_brand = 'Brand#23', p_container = 'MED BOX'), appended (q14 ignores them).
		Field::new("p_brand", DataType::Utf8, false),
		Field::new("p_container", DataType::Utf8, false),
		// ADR 284: q02 filter/projection columns, appended (q14/q17 ignore them).
		Field::new("p_size", DataType::Int32, false),
		Field::new("p_mfgr", DataType::Utf8, false),
		// ADR 285: q20 filters by part name prefix; appended so prior projections keep their positions.
		Field::new("p_name", DataType::Utf8, false),
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
	let mut brand = Vec::new();
	let mut container = Vec::new();
	let mut size = Vec::new();
	let mut mfgr = Vec::new();
	let mut name = Vec::new();
	for item in generator.iter() {
		partkey.push(item.p_partkey);
		part_type.push(item.p_type.to_string());
		brand.push(item.p_brand.to_string());
		container.push(item.p_container.to_string());
		size.push(item.p_size);
		mfgr.push(item.p_mfgr.to_string());
		name.push(item.p_name.to_string());
	}
	RecordBatch::try_new(
		part_schema(),
		vec![
			Arc::new(Int64Array::from(partkey)),
			Arc::new(StringArray::from(part_type)),
			Arc::new(StringArray::from(brand)),
			Arc::new(StringArray::from(container)),
			Arc::new(Int32Array::from(size)),
			Arc::new(StringArray::from(mfgr)),
			Arc::new(StringArray::from(name)),
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

/// The shared `ORDERS` fixture: the keys, dates, status, comments, and q18 total price.
fn orders_schema(strict: bool) -> SchemaRef {
	let totalprice = if strict {
		DataType::Decimal128(15, 2)
	} else {
		DataType::Float64
	};
	Arc::new(Schema::new(vec![
		Field::new("o_orderkey", DataType::Int64, false),
		Field::new("o_custkey", DataType::Int64, false),
		Field::new("o_orderdate", DataType::Date32, false),
		Field::new("o_shippriority", DataType::Int32, false),
		// ADR 278: q04 GROUP BY key, appended (q03/q10 don't reference it).
		Field::new("o_orderpriority", DataType::Utf8, false),
		Field::new("o_comment", DataType::Utf8, false),
		// ADR 322: q18 SELECT/GROUP BY/ORDER BY surface, appended to preserve existing fixtures.
		Field::new("o_totalprice", totalprice, false),
		// ADR 323: q21 filters final orders; appended to preserve existing fixtures.
		Field::new("o_orderstatus", DataType::Utf8, false),
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

/// Generate the reached-query `ORDERS` columns at `scale_factor`. `o_orderdate` converts `TPCHDate`
/// to `Date32` like `l_shipdate`; `o_totalprice` follows the harness mode's numeric representation.
pub fn generate_orders(strict: bool, scale_factor: f64) -> RecordBatch {
	let generator = OrderGenerator::new(scale_factor, 1, 1);
	let mut orderkey = Vec::new();
	let mut custkey = Vec::new();
	let mut orderdate = Vec::new();
	let mut shippriority = Vec::new();
	let mut orderpriority = Vec::new();
	let mut comment = Vec::new();
	let mut orderstatus = Vec::new();
	let mut totalprice_strict = Vec::new();
	let mut totalprice_dev = Vec::new();
	for item in generator.iter() {
		orderkey.push(item.o_orderkey);
		custkey.push(item.o_custkey);
		orderdate.push(item.o_orderdate.to_unix_epoch());
		shippriority.push(item.o_shippriority);
		orderpriority.push(item.o_orderpriority.to_string());
		comment.push(item.o_comment.to_string());
		orderstatus.push(item.o_orderstatus.to_string());
		totalprice_strict.push(item.o_totalprice.into_inner() as i128);
		totalprice_dev.push(item.o_totalprice.as_f64());
	}
	let totalprice: Arc<dyn Array> = if strict {
		Arc::new(
			Decimal128Array::from(totalprice_strict)
				.with_precision_and_scale(15, 2)
				.expect("o_totalprice decimal column"),
		)
	} else {
		Arc::new(Float64Array::from(totalprice_dev))
	};
	RecordBatch::try_new(
		orders_schema(strict),
		vec![
			Arc::new(Int64Array::from(orderkey)),
			Arc::new(Int64Array::from(custkey)),
			Arc::new(Date32Array::from(orderdate)),
			Arc::new(Int32Array::from(shippriority)),
			Arc::new(StringArray::from(orderpriority)),
			Arc::new(StringArray::from(comment)),
			totalprice,
			Arc::new(StringArray::from(orderstatus)),
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

/// The `SUPPLIER` fixture: q05 needs the keys; q02 appends the output columns and `s_acctbal`.
/// Mode-sensitive like `customer`: account balance is `Float64` in dev and `Decimal128(15,2)` in strict.
fn supplier_schema(strict: bool) -> SchemaRef {
	let acctbal = if strict {
		Field::new("s_acctbal", DataType::Decimal128(15, 2), false)
	} else {
		Field::new("s_acctbal", DataType::Float64, false)
	};
	Arc::new(Schema::new(vec![
		Field::new("s_suppkey", DataType::Int64, false),
		Field::new("s_nationkey", DataType::Int64, false),
		Field::new("s_name", DataType::Utf8, false),
		Field::new("s_address", DataType::Utf8, false),
		Field::new("s_phone", DataType::Utf8, false),
		acctbal,
		Field::new("s_comment", DataType::Utf8, false),
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

/// Generate `SUPPLIER` at `scale_factor`. `s_acctbal` is `Float64` (dev) or scale-2
/// `Decimal128(15,2)` (strict).
pub fn generate_supplier(strict: bool, scale_factor: f64) -> RecordBatch {
	let generator = SupplierGenerator::new(scale_factor, 1, 1);
	let mut suppkey = Vec::new();
	let mut nationkey = Vec::new();
	let mut name = Vec::new();
	let mut address = Vec::new();
	let mut phone = Vec::new();
	let mut acctbal_dev = Vec::new();
	let mut acctbal_strict = Vec::new();
	let mut comment = Vec::new();
	for item in generator.iter() {
		suppkey.push(item.s_suppkey);
		nationkey.push(item.s_nationkey);
		name.push(item.s_name.to_string());
		address.push(item.s_address.to_string());
		phone.push(item.s_phone.to_string());
		if strict {
			acctbal_strict.push(item.s_acctbal.into_inner() as i128);
		} else {
			acctbal_dev.push(item.s_acctbal.as_f64());
		}
		comment.push(item.s_comment);
	}
	let acctbal: arrow::array::ArrayRef = if strict {
		Arc::new(
			Decimal128Array::from(acctbal_strict)
				.with_precision_and_scale(15, 2)
				.expect("s_acctbal decimal column"),
		)
	} else {
		Arc::new(Float64Array::from(acctbal_dev))
	};
	RecordBatch::try_new(
		supplier_schema(strict),
		vec![
			Arc::new(Int64Array::from(suppkey)),
			Arc::new(Int64Array::from(nationkey)),
			Arc::new(StringArray::from(name)),
			Arc::new(StringArray::from(address)),
			Arc::new(StringArray::from(phone)),
			acctbal,
			Arc::new(StringArray::from(comment)),
		],
	)
	.expect("supplier batch")
}

/// The q02 `PARTSUPP` fixture. `ps_supplycost` is mode-sensitive like the TPC-H money columns.
fn partsupp_schema(strict: bool) -> SchemaRef {
	let supplycost = if strict {
		Field::new("ps_supplycost", DataType::Decimal128(15, 2), false)
	} else {
		Field::new("ps_supplycost", DataType::Float64, false)
	};
	Arc::new(Schema::new(vec![
		Field::new("ps_partkey", DataType::Int64, false),
		Field::new("ps_suppkey", DataType::Int64, false),
		Field::new("ps_availqty", DataType::Int32, false),
		supplycost,
	]))
}

fn partsupp_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([20; 16]),
		1,
		"default",
		"partsupp",
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("partsupp descriptor")
}

/// Generate q02-needed `PARTSUPP` columns at `scale_factor`.
pub fn generate_partsupp(strict: bool, scale_factor: f64) -> RecordBatch {
	let generator = PartSuppGenerator::new(scale_factor, 1, 1);
	let mut partkey = Vec::new();
	let mut suppkey = Vec::new();
	let mut availqty = Vec::new();
	let mut supplycost_dev = Vec::new();
	let mut supplycost_strict = Vec::new();
	for item in generator.iter() {
		partkey.push(item.ps_partkey);
		suppkey.push(item.ps_suppkey);
		availqty.push(item.ps_availqty);
		if strict {
			supplycost_strict.push(item.ps_supplycost.into_inner() as i128);
		} else {
			supplycost_dev.push(item.ps_supplycost.as_f64());
		}
	}
	let supplycost: arrow::array::ArrayRef = if strict {
		Arc::new(
			Decimal128Array::from(supplycost_strict)
				.with_precision_and_scale(15, 2)
				.expect("ps_supplycost decimal column"),
		)
	} else {
		Arc::new(Float64Array::from(supplycost_dev))
	};
	RecordBatch::try_new(
		partsupp_schema(strict),
		vec![
			Arc::new(Int64Array::from(partkey)),
			Arc::new(Int64Array::from(suppkey)),
			Arc::new(Int32Array::from(availqty)),
			supplycost,
		],
	)
	.expect("partsupp batch")
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

		if let Some(runtime_filter) = request.runtime_filter.as_ref() {
			batch = apply_harness_runtime_filter(batch, runtime_filter)?;
		}

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

/// Apply a runtime filter in the harness at the same source-schema boundary as the storage ring.
/// Invalid descriptors fail open: a filter is an optimization and declining it must preserve rows.
fn apply_harness_runtime_filter(
	batch: RecordBatch,
	descriptor: &RuntimeFilterDescriptor,
) -> Result<RecordBatch, StorageError> {
	let Ok(mask) = build_runtime_filter_mask_for_batch(&batch, descriptor) else {
		return Ok(batch);
	};
	filter_record_batch(&batch, &mask).map_err(StorageError::from)
}

#[derive(Debug, Clone)]
pub struct ParquetHarnessTableSource {
	pub name: String,
	pub path: PathBuf,
	pub schema: SchemaRef,
}

#[derive(Debug, Clone)]
pub struct ParquetHarnessScan {
	pub tables: HashMap<TableId, ParquetHarnessTableSource>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParquetScanBatchLedger {
	pub parquet_decode_micros: u64,
	pub raw_rows: usize,
	pub raw_columns: usize,
	pub raw_bytes: usize,
	pub inflated_bytes: Option<usize>,
	pub output_rows: usize,
	pub output_columns: usize,
	pub output_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParquetScanLedger {
	pub table_id: u64,
	pub table_name: String,
	pub full_column_count: usize,
	pub requested_projected_columns: Option<Vec<u32>>,
	pub pbc_predicate_bytes: Option<usize>,
	pub pbc_referenced_columns: Vec<usize>,
	pub parquet_read_columns: Option<Vec<usize>>,
	pub output_column_count: usize,
	pub inflated_to_full_schema: bool,
	pub batches: Vec<ParquetScanBatchLedger>,
}

fn parquet_scan_ledger_enabled() -> bool {
	static ENABLED: OnceLock<bool> = OnceLock::new();
	*ENABLED.get_or_init(|| std::env::var_os("TORON_TPCH_PARQUET_SCAN_LEDGER").is_some())
}

fn parquet_scan_ledger_store() -> &'static Mutex<Vec<ParquetScanLedger>> {
	static LEDGER: OnceLock<Mutex<Vec<ParquetScanLedger>>> = OnceLock::new();
	LEDGER.get_or_init(|| Mutex::new(Vec::new()))
}

fn clear_parquet_scan_ledger() {
	if parquet_scan_ledger_enabled() {
		parquet_scan_ledger_store()
			.lock()
			.expect("parquet scan ledger lock")
			.clear();
	}
}

pub fn take_parquet_scan_ledger() -> Vec<ParquetScanLedger> {
	if !parquet_scan_ledger_enabled() {
		return Vec::new();
	}
	std::mem::take(
		&mut *parquet_scan_ledger_store()
			.lock()
			.expect("parquet scan ledger lock"),
	)
}

fn record_parquet_scan_ledger(ledger: ParquetScanLedger) {
	if parquet_scan_ledger_enabled() {
		parquet_scan_ledger_store()
			.lock()
			.expect("parquet scan ledger lock")
			.push(ledger);
	}
}

fn parquet_scan_progress_path() -> Option<&'static PathBuf> {
	static PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
	PATH.get_or_init(|| {
		std::env::var_os("TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH").map(PathBuf::from)
	})
	.as_ref()
}

fn parquet_scan_progress_lock() -> &'static Mutex<()> {
	static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
	LOCK.get_or_init(|| Mutex::new(()))
}

fn next_parquet_scan_progress_id() -> u64 {
	static NEXT_ID: AtomicU64 = AtomicU64::new(1);
	NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn record_parquet_scan_progress<T: Serialize>(event: &T) {
	let Some(path) = parquet_scan_progress_path() else {
		return;
	};
	let Ok(_guard) = parquet_scan_progress_lock().lock() else {
		return;
	};
	let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
		return;
	};
	if serde_json::to_writer(&mut file, event).is_ok() {
		let _ = file.write_all(b"\n");
	}
}

fn record_batch_bytes(batch: &RecordBatch) -> usize {
	batch.get_array_memory_size()
}

#[async_trait]
impl StorageScan for ParquetHarnessScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let source =
			self.tables
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;

		let table_id = request.table_id.0;
		let projected_columns = request.projected_columns.clone();
		let runtime_filter_present = request.runtime_filter.is_some();
		let mut pbc = request.pbc_predicate.clone();
		let full_column_count = source.schema.fields().len();
		let mut pbc_referenced_columns = Vec::new();
		let mut pbc_projection = if let Some(program) = &mut pbc {
			let compiled =
				Program::compile(program).map_err(|error| StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				})?;
			let mut columns = match &projected_columns {
				Some(projected) => projected
					.iter()
					.map(|column| column.0 as usize)
					.collect::<Vec<_>>(),
				None => (0..full_column_count).collect::<Vec<_>>(),
			};
			for column in compiled.referenced_columns() {
				let column = column as usize;
				columns.push(column);
				pbc_referenced_columns.push(column);
			}
			columns.sort_unstable();
			columns.dedup();
			pbc_referenced_columns.sort_unstable();
			pbc_referenced_columns.dedup();
			Some(columns)
		} else {
			None
		};
		if runtime_filter_present {
			// Runtime-filter ordinals are source-schema ordinals. Read the filter keys
			// alongside the columns needed by PBC/output, then inflate to the source
			// schema before filtering. This preserves source-ordinal semantics without
			// forcing every lineitem column through the harness.
			let mut columns = pbc_projection.take().unwrap_or_default();
			if let Some(projected) = &projected_columns {
				columns.extend(projected.iter().map(|column| column.0 as usize));
			}
			if let Some(descriptor) = request.runtime_filter.as_ref() {
				columns.extend(
					descriptor
						.probe_key_columns
						.iter()
						.map(|column| *column as usize),
				);
			}
			columns.sort_unstable();
			columns.dedup();
			pbc_projection = Some(columns);
		}
		if pbc_projection.is_none() {
			// Keep the control arm on the same source-column/projection pipeline
			// even when there is no PBC program.
			pbc_projection = projected_columns.as_ref().map(|columns| {
				columns
					.iter()
					.map(|column| column.0 as usize)
					.collect::<Vec<_>>()
			});
		}
		let output_schema = if let Some(columns) = &projected_columns {
			let indices = columns
				.iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			Arc::new(source.schema.as_ref().project(&indices)?)
		} else {
			source.schema.clone()
		};

		let file = File::open(&source.path).map_err(|error| StorageError::Internal {
			message: format!("open parquet {}: {error}", source.path.display()),
		})?;
		let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
			StorageError::Internal {
				message: format!("read parquet metadata {}: {error}", source.path.display()),
			}
		})?;
		if let Some(columns) = &pbc_projection {
			if columns.len() < full_column_count {
				let mask = ProjectionMask::leaves(builder.parquet_schema(), columns.clone());
				builder = builder.with_projection(mask);
			}
		} else if let Some(columns) = &projected_columns {
			let leaves = columns
				.iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			let mask = ProjectionMask::leaves(builder.parquet_schema(), leaves);
			builder = builder.with_projection(mask);
		}
		let reader = builder.build().map_err(|error| StorageError::Internal {
			message: format!("open parquet reader {}: {error}", source.path.display()),
		})?;
		let full_schema = source.schema.clone();
		let scan_progress_id = next_parquet_scan_progress_id();
		let requested_projected_columns = projected_columns
			.as_ref()
			.map(|columns| columns.iter().map(|column| column.0).collect::<Vec<_>>());
		let parquet_read_columns = if let Some(columns) = &pbc_projection {
			(columns.len() < full_column_count).then_some(columns.clone())
		} else {
			projected_columns.as_ref().map(|columns| {
				columns
					.iter()
					.map(|column| column.0 as usize)
					.collect::<Vec<_>>()
			})
		};
		record_parquet_scan_progress(&serde_json::json!({
			"event": "scan_start",
			"scan_id": scan_progress_id,
			"table_id": table_id,
			"table_name": source.name.as_str(),
			"full_column_count": full_column_count,
			"requested_projected_columns": requested_projected_columns,
			"pbc_predicate_bytes": pbc.as_ref().map(Vec::len),
			"pbc_referenced_columns": pbc_referenced_columns.clone(),
			"parquet_read_columns": parquet_read_columns,
			"output_column_count": output_schema.fields().len(),
			"inflated_to_full_schema": pbc.is_some()
				&& pbc_projection
					.as_ref()
					.is_some_and(|columns| columns.len() < full_column_count),
		}));
		// The original detailed ledger materializes every batch before returning a stream. Keep that
		// forensic mode explicit; ordinary timing uses the lazy stream below so profiling cannot
		// change the scan's execution model.
		if parquet_scan_ledger_enabled()
			&& std::env::var_os("TORON_TPCH_PARQUET_SCAN_EAGER_LEDGER").is_some()
		{
			let mut ledger = ParquetScanLedger {
				table_id,
				table_name: source.name.clone(),
				full_column_count,
				requested_projected_columns: requested_projected_columns.clone(),
				pbc_predicate_bytes: pbc.as_ref().map(Vec::len),
				pbc_referenced_columns: pbc_referenced_columns.clone(),
				parquet_read_columns: parquet_read_columns.clone(),
				output_column_count: output_schema.fields().len(),
				inflated_to_full_schema: pbc.is_some()
					&& pbc_projection
						.as_ref()
						.is_some_and(|columns| columns.len() < full_column_count),
				batches: Vec::new(),
			};
			let mut batches = Vec::new();
			let mut reader = reader;
			loop {
				let decode_started = Instant::now();
				let Some(batch) = reader.next() else {
					break;
				};
				let raw_batch = batch.map_err(|error| StorageError::Internal {
					message: format!("read parquet batch {}: {error}", source.path.display()),
				})?;
				let parquet_decode_micros =
					u64::try_from(decode_started.elapsed().as_micros()).unwrap_or(u64::MAX);
				let raw_rows = raw_batch.num_rows();
				let raw_columns = raw_batch.num_columns();
				let raw_bytes = record_batch_bytes(&raw_batch);
				let mut inflated_bytes = None;
				let output_batch = {
					let source_batch = if let Some(columns) = &pbc_projection {
						let inflated = inflate_projected_parquet_batch(
							raw_batch,
							full_schema.clone(),
							columns,
						)?;
						if columns.len() < full_column_count {
							inflated_bytes = Some(record_batch_bytes(&inflated));
						}
						inflated
					} else {
						raw_batch
					};
					let source_batch = if let Some(descriptor) = request.runtime_filter.as_ref() {
						apply_harness_runtime_filter(source_batch, descriptor)?
					} else {
						source_batch
					};
					if let Some(program) = &pbc {
						let filtered = wendelon::schema::pbc::execute(program, &source_batch)
							.map_err(|error| StorageError::InvalidPbcProgram {
								message: format!("{error:?}"),
							})?;
						let columns = projected_columns
							.as_ref()
							.expect("projection for persisted scan");
						filtered.project(
							&columns
								.iter()
								.map(|column| column.0 as usize)
								.collect::<Vec<_>>(),
						)?
					} else if runtime_filter_present {
						let columns = projected_columns
							.as_ref()
							.expect("projection when filtering");
						source_batch.project(
							&columns
								.iter()
								.map(|column| column.0 as usize)
								.collect::<Vec<_>>(),
						)?
					} else if let Some(columns) = &projected_columns {
						source_batch.project(
							&columns
								.iter()
								.map(|column| column.0 as usize)
								.collect::<Vec<_>>(),
						)?
					} else {
						source_batch
					}
				};
				ledger.batches.push(ParquetScanBatchLedger {
					parquet_decode_micros,
					raw_rows,
					raw_columns,
					raw_bytes,
					inflated_bytes,
					output_rows: output_batch.num_rows(),
					output_columns: output_batch.num_columns(),
					output_bytes: record_batch_bytes(&output_batch),
				});
				record_parquet_scan_progress(&serde_json::json!({
					"event": "batch",
					"scan_id": scan_progress_id,
					"table_id": table_id,
					"table_name": source.name.as_str(),
					"batch_index": ledger.batches.len() - 1,
					"raw_rows": raw_rows,
					"raw_columns": raw_columns,
					"raw_bytes": raw_bytes,
					"inflated_bytes": inflated_bytes,
					"output_rows": output_batch.num_rows(),
					"output_columns": output_batch.num_columns(),
					"output_bytes": record_batch_bytes(&output_batch),
				}));
				batches.push(output_batch);
			}
			record_parquet_scan_progress(&serde_json::json!({
				"event": "scan_end",
				"scan_id": scan_progress_id,
				"table_id": table_id,
				"table_name": source.name.as_str(),
				"batch_count": ledger.batches.len(),
			}));
			record_parquet_scan_ledger(ledger);
			return Ok(ScanStream::new(
				output_schema,
				request.session_id,
				request.query_id,
				stream::iter(batches.into_iter().map(Ok)).boxed(),
			));
		}
		let mut batch_index = 0usize;
		let lazy_ledger_enabled = parquet_scan_ledger_enabled();
		let lazy_output_column_count = output_schema.fields().len();
		// `Iterator::map` invokes the parquet reader before it enters its closure. Wrap `next()`
		// instead, so the lazy receipt measures the real decode boundary without materializing.
		let mut reader = reader;
		let timed_reader = std::iter::from_fn(move || {
			let started = lazy_ledger_enabled.then(Instant::now);
			reader.next().map(|batch| {
				(
					started.map(|started| {
						u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
					}),
					batch,
				)
			})
		});
		let stream = stream::iter(timed_reader.map(move |(parquet_decode_micros, batch)| {
			let batch = batch.map_err(|error| StorageError::Internal {
				message: format!("read parquet batch {}: {error}", source.path.display()),
			})?;
			let raw_rows = batch.num_rows();
			let raw_columns = batch.num_columns();
			let raw_bytes = record_batch_bytes(&batch);
			let batch = if let Some(program) = &pbc {
				let batch = if let Some(columns) = &pbc_projection {
					inflate_projected_parquet_batch(batch, full_schema.clone(), columns)?
				} else {
					batch
				};
				let batch = if let Some(descriptor) = request.runtime_filter.as_ref() {
					apply_harness_runtime_filter(batch, descriptor)?
				} else {
					batch
				};
				let filtered =
					wendelon::schema::pbc::execute(program, &batch).map_err(|error| {
						StorageError::InvalidPbcProgram {
							message: format!("{error:?}"),
						}
					})?;
				let columns = projected_columns
					.as_ref()
					.expect("projection for persisted scan");
				filtered.project(
					&columns
						.iter()
						.map(|column| column.0 as usize)
						.collect::<Vec<_>>(),
				)?
			} else {
				let batch = if let Some(columns) = &pbc_projection {
					inflate_projected_parquet_batch(batch, full_schema.clone(), columns)?
				} else {
					batch
				};
				let batch = if let Some(descriptor) = request.runtime_filter.as_ref() {
					apply_harness_runtime_filter(batch, descriptor)?
				} else {
					batch
				};
				if let Some(columns) = &projected_columns {
					batch.project(
						&columns
							.iter()
							.map(|column| column.0 as usize)
							.collect::<Vec<_>>(),
					)?
				} else {
					batch
				}
			};
			record_parquet_scan_progress(&serde_json::json!({
				"event": "batch",
				"scan_id": scan_progress_id,
				"table_id": table_id,
				"table_name": source.name.as_str(),
				"batch_index": batch_index,
				"raw_rows": raw_rows,
				"raw_columns": raw_columns,
				"raw_bytes": raw_bytes,
				"inflated_bytes": null,
				"output_rows": batch.num_rows(),
				"output_columns": batch.num_columns(),
				"output_bytes": record_batch_bytes(&batch),
			}));
			batch_index += 1;
			if let Some(parquet_decode_micros) = parquet_decode_micros {
				record_parquet_scan_ledger(ParquetScanLedger {
					table_id,
					table_name: source.name.clone(),
					full_column_count,
					requested_projected_columns: requested_projected_columns.clone(),
					pbc_predicate_bytes: pbc.as_ref().map(Vec::len),
					pbc_referenced_columns: pbc_referenced_columns.clone(),
					parquet_read_columns: parquet_read_columns.clone(),
					output_column_count: lazy_output_column_count,
					inflated_to_full_schema: false,
					batches: vec![ParquetScanBatchLedger {
						parquet_decode_micros,
						raw_rows,
						raw_columns,
						raw_bytes,
						inflated_bytes: None,
						output_rows: batch.num_rows(),
						output_columns: batch.num_columns(),
						output_bytes: record_batch_bytes(&batch),
					}],
				});
			}
			Ok(batch)
		}))
		.boxed();

		Ok(ScanStream::new(
			output_schema,
			request.session_id,
			request.query_id,
			stream,
		))
	}
}

fn inflate_projected_parquet_batch(
	batch: RecordBatch,
	full_schema: SchemaRef,
	columns: &[usize],
) -> Result<RecordBatch, StorageError> {
	if columns.len() == full_schema.fields().len() {
		return Ok(batch);
	}
	let mut arrays = Vec::with_capacity(full_schema.fields().len());
	let mut fields = Vec::with_capacity(full_schema.fields().len());
	let mut projected_index = 0;
	for column in 0..full_schema.fields().len() {
		if projected_index < columns.len() && columns[projected_index] == column {
			fields.push(full_schema.field(column).as_ref().clone());
			arrays.push(batch.column(projected_index).clone());
			projected_index += 1;
		} else {
			fields.push(
				full_schema
					.field(column)
					.as_ref()
					.clone()
					.with_nullable(true),
			);
			arrays.push(new_null_array(
				full_schema.field(column).data_type(),
				batch.num_rows(),
			));
		}
	}
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(StorageError::from)
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

/// The exact SQL + generated table fixtures used by the perf rail for one query/mode/scale.
pub struct PerfQuerySpec {
	pub query_id: &'static str,
	pub sql: Cow<'static, str>,
	pub tables: Vec<HarnessTable>,
}

#[derive(Debug, Clone)]
pub struct TpchParquetTableExport {
	pub name: String,
	pub path: PathBuf,
	pub row_count: u64,
	pub schema_fingerprint: String,
	pub schema_signature: String,
	pub file_hash: String,
}

#[derive(Debug, Clone)]
pub struct TpchParquetExport {
	pub query_id: String,
	pub mode: String,
	pub scale_factor: f64,
	pub sql: String,
	pub root_dir: PathBuf,
	pub manifest_path: PathBuf,
	pub manifest_fingerprint: String,
	pub tables: Vec<TpchParquetTableExport>,
}

#[derive(Debug, Clone)]
pub struct TpchPersistedTable {
	pub name: String,
	pub path: PathBuf,
	pub row_count: u64,
	pub logical_bytes: u64,
	pub schema_fingerprint: String,
	pub schema_signature: String,
	pub file_hash: String,
}

#[derive(Debug, Clone)]
pub struct TpchPersistedDataset {
	pub mode: String,
	pub scale_factor: f64,
	pub root_dir: PathBuf,
	pub manifest_path: PathBuf,
	pub manifest_fingerprint: String,
	pub tables: BTreeMap<String, TpchPersistedTable>,
}

/// A query table registration decoupled from the in-memory fixture batch. Persisted scans seed the same
/// catalog descriptors/facts as [`HarnessTable`] without constructing a full `RecordBatch`.
#[derive(Debug, Clone)]
pub struct HarnessTableMetadata {
	pub id: TableId,
	pub name: &'static str,
	pub descriptor: TableSchemaDescriptor,
	pub facts: TableOptimizerFacts,
}

pub struct PersistedPerfQuerySpec {
	pub query_id: &'static str,
	pub sql: Cow<'static, str>,
	pub tables: Vec<HarnessTableMetadata>,
	pub storage: Arc<ParquetHarnessScan>,
	pub export: TpchParquetExport,
}

pub fn stable_fingerprint(text: &str) -> String {
	let mut hash = FNV_OFFSET;
	for byte in text.as_bytes() {
		hash = fnv1a_update(hash, &[*byte]);
	}
	format!("fnv1a64:{hash:016x}")
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001b3;

fn fnv1a_update(mut hash: u64, bytes: &[u8]) -> u64 {
	for byte in bytes {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(FNV_PRIME);
	}
	hash
}

fn stable_file_hash(path: &Path) -> Result<String, String> {
	let file = File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
	let mut reader = BufReader::new(file);
	let mut buffer = [0_u8; 64 * 1024];
	let mut hash = FNV_OFFSET;
	loop {
		let read = reader
			.read(&mut buffer)
			.map_err(|error| format!("read {}: {error}", path.display()))?;
		if read == 0 {
			break;
		}
		hash = fnv1a_update(hash, &buffer[..read]);
	}
	Ok(format!("fnv1a64:{hash:016x}"))
}

fn dataset_sf_dir_name(scale_factor: f64) -> String {
	format!("sf{}", scale_factor.to_string().replace('.', "_"))
}

fn persisted_dataset_dir(root: &Path, mode: &str, scale_factor: f64) -> PathBuf {
	root.join(mode).join(dataset_sf_dir_name(scale_factor))
}

fn table_schema_for_name(name: &str, strict: bool) -> Result<SchemaRef, String> {
	match name {
		"lineitem" if strict => Ok(strict_lineitem_schema()),
		"lineitem" => Ok(dev_lineitem_schema()),
		"part" => Ok(part_schema()),
		"customer" => Ok(customer_schema(strict)),
		"orders" => Ok(orders_schema(strict)),
		"nation" => Ok(nation_schema()),
		"supplier" => Ok(supplier_schema(strict)),
		"partsupp" => Ok(partsupp_schema(strict)),
		"region" => Ok(region_schema()),
		other => Err(format!("unknown TPC-H table {other:?}")),
	}
}

fn table_descriptor_for_name(name: &str, strict: bool) -> Result<TableSchemaDescriptor, String> {
	let schema = table_schema_for_name(name, strict)?;
	match name {
		"lineitem" if strict => Ok(strict_lineitem_descriptor(schema.as_ref())),
		"lineitem" => Ok(dev_lineitem_descriptor(schema.as_ref())),
		"part" => Ok(part_descriptor(schema.as_ref())),
		"customer" => Ok(customer_descriptor(schema.as_ref())),
		"orders" => Ok(orders_descriptor(schema.as_ref())),
		"nation" => Ok(nation_descriptor(schema.as_ref())),
		"supplier" => Ok(supplier_descriptor(schema.as_ref())),
		"partsupp" => Ok(partsupp_descriptor(schema.as_ref())),
		"region" => Ok(region_descriptor(schema.as_ref())),
		other => Err(format!("unknown TPC-H table {other:?}")),
	}
}

fn table_facts_from_manifest(table: &TpchPersistedTable) -> TableOptimizerFacts {
	TableOptimizerFacts {
		row_upper_bound: Some(RowCount::new(table.row_count)),
		byte_upper_bound: Some(ByteCount::new(table.logical_bytes)),
		// ADR 471: footer-derived column ranges feed filter selectivity, which
		// the placement comparison scales by. Metadata-only; ranges that fail to
		// derive are simply absent (an absent range means an unscaled bound,
		// never a guess).
		column_range: column_ranges_from_parquet_footer(&table.path),
		..Default::default()
	}
}

/// Column min/max facts from parquet row-group footer statistics — no data
/// scan. Only types the selectivity decoder understands are emitted
/// (INT32/INT64/DATE32/FLOAT32/FLOAT64); everything else is skipped.
fn column_ranges_from_parquet_footer(
	path: &std::path::Path,
) -> BTreeMap<toron::ir::ColumnOrdinal, toron::catalog::RangeFact> {
	use parquet::file::statistics::Statistics;
	use wendelon::storage::stats::{
		ARROW_STATS_TAG_DATE32, ARROW_STATS_TAG_FLOAT32, ARROW_STATS_TAG_FLOAT64,
		ARROW_STATS_TAG_INT32, ARROW_STATS_TAG_INT64,
	};
	use wendelon::storage::stats::{
		encode_float32_stat_value, encode_float64_stat_value, encode_signed_stat_value,
	};
	let Ok(file) = std::fs::File::open(path) else {
		return BTreeMap::new();
	};
	let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) else {
		return BTreeMap::new();
	};
	let arrow_schema = builder.schema().clone();
	let metadata = builder.metadata().clone();
	let mut facts = BTreeMap::new();
	for (ordinal, field) in arrow_schema.fields().iter().enumerate() {
		let tag = match field.data_type() {
			DataType::Int32 => ARROW_STATS_TAG_INT32,
			DataType::Int64 => ARROW_STATS_TAG_INT64,
			DataType::Date32 => ARROW_STATS_TAG_DATE32,
			DataType::Float32 => ARROW_STATS_TAG_FLOAT32,
			DataType::Float64 => ARROW_STATS_TAG_FLOAT64,
			_ => continue,
		};
		let mut min_i: Option<i64> = None;
		let mut max_i: Option<i64> = None;
		let mut min_f: Option<f64> = None;
		let mut max_f: Option<f64> = None;
		let mut null_count = 0u64;
		let mut rows = 0u64;
		let mut complete = true;
		for group in metadata.row_groups() {
			rows = rows.saturating_add(group.num_rows().max(0) as u64);
			let column = group.column(ordinal);
			let Some(stats) = column.statistics() else {
				complete = false;
				break;
			};
			null_count = null_count.saturating_add(stats.null_count_opt().unwrap_or(0));
			match stats {
				Statistics::Int32(typed) => {
					let (Some(lo), Some(hi)) = (typed.min_opt(), typed.max_opt()) else {
						complete = false;
						break;
					};
					min_i = Some(min_i.map_or(*lo as i64, |m| m.min(*lo as i64)));
					max_i = Some(max_i.map_or(*hi as i64, |m| m.max(*hi as i64)));
				}
				Statistics::Int64(typed) => {
					let (Some(lo), Some(hi)) = (typed.min_opt(), typed.max_opt()) else {
						complete = false;
						break;
					};
					min_i = Some(min_i.map_or(*lo, |m| m.min(*lo)));
					max_i = Some(max_i.map_or(*hi, |m| m.max(*hi)));
				}
				Statistics::Float(typed) => {
					let (Some(lo), Some(hi)) = (typed.min_opt(), typed.max_opt()) else {
						complete = false;
						break;
					};
					min_f = Some(min_f.map_or(*lo as f64, |m| m.min(*lo as f64)));
					max_f = Some(max_f.map_or(*hi as f64, |m| m.max(*hi as f64)));
				}
				Statistics::Double(typed) => {
					let (Some(lo), Some(hi)) = (typed.min_opt(), typed.max_opt()) else {
						complete = false;
						break;
					};
					min_f = Some(min_f.map_or(*lo, |m| m.min(*lo)));
					max_f = Some(max_f.map_or(*hi, |m| m.max(*hi)));
				}
				_ => {
					complete = false;
					break;
				}
			}
		}
		if !complete {
			continue;
		}
		let width = if tag == ARROW_STATS_TAG_INT64 || tag == ARROW_STATS_TAG_FLOAT64 {
			8
		} else {
			4
		};
		let encoded = |i: Option<i64>, f: Option<f64>| -> Option<Vec<u8>> {
			if tag == ARROW_STATS_TAG_FLOAT32 {
				encode_float32_stat_value(f? as f32).ok()
			} else if tag == ARROW_STATS_TAG_FLOAT64 {
				encode_float64_stat_value(f?).ok()
			} else {
				encode_signed_stat_value(i?, width).ok()
			}
		};
		let (Some(min), Some(max)) = (encoded(min_i, min_f), encoded(max_i, max_f)) else {
			continue;
		};
		facts.insert(
			toron::ir::ColumnOrdinal(ordinal as u32),
			toron::catalog::RangeFact {
				arrow_type_tag: tag,
				null_count,
				non_null_count: rows.saturating_sub(null_count),
				min: Some(min),
				max: Some(max),
			},
		);
	}
	facts
}

fn persisted_table_metadata(
	dataset: &TpchPersistedDataset,
	strict: bool,
	id: TableId,
	name: &'static str,
) -> Result<HarnessTableMetadata, String> {
	let table = dataset
		.tables
		.get(name)
		.ok_or_else(|| format!("persisted dataset is missing table {name}"))?;
	Ok(HarnessTableMetadata {
		id,
		name,
		descriptor: table_descriptor_for_name(name, strict)?,
		facts: table_facts_from_manifest(table),
	})
}

fn persisted_query_tables(
	dataset: &TpchPersistedDataset,
	strict: bool,
	tables: &[(u64, &'static str)],
) -> Result<Vec<HarnessTableMetadata>, String> {
	tables
		.iter()
		.map(|(id, name)| persisted_table_metadata(dataset, strict, TableId(*id), name))
		.collect()
}

fn persisted_query_table_names(query: &str) -> Result<Vec<(u64, &'static str)>, String> {
	let tables = match query {
		"m00" | "m01" | "m02" | "m03" | "m04" | "m05" | "m07" | "m08" | "m09" | "m10" => {
			vec![(1, "lineitem")]
		}
		"m06" => vec![(1, "lineitem"), (2, "orders")],
		"q01" | "q1" | "q06" | "q6" => vec![(1, "lineitem")],
		"q02" | "q2" => vec![
			(1, "part"),
			(2, "supplier"),
			(3, "partsupp"),
			(4, "nation"),
			(5, "region"),
		],
		"q03" | "q3" | "q18" => vec![(1, "lineitem"), (2, "customer"), (3, "orders")],
		"q04" | "q4" | "q12" => vec![(1, "lineitem"), (3, "orders")],
		"q05" | "q5" => vec![
			(1, "lineitem"),
			(2, "customer"),
			(3, "orders"),
			(4, "nation"),
			(5, "supplier"),
			(6, "region"),
		],
		"q07" | "q7" => vec![
			(1, "lineitem"),
			(2, "customer"),
			(3, "orders"),
			(4, "nation"),
			(5, "supplier"),
		],
		"q08" | "q8" => vec![
			(1, "lineitem"),
			(2, "customer"),
			(3, "orders"),
			(4, "nation"),
			(5, "supplier"),
			(6, "region"),
			(7, "part"),
		],
		"q09" | "q9" => vec![
			(1, "part"),
			(2, "supplier"),
			(3, "lineitem"),
			(4, "partsupp"),
			(5, "orders"),
			(6, "nation"),
		],
		"q10" => vec![
			(1, "lineitem"),
			(2, "customer"),
			(3, "orders"),
			(4, "nation"),
		],
		"q11" => vec![(1, "partsupp"), (2, "supplier"), (3, "nation")],
		"q13" | "q22" => vec![(1, "customer"), (2, "orders")],
		"q14" | "q17" | "q19" => vec![(1, "lineitem"), (2, "part")],
		"q15" => vec![(1, "supplier"), (2, "lineitem")],
		"q16" => vec![(1, "part"), (2, "partsupp"), (3, "supplier")],
		"q20" => vec![
			(1, "supplier"),
			(2, "nation"),
			(3, "partsupp"),
			(4, "part"),
			(5, "lineitem"),
		],
		"q21" => vec![
			(1, "supplier"),
			(2, "lineitem"),
			(3, "orders"),
			(4, "nation"),
		],
		other => return Err(format!("perf --query {other} is not supported")),
	};
	Ok(tables)
}

fn manifest_signature_for_tables(
	mode: &str,
	scale_factor: f64,
	tables: &BTreeMap<String, TpchPersistedTable>,
) -> String {
	let table_signature = tables
		.values()
		.map(|table| {
			format!(
				"{}:{}:{}:{}:{}",
				table.name,
				table.row_count,
				table.logical_bytes,
				table.schema_fingerprint,
				table.file_hash
			)
		})
		.collect::<Vec<_>>()
		.join("|");
	format!("suite=tpch_persisted_dataset;mode={mode};sf={scale_factor};tables={table_signature}")
}

fn manifest_fingerprint_for_tables(
	mode: &str,
	scale_factor: f64,
	tables: &BTreeMap<String, TpchPersistedTable>,
) -> String {
	stable_fingerprint(&manifest_signature_for_tables(mode, scale_factor, tables))
}

pub fn load_persisted_tpch_dataset(
	mode: &str,
	scale_factor: f64,
	root: &Path,
) -> Result<TpchPersistedDataset, String> {
	let root_dir = persisted_dataset_dir(root, mode, scale_factor);
	let manifest_path = root_dir.join("manifest.json");
	let body = std::fs::read_to_string(&manifest_path)
		.map_err(|error| format!("read {}: {error}", manifest_path.display()))?;
	let value: serde_json::Value = serde_json::from_str(&body)
		.map_err(|error| format!("parse {}: {error}", manifest_path.display()))?;
	if value.get("suite").and_then(serde_json::Value::as_str) != Some("tpch_persisted_dataset") {
		return Err(format!(
			"{} is not a persisted TPC-H manifest",
			manifest_path.display()
		));
	}
	let manifest_mode = value
		.get("mode")
		.and_then(serde_json::Value::as_str)
		.ok_or_else(|| format!("{} missing mode", manifest_path.display()))?;
	if manifest_mode != mode {
		return Err(format!(
			"{} mode mismatch: manifest={manifest_mode:?} requested={mode:?}",
			manifest_path.display()
		));
	}
	let manifest_sf = value
		.get("scale_factor")
		.and_then(serde_json::Value::as_f64)
		.ok_or_else(|| format!("{} missing scale_factor", manifest_path.display()))?;
	if (manifest_sf - scale_factor).abs() > f64::EPSILON {
		return Err(format!(
			"{} scale mismatch: manifest={manifest_sf} requested={scale_factor}",
			manifest_path.display()
		));
	}

	let mut tables = BTreeMap::new();
	let table_values = value
		.get("tables")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| format!("{} missing tables", manifest_path.display()))?;
	for table in table_values {
		let name = table
			.get("name")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("{} table missing name", manifest_path.display()))?
			.to_string();
		let path = table
			.get("path")
			.and_then(serde_json::Value::as_str)
			.map(PathBuf::from)
			.ok_or_else(|| format!("{} table {name} missing path", manifest_path.display()))?;
		let row_count = table
			.get("row_count")
			.and_then(serde_json::Value::as_u64)
			.ok_or_else(|| format!("{} table {name} missing row_count", manifest_path.display()))?;
		let logical_bytes = table
			.get("logical_bytes")
			.and_then(serde_json::Value::as_u64)
			.ok_or_else(|| {
				format!(
					"{} table {name} missing logical_bytes",
					manifest_path.display()
				)
			})?;
		let schema_fingerprint = table
			.get("schema_fingerprint")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| {
				format!(
					"{} table {name} missing schema_fingerprint",
					manifest_path.display()
				)
			})?
			.to_string();
		let schema_signature = table
			.get("schema_signature")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| {
				format!(
					"{} table {name} missing schema_signature",
					manifest_path.display()
				)
			})?
			.to_string();
		let file_hash = table
			.get("file_hash")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| format!("{} table {name} missing file_hash", manifest_path.display()))?
			.to_string();
		if !path.exists() {
			return Err(format!(
				"manifest table {name} path does not exist: {}",
				path.display()
			));
		}
		let observed_hash = stable_file_hash(&path)?;
		if observed_hash != file_hash {
			return Err(format!(
				"manifest table {name} hash mismatch: manifest={file_hash} observed={observed_hash}"
			));
		}
		tables.insert(
			name.clone(),
			TpchPersistedTable {
				name,
				path,
				row_count,
				logical_bytes,
				schema_fingerprint,
				schema_signature,
				file_hash,
			},
		);
	}
	let manifest_fingerprint = manifest_fingerprint_for_tables(mode, scale_factor, &tables);
	let recorded = value
		.get("manifest_fingerprint")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("");
	if recorded != manifest_fingerprint {
		return Err(format!(
			"{} manifest fingerprint mismatch: manifest={recorded} observed={manifest_fingerprint}",
			manifest_path.display()
		));
	}
	Ok(TpchPersistedDataset {
		mode: mode.to_string(),
		scale_factor,
		root_dir,
		manifest_path,
		manifest_fingerprint,
		tables,
	})
}

pub fn prepare_persisted_tpch_dataset(
	mode: &str,
	scale_factor: f64,
	root: &Path,
) -> Result<TpchPersistedDataset, String> {
	match mode {
		"strict" | "dev" => {}
		other => return Err(format!("persisted data mode {other:?} is not supported")),
	}
	if let Ok(dataset) = load_persisted_tpch_dataset(mode, scale_factor, root) {
		return Ok(dataset);
	}
	let strict = mode == "strict";
	let root_dir = persisted_dataset_dir(root, mode, scale_factor);
	std::fs::create_dir_all(&root_dir)
		.map_err(|error| format!("create {}: {error}", root_dir.display()))?;

	let table_names = [
		"lineitem", "part", "customer", "orders", "nation", "supplier", "partsupp", "region",
	];
	let mut tables = BTreeMap::new();
	for name in table_names {
		let path = root_dir.join(format!("{name}.parquet"));
		let schema = table_schema_for_name(name, strict)?;
		let stats = write_persisted_table(name, strict, scale_factor, schema.clone(), &path)?;
		let signature = schema_signature(schema.as_ref());
		tables.insert(
			name.to_string(),
			TpchPersistedTable {
				name: name.to_string(),
				path: path.clone(),
				row_count: stats.row_count,
				logical_bytes: stats.logical_bytes,
				schema_fingerprint: stable_fingerprint(&signature),
				schema_signature: signature,
				file_hash: stable_file_hash(&path)?,
			},
		);
	}

	let manifest_fingerprint = manifest_fingerprint_for_tables(mode, scale_factor, &tables);
	let manifest_path = root_dir.join("manifest.json");
	let manifest_tables = tables
		.values()
		.map(|table| {
			serde_json::json!({
				"name": table.name,
				"path": table.path.display().to_string(),
				"row_count": table.row_count,
				"logical_bytes": table.logical_bytes,
				"schema_fingerprint": table.schema_fingerprint,
				"schema_signature": table.schema_signature,
				"file_hash": table.file_hash,
			})
		})
		.collect::<Vec<_>>();
	let manifest = serde_json::json!({
		"suite": "tpch_persisted_dataset",
		"mode": mode,
		"scale_factor": scale_factor,
		"manifest_fingerprint": manifest_fingerprint,
		"chunk_rows": TPCH_PERSISTED_CHUNK_ROWS,
		"tables": manifest_tables,
	});
	let body = serde_json::to_string_pretty(&manifest)
		.map_err(|error| format!("encode {}: {error}", manifest_path.display()))?;
	std::fs::write(&manifest_path, format!("{body}\n"))
		.map_err(|error| format!("write {}: {error}", manifest_path.display()))?;
	Ok(TpchPersistedDataset {
		mode: mode.to_string(),
		scale_factor,
		root_dir,
		manifest_path,
		manifest_fingerprint,
		tables,
	})
}

#[derive(Debug, Clone, Copy, Default)]
struct PersistedWriteStats {
	row_count: u64,
	logical_bytes: u64,
}

impl PersistedWriteStats {
	fn add_batch(&mut self, batch: &RecordBatch) {
		self.row_count += batch.num_rows() as u64;
		self.logical_bytes += batch.get_array_memory_size() as u64;
	}
}

fn decimal_array(values: Vec<i128>) -> ArrayRef {
	Arc::new(
		Decimal128Array::from(values)
			.with_precision_and_scale(15, 2)
			.expect("decimal128 column"),
	)
}

fn parquet_writer(path: &Path, schema: SchemaRef) -> Result<ArrowWriter<File>, String> {
	let file = File::create(path).map_err(|error| format!("create {}: {error}", path.display()))?;
	let properties = WriterProperties::builder()
		.set_max_row_group_row_count(Some(TPCH_PERSISTED_CHUNK_ROWS))
		.build();
	ArrowWriter::try_new(file, schema, Some(properties))
		.map_err(|error| format!("create parquet writer for {}: {error}", path.display()))
}

fn write_batch(
	writer: &mut ArrowWriter<File>,
	path: &Path,
	batch: &RecordBatch,
	stats: &mut PersistedWriteStats,
) -> Result<(), String> {
	stats.add_batch(batch);
	writer
		.write(batch)
		.map_err(|error| format!("write parquet {}: {error}", path.display()))
}

fn write_persisted_table(
	name: &str,
	strict: bool,
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	match name {
		"lineitem" => write_lineitem_parquet(strict, scale_factor, schema, path),
		"part" => write_part_parquet(scale_factor, schema, path),
		"customer" => write_customer_parquet(strict, scale_factor, schema, path),
		"orders" => write_orders_parquet(strict, scale_factor, schema, path),
		"nation" => write_nation_parquet(scale_factor, schema, path),
		"supplier" => write_supplier_parquet(strict, scale_factor, schema, path),
		"partsupp" => write_partsupp_parquet(strict, scale_factor, schema, path),
		"region" => write_region_parquet(scale_factor, schema, path),
		other => Err(format!("cannot persist unknown TPC-H table {other:?}")),
	}
}

struct LineitemChunk {
	quantity_dev: Vec<f64>,
	quantity_strict: Vec<i128>,
	extendedprice_dev: Vec<f64>,
	extendedprice_strict: Vec<i128>,
	discount_dev: Vec<f64>,
	discount_strict: Vec<i128>,
	shipdate: Vec<i32>,
	returnflag: Vec<String>,
	linestatus: Vec<String>,
	tax_dev: Vec<f64>,
	tax_strict: Vec<i128>,
	partkey: Vec<i64>,
	orderkey: Vec<i64>,
	suppkey: Vec<i64>,
	commitdate: Vec<i32>,
	receiptdate: Vec<i32>,
	shipmode: Vec<String>,
	shipinstruct: Vec<String>,
}

impl LineitemChunk {
	fn new() -> Self {
		Self {
			quantity_dev: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			quantity_strict: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			extendedprice_dev: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			extendedprice_strict: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			discount_dev: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			discount_strict: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			shipdate: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			returnflag: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			linestatus: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			tax_dev: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			tax_strict: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			partkey: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			orderkey: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			suppkey: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			commitdate: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			receiptdate: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			shipmode: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
			shipinstruct: Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS),
		}
	}

	fn len(&self) -> usize {
		self.shipdate.len()
	}

	fn is_empty(&self) -> bool {
		self.len() == 0
	}

	fn push(&mut self, item: tpchgen::generators::LineItem<'_>, strict: bool) {
		if strict {
			self.quantity_strict.push(item.l_quantity as i128 * 100);
			self.extendedprice_strict
				.push(item.l_extendedprice.into_inner() as i128);
			self.discount_strict
				.push(item.l_discount.into_inner() as i128);
			self.tax_strict.push(item.l_tax.into_inner() as i128);
		} else {
			self.quantity_dev.push(item.l_quantity as f64);
			self.extendedprice_dev.push(item.l_extendedprice.as_f64());
			self.discount_dev.push(item.l_discount.as_f64());
			self.tax_dev.push(item.l_tax.as_f64());
		}
		self.shipdate.push(item.l_shipdate.to_unix_epoch());
		self.returnflag.push(item.l_returnflag.to_string());
		self.linestatus.push(item.l_linestatus.to_string());
		self.partkey.push(item.l_partkey);
		self.orderkey.push(item.l_orderkey);
		self.suppkey.push(item.l_suppkey);
		self.commitdate.push(item.l_commitdate.to_unix_epoch());
		self.receiptdate.push(item.l_receiptdate.to_unix_epoch());
		self.shipmode.push(item.l_shipmode.to_string());
		self.shipinstruct.push(item.l_shipinstruct.to_string());
	}

	fn flush(
		&mut self,
		strict: bool,
		schema: SchemaRef,
		writer: &mut ArrowWriter<File>,
		path: &Path,
		stats: &mut PersistedWriteStats,
	) -> Result<(), String> {
		if self.is_empty() {
			return Ok(());
		}
		let quantity: ArrayRef = if strict {
			decimal_array(std::mem::take(&mut self.quantity_strict))
		} else {
			Arc::new(Float64Array::from(std::mem::take(&mut self.quantity_dev)))
		};
		let extendedprice: ArrayRef = if strict {
			decimal_array(std::mem::take(&mut self.extendedprice_strict))
		} else {
			Arc::new(Float64Array::from(std::mem::take(
				&mut self.extendedprice_dev,
			)))
		};
		let discount: ArrayRef = if strict {
			decimal_array(std::mem::take(&mut self.discount_strict))
		} else {
			Arc::new(Float64Array::from(std::mem::take(&mut self.discount_dev)))
		};
		let tax: ArrayRef = if strict {
			decimal_array(std::mem::take(&mut self.tax_strict))
		} else {
			Arc::new(Float64Array::from(std::mem::take(&mut self.tax_dev)))
		};
		let batch = RecordBatch::try_new(
			schema,
			vec![
				quantity,
				extendedprice,
				discount,
				Arc::new(Date32Array::from(std::mem::take(&mut self.shipdate))),
				Arc::new(StringArray::from(std::mem::take(&mut self.returnflag))),
				Arc::new(StringArray::from(std::mem::take(&mut self.linestatus))),
				tax,
				Arc::new(Int64Array::from(std::mem::take(&mut self.partkey))),
				Arc::new(Int64Array::from(std::mem::take(&mut self.orderkey))),
				Arc::new(Int64Array::from(std::mem::take(&mut self.suppkey))),
				Arc::new(Date32Array::from(std::mem::take(&mut self.commitdate))),
				Arc::new(Date32Array::from(std::mem::take(&mut self.receiptdate))),
				Arc::new(StringArray::from(std::mem::take(&mut self.shipmode))),
				Arc::new(StringArray::from(std::mem::take(&mut self.shipinstruct))),
			],
		)
		.map_err(|error| format!("build persisted lineitem batch: {error}"))?;
		write_batch(writer, path, &batch, stats)
	}
}

fn write_lineitem_parquet(
	strict: bool,
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut chunk = LineitemChunk::new();
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		chunk.push(item, strict);
		if chunk.len() >= TPCH_PERSISTED_CHUNK_ROWS {
			chunk.flush(strict, schema.clone(), &mut writer, path, &mut stats)?;
		}
	}
	chunk.flush(strict, schema, &mut writer, path, &mut stats)?;
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_part_parquet(
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut partkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut part_type = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut brand = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut container = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut size = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut mfgr = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut name = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in PartGenerator::new(scale_factor, 1, 1).iter() {
		partkey.push(item.p_partkey);
		part_type.push(item.p_type.to_string());
		brand.push(item.p_brand.to_string());
		container.push(item.p_container.to_string());
		size.push(item.p_size);
		mfgr.push(item.p_mfgr.to_string());
		name.push(item.p_name.to_string());
		if partkey.len() >= TPCH_PERSISTED_CHUNK_ROWS {
			let batch = RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from(std::mem::take(&mut partkey))),
					Arc::new(StringArray::from(std::mem::take(&mut part_type))),
					Arc::new(StringArray::from(std::mem::take(&mut brand))),
					Arc::new(StringArray::from(std::mem::take(&mut container))),
					Arc::new(Int32Array::from(std::mem::take(&mut size))),
					Arc::new(StringArray::from(std::mem::take(&mut mfgr))),
					Arc::new(StringArray::from(std::mem::take(&mut name))),
				],
			)
			.map_err(|error| format!("build persisted part batch: {error}"))?;
			write_batch(&mut writer, path, &batch, &mut stats)?;
		}
	}
	if !partkey.is_empty() {
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(partkey)),
				Arc::new(StringArray::from(part_type)),
				Arc::new(StringArray::from(brand)),
				Arc::new(StringArray::from(container)),
				Arc::new(Int32Array::from(size)),
				Arc::new(StringArray::from(mfgr)),
				Arc::new(StringArray::from(name)),
			],
		)
		.map_err(|error| format!("build persisted part batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_customer_parquet(
	strict: bool,
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut custkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut mktsegment = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut name = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut acctbal_dev = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut acctbal_strict = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut address = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut phone = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut comment = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut nationkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in CustomerGenerator::new(scale_factor, 1, 1).iter() {
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
		if custkey.len() >= TPCH_PERSISTED_CHUNK_ROWS {
			let acctbal: ArrayRef = if strict {
				decimal_array(std::mem::take(&mut acctbal_strict))
			} else {
				Arc::new(Float64Array::from(std::mem::take(&mut acctbal_dev)))
			};
			let batch = RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from(std::mem::take(&mut custkey))),
					Arc::new(StringArray::from(std::mem::take(&mut mktsegment))),
					Arc::new(StringArray::from(std::mem::take(&mut name))),
					acctbal,
					Arc::new(StringArray::from(std::mem::take(&mut address))),
					Arc::new(StringArray::from(std::mem::take(&mut phone))),
					Arc::new(StringArray::from(std::mem::take(&mut comment))),
					Arc::new(Int64Array::from(std::mem::take(&mut nationkey))),
				],
			)
			.map_err(|error| format!("build persisted customer batch: {error}"))?;
			write_batch(&mut writer, path, &batch, &mut stats)?;
		}
	}
	if !custkey.is_empty() {
		let acctbal: ArrayRef = if strict {
			decimal_array(acctbal_strict)
		} else {
			Arc::new(Float64Array::from(acctbal_dev))
		};
		let batch = RecordBatch::try_new(
			schema,
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
		.map_err(|error| format!("build persisted customer batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_orders_parquet(
	strict: bool,
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut orderkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut custkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut orderdate = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut shippriority = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut orderpriority = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut comment = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut totalprice_dev = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut totalprice_strict = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut orderstatus = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in OrderGenerator::new(scale_factor, 1, 1).iter() {
		orderkey.push(item.o_orderkey);
		custkey.push(item.o_custkey);
		orderdate.push(item.o_orderdate.to_unix_epoch());
		shippriority.push(item.o_shippriority);
		orderpriority.push(item.o_orderpriority.to_string());
		comment.push(item.o_comment.to_string());
		if strict {
			totalprice_strict.push(item.o_totalprice.into_inner() as i128);
		} else {
			totalprice_dev.push(item.o_totalprice.as_f64());
		}
		orderstatus.push(item.o_orderstatus.to_string());
		if orderkey.len() >= TPCH_PERSISTED_CHUNK_ROWS {
			let totalprice: ArrayRef = if strict {
				decimal_array(std::mem::take(&mut totalprice_strict))
			} else {
				Arc::new(Float64Array::from(std::mem::take(&mut totalprice_dev)))
			};
			let batch = RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from(std::mem::take(&mut orderkey))),
					Arc::new(Int64Array::from(std::mem::take(&mut custkey))),
					Arc::new(Date32Array::from(std::mem::take(&mut orderdate))),
					Arc::new(Int32Array::from(std::mem::take(&mut shippriority))),
					Arc::new(StringArray::from(std::mem::take(&mut orderpriority))),
					Arc::new(StringArray::from(std::mem::take(&mut comment))),
					totalprice,
					Arc::new(StringArray::from(std::mem::take(&mut orderstatus))),
				],
			)
			.map_err(|error| format!("build persisted orders batch: {error}"))?;
			write_batch(&mut writer, path, &batch, &mut stats)?;
		}
	}
	if !orderkey.is_empty() {
		let totalprice: ArrayRef = if strict {
			decimal_array(totalprice_strict)
		} else {
			Arc::new(Float64Array::from(totalprice_dev))
		};
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(orderkey)),
				Arc::new(Int64Array::from(custkey)),
				Arc::new(Date32Array::from(orderdate)),
				Arc::new(Int32Array::from(shippriority)),
				Arc::new(StringArray::from(orderpriority)),
				Arc::new(StringArray::from(comment)),
				totalprice,
				Arc::new(StringArray::from(orderstatus)),
			],
		)
		.map_err(|error| format!("build persisted orders batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_nation_parquet(
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut nationkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut name = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut regionkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in NationGenerator::new(scale_factor, 1, 1).iter() {
		nationkey.push(item.n_nationkey);
		name.push(item.n_name.to_string());
		regionkey.push(item.n_regionkey);
	}
	if !nationkey.is_empty() {
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(nationkey)),
				Arc::new(StringArray::from(name)),
				Arc::new(Int64Array::from(regionkey)),
			],
		)
		.map_err(|error| format!("build persisted nation batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_supplier_parquet(
	strict: bool,
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut suppkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut nationkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut name = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut address = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut phone = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut acctbal_dev = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut acctbal_strict = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut comment = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		suppkey.push(item.s_suppkey);
		nationkey.push(item.s_nationkey);
		name.push(item.s_name.to_string());
		address.push(item.s_address.to_string());
		phone.push(item.s_phone.to_string());
		if strict {
			acctbal_strict.push(item.s_acctbal.into_inner() as i128);
		} else {
			acctbal_dev.push(item.s_acctbal.as_f64());
		}
		comment.push(item.s_comment.to_string());
		if suppkey.len() >= TPCH_PERSISTED_CHUNK_ROWS {
			let acctbal: ArrayRef = if strict {
				decimal_array(std::mem::take(&mut acctbal_strict))
			} else {
				Arc::new(Float64Array::from(std::mem::take(&mut acctbal_dev)))
			};
			let batch = RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from(std::mem::take(&mut suppkey))),
					Arc::new(Int64Array::from(std::mem::take(&mut nationkey))),
					Arc::new(StringArray::from(std::mem::take(&mut name))),
					Arc::new(StringArray::from(std::mem::take(&mut address))),
					Arc::new(StringArray::from(std::mem::take(&mut phone))),
					acctbal,
					Arc::new(StringArray::from(std::mem::take(&mut comment))),
				],
			)
			.map_err(|error| format!("build persisted supplier batch: {error}"))?;
			write_batch(&mut writer, path, &batch, &mut stats)?;
		}
	}
	if !suppkey.is_empty() {
		let acctbal: ArrayRef = if strict {
			decimal_array(acctbal_strict)
		} else {
			Arc::new(Float64Array::from(acctbal_dev))
		};
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(suppkey)),
				Arc::new(Int64Array::from(nationkey)),
				Arc::new(StringArray::from(name)),
				Arc::new(StringArray::from(address)),
				Arc::new(StringArray::from(phone)),
				acctbal,
				Arc::new(StringArray::from(comment)),
			],
		)
		.map_err(|error| format!("build persisted supplier batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_partsupp_parquet(
	strict: bool,
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut partkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut suppkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut availqty = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut supplycost_dev = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut supplycost_strict = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in PartSuppGenerator::new(scale_factor, 1, 1).iter() {
		partkey.push(item.ps_partkey);
		suppkey.push(item.ps_suppkey);
		availqty.push(item.ps_availqty);
		if strict {
			supplycost_strict.push(item.ps_supplycost.into_inner() as i128);
		} else {
			supplycost_dev.push(item.ps_supplycost.as_f64());
		}
		if partkey.len() >= TPCH_PERSISTED_CHUNK_ROWS {
			let supplycost: ArrayRef = if strict {
				decimal_array(std::mem::take(&mut supplycost_strict))
			} else {
				Arc::new(Float64Array::from(std::mem::take(&mut supplycost_dev)))
			};
			let batch = RecordBatch::try_new(
				schema.clone(),
				vec![
					Arc::new(Int64Array::from(std::mem::take(&mut partkey))),
					Arc::new(Int64Array::from(std::mem::take(&mut suppkey))),
					Arc::new(Int32Array::from(std::mem::take(&mut availqty))),
					supplycost,
				],
			)
			.map_err(|error| format!("build persisted partsupp batch: {error}"))?;
			write_batch(&mut writer, path, &batch, &mut stats)?;
		}
	}
	if !partkey.is_empty() {
		let supplycost: ArrayRef = if strict {
			decimal_array(supplycost_strict)
		} else {
			Arc::new(Float64Array::from(supplycost_dev))
		};
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(partkey)),
				Arc::new(Int64Array::from(suppkey)),
				Arc::new(Int32Array::from(availqty)),
				supplycost,
			],
		)
		.map_err(|error| format!("build persisted partsupp batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn write_region_parquet(
	scale_factor: f64,
	schema: SchemaRef,
	path: &Path,
) -> Result<PersistedWriteStats, String> {
	let mut writer = parquet_writer(path, schema.clone())?;
	let mut stats = PersistedWriteStats::default();
	let mut regionkey = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	let mut name = Vec::with_capacity(TPCH_PERSISTED_CHUNK_ROWS);
	for item in RegionGenerator::new(scale_factor, 1, 1).iter() {
		regionkey.push(item.r_regionkey);
		name.push(item.r_name.to_string());
	}
	if !regionkey.is_empty() {
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(regionkey)),
				Arc::new(StringArray::from(name)),
			],
		)
		.map_err(|error| format!("build persisted region batch: {error}"))?;
		write_batch(&mut writer, path, &batch, &mut stats)?;
	}
	writer
		.close()
		.map_err(|error| format!("close parquet {}: {error}", path.display()))?;
	Ok(stats)
}

fn canonical_sql_type(data_type: &DataType) -> String {
	match data_type {
		DataType::Boolean => "BOOLEAN".to_string(),
		DataType::Int8 => "TINYINT".to_string(),
		DataType::Int16 => "SMALLINT".to_string(),
		DataType::Int32 => "INTEGER".to_string(),
		DataType::Int64 => "BIGINT".to_string(),
		DataType::UInt8 => "UTINYINT".to_string(),
		DataType::UInt16 => "USMALLINT".to_string(),
		DataType::UInt32 => "UINTEGER".to_string(),
		DataType::UInt64 => "UBIGINT".to_string(),
		DataType::Float32 => "FLOAT".to_string(),
		DataType::Float64 => "DOUBLE".to_string(),
		DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
		DataType::Date32 | DataType::Date64 => "DATE".to_string(),
		DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
		DataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
		other => format!("{other:?}").to_ascii_uppercase(),
	}
}

pub fn normalize_duckdb_type(raw: &str) -> String {
	raw.trim()
		.replace(' ', "")
		.to_ascii_uppercase()
		.replace("INT64", "BIGINT")
		.replace("INT32", "INTEGER")
}

pub fn schema_signature(schema: &Schema) -> String {
	schema
		.fields()
		.iter()
		.map(|field| format!("{}:{}", field.name(), canonical_sql_type(field.data_type())))
		.collect::<Vec<_>>()
		.join("|")
}

pub fn schema_fingerprint(schema: &Schema) -> String {
	stable_fingerprint(&schema_signature(schema))
}

pub fn schema_fingerprint_from_duckdb_fields(fields: &[(String, String)]) -> String {
	let signature = fields
		.iter()
		.map(|(name, data_type)| format!("{name}:{}", normalize_duckdb_type(data_type)))
		.collect::<Vec<_>>()
		.join("|");
	stable_fingerprint(&signature)
}

pub fn output_row_count(batches: &[RecordBatch]) -> u64 {
	batches.iter().map(|batch| batch.num_rows() as u64).sum()
}

pub fn output_schema_fingerprint(batches: &[RecordBatch]) -> Option<String> {
	batches
		.first()
		.map(|batch| schema_fingerprint(batch.schema().as_ref()))
}

pub fn export_perf_query_parquet(
	query: &str,
	mode: &str,
	scale_factor: f64,
	root: &Path,
) -> Result<TpchParquetExport, String> {
	let spec = build_perf_query_spec(query, mode, scale_factor)?;
	let export_dir = root
		.join(mode)
		.join(format!("sf{}", scale_factor.to_string().replace('.', "_")))
		.join(spec.query_id);
	std::fs::create_dir_all(&export_dir)
		.map_err(|error| format!("create {}: {error}", export_dir.display()))?;

	let mut table_exports = Vec::new();
	for table in spec.tables {
		let path = export_dir.join(format!("{}.parquet", table.name));
		let file =
			File::create(&path).map_err(|error| format!("create {}: {error}", path.display()))?;
		let mut writer = ArrowWriter::try_new(file, table.batch.schema(), None)
			.map_err(|error| format!("create parquet writer for {}: {error}", table.name))?;
		writer
			.write(&table.batch)
			.map_err(|error| format!("write parquet {}: {error}", table.name))?;
		writer
			.close()
			.map_err(|error| format!("close parquet {}: {error}", table.name))?;

		let schema_signature = schema_signature(table.batch.schema().as_ref());
		table_exports.push(TpchParquetTableExport {
			name: table.name.to_string(),
			path: path.clone(),
			row_count: table.batch.num_rows() as u64,
			schema_fingerprint: stable_fingerprint(&schema_signature),
			schema_signature,
			file_hash: stable_file_hash(&path)?,
		});
	}

	let manifest_signature = table_exports
		.iter()
		.map(|table| {
			format!(
				"{}:{}:{}:{}",
				table.name, table.row_count, table.schema_fingerprint, table.file_hash
			)
		})
		.collect::<Vec<_>>()
		.join("|");
	let manifest_fingerprint = stable_fingerprint(&format!(
		"query={};mode={mode};sf={scale_factor};tables={manifest_signature}",
		spec.query_id
	));
	let manifest_path = export_dir.join("manifest.json");
	let manifest_tables = table_exports
		.iter()
		.map(|table| {
			serde_json::json!({
				"name": table.name,
				"path": table.path.display().to_string(),
				"row_count": table.row_count,
				"schema_fingerprint": table.schema_fingerprint,
				"schema_signature": table.schema_signature,
				"file_hash": table.file_hash,
			})
		})
		.collect::<Vec<_>>();
	let manifest = serde_json::json!({
		"suite": "tpch_perf_parquet_manifest",
		"query": spec.query_id,
		"mode": mode,
		"scale_factor": scale_factor,
		"manifest_fingerprint": manifest_fingerprint,
		"tables": manifest_tables,
	});
	let manifest_body = serde_json::to_string_pretty(&manifest)
		.map_err(|error| format!("encode manifest {}: {error}", manifest_path.display()))?;
	std::fs::write(&manifest_path, format!("{manifest_body}\n"))
		.map_err(|error| format!("write {}: {error}", manifest_path.display()))?;

	Ok(TpchParquetExport {
		query_id: spec.query_id.to_string(),
		mode: mode.to_string(),
		scale_factor,
		sql: spec.sql.to_string(),
		root_dir: export_dir,
		manifest_path,
		manifest_fingerprint,
		tables: table_exports,
	})
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

/// A stage error from the shared execution path: the deepest phase reached, its classified reason,
/// and the raw message that produced the classification.
#[derive(Debug)]
struct StageError {
	phase: Phase,
	reason: String,
	message: String,
	timings: PerfPhaseTimings,
	plan_fingerprint: Option<String>,
	spill_bytes: u64,
}

impl StageError {
	fn from_message(phase: Phase, message: String) -> Self {
		let reason = reason_for_error(&message).to_string();
		Self {
			phase,
			reason,
			message,
			timings: PerfPhaseTimings::default(),
			plan_fingerprint: None,
			spill_bytes: 0,
		}
	}

	fn from_debug(phase: Phase, error: impl std::fmt::Debug) -> Self {
		Self::from_message(phase, format!("{error:?}"))
	}

	fn from_display(phase: Phase, error: impl std::fmt::Display) -> Self {
		Self::from_message(phase, error.to_string())
	}

	fn with_perf(
		mut self,
		timings: PerfPhaseTimings,
		plan_fingerprint: Option<String>,
		spill_bytes: u64,
	) -> Self {
		self.timings = timings;
		self.plan_fingerprint = plan_fingerprint;
		self.spill_bytes = spill_bytes;
		self
	}
}

struct QueryExecution {
	batches: Vec<RecordBatch>,
	timings: PerfPhaseTimings,
	plan_fingerprint: String,
	spill_bytes: u64,
	local_join_admissions: Vec<String>,
	join_spill_observations: Vec<JoinSpillObservation>,
}

/// The shared Load→Plan→Bind→Execute substrate for both the reachability and correctness rails: register
/// every descriptor, seed its optimizer facts, assemble storage, then plan/bind/execute, returning the
/// result batches or the deepest-phase stage error. Reachability adds a digest; correctness adds the oracle
/// compare — neither forks the execution path (ADR 273).
async fn execute_query_with_tables(
	sql: &str,
	tables: Vec<HarnessTable>,
) -> Result<Vec<RecordBatch>, StageError> {
	execute_query_with_tables_timed(sql, tables)
		.await
		.map(|execution| execution.batches)
}

async fn execute_query_with_tables_timed(
	sql: &str,
	tables: Vec<HarnessTable>,
) -> Result<QueryExecution, StageError> {
	let (metadata, storage) = metadata_storage_from_tables(tables);
	execute_query_with_metadata_timed(sql, metadata, storage).await
}

fn metadata_storage_from_tables(
	tables: Vec<HarnessTable>,
) -> (Vec<HarnessTableMetadata>, Arc<dyn StorageScan>) {
	let mut batches = HashMap::new();
	let mut metadata = Vec::new();
	for table in tables {
		metadata.push(HarnessTableMetadata {
			id: table.id,
			name: table.name,
			descriptor: table.descriptor,
			facts: table.facts,
		});
		batches.insert(table.id, table.batch);
	}
	let storage: Arc<dyn StorageScan> = Arc::new(HarnessScan { batches });
	(metadata, storage)
}

fn load_catalog_from_metadata(
	tables: Vec<HarnessTableMetadata>,
	timings: &mut PerfPhaseTimings,
	total_start: Instant,
) -> Result<MockCatalog, StageError> {
	let load_start = Instant::now();
	let mut catalog = MockCatalog::new();
	for table in tables {
		if let Err(error) = catalog.register_descriptor(table.id, table.descriptor) {
			timings.load_ms = load_start.elapsed().as_secs_f64() * 1000.0;
			timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
			return Err(StageError::from_debug(Phase::Load, error).with_perf(*timings, None, 0));
		}
		if let Err(error) = catalog.seed_optimizer_facts(&TableRef::new(table.name), table.facts) {
			timings.load_ms = load_start.elapsed().as_secs_f64() * 1000.0;
			timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
			return Err(StageError::from_debug(Phase::Load, error).with_perf(*timings, None, 0));
		}
	}
	timings.load_ms = load_start.elapsed().as_secs_f64() * 1000.0;
	Ok(catalog)
}

async fn execute_query_with_metadata_timed(
	sql: &str,
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
) -> Result<QueryExecution, StageError> {
	let total_start = Instant::now();
	let mut timings = PerfPhaseTimings::default();
	let catalog = load_catalog_from_metadata(tables, &mut timings, total_start)?;

	let plan_start = Instant::now();
	let logical = match SqlPlanner::new().plan(sql) {
		Ok(logical) => logical,
		Err(error) => {
			timings.plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;
			timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
			return Err(StageError::from_display(Phase::Plan, error).with_perf(timings, None, 0));
		}
	};
	timings.plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

	execute_logical_with_catalog_timed(total_start, timings, catalog, logical, storage).await
}

async fn execute_rune_q03_with_metadata_timed(
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
) -> Result<QueryExecution, StageError> {
	let total_start = Instant::now();
	let mut timings = PerfPhaseTimings::default();
	let catalog = load_catalog_from_metadata(tables, &mut timings, total_start)?;

	let plan_start = Instant::now();
	let logical = match rune_q03_logical_plan().await {
		Ok(logical) => logical,
		Err(error) => {
			timings.plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;
			timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
			return Err(error.with_perf(timings, None, 0));
		}
	};
	timings.plan_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

	execute_logical_with_catalog_timed(total_start, timings, catalog, logical, storage).await
}

async fn rune_q03_logical_plan() -> Result<LogicalPlan, StageError> {
	let config = RuneSandboxConfig::v1_default();
	let engine = RuneEngine::with_config(config)
		.map_err(|error| StageError::from_debug(Phase::Plan, error))?;
	engine
		.execute_script(RUNE_Q3_SCRIPT, &config)
		.await
		.map_err(|error| StageError::from_debug(Phase::Plan, error))
}

async fn execute_logical_with_catalog_timed(
	total_start: Instant,
	mut timings: PerfPhaseTimings,
	catalog: MockCatalog,
	logical: LogicalPlan,
	storage: Arc<dyn StorageScan>,
) -> Result<QueryExecution, StageError> {
	let mut plan_fingerprint = None;
	let mut spill_bytes = 0;
	let bind_start = Instant::now();
	let (bound, local_join_admissions) =
		match bind_with_local_join_admissions(&logical, &catalog, &gauge_planner_config()).await {
			Ok(bound) => bound,
			Err(error) => {
				timings.bind_ms = bind_start.elapsed().as_secs_f64() * 1000.0;
				timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
				return Err(StageError::from_debug(Phase::Bind, error).with_perf(
					timings,
					plan_fingerprint,
					spill_bytes,
				));
			}
		};
	timings.bind_ms = bind_start.elapsed().as_secs_f64() * 1000.0;
	plan_fingerprint = Some(physical_plan_fingerprint(&bound));

	let task_telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
	let context = tpch_execution_context(task_telemetry.clone());
	let execute_start = Instant::now();
	let batches = match Executor::with_context(storage, context)
		.execute_bound(&bound, None)
		.await
	{
		Ok(batches) => batches,
		Err(error) => {
			timings.execute_ms = execute_start.elapsed().as_secs_f64() * 1000.0;
			timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
			spill_bytes = task_telemetry
				.lock()
				.expect("task telemetry lock")
				.counters
				.spill_bytes;
			return Err(StageError::from_debug(Phase::Execute, error).with_perf(
				timings,
				plan_fingerprint,
				spill_bytes,
			));
		}
	};
	timings.execute_ms = execute_start.elapsed().as_secs_f64() * 1000.0;
	timings.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
	let telemetry = task_telemetry.lock().expect("task telemetry lock");
	spill_bytes = telemetry.counters.spill_bytes;
	let join_spill_observations = telemetry.join_spill_observations.clone();
	drop(telemetry);

	Ok(QueryExecution {
		batches,
		timings,
		plan_fingerprint: plan_fingerprint.expect("bound plan has a fingerprint"),
		spill_bytes,
		local_join_admissions: local_join_admissions
			.iter()
			.map(|admission| admission.describe())
			.collect(),
		join_spill_observations,
	})
}

pub const GAUGE_MEMORY_PER_TASK_ENV: &str = "TORON_TPCH_MEMORY_PER_TASK";

/// The gauge's memory budget and where it came from. `default` means the
/// engine's 64 MiB test constant; `explicit` means the operator set the env.
/// Every perf row records this pair — a gauge figure never floats free of the
/// budget regime it was measured under.
pub fn gauge_memory_per_task() -> Result<(u64, &'static str), String> {
	match std::env::var(GAUGE_MEMORY_PER_TASK_ENV) {
		Ok(raw) => match raw.parse::<u64>() {
			Ok(bytes) if bytes > 0 => Ok((bytes, "explicit")),
			_ => Err(format!(
				"{GAUGE_MEMORY_PER_TASK_ENV} must be a positive byte count, got {raw:?}"
			)),
		},
		Err(std::env::VarError::NotPresent) => {
			Ok((QueryBudget::test_default().memory_per_task.get(), "default"))
		}
		Err(error) => Err(format!("{GAUGE_MEMORY_PER_TASK_ENV}: {error}")),
	}
}

pub const GAUGE_SPILL_BYTES_RESERVED_ENV: &str = "TORON_TPCH_SPILL_BYTES_RESERVED";
pub const GAUGE_HASH_SMALLER_BUILD_ENV: &str = "TORON_TPCH_HASH_SMALLER_BUILD";

/// The gauge's smaller-build placement setting and its provenance: unset means
/// the engine default; "0"/"1" are explicit arms. Same loud contract as the
/// budget knobs.
pub fn gauge_hash_smaller_build() -> Result<(bool, &'static str), String> {
	match std::env::var(GAUGE_HASH_SMALLER_BUILD_ENV) {
		Ok(raw) => match raw.as_str() {
			"0" => Ok((false, "explicit")),
			"1" => Ok((true, "explicit")),
			_ => Err(format!(
				"{GAUGE_HASH_SMALLER_BUILD_ENV} must be \"0\" or \"1\", got {raw:?}"
			)),
		},
		Err(std::env::VarError::NotPresent) => Ok((
			PlannerConfig::default().hash_smaller_build_placement,
			"default",
		)),
		Err(error) => Err(format!("{GAUGE_HASH_SMALLER_BUILD_ENV}: {error}")),
	}
}

/// Every gauge bind flows through this constructor so the knob cannot be
/// silently bypassed.
pub const GAUGE_LOCAL_JOIN_ADMISSION_ENV: &str = "TORON_TPCH_LOCAL_JOIN_ADMISSION";

pub fn gauge_planner_config() -> PlannerConfig {
	// The budget env is validated loudly at row emission; a malformed value must
	// abort the bind too, never degrade to a silent None (which would flip the
	// local-admission policy off while the row still reports an explicit budget).
	let (bytes, _provenance) =
		gauge_memory_per_task().unwrap_or_else(|error| panic!("gauge planner config: {error}"));
	// A/B control arm: "0" withholds the planner's policy input (admission
	// declines with an explicit no_memory_budget receipt) while the execution
	// budget itself is untouched, so both arms run the identical coordinate.
	let policy_input = match std::env::var(GAUGE_LOCAL_JOIN_ADMISSION_ENV) {
		Ok(raw) if raw == "0" => None,
		Ok(raw) if raw == "1" => Some(ByteCount::new(bytes)),
		Ok(raw) => panic!("{GAUGE_LOCAL_JOIN_ADMISSION_ENV} must be \"0\" or \"1\", got {raw:?}"),
		Err(std::env::VarError::NotPresent) => Some(ByteCount::new(bytes)),
		Err(error) => panic!("{GAUGE_LOCAL_JOIN_ADMISSION_ENV}: {error}"),
	};
	gauge_planner_config_with_task_memory(policy_input)
}

fn gauge_planner_config_with_task_memory(
	local_hash_join_memory_per_task: Option<ByteCount>,
) -> PlannerConfig {
	let mut config = PlannerConfig::default();
	if let Ok((value, "explicit")) = gauge_hash_smaller_build() {
		config.hash_smaller_build_placement = value;
	}
	// The gauge binds and immediately runs through `Executor::with_context`; it is
	// the caller that can prove this plan will not be fragmented. ADR 489's
	// distributed coordinator forcibly clears the corresponding signal.
	config.single_partition_execution = true;
	config.local_hash_join_memory_per_task = local_hash_join_memory_per_task;
	config
}

#[cfg(test)]
#[test]
fn adr490_single_node_gauge_config_carries_exact_raw_budget_without_admission() {
	let configured =
		gauge_planner_config_with_task_memory(Some(ByteCount::new(2 * 1024 * 1024 * 1024)));
	assert!(
		configured.single_partition_execution,
		"the direct gauge path proves this bind will not be fragmented"
	);
	assert_eq!(
		configured.local_hash_join_memory_per_task,
		Some(ByteCount::new(2 * 1024 * 1024 * 1024)),
		"the binder policy input must equal the execution coordinate"
	);
	assert!(
		PlannerConfig::default()
			.local_hash_join_memory_per_task
			.is_none(),
		"non-gauge callers remain conservative until they supply budget evidence"
	);
}

/// The gauge's per-query spill reserve and its provenance, same contract as
/// [`gauge_memory_per_task`]: a frontier probe must be able to move one budget
/// dimension while holding the others explicitly, not implicitly.
pub fn gauge_spill_bytes_reserved() -> Result<(u64, &'static str), String> {
	match std::env::var(GAUGE_SPILL_BYTES_RESERVED_ENV) {
		Ok(raw) => match raw.parse::<u64>() {
			Ok(bytes) if bytes > 0 => Ok((bytes, "explicit")),
			_ => Err(format!(
				"{GAUGE_SPILL_BYTES_RESERVED_ENV} must be a positive byte count, got {raw:?}"
			)),
		},
		Err(std::env::VarError::NotPresent) => Ok((
			QueryBudget::test_default()
				.spill_bytes_reserved_per_query
				.get(),
			"default",
		)),
		Err(error) => Err(format!("{GAUGE_SPILL_BYTES_RESERVED_ENV}: {error}")),
	}
}

fn tpch_execution_context(task_telemetry: Arc<Mutex<TaskTelemetry>>) -> ExecutionContext {
	let mut context = ExecutionContext::local_test()
		.with_task_telemetry(task_telemetry)
		.with_join_spill_store(
			Arc::new(wendelon::storage::BlockingFileOps),
			PathBuf::from(CACHE_ROOT).join("join_spill"),
		);
	let memory = gauge_memory_per_task();
	let spill = gauge_spill_bytes_reserved();
	let explicit =
		|value: &Result<(u64, &'static str), String>| matches!(value, Ok((_, "explicit")));
	if explicit(&memory) || explicit(&spill) {
		let mut budget = QueryBudget::test_default();
		if let Ok((bytes, "explicit")) = memory {
			budget.memory_per_task = ByteCount::new(bytes);
		}
		if let Ok((bytes, "explicit")) = spill {
			budget.spill_bytes_reserved_per_query = ByteCount::new(bytes);
		}
		context = context.with_task_budget(TaskBudget::from_query_budget(budget));
	}
	context
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
	let attempt = |phase: Phase,
	               status: Status,
	               reason: Option<String>,
	               digest: Option<String>,
	               diagnostic_message: Option<String>| Attempt {
		query: query_id.to_string(),
		scale_factor,
		mode: mode.to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
		digest,
		diagnostic_message,
	};

	let batches = match execute_query_with_tables(sql, tables).await {
		Ok(batches) => batches,
		Err(StageError {
			phase,
			reason,
			message,
			..
		}) => {
			return attempt(
				phase,
				status_for_reason(&reason),
				Some(reason),
				None,
				Some(message),
			);
		}
	};

	let digest = match canonical_arrow_ipc_digest(&batches, SortPolicy::FullRowLex) {
		Ok(digest) => format!("{digest:?}"),
		Err(error) => {
			let message = format!("{error:?}");
			let reason = reason_for_error(&message);
			return attempt(
				Phase::Execute,
				status_for_reason(reason),
				Some(reason.to_string()),
				None,
				Some(message),
			);
		}
	};

	attempt(Phase::Execute, Status::Pass, None, Some(digest), None)
}

fn physical_plan_fingerprint(plan: &ExecutionPlan) -> String {
	const FNV_OFFSET: u64 = 0xcbf29ce484222325;
	const FNV_PRIME: u64 = 0x00000100000001b3;
	let mut hash = FNV_OFFSET;
	let shape = plan_fingerprint_shape(plan);
	for byte in shape.as_bytes() {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(FNV_PRIME);
	}
	format!("{hash:016x}")
}

fn plan_fingerprint_shape(plan: &ExecutionPlan) -> String {
	strip_plan_fingerprint_routing_hints(&format!("{plan:?}"))
}

fn strip_plan_fingerprint_routing_hints(shape: &str) -> String {
	const FIELD: &str = "estimated_byte_size: ";
	let mut output = String::with_capacity(shape.len());
	let mut cursor = 0;
	while let Some(relative_start) = shape[cursor..].find(FIELD) {
		let field_start = cursor + relative_start;
		output.push_str(&shape[cursor..field_start]);
		let mut field_end = field_start + FIELD.len();
		if shape[field_end..].starts_with("Some(") {
			field_end += "Some(".len();
			while field_end < shape.len() && shape.as_bytes()[field_end] != b')' {
				field_end += 1;
			}
			if field_end < shape.len() {
				field_end += 1;
			}
		} else if shape[field_end..].starts_with("None") {
			field_end += "None".len();
		}
		if shape[field_end..].starts_with(", ") {
			field_end += 2;
		}
		cursor = field_end;
	}
	output.push_str(&shape[cursor..]);
	output
}

#[derive(Debug, Clone)]
pub struct RuneQ3IsolationProof {
	pub data_source: String,
	pub manifest_fingerprint: Option<String>,
	pub sql_plan_fingerprint: String,
	pub rune_plan_fingerprint: String,
	pub sql_compact_shape: String,
	pub rune_compact_shape: String,
	pub sql_plan_shape: String,
	pub rune_plan_shape: String,
	pub optimizer_table_facts: String,
	pub optimizer_evidence_gap: String,
	pub sql_join_decisions: String,
	pub rune_join_decisions: String,
	pub sql_join_order_edges: String,
	pub rune_join_order_edges: String,
}

impl RuneQ3IsolationProof {
	pub fn plans_differ(&self) -> bool {
		self.sql_plan_fingerprint != self.rune_plan_fingerprint
			|| self.sql_plan_shape != self.rune_plan_shape
	}
}

#[derive(Debug, Clone)]
pub struct RuneQ03ProfileLoopSummary {
	pub iterations: usize,
	pub prepare_ms: f64,
	pub warmup_execute_ms: f64,
	pub loop_execute_ms: f64,
	pub iteration_execute_ms: Vec<f64>,
	pub plan_fingerprint: String,
	pub spill_bytes_max: u64,
	pub output_row_count: u64,
	pub output_schema_fingerprint: Option<String>,
	pub join_build_profiles: Vec<JoinBuildProfile>,
	pub parquet_scan_ledger: Vec<ParquetScanLedger>,
}

struct ProfileLoopIteration {
	execute_ms: f64,
	spill_bytes: u64,
	output_row_count: u64,
	output_schema_fingerprint: Option<String>,
	join_build_profiles: Vec<JoinBuildProfile>,
	parquet_scan_ledger: Vec<ParquetScanLedger>,
}

pub async fn rune_q03_isolation_proof(scale_factor: f64) -> Result<RuneQ3IsolationProof, String> {
	rune_q03_isolation_proof_with_data_source(
		"strict",
		scale_factor,
		"memory",
		Path::new(CACHE_ROOT),
	)
	.await
}

pub async fn rune_q03_isolation_proof_with_data_source(
	mode: &str,
	scale_factor: f64,
	data_source: &str,
	data_root: &Path,
) -> Result<RuneQ3IsolationProof, String> {
	let (metadata, sql, manifest_fingerprint) = match data_source {
		"memory" => {
			let spec = build_perf_query_spec("q03", mode, scale_factor)?;
			let sql = spec.sql;
			let (metadata, _) = metadata_storage_from_tables(spec.tables);
			(metadata, sql, None)
		}
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec("q03", mode, scale_factor, data_root)?;
			(
				spec.tables,
				spec.sql,
				Some(spec.export.manifest_fingerprint.clone()),
			)
		}
		other => {
			return Err(format!(
				"q03 isolation proof data source {other:?} is not supported"
			));
		}
	};
	let optimizer_table_facts = summarize_harness_table_facts(&metadata);
	let mut timings = PerfPhaseTimings::default();
	let catalog =
		load_catalog_from_metadata(metadata, &mut timings, Instant::now()).map_err(|error| {
			format!(
				"load q03 catalog for rune isolation proof failed: {}",
				error.message
			)
		})?;
	let sql_logical = SqlPlanner::new()
		.plan(&sql)
		.map_err(|error| format!("plan SQL q03 for rune isolation proof failed: {error}"))?;
	let rune_logical = rune_q03_logical_plan().await.map_err(|error| {
		format!(
			"plan Rune q03 for rune isolation proof failed: {}",
			error.message
		)
	})?;
	let (sql_bound, sql_join_decisions, sql_join_order_edges) =
		bind_with_explain_summary(&sql_logical, &catalog, &gauge_planner_config())
			.await
			.map_err(|error| format!("bind SQL q03 for rune isolation proof failed: {error:?}"))?;
	let (rune_bound, rune_join_decisions, rune_join_order_edges) =
		bind_with_explain_summary(&rune_logical, &catalog, &gauge_planner_config())
			.await
			.map_err(|error| format!("bind Rune q03 for rune isolation proof failed: {error:?}"))?;
	let optimizer_evidence_gap =
		q03_optimizer_evidence_gap(&sql_join_order_edges, &rune_join_order_edges);
	Ok(RuneQ3IsolationProof {
		data_source: data_source.to_string(),
		manifest_fingerprint,
		sql_plan_fingerprint: physical_plan_fingerprint(&sql_bound),
		rune_plan_fingerprint: physical_plan_fingerprint(&rune_bound),
		sql_compact_shape: compact_execution_plan_shape(&sql_bound),
		rune_compact_shape: compact_execution_plan_shape(&rune_bound),
		sql_plan_shape: format!("{sql_bound:#?}"),
		rune_plan_shape: format!("{rune_bound:#?}"),
		optimizer_table_facts,
		optimizer_evidence_gap,
		sql_join_decisions,
		rune_join_decisions,
		sql_join_order_edges,
		rune_join_order_edges,
	})
}

fn summarize_harness_table_facts(metadata: &[HarnessTableMetadata]) -> String {
	metadata
		.iter()
		.map(|table| {
			format!(
				"{} rows={:?} bytes={:?} ndv={} ranges={} range_dist={} heavy_hitters={} group_top_k={} group_ndv={}",
				table.name,
				table.facts.row_upper_bound.map(RowCount::get),
				table.facts.byte_upper_bound.map(ByteCount::get),
				table.facts.column_ndv.len(),
				table.facts.column_range.len(),
				table.facts.column_range_distributions.len(),
				table.facts.column_heavy_hitters.len(),
				table.facts.column_group_top_k.len(),
				table.facts.column_group_ndv.len()
			)
		})
		.collect::<Vec<_>>()
		.join(" | ")
}

fn q03_optimizer_evidence_gap(sql_join_order_edges: &str, rune_join_order_edges: &str) -> String {
	format!(
		"sql_join_order_rows_missing={}; rune_join_order_rows_missing={}; join_order_edge_fields=join_index,kind,probe,dim,left_keys,right_keys,rows,linearity; missing_evidence=post_filter_cardinality,multi_relation_cost",
		sql_join_order_edges.contains("rows=None"),
		rune_join_order_edges.contains("rows=None")
	)
}

fn compact_execution_plan_shape(plan: &ExecutionPlan) -> String {
	match plan {
		ExecutionPlan::Scan {
			local_table_id,
			schema,
			projection,
			pbc_program,
			..
		} => format!(
			"Scan(t={},cols=[{}],projection={:?},pbc={})",
			local_table_id.0,
			schema_field_names(schema),
			projection,
			pbc_program.is_some()
		),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => format!(
			"Filter(disposition={disposition:?},predicate={},input={})",
			predicate.pretty(),
			compact_execution_plan_shape(input)
		),
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => format!(
			"Project(roots={},cols=[{}],input={})",
			exprs.root_count(),
			schema_field_names(output_schema),
			compact_execution_plan_shape(input)
		),
		ExecutionPlan::Aggregate {
			input,
			group_expr,
			aggr_expr,
			output_schema,
		} => format!(
			"Aggregate(groups={},aggrs={},cols=[{}],input={})",
			group_expr.root_count(),
			aggr_expr.len(),
			schema_field_names(output_schema),
			compact_execution_plan_shape(input)
		),
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => format!(
			"Exchange(parts={partition_count},keys={},input={})",
			key.root_count(),
			compact_execution_plan_shape(input)
		),
		ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			..
		} => {
			let keys = order_by
				.iter()
				.map(|key| {
					format!(
						"{}:{}:{}",
						key.ordinal.0,
						if key.descending { "desc" } else { "asc" },
						if key.nulls_first {
							"nulls_first"
						} else {
							"nulls_last"
						}
					)
				})
				.collect::<Vec<_>>()
				.join(",");
			format!(
				"Sort(keys=[{}],limit={limit:?},offset={offset},input={})",
				keys,
				compact_execution_plan_shape(input)
			)
		}
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			strategy,
			..
		} => format!(
			"Join(kind={kind:?},strategy={strategy:?},keys={:?}/{:?},left={},right={})",
			left_keys,
			right_keys,
			compact_execution_plan_shape(left),
			compact_execution_plan_shape(right)
		),
		ExecutionPlan::Union { left, right, .. } => format!(
			"Union(left={},right={})",
			compact_execution_plan_shape(left),
			compact_execution_plan_shape(right)
		),
		ExecutionPlan::Window { input, .. } => {
			format!("Window(input={})", compact_execution_plan_shape(input))
		}
		ExecutionPlan::Take { input, limit, .. } => {
			format!(
				"Take(limit={limit},input={})",
				compact_execution_plan_shape(input)
			)
		}
		ExecutionPlan::Repeat { input, .. } => {
			format!("Repeat(input={})", compact_execution_plan_shape(input))
		}
		ExecutionPlan::CreateTableAsSelect { source, .. } => {
			format!(
				"CreateTableAsSelect(input={})",
				compact_execution_plan_shape(source)
			)
		}
		ExecutionPlan::Update { source, .. } => {
			format!("Update(input={})", compact_execution_plan_shape(source))
		}
		ExecutionPlan::InsertSelect { source, .. } => {
			format!(
				"InsertSelect(input={})",
				compact_execution_plan_shape(source)
			)
		}
		ExecutionPlan::EmptyRelation { .. } => "EmptyRelation".to_string(),
		ExecutionPlan::SystemTable { kind, .. } => format!("SystemTable({kind:?})"),
		ExecutionPlan::CreateTable { .. } => "CreateTable".to_string(),
		ExecutionPlan::CreateLogicalType { .. } => "CreateLogicalType".to_string(),
		ExecutionPlan::DropTable { .. } => "DropTable".to_string(),
		ExecutionPlan::Delete { .. } => "Delete".to_string(),
		ExecutionPlan::Insert { .. } => "Insert".to_string(),
		ExecutionPlan::RecursiveCte { .. } => "RecursiveCte".to_string(),
		ExecutionPlan::RecursiveRef { .. } => "RecursiveRef".to_string(),
	}
}

fn schema_field_names(schema: &SchemaRef) -> String {
	schema
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect::<Vec<_>>()
		.join(",")
}

pub async fn run_rune_q03_profile_loop_with_data_source(
	mode: &str,
	scale_factor: f64,
	data_source: &str,
	data_root: &Path,
	iterations: usize,
) -> Result<RuneQ03ProfileLoopSummary, String> {
	if iterations == 0 {
		return Err("profile loop iterations must be > 0".to_string());
	}
	let (tables, storage) = match data_source {
		"memory" => {
			let spec = build_perf_query_spec("q03", mode, scale_factor)?;
			let (metadata, storage) = metadata_storage_from_tables(spec.tables);
			(metadata, storage)
		}
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec("q03", mode, scale_factor, data_root)?;
			let storage: Arc<dyn StorageScan> = spec.storage;
			(spec.tables, storage)
		}
		other => return Err(format!("perf --data-source {other:?} is not supported")),
	};

	let prepare_start = Instant::now();
	let mut timings = PerfPhaseTimings::default();
	let catalog =
		load_catalog_from_metadata(tables, &mut timings, prepare_start).map_err(|error| {
			format!(
				"load q03 catalog for rune profile loop failed: {}",
				error.message
			)
		})?;
	let logical = rune_q03_logical_plan()
		.await
		.map_err(|error| format!("plan Rune q03 for profile loop failed: {}", error.message))?;
	let bound = bind(&logical, &catalog, &gauge_planner_config())
		.await
		.map_err(|error| format!("bind Rune q03 for profile loop failed: {error:?}"))?;
	let plan_fingerprint = physical_plan_fingerprint(&bound);
	let prepare_ms = prepare_start.elapsed().as_secs_f64() * 1000.0;

	let warmup = execute_bound_profile_iteration(&bound, storage.clone())
		.await
		.map_err(|error| {
			format!(
				"warmup Rune q03 profile loop failed at {:?}: {}",
				error.phase, error.message
			)
		})?;

	let loop_start = Instant::now();
	let mut iteration_execute_ms = Vec::with_capacity(iterations);
	let mut spill_bytes_max = warmup.spill_bytes;
	let mut output_row_count = warmup.output_row_count;
	let mut output_schema_fingerprint = warmup.output_schema_fingerprint.clone();
	let mut join_build_profiles = warmup.join_build_profiles.clone();
	let mut parquet_scan_ledger = warmup.parquet_scan_ledger.clone();
	for _ in 0..iterations {
		let iteration = execute_bound_profile_iteration(&bound, storage.clone())
			.await
			.map_err(|error| {
				format!(
					"Rune q03 profile loop failed at {:?}: {}",
					error.phase, error.message
				)
			})?;
		spill_bytes_max = spill_bytes_max.max(iteration.spill_bytes);
		output_row_count = iteration.output_row_count;
		output_schema_fingerprint = iteration.output_schema_fingerprint.clone();
		join_build_profiles = iteration.join_build_profiles.clone();
		parquet_scan_ledger = iteration.parquet_scan_ledger.clone();
		iteration_execute_ms.push(iteration.execute_ms);
	}
	let loop_execute_ms = loop_start.elapsed().as_secs_f64() * 1000.0;

	Ok(RuneQ03ProfileLoopSummary {
		iterations,
		prepare_ms,
		warmup_execute_ms: warmup.execute_ms,
		loop_execute_ms,
		iteration_execute_ms,
		plan_fingerprint,
		spill_bytes_max,
		output_row_count,
		output_schema_fingerprint,
		join_build_profiles,
		parquet_scan_ledger,
	})
}

async fn execute_bound_profile_iteration(
	bound: &ExecutionPlan,
	storage: Arc<dyn StorageScan>,
) -> Result<ProfileLoopIteration, StageError> {
	let task_telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
	let context = tpch_execution_context(task_telemetry.clone());
	clear_parquet_scan_ledger();
	let execute_start = Instant::now();
	let batches = Executor::with_context(storage, context)
		.execute_bound(bound, None)
		.await
		.map_err(|error| StageError::from_debug(Phase::Execute, error))?;
	let execute_ms = execute_start.elapsed().as_secs_f64() * 1000.0;
	let telemetry = task_telemetry.lock().expect("task telemetry lock");
	let spill_bytes = telemetry.counters.spill_bytes;
	let join_build_profiles = telemetry.join_build_profiles.clone();
	drop(telemetry);
	let parquet_scan_ledger = take_parquet_scan_ledger();
	Ok(ProfileLoopIteration {
		execute_ms,
		spill_bytes,
		output_row_count: output_row_count(&batches),
		output_schema_fingerprint: output_schema_fingerprint(&batches),
		join_build_profiles,
		parquet_scan_ledger,
	})
}

async fn run_rune_q03_perf_query_with_metadata(
	query_id: &str,
	mode: &str,
	scale_factor: f64,
	run_index: u32,
	warmup: bool,
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
) -> PerfAttempt {
	let mut execution = match execute_rune_q03_with_metadata_timed(tables, storage).await {
		Ok(execution) => execution,
		Err(StageError {
			phase,
			reason,
			message,
			timings,
			plan_fingerprint,
			spill_bytes,
		}) => {
			return PerfAttempt {
				engine: "rune".to_string(),
				query: query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				run_index,
				warmup,
				phase,
				status: status_for_reason(&reason),
				reason: Some(reason),
				diagnostic_message: Some(message),
				timings,
				parent_wall_ms: None,
				peak_rss_kib: None,
				spill_bytes,
				plan_fingerprint,
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: None,
				engine_version: None,
				duckdb_version: None,
				debug_build: cfg!(debug_assertions),
				local_join_admissions: None,
				join_spill_observations: None,
			};
		}
	};

	let digest_start = Instant::now();
	let digest = match canonical_arrow_ipc_digest(&execution.batches, SortPolicy::FullRowLex) {
		Ok(digest) => format!("{digest:?}"),
		Err(error) => {
			let message = format!("{error:?}");
			let reason = reason_for_error(&message);
			execution.timings.digest_ms = digest_start.elapsed().as_secs_f64() * 1000.0;
			execution.timings.total_ms += execution.timings.digest_ms;
			return PerfAttempt {
				engine: "rune".to_string(),
				query: query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				run_index,
				warmup,
				phase: Phase::Execute,
				status: status_for_reason(reason),
				reason: Some(reason.to_string()),
				diagnostic_message: Some(message),
				timings: execution.timings,
				parent_wall_ms: None,
				peak_rss_kib: None,
				spill_bytes: execution.spill_bytes,
				plan_fingerprint: Some(execution.plan_fingerprint),
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: None,
				engine_version: None,
				duckdb_version: None,
				debug_build: cfg!(debug_assertions),
				local_join_admissions: None,
				join_spill_observations: None,
			};
		}
	};
	execution.timings.digest_ms = digest_start.elapsed().as_secs_f64() * 1000.0;
	execution.timings.total_ms += execution.timings.digest_ms;
	let output_row_count = output_row_count(&execution.batches);
	let output_schema_fingerprint = output_schema_fingerprint(&execution.batches);

	if mode != "strict" {
		return PerfAttempt {
			engine: "rune".to_string(),
			query: query_id.to_string(),
			scale_factor,
			mode: mode.to_string(),
			run_index,
			warmup,
			phase: Phase::Compare,
			status: Status::Fail,
			reason: Some("rune_q03_requires_strict_mode".to_string()),
			diagnostic_message: Some(
				"Rune q03 perf rows are oracle-checked only in strict mode".to_string(),
			),
			timings: execution.timings,
			parent_wall_ms: None,
			peak_rss_kib: None,
			spill_bytes: execution.spill_bytes,
			plan_fingerprint: Some(execution.plan_fingerprint),
			digest: Some(digest),
			output_row_count: Some(output_row_count),
			output_schema_fingerprint,
			engine_version: None,
			duckdb_version: None,
			debug_build: cfg!(debug_assertions),
			local_join_admissions: None,
			join_spill_observations: None,
		};
	}

	if let Err(reason) = compare_strict_query_batches(query_id, scale_factor, &execution.batches) {
		return PerfAttempt {
			engine: "rune".to_string(),
			query: query_id.to_string(),
			scale_factor,
			mode: mode.to_string(),
			run_index,
			warmup,
			phase: Phase::Compare,
			status: Status::Fail,
			reason: Some("rune_q03_oracle_mismatch".to_string()),
			diagnostic_message: Some(reason),
			timings: execution.timings,
			parent_wall_ms: None,
			peak_rss_kib: None,
			spill_bytes: execution.spill_bytes,
			plan_fingerprint: Some(execution.plan_fingerprint),
			digest: Some(digest),
			output_row_count: Some(output_row_count),
			output_schema_fingerprint,
			engine_version: None,
			duckdb_version: None,
			debug_build: cfg!(debug_assertions),
			local_join_admissions: None,
			join_spill_observations: None,
		};
	}

	PerfAttempt {
		engine: "rune".to_string(),
		query: query_id.to_string(),
		scale_factor,
		mode: mode.to_string(),
		run_index,
		warmup,
		phase: Phase::Compare,
		status: Status::Pass,
		reason: None,
		diagnostic_message: None,
		timings: execution.timings,
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: execution.spill_bytes,
		plan_fingerprint: Some(execution.plan_fingerprint),
		digest: Some(digest),
		output_row_count: Some(output_row_count),
		output_schema_fingerprint,
		engine_version: None,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: None,
		join_spill_observations: None,
	}
}

async fn run_perf_query_with_tables(
	query_id: &str,
	mode: &str,
	scale_factor: f64,
	run_index: u32,
	warmup: bool,
	sql: &str,
	tables: Vec<HarnessTable>,
) -> PerfAttempt {
	let mut execution = match execute_query_with_tables_timed(sql, tables).await {
		Ok(execution) => execution,
		Err(StageError {
			phase,
			reason,
			message,
			timings,
			plan_fingerprint,
			spill_bytes,
		}) => {
			return PerfAttempt {
				engine: "toron".to_string(),
				query: query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				run_index,
				warmup,
				phase,
				status: status_for_reason(&reason),
				reason: Some(reason),
				diagnostic_message: Some(message),
				timings,
				parent_wall_ms: None,
				peak_rss_kib: None,
				spill_bytes,
				plan_fingerprint,
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: None,
				engine_version: None,
				duckdb_version: None,
				debug_build: cfg!(debug_assertions),
				local_join_admissions: None,
				join_spill_observations: None,
			};
		}
	};

	let digest_start = Instant::now();
	let digest = match canonical_arrow_ipc_digest(&execution.batches, SortPolicy::FullRowLex) {
		Ok(digest) => format!("{digest:?}"),
		Err(error) => {
			let message = format!("{error:?}");
			let reason = reason_for_error(&message);
			execution.timings.digest_ms = digest_start.elapsed().as_secs_f64() * 1000.0;
			execution.timings.total_ms += execution.timings.digest_ms;
			return PerfAttempt {
				engine: "toron".to_string(),
				query: query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				run_index,
				warmup,
				phase: Phase::Execute,
				status: status_for_reason(reason),
				reason: Some(reason.to_string()),
				diagnostic_message: Some(message),
				timings: execution.timings,
				parent_wall_ms: None,
				peak_rss_kib: None,
				spill_bytes: execution.spill_bytes,
				plan_fingerprint: Some(execution.plan_fingerprint),
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: None,
				engine_version: None,
				duckdb_version: None,
				debug_build: cfg!(debug_assertions),
				local_join_admissions: None,
				join_spill_observations: None,
			};
		}
	};
	execution.timings.digest_ms = digest_start.elapsed().as_secs_f64() * 1000.0;
	execution.timings.total_ms += execution.timings.digest_ms;
	let output_row_count = output_row_count(&execution.batches);
	let output_schema_fingerprint = output_schema_fingerprint(&execution.batches);

	PerfAttempt {
		engine: "toron".to_string(),
		query: query_id.to_string(),
		scale_factor,
		mode: mode.to_string(),
		run_index,
		warmup,
		phase: Phase::Execute,
		status: Status::Pass,
		reason: None,
		diagnostic_message: None,
		timings: execution.timings,
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: execution.spill_bytes,
		plan_fingerprint: Some(execution.plan_fingerprint),
		digest: Some(digest),
		output_row_count: Some(output_row_count),
		output_schema_fingerprint,
		engine_version: None,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: Some(execution.local_join_admissions),
		join_spill_observations: Some(execution.join_spill_observations),
	}
}

async fn run_perf_query_with_metadata(
	query_id: &str,
	mode: &str,
	scale_factor: f64,
	run_index: u32,
	warmup: bool,
	sql: &str,
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
) -> PerfAttempt {
	let mut execution = match execute_query_with_metadata_timed(sql, tables, storage).await {
		Ok(execution) => execution,
		Err(StageError {
			phase,
			reason,
			message,
			timings,
			plan_fingerprint,
			spill_bytes,
		}) => {
			return PerfAttempt {
				engine: "toron".to_string(),
				query: query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				run_index,
				warmup,
				phase,
				status: status_for_reason(&reason),
				reason: Some(reason),
				diagnostic_message: Some(message),
				timings,
				parent_wall_ms: None,
				peak_rss_kib: None,
				spill_bytes,
				plan_fingerprint,
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: None,
				engine_version: None,
				duckdb_version: None,
				debug_build: cfg!(debug_assertions),
				local_join_admissions: None,
				join_spill_observations: None,
			};
		}
	};

	let digest_start = Instant::now();
	let digest = match canonical_arrow_ipc_digest(&execution.batches, SortPolicy::FullRowLex) {
		Ok(digest) => format!("{digest:?}"),
		Err(error) => {
			let message = format!("{error:?}");
			let reason = reason_for_error(&message);
			execution.timings.digest_ms = digest_start.elapsed().as_secs_f64() * 1000.0;
			execution.timings.total_ms += execution.timings.digest_ms;
			return PerfAttempt {
				engine: "toron".to_string(),
				query: query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				run_index,
				warmup,
				phase: Phase::Execute,
				status: status_for_reason(reason),
				reason: Some(reason.to_string()),
				diagnostic_message: Some(message),
				timings: execution.timings,
				parent_wall_ms: None,
				peak_rss_kib: None,
				spill_bytes: execution.spill_bytes,
				plan_fingerprint: Some(execution.plan_fingerprint),
				digest: None,
				output_row_count: None,
				output_schema_fingerprint: None,
				engine_version: None,
				duckdb_version: None,
				debug_build: cfg!(debug_assertions),
				local_join_admissions: None,
				join_spill_observations: None,
			};
		}
	};
	execution.timings.digest_ms = digest_start.elapsed().as_secs_f64() * 1000.0;
	execution.timings.total_ms += execution.timings.digest_ms;
	let output_row_count = output_row_count(&execution.batches);
	let output_schema_fingerprint = output_schema_fingerprint(&execution.batches);

	PerfAttempt {
		engine: "toron".to_string(),
		query: query_id.to_string(),
		scale_factor,
		mode: mode.to_string(),
		run_index,
		warmup,
		phase: Phase::Execute,
		status: Status::Pass,
		reason: None,
		diagnostic_message: None,
		timings: execution.timings,
		parent_wall_ms: None,
		peak_rss_kib: None,
		spill_bytes: execution.spill_bytes,
		plan_fingerprint: Some(execution.plan_fingerprint),
		digest: Some(digest),
		output_row_count: Some(output_row_count),
		output_schema_fingerprint,
		engine_version: None,
		duckdb_version: None,
		debug_build: cfg!(debug_assertions),
		local_join_admissions: Some(execution.local_join_admissions),
		join_spill_observations: Some(execution.join_spill_observations),
	}
}

/// Run one query through the performance rail. This uses the same SQL and fixture constructors as the
/// reachability rail, but it bypasses reachability-only preflight assertions so the timed phases are strictly
/// load, plan, bind, execute, and digest.
pub async fn run_perf_query(
	query: &str,
	mode: &str,
	scale_factor: f64,
	run_index: u32,
	warmup: bool,
) -> Result<PerfAttempt, String> {
	let spec = build_perf_query_spec(query, mode, scale_factor)?;
	Ok(run_perf_query_with_tables(
		spec.query_id,
		mode,
		scale_factor,
		run_index,
		warmup,
		&spec.sql,
		spec.tables,
	)
	.await)
}

pub async fn run_perf_query_with_data_source(
	query: &str,
	mode: &str,
	scale_factor: f64,
	run_index: u32,
	warmup: bool,
	data_source: &str,
	data_root: &Path,
) -> Result<PerfAttempt, String> {
	match data_source {
		"memory" => run_perf_query(query, mode, scale_factor, run_index, warmup).await,
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec(query, mode, scale_factor, data_root)?;
			Ok(run_perf_query_with_metadata(
				spec.query_id,
				mode,
				scale_factor,
				run_index,
				warmup,
				&spec.sql,
				spec.tables,
				spec.storage,
			)
			.await)
		}
		other => Err(format!("perf --data-source {other:?} is not supported")),
	}
}

pub async fn run_rune_perf_query_with_data_source(
	query: &str,
	mode: &str,
	scale_factor: f64,
	run_index: u32,
	warmup: bool,
	data_source: &str,
	data_root: &Path,
) -> Result<PerfAttempt, String> {
	if !matches!(query, "q03" | "q3") {
		return Err(format!(
			"perf --engines rune supports only --query q03; got {query}"
		));
	}
	match data_source {
		"memory" => {
			let spec = build_perf_query_spec("q03", mode, scale_factor)?;
			let (metadata, storage) = metadata_storage_from_tables(spec.tables);
			Ok(run_rune_q03_perf_query_with_metadata(
				spec.query_id,
				mode,
				scale_factor,
				run_index,
				warmup,
				metadata,
				storage,
			)
			.await)
		}
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec("q03", mode, scale_factor, data_root)?;
			Ok(run_rune_q03_perf_query_with_metadata(
				spec.query_id,
				mode,
				scale_factor,
				run_index,
				warmup,
				spec.tables,
				spec.storage,
			)
			.await)
		}
		other => Err(format!("perf --data-source {other:?} is not supported")),
	}
}

pub async fn check_strict_query_with_data_source(
	query: &str,
	scale_factor: f64,
	data_source: &str,
	data_root: &Path,
) -> Result<CorrectnessAttempt, String> {
	validate_query_scale(query, scale_factor)?;
	if query == "q17" {
		q17_correctness_scale(Some(scale_factor))?;
	}
	match data_source {
		"memory" => check_strict_query_memory(query, scale_factor).await,
		"parquet" | "persisted" => {
			check_strict_query_with_persisted_source(query, scale_factor, data_root).await
		}
		other => Err(format!(
			"correctness --data-source {other:?} is not supported"
		)),
	}
}

async fn check_strict_query_memory(
	query: &str,
	scale_factor: f64,
) -> Result<CorrectnessAttempt, String> {
	let attempt = match query {
		"q06" | "q6" => check_strict_q6(scale_factor).await,
		"q01" | "q1" => check_strict_q1(scale_factor).await,
		"q02" | "q2" => check_strict_q02(scale_factor).await,
		"q03" | "q3" => check_strict_q3(scale_factor).await,
		"q04" | "q4" => check_strict_q04(scale_factor).await,
		"q05" | "q5" => check_strict_q5(scale_factor).await,
		"q07" | "q7" => check_strict_q07(scale_factor).await,
		"q08" | "q8" => check_strict_q08(scale_factor).await,
		"q09" | "q9" => check_strict_q09(scale_factor).await,
		"q10" => check_strict_q10(scale_factor).await,
		"q11" => return check_strict_q11(scale_factor).await,
		"q12" => check_strict_q12(scale_factor).await,
		"q13" => check_strict_q13(scale_factor).await,
		"q14" => check_strict_q14(scale_factor).await,
		"q15" => check_strict_q15(scale_factor).await,
		"q16" => check_strict_q16(scale_factor).await,
		"q17" => check_strict_q17(scale_factor).await,
		"q18" => check_strict_q18(scale_factor).await,
		"q19" => check_strict_q19(scale_factor).await,
		"q20" => check_strict_q20(scale_factor).await,
		"q21" => check_strict_q21(scale_factor).await,
		"q22" => check_strict_q22(scale_factor).await,
		other => return Err(format!("correctness --query {other} is not supported")),
	};
	Ok(attempt)
}

async fn check_strict_query_with_persisted_source(
	query: &str,
	scale_factor: f64,
	data_root: &Path,
) -> Result<CorrectnessAttempt, String> {
	let start = Instant::now();
	let spec = build_persisted_perf_query_spec(query, "strict", scale_factor, data_root)?;
	let query_id = spec.query_id;
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: query_id.to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let storage: Arc<dyn StorageScan> = spec.storage;
	let batches = match execute_query_with_metadata_timed(&spec.sql, spec.tables, storage).await {
		Ok(execution) => execution.batches,
		Err(StageError { phase, reason, .. }) => {
			return Ok(outcome(phase, status_for_reason(&reason), Some(reason)));
		}
	};
	Ok(
		match compare_strict_query_batches(query_id, scale_factor, &batches) {
			Ok(()) => outcome(Phase::Compare, Status::Pass, None),
			Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
		},
	)
}

fn compare_strict_query_batches(
	query_id: &str,
	scale_factor: f64,
	batches: &[RecordBatch],
) -> Result<(), String> {
	match query_id {
		"q01" => {
			let oracle = strict_q1_oracle(scale_factor);
			let selected_rows: i64 = oracle.values().map(|group| group.count).sum();
			if selected_rows < 1 || oracle.len() <= 1 {
				return Err(format!(
					"oracle non-vacuous guard failed: selected_rows={selected_rows} groups={}",
					oracle.len()
				));
			}
			compare_strict_q1(batches, &oracle)
		}
		"q02" => {
			let oracle = strict_q02_oracle(scale_factor);
			if oracle.matched_parts == 0 || oracle.rows.is_empty() {
				return Err(format!(
					"oracle non-vacuous guard failed: matched_parts={} rows={}",
					oracle.matched_parts,
					oracle.rows.len()
				));
			}
			compare_strict_q02(batches, &oracle)
		}
		"q03" => {
			let oracle = strict_q3_oracle(scale_factor);
			if oracle.selected_rows < 1 || oracle.groups.len() <= 1 {
				return Err(format!(
					"oracle non-vacuous guard failed: selected_rows={} groups={}",
					oracle.selected_rows,
					oracle.groups.len()
				));
			}
			compare_strict_q3(batches, &oracle.groups)
		}
		"q04" => {
			let oracle = strict_q04_oracle(scale_factor);
			compare_strict_q04(batches, &oracle)
		}
		"q05" => {
			let oracle = strict_q5_oracle(scale_factor);
			if oracle.selected_rows < 1 || oracle.groups.len() <= 1 {
				return Err(format!(
					"oracle non-vacuous guard failed: selected_rows={} groups={}",
					oracle.selected_rows,
					oracle.groups.len()
				));
			}
			compare_strict_q5(batches, &oracle.groups)
		}
		"q06" => {
			let (expected, matched) = strict_q6_oracle(scale_factor);
			if matched == 0 {
				return Err("oracle matched zero rows at this scale".to_string());
			}
			compare_strict_q6(batches, expected)
		}
		"q07" => {
			let oracle = strict_q07_oracle(scale_factor);
			let missing_keys: Vec<String> = q07_expected_group_keys()
				.iter()
				.filter(|(supp, cust, year)| {
					!oracle
						.groups
						.contains_key(&(supp.to_string(), cust.to_string(), *year))
				})
				.map(|(supp, cust, year)| format!("({supp},{cust},{year})"))
				.collect();
			if oracle.selected_rows < 1 || oracle.groups.len() != 4 || !missing_keys.is_empty() {
				return Err(format!(
					"oracle non-vacuous guard failed: selected_rows={} groups={} missing={missing_keys:?}",
					oracle.selected_rows,
					oracle.groups.len()
				));
			}
			compare_strict_q07(batches, &oracle)
		}
		"q08" => {
			let oracle = strict_q08_oracle(scale_factor)?;
			compare_strict_q08(batches, &oracle)
		}
		"q09" => {
			let oracle = strict_q09_oracle(scale_factor);
			compare_strict_q09(batches, &oracle)
		}
		"q10" => {
			let oracle = strict_q10_oracle(scale_factor);
			if oracle.selected_rows <= 1 || oracle.rows.len() <= 1 {
				return Err(format!(
					"oracle non-vacuous guard failed: selected_rows={} rows={}",
					oracle.selected_rows,
					oracle.rows.len()
				));
			}
			compare_strict_q10(batches, &oracle)
		}
		"q11" => {
			let oracle = strict_q11_oracle(scale_factor)?;
			compare_strict_q11(batches, &oracle)
		}
		"q12" => {
			let oracle = strict_q12_oracle(scale_factor);
			compare_strict_q12(batches, &oracle)
		}
		"q13" => {
			let oracle = strict_q13_oracle(scale_factor);
			compare_strict_q13(batches, &oracle)
		}
		"q14" => {
			let oracle = strict_q14_oracle(scale_factor)?;
			compare_strict_q14(batches, &oracle)
		}
		"q15" => {
			let oracle = strict_q15_oracle(scale_factor)?;
			compare_strict_q15(batches, &oracle)
		}
		"q16" => {
			let oracle = strict_q16_oracle(scale_factor);
			compare_strict_q16(batches, &oracle)
		}
		"q17" => {
			let oracle = strict_q17_oracle(scale_factor)?;
			compare_strict_q17(batches, &oracle)
		}
		"q18" => {
			let oracle = strict_q18_oracle(scale_factor)?;
			compare_strict_q18(batches, &oracle)
		}
		"q19" => {
			let oracle = strict_q19_oracle(scale_factor)?;
			compare_strict_q19(batches, &oracle)
		}
		"q20" => {
			let oracle = strict_q20_oracle(scale_factor);
			compare_strict_q20(batches, &oracle)
		}
		"q21" => {
			let oracle = strict_q21_oracle(scale_factor)?;
			compare_strict_q21(batches, &oracle)
		}
		"q22" => {
			let oracle = strict_q22_oracle(scale_factor)?;
			compare_strict_q22(batches, &oracle)
		}
		other => Err(format!("correctness --query {other} is not supported")),
	}
}

pub fn build_persisted_perf_query_spec(
	query: &str,
	mode: &str,
	scale_factor: f64,
	root: &Path,
) -> Result<PersistedPerfQuerySpec, String> {
	let strict = match mode {
		"strict" => true,
		"dev" => false,
		other => return Err(format!("perf --mode {other} is not supported")),
	};
	validate_query_scale(query, scale_factor)?;
	let (query_id, sql) = perf_query_sql(query, strict, scale_factor)?;
	let dataset = prepare_persisted_tpch_dataset(mode, scale_factor, root)?;
	let table_names = persisted_query_table_names(query)?;
	let tables = persisted_query_tables(&dataset, strict, &table_names)?;
	let mut scan_tables = HashMap::new();
	let mut export_tables = Vec::new();
	for table in &tables {
		let persisted = dataset
			.tables
			.get(table.name)
			.ok_or_else(|| format!("persisted dataset is missing table {}", table.name))?;
		let schema = table_schema_for_name(table.name, strict)?;
		scan_tables.insert(
			table.id,
			ParquetHarnessTableSource {
				name: persisted.name.clone(),
				path: persisted.path.clone(),
				schema,
			},
		);
		export_tables.push(TpchParquetTableExport {
			name: persisted.name.clone(),
			path: persisted.path.clone(),
			row_count: persisted.row_count,
			schema_fingerprint: persisted.schema_fingerprint.clone(),
			schema_signature: persisted.schema_signature.clone(),
			file_hash: persisted.file_hash.clone(),
		});
	}
	let export = TpchParquetExport {
		query_id: query_id.to_string(),
		mode: mode.to_string(),
		scale_factor,
		sql: sql.to_string(),
		root_dir: dataset.root_dir.clone(),
		manifest_path: dataset.manifest_path.clone(),
		manifest_fingerprint: stable_fingerprint(&format!(
			"query={query_id};dataset={};tables={}",
			dataset.manifest_fingerprint,
			export_tables
				.iter()
				.map(|table| table.name.as_str())
				.collect::<Vec<_>>()
				.join(",")
		)),
		tables: export_tables,
	};
	Ok(PersistedPerfQuerySpec {
		query_id,
		sql,
		tables,
		storage: Arc::new(ParquetHarnessScan {
			tables: scan_tables,
		}),
		export,
	})
}

pub fn export_persisted_perf_query_parquet(
	query: &str,
	mode: &str,
	scale_factor: f64,
	root: &Path,
) -> Result<TpchParquetExport, String> {
	build_persisted_perf_query_spec(query, mode, scale_factor, root).map(|spec| spec.export)
}

pub fn build_perf_query_spec(
	query: &str,
	mode: &str,
	scale_factor: f64,
) -> Result<PerfQuerySpec, String> {
	let strict = match mode {
		"strict" => true,
		"dev" => false,
		other => return Err(format!("perf --mode {other} is not supported")),
	};
	validate_query_scale(query, scale_factor)?;
	let (query_id, sql, tables) = perf_query_spec(query, strict, scale_factor)?;
	Ok(PerfQuerySpec {
		query_id,
		sql,
		tables,
	})
}

fn perf_query_sql(
	query: &str,
	strict: bool,
	scale_factor: f64,
) -> Result<(&'static str, Cow<'static, str>), String> {
	let query_sql = match query {
		// m00 proves m01 actually scans: a metadata/count shortcut would make m01
		// far cheaper than the summing form over the same rows.
		"m00" => ("m00", Cow::Borrowed(M00_SQL)),
		"m01" => ("m01", Cow::Borrowed(M01_SQL)),
		"m02" => ("m02", Cow::Borrowed(M02_SQL)),
		"m03" => ("m03", Cow::Borrowed(M03_SQL)),
		"m04" => ("m04", Cow::Borrowed(M04_SQL)),
		"m05" => ("m05", Cow::Borrowed(M05_SQL)),
		"m06" => ("m06", Cow::Borrowed(M06_SQL)),
		"m07" => ("m07", Cow::Borrowed(M07_SQL)),
		"m08" => ("m08", Cow::Borrowed(M08_SQL)),
		"m09" => ("m09", Cow::Borrowed(M09_SQL)),
		"m10" => ("m10", Cow::Borrowed(M10_SQL)),
		"q01" | "q1" => (
			"q01",
			Cow::Borrowed(if strict { STRICT_Q1_SQL } else { Q1_DEV_SQL }),
		),
		"q06" | "q6" => (
			"q06",
			Cow::Borrowed(if strict { STRICT_Q6_SQL } else { Q6_DEV_SQL }),
		),
		"q02" | "q2" => (
			"q02",
			Cow::Borrowed(if strict { STRICT_Q2_SQL } else { Q2_DEV_SQL }),
		),
		"q03" | "q3" => (
			"q03",
			Cow::Borrowed(if strict { STRICT_Q3_SQL } else { Q3_DEV_SQL }),
		),
		"q04" | "q4" => (
			"q04",
			Cow::Borrowed(if strict { STRICT_Q4_SQL } else { Q4_DEV_SQL }),
		),
		"q05" | "q5" => (
			"q05",
			Cow::Borrowed(if strict { STRICT_Q5_SQL } else { Q5_DEV_SQL }),
		),
		"q07" | "q7" => (
			"q07",
			Cow::Borrowed(if strict { STRICT_Q7_SQL } else { Q7_DEV_SQL }),
		),
		"q08" | "q8" => (
			"q08",
			Cow::Borrowed(if strict { STRICT_Q8_SQL } else { Q8_DEV_SQL }),
		),
		"q09" | "q9" => (
			"q09",
			Cow::Borrowed(if strict { STRICT_Q9_SQL } else { Q9_DEV_SQL }),
		),
		"q10" => (
			"q10",
			Cow::Borrowed(if strict { STRICT_Q10_SQL } else { Q10_DEV_SQL }),
		),
		"q11" => (
			"q11",
			Cow::Owned(q11_sql(Q11Scale::try_from_f64(scale_factor)?)),
		),
		"q12" => (
			"q12",
			Cow::Borrowed(if strict { STRICT_Q12_SQL } else { Q12_DEV_SQL }),
		),
		"q13" => (
			"q13",
			Cow::Borrowed(if strict { STRICT_Q13_SQL } else { Q13_DEV_SQL }),
		),
		"q14" => (
			"q14",
			Cow::Borrowed(if strict { STRICT_Q14_SQL } else { Q14_DEV_SQL }),
		),
		"q15" => (
			"q15",
			Cow::Borrowed(if strict { STRICT_Q15_SQL } else { Q15_DEV_SQL }),
		),
		"q16" => (
			"q16",
			Cow::Borrowed(if strict { STRICT_Q16_SQL } else { Q16_DEV_SQL }),
		),
		"q17" => (
			"q17",
			Cow::Borrowed(if strict { STRICT_Q17_SQL } else { Q17_DEV_SQL }),
		),
		"q18" => (
			"q18",
			Cow::Borrowed(if strict { STRICT_Q18_SQL } else { Q18_DEV_SQL }),
		),
		"q19" => (
			"q19",
			Cow::Borrowed(if strict { STRICT_Q19_SQL } else { Q19_DEV_SQL }),
		),
		"q20" => (
			"q20",
			Cow::Borrowed(if strict { STRICT_Q20_SQL } else { Q20_DEV_SQL }),
		),
		"q21" => (
			"q21",
			Cow::Borrowed(if strict { STRICT_Q21_SQL } else { Q21_DEV_SQL }),
		),
		"q22" => (
			"q22",
			Cow::Borrowed(if strict { STRICT_Q22_SQL } else { Q22_DEV_SQL }),
		),
		other => return Err(format!("perf --query {other} is not supported")),
	};
	Ok(query_sql)
}

fn perf_query_spec(
	query: &str,
	strict: bool,
	scale_factor: f64,
) -> Result<(&'static str, Cow<'static, str>, Vec<HarnessTable>), String> {
	let (query_id, sql) = perf_query_sql(query, strict, scale_factor)?;
	let one_lineitem_table = || {
		let batch = if strict {
			generate_strict_lineitem(scale_factor)
		} else {
			generate_dev_lineitem(scale_factor)
		};
		let descriptor = if strict {
			strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
		} else {
			dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
		};
		vec![HarnessTable {
			id: TableId(1),
			name: "lineitem",
			descriptor,
			batch,
			facts: TableOptimizerFacts::default(),
		}]
	};
	let tables = match query {
		"q01" | "q1" | "q06" | "q6" => one_lineitem_table(),
		"q02" | "q2" => q02_tables(strict, scale_factor),
		"q03" | "q3" => q03_tables(strict, scale_factor),
		"q04" | "q4" => q04_tables(strict, scale_factor),
		"q05" | "q5" => q05_tables(strict, scale_factor),
		"q07" | "q7" => q07_tables(strict, scale_factor),
		"q08" | "q8" => q08_tables(strict, scale_factor),
		"q09" | "q9" => q09_tables(strict, scale_factor),
		"q10" => q10_tables(strict, scale_factor),
		"q11" => q11_tables(strict, scale_factor),
		"q12" => q12_tables(strict, scale_factor),
		"q13" => q13_tables(strict, scale_factor),
		"q14" => q14_tables(strict, scale_factor),
		"q15" => q15_tables(strict, scale_factor),
		"q16" => q16_tables(strict, scale_factor),
		"q17" => q17_tables(strict, scale_factor),
		"q18" => q18_tables(strict, scale_factor),
		"q19" => q19_tables(strict, scale_factor),
		"q20" => q20_tables(strict, scale_factor),
		"q21" => q21_tables(strict, scale_factor),
		"q22" => q22_tables(strict, scale_factor),
		other => return Err(format!("perf --query {other} is not supported")),
	};
	Ok((query_id, sql, tables))
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
		Err(StageError { phase, reason, .. }) => {
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
		Err(StageError { phase, reason, .. }) => {
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

/// ADR 280: one q03 result group, keyed by the unique `l_orderkey` (a given orderkey carries exactly one
/// `(o_orderdate, o_shippriority)` from its 1:1 order). `revenue` is raw `i128` at scale 4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q3Group {
	pub revenue: i128,
	pub orderdate: i32,
	pub shippriority: i32,
}

/// ADR 280: the independent strict-q03 oracle result — the sorted group map plus the count of contributing
/// lineitem rows (the non-vacuity guard reads `selected_rows ≥ 1` AND `groups.len() > 1`).
pub struct Q3Oracle {
	pub groups: BTreeMap<i64, Q3Group>,
	pub selected_rows: usize,
}

fn discounted_price_raw(extprice_raw: i128, discount_raw: i128) -> i128 {
	extprice_raw * (100 - discount_raw)
}

/// ADR 280: the independent strict-q03 oracle — traverse the Customer/Order/LineItem generators directly,
/// apply q03's `c_mktsegment = 'BUILDING'` + date filters and the two join keys, and accumulate raw scale-4
/// revenue per `l_orderkey`. `DATE '1995-03-15'` is day-count `9204` (cross-check: q01 used `10471` for
/// `1998-09-02`; the 1267-day gap gives `9204 + 1267 = 10471`). `revenue` mirrors q01's `sum_disc_price`:
/// `extprice_raw * (100 - disc_raw)` at scale 4 — no float, no tolerance.
pub fn strict_q3_oracle(scale_factor: f64) -> Q3Oracle {
	const CUTOFF: i32 = 9204; // DATE '1995-03-15' as a Date32 day-count.

	let mut building_customers: HashSet<i64> = HashSet::new();
	for customer in CustomerGenerator::new(scale_factor, 1, 1).iter() {
		if customer.c_mktsegment == "BUILDING" {
			building_customers.insert(customer.c_custkey);
		}
	}

	let mut orders: HashMap<i64, (i32, i32)> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		if order.o_orderdate.to_unix_epoch() < CUTOFF
			&& building_customers.contains(&order.o_custkey)
		{
			orders.insert(
				order.o_orderkey,
				(order.o_orderdate.to_unix_epoch(), order.o_shippriority),
			);
		}
	}

	let mut groups: BTreeMap<i64, Q3Group> = BTreeMap::new();
	let mut selected_rows = 0usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		if item.l_shipdate.to_unix_epoch() <= CUTOFF {
			continue;
		}
		let Some(&(orderdate, shippriority)) = orders.get(&item.l_orderkey) else {
			continue;
		};
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let disc_raw = item.l_discount.into_inner() as i128;
		let group = groups.entry(item.l_orderkey).or_insert(Q3Group {
			revenue: 0,
			orderdate,
			shippriority,
		});
		group.revenue += discounted_price_raw(extprice_raw, disc_raw);
		selected_rows += 1;
	}

	Q3Oracle {
		groups,
		selected_rows,
	}
}

/// strict q03's measured output schema (read from the executor's actual output — `revenue` is q01's
/// `sum_disc_price` expression, measured `Decimal128(32,4)` in ADR 276).
fn strict_q3_expected_schema() -> [(&'static str, DataType); 4] {
	[
		("l_orderkey", DataType::Int64),
		("revenue", DataType::Decimal128(32, 4)),
		("o_orderdate", DataType::Date32),
		("o_shippriority", DataType::Int32),
	]
}

/// Assert strict q03's actual output schema matches [`strict_q3_expected_schema`] (column count, names, exact
/// `(p,s)`) before any value compare. A drift is a compare failure — never a wrong-scale value comparison.
pub fn assert_strict_q3_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q3_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q03 produced no output batch".to_string());
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

/// Compare strict q03's output to the independent oracle. Assert the schema, then verify the two things SQL
/// specifies: (1) **answer-correctness** — the emitted rows form a bijection with the oracle groups keyed by
/// the unique `l_orderkey` (a duplicate emitted key is rejected up front, not silently overwritten), with each
/// cell's raw value matching exactly; (2) **order-correctness** — the emitted sequence respects the `ORDER BY
/// revenue DESC, o_orderdate` prefix. Ties on `l_orderkey`/`o_shippriority` are unspecified by SQL (the
/// executor's `bind_sort_keys` totalizes — an implementation detail this comparator does not pin). Any
/// mismatch → `Err` (`fail@compare`).
pub fn compare_strict_q3(
	batches: &[RecordBatch],
	oracle: &BTreeMap<i64, Q3Group>,
) -> Result<(), String> {
	assert_strict_q3_schema(batches)?;

	// Emitted rows, in emit order (used as-is for the order check; deduped into a map for the bijection).
	let mut emitted: Vec<(i64, i128, i32, i32)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 4 {
			return Err(format!("expected 4 columns, got {}", batch.num_columns()));
		}
		let orderkeys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 0 (l_orderkey) not Int64")?;
		let revenues = batch
			.column(1)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 1 (revenue) not Decimal128")?;
		let orderdates = batch
			.column(2)
			.as_any()
			.downcast_ref::<Date32Array>()
			.ok_or("column 2 (o_orderdate) not Date32")?;
		let shippriorities = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int32Array>()
			.ok_or("column 3 (o_shippriority) not Int32")?;
		for i in 0..batch.num_rows() {
			if orderkeys.is_null(i)
				|| revenues.is_null(i)
				|| orderdates.is_null(i)
				|| shippriorities.is_null(i)
			{
				return Err(format!("unexpected NULL in emitted row {}", emitted.len()));
			}
			emitted.push((
				orderkeys.value(i),
				revenues.value(i),
				orderdates.value(i),
				shippriorities.value(i),
			));
		}
	}

	// (1) Answer-correctness: a bijection with the oracle, keyed by the unique l_orderkey.
	if emitted.len() != oracle.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<i64, (i128, i32, i32)> = BTreeMap::new();
	for &(orderkey, revenue, orderdate, shippriority) in &emitted {
		if seen
			.insert(orderkey, (revenue, orderdate, shippriority))
			.is_some()
		{
			return Err(format!("duplicate emitted l_orderkey {orderkey}"));
		}
	}
	// |seen| == |oracle| and every oracle key present below ⇒ the key sets are equal (a bijection).
	for (orderkey, group) in oracle {
		let Some(&(revenue, orderdate, shippriority)) = seen.get(orderkey) else {
			return Err(format!("oracle l_orderkey {orderkey} missing from output"));
		};
		if revenue != group.revenue {
			return Err(format!(
				"l_orderkey {orderkey}: revenue mismatch: expected {}, got {revenue}",
				group.revenue
			));
		}
		if orderdate != group.orderdate {
			return Err(format!(
				"l_orderkey {orderkey}: o_orderdate mismatch: expected {}, got {orderdate}",
				group.orderdate
			));
		}
		if shippriority != group.shippriority {
			return Err(format!(
				"l_orderkey {orderkey}: o_shippriority mismatch: expected {}, got {shippriority}",
				group.shippriority
			));
		}
	}

	// (2) Order-correctness: ORDER BY revenue DESC, o_orderdate (ASC) — the prefix SQL specifies.
	for pair in emitted.windows(2) {
		let (_, rev_a, date_a, _) = pair[0];
		let (_, rev_b, date_b, _) = pair[1];
		if rev_a < rev_b {
			return Err(format!(
				"ORDER BY revenue DESC violated: {rev_a} emitted before {rev_b}"
			));
		}
		if rev_a == rev_b && date_a > date_b {
			return Err(format!(
				"ORDER BY o_orderdate ASC violated within equal revenue: {date_a} emitted before {date_b}"
			));
		}
	}

	Ok(())
}

/// ADR 280: the strict-q03 correctness check — execute strict q03, assert its schema, guard non-vacuity (≥ 1
/// contributing lineitem row AND > 1 group), then verify the answer bijection + `ORDER BY` prefix against the
/// independent oracle. A mismatch is `fail@compare`; a query that can't execute carries its deepest phase.
pub async fn check_strict_q3(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q03_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q03".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q3_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q3_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q3_oracle(scale_factor);
	if oracle.selected_rows < 1 || oracle.groups.len() <= 1 {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!(
				"oracle non-vacuous guard failed: selected_rows={} groups={}",
				oracle.selected_rows,
				oracle.groups.len()
			)),
		);
	}
	match compare_strict_q3(&batches, &oracle.groups) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q5Group {
	pub revenue: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q5StageCensus {
	pub dated_orders: usize,
	pub lineitem_rows: usize,
	pub dated_order_lineitems: usize,
	pub supplier_rows: usize,
	pub same_nation_rows: usize,
	pub nation_rows: usize,
	pub asia_region_rows: usize,
	pub asia_rows: usize,
}

pub struct Q5Oracle {
	pub groups: BTreeMap<String, Q5Group>,
	pub selected_rows: usize,
	pub stage_census: Q5StageCensus,
}

/// ADR 287: the independent strict-q05 oracle — traverse the six q05 generators directly, apply every join
/// gate including the cyclic `c_nationkey = s_nationkey`, then group raw scale-4 revenue by `n_name`.
pub fn strict_q5_oracle(scale_factor: f64) -> Q5Oracle {
	const START_1994: i32 = 8766;
	const START_1995: i32 = 9131;

	let mut asia_regions: HashSet<i64> = HashSet::new();
	for region in RegionGenerator::new(scale_factor, 1, 1).iter() {
		if region.r_name == "ASIA" {
			asia_regions.insert(region.r_regionkey);
		}
	}
	let asia_region_rows = asia_regions.len();

	let mut asia_nations: HashMap<i64, String> = HashMap::new();
	let mut nation_rows = 0usize;
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		q5_increment(&mut nation_rows, "nation rows");
		if asia_regions.contains(&nation.n_regionkey) {
			asia_nations.insert(nation.n_nationkey, nation.n_name.to_string());
		}
	}

	let mut suppliers: HashMap<i64, i64> = HashMap::new();
	for supplier in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		suppliers.insert(supplier.s_suppkey, supplier.s_nationkey);
	}
	let supplier_rows = suppliers.len();

	let mut customers: HashMap<i64, i64> = HashMap::new();
	for customer in CustomerGenerator::new(scale_factor, 1, 1).iter() {
		customers.insert(customer.c_custkey, customer.c_nationkey);
	}

	let mut orders: HashMap<i64, i64> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		let orderdate = order.o_orderdate.to_unix_epoch();
		if (START_1994..START_1995).contains(&orderdate) {
			orders.insert(order.o_orderkey, order.o_custkey);
		}
	}
	let dated_orders = orders.len();

	let mut groups: BTreeMap<String, Q5Group> = BTreeMap::new();
	let mut selected_rows = 0usize;
	let mut lineitem_rows = 0usize;
	let mut dated_order_lineitems = 0usize;
	let mut same_nation_rows = 0usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		q5_increment(&mut lineitem_rows, "lineitem rows");
		let Some(&custkey) = orders.get(&item.l_orderkey) else {
			continue;
		};
		q5_increment(&mut dated_order_lineitems, "dated-order lineitems");
		let Some(&customer_nationkey) = customers.get(&custkey) else {
			continue;
		};
		let Some(&supplier_nationkey) = suppliers.get(&item.l_suppkey) else {
			continue;
		};
		if customer_nationkey != supplier_nationkey {
			continue;
		}
		q5_increment(&mut same_nation_rows, "same-nation rows");
		let Some(nation_name) = asia_nations.get(&supplier_nationkey) else {
			continue;
		};
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let disc_raw = item.l_discount.into_inner() as i128;
		groups
			.entry(nation_name.clone())
			.or_insert(Q5Group { revenue: 0 })
			.revenue += discounted_price_raw(extprice_raw, disc_raw);
		q5_increment(&mut selected_rows, "selected rows");
	}

	Q5Oracle {
		groups,
		selected_rows,
		stage_census: Q5StageCensus {
			dated_orders,
			lineitem_rows,
			dated_order_lineitems,
			supplier_rows,
			same_nation_rows,
			nation_rows,
			asia_region_rows,
			asia_rows: selected_rows,
		},
	}
}

#[inline]
fn q5_increment(counter: &mut usize, label: &'static str) {
	*counter = counter
		.checked_add(1)
		.unwrap_or_else(|| panic!("q05 oracle {label} overflow"));
}

/// strict q05's measured output schema (Phase-0 measured at sf=0.01): q05's revenue expression is q03's
/// `sum_disc_price` shape, and the executor returns scale-4 revenue.
fn strict_q5_expected_schema() -> [(&'static str, DataType); 2] {
	[
		("n_name", DataType::Utf8),
		("revenue", DataType::Decimal128(32, 4)),
	]
}

pub fn assert_strict_q5_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q5_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q05 produced no output batch".to_string());
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

/// Compare strict q05's grouped output to the independent oracle. Answers are matched by unique `n_name`;
/// ordering is only the SQL-specified `ORDER BY revenue DESC` prefix.
pub fn compare_strict_q5(
	batches: &[RecordBatch],
	oracle: &BTreeMap<String, Q5Group>,
) -> Result<(), String> {
	assert_strict_q5_schema(batches)?;

	let mut emitted: Vec<(String, i128)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let names = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (n_name) not Utf8")?;
		let revenues = batch
			.column(1)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 1 (revenue) not Decimal128")?;
		for i in 0..batch.num_rows() {
			if names.is_null(i) {
				return Err(format!("column n_name row {} is NULL", emitted.len()));
			}
			if revenues.is_null(i) {
				return Err(format!("column revenue row {} is NULL", emitted.len()));
			}
			emitted.push((names.value(i).to_string(), revenues.value(i)));
		}
	}

	if emitted.len() != oracle.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<String, i128> = BTreeMap::new();
	for (name, revenue) in &emitted {
		if seen.insert(name.clone(), *revenue).is_some() {
			return Err(format!("duplicate emitted n_name {name:?}"));
		}
	}
	for (name, group) in oracle {
		let Some(&revenue) = seen.get(name) else {
			return Err(format!("oracle n_name {name:?} missing from output"));
		};
		if revenue != group.revenue {
			return Err(format!(
				"n_name {name:?}: revenue mismatch: expected {}, got {revenue}",
				group.revenue
			));
		}
	}

	for pair in emitted.windows(2) {
		let (_, rev_a) = &pair[0];
		let (_, rev_b) = &pair[1];
		if rev_a < rev_b {
			return Err(format!(
				"ORDER BY revenue DESC violated: {rev_a} emitted before {rev_b}"
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q5(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q05_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q05".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q5_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q5_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q5_oracle(scale_factor);
	if oracle.selected_rows < 1 || oracle.groups.len() <= 1 {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!(
				"oracle non-vacuous guard failed: selected_rows={} groups={}",
				oracle.selected_rows,
				oracle.groups.len()
			)),
		);
	}
	match compare_strict_q5(&batches, &oracle.groups) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone)]
pub struct Q07Oracle {
	pub groups: BTreeMap<(String, String, i32), i128>,
	pub selected_rows: usize,
}

fn q07_ship_year(shipdate: i32) -> Option<i32> {
	const START_1995: i32 = 9131;
	const START_1996: i32 = 9496;
	const START_1997: i32 = 9862;

	match shipdate {
		START_1995..START_1996 => Some(1995),
		START_1996..START_1997 => Some(1996),
		_ => None,
	}
}

pub fn q07_contribution(
	supp_nation: &str,
	cust_nation: &str,
	shipdate: i32,
	extprice_raw: i128,
	discount_raw: i128,
) -> Option<((String, String, i32), i128)> {
	if !((supp_nation == "FRANCE" && cust_nation == "GERMANY")
		|| (supp_nation == "GERMANY" && cust_nation == "FRANCE"))
	{
		return None;
	}
	let l_year = q07_ship_year(shipdate)?;
	let key = (supp_nation.to_string(), cust_nation.to_string(), l_year);
	Some((key, discounted_price_raw(extprice_raw, discount_raw)))
}

/// ADR 299: the independent strict-q07 oracle. Resolve q07's nation self-join explicitly: supplier nation
/// comes through `l_suppkey`, customer nation through `l_orderkey -> o_custkey`.
pub fn strict_q07_oracle(scale_factor: f64) -> Q07Oracle {
	let mut nations: HashMap<i64, String> = HashMap::new();
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		nations.insert(nation.n_nationkey, nation.n_name.to_string());
	}

	let mut suppliers: HashMap<i64, i64> = HashMap::new();
	for supplier in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		suppliers.insert(supplier.s_suppkey, supplier.s_nationkey);
	}

	let mut customers: HashMap<i64, i64> = HashMap::new();
	for customer in CustomerGenerator::new(scale_factor, 1, 1).iter() {
		customers.insert(customer.c_custkey, customer.c_nationkey);
	}

	let mut orders: HashMap<i64, i64> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		orders.insert(order.o_orderkey, order.o_custkey);
	}

	let mut groups: BTreeMap<(String, String, i32), i128> = BTreeMap::new();
	let mut selected_rows = 0_usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		let Some(&supplier_nationkey) = suppliers.get(&item.l_suppkey) else {
			continue;
		};
		let Some(supp_nation) = nations.get(&supplier_nationkey) else {
			continue;
		};
		let Some(&custkey) = orders.get(&item.l_orderkey) else {
			continue;
		};
		let Some(&customer_nationkey) = customers.get(&custkey) else {
			continue;
		};
		let Some(cust_nation) = nations.get(&customer_nationkey) else {
			continue;
		};
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let discount_raw = item.l_discount.into_inner() as i128;
		let Some((key, revenue)) = q07_contribution(
			supp_nation,
			cust_nation,
			item.l_shipdate.to_unix_epoch(),
			extprice_raw,
			discount_raw,
		) else {
			continue;
		};
		*groups.entry(key).or_default() += revenue;
		selected_rows += 1;
	}

	Q07Oracle {
		groups,
		selected_rows,
	}
}

fn strict_q07_expected_schema() -> [(&'static str, DataType); 4] {
	[
		("supp_nation", DataType::Utf8),
		("cust_nation", DataType::Utf8),
		("l_year", DataType::Int32),
		("revenue", DataType::Decimal128(32, 4)),
	]
}

pub fn assert_strict_q07_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q07_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q07 produced no output batch".to_string());
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

pub fn compare_strict_q07(batches: &[RecordBatch], oracle: &Q07Oracle) -> Result<(), String> {
	assert_strict_q07_schema(batches)?;

	let mut emitted: Vec<((String, String, i32), i128)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 4 {
			return Err(format!("expected 4 columns, got {}", batch.num_columns()));
		}
		let supp_nations = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (supp_nation) not Utf8")?;
		let cust_nations = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 1 (cust_nation) not Utf8")?;
		let years = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int32Array>()
			.ok_or("column 2 (l_year) not Int32")?;
		let revenues = batch
			.column(3)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 3 (revenue) not Decimal128")?;
		for i in 0..batch.num_rows() {
			if supp_nations.is_null(i) {
				return Err(format!("column supp_nation row {} is NULL", emitted.len()));
			}
			if cust_nations.is_null(i) {
				return Err(format!("column cust_nation row {} is NULL", emitted.len()));
			}
			if years.is_null(i) {
				return Err(format!("column l_year row {} is NULL", emitted.len()));
			}
			if revenues.is_null(i) {
				return Err(format!("column revenue row {} is NULL", emitted.len()));
			}
			emitted.push((
				(
					supp_nations.value(i).to_string(),
					cust_nations.value(i).to_string(),
					years.value(i),
				),
				revenues.value(i),
			));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<(String, String, i32), i128> = BTreeMap::new();
	for (key, revenue) in &emitted {
		if seen.insert(key.clone(), *revenue).is_some() {
			return Err(format!("duplicate emitted q07 group {key:?}"));
		}
	}
	for (key, expected_revenue) in &oracle.groups {
		let Some(actual_revenue) = seen.get(key) else {
			return Err(format!("oracle q07 group {key:?} missing from output"));
		};
		if actual_revenue != expected_revenue {
			return Err(format!(
				"q07 group {key:?}: revenue mismatch: expected {expected_revenue}, got {actual_revenue}"
			));
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].0 > pair[1].0 {
			return Err(format!(
				"ORDER BY supp_nation, cust_nation, l_year violated: {:?} emitted before {:?}",
				pair[0].0, pair[1].0
			));
		}
	}

	Ok(())
}

fn q07_expected_group_keys() -> [(&'static str, &'static str, i32); 4] {
	[
		("FRANCE", "GERMANY", 1995),
		("FRANCE", "GERMANY", 1996),
		("GERMANY", "FRANCE", 1995),
		("GERMANY", "FRANCE", 1996),
	]
}

pub async fn check_strict_q07(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q07_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q07".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q7_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q07_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q07_oracle(scale_factor);
	let missing_keys: Vec<String> = q07_expected_group_keys()
		.iter()
		.filter(|(supp, cust, year)| {
			!oracle
				.groups
				.contains_key(&(supp.to_string(), cust.to_string(), *year))
		})
		.map(|(supp, cust, year)| format!("({supp},{cust},{year})"))
		.collect();
	if oracle.selected_rows < 1 || oracle.groups.len() != 4 || !missing_keys.is_empty() {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!(
				"oracle non-vacuous guard failed: selected_rows={} groups={} missing={missing_keys:?}",
				oracle.selected_rows,
				oracle.groups.len()
			)),
		);
	}
	match compare_strict_q07(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone)]
pub struct Q08Oracle {
	pub groups: BTreeMap<i32, DecimalRaw>,
	pub denominator_rows: usize,
	pub brazil_rows: usize,
}

fn q08_order_year(orderdate: i32) -> Option<i32> {
	q07_ship_year(orderdate)
}

pub fn q08_market_share_raw(
	numerator_raw: i128,
	denominator_raw: i128,
) -> Result<DecimalRaw, String> {
	decimal_division_raw(numerator_raw, 32, 4, denominator_raw, 32, 4)
}

pub fn q08_contribution(
	part_type: &str,
	customer_region: &str,
	supplier_nation: &str,
	orderdate: i32,
	extprice_raw: i128,
	discount_raw: i128,
) -> Option<(i32, i128, bool)> {
	if part_type != "ECONOMY ANODIZED STEEL" {
		return None;
	}
	if customer_region != "AMERICA" {
		return None;
	}
	let o_year = q08_order_year(orderdate)?;
	let volume = discounted_price_raw(extprice_raw, discount_raw);
	Some((o_year, volume, supplier_nation == "BRAZIL"))
}

/// ADR 300: the independent strict-q08 oracle. Resolve the `nation` self-join roles explicitly: customer nation
/// supplies the AMERICA region filter, supplier nation supplies the BRAZIL CASE branch.
pub fn strict_q08_oracle(scale_factor: f64) -> Result<Q08Oracle, String> {
	let america_regions: HashSet<i64> = RegionGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter(|region| region.r_name == "AMERICA")
		.map(|region| region.r_regionkey)
		.collect();

	let mut nations: HashMap<i64, (String, i64)> = HashMap::new();
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		nations.insert(
			nation.n_nationkey,
			(nation.n_name.to_string(), nation.n_regionkey),
		);
	}

	let economy_anodized_steel_parts: HashSet<i64> = PartGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter(|part| part.p_type == "ECONOMY ANODIZED STEEL")
		.map(|part| part.p_partkey)
		.collect();

	let mut suppliers: HashMap<i64, i64> = HashMap::new();
	for supplier in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		suppliers.insert(supplier.s_suppkey, supplier.s_nationkey);
	}

	let mut customers: HashMap<i64, i64> = HashMap::new();
	for customer in CustomerGenerator::new(scale_factor, 1, 1).iter() {
		customers.insert(customer.c_custkey, customer.c_nationkey);
	}

	let mut orders: HashMap<i64, (i64, i32, i32)> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		let orderdate = order.o_orderdate.to_unix_epoch();
		let Some(o_year) = q08_order_year(orderdate) else {
			continue;
		};
		orders.insert(order.o_orderkey, (order.o_custkey, orderdate, o_year));
	}

	let mut raw_groups: BTreeMap<i32, (i128, i128)> = BTreeMap::new();
	let mut denominator_rows = 0_usize;
	let mut brazil_rows = 0_usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		if !economy_anodized_steel_parts.contains(&item.l_partkey) {
			continue;
		}
		let Some(&(custkey, orderdate, _)) = orders.get(&item.l_orderkey) else {
			continue;
		};
		let Some(&customer_nationkey) = customers.get(&custkey) else {
			continue;
		};
		let Some((_, customer_regionkey)) = nations.get(&customer_nationkey) else {
			continue;
		};
		let customer_region = if america_regions.contains(customer_regionkey) {
			"AMERICA"
		} else {
			"OTHER"
		};
		let Some(&supplier_nationkey) = suppliers.get(&item.l_suppkey) else {
			continue;
		};
		let Some((supplier_nation, _)) = nations.get(&supplier_nationkey) else {
			continue;
		};
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let discount_raw = item.l_discount.into_inner() as i128;
		let Some((o_year, volume, is_brazil)) = q08_contribution(
			"ECONOMY ANODIZED STEEL",
			customer_region,
			supplier_nation,
			orderdate,
			extprice_raw,
			discount_raw,
		) else {
			continue;
		};
		let (num_raw, den_raw) = raw_groups.entry(o_year).or_insert((0, 0));
		*den_raw += volume;
		denominator_rows += 1;
		if is_brazil {
			*num_raw += volume;
			brazil_rows += 1;
		}
	}

	let mut groups = BTreeMap::new();
	for (o_year, (num_raw, den_raw)) in raw_groups {
		let mkt_share = q08_market_share_raw(num_raw, den_raw)?;
		if mkt_share.precision != 38 || mkt_share.scale != 8 {
			return Err(format!(
				"q08 mkt_share type drift: Decimal128({}, {})",
				mkt_share.precision, mkt_share.scale
			));
		}
		groups.insert(o_year, mkt_share);
	}

	Ok(Q08Oracle {
		groups,
		denominator_rows,
		brazil_rows,
	})
}

fn strict_q08_expected_schema() -> [(&'static str, DataType); 2] {
	[
		("o_year", DataType::Int32),
		("mkt_share", DataType::Decimal128(38, 8)),
	]
}

pub fn assert_strict_q08_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q08_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q08 produced no output batch".to_string());
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

pub fn compare_strict_q08(batches: &[RecordBatch], oracle: &Q08Oracle) -> Result<(), String> {
	assert_strict_q08_schema(batches)?;
	if oracle.groups.len() != 2
		|| !oracle.groups.contains_key(&1995)
		|| !oracle.groups.contains_key(&1996)
	{
		return Err(format!(
			"oracle non-vacuous guard failed: groups={:?}",
			oracle.groups.keys().collect::<Vec<_>>()
		));
	}
	if oracle.denominator_rows == 0 {
		return Err("oracle non-vacuous guard failed: denominator_rows=0".to_string());
	}

	let mut emitted: Vec<(i32, i128)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let years = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int32Array>()
			.ok_or("column 0 (o_year) not Int32")?;
		let shares = batch
			.column(1)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 1 (mkt_share) not Decimal128")?;
		for i in 0..batch.num_rows() {
			if years.is_null(i) {
				return Err(format!("column o_year row {} is NULL", emitted.len()));
			}
			if shares.is_null(i) {
				return Err(format!("column mkt_share row {} is NULL", emitted.len()));
			}
			emitted.push((years.value(i), shares.value(i)));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<i32, i128> = BTreeMap::new();
	for (year, mkt_share) in &emitted {
		if seen.insert(*year, *mkt_share).is_some() {
			return Err(format!("duplicate emitted o_year {year}"));
		}
	}
	for (year, expected) in &oracle.groups {
		let Some(expected_value) = expected.value else {
			return Err(format!("oracle q08 year {year} is NULL"));
		};
		if expected.precision != 38 || expected.scale != 8 {
			return Err(format!(
				"oracle q08 year {year} type drift: Decimal128({}, {})",
				expected.precision, expected.scale
			));
		}
		let Some(actual_value) = seen.get(year) else {
			return Err(format!("oracle q08 year {year} missing from output"));
		};
		if *actual_value != expected_value {
			return Err(format!(
				"q08 year {year}: mkt_share mismatch: expected {expected_value}, got {actual_value}"
			));
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].0 > pair[1].0 {
			return Err(format!(
				"ORDER BY o_year violated: {} emitted before {}",
				pair[0].0, pair[1].0
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q08(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q08_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q08".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q8_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q08_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q08_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q08(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone)]
pub struct Q09Oracle {
	pub groups: BTreeMap<(String, i32), i128>,
	pub selected_rows: usize,
	pub negative_amount_rows: usize,
}

pub fn q09_part_name_contains_green(name: &str) -> bool {
	name.contains("green")
}

pub fn q09_amount_raw(
	extendedprice_raw: i128,
	discount_raw: i128,
	supplycost_raw: i128,
	quantity_raw: i128,
) -> i128 {
	discounted_price_raw(extendedprice_raw, discount_raw) - supplycost_raw * quantity_raw
}

pub fn q09_partsupp_cost(
	costs: &HashMap<(i64, i64), i128>,
	partkey: i64,
	suppkey: i64,
) -> Option<i128> {
	costs.get(&(partkey, suppkey)).copied()
}

pub fn q09_order_year(orderdate: i32) -> i32 {
	let (year, _, _) = civil_from_date32_days(i64::from(orderdate));
	i32::try_from(year).expect("q09 order year fits i32")
}

fn civil_from_date32_days(days: i64) -> (i64, u32, u32) {
	let days = days + 719_468;
	let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
	let day_of_era = days - era * 146_097;
	let year_of_era =
		(day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
	let mut year = year_of_era + era * 400;
	let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
	let month_prime = (5 * day_of_year + 2) / 153;
	let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
	let month = month_prime + if month_prime < 10 { 3 } else { -9 };
	year += i64::from(month <= 2);
	(
		year,
		u32::try_from(month).expect("month in range"),
		u32::try_from(day).expect("day in range"),
	)
}

/// ADR 303: the independent strict-q09 oracle. The profit term is raw scale-4 signed Decimal arithmetic:
/// `extendedprice*(1-discount) - supplycost*quantity`, grouped by `(nation, EXTRACT(year FROM o_orderdate))`.
pub fn strict_q09_oracle(scale_factor: f64) -> Q09Oracle {
	let green_parts: HashSet<i64> = PartGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter(|part| q09_part_name_contains_green(&part.p_name.to_string()))
		.map(|part| part.p_partkey)
		.collect();

	let mut partsupp_costs: HashMap<(i64, i64), i128> = HashMap::new();
	for partsupp in PartSuppGenerator::new(scale_factor, 1, 1).iter() {
		partsupp_costs.insert(
			(partsupp.ps_partkey, partsupp.ps_suppkey),
			partsupp.ps_supplycost.into_inner() as i128,
		);
	}

	let mut nations: HashMap<i64, String> = HashMap::new();
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		nations.insert(nation.n_nationkey, nation.n_name.to_string());
	}

	let mut suppliers: HashMap<i64, i64> = HashMap::new();
	for supplier in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		suppliers.insert(supplier.s_suppkey, supplier.s_nationkey);
	}

	let mut orders: HashMap<i64, i32> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		orders.insert(
			order.o_orderkey,
			q09_order_year(order.o_orderdate.to_unix_epoch()),
		);
	}

	let mut groups: BTreeMap<(String, i32), i128> = BTreeMap::new();
	let mut selected_rows = 0usize;
	let mut negative_amount_rows = 0usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		if !green_parts.contains(&item.l_partkey) {
			continue;
		}
		let Some(supplycost_raw) =
			q09_partsupp_cost(&partsupp_costs, item.l_partkey, item.l_suppkey)
		else {
			continue;
		};
		let Some(&o_year) = orders.get(&item.l_orderkey) else {
			continue;
		};
		let Some(&supplier_nationkey) = suppliers.get(&item.l_suppkey) else {
			continue;
		};
		let Some(nation) = nations.get(&supplier_nationkey) else {
			continue;
		};
		let amount_raw = q09_amount_raw(
			item.l_extendedprice.into_inner() as i128,
			item.l_discount.into_inner() as i128,
			supplycost_raw,
			item.l_quantity as i128 * 100,
		);
		if amount_raw < 0 {
			negative_amount_rows += 1;
		}
		*groups.entry((nation.clone(), o_year)).or_default() += amount_raw;
		selected_rows += 1;
	}

	Q09Oracle {
		groups,
		selected_rows,
		negative_amount_rows,
	}
}

fn strict_q09_expected_schema() -> [(&'static str, DataType); 3] {
	[
		("nation", DataType::Utf8),
		("o_year", DataType::Int32),
		("sum_profit", DataType::Decimal128(33, 4)),
	]
}

pub fn assert_strict_q09_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q09_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q09 produced no output batch".to_string());
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

pub fn compare_strict_q09(batches: &[RecordBatch], oracle: &Q09Oracle) -> Result<(), String> {
	assert_strict_q09_schema(batches)?;
	if oracle.selected_rows == 0 || oracle.groups.len() <= 1 {
		return Err(format!(
			"oracle non-vacuous guard failed: selected_rows={} groups={}",
			oracle.selected_rows,
			oracle.groups.len()
		));
	}

	let mut emitted: Vec<((String, i32), i128)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 3 {
			return Err(format!("expected 3 columns, got {}", batch.num_columns()));
		}
		let nations = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (nation) not Utf8")?;
		let years = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.ok_or("column 1 (o_year) not Int32")?;
		let profits = batch
			.column(2)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 2 (sum_profit) not Decimal128")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted.len();
			if nations.is_null(i) {
				return Err(format!("column nation row {row_index} is NULL"));
			}
			if years.is_null(i) {
				return Err(format!("column o_year row {row_index} is NULL"));
			}
			if profits.is_null(i) {
				return Err(format!("column sum_profit row {row_index} is NULL"));
			}
			emitted.push((
				(nations.value(i).to_string(), years.value(i)),
				profits.value(i),
			));
		}
	}

	for pair in emitted.windows(2) {
		let (nation_a, year_a) = &pair[0].0;
		let (nation_b, year_b) = &pair[1].0;
		if nation_a > nation_b {
			return Err(format!(
				"ORDER BY nation ASC violated: {nation_a:?} emitted before {nation_b:?}"
			));
		}
		if nation_a == nation_b && year_a < year_b {
			return Err(format!(
				"ORDER BY o_year DESC violated within nation {nation_a:?}: {year_a} emitted before {year_b}"
			));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<(String, i32), i128> = BTreeMap::new();
	for (key, sum_profit) in emitted {
		if seen.insert(key.clone(), sum_profit).is_some() {
			return Err(format!("duplicate emitted q09 group {key:?}"));
		}
	}
	for (key, expected_profit) in &oracle.groups {
		let Some(actual_profit) = seen.get(key) else {
			return Err(format!("oracle q09 group {key:?} missing from output"));
		};
		if actual_profit != expected_profit {
			return Err(format!(
				"q09 group {key:?}: sum_profit mismatch: expected {expected_profit}, got {actual_profit}"
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q09(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q09_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q09".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q9_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q09_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q09_oracle(scale_factor);
	match compare_strict_q09(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q10Row {
	pub c_custkey: i64,
	pub c_name: String,
	pub revenue: i128,
	pub c_acctbal: i128,
	pub n_name: String,
	pub c_address: String,
	pub c_phone: String,
	pub c_comment: String,
}

pub struct Q10Oracle {
	pub rows: Vec<Q10Row>,
	pub selected_rows: usize,
}

#[derive(Debug, Clone)]
struct Q10Customer {
	name: String,
	acctbal_raw: i128,
	nationkey: i64,
	address: String,
	phone: String,
	comment: String,
}

/// ADR 295: the independent strict-q10 oracle — traverse the four q10 generators directly, apply the
/// returned-item date/window filters, then group raw scale-4 revenue by the customer key while checking every
/// projected customer/nation attribute.
pub fn strict_q10_oracle(scale_factor: f64) -> Q10Oracle {
	const START_1993_10_01: i32 = 8674;
	const START_1994_01_01: i32 = 8766;

	let mut nations: HashMap<i64, String> = HashMap::new();
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		nations.insert(nation.n_nationkey, nation.n_name.to_string());
	}

	let mut customers: HashMap<i64, Q10Customer> = HashMap::new();
	for customer in CustomerGenerator::new(scale_factor, 1, 1).iter() {
		customers.insert(
			customer.c_custkey,
			Q10Customer {
				name: customer.c_name.to_string(),
				acctbal_raw: customer.c_acctbal.into_inner() as i128,
				nationkey: customer.c_nationkey,
				address: customer.c_address.to_string(),
				phone: customer.c_phone.to_string(),
				comment: customer.c_comment.to_string(),
			},
		);
	}

	let mut orders: HashMap<i64, i64> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		let orderdate = order.o_orderdate.to_unix_epoch();
		if (START_1993_10_01..START_1994_01_01).contains(&orderdate) {
			orders.insert(order.o_orderkey, order.o_custkey);
		}
	}

	let mut groups: BTreeMap<i64, Q10Row> = BTreeMap::new();
	let mut selected_rows = 0usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		if item.l_returnflag != "R" {
			continue;
		}
		let Some(&custkey) = orders.get(&item.l_orderkey) else {
			continue;
		};
		let Some(customer) = customers.get(&custkey) else {
			continue;
		};
		let Some(nation_name) = nations.get(&customer.nationkey) else {
			continue;
		};
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let disc_raw = item.l_discount.into_inner() as i128;
		groups
			.entry(custkey)
			.or_insert_with(|| Q10Row {
				c_custkey: custkey,
				c_name: customer.name.clone(),
				revenue: 0,
				c_acctbal: customer.acctbal_raw,
				n_name: nation_name.clone(),
				c_address: customer.address.clone(),
				c_phone: customer.phone.clone(),
				c_comment: customer.comment.clone(),
			})
			.revenue += discounted_price_raw(extprice_raw, disc_raw);
		selected_rows += 1;
	}

	let mut rows: Vec<Q10Row> = groups.into_values().collect();
	rows.sort_by(|left, right| right.revenue.cmp(&left.revenue));
	Q10Oracle {
		rows,
		selected_rows,
	}
}

fn strict_q10_expected_schema() -> [(&'static str, DataType); 8] {
	[
		("c_custkey", DataType::Int64),
		("c_name", DataType::Utf8),
		("revenue", DataType::Decimal128(32, 4)),
		("c_acctbal", DataType::Decimal128(15, 2)),
		("n_name", DataType::Utf8),
		("c_address", DataType::Utf8),
		("c_phone", DataType::Utf8),
		("c_comment", DataType::Utf8),
	]
}

pub fn assert_strict_q10_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q10_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q10 produced no output batch".to_string());
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

pub fn compare_strict_q10(batches: &[RecordBatch], oracle: &Q10Oracle) -> Result<(), String> {
	assert_strict_q10_schema(batches)?;
	if oracle.selected_rows == 0 || oracle.rows.is_empty() {
		return Err(format!(
			"oracle non-vacuous guard failed: selected_rows={} rows={}",
			oracle.selected_rows,
			oracle.rows.len()
		));
	}

	let mut emitted = Vec::new();
	for batch in batches {
		if batch.num_columns() != 8 {
			return Err(format!("expected 8 columns, got {}", batch.num_columns()));
		}
		let custkeys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 0 (c_custkey) not Int64")?;
		let customer_names = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 1 (c_name) not Utf8")?;
		let revenues = batch
			.column(2)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 2 (revenue) not Decimal128")?;
		let acctbals = batch
			.column(3)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 3 (c_acctbal) not Decimal128")?;
		let nation_names = batch
			.column(4)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 4 (n_name) not Utf8")?;
		let addresses = batch
			.column(5)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 5 (c_address) not Utf8")?;
		let phones = batch
			.column(6)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 6 (c_phone) not Utf8")?;
		let comments = batch
			.column(7)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 7 (c_comment) not Utf8")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted.len();
			if custkeys.is_null(i) {
				return Err(format!("column c_custkey row {row_index} is NULL"));
			}
			if customer_names.is_null(i) {
				return Err(format!("column c_name row {row_index} is NULL"));
			}
			if revenues.is_null(i) {
				return Err(format!("column revenue row {row_index} is NULL"));
			}
			if acctbals.is_null(i) {
				return Err(format!("column c_acctbal row {row_index} is NULL"));
			}
			if nation_names.is_null(i) {
				return Err(format!("column n_name row {row_index} is NULL"));
			}
			if addresses.is_null(i) {
				return Err(format!("column c_address row {row_index} is NULL"));
			}
			if phones.is_null(i) {
				return Err(format!("column c_phone row {row_index} is NULL"));
			}
			if comments.is_null(i) {
				return Err(format!("column c_comment row {row_index} is NULL"));
			}
			emitted.push(Q10Row {
				c_custkey: custkeys.value(i),
				c_name: customer_names.value(i).to_string(),
				revenue: revenues.value(i),
				c_acctbal: acctbals.value(i),
				n_name: nation_names.value(i).to_string(),
				c_address: addresses.value(i).to_string(),
				c_phone: phones.value(i).to_string(),
				c_comment: comments.value(i).to_string(),
			});
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].revenue < pair[1].revenue {
			return Err(format!(
				"ORDER BY revenue DESC violated: {} emitted before {}",
				pair[0].revenue, pair[1].revenue
			));
		}
	}

	if emitted.len() != oracle.rows.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			oracle.rows.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<i64, Q10Row> = BTreeMap::new();
	for row in emitted {
		if seen.insert(row.c_custkey, row.clone()).is_some() {
			return Err(format!("duplicate emitted c_custkey {}", row.c_custkey));
		}
	}
	for expected in &oracle.rows {
		let Some(actual) = seen.get(&expected.c_custkey) else {
			return Err(format!(
				"oracle c_custkey {} missing from output",
				expected.c_custkey
			));
		};
		if actual != expected {
			return Err(format!(
				"c_custkey {} mismatch: expected {:?}, got {:?}",
				expected.c_custkey, expected, actual
			));
		}
	}
	Ok(())
}

pub async fn check_strict_q10(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q10_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q10".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q10_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q10_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q10_oracle(scale_factor);
	if oracle.selected_rows <= 1 || oracle.rows.len() <= 1 {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!(
				"oracle non-vacuous guard failed: selected_rows={} rows={}",
				oracle.selected_rows,
				oracle.rows.len()
			)),
		);
	}
	match compare_strict_q10(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

pub const Q11_DECIMAL_LITERAL_SCALE_REASON: &str = "decimal_literal_scale";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q11Row {
	pub ps_partkey: i64,
	pub value_raw: i128,
}

#[derive(Debug, Clone)]
pub struct Q11Oracle {
	pub rows: Vec<Q11Row>,
	pub global_value_raw: i128,
	pub total_german_parts: usize,
	pub positive_german_parts: usize,
	pub threshold_fail_groups: usize,
	pub threshold_failed_partkeys: BTreeSet<i64>,
}

pub fn q11_threshold_passes(
	value_raw: i128,
	global_value_raw: i128,
	scale: Q11Scale,
) -> Result<bool, String> {
	let multiplier = 100_i128
		.checked_mul(scale.hundredths_n())
		.ok_or_else(|| "q11 threshold comparison overflow".to_string())?;
	let scaled_value = value_raw
		.checked_mul(multiplier)
		.ok_or_else(|| "q11 threshold comparison overflow".to_string())?;
	Ok(scaled_value > global_value_raw)
}

pub fn q11_oracle_from_rows(
	partsupp: impl IntoIterator<Item = (i64, i64, i32, i128)>,
	suppliers: impl IntoIterator<Item = (i64, i64)>,
	nations: impl IntoIterator<Item = (i64, String)>,
	scale: Q11Scale,
) -> Result<Q11Oracle, String> {
	let germany_nations: HashSet<i64> = nations
		.into_iter()
		.filter_map(|(nation_key, name)| (name == "GERMANY").then_some(nation_key))
		.collect();
	let germany_suppliers: HashSet<i64> = suppliers
		.into_iter()
		.filter_map(|(suppkey, nation_key)| {
			germany_nations.contains(&nation_key).then_some(suppkey)
		})
		.collect();

	let mut groups: BTreeMap<i64, i128> = BTreeMap::new();
	for (partkey, suppkey, availqty, supplycost_raw) in partsupp {
		if !germany_suppliers.contains(&suppkey) {
			continue;
		}
		let value_raw = supplycost_raw
			.checked_mul(i128::from(availqty))
			.ok_or_else(|| "q11 supplycost*availqty overflow".to_string())?;
		let current = groups.entry(partkey).or_insert(0);
		*current = current
			.checked_add(value_raw)
			.ok_or_else(|| "q11 grouped value overflow".to_string())?;
	}

	let mut global_value_raw = 0_i128;
	for value in groups.values() {
		global_value_raw = global_value_raw
			.checked_add(*value)
			.ok_or_else(|| "q11 global value overflow".to_string())?;
	}

	let total_german_parts = groups.len();
	let positive_german_parts = groups.values().filter(|&&value| value > 0).count();
	let mut threshold_fail_groups = 0_usize;
	let mut threshold_failed_partkeys = BTreeSet::new();
	let mut rows = Vec::new();
	for (partkey, value_raw) in groups {
		if q11_threshold_passes(value_raw, global_value_raw, scale)? {
			rows.push(Q11Row {
				ps_partkey: partkey,
				value_raw,
			});
		} else if value_raw > 0 {
			threshold_fail_groups += 1;
			threshold_failed_partkeys.insert(partkey);
		}
	}
	rows.sort_by(|left, right| {
		right
			.value_raw
			.cmp(&left.value_raw)
			.then_with(|| left.ps_partkey.cmp(&right.ps_partkey))
	});

	Ok(Q11Oracle {
		rows,
		global_value_raw,
		total_german_parts,
		positive_german_parts,
		threshold_fail_groups,
		threshold_failed_partkeys,
	})
}

pub fn strict_q11_oracle(scale_factor: f64) -> Result<Q11Oracle, String> {
	let scale = Q11Scale::try_from_f64(scale_factor)?;
	q11_oracle_from_rows(
		PartSuppGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|partsupp| {
				(
					partsupp.ps_partkey,
					partsupp.ps_suppkey,
					partsupp.ps_availqty,
					partsupp.ps_supplycost.into_inner() as i128,
				)
			}),
		SupplierGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|supplier| (supplier.s_suppkey, supplier.s_nationkey)),
		NationGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|nation| (nation.n_nationkey, nation.n_name.to_string())),
		scale,
	)
}

fn strict_q11_expected_schema() -> [(&'static str, DataType); 2] {
	[
		("ps_partkey", DataType::Int64),
		("value", DataType::Decimal128(26, 2)),
	]
}

pub fn assert_strict_q11_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q11_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q11 produced no output batch".to_string());
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

pub fn compare_strict_q11(batches: &[RecordBatch], oracle: &Q11Oracle) -> Result<(), String> {
	assert_strict_q11_schema(batches)?;
	if oracle.rows.is_empty()
		|| oracle.positive_german_parts <= oracle.rows.len()
		|| oracle.threshold_fail_groups == 0
	{
		return Err(format!(
			"oracle non-vacuous guard failed: rows={} total_german_parts={} positive_german_parts={} threshold_fail_groups={} global_value_raw={}",
			oracle.rows.len(),
			oracle.total_german_parts,
			oracle.positive_german_parts,
			oracle.threshold_fail_groups,
			oracle.global_value_raw
		));
	}

	let mut emitted = Vec::new();
	let mut emitted_by_part: BTreeMap<i64, i128> = BTreeMap::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let partkeys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 0 (ps_partkey) not Int64")?;
		let values = batch
			.column(1)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 1 (value) not Decimal128")?;
		for row in 0..batch.num_rows() {
			let row_index = emitted.len();
			if partkeys.is_null(row) {
				return Err(format!("column ps_partkey row {row_index} is NULL"));
			}
			if values.is_null(row) {
				return Err(format!("column value row {row_index} is NULL"));
			}
			let output = Q11Row {
				ps_partkey: partkeys.value(row),
				value_raw: values.value(row),
			};
			if emitted_by_part
				.insert(output.ps_partkey, output.value_raw)
				.is_some()
			{
				return Err(format!(
					"duplicate emitted ps_partkey {}",
					output.ps_partkey
				));
			}
			emitted.push(output);
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].value_raw < pair[1].value_raw {
			return Err(format!(
				"ORDER BY value DESC violated: {:?} emitted before {:?}",
				pair[0], pair[1]
			));
		}
	}

	let expected_by_part: BTreeMap<i64, i128> = oracle
		.rows
		.iter()
		.map(|row| (row.ps_partkey, row.value_raw))
		.collect();
	let missing = expected_by_part
		.keys()
		.filter(|partkey| !emitted_by_part.contains_key(*partkey))
		.count();
	let extra_partkeys: BTreeSet<i64> = emitted_by_part
		.keys()
		.filter(|partkey| !expected_by_part.contains_key(*partkey))
		.copied()
		.collect();
	let extras = extra_partkeys.len();
	if missing == 0
		&& extra_partkeys == oracle.threshold_failed_partkeys
		&& emitted_by_part.len() == oracle.positive_german_parts
		&& emitted_by_part.values().all(|value| *value > 0)
	{
		return Err(Q11_DECIMAL_LITERAL_SCALE_REASON.to_string());
	}

	if emitted_by_part.len() != expected_by_part.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			expected_by_part.len(),
			emitted_by_part.len()
		));
	}
	if missing > 0 || extras > 0 {
		return Err(format!(
			"q11 key set mismatch: missing {missing} expected rows, extra {extras} output rows"
		));
	}
	for (partkey, expected_value) in expected_by_part {
		let Some(actual_value) = emitted_by_part.get(&partkey) else {
			return Err(format!("oracle ps_partkey {partkey} missing from output"));
		};
		if *actual_value != expected_value {
			return Err(format!(
				"ps_partkey {partkey}: value mismatch: expected {expected_value}, got {actual_value}"
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q11(scale_factor: f64) -> Result<CorrectnessAttempt, String> {
	let start = Instant::now();
	let scale = Q11Scale::try_from_f64(scale_factor)?;
	let sql = q11_sql(scale);
	let tables = q11_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q11".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(&sql, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return Ok(outcome(phase, status_for_reason(&reason), Some(reason)));
		}
	};
	if let Err(reason) = assert_strict_q11_schema(&batches) {
		return Ok(outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		));
	}
	let oracle = match strict_q11_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return Ok(outcome(Phase::Compare, Status::Fail, Some(reason))),
	};
	Ok(match compare_strict_q11(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	})
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Q02Row {
	pub s_acctbal: i128,
	pub s_name: String,
	pub n_name: String,
	pub p_partkey: i64,
	pub p_mfgr: String,
	pub s_address: String,
	pub s_phone: String,
	pub s_comment: String,
}

#[derive(Debug, Clone)]
pub struct Q02Oracle {
	pub rows: Vec<Q02Row>,
	pub matched_parts: usize,
	pub min_tie_parts: usize,
}

pub fn q02_type_matches_brass(part_type: &str) -> bool {
	part_type.ends_with("BRASS")
}

#[derive(Debug, Clone)]
struct Q02Supplier {
	acctbal_raw: i128,
	name: String,
	nation_name: String,
	address: String,
	phone: String,
	comment: String,
}

#[derive(Debug, Clone)]
struct Q02PartSupp {
	partkey: i64,
	suppkey: i64,
	supplycost_raw: i128,
}

/// Independent strict-q02 oracle: traverse the five q02 generators directly, compute the EUROPE-scoped
/// per-part minimum supply cost, then emit every min-cost supplier row.
pub fn strict_q02_oracle(scale_factor: f64) -> Q02Oracle {
	let mut europe_regions: HashSet<i64> = HashSet::new();
	for region in RegionGenerator::new(scale_factor, 1, 1).iter() {
		if region.r_name == "EUROPE" {
			europe_regions.insert(region.r_regionkey);
		}
	}

	let mut europe_nations: HashMap<i64, String> = HashMap::new();
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		if europe_regions.contains(&nation.n_regionkey) {
			europe_nations.insert(nation.n_nationkey, nation.n_name.to_string());
		}
	}

	let mut suppliers: HashMap<i64, Q02Supplier> = HashMap::new();
	for supplier in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		let Some(nation_name) = europe_nations.get(&supplier.s_nationkey) else {
			continue;
		};
		suppliers.insert(
			supplier.s_suppkey,
			Q02Supplier {
				acctbal_raw: supplier.s_acctbal.into_inner() as i128,
				name: supplier.s_name.to_string(),
				nation_name: nation_name.clone(),
				address: supplier.s_address.to_string(),
				phone: supplier.s_phone.to_string(),
				comment: supplier.s_comment.to_string(),
			},
		);
	}

	let mut parts: HashMap<i64, String> = HashMap::new();
	for part in PartGenerator::new(scale_factor, 1, 1).iter() {
		if part.p_size == 15 && q02_type_matches_brass(part.p_type) {
			parts.insert(part.p_partkey, part.p_mfgr.to_string());
		}
	}

	let mut candidates = Vec::new();
	let mut min_by_part: BTreeMap<i64, i128> = BTreeMap::new();
	for partsupp in PartSuppGenerator::new(scale_factor, 1, 1).iter() {
		if !parts.contains_key(&partsupp.ps_partkey)
			|| !suppliers.contains_key(&partsupp.ps_suppkey)
		{
			continue;
		}
		let supplycost_raw = partsupp.ps_supplycost.into_inner() as i128;
		candidates.push(Q02PartSupp {
			partkey: partsupp.ps_partkey,
			suppkey: partsupp.ps_suppkey,
			supplycost_raw,
		});
		min_by_part
			.entry(partsupp.ps_partkey)
			.and_modify(|current| *current = (*current).min(supplycost_raw))
			.or_insert(supplycost_raw);
	}

	let mut min_counts: BTreeMap<i64, usize> = BTreeMap::new();
	let mut rows = Vec::new();
	for candidate in candidates {
		let Some(&min_cost) = min_by_part.get(&candidate.partkey) else {
			continue;
		};
		if candidate.supplycost_raw != min_cost {
			continue;
		}
		*min_counts.entry(candidate.partkey).or_insert(0) += 1;
		let Some(part_mfgr) = parts.get(&candidate.partkey) else {
			continue;
		};
		let Some(supplier) = suppliers.get(&candidate.suppkey) else {
			continue;
		};
		rows.push(Q02Row {
			s_acctbal: supplier.acctbal_raw,
			s_name: supplier.name.clone(),
			n_name: supplier.nation_name.clone(),
			p_partkey: candidate.partkey,
			p_mfgr: part_mfgr.clone(),
			s_address: supplier.address.clone(),
			s_phone: supplier.phone.clone(),
			s_comment: supplier.comment.clone(),
		});
	}

	rows.sort_by(q02_ordering);
	let min_tie_parts = min_counts.values().filter(|&&count| count >= 2).count();
	Q02Oracle {
		rows,
		matched_parts: min_by_part.len(),
		min_tie_parts,
	}
}

fn q02_ordering(left: &Q02Row, right: &Q02Row) -> std::cmp::Ordering {
	right
		.s_acctbal
		.cmp(&left.s_acctbal)
		.then_with(|| left.n_name.cmp(&right.n_name))
		.then_with(|| left.s_name.cmp(&right.s_name))
		.then_with(|| left.p_partkey.cmp(&right.p_partkey))
}

fn q02_order_pair_is_valid(left: &Q02Row, right: &Q02Row) -> bool {
	use std::cmp::Ordering;

	match left.s_acctbal.cmp(&right.s_acctbal) {
		Ordering::Greater => return true,
		Ordering::Less => return false,
		Ordering::Equal => {}
	}
	match left.n_name.cmp(&right.n_name) {
		Ordering::Less => return true,
		Ordering::Greater => return false,
		Ordering::Equal => {}
	}
	match left.s_name.cmp(&right.s_name) {
		Ordering::Less => return true,
		Ordering::Greater => return false,
		Ordering::Equal => {}
	}
	left.p_partkey <= right.p_partkey
}

fn strict_q02_expected_schema() -> [(&'static str, DataType); 8] {
	[
		("s_acctbal", DataType::Decimal128(15, 2)),
		("s_name", DataType::Utf8),
		("n_name", DataType::Utf8),
		("p_partkey", DataType::Int64),
		("p_mfgr", DataType::Utf8),
		("s_address", DataType::Utf8),
		("s_phone", DataType::Utf8),
		("s_comment", DataType::Utf8),
	]
}

pub fn assert_strict_q02_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q02_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q02 produced no output batch".to_string());
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

pub fn compare_strict_q02(batches: &[RecordBatch], oracle: &Q02Oracle) -> Result<(), String> {
	assert_strict_q02_schema(batches)?;
	if oracle.matched_parts == 0 || oracle.rows.is_empty() {
		return Err(format!(
			"oracle non-vacuous guard failed: matched_parts={} rows={}",
			oracle.matched_parts,
			oracle.rows.len()
		));
	}

	let mut emitted = Vec::new();
	for batch in batches {
		if batch.num_columns() != 8 {
			return Err(format!("expected 8 columns, got {}", batch.num_columns()));
		}
		let acctbals = batch
			.column(0)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 0 (s_acctbal) not Decimal128")?;
		let supplier_names = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 1 (s_name) not Utf8")?;
		let nation_names = batch
			.column(2)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 2 (n_name) not Utf8")?;
		let partkeys = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 3 (p_partkey) not Int64")?;
		let mfgrs = batch
			.column(4)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 4 (p_mfgr) not Utf8")?;
		let addresses = batch
			.column(5)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 5 (s_address) not Utf8")?;
		let phones = batch
			.column(6)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 6 (s_phone) not Utf8")?;
		let comments = batch
			.column(7)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 7 (s_comment) not Utf8")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted.len();
			if acctbals.is_null(i) {
				return Err(format!("column s_acctbal row {row_index} is NULL"));
			}
			if supplier_names.is_null(i) {
				return Err(format!("column s_name row {row_index} is NULL"));
			}
			if nation_names.is_null(i) {
				return Err(format!("column n_name row {row_index} is NULL"));
			}
			if partkeys.is_null(i) {
				return Err(format!("column p_partkey row {row_index} is NULL"));
			}
			if mfgrs.is_null(i) {
				return Err(format!("column p_mfgr row {row_index} is NULL"));
			}
			if addresses.is_null(i) {
				return Err(format!("column s_address row {row_index} is NULL"));
			}
			if phones.is_null(i) {
				return Err(format!("column s_phone row {row_index} is NULL"));
			}
			if comments.is_null(i) {
				return Err(format!("column s_comment row {row_index} is NULL"));
			}
			emitted.push(Q02Row {
				s_acctbal: acctbals.value(i),
				s_name: supplier_names.value(i).to_string(),
				n_name: nation_names.value(i).to_string(),
				p_partkey: partkeys.value(i),
				p_mfgr: mfgrs.value(i).to_string(),
				s_address: addresses.value(i).to_string(),
				s_phone: phones.value(i).to_string(),
				s_comment: comments.value(i).to_string(),
			});
		}
	}

	for pair in emitted.windows(2) {
		if !q02_order_pair_is_valid(&pair[0], &pair[1]) {
			return Err(format!(
				"ORDER BY s_acctbal DESC, n_name, s_name, p_partkey violated between {:?} and {:?}",
				pair[0], pair[1]
			));
		}
	}

	let mut actual = emitted;
	let mut expected = oracle.rows.clone();
	actual.sort();
	expected.sort();
	if actual.len() != expected.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			expected.len(),
			actual.len()
		));
	}
	for (idx, (actual_row, expected_row)) in actual.iter().zip(expected.iter()).enumerate() {
		if actual_row != expected_row {
			return Err(format!(
				"row {idx} mismatch after canonical sort: expected {:?}, got {:?}",
				expected_row, actual_row
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q02(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q02_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q02".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q2_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q02_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q02_oracle(scale_factor);
	if oracle.matched_parts == 0 || oracle.rows.is_empty() {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!(
				"oracle non-vacuous guard failed: matched_parts={} rows={}",
				oracle.matched_parts,
				oracle.rows.len()
			)),
		);
	}
	match compare_strict_q02(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone)]
pub struct Q04Oracle {
	pub groups: BTreeMap<String, i64>,
	pub orders_pass_exists: usize,
	pub orders_fail_exists: usize,
}

pub fn q04_oracle_from_rows(
	orders: impl IntoIterator<Item = (i64, i32, String)>,
	lineitems: impl IntoIterator<Item = (i64, i32, i32)>,
) -> Q04Oracle {
	const START_1993_Q3: i32 = 8582;
	const START_1993_Q4: i32 = 8674;

	let mut exists_orderkeys: HashSet<i64> = HashSet::new();
	for (orderkey, commitdate, receiptdate) in lineitems {
		if commitdate < receiptdate {
			exists_orderkeys.insert(orderkey);
		}
	}

	let mut groups: BTreeMap<String, i64> = BTreeMap::new();
	let mut orders_pass_exists = 0_usize;
	let mut orders_fail_exists = 0_usize;
	for (orderkey, orderdate, orderpriority) in orders {
		if !(START_1993_Q3..START_1993_Q4).contains(&orderdate) {
			continue;
		}
		if exists_orderkeys.contains(&orderkey) {
			*groups.entry(orderpriority).or_default() += 1;
			orders_pass_exists += 1;
		} else {
			orders_fail_exists += 1;
		}
	}

	Q04Oracle {
		groups,
		orders_pass_exists,
		orders_fail_exists,
	}
}

/// Independent strict-q04 oracle: traverse orders and lineitem generators directly, model the correlated EXISTS
/// as a semijoin, then group counted orders by order priority.
pub fn strict_q04_oracle(scale_factor: f64) -> Q04Oracle {
	q04_oracle_from_rows(
		OrderGenerator::new(scale_factor, 1, 1).iter().map(|order| {
			(
				order.o_orderkey,
				order.o_orderdate.to_unix_epoch(),
				order.o_orderpriority.to_string(),
			)
		}),
		LineItemGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|item| {
				(
					item.l_orderkey,
					item.l_commitdate.to_unix_epoch(),
					item.l_receiptdate.to_unix_epoch(),
				)
			}),
	)
}

fn strict_q04_expected_schema() -> [(&'static str, DataType); 2] {
	[
		("o_orderpriority", DataType::Utf8),
		("order_count", DataType::Int64),
	]
}

pub fn assert_strict_q04_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q04_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q04 produced no output batch".to_string());
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

pub fn compare_strict_q04(batches: &[RecordBatch], oracle: &Q04Oracle) -> Result<(), String> {
	assert_strict_q04_schema(batches)?;

	if oracle.orders_pass_exists == 0 || oracle.orders_fail_exists == 0 || oracle.groups.is_empty()
	{
		return Err(format!(
			"oracle non-vacuous guard failed: pass={} fail={} groups={}",
			oracle.orders_pass_exists,
			oracle.orders_fail_exists,
			oracle.groups.len()
		));
	}

	let mut emitted: Vec<(String, i64)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let priorities = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (o_orderpriority) not Utf8")?;
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 1 (order_count) not Int64")?;
		for i in 0..batch.num_rows() {
			if priorities.is_null(i) {
				return Err(format!(
					"column o_orderpriority row {} is NULL",
					emitted.len()
				));
			}
			if counts.is_null(i) {
				return Err(format!("column order_count row {} is NULL", emitted.len()));
			}
			emitted.push((priorities.value(i).to_string(), counts.value(i)));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<String, i64> = BTreeMap::new();
	for (priority, count) in &emitted {
		if seen.insert(priority.clone(), *count).is_some() {
			return Err(format!("duplicate emitted o_orderpriority {priority:?}"));
		}
	}
	for (priority, expected_count) in &oracle.groups {
		let Some(actual_count) = seen.get(priority) else {
			return Err(format!(
				"oracle o_orderpriority {priority:?} missing from output"
			));
		};
		if actual_count != expected_count {
			return Err(format!(
				"o_orderpriority {priority:?}: count mismatch: expected {expected_count}, got {actual_count}"
			));
		}
	}

	let expected_order: Vec<&String> = oracle.groups.keys().collect();
	for (idx, (priority, _)) in emitted.iter().enumerate() {
		if priority != expected_order[idx] {
			return Err(format!(
				"ORDER BY o_orderpriority violated at row {idx}: expected {:?}, got {priority:?}",
				expected_order[idx]
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q04(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q04_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q04".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q4_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q04_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q04_oracle(scale_factor);
	match compare_strict_q04(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q12Group {
	pub high_line_count: i64,
	pub low_line_count: i64,
}

#[derive(Debug, Clone)]
pub struct Q12Oracle {
	pub groups: BTreeMap<String, Q12Group>,
	pub selected_rows: usize,
	pub high_rows: usize,
	pub low_rows: usize,
}

/// Independent strict-q12 oracle: traverse orders and lineitem generators directly, apply every q12 filter,
/// then group high/low priority counts by ship mode.
pub fn strict_q12_oracle(scale_factor: f64) -> Q12Oracle {
	const START_1994: i32 = 8766;
	const START_1995: i32 = 9131;

	let mut orders_by_key: HashMap<i64, String> = HashMap::new();
	for order in OrderGenerator::new(scale_factor, 1, 1).iter() {
		orders_by_key.insert(order.o_orderkey, order.o_orderpriority.to_string());
	}

	let mut groups: BTreeMap<String, Q12Group> = BTreeMap::new();
	let mut selected_rows = 0_usize;
	let mut high_rows = 0_usize;
	let mut low_rows = 0_usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		let shipmode = item.l_shipmode;
		if shipmode != "MAIL" && shipmode != "SHIP" {
			continue;
		}
		if item.l_commitdate.to_unix_epoch() >= item.l_receiptdate.to_unix_epoch() {
			continue;
		}
		if item.l_shipdate.to_unix_epoch() >= item.l_commitdate.to_unix_epoch() {
			continue;
		}
		let receiptdate = item.l_receiptdate.to_unix_epoch();
		if !(START_1994..START_1995).contains(&receiptdate) {
			continue;
		}
		let Some(priority) = orders_by_key.get(&item.l_orderkey) else {
			continue;
		};

		selected_rows += 1;
		let group = groups.entry(shipmode.to_string()).or_insert(Q12Group {
			high_line_count: 0,
			low_line_count: 0,
		});
		if priority == "1-URGENT" || priority == "2-HIGH" {
			group.high_line_count += 1;
			high_rows += 1;
		} else {
			group.low_line_count += 1;
			low_rows += 1;
		}
	}

	Q12Oracle {
		groups,
		selected_rows,
		high_rows,
		low_rows,
	}
}

fn strict_q12_expected_schema() -> [(&'static str, DataType); 3] {
	[
		("l_shipmode", DataType::Utf8),
		("high_line_count", DataType::Int64),
		("low_line_count", DataType::Int64),
	]
}

pub fn assert_strict_q12_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q12_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q12 produced no output batch".to_string());
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

pub fn compare_strict_q12(batches: &[RecordBatch], oracle: &Q12Oracle) -> Result<(), String> {
	assert_strict_q12_schema(batches)?;

	if !oracle.groups.contains_key("MAIL") || !oracle.groups.contains_key("SHIP") {
		return Err(format!(
			"oracle non-vacuous guard failed: groups={:?}",
			oracle.groups.keys().collect::<Vec<_>>()
		));
	}
	if oracle.selected_rows == 0 || oracle.high_rows == 0 || oracle.low_rows == 0 {
		return Err(format!(
			"oracle non-vacuous guard failed: selected_rows={} high_rows={} low_rows={}",
			oracle.selected_rows, oracle.high_rows, oracle.low_rows
		));
	}
	for (shipmode, group) in &oracle.groups {
		if group.high_line_count <= 0 || group.low_line_count <= 0 {
			return Err(format!(
				"oracle non-vacuous guard failed for {shipmode}: high={} low={}",
				group.high_line_count, group.low_line_count
			));
		}
	}

	let mut emitted: Vec<(String, i64, i64)> = Vec::new();
	for batch in batches {
		if batch.num_columns() != 3 {
			return Err(format!("expected 3 columns, got {}", batch.num_columns()));
		}
		let shipmodes = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (l_shipmode) not Utf8")?;
		let high_counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 1 (high_line_count) not Int64")?;
		let low_counts = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 2 (low_line_count) not Int64")?;
		for i in 0..batch.num_rows() {
			if shipmodes.is_null(i) {
				return Err(format!("column l_shipmode row {} is NULL", emitted.len()));
			}
			if high_counts.is_null(i) {
				return Err(format!(
					"column high_line_count row {} is NULL",
					emitted.len()
				));
			}
			if low_counts.is_null(i) {
				return Err(format!(
					"column low_line_count row {} is NULL",
					emitted.len()
				));
			}
			emitted.push((
				shipmodes.value(i).to_string(),
				high_counts.value(i),
				low_counts.value(i),
			));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<String, (i64, i64)> = BTreeMap::new();
	for (shipmode, high, low) in &emitted {
		if seen.insert(shipmode.clone(), (*high, *low)).is_some() {
			return Err(format!("duplicate emitted l_shipmode {shipmode:?}"));
		}
	}
	for (shipmode, group) in &oracle.groups {
		let Some(&(high, low)) = seen.get(shipmode) else {
			return Err(format!(
				"oracle l_shipmode {shipmode:?} missing from output"
			));
		};
		if high != group.high_line_count || low != group.low_line_count {
			return Err(format!(
				"l_shipmode {shipmode:?}: count mismatch: expected high={} low={}, got high={high} low={low}",
				group.high_line_count, group.low_line_count
			));
		}
	}

	let expected_order: Vec<&String> = oracle.groups.keys().collect();
	for (idx, (shipmode, _, _)) in emitted.iter().enumerate() {
		if shipmode != expected_order[idx] {
			return Err(format!(
				"ORDER BY l_shipmode violated at row {idx}: expected {:?}, got {shipmode:?}",
				expected_order[idx]
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q12(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q12_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q12".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q12_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q12_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q12_oracle(scale_factor);
	match compare_strict_q12(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

pub const Q17_CORRECTNESS_SCALE: f64 = 0.02;
pub const Q17_SF10_CORRECTNESS_SCALE: f64 = 10.0;

/// ADR 431 — the shared pure q17 correctness-scale policy. The small non-vacuous fixture stays the
/// implicit default; sf=10 is the only board-scale extension certified by the structural census.
pub fn q17_correctness_scale(explicit: Option<f64>) -> Result<f64, String> {
	match explicit {
		None => Ok(Q17_CORRECTNESS_SCALE),
		Some(scale)
			if (scale - Q17_CORRECTNESS_SCALE).abs() <= f64::EPSILON
				|| (scale - Q17_SF10_CORRECTNESS_SCALE).abs() <= f64::EPSILON =>
		{
			Ok(scale)
		}
		Some(scale) => Err(format!(
			"q17_sf10_oracle_policy_gap: q17 correctness supports --sf {Q17_CORRECTNESS_SCALE} or --sf {Q17_SF10_CORRECTNESS_SCALE}; got {scale}"
		)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalRaw {
	pub value: Option<i128>,
	pub precision: u8,
	pub scale: i8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Q17PartAggregate {
	pub partkey: i64,
	pub quantity_sum_raw: i128,
	pub quantity_count: i64,
	pub quantity_avg_raw: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Q17SelectedRow {
	pub partkey: i64,
	pub orderkey: i64,
	pub suppkey: i64,
	pub quantity_raw: i128,
	pub extendedprice_raw: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q17CensusCommon {
	pub part_aggregates: Vec<Q17PartAggregate>,
	pub selected_rows: Vec<Q17SelectedRow>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Q17BoundaryCounts {
	pub below: usize,
	pub equal: usize,
	pub above: usize,
}

impl Q17BoundaryCounts {
	fn record(&mut self, ordering: std::cmp::Ordering) {
		match ordering {
			std::cmp::Ordering::Less => self.below += 1,
			std::cmp::Ordering::Equal => self.equal += 1,
			std::cmp::Ordering::Greater => self.above += 1,
		}
	}

	fn add_assign(&mut self, other: &Self) {
		self.below += other.below;
		self.equal += other.equal;
		self.above += other.above;
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q17PartBoundaryReceipt {
	pub partkey: i64,
	pub rational: Q17BoundaryCounts,
	pub rounded: Q17BoundaryCounts,
	pub membership_difference_rows: usize,
}

impl Q17PartBoundaryReceipt {
	fn record(&mut self, rational: std::cmp::Ordering, rounded: std::cmp::Ordering) -> bool {
		self.rational.record(rational);
		self.rounded.record(rounded);
		let differs =
			(rational == std::cmp::Ordering::Less) != (rounded == std::cmp::Ordering::Less);
		if differs {
			self.membership_difference_rows += 1;
		}
		differs
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q17BoundaryReceipt {
	pub parts: Vec<Q17PartBoundaryReceipt>,
	pub membership_difference_rows: usize,
}

impl Q17BoundaryReceipt {
	pub fn totals(&self) -> (Q17BoundaryCounts, Q17BoundaryCounts) {
		let mut rational = Q17BoundaryCounts::default();
		let mut rounded = Q17BoundaryCounts::default();
		for part in &self.parts {
			rational.add_assign(&part.rational);
			rounded.add_assign(&part.rounded);
		}
		(rational, rounded)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q17BoundaryOracle {
	pub common: Q17CensusCommon,
	pub receipt: Q17BoundaryReceipt,
	pub eligible_parts: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q17CensusFingerprints {
	pub part_aggregates: String,
	pub selected_rows: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Q17BoundaryCensusOutcome {
	Clean,
	DecimalContractDecision,
	ImplementationOracleDivergence,
}

impl Q17BoundaryCensusOutcome {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Clean => "clean",
			Self::DecimalContractDecision => "decimal_contract_decision",
			Self::ImplementationOracleDivergence => "implementation_oracle_divergence",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q17BoundaryCensus {
	pub toron: Q17CensusCommon,
	pub oracle: Q17CensusCommon,
	pub receipt: Q17BoundaryReceipt,
	pub eligible_parts: usize,
	pub common_comparison_error: Option<String>,
	pub toron_fingerprints: Q17CensusFingerprints,
	pub oracle_fingerprints: Q17CensusFingerprints,
	pub outcome: Q17BoundaryCensusOutcome,
}

/// Mirror the executor's scalar `Decimal128 / Decimal128` path: NULL/zero divisor -> NULL and
/// non-integral quotients round half away from zero.
pub fn decimal_division_raw(
	numerator: i128,
	numerator_precision: u8,
	numerator_scale: i8,
	divisor: i128,
	divisor_precision: u8,
	divisor_scale: i8,
) -> Result<DecimalRaw, String> {
	let (precision, scale) = decimal_division_output_type(
		numerator_precision,
		numerator_scale,
		divisor_precision,
		divisor_scale,
	)
	.ok_or_else(|| "decimal division output type out of range".to_string())?;
	if divisor == 0 {
		return Ok(DecimalRaw {
			value: None,
			precision,
			scale,
		});
	}

	let scale_delta = i32::from(scale) + i32::from(divisor_scale) - i32::from(numerator_scale);
	if scale_delta < 0 {
		return Err("decimal division scale delta below zero".to_string());
	}
	let multiplier = 10_i128
		.checked_pow(scale_delta as u32)
		.ok_or_else(|| "decimal division scale overflow".to_string())?;
	let scaled = numerator
		.checked_mul(multiplier)
		.ok_or_else(|| "decimal division scale overflow".to_string())?;
	let mut quotient = scaled / divisor;
	let remainder = scaled % divisor;
	if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
		quotient = quotient
			.checked_add(scaled.signum() * divisor.signum())
			.ok_or_else(|| "decimal division rounding overflow".to_string())?;
	}
	if !decimal_fits_precision(quotient, precision) {
		return Err(format!(
			"decimal division result {quotient} does not fit output precision {precision}"
		));
	}
	Ok(DecimalRaw {
		value: Some(quotient),
		precision,
		scale,
	})
}

#[derive(Debug, Clone)]
pub struct Q14Oracle {
	pub promo_revenue: DecimalRaw,
	pub denominator_rows: usize,
	pub promo_rows: usize,
}

/// Independent strict-q14 oracle at the default correctness scale. `disc_price` is raw scale 4; the
/// exact `100.00` literal contributes raw 10000 at scale 2 before Decimal division.
pub fn strict_q14_oracle(scale_factor: f64) -> Result<Q14Oracle, String> {
	let promo_by_part: HashMap<i64, bool> = PartGenerator::new(scale_factor, 1, 1)
		.iter()
		.map(|part| (part.p_partkey, part.p_type.to_string().starts_with("PROMO")))
		.collect();

	let mut sum_all_raw = 0_i128;
	let mut sum_promo_raw = 0_i128;
	let mut denominator_rows = 0_usize;
	let mut promo_rows = 0_usize;

	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		let shipdate = item.l_shipdate.to_unix_epoch();
		if !(9374..9404).contains(&shipdate) {
			continue;
		}
		let Some(&is_promo) = promo_by_part.get(&item.l_partkey) else {
			continue;
		};

		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let discount_raw = item.l_discount.into_inner() as i128;
		let disc_price = discounted_price_raw(extprice_raw, discount_raw);
		sum_all_raw = sum_all_raw
			.checked_add(disc_price)
			.ok_or_else(|| "q14 denominator sum overflow".to_string())?;
		denominator_rows += 1;

		if is_promo {
			sum_promo_raw = sum_promo_raw
				.checked_add(disc_price)
				.ok_or_else(|| "q14 promo sum overflow".to_string())?;
			promo_rows += 1;
		}
	}

	let promo_revenue = if denominator_rows == 0 || sum_all_raw == 0 {
		DecimalRaw {
			value: None,
			precision: 38,
			scale: 10,
		}
	} else {
		let numerator = sum_promo_raw
			.checked_mul(10_000)
			.ok_or_else(|| "q14 numerator multiplication overflow".to_string())?;
		decimal_division_raw(numerator, 38, 6, sum_all_raw, 38, 4)?
	};

	Ok(Q14Oracle {
		promo_revenue,
		denominator_rows,
		promo_rows,
	})
}

fn strict_q14_expected_schema() -> [(&'static str, DataType); 1] {
	[("promo_revenue", DataType::Decimal128(38, 10))]
}

pub fn assert_strict_q14_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q14_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q14 produced no output batch".to_string());
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

pub fn compare_strict_q14(batches: &[RecordBatch], oracle: &Q14Oracle) -> Result<(), String> {
	assert_strict_q14_schema(batches)?;
	if oracle.denominator_rows == 0 || oracle.promo_rows == 0 {
		return Err(format!(
			"oracle non-vacuous guard failed: denominator_rows={} promo_rows={}",
			oracle.denominator_rows, oracle.promo_rows
		));
	}
	let Some(expected_value) = oracle.promo_revenue.value else {
		return Err("oracle promo_revenue is NULL".to_string());
	};
	if oracle.promo_revenue.precision != 38 || oracle.promo_revenue.scale != 10 {
		return Err(format!(
			"oracle promo_revenue type drift: Decimal128({}, {})",
			oracle.promo_revenue.precision, oracle.promo_revenue.scale
		));
	}

	let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
	if row_count != 1 {
		return Err(format!("expected exactly one q14 row, got {row_count}"));
	}
	for batch in batches {
		if batch.num_rows() == 0 {
			continue;
		}
		let column = batch
			.column(0)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 0 (promo_revenue) not Decimal128")?;
		if column.is_null(0) {
			return Err("column promo_revenue row 0 is NULL".to_string());
		}
		let actual_value = column.value(0);
		if actual_value != expected_value {
			return Err(format!(
				"promo_revenue mismatch: expected {expected_value}, got {actual_value}"
			));
		}
		return Ok(());
	}
	Err("strict q14 produced no row".to_string())
}

pub async fn check_strict_q14(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q14_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q14".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q14_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q14_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q14_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q14(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone)]
pub struct Q17Oracle {
	pub avg_yearly: DecimalRaw,
	pub eligible_parts: usize,
	pub contributing_rows: usize,
}

/// Independent strict-q17 oracle at the non-empty scale. Quantity and money raws are scale-2, matching the
/// strict fixture; exact `0.2` carries scale 1 and `0.2 * AVG(l_quantity)` compares at scale 7.
pub fn strict_q17_boundary_oracle(scale_factor: f64) -> Result<Q17BoundaryOracle, String> {
	let eligible_partkeys = q17_eligible_partkeys(scale_factor);
	let eligible_parts: HashSet<i64> = eligible_partkeys.iter().copied().collect();

	let mut quantity_by_part: BTreeMap<i64, (i128, i64)> = BTreeMap::new();
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		if eligible_parts.contains(&item.l_partkey) {
			let entry = quantity_by_part.entry(item.l_partkey).or_insert((0, 0));
			entry.0 = entry
				.0
				.checked_add(item.l_quantity as i128 * 100)
				.ok_or_else(|| "q17 quantity sum overflow".to_string())?;
			entry.1 = entry
				.1
				.checked_add(1)
				.ok_or_else(|| "q17 quantity count overflow".to_string())?;
		}
	}

	let mut boundary_by_part: BTreeMap<i64, (i128, i64, i128, i128)> = BTreeMap::new();
	let mut part_aggregates = Vec::with_capacity(quantity_by_part.len());
	for (&partkey, &(quantity_sum, count)) in &quantity_by_part {
		let avg_raw = avg_decimal_raw(quantity_sum, count, 2, 6, 19)?;
		let threshold_raw = 2_i128
			.checked_mul(avg_raw)
			.ok_or_else(|| "q17 threshold multiplication overflow".to_string())?;
		boundary_by_part.insert(partkey, (quantity_sum, count, avg_raw, threshold_raw));
		part_aggregates.push(Q17PartAggregate {
			partkey,
			quantity_sum_raw: quantity_sum,
			quantity_count: count,
			quantity_avg_raw: avg_raw,
		});
	}

	let mut receipt_by_part: BTreeMap<i64, Q17PartBoundaryReceipt> = boundary_by_part
		.keys()
		.map(|&partkey| {
			(
				partkey,
				Q17PartBoundaryReceipt {
					partkey,
					rational: Q17BoundaryCounts::default(),
					rounded: Q17BoundaryCounts::default(),
					membership_difference_rows: 0,
				},
			)
		})
		.collect();
	let mut selected_rows = Vec::new();
	let mut membership_difference_rows = 0_usize;
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		let Some(&(quantity_sum, count, _avg_raw, threshold_raw)) =
			boundary_by_part.get(&item.l_partkey)
		else {
			continue;
		};
		let quantity_raw = item.l_quantity as i128 * 100;
		let rational_left = 5_i128
			.checked_mul(quantity_raw)
			.and_then(|value| value.checked_mul(i128::from(count)))
			.ok_or_else(|| "q17 rational boundary multiplication overflow".to_string())?;
		let quantity_at_threshold_scale = quantity_raw
			.checked_mul(100_000)
			.ok_or_else(|| "q17 quantity rescale overflow".to_string())?;
		let rational = rational_left.cmp(&quantity_sum);
		let rounded = quantity_at_threshold_scale.cmp(&threshold_raw);
		let receipt = receipt_by_part
			.get_mut(&item.l_partkey)
			.expect("q17 receipt exists for every boundary");
		if receipt.record(rational, rounded) {
			membership_difference_rows += 1;
		}
		if rounded == std::cmp::Ordering::Less {
			selected_rows.push(Q17SelectedRow {
				partkey: item.l_partkey,
				orderkey: item.l_orderkey,
				suppkey: item.l_suppkey,
				quantity_raw,
				extendedprice_raw: item.l_extendedprice.into_inner() as i128,
			});
		}
	}
	selected_rows.sort();

	Ok(Q17BoundaryOracle {
		common: Q17CensusCommon {
			part_aggregates,
			selected_rows,
		},
		receipt: Q17BoundaryReceipt {
			parts: receipt_by_part.into_values().collect(),
			membership_difference_rows,
		},
		eligible_parts: eligible_parts.len(),
	})
}

pub fn strict_q17_oracle(scale_factor: f64) -> Result<Q17Oracle, String> {
	let boundary = strict_q17_boundary_oracle(scale_factor)?;
	let extendedprice_sum = boundary
		.common
		.selected_rows
		.iter()
		.try_fold(0_i128, |sum, row| {
			sum.checked_add(row.extendedprice_raw)
				.ok_or_else(|| "q17 extendedprice sum overflow".to_string())
		})?;
	let contributing_rows = boundary.common.selected_rows.len();

	let avg_yearly = if contributing_rows == 0 {
		DecimalRaw {
			value: None,
			precision: 20,
			scale: 6,
		}
	} else {
		decimal_division_raw(extendedprice_sum, 15, 2, 70, 2, 1)?
	};
	Ok(Q17Oracle {
		avg_yearly,
		eligible_parts: boundary.eligible_parts,
		contributing_rows,
	})
}

fn q17_assert_census_schema(
	batches: &[RecordBatch],
	expected: &[(&str, DataType)],
	label: &str,
) -> Result<(), String> {
	let Some(first) = batches.first() else {
		return Err(format!("q17 census {label} produced no output batch"));
	};
	for batch in batches {
		let schema = batch.schema();
		if schema.fields().len() != expected.len() {
			return Err(format!(
				"q17 census {label}: expected {} columns, got {}",
				expected.len(),
				schema.fields().len()
			));
		}
		for (index, (name, data_type)) in expected.iter().enumerate() {
			let field = schema.field(index);
			if field.name() != name || field.data_type() != data_type {
				return Err(format!(
					"q17 census {label} column {index}: expected {name} {data_type:?}, got {} {:?}",
					field.name(),
					field.data_type()
				));
			}
		}
	}
	let _ = first;
	Ok(())
}

fn q17_census_common_from_batches(
	aggregate_batches: &[RecordBatch],
	selected_batches: &[RecordBatch],
) -> Result<Q17CensusCommon, String> {
	q17_assert_census_schema(
		aggregate_batches,
		&[
			("p_partkey", DataType::Int64),
			("quantity_sum", DataType::Decimal128(15, 2)),
			("quantity_count", DataType::Int64),
			("quantity_avg", DataType::Decimal128(19, 6)),
		],
		"aggregate",
	)?;
	q17_assert_census_schema(
		selected_batches,
		&[
			("p_partkey", DataType::Int64),
			("l_orderkey", DataType::Int64),
			("l_suppkey", DataType::Int64),
			("l_quantity", DataType::Decimal128(15, 2)),
			("l_extendedprice", DataType::Decimal128(15, 2)),
		],
		"selected rows",
	)?;

	let mut part_aggregates = Vec::new();
	for batch in aggregate_batches {
		let partkey = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("q17 census aggregate column p_partkey is not Int64")?;
		let quantity_sum = batch
			.column(1)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("q17 census aggregate column quantity_sum is not Decimal128")?;
		let quantity_count = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("q17 census aggregate column quantity_count is not Int64")?;
		let quantity_avg = batch
			.column(3)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("q17 census aggregate column quantity_avg is not Decimal128")?;
		for row in 0..batch.num_rows() {
			if partkey.is_null(row)
				|| quantity_sum.is_null(row)
				|| quantity_count.is_null(row)
				|| quantity_avg.is_null(row)
			{
				return Err(format!("q17 census aggregate row {row} contains NULL"));
			}
			part_aggregates.push(Q17PartAggregate {
				partkey: partkey.value(row),
				quantity_sum_raw: quantity_sum.value(row),
				quantity_count: quantity_count.value(row),
				quantity_avg_raw: quantity_avg.value(row),
			});
		}
	}
	part_aggregates.sort();

	let mut selected_rows = Vec::new();
	for batch in selected_batches {
		let partkey = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("q17 census selected column p_partkey is not Int64")?;
		let orderkey = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("q17 census selected column l_orderkey is not Int64")?;
		let suppkey = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("q17 census selected column l_suppkey is not Int64")?;
		let quantity = batch
			.column(3)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("q17 census selected column l_quantity is not Decimal128")?;
		let extendedprice = batch
			.column(4)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("q17 census selected column l_extendedprice is not Decimal128")?;
		for row in 0..batch.num_rows() {
			if partkey.is_null(row)
				|| orderkey.is_null(row)
				|| suppkey.is_null(row)
				|| quantity.is_null(row)
				|| extendedprice.is_null(row)
			{
				return Err(format!("q17 census selected row {row} contains NULL"));
			}
			selected_rows.push(Q17SelectedRow {
				partkey: partkey.value(row),
				orderkey: orderkey.value(row),
				suppkey: suppkey.value(row),
				quantity_raw: quantity.value(row),
				extendedprice_raw: extendedprice.value(row),
			});
		}
	}
	selected_rows.sort();

	Ok(Q17CensusCommon {
		part_aggregates,
		selected_rows,
	})
}

fn q17_first_difference<T: std::fmt::Debug + PartialEq>(
	label: &str,
	actual: &[T],
	expected: &[T],
) -> Option<String> {
	let different = actual
		.iter()
		.zip(expected)
		.position(|(actual, expected)| actual != expected);
	if let Some(index) = different {
		return Some(format!(
			"q17 census {label} mismatch at {index}: Toron={:?}, oracle={:?}",
			actual[index], expected[index]
		));
	}
	if actual.len() != expected.len() {
		let index = actual.len().min(expected.len());
		return Some(format!(
			"q17 census {label} length mismatch: Toron={} oracle={} first_extra_Toron={:?} first_extra_oracle={:?}",
			actual.len(),
			expected.len(),
			actual.get(index),
			expected.get(index)
		));
	}
	None
}

pub fn compare_q17_census_common(
	actual: &Q17CensusCommon,
	expected: &Q17CensusCommon,
) -> Result<(), String> {
	if let Some(error) = q17_first_difference(
		"part aggregates",
		&actual.part_aggregates,
		&expected.part_aggregates,
	) {
		return Err(error);
	}
	if let Some(error) = q17_first_difference(
		"selected rows",
		&actual.selected_rows,
		&expected.selected_rows,
	) {
		return Err(error);
	}
	Ok(())
}

fn q17_fingerprint_part_aggregates(rows: &[Q17PartAggregate]) -> String {
	let mut hash = fnv1a_update(FNV_OFFSET, &(rows.len() as u64).to_le_bytes());
	for row in rows {
		hash = fnv1a_update(hash, &row.partkey.to_le_bytes());
		hash = fnv1a_update(hash, &row.quantity_sum_raw.to_le_bytes());
		hash = fnv1a_update(hash, &row.quantity_count.to_le_bytes());
		hash = fnv1a_update(hash, &row.quantity_avg_raw.to_le_bytes());
	}
	format!("fnv1a64:{hash:016x}")
}

fn q17_fingerprint_selected_rows(rows: &[Q17SelectedRow]) -> String {
	let mut hash = fnv1a_update(FNV_OFFSET, &(rows.len() as u64).to_le_bytes());
	for row in rows {
		hash = fnv1a_update(hash, &row.partkey.to_le_bytes());
		hash = fnv1a_update(hash, &row.orderkey.to_le_bytes());
		hash = fnv1a_update(hash, &row.suppkey.to_le_bytes());
		hash = fnv1a_update(hash, &row.quantity_raw.to_le_bytes());
		hash = fnv1a_update(hash, &row.extendedprice_raw.to_le_bytes());
	}
	format!("fnv1a64:{hash:016x}")
}

pub fn q17_census_fingerprints(common: &Q17CensusCommon) -> Q17CensusFingerprints {
	Q17CensusFingerprints {
		part_aggregates: q17_fingerprint_part_aggregates(&common.part_aggregates),
		selected_rows: q17_fingerprint_selected_rows(&common.selected_rows),
	}
}

pub async fn run_q17_boundary_census_with_data_source(
	scale_factor: f64,
	data_source: &str,
	data_root: &Path,
) -> Result<Q17BoundaryCensus, String> {
	const PARTKEYS_PER_QUERY: usize = 256;
	let eligible_partkeys = q17_eligible_partkeys(scale_factor);
	let (tables, storage): (Vec<HarnessTableMetadata>, Arc<dyn StorageScan>) = match data_source {
		"memory" => metadata_storage_from_tables(q17_tables(true, scale_factor)),
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec("q17", "strict", scale_factor, data_root)?;
			(spec.tables, spec.storage)
		}
		other => {
			return Err(format!(
				"q17 census --data-source {other:?} is not supported"
			));
		}
	};

	let mut aggregate_batches = Vec::new();
	let mut selected_batches = Vec::new();
	for (chunk_index, partkeys) in eligible_partkeys.chunks(PARTKEYS_PER_QUERY).enumerate() {
		let aggregate_sql = strict_q17_census_agg_sql(partkeys)?;
		let selected_sql = strict_q17_census_rows_sql(partkeys)?;
		aggregate_batches.extend(
			execute_query_with_metadata_timed(&aggregate_sql, tables.clone(), storage.clone())
				.await
				.map_err(|error| {
					format!(
						"q17 census aggregate chunk {chunk_index} failed at {:?}: {} ({})",
						error.phase, error.reason, error.message
					)
				})?
				.batches,
		);
		selected_batches.extend(
			execute_query_with_metadata_timed(&selected_sql, tables.clone(), storage.clone())
				.await
				.map_err(|error| {
					format!(
						"q17 census selected-row chunk {chunk_index} failed at {:?}: {} ({})",
						error.phase, error.reason, error.message
					)
				})?
				.batches,
		);
	}
	let toron = q17_census_common_from_batches(&aggregate_batches, &selected_batches)?;
	let boundary_oracle = strict_q17_boundary_oracle(scale_factor)?;
	let common_comparison_error = compare_q17_census_common(&toron, &boundary_oracle.common).err();
	let outcome = if common_comparison_error.is_some() {
		Q17BoundaryCensusOutcome::ImplementationOracleDivergence
	} else if boundary_oracle.receipt.membership_difference_rows != 0 {
		Q17BoundaryCensusOutcome::DecimalContractDecision
	} else {
		Q17BoundaryCensusOutcome::Clean
	};
	let toron_fingerprints = q17_census_fingerprints(&toron);
	let oracle_fingerprints = q17_census_fingerprints(&boundary_oracle.common);

	Ok(Q17BoundaryCensus {
		toron,
		oracle: boundary_oracle.common,
		receipt: boundary_oracle.receipt,
		eligible_parts: boundary_oracle.eligible_parts,
		common_comparison_error,
		toron_fingerprints,
		oracle_fingerprints,
		outcome,
	})
}

fn strict_q17_expected_schema() -> [(&'static str, DataType); 1] {
	[("avg_yearly", DataType::Decimal128(20, 6))]
}

pub fn assert_strict_q17_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q17_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q17 produced no output batch".to_string());
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

pub fn compare_strict_q17(batches: &[RecordBatch], oracle: &Q17Oracle) -> Result<(), String> {
	assert_strict_q17_schema(batches)?;
	if oracle.eligible_parts == 0 || oracle.contributing_rows == 0 {
		return Err(format!(
			"oracle non-vacuous guard failed: eligible_parts={} contributing_rows={}",
			oracle.eligible_parts, oracle.contributing_rows
		));
	}
	let Some(expected_value) = oracle.avg_yearly.value else {
		return Err("oracle avg_yearly is NULL".to_string());
	};
	if oracle.avg_yearly.precision != 20 || oracle.avg_yearly.scale != 6 {
		return Err(format!(
			"oracle avg_yearly type drift: Decimal128({}, {})",
			oracle.avg_yearly.precision, oracle.avg_yearly.scale
		));
	}

	let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
	if row_count != 1 {
		return Err(format!("expected exactly one q17 row, got {row_count}"));
	}
	for batch in batches {
		if batch.num_rows() == 0 {
			continue;
		}
		let column = batch
			.column(0)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 0 (avg_yearly) not Decimal128")?;
		if column.is_null(0) {
			return Err("column avg_yearly row 0 is NULL".to_string());
		}
		let actual_value = column.value(0);
		if actual_value != expected_value {
			return Err(format!(
				"avg_yearly mismatch: expected {expected_value}, got {actual_value}"
			));
		}
		return Ok(());
	}
	Err("strict q17 produced no row".to_string())
}

pub async fn check_strict_q17(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q17_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q17".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q17_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q17_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q17_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q17(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q20Row {
	pub s_name: String,
	pub s_address: String,
}

#[derive(Debug, Clone)]
pub struct Q20Oracle {
	pub rows: Vec<Q20Row>,
	pub selected_partsupp_rows: usize,
	pub qualifying_suppliers: usize,
	pub threshold_pass_rows: usize,
	pub threshold_fail_rows: usize,
	pub absent_group_rows: usize,
}

pub fn q20_part_name_is_forest_prefix(name: &str) -> bool {
	name.starts_with("forest")
}

pub fn q20_threshold_passes(ps_availqty: i32, sum_qty_raw: Option<i128>) -> bool {
	let Some(sum_qty_raw) = sum_qty_raw else {
		return false;
	};
	(ps_availqty as i128) * 10_000 > 50 * sum_qty_raw
}

/// ADR 296: the independent strict-q20 oracle traverses the five q20 generators directly, preserves SQL's
/// `SUM(empty) = NULL` threshold semantics, then answer-check the qualifying CANADA suppliers by `s_name`.
pub fn strict_q20_oracle(scale_factor: f64) -> Q20Oracle {
	const START_1994: i32 = 8766;
	const START_1995: i32 = 9131;

	let forest_parts: HashSet<i64> = PartGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter(|part| q20_part_name_is_forest_prefix(&part.p_name.to_string()))
		.map(|part| part.p_partkey)
		.collect();

	let mut canada_nations: HashSet<i64> = HashSet::new();
	for nation in NationGenerator::new(scale_factor, 1, 1).iter() {
		if nation.n_name == "CANADA" {
			canada_nations.insert(nation.n_nationkey);
		}
	}

	let mut canada_suppliers: HashMap<i64, Q20Row> = HashMap::new();
	for supplier in SupplierGenerator::new(scale_factor, 1, 1).iter() {
		if canada_nations.contains(&supplier.s_nationkey) {
			canada_suppliers.insert(
				supplier.s_suppkey,
				Q20Row {
					s_name: supplier.s_name.to_string(),
					s_address: supplier.s_address.to_string(),
				},
			);
		}
	}

	let mut quantity_by_part_supp: HashMap<(i64, i64), i128> = HashMap::new();
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		let shipdate = item.l_shipdate.to_unix_epoch();
		if !(START_1994..START_1995).contains(&shipdate) {
			continue;
		}
		*quantity_by_part_supp
			.entry((item.l_partkey, item.l_suppkey))
			.or_insert(0) += item.l_quantity as i128 * 100;
	}

	let mut selected_partsupp_rows = 0usize;
	let mut threshold_pass_rows = 0usize;
	let mut threshold_fail_rows = 0usize;
	let mut absent_group_rows = 0usize;
	let mut qualifying_suppkeys: HashSet<i64> = HashSet::new();
	for partsupp in PartSuppGenerator::new(scale_factor, 1, 1).iter() {
		if !forest_parts.contains(&partsupp.ps_partkey) {
			continue;
		}
		selected_partsupp_rows += 1;
		let sum_qty_raw = quantity_by_part_supp
			.get(&(partsupp.ps_partkey, partsupp.ps_suppkey))
			.copied();
		if sum_qty_raw.is_none() {
			absent_group_rows += 1;
		}
		if q20_threshold_passes(partsupp.ps_availqty, sum_qty_raw) {
			threshold_pass_rows += 1;
			qualifying_suppkeys.insert(partsupp.ps_suppkey);
		} else {
			threshold_fail_rows += 1;
		}
	}

	let mut rows: Vec<Q20Row> = qualifying_suppkeys
		.iter()
		.filter_map(|suppkey| canada_suppliers.get(suppkey).cloned())
		.collect();
	rows.sort_by(|left, right| left.s_name.cmp(&right.s_name));
	Q20Oracle {
		rows,
		selected_partsupp_rows,
		qualifying_suppliers: qualifying_suppkeys.len(),
		threshold_pass_rows,
		threshold_fail_rows,
		absent_group_rows,
	}
}

fn strict_q20_expected_schema() -> [(&'static str, DataType); 2] {
	[("s_name", DataType::Utf8), ("s_address", DataType::Utf8)]
}

pub fn assert_strict_q20_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q20_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q20 produced no output batch".to_string());
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

pub fn compare_strict_q20(batches: &[RecordBatch], oracle: &Q20Oracle) -> Result<(), String> {
	assert_strict_q20_schema(batches)?;
	if oracle.rows.is_empty()
		|| oracle.selected_partsupp_rows == 0
		|| oracle.threshold_pass_rows == 0
	{
		return Err(format!(
			"oracle non-vacuous guard failed: rows={} selected_partsupp_rows={} threshold_pass_rows={}",
			oracle.rows.len(),
			oracle.selected_partsupp_rows,
			oracle.threshold_pass_rows
		));
	}

	let mut emitted = Vec::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let names = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (s_name) not Utf8")?;
		let addresses = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 1 (s_address) not Utf8")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted.len();
			if names.is_null(i) {
				return Err(format!("column s_name row {row_index} is NULL"));
			}
			if addresses.is_null(i) {
				return Err(format!("column s_address row {row_index} is NULL"));
			}
			emitted.push(Q20Row {
				s_name: names.value(i).to_string(),
				s_address: addresses.value(i).to_string(),
			});
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].s_name > pair[1].s_name {
			return Err(format!(
				"ORDER BY s_name violated: {:?} emitted before {:?}",
				pair[0].s_name, pair[1].s_name
			));
		}
	}

	if emitted.len() != oracle.rows.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			oracle.rows.len(),
			emitted.len()
		));
	}
	let mut seen: BTreeMap<String, String> = BTreeMap::new();
	for row in emitted {
		if seen
			.insert(row.s_name.clone(), row.s_address.clone())
			.is_some()
		{
			return Err(format!("duplicate emitted s_name {:?}", row.s_name));
		}
	}
	for expected in &oracle.rows {
		let Some(address) = seen.get(&expected.s_name) else {
			return Err(format!(
				"oracle s_name {:?} missing from output",
				expected.s_name
			));
		};
		if address != &expected.s_address {
			return Err(format!(
				"s_name {:?}: s_address mismatch: expected {:?}, got {:?}",
				expected.s_name, expected.s_address, address
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q20(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q20_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q20".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q20_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q20_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q20_oracle(scale_factor);
	match compare_strict_q20(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q13Oracle {
	pub groups: Vec<(i64, i64)>,
	pub total_customers: usize,
	pub qualifying_orders: usize,
	pub comment_excluded_orders: usize,
	pub zero_count_customers: usize,
	pub positive_count_customers: usize,
	pub no_order_customers: usize,
	pub only_excluded_order_customers: usize,
}

pub fn q13_comment_matches_pattern(comment: &str) -> bool {
	let Some(special) = comment.find("special") else {
		return false;
	};
	comment[special + "special".len()..].contains("requests")
}

pub fn q13_oracle_from_rows(
	customers: impl IntoIterator<Item = i64>,
	orders: impl IntoIterator<Item = (i64, String)>,
) -> Q13Oracle {
	let customers: Vec<i64> = customers.into_iter().collect();
	let total_customers = customers.len();
	let customer_keys: HashSet<i64> = customers.iter().copied().collect();
	let mut total_orders_by_customer: HashMap<i64, i64> = HashMap::new();
	let mut qualifying_orders_by_customer: HashMap<i64, i64> = HashMap::new();
	let mut qualifying_orders = 0_usize;
	let mut comment_excluded_orders = 0_usize;

	for (custkey, comment) in orders {
		if !customer_keys.contains(&custkey) {
			continue;
		}
		*total_orders_by_customer.entry(custkey).or_default() += 1;
		if q13_comment_matches_pattern(&comment) {
			comment_excluded_orders += 1;
		} else {
			*qualifying_orders_by_customer.entry(custkey).or_default() += 1;
			qualifying_orders += 1;
		}
	}

	let mut group_counts: BTreeMap<i64, i64> = BTreeMap::new();
	let mut zero_count_customers = 0_usize;
	let mut positive_count_customers = 0_usize;
	let mut no_order_customers = 0_usize;
	let mut only_excluded_order_customers = 0_usize;
	for custkey in customers {
		let c_count = qualifying_orders_by_customer
			.get(&custkey)
			.copied()
			.unwrap_or(0);
		*group_counts.entry(c_count).or_default() += 1;
		if c_count == 0 {
			zero_count_customers += 1;
		} else {
			positive_count_customers += 1;
		}
		let total_orders = total_orders_by_customer.get(&custkey).copied().unwrap_or(0);
		if total_orders == 0 {
			no_order_customers += 1;
		} else if c_count == 0 {
			only_excluded_order_customers += 1;
		}
	}

	let mut groups: Vec<(i64, i64)> = group_counts.into_iter().collect();
	groups.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));

	Q13Oracle {
		groups,
		total_customers,
		qualifying_orders,
		comment_excluded_orders,
		zero_count_customers,
		positive_count_customers,
		no_order_customers,
		only_excluded_order_customers,
	}
}

pub fn strict_q13_oracle(scale_factor: f64) -> Q13Oracle {
	q13_oracle_from_rows(
		CustomerGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|customer| customer.c_custkey),
		OrderGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|order| (order.o_custkey, order.o_comment.to_string())),
	)
}

fn strict_q13_expected_schema() -> [(&'static str, DataType); 2] {
	[("c_count", DataType::Int64), ("custdist", DataType::Int64)]
}

pub fn assert_strict_q13_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q13_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q13 produced no output batch".to_string());
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

pub fn compare_strict_q13(batches: &[RecordBatch], oracle: &Q13Oracle) -> Result<(), String> {
	assert_strict_q13_schema(batches)?;
	if oracle.total_customers == 0
		|| oracle.groups.len() <= 1
		|| oracle.zero_count_customers == 0
		|| oracle.positive_count_customers == 0
		|| oracle.qualifying_orders == 0
		|| oracle.comment_excluded_orders == 0
	{
		return Err(format!(
			"oracle non-vacuous guard failed: total_customers={} groups={} zero_count_customers={} positive_count_customers={} qualifying_orders={} comment_excluded_orders={}",
			oracle.total_customers,
			oracle.groups.len(),
			oracle.zero_count_customers,
			oracle.positive_count_customers,
			oracle.qualifying_orders,
			oracle.comment_excluded_orders
		));
	}

	let mut emitted: Vec<(i64, i64)> = Vec::new();
	let mut emitted_by_count: BTreeMap<i64, i64> = BTreeMap::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let counts = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 0 (c_count) not Int64")?;
		let distributions = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 1 (custdist) not Int64")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted.len();
			if counts.is_null(i) {
				return Err(format!("column c_count row {row_index} is NULL"));
			}
			if distributions.is_null(i) {
				return Err(format!("column custdist row {row_index} is NULL"));
			}
			let c_count = counts.value(i);
			let custdist = distributions.value(i);
			if emitted_by_count.insert(c_count, custdist).is_some() {
				return Err(format!("duplicate emitted c_count {c_count}"));
			}
			emitted.push((c_count, custdist));
		}
	}

	for pair in emitted.windows(2) {
		let (prev_count, prev_dist) = pair[0];
		let (next_count, next_dist) = pair[1];
		if prev_dist < next_dist || (prev_dist == next_dist && prev_count < next_count) {
			return Err(format!(
				"ORDER BY custdist DESC, c_count DESC violated: ({prev_count}, {prev_dist}) emitted before ({next_count}, {next_dist})"
			));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	for (c_count, expected_custdist) in &oracle.groups {
		let Some(actual_custdist) = emitted_by_count.get(c_count) else {
			return Err(format!("oracle c_count {c_count} missing from output"));
		};
		if actual_custdist != expected_custdist {
			return Err(format!(
				"c_count {c_count} custdist mismatch: expected {expected_custdist}, got {actual_custdist}"
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q13(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q13_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q13".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q13_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q13_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q13_oracle(scale_factor);
	match compare_strict_q13(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q16Oracle {
	pub groups: BTreeMap<(String, String, i32), HashSet<i64>>,
	pub excluded_suppliers: usize,
	pub kept_partsupp_rows: usize,
	pub dedup_groups: usize,
	pub pruned_parts: usize,
}

const Q16_SIZES: [i32; 8] = [49, 14, 23, 45, 19, 3, 36, 9];

pub fn q16_customer_complaints_matches_pattern(comment: &str) -> bool {
	let Some(customer) = comment.find("Customer") else {
		return false;
	};
	comment[customer + "Customer".len()..].contains("Complaints")
}

fn q16_part_is_kept(brand: &str, part_type: &str, size: i32) -> bool {
	brand != "Brand#45" && !part_type.starts_with("MEDIUM POLISHED") && Q16_SIZES.contains(&size)
}

pub fn q16_oracle_from_rows(
	parts: impl IntoIterator<Item = (i64, String, String, i32)>,
	suppliers: impl IntoIterator<Item = (i64, String)>,
	partsupp: impl IntoIterator<Item = (i64, i64)>,
) -> Q16Oracle {
	let excluded: HashSet<i64> = suppliers
		.into_iter()
		.filter_map(|(suppkey, comment)| {
			q16_customer_complaints_matches_pattern(&comment).then_some(suppkey)
		})
		.collect();
	let mut kept_parts = HashMap::new();
	let mut pruned_parts = 0_usize;
	for (partkey, brand, part_type, size) in parts {
		if q16_part_is_kept(&brand, &part_type, size) {
			kept_parts.insert(partkey, (brand, part_type, size));
		} else {
			pruned_parts += 1;
		}
	}

	let mut groups: BTreeMap<(String, String, i32), HashSet<i64>> = BTreeMap::new();
	let mut row_counts: BTreeMap<(String, String, i32), usize> = BTreeMap::new();
	let mut kept_partsupp_rows = 0_usize;
	for (partkey, suppkey) in partsupp {
		if excluded.contains(&suppkey) {
			continue;
		}
		let Some(group_key) = kept_parts.get(&partkey) else {
			continue;
		};
		kept_partsupp_rows += 1;
		let group_key = group_key.clone();
		*row_counts.entry(group_key.clone()).or_default() += 1;
		groups.entry(group_key).or_default().insert(suppkey);
	}
	let dedup_groups = row_counts
		.iter()
		.filter(|(key, row_count)| {
			groups
				.get(*key)
				.is_some_and(|suppliers| suppliers.len() < **row_count)
		})
		.count();

	Q16Oracle {
		groups,
		excluded_suppliers: excluded.len(),
		kept_partsupp_rows,
		dedup_groups,
		pruned_parts,
	}
}

pub fn strict_q16_oracle(scale_factor: f64) -> Q16Oracle {
	q16_oracle_from_rows(
		PartGenerator::new(scale_factor, 1, 1).iter().map(|part| {
			(
				part.p_partkey,
				part.p_brand.to_string(),
				part.p_type.to_string(),
				part.p_size,
			)
		}),
		SupplierGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|supplier| (supplier.s_suppkey, supplier.s_comment.to_string())),
		PartSuppGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|partsupp| (partsupp.ps_partkey, partsupp.ps_suppkey)),
	)
}

fn strict_q16_expected_schema() -> [(&'static str, DataType); 4] {
	[
		("p_brand", DataType::Utf8),
		("p_type", DataType::Utf8),
		("p_size", DataType::Int32),
		("supplier_cnt", DataType::Int64),
	]
}

pub fn assert_strict_q16_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q16_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q16 produced no output batch".to_string());
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q16Row {
	pub p_brand: String,
	pub p_type: String,
	pub p_size: i32,
	pub supplier_cnt: i64,
}

pub fn compare_strict_q16(batches: &[RecordBatch], oracle: &Q16Oracle) -> Result<(), String> {
	assert_strict_q16_schema(batches)?;
	if oracle.groups.is_empty()
		|| oracle.kept_partsupp_rows == 0
		|| oracle.dedup_groups == 0
		|| oracle.pruned_parts == 0
	{
		return Err(format!(
			"oracle non-vacuous guard failed: groups={} kept_partsupp_rows={} dedup_groups={} pruned_parts={}",
			oracle.groups.len(),
			oracle.kept_partsupp_rows,
			oracle.dedup_groups,
			oracle.pruned_parts
		));
	}

	let mut emitted = Vec::new();
	let mut emitted_by_key: BTreeMap<(String, String, i32), i64> = BTreeMap::new();
	for batch in batches {
		if batch.num_columns() != 4 {
			return Err(format!("expected 4 columns, got {}", batch.num_columns()));
		}
		let brands = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (p_brand) not Utf8")?;
		let types = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 1 (p_type) not Utf8")?;
		let sizes = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int32Array>()
			.ok_or("column 2 (p_size) not Int32")?;
		let counts = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 3 (supplier_cnt) not Int64")?;
		for row in 0..batch.num_rows() {
			let row_index = emitted.len();
			if brands.is_null(row) {
				return Err(format!("column p_brand row {row_index} is NULL"));
			}
			if types.is_null(row) {
				return Err(format!("column p_type row {row_index} is NULL"));
			}
			if sizes.is_null(row) {
				return Err(format!("column p_size row {row_index} is NULL"));
			}
			if counts.is_null(row) {
				return Err(format!("column supplier_cnt row {row_index} is NULL"));
			}
			let output = Q16Row {
				p_brand: brands.value(row).to_string(),
				p_type: types.value(row).to_string(),
				p_size: sizes.value(row),
				supplier_cnt: counts.value(row),
			};
			let key = (output.p_brand.clone(), output.p_type.clone(), output.p_size);
			if emitted_by_key
				.insert(key.clone(), output.supplier_cnt)
				.is_some()
			{
				return Err(format!("duplicate emitted q16 group {key:?}"));
			}
			emitted.push(output);
		}
	}

	for pair in emitted.windows(2) {
		let previous = &pair[0];
		let next = &pair[1];
		let order_violation = previous.supplier_cnt < next.supplier_cnt
			|| (previous.supplier_cnt == next.supplier_cnt
				&& (
					previous.p_brand.as_str(),
					previous.p_type.as_str(),
					previous.p_size,
				) > (next.p_brand.as_str(), next.p_type.as_str(), next.p_size));
		if order_violation {
			return Err(format!(
				"ORDER BY supplier_cnt DESC, p_brand, p_type, p_size violated: {previous:?} emitted before {next:?}"
			));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	for (key, suppliers) in &oracle.groups {
		let expected = suppliers.len() as i64;
		let Some(actual) = emitted_by_key.get(key) else {
			return Err(format!("oracle q16 group {key:?} missing from output"));
		};
		if *actual != expected {
			return Err(format!(
				"q16 group {key:?}: supplier_cnt mismatch: expected {expected}, got {actual}"
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q16(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q16_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q16".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q16_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q16_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = strict_q16_oracle(scale_factor);
	match compare_strict_q16(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q22Group {
	pub numcust: i64,
	pub totacctbal_raw: i128,
}

#[derive(Debug, Clone)]
pub struct Q22Oracle {
	pub groups: BTreeMap<String, Q22Group>,
	pub avg_threshold_raw: i128,
	pub with_orders_excluded: usize,
	pub selected_rows: usize,
}

const Q22_COUNTRY_CODES: [&str; 7] = ["13", "31", "23", "29", "30", "18", "17"];

pub fn q22_country_code(phone: &str) -> Option<String> {
	let mut chars = phone.chars();
	let first = chars.next()?;
	let second = chars.next()?;
	Some(format!("{first}{second}"))
}

pub fn q22_country_code_is_selected(code: &str) -> bool {
	Q22_COUNTRY_CODES.contains(&code)
}

pub fn q22_acctbal_above_threshold(
	acctbal_raw: i128,
	avg_threshold_raw: i128,
) -> Result<bool, String> {
	let aligned = acctbal_raw
		.checked_mul(10_000)
		.ok_or_else(|| "q22 account-balance rescale overflow".to_string())?;
	Ok(aligned > avg_threshold_raw)
}

pub fn q22_oracle_from_rows(
	customers: impl IntoIterator<Item = (i64, String, i128)>,
	order_custkeys: impl IntoIterator<Item = i64>,
) -> Result<Q22Oracle, String> {
	let customers: Vec<(i64, String, i128)> = customers.into_iter().collect();
	let mut threshold_sum = 0_i128;
	let mut threshold_count = 0_i64;
	for (_, phone, acctbal_raw) in &customers {
		let Some(code) = q22_country_code(phone) else {
			continue;
		};
		if q22_country_code_is_selected(&code) && *acctbal_raw > 0 {
			threshold_sum = threshold_sum
				.checked_add(*acctbal_raw)
				.ok_or_else(|| "q22 AVG threshold sum overflow".to_string())?;
			threshold_count += 1;
		}
	}
	let avg_threshold_raw = avg_decimal_raw(threshold_sum, threshold_count, 2, 6, 19)?;
	let order_custkeys: HashSet<i64> = order_custkeys.into_iter().collect();

	let mut groups: BTreeMap<String, Q22Group> = BTreeMap::new();
	let mut with_orders_excluded = 0_usize;
	let mut selected_rows = 0_usize;
	for (custkey, phone, acctbal_raw) in customers {
		let Some(code) = q22_country_code(&phone) else {
			continue;
		};
		if !q22_country_code_is_selected(&code) {
			continue;
		}
		if !q22_acctbal_above_threshold(acctbal_raw, avg_threshold_raw)? {
			continue;
		}
		if order_custkeys.contains(&custkey) {
			with_orders_excluded += 1;
			continue;
		}

		let group = groups.entry(code).or_insert(Q22Group {
			numcust: 0,
			totacctbal_raw: 0,
		});
		group.numcust += 1;
		group.totacctbal_raw = group
			.totacctbal_raw
			.checked_add(acctbal_raw)
			.ok_or_else(|| "q22 totacctbal sum overflow".to_string())?;
		selected_rows += 1;
	}

	Ok(Q22Oracle {
		groups,
		avg_threshold_raw,
		with_orders_excluded,
		selected_rows,
	})
}

pub fn strict_q22_oracle(scale_factor: f64) -> Result<Q22Oracle, String> {
	q22_oracle_from_rows(
		CustomerGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|customer| {
				(
					customer.c_custkey,
					customer.c_phone.to_string(),
					customer.c_acctbal.into_inner() as i128,
				)
			}),
		OrderGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|order| order.o_custkey),
	)
}

fn strict_q22_expected_schema() -> [(&'static str, DataType); 3] {
	[
		("cntrycode", DataType::Utf8),
		("numcust", DataType::Int64),
		("totacctbal", DataType::Decimal128(15, 2)),
	]
}

pub fn assert_strict_q22_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q22_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q22 produced no output batch".to_string());
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

pub fn compare_strict_q22(batches: &[RecordBatch], oracle: &Q22Oracle) -> Result<(), String> {
	assert_strict_q22_schema(batches)?;
	if oracle.selected_rows == 0 || oracle.groups.is_empty() {
		return Err(format!(
			"oracle non-vacuous guard failed: selected_rows={} groups={}",
			oracle.selected_rows,
			oracle.groups.len()
		));
	}
	if oracle.avg_threshold_raw <= 0 {
		return Err(format!(
			"oracle AVG threshold should be positive, got {}",
			oracle.avg_threshold_raw
		));
	}

	let mut emitted_order = Vec::new();
	let mut emitted: BTreeMap<String, Q22Group> = BTreeMap::new();
	for batch in batches {
		if batch.num_columns() != 3 {
			return Err(format!("expected 3 columns, got {}", batch.num_columns()));
		}
		let codes = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (cntrycode) not Utf8")?;
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 1 (numcust) not Int64")?;
		let totals = batch
			.column(2)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 2 (totacctbal) not Decimal128")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted_order.len();
			if codes.is_null(i) {
				return Err(format!("column cntrycode row {row_index} is NULL"));
			}
			if counts.is_null(i) {
				return Err(format!("column numcust row {row_index} is NULL"));
			}
			if totals.is_null(i) {
				return Err(format!("column totacctbal row {row_index} is NULL"));
			}
			let code = codes.value(i).to_string();
			let group = Q22Group {
				numcust: counts.value(i),
				totacctbal_raw: totals.value(i),
			};
			if emitted.insert(code.clone(), group).is_some() {
				return Err(format!("duplicate emitted cntrycode {code:?}"));
			}
			emitted_order.push(code);
		}
	}

	for pair in emitted_order.windows(2) {
		if pair[0] > pair[1] {
			return Err(format!(
				"ORDER BY cntrycode violated: {:?} emitted before {:?}",
				pair[0], pair[1]
			));
		}
	}

	if emitted.len() != oracle.groups.len() {
		return Err(format!(
			"row count mismatch: oracle has {} groups, output has {} rows",
			oracle.groups.len(),
			emitted.len()
		));
	}
	for (code, expected) in &oracle.groups {
		let Some(actual) = emitted.get(code) else {
			return Err(format!("oracle cntrycode {code:?} missing from output"));
		};
		if actual.numcust != expected.numcust {
			return Err(format!(
				"cntrycode {code:?} numcust mismatch: expected {}, got {}",
				expected.numcust, actual.numcust
			));
		}
		if actual.totacctbal_raw != expected.totacctbal_raw {
			return Err(format!(
				"cntrycode {code:?} totacctbal mismatch: expected {}, got {}",
				expected.totacctbal_raw, actual.totacctbal_raw
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q22(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q22_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q22".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q22_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q22_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q22_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q22(&batches, &oracle) {
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
	run_query_with_tables(
		"q14",
		"dev",
		scale_factor,
		Q14_DEV_SQL,
		q14_tables(false, scale_factor),
	)
	.await
}

/// ADR 269 — strict q14: the same shape with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q14(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q14",
		"strict",
		scale_factor,
		STRICT_Q14_SQL,
		q14_tables(true, scale_factor),
	)
	.await
}

/// The two q14 harness tables: `lineitem` and `part`.
fn q14_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let part = generate_part(scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "lineitem",
			descriptor: if strict {
				strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
			} else {
				dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
			},
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
	]
}

/// ADR 281 — run q17 over the `Float64` proxy `lineitem` (+ `part`). The correlated scalar subquery is the
/// measured frontier.
pub async fn run_dev_q17(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q17",
		"dev",
		scale_factor,
		Q17_DEV_SQL,
		q17_tables(false, scale_factor),
	)
	.await
}

/// ADR 281 — strict q17: the same shape with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q17(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q17",
		"strict",
		scale_factor,
		STRICT_Q17_SQL,
		q17_tables(true, scale_factor),
	)
	.await
}

/// ADR 322 - dev q18: grouped-IN body is admitted and the membership semi-join keys on the body output.
pub async fn run_dev_q18(scale_factor: f64) -> Attempt {
	run_q18("dev", false, scale_factor, Q18_DEV_SQL).await
}

/// ADR 322 - strict q18: identical SQL shape with strict Decimal128 lineitem quantity/order total.
pub async fn run_strict_q18(scale_factor: f64) -> Attempt {
	run_q18("strict", true, scale_factor, STRICT_Q18_SQL).await
}

async fn run_q18(mode: &str, strict: bool, scale_factor: f64, sql: &str) -> Attempt {
	let start = Instant::now();
	if let Err(message) = q18_grouped_membership_join_is_keyed(strict, scale_factor).await {
		let phase = if message.starts_with("plan q18") {
			Phase::Plan
		} else {
			Phase::Bind
		};
		let reason = if message.contains("was not a keyed LeftSemi join") {
			"unsupported_cross_join"
		} else {
			reason_for_error(&message)
		};
		return Attempt {
			query: "q18".to_string(),
			scale_factor,
			mode: mode.to_string(),
			phase,
			status: status_for_reason(reason),
			reason: Some(reason.to_string()),
			elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
			digest: None,
			diagnostic_message: Some(message),
		};
	}
	run_query_with_tables(
		"q18",
		mode,
		scale_factor,
		sql,
		q18_tables(strict, scale_factor),
	)
	.await
}

/// Bind q18 and check whether the grouped-IN subquery is a keyed LeftSemi over a grouped body.
pub async fn q18_grouped_membership_join_is_keyed(
	strict: bool,
	scale_factor: f64,
) -> Result<(), String> {
	let mut catalog = MockCatalog::new();
	for table in q18_tables(strict, scale_factor) {
		catalog
			.register_descriptor(table.id, table.descriptor)
			.map_err(|error| format!("register {}: {error:?}", table.name))?;
		catalog
			.seed_optimizer_facts(&TableRef::new(table.name), table.facts)
			.map_err(|error| format!("seed facts {}: {error:?}", table.name))?;
	}
	let sql = if strict { STRICT_Q18_SQL } else { Q18_DEV_SQL };
	let logical = SqlPlanner::new()
		.plan(sql)
		.map_err(|error| format!("plan q18: {error}"))?;
	let bound = bind(&logical, &catalog, &gauge_planner_config())
		.await
		.map_err(|error| format!("bind q18: {error:?}"))?;
	if has_q18_grouped_membership_keyed_join(&bound) {
		Ok(())
	} else {
		let mut summaries = Vec::new();
		collect_join_summaries(&bound, &mut summaries);
		Err(format!(
			"q18 grouped-IN membership was not a keyed LeftSemi join; joins={summaries:?}"
		))
	}
}

/// ADR 319 - dev q19: discounted-revenue reachability over a keyed lineitem/part OR-branch join.
pub async fn run_dev_q19(scale_factor: f64) -> Attempt {
	run_q19("dev", false, scale_factor, Q19_DEV_SQL).await
}

/// ADR 319 - strict q19: identical SQL shape with strict Decimal128 lineitem columns.
pub async fn run_strict_q19(scale_factor: f64) -> Attempt {
	run_q19("strict", true, scale_factor, STRICT_Q19_SQL).await
}

/// ADR 317 - dev q21: final reachability probe over supplier/lineitem/orders/nation with correlated
/// EXISTS/NOT EXISTS self-aliases.
pub async fn run_dev_q21(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q21",
		"dev",
		scale_factor,
		Q21_DEV_SQL,
		q21_tables(false, scale_factor),
	)
	.await
}

/// ADR 317 - strict q21: identical SQL shape; strict mode carries official Decimal128 lineitem columns.
pub async fn run_strict_q21(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q21",
		"strict",
		scale_factor,
		STRICT_Q21_SQL,
		q21_tables(true, scale_factor),
	)
	.await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q21Row {
	pub s_name: String,
	pub numwait: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q21Oracle {
	pub rows: Vec<Q21Row>,
	pub contributing_rows: usize,
}

struct Q21LineitemGroups<I>
where
	I: Iterator<Item = (i64, i64, i32, i32)>,
{
	rows: std::iter::Peekable<I>,
}

impl<I> Q21LineitemGroups<I>
where
	I: Iterator<Item = (i64, i64, i32, i32)>,
{
	fn new(rows: I) -> Self {
		Self {
			rows: rows.peekable(),
		}
	}

	fn next_group(&mut self) -> Result<Option<(i64, Vec<(i64, i32, i32)>)>, String> {
		let Some((orderkey, suppkey, commitdate, receiptdate)) = self.rows.next() else {
			return Ok(None);
		};
		let mut items = vec![(suppkey, commitdate, receiptdate)];

		while let Some((next_orderkey, _, _, _)) = self.rows.peek() {
			if *next_orderkey < orderkey {
				return Err(format!(
					"q21 lineitem order stream is not nondecreasing: previous group key {orderkey}, current key {next_orderkey}"
				));
			}
			if *next_orderkey != orderkey {
				break;
			}
			let (_, suppkey, commitdate, receiptdate) =
				self.rows.next().expect("peeked q21 lineitem row");
			items.push((suppkey, commitdate, receiptdate));
		}

		Ok(Some((orderkey, items)))
	}
}

fn q21_next_order<I>(
	orders: &mut I,
	previous_orderkey: &mut Option<i64>,
) -> Result<Option<(i64, String)>, String>
where
	I: Iterator<Item = (i64, String)>,
{
	let Some((orderkey, status)) = orders.next() else {
		return Ok(None);
	};
	if previous_orderkey.is_some_and(|previous| orderkey <= previous) {
		return Err(format!(
			"q21 orders stream is not strictly increasing: previous key {}, current key {orderkey}",
			previous_orderkey.expect("checked q21 previous orderkey")
		));
	}
	*previous_orderkey = Some(orderkey);
	Ok(Some((orderkey, status)))
}

fn q21_count_order(
	order_status: &str,
	items: &[(i64, i32, i32)],
	suppliers: &HashMap<i64, (String, i64)>,
	nations: &HashMap<i64, String>,
	counts: &mut HashMap<String, i64>,
	contributing_rows: &mut usize,
) -> Result<(), String> {
	if order_status != "F" {
		return Ok(());
	}

	let supplier_set: HashSet<i64> = items.iter().map(|(suppkey, _, _)| *suppkey).collect();
	let late_supplier_set: HashSet<i64> = items
		.iter()
		.filter(|(_, commitdate, receiptdate)| receiptdate > commitdate)
		.map(|(suppkey, _, _)| *suppkey)
		.collect();

	for (suppkey, commitdate, receiptdate) in items {
		if receiptdate <= commitdate {
			continue;
		}
		let Some((s_name, nationkey)) = suppliers.get(suppkey) else {
			continue;
		};
		let Some(nation_name) = nations.get(nationkey) else {
			continue;
		};
		if nation_name != "SAUDI ARABIA" {
			continue;
		}
		if !supplier_set
			.iter()
			.any(|other_suppkey| other_suppkey != suppkey)
		{
			continue;
		}
		if late_supplier_set
			.iter()
			.any(|other_suppkey| other_suppkey != suppkey)
		{
			continue;
		}

		*contributing_rows = contributing_rows
			.checked_add(1)
			.ok_or_else(|| "q21 contributing_rows counter overflow".to_string())?;
		let count = counts.entry(s_name.clone()).or_insert(0);
		*count = count
			.checked_add(1)
			.ok_or_else(|| format!("q21 supplier count overflow for {s_name:?}"))?;
	}

	Ok(())
}

fn q21_finish_oracle(counts: HashMap<String, i64>, contributing_rows: usize) -> Q21Oracle {
	let mut rows = counts
		.into_iter()
		.map(|(s_name, numwait)| Q21Row { s_name, numwait })
		.collect::<Vec<_>>();
	rows.sort_by(|left, right| {
		right
			.numwait
			.cmp(&left.numwait)
			.then_with(|| left.s_name.cmp(&right.s_name))
	});

	Q21Oracle {
		rows,
		contributing_rows,
	}
}

/// Independent strict-q21 oracle: traverse the generators directly, count every qualifying `l1` row, and
/// group the answer by supplier name with the executor's `numwait DESC, s_name` ordering.
pub fn q21_oracle_from_rows(
	suppliers: impl IntoIterator<Item = (i64, String, i64)>,
	nations: impl IntoIterator<Item = (i64, String)>,
	orders: impl IntoIterator<Item = (i64, String)>,
	lineitems: impl IntoIterator<Item = (i64, i64, i32, i32)>,
) -> Result<Q21Oracle, String> {
	let suppliers: HashMap<i64, (String, i64)> = suppliers
		.into_iter()
		.map(|(suppkey, name, nationkey)| (suppkey, (name, nationkey)))
		.collect();
	let nations: HashMap<i64, String> = nations.into_iter().collect();
	let mut counts: HashMap<String, i64> = HashMap::new();
	let mut contributing_rows = 0_usize;

	let mut orders = orders.into_iter();
	let mut previous_orderkey = None;
	let mut next_order = q21_next_order(&mut orders, &mut previous_orderkey)?;
	let mut lineitem_groups = Q21LineitemGroups::new(lineitems.into_iter());
	let mut next_group = lineitem_groups.next_group()?;

	while let (Some((orderkey, order_status)), Some((lineitem_orderkey, items))) =
		(next_order.as_ref(), next_group.as_ref())
	{
		match orderkey.cmp(lineitem_orderkey) {
			std::cmp::Ordering::Less => {
				next_order = q21_next_order(&mut orders, &mut previous_orderkey)?;
			}
			std::cmp::Ordering::Greater => {
				next_group = lineitem_groups.next_group()?;
			}
			std::cmp::Ordering::Equal => {
				q21_count_order(
					order_status,
					items,
					&suppliers,
					&nations,
					&mut counts,
					&mut contributing_rows,
				)?;
				next_order = q21_next_order(&mut orders, &mut previous_orderkey)?;
				next_group = lineitem_groups.next_group()?;
			}
		}
	}

	while next_order.is_some() {
		next_order = q21_next_order(&mut orders, &mut previous_orderkey)?;
	}
	while next_group.is_some() {
		next_group = lineitem_groups.next_group()?;
	}

	Ok(q21_finish_oracle(counts, contributing_rows))
}

pub fn strict_q21_oracle(scale_factor: f64) -> Result<Q21Oracle, String> {
	q21_oracle_from_rows(
		SupplierGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|supplier| {
				(
					supplier.s_suppkey,
					supplier.s_name.to_string(),
					supplier.s_nationkey,
				)
			}),
		NationGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|nation| (nation.n_nationkey, nation.n_name.to_string())),
		OrderGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|order| (order.o_orderkey, order.o_orderstatus.to_string())),
		LineItemGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|item| {
				(
					item.l_orderkey,
					item.l_suppkey,
					item.l_commitdate.to_unix_epoch(),
					item.l_receiptdate.to_unix_epoch(),
				)
			}),
	)
}

#[cfg(test)]
mod adr428_q21_frugal_oracle {
	use super::*;

	type Q21SupplierRow = (i64, String, i64);
	type Q21NationRow = (i64, String);
	type Q21OrderRow = (i64, String);
	type Q21LineitemRow = (i64, i64, i32, i32);

	fn q21_oracle_dense_from_rows(
		suppliers: impl IntoIterator<Item = Q21SupplierRow>,
		nations: impl IntoIterator<Item = Q21NationRow>,
		orders: impl IntoIterator<Item = Q21OrderRow>,
		lineitems: impl IntoIterator<Item = Q21LineitemRow>,
	) -> Result<Q21Oracle, String> {
		let suppliers: HashMap<i64, (String, i64)> = suppliers
			.into_iter()
			.map(|(suppkey, name, nationkey)| (suppkey, (name, nationkey)))
			.collect();
		let nations: HashMap<i64, String> = nations.into_iter().collect();
		let orders: HashMap<i64, String> = orders.into_iter().collect();
		let mut lineitems_by_order: HashMap<i64, Vec<(i64, i32, i32)>> = HashMap::new();
		for (orderkey, suppkey, commitdate, receiptdate) in lineitems {
			lineitems_by_order.entry(orderkey).or_default().push((
				suppkey,
				commitdate,
				receiptdate,
			));
		}

		let mut counts = HashMap::new();
		let mut contributing_rows = 0;
		for (orderkey, items) in lineitems_by_order {
			let Some(order_status) = orders.get(&orderkey) else {
				continue;
			};
			q21_count_order(
				order_status,
				&items,
				&suppliers,
				&nations,
				&mut counts,
				&mut contributing_rows,
			)?;
		}

		Ok(q21_finish_oracle(counts, contributing_rows))
	}

	fn strict_q21_oracle_dense(scale_factor: f64) -> Result<Q21Oracle, String> {
		q21_oracle_dense_from_rows(
			SupplierGenerator::new(scale_factor, 1, 1)
				.iter()
				.map(|supplier| {
					(
						supplier.s_suppkey,
						supplier.s_name.to_string(),
						supplier.s_nationkey,
					)
				}),
			NationGenerator::new(scale_factor, 1, 1)
				.iter()
				.map(|nation| (nation.n_nationkey, nation.n_name.to_string())),
			OrderGenerator::new(scale_factor, 1, 1)
				.iter()
				.map(|order| (order.o_orderkey, order.o_orderstatus.to_string())),
			LineItemGenerator::new(scale_factor, 1, 1)
				.iter()
				.map(|item| {
					(
						item.l_orderkey,
						item.l_suppkey,
						item.l_commitdate.to_unix_epoch(),
						item.l_receiptdate.to_unix_epoch(),
					)
				}),
		)
	}

	fn assert_dense_frugal_equal(
		suppliers: Vec<Q21SupplierRow>,
		nations: Vec<Q21NationRow>,
		orders: Vec<Q21OrderRow>,
		lineitems: Vec<Q21LineitemRow>,
	) -> Q21Oracle {
		let frugal = q21_oracle_from_rows(
			suppliers.clone(),
			nations.clone(),
			orders.clone(),
			lineitems.clone(),
		)
		.expect("frugal q21 oracle");
		let dense = q21_oracle_dense_from_rows(suppliers, nations, orders, lineitems)
			.expect("dense q21 oracle");
		assert_eq!(frugal, dense);
		frugal
	}

	#[test]
	fn frugal_matches_dense_at_supported_parity_scales() {
		for scale_factor in [0.01, 1.0] {
			let frugal = strict_q21_oracle(scale_factor).expect("frugal q21 oracle");
			let dense = strict_q21_oracle_dense(scale_factor).expect("dense q21 oracle");
			assert_eq!(frugal, dense, "q21 oracle drift at sf={scale_factor}");
		}
	}

	#[test]
	fn shared_counter_preserves_all_semi_anti_edges() {
		let suppliers = vec![
			(1, "SAUDI-1".to_string(), 10),
			(2, "SAUDI-2".to_string(), 10),
			(3, "UNKNOWN-NATION".to_string(), 99),
			(4, "MOROCCAN".to_string(), 11),
		];
		let nations = vec![
			(10, "SAUDI ARABIA".to_string()),
			(11, "MOROCCO".to_string()),
		];
		let orders = vec![
			(10, "O".to_string()),
			(20, "F".to_string()),
			(30, "F".to_string()),
			(40, "F".to_string()),
			(50, "F".to_string()),
			(60, "F".to_string()),
			(70, "F".to_string()),
			(80, "F".to_string()),
			(90, "F".to_string()),
		];
		let lineitems = vec![
			(10, 1, 10, 20),
			(10, 2, 10, 10),
			(20, 1, 10, 20),
			(20, 1, 11, 21),
			(30, 1, 10, 20),
			(30, 2, 10, 20),
			(40, 1, 10, 20),
			(40, 1, 11, 21),
			(40, 2, 10, 10),
			(50, 1, 10, 10),
			(50, 2, 10, 10),
			(60, 2, 10, 10),
			(60, 4, 10, 20),
			(70, 2, 10, 10),
			(70, 999, 10, 20),
			(80, 2, 10, 10),
			(80, 3, 10, 20),
			(100, 1, 10, 20),
			(100, 2, 10, 10),
		];

		let oracle = assert_dense_frugal_equal(suppliers, nations, orders, lineitems);
		assert_eq!(oracle.contributing_rows, 2);
		assert_eq!(
			oracle.rows,
			vec![Q21Row {
				s_name: "SAUDI-1".to_string(),
				numwait: 2,
			}]
		);
	}

	#[test]
	fn contiguous_lineitems_form_one_group() {
		let oracle = q21_oracle_from_rows(
			[(1, "SAUDI".to_string(), 10), (2, "OTHER".to_string(), 11)],
			[
				(10, "SAUDI ARABIA".to_string()),
				(11, "MOROCCO".to_string()),
			],
			[(7, "F".to_string())],
			[(7, 1, 10, 20), (7, 1, 11, 21), (7, 2, 10, 10)],
		)
		.expect("contiguous q21 group");
		assert_eq!(oracle.contributing_rows, 2);
	}

	#[test]
	fn order_and_lineitem_invariants_fail_loudly() {
		let duplicate_orders = q21_oracle_from_rows(
			Vec::<Q21SupplierRow>::new(),
			Vec::<Q21NationRow>::new(),
			[(1, "F".to_string()), (1, "F".to_string())],
			Vec::<Q21LineitemRow>::new(),
		)
		.expect_err("duplicate q21 orders must fail");
		assert!(duplicate_orders.contains("orders stream is not strictly increasing"));

		let decreasing_orders = q21_oracle_from_rows(
			Vec::<Q21SupplierRow>::new(),
			Vec::<Q21NationRow>::new(),
			[(2, "F".to_string()), (1, "F".to_string())],
			Vec::<Q21LineitemRow>::new(),
		)
		.expect_err("decreasing q21 orders must fail");
		assert!(decreasing_orders.contains("previous key 2, current key 1"));

		let reappearing_lineitems = q21_oracle_from_rows(
			Vec::<Q21SupplierRow>::new(),
			Vec::<Q21NationRow>::new(),
			Vec::<Q21OrderRow>::new(),
			[(1, 1, 10, 10), (2, 1, 10, 10), (1, 1, 10, 10)],
		)
		.expect_err("reappearing q21 lineitem group must fail");
		assert!(
			reappearing_lineitems.contains("lineitem order stream is not nondecreasing")
				&& reappearing_lineitems.contains("previous group key 2, current key 1")
		);
	}

	#[test]
	fn both_unmatched_tails_are_drained_and_validated() {
		let order_tail = q21_oracle_from_rows(
			Vec::<Q21SupplierRow>::new(),
			Vec::<Q21NationRow>::new(),
			[
				(1, "F".to_string()),
				(3, "F".to_string()),
				(2, "F".to_string()),
			],
			[(1, 1, 10, 10)],
		)
		.expect_err("q21 order-tail violation must fail");
		assert!(order_tail.contains("previous key 3, current key 2"));

		let lineitem_tail = q21_oracle_from_rows(
			Vec::<Q21SupplierRow>::new(),
			Vec::<Q21NationRow>::new(),
			[(1, "F".to_string())],
			[(1, 1, 10, 10), (3, 1, 10, 10), (2, 1, 10, 10)],
		)
		.expect_err("q21 lineitem-tail violation must fail");
		assert!(lineitem_tail.contains("previous group key 3, current key 2"));
	}

	#[test]
	fn checked_counter_overflow_is_loud() {
		let suppliers = HashMap::from([
			(1, ("SAUDI".to_string(), 10)),
			(2, ("OTHER".to_string(), 11)),
		]);
		let nations = HashMap::from([
			(10, "SAUDI ARABIA".to_string()),
			(11, "MOROCCO".to_string()),
		]);
		let items = [(1, 10, 20), (2, 10, 10)];

		let mut counts = HashMap::new();
		let mut contributing_rows = usize::MAX;
		let rows_error = q21_count_order(
			"F",
			&items,
			&suppliers,
			&nations,
			&mut counts,
			&mut contributing_rows,
		)
		.expect_err("q21 contributing row overflow must fail");
		assert_eq!(rows_error, "q21 contributing_rows counter overflow");

		let mut counts = HashMap::from([("SAUDI".to_string(), i64::MAX)]);
		let mut contributing_rows = 0;
		let count_error = q21_count_order(
			"F",
			&items,
			&suppliers,
			&nations,
			&mut counts,
			&mut contributing_rows,
		)
		.expect_err("q21 supplier count overflow must fail");
		assert!(count_error.contains("supplier count overflow for \"SAUDI\""));
	}

	#[test]
	fn unchanged_non_vacuity_guard_stays_armed() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("s_name", DataType::Utf8, false),
			Field::new("numwait", DataType::Int64, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(StringArray::from(Vec::<String>::new())),
				Arc::new(Int64Array::from(Vec::<i64>::new())),
			],
		)
		.expect("empty q21 batch");
		let oracle = Q21Oracle {
			rows: Vec::new(),
			contributing_rows: 0,
		};

		let error =
			compare_strict_q21(&[batch], &oracle).expect_err("empty q21 oracle must be rejected");
		assert!(error.contains("oracle non-vacuous guard failed"));
	}
}

fn strict_q21_expected_schema() -> [(&'static str, DataType); 2] {
	[("s_name", DataType::Utf8), ("numwait", DataType::Int64)]
}

pub fn assert_strict_q21_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q21_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q21 produced no output batch".to_string());
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

pub fn compare_strict_q21(batches: &[RecordBatch], oracle: &Q21Oracle) -> Result<(), String> {
	assert_strict_q21_schema(batches)?;
	if oracle.contributing_rows == 0 || oracle.rows.is_empty() {
		return Err(format!(
			"oracle non-vacuous guard failed: contributing_rows={} rows={}",
			oracle.contributing_rows,
			oracle.rows.len()
		));
	}

	let mut emitted = Vec::new();
	for batch in batches {
		if batch.num_columns() != 2 {
			return Err(format!("expected 2 columns, got {}", batch.num_columns()));
		}
		let names = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (s_name) not Utf8")?;
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 1 (numwait) not Int64")?;
		for row in 0..batch.num_rows() {
			let row_index = emitted.len();
			if names.is_null(row) {
				return Err(format!("column s_name row {row_index} is NULL"));
			}
			if counts.is_null(row) {
				return Err(format!("column numwait row {row_index} is NULL"));
			}
			emitted.push(Q21Row {
				s_name: names.value(row).to_string(),
				numwait: counts.value(row),
			});
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].numwait < pair[1].numwait
			|| (pair[0].numwait == pair[1].numwait && pair[0].s_name > pair[1].s_name)
		{
			return Err(format!(
				"ORDER BY numwait DESC, s_name violated: {:?} emitted before {:?}",
				pair[0], pair[1]
			));
		}
	}

	let expected = oracle.rows.iter().take(100).collect::<Vec<_>>();
	if emitted.len() != expected.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			expected.len(),
			emitted.len()
		));
	}
	for (idx, (actual, expected_row)) in emitted.iter().zip(expected.iter()).enumerate() {
		if actual.s_name != expected_row.s_name {
			return Err(format!(
				"row {idx}: expected s_name {:?}, got {:?}",
				expected_row.s_name, actual.s_name
			));
		}
		if actual.numwait != expected_row.numwait {
			return Err(format!(
				"row {idx} ({}) numwait mismatch: expected {}, got {}",
				actual.s_name, expected_row.numwait, actual.numwait
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q21(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q21_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q21".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q21_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q21_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q21_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q21(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

async fn run_q19(mode: &str, strict: bool, scale_factor: f64, sql: &str) -> Attempt {
	let start = Instant::now();
	if let Err(message) = q19_lineitem_part_join_is_keyed(strict, scale_factor).await {
		let phase = if message.starts_with("plan q19") {
			Phase::Plan
		} else {
			Phase::Bind
		};
		let reason = if message.contains("was not a keyed Inner join") {
			"unsupported_cross_join"
		} else {
			reason_for_error(&message)
		};
		return Attempt {
			query: "q19".to_string(),
			scale_factor,
			mode: mode.to_string(),
			phase,
			status: status_for_reason(reason),
			reason: Some(reason.to_string()),
			elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
			digest: None,
			diagnostic_message: Some(message),
		};
	}
	run_query_with_tables(
		"q19",
		mode,
		scale_factor,
		sql,
		q19_tables(strict, scale_factor),
	)
	.await
}

/// Bind q19 and check whether the lineitem/part comma equality becomes a keyed Inner join.
pub async fn q19_lineitem_part_join_is_keyed(
	strict: bool,
	scale_factor: f64,
) -> Result<(), String> {
	let mut catalog = MockCatalog::new();
	for table in q19_tables(strict, scale_factor) {
		catalog
			.register_descriptor(table.id, table.descriptor)
			.map_err(|error| format!("register {}: {error:?}", table.name))?;
		catalog
			.seed_optimizer_facts(&TableRef::new(table.name), table.facts)
			.map_err(|error| format!("seed facts {}: {error:?}", table.name))?;
	}
	let sql = if strict { STRICT_Q19_SQL } else { Q19_DEV_SQL };
	let logical = SqlPlanner::new()
		.plan(sql)
		.map_err(|error| format!("plan q19: {error}"))?;
	let bound = bind(&logical, &catalog, &gauge_planner_config())
		.await
		.map_err(|error| format!("bind q19: {error:?}"))?;
	if has_q19_lineitem_part_keyed_join(&bound) {
		Ok(())
	} else {
		let mut summaries = Vec::new();
		collect_join_summaries(&bound, &mut summaries);
		Err(format!(
			"q19 lineitem/part equality was not a keyed Inner join; joins={summaries:?}"
		))
	}
}

fn has_q19_lineitem_part_keyed_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			output_schema,
			..
		} => {
			(*kind == JoinKind::Inner
				&& left_keys.len() == 1
				&& right_keys.len() == 1
				&& schema_has_fields(
					output_schema,
					&["l_partkey", "p_partkey", "p_brand", "l_quantity"],
				) && !schema_has_fields(output_schema, &["l_shipmode"])
				&& !schema_has_fields(output_schema, &["l_shipinstruct"])
				&& (has_q19_probe_ship_filter(left) || has_q19_probe_ship_filter(right)))
				|| has_q19_lineitem_part_keyed_join(left)
				|| has_q19_lineitem_part_keyed_join(right)
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => has_q19_lineitem_part_keyed_join(input),
		ExecutionPlan::Union { left, right, .. } => {
			has_q19_lineitem_part_keyed_join(left) || has_q19_lineitem_part_keyed_join(right)
		}
		ExecutionPlan::RecursiveCte {
			seed, recursive, ..
		} => has_q19_lineitem_part_keyed_join(seed) || has_q19_lineitem_part_keyed_join(recursive),
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::InsertSelect { source, .. }
		| ExecutionPlan::Update { source, .. } => has_q19_lineitem_part_keyed_join(source),
		_ => false,
	}
}

fn has_q19_probe_ship_filter(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Filter {
			input, predicate, ..
		} => {
			bound_program_has_column(predicate, "l_shipmode")
				&& bound_program_has_column(predicate, "l_shipinstruct")
				&& matches!(input.as_ref(), ExecutionPlan::Scan { .. })
				|| has_q19_probe_ship_filter(input)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => has_q19_probe_ship_filter(input),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			has_q19_probe_ship_filter(left) || has_q19_probe_ship_filter(right)
		}
		_ => false,
	}
}

fn bound_program_has_column(program: &BoundExprProgram, name: &str) -> bool {
	program
		.nodes
		.iter()
		.any(|node| matches!(node, BoundExprNode::Column(column) if column.name.0 == name))
}

fn has_q18_grouped_membership_keyed_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			output_schema,
			..
		} => {
			(*kind == JoinKind::LeftSemi
				&& left_keys.len() == 1
				&& right_keys.len() == 1
				&& schema_has_fields(output_schema, &["c_name", "o_orderkey", "l_orderkey"])
				&& execution_plan_contains_aggregate(right))
				|| has_q18_grouped_membership_keyed_join(left)
				|| has_q18_grouped_membership_keyed_join(right)
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => has_q18_grouped_membership_keyed_join(input),
		ExecutionPlan::Union { left, right, .. } => {
			has_q18_grouped_membership_keyed_join(left)
				|| has_q18_grouped_membership_keyed_join(right)
		}
		ExecutionPlan::RecursiveCte {
			seed, recursive, ..
		} => {
			has_q18_grouped_membership_keyed_join(seed)
				|| has_q18_grouped_membership_keyed_join(recursive)
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::InsertSelect { source, .. }
		| ExecutionPlan::Update { source, .. } => has_q18_grouped_membership_keyed_join(source),
		_ => false,
	}
}

fn execution_plan_contains_aggregate(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Aggregate { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => execution_plan_contains_aggregate(input),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			execution_plan_contains_aggregate(left) || execution_plan_contains_aggregate(right)
		}
		ExecutionPlan::RecursiveCte {
			seed, recursive, ..
		} => execution_plan_contains_aggregate(seed) || execution_plan_contains_aggregate(recursive),
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::InsertSelect { source, .. }
		| ExecutionPlan::Update { source, .. } => execution_plan_contains_aggregate(source),
		_ => false,
	}
}

/// The two q17 harness tables: `lineitem` and `part`.
fn q17_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let part = generate_part(scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "lineitem",
			descriptor: if strict {
				strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
			} else {
				dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
			},
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
	]
}

/// ADR 284 — dev q02: first multi-table-inner correlated scalar query.
pub async fn run_dev_q02(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q02",
		"dev",
		scale_factor,
		Q2_DEV_SQL,
		q02_tables(false, scale_factor),
	)
	.await
}

/// ADR 284 — strict q02: same shape with official `Decimal128(15,2)` supplier/partsupp money columns.
pub async fn run_strict_q02(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q02",
		"strict",
		scale_factor,
		STRICT_Q2_SQL,
		q02_tables(true, scale_factor),
	)
	.await
}

/// ADR 285 — dev q20: nested membership plus a correlated scalar in the membership body.
pub async fn run_dev_q20(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q20",
		"dev",
		scale_factor,
		Q20_DEV_SQL,
		q20_tables(false, scale_factor),
	)
	.await
}

/// ADR 285 — strict q20: same shape, with the strict `Decimal128(15,2)` lineitem quantity.
pub async fn run_strict_q20(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q20",
		"strict",
		scale_factor,
		STRICT_Q20_SQL,
		q20_tables(true, scale_factor),
	)
	.await
}

/// ADR 307/308 - dev q13: customer distribution reachability probe for side-local LEFT OUTER ON residuals.
pub async fn run_dev_q13(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q13",
		"dev",
		scale_factor,
		Q13_DEV_SQL,
		q13_tables(false, scale_factor),
	)
	.await
}

/// ADR 307/308 - strict q13: identical SQL shape; q13 has no strict-only decimal surface.
pub async fn run_strict_q13(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q13",
		"strict",
		scale_factor,
		STRICT_Q13_SQL,
		q13_tables(true, scale_factor),
	)
	.await
}

/// ADR 320/321/422 - dev q11: HAVING scalar threshold recombines with the scale-correct FRACTION.
pub async fn run_dev_q11(scale_factor: f64) -> Result<Attempt, String> {
	let scale = Q11Scale::try_from_f64(scale_factor)?;
	let sql = q11_sql(scale);
	Ok(run_query_with_tables(
		"q11",
		"dev",
		scale_factor,
		&sql,
		q11_tables(false, scale_factor),
	)
	.await)
}

/// ADR 321/325/422 - strict q11: Decimal x Int multiply aligns and the bit-exact scale contract
/// renders the corresponding decimal FRACTION.
pub async fn run_strict_q11(scale_factor: f64) -> Result<Attempt, String> {
	let scale = Q11Scale::try_from_f64(scale_factor)?;
	let sql = q11_sql(scale);
	Ok(run_query_with_tables(
		"q11",
		"strict",
		scale_factor,
		&sql,
		q11_tables(true, scale_factor),
	)
	.await)
}

/// ADR 313 - dev q15: CTE-as-FROM-source plus scalar aggregate over the same CTE.
pub async fn run_dev_q15(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q15",
		"dev",
		scale_factor,
		Q15_DEV_SQL,
		q15_tables(false, scale_factor),
	)
	.await
}

/// ADR 313 - strict q15: identical SQL shape; strict mode carries official Decimal128 lineitem pricing columns.
pub async fn run_strict_q15(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q15",
		"strict",
		scale_factor,
		STRICT_Q15_SQL,
		q15_tables(true, scale_factor),
	)
	.await
}

/// Bind q15 and check whether the supplier/revenue comma equality becomes a keyed Inner join.
pub async fn q15_supplier_revenue_join_is_keyed(
	strict: bool,
	scale_factor: f64,
) -> Result<(), String> {
	let mut catalog = MockCatalog::new();
	for table in q15_tables(strict, scale_factor) {
		catalog
			.register_descriptor(table.id, table.descriptor)
			.map_err(|error| format!("register {}: {error:?}", table.name))?;
		catalog
			.seed_optimizer_facts(&TableRef::new(table.name), table.facts)
			.map_err(|error| format!("seed facts {}: {error:?}", table.name))?;
	}
	let sql = if strict { STRICT_Q15_SQL } else { Q15_DEV_SQL };
	let logical = SqlPlanner::new()
		.plan(sql)
		.map_err(|error| format!("plan q15: {error}"))?;
	let bound = bind(&logical, &catalog, &gauge_planner_config())
		.await
		.map_err(|error| format!("bind q15: {error:?}"))?;
	if has_q15_supplier_revenue_keyed_join(&bound) {
		Ok(())
	} else {
		let mut summaries = Vec::new();
		collect_join_summaries(&bound, &mut summaries);
		Err(format!(
			"q15 supplier/revenue equality was not a keyed Inner join; joins={summaries:?}"
		))
	}
}

fn has_q15_supplier_revenue_keyed_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			output_schema,
			..
		} => {
			(*kind == JoinKind::Inner
				&& left_keys.len() == 1
				&& right_keys.len() == 1
				&& schema_has_fields(
					output_schema,
					&["s_suppkey", "s_name", "supplier_no", "total_revenue"],
				)) || has_q15_supplier_revenue_keyed_join(left)
				|| has_q15_supplier_revenue_keyed_join(right)
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => has_q15_supplier_revenue_keyed_join(input),
		ExecutionPlan::Union { left, right, .. } => {
			has_q15_supplier_revenue_keyed_join(left) || has_q15_supplier_revenue_keyed_join(right)
		}
		ExecutionPlan::RecursiveCte {
			seed, recursive, ..
		} => {
			has_q15_supplier_revenue_keyed_join(seed)
				|| has_q15_supplier_revenue_keyed_join(recursive)
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::InsertSelect { source, .. }
		| ExecutionPlan::Update { source, .. } => has_q15_supplier_revenue_keyed_join(source),
		_ => false,
	}
}

fn schema_has_fields(schema: &SchemaRef, names: &[&str]) -> bool {
	names
		.iter()
		.all(|name| schema.field_with_name(name).is_ok())
}

fn collect_join_summaries(plan: &ExecutionPlan, summaries: &mut Vec<String>) {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			output_schema,
			..
		} => {
			let fields = output_schema
				.fields()
				.iter()
				.map(|field| field.name().as_str())
				.collect::<Vec<_>>();
			summaries.push(format!(
				"{kind:?} left_keys={} right_keys={} fields={fields:?}",
				left_keys.len(),
				right_keys.len()
			));
			collect_join_summaries(left, summaries);
			collect_join_summaries(right, summaries);
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => collect_join_summaries(input, summaries),
		ExecutionPlan::Union { left, right, .. } => {
			collect_join_summaries(left, summaries);
			collect_join_summaries(right, summaries);
		}
		ExecutionPlan::RecursiveCte {
			seed, recursive, ..
		} => {
			collect_join_summaries(seed, summaries);
			collect_join_summaries(recursive, summaries);
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::InsertSelect { source, .. }
		| ExecutionPlan::Update { source, .. } => collect_join_summaries(source, summaries),
		_ => {}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q15Row {
	pub s_suppkey: i64,
	pub s_name: String,
	pub s_address: String,
	pub s_phone: String,
	pub total_revenue_raw: i128,
}

#[derive(Debug, Clone)]
pub struct Q15Oracle {
	pub rows: Vec<Q15Row>,
	pub suppliers_in_window: usize,
	pub selected_rows: usize,
	pub max_total_revenue_raw: i128,
}

fn q15_shipdate_in_window(shipdate: i32) -> bool {
	(9496..9587).contains(&shipdate)
}

/// Independent strict-q15 oracle: traverse the supplier and lineitem generators directly, filter the
/// `1996-01-01` to `1996-04-01` shipdate window, compute per-supplier revenue at the executor's raw
/// Decimal128 scale, then keep every supplier tied at the max.
pub fn q15_oracle_from_rows(
	suppliers: impl IntoIterator<Item = (i64, String, String, String)>,
	lineitems: impl IntoIterator<Item = (i64, i32, i128, i128)>,
) -> Result<Q15Oracle, String> {
	let suppliers: HashMap<i64, (String, String, String)> = suppliers
		.into_iter()
		.map(|(suppkey, name, address, phone)| (suppkey, (name, address, phone)))
		.collect();

	let mut revenue_by_supplier: BTreeMap<i64, i128> = BTreeMap::new();
	for (suppkey, shipdate, extprice_raw, discount_raw) in lineitems {
		if !q15_shipdate_in_window(shipdate) {
			continue;
		}
		let Some(_) = suppliers.get(&suppkey) else {
			continue;
		};
		let revenue_raw = discounted_price_raw(extprice_raw, discount_raw);
		let current = revenue_by_supplier.entry(suppkey).or_insert(0);
		*current = current
			.checked_add(revenue_raw)
			.ok_or_else(|| "q15 supplier revenue overflow".to_string())?;
	}

	let suppliers_in_window = revenue_by_supplier.len();
	let max_total_revenue_raw = revenue_by_supplier.values().copied().max().unwrap_or(0);
	let mut rows = Vec::new();
	for (suppkey, total_revenue_raw) in revenue_by_supplier {
		if total_revenue_raw != max_total_revenue_raw {
			continue;
		}
		let Some((name, address, phone)) = suppliers.get(&suppkey) else {
			continue;
		};
		rows.push(Q15Row {
			s_suppkey: suppkey,
			s_name: name.clone(),
			s_address: address.clone(),
			s_phone: phone.clone(),
			total_revenue_raw,
		});
	}
	let selected_rows = rows.len();

	Ok(Q15Oracle {
		rows,
		suppliers_in_window,
		selected_rows,
		max_total_revenue_raw,
	})
}

pub fn strict_q15_oracle(scale_factor: f64) -> Result<Q15Oracle, String> {
	q15_oracle_from_rows(
		SupplierGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|supplier| {
				(
					supplier.s_suppkey,
					supplier.s_name.to_string(),
					supplier.s_address.to_string(),
					supplier.s_phone.to_string(),
				)
			}),
		LineItemGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|item| {
				(
					item.l_suppkey,
					item.l_shipdate.to_unix_epoch(),
					item.l_extendedprice.into_inner() as i128,
					item.l_discount.into_inner() as i128,
				)
			}),
	)
}

fn strict_q15_expected_schema() -> [(&'static str, DataType); 5] {
	[
		("s_suppkey", DataType::Int64),
		("s_name", DataType::Utf8),
		("s_address", DataType::Utf8),
		("s_phone", DataType::Utf8),
		("total_revenue", DataType::Decimal128(32, 4)),
	]
}

pub fn assert_strict_q15_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q15_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q15 produced no output batch".to_string());
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

pub fn compare_strict_q15(batches: &[RecordBatch], oracle: &Q15Oracle) -> Result<(), String> {
	assert_strict_q15_schema(batches)?;
	if oracle.rows.is_empty()
		|| oracle.selected_rows == 0
		|| oracle.suppliers_in_window == 0
		|| oracle.selected_rows >= oracle.suppliers_in_window
	{
		return Err(format!(
			"oracle non-vacuous guard failed: rows={} suppliers_in_window={} selected_rows={} max_total_revenue_raw={}",
			oracle.rows.len(),
			oracle.suppliers_in_window,
			oracle.selected_rows,
			oracle.max_total_revenue_raw
		));
	}

	let mut emitted_order = Vec::new();
	let mut emitted: BTreeMap<i64, Q15Row> = BTreeMap::new();
	for batch in batches {
		if batch.num_columns() != 5 {
			return Err(format!("expected 5 columns, got {}", batch.num_columns()));
		}
		let suppkeys = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 0 (s_suppkey) not Int64")?;
		let names = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 1 (s_name) not Utf8")?;
		let addresses = batch
			.column(2)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 2 (s_address) not Utf8")?;
		let phones = batch
			.column(3)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 3 (s_phone) not Utf8")?;
		let revenues = batch
			.column(4)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 4 (total_revenue) not Decimal128")?;
		for row in 0..batch.num_rows() {
			let row_index = emitted_order.len();
			if suppkeys.is_null(row)
				|| names.is_null(row)
				|| addresses.is_null(row)
				|| phones.is_null(row)
				|| revenues.is_null(row)
			{
				return Err(format!("unexpected NULL in emitted row {row_index}"));
			}
			let output = Q15Row {
				s_suppkey: suppkeys.value(row),
				s_name: names.value(row).to_string(),
				s_address: addresses.value(row).to_string(),
				s_phone: phones.value(row).to_string(),
				total_revenue_raw: revenues.value(row),
			};
			if emitted.insert(output.s_suppkey, output.clone()).is_some() {
				return Err(format!("duplicate emitted s_suppkey {}", output.s_suppkey));
			}
			emitted_order.push(output.s_suppkey);
		}
	}

	for pair in emitted_order.windows(2) {
		if pair[0] > pair[1] {
			return Err(format!(
				"ORDER BY s_suppkey violated: {} emitted before {}",
				pair[0], pair[1]
			));
		}
	}

	if emitted.len() != oracle.rows.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			oracle.rows.len(),
			emitted.len()
		));
	}
	for expected in &oracle.rows {
		let Some(actual) = emitted.get(&expected.s_suppkey) else {
			return Err(format!(
				"oracle s_suppkey {} missing from output",
				expected.s_suppkey
			));
		};
		if actual != expected {
			return Err(format!(
				"s_suppkey {} mismatch: expected {:?}, got {:?}",
				expected.s_suppkey, expected, actual
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q15(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q15_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q15".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q15_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q15_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q15_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q15(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q18Row {
	pub c_name: String,
	pub c_custkey: i64,
	pub o_orderkey: i64,
	pub o_orderdate: i32,
	pub o_totalprice_raw: i128,
	pub sum_raw: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q18Oracle {
	pub rows: Vec<Q18Row>,
	pub qualifying_orders: usize,
	pub threshold_fail_orders: usize,
	pub selected_lineitem_rows: usize,
	pub qualifying_lineitem_rows: usize,
}

#[derive(Debug)]
struct Q18PendingRow {
	c_custkey: i64,
	o_orderkey: i64,
	o_orderdate: i32,
	o_totalprice_raw: i128,
	sum_raw: i128,
}

struct Q18LineitemGroups<I>
where
	I: Iterator<Item = (i64, i128)>,
{
	rows: std::iter::Peekable<I>,
	selected_lineitem_rows: usize,
}

impl<I> Q18LineitemGroups<I>
where
	I: Iterator<Item = (i64, i128)>,
{
	fn new(rows: I) -> Self {
		Self {
			rows: rows.peekable(),
			selected_lineitem_rows: 0,
		}
	}

	fn next_group(&mut self) -> Result<Option<(i64, i128, usize)>, String> {
		let Some((orderkey, quantity_raw)) = self.rows.next() else {
			return Ok(None);
		};
		checked_q18_counter_increment(&mut self.selected_lineitem_rows, "selected_lineitem_rows")?;
		let mut sum_raw = quantity_raw;
		let mut group_rows = 1_usize;

		while let Some((next_orderkey, _)) = self.rows.peek() {
			if *next_orderkey < orderkey {
				return Err(format!(
					"q18 lineitem order stream is not monotonic: previous group key {orderkey}, current key {next_orderkey}"
				));
			}
			if *next_orderkey != orderkey {
				break;
			}
			let (_, quantity_raw) = self.rows.next().expect("peeked q18 lineitem row");
			checked_q18_counter_increment(
				&mut self.selected_lineitem_rows,
				"selected_lineitem_rows",
			)?;
			checked_q18_counter_increment(&mut group_rows, "lineitem_group_rows")?;
			sum_raw = sum_raw
				.checked_add(quantity_raw)
				.ok_or_else(|| format!("q18 order quantity overflow for orderkey {orderkey}"))?;
		}

		Ok(Some((orderkey, sum_raw, group_rows)))
	}
}

fn checked_q18_counter_increment(counter: &mut usize, name: &str) -> Result<(), String> {
	*counter = counter
		.checked_add(1)
		.ok_or_else(|| format!("q18 {name} counter overflow"))?;
	Ok(())
}

fn q18_oracle_frugal_from_rows(
	customer_keys: impl IntoIterator<Item = i64>,
	customer_names: impl IntoIterator<Item = (i64, String)>,
	orders: impl IntoIterator<Item = (i64, i64, i32, i128)>,
	lineitems: impl IntoIterator<Item = (i64, i128)>,
) -> Result<Q18Oracle, String> {
	let mut customers = HashSet::new();
	for custkey in customer_keys {
		if !customers.insert(custkey) {
			return Err(format!(
				"q18 customer membership stream has duplicate key {custkey}"
			));
		}
	}

	let mut grouped_lineitems = Q18LineitemGroups::new(lineitems.into_iter());
	let mut next_group = grouped_lineitems.next_group()?;
	let mut pending_rows = Vec::new();
	let mut qualifying_orders = 0_usize;
	let mut qualifying_lineitem_rows = 0_usize;
	let mut threshold_fail_orders = 0_usize;
	let mut previous_orderkey = None;
	for (orderkey, custkey, orderdate, totalprice_raw) in orders {
		if previous_orderkey.is_some_and(|previous| orderkey <= previous) {
			return Err(format!(
				"q18 orders stream is not strictly increasing: previous key {}, current key {orderkey}",
				previous_orderkey.expect("checked q18 previous orderkey")
			));
		}
		previous_orderkey = Some(orderkey);

		while next_group.is_some_and(|(lineitem_orderkey, _, _)| lineitem_orderkey < orderkey) {
			next_group = grouped_lineitems.next_group()?;
		}
		let Some((lineitem_orderkey, sum_raw, group_rows)) = next_group else {
			continue;
		};
		if lineitem_orderkey != orderkey {
			continue;
		}
		next_group = grouped_lineitems.next_group()?;
		if !customers.contains(&custkey) {
			continue;
		}
		if sum_raw > 30_000 {
			checked_q18_counter_increment(&mut qualifying_orders, "qualifying_orders")?;
			qualifying_lineitem_rows = qualifying_lineitem_rows
				.checked_add(group_rows)
				.ok_or_else(|| "q18 qualifying_lineitem_rows counter overflow".to_string())?;
			pending_rows.push(Q18PendingRow {
				c_custkey: custkey,
				o_orderkey: orderkey,
				o_orderdate: orderdate,
				o_totalprice_raw: totalprice_raw,
				sum_raw,
			});
		} else {
			checked_q18_counter_increment(&mut threshold_fail_orders, "threshold_fail_orders")?;
		}
	}
	while next_group.is_some() {
		next_group = grouped_lineitems.next_group()?;
	}

	let qualifying_customer_keys: HashSet<i64> = pending_rows
		.iter()
		.map(|candidate| candidate.c_custkey)
		.collect();
	let mut qualifying_customer_names = HashMap::with_capacity(qualifying_customer_keys.len());
	for (custkey, name) in customer_names {
		if qualifying_customer_keys.contains(&custkey)
			&& qualifying_customer_names.insert(custkey, name).is_some()
		{
			return Err(format!(
				"q18 customer name stream has duplicate key {custkey}"
			));
		}
	}
	let mut rows = Vec::with_capacity(pending_rows.len());
	for candidate in pending_rows {
		let c_name = qualifying_customer_names
			.get(&candidate.c_custkey)
			.ok_or_else(|| {
				format!(
					"q18 qualifying customer {} is missing from the customer name stream",
					candidate.c_custkey
				)
			})?;
		rows.push(Q18Row {
			c_name: c_name.clone(),
			c_custkey: candidate.c_custkey,
			o_orderkey: candidate.o_orderkey,
			o_orderdate: candidate.o_orderdate,
			o_totalprice_raw: candidate.o_totalprice_raw,
			sum_raw: candidate.sum_raw,
		});
	}
	rows.sort_by(|left, right| {
		right
			.o_totalprice_raw
			.cmp(&left.o_totalprice_raw)
			.then_with(|| left.o_orderdate.cmp(&right.o_orderdate))
			.then_with(|| left.o_orderkey.cmp(&right.o_orderkey))
	});

	Ok(Q18Oracle {
		rows,
		qualifying_orders,
		threshold_fail_orders,
		selected_lineitem_rows: grouped_lineitems.selected_lineitem_rows,
		qualifying_lineitem_rows,
	})
}

pub fn strict_q18_oracle(scale_factor: f64) -> Result<Q18Oracle, String> {
	q18_oracle_frugal_from_rows(
		CustomerGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|customer| customer.c_custkey),
		CustomerGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|customer| (customer.c_custkey, customer.c_name.to_string())),
		OrderGenerator::new(scale_factor, 1, 1).iter().map(|order| {
			(
				order.o_orderkey,
				order.o_custkey,
				order.o_orderdate.to_unix_epoch(),
				order.o_totalprice.into_inner() as i128,
			)
		}),
		LineItemGenerator::new(scale_factor, 1, 1)
			.iter()
			.map(|item| (item.l_orderkey, item.l_quantity as i128 * 100)),
	)
}

#[cfg(test)]
mod adr425_q18_frugal_oracle {
	use super::*;

	fn q18_oracle_dense_from_rows(
		customers: impl IntoIterator<Item = (i64, String)>,
		orders: impl IntoIterator<Item = (i64, i64, i32, i128)>,
		lineitems: impl IntoIterator<Item = (i64, i128)>,
	) -> Result<Q18Oracle, String> {
		let customers: HashMap<i64, String> = customers.into_iter().collect();
		let orders: HashMap<i64, (i64, i32, i128)> = orders
			.into_iter()
			.map(|(orderkey, custkey, orderdate, totalprice_raw)| {
				(orderkey, (custkey, orderdate, totalprice_raw))
			})
			.collect();
		let mut quantity_by_order: HashMap<i64, (i128, usize)> = HashMap::new();
		let mut selected_lineitem_rows = 0_usize;
		for (orderkey, quantity_raw) in lineitems {
			selected_lineitem_rows += 1;
			let entry = quantity_by_order.entry(orderkey).or_insert((0, 0));
			entry.0 = entry
				.0
				.checked_add(quantity_raw)
				.ok_or_else(|| "q18 order quantity overflow".to_string())?;
			entry.1 = entry
				.1
				.checked_add(1)
				.ok_or_else(|| "q18 lineitem group row counter overflow".to_string())?;
		}

		let mut rows = Vec::new();
		let mut qualifying_orders = 0_usize;
		let mut qualifying_lineitem_rows = 0_usize;
		let mut threshold_fail_orders = 0_usize;
		for (orderkey, (custkey, orderdate, totalprice_raw)) in orders {
			let Some(c_name) = customers.get(&custkey) else {
				continue;
			};
			let Some((sum_raw, group_rows)) = quantity_by_order.get(&orderkey).copied() else {
				continue;
			};
			if sum_raw > 30_000 {
				qualifying_orders += 1;
				qualifying_lineitem_rows = qualifying_lineitem_rows
					.checked_add(group_rows)
					.ok_or_else(|| "q18 qualifying lineitem counter overflow".to_string())?;
				rows.push(Q18Row {
					c_name: c_name.clone(),
					c_custkey: custkey,
					o_orderkey: orderkey,
					o_orderdate: orderdate,
					o_totalprice_raw: totalprice_raw,
					sum_raw,
				});
			} else {
				threshold_fail_orders += 1;
			}
		}
		rows.sort_by(|left, right| {
			right
				.o_totalprice_raw
				.cmp(&left.o_totalprice_raw)
				.then_with(|| left.o_orderdate.cmp(&right.o_orderdate))
				.then_with(|| left.o_orderkey.cmp(&right.o_orderkey))
		});

		Ok(Q18Oracle {
			rows,
			qualifying_orders,
			threshold_fail_orders,
			selected_lineitem_rows,
			qualifying_lineitem_rows,
		})
	}

	fn strict_q18_oracle_dense(scale_factor: f64) -> Result<Q18Oracle, String> {
		q18_oracle_dense_from_rows(
			CustomerGenerator::new(scale_factor, 1, 1)
				.iter()
				.map(|customer| (customer.c_custkey, customer.c_name.to_string())),
			OrderGenerator::new(scale_factor, 1, 1).iter().map(|order| {
				(
					order.o_orderkey,
					order.o_custkey,
					order.o_orderdate.to_unix_epoch(),
					order.o_totalprice.into_inner() as i128,
				)
			}),
			LineItemGenerator::new(scale_factor, 1, 1)
				.iter()
				.map(|item| (item.l_orderkey, item.l_quantity as i128 * 100)),
		)
	}

	fn frugal_from_vectors(
		customers: Vec<(i64, String)>,
		orders: Vec<(i64, i64, i32, i128)>,
		lineitems: Vec<(i64, i128)>,
	) -> Result<Q18Oracle, String> {
		q18_oracle_frugal_from_rows(
			customers.iter().map(|(custkey, _)| *custkey),
			customers.clone(),
			orders,
			lineitems,
		)
	}

	#[test]
	fn frugal_matches_dense_at_supported_parity_scales() {
		for scale_factor in [0.01, 1.0] {
			let frugal = strict_q18_oracle(scale_factor).expect("frugal q18 oracle");
			let dense = strict_q18_oracle_dense(scale_factor).expect("dense q18 oracle");
			assert_eq!(frugal, dense, "q18 oracle drift at sf={scale_factor}");
		}
	}

	#[test]
	#[ignore = "sf=10 oracle census; run explicitly for ADR evidence"]
	fn sf10_independent_row_flow_receipt() {
		let oracle = strict_q18_oracle(10.0).expect("sf=10 q18 oracle");
		assert_eq!(oracle.qualifying_orders, 624);
		assert_eq!(oracle.selected_lineitem_rows, 59_986_052);
		assert_eq!(oracle.qualifying_lineitem_rows, 4_368);
		eprintln!(
			"q18 sf10 oracle: qualifying_orders={} selected_lineitem_rows={} qualifying_lineitem_rows={}",
			oracle.qualifying_orders,
			oracle.selected_lineitem_rows,
			oracle.qualifying_lineitem_rows
		);
	}

	#[test]
	fn merge_preserves_all_dense_continue_cases_and_counters() {
		let customers = vec![(1, "one".to_string()), (2, "two".to_string())];
		let orders = vec![
			(10, 1, 100, 1_000),
			(20, 99, 101, 2_000),
			(30, 2, 102, 3_000),
			(40, 1, 103, 4_000),
		];
		let lineitems = vec![
			(10, 10_000),
			(10, 20_000),
			(20, 31_000),
			(40, 30_001),
			(50, 500),
		];
		let frugal = frugal_from_vectors(customers.clone(), orders.clone(), lineitems.clone())
			.expect("frugal edge oracle");
		let dense =
			q18_oracle_dense_from_rows(customers, orders, lineitems).expect("dense edge oracle");
		assert_eq!(frugal, dense);
		assert_eq!(frugal.qualifying_orders, 1);
		assert_eq!(frugal.threshold_fail_orders, 1);
		assert_eq!(frugal.selected_lineitem_rows, 5);
		assert_eq!(frugal.qualifying_lineitem_rows, 1);
		assert_eq!(frugal.rows.len(), 1);
		assert_eq!(frugal.rows[0].o_orderkey, 40);
		assert_eq!(frugal.rows[0].sum_raw, 30_001);
	}

	#[test]
	fn final_group_is_flushed_and_contiguous_same_key_rows_succeed() {
		let oracle = frugal_from_vectors(
			vec![(1, "one".to_string())],
			vec![(7, 1, 100, 1_000)],
			vec![(7, 15_000), (7, 15_001)],
		)
		.expect("one-group q18 oracle");
		assert_eq!(oracle.selected_lineitem_rows, 2);
		assert_eq!(oracle.qualifying_orders, 1);
		assert_eq!(oracle.qualifying_lineitem_rows, 2);
		assert_eq!(oracle.rows[0].sum_raw, 30_001);
	}

	#[test]
	fn order_and_lineitem_invariants_fail_loudly() {
		let customers = vec![(1, "one".to_string())];
		let shuffled = frugal_from_vectors(
			customers.clone(),
			vec![(2, 1, 100, 1_000), (1, 1, 100, 1_000)],
			Vec::new(),
		)
		.expect_err("shuffled orders must fail");
		assert!(shuffled.contains("orders stream") && shuffled.contains("previous key 2"));

		let duplicate = frugal_from_vectors(
			customers.clone(),
			vec![(1, 1, 100, 1_000), (1, 1, 100, 1_000)],
			Vec::new(),
		)
		.expect_err("duplicate orders must fail");
		assert!(duplicate.contains("orders stream") && duplicate.contains("current key 1"));

		let reappearing = frugal_from_vectors(
			customers,
			vec![(1, 1, 100, 1_000), (2, 1, 100, 1_000)],
			vec![(1, 10_000), (2, 10_000), (1, 10_000)],
		)
		.expect_err("reappearing lineitem group must fail");
		assert!(
			reappearing.contains("lineitem order stream")
				&& reappearing.contains("previous group key 2")
				&& reappearing.contains("current key 1")
		);
	}

	#[test]
	fn duplicate_or_missing_customer_names_fail_loudly() {
		let duplicate = frugal_from_vectors(
			vec![(1, "one".to_string()), (1, "again".to_string())],
			Vec::new(),
			Vec::new(),
		)
		.expect_err("duplicate customer membership must fail");
		assert!(duplicate.contains("customer membership stream has duplicate key 1"));

		let missing_name = q18_oracle_frugal_from_rows(
			[1],
			std::iter::empty(),
			[(1, 1, 100, 1_000)],
			[(1, 30_001)],
		)
		.expect_err("missing qualifying customer name must fail");
		assert!(missing_name.contains("qualifying customer 1 is missing"));
	}

	#[test]
	fn checked_quantity_and_counter_overflow_are_loud() {
		let quantity = frugal_from_vectors(
			vec![(1, "one".to_string())],
			vec![(1, 1, 100, 1_000)],
			vec![(1, i128::MAX), (1, 1)],
		)
		.expect_err("quantity overflow must fail");
		assert!(quantity.contains("quantity overflow for orderkey 1"));

		let mut counter = usize::MAX;
		let counter_error = checked_q18_counter_increment(&mut counter, "synthetic")
			.expect_err("counter overflow must fail");
		assert_eq!(counter_error, "q18 synthetic counter overflow");
	}

	#[test]
	fn frugal_oracle_retains_all_qualifying_rows_before_limit() {
		let orders: Vec<_> = (1_i64..=101)
			.map(|orderkey| (orderkey, 1, 100, orderkey as i128))
			.collect();
		let lineitems: Vec<_> = (1_i64..=101).map(|orderkey| (orderkey, 30_001)).collect();
		let oracle = frugal_from_vectors(vec![(1, "one".to_string())], orders, lineitems)
			.expect("101-row frugal oracle");
		assert_eq!(oracle.rows.len(), 101);
		assert_eq!(oracle.qualifying_orders, 101);
		assert_eq!(oracle.qualifying_lineitem_rows, 101);
	}
}

fn strict_q18_expected_schema() -> [(&'static str, DataType); 6] {
	[
		("c_name", DataType::Utf8),
		("c_custkey", DataType::Int64),
		("o_orderkey", DataType::Int64),
		("o_orderdate", DataType::Date32),
		("o_totalprice", DataType::Decimal128(15, 2)),
		("sum", DataType::Decimal128(15, 2)),
	]
}

pub fn assert_strict_q18_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q18_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q18 produced no output batch".to_string());
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

pub fn compare_strict_q18(batches: &[RecordBatch], oracle: &Q18Oracle) -> Result<(), String> {
	assert_strict_q18_schema(batches)?;
	if oracle.rows.is_empty()
		|| oracle.qualifying_orders == 0
		|| oracle.threshold_fail_orders == 0
		|| oracle.selected_lineitem_rows == 0
	{
		return Err(format!(
			"oracle non-vacuous guard failed: rows={} qualifying_orders={} threshold_fail_orders={} selected_lineitem_rows={}",
			oracle.rows.len(),
			oracle.qualifying_orders,
			oracle.threshold_fail_orders,
			oracle.selected_lineitem_rows
		));
	}

	let mut emitted = Vec::new();
	let mut emitted_by_key: BTreeMap<i64, Q18Row> = BTreeMap::new();
	for batch in batches {
		if batch.num_columns() != 6 {
			return Err(format!("expected 6 columns, got {}", batch.num_columns()));
		}
		let names = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.ok_or("column 0 (c_name) not Utf8")?;
		let custkeys = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 1 (c_custkey) not Int64")?;
		let orderkeys = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.ok_or("column 2 (o_orderkey) not Int64")?;
		let orderdates = batch
			.column(3)
			.as_any()
			.downcast_ref::<Date32Array>()
			.ok_or("column 3 (o_orderdate) not Date32")?;
		let totalprices = batch
			.column(4)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 4 (o_totalprice) not Decimal128")?;
		let sums = batch
			.column(5)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 5 (sum) not Decimal128")?;
		for i in 0..batch.num_rows() {
			let row_index = emitted.len();
			if names.is_null(i) {
				return Err(format!("column c_name row {row_index} is NULL"));
			}
			if custkeys.is_null(i) {
				return Err(format!("column c_custkey row {row_index} is NULL"));
			}
			if orderkeys.is_null(i) {
				return Err(format!("column o_orderkey row {row_index} is NULL"));
			}
			if orderdates.is_null(i) {
				return Err(format!("column o_orderdate row {row_index} is NULL"));
			}
			if totalprices.is_null(i) {
				return Err(format!("column o_totalprice row {row_index} is NULL"));
			}
			if sums.is_null(i) {
				return Err(format!("column sum row {row_index} is NULL"));
			}
			let row = Q18Row {
				c_name: names.value(i).to_string(),
				c_custkey: custkeys.value(i),
				o_orderkey: orderkeys.value(i),
				o_orderdate: orderdates.value(i),
				o_totalprice_raw: totalprices.value(i),
				sum_raw: sums.value(i),
			};
			if emitted_by_key.insert(row.o_orderkey, row.clone()).is_some() {
				return Err(format!("duplicate emitted o_orderkey {}", row.o_orderkey));
			}
			emitted.push(row);
		}
	}

	for pair in emitted.windows(2) {
		if pair[0].o_totalprice_raw < pair[1].o_totalprice_raw
			|| (pair[0].o_totalprice_raw == pair[1].o_totalprice_raw
				&& pair[0].o_orderdate > pair[1].o_orderdate)
		{
			return Err(format!(
				"ORDER BY o_totalprice DESC, o_orderdate violated: {:?} emitted before {:?}",
				pair[0], pair[1]
			));
		}
	}

	let expected = oracle.rows.iter().take(100).collect::<Vec<_>>();
	if emitted.len() != expected.len() {
		return Err(format!(
			"row count mismatch: oracle has {} rows, output has {} rows",
			expected.len(),
			emitted.len()
		));
	}
	for expected_row in expected {
		let Some(actual) = emitted_by_key.get(&expected_row.o_orderkey) else {
			return Err(format!(
				"oracle o_orderkey {} missing from output",
				expected_row.o_orderkey
			));
		};
		if actual != expected_row {
			return Err(format!(
				"o_orderkey {} mismatch: expected {:?}, got {:?}",
				expected_row.o_orderkey, expected_row, actual
			));
		}
	}

	Ok(())
}

pub async fn check_strict_q18(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q18_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q18".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q18_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q18_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q18_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q18(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

#[derive(Debug, Clone)]
pub struct Q19Oracle {
	pub revenue: DecimalRaw,
	pub qualifying_rows: usize,
	pub branch_hits: [usize; 3],
}

fn q19_branch_index(
	brand: &str,
	container: &str,
	size: i32,
	quantity_raw: i128,
	shipmode: &str,
	shipinstruct: &str,
) -> Option<usize> {
	if shipinstruct != "DELIVER IN PERSON" || !matches!(shipmode, "AIR" | "AIR REG") {
		return None;
	}

	let container_matches = |candidates: &[&str]| candidates.contains(&container);
	if brand == "Brand#12"
		&& container_matches(&["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
		&& (100..=1100).contains(&quantity_raw)
		&& (1..=5).contains(&size)
	{
		Some(0)
	} else if brand == "Brand#23"
		&& container_matches(&["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
		&& (1000..=2000).contains(&quantity_raw)
		&& (1..=10).contains(&size)
	{
		Some(1)
	} else if brand == "Brand#34"
		&& container_matches(&["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
		&& (2000..=3000).contains(&quantity_raw)
		&& (1..=15).contains(&size)
	{
		Some(2)
	} else {
		None
	}
}

/// The independent strict-q19 oracle — traverse the generated `PART`/`LINEITEM` rows directly, sum the
/// matching branch contributions, and keep a qualifying-row count so the answer check can reject vacuous
/// `NULL == NULL` passes.
pub fn strict_q19_oracle(scale_factor: f64) -> Result<Q19Oracle, String> {
	let mut parts: HashMap<i64, (String, String, i32)> = HashMap::new();
	for part in PartGenerator::new(scale_factor, 1, 1).iter() {
		parts.insert(
			part.p_partkey,
			(
				part.p_brand.to_string(),
				part.p_container.to_string(),
				part.p_size,
			),
		);
	}

	let mut revenue_raw = 0_i128;
	let mut qualifying_rows = 0_usize;
	let mut branch_hits = [0_usize; 3];
	for item in LineItemGenerator::new(scale_factor, 1, 1).iter() {
		let Some((brand, container, size)) = parts.get(&item.l_partkey) else {
			continue;
		};
		let quantity_raw = item.l_quantity as i128 * 100;
		let Some(branch) = q19_branch_index(
			brand,
			container,
			*size,
			quantity_raw,
			&item.l_shipmode.to_string(),
			&item.l_shipinstruct.to_string(),
		) else {
			continue;
		};
		let extprice_raw = item.l_extendedprice.into_inner() as i128;
		let discount_raw = item.l_discount.into_inner() as i128;
		revenue_raw = revenue_raw
			.checked_add(discounted_price_raw(extprice_raw, discount_raw))
			.ok_or_else(|| "q19 revenue overflow".to_string())?;
		qualifying_rows += 1;
		branch_hits[branch] += 1;
	}

	let revenue = if qualifying_rows == 0 {
		DecimalRaw {
			value: None,
			precision: 32,
			scale: 4,
		}
	} else {
		DecimalRaw {
			value: Some(revenue_raw),
			precision: 32,
			scale: 4,
		}
	};
	Ok(Q19Oracle {
		revenue,
		qualifying_rows,
		branch_hits,
	})
}

fn strict_q19_expected_schema() -> [(&'static str, DataType); 1] {
	[("revenue", DataType::Decimal128(32, 4))]
}

pub fn assert_strict_q19_schema(batches: &[RecordBatch]) -> Result<(), String> {
	let expected = strict_q19_expected_schema();
	let Some(first) = batches.first() else {
		return Err("strict q19 produced no output batch".to_string());
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

pub fn compare_strict_q19(batches: &[RecordBatch], oracle: &Q19Oracle) -> Result<(), String> {
	assert_strict_q19_schema(batches)?;
	if oracle.qualifying_rows == 0 {
		return Err(format!(
			"oracle non-vacuous guard failed: qualifying_rows={} branch_hits={:?}",
			oracle.qualifying_rows, oracle.branch_hits
		));
	}
	if oracle.revenue.precision != 32 || oracle.revenue.scale != 4 {
		return Err(format!(
			"oracle revenue type drift: Decimal128({}, {})",
			oracle.revenue.precision, oracle.revenue.scale
		));
	}
	let Some(expected_value) = oracle.revenue.value else {
		return Err("oracle revenue is NULL".to_string());
	};

	let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
	if row_count != 1 {
		return Err(format!("expected exactly one q19 row, got {row_count}"));
	}
	for batch in batches {
		if batch.num_rows() == 0 {
			continue;
		}
		let column = batch
			.column(0)
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.ok_or("column 0 (revenue) not Decimal128")?;
		if column.is_null(0) {
			return Err("column revenue row 0 is NULL".to_string());
		}
		let actual_value = column.value(0);
		if actual_value != expected_value {
			return Err(format!(
				"revenue mismatch: expected {expected_value}, got {actual_value}"
			));
		}
		return Ok(());
	}
	Err("strict q19 produced no row".to_string())
}

pub async fn check_strict_q19(scale_factor: f64) -> CorrectnessAttempt {
	let start = Instant::now();
	let tables = q19_tables(true, scale_factor);
	let outcome = |phase: Phase, status: Status, reason: Option<String>| CorrectnessAttempt {
		query: "q19".to_string(),
		scale_factor,
		mode: "strict".to_string(),
		phase,
		status,
		reason,
		elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
	};
	let batches = match execute_query_with_tables(STRICT_Q19_SQL, tables).await {
		Ok(batches) => batches,
		Err(StageError { phase, reason, .. }) => {
			return outcome(phase, status_for_reason(&reason), Some(reason));
		}
	};
	if let Err(reason) = assert_strict_q19_schema(&batches) {
		return outcome(
			Phase::Compare,
			Status::Fail,
			Some(format!("schema drift: {reason}")),
		);
	}
	let oracle = match strict_q19_oracle(scale_factor) {
		Ok(oracle) => oracle,
		Err(reason) => return outcome(Phase::Compare, Status::Fail, Some(reason)),
	};
	match compare_strict_q19(&batches, &oracle) {
		Ok(()) => outcome(Phase::Compare, Status::Pass, None),
		Err(reason) => outcome(Phase::Compare, Status::Fail, Some(reason)),
	}
}

/// ADR 310 - dev q16: parts/supplier relationship reachability over part + partsupp + supplier.
pub async fn run_dev_q16(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q16",
		"dev",
		scale_factor,
		Q16_DEV_SQL,
		q16_tables(false, scale_factor),
	)
	.await
}

/// ADR 310 - strict q16: identical SQL shape; strict-only Decimal columns are not projected.
pub async fn run_strict_q16(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q16",
		"strict",
		scale_factor,
		STRICT_Q16_SQL,
		q16_tables(true, scale_factor),
	)
	.await
}

/// ADR 304/305 - dev q22: customer + orders reachability probe for SQL SUBSTRING.
pub async fn run_dev_q22(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q22",
		"dev",
		scale_factor,
		Q22_DEV_SQL,
		q22_tables(false, scale_factor),
	)
	.await
}

/// ADR 304/305 - strict q22: same SQL shape, with strict Decimal128 customer balances.
pub async fn run_strict_q22(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q22",
		"strict",
		scale_factor,
		STRICT_Q22_SQL,
		q22_tables(true, scale_factor),
	)
	.await
}

/// The five q02 harness tables: `part`, `supplier`, `partsupp`, `nation`, and `region`.
fn q02_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let part = generate_part(scale_factor);
	let supplier = generate_supplier(strict, scale_factor);
	let partsupp = generate_partsupp(strict, scale_factor);
	let nation = generate_nation(scale_factor);
	let region = generate_region(scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "part",
			descriptor: part_descriptor(part_schema().as_ref()),
			facts: facts_for_batch(&part),
			batch: part,
		},
		HarnessTable {
			id: TableId(2),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
		HarnessTable {
			id: TableId(3),
			name: "partsupp",
			descriptor: partsupp_descriptor(partsupp_schema(strict).as_ref()),
			facts: facts_for_batch(&partsupp),
			batch: partsupp,
		},
		HarnessTable {
			id: TableId(4),
			name: "nation",
			descriptor: nation_descriptor(nation_schema().as_ref()),
			facts: facts_for_batch(&nation),
			batch: nation,
		},
		HarnessTable {
			id: TableId(5),
			name: "region",
			descriptor: region_descriptor(region_schema().as_ref()),
			facts: facts_for_batch(&region),
			batch: region,
		},
	]
}

/// The two q13 harness tables: `customer` + `orders`.
fn q13_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let customer = generate_customer(strict, scale_factor);
	let orders = generate_orders(strict, scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "customer",
			descriptor: customer_descriptor(customer_schema(strict).as_ref()),
			facts: facts_for_batch(&customer),
			batch: customer,
		},
		HarnessTable {
			id: TableId(2),
			name: "orders",
			descriptor: orders_descriptor(orders_schema(strict).as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
	]
}

/// The three q11 harness tables: `partsupp`, `supplier`, and `nation`.
fn q11_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let partsupp = generate_partsupp(strict, scale_factor);
	let supplier = generate_supplier(strict, scale_factor);
	let nation = generate_nation(scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "partsupp",
			descriptor: partsupp_descriptor(partsupp_schema(strict).as_ref()),
			facts: facts_for_batch(&partsupp),
			batch: partsupp,
		},
		HarnessTable {
			id: TableId(2),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
		HarnessTable {
			id: TableId(3),
			name: "nation",
			descriptor: nation_descriptor(nation_schema().as_ref()),
			facts: facts_for_batch(&nation),
			batch: nation,
		},
	]
}

/// The two q15 harness tables: `supplier` plus mode-sensitive `lineitem`.
fn q15_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let supplier = generate_supplier(strict, scale_factor);
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let lineitem_descriptor = if strict {
		strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
	} else {
		dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
	};
	vec![
		HarnessTable {
			id: TableId(1),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
		HarnessTable {
			id: TableId(2),
			name: "lineitem",
			descriptor: lineitem_descriptor,
			facts: facts_for_batch(&lineitem),
			batch: lineitem,
		},
	]
}

/// The three q16 harness tables: `part`, `partsupp`, and `supplier`.
fn q16_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let part = generate_part(scale_factor);
	let partsupp = generate_partsupp(strict, scale_factor);
	let supplier = generate_supplier(strict, scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "part",
			descriptor: part_descriptor(part_schema().as_ref()),
			facts: facts_for_batch(&part),
			batch: part,
		},
		HarnessTable {
			id: TableId(2),
			name: "partsupp",
			descriptor: partsupp_descriptor(partsupp_schema(strict).as_ref()),
			facts: facts_for_batch(&partsupp),
			batch: partsupp,
		},
		HarnessTable {
			id: TableId(3),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
	]
}

/// The two q22 harness tables: `customer` (SUBSTRING/account-balance source) and `orders` (`NOT EXISTS` target).
fn q22_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let customer = generate_customer(strict, scale_factor);
	let orders = generate_orders(strict, scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "customer",
			descriptor: customer_descriptor(customer_schema(strict).as_ref()),
			facts: facts_for_batch(&customer),
			batch: customer,
		},
		HarnessTable {
			id: TableId(2),
			name: "orders",
			descriptor: orders_descriptor(orders_schema(strict).as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
	]
}

/// The five q20 harness tables: supplier/nation outer query, partsupp/part nested membership, lineitem scalar.
fn q20_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let supplier = generate_supplier(strict, scale_factor);
	let nation = generate_nation(scale_factor);
	let partsupp = generate_partsupp(strict, scale_factor);
	let part = generate_part(scale_factor);
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let lineitem_descriptor = if strict {
		strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
	} else {
		dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
	};
	vec![
		HarnessTable {
			id: TableId(1),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
		HarnessTable {
			id: TableId(2),
			name: "nation",
			descriptor: nation_descriptor(nation_schema().as_ref()),
			facts: facts_for_batch(&nation),
			batch: nation,
		},
		HarnessTable {
			id: TableId(3),
			name: "partsupp",
			descriptor: partsupp_descriptor(partsupp_schema(strict).as_ref()),
			facts: facts_for_batch(&partsupp),
			batch: partsupp,
		},
		HarnessTable {
			id: TableId(4),
			name: "part",
			descriptor: part_descriptor(part_schema().as_ref()),
			facts: facts_for_batch(&part),
			batch: part,
		},
		HarnessTable {
			id: TableId(5),
			name: "lineitem",
			descriptor: lineitem_descriptor,
			facts: facts_for_batch(&lineitem),
			batch: lineitem,
		},
	]
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
/// customer for `c_acctbal`, ADR 274; orders for `o_totalprice`, ADR 322).
fn q03_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let customer = generate_customer(strict, scale_factor);
	let orders = generate_orders(strict, scale_factor);
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
			descriptor: orders_descriptor(orders_schema(strict).as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
	]
}

/// The three q18 harness tables match q03's customer/orders/lineitem family.
fn q18_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	q03_tables(strict, scale_factor)
}

/// The two q19 harness tables match q14/q17's lineitem/part family.
fn q19_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	q14_tables(strict, scale_factor)
}

/// The four q21 harness tables: supplier + mode-sensitive lineitem + orders + nation.
fn q21_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let supplier = generate_supplier(strict, scale_factor);
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let lineitem_descriptor = if strict {
		strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
	} else {
		dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
	};
	let orders = generate_orders(strict, scale_factor);
	let nation = generate_nation(scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
		HarnessTable {
			id: TableId(2),
			name: "lineitem",
			descriptor: lineitem_descriptor,
			facts: facts_for_batch(&lineitem),
			batch: lineitem,
		},
		HarnessTable {
			id: TableId(3),
			name: "orders",
			descriptor: orders_descriptor(orders_schema(strict).as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
		HarnessTable {
			id: TableId(4),
			name: "nation",
			descriptor: nation_descriptor(nation_schema().as_ref()),
			facts: facts_for_batch(&nation),
			batch: nation,
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
	let supplier = generate_supplier(strict, scale_factor);
	tables.push(HarnessTable {
		id: TableId(5),
		name: "supplier",
		descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
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

/// ADR 293 — dev q07: a six-relation logical join with one physical `nation` table scanned twice via SQL
/// aliases (`nation n1`, `nation n2`).
pub async fn run_dev_q07(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q07",
		"dev",
		scale_factor,
		Q7_DEV_SQL,
		q07_tables(false, scale_factor),
	)
	.await
}

/// ADR 293 — strict q07: same shape with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q07(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q07",
		"strict",
		scale_factor,
		STRICT_Q7_SQL,
		q07_tables(true, scale_factor),
	)
	.await
}

/// The five physical q07 harness tables. `nation` is registered once; q07's `n1`/`n2` are SQL aliases.
fn q07_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let mut tables = q05_tables(strict, scale_factor);
	tables.retain(|table| table.name != "region");
	tables
}

/// ADR 297 — dev q08: q07's derived self-join shape plus `part` and `region`, with an outer ratio aggregate.
pub async fn run_dev_q08(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q08",
		"dev",
		scale_factor,
		Q8_DEV_SQL,
		q08_tables(false, scale_factor),
	)
	.await
}

/// ADR 297 — strict q08: same shape with official `Decimal128(15,2)` `lineitem` pricing columns.
pub async fn run_strict_q08(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q08",
		"strict",
		scale_factor,
		STRICT_Q8_SQL,
		q08_tables(true, scale_factor),
	)
	.await
}

/// ADR 301 — dev q09: six-table product-profit reachability query.
pub async fn run_dev_q09(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q09",
		"dev",
		scale_factor,
		Q9_DEV_SQL,
		q09_tables(false, scale_factor),
	)
	.await
}

/// ADR 301 — strict q09: same shape with official Decimal128 lineitem/partsupp numeric columns.
pub async fn run_strict_q09(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q09",
		"strict",
		scale_factor,
		STRICT_Q9_SQL,
		q09_tables(true, scale_factor),
	)
	.await
}

/// The seven physical q08 harness tables. `nation` is registered once; q08's `n1`/`n2` are SQL aliases.
fn q08_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let mut tables = q05_tables(strict, scale_factor);
	let part = generate_part(scale_factor);
	tables.push(HarnessTable {
		id: TableId(7),
		name: "part",
		descriptor: part_descriptor(part_schema().as_ref()),
		facts: facts_for_batch(&part),
		batch: part,
	});
	tables
}

/// The six physical q09 harness tables. Unlike q08, q09 needs `partsupp` and does not use customer/region.
fn q09_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let part = generate_part(scale_factor);
	let supplier = generate_supplier(strict, scale_factor);
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let partsupp = generate_partsupp(strict, scale_factor);
	let orders = generate_orders(strict, scale_factor);
	let nation = generate_nation(scale_factor);
	let lineitem_descriptor = if strict {
		strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
	} else {
		dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
	};
	vec![
		HarnessTable {
			id: TableId(1),
			name: "part",
			descriptor: part_descriptor(part_schema().as_ref()),
			facts: facts_for_batch(&part),
			batch: part,
		},
		HarnessTable {
			id: TableId(2),
			name: "supplier",
			descriptor: supplier_descriptor(supplier_schema(strict).as_ref()),
			facts: facts_for_batch(&supplier),
			batch: supplier,
		},
		HarnessTable {
			id: TableId(3),
			name: "lineitem",
			descriptor: lineitem_descriptor,
			facts: facts_for_batch(&lineitem),
			batch: lineitem,
		},
		HarnessTable {
			id: TableId(4),
			name: "partsupp",
			descriptor: partsupp_descriptor(partsupp_schema(strict).as_ref()),
			facts: facts_for_batch(&partsupp),
			batch: partsupp,
		},
		HarnessTable {
			id: TableId(5),
			name: "orders",
			descriptor: orders_descriptor(orders_schema(strict).as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
		HarnessTable {
			id: TableId(6),
			name: "nation",
			descriptor: nation_descriptor(nation_schema().as_ref()),
			facts: facts_for_batch(&nation),
			batch: nation,
		},
	]
}

/// ADR 278 — dev q04: the first correlated-subquery query (orders + an EXISTS over lineitem). A reachability
/// attempt — records whichever wall q04 hits first (date-interval or the unqualified correlated EXISTS).
pub async fn run_dev_q04(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q04",
		"dev",
		scale_factor,
		Q4_DEV_SQL,
		q04_tables(false, scale_factor),
	)
	.await
}

/// ADR 278 — strict q04: the same shape with official `Decimal128(15,2)` `lineitem` types.
pub async fn run_strict_q04(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q04",
		"strict",
		scale_factor,
		STRICT_Q4_SQL,
		q04_tables(true, scale_factor),
	)
	.await
}

/// ADR 290 — dev q12: two-table shipping-mode reachability query over orders + lineitem.
pub async fn run_dev_q12(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q12",
		"dev",
		scale_factor,
		Q12_DEV_SQL,
		q12_tables(false, scale_factor),
	)
	.await
}

/// ADR 290 — strict q12: same shape; q12 uses no mode-specific Decimal columns.
pub async fn run_strict_q12(scale_factor: f64) -> Attempt {
	run_query_with_tables(
		"q12",
		"strict",
		scale_factor,
		STRICT_Q12_SQL,
		q12_tables(true, scale_factor),
	)
	.await
}

/// The two q04 harness tables: `orders` (the FROM) + the mode-sensitive `lineitem` (the EXISTS subquery's
/// relation). q04 references no customer/nation/etc.
fn q04_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	let lineitem = if strict {
		generate_strict_lineitem(scale_factor)
	} else {
		generate_dev_lineitem(scale_factor)
	};
	let lineitem_descriptor = if strict {
		strict_lineitem_descriptor(strict_lineitem_schema().as_ref())
	} else {
		dev_lineitem_descriptor(dev_lineitem_schema().as_ref())
	};
	let orders = generate_orders(strict, scale_factor);
	vec![
		HarnessTable {
			id: TableId(1),
			name: "lineitem",
			descriptor: lineitem_descriptor,
			facts: facts_for_batch(&lineitem),
			batch: lineitem,
		},
		HarnessTable {
			id: TableId(3),
			name: "orders",
			descriptor: orders_descriptor(orders_schema(strict).as_ref()),
			facts: facts_for_batch(&orders),
			batch: orders,
		},
	]
}

/// The two q12 harness tables: `orders` and mode-sensitive `lineitem`.
fn q12_tables(strict: bool, scale_factor: f64) -> Vec<HarnessTable> {
	q04_tables(strict, scale_factor)
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
					|| *id == "q05" || *id == "q04"
					|| *id == "q17" || *id == "q02"
					|| *id == "q20" || *id == "q12"
					|| *id == "q07" || *id == "q08"
					|| *id == "q09" || *id == "q13"
					|| *id == "q11" || *id == "q15"
					|| *id == "q16" || *id == "q18"
					|| *id == "q19" || *id == "q21"
					|| *id == "q22"
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
mod adr332_unrecognized_diagnostics {
	use super::*;

	#[test]
	fn attempt_jsonl_carries_diagnostic_message_only_when_present() {
		let pass = Attempt {
			query: "q04".to_string(),
			scale_factor: 1.0,
			mode: "strict".to_string(),
			phase: Phase::Execute,
			status: Status::Pass,
			reason: None,
			elapsed_ms: 1.0,
			digest: Some("digest".to_string()),
			diagnostic_message: None,
		};
		let pass_row: serde_json::Value =
			serde_json::from_str(&pass.jsonl()).expect("pass attempt json");
		assert!(
			pass_row.get("diagnostic_message").is_none(),
			"successful rows should not grow a diagnostic field"
		);

		let fail = Attempt {
			query: "q04".to_string(),
			scale_factor: 1.0,
			mode: "strict".to_string(),
			phase: Phase::Execute,
			status: Status::Fail,
			reason: Some("unrecognized".to_string()),
			elapsed_ms: 1.0,
			digest: None,
			diagnostic_message: Some("opaque executor failure".to_string()),
		};
		let fail_row: serde_json::Value =
			serde_json::from_str(&fail.jsonl()).expect("fail attempt json");
		assert_eq!(
			fail_row["diagnostic_message"],
			serde_json::Value::String("opaque executor failure".to_string())
		);
	}

	#[test]
	fn matrix_ignores_diagnostic_message_and_renders_classified_reason() {
		let row = serde_json::json!({
			"suite": "tpch",
			"query": "q04",
			"scale_factor": 1.0,
			"mode": "strict",
			"phase": "execute",
			"status": "fail",
			"reason": "named_execute_bug",
			"elapsed_ms": 1.0,
			"digest": null,
			"diagnostic_message": "raw executor detail"
		});
		let rows = matrix_from_log(&row.to_string());
		assert!(
			rows.iter()
				.any(|line| line == "q04 dev=not_run strict=fail(named_execute_bug@execute)"),
			"matrix must keep rendering the classified reason cell"
		);
	}

	#[test]
	fn stage_error_retains_the_message_it_classifies() {
		let message = "opaque executor failure".to_string();
		let error = StageError::from_message(Phase::Execute, message.clone());
		assert_eq!(error.reason, "unrecognized");
		assert_eq!(error.message, message);
	}

	#[test]
	fn memory_budget_exhausted_is_named_fail_not_blocked() {
		let message = "BudgetExhausted { dimension: Memory, requested: ByteCount(121631060), available: ByteCount(67108864) }";
		assert_eq!(reason_for_error(message), "memory_budget_exhausted");
		assert_eq!(
			status_for_reason("memory_budget_exhausted"),
			Status::Fail,
			"memory budget exhaustion is a scale wall, not an allowlisted blocker"
		);
	}

	#[test]
	fn spill_budget_exhausted_is_named_fail_not_blocked() {
		let message = "BudgetExhausted { dimension: SpillBytes, producer: \"streamed inner join build spill writer\", requested: ByteCount(537631256), available: ByteCount(536870912) }";
		assert_eq!(reason_for_error(message), "spill_budget_exhausted");
		assert_eq!(
			status_for_reason("spill_budget_exhausted"),
			Status::Fail,
			"spill budget exhaustion is a scale wall, not an allowlisted blocker"
		);
	}

	#[test]
	fn budget_classifier_stays_dimension_specific() {
		let message =
			"BudgetExhausted { dimension: Cpu, requested: ByteCount(2), available: ByteCount(1) }";
		assert_eq!(reason_for_error(message), "unrecognized");
	}
}

#[cfg(test)]
mod adr356_perf_rail {
	use super::*;

	#[tokio::test]
	async fn perf_attempt_carries_execute_timing_digest_and_stable_plan_fingerprint() {
		let first = run_perf_query("q06", "strict", 0.01, 0, false)
			.await
			.expect("perf run");
		let second = run_perf_query("q6", "strict", 0.01, 1, false)
			.await
			.expect("perf run");
		assert_eq!(first.status, Status::Pass);
		assert!(first.timings.execute_ms > 0.0);
		assert!(first.timings.total_ms >= first.timings.execute_ms);
		assert_eq!(first.spill_bytes, 0);
		assert!(first.digest.is_some());
		assert!(first.plan_fingerprint.is_some());
		assert_eq!(first.plan_fingerprint, second.plan_fingerprint);
		assert!(first.output_row_count.is_some());
		assert!(first.output_schema_fingerprint.is_some());

		let row = first.json_value();
		assert_eq!(row["engine"], "toron");
		assert_eq!(row["query"], "q06");
		assert!(row["execute_ms"].as_f64().expect("execute_ms") > 0.0);
		assert!(row.get("elapsed_ms").is_none());
		assert_eq!(row["spill_bytes"], 0);
		assert!(row["output_row_count"].as_u64().expect("row count") > 0);
		assert!(
			row["output_schema_fingerprint"]
				.as_str()
				.expect("schema fingerprint")
				.starts_with("fnv1a64:")
		);
	}

	#[test]
	fn parquet_export_manifest_matches_exact_perf_tables() {
		let root =
			std::env::temp_dir().join(format!("toron_tpch_adr358_export_{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		let export = export_perf_query_parquet("q06", "strict", 0.01, &root).expect("export");
		assert_eq!(export.query_id, "q06");
		assert_eq!(export.tables.len(), 1);
		let lineitem = &export.tables[0];
		assert_eq!(lineitem.name, "lineitem");
		assert!(lineitem.row_count > 0);
		assert!(lineitem.path.exists());
		assert!(export.manifest_path.exists());
		assert!(lineitem.schema_signature.contains("l_shipdate:DATE"));
		assert!(
			lineitem
				.schema_signature
				.contains("l_extendedprice:DECIMAL(15,2)")
		);
		assert!(lineitem.file_hash.starts_with("fnv1a64:"));
		assert!(export.manifest_fingerprint.starts_with("fnv1a64:"));
		let _ = std::fs::remove_dir_all(&root);
	}
}

#[cfg(test)]
mod adr422_q11_vacuous_baseline {
	use super::*;

	#[test]
	#[ignore = "ADR 422 baseline executes the old unscaled q11 contract at sf=10"]
	fn unscaled_sf10_sql_and_oracle_are_both_empty_but_instrument_rejected() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let batches = execute_query_with_tables(Q11_DEV_SQL, q11_tables(true, 10.0))
						.await
						.expect("old unscaled q11 executes");
					assert_eq!(output_row_count(&batches), 0);

					let oracle = q11_oracle_from_rows(
						PartSuppGenerator::new(10.0, 1, 1).iter().map(|partsupp| {
							(
								partsupp.ps_partkey,
								partsupp.ps_suppkey,
								partsupp.ps_availqty,
								partsupp.ps_supplycost.into_inner() as i128,
							)
						}),
						SupplierGenerator::new(10.0, 1, 1)
							.iter()
							.map(|supplier| (supplier.s_suppkey, supplier.s_nationkey)),
						NationGenerator::new(10.0, 1, 1)
							.iter()
							.map(|nation| (nation.n_nationkey, nation.n_name.to_string())),
						Q11Scale::try_from_f64(1.0).expect("old unscaled threshold"),
					)
					.expect("old unscaled q11 oracle");
					assert!(oracle.rows.is_empty());
					let error = compare_strict_q11(&batches, &oracle)
						.expect_err("empty-vs-empty baseline must stay instrument-rejected");
					assert!(error.contains("oracle non-vacuous guard failed"));
				});
			})
			.expect("spawn q11 baseline thread")
			.join()
			.expect("q11 baseline thread panicked");
	}
}

#[cfg(test)]
mod adr391_rune_perf_decomposition {
	use super::*;

	struct TempRootCleanup(PathBuf);

	impl Drop for TempRootCleanup {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}

	fn print_q03_proof(label: &str, proof: &RuneQ3IsolationProof) {
		eprintln!(
			"{label} q03 proof data_source={} manifest={:?}",
			proof.data_source, proof.manifest_fingerprint
		);
		eprintln!(
			"{label} fingerprints sql={} rune={}",
			proof.sql_plan_fingerprint, proof.rune_plan_fingerprint
		);
		eprintln!("{label} SQL compact: {}", proof.sql_compact_shape);
		eprintln!("{label} Rune compact: {}", proof.rune_compact_shape);
		eprintln!("{label} SQL decisions: {}", proof.sql_join_decisions);
		eprintln!("{label} Rune decisions: {}", proof.rune_join_decisions);
		eprintln!("{label} SQL edges: {}", proof.sql_join_order_edges);
		eprintln!("{label} Rune edges: {}", proof.rune_join_order_edges);
	}

	#[test]
	fn rune_q03_isolation_proof_tracks_sql_rune_shape_closure() {
		run_on_large_stack(|| async {
			let proof = rune_q03_isolation_proof(0.01)
				.await
				.expect("isolation proof");
			if std::env::var_os("TORON_TPCH_PRINT_Q03_PROOF").is_some() {
				print_q03_proof("memory", &proof);
			}
			assert!(!proof.sql_plan_fingerprint.is_empty());
			assert!(!proof.rune_plan_fingerprint.is_empty());
			assert!(
				proof.sql_compact_shape.contains("Aggregate(groups=3")
					&& !proof
						.sql_compact_shape
						.contains("input=Exchange(parts=4,keys=3,input=Join"),
				"SQL q03 memory proof should not carry the pre-aggregate exchange residual: {}",
				proof.sql_compact_shape
			);
			assert!(
				proof.rune_plan_shape.contains("Join") || proof.rune_plan_shape.contains("join"),
				"Rune q03 proof should expose a join-bearing physical plan"
			);
		});
	}

	#[test]
	fn rune_q03_persisted_isolation_proof_uses_persisted_path() {
		run_on_large_stack(|| async {
			let root = std::env::temp_dir().join(format!(
				"toron_tpch_adr402_q03_persisted_{}",
				std::process::id()
			));
			let _cleanup = TempRootCleanup(root.clone());
			let _ = std::fs::remove_dir_all(&root);
			let proof = rune_q03_isolation_proof_with_data_source("strict", 0.01, "parquet", &root)
				.await
				.expect("persisted isolation proof");
			if std::env::var_os("TORON_TPCH_PRINT_Q03_PROOF").is_some() {
				print_q03_proof("persisted", &proof);
			}
			assert_eq!(proof.data_source, "parquet");
			assert!(proof.manifest_fingerprint.is_some());
			assert!(!proof.sql_plan_fingerprint.is_empty());
			assert!(!proof.rune_plan_fingerprint.is_empty());
			for table_name in ["customer", "orders", "lineitem"] {
				assert!(
					proof.optimizer_table_facts.contains(table_name),
					"persisted q03 proof should expose optimizer facts for {table_name}: {}",
					proof.optimizer_table_facts
				);
			}
			assert!(
				proof.optimizer_table_facts.contains("rows=Some(")
					&& proof.optimizer_table_facts.contains("bytes=Some("),
				"persisted q03 proof should expose row and byte facts: {}",
				proof.optimizer_table_facts
			);
			assert!(
				proof
					.optimizer_evidence_gap
					.contains("sql_join_order_rows_missing=true")
					&& proof
						.optimizer_evidence_gap
						.contains("rune_join_order_rows_missing=true")
					&& proof.optimizer_evidence_gap.contains(
						"join_order_edge_fields=join_index,kind,probe,dim,left_keys,right_keys,rows,linearity"
					) && proof
					.optimizer_evidence_gap
					.contains("missing_evidence=post_filter_cardinality,multi_relation_cost"),
				"persisted q03 proof should compute the remaining optimizer evidence gap: {}",
				proof.optimizer_evidence_gap
			);
			assert!(!proof.sql_join_decisions.is_empty());
			assert!(!proof.rune_join_decisions.is_empty());
			assert!(!proof.sql_join_order_edges.is_empty());
			assert!(!proof.rune_join_order_edges.is_empty());
			assert_eq!(proof.sql_join_decisions, proof.rune_join_decisions);
			for predicate in ["c_mktsegment", "o_orderdate", "l_shipdate"] {
				assert!(
					proof.sql_compact_shape.contains(predicate),
					"SQL compact shape should retain pushed {predicate} filter: {}",
					proof.sql_compact_shape
				);
				assert!(
					proof.rune_compact_shape.contains(predicate),
					"Rune compact shape should retain {predicate} filter: {}",
					proof.rune_compact_shape
				);
			}
			assert_eq!(proof.sql_plan_fingerprint, proof.rune_plan_fingerprint);
			assert_eq!(proof.sql_compact_shape, proof.rune_compact_shape);
			assert!(
				proof.sql_compact_shape.contains("Aggregate(groups=3")
					&& proof.sql_compact_shape.contains("input=Join")
					&& !proof
						.sql_compact_shape
						.contains("input=Exchange(parts=4,keys=3,input=Join"),
				"SQL q03 aggregate should match Rune's no-pre-aggregate-exchange shape: {}",
				proof.sql_compact_shape
			);
		});
	}

	#[test]
	fn rune_q03_perf_row_matches_strict_oracle() {
		run_on_large_stack(|| async {
			let root = std::env::temp_dir()
				.join(format!("toron_tpch_adr391_rune_q03_{}", std::process::id()));
			let _ = std::fs::remove_dir_all(&root);
			let attempt = run_rune_perf_query_with_data_source(
				"q03", "strict", 0.01, 0, false, "memory", &root,
			)
			.await
			.expect("rune q03 perf");
			assert_eq!(attempt.engine, "rune");
			assert_eq!(attempt.query, "q03");
			assert_eq!(attempt.phase, Phase::Compare);
			assert_eq!(attempt.status, Status::Pass);
			assert_eq!(attempt.reason, None);
			assert!(attempt.digest.is_some());
			let _ = std::fs::remove_dir_all(&root);
		});
	}

	fn run_on_large_stack<F, Fut>(body: F)
	where
		F: FnOnce() -> Fut + Send + 'static,
		Fut: std::future::Future<Output = ()> + 'static,
	{
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(move || {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(body());
			})
			.expect("spawn rune q03 probe thread")
			.join()
			.expect("rune q03 probe thread panicked");
	}
}

#[cfg(test)]
mod adr360_persisted_data_surface {
	use super::*;

	fn temp_root(label: &str) -> PathBuf {
		let root =
			std::env::temp_dir().join(format!("toron_tpch_adr360_{label}_{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		root
	}

	#[test]
	fn persisted_manifest_is_shared_and_idempotent() {
		let root = temp_root("manifest");
		let first =
			prepare_persisted_tpch_dataset("strict", 0.01, &root).expect("prepare persisted");
		assert_eq!(first.tables.len(), 8);
		assert!(first.manifest_path.exists());
		assert!(first.tables["lineitem"].path.exists());
		assert_eq!(
			first.tables["lineitem"].path.parent(),
			Some(first.root_dir.as_path()),
			"persisted tables live under the shared dataset root, not a query-shaped export"
		);

		let second =
			prepare_persisted_tpch_dataset("strict", 0.01, &root).expect("reload persisted");
		assert_eq!(first.manifest_fingerprint, second.manifest_fingerprint);
		assert_eq!(
			first.tables["lineitem"].file_hash,
			second.tables["lineitem"].file_hash
		);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[tokio::test]
	async fn persisted_q06_matches_in_memory_digest_at_default_scale() {
		let root = temp_root("q06_parity");
		let memory =
			run_perf_query_with_data_source("q06", "strict", 0.01, 0, false, "memory", &root)
				.await
				.expect("memory perf");
		let persisted =
			run_perf_query_with_data_source("q06", "strict", 0.01, 0, false, "parquet", &root)
				.await
				.expect("persisted perf");
		assert_eq!(memory.status, Status::Pass);
		assert_eq!(persisted.status, Status::Pass);
		assert_eq!(memory.digest, persisted.digest);
		assert_eq!(
			memory.output_schema_fingerprint,
			persisted.output_schema_fingerprint
		);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn persisted_duckdb_export_uses_shared_dataset_manifest() {
		let root = temp_root("duckdb_export");
		let export =
			export_persisted_perf_query_parquet("q14", "strict", 0.01, &root).expect("export");
		assert_eq!(export.query_id, "q14");
		assert_eq!(export.tables.len(), 2);
		assert!(
			export
				.tables
				.iter()
				.all(|table| table.path.parent() == Some(export.root_dir.as_path()))
		);
		assert_eq!(export.manifest_path, export.root_dir.join("manifest.json"));
		assert!(export.manifest_fingerprint.starts_with("fnv1a64:"));
		let _ = std::fs::remove_dir_all(&root);
	}
}

#[cfg(test)]
mod adr365_persisted_correctness_surface {
	use super::*;

	fn temp_root(label: &str) -> PathBuf {
		let root =
			std::env::temp_dir().join(format!("toron_tpch_adr365_{label}_{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		root
	}

	#[tokio::test]
	async fn persisted_correctness_q06_matches_memory_at_default_scale() {
		let root = temp_root("q06_correctness");
		let memory = check_strict_query_with_data_source("q06", 0.01, "memory", &root)
			.await
			.expect("memory correctness");
		let persisted = check_strict_query_with_data_source("q6", 0.01, "parquet", &root)
			.await
			.expect("persisted correctness");
		assert_eq!(memory.status, Status::Pass);
		assert_eq!(persisted.status, Status::Pass);
		assert_eq!(persisted.query, "q06");
		assert_eq!(persisted.phase, Phase::Compare);
		assert_eq!(persisted.reason, None);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[tokio::test]
	async fn persisted_correctness_rejects_unknown_data_source_as_usage() {
		let root = temp_root("bad_source");
		let error = check_strict_query_with_data_source("q06", 0.01, "csv", &root)
			.await
			.expect_err("unsupported source should be a usage error");
		assert!(error.contains("correctness --data-source"));
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn q17_correctness_scale_policy_is_pure() {
		assert_eq!(q17_correctness_scale(None).unwrap(), 0.02);
		assert_eq!(q17_correctness_scale(Some(0.02)).unwrap(), 0.02);
		assert_eq!(q17_correctness_scale(Some(10.0)).unwrap(), 10.0);
		assert!(q17_correctness_scale(Some(0.01)).is_err());
		assert!(q17_correctness_scale(Some(1.0)).is_err());
	}
}

#[cfg(test)]
mod adr431_q17_boundary_census {
	use super::*;

	fn common_fixture() -> Q17CensusCommon {
		Q17CensusCommon {
			part_aggregates: vec![Q17PartAggregate {
				partkey: 7,
				quantity_sum_raw: 50_001,
				quantity_count: 20_001,
				quantity_avg_raw: 25_003,
			}],
			selected_rows: vec![Q17SelectedRow {
				partkey: 7,
				orderkey: 11,
				suppkey: 13,
				quantity_raw: 100,
				extendedprice_raw: 17_00,
			}],
		}
	}

	#[test]
	fn exact_common_comparison_is_the_gate_not_the_fingerprint() {
		let expected = common_fixture();
		assert_eq!(compare_q17_census_common(&expected, &expected), Ok(()));

		let mut changed_aggregate = expected.clone();
		changed_aggregate.part_aggregates[0].quantity_sum_raw += 1;
		assert!(compare_q17_census_common(&changed_aggregate, &expected).is_err());

		let mut changed_row = expected.clone();
		changed_row.selected_rows[0].extendedprice_raw += 1;
		assert!(compare_q17_census_common(&changed_row, &expected).is_err());
		assert_ne!(
			q17_census_fingerprints(&changed_row).selected_rows,
			q17_census_fingerprints(&expected).selected_rows
		);
	}

	#[test]
	fn synthetic_rational_vs_rounded_membership_difference_is_counted() {
		let quantity_raw = 100_i128;
		let count = 20_001_i64;
		let quantity_sum = 5 * quantity_raw * i128::from(count) + 1;
		let avg_raw = avg_decimal_raw(quantity_sum, count, 2, 6, 19).unwrap();
		let rational = (5 * quantity_raw * i128::from(count)).cmp(&quantity_sum);
		let rounded = (quantity_raw * 100_000).cmp(&(2 * avg_raw));

		let mut receipt = Q17PartBoundaryReceipt {
			partkey: 7,
			rational: Q17BoundaryCounts::default(),
			rounded: Q17BoundaryCounts::default(),
			membership_difference_rows: 0,
		};
		assert!(receipt.record(rational, rounded));
		assert_eq!(receipt.rational.below, 1);
		assert_eq!(receipt.rounded.equal, 1);
		assert_eq!(receipt.membership_difference_rows, 1);
	}

	#[test]
	fn eligible_part_filter_chunks_at_the_frontend_in_list_limit() {
		let partkeys: Vec<i64> = (1..=65).collect();
		let aggregate_sql = strict_q17_census_agg_sql(&partkeys).unwrap();
		assert_eq!(aggregate_sql.matches(" IN (").count(), 3);
		SqlPlanner::new()
			.plan(&aggregate_sql)
			.expect("chunked q17 census predicate stays inside the frontend limit");
	}
}

#[cfg(test)]
mod adr301_q09_probe {
	use super::*;

	#[test]
	fn dev_q09_is_non_empty_at_default_scale() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					let batches = execute_query_with_tables(Q9_DEV_SQL, q09_tables(false, 0.01))
						.await
						.expect("dev q09 reaches execute at the default scale");
					let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
					assert!(rows > 0, "dev q09 has rows at sf=0.01");
				});
			})
			.expect("spawn q09 probe thread")
			.join()
			.expect("q09 probe thread panicked");
	}

	#[test]
	fn strict_q09_is_non_empty_at_default_scale() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					let batches = execute_query_with_tables(STRICT_Q9_SQL, q09_tables(true, 0.01))
						.await
						.expect("strict q09 reaches execute at the default scale");
					let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
					assert!(rows > 0, "strict q09 has rows at sf=0.01");
				});
			})
			.expect("spawn q09 probe thread")
			.join()
			.expect("q09 probe thread panicked");
	}
}

#[cfg(test)]
mod adr331_q08_scale_probe {
	use super::*;
	use toron::binder::bind_with_explain_summary;

	fn catalog_from_tables(tables: Vec<HarnessTable>) -> MockCatalog {
		let mut catalog = MockCatalog::new();
		for table in tables {
			catalog
				.register_descriptor(table.id, table.descriptor)
				.expect("register q08 table");
			catalog
				.seed_optimizer_facts(&TableRef::new(table.name), table.facts)
				.expect("seed q08 facts");
		}
		catalog
	}

	fn join_kinds(plan: &ExecutionPlan, kinds: &mut Vec<JoinKind>) {
		match plan {
			ExecutionPlan::Join {
				left, right, kind, ..
			} => {
				kinds.push(*kind);
				join_kinds(left, kinds);
				join_kinds(right, kinds);
			}
			ExecutionPlan::Filter { input, .. }
			| ExecutionPlan::Project { input, .. }
			| ExecutionPlan::Aggregate { input, .. }
			| ExecutionPlan::Exchange { input, .. }
			| ExecutionPlan::Sort { input, .. }
			| ExecutionPlan::Window { input, .. }
			| ExecutionPlan::Take { input, .. }
			| ExecutionPlan::Repeat { input, .. } => join_kinds(input, kinds),
			ExecutionPlan::Union { left, right, .. } => {
				join_kinds(left, kinds);
				join_kinds(right, kinds);
			}
			ExecutionPlan::EmptyRelation { .. }
			| ExecutionPlan::SystemTable { .. }
			| ExecutionPlan::Scan { .. }
			| ExecutionPlan::CreateTable { .. }
			| ExecutionPlan::CreateLogicalType { .. }
			| ExecutionPlan::CreateTableAsSelect { .. }
			| ExecutionPlan::DropTable { .. }
			| ExecutionPlan::Delete { .. }
			| ExecutionPlan::Update { .. }
			| ExecutionPlan::Insert { .. }
			| ExecutionPlan::InsertSelect { .. }
			| ExecutionPlan::RecursiveCte { .. }
			| ExecutionPlan::RecursiveRef { .. } => {}
		}
	}

	fn plan_shape(plan: &ExecutionPlan) -> String {
		match plan {
			ExecutionPlan::Join {
				left,
				right,
				kind,
				strategy,
				replay_id,
				left_keys,
				right_keys,
				..
			} => format!(
				"Join(id={replay_id:?},{kind:?},{strategy:?},keys={left_keys:?}/{right_keys:?},left={},right={})",
				plan_shape(left),
				plan_shape(right)
			),
			ExecutionPlan::Filter { input, .. } => format!("Filter({})", plan_shape(input)),
			ExecutionPlan::Project { input, .. } => format!("Project({})", plan_shape(input)),
			ExecutionPlan::Aggregate { input, .. } => format!("Agg({})", plan_shape(input)),
			ExecutionPlan::Exchange { input, .. } => format!("Exchange({})", plan_shape(input)),
			ExecutionPlan::Sort { input, .. } => format!("Sort({})", plan_shape(input)),
			ExecutionPlan::Window { input, .. } => format!("Window({})", plan_shape(input)),
			ExecutionPlan::Take { input, .. } => format!("Take({})", plan_shape(input)),
			ExecutionPlan::Repeat { input, .. } => format!("Repeat({})", plan_shape(input)),
			ExecutionPlan::Union { left, right, .. } => {
				format!(
					"Union(left={},right={})",
					plan_shape(left),
					plan_shape(right)
				)
			}
			ExecutionPlan::Scan { local_table_id, .. } => format!("Scan({local_table_id:?})"),
			ExecutionPlan::EmptyRelation { .. } => "EmptyRelation".to_string(),
			ExecutionPlan::SystemTable { .. } => "SystemTable".to_string(),
			ExecutionPlan::CreateTable { .. } => "CreateTable".to_string(),
			ExecutionPlan::CreateLogicalType { .. } => "CreateLogicalType".to_string(),
			ExecutionPlan::CreateTableAsSelect { source, .. } => {
				format!("CTAS({})", plan_shape(source))
			}
			ExecutionPlan::DropTable { .. } => "DropTable".to_string(),
			ExecutionPlan::Delete { .. } => "Delete".to_string(),
			ExecutionPlan::Update { .. } => "Update".to_string(),
			ExecutionPlan::Insert { .. } => "Insert".to_string(),
			ExecutionPlan::InsertSelect { source, .. } => {
				format!("InsertSelect({})", plan_shape(source))
			}
			ExecutionPlan::RecursiveCte { .. } => "RecursiveCte".to_string(),
			ExecutionPlan::RecursiveRef { .. } => "RecursiveRef".to_string(),
		}
	}

	fn assert_no_cross_join(query: &str, scale: &str, plan: &ExecutionPlan) {
		let mut kinds = Vec::new();
		join_kinds(plan, &mut kinds);
		assert!(
			!kinds.contains(&JoinKind::Cross),
			"{query} {scale} should stay keyed, got: {}",
			plan_shape(plan)
		);
	}

	async fn assert_explain_stays_keyed(
		query: &str,
		scale: &str,
		logical: &toron::ir::LogicalPlan,
		catalog: &MockCatalog,
		config: &PlannerConfig,
	) {
		let (_plan, _decisions, edges) = bind_with_explain_summary(logical, catalog, config)
			.await
			.unwrap_or_else(|err| panic!("{query} {scale} explain bind should succeed: {err}"));
		assert!(
			!edges.contains(" Cross"),
			"{query} {scale} explain path should stay keyed, got edges: {edges}"
		);
	}

	async fn assert_strict_query_stays_keyed(
		query: &str,
		sql: &str,
		small_catalog: MockCatalog,
		scaled_catalog: MockCatalog,
	) {
		let logical = SqlPlanner::new()
			.plan(sql)
			.unwrap_or_else(|err| panic!("plan strict {query}: {err}"));
		let default_cap = PlannerConfig::default();

		assert_strict_query_stays_keyed_at_scale(
			query,
			"sf=0.01",
			&logical,
			&small_catalog,
			&default_cap,
		)
		.await;
		assert_strict_query_stays_keyed_at_scale(
			query,
			"sf=1",
			&logical,
			&scaled_catalog,
			&default_cap,
		)
		.await;
	}

	async fn assert_strict_query_stays_keyed_at_scale(
		query: &str,
		scale: &str,
		logical: &toron::ir::LogicalPlan,
		catalog: &MockCatalog,
		config: &PlannerConfig,
	) {
		let plan = match bind(logical, catalog, config).await {
			Ok(plan) => plan,
			Err(err) => {
				let relaxed_cap = PlannerConfig {
					auto_broadcast_max_build_bytes: Some(ByteCount::new(u64::MAX / 4)),
					..PlannerConfig::default()
				};
				let relaxed = match bind_with_explain_summary(logical, catalog, &relaxed_cap).await
				{
					Ok((_plan, decisions, edges)) => {
						format!("relaxed decisions=[{decisions}] edges=[{edges}]")
					}
					Err(relaxed_err) => format!("relaxed explain failed: {relaxed_err}"),
				};
				panic!("bind strict {query} {scale}: {err}; {relaxed}");
			}
		};
		assert_no_cross_join(query, scale, &plan);
		assert_explain_stays_keyed(query, scale, logical, catalog, config).await;
	}

	#[test]
	fn strict_q08_stays_keyed_at_small_and_scaled() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					assert_strict_query_stays_keyed(
						"q08",
						STRICT_Q8_SQL,
						catalog_from_tables(q08_tables(true, 0.01)),
						catalog_from_tables(q08_tables(true, 1.0)),
					)
					.await;
				});
			})
			.expect("spawn q08 thread")
			.join()
			.expect("q08 thread panicked");
	}

	#[test]
	fn strict_q09_stays_keyed_at_small_and_scaled() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					assert_strict_query_stays_keyed(
						"q09",
						STRICT_Q9_SQL,
						catalog_from_tables(q09_tables(true, 0.01)),
						catalog_from_tables(q09_tables(true, 1.0)),
					)
					.await;
				});
			})
			.expect("spawn q09 thread")
			.join()
			.expect("q09 thread panicked");
	}

	// ADR 331: q02 is a free beneficiary of the same comma-Cross reassociation - its `(A x B) join F`
	// shape keyed at sf=1 once q08/q09 did. This canary guards that: revert the reassociation and q02
	// regresses to a keyless Cross at sf=1.
	#[test]
	fn strict_q02_stays_keyed_at_small_and_scaled() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					assert_strict_query_stays_keyed(
						"q02",
						STRICT_Q2_SQL,
						catalog_from_tables(q02_tables(true, 0.01)),
						catalog_from_tables(q02_tables(true, 1.0)),
					)
					.await;
				});
			})
			.expect("spawn q02 thread")
			.join()
			.expect("q02 thread panicked");
	}

	#[test]
	fn strict_q05_stays_keyed_at_default_scale() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					let logical = SqlPlanner::new()
						.plan(STRICT_Q5_SQL)
						.expect("plan strict q05");
					let default_cap = PlannerConfig::default();
					let catalog = catalog_from_tables(q05_tables(true, 0.01));
					assert_strict_query_stays_keyed_at_scale(
						"q05",
						"sf=0.01",
						&logical,
						&catalog,
						&default_cap,
					)
					.await;
				});
			})
			.expect("spawn q05 thread")
			.join()
			.expect("q05 thread panicked");
	}

	#[test]
	fn strict_q10_stays_keyed_at_default_scale() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("tokio runtime");
				runtime.block_on(async {
					let logical = SqlPlanner::new()
						.plan(STRICT_Q10_SQL)
						.expect("plan strict q10");
					let default_cap = PlannerConfig::default();
					let catalog = catalog_from_tables(q10_tables(true, 0.01));
					assert_strict_query_stays_keyed_at_scale(
						"q10",
						"sf=0.01",
						&logical,
						&catalog,
						&default_cap,
					)
					.await;
				});
			})
			.expect("spawn q10 thread")
			.join()
			.expect("q10 thread panicked");
	}
}

#[cfg(test)]
mod parquet_scan_projection_tests {
	use super::*;
	use wendelon::schema::{
		RUNTIME_FILTER_HASH_SEED, RUNTIME_FILTER_KEY_ENCODING_VERSION, RuntimeFilterBloom,
		RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind,
		encode_runtime_filter_key, insert_runtime_filter_encoded_key,
	};

	#[test]
	fn physical_plan_fingerprint_shape_ignores_scan_size_routing_hint() {
		let with_none = "Scan { local_table_id: TableId(1), profile_hint: Scan, estimated_byte_size: None, primary_key_columns: [] }";
		let with_bytes = "Scan { local_table_id: TableId(1), profile_hint: Scan, estimated_byte_size: Some(8520711296), primary_key_columns: [] }";
		let expected =
			"Scan { local_table_id: TableId(1), profile_hint: Scan, primary_key_columns: [] }";

		assert_eq!(strip_plan_fingerprint_routing_hints(with_none), expected);
		assert_eq!(strip_plan_fingerprint_routing_hints(with_bytes), expected);
	}

	#[test]
	fn projected_pbc_inflation_allows_omitted_non_nullable_columns() {
		let full_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, false),
		]));
		let projected = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
		)
		.expect("projected batch");

		let inflated = inflate_projected_parquet_batch(projected, full_schema, &[0])
			.expect("inflate projected batch");

		assert_eq!(inflated.num_columns(), 2);
		assert!(!inflated.schema().field(0).is_nullable());
		assert!(inflated.schema().field(1).is_nullable());
		assert_eq!(inflated.column(1).null_count(), 2);
	}

	#[test]
	fn adr446_harness_filter_uses_source_ordinal_before_projection() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("sibling", DataType::Int64, false),
			Field::new("join_key", DataType::Int64, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
				Arc::new(Int64Array::from(vec![7_i64, 8, 7])),
			],
		)
		.expect("source batch");
		let bit_count = 64;
		let hash_count = 3;
		let mut payload = vec![0_u8; 8];
		let key = encode_runtime_filter_key(batch.column(1), 0)
			.expect("encode")
			.expect("non-null");
		insert_runtime_filter_encoded_key(
			&mut payload,
			bit_count,
			hash_count,
			RUNTIME_FILTER_HASH_SEED,
			&key,
		);
		let descriptor = RuntimeFilterDescriptor {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![1],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: RUNTIME_FILTER_HASH_SEED,
				bit_count,
				hash_count,
				byte_budget: 8,
				target_fp_rate_bp: 100,
				payload,
			}),
			probe_descriptor_fingerprint: postcard::from_bytes(&[0_u8; 32]).expect("fingerprint"),
		};
		let filtered = apply_harness_runtime_filter(batch, &descriptor).expect("filter");
		let projected = filtered.project(&[0]).expect("project after filter");
		let values = projected
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("int64 projection");
		assert_eq!(values.values(), &[10, 30]);
	}
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
			Err(StageError { phase, reason, .. }) => {
				panic!("explicit Decimal equi-join walled at {phase:?}: {reason}");
			}
		}
	}
}

#[cfg(test)]
mod adr471_probe {
	use super::*;
	use std::time::Instant;

	#[tokio::test]
	async fn adr472_q19_binds_natural_with_flag_on() {
		// ADR 472's residual fixture: q19's filters (Decimal quantity, string
		// categoricals) are unestimated, so the unknown-size guard must decline
		// the swap that priced -75% in the ADR 471 table.
		assert_natural_with_flag_on("q19").await;
	}

	#[tokio::test]
	async fn adr471_q14_binds_natural_with_flag_on() {
		assert_natural_with_flag_on("q14").await;
	}

	async fn assert_natural_with_flag_on(query: &str) {
		let spec = build_persisted_perf_query_spec(
			query,
			"strict",
			1.0,
			std::path::Path::new("target/tpch/data"),
		)
		.expect("persisted spec");
		let mut timings = PerfPhaseTimings::default();
		let catalog =
			load_catalog_from_metadata(spec.tables, &mut timings, Instant::now()).expect("catalog");
		let logical = SqlPlanner::new().plan(&spec.sql).expect("plan query");
		let mut config = gauge_planner_config();
		config.hash_smaller_build_placement = true;
		let (_plan, joins, _edges) = bind_with_explain_summary(&logical, &catalog, &config)
			.await
			.expect("bind query");
		assert!(
			joins.contains("natural"),
			"{query} flag-on must bind natural: {joins}"
		);
		assert!(!joins.contains("swapped_right_build"), "{joins}");
	}
}
