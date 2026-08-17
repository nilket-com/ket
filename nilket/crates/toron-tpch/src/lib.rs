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
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
use toron::catalog::{Catalog, MockCatalog, TableOptimizerFacts, TableRef, UniqueKeyProof};
use toron::coordinator::DistributedQueryCoordinator;
use toron::distributed::budget::{BudgetDimension, ByteCount, QueryBudget, TaskBudget};
use toron::distributed::cancellation::CancellationTrigger;
use toron::distributed::context::TaskExecutionContext;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{LeaseEpoch, QueryId, WorkerId};
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::scheduler::{StagePlan, live_publication_report};
use toron::distributed::task::{
	JoinBuildProfile, JoinSpillObservation, PartitionWorkConcurrencyObservation, RowCount,
	TaskOutputBinding, TaskPhaseReceipt, TaskTelemetry, TaskTelemetryPhases,
};
use toron::distributed::worker_registry::{
	ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
	WorkerEndpoint, WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
};
use toron::error::{ExecutionError, QueryError, RuntimeError, StorageError, TaskFailureKind};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	BoundExprNode, BoundExprProgram, ColumnOrdinal, ExecutionPlan, JoinKind, LogicalPlan, TableId,
	decimal_division_output_type,
};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::server::{ToronWorker, memory::MemoryManager, transport::TcpTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	RuntimeFilterDescriptor, SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	build_runtime_filter_mask_for_batch, pbc::Program,
};

use telemetry::{
	Attempt, CorrectnessAttempt, DatasetVerification, ExecutionCensusBackpressureReceipt,
	ExecutionCensusRow, FragmentCensusRow, PerfAttempt, PerfPhaseTimings, Phase, Status,
	reason_for_error, status_for_reason,
};

/// Default cache / output root — always under `target/`, never a tracked path.
pub const CACHE_ROOT: &str = "target/tpch";

const TPCH_PERSISTED_CHUNK_ROWS: usize = 65_536;
const TPCH_PERSISTED_MANIFEST_FORMAT: u64 = 2;
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
			bits if bits == 8.0_f64.to_bits() => Self {
				hundredths_n: 800,
				fraction_sql: "0.0000125",
			},
			bits if bits == 10.0_f64.to_bits() => Self {
				hundredths_n: 1_000,
				fraction_sql: "0.00001",
			},
			_ => {
				return Err(format!(
					"q11 requires an exactly supported scale (0.01, 1, 8, or 10); got {scale_factor:?}"
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

/// Whether a query's fixture is defined at this scale. q11's HAVING threshold is a scale-specific
/// constant, so it is only defined at the scales that have one. Public so the perf sweep can record
/// an out-of-scope cell as `not_attempted` instead of discovering it as a fatal child error.
pub fn validate_query_scale(query: &str, scale_factor: f64) -> Result<(), String> {
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
			toron::storage::ScanOrigin::BaseRelation,
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
		// ADR 501: Parquet-optimal proxy for production's large-batch contract. The
		// environment override remains available for fidelity sweeps.
		let batch_size = std::env::var("TORON_TPCH_PARQUET_BATCH_SIZE")
			.ok()
			.and_then(|raw| raw.parse::<usize>().ok())
			.filter(|size| *size > 0)
			.unwrap_or(8_192);
		builder = builder.with_batch_size(batch_size);
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
				toron::storage::ScanOrigin::BaseRelation,
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
			toron::storage::ScanOrigin::BaseRelation,
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
	/// Set when this export *is* the shared persisted dataset, so a peer-engine row reading these
	/// files reports the same verification the toron row does. `None` for the in-memory export,
	/// which writes throwaway parquet from fixtures rather than reading a persisted dataset.
	pub dataset_verification: Option<DatasetVerification>,
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
	pub unique_key_evidence: Vec<UniqueKeyProof>,
}

#[derive(Debug, Clone)]
pub struct TpchPersistedDataset {
	pub mode: String,
	pub scale_factor: f64,
	pub root_dir: PathBuf,
	pub manifest_path: PathBuf,
	pub manifest_fingerprint: String,
	pub tables: BTreeMap<String, TpchPersistedTable>,
	/// What this load actually proved about the files on disk. Travels with the dataset so every
	/// consumer — perf row, correctness gate, ablation gate — reads the same fact rather than
	/// re-deriving it from the environment at a later, possibly different, point.
	pub verification: DatasetVerification,
}

/// A query table registration decoupled from the in-memory fixture batch. Persisted scans seed the same
/// catalog descriptors/facts as [`HarnessTable`] without constructing a full `RecordBatch`.
#[derive(Debug, Clone)]
pub struct HarnessTableMetadata {
	pub id: TableId,
	pub name: &'static str,
	pub descriptor: TableSchemaDescriptor,
	pub facts: TableOptimizerFacts,
	pub unique_key_evidence: Vec<UniqueKeyProof>,
}

pub struct PersistedPerfQuerySpec {
	pub query_id: &'static str,
	pub sql: Cow<'static, str>,
	pub tables: Vec<HarnessTableMetadata>,
	pub storage: Arc<ParquetHarnessScan>,
	pub export: TpchParquetExport,
	/// Carried from the dataset this spec was built against, so the row it produces reports the
	/// verification that actually happened rather than re-reading the environment after the fact.
	pub dataset_verification: DatasetVerification,
}

/// ADR 507 step 4/5: the reader-side outer time reconciliation.
///
/// The node-attribution profiler measures the `execute_bound` root span and reconciles node stages to
/// it in-tree (a near-zero, telescoping residual). The outer band — root span ⊆ the query's
/// `execute_ms` — needs a number the profiler cannot see: `execute_ms` is a harness measurement. This
/// is where profile meets perf row.
///
/// The boundary is declared here, before any run. `execute_ms` starts immediately before
/// `execute_bound` and stops immediately after, so this residual is *not* binding — binding completes
/// before the timer starts. It is executor construction (building and entering the boxed future
/// before the root node's span opens) plus timer-edge framework at both ends: fixed, sub-millisecond,
/// and independent of cell size. A residual over 10 ms therefore means the node tree failed to
/// account for the query, not tolerable overhead.
pub const NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS: u64 = 10_000;

/// Reconcile a node profile against its row's `execute_ms`. **Fails closed**: any input it cannot
/// trust — an absent span, a non-finite or negative `execute_ms`, or an inverted band where the span
/// exceeds `execute_ms` — reports `within_boundary: false` with a reason. A reconciliation that cannot
/// be computed must never read as a pass, and an inverted band is a broken measurement rather than a
/// zero residual, so the subtraction is checked, not saturating.
pub fn node_attribution_outer_reconciliation(
	profile: &serde_json::Value,
	execute_ms: f64,
) -> serde_json::Value {
	let Some(span_micros) = profile
		.get("execute_bound_span_micros")
		.and_then(serde_json::Value::as_u64)
	else {
		return serde_json::json!({
			"error": "profile is missing a numeric execute_bound_span_micros",
			"boundary_micros": NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS,
			"within_boundary": false,
		});
	};
	if !execute_ms.is_finite() || execute_ms < 0.0 {
		return serde_json::json!({
			"error": format!("row execute_ms is not a finite non-negative value: {execute_ms}"),
			"execute_bound_span_micros": span_micros,
			"boundary_micros": NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS,
			"within_boundary": false,
		});
	}
	// execute_ms is milliseconds with sub-microsecond noise; round to whole micros for the residual.
	let execute_micros = (execute_ms * 1000.0).round() as u64;
	let Some(outer_residual_micros) = execute_micros.checked_sub(span_micros) else {
		return serde_json::json!({
			"error": "inverted band: execute_bound span exceeds the row's execute_ms",
			"execute_bound_span_micros": span_micros,
			"execute_micros": execute_micros,
			"boundary_micros": NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS,
			"within_boundary": false,
		});
	};
	serde_json::json!({
		"execute_bound_span_micros": span_micros,
		"execute_micros": execute_micros,
		"outer_residual_micros": outer_residual_micros,
		"boundary_micros": NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS,
		"within_boundary": outer_residual_micros <= NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS,
	})
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

const SKIP_DATASET_HASH_VERIFY_ENV: &str = "TORON_TPCH_SKIP_DATASET_HASH_VERIFY";

/// Opt-in fast path for measurement sessions. `stable_file_hash` re-reads every (multi-GB) parquet
/// file in full on each dataset load to verify it against the manifest — ~41% of the load window on
/// a repeated gauge run. When this env is set, the manifest hash check is skipped (the cheap
/// path-exists check is kept). Default off, and the mode lands on every perf row it produced, so a
/// weakened load can never be read back as a full one. Set it only when the dataset is known-stable
/// within a measurement session.
///
/// Split from the env read so the mode and its gates are testable without mutating process env.
fn dataset_verification_from_env(value: Option<&OsStr>) -> DatasetVerification {
	match value {
		Some(_) => DatasetVerification::ManifestOnly,
		None => DatasetVerification::FullHash,
	}
}

fn dataset_verification_mode() -> DatasetVerification {
	dataset_verification_from_env(std::env::var_os(SKIP_DATASET_HASH_VERIFY_ENV).as_deref())
}

/// The shared refusal text for a rail that may not run manifest-only. Named once so the rails cannot
/// drift into describing the same weakened state differently, and so each names the env var to unset.
fn manifest_only_dataset_rejection(rail: &str) -> String {
	format!(
		"{rail} requires full dataset hash verification; unset {SKIP_DATASET_HASH_VERIFY_ENV} \
		 (it is a measurement-only fast path and leaves the dataset manifest-only)"
	)
}

/// Rails that make a pass/fail claim about *answers* — correctness, and the paired-ablation gate that
/// admits a measurement pair — may not run against a dataset whose bytes were never checked. They
/// refuse rather than downgrade: a green from an unverified dataset is exactly the lying green the
/// harness exists to prevent.
fn require_full_dataset_verification_from_env(
	value: Option<&OsStr>,
	rail: &str,
) -> Result<(), String> {
	match dataset_verification_from_env(value) {
		DatasetVerification::FullHash => Ok(()),
		DatasetVerification::ManifestOnly => Err(manifest_only_dataset_rejection(rail)),
	}
}

fn require_full_dataset_verification(rail: &str) -> Result<(), String> {
	require_full_dataset_verification_from_env(
		std::env::var_os(SKIP_DATASET_HASH_VERIFY_ENV).as_deref(),
		rail,
	)
}

/// The paired-ablation admission gate: yield the fingerprint both arms will be held to, but only if
/// the dataset it names was proved byte-for-byte. Kept a function over the *loaded dataset* — rather
/// than an env check at the call site — so the gate tests the fact that was established, and so a
/// future non-env source of manifest-only loads is caught by the same rail.
pub fn paired_ablation_dataset_fingerprint(
	dataset: &TpchPersistedDataset,
) -> Result<String, String> {
	match dataset.verification {
		DatasetVerification::FullHash => Ok(dataset.manifest_fingerprint.clone()),
		DatasetVerification::ManifestOnly => {
			Err(manifest_only_dataset_rejection("paired ablation"))
		}
	}
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
		unique_key_evidence: table.unique_key_evidence.clone(),
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
			let unique_keys = table
				.unique_key_evidence
				.iter()
				.map(|proof| {
					proof
						.columns()
						.iter()
						.map(|column| column.0.to_string())
						.collect::<Vec<_>>()
						.join(",")
				})
				.collect::<Vec<_>>()
				.join(";");
			format!(
				"{}:{}:{}:{}:{}:{}",
				table.name,
				table.row_count,
				table.logical_bytes,
				table.schema_fingerprint,
				table.file_hash,
				unique_keys
			)
		})
		.collect::<Vec<_>>()
		.join("|");
	format!(
		"suite=tpch_persisted_dataset;format={TPCH_PERSISTED_MANIFEST_FORMAT};mode={mode};sf={scale_factor};tables={table_signature}"
	)
}

fn manifest_fingerprint_for_tables(
	mode: &str,
	scale_factor: f64,
	tables: &BTreeMap<String, TpchPersistedTable>,
) -> String {
	stable_fingerprint(&manifest_signature_for_tables(mode, scale_factor, tables))
}

#[derive(Debug, Clone, Copy)]
struct SnapshotUniqueKeyCandidate {
	table: &'static str,
	columns: &'static [&'static str],
}

const TPCH_SNAPSHOT_UNIQUE_KEY_CANDIDATES: &[SnapshotUniqueKeyCandidate] =
	&[SnapshotUniqueKeyCandidate {
		table: "customer",
		columns: &["c_custkey"],
	}];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SnapshotKeyValue {
	Int32(i32),
	Int64(i64),
}

enum SnapshotKeyArray<'a> {
	Int32(&'a Int32Array),
	Int64(&'a Int64Array),
}

impl SnapshotKeyArray<'_> {
	fn value(&self, row: usize) -> Option<SnapshotKeyValue> {
		match self {
			Self::Int32(values) => {
				(!values.is_null(row)).then(|| SnapshotKeyValue::Int32(values.value(row)))
			}
			Self::Int64(values) => {
				(!values.is_null(row)).then(|| SnapshotKeyValue::Int64(values.value(row)))
			}
		}
	}
}

fn verify_snapshot_unique_key_candidate(
	path: &Path,
	schema: &Schema,
	expected_rows: u64,
	candidate: SnapshotUniqueKeyCandidate,
) -> Result<Option<UniqueKeyProof>, String> {
	let mut ordinals = Vec::with_capacity(candidate.columns.len());
	let mut seen_ordinals = HashSet::new();
	if candidate.columns.is_empty() {
		return Err(format!(
			"snapshot unique-key candidate for {} has no columns",
			candidate.table
		));
	}
	for column in candidate.columns {
		let ordinal = schema.index_of(column).map_err(|_| {
			format!(
				"snapshot unique-key candidate {}.{column} is absent from the schema",
				candidate.table
			)
		})?;
		if !seen_ordinals.insert(ordinal) {
			return Err(format!(
				"snapshot unique-key candidate for {} repeats column {column}",
				candidate.table
			));
		}
		let field = schema.field(ordinal);
		if field.is_nullable() {
			return Ok(None);
		}
		if !matches!(field.data_type(), DataType::Int32 | DataType::Int64) {
			return Ok(None);
		}
		ordinals.push(ColumnOrdinal(u32::try_from(ordinal).map_err(|_| {
			format!("snapshot unique-key ordinal overflow for {column}")
		})?));
	}

	let file = File::open(path).map_err(|error| {
		format!(
			"open snapshot unique-key source {}: {error}",
			path.display()
		)
	})?;
	let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
		format!(
			"read snapshot unique-key source {}: {error}",
			path.display()
		)
	})?;
	let reader = builder
		.with_batch_size(TPCH_PERSISTED_CHUNK_ROWS)
		.build()
		.map_err(|error| {
			format!(
				"build snapshot unique-key reader {}: {error}",
				path.display()
			)
		})?;
	let mut keys = HashSet::new();
	let mut scanned_rows = 0_u64;
	for batch in reader {
		let batch = batch.map_err(|error| {
			format!(
				"scan snapshot unique-key source {}: {error}",
				path.display()
			)
		})?;
		scanned_rows = scanned_rows
			.checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
				format!(
					"snapshot unique-key row count overflow for {}",
					candidate.table
				)
			})?)
			.ok_or_else(|| {
				format!(
					"snapshot unique-key row count overflow for {}",
					candidate.table
				)
			})?;
		let arrays = ordinals
			.iter()
			.map(|ordinal| {
				let array = batch.column(ordinal.0 as usize);
				match array.data_type() {
					DataType::Int32 => array
						.as_any()
						.downcast_ref::<Int32Array>()
						.map(SnapshotKeyArray::Int32),
					DataType::Int64 => array
						.as_any()
						.downcast_ref::<Int64Array>()
						.map(SnapshotKeyArray::Int64),
					_ => None,
				}
				.ok_or_else(|| {
					format!(
						"snapshot unique-key type changed while scanning {} column {}",
						candidate.table, ordinal.0
					)
				})
			})
			.collect::<Result<Vec<_>, _>>()?;
		for row in 0..batch.num_rows() {
			let Some(key) = arrays
				.iter()
				.map(|array| array.value(row))
				.collect::<Option<Vec<_>>>()
			else {
				return Ok(None);
			};
			if !keys.insert(key) {
				return Ok(None);
			}
		}
	}
	if scanned_rows != expected_rows {
		return Err(format!(
			"snapshot unique-key scan for {} was incomplete: expected {expected_rows} rows, scanned {scanned_rows}",
			candidate.table
		));
	}
	Ok(UniqueKeyProof::verified_snapshot(ordinals))
}

fn verify_snapshot_unique_keys_for_table(
	name: &str,
	path: &Path,
	schema: &Schema,
	expected_rows: u64,
	candidates: &[SnapshotUniqueKeyCandidate],
) -> Result<Vec<UniqueKeyProof>, String> {
	let mut proofs = Vec::new();
	for candidate in candidates
		.iter()
		.copied()
		.filter(|candidate| candidate.table == name)
	{
		if let Some(proof) =
			verify_snapshot_unique_key_candidate(path, schema, expected_rows, candidate)?
		{
			proofs.push(proof);
		}
	}
	Ok(proofs)
}

fn parse_manifest_unique_key_proofs(
	table_value: &serde_json::Value,
	table_name: &str,
	schema: &Schema,
	schema_fingerprint: &str,
	file_hash: &str,
	manifest_path: &Path,
) -> Result<Vec<UniqueKeyProof>, String> {
	let values = table_value
		.get("unique_key_proofs")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| {
			format!(
				"{} table {table_name} missing unique_key_proofs",
				manifest_path.display()
			)
		})?;
	let mut proofs = Vec::with_capacity(values.len());
	let mut seen = HashSet::new();
	for value in values {
		let provenance = value
			.get("provenance")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| {
				format!(
					"{} table {table_name} unique-key proof missing provenance",
					manifest_path.display()
				)
			})?;
		if provenance != "verified_snapshot" {
			return Err(format!(
				"{} table {table_name} unique-key proof has unsupported provenance {provenance:?}",
				manifest_path.display()
			));
		}
		let proof_schema = value
			.get("schema_fingerprint")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| {
				format!(
					"{} table {table_name} unique-key proof missing schema_fingerprint",
					manifest_path.display()
				)
			})?;
		let proof_file_hash = value
			.get("file_hash")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| {
				format!(
					"{} table {table_name} unique-key proof missing file_hash",
					manifest_path.display()
				)
			})?;
		if proof_schema != schema_fingerprint || proof_file_hash != file_hash {
			return Err(format!(
				"{} table {table_name} unique-key proof binding mismatch",
				manifest_path.display()
			));
		}
		let columns = value
			.get("columns")
			.and_then(serde_json::Value::as_array)
			.ok_or_else(|| {
				format!(
					"{} table {table_name} unique-key proof missing columns",
					manifest_path.display()
				)
			})?
			.iter()
			.map(|column| {
				let ordinal = column.as_u64().ok_or_else(|| {
					format!(
						"{} table {table_name} unique-key proof column is not an ordinal",
						manifest_path.display()
					)
				})?;
				let ordinal = u32::try_from(ordinal).map_err(|_| {
					format!(
						"{} table {table_name} unique-key proof column overflows u32",
						manifest_path.display()
					)
				})?;
				let field = schema.fields().get(ordinal as usize).ok_or_else(|| {
					format!(
						"{} table {table_name} unique-key proof column {ordinal} is outside schema width {}",
						manifest_path.display(),
						schema.fields().len()
					)
				})?;
				if field.is_nullable()
					|| !matches!(field.data_type(), DataType::Int32 | DataType::Int64)
				{
					return Err(format!(
						"{} table {table_name} unique-key proof column {ordinal} is not a supported non-null key",
						manifest_path.display()
					));
				}
				Ok(ColumnOrdinal(ordinal))
			})
			.collect::<Result<Vec<_>, String>>()?;
		let proof = UniqueKeyProof::verified_snapshot(columns).ok_or_else(|| {
			format!(
				"{} table {table_name} unique-key proof has an empty or repeated column tuple",
				manifest_path.display()
			)
		})?;
		if !seen.insert(proof.columns().to_vec()) {
			return Err(format!(
				"{} table {table_name} repeats unique-key proof {:?}",
				manifest_path.display(),
				proof.columns()
			));
		}
		proofs.push(proof);
	}
	Ok(proofs)
}

fn manifest_unique_key_proofs_json(table: &TpchPersistedTable) -> Vec<serde_json::Value> {
	table
		.unique_key_evidence
		.iter()
		.map(|proof| {
			serde_json::json!({
				"columns": proof.columns().iter().map(|column| column.0).collect::<Vec<_>>(),
				"provenance": "verified_snapshot",
				"schema_fingerprint": table.schema_fingerprint,
				"file_hash": table.file_hash,
			})
		})
		.collect()
}

pub fn load_persisted_tpch_dataset(
	mode: &str,
	scale_factor: f64,
	root: &Path,
) -> Result<TpchPersistedDataset, String> {
	load_persisted_tpch_dataset_with_verification(
		mode,
		scale_factor,
		root,
		dataset_verification_mode(),
	)
}

fn load_persisted_tpch_dataset_with_verification(
	mode: &str,
	scale_factor: f64,
	root: &Path,
	verification: DatasetVerification,
) -> Result<TpchPersistedDataset, String> {
	// Resolved once for the whole load so every table in this dataset is reconciled the same way and
	// the returned dataset can state, as a fact, what was proved about it.
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
	let format = value
		.get("format")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| format!("{} missing format", manifest_path.display()))?;
	if format != TPCH_PERSISTED_MANIFEST_FORMAT {
		return Err(format!(
			"{} format mismatch: manifest={format} expected={TPCH_PERSISTED_MANIFEST_FORMAT}",
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
		let schema = table_schema_for_name(&name, mode == "strict")?;
		let unique_key_evidence = parse_manifest_unique_key_proofs(
			table,
			&name,
			schema.as_ref(),
			&schema_fingerprint,
			&file_hash,
			&manifest_path,
		)?;
		if !path.exists() {
			return Err(format!(
				"manifest table {name} path does not exist: {}",
				path.display()
			));
		}
		if verification == DatasetVerification::FullHash {
			let observed_hash = stable_file_hash(&path)?;
			if observed_hash != file_hash {
				return Err(format!(
					"manifest table {name} hash mismatch: manifest={file_hash} observed={observed_hash}"
				));
			}
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
				unique_key_evidence,
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
	if verification == DatasetVerification::ManifestOnly {
		for table in tables.values_mut() {
			table.unique_key_evidence.clear();
		}
	}
	Ok(TpchPersistedDataset {
		mode: mode.to_string(),
		scale_factor,
		root_dir,
		manifest_path,
		manifest_fingerprint,
		tables,
		verification,
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
		let unique_key_evidence = verify_snapshot_unique_keys_for_table(
			name,
			&path,
			schema.as_ref(),
			stats.row_count,
			TPCH_SNAPSHOT_UNIQUE_KEY_CANDIDATES,
		)?;
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
				unique_key_evidence,
			},
		);
	}

	let manifest_fingerprint = manifest_fingerprint_for_tables(mode, scale_factor, &tables);
	let manifest_path = root_dir.join("manifest.json");
	let manifest_tables = tables
		.values()
		.map(|table| {
			let unique_key_proofs = manifest_unique_key_proofs_json(table);
			serde_json::json!({
				"name": table.name,
				"path": table.path.display().to_string(),
				"row_count": table.row_count,
				"logical_bytes": table.logical_bytes,
				"schema_fingerprint": table.schema_fingerprint,
				"schema_signature": table.schema_signature,
				"file_hash": table.file_hash,
				"unique_key_proofs": unique_key_proofs,
			})
		})
		.collect::<Vec<_>>();
	let manifest = serde_json::json!({
		"suite": "tpch_persisted_dataset",
		"format": TPCH_PERSISTED_MANIFEST_FORMAT,
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
		// Written here, this pass: every table's manifest hash is the one `stable_file_hash` just
		// computed over the bytes on disk, so a freshly-generated dataset is full-hash by
		// construction regardless of the skip env, which only ever short-circuits a *re-read*.
		verification: DatasetVerification::FullHash,
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
		dataset_verification: None,
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
	partition_work_concurrency_observations: Vec<PartitionWorkConcurrencyObservation>,
	/// The executor's phase counters for this run. `Copy`, read once after the elapsed
	/// boundary — the local arm's raw material for ADR 538 component receipts.
	task_phases: TaskTelemetryPhases,
	/// The counters above are authoritative only when the task ledger sealed.
	phase_receipt: TaskPhaseReceipt,
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
			unique_key_evidence: Vec::new(),
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
		if let Err(error) =
			catalog.seed_unique_key_evidence(&TableRef::new(table.name), table.unique_key_evidence)
		{
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
	execute_query_with_metadata_coordinate(sql, tables, storage, gauge_planner_config(), None).await
}

async fn execute_query_with_metadata_coordinate(
	sql: &str,
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
	planner_config: PlannerConfig,
	query_budget: Option<QueryBudget>,
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

	execute_logical_with_catalog_timed(
		total_start,
		timings,
		catalog,
		logical,
		storage,
		planner_config,
		query_budget,
	)
	.await
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

	execute_logical_with_catalog_timed(
		total_start,
		timings,
		catalog,
		logical,
		storage,
		gauge_planner_config(),
		None,
	)
	.await
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
	planner_config: PlannerConfig,
	query_budget: Option<QueryBudget>,
) -> Result<QueryExecution, StageError> {
	let mut plan_fingerprint = None;
	let mut spill_bytes = 0;
	let bind_start = Instant::now();
	let (bound, local_join_admissions) =
		match bind_with_local_join_admissions(&logical, &catalog, &planner_config).await {
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
	let context = query_budget.map_or_else(
		|| tpch_execution_context(task_telemetry.clone()),
		|budget| tpch_execution_context_with_budget(task_telemetry.clone(), budget),
	);
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
	let partition_work_concurrency_observations =
		telemetry.partition_work_concurrency_observations.clone();
	let task_phases = telemetry.phases;
	let phase_receipt = telemetry.phase_receipt;
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
		partition_work_concurrency_observations,
		task_phases,
		phase_receipt,
	})
}

pub const GAUGE_MEMORY_PER_TASK_ENV: &str = "TORON_TPCH_MEMORY_PER_TASK";
pub const PARTITION_WORK_CONCURRENCY_ENV: &str = "TORON_PARTITION_PARALLELISM";

/// ADR 527 names this setting by its actual reach: it is the requested upper bound consumed by
/// one partitioned scan-build path, not a query-wide thread count. The absent arm mirrors Toron's
/// engine default; malformed explicit values fail loudly in the performance rail.
pub fn gauge_partition_work_concurrency_requested() -> Result<(u64, &'static str), String> {
	let default = std::thread::available_parallelism()
		.map(std::num::NonZeroUsize::get)
		.unwrap_or(1)
		.max(1);
	parse_partition_work_concurrency(std::env::var(PARTITION_WORK_CONCURRENCY_ENV), default)
}

fn parse_partition_work_concurrency(
	raw: Result<String, std::env::VarError>,
	default: usize,
) -> Result<(u64, &'static str), String> {
	match raw {
		Ok(raw) => match raw.parse::<usize>() {
			Ok(value) if value > 0 => u64::try_from(value)
				.map(|value| (value, "explicit"))
				.map_err(|_| format!("{PARTITION_WORK_CONCURRENCY_ENV} does not fit u64")),
			_ => Err(format!(
				"{PARTITION_WORK_CONCURRENCY_ENV} must be a positive integer, got {raw:?}"
			)),
		},
		Err(std::env::VarError::NotPresent) => u64::try_from(default)
			.map(|value| (value, "default"))
			.map_err(|_| format!("{PARTITION_WORK_CONCURRENCY_ENV} default does not fit u64")),
		Err(error) => Err(format!("{PARTITION_WORK_CONCURRENCY_ENV}: {error}")),
	}
}

/// The gauge's memory budget and where it came from. `default` means the
/// engine's 64 MiB test constant; `explicit` means the operator set the env.
/// Every perf row records this pair — a gauge figure never floats free of the
/// budget regime it was measured under.
pub fn gauge_memory_per_task() -> Result<(u64, &'static str), String> {
	parse_gauge_byte_budget(
		GAUGE_MEMORY_PER_TASK_ENV,
		std::env::var(GAUGE_MEMORY_PER_TASK_ENV),
		QueryBudget::test_default().memory_per_task.get(),
	)
}

fn parse_gauge_byte_budget(
	name: &str,
	raw: Result<String, std::env::VarError>,
	default: u64,
) -> Result<(u64, &'static str), String> {
	match raw {
		Ok(raw) => match raw.parse::<u64>() {
			Ok(bytes) if bytes > 0 => Ok((bytes, "explicit")),
			_ => Err(format!("{name} must be a positive byte count, got {raw:?}")),
		},
		Err(std::env::VarError::NotPresent) => Ok((default, "default")),
		Err(error) => Err(format!("{name}: {error}")),
	}
}

pub const GAUGE_SPILL_BYTES_RESERVED_ENV: &str = "TORON_TPCH_SPILL_BYTES_RESERVED";
pub const GAUGE_HASH_SMALLER_BUILD_ENV: &str = "TORON_TPCH_HASH_SMALLER_BUILD";

#[derive(Clone, Copy)]
enum BooleanGauge {
	HashSmallerBuild,
	FilteredBuildSideSwap,
	ScalarAggregateInputReduction,
	MembershipFirst,
	SideLocalFilterRouting,
	SemiFilteredReachableKeys,
	OuterCountPreaggregation,
}

impl BooleanGauge {
	fn name(self) -> &'static str {
		match self {
			Self::HashSmallerBuild => GAUGE_HASH_SMALLER_BUILD_ENV,
			Self::FilteredBuildSideSwap => GAUGE_FILTERED_BUILD_SIDE_SWAP_ENV,
			Self::ScalarAggregateInputReduction => GAUGE_SCALAR_AGG_INPUT_REDUCTION_ENV,
			Self::MembershipFirst => GAUGE_MEMBERSHIP_FIRST_ENV,
			Self::SideLocalFilterRouting => GAUGE_SIDE_LOCAL_FILTER_ROUTING_ENV,
			Self::SemiFilteredReachableKeys => GAUGE_SEMI_FILTERED_REACHABLE_KEYS_ENV,
			Self::OuterCountPreaggregation => GAUGE_OUTER_COUNT_PREAGGREGATION_ENV,
		}
	}

	fn default(self) -> bool {
		let planner = PlannerConfig::default();
		match self {
			Self::HashSmallerBuild => planner.hash_smaller_build_placement,
			Self::FilteredBuildSideSwap => planner.filtered_build_side_swap,
			Self::ScalarAggregateInputReduction => planner.scalar_aggregate_input_reduction,
			Self::MembershipFirst => planner.membership_first_composition,
			Self::SideLocalFilterRouting => planner.side_local_filter_routing,
			Self::SemiFilteredReachableKeys => planner.semi_filtered_reachable_keys,
			Self::OuterCountPreaggregation => planner.outer_count_preaggregation,
		}
	}

	fn parse(
		self,
		raw: Result<String, std::env::VarError>,
	) -> Result<(bool, &'static str), String> {
		parse_gauge_bool(self.name(), raw, self.default())
	}

	fn from_environment(self) -> Result<(bool, &'static str), String> {
		self.parse(std::env::var(self.name()))
	}
}

/// The gauge's smaller-build placement setting and its provenance: unset means
/// the engine default; "0"/"1" are explicit arms. Same loud contract as the
/// budget knobs.
pub fn gauge_hash_smaller_build() -> Result<(bool, &'static str), String> {
	BooleanGauge::HashSmallerBuild.from_environment()
}

fn parse_gauge_bool(
	name: &str,
	raw: Result<String, std::env::VarError>,
	default: bool,
) -> Result<(bool, &'static str), String> {
	match raw {
		Ok(raw) => match raw.as_str() {
			"0" => Ok((false, "explicit")),
			"1" => Ok((true, "explicit")),
			_ => Err(format!("{name} must be \"0\" or \"1\", got {raw:?}")),
		},
		Err(std::env::VarError::NotPresent) => Ok((default, "default")),
		Err(error) => Err(format!("{name}: {error}")),
	}
}

pub const GAUGE_FILTERED_BUILD_SIDE_SWAP_ENV: &str = "TORON_TPCH_FILTERED_BUILD_SIDE_SWAP";

/// The gauge's filtered build-side swap setting and its provenance: unset means
/// the engine default; "0"/"1" are explicit arms. Same loud contract as the
/// budget knobs.
pub fn gauge_filtered_build_side_swap() -> Result<(bool, &'static str), String> {
	BooleanGauge::FilteredBuildSideSwap.from_environment()
}

pub const GAUGE_SCALAR_AGG_INPUT_REDUCTION_ENV: &str = "TORON_TPCH_SCALAR_AGG_INPUT_REDUCTION";

/// The gauge's reachable-key scalar-aggregate reduction setting and its
/// provenance: unset means the engine default; "0"/"1" are explicit arms. Same
/// loud contract as the budget knobs.
pub fn gauge_scalar_agg_input_reduction() -> Result<(bool, &'static str), String> {
	BooleanGauge::ScalarAggregateInputReduction.from_environment()
}

pub const GAUGE_MEMBERSHIP_FIRST_ENV: &str = "TORON_TPCH_MEMBERSHIP_FIRST";

/// The gauge's membership-first composition setting and its provenance: unset
/// means the engine default; "0"/"1" are explicit arms. Same loud contract as
/// the budget knobs.
pub fn gauge_membership_first() -> Result<(bool, &'static str), String> {
	BooleanGauge::MembershipFirst.from_environment()
}

pub const GAUGE_SIDE_LOCAL_FILTER_ROUTING_ENV: &str = "TORON_TPCH_SIDE_LOCAL_FILTER_ROUTING";

/// The gauge's side-local filter routing setting and its provenance: unset
/// means the engine default; "0"/"1" are explicit arms. Same loud contract as
/// the budget knobs.
pub fn gauge_side_local_filter_routing() -> Result<(bool, &'static str), String> {
	BooleanGauge::SideLocalFilterRouting.from_environment()
}

pub const GAUGE_SEMI_FILTERED_REACHABLE_KEYS_ENV: &str = "TORON_TPCH_SEMI_FILTERED_REACHABLE_KEYS";

/// The gauge's membership-filtered reachable-key coverage setting and its
/// provenance: unset means the engine default; "0"/"1" are explicit arms.
pub fn gauge_semi_filtered_reachable_keys() -> Result<(bool, &'static str), String> {
	BooleanGauge::SemiFilteredReachableKeys.from_environment()
}

pub const GAUGE_OUTER_COUNT_PREAGGREGATION_ENV: &str = "TORON_TPCH_OUTER_COUNT_PREAGGREGATION";

/// The gauge's proven-key outer-count rewrite setting and provenance: unset
/// means the compiled default; "0"/"1" are explicit measurement arms.
pub fn gauge_outer_count_preaggregation() -> Result<(bool, &'static str), String> {
	BooleanGauge::OuterCountPreaggregation.from_environment()
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
	if let Ok((value, "explicit")) = gauge_filtered_build_side_swap() {
		config.filtered_build_side_swap = value;
	}
	if let Ok((value, "explicit")) = gauge_scalar_agg_input_reduction() {
		config.scalar_aggregate_input_reduction = value;
	}
	if let Ok((value, "explicit")) = gauge_membership_first() {
		config.membership_first_composition = value;
	}
	if let Ok((value, "explicit")) = gauge_side_local_filter_routing() {
		config.side_local_filter_routing = value;
	}
	if let Ok((value, "explicit")) = gauge_semi_filtered_reachable_keys() {
		config.semi_filtered_reachable_keys = value;
	}
	if let Ok((value, "explicit")) = gauge_outer_count_preaggregation() {
		config.outer_count_preaggregation = value;
	}
	// The gauge binds and immediately runs through `Executor::with_context`; it is
	// the caller that can prove this plan will not be fragmented. ADR 489's
	// distributed coordinator forcibly clears the corresponding signal.
	config.single_partition_execution = true;
	config.local_hash_join_memory_per_task = local_hash_join_memory_per_task;
	config
}

/// ADR 528: the census bind posture — the gauge config with the coordinator's exact one-field
/// delta. The distributed coordinator is never evidence that a join will execute in one
/// partition (`coordinator.rs`, ADR 489), so the census must bind the plan the fragmenter
/// would actually receive: ADR 490's local hash-join admission declines by design, and the
/// resulting fingerprint legitimately differs from the single-partition gauge plans.
pub fn distributed_gauge_planner_config() -> PlannerConfig {
	let (bytes, _provenance) =
		gauge_memory_per_task().unwrap_or_else(|error| panic!("gauge planner config: {error}"));
	distributed_gauge_planner_config_with_task_memory(Some(ByteCount::new(bytes)))
}

fn distributed_gauge_planner_config_with_task_memory(
	local_hash_join_memory_per_task: Option<ByteCount>,
) -> PlannerConfig {
	let mut config = gauge_planner_config_with_task_memory(local_hash_join_memory_per_task);
	config.single_partition_execution = false;
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

#[cfg(test)]
#[test]
fn adr528_distributed_posture_differs_only_in_single_partition_execution() {
	let gauge = gauge_planner_config();
	let mut distributed = distributed_gauge_planner_config();
	assert!(
		!distributed.single_partition_execution,
		"the census binds under the distributed posture"
	);
	assert!(
		gauge.single_partition_execution,
		"default gauge rails keep the single-partition posture"
	);
	distributed.single_partition_execution = true;
	assert_eq!(
		format!("{distributed:?}"),
		format!("{gauge:?}"),
		"the posture delta is exactly one field"
	);
}

/// The gauge's per-query spill reserve and its provenance, same contract as
/// [`gauge_memory_per_task`]: a frontier probe must be able to move one budget
/// dimension while holding the others explicitly, not implicitly.
pub fn gauge_spill_bytes_reserved() -> Result<(u64, &'static str), String> {
	parse_gauge_byte_budget(
		GAUGE_SPILL_BYTES_RESERVED_ENV,
		std::env::var(GAUGE_SPILL_BYTES_RESERVED_ENV),
		QueryBudget::test_default()
			.spill_bytes_reserved_per_query
			.get(),
	)
}

#[cfg(test)]
mod gauge_value_tests {
	use super::*;

	#[test]
	fn defaults_overrides_and_invalid_values_do_not_mutate_process_environment() {
		assert_eq!(
			parse_gauge_byte_budget(
				GAUGE_MEMORY_PER_TASK_ENV,
				Err(std::env::VarError::NotPresent),
				64 * 1024 * 1024,
			)
			.unwrap(),
			(64 * 1024 * 1024, "default")
		);
		assert_eq!(
			parse_gauge_byte_budget(
				GAUGE_MEMORY_PER_TASK_ENV,
				Ok("268435456".to_string()),
				64 * 1024 * 1024,
			)
			.unwrap(),
			(268435456, "explicit")
		);
		for invalid in ["0", "lots"] {
			assert!(
				parse_gauge_byte_budget(
					GAUGE_MEMORY_PER_TASK_ENV,
					Ok(invalid.to_string()),
					64 * 1024 * 1024,
				)
				.is_err()
			);
		}
	}

	#[test]
	fn boolean_gauge_specs_parse_without_mutating_process_environment() {
		for gauge in [
			BooleanGauge::HashSmallerBuild,
			BooleanGauge::FilteredBuildSideSwap,
			BooleanGauge::ScalarAggregateInputReduction,
			BooleanGauge::MembershipFirst,
			BooleanGauge::SideLocalFilterRouting,
			BooleanGauge::SemiFilteredReachableKeys,
			BooleanGauge::OuterCountPreaggregation,
		] {
			assert_eq!(
				gauge.parse(Err(std::env::VarError::NotPresent)).unwrap(),
				(gauge.default(), "default"),
				"{} default",
				gauge.name()
			);
			assert_eq!(
				gauge.parse(Ok("0".to_string())).unwrap(),
				(false, "explicit"),
				"{} disabled arm",
				gauge.name()
			);
			assert_eq!(
				gauge.parse(Ok("1".to_string())).unwrap(),
				(true, "explicit"),
				"{} enabled arm",
				gauge.name()
			);
			assert!(
				gauge.parse(Ok("yes".to_string())).is_err(),
				"{} malformed arm",
				gauge.name()
			);
		}
	}

	#[test]
	fn partition_work_concurrency_parses_without_mutating_process_environment() {
		assert_eq!(
			parse_partition_work_concurrency(Err(std::env::VarError::NotPresent), 3).unwrap(),
			(3, "default")
		);
		assert_eq!(
			parse_partition_work_concurrency(Ok("8".to_string()), 3).unwrap(),
			(8, "explicit")
		);
		for invalid in ["0", "many"] {
			assert!(parse_partition_work_concurrency(Ok(invalid.to_string()), 3).is_err());
		}
	}
}

fn tpch_execution_context_with_budget(
	task_telemetry: Arc<Mutex<TaskTelemetry>>,
	budget: QueryBudget,
) -> ExecutionContext {
	ExecutionContext::local_test()
		.with_task_telemetry(task_telemetry)
		.with_join_spill_store(
			Arc::new(wendelon::storage::BlockingFileOps),
			PathBuf::from(CACHE_ROOT).join("join_spill"),
		)
		.with_task_budget(TaskBudget::from_query_budget(budget))
}

fn tpch_execution_context(task_telemetry: Arc<Mutex<TaskTelemetry>>) -> ExecutionContext {
	let mut budget = QueryBudget::test_default();
	let memory = gauge_memory_per_task();
	let spill = gauge_spill_bytes_reserved();
	if let Ok((bytes, "explicit")) = memory {
		budget.memory_per_task = ByteCount::new(bytes);
	}
	if let Ok((bytes, "explicit")) = spill {
		budget.spill_bytes_reserved_per_query = ByteCount::new(bytes);
	}
	tpch_execution_context_with_budget(task_telemetry, budget)
}

/// ADR 528: the census's typed per-cell answer. Classification derives from this value alone —
/// never hand-assigned — and a `Fragmented` receipt proves plan admission only, not
/// executability.
#[derive(Debug, Clone)]
pub enum FragmentCensusOutcome {
	/// A pre-fragment wall (load/plan/bind); `phase` carries the exact stage.
	StageRejected {
		phase: Phase,
		reason: String,
		message: String,
	},
	/// The fragmenter answered no; `reason` is the typed [`FragmentError`] slug.
	FragmentRejected {
		reason: &'static str,
		message: String,
	},
	Fragmented(FragmentShapeReceipt),
}

/// ADR 528: the shape facts a fragmenting cell admits. `max_stage_width` is the widest
/// same-stage task fan-out — the substrate's admitted intra-query parallelism ceiling for
/// this plan; `single_task_producer_stages` counts the stages pinned to one task by the
/// producer path.
#[derive(Debug, Clone)]
pub struct FragmentShapeReceipt {
	pub stage_count: usize,
	pub stage_task_counts: Vec<usize>,
	pub max_stage_width: usize,
	pub single_task_producer_stages: usize,
	/// ADR 529 refinement, corrected by ADR 536: shuffle-consumer tasks the scheduler may
	/// dispatch against a live (unsealed) producer. The stall classification
	/// (`blocked_appenders > 0` ⟺ non-returning wait) is sound only when this is zero — a
	/// live-eligible consumer drains concurrently, making transient backpressure healthy
	/// rather than fatal. This now consumes the scheduler's own predicate, so an ADR 123
	/// multi-upstream join consumer is counted; the original narrower test saw zero.
	pub live_eligible_consumer_tasks: usize,
	/// ADR 536 Phase 0, observe-only: the live publication structure this plan would ask
	/// admission for. Recorded so the ADR 123 blast radius is enumerable from the artifact
	/// instead of inferred from stage widths.
	pub stage_upstreams: Vec<(u32, Vec<u32>)>,
	pub live_producer_stages: usize,
	pub multi_upstream_live_eligible_consumer_tasks: usize,
	pub live_residency_requirement: usize,
	pub admission_max_in_flight_tasks: Option<usize>,
	pub residency_exceeds_admission: bool,
}

// ===== ADR 538: matched-envelope timing atlas — protocol core =====

/// Measured pairs per cell. Fixed by ADR 538 before measurement.
pub const TIMING_ATLAS_MEASURED_PAIRS: usize = 7;
/// Discarded warmup pairs per cell (two children: one per arm).
pub const TIMING_ATLAS_WARMUP_PAIRS: usize = 1;
/// q15 is an inherited capability wall (`BroadcastProbeNotStarLinear`, never attempted).
/// It is excluded from the executable set by construction, not by omission.
pub const TIMING_ATLAS_EXCLUDED_CELLS: [&str; 1] = ["q15"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingArm {
	Local,
	Distributed,
}

impl TimingArm {
	pub fn label(self) -> &'static str {
		match self {
			Self::Local => "local",
			Self::Distributed => "distributed",
		}
	}

	fn other(self) -> Self {
		match self {
			Self::Local => Self::Distributed,
			Self::Distributed => Self::Local,
		}
	}
}

/// The 21 cells the atlas may time. q15 cannot enter this set.
pub fn timing_atlas_cells() -> Vec<&'static str> {
	ALL_QUERY_IDS
		.iter()
		.copied()
		.filter(|query| !TIMING_ATLAS_EXCLUDED_CELLS.contains(query))
		.collect()
}

/// Which arm a cell starts with. Seven pairs leave one arm first four times, so the
/// asymmetry is fixed by cell parity rather than left to run order: odd q-numbers start
/// local, even q-numbers start distributed.
pub fn timing_atlas_starting_arm(query: &str) -> TimingArm {
	let number: u32 = query
		.trim_start_matches('q')
		.parse()
		.expect("atlas cell ids are qNN");
	if number % 2 == 1 {
		TimingArm::Local
	} else {
		TimingArm::Distributed
	}
}

/// Arm order for one pair. Alternates across repetitions from the cell's fixed start, so
/// neither arm accrues a systematic position advantage.
pub fn timing_atlas_pair_order(query: &str, pair_index: usize) -> (TimingArm, TimingArm) {
	let start = timing_atlas_starting_arm(query);
	let first = if pair_index % 2 == 0 {
		start
	} else {
		start.other()
	};
	(first, first.other())
}

/// Paired estimators. ADR 538 makes these primary: per-arm medians are descriptive only,
/// because taking them would discard the drift control the adjacent-pair design pays for.
pub fn timing_atlas_median(values: &mut [f64]) -> Option<f64> {
	if values.is_empty() {
		return None;
	}
	values.sort_by(|left, right| left.partial_cmp(right).expect("elapsed is comparable"));
	let middle = values.len() / 2;
	Some(if values.len() % 2 == 1 {
		values[middle]
	} else {
		(values[middle - 1] + values[middle]) / 2.0
	})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingAtlasVerdict {
	Distinguished,
	Indistinguishable,
}

/// A cell is distinguished only when every paired delta is strictly positive or strictly
/// negative. Any zero makes it indistinguishable, whatever the medians show.
pub fn timing_atlas_verdict(paired_deltas: &[f64]) -> TimingAtlasVerdict {
	if paired_deltas.len() != TIMING_ATLAS_MEASURED_PAIRS {
		return TimingAtlasVerdict::Indistinguishable;
	}
	let all_positive = paired_deltas.iter().all(|delta| *delta > 0.0);
	let all_negative = paired_deltas.iter().all(|delta| *delta < 0.0);
	if all_positive || all_negative {
		TimingAtlasVerdict::Distinguished
	} else {
		TimingAtlasVerdict::Indistinguishable
	}
}

/// Every condition that must hold for a row to claim timing authority. Any failure yields
/// `timing_authority=false` with the reasons stated, never a silent downgrade.
#[derive(Debug, Clone)]
pub struct TimingAuthorityConditions {
	pub clean_build: bool,
	pub full_hash_dataset: bool,
	pub quiescent_host: bool,
	pub protocol_unmodified: bool,
	pub complete_cohort: bool,
}

impl TimingAuthorityConditions {
	pub fn granted(&self) -> bool {
		self.clean_build
			&& self.full_hash_dataset
			&& self.quiescent_host
			&& self.protocol_unmodified
			&& self.complete_cohort
	}

	pub fn withheld_reasons(&self) -> Vec<&'static str> {
		let mut reasons = Vec::new();
		if !self.clean_build {
			reasons.push("build_not_clean");
		}
		if !self.full_hash_dataset {
			reasons.push("dataset_not_full_hash");
		}
		if !self.quiescent_host {
			reasons.push("host_not_quiescent");
		}
		if !self.protocol_unmodified {
			reasons.push("protocol_constants_overridden");
		}
		if !self.complete_cohort {
			reasons.push("cohort_incomplete");
		}
		reasons
	}
}

/// One timed child: exactly one query execution on one arm. `query_elapsed_ms` covers the
/// execution alone — digesting, identity checks and receipt processing happen after the
/// elapsed boundary so they cannot inflate what the atlas attributes to the engine.
#[derive(Debug, Clone)]
pub struct TimingAtlasChild {
	pub query: String,
	pub arm: &'static str,
	pub pair_index: Option<usize>,
	pub position_in_pair: usize,
	pub attempt: u32,
	pub query_elapsed_ms: f64,
	pub row_count: Option<usize>,
	pub identity_matches: Option<bool>,
	pub valid: bool,
	pub failure: Option<String>,
	pub failure_is_io: bool,
	/// Derived after the elapsed boundary from the child's actual receipts. `None` on
	/// warmups, failures and invalid attempts — those may retain raw telemetry elsewhere
	/// but must never produce eligible attribution.
	pub attribution: Option<TimingAtlasChildAttribution>,
}

#[derive(Debug, Clone)]
pub struct TimingAtlasPair {
	pub pair_index: usize,
	pub first_arm: &'static str,
	pub local: TimingAtlasChild,
	pub distributed: TimingAtlasChild,
	pub delta_ms: f64,
	/// `None` when the local arm measured zero: a ratio is undefined there, and NaN or
	/// infinity must never reach the journal.
	pub ratio: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct TimingAtlasCellOutcome {
	pub query: String,
	pub setup_ms: f64,
	pub teardown_ms: f64,
	pub verification_ms: f64,
	pub warmups: Vec<TimingAtlasChild>,
	pub pairs: Vec<TimingAtlasPair>,
	pub invalid_attempts: Vec<TimingAtlasChild>,
	/// Inter-child reclamation, recorded separately and never inside query elapsed.
	pub cleanup_ms: f64,
	/// Whether any reclamation actually failed, distinct from the cell failing for some
	/// other reason.
	pub cleanup_failed: bool,
	/// Peer closes tolerated across this cell's workers. Reported so the count is
	/// visible in the artifact rather than absorbed by the tolerance.
	pub peer_closes: usize,
	pub failure: Option<String>,
}

impl TimingAtlasCellOutcome {
	/// A cell finalizes only with exactly seven valid pairs and no recorded failure.
	pub fn finalized(&self) -> bool {
		self.failure.is_none() && self.pairs.len() == TIMING_ATLAS_MEASURED_PAIRS
	}

	pub fn paired_deltas(&self) -> Vec<f64> {
		self.pairs.iter().map(|pair| pair.delta_ms).collect()
	}

	pub fn paired_ratios(&self) -> Vec<f64> {
		self.pairs.iter().filter_map(|pair| pair.ratio).collect()
	}

	pub fn verdict(&self) -> TimingAtlasVerdict {
		if !self.finalized() {
			return TimingAtlasVerdict::Indistinguishable;
		}
		timing_atlas_verdict(&self.paired_deltas())
	}
}

/// Classify a child failure. Only an I/O failure is retryable, and then only as a whole
/// pair; everything else forbids timing authority for the cell.
fn timing_atlas_failure_is_io(message: &str) -> bool {
	message.contains("QueryError::Io") || message.contains("Io(")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingAtlasPairOutcome {
	Valid,
	RetryableIo,
	Fatal,
}

/// Decide a pair's fate from its two children. An I/O failure invalidates the *whole* pair —
/// a retried distributed child must never be timed against its original local partner,
/// because they would come from different machine states.
pub fn timing_atlas_pair_outcome(
	first: &TimingAtlasChild,
	second: &TimingAtlasChild,
	already_retried: bool,
) -> TimingAtlasPairOutcome {
	for child in [first, second] {
		if child.identity_matches == Some(false) {
			return TimingAtlasPairOutcome::Fatal;
		}
	}
	for child in [first, second] {
		if !child.valid {
			return if child.failure_is_io && !already_retried {
				TimingAtlasPairOutcome::RetryableIo
			} else {
				TimingAtlasPairOutcome::Fatal
			};
		}
	}
	TimingAtlasPairOutcome::Valid
}

/// Artifact naming. A partial artifact can never be mistaken for an authoritative one:
/// the suffix is part of the contract, not a convention.
pub const TIMING_ATLAS_PARTIAL_SUFFIX: &str = ".partial";
pub const TIMING_ATLAS_SEAL_KIND: &str = "timing_atlas_seal";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingAtlasChildRole {
	/// Discarded. Runs in parity order but never enters an estimator.
	Warmup,
	Measured {
		pair_index: usize,
		position: usize,
	},
}

impl TimingAtlasChildRole {
	pub fn is_measured(self) -> bool {
		matches!(self, Self::Measured { .. })
	}
}

/// One planned child. The plan is built before execution so arm order, warmup placement and
/// query identity are decidable — and testable — without a cluster.
#[derive(Debug, Clone)]
pub struct TimingAtlasChildPlan {
	pub arm: TimingArm,
	pub role: TimingAtlasChildRole,
	pub query_id: QueryId,
}

/// The full child sequence for one cell: two discarded warmups in parity order, then seven
/// adjacent pairs alternating from that same start. Every child gets a distinct identity.
pub fn timing_atlas_cell_plan(query: &str) -> Vec<TimingAtlasChildPlan> {
	let mut plan = Vec::new();
	let (warm_first, warm_second) = timing_atlas_pair_order(query, 0);
	for arm in [warm_first, warm_second] {
		plan.push(TimingAtlasChildPlan {
			arm,
			role: TimingAtlasChildRole::Warmup,
			query_id: QueryId::new(),
		});
	}
	for pair_index in 0..TIMING_ATLAS_MEASURED_PAIRS {
		let (first, second) = timing_atlas_pair_order(query, pair_index);
		for (position, arm) in [first, second].into_iter().enumerate() {
			plan.push(TimingAtlasChildPlan {
				arm,
				role: TimingAtlasChildRole::Measured {
					pair_index,
					position,
				},
				query_id: QueryId::new(),
			});
		}
	}
	plan
}

/// Whether a finalized artifact may assert timing authority. Every clause is a veto:
/// a partial filename, a missing seal, an incomplete cohort, or any failed global
/// condition all mean non-authoritative.
pub fn timing_atlas_artifact_is_authoritative(
	file_name: &str,
	has_terminal_seal: bool,
	finalized_cells: usize,
	conditions: &TimingAuthorityConditions,
) -> bool {
	!file_name.ends_with(TIMING_ATLAS_PARTIAL_SUFFIX)
		&& has_terminal_seal
		&& finalized_cells == timing_atlas_cells().len()
		&& conditions.granted()
}

/// What one child execution yields. Produced inside the timed region; everything derived
/// from it is produced outside.
#[derive(Debug, Clone)]
pub struct TimingAtlasExecution {
	pub row_count: usize,
}

/// Facts derived after the elapsed boundary: digesting, identity comparison and receipt
/// reduction. None of this may contribute to `query_elapsed_ms`.
#[derive(Debug, Clone)]
pub struct TimingAtlasReduction {
	pub identity_matches: bool,
	/// The child's component attribution, derived from actual receipts strictly after the
	/// elapsed boundary.
	pub attribution: Option<TimingAtlasChildAttribution>,
}

/// The seam the atlas runner drives. Injecting it lets the worker scope and the timer
/// placement be observed deterministically — the pre-built plan prevents child reordering,
/// but cannot prove the caller constructs workers once per cell or times only execution.
// Consumed only through generics inside this crate and never as `dyn`, so the auto-trait
// bounds this lint protects cannot be needed here.
#[allow(async_fn_in_trait)]
pub trait TimingAtlasHarness {
	/// Construct workers and coordinator for a cell. Exactly once per cell, before warmups.
	async fn setup_cell(&mut self, query: &str) -> Result<(), String>;
	/// Execute one child. This call alone is inside the elapsed boundary.
	async fn execute_child(
		&mut self,
		plan: &TimingAtlasChildPlan,
	) -> Result<TimingAtlasExecution, String>;
	/// Digest, compare identity and reduce receipts. Always after the elapsed boundary.
	async fn reduce_child(
		&mut self,
		plan: &TimingAtlasChildPlan,
		execution: &TimingAtlasExecution,
	) -> Result<TimingAtlasReduction, String>;
	/// Reclaim one child's query namespace. Runs after the elapsed boundary for every
	/// child — successful, failed or invalidated — and must return retained bytes to the
	/// post-setup baseline before the next child begins.
	async fn cleanup_child(&mut self, plan: &TimingAtlasChildPlan) -> Result<(), String>;
	/// Tear down workers. Exactly once per cell, after the last measured child.
	async fn teardown_cell(&mut self, query: &str) -> Result<(), String>;
	/// Peer closes tolerated across this cell, read after `teardown_cell`.
	///
	/// Defaults to zero for harnesses with no network peers, where zero is the
	/// true count rather than a stand-in for an unwired counter.
	fn peer_closes(&self) -> usize {
		0
	}
	/// Elapsed source, in milliseconds.
	fn now_ms(&mut self) -> f64;
}

/// Drive one cell: setup once, both warmups, seven pairs, teardown once. Setup, teardown
/// and reduction are recorded but never enter `query_elapsed_ms`.
pub async fn run_timing_atlas_cell_with<H: TimingAtlasHarness>(
	harness: &mut H,
	query: &str,
) -> TimingAtlasCellOutcome {
	let mut outcome = TimingAtlasCellOutcome {
		query: query.to_string(),
		setup_ms: 0.0,
		teardown_ms: 0.0,
		verification_ms: 0.0,
		warmups: Vec::new(),
		pairs: Vec::new(),
		invalid_attempts: Vec::new(),
		cleanup_ms: 0.0,
		cleanup_failed: false,
		peer_closes: 0,
		failure: None,
	};

	let setup_start = harness.now_ms();
	if let Err(error) = harness.setup_cell(query).await {
		outcome.failure = Some(format!("setup: {error}"));
		return outcome;
	}
	outcome.setup_ms = harness.now_ms() - setup_start;

	let plan = timing_atlas_cell_plan(query);
	let mut pending: Vec<TimingAtlasChild> = Vec::new();
	for child_plan in &plan {
		let child = run_timing_atlas_child(harness, query, child_plan, &mut outcome).await;
		match child_plan.role {
			TimingAtlasChildRole::Warmup => outcome.warmups.push(child),
			TimingAtlasChildRole::Measured { pair_index, .. } => {
				pending.push(child);
				if pending.len() < 2 {
					continue;
				}
				let second = pending.pop().expect("second child");
				let first = pending.pop().expect("first child");
				match timing_atlas_pair_outcome(&first, &second, false) {
					TimingAtlasPairOutcome::Valid => {
						outcome
							.pairs
							.push(assemble_timing_atlas_pair(pair_index, first, second));
					}
					TimingAtlasPairOutcome::RetryableIo => {
						// The whole pair is invalidated: a retried child must never be
						// timed against a partner from a different machine state. Both
						// arms rerun once, in the same order, with fresh identities.
						outcome.invalid_attempts.push(first);
						outcome.invalid_attempts.push(second);
						match retry_timing_atlas_pair(harness, query, pair_index, &mut outcome)
							.await
						{
							Some(pair) => outcome.pairs.push(pair),
							None => break,
						}
					}
					TimingAtlasPairOutcome::Fatal => {
						outcome.failure.get_or_insert_with(|| {
							first
								.failure
								.clone()
								.or_else(|| second.failure.clone())
								.unwrap_or_else(|| "identity mismatch".to_string())
						});
						outcome.invalid_attempts.push(first);
						outcome.invalid_attempts.push(second);
						break;
					}
				}
			}
		}
	}

	let teardown_start = harness.now_ms();
	if let Err(error) = harness.teardown_cell(query).await {
		outcome
			.failure
			.get_or_insert_with(|| format!("teardown: {error}"));
	}
	outcome.teardown_ms = harness.now_ms() - teardown_start;
	outcome.peer_closes = harness.peer_closes();
	outcome
}

async fn run_timing_atlas_child<H: TimingAtlasHarness>(
	harness: &mut H,
	query: &str,
	child_plan: &TimingAtlasChildPlan,
	outcome: &mut TimingAtlasCellOutcome,
) -> TimingAtlasChild {
	let (pair_index, position) = match child_plan.role {
		TimingAtlasChildRole::Warmup => (None, 0),
		TimingAtlasChildRole::Measured {
			pair_index,
			position,
		} => (Some(pair_index), position),
	};
	let mut child = TimingAtlasChild {
		query: query.to_string(),
		arm: child_plan.arm.label(),
		pair_index,
		position_in_pair: position,
		attempt: 1,
		query_elapsed_ms: 0.0,
		row_count: None,
		identity_matches: None,
		valid: false,
		failure: None,
		failure_is_io: false,
		attribution: None,
	};

	// The elapsed boundary encloses execution and nothing else.
	let started = harness.now_ms();
	let executed = harness.execute_child(child_plan).await;
	child.query_elapsed_ms = harness.now_ms() - started;

	let execution = match executed {
		Ok(execution) => execution,
		Err(error) => {
			child.failure_is_io = timing_atlas_failure_is_io(&error);
			child.failure = Some(error);
			// A failed child may still have published partitions before it failed, so its
			// namespace is reclaimed like any other. Cleanup is awaited on every path.
			let cleanup_start = harness.now_ms();
			if let Err(cleanup_error) = harness.cleanup_child(child_plan).await {
				outcome.cleanup_failed = true;
				outcome.failure.get_or_insert_with(|| {
					format!("cleanup failed, reclamation unproven: {cleanup_error}")
				});
			}
			outcome.cleanup_ms += harness.now_ms() - cleanup_start;
			return child;
		}
	};
	child.row_count = Some(execution.row_count);

	// Digesting, identity and receipt reduction happen after the boundary.
	let verification_start = harness.now_ms();
	match harness.reduce_child(child_plan, &execution).await {
		Ok(reduction) => {
			child.identity_matches = Some(reduction.identity_matches);
			child.valid = reduction.identity_matches;
			if !reduction.identity_matches {
				child.failure = Some("identity mismatch".to_string());
			}
			// Attribution rides only a valid child: an invalid attempt keeps its raw
			// telemetry in the harness but must not carry an eligible receipt.
			if child.valid {
				child.attribution = reduction.attribution;
			}
		}
		Err(error) => {
			child.failure_is_io = timing_atlas_failure_is_io(&error);
			child.failure = Some(error);
		}
	}
	outcome.verification_ms += harness.now_ms() - verification_start;

	// Reclamation runs for every child, including failed and invalidated ones, so no
	// namespace survives into the next child's machine state.
	let cleanup_start = harness.now_ms();
	if let Err(error) = harness.cleanup_child(child_plan).await {
		outcome.cleanup_failed = true;
		// An attempted cleanup is not a successful reclamation. If the namespace may still
		// be resident, every later child of this cell measures a different machine, so the
		// cell — and its timing authority — is forfeit.
		child.valid = false;
		child
			.failure
			.get_or_insert_with(|| format!("cleanup: {error}"));
		outcome
			.failure
			.get_or_insert_with(|| format!("cleanup failed, reclamation unproven: {error}"));
	}
	outcome.cleanup_ms += harness.now_ms() - cleanup_start;
	child
}

/// Rerun one invalidated pair. Both arms rerun in the same order with two fresh identities,
/// distinct from both identities of the pair being replaced.
async fn retry_timing_atlas_pair<H: TimingAtlasHarness>(
	harness: &mut H,
	query: &str,
	pair_index: usize,
	outcome: &mut TimingAtlasCellOutcome,
) -> Option<TimingAtlasPair> {
	let (first_arm, second_arm) = timing_atlas_pair_order(query, pair_index);
	let mut replacements = Vec::with_capacity(2);
	for (position, arm) in [first_arm, second_arm].into_iter().enumerate() {
		let child_plan = TimingAtlasChildPlan {
			arm,
			role: TimingAtlasChildRole::Measured {
				pair_index,
				position,
			},
			query_id: QueryId::new(),
		};
		let mut child = run_timing_atlas_child(harness, query, &child_plan, outcome).await;
		child.attempt = 2;
		replacements.push(child);
	}
	let second = replacements.pop().expect("second replacement");
	let first = replacements.pop().expect("first replacement");
	match timing_atlas_pair_outcome(&first, &second, true) {
		TimingAtlasPairOutcome::Valid => {
			Some(assemble_timing_atlas_pair(pair_index, first, second))
		}
		_ => {
			outcome.failure.get_or_insert_with(|| {
				first
					.failure
					.clone()
					.or_else(|| second.failure.clone())
					.unwrap_or_else(|| "pair retry exhausted".to_string())
			});
			outcome.invalid_attempts.push(first);
			outcome.invalid_attempts.push(second);
			None
		}
	}
}

fn assemble_timing_atlas_pair(
	pair_index: usize,
	first: TimingAtlasChild,
	second: TimingAtlasChild,
) -> TimingAtlasPair {
	let first_arm = first.arm;
	let (local, distributed) = if first.arm == TimingArm::Local.label() {
		(first, second)
	} else {
		(second, first)
	};
	let delta_ms = distributed.query_elapsed_ms - local.query_elapsed_ms;
	let ratio = (local.query_elapsed_ms > 0.0)
		.then(|| distributed.query_elapsed_ms / local.query_elapsed_ms)
		.filter(|ratio| ratio.is_finite());
	TimingAtlasPair {
		pair_index,
		first_arm,
		local,
		distributed,
		delta_ms,
		ratio,
	}
}

/// One observation of host quiescence. ADR 538 requires this at the start of the run and
/// again at the final seal: a service restarted mid-atlas would otherwise pass a single
/// check and still contaminate the measurements it was supposed to be absent from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostQuiescence {
	pub service_inactive: bool,
	pub sql_port_free: bool,
}

impl HostQuiescence {
	pub fn quiescent(&self) -> bool {
		self.service_inactive && self.sql_port_free
	}
}

/// Observe the host: the local deployment service must be inactive and its SQL port free.
pub fn observe_host_quiescence() -> HostQuiescence {
	let service_inactive = std::process::Command::new("systemctl")
		.args(["--user", "is-active", "nilket-local.service"])
		.output()
		.map(|output| String::from_utf8_lossy(&output.stdout).trim() != "active")
		.unwrap_or(true);
	// Binding succeeds only when nothing else holds the endpoint.
	let sql_port_free = std::net::UdpSocket::bind("127.0.0.1:7742").is_ok();
	HostQuiescence {
		service_inactive,
		sql_port_free,
	}
}

/// Authority requires quiescence at both observations, not just one.
pub fn timing_atlas_quiescent_throughout(start: &HostQuiescence, seal: &HostQuiescence) -> bool {
	start.quiescent() && seal.quiescent()
}

#[derive(Debug)]
pub struct TimingAtlasArtifact {
	partial_path: PathBuf,
	canonical_path: PathBuf,
	file: std::fs::File,
	sealed: bool,
}

impl TimingAtlasArtifact {
	/// Open a fresh partial artifact. Refuses when either the partial or the canonical
	/// destination already exists — a run never overwrites a previous atlas.
	pub fn create(canonical_path: &Path) -> Result<Self, String> {
		let partial_path = PathBuf::from(format!(
			"{}{TIMING_ATLAS_PARTIAL_SUFFIX}",
			canonical_path.display()
		));
		if canonical_path.exists() {
			return Err(format!(
				"timing atlas refuses to overwrite existing artifact {}",
				canonical_path.display()
			));
		}
		if partial_path.exists() {
			return Err(format!(
				"timing atlas refuses to reuse existing partial {}",
				partial_path.display()
			));
		}
		if let Some(parent) = canonical_path.parent() {
			std::fs::create_dir_all(parent)
				.map_err(|error| format!("timing atlas output dir: {error}"))?;
		}
		let file = std::fs::File::options()
			.write(true)
			.create_new(true)
			.open(&partial_path)
			.map_err(|error| format!("timing atlas partial {}: {error}", partial_path.display()))?;
		Ok(Self {
			partial_path,
			canonical_path: canonical_path.to_path_buf(),
			file,
			sealed: false,
		})
	}

	pub fn partial_path(&self) -> &Path {
		&self.partial_path
	}

	/// Append one row. Rows are written as they are produced so a crash preserves progress.
	pub fn append_row(&mut self, row: &serde_json::Value) -> Result<(), String> {
		use std::io::Write;
		writeln!(self.file, "{row}").map_err(|error| format!("timing atlas append: {error}"))
	}

	/// Append the terminal seal. Written last, after every row, and only once.
	pub fn append_seal(&mut self, seal: &serde_json::Value) -> Result<(), String> {
		if self.sealed {
			return Err("timing atlas already sealed".to_string());
		}
		self.append_row(seal)?;
		self.sealed = true;
		Ok(())
	}

	/// Flush, sync and atomically rename the partial into place. Refuses to publish an
	/// unsealed artifact or to clobber a canonical destination that appeared meanwhile.
	pub fn publish(mut self) -> Result<PathBuf, String> {
		use std::io::Write;
		if !self.sealed {
			return Err("timing atlas refuses to publish without a terminal seal".to_string());
		}
		if self.canonical_path.exists() {
			return Err(format!(
				"timing atlas refuses to overwrite existing artifact {}",
				self.canonical_path.display()
			));
		}
		self.file
			.flush()
			.map_err(|error| format!("timing atlas flush: {error}"))?;
		self.file
			.sync_all()
			.map_err(|error| format!("timing atlas sync: {error}"))?;
		std::fs::rename(&self.partial_path, &self.canonical_path)
			.map_err(|error| format!("timing atlas publish: {error}"))?;
		Ok(self.canonical_path.clone())
	}
}

/// Identity of the resident infrastructure for one cell. Captured per child so a harness
/// that quietly rebuilds workers between children is caught: the trait proves the runner's
/// call topology, but only this proves `setup_cell` owns the workers and `execute_child`
/// reuses them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimingAtlasResidency {
	pub worker_ids: Vec<u128>,
	pub endpoints: Vec<String>,
}

/// Records the residency observed at each child, so a whole cell can be checked at once.
#[derive(Debug, Clone, Default)]
pub struct TimingAtlasResidencyLog {
	observations: Vec<TimingAtlasResidency>,
}

impl TimingAtlasResidencyLog {
	pub fn observe(&mut self, residency: TimingAtlasResidency) {
		self.observations.push(residency);
	}

	pub fn len(&self) -> usize {
		self.observations.len()
	}

	pub fn is_empty(&self) -> bool {
		self.observations.is_empty()
	}

	/// Every child of a cell saw the same worker ids and endpoints — infrastructure was
	/// constructed once and reused, not recreated inside `execute_child`.
	pub fn stable(&self) -> bool {
		let mut distinct = self.observations.iter();
		let Some(first) = distinct.next() else {
			return false;
		};
		distinct.all(|observation| observation == first)
	}
}

/// The real harness: census workers and coordinator, constructed once per cell and reused
/// by every child. The spec is built once outside every timed region.
pub struct CensusTimingAtlasHarness {
	sql: String,
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
	catalog: Option<MockCatalog>,
	width: usize,
	workers: Vec<ExecutionCensusWorkerHarness>,
	coordinator: Option<DistributedQueryCoordinator<StaticWorkerRegistry, TcpTransport>>,
	reference_identity: Option<ExecutionIdentityFacts>,
	last_batches: Option<Vec<RecordBatch>>,
	/// Retained bytes immediately after setup, captured by the harness that owns setup so a
	/// caller cannot observe it at the wrong moment.
	baseline_bytes: Option<u64>,
	/// Retained bytes observed at teardown, before the workers are dropped — reading after
	/// teardown would always be zero and prove nothing.
	final_retained_bytes: Option<u64>,
	/// Peer closes tolerated across this cell's workers, summed at teardown. Zero
	/// until teardown runs, and zero is a real observation, not a missing one.
	peer_closes: usize,
	pub residency: TimingAtlasResidencyLog,
	/// Opt-in retention the coordinator fills with each distributed child's per-task
	/// telemetry report. Drained in `reduce_child`, so it never accumulates across children.
	telemetry_retention: toron::coordinator::TaskTelemetryRetention,
	/// The local arm's executor window and phase counters, captured in `execute_child` (the
	/// copy is `Copy`-cheap) and consumed in `reduce_child` after the boundary.
	last_local_execution: Option<(f64, TaskTelemetryPhases, TaskPhaseReceipt)>,
	/// ADR 554 §3 B2: SQL planning is **harness-owned**. It happens before the
	/// coordinator is called at all, so no coordinator span can see it, and the
	/// two arms declare different boundaries — the local arm's `execute_ms` starts
	/// after binding, the distributed envelope contains it. Kept separate from
	/// `pre_dispatch_ms` for exactly that reason.
	last_sql_planning_ms: Option<f64>,
	/// Harness-owned by necessity: the true drain ends at **consumer exhaustion**,
	/// which the coordinator cannot observe — it stops at producer finalization.
	last_consumer_drain_ms: Option<f64>,
}

impl CensusTimingAtlasHarness {
	/// Build the shared query spec. Outside every timed region by construction: this runs
	/// before `setup_cell`.
	pub async fn prepare(
		query: &str,
		mode: &str,
		scale_factor: f64,
		data_root: &Path,
	) -> Result<Self, String> {
		let spec = build_persisted_perf_query_spec(query, mode, scale_factor, data_root)?;
		let storage: Arc<dyn StorageScan> = spec.storage;
		let preflight = execution_census_preflight(&spec.sql, spec.tables.clone())
			.await
			.map_err(|rejection| {
				format!(
					"timing atlas preflight rejected {query} at {}: {} ({})",
					rejection.phase.as_str(),
					rejection.reason,
					rejection.message
				)
			})?;
		let width = preflight.shape.max_stage_width.max(1);
		Ok(Self {
			sql: spec.sql.into_owned(),
			tables: spec.tables,
			storage,
			catalog: Some(preflight.catalog),
			width,
			workers: Vec::new(),
			coordinator: None,
			reference_identity: None,
			last_batches: None,
			baseline_bytes: None,
			final_retained_bytes: None,
			peer_closes: 0,
			residency: TimingAtlasResidencyLog::default(),
			telemetry_retention: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
			last_local_execution: None,
			last_sql_planning_ms: None,
			last_consumer_drain_ms: None,
		})
	}

	/// Residency read from the *actual* resident worker entries, never from the values
	/// handed to setup — recording the intended identity would make stability vacuous.
	fn observed_residency(&self) -> TimingAtlasResidency {
		TimingAtlasResidency {
			worker_ids: self
				.workers
				.iter()
				.map(|worker| worker.entry.worker_id.get().as_u128())
				.collect(),
			endpoints: self
				.workers
				.iter()
				.map(|worker| worker.entry.endpoint.as_str().to_string())
				.collect(),
		}
	}
}

impl TimingAtlasHarness for CensusTimingAtlasHarness {
	async fn setup_cell(&mut self, query: &str) -> Result<(), String> {
		let distributed_budget = execution_census_distributed_budget(self.width);
		let memory_pool_max_buffer_capacity_bytes =
			usize::try_from(distributed_budget.memory_per_task.get()).unwrap_or(usize::MAX);
		for worker_index in 0..EXECUTION_CENSUS_WORKER_COUNT {
			match spawn_execution_census_worker(
				worker_index,
				self.storage.clone(),
				distributed_budget,
				memory_pool_max_buffer_capacity_bytes,
			)
			.await
			{
				Ok(worker) => self.workers.push(worker),
				Err(error) => {
					let workers = std::mem::take(&mut self.workers);
					shutdown_execution_census_workers(workers).await?;
					return Err(format!("timing atlas setup {query}: {error}"));
				}
			}
		}
		let catalog: Arc<dyn Catalog> = Arc::new(
			self.catalog
				.take()
				.ok_or_else(|| "timing atlas catalog already consumed".to_string())?,
		);
		// ADR 529's sole construction rail; the atlas only adds its own telemetry
		// retention on top, rather than reconstructing the coordinator.
		let coordinator = Some(
			build_execution_census_coordinator(
				catalog,
				&self.workers,
				memory_pool_max_buffer_capacity_bytes,
				distributed_budget,
			)
			.with_task_telemetry_retention(self.telemetry_retention.clone()),
		);
		self.coordinator = coordinator;
		self.baseline_bytes = Some(self.resident_partition_bytes());
		Ok(())
	}

	async fn execute_child(
		&mut self,
		plan: &TimingAtlasChildPlan,
	) -> Result<TimingAtlasExecution, String> {
		// Reuse the resident infrastructure; never rebuild it here.
		self.residency.observe(self.observed_residency());
		let batches = match plan.arm {
			TimingArm::Local => {
				let budget = execution_census_local_budget();
				let execution = execute_query_with_metadata_coordinate(
					&self.sql,
					self.tables.clone(),
					self.storage.clone(),
					gauge_planner_config_with_task_memory(Some(budget.memory_per_task)),
					Some(budget),
				)
				.await
				.map_err(|error| {
					format!("local arm {}: {}", error.phase.as_str(), error.message)
				})?;
				// A `Copy` of the counters and one float: the only attribution work inside
				// the boundary. Node building and path derivation wait for `reduce_child`.
				self.last_local_execution = Some((
					execution.timings.execute_ms,
					execution.task_phases,
					execution.phase_receipt,
				));
				execution.batches
			}
			TimingArm::Distributed => {
				let coordinator = self
					.coordinator
					.as_ref()
					.ok_or_else(|| "timing atlas coordinator absent".to_string())?;
				let planning_started = std::time::Instant::now();
				let logical = SqlPlanner::new()
					.plan(&self.sql)
					.map_err(|error| format!("distributed arm plan: {error}"))?;
				self.last_sql_planning_ms = Some(planning_started.elapsed().as_secs_f64() * 1000.0);
				// The plan's identity is the child's identity. Letting the coordinator mint
				// its own would collide in the resident partition store, because workers
				// persist across all sixteen children of a cell.
				let budget = execution_census_distributed_budget(self.width);
				let context = ExecutionContext::new(
					toron::ir::SessionId(538),
					plan.query_id,
					toron::storage::TenantId(1),
					toron::storage::HlcBound(1),
				)
				.with_task_budget(TaskBudget::from_query_budget(budget));
				// Split from `execute_query_with_context`, which for a distributed read
				// is exactly this: open the stream, then drain it. Same work, same
				// order, same envelope — the split only makes consumer exhaustion
				// observable, which no coordinator-side clock can see.
				let mut stream = coordinator
					.execute_query_stream_with_context(&logical, context)
					.await
					.map_err(|error| format!("distributed arm: {error:?}"))?;
				let drain_started = std::time::Instant::now();
				let mut drained = Vec::new();
				while let Some(batch) = stream.next().await {
					drained.push(batch.map_err(|error| format!("distributed arm: {error:?}"))?);
				}
				self.last_consumer_drain_ms = Some(drain_started.elapsed().as_secs_f64() * 1000.0);
				drained
			}
		};
		let row_count = batches.iter().map(RecordBatch::num_rows).sum();
		self.last_batches = Some(batches);
		Ok(TimingAtlasExecution { row_count })
	}

	async fn reduce_child(
		&mut self,
		plan: &TimingAtlasChildPlan,
		_execution: &TimingAtlasExecution,
	) -> Result<TimingAtlasReduction, String> {
		let batches = self
			.last_batches
			.take()
			.ok_or_else(|| "timing atlas child produced no batches".to_string())?;
		let facts = execution_identity_facts(&batches)?;
		let identity_matches = match &self.reference_identity {
			Some(reference) => {
				reference.digest == facts.digest
					&& reference.row_count == facts.row_count
					&& reference.schema_fingerprint == facts.schema_fingerprint
			}
			None => {
				self.reference_identity = Some(facts);
				true
			}
		};
		// Attribution is derived here, strictly after the elapsed boundary, from what the
		// child actually recorded. A completed child whose receipts are missing is an
		// apparatus defect and fails loudly — substituting an empty attribution would let
		// the cell publish receipts it cannot back.
		// The local arm reports no lifecycle spans: its declared boundary starts after
		// binding and it never dispatches, so there is nothing for these clocks to
		// measure. Absent, not zero — a zero would read as "measured and found none".
		let mut sql_planning_ms = None;
		let mut consumer_drain_ms = None;
		let mut lifecycle = toron::coordinator::QueryLifecycleSpans::default();
		let nodes = match plan.arm {
			TimingArm::Local => {
				let (execute_ms, phases, receipt) = self
					.last_local_execution
					.take()
					.ok_or_else(|| "local child completed without executor phases".to_string())?;
				self.last_sql_planning_ms = None;
				self.last_consumer_drain_ms = None;
				vec![timing_atlas_local_task_node(execute_ms, &phases, receipt)?]
			}
			TimingArm::Distributed => {
				let report = self
					.telemetry_retention
					.lock()
					.map_err(|_| "telemetry retention poisoned".to_string())?
					.remove(&plan.query_id)
					.ok_or_else(|| {
						"distributed child completed without a task telemetry report".to_string()
					})?;
				lifecycle = report.lifecycle;
				sql_planning_ms = self.last_sql_planning_ms.take();
				consumer_drain_ms = self.last_consumer_drain_ms.take();
				timing_atlas_nodes_from_report(&report)?
			}
		};
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			nodes,
			sql_planning_ms,
			consumer_drain_ms,
			lifecycle,
		);
		if attribution.is_none() {
			return Err("child receipts yielded no attributable task nodes".to_string());
		}
		Ok(TimingAtlasReduction {
			identity_matches,
			attribution,
		})
	}

	async fn cleanup_child(&mut self, plan: &TimingAtlasChildPlan) -> Result<(), String> {
		// Production completion does not reclaim shuffle state, so the atlas does it
		// explicitly. This also closes any live pacing semaphore and drops manifests,
		// reservations and cancellation state for the namespace.
		for worker in &self.workers {
			toron::distributed::partition_store::PartitionStore::remove_query(
				worker.partition_store.as_ref(),
				plan.query_id,
			);
		}
		Ok(())
	}

	async fn teardown_cell(&mut self, _query: &str) -> Result<(), String> {
		self.final_retained_bytes = Some(self.resident_partition_bytes());
		self.coordinator = None;
		let workers = std::mem::take(&mut self.workers);
		self.peer_closes = shutdown_execution_census_workers(workers).await?;
		Ok(())
	}

	fn peer_closes(&self) -> usize {
		self.peer_closes
	}

	fn now_ms(&mut self) -> f64 {
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|elapsed| elapsed.as_secs_f64() * 1000.0)
			.unwrap_or(0.0)
	}
}

impl CensusTimingAtlasHarness {
	/// Bytes the resident partition stores still hold. Read from the live stores, so an
	/// accumulating namespace shows up rather than being assumed away.
	/// Retained bytes captured immediately after setup, or zero before setup ran.
	pub fn baseline_bytes(&self) -> u64 {
		self.baseline_bytes.unwrap_or(0)
	}

	/// Retained bytes at teardown, before workers were dropped.
	pub fn final_retained_bytes(&self) -> u64 {
		self.final_retained_bytes.unwrap_or(0)
	}

	pub fn resident_partition_bytes(&self) -> u64 {
		self.workers
			.iter()
			.map(|worker| {
				worker
					.partition_store
					.budget_used(toron::distributed::budget::BudgetDimension::ShuffleBytes)
					.get()
			})
			.sum()
	}
}

// ===== ADR 538: the journal =====
//
// Rows are a replayable evidence journal, not formatted output: every verdict the atlas
// reports must be recomputable from the rows alone. The validator below reconstructs each
// one by replaying the file, so a summary that memory believes but the journal cannot
// support fails publication.

pub const TIMING_ATLAS_KIND_HEADER: &str = "artifact_header";
pub const TIMING_ATLAS_KIND_EXCLUSION: &str = "excluded_cell";
pub const TIMING_ATLAS_KIND_CELL_SETUP: &str = "cell_setup";
pub const TIMING_ATLAS_KIND_CHILD: &str = "child_attempt";
pub const TIMING_ATLAS_KIND_PAIR: &str = "pair_outcome";
pub const TIMING_ATLAS_KIND_RESIDENCY: &str = "residency";
pub const TIMING_ATLAS_KIND_CLEANUP: &str = "cleanup";
pub const TIMING_ATLAS_KIND_CELL_SUMMARY: &str = "cell_summary";
/// One receipt-backed overhead component for a cell. Only components carrying a
/// non-overlap or critical-path justification may enter the additive breakdown.
pub const TIMING_ATLAS_KIND_ATTRIBUTION: &str = "attribution_component";
/// One measured child's full attribution receipt: its task nodes, the selected path and
/// the portions along it, replayable without trusting any emitted figure.
pub const TIMING_ATLAS_KIND_ATTRIBUTION_PATH: &str = "attribution_path";
pub const TIMING_ATLAS_KIND_HOST_END: &str = "host_end";

fn json_ratio(ratio: Option<f64>) -> serde_json::Value {
	// Never serialize NaN or infinity: a null ratio is honest, a poisoned float is not.
	match ratio {
		Some(value) if value.is_finite() => serde_json::json!(value),
		_ => serde_json::Value::Null,
	}
}

pub fn timing_atlas_header_row(
	conditions_start: &HostQuiescence,
	dataset_fingerprint: &str,
	scale_factor: f64,
	mode: &str,
) -> serde_json::Value {
	serde_json::json!({
		"kind": TIMING_ATLAS_KIND_HEADER,
		"suite": "tpch_timing_atlas",
		"build_commit": crate::telemetry::build_commit(),
		"binary_modified_unix": crate::telemetry::binary_modified_unix(),
		"scale_factor": scale_factor,
		"mode": mode,
		"dataset_manifest_fingerprint": dataset_fingerprint,
		"measured_pairs": TIMING_ATLAS_MEASURED_PAIRS,
		"warmup_pairs": TIMING_ATLAS_WARMUP_PAIRS,
		"cohort": timing_atlas_cells(),
		"worker_count": EXECUTION_CENSUS_WORKER_COUNT,
		"transport": "tcp_loopback",
		"task_memory_envelope_bytes": EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES,
		"spill_bytes_reserved_per_query": EXECUTION_CENSUS_SPILL_BYTES,
		"host_start_service_inactive": conditions_start.service_inactive,
		"host_start_sql_port_free": conditions_start.sql_port_free,
	})
}

pub fn timing_atlas_exclusion_row(query: &str) -> serde_json::Value {
	serde_json::json!({
		"kind": TIMING_ATLAS_KIND_EXCLUSION,
		"query": query,
		"reason": "fragment_broadcast_probe_not_star_linear",
		"note": "inherited typed capability wall; never attempted, never timed",
	})
}

pub fn timing_atlas_child_row(child: &TimingAtlasChild) -> serde_json::Value {
	serde_json::json!({
		"kind": TIMING_ATLAS_KIND_CHILD,
		"query": child.query,
		"arm": child.arm,
		"pair_index": child.pair_index,
		"position_in_pair": child.position_in_pair,
		"attempt": child.attempt,
		"query_elapsed_ms": child.query_elapsed_ms,
		"row_count": child.row_count,
		"identity_matches": child.identity_matches,
		"valid": child.valid,
		"failure": child.failure,
		"failure_is_io": child.failure_is_io,
	})
}

pub fn timing_atlas_pair_row(query: &str, pair: &TimingAtlasPair) -> serde_json::Value {
	serde_json::json!({
		"kind": TIMING_ATLAS_KIND_PAIR,
		"query": query,
		"pair_index": pair.pair_index,
		"first_arm": pair.first_arm,
		"local_elapsed_ms": pair.local.query_elapsed_ms,
		"distributed_elapsed_ms": pair.distributed.query_elapsed_ms,
		"delta_ms": pair.delta_ms,
		"ratio": json_ratio(pair.ratio),
		"local_attempt": pair.local.attempt,
		"distributed_attempt": pair.distributed.attempt,
	})
}

pub fn timing_atlas_cell_summary_row(outcome: &TimingAtlasCellOutcome) -> serde_json::Value {
	let mut deltas = outcome.paired_deltas();
	let mut ratios = outcome.paired_ratios();
	serde_json::json!({
		"kind": TIMING_ATLAS_KIND_CELL_SUMMARY,
		"query": outcome.query,
		"finalized": outcome.finalized(),
		"pairs": outcome.pairs.len(),
		"invalid_attempts": outcome.invalid_attempts.len(),
		"median_paired_delta_ms": timing_atlas_median(&mut deltas),
		"median_paired_ratio": json_ratio(timing_atlas_median(&mut ratios)),
		"verdict": match outcome.verdict() {
			TimingAtlasVerdict::Distinguished => "distinguished",
			TimingAtlasVerdict::Indistinguishable => "indistinguishable",
		},
		"setup_ms": outcome.setup_ms,
		"teardown_ms": outcome.teardown_ms,
		"verification_ms": outcome.verification_ms,
		"cleanup_ms": outcome.cleanup_ms,
		"peer_closes": outcome.peer_closes,
		"failure": outcome.failure,
	})
}

/// What replaying a journal establishes. Every field is recomputed from rows, never copied
/// from an in-memory summary — a claim the journal cannot support must not be publishable.
#[derive(Debug, Clone, Default)]
pub struct TimingAtlasReplay {
	pub headers: usize,
	pub exclusions: Vec<String>,
	pub cells_seen: BTreeSet<String>,
	pub finalized_cells: BTreeSet<String>,
	pub pairs_per_cell: BTreeMap<String, usize>,
	pub child_identities: usize,
	pub distinct_child_rows: usize,
	pub retried_children: usize,
	pub identity_failures: Vec<String>,
	pub unstable_residency: Vec<String>,
	pub cleanup_failures: Vec<String>,
	pub baseline_violations: Vec<String>,
	pub distinguished_slower: BTreeSet<String>,
	/// Cells carrying at least one receipt-backed attribution component.
	pub cells_with_attribution: BTreeSet<String>,
	/// Components rejected from the additive breakdown for lacking a non-overlap or
	/// critical-path justification. They stay in the journal; they just cannot be summed.
	pub non_additive_components: Vec<String>,
	pub host_end_quiescent: Option<bool>,
	pub terminal_seal: bool,
	pub violations: Vec<String>,
}

impl TimingAtlasReplay {
	/// Everything the journal must show for authority *except* the terminal seal. Used when
	/// deciding what the seal should record: the seal carries the decision, so it cannot be
	/// a precondition of making it.
	pub fn supports_authority(&self, conditions: &TimingAuthorityConditions) -> bool {
		self.withheld_reasons().is_empty() && conditions.granted()
	}

	/// Why the journal itself withholds authority, independent of host conditions. Timing
	/// deltas alone do not discharge ADR 538: the dominance fork is decided from
	/// receipt-backed components, so a journal without them is incomplete evidence rather
	/// than a lighter form of the same result.
	pub fn withheld_reasons(&self) -> Vec<String> {
		let mut reasons = Vec::new();
		if !self.violations.is_empty() {
			reasons.push("replay_violations".to_string());
		}
		if self.headers != 1 {
			reasons.push("header_count".to_string());
		}
		if self.exclusions.len() != 1 {
			reasons.push("exclusion_count".to_string());
		}
		if self.finalized_cells.len() != timing_atlas_cells().len() {
			reasons.push("cohort_incomplete".to_string());
		}
		if self.host_end_quiescent != Some(true) {
			reasons.push("host_not_quiescent_at_seal".to_string());
		}
		let missing = self
			.finalized_cells
			.difference(&self.cells_with_attribution)
			.count();
		if missing > 0 {
			reasons.push(format!("attribution_receipts_missing({missing} cells)"));
		}
		if !self.non_additive_components.is_empty() {
			reasons.push("non_additive_components".to_string());
		}
		reasons
	}

	/// A *published* artifact must additionally carry its terminal seal. This is the check
	/// for reading an artifact back, not for writing one.
	pub fn authoritative(&self, conditions: &TimingAuthorityConditions) -> bool {
		self.supports_authority(conditions) && self.terminal_seal
	}
}

/// Re-derive one attribution-path row from its own nodes. The persisted path, its wall,
/// its portions and its remainder are all recomputed and compared; on success the
/// recomputed portions are returned for the cell-level component checks.
fn timing_atlas_replay_attribution_path(
	row: &serde_json::Value,
) -> Result<BTreeMap<String, f64>, Vec<String>> {
	let mut problems = Vec::new();
	let mut nodes = Vec::new();
	for node in row
		.get("nodes")
		.and_then(|value| value.as_array())
		.map(Vec::as_slice)
		.unwrap_or(&[])
	{
		let stage_id = node.get("stage").and_then(|value| value.as_u64());
		let task_id = node.get("task").and_then(|value| value.as_u64());
		let wall_ms = node.get("wall_ms").and_then(|value| value.as_f64());
		let (Some(stage_id), Some(task_id), Some(wall_ms)) = (stage_id, task_id, wall_ms) else {
			problems.push("node lacks stage, task or wall".to_string());
			continue;
		};
		let mut components = Vec::new();
		if let Some(map) = node.get("components").and_then(|value| value.as_object()) {
			for (label, portion) in map {
				match timing_atlas_component_from_label(label) {
					Some(component) => {
						components.push((component, portion.as_f64().unwrap_or(f64::NAN)))
					}
					None => problems.push(format!("node names unknown component {label}")),
				}
			}
		}
		let mut upstream = Vec::new();
		for edge in node
			.get("upstream")
			.and_then(|value| value.as_array())
			.map(Vec::as_slice)
			.unwrap_or(&[])
		{
			let pair = edge.as_array();
			let stage = pair
				.and_then(|pair| pair.first())
				.and_then(|value| value.as_u64());
			let task = pair
				.and_then(|pair| pair.get(1))
				.and_then(|value| value.as_u64());
			match (stage, task) {
				(Some(stage), Some(task)) => upstream.push((stage as u32, task as u32)),
				_ => problems.push("node carries a malformed upstream edge".to_string()),
			}
		}
		nodes.push(TimingAtlasTaskNode {
			stage_id: stage_id as u32,
			task_id: task_id as u32,
			wall_ms,
			components,
			// Read back, never defaulted: replay re-derives every figure from the
			// nodes, so a zero here would recompute a residual that silently
			// equalled the remainder no matter what the run measured.
			worker_id: node
				.get("worker_id")
				.and_then(|value| value.as_str())
				.unwrap_or_default()
				.to_owned(),
			aggregate_accumulation_ms: node
				.get("aggregate_accumulation_ms")
				.and_then(|value| value.as_f64())
				.unwrap_or(0.0),
			telemetry_lock_ms: node
				.get("telemetry_lock_ms")
				.and_then(|value| value.as_f64())
				.unwrap_or(0.0),
			framework_overhead_ms: node
				.get("framework_overhead_ms")
				.and_then(|value| value.as_f64())
				.unwrap_or(0.0),
			upstream,
		});
	}
	if !problems.is_empty() {
		return Err(problems);
	}
	// The partition claim is per node: a task's eligible portions may not exceed its own
	// wall. Checked on every node, on or off the path — run 4 proved this class hides
	// wherever a cell-level remainder is thick enough to absorb it.
	for node in &nodes {
		let eligible: f64 = node
			.components
			.iter()
			.filter(|(component, _)| {
				component.participates_in_timed_attribution()
					&& !matches!(
						component.local_pairing(),
						TimingAtlasLocalPairing::NoLocalEquivalent
					)
			})
			.map(|(_, portion)| portion)
			.sum();
		if eligible > node.wall_ms + 1e-6 {
			problems.push(format!(
				"node ({}, {}) eligible portions {eligible} exceed its own wall {} — the \
				 phase counters stopped partitioning that task's wall",
				node.stage_id, node.task_id, node.wall_ms
			));
		}
	}
	if !problems.is_empty() {
		return Err(problems);
	}
	let Some(recomputed) = timing_atlas_critical_path(&nodes) else {
		return Err(vec!["path row carries no attributable nodes".to_string()]);
	};

	let persisted_path: Vec<(u32, u32)> = row
		.get("path")
		.and_then(|value| value.as_array())
		.map(Vec::as_slice)
		.unwrap_or(&[])
		.iter()
		.filter_map(|edge| {
			let pair = edge.as_array()?;
			Some((
				pair.first()?.as_u64()? as u32,
				pair.get(1)?.as_u64()? as u32,
			))
		})
		.collect();
	if persisted_path != recomputed.path {
		problems.push(format!(
			"persisted path {persisted_path:?} disagrees with the path re-derived from \
			 its nodes {:?}",
			recomputed.path
		));
	}
	let claimed_wall = row
		.get("path_wall_ms")
		.and_then(|value| value.as_f64())
		.unwrap_or(f64::NAN);
	if !claimed_wall.is_finite() || (claimed_wall - recomputed.total_wall_ms).abs() > 1e-6 {
		problems.push(format!(
			"path wall {claimed_wall} disagrees with recomputed {}",
			recomputed.total_wall_ms
		));
	}
	if let Some(claimed_components) = row
		.get("path_components")
		.and_then(|value| value.as_object())
	{
		for (label, claimed) in claimed_components {
			let claimed = claimed.as_f64().unwrap_or(f64::NAN);
			let recomputed_portion = recomputed.components.get(label).copied().unwrap_or(0.0);
			if !claimed.is_finite() || (claimed - recomputed_portion).abs() > 1e-6 {
				problems.push(format!(
					"path component {label} claims {claimed}, recomputed \
					 {recomputed_portion}"
				));
			}
		}
	}

	// ADR 554 Branch A: the path-level accumulation must equal the sum over the
	// nodes the path actually selected. A path total emitted without node detail
	// cannot be audited — the first fully-wired run reported a correct total with
	// no per-node breakdown, which left one cell's coverage undecidable.
	if let Some(claimed_accumulation) = row
		.get("aggregate_accumulation_ms")
		.and_then(|value| value.as_f64())
	{
		let node_sum: f64 = recomputed
			.path
			.iter()
			.filter_map(|key| nodes.iter().find(|node| node.key() == *key))
			.map(|node| node.aggregate_accumulation_ms)
			.sum();
		if (claimed_accumulation - node_sum).abs() > 1e-6 {
			problems.push(format!(
				"path aggregate accumulation {claimed_accumulation} disagrees with the \
				 selected nodes' sum {node_sum}"
			));
		}
	}

	if let Some(claimed_bookkeeping) = row
		.get("in_task_unattributed_ms")
		.and_then(|value| value.as_f64())
		&& (claimed_bookkeeping - recomputed.in_task_unattributed_ms).abs() > 1e-6
	{
		problems.push(format!(
			"path bookkeeping {claimed_bookkeeping} disagrees with recomputed {}",
			recomputed.in_task_unattributed_ms
		));
	}

	// ADR 554 §3 B2: the three lifecycle spans are caller-owned clocks, not derived
	// from nodes, so replay cannot recompute them. What it can and must check is the
	// arithmetic that binds them — that the declared residual is exactly what the
	// spans leave over. A receipt whose residual does not follow from its own spans
	// is asserting coverage it has not shown.
	if let Some(claimed_residual) = row
		.get("lifecycle_residual_ms")
		.and_then(|value| value.as_f64())
	{
		// Exactly the sequential terms the row subtracts. Envelopes — `consumer_drain_ms`
		// and `dispatch_loop_wall_ms` — are reported raw and never enter this sum.
		let spans = [
			"sql_planning_ms",
			"pre_dispatch_ms",
			"post_dispatch_finalization_ms",
		]
		.iter()
		.filter_map(|key| row.get(*key).and_then(|value| value.as_f64()))
		.sum::<f64>();
		let claimed_unnamed = row
			.get("unnamed_residual_ms")
			.and_then(|value| value.as_f64())
			.unwrap_or(f64::NAN);
		if !claimed_unnamed.is_finite()
			|| ((claimed_unnamed - spans) - claimed_residual).abs() > 1e-6
		{
			problems.push(format!(
				"lifecycle residual {claimed_residual} does not follow from unnamed \
				 residual {claimed_unnamed} less spans {spans}"
			));
		}
		// Arithmetic consistency is not enough. The spans decompose the unnamed gap,
		// so they cannot exceed it: a negative residual means a span is measuring
		// something outside the interval it claims to subdivide. The first B2 run
		// reported drain spans larger than the whole query envelope — arithmetically
		// consistent, semantically nonsense — and only the sign caught it.
		if claimed_residual < -1e-6 {
			problems.push(format!(
				"lifecycle residual {claimed_residual} is negative: spans {spans} exceed \
				 the unnamed residual {claimed_unnamed} they decompose"
			));
		}
	}

	// Reconciliation: eligible portions plus the remainder must cover measured elapsed,
	// and attribution may never exceed it.
	let measured = row
		.get("measured_elapsed_ms")
		.and_then(|value| value.as_f64())
		.unwrap_or(f64::NAN);
	let attributed: f64 = recomputed
		.components
		.iter()
		.filter(|(label, _)| {
			timing_atlas_component_from_label(label).is_some_and(|component| {
				component.participates_in_timed_attribution()
					&& !matches!(
						component.local_pairing(),
						TimingAtlasLocalPairing::NoLocalEquivalent
					)
			})
		})
		.map(|(_, value)| value)
		.sum();
	if !measured.is_finite() {
		problems.push("path row lacks a measured elapsed".to_string());
	} else if attributed - measured > 1e-6 {
		problems.push(format!(
			"attributed {attributed} exceeds measured elapsed {measured}"
		));
	}

	if problems.is_empty() {
		Ok(recomputed.components)
	} else {
		Err(problems)
	}
}

/// Replay a journal and recompute its claims. Verdicts, deltas, ratios and cohort counts are
/// derived here from child rows, so a pair row that disagrees with its children is a
/// violation rather than an accepted summary.
pub fn timing_atlas_replay(journal: &str) -> TimingAtlasReplay {
	let mut replay = TimingAtlasReplay::default();
	let mut children: BTreeMap<(String, i64, i64), Vec<serde_json::Value>> = BTreeMap::new();
	let mut seen_ids: BTreeSet<String> = BTreeSet::new();
	let mut pair_rows: Vec<serde_json::Value> = Vec::new();
	let mut summary_rows: Vec<serde_json::Value> = Vec::new();
	let mut component_rows: Vec<serde_json::Value> = Vec::new();
	let mut path_rows: BTreeMap<(String, i64, String), serde_json::Value> = BTreeMap::new();
	let mut sealed_at: Option<usize> = None;
	let lines = journal.lines().filter(|line| !line.trim().is_empty());

	for (index, line) in lines.enumerate() {
		let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
			replay
				.violations
				.push(format!("row {index} is not valid json"));
			continue;
		};
		if sealed_at.is_some() {
			replay
				.violations
				.push(format!("row {index} follows the terminal seal"));
		}
		let kind = row.get("kind").and_then(|kind| kind.as_str()).unwrap_or("");
		match kind {
			TIMING_ATLAS_KIND_HEADER => replay.headers += 1,
			TIMING_ATLAS_KIND_EXCLUSION => {
				if let Some(query) = row.get("query").and_then(|q| q.as_str()) {
					replay.exclusions.push(query.to_string());
				}
			}
			TIMING_ATLAS_KIND_CELL_SETUP => {
				if let Some(query) = row.get("query").and_then(|q| q.as_str()) {
					replay.cells_seen.insert(query.to_string());
				}
			}
			TIMING_ATLAS_KIND_CHILD => {
				replay.child_identities += 1;
				let query = row
					.get("query")
					.and_then(|q| q.as_str())
					.unwrap_or_default()
					.to_string();
				let attempt = row.get("attempt").and_then(|a| a.as_i64()).unwrap_or(1);
				if attempt > 1 {
					replay.retried_children += 1;
				}
				if row.get("identity_matches") == Some(&serde_json::Value::Bool(false)) {
					replay.identity_failures.push(query.clone());
				}
				let pair_index = row.get("pair_index").and_then(|p| p.as_i64()).unwrap_or(-1);
				children
					.entry((query, pair_index, attempt))
					.or_default()
					.push(row.clone());
				if let Some(id) = row.get("child_id").and_then(|id| id.as_str()) {
					if !seen_ids.insert(id.to_string()) {
						replay
							.violations
							.push(format!("duplicate child identity {id}"));
					}
				}
			}
			TIMING_ATLAS_KIND_PAIR => pair_rows.push(row.clone()),
			TIMING_ATLAS_KIND_RESIDENCY => {
				if row.get("stable") == Some(&serde_json::Value::Bool(false)) {
					if let Some(query) = row.get("query").and_then(|q| q.as_str()) {
						replay.unstable_residency.push(query.to_string());
					}
				}
			}
			TIMING_ATLAS_KIND_CLEANUP => {
				let query = row
					.get("query")
					.and_then(|q| q.as_str())
					.unwrap_or_default()
					.to_string();
				if row.get("succeeded") == Some(&serde_json::Value::Bool(false)) {
					replay.cleanup_failures.push(query.clone());
				}
				if row.get("returned_to_baseline") == Some(&serde_json::Value::Bool(false)) {
					replay.baseline_violations.push(query);
				}
			}
			TIMING_ATLAS_KIND_ATTRIBUTION => {
				let query = row
					.get("query")
					.and_then(|q| q.as_str())
					.unwrap_or_default()
					.to_string();
				let additive = row
					.get("additive")
					.and_then(|value| value.as_bool())
					.unwrap_or(false);
				let eligible = row
					.get("eligible")
					.and_then(|value| value.as_bool())
					.unwrap_or(additive);
				let basis = row
					.get("basis")
					.and_then(|value| value.as_str())
					.unwrap_or("");
				let label = row
					.get("component")
					.and_then(|value| value.as_str())
					.unwrap_or("");
				// Only a non-overlap or critical-path basis licenses addition. A stage
				// duration summed from concurrent tasks measures work, not wall clock.
				if additive && !matches!(basis, "non_overlapping" | "critical_path") {
					replay.non_additive_components.push(format!(
						"{query}: component claims additive with basis {basis}"
					));
				}
				match timing_atlas_component_from_label(label) {
					None => replay.violations.push(format!(
						"{query} attribution names unknown component {label}"
					)),
					Some(component) => {
						if !component.participates_in_timed_attribution() {
							replay.violations.push(format!(
								"{query} {label} is outside the timed boundary and \
								 must not carry an attribution row"
							));
						}
						if matches!(
							component.local_pairing(),
							TimingAtlasLocalPairing::NoLocalEquivalent
						) && (eligible || additive)
						{
							replay.violations.push(format!(
								"{query} {label} has no local equivalent and must not \
								 be eligible or additive"
							));
						}
					}
				}
				component_rows.push(row.clone());
				replay.cells_with_attribution.insert(query);
			}
			TIMING_ATLAS_KIND_ATTRIBUTION_PATH => {
				let query = row
					.get("query")
					.and_then(|q| q.as_str())
					.unwrap_or_default()
					.to_string();
				let arm = row
					.get("arm")
					.and_then(|value| value.as_str())
					.unwrap_or_default()
					.to_string();
				let pair_index = row
					.get("pair_index")
					.and_then(|value| value.as_i64())
					.unwrap_or(-1);
				if path_rows
					.insert((query.clone(), pair_index, arm.clone()), row.clone())
					.is_some()
				{
					replay.violations.push(format!(
						"{query} pair {pair_index} {arm} has more than one attribution path"
					));
				}
			}
			TIMING_ATLAS_KIND_CELL_SUMMARY => summary_rows.push(row.clone()),
			TIMING_ATLAS_KIND_HOST_END => {
				let inactive = row
					.get("service_inactive")
					.and_then(|value| value.as_bool())
					.unwrap_or(false);
				let port_free = row
					.get("sql_port_free")
					.and_then(|value| value.as_bool())
					.unwrap_or(false);
				replay.host_end_quiescent = Some(inactive && port_free);
			}
			TIMING_ATLAS_SEAL_KIND => {
				replay.terminal_seal = true;
				sealed_at = Some(index);
			}
			other => replay
				.violations
				.push(format!("row {index} has unknown kind {other}")),
		}
	}
	replay.distinct_child_rows = seen_ids.len();

	// Recompute each pair from its own children rather than trusting the pair row.
	for pair in &pair_rows {
		let query = pair
			.get("query")
			.and_then(|q| q.as_str())
			.unwrap_or_default()
			.to_string();
		*replay.pairs_per_cell.entry(query.clone()).or_default() += 1;
		let local = pair
			.get("local_elapsed_ms")
			.and_then(|value| value.as_f64())
			.unwrap_or(f64::NAN);
		let distributed = pair
			.get("distributed_elapsed_ms")
			.and_then(|value| value.as_f64())
			.unwrap_or(f64::NAN);
		let claimed_delta = pair
			.get("delta_ms")
			.and_then(|value| value.as_f64())
			.unwrap_or(f64::NAN);
		let recomputed = distributed - local;
		if (recomputed - claimed_delta).abs() > 1e-9 {
			replay.violations.push(format!(
				"{query} pair delta disagrees with its children: claimed {claimed_delta}, \
				 recomputed {recomputed}"
			));
		}
		match pair.get("ratio") {
			Some(serde_json::Value::Null) => {
				if local > 0.0 {
					replay
						.violations
						.push(format!("{query} null ratio with nonzero local arm"));
				}
			}
			Some(value) => {
				let claimed = value.as_f64().unwrap_or(f64::NAN);
				if !claimed.is_finite() {
					replay
						.violations
						.push(format!("{query} ratio is not finite"));
				} else if local > 0.0 && ((distributed / local) - claimed).abs() > 1e-9 {
					replay
						.violations
						.push(format!("{query} ratio disagrees with its children"));
				}
			}
			None => replay
				.violations
				.push(format!("{query} pair row lacks a ratio field")),
		}
	}

	// Recompute cohort, per-cell pair counts and the sign verdict.
	for summary in &summary_rows {
		let query = summary
			.get("query")
			.and_then(|q| q.as_str())
			.unwrap_or_default()
			.to_string();
		let pairs = replay.pairs_per_cell.get(&query).copied().unwrap_or(0);
		if summary.get("finalized") == Some(&serde_json::Value::Bool(true)) {
			if pairs != TIMING_ATLAS_MEASURED_PAIRS {
				replay.violations.push(format!(
					"{query} claims finalized with {pairs} pair rows, expected {}",
					TIMING_ATLAS_MEASURED_PAIRS
				));
				continue;
			}
			replay.finalized_cells.insert(query.clone());
			let deltas = pair_rows
				.iter()
				.filter(|row| row.get("query").and_then(|q| q.as_str()) == Some(query.as_str()))
				.filter_map(|row| row.get("delta_ms").and_then(|value| value.as_f64()))
				.collect::<Vec<_>>();
			let recomputed = timing_atlas_verdict(&deltas);
			let claimed = summary
				.get("verdict")
				.and_then(|value| value.as_str())
				.unwrap_or("");
			let claimed_verdict = match claimed {
				"distinguished" => TimingAtlasVerdict::Distinguished,
				_ => TimingAtlasVerdict::Indistinguishable,
			};
			if recomputed != claimed_verdict {
				replay
					.violations
					.push(format!("{query} verdict disagrees with its pair deltas"));
			}
			// Dominance input: distinguished and distributed-slower.
			let mut sorted = deltas.clone();
			if recomputed == TimingAtlasVerdict::Distinguished
				&& timing_atlas_median(&mut sorted).unwrap_or(0.0) > 0.0
			{
				replay.distinguished_slower.insert(query.clone());
			}
		}
	}

	// Re-derive every attribution path from its own nodes, then recompute each cell's
	// component figures from the re-derived paths. Nothing an emitter wrote is trusted:
	// the persisted path, its portions and every contribution are recomputed here.
	let mut verified_paths: BTreeMap<(String, i64, String), BTreeMap<String, f64>> =
		BTreeMap::new();
	for ((query, pair_index, arm), row) in &path_rows {
		match timing_atlas_replay_attribution_path(row) {
			Ok(components) => {
				verified_paths.insert((query.clone(), *pair_index, arm.clone()), components);
			}
			Err(mut problems) => {
				for problem in problems.drain(..) {
					replay
						.violations
						.push(format!("{query} pair {pair_index} {arm}: {problem}"));
				}
			}
		}
	}
	for row in &component_rows {
		let query = row
			.get("query")
			.and_then(|q| q.as_str())
			.unwrap_or_default()
			.to_string();
		let pairs = replay.pairs_per_cell.get(&query).copied().unwrap_or(0);
		if pairs == 0 {
			continue;
		}
		let label = row
			.get("component")
			.and_then(|value| value.as_str())
			.unwrap_or_default();
		let Some(component) = timing_atlas_component_from_label(label) else {
			continue;
		};
		// A component row is a summary over the cell's paths; a cell that claims component
		// receipts must carry a verified path for both arms of every measured pair.
		let mut contributions = Vec::new();
		let mut complete = true;
		for pair_index in 0..pairs as i64 {
			let local = verified_paths.get(&(query.clone(), pair_index, "local".to_string()));
			let distributed =
				verified_paths.get(&(query.clone(), pair_index, "distributed".to_string()));
			let (Some(local), Some(distributed)) = (local, distributed) else {
				replay.violations.push(format!(
					"{query} claims component receipts but pair {pair_index} lacks a \
					 verified attribution path on both arms"
				));
				complete = false;
				break;
			};
			let local_ms = local
				.get(label)
				.copied()
				.unwrap_or(match component.local_pairing() {
					TimingAtlasLocalPairing::DeclaredZero
					| TimingAtlasLocalPairing::NoLocalEquivalent => 0.0,
					_ => f64::NAN,
				});
			if local_ms.is_nan() {
				replay.violations.push(format!(
					"{query} {label} is required on the local arm but pair {pair_index} \
					 carries no local portion"
				));
				complete = false;
				break;
			}
			let distributed_ms = distributed.get(label).copied().unwrap_or(0.0);
			contributions.push(distributed_ms - local_ms);
		}
		if !complete {
			continue;
		}
		let eligible = row
			.get("eligible")
			.and_then(|value| value.as_bool())
			.unwrap_or(false);
		if eligible {
			let recomputed = timing_atlas_median(&mut contributions).unwrap_or(f64::NAN);
			let claimed = row
				.get("contribution_ms")
				.and_then(|value| value.as_f64())
				.unwrap_or(f64::NAN);
			if !claimed.is_finite() || (claimed - recomputed).abs() > 1e-6 {
				replay.violations.push(format!(
					"{query} {label} contribution disagrees with its paths: claimed \
					 {claimed}, recomputed {recomputed}"
				));
			}
		}
	}

	if replay.exclusions.len() != 1 {
		replay.violations.push(format!(
			"expected exactly one typed exclusion, found {}",
			replay.exclusions.len()
		));
	} else if replay.exclusions[0] != "q15" {
		replay
			.violations
			.push("the excluded cell must be q15".to_string());
	}
	if !replay.identity_failures.is_empty() {
		replay.violations.push(format!(
			"identity failures recorded for {:?}",
			replay.identity_failures
		));
	}
	if !replay.unstable_residency.is_empty() {
		replay.violations.push(format!(
			"unstable worker residency for {:?}",
			replay.unstable_residency
		));
	}
	if !replay.cleanup_failures.is_empty() {
		replay
			.violations
			.push(format!("cleanup failed for {:?}", replay.cleanup_failures));
	}
	if !replay.baseline_violations.is_empty() {
		replay.violations.push(format!(
			"retained bytes did not return to baseline for {:?}",
			replay.baseline_violations
		));
	}
	replay
}

/// Drive the whole atlas: one process, 21 cells, journal written as evidence accrues, then
/// replayed and only then published. The only entry point — there is no implicit path that
/// produces an authoritative artifact.
pub async fn run_timing_atlas(
	mode: &str,
	scale_factor: f64,
	data_root: &Path,
	canonical_path: &Path,
) -> Result<PathBuf, String> {
	// Refuse a destination that already exists, before doing any work.
	let mut artifact = TimingAtlasArtifact::create(canonical_path)?;

	// Host-start quiescence gates the run before the first warmup, not after.
	let host_start = observe_host_quiescence();
	if !host_start.quiescent() {
		artifact.append_row(&serde_json::json!({
			"kind": TIMING_ATLAS_KIND_HEADER,
			"aborted": "host_not_quiescent",
			"host_start_service_inactive": host_start.service_inactive,
			"host_start_sql_port_free": host_start.sql_port_free,
		}))?;
		return Err(
			"timing atlas refuses to start: nilket-local.service active or port 7742 held"
				.to_string(),
		);
	}

	// Dataset identity comes from the spec the cells will actually load.
	// Dataset identity from the export the cells will actually load.
	let dataset_fingerprint = build_persisted_perf_query_spec("q01", mode, scale_factor, data_root)
		.map(|spec| spec.export.manifest_fingerprint.clone())
		.unwrap_or_else(|_| "unknown".to_string());
	artifact.append_row(&timing_atlas_header_row(
		&host_start,
		&dataset_fingerprint,
		scale_factor,
		mode,
	))?;
	for excluded in TIMING_ATLAS_EXCLUDED_CELLS {
		artifact.append_row(&timing_atlas_exclusion_row(excluded))?;
	}

	let mut finalized = 0usize;
	for query in timing_atlas_cells() {
		// Scratch is cleared per cell, before its warmup pair.
		let _ = std::fs::remove_dir_all(data_root.parent().unwrap_or(data_root).join("join_spill"));
		artifact.append_row(&serde_json::json!({
			"kind": TIMING_ATLAS_KIND_CELL_SETUP, "query": query
		}))?;

		let mut harness =
			CensusTimingAtlasHarness::prepare(query, mode, scale_factor, data_root).await?;
		// The runner owns the cell lifecycle: setting up here as well would construct a
		// second set of workers and consume the catalog twice.
		// Boxed: the per-cell future is large, and twenty-one of them inline in one driver
		// future would live on the stack.
		let outcome = Box::pin(run_timing_atlas_cell_with(&mut harness, query)).await;
		let baseline_after_setup = harness.baseline_bytes();
		let retained = harness.final_retained_bytes();

		for child in outcome
			.warmups
			.iter()
			.chain(outcome.invalid_attempts.iter())
		{
			artifact.append_row(&timing_atlas_child_row(child))?;
		}
		for pair in &outcome.pairs {
			artifact.append_row(&timing_atlas_child_row(&pair.local))?;
			artifact.append_row(&timing_atlas_child_row(&pair.distributed))?;
			artifact.append_row(&timing_atlas_pair_row(query, pair))?;
			for child in [&pair.local, &pair.distributed] {
				if let Some(attribution) = child.attribution.as_ref() {
					artifact.append_row(&timing_atlas_attribution_path_row(
						query,
						child.arm,
						pair.pair_index,
						child.attempt,
						child.query_elapsed_ms,
						attribution,
					))?;
				}
			}
		}
		for component_row in timing_atlas_component_rows(query, &outcome.pairs) {
			artifact.append_row(&component_row)?;
		}
		artifact.append_row(&serde_json::json!({
			"kind": TIMING_ATLAS_KIND_RESIDENCY,
			"query": query,
			"observations": harness.residency.len(),
			"stable": harness.residency.stable(),
		}))?;
		artifact.append_row(&serde_json::json!({
			"kind": TIMING_ATLAS_KIND_CLEANUP,
			"query": query,
			"succeeded": !outcome.cleanup_failed,
			"returned_to_baseline": retained == baseline_after_setup,
			"baseline_bytes": baseline_after_setup,
			"retained_bytes": retained,
			"cleanup_ms": outcome.cleanup_ms,
		}))?;
		artifact.append_row(&timing_atlas_cell_summary_row(&outcome))?;
		if outcome.finalized() {
			finalized += 1;
		}
	}

	let host_end = observe_host_quiescence();
	artifact.append_row(&serde_json::json!({
		"kind": TIMING_ATLAS_KIND_HOST_END,
		"service_inactive": host_end.service_inactive,
		"sql_port_free": host_end.sql_port_free,
	}))?;

	let conditions = TimingAuthorityConditions {
		clean_build: !crate::telemetry::build_commit().contains("dirty"),
		full_hash_dataset: dataset_fingerprint != "unknown",
		quiescent_host: timing_atlas_quiescent_throughout(&host_start, &host_end),
		protocol_unmodified: true,
		complete_cohort: finalized == timing_atlas_cells().len(),
	};

	// Replay what was written rather than trusting what was remembered.
	let journal = std::fs::read_to_string(artifact.partial_path())
		.map_err(|error| format!("timing atlas reread: {error}"))?;
	let replay = timing_atlas_replay(&journal);
	// The seal has not been written yet, so authority is decided on everything else the
	// journal must show; reading the artifact back later checks the seal too.
	let authoritative = replay.supports_authority(&conditions);

	artifact.append_seal(&serde_json::json!({
		"kind": TIMING_ATLAS_SEAL_KIND,
		"timing_authority": authoritative,
		"withheld_reasons": conditions.withheld_reasons(),
		"replay_violations": replay.violations,
		"finalized_cells": replay.finalized_cells.len(),
		"distinguished_slower_cells": replay.distinguished_slower.len(),
	}))?;

	if !authoritative {
		return Err(format!(
			"timing atlas withheld authority; partial retained. reasons={:?} violations={:?}",
			conditions.withheld_reasons(),
			replay.violations
		));
	}
	artifact.publish()
}

/// The frozen component taxonomy. Shared by both arms so a contribution always compares
/// like with like, and kept deliberately granular: a `stage` component bracketing several
/// concurrent operators would bound where cost sits, not what it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimingAtlasComponent {
	Scan,
	Operator,
	Shuffle,
	Dispatch,
	Transport,
	Reduction,
}

impl TimingAtlasComponent {
	pub fn label(self) -> &'static str {
		match self {
			Self::Scan => "scan",
			Self::Operator => "operator",
			Self::Shuffle => "shuffle",
			Self::Dispatch => "dispatch",
			Self::Transport => "transport",
			Self::Reduction => "reduction",
		}
	}

	pub const ALL: [Self; 6] = [
		Self::Scan,
		Self::Operator,
		Self::Shuffle,
		Self::Dispatch,
		Self::Transport,
		Self::Reduction,
	];

	/// How this component's local side must be supplied. Three distinct states, because
	/// collapsing them is exactly how a zero gets invented or a real gap gets reported as a
	/// defect.
	pub fn local_pairing(self) -> TimingAtlasLocalPairing {
		match self {
			// The local arm genuinely does this work; a missing receipt is a defect.
			// `shuffle` belongs here on evidence: twenty of twenty-one local plans carry an
			// `Exchange` and repartition in memory.
			Self::Scan | Self::Operator | Self::Shuffle => TimingAtlasLocalPairing::Required,
			// The local arm structurally cannot incur it, so zero is the true value.
			Self::Transport => TimingAtlasLocalPairing::DeclaredZero,
			// No semantically equivalent local interval exists. Retained raw as evidence,
			// but never paired and never allowed to lead — a distributed-only figure is not
			// a contribution.
			Self::Dispatch => TimingAtlasLocalPairing::NoLocalEquivalent,
			// After the elapsed boundary; not a timed component at all.
			Self::Reduction => TimingAtlasLocalPairing::NotTimed,
		}
	}

	pub fn local_counterpart_is_defined_zero(self) -> bool {
		matches!(self.local_pairing(), TimingAtlasLocalPairing::DeclaredZero)
	}

	/// Whether this component participates in timed reconciliation and dominance.
	/// `reduction` happens after the elapsed boundary by construction, so it is metadata
	/// rather than a contribution — and must not be given a fabricated in-bound zero.
	pub fn participates_in_timed_attribution(self) -> bool {
		!matches!(self, Self::Reduction)
	}
}

/// How a component's local side is supplied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingAtlasLocalPairing {
	/// Both arms must emit a receipt; a missing local side is a defect.
	Required,
	/// The local arm structurally cannot incur it, so zero is the true value.
	DeclaredZero,
	/// No semantically equivalent local interval exists. Kept as raw evidence, excluded
	/// from paired contributions and from dominance.
	NoLocalEquivalent,
	/// Outside the elapsed boundary entirely.
	NotTimed,
}

impl TimingAtlasLocalPairing {
	pub fn label(self) -> &'static str {
		match self {
			Self::Required => "paired",
			Self::DeclaredZero => "declared_zero_local",
			Self::NoLocalEquivalent => "no_local_equivalent",
			Self::NotTimed => "not_timed",
		}
	}
}

/// Why an interval may be added. A sum of concurrent task durations is not a basis: it
/// measures work, not wall clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingAtlasBasis {
	NonOverlapping,
	CriticalPath,
	/// Retained as evidence, never added.
	ConcurrentSpan,
}

impl TimingAtlasBasis {
	pub fn label(self) -> &'static str {
		match self {
			Self::NonOverlapping => "non_overlapping",
			Self::CriticalPath => "critical_path",
			Self::ConcurrentSpan => "concurrent_span",
		}
	}

	pub fn admissible(self) -> bool {
		matches!(self, Self::NonOverlapping | Self::CriticalPath)
	}
}

/// One component measurement on one arm of one child.
#[derive(Debug, Clone)]
pub struct TimingAtlasComponentReceipt {
	pub component: TimingAtlasComponent,
	pub scope: String,
	pub arm: TimingArm,
	pub child_query_id: QueryId,
	pub raw_elapsed_ms: f64,
	pub basis: TimingAtlasBasis,
}

/// A component's paired contribution, formed only from a local/distributed pair of receipts
/// or from a component whose local counterpart is a declared zero.
#[derive(Debug, Clone, PartialEq)]
pub struct TimingAtlasContribution {
	pub component: TimingAtlasComponent,
	pub local_ms: f64,
	pub distributed_ms: f64,
	pub contribution_ms: f64,
	pub basis: TimingAtlasBasis,
	pub admissible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimingAtlasPairingError {
	MissingLocalCounterpart(TimingAtlasComponent),
	MissingDistributedReceipt(TimingAtlasComponent),
	BasisDisagreement(TimingAtlasComponent),
}

/// Pair receipts into contributions. A distributed receipt with no local counterpart is an
/// error unless the taxonomy declares that counterpart zero — silently substituting zero
/// would let an unimplemented probe claim the whole delta.
pub fn timing_atlas_contributions(
	receipts: &[TimingAtlasComponentReceipt],
) -> Result<Vec<TimingAtlasContribution>, TimingAtlasPairingError> {
	let mut contributions = Vec::new();
	for component in TimingAtlasComponent::ALL {
		if !component.participates_in_timed_attribution() {
			continue;
		}
		let local = receipts
			.iter()
			.find(|receipt| receipt.component == component && receipt.arm == TimingArm::Local);
		let distributed = receipts.iter().find(|receipt| {
			receipt.component == component && receipt.arm == TimingArm::Distributed
		});
		let Some(distributed) = distributed else {
			if local.is_some() {
				return Err(TimingAtlasPairingError::MissingDistributedReceipt(
					component,
				));
			}
			continue;
		};
		let local_ms = match (local, component.local_pairing()) {
			(Some(local), _) => {
				if local.basis != distributed.basis {
					return Err(TimingAtlasPairingError::BasisDisagreement(component));
				}
				local.raw_elapsed_ms
			}
			(None, TimingAtlasLocalPairing::DeclaredZero) => 0.0,
			// Retained raw elsewhere; never a contribution, so it cannot lead a cell.
			(None, TimingAtlasLocalPairing::NoLocalEquivalent) => continue,
			(None, TimingAtlasLocalPairing::NotTimed) => continue,
			(None, TimingAtlasLocalPairing::Required) => {
				return Err(TimingAtlasPairingError::MissingLocalCounterpart(component));
			}
		};
		contributions.push(TimingAtlasContribution {
			component,
			local_ms,
			distributed_ms: distributed.raw_elapsed_ms,
			contribution_ms: distributed.raw_elapsed_ms - local_ms,
			basis: distributed.basis,
			admissible: distributed.basis.admissible(),
		});
	}
	Ok(contributions)
}

/// Whether one arm's admissible components plus its unattributed remainder reconcile to that
/// arm's measured elapsed.
pub fn timing_atlas_arm_reconciles(
	receipts: &[TimingAtlasComponentReceipt],
	arm: TimingArm,
	measured_elapsed_ms: f64,
	unattributed_ms: f64,
) -> bool {
	let attributed: f64 = receipts
		.iter()
		.filter(|receipt| receipt.arm == arm && receipt.basis.admissible())
		.map(|receipt| receipt.raw_elapsed_ms)
		.sum();
	(attributed + unattributed_ms - measured_elapsed_ms).abs() < 1e-6
}

/// The leading component for a cell: at least half the cell's median paired delta, counted
/// only from admissible paired contributions.
pub fn timing_atlas_leading_component(
	contributions: &[TimingAtlasContribution],
	median_paired_delta_ms: f64,
) -> Option<TimingAtlasComponent> {
	if median_paired_delta_ms <= 0.0 {
		return None;
	}
	contributions
		.iter()
		.filter(|contribution| contribution.admissible)
		.find(|contribution| contribution.contribution_ms >= 0.5 * median_paired_delta_ms)
		.map(|contribution| contribution.component)
}

/// One task in the stage DAG, with its wall clock partitioned into component portions.
/// The partition is `non_overlapping` *within this task* — that is what the phase counters
/// establish — and says nothing about tasks running beside it.
#[derive(Debug, Clone)]
pub struct TimingAtlasTaskNode {
	pub stage_id: u32,
	pub task_id: u32,
	/// The task's own wall. A **non-additive envelope**: it contains the
	/// components and the bookkeeping below, so it never enters a subtraction
	/// against `remainder_ms` (ADR 551 follow-up).
	pub wall_ms: f64,
	pub components: Vec<(TimingAtlasComponent, f64)>,
	/// In-task time no component claims, kept as two counters because they are
	/// different in kind. `telemetry_lock` is a measured activity;
	/// `framework_overhead` is a **residual the worker computes** —
	/// `task_wall - accounted` in `server/mod.rs` — so it says the time was
	/// inside a task, not what the task was doing. Naming is not explaining.
	/// `task_wall_micros` is deliberately absent: it is the envelope, not a term.
	pub telemetry_lock_ms: f64,
	pub framework_overhead_ms: f64,
	/// ADR 554 Branch A: in-task time **named** as aggregate accumulation.
	///
	/// Reported beside the unnamed in-task terms, never inside them. The engine
	/// already subtracts it from `framework_overhead`, so omitting it here does
	/// not hide it — it relocates it into the unnamed residual, which is worse
	/// than leaving it unnamed. The first Branch A run did exactly that.
	pub aggregate_accumulation_ms: f64,
	/// The worker this task ran on, as its UUID string. Present so an artifact can
	/// answer whether concurrent stage siblings actually landed on different
	/// workers — the question ADR 554 Branch B could only answer "consistent
	/// with" until now. Empty on an arm that runs no distributed task.
	pub worker_id: String,
	/// Immediate predecessors, as (stage, task).
	pub upstream: Vec<(u32, u32)>,
}

impl TimingAtlasTaskNode {
	pub fn key(&self) -> (u32, u32) {
		(self.stage_id, self.task_id)
	}
}

/// The selected path and the components attributable along it. Persisted so replay can walk
/// every edge and re-derive every figure.
#[derive(Debug, Clone, PartialEq)]
pub struct TimingAtlasCriticalPath {
	pub path: Vec<(u32, u32)>,
	pub total_wall_ms: f64,
	pub components: BTreeMap<String, f64>,
	/// Named aggregate accumulation summed over the same path nodes.
	pub aggregate_accumulation_ms: f64,
	/// In-task unattributed time summed over **the same path nodes** the
	/// components are summed over. Summing it over a different node set would
	/// make the residual incoherent with the components it sits beside.
	pub in_task_unattributed_ms: f64,
}

/// Derive a per-cell figure by critical path through the stage DAG.
///
/// The rule that matters: **one path is selected by total wall duration, and only that
/// path's component portions are attributed.** Taking each component's maximum across tasks
/// independently and summing those maxima would recreate exactly the cross-task
/// overcounting the task-scope partition was meant to avoid — it would describe a task that
/// never existed.
pub fn timing_atlas_critical_path(
	nodes: &[TimingAtlasTaskNode],
) -> Option<TimingAtlasCriticalPath> {
	if nodes.is_empty() {
		return None;
	}
	let index: BTreeMap<(u32, u32), &TimingAtlasTaskNode> =
		nodes.iter().map(|node| (node.key(), node)).collect();

	// Longest-wall path ending at each node, memoized over the DAG.
	fn best_path(
		key: (u32, u32),
		index: &BTreeMap<(u32, u32), &TimingAtlasTaskNode>,
		memo: &mut BTreeMap<(u32, u32), (f64, Vec<(u32, u32)>)>,
		visiting: &mut BTreeSet<(u32, u32)>,
	) -> (f64, Vec<(u32, u32)>) {
		if let Some(found) = memo.get(&key) {
			return found.clone();
		}
		// A cycle is not a DAG; refuse rather than loop.
		if !visiting.insert(key) {
			return (0.0, Vec::new());
		}
		let Some(node) = index.get(&key) else {
			visiting.remove(&key);
			return (0.0, Vec::new());
		};
		let mut best = (0.0_f64, Vec::new());
		for upstream in &node.upstream {
			let candidate = best_path(*upstream, index, memo, visiting);
			if candidate.0 > best.0 {
				best = candidate;
			}
		}
		let mut path = best.1;
		path.push(key);
		let total = best.0 + node.wall_ms;
		visiting.remove(&key);
		memo.insert(key, (total, path.clone()));
		(total, path)
	}

	let mut memo = BTreeMap::new();
	let mut selected: Option<(f64, Vec<(u32, u32)>)> = None;
	for node in nodes {
		let mut visiting = BTreeSet::new();
		let candidate = best_path(node.key(), &index, &mut memo, &mut visiting);
		if selected
			.as_ref()
			.is_none_or(|current| candidate.0 > current.0)
		{
			selected = Some(candidate);
		}
	}
	let (total_wall_ms, path) = selected?;

	// Components come only from nodes on the selected path.
	let mut components: BTreeMap<String, f64> = BTreeMap::new();
	let mut in_task_unattributed_ms = 0.0_f64;
	let mut aggregate_accumulation_ms = 0.0_f64;
	for key in &path {
		let Some(node) = index.get(key) else { continue };
		for (component, portion) in &node.components {
			*components
				.entry(component.label().to_string())
				.or_insert(0.0) += portion;
		}
		in_task_unattributed_ms += node.telemetry_lock_ms + node.framework_overhead_ms;
		aggregate_accumulation_ms += node.aggregate_accumulation_ms;
	}
	Some(TimingAtlasCriticalPath {
		path,
		total_wall_ms,
		components,
		in_task_unattributed_ms,
		aggregate_accumulation_ms,
	})
}

/// A component receipt bound to an exact task in the stage DAG. Eligibility is a claim the
/// validator checks, never a property the emitter can assert on its own.
#[derive(Debug, Clone)]
pub struct TimingAtlasPathBoundReceipt {
	pub component: TimingAtlasComponent,
	pub arm: TimingArm,
	pub stage_id: u32,
	pub task_id: u32,
	pub scope: String,
	pub basis: TimingAtlasBasis,
	pub raw_elapsed_ms: f64,
	/// What the emitter claimed. Recomputed and compared; never trusted.
	pub emitted_contribution_ms: Option<f64>,
	pub eligible: bool,
}

impl TimingAtlasPathBoundReceipt {
	pub fn node(&self) -> (u32, u32) {
		(self.stage_id, self.task_id)
	}
}

#[derive(Debug, Clone, Default)]
pub struct TimingAtlasPathValidation {
	pub violations: Vec<String>,
	/// Recomputed from raw values on the selected path — never copied from an emitted figure.
	pub contributions: Vec<TimingAtlasContribution>,
	pub off_path_retained: usize,
	pub selected_wall_ms: f64,
	pub remainder_ms: f64,
}

impl TimingAtlasPathValidation {
	pub fn dominance_allowed(&self) -> bool {
		self.violations.is_empty()
	}
}

/// Validate receipts against the persisted path, then recompute every figure from raw
/// values. An eligible receipt from a task that was not on the selected path is rejected:
/// otherwise a component from a road not taken could lead a cell.
pub fn timing_atlas_validate_path_bound_receipts(
	nodes: &[TimingAtlasTaskNode],
	path: &[(u32, u32)],
	receipts: &[TimingAtlasPathBoundReceipt],
	measured_elapsed_ms: f64,
) -> TimingAtlasPathValidation {
	let mut validation = TimingAtlasPathValidation::default();
	let index: BTreeMap<(u32, u32), &TimingAtlasTaskNode> =
		nodes.iter().map(|node| (node.key(), node)).collect();
	let on_path: BTreeSet<(u32, u32)> = path.iter().copied().collect();

	if path.is_empty() {
		validation
			.violations
			.push("no selected path was persisted".to_string());
		return validation;
	}

	for window in path.windows(2) {
		let Some(successor) = index.get(&window[1]) else {
			validation
				.violations
				.push(format!("path node {:?} is not in the graph", window[1]));
			continue;
		};
		if !successor.upstream.contains(&window[0]) {
			validation.violations.push(format!(
				"persisted edge {:?} -> {:?} is not an edge in the stage graph",
				window[0], window[1]
			));
		}
	}

	validation.selected_wall_ms = path
		.iter()
		.filter_map(|key| index.get(key))
		.map(|node| node.wall_ms)
		.sum();

	let mut eligible: Vec<&TimingAtlasPathBoundReceipt> = Vec::new();
	for receipt in receipts {
		if !receipt.eligible {
			validation.off_path_retained += 1;
			continue;
		}
		if !on_path.contains(&receipt.node()) {
			validation.violations.push(format!(
				"{} receipt claims eligibility from task {:?}, which is not on the selected path",
				receipt.component.label(),
				receipt.node()
			));
			continue;
		}
		if !receipt.basis.admissible() {
			validation.violations.push(format!(
				"{} receipt claims eligibility with inadmissible basis {}",
				receipt.component.label(),
				receipt.basis.label()
			));
			continue;
		}
		eligible.push(receipt);
	}

	let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
	for receipt in &eligible {
		if !seen.insert((receipt.component.label(), receipt.arm.label())) {
			validation.violations.push(format!(
				"{} has more than one eligible {} receipt",
				receipt.component.label(),
				receipt.arm.label()
			));
		}
	}

	for component in TimingAtlasComponent::ALL {
		let local = eligible
			.iter()
			.find(|receipt| receipt.component == component && receipt.arm == TimingArm::Local);
		let distributed = eligible.iter().find(|receipt| {
			receipt.component == component && receipt.arm == TimingArm::Distributed
		});
		let Some(distributed) = distributed else {
			continue;
		};
		let local_ms = match (local, component.local_pairing()) {
			(Some(local), _) => {
				if local.basis != distributed.basis {
					validation
						.violations
						.push(format!("{} basis disagrees across arms", component.label()));
					continue;
				}
				if local.scope != distributed.scope {
					validation.violations.push(format!(
						"{} scope disagrees across arms: {} vs {}",
						component.label(),
						local.scope,
						distributed.scope
					));
					continue;
				}
				local.raw_elapsed_ms
			}
			(None, TimingAtlasLocalPairing::DeclaredZero) => 0.0,
			(None, TimingAtlasLocalPairing::NoLocalEquivalent) => {
				validation.violations.push(format!(
					"{} has no local equivalent and must not be eligible",
					component.label()
				));
				continue;
			}
			(None, TimingAtlasLocalPairing::NotTimed) => continue,
			(None, TimingAtlasLocalPairing::Required) => {
				validation.violations.push(format!(
					"{} is missing its local counterpart",
					component.label()
				));
				continue;
			}
		};
		let recomputed = distributed.raw_elapsed_ms - local_ms;
		if let Some(claimed) = distributed.emitted_contribution_ms
			&& (claimed - recomputed).abs() > 1e-9
		{
			validation.violations.push(format!(
				"{} emitted contribution {claimed} disagrees with recomputed {recomputed}",
				component.label()
			));
		}
		validation.contributions.push(TimingAtlasContribution {
			component,
			local_ms,
			distributed_ms: distributed.raw_elapsed_ms,
			contribution_ms: recomputed,
			basis: distributed.basis,
			admissible: true,
		});
	}

	let attributed: f64 = validation
		.contributions
		.iter()
		.map(|contribution| contribution.distributed_ms)
		.sum();
	validation.remainder_ms = measured_elapsed_ms - attributed;
	if validation.remainder_ms < -1e-9 {
		validation.violations.push(format!(
			"attributed {attributed} exceeds measured {measured_elapsed_ms}"
		));
	}
	validation
}

// ===== ADR 538: phase-to-component wiring =====
//
// The one place phase counters become component portions. Everything downstream — task
// nodes, critical paths, receipts, replay — consumes this mapping, so a wrong answer here
// is the one defect internal-consistency checks cannot catch. The mapping is therefore
// deliberately total over the counters it claims: a counter is either mapped to exactly one
// component, or named in `TIMING_ATLAS_UNATTRIBUTED_PHASES` as remainder.

/// Phase counters that stay in the unattributed remainder: bookkeeping, the residual the
/// worker computes, and the task wall itself.
pub const TIMING_ATLAS_UNATTRIBUTED_PHASES: [&str; 3] = [
	"telemetry_lock_micros",
	"framework_overhead_micros",
	"task_wall_micros",
];

/// The frozen phase → component mapping (operator-set):
/// `operator` = join build/probe/spill; `shuffle` = exchange repartition plus shuffle
/// pull/publish; `transport` = encode/send/decode; `dispatch` = wait/setup, retained raw
/// and never eligible; `scan` = the scan interval. Every timed component is present in the
/// result even at zero — presence is what distinguishes an observed zero from a missing
/// receipt.
pub fn timing_atlas_component_portions_ms(
	phases: &TaskTelemetryPhases,
) -> Vec<(TimingAtlasComponent, f64)> {
	let ms = |micros: u64| micros as f64 / 1000.0;
	vec![
		(TimingAtlasComponent::Scan, ms(phases.scan_micros)),
		(
			TimingAtlasComponent::Operator,
			ms(phases
				.build_micros
				.saturating_add(phases.probe_micros)
				.saturating_add(phases.spill_micros)),
		),
		(
			TimingAtlasComponent::Shuffle,
			ms(phases
				.exchange_repartition_micros
				.saturating_add(phases.shuffle_pull_decode_micros)
				.saturating_add(phases.output_publish_micros)),
		),
		(
			TimingAtlasComponent::Dispatch,
			ms(phases
				.dispatch_wait_micros
				.saturating_add(phases.task_setup_micros)),
		),
		(
			TimingAtlasComponent::Transport,
			ms(phases
				.batch_encode_micros
				.saturating_add(phases.batch_send_micros)
				.saturating_add(phases.batch_decode_micros)),
		),
	]
}

pub fn timing_atlas_component_from_label(label: &str) -> Option<TimingAtlasComponent> {
	TimingAtlasComponent::ALL
		.into_iter()
		.find(|component| component.label() == label)
}

/// The `scope` a component's receipts carry. `operator` is scoped to joins for this ADR:
/// build, probe and spill are the only captured operator phases, and labelling that
/// fraction as generic operator time would let it compete in the dominance test as though
/// it were the whole.
pub fn timing_atlas_component_scope(component: TimingAtlasComponent) -> &'static str {
	match component {
		TimingAtlasComponent::Scan => "scan",
		TimingAtlasComponent::Operator => "join",
		TimingAtlasComponent::Shuffle => "exchange",
		TimingAtlasComponent::Dispatch => "scheduler",
		TimingAtlasComponent::Transport => "wire",
		TimingAtlasComponent::Reduction => "out_of_boundary",
	}
}

/// One child's derived attribution: the task nodes it actually ran, the selected path, and
/// the component portions along that path. Built from actual child receipts after the
/// elapsed boundary, never from planned stage shapes.
#[derive(Debug, Clone)]
pub struct TimingAtlasChildAttribution {
	pub nodes: Vec<TimingAtlasTaskNode>,
	pub path: Vec<(u32, u32)>,
	pub path_wall_ms: f64,
	pub path_components: BTreeMap<String, f64>,
	/// Named aggregate accumulation along the path. Reporting only: a remainder
	/// subterm, never an ADR 538 component (§2).
	pub aggregate_accumulation_ms: f64,
	/// In-task unattributed time along the path. Reporting only: it is not a
	/// component, is not eligible, and never enters `attributed_ms`.
	pub in_task_unattributed_ms: f64,
	/// ADR 554 §3 B2 lifecycle spans. Reporting only, never components, and never
	/// summed into one number: a single lifecycle figure is the opaque bucket that
	/// record exists to avoid. `sql_planning_ms` is `None` on an arm whose caller
	/// did not time it — absent, not zero.
	pub sql_planning_ms: Option<f64>,
	pub pre_dispatch_ms: f64,
	/// Envelope over every task dispatch window, not a term beside them.
	pub dispatch_loop_wall_ms: f64,
	/// Producer finalization, ending when the coordinator stops producing.
	pub post_dispatch_finalization_ms: f64,
	/// True drain, ending at consumer exhaustion. Harness-owned: no
	/// coordinator-side clock can see this boundary. `None` on an arm that
	/// does not stream.
	pub consumer_drain_ms: Option<f64>,
}

impl TimingAtlasChildAttribution {
	/// The sum a receipt may claim against measured elapsed: eligible components only.
	/// `dispatch` is retained in the map as raw evidence but has no local equivalent, so it
	/// never enters the attributed figure.
	pub fn attributed_ms(&self) -> f64 {
		self.path_components
			.iter()
			.filter(|(label, _)| {
				timing_atlas_component_from_label(label).is_some_and(|component| {
					component.participates_in_timed_attribution()
						&& !matches!(
							component.local_pairing(),
							TimingAtlasLocalPairing::NoLocalEquivalent
						)
				})
			})
			.map(|(_, value)| value)
			.sum()
	}
}

/// Build task nodes from a distributed run's retained report. Node identity comes from the
/// tasks that actually completed; stage-level upstream edges expand to task-level edges
/// against the tasks the report actually contains. Where a task retried, the last attempt
/// wins — it is the attempt whose output was accepted.
/// Placement facts for one measured attempt, split into the two classes the
/// placement path actually distinguishes (ADR 555 §7).
///
/// A single combined figure would credit a placement change for structure it
/// never touched: shuffle affinity co-locates a lone consumer with its single
/// producer by design, and that is correct in **both** arms.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlacementFacts {
	/// Multi-task stages. Affinity declines for these because a producer stage
	/// has more than one task, so the fallback policy decides.
	pub sibling_groups: usize,
	pub sibling_groups_spread: usize,
	/// A lone consumer with exactly one upstream task: affinity's own case.
	pub affinity_links: usize,
	pub affinity_links_colocated: usize,
}

/// Classify one attempt's task graph. Pure, so the gate below is testable
/// without a run.
pub fn placement_facts_from_nodes(nodes: &[TimingAtlasTaskNode]) -> PlacementFacts {
	let mut facts = PlacementFacts::default();
	let mut by_stage: BTreeMap<u32, Vec<&TimingAtlasTaskNode>> = BTreeMap::new();
	for node in nodes {
		by_stage.entry(node.stage_id).or_default().push(node);
	}
	for stage_nodes in by_stage.values() {
		if stage_nodes.len() > 1 {
			facts.sibling_groups += 1;
			let distinct: BTreeSet<&str> = stage_nodes
				.iter()
				.map(|node| node.worker_id.as_str())
				.collect();
			if distinct.len() > 1 {
				facts.sibling_groups_spread += 1;
			}
			continue;
		}
		let node = stage_nodes[0];
		if node.upstream.len() != 1 {
			continue;
		}
		facts.affinity_links += 1;
		let (stage, task) = node.upstream[0];
		if nodes
			.iter()
			.find(|candidate| candidate.stage_id == stage && candidate.task_id == task)
			.is_some_and(|producer| producer.worker_id == node.worker_id)
		{
			facts.affinity_links_colocated += 1;
		}
	}
	facts
}

/// Reject an attempt whose placement does not match the arm that produced it
/// (ADR 555 §7).
///
/// The seal consumes this: an artifact exposes **no** paired timing verdict
/// unless every relevant pair passes. A timing comparison between two arms that
/// did not actually place differently would measure noise and read as a result.
pub fn placement_gate_problems(
	query: &str,
	pair_index: usize,
	affinity_only: PlacementFacts,
	load_observing: PlacementFacts,
) -> Vec<String> {
	let mut problems = Vec::new();
	let cell = format!("{query} pair {pair_index}");

	// The baseline arm must serialize every fallback-eligible group.
	if affinity_only.sibling_groups_spread != 0 {
		problems.push(format!(
			"{cell} affinity-only arm spread {} of {} sibling groups; the baseline must co-locate them",
			affinity_only.sibling_groups_spread, affinity_only.sibling_groups
		));
	}
	// The treatment arm must spread every one of them, or the arms did not differ.
	if load_observing.sibling_groups_spread != load_observing.sibling_groups {
		problems.push(format!(
			"{cell} load-observing arm spread {} of {} sibling groups; the treatment must spread them",
			load_observing.sibling_groups_spread, load_observing.sibling_groups
		));
	}
	// Locality is preserved in both arms, or the change traded it for balancing.
	for (arm, facts) in [
		("affinity-only", affinity_only),
		("load-observing", load_observing),
	] {
		if facts.affinity_links_colocated != facts.affinity_links {
			problems.push(format!(
				"{cell} {arm} arm co-located {} of {} single-producer links; affinity must keep winning",
				facts.affinity_links_colocated, facts.affinity_links
			));
		}
	}
	// Identical plan structure, or the arms are not comparable at all.
	if affinity_only.sibling_groups != load_observing.sibling_groups
		|| affinity_only.affinity_links != load_observing.affinity_links
	{
		problems.push(format!(
			"{cell} arms saw different plan structure: affinity-only {affinity_only:?} vs load-observing {load_observing:?}"
		));
	}
	problems
}

pub fn timing_atlas_nodes_from_report(
	report: &toron::coordinator::QueryTaskTelemetryReport,
) -> Result<Vec<TimingAtlasTaskNode>, String> {
	use std::collections::btree_map::Entry;
	let mut latest: BTreeMap<(u32, u32), &toron::distributed::scheduler::TaskTelemetryRecord> =
		BTreeMap::new();
	for record in &report.tasks {
		let key = (record.identity.stage_id.0, record.identity.task_id.0);
		match latest.entry(key) {
			Entry::Vacant(vacant) => {
				vacant.insert(record);
			}
			Entry::Occupied(mut occupied) => {
				if record.identity.attempt_id >= occupied.get().identity.attempt_id {
					occupied.insert(record);
				}
			}
		}
	}
	let mut tasks_per_stage: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
	for (stage, task) in latest.keys() {
		tasks_per_stage.entry(*stage).or_default().push(*task);
	}
	let upstream_stages: BTreeMap<u32, Vec<u32>> = report
		.stage_upstreams
		.iter()
		.map(|(stage, upstreams)| {
			(
				stage.0,
				upstreams.iter().map(|upstream| upstream.0).collect(),
			)
		})
		.collect();
	latest
		.iter()
		.map(|((stage, task), record)| {
			if record.telemetry.phase_receipt != TaskPhaseReceipt::Sealed {
				return Err(format!(
					"task {stage}/{task} phase receipt is not sealed: {:?}",
					record.telemetry.phase_receipt
				));
			}
			let mut upstream = Vec::new();
			if let Some(upstreams) = upstream_stages.get(stage) {
				for upstream_stage in upstreams {
					if let Some(upstream_tasks) = tasks_per_stage.get(upstream_stage) {
						for upstream_task in upstream_tasks {
							upstream.push((*upstream_stage, *upstream_task));
						}
					}
				}
			}
			Ok(TimingAtlasTaskNode {
				stage_id: *stage,
				task_id: *task,
				wall_ms: record.telemetry.phases.task_wall_micros as f64 / 1000.0,
				components: timing_atlas_component_portions_ms(&record.telemetry.phases),
				worker_id: record.worker_id.get().to_string(),
				aggregate_accumulation_ms: record.telemetry.phases.aggregate_accumulation_micros
					as f64 / 1000.0,
				telemetry_lock_ms: record.telemetry.phases.telemetry_lock_micros as f64 / 1000.0,
				framework_overhead_ms: record.telemetry.phases.framework_overhead_micros as f64
					/ 1000.0,
				upstream,
			})
		})
		.collect()
}

/// The local arm's single execution path as one task node, so its evidence flows through
/// the same critical-path machinery as the distributed arm's. Wall clock is the executor
/// window; the phases partition it exactly as they partition a distributed task's wall.
pub fn timing_atlas_local_task_node(
	execute_wall_ms: f64,
	phases: &TaskTelemetryPhases,
	receipt: TaskPhaseReceipt,
) -> Result<TimingAtlasTaskNode, String> {
	if receipt != TaskPhaseReceipt::Sealed {
		return Err(format!("local phase receipt is not sealed: {receipt:?}"));
	}
	Ok(TimingAtlasTaskNode {
		stage_id: 0,
		task_id: 0,
		wall_ms: execute_wall_ms,
		components: timing_atlas_component_portions_ms(phases),
		// The local arm runs no distributed task, so it has no worker. Empty is the
		// declared absence — not a fabricated id that would join against nothing.
		worker_id: String::new(),
		aggregate_accumulation_ms: phases.aggregate_accumulation_micros as f64 / 1000.0,
		telemetry_lock_ms: phases.telemetry_lock_micros as f64 / 1000.0,
		framework_overhead_ms: phases.framework_overhead_micros as f64 / 1000.0,
		upstream: Vec::new(),
	})
}

/// Derive one child's attribution from its task nodes. `None` when the child produced no
/// nodes — the caller decides whether that is a defect (a completed distributed child with
/// no retained report is) rather than this function inventing an empty receipt.
pub fn timing_atlas_child_attribution(
	nodes: Vec<TimingAtlasTaskNode>,
) -> Option<TimingAtlasChildAttribution> {
	timing_atlas_child_attribution_with_lifecycle(nodes, None, None, Default::default())
}

/// As above, carrying the ADR 554 §3 B2 lifecycle spans measured outside the tasks.
pub fn timing_atlas_child_attribution_with_lifecycle(
	nodes: Vec<TimingAtlasTaskNode>,
	sql_planning_ms: Option<f64>,
	consumer_drain_ms: Option<f64>,
	lifecycle: toron::coordinator::QueryLifecycleSpans,
) -> Option<TimingAtlasChildAttribution> {
	let selected = timing_atlas_critical_path(&nodes)?;
	Some(TimingAtlasChildAttribution {
		nodes,
		path: selected.path,
		path_wall_ms: selected.total_wall_ms,
		path_components: selected.components,
		in_task_unattributed_ms: selected.in_task_unattributed_ms,
		aggregate_accumulation_ms: selected.aggregate_accumulation_ms,
		sql_planning_ms,
		pre_dispatch_ms: lifecycle.pre_dispatch_micros as f64 / 1000.0,
		dispatch_loop_wall_ms: lifecycle.dispatch_loop_wall_micros as f64 / 1000.0,
		post_dispatch_finalization_ms: lifecycle.post_dispatch_finalization_micros as f64 / 1000.0,
		consumer_drain_ms,
	})
}

/// One measured child's attribution receipt as a journal row: the full node set, the
/// selected path, and the portions along it, so replay can re-derive every figure from the
/// nodes rather than trusting the emitted path.
pub fn timing_atlas_attribution_path_row(
	query: &str,
	arm: &str,
	pair_index: usize,
	attempt: u32,
	measured_elapsed_ms: f64,
	attribution: &TimingAtlasChildAttribution,
) -> serde_json::Value {
	let nodes: Vec<serde_json::Value> = attribution
		.nodes
		.iter()
		.map(|node| {
			let components: serde_json::Map<String, serde_json::Value> = node
				.components
				.iter()
				.map(|(component, portion)| {
					(component.label().to_string(), serde_json::json!(portion))
				})
				.collect();
			serde_json::json!({
				"stage": node.stage_id,
				"task": node.task_id,
				"wall_ms": node.wall_ms,
				"components": components,
				"worker_id": node.worker_id,
				"aggregate_accumulation_ms": node.aggregate_accumulation_ms,
				"telemetry_lock_ms": node.telemetry_lock_ms,
				"framework_overhead_ms": node.framework_overhead_ms,
				"upstream": node.upstream,
			})
		})
		.collect();
	let remainder_ms = measured_elapsed_ms - attribution.attributed_ms();
	// Accumulation is in-task time that is now **named**, so it leaves the unnamed
	// residual alongside the unnamed in-task terms. Omitting it here would not hide
	// it — the engine already removed it from `framework_overhead`, so it would
	// silently reappear as unnamed. The first Branch A run did exactly that.
	let unnamed_residual_ms =
		remainder_ms - attribution.in_task_unattributed_ms - attribution.aggregate_accumulation_ms;
	let path_dispatch_sum_ms = attribution
		.path_components
		.get("dispatch")
		.copied()
		.unwrap_or(0.0);
	// Containment is asserted, never assumed: if the path's dispatch windows sum
	// past the loop that ran them they overlapped, and the difference means nothing.
	//
	// Named for what it is, not what it might be. This is loop time outside the
	// **selected path's** task windows — it cannot distinguish scheduler or
	// dependency-release cost from off-path sibling tasks doing real work, because
	// the path carries one task per stage and a stage may have many. Calling it
	// "coordination" would assert a mechanism the subtraction cannot see.
	let loop_non_path_ms = (attribution.dispatch_loop_wall_ms > 0.0
		&& path_dispatch_sum_ms <= attribution.dispatch_loop_wall_ms + 1e-6)
		.then(|| attribution.dispatch_loop_wall_ms - path_dispatch_sum_ms);
	let telemetry_lock_ms: f64 = attribution
		.path
		.iter()
		.filter_map(|key| attribution.nodes.iter().find(|node| node.key() == *key))
		.map(|node| node.telemetry_lock_ms)
		.sum();
	let framework_overhead_ms: f64 = attribution
		.path
		.iter()
		.filter_map(|key| attribution.nodes.iter().find(|node| node.key() == *key))
		.map(|node| node.framework_overhead_ms)
		.sum();
	serde_json::json!({
		"kind": TIMING_ATLAS_KIND_ATTRIBUTION_PATH,
		"query": query,
		"arm": arm,
		"pair_index": pair_index,
		"attempt": attempt,
		"measured_elapsed_ms": measured_elapsed_ms,
		"nodes": nodes,
		"path": attribution.path,
		"path_wall_ms": attribution.path_wall_ms,
		"path_components": attribution.path_components,
		"remainder_ms": remainder_ms,
		// ADR 551 follow-up: remainder split into what is measured and what is
		// not. `in_task_unattributed_ms` is exactly the two counters above; the residual
		// is what no counter names at all. `path_wall_ms` above is the envelope
		// and is deliberately absent from this subtraction.
		"telemetry_lock_ms": telemetry_lock_ms,
		"framework_overhead_ms": framework_overhead_ms,
		"in_task_unattributed_ms": attribution.in_task_unattributed_ms,
		"aggregate_accumulation_ms": attribution.aggregate_accumulation_ms,
		"unnamed_residual_ms": unnamed_residual_ms,
		// ADR 554 §3 B2: three sequential spans with distinct owners, plus what
		// they do not cover. The residual is carried even when small — dropping it
		// once three plausible timers exist would replace one opaque number with
		// three confident-looking but incomplete ones.
		"sql_planning_ms": attribution.sql_planning_ms,
		"pre_dispatch_ms": attribution.pre_dispatch_ms,
		"post_dispatch_finalization_ms": attribution.post_dispatch_finalization_ms,
		"consumer_drain_ms": attribution.consumer_drain_ms,
		// The loop wall is an envelope over the task dispatch windows, so it is
		// emitted raw beside their sum rather than differenced blindly. The derived
		// figure appears only when the windows are contained within the wall;
		// otherwise both raw figures stand and the subtraction is refused, because a
		// negative difference would be an artefact of overlap, not a measurement.
		"dispatch_loop_wall_ms": attribution.dispatch_loop_wall_ms,
		"path_dispatch_sum_ms": path_dispatch_sum_ms,
		"loop_non_path_ms": loop_non_path_ms,
		// Only sequential, non-overlapping spans are subtracted. `consumer_drain_ms`
		// is deliberately **not** among them: a streaming consumer drains while tasks
		// still produce, so it overlaps the dispatch window and is an envelope like
		// the loop wall. Subtracting it drove residuals below the gap itself on real
		// cells, which the sign guard refused — the guard working, not a nuisance.
		"lifecycle_residual_ms": unnamed_residual_ms
			- attribution.sql_planning_ms.unwrap_or(0.0)
			- attribution.pre_dispatch_ms
			- attribution.post_dispatch_finalization_ms,
	})
}

/// Per-cell component rows from the cell's measured pairs. The estimator is symmetric with
/// the timing estimator: the cell's component contribution is the median over pairs of
/// (distributed portion − local portion), exactly as the cell's delta is the median of
/// paired deltas. Components pair per the taxonomy: `dispatch` is emitted raw and never
/// additive; `transport`'s absent local side is the declared zero; `reduction` is out of
/// the boundary and gets no row at all.
pub fn timing_atlas_component_rows(
	query: &str,
	pairs: &[TimingAtlasPair],
) -> Vec<serde_json::Value> {
	let mut rows = Vec::new();
	let attributed: Vec<(&TimingAtlasChildAttribution, &TimingAtlasChildAttribution)> = pairs
		.iter()
		.filter_map(|pair| {
			Some((
				pair.local.attribution.as_ref()?,
				pair.distributed.attribution.as_ref()?,
			))
		})
		.collect();
	if attributed.len() != pairs.len() || attributed.is_empty() {
		// A measured pair without attribution on both children is an apparatus defect the
		// caller has already surfaced; emitting partial component rows here would let a
		// cell claim receipts it cannot back.
		return rows;
	}
	for component in TimingAtlasComponent::ALL {
		if !component.participates_in_timed_attribution() {
			continue;
		}
		let label = component.label();
		let mut local_values = Vec::new();
		let mut distributed_values = Vec::new();
		let mut contributions = Vec::new();
		for (local, distributed) in &attributed {
			let local_ms = local.path_components.get(label).copied();
			let distributed_ms = distributed
				.path_components
				.get(label)
				.copied()
				.unwrap_or(0.0);
			let local_ms = match (local_ms, component.local_pairing()) {
				(Some(value), _) => value,
				(None, TimingAtlasLocalPairing::DeclaredZero) => 0.0,
				(None, TimingAtlasLocalPairing::NoLocalEquivalent) => 0.0,
				// Required components are always present in a local portion map by
				// construction; an absent entry means the mapping regressed, and a zero
				// here would silently hand the whole delta to this component.
				(None, _) => continue,
			};
			local_values.push(local_ms);
			distributed_values.push(distributed_ms);
			contributions.push(distributed_ms - local_ms);
		}
		if contributions.len() != attributed.len() {
			continue;
		}
		let eligible = !matches!(
			component.local_pairing(),
			TimingAtlasLocalPairing::NoLocalEquivalent
		);
		let median_contribution = timing_atlas_median(&mut contributions);
		rows.push(serde_json::json!({
			"kind": TIMING_ATLAS_KIND_ATTRIBUTION,
			"query": query,
			"component": label,
			"scope": timing_atlas_component_scope(component),
			"basis": TimingAtlasBasis::CriticalPath.label(),
			"pairing": component.local_pairing().label(),
			"eligible": eligible,
			"additive": eligible,
			"pairs": attributed.len(),
			"median_local_ms": timing_atlas_median(&mut local_values),
			"median_distributed_ms": timing_atlas_median(&mut distributed_values),
			"contribution_ms": if eligible { serde_json::json!(median_contribution) } else { serde_json::Value::Null },
			"raw_distributed_median_ms": timing_atlas_median(&mut distributed_values),
		}));
	}
	rows
}

/// ADR 528: total classification of the fragmenter's reject taxonomy. Exhaustive by
/// construction — a new `FragmentError` variant fails compilation here instead of falling
/// through to an unrecognized bucket.
pub fn classify_fragment_error(error: &FragmentError) -> &'static str {
	match error {
		FragmentError::ZeroPartitionCount => "fragment_zero_partition_count",
		FragmentError::MultipleExchangeBoundaries { .. } => "fragment_multiple_exchange_boundaries",
		FragmentError::MissingScanMetadata => "fragment_missing_scan_metadata",
		FragmentError::JoinInputNotExchanged => "fragment_join_input_not_exchanged",
		FragmentError::SortBarrierInsideJoinInput => "fragment_sort_barrier_inside_join_input",
		FragmentError::JoinConsumerAggregateNotDecomposable { .. } => {
			"fragment_join_consumer_aggregate_not_decomposable"
		}
		FragmentError::BroadcastNonInner { .. } => "fragment_broadcast_non_inner",
		FragmentError::BroadcastBuildNotPublished => "fragment_broadcast_build_not_published",
		FragmentError::BroadcastProbeNotStarLinear => "fragment_broadcast_probe_not_star_linear",
		FragmentError::CreateTableNotFragmentable
		| FragmentError::DropTableNotFragmentable
		| FragmentError::DeleteNotFragmentable
		| FragmentError::UpdateNotFragmentable
		| FragmentError::InsertNotFragmentable
		| FragmentError::InsertSelectNotFragmentable
		| FragmentError::CreateTableAsSelectNotFragmentable => "fragment_coordinator_only_operation",
		FragmentError::FragmentIdNamespaceOverflow { .. } => "fragment_id_namespace_overflow",
		FragmentError::UndrainableLiveShuffle { .. } => "fragment_undrainable_live_shuffle",
		FragmentError::LiveComponentExceedsAdmission { .. } => {
			"fragment_live_component_exceeds_admission"
		}
	}
}

/// ADR 528: extract the shape receipt from a fragmented plan. A producer stage is one whose
/// tasks publish to an exchange; a single-task producer is the fragmenter's hardcoded
/// one-task-per-join-input shape.
pub fn fragment_shape_receipt(
	stages: &[StagePlan],
	max_in_flight_tasks: Option<usize>,
) -> FragmentShapeReceipt {
	let stage_task_counts: Vec<usize> = stages.iter().map(|stage| stage.tasks.len()).collect();
	let max_stage_width = stage_task_counts.iter().copied().max().unwrap_or(0);
	let single_task_producer_stages = stages
		.iter()
		.filter(|stage| {
			stage.tasks.len() == 1
				&& stage.tasks.iter().all(|task| {
					matches!(task.spec.output, TaskOutputBinding::ShuffleProducer { .. })
				})
		})
		.count();
	let report = live_publication_report(stages, max_in_flight_tasks);
	FragmentShapeReceipt {
		stage_count: stages.len(),
		stage_task_counts,
		max_stage_width,
		single_task_producer_stages,
		live_eligible_consumer_tasks: report.live_eligible_consumer_tasks,
		stage_upstreams: report
			.stage_upstreams
			.iter()
			.map(|(stage_id, upstreams)| {
				(
					stage_id.get(),
					upstreams.iter().map(|upstream| upstream.get()).collect(),
				)
			})
			.collect(),
		live_producer_stages: report.live_producer_stages.len(),
		multi_upstream_live_eligible_consumer_tasks: report
			.multi_upstream_live_eligible_consumer_tasks,
		live_residency_requirement: report.live_residency_requirement,
		admission_max_in_flight_tasks: report.max_in_flight_tasks,
		residency_exceeds_admission: report.residency_exceeds_admission,
	}
}

/// ADR 528: bind under the distributed posture and ask the fragmenter — never execute. The
/// fragment config mirrors the coordinator's derivation exactly: the same
/// `TaskBudget::from_query_budget` envelope the gauge executes under, the task context lifted
/// from the execution context, and the context's observation mode.
async fn fragment_census_outcome_with_metadata(
	sql: &str,
	tables: Vec<HarnessTableMetadata>,
) -> (Option<String>, FragmentCensusOutcome) {
	let total_start = Instant::now();
	let mut timings = PerfPhaseTimings::default();
	let catalog = match load_catalog_from_metadata(tables, &mut timings, total_start) {
		Ok(catalog) => catalog,
		Err(error) => return (None, stage_rejection(error)),
	};
	let logical = match SqlPlanner::new().plan(sql) {
		Ok(logical) => logical,
		Err(error) => {
			return (
				None,
				stage_rejection(StageError::from_display(Phase::Plan, error)),
			);
		}
	};
	let bound = match bind_with_local_join_admissions(
		&logical,
		&catalog,
		&distributed_gauge_planner_config(),
	)
	.await
	{
		Ok((bound, _admissions)) => bound,
		Err(error) => {
			return (
				None,
				stage_rejection(StageError::from_debug(Phase::Bind, error)),
			);
		}
	};
	let plan_fingerprint = physical_plan_fingerprint(&bound);

	let task_telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
	let context = tpch_execution_context(task_telemetry);
	let config = FragmentPlanConfig::new(
		context.query_id,
		TaskExecutionContext::from_execution_context(&context),
		context.task_budget,
	)
	.with_join_runtime_observation_mode(context.join_runtime_observation_mode.clone());
	match fragment_plan(bound, config) {
		Ok(stages) => (
			Some(plan_fingerprint),
			FragmentCensusOutcome::Fragmented(fragment_shape_receipt(&stages, None)),
		),
		Err(error) => (
			Some(plan_fingerprint),
			FragmentCensusOutcome::FragmentRejected {
				reason: classify_fragment_error(&error),
				message: error.to_string(),
			},
		),
	}
}

fn stage_rejection(error: StageError) -> FragmentCensusOutcome {
	FragmentCensusOutcome::StageRejected {
		phase: error.phase,
		reason: error.reason,
		message: error.message,
	}
}

/// ADR 528: one census cell, end to end. `data_source` mirrors the perf rail (`memory` /
/// `parquet` / `persisted`); the persisted arm loads bind-time metadata and facts only —
/// no data is scanned and nothing executes.
pub async fn run_fragment_census_query(
	query: &str,
	mode: &str,
	scale_factor: f64,
	data_source: &str,
	data_root: &Path,
) -> Result<FragmentCensusRow, String> {
	let total_start = Instant::now();
	let (query_id, sql, tables) = match data_source {
		"memory" => {
			let spec = build_perf_query_spec(query, mode, scale_factor)?;
			let (metadata, _storage) = metadata_storage_from_tables(spec.tables);
			(spec.query_id, spec.sql, metadata)
		}
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec(query, mode, scale_factor, data_root)?;
			(spec.query_id, spec.sql, spec.tables)
		}
		other => {
			return Err(format!(
				"fragment-census --data-source {other:?} is not supported"
			));
		}
	};
	let (plan_fingerprint, outcome) = fragment_census_outcome_with_metadata(&sql, tables).await;
	let elapsed_ms = total_start.elapsed().as_secs_f64() * 1000.0;
	Ok(fragment_census_row(
		query_id,
		mode,
		scale_factor,
		plan_fingerprint,
		outcome,
		elapsed_ms,
	))
}

fn fragment_census_row(
	query_id: &str,
	mode: &str,
	scale_factor: f64,
	plan_fingerprint: Option<String>,
	outcome: FragmentCensusOutcome,
	elapsed_ms: f64,
) -> FragmentCensusRow {
	match outcome {
		FragmentCensusOutcome::StageRejected {
			phase,
			reason,
			message,
		} => FragmentCensusRow {
			query: query_id.to_string(),
			scale_factor,
			mode: mode.to_string(),
			phase,
			classification: "bind_rejected".to_string(),
			reason: Some(reason),
			plan_fingerprint,
			stage_count: None,
			stage_task_counts: None,
			max_stage_width: None,
			single_task_producer_stages: None,
			live_eligible_consumer_tasks: None,
			elapsed_ms,
			diagnostic_message: Some(message),
		},
		FragmentCensusOutcome::FragmentRejected { reason, message } => FragmentCensusRow {
			query: query_id.to_string(),
			scale_factor,
			mode: mode.to_string(),
			phase: Phase::Fragment,
			classification: "fragment_rejected".to_string(),
			reason: Some(reason.to_string()),
			plan_fingerprint,
			stage_count: None,
			stage_task_counts: None,
			max_stage_width: None,
			single_task_producer_stages: None,
			live_eligible_consumer_tasks: None,
			elapsed_ms,
			diagnostic_message: Some(message),
		},
		FragmentCensusOutcome::Fragmented(receipt) => FragmentCensusRow {
			query: query_id.to_string(),
			scale_factor,
			mode: mode.to_string(),
			phase: Phase::Fragment,
			classification: if receipt.max_stage_width > 1 {
				"fragments_parallel".to_string()
			} else {
				"fragments_sequential".to_string()
			},
			reason: None,
			plan_fingerprint,
			stage_count: Some(receipt.stage_count),
			stage_task_counts: Some(receipt.stage_task_counts),
			max_stage_width: Some(receipt.max_stage_width),
			single_task_producer_stages: Some(receipt.single_task_producer_stages),
			live_eligible_consumer_tasks: Some(receipt.live_eligible_consumer_tasks),
			elapsed_ms,
			diagnostic_message: None,
		},
	}
}

const EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const EXECUTION_CENSUS_SPILL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const EXECUTION_CENSUS_GENEROUS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const EXECUTION_CENSUS_BYTES_IN_FLIGHT: u64 = 2 * 1024 * 1024 * 1024;
const EXECUTION_CENSUS_WORKER_COUNT: usize = 2;
const EXECUTION_CENSUS_FALLBACK_WIDTH: usize = 4;
const EXECUTION_CENSUS_MEMORY_POOL_RETAINED_BUFFERS: usize = 32;
/// How long the live shuffle must stand completely still, with an appender blocked
/// throughout, before the wait is evidence of a non-returning cycle rather than pacing.
const EXECUTION_CENSUS_STALL_PERSISTENCE_WINDOW: std::time::Duration =
	std::time::Duration::from_secs(2);

/// One sample of live shuffle state across the workers. A bounded backlog blocks an
/// appender by design once it fills, so a blocked appender alone proves nothing; what
/// distinguishes a stall is that nothing moves while it stays blocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveProgressSignature(Vec<(u128, usize, usize, usize)>);

impl LiveProgressSignature {
	pub fn from_receipts(receipts: &[ExecutionCensusBackpressureReceipt]) -> Self {
		let mut samples = receipts
			.iter()
			.map(|receipt| {
				(
					receipt.worker_id.get().as_u128(),
					receipt.observation.live_partitions,
					receipt.observation.buffered_chunks,
					receipt.observation.blocked_appenders,
				)
			})
			.collect::<Vec<_>>();
		samples.sort_unstable();
		Self(samples)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallVerdict {
	Progressing,
	Stalled,
}

/// Decides a stall from persistence rather than from a single sample. Pure and driven by a
/// caller-supplied elapsed value so both controls are deterministic.
#[derive(Debug)]
pub struct StallPersistenceTracker {
	window: std::time::Duration,
	last: Option<LiveProgressSignature>,
	unchanged_since: Option<std::time::Duration>,
}

impl StallPersistenceTracker {
	pub fn new(window: std::time::Duration) -> Self {
		Self {
			window,
			last: None,
			unchanged_since: None,
		}
	}

	/// `signature` is `None` when no appender is blocked anywhere — unambiguous progress.
	/// A changed signature is progress too: chunks were released or partitions advanced.
	pub fn observe(
		&mut self,
		elapsed: std::time::Duration,
		signature: Option<LiveProgressSignature>,
	) -> StallVerdict {
		let Some(signature) = signature else {
			self.last = None;
			self.unchanged_since = None;
			return StallVerdict::Progressing;
		};
		if self.last.as_ref() != Some(&signature) {
			self.last = Some(signature);
			self.unchanged_since = Some(elapsed);
			return StallVerdict::Progressing;
		}
		match self.unchanged_since {
			Some(since) if elapsed.saturating_sub(since) >= self.window => StallVerdict::Stalled,
			Some(_) => StallVerdict::Progressing,
			None => {
				self.unchanged_since = Some(elapsed);
				StallVerdict::Progressing
			}
		}
	}
}

fn execution_census_local_budget() -> QueryBudget {
	QueryBudget {
		memory_per_task: ByteCount::new(EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES),
		spill_bytes_reserved_per_query: ByteCount::new(EXECUTION_CENSUS_SPILL_BYTES),
		..QueryBudget::test_default()
	}
}

fn execution_census_distributed_budget(width: usize) -> QueryBudget {
	execution_census_distributed_budget_with_memory(
		width,
		EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES / u64::try_from(width.max(1)).unwrap_or(1),
	)
}

fn execution_census_distributed_budget_with_memory(
	width: usize,
	memory_per_task: u64,
) -> QueryBudget {
	QueryBudget {
		bytes_in_flight_per_query: ByteCount::new(EXECUTION_CENSUS_BYTES_IN_FLIGHT),
		bytes_in_flight_per_worker_stream: ByteCount::new(512 * 1024 * 1024),
		memory_per_task: ByteCount::new(memory_per_task.max(1)),
		shuffle_bytes_published_per_task: ByteCount::new(EXECUTION_CENSUS_GENEROUS_BYTES),
		spill_bytes_reserved_per_query: ByteCount::new(EXECUTION_CENSUS_SPILL_BYTES),
		broadcast_build_bytes_per_task: ByteCount::new(EXECUTION_CENSUS_GENEROUS_BYTES),
		broadcast_replicated_bytes_per_query: ByteCount::new(EXECUTION_CENSUS_GENEROUS_BYTES),
		max_in_flight_tasks: Some(width.max(1)),
		..QueryBudget::test_default()
	}
}

fn execution_census_distributed_planner_config(memory_per_task: ByteCount) -> PlannerConfig {
	distributed_gauge_planner_config_with_task_memory(Some(memory_per_task))
}

/// The single ADR 529 coordinator-construction rail.
///
/// Both the execution census and the timing-atlas harness build their coordinator here, so
/// the registry shape, memory-pool sizing, transport and planner config cannot drift apart
/// between them. A second construction site is exactly how they diverged before: the
/// duplicate looked harmless because it was copied faithfully, and nothing then kept it
/// faithful. Callers that need extra configuration chain it onto the returned coordinator
/// rather than re-calling the constructor.
fn build_execution_census_coordinator(
	catalog: Arc<dyn Catalog>,
	workers: &[ExecutionCensusWorkerHarness],
	memory_pool_max_buffer_capacity_bytes: usize,
	distributed_budget: QueryBudget,
) -> DistributedQueryCoordinator<StaticWorkerRegistry, TcpTransport> {
	let entries = workers
		.iter()
		.map(|worker| worker.entry.clone())
		.collect::<Vec<_>>();
	let registry = Arc::new(StaticWorkerRegistry::new(entries));
	let coordinator_memory = Arc::new(MemoryManager::new(
		EXECUTION_CENSUS_MEMORY_POOL_RETAINED_BUFFERS,
		memory_pool_max_buffer_capacity_bytes,
	));
	DistributedQueryCoordinator::new(
		catalog,
		registry,
		coordinator_memory,
		Arc::new(TcpTransport::new()),
		execution_census_distributed_planner_config(distributed_budget.memory_per_task),
		distributed_budget,
	)
}

struct ExecutionCensusPreflight {
	logical: LogicalPlan,
	catalog: MockCatalog,
	plan_fingerprint: String,
	shape: FragmentShapeReceipt,
}

#[derive(Debug)]
struct ExecutionCensusPreflightRejection {
	phase: Phase,
	reason: String,
	message: String,
	plan_fingerprint: Option<String>,
}

async fn execution_census_preflight(
	sql: &str,
	tables: Vec<HarnessTableMetadata>,
) -> Result<ExecutionCensusPreflight, ExecutionCensusPreflightRejection> {
	let mut timings = PerfPhaseTimings::default();
	let catalog =
		load_catalog_from_metadata(tables, &mut timings, Instant::now()).map_err(|error| {
			ExecutionCensusPreflightRejection {
				phase: error.phase,
				reason: "preflight_load".to_string(),
				message: error.message,
				plan_fingerprint: None,
			}
		})?;
	let logical =
		SqlPlanner::new()
			.plan(sql)
			.map_err(|error| ExecutionCensusPreflightRejection {
				phase: Phase::Plan,
				reason: "preflight_plan".to_string(),
				message: error.to_string(),
				plan_fingerprint: None,
			})?;
	let seed_budget = execution_census_distributed_budget(EXECUTION_CENSUS_FALLBACK_WIDTH);
	let planner_config = execution_census_distributed_planner_config(seed_budget.memory_per_task);
	let bound = bind_with_local_join_admissions(&logical, &catalog, &planner_config)
		.await
		.map(|(bound, _)| bound)
		.map_err(|error| ExecutionCensusPreflightRejection {
			phase: Phase::Bind,
			reason: "preflight_bind".to_string(),
			message: format!("{error:?}"),
			plan_fingerprint: None,
		})?;
	let plan_fingerprint = physical_plan_fingerprint(&bound);
	let task_telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
	let context = tpch_execution_context_with_budget(task_telemetry, seed_budget);
	let fragment_config = FragmentPlanConfig::new(
		context.query_id,
		TaskExecutionContext::from_execution_context(&context),
		context.task_budget,
	)
	.with_join_runtime_observation_mode(context.join_runtime_observation_mode.clone())
	.with_max_in_flight_tasks(seed_budget.max_in_flight_tasks)
	.with_runtime_filters_enabled(planner_config.runtime_filters_enabled);
	let stages = fragment_plan(bound, fragment_config).map_err(|error| {
		ExecutionCensusPreflightRejection {
			phase: Phase::Fragment,
			reason: classify_fragment_error(&error).to_string(),
			message: error.to_string(),
			plan_fingerprint: Some(plan_fingerprint.clone()),
		}
	})?;
	Ok(ExecutionCensusPreflight {
		logical,
		catalog,
		plan_fingerprint,
		shape: fragment_shape_receipt(&stages, seed_budget.max_in_flight_tasks),
	})
}

#[derive(Debug)]
struct ExecutionIdentityFacts {
	digest: String,
	row_count: u64,
	schema_fingerprint: Option<String>,
}

fn execution_identity_facts(batches: &[RecordBatch]) -> Result<ExecutionIdentityFacts, String> {
	let digest = canonical_arrow_ipc_digest(batches, SortPolicy::FullRowLex)
		.map_err(|error| format!("execution census canonical digest: {error:?}"))?;
	Ok(ExecutionIdentityFacts {
		digest: format!("{digest:?}"),
		row_count: output_row_count(batches),
		schema_fingerprint: output_schema_fingerprint(batches),
	})
}

fn budget_dimension_slug(dimension: BudgetDimension) -> &'static str {
	match dimension {
		BudgetDimension::Memory => "budget_memory",
		BudgetDimension::ShuffleBytes => "budget_shuffle_bytes",
		BudgetDimension::SpillBytes => "budget_spill_bytes",
		BudgetDimension::BroadcastReplication => "budget_broadcast_replication",
		BudgetDimension::BytesInFlightPerQuery => "budget_bytes_in_flight_per_query",
		BudgetDimension::BytesInFlightPerWorkerStream => "budget_bytes_in_flight_per_worker_stream",
		BudgetDimension::FrameSize => "budget_frame_size",
	}
}

fn task_failure_reason(kind: &TaskFailureKind) -> String {
	match kind {
		TaskFailureKind::ProtocolMismatch => "task_protocol_mismatch".to_string(),
		TaskFailureKind::DescriptorEvolved { .. } => "task_descriptor_evolved".to_string(),
		TaskFailureKind::WorkerLost { .. } => "task_worker_lost".to_string(),
		TaskFailureKind::TaskFailed { .. } => "task_failed".to_string(),
		TaskFailureKind::BudgetExhausted { dimension, .. } => {
			budget_dimension_slug(*dimension).to_string()
		}
		TaskFailureKind::Cancelled => "task_cancelled".to_string(),
		TaskFailureKind::QueryLeaseMissing => "task_query_lease_missing".to_string(),
		TaskFailureKind::QueryLeaseExpired => "task_query_lease_expired".to_string(),
		TaskFailureKind::Internal { .. } => "task_internal".to_string(),
	}
}

fn execution_error_reason(error: &ExecutionError) -> String {
	match error {
		ExecutionError::Storage(_) => "execution_storage".to_string(),
		ExecutionError::Compute(_) => "execution_compute".to_string(),
		ExecutionError::Binding(_) => "execution_binding".to_string(),
		ExecutionError::NotImplemented { .. } => "execution_not_implemented".to_string(),
		ExecutionError::ColumnNotFound { .. } => "execution_column_not_found".to_string(),
		ExecutionError::InvalidExpression { .. } => "execution_invalid_expression".to_string(),
		ExecutionError::Kernel { .. } => "execution_kernel".to_string(),
		ExecutionError::BudgetExhausted { dimension, .. } => {
			budget_dimension_slug(*dimension).to_string()
		}
		ExecutionError::ConcurrencyExhausted { .. } => {
			"execution_concurrency_exhausted".to_string()
		}
	}
}

fn execution_census_query_error_reason(error: &QueryError) -> String {
	match error {
		QueryError::Fragment(error) => classify_fragment_error(error).to_string(),
		QueryError::Execution(error) => execution_error_reason(error),
		QueryError::Storage(_) => "query_storage".to_string(),
		QueryError::Binding(_) => "query_binding".to_string(),
		QueryError::Catalog(_) => "query_catalog".to_string(),
		QueryError::Frontend(_) => "query_frontend".to_string(),
		QueryError::Wire(_) => "query_wire".to_string(),
		QueryError::Spill(_) => "query_spill".to_string(),
		QueryError::Runtime(RuntimeError::ProtocolMismatch { .. }) => {
			"runtime_protocol_mismatch".to_string()
		}
		QueryError::Runtime(RuntimeError::PartitionStoreMismatch { .. }) => {
			"runtime_partition_store_mismatch".to_string()
		}
		QueryError::Runtime(RuntimeError::TaskFailure { kind }) => task_failure_reason(kind),
		QueryError::Io(_) => "query_io".to_string(),
		QueryError::Arrow(_) => "query_arrow".to_string(),
		QueryError::Remote { .. } => "query_remote".to_string(),
		QueryError::UnexpectedMessage { .. } => "query_unexpected_message".to_string(),
		QueryError::SchemaInstability => "query_schema_instability".to_string(),
		QueryError::Internal { .. } => "query_internal".to_string(),
	}
}

struct ExecutionCensusWorkerHarness {
	entry: WorkerEntry,
	worker: Arc<ToronWorker>,
	partition_store: Arc<InMemoryPartitionStore>,
	cancellation_cleanup: Arc<AtomicBool>,
	shutdown: tokio::sync::oneshot::Sender<()>,
	/// Peer closes tolerated on this worker's connections, reported rather than
	/// charged. Always published, including zero — a cell that saw none must be
	/// distinguishable from one whose counter was never wired.
	peer_closes: Arc<AtomicUsize>,
	handle: tokio::task::JoinHandle<Result<(), String>>,
}
/// Whether a connection error is the ordinary end of a peer's session rather
/// than a worker fault.
///
/// A census worker cannot tell "the coordinator finished with me" from "the
/// coordinator died" — both arrive as a broken pipe on the worker's side. That
/// makes this signal non-discriminating by construction, so it is counted and
/// never charged. What actually guards against a truncated query is the
/// coordinator's own error path and the cell's result verification, both of
/// which observe the query rather than the socket.
///
/// Matched on `io::ErrorKind` and never on rendered text: the message is a
/// presentation of the error, not its identity.
fn is_expected_peer_close(error: &toron::error::QueryError) -> bool {
	matches!(
		error,
		toron::error::QueryError::Io(io)
			if matches!(
				io.kind(),
				std::io::ErrorKind::BrokenPipe
					| std::io::ErrorKind::ConnectionReset
					| std::io::ErrorKind::ConnectionAborted
					| std::io::ErrorKind::UnexpectedEof
			)
	)
}

/// Decide whether a finished census connection failed the cell, counting the
/// closes it tolerates.
///
/// Deliberately free of any notion of *when* in the cell's life the connection
/// ended. Measured, not assumed: instrumenting the moment each connection ends
/// showed q01 taking a broken pipe at the close of *every* distributed attempt —
/// seven of eight with teardown not yet begun — while all seven pairs measured
/// cleanly and no attempt was invalid. The close is how a finished query ends,
/// not how one fails; charging it failed a complete cell as `cohort_incomplete`.
///
/// Two earlier fixes gated this on a teardown flag because the failure text
/// carried a `teardown:` prefix. That prefix marks where the error surfaced —
/// the JoinSet holds each result until join — not where it occurred.
fn classify_census_connection_outcome(
	outcome: Result<(), toron::error::QueryError>,
	peer_closes: &AtomicUsize,
) -> Result<(), String> {
	match outcome {
		Ok(()) => Ok(()),
		Err(error) if is_expected_peer_close(&error) => {
			peer_closes.fetch_add(1, Ordering::Relaxed);
			Ok(())
		}
		Err(error) => Err(format!("execution census worker connection: {error}")),
	}
}

async fn spawn_execution_census_worker(
	worker_index: usize,
	storage: Arc<dyn StorageScan>,
	budget: QueryBudget,
	memory_pool_max_buffer_capacity_bytes: usize,
) -> Result<ExecutionCensusWorkerHarness, String> {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.map_err(|error| format!("execution census worker bind: {error}"))?;
	let endpoint = WorkerEndpoint::new(
		listener
			.local_addr()
			.map_err(|error| format!("execution census worker address: {error}"))?
			.to_string(),
	);
	let worker_id = WorkerId::from_u128(529_000 + worker_index as u128);
	let memory = Arc::new(MemoryManager::new(
		EXECUTION_CENSUS_MEMORY_POOL_RETAINED_BUFFERS,
		memory_pool_max_buffer_capacity_bytes,
	));
	let partition_store = Arc::new(InMemoryPartitionStore::with_budget(budget));
	// Routing receipts hash every key value crossing a shuffle edge, so the census only
	// pays for them when a run is explicitly asking where rows went.
	let base_worker = ToronWorker::with_partition_store_and_endpoint(
		storage,
		memory,
		partition_store.clone(),
		worker_id,
		endpoint.clone(),
	);
	let worker = Arc::new(if std::env::var("TORON_CENSUS_ROUTING_RECEIPTS").is_ok() {
		base_worker.with_shuffle_routing_observations()
	} else {
		base_worker
	});
	let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();
	let cancellation_cleanup = Arc::new(AtomicBool::new(false));
	let worker_cleanup = cancellation_cleanup.clone();
	let observed_worker = worker.clone();
	let peer_closes = Arc::new(AtomicUsize::new(0));
	let listener_peer_closes = peer_closes.clone();
	let handle = tokio::spawn(async move {
		let mut connections = tokio::task::JoinSet::new();
		loop {
			tokio::select! {
				_ = &mut shutdown_rx => break,
				accepted = listener.accept() => {
					let (stream, _) = accepted
						.map_err(|error| format!("execution census worker accept: {error}"))?;
					let worker = worker.clone();
					let peer_closes = listener_peer_closes.clone();
					connections.spawn(async move {
						let outcome = worker.handle_connection(stream).await;
						classify_census_connection_outcome(outcome, &peer_closes)
					});
				}
			}
		}
		while let Some(result) = connections.join_next().await {
			match result.map_err(|error| format!("execution census worker task join: {error}"))? {
				Ok(()) => {}
				Err(_) if worker_cleanup.load(Ordering::Acquire) => {}
				Err(error) => return Err(error),
			}
		}
		Ok(())
	});
	Ok(ExecutionCensusWorkerHarness {
		peer_closes,
		worker: observed_worker,
		entry: WorkerEntry {
			worker_id,
			endpoint,
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(1),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: budget.memory_per_task,
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: budget.spill_bytes_reserved_per_query,
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks: ActiveTaskCount::default(),
			},
			status: WorkerStatus::Live,
		},
		partition_store,
		cancellation_cleanup,
		shutdown,
		handle,
	})
}

enum ExecutionCensusDistributedAttempt {
	Completed(Result<Vec<RecordBatch>, QueryError>),
	LiveBackpressure(Vec<ExecutionCensusBackpressureReceipt>),
}

async fn execute_census_distributed_attempt(
	coordinator: &DistributedQueryCoordinator<StaticWorkerRegistry, TcpTransport>,
	workers: &[ExecutionCensusWorkerHarness],
	logical: &LogicalPlan,
	budget: QueryBudget,
) -> Result<ExecutionCensusDistributedAttempt, String> {
	let tenant_id = toron::storage::TenantId(1);
	let context = ExecutionContext::new(
		toron::ir::SessionId(529),
		QueryId::new(),
		tenant_id,
		toron::storage::HlcBound(1),
	)
	.with_task_budget(TaskBudget::from_query_budget(budget));
	let query_id = context.query_id;
	let mut execution = Box::pin(coordinator.execute_query_with_context(logical, context));
	let observation_start = Instant::now();
	let mut stall_tracker = StallPersistenceTracker::new(EXECUTION_CENSUS_STALL_PERSISTENCE_WINDOW);
	let mut observation_tick = tokio::time::interval(std::time::Duration::from_millis(1));
	observation_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	loop {
		tokio::select! {
			biased;
			result = &mut execution => {
				return Ok(ExecutionCensusDistributedAttempt::Completed(result));
			}
			_ = observation_tick.tick() => {
				let receipts = workers
					.iter()
					.filter_map(|worker| {
						let observation = worker
							.partition_store
							.live_backpressure_observation(query_id);
						(observation.blocked_appenders > 0).then_some(
							ExecutionCensusBackpressureReceipt {
								worker_id: worker.entry.worker_id,
								observation,
							},
						)
					})
					.collect::<Vec<_>>();
				let signature = (!receipts.is_empty())
					.then(|| LiveProgressSignature::from_receipts(&receipts));
				if matches!(
					stall_tracker.observe(observation_start.elapsed(), signature),
					StallVerdict::Progressing
				) {
					continue;
				}
				for worker in workers {
					worker.cancellation_cleanup.store(true, Ordering::Release);
				}
				coordinator.cancel_query_for_tenant(
					tenant_id,
					query_id,
					CancellationTrigger::SqlCancel,
				);
				let _cancellation_result =
					tokio::time::timeout(std::time::Duration::from_secs(5), &mut execution)
					.await
					.map_err(|_| "execution census backpressure cancellation did not quiesce".to_string())?;
				return Ok(ExecutionCensusDistributedAttempt::LiveBackpressure(receipts));
			}
		}
	}
}

/// Returns the number of peer closes tolerated across every worker, so a cell
/// can report the count it observed instead of it vanishing into a tolerance.
async fn shutdown_execution_census_workers(
	workers: Vec<ExecutionCensusWorkerHarness>,
) -> Result<usize, String> {
	let mut handles = Vec::with_capacity(workers.len());
	let mut counters = Vec::with_capacity(workers.len());
	for worker in workers {
		let _ = worker.shutdown.send(());
		counters.push(worker.peer_closes);
		handles.push(worker.handle);
	}
	for handle in handles {
		handle
			.await
			.map_err(|error| format!("execution census worker server join: {error}"))??;
	}
	// Read after every listener has joined: a connection still draining at the
	// call would otherwise be counted as zero.
	Ok(counters
		.iter()
		.map(|counter| counter.load(Ordering::Relaxed))
		.sum())
}

struct ExecutionCensusInput {
	query_id: &'static str,
	sql: Cow<'static, str>,
	tables: Vec<HarnessTableMetadata>,
	storage: Arc<dyn StorageScan>,
	dataset_verification: Option<DatasetVerification>,
}

async fn run_execution_census_input(
	input: ExecutionCensusInput,
	scale_factor: f64,
	mode: &str,
	distributed_memory_override: Option<u64>,
) -> Result<ExecutionCensusRow, String> {
	let total_start = Instant::now();
	let local_budget = execution_census_local_budget();
	let local_start = Instant::now();
	let local = execute_query_with_metadata_coordinate(
		&input.sql,
		input.tables.clone(),
		input.storage.clone(),
		gauge_planner_config_with_task_memory(Some(local_budget.memory_per_task)),
		Some(local_budget),
	)
	.await
	.map_err(|error| {
		format!(
			"execution census local oracle rejected at {}: {} ({})",
			error.phase.as_str(),
			error.reason,
			error.message
		)
	})?;
	let local_elapsed_ms = local_start.elapsed().as_secs_f64() * 1000.0;
	let local_identity = execution_identity_facts(&local.batches)?;

	let preflight_result = execution_census_preflight(&input.sql, input.tables).await;
	let rejected_width = EXECUTION_CENSUS_FALLBACK_WIDTH;
	let rejected_budget = execution_census_distributed_budget(rejected_width);
	let preflight = match preflight_result {
		Ok(preflight) => preflight,
		Err(rejection) => {
			return Ok(ExecutionCensusRow {
				query: input.query_id.to_string(),
				scale_factor,
				mode: mode.to_string(),
				phase: rejection.phase,
				classification: "execution_rejected".to_string(),
				reason: Some(rejection.reason),
				local_plan_fingerprint: Some(local.plan_fingerprint),
				distributed_plan_fingerprint: rejection.plan_fingerprint,
				local_digest: Some(local_identity.digest),
				distributed_digest: None,
				local_row_count: Some(local_identity.row_count),
				distributed_row_count: None,
				local_schema_fingerprint: local_identity.schema_fingerprint,
				distributed_schema_fingerprint: None,
				stage_count: None,
				stage_task_counts: None,
				live_eligible_consumer_tasks: None,
				stage_upstreams: None,
				live_producer_stages: None,
				multi_upstream_live_eligible_consumer_tasks: None,
				live_residency_requirement: None,
				admission_max_in_flight_tasks: None,
				residency_exceeds_admission: None,
				admitted_width: None,
				task_memory_envelope_bytes: EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES,
				local_memory_bytes: local_budget.memory_per_task.get(),
				local_spill_bytes: local_budget.spill_bytes_reserved_per_query.get(),
				distributed_budget: rejected_budget,
				worker_count: EXECUTION_CENSUS_WORKER_COUNT,
				transport: "tcp_loopback",
				partition_store: "in_memory",
				memory_pool_retained_buffers: EXECUTION_CENSUS_MEMORY_POOL_RETAINED_BUFFERS,
				memory_pool_max_buffer_capacity_bytes: usize::try_from(
					rejected_budget.memory_per_task.get(),
				)
				.unwrap_or(usize::MAX),
				distributed_attempts: 0,
				transport_retry: false,
				live_backpressure_receipts: Vec::new(),
				dataset_verification: input.dataset_verification,
				local_elapsed_ms: Some(local_elapsed_ms),
				distributed_elapsed_ms: None,
				total_elapsed_ms: total_start.elapsed().as_secs_f64() * 1000.0,
				diagnostic_message: Some(rejection.message),
			});
		}
	};
	let width = preflight.shape.max_stage_width.max(1);
	let distributed_budget = distributed_memory_override.map_or_else(
		|| execution_census_distributed_budget(width),
		|bytes| execution_census_distributed_budget_with_memory(width, bytes),
	);
	let memory_pool_max_buffer_capacity_bytes =
		usize::try_from(distributed_budget.memory_per_task.get()).unwrap_or(usize::MAX);
	let mut workers = Vec::with_capacity(EXECUTION_CENSUS_WORKER_COUNT);
	for worker_index in 0..EXECUTION_CENSUS_WORKER_COUNT {
		match spawn_execution_census_worker(
			worker_index,
			input.storage.clone(),
			distributed_budget,
			memory_pool_max_buffer_capacity_bytes,
		)
		.await
		{
			Ok(worker) => workers.push(worker),
			Err(error) => {
				shutdown_execution_census_workers(workers).await?;
				return Err(error);
			}
		}
	}
	let catalog: Arc<dyn Catalog> = Arc::new(preflight.catalog);
	let coordinator = build_execution_census_coordinator(
		catalog,
		&workers,
		memory_pool_max_buffer_capacity_bytes,
		distributed_budget,
	);
	let distributed_start = Instant::now();
	let mut distributed_attempts = 1;
	let mut transport_retry = false;
	let first_attempt = execute_census_distributed_attempt(
		&coordinator,
		&workers,
		&preflight.logical,
		distributed_budget,
	)
	.await?;
	let (distributed_result, live_backpressure_receipts) = match first_attempt {
		ExecutionCensusDistributedAttempt::Completed(Err(QueryError::Io(_))) => {
			transport_retry = true;
			distributed_attempts += 1;
			match execute_census_distributed_attempt(
				&coordinator,
				&workers,
				&preflight.logical,
				distributed_budget,
			)
			.await?
			{
				ExecutionCensusDistributedAttempt::Completed(result) => (Some(result), Vec::new()),
				ExecutionCensusDistributedAttempt::LiveBackpressure(receipts) => (None, receipts),
			}
		}
		ExecutionCensusDistributedAttempt::Completed(result) => (Some(result), Vec::new()),
		ExecutionCensusDistributedAttempt::LiveBackpressure(receipts) => (None, receipts),
	};
	let distributed_elapsed_ms = distributed_start.elapsed().as_secs_f64() * 1000.0;
	if std::env::var("TORON_CENSUS_ROUTING_RECEIPTS").is_ok() {
		for worker in &workers {
			for receipt in worker.worker.drain_shuffle_routing_observations() {
				eprintln!(
					"routing_receipt query={} role={:?} exchange={:?} stage={:?} partition={} rows={} schema_fp={:#x} key_fp={:?}",
					input.query_id,
					receipt.role,
					receipt.exchange_id,
					receipt.stage_id,
					receipt.partition_id,
					receipt.row_count,
					receipt.schema_fingerprint,
					receipt
						.key_fingerprint
						.map(|fingerprint| format!("{fingerprint:#x}")),
				);
			}
		}
	}
	shutdown_execution_census_workers(workers).await?;

	// Receipts now arrive only after the live shuffle stood still, with an appender blocked
	// throughout, for the whole persistence window. That is direct evidence of non-progress,
	// so it no longer matters whether some consumer was *eligible* to drain — it demonstrably
	// did not. This supersedes the earlier shape-based refusal, which asked whether a drainer
	// could exist and had to abandon the cell when one did; that question is answered by the
	// observation itself. Eligibility stays on the row as a diagnostic.
	let (classification, reason, distributed_identity, diagnostic_message) =
		if !live_backpressure_receipts.is_empty() {
			(
				"execution_rejected",
				Some("live_shuffle_backpressure_stalled".to_string()),
				None,
				Some(format!("{live_backpressure_receipts:?}")),
			)
		} else {
			match distributed_result.expect("completed attempt has a result") {
				Ok(batches) => {
					let identity = execution_identity_facts(&batches)?;
					let identical = local_identity.digest == identity.digest
						&& local_identity.row_count == identity.row_count
						&& local_identity.schema_fingerprint == identity.schema_fingerprint;
					(
						if identical {
							"executes_identical"
						} else {
							"executes_divergent"
						},
						None,
						Some(identity),
						None,
					)
				}
				Err(error) => (
					"execution_rejected",
					Some(execution_census_query_error_reason(&error)),
					None,
					Some(format!("{error:?}")),
				),
			}
		};
	let (distributed_digest, distributed_row_count, distributed_schema_fingerprint) =
		distributed_identity.map_or((None, None, None), |identity| {
			(
				Some(identity.digest),
				Some(identity.row_count),
				identity.schema_fingerprint,
			)
		});
	Ok(ExecutionCensusRow {
		query: input.query_id.to_string(),
		scale_factor,
		mode: mode.to_string(),
		phase: if classification == "execution_rejected" {
			Phase::Execute
		} else {
			Phase::Compare
		},
		classification: classification.to_string(),
		reason,
		local_plan_fingerprint: Some(local.plan_fingerprint),
		distributed_plan_fingerprint: Some(preflight.plan_fingerprint),
		local_digest: Some(local_identity.digest),
		distributed_digest,
		local_row_count: Some(local_identity.row_count),
		distributed_row_count,
		local_schema_fingerprint: local_identity.schema_fingerprint,
		distributed_schema_fingerprint,
		stage_count: Some(preflight.shape.stage_count),
		stage_task_counts: Some(preflight.shape.stage_task_counts),
		live_eligible_consumer_tasks: Some(preflight.shape.live_eligible_consumer_tasks),
		stage_upstreams: Some(preflight.shape.stage_upstreams.clone()),
		live_producer_stages: Some(preflight.shape.live_producer_stages),
		multi_upstream_live_eligible_consumer_tasks: Some(
			preflight.shape.multi_upstream_live_eligible_consumer_tasks,
		),
		live_residency_requirement: Some(preflight.shape.live_residency_requirement),
		admission_max_in_flight_tasks: preflight.shape.admission_max_in_flight_tasks,
		residency_exceeds_admission: Some(preflight.shape.residency_exceeds_admission),
		admitted_width: Some(width),
		task_memory_envelope_bytes: EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES,
		local_memory_bytes: local_budget.memory_per_task.get(),
		local_spill_bytes: local_budget.spill_bytes_reserved_per_query.get(),
		distributed_budget,
		worker_count: EXECUTION_CENSUS_WORKER_COUNT,
		transport: "tcp_loopback",
		partition_store: "in_memory",
		memory_pool_retained_buffers: EXECUTION_CENSUS_MEMORY_POOL_RETAINED_BUFFERS,
		memory_pool_max_buffer_capacity_bytes,
		distributed_attempts,
		transport_retry,
		live_backpressure_receipts,
		dataset_verification: input.dataset_verification,
		local_elapsed_ms: Some(local_elapsed_ms),
		distributed_elapsed_ms: Some(distributed_elapsed_ms),
		total_elapsed_ms: total_start.elapsed().as_secs_f64() * 1000.0,
		diagnostic_message,
	})
}

/// ADR 529's explicit execution-reachability cell. The persisted and in-memory forms both build
/// one spec and share its storage between the local oracle and the two production-path workers.
/// Elapsed fields are diagnostic only; classification is derived from typed walls or total identity.
pub async fn run_execution_census_query(
	query: &str,
	mode: &str,
	scale_factor: f64,
	data_source: &str,
	data_root: &Path,
) -> Result<ExecutionCensusRow, String> {
	let input = match data_source {
		"memory" => {
			let spec = build_perf_query_spec(query, mode, scale_factor)?;
			let (tables, storage) = metadata_storage_from_tables(spec.tables);
			ExecutionCensusInput {
				query_id: spec.query_id,
				sql: spec.sql,
				tables,
				storage,
				dataset_verification: None,
			}
		}
		"parquet" | "persisted" => {
			let spec = build_persisted_perf_query_spec(query, mode, scale_factor, data_root)?;
			let storage: Arc<dyn StorageScan> = spec.storage;
			ExecutionCensusInput {
				query_id: spec.query_id,
				sql: spec.sql,
				tables: spec.tables,
				storage,
				dataset_verification: Some(spec.dataset_verification),
			}
		}
		other => {
			return Err(format!(
				"execution-census --data-source {other:?} is not supported"
			));
		}
	};
	run_execution_census_input(input, scale_factor, mode, None).await
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
		ExecutionPlan::KernelDensity { input, spec } => format!(
			"KernelDensity(points={},cols=[{}],input={})",
			spec.points,
			schema_field_names(&spec.output_schema),
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
				partition_work_concurrency_observations: None,
				dataset_verification: None,
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
				partition_work_concurrency_observations: None,
				dataset_verification: None,
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
			partition_work_concurrency_observations: None,
			dataset_verification: None,
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
			partition_work_concurrency_observations: None,
			dataset_verification: None,
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
		partition_work_concurrency_observations: None,
		dataset_verification: None,
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
				partition_work_concurrency_observations: None,
				dataset_verification: None,
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
				partition_work_concurrency_observations: None,
				dataset_verification: None,
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
		partition_work_concurrency_observations: Some(
			execution.partition_work_concurrency_observations,
		),
		dataset_verification: None,
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
				partition_work_concurrency_observations: None,
				dataset_verification: None,
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
				partition_work_concurrency_observations: None,
				dataset_verification: None,
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
		partition_work_concurrency_observations: Some(
			execution.partition_work_concurrency_observations,
		),
		dataset_verification: None,
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
			let dataset_verification = spec.dataset_verification;
			let mut attempt = run_perf_query_with_metadata(
				spec.query_id,
				mode,
				scale_factor,
				run_index,
				warmup,
				&spec.sql,
				spec.tables,
				spec.storage,
			)
			.await;
			attempt.dataset_verification = Some(dataset_verification);
			Ok(attempt)
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
			let dataset_verification = spec.dataset_verification;
			let mut attempt = run_rune_q03_perf_query_with_metadata(
				spec.query_id,
				mode,
				scale_factor,
				run_index,
				warmup,
				spec.tables,
				spec.storage,
			)
			.await;
			attempt.dataset_verification = Some(dataset_verification);
			Ok(attempt)
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
	// Gated at the rail entry, not just the persisted arm: a correctness sweep that sets the skip env
	// should fail loudly and uniformly rather than silently producing a mix of fully-verified and
	// manifest-only answers depending on which `--data-source` each cell happened to use.
	require_full_dataset_verification("correctness")?;
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
		dataset_verification: Some(dataset.verification),
	};
	Ok(PersistedPerfQuerySpec {
		query_id,
		sql,
		tables,
		storage: Arc::new(ParquetHarnessScan {
			tables: scan_tables,
		}),
		export,
		dataset_verification: dataset.verification,
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

#[cfg(test)]
mod adr537_q11_constant_key_join {
	use super::*;

	/// Grounding for the q11 divergence: the HAVING scalar subquery lowers to an Inner
	/// HashPartitioned join whose key is the literal `1` on both sides. This pins the
	/// symmetry of the two join-input exchanges, which rules out the static explanations
	/// for zero distributed rows — the cause is not partition count, key type, key value,
	/// or consumer pairing. A future asymmetry here would reintroduce the class.
	/// Deep multi-join binds need an explicit stack; run on a dedicated 64 MB thread.
	#[test]
	fn q11_constant_key_join_exchanges_are_symmetric() {
		let (producers, consumers) = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("current-thread runtime");
				runtime.block_on(async {
					let spec =
						build_perf_query_spec("q11", "strict", 0.01).expect("q11 spec at sf=0.01");
					let (tables, _storage) = metadata_storage_from_tables(spec.tables);
					let mut timings = PerfPhaseTimings::default();
					let catalog = load_catalog_from_metadata(tables, &mut timings, Instant::now())
						.expect("catalog");
					let logical = SqlPlanner::new().plan(&spec.sql).expect("plan q11");
					let seed_budget =
						execution_census_distributed_budget(EXECUTION_CENSUS_FALLBACK_WIDTH);
					let planner_config =
						execution_census_distributed_planner_config(seed_budget.memory_per_task);
					let bound =
						bind_with_local_join_admissions(&logical, &catalog, &planner_config)
							.await
							.map(|(bound, _)| bound)
							.expect("bind q11 under the distributed posture");
					let task_telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
					let context = tpch_execution_context_with_budget(task_telemetry, seed_budget);
					let fragment_config = FragmentPlanConfig::new(
						context.query_id,
						TaskExecutionContext::from_execution_context(&context),
						context.task_budget,
					)
					.with_join_runtime_observation_mode(
						context.join_runtime_observation_mode.clone(),
					)
					.with_max_in_flight_tasks(seed_budget.max_in_flight_tasks);
					let stages = fragment_plan(bound, fragment_config).expect("fragment q11");

					// The two join-input producers, by partition count and key type.
					let producers = stages
						.iter()
						.flat_map(|stage| stage.tasks.iter())
						.filter_map(|task| match &task.spec.output {
							TaskOutputBinding::ShuffleProducer {
								partition_count,
								partition_key,
								..
							} if !partition_key.roots.is_empty() => {
								Some((partition_count.get(), partition_key.node_types.clone()))
							}
							_ => None,
						})
						.collect::<Vec<_>>();

					// Every consuming task pairs the same partition id from both upstreams.
					let consumers = stages
						.iter()
						.flat_map(|stage| stage.tasks.iter())
						.filter_map(|task| {
							let bound = task
								.spec
								.inputs
								.iter()
								.filter_map(|input| {
									match input {
									toron::distributed::task::TaskInputBinding::ShuffleConsumer {
										partition_id,
										..
									} => Some(*partition_id),
									toron::distributed::task::TaskInputBinding::Scan { .. } => None,
								}
								})
								.collect::<Vec<_>>();
							(bound.len() > 1).then_some(bound)
						})
						.collect::<Vec<_>>();

					(producers, consumers)
				})
			})
			.expect("spawn grounding thread")
			.join()
			.expect("grounding thread");

		assert_eq!(
			producers.len(),
			2,
			"q11 lowers to exactly two keyed join-input producers"
		);
		assert_eq!(
			producers[0], producers[1],
			"both sides of a constant-key join must partition identically; an asymmetry \
			 here sends the sides to different partitions and empties the join"
		);
		assert!(
			producers
				.iter()
				.all(|(count, types)| *count > 1 && types.len() == 1),
			"each side partitions on a single key column across more than one partition: {producers:?}"
		);

		assert!(
			!consumers.is_empty(),
			"the join consumer stage must bind both upstreams"
		);
		for bound in &consumers {
			assert!(
				bound.windows(2).all(|pair| pair[0] == pair[1]),
				"a consuming task must read the same partition id from every upstream: {bound:?}"
			);
		}
	}
}

#[cfg(test)]
mod adr536_stall_persistence {
	use super::*;
	use std::time::Duration;

	const WINDOW: Duration = Duration::from_secs(2);

	fn signature(live_partitions: usize, buffered: usize, blocked: usize) -> LiveProgressSignature {
		LiveProgressSignature(vec![(7, live_partitions, buffered, blocked)])
	}

	/// Control 1 — transient blocked pacing. A bounded backlog blocks an appender while
	/// the consumer drains; chunks keep moving, so no sample sequence may ever be
	/// classified stalled, however long it runs.
	#[test]
	fn blocked_appender_that_keeps_draining_is_never_stalled() {
		let mut tracker = StallPersistenceTracker::new(WINDOW);
		let mut buffered = 32;
		for tick in 0..200 {
			// Blocked throughout, but the backlog is being released.
			buffered = if buffered == 0 { 32 } else { buffered - 1 };
			let verdict = tracker.observe(
				Duration::from_millis(tick * 50),
				Some(signature(8, buffered, 1)),
			);
			assert_eq!(
				verdict,
				StallVerdict::Progressing,
				"a draining backlog is designed pacing, not a stall (tick {tick})"
			);
		}
	}

	/// A query that clears its backpressure entirely completes normally.
	#[test]
	fn cleared_backpressure_resets_the_persistence_window() {
		let mut tracker = StallPersistenceTracker::new(WINDOW);
		assert_eq!(
			tracker.observe(Duration::ZERO, Some(signature(8, 32, 1))),
			StallVerdict::Progressing
		);
		// No blocked appender anywhere: unambiguous progress.
		assert_eq!(
			tracker.observe(Duration::from_secs(1), None),
			StallVerdict::Progressing
		);
		// The same signature returning later starts a fresh window rather than
		// inheriting the earlier one.
		assert_eq!(
			tracker.observe(Duration::from_secs(2), Some(signature(8, 32, 1))),
			StallVerdict::Progressing
		);
		assert_eq!(
			tracker.observe(Duration::from_millis(3_999), Some(signature(8, 32, 1))),
			StallVerdict::Progressing
		);
	}

	/// Control 2 — persistent non-progress. Nothing moves while an appender stays
	/// blocked, so past the window the wait is evidence of a non-returning cycle.
	#[test]
	fn unchanging_blocked_state_past_the_window_is_stalled() {
		let mut tracker = StallPersistenceTracker::new(WINDOW);
		assert_eq!(
			tracker.observe(Duration::ZERO, Some(signature(8, 32, 1))),
			StallVerdict::Progressing,
			"the first sample can never be the verdict"
		);
		assert_eq!(
			tracker.observe(Duration::from_millis(1_999), Some(signature(8, 32, 1))),
			StallVerdict::Progressing,
			"inside the window the wait is still admissible"
		);
		assert_eq!(
			tracker.observe(Duration::from_secs(2), Some(signature(8, 32, 1))),
			StallVerdict::Stalled,
			"at the window boundary the non-progress is proven"
		);
	}

	/// The pre-ADR-536 rail cancelled on the first blocked-appender sample. That behavior
	/// is what these controls forbid: it cannot distinguish the two cases above.
	#[test]
	fn first_sample_alone_is_never_a_verdict() {
		for blocked in [1, 2, 9] {
			let mut tracker = StallPersistenceTracker::new(WINDOW);
			assert_eq!(
				tracker.observe(Duration::ZERO, Some(signature(8, 32, blocked))),
				StallVerdict::Progressing
			);
		}
	}
}

#[cfg(test)]
mod adr508_q11_sf8_scale {
	use super::*;

	#[test]
	fn sf8_uses_one_exact_scale_contract_for_sql_and_oracle() {
		let scale = Q11Scale::try_from_f64(8.0).expect("sf=8 is exactly supported");
		assert_eq!(scale.hundredths_n(), 800);
		assert_eq!(scale.fraction_sql(), "0.0000125");

		let sql = q11_sql(scale);
		assert_eq!(sql.matches("0.0000125").count(), 1);
		assert!(!sql.contains("[FRACTION]"));

		// At sf=8 the threshold is global / 80,000. A value exactly on that
		// boundary must fail because TPC-H q11 uses a strict comparison.
		let oracle = q11_oracle_from_rows(
			[(1, 1, 1, 1), (2, 1, 1, 79_999)],
			[(1, 1)],
			[(1, "GERMANY".to_string())],
			scale,
		)
		.expect("sf=8 oracle");
		assert_eq!(oracle.global_value_raw, 80_000);
		assert_eq!(
			oracle.rows,
			vec![Q11Row {
				ps_partkey: 2,
				value_raw: 79_999,
			}]
		);
		assert_eq!(oracle.threshold_fail_groups, 1);
		assert_eq!(oracle.threshold_failed_partkeys, BTreeSet::from([1]));
	}
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
			// The membership invariant is position-agnostic: a keyed LeftSemi
			// whose build side is the grouped body and whose preserved side
			// carries the membership key. ADR 515's commute relocates the semi
			// onto the orders branch, so the output schema is the branch's, not
			// the composite's.
			(*kind == JoinKind::LeftSemi
				&& left_keys.len() == 1
				&& right_keys.len() == 1
				&& schema_has_fields(output_schema, &["o_orderkey"])
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
		assert_eq!(
			row["partition_work_concurrency_observations"],
			serde_json::json!([]),
			"an in-memory q06 must not counterfeit reach of the route-local control"
		);
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

	fn write_unique_key_fixture(
		label: &str,
		schema: SchemaRef,
		columns: Vec<ArrayRef>,
	) -> (PathBuf, RecordBatch) {
		let root = temp_root(label);
		std::fs::create_dir_all(&root).expect("fixture root");
		let path = root.join("keys.parquet");
		let batch = RecordBatch::try_new(schema.clone(), columns).expect("key fixture batch");
		let mut writer = parquet_writer(&path, schema).expect("key fixture writer");
		writer.write(&batch).expect("write key fixture");
		writer.close().expect("close key fixture");
		(path, batch)
	}

	#[test]
	fn snapshot_unique_key_verifier_is_exact_ordered_and_fail_closed() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("a", DataType::Int64, false),
			Field::new("b", DataType::Int32, false),
		]));
		let (path, batch) = write_unique_key_fixture(
			"unique_composite",
			schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1_i64, 1, 2])),
				Arc::new(Int32Array::from(vec![10_i32, 11, 10])),
			],
		);
		let proof = verify_snapshot_unique_key_candidate(
			&path,
			schema.as_ref(),
			batch.num_rows() as u64,
			SnapshotUniqueKeyCandidate {
				table: "fixture",
				columns: &["b", "a"],
			},
		)
		.expect("verify composite")
		.expect("composite is unique");
		assert_eq!(proof.columns(), &[ColumnOrdinal(1), ColumnOrdinal(0)]);

		let incomplete = verify_snapshot_unique_key_candidate(
			&path,
			schema.as_ref(),
			(batch.num_rows() + 1) as u64,
			SnapshotUniqueKeyCandidate {
				table: "fixture",
				columns: &["a", "b"],
			},
		)
		.expect_err("row-count mismatch is an incomplete scan");
		assert!(incomplete.contains("incomplete"));
		let invalid = verify_snapshot_unique_key_candidate(
			&path,
			schema.as_ref(),
			batch.num_rows() as u64,
			SnapshotUniqueKeyCandidate {
				table: "fixture",
				columns: &["missing"],
			},
		)
		.expect_err("unknown candidate column");
		assert!(invalid.contains("absent"));
		let _ = std::fs::remove_dir_all(path.parent().expect("fixture parent"));

		let duplicate_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
		let (duplicate_path, duplicate_batch) = write_unique_key_fixture(
			"unique_duplicate",
			duplicate_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 1]))],
		);
		assert!(
			verify_snapshot_unique_key_candidate(
				&duplicate_path,
				duplicate_schema.as_ref(),
				duplicate_batch.num_rows() as u64,
				SnapshotUniqueKeyCandidate {
					table: "fixture",
					columns: &["k"],
				},
			)
			.expect("duplicate verification")
			.is_none()
		);
		let _ = std::fs::remove_dir_all(duplicate_path.parent().expect("fixture parent"));

		let nullable_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
		let (nullable_path, nullable_batch) = write_unique_key_fixture(
			"unique_null",
			nullable_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![Some(1_i64), None]))],
		);
		assert!(
			verify_snapshot_unique_key_candidate(
				&nullable_path,
				nullable_schema.as_ref(),
				nullable_batch.num_rows() as u64,
				SnapshotUniqueKeyCandidate {
					table: "fixture",
					columns: &["k"],
				},
			)
			.expect("nullable verification")
			.is_none()
		);
		let _ = std::fs::remove_dir_all(nullable_path.parent().expect("fixture parent"));

		let unsupported_schema =
			Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
		let (unsupported_path, unsupported_batch) = write_unique_key_fixture(
			"unique_unsupported",
			unsupported_schema.clone(),
			vec![Arc::new(StringArray::from(vec!["a", "b"]))],
		);
		assert!(
			verify_snapshot_unique_key_candidate(
				&unsupported_path,
				unsupported_schema.as_ref(),
				unsupported_batch.num_rows() as u64,
				SnapshotUniqueKeyCandidate {
					table: "fixture",
					columns: &["k"],
				},
			)
			.expect("unsupported verification")
			.is_none()
		);
		let _ = std::fs::remove_dir_all(unsupported_path.parent().expect("fixture parent"));
	}

	#[test]
	fn manifest_unique_key_round_trip_preserves_composite_order_and_binding() {
		let schema = Schema::new(vec![
			Field::new("a", DataType::Int64, false),
			Field::new("b", DataType::Int32, false),
		]);
		let table = TpchPersistedTable {
			name: "fixture".to_string(),
			path: PathBuf::from("fixture.parquet"),
			row_count: 2,
			logical_bytes: 16,
			schema_fingerprint: "schema".to_string(),
			schema_signature: "signature".to_string(),
			file_hash: "hash".to_string(),
			unique_key_evidence: vec![
				UniqueKeyProof::verified_snapshot(vec![ColumnOrdinal(1), ColumnOrdinal(0)])
					.expect("proof"),
			],
		};
		let mut table_json = serde_json::json!({
			"unique_key_proofs": manifest_unique_key_proofs_json(&table),
		});
		let parsed = parse_manifest_unique_key_proofs(
			&table_json,
			&table.name,
			&schema,
			&table.schema_fingerprint,
			&table.file_hash,
			Path::new("manifest.json"),
		)
		.expect("parse proof");
		assert_eq!(parsed[0].columns(), &[ColumnOrdinal(1), ColumnOrdinal(0)]);

		table_json["unique_key_proofs"][0]["file_hash"] =
			serde_json::Value::String("other".to_string());
		assert!(
			parse_manifest_unique_key_proofs(
				&table_json,
				&table.name,
				&schema,
				&table.schema_fingerprint,
				&table.file_hash,
				Path::new("manifest.json"),
			)
			.expect_err("tampered binding")
			.contains("binding mismatch")
		);
	}

	#[test]
	fn persisted_manifest_rejects_a_tampered_unique_key_binding() {
		let root = temp_root("tampered_unique_binding");
		let dataset =
			prepare_persisted_tpch_dataset("strict", 0.01, &root).expect("prepare dataset");
		let body = std::fs::read_to_string(&dataset.manifest_path).expect("read manifest");
		let mut manifest: serde_json::Value = serde_json::from_str(&body).expect("parse manifest");
		let customer = manifest["tables"]
			.as_array_mut()
			.expect("table array")
			.iter_mut()
			.find(|table| table["name"] == "customer")
			.expect("customer table");
		customer["unique_key_proofs"][0]["file_hash"] =
			serde_json::Value::String("fnv1a64:0000000000000000".to_string());
		std::fs::write(
			&dataset.manifest_path,
			format!(
				"{}\n",
				serde_json::to_string_pretty(&manifest).expect("encode manifest")
			),
		)
		.expect("write tampered manifest");

		let error = load_persisted_tpch_dataset_with_verification(
			"strict",
			0.01,
			&root,
			DatasetVerification::FullHash,
		)
		.expect_err("tampered proof binding must fail manifest load");
		assert!(
			error.contains("unique-key proof binding mismatch"),
			"{error}"
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

/// The dataset-verification provenance rail: which reconciliation a persisted dataset actually got,
/// that it reaches the row, and that the rails making answer claims refuse the weakened one.
#[cfg(test)]
mod dataset_verification_provenance {
	use super::*;
	use serde_json::Value;

	fn temp_root(label: &str) -> PathBuf {
		let root = std::env::temp_dir().join(format!(
			"toron_tpch_dataset_verification_{label}_{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&root);
		root
	}

	fn dataset_with(verification: DatasetVerification) -> TpchPersistedDataset {
		TpchPersistedDataset {
			mode: "strict".to_string(),
			scale_factor: 0.01,
			root_dir: PathBuf::from("/nonexistent"),
			manifest_path: PathBuf::from("/nonexistent/manifest.json"),
			manifest_fingerprint: "fnv1a64:0123456789abcdef".to_string(),
			tables: BTreeMap::new(),
			verification,
		}
	}

	#[test]
	fn any_value_of_the_skip_env_weakens_verification() {
		assert_eq!(
			dataset_verification_from_env(None),
			DatasetVerification::FullHash
		);
		assert_eq!(
			dataset_verification_from_env(Some(OsStr::new("1"))),
			DatasetVerification::ManifestOnly
		);
		// The footgun worth pinning: the gate is presence-based, so `=0` does NOT re-enable full
		// verification. A reader who assumes otherwise would ship a manifest-only measurement
		// believing it was full-hash.
		assert_eq!(
			dataset_verification_from_env(Some(OsStr::new("0"))),
			DatasetVerification::ManifestOnly
		);
		assert_eq!(
			dataset_verification_from_env(Some(OsStr::new(""))),
			DatasetVerification::ManifestOnly
		);
	}

	#[test]
	fn answer_claiming_rails_refuse_a_manifest_only_dataset() {
		assert!(require_full_dataset_verification_from_env(None, "correctness").is_ok());

		let error =
			require_full_dataset_verification_from_env(Some(OsStr::new("1")), "correctness")
				.expect_err("correctness must refuse a manifest-only dataset");
		assert!(error.contains("correctness"), "names the rail: {error}");
		assert!(
			error.contains(SKIP_DATASET_HASH_VERIFY_ENV),
			"names the env var to unset: {error}"
		);
	}

	#[test]
	fn paired_ablation_admits_full_hash_and_rejects_manifest_only() {
		let admitted =
			paired_ablation_dataset_fingerprint(&dataset_with(DatasetVerification::FullHash))
				.expect("full-hash dataset is admitted");
		assert_eq!(admitted, "fnv1a64:0123456789abcdef");

		let error =
			paired_ablation_dataset_fingerprint(&dataset_with(DatasetVerification::ManifestOnly))
				.expect_err("manifest-only dataset is refused");
		assert!(error.contains("paired ablation"), "names the rail: {error}");
		assert!(
			error.contains(SKIP_DATASET_HASH_VERIFY_ENV),
			"names the env var to unset: {error}"
		);
	}

	#[test]
	fn perf_row_reports_each_verification_state_distinctly() {
		let mut attempt = PerfAttempt {
			engine: "toron".to_string(),
			query: "q06".to_string(),
			scale_factor: 0.01,
			mode: "strict".to_string(),
			run_index: 0,
			warmup: false,
			phase: Phase::Execute,
			status: Status::Pass,
			reason: None,
			diagnostic_message: None,
			timings: PerfPhaseTimings::default(),
			parent_wall_ms: None,
			peak_rss_kib: None,
			spill_bytes: 0,
			plan_fingerprint: None,
			digest: None,
			output_row_count: None,
			output_schema_fingerprint: None,
			engine_version: None,
			duckdb_version: None,
			debug_build: cfg!(debug_assertions),
			local_join_admissions: None,
			join_spill_observations: None,
			partition_work_concurrency_observations: None,
			dataset_verification: None,
		};

		assert_eq!(attempt.json_value()["dataset_verification"], Value::Null);

		attempt.dataset_verification = Some(DatasetVerification::FullHash);
		assert_eq!(
			attempt.json_value()["dataset_verification"],
			Value::from("full_hash")
		);

		attempt.dataset_verification = Some(DatasetVerification::ManifestOnly);
		assert_eq!(
			attempt.json_value()["dataset_verification"],
			Value::from("manifest_only")
		);
	}

	#[test]
	fn a_freshly_prepared_dataset_is_full_hash_and_survives_reload() {
		let root = temp_root("prepare_reload");
		let prepared = prepare_persisted_tpch_dataset("strict", 0.01, &root).expect("prepare");
		assert_eq!(
			prepared.verification,
			DatasetVerification::FullHash,
			"a dataset whose manifest hashes were just computed over the written bytes is full-hash"
		);

		let reloaded = load_persisted_tpch_dataset("strict", 0.01, &root).expect("reload");
		assert_eq!(reloaded.verification, DatasetVerification::FullHash);
		assert_eq!(prepared.manifest_fingerprint, reloaded.manifest_fingerprint);
		let customer_proof = reloaded.tables["customer"]
			.unique_key_evidence
			.first()
			.expect("customer snapshot proof");
		assert_eq!(customer_proof.columns(), &[ColumnOrdinal(0)]);
		assert_eq!(
			customer_proof.provenance(),
			toron::catalog::UniqueKeyProofProvenance::VerifiedSnapshot
		);

		let manifest_only = load_persisted_tpch_dataset_with_verification(
			"strict",
			0.01,
			&root,
			DatasetVerification::ManifestOnly,
		)
		.expect("manifest-only load");
		assert!(
			manifest_only
				.tables
				.values()
				.all(|table| table.unique_key_evidence.is_empty()),
			"manifest-only loads cannot expose snapshot proofs"
		);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[tokio::test]
	async fn q13_catalog_load_exposes_the_verified_customer_key() {
		let root = temp_root("q13_unique_key");
		let spec = build_persisted_perf_query_spec("q13", "strict", 0.01, &root).expect("q13 spec");
		let mut timings = PerfPhaseTimings::default();
		let catalog = load_catalog_from_metadata(spec.tables, &mut timings, Instant::now())
			.expect("load q13 catalog");
		let customer = toron::catalog::Catalog::resolve_table(&catalog, &TableRef::new("customer"))
			.await
			.expect("resolve customer");

		assert_eq!(
			customer
				.unique_key_proof(&[ColumnOrdinal(0)])
				.expect("q13 uniqueness gate proof")
				.provenance(),
			toron::catalog::UniqueKeyProofProvenance::VerifiedSnapshot
		);
		let _ = std::fs::remove_dir_all(&root);
	}

	/// The peer-engine arm reads the same persisted files through the export, so a `toron,duckdb`
	/// board must not be half-audited: the export carries the same verification the toron row states.
	#[test]
	fn the_persisted_export_carries_verification_for_peer_engine_rows() {
		let root = temp_root("export_provenance");
		let persisted =
			export_persisted_perf_query_parquet("q06", "strict", 0.01, &root).expect("export");
		assert_eq!(
			persisted.dataset_verification,
			Some(DatasetVerification::FullHash)
		);

		// The in-memory export writes throwaway parquet from fixtures; no persisted dataset backed
		// it, so it claims nothing rather than inheriting a verification it never performed.
		let from_memory = export_perf_query_parquet("q06", "strict", 0.01, &root).expect("export");
		assert_eq!(from_memory.dataset_verification, None);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn node_attribution_outer_reconciliation_computes_residual_and_boundary() {
		use super::{
			NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS, node_attribution_outer_reconciliation,
		};

		// A profile whose root span is 149 us short of execute_ms — the real q17 shape. The outer
		// residual is that gap, and it is well within the declared 10 ms boundary.
		let profile = serde_json::json!({ "execute_bound_span_micros": 34_897_625u64 });
		let reconciliation = node_attribution_outer_reconciliation(&profile, 34_897.774);
		assert_eq!(reconciliation["outer_residual_micros"], 149);
		assert_eq!(
			reconciliation["boundary_micros"],
			NODE_ATTRIBUTION_OUTER_BOUNDARY_MICROS
		);
		assert_eq!(reconciliation["within_boundary"], true);

		// A span that accounts for only half of execute_ms — a profiler that missed whole operators.
		// The residual blows the boundary; the join reports the failure rather than hiding it.
		let broken = serde_json::json!({ "execute_bound_span_micros": 5_000_000u64 });
		let reconciliation = node_attribution_outer_reconciliation(&broken, 10_000.0);
		assert_eq!(reconciliation["outer_residual_micros"], 5_000_000);
		assert_eq!(reconciliation["within_boundary"], false);
	}

	/// The reader must fail closed. Every input it cannot trust has to report `within_boundary: false`
	/// with a reason — an unusable reconciliation that read as a pass would be worse than none, since
	/// the whole point of the outer band is to catch a profile that did not account for its query.
	#[test]
	fn node_attribution_outer_reconciliation_fails_closed_on_untrustworthy_input() {
		use super::node_attribution_outer_reconciliation;

		// An inverted band — span longer than execute_ms — is physically impossible and means the
		// measurement is broken. It must not saturate to a zero residual and pass.
		let profile = serde_json::json!({ "execute_bound_span_micros": 2_000_000u64 });
		let inverted = node_attribution_outer_reconciliation(&profile, 1_000.0);
		assert_eq!(inverted["within_boundary"], false);
		assert!(
			inverted["error"]
				.as_str()
				.expect("reason")
				.contains("inverted band"),
			"inverted band names its cause: {inverted}"
		);
		assert!(
			inverted.get("outer_residual_micros").is_none(),
			"no residual is published for a band that cannot be reconciled"
		);

		// A missing execute_ms (the caller passes NaN) is not a zero-length query.
		let missing_execute = node_attribution_outer_reconciliation(&profile, f64::NAN);
		assert_eq!(missing_execute["within_boundary"], false);
		assert!(
			missing_execute["error"]
				.as_str()
				.expect("reason")
				.contains("execute_ms")
		);
		// A negative execute_ms is equally untrustworthy.
		assert_eq!(
			node_attribution_outer_reconciliation(&profile, -1.0)["within_boundary"],
			false
		);

		// A profile with no span field at all — e.g. a truncated or foreign JSON document.
		let no_span = node_attribution_outer_reconciliation(&serde_json::json!({}), 1_000.0);
		assert_eq!(no_span["within_boundary"], false);
		assert!(
			no_span["error"]
				.as_str()
				.expect("reason")
				.contains("execute_bound_span_micros")
		);
		// A span present but not a number must fail the same way, not coerce to zero.
		let bad_span = node_attribution_outer_reconciliation(
			&serde_json::json!({ "execute_bound_span_micros": "1000" }),
			1_000.0,
		);
		assert_eq!(bad_span["within_boundary"], false);
	}

	#[tokio::test]
	async fn persisted_rows_carry_verification_and_memory_rows_do_not() {
		let root = temp_root("row_provenance");
		let persisted =
			run_perf_query_with_data_source("q06", "strict", 0.01, 0, false, "parquet", &root)
				.await
				.expect("persisted perf");
		assert_eq!(persisted.status, Status::Pass);
		assert_eq!(
			persisted.json_value()["dataset_verification"],
			Value::from("full_hash"),
			"a persisted row states what its dataset load proved"
		);

		let memory =
			run_perf_query_with_data_source("q06", "strict", 0.01, 0, false, "memory", &root)
				.await
				.expect("memory perf");
		assert_eq!(
			memory.json_value()["dataset_verification"],
			Value::Null,
			"an in-memory row loaded no dataset, so it claims no verification"
		);
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
			| ExecutionPlan::Repeat { input, .. }
			| ExecutionPlan::KernelDensity { input, .. } => join_kinds(input, kinds),
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
			ExecutionPlan::KernelDensity { input, .. } => {
				format!("KernelDensity({})", plan_shape(input))
			}
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

#[cfg(test)]
mod adr528_fragment_census {
	use super::*;

	#[test]
	fn fragment_error_classification_is_typed_slugs() {
		assert_eq!(
			classify_fragment_error(&FragmentError::JoinInputNotExchanged),
			"fragment_join_input_not_exchanged"
		);
		assert_eq!(
			classify_fragment_error(&FragmentError::MultipleExchangeBoundaries { count: 3 }),
			"fragment_multiple_exchange_boundaries"
		);
		assert_eq!(
			classify_fragment_error(&FragmentError::BroadcastProbeNotStarLinear),
			"fragment_broadcast_probe_not_star_linear"
		);
		assert_eq!(
			classify_fragment_error(&FragmentError::CreateTableNotFragmentable),
			"fragment_coordinator_only_operation"
		);
	}

	/// A grouped aggregate is the by-construction fragmenting shape (producer + group-keyed
	/// exchange consumer, pinned at the toron fragmenter level): one single-task producer
	/// stage publishing to the exchange, one consumer stage at the aggregate partition width.
	#[tokio::test]
	async fn grouped_aggregate_fragments_parallel_with_shape_receipt() {
		let spec = build_perf_query_spec("q06", "strict", 0.01).expect("q06 spec");
		let (metadata, _storage) = metadata_storage_from_tables(spec.tables);
		let (fingerprint, outcome) = fragment_census_outcome_with_metadata(
			"SELECT l_shipdate, COUNT(*) FROM lineitem GROUP BY l_shipdate",
			metadata,
		)
		.await;
		assert!(fingerprint.is_some(), "a bound plan has a fingerprint");
		let FragmentCensusOutcome::Fragmented(receipt) = outcome else {
			panic!("grouped aggregate must fragment, got {outcome:?}");
		};
		assert_eq!(receipt.stage_count, 2);
		assert_eq!(receipt.stage_task_counts.len(), receipt.stage_count);
		assert!(
			receipt.max_stage_width > 1,
			"the consumer stage fans out to the aggregate partition width: {receipt:?}"
		);
		assert_eq!(
			receipt.single_task_producer_stages, 1,
			"the fragmenter pins one single-task producer: {receipt:?}"
		);
		let row = fragment_census_row(
			"probe",
			"strict",
			0.01,
			fingerprint,
			FragmentCensusOutcome::Fragmented(receipt),
			0.0,
		);
		assert_eq!(row.classification, "fragments_parallel");
		assert_eq!(row.phase, Phase::Fragment);
	}

	/// DDL is the by-construction rejecting shape: `fragment_plan` refuses coordinator-only
	/// operations at its entry, before any recursion.
	#[tokio::test]
	async fn ddl_statement_is_fragment_rejected_with_typed_reason() {
		let spec = build_perf_query_spec("q06", "strict", 0.01).expect("q06 spec");
		let (metadata, _storage) = metadata_storage_from_tables(spec.tables);
		let (_fingerprint, outcome) = fragment_census_outcome_with_metadata(
			"CREATE TABLE fragment_census_probe (x INTEGER)",
			metadata,
		)
		.await;
		match outcome {
			FragmentCensusOutcome::FragmentRejected { reason, .. } => {
				assert_eq!(reason, "fragment_coordinator_only_operation");
			}
			FragmentCensusOutcome::StageRejected { phase, reason, .. } => {
				panic!("DDL must reach the fragmenter, rejected at {phase:?}: {reason}");
			}
			FragmentCensusOutcome::Fragmented(receipt) => {
				panic!("DDL must not fragment: {receipt:?}");
			}
		}
	}

	/// The census cell entry produces a definitive predeclared classification and a coherent
	/// row — without pre-judging any q-cell's census answer.
	#[tokio::test]
	async fn census_cell_yields_definitive_classification() {
		let row = run_fragment_census_query("q06", "strict", 0.01, "memory", Path::new("unused"))
			.await
			.expect("census cell");
		assert!(
			[
				"bind_rejected",
				"fragment_rejected",
				"fragments_sequential",
				"fragments_parallel"
			]
			.contains(&row.classification.as_str()),
			"classification {} is outside the predeclared vocabulary",
			row.classification
		);
		match row.classification.as_str() {
			"fragments_sequential" | "fragments_parallel" => {
				assert_eq!(row.phase, Phase::Fragment);
				let counts = row.stage_task_counts.as_ref().expect("stage task counts");
				assert_eq!(row.stage_count, Some(counts.len()));
				assert_eq!(row.max_stage_width, counts.iter().copied().max());
				assert!(row.reason.is_none());
			}
			"fragment_rejected" => {
				assert_eq!(row.phase, Phase::Fragment);
				assert!(row.reason.as_deref().unwrap_or("").starts_with("fragment_"));
				assert!(row.stage_count.is_none());
			}
			_ => {
				assert_ne!(row.phase, Phase::Fragment);
				assert!(row.reason.is_some());
			}
		}
		let jsonl = row.jsonl();
		assert!(jsonl.contains("\"suite\":\"tpch_fragment_census\""));
	}
}

#[cfg(test)]
mod adr529_execution_census {
	use super::*;

	/// Board-wide blast radius of live multi-upstream publication, pinned at unbounded
	/// admission. Exactly one cell reaches it; every other cell fragments with no
	/// live-eligible shuffle consumer at all.
	///
	/// This replaces an earlier pin asserting zero board-wide. That held only because the
	/// counter recognized the unary streamable consumer alone and was blind to the
	/// multi-upstream arm; corrected, q04 reports its four join consumer tasks. Stall
	/// classification no longer rests on this count — persistence evidence does — so a
	/// nonzero cell is now a fact about plan shape, not a threat to soundness.
	/// Deep multi-join binds (q10+) need an explicit stack; run on a dedicated 64 MB thread
	/// with a current-thread runtime.
	#[test]
	fn live_eligible_consumers_are_confined_to_one_board_cell() {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("current-thread runtime");
				let mut live_cells: Vec<(String, usize)> = Vec::new();
				runtime.block_on(async {
					for query in ALL_QUERY_IDS {
						let row = run_fragment_census_query(
							query,
							"strict",
							0.01,
							"memory",
							Path::new("unused"),
						)
						.await
						.expect("fragment census cell");
						if row.stage_count.is_some()
							&& row.live_eligible_consumer_tasks.unwrap_or(0) > 0
						{
							live_cells.push((
								query.to_string(),
								row.live_eligible_consumer_tasks.unwrap_or(0),
							));
						}
					}
				});
				assert_eq!(
					live_cells,
					vec![("q04".to_string(), 4), ("q13".to_string(), 4)],
					"live multi-upstream publication is confined to a known set at this \
					 scale and admission; a change here re-derives the blast radius"
				);
			})
			.expect("spawn premise thread")
			.join()
			.expect("premise thread");
	}

	fn memory_execution_input(query: &str, sql: Option<Cow<'static, str>>) -> ExecutionCensusInput {
		let spec = build_perf_query_spec(query, "strict", 0.01).expect("execution census spec");
		let (tables, storage) = metadata_storage_from_tables(spec.tables);
		ExecutionCensusInput {
			query_id: spec.query_id,
			sql: sql.unwrap_or(spec.sql),
			tables,
			storage,
			dataset_verification: None,
		}
	}

	#[tokio::test]
	async fn two_worker_grouped_aggregate_matches_local_identity() {
		let input = memory_execution_input(
			"q06",
			Some(Cow::Borrowed(
				"SELECT l_shipdate, COUNT(*) FROM lineitem GROUP BY l_shipdate",
			)),
		);
		let row = run_execution_census_input(input, 0.01, "strict", None)
			.await
			.expect("grouped execution census");
		assert_eq!(row.classification, "executes_identical");
		assert_eq!(row.worker_count, 2);
		assert_eq!(row.admitted_width, Some(4));
		assert_eq!(row.local_digest, row.distributed_digest);
		assert_eq!(row.local_row_count, row.distributed_row_count);
		assert_eq!(
			row.local_schema_fingerprint,
			row.distributed_schema_fingerprint
		);
	}

	#[tokio::test]
	async fn two_worker_join_matches_local_identity() {
		let row = run_execution_census_query("q03", "strict", 0.01, "memory", Path::new("unused"))
			.await
			.expect("join execution census");
		assert_eq!(row.classification, "executes_identical");
		assert_eq!(row.admitted_width, Some(4));
		assert_eq!(row.local_digest, row.distributed_digest);
	}

	#[tokio::test]
	async fn tiny_task_budget_is_a_typed_execution_rejection() {
		let row = run_execution_census_input(
			memory_execution_input("q03", None),
			0.01,
			"strict",
			Some(1),
		)
		.await
		.expect("tiny-budget execution census");
		assert_eq!(row.classification, "execution_rejected");
		assert_eq!(row.reason.as_deref(), Some("budget_memory"));
		assert_eq!(row.distributed_budget.memory_per_task.get(), 1);
		assert!(row.distributed_digest.is_none());
	}

	#[test]
	fn envelope_is_per_cell_and_row_disclosure_is_complete() {
		let sequential = execution_census_distributed_budget(1);
		let parallel = execution_census_distributed_budget(4);
		assert_eq!(
			sequential.memory_per_task.get(),
			EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES
		);
		assert_eq!(sequential.max_in_flight_tasks, Some(1));
		assert_eq!(parallel.memory_per_task.get(), 512 * 1024 * 1024);
		assert_eq!(parallel.max_in_flight_tasks, Some(4));
		assert_eq!(
			parallel.memory_per_task.get() * 4,
			EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES
		);
		let error = QueryError::Runtime(RuntimeError::TaskFailure {
			kind: TaskFailureKind::BudgetExhausted {
				dimension: BudgetDimension::ShuffleBytes,
				requested: ByteCount::new(2),
				available: ByteCount::new(1),
			},
		});
		assert_eq!(
			execution_census_query_error_reason(&error),
			"budget_shuffle_bytes"
		);
	}

	#[tokio::test]
	async fn execution_row_discloses_the_full_non_timing_coordinate() {
		let row = run_execution_census_query("q06", "strict", 0.01, "memory", Path::new("unused"))
			.await
			.expect("q06 execution census");
		let value: serde_json::Value =
			serde_json::from_str(&row.jsonl()).expect("execution census JSON");
		assert_eq!(value["suite"], "tpch_execution_census");
		assert_eq!(value["timing_authority"], false);
		assert_eq!(value["worker_count"], 2);
		assert_eq!(value["transport"], "tcp_loopback");
		assert_eq!(value["partition_store"], "in_memory");
		assert_eq!(value["distributed_attempts"], 1);
		assert_eq!(value["transport_retry"], false);
		assert_eq!(value["live_backpressure_receipts"], serde_json::json!([]));
		assert_eq!(
			value["distributed_budget"]["spill_bytes_reserved_per_query"],
			EXECUTION_CENSUS_SPILL_BYTES
		);
		assert_eq!(value["distributed_budget"]["max_in_flight_tasks"], 1);
		assert_eq!(
			value["task_memory_envelope_bytes"],
			EXECUTION_CENSUS_TASK_MEMORY_ENVELOPE_BYTES
		);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_protocol {
	use super::*;

	/// The census is a correctness rail. Its `timing_authority=false` is permanent, and
	/// only the atlas may claim authority — so the two can never be conflated by editing
	/// one line in the wrong file.
	#[test]
	fn execution_census_remains_permanently_non_authoritative() {
		let source = include_str!("telemetry.rs");
		let census = source
			.split("pub struct ExecutionCensusRow")
			.nth(1)
			.expect("execution census row");
		assert!(
			census.contains("\"timing_authority\": false"),
			"the execution census must hard-code timing_authority=false; timing authority \
			 belongs to the ADR 538 atlas rail alone"
		);
		// Scan production source only: this file's own test module contains the very
		// literal being searched for, and a guard that matches itself proves nothing.
		let lib = include_str!("lib.rs");
		let production = lib.split("#[cfg(test)]").next().expect("production source");
		assert!(
			!production.contains("timing_authority")
				|| production.contains("TimingAuthorityConditions"),
			"timing authority must be derived from TimingAuthorityConditions, never set as \
			 a bare field default"
		);
	}

	/// q15 is an inherited capability wall and cannot enter the executable set.
	#[test]
	fn q15_cannot_enter_the_atlas_cohort() {
		let cells = timing_atlas_cells();
		assert_eq!(
			cells.len(),
			21,
			"the atlas cohort is the 21 identical cells"
		);
		assert!(
			!cells.contains(&"q15"),
			"q15 is a typed capability wall, excluded by construction"
		);
		for excluded in TIMING_ATLAS_EXCLUDED_CELLS {
			assert!(ALL_QUERY_IDS.contains(&excluded));
		}
	}

	/// Protocol constants are fixed by the ADR before measurement. A run that could tune
	/// them from the environment would not be measuring the declared protocol.
	#[test]
	fn protocol_constants_are_not_environment_overridable() {
		assert_eq!(TIMING_ATLAS_MEASURED_PAIRS, 7);
		assert_eq!(TIMING_ATLAS_WARMUP_PAIRS, 1);
		let lib = include_str!("lib.rs");
		let core = lib
			.split("ADR 538: matched-envelope timing atlas — protocol core")
			.nth(1)
			.expect("protocol core")
			.split("ADR 528: total classification")
			.next()
			.expect("protocol core end");
		assert!(
			!core.contains("std::env::var"),
			"protocol constants must not be readable from the environment"
		);
	}

	/// Seven pairs leave one arm first four times; parity fixes which, and the order
	/// alternates from there.
	#[test]
	fn arm_order_is_parity_fixed_and_alternating() {
		assert_eq!(timing_atlas_starting_arm("q01"), TimingArm::Local);
		assert_eq!(timing_atlas_starting_arm("q22"), TimingArm::Distributed);

		let (first, second) = timing_atlas_pair_order("q01", 0);
		assert_eq!((first, second), (TimingArm::Local, TimingArm::Distributed));
		let (first, second) = timing_atlas_pair_order("q01", 1);
		assert_eq!((first, second), (TimingArm::Distributed, TimingArm::Local));

		// Odd cell: local leads four of seven. Even cell: distributed leads four of seven.
		let local_first = (0..TIMING_ATLAS_MEASURED_PAIRS)
			.filter(|index| timing_atlas_pair_order("q01", *index).0 == TimingArm::Local)
			.count();
		assert_eq!(local_first, 4);
		let distributed_first = (0..TIMING_ATLAS_MEASURED_PAIRS)
			.filter(|index| timing_atlas_pair_order("q22", *index).0 == TimingArm::Distributed)
			.count();
		assert_eq!(distributed_first, 4);
	}

	/// Any zero delta makes a cell indistinguishable, however far apart the medians are.
	#[test]
	fn verdict_requires_seven_strictly_signed_deltas() {
		assert_eq!(
			timing_atlas_verdict(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
			TimingAtlasVerdict::Distinguished
		);
		assert_eq!(
			timing_atlas_verdict(&[-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0]),
			TimingAtlasVerdict::Distinguished
		);
		assert_eq!(
			timing_atlas_verdict(&[1.0, 2.0, 0.0, 4.0, 5.0, 6.0, 7.0]),
			TimingAtlasVerdict::Indistinguishable,
			"a zero delta forbids a verdict"
		);
		assert_eq!(
			timing_atlas_verdict(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
			TimingAtlasVerdict::Indistinguishable,
			"an incomplete cohort of pairs cannot be distinguished"
		);
	}

	/// Timing authority is granted only when every condition holds, and withholding names
	/// its reasons.
	#[test]
	fn timing_authority_requires_every_condition() {
		let all = TimingAuthorityConditions {
			clean_build: true,
			full_hash_dataset: true,
			quiescent_host: true,
			protocol_unmodified: true,
			complete_cohort: true,
		};
		assert!(all.granted());
		assert!(all.withheld_reasons().is_empty());

		let noisy_host = TimingAuthorityConditions {
			quiescent_host: false,
			..all.clone()
		};
		assert!(!noisy_host.granted());
		assert_eq!(noisy_host.withheld_reasons(), vec!["host_not_quiescent"]);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_failure_paths {
	use super::*;

	fn child(arm: &'static str, valid: bool, failure_is_io: bool) -> TimingAtlasChild {
		TimingAtlasChild {
			query: "q01".to_string(),
			arm,
			pair_index: Some(0),
			position_in_pair: 0,
			attempt: 1,
			query_elapsed_ms: 10.0,
			row_count: Some(4),
			identity_matches: Some(true),
			valid,
			failure: (!valid).then(|| "boom".to_string()),
			failure_is_io,
			attribution: None,
		}
	}

	#[test]
	fn a_clean_pair_is_valid() {
		let outcome = timing_atlas_pair_outcome(
			&child("local", true, false),
			&child("distributed", true, false),
			false,
		);
		assert_eq!(outcome, TimingAtlasPairOutcome::Valid);
	}

	/// An I/O failure invalidates the whole pair, not just its own child.
	#[test]
	fn io_failure_invalidates_the_whole_pair_once() {
		let outcome = timing_atlas_pair_outcome(
			&child("local", true, false),
			&child("distributed", false, true),
			false,
		);
		assert_eq!(outcome, TimingAtlasPairOutcome::RetryableIo);
	}

	/// A second I/O failure is fatal — one retry, never a loop until the numbers look good.
	#[test]
	fn second_io_failure_is_fatal() {
		let outcome = timing_atlas_pair_outcome(
			&child("local", true, false),
			&child("distributed", false, true),
			true,
		);
		assert_eq!(outcome, TimingAtlasPairOutcome::Fatal);
	}

	/// Any non-I/O failure forbids authority immediately.
	#[test]
	fn non_io_failure_is_fatal() {
		let outcome = timing_atlas_pair_outcome(
			&child("local", true, false),
			&child("distributed", false, false),
			false,
		);
		assert_eq!(outcome, TimingAtlasPairOutcome::Fatal);
	}

	/// An identity mismatch aborts the cell as an ADR failure and is never retried — a
	/// wrong answer is not a flake.
	#[test]
	fn identity_mismatch_is_fatal_even_when_io_would_retry() {
		let mut mismatched = child("distributed", false, true);
		mismatched.identity_matches = Some(false);
		let outcome = timing_atlas_pair_outcome(&child("local", true, false), &mismatched, false);
		assert_eq!(
			outcome,
			TimingAtlasPairOutcome::Fatal,
			"an identity mismatch outranks a retryable transport flake"
		);
	}

	#[test]
	fn io_classification_recognizes_transport_errors() {
		assert!(timing_atlas_failure_is_io("QueryError::Io(...)"));
		assert!(!timing_atlas_failure_is_io("QueryError::Internal { .. }"));
	}

	/// An incomplete cohort cannot finalize or claim a verdict, however clean its pairs.
	#[test]
	fn incomplete_cohort_never_finalizes() {
		let outcome = TimingAtlasCellOutcome {
			query: "q01".to_string(),
			setup_ms: 1.0,
			teardown_ms: 1.0,
			verification_ms: 1.0,
			warmups: Vec::new(),
			pairs: Vec::new(),
			invalid_attempts: Vec::new(),
			cleanup_ms: 0.0,
			cleanup_failed: false,
			peer_closes: 0,
			failure: None,
		};
		assert!(!outcome.finalized());
		assert_eq!(outcome.verdict(), TimingAtlasVerdict::Indistinguishable);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_execution {
	use super::*;
	use std::collections::BTreeSet;

	/// Every child gets a distinct query identity, so no residue can be attributed across
	/// children and no two rows can collide in the artifact.
	#[test]
	fn every_child_receives_a_distinct_query_identity() {
		let plan = timing_atlas_cell_plan("q01");
		let identities = plan
			.iter()
			.map(|child| child.query_id)
			.collect::<BTreeSet<_>>();
		assert_eq!(
			identities.len(),
			plan.len(),
			"every child needs its own identity"
		);
	}

	/// Two discarded warmups run in parity order, then seven pairs. Warmups are never
	/// measured children.
	#[test]
	fn warmups_follow_parity_order_and_never_enter_estimators() {
		let plan = timing_atlas_cell_plan("q01");
		let warmups = plan
			.iter()
			.filter(|child| !child.role.is_measured())
			.collect::<Vec<_>>();
		assert_eq!(warmups.len(), 2, "two discarded warmups, one per arm");
		assert_eq!(warmups[0].arm, TimingArm::Local, "q01 is odd: local first");
		assert_eq!(warmups[1].arm, TimingArm::Distributed);
		assert!(
			plan.iter().take(2).all(|child| !child.role.is_measured()),
			"warmups precede every measured child"
		);

		let measured = plan.iter().filter(|child| child.role.is_measured()).count();
		assert_eq!(measured, TIMING_ATLAS_MEASURED_PAIRS * 2);
	}

	/// Each pair contains exactly one child per arm, and the order alternates.
	#[test]
	fn pairs_alternate_and_cover_both_arms() {
		let plan = timing_atlas_cell_plan("q22");
		for pair_index in 0..TIMING_ATLAS_MEASURED_PAIRS {
			let arms = plan
				.iter()
				.filter_map(|child| match child.role {
					TimingAtlasChildRole::Measured {
						pair_index: index, ..
					} if index == pair_index => Some(child.arm),
					_ => None,
				})
				.collect::<Vec<_>>();
			assert_eq!(arms.len(), 2, "pair {pair_index} has two children");
			assert_ne!(arms[0], arms[1], "a pair covers both arms");
		}
		// q22 is even: distributed leads the first pair.
		let first = plan
			.iter()
			.find(|child| {
				matches!(
					child.role,
					TimingAtlasChildRole::Measured {
						pair_index: 0,
						position: 0
					}
				)
			})
			.expect("first measured child");
		assert_eq!(first.arm, TimingArm::Distributed);
	}

	/// q15 appears only as the typed manifest exclusion, never as a timed child.
	#[test]
	fn q15_is_only_a_manifest_exclusion() {
		assert!(!timing_atlas_cells().contains(&"q15"));
		assert!(TIMING_ATLAS_EXCLUDED_CELLS.contains(&"q15"));
	}

	/// A partial artifact is never authoritative, whatever else holds.
	#[test]
	fn partial_artifact_is_never_authoritative() {
		let conditions = TimingAuthorityConditions {
			clean_build: true,
			full_hash_dataset: true,
			quiescent_host: true,
			protocol_unmodified: true,
			complete_cohort: true,
		};
		let cells = timing_atlas_cells().len();
		assert!(timing_atlas_artifact_is_authoritative(
			"timing_atlas.jsonl",
			true,
			cells,
			&conditions
		));
		assert!(
			!timing_atlas_artifact_is_authoritative(
				"timing_atlas.jsonl.partial",
				true,
				cells,
				&conditions
			),
			"a partial filename vetoes authority"
		);
		assert!(
			!timing_atlas_artifact_is_authoritative(
				"timing_atlas.jsonl",
				false,
				cells,
				&conditions
			),
			"a missing terminal seal vetoes authority — a crash leaves no seal"
		);
		assert!(
			!timing_atlas_artifact_is_authoritative(
				"timing_atlas.jsonl",
				true,
				cells - 1,
				&conditions
			),
			"20 of 21 cells vetoes authority"
		);
		let noisy = TimingAuthorityConditions {
			quiescent_host: false,
			..conditions
		};
		assert!(
			!timing_atlas_artifact_is_authoritative("timing_atlas.jsonl", true, cells, &noisy),
			"a failed global condition vetoes authority"
		);
	}

	/// Finalization refuses six of seven pairs, and refuses any cell carrying a failure.
	#[test]
	fn finalization_refuses_short_cohorts_and_failed_cells() {
		let pair = |index: usize| TimingAtlasPair {
			pair_index: index,
			first_arm: "local",
			local: TimingAtlasChild {
				query: "q01".to_string(),
				arm: "local",
				pair_index: Some(index),
				position_in_pair: 0,
				attempt: 1,
				query_elapsed_ms: 10.0,
				row_count: Some(4),
				identity_matches: Some(true),
				valid: true,
				failure: None,
				failure_is_io: false,
				attribution: None,
			},
			distributed: TimingAtlasChild {
				query: "q01".to_string(),
				arm: "distributed",
				pair_index: Some(index),
				position_in_pair: 1,
				attempt: 1,
				query_elapsed_ms: 20.0,
				row_count: Some(4),
				identity_matches: Some(true),
				valid: true,
				failure: None,
				failure_is_io: false,
				attribution: None,
			},
			delta_ms: 10.0,
			ratio: Some(2.0),
		};
		let base = TimingAtlasCellOutcome {
			query: "q01".to_string(),
			setup_ms: 5.0,
			teardown_ms: 5.0,
			verification_ms: 5.0,
			warmups: Vec::new(),
			pairs: (0..TIMING_ATLAS_MEASURED_PAIRS).map(pair).collect(),
			invalid_attempts: Vec::new(),
			cleanup_ms: 0.0,
			cleanup_failed: false,
			peer_closes: 0,
			failure: None,
		};
		assert!(base.finalized());
		assert_eq!(base.verdict(), TimingAtlasVerdict::Distinguished);

		let short = TimingAtlasCellOutcome {
			pairs: (0..TIMING_ATLAS_MEASURED_PAIRS - 1).map(pair).collect(),
			..base.clone()
		};
		assert!(!short.finalized(), "six of seven pairs cannot finalize");

		let failed = TimingAtlasCellOutcome {
			failure: Some("identity mismatch".to_string()),
			..base.clone()
		};
		assert!(!failed.finalized(), "a recorded failure cannot finalize");
	}

	/// Setup and teardown are recorded but excluded from the paired estimators, which read
	/// query elapsed alone.
	#[test]
	fn setup_and_teardown_are_recorded_but_excluded() {
		let outcome = TimingAtlasCellOutcome {
			query: "q01".to_string(),
			setup_ms: 900.0,
			teardown_ms: 800.0,
			verification_ms: 700.0,
			warmups: Vec::new(),
			pairs: vec![TimingAtlasPair {
				pair_index: 0,
				first_arm: "local",
				local: TimingAtlasChild {
					query: "q01".to_string(),
					arm: "local",
					pair_index: Some(0),
					position_in_pair: 0,
					attempt: 1,
					query_elapsed_ms: 10.0,
					row_count: Some(4),
					identity_matches: Some(true),
					valid: true,
					failure: None,
					failure_is_io: false,
					attribution: None,
				},
				distributed: TimingAtlasChild {
					query: "q01".to_string(),
					arm: "distributed",
					pair_index: Some(0),
					position_in_pair: 1,
					attempt: 1,
					query_elapsed_ms: 25.0,
					row_count: Some(4),
					identity_matches: Some(true),
					valid: true,
					failure: None,
					failure_is_io: false,
					attribution: None,
				},
				delta_ms: 15.0,
				ratio: Some(2.5),
			}],
			invalid_attempts: Vec::new(),
			cleanup_ms: 0.0,
			cleanup_failed: false,
			peer_closes: 0,
			failure: None,
		};
		assert!(outcome.setup_ms > 0.0 && outcome.teardown_ms > 0.0);
		assert_eq!(
			outcome.paired_deltas(),
			vec![15.0],
			"the delta reflects query elapsed only; persistent setup is not a per-query cost"
		);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_runner {
	use super::*;

	#[derive(Debug, Clone, PartialEq, Eq)]
	enum Event {
		Setup(String),
		Execute(String),
		Reduce(String),
		Cleanup(String),
		Teardown(String),
	}

	/// Fake harness with an event log and a fake clock. Execution advances the clock by
	/// `execute_ms`; reduction advances it by `reduce_ms`, which must never reach
	/// `query_elapsed_ms`.
	struct FakeHarness {
		events: Vec<Event>,
		clock_ms: f64,
		execute_ms: f64,
		reduce_ms: f64,
		cleanup_ms: f64,
		setup_ms: f64,
		teardown_ms: f64,
	}

	impl FakeHarness {
		fn new(execute_ms: f64, reduce_ms: f64) -> Self {
			Self {
				events: Vec::new(),
				clock_ms: 0.0,
				execute_ms,
				reduce_ms,
				cleanup_ms: 40.0,
				setup_ms: 500.0,
				teardown_ms: 300.0,
			}
		}
	}

	impl TimingAtlasHarness for FakeHarness {
		async fn setup_cell(&mut self, query: &str) -> Result<(), String> {
			self.events.push(Event::Setup(query.to_string()));
			self.clock_ms += self.setup_ms;
			Ok(())
		}

		async fn execute_child(
			&mut self,
			plan: &TimingAtlasChildPlan,
		) -> Result<TimingAtlasExecution, String> {
			self.events
				.push(Event::Execute(plan.arm.label().to_string()));
			self.clock_ms += self.execute_ms;
			Ok(TimingAtlasExecution { row_count: 4 })
		}

		async fn reduce_child(
			&mut self,
			plan: &TimingAtlasChildPlan,
			_execution: &TimingAtlasExecution,
		) -> Result<TimingAtlasReduction, String> {
			self.events
				.push(Event::Reduce(plan.arm.label().to_string()));
			self.clock_ms += self.reduce_ms;
			Ok(TimingAtlasReduction {
				identity_matches: true,
				attribution: None,
			})
		}

		async fn cleanup_child(&mut self, plan: &TimingAtlasChildPlan) -> Result<(), String> {
			self.events
				.push(Event::Cleanup(plan.arm.label().to_string()));
			self.clock_ms += self.cleanup_ms;
			Ok(())
		}

		async fn teardown_cell(&mut self, query: &str) -> Result<(), String> {
			self.events.push(Event::Teardown(query.to_string()));
			self.clock_ms += self.teardown_ms;
			Ok(())
		}

		fn now_ms(&mut self) -> f64 {
			self.clock_ms
		}
	}

	/// Workers are constructed exactly once per cell and stay resident: all sixteen
	/// children execute between the single setup and the single teardown, including the
	/// local arms, so alternating pairs see one host footprint.
	#[tokio::test]
	async fn workers_start_once_per_cell_and_stay_resident_through_every_child() {
		let mut harness = FakeHarness::new(10.0, 1_000.0);
		let outcome = run_timing_atlas_cell_with(&mut harness, "q01").await;
		assert!(outcome.finalized());

		let setups = harness
			.events
			.iter()
			.filter(|event| matches!(event, Event::Setup(_)))
			.count();
		let teardowns = harness
			.events
			.iter()
			.filter(|event| matches!(event, Event::Teardown(_)))
			.count();
		assert_eq!(setups, 1, "workers are constructed once per cell");
		assert_eq!(teardowns, 1, "and torn down once");

		let setup_at = harness
			.events
			.iter()
			.position(|event| matches!(event, Event::Setup(_)))
			.expect("setup");
		let teardown_at = harness
			.events
			.iter()
			.position(|event| matches!(event, Event::Teardown(_)))
			.expect("teardown");
		let executions = harness
			.events
			.iter()
			.enumerate()
			.filter(|(_, event)| matches!(event, Event::Execute(_)))
			.map(|(index, _)| index)
			.collect::<Vec<_>>();
		assert_eq!(
			executions.len(),
			2 + TIMING_ATLAS_MEASURED_PAIRS * 2,
			"two warmups plus seven pairs"
		);
		assert!(
			executions
				.iter()
				.all(|index| *index > setup_at && *index < teardown_at),
			"every child runs while the workers are resident"
		);
	}

	/// The timer surrounds execution only. Reduction is made 100x more expensive than
	/// execution here: if digesting leaked inside the boundary the deltas would explode.
	#[tokio::test]
	async fn query_elapsed_covers_execution_only_not_digest_or_reduction() {
		let mut harness = FakeHarness::new(10.0, 1_000.0);
		let outcome = run_timing_atlas_cell_with(&mut harness, "q01").await;
		assert!(outcome.finalized());

		for pair in &outcome.pairs {
			assert_eq!(
				pair.local.query_elapsed_ms, 10.0,
				"execution alone contributes to query elapsed"
			);
			assert_eq!(pair.distributed.query_elapsed_ms, 10.0);
			assert_eq!(pair.delta_ms, 0.0);
		}
		assert!(
			outcome.verification_ms >= 1_000.0 * 16.0,
			"reduction time is recorded separately, not discarded"
		);
		assert_eq!(outcome.setup_ms, 500.0, "setup recorded but excluded");
		assert_eq!(outcome.teardown_ms, 300.0, "teardown recorded but excluded");
	}
}

#[cfg(test)]
mod adr538_timing_atlas_publication {
	use super::*;

	fn temp_dir(name: &str) -> PathBuf {
		let root = std::env::temp_dir().join(format!("adr538-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).expect("temp dir");
		root
	}

	fn row(kind: &str) -> serde_json::Value {
		serde_json::json!({ "kind": kind })
	}

	/// While a run is in flight only the partial exists; the canonical artifact appears at
	/// publication and never before.
	#[test]
	fn partial_exists_while_running_and_canonical_does_not() {
		let root = temp_dir("running");
		let canonical = root.join("timing_atlas.jsonl");
		let mut artifact = TimingAtlasArtifact::create(&canonical).expect("create");
		artifact.append_row(&row("child")).expect("append");

		assert!(
			artifact.partial_path().exists(),
			"partial exists while running"
		);
		assert!(
			!canonical.exists(),
			"canonical must not exist before publication"
		);
		let _ = std::fs::remove_dir_all(&root);
	}

	/// A failed or abandoned run retains the partial and publishes nothing.
	#[test]
	fn failed_run_retains_partial_and_never_publishes() {
		let root = temp_dir("failed");
		let canonical = root.join("timing_atlas.jsonl");
		let mut artifact = TimingAtlasArtifact::create(&canonical).expect("create");
		artifact.append_row(&row("child")).expect("append");
		let partial = artifact.partial_path().to_path_buf();

		// No seal: publication is refused.
		let error = artifact.publish().expect_err("unsealed publish must fail");
		assert!(error.contains("terminal seal"));
		assert!(partial.exists(), "the partial is retained for inspection");
		assert!(!canonical.exists());
		let _ = std::fs::remove_dir_all(&root);
	}

	/// Successful finalization writes the seal last and publishes atomically.
	#[test]
	fn successful_finalization_seals_then_publishes_atomically() {
		let root = temp_dir("sealed");
		let canonical = root.join("timing_atlas.jsonl");
		let mut artifact = TimingAtlasArtifact::create(&canonical).expect("create");
		artifact.append_row(&row("child")).expect("append");
		artifact.append_row(&row("pair")).expect("append");
		artifact
			.append_seal(&serde_json::json!({ "kind": TIMING_ATLAS_SEAL_KIND }))
			.expect("seal");
		let partial = artifact.partial_path().to_path_buf();
		let published = artifact.publish().expect("publish");

		assert_eq!(published, canonical);
		assert!(canonical.exists(), "canonical artifact is published");
		assert!(!partial.exists(), "the partial is renamed, not copied");

		let contents = std::fs::read_to_string(&canonical).expect("read");
		let last = contents.lines().last().expect("last line");
		assert!(
			last.contains(TIMING_ATLAS_SEAL_KIND),
			"the terminal seal is the final record"
		);
		let _ = std::fs::remove_dir_all(&root);
	}

	/// A run never overwrites an existing atlas, at creation or at publication.
	#[test]
	fn publication_refuses_to_overwrite_an_existing_artifact() {
		let root = temp_dir("existing");
		let canonical = root.join("timing_atlas.jsonl");
		std::fs::write(&canonical, "{}\n").expect("pre-existing artifact");

		let error = TimingAtlasArtifact::create(&canonical).expect_err("create must refuse");
		assert!(error.contains("refuses to overwrite"));
		let _ = std::fs::remove_dir_all(&root);
	}

	/// A double seal is refused: the terminal record is written once.
	#[test]
	fn a_second_seal_is_refused() {
		let root = temp_dir("double-seal");
		let canonical = root.join("timing_atlas.jsonl");
		let mut artifact = TimingAtlasArtifact::create(&canonical).expect("create");
		let seal = serde_json::json!({ "kind": TIMING_ATLAS_SEAL_KIND });
		artifact.append_seal(&seal).expect("first seal");
		assert!(artifact.append_seal(&seal).is_err(), "one seal only");
		let _ = std::fs::remove_dir_all(&root);
	}

	/// Authority requires quiescence at the start AND at the final seal — a service
	/// restarted mid-atlas must not pass on a single early observation.
	#[test]
	fn quiescence_is_required_at_both_observations() {
		let quiet = HostQuiescence {
			service_inactive: true,
			sql_port_free: true,
		};
		let noisy = HostQuiescence {
			service_inactive: false,
			sql_port_free: false,
		};
		assert!(timing_atlas_quiescent_throughout(&quiet, &quiet));
		assert!(
			!timing_atlas_quiescent_throughout(&quiet, &noisy),
			"a service restarted before the seal forfeits authority"
		);
		assert!(!timing_atlas_quiescent_throughout(&noisy, &quiet));
		assert!(
			!HostQuiescence {
				service_inactive: true,
				sql_port_free: false
			}
			.quiescent(),
			"both the service and the port must be clear"
		);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_residency {
	use super::*;

	fn residency(worker: u128) -> TimingAtlasResidency {
		TimingAtlasResidency {
			worker_ids: vec![worker, worker + 1],
			endpoints: vec![format!("127.0.0.1:{}", 9000 + worker)],
		}
	}

	/// The real harness must reuse the workers `setup_cell` built. Stable ids and endpoints
	/// across all sixteen children is what distinguishes reuse from silent reconstruction.
	#[test]
	fn stable_worker_identity_across_every_child_of_a_cell() {
		let mut log = TimingAtlasResidencyLog::default();
		for _ in 0..(2 + TIMING_ATLAS_MEASURED_PAIRS * 2) {
			log.observe(residency(1));
		}
		assert_eq!(log.len(), 16);
		assert!(log.stable(), "one cell, one set of resident workers");
	}

	/// A harness that rebuilt infrastructure mid-cell would change identity, and that is a
	/// failure even if every child otherwise succeeded.
	#[test]
	fn rebuilt_workers_are_detected_as_unstable() {
		let mut log = TimingAtlasResidencyLog::default();
		for index in 0..16u128 {
			// A rebuild between children 7 and 8.
			log.observe(residency(if index < 8 { 1 } else { 2 }));
		}
		assert!(
			!log.stable(),
			"changed worker identity means execute_child recreated infrastructure"
		);
	}

	#[test]
	fn an_empty_log_is_not_stable() {
		let log = TimingAtlasResidencyLog::default();
		assert!(
			!log.stable(),
			"no observations cannot vacuously prove residency"
		);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_real_harness {
	use super::*;

	/// The real harness must reuse what `setup_cell` built. Residency is read from the
	/// resident worker entries themselves, so a harness that rebuilt infrastructure inside
	/// `execute_child` would show changed ids and fail here.
	/// Deep binds need an explicit stack; run on a dedicated 64 MB thread.
	#[test]
	fn real_harness_keeps_workers_resident_across_every_child() {
		let outcome = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.worker_threads(2)
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let mut harness = CensusTimingAtlasHarness::prepare(
						"q06",
						"strict",
						0.01,
						Path::new("unused"),
					)
					.await
					.expect("prepare q06");
					let cell = run_timing_atlas_cell_with(&mut harness, "q06").await;
					(cell, harness.residency)
				})
			})
			.expect("spawn")
			.join()
			.expect("join");

		let (cell, residency) = outcome;
		assert!(
			cell.failure.is_none(),
			"the cell must complete: {:?}",
			cell.failure
		);
		assert_eq!(
			cell.pairs.len(),
			TIMING_ATLAS_MEASURED_PAIRS,
			"seven valid pairs"
		);
		assert_eq!(
			residency.len(),
			2 + TIMING_ATLAS_MEASURED_PAIRS * 2,
			"one residency observation per child"
		);
		assert!(
			residency.stable(),
			"workers built by setup_cell must be reused by every child"
		);
		assert!(cell.setup_ms > 0.0 && cell.teardown_ms > 0.0);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_reclamation {
	use super::*;

	/// Fresh query ids stop collisions; they do not prove the store gave the memory back.
	/// With workers resident across sixteen children, an accumulating store would raise
	/// memory pressure as a cell progresses — drift correlated with pair index, which the
	/// paired design cannot subtract because it is not machine noise.
	/// Deep binds need an explicit stack; run on a dedicated 64 MB thread.
	#[test]
	fn resident_partition_store_returns_to_baseline_between_children() {
		let observations = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.worker_threads(2)
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let mut harness = CensusTimingAtlasHarness::prepare(
						"q06",
						"strict",
						0.01,
						Path::new("unused"),
					)
					.await
					.expect("prepare q06");
					harness.setup_cell("q06").await.expect("setup");

					let baseline = harness.resident_partition_bytes();
					let plan = timing_atlas_cell_plan("q06");
					let mut after_each = Vec::new();
					for child_plan in plan.iter().take(4) {
						let execution = harness.execute_child(child_plan).await.expect("child");
						harness
							.reduce_child(child_plan, &execution)
							.await
							.expect("reduce");
						harness.cleanup_child(child_plan).await.expect("cleanup");
						after_each.push(harness.resident_partition_bytes());
					}
					harness.teardown_cell("q06").await.expect("teardown");
					(baseline, after_each)
				})
			})
			.expect("spawn")
			.join()
			.expect("join");

		let (baseline, after_each) = observations;
		for (index, retained) in after_each.iter().enumerate() {
			assert_eq!(
				*retained, baseline,
				"child {index} left {retained} retained bytes against a {baseline} baseline; \
				 a resident store must return to baseline before the next child or later \
				 pairs measure a different machine"
			);
		}
	}
}

#[cfg(test)]
mod adr538_timing_atlas_retry_and_reclamation_controls {
	use super::*;
	use std::collections::BTreeSet;

	/// Fails the first distributed child once with an I/O error, then succeeds.
	struct FlakyHarness {
		clock_ms: f64,
		seen: Vec<(String, QueryId)>,
		failed_once: bool,
	}

	impl TimingAtlasHarness for FlakyHarness {
		async fn setup_cell(&mut self, _query: &str) -> Result<(), String> {
			Ok(())
		}

		async fn execute_child(
			&mut self,
			plan: &TimingAtlasChildPlan,
		) -> Result<TimingAtlasExecution, String> {
			self.clock_ms += 10.0;
			self.seen
				.push((plan.arm.label().to_string(), plan.query_id));
			if !self.failed_once
				&& plan.arm == TimingArm::Distributed
				&& matches!(
					plan.role,
					TimingAtlasChildRole::Measured { pair_index: 0, .. }
				) {
				self.failed_once = true;
				return Err("QueryError::Io(transport reset)".to_string());
			}
			Ok(TimingAtlasExecution { row_count: 4 })
		}

		async fn reduce_child(
			&mut self,
			_plan: &TimingAtlasChildPlan,
			_execution: &TimingAtlasExecution,
		) -> Result<TimingAtlasReduction, String> {
			Ok(TimingAtlasReduction {
				identity_matches: true,
				attribution: None,
			})
		}

		async fn cleanup_child(&mut self, _plan: &TimingAtlasChildPlan) -> Result<(), String> {
			Ok(())
		}

		async fn teardown_cell(&mut self, _query: &str) -> Result<(), String> {
			Ok(())
		}

		fn now_ms(&mut self) -> f64 {
			self.clock_ms
		}
	}

	/// A retryable I/O failure replaces the whole pair: four distinct identities across the
	/// original and replacement children, and the replacement keeps the original arm order.
	#[tokio::test]
	async fn retried_pair_uses_four_distinct_identities_and_keeps_arm_order() {
		let mut harness = FlakyHarness {
			clock_ms: 0.0,
			seen: Vec::new(),
			failed_once: false,
		};
		let outcome = run_timing_atlas_cell_with(&mut harness, "q01").await;
		assert!(
			outcome.failure.is_none(),
			"one I/O failure is retryable: {:?}",
			outcome.failure
		);
		assert_eq!(outcome.pairs.len(), TIMING_ATLAS_MEASURED_PAIRS);
		assert_eq!(
			outcome.invalid_attempts.len(),
			2,
			"both children of the invalidated pair are retained"
		);

		// The two invalidated children and their two replacements: four distinct ids.
		let invalid_ids = outcome
			.invalid_attempts
			.iter()
			.map(|child| child.query_elapsed_ms.to_bits())
			.count();
		assert_eq!(invalid_ids, 2);

		let pair_zero_children = harness.seen.iter().take(6).cloned().collect::<Vec<_>>();
		// children 0,1 = warmups; 2,3 = original pair 0; 4,5 = replacement pair 0.
		let identities = pair_zero_children
			.iter()
			.skip(2)
			.map(|(_, id)| *id)
			.collect::<BTreeSet<_>>();
		assert_eq!(
			identities.len(),
			4,
			"original and replacement children need four distinct identities"
		);
		assert_eq!(
			pair_zero_children[2].0, pair_zero_children[4].0,
			"the replacement keeps the original first arm"
		);
		assert_eq!(
			pair_zero_children[3].0, pair_zero_children[5].0,
			"and the original second arm"
		);
	}

	/// A cleanup failure is fatal: an attempted reclamation is not a proven one, and every
	/// later child would measure a different machine.
	#[tokio::test]
	async fn cleanup_failure_forfeits_the_cell() {
		struct DirtyHarness {
			clock_ms: f64,
		}
		impl TimingAtlasHarness for DirtyHarness {
			async fn setup_cell(&mut self, _query: &str) -> Result<(), String> {
				Ok(())
			}
			async fn execute_child(
				&mut self,
				_plan: &TimingAtlasChildPlan,
			) -> Result<TimingAtlasExecution, String> {
				self.clock_ms += 10.0;
				Ok(TimingAtlasExecution { row_count: 4 })
			}
			async fn reduce_child(
				&mut self,
				_plan: &TimingAtlasChildPlan,
				_execution: &TimingAtlasExecution,
			) -> Result<TimingAtlasReduction, String> {
				Ok(TimingAtlasReduction {
					identity_matches: true,
					attribution: None,
				})
			}
			async fn cleanup_child(&mut self, _plan: &TimingAtlasChildPlan) -> Result<(), String> {
				Err("store still resident".to_string())
			}
			async fn teardown_cell(&mut self, _query: &str) -> Result<(), String> {
				Ok(())
			}
			fn now_ms(&mut self) -> f64 {
				self.clock_ms
			}
		}

		let mut harness = DirtyHarness { clock_ms: 0.0 };
		let outcome = run_timing_atlas_cell_with(&mut harness, "q01").await;
		assert!(
			outcome.failure.is_some(),
			"a failed reclamation must forfeit the cell"
		);
		assert!(!outcome.finalized(), "and therefore its timing authority");
	}

	/// Reclamation frees the namespace itself, not merely its bytes: the same query id can
	/// be used again with no duplicate address, and no stale cancellation or live state
	/// blocks it.
	/// Deep binds need an explicit stack; run on a dedicated 64 MB thread.
	#[test]
	fn reclaimed_namespace_can_be_reused_without_stale_state() {
		let result = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.worker_threads(2)
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let mut harness = CensusTimingAtlasHarness::prepare(
						"q06",
						"strict",
						0.01,
						Path::new("unused"),
					)
					.await
					.expect("prepare");
					harness.setup_cell("q06").await.expect("setup");

					// One namespace, exercised then reclaimed, then exercised again.
					let reused = QueryId::new();
					let plan = TimingAtlasChildPlan {
						arm: TimingArm::Distributed,
						role: TimingAtlasChildRole::Measured {
							pair_index: 0,
							position: 0,
						},
						query_id: reused,
					};
					let first = harness.execute_child(&plan).await;
					harness.cleanup_child(&plan).await.expect("cleanup");
					let after_cleanup = harness.resident_partition_bytes();
					let second = harness.execute_child(&plan).await;
					harness.cleanup_child(&plan).await.expect("cleanup");
					harness.teardown_cell("q06").await.expect("teardown");
					(first.is_ok(), after_cleanup, second)
				})
			})
			.expect("spawn")
			.join()
			.expect("join");

		let (first_ok, after_cleanup, second) = result;
		assert!(first_ok, "the first use of the namespace succeeds");
		assert_eq!(after_cleanup, 0, "reclamation returns the bytes");
		assert!(
			second.is_ok(),
			"the same partition addresses must publish again after reclamation — a \
			 surviving manifest, live entry or cancellation record would reject this: {:?}",
			second.err()
		);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_journal_replay {
	use super::*;

	/// Per-node accumulation survives to the artifact and its sum over the selected
	/// path equals the path-level total.
	///
	/// The first fully-wired Branch A run emitted a correct path total with no node
	/// detail, which left one cell's coverage undecidable: a near-zero total could
	/// not be told apart from an uninstrumented path. Replay now refuses that.
	#[test]
	fn adr554_per_node_accumulation_sums_to_the_path_total() {
		let mut left = decomposition_node(0.0, 0.0, 10.0);
		left.aggregate_accumulation_ms = 3.0;
		let mut right = decomposition_node(0.0, 0.0, 10.0);
		right.stage_id = 1;
		right.upstream = vec![(0, 0)];
		right.aggregate_accumulation_ms = 4.0;

		let attribution =
			timing_atlas_child_attribution(vec![left, right]).expect("two-node fixture");
		let row = timing_atlas_attribution_path_row("q13", "distributed", 0, 1, 50.0, &attribution);

		let per_node: Vec<f64> = row["nodes"]
			.as_array()
			.expect("nodes array")
			.iter()
			.map(|node| {
				node["aggregate_accumulation_ms"]
					.as_f64()
					.expect("per-node accumulation present")
			})
			.collect();
		assert_eq!(
			per_node,
			vec![3.0, 4.0],
			"each node carries its own accumulation"
		);
		assert_eq!(row["aggregate_accumulation_ms"], 7.0);

		timing_atlas_replay_attribution_path(&row).expect("node sum agrees with the path total");
	}

	fn placement_node(
		stage: u32,
		task: u32,
		worker: &str,
		upstream: Vec<(u32, u32)>,
	) -> TimingAtlasTaskNode {
		let mut node = decomposition_node(0.0, 0.0, 1.0);
		node.stage_id = stage;
		node.task_id = task;
		node.worker_id = worker.to_owned();
		node.upstream = upstream;
		node
	}

	/// One producer feeding a two-task sibling stage: one affinity link, one
	/// fallback-eligible group.
	fn placement_shape(sibling_a: &str, sibling_b: &str) -> Vec<TimingAtlasTaskNode> {
		vec![
			placement_node(0, 0, "w1", vec![]),
			placement_node(1, 0, "w1", vec![(0, 0)]),
			placement_node(2, 0, sibling_a, vec![(1, 0)]),
			placement_node(2, 1, sibling_b, vec![(1, 0)]),
		]
	}

	#[test]
	fn adr555_placement_facts_split_siblings_from_affinity_links() {
		let facts = placement_facts_from_nodes(&placement_shape("w1", "w2"));
		assert_eq!(facts.sibling_groups, 1);
		assert_eq!(facts.sibling_groups_spread, 1);
		assert_eq!(facts.affinity_links, 1);
		assert_eq!(facts.affinity_links_colocated, 1);
	}

	/// The gate passes only when each arm shows its own placement signature.
	#[test]
	fn adr555_placement_gate_accepts_arms_that_actually_differ() {
		let baseline = placement_facts_from_nodes(&placement_shape("w1", "w1"));
		let treatment = placement_facts_from_nodes(&placement_shape("w1", "w2"));
		assert!(placement_gate_problems("q13", 0, baseline, treatment).is_empty());
	}

	/// Every failure mode is refused, and each is refused for its own reason —
	/// a single combined assertion would let two of them rot.
	#[test]
	fn adr555_placement_gate_rejects_each_failure_mode() {
		let spread = placement_facts_from_nodes(&placement_shape("w1", "w2"));
		let colocated = placement_facts_from_nodes(&placement_shape("w1", "w1"));

		// Baseline arm spread when it should have serialized.
		let problems = placement_gate_problems("q13", 0, spread, spread);
		assert!(
			problems
				.iter()
				.any(|p| p.contains("baseline must co-locate")),
			"{problems:?}"
		);

		// Treatment arm serialized when it should have spread.
		let problems = placement_gate_problems("q13", 0, colocated, colocated);
		assert!(
			problems.iter().any(|p| p.contains("treatment must spread")),
			"{problems:?}"
		);

		// Affinity broken in one arm: locality traded for balancing.
		let mut broken = placement_shape("w1", "w2");
		broken[1].worker_id = "w2".to_owned();
		let problems =
			placement_gate_problems("q13", 0, colocated, placement_facts_from_nodes(&broken));
		assert!(
			problems
				.iter()
				.any(|p| p.contains("affinity must keep winning")),
			"{problems:?}"
		);

		// Arms that saw different plans are not comparable.
		let mut smaller = colocated;
		smaller.sibling_groups = 2;
		let problems = placement_gate_problems("q13", 0, smaller, spread);
		assert!(
			problems
				.iter()
				.any(|p| p.contains("different plan structure")),
			"{problems:?}"
		);
	}

	/// ADR 554 Branch A: naming accumulation must **reduce** the unnamed residual,
	/// not relocate time into it.
	///
	/// The first Branch A run wired the engine side only: the counter left
	/// `framework_overhead`, the harness never carried it, and the time surfaced
	/// as unnamed residual instead. q13's in-task figure fell 2,592 to 783 ms
	/// while its unnamed residual rose 296 to 2,528 ms. Both halves must move
	/// together, and this control is what fails if only one is wired.
	#[test]
	fn adr554_naming_accumulation_reduces_the_unnamed_residual() {
		let mut node = decomposition_node(0.0, 0.0, 100.0);
		node.aggregate_accumulation_ms = 12.0;
		let attribution = timing_atlas_child_attribution(vec![node]).expect("fixture attributes");
		assert_eq!(attribution.aggregate_accumulation_ms, 12.0);

		let row = timing_atlas_attribution_path_row("q13", "distributed", 0, 1, 50.0, &attribution);
		assert_eq!(row["aggregate_accumulation_ms"], 12.0);
		// attributed = 5; remainder = 45; in-task unnamed = 0; named accumulation = 12.
		assert_eq!(
			row["unnamed_residual_ms"], 33.0,
			"named accumulation leaves the unnamed residual rather than inflating it"
		);
	}

	/// ADR 554 §2: accumulation is a **remainder subterm**, never an ADR 538
	/// component. The frozen phase-to-component mapping must not have grown.
	#[test]
	fn adr554_aggregate_accumulation_is_not_an_adr538_component() {
		let mut phases = TaskTelemetryPhases::default();
		phases.aggregate_accumulation_micros = 7_000;
		let portions = timing_atlas_component_portions_ms(&phases);

		assert!(
			portions.iter().all(|(_, ms)| *ms == 0.0),
			"accumulation must not surface as any component portion: {portions:?}"
		);
		let labels: Vec<&str> = portions
			.iter()
			.map(|(component, _)| component.label())
			.collect();
		assert!(
			!labels.iter().any(|label| label.contains("aggregate")),
			"no aggregate component may exist in the frozen mapping: {labels:?}"
		);
	}

	/// Task-to-worker identity survives from the scheduler to the artifact and back
	/// through replay.
	///
	/// The scheduler always knew which worker ran an attempt; it was dropped before
	/// the telemetry record, so no artifact could test whether concurrent stage
	/// siblings landed on different workers. This control fails if that identity is
	/// dropped again anywhere along the path.
	#[test]
	fn adr554_task_to_worker_identity_survives_into_the_artifact() {
		let mut left = decomposition_node(0.0, 0.0, 10.0);
		left.worker_id = "worker-a".to_owned();
		let mut right = decomposition_node(0.0, 0.0, 10.0);
		right.stage_id = 1;
		right.worker_id = "worker-b".to_owned();
		right.upstream = vec![(0, 0)];

		let attribution =
			timing_atlas_child_attribution(vec![left, right]).expect("two-worker fixture");
		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);

		let workers: Vec<&str> = row["nodes"]
			.as_array()
			.expect("nodes array")
			.iter()
			.map(|node| node["worker_id"].as_str().expect("worker id present"))
			.collect();
		assert_eq!(
			workers,
			vec!["worker-a", "worker-b"],
			"each task carries the worker that actually ran it"
		);

		// And it round-trips: replay rebuilds nodes from the row, so an analyst
		// re-deriving from the artifact sees the same assignment.
		timing_atlas_replay_attribution_path(&row).expect("row replays");
	}

	/// ADR 554 §3 B2: each span is reported under its own owner, and the residual is
	/// exactly what they leave over. One combined lifecycle number would fail this.
	#[test]
	fn adr554_lifecycle_spans_are_reported_separately_with_an_explicit_residual() {
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			Some(4.0),
			Some(3.0),
			toron::coordinator::QueryLifecycleSpans {
				pre_dispatch_micros: 6_000,
				dispatch_loop_wall_micros: 20_000,
				post_dispatch_finalization_micros: 2_000,
			},
		)
		.expect("fixture attributes");

		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);
		assert_eq!(row["sql_planning_ms"], 4.0);
		assert_eq!(row["pre_dispatch_ms"], 6.0);
		assert_eq!(row["post_dispatch_finalization_ms"], 2.0);
		assert_eq!(row["consumer_drain_ms"], 3.0);
		// Both the loop wall and the drain are envelopes, never terms in the residual.
		assert_eq!(row["dispatch_loop_wall_ms"], 20.0);
		// attributed = 5; remainder = 45; in-task = 0; unnamed = 45.
		// Sequential terms = 4 + 6 + 2 = 12; the 3ms drain overlaps and is excluded.
		assert_eq!(row["unnamed_residual_ms"], 45.0);
		assert_eq!(row["lifecycle_residual_ms"], 33.0);
	}

	/// Producer finalization is a real, nonzero span when the driver does post-loop
	/// work. Without this the boundary could regress to two adjacent statements —
	/// which is exactly what the first corrected run measured — and still pass.
	#[test]
	fn adr554_finalization_span_is_nonzero_when_post_loop_work_exists() {
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			None,
			None,
			toron::coordinator::QueryLifecycleSpans {
				pre_dispatch_micros: 1_000,
				dispatch_loop_wall_micros: 10_000,
				post_dispatch_finalization_micros: 7_500,
			},
		)
		.expect("fixture attributes");
		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);
		assert_eq!(row["post_dispatch_finalization_ms"], 7.5);
		assert!(
			row["post_dispatch_finalization_ms"]
				.as_f64()
				.is_some_and(|ms| ms > 0.0),
			"a driver with post-loop work must not report a zero finalization span"
		);
	}

	/// True drain runs past producer finalization to consumer exhaustion, so it is
	/// reported independently and can exceed the finalization span.
	#[test]
	fn adr554_consumer_drain_extends_beyond_producer_finalization() {
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			None,
			// Buffered batches: the consumer keeps pulling after the producer is done.
			Some(9.0),
			toron::coordinator::QueryLifecycleSpans {
				pre_dispatch_micros: 1_000,
				dispatch_loop_wall_micros: 10_000,
				post_dispatch_finalization_micros: 500,
			},
		)
		.expect("fixture attributes");
		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);
		assert_eq!(row["consumer_drain_ms"], 9.0);
		assert!(
			row["consumer_drain_ms"].as_f64().unwrap()
				> row["post_dispatch_finalization_ms"].as_f64().unwrap(),
			"drain must be able to outlast the producer, or it is not a drain"
		);
		// Reported, never subtracted: overlapping spans cannot be sequential terms.
		// unnamed = 45; sequential terms = 1 + 0.5; the 9 leaves the residual alone.
		assert_eq!(row["lifecycle_residual_ms"], 43.5);
	}

	/// The loop-minus-path derivation appears only when the dispatch windows are
	/// actually contained in the loop that ran them. Overlapping windows sum past
	/// the wall, and the difference would be an artefact — so both raw figures stand
	/// and the subtraction is refused rather than clamped.
	#[test]
	fn adr554_loop_non_path_is_refused_when_windows_exceed_the_loop() {
		let contained = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			None,
			None,
			toron::coordinator::QueryLifecycleSpans {
				pre_dispatch_micros: 0,
				dispatch_loop_wall_micros: 40_000,
				post_dispatch_finalization_micros: 0,
			},
		)
		.expect("contained fixture");
		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &contained);
		// The node carries no dispatch portion, so the sum is 0 and 40ms sits outside
		// the path — whatever it turns out to be.
		assert_eq!(row["dispatch_loop_wall_ms"], 40.0);
		assert_eq!(row["path_dispatch_sum_ms"], 0.0);
		assert_eq!(row["loop_non_path_ms"], 40.0);

		let mut overlapping = contained.clone();
		overlapping
			.path_components
			.insert("dispatch".to_owned(), 90.0);
		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &overlapping);
		assert_eq!(row["path_dispatch_sum_ms"], 90.0);
		assert_eq!(row["dispatch_loop_wall_ms"], 40.0);
		assert!(
			row["loop_non_path_ms"].is_null(),
			"windows summing past the loop must refuse the subtraction, not report -50"
		);
	}

	/// The residual is carried even when the spans nearly cover the gap. A receipt
	/// that dropped it once three plausible timers existed would have stopped
	/// measuring coverage and started assuming it.
	#[test]
	fn adr554_lifecycle_residual_is_present_even_when_near_zero() {
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			Some(5.0),
			None,
			toron::coordinator::QueryLifecycleSpans {
				pre_dispatch_micros: 30_000,
				dispatch_loop_wall_micros: 0,
				post_dispatch_finalization_micros: 10_000,
			},
		)
		.expect("fixture attributes");

		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);
		assert!(
			row.get("lifecycle_residual_ms").is_some(),
			"the residual is reported, not elided when it looks small"
		);
		// 45 unnamed less sequential terms 5 + 30 + 10 = 45 leaves exactly zero, and
		// the field is still emitted rather than dropped as uninteresting.
		assert_eq!(row["lifecycle_residual_ms"], 0.0);
	}

	/// A local child reports **no** planning figure. Absent is not zero: a zero
	/// would read as "measured and found none", and the local arm's declared
	/// boundary starts after binding, so no clock ever ran.
	#[test]
	fn adr554_an_unmeasured_arm_reports_absent_planning_not_zero() {
		let attribution = timing_atlas_child_attribution(vec![decomposition_node(0.0, 0.0, 100.0)])
			.expect("fixture attributes");
		let row = timing_atlas_attribution_path_row("q19", "local", 0, 1, 50.0, &attribution);
		assert!(row["sql_planning_ms"].is_null());
		assert_eq!(
			row["lifecycle_residual_ms"], row["unnamed_residual_ms"],
			"with no spans measured the residual is the whole unnamed gap"
		);
	}

	/// A span that measures more than the gap it decomposes is refused by sign.
	///
	/// This is the control the first B2 run lacked. Its drain clock opened before
	/// `dispatch_loop` rather than after, so the span covered the entire query and
	/// the residual went to -2,138 ms on q19. Replay checked the arithmetic, which
	/// held, and passed it. Sign is the check that does not.
	#[test]
	fn adr554_replay_rejects_spans_larger_than_the_gap_they_decompose() {
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			Some(1.0),
			None,
			toron::coordinator::QueryLifecycleSpans {
				// A finalization span that swallowed the dispatch loop, as the first
				// B2 run's drain clock did.
				pre_dispatch_micros: 1_000,
				dispatch_loop_wall_micros: 0,
				post_dispatch_finalization_micros: 90_000,
			},
		)
		.expect("fixture attributes");
		let row = timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);
		assert!(
			row["lifecycle_residual_ms"]
				.as_f64()
				.is_some_and(|ms| ms < 0.0),
			"the fixture must reproduce the defect it guards against"
		);

		let problems = timing_atlas_replay_attribution_path(&row)
			.expect_err("a negative residual must be refused");
		assert!(
			problems
				.iter()
				.any(|problem| problem.contains("is negative")),
			"{problems:?}"
		);
	}

	/// Replay refuses a receipt whose residual does not follow from its own spans.
	#[test]
	fn adr554_replay_rejects_a_residual_that_does_not_follow_from_its_spans() {
		let attribution = timing_atlas_child_attribution_with_lifecycle(
			vec![decomposition_node(0.0, 0.0, 100.0)],
			None,
			Some(4.0),
			toron::coordinator::QueryLifecycleSpans {
				dispatch_loop_wall_micros: 0,
				pre_dispatch_micros: 6_000,
				post_dispatch_finalization_micros: 2_000,
			},
		)
		.expect("fixture attributes");
		let mut row =
			timing_atlas_attribution_path_row("q19", "distributed", 0, 1, 50.0, &attribution);
		row["lifecycle_residual_ms"] = serde_json::json!(1.0);

		let problems = timing_atlas_replay_attribution_path(&row)
			.expect_err("a residual that does not follow must be refused");
		assert!(
			problems
				.iter()
				.any(|problem| problem.contains("lifecycle residual")),
			"{problems:?}"
		);
	}

	/// One node carrying components, bookkeeping, and a wall far larger than either.
	fn decomposition_node(
		telemetry_lock_ms: f64,
		framework_overhead_ms: f64,
		wall_ms: f64,
	) -> TimingAtlasTaskNode {
		TimingAtlasTaskNode {
			aggregate_accumulation_ms: 0.0,
			stage_id: 0,
			task_id: 0,
			wall_ms,
			components: vec![
				(TimingAtlasComponent::Scan, 2.0),
				(TimingAtlasComponent::Operator, 3.0),
				(TimingAtlasComponent::Shuffle, 0.0),
				(TimingAtlasComponent::Dispatch, 0.0),
				(TimingAtlasComponent::Transport, 0.0),
			],
			worker_id: String::new(),
			telemetry_lock_ms,
			framework_overhead_ms,
			upstream: Vec::new(),
		}
	}

	/// ADR 551 follow-up: the unnamed residual is the remainder minus **only** the two
	/// in-task counters.
	#[test]
	fn adr551_unnamed_residual_is_remainder_less_in_task_time() {
		let attribution = timing_atlas_child_attribution(vec![decomposition_node(0.5, 1.5, 100.0)])
			.expect("fixture attributes");
		assert_eq!(attribution.in_task_unattributed_ms, 2.0);

		let row = timing_atlas_attribution_path_row("q01", "distributed", 0, 1, 50.0, &attribution);
		assert_eq!(row["telemetry_lock_ms"], 0.5);
		assert_eq!(row["framework_overhead_ms"], 1.5);
		assert_eq!(row["in_task_unattributed_ms"], 2.0);
		// attributed = scan 2 + operator 3; remainder = 50 - 5 = 45; residual = 45 - 2.
		assert_eq!(row["remainder_ms"], 45.0);
		assert_eq!(row["unnamed_residual_ms"], 43.0);
	}

	/// The envelope exclusion, stated as a control because it is the one substitution that
	/// would look reasonable and be wrong: raw `task_wall` **contains** the components and
	/// the in-task counters, so folding it into either would collapse the residual to
	/// nonsense. This is not in tension with `framework_overhead` being wall-derived — the
	/// worker computes that by subtracting its own accounted phases from the wall, which is
	/// the opposite operation to adding the whole envelope back as a term.
	#[test]
	fn adr551_task_wall_never_enters_the_subtraction() {
		let narrow = timing_atlas_child_attribution(vec![decomposition_node(0.5, 1.5, 10.0)])
			.expect("narrow wall attributes");
		let wide = timing_atlas_child_attribution(vec![decomposition_node(0.5, 1.5, 10_000.0)])
			.expect("wide wall attributes");

		assert_eq!(narrow.in_task_unattributed_ms, wide.in_task_unattributed_ms);
		let narrow_row =
			timing_atlas_attribution_path_row("q01", "distributed", 0, 1, 50.0, &narrow);
		let wide_row = timing_atlas_attribution_path_row("q01", "distributed", 0, 1, 50.0, &wide);
		assert_eq!(
			narrow_row["unnamed_residual_ms"], wide_row["unnamed_residual_ms"],
			"a thousandfold wall must not move the residual"
		);
		assert_ne!(
			wide_row["path_wall_ms"], wide_row["in_task_unattributed_ms"],
			"the envelope is reported, and reported separately"
		);
	}

	/// In-task time sums over the selected path only, exactly as components do. A node off
	/// the path contributing here would make the residual incoherent with the components it
	/// sits beside.
	#[test]
	fn adr551_in_task_time_sums_over_the_selected_path_only() {
		let mut on_path = decomposition_node(1.0, 1.0, 100.0);
		on_path.stage_id = 0;
		let mut off_path = decomposition_node(50.0, 50.0, 1.0);
		off_path.stage_id = 1;
		off_path.task_id = 1;

		let attribution = timing_atlas_child_attribution(vec![on_path, off_path])
			.expect("two disconnected nodes attribute");
		assert_eq!(
			attribution.in_task_unattributed_ms, 2.0,
			"the unselected node's 100ms of in-task time is not on the path"
		);
	}

	/// The fixture's local arm: one node whose portions include an observed shuffle and no
	/// transport, exactly the shape a real local plan with an in-memory exchange produces.
	fn fixture_local_attribution() -> TimingAtlasChildAttribution {
		timing_atlas_child_attribution(vec![TimingAtlasTaskNode {
			stage_id: 0,
			task_id: 0,
			wall_ms: 9.0,
			components: vec![
				(TimingAtlasComponent::Scan, 2.0),
				(TimingAtlasComponent::Operator, 3.0),
				(TimingAtlasComponent::Shuffle, 1.0),
				(TimingAtlasComponent::Dispatch, 0.0),
				(TimingAtlasComponent::Transport, 0.0),
			],
			worker_id: String::new(),
			aggregate_accumulation_ms: 0.0,
			telemetry_lock_ms: 0.0,
			framework_overhead_ms: 0.0,
			upstream: Vec::new(),
		}])
		.expect("local fixture attributes")
	}

	/// The fixture's distributed arm: a producer feeding a consumer, so the selected path
	/// has a real edge for replay to verify.
	fn fixture_distributed_attribution() -> TimingAtlasChildAttribution {
		timing_atlas_child_attribution(vec![
			TimingAtlasTaskNode {
				stage_id: 0,
				task_id: 0,
				wall_ms: 8.0,
				components: vec![
					(TimingAtlasComponent::Scan, 1.5),
					(TimingAtlasComponent::Operator, 2.0),
					(TimingAtlasComponent::Shuffle, 2.0),
					(TimingAtlasComponent::Dispatch, 1.0),
					(TimingAtlasComponent::Transport, 0.5),
				],
				worker_id: String::new(),
				aggregate_accumulation_ms: 0.0,
				telemetry_lock_ms: 0.0,
				framework_overhead_ms: 0.0,
				upstream: Vec::new(),
			},
			TimingAtlasTaskNode {
				stage_id: 1,
				task_id: 0,
				wall_ms: 10.0,
				components: vec![
					(TimingAtlasComponent::Scan, 0.0),
					(TimingAtlasComponent::Operator, 3.0),
					(TimingAtlasComponent::Shuffle, 1.0),
					(TimingAtlasComponent::Dispatch, 0.5),
					(TimingAtlasComponent::Transport, 1.0),
				],
				worker_id: String::new(),
				aggregate_accumulation_ms: 0.0,
				telemetry_lock_ms: 0.0,
				framework_overhead_ms: 0.0,
				upstream: vec![(0, 0)],
			},
		])
		.expect("distributed fixture attributes")
	}

	fn fixture_child(
		query: &str,
		arm: &'static str,
		pair_index: usize,
		elapsed: f64,
		attribution: TimingAtlasChildAttribution,
	) -> TimingAtlasChild {
		TimingAtlasChild {
			query: query.to_string(),
			arm,
			pair_index: Some(pair_index),
			position_in_pair: 0,
			attempt: 1,
			query_elapsed_ms: elapsed,
			row_count: Some(4),
			identity_matches: Some(true),
			valid: true,
			failure: None,
			failure_is_io: false,
			attribution: Some(attribution),
		}
	}

	pub(crate) fn journal_lines(cells: &[&str], pairs_per_cell: usize) -> Vec<serde_json::Value> {
		let mut rows = vec![
			timing_atlas_header_row(
				&HostQuiescence {
					service_inactive: true,
					sql_port_free: true,
				},
				"fnv1a64:test",
				8.0,
				"strict",
			),
			timing_atlas_exclusion_row("q15"),
		];
		for query in cells {
			rows.push(serde_json::json!({
				"kind": TIMING_ATLAS_KIND_CELL_SETUP, "query": query
			}));
			let mut pairs = Vec::new();
			for pair_index in 0..pairs_per_cell {
				rows.push(serde_json::json!({
					"kind": TIMING_ATLAS_KIND_PAIR,
					"query": query,
					"pair_index": pair_index,
					"first_arm": "local",
					"local_elapsed_ms": 10.0,
					"distributed_elapsed_ms": 20.0,
					"delta_ms": 10.0,
					"ratio": 2.0,
					"local_attempt": 1,
					"distributed_attempt": 1,
				}));
				// Attribution rows come from the production emitters, so the fixture
				// cannot drift from what the driver actually writes.
				let local = fixture_child(
					query,
					"local",
					pair_index,
					10.0,
					fixture_local_attribution(),
				);
				let distributed = fixture_child(
					query,
					"distributed",
					pair_index,
					20.0,
					fixture_distributed_attribution(),
				);
				for child in [&local, &distributed] {
					rows.push(timing_atlas_attribution_path_row(
						query,
						child.arm,
						pair_index,
						child.attempt,
						child.query_elapsed_ms,
						child.attribution.as_ref().expect("fixture attribution"),
					));
				}
				pairs.push(TimingAtlasPair {
					pair_index,
					first_arm: "local",
					local,
					distributed,
					delta_ms: 10.0,
					ratio: Some(2.0),
				});
			}
			rows.extend(timing_atlas_component_rows(query, &pairs));
			rows.push(serde_json::json!({
				"kind": TIMING_ATLAS_KIND_CLEANUP,
				"query": query,
				"succeeded": true,
				"returned_to_baseline": true,
			}));
			rows.push(serde_json::json!({
				"kind": TIMING_ATLAS_KIND_RESIDENCY, "query": query, "stable": true
			}));
			rows.push(serde_json::json!({
				"kind": TIMING_ATLAS_KIND_CELL_SUMMARY,
				"query": query,
				"finalized": pairs_per_cell == TIMING_ATLAS_MEASURED_PAIRS,
				"pairs": pairs_per_cell,
				"invalid_attempts": 0,
				"verdict": "distinguished",
			}));
		}
		rows.push(serde_json::json!({
			"kind": TIMING_ATLAS_KIND_HOST_END,
			"service_inactive": true,
			"sql_port_free": true,
		}));
		rows.push(serde_json::json!({ "kind": TIMING_ATLAS_SEAL_KIND }));
		rows
	}

	fn render(rows: &[serde_json::Value]) -> String {
		rows.iter()
			.map(|row| row.to_string())
			.collect::<Vec<_>>()
			.join("\n")
	}

	fn granted() -> TimingAuthorityConditions {
		TimingAuthorityConditions {
			clean_build: true,
			full_hash_dataset: true,
			quiescent_host: true,
			protocol_unmodified: true,
			complete_cohort: true,
		}
	}

	#[test]
	fn a_complete_journal_replays_to_authority() {
		let cells = timing_atlas_cells();
		let rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		let replay = timing_atlas_replay(&render(&rows));
		assert!(
			replay.violations.is_empty(),
			"violations: {:?}",
			replay.violations
		);
		assert_eq!(replay.finalized_cells.len(), 21);
		assert_eq!(replay.exclusions, vec!["q15".to_string()]);
		assert_eq!(replay.distinguished_slower.len(), 21);
		assert!(replay.authoritative(&granted()));
	}

	/// Six pairs per cell cannot replay to authority, whatever the summary claims.
	#[test]
	fn six_of_seven_pairs_fails_replay() {
		let cells = timing_atlas_cells();
		let mut rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS - 1);
		// Force the summary to lie about finalization.
		for row in rows.iter_mut() {
			if row.get("kind").and_then(|k| k.as_str()) == Some(TIMING_ATLAS_KIND_CELL_SUMMARY) {
				row["finalized"] = serde_json::Value::Bool(true);
			}
		}
		let replay = timing_atlas_replay(&render(&rows));
		assert!(
			!replay.violations.is_empty(),
			"a lying summary must be caught by replay"
		);
		assert!(!replay.authoritative(&granted()));
	}

	/// A pair row whose delta disagrees with its own elapsed values is a violation — the
	/// journal is recomputed, not trusted.
	#[test]
	fn a_pair_row_that_contradicts_itself_is_rejected() {
		let cells = timing_atlas_cells();
		let mut rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		for row in rows.iter_mut() {
			if row.get("kind").and_then(|k| k.as_str()) == Some(TIMING_ATLAS_KIND_PAIR) {
				row["delta_ms"] = serde_json::json!(999.0);
				break;
			}
		}
		let replay = timing_atlas_replay(&render(&rows));
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("delta disagrees")),
			"violations: {:?}",
			replay.violations
		);
	}

	/// A missing terminal seal, a missing host-end observation, or a second exclusion each
	/// forfeit authority.
	#[test]
	fn missing_seal_or_host_end_or_extra_exclusion_forfeits_authority() {
		let cells = timing_atlas_cells();

		let mut unsealed = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		unsealed.pop();
		let replay = timing_atlas_replay(&render(&unsealed));
		assert!(!replay.terminal_seal);
		assert!(!replay.authoritative(&granted()));

		let mut noisy_end = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		for row in noisy_end.iter_mut() {
			if row.get("kind").and_then(|k| k.as_str()) == Some(TIMING_ATLAS_KIND_HOST_END) {
				row["service_inactive"] = serde_json::Value::Bool(false);
			}
		}
		let replay = timing_atlas_replay(&render(&noisy_end));
		assert_eq!(replay.host_end_quiescent, Some(false));
		assert!(!replay.authoritative(&granted()));

		let mut twice = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		twice.insert(2, timing_atlas_exclusion_row("q15"));
		let replay = timing_atlas_replay(&render(&twice));
		assert!(!replay.authoritative(&granted()));
	}

	/// Rows after the terminal seal are a violation: the seal is terminal.
	#[test]
	fn rows_after_the_seal_are_rejected() {
		let cells = timing_atlas_cells();
		let mut rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		rows.push(serde_json::json!({
			"kind": TIMING_ATLAS_KIND_CELL_SETUP, "query": "q01"
		}));
		let replay = timing_atlas_replay(&render(&rows));
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("follows the terminal seal"))
		);
	}

	/// A ratio must never be NaN or infinity; a zero local arm serializes null.
	#[test]
	fn zero_local_arm_serializes_a_null_ratio() {
		let child = |arm: &'static str, elapsed: f64| TimingAtlasChild {
			query: "q01".to_string(),
			arm,
			pair_index: Some(0),
			position_in_pair: 0,
			attempt: 1,
			query_elapsed_ms: elapsed,
			row_count: Some(1),
			identity_matches: Some(true),
			valid: true,
			failure: None,
			failure_is_io: false,
			attribution: None,
		};
		let pair = assemble_timing_atlas_pair(0, child("local", 0.0), child("distributed", 5.0));
		assert!(pair.ratio.is_none(), "a zero local arm has no ratio");
		let row = timing_atlas_pair_row("q01", &pair);
		assert_eq!(row["ratio"], serde_json::Value::Null);
		assert!(!row.to_string().contains("inf"));
		assert!(!row.to_string().contains("NaN"));
	}

	/// The seal records the authority decision, so it cannot also be required to make it.
	/// A journal missing only its seal must still *support* authority, while a published
	/// artifact must carry one.
	#[test]
	fn seal_is_required_to_read_back_but_not_to_decide() {
		let cells = timing_atlas_cells();
		let mut rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		rows.pop(); // drop the seal, as it stands when the decision is made
		let replay = timing_atlas_replay(&render(&rows));
		assert!(
			replay.supports_authority(&granted()),
			"a complete journal supports authority before its seal exists: {:?}",
			replay.violations
		);
		assert!(
			!replay.authoritative(&granted()),
			"but it is not a published artifact until sealed"
		);
	}

	/// The defect this veto closes: the first authoritative run published complete timing
	/// for all 21 cells with zero component receipts, so the dominance fork it was meant to
	/// decide could not be evaluated. Timing deltas alone must withhold authority.
	#[test]
	fn complete_timing_without_component_receipts_is_withheld() {
		let cells = timing_atlas_cells();
		// Exactly the first authoritative run's shape: full timing, no component receipts.
		let rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS)
			.into_iter()
			.filter(|row| {
				row.get("kind").and_then(|kind| kind.as_str())
					!= Some(TIMING_ATLAS_KIND_ATTRIBUTION)
			})
			.collect::<Vec<_>>();
		let replay = timing_atlas_replay(&render(&rows));
		assert_eq!(
			replay.finalized_cells.len(),
			21,
			"timing itself is complete"
		);
		assert!(replay.violations.is_empty(), "and internally consistent");
		assert!(
			replay
				.withheld_reasons()
				.iter()
				.any(|reason| reason.starts_with("attribution_receipts_missing")),
			"but authority is withheld for missing receipts: {:?}",
			replay.withheld_reasons()
		);
		assert!(!replay.supports_authority(&granted()));
	}

	/// A component claiming additivity without a non-overlap or critical-path basis cannot
	/// enter the breakdown, and forfeits authority for the run.
	#[test]
	fn non_additive_components_cannot_produce_a_dominance_verdict() {
		let cells = timing_atlas_cells();
		let mut rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		for query in &cells {
			rows.insert(
				2,
				serde_json::json!({
					"kind": TIMING_ATLAS_KIND_ATTRIBUTION,
					"query": query,
					"component": "stage_durations",
					"additive": true,
					// Summing concurrent task durations measures work, not wall clock.
					"basis": "concurrent_task_sum",
					"contribution_ms": 100.0,
				}),
			);
		}
		let replay = timing_atlas_replay(&render(&rows));
		assert!(
			!replay.non_additive_components.is_empty(),
			"a concurrent-sum basis must be rejected"
		);
		assert!(
			replay
				.withheld_reasons()
				.contains(&"non_additive_components".to_string())
		);
		assert!(!replay.supports_authority(&granted()));
	}

	/// Only receipt-backed components with an admissible basis satisfy the veto — and the
	/// component rows the emitters produce carry exactly the taxonomy's pairing: four
	/// eligible additive components, dispatch raw with a null contribution, reduction
	/// absent entirely.
	#[test]
	fn receipt_backed_components_satisfy_the_attribution_requirement() {
		let cells = timing_atlas_cells();
		let rows = journal_lines(&cells, TIMING_ATLAS_MEASURED_PAIRS);
		let replay = timing_atlas_replay(&render(&rows));
		assert_eq!(replay.cells_with_attribution.len(), 21);
		assert!(replay.non_additive_components.is_empty());
		assert!(
			replay.withheld_reasons().is_empty(),
			"reasons: {:?}",
			replay.withheld_reasons()
		);
		assert!(replay.supports_authority(&granted()));

		let q01_components: Vec<&serde_json::Value> = rows
			.iter()
			.filter(|row| {
				row.get("kind").and_then(|kind| kind.as_str())
					== Some(TIMING_ATLAS_KIND_ATTRIBUTION)
					&& row.get("query").and_then(|q| q.as_str()) == Some("q01")
			})
			.collect();
		let labels: Vec<&str> = q01_components
			.iter()
			.filter_map(|row| row.get("component").and_then(|c| c.as_str()))
			.collect();
		assert_eq!(
			labels,
			vec!["scan", "operator", "shuffle", "dispatch", "transport"],
			"every timed component gets a row; reduction never does"
		);
		for row in &q01_components {
			let label = row.get("component").and_then(|c| c.as_str()).unwrap();
			let eligible = row.get("eligible").and_then(|e| e.as_bool()).unwrap();
			let additive = row.get("additive").and_then(|a| a.as_bool()).unwrap();
			if label == "dispatch" {
				assert!(!eligible && !additive, "dispatch stays raw");
				assert!(row.get("contribution_ms").unwrap().is_null());
			} else {
				assert!(eligible && additive, "{label} is receipt-backed");
				assert!(row.get("contribution_ms").unwrap().is_f64());
			}
		}
	}
}

#[cfg(test)]
mod adr538_timing_atlas_driver {
	use super::*;

	/// End-to-end through the real driver at a small scale. The unit tests all passed while
	/// the driver constructed workers twice and consumed the catalog before the first child,
	/// because nothing exercised `run_timing_atlas` itself.
	/// Deep binds need an explicit stack; run on a dedicated 64 MB thread.
	#[test]
	fn driver_produces_a_journal_that_replays_without_violations() {
		let root = std::env::temp_dir().join(format!("adr538-driver-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&root);
		let canonical = root.join("timing_atlas.jsonl");

		let journal = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn({
				let canonical = canonical.clone();
				move || {
					// Current-thread runtime: deep multi-join binds (q10+) then run on this
					// thread's 64 MB stack rather than on runtime workers, whose stacks are
					// independent of the spawning thread's.
					let runtime = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.expect("runtime");
					runtime.block_on(async {
						let _ =
							run_timing_atlas("strict", 0.01, Path::new("unused"), &canonical).await;
						let partial = PathBuf::from(format!("{}.partial", canonical.display()));
						std::fs::read_to_string(&partial)
							.or_else(|_| std::fs::read_to_string(&canonical))
							.unwrap_or_default()
					})
				}
			})
			.expect("spawn")
			.join()
			.expect("join");

		assert!(!journal.is_empty(), "the driver must write a journal");
		let replay = timing_atlas_replay(&journal);

		// The driver refuses to start on a non-quiescent host, so this test asserts the
		// behavior appropriate to the machine it runs on rather than silently depending on
		// whether a local deployment happens to be up.
		if journal.contains("host_not_quiescent") {
			assert_eq!(
				replay.finalized_cells.len(),
				0,
				"an aborted run measures nothing"
			);
			assert!(
				!replay.supports_authority(&TimingAuthorityConditions {
					clean_build: true,
					full_hash_dataset: true,
					quiescent_host: false,
					protocol_unmodified: true,
					complete_cohort: false,
				}),
				"and cannot support authority"
			);
		} else {
			assert_eq!(
				replay.finalized_cells.len(),
				timing_atlas_cells().len(),
				"every cell must finalize; violations: {:?}",
				replay.violations
			);
			assert!(
				replay.violations.is_empty(),
				"the journal must replay without violations: {:?}",
				replay.violations
			);
			assert_eq!(replay.exclusions, vec!["q15".to_string()]);
		}
		let _ = std::fs::remove_dir_all(&root);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_component_taxonomy {
	use super::*;

	fn receipt(
		component: TimingAtlasComponent,
		arm: TimingArm,
		elapsed: f64,
		basis: TimingAtlasBasis,
	) -> TimingAtlasComponentReceipt {
		TimingAtlasComponentReceipt {
			component,
			scope: format!("{}::{}", component.label(), arm.label()),
			arm,
			child_query_id: QueryId::new(),
			raw_elapsed_ms: elapsed,
			basis,
		}
	}

	/// Admissible components plus the unattributed remainder reconcile each arm's measured
	/// elapsed — per arm, not pooled.
	#[test]
	fn admissible_components_reconcile_each_arm() {
		let receipts = vec![
			receipt(
				TimingAtlasComponent::Scan,
				TimingArm::Local,
				40.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Local,
				30.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Scan,
				TimingArm::Distributed,
				45.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Distributed,
				35.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Transport,
				TimingArm::Distributed,
				20.0,
				TimingAtlasBasis::CriticalPath,
			),
		];
		assert!(timing_atlas_arm_reconciles(
			&receipts,
			TimingArm::Local,
			100.0,
			30.0
		));
		assert!(timing_atlas_arm_reconciles(
			&receipts,
			TimingArm::Distributed,
			120.0,
			20.0
		));
		assert!(
			!timing_atlas_arm_reconciles(&receipts, TimingArm::Local, 100.0, 0.0),
			"a remainder that does not close the arm must fail"
		);
	}

	/// A concurrent span is retained as evidence but never enters the additive breakdown,
	/// unless the receipt explicitly claims a critical path.
	#[test]
	fn overlapping_spans_are_rejected_unless_marked_critical_path() {
		let concurrent = vec![
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Local,
				10.0,
				TimingAtlasBasis::ConcurrentSpan,
			),
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Distributed,
				90.0,
				TimingAtlasBasis::ConcurrentSpan,
			),
		];
		let contributions = timing_atlas_contributions(&concurrent).expect("pairs");
		assert_eq!(contributions.len(), 1);
		assert!(
			!contributions[0].admissible,
			"a concurrent span measures work, not wall clock"
		);
		assert_eq!(
			timing_atlas_leading_component(&contributions, 100.0),
			None,
			"and therefore cannot lead a cell"
		);

		let critical = vec![
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Local,
				10.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Distributed,
				90.0,
				TimingAtlasBasis::CriticalPath,
			),
		];
		let contributions = timing_atlas_contributions(&critical).expect("pairs");
		assert!(contributions[0].admissible);
		assert_eq!(
			timing_atlas_leading_component(&contributions, 100.0),
			Some(TimingAtlasComponent::Operator),
			"80 of a 100 ms delta leads"
		);
	}

	/// A distributed receipt with no local counterpart is an error, not a zero — otherwise
	/// an unimplemented local probe would hand the whole delta to that component.
	#[test]
	fn missing_local_counterparts_cannot_silently_become_zero() {
		let orphan = vec![receipt(
			TimingAtlasComponent::Scan,
			TimingArm::Distributed,
			90.0,
			TimingAtlasBasis::CriticalPath,
		)];
		assert_eq!(
			timing_atlas_contributions(&orphan),
			Err(TimingAtlasPairingError::MissingLocalCounterpart(
				TimingAtlasComponent::Scan
			)),
			"scan exists on both arms, so a missing local side is a defect"
		);

		// Transport is the declared exception: the local arm cannot incur it.
		let transport = vec![receipt(
			TimingAtlasComponent::Transport,
			TimingArm::Distributed,
			25.0,
			TimingAtlasBasis::CriticalPath,
		)];
		let contributions = timing_atlas_contributions(&transport).expect("declared zero");
		assert_eq!(contributions[0].local_ms, 0.0);
		assert_eq!(contributions[0].contribution_ms, 25.0);
		assert!(
			TimingAtlasComponent::Transport.local_counterpart_is_defined_zero(),
			"and the exception is declared in the taxonomy, not inferred"
		);
		assert!(!TimingAtlasComponent::Scan.local_counterpart_is_defined_zero());
	}

	/// Basis must agree across arms: comparing a critical-path distributed figure against a
	/// concurrent-span local one would not be like for like.
	#[test]
	fn basis_must_agree_across_arms() {
		let mismatched = vec![
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Local,
				5.0,
				TimingAtlasBasis::ConcurrentSpan,
			),
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Distributed,
				50.0,
				TimingAtlasBasis::CriticalPath,
			),
		];
		assert_eq!(
			timing_atlas_contributions(&mismatched),
			Err(TimingAtlasPairingError::BasisDisagreement(
				TimingAtlasComponent::Shuffle
			))
		);
	}

	/// Only paired contributions drive dominance, and a negative or absent delta leads
	/// nothing.
	#[test]
	fn dominance_uses_paired_contributions_only() {
		let contributions = vec![TimingAtlasContribution {
			component: TimingAtlasComponent::Dispatch,
			local_ms: 5.0,
			distributed_ms: 60.0,
			contribution_ms: 55.0,
			basis: TimingAtlasBasis::CriticalPath,
			admissible: true,
		}];
		assert_eq!(
			timing_atlas_leading_component(&contributions, 100.0),
			Some(TimingAtlasComponent::Dispatch)
		);
		assert_eq!(
			timing_atlas_leading_component(&contributions, 200.0),
			None,
			"55 of 200 ms does not reach half"
		);
		assert_eq!(
			timing_atlas_leading_component(&contributions, -10.0),
			None,
			"a distributed-faster cell is not in S and leads nothing"
		);
	}

	/// The taxonomy stays granular: no summed stage component may stand in for concurrent
	/// operator spans.
	#[test]
	fn taxonomy_keeps_categories_distinct() {
		let labels = TimingAtlasComponent::ALL
			.iter()
			.map(|component| component.label())
			.collect::<Vec<_>>();
		assert_eq!(
			labels,
			vec![
				"scan",
				"operator",
				"shuffle",
				"dispatch",
				"transport",
				"reduction"
			]
		);
		assert!(
			!labels.contains(&"stage"),
			"a stage component would hide concurrent spans behind one figure"
		);
	}

	/// `reduction` happens after the elapsed boundary, so it must never appear among timed
	/// contributions — and must not be handed a fabricated zero to make it fit.
	#[test]
	fn reduction_does_not_participate_in_timed_attribution() {
		assert!(!TimingAtlasComponent::Reduction.participates_in_timed_attribution());
		for component in TimingAtlasComponent::ALL {
			if component != TimingAtlasComponent::Reduction {
				assert!(component.participates_in_timed_attribution());
			}
		}
		let receipts = vec![
			receipt(
				TimingAtlasComponent::Reduction,
				TimingArm::Local,
				10.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Reduction,
				TimingArm::Distributed,
				900.0,
				TimingAtlasBasis::CriticalPath,
			),
		];
		let contributions = timing_atlas_contributions(&receipts).expect("pairs");
		assert!(
			contributions.is_empty(),
			"reduction is metadata, not a timed contribution"
		);
		assert_eq!(timing_atlas_leading_component(&contributions, 100.0), None);
	}

	/// Only `transport` carries the zero-local exception. `shuffle` was refused on evidence:
	/// twenty of twenty-one local plans repartition in memory.
	#[test]
	fn transport_is_the_only_declared_zero_local_component() {
		let declared = TimingAtlasComponent::ALL
			.iter()
			.filter(|component| component.local_counterpart_is_defined_zero())
			.copied()
			.collect::<Vec<_>>();
		assert_eq!(declared, vec![TimingAtlasComponent::Transport]);
		assert!(!TimingAtlasComponent::Shuffle.local_counterpart_is_defined_zero());
		assert!(!TimingAtlasComponent::Dispatch.local_counterpart_is_defined_zero());
		assert_eq!(
			timing_atlas_contributions(&[receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Distributed,
				50.0,
				TimingAtlasBasis::CriticalPath
			)]),
			Err(TimingAtlasPairingError::MissingLocalCounterpart(
				TimingAtlasComponent::Shuffle
			))
		);
	}

	/// Dispatch has no semantically equivalent local interval. It must be retained as raw
	/// evidence — not invented as a zero, and not reported as a missing-counterpart defect.
	#[test]
	fn dispatch_without_a_counterpart_is_retained_not_zeroed_nor_an_error() {
		let dispatch_only = vec![receipt(
			TimingAtlasComponent::Dispatch,
			TimingArm::Distributed,
			400.0,
			TimingAtlasBasis::CriticalPath,
		)];
		let contributions =
			timing_atlas_contributions(&dispatch_only).expect("must not be an error");
		assert!(
			contributions.is_empty(),
			"a distributed-only figure is not a contribution"
		);
		assert_eq!(
			timing_atlas_leading_component(&contributions, 500.0),
			None,
			"and raw distributed dispatch time must never lead a cell"
		);
		assert_eq!(
			TimingAtlasComponent::Dispatch.local_pairing(),
			TimingAtlasLocalPairing::NoLocalEquivalent
		);
		assert!(!TimingAtlasComponent::Dispatch.local_counterpart_is_defined_zero());
	}

	/// Shuffle is paired per cell. A distributed-only shuffle receipt is a defect, and q06's
	/// structural zero must arrive as an observed local receipt rather than an exception.
	#[test]
	fn shuffle_pairs_per_cell_with_q06_as_an_observed_zero() {
		assert_eq!(
			TimingAtlasComponent::Shuffle.local_pairing(),
			TimingAtlasLocalPairing::Required
		);
		assert_eq!(
			timing_atlas_contributions(&[receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Distributed,
				60.0,
				TimingAtlasBasis::CriticalPath
			)]),
			Err(TimingAtlasPairingError::MissingLocalCounterpart(
				TimingAtlasComponent::Shuffle
			)),
			"a missing local shuffle receipt is a defect, not a zero"
		);

		// q06: the one cell whose local plan carries no Exchange. Its local receipt is
		// observed and reads zero, which is evidence rather than a declaration.
		let q06 = vec![
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Local,
				0.0,
				TimingAtlasBasis::CriticalPath,
			),
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Distributed,
				60.0,
				TimingAtlasBasis::CriticalPath,
			),
		];
		let contributions = timing_atlas_contributions(&q06).expect("observed zero pairs");
		assert_eq!(contributions[0].local_ms, 0.0);
		assert_eq!(contributions[0].contribution_ms, 60.0);
		assert!(contributions[0].admissible);
	}

	/// Only transport may reach dominance without a local receipt.
	#[test]
	fn only_transport_can_lead_without_a_local_receipt() {
		let transport_only = vec![receipt(
			TimingAtlasComponent::Transport,
			TimingArm::Distributed,
			80.0,
			TimingAtlasBasis::CriticalPath,
		)];
		let contributions = timing_atlas_contributions(&transport_only).expect("declared zero");
		assert_eq!(
			timing_atlas_leading_component(&contributions, 100.0),
			Some(TimingAtlasComponent::Transport)
		);

		let pairings = TimingAtlasComponent::ALL
			.iter()
			.filter(|component| component.local_pairing() == TimingAtlasLocalPairing::DeclaredZero)
			.copied()
			.collect::<Vec<_>>();
		assert_eq!(pairings, vec![TimingAtlasComponent::Transport]);
	}
}

#[cfg(test)]
mod adr538_local_arm_structure {
	use super::*;

	fn contains_exchange(plan: &toron::ir::ExecutionPlan) -> bool {
		use toron::ir::ExecutionPlan as P;
		match plan {
			P::Exchange { .. } => true,
			P::Join { left, right, .. } => contains_exchange(left) || contains_exchange(right),
			P::Union { left, right, .. } => contains_exchange(left) || contains_exchange(right),
			P::Filter { input, .. }
			| P::Project { input, .. }
			| P::Aggregate { input, .. }
			| P::Sort { input, .. }
			| P::Window { input, .. }
			| P::Take { input, .. }
			| P::Repeat { input, .. } => contains_exchange(input),
			_ => false,
		}
	}

	/// ADR 538 decision 2, answered empirically: `shuffle` is NOT a structural zero-local
	/// component. Twenty of the twenty-one eligible cells bind a local plan containing an
	/// `Exchange`, which the local executor runs through `bound_exchange` as real in-memory
	/// repartitioning. Only q06 — a scalar aggregate with no exchange — lacks one. Shuffle
	/// therefore requires a real local counterpart and cannot borrow transport's exception.
	/// Deep multi-join binds (q10+) need an explicit stack.
	#[test]
	fn local_plans_carry_exchanges_so_shuffle_is_not_zero_local() {
		let offenders = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let mut offenders: Vec<String> = Vec::new();
					for query in timing_atlas_cells() {
						let spec = build_perf_query_spec(query, "strict", 0.01).expect("spec");
						let (tables, _storage) = metadata_storage_from_tables(spec.tables);
						let mut timings = PerfPhaseTimings::default();
						let catalog =
							load_catalog_from_metadata(tables, &mut timings, Instant::now())
								.expect("catalog");
						let logical = SqlPlanner::new().plan(&spec.sql).expect("plan");
						let budget = execution_census_local_budget();
						let config =
							gauge_planner_config_with_task_memory(Some(budget.memory_per_task));
						let bound = bind(&logical, &catalog, &config).await.expect("bind");
						if contains_exchange(&bound) {
							offenders.push(query.to_string());
						}
					}
					offenders
				})
			})
			.expect("spawn")
			.join()
			.expect("join");

		let with_exchange = offenders;
		assert_eq!(
			with_exchange.len(),
			20,
			"twenty cells bind a local exchange; a change here re-opens whether shuffle can \
			 be a declared zero-local component: {with_exchange:?}"
		);
		assert!(
			!with_exchange.contains(&"q06".to_string()),
			"q06 is the sole exchange-free local plan"
		);
	}
}

#[cfg(test)]
mod adr538_timing_atlas_critical_path {
	use super::*;

	fn node(
		stage: u32,
		task: u32,
		wall: f64,
		components: Vec<(TimingAtlasComponent, f64)>,
		upstream: Vec<(u32, u32)>,
	) -> TimingAtlasTaskNode {
		TimingAtlasTaskNode {
			stage_id: stage,
			task_id: task,
			wall_ms: wall,
			components,
			worker_id: String::new(),
			aggregate_accumulation_ms: 0.0,
			telemetry_lock_ms: 0.0,
			framework_overhead_ms: 0.0,
			upstream,
		}
	}

	/// The path is chosen by total wall through the DAG, and only that path's portions are
	/// attributed. This fixture is built so the per-component maxima taken independently
	/// would exceed the selected path — the overcounting the rule forbids.
	#[test]
	fn attribution_follows_one_selected_path_not_per_component_maxima() {
		// Producer, then two concurrent consumers, then a terminal.
		let nodes = vec![
			node(
				0,
				0,
				100.0,
				vec![(TimingAtlasComponent::Scan, 100.0)],
				vec![],
			),
			// Slow on operator, cheap on shuffle.
			node(
				1,
				0,
				400.0,
				vec![
					(TimingAtlasComponent::Operator, 380.0),
					(TimingAtlasComponent::Shuffle, 20.0),
				],
				vec![(0, 0)],
			),
			// Faster overall, but the largest shuffle figure in the graph.
			node(
				1,
				1,
				200.0,
				vec![
					(TimingAtlasComponent::Operator, 10.0),
					(TimingAtlasComponent::Shuffle, 190.0),
				],
				vec![(0, 0)],
			),
			node(
				2,
				0,
				50.0,
				vec![(TimingAtlasComponent::Operator, 50.0)],
				vec![(1, 0), (1, 1)],
			),
		];

		let derived = timing_atlas_critical_path(&nodes).expect("a path");
		assert_eq!(
			derived.path,
			vec![(0, 0), (1, 0), (2, 0)],
			"the longest-wall path runs through the slow consumer"
		);
		assert_eq!(derived.total_wall_ms, 550.0);
		assert_eq!(derived.components.get("scan"), Some(&100.0));
		assert_eq!(derived.components.get("operator"), Some(&430.0));
		assert_eq!(
			derived.components.get("shuffle"),
			Some(&20.0),
			"shuffle comes from the selected path's task, not from the graph's largest \
			 shuffle figure"
		);

		// The forbidden derivation, computed here only to show it differs.
		let per_component_maxima: f64 = [
			nodes
				.iter()
				.flat_map(|n| n.components.iter())
				.filter(|(component, _)| *component == TimingAtlasComponent::Shuffle)
				.map(|(_, portion)| *portion)
				.fold(0.0_f64, f64::max),
			nodes
				.iter()
				.flat_map(|n| n.components.iter())
				.filter(|(component, _)| *component == TimingAtlasComponent::Operator)
				.map(|(_, portion)| *portion)
				.fold(0.0_f64, f64::max),
		]
		.iter()
		.sum();
		assert!(
			per_component_maxima > derived.components["operator"] + derived.components["shuffle"],
			"summing independent per-component maxima overcounts, which is why the rule \
			 selects one path first"
		);
	}

	/// The selected path is persisted edge by edge so replay can re-walk it.
	#[test]
	fn selected_path_is_persisted_for_replay() {
		let nodes = vec![
			node(0, 0, 10.0, vec![(TimingAtlasComponent::Scan, 10.0)], vec![]),
			node(
				1,
				0,
				30.0,
				vec![(TimingAtlasComponent::Operator, 30.0)],
				vec![(0, 0)],
			),
		];
		let derived = timing_atlas_critical_path(&nodes).expect("a path");
		assert_eq!(derived.path, vec![(0, 0), (1, 0)]);
		for window in derived.path.windows(2) {
			let successor = nodes
				.iter()
				.find(|n| n.key() == window[1])
				.expect("successor on the path");
			assert!(
				successor.upstream.contains(&window[0]),
				"every persisted edge must exist in the DAG"
			);
		}
		assert_eq!(derived.total_wall_ms, 40.0);
	}

	/// A single-path local execution uses the same rule, and q06's observed zero survives it.
	#[test]
	fn local_single_path_uses_the_same_rule_including_an_observed_zero() {
		let q06_local = vec![node(
			0,
			0,
			120.0,
			vec![
				(TimingAtlasComponent::Scan, 100.0),
				(TimingAtlasComponent::Operator, 20.0),
				// No Exchange in q06's local plan: observed, not declared.
				(TimingAtlasComponent::Shuffle, 0.0),
			],
			vec![],
		)];
		let derived = timing_atlas_critical_path(&q06_local).expect("a path");
		assert_eq!(derived.path, vec![(0, 0)]);
		assert_eq!(derived.components.get("shuffle"), Some(&0.0));
		assert_eq!(derived.total_wall_ms, 120.0);
	}

	#[test]
	fn an_empty_graph_yields_no_path() {
		assert!(timing_atlas_critical_path(&[]).is_none());
	}
}

#[cfg(test)]
mod adr538_exchange_receipts {
	use super::*;

	/// The exchange component must be produced on BOTH arms, or it cannot be paired. It is
	/// captured in the executor, so a local plan carrying an `Exchange` records it exactly
	/// as a distributed producer task does — and q06, whose local plan has none, records a
	/// genuine observed zero.
	/// Deep binds need an explicit stack.
	#[test]
	fn exchange_repartition_is_recorded_on_the_local_arm() {
		let (with_exchange, q06) = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let measure = |query: &'static str| async move {
						let spec = build_perf_query_spec(query, "strict", 0.01).expect("spec");
						let (tables, storage) = metadata_storage_from_tables(spec.tables);
						let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
						let budget = execution_census_local_budget();
						let context = tpch_execution_context_with_budget(telemetry.clone(), budget);
						let catalog = {
							let mut timings = PerfPhaseTimings::default();
							load_catalog_from_metadata(tables, &mut timings, Instant::now())
								.expect("catalog")
						};
						let logical = SqlPlanner::new().plan(&spec.sql).expect("plan");
						let config =
							gauge_planner_config_with_task_memory(Some(budget.memory_per_task));
						let bound = bind(&logical, &catalog, &config).await.expect("bind");
						let executor = Executor::with_context(storage, context);
						executor.execute_bound(&bound, None).await.expect("execute");
						let phases = telemetry.lock().expect("telemetry").phases;
						phases.exchange_repartition_micros
					};
					(measure("q03").await, measure("q06").await)
				})
			})
			.expect("spawn")
			.join()
			.expect("join");

		assert!(
			with_exchange > 0,
			"a local plan containing an Exchange must record repartition time, or the \
			 component cannot be paired against the distributed arm"
		);
		assert_eq!(
			q06, 0,
			"q06's local plan carries no Exchange, so its zero is observed rather than \
			 declared"
		);
	}
}

#[cfg(test)]
mod adr538_path_bound_validation {
	use super::*;

	fn nodes() -> Vec<TimingAtlasTaskNode> {
		vec![
			TimingAtlasTaskNode {
				stage_id: 0,
				task_id: 0,
				wall_ms: 100.0,
				components: vec![],
				worker_id: String::new(),
				aggregate_accumulation_ms: 0.0,
				telemetry_lock_ms: 0.0,
				framework_overhead_ms: 0.0,
				upstream: vec![],
			},
			TimingAtlasTaskNode {
				stage_id: 1,
				task_id: 0,
				wall_ms: 400.0,
				components: vec![],
				worker_id: String::new(),
				aggregate_accumulation_ms: 0.0,
				telemetry_lock_ms: 0.0,
				framework_overhead_ms: 0.0,
				upstream: vec![(0, 0)],
			},
			// Concurrent sibling: never on the selected path.
			TimingAtlasTaskNode {
				stage_id: 1,
				task_id: 1,
				wall_ms: 200.0,
				components: vec![],
				worker_id: String::new(),
				aggregate_accumulation_ms: 0.0,
				telemetry_lock_ms: 0.0,
				framework_overhead_ms: 0.0,
				upstream: vec![(0, 0)],
			},
		]
	}

	fn receipt(
		component: TimingAtlasComponent,
		arm: TimingArm,
		node: (u32, u32),
		elapsed: f64,
		eligible: bool,
	) -> TimingAtlasPathBoundReceipt {
		TimingAtlasPathBoundReceipt {
			component,
			arm,
			stage_id: node.0,
			task_id: node.1,
			scope: component.label().to_string(),
			basis: TimingAtlasBasis::CriticalPath,
			raw_elapsed_ms: elapsed,
			emitted_contribution_ms: None,
			eligible,
		}
	}

	#[test]
	fn a_well_formed_path_bound_set_validates_and_recomputes() {
		let receipts = vec![
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Local,
				(1, 0),
				30.0,
				true,
			),
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Distributed,
				(1, 0),
				80.0,
				true,
			),
		];
		let validation = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&receipts,
			500.0,
		);
		assert!(
			validation.violations.is_empty(),
			"{:?}",
			validation.violations
		);
		assert!(validation.dominance_allowed());
		assert_eq!(validation.contributions.len(), 1);
		assert_eq!(validation.contributions[0].contribution_ms, 50.0);
		assert_eq!(validation.selected_wall_ms, 500.0);
		assert_eq!(validation.remainder_ms, 420.0);
	}

	/// A receipt from the concurrent sibling is rejected however plausible its figure.
	#[test]
	fn eligible_receipts_from_off_path_tasks_are_rejected() {
		let receipts = vec![
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Local,
				(1, 1),
				5.0,
				true,
			),
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Distributed,
				(1, 1),
				190.0,
				true,
			),
		];
		let validation = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&receipts,
			500.0,
		);
		assert_eq!(
			validation.violations.len(),
			2,
			"both off-path receipts are rejected: {:?}",
			validation.violations
		);
		assert!(
			validation
				.violations
				.iter()
				.all(|v| v.contains("not on the selected path"))
		);
		assert!(!validation.dominance_allowed());
		assert!(validation.contributions.is_empty());
	}

	/// Off-path receipts may be retained as raw evidence, and stay permanently ineligible.
	#[test]
	fn off_path_receipts_are_retained_raw_and_never_eligible() {
		let receipts = vec![
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Local,
				(1, 1),
				5.0,
				false,
			),
			receipt(
				TimingAtlasComponent::Shuffle,
				TimingArm::Distributed,
				(1, 1),
				190.0,
				false,
			),
		];
		let validation = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&receipts,
			500.0,
		);
		assert!(validation.violations.is_empty());
		assert_eq!(validation.off_path_retained, 2);
		assert!(
			validation.contributions.is_empty(),
			"retained evidence never becomes a contribution"
		);
	}

	/// The emitted contribution is compared against a recomputation, not trusted.
	#[test]
	fn an_emitted_contribution_that_disagrees_is_rejected() {
		let mut distributed = receipt(
			TimingAtlasComponent::Operator,
			TimingArm::Distributed,
			(1, 0),
			80.0,
			true,
		);
		distributed.emitted_contribution_ms = Some(500.0);
		let receipts = vec![
			receipt(
				TimingAtlasComponent::Operator,
				TimingArm::Local,
				(1, 0),
				30.0,
				true,
			),
			distributed,
		];
		let validation = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&receipts,
			500.0,
		);
		assert!(
			validation
				.violations
				.iter()
				.any(|v| v.contains("disagrees with recomputed")),
			"{:?}",
			validation.violations
		);
	}

	/// A persisted path claiming an edge the graph does not have is rejected.
	#[test]
	fn an_invalid_persisted_edge_is_rejected() {
		let validation = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			// (1,1) is not upstream of (1,0).
			&[(1, 1), (1, 0)],
			&[],
			500.0,
		);
		assert!(
			validation
				.violations
				.iter()
				.any(|v| v.contains("not an edge in the stage graph")),
			"{:?}",
			validation.violations
		);
	}

	/// Scope, basis, uniqueness and pairing are all checked before dominance is allowed.
	#[test]
	fn scope_basis_uniqueness_and_pairing_gate_dominance() {
		let mut mismatched_scope = receipt(
			TimingAtlasComponent::Operator,
			TimingArm::Local,
			(1, 0),
			30.0,
			true,
		);
		mismatched_scope.scope = "operator::something_else".to_string();
		let validation = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&[
				mismatched_scope,
				receipt(
					TimingAtlasComponent::Operator,
					TimingArm::Distributed,
					(1, 0),
					80.0,
					true,
				),
			],
			500.0,
		);
		assert!(
			validation
				.violations
				.iter()
				.any(|v| v.contains("scope disagrees"))
		);

		let duplicated = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&[
				receipt(
					TimingAtlasComponent::Scan,
					TimingArm::Distributed,
					(1, 0),
					10.0,
					true,
				),
				receipt(
					TimingAtlasComponent::Scan,
					TimingArm::Distributed,
					(1, 0),
					20.0,
					true,
				),
			],
			500.0,
		);
		assert!(
			duplicated
				.violations
				.iter()
				.any(|v| v.contains("more than one eligible"))
		);

		let dispatch = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&[receipt(
				TimingAtlasComponent::Dispatch,
				TimingArm::Distributed,
				(1, 0),
				40.0,
				true,
			)],
			500.0,
		);
		assert!(
			dispatch
				.violations
				.iter()
				.any(|v| v.contains("no local equivalent")),
			"dispatch must never be eligible: {:?}",
			dispatch.violations
		);

		let inadmissible = timing_atlas_validate_path_bound_receipts(
			&nodes(),
			&[(0, 0), (1, 0)],
			&[TimingAtlasPathBoundReceipt {
				basis: TimingAtlasBasis::ConcurrentSpan,
				..receipt(
					TimingAtlasComponent::Scan,
					TimingArm::Distributed,
					(1, 0),
					10.0,
					true,
				)
			}],
			500.0,
		);
		assert!(
			inadmissible
				.violations
				.iter()
				.any(|v| v.contains("inadmissible basis"))
		);
	}
}

#[cfg(test)]
mod adr538_phase_component_wiring {
	use super::*;
	use toron::coordinator::QueryTaskTelemetryReport;
	use toron::distributed::identity::{AttemptId, StageId, TaskId, TaskIdentity};
	use toron::distributed::scheduler::TaskTelemetryRecord;

	/// Distinct primes per counter, so any counter mapped to the wrong component — or to
	/// two components — changes a sum and fails. The ten mapped counters and the three
	/// named remainder counters are the whole of `TaskTelemetryPhases`.
	#[test]
	fn every_phase_counter_is_mapped_once_or_named_as_remainder() {
		let phases = TaskTelemetryPhases {
			aggregate_accumulation_micros: 0,
			task_wall_micros: 104_729,
			shuffle_pull_decode_micros: 2,
			build_micros: 3,
			probe_micros: 5,
			spill_micros: 7,
			output_publish_micros: 11,
			task_setup_micros: 13,
			batch_encode_micros: 17,
			batch_send_micros: 19,
			batch_decode_micros: 23,
			dispatch_wait_micros: 29,
			telemetry_lock_micros: 31,
			exchange_repartition_micros: 37,
			scan_micros: 41,
			framework_overhead_micros: 43,
		};
		let portions = timing_atlas_component_portions_ms(&phases);
		let by_label: BTreeMap<&str, f64> = portions
			.iter()
			.map(|(component, portion)| (component.label(), *portion))
			.collect();
		assert_eq!(by_label["scan"], 41.0 / 1000.0);
		assert_eq!(by_label["operator"], (3.0 + 5.0 + 7.0) / 1000.0);
		assert_eq!(by_label["shuffle"], (37.0 + 2.0 + 11.0) / 1000.0);
		assert_eq!(by_label["dispatch"], (29.0 + 13.0) / 1000.0);
		assert_eq!(by_label["transport"], (17.0 + 19.0 + 23.0) / 1000.0);
		// Totality: mapped micros plus the named remainder counters account for every
		// counter except the wall itself.
		let mapped_micros: f64 = portions.iter().map(|(_, portion)| portion * 1000.0).sum();
		let remainder_micros =
			(phases.telemetry_lock_micros + phases.framework_overhead_micros) as f64;
		assert_eq!(
			mapped_micros + remainder_micros,
			(2 + 3 + 5 + 7 + 11 + 13 + 17 + 19 + 23 + 29 + 31 + 37 + 41 + 43) as f64,
			"a counter is either mapped to exactly one component or named as remainder"
		);
		assert_eq!(TIMING_ATLAS_UNATTRIBUTED_PHASES.len(), 3);
	}

	/// Presence at zero is what distinguishes an observed zero from a missing receipt, so
	/// every timed component appears even when its counters are all zero.
	#[test]
	fn every_timed_component_is_present_even_at_zero() {
		let portions = timing_atlas_component_portions_ms(&TaskTelemetryPhases::default());
		let labels: Vec<&str> = portions
			.iter()
			.map(|(component, _)| component.label())
			.collect();
		assert_eq!(
			labels,
			vec!["scan", "operator", "shuffle", "dispatch", "transport"]
		);
		assert!(portions.iter().all(|(_, portion)| *portion == 0.0));
	}

	fn record(stage: u32, task: u32, attempt: u32, wall_micros: u64) -> TaskTelemetryRecord {
		let mut telemetry = TaskTelemetry::default();
		telemetry.phases.task_wall_micros = wall_micros;
		telemetry.phase_receipt = TaskPhaseReceipt::Sealed;
		TaskTelemetryRecord {
			identity: TaskIdentity::new(
				QueryId::from_u128(538),
				StageId::new(stage),
				TaskId::new(task),
				AttemptId::new(attempt),
			),
			worker_id: Default::default(),
			telemetry,
		}
	}

	/// Stage-level upstream edges expand against the tasks the report actually contains,
	/// and a retried task contributes only its accepted attempt.
	#[test]
	fn nodes_expand_stage_edges_from_actual_tasks_and_last_attempt_wins() {
		let report = QueryTaskTelemetryReport {
			lifecycle: Default::default(),
			query_id: QueryId::from_u128(538),
			stage_upstreams: vec![
				(StageId::new(0), vec![]),
				(StageId::new(1), vec![StageId::new(0)]),
			],
			tasks: vec![
				record(0, 0, 0, 8_000),
				record(0, 1, 0, 6_000),
				// A retry: the first attempt's wall must not survive into the node.
				record(1, 0, 0, 1_000),
				record(1, 0, 1, 12_000),
			],
		};
		let nodes = timing_atlas_nodes_from_report(&report).expect("sealed receipts");
		assert_eq!(nodes.len(), 3, "one node per task, not per attempt");
		let consumer = nodes
			.iter()
			.find(|node| node.key() == (1, 0))
			.expect("consumer node");
		assert_eq!(consumer.wall_ms, 12.0, "the accepted attempt's wall");
		assert_eq!(
			consumer.upstream,
			vec![(0, 0), (0, 1)],
			"edges expand to every task of the upstream stage"
		);
		let path = timing_atlas_critical_path(&nodes).expect("path");
		assert_eq!(
			path.path,
			vec![(0, 0), (1, 0)],
			"selected through the slower producer"
		);
	}

	/// The local arm's evidence flows through the same machinery: a single node whose
	/// observed shuffle portion — including a genuine zero — survives path derivation.
	#[test]
	fn local_node_flows_through_the_same_critical_path_machinery() {
		let mut phases = TaskTelemetryPhases::default();
		phases.exchange_repartition_micros = 4_200;
		phases.scan_micros = 1_800;
		let node = timing_atlas_local_task_node(25.0, &phases, TaskPhaseReceipt::Sealed)
			.expect("sealed receipt");
		let attribution = timing_atlas_child_attribution(vec![node]).expect("local attributes");
		assert_eq!(attribution.path, vec![(0, 0)]);
		assert_eq!(attribution.path_wall_ms, 25.0);
		assert_eq!(attribution.path_components["shuffle"], 4.2);
		assert_eq!(attribution.path_components["scan"], 1.8);
		assert_eq!(attribution.path_components["transport"], 0.0);
	}

	/// A numeric zero is evidence only after ADR 551's ledger seals. Both arms
	/// reject an absent/vetoed receipt before component rows are constructed.
	#[test]
	fn timing_atlas_refuses_unsealed_phase_receipts_on_both_arms() {
		let local = timing_atlas_local_task_node(
			25.0,
			&TaskTelemetryPhases::default(),
			TaskPhaseReceipt::NotCollected,
		);
		assert!(
			local
				.expect_err("an uncollected local receipt is not zero")
				.contains("not sealed")
		);

		let report = QueryTaskTelemetryReport {
			lifecycle: Default::default(),
			query_id: QueryId::from_u128(538),
			stage_upstreams: vec![(StageId::new(0), vec![])],
			tasks: vec![TaskTelemetryRecord {
				identity: TaskIdentity::new(
					QueryId::from_u128(538),
					StageId::new(0),
					TaskId::new(0),
					AttemptId::new(0),
				),
				worker_id: Default::default(),
				telemetry: TaskTelemetry::default(),
			}],
		};
		assert!(
			timing_atlas_nodes_from_report(&report)
				.expect_err("an uncollected distributed receipt is not zero")
				.contains("not sealed")
		);
	}
}

#[cfg(test)]
mod adr538_attribution_replay_vetoes {
	use super::*;

	fn tampered_journal<F: FnMut(&mut serde_json::Value) -> bool>(
		kind: &str,
		mut tamper: F,
	) -> TimingAtlasReplay {
		let cells = timing_atlas_cells();
		let mut rows = super::adr538_timing_atlas_journal_replay::journal_lines(
			&cells,
			TIMING_ATLAS_MEASURED_PAIRS,
		);
		let mut tampered = false;
		for row in rows.iter_mut() {
			if row.get("kind").and_then(|k| k.as_str()) == Some(kind) && tamper(row) {
				tampered = true;
				break;
			}
		}
		assert!(tampered, "fixture must contain a {kind} row to tamper");
		let journal = rows
			.iter()
			.map(|row| row.to_string())
			.collect::<Vec<_>>()
			.join("\n");
		timing_atlas_replay(&journal)
	}

	/// A persisted path that disagrees with the path re-derived from its own nodes is
	/// rejected — the emitter cannot assert its own selection.
	#[test]
	fn a_path_row_lying_about_its_selection_is_rejected() {
		let replay = tampered_journal(TIMING_ATLAS_KIND_ATTRIBUTION_PATH, |row| {
			if row.get("arm").and_then(|a| a.as_str()) != Some("distributed") {
				return false;
			}
			row["path"] = serde_json::json!([[1, 0]]);
			true
		});
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("disagrees with the path re-derived")),
			"violations: {:?}",
			replay.violations
		);
	}

	/// A path component portion that disagrees with its nodes is rejected.
	#[test]
	fn a_path_row_lying_about_a_portion_is_rejected() {
		let replay = tampered_journal(TIMING_ATLAS_KIND_ATTRIBUTION_PATH, |row| {
			row["path_components"]["operator"] = serde_json::json!(400.0);
			true
		});
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("path component operator claims")),
			"violations: {:?}",
			replay.violations
		);
	}

	/// Attribution can never exceed the child's measured elapsed: eligible portions plus
	/// remainder must cover the measurement, so an inflated attribution is a defect.
	#[test]
	fn attribution_exceeding_measured_elapsed_is_rejected() {
		let replay = tampered_journal(TIMING_ATLAS_KIND_ATTRIBUTION_PATH, |row| {
			row["measured_elapsed_ms"] = serde_json::json!(1.0);
			true
		});
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("exceeds measured elapsed")),
			"violations: {:?}",
			replay.violations
		);
	}

	/// A component row whose contribution disagrees with the paths it summarizes is
	/// rejected — the emitted median is recomputed, never trusted.
	#[test]
	fn a_component_row_lying_about_its_contribution_is_rejected() {
		let replay = tampered_journal(TIMING_ATLAS_KIND_ATTRIBUTION, |row| {
			if row.get("component").and_then(|c| c.as_str()) != Some("shuffle") {
				return false;
			}
			row["contribution_ms"] = serde_json::json!(999.0);
			true
		});
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("shuffle contribution disagrees")),
			"violations: {:?}",
			replay.violations
		);
	}

	/// Component rows without a verified path behind every measured pair are rejected: a
	/// cell cannot claim receipts its journal does not back.
	#[test]
	fn component_rows_without_backing_paths_are_rejected() {
		let cells = timing_atlas_cells();
		let rows = super::adr538_timing_atlas_journal_replay::journal_lines(
			&cells,
			TIMING_ATLAS_MEASURED_PAIRS,
		)
		.into_iter()
		.filter(|row| {
			row.get("kind").and_then(|kind| kind.as_str())
				!= Some(TIMING_ATLAS_KIND_ATTRIBUTION_PATH)
		})
		.collect::<Vec<_>>();
		let journal = rows
			.iter()
			.map(|row| row.to_string())
			.collect::<Vec<_>>()
			.join("\n");
		let replay = timing_atlas_replay(&journal);
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("lacks a verified attribution path")),
			"violations: {:?}",
			replay.violations
		);
	}
}

#[cfg(test)]
mod adr538_attribution_end_to_end {
	use super::*;

	/// Run one full cell on the real harness at sf 0.01 and journal it exactly as the
	/// driver would, so the rows replay through the strict validator.
	/// Deep binds need an explicit stack; run on a dedicated 64 MB thread.
	fn run_cell(query: &'static str) -> TimingAtlasCellOutcome {
		std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(move || {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.worker_threads(2)
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let mut harness = CensusTimingAtlasHarness::prepare(
						query,
						"strict",
						0.01,
						Path::new("unused"),
					)
					.await
					.expect("prepare");
					run_timing_atlas_cell_with(&mut harness, query).await
				})
			})
			.expect("spawn")
			.join()
			.expect("join")
	}

	fn journal_for(outcome: &TimingAtlasCellOutcome) -> String {
		let mut rows = vec![
			timing_atlas_header_row(
				&HostQuiescence {
					service_inactive: true,
					sql_port_free: true,
				},
				"fnv1a64:test",
				0.01,
				"strict",
			),
			timing_atlas_exclusion_row("q15"),
			serde_json::json!({
				"kind": TIMING_ATLAS_KIND_CELL_SETUP, "query": outcome.query
			}),
		];
		for pair in &outcome.pairs {
			rows.push(timing_atlas_child_row(&pair.local));
			rows.push(timing_atlas_child_row(&pair.distributed));
			rows.push(timing_atlas_pair_row(&outcome.query, pair));
			for child in [&pair.local, &pair.distributed] {
				let attribution = child
					.attribution
					.as_ref()
					.expect("every valid measured child carries attribution");
				rows.push(timing_atlas_attribution_path_row(
					&outcome.query,
					child.arm,
					pair.pair_index,
					child.attempt,
					child.query_elapsed_ms,
					attribution,
				));
			}
		}
		rows.extend(timing_atlas_component_rows(&outcome.query, &outcome.pairs));
		rows.push(timing_atlas_cell_summary_row(outcome));
		rows.push(serde_json::json!({
			"kind": TIMING_ATLAS_KIND_HOST_END,
			"service_inactive": true,
			"sql_port_free": true,
		}));
		rows.push(serde_json::json!({ "kind": TIMING_ATLAS_SEAL_KIND }));
		rows.iter()
			.map(|row| row.to_string())
			.collect::<Vec<_>>()
			.join("\n")
	}

	fn shuffle_portion(child: &TimingAtlasChild) -> f64 {
		child
			.attribution
			.as_ref()
			.expect("attribution")
			.path_components
			.get("shuffle")
			.copied()
			.expect("shuffle portion is always present")
	}

	/// q03 binds an `Exchange` in its local plan, so shuffle must appear as a *paired*
	/// figure: real repartition time on the local arm, real shuffle time on the
	/// distributed arm — and the whole cell must replay through the strict validator.
	#[test]
	fn q03_produces_paired_shuffle_receipts_on_both_arms() {
		let outcome = run_cell("q03");
		assert!(outcome.failure.is_none(), "cell: {:?}", outcome.failure);
		assert_eq!(outcome.pairs.len(), TIMING_ATLAS_MEASURED_PAIRS);
		for pair in &outcome.pairs {
			assert!(
				shuffle_portion(&pair.local) > 0.0,
				"q03's local exchange repartitions in memory; its shuffle is a real \
				 local figure, not a declared zero"
			);
			assert!(
				shuffle_portion(&pair.distributed) > 0.0,
				"the distributed arm publishes and pulls shuffle partitions"
			);
		}
		let replay = timing_atlas_replay(&journal_for(&outcome));
		assert!(
			replay.violations.is_empty(),
			"violations: {:?}",
			replay.violations
		);
		assert!(replay.cells_with_attribution.contains("q03"));
	}

	/// q06 is the one eligible cell whose local plan has no exchange: its local shuffle
	/// arrives as an *observed* zero through the same instrument as every other figure,
	/// never as a declaration.
	#[test]
	fn q06_local_shuffle_is_an_observed_zero() {
		let outcome = run_cell("q06");
		assert!(outcome.failure.is_none(), "cell: {:?}", outcome.failure);
		assert_eq!(outcome.pairs.len(), TIMING_ATLAS_MEASURED_PAIRS);
		for pair in &outcome.pairs {
			assert_eq!(
				shuffle_portion(&pair.local),
				0.0,
				"q06's local plan has no exchange; the zero is observed, not declared"
			);
		}
		let journal = journal_for(&outcome);
		let replay = timing_atlas_replay(&journal);
		assert!(
			replay.violations.is_empty(),
			"violations: {:?}",
			replay.violations
		);
		// The dispatch rows the cell emitted stay raw: present, ineligible, null
		// contribution — on real receipts, not just fixtures.
		let dispatch_row = journal
			.lines()
			.filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
			.find(|row| {
				row.get("kind").and_then(|k| k.as_str()) == Some(TIMING_ATLAS_KIND_ATTRIBUTION)
					&& row.get("component").and_then(|c| c.as_str()) == Some("dispatch")
			})
			.expect("dispatch component row");
		assert_eq!(dispatch_row["eligible"], serde_json::json!(false));
		assert!(dispatch_row["contribution_ms"].is_null());
	}
}

#[cfg(test)]
mod adr538_phase_partition_probe {
	use super::*;

	/// Diagnostic probe pinning the run-4 finding: on which counters does a producer
	/// task's phase sum exceed its own wall? Prints raw counters so the overlap class is
	/// identified from evidence rather than hypothesis.
	#[test]
	#[ignore = "diagnostic probe; run explicitly"]
	fn dump_q01_distributed_raw_phases() {
		let report = std::thread::Builder::new()
			.stack_size(64 * 1024 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.worker_threads(2)
					.enable_all()
					.build()
					.expect("runtime");
				runtime.block_on(async {
					let mut harness = CensusTimingAtlasHarness::prepare(
						"q01",
						"strict",
						0.01,
						Path::new("unused"),
					)
					.await
					.expect("prepare");
					harness.setup_cell("q01").await.expect("setup");
					let plan = TimingAtlasChildPlan {
						arm: TimingArm::Distributed,
						role: TimingAtlasChildRole::Warmup,
						query_id: QueryId::new(),
					};
					harness.execute_child(&plan).await.expect("execute");
					let report = harness
						.telemetry_retention
						.lock()
						.expect("retention")
						.remove(&plan.query_id)
						.expect("report");
					harness.teardown_cell("q01").await.expect("teardown");
					report
				})
			})
			.expect("spawn")
			.join()
			.expect("join");
		for record in &report.tasks {
			let p = &record.telemetry.phases;
			let accounted = p.shuffle_pull_decode_micros
				+ p.build_micros
				+ p.probe_micros
				+ p.spill_micros
				+ p.output_publish_micros
				+ p.task_setup_micros
				+ p.batch_encode_micros
				+ p.batch_send_micros
				+ p.telemetry_lock_micros
				+ p.exchange_repartition_micros
				+ p.scan_micros;
			println!(
				"stage {} task {} attempt {}: wall={} accounted={} over={} | pull_decode={} build={} probe={} spill={} publish={} setup={} enc={} send={} dec={} dispatch_wait={} lock={} repart={} scan={} overhead={}",
				record.identity.stage_id.0,
				record.identity.task_id.0,
				record.identity.attempt_id.0,
				p.task_wall_micros,
				accounted,
				accounted.saturating_sub(p.task_wall_micros),
				p.shuffle_pull_decode_micros,
				p.build_micros,
				p.probe_micros,
				p.spill_micros,
				p.output_publish_micros,
				p.task_setup_micros,
				p.batch_encode_micros,
				p.batch_send_micros,
				p.batch_decode_micros,
				p.dispatch_wait_micros,
				p.telemetry_lock_micros,
				p.exchange_repartition_micros,
				p.scan_micros,
				p.framework_overhead_micros,
			);
		}
	}
}

#[cfg(test)]
mod adr538_node_partition_veto {
	use super::*;

	/// A node whose eligible portions exceed its own wall is rejected wherever it sits —
	/// run 4 carried 81 such nodes that the cell-level check caught only where the
	/// remainder was thin.
	#[test]
	fn a_node_whose_portions_exceed_its_wall_is_rejected() {
		let cells = timing_atlas_cells();
		let mut rows = super::adr538_timing_atlas_journal_replay::journal_lines(
			&cells,
			TIMING_ATLAS_MEASURED_PAIRS,
		);
		let mut tampered = false;
		for row in rows.iter_mut() {
			if row.get("kind").and_then(|k| k.as_str()) == Some(TIMING_ATLAS_KIND_ATTRIBUTION_PATH)
				&& !tampered
			{
				// Inflate one node's transport beyond its wall; the same wall segment
				// counted twice is exactly the defect class this check exists to catch.
				row["nodes"][0]["components"]["transport"] = serde_json::json!(500.0);
				tampered = true;
			}
		}
		assert!(tampered);
		let journal = rows
			.iter()
			.map(|row| row.to_string())
			.collect::<Vec<_>>()
			.join("\n");
		let replay = timing_atlas_replay(&journal);
		assert!(
			replay
				.violations
				.iter()
				.any(|violation| violation.contains("exceed its own wall")),
			"violations: {:?}",
			replay.violations
		);
		assert!(!replay.supports_authority(&TimingAuthorityConditions {
			clean_build: true,
			full_hash_dataset: true,
			quiescent_host: true,
			protocol_unmodified: true,
			complete_cohort: true,
		}));
	}
}

#[cfg(test)]
mod adr551_census_worker_shutdown {
	use super::*;
	use toron::error::QueryError;

	fn io(kind: std::io::ErrorKind) -> QueryError {
		QueryError::Io(std::io::Error::new(kind, "test"))
	}

	/// A peer that goes away when it is done with the worker is an ordinary close.
	///
	/// This is the q01 failure: seven pairs measured, cleanup succeeded, residency
	/// stable, `retained_bytes: 0` — and then `cohort_incomplete`, because a
	/// connection returned `BrokenPipe` and teardown could not tell that from a
	/// worker fault.
	#[test]
	fn coordinator_initiated_peer_closes_are_not_faults() {
		for kind in [
			std::io::ErrorKind::BrokenPipe,
			std::io::ErrorKind::ConnectionReset,
			std::io::ErrorKind::ConnectionAborted,
			std::io::ErrorKind::UnexpectedEof,
		] {
			assert!(
				is_expected_peer_close(&io(kind)),
				"{kind:?} at shutdown is an expected close"
			);
		}
	}

	/// The tolerance is narrow. A worker that fails for its own reasons still
	/// fails the cell, so this cannot be used to make teardown unconditionally
	/// forgiving — which would trade a false `cohort_incomplete` for a silently
	/// accepted broken worker.
	#[test]
	fn unexpected_worker_failures_still_fail() {
		for kind in [
			std::io::ErrorKind::PermissionDenied,
			std::io::ErrorKind::InvalidData,
			std::io::ErrorKind::OutOfMemory,
			std::io::ErrorKind::TimedOut,
		] {
			assert!(
				!is_expected_peer_close(&io(kind)),
				"{kind:?} is a genuine fault, not a shutdown close"
			);
		}

		assert!(
			!is_expected_peer_close(&QueryError::Remote {
				message: "worker aborted".to_string()
			}),
			"a non-IO worker error is never an expected close"
		);
	}
}

/// Where in a cell's life a peer close may be tolerated.
///
/// The measured answer is "anywhere". Instrumenting the moment each connection
/// ends showed q01 taking a `BrokenPipe` at the close of every distributed
/// attempt — seven of eight with teardown not yet begun — while all seven pairs
/// measured cleanly and no attempt was invalid.
#[cfg(test)]
mod adr551_census_peer_close_lifetime {
	use super::*;
	use std::sync::atomic::{AtomicUsize, Ordering};

	/// Drives the production classifier itself, not a restatement of it — a
	/// second copy of the rule could pass while the connection task disagreed.
	fn classify(kind: std::io::ErrorKind, peer_closes: &AtomicUsize) -> Result<(), String> {
		classify_census_connection_outcome(
			Err(toron::error::QueryError::Io(std::io::Error::new(
				kind,
				"peer closed",
			))),
			peer_closes,
		)
	}

	/// The discriminating control. Under the rejected design — tolerance gated on
	/// a `teardown_started` flag — this case is the one that fails, because the
	/// flag is false for the close that ends each measured attempt. It is the
	/// case that actually occurred, seven times per q01 cell.
	#[test]
	fn a_close_during_measurement_is_tolerated_and_counted() {
		let peer_closes = AtomicUsize::new(0);
		assert!(
			classify(std::io::ErrorKind::BrokenPipe, &peer_closes).is_ok(),
			"a close at the end of a measured attempt must not fail the cell"
		);
		assert_eq!(
			peer_closes.load(Ordering::Relaxed),
			1,
			"a tolerated close is counted, never silently absorbed"
		);
	}

	/// The same close during teardown. Both positions are tolerated, so the
	/// classification no longer depends on knowing which one it is — which is the
	/// point, because the worker cannot tell.
	#[test]
	fn a_close_at_teardown_is_tolerated_and_counted() {
		let peer_closes = AtomicUsize::new(0);
		assert!(classify(std::io::ErrorKind::BrokenPipe, &peer_closes).is_ok());
		assert_eq!(peer_closes.load(Ordering::Relaxed), 1);
	}

	/// The tolerance stays narrow: a worker failing for its own reasons fails the
	/// cell wherever it happens, and never inflates the peer-close count.
	#[test]
	fn a_genuine_fault_fails_the_cell_and_is_not_counted() {
		let peer_closes = AtomicUsize::new(0);
		assert!(
			classify(std::io::ErrorKind::OutOfMemory, &peer_closes).is_err(),
			"a genuine worker fault must still fail the cell"
		);
		assert_eq!(
			peer_closes.load(Ordering::Relaxed),
			0,
			"a fault is not a peer close"
		);
	}

	/// Zero is an observation. A connection that ends cleanly must leave the
	/// counter at zero, so a reported `0` means "none seen" rather than "never
	/// counted".
	#[test]
	fn a_clean_connection_leaves_the_count_at_zero() {
		let peer_closes = AtomicUsize::new(0);
		assert!(classify_census_connection_outcome(Ok(()), &peer_closes).is_ok());
		assert_eq!(
			peer_closes.load(Ordering::Relaxed),
			0,
			"a clean close is not a peer close"
		);
	}
}
