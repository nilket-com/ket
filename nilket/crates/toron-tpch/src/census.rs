//! ADR 604: the structural census's DuckDB side and its shared rules.
//!
//! D1 is one profiled DuckDB execution with operator metrics. Its JSON profile is a tree: each
//! node carries `operator_type`, `operator_name`, `operator_timing` (seconds, the node's own
//! work), `operator_cardinality`, a cumulative `cpu_time`, and `extra_info`; the root carries the
//! query's `latency`, `cpu_time`, and `rows_returned`. The census sums `operator_timing` per
//! Toron node kind and reports the signed residual `latency − sum(operator_timing)`, which is the
//! difference between two measures, not uncovered work. `cpu_time` is cumulative over the
//! operator timings, so it is never summed per node; only the root's is kept as a cross-check.

use std::collections::BTreeMap;

use serde::Serialize;

/// The profiler settings for D1: operator metrics plus the query's latency and CPU time.
pub const DUCKDB_OPERATOR_PROFILE_SETTINGS: &str = r#"{"OPERATOR_TIMING": "true", "OPERATOR_CARDINALITY": "true", "OPERATOR_TYPE": "true", "OPERATOR_NAME": "true", "CPU_TIME": "true", "LATENCY": "true", "ROWS_RETURNED": "true", "EXTRA_INFO": "true"}"#;

/// Toron's node kinds, as node attribution reports them.
pub const TORON_NODE_KINDS: [&str; 7] = [
	"scan",
	"filter",
	"join",
	"aggregate",
	"exchange",
	"sort",
	"other",
];

/// The declared mapping from DuckDB 1.5.4 operator types to Toron node kinds. A type not listed
/// is reported by name as unmapped, never folded into `other`.
pub const DUCKDB_KIND_MAPPING: [(&str, &str); 24] = [
	("TABLE_SCAN", "scan"),
	("READ_PARQUET", "scan"),
	("DELIM_SCAN", "scan"),
	("COLUMN_DATA_SCAN", "scan"),
	("FILTER", "filter"),
	("HASH_JOIN", "join"),
	("PIECEWISE_MERGE_JOIN", "join"),
	("NESTED_LOOP_JOIN", "join"),
	("BLOCKWISE_NL_JOIN", "join"),
	("RIGHT_DELIM_JOIN", "join"),
	("LEFT_DELIM_JOIN", "join"),
	("CROSS_PRODUCT", "join"),
	("IE_JOIN", "join"),
	("HASH_GROUP_BY", "aggregate"),
	("PERFECT_HASH_GROUP_BY", "aggregate"),
	("UNGROUPED_AGGREGATE", "aggregate"),
	("STREAMING_WINDOW", "aggregate"),
	("WINDOW", "aggregate"),
	("ORDER_BY", "sort"),
	("TOP_N", "sort"),
	("PROJECTION", "other"),
	("RESULT_COLLECTOR", "other"),
	("STREAMING_LIMIT", "other"),
	("LIMIT", "other"),
];

pub fn duckdb_operator_kind(operator_type: &str) -> Option<&'static str> {
	DUCKDB_KIND_MAPPING
		.iter()
		.find(|(name, _)| *name == operator_type)
		.map(|(_, kind)| *kind)
}

/// One operator of D1's executed tree, flattened in pre-order with its depth.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DuckDbOperator {
	pub depth: usize,
	pub operator_type: String,
	pub operator_name: String,
	pub kind: Option<&'static str>,
	pub operator_timing_seconds: f64,
	pub operator_cardinality: u64,
	pub extra_info: serde_json::Value,
}

/// D1's profile as the census reads it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DuckDbOperatorProfile {
	pub latency_seconds: f64,
	pub root_cpu_time_seconds: f64,
	pub rows_returned: u64,
	pub operators: Vec<DuckDbOperator>,
	/// `operator_timing` summed per Toron kind; unmapped types are kept apart, by name.
	pub timing_by_kind_seconds: BTreeMap<&'static str, f64>,
	pub unmapped_timing_seconds: BTreeMap<String, f64>,
	pub operator_timing_sum_seconds: f64,
	/// `latency − sum(operator_timing)`, signed: the difference between the two measures.
	pub latency_minus_operator_sum_seconds: f64,
}

pub fn parse_duckdb_operator_profile(
	profile: &serde_json::Value,
) -> Result<DuckDbOperatorProfile, String> {
	let number = |node: &serde_json::Value, name: &str| {
		node.get(name)
			.and_then(serde_json::Value::as_f64)
			.ok_or_else(|| format!("DuckDB profile node has no numeric {name}"))
	};
	let latency_seconds = number(profile, "latency")?;
	let root_cpu_time_seconds = number(profile, "cpu_time")?;
	let rows_returned = profile
		.get("rows_returned")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| "DuckDB profile has no rows_returned".to_string())?;
	let mut operators = Vec::new();
	let mut stack = profile
		.get("children")
		.and_then(serde_json::Value::as_array)
		.ok_or_else(|| "DuckDB profile has no operator tree".to_string())?
		.iter()
		.rev()
		.map(|child| (0usize, child))
		.collect::<Vec<_>>();
	while let Some((depth, node)) = stack.pop() {
		let operator_type = node
			.get("operator_type")
			.and_then(serde_json::Value::as_str)
			.ok_or_else(|| "DuckDB operator has no operator_type".to_string())?
			.to_string();
		operators.push(DuckDbOperator {
			depth,
			kind: duckdb_operator_kind(&operator_type),
			operator_name: node
				.get("operator_name")
				.and_then(serde_json::Value::as_str)
				.unwrap_or_default()
				.to_string(),
			operator_type,
			operator_timing_seconds: number(node, "operator_timing")?,
			operator_cardinality: node
				.get("operator_cardinality")
				.and_then(serde_json::Value::as_u64)
				.ok_or_else(|| "DuckDB operator has no operator_cardinality".to_string())?,
			extra_info: node.get("extra_info").cloned().unwrap_or_default(),
		});
		for child in node
			.get("children")
			.and_then(serde_json::Value::as_array)
			.into_iter()
			.flatten()
			.rev()
		{
			stack.push((depth + 1, child));
		}
	}
	if operators.is_empty() {
		return Err("DuckDB profile has no operators".to_string());
	}
	let mut timing_by_kind_seconds = BTreeMap::new();
	let mut unmapped_timing_seconds = BTreeMap::<String, f64>::new();
	for operator in &operators {
		match operator.kind {
			Some(kind) => {
				*timing_by_kind_seconds.entry(kind).or_insert(0.0) +=
					operator.operator_timing_seconds
			}
			None => {
				*unmapped_timing_seconds
					.entry(operator.operator_type.clone())
					.or_insert(0.0) += operator.operator_timing_seconds
			}
		}
	}
	let operator_timing_sum_seconds = operators
		.iter()
		.map(|operator| operator.operator_timing_seconds)
		.sum::<f64>();
	Ok(DuckDbOperatorProfile {
		latency_seconds,
		root_cpu_time_seconds,
		rows_returned,
		operators,
		timing_by_kind_seconds,
		unmapped_timing_seconds,
		operator_timing_sum_seconds,
		latency_minus_operator_sum_seconds: latency_seconds - operator_timing_sum_seconds,
	})
}

/// ADR 604's triage flag. Two nodes are compared only at the same logical boundary (the same set
/// of input relations and the same predicate or grouping); there, Toron carrying at least twice
/// DuckDB's rows raises the flag. Rows at unmatched nodes are non-comparable and raise nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryReading {
	Flagged,
	Comparable,
	NonComparable,
	/// DuckDB reports no rows where Toron reports some: no finite ratio, named apart from `2×`.
	DuckDbZeroToronPositive,
}

pub fn boundary_reading(
	same_logical_boundary: bool,
	toron_rows: u64,
	duckdb_rows: u64,
) -> BoundaryReading {
	if !same_logical_boundary {
		return BoundaryReading::NonComparable;
	}
	if duckdb_rows == 0 {
		return if toron_rows > 0 {
			BoundaryReading::DuckDbZeroToronPositive
		} else {
			BoundaryReading::Comparable
		};
	}
	if toron_rows >= duckdb_rows.saturating_mul(2) {
		BoundaryReading::Flagged
	} else {
		BoundaryReading::Comparable
	}
}

/// ADR 604: D0's plain `EXPLAIN (FORMAT JSON)` tree, read from DuckDB's output after its box
/// header, as the pre-order sequence of `(depth, operator name)`.
pub fn duckdb_explain_names(output: &str) -> Result<Vec<(usize, String)>, String> {
	let start = output
		.find('[')
		.ok_or_else(|| "DuckDB EXPLAIN output has no JSON plan".to_string())?;
	let mut stream =
		serde_json::Deserializer::from_str(&output[start..]).into_iter::<serde_json::Value>();
	let tree = stream
		.next()
		.ok_or_else(|| "DuckDB EXPLAIN output has no JSON plan".to_string())?
		.map_err(|error| format!("parse DuckDB EXPLAIN JSON: {error}"))?;
	let mut names = Vec::new();
	let mut stack = tree
		.as_array()
		.ok_or_else(|| "DuckDB EXPLAIN JSON is not a list of nodes".to_string())?
		.iter()
		.rev()
		.map(|node| (0usize, node))
		.collect::<Vec<_>>();
	while let Some((depth, node)) = stack.pop() {
		names.push((
			depth,
			node.get("name")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| "DuckDB EXPLAIN node has no name".to_string())?
				.to_string(),
		));
		for child in node
			.get("children")
			.and_then(serde_json::Value::as_array)
			.into_iter()
			.flatten()
			.rev()
		{
			stack.push((depth + 1, child));
		}
	}
	Ok(names)
}

/// D0 against D1, compared by `(depth, operator name)` in pre-order. A match is a shape-and-name
/// match, weaker than exact plan correspondence: sources and predicates are not compared. D1 takes
/// precedence either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DuckDbPlanCorrespondence {
	ShapeNameMatch,
	Unmatched,
	Unreadable,
}

pub fn duckdb_plan_correspondence(
	explain: &Result<Vec<(usize, String)>, String>,
	executed: &DuckDbOperatorProfile,
) -> DuckDbPlanCorrespondence {
	let Ok(explain) = explain else {
		return DuckDbPlanCorrespondence::Unreadable;
	};
	let same = explain.len() == executed.operators.len()
		&& explain
			.iter()
			.zip(&executed.operators)
			.all(|((depth, name), operator)| {
				*depth == operator.depth && *name == operator.operator_name
			});
	if same {
		DuckDbPlanCorrespondence::ShapeNameMatch
	} else {
		DuckDbPlanCorrespondence::Unmatched
	}
}

/// Toron's plan correspondence: the explain binder's fingerprint must equal T2's and the sealed
/// anchor's (one fingerprint across its timed rows). Agreement is indirect, since T1 returns no
/// plan of its own; anything else leaves the cell's join comparison unresolved.
pub fn toron_plan_correspondence(
	binder: Option<&str>,
	t2: Option<&str>,
	anchor: &std::collections::BTreeSet<String>,
) -> &'static str {
	match (binder, t2) {
		(Some(binder), Some(t2))
			if binder == t2 && anchor.len() == 1 && anchor.contains(binder) =>
		{
			"indirect_agreement"
		}
		_ => "unresolved",
	}
}

/// One join of Toron's plan, as the explain binder's join-order edges describe it, with the
/// estimate where the binder has one and T1's coverage named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToronJoin {
	pub index: u64,
	pub kind: String,
	pub probe: Option<String>,
	pub dim: Option<String>,
	/// `Some` where the binder estimated the join's rows; `estimate_unknown` otherwise.
	pub estimated_rows: Option<u64>,
	pub estimate: &'static str,
	/// T1's actual rows are attached only through a stable join identity. This census has none
	/// that ties an analyze row to an edge, so actuals are never attached by position: a join is
	/// `not_emitted` when T1 emitted nothing, and `unassigned` when T1 emitted rows it cannot
	/// attribute.
	pub actual: &'static str,
	pub raw: String,
}

fn quoted_field(edge: &str, name: &str) -> Option<String> {
	let rest = edge.split(&format!("{name}=")).nth(1)?;
	let value = rest.split_whitespace().next()?;
	value
		.strip_prefix("Some(\"")
		.and_then(|value| value.strip_suffix("\")"))
		.map(str::to_string)
}

fn rows_field(edge: &str) -> Option<u64> {
	let rest = edge.split("rows=").nth(1)?;
	let value = rest.split_whitespace().next()?;
	value
		.strip_prefix("Some(")
		.and_then(|value| value.strip_suffix(')'))
		.and_then(|value| value.parse().ok())
}

/// The binder's join-order edges (`#N Kind probe=… dim=… … rows=… …`, joined by ` | `) as a
/// per-join table. An edge that does not parse keeps its raw text with its fields empty.
pub fn toron_join_table(join_order_edges: &str, t1_rows_emitted: usize) -> Vec<ToronJoin> {
	join_order_edges
		.split(" | ")
		.map(str::trim)
		.filter(|edge| !edge.is_empty())
		.enumerate()
		.map(|(position, edge)| {
			let mut tokens = edge.split_whitespace();
			let index = tokens
				.next()
				.and_then(|token| token.strip_prefix('#'))
				.and_then(|token| token.parse().ok())
				.unwrap_or(position as u64);
			let kind = tokens.next().unwrap_or_default().to_string();
			let estimated_rows = rows_field(edge);
			ToronJoin {
				index,
				kind,
				probe: quoted_field(edge, "probe"),
				dim: quoted_field(edge, "dim"),
				estimate: if estimated_rows.is_some() {
					"estimated"
				} else {
					"estimate_unknown"
				},
				estimated_rows,
				actual: if t1_rows_emitted == 0 {
					"not_emitted"
				} else {
					"unassigned"
				},
				raw: edge.to_string(),
			}
		})
		.collect()
}

/// ADR 604: why a cell stays out of the structural reading. A cell enters only when T2 passed
/// with its cost and volume evidence present (node attribution whose outer reconciliation is
/// within its boundary, and the parquet scan receipt), and when both engines' diagnostic results
/// and D1's memory read-back match the sealed anchor's timed rows.
pub fn census_exclusions(
	t2: &serde_json::Value,
	t2_digest_matches: bool,
	d1_digest_matches: bool,
	d1_memory_matches: bool,
) -> Vec<&'static str> {
	let mut excluded = Vec::new();
	if t2.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
		excluded.push("t2_did_not_pass");
	}
	if !t2
		.get("node_attribution")
		.is_some_and(serde_json::Value::is_object)
	{
		excluded.push("t2_node_attribution_missing");
	}
	if t2
		.get("node_attribution_outer_reconciliation")
		.and_then(|reconciliation| reconciliation.get("within_boundary"))
		.and_then(serde_json::Value::as_bool)
		!= Some(true)
	{
		excluded.push("t2_attribution_reconciliation_not_accepted");
	}
	if !t2
		.get("parquet_scan_receipt")
		.is_some_and(serde_json::Value::is_object)
	{
		excluded.push("t2_parquet_scan_receipt_missing");
	}
	if !t2_digest_matches {
		excluded.push("t2_digest_differs_from_anchor_toron");
	}
	if !d1_digest_matches {
		excluded.push("d1_digest_differs_from_anchor_duckdb");
	}
	if !d1_memory_matches {
		excluded.push("d1_memory_limit_read_back_differs_from_anchor");
	}
	excluded
}

/// ADR 604's selected coordinate: the sealed anchor header's fields a census run must share. It
/// is a selection, not every header field (identity and quiescence fields describe the board's
/// own run).
pub const CENSUS_COORDINATE_FIELDS: [&str; 18] = [
	"scale_factor",
	"mode",
	"data_source",
	"dataset_manifest_fingerprint",
	"memory_per_task",
	"memory_per_task_provenance",
	"spill_bytes_reserved",
	"spill_bytes_reserved_provenance",
	"partition_work_concurrency_requested",
	"partition_work_concurrency_requested_provenance",
	"duckdb_threads",
	"duckdb_threads_provenance",
	"duckdb_memory_limit",
	"duckdb_memory_limit_provenance",
	"duckdb_executable_sha256",
	"cpu_affinity_requested",
	"cpu_affinity_observed",
	"dataset_verification",
];

/// The fields on which a census run's coordinate differs from the sealed header, by name. A field
/// missing on either side is a difference.
pub fn coordinate_mismatches(
	census: &serde_json::Value,
	sealed: &serde_json::Value,
) -> Vec<String> {
	CENSUS_COORDINATE_FIELDS
		.iter()
		.filter(|field| match (census.get(**field), sealed.get(**field)) {
			(Some(left), Some(right)) => left != right,
			_ => true,
		})
		.map(|field| (*field).to_string())
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The shape DuckDB 1.5.4 produced on bigfoot for a small join, group, and sort.
	fn recorded_profile() -> serde_json::Value {
		serde_json::json!({
			"latency": 0.0012, "cpu_time": 0.000816472, "rows_returned": 7,
			"extra_info": {},
			"children": [{
				"operator_type": "PROJECTION", "operator_name": "PROJECTION",
				"operator_timing": 1.88e-07, "operator_cardinality": 7, "cpu_time": 0.000816472,
				"extra_info": {},
				"children": [{
					"operator_type": "ORDER_BY", "operator_name": "ORDER_BY",
					"operator_timing": 2.9137e-05, "operator_cardinality": 7, "cpu_time": 0.000816284,
					"extra_info": {},
					"children": [{
						"operator_type": "PERFECT_HASH_GROUP_BY", "operator_name": "PERFECT_HASH_GROUP_BY",
						"operator_timing": 2.6214e-05, "operator_cardinality": 7, "cpu_time": 0.000785942,
						"extra_info": {},
						"children": [{
							"operator_type": "HASH_JOIN", "operator_name": "HASH_JOIN",
							"operator_timing": 0.000454545, "operator_cardinality": 16667,
							"cpu_time": 0.000712, "extra_info": {"Join Type": "INNER"},
							"children": [
								{"operator_type": "TABLE_SCAN", "operator_name": "SEQ_SCAN",
								 "operator_timing": 0.000174514, "operator_cardinality": 49999,
								 "cpu_time": 0.000174514, "extra_info": {"Table": "a"}, "children": []},
								{"operator_type": "TABLE_SCAN", "operator_name": "SEQ_SCAN",
								 "operator_timing": 8.2941e-05, "operator_cardinality": 16667,
								 "cpu_time": 8.2941e-05, "extra_info": {"Table": "b"}, "children": []}
							]
						}]
					}]
				}]
			}]
		})
	}

	#[test]
	fn the_operator_tree_flattens_and_sums_by_kind_against_its_own_latency() {
		let profile = parse_duckdb_operator_profile(&recorded_profile()).unwrap();
		assert_eq!(profile.rows_returned, 7);
		assert_eq!(
			profile
				.operators
				.iter()
				.map(|operator| (operator.depth, operator.operator_type.as_str()))
				.collect::<Vec<_>>(),
			vec![
				(0, "PROJECTION"),
				(1, "ORDER_BY"),
				(2, "PERFECT_HASH_GROUP_BY"),
				(3, "HASH_JOIN"),
				(4, "TABLE_SCAN"),
				(4, "TABLE_SCAN"),
			]
		);
		let close = |a: f64, b: f64| (a - b).abs() < 1e-12;
		assert!(close(
			profile.timing_by_kind_seconds["scan"],
			0.000174514 + 8.2941e-05
		));
		assert!(close(profile.timing_by_kind_seconds["join"], 0.000454545));
		assert!(close(
			profile.timing_by_kind_seconds["aggregate"],
			2.6214e-05
		));
		assert!(close(profile.timing_by_kind_seconds["sort"], 2.9137e-05));
		assert!(close(profile.timing_by_kind_seconds["other"], 1.88e-07));
		assert!(profile.unmapped_timing_seconds.is_empty());
		let sum = 1.88e-07 + 2.9137e-05 + 2.6214e-05 + 0.000454545 + 0.000174514 + 8.2941e-05;
		assert!(close(profile.operator_timing_sum_seconds, sum));
		assert!(close(
			profile.latency_minus_operator_sum_seconds,
			0.0012 - sum
		));
		// The root's cumulative CPU time is kept as it is; per-node CPU time is never summed.
		assert!(close(profile.root_cpu_time_seconds, 0.000816472));
	}

	#[test]
	fn an_unmapped_operator_is_named_and_not_folded_into_other() {
		let mut raw = recorded_profile();
		raw["children"][0]["operator_type"] = serde_json::json!("MYSTERY_OPERATOR");
		let profile = parse_duckdb_operator_profile(&raw).unwrap();
		assert!((profile.unmapped_timing_seconds["MYSTERY_OPERATOR"] - 1.88e-07).abs() < 1e-15);
		assert!(!profile.timing_by_kind_seconds.contains_key("other"));
		assert_eq!(profile.operators[0].kind, None);
	}

	#[test]
	fn every_mapped_type_maps_to_a_toron_kind() {
		for (operator_type, kind) in DUCKDB_KIND_MAPPING {
			assert_eq!(duckdb_operator_kind(operator_type), Some(kind));
			assert!(TORON_NODE_KINDS.contains(&kind), "{operator_type} → {kind}");
		}
		assert_eq!(duckdb_operator_kind("PROJECTIONS"), None);
	}

	#[test]
	fn a_malformed_profile_is_refused() {
		for broken in [
			serde_json::json!({"cpu_time": 1.0, "rows_returned": 1, "children": []}),
			serde_json::json!({"latency": 1.0, "cpu_time": 1.0, "rows_returned": 1}),
			serde_json::json!({"latency": 1.0, "cpu_time": 1.0, "rows_returned": 1,
				"children": [{"operator_name": "X", "operator_timing": 0.1,
					"operator_cardinality": 1, "children": []}]}),
		] {
			assert!(parse_duckdb_operator_profile(&broken).is_err(), "{broken}");
		}
	}

	#[test]
	fn the_triage_flag_stands_only_at_a_matched_boundary() {
		assert_eq!(
			boundary_reading(true, 2_000, 1_000),
			BoundaryReading::Flagged
		);
		assert_eq!(
			boundary_reading(true, 1_999, 1_000),
			BoundaryReading::Comparable
		);
		assert_eq!(
			boundary_reading(false, 9_000_000, 1),
			BoundaryReading::NonComparable
		);
		assert_eq!(boundary_reading(true, 0, 0), BoundaryReading::Comparable);
		assert_eq!(
			boundary_reading(true, 5, 0),
			BoundaryReading::DuckDbZeroToronPositive,
			"no finite ratio, and not a 2x flag"
		);
	}

	#[test]
	fn the_described_plan_is_read_after_the_box_header_and_matched_to_the_executed_tree() {
		let output = "┌──────┐\n│ Physical Plan │\n└──────┘\n[\n  {\"name\": \"PROJECTION\", \"children\": [\n    {\"name\": \"ORDER_BY\", \"children\": [\n      {\"name\": \"PERFECT_HASH_GROUP_BY\", \"children\": [\n        {\"name\": \"HASH_JOIN\", \"children\": [\n          {\"name\": \"SEQ_SCAN\", \"children\": []},\n          {\"name\": \"SEQ_SCAN\", \"children\": []}]}]}]}]}\n]\n";
		let names = duckdb_explain_names(output).unwrap();
		assert_eq!(
			names,
			[
				(0, "PROJECTION".to_string()),
				(1, "ORDER_BY".to_string()),
				(2, "PERFECT_HASH_GROUP_BY".to_string()),
				(3, "HASH_JOIN".to_string()),
				(4, "SEQ_SCAN".to_string()),
				(4, "SEQ_SCAN".to_string()),
			]
		);
		let executed = parse_duckdb_operator_profile(&recorded_profile()).unwrap();
		assert_eq!(
			duckdb_plan_correspondence(&Ok(names.clone()), &executed),
			DuckDbPlanCorrespondence::ShapeNameMatch
		);
		// The same names in the same order, nested differently, do not match.
		let mut renested = names.clone();
		renested[5].0 = 5;
		assert_eq!(
			duckdb_plan_correspondence(&Ok(renested), &executed),
			DuckDbPlanCorrespondence::Unmatched
		);
		let mut other = names.clone();
		other.swap(4, 3);
		assert_eq!(
			duckdb_plan_correspondence(&Ok(other), &executed),
			DuckDbPlanCorrespondence::Unmatched
		);
		assert_eq!(
			duckdb_plan_correspondence(&Ok(names[..5].to_vec()), &executed),
			DuckDbPlanCorrespondence::Unmatched
		);
		let unreadable = duckdb_explain_names("no plan here");
		assert!(unreadable.is_err());
		assert_eq!(
			duckdb_plan_correspondence(&unreadable, &executed),
			DuckDbPlanCorrespondence::Unreadable
		);
	}

	/// The edges the binder printed for q05 at SF 0.01, plus one estimated edge.
	#[test]
	fn a_non_star_join_list_names_missing_estimates_and_uncovered_actuals() {
		let edges = "#0 Inner probe=Some(\"customer\") dim=Some(\"orders\") left_keys=[\"customer.c_custkey\"] right_keys=[\"orders.o_custkey\"] rows=None linearity=Linear | #1 Inner probe=Some(\"orders\") dim=Some(\"lineitem\") left_keys=[\"orders.o_orderkey\"] right_keys=[\"lineitem.l_orderkey\"] rows=Some(6001215) linearity=Linear";
		let joins = toron_join_table(edges, 0);
		assert_eq!(joins.len(), 2);
		assert_eq!(joins[0].index, 0);
		assert_eq!(joins[0].kind, "Inner");
		assert_eq!(joins[0].probe.as_deref(), Some("customer"));
		assert_eq!(joins[0].dim.as_deref(), Some("orders"));
		assert_eq!(joins[0].estimated_rows, None);
		assert_eq!(joins[0].estimate, "estimate_unknown");
		assert_eq!(joins[1].estimated_rows, Some(6_001_215));
		assert_eq!(joins[1].estimate, "estimated");
		assert!(joins.iter().all(|join| join.actual == "not_emitted"));
		// T1 rows that no join identity ties to an edge are unassigned, never taken by position.
		assert!(
			toron_join_table(edges, 3)
				.iter()
				.all(|join| join.actual == "unassigned")
		);
		assert!(toron_join_table("", 0).is_empty());
	}

	#[test]
	fn toron_correspondence_needs_the_binder_t2_and_the_anchor_to_agree() {
		let anchor = |values: &[&str]| {
			values
				.iter()
				.map(|value| value.to_string())
				.collect::<std::collections::BTreeSet<_>>()
		};
		assert_eq!(
			toron_plan_correspondence(Some("a"), Some("a"), &anchor(&["a"])),
			"indirect_agreement"
		);
		assert_eq!(
			toron_plan_correspondence(Some("a"), Some("b"), &anchor(&["a"])),
			"unresolved",
			"a fingerprint mismatch leaves the join comparison unresolved"
		);
		assert_eq!(
			toron_plan_correspondence(Some("a"), Some("a"), &anchor(&["b"])),
			"unresolved"
		);
		assert_eq!(
			toron_plan_correspondence(Some("a"), Some("a"), &anchor(&["a", "b"])),
			"unresolved",
			"the anchor's rows disagree among themselves"
		);
		assert_eq!(
			toron_plan_correspondence(None, Some("a"), &anchor(&["a"])),
			"unresolved"
		);
	}

	#[test]
	fn the_coordinate_check_names_every_differing_or_missing_field() {
		let sealed = serde_json::json!({
			"scale_factor": 8.0, "mode": "strict", "data_source": "parquet",
			"dataset_manifest_fingerprint": "fnv1a64:0fc5c06d19c5ed23",
			"memory_per_task": 2147483648u64, "memory_per_task_provenance": "explicit",
			"spill_bytes_reserved": 2147483648u64, "spill_bytes_reserved_provenance": "explicit",
			"partition_work_concurrency_requested": 1,
			"partition_work_concurrency_requested_provenance": "explicit",
			"duckdb_threads": "1", "duckdb_threads_provenance": "explicit",
			"duckdb_memory_limit": null, "duckdb_memory_limit_provenance": "default",
			"duckdb_executable_sha256": "c478", "cpu_affinity_requested": "0-15",
			"cpu_affinity_observed": "0-15", "dataset_verification": "full_hash",
		});
		assert!(coordinate_mismatches(&sealed, &sealed).is_empty());
		let mut census = sealed.clone();
		census["memory_per_task"] = serde_json::json!(34359738368u64);
		census
			.as_object_mut()
			.unwrap()
			.remove("cpu_affinity_requested");
		assert_eq!(
			coordinate_mismatches(&census, &sealed),
			["memory_per_task", "cpu_affinity_requested"]
		);
	}

	#[test]
	fn a_cell_without_its_cost_or_volume_evidence_stays_out_of_the_reading() {
		let complete = serde_json::json!({
			"status": "pass",
			"node_attribution": {"nodes": []},
			"node_attribution_outer_reconciliation": {"within_boundary": true},
			"parquet_scan_receipt": {"rows": 1},
		});
		assert!(census_exclusions(&complete, true, true, true).is_empty());
		// The perf child tolerates an absent profile file; the census does not.
		let mut missing = complete.clone();
		let object = missing.as_object_mut().unwrap();
		object.remove("node_attribution");
		object.remove("parquet_scan_receipt");
		assert_eq!(
			census_exclusions(&missing, true, true, true),
			[
				"t2_node_attribution_missing",
				"t2_parquet_scan_receipt_missing"
			]
		);
		let mut unreconciled = complete.clone();
		unreconciled["node_attribution_outer_reconciliation"]["within_boundary"] =
			serde_json::json!(false);
		assert_eq!(
			census_exclusions(&unreconciled, true, true, true),
			["t2_attribution_reconciliation_not_accepted"]
		);
		let mut failed = complete.clone();
		failed["status"] = serde_json::json!("fail");
		assert_eq!(
			census_exclusions(&failed, false, false, false),
			[
				"t2_did_not_pass",
				"t2_digest_differs_from_anchor_toron",
				"d1_digest_differs_from_anchor_duckdb",
				"d1_memory_limit_read_back_differs_from_anchor",
			]
		);
	}

	#[test]
	fn a_profile_with_no_operators_is_refused() {
		let empty = serde_json::json!({"latency": 1.0, "cpu_time": 1.0, "rows_returned": 0,
			"children": []});
		assert!(parse_duckdb_operator_profile(&empty).is_err());
	}
}
