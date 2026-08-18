use crate::error::ExecutionError;
use crate::ir::{BoundExprId, BoundExprNode, BoundExprProgram, JoinReplayId};
use arrow::datatypes::SchemaRef;
use serde::Serialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub(in crate::exec) const GLOBAL_FOLD_JOIN_FILTER_PROFILE_ENV: &str =
	"TORON_GLOBAL_FOLD_JOIN_FILTER_PROFILE_DIR";
const PROFILE_SCHEMA_VERSION: u32 = 1;
const LEAF_COUNT: usize = 8;
static NEXT_PROFILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::exec) enum GlobalFoldProfileLeaf {
	ProbeLookupSelect = 0,
	ArrowMaterialize = 1,
	PredicateEvalFilter = 2,
	DecimalExpr = 3,
	NumericFold = 4,
	ScanPollDecode = 5,
	SpillPageRead = 6,
	BuildSpillWrite = 7,
}

impl GlobalFoldProfileLeaf {
	const ALL: [Self; LEAF_COUNT] = [
		Self::ProbeLookupSelect,
		Self::ArrowMaterialize,
		Self::PredicateEvalFilter,
		Self::DecimalExpr,
		Self::NumericFold,
		Self::ScanPollDecode,
		Self::SpillPageRead,
		Self::BuildSpillWrite,
	];

	fn index(self) -> usize {
		self as usize
	}

	fn name(self) -> &'static str {
		match self {
			Self::ProbeLookupSelect => "probe_lookup_select",
			Self::ArrowMaterialize => "arrow_materialize",
			Self::PredicateEvalFilter => "predicate_eval_filter",
			Self::DecimalExpr => "decimal_expr",
			Self::NumericFold => "numeric_fold",
			Self::ScanPollDecode => "scan_poll_decode",
			Self::SpillPageRead => "spill_page_read",
			Self::BuildSpillWrite => "build_spill_write",
		}
	}
}

#[derive(Debug, Serialize)]
struct ExprNodeCensus {
	node_index: u64,
	node_kind: &'static str,
	reference_multiplicity: u64,
	node_debug: String,
}

#[derive(Debug, Serialize)]
struct LeafSnapshot {
	name: &'static str,
	micros: u64,
	calls: u64,
}

#[derive(Debug, Serialize)]
struct GlobalFoldJoinFilterSnapshot {
	schema_version: u32,
	profile_id: u64,
	path: String,
	replay_id: u32,
	probe_columns_first: bool,
	build_fields: Vec<String>,
	probe_fields: Vec<String>,
	root_span_micros: u64,
	leaves: Vec<LeafSnapshot>,
	domain_1_micros: u64,
	domain_2_micros: u64,
	domain_3_micros: u64,
	domain_4_micros: u64,
	in_root_residual_micros: u64,
	in_root_residual_ratio: f64,
	in_root_residual_within_ceiling: bool,
	predicate_input_rows: u64,
	predicate_output_rows: u64,
	sum_argument_rows: u64,
	numeric_fold_rows: u64,
	physical_expression_nodes: u64,
	expression_nodes: Vec<ExprNodeCensus>,
}

#[derive(Debug, Default)]
struct ProfileState {
	active_leaf: Option<GlobalFoldProfileLeaf>,
	root_started: bool,
	root_span_micros: Option<u64>,
	leaf_micros: [u64; LEAF_COUNT],
	leaf_calls: [u64; LEAF_COUNT],
	path: Option<String>,
	replay_id: Option<u32>,
	probe_columns_first: Option<bool>,
	build_fields: Vec<String>,
	probe_fields: Vec<String>,
	predicate_input_rows: u64,
	predicate_output_rows: u64,
	sum_argument_rows: u64,
	numeric_fold_rows: u64,
}

#[derive(Debug)]
pub(in crate::exec) struct GlobalFoldJoinFilterProfile {
	directory: PathBuf,
	profile_id: u64,
	expression_nodes: Vec<ExprNodeCensus>,
	state: Mutex<ProfileState>,
}

pub(in crate::exec) struct GlobalFoldProfileTimer<'a> {
	profile: &'a GlobalFoldJoinFilterProfile,
	leaf: GlobalFoldProfileLeaf,
	started: Instant,
	finished: bool,
}

impl GlobalFoldJoinFilterProfile {
	pub(in crate::exec) fn from_env(
		predicate: &BoundExprProgram,
	) -> Result<Option<Box<Self>>, ExecutionError> {
		Self::from_lookup(predicate, |name| std::env::var_os(name))
			.map(|profile| profile.map(Box::new))
	}

	fn from_lookup(
		predicate: &BoundExprProgram,
		lookup: impl FnOnce(&str) -> Option<OsString>,
	) -> Result<Option<Self>, ExecutionError> {
		let Some(raw) = lookup(GLOBAL_FOLD_JOIN_FILTER_PROFILE_ENV) else {
			return Ok(None);
		};
		if raw.is_empty() {
			return Ok(None);
		}
		Ok(Some(Self {
			directory: PathBuf::from(raw),
			profile_id: NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed),
			expression_nodes: expression_census(predicate)?,
			state: Mutex::new(ProfileState::default()),
		}))
	}

	pub(in crate::exec) fn begin_root(&self) -> Result<Instant, ExecutionError> {
		let mut state = self.lock_state()?;
		if state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error("root span started more than once"));
		}
		state.root_started = true;
		Ok(Instant::now())
	}

	pub(in crate::exec) fn finish_root(&self, started: Instant) -> Result<(), ExecutionError> {
		let elapsed = checked_duration_micros(started.elapsed())?;
		let mut state = self.lock_state()?;
		if !state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error("root span finished without one active root"));
		}
		if state.active_leaf.is_some() {
			return Err(profile_error(
				"root span finished while an exclusive leaf was active",
			));
		}
		state.root_span_micros = Some(elapsed);
		Ok(())
	}

	pub(in crate::exec) fn begin_leaf(
		&self,
		leaf: GlobalFoldProfileLeaf,
	) -> Result<GlobalFoldProfileTimer<'_>, ExecutionError> {
		let mut state = self.lock_state()?;
		if !state.root_started || state.root_span_micros.is_some() {
			return Err(profile_error(format!(
				"exclusive leaf {} started outside the root span",
				leaf.name()
			)));
		}
		if let Some(active) = state.active_leaf {
			return Err(profile_error(format!(
				"exclusive leaf {} overlapped active leaf {}",
				leaf.name(),
				active.name()
			)));
		}
		state.active_leaf = Some(leaf);
		drop(state);
		Ok(GlobalFoldProfileTimer {
			profile: self,
			leaf,
			started: Instant::now(),
			finished: false,
		})
	}

	pub(in crate::exec) fn record_dispatch(
		&self,
		path: &'static str,
		replay_id: JoinReplayId,
		probe_columns_first: bool,
		build_schema: &SchemaRef,
		probe_schema: &SchemaRef,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		if state.path.is_some() {
			return Err(profile_error("live dispatch recorded more than once"));
		}
		state.path = Some(path.to_string());
		state.replay_id = Some(replay_id.0);
		state.probe_columns_first = Some(probe_columns_first);
		state.build_fields = build_schema
			.fields()
			.iter()
			.map(|field| field.name().to_string())
			.collect();
		state.probe_fields = probe_schema
			.fields()
			.iter()
			.map(|field| field.name().to_string())
			.collect();
		Ok(())
	}

	pub(in crate::exec) fn record_predicate_rows(
		&self,
		input_rows: usize,
		output_rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add_usize(
			&mut state.predicate_input_rows,
			input_rows,
			"predicate input rows",
		)?;
		checked_add_usize(
			&mut state.predicate_output_rows,
			output_rows,
			"predicate output rows",
		)
	}

	pub(in crate::exec) fn record_sum_argument_rows(
		&self,
		rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add_usize(&mut state.sum_argument_rows, rows, "SUM argument rows")
	}

	pub(in crate::exec) fn record_numeric_fold_rows(
		&self,
		rows: usize,
	) -> Result<(), ExecutionError> {
		let mut state = self.lock_state()?;
		checked_add_usize(&mut state.numeric_fold_rows, rows, "numeric fold rows")
	}

	pub(in crate::exec) fn persist(&self) -> Result<(), ExecutionError> {
		let state = self.lock_state()?;
		if state.active_leaf.is_some() {
			return Err(profile_error(
				"cannot persist while an exclusive leaf is active",
			));
		}
		let root_span_micros = state
			.root_span_micros
			.ok_or_else(|| profile_error("cannot persist before the root span finishes"))?;
		let path = state
			.path
			.clone()
			.ok_or_else(|| profile_error("profiled fused path did not record a live dispatch"))?;
		let replay_id = state
			.replay_id
			.ok_or_else(|| profile_error("profiled fused path did not record a replay id"))?;
		let probe_columns_first = state.probe_columns_first.ok_or_else(|| {
			profile_error("profiled fused path did not record its build/probe orientation")
		})?;
		if state.predicate_output_rows != state.sum_argument_rows
			|| state.sum_argument_rows != state.numeric_fold_rows
		{
			return Err(profile_error(format!(
				"M -> S reconciliation failed: predicate_output={} sum_argument={} numeric_fold={}",
				state.predicate_output_rows, state.sum_argument_rows, state.numeric_fold_rows
			)));
		}
		let leaf_total = checked_sum(&state.leaf_micros, "exclusive leaf micros")?;
		let residual = root_span_micros.checked_sub(leaf_total).ok_or_else(|| {
			profile_error(format!(
				"exclusive leaves ({leaf_total}) exceeded root span ({root_span_micros})"
			))
		})?;
		let domain_1_micros = checked_sum(
			&[
				state.leaf_micros[GlobalFoldProfileLeaf::ProbeLookupSelect.index()],
				state.leaf_micros[GlobalFoldProfileLeaf::ArrowMaterialize.index()],
			],
			"D1 micros",
		)?;
		let domain_2_micros = state.leaf_micros[GlobalFoldProfileLeaf::PredicateEvalFilter.index()];
		let domain_3_micros = checked_sum(
			&[
				state.leaf_micros[GlobalFoldProfileLeaf::DecimalExpr.index()],
				state.leaf_micros[GlobalFoldProfileLeaf::NumericFold.index()],
			],
			"D3 micros",
		)?;
		let domain_4_micros = checked_sum(
			&[
				state.leaf_micros[GlobalFoldProfileLeaf::ScanPollDecode.index()],
				state.leaf_micros[GlobalFoldProfileLeaf::SpillPageRead.index()],
				state.leaf_micros[GlobalFoldProfileLeaf::BuildSpillWrite.index()],
			],
			"D4 micros",
		)?;
		let residual_ratio = if root_span_micros == 0 {
			0.0
		} else {
			residual as f64 / root_span_micros as f64
		};
		let leaves = GlobalFoldProfileLeaf::ALL
			.into_iter()
			.map(|leaf| LeafSnapshot {
				name: leaf.name(),
				micros: state.leaf_micros[leaf.index()],
				calls: state.leaf_calls[leaf.index()],
			})
			.collect();
		let snapshot = GlobalFoldJoinFilterSnapshot {
			schema_version: PROFILE_SCHEMA_VERSION,
			profile_id: self.profile_id,
			path,
			replay_id,
			probe_columns_first,
			build_fields: state.build_fields.clone(),
			probe_fields: state.probe_fields.clone(),
			root_span_micros,
			leaves,
			domain_1_micros,
			domain_2_micros,
			domain_3_micros,
			domain_4_micros,
			in_root_residual_micros: residual,
			in_root_residual_ratio: residual_ratio,
			in_root_residual_within_ceiling: residual_ratio <= 0.10,
			predicate_input_rows: state.predicate_input_rows,
			predicate_output_rows: state.predicate_output_rows,
			sum_argument_rows: state.sum_argument_rows,
			numeric_fold_rows: state.numeric_fold_rows,
			physical_expression_nodes: u64::try_from(self.expression_nodes.len())
				.map_err(|_| profile_error("expression node count exceeds u64"))?,
			expression_nodes: expression_census_clone(&self.expression_nodes),
		};
		drop(state);
		write_snapshot(&self.directory, self.profile_id, &snapshot)
	}

	fn finish_leaf(
		&self,
		leaf: GlobalFoldProfileLeaf,
		elapsed: Duration,
	) -> Result<(), ExecutionError> {
		let elapsed = checked_duration_micros(elapsed)?;
		let mut state = self.lock_state()?;
		if state.active_leaf != Some(leaf) {
			return Err(profile_error(format!(
				"exclusive leaf {} finished while {:?} was active",
				leaf.name(),
				state.active_leaf
			)));
		}
		let index = leaf.index();
		state.leaf_micros[index] = state.leaf_micros[index]
			.checked_add(elapsed)
			.ok_or_else(|| profile_error(format!("{} micros overflow", leaf.name())))?;
		state.leaf_calls[index] = state.leaf_calls[index]
			.checked_add(1)
			.ok_or_else(|| profile_error(format!("{} calls overflow", leaf.name())))?;
		state.active_leaf = None;
		Ok(())
	}

	fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ProfileState>, ExecutionError> {
		self.state
			.lock()
			.map_err(|_| profile_error("profile state mutex poisoned"))
	}

	#[cfg(test)]
	pub(in crate::exec) fn at_test_directory(
		directory: PathBuf,
		predicate: &BoundExprProgram,
	) -> Result<Self, ExecutionError> {
		Self::from_lookup(predicate, |_| Some(directory.into_os_string()))?
			.ok_or_else(|| profile_error("test profile unexpectedly disabled"))
	}
}

impl GlobalFoldProfileTimer<'_> {
	pub(in crate::exec) fn finish(mut self) -> Result<(), ExecutionError> {
		self.profile
			.finish_leaf(self.leaf, self.started.elapsed())?;
		self.finished = true;
		Ok(())
	}
}

impl Drop for GlobalFoldProfileTimer<'_> {
	fn drop(&mut self) {
		if self.finished {
			return;
		}
		if let Ok(mut state) = self.profile.state.lock()
			&& state.active_leaf == Some(self.leaf)
		{
			state.active_leaf = None;
		}
	}
}

fn checked_add_usize(counter: &mut u64, delta: usize, label: &str) -> Result<(), ExecutionError> {
	let delta =
		u64::try_from(delta).map_err(|_| profile_error(format!("{label} delta exceeds u64")))?;
	*counter = counter
		.checked_add(delta)
		.ok_or_else(|| profile_error(format!("{label} overflow")))?;
	Ok(())
}

fn checked_duration_micros(duration: Duration) -> Result<u64, ExecutionError> {
	u64::try_from(duration.as_micros())
		.map_err(|_| profile_error("profile duration exceeds u64 microseconds"))
}

fn checked_sum(values: &[u64], label: &str) -> Result<u64, ExecutionError> {
	values.iter().try_fold(0u64, |total, value| {
		total
			.checked_add(*value)
			.ok_or_else(|| profile_error(format!("{label} overflow")))
	})
}

fn expression_census(program: &BoundExprProgram) -> Result<Vec<ExprNodeCensus>, ExecutionError> {
	let mut references = vec![0u64; program.nodes.len()];
	for root in &program.roots {
		increment_reference(&mut references, *root)?;
	}
	for node in &program.nodes {
		for child in expression_children(node) {
			increment_reference(&mut references, child)?;
		}
	}
	program
		.nodes
		.iter()
		.enumerate()
		.map(|(index, node)| {
			Ok(ExprNodeCensus {
				node_index: u64::try_from(index)
					.map_err(|_| profile_error("expression node index exceeds u64"))?,
				node_kind: expression_node_kind(node),
				reference_multiplicity: references[index],
				node_debug: format!("{node:?}"),
			})
		})
		.collect()
}

fn expression_census_clone(nodes: &[ExprNodeCensus]) -> Vec<ExprNodeCensus> {
	nodes
		.iter()
		.map(|node| ExprNodeCensus {
			node_index: node.node_index,
			node_kind: node.node_kind,
			reference_multiplicity: node.reference_multiplicity,
			node_debug: node.node_debug.clone(),
		})
		.collect()
}

fn increment_reference(references: &mut [u64], child: BoundExprId) -> Result<(), ExecutionError> {
	let reference = references
		.get_mut(child.0 as usize)
		.ok_or_else(|| profile_error(format!("expression child {} out of bounds", child.0)))?;
	*reference = reference
		.checked_add(1)
		.ok_or_else(|| profile_error("expression reference multiplicity overflow"))?;
	Ok(())
}

fn expression_children(node: &BoundExprNode) -> Vec<BoundExprId> {
	match node {
		BoundExprNode::Column(_) | BoundExprNode::Literal(_) | BoundExprNode::NullLiteral => {
			Vec::new()
		}
		BoundExprNode::Cast { input, .. }
		| BoundExprNode::TryCast { input, .. }
		| BoundExprNode::Unary { input, .. }
		| BoundExprNode::Like { expr: input, .. }
		| BoundExprNode::AtTimeZone { input, .. }
		| BoundExprNode::Extract { input, .. }
		| BoundExprNode::DateTrunc { input, .. } => vec![*input],
		BoundExprNode::Binary { left, right, .. } => vec![*left, *right],
		BoundExprNode::InList { expr, list, .. } => {
			let mut children = Vec::with_capacity(list.len().saturating_add(1));
			children.push(*expr);
			children.extend(list.iter().copied());
			children
		}
		BoundExprNode::Case {
			when_then,
			else_input,
		} => {
			let mut children =
				Vec::with_capacity(when_then.len().saturating_mul(2).saturating_add(1));
			for (when, then) in when_then {
				children.push(*when);
				children.push(*then);
			}
			children.push(*else_input);
			children
		}
		BoundExprNode::ScalarFunction { args, .. } => args.clone(),
	}
}

fn expression_node_kind(node: &BoundExprNode) -> &'static str {
	match node {
		BoundExprNode::Column(_) => "column",
		BoundExprNode::Literal(_) => "literal",
		BoundExprNode::NullLiteral => "null_literal",
		BoundExprNode::Cast { .. } => "cast",
		BoundExprNode::TryCast { .. } => "try_cast",
		BoundExprNode::Binary { .. } => "binary",
		BoundExprNode::Unary { .. } => "unary",
		BoundExprNode::InList { .. } => "in_list",
		BoundExprNode::Like { .. } => "like",
		BoundExprNode::Case { .. } => "case",
		BoundExprNode::ScalarFunction { .. } => "scalar_function",
		BoundExprNode::AtTimeZone { .. } => "at_time_zone",
		BoundExprNode::Extract { .. } => "extract",
		BoundExprNode::DateTrunc { .. } => "date_trunc",
	}
}

fn write_snapshot(
	directory: &Path,
	profile_id: u64,
	snapshot: &GlobalFoldJoinFilterSnapshot,
) -> Result<(), ExecutionError> {
	std::fs::create_dir_all(directory)
		.map_err(|error| profile_error(format!("create profile directory: {error}")))?;
	let bytes = serde_json::to_vec_pretty(snapshot)
		.map_err(|error| profile_error(format!("serialize snapshot: {error}")))?;
	let final_path = directory.join(format!("global_fold_{profile_id:04}.json"));
	let temporary = directory.join(format!("global_fold_{profile_id:04}.tmp"));
	std::fs::write(&temporary, bytes)
		.map_err(|error| profile_error(format!("write temporary snapshot: {error}")))?;
	std::fs::rename(&temporary, final_path)
		.map_err(|error| profile_error(format!("publish snapshot: {error}")))
}

fn profile_error(message: impl Into<String>) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("global fold join/filter profile: {}", message.into()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ir::{
		BoundBinaryOp, BoundExprProgramBuilder, ColumnName, ColumnOrdinal, ResolvedColumn,
	};
	use arrow::datatypes::DataType;

	fn predicate() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let column = builder.append_column(ResolvedColumn {
			ordinal: ColumnOrdinal(0),
			name: ColumnName("flag".to_string()),
			data_type: DataType::Boolean,
			nullable: false,
			pbc_eligible: false,
		});
		let repeated = builder.append_binary(BoundBinaryOp::And, column, column, DataType::Boolean);
		builder.push_root(repeated);
		builder.build()
	}

	fn test_directory(label: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"toron_global_fold_join_filter_{label}_{}_{}",
			std::process::id(),
			NEXT_PROFILE_ID.fetch_add(1, Ordering::Relaxed)
		))
	}

	fn complete_profile(profile: &GlobalFoldJoinFilterProfile) {
		let root = profile.begin_root().expect("begin root");
		let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
		profile
			.record_dispatch("test_path", JoinReplayId(7), true, &schema, &schema)
			.expect("record dispatch");
		for leaf in GlobalFoldProfileLeaf::ALL {
			profile
				.begin_leaf(leaf)
				.expect("begin leaf")
				.finish()
				.expect("finish leaf");
		}
		profile
			.record_predicate_rows(10, 3)
			.expect("predicate rows");
		profile.record_sum_argument_rows(3).expect("sum rows");
		profile.record_numeric_fold_rows(3).expect("fold rows");
		profile.finish_root(root).expect("finish root");
	}

	#[test]
	fn disabled_profile_constructs_no_state_or_sink() {
		let directory = test_directory("disabled");
		let profile = GlobalFoldJoinFilterProfile::from_lookup(&predicate(), |_| None)
			.expect("disabled lookup");
		assert!(profile.is_none());
		assert!(!directory.exists());
	}

	#[test]
	fn expression_census_counts_physical_nodes_and_references() {
		let census = expression_census(&predicate()).expect("expression census");
		assert_eq!(census.len(), 2);
		assert_eq!(census[0].reference_multiplicity, 2);
		assert_eq!(census[1].reference_multiplicity, 1);
	}

	#[test]
	fn exclusive_leaf_overlap_is_loud_in_release_state() {
		let directory = test_directory("overlap");
		let profile = GlobalFoldJoinFilterProfile::at_test_directory(directory, &predicate())
			.expect("test profile");
		let root = profile.begin_root().expect("begin root");
		let timer = profile
			.begin_leaf(GlobalFoldProfileLeaf::ProbeLookupSelect)
			.expect("begin first leaf");
		let error = profile
			.begin_leaf(GlobalFoldProfileLeaf::ArrowMaterialize)
			.err()
			.expect("overlap should fail");
		assert!(error.to_string().contains("overlapped active leaf"));
		timer.finish().expect("finish first leaf");
		profile.finish_root(root).expect("finish root");
	}

	#[test]
	fn row_reconciliation_rejects_misaligned_survivors() {
		let directory = test_directory("rows");
		let profile = GlobalFoldJoinFilterProfile::at_test_directory(directory, &predicate())
			.expect("test profile");
		let root = profile.begin_root().expect("begin root");
		let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
		profile
			.record_dispatch("test_path", JoinReplayId(0), false, &schema, &schema)
			.expect("record dispatch");
		profile
			.record_predicate_rows(10, 3)
			.expect("predicate rows");
		profile.record_sum_argument_rows(2).expect("sum rows");
		profile.record_numeric_fold_rows(2).expect("fold rows");
		profile.finish_root(root).expect("finish root");
		let error = profile.persist().expect_err("reconciliation should fail");
		assert!(error.to_string().contains("M -> S reconciliation failed"));
	}

	#[test]
	fn enabled_profile_persists_exact_exclusive_counts_atomically() {
		let directory = test_directory("enabled");
		let profile =
			GlobalFoldJoinFilterProfile::at_test_directory(directory.clone(), &predicate())
				.expect("test profile");
		let profile_id = profile.profile_id;
		complete_profile(&profile);
		profile.persist().expect("persist profile");

		let final_path = directory.join(format!("global_fold_{profile_id:04}.json"));
		let snapshot: serde_json::Value =
			serde_json::from_slice(&std::fs::read(&final_path).expect("read persisted profile"))
				.expect("parse persisted profile");
		assert_eq!(snapshot["schema_version"], PROFILE_SCHEMA_VERSION);
		assert_eq!(snapshot["path"], "test_path");
		assert_eq!(snapshot["replay_id"], 7);
		assert_eq!(snapshot["predicate_input_rows"], 10);
		assert_eq!(snapshot["predicate_output_rows"], 3);
		assert_eq!(snapshot["sum_argument_rows"], 3);
		assert_eq!(snapshot["numeric_fold_rows"], 3);
		assert_eq!(snapshot["physical_expression_nodes"], 2);
		let leaves = snapshot["leaves"].as_array().expect("leaf snapshots");
		assert_eq!(leaves.len(), LEAF_COUNT);
		assert!(leaves.iter().all(|leaf| leaf["calls"] == 1));
		assert!(
			!directory
				.join(format!("global_fold_{profile_id:04}.tmp"))
				.exists(),
			"atomic publish must not retain the temporary snapshot"
		);
		std::fs::remove_dir_all(directory).expect("remove test profile directory");
	}

	#[test]
	fn enabled_snapshot_io_failures_are_loud() {
		let directory = test_directory("io_failure");
		std::fs::write(&directory, b"not a directory").expect("create path obstruction");
		let profile =
			GlobalFoldJoinFilterProfile::at_test_directory(directory.clone(), &predicate())
				.expect("test profile");
		complete_profile(&profile);
		let error = profile.persist().expect_err("snapshot I/O should fail");
		assert!(error.to_string().contains("create profile directory"));
		std::fs::remove_file(directory).expect("remove path obstruction");
	}
}
