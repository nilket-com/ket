//! ADR 507 step 3: per-plan-node attribution for the generic executor tree.
//!
//! The composed-gap grounding (impl/507) found that six of the seven candidate cells — every one
//! except the reoriented-chain cell the `ChainStageProfiler` already covers — execute primarily as a
//! tree of `ExecutionPlan` nodes through `execute_bound` recursion, not as a single fused loop. The
//! fused-loop profilers attribute the *inside* of one operator; nothing attributed the *tree* that
//! contains them. This profiler does: it times each node exclusively (its own work, with its
//! children's time subtracted) and attributes spill to the node that produced it, so "where did this
//! query's time and bytes go" is answered per operator across the whole plan.
//!
//! It is one instrument spanning both mechanism classes: a spill-heavy cell and a compute-heavy cell
//! differ only in *which* node kind carries the exclusive time and bytes, within one node-kind
//! vocabulary. That is the ADR-507 step-3 result — the universal layer is per-node, and the
//! fused-loop profilers refine specific nodes beneath it.
//!
//! Emitted through the attribution contract ([`super::join::stage_attribution`]-shaped): exclusive
//! per-node time reconciles to the *`execute_bound` root span* (the span this profiler can measure)
//! with an explicit in-tree residual, and spill is a flow domain (per-node exclusive spill sums to the
//! root spill delta). The outer band — root span ⊆ the query's `execute_ms` — is reconciled reader-
//! side by the harness, which holds `execute_ms`; the profiler cannot. It attributes time and spill,
//! not row flow: a `Drop`-closed span cannot see a node's output, and the reconciliation needs neither.
//!
//! ## Timing model and its one boundary
//!
//! Exclusive time uses a LIFO frame stack: each node pushes a frame on enter and, on exit, computes
//! `inclusive = elapsed`, `exclusive = inclusive - children_inclusive`, and rolls its inclusive time
//! into its parent's `children_inclusive`. This is exact when children are awaited sequentially,
//! which holds for the single-node perf path (`execute_bound(&bound, None)`). If a node executes
//! children concurrently, their frames interleave and an exclusive figure can go negative; the
//! profiler clamps to zero and records an `interleaved_frames` count so the anomaly is visible rather
//! than silent. The contract's time reconciliation is the backstop: a stack that mis-nested badly
//! would push the residual outside its boundary and fail the pilot honestly.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use crate::distributed::task::TaskTelemetry;
use crate::error::ExecutionError;
use crate::exec::join::stage_attribution;
use crate::ir::ExecutionPlan;

const NODE_ATTRIBUTION_ENV: &str = "TORON_NODE_ATTRIBUTION_PROFILE_PATH";

/// The `ExecutionPlan` node kinds the executor tree is attributed against. Deliberately the operator
/// vocabulary, not a lifecycle vocabulary: it is what makes one profile comparable across queries of
/// different shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum NodeKind {
	Scan,
	Filter,
	Join,
	Aggregate,
	Exchange,
	Sort,
	Other,
}

const NODE_KIND_COUNT: usize = 7;

/// Classify a plan node into its attribution kind. Kept beside the profiler so the vocabulary and its
/// mapping live together; the executor calls this once per node.
pub(in crate::exec) fn node_kind_of(plan: &ExecutionPlan) -> NodeKind {
	match plan {
		ExecutionPlan::Scan { .. } => NodeKind::Scan,
		ExecutionPlan::Filter { .. } => NodeKind::Filter,
		ExecutionPlan::Join { .. } => NodeKind::Join,
		ExecutionPlan::Aggregate { .. } => NodeKind::Aggregate,
		ExecutionPlan::Exchange { .. } => NodeKind::Exchange,
		ExecutionPlan::Sort { .. } => NodeKind::Sort,
		_ => NodeKind::Other,
	}
}

impl NodeKind {
	const ALL: [Self; NODE_KIND_COUNT] = [
		Self::Scan,
		Self::Filter,
		Self::Join,
		Self::Aggregate,
		Self::Exchange,
		Self::Sort,
		Self::Other,
	];

	fn index(self) -> usize {
		self as usize
	}

	fn name(self) -> &'static str {
		match self {
			Self::Scan => "scan",
			Self::Filter => "filter",
			Self::Join => "join",
			Self::Aggregate => "aggregate",
			Self::Exchange => "exchange",
			Self::Sort => "sort",
			Self::Other => "other",
		}
	}
}

#[derive(Clone, Copy, Default)]
struct NodeMeasurement {
	exclusive_micros: u64,
	calls: u64,
	exclusive_spill_bytes: u64,
}

/// A frame carries only what its children accumulate into it. The measured span (elapsed, spill
/// delta) is supplied to [`NodeAccounting::exit`] by the caller, so the accounting is pure — no clock
/// and no telemetry — and can be exercised deterministically.
#[derive(Default)]
struct Frame {
	children_micros: u64,
	children_spill: u64,
}

/// The pure stack accounting. Given, per node, its measured inclusive time and inclusive spill, it
/// derives exclusive figures (own work, children subtracted) and the root's totals. It never reads a
/// clock or telemetry, so its nesting arithmetic is unit-testable in isolation.
#[derive(Default)]
struct NodeAccounting {
	stack: Vec<Frame>,
	nodes: [NodeMeasurement; NODE_KIND_COUNT],
	interleaved_frames: u64,
	/// Set when the root frame pops: the whole-tree inclusive span and spill delta the per-node
	/// exclusive figures must reconcile to.
	root_span_micros: u64,
	root_spill_delta: u64,
	root_spill_before: u64,
}

impl NodeAccounting {
	fn enter(&mut self) {
		self.stack.push(Frame::default());
	}

	/// Close the current node with its measured inclusive span and spill. Returns whether this pop
	/// emptied the stack (the root), at which point the root totals are final.
	fn exit(
		&mut self,
		kind: NodeKind,
		inclusive_micros: u64,
		inclusive_spill: u64,
		spill_after: u64,
	) -> bool {
		let Some(frame) = self.stack.pop() else {
			return false;
		};
		// Exclusive = inclusive minus what children accounted for. A negative here means frames
		// interleaved (concurrent children); clamp and count it rather than panic.
		let exclusive_micros = match inclusive_micros.checked_sub(frame.children_micros) {
			Some(value) => value,
			None => {
				self.interleaved_frames = self.interleaved_frames.saturating_add(1);
				0
			}
		};
		let exclusive_spill = inclusive_spill.saturating_sub(frame.children_spill);
		let measurement = &mut self.nodes[kind.index()];
		measurement.exclusive_micros = measurement
			.exclusive_micros
			.saturating_add(exclusive_micros);
		measurement.calls = measurement.calls.saturating_add(1);
		measurement.exclusive_spill_bytes = measurement
			.exclusive_spill_bytes
			.saturating_add(exclusive_spill);
		if let Some(parent) = self.stack.last_mut() {
			parent.children_micros = parent.children_micros.saturating_add(inclusive_micros);
			parent.children_spill = parent.children_spill.saturating_add(inclusive_spill);
			false
		} else {
			self.root_span_micros = inclusive_micros;
			self.root_spill_delta = inclusive_spill;
			self.root_spill_before = spill_after.saturating_sub(inclusive_spill);
			true
		}
	}

	fn attributed_exclusive_micros(&self) -> u64 {
		self.nodes.iter().fold(0u64, |total, node| {
			total.saturating_add(node.exclusive_micros)
		})
	}
}

static NODE_LEDGER: Mutex<Option<NodeAccounting>> = Mutex::new(None);

fn node_attribution_path() -> Option<&'static PathBuf> {
	static PATH: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
	PATH.get_or_init(|| std::env::var_os(NODE_ATTRIBUTION_ENV).map(PathBuf::from))
		.as_ref()
}

fn enabled() -> bool {
	node_attribution_path().is_some()
}

fn read_spill(telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>) -> u64 {
	telemetry
		.as_ref()
		.and_then(|t| t.lock().ok().map(|t| t.counters.spill_bytes))
		.unwrap_or(0)
}

/// Begin attributing one plan node and return a guard that closes it on drop — normal exit, an arm's
/// early `return`, an error, or a panic — the same fail-safe shape as the other executor profilers.
/// The guard being dropped at the end of `execute_bound`'s body is what makes the node match itself
/// untouched. A disabled run returns an inert guard: no lock, no clock, no allocation.
pub(in crate::exec) fn enter(
	kind: NodeKind,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> NodeSpan {
	if !enabled() {
		return NodeSpan {
			kind,
			start: None,
			spill_before: 0,
			telemetry: None,
			active: false,
		};
	}
	let spill_before = read_spill(telemetry);
	if let Ok(mut slot) = NODE_LEDGER.lock() {
		slot.get_or_insert_with(NodeAccounting::default).enter();
	}
	NodeSpan {
		kind,
		start: Some(Instant::now()),
		spill_before,
		telemetry: telemetry.clone(),
		active: true,
	}
}

/// The open attribution span for one plan node. Held for the whole of `execute_bound`'s body and
/// closed on drop. It carries its own clock start and spill baseline so the shared accounting stays a
/// pure function of measured values.
pub(in crate::exec) struct NodeSpan {
	kind: NodeKind,
	start: Option<Instant>,
	spill_before: u64,
	telemetry: Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
	active: bool,
}

impl Drop for NodeSpan {
	fn drop(&mut self) {
		if !self.active {
			return;
		}
		let inclusive_micros = self
			.start
			.map(|start| start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64)
			.unwrap_or(0);
		let spill_after = read_spill(&self.telemetry);
		let inclusive_spill = spill_after.saturating_sub(self.spill_before);
		let mut finished: Option<NodeAccounting> = None;
		if let Ok(mut slot) = NODE_LEDGER.lock() {
			if let Some(accounting) = slot.as_mut() {
				if accounting.exit(self.kind, inclusive_micros, inclusive_spill, spill_after) {
					finished = slot.take();
				}
			}
		}
		if let Some(accounting) = finished {
			// Write outside the lock; a diagnostic write failure must never mask the query result.
			let _ = write_profile(&accounting);
		}
	}
}

fn write_profile(accounting: &NodeAccounting) -> Result<(), ExecutionError> {
	let Some(path) = node_attribution_path() else {
		return Ok(());
	};
	let exclusive_sum = accounting.attributed_exclusive_micros();
	// Time: node exclusive spans reconcile to the `execute_bound` root span — NOT `execute_ms`. The
	// root span is what this profiler can measure; the outer band (root span ⊆ `execute_ms`) is the
	// reader's, joined from the perf row, exactly as the chain profiler's drive ⊆ execute. The
	// contract computes the in-tree residual and refuses an inverted band, so a mis-nested stack fails
	// here rather than emitting false precision. This residual is near-zero by telescoping — it
	// asserts internal consistency (no lost or double-counted frame), not completeness against
	// `execute_ms`.
	let in_tree_residual_micros =
		stage_attribution::reconcile_stage_within_drive(exclusive_sum, accounting.root_span_micros)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("node attribution time reconciliation failed: {error}"),
			})?;
	// Bytes: per-node exclusive spill is a flow domain reconciling to the root spill delta. Enforced,
	// not observed — the contract computes `unattributed_bytes` and refuses over-attribution. The
	// boundary is zero: exclusive spills are integers that telescope to the root delta exactly, so any
	// residual is a bug, not tolerance.
	let spill_parts: Vec<(&'static str, u64)> = NodeKind::ALL
		.iter()
		.map(|kind| {
			(
				kind.name(),
				accounting.nodes[kind.index()].exclusive_spill_bytes,
			)
		})
		.collect();
	let spill_domain = stage_attribution::FlowByteDomain {
		name: "node_exclusive_spill",
		parts: &spill_parts,
		whole: accounting.root_spill_delta,
		boundary: 0,
	};
	let spill_reconciliation =
		spill_domain
			.reconcile()
			.map_err(|error| ExecutionError::Kernel {
				message: format!("node attribution spill reconciliation failed: {error}"),
			})?;
	let nodes = NodeKind::ALL
		.iter()
		.map(|kind| {
			let measurement = accounting.nodes[kind.index()];
			serde_json::json!({
				"node": kind.name(),
				"exclusive_micros": measurement.exclusive_micros,
				"calls": measurement.calls,
				"exclusive_spill_bytes": measurement.exclusive_spill_bytes,
			})
		})
		.collect::<Vec<_>>();
	let value = serde_json::json!({
		"suite": "node_attribution",
		// The span this profiler measures — begins inside `execute_bound`, ends at the root pop. It is
		// a subset of the perf row's `execute_ms`; reconciling to `execute_ms` is the reader's step.
		"execute_bound_span_micros": accounting.root_span_micros,
		"execute_ms_reconciliation": "external: root span is a subset of the perf row execute_ms",
		"attributed_exclusive_micros": exclusive_sum,
		"in_tree_residual_micros": in_tree_residual_micros,
		"root_spill_delta_bytes": accounting.root_spill_delta,
		"root_spill_before_bytes": accounting.root_spill_before,
		"spill_reconciliation": spill_reconciliation,
		"interleaved_frames": accounting.interleaved_frames,
		"nodes": nodes,
	});
	let temporary = path.with_extension("tmp");
	let contents = serde_json::to_vec_pretty(&value).map_err(|error| ExecutionError::Kernel {
		message: format!("node attribution serialization failed: {error}"),
	})?;
	std::fs::write(&temporary, contents).map_err(|error| ExecutionError::Kernel {
		message: format!("node attribution write failed: {error}"),
	})?;
	std::fs::rename(&temporary, path).map_err(|error| ExecutionError::Kernel {
		message: format!("node attribution rename failed: {error}"),
	})?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::join::stage_attribution::FlowByteDomain;

	fn spill_parts(accounting: &NodeAccounting) -> Vec<(&'static str, u64)> {
		NodeKind::ALL
			.iter()
			.map(|kind| {
				(
					kind.name(),
					accounting.nodes[kind.index()].exclusive_spill_bytes,
				)
			})
			.collect()
	}

	// A tree: Aggregate(root) → Join → Scan, awaited in sequence, with a sibling Filter under the
	// Aggregate. Enter/exit are driven with explicit measured spans and spill so the nesting
	// arithmetic is checked without a clock or telemetry. Inclusive figures are what a parent sees
	// for a child; the caller (the Drop guard in production) passes each node its own inclusive span.
	#[test]
	fn exclusive_time_and_spill_subtract_children_across_a_nested_tree() {
		let mut acc = NodeAccounting::default();
		// Aggregate root opens (inclusive 1000us / 900B once its subtree is done).
		acc.enter();
		//   Join opens (inclusive 600us / 500B).
		acc.enter();
		//     Scan opens and closes: leaf, inclusive 250us / 200B, spill_after 200.
		acc.enter();
		assert!(!acc.exit(NodeKind::Scan, 250, 200, 200));
		//   Join closes: inclusive 600 / 500, spill_after 500. Exclusive = 600-250 / 500-200.
		assert!(!acc.exit(NodeKind::Join, 600, 500, 500));
		//   Filter sibling under the root: inclusive 100us / 50B, spill_after 550.
		acc.enter();
		assert!(!acc.exit(NodeKind::Filter, 100, 50, 550));
		// Aggregate root closes: inclusive 1000 / 900, spill_after 900. Root pop returns true.
		assert!(acc.exit(NodeKind::Aggregate, 1000, 900, 900));

		// Scan is a leaf: exclusive == inclusive.
		assert_eq!(acc.nodes[NodeKind::Scan.index()].exclusive_micros, 250);
		assert_eq!(acc.nodes[NodeKind::Scan.index()].exclusive_spill_bytes, 200);
		// Join had one child (Scan): exclusive = 600-250 = 350us, 500-200 = 300B.
		assert_eq!(acc.nodes[NodeKind::Join.index()].exclusive_micros, 350);
		assert_eq!(acc.nodes[NodeKind::Join.index()].exclusive_spill_bytes, 300);
		// Filter leaf.
		assert_eq!(acc.nodes[NodeKind::Filter.index()].exclusive_micros, 100);
		assert_eq!(
			acc.nodes[NodeKind::Filter.index()].exclusive_spill_bytes,
			50
		);
		// Aggregate root had two children accounting for 600+100 us and 500+50 B inclusive.
		// Exclusive = 1000-700 = 300us, 900-550 = 350B.
		assert_eq!(acc.nodes[NodeKind::Aggregate.index()].exclusive_micros, 300);
		assert_eq!(
			acc.nodes[NodeKind::Aggregate.index()].exclusive_spill_bytes,
			350
		);

		// The whole point of the contract: exclusive spans telescope to the root span exactly.
		assert_eq!(acc.root_span_micros, 1000);
		assert_eq!(acc.attributed_exclusive_micros(), 1000);
		assert_eq!(acc.root_spill_delta, 900);
		assert_eq!(acc.interleaved_frames, 0);
		assert_eq!(acc.root_spill_before, 0);

		// And both contract axes reconcile with zero residual — enforced, not observed.
		assert_eq!(
			stage_attribution::reconcile_stage_within_drive(
				acc.attributed_exclusive_micros(),
				acc.root_span_micros,
			),
			Ok(0)
		);
		let parts = spill_parts(&acc);
		let domain = FlowByteDomain {
			name: "node_exclusive_spill",
			parts: &parts,
			whole: acc.root_spill_delta,
			boundary: 0,
		};
		assert_eq!(
			domain.reconcile().expect("reconciles").unattributed_bytes,
			0
		);
	}

	// A non-zero root spill baseline (spill written before the tree began) must not leak into
	// per-node exclusive spill: the root delta and every node stay scoped to in-tree spill.
	#[test]
	fn spill_before_the_root_is_excluded_from_in_tree_attribution() {
		let mut acc = NodeAccounting::default();
		// Root enters with 1000 bytes already spilled by earlier work outside execute_bound. The
		// guard captured spill_before=1000; it passes inclusive_spill = spill_after - spill_before.
		acc.enter();
		acc.enter();
		// Scan wrote 300 bytes: spill went 1000 -> 1300, inclusive_spill 300, spill_after 1300.
		assert!(!acc.exit(NodeKind::Scan, 100, 300, 1300));
		// Root: wrote 200 more of its own; spill 1300 -> 1500. inclusive_spill = 1500-1000 = 500.
		assert!(acc.exit(NodeKind::Aggregate, 400, 500, 1500));

		assert_eq!(acc.root_spill_delta, 500, "only in-tree spill");
		assert_eq!(
			acc.root_spill_before, 1000,
			"pre-tree spill recorded, not attributed"
		);
		assert_eq!(acc.nodes[NodeKind::Scan.index()].exclusive_spill_bytes, 300);
		assert_eq!(
			acc.nodes[NodeKind::Aggregate.index()].exclusive_spill_bytes,
			200
		);
		let parts = spill_parts(&acc);
		let domain = FlowByteDomain {
			name: "node_exclusive_spill",
			parts: &parts,
			whole: acc.root_spill_delta,
			boundary: 0,
		};
		assert_eq!(
			domain.reconcile().expect("reconciles").unattributed_bytes,
			0
		);
	}

	// Interleaved (concurrent) children would drive an exclusive figure negative. The accounting
	// clamps to zero and counts the anomaly rather than panicking or emitting false precision.
	#[test]
	fn interleaved_children_are_clamped_and_counted_not_panicked() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		// A child reports MORE inclusive time than its parent — only possible if frames interleaved.
		assert!(!acc.exit(NodeKind::Join, 900, 0, 0));
		// Parent inclusive 500 < child's rolled-in 900 → exclusive clamps to 0, anomaly counted.
		assert!(acc.exit(NodeKind::Aggregate, 500, 0, 0));
		assert_eq!(acc.nodes[NodeKind::Aggregate.index()].exclusive_micros, 0);
		assert_eq!(acc.interleaved_frames, 1);
	}
}
