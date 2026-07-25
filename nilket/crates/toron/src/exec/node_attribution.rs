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
	/// A stable identity assigned when the frame opens, strictly increasing within a run. ADR 508
	/// step 2 matches an inner handler profile to its invocation by this id and nothing else — not by
	/// route name, completion order, or timing, any of which is ambiguous when one handler family
	/// takes two invocations in the same query.
	id: u64,
	children_micros: u64,
	children_spill: u64,
	/// ADR 508 step 1: which streaming handler took this node, stamped by the dispatcher while the
	/// frame is open. Per frame, not per query, because a query can invoke the same node kind more
	/// than once and each invocation may route differently.
	route: Option<&'static str>,
	/// ADR 508 step 2: what the winning handler accounted for inside this frame. Stored on the frame
	/// the handler ran in, so the attribution is bound to its invocation at the moment it is measured
	/// rather than correlated afterwards.
	handler: Option<HandlerAttribution>,
	/// ADR 508 step 2: the handler's own stage vocabulary and its spill counter kind, declared by the
	/// handler before it records any stage. Vocabularies differ per handler by design — comparability
	/// comes from the shared contract and explicit residuals, not from one shared stage list.
	vocabulary: Option<Vocabulary>,
	/// A two-phase declaration awaiting commit. Only one declaration mechanism is live per frame: a
	/// predeclare replaces a prior pending attempt, and a commit consumes this into `vocabulary`.
	pending: Option<PendingVocabulary>,
	/// A misused declaration — an unknown, missing, or conflicting commit — recorded so the invocation
	/// fails by name rather than publishing whichever definition survived the misuse.
	vocabulary_failure: Option<&'static str>,
	stages: Vec<HandlerStage>,
	/// Attached while this frame is still open — a handler publishes its census before its own
	/// `execute_bound` returns, so the completed record does not exist yet.
	growth_census: Option<serde_json::Value>,
}

/// What a fused handler accounted for inside one `Aggregate` invocation, already made exclusive of the
/// nested `execute_bound` children it drove — the same domain as the node's exclusive cost, so the two
/// are directly comparable and their difference is a real residual rather than a units mismatch.
///
/// Fails closed: an attribution that cannot be computed is a named failure, never a clamped number. A
/// handler span shorter than the children it drove, for instance, is a broken measurement, and
/// saturating it to zero would publish a confident wrong answer.
#[derive(Clone, Copy)]
enum HandlerAttribution {
	Reconciled {
		exclusive_micros: u64,
		exclusive_spill_bytes: u64,
	},
	Failed(&'static str),
}

#[cfg(test)]
impl HandlerAttribution {
	fn reconciled_micros(self) -> u64 {
		match self {
			Self::Reconciled {
				exclusive_micros, ..
			} => exclusive_micros,
			Self::Failed(reason) => panic!("expected a reconciled attribution, got: {reason}"),
		}
	}

	fn reconciled_spill(self) -> u64 {
		match self {
			Self::Reconciled {
				exclusive_spill_bytes,
				..
			} => exclusive_spill_bytes,
			Self::Failed(reason) => panic!("expected a reconciled attribution, got: {reason}"),
		}
	}
}

/// What kind of counter a vocabulary's spill fields are, declared *before* reconciliation so the
/// arithmetic is chosen by the instrumenter rather than assumed by the reader — the ADR 507 byte-domain
/// rule applied one layer down.
///
/// There is deliberately no `Peak`. The only handler-level spill figure available is a task-telemetry
/// **flow delta** (bytes written across the handler span), and a peak stage counter has no same-domain
/// whole to close against — comparing a max-of-stages to a flow total would relabel one domain as the
/// other and report a reconciliation that never happened. A vocabulary that genuinely needs peak
/// residency must first add a separately measured handler-level peak; until then its stages are Flow
/// or an explicitly-reasoned Subset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum SpillCounterKind {
	/// Stage spills sum to the handler's spill flow delta.
	Flow,
	/// Stage spills cover only part of the handler's flow delta by construction. The reason is
	/// vocabulary-specific and must name where the remainder is accounted, not restate the category.
	///
	/// No vocabulary declares this yet, and spilling alone is not what would justify one: a handler
	/// that writes 83 MB has been decomposed and closed under `Flow` with every byte attributed. What
	/// this kind requires is a *deliberately excluded writer* — a spill path inside the handler span
	/// that some other domain accounts for — positively identified and named at the declaration.
	///
	/// A `Flow` domain that fails to close is **not** that evidence. It says the declared lifecycle is
	/// incomplete, and the usual cause is a missing stage or a mis-drawn boundary; the fix is to name
	/// the writer, not to widen the domain until the red goes away. Unlike a domain that publishes a
	/// reconciliation it cannot support, an unreachable domain cannot produce a false verdict, and its
	/// arithmetic is exercised directly in `stage_attribution`.
	#[allow(dead_code)]
	Subset(&'static str),
}

impl SpillCounterKind {
	fn name(self) -> &'static str {
		match self {
			Self::Flow => "flow",
			Self::Subset(_) => "subset",
		}
	}
}

/// One required stage: a name, scoped to a replay identity or to the whole invocation. A replay-scoped
/// requirement is satisfied only by a reconciled stage carrying the same replay id — one observed
/// `suffix_probe` must not stand in for a six-replay chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) struct RequiredStage {
	pub name: &'static str,
	pub replay: Option<u64>,
}

/// A vocabulary's promised stages: static for lifecycles fixed at compile time, owned where the
/// requirement is runtime data — a suffix chain's replay count is read from the extracted chain, which
/// a `&'static` list cannot express. The static arm keeps the existing declarations allocation-free.
#[derive(Debug, Clone)]
pub(in crate::exec) enum RequiredStages {
	/// Replay-unscoped names fixed at compile time.
	Static(&'static [&'static str]),
	/// Runtime-derived entries, each scoped to a replay identity or unscoped.
	Owned(Vec<RequiredStage>),
}

impl RequiredStages {
	fn entries(&self) -> impl Iterator<Item = (&'static str, Option<u64>)> + '_ {
		let (fixed, owned) = match self {
			Self::Static(names) => (Some(names.iter()), None),
			Self::Owned(entries) => (None, Some(entries.iter())),
		};
		fixed.into_iter().flatten().map(|name| (*name, None)).chain(
			owned
				.into_iter()
				.flatten()
				.map(|entry| (entry.name, entry.replay)),
		)
	}

	fn contains_unscoped(&self, required: &'static str) -> bool {
		self.entries()
			.any(|(name, replay)| name == required && replay.is_none())
	}
}

/// One candidate stage contract offered at predeclaration. Every definition a commit may select is
/// fixed here, before any stage is measured — commit only chooses among these, so the coverage
/// requirement can never be shaped by what the measurement happened to record.
#[derive(Debug, Clone)]
pub(in crate::exec) struct VocabularyVariant {
	pub name: &'static str,
	pub spill: SpillCounterKind,
	pub required_stages: RequiredStages,
	pub residual_boundary_micros: u64,
}

/// A handler's declared stage contract: what it will report, and the boundary its residual must sit
/// inside. Declared before measurement so coverage is a promise checked against, not a description of
/// whatever happened to be recorded.
#[derive(Clone)]
struct Vocabulary {
	/// The invocation this vocabulary was declared on. Stages must open against the same id.
	invocation_id: u64,
	name: &'static str,
	spill: SpillCounterKind,
	/// Stages this vocabulary promises to observe. A missing one is incomplete coverage even when the
	/// arithmetic reconciles.
	required_stages: RequiredStages,
	/// The largest un-staged remainder of the handler span this vocabulary accepts, declared up front.
	residual_boundary_micros: u64,
}

/// A predeclared-but-uncommitted contract. Stages recorded while it is pending are buffered here and
/// spliced into the frame only at commit: an attempted branch that declines never publishes them, so a
/// lifecycle the query did not commit through cannot leak into another vocabulary's arithmetic.
struct PendingVocabulary {
	variants: Vec<VocabularyVariant>,
	stages: Vec<HandlerStage>,
}

/// One stage inside a handler's own work, exclusive of the nested `execute_bound` children it drove.
///
/// Fails closed like the handler attribution above it. A stage whose exclusivity cannot be computed
/// is `Failed`, never a clamped zero: a zero-valued stage would satisfy its name in the required set
/// and disappear inside a permissive residual, which is precisely a broken measurement reading as
/// coverage.
#[derive(Clone, Copy)]
struct HandlerStage {
	name: &'static str,
	/// How many times this stage was opened and closed. With a 1 us resolution floor, `calls` is what
	/// bounds a zero-duration stage: its true cost is strictly below `calls x 1 us`.
	calls: u64,
	/// `None` for a top-level stage, which reconciles to the handler-exclusive span. `Some(parent)`
	/// for a refinement, which reconciles to *its parent stage's* micros — a sub-stage that closed
	/// against the handler instead would double-count with the parent it sits inside.
	parent: Option<&'static str>,
	/// The replay identity this stage instance belongs to, `None` for an unreplayed stage. Part of the
	/// stage's key: instances of one name merge only within a replay, and a refinement reconciles only
	/// against the parent instance carrying its own replay — without this, one record would satisfy an
	/// N-replay chain and children from several replays would reconcile against the first parent found.
	replay: Option<u64>,
	outcome: StageOutcome,
}

#[derive(Clone, Copy)]
enum StageOutcome {
	Reconciled { micros: u64, spill_bytes: u64 },
	Failed(&'static str),
}

impl StageOutcome {
	fn reconciled(self) -> Option<(u64, u64)> {
		match self {
			Self::Reconciled {
				micros,
				spill_bytes,
			} => Some((micros, spill_bytes)),
			Self::Failed(_) => None,
		}
	}
}

/// One `Aggregate` node invocation: its identity, route, own exclusive cost, and the handler
/// attribution bridged to it. The unit ADR 508 requires, because a cell's dominant `aggregate` share
/// can be a sum across several invocations — matching a handler profile to the right one is impossible
/// from a per-kind total alone.
#[derive(Clone)]
struct AggregateInvocation {
	id: u64,
	route: Option<&'static str>,
	exclusive_micros: u64,
	exclusive_spill_bytes: u64,
	handler: Option<HandlerAttribution>,
	vocabulary: Option<Vocabulary>,
	/// Carried off the frame at exit: a declaration misuse fails the completed record too.
	vocabulary_failure: Option<&'static str>,
	stages: Vec<HandlerStage>,
	/// The fold's growth census, published beside the exact stages. It never enters their arithmetic.
	growth_census: Option<serde_json::Value>,
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
	aggregate_invocations: Vec<AggregateInvocation>,
	next_frame_id: u64,
	/// Handler spans that closed against a frame that was not the one they opened on. Published so a
	/// mismatch is a counted anomaly rather than an invocation that silently carries no attribution.
	bridge_mismatches: u64,
	/// Stages that closed against a frame they did not open on.
	stage_mismatches: u64,
	/// Stage failures whose invocation had already exited, kept keyed to it so the record still fails.
	orphaned_stage_failures: Vec<(u64, &'static str)>,
}

impl NodeAccounting {
	fn enter(&mut self) {
		let id = self.next_frame_id;
		self.next_frame_id = self.next_frame_id.saturating_add(1);
		self.stack.push(Frame {
			id,
			..Frame::default()
		});
	}

	/// Snapshot the open frame's identity and the child work it has absorbed so far. Taken when a
	/// handler begins so its span can later be made exclusive of the children it drives.
	fn open_handler(&self) -> Option<HandlerSpan> {
		let frame = self.stack.last()?;
		Some(HandlerSpan {
			invocation_id: frame.id,
			children_micros_at_open: frame.children_micros,
			children_spill_at_open: frame.children_spill,
			spill_before: 0,
			started: None,
		})
	}

	/// Close a handler span against the frame it ran in. `span_micros`/`span_spill` are the handler's
	/// raw inclusive figures; the children it drove in the meantime are subtracted here so the result
	/// sits in the same domain as the node's exclusive cost.
	fn close_handler(&mut self, span: HandlerSpan, span_micros: u64, span_spill: Option<u64>) {
		let Some(frame) = self.stack.last_mut() else {
			self.bridge_mismatches = self.bridge_mismatches.saturating_add(1);
			return;
		};
		if frame.id != span.invocation_id {
			// The frame moved under the handler. Refuse to attribute — and count it, so a mismatch is
			// a visible number rather than an invocation that quietly carries no handler.
			self.bridge_mismatches = self.bridge_mismatches.saturating_add(1);
			return;
		}
		// Children only accumulate, so an underflow here means the frame's accounting moved
		// inconsistently under the span; that is a defect, not something to clamp.
		let (Some(nested_micros), Some(nested_spill)) = (
			frame
				.children_micros
				.checked_sub(span.children_micros_at_open),
			frame
				.children_spill
				.checked_sub(span.children_spill_at_open),
		) else {
			frame.handler = Some(HandlerAttribution::Failed(
				"frame child accounting moved backwards under the handler span",
			));
			return;
		};
		let Some(span_spill) = span_spill else {
			frame.handler = Some(HandlerAttribution::Failed(
				"handler spill delta could not be read or went backwards",
			));
			return;
		};
		let (Some(exclusive_micros), Some(exclusive_spill_bytes)) = (
			span_micros.checked_sub(nested_micros),
			span_spill.checked_sub(nested_spill),
		) else {
			frame.handler = Some(HandlerAttribution::Failed(
				"handler span is smaller than the children it drove",
			));
			return;
		};
		frame.handler = Some(HandlerAttribution::Reconciled {
			exclusive_micros,
			exclusive_spill_bytes,
		});
	}

	/// Stamp the open node with the handler that took it. Called by the aggregate dispatcher at the
	/// point an arm wins, when that arm's own children have already popped — so the top of the stack
	/// is the `Aggregate` frame being routed, not one of its descendants.
	fn set_route(&mut self, route: &'static str) {
		if let Some(frame) = self.stack.last_mut() {
			frame.route = Some(route);
		}
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
		if kind == NodeKind::Aggregate {
			// One record per invocation, keyed by the frame's stable id — assigned at open, so it is
			// the same id an inner profile stamped itself with while running. An invocation with no
			// route means every streaming arm declined and the generic fold ran — uncovered, named.
			// A pending declaration that was never committed is dropped here with its buffered
			// stages: an attempted branch that declined publishes nothing.
			self.aggregate_invocations.push(AggregateInvocation {
				id: frame.id,
				route: frame.route,
				exclusive_micros,
				exclusive_spill_bytes: exclusive_spill,
				handler: frame.handler,
				vocabulary: frame.vocabulary,
				vocabulary_failure: frame.vocabulary_failure,
				stages: frame.stages,
				growth_census: frame.growth_census,
			});
		}
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

	/// Accumulate a closed stage, exclusive of the children it drove. Every arithmetic step is
	/// checked — child deltas, the exclusivity subtraction, and the merge of a repeated stage — so an
	/// impossible figure becomes a named failure and never a clamped zero. Identity is verified
	/// against the frame the stage opened on; if that frame has already exited, the failure is
	/// orphaned to its invocation id rather than dropped.
	/// A handler publishes its estimate while its own `execute_bound` is still running, so the frame
	/// is on the stack and no completed record exists yet. Look there first, and fall back to the
	/// completed list so a late publisher still lands on the right invocation rather than nowhere.
	fn stage_micros(
		&self,
		id: u64,
		name: &'static str,
		parent: Option<&'static str>,
	) -> Option<u64> {
		let stages = self
			.stack
			.iter()
			.find(|frame| frame.id == id)
			.map(|frame| &frame.stages)
			.or_else(|| {
				self.aggregate_invocations
					.iter()
					.find(|invocation| invocation.id == id)
					.map(|invocation| &invocation.stages)
			})?;
		stages
			.iter()
			// The parent-authority lookup serves the unreplayed census path; a replay-scoped stage is
			// never a whole-invocation authority.
			.find(|stage| stage.name == name && stage.parent == parent && stage.replay.is_none())
			.and_then(|stage| stage.outcome.reconciled().map(|(micros, _)| micros))
	}

	/// Phase one of the two-phase declaration: fix every definition a later commit may select, before
	/// any stage is measured. Returns the invocation id the handle binds to. A predeclaration replaces
	/// a prior uncommitted attempt (that attempt's buffered stages are dropped); predeclaring over an
	/// already-committed vocabulary is a misuse and fails the invocation by name.
	fn predeclare(&mut self, variants: Vec<VocabularyVariant>) -> Option<u64> {
		let frame = self.stack.last_mut()?;
		if frame.vocabulary.is_some() {
			frame.vocabulary_failure = Some("vocabulary predeclared after a committed vocabulary");
			return None;
		}
		frame.pending = Some(PendingVocabulary {
			variants,
			stages: Vec::new(),
		});
		Some(frame.id)
	}

	/// Phase two: select one predeclared variant as the observed lifecycle and splice the buffered
	/// stages into the frame. Commit may only choose among definitions fixed at predeclaration — an
	/// unknown variant, a commit with nothing predeclared, or a conflict with a committed vocabulary
	/// each fails the invocation by name, and the buffered stages are discarded rather than published
	/// under a definition the misuse has made untrustworthy.
	fn commit_vocabulary(&mut self, invocation_id: u64, observed: &'static str) {
		let Some(frame) = self.stack.iter_mut().find(|f| f.id == invocation_id) else {
			if let Some(invocation) = self
				.aggregate_invocations
				.iter_mut()
				.find(|invocation| invocation.id == invocation_id)
			{
				invocation.vocabulary_failure =
					Some("vocabulary commit after its invocation exited");
			}
			return;
		};
		if frame.vocabulary.is_some() {
			frame.vocabulary_failure =
				Some("vocabulary commit conflicts with an already-committed vocabulary");
			return;
		}
		let Some(pending) = frame.pending.take() else {
			frame.vocabulary_failure = Some("vocabulary commit without a predeclaration");
			return;
		};
		let Some(variant) = pending
			.variants
			.into_iter()
			.find(|variant| variant.name == observed)
		else {
			frame.vocabulary_failure =
				Some("vocabulary commit named a variant that was not predeclared");
			return;
		};
		frame.vocabulary = Some(Vocabulary {
			invocation_id,
			name: variant.name,
			spill: variant.spill,
			required_stages: variant.required_stages,
			residual_boundary_micros: variant.residual_boundary_micros,
		});
		for stage in pending.stages {
			Self::merge_stage(&mut frame.stages, stage);
		}
	}

	fn attach_growth_census(&mut self, id: u64, estimate: serde_json::Value) {
		if let Some(frame) = self.stack.iter_mut().find(|frame| frame.id == id) {
			frame.growth_census = Some(estimate);
			return;
		}
		if let Some(invocation) = self
			.aggregate_invocations
			.iter_mut()
			.find(|invocation| invocation.id == id)
		{
			invocation.growth_census = Some(estimate);
		}
	}

	fn close_stage(
		&mut self,
		token: StageToken,
		inclusive_micros: u64,
		inclusive_spill: Option<u64>,
	) {
		let Some(position) = self
			.stack
			.iter()
			.rposition(|frame| frame.id == token.invocation_id)
		else {
			self.stage_mismatches = self.stage_mismatches.saturating_add(1);
			self.orphaned_stage_failures
				.push((token.invocation_id, token.name));
			return;
		};
		// The invocation is open but is not the innermost frame: a child frame is still running, so
		// its work has not rolled into the parent and would remain inside this stage's span. Record
		// the failure on the right invocation and stop — do not compute a figure that may be
		// child-contaminated and would otherwise read as reconciled.
		if position + 1 != self.stack.len() {
			self.stage_mismatches = self.stage_mismatches.saturating_add(1);
			Self::record_stage_outcome(
				&mut self.stack[position],
				token.name,
				token.parent,
				token.replay,
				StageOutcome::Failed("stage closed while a child frame was still open"),
			);
			return;
		}
		let frame = &mut self.stack[position];
		let outcome = match (
			frame
				.children_micros
				.checked_sub(token.children_micros_at_open),
			frame
				.children_spill
				.checked_sub(token.children_spill_at_open),
			inclusive_spill,
		) {
			(Some(nested_micros), Some(nested_spill), Some(inclusive_spill)) => match (
				inclusive_micros.checked_sub(nested_micros),
				inclusive_spill.checked_sub(nested_spill),
			) {
				(Some(micros), Some(spill_bytes)) => StageOutcome::Reconciled {
					micros,
					spill_bytes,
				},
				_ => StageOutcome::Failed("stage span is smaller than the children it drove"),
			},
			(_, _, None) => {
				StageOutcome::Failed("stage spill delta could not be read or went backwards")
			}
			_ => StageOutcome::Failed("frame child accounting moved backwards under the stage"),
		};
		Self::record_stage_outcome(frame, token.name, token.parent, token.replay, outcome);
	}

	/// Record a stage outcome on a frame. While a declaration is pending, the record lands in its
	/// buffer — published only if that declaration commits — otherwise directly on the frame.
	fn record_stage_outcome(
		frame: &mut Frame,
		name: &'static str,
		parent: Option<&'static str>,
		replay: Option<u64>,
		outcome: StageOutcome,
	) {
		let stages = match frame.pending.as_mut() {
			Some(pending) => &mut pending.stages,
			None => &mut frame.stages,
		};
		Self::merge_stage(
			stages,
			HandlerStage {
				name,
				calls: 1,
				parent,
				replay,
				outcome,
			},
		);
	}

	/// Merge a stage instance into a list, keyed by `(name, parent, replay)` — a repeat merges only
	/// within its own replay identity. The merge is checked: an overflow makes the combined stage
	/// `Failed`, and either part failing fails the whole.
	fn merge_stage(stages: &mut Vec<HandlerStage>, incoming: HandlerStage) {
		if let Some(existing) = stages.iter_mut().find(|s| {
			s.name == incoming.name && s.parent == incoming.parent && s.replay == incoming.replay
		}) {
			existing.calls = existing.calls.saturating_add(incoming.calls);
			existing.outcome = match (existing.outcome, incoming.outcome) {
				(
					StageOutcome::Reconciled {
						micros: a,
						spill_bytes: b,
					},
					StageOutcome::Reconciled {
						micros: c,
						spill_bytes: d,
					},
				) => match (a.checked_add(c), b.checked_add(d)) {
					(Some(micros), Some(spill_bytes)) => StageOutcome::Reconciled {
						micros,
						spill_bytes,
					},
					_ => StageOutcome::Failed("repeated stage totals overflowed"),
				},
				(StageOutcome::Failed(reason), _) | (_, StageOutcome::Failed(reason)) => {
					StageOutcome::Failed(reason)
				}
			};
		} else {
			stages.push(incoming);
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

/// Read the task spill counter, distinguishing "no telemetry configured" (a consistent zero scope
/// both sides of every bridge agree on) from "the counter could not be read" (a poisoned lock), which
/// must never be silently reported as zero bytes.
fn try_read_spill(telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>) -> Option<u64> {
	match telemetry {
		None => Some(0),
		Some(handle) => handle.lock().ok().map(|t| t.counters.spill_bytes),
	}
}

fn read_spill(telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>) -> u64 {
	try_read_spill(telemetry).unwrap_or(0)
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

/// ADR 508 step 1: stamp the currently-open node with the streaming handler that took it.
///
/// Called from the aggregate dispatcher at the point an arm wins. This is the routing evidence the
/// handler-map receipt rests on: durable and per invocation, rather than a throwaway trace, so a cell
/// with several `Aggregate` invocations can have each one matched to its handler. No-op when
/// profiling is off.
pub(in crate::exec) fn record_route(route: &'static str) {
	if !enabled() {
		return;
	}
	if let Ok(mut slot) = NODE_LEDGER.lock() {
		if let Some(accounting) = slot.as_mut() {
			accounting.set_route(route);
		}
	}
}

/// A handler's span within one invocation, opened before the handler runs and closed after. Carries
/// the invocation id so the attribution lands on the frame it was measured in — never correlated by
/// route, order, or time.
#[derive(Clone, Copy)]
pub(in crate::exec) struct HandlerSpan {
	pub invocation_id: u64,
	children_micros_at_open: u64,
	children_spill_at_open: u64,
	spill_before: u64,
	started: Option<Instant>,
}

/// ADR 508 step 2: open a handler span on the currently-executing node. The returned handle carries
/// the stable invocation id an inner profile stamps itself with, so profile and invocation are matched
/// by identity. Captures the spill baseline in the same scope the node uses, so both sides of the
/// bridge measure the same counter. `None` when profiling is off or no node frame is open.
pub(in crate::exec) fn open_handler_span(
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> Option<HandlerSpan> {
	if !enabled() {
		return None;
	}
	let spill_before = read_spill(telemetry);
	let mut span = NODE_LEDGER
		.lock()
		.ok()
		.and_then(|slot| slot.as_ref().and_then(NodeAccounting::open_handler))?;
	span.spill_before = spill_before;
	span.started = Some(Instant::now());
	Some(span)
}

/// A handler's declared stage contract, bound to the invocation it was declared on. Stages open
/// against this handle, so both the declaration and every stage under it belong to one invocation —
/// not to whichever frame happens to be on top when the call lands.
#[derive(Clone, Copy)]
pub(in crate::exec) struct VocabularyHandle {
	invocation_id: u64,
}

/// ADR 508 step 2: declare a handler's stage contract before it records anything — the vocabulary
/// name, its spill counter kind, the stages it promises to observe, and the residual boundary it
/// accepts. Declaring coverage up front is what makes a missing stage detectable; a vocabulary that
/// merely described whatever it happened to record could never be incomplete.
pub(in crate::exec) fn declare_stage_vocabulary(
	name: &'static str,
	spill: SpillCounterKind,
	required_stages: &'static [&'static str],
	residual_boundary_micros: u64,
) -> Option<VocabularyHandle> {
	if !enabled() {
		return None;
	}
	let mut slot = NODE_LEDGER.lock().ok()?;
	let frame = slot.as_mut()?.stack.last_mut()?;
	let invocation_id = frame.id;
	// A single-phase declaration supersedes an abandoned two-phase attempt: the attempt's buffered
	// stages must not leak into this vocabulary's arithmetic.
	frame.pending = None;
	frame.vocabulary = Some(Vocabulary {
		invocation_id,
		name,
		spill,
		required_stages: RequiredStages::Static(required_stages),
		residual_boundary_micros,
	});
	Some(VocabularyHandle { invocation_id })
}

/// Phase one of the two-phase declaration, for a handler whose lifecycle is decided mid-flight:
/// runtime preparation runs — and is measured — before the branch that names the lifecycle wins.
/// Every variant a commit may later select is fixed here, before any measurement, so the coverage
/// requirement can never be chosen by what the stages happened to record. The builder runs only when
/// attribution is enabled: a disabled run constructs no requirement lists at all.
pub(in crate::exec) fn predeclare_stage_vocabulary(
	build: impl FnOnce() -> Vec<VocabularyVariant>,
) -> Option<VocabularyHandle> {
	if !enabled() {
		return None;
	}
	let mut slot = NODE_LEDGER.lock().ok()?;
	let invocation_id = slot.as_mut()?.predeclare(build())?;
	Some(VocabularyHandle { invocation_id })
}

/// Phase two: commit the observed lifecycle variant. Only a variant fixed at predeclaration can be
/// selected; anything else fails the invocation closed. An attempted branch that declines simply
/// never calls this, and its buffered stages are discarded when the frame exits.
pub(in crate::exec) fn commit_stage_vocabulary(handle: &VocabularyHandle, observed: &'static str) {
	if !enabled() {
		return;
	}
	if let Ok(mut slot) = NODE_LEDGER.lock()
		&& let Some(accounting) = slot.as_mut()
	{
		accounting.commit_vocabulary(handle.invocation_id, observed);
	}
}

/// An open stage inside a handler. Like [`HandlerSpan`], it carries the invocation id and a child
/// accounting snapshot, so closing it verifies identity and subtracts nested `execute_bound` work
/// itself. The caller never supplies a duration: a stage cannot be accidentally timed inclusive of
/// children, which a caller-supplied figure could be while still fitting inside a large residual and
/// passing.
#[derive(Clone, Copy)]
pub(in crate::exec) struct StageToken {
	name: &'static str,
	parent: Option<&'static str>,
	/// The replay identity this instance records under; part of the stage key at close.
	replay: Option<u64>,
	invocation_id: u64,
	children_micros_at_open: u64,
	children_spill_at_open: u64,
	started: Instant,
	spill_before: u64,
}

/// The vocabulary handle of the invocation currently executing, if its committed contract requires
/// `parent` as an unscoped top-level stage. Lets a refinement deep inside a handler attach without
/// threading the handle through every call, while preventing a shared helper from publishing children
/// under a parent that belongs only to another handler's lifecycle.
pub(in crate::exec) fn current_handle_requiring(parent: &'static str) -> Option<VocabularyHandle> {
	if !enabled() {
		return None;
	}
	NODE_LEDGER.lock().ok().and_then(|slot| {
		slot.as_ref().and_then(|a| {
			a.stack.last().and_then(|f| {
				f.vocabulary.as_ref().and_then(|v| {
					v.required_stages
						.contains_unscoped(parent)
						.then_some(VocabularyHandle {
							invocation_id: v.invocation_id,
						})
				})
			})
		})
	})
}

/// The exact micros a named stage has accumulated so far on this invocation. A sampled estimator
/// needs it as its parent authority; it never contributes to it.
pub(in crate::exec) fn accumulated_stage_micros(
	handle: &VocabularyHandle,
	name: &'static str,
	parent: Option<&'static str>,
) -> Option<u64> {
	if !enabled() {
		return None;
	}
	NODE_LEDGER.lock().ok().and_then(|slot| {
		slot.as_ref()
			.and_then(|accounting| accounting.stage_micros(handle.invocation_id, name, parent))
	})
}

/// Attach the fold's growth census to this invocation. It is published beside the exact stages and
/// deliberately outside their reconciliation: the exact stage remains the parent authority.
pub(in crate::exec) fn record_growth_census(
	handle: &VocabularyHandle,
	estimate: serde_json::Value,
) {
	if !enabled() {
		return;
	}
	if let Ok(mut slot) = NODE_LEDGER.lock()
		&& let Some(accounting) = slot.as_mut()
	{
		accounting.attach_growth_census(handle.invocation_id, estimate);
	}
}

/// Open a refinement stage *inside* an outer stage. It reconciles to `parent`'s micros, giving the
/// two-level lifecycle: outer phases explain the handler, refinements explain one outer phase.
pub(in crate::exec) fn open_substage(
	handle: &VocabularyHandle,
	parent: &'static str,
	name: &'static str,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> Option<StageToken> {
	open_stage_inner(handle, Some(parent), name, None, telemetry)
}

/// Open a stage under a declared vocabulary. Refuses if the executing invocation is not the one the
/// vocabulary was declared on.
pub(in crate::exec) fn open_stage(
	handle: &VocabularyHandle,
	name: &'static str,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> Option<StageToken> {
	open_stage_inner(handle, None, name, None, telemetry)
}

/// Open a stage instance bound to one replay identity of a repeated lifecycle — a suffix chain's Nth
/// probe records under `(name, replay)`, never merged with its siblings.
pub(in crate::exec) fn open_replay_stage(
	handle: &VocabularyHandle,
	name: &'static str,
	replay: u64,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> Option<StageToken> {
	open_stage_inner(handle, None, name, Some(replay), telemetry)
}

/// Open a refinement inside one replay instance of its parent. The refinement reconciles against the
/// parent instance carrying the same replay id, not against a name-matched parent from another replay.
///
/// No refinement is wired through this yet — the scan_probe_base cut records top-level replay stages
/// only; it exists so a later refinement cannot be added with an unscoped parent lookup.
#[allow(dead_code)]
pub(in crate::exec) fn open_replay_substage(
	handle: &VocabularyHandle,
	parent: &'static str,
	name: &'static str,
	replay: u64,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> Option<StageToken> {
	open_stage_inner(handle, Some(parent), name, Some(replay), telemetry)
}

fn open_stage_inner(
	handle: &VocabularyHandle,
	parent: Option<&'static str>,
	name: &'static str,
	replay: Option<u64>,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) -> Option<StageToken> {
	if !enabled() {
		return None;
	}
	let spill_before = read_spill(telemetry);
	let (invocation_id, children_micros_at_open, children_spill_at_open) =
		NODE_LEDGER.lock().ok().and_then(|slot| {
			slot.as_ref().and_then(|a| {
				a.stack
					.last()
					.map(|f| (f.id, f.children_micros, f.children_spill))
			})
		})?;
	if invocation_id != handle.invocation_id {
		return None;
	}
	Some(StageToken {
		name,
		parent,
		replay,
		invocation_id,
		children_micros_at_open,
		children_spill_at_open,
		started: Instant::now(),
		spill_before,
	})
}

/// Close a stage: verify it is still the invocation it opened on, subtract the nested children it
/// drove, and accumulate. An identity mismatch is counted, never attributed to the wrong invocation.
pub(in crate::exec) fn close_stage(
	token: StageToken,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) {
	if !enabled() {
		return;
	}
	let inclusive_micros = token
		.started
		.elapsed()
		.as_micros()
		.min(u128::from(u64::MAX)) as u64;
	// Checked, and a failed read is not zero bytes: either fails the stage.
	let inclusive_spill =
		try_read_spill(telemetry).and_then(|after| after.checked_sub(token.spill_before));
	if let Ok(mut slot) = NODE_LEDGER.lock() {
		if let Some(accounting) = slot.as_mut() {
			accounting.close_stage(token, inclusive_micros, inclusive_spill);
		}
	}
}

/// Close a handler span that *won* its dispatch. The nested `execute_bound` children the handler drove
/// are subtracted here, so the recorded attribution is exclusive — the same domain as the node's
/// exclusive cost, which is what makes the bridge's residual meaningful. A losing arm never closes its
/// span, so its probe time stays in the node's exclusive cost and surfaces as dispatch residual.
pub(in crate::exec) fn close_handler_span(
	span: HandlerSpan,
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
) {
	if !enabled() {
		return;
	}
	let span_micros = span
		.started
		.map(|start| start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64)
		.unwrap_or(0);
	// Checked, and a failed read is not zero bytes: either becomes a failed attribution.
	let span_spill =
		try_read_spill(telemetry).and_then(|after| after.checked_sub(span.spill_before));
	if let Ok(mut slot) = NODE_LEDGER.lock() {
		if let Some(accounting) = slot.as_mut() {
			accounting.close_handler(span, span_micros, span_spill);
		}
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

/// The dispatch residual a routed invocation may carry: the arms that probed and declined before the
/// winner, plus the dispatcher's own overhead. Declared before measuring. Probing is plan-shape
/// inspection that touches no data, so it is microseconds; a residual over 10 ms means the handler did
/// not account for its node, which is a bridge failure and not tolerable overhead.
const DISPATCH_RESIDUAL_BOUNDARY_MICROS: u64 = 10_000;

/// The node-to-handler bridge for one invocation: parts, residuals, boundary, and pass result.
///
/// Fails closed throughout. A handler exceeding its matched node on either axis is an inverted band —
/// physically impossible, so a broken measurement — and is reported as a failure with no residual,
/// never saturated to zero. Both axes go through the ADR 507 contract's checked reconcilers rather
/// than raw subtraction.
fn bridge_value(invocation: &AggregateInvocation) -> serde_json::Value {
	let (handler_micros, handler_spill) = match invocation.handler {
		Some(HandlerAttribution::Reconciled {
			exclusive_micros,
			exclusive_spill_bytes,
		}) => (exclusive_micros, exclusive_spill_bytes),
		Some(HandlerAttribution::Failed(reason)) => {
			return serde_json::json!({
				"status": "handler_attribution_failed",
				"error": reason,
				"passed": false,
			});
		}
		None => {
			// A routed invocation must be bridged; an unrouted one legitimately has nothing to bridge.
			return match invocation.route {
				Some(route) => serde_json::json!({
					"status": "handler_attribution_missing",
					"error": format!("invocation routed to {route} produced no handler attribution"),
					"passed": false,
				}),
				None => serde_json::json!({
					"status": "uncovered_generic_fold",
					"passed": true,
				}),
			};
		}
	};
	// Time: handler exclusive ⊆ node exclusive, checked. An inversion is a typed error, not a zero.
	let residual_micros = match stage_attribution::reconcile_stage_within_drive(
		handler_micros,
		invocation.exclusive_micros,
	) {
		Ok(residual) => residual,
		Err(error) => {
			return serde_json::json!({
				"status": "inverted_time_band",
				"error": error.to_string(),
				"handler_exclusive_micros": handler_micros,
				"node_exclusive_micros": invocation.exclusive_micros,
				"passed": false,
			});
		}
	};
	// Bytes: the handler is the only named part of the node's exclusive spill; the remainder is spill
	// attributed to dispatch. Boundary zero — probing does not write spill, so any is a finding.
	let spill_parts = [("handler", handler_spill)];
	let spill_domain = stage_attribution::FlowByteDomain {
		name: "aggregate_invocation_spill",
		parts: &spill_parts,
		whole: invocation.exclusive_spill_bytes,
		boundary: 0,
	};
	let (residual_spill, spill_error) = match spill_domain.reconcile() {
		Ok(reconciliation) => (Some(reconciliation.unattributed_bytes), None),
		Err(error) => (None, Some(error.to_string())),
	};
	let within_time = residual_micros <= DISPATCH_RESIDUAL_BOUNDARY_MICROS;
	serde_json::json!({
		"status": "reconciled",
		"handler_exclusive_micros": handler_micros,
		"handler_exclusive_spill_bytes": handler_spill,
		"node_exclusive_micros": invocation.exclusive_micros,
		"node_exclusive_spill_bytes": invocation.exclusive_spill_bytes,
		"dispatch_residual_micros": residual_micros,
		"dispatch_residual_spill_bytes": residual_spill,
		"boundary_micros": DISPATCH_RESIDUAL_BOUNDARY_MICROS,
		"spill_error": spill_error,
		"passed": within_time && spill_error.is_none(),
	})
}

/// The innermost link: a handler's stages reconcile to its *handler-exclusive* span — never straight
/// to the node, which is the separate bridge above.
///
/// Two independent verdicts, deliberately not collapsed. **Arithmetic** asks whether the numbers add
/// up: stages inside their span, spill closing by the declared kind. **Coverage** asks whether the
/// numbers mean anything: every promised stage observed, and the un-staged remainder inside the
/// boundary the vocabulary declared. A vocabulary that records nothing reconciles perfectly and
/// explains nothing — arithmetic passes, coverage fails, and `passed` requires both.
fn stages_value(invocation: &AggregateInvocation, orphaned: &[&'static str]) -> serde_json::Value {
	// A declaration misuse dominates every other verdict: whatever definition survived it cannot be
	// trusted to describe the measurement, so nothing downstream is computed.
	if let Some(reason) = invocation.vocabulary_failure {
		return serde_json::json!({
			"status": "vocabulary_commit_failed",
			"error": reason,
			"passed": false,
		});
	}
	let handler_micros = match invocation.handler {
		Some(HandlerAttribution::Reconciled {
			exclusive_micros, ..
		}) => exclusive_micros,
		_ => {
			return serde_json::json!({
				"status": "no_handler_span_to_reconcile_against",
				"passed": invocation.route.is_none(),
			});
		}
	};
	let Some(vocabulary) = invocation.vocabulary.as_ref() else {
		return serde_json::json!({
			"status": "stage_attribution_missing",
			"error": invocation.route.map(|route| {
				format!("handler {route} produced no stage vocabulary")
			}),
			"passed": invocation.route.is_none(),
		});
	};
	// Only reconciled stages contribute time; a failed stage contributes nothing and, below, does not
	// satisfy its name in the required set.
	// Only top-level stages close against the handler span; refinements close against their parent
	// below, so counting them here would double the parent they sit inside.
	let attributed_micros = invocation
		.stages
		.iter()
		.filter(|stage| stage.parent.is_none())
		.fold(0u64, |total, stage| {
			total.saturating_add(stage.outcome.reconciled().map_or(0, |(micros, _)| micros))
		});
	let failed_stages: Vec<serde_json::Value> = invocation
		.stages
		.iter()
		.filter_map(|stage| match stage.outcome {
			StageOutcome::Failed(reason) => Some(serde_json::json!({
				"stage": stage.name,
				"error": reason,
			})),
			StageOutcome::Reconciled { .. } => None,
		})
		.chain(orphaned.iter().map(|name| {
			serde_json::json!({
				"stage": name,
				"error": "stage closed after its invocation had already exited",
			})
		}))
		.collect();
	let stage_residual_micros =
		match stage_attribution::reconcile_stage_within_drive(attributed_micros, handler_micros) {
			Ok(residual) => residual,
			Err(error) => {
				return serde_json::json!({
					"status": "stages_exceed_handler_span",
					"vocabulary": vocabulary.name,
					"error": error.to_string(),
					"passed": false,
				});
			}
		};
	let handler_spill = handler_spill_of(invocation);
	let stage_spill_parts: Vec<(&'static str, u64)> = invocation
		.stages
		.iter()
		.filter(|stage| stage.parent.is_none())
		.filter_map(|stage| {
			stage
				.outcome
				.reconciled()
				.map(|(_, spill_bytes)| (stage.name, spill_bytes))
		})
		.collect();
	let (stage_spill_total, spill_error) = match vocabulary.spill {
		SpillCounterKind::Flow => {
			let domain = stage_attribution::FlowByteDomain {
				name: "handler_stage_spill",
				parts: &stage_spill_parts,
				whole: handler_spill,
				boundary: 0,
			};
			match domain.reconcile() {
				Ok(reconciliation) => (reconciliation.parts_total, None),
				Err(error) => (0, Some(error.to_string())),
			}
		}
		SpillCounterKind::Subset(reason) => {
			let subset = stage_spill_parts
				.iter()
				.fold(0u64, |total, (_, bytes)| total.saturating_add(*bytes));
			match stage_attribution::reconcile_subset_of_wider_scope(
				"handler_stage_spill",
				subset,
				handler_spill,
				reason,
			) {
				Ok(_) => (subset, None),
				Err(error) => (subset, Some(error.to_string())),
			}
		}
	};
	// Coverage: every promised stage observed, and the remainder inside the declared boundary. A
	// replay-scoped requirement is satisfied only within its own replay identity, so the failure names
	// the exact missing ids rather than letting one observed instance stand in for a chain.
	let missing_stages: Vec<serde_json::Value> = vocabulary
		.required_stages
		.entries()
		// A failed stage does not satisfy its name: coverage means an observed, reconciled stage.
		.filter(|(required, replay)| {
			!invocation.stages.iter().any(|s| {
				s.name == *required
					&& s.parent.is_none()
					&& s.replay == *replay
					&& matches!(s.outcome, StageOutcome::Reconciled { .. })
			})
		})
		.map(|(name, replay)| required_stage_value(name, replay))
		.collect();
	let within_residual_boundary = stage_residual_micros <= vocabulary.residual_boundary_micros;
	// A failed stage — or one orphaned by an exited invocation — is an arithmetic failure of this
	// invocation, not merely a global counter that no verdict reads.
	let arithmetic_reconciled = spill_error.is_none() && failed_stages.is_empty();
	let coverage_complete = missing_stages.is_empty() && within_residual_boundary;
	let stages = invocation
		.stages
		.iter()
		.map(|stage| match stage.outcome {
			StageOutcome::Reconciled {
				micros,
				spill_bytes,
			} => serde_json::json!({
				"stage": stage.name,
				"parent": stage.parent,
				"replay_id": stage.replay,
				"micros": micros,
				"spill_bytes": spill_bytes,
				"calls": stage.calls,
				// A stage that observed calls but measured zero is below the timer's resolution, not
				// free. Each call loses strictly less than one tick to truncation, so `calls` bounds
				// the LOSS — a nonzero stage's true cost is bounded by `micros + calls`, not by
				// `calls` alone.
				"below_resolution": micros == 0 && stage.calls > 0,
				// A stage averaging under a tick per call is quantization-dominated even when a few
				// calls happened to cross a tick boundary: 5us over 5859 calls is not a measurement
				// of 5us. Reading only `below_resolution` would treat that stage as measured.
				"quantization_dominated": stage.calls > 0 && micros < stage.calls,
				"quantization_loss_upper_bound_micros": stage.calls,
			}),
			StageOutcome::Failed(reason) => serde_json::json!({
				"stage": stage.name,
				"parent": stage.parent,
				"replay_id": stage.replay,
				"calls": stage.calls,
				"error": reason,
			}),
		})
		.collect::<Vec<_>>();
	// Each refined parent INSTANCE gets its own reconciliation link: sub-stages ⊆ that instance's
	// micros. The instance is `(name, replay)` — parent lookup and child grouping are both
	// replay-scoped, or children from several replays would all reconcile against the first parent
	// whose name matched, silently mixing replays.
	let mut refinements = Vec::new();
	let mut refined_parents: Vec<(&'static str, Option<u64>)> = Vec::new();
	for stage in &invocation.stages {
		if let Some(parent) = stage.parent
			&& !refined_parents.contains(&(parent, stage.replay))
		{
			refined_parents.push((parent, stage.replay));
		}
	}
	let mut refinements_reconciled = true;
	for (parent, replay) in refined_parents {
		let parent_micros = invocation
			.stages
			.iter()
			.find(|s| s.name == parent && s.parent.is_none() && s.replay == replay)
			.and_then(|s| s.outcome.reconciled())
			.map(|(micros, _)| micros);
		let child_micros = invocation
			.stages
			.iter()
			.filter(|s| s.parent == Some(parent) && s.replay == replay)
			.fold(0u64, |total, s| {
				total.saturating_add(s.outcome.reconciled().map_or(0, |(micros, _)| micros))
			});
		let entry = match parent_micros {
			None => {
				refinements_reconciled = false;
				serde_json::json!({
					"parent": parent,
					"replay_id": replay,
					"status": "parent_stage_not_reconciled",
					"passed": false,
				})
			}
			Some(parent_micros) => {
				match stage_attribution::reconcile_stage_within_drive(child_micros, parent_micros) {
					Ok(residual) => serde_json::json!({
						"parent": parent,
						"replay_id": replay,
						"status": "reconciled",
						"attributed_micros": child_micros,
						"parent_micros": parent_micros,
						"residual_micros": residual,
						"passed": true,
					}),
					Err(error) => {
						refinements_reconciled = false;
						serde_json::json!({
							"parent": parent,
							"replay_id": replay,
							"status": "refinement_exceeds_parent_stage",
							"error": error.to_string(),
							"passed": false,
						})
					}
				}
			}
		};
		refinements.push(entry);
	}
	serde_json::json!({
		"status": "reconciled",
		"vocabulary": vocabulary.name,
		"spill_counter_kind": vocabulary.spill.name(),
		"required_stages": vocabulary
			.required_stages
			.entries()
			.map(|(name, replay)| required_stage_value(name, replay))
			.collect::<Vec<_>>(),
		// Stage durations are whole microseconds: `Instant::elapsed().as_micros()` truncates, so a
		// per-call stage under a microsecond records zero every time. Zero means "below resolution",
		// never "free" — `calls` and `quantization_upper_bound_micros` bound what was lost, and the
		// refinement residual bounds it collectively.
		"resolution_micros": 1,
		"stages": stages,
		"refinements": refinements,
		"attributed_micros": attributed_micros,
		"handler_exclusive_micros": handler_micros,
		"stage_residual_micros": stage_residual_micros,
		"residual_boundary_micros": vocabulary.residual_boundary_micros,
		"within_residual_boundary": within_residual_boundary,
		"stage_spill_total": stage_spill_total,
		"handler_exclusive_spill_bytes": handler_spill,
		"spill_error": spill_error,
		"missing_required_stages": missing_stages,
		"failed_stages": failed_stages,
		"arithmetic_reconciled": arithmetic_reconciled,
		"coverage_complete": coverage_complete,
		"refinements_reconciled": refinements_reconciled,
		"passed": arithmetic_reconciled && coverage_complete && refinements_reconciled,
	})
}

/// How one required stage is named in the profile. An unscoped requirement stays the bare string the
/// static vocabularies have always published; a replay-scoped one names its exact id, which is what
/// makes a missing-replay failure actionable.
fn required_stage_value(name: &'static str, replay: Option<u64>) -> serde_json::Value {
	match replay {
		None => serde_json::json!(name),
		Some(id) => serde_json::json!({"stage": name, "replay_id": id}),
	}
}

fn handler_spill_of(invocation: &AggregateInvocation) -> u64 {
	match invocation.handler {
		Some(HandlerAttribution::Reconciled {
			exclusive_spill_bytes,
			..
		}) => exclusive_spill_bytes,
		_ => 0,
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
	// One record per Aggregate invocation, with its route and its own exclusive cost — the unit the
	// handler map is keyed on, and the span a step-2 inner profile must close against.
	let aggregate_invocations = accounting
		.aggregate_invocations
		.iter()
		.map(|invocation| {
			// Failures orphaned by an already-exited invocation still land on that invocation's record.
			let orphaned: Vec<&'static str> = accounting
				.orphaned_stage_failures
				.iter()
				.filter(|(id, _)| *id == invocation.id)
				.map(|(_, name)| *name)
				.collect();
			serde_json::json!({
				"invocation_id": invocation.id,
				"route": invocation.route.unwrap_or("uncovered_generic_fold"),
				"exclusive_micros": invocation.exclusive_micros,
				"exclusive_spill_bytes": invocation.exclusive_spill_bytes,
				"bridge": bridge_value(invocation),
				"stages": stages_value(invocation, &orphaned),
				// Beside the stages, never inside their arithmetic: `passed` above is computed
				// without ever consulting this.
				"growth_census": invocation.growth_census.clone(),
			})
		})
		.collect::<Vec<_>>();
	let value = serde_json::json!({
		"suite": "node_attribution",
		"aggregate_invocations": aggregate_invocations,
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
		"bridge_mismatches": accounting.bridge_mismatches,
		"stage_mismatches": accounting.stage_mismatches,
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

	// A handler publishes from inside its own `execute_bound`, so at that moment its frame is still on
	// the stack and no completed invocation record exists. Looking only at the completed list silently
	// drops every census — which is exactly what happened before this test.
	#[test]
	fn a_census_published_from_an_open_frame_lands_on_that_invocation() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let open_id = acc.stack.last().expect("open frame").id;
		acc.attach_growth_census(open_id, serde_json::json!({"separated": true}));
		// Still open: the estimate cannot be on the completed list yet.
		assert!(acc.aggregate_invocations.is_empty());
		assert!(!acc.exit(NodeKind::Aggregate, 400, 0, 0));
		assert!(acc.exit(NodeKind::Aggregate, 1_000, 0, 0));
		let matched = acc
			.aggregate_invocations
			.iter()
			.find(|invocation| invocation.id == open_id)
			.expect("invocation");
		assert_eq!(
			matched.growth_census,
			Some(serde_json::json!({"separated": true})),
			"the estimate must ride the frame it was published on"
		);
		// The sibling invocation must not inherit it.
		assert!(
			acc.aggregate_invocations
				.iter()
				.filter(|invocation| invocation.id != open_id)
				.all(|invocation| invocation.growth_census.is_none())
		);
	}

	// ADR 508 step 1: a query with several Aggregate invocations must yield one routed record per
	// invocation, each carrying its own exclusive cost. A per-kind total cannot be matched to a
	// handler profile; this is the unit that can. An invocation no streaming arm claimed is named
	// uncovered rather than silently attributed.
	#[test]
	fn each_aggregate_invocation_records_its_own_route_and_cost() {
		let mut acc = NodeAccounting::default();
		// Root Aggregate routed to a streaming handler, with a child Aggregate under it that no arm
		// claimed — the shape that would otherwise let one profile stand in for two invocations.
		acc.enter();
		acc.enter();
		// Inner aggregate: never routed, so it fell through to the generic fold.
		assert!(!acc.exit(NodeKind::Aggregate, 400, 100, 100));
		// Outer aggregate: the dispatcher stamped it while its frame was the open one.
		acc.set_route("reoriented_inner_chain_grouped");
		assert!(acc.exit(NodeKind::Aggregate, 1000, 250, 250));

		assert_eq!(
			acc.aggregate_invocations.len(),
			2,
			"one record per invocation"
		);
		// Records land in completion order, but ids are assigned at open — the inner invocation
		// (opened second, closed first) is record 0 carrying id 1. That position and id disagree is
		// exactly why matching must be by id.
		let inner = &acc.aggregate_invocations[0];
		assert_eq!(
			inner.id, 1,
			"opened second, so id 1, though it closed first"
		);
		assert_eq!(
			inner.route, None,
			"an unclaimed invocation carries no route"
		);
		assert_eq!(inner.exclusive_micros, 400);
		let outer = &acc.aggregate_invocations[1];
		assert_eq!(outer.id, 0, "opened first, so id 0, though it closed last");
		assert_eq!(outer.route, Some("reoriented_inner_chain_grouped"));
		// Outer exclusive subtracts the inner child, so invocation costs do not double-count.
		assert_eq!(outer.exclusive_micros, 600);
		assert_eq!(outer.exclusive_spill_bytes, 150);
		// And the invocations sum to the Aggregate node-kind total the per-node layer reports.
		assert_eq!(
			acc.aggregate_invocations
				.iter()
				.map(|i| i.exclusive_micros)
				.sum::<u64>(),
			acc.nodes[NodeKind::Aggregate.index()].exclusive_micros
		);
	}

	// ADR 508 step 2, guard 1: identity, not route or order. Two invocations of the SAME handler in
	// one query must stay distinguishable, which route name and completion order cannot do.
	#[test]
	fn invocation_ids_are_stable_and_distinct_for_a_repeated_handler() {
		let mut acc = NodeAccounting::default();
		// A root wrapping both invocations, so neither aggregate pop is the root pop.
		acc.enter();
		acc.enter();
		let first = acc.open_handler().expect("frame open");
		acc.close_handler(first, 300, Some(0));
		acc.set_route("scan_grouped");
		assert!(!acc.exit(NodeKind::Aggregate, 400, 0, 0));

		acc.enter();
		let second = acc.open_handler().expect("frame open");
		acc.close_handler(second, 700, Some(0));
		acc.set_route("scan_grouped");
		assert!(!acc.exit(NodeKind::Aggregate, 900, 0, 0));
		assert!(acc.exit(NodeKind::Other, 2000, 0, 0));

		// Same route both times — only the id separates them.
		assert_ne!(first.invocation_id, second.invocation_id);
		let ids: Vec<u64> = acc.aggregate_invocations.iter().map(|i| i.id).collect();
		assert_eq!(ids.len(), 2);
		assert_ne!(ids[0], ids[1], "ids distinguish repeated handlers");
		assert_eq!(acc.aggregate_invocations[0].id, first.invocation_id);
		assert_eq!(acc.aggregate_invocations[1].id, second.invocation_id);
		// And each invocation kept its own handler attribution.
		assert_eq!(
			acc.aggregate_invocations[0]
				.handler
				.expect("attributed")
				.reconciled_micros(),
			300
		);
		assert_eq!(
			acc.aggregate_invocations[1]
				.handler
				.expect("attributed")
				.reconciled_micros(),
			700
		);
	}

	// Guard 2: the handler's span includes the `execute_bound` children it drives; the node's
	// exclusive cost does not. Subtracting the nested children puts both in one domain, and what is
	// left over is dispatch probing, published rather than absorbed.
	#[test]
	fn handler_attribution_excludes_nested_children_and_leaves_dispatch_as_residual() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		// The dispatcher probes a losing arm for 50us before the winner starts.
		let span = acc.open_handler().expect("frame open");
		// The winning handler runs 800us / 500B, and inside that drives a child scan of 300us / 200B.
		acc.enter();
		assert!(!acc.exit(NodeKind::Scan, 300, 200, 200));
		acc.close_handler(span, 800, Some(500));
		acc.set_route("reoriented_inner_chain_grouped");
		// Node inclusive 900us / 500B: 800 handler + 50 losing probe + 50 dispatcher overhead.
		assert!(acc.exit(NodeKind::Aggregate, 900, 500, 500));

		let invocation = &acc.aggregate_invocations[0];
		// Node exclusive subtracts the child: 900 - 300 = 600.
		assert_eq!(invocation.exclusive_micros, 600);
		// Handler exclusive subtracts the same child: 800 - 300 = 500. Same domain.
		let handler = invocation.handler.expect("attributed");
		assert_eq!(handler.reconciled_micros(), 500);
		// The 100us difference is the losing probe plus dispatcher overhead — a real residual.
		assert_eq!(
			invocation.exclusive_micros - handler.reconciled_micros(),
			100
		);
		// Spill aligns the same way: node 500-200=300, handler 500-200=300, residual 0.
		assert_eq!(invocation.exclusive_spill_bytes, 300);
		assert_eq!(handler.reconciled_spill(), 300);
	}

	// A handler span closed against a frame that is no longer the one it opened on must not attribute
	// to whatever frame happens to be on top.
	#[test]
	fn a_handler_span_refuses_to_attribute_to_a_different_frame() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let span = acc.open_handler().expect("frame open");
		assert!(!acc.exit(NodeKind::Aggregate, 100, 0, 0));
		// A different frame is now open; the stale span must not land on it.
		acc.enter();
		acc.close_handler(span, 999, Some(999));
		assert!(!acc.exit(NodeKind::Aggregate, 200, 0, 0));
		assert!(acc.exit(NodeKind::Other, 500, 0, 0));
		assert!(
			acc.aggregate_invocations[1].handler.is_none(),
			"a mismatched span attributes nothing"
		);
	}

	// The bridge must fail closed on every input it cannot justify. A clamped zero residual would be
	// the worst outcome: it reads as a perfect bridge while hiding a broken measurement.
	#[test]
	fn the_bridge_fails_closed_rather_than_publishing_a_zero_residual() {
		// A handler exceeding its matched node on time is an inverted band, not a zero residual.
		let inverted = AggregateInvocation {
			id: 7,
			route: Some("scan_grouped"),
			exclusive_micros: 100,
			exclusive_spill_bytes: 0,
			handler: Some(HandlerAttribution::Reconciled {
				exclusive_micros: 500,
				exclusive_spill_bytes: 0,
			}),
			vocabulary: None,
			vocabulary_failure: None,
			stages: Vec::new(),
			growth_census: None,
		};
		let value = bridge_value(&inverted);
		assert_eq!(value["status"], "inverted_time_band");
		assert_eq!(value["passed"], false);
		assert!(
			value.get("dispatch_residual_micros").is_none(),
			"no residual is published for a band that cannot be reconciled"
		);

		// A handler exceeding its node on spill is over-attribution, caught by the flow domain.
		let over_spill = AggregateInvocation {
			exclusive_micros: 1_000,
			exclusive_spill_bytes: 10,
			handler: Some(HandlerAttribution::Reconciled {
				exclusive_micros: 900,
				exclusive_spill_bytes: 500,
			}),
			..inverted.clone()
		};
		let value = bridge_value(&over_spill);
		assert_eq!(value["passed"], false);
		assert!(value["spill_error"].is_string(), "names the byte failure");

		// A routed invocation that produced no attribution is a failure, not a blank.
		let missing = AggregateInvocation {
			handler: None,
			..inverted.clone()
		};
		let value = bridge_value(&missing);
		assert_eq!(value["status"], "handler_attribution_missing");
		assert_eq!(value["passed"], false);

		// An unrouted invocation legitimately has nothing to bridge and is not a failure.
		let unrouted = AggregateInvocation {
			route: None,
			handler: None,
			..inverted.clone()
		};
		let value = bridge_value(&unrouted);
		assert_eq!(value["status"], "uncovered_generic_fold");
		assert_eq!(value["passed"], true);

		// A failed attribution carries its reason forward.
		let failed = AggregateInvocation {
			handler: Some(HandlerAttribution::Failed("span smaller than children")),
			..inverted.clone()
		};
		let value = bridge_value(&failed);
		assert_eq!(value["status"], "handler_attribution_failed");
		assert_eq!(value["passed"], false);

		// The healthy case still reconciles, with the residual and boundary published.
		let healthy = AggregateInvocation {
			exclusive_micros: 1_000,
			exclusive_spill_bytes: 300,
			handler: Some(HandlerAttribution::Reconciled {
				exclusive_micros: 994,
				exclusive_spill_bytes: 300,
			}),
			..inverted.clone()
		};
		let value = bridge_value(&healthy);
		assert_eq!(value["status"], "reconciled");
		assert_eq!(value["dispatch_residual_micros"], 6);
		assert_eq!(value["dispatch_residual_spill_bytes"], 0);
		assert_eq!(value["boundary_micros"], DISPATCH_RESIDUAL_BOUNDARY_MICROS);
		assert_eq!(value["passed"], true);
	}

	// A handler span closing against a frame it did not open on is counted, so the anomaly is a
	// number in the profile rather than an invocation that quietly carries no attribution.
	#[test]
	fn a_mismatched_handler_span_is_counted_not_silent() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let span = acc.open_handler().expect("frame open");
		assert!(!acc.exit(NodeKind::Aggregate, 100, 0, 0));
		acc.enter();
		acc.close_handler(span, 999, Some(0));
		assert!(!acc.exit(NodeKind::Aggregate, 200, 0, 0));
		assert!(acc.exit(NodeKind::Other, 500, 0, 0));
		assert_eq!(acc.bridge_mismatches, 1, "the mismatch is counted");
	}

	// A handler whose span is shorter than the children it drove is a broken measurement; the
	// attribution fails by name instead of saturating to zero.
	#[test]
	fn a_handler_span_shorter_than_its_children_fails_by_name() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let span = acc.open_handler().expect("frame open");
		acc.enter();
		assert!(!acc.exit(NodeKind::Scan, 900, 0, 0));
		// The handler claims a 100us span while having driven a 900us child.
		acc.close_handler(span, 100, Some(0));
		acc.set_route("scan_grouped");
		assert!(!acc.exit(NodeKind::Aggregate, 1000, 0, 0));
		assert!(acc.exit(NodeKind::Other, 2000, 0, 0));
		let value = bridge_value(&acc.aggregate_invocations[0]);
		assert_eq!(value["status"], "handler_attribution_failed");
		assert_eq!(value["passed"], false);
	}

	fn invocation_with_stages(
		handler_micros: u64,
		handler_spill: u64,
		vocabulary: Option<Vocabulary>,
		stages: Vec<HandlerStage>,
	) -> AggregateInvocation {
		AggregateInvocation {
			id: 3,
			route: Some("scan_grouped"),
			exclusive_micros: handler_micros + 5,
			exclusive_spill_bytes: handler_spill,
			handler: Some(HandlerAttribution::Reconciled {
				exclusive_micros: handler_micros,
				exclusive_spill_bytes: handler_spill,
			}),
			vocabulary,
			vocabulary_failure: None,
			stages,
			growth_census: None,
		}
	}

	// The innermost link reconciles to the HANDLER-exclusive span, not the node. The node link is the
	// separate bridge; conflating them would let dispatch overhead hide inside the stage residual.
	#[test]
	fn stages_reconcile_to_the_handler_span_not_the_node() {
		let invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_grouped",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Static(&["probe", "fold"]),
				residual_boundary_micros: 100,
			}),
			vec![
				HandlerStage {
					name: "probe",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 600,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "fold",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 350,
						spill_bytes: 0,
					},
				},
			],
		);
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["status"], "reconciled");
		assert_eq!(value["attributed_micros"], 950);
		// Residual is against the handler's 1000us, NOT the node's 1005us.
		assert_eq!(value["handler_exclusive_micros"], 1_000);
		assert_eq!(value["stage_residual_micros"], 50);
		assert_eq!(value["passed"], true);
	}

	// Stage timing is whole microseconds, so a per-batch stage costing well under a tick reads zero on
	// every call. Zero must be published as "below resolution" with the quantization bound that
	// `calls` implies — reporting a bare 0 would read as "this stage is free", which the timer cannot
	// support.
	#[test]
	fn zero_duration_stages_publish_the_resolution_floor_and_their_quantization_bound() {
		let invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_grouped",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Static(&["fold"]),
				residual_boundary_micros: 2_000,
			}),
			vec![
				HandlerStage {
					name: "fold",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 900,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "group_expr_eval",
					calls: 400,
					parent: Some("fold"),
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 0,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "arg_eval_and_readers",
					calls: 400,
					parent: Some("fold"),
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 5,
						spill_bytes: 0,
					},
				},
			],
		);
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["resolution_micros"], 1);
		let stages = value["stages"].as_array().expect("stages array");
		let measured = &stages[0];
		assert_eq!(measured["below_resolution"], false);
		assert_eq!(measured["quantization_dominated"], false);
		// 5us across 400 calls crossed a tick a handful of times; it is still not a measurement of
		// 5us, so it must not read as measured just because it is not exactly zero.
		let barely_nonzero = &stages[2];
		assert_eq!(barely_nonzero["below_resolution"], false);
		assert_eq!(barely_nonzero["quantization_dominated"], true);
		// `calls` bounds the truncation loss, so this stage's true cost is under 5 + 400 us.
		assert_eq!(barely_nonzero["quantization_loss_upper_bound_micros"], 400);
		let truncated = &stages[1];
		assert_eq!(truncated["micros"], 0);
		assert_eq!(truncated["calls"], 400);
		assert_eq!(truncated["below_resolution"], true);
		assert_eq!(truncated["quantization_dominated"], true);
		// One tick per call bounds the loss; for a zero-measured stage that is also the cost bound,
		// so this stage is strictly below 400us rather than zero.
		assert_eq!(truncated["quantization_loss_upper_bound_micros"], 400);
		assert_eq!(truncated["parent"], "fold");
		// A below-resolution stage still reconciles: it consumes none of its parent's budget, and the
		// unmeasured cost stays visible in the refinement residual rather than vanishing.
		assert_eq!(value["passed"], true);
		assert_eq!(value["refinements"][0]["residual_micros"], 895);
	}

	// Fail closed on the two ways stage attribution can be absent or wrong.
	#[test]
	fn stage_attribution_fails_closed_when_missing_or_overrunning() {
		// A routed handler that declared no vocabulary owes attribution it did not provide.
		let missing = invocation_with_stages(1_000, 0, None, Vec::new());
		let value = stages_value(&missing, &[]);
		assert_eq!(value["status"], "stage_attribution_missing");
		assert_eq!(value["passed"], false);

		// Stages summing past the handler span mean a stage counted nested child work.
		let overrun = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_grouped",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Static(&["probe"]),
				residual_boundary_micros: 100,
			}),
			vec![HandlerStage {
				name: "probe",
				calls: 1,
				parent: None,
				replay: None,
				outcome: StageOutcome::Reconciled {
					micros: 5_000,
					spill_bytes: 0,
				},
			}],
		);
		let value = stages_value(&overrun, &[]);
		assert_eq!(value["status"], "stages_exceed_handler_span");
		assert_eq!(value["passed"], false);

		// An unrouted invocation owes nothing.
		let mut unrouted = invocation_with_stages(1_000, 0, None, Vec::new());
		unrouted.route = None;
		assert_eq!(stages_value(&unrouted, &[])["passed"], true);
	}

	fn vocab(spill: SpillCounterKind) -> Vocabulary {
		Vocabulary {
			invocation_id: 3,
			name: "h",
			spill,
			required_stages: RequiredStages::Static(&["write", "read_back"]),
			residual_boundary_micros: 1_000,
		}
	}

	// Each declared spill kind reconciles by its own arithmetic — the declaration selects it. There is
	// no Peak here by design: the handler-level figure is a flow delta, so a max-of-stages would have
	// no same-domain whole to close against.
	#[test]
	fn declared_spill_kind_selects_the_reconciliation_arithmetic() {
		let stages = || {
			vec![
				HandlerStage {
					name: "write",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 100,
						spill_bytes: 400,
					},
				},
				HandlerStage {
					name: "read_back",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 100,
						spill_bytes: 600,
					},
				},
			]
		};
		// Flow: parts sum to the handler's 1000B flow delta exactly.
		let flow =
			invocation_with_stages(1_000, 1_000, Some(vocab(SpillCounterKind::Flow)), stages());
		let value = stages_value(&flow, &[]);
		assert_eq!(value["stage_spill_total"], 1_000);
		assert_eq!(value["spill_counter_kind"], "flow");
		assert_eq!(value["arithmetic_reconciled"], true);

		// Subset: stages cover 1000B of a wider 5000B handler delta, with a vocabulary-specific reason.
		let subset = invocation_with_stages(
			1_000,
			5_000,
			Some(vocab(SpillCounterKind::Subset(
				"the remainder is written by the shared reservoir writer, attributed at the node",
			))),
			stages(),
		);
		let value = stages_value(&subset, &[]);
		assert_eq!(value["stage_spill_total"], 1_000);
		assert_eq!(value["spill_counter_kind"], "subset");
		assert_eq!(value["arithmetic_reconciled"], true);

		// A flow vocabulary whose parts overrun the handler's bytes is over-attribution, not slack.
		let bad_flow =
			invocation_with_stages(1_000, 100, Some(vocab(SpillCounterKind::Flow)), stages());
		let value = stages_value(&bad_flow, &[]);
		assert_eq!(value["arithmetic_reconciled"], false);
		assert_eq!(value["passed"], false);
		assert!(value["spill_error"].is_string());
	}

	// Hole 2: arithmetic and coverage are independent verdicts. A vocabulary that records nothing
	// reconciles perfectly and explains nothing — that must not read as a pass.
	#[test]
	fn empty_or_partial_coverage_fails_even_when_the_arithmetic_reconciles() {
		// No stages at all: the whole handler span is residual. Arithmetic fine, coverage absent.
		let empty =
			invocation_with_stages(1_000, 0, Some(vocab(SpillCounterKind::Flow)), Vec::new());
		let value = stages_value(&empty, &[]);
		assert_eq!(
			value["arithmetic_reconciled"], true,
			"nothing to contradict"
		);
		assert_eq!(value["coverage_complete"], false, "but it explains nothing");
		assert_eq!(value["passed"], false);
		assert_eq!(value["stage_residual_micros"], 1_000);
		assert_eq!(
			value["missing_required_stages"],
			serde_json::json!(["write", "read_back"])
		);

		// One promised stage observed, one missing: still incomplete.
		let partial = invocation_with_stages(
			1_000,
			0,
			Some(vocab(SpillCounterKind::Flow)),
			vec![HandlerStage {
				name: "write",
				calls: 1,
				parent: None,
				replay: None,
				outcome: StageOutcome::Reconciled {
					micros: 999,
					spill_bytes: 0,
				},
			}],
		);
		let value = stages_value(&partial, &[]);
		assert_eq!(value["coverage_complete"], false);
		assert_eq!(
			value["missing_required_stages"],
			serde_json::json!(["read_back"])
		);

		// All stages present but a residual past the declared boundary is also incomplete coverage.
		let leaky = invocation_with_stages(
			10_000,
			0,
			Some(vocab(SpillCounterKind::Flow)),
			vec![
				HandlerStage {
					name: "write",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 100,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "read_back",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 100,
						spill_bytes: 0,
					},
				},
			],
		);
		let value = stages_value(&leaky, &[]);
		assert_eq!(value["missing_required_stages"], serde_json::json!([]));
		assert_eq!(value["within_residual_boundary"], false);
		assert_eq!(value["coverage_complete"], false);
		assert_eq!(value["passed"], false);

		// Complete coverage inside the boundary passes on both verdicts.
		let complete = invocation_with_stages(
			1_000,
			0,
			Some(vocab(SpillCounterKind::Flow)),
			vec![
				HandlerStage {
					name: "write",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 500,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "read_back",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 499,
						spill_bytes: 0,
					},
				},
			],
		);
		let value = stages_value(&complete, &[]);
		assert_eq!(value["coverage_complete"], true);
		assert_eq!(value["passed"], true);
	}

	// A failed stage must not satisfy its name in the required set, and must fail the invocation's
	// arithmetic verdict — not merely bump a global counter no verdict reads.
	#[test]
	fn a_failed_stage_neither_satisfies_coverage_nor_passes_arithmetic() {
		let failed = invocation_with_stages(
			1_000,
			0,
			Some(vocab(SpillCounterKind::Flow)),
			vec![
				HandlerStage {
					name: "write",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 500,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "read_back",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Failed(
						"stage span is smaller than the children it drove",
					),
				},
			],
		);
		let value = stages_value(&failed, &[]);
		assert_eq!(
			value["arithmetic_reconciled"], false,
			"a failed stage fails arithmetic"
		);
		assert_eq!(
			value["missing_required_stages"],
			serde_json::json!(["read_back"]),
			"a failed stage does not satisfy its name"
		);
		assert_eq!(value["passed"], false);
		assert_eq!(value["failed_stages"][0]["stage"], "read_back");
		// Its time contributes nothing, so it cannot inflate the attributed total either.
		assert_eq!(value["attributed_micros"], 500);
	}

	// A stage orphaned by an already-exited invocation still lands on that invocation's record.
	#[test]
	fn an_orphaned_stage_failure_fails_its_invocation() {
		let complete = invocation_with_stages(
			1_000,
			0,
			Some(vocab(SpillCounterKind::Flow)),
			vec![
				HandlerStage {
					name: "write",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 500,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "read_back",
					calls: 1,
					parent: None,
					replay: None,
					outcome: StageOutcome::Reconciled {
						micros: 499,
						spill_bytes: 0,
					},
				},
			],
		);
		// Without the orphan this invocation passes outright.
		assert_eq!(stages_value(&complete, &[])["passed"], true);
		// With one, the same invocation fails — the counter is not merely decorative.
		let value = stages_value(&complete, &["read_back"]);
		assert_eq!(value["arithmetic_reconciled"], false);
		assert_eq!(value["passed"], false);
	}

	// close_stage must not clamp: a stage whose span is smaller than the children it drove is Failed.
	#[test]
	fn a_stage_shorter_than_its_children_is_failed_not_zeroed() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let token = StageToken {
			name: "probe",
			parent: None,
			replay: None,
			invocation_id: 1,
			children_micros_at_open: 0,
			children_spill_at_open: 0,
			started: Instant::now(),
			spill_before: 0,
		};
		// A child of 900us runs under the stage; the stage then reports a 100us inclusive span.
		acc.enter();
		assert!(!acc.exit(NodeKind::Scan, 900, 0, 0));
		acc.close_stage(token, 100, Some(0));
		let frame_stage = acc.stack.last().expect("frame").stages[0];
		assert!(
			matches!(frame_stage.outcome, StageOutcome::Failed(_)),
			"an impossible exclusivity is Failed, never a clamped zero"
		);
		assert!(frame_stage.outcome.reconciled().is_none());
	}

	// A stage opened under one vocabulary cannot be opened against a different invocation.
	#[test]
	fn a_stage_refuses_to_open_against_another_invocation() {
		let handle = VocabularyHandle { invocation_id: 42 };
		let mut acc = NodeAccounting::default();
		acc.enter();
		// The live frame is id 0, not 42 — the guard is the id comparison open_stage performs.
		assert_ne!(acc.stack.last().expect("frame").id, handle.invocation_id);
		// And a close for a vanished invocation orphans rather than landing on the wrong frame.
		let token = StageToken {
			name: "probe",
			parent: None,
			replay: None,
			invocation_id: 42,
			children_micros_at_open: 0,
			children_spill_at_open: 0,
			started: Instant::now(),
			spill_before: 0,
		};
		acc.close_stage(token, 100, Some(0));
		assert_eq!(acc.stage_mismatches, 1);
		assert_eq!(acc.orphaned_stage_failures, vec![(42, "probe")]);
		assert!(acc.stack.last().expect("frame").stages.is_empty());
	}

	fn stage_token(name: &'static str, invocation_id: u64) -> StageToken {
		StageToken {
			name,
			parent: None,
			replay: None,
			invocation_id,
			children_micros_at_open: 0,
			children_spill_at_open: 0,
			started: Instant::now(),
			spill_before: 0,
		}
	}

	// An unreadable spill counter is not zero bytes. It fails the stage by name rather than recording
	// a confident zero that would satisfy coverage.
	#[test]
	fn an_unreadable_spill_delta_fails_the_stage() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.close_stage(stage_token("probe", 0), 100, None);
		let stage = acc.stack.last().expect("frame").stages[0];
		assert!(matches!(stage.outcome, StageOutcome::Failed(_)));
		assert!(stage.outcome.reconciled().is_none());
	}

	// Closing a stage while a child frame is still open would leave the child's work inside the
	// stage's span. That must fail immediately for the right invocation, not compute a figure that
	// reads as reconciled while the global counter quietly ticks.
	#[test]
	fn closing_a_stage_over_an_open_child_fails_that_invocation() {
		let mut acc = NodeAccounting::default();
		acc.enter(); // invocation, id 0
		acc.enter(); // a child frame that has NOT closed
		acc.close_stage(stage_token("probe", 0), 100, Some(0));
		assert_eq!(acc.stage_mismatches, 1);
		// The failure lands on the invocation's frame (index 0), not the open child.
		let stage = acc.stack[0].stages[0];
		assert!(
			matches!(stage.outcome, StageOutcome::Failed(_)),
			"a stage spanning an open child is failed, never reconciled"
		);
		assert!(acc.stack[1].stages.is_empty(), "the child records nothing");
	}

	// A repeated stage whose totals overflow becomes Failed rather than clamping to u64::MAX.
	#[test]
	fn a_repeated_stage_that_overflows_becomes_failed() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.close_stage(stage_token("probe", 0), u64::MAX, Some(0));
		acc.close_stage(stage_token("probe", 0), 10, Some(0));
		let stage = acc.stack.last().expect("frame").stages[0];
		assert!(
			matches!(stage.outcome, StageOutcome::Failed(_)),
			"an overflowing merge is failed, not saturated"
		);
	}

	// Two-level: refinements reconcile to their PARENT stage, not the handler. Counting them at the
	// top level would double the parent they sit inside, so the split is what makes the second level
	// safe to add at all.
	#[test]
	fn refinements_reconcile_to_their_parent_stage_not_the_handler() {
		let mut invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_grouped_generic",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Static(&["fold"]),
				residual_boundary_micros: 100,
			}),
			vec![HandlerStage {
				name: "fold",
				calls: 1,
				parent: None,
				replay: None,
				outcome: StageOutcome::Reconciled {
					micros: 950,
					spill_bytes: 0,
				},
			}],
		);
		// Three refinements inside the 950us fold, summing to 900us.
		for (name, micros) in [
			("group_expr", 300u64),
			("arg_eval", 200),
			("row_update", 400),
		] {
			invocation.stages.push(HandlerStage {
				name,
				calls: 1,
				parent: Some("fold"),
				replay: None,
				outcome: StageOutcome::Reconciled {
					micros,
					spill_bytes: 0,
				},
			});
		}
		let value = stages_value(&invocation, &[]);
		// The top level still sees only `fold` — refinements do not inflate it.
		assert_eq!(value["attributed_micros"], 950);
		assert_eq!(value["stage_residual_micros"], 50);
		let refinement = &value["refinements"][0];
		assert_eq!(refinement["parent"], "fold");
		assert_eq!(refinement["attributed_micros"], 900);
		assert_eq!(refinement["parent_micros"], 950);
		assert_eq!(refinement["residual_micros"], 50);
		assert_eq!(value["refinements_reconciled"], true);
		assert_eq!(value["passed"], true);

		// Refinements overrunning their parent fail the invocation, not just the refinement block.
		invocation.stages.push(HandlerStage {
			name: "overrun",
			calls: 1,
			parent: Some("fold"),
			replay: None,
			outcome: StageOutcome::Reconciled {
				micros: 5_000,
				spill_bytes: 0,
			},
		});
		let value = stages_value(&invocation, &[]);
		assert_eq!(
			value["refinements"][0]["status"],
			"refinement_exceeds_parent_stage"
		);
		assert_eq!(value["refinements_reconciled"], false);
		assert_eq!(value["passed"], false);
	}

	// A route stamped while a descendant frame is open must not land on the descendant. The
	// dispatcher stamps only after its arm's children have popped, so the top frame is the routed
	// node; this pins that the stamp follows the stack rather than the most recent call.
	#[test]
	fn a_route_lands_on_the_open_frame_not_a_completed_child() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		assert!(!acc.exit(NodeKind::Scan, 100, 0, 0));
		// Child popped; the Aggregate frame is open again and is what gets stamped.
		acc.set_route("scan_grouped");
		assert!(acc.exit(NodeKind::Aggregate, 500, 0, 0));
		assert_eq!(acc.aggregate_invocations.len(), 1);
		assert_eq!(acc.aggregate_invocations[0].route, Some("scan_grouped"));
	}

	// ---- Acceptance for the replay-scoped two-phase contract (ADR 508 step 2 refactor) ----

	fn variant(name: &'static str) -> VocabularyVariant {
		VocabularyVariant {
			name,
			spill: SpillCounterKind::Flow,
			required_stages: RequiredStages::Static(&[]),
			residual_boundary_micros: 1_000,
		}
	}

	fn replayed(name: &'static str, ids: &[u64]) -> Vec<RequiredStage> {
		ids.iter()
			.map(|id| RequiredStage {
				name,
				replay: Some(*id),
			})
			.collect()
	}

	fn replay_stage(name: &'static str, replay: u64, micros: u64, spill: u64) -> HandlerStage {
		HandlerStage {
			name,
			calls: 1,
			parent: None,
			replay: Some(replay),
			outcome: StageOutcome::Reconciled {
				micros,
				spill_bytes: spill,
			},
		}
	}

	fn replay_token(name: &'static str, invocation_id: u64, replay: u64) -> StageToken {
		StageToken {
			name,
			parent: None,
			replay: Some(replay),
			invocation_id,
			children_micros_at_open: 0,
			children_spill_at_open: 0,
			started: Instant::now(),
			spill_before: 0,
		}
	}

	// Acceptance 1: one observed replay must not satisfy a six-replay requirement, and the failure
	// names the exact missing identities — an actionable verdict, not a count.
	#[test]
	fn one_observed_replay_fails_a_six_replay_requirement_naming_the_missing_ids() {
		let invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_probe_base",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Owned(replayed(
					"suffix_probe",
					&[0, 1, 2, 3, 4, 5],
				)),
				residual_boundary_micros: 1_000,
			}),
			vec![replay_stage("suffix_probe", 2, 100, 0)],
		);
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["coverage_complete"], false);
		assert_eq!(value["passed"], false);
		assert_eq!(
			value["missing_required_stages"],
			serde_json::json!([
				{"stage": "suffix_probe", "replay_id": 0},
				{"stage": "suffix_probe", "replay_id": 1},
				{"stage": "suffix_probe", "replay_id": 3},
				{"stage": "suffix_probe", "replay_id": 4},
				{"stage": "suffix_probe", "replay_id": 5},
			]),
			"the five unobserved replays are named exactly"
		);
	}

	// Acceptance 2: coverage is a set over replay identities, not a sequence — observing every
	// expected id out of completion order passes.
	#[test]
	fn all_expected_replays_observed_out_of_completion_order_pass() {
		let invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_probe_base",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Owned(replayed("suffix_probe", &[0, 1, 2, 3])),
				residual_boundary_micros: 1_000,
			}),
			// Completion order 3, 0, 2, 1 — the record order the frame saw.
			vec![
				replay_stage("suffix_probe", 3, 100, 0),
				replay_stage("suffix_probe", 0, 200, 0),
				replay_stage("suffix_probe", 2, 300, 0),
				replay_stage("suffix_probe", 1, 150, 0),
			],
		);
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["missing_required_stages"], serde_json::json!([]));
		assert_eq!(value["coverage_complete"], true);
		assert_eq!(value["passed"], true);
	}

	// Acceptance 3: an unknown, missing, or conflicting variant commit fails the invocation closed —
	// the misuse dominates whatever definition survived it.
	#[test]
	fn an_unknown_missing_or_conflicting_variant_commit_fails_closed() {
		// Unknown: commit names a variant that was never predeclared.
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let id = acc
			.predeclare(vec![variant("scan_probe_base")])
			.expect("predeclared");
		acc.commit_vocabulary(id, "not_predeclared");
		acc.set_route("reoriented_inner_chain_grouped");
		assert!(!acc.exit(NodeKind::Aggregate, 100, 0, 0));
		assert!(acc.exit(NodeKind::Other, 200, 0, 0));
		let value = stages_value(&acc.aggregate_invocations[0], &[]);
		assert_eq!(value["status"], "vocabulary_commit_failed");
		assert_eq!(value["passed"], false);

		// Missing: commit with nothing predeclared.
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		acc.commit_vocabulary(1, "scan_probe_base");
		assert!(!acc.exit(NodeKind::Aggregate, 100, 0, 0));
		assert!(acc.exit(NodeKind::Other, 200, 0, 0));
		let value = stages_value(&acc.aggregate_invocations[0], &[]);
		assert_eq!(value["status"], "vocabulary_commit_failed");
		assert_eq!(value["passed"], false);

		// Conflicting: a second commit against an already-committed vocabulary.
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let id = acc
			.predeclare(vec![variant("scan_probe_base")])
			.expect("predeclared");
		acc.commit_vocabulary(id, "scan_probe_base");
		assert!(
			acc.stack.last().expect("frame").vocabulary.is_some(),
			"the first commit lands"
		);
		acc.commit_vocabulary(id, "scan_probe_base");
		assert!(!acc.exit(NodeKind::Aggregate, 100, 0, 0));
		assert!(acc.exit(NodeKind::Other, 200, 0, 0));
		let value = stages_value(&acc.aggregate_invocations[0], &[]);
		assert_eq!(value["status"], "vocabulary_commit_failed");
		assert_eq!(value["passed"], false);

		// And a commit whose invocation already exited still fails that invocation's record.
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let id = acc
			.predeclare(vec![variant("scan_probe_base")])
			.expect("predeclared");
		assert!(!acc.exit(NodeKind::Aggregate, 100, 0, 0));
		acc.commit_vocabulary(id, "scan_probe_base");
		assert!(acc.exit(NodeKind::Other, 200, 0, 0));
		let value = stages_value(&acc.aggregate_invocations[0], &[]);
		assert_eq!(value["status"], "vocabulary_commit_failed");
		assert_eq!(value["passed"], false);
	}

	// Acceptance 4: an attempted branch that declines never commits, and the stages it buffered are
	// never published — the invocation reports explicit missing attribution, not a borrowed lifecycle.
	#[test]
	fn an_attempted_branch_that_declines_is_never_committed() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.enter();
		let span = acc.open_handler().expect("frame open");
		let id = acc
			.predeclare(vec![variant("scan_probe_base")])
			.expect("predeclared");
		// Runtime preparation is measured while the declaration is pending...
		acc.close_stage(replay_token("runtime_prep", id, 0), 40, Some(0));
		let frame = acc.stack.last().expect("frame");
		assert!(
			frame.stages.is_empty(),
			"a pending stage is buffered, not on the frame"
		);
		assert_eq!(frame.pending.as_ref().expect("pending").stages.len(), 1);
		// ...then the branch declines: no commit. A different arm wins the invocation.
		acc.close_handler(span, 100, Some(0));
		acc.set_route("reoriented_inner_chain_grouped");
		assert!(!acc.exit(NodeKind::Aggregate, 200, 0, 0));
		assert!(acc.exit(NodeKind::Other, 300, 0, 0));
		let invocation = &acc.aggregate_invocations[0];
		assert!(invocation.vocabulary.is_none(), "nothing was committed");
		assert!(
			invocation.vocabulary_failure.is_none(),
			"declining is not a misuse"
		);
		assert!(
			invocation.stages.is_empty(),
			"buffered stages of an uncommitted attempt are never published"
		);
		let value = stages_value(invocation, &[]);
		assert_eq!(value["status"], "stage_attribution_missing");
		assert_eq!(value["passed"], false);
	}

	// Acceptance 5: repeated stages merge only within one replay identity — instances of one name
	// across replays stay distinct records.
	#[test]
	fn repeated_stages_merge_only_within_the_same_replay_id() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		acc.close_stage(replay_token("suffix_probe", 0, 1), 100, Some(0));
		acc.close_stage(replay_token("suffix_probe", 0, 1), 40, Some(0));
		acc.close_stage(replay_token("suffix_probe", 0, 2), 7, Some(0));
		let stages = &acc.stack.last().expect("frame").stages;
		assert_eq!(stages.len(), 2, "two replay identities, two records");
		assert_eq!(stages[0].replay, Some(1));
		assert_eq!(stages[0].calls, 2);
		assert_eq!(stages[0].outcome.reconciled(), Some((140, 0)));
		assert_eq!(stages[1].replay, Some(2));
		assert_eq!(stages[1].calls, 1);
		assert_eq!(stages[1].outcome.reconciled(), Some((7, 0)));
	}

	// Acceptance 6: a refinement reconciles against the parent instance carrying its own replay id.
	// Name-only lookup would check every child against the first matching parent and pass both; the
	// replay-scoped lookup fails the child that overruns its own replay's parent.
	#[test]
	fn refinements_reconcile_against_the_matching_replay_scoped_parent() {
		let mut invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_probe_base",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Owned(replayed("suffix_probe", &[1, 2])),
				residual_boundary_micros: 1_000,
			}),
			vec![
				replay_stage("suffix_probe", 1, 500, 0),
				replay_stage("suffix_probe", 2, 100, 0),
			],
		);
		for replay in [1u64, 2] {
			invocation.stages.push(HandlerStage {
				name: "probe_filter",
				calls: 1,
				parent: Some("suffix_probe"),
				replay: Some(replay),
				outcome: StageOutcome::Reconciled {
					micros: 450,
					spill_bytes: 0,
				},
			});
		}
		let value = stages_value(&invocation, &[]);
		let refinements = value["refinements"].as_array().expect("refinements");
		assert_eq!(refinements.len(), 2, "one link per parent instance");
		let of_replay = |id: u64| {
			refinements
				.iter()
				.find(|entry| entry["replay_id"] == id)
				.expect("entry")
		};
		// 450 inside replay 1's 500us parent reconciles...
		assert_eq!(of_replay(1)["status"], "reconciled");
		assert_eq!(of_replay(1)["residual_micros"], 50);
		// ...while the same 450 against replay 2's 100us parent is an overrun, which name-only
		// parent lookup (finding replay 1's 500us first) would have hidden.
		assert_eq!(of_replay(2)["status"], "refinement_exceeds_parent_stage");
		assert_eq!(value["refinements_reconciled"], false);
		assert_eq!(value["passed"], false);
	}

	// Acceptance 7: spill stays one Flow domain across the whole invocation — every replay instance's
	// bytes sum against the handler's flow delta.
	#[test]
	fn flow_spill_sums_across_every_replay_instance() {
		let invocation = invocation_with_stages(
			1_000,
			1_000,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_probe_base",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Owned(replayed("suffix_probe", &[1, 2])),
				residual_boundary_micros: 1_000,
			}),
			vec![
				replay_stage("suffix_probe", 1, 100, 400),
				replay_stage("suffix_probe", 2, 100, 600),
			],
		);
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["stage_spill_total"], 1_000);
		assert_eq!(value["arithmetic_reconciled"], true);
		assert_eq!(value["passed"], true);
	}

	// Acceptance 8: the static scan_grouped declarations keep their behaviour — unreplayed coverage,
	// bare-string requirement names, and the same verdicts.
	#[test]
	fn static_scan_grouped_declarations_keep_their_behaviour() {
		let vocabulary = || Vocabulary {
			invocation_id: 3,
			name: "scan_grouped_generic_no_projection",
			spill: SpillCounterKind::Flow,
			required_stages: RequiredStages::Static(&["stream_pull", "fold", "finalize"]),
			residual_boundary_micros: 1_000,
		};
		let stage = |name: &'static str, micros: u64| HandlerStage {
			name,
			calls: 1,
			parent: None,
			replay: None,
			outcome: StageOutcome::Reconciled {
				micros,
				spill_bytes: 0,
			},
		};
		let complete = invocation_with_stages(
			1_000,
			0,
			Some(vocabulary()),
			vec![
				stage("stream_pull", 300),
				stage("fold", 500),
				stage("finalize", 150),
			],
		);
		let value = stages_value(&complete, &[]);
		assert_eq!(value["passed"], true);
		assert_eq!(
			value["required_stages"],
			serde_json::json!(["stream_pull", "fold", "finalize"]),
			"an unreplayed requirement stays the bare string it has always been"
		);
		let partial = invocation_with_stages(
			1_000,
			0,
			Some(vocabulary()),
			vec![stage("stream_pull", 300), stage("fold", 500)],
		);
		let value = stages_value(&partial, &[]);
		assert_eq!(
			value["missing_required_stages"],
			serde_json::json!(["finalize"])
		);
		assert_eq!(value["passed"], false);
	}

	#[test]
	fn shared_refinements_require_their_unscoped_parent() {
		assert!(
			RequiredStages::Static(&["stream_pull", "fold", "finalize"]).contains_unscoped("fold")
		);
		assert!(!RequiredStages::Static(&["deferred_replay"]).contains_unscoped("fold"));
		assert!(
			!RequiredStages::Owned(vec![RequiredStage {
				name: "fold",
				replay: Some(1),
			}])
			.contains_unscoped("fold")
		);
	}

	// Acceptance 9: a disabled run constructs no requirement lists — the builder must not even run,
	// so the dynamic path costs nothing when attribution is off.
	#[test]
	fn dynamic_requirement_construction_allocates_nothing_when_disabled() {
		let mut built = false;
		let handle = predeclare_stage_vocabulary(|| {
			built = true;
			vec![variant("scan_probe_base")]
		});
		assert!(handle.is_none(), "no attribution, no handle");
		assert!(!built, "the requirement builder never ran");
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
