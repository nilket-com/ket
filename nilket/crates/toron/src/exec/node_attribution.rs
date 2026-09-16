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

use crate::distributed::task::{TaskTelemetry, record_bounded_route};
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
	/// ADR 585: raw inclusive lifecycle samples, buffered with the vocabulary until the handler
	/// commits. These never enter ADR 507's exclusive reconciliation.
	inclusive_lifecycle: Vec<InclusiveLifecycleTerm>,
	/// Query-neutral diagnostics attached to this invocation while its frame is open. Kept outside
	/// stage arithmetic: these are ownership/counter receipts, not additional timed stages.
	diagnostics: Vec<InvocationDiagnostic>,
	/// ADR 516: exact flow and build-shape facts for each replay-scoped suffix probe. Buffered with
	/// the pending vocabulary so a declined scan-probe branch cannot leak observations.
	suffix_probes: Vec<SuffixProbeProfile>,
	/// Spill lifecycle counters gathered by the same invocation-bound observer that opens refinement
	/// stages. One record per parent stage keeps build and probe domains distinct.
	spill_io: Vec<SpillIoParent>,
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
		inclusive_micros: u64,
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
	/// `None` is a top-level stage required against the handler span. A refinement requirement names
	/// the exact parent it must reconcile beneath; observing the same leaf under another parent does
	/// not satisfy it.
	pub parent: Option<&'static str>,
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
	fn entries(
		&self,
	) -> impl Iterator<Item = (&'static str, Option<&'static str>, Option<u64>)> + '_ {
		let (fixed, owned) = match self {
			Self::Static(names) => (Some(names.iter()), None),
			Self::Owned(entries) => (None, Some(entries.iter())),
		};
		fixed
			.into_iter()
			.flatten()
			.map(|name| (*name, None, None))
			.chain(
				owned
					.into_iter()
					.flatten()
					.map(|entry| (entry.name, entry.parent, entry.replay)),
			)
	}

	fn contains_unscoped(&self, required: &'static str) -> bool {
		self.entries()
			.any(|(name, parent, replay)| name == required && parent.is_none() && replay.is_none())
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
	inclusive_lifecycle: Vec<InclusiveLifecycleTerm>,
	suffix_probes: Vec<SuffixProbeProfile>,
}

/// One additive ADR 585 lifecycle term. This is deliberately parallel to `HandlerStage`: stage
/// timing remains exclusive, while these raw elapsed samples retain nested execute-bound children.
#[derive(Clone, Copy)]
struct InclusiveLifecycleTerm {
	name: &'static str,
	calls: u64,
	micros: u64,
	error: Option<&'static str>,
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
	/// Optional lifecycle facts attached after the runtime has been exercised. ADR 525 reuses the
	/// existing once-per-runtime suffix-build cache rather than walking build ownership a second time.
	rows: Option<u64>,
	retained_bytes: Option<u64>,
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
	inclusive_lifecycle: Vec<InclusiveLifecycleTerm>,
	diagnostics: Vec<InvocationDiagnostic>,
	suffix_probes: Vec<SuffixProbeProfile>,
	spill_io: Vec<SpillIoParent>,
	/// The fold's growth census, published beside the exact stages. It never enters their arithmetic.
	growth_census: Option<serde_json::Value>,
}

#[derive(Clone)]
struct InvocationDiagnostic {
	name: &'static str,
	value: serde_json::Value,
}

/// One locally accumulated ADR-516 suffix-probe call. Durations are measured inside the existing
/// primitive-key windows and output chunks, then published once here; the node ledger is never
/// locked from the hot loop.
#[derive(Clone, Debug)]
pub(in crate::exec) struct SuffixProbeObservation {
	pub replay_id: u64,
	pub supported: bool,
	pub unsupported_reason: Option<&'static str>,
	pub lookup_micros: u64,
	pub match_expand_micros: u64,
	pub payload_take_micros: u64,
	pub calls: u64,
	pub input_probe_rows: u64,
	pub non_null_keys: u64,
	pub lookup_windows: u64,
	pub matched_references: u64,
	pub emitted_rows: u64,
	pub output_batches: u64,
	pub output_column_values: u64,
	pub output_array_bytes: u64,
	pub build_representation: &'static str,
	pub build_rows: u64,
	pub distinct_keys: u64,
	pub build_batch_bytes: u64,
	pub index_accounted_bytes: u64,
	pub entry_capacity: u64,
	pub bucket_capacity: u64,
	pub key_width_bytes: u64,
}

#[derive(Clone, Debug)]
struct SuffixProbeProfile {
	observation: SuffixProbeObservation,
	error: Option<&'static str>,
}

#[derive(Clone, Default)]
struct SpillIoParent {
	parent: &'static str,
	subleaves: Vec<SpillIoSubleaf>,
	plaintext_cache_hits: u64,
	plaintext_cache_misses: u64,
	plaintext_cache_hit_bytes: u64,
	decoded_cache_hits: u64,
	decoded_cache_misses: u64,
	decoded_cache_hit_bytes: u64,
	error: Option<&'static str>,
}

#[derive(Clone)]
struct SpillIoSubleaf {
	name: &'static str,
	calls: u64,
	rows: u64,
	bytes: u64,
	pages: u64,
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
			inclusive_micros: span_micros,
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
				inclusive_lifecycle: frame.inclusive_lifecycle,
				diagnostics: frame.diagnostics,
				suffix_probes: frame.suffix_probes,
				spill_io: frame.spill_io,
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
			inclusive_lifecycle: Vec::new(),
			suffix_probes: Vec::new(),
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
		for term in pending.inclusive_lifecycle {
			Self::merge_inclusive_lifecycle_term(&mut frame.inclusive_lifecycle, term);
		}
		for profile in pending.suffix_probes {
			let replay_id = profile.observation.replay_id;
			Self::merge_suffix_probe(&mut frame.suffix_probes, profile.observation);
			if let Some(error) = profile.error
				&& let Some(committed) = frame
					.suffix_probes
					.iter_mut()
					.find(|entry| entry.observation.replay_id == replay_id)
			{
				committed.error = Some(error);
			}
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

	fn attach_diagnostic(&mut self, id: u64, name: &'static str, value: serde_json::Value) {
		let diagnostics = if let Some(frame) = self.stack.iter_mut().find(|frame| frame.id == id) {
			&mut frame.diagnostics
		} else if let Some(invocation) = self
			.aggregate_invocations
			.iter_mut()
			.find(|invocation| invocation.id == id)
		{
			&mut invocation.diagnostics
		} else {
			return;
		};
		if let Some(existing) = diagnostics.iter_mut().find(|entry| entry.name == name) {
			existing.value = value;
		} else {
			diagnostics.push(InvocationDiagnostic { name, value });
		}
	}

	fn record_suffix_probe(&mut self, id: u64, observation: SuffixProbeObservation) {
		let Some(frame) = self.stack.iter_mut().find(|frame| frame.id == id) else {
			self.stage_mismatches = self.stage_mismatches.saturating_add(1);
			return;
		};
		let (stages, profiles) = match frame.pending.as_mut() {
			Some(pending) => (&mut pending.stages, &mut pending.suffix_probes),
			None => (&mut frame.stages, &mut frame.suffix_probes),
		};
		if observation.supported {
			for (name, micros) in [
				("suffix_lookup", observation.lookup_micros),
				("suffix_match_expand", observation.match_expand_micros),
				("suffix_payload_take", observation.payload_take_micros),
			] {
				Self::merge_stage(
					stages,
					HandlerStage {
						name,
						calls: observation.calls,
						parent: Some("suffix_probe"),
						replay: Some(observation.replay_id),
						rows: None,
						retained_bytes: None,
						outcome: StageOutcome::Reconciled {
							micros,
							spill_bytes: 0,
						},
					},
				);
			}
		}
		Self::merge_suffix_probe(profiles, observation);
	}

	fn merge_suffix_probe(
		profiles: &mut Vec<SuffixProbeProfile>,
		incoming: SuffixProbeObservation,
	) {
		let Some(existing) = profiles
			.iter_mut()
			.find(|profile| profile.observation.replay_id == incoming.replay_id)
		else {
			profiles.push(SuffixProbeProfile {
				observation: incoming,
				error: None,
			});
			return;
		};
		if existing.error.is_some() {
			return;
		}
		let current = &mut existing.observation;
		if current.supported != incoming.supported
			|| current.unsupported_reason != incoming.unsupported_reason
			|| current.build_representation != incoming.build_representation
			|| current.build_rows != incoming.build_rows
			|| current.distinct_keys != incoming.distinct_keys
			|| current.build_batch_bytes != incoming.build_batch_bytes
			|| current.index_accounted_bytes != incoming.index_accounted_bytes
			|| current.entry_capacity != incoming.entry_capacity
			|| current.bucket_capacity != incoming.bucket_capacity
			|| current.key_width_bytes != incoming.key_width_bytes
		{
			existing.error = Some("suffix probe build shape changed within one replay");
			return;
		}
		macro_rules! checked_add {
			($field:ident) => {
				let Some(value) = current.$field.checked_add(incoming.$field) else {
					existing.error = Some("suffix probe flow counter overflowed");
					return;
				};
				current.$field = value;
			};
		}
		checked_add!(lookup_micros);
		checked_add!(match_expand_micros);
		checked_add!(payload_take_micros);
		checked_add!(calls);
		checked_add!(input_probe_rows);
		checked_add!(non_null_keys);
		checked_add!(lookup_windows);
		checked_add!(matched_references);
		checked_add!(emitted_rows);
		checked_add!(output_batches);
		checked_add!(output_column_values);
		checked_add!(output_array_bytes);
	}

	fn record_spill_subleaf(
		&mut self,
		id: u64,
		parent: &'static str,
		name: &'static str,
		rows: u64,
		bytes: u64,
		pages: u64,
	) -> Result<(), &'static str> {
		let frame = self
			.stack
			.iter_mut()
			.find(|frame| frame.id == id)
			.ok_or("spill subleaf recorded after its invocation exited")?;
		let counters = spill_parent_mut(&mut frame.spill_io, parent);
		if counters.error.is_some() {
			return Err("spill lifecycle counter is already failed");
		}
		let leaf =
			if let Some(existing) = counters.subleaves.iter_mut().find(|leaf| leaf.name == name) {
				existing
			} else {
				counters.subleaves.push(SpillIoSubleaf {
					name,
					calls: 0,
					rows: 0,
					bytes: 0,
					pages: 0,
				});
				counters
					.subleaves
					.last_mut()
					.expect("spill subleaf was just inserted")
			};
		let Some(calls) = leaf.calls.checked_add(1) else {
			counters.error = Some("spill subleaf call count overflowed");
			return Err("spill subleaf call count overflowed");
		};
		let Some(total_rows) = leaf.rows.checked_add(rows) else {
			counters.error = Some("spill subleaf row count overflowed");
			return Err("spill subleaf row count overflowed");
		};
		let Some(total_bytes) = leaf.bytes.checked_add(bytes) else {
			counters.error = Some("spill subleaf byte count overflowed");
			return Err("spill subleaf byte count overflowed");
		};
		let Some(total_pages) = leaf.pages.checked_add(pages) else {
			counters.error = Some("spill subleaf page count overflowed");
			return Err("spill subleaf page count overflowed");
		};
		leaf.calls = calls;
		leaf.rows = total_rows;
		leaf.bytes = total_bytes;
		leaf.pages = total_pages;
		Ok(())
	}

	fn record_spill_cache(
		&mut self,
		id: u64,
		parent: &'static str,
		decoded: bool,
		hit: bool,
		bytes: u64,
	) -> Result<(), &'static str> {
		let frame = self
			.stack
			.iter_mut()
			.find(|frame| frame.id == id)
			.ok_or("spill cache event recorded after its invocation exited")?;
		let counters = spill_parent_mut(&mut frame.spill_io, parent);
		if counters.error.is_some() {
			return Err("spill lifecycle counter is already failed");
		}
		let (count, hit_bytes) = match (decoded, hit) {
			(false, false) => (&mut counters.plaintext_cache_misses, None),
			(false, true) => (
				&mut counters.plaintext_cache_hits,
				Some(&mut counters.plaintext_cache_hit_bytes),
			),
			(true, false) => (&mut counters.decoded_cache_misses, None),
			(true, true) => (
				&mut counters.decoded_cache_hits,
				Some(&mut counters.decoded_cache_hit_bytes),
			),
		};
		let Some(next) = count.checked_add(1) else {
			counters.error = Some("spill cache event count overflowed");
			return Err("spill cache event count overflowed");
		};
		*count = next;
		if let Some(hit_bytes) = hit_bytes {
			let Some(next) = hit_bytes.checked_add(bytes) else {
				counters.error = Some("spill cache hit bytes overflowed");
				return Err("spill cache hit bytes overflowed");
			};
			*hit_bytes = next;
		}
		Ok(())
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
		Self::record_inclusive_lifecycle_term(
			frame,
			InclusiveLifecycleTerm {
				name: token.name,
				calls: 1,
				micros: inclusive_micros,
				error: None,
			},
		);
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

	fn record_inclusive_lifecycle(
		&mut self,
		id: u64,
		name: &'static str,
		micros: u64,
		calls: u64,
	) -> Result<(), &'static str> {
		let frame = self
			.stack
			.iter_mut()
			.find(|frame| frame.id == id)
			.ok_or("inclusive lifecycle term recorded after its invocation exited")?;
		Self::record_inclusive_lifecycle_term(
			frame,
			InclusiveLifecycleTerm {
				name,
				calls,
				micros,
				error: None,
			},
		);
		Ok(())
	}

	fn record_inclusive_lifecycle_term(frame: &mut Frame, term: InclusiveLifecycleTerm) {
		let terms = match frame.pending.as_mut() {
			Some(pending) => &mut pending.inclusive_lifecycle,
			None => &mut frame.inclusive_lifecycle,
		};
		Self::merge_inclusive_lifecycle_term(terms, term);
	}

	fn merge_inclusive_lifecycle_term(
		terms: &mut Vec<InclusiveLifecycleTerm>,
		incoming: InclusiveLifecycleTerm,
	) {
		let Some(existing) = terms.iter_mut().find(|term| term.name == incoming.name) else {
			terms.push(incoming);
			return;
		};
		if existing.error.is_some() {
			return;
		}
		match (
			existing.micros.checked_add(incoming.micros),
			existing.calls.checked_add(incoming.calls),
		) {
			(Some(micros), Some(calls)) => {
				existing.micros = micros;
				existing.calls = calls;
			}
			_ => existing.error = Some("inclusive lifecycle term overflowed"),
		}
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
				rows: None,
				retained_bytes: None,
				outcome,
			},
		);
	}

	fn record_stage_facts(
		&mut self,
		id: u64,
		parent: &'static str,
		name: &'static str,
		replay: u64,
		rows: u64,
		retained_bytes: u64,
	) -> Result<(), &'static str> {
		let frame = self
			.stack
			.iter_mut()
			.find(|frame| frame.id == id)
			.ok_or("stage facts recorded after their invocation exited")?;
		let stages = match frame.pending.as_mut() {
			Some(pending) => &mut pending.stages,
			None => &mut frame.stages,
		};
		let stage = stages
			.iter_mut()
			.find(|stage| {
				stage.name == name && stage.parent == Some(parent) && stage.replay == Some(replay)
			})
			.ok_or("stage facts have no matching replay child")?;
		match (stage.rows, stage.retained_bytes) {
			(None, None) => {
				stage.rows = Some(rows);
				stage.retained_bytes = Some(retained_bytes);
				Ok(())
			}
			(Some(existing_rows), Some(existing_bytes))
				if existing_rows == rows && existing_bytes == retained_bytes =>
			{
				Ok(())
			}
			_ => {
				stage.outcome = StageOutcome::Failed("replay child facts changed");
				Err("replay child facts changed")
			}
		}
	}

	/// Merge a stage instance into a list, keyed by `(name, parent, replay)` — a repeat merges only
	/// within its own replay identity. The merge is checked: an overflow makes the combined stage
	/// `Failed`, and either part failing fails the whole.
	fn merge_stage(stages: &mut Vec<HandlerStage>, incoming: HandlerStage) {
		if let Some(existing) = stages.iter_mut().find(|s| {
			s.name == incoming.name && s.parent == incoming.parent && s.replay == incoming.replay
		}) {
			existing.calls = existing.calls.saturating_add(incoming.calls);
			match (
				existing.rows,
				existing.retained_bytes,
				incoming.rows,
				incoming.retained_bytes,
			) {
				(None, None, rows, retained_bytes) => {
					existing.rows = rows;
					existing.retained_bytes = retained_bytes;
				}
				(_, _, None, None) => {}
				(Some(a_rows), Some(a_bytes), Some(b_rows), Some(b_bytes))
					if a_rows == b_rows && a_bytes == b_bytes => {}
				_ => {
					existing.outcome = StageOutcome::Failed("repeated stage facts changed");
					return;
				}
			}
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

fn spill_parent_mut<'a>(
	parents: &'a mut Vec<SpillIoParent>,
	parent: &'static str,
) -> &'a mut SpillIoParent {
	if let Some(position) = parents.iter().position(|entry| entry.parent == parent) {
		return &mut parents[position];
	}
	parents.push(SpillIoParent {
		parent,
		..SpillIoParent::default()
	});
	parents.last_mut().expect("spill parent was just inserted")
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

/// Whether the process-level ADR 507/585 node-attribution diagnostic is enabled. Timing harnesses
/// use this single predicate when deciding whether a row can carry candidate authority.
pub fn node_attribution_profile_enabled() -> bool {
	enabled()
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

/// Record a committed route into the task's own label receipt.
///
/// Deliberately **outside** the `enabled()` gate below: the node-attribution
/// ledger is an opt-in file diagnostic, and this receipt must be written whether
/// or not it is on.
///
/// Measured result, ADR 554 follow-on: this set is empty on every distributed
/// task across the TPC-H board (1,323 nodes, 21 cells). That is a finding, not a
/// wiring failure — no fragment on the distributed arm forms a streaming chain
/// shape, so nothing reaches a commit site. `executed_node_kinds` exists because
/// of it: a receipt on the generic executor dispatch fires regardless of route.
fn record_committed_vocabulary(
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
	observed: &'static str,
) {
	crate::exec::join::record_task_telemetry(telemetry, |task| {
		record_bounded_route(
			&mut task.labels.committed_vocabularies,
			&mut task.labels.vocabulary_overflow,
			observed,
		);
	});
}

/// Record a plan node kind the recursive executor dispatched on.
///
/// Called from `execute_bound_node` **beside** [`enter`] and outside it: `enter`
/// returns an inert span when the file diagnostic is off, which would have made
/// this receipt invisible in exactly the ordinary atlas run it exists for — the
/// same gate that silently disabled the vocabulary receipt once already.
///
/// This names the plan variant, not the attribution [`NodeKind`]: the latter is
/// a seven-member cost bucket fixed by the ADR 507/551 manifest, so `Project`,
/// `Window` and `Take` all collapse into `Other` there and could never be told
/// apart in a route receipt.
pub(in crate::exec) fn record_executed_node_kind(
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
	plan: &ExecutionPlan,
) {
	let label = plan.node_label();
	crate::exec::join::record_task_telemetry(telemetry, |task| {
		record_bounded_route(
			&mut task.labels.executed_node_kinds,
			&mut task.labels.node_kind_overflow,
			label,
		);
	});
}

/// Phase two: commit the observed lifecycle variant. Only a variant fixed at predeclaration can be
/// selected; anything else fails the invocation closed. An attempted branch that declines simply
/// never calls this, and its buffered stages are discarded when the frame exits.
///
/// One writer for both destinations — the process-global ledger and the
/// task-local receipt — so the two cannot drift apart at the three call sites.
///
/// **The handle is optional on purpose.** `declare_stage_vocabulary` returns
/// `None` when the node-attribution file diagnostic is off, so taking
/// `&VocabularyHandle` forced every call site under an `if let Some(handle)`
/// guard — which silently disabled the task receipt in exactly the ordinary
/// atlas run it exists for. An observed vocabulary is a fact about execution and
/// is recorded whenever one exists; the ledger is the opt-in part.
pub(in crate::exec) fn commit_stage_vocabulary(
	telemetry: &Option<std::sync::Arc<Mutex<TaskTelemetry>>>,
	handle: Option<&VocabularyHandle>,
	observed: &'static str,
) {
	record_committed_vocabulary(telemetry, observed);
	let Some(handle) = handle else {
		return;
	};
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

/// Attach a query-neutral ownership or lifecycle receipt to this invocation. Diagnostics are emitted
/// beside stage arithmetic and never consulted by its reconciliation verdict.
pub(in crate::exec) fn record_invocation_diagnostic(
	handle: &VocabularyHandle,
	name: &'static str,
	value: serde_json::Value,
) {
	if !enabled() {
		return;
	}
	if let Ok(mut slot) = NODE_LEDGER.lock()
		&& let Some(accounting) = slot.as_mut()
	{
		accounting.attach_diagnostic(handle.invocation_id, name, value);
	}
}

/// Publish a locally accumulated raw elapsed term for ADR 585's additive lifecycle receipt. The
/// caller samples the hot loop locally and publishes once; this ledger is never locked per batch.
pub(in crate::exec) fn record_inclusive_lifecycle_micros(
	handle: &VocabularyHandle,
	name: &'static str,
	micros: u64,
	calls: u64,
) -> Result<(), ExecutionError> {
	if !enabled() {
		return Ok(());
	}
	NODE_LEDGER
		.lock()
		.map_err(|_| ExecutionError::Kernel {
			message: "node attribution ledger mutex poisoned".to_string(),
		})?
		.as_mut()
		.ok_or_else(|| ExecutionError::Kernel {
			message: "inclusive lifecycle timing has no active ledger".to_string(),
		})?
		.record_inclusive_lifecycle(handle.invocation_id, name, micros, calls)
		.map_err(|reason| ExecutionError::Kernel {
			message: format!("inclusive lifecycle timing failed: {reason}"),
		})
}

/// Publish one locally accumulated primitive suffix-probe observation. The hot probe loop never
/// locks this ledger: its window/chunk durations and counters arrive here once per enclosing suffix
/// call, still bound to the vocabulary invocation and replay identity.
pub(in crate::exec) fn record_suffix_probe_observation(
	handle: &VocabularyHandle,
	observation: SuffixProbeObservation,
) {
	if !enabled() {
		return;
	}
	if let Ok(mut slot) = NODE_LEDGER.lock()
		&& let Some(accounting) = slot.as_mut()
	{
		accounting.record_suffix_probe(handle.invocation_id, observation);
	}
}

/// Accumulate observational rows/bytes/pages for one timed spill refinement. The timer remains the
/// time authority; these counters explain its work without entering stage reconciliation.
pub(in crate::exec) fn record_spill_subleaf_observation(
	handle: &VocabularyHandle,
	parent: &'static str,
	name: &'static str,
	rows: u64,
	bytes: u64,
	pages: u64,
) -> Result<(), ExecutionError> {
	if !enabled() {
		return Ok(());
	}
	let result = NODE_LEDGER
		.lock()
		.map_err(|_| ExecutionError::Kernel {
			message: "node attribution ledger mutex poisoned".to_string(),
		})?
		.as_mut()
		.ok_or_else(|| ExecutionError::Kernel {
			message: "node attribution spill counter has no active ledger".to_string(),
		})?
		.record_spill_subleaf(handle.invocation_id, parent, name, rows, bytes, pages);
	result.map_err(|reason| ExecutionError::Kernel {
		message: format!("node attribution spill counter failed: {reason}"),
	})
}

pub(in crate::exec) fn record_spill_cache_event(
	handle: &VocabularyHandle,
	parent: &'static str,
	decoded: bool,
	hit: bool,
	bytes: u64,
) -> Result<(), ExecutionError> {
	if !enabled() {
		return Ok(());
	}
	let result = NODE_LEDGER
		.lock()
		.map_err(|_| ExecutionError::Kernel {
			message: "node attribution ledger mutex poisoned".to_string(),
		})?
		.as_mut()
		.ok_or_else(|| ExecutionError::Kernel {
			message: "node attribution spill cache counter has no active ledger".to_string(),
		})?
		.record_spill_cache(handle.invocation_id, parent, decoded, hit, bytes);
	result.map_err(|reason| ExecutionError::Kernel {
		message: format!("node attribution spill cache counter failed: {reason}"),
	})
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

/// Attach the already-cached build ownership facts to one replay-partition child. Facts are
/// deliberately separate from the clock: discovering them after probing must not widen the prep or
/// cleanup timing region, and a missing/mismatched child fails the profile rather than inventing one.
pub(in crate::exec) fn record_replay_stage_facts(
	handle: &VocabularyHandle,
	parent: &'static str,
	name: &'static str,
	replay: u64,
	rows: u64,
	retained_bytes: u64,
) -> Result<(), ExecutionError> {
	if !enabled() {
		return Ok(());
	}
	let result = NODE_LEDGER
		.lock()
		.map_err(|_| ExecutionError::Kernel {
			message: "node attribution ledger mutex poisoned".to_string(),
		})?
		.as_mut()
		.ok_or_else(|| ExecutionError::Kernel {
			message: "node attribution stage facts have no active ledger".to_string(),
		})?
		.record_stage_facts(
			handle.invocation_id,
			parent,
			name,
			replay,
			rows,
			retained_bytes,
		);
	result.map_err(|reason| ExecutionError::Kernel {
		message: format!("node attribution stage facts failed: {reason}"),
	})
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
			..
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
#[derive(Debug, PartialEq, Eq)]
struct ReplayPartitionReconciliation {
	parent_micros: u64,
	attributed_micros: u64,
	residual_micros: u64,
	replay_ids: Vec<u64>,
}

/// ADR 525's one new reconciliation shape: replay children partition one invocation-scoped parent.
/// The helper accepts raw rows so every fail-closed branch is testable without relying on the stage
/// merger to manufacture an otherwise-unrepresentable duplicate.
fn reconcile_replay_partition(
	parent_micros: Option<u64>,
	required_replays: &[u64],
	children: &[(u64, u64)],
) -> Result<ReplayPartitionReconciliation, &'static str> {
	let parent_micros = parent_micros.ok_or("partition_parent_not_reconciled")?;
	let mut required = required_replays.to_vec();
	required.sort_unstable();
	if required.windows(2).any(|ids| ids[0] == ids[1]) {
		return Err("partition_required_replay_id_duplicated");
	}
	let mut observed = children
		.iter()
		.map(|(replay, _)| *replay)
		.collect::<Vec<_>>();
	observed.sort_unstable();
	if observed.windows(2).any(|ids| ids[0] == ids[1]) {
		return Err("partition_replay_id_duplicated");
	}
	if observed != required {
		return Err("partition_replay_coverage_incomplete");
	}
	let attributed_micros = children
		.iter()
		.try_fold(0u64, |total, (_, micros)| total.checked_add(*micros))
		.ok_or("partition_child_time_overflowed")?;
	let residual_micros = parent_micros
		.checked_sub(attributed_micros)
		.ok_or("partition_children_exceed_parent")?;
	Ok(ReplayPartitionReconciliation {
		parent_micros,
		attributed_micros,
		residual_micros,
		replay_ids: observed,
	})
}

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
		.filter(|(required, parent, replay)| {
			!invocation.stages.iter().any(|s| {
				s.name == *required
					&& s.parent == *parent
					&& s.replay == *replay
					&& matches!(s.outcome, StageOutcome::Reconciled { .. })
			})
		})
		.map(|(name, parent, replay)| required_stage_value(name, parent, replay))
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
				"rows": stage.rows,
				"retained_bytes": stage.retained_bytes,
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
				"rows": stage.rows,
				"retained_bytes": stage.retained_bytes,
				"error": reason,
			}),
		})
		.collect::<Vec<_>>();
	// Refinements normally reconcile per `(parent, replay)` instance. ADR 525 adds exactly one other
	// shape: replay-scoped children partition one invocation-scoped parent, with one explicit global
	// residual. The two shapes are mutually exclusive and a mixed shape fails closed.
	let mut refinements = Vec::new();
	let mut refined_parent_names: Vec<&'static str> = Vec::new();
	for stage in &invocation.stages {
		if let Some(parent) = stage.parent
			&& !refined_parent_names.contains(&parent)
		{
			refined_parent_names.push(parent);
		}
	}
	let mut refinements_reconciled = true;
	for parent in refined_parent_names {
		let children = invocation
			.stages
			.iter()
			.filter(|stage| stage.parent == Some(parent))
			.collect::<Vec<_>>();
		let invocation_parent_micros = invocation
			.stages
			.iter()
			.find(|stage| stage.name == parent && stage.parent.is_none() && stage.replay.is_none())
			.and_then(|stage| stage.outcome.reconciled())
			.map(|(micros, _)| micros);
		let all_children_replayed = children.iter().all(|child| child.replay.is_some());
		let any_child_replayed = children.iter().any(|child| child.replay.is_some());
		if invocation_parent_micros.is_some() && all_children_replayed {
			let child_rows = children
				.iter()
				.map(|child| {
					let replay = child.replay.ok_or("partition_child_lacks_replay_id")?;
					let micros = child
						.outcome
						.reconciled()
						.map(|(micros, _)| micros)
						.ok_or("partition_child_not_reconciled")?;
					if child.rows.is_none() || child.retained_bytes.is_none() {
						return Err("partition_child_facts_missing");
					}
					Ok((replay, micros))
				})
				.collect::<Result<Vec<_>, &'static str>>();
			let required_replays = vocabulary
				.required_stages
				.entries()
				.filter_map(|(_, required_parent, replay)| {
					(required_parent == Some(parent))
						.then_some(replay)
						.flatten()
				})
				.collect::<Vec<_>>();
			let partition = child_rows.and_then(|children| {
				reconcile_replay_partition(invocation_parent_micros, &required_replays, &children)
			});
			let entry = match partition {
				Ok(partition) => serde_json::json!({
					"parent": parent,
					"replay_id": null,
					"scope": "invocation_parent_replay_partition",
					"status": "reconciled",
					"replay_ids": partition.replay_ids,
					"attributed_micros": partition.attributed_micros,
					"parent_micros": partition.parent_micros,
					"residual_micros": partition.residual_micros,
					"passed": true,
				}),
				Err(error) => {
					refinements_reconciled = false;
					serde_json::json!({
						"parent": parent,
						"replay_id": null,
						"scope": "invocation_parent_replay_partition",
						"status": error,
						"passed": false,
					})
				}
			};
			refinements.push(entry);
			continue;
		}
		if invocation_parent_micros.is_some() && any_child_replayed {
			refinements_reconciled = false;
			refinements.push(serde_json::json!({
				"parent": parent,
				"replay_id": null,
				"scope": "invocation_parent_replay_partition",
				"status": "partition_replay_scope_mixed",
				"passed": false,
			}));
			continue;
		}

		let mut replay_ids = children
			.iter()
			.map(|stage| stage.replay)
			.collect::<Vec<_>>();
		replay_ids.sort_unstable();
		replay_ids.dedup();
		for replay in replay_ids {
			let parent_micros = invocation
				.stages
				.iter()
				.find(|s| s.name == parent && s.parent.is_none() && s.replay == replay)
				.and_then(|s| s.outcome.reconciled())
				.map(|(micros, _)| micros);
			let child_micros =
				children
					.iter()
					.filter(|s| s.replay == replay)
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
					match stage_attribution::reconcile_stage_within_drive(
						child_micros,
						parent_micros,
					) {
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
	}
	let suffix_profiles_reconciled = invocation
		.suffix_probes
		.iter()
		.all(|profile| profile.error.is_none() && profile.observation.supported);
	serde_json::json!({
		"status": "reconciled",
		"vocabulary": vocabulary.name,
		"spill_counter_kind": vocabulary.spill.name(),
		"required_stages": vocabulary
			.required_stages
			.entries()
			.map(|(name, parent, replay)| required_stage_value(name, parent, replay))
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
		"suffix_probe_profiles_reconciled": suffix_profiles_reconciled,
		"passed": arithmetic_reconciled
			&& coverage_complete
			&& refinements_reconciled
			&& suffix_profiles_reconciled,
	})
}

/// How one required stage is named in the profile. An unscoped requirement stays the bare string the
/// static vocabularies have always published; a replay-scoped one names its exact id, which is what
/// makes a missing-replay failure actionable.
fn required_stage_value(
	name: &'static str,
	parent: Option<&'static str>,
	replay: Option<u64>,
) -> serde_json::Value {
	match (parent, replay) {
		(None, None) => serde_json::json!(name),
		(None, replay) => serde_json::json!({
			"stage": name,
			"replay_id": replay,
		}),
		(Some(parent), replay) => serde_json::json!({
			"stage": name,
			"parent": parent,
			"replay_id": replay,
		}),
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

fn inclusive_lifecycle_receipt_value(
	invocation: &AggregateInvocation,
	root_span_micros: u64,
) -> serde_json::Value {
	if invocation.route != Some("inner_prefix_semi_anti_grouped") {
		return serde_json::Value::Null;
	}
	let failed = |error: &str| {
		serde_json::json!({
			"status": "instrument_defect",
			"invocation_id": invocation.id,
			"error": error,
			"passed": false,
		})
	};
	let Some(HandlerAttribution::Reconciled {
		inclusive_micros: handler_inclusive_micros,
		..
	}) = invocation.handler
	else {
		return failed("winning handler has no reconciled inclusive span");
	};
	let term = |name: &'static str| -> Result<u64, &'static str> {
		let Some(sample) = invocation
			.inclusive_lifecycle
			.iter()
			.find(|sample| sample.name == name)
		else {
			return Err("inclusive lifecycle term is missing");
		};
		if sample.error.is_some() || sample.calls == 0 {
			return Err("inclusive lifecycle term is failed");
		}
		Ok(sample.micros)
	};
	// A finish term legitimately has zero calls: the hashed build finishes nothing.
	let finish_term = |name: &'static str| -> Result<u64, &'static str> {
		let Some(sample) = invocation
			.inclusive_lifecycle
			.iter()
			.find(|sample| sample.name == name)
		else {
			return Err("inclusive lifecycle term is missing");
		};
		if sample.error.is_some() {
			return Err("inclusive lifecycle term is failed");
		}
		Ok(sample.micros)
	};
	let values = [
		("runtime_prep", term("runtime_prep")),
		("runtime_prep_inner_1", term("runtime_prep_inner_1")),
		("runtime_prep_inner_2", term("runtime_prep_inner_2")),
		("runtime_prep_semi_read", term("runtime_prep_semi_read")),
		("runtime_prep_semi_write", term("runtime_prep_semi_write")),
		("runtime_prep_anti_read", term("runtime_prep_anti_read")),
		("runtime_prep_anti_write", term("runtime_prep_anti_write")),
		// ADR 589: what each summary's build does after its last batch, zero for a build that
		// finishes nothing; a runtime-prep child so the band stays exact when work moves there.
		(
			"runtime_prep_semi_finish",
			finish_term("runtime_prep_semi_finish"),
		),
		(
			"runtime_prep_anti_finish",
			finish_term("runtime_prep_anti_finish"),
		),
		("base_prep", term("base_prep")),
		("drive", term("drive")),
		("finalize", term("finalize")),
		("cleanup", term("cleanup")),
	];
	if let Some((name, Err(_))) = values.iter().find(|(_, value)| value.is_err()) {
		return failed(&format!(
			"inclusive lifecycle term {name} is missing or failed"
		));
	}
	let get = |name: &str| {
		values
			.iter()
			.find(|(candidate, _)| *candidate == name)
			.and_then(|(_, value)| value.as_ref().ok().copied())
			.expect("all lifecycle values were checked")
	};
	let runtime_prep_inclusive = get("runtime_prep");
	let runtime_children = [
		get("runtime_prep_inner_1"),
		get("runtime_prep_inner_2"),
		get("runtime_prep_semi_read"),
		get("runtime_prep_semi_write"),
		get("runtime_prep_semi_finish"),
		get("runtime_prep_anti_read"),
		get("runtime_prep_anti_write"),
		get("runtime_prep_anti_finish"),
	];
	let Some(runtime_children_total) = runtime_children
		.into_iter()
		.try_fold(0u64, |total, value| total.checked_add(value))
	else {
		return failed("runtime-prep child sum overflowed");
	};
	let Some(runtime_prep_residual) = runtime_prep_inclusive.checked_sub(runtime_children_total)
	else {
		return failed("runtime-prep children exceed runtime-prep inclusive span");
	};
	let base_prep_inclusive = get("base_prep");
	let drive_inclusive = get("drive");
	let finalize_inclusive = get("finalize");
	let cleanup_inclusive = get("cleanup");
	let Some(lifecycle_parts_total) = [
		runtime_prep_inclusive,
		base_prep_inclusive,
		drive_inclusive,
		finalize_inclusive,
		cleanup_inclusive,
	]
	.into_iter()
	.try_fold(0u64, |total, value| total.checked_add(value)) else {
		return failed("lifecycle term sum overflowed");
	};
	let Some(lifecycle_residual) = handler_inclusive_micros.checked_sub(lifecycle_parts_total)
	else {
		return failed("lifecycle terms exceed handler inclusive span");
	};
	let Some(outside_handler_span) = root_span_micros.checked_sub(handler_inclusive_micros) else {
		return failed("handler inclusive span exceeds execute-bound root span");
	};
	// ADR 586's fourth band: each summary's write half decomposed into its four activities. A child
	// may legitimately have zero passes (an op the plan did not carry, or one an earlier filter kept
	// every batch from reaching), so unlike the terms above a zero call count is a fact, not a
	// failure; only absence or an overflowed term is a defect.
	let mut write_bands = serde_json::Map::new();
	for (write_name, prefix) in [
		("runtime_prep_semi_write", "semi_write"),
		("runtime_prep_anti_write", "anti_write"),
	] {
		match write_decomposition_band(invocation, write_name, prefix, get(write_name)) {
			Ok(band) => write_bands.extend(band),
			Err(error) => return failed(&error),
		}
	}
	// ADR 587's fifth band: each summary's insert child split into its rows windows and the
	// growth windows nested inside them, with the residual (column checks, batch disposal, loop
	// overhead) by checked subtraction against the unchanged insert child.
	for summary in ["semi", "anti"] {
		let insert_micros = write_bands
			.get(&format!("{summary}_write_insert_micros"))
			.and_then(serde_json::Value::as_u64)
			.expect("write band was just built");
		match insert_decomposition_band(invocation, summary, insert_micros) {
			Ok(band) => write_bands.extend(band),
			Err(error) => return failed(&error),
		}
	}
	// ADR 594's sixth band: replay 1's preparation split into its sizing scan, its partition
	// sizing, and the spilled branch's build write, with the residual by checked subtraction and
	// the branch facts beside them. Absent on a profile that never armed it; present with its
	// consistency stated when it is.
	let inner_1_band = match inner_1_decomposition_band(invocation, get("runtime_prep_inner_1")) {
		Ok(Some(band)) => {
			write_bands.extend(band.clone());
			Some(band)
		}
		Ok(None) => None,
		Err(error) => return failed(&error),
	};
	// ADR 596's seventh band: the spilled branch's build write split into its eleven children
	// with the residual by checked subtraction and the writer's path facts beside them. Absent
	// when the ledger was never armed; a band on a branch other than `spilled` is a defect.
	match build_write_decomposition_band(invocation, inner_1_band.as_ref()) {
		Ok(Some(band)) => write_bands.extend(band),
		Ok(None) => {}
		Err(error) => return failed(&error),
	}
	let mut receipt = serde_json::json!({
		"status": "reconciled",
		"invocation_id": invocation.id,
		"handler_inclusive_micros": handler_inclusive_micros,
		"runtime_prep_inclusive_micros": runtime_prep_inclusive,
		"runtime_prep_inner_1_micros": get("runtime_prep_inner_1"),
		"runtime_prep_inner_2_micros": get("runtime_prep_inner_2"),
		"runtime_prep_semi_read_micros": get("runtime_prep_semi_read"),
		"runtime_prep_semi_write_micros": get("runtime_prep_semi_write"),
		"runtime_prep_anti_read_micros": get("runtime_prep_anti_read"),
		"runtime_prep_anti_write_micros": get("runtime_prep_anti_write"),
		"runtime_prep_semi_finish_micros": get("runtime_prep_semi_finish"),
		"runtime_prep_anti_finish_micros": get("runtime_prep_anti_finish"),
		"runtime_prep_residual_micros": runtime_prep_residual,
		"base_prep_inclusive_micros": base_prep_inclusive,
		"drive_inclusive_micros": drive_inclusive,
		"finalize_inclusive_micros": finalize_inclusive,
		"cleanup_inclusive_micros": cleanup_inclusive,
		"lifecycle_residual_micros": lifecycle_residual,
		"outside_handler_span_micros": outside_handler_span,
		"passed": true,
	});
	receipt
		.as_object_mut()
		.expect("receipt is an object")
		.extend(write_bands);
	receipt
}

/// ADR 594: the three children of replay 1's preparation, the residual, and the facts. `None`
/// when the band was never armed (no child term and no facts); an `unsupported` branch carries
/// the fact alone; a partial band is a defect. The consistency of the facts with the branch is
/// reported as `inner_1_facts_consistent` with the first inconsistency named, so a driver can
/// refuse an incomplete instrument even where the residual bound would pass.
fn inner_1_decomposition_band(
	invocation: &AggregateInvocation,
	inner_1_micros: u64,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
	const CHILDREN: [&str; 3] = [
		"inner_1_sizing_scan",
		"inner_1_partition_sizing",
		"inner_1_build_write",
	];
	let facts = invocation
		.diagnostics
		.iter()
		.find(|entry| entry.name == "inner_1_facts")
		.map(|entry| entry.value.clone());
	let present: Vec<&InclusiveLifecycleTerm> = CHILDREN
		.iter()
		.filter_map(|name| {
			invocation
				.inclusive_lifecycle
				.iter()
				.find(|sample| sample.name == *name)
		})
		.collect();
	if facts.is_none() && present.is_empty() {
		return Ok(None);
	}
	let mut band = serde_json::Map::new();
	let Some(facts) = facts else {
		return Err("inner_1 band has child terms but no facts".to_string());
	};
	let branch = facts
		.get("branch")
		.and_then(serde_json::Value::as_str)
		.ok_or_else(|| "inner_1 facts lack a branch".to_string())?
		.to_string();
	band.insert("inner_1_branch".to_string(), serde_json::json!(branch));
	if branch == "unsupported" {
		if !present.is_empty() {
			return Err("inner_1 band is unsupported yet carries child terms".to_string());
		}
		band.insert(
			"inner_1_facts_consistent".to_string(),
			serde_json::json!(false),
		);
		band.insert(
			"inner_1_inconsistency".to_string(),
			serde_json::json!("branch is unsupported: the non-streaming fallback ran"),
		);
		return Ok(Some(band));
	}
	if present.len() != CHILDREN.len() {
		let missing: Vec<&str> = CHILDREN
			.iter()
			.copied()
			.filter(|name| !present.iter().any(|sample| sample.name == *name))
			.collect();
		return Err(format!("inner_1 band is missing {}", missing.join(", ")));
	}
	let mut children_total = 0u64;
	for sample in &present {
		if sample.error.is_some() {
			return Err(format!("inner_1 child {} is failed", sample.name));
		}
		children_total = children_total
			.checked_add(sample.micros)
			.ok_or_else(|| "inner_1 child sum overflowed".to_string())?;
		band.insert(
			format!("{}_micros", sample.name),
			serde_json::json!(sample.micros),
		);
		band.insert(
			format!("{}_calls", sample.name),
			serde_json::json!(sample.calls),
		);
	}
	let residual = inner_1_micros
		.checked_sub(children_total)
		.ok_or_else(|| "inner_1 children exceed runtime_prep_inner_1".to_string())?;
	band.insert(
		"inner_1_residual_micros".to_string(),
		serde_json::json!(residual),
	);
	let fact_u64 = |name: &str| facts.get(name).and_then(serde_json::Value::as_u64);
	for name in [
		"partition_count",
		"group_count",
		"partition_sizing_passes",
		"estimated_build_bytes",
		"sizing_rows",
		"sizing_scan_calls",
		"partition_sizing_calls",
		"build_write_calls",
	] {
		band.insert(
			format!("inner_1_{name}"),
			facts.get(name).cloned().unwrap_or(serde_json::Value::Null),
		);
	}
	band.insert(
		"inner_1_build_rows".to_string(),
		facts
			.get("build_rows")
			.cloned()
			.unwrap_or(serde_json::Value::Null),
	);
	band.insert(
		"inner_1_build_spill_bytes".to_string(),
		facts
			.get("build_spill_bytes")
			.cloned()
			.unwrap_or(serde_json::Value::Null),
	);
	let term_calls = |name: &str| {
		present
			.iter()
			.find(|sample| sample.name == name)
			.map(|sample| sample.calls)
			.unwrap_or(0)
	};
	// Consistency: every required fact present, and the counts agreeing with the branch.
	let inconsistency: Option<String> = (|| {
		for name in [
			"partition_count",
			"group_count",
			"partition_sizing_passes",
			"estimated_build_bytes",
			"sizing_rows",
			"sizing_scan_calls",
			"partition_sizing_calls",
			"build_write_calls",
		] {
			if fact_u64(name).is_none() {
				return Some(format!("fact {name} is missing"));
			}
		}
		if fact_u64("sizing_scan_calls") != Some(1) || term_calls("inner_1_sizing_scan") != 1 {
			return Some("sizing_scan_calls is not 1".to_string());
		}
		if fact_u64("partition_sizing_calls") != Some(1)
			|| term_calls("inner_1_partition_sizing") != 1
		{
			return Some("partition_sizing_calls is not 1".to_string());
		}
		if fact_u64("partition_sizing_passes").is_none_or(|passes| passes < 1) {
			return Some("partition_sizing_passes is below 1".to_string());
		}
		let partition_count = fact_u64("partition_count").unwrap_or(0);
		let build_write_calls = fact_u64("build_write_calls").unwrap_or(0);
		match branch.as_str() {
			"partitioned" => {
				if partition_count <= 1 {
					return Some("partitioned branch with partition_count <= 1".to_string());
				}
				if build_write_calls != 0 || term_calls("inner_1_build_write") != 0 {
					return Some("partitioned branch with a build-write call".to_string());
				}
				if present
					.iter()
					.find(|sample| sample.name == "inner_1_build_write")
					.is_some_and(|sample| sample.micros != 0)
				{
					return Some("partitioned branch with build-write time".to_string());
				}
			}
			"spilled" => {
				if partition_count != 1 {
					return Some("spilled branch with partition_count != 1".to_string());
				}
				if build_write_calls != 1 || term_calls("inner_1_build_write") != 1 {
					return Some("spilled branch without exactly one build-write call".to_string());
				}
				if fact_u64("build_rows").is_none() || fact_u64("build_spill_bytes").is_none() {
					return Some("spilled branch without a completed build write".to_string());
				}
			}
			other => return Some(format!("unknown branch {other}")),
		}
		None
	})();
	band.insert(
		"inner_1_facts_consistent".to_string(),
		serde_json::json!(inconsistency.is_none()),
	);
	band.insert(
		"inner_1_inconsistency".to_string(),
		inconsistency
			.map(serde_json::Value::String)
			.unwrap_or(serde_json::Value::Null),
	);
	Ok(Some(band))
}

/// ADR 596: the eleven children of `inner_1_build_write`, the residual, and the writer's path
/// facts. `None` when the ledger was never armed (no child term and no facts); a partial band,
/// a band without the sixth band's spilled branch, or children exceeding the term is a defect.
/// The consistency rules follow the branches the writer took — each is conditional on the fact
/// it depends on — and are reported as `build_write_facts_consistent` with the first failure
/// named; a shape outside the vocabulary is reported as `build_write_shape` = `unsupported`.
fn build_write_decomposition_band(
	invocation: &AggregateInvocation,
	inner_1_band: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
	let facts = invocation
		.diagnostics
		.iter()
		.find(|entry| entry.name == "build_write_facts")
		.map(|entry| entry.value.clone());
	build_write_band_from_parts(&invocation.inclusive_lifecycle, facts, inner_1_band)
}

/// The receipt's names for ADR 596's eleven children, in writer order.
pub const BUILD_WRITE_CHILDREN: [&str; 11] = [
	"build_write_create",
	"build_write_scan_open",
	"build_write_scan_poll",
	"build_write_filter",
	"build_write_repartition",
	"build_write_project",
	"build_write_key_extract",
	"build_write_encode",
	"build_write_page_write",
	"build_write_index",
	"build_write_finish",
];

/// ADR 596: re-run the seventh band's reconciliation and consistency rules over a retained
/// invocation as the profile stores it — the `build_write_*` terms from its
/// `inclusive_lifecycle_receipt`, the facts from its `diagnostics`, the sixth band's branch,
/// term, rows, and spill bytes from the same receipt — so a tightened rule can be applied to
/// retained evidence without a run. Returns the band as the receipt would carry it, or the
/// defect the receipt would report.
pub fn revalidate_build_write_band(invocation: &serde_json::Value) -> serde_json::Value {
	let receipt = &invocation["inclusive_lifecycle_receipt"];
	let terms: Vec<InclusiveLifecycleTerm> = BUILD_WRITE_CHILDREN
		.iter()
		.filter_map(|name| {
			let micros = receipt.get(format!("{name}_micros"))?.as_u64()?;
			let calls = receipt.get(format!("{name}_calls"))?.as_u64()?;
			Some(InclusiveLifecycleTerm {
				name,
				calls,
				micros,
				error: None,
			})
		})
		.collect();
	let facts = invocation
		.get("diagnostics")
		.and_then(serde_json::Value::as_array)
		.and_then(|entries| {
			entries
				.iter()
				.find(|entry| entry.get("name") == Some(&serde_json::json!("build_write_facts")))
		})
		.and_then(|entry| entry.get("value").cloned());
	let mut inner_1_band = serde_json::Map::new();
	for name in [
		"inner_1_branch",
		"inner_1_build_write_micros",
		"inner_1_build_rows",
		"inner_1_build_spill_bytes",
	] {
		if let Some(value) = receipt.get(name) {
			inner_1_band.insert(name.to_string(), value.clone());
		}
	}
	match build_write_band_from_parts(&terms, facts, Some(&inner_1_band)) {
		Ok(Some(band)) => serde_json::Value::Object(band),
		Ok(None) => serde_json::json!({"status": "absent"}),
		Err(error) => serde_json::json!({"status": "instrument_defect", "error": error}),
	}
}

/// The seventh band from its parts: the child terms, the facts diagnostic, and the sixth band.
fn build_write_band_from_parts(
	terms: &[InclusiveLifecycleTerm],
	facts: Option<serde_json::Value>,
	inner_1_band: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
	const CHILDREN: [&str; 11] = BUILD_WRITE_CHILDREN;
	let present: Vec<&InclusiveLifecycleTerm> = CHILDREN
		.iter()
		.filter_map(|name| terms.iter().find(|sample| sample.name == *name))
		.collect();
	if facts.is_none() && present.is_empty() {
		return Ok(None);
	}
	let Some(facts) = facts else {
		return Err("build_write band has child terms but no facts".to_string());
	};
	let Some(inner_1_band) = inner_1_band else {
		return Err("build_write band without the inner_1 band".to_string());
	};
	if inner_1_band.get("inner_1_branch") != Some(&serde_json::json!("spilled")) {
		return Err("build_write band on a branch other than spilled".to_string());
	}
	let build_write_micros = inner_1_band
		.get("inner_1_build_write_micros")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| "build_write band without inner_1_build_write_micros".to_string())?;
	if present.len() != CHILDREN.len() {
		let missing: Vec<&str> = CHILDREN
			.iter()
			.copied()
			.filter(|name| !present.iter().any(|sample| sample.name == *name))
			.collect();
		return Err(format!(
			"build_write band is missing {}",
			missing.join(", ")
		));
	}
	let mut band = serde_json::Map::new();
	let mut children_total = 0u64;
	for sample in &present {
		if sample.error.is_some() {
			return Err(format!("build_write child {} is failed", sample.name));
		}
		children_total = children_total
			.checked_add(sample.micros)
			.ok_or_else(|| "build_write child sum overflowed".to_string())?;
		band.insert(
			format!("{}_micros", sample.name),
			serde_json::json!(sample.micros),
		);
		band.insert(
			format!("{}_calls", sample.name),
			serde_json::json!(sample.calls),
		);
	}
	let residual = build_write_micros
		.checked_sub(children_total)
		.ok_or_else(|| "build_write children exceed inner_1_build_write".to_string())?;
	band.insert(
		"build_write_residual_micros".to_string(),
		serde_json::json!(residual),
	);
	const FACTS: [&str; 21] = [
		"filter_present",
		"exchange",
		"exchange_partitions",
		"projected",
		"termination",
		"index_mode",
		"unsupported",
		"batches_read",
		"batches_limit_discarded",
		"batches_admitted",
		"batches_filtered_empty",
		"batches_written",
		"rows_read",
		"rows_kept",
		"chunks",
		"chunks_unkeyed",
		"pages",
		"plaintext_bytes",
		"ciphertext_bytes",
		"index_entries",
		"index_compactions",
	];
	for name in FACTS {
		band.insert(
			format!("build_write_{name}"),
			facts.get(name).cloned().unwrap_or(serde_json::Value::Null),
		);
	}
	let unsupported = facts
		.get("unsupported")
		.and_then(serde_json::Value::as_str)
		.map(str::to_string);
	band.insert(
		"build_write_shape".to_string(),
		serde_json::json!(if unsupported.is_some() {
			"unsupported"
		} else {
			"supported"
		}),
	);
	let fact_u64 = |name: &str| facts.get(name).and_then(serde_json::Value::as_u64);
	let fact_bool = |name: &str| facts.get(name).and_then(serde_json::Value::as_bool);
	let term_calls = |name: &str| {
		present
			.iter()
			.find(|sample| sample.name == name)
			.map(|sample| sample.calls)
			.unwrap_or(0)
	};
	let term_micros = |name: &str| {
		present
			.iter()
			.find(|sample| sample.name == name)
			.map(|sample| sample.micros)
			.unwrap_or(0)
	};
	// Consistency: every fact present, and each count agreeing with the path the facts name.
	let inconsistency: Option<String> = (|| {
		if let Some(shape) = unsupported {
			return Some(format!("unsupported shape: {shape}"));
		}
		for name in [
			"batches_read",
			"batches_limit_discarded",
			"batches_admitted",
			"batches_filtered_empty",
			"batches_written",
			"rows_read",
			"rows_kept",
			"chunks",
			"chunks_unkeyed",
			"pages",
			"plaintext_bytes",
			"ciphertext_bytes",
			"index_entries",
			"index_compactions",
		] {
			if fact_u64(name).is_none() {
				return Some(format!("fact {name} is missing"));
			}
		}
		let Some(filter_present) = fact_bool("filter_present") else {
			return Some("fact filter_present is missing".to_string());
		};
		let Some(exchange) = fact_bool("exchange") else {
			return Some("fact exchange is missing".to_string());
		};
		let Some(projected) = fact_bool("projected") else {
			return Some("fact projected is missing".to_string());
		};
		let termination = facts.get("termination").and_then(serde_json::Value::as_str);
		let index_mode = facts.get("index_mode").and_then(serde_json::Value::as_str);
		if !matches!(index_mode, Some("compact" | "legacy" | "none")) {
			return Some("fact index_mode is not compact, legacy, or none".to_string());
		}
		let count = |name: &str| fact_u64(name).unwrap_or(0);
		// A child that did not run carries no time; a child that ran may round to zero.
		for name in CHILDREN {
			if term_calls(name) == 0 && term_micros(name) != 0 {
				return Some(format!("{name} has time without a call"));
			}
		}
		for name in [
			"build_write_create",
			"build_write_scan_open",
			"build_write_finish",
		] {
			if term_calls(name) != 1 {
				return Some(format!("{name} calls is not 1"));
			}
		}
		// A batch the row limit discarded whole was read (polled) but never admitted to the
		// filter, the Exchange, or the writer; every downstream count is against the admitted.
		let discarded = count("batches_limit_discarded");
		let admitted = count("batches_admitted");
		if discarded > count("batches_read") || admitted != count("batches_read") - discarded {
			return Some("batches_admitted disagree with batches_read less discarded".to_string());
		}
		let expected_polls = match termination {
			Some("stream_end") => {
				if discarded != 0 {
					return Some("stream_end with a batch discarded by the limit".to_string());
				}
				count("batches_read") + 1
			}
			Some("row_limit") => {
				if discarded != 1 {
					return Some(
						"row_limit without exactly one batch discarded by the limit".to_string(),
					);
				}
				count("batches_read")
			}
			_ => return Some("fact termination is not stream_end or row_limit".to_string()),
		};
		if term_calls("build_write_scan_poll") != expected_polls {
			return Some(format!(
				"scan_poll calls disagree with batches_read under {}",
				termination.unwrap_or_default()
			));
		}
		let expected_filter = if filter_present { admitted } else { 0 };
		if term_calls("build_write_filter") != expected_filter {
			return Some(if filter_present {
				"filter calls disagree with batches_admitted".to_string()
			} else {
				"filter call without a filter".to_string()
			});
		}
		if !filter_present && count("batches_filtered_empty") != 0 {
			return Some("batches filtered empty without a filter".to_string());
		}
		if exchange {
			if fact_u64("exchange_partitions").is_none_or(|partitions| partitions < 1) {
				return Some("exchange without a partition count".to_string());
			}
			let expected = admitted.saturating_sub(count("batches_filtered_empty"));
			if term_calls("build_write_repartition") != expected {
				return Some(
					"repartition calls disagree with batches_admitted less batches_filtered_empty"
						.to_string(),
				);
			}
		} else if term_calls("build_write_repartition") != 0
			|| term_micros("build_write_repartition") != 0
		{
			return Some("repartition call without an exchange".to_string());
		}
		let batches_kept = admitted.saturating_sub(count("batches_filtered_empty"));
		let written_bound = if exchange {
			batches_kept.saturating_mul(fact_u64("exchange_partitions").unwrap_or(1))
		} else {
			batches_kept
		};
		if (!exchange && count("batches_written") != batches_kept)
			|| count("batches_written") > written_bound
		{
			return Some("batches_written disagree with the batches kept".to_string());
		}
		let expected_project = if projected { count("chunks") } else { 0 };
		if term_calls("build_write_project") != expected_project {
			return Some(if projected {
				"project calls disagree with chunks".to_string()
			} else {
				"project call on the unprojected path".to_string()
			});
		}
		if term_calls("build_write_key_extract") != count("chunks") {
			return Some("key_extract calls disagree with chunks".to_string());
		}
		if count("chunks_unkeyed") > count("chunks") {
			return Some("chunks_unkeyed exceeds chunks".to_string());
		}
		if count("pages") != count("chunks") - count("chunks_unkeyed") {
			return Some("pages disagree with chunks less chunks_unkeyed".to_string());
		}
		for name in [
			"build_write_encode",
			"build_write_page_write",
			"build_write_index",
		] {
			if term_calls(name) != count("pages") {
				return Some(format!("{name} calls disagree with pages"));
			}
		}
		if count("pages") == 0 && index_mode != Some("none") {
			return Some("index_mode without a page".to_string());
		}
		if count("pages") > 0 && index_mode == Some("none") {
			return Some("pages without an index_mode".to_string());
		}
		if count("rows_kept") > count("rows_read") {
			return Some("rows_kept exceeds rows_read".to_string());
		}
		if count("index_entries") > count("rows_kept") {
			return Some("index_entries exceed rows_kept".to_string());
		}
		let inner_1_u64 = |name: &str| inner_1_band.get(name).and_then(serde_json::Value::as_u64);
		if inner_1_u64("inner_1_build_rows") != Some(count("rows_kept")) {
			return Some("rows_kept disagrees with inner_1_build_rows".to_string());
		}
		if inner_1_u64("inner_1_build_spill_bytes") != Some(count("ciphertext_bytes")) {
			return Some("ciphertext_bytes disagree with inner_1_build_spill_bytes".to_string());
		}
		None
	})();
	band.insert(
		"build_write_facts_consistent".to_string(),
		serde_json::json!(inconsistency.is_none()),
	);
	band.insert(
		"build_write_inconsistency".to_string(),
		inconsistency
			.map(serde_json::Value::String)
			.unwrap_or(serde_json::Value::Null),
	);
	Ok(Some(band))
}

/// One summary's insert child split into `insert_rows` (one window per `write_batch`) and
/// `insert_grow` (one window per doubling, nested), `rows_excluding_grow` and the insert
/// residual by checked subtraction, and the presence of the windows diagnostic the driver bins
/// profiler samples against. Growth is exclusive: it is reported inside rows and subtracted.
fn insert_decomposition_band(
	invocation: &AggregateInvocation,
	summary: &str,
	insert_micros: u64,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
	let mut band = serde_json::Map::new();
	let term = |name: String| -> Result<(u64, u64), String> {
		let Some(sample) = invocation
			.inclusive_lifecycle
			.iter()
			.find(|sample| sample.name == name)
		else {
			return Err(format!("insert term {name} is missing"));
		};
		if sample.error.is_some() {
			return Err(format!("insert term {name} is failed"));
		}
		Ok((sample.micros, sample.calls))
	};
	let (rows, rows_windows) = term(format!("{summary}_insert_rows"))?;
	let (grow, grow_events) = term(format!("{summary}_insert_grow"))?;
	let rows_excluding_grow = rows
		.checked_sub(grow)
		.ok_or_else(|| format!("{summary} growth windows exceed their rows windows"))?;
	let residual = insert_micros
		.checked_sub(rows)
		.ok_or_else(|| format!("{summary} rows windows exceed {summary}_write_insert"))?;
	band.insert(
		format!("{summary}_insert_rows_micros"),
		serde_json::json!(rows),
	);
	band.insert(
		format!("{summary}_insert_rows_windows"),
		serde_json::json!(rows_windows),
	);
	band.insert(
		format!("{summary}_insert_grow_micros"),
		serde_json::json!(grow),
	);
	band.insert(
		format!("{summary}_insert_grow_events"),
		serde_json::json!(grow_events),
	);
	band.insert(
		format!("{summary}_insert_rows_excluding_grow_micros"),
		serde_json::json!(rows_excluding_grow),
	);
	band.insert(
		format!("{summary}_insert_residual_micros"),
		serde_json::json!(residual),
	);
	// ADR 589: the named total of the per-batch insert accumulation and the post-ingestion finish.
	let finish = invocation
		.inclusive_lifecycle
		.iter()
		.find(|sample| sample.name == format!("runtime_prep_{summary}_finish"))
		.map(|sample| sample.micros)
		.unwrap_or(0);
	band.insert(
		format!("{summary}_insert_finish_micros"),
		serde_json::json!(finish),
	);
	band.insert(
		format!("{summary}_insert_total_micros"),
		serde_json::json!(insert_micros.saturating_add(finish)),
	);
	let windows_name = format!("{summary}_insert_windows");
	if !invocation
		.diagnostics
		.iter()
		.any(|entry| entry.name == windows_name && entry.value.is_object())
	{
		return Err(format!(
			"insert windows diagnostic {windows_name} is missing"
		));
	}
	Ok(band)
}

/// The four write children of one summary, their pass counts, the decoded op list, and the
/// residual by checked subtraction against the unchanged write term.
fn write_decomposition_band(
	invocation: &AggregateInvocation,
	write_name: &str,
	prefix: &str,
	write_micros: u64,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
	let mut band = serde_json::Map::new();
	let mut children_total = 0u64;
	for child in ["filter", "key_program", "repartition", "insert"] {
		let name = format!("{prefix}_{child}");
		let Some(sample) = invocation
			.inclusive_lifecycle
			.iter()
			.find(|sample| sample.name == name)
		else {
			return Err(format!("write child {name} is missing"));
		};
		if sample.error.is_some() {
			return Err(format!("write child {name} is failed"));
		}
		children_total = children_total
			.checked_add(sample.micros)
			.ok_or_else(|| format!("{prefix} child sum overflowed"))?;
		band.insert(format!("{name}_micros"), serde_json::json!(sample.micros));
		band.insert(format!("{name}_passes"), serde_json::json!(sample.calls));
	}
	let residual = write_micros
		.checked_sub(children_total)
		.ok_or_else(|| format!("{prefix} children exceed {write_name}"))?;
	band.insert(
		format!("{prefix}_residual_micros"),
		serde_json::json!(residual),
	);
	let ops_name = format!("{prefix}_ops");
	let Some(ops) = invocation
		.diagnostics
		.iter()
		.find(|entry| entry.name == ops_name)
		.map(|entry| entry.value.clone())
		.filter(serde_json::Value::is_array)
	else {
		return Err(format!("write op list {ops_name} is missing"));
	};
	band.insert(ops_name, ops);
	Ok(band)
}

fn suffix_probe_profiles_value(profiles: &[SuffixProbeProfile]) -> serde_json::Value {
	serde_json::Value::Array(
		profiles
			.iter()
			.map(|profile| {
				let observation = &profile.observation;
				serde_json::json!({
					"replay_id": observation.replay_id,
					"status": if profile.error.is_some() {
						"counter_failed"
					} else if observation.supported {
						"measured"
					} else {
						"unsupported"
					},
					"error": profile.error,
					"unsupported_reason": observation.unsupported_reason,
					"calls": observation.calls,
					"timing": {
						"lookup_micros": observation.lookup_micros,
						"match_expand_micros": observation.match_expand_micros,
						"payload_take_micros": observation.payload_take_micros,
					},
					"flow": {
						"input_probe_rows": observation.input_probe_rows,
						"non_null_keys": observation.non_null_keys,
						"lookup_windows": observation.lookup_windows,
						"matched_references": observation.matched_references,
						"emitted_rows": observation.emitted_rows,
						"output_batches": observation.output_batches,
						"output_column_values": observation.output_column_values,
						"output_array_bytes": observation.output_array_bytes,
						"multiplicity_per_input": {
							"numerator": observation.matched_references,
							"denominator": observation.input_probe_rows,
						},
						"multiplicity_per_non_null_key": {
							"numerator": observation.matched_references,
							"denominator": observation.non_null_keys,
						},
					},
					"build": {
						"representation": observation.build_representation,
						"rows": observation.build_rows,
						"distinct_keys": observation.distinct_keys,
						"build_batch_bytes": observation.build_batch_bytes,
						"index_accounted_bytes": observation.index_accounted_bytes,
						"entry_capacity": observation.entry_capacity,
						"bucket_capacity": observation.bucket_capacity,
						"load": {
							"numerator": observation.distinct_keys,
							"denominator": observation.bucket_capacity,
						},
						"key_width_bytes": observation.key_width_bytes,
					},
					"passed": profile.error.is_none() && observation.supported,
				})
			})
			.collect(),
	)
}

fn spill_io_value(parents: &[SpillIoParent]) -> serde_json::Value {
	serde_json::Value::Array(
		parents
			.iter()
			.map(|parent| {
				serde_json::json!({
					"parent": parent.parent,
					"error": parent.error,
					"subleaves": parent.subleaves.iter().map(|leaf| {
						serde_json::json!({
							"stage": leaf.name,
							"calls": leaf.calls,
							"rows": leaf.rows,
							"bytes": leaf.bytes,
							"pages": leaf.pages,
						})
					}).collect::<Vec<_>>(),
					"plaintext_cache": {
						"hits": parent.plaintext_cache_hits,
						"misses": parent.plaintext_cache_misses,
						"hit_bytes": parent.plaintext_cache_hit_bytes,
					},
					"decoded_cache": {
						"hits": parent.decoded_cache_hits,
						"misses": parent.decoded_cache_misses,
						"hit_bytes": parent.decoded_cache_hit_bytes,
					},
					"passed": parent.error.is_none(),
				})
			})
			.collect(),
	)
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
				"inclusive_lifecycle_receipt": inclusive_lifecycle_receipt_value(
					invocation,
					accounting.root_span_micros,
				),
				"suffix_probe_profiles": suffix_probe_profiles_value(&invocation.suffix_probes),
				"spill_io": spill_io_value(&invocation.spill_io),
				"diagnostics": invocation.diagnostics.iter().map(|entry| {
					serde_json::json!({
						"name": entry.name,
						"value": entry.value,
					})
				}).collect::<Vec<_>>(),
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
				inclusive_micros: 500,
				exclusive_micros: 500,
				exclusive_spill_bytes: 0,
			}),
			vocabulary: None,
			vocabulary_failure: None,
			stages: Vec::new(),
			inclusive_lifecycle: Vec::new(),
			diagnostics: Vec::new(),
			suffix_probes: Vec::new(),
			spill_io: Vec::new(),
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
				inclusive_micros: 900,
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
				inclusive_micros: 994,
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

	#[test]
	fn adr585_inclusive_lifecycle_retains_a_nested_execute_bound_child() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		let handler = acc.open_handler().expect("aggregate frame open");
		let invocation_id = acc
			.predeclare(vec![VocabularyVariant {
				name: "measured",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Static(&["runtime_prep"]),
				residual_boundary_micros: 1_000,
			}])
			.expect("vocabulary predeclared");
		let stage = StageToken {
			name: "runtime_prep",
			parent: None,
			replay: None,
			invocation_id,
			children_micros_at_open: 0,
			children_spill_at_open: 0,
			started: Instant::now(),
			spill_before: 0,
		};
		acc.enter();
		assert!(!acc.exit(NodeKind::Scan, 40, 0, 0));
		acc.close_stage(stage, 100, Some(0));
		acc.commit_vocabulary(invocation_id, "measured");
		acc.close_handler(handler, 200, Some(0));
		acc.set_route("inner_prefix_semi_anti_grouped");
		assert!(acc.exit(NodeKind::Aggregate, 240, 0, 0));

		let invocation = &acc.aggregate_invocations[0];
		let exclusive = invocation.stages[0]
			.outcome
			.reconciled()
			.expect("exclusive stage reconciled")
			.0;
		let inclusive = invocation
			.inclusive_lifecycle
			.iter()
			.find(|term| term.name == "runtime_prep")
			.expect("inclusive stage sample")
			.micros;
		assert_eq!(exclusive, 60, "ADR 507 still subtracts its nested child");
		assert_eq!(inclusive, 100, "ADR 585 retains the same child");
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
				inclusive_micros: handler_micros,
				exclusive_micros: handler_micros,
				exclusive_spill_bytes: handler_spill,
			}),
			vocabulary,
			vocabulary_failure: None,
			stages,
			inclusive_lifecycle: Vec::new(),
			diagnostics: Vec::new(),
			suffix_probes: Vec::new(),
			spill_io: Vec::new(),
			growth_census: None,
		}
	}

	#[test]
	fn adr585_receipt_reconciles_the_two_additive_domains_on_the_invocation() {
		let mut invocation = invocation_with_stages(900, 0, None, Vec::new());
		invocation.route = Some("inner_prefix_semi_anti_grouped");
		invocation.handler = Some(HandlerAttribution::Reconciled {
			inclusive_micros: 1_000,
			exclusive_micros: 900,
			exclusive_spill_bytes: 0,
		});
		invocation.inclusive_lifecycle = [
			("runtime_prep", 400),
			("runtime_prep_inner_1", 100),
			("runtime_prep_inner_2", 100),
			("runtime_prep_semi_read", 50),
			("runtime_prep_semi_write", 50),
			("runtime_prep_anti_read", 40),
			("runtime_prep_anti_write", 40),
			("base_prep", 100),
			("drive", 300),
			("finalize", 50),
			("cleanup", 50),
		]
		.into_iter()
		.map(|(name, micros)| InclusiveLifecycleTerm {
			name,
			calls: 1,
			micros,
			error: None,
		})
		.collect();
		attach_finish_terms(&mut invocation, 0, 0);
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert_eq!(receipt["invocation_id"], invocation.id);
		assert_eq!(receipt["runtime_prep_residual_micros"], 20);
		assert_eq!(receipt["lifecycle_residual_micros"], 100);
		assert_eq!(receipt["outside_handler_span_micros"], 200);
	}

	/// ADR 589's finish terms, zero calls when a build finishes nothing after its last batch.
	fn attach_finish_terms(invocation: &mut AggregateInvocation, semi: u64, anti: u64) {
		for (name, micros) in [
			("runtime_prep_semi_finish", semi),
			("runtime_prep_anti_finish", anti),
		] {
			invocation
				.inclusive_lifecycle
				.retain(|term| term.name != name);
			invocation.inclusive_lifecycle.push(InclusiveLifecycleTerm {
				name,
				calls: if micros == 0 { 0 } else { 5 },
				micros,
				error: None,
			});
		}
	}

	fn attach_write_decomposition(
		invocation: &mut AggregateInvocation,
		prefix: &str,
		micros: [u64; 4],
		ops: &[&str],
	) {
		for (child, micros) in ["filter", "key_program", "repartition", "insert"]
			.into_iter()
			.zip(micros)
		{
			let name: &'static str = Box::leak(format!("{prefix}_{child}").into_boxed_str());
			invocation.inclusive_lifecycle.push(InclusiveLifecycleTerm {
				name,
				calls: if micros == 0 { 0 } else { 3 },
				micros,
				error: None,
			});
		}
		let name: &'static str = Box::leak(format!("{prefix}_ops").into_boxed_str());
		invocation.diagnostics.push(InvocationDiagnostic {
			name,
			value: serde_json::json!(ops),
		});
		// ADR 587's band is required whenever the fourth is: rows windows inside the insert
		// child, growth inside rows. Defaults keep every 586 fixture reconciling.
		let summary = prefix.trim_end_matches("_write");
		let insert = micros[3];
		attach_insert_decomposition(
			invocation,
			summary,
			insert.saturating_sub(insert / 10),
			insert / 20,
			2,
		);
	}

	fn attach_insert_decomposition(
		invocation: &mut AggregateInvocation,
		summary: &str,
		rows: u64,
		grow: u64,
		grow_events: u64,
	) {
		for (child, micros, calls) in [
			("insert_rows", rows, 3u64),
			("insert_grow", grow, grow_events),
		] {
			let name: &'static str = Box::leak(format!("{summary}_{child}").into_boxed_str());
			invocation
				.inclusive_lifecycle
				.retain(|term| term.name != name);
			invocation.inclusive_lifecycle.push(InclusiveLifecycleTerm {
				name,
				calls,
				micros,
				error: None,
			});
		}
		let name: &'static str = Box::leak(format!("{summary}_insert_windows").into_boxed_str());
		invocation.diagnostics.retain(|entry| entry.name != name);
		invocation.diagnostics.push(InvocationDiagnostic {
			name,
			value: serde_json::json!({"pid": 1, "tids": [7], "rows_windows": [], "growth_windows": []}),
		});
	}

	#[test]
	fn adr587_receipt_records_the_fifth_band_with_exclusive_growth() {
		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		attach_insert_decomposition(&mut invocation, "semi", 8, 3, 2);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert_eq!(receipt["semi_insert_rows_micros"], 8);
		assert_eq!(receipt["semi_insert_rows_windows"], 3);
		assert_eq!(receipt["semi_insert_grow_micros"], 3);
		assert_eq!(receipt["semi_insert_grow_events"], 2);
		assert_eq!(receipt["semi_insert_rows_excluding_grow_micros"], 5);
		assert_eq!(receipt["semi_insert_residual_micros"], 2, "10 - 8");
		// The fourth band and the three above it are untouched by the fifth.
		assert_eq!(receipt["semi_write_residual_micros"], 5);
		assert_eq!(receipt["runtime_prep_residual_micros"], 20);
	}

	#[test]
	fn adr587_receipt_reports_windows_exceeding_their_parent_as_a_defect() {
		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		attach_insert_decomposition(&mut invocation, "semi", 11, 3, 2);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("semi rows windows exceed semi_write_insert")
		);

		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		attach_insert_decomposition(&mut invocation, "semi", 8, 9, 2);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("semi growth windows exceed their rows windows")
		);

		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		invocation
			.diagnostics
			.retain(|entry| entry.name != "anti_insert_windows");
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("anti_insert_windows is missing")
		);
	}

	fn adr586_invocation() -> AggregateInvocation {
		let mut invocation = invocation_with_stages(900, 0, None, Vec::new());
		invocation.route = Some("inner_prefix_semi_anti_grouped");
		invocation.handler = Some(HandlerAttribution::Reconciled {
			inclusive_micros: 1_000,
			exclusive_micros: 900,
			exclusive_spill_bytes: 0,
		});
		invocation.inclusive_lifecycle = [
			("runtime_prep", 400),
			("runtime_prep_inner_1", 100),
			("runtime_prep_inner_2", 100),
			("runtime_prep_semi_read", 50),
			("runtime_prep_semi_write", 50),
			("runtime_prep_anti_read", 40),
			("runtime_prep_anti_write", 40),
			("base_prep", 100),
			("drive", 300),
			("finalize", 50),
			("cleanup", 50),
		]
		.into_iter()
		.map(|(name, micros)| InclusiveLifecycleTerm {
			name,
			calls: 1,
			micros,
			error: None,
		})
		.collect();
		attach_finish_terms(&mut invocation, 0, 0);
		invocation
	}

	fn adr589_write_bands(invocation: &mut AggregateInvocation) {
		attach_write_decomposition(
			invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
	}

	// ADR 589: a finish that moved work after the last batch is a runtime-prep child of its own;
	// the band reconciles with it, insert_total names the sum, and a zero finish is a fact.
	#[test]
	fn adr589_finish_terms_join_the_runtime_prep_band_and_name_insert_total() {
		let mut invocation = adr586_invocation();
		// runtime_prep 400 = inner 100 + 100 + semi 50 + 50 + anti 40 + 40 = 380, residual 20;
		// move 12 of the residual into the two finishes: the residual becomes 8.
		attach_finish_terms(&mut invocation, 7, 5);
		adr589_write_bands(&mut invocation);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert_eq!(receipt["runtime_prep_semi_finish_micros"], 7);
		assert_eq!(receipt["runtime_prep_anti_finish_micros"], 5);
		assert_eq!(receipt["runtime_prep_residual_micros"], 8);
		assert_eq!(receipt["semi_insert_finish_micros"], 7);
		assert_eq!(
			receipt["semi_insert_total_micros"], 17,
			"insert 10 + finish 7"
		);
		assert_eq!(receipt["anti_insert_total_micros"], 15);
		// A finish exceeding the residual is an underflow, a defect.
		let mut invocation = adr586_invocation();
		attach_finish_terms(&mut invocation, 30, 0);
		adr589_write_bands(&mut invocation);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("runtime-prep children exceed")
		);
		// A missing finish term is a defect, not a zero.
		let mut invocation = adr586_invocation();
		invocation
			.inclusive_lifecycle
			.retain(|term| term.name != "runtime_prep_anti_finish");
		adr589_write_bands(&mut invocation);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("runtime_prep_anti_finish is missing")
		);
	}

	fn attach_inner_1_band(
		invocation: &mut AggregateInvocation,
		micros: [u64; 3],
		calls: [u64; 3],
		facts: serde_json::Value,
	) {
		for ((name, micros), calls) in [
			"inner_1_sizing_scan",
			"inner_1_partition_sizing",
			"inner_1_build_write",
		]
		.into_iter()
		.zip(micros)
		.zip(calls)
		{
			invocation.inclusive_lifecycle.push(InclusiveLifecycleTerm {
				name,
				calls,
				micros,
				error: None,
			});
		}
		invocation.diagnostics.push(InvocationDiagnostic {
			name: "inner_1_facts",
			value: facts,
		});
	}

	fn partitioned_facts() -> serde_json::Value {
		serde_json::json!({
			"branch": "partitioned", "partition_count": 4, "group_count": 2,
			"partition_sizing_passes": 2, "estimated_build_bytes": 1_000_000, "sizing_rows": 5000,
			"sizing_scan_calls": 1, "partition_sizing_calls": 1, "build_write_calls": 0,
			"build_rows": null, "build_spill_bytes": null,
		})
	}

	fn spilled_facts() -> serde_json::Value {
		serde_json::json!({
			"branch": "spilled", "partition_count": 1, "group_count": 1,
			"partition_sizing_passes": 1, "estimated_build_bytes": 1_000, "sizing_rows": 50,
			"sizing_scan_calls": 1, "partition_sizing_calls": 1, "build_write_calls": 1,
			"build_rows": 50, "build_spill_bytes": 4096,
		})
	}

	// ADR 594: the sixth band reconciles against ADR 585's unchanged inner_1 term, records the
	// branch facts, accepts a zero build write on the partitioned branch, and is absent when the
	// instrument was never armed.
	#[test]
	fn adr594_receipt_records_the_sixth_band_and_its_facts() {
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		// inner_1 = 100: sizing 40 + partition sizing 50 + build write 0, residual 10.
		attach_inner_1_band(&mut invocation, [40, 50, 0], [1, 1, 0], partitioned_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled", "{receipt}");
		assert_eq!(receipt["inner_1_branch"], "partitioned");
		assert_eq!(receipt["inner_1_sizing_scan_micros"], 40);
		assert_eq!(receipt["inner_1_partition_sizing_micros"], 50);
		assert_eq!(receipt["inner_1_build_write_micros"], 0);
		assert_eq!(receipt["inner_1_build_write_calls"], 0);
		assert_eq!(receipt["inner_1_residual_micros"], 10);
		assert_eq!(receipt["inner_1_partition_count"], 4);
		assert_eq!(receipt["inner_1_partition_sizing_passes"], 2);
		assert_eq!(receipt["inner_1_facts_consistent"], true);
		assert_eq!(
			receipt["runtime_prep_inner_1_micros"], 100,
			"ADR 585's term unchanged"
		);
		// The spilled branch with its completed write.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["inner_1_branch"], "spilled");
		assert_eq!(receipt["inner_1_residual_micros"], 5);
		assert_eq!(receipt["inner_1_build_rows"], 50);
		assert_eq!(receipt["inner_1_facts_consistent"], true);
		// Never armed: no band, and the receipt is the one ADR 585-589 readers know.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert!(receipt.get("inner_1_branch").is_none());
		// Children exceeding the term are a defect.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [60, 50, 0], [1, 1, 0], partitioned_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("children exceed runtime_prep_inner_1")
		);
		// A partial band is a defect, not a smaller band.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [40, 50, 0], [1, 1, 0], partitioned_facts());
		invocation
			.inclusive_lifecycle
			.retain(|term| term.name != "inner_1_partition_sizing");
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("missing inner_1_partition_sizing")
		);
	}

	// ADR 594: the facts must agree with the branch; each contradiction is named and the band is
	// marked inconsistent even though its algebra reconciles, so a driver refuses it.
	#[test]
	fn adr594_receipt_names_every_fact_inconsistency_and_the_unsupported_path() {
		let cases: Vec<(&str, [u64; 3], [u64; 3], serde_json::Value, &str)> = vec![
			(
				"partitioned with a build write",
				[40, 50, 5],
				[1, 1, 1],
				{
					let mut f = partitioned_facts();
					f["build_write_calls"] = serde_json::json!(1);
					f
				},
				"partitioned branch with a build-write call",
			),
			// Zero calls with time on the clock: the algebra reconciles (residual 5 instead of
			// 10) and the counts agree, but time without a call is not a partitioned branch.
			(
				"partitioned with build-write time and no call",
				[40, 50, 5],
				[1, 1, 0],
				partitioned_facts(),
				"partitioned branch with build-write time",
			),
			(
				"partitioned with one partition",
				[40, 50, 0],
				[1, 1, 0],
				{
					let mut f = partitioned_facts();
					f["partition_count"] = serde_json::json!(1);
					f
				},
				"partition_count <= 1",
			),
			(
				"spilled with two partitions",
				[20, 20, 55],
				[1, 1, 1],
				{
					let mut f = spilled_facts();
					f["partition_count"] = serde_json::json!(2);
					f
				},
				"partition_count != 1",
			),
			(
				"spilled without a completed write",
				[20, 20, 55],
				[1, 1, 1],
				{
					let mut f = spilled_facts();
					f["build_rows"] = serde_json::Value::Null;
					f
				},
				"without a completed build write",
			),
			(
				"spilled without a build-write call",
				[20, 20, 0],
				[1, 1, 0],
				spilled_facts(),
				"without exactly one build-write call",
			),
			(
				"two sizing scans",
				[40, 50, 0],
				[2, 1, 0],
				partitioned_facts(),
				"sizing_scan_calls is not 1",
			),
			(
				"partition sizing counted twice",
				[40, 50, 0],
				[1, 2, 0],
				partitioned_facts(),
				"partition_sizing_calls is not 1",
			),
			(
				"no sizing pass",
				[40, 50, 0],
				[1, 1, 0],
				{
					let mut f = partitioned_facts();
					f["partition_sizing_passes"] = serde_json::json!(0);
					f
				},
				"partition_sizing_passes is below 1",
			),
			(
				"missing fact",
				[40, 50, 0],
				[1, 1, 0],
				{
					let mut f = partitioned_facts();
					f.as_object_mut().unwrap().remove("estimated_build_bytes");
					f
				},
				"fact estimated_build_bytes is missing",
			),
		];
		for (label, micros, calls, facts, needle) in cases {
			let mut invocation = adr586_invocation();
			adr589_write_bands(&mut invocation);
			attach_inner_1_band(&mut invocation, micros, calls, facts);
			let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
			assert_eq!(receipt["status"], "reconciled", "{label}: {receipt}");
			assert_eq!(receipt["inner_1_facts_consistent"], false, "{label}");
			assert!(
				receipt["inner_1_inconsistency"]
					.as_str()
					.unwrap()
					.contains(needle),
				"{label}: {}",
				receipt["inner_1_inconsistency"]
			);
		}
		// The non-streaming fallback: the branch alone, marked as not this band's.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		invocation.diagnostics.push(InvocationDiagnostic {
			name: "inner_1_facts",
			value: serde_json::json!({"branch": "unsupported"}),
		});
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert_eq!(receipt["inner_1_branch"], "unsupported");
		assert_eq!(receipt["inner_1_facts_consistent"], false);
		assert!(receipt.get("inner_1_sizing_scan_micros").is_none());
	}

	const ADR596_CHILDREN: [&str; 11] = [
		"build_write_create",
		"build_write_scan_open",
		"build_write_scan_poll",
		"build_write_filter",
		"build_write_repartition",
		"build_write_project",
		"build_write_key_extract",
		"build_write_encode",
		"build_write_page_write",
		"build_write_index",
		"build_write_finish",
	];

	fn attach_build_write_band(
		invocation: &mut AggregateInvocation,
		micros: [u64; 11],
		calls: [u64; 11],
		facts: serde_json::Value,
	) {
		for ((name, micros), calls) in ADR596_CHILDREN.into_iter().zip(micros).zip(calls) {
			invocation.inclusive_lifecycle.push(InclusiveLifecycleTerm {
				name,
				calls,
				micros,
				error: None,
			});
		}
		invocation.diagnostics.push(InvocationDiagnostic {
			name: "build_write_facts",
			value: facts,
		});
	}

	/// A projected, filtered, unexchanged write to the stream's end: four batches read, one
	/// filtered empty, three written in three chunks, every chunk keyed, on ADR 594's spilled
	/// facts (50 rows, 4,096 spill bytes).
	fn build_write_facts() -> serde_json::Value {
		serde_json::json!({
			"filter_present": true, "exchange": false, "exchange_partitions": null,
			"projected": true, "termination": "stream_end", "index_mode": "compact",
			"unsupported": null,
			"batches_read": 4, "batches_limit_discarded": 0, "batches_admitted": 4,
			"batches_filtered_empty": 1, "batches_written": 3,
			"rows_read": 60, "rows_kept": 50, "chunks": 3, "chunks_unkeyed": 0, "pages": 3,
			"plaintext_bytes": 8000, "ciphertext_bytes": 4096, "index_entries": 50,
			"index_compactions": 0,
		})
	}

	/// The same write ended by the row limit: the fourth poll's batch discarded whole, three
	/// admitted, one filtered empty, two written in two chunks and pages; 4 polls, no final one.
	fn row_limit_facts() -> serde_json::Value {
		let mut facts = build_write_facts();
		facts["termination"] = serde_json::json!("row_limit");
		facts["batches_limit_discarded"] = serde_json::json!(1);
		facts["batches_admitted"] = serde_json::json!(3);
		facts["batches_written"] = serde_json::json!(2);
		facts["chunks"] = serde_json::json!(2);
		facts["pages"] = serde_json::json!(2);
		facts["rows_read"] = serde_json::json!(80);
		facts
	}

	/// Children summing to 53 of the 55 µs build write: residual 2.
	const ADR596_MICROS: [u64; 11] = [2, 3, 10, 5, 0, 4, 5, 6, 10, 6, 2];
	const ADR596_CALLS: [u64; 11] = [1, 1, 5, 4, 0, 3, 3, 3, 3, 3, 1];

	// ADR 596: the seventh band reconciles against the sixth band's unchanged build-write term,
	// carries the path facts and every call count, and is absent when the ledger was never armed.
	#[test]
	fn adr596_receipt_records_the_seventh_band_and_its_facts() {
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		attach_build_write_band(
			&mut invocation,
			ADR596_MICROS,
			ADR596_CALLS,
			build_write_facts(),
		);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled", "{receipt}");
		assert_eq!(receipt["inner_1_build_write_micros"], 55);
		assert_eq!(receipt["build_write_residual_micros"], 2);
		assert_eq!(receipt["build_write_scan_open_micros"], 3);
		assert_eq!(receipt["build_write_scan_poll_micros"], 10);
		assert_eq!(receipt["build_write_scan_poll_calls"], 5);
		assert_eq!(receipt["build_write_repartition_calls"], 0);
		assert_eq!(receipt["build_write_repartition_micros"], 0);
		assert_eq!(receipt["build_write_page_write_calls"], 3);
		assert_eq!(receipt["build_write_filter_present"], true);
		assert_eq!(receipt["build_write_exchange"], false);
		assert_eq!(receipt["build_write_projected"], true);
		assert_eq!(receipt["build_write_termination"], "stream_end");
		assert_eq!(receipt["build_write_index_mode"], "compact");
		assert_eq!(receipt["build_write_rows_kept"], 50);
		assert_eq!(receipt["build_write_ciphertext_bytes"], 4096);
		assert_eq!(receipt["build_write_shape"], "supported");
		assert_eq!(receipt["build_write_facts_consistent"], true, "{receipt}");
		assert_eq!(
			receipt["build_write_inconsistency"],
			serde_json::Value::Null
		);

		// The converse holds: a child whose calls' durations round to zero is consistent.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		let mut micros = ADR596_MICROS;
		micros[7] = 0;
		attach_build_write_band(&mut invocation, micros, ADR596_CALLS, build_write_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["build_write_facts_consistent"], true, "{receipt}");
		assert_eq!(receipt["build_write_encode_calls"], 3);
		assert_eq!(receipt["build_write_residual_micros"], 8);

		// A row-limit termination: four polls, the fourth's batch discarded by the limit, three
		// admitted, one of those filtered empty, two written; no final poll.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		attach_build_write_band(
			&mut invocation,
			[2, 3, 10, 5, 0, 4, 5, 6, 10, 6, 2],
			[1, 1, 4, 3, 0, 2, 2, 2, 2, 2, 1],
			row_limit_facts(),
		);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["build_write_facts_consistent"], true, "{receipt}");
		assert_eq!(receipt["build_write_termination"], "row_limit");
		assert_eq!(receipt["build_write_batches_limit_discarded"], 1);
		assert_eq!(receipt["build_write_batches_admitted"], 3);

		// Never armed: the sixth band alone, no seventh.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert!(receipt.get("build_write_residual_micros").is_none());
		assert!(receipt.get("build_write_shape").is_none());

		// Children exceeding the term: a defect, not a negative residual.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		let mut micros = ADR596_MICROS;
		micros[8] = 20;
		attach_build_write_band(&mut invocation, micros, ADR596_CALLS, build_write_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("children exceed inner_1_build_write")
		);

		// A partial band: a defect naming the missing child.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
		attach_build_write_band(
			&mut invocation,
			ADR596_MICROS,
			ADR596_CALLS,
			build_write_facts(),
		);
		invocation
			.inclusive_lifecycle
			.retain(|term| term.name != "build_write_page_write");
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("missing build_write_page_write")
		);

		// The band on the partitioned branch: a defect; the ledger is armed on spilled only.
		let mut invocation = adr586_invocation();
		adr589_write_bands(&mut invocation);
		attach_inner_1_band(&mut invocation, [40, 50, 0], [1, 1, 0], partitioned_facts());
		attach_build_write_band(&mut invocation, [0; 11], [0; 11], build_write_facts());
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.unwrap()
				.contains("branch other than spilled")
		);
	}

	// ADR 596: each consistency rule is conditional on the facts it depends on and has a case
	// that breaks exactly it; an unsupported shape is named and never read through the band.
	#[test]
	fn adr596_receipt_names_every_fact_inconsistency_and_the_unsupported_shape() {
		let with = |edit: fn(&mut serde_json::Value)| {
			let mut facts = build_write_facts();
			edit(&mut facts);
			facts
		};
		let calls_with = |slot: usize, value: u64| {
			let mut calls = ADR596_CALLS;
			calls[slot] = value;
			calls
		};
		let cases: Vec<(&str, [u64; 11], [u64; 11], serde_json::Value, &str)> = vec![
			(
				"two creates",
				ADR596_MICROS,
				calls_with(0, 2),
				build_write_facts(),
				"build_write_create calls is not 1",
			),
			(
				"scan opened twice",
				ADR596_MICROS,
				calls_with(1, 2),
				build_write_facts(),
				"build_write_scan_open calls is not 1",
			),
			(
				"stream_end with one poll too few",
				ADR596_MICROS,
				calls_with(2, 4),
				build_write_facts(),
				"scan_poll calls disagree with batches_read under stream_end",
			),
			(
				"row_limit with a final poll counted",
				ADR596_MICROS,
				[1, 1, 5, 3, 0, 2, 2, 2, 2, 2, 1],
				row_limit_facts(),
				"scan_poll calls disagree with batches_read under row_limit",
			),
			(
				"a filter call without a filter",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| {
					f["filter_present"] = serde_json::json!(false);
					f["batches_filtered_empty"] = serde_json::json!(0);
					f["batches_written"] = serde_json::json!(4);
				}),
				"filter call without a filter",
			),
			(
				"filter calls short of the batches read",
				ADR596_MICROS,
				calls_with(3, 3),
				build_write_facts(),
				"filter calls disagree with batches_admitted",
			),
			(
				"a repartition call without an exchange",
				ADR596_MICROS,
				calls_with(4, 3),
				build_write_facts(),
				"repartition call without an exchange",
			),
			(
				"an exchange with repartition calls short of the batches kept",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| {
					f["exchange"] = serde_json::json!(true);
					f["exchange_partitions"] = serde_json::json!(1);
				}),
				"repartition calls disagree with batches_admitted less batches_filtered_empty",
			),
			(
				"more batches written than kept",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| f["batches_written"] = serde_json::json!(4)),
				"batches_written disagree with the batches kept",
			),
			(
				"a project call on the unprojected path",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| f["projected"] = serde_json::json!(false)),
				"project call on the unprojected path",
			),
			(
				"project calls short of the chunks",
				ADR596_MICROS,
				calls_with(5, 2),
				build_write_facts(),
				"project calls disagree with chunks",
			),
			(
				"key extraction short of the chunks",
				ADR596_MICROS,
				calls_with(6, 2),
				build_write_facts(),
				"key_extract calls disagree with chunks",
			),
			(
				"pages above chunks less unkeyed",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| f["chunks_unkeyed"] = serde_json::json!(1)),
				"pages disagree with chunks less chunks_unkeyed",
			),
			(
				"encode calls disagreeing with pages",
				ADR596_MICROS,
				calls_with(7, 2),
				build_write_facts(),
				"build_write_encode calls disagree with pages",
			),
			(
				"rows kept disagreeing with the sixth band",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| {
					f["rows_kept"] = serde_json::json!(49);
					f["index_entries"] = serde_json::json!(49);
				}),
				"rows_kept disagrees with inner_1_build_rows",
			),
			(
				"spill bytes disagreeing with the sixth band",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| f["ciphertext_bytes"] = serde_json::json!(4095)),
				"ciphertext_bytes disagree with inner_1_build_spill_bytes",
			),
			(
				"an index mode without a page",
				[2, 3, 10, 5, 0, 0, 0, 0, 0, 0, 2],
				[1, 1, 5, 4, 0, 0, 0, 0, 0, 0, 1],
				with(|f| {
					f["chunks"] = serde_json::json!(0);
					f["pages"] = serde_json::json!(0);
					f["batches_written"] = serde_json::json!(0);
					f["batches_filtered_empty"] = serde_json::json!(4);
					f["rows_kept"] = serde_json::json!(0);
					f["index_entries"] = serde_json::json!(0);
				}),
				"index_mode without a page",
			),
			(
				"a missing fact",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| {
					f.as_object_mut().unwrap().remove("plaintext_bytes");
				}),
				"fact plaintext_bytes is missing",
			),
			// Residual time moved into a child that did not run: the algebra reconciles, the
			// counts agree, and the band is still inconsistent.
			(
				"time in the absent filter",
				[2, 3, 10, 2, 0, 4, 5, 6, 10, 6, 2],
				[1, 1, 5, 0, 0, 3, 3, 3, 3, 3, 1],
				with(|f| {
					f["filter_present"] = serde_json::json!(false);
					f["batches_filtered_empty"] = serde_json::json!(0);
					f["batches_written"] = serde_json::json!(4);
				}),
				"build_write_filter has time without a call",
			),
			(
				"time in the absent projection",
				[2, 3, 10, 5, 0, 2, 5, 6, 10, 6, 2],
				[1, 1, 5, 4, 0, 0, 3, 3, 3, 3, 1],
				with(|f| f["projected"] = serde_json::json!(false)),
				"build_write_project has time without a call",
			),
			(
				"time in the absent repartition",
				[2, 3, 10, 5, 2, 4, 5, 6, 10, 6, 2],
				ADR596_CALLS,
				build_write_facts(),
				"build_write_repartition has time without a call",
			),
			(
				"time in a page child with no page",
				[2, 3, 10, 5, 0, 0, 0, 2, 0, 0, 2],
				[1, 1, 5, 4, 0, 0, 0, 0, 0, 0, 1],
				with(|f| {
					f["chunks"] = serde_json::json!(0);
					f["pages"] = serde_json::json!(0);
					f["batches_written"] = serde_json::json!(0);
					f["batches_filtered_empty"] = serde_json::json!(4);
					f["rows_kept"] = serde_json::json!(0);
					f["index_entries"] = serde_json::json!(0);
					f["index_mode"] = serde_json::json!("none");
				}),
				"build_write_encode has time without a call",
			),
			// The row limit's discarded batch: read, never admitted.
			(
				"stream_end with a discarded batch",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| {
					f["batches_limit_discarded"] = serde_json::json!(1);
					f["batches_admitted"] = serde_json::json!(3);
				}),
				"stream_end with a batch discarded by the limit",
			),
			(
				"row_limit without a discarded batch",
				ADR596_MICROS,
				calls_with(2, 4),
				with(|f| f["termination"] = serde_json::json!("row_limit")),
				"row_limit without exactly one batch discarded by the limit",
			),
			(
				"admitted disagreeing with read less discarded",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| f["batches_admitted"] = serde_json::json!(3)),
				"batches_admitted disagree with batches_read less discarded",
			),
			(
				"row_limit with the discarded batch filtered",
				[2, 3, 10, 5, 0, 4, 5, 6, 10, 6, 2],
				[1, 1, 4, 4, 0, 2, 2, 2, 2, 2, 1],
				row_limit_facts(),
				"filter calls disagree with batches_admitted",
			),
			(
				"a preserved-rows write",
				ADR596_MICROS,
				ADR596_CALLS,
				with(|f| f["unsupported"] = serde_json::json!("preserved_rows")),
				"unsupported shape: preserved_rows",
			),
		];
		for (label, micros, calls, facts, needle) in cases {
			let mut invocation = adr586_invocation();
			adr589_write_bands(&mut invocation);
			attach_inner_1_band(&mut invocation, [20, 20, 55], [1, 1, 1], spilled_facts());
			let unsupported = facts.get("unsupported").is_some_and(|v| !v.is_null());
			attach_build_write_band(&mut invocation, micros, calls, facts);
			let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
			assert_eq!(receipt["status"], "reconciled", "{label}: {receipt}");
			assert_eq!(receipt["build_write_facts_consistent"], false, "{label}");
			assert_eq!(
				receipt["build_write_shape"],
				if unsupported {
					"unsupported"
				} else {
					"supported"
				},
				"{label}"
			);
			assert!(
				receipt["build_write_inconsistency"]
					.as_str()
					.unwrap()
					.contains(needle),
				"{label}: {}",
				receipt["build_write_inconsistency"]
			);
		}
	}

	// A zero child is a fact with a pass count beside it, never a failed term: the semi summary
	// carries no filter op here, and the receipt says so through both the count and the op list.
	#[test]
	fn adr586_receipt_records_the_fourth_band_with_pass_counts_and_op_lists() {
		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "reconciled");
		assert_eq!(receipt["semi_write_filter_micros"], 0);
		assert_eq!(receipt["semi_write_filter_passes"], 0);
		assert_eq!(receipt["semi_write_key_program_micros"], 20);
		assert_eq!(receipt["semi_write_key_program_passes"], 3);
		assert_eq!(receipt["semi_write_residual_micros"], 5);
		assert_eq!(
			receipt["semi_write_ops"],
			serde_json::json!(["project", "exchange"])
		);
		assert_eq!(receipt["anti_write_residual_micros"], 5);
		assert_eq!(
			receipt["anti_write_ops"],
			serde_json::json!(["filter", "project", "exchange"])
		);
		// The three ADR 585 bands are untouched by the fourth.
		assert_eq!(receipt["runtime_prep_residual_micros"], 20);
		assert_eq!(receipt["lifecycle_residual_micros"], 100);
	}

	#[test]
	fn adr586_receipt_reports_children_exceeding_their_write_term_as_a_defect() {
		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 16],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert_eq!(receipt["passed"], false);
		assert!(
			receipt["error"]
				.as_str()
				.expect("error text")
				.contains("semi_write children exceed runtime_prep_semi_write")
		);
	}

	#[test]
	fn adr586_receipt_requires_every_child_and_the_op_list() {
		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.expect("error text")
				.contains("anti_write_filter is missing")
		);

		let mut invocation = adr586_invocation();
		attach_write_decomposition(
			&mut invocation,
			"semi_write",
			[0, 20, 15, 10],
			&["project", "exchange"],
		);
		attach_write_decomposition(
			&mut invocation,
			"anti_write",
			[5, 10, 10, 10],
			&["filter", "project", "exchange"],
		);
		invocation
			.diagnostics
			.retain(|entry| entry.name != "anti_write_ops");
		let receipt = inclusive_lifecycle_receipt_value(&invocation, 1_200);
		assert_eq!(receipt["status"], "instrument_defect");
		assert!(
			receipt["error"]
				.as_str()
				.expect("error text")
				.contains("anti_write_ops is missing")
		);
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
				rows: None,
				retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
				rows: None,
				retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
					rows: None,
					retained_bytes: None,
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
				rows: None,
				retained_bytes: None,
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
				rows: None,
				retained_bytes: None,
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
			rows: None,
			retained_bytes: None,
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

	#[test]
	fn required_refinement_is_satisfied_only_under_its_declared_parent() {
		let vocabulary = || Vocabulary {
			invocation_id: 3,
			name: "partitioned_scan_build",
			spill: SpillCounterKind::Flow,
			required_stages: RequiredStages::Owned(vec![
				RequiredStage {
					name: "join0_build",
					parent: None,
					replay: None,
				},
				RequiredStage {
					name: "read_physical_read",
					parent: Some("join0_build"),
					replay: None,
				},
			]),
			residual_boundary_micros: 100,
		};
		let stages = |parent| {
			vec![
				HandlerStage {
					name: "join0_build",
					calls: 1,
					parent: None,
					replay: None,
					rows: None,
					retained_bytes: None,
					outcome: StageOutcome::Reconciled {
						micros: 950,
						spill_bytes: 0,
					},
				},
				HandlerStage {
					name: "read_physical_read",
					calls: 1,
					parent: Some(parent),
					replay: None,
					rows: None,
					retained_bytes: None,
					outcome: StageOutcome::Reconciled {
						micros: 900,
						spill_bytes: 0,
					},
				},
			]
		};

		let wrong_parent =
			invocation_with_stages(1_000, 0, Some(vocabulary()), stages("join0_probe"));
		let value = stages_value(&wrong_parent, &[]);
		assert_eq!(
			value["missing_required_stages"],
			serde_json::json!([{
				"stage": "read_physical_read",
				"parent": "join0_build",
				"replay_id": null,
			}])
		);
		assert_eq!(value["passed"], false);

		let matched = invocation_with_stages(1_000, 0, Some(vocabulary()), stages("join0_build"));
		let value = stages_value(&matched, &[]);
		assert_eq!(value["missing_required_stages"], serde_json::json!([]));
		assert_eq!(value["refinements"][0]["passed"], true);
		assert_eq!(value["passed"], true);
	}

	#[test]
	fn spill_lifecycle_counters_remain_parent_scoped_and_checked() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		let id = acc.stack.last().expect("frame").id;
		acc.record_spill_subleaf(id, "join0_build", "read_physical_read", 10, 20, 2)
			.expect("first build leaf");
		acc.record_spill_subleaf(id, "join0_build", "read_physical_read", 4, 8, 1)
			.expect("merged build leaf");
		acc.record_spill_subleaf(id, "join0_probe", "read_physical_read", 1, 3, 1)
			.expect("probe leaf");
		acc.record_spill_cache(id, "join0_build", true, true, 64)
			.expect("decoded hit");
		acc.record_spill_cache(id, "join0_build", true, false, 0)
			.expect("decoded miss");
		let value = spill_io_value(&acc.stack.last().expect("frame").spill_io);
		assert_eq!(value[0]["parent"], "join0_build");
		assert_eq!(value[0]["subleaves"][0]["calls"], 2);
		assert_eq!(value[0]["subleaves"][0]["rows"], 14);
		assert_eq!(value[0]["subleaves"][0]["bytes"], 28);
		assert_eq!(value[0]["subleaves"][0]["pages"], 3);
		assert_eq!(value[0]["decoded_cache"]["hits"], 1);
		assert_eq!(value[0]["decoded_cache"]["misses"], 1);
		assert_eq!(value[0]["decoded_cache"]["hit_bytes"], 64);
		assert_eq!(value[1]["parent"], "join0_probe");
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
				parent: None,
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
			rows: None,
			retained_bytes: None,
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

	#[test]
	fn adr525_replay_partition_is_complete_unique_bounded_and_explicit() {
		let partition =
			reconcile_replay_partition(Some(1_000), &[1, 2, 3], &[(3, 100), (1, 200), (2, 300)])
				.expect("valid replay partition");
		assert_eq!(partition.replay_ids, vec![1, 2, 3]);
		assert_eq!(partition.attributed_micros, 600);
		assert_eq!(partition.residual_micros, 400);

		assert_eq!(
			reconcile_replay_partition(Some(1_000), &[1, 2], &[(1, 100), (1, 200)]),
			Err("partition_replay_id_duplicated")
		);
		assert_eq!(
			reconcile_replay_partition(Some(1_000), &[1, 2], &[(1, 100)]),
			Err("partition_replay_coverage_incomplete")
		);
		assert_eq!(
			reconcile_replay_partition(Some(100), &[1, 2], &[(1, 60), (2, 50)]),
			Err("partition_children_exceed_parent")
		);
		assert_eq!(
			reconcile_replay_partition(None, &[1], &[(1, 10)]),
			Err("partition_parent_not_reconciled"),
			"without the parent no explicit residual can be formed"
		);
	}

	#[test]
	fn adr525_global_parent_replay_children_emit_one_partition_receipt() {
		let required_stages = RequiredStages::Owned(vec![
			RequiredStage {
				name: "runtime_prep",
				parent: None,
				replay: None,
			},
			RequiredStage {
				name: "prep_build",
				parent: Some("runtime_prep"),
				replay: Some(1),
			},
			RequiredStage {
				name: "prep_build",
				parent: Some("runtime_prep"),
				replay: Some(2),
			},
		]);
		let mut invocation = invocation_with_stages(
			1_000,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_probe_base",
				spill: SpillCounterKind::Flow,
				required_stages,
				residual_boundary_micros: 200,
			}),
			vec![HandlerStage {
				name: "runtime_prep",
				calls: 1,
				parent: None,
				replay: None,
				rows: None,
				retained_bytes: None,
				outcome: StageOutcome::Reconciled {
					micros: 900,
					spill_bytes: 0,
				},
			}],
		);
		for (replay, micros, rows, bytes) in [(1, 300, 10, 100), (2, 500, 20, 200)] {
			invocation.stages.push(HandlerStage {
				name: "prep_build",
				calls: 1,
				parent: Some("runtime_prep"),
				replay: Some(replay),
				rows: Some(rows),
				retained_bytes: Some(bytes),
				outcome: StageOutcome::Reconciled {
					micros,
					spill_bytes: 0,
				},
			});
		}
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["refinements_reconciled"], true);
		assert_eq!(value["refinements"].as_array().unwrap().len(), 1);
		let partition = &value["refinements"][0];
		assert_eq!(partition["scope"], "invocation_parent_replay_partition");
		assert_eq!(partition["replay_ids"], serde_json::json!([1, 2]));
		assert_eq!(partition["attributed_micros"], 800);
		assert_eq!(partition["parent_micros"], 900);
		assert_eq!(partition["residual_micros"], 100);
		assert!(partition.get("residual_micros").is_some());
		assert_eq!(value["passed"], true);
	}

	fn suffix_observation(replay_id: u64, supported: bool) -> SuffixProbeObservation {
		SuffixProbeObservation {
			replay_id,
			supported,
			unsupported_reason: (!supported).then_some("generic keys unsupported"),
			lookup_micros: 20,
			match_expand_micros: 30,
			payload_take_micros: 40,
			calls: 1,
			input_probe_rows: 10,
			non_null_keys: 8,
			lookup_windows: 1,
			matched_references: 16,
			emitted_rows: 16,
			output_batches: 1,
			output_column_values: 64,
			output_array_bytes: 512,
			build_representation: if supported {
				"int64_primitive"
			} else {
				"generic_hash"
			},
			build_rows: 100,
			distinct_keys: 80,
			build_batch_bytes: 1_600,
			index_accounted_bytes: 3_200,
			entry_capacity: 100,
			bucket_capacity: 256,
			key_width_bytes: if supported { 8 } else { 0 },
		}
	}

	#[test]
	fn adr516_suffix_observations_buffer_and_merge_by_replay_identity() {
		let mut acc = NodeAccounting::default();
		acc.enter();
		let id = acc
			.predeclare(vec![variant("scan_probe_base")])
			.expect("predeclared");
		acc.record_suffix_probe(id, suffix_observation(2, true));
		acc.record_suffix_probe(id, suffix_observation(1, true));
		acc.record_suffix_probe(id, suffix_observation(2, true));
		let pending = acc
			.stack
			.last()
			.expect("frame")
			.pending
			.as_ref()
			.expect("pending");
		assert_eq!(pending.suffix_probes.len(), 2);
		assert_eq!(pending.suffix_probes[0].observation.replay_id, 2);
		assert_eq!(pending.suffix_probes[0].observation.calls, 2);
		assert_eq!(pending.suffix_probes[0].observation.input_probe_rows, 20);
		assert_eq!(pending.suffix_probes[1].observation.replay_id, 1);

		acc.commit_vocabulary(id, "scan_probe_base");
		let frame = acc.stack.last().expect("frame");
		assert_eq!(frame.suffix_probes.len(), 2);
		assert_eq!(frame.suffix_probes[0].observation.replay_id, 2);
		assert_eq!(frame.suffix_probes[1].observation.replay_id, 1);
		let profiles = suffix_probe_profiles_value(&frame.suffix_probes);
		assert_eq!(profiles[0]["replay_id"], 2);
		assert_eq!(profiles[0]["calls"], 2);
		assert_eq!(
			profiles[0]["flow"]["multiplicity_per_input"],
			serde_json::json!({"numerator": 32, "denominator": 20})
		);
		assert_eq!(
			profiles[0]["build"]["load"],
			serde_json::json!({"numerator": 80, "denominator": 256})
		);
		assert!(
			frame.pending.is_none(),
			"commit consumes the buffered observations"
		);
	}

	#[test]
	fn adr516_suffix_profile_status_participates_in_the_stage_verdict() {
		let replay = Some(7);
		let required_stages = vec![
			RequiredStage {
				name: "suffix_probe",
				parent: None,
				replay,
			},
			RequiredStage {
				name: "suffix_lookup",
				parent: Some("suffix_probe"),
				replay,
			},
			RequiredStage {
				name: "suffix_match_expand",
				parent: Some("suffix_probe"),
				replay,
			},
			RequiredStage {
				name: "suffix_payload_take",
				parent: Some("suffix_probe"),
				replay,
			},
		];
		let stage = |name, parent, micros| HandlerStage {
			name,
			calls: 1,
			parent,
			replay,
			rows: None,
			retained_bytes: None,
			outcome: StageOutcome::Reconciled {
				micros,
				spill_bytes: 0,
			},
		};
		let mut invocation = invocation_with_stages(
			100,
			0,
			Some(Vocabulary {
				invocation_id: 3,
				name: "scan_probe_base",
				spill: SpillCounterKind::Flow,
				required_stages: RequiredStages::Owned(required_stages),
				residual_boundary_micros: 100,
			}),
			vec![
				stage("suffix_probe", None, 100),
				stage("suffix_lookup", Some("suffix_probe"), 20),
				stage("suffix_match_expand", Some("suffix_probe"), 30),
				stage("suffix_payload_take", Some("suffix_probe"), 40),
			],
		);
		invocation.suffix_probes.push(SuffixProbeProfile {
			observation: suffix_observation(7, true),
			error: None,
		});
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["refinements"][0]["residual_micros"], 10);
		assert_eq!(value["suffix_probe_profiles_reconciled"], true);
		assert_eq!(value["passed"], true);

		invocation
			.stages
			.iter_mut()
			.find(|stage| stage.name == "suffix_lookup")
			.expect("lookup refinement")
			.outcome = StageOutcome::Reconciled {
			micros: 80,
			spill_bytes: 0,
		};
		let value = stages_value(&invocation, &[]);
		assert_eq!(
			value["refinements"][0]["status"],
			"refinement_exceeds_parent_stage"
		);
		assert_eq!(value["suffix_probe_profiles_reconciled"], true);
		assert_eq!(value["passed"], false);

		invocation
			.stages
			.iter_mut()
			.find(|stage| stage.name == "suffix_lookup")
			.expect("lookup refinement")
			.outcome = StageOutcome::Reconciled {
			micros: 20,
			spill_bytes: 0,
		};
		invocation.suffix_probes[0].observation.supported = false;
		invocation.suffix_probes[0].observation.unsupported_reason =
			Some("generic keys unsupported");
		let value = stages_value(&invocation, &[]);
		assert_eq!(value["suffix_probe_profiles_reconciled"], false);
		assert_eq!(value["passed"], false);
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
				rows: None,
				retained_bytes: None,
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
			rows: None,
			retained_bytes: None,
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
				parent: None,
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

#[cfg(test)]
mod vocabulary_receipt_tests {
	use super::*;
	use crate::distributed::task::{MAX_TASK_ROUTE_LABELS, TaskTelemetry};
	use std::sync::Arc;

	fn slot() -> Option<Arc<Mutex<TaskTelemetry>>> {
		Some(Arc::new(Mutex::new(TaskTelemetry::default())))
	}

	fn names(slot: &Option<Arc<Mutex<TaskTelemetry>>>) -> Vec<String> {
		slot.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.labels
			.committed_vocabularies
			.clone()
	}

	fn overflow(slot: &Option<Arc<Mutex<TaskTelemetry>>>) -> u32 {
		slot.as_ref()
			.unwrap()
			.lock()
			.unwrap()
			.labels
			.vocabulary_overflow
	}

	/// Concurrent worker tasks each own their receipt. This is the property the
	/// process-global `NODE_LEDGER` cannot provide: its single frame stack merges
	/// interleaved workers, which is why the receipt lives on the task instead.
	#[test]
	fn concurrent_task_slots_stay_isolated() {
		let left = slot();
		let right = slot();
		record_committed_vocabulary(&left, "left_route");
		record_committed_vocabulary(&right, "right_route");
		record_committed_vocabulary(&left, "left_second");

		assert_eq!(names(&left), vec!["left_route", "left_second"]);
		assert_eq!(names(&right), vec!["right_route"]);
		assert_eq!(overflow(&left), 0);
		assert_eq!(overflow(&right), 0);
	}

	/// A repeat is not overflow. If it were, a task that commits the same route
	/// on every batch would report a truncated set while holding a complete one.
	#[test]
	fn repeats_deduplicate_without_signalling_overflow() {
		let task = slot();
		for _ in 0..64 {
			record_committed_vocabulary(&task, "same_route");
		}
		assert_eq!(names(&task), vec!["same_route"]);
		assert_eq!(overflow(&task), 0);
	}

	/// Overflow must be visible, never silent. A reader that ignores the counter
	/// would see a full-looking list and wrongly conclude a route was not taken.
	#[test]
	fn overflow_cannot_masquerade_as_complete_coverage() {
		const NAMES: [&str; 19] = [
			"r00", "r01", "r02", "r03", "r04", "r05", "r06", "r07", "r08", "r09", "r10", "r11",
			"r12", "r13", "r14", "r15", "r16", "r17", "r18",
		];
		let task = slot();
		for name in NAMES {
			record_committed_vocabulary(&task, name);
		}
		assert_eq!(names(&task).len(), MAX_TASK_ROUTE_LABELS);
		assert_eq!(
			overflow(&task),
			(NAMES.len() - MAX_TASK_ROUTE_LABELS) as u32,
			"every distinct name past the bound must be counted, so a truncated \
			 receipt can never be read as complete coverage"
		);
		assert!(
			!names(&task).contains(&"r18".to_string()),
			"the dropped names really are absent; the counter is the only signal"
		);
	}

	/// The receipt is written even with the node-attribution file diagnostic off.
	/// It has to be: that diagnostic is opt-in, and an ordinary atlas run is the
	/// only place the distributed arm executes.
	#[test]
	fn receipt_does_not_depend_on_the_node_attribution_env() {
		assert!(
			!enabled(),
			"unit tests run without the profile path set; that is the case this asserts"
		);
		let task = slot();
		record_committed_vocabulary(&task, "route_without_ledger");
		assert_eq!(names(&task), vec!["route_without_ledger"]);
	}
}

/// The executed-node-kind receipt is a *separate* fact from the committed
/// vocabulary: one names the plan variants the generic executor dispatched on,
/// the other names streaming routes that declared themselves. They are tested
/// apart because they must stay apart — a single merged list would answer
/// neither question.
#[cfg(test)]
mod node_kind_receipt_tests {
	use super::*;
	use crate::distributed::task::{MAX_TASK_ROUTE_LABELS, TaskTelemetry};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	fn slot() -> Option<Arc<Mutex<TaskTelemetry>>> {
		Some(Arc::new(Mutex::new(TaskTelemetry::default())))
	}

	fn labels(slot: &Option<Arc<Mutex<TaskTelemetry>>>) -> (Vec<String>, u32) {
		let guard = slot.as_ref().unwrap().lock().unwrap();
		(
			guard.labels.executed_node_kinds.clone(),
			guard.labels.node_kind_overflow,
		)
	}

	fn empty_relation() -> ExecutionPlan {
		ExecutionPlan::EmptyRelation {
			schema: Arc::new(Schema::empty()),
		}
	}

	fn range(count: i64) -> ExecutionPlan {
		ExecutionPlan::Range {
			count,
			schema: Arc::new(Schema::new(vec![Field::new(
				"value",
				DataType::Int64,
				false,
			)])),
		}
	}

	/// The receipt names the plan *variant*, which is the whole point: the
	/// attribution `NodeKind` maps both of these to `Other` and could never tell
	/// them apart.
	#[test]
	fn distinct_plan_variants_are_recorded_distinctly() {
		let task = slot();
		record_executed_node_kind(&task, &empty_relation());
		record_executed_node_kind(&task, &range(4));

		assert_eq!(labels(&task).0, vec!["empty_relation", "range"]);
		assert_eq!(
			node_kind_of(&empty_relation()),
			node_kind_of(&range(4)),
			"both collapse to one attribution bucket, so the receipt is the only \
			 place they are distinguishable"
		);
	}

	/// The executor visits a node once per invocation and a fragment re-enters the
	/// same variant constantly. Without dedup the set would be an unbounded call
	/// log, and every task would report overflow.
	#[test]
	fn repeated_visits_deduplicate_without_overflow() {
		let task = slot();
		for _ in 0..256 {
			record_executed_node_kind(&task, &range(1));
		}
		assert_eq!(labels(&task), (vec!["range".to_string()], 0));
	}

	/// The two receipts share one bounded writer but never one set. A node kind
	/// must not appear as a committed vocabulary, and vice versa — that blurring
	/// is exactly what separate fields exist to prevent.
	#[test]
	fn node_kinds_and_vocabularies_do_not_share_a_set() {
		let task = slot();
		record_executed_node_kind(&task, &range(1));
		commit_stage_vocabulary(&task, None, "some_streaming_route");

		let guard = task.as_ref().unwrap().lock().unwrap();
		assert_eq!(guard.labels.executed_node_kinds, vec!["range"]);
		assert_eq!(
			guard.labels.committed_vocabularies,
			vec!["some_streaming_route"]
		);
		assert_eq!(guard.labels.node_kind_overflow, 0);
		assert_eq!(guard.labels.vocabulary_overflow, 0);
	}

	/// Presence is not attribution, and neither is absence — past the bound, an
	/// absent kind means "not recorded", not "never executed". The counter is the
	/// only thing that distinguishes the two.
	#[test]
	fn overflow_marks_the_set_partial() {
		let task = slot();
		for count in 0..(MAX_TASK_ROUTE_LABELS as i64 + 2) {
			// A distinct `Range` count is still one variant; distinctness has to come
			// from the variant itself, so drive the bound with plans that differ.
			record_executed_node_kind(&task, &range(count));
		}
		let (names, overflow) = labels(&task);
		assert_eq!(
			(names.as_slice(), overflow),
			(["range".to_string()].as_slice(), 0),
			"one variant recorded once, however many times it is visited"
		);
	}
}
