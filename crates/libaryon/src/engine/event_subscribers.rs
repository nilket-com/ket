use super::event_bus::{EngineEvent, MAX_DRAIN_PASSES};
use super::request::{InternalEvent, InternalSender};
use super::runtime_registry::{
	ActionError, InvocationGate, LiveSubscriptionSnapshot, SubscriberInvocation,
};
use super::session::EditorSession;
use crate::core::{BufferId, RegistryOwnerId, SourceId, StateId, SubscriptionId, ViewId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

pub(crate) const MAX_BASELINE_VIEWS: usize = 16_384;
pub(crate) const MAX_BASELINE_BUFFERS: usize = 16_384;
pub(crate) const MAX_BASELINE_SOURCES: usize = 4_096;
pub(crate) const MAX_BASELINE_ATTEMPTS: u8 = 3;
pub(crate) const MAX_SUBSCRIBER_STEPS_PER_DRAIN: usize = 256;
pub(crate) const MAX_SUBSCRIBER_STATUS_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BaselineBuffer {
	pub(crate) buffer: BufferId,
	pub(crate) state: StateId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BaselineView {
	pub(crate) view: ViewId,
	pub(crate) buffer: BufferId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BaselineSource {
	pub(crate) source: SourceId,
	pub(crate) owner: RegistryOwnerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriberBaseline {
	pub(crate) buffers: Vec<BaselineBuffer>,
	pub(crate) views: Vec<BaselineView>,
	pub(crate) focused: Option<ViewId>,
	pub(crate) sources: Vec<BaselineSource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BaselineDomain {
	Buffers,
	Views,
	Sources,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BaselineError {
	TooMany {
		domain: BaselineDomain,
		limit: usize,
	},
	AllocationFailed {
		domain: BaselineDomain,
	},
	DuplicateView(ViewId),
	ConflictingBufferState(BufferId),
	OutOfOrderSource(SourceId),
	FocusedViewMissing(ViewId),
}

impl fmt::Display for BaselineError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::TooMany { domain, limit } => {
				write!(
					f,
					"Subscriber baseline {domain:?} exceed the {limit}-entry limit"
				)
			}
			Self::AllocationFailed { domain } => {
				write!(f, "Could not allocate subscriber baseline {domain:?}")
			}
			Self::DuplicateView(view) => {
				write!(f, "Subscriber baseline contains duplicate view {view}")
			}
			Self::ConflictingBufferState(buffer) => write!(
				f,
				"Subscriber baseline observed two states for buffer {buffer}"
			),
			Self::OutOfOrderSource(source) => write!(
				f,
				"Subscriber baseline source {source} is duplicated or out of order"
			),
			Self::FocusedViewMissing(view) => {
				write!(f, "Subscriber baseline focused view {view} is not live")
			}
		}
	}
}

impl std::error::Error for BaselineError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundedStatus(String);

impl BoundedStatus {
	pub(crate) fn new(value: impl Into<String>) -> Result<Self, StatusTooLong> {
		let value = value.into();
		if value.len() > MAX_SUBSCRIBER_STATUS_BYTES {
			return Err(StatusTooLong {
				actual: value.len(),
				limit: MAX_SUBSCRIBER_STATUS_BYTES,
			});
		}
		Ok(Self(value))
	}

	pub(crate) fn fixed(value: &'static str) -> Self {
		Self::new(value).expect("fixed subscriber status must fit the structural bound")
	}

	pub(crate) fn into_string(self) -> String {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatusTooLong {
	pub(crate) actual: usize,
	pub(crate) limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriberResult {
	Delivered,
	Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriberOutcome {
	pub(crate) needs_render: bool,
	pub(crate) foreground_status: Option<BoundedStatus>,
	pub(crate) result: SubscriberResult,
}

impl SubscriberOutcome {
	#[allow(dead_code)]
	pub(crate) const fn delivered() -> Self {
		Self {
			needs_render: false,
			foreground_status: None,
			result: SubscriberResult::Delivered,
		}
	}

	pub(crate) fn failed(status: BoundedStatus) -> Self {
		Self {
			needs_render: true,
			foreground_status: Some(status),
			result: SubscriberResult::Failed,
		}
	}
}

pub(crate) trait SubscriberDispatcher<C> {
	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool;

	fn initialize(
		&mut self,
		context: &mut C,
		invocation: SubscriberInvocation,
		baseline: &Arc<SubscriberBaseline>,
	) -> SubscriberOutcome;

	fn deliver(
		&mut self,
		context: &mut C,
		invocation: SubscriberInvocation,
		event: &EngineEvent,
	) -> SubscriberOutcome;
}

pub(crate) struct UnavailableSubscriberDispatcher;

impl<C> SubscriberDispatcher<C> for UnavailableSubscriberDispatcher {
	fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
		true
	}

	fn initialize(
		&mut self,
		_context: &mut C,
		_invocation: SubscriberInvocation,
		_baseline: &Arc<SubscriberBaseline>,
	) -> SubscriberOutcome {
		SubscriberOutcome::failed(BoundedStatus::fixed(
			"Runtime event subscriber dispatcher is unavailable",
		))
	}

	fn deliver(
		&mut self,
		_context: &mut C,
		_invocation: SubscriberInvocation,
		_event: &EngineEvent,
	) -> SubscriberOutcome {
		SubscriberOutcome::failed(BoundedStatus::fixed(
			"Runtime event subscriber dispatcher is unavailable",
		))
	}
}

pub(crate) trait SubscriberContext {
	fn subscriber_session(&self) -> &EditorSession;
	fn subscriber_session_mut(&mut self) -> &mut EditorSession;
}

pub(crate) trait BaselineContributor {
	fn contribute(&self, builder: &mut BaselineBuilder) -> Result<(), BaselineError>;
}

pub(crate) struct BaselineBuilder {
	buffers: Vec<BaselineBuffer>,
	buffer_states: HashMap<BufferId, StateId>,
	views: Vec<BaselineView>,
	view_ids: HashSet<ViewId>,
	focused: Option<ViewId>,
	sources: Vec<BaselineSource>,
}

impl BaselineBuilder {
	pub(crate) fn new() -> Self {
		Self {
			buffers: Vec::new(),
			buffer_states: HashMap::new(),
			views: Vec::new(),
			view_ids: HashSet::new(),
			focused: None,
			sources: Vec::new(),
		}
	}

	pub(crate) fn push_buffer(
		&mut self,
		buffer: BufferId,
		state: StateId,
	) -> Result<(), BaselineError> {
		if let Some(previous) = self.buffer_states.get(&buffer) {
			return if *previous == state {
				Ok(())
			} else {
				Err(BaselineError::ConflictingBufferState(buffer))
			};
		}
		reserve_one(
			&mut self.buffers,
			MAX_BASELINE_BUFFERS,
			BaselineDomain::Buffers,
		)?;
		self.buffer_states
			.try_reserve(1)
			.map_err(|_| BaselineError::AllocationFailed {
				domain: BaselineDomain::Buffers,
			})?;
		self.buffer_states.insert(buffer, state);
		self.buffers.push(BaselineBuffer { buffer, state });
		Ok(())
	}

	pub(crate) fn push_view(
		&mut self,
		view: ViewId,
		buffer: BufferId,
	) -> Result<(), BaselineError> {
		if self.views.len() >= MAX_BASELINE_VIEWS {
			return Err(BaselineError::TooMany {
				domain: BaselineDomain::Views,
				limit: MAX_BASELINE_VIEWS,
			});
		}
		self.views
			.try_reserve(1)
			.map_err(|_| BaselineError::AllocationFailed {
				domain: BaselineDomain::Views,
			})?;
		self.view_ids
			.try_reserve(1)
			.map_err(|_| BaselineError::AllocationFailed {
				domain: BaselineDomain::Views,
			})?;
		if !self.view_ids.insert(view) {
			return Err(BaselineError::DuplicateView(view));
		}
		self.views.push(BaselineView { view, buffer });
		Ok(())
	}

	pub(crate) fn set_focused(&mut self, focused: Option<ViewId>) {
		self.focused = focused;
	}

	pub(crate) fn push_source(
		&mut self,
		source: SourceId,
		owner: RegistryOwnerId,
	) -> Result<(), BaselineError> {
		if self
			.sources
			.last()
			.is_some_and(|previous| previous.source >= source)
		{
			return Err(BaselineError::OutOfOrderSource(source));
		}
		reserve_one(
			&mut self.sources,
			MAX_BASELINE_SOURCES,
			BaselineDomain::Sources,
		)?;
		self.sources.push(BaselineSource { source, owner });
		Ok(())
	}

	pub(crate) fn finish(mut self) -> Result<SubscriberBaseline, BaselineError> {
		if let Some(focused) = self.focused
			&& !self.view_ids.contains(&focused)
		{
			return Err(BaselineError::FocusedViewMissing(focused));
		}
		self.buffers.sort_unstable_by_key(|entry| entry.buffer);
		Ok(SubscriberBaseline {
			buffers: self.buffers,
			views: self.views,
			focused: self.focused,
			sources: self.sources,
		})
	}
}

fn reserve_one<T>(
	values: &mut Vec<T>,
	limit: usize,
	domain: BaselineDomain,
) -> Result<(), BaselineError> {
	if values.len() >= limit {
		return Err(BaselineError::TooMany { domain, limit });
	}
	values
		.try_reserve(1)
		.map_err(|_| BaselineError::AllocationFailed { domain })
}

fn compose_baseline<C: SubscriberContext>(
	context: &C,
	contributor: Option<&dyn BaselineContributor>,
) -> Result<SubscriberBaseline, BaselineError> {
	let mut builder = BaselineBuilder::new();
	context
		.subscriber_session()
		.contribute_subscriber_baseline(&mut builder)?;
	if let Some(contributor) = contributor {
		contributor.contribute(&mut builder)?;
	}
	builder.finish()
}

#[derive(Default)]
pub(crate) struct SubscriberRuntime {
	pending: Option<PendingSubscriberPass>,
}

impl SubscriberRuntime {
	pub(crate) const fn has_pending(&self) -> bool {
		self.pending.is_some()
	}
}

struct PendingSubscriberPass {
	events: VecDeque<EngineEvent>,
	live: Vec<LiveSubscriptionSnapshot>,
	needs_baseline: Vec<SubscriptionId>,
	baseline: Option<Result<Arc<SubscriberBaseline>, BaselineError>>,
	initialization_index: usize,
	event_index: usize,
	subscriber_index: usize,
	initialization_failed: bool,
}

impl PendingSubscriberPass {
	fn complete(&self) -> bool {
		self.initialization_index >= self.needs_baseline.len()
			&& self.event_index >= self.events.len()
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubscriberDrainOutcome {
	pub(crate) needs_render: bool,
	pub(crate) foreground_status: Option<BoundedStatus>,
	pub(crate) steps_used: usize,
	pub(crate) allocation_failed: bool,
}

impl SubscriberDrainOutcome {
	fn merge(&mut self, mut outcome: SubscriberOutcome) {
		self.needs_render |= outcome.needs_render;
		if let Some(status) = outcome.foreground_status.take() {
			self.foreground_status = Some(status);
			self.needs_render = true;
		}
	}

	fn status(&mut self, status: BoundedStatus) {
		self.foreground_status = Some(status);
		self.needs_render = true;
	}
}

enum CaptureResult {
	Captured,
	NoWork,
	AllocationFailed,
}

#[cfg(test)]
pub(crate) fn drain_engine_subscribers<C, D>(
	runtime: &mut SubscriberRuntime,
	context: &mut C,
	contributor: Option<&dyn BaselineContributor>,
	internal: &InternalSender,
	gate: &InvocationGate,
	dispatcher: &mut D,
	observe: impl FnMut(&EngineEvent),
) -> SubscriberDrainOutcome
where
	C: SubscriberContext,
	D: SubscriberDispatcher<C> + ?Sized,
{
	let mut routed = RoutedSubscriberDispatcher {
		dispatcher,
		contributor,
	};
	drain_engine_subscribers_combined(runtime, context, internal, gate, &mut routed, observe)
}

#[cfg(test)]
struct RoutedSubscriberDispatcher<'a, D: ?Sized> {
	dispatcher: &'a mut D,
	contributor: Option<&'a dyn BaselineContributor>,
}

#[cfg(test)]
impl<D: ?Sized> BaselineContributor for RoutedSubscriberDispatcher<'_, D> {
	fn contribute(&self, builder: &mut BaselineBuilder) -> Result<(), BaselineError> {
		if let Some(contributor) = self.contributor {
			contributor.contribute(builder)?;
		}
		Ok(())
	}
}

#[cfg(test)]
impl<C, D> SubscriberDispatcher<C> for RoutedSubscriberDispatcher<'_, D>
where
	D: SubscriberDispatcher<C> + ?Sized,
{
	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.dispatcher.owner_is_live(owner)
	}

	fn initialize(
		&mut self,
		context: &mut C,
		invocation: SubscriberInvocation,
		baseline: &Arc<SubscriberBaseline>,
	) -> SubscriberOutcome {
		self.dispatcher.initialize(context, invocation, baseline)
	}

	fn deliver(
		&mut self,
		context: &mut C,
		invocation: SubscriberInvocation,
		event: &EngineEvent,
	) -> SubscriberOutcome {
		self.dispatcher.deliver(context, invocation, event)
	}
}

pub(crate) fn drain_engine_subscribers_combined<C, D>(
	runtime: &mut SubscriberRuntime,
	context: &mut C,
	internal: &InternalSender,
	gate: &InvocationGate,
	dispatcher: &mut D,
	mut observe: impl FnMut(&EngineEvent),
) -> SubscriberDrainOutcome
where
	C: SubscriberContext,
	D: SubscriberDispatcher<C> + BaselineContributor,
{
	let mut outcome = SubscriberDrainOutcome::default();
	let mut steps = 0usize;
	let mut passes = 0usize;

	loop {
		if runtime.pending.is_none() {
			if passes >= MAX_DRAIN_PASSES {
				let session = context.subscriber_session();
				if !session.events.is_empty() || session.runtime_registries.has_needs_baseline() {
					let _ = internal.try_send(InternalEvent::DrainEvents);
				}
				break;
			}
			match capture_pending(runtime, context, Some(&*dispatcher), &mut observe) {
				CaptureResult::Captured => passes += 1,
				CaptureResult::NoWork => break,
				CaptureResult::AllocationFailed => {
					outcome.allocation_failed = true;
					outcome.status(BoundedStatus::fixed(
						"Could not allocate event subscriber snapshots",
					));
					break;
				}
			}
		}

		process_pending(
			runtime
				.pending
				.as_mut()
				.expect("captured subscriber pass must be pending"),
			context,
			gate,
			dispatcher,
			&mut steps,
			&mut outcome,
		);

		let complete = runtime
			.pending
			.as_ref()
			.is_some_and(PendingSubscriberPass::complete);
		if !complete {
			let _ = internal.try_send(InternalEvent::DrainEvents);
			break;
		}
		let completed = runtime
			.pending
			.take()
			.expect("completed subscriber pass must remain pending");
		if completed.initialization_failed && context.subscriber_session().events.is_empty() {
			break;
		}
	}
	outcome.steps_used = steps;
	outcome
}

fn capture_pending<C: SubscriberContext>(
	runtime: &mut SubscriberRuntime,
	context: &mut C,
	contributor: Option<&dyn BaselineContributor>,
	observe: &mut impl FnMut(&EngineEvent),
) -> CaptureResult {
	debug_assert!(runtime.pending.is_none());
	let session = context.subscriber_session();
	if session.events.is_empty() && !session.runtime_registries.has_needs_baseline() {
		return CaptureResult::NoWork;
	}
	let count = session.runtime_registries.subscription_count();
	let mut live = Vec::new();
	let mut needs_baseline = Vec::new();
	if live.try_reserve(count).is_err() || needs_baseline.try_reserve(count).is_err() {
		return CaptureResult::AllocationFailed;
	}

	let mut events = context.subscriber_session_mut().events.take_pass();
	for event in &events {
		observe(event);
	}
	let barrier = matches!(events.front(), Some(EngineEvent::EventsLost { .. }));
	if barrier {
		debug_assert_eq!(events.len(), 1, "loss marker must be the captured barrier");
		context
			.subscriber_session_mut()
			.runtime_registries
			.demote_live_subscriptions();
		events.clear();
	}
	context
		.subscriber_session()
		.runtime_registries
		.snapshot_subscriptions(&mut live, &mut needs_baseline);
	let baseline =
		(!needs_baseline.is_empty()).then(|| compose_baseline(context, contributor).map(Arc::new));
	runtime.pending = Some(PendingSubscriberPass {
		events,
		live,
		needs_baseline,
		baseline,
		initialization_index: 0,
		event_index: 0,
		subscriber_index: 0,
		initialization_failed: false,
	});
	CaptureResult::Captured
}

fn process_pending<C, D>(
	pending: &mut PendingSubscriberPass,
	context: &mut C,
	gate: &InvocationGate,
	dispatcher: &mut D,
	steps: &mut usize,
	drain_outcome: &mut SubscriberDrainOutcome,
) where
	C: SubscriberContext,
	D: SubscriberDispatcher<C> + ?Sized,
{
	while pending.initialization_index < pending.needs_baseline.len()
		&& *steps < MAX_SUBSCRIBER_STEPS_PER_DRAIN
	{
		let id = pending.needs_baseline[pending.initialization_index];
		pending.initialization_index += 1;
		*steps += 1;
		match pending
			.baseline
			.as_ref()
			.expect("initialization ids require one baseline result")
		{
			Ok(baseline) => {
				let invocation = context
					.subscriber_session()
					.runtime_registries
					.baseline_subscription_invocation(id);
				let Some(invocation) = invocation else {
					continue;
				};
				let result = invoke_initialize(gate, dispatcher, context, invocation, baseline);
				let delivered = result.result == SubscriberResult::Delivered;
				drain_outcome.merge(result);
				if delivered {
					context
						.subscriber_session_mut()
						.runtime_registries
						.mark_subscription_live(id);
				} else {
					pending.initialization_failed = true;
					context
						.subscriber_session_mut()
						.runtime_registries
						.record_subscription_baseline_failure(id, MAX_BASELINE_ATTEMPTS);
				}
			}
			Err(_) => {
				pending.initialization_failed = true;
				context
					.subscriber_session_mut()
					.runtime_registries
					.record_subscription_baseline_failure(id, MAX_BASELINE_ATTEMPTS);
				drain_outcome.status(BoundedStatus::fixed(
					"Could not construct event subscriber baseline",
				));
			}
		}
	}
	if pending.initialization_index >= pending.needs_baseline.len() {
		pending.baseline = None;
	}

	while pending.initialization_index >= pending.needs_baseline.len()
		&& pending.event_index < pending.events.len()
		&& *steps < MAX_SUBSCRIBER_STEPS_PER_DRAIN
	{
		if pending.live.is_empty() {
			pending.event_index = pending.events.len();
			break;
		}
		if pending.subscriber_index >= pending.live.len() {
			pending.event_index += 1;
			pending.subscriber_index = 0;
			continue;
		}
		let snapshot = pending.live[pending.subscriber_index];
		pending.subscriber_index += 1;
		*steps += 1;
		let event = pending
			.events
			.get(pending.event_index)
			.expect("event cursor must remain in bounds");
		let Some(kind) = event.kind() else {
			continue;
		};
		if !snapshot.kinds.contains(kind) {
			continue;
		}
		let invocation = context
			.subscriber_session()
			.runtime_registries
			.live_subscription_invocation(snapshot.id);
		let Some(invocation) = invocation else {
			continue;
		};
		let result = invoke_delivery(gate, dispatcher, context, invocation, event);
		drain_outcome.merge(result);
	}
	if pending.event_index < pending.events.len() && pending.subscriber_index >= pending.live.len()
	{
		pending.event_index += 1;
		pending.subscriber_index = 0;
	}
}

fn invoke_initialize<C, D>(
	gate: &InvocationGate,
	dispatcher: &mut D,
	context: &mut C,
	invocation: SubscriberInvocation,
	baseline: &Arc<SubscriberBaseline>,
) -> SubscriberOutcome
where
	D: SubscriberDispatcher<C> + ?Sized,
{
	let owner = invocation.owner();
	let owner_is_live = dispatcher.owner_is_live(owner);
	match gate.invoke_guarded(owner, owner_is_live, || {
		dispatcher.initialize(context, invocation, baseline)
	}) {
		Ok(outcome) => outcome,
		Err(error) => gate_failure(error),
	}
}

fn invoke_delivery<C, D>(
	gate: &InvocationGate,
	dispatcher: &mut D,
	context: &mut C,
	invocation: SubscriberInvocation,
	event: &EngineEvent,
) -> SubscriberOutcome
where
	D: SubscriberDispatcher<C> + ?Sized,
{
	let owner = invocation.owner();
	let owner_is_live = dispatcher.owner_is_live(owner);
	match gate.invoke_guarded(owner, owner_is_live, || {
		dispatcher.deliver(context, invocation, event)
	}) {
		Ok(outcome) => outcome,
		Err(error) => gate_failure(error),
	}
}

fn gate_failure(error: ActionError) -> SubscriberOutcome {
	let status = BoundedStatus::new(error.to_string())
		.unwrap_or_else(|_| BoundedStatus::fixed("Runtime event subscriber invocation failed"));
	SubscriberOutcome::failed(status)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ecs::UastRegistry;
	use crate::engine::event_bus::{EventKind, EventKindSet};
	use crate::engine::request::EngineRequest;
	use crate::engine::runtime_registry::{
		ActionLease, DynamicActionExecutor, DynamicActionOutcome, DynamicInvocation,
		InvocationSnapshot, RegistrationBatch, RuntimeSubscriptionRegistration,
		SubscriptionDeliveryState,
	};
	use crate::engine::settings::SaveDurability;
	use crate::engine::ui_colors::UiColorOverrides;
	use std::collections::{BTreeMap, HashSet};
	use std::sync::mpsc;

	struct TestContext {
		_registry: UastRegistry,
		session: EditorSession,
	}

	impl TestContext {
		fn new() -> Self {
			let registry = UastRegistry::new(256);
			let session = EditorSession::new(
				&registry,
				"onedark".to_string(),
				true,
				true,
				true,
				false,
				true,
				true,
				SaveDurability::default(),
				UiColorOverrides::default(),
				BTreeMap::new(),
				None,
			);
			Self {
				_registry: registry,
				session,
			}
		}
	}

	impl SubscriberContext for TestContext {
		fn subscriber_session(&self) -> &EditorSession {
			&self.session
		}

		fn subscriber_session_mut(&mut self) -> &mut EditorSession {
			&mut self.session
		}
	}

	struct FailingContributor;

	impl BaselineContributor for FailingContributor {
		fn contribute(&self, _builder: &mut BaselineBuilder) -> Result<(), BaselineError> {
			Err(BaselineError::AllocationFailed {
				domain: BaselineDomain::Sources,
			})
		}
	}

	struct ReentryExecutor;

	impl DynamicActionExecutor<()> for ReentryExecutor {
		type Output = DynamicActionOutcome;

		fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
			true
		}

		fn invoke(
			&mut self,
			_context: &mut (),
			_invocation: InvocationSnapshot,
		) -> Result<Self::Output, ActionError> {
			Ok(DynamicActionOutcome::default())
		}
	}

	#[derive(Debug, Clone)]
	struct TestSubscriber(String);

	#[derive(Default)]
	struct RecordingDispatcher {
		calls: Vec<String>,
		fail_initialization: HashSet<String>,
		fail_delivery: HashSet<String>,
		fire_during_initialization: Option<(String, EngineEvent)>,
		overflow_during_initialization: Option<String>,
		remove_on_delivery: Option<(String, RegistryOwnerId)>,
		register_on_delivery: Option<(String, String, EventKindSet)>,
		reenter_gate: Option<std::rc::Rc<InvocationGate>>,
		reentry_rejected: bool,
	}

	impl SubscriberDispatcher<TestContext> for RecordingDispatcher {
		fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
			true
		}

		fn initialize(
			&mut self,
			context: &mut TestContext,
			invocation: SubscriberInvocation,
			baseline: &Arc<SubscriberBaseline>,
		) -> SubscriberOutcome {
			let label = invocation
				.action::<TestSubscriber>()
				.expect("test subscriber payload")
				.0
				.clone();
			self.calls.push(format!("init:{label}"));
			assert!(!baseline.buffers.is_empty());
			if self
				.fire_during_initialization
				.as_ref()
				.is_some_and(|(target, _)| target == &label)
			{
				let (_, event) = self
					.fire_during_initialization
					.take()
					.expect("matched initializer event");
				context.session.events.push(event);
			}
			if self
				.overflow_during_initialization
				.as_ref()
				.is_some_and(|target| target == &label)
			{
				self.overflow_during_initialization = None;
				for index in 0..=super::super::event_bus::MAX_PENDING_EVENTS {
					context.session.events.push(EngineEvent::BufferOpened {
						buffer: BufferId::new(index as u64 + 1),
						state: StateId::new(1),
					});
				}
			}
			if self.fail_initialization.contains(&label) {
				SubscriberOutcome::failed(BoundedStatus::fixed("test initialization failed"))
			} else {
				SubscriberOutcome::delivered()
			}
		}

		fn deliver(
			&mut self,
			context: &mut TestContext,
			invocation: SubscriberInvocation,
			event: &EngineEvent,
		) -> SubscriberOutcome {
			let label = invocation
				.action::<TestSubscriber>()
				.expect("test subscriber payload")
				.0
				.clone();
			self.calls.push(format!(
				"event:{label}:{:?}",
				event.kind().expect("ordinary event")
			));
			if self
				.remove_on_delivery
				.as_ref()
				.is_some_and(|(target, _)| target == &label)
			{
				let (_, owner) = self.remove_on_delivery.take().expect("matched removal");
				context
					.session
					.runtime_registries
					.remove_owner(owner)
					.expect("remove subscriber owner");
			}
			if self
				.register_on_delivery
				.as_ref()
				.is_some_and(|(target, _, _)| target == &label)
			{
				let (_, registered, kinds) = self
					.register_on_delivery
					.take()
					.expect("matched registration");
				publish(context, [registered], kinds);
			}
			if let Some(gate) = &self.reenter_gate {
				let owner = invocation.owner();
				let nested = InvocationSnapshot::new(
					crate::core::RegistryRev::ZERO,
					ActionLease::new(owner, TestSubscriber("nested".to_string())),
					DynamicInvocation::Command {
						canonical_name: "nested".to_string(),
						spelling: "nested".to_string(),
						args: String::new(),
						bang: false,
					},
				);
				let mut executor = ReentryExecutor;
				self.reentry_rejected = matches!(
					gate.invoke_dynamic(&mut executor, &mut (), nested),
					Err(ActionError::ReentrantInvocation)
				);
			}
			if self.fail_delivery.contains(&label) {
				SubscriberOutcome::failed(BoundedStatus::fixed("test delivery failed"))
			} else {
				SubscriberOutcome::delivered()
			}
		}
	}

	fn publish(
		context: &mut TestContext,
		labels: impl IntoIterator<Item = String>,
		kinds: EventKindSet,
	) -> (RegistryOwnerId, Vec<SubscriptionId>) {
		let owner = context
			.session
			.runtime_registries
			.allocate_owner()
			.expect("owner");
		let mut batch = RegistrationBatch::new(owner);
		batch.subscriptions = labels
			.into_iter()
			.map(|label| RuntimeSubscriptionRegistration {
				kinds,
				action: ActionLease::new(owner, TestSubscriber(label)),
			})
			.collect();
		context
			.session
			.runtime_registries
			.commit_owner_batch(batch)
			.expect("publish subscribers");
		let mut live = Vec::new();
		let mut needs = Vec::new();
		context
			.session
			.runtime_registries
			.snapshot_subscriptions(&mut live, &mut needs);
		let ids = needs
			.into_iter()
			.filter(|id| {
				context
					.session
					.runtime_registries
					.baseline_subscription_invocation(*id)
					.is_some_and(|invocation| invocation.owner() == owner)
			})
			.collect();
		(owner, ids)
	}

	fn drain(
		context: &mut TestContext,
		dispatcher: &mut RecordingDispatcher,
		internal: &InternalSender,
		gate: &InvocationGate,
	) -> SubscriberDrainOutcome {
		drain_with_contributor(context, dispatcher, internal, gate, None)
	}

	fn drain_with_contributor(
		context: &mut TestContext,
		dispatcher: &mut RecordingDispatcher,
		internal: &InternalSender,
		gate: &InvocationGate,
		contributor: Option<&dyn BaselineContributor>,
	) -> SubscriberDrainOutcome {
		let mut runtime = std::mem::take(&mut context.session.subscribers);
		let outcome = drain_engine_subscribers(
			&mut runtime,
			context,
			contributor,
			internal,
			gate,
			dispatcher,
			|_| {},
		);
		context.session.subscribers = runtime;
		outcome
	}

	#[test]
	fn builder_preserves_declared_orders_and_rejects_duplicates() {
		let mut builder = BaselineBuilder::new();
		builder
			.push_buffer(BufferId::new(1), StateId::new(2))
			.expect("buffer");
		builder
			.push_view(ViewId::new(2), BufferId::new(1))
			.expect("view");
		builder.set_focused(Some(ViewId::new(2)));
		builder
			.push_source(SourceId::new(4), RegistryOwnerId::new(5))
			.expect("source");
		let baseline = builder.finish().expect("baseline");
		assert_eq!(baseline.buffers[0].buffer, BufferId::new(1));
		assert_eq!(baseline.views[0].view, ViewId::new(2));
		assert_eq!(baseline.focused, Some(ViewId::new(2)));
		assert_eq!(baseline.sources[0].source, SourceId::new(4));

		let mut builder = BaselineBuilder::new();
		builder
			.push_view(ViewId::new(1), BufferId::new(1))
			.expect("first view");
		assert_eq!(
			builder.push_view(ViewId::new(1), BufferId::new(1)),
			Err(BaselineError::DuplicateView(ViewId::new(1)))
		);
	}

	#[test]
	fn baseline_count_caps_accept_the_boundary_and_reject_the_next_entry() {
		let mut buffers = BaselineBuilder::new();
		for index in 0..MAX_BASELINE_BUFFERS {
			buffers
				.push_buffer(BufferId::new(index as u64 + 1), StateId::new(1))
				.expect("buffer boundary");
		}
		assert_eq!(
			buffers.push_buffer(
				BufferId::new(MAX_BASELINE_BUFFERS as u64 + 1),
				StateId::new(1)
			),
			Err(BaselineError::TooMany {
				domain: BaselineDomain::Buffers,
				limit: MAX_BASELINE_BUFFERS,
			})
		);

		let mut views = BaselineBuilder::new();
		for index in 0..MAX_BASELINE_VIEWS {
			views
				.push_view(ViewId::new(index as u64 + 1), BufferId::new(1))
				.expect("view boundary");
		}
		assert_eq!(
			views.push_view(ViewId::new(MAX_BASELINE_VIEWS as u64 + 1), BufferId::new(1)),
			Err(BaselineError::TooMany {
				domain: BaselineDomain::Views,
				limit: MAX_BASELINE_VIEWS,
			})
		);

		let mut sources = BaselineBuilder::new();
		for index in 0..MAX_BASELINE_SOURCES {
			sources
				.push_source(
					SourceId::new(index as u64 + 1),
					RegistryOwnerId::new(index as u64 + 1),
				)
				.expect("source boundary");
		}
		assert_eq!(
			sources.push_source(
				SourceId::new(MAX_BASELINE_SOURCES as u64 + 1),
				RegistryOwnerId::new(1),
			),
			Err(BaselineError::TooMany {
				domain: BaselineDomain::Sources,
				limit: MAX_BASELINE_SOURCES,
			})
		);
	}

	#[test]
	fn baseline_cutoff_excludes_new_subscriber_from_the_captured_pass() {
		let mut context = TestContext::new();
		let kinds = EventKindSet::from_kinds([EventKind::BufferOpened, EventKind::BufferClosed]);
		let (_, first_ids) = publish(&mut context, ["first".to_string()], kinds);
		context
			.session
			.runtime_registries
			.mark_subscription_live(first_ids[0]);
		let (_, second_ids) = publish(&mut context, ["second".to_string()], kinds);
		let mut dispatcher = RecordingDispatcher {
			fire_during_initialization: Some((
				"second".to_string(),
				EngineEvent::BufferClosed {
					buffer: BufferId::new(1),
				},
			)),
			..RecordingDispatcher::default()
		};
		context.session.events.push(EngineEvent::BufferOpened {
			buffer: BufferId::new(1),
			state: StateId::new(1),
		});
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert_eq!(
			dispatcher.calls,
			[
				"init:second",
				"event:first:BufferOpened",
				"event:first:BufferClosed",
				"event:second:BufferClosed",
			]
		);
		assert_eq!(
			context
				.session
				.runtime_registries
				.subscription_state(second_ids[0]),
			Some(SubscriptionDeliveryState::Live)
		);
	}

	#[test]
	fn empty_queue_initialization_retries_once_per_external_drain_then_disables() {
		let mut context = TestContext::new();
		let (_, ids) = publish(
			&mut context,
			["broken".to_string()],
			EventKindSet::one(EventKind::BufferOpened),
		);
		let revision = context.session.runtime_registries.revision();
		let mut dispatcher = RecordingDispatcher::default();
		dispatcher.fail_initialization.insert("broken".to_string());
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		let gate = InvocationGate::default();
		for failures in 1..=MAX_BASELINE_ATTEMPTS {
			drain(&mut context, &mut dispatcher, &internal, &gate);
			let expected = if failures == MAX_BASELINE_ATTEMPTS {
				SubscriptionDeliveryState::Disabled
			} else {
				SubscriptionDeliveryState::NeedsBaseline {
					consecutive_failures: failures,
				}
			};
			assert_eq!(
				context
					.session
					.runtime_registries
					.subscription_state(ids[0]),
				Some(expected)
			);
		}
		assert_eq!(dispatcher.calls.len(), MAX_BASELINE_ATTEMPTS as usize);
		assert_eq!(context.session.runtime_registries.revision(), revision);
	}

	#[test]
	fn owner_removal_during_delivery_suppresses_the_stale_snapshot_id() {
		let mut context = TestContext::new();
		let kinds = EventKindSet::one(EventKind::BufferOpened);
		let (_, first_ids) = publish(&mut context, ["first".to_string()], kinds);
		let (second_owner, second_ids) = publish(&mut context, ["second".to_string()], kinds);
		for id in [first_ids[0], second_ids[0]] {
			context
				.session
				.runtime_registries
				.mark_subscription_live(id);
		}
		let mut dispatcher = RecordingDispatcher {
			remove_on_delivery: Some(("first".to_string(), second_owner)),
			..RecordingDispatcher::default()
		};
		context.session.events.push(EngineEvent::BufferOpened {
			buffer: BufferId::new(1),
			state: StateId::new(1),
		});
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert_eq!(dispatcher.calls, ["event:first:BufferOpened"]);
		assert!(
			context
				.session
				.runtime_registries
				.subscription_state(second_ids[0])
				.is_none()
		);
	}

	#[test]
	fn registration_during_delivery_starts_at_the_next_boundary_and_skips_current_event() {
		let mut context = TestContext::new();
		let kinds = EventKindSet::one(EventKind::BufferOpened);
		let (_, ids) = publish(&mut context, ["first".to_string()], kinds);
		context
			.session
			.runtime_registries
			.mark_subscription_live(ids[0]);
		let mut dispatcher = RecordingDispatcher {
			register_on_delivery: Some(("first".to_string(), "new".to_string(), kinds)),
			..RecordingDispatcher::default()
		};
		context.session.events.push(EngineEvent::BufferOpened {
			buffer: BufferId::new(1),
			state: StateId::new(1),
		});
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert_eq!(dispatcher.calls, ["event:first:BufferOpened", "init:new"]);
	}

	#[test]
	fn failed_delivery_does_not_stop_sibling_delivery() {
		let mut context = TestContext::new();
		let kinds = EventKindSet::one(EventKind::BufferOpened);
		let (_, ids) = publish(
			&mut context,
			["broken".to_string(), "healthy".to_string()],
			kinds,
		);
		for id in ids {
			context
				.session
				.runtime_registries
				.mark_subscription_live(id);
		}
		let mut dispatcher = RecordingDispatcher::default();
		dispatcher.fail_delivery.insert("broken".to_string());
		context.session.events.push(EngineEvent::BufferOpened {
			buffer: BufferId::new(1),
			state: StateId::new(1),
		});
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		let outcome = drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert_eq!(
			dispatcher.calls,
			["event:broken:BufferOpened", "event:healthy:BufferOpened"]
		);
		assert!(outcome.needs_render);
		assert_eq!(
			outcome.foreground_status,
			Some(BoundedStatus::fixed("test delivery failed"))
		);
	}

	#[test]
	fn baseline_construction_failure_retries_and_disables_without_invoking_dispatcher() {
		let mut context = TestContext::new();
		let (_, ids) = publish(
			&mut context,
			["blocked".to_string()],
			EventKindSet::one(EventKind::BufferOpened),
		);
		let mut dispatcher = RecordingDispatcher::default();
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		let gate = InvocationGate::default();
		for _ in 0..MAX_BASELINE_ATTEMPTS {
			let outcome = drain_with_contributor(
				&mut context,
				&mut dispatcher,
				&internal,
				&gate,
				Some(&FailingContributor),
			);
			assert!(outcome.needs_render);
		}
		assert!(dispatcher.calls.is_empty());
		assert_eq!(
			context
				.session
				.runtime_registries
				.subscription_state(ids[0]),
			Some(SubscriptionDeliveryState::Disabled)
		);
	}

	#[test]
	fn fanout_slices_resume_before_newer_events_and_share_the_invocation_gate() {
		let mut context = TestContext::new();
		let kinds = EventKindSet::from_kinds([
			EventKind::BufferOpened,
			EventKind::BufferClosed,
			EventKind::HydrationCompleted,
		]);
		let labels = (0..200).map(|index| format!("s{index:03}"));
		let (_, ids) = publish(&mut context, labels, kinds);
		for id in ids {
			context
				.session
				.runtime_registries
				.mark_subscription_live(id);
		}
		context.session.events.push(EngineEvent::BufferOpened {
			buffer: BufferId::new(1),
			state: StateId::new(1),
		});
		context.session.events.push(EngineEvent::BufferClosed {
			buffer: BufferId::new(1),
		});
		let (_commands, _input, internal, requests) = super::super::request::channel();
		let gate = std::rc::Rc::new(InvocationGate::default());
		let mut dispatcher = RecordingDispatcher {
			reenter_gate: Some(std::rc::Rc::clone(&gate)),
			..RecordingDispatcher::default()
		};
		drain(&mut context, &mut dispatcher, &internal, gate.as_ref());
		assert_eq!(dispatcher.calls.len(), MAX_SUBSCRIBER_STEPS_PER_DRAIN);
		assert!(dispatcher.reentry_rejected);
		assert!(matches!(
			requests.try_recv(),
			Ok(EngineRequest::Internal(InternalEvent::DrainEvents))
		));

		context
			.session
			.events
			.push(EngineEvent::HydrationCompleted {
				buffer: BufferId::new(1),
			});
		while context.session.subscribers.pending.is_some() || !context.session.events.is_empty() {
			drain(&mut context, &mut dispatcher, &internal, gate.as_ref());
		}
		let first_new = dispatcher
			.calls
			.iter()
			.position(|call| call.ends_with("HydrationCompleted"))
			.expect("newer event delivered");
		let last_old = dispatcher
			.calls
			.iter()
			.rposition(|call| call.ends_with("BufferClosed"))
			.expect("older event delivered");
		assert!(last_old < first_new);
	}

	#[test]
	fn full_request_channel_still_contains_a_request_that_will_reach_the_drain_point() {
		let mut context = TestContext::new();
		let kinds = EventKindSet::from_kinds([EventKind::BufferOpened, EventKind::BufferClosed]);
		let (_, ids) = publish(
			&mut context,
			(0..200).map(|index| format!("s{index:03}")),
			kinds,
		);
		for id in ids {
			context
				.session
				.runtime_registries
				.mark_subscription_live(id);
		}
		context.session.events.push(EngineEvent::BufferOpened {
			buffer: BufferId::new(1),
			state: StateId::new(1),
		});
		context.session.events.push(EngineEvent::BufferClosed {
			buffer: BufferId::new(1),
		});
		let (_commands, _input, internal, requests) = super::super::request::channel();
		loop {
			match internal.try_send(InternalEvent::Refresh) {
				Ok(()) => {}
				Err(mpsc::TrySendError::Full(InternalEvent::Refresh)) => break,
				Err(error) => panic!("unexpected request fill result: {error:?}"),
			}
		}
		let mut dispatcher = RecordingDispatcher::default();
		drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert!(context.session.subscribers.pending.is_some());
		assert!(matches!(
			requests.try_recv(),
			Ok(EngineRequest::Internal(InternalEvent::Refresh))
		));
	}

	#[test]
	fn overflow_barrier_rebuilds_live_subscriptions_without_delivering_the_marker() {
		let mut context = TestContext::new();
		let (_, ids) = publish(
			&mut context,
			["live".to_string()],
			EventKindSet::one(EventKind::BufferOpened),
		);
		context
			.session
			.runtime_registries
			.mark_subscription_live(ids[0]);
		for index in 0..=super::super::event_bus::MAX_PENDING_EVENTS {
			context.session.events.push(EngineEvent::BufferOpened {
				buffer: BufferId::new(index as u64 + 1),
				state: StateId::new(1),
			});
		}
		let mut dispatcher = RecordingDispatcher::default();
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert_eq!(dispatcher.calls, ["init:live"]);
		assert_eq!(
			context
				.session
				.runtime_registries
				.subscription_state(ids[0]),
			Some(SubscriptionDeliveryState::Live)
		);
	}

	#[test]
	fn recovery_overflow_during_initialization_triggers_a_second_nonrecursive_rebuild() {
		let mut context = TestContext::new();
		let (_, ids) = publish(
			&mut context,
			["live".to_string()],
			EventKindSet::one(EventKind::BufferOpened),
		);
		context
			.session
			.runtime_registries
			.mark_subscription_live(ids[0]);
		for index in 0..=super::super::event_bus::MAX_PENDING_EVENTS {
			context.session.events.push(EngineEvent::BufferOpened {
				buffer: BufferId::new(index as u64 + 1),
				state: StateId::new(1),
			});
		}
		let mut dispatcher = RecordingDispatcher {
			overflow_during_initialization: Some("live".to_string()),
			..RecordingDispatcher::default()
		};
		let (_commands, _input, internal, _requests) = super::super::request::channel();
		drain(
			&mut context,
			&mut dispatcher,
			&internal,
			&InvocationGate::default(),
		);
		assert_eq!(dispatcher.calls, ["init:live", "init:live"]);
		assert_eq!(
			context
				.session
				.runtime_registries
				.subscription_state(ids[0]),
			Some(SubscriptionDeliveryState::Live)
		);
	}

	#[test]
	fn bounded_status_rejects_oversized_dispatcher_output() {
		assert_eq!(
			BoundedStatus::new("x".repeat(MAX_SUBSCRIBER_STATUS_BYTES + 1)),
			Err(StatusTooLong {
				actual: MAX_SUBSCRIBER_STATUS_BYTES + 1,
				limit: MAX_SUBSCRIBER_STATUS_BYTES,
			})
		);
	}
}
