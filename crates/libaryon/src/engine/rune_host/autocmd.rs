use super::buffer_api::{RuneBuffer, RuneVersion, RuneView};
use super::facade::{EditorHost, lock_services};
use super::registration::{REGISTRATION_PHASE_ERROR, RuneCallbackRef, stage};
use crate::core::{BufferId, RegistryOwnerId, SourceId};
use crate::engine::event_bus::{EngineEvent, EventKind, EventKindSet};
use crate::engine::event_subscribers::SubscriberBaseline;
use rune::alloc::Vec as RuneVec;
use rune::alloc::fmt::TryWrite;
use rune::runtime::{Formatter, Function, Protocol, VmResult};
use rune::{Any, Module};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuneAutocmdRef {
	pub(crate) initialize: RuneCallbackRef,
	pub(crate) event: RuneCallbackRef,
	pub(crate) buffer_filter: Option<BufferId>,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = EventKind)]
pub(crate) enum RuneEventKind {
	#[rune(constructor)]
	BufferOpened,
	#[rune(constructor)]
	BufferClosed,
	#[rune(constructor)]
	ViewCreated,
	#[rune(constructor)]
	ViewRetargeted,
	#[rune(constructor)]
	ViewClosed,
	#[rune(constructor)]
	FocusedTextViewChanged,
	#[rune(constructor)]
	BufferEdited,
	#[rune(constructor)]
	BufferReplaced,
	#[rune(constructor)]
	SourceLoaded,
	#[rune(constructor)]
	SourceUnloaded,
	#[rune(constructor)]
	HydrationCompleted,
}

impl From<RuneEventKind> for EventKind {
	fn from(value: RuneEventKind) -> Self {
		match value {
			RuneEventKind::BufferOpened => Self::BufferOpened,
			RuneEventKind::BufferClosed => Self::BufferClosed,
			RuneEventKind::ViewCreated => Self::ViewCreated,
			RuneEventKind::ViewRetargeted => Self::ViewRetargeted,
			RuneEventKind::ViewClosed => Self::ViewClosed,
			RuneEventKind::FocusedTextViewChanged => Self::FocusedTextViewChanged,
			RuneEventKind::BufferEdited => Self::BufferEdited,
			RuneEventKind::BufferReplaced => Self::BufferReplaced,
			RuneEventKind::SourceLoaded => Self::SourceLoaded,
			RuneEventKind::SourceUnloaded => Self::SourceUnloaded,
			RuneEventKind::HydrationCompleted => Self::HydrationCompleted,
		}
	}
}

impl From<EventKind> for RuneEventKind {
	fn from(value: EventKind) -> Self {
		match value {
			EventKind::BufferOpened => Self::BufferOpened,
			EventKind::BufferClosed => Self::BufferClosed,
			EventKind::ViewCreated => Self::ViewCreated,
			EventKind::ViewRetargeted => Self::ViewRetargeted,
			EventKind::ViewClosed => Self::ViewClosed,
			EventKind::FocusedTextViewChanged => Self::FocusedTextViewChanged,
			EventKind::BufferEdited => Self::BufferEdited,
			EventKind::BufferReplaced => Self::BufferReplaced,
			EventKind::SourceLoaded => Self::SourceLoaded,
			EventKind::SourceUnloaded => Self::SourceUnloaded,
			EventKind::HydrationCompleted => Self::HydrationCompleted,
		}
	}
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Source)]
pub(crate) struct RuneSource(SourceId);

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Owner)]
pub(crate) struct RuneOwner(RegistryOwnerId);

#[derive(Any)]
#[rune(item = ::baryon, name = AutocmdEvent)]
pub(crate) struct RuneAutocmdEvent {
	event: EngineEvent,
}

impl RuneAutocmdEvent {
	pub(crate) fn new(event: &EngineEvent) -> Self {
		Self {
			event: event.clone(),
		}
	}
}

#[derive(Any)]
#[rune(item = ::baryon, name = AutocmdBaseline)]
pub(crate) struct RuneAutocmdBaseline {
	baseline: Arc<SubscriberBaseline>,
}

impl RuneAutocmdBaseline {
	pub(crate) fn new(baseline: Arc<SubscriberBaseline>) -> Self {
		Self { baseline }
	}
}

pub(crate) fn event_matches_buffer(event: &EngineEvent, filter: BufferId) -> bool {
	match event {
		EngineEvent::BufferOpened { buffer, .. }
		| EngineEvent::BufferClosed { buffer }
		| EngineEvent::BufferEdited { buffer, .. }
		| EngineEvent::BufferReplaced { buffer, .. }
		| EngineEvent::HydrationCompleted { buffer }
		| EngineEvent::ViewCreated { buffer, .. }
		| EngineEvent::ViewClosed { buffer, .. } => *buffer == filter,
		EngineEvent::ViewRetargeted {
			old_buffer,
			new_buffer,
			..
		} => *old_buffer == filter || *new_buffer == filter,
		EngineEvent::FocusedTextViewChanged { .. }
		| EngineEvent::SourceLoaded { .. }
		| EngineEvent::SourceUnloaded { .. }
		| EngineEvent::EventsLost { .. } => false,
	}
}

fn charge_call(editor: &EditorHost) -> Result<(), &'static str> {
	lock_services(&editor.services).budget.charge()
}

fn register_autocmd(
	editor: &mut EditorHost,
	kinds: RuneVec<RuneEventKind>,
	initialize: Function,
	event: Function,
) -> VmResult<()> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	if editor.staging.is_none() {
		return VmResult::panic(REGISTRATION_PHASE_ERROR);
	}
	register_autocmd_inner(editor, kinds, initialize, event, None)
}

fn register_autocmd_for_buffer(
	editor: &mut EditorHost,
	buffer: &RuneBuffer,
	kinds: RuneVec<RuneEventKind>,
	initialize: Function,
	event: Function,
) -> VmResult<()> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	if editor.staging.is_none() {
		return VmResult::panic(REGISTRATION_PHASE_ERROR);
	}
	let buffer_id = buffer.id();
	let Some(buffer) = editor.session.buffers.get(buffer_id) else {
		return VmResult::panic("Rune autocmd buffer filter is stale");
	};
	if buffer.text().is_none() {
		return VmResult::panic("Rune autocmd buffer filter is not a text buffer");
	}
	register_autocmd_inner(editor, kinds, initialize, event, Some(buffer_id))
}

fn register_autocmd_inner(
	editor: &mut EditorHost,
	kinds: RuneVec<RuneEventKind>,
	initialize: Function,
	event: Function,
	buffer_filter: Option<BufferId>,
) -> VmResult<()> {
	let kinds = match collect_kind_set(&kinds) {
		Ok(kinds) => kinds,
		Err(error) => return VmResult::panic(error),
	};
	if let Err(error) = validate_buffer_filter(buffer_filter, kinds) {
		return VmResult::panic(error);
	}
	stage(editor, |staging| {
		staging.push_autocmd(kinds, initialize, event, buffer_filter)
	})
}

fn collect_kind_set(kinds: &RuneVec<RuneEventKind>) -> Result<EventKindSet, &'static str> {
	if kinds.is_empty() {
		return Err("Rune autocmd event-kind set cannot be empty");
	}
	if kinds.len() > EventKind::COUNT {
		return Err("Rune autocmd event-kind input exceeds the event-kind count");
	}
	Ok(EventKindSet::from_kinds(
		kinds.iter().copied().map(EventKind::from),
	))
}

fn validate_buffer_filter(
	buffer_filter: Option<BufferId>,
	kinds: EventKindSet,
) -> Result<(), &'static str> {
	if buffer_filter.is_some()
		&& [
			EventKind::FocusedTextViewChanged,
			EventKind::SourceLoaded,
			EventKind::SourceUnloaded,
		]
		.into_iter()
		.any(|kind| kinds.contains(kind))
	{
		return Err("Buffer-filtered Rune autocmds cannot subscribe to buffer-less events");
	}
	Ok(())
}

fn event_kind(event: &RuneAutocmdEvent) -> RuneEventKind {
	event
		.event
		.kind()
		.expect("Rune autocmds never receive the loss barrier")
		.into()
}

fn event_buffer(event: &RuneAutocmdEvent) -> Option<RuneBuffer> {
	let buffer = match &event.event {
		EngineEvent::BufferOpened { buffer, .. }
		| EngineEvent::BufferClosed { buffer }
		| EngineEvent::BufferEdited { buffer, .. }
		| EngineEvent::BufferReplaced { buffer, .. }
		| EngineEvent::HydrationCompleted { buffer }
		| EngineEvent::ViewCreated { buffer, .. }
		| EngineEvent::ViewClosed { buffer, .. } => *buffer,
		_ => return None,
	};
	Some(RuneBuffer::from_id(buffer))
}

fn event_view(event: &RuneAutocmdEvent) -> Option<RuneView> {
	let view = match &event.event {
		EngineEvent::ViewCreated { view, .. }
		| EngineEvent::ViewRetargeted { view, .. }
		| EngineEvent::ViewClosed { view, .. } => *view,
		_ => return None,
	};
	Some(RuneView::from_id(view))
}

fn event_acting_view(event: &RuneAutocmdEvent) -> Option<RuneView> {
	match &event.event {
		EngineEvent::BufferEdited { acting_view, .. } => Some(RuneView::from_id(*acting_view)),
		_ => None,
	}
}

fn event_old_buffer(event: &RuneAutocmdEvent) -> Option<RuneBuffer> {
	match &event.event {
		EngineEvent::ViewRetargeted { old_buffer, .. } => Some(RuneBuffer::from_id(*old_buffer)),
		_ => None,
	}
}

fn event_new_buffer(event: &RuneAutocmdEvent) -> Option<RuneBuffer> {
	match &event.event {
		EngineEvent::ViewRetargeted { new_buffer, .. } => Some(RuneBuffer::from_id(*new_buffer)),
		_ => None,
	}
}

fn event_version(event: &RuneAutocmdEvent) -> Option<RuneVersion> {
	match &event.event {
		EngineEvent::BufferOpened { state, .. } => Some(RuneVersion::from_state(*state)),
		_ => None,
	}
}

fn event_old_version(event: &RuneAutocmdEvent) -> Option<RuneVersion> {
	match &event.event {
		EngineEvent::BufferEdited { old_state, .. }
		| EngineEvent::BufferReplaced { old_state, .. } => Some(RuneVersion::from_state(*old_state)),
		_ => None,
	}
}

fn event_new_version(event: &RuneAutocmdEvent) -> Option<RuneVersion> {
	match &event.event {
		EngineEvent::BufferEdited { new_state, .. }
		| EngineEvent::BufferReplaced { new_state, .. } => Some(RuneVersion::from_state(*new_state)),
		_ => None,
	}
}

fn event_delta_count(event: &RuneAutocmdEvent) -> Option<u64> {
	match &event.event {
		EngineEvent::BufferEdited { delta_count, .. } => Some(*delta_count),
		_ => None,
	}
}

fn event_deleted_bytes(event: &RuneAutocmdEvent) -> Option<u64> {
	match &event.event {
		EngineEvent::BufferEdited { deleted_bytes, .. } => Some(*deleted_bytes),
		_ => None,
	}
}

fn event_inserted_bytes(event: &RuneAutocmdEvent) -> Option<u64> {
	match &event.event {
		EngineEvent::BufferEdited { inserted_bytes, .. } => Some(*inserted_bytes),
		_ => None,
	}
}

fn event_from(event: &RuneAutocmdEvent) -> Option<RuneView> {
	match &event.event {
		EngineEvent::FocusedTextViewChanged { from, .. } => from.map(RuneView::from_id),
		_ => None,
	}
}

fn event_to(event: &RuneAutocmdEvent) -> Option<RuneView> {
	match &event.event {
		EngineEvent::FocusedTextViewChanged { to, .. } => to.map(RuneView::from_id),
		_ => None,
	}
}

fn event_source(event: &RuneAutocmdEvent) -> Option<RuneSource> {
	match &event.event {
		EngineEvent::SourceLoaded { source, .. } | EngineEvent::SourceUnloaded { source, .. } => {
			Some(RuneSource(*source))
		}
		_ => None,
	}
}

fn event_owner(event: &RuneAutocmdEvent) -> Option<RuneOwner> {
	match &event.event {
		EngineEvent::SourceLoaded { owner, .. } | EngineEvent::SourceUnloaded { owner, .. } => {
			Some(RuneOwner(*owner))
		}
		_ => None,
	}
}

fn baseline_buffers(
	baseline: &RuneAutocmdBaseline,
	editor: &mut EditorHost,
) -> VmResult<RuneVec<(RuneBuffer, RuneVersion)>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let mut values = match RuneVec::try_with_capacity(baseline.baseline.buffers.len()) {
		Ok(values) => values,
		Err(error) => return VmResult::err(error),
	};
	for entry in &baseline.baseline.buffers {
		if let Err(error) = values.try_push((
			RuneBuffer::from_id(entry.buffer),
			RuneVersion::from_state(entry.state),
		)) {
			return VmResult::err(error);
		}
	}
	VmResult::Ok(values)
}

fn baseline_views(
	baseline: &RuneAutocmdBaseline,
	editor: &mut EditorHost,
) -> VmResult<RuneVec<(RuneView, RuneBuffer)>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let mut values = match RuneVec::try_with_capacity(baseline.baseline.views.len()) {
		Ok(values) => values,
		Err(error) => return VmResult::err(error),
	};
	for entry in &baseline.baseline.views {
		if let Err(error) = values.try_push((
			RuneView::from_id(entry.view),
			RuneBuffer::from_id(entry.buffer),
		)) {
			return VmResult::err(error);
		}
	}
	VmResult::Ok(values)
}

fn baseline_focused(baseline: &RuneAutocmdBaseline) -> Option<RuneView> {
	baseline.baseline.focused.map(RuneView::from_id)
}

fn baseline_sources(
	baseline: &RuneAutocmdBaseline,
	editor: &mut EditorHost,
) -> VmResult<RuneVec<(RuneSource, RuneOwner)>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let mut values = match RuneVec::try_with_capacity(baseline.baseline.sources.len()) {
		Ok(values) => values,
		Err(error) => return VmResult::err(error),
	};
	for entry in &baseline.baseline.sources {
		if let Err(error) = values.try_push((RuneSource(entry.source), RuneOwner(entry.owner))) {
			return VmResult::err(error);
		}
	}
	VmResult::Ok(values)
}

fn clone_kind(value: &RuneEventKind) -> RuneEventKind {
	*value
}

fn clone_source(value: &RuneSource) -> RuneSource {
	*value
}

fn clone_owner(value: &RuneOwner) -> RuneOwner {
	*value
}

fn eq_kind(left: &RuneEventKind, right: &RuneEventKind) -> bool {
	left == right
}

fn eq_source(left: &RuneSource, right: &RuneSource) -> bool {
	left == right
}

fn eq_owner(left: &RuneOwner, right: &RuneOwner) -> bool {
	left == right
}

fn debug_kind(value: &RuneEventKind, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "{value:?}")
}

fn debug_source(value: &RuneSource, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "Source({})", value.0.get())
}

fn debug_owner(value: &RuneOwner, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "Owner({})", value.0.get())
}

pub(crate) fn install(module: &mut Module) -> Result<(), rune::ContextError> {
	module.ty::<RuneEventKind>()?;
	module.ty::<RuneSource>()?;
	module.ty::<RuneOwner>()?;
	module.ty::<RuneAutocmdEvent>()?;
	module.ty::<RuneAutocmdBaseline>()?;

	module.function("autocmd", register_autocmd).build()?;
	module
		.function("autocmd_for_buffer", register_autocmd_for_buffer)
		.build()?;

	module.associated_function("kind", event_kind)?;
	module.associated_function("buffer", event_buffer)?;
	module.associated_function("view", event_view)?;
	module.associated_function("acting_view", event_acting_view)?;
	module.associated_function("old_buffer", event_old_buffer)?;
	module.associated_function("new_buffer", event_new_buffer)?;
	module.associated_function("version", event_version)?;
	module.associated_function("old_version", event_old_version)?;
	module.associated_function("new_version", event_new_version)?;
	module.associated_function("delta_count", event_delta_count)?;
	module.associated_function("deleted_bytes", event_deleted_bytes)?;
	module.associated_function("inserted_bytes", event_inserted_bytes)?;
	module.associated_function("from", event_from)?;
	module.associated_function("to", event_to)?;
	module.associated_function("source", event_source)?;
	module.associated_function("owner", event_owner)?;

	module.associated_function("buffers", baseline_buffers)?;
	module.associated_function("views", baseline_views)?;
	module.associated_function("focused", baseline_focused)?;
	module.associated_function("sources", baseline_sources)?;

	module.associated_function(&Protocol::CLONE, clone_kind)?;
	module.associated_function(&Protocol::CLONE, clone_source)?;
	module.associated_function(&Protocol::CLONE, clone_owner)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_kind)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_source)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_owner)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_kind)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_source)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_owner)?;
	module.implement_trait::<RuneEventKind>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneSource>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneOwner>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneEventKind>(rune::item!(::std::cmp::PartialEq))?;
	module.implement_trait::<RuneSource>(rune::item!(::std::cmp::PartialEq))?;
	module.implement_trait::<RuneOwner>(rune::item!(::std::cmp::PartialEq))?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::{StateId, ViewId};

	#[test]
	fn accessors_cover_each_engine_event_without_fabricating_fields() {
		let opened = RuneAutocmdEvent::new(&EngineEvent::BufferOpened {
			buffer: BufferId::new(3),
			state: StateId::new(4),
		});
		assert_eq!(event_kind(&opened), RuneEventKind::BufferOpened);
		assert_eq!(
			event_buffer(&opened),
			Some(RuneBuffer::from_id(BufferId::new(3)))
		);
		assert_eq!(
			event_version(&opened),
			Some(RuneVersion::from_state(StateId::new(4)))
		);
		assert_eq!(event_view(&opened), None);

		let retargeted = RuneAutocmdEvent::new(&EngineEvent::ViewRetargeted {
			view: ViewId::new(5),
			old_buffer: BufferId::new(6),
			new_buffer: BufferId::new(7),
		});
		assert_eq!(event_buffer(&retargeted), None);
		assert_eq!(
			event_view(&retargeted),
			Some(RuneView::from_id(ViewId::new(5)))
		);
		assert_eq!(
			event_old_buffer(&retargeted),
			Some(RuneBuffer::from_id(BufferId::new(6)))
		);
		assert_eq!(
			event_new_buffer(&retargeted),
			Some(RuneBuffer::from_id(BufferId::new(7)))
		);
	}

	#[test]
	fn buffer_filter_matches_both_sides_of_a_retarget() {
		let event = EngineEvent::ViewRetargeted {
			view: ViewId::new(1),
			old_buffer: BufferId::new(2),
			new_buffer: BufferId::new(3),
		};
		assert!(event_matches_buffer(&event, BufferId::new(2)));
		assert!(event_matches_buffer(&event, BufferId::new(3)));
		assert!(!event_matches_buffer(&event, BufferId::new(4)));
	}

	#[test]
	fn typed_accessors_cover_edit_focus_source_and_replacement_payloads() {
		let edited = RuneAutocmdEvent::new(&EngineEvent::BufferEdited {
			buffer: BufferId::new(1),
			acting_view: ViewId::new(2),
			old_state: StateId::new(3),
			new_state: StateId::new(4),
			delta_count: 5,
			deleted_bytes: 6,
			inserted_bytes: 7,
		});
		assert_eq!(
			event_acting_view(&edited),
			Some(RuneView::from_id(ViewId::new(2)))
		);
		assert_eq!(
			event_old_version(&edited),
			Some(RuneVersion::from_state(StateId::new(3)))
		);
		assert_eq!(
			event_new_version(&edited),
			Some(RuneVersion::from_state(StateId::new(4)))
		);
		assert_eq!(event_delta_count(&edited), Some(5));
		assert_eq!(event_deleted_bytes(&edited), Some(6));
		assert_eq!(event_inserted_bytes(&edited), Some(7));

		let replaced = RuneAutocmdEvent::new(&EngineEvent::BufferReplaced {
			buffer: BufferId::new(8),
			old_state: StateId::new(9),
			new_state: StateId::new(10),
		});
		assert_eq!(
			event_old_version(&replaced),
			Some(RuneVersion::from_state(StateId::new(9)))
		);
		assert_eq!(
			event_new_version(&replaced),
			Some(RuneVersion::from_state(StateId::new(10)))
		);
		assert_eq!(event_delta_count(&replaced), None);

		let focused = RuneAutocmdEvent::new(&EngineEvent::FocusedTextViewChanged {
			from: None,
			to: Some(ViewId::new(11)),
		});
		assert_eq!(event_from(&focused), None);
		assert_eq!(event_to(&focused), Some(RuneView::from_id(ViewId::new(11))));

		let loaded = RuneAutocmdEvent::new(&EngineEvent::SourceLoaded {
			source: SourceId::new(12),
			owner: RegistryOwnerId::new(13),
		});
		assert_eq!(event_source(&loaded), Some(RuneSource(SourceId::new(12))));
		assert_eq!(
			event_owner(&loaded),
			Some(RuneOwner(RegistryOwnerId::new(13)))
		);
		assert_eq!(event_buffer(&loaded), None);

		let hydrated = RuneAutocmdEvent::new(&EngineEvent::HydrationCompleted {
			buffer: BufferId::new(14),
		});
		assert_eq!(
			event_buffer(&hydrated),
			Some(RuneBuffer::from_id(BufferId::new(14)))
		);
	}

	#[test]
	fn kind_input_and_filter_validation_are_structurally_bounded() {
		let all = [
			EventKind::BufferOpened,
			EventKind::BufferClosed,
			EventKind::ViewCreated,
			EventKind::ViewRetargeted,
			EventKind::ViewClosed,
			EventKind::FocusedTextViewChanged,
			EventKind::BufferEdited,
			EventKind::BufferReplaced,
			EventKind::SourceLoaded,
			EventKind::SourceUnloaded,
			EventKind::HydrationCompleted,
		];
		assert_eq!(all.len(), EventKind::COUNT);
		for kind in all {
			assert_eq!(EventKind::from(RuneEventKind::from(kind)), kind);
		}
		let mut exact = RuneVec::new();
		for _ in 0..EventKind::COUNT {
			exact.try_push(RuneEventKind::BufferEdited).unwrap();
		}
		let deduplicated = collect_kind_set(&exact).unwrap();
		assert!(deduplicated.contains(EventKind::BufferEdited));
		exact.try_push(RuneEventKind::BufferEdited).unwrap();
		assert!(collect_kind_set(&exact).is_err());
		assert!(collect_kind_set(&RuneVec::new()).is_err());
		assert!(
			validate_buffer_filter(
				Some(BufferId::new(1)),
				EventKindSet::one(EventKind::SourceLoaded),
			)
			.is_err()
		);
		assert!(
			validate_buffer_filter(
				Some(BufferId::new(1)),
				EventKindSet::one(EventKind::BufferEdited),
			)
			.is_ok()
		);
	}
}
