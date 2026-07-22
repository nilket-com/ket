use crate::core::{BufferId, RegistryOwnerId, SourceId, StateId, ViewId};
#[cfg(test)]
use crate::engine::request::InternalEvent;
use std::collections::VecDeque;

pub(crate) const MAX_PENDING_EVENTS: usize = 4_096;
pub(crate) const MAX_DRAIN_PASSES: usize = 4;

/// Stable, append-only classification for subscribable engine events.
///
/// The bit mapping is explicit rather than derived from [`EngineEvent`]'s
/// discriminant. `EventsLost` is a recovery barrier and deliberately has no
/// subscribable kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub(crate) enum EventKind {
	BufferOpened = 0,
	BufferClosed = 1,
	ViewCreated = 2,
	ViewRetargeted = 3,
	ViewClosed = 4,
	FocusedTextViewChanged = 5,
	BufferEdited = 6,
	BufferReplaced = 7,
	SourceLoaded = 8,
	SourceUnloaded = 9,
	HydrationCompleted = 10,
}

impl EventKind {
	pub(crate) const COUNT: usize = 11;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct EventKindSet(u64);

// Construction is consumed by the next binding layer; the engine-only cut
// currently constructs sets only in characterization tests.
#[allow(dead_code)]
impl EventKindSet {
	pub(crate) const EMPTY: Self = Self(0);

	pub(crate) const fn one(kind: EventKind) -> Self {
		Self(1_u64 << kind as u8)
	}

	pub(crate) fn from_kinds(kinds: impl IntoIterator<Item = EventKind>) -> Self {
		let mut set = Self::EMPTY;
		for kind in kinds {
			set.insert(kind);
		}
		set
	}

	pub(crate) const fn is_empty(self) -> bool {
		self.0 == 0
	}

	pub(crate) const fn contains(self, kind: EventKind) -> bool {
		self.0 & Self::one(kind).0 != 0
	}

	pub(crate) fn insert(&mut self, kind: EventKind) {
		self.0 |= Self::one(kind).0;
	}
}

/// An owned statement of committed editor state.
///
/// Events never carry live borrows, document topology, or content. Consumers
/// re-query through the bounded engine APIs after delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum EngineEvent {
	BufferOpened {
		buffer: BufferId,
		state: StateId,
	},
	BufferClosed {
		buffer: BufferId,
	},
	ViewCreated {
		view: ViewId,
		buffer: BufferId,
	},
	ViewRetargeted {
		view: ViewId,
		old_buffer: BufferId,
		new_buffer: BufferId,
	},
	ViewClosed {
		view: ViewId,
		buffer: BufferId,
	},
	FocusedTextViewChanged {
		from: Option<ViewId>,
		to: Option<ViewId>,
	},
	BufferEdited {
		buffer: BufferId,
		acting_view: ViewId,
		old_state: StateId,
		new_state: StateId,
		delta_count: u64,
		deleted_bytes: u64,
		inserted_bytes: u64,
	},
	BufferReplaced {
		buffer: BufferId,
		old_state: StateId,
		new_state: StateId,
	},
	SourceLoaded {
		source: SourceId,
		owner: RegistryOwnerId,
	},
	SourceUnloaded {
		source: SourceId,
		owner: RegistryOwnerId,
	},
	/// Queue overflow is a resynchronization barrier. While it is pending,
	/// later events are absorbed into this count instead of queued behind it.
	EventsLost {
		dropped: u64,
	},
	HydrationCompleted {
		buffer: BufferId,
	},
}

impl EngineEvent {
	pub(crate) const fn kind(&self) -> Option<EventKind> {
		Some(match self {
			Self::BufferOpened { .. } => EventKind::BufferOpened,
			Self::BufferClosed { .. } => EventKind::BufferClosed,
			Self::ViewCreated { .. } => EventKind::ViewCreated,
			Self::ViewRetargeted { .. } => EventKind::ViewRetargeted,
			Self::ViewClosed { .. } => EventKind::ViewClosed,
			Self::FocusedTextViewChanged { .. } => EventKind::FocusedTextViewChanged,
			Self::BufferEdited { .. } => EventKind::BufferEdited,
			Self::BufferReplaced { .. } => EventKind::BufferReplaced,
			Self::SourceLoaded { .. } => EventKind::SourceLoaded,
			Self::SourceUnloaded { .. } => EventKind::SourceUnloaded,
			Self::EventsLost { .. } => return None,
			Self::HydrationCompleted { .. } => EventKind::HydrationCompleted,
		})
	}

	fn coalesces_with(&self, other: &Self) -> bool {
		matches!(
			(self, other),
			(
				Self::HydrationCompleted { buffer: left },
				Self::HydrationCompleted { buffer: right }
			) if left == right
		)
	}
}

/// Bounded, engine-thread-local storage for deferred events.
pub(crate) struct EngineEventQueue {
	pending: VecDeque<EngineEvent>,
	limit: usize,
	#[cfg(test)]
	delivered: Option<Vec<EngineEvent>>,
}

impl Default for EngineEventQueue {
	fn default() -> Self {
		Self::with_limit(MAX_PENDING_EVENTS)
	}
}

impl EngineEventQueue {
	fn with_limit(limit: usize) -> Self {
		assert!(limit > 0, "event queue must retain its overflow marker");
		Self {
			pending: VecDeque::new(),
			limit,
			#[cfg(test)]
			delivered: None,
		}
	}

	pub(crate) fn push(&mut self, event: EngineEvent) {
		debug_assert!(
			!matches!(event, EngineEvent::EventsLost { .. }),
			"EventsLost is synthesized by the queue"
		);

		if matches!(self.pending.front(), Some(EngineEvent::EventsLost { .. })) {
			debug_assert_eq!(self.pending.len(), 1, "loss marker must be a barrier");
			let Some(EngineEvent::EventsLost { dropped }) = self.pending.front_mut() else {
				unreachable!("loss marker was just matched")
			};
			*dropped = dropped.saturating_add(1);
			return;
		}

		if self
			.pending
			.iter()
			.any(|pending| pending.coalesces_with(&event))
		{
			return;
		}

		if self.pending.len() < self.limit {
			self.pending.push_back(event);
			return;
		}

		let dropped = u64::try_from(self.pending.len())
			.unwrap_or(u64::MAX)
			.saturating_add(1);
		self.pending.clear();
		self.pending.push_back(EngineEvent::EventsLost { dropped });
	}

	pub(crate) fn take_pass(&mut self) -> VecDeque<EngineEvent> {
		std::mem::take(&mut self.pending)
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.pending.is_empty()
	}

	#[cfg(test)]
	pub(crate) fn record_delivered(&mut self, event: EngineEvent) {
		if let Some(delivered) = &mut self.delivered {
			delivered.push(event);
		}
	}

	#[cfg(test)]
	pub(crate) fn take_delivered(&mut self) -> Vec<EngineEvent> {
		self.delivered
			.as_mut()
			.map(std::mem::take)
			.unwrap_or_default()
	}

	#[cfg(test)]
	fn enable_test_tap(&mut self) {
		self.delivered = Some(Vec::new());
	}
}

#[cfg(test)]
fn drain_engine_events_with(
	queue: &mut EngineEventQueue,
	mut wake: impl FnMut(),
	mut dispatch: impl FnMut(&EngineEvent, &mut EngineEventQueue),
) {
	for _ in 0..MAX_DRAIN_PASSES {
		if queue.is_empty() {
			return;
		}
		let pass = queue.take_pass();
		for event in pass {
			#[cfg(test)]
			queue.record_delivered(event.clone());
			dispatch(&event, queue);
		}
	}
	if !queue.is_empty() {
		wake();
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn opened(id: u64) -> EngineEvent {
		EngineEvent::BufferOpened {
			buffer: BufferId::new(id),
			state: StateId::new(id),
		}
	}

	#[test]
	fn subscribable_kind_mapping_is_stable_and_excludes_the_loss_barrier() {
		assert_eq!(opened(1).kind(), Some(EventKind::BufferOpened));
		assert_eq!(
			EngineEvent::HydrationCompleted {
				buffer: BufferId::new(1),
			}
			.kind(),
			Some(EventKind::HydrationCompleted)
		);
		assert_eq!(EngineEvent::EventsLost { dropped: 1 }.kind(), None);
		let kinds =
			EventKindSet::from_kinds([EventKind::BufferOpened, EventKind::HydrationCompleted]);
		assert!(kinds.contains(EventKind::BufferOpened));
		assert!(kinds.contains(EventKind::HydrationCompleted));
		assert!(!kinds.contains(EventKind::BufferClosed));
	}

	#[test]
	fn events_remain_fifo_below_the_limit() {
		let mut queue = EngineEventQueue::with_limit(3);
		queue.push(opened(1));
		queue.push(opened(2));
		assert_eq!(queue.take_pass(), VecDeque::from([opened(1), opened(2)]));
	}

	#[test]
	fn equal_hydration_wakes_coalesce_but_distinct_buffers_do_not() {
		let mut queue = EngineEventQueue::with_limit(3);
		let first = EngineEvent::HydrationCompleted {
			buffer: BufferId::new(1),
		};
		let second = EngineEvent::HydrationCompleted {
			buffer: BufferId::new(2),
		};
		queue.push(first.clone());
		queue.push(first.clone());
		queue.push(second.clone());
		assert_eq!(queue.take_pass(), VecDeque::from([first, second]));
	}

	#[test]
	fn overflow_collapses_to_one_cumulative_resynchronization_barrier() {
		let mut queue = EngineEventQueue::with_limit(2);
		queue.push(opened(1));
		queue.push(opened(2));
		queue.push(opened(3));
		queue.push(opened(4));
		queue.push(EngineEvent::HydrationCompleted {
			buffer: BufferId::new(1),
		});

		assert_eq!(
			queue.take_pass(),
			VecDeque::from([EngineEvent::EventsLost { dropped: 5 }])
		);
	}

	#[test]
	fn a_fresh_queue_accepts_exact_events_after_the_barrier_is_captured() {
		let mut queue = EngineEventQueue::with_limit(1);
		queue.push(opened(1));
		queue.push(opened(2));
		assert!(matches!(
			queue.take_pass().front(),
			Some(EngineEvent::EventsLost { dropped: 2 })
		));

		queue.push(opened(3));
		assert_eq!(queue.take_pass(), VecDeque::from([opened(3)]));
	}

	#[test]
	fn subscriber_events_wait_for_the_next_pass_and_cascades_are_bounded() {
		let mut queue = EngineEventQueue::with_limit(32);
		queue.enable_test_tap();
		queue.push(opened(1));
		let mut wake_count = 0;
		drain_engine_events_with(
			&mut queue,
			|| wake_count += 1,
			|event, queue| {
				let EngineEvent::BufferOpened { buffer, .. } = event else {
					return;
				};
				queue.push(opened(buffer.get() + 1));
			},
		);

		assert_eq!(
			queue.take_delivered(),
			vec![opened(1), opened(2), opened(3), opened(4)]
		);
		assert_eq!(queue.take_pass(), VecDeque::from([opened(5)]));
		assert_eq!(wake_count, 1);
	}

	#[test]
	fn a_full_request_stream_already_guarantees_the_deferred_drain_wake() {
		let (_commands, _input, internal, requests) = crate::engine::request::channel();
		let mut filled = 0usize;
		loop {
			match internal.try_send(InternalEvent::Refresh) {
				Ok(()) => filled += 1,
				Err(std::sync::mpsc::TrySendError::Full(_)) => break,
				Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
					panic!("request receiver remains live")
				}
			}
		}
		assert!(filled > 0);

		let mut queue = EngineEventQueue::with_limit(32);
		queue.push(opened(1));
		let mut wake_was_full = false;
		drain_engine_events_with(
			&mut queue,
			|| {
				wake_was_full = matches!(
					internal.try_send(InternalEvent::DrainEvents),
					Err(std::sync::mpsc::TrySendError::Full(_))
				);
			},
			|event, queue| {
				let EngineEvent::BufferOpened { buffer, .. } = event else {
					return;
				};
				queue.push(opened(buffer.get() + 1));
			},
		);
		assert!(wake_was_full);
		assert!(!queue.is_empty(), "cascade remainder stays queued");
		assert!(
			requests.recv().is_ok(),
			"an existing request wakes the loop"
		);

		drain_engine_events_with(&mut queue, || {}, |_, _| {});
		assert!(queue.is_empty());
	}

	#[test]
	fn configured_queue_memory_is_small_and_bounded() {
		assert!(std::mem::size_of::<EngineEvent>() <= 64);
		assert!(
			std::mem::size_of::<EngineEvent>() * MAX_PENDING_EVENTS <= 256 * 1024,
			"the calibrated queue stays within 256 KiB of inline event storage"
		);
	}
}
