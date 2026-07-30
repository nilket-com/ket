use crate::ecs::NodeId;
use crate::ecs::registry::{ReclaimFence, RetiredForestNotice, UastRegistry};
use std::collections::VecDeque;

pub(crate) const RECLAIM_NODES_PER_FRAME: usize = 4_096;

#[derive(Debug)]
struct RetiredTree {
	head: NodeId,
}

#[derive(Debug)]
struct ReclaimCursor {
	stack: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReclaimProgress {
	pub(crate) visited_nodes: usize,
	pub(crate) authoritative_bytes: u64,
	pub(crate) hydrated_bytes: u64,
	pub(crate) work_remaining: bool,
}

#[derive(Debug, Default)]
pub(crate) struct ReclaimQueue {
	quarantined: VecDeque<RetiredTree>,
	eligible: VecDeque<RetiredTree>,
	in_progress: Option<ReclaimCursor>,
	maintenance_wake_pending: bool,
}

impl ReclaimQueue {
	pub(crate) fn collect_registry_notices(&mut self, registry: &UastRegistry) {
		for RetiredForestNotice { heads, fence } in registry.drain_retired_forest_notices() {
			let target = match fence {
				ReclaimFence::AfterPublication => &mut self.quarantined,
				ReclaimFence::NeverPublished => &mut self.eligible,
			};
			target.extend(heads.into_iter().map(|head| RetiredTree { head }));
		}
	}

	pub(crate) fn drain_eligible(
		&mut self,
		registry: &UastRegistry,
		budget: usize,
	) -> ReclaimProgress {
		let mut progress = ReclaimProgress::default();
		while progress.visited_nodes < budget {
			if self.in_progress.is_none() {
				let Some(tree) = self.eligible.pop_front() else {
					break;
				};
				self.in_progress = Some(ReclaimCursor {
					stack: vec![tree.head],
				});
			}

			let cursor = self.in_progress.as_mut().expect("cursor just installed");
			let Some(node) = cursor.stack.pop() else {
				self.in_progress = None;
				continue;
			};

			let mut children = Vec::new();
			let mut child = registry.get_first_child(node);
			while let Some(current) = child {
				children.push(current);
				child = registry.get_next_sibling(current);
			}
			cursor.stack.extend(children.into_iter().rev());

			let reclaimed = registry.reclaim_orphaned_payload(node);
			progress.visited_nodes += 1;
			progress.authoritative_bytes = progress
				.authoritative_bytes
				.checked_add(reclaimed.authoritative_bytes)
				.expect("per-frame authoritative reclaim overflow");
			progress.hydrated_bytes = progress
				.hydrated_bytes
				.checked_add(reclaimed.hydrated_bytes)
				.expect("per-frame hydrated reclaim overflow");

			if cursor.stack.is_empty() {
				self.in_progress = None;
			}
		}
		progress.work_remaining = self.has_eligible_work();
		progress
	}

	/// Advance the publication fence after the fully projected viewport has
	/// been sent. Newly retired trees cannot be reclaimed before this point.
	pub(crate) fn finish_frame(&mut self) {
		self.eligible.append(&mut self.quarantined);
	}

	pub(crate) fn has_eligible_work(&self) -> bool {
		self.in_progress.is_some() || !self.eligible.is_empty()
	}

	pub(crate) fn begin_maintenance_turn(&mut self) {
		self.maintenance_wake_pending = false;
	}

	pub(crate) fn schedule_maintenance(&mut self, internal: &super::request::InternalSender) {
		if !self.has_eligible_work() || self.maintenance_wake_pending {
			return;
		}
		self.maintenance_wake_pending = true;
		if internal
			.try_send(super::request::InternalEvent::Reclaim)
			.is_err()
		{
			self.maintenance_wake_pending = false;
		}
	}

	pub(crate) fn pending_heads(&self) -> usize {
		self.quarantined.len() + self.eligible.len() + usize::from(self.in_progress.is_some())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ecs::UastRegistry;
	use crate::uast::{SemanticKind, SpanMetrics};
	use std::sync::Arc;

	#[test]
	fn a_forest_head_does_not_walk_its_detached_root_sibling() {
		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(4).expect("registry chunk");
		let first = chunk.spawn_node(SemanticKind::Group, None, SpanMetrics::default());
		let child = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		let sibling = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		let sibling_child = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		chunk.append_local_child(first, child);
		unsafe {
			(*registry.edges[first.index()].get()).next_sibling = Some(sibling);
		}
		chunk.append_local_child(sibling, sibling_child);
		registry.replace_authoritative_data(child, b"a".to_vec());
		registry.replace_authoritative_data(sibling_child, b"b".to_vec());
		registry.retire_orphaned_forest([first], 2, 1, ReclaimFence::NeverPublished);

		let mut queue = ReclaimQueue::default();
		queue.collect_registry_notices(&registry);
		let progress = queue.drain_eligible(&registry, usize::MAX);

		assert_eq!(progress.visited_nodes, 2);
		assert_eq!(progress.authoritative_bytes, 1);
		assert_eq!(registry.authoritative_payload_len(sibling_child), 1);
	}

	#[test]
	fn a_large_tree_waits_for_publication_and_drains_within_budget() {
		const LEAVES: usize = RECLAIM_NODES_PER_FRAME + 17;
		let registry = UastRegistry::new((LEAVES + 1) as u32);
		let mut chunk = registry
			.reserve_chunk((LEAVES + 1) as u32)
			.expect("registry chunk");
		let root = chunk.spawn_node(SemanticKind::Group, None, SpanMetrics::default());
		for _ in 0..LEAVES {
			let leaf = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
			chunk.append_local_child(root, leaf);
			registry.replace_authoritative_data(leaf, vec![b'x']);
		}
		registry.retire_orphaned_forest(
			[root],
			(LEAVES + 1) as u64,
			LEAVES as u64,
			ReclaimFence::AfterPublication,
		);

		let mut queue = ReclaimQueue::default();
		queue.collect_registry_notices(&registry);
		assert_eq!(
			queue.drain_eligible(&registry, RECLAIM_NODES_PER_FRAME),
			ReclaimProgress::default(),
			"same-frame quarantine must not be eligible"
		);
		assert_eq!(
			registry.stats().reusable_slots,
			0,
			"publication-quarantined nodes cannot feed the allocator"
		);
		assert_eq!(registry.stats().orphaned_payload_bytes, LEAVES as u64);

		queue.finish_frame();
		let first = queue.drain_eligible(&registry, RECLAIM_NODES_PER_FRAME);
		assert_eq!(first.visited_nodes, RECLAIM_NODES_PER_FRAME);
		assert!(first.work_remaining);
		assert_eq!(
			registry.stats().authoritative_payload_bytes,
			registry.stats().orphaned_payload_bytes,
			"partial drain must preserve retained = orphaned without owners"
		);

		let second = queue.drain_eligible(&registry, RECLAIM_NODES_PER_FRAME);
		assert_eq!(second.visited_nodes, 18);
		assert!(!second.work_remaining);
		assert_eq!(registry.stats().authoritative_payload_bytes, 0);
		assert_eq!(registry.stats().orphaned_payload_bytes, 0);
		assert_eq!(
			registry.stats().orphaned_payload_bytes_reclaimed,
			LEAVES as u64
		);
		assert_eq!(
			registry.stats().reusable_slots,
			(LEAVES + 1) as u64,
			"every visited, quiescent node becomes allocator-eligible exactly once"
		);
	}

	#[test]
	#[should_panic(expected = "retired forest head must be detached")]
	fn an_attached_subtree_cannot_enter_reclamation() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(SemanticKind::Group, None, SpanMetrics::default());
		let child = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		chunk.append_local_child(root, child);
		registry.retire_orphaned_forest([child], 1, 0, ReclaimFence::AfterPublication);
	}

	#[test]
	fn maintenance_wakes_coalesce_until_the_queued_turn_begins() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let root = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		registry.retire_orphaned_forest([root], 1, 0, ReclaimFence::NeverPublished);
		let mut queue = ReclaimQueue::default();
		queue.collect_registry_notices(&registry);
		let (_commands, _input, internal, requests) = crate::engine::request::channel();

		queue.schedule_maintenance(&internal);
		queue.schedule_maintenance(&internal);
		assert!(matches!(
			requests.recv(),
			Ok(crate::engine::request::EngineRequest::Internal(
				crate::engine::request::InternalEvent::Reclaim
			))
		));
		assert!(
			requests.try_recv().is_err(),
			"one wake is enough for one queue"
		);

		queue.begin_maintenance_turn();
		queue.schedule_maintenance(&internal);
		assert!(matches!(
			requests.recv(),
			Ok(crate::engine::request::EngineRequest::Internal(
				crate::engine::request::InternalEvent::Reclaim
			))
		));
	}

	#[test]
	fn reclaimed_slot_waits_for_its_last_io_lease_then_reuses_with_new_identity() {
		let registry = Arc::new(UastRegistry::new(1));
		let mut reservation = registry.reserve_chunk(1).expect("initial slot");
		let old = reservation.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 7,
				newlines: 3,
			},
		);
		drop(reservation);
		let lease = registry.acquire_io_lifetime(old).expect("live node lease");
		registry.retire_orphaned_forest([old], 1, 0, ReclaimFence::NeverPublished);
		let mut queue = ReclaimQueue::default();
		queue.collect_registry_notices(&registry);
		assert_eq!(queue.drain_eligible(&registry, 1).visited_nodes, 1);
		assert_eq!(registry.stats().pending_reuse_slots, 1);
		assert_eq!(registry.stats().reusable_slots, 0);
		assert!(registry.reserve_chunk(1).is_none());

		drop(lease);
		assert_eq!(registry.stats().pending_reuse_slots, 0);
		assert_eq!(registry.stats().reusable_slots, 1);
		let mut recycled = registry.reserve_chunk(1).expect("quiescent slot");
		let new = recycled.spawn_node(
			SemanticKind::Group,
			None,
			SpanMetrics {
				byte_length: 11,
				newlines: 5,
			},
		);
		assert_eq!(new.index(), old.index());
		assert_ne!(new, old);
		assert_eq!(new.generation(), old.generation() + 1);
		assert!(!registry.is_current_node(old));
		assert_eq!(registry.get_total_bytes(old), 0);
		assert_eq!(registry.get_first_child(old), None);
		assert_eq!(registry.get_total_bytes(new), 11);
		registry.replace_authoritative_data(new, b"new lifetime".to_vec());
		let stale_write = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			registry.replace_authoritative_data(old, b"stale overwrite".to_vec());
		}));
		assert!(
			stale_write.is_err(),
			"a stale identity must not mutate its replacement"
		);
		assert_eq!(
			registry.read_node_bytes_sync(new).expect("new payload"),
			b"new lifetime"
		);
	}

	#[test]
	fn fragmented_reuse_and_tail_slots_form_one_exact_atomic_reservation() {
		let registry = UastRegistry::new(4);
		let mut initial = registry.reserve_chunk(2).expect("initial slots");
		let first = initial.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		let _second = initial.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		drop(initial);
		registry.retire_orphaned_forest([first], 1, 0, ReclaimFence::NeverPublished);
		let mut queue = ReclaimQueue::default();
		queue.collect_registry_notices(&registry);
		queue.drain_eligible(&registry, 1);

		let before = registry.stats();
		let mut mixed = registry
			.reserve_chunk(3)
			.expect("one free plus two tail slots");
		let parent = mixed.spawn_node(SemanticKind::Group, None, SpanMetrics::default());
		let left = mixed.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		let right = mixed.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		mixed.append_local_child(parent, left);
		mixed.append_local_child(parent, right);
		assert_eq!(parent.index(), first.index());
		assert_ne!(parent.generation(), first.generation());
		assert_eq!(registry.allocated_node_count(), 4);
		assert_eq!(
			registry.allocation_event_count(),
			before.allocation_events + 3
		);
		assert_eq!(registry.get_first_child(parent), Some(left));
		assert_eq!(registry.get_next_sibling(left), Some(right));
	}

	#[test]
	fn insufficient_mixed_reservation_changes_neither_free_list_nor_tail() {
		let registry = UastRegistry::new(3);
		let mut initial = registry.reserve_chunk(2).expect("initial slots");
		let retired = initial.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		let _live = initial.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		drop(initial);
		registry.retire_orphaned_forest([retired], 1, 0, ReclaimFence::NeverPublished);
		let mut queue = ReclaimQueue::default();
		queue.collect_registry_notices(&registry);
		queue.drain_eligible(&registry, 1);
		let before = registry.stats();

		assert!(registry.reserve_chunk(3).is_none());
		assert_eq!(registry.stats(), before);
		assert!(
			registry.reserve_chunk(2).is_some(),
			"the failed request must leave one free and one tail slot intact"
		);
	}

	#[test]
	fn generation_255_retires_to_burned_instead_of_wrapping() {
		let registry = UastRegistry::new(1);
		let mut reservation = registry.reserve_chunk(1).expect("initial slot");
		let mut node = reservation.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		drop(reservation);
		let mut queue = ReclaimQueue::default();
		for expected in 0..=u8::MAX {
			assert_eq!(node.generation(), expected);
			registry.retire_orphaned_forest([node], 1, 0, ReclaimFence::NeverPublished);
			queue.collect_registry_notices(&registry);
			queue.drain_eligible(&registry, 1);
			if expected == u8::MAX {
				break;
			}
			let mut next = registry.reserve_chunk(1).expect("generation remains");
			node = next.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		}
		let stats = registry.stats();
		assert_eq!(stats.burned_slots, 1);
		assert_eq!(stats.reusable_slots, 0);
		assert!(registry.reserve_chunk(1).is_none());
		assert_eq!(stats.allocation_events, 256);
		assert_eq!(
			stats.allocated_nodes, 1,
			"reuse churn must hold high-water flat"
		);
	}
}
