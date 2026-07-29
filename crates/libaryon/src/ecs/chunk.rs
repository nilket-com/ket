use crate::ecs::id::NodeId;
use crate::ecs::registry::UastRegistry;
use crate::svp::pointer::SvpPointer;
use crate::uast::ProviderKindId;
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReservedSlot {
	pub(crate) id: NodeId,
	/// True when this ticket follows a previously published node lifetime.
	pub(crate) reused: bool,
}

/// ==========================================
/// THREAD-LOCAL CHUNK (THE WRITE CAPABILITY)
/// ==========================================
/// Represents exclusive ownership over an exact ordered set of ECS slots.
pub struct RegistryChunk<'a> {
	registry: &'a UastRegistry,
	slots: Vec<ReservedSlot>,
	owned: HashSet<NodeId>,
	offset: usize,
}

impl<'a> RegistryChunk<'a> {
	pub(crate) fn new(registry: &'a UastRegistry, slots: Vec<ReservedSlot>) -> Self {
		let owned = slots.iter().map(|slot| slot.id).collect();
		Self {
			registry,
			slots,
			owned,
			offset: 0,
		}
	}

	pub(crate) fn remaining(&self) -> u32 {
		u32::try_from(self.slots.len() - self.offset).expect("reservation length fits u32")
	}

	/// $O(1)$ lock-free entity allocation within the thread's reserved chunk.
	pub fn spawn_node(
		&mut self,
		kind: SemanticKind,
		span: Option<SvpPointer>,
		metric: SpanMetrics,
	) -> NodeId {
		let ticket = *self
			.slots
			.get(self.offset)
			.expect("Registry reservation capacity exceeded");
		self.offset += 1;
		self.registry
			.initialize_reserved_node(ticket, kind, ProviderKindId::NONE, span, metric)
	}

	/// Appends a child in $O(1)$.
	/// Panics if the thread attempts to link nodes it does not own.
	#[allow(dead_code)]
	pub fn append_local_child(&mut self, parent: NodeId, child: NodeId) {
		// SECURITY BOUNDS CHECK: Ensure we only mutate nodes allocated by THIS chunk.
		assert!(self.owned.contains(&parent), "Parent out of chunk bounds");
		assert!(self.owned.contains(&child), "Child out of chunk bounds");

		let p_idx = parent.index();
		let c_idx = child.index();

		// SAFETY: Bounds check above guarantees exclusive chunk ownership.
		unsafe {
			(*self.registry.edges[c_idx].get()).parent = Some(parent);

			let tail_ptr = self.registry.child_tails[p_idx].get();
			if let Some(tail) = *tail_ptr {
				(*self.registry.edges[tail.index()].get()).next_sibling = Some(child);
			} else {
				(*self.registry.edges[p_idx].get()).first_child = Some(child);
			}
			*tail_ptr = Some(child);
		}
	}
}

impl Drop for RegistryChunk<'_> {
	fn drop(&mut self) {
		if self.offset < self.slots.len() {
			self.registry
				.return_unpublished_slots(&self.slots[self.offset..]);
		}
	}
}
