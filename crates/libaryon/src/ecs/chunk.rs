use crate::ecs::id::NodeId;
use crate::ecs::registry::UastRegistry;
use crate::svp::pointer::SvpPointer;
use crate::uast::ProviderKindId;
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::num::NonZeroU32;

/// ==========================================
/// THREAD-LOCAL CHUNK (THE WRITE CAPABILITY)
/// ==========================================
/// Represents exclusive ownership over a slice of the ECS component arrays.
pub struct RegistryChunk<'a> {
	registry: &'a UastRegistry,
	start_id: u32,
	len: u32,
	offset: u32,
}

impl<'a> RegistryChunk<'a> {
	pub(crate) fn new(registry: &'a UastRegistry, start_id: u32, len: u32) -> Self {
		Self {
			registry,
			start_id,
			len,
			offset: 0,
		}
	}

	/// $O(1)$ lock-free entity allocation within the thread's reserved chunk.
	pub fn spawn_node(
		&mut self,
		kind: SemanticKind,
		span: Option<SvpPointer>,
		metric: SpanMetrics,
	) -> NodeId {
		assert!(self.offset < self.len, "Chunk capacity exceeded");

		let id_val = self.start_id + self.offset;
		let id = NodeId(NonZeroU32::new(id_val).unwrap());
		let idx = id.index();
		self.offset += 1;

		// SAFETY: The chunk exclusively owns indices from `start_id` to `start_id + len`.
		// No other thread can access these UnsafeCells.
		unsafe {
			*self.registry.kinds[idx].get() = kind;
			*self.registry.provider_kinds[idx].get() = ProviderKindId::NONE;
			*self.registry.spans[idx].get() = span;
			*self.registry.metrics[idx].get() = metric;
			// edges and child_tails are already zeroed out by default
		}
		id
	}

	/// Appends a child in $O(1)$.
	/// Panics if the thread attempts to link nodes it does not own.
	#[allow(dead_code)]
	pub fn append_local_child(&mut self, parent: NodeId, child: NodeId) {
		let p_val = parent.0.get();
		let c_val = child.0.get();

		// SECURITY BOUNDS CHECK: Ensure we only mutate nodes allocated by THIS chunk.
		assert!(
			p_val >= self.start_id && p_val < self.start_id + self.len,
			"Parent out of chunk bounds"
		);
		assert!(
			c_val >= self.start_id && c_val < self.start_id + self.len,
			"Child out of chunk bounds"
		);

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
