use crate::ecs::chunk::{RegistryChunk, ReservedSlot};
use crate::ecs::id::{NODE_MAX_GENERATION, NODE_SLOT_MASK, NodeId};
use crate::ecs::paged::PagedArray;
use crate::svp::backing_store::{BackingIdentity, BackingStore, FileBackingStore};
use crate::svp::pointer::SvpPointer;
use crate::uast::kind::{ProviderKindId, SemanticKind};
use crate::uast::metrics::SpanMetrics;
use crate::uast::projection::RootChildLineIndex;
use crate::uast::topology::TreeEdges;
use memchr::memchr_iter;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegistryStatsSnapshot {
	/// Highest bump-tail slot ever committed. This does not count reuse.
	pub allocated_nodes: u64,
	/// Published node lifetimes, including recycled slots.
	pub allocation_events: u64,
	/// Fixed node-slot capacity. Retired slots can be reused with a new identity.
	pub capacity: u64,
	pub reusable_slots: u64,
	pub pending_reuse_slots: u64,
	pub burned_slots: u64,
	/// Cumulative node-lifetime retirements. This is traffic, not a live gauge.
	pub retired_nodes: u64,
	pub cold_extents: u64,
	pub hot_extents: u64,
	pub scanned_bytes: u64,
	pub pending_scan_blocks: u64,
	pub resident_bytes: u64,
	/// Authoritative bytes ever added minus bytes physically removed or
	/// reclassified as reconstructible.
	pub authoritative_payload_bytes: u64,
	/// Authoritative bytes detached from their final owner but not yet freed.
	pub orphaned_payload_bytes: u64,
	/// Authoritative orphan bytes physically reclaimed since registry creation.
	pub orphaned_payload_bytes_reclaimed: u64,
	pub sum_tree_rebuilds: u64,
	/// UAST nodes examined while viewport queries seek their first visible leaf.
	pub viewport_descent_node_visits: u64,
	pub high_priority_io_count: u64,
	pub high_priority_io_total_micros: u64,
	pub high_priority_io_max_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimFence {
	AfterPublication,
	NeverPublished,
}

#[derive(Debug)]
pub(crate) struct RetiredForestNotice {
	pub(crate) heads: Vec<NodeId>,
	pub(crate) fence: ReclaimFence,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReclaimedPayload {
	pub(crate) authoritative_bytes: u64,
	pub(crate) hydrated_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AuthoritativePayloadSnapshot {
	pub(crate) added: u64,
	pub(crate) removed: u64,
	pub(crate) orphaned_added: u64,
	pub(crate) orphaned_removed: u64,
}

impl AuthoritativePayloadSnapshot {
	pub(crate) fn retained(self) -> u64 {
		self.added
			.checked_sub(self.removed)
			.expect("authoritative payload removal exceeds additions")
	}

	pub(crate) fn orphaned(self) -> u64 {
		self.orphaned_added
			.checked_sub(self.orphaned_removed)
			.expect("orphaned payload removal exceeds additions")
	}
}

#[derive(Debug, Clone)]
pub(crate) struct InspectorNodeMetadata {
	pub has_physical_span: bool,
	pub has_full_residency: bool,
	pub has_partial_residency: bool,
	pub resident_bytes: u64,
	pub snippet: Option<Vec<u8>>,
	pub metric_inflated: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ResidentRange {
	pub offset: u32,
	pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MetricScanClaim {
	pub(crate) node_id: NodeId,
	pub(crate) pointer: SvpPointer,
	pub(crate) content_generation: u64,
}

#[derive(Debug)]
struct MetricScanState {
	unclaimed: VecDeque<MetricScanClaim>,
	pending: HashSet<NodeId>,
	in_flight: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct ResidentLruLinks {
	prev: Option<NodeId>,
	next: Option<NodeId>,
	linked: bool,
}

#[derive(Debug, Default)]
struct ResidentLru {
	head: Option<NodeId>,
	tail: Option<NodeId>,
	len: usize,
}

#[derive(Debug, Clone, Copy)]
struct PendingMetricInflation {
	node_id: NodeId,
	newline_count: u32,
	ends_with_newline: bool,
	scan_root: Option<NodeId>,
	content_generation: u64,
}

const IO_LIFETIME_RETIRED: u64 = 1 << 63;
const IO_LIFETIME_COUNT_MASK: u64 = IO_LIFETIME_RETIRED - 1;

#[derive(Debug, Default)]
struct SlotAllocator {
	available: Vec<ReservedSlot>,
	pending_reuse: u64,
	burned: u64,
}

/// Keeps one node lifetime unavailable for reuse until a resolver request has
/// performed its final slot and side-table write.
pub(crate) struct IoLifetimeLease {
	registry: Arc<UastRegistry>,
	node: NodeId,
}

impl Drop for IoLifetimeLease {
	fn drop(&mut self) {
		self.registry.release_io_lifetime(self.node);
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WholeLeafReadOutcome {
	InFlight,
	Failed,
}

/// ==========================================
/// THE CONCURRENT ECS REGISTRY (WORLD)
/// ==========================================
/// Process-wide Structure of Arrays (SoA) store for a forest of document
/// trees. `Registry` names its ECS role; each document root and its descendants
/// form one tree, while multiple roots coexist in the same registry.
pub struct UastRegistry {
	capacity: u32,
	next_id: AtomicU32,
	allocator: Mutex<SlotAllocator>,
	allocation_events: AtomicU64,
	topology_revision: AtomicU64,
	object_generation: PagedArray<AtomicU8>,
	slot_published: PagedArray<AtomicBool>,
	io_lifetime: PagedArray<AtomicU64>,

	// Boxed slices of UnsafeCells.
	// This allows concurrent mutable writes to disjoint indices without locks.
	pub kinds: PagedArray<UnsafeCell<SemanticKind>>,
	pub provider_kinds: PagedArray<UnsafeCell<ProviderKindId>>,
	pub spans: PagedArray<UnsafeCell<Option<SvpPointer>>>,
	pub metrics: PagedArray<UnsafeCell<SpanMetrics>>,
	pub edges: PagedArray<UnsafeCell<TreeEdges>>,

	// MUTATION STATE: Tracks the last child of a node.
	// Separated from `edges` to keep `edges` densely packed for read-only traversal.
	// Cost: Exactly 4 bytes per node via Option<NonZeroU32> optimization.
	#[allow(dead_code)]
	pub(crate) child_tails: PagedArray<UnsafeCell<Option<NodeId>>>,

	// VIRTUAL DATA: Stores uncommitted text for virtual nodes.
	// In a production system, this would be a more sophisticated buffer pool.
	pub virtual_data: PagedArray<UnsafeCell<Option<Vec<u8>>>>,
	partial_residency: PagedArray<UnsafeCell<Vec<ResidentRange>>>,
	resident_data_lock: RwLock<()>,

	/// Tracks nodes currently being resolved by the I/O thread.
	pub dma_in_flight: PagedArray<AtomicBool>,

	/// Tracks newline inflation in progress to avoid double-counting.
	pub metric_inflating: PagedArray<AtomicBool>,

	/// Ensures physical nodes are only inflated once.
	pub metrics_inflated: PagedArray<AtomicBool>,
	content_generation: PagedArray<AtomicU64>,
	/// Set permanently for a slot when a content-generation increment can no
	/// longer produce a unique value. The slot remains readable until logical
	/// retirement, but rejects all resolver admission/publication and burns
	/// instead of re-entering the allocator.
	content_generation_exhausted: PagedArray<AtomicBool>,

	/// 0 = unknown, 1 = known not to end in `\n`, 2 = known to end in `\n`.
	/// This is separate from the newline count because line ownership at an
	/// extent boundary cannot be inferred from the count alone.
	terminal_newline_state: PagedArray<AtomicU8>,

	/// Roots whose exact newline metric is still being populated by the
	/// background scan. The value is the number of physical leaves that have
	/// not yet contributed their newline count.
	metric_scans: Mutex<HashMap<NodeId, MetricScanState>>,
	incomplete_metric_roots: Mutex<HashSet<NodeId>>,
	pending_metric_inflations: Mutex<VecDeque<PendingMetricInflation>>,
	whole_leaf_read_outcomes: Mutex<HashMap<(NodeId, u64), WholeLeafReadOutcome>>,

	/// Production prefix indexes for flat source-backed document roots.
	pub(crate) root_child_line_indexes: RwLock<HashMap<NodeId, RootChildLineIndex>>,
	pub(crate) root_child_line_index_full_builds: AtomicU64,
	pub(crate) root_child_line_index_splices: AtomicU64,
	pub(crate) root_child_line_index_revision: AtomicU64,
	pub(crate) cold_extent_count: AtomicU64,
	pub(crate) hot_extent_count: AtomicU64,
	pub(crate) scanned_bytes: AtomicU64,
	pub(crate) pending_scan_blocks: AtomicU64,
	pub(crate) resident_bytes: AtomicU64,
	retired_nodes: AtomicU64,
	authoritative_bytes_added: AtomicU64,
	authoritative_bytes_removed: AtomicU64,
	orphaned_payload_bytes_added: AtomicU64,
	orphaned_payload_bytes_removed: AtomicU64,
	retired_forest_outbox: Mutex<Vec<RetiredForestNotice>>,
	/// Monotonic invalidation token for metadata-only inspector snapshots.
	/// Residency, metric knowledge, allocation, and topology changes bump it.
	inspector_revision: AtomicU64,
	resident_accounted_bytes: PagedArray<AtomicU64>,
	resident_counted_hot: PagedArray<AtomicBool>,
	resident_budget_high: AtomicU64,
	resident_budget_low: AtomicU64,
	resident_lru: Mutex<ResidentLru>,
	resident_lru_links: PagedArray<UnsafeCell<ResidentLruLinks>>,
	viewport_resident_pins: Mutex<HashMap<u32, HashSet<NodeId>>>,
	pub(crate) sum_tree_rebuilds: AtomicU64,
	pub(crate) viewport_descent_node_visits: AtomicU64,
	high_priority_started: Mutex<HashMap<NodeId, Instant>>,
	high_priority_io_count: AtomicU64,
	high_priority_io_total_micros: AtomicU64,
	high_priority_io_max_micros: AtomicU64,

	pub(crate) device_paths: Mutex<HashMap<u16, PathBuf>>,
	backing_stores: Mutex<HashMap<u16, std::sync::Arc<dyn BackingStore>>>,
}

// SAFETY: reserved chunks give their owner exclusive access to newly allocated
// indices. After publication, topology, spans, and metrics are mutated only by
// the engine thread; resolver-produced metric values are queued and drained by
// that thread. Resident byte cells are protected by `resident_data_lock`, and
// all other cross-thread state uses atomics or standard synchronization types.
unsafe impl Sync for UastRegistry {}
unsafe impl Send for UastRegistry {}

impl UastRegistry {
	fn allocator_state(&self) -> std::sync::MutexGuard<'_, SlotAllocator> {
		self.allocator
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
	}

	/// Pre-allocates the registry. 10GB dataset -> ~200M capacity.
	pub fn new(capacity: u32) -> Self {
		assert!(
			capacity <= NODE_SLOT_MASK,
			"UAST registry capacity exceeds the 24-bit NodeId slot field"
		);
		let cap = capacity as usize;
		Self {
			capacity,
			next_id: AtomicU32::new(1), // Start at 1 to allow NonZeroU32 for NodeId
			allocator: Mutex::new(SlotAllocator::default()),
			allocation_events: AtomicU64::new(0),
			topology_revision: AtomicU64::new(0),
			object_generation: PagedArray::new(cap, || AtomicU8::new(0)),
			slot_published: PagedArray::new(cap, || AtomicBool::new(false)),
			io_lifetime: PagedArray::new(cap, || AtomicU64::new(0)),
			kinds: PagedArray::new(cap, || UnsafeCell::new(SemanticKind::None)),
			provider_kinds: PagedArray::new(cap, || UnsafeCell::new(ProviderKindId::NONE)),
			spans: PagedArray::new(cap, || UnsafeCell::new(None)),
			metrics: PagedArray::new(cap, || UnsafeCell::new(SpanMetrics::default())),
			edges: PagedArray::new(cap, || UnsafeCell::new(TreeEdges::default())),
			child_tails: PagedArray::new(cap, || UnsafeCell::new(None)),
			virtual_data: PagedArray::new(cap, || UnsafeCell::new(None)),
			partial_residency: PagedArray::new(cap, || UnsafeCell::new(Vec::new())),
			resident_data_lock: RwLock::new(()),
			dma_in_flight: PagedArray::new(cap, || AtomicBool::new(false)),
			metric_inflating: PagedArray::new(cap, || AtomicBool::new(false)),
			metrics_inflated: PagedArray::new(cap, || AtomicBool::new(false)),
			content_generation: PagedArray::new(cap, || AtomicU64::new(0)),
			content_generation_exhausted: PagedArray::new(cap, || AtomicBool::new(false)),
			terminal_newline_state: PagedArray::new(cap, || AtomicU8::new(0)),
			metric_scans: Mutex::new(HashMap::new()),
			incomplete_metric_roots: Mutex::new(HashSet::new()),
			pending_metric_inflations: Mutex::new(VecDeque::new()),
			whole_leaf_read_outcomes: Mutex::new(HashMap::new()),
			root_child_line_indexes: RwLock::new(HashMap::new()),
			root_child_line_index_full_builds: AtomicU64::new(0),
			root_child_line_index_splices: AtomicU64::new(0),
			root_child_line_index_revision: AtomicU64::new(0),
			cold_extent_count: AtomicU64::new(0),
			hot_extent_count: AtomicU64::new(0),
			scanned_bytes: AtomicU64::new(0),
			pending_scan_blocks: AtomicU64::new(0),
			resident_bytes: AtomicU64::new(0),
			retired_nodes: AtomicU64::new(0),
			authoritative_bytes_added: AtomicU64::new(0),
			authoritative_bytes_removed: AtomicU64::new(0),
			orphaned_payload_bytes_added: AtomicU64::new(0),
			orphaned_payload_bytes_removed: AtomicU64::new(0),
			retired_forest_outbox: Mutex::new(Vec::new()),
			inspector_revision: AtomicU64::new(0),
			resident_accounted_bytes: PagedArray::new(cap, || AtomicU64::new(0)),
			resident_counted_hot: PagedArray::new(cap, || AtomicBool::new(false)),
			resident_budget_high: AtomicU64::new(64 * 1024 * 1024),
			resident_budget_low: AtomicU64::new(48 * 1024 * 1024),
			resident_lru: Mutex::new(ResidentLru::default()),
			resident_lru_links: PagedArray::new(cap, || {
				UnsafeCell::new(ResidentLruLinks::default())
			}),
			viewport_resident_pins: Mutex::new(HashMap::new()),
			sum_tree_rebuilds: AtomicU64::new(0),
			viewport_descent_node_visits: AtomicU64::new(0),
			high_priority_started: Mutex::new(HashMap::new()),
			high_priority_io_count: AtomicU64::new(0),
			high_priority_io_total_micros: AtomicU64::new(0),
			high_priority_io_max_micros: AtomicU64::new(0),
			device_paths: Mutex::new(HashMap::new()),
			backing_stores: Mutex::new(HashMap::new()),
		}
	}

	#[cfg(test)]
	pub(crate) fn try_alloc_test_node(&self) -> Option<NodeId> {
		let mut reservation = self.reserve_chunk(1)?;
		Some(reservation.spawn_node(SemanticKind::Token, None, SpanMetrics::default()))
	}

	#[cfg(test)]
	pub(crate) fn alloc_test_node(&self) -> NodeId {
		self.try_alloc_test_node()
			.expect("test registry node capacity")
	}

	/// Atomically reserves an exact ordered set of node slots for one thread.
	/// Reusable slots are consumed before the bump tail; insufficient mixed
	/// capacity changes no allocator state.
	pub fn reserve_chunk(&self, size: u32) -> Option<RegistryChunk<'_>> {
		let mut allocator = self.allocator_state();
		let next = self.next_id.load(Ordering::Relaxed);
		let tail_available = self.capacity.saturating_add(1).saturating_sub(next);
		let available = u64::try_from(allocator.available.len())
			.expect("available slot count fits u64")
			.saturating_add(u64::from(tail_available));
		if u64::from(size) > available {
			return None;
		}

		let reused_count = usize::try_from(size)
			.expect("reservation size fits usize")
			.min(allocator.available.len());
		let mut slots = Vec::with_capacity(size as usize);
		for _ in 0..reused_count {
			slots.push(allocator.available.pop().expect("count checked"));
		}
		let fresh_count = size - u32::try_from(reused_count).expect("reused count fits u32");
		for offset in 0..fresh_count {
			let slot_code = next.checked_add(offset).expect("capacity bounds slot code");
			let id = NodeId::from_parts((slot_code - 1) as usize, 0)
				.expect("registry capacity fits NodeId slot field");
			slots.push(ReservedSlot { id, reused: false });
		}
		if fresh_count > 0 {
			self.next_id.store(next + fresh_count, Ordering::Release);
		}
		drop(allocator);
		self.bump_inspector_revision();
		Some(RegistryChunk::new(self, slots))
	}

	pub(crate) fn return_unpublished_slots(&self, slots: &[ReservedSlot]) {
		if slots.is_empty() {
			return;
		}
		self.allocator_state().available.extend_from_slice(slots);
		self.bump_inspector_revision();
	}

	pub(crate) fn initialize_reserved_node(
		&self,
		ticket: ReservedSlot,
		kind: SemanticKind,
		provider_kind: ProviderKindId,
		span: Option<SvpPointer>,
		metric: SpanMetrics,
	) -> NodeId {
		let node = ticket.id;
		let idx = node.index();
		assert!(
			idx < self.capacity as usize,
			"reserved slot exceeds registry"
		);
		if ticket.reused {
			assert!(
				!self.content_generation_exhausted[idx].load(Ordering::Acquire),
				"content-exhausted slots must be burned, never reserved"
			);
			assert_eq!(
				self.object_generation[idx]
					.load(Ordering::Acquire)
					.checked_add(1)
					.expect("generation-255 slots must burn before reservation"),
				node.generation(),
				"recycled slot generation must advance exactly once"
			);
			assert_eq!(
				self.io_lifetime[idx].load(Ordering::Acquire),
				IO_LIFETIME_RETIRED,
				"recycled slot must be retired and I/O-quiescent"
			);
			let _ = self.bump_content_generation_index(idx, node);
		} else {
			assert_eq!(node.generation(), 0, "fresh slots start at generation zero");
		}

		self.scrub_node_side_tables(node);
		{
			let _resident_write = self.resident_data_lock.write().unwrap();
			unsafe {
				*self.kinds[idx].get() = kind;
				*self.provider_kinds[idx].get() = provider_kind;
				*self.spans[idx].get() = span;
				*self.metrics[idx].get() = metric;
				*self.edges[idx].get() = TreeEdges::default();
				*self.child_tails[idx].get() = None;
				*self.virtual_data[idx].get() = None;
				(&mut *self.partial_residency[idx].get()).clear();
				*self.resident_lru_links[idx].get() = ResidentLruLinks::default();
			}
			self.dma_in_flight[idx].store(false, Ordering::Release);
			self.metric_inflating[idx].store(false, Ordering::Release);
			self.metrics_inflated[idx].store(false, Ordering::Release);
			self.terminal_newline_state[idx].store(0, Ordering::Release);
			assert_eq!(
				self.resident_accounted_bytes[idx].swap(0, Ordering::AcqRel),
				0,
				"reusable slot retained resident accounting"
			);
			assert!(
				!self.resident_counted_hot[idx].swap(false, Ordering::AcqRel),
				"reusable slot retained hot-residency accounting"
			);
		}
		self.object_generation[idx].store(node.generation(), Ordering::Release);
		self.io_lifetime[idx].store(0, Ordering::Release);
		self.slot_published[idx].store(true, Ordering::Release);
		Self::checked_monotonic_add(
			&self.allocation_events,
			1,
			"registry allocation-event counter exhausted",
		);
		Self::checked_monotonic_add(
			&self.topology_revision,
			1,
			"registry topology revision exhausted",
		);
		self.bump_inspector_revision();
		node
	}

	pub(crate) fn is_current_node(&self, node: NodeId) -> bool {
		let idx = node.index();
		idx < self.capacity as usize
			&& self.slot_published[idx].load(Ordering::Acquire)
			&& self.object_generation[idx].load(Ordering::Acquire) == node.generation()
	}

	pub(crate) fn checked_node_index(&self, node: NodeId) -> Option<usize> {
		self.is_current_node(node).then(|| node.index())
	}

	pub(crate) fn acquire_io_lifetime(self: &Arc<Self>, node: NodeId) -> Option<IoLifetimeLease> {
		// Serialize the validation-to-lease transition with retirement and
		// free-list publication. Without this lock, an old identity could pass
		// validation, lose its lifetime, and increment the replacement
		// object's lease count after first reuse.
		let _allocator = self.allocator_state();
		let idx = self.checked_node_index(node)?;
		if self.content_generation_exhausted[idx].load(Ordering::Acquire) {
			return None;
		}
		let state = &self.io_lifetime[idx];
		loop {
			let current = state.load(Ordering::Acquire);
			if current & IO_LIFETIME_RETIRED != 0
				|| current & IO_LIFETIME_COUNT_MASK == IO_LIFETIME_COUNT_MASK
			{
				return None;
			}
			if state
				.compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
			{
				return Some(IoLifetimeLease {
					registry: Arc::clone(self),
					node,
				});
			}
		}
	}

	fn release_io_lifetime(&self, node: NodeId) {
		let idx = node.index();
		let previous = self.io_lifetime[idx].fetch_sub(1, Ordering::AcqRel);
		assert!(
			previous & IO_LIFETIME_COUNT_MASK > 0,
			"resolver lifetime lease underflow"
		);
		if previous == IO_LIFETIME_RETIRED | 1 {
			self.finalize_reusable_slot(node, true);
		}
	}

	fn retire_io_lifetime(&self, node: NodeId) {
		let idx = node.index();
		let mut allocator = self.allocator_state();
		let previous = self.io_lifetime[idx].fetch_or(IO_LIFETIME_RETIRED, Ordering::AcqRel);
		if previous & IO_LIFETIME_RETIRED != 0 {
			return;
		}
		if previous & IO_LIFETIME_COUNT_MASK == 0 {
			drop(allocator);
			self.finalize_reusable_slot(node, false);
		} else {
			allocator.pending_reuse += 1;
			drop(allocator);
			self.bump_inspector_revision();
		}
	}

	fn finalize_reusable_slot(&self, node: NodeId, was_pending: bool) {
		let idx = node.index();
		assert_eq!(
			self.io_lifetime[idx].load(Ordering::Acquire),
			IO_LIFETIME_RETIRED,
			"only retired I/O-quiescent slots are reusable"
		);
		self.scrub_node_side_tables(node);
		self.slot_published[idx].store(false, Ordering::Release);
		let content_exhausted = self.content_generation_exhausted[idx].load(Ordering::Acquire);
		let mut allocator = self.allocator_state();
		if was_pending {
			allocator.pending_reuse = allocator
				.pending_reuse
				.checked_sub(1)
				.expect("pending reuse counter underflow");
		}
		if node.generation() == NODE_MAX_GENERATION || content_exhausted {
			allocator.burned = allocator.burned.saturating_add(1);
		} else {
			let id = NodeId::from_parts(idx, node.generation() + 1)
				.expect("existing slot fits packed NodeId");
			allocator.available.push(ReservedSlot { id, reused: true });
		}
		drop(allocator);
		self.bump_inspector_revision();
	}

	fn scrub_node_side_tables(&self, node: NodeId) {
		let mut removed_pending = 0u64;
		let mut empty_scans = Vec::new();
		{
			let mut scans = self.metric_scans.lock().unwrap();
			if let Some(scan) = scans.remove(&node) {
				removed_pending = removed_pending.saturating_add(scan.pending.len() as u64);
			}
			for (root, scan) in scans.iter_mut() {
				if scan.pending.remove(&node) {
					removed_pending = removed_pending.saturating_add(1);
				}
				scan.unclaimed.retain(|claim| claim.node_id != node);
				if scan.pending.is_empty() && scan.in_flight == 0 {
					empty_scans.push(*root);
				}
			}
			for root in empty_scans {
				scans.remove(&root);
			}
		}
		if removed_pending > 0 {
			let _ = self.pending_scan_blocks.fetch_update(
				Ordering::AcqRel,
				Ordering::Acquire,
				|current| Some(current.saturating_sub(removed_pending)),
			);
		}
		self.whole_leaf_read_outcomes
			.lock()
			.unwrap()
			.retain(|(candidate, _), _| *candidate != node);
		self.pending_metric_inflations
			.lock()
			.unwrap()
			.retain(|entry| entry.node_id != node && entry.scan_root != Some(node));
		self.incomplete_metric_roots.lock().unwrap().remove(&node);
		let mut indexes = self.root_child_line_indexes.write().unwrap();
		let removed_root = usize::from(indexes.remove(&node).is_some());
		let mut removed_entries = 0usize;
		for index in indexes.values_mut() {
			removed_entries = removed_entries.saturating_add(usize::from(index.remove(node)));
		}
		drop(indexes);
		if removed_root > 0 || removed_entries > 0 {
			self.note_root_child_line_index_change(removed_entries as u64);
		}
		for nodes in self.viewport_resident_pins.lock().unwrap().values_mut() {
			nodes.remove(&node);
		}
		self.high_priority_started.lock().unwrap().remove(&node);
	}

	pub(crate) fn topology_revision(&self) -> u64 {
		self.topology_revision.load(Ordering::Acquire)
	}

	/// Highest bump-tail slot ever committed. Recycled allocation does not
	/// change this value.
	pub(crate) fn allocated_node_count(&self) -> u64 {
		u64::from(self.next_id.load(Ordering::Acquire).saturating_sub(1))
	}

	pub(crate) fn allocation_event_count(&self) -> u64 {
		self.allocation_events.load(Ordering::Acquire)
	}

	pub(crate) fn capacity(&self) -> u64 {
		u64::from(self.capacity)
	}

	pub(crate) fn remaining_node_capacity(&self) -> u64 {
		let allocator = self.allocator_state();
		self.capacity()
			.saturating_sub(self.allocated_node_count())
			.saturating_add(allocator.available.len() as u64)
	}

	pub(crate) fn capacity_refusal_message(&self, operation: &str, required: u64) -> String {
		format!(
			"{operation} refused: UAST registry node capacity exhausted; needs {required} \
			 nodes but only {} immediately reusable slots remain. Wait for reclamation \
			 or restart the editor",
			self.remaining_node_capacity()
		)
	}

	pub(crate) fn retired_node_count(&self) -> u64 {
		self.retired_nodes.load(Ordering::Acquire)
	}

	/// Records a logical retirement without reclaiming the node or changing
	/// stale-`NodeId` behavior.
	pub(crate) fn note_nodes_retired(&self, count: u64) {
		if count == 0 {
			return;
		}
		let _ = self
			.retired_nodes
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
				Some(current.saturating_add(count))
			});
		self.bump_inspector_revision();
	}

	fn checked_monotonic_add(counter: &AtomicU64, delta: u64, message: &'static str) {
		if delta == 0 {
			return;
		}
		counter
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
				current.checked_add(delta)
			})
			.expect(message);
	}

	fn note_authoritative_length_delta(&self, old_len: u64, new_len: u64) {
		if new_len >= old_len {
			Self::checked_monotonic_add(
				&self.authoritative_bytes_added,
				new_len - old_len,
				"authoritative payload additions overflow",
			);
		} else {
			Self::checked_monotonic_add(
				&self.authoritative_bytes_removed,
				old_len - new_len,
				"authoritative payload removals overflow",
			);
		}
	}

	pub(crate) fn authoritative_payload_snapshot(&self) -> AuthoritativePayloadSnapshot {
		AuthoritativePayloadSnapshot {
			added: self.authoritative_bytes_added.load(Ordering::Acquire),
			removed: self.authoritative_bytes_removed.load(Ordering::Acquire),
			orphaned_added: self.orphaned_payload_bytes_added.load(Ordering::Acquire),
			orphaned_removed: self.orphaned_payload_bytes_removed.load(Ordering::Acquire),
		}
	}

	pub(crate) fn authoritative_payload_len(&self, node: NodeId) -> u64 {
		let _resident_read = self.resident_data_lock.read().unwrap();
		let idx = node.index();
		unsafe {
			if (*self.spans[idx].get()).is_some() {
				0
			} else {
				(&*self.virtual_data[idx].get())
					.as_ref()
					.map_or(0, |bytes| bytes.len() as u64)
			}
		}
	}

	pub(crate) fn note_orphaned_payload(&self, bytes: u64) {
		if bytes == 0 {
			return;
		}
		Self::checked_monotonic_add(
			&self.orphaned_payload_bytes_added,
			bytes,
			"orphaned payload additions overflow",
		);
		self.bump_inspector_revision();
	}

	pub(crate) fn retire_orphaned_forest(
		&self,
		heads: impl IntoIterator<Item = NodeId>,
		retired_nodes: u64,
		authoritative_payload: u64,
		fence: ReclaimFence,
	) {
		let heads = heads.into_iter().collect::<Vec<_>>();
		assert!(
			!heads.is_empty() || (retired_nodes == 0 && authoritative_payload == 0),
			"nonempty retirement requires at least one forest head"
		);
		for head in &heads {
			assert!(
				unsafe { (*self.edges[head.index()].get()).parent.is_none() },
				"retired forest head must be detached"
			);
		}
		self.note_nodes_retired(retired_nodes);
		self.note_orphaned_payload(authoritative_payload);
		if !heads.is_empty() {
			self.retired_forest_outbox
				.lock()
				.unwrap()
				.push(RetiredForestNotice { heads, fence });
		}
	}

	pub(crate) fn drain_retired_forest_notices(&self) -> Vec<RetiredForestNotice> {
		std::mem::take(&mut *self.retired_forest_outbox.lock().unwrap())
	}

	/// Make one node in a retired forest semantically and physically inert,
	/// then retire its object lifetime once all resolver leases are quiescent.
	///
	/// Reclamation and physical splits are engine-thread serialized. Advancing
	/// the generation while holding the residency write lock makes every late
	/// whole/partial resolver publication fail before it can resurrect bytes.
	pub(crate) fn reclaim_orphaned_node(&self, node: NodeId) -> ReclaimedPayload {
		let _resident_write = self.resident_data_lock.write().unwrap();
		let Some(idx) = self.checked_node_index(node) else {
			return ReclaimedPayload::default();
		};
		let _ = self.bump_content_generation_index(idx, node);
		unsafe {
			*self.kinds[idx].get() = SemanticKind::None;
			*self.provider_kinds[idx].get() = ProviderKindId::NONE;
		}
		let spanless = unsafe { (*self.spans[idx].get()).is_none() };
		let full_len = unsafe {
			(&mut *self.virtual_data[idx].get())
				.take()
				.map_or(0, |bytes| bytes.len() as u64)
		};
		unsafe {
			(&mut *self.partial_residency[idx].get()).clear();
		}
		let accounted = self.resident_accounted_bytes[idx].swap(0, Ordering::AcqRel);
		if accounted > 0 {
			self.resident_bytes.fetch_sub(accounted, Ordering::AcqRel);
		}
		if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
			self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
		}
		self.touch_resident_lru(node, false);

		let reclaimed = if spanless {
			Self::checked_monotonic_add(
				&self.authoritative_bytes_removed,
				full_len,
				"authoritative payload removals overflow",
			);
			Self::checked_monotonic_add(
				&self.orphaned_payload_bytes_removed,
				full_len,
				"orphaned payload removals overflow",
			);
			ReclaimedPayload {
				authoritative_bytes: full_len,
				hydrated_bytes: 0,
			}
		} else {
			ReclaimedPayload {
				authoritative_bytes: 0,
				hydrated_bytes: accounted,
			}
		};
		self.bump_inspector_revision();
		drop(_resident_write);
		self.retire_io_lifetime(node);
		reclaimed
	}

	/// Replace the sole copy of a span-less node's content.
	///
	/// Authoritative writes are engine-serialized. The residency lock keeps the
	/// payload and its byte accounting atomic with respect to resolver threads.
	pub(crate) fn replace_authoritative_data(&self, node: NodeId, data: Vec<u8>) {
		let idx = self
			.checked_node_index(node)
			.expect("authoritative write targets a stale UAST node");
		let _resident_write = self.resident_data_lock.write().unwrap();
		assert!(
			self.is_current_node(node),
			"authoritative write raced a retired UAST node"
		);
		assert!(
			unsafe { (*self.spans[idx].get()).is_none() },
			"authoritative payload requires a span-less node"
		);
		let old_len = unsafe {
			(&*self.virtual_data[idx].get())
				.as_ref()
				.map_or(0, |bytes| bytes.len() as u64)
		};
		let new_len = data.len() as u64;
		self.replace_virtual_data_locked(node, data);
		self.note_authoritative_length_delta(old_len, new_len);
	}

	/// Mutate a span-less node's authoritative bytes without exposing an
	/// uncounted raw `Vec` write.
	pub(crate) fn mutate_authoritative_data<R>(
		&self,
		node: NodeId,
		mutate: impl FnOnce(&mut Vec<u8>) -> R,
	) -> R {
		let idx = self
			.checked_node_index(node)
			.expect("authoritative mutation targets a stale UAST node");
		let _resident_write = self.resident_data_lock.write().unwrap();
		assert!(
			self.is_current_node(node),
			"authoritative mutation raced a retired UAST node"
		);
		assert!(
			unsafe { (*self.spans[idx].get()).is_none() },
			"authoritative payload mutation requires a span-less node"
		);
		let mut data = unsafe {
			(&mut *self.virtual_data[idx].get())
				.take()
				.expect("authoritative leaf must retain its sole payload")
		};
		let old_len = data.len() as u64;
		let result = mutate(&mut data);
		let new_len = data.len() as u64;
		self.replace_virtual_data_locked(node, data);
		self.note_authoritative_length_delta(old_len, new_len);
		result
	}

	/// Publish reconstructible full data for a span-backed node.
	pub(crate) fn publish_hydrated_data(&self, node: NodeId, data: Vec<u8>) {
		let idx = self
			.checked_node_index(node)
			.expect("hydration targets a stale UAST node");
		let _resident_write = self.resident_data_lock.write().unwrap();
		assert!(
			self.is_current_node(node),
			"hydration raced a retired UAST node"
		);
		assert!(
			unsafe { (*self.spans[idx].get()).is_some() },
			"hydrated payload requires a physical span"
		);
		self.replace_virtual_data_locked(node, data);
	}

	/// Remove reconstructible full data for a physical split.
	pub(crate) fn take_hydrated_data(&self, node: NodeId) -> Option<Vec<u8>> {
		let idx = self
			.checked_node_index(node)
			.expect("hydrated payload take targets a stale UAST node");
		let _resident_write = self.resident_data_lock.write().unwrap();
		assert!(
			self.is_current_node(node),
			"hydrated payload take raced a retired UAST node"
		);
		assert!(
			unsafe { (*self.spans[idx].get()).is_some() },
			"hydrated payload take requires a physical span"
		);
		let data = unsafe { (&mut *self.virtual_data[idx].get()).take() };
		unsafe {
			(&mut *self.partial_residency[idx].get()).clear();
		}
		let accounted = self.resident_accounted_bytes[idx].swap(0, Ordering::AcqRel);
		if accounted > 0 {
			self.resident_bytes.fetch_sub(accounted, Ordering::AcqRel);
			if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
				self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
			}
		}
		self.touch_resident_lru(node, false);
		self.bump_inspector_revision();
		data
	}

	/// Convert a physical node's retained full payload into its sole copy.
	pub(crate) fn promote_hydrated_data(&self, node: NodeId) {
		let idx = self
			.checked_node_index(node)
			.expect("promotion targets a stale UAST node");
		let _resident_write = self.resident_data_lock.write().unwrap();
		assert!(
			self.is_current_node(node),
			"promotion raced a retired UAST node"
		);
		assert!(
			unsafe { (*self.spans[idx].get()).is_some() },
			"promotion requires a physical span"
		);
		let authoritative_len = unsafe {
			*self.spans[idx].get() = None;
			(&mut *self.partial_residency[idx].get()).clear();
			(&*self.virtual_data[idx].get())
				.as_ref()
				.map_or(0, |bytes| bytes.len() as u64)
		};
		let old_accounted =
			self.resident_accounted_bytes[idx].swap(authoritative_len, Ordering::AcqRel);
		if authoritative_len >= old_accounted {
			self.resident_bytes
				.fetch_add(authoritative_len - old_accounted, Ordering::AcqRel);
		} else {
			self.resident_bytes
				.fetch_sub(old_accounted - authoritative_len, Ordering::AcqRel);
		}
		if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
			self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
		}
		self.touch_resident_lru(node, false);
		Self::checked_monotonic_add(
			&self.authoritative_bytes_added,
			authoritative_len,
			"authoritative payload additions overflow",
		);
		self.bump_inspector_revision();
	}

	/// Rebind a leaf to the just-saved file generation. Unpinned resident bytes
	/// are discarded so a save still re-sparsifies the document, but leaves in a
	/// current viewport keep their resident copy. The latter is important because
	/// the save command renders immediately, before a cold-leaf hydration can
	/// complete; dropping a visible leaf would paint a blank frame until the next
	/// edit happened to request another repaint.
	pub(crate) fn rebind_data_to_saved_span(&self, node: NodeId, span: Option<SvpPointer>) {
		let idx = self
			.checked_node_index(node)
			.expect("saved-span rebind targets a stale UAST node");
		let _resident_write = self.resident_data_lock.write().unwrap();
		assert!(
			self.is_current_node(node),
			"saved-span rebind raced a retired UAST node"
		);
		let was_authoritative = unsafe { (*self.spans[idx].get()).is_none() };
		let preserve_resident = self
			.viewport_resident_pins
			.lock()
			.unwrap()
			.values()
			.any(|nodes| nodes.contains(&node));
		let authoritative_len = if was_authoritative {
			unsafe {
				(&*self.virtual_data[idx].get())
					.as_ref()
					.map_or(0, |bytes| bytes.len() as u64)
			}
		} else {
			0
		};
		unsafe {
			*self.spans[idx].get() = span;
			if !preserve_resident && span.is_some() {
				(&mut *self.virtual_data[idx].get()).take();
				(&mut *self.partial_residency[idx].get()).clear();
			} else if span.is_none() {
				assert_eq!(
					authoritative_len, 0,
					"nonempty saved leaf requires a physical span"
				);
				if !preserve_resident {
					*(&mut *self.virtual_data[idx].get()) = Some(Vec::new());
				}
			}
		}
		if preserve_resident && span.is_some() {
			let new_accounted = unsafe {
				let full = (&*self.virtual_data[idx].get())
					.as_ref()
					.map_or(0, |bytes| bytes.len() as u64);
				let partial = (&*self.partial_residency[idx].get())
					.iter()
					.map(|range| range.bytes.len() as u64)
					.sum::<u64>();
				full.saturating_add(partial)
			};
			let old_accounted =
				self.resident_accounted_bytes[idx].swap(new_accounted, Ordering::AcqRel);
			if new_accounted >= old_accounted {
				self.resident_bytes
					.fetch_add(new_accounted - old_accounted, Ordering::AcqRel);
			} else {
				self.resident_bytes
					.fetch_sub(old_accounted - new_accounted, Ordering::AcqRel);
			}
			if new_accounted > 0 {
				if !self.resident_counted_hot[idx].swap(true, Ordering::AcqRel) {
					self.hot_extent_count.fetch_add(1, Ordering::Relaxed);
				}
			} else if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
				self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
			}
			self.touch_resident_lru(node, new_accounted > 0);
		} else {
			let accounted = self.resident_accounted_bytes[idx].swap(0, Ordering::AcqRel);
			if accounted > 0 {
				self.resident_bytes.fetch_sub(accounted, Ordering::AcqRel);
			}
			if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
				self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
			}
			self.touch_resident_lru(node, false);
		}
		Self::checked_monotonic_add(
			&self.authoritative_bytes_removed,
			authoritative_len,
			"authoritative payload removals overflow",
		);
		self.bump_inspector_revision();
	}

	/// Replace the payload and metrics of a stable synthetic one-leaf document.
	///
	/// Output panels use this instead of allocating a new root/leaf on every
	/// refresh. The caller owns the topology and supplies both nodes so the
	/// aggregate root metrics can be kept in sync with the leaf.
	pub(crate) fn replace_virtual_document_data(&self, root: NodeId, leaf: NodeId, data: Vec<u8>) {
		let byte_length = data.len() as u64;
		let newline_count = data.iter().filter(|&&byte| byte == b'\n').count() as u32;
		let ends_with_newline = data.last() == Some(&b'\n');
		self.replace_authoritative_data(leaf, data);
		self.complete_metric_inflation(leaf, newline_count, ends_with_newline);
		unsafe {
			(*self.metrics[leaf.index()].get()).byte_length = byte_length;
			(*self.metrics[root.index()].get()).byte_length = byte_length;
		}
	}

	fn unlink_resident_lru_locked(&self, lru: &mut ResidentLru, node: NodeId) -> bool {
		let links = unsafe { &mut *self.resident_lru_links[node.index()].get() };
		if !links.linked {
			return false;
		}
		if let Some(prev) = links.prev {
			unsafe {
				(*self.resident_lru_links[prev.index()].get()).next = links.next;
			}
		} else {
			lru.head = links.next;
		}
		if let Some(next) = links.next {
			unsafe {
				(*self.resident_lru_links[next.index()].get()).prev = links.prev;
			}
		} else {
			lru.tail = links.prev;
		}
		*links = ResidentLruLinks::default();
		lru.len = lru.len.saturating_sub(1);
		true
	}

	fn push_resident_lru_tail_locked(&self, lru: &mut ResidentLru, node: NodeId) {
		let links = unsafe { &mut *self.resident_lru_links[node.index()].get() };
		debug_assert!(!links.linked, "resident LRU node linked twice");
		links.prev = lru.tail;
		links.next = None;
		links.linked = true;
		if let Some(tail) = lru.tail {
			unsafe {
				(*self.resident_lru_links[tail.index()].get()).next = Some(node);
			}
		} else {
			lru.head = Some(node);
		}
		lru.tail = Some(node);
		lru.len = lru.len.saturating_add(1);
	}

	fn touch_resident_lru(&self, node: NodeId, track: bool) {
		let mut lru = self.resident_lru.lock().unwrap();
		self.unlink_resident_lru_locked(&mut lru, node);
		if track {
			self.push_resident_lru_tail_locked(&mut lru, node);
		}
	}

	fn pop_resident_lru_head_locked(&self, lru: &mut ResidentLru) -> Option<NodeId> {
		let node = lru.head?;
		self.unlink_resident_lru_locked(lru, node);
		Some(node)
	}

	fn replace_virtual_data_locked(&self, node: NodeId, data: Vec<u8>) {
		let idx = node.index();
		let has_data = !data.is_empty();
		unsafe {
			let slot = &mut *self.virtual_data[idx].get();
			(&mut *self.partial_residency[idx].get()).clear();
			let new_len = data.len() as u64;
			let old_len = self.resident_accounted_bytes[idx].swap(new_len, Ordering::AcqRel);
			if old_len == 0
				&& new_len > 0
				&& (*self.spans[idx].get()).is_some()
				&& !self.resident_counted_hot[idx].swap(true, Ordering::AcqRel)
			{
				self.hot_extent_count.fetch_add(1, Ordering::Relaxed);
			}
			self.resident_bytes.fetch_add(new_len, Ordering::Relaxed);
			self.resident_bytes.fetch_sub(old_len, Ordering::Relaxed);
			*slot = Some(data);
		}
		let reconstructible = unsafe { (*self.spans[idx].get()).is_some() };
		self.touch_resident_lru(node, reconstructible && has_data);
		self.bump_inspector_revision();
	}

	pub(crate) fn publish_resolved_data(
		&self,
		node: NodeId,
		expected_generation: u64,
		data: Vec<u8>,
	) -> bool {
		let _resident_write = self.resident_data_lock.write().unwrap();
		if !self.is_current_node(node)
			|| self.io_lifetime[node.index()].load(Ordering::Acquire) & IO_LIFETIME_RETIRED != 0
			|| self.content_generation_exhausted[node.index()].load(Ordering::Acquire)
			|| self.content_generation(node) != expected_generation
		{
			return false;
		}
		assert!(
			unsafe { (*self.spans[node.index()].get()).is_some() },
			"resolver hydration requires a physical span"
		);
		self.replace_virtual_data_locked(node, data);
		true
	}

	pub(crate) fn content_generation(&self, node: NodeId) -> u64 {
		self.checked_node_index(node).map_or(u64::MAX, |idx| {
			self.content_generation[idx].load(Ordering::Acquire)
		})
	}

	pub(crate) fn bump_content_generation(&self, node: NodeId) -> u64 {
		let Some(idx) = self.checked_node_index(node) else {
			return u64::MAX;
		};
		self.bump_content_generation_index(idx, node)
	}

	fn bump_content_generation_index(&self, idx: usize, node: NodeId) -> u64 {
		let generation = match self.content_generation[idx].fetch_update(
			Ordering::AcqRel,
			Ordering::Acquire,
			|current| current.checked_add(1),
		) {
			Ok(previous) => previous + 1,
			Err(_) => {
				self.content_generation_exhausted[idx].store(true, Ordering::Release);
				u64::MAX
			}
		};
		self.whole_leaf_read_outcomes
			.lock()
			.unwrap()
			.retain(|(candidate, _), _| *candidate != node);
		generation
	}

	pub(crate) fn whole_leaf_is_resident(&self, node: NodeId) -> bool {
		let _resident_read = self.resident_data_lock.read().unwrap();
		self.checked_node_index(node)
			.is_some_and(|idx| unsafe { (&*self.virtual_data[idx].get()).is_some() })
	}

	pub(crate) fn node_span(&self, node: NodeId) -> Option<SvpPointer> {
		let idx = self.checked_node_index(node)?;
		unsafe { *self.spans[idx].get() }
	}

	pub(crate) fn begin_dma(&self, node: NodeId) -> bool {
		let Some(idx) = self.checked_node_index(node) else {
			return false;
		};
		!self.dma_in_flight[idx].swap(true, Ordering::AcqRel)
	}

	pub(crate) fn finish_dma(&self, node: NodeId) {
		if let Some(idx) = self.checked_node_index(node) {
			self.dma_in_flight[idx].store(false, Ordering::Release);
		}
	}

	pub(crate) fn whole_leaf_read_outcome(
		&self,
		node: NodeId,
		generation: u64,
	) -> Option<WholeLeafReadOutcome> {
		self.whole_leaf_read_outcomes
			.lock()
			.unwrap()
			.get(&(node, generation))
			.copied()
	}

	pub(crate) fn begin_whole_leaf_read(
		&self,
		node: NodeId,
		generation: u64,
	) -> WholeLeafReadOutcome {
		let mut outcomes = self.whole_leaf_read_outcomes.lock().unwrap();
		*outcomes
			.entry((node, generation))
			.or_insert(WholeLeafReadOutcome::InFlight)
	}

	pub(crate) fn cancel_whole_leaf_read(&self, node: NodeId, generation: u64) {
		let mut outcomes = self.whole_leaf_read_outcomes.lock().unwrap();
		if outcomes.get(&(node, generation)) == Some(&WholeLeafReadOutcome::InFlight) {
			outcomes.remove(&(node, generation));
		}
	}

	pub(crate) fn finish_whole_leaf_read(&self, node: NodeId, generation: u64, succeeded: bool) {
		let mut outcomes = self.whole_leaf_read_outcomes.lock().unwrap();
		if !self.is_current_node(node) || self.content_generation(node) != generation {
			outcomes.remove(&(node, generation));
			return;
		}
		if succeeded {
			outcomes.remove(&(node, generation));
		} else {
			outcomes.insert((node, generation), WholeLeafReadOutcome::Failed);
		}
	}

	#[allow(dead_code)] // Wired by the bounded-read API's explicit retry operation.
	pub(crate) fn retry_whole_leaf_read(&self, node: NodeId) {
		let generation = self.content_generation(node);
		let mut outcomes = self.whole_leaf_read_outcomes.lock().unwrap();
		if outcomes.get(&(node, generation)) == Some(&WholeLeafReadOutcome::Failed) {
			outcomes.remove(&(node, generation));
		}
	}

	#[cfg(test)]
	pub(crate) fn store_partial_resident_range(&self, node: NodeId, offset: u32, data: Vec<u8>) {
		let _resident_write = self.resident_data_lock.write().unwrap();
		self.store_partial_resident_range_locked(node, offset, data);
	}

	fn store_partial_resident_range_locked(&self, node: NodeId, offset: u32, data: Vec<u8>) {
		let idx = node.index();
		let old_accounted = self.resident_accounted_bytes[idx].load(Ordering::Acquire);
		unsafe {
			let ranges = &mut *self.partial_residency[idx].get();
			let end = offset.saturating_add(data.len() as u32);
			ranges.retain(|range| {
				let range_end = range.offset.saturating_add(range.bytes.len() as u32);
				range_end <= offset || range.offset >= end
			});
			ranges.push(ResidentRange {
				offset,
				bytes: data,
			});
			ranges.sort_unstable_by_key(|range| range.offset);
			let partial_bytes = ranges
				.iter()
				.map(|range| range.bytes.len() as u64)
				.sum::<u64>();
			let full_bytes = (&*self.virtual_data[idx].get())
				.as_ref()
				.map_or(0, |bytes| bytes.len() as u64);
			let new_accounted = full_bytes.saturating_add(partial_bytes);
			self.resident_accounted_bytes[idx].store(new_accounted, Ordering::Release);
			if old_accounted == 0
				&& new_accounted > 0
				&& (*self.spans[idx].get()).is_some()
				&& !self.resident_counted_hot[idx].swap(true, Ordering::AcqRel)
			{
				self.hot_extent_count.fetch_add(1, Ordering::Relaxed);
			}
			if new_accounted >= old_accounted {
				self.resident_bytes
					.fetch_add(new_accounted - old_accounted, Ordering::AcqRel);
			} else {
				self.resident_bytes
					.fetch_sub(old_accounted - new_accounted, Ordering::AcqRel);
			}
		}
		let reconstructible = unsafe { (*self.spans[idx].get()).is_some() };
		self.touch_resident_lru(
			node,
			reconstructible && self.resident_accounted_bytes[idx].load(Ordering::Acquire) > 0,
		);
		self.bump_inspector_revision();
	}

	pub(crate) fn publish_partial_resident_range(
		&self,
		node: NodeId,
		expected_generation: u64,
		offset: u32,
		data: Vec<u8>,
	) -> bool {
		let _resident_write = self.resident_data_lock.write().unwrap();
		if !self.is_current_node(node)
			|| self.io_lifetime[node.index()].load(Ordering::Acquire) & IO_LIFETIME_RETIRED != 0
			|| self.content_generation_exhausted[node.index()].load(Ordering::Acquire)
			|| self.content_generation(node) != expected_generation
		{
			return false;
		}
		assert!(
			unsafe { (*self.spans[node.index()].get()).is_some() },
			"partial resolver hydration requires a physical span"
		);
		self.store_partial_resident_range_locked(node, offset, data);
		true
	}

	pub(crate) fn refresh_resident_accounting(&self, node: NodeId) {
		let _resident_read = self.resident_data_lock.read().unwrap();
		let idx = node.index();
		let new_accounted = unsafe {
			let full = (&*self.virtual_data[idx].get())
				.as_ref()
				.map_or(0, |bytes| bytes.len() as u64);
			let partial = (&*self.partial_residency[idx].get())
				.iter()
				.map(|range| range.bytes.len() as u64)
				.sum::<u64>();
			full.saturating_add(partial)
		};
		let old_accounted =
			self.resident_accounted_bytes[idx].swap(new_accounted, Ordering::AcqRel);
		if new_accounted >= old_accounted {
			self.resident_bytes
				.fetch_add(new_accounted - old_accounted, Ordering::AcqRel);
		} else {
			self.resident_bytes
				.fetch_sub(old_accounted - new_accounted, Ordering::AcqRel);
		}
		let reconstructible = unsafe { (*self.spans[idx].get()).is_some() };
		if reconstructible && new_accounted > 0 {
			if !self.resident_counted_hot[idx].swap(true, Ordering::AcqRel) {
				self.hot_extent_count.fetch_add(1, Ordering::Relaxed);
			}
		} else if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
			self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
		}
		self.touch_resident_lru(node, reconstructible && new_accounted > 0);
		self.bump_inspector_revision();
	}

	pub fn read_partial_resident_range(
		&self,
		node: NodeId,
		offset: u32,
		length: u32,
	) -> Option<Vec<u8>> {
		let _resident_read = self.resident_data_lock.read().unwrap();
		let end = offset.checked_add(length)?;
		unsafe {
			(&*self.partial_residency[node.index()].get())
				.iter()
				.find_map(|range| {
					let range_end = range.offset.checked_add(range.bytes.len() as u32)?;
					if offset >= range.offset && end <= range_end {
						let start = (offset - range.offset) as usize;
						let finish = start + length as usize;
						Some(range.bytes[start..finish].to_vec())
					} else {
						None
					}
				})
		}
	}

	pub(crate) fn drop_resident_data(&self, node: NodeId) {
		let _resident_write = self.resident_data_lock.write().unwrap();
		let idx = node.index();
		let spanless_payload_len = unsafe {
			if (*self.spans[idx].get()).is_none() {
				(&*self.virtual_data[idx].get())
					.as_ref()
					.map_or(0, |bytes| bytes.len())
			} else {
				0
			}
		};
		assert_eq!(
			spanless_payload_len, 0,
			"authoritative payload must not be dropped as evictable residency"
		);
		unsafe {
			(&mut *self.virtual_data[idx].get()).take();
			(&mut *self.partial_residency[idx].get()).clear();
		}
		let accounted = self.resident_accounted_bytes[idx].swap(0, Ordering::AcqRel);
		if accounted > 0 {
			self.resident_bytes.fetch_sub(accounted, Ordering::AcqRel);
			if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
				self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
			}
		}
		self.touch_resident_lru(node, false);
		self.bump_inspector_revision();
	}

	pub fn set_resident_budget(&self, high_water_bytes: u64, low_water_bytes: u64) {
		let low_water_bytes = low_water_bytes.min(high_water_bytes);
		self.resident_budget_high
			.store(high_water_bytes, Ordering::Release);
		self.resident_budget_low
			.store(low_water_bytes, Ordering::Release);
		self.enforce_resident_budget(None);
	}

	pub(crate) fn set_viewport_resident_pins(
		&self,
		owner: u32,
		nodes: impl IntoIterator<Item = NodeId>,
	) {
		self.viewport_resident_pins
			.lock()
			.unwrap()
			.insert(owner, nodes.into_iter().collect());
	}

	pub(crate) fn retain_viewport_pin_owners(&self, owners: &HashSet<u32>) {
		self.viewport_resident_pins
			.lock()
			.unwrap()
			.retain(|owner, _| owners.contains(owner));
	}

	/// Demotes clean physical leaves until the low-water mark is restored.
	///
	/// MUST be called only from the engine thread. Demotion drops the `Vec` behind
	/// `virtual_data`, and engine-thread readers hand out `&[u8]` borrowed directly
	/// into those buffers (`for_each_loaded_slice_fragment`, `SumTree::build_from_registry`,
	/// `stream_document_to_writer`). Calling this from a resolver thread frees memory
	/// those readers are still walking. Enforcement therefore runs once per frame, at
	/// the end of `render_frame`, where no such borrow is outstanding.
	pub fn enforce_resident_budget(&self, pinned: Option<NodeId>) {
		let _resident_write = self.resident_data_lock.write().unwrap();
		let high = self.resident_budget_high.load(Ordering::Acquire);
		if self.resident_bytes.load(Ordering::Acquire) <= high {
			return;
		}
		let low = self.resident_budget_low.load(Ordering::Acquire);
		let mut lru = self.resident_lru.lock().unwrap();
		let viewport_pins = self.viewport_resident_pins.lock().unwrap();
		let candidates = lru.len;
		for _ in 0..candidates {
			if self.resident_bytes.load(Ordering::Acquire) <= low {
				break;
			}
			let Some(candidate) = self.pop_resident_lru_head_locked(&mut lru) else {
				break;
			};
			let idx = candidate.index();
			let reconstructible = unsafe { (*self.spans[idx].get()).is_some() };
			let in_flight = self.dma_in_flight[idx].load(Ordering::Acquire);
			let visible = viewport_pins
				.values()
				.any(|nodes| nodes.contains(&candidate));
			if Some(candidate) == pinned || visible || !reconstructible || in_flight {
				self.push_resident_lru_tail_locked(&mut lru, candidate);
				continue;
			}
			let removed_any = unsafe {
				let full = (&mut *self.virtual_data[idx].get()).take().is_some();
				let ranges = &mut *self.partial_residency[idx].get();
				let partial = !ranges.is_empty();
				ranges.clear();
				full || partial
			};
			if removed_any {
				let accounted = self.resident_accounted_bytes[idx].swap(0, Ordering::AcqRel);
				self.resident_bytes.fetch_sub(accounted, Ordering::AcqRel);
				if self.resident_counted_hot[idx].swap(false, Ordering::AcqRel) {
					self.hot_extent_count.fetch_sub(1, Ordering::AcqRel);
				}
				self.bump_inspector_revision();
			}
		}
	}

	pub(crate) fn resident_data_guard(&self) -> RwLockReadGuard<'_, ()> {
		self.resident_data_lock.read().unwrap()
	}

	#[cfg(test)]
	pub(crate) fn resident_lru_len(&self) -> usize {
		self.resident_lru.lock().unwrap().len
	}

	pub(crate) fn refresh_terminal_newline_from_resident(&self, node: NodeId) {
		let _resident_read = self.resident_data_lock.read().unwrap();
		if let Some(bytes) = unsafe { (&*self.virtual_data[node.index()].get()).as_deref() } {
			self.set_terminal_newline(node, bytes.last() == Some(&b'\n'));
		}
	}

	pub fn stats(&self) -> RegistryStatsSnapshot {
		let payload = self.authoritative_payload_snapshot();
		let allocator = self.allocator_state();
		RegistryStatsSnapshot {
			allocated_nodes: self.allocated_node_count(),
			allocation_events: self.allocation_event_count(),
			capacity: self.capacity(),
			reusable_slots: allocator.available.len() as u64,
			pending_reuse_slots: allocator.pending_reuse,
			burned_slots: allocator.burned,
			retired_nodes: self.retired_node_count(),
			cold_extents: self.cold_extent_count.load(Ordering::Acquire),
			hot_extents: self.hot_extent_count.load(Ordering::Acquire),
			scanned_bytes: self.scanned_bytes.load(Ordering::Acquire),
			pending_scan_blocks: self.pending_scan_blocks.load(Ordering::Acquire),
			resident_bytes: self.resident_bytes.load(Ordering::Acquire),
			authoritative_payload_bytes: payload.retained(),
			orphaned_payload_bytes: payload.orphaned(),
			orphaned_payload_bytes_reclaimed: payload.orphaned_removed,
			sum_tree_rebuilds: self.sum_tree_rebuilds.load(Ordering::Acquire),
			viewport_descent_node_visits: self.viewport_descent_node_visits.load(Ordering::Acquire),
			high_priority_io_count: self.high_priority_io_count.load(Ordering::Acquire),
			high_priority_io_total_micros: self
				.high_priority_io_total_micros
				.load(Ordering::Acquire),
			high_priority_io_max_micros: self.high_priority_io_max_micros.load(Ordering::Acquire),
		}
	}

	fn bump_inspector_revision(&self) {
		self.inspector_revision.fetch_add(1, Ordering::Release);
	}

	pub(crate) fn inspector_revision(&self) -> u64 {
		self.inspector_revision.load(Ordering::Acquire)
	}

	pub(crate) fn root_child_line_index_revision(&self) -> u64 {
		self.root_child_line_index_revision.load(Ordering::Acquire)
	}

	pub(crate) fn root_child_line_index_counters(&self) -> (u64, u64) {
		(
			self.root_child_line_index_full_builds
				.load(Ordering::Acquire),
			self.root_child_line_index_splices.load(Ordering::Acquire),
		)
	}

	pub(crate) fn note_root_child_line_index_change(&self, splice_delta: u64) {
		if splice_delta > 0 {
			Self::checked_monotonic_add(
				&self.root_child_line_index_splices,
				splice_delta,
				"root-child index splice counter exhausted",
			);
		}
		Self::checked_monotonic_add(
			&self.root_child_line_index_revision,
			1,
			"root-child index revision exhausted",
		);
		self.bump_inspector_revision();
	}

	/// Returns only state already present in memory. This is intentionally
	/// separate from the read APIs so UAST inspection cannot initiate I/O or
	/// inflate a cold leaf merely by displaying it.
	pub(crate) fn inspector_node_metadata(
		&self,
		node: NodeId,
		snippet_limit: usize,
	) -> InspectorNodeMetadata {
		if !self.is_current_node(node) {
			return InspectorNodeMetadata {
				has_physical_span: false,
				has_full_residency: false,
				has_partial_residency: false,
				resident_bytes: 0,
				snippet: None,
				metric_inflated: false,
			};
		}
		let _resident_read = self.resident_data_lock.read().unwrap();
		let idx = node.index();
		let (has_physical_span, full, partial) = unsafe {
			(
				(*self.spans[idx].get()).is_some(),
				&*self.virtual_data[idx].get(),
				&*self.partial_residency[idx].get(),
			)
		};
		let snippet = if snippet_limit == 0 {
			None
		} else if let Some(bytes) = full.as_deref() {
			Some(bytes[..bytes.len().min(snippet_limit)].to_vec())
		} else {
			partial
				.first()
				.filter(|range| range.offset == 0)
				.map(|range| range.bytes[..range.bytes.len().min(snippet_limit)].to_vec())
		};
		InspectorNodeMetadata {
			has_physical_span,
			has_full_residency: full.is_some(),
			has_partial_residency: !partial.is_empty(),
			resident_bytes: self.resident_accounted_bytes[idx].load(Ordering::Acquire),
			snippet,
			metric_inflated: self.metrics_inflated[idx].load(Ordering::Acquire),
		}
	}

	pub(crate) fn inspector_root_metrics_incomplete(&self, root: NodeId) -> bool {
		self.metric_scans.lock().unwrap().contains_key(&root)
			|| self.incomplete_metric_roots.lock().unwrap().contains(&root)
	}

	pub(crate) fn record_high_priority_start(&self, node: NodeId) {
		self.high_priority_started
			.lock()
			.unwrap()
			.insert(node, Instant::now());
	}

	pub(crate) fn record_high_priority_finish(&self, node: NodeId) {
		let Some(started) = self.high_priority_started.lock().unwrap().remove(&node) else {
			return;
		};
		let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
		self.high_priority_io_count.fetch_add(1, Ordering::Relaxed);
		self.high_priority_io_total_micros
			.fetch_add(micros, Ordering::Relaxed);
		self.high_priority_io_max_micros
			.fetch_max(micros, Ordering::Relaxed);
	}

	pub fn register_device_path(&self, device_id: u16, path: &str) -> io::Result<()> {
		let path = PathBuf::from(path);
		let store = FileBackingStore::open(&path)?;
		let mut backing_stores = self.backing_stores.lock().unwrap();
		if backing_stores.contains_key(&device_id) {
			return Err(io::Error::new(
				io::ErrorKind::AlreadyExists,
				format!("backing device id {device_id} is already registered"),
			));
		}
		backing_stores.insert(device_id, std::sync::Arc::new(store));
		drop(backing_stores);
		self.device_paths.lock().unwrap().insert(device_id, path);
		Ok(())
	}

	pub(crate) fn unregister_device(&self, device_id: u16) {
		self.backing_stores.lock().unwrap().remove(&device_id);
		self.device_paths.lock().unwrap().remove(&device_id);
	}

	#[cfg(test)]
	pub(crate) fn registered_device_count(&self) -> usize {
		self.backing_stores.lock().unwrap().len()
	}

	pub fn register_backing_store(&self, device_id: u16, store: std::sync::Arc<dyn BackingStore>) {
		self.backing_stores.lock().unwrap().insert(device_id, store);
	}

	pub(crate) fn read_backing_exact_at(
		&self,
		device_id: u16,
		offset: u64,
		dst: &mut [u8],
	) -> io::Result<()> {
		let store = self
			.backing_stores
			.lock()
			.unwrap()
			.get(&device_id)
			.cloned()
			.ok_or_else(|| {
				io::Error::new(io::ErrorKind::NotFound, "backing store not registered")
			})?;
		store.read_exact_at(offset, dst)
	}

	pub(crate) fn backing_identity(&self, device_id: u16) -> Option<BackingIdentity> {
		self.backing_stores
			.lock()
			.unwrap()
			.get(&device_id)
			.map(|store| store.identity())
	}

	pub(crate) fn disconnect_node(&self, node: NodeId) {
		let idx = node.index();
		let (parent, next_sibling) = unsafe {
			let e = &*self.edges[idx].get();
			(e.parent, e.next_sibling)
		};
		let orphaned_payload = if parent.is_some() {
			self.authoritative_payload_len(node)
		} else {
			0
		};

		if let Some(p) = parent {
			let p_idx = p.index();
			let prev = self.get_prev_sibling(node);
			if let Some(prev_node) = prev {
				unsafe {
					(*self.edges[prev_node.index()].get()).next_sibling = next_sibling;
				}
			} else {
				unsafe {
					(*self.edges[p_idx].get()).first_child = next_sibling;
				}
			}

			if next_sibling.is_none() {
				unsafe {
					*self.child_tails[p_idx].get() = prev;
				}
			}
		}

		unsafe {
			let e = &mut *self.edges[idx].get();
			e.parent = None;
			e.next_sibling = None;
			if e.first_child.is_none() {
				*self.child_tails[idx].get() = None;
			}
		}
		if parent.is_some() {
			self.retire_orphaned_forest(
				[node],
				1,
				orphaned_payload,
				ReclaimFence::AfterPublication,
			);
		} else {
			self.bump_inspector_revision();
		}
	}

	fn apply_newline_delta(&self, target: NodeId, added_newlines: i64) {
		let mut curr = Some(target);
		while let Some(node) = curr {
			let idx = node.index();
			unsafe {
				let metrics = &mut *self.metrics[idx].get();
				metrics.newlines = metrics
					.newlines
					.checked_add_signed(added_newlines)
					.expect("document newline metric underflow/overflow");
				curr = (*self.edges[idx].get()).parent;
			}
		}
	}

	pub fn begin_metric_scan(&self, root: NodeId, leaves: impl IntoIterator<Item = NodeId>) {
		assert!(
			self.is_current_node(root),
			"metric scan root must be a current UAST node"
		);
		self.incomplete_metric_roots.lock().unwrap().remove(&root);
		let mut unclaimed = VecDeque::new();
		let mut pending = HashSet::new();
		for node_id in leaves {
			let index = self
				.checked_node_index(node_id)
				.expect("metric scan leaf must be a current UAST node");
			let pointer = unsafe { *self.spans[index].get() }
				.expect("initial metric scan range contains only physical extents");
			unclaimed.push_back(MetricScanClaim {
				node_id,
				pointer,
				content_generation: self.content_generation(node_id),
			});
			pending.insert(node_id);
		}
		let leaf_count = u64::try_from(pending.len()).expect("metric scan extent count fits u64");
		if leaf_count == 0 {
			return;
		}
		self.metric_scans.lock().unwrap().insert(
			root,
			MetricScanState {
				unclaimed,
				pending,
				in_flight: 0,
			},
		);
		self.pending_scan_blocks
			.fetch_add(leaf_count, Ordering::Relaxed);
		self.bump_inspector_revision();
	}

	/// Claims the next original ingestion extent for a scan worker. The cursor
	/// lives in scan state rather than in sibling topology, so edit-time splits
	/// and disconnects cannot strand a lane or cause it to scan new hot nodes.
	pub(crate) fn claim_next_metric_scan(&self, root: NodeId) -> Option<MetricScanClaim> {
		let mut scans = self.metric_scans.lock().unwrap();
		let scan = scans.get_mut(&root)?;
		if scan.in_flight >= crate::svp::resolver::METRIC_SCAN_LANES {
			return None;
		}
		while let Some(claim) = scan.unclaimed.pop_front() {
			if scan.pending.contains(&claim.node_id) {
				scan.in_flight += 1;
				return Some(claim);
			}
		}
		None
	}

	pub(crate) fn return_metric_scan_claim(&self, root: NodeId, claim: MetricScanClaim) {
		let mut scans = self.metric_scans.lock().unwrap();
		if let Some(scan) = scans.get_mut(&root) {
			scan.in_flight = scan.in_flight.saturating_sub(1);
			if scan.pending.contains(&claim.node_id) {
				scan.unclaimed.push_front(claim);
			}
		}
	}

	pub(crate) fn finish_metric_scan_io(&self, root: NodeId) {
		if let Some(scan) = self.metric_scans.lock().unwrap().get_mut(&root) {
			scan.in_flight = scan.in_flight.saturating_sub(1);
		}
	}

	pub(crate) fn active_metric_scan_roots(&self) -> Vec<NodeId> {
		let mut roots: Vec<_> = self.metric_scans.lock().unwrap().keys().copied().collect();
		roots.sort_unstable_by_key(|node| node.index());
		roots
	}

	pub(crate) fn cancel_metric_scan(&self, root: NodeId) {
		let mut changed = false;
		if let Some(scan) = self.metric_scans.lock().unwrap().remove(&root) {
			changed = true;
			self.pending_scan_blocks
				.fetch_sub(scan.pending.len() as u64, Ordering::AcqRel);
			for node in scan.pending {
				self.bump_content_generation(node);
			}
		}
		changed |= self.incomplete_metric_roots.lock().unwrap().remove(&root);
		if changed {
			self.bump_inspector_revision();
		}
	}

	fn record_metric_inflation(&self, node: NodeId, scan_root: Option<NodeId>) -> bool {
		let mut scans = self.metric_scans.lock().unwrap();
		let root = scan_root.or_else(|| {
			let mut ancestor = unsafe { (*self.edges[node.index()].get()).parent };
			while let Some(candidate) = ancestor {
				if scans.contains_key(&candidate) {
					return Some(candidate);
				}
				ancestor = unsafe { (*self.edges[candidate.index()].get()).parent };
			}
			None
		});
		let Some(root) = root else {
			return false;
		};
		let Some(scan) = scans.get_mut(&root) else {
			return false;
		};
		if scan.pending.remove(&node) {
			self.pending_scan_blocks.fetch_sub(1, Ordering::Relaxed);
		}
		if scan.pending.is_empty() {
			scans.remove(&root);
			return true;
		}
		false
	}

	pub(crate) fn record_metric_scan_failure(&self, root: NodeId, node: NodeId) {
		let mut scans = self.metric_scans.lock().unwrap();
		let Some(scan) = scans.get_mut(&root) else {
			return;
		};
		if scan.pending.remove(&node) {
			self.incomplete_metric_roots.lock().unwrap().insert(root);
			self.pending_scan_blocks.fetch_sub(1, Ordering::Relaxed);
			self.bump_inspector_revision();
		}
		if scan.pending.is_empty() {
			scans.remove(&root);
		}
	}

	pub(crate) fn terminal_newline(&self, node: NodeId) -> Option<bool> {
		let idx = self.checked_node_index(node)?;
		match self.terminal_newline_state[idx].load(Ordering::Acquire) {
			1 => Some(false),
			2 => Some(true),
			_ => None,
		}
	}

	pub(crate) fn set_terminal_newline(&self, node: NodeId, ends_with_newline: bool) {
		let idx = self
			.checked_node_index(node)
			.expect("terminal-newline write targets a stale UAST node");
		self.terminal_newline_state[idx]
			.store(if ends_with_newline { 2 } else { 1 }, Ordering::Release);
	}

	pub(crate) fn clear_terminal_newline(&self, node: NodeId) {
		if let Some(idx) = self.checked_node_index(node) {
			self.terminal_newline_state[idx].store(0, Ordering::Release);
		}
	}

	pub(crate) fn queue_metric_inflation(
		&self,
		node_id: NodeId,
		newline_count: u32,
		ends_with_newline: bool,
		scan_root: Option<NodeId>,
		content_generation: u64,
	) -> bool {
		let mut pending = self.pending_metric_inflations.lock().unwrap();
		let needs_refresh = pending.is_empty();
		pending.push_back(PendingMetricInflation {
			node_id,
			newline_count,
			ends_with_newline,
			scan_root,
			content_generation,
		});
		needs_refresh
	}

	/// Applies resolver-produced metrics on the engine thread. Keeping every
	/// `SpanMetrics` and topology mutation on that thread is part of the safety
	/// contract behind the registry's `UnsafeCell` storage.
	pub(crate) fn drain_metric_inflations(&self) -> Vec<NodeId> {
		let pending: Vec<_> = self
			.pending_metric_inflations
			.lock()
			.unwrap()
			.drain(..)
			.collect();
		let mut completed_roots = Vec::new();
		for result in pending {
			if !self.is_current_node(result.node_id)
				|| self.content_generation(result.node_id) != result.content_generation
			{
				if let Some(root) = result.scan_root {
					self.record_metric_scan_failure(root, result.node_id);
				}
				continue;
			}
			let completed = if let Some(root) = result.scan_root {
				self.try_complete_metric_scan_block(
					root,
					result.node_id,
					result.newline_count,
					result.ends_with_newline,
				)
			} else {
				self.try_complete_metric_inflation(
					result.node_id,
					result.newline_count,
					result.ends_with_newline,
				)
			};
			if completed {
				let root = result
					.scan_root
					.unwrap_or_else(|| self.document_root_for_node(result.node_id));
				if !self.incomplete_metric_roots.lock().unwrap().contains(&root) {
					completed_roots.push(root);
				}
			}
		}
		completed_roots.sort_unstable_by_key(|node| node.index());
		completed_roots.dedup();
		completed_roots
	}

	/// Returns the exact document line count once every physical leaf has had
	/// its newline metric inflated. `None` deliberately distinguishes an
	/// in-progress scan from a real one-line document.
	pub fn exact_line_count(&self, root: NodeId) -> Option<u64> {
		if self.metric_scans.lock().unwrap().contains_key(&root)
			|| self.incomplete_metric_roots.lock().unwrap().contains(&root)
		{
			None
		} else {
			Some(self.get_total_newlines(root).saturating_add(1))
		}
	}

	#[allow(dead_code)] // Consumed by the bounded line-access layer.
	pub(crate) fn line_metrics_failed(&self, root: NodeId) -> bool {
		self.incomplete_metric_roots.lock().unwrap().contains(&root)
	}

	fn complete_metric_inflation_for_scan(
		&self,
		node: NodeId,
		newline_count: u32,
		ends_with_newline: bool,
		scan_root: Option<NodeId>,
	) -> bool {
		let idx = node.index();
		let existing = unsafe { (*self.metrics[idx].get()).newlines };
		let delta =
			i64::from(newline_count) - i64::try_from(existing).expect("newline metric fits i64");
		if delta != 0 {
			self.apply_newline_delta(node, delta);
		}
		self.metrics_inflated[idx].store(true, Ordering::Release);
		self.set_terminal_newline(node, ends_with_newline);
		self.metric_inflating[idx].store(false, Ordering::Release);
		self.update_root_child_line_index_metric(node, u64::from(newline_count), ends_with_newline);
		let completed = self.record_metric_inflation(node, scan_root);
		self.bump_inspector_revision();
		completed
	}

	pub fn complete_metric_inflation(
		&self,
		node: NodeId,
		newline_count: u32,
		ends_with_newline: bool,
	) -> bool {
		self.complete_metric_inflation_for_scan(node, newline_count, ends_with_newline, None)
	}

	fn try_complete_metric_inflation_for_scan(
		&self,
		node: NodeId,
		newline_count: u32,
		ends_with_newline: bool,
		scan_root: Option<NodeId>,
	) -> bool {
		let idx = node.index();
		if self.metrics_inflated[idx].load(Ordering::Acquire) {
			return scan_root.is_some_and(|root| self.record_metric_inflation(node, Some(root)));
		}
		if self.metric_inflating[idx]
			.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
			.is_ok()
		{
			return self.complete_metric_inflation_for_scan(
				node,
				newline_count,
				ends_with_newline,
				scan_root,
			);
		}
		false
	}

	pub fn try_complete_metric_inflation(
		&self,
		node: NodeId,
		newline_count: u32,
		ends_with_newline: bool,
	) -> bool {
		self.try_complete_metric_inflation_for_scan(node, newline_count, ends_with_newline, None)
	}

	pub(crate) fn try_complete_metric_scan_block(
		&self,
		root: NodeId,
		node: NodeId,
		newline_count: u32,
		ends_with_newline: bool,
	) -> bool {
		self.try_complete_metric_inflation_for_scan(
			node,
			newline_count,
			ends_with_newline,
			Some(root),
		)
	}

	pub fn read_node_bytes_sync(&self, node: NodeId) -> io::Result<Vec<u8>> {
		let _resident_read = self.resident_data_lock.read().unwrap();
		let idx = self
			.checked_node_index(node)
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "stale UAST node identity"))?;
		unsafe {
			if let Some(v_data) = &*self.virtual_data[idx].get() {
				return Ok(v_data.clone());
			}
		}

		let span = unsafe { *self.spans[idx].get() }
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "node has no readable span"))?;
		let mut bytes = vec![0u8; span.byte_length as usize];
		self.read_backing_exact_at(
			span.device_id,
			span.lba * 512 + u64::from(span.head_trim),
			&mut bytes,
		)?;
		Ok(bytes)
	}

	pub fn ensure_metrics_inflated(&self, node: NodeId) {
		let Some(idx) = self.checked_node_index(node) else {
			return;
		};
		loop {
			if self.metrics_inflated[idx].load(Ordering::Acquire) {
				return;
			}
			if self.metric_inflating[idx]
				.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
			{
				break;
			}
			std::thread::yield_now();
		}

		match self.read_node_bytes_sync(node) {
			Ok(bytes) => {
				let newline_count = memchr_iter(b'\n', &bytes).count() as u32;
				let _ = self.complete_metric_inflation(
					node,
					newline_count,
					bytes.last() == Some(&b'\n'),
				);
			}
			Err(_) => {
				self.metric_inflating[idx].store(false, Ordering::Release);
			}
		}
	}

	/// Resolves either physical SvpPointer or virtual_data into raw bytes.
	pub fn resolve_physical_bytes(&self, node: NodeId) -> Vec<u8> {
		let _resident_read = self.resident_data_lock.read().unwrap();
		let Some(idx) = self.checked_node_index(node) else {
			return Vec::new();
		};
		unsafe {
			if let Some(v_data) = &*self.virtual_data[idx].get() {
				return v_data.clone();
			}
			// Physical nodes return empty buffers until the resolver hot-swaps them.
		}
		Vec::new()
	}

	pub fn get_first_child(&self, node: NodeId) -> Option<NodeId> {
		let idx = self.checked_node_index(node)?;
		unsafe { (*self.edges[idx].get()).first_child }.filter(|child| self.is_current_node(*child))
	}

	pub fn get_total_newlines(&self, node: NodeId) -> u64 {
		self.checked_node_index(node)
			.map_or(0, |idx| unsafe { (*self.metrics[idx].get()).newlines })
	}

	pub fn get_total_bytes(&self, node: NodeId) -> u64 {
		self.checked_node_index(node)
			.map_or(0, |idx| unsafe { (*self.metrics[idx].get()).byte_length })
	}

	pub fn get_prev_sibling(&self, node: NodeId) -> Option<NodeId> {
		let idx = self.checked_node_index(node)?;
		let parent = unsafe { (*self.edges[idx].get()).parent }
			.filter(|parent| self.is_current_node(*parent))?;
		let parent_idx = self.checked_node_index(parent)?;
		let first_child = unsafe { (*self.edges[parent_idx].get()).first_child }
			.filter(|child| self.is_current_node(*child))?;
		if first_child == node {
			return None;
		}
		let mut curr = first_child;
		loop {
			let curr_idx = self.checked_node_index(curr)?;
			let next = unsafe { (*self.edges[curr_idx].get()).next_sibling }
				.filter(|candidate| self.is_current_node(*candidate));
			if next == Some(node) {
				return Some(curr);
			}
			curr = next?;
		}
	}

	pub fn get_next_sibling(&self, node: NodeId) -> Option<NodeId> {
		let idx = self.checked_node_index(node)?;
		unsafe { (*self.edges[idx].get()).next_sibling }
			.filter(|sibling| self.is_current_node(*sibling))
	}

	pub(crate) fn document_root_for_node(&self, node: NodeId) -> NodeId {
		if !self.is_current_node(node) {
			return node;
		}
		let mut root = node;
		while let Some(parent) = self
			.checked_node_index(root)
			.and_then(|idx| unsafe { (*self.edges[idx].get()).parent })
			.filter(|parent| self.is_current_node(*parent))
		{
			root = parent;
		}
		root
	}
}

#[cfg(test)]
mod tests {
	use super::{ReclaimFence, ReclaimedPayload, UastRegistry};
	use crate::svp::pointer::SvpPointer;
	use crate::uast::TreeEdges;
	use crate::uast::kind::{ProviderKindId, SemanticKind};
	use crate::uast::metrics::SpanMetrics;
	use std::sync::Arc;
	use std::sync::atomic::Ordering;
	use std::time::{SystemTime, UNIX_EPOCH};

	fn temp_backing_path(label: &str) -> std::path::PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock")
			.as_nanos();
		std::env::temp_dir().join(format!(
			"baryon-registry-{label}-{}-{nonce}",
			std::process::id()
		))
	}

	#[test]
	fn failed_device_registration_does_not_install_a_backing() {
		let registry = UastRegistry::new(1);
		let path = temp_backing_path("missing");
		let _ = std::fs::remove_file(&path);

		let error = registry
			.register_device_path(17, path.to_str().expect("utf8 path"))
			.expect_err("missing backing must fail registration");

		assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
		assert!(registry.backing_identity(17).is_none());
		assert!(!registry.device_paths.lock().unwrap().contains_key(&17));
	}

	#[test]
	fn duplicate_device_registration_cannot_redirect_an_existing_backing() {
		let registry = UastRegistry::new(1);
		let first_path = temp_backing_path("first");
		let second_path = temp_backing_path("second");
		std::fs::write(&first_path, b"AAAA").expect("write first backing");
		std::fs::write(&second_path, b"BBBB").expect("write second backing");

		registry
			.register_device_path(23, first_path.to_str().expect("utf8 first path"))
			.expect("register first backing");
		let error = registry
			.register_device_path(23, second_path.to_str().expect("utf8 second path"))
			.expect_err("duplicate device id must be rejected");

		assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
		let mut bytes = [0; 4];
		registry
			.read_backing_exact_at(23, 0, &mut bytes)
			.expect("read original backing");
		assert_eq!(&bytes, b"AAAA");
		assert_eq!(
			registry.device_paths.lock().unwrap().get(&23),
			Some(&first_path)
		);
		registry.unregister_device(23);
		assert!(registry.backing_identity(23).is_none());
		assert!(!registry.device_paths.lock().unwrap().contains_key(&23));

		let _ = std::fs::remove_file(first_path);
		let _ = std::fs::remove_file(second_path);
	}

	#[test]
	fn exact_line_count_is_hidden_until_all_leaf_metrics_are_known() {
		let registry = UastRegistry::new(3);
		let mut chunk = registry.reserve_chunk(3).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 12,
				newlines: 0,
			},
		);
		let first = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 6,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 6,
				newlines: 0,
			},
		);
		let second = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 6,
				device_id: 1,
				head_trim: 6,
			}),
			SpanMetrics {
				byte_length: 6,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, first);
		chunk.append_local_child(root, second);
		registry.begin_metric_scan(root, [first, second]);

		assert_eq!(registry.exact_line_count(root), None);
		assert!(!registry.try_complete_metric_inflation(first, 2, true));
		assert_eq!(registry.exact_line_count(root), None);
		assert!(registry.try_complete_metric_inflation(second, 1, false));
		assert_eq!(registry.exact_line_count(root), Some(4));

		assert!(!registry.try_complete_metric_inflation(second, 99, false));
		assert_eq!(registry.exact_line_count(root), Some(4));
	}

	#[test]
	fn metric_scan_uses_explicit_leaf_identity_order_not_slot_adjacency() {
		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(4).expect("registry chunk");
		let root = chunk.spawn_node(SemanticKind::RelationalTable, None, SpanMetrics::default());
		let leaves = (0..3)
			.map(|offset| {
				chunk.spawn_node(
					SemanticKind::Token,
					Some(SvpPointer {
						lba: offset,
						byte_length: 1,
						device_id: 1,
						head_trim: 0,
					}),
					SpanMetrics {
						byte_length: 1,
						newlines: 0,
					},
				)
			})
			.collect::<Vec<_>>();
		let explicit = [leaves[2], leaves[0], leaves[1]];
		registry.begin_metric_scan(root, explicit);
		for expected in explicit {
			assert_eq!(
				registry
					.claim_next_metric_scan(root)
					.map(|claim| claim.node_id),
				Some(expected)
			);
			registry.finish_metric_scan_io(root);
		}
	}

	#[test]
	fn document_metrics_preserve_values_beyond_u32() {
		let bytes = u64::from(u32::MAX) + 8 * 1024 * 1024;
		let newlines = u64::from(u32::MAX) + 123;
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: bytes,
				newlines,
			},
		);

		assert_eq!(registry.get_total_bytes(root), bytes);
		assert_eq!(registry.get_total_newlines(root), newlines);
	}

	#[test]
	fn failed_reservation_is_recoverable_and_does_not_consume_ids() {
		let registry = UastRegistry::new(1);
		assert!(registry.reserve_chunk(2).is_none());
		assert_eq!(registry.stats().allocated_nodes, 0);
		assert!(registry.reserve_chunk(1).is_some());
		assert_eq!(registry.stats().allocated_nodes, 1);
	}

	#[test]
	fn untouched_slots_begin_at_the_semantic_and_provider_zero_state() {
		let registry = UastRegistry::new(1);
		assert!(!registry.slot_published[0].load(Ordering::Acquire));
		assert_eq!(unsafe { *registry.kinds[0].get() }, SemanticKind::None);
		assert_eq!(
			unsafe { *registry.provider_kinds[0].get() },
			ProviderKindId::NONE
		);
	}

	#[test]
	fn unused_exact_reservation_slots_return_without_consuming_a_lifetime() {
		let registry = UastRegistry::new(2);
		let mut reservation = registry.reserve_chunk(2).expect("two slots");
		let first = reservation.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		drop(reservation);
		let after_drop = registry.stats();
		assert_eq!(
			after_drop.allocated_nodes, 2,
			"tail high-water was committed"
		);
		assert_eq!(after_drop.allocation_events, 1);
		assert_eq!(after_drop.reusable_slots, 1);

		let mut returned = registry
			.reserve_chunk(1)
			.expect("unpublished ticket returned");
		let second = returned.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		assert_ne!(first.index(), second.index());
		assert_eq!(
			second.generation(),
			0,
			"an unpublished generation is not consumed"
		);
		assert_eq!(registry.stats().allocation_events, 2);
	}

	#[test]
	fn recycled_slot_resets_every_object_class_but_keeps_monotonic_generations() {
		let registry = UastRegistry::new(1);
		let mut reservation = registry.reserve_chunk(1).expect("initial slot");
		let old = reservation.spawn_node(SemanticKind::RelationalRow, None, SpanMetrics::default());
		drop(reservation);
		let old_content_generation = registry.content_generation(old);
		unsafe {
			*registry.provider_kinds[old.index()].get() =
				crate::uast::kind::ProviderKindId::new(17);
			*registry.edges[old.index()].get() = TreeEdges {
				parent: None,
				first_child: None,
				next_sibling: Some(old),
			};
			*registry.child_tails[old.index()].get() = Some(old);
		}
		registry.metrics_inflated[old.index()].store(true, Ordering::Release);
		registry.metric_inflating[old.index()].store(true, Ordering::Release);
		registry.set_terminal_newline(old, true);
		registry.begin_whole_leaf_read(old, old_content_generation);
		registry.set_viewport_resident_pins(9, [old]);
		registry.record_high_priority_start(old);

		registry.retire_orphaned_forest([old], 1, 0, ReclaimFence::NeverPublished);
		registry.reclaim_orphaned_node(old);
		assert_eq!(
			unsafe { *registry.kinds[old.index()].get() },
			SemanticKind::None,
			"bounded reclaim must remove the old semantic claim"
		);
		assert_eq!(
			unsafe { *registry.provider_kinds[old.index()].get() },
			ProviderKindId::NONE,
			"bounded reclaim must remove the old provider identity"
		);
		let mut next = registry.reserve_chunk(1).expect("reclaimed slot");
		let new = next.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 42,
				newlines: 6,
			},
		);
		assert_eq!(new.index(), old.index());
		assert_eq!(new.generation(), old.generation() + 1);
		assert!(registry.content_generation(new) > old_content_generation);
		assert_eq!(
			unsafe { *registry.kinds[new.index()].get() },
			SemanticKind::RelationalTable,
			"publication must overwrite the inert semantic zero-state"
		);
		assert_eq!(
			unsafe { *registry.provider_kinds[new.index()].get() },
			crate::uast::kind::ProviderKindId::NONE
		);
		assert_eq!(
			unsafe { *registry.edges[new.index()].get() },
			TreeEdges::default()
		);
		assert_eq!(unsafe { *registry.child_tails[new.index()].get() }, None);
		assert!(!registry.metrics_inflated[new.index()].load(Ordering::Acquire));
		assert!(!registry.metric_inflating[new.index()].load(Ordering::Acquire));
		assert_eq!(registry.terminal_newline(new), None);
		assert!(
			!registry.content_generation_exhausted[new.index()].load(Ordering::Acquire),
			"a reusable slot cannot carry content-generation exhaustion"
		);
		assert_eq!(
			registry.whole_leaf_read_outcome(old, old_content_generation),
			None
		);
		assert_eq!(registry.get_total_bytes(new), 42);
		assert_eq!(registry.get_total_newlines(new), 6);
	}

	#[test]
	fn exhausted_content_generation_fails_closed_and_burns_the_slot() {
		let registry = Arc::new(UastRegistry::new(1));
		let mut reservation = registry.reserve_chunk(1).expect("initial slot");
		let node = reservation.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		drop(reservation);
		registry.content_generation[node.index()].store(u64::MAX, Ordering::Release);
		assert_eq!(registry.bump_content_generation(node), u64::MAX);
		assert!(
			registry.acquire_io_lifetime(node).is_none(),
			"content-generation exhaustion must close resolver admission"
		);
		assert!(
			!registry.publish_resolved_data(node, u64::MAX, b"late".to_vec()),
			"an old max-generation completion must fail closed"
		);
		registry.retire_orphaned_forest([node], 1, 0, ReclaimFence::NeverPublished);
		registry.reclaim_orphaned_node(node);
		assert_eq!(registry.stats().burned_slots, 1);
		assert_eq!(registry.stats().reusable_slots, 0);
		assert!(registry.reserve_chunk(1).is_none());
	}

	#[test]
	fn disconnect_records_one_retirement_only_for_an_attached_node() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(SemanticKind::RelationalTable, None, SpanMetrics::default());
		let leaf = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		chunk.append_local_child(root, leaf);
		registry.replace_authoritative_data(leaf, b"sole".to_vec());

		registry.disconnect_node(leaf);
		assert_eq!(registry.stats().retired_nodes, 1);
		assert_eq!(registry.stats().orphaned_payload_bytes, 4);
		registry.disconnect_node(leaf);
		assert_eq!(
			registry.stats().retired_nodes,
			1,
			"an already-detached node must not retire twice"
		);
		assert_eq!(
			registry.stats().orphaned_payload_bytes,
			4,
			"an already-detached payload must not orphan twice"
		);
	}

	#[test]
	fn replacing_virtual_document_data_reuses_its_nodes() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(SemanticKind::RelationalTable, None, SpanMetrics::default());
		let leaf = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		chunk.append_local_child(root, leaf);

		for payload in [b"one\n".as_slice(), b"two\nthree\n".as_slice()] {
			registry.replace_virtual_document_data(root, leaf, payload.to_vec());
		}

		assert_eq!(registry.stats().allocated_nodes, 2);
		assert_eq!(registry.get_total_bytes(root), 10);
		assert_eq!(registry.get_total_newlines(root), 2);
		assert_eq!(
			unsafe { (&*registry.virtual_data[leaf.index()].get()).as_deref() },
			Some(&b"two\nthree\n"[..])
		);
	}

	#[test]
	fn authoritative_payload_counters_follow_replace_and_in_place_mutation() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let leaf = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());

		registry.replace_authoritative_data(leaf, b"abc".to_vec());
		registry.mutate_authoritative_data(leaf, |bytes| bytes.extend_from_slice(b"de"));
		registry.mutate_authoritative_data(leaf, |bytes| bytes.truncate(1));

		let payload = registry.authoritative_payload_snapshot();
		assert_eq!(payload.added, 5);
		assert_eq!(payload.removed, 4);
		assert_eq!(payload.retained(), 1);
		assert_eq!(payload.orphaned(), 0);
		assert_eq!(registry.authoritative_payload_len(leaf), 1);
	}

	#[test]
	fn hydration_is_neutral_and_promotion_rebind_is_counted_once() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let span = SvpPointer {
			lba: 0,
			byte_length: 4,
			device_id: 1,
			head_trim: 0,
		};
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(span),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let before = registry.authoritative_payload_snapshot();
		registry.publish_hydrated_data(leaf, b"data".to_vec());
		assert!(registry.publish_partial_resident_range(
			leaf,
			registry.content_generation(leaf),
			1,
			b"at".to_vec(),
		));
		assert_eq!(registry.authoritative_payload_snapshot(), before);

		registry.promote_hydrated_data(leaf);
		registry.mutate_authoritative_data(leaf, |bytes| bytes.extend_from_slice(b"++"));
		let promoted = registry.authoritative_payload_snapshot();
		assert_eq!(promoted.added, 6);
		assert_eq!(promoted.removed, 0);
		assert_eq!(promoted.retained(), 6);

		registry.rebind_data_to_saved_span(
			leaf,
			Some(SvpPointer {
				byte_length: 6,
				..span
			}),
		);
		let rebound = registry.authoritative_payload_snapshot();
		assert_eq!(rebound.added, 6);
		assert_eq!(rebound.removed, 6);
		assert_eq!(rebound.retained(), 0);
		assert_eq!(registry.stats().resident_bytes, 0);
	}

	#[test]
	fn rebinding_an_empty_leaf_keeps_its_writable_authoritative_sentinel() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let leaf = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		registry.replace_authoritative_data(leaf, Vec::new());
		registry.rebind_data_to_saved_span(leaf, None);
		registry.mutate_authoritative_data(leaf, |bytes| bytes.extend_from_slice(b"x"));
		assert_eq!(registry.authoritative_payload_len(leaf), 1);
		assert_eq!(registry.stats().authoritative_payload_bytes, 1);
	}

	#[test]
	#[should_panic(expected = "authoritative payload must not be dropped")]
	fn authoritative_payload_cannot_use_residency_eviction() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let leaf = chunk.spawn_node(SemanticKind::Token, None, SpanMetrics::default());
		registry.replace_authoritative_data(leaf, b"data".to_vec());
		registry.drop_resident_data(leaf);
	}

	#[test]
	fn allocator_eligible_spanless_nodes_read_as_inert() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		registry.replace_authoritative_data(leaf, b"data".to_vec());
		registry.retire_orphaned_forest([leaf], 1, 4, ReclaimFence::NeverPublished);

		assert_eq!(
			registry.reclaim_orphaned_node(leaf),
			ReclaimedPayload {
				authoritative_bytes: 4,
				hydrated_bytes: 0,
			}
		);
		assert_eq!(
			registry.reclaim_orphaned_node(leaf),
			ReclaimedPayload::default()
		);
		assert_eq!(registry.get_total_bytes(leaf), 0);
		assert_eq!(registry.get_first_child(leaf), None);
		assert!(registry.resolve_physical_bytes(leaf).is_empty());
		assert_eq!(
			registry
				.read_node_bytes_sync(leaf)
				.expect_err("reclaimed authoritative payload is absent")
				.kind(),
			std::io::ErrorKind::NotFound
		);
		let metadata = registry.inspector_node_metadata(leaf, 0);
		assert!(!metadata.has_physical_span);
		assert_eq!(metadata.resident_bytes, 0);
		assert_eq!(registry.stats().orphaned_payload_bytes, 0);
		assert_eq!(registry.stats().orphaned_payload_bytes_reclaimed, 4);
	}

	#[test]
	fn reclaim_generation_rejects_late_whole_and_partial_hydration() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let admitted_generation = registry.content_generation(leaf);
		registry.publish_hydrated_data(leaf, b"old!".to_vec());
		registry.retire_orphaned_forest([leaf], 1, 0, ReclaimFence::NeverPublished);

		assert_eq!(
			registry.reclaim_orphaned_node(leaf),
			ReclaimedPayload {
				authoritative_bytes: 0,
				hydrated_bytes: 4,
			}
		);
		assert!(!registry.publish_resolved_data(leaf, admitted_generation, b"late".to_vec()));
		assert!(!registry.publish_partial_resident_range(
			leaf,
			admitted_generation,
			0,
			b"late".to_vec(),
		));
		assert!(!registry.whole_leaf_is_resident(leaf));
		assert_eq!(registry.stats().resident_bytes, 0);
	}

	#[test]
	fn production_payload_writes_use_the_intent_api() {
		let sources = [
			("mutation", include_str!("../uast/mutation.rs")),
			("graft", include_str!("../engine/structural_graft.rs")),
			("folding", include_str!("../engine/folding/synthetic.rs")),
			("semantic", include_str!("../engine/semantic.rs")),
			("session", include_str!("../engine/session.rs")),
			("file-browser", include_str!("../engine/file_browser.rs")),
			("file-browser", include_str!("../engine/file_browser.rs")),
			("runtime", include_str!("../engine/runtime.rs")),
			("runtime-edit", include_str!("../engine/runtime/edit.rs")),
			("system", include_str!("../engine/runtime/system.rs")),
			("document-io", include_str!("../engine/document/io.rs")),
			("resolver", include_str!("../svp/resolver.rs")),
			("ingest", include_str!("../svp/ingest.rs")),
			("python", include_str!("../svp/python_ast.rs")),
			("rune", include_str!("../svp/rune_ast.rs")),
		];
		for (label, source) in sources {
			assert!(
				!source.contains("hot_swap_virtual_data"),
				"{label} reintroduced the ambiguous payload writer"
			);
			let production = source.split("\nmod tests {").next().unwrap_or(source);
			for (line_number, line) in production.lines().enumerate() {
				let trimmed = line.trim_start();
				let direct_assignment = (trimmed.starts_with("*registry.virtual_data")
					|| trimmed.starts_with("*self.virtual_data"))
					&& trimmed.contains('=');
				let raw_mutation = line.contains("&mut *registry.virtual_data")
					|| line.contains("&mut *self.virtual_data")
					|| (line.contains("virtual_data") && line.contains(".take()"));
				assert!(
					!direct_assignment && !raw_mutation,
					"{label}:{} bypasses authoritative payload accounting: {trimmed}",
					line_number + 1
				);
				assert!(
					!line.contains("note_nodes_retired") && !line.contains("note_orphaned_payload"),
					"{label}:{} bypasses typed retired-forest transport: {trimmed}",
					line_number + 1
				);
			}
		}
		assert!(
			include_str!("../svp/python_ast.rs")
				.contains("#[cfg(test)]\npub fn apply_python_semantic_update"),
			"obsolete Python semantic application must remain test-only"
		);
		assert!(
			include_str!("../svp/rune_ast.rs")
				.contains("#[cfg(test)]\npub fn apply_rune_semantic_update"),
			"obsolete Rune semantic application must remain test-only"
		);
		assert!(
			include_str!("../engine/structural_graft.rs")
				.contains("#[cfg(test)]\npub fn apply_graft"),
			"production grafts must return exact retired forest heads"
		);
	}

	#[test]
	fn production_node_allocations_have_no_panicking_capacity_path() {
		let sources = [
			("registry", include_str!("registry.rs")),
			("mutation", include_str!("../uast/mutation.rs")),
			("graft", include_str!("../engine/structural_graft.rs")),
			("folding", include_str!("../engine/folding/synthetic.rs")),
			("session", include_str!("../engine/session.rs")),
			("system", include_str!("../engine/runtime/system.rs")),
			("ingest", include_str!("../svp/ingest.rs")),
		];
		for (label, source) in sources {
			let production = source
				.split("\n#[cfg(test)]\nmod tests {")
				.next()
				.unwrap_or(source);
			assert!(
				!production.contains("alloc_node_internal"),
				"{label} reintroduced the panicking per-node allocator"
			);
			let lines = production.lines().collect::<Vec<_>>();
			for (line_number, line) in lines.iter().enumerate() {
				if !line.contains("reserve_chunk") && !line.contains("reserve_ids") {
					continue;
				}
				let window = lines[line_number..lines.len().min(line_number + 4)].join("\n");
				assert!(
					!window.contains(".expect(") && !window.contains(".unwrap("),
					"{label}:{} unwraps a registry capacity reservation:\n{window}",
					line_number + 1
				);
			}
		}
	}

	#[test]
	fn production_identity_holders_preserve_generation_and_io_lifetime_guards() {
		let resolver = include_str!("../svp/resolver.rs")
			.split("\n#[cfg(test)]\nmod tests {")
			.next()
			.expect("resolver production section");
		assert!(
			resolver.contains("lifetime_lease: IoLifetimeLease"),
			"resolver requests must own a slot-lifetime lease"
		);
		assert!(
			resolver.contains("_lifetime_lease: IoLifetimeLease"),
			"resolver completions must retain the lease through final cleanup"
		);
		assert!(
			!resolver.contains(".push(request).is_err()")
				&& resolver.contains("if let Err(request)")
				&& resolver.contains("drop(request);"),
			"queue refusal must retain the request lease through every final cleanup write"
		);
		for raw_column in [".spans[", ".dma_in_flight[", ".content_generation["] {
			assert!(
				!resolver.contains(raw_column),
				"resolver must use generation-checked registry entry points, not {raw_column}"
			);
		}
		let lines = resolver.lines().collect::<Vec<_>>();
		for (line_number, line) in lines.iter().enumerate() {
			if !line.contains("SvpRequest {") || line.contains("pub struct") {
				continue;
			}
			let window = lines[line_number..lines.len().min(line_number + 16)].join("\n");
			assert!(
				window.contains("lifetime_lease"),
				"resolver:{} constructs an unleased request:\n{window}",
				line_number + 1
			);
		}

		let folds = include_str!("../engine/folding/set.rs")
			.split("\n#[cfg(test)]")
			.next()
			.unwrap_or_default();
		assert!(
			folds.contains("binary_search(&node)") && !folds.contains("binary_search_by_key"),
			"FoldSet membership must compare full packed NodeId identity"
		);
		assert!(
			folds.contains("is_current_node(candidate)"),
			"fold ancestry must reject stale identities before dereference"
		);

		let inspector = include_str!("../engine/uast_inspector.rs")
			.split("\n#[cfg(test)]")
			.next()
			.unwrap_or_default();
		assert!(
			inspector.contains("id: node.node_id.raw()")
				&& inspector.contains("root_id: stamp.root.raw()")
				&& inspector.contains("and_then(NodeId::from_raw)"),
			"graph transport must preserve packed generations end to end"
		);

		for (label, source) in [
			("registry", include_str!("registry.rs")),
			("resolver", include_str!("../svp/resolver.rs")),
			("ingest", include_str!("../svp/ingest.rs")),
			("inspector", include_str!("../engine/uast_inspector.rs")),
		] {
			let production = source.split("\n#[cfg(test)]").next().unwrap_or(source);
			assert!(
				!production.contains("NodeId::from_index"),
				"{label} reconstructs an unqualified generation-zero identity"
			);
		}
	}

	#[test]
	fn resident_budget_demotes_old_clean_physical_data() {
		let registry = UastRegistry::new(3);
		let mut chunk = registry.reserve_chunk(3).expect("registry chunk");
		let mut leaves = Vec::new();
		for lba in 0..3 {
			leaves.push(chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba,
					byte_length: 4,
					device_id: 1,
					head_trim: 0,
				}),
				SpanMetrics {
					byte_length: 4,
					newlines: 0,
				},
			));
		}
		registry.set_resident_budget(8, 4);
		for &leaf in &leaves {
			registry.publish_hydrated_data(leaf, vec![leaf.index() as u8; 4]);
		}
		// Eviction is engine-thread work; the resolver only publishes bytes.
		registry.enforce_resident_budget(None);

		assert!(registry.stats().resident_bytes <= 8);
		assert!(unsafe { (&*registry.virtual_data[leaves[0].index()].get()).is_none() });
		assert!(unsafe { (&*registry.virtual_data[leaves[2].index()].get()).is_some() });

		registry.publish_hydrated_data(leaves[0], vec![9; 4]);
		assert_eq!(
			unsafe { (&*registry.virtual_data[leaves[0].index()].get()).as_deref() },
			Some(&[9, 9, 9, 9][..])
		);
		assert!(registry.stats().resident_bytes <= 8);
	}

	#[test]
	fn resident_lru_deduplicates_repeated_publication_and_prunes_evictions() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let first = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let second = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 1,
				byte_length: 4,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);

		for generation in 0..1_000u16 {
			registry.publish_hydrated_data(first, vec![generation as u8; 4]);
		}
		assert_eq!(registry.resident_lru_len(), 1);

		registry.set_resident_budget(4, 4);
		registry.publish_hydrated_data(second, vec![2; 4]);
		registry.enforce_resident_budget(None);
		assert_eq!(registry.resident_lru_len(), 1);
		assert!(unsafe { (&*registry.virtual_data[first.index()].get()).is_none() });
	}

	#[test]
	fn strided_residency_churn_keeps_bytes_and_lru_metadata_bounded() {
		const EXTENTS: u32 = 128;
		const EXTENT_BYTES: u32 = 1024;
		const RESIDENT_EXTENTS: usize = 8;
		let registry = UastRegistry::new(EXTENTS);
		let mut chunk = registry.reserve_chunk(EXTENTS).expect("registry chunk");
		let leaves: Vec<_> = (0..EXTENTS)
			.map(|extent| {
				chunk.spawn_node(
					SemanticKind::Token,
					Some(SvpPointer {
						lba: u64::from(extent) * u64::from(EXTENT_BYTES / 512),
						byte_length: EXTENT_BYTES,
						device_id: 1,
						head_trim: 0,
					}),
					SpanMetrics {
						byte_length: u64::from(EXTENT_BYTES),
						newlines: 0,
					},
				)
			})
			.collect();
		let budget = EXTENT_BYTES as u64 * RESIDENT_EXTENTS as u64;
		registry.set_resident_budget(budget, budget);

		for &leaf in &leaves {
			registry.publish_hydrated_data(leaf, vec![0; EXTENT_BYTES as usize]);
			registry.enforce_resident_budget(None);
			assert!(registry.stats().resident_bytes <= budget);
			assert!(registry.resident_lru_len() <= RESIDENT_EXTENTS);
		}
	}

	#[test]
	fn partial_residency_tracks_coverage_without_marking_extent_fully_loaded() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 32,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 32,
				newlines: 0,
			},
		);
		registry.store_partial_resident_range(leaf, 8, vec![1, 2, 3, 4]);

		assert_eq!(
			registry.read_partial_resident_range(leaf, 9, 2),
			Some(vec![2, 3])
		);
		assert_eq!(registry.read_partial_resident_range(leaf, 7, 2), None);
		assert!(unsafe { (&*registry.virtual_data[leaf.index()].get()).is_none() });
		assert_eq!(registry.stats().resident_bytes, 4);
	}

	#[test]
	fn visible_resident_data_is_pinned_until_the_viewport_moves() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let make_span = |lba| SvpPointer {
			lba,
			byte_length: 4,
			device_id: 1,
			head_trim: 0,
		};
		let first = chunk.spawn_node(
			SemanticKind::Token,
			Some(make_span(0)),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let second = chunk.spawn_node(
			SemanticKind::Token,
			Some(make_span(1)),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		registry.set_resident_budget(4, 0);
		registry.set_viewport_resident_pins(7, [first]);
		registry.publish_hydrated_data(first, vec![1; 4]);
		registry.publish_hydrated_data(second, vec![2; 4]);
		assert!(unsafe { (&*registry.virtual_data[first.index()].get()).is_some() });

		registry.set_viewport_resident_pins(7, []);
		registry.enforce_resident_budget(None);
		assert!(registry.stats().resident_bytes <= 4);
	}
}
