use crate::ecs::chunk::RegistryChunk;
use crate::ecs::id::NodeId;
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
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegistryStatsSnapshot {
	pub allocated_nodes: u64,
	pub cold_extents: u64,
	pub hot_extents: u64,
	pub scanned_bytes: u64,
	pub pending_scan_blocks: u64,
	pub resident_bytes: u64,
	pub sum_tree_rebuilds: u64,
	/// UAST nodes examined while viewport queries seek their first visible leaf.
	pub viewport_descent_node_visits: u64,
	pub high_priority_io_count: u64,
	pub high_priority_io_total_micros: u64,
	pub high_priority_io_max_micros: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WholeLeafReadOutcome {
	InFlight,
	Failed,
}

/// ==========================================
/// THE CONCURRENT ECS REGISTRY (WORLD)
/// ==========================================
/// Structure of Arrays (SoA) layout.
pub struct UastRegistry {
	pub capacity: u32,
	pub next_id: AtomicU32,

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
	pub(crate) cold_extent_count: AtomicU64,
	pub(crate) hot_extent_count: AtomicU64,
	pub(crate) scanned_bytes: AtomicU64,
	pub(crate) pending_scan_blocks: AtomicU64,
	pub(crate) resident_bytes: AtomicU64,
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
	/// Pre-allocates the registry. 10GB dataset -> ~200M capacity.
	pub fn new(capacity: u32) -> Self {
		let cap = capacity as usize;
		Self {
			capacity,
			next_id: AtomicU32::new(1), // Start at 1 to allow NonZeroU32 for NodeId
			kinds: PagedArray::new(cap, || UnsafeCell::new(SemanticKind::Token)),
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
			terminal_newline_state: PagedArray::new(cap, || AtomicU8::new(0)),
			metric_scans: Mutex::new(HashMap::new()),
			incomplete_metric_roots: Mutex::new(HashSet::new()),
			pending_metric_inflations: Mutex::new(VecDeque::new()),
			whole_leaf_read_outcomes: Mutex::new(HashMap::new()),
			root_child_line_indexes: RwLock::new(HashMap::new()),
			root_child_line_index_full_builds: AtomicU64::new(0),
			cold_extent_count: AtomicU64::new(0),
			hot_extent_count: AtomicU64::new(0),
			scanned_bytes: AtomicU64::new(0),
			pending_scan_blocks: AtomicU64::new(0),
			resident_bytes: AtomicU64::new(0),
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

	/// Internal node allocator for mutations.
	pub(crate) fn alloc_node_internal(&self) -> NodeId {
		self.try_alloc_node_internal()
			.expect("UastRegistry capacity exceeded during split")
	}

	pub(crate) fn try_alloc_node_internal(&self) -> Option<NodeId> {
		let id_val = self.reserve_ids(1)?;
		Some(NodeId::from_index(id_val as usize - 1))
	}

	/// Atomically reserves a chunk of `NodeId`s for a single thread.
	/// Returns a `RegistryChunk` which grants lock-free write access to those specific indices.
	pub fn reserve_chunk(&self, size: u32) -> Option<RegistryChunk<'_>> {
		let start_id = self.reserve_ids(size)?;
		Some(RegistryChunk::new(self, start_id, size))
	}

	fn reserve_ids(&self, size: u32) -> Option<u32> {
		let limit = self.capacity.saturating_add(1);
		let mut start_id = self.next_id.load(Ordering::Relaxed);
		loop {
			let end_id = start_id.checked_add(size)?;
			if end_id > limit {
				return None;
			}
			match self.next_id.compare_exchange_weak(
				start_id,
				end_id,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => {
					self.bump_inspector_revision();
					return Some(start_id);
				}
				Err(observed) => start_id = observed,
			}
		}
	}

	/// ATOMIC HOT-SWAP: Allows the I/O thread to populate virtual_data once DMA completes.
	pub fn hot_swap_virtual_data(&self, node: NodeId, data: Vec<u8>) {
		let _resident_write = self.resident_data_lock.write().unwrap();
		self.hot_swap_virtual_data_locked(node, data);
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
		self.hot_swap_virtual_data(leaf, data);
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

	fn hot_swap_virtual_data_locked(&self, node: NodeId, data: Vec<u8>) {
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
		if self.content_generation(node) != expected_generation {
			return false;
		}
		self.hot_swap_virtual_data_locked(node, data);
		true
	}

	pub(crate) fn content_generation(&self, node: NodeId) -> u64 {
		self.content_generation[node.index()].load(Ordering::Acquire)
	}

	pub(crate) fn bump_content_generation(&self, node: NodeId) -> u64 {
		let generation = self.content_generation[node.index()]
			.fetch_add(1, Ordering::AcqRel)
			.saturating_add(1);
		self.whole_leaf_read_outcomes
			.lock()
			.unwrap()
			.retain(|(candidate, _), _| *candidate != node);
		generation
	}

	pub(crate) fn whole_leaf_is_resident(&self, node: NodeId) -> bool {
		let _resident_read = self.resident_data_lock.read().unwrap();
		unsafe { (&*self.virtual_data[node.index()].get()).is_some() }
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
		if self.content_generation(node) != generation {
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
		if self.content_generation(node) != expected_generation {
			return false;
		}
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

	pub(crate) fn resident_data_write_guard(&self) -> RwLockWriteGuard<'_, ()> {
		self.resident_data_lock.write().unwrap()
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
		RegistryStatsSnapshot {
			allocated_nodes: u64::from(self.next_id.load(Ordering::Acquire).saturating_sub(1)),
			cold_extents: self.cold_extent_count.load(Ordering::Acquire),
			hot_extents: self.hot_extent_count.load(Ordering::Acquire),
			scanned_bytes: self.scanned_bytes.load(Ordering::Acquire),
			pending_scan_blocks: self.pending_scan_blocks.load(Ordering::Acquire),
			resident_bytes: self.resident_bytes.load(Ordering::Acquire),
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

	/// Returns only state already present in memory. This is intentionally
	/// separate from the read APIs so UAST inspection cannot initiate I/O or
	/// inflate a cold leaf merely by displaying it.
	pub(crate) fn inspector_node_metadata(
		&self,
		node: NodeId,
		snippet_limit: usize,
	) -> InspectorNodeMetadata {
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
		self.bump_inspector_revision();
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

	pub fn begin_metric_scan(&self, root: NodeId, first_leaf: NodeId, last_leaf: NodeId) {
		self.incomplete_metric_roots.lock().unwrap().remove(&root);
		let end = last_leaf
			.index()
			.checked_add(1)
			.expect("metric scan extent range overflow");
		let mut unclaimed = VecDeque::new();
		let mut pending = HashSet::new();
		for index in first_leaf.index()..end {
			let node_id = NodeId::from_index(index);
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
		match self.terminal_newline_state[node.index()].load(Ordering::Acquire) {
			1 => Some(false),
			2 => Some(true),
			_ => None,
		}
	}

	pub(crate) fn set_terminal_newline(&self, node: NodeId, ends_with_newline: bool) {
		self.terminal_newline_state[node.index()]
			.store(if ends_with_newline { 2 } else { 1 }, Ordering::Release);
	}

	pub(crate) fn clear_terminal_newline(&self, node: NodeId) {
		self.terminal_newline_state[node.index()].store(0, Ordering::Release);
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
			if self.content_generation(result.node_id) != result.content_generation {
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
		let idx = node.index();
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
		let idx = node.index();
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
		let idx = node.index();
		unsafe {
			if let Some(v_data) = &*self.virtual_data[idx].get() {
				return v_data.clone();
			}
			// Physical nodes return empty buffers until the resolver hot-swaps them.
		}
		Vec::new()
	}

	pub fn get_first_child(&self, node: NodeId) -> Option<NodeId> {
		unsafe { (*self.edges[node.index()].get()).first_child }
	}

	pub fn get_total_newlines(&self, node: NodeId) -> u64 {
		unsafe { (*self.metrics[node.index()].get()).newlines }
	}

	pub fn get_total_bytes(&self, node: NodeId) -> u64 {
		unsafe { (*self.metrics[node.index()].get()).byte_length }
	}

	pub fn get_prev_sibling(&self, node: NodeId) -> Option<NodeId> {
		let parent = unsafe { (*self.edges[node.index()].get()).parent }?;
		let first_child = unsafe { (*self.edges[parent.index()].get()).first_child }?;
		if first_child == node {
			return None;
		}
		let mut curr = first_child;
		loop {
			let next = unsafe { (*self.edges[curr.index()].get()).next_sibling };
			if next == Some(node) {
				return Some(curr);
			}
			curr = next?;
		}
	}

	pub fn get_next_sibling(&self, node: NodeId) -> Option<NodeId> {
		unsafe { (*self.edges[node.index()].get()).next_sibling }
	}

	pub(crate) fn document_root_for_node(&self, node: NodeId) -> NodeId {
		let mut root = node;
		while let Some(parent) = unsafe { (*self.edges[root.index()].get()).parent } {
			root = parent;
		}
		root
	}
}

#[cfg(test)]
mod tests {
	use super::UastRegistry;
	use crate::svp::pointer::SvpPointer;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;
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
		registry.begin_metric_scan(root, first, second);

		assert_eq!(registry.exact_line_count(root), None);
		assert!(!registry.try_complete_metric_inflation(first, 2, true));
		assert_eq!(registry.exact_line_count(root), None);
		assert!(registry.try_complete_metric_inflation(second, 1, false));
		assert_eq!(registry.exact_line_count(root), Some(4));

		assert!(!registry.try_complete_metric_inflation(second, 99, false));
		assert_eq!(registry.exact_line_count(root), Some(4));
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
			registry.hot_swap_virtual_data(leaf, vec![leaf.index() as u8; 4]);
		}
		// Eviction is engine-thread work; the resolver only publishes bytes.
		registry.enforce_resident_budget(None);

		assert!(registry.stats().resident_bytes <= 8);
		assert!(unsafe { (&*registry.virtual_data[leaves[0].index()].get()).is_none() });
		assert!(unsafe { (&*registry.virtual_data[leaves[2].index()].get()).is_some() });

		registry.hot_swap_virtual_data(leaves[0], vec![9; 4]);
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
			registry.hot_swap_virtual_data(first, vec![generation as u8; 4]);
		}
		assert_eq!(registry.resident_lru_len(), 1);

		registry.set_resident_budget(4, 4);
		registry.hot_swap_virtual_data(second, vec![2; 4]);
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
			registry.hot_swap_virtual_data(leaf, vec![0; EXTENT_BYTES as usize]);
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
		registry.hot_swap_virtual_data(first, vec![1; 4]);
		registry.hot_swap_virtual_data(second, vec![2; 4]);
		assert!(unsafe { (&*registry.virtual_data[first.index()].get()).is_some() });

		registry.set_viewport_resident_pins(7, []);
		registry.enforce_resident_budget(None);
		assert!(registry.stats().resident_bytes <= 4);
	}
}
