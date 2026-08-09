use crate::core::BufferId;
use crate::ecs::id::NodeId;
use crate::ecs::registry::{IoLifetimeLease, UastRegistry, WholeLeafReadOutcome};
use crate::svp::pointer::SvpPointer;
use memchr::memchr_iter;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

mod blocking;
#[cfg(target_os = "linux")]
mod linux_uring;

#[cfg(target_os = "linux")]
use blocking::BlockingBackend;
#[cfg(not(target_os = "linux"))]
use blocking::BlockingBackend as PlatformBackend;
#[cfg(target_os = "linux")]
use linux_uring::LinuxUringBackend as PlatformBackend;

/// Mutation-generated hot leaf size. This remains small so a one-byte edit
/// copies only bounded data.
pub const DMA_CHUNK_SIZE: usize = 64 * 1024;
/// Maximum bounded resolver transfer. Kept separate from hot-leaf rechunking.
pub const RESOLVER_TRANSFER_SIZE: usize = 1024 * 1024;
/// Background scan reads kept outstanding. `SvpRequestQueue` drains viewport
/// work ahead of scan work, so scan depth buys throughput without ever putting
/// a bounded transfer in front of the viewport.
pub(crate) const METRIC_SCAN_LANES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPriority {
	High, // Viewport (Immediate hot-swap)
	Low,  // Metric Scan (Inflate SpanMetrics)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WholeLeafHydration {
	Ready,
	Pending,
	Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WholeLeafHydrationError {
	QueueFull,
	InvalidRequest,
}

pub struct SvpRequest {
	pub node_id: NodeId,
	pub pointer: SvpPointer,
	pub extent_offset: u32,
	pub request_length: u32,
	pub priority: RequestPriority,
	pub scan_root: Option<NodeId>,
	pub content_generation: u64,
	pub(crate) hydration_buffer: Option<BufferId>,
	pub(crate) lifetime_lease: IoLifetimeLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolverNotification {
	Refresh,
	HydrationCompleted(BufferId),
}

#[derive(Clone)]
enum ResolverNotificationTarget {
	RefreshOnly(mpsc::Sender<()>),
	Attributed(mpsc::Sender<ResolverNotification>),
}

#[derive(Clone)]
pub(super) struct ResolverNotifiers {
	target: ResolverNotificationTarget,
}

impl ResolverNotifiers {
	pub(super) fn notify(&self, needs_refresh: bool, hydration_buffer: Option<BufferId>) {
		if !needs_refresh {
			return;
		}
		self.notify_batch(&[hydration_buffer]);
	}

	/// Publish one completion batch. Callers provide at most one entry for each
	/// distinct attributed buffer; refresh-only consumers retain the historical
	/// one-wake-per-batch behavior.
	pub(super) fn notify_batch(&self, notifications: &[Option<BufferId>]) {
		if notifications.is_empty() {
			return;
		}
		match &self.target {
			ResolverNotificationTarget::RefreshOnly(tx) => {
				let _ = tx.send(());
			}
			ResolverNotificationTarget::Attributed(tx) => {
				for notification in notifications {
					let notification = match notification {
						Some(buffer) => ResolverNotification::HydrationCompleted(*buffer),
						None => ResolverNotification::Refresh,
					};
					let _ = tx.send(notification);
				}
			}
		}
	}
}

/// Request queue that always hands a backend viewport work before scan work.
///
/// `RequestPriority` is only meaningful if something acts on it. A single FIFO
/// cannot: a run of low-priority scan reads sits in front of a viewport read and
/// the backend never sees the priority at all. Splitting the lanes lets the
/// scanner keep many reads in flight for throughput while a viewport request
/// still waits behind at most the one transfer already issued.
///
/// Both lanes live under one lock because the choice between them has to be a
/// single atomic step. Popping lock-free lanes in sequence — try high, fall back
/// to low — lets a high request arrive after the consumer has looked at the high
/// lane and found it empty, and it is then handed a scan read anyway. The
/// invariant is that a queued viewport read is never passed over, so selection
/// must observe both lanes at one instant.
pub(crate) struct SvpRequestQueue {
	lanes: Mutex<RequestLanes>,
	capacity: usize,
}

struct RequestLanes {
	high: VecDeque<SvpRequest>,
	low: VecDeque<SvpRequest>,
}

impl SvpRequestQueue {
	fn new(capacity: usize) -> Self {
		Self {
			lanes: Mutex::new(RequestLanes {
				high: VecDeque::with_capacity(capacity),
				low: VecDeque::with_capacity(capacity),
			}),
			capacity,
		}
	}

	fn lanes(&self) -> std::sync::MutexGuard<'_, RequestLanes> {
		self.lanes
			.lock()
			.unwrap_or_else(|poison| poison.into_inner())
	}

	/// Enqueues a request, or hands it back when its lane is full. Each lane is
	/// bounded separately: a saturated scan lane must never be able to crowd out
	/// viewport reads, which is the same reason the lanes are split at all.
	pub(crate) fn push(&self, request: SvpRequest) -> Result<(), SvpRequest> {
		let mut lanes = self.lanes();
		let lane = match request.priority {
			RequestPriority::High => &mut lanes.high,
			RequestPriority::Low => &mut lanes.low,
		};
		if lane.len() >= self.capacity {
			return Err(request);
		}
		lane.push_back(request);
		Ok(())
	}

	pub(crate) fn pop(&self) -> Option<SvpRequest> {
		let mut lanes = self.lanes();
		lanes.high.pop_front().or_else(|| lanes.low.pop_front())
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		let lanes = self.lanes();
		lanes.high.len() + lanes.low.len()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendPreference {
	PlatformDefault,
	Blocking,
}

#[cfg(target_os = "linux")]
enum ResolverBackend {
	LinuxUring(PlatformBackend),
	Blocking(BlockingBackend),
}

#[cfg(target_os = "linux")]
impl ResolverBackend {
	fn spawn(
		preference: BackendPreference,
		registry: Arc<UastRegistry>,
		queue: Arc<SvpRequestQueue>,
		notifiers: ResolverNotifiers,
	) -> Self {
		match preference {
			BackendPreference::Blocking => {
				Self::Blocking(BlockingBackend::spawn(registry, queue, notifiers))
			}
			BackendPreference::PlatformDefault => {
				Self::LinuxUring(PlatformBackend::spawn(registry, queue, notifiers))
			}
		}
	}

	fn register_device(&self, device_id: u16, path: &Path) -> std::io::Result<()> {
		match self {
			Self::LinuxUring(backend) => backend.register_device(device_id, path),
			Self::Blocking(backend) => backend.register_device(device_id, path),
		}
	}

	fn unregister_device(&self, device_id: u16) {
		match self {
			Self::LinuxUring(backend) => backend.unregister_device(device_id),
			Self::Blocking(backend) => backend.unregister_device(device_id),
		}
	}

	#[cfg(test)]
	fn registered_device_count(&self) -> usize {
		match self {
			Self::LinuxUring(backend) => backend.registered_device_count(),
			Self::Blocking(backend) => backend.registered_device_count(),
		}
	}
}

#[cfg(not(target_os = "linux"))]
struct ResolverBackend {
	backend: PlatformBackend,
}

#[cfg(not(target_os = "linux"))]
impl ResolverBackend {
	fn spawn(
		_preference: BackendPreference,
		registry: Arc<UastRegistry>,
		queue: Arc<SvpRequestQueue>,
		notifiers: ResolverNotifiers,
	) -> Self {
		Self {
			backend: PlatformBackend::spawn(registry, queue, notifiers),
		}
	}

	fn register_device(&self, device_id: u16, path: &Path) -> std::io::Result<()> {
		self.backend.register_device(device_id, path)
	}

	fn unregister_device(&self, device_id: u16) {
		self.backend.unregister_device(device_id);
	}

	#[cfg(test)]
	fn registered_device_count(&self) -> usize {
		self.backend.registered_device_count()
	}
}

#[allow(dead_code)]
pub struct SvpResolver {
	_registry: Arc<UastRegistry>,
	pub(crate) request_queue: Arc<SvpRequestQueue>,
	backend: ResolverBackend,
}

impl SvpResolver {
	pub fn new(registry: Arc<UastRegistry>, notifier: mpsc::Sender<()>) -> Self {
		Self::with_backend_preference(
			default_backend_preference(),
			registry,
			ResolverNotifiers {
				target: ResolverNotificationTarget::RefreshOnly(notifier),
			},
		)
	}

	pub(crate) fn new_with_notifications(
		registry: Arc<UastRegistry>,
		notifier: mpsc::Sender<ResolverNotification>,
	) -> Self {
		Self::with_backend_preference(
			default_backend_preference(),
			registry,
			ResolverNotifiers {
				target: ResolverNotificationTarget::Attributed(notifier),
			},
		)
	}

	#[cfg(test)]
	pub(crate) fn new_blocking_for_tests(
		registry: Arc<UastRegistry>,
		notifier: mpsc::Sender<()>,
	) -> Self {
		Self::with_backend_preference(
			BackendPreference::Blocking,
			registry,
			ResolverNotifiers {
				target: ResolverNotificationTarget::RefreshOnly(notifier),
			},
		)
	}

	#[cfg(test)]
	pub(crate) fn new_blocking_with_notifications_for_tests(
		registry: Arc<UastRegistry>,
		notifier: mpsc::Sender<ResolverNotification>,
	) -> Self {
		Self::with_backend_preference(
			BackendPreference::Blocking,
			registry,
			ResolverNotifiers {
				target: ResolverNotificationTarget::Attributed(notifier),
			},
		)
	}

	fn with_backend_preference(
		preference: BackendPreference,
		registry: Arc<UastRegistry>,
		notifiers: ResolverNotifiers,
	) -> Self {
		let request_queue = Arc::new(SvpRequestQueue::new(8192));
		let backend = ResolverBackend::spawn(
			preference,
			registry.clone(),
			request_queue.clone(),
			notifiers,
		);

		Self {
			_registry: registry,
			request_queue,
			backend,
		}
	}

	pub fn register_device(&self, device_id: u16, path: &Path) -> std::io::Result<()> {
		self._registry.register_device_path(device_id, path)?;
		if let Err(error) = self.backend.register_device(device_id, path) {
			self._registry.unregister_device(device_id);
			return Err(error);
		}
		Ok(())
	}

	pub(crate) fn unregister_device(&self, device_id: u16) {
		// Stop new platform reads before removing the blocking backing store.
		// Reads already submitted retain their own Arc<File> until completion.
		self.backend.unregister_device(device_id);
		self._registry.unregister_device(device_id);
	}

	#[cfg(test)]
	pub(crate) fn registered_device_counts(&self) -> (usize, usize) {
		(
			self._registry.registered_device_count(),
			self.backend.registered_device_count(),
		)
	}

	pub fn request_dma(&self, node_id: NodeId, pointer: SvpPointer, priority: RequestPriority) {
		let Some(lifetime_lease) = self._registry.acquire_io_lifetime(node_id) else {
			return;
		};
		if priority == RequestPriority::High {
			self._registry.record_high_priority_start(node_id);
		}
		if let Err(request) = self.request_queue.push(SvpRequest {
			node_id,
			pointer,
			extent_offset: 0,
			request_length: pointer.byte_length,
			priority,
			scan_root: None,
			content_generation: self._registry.content_generation(node_id),
			hydration_buffer: None,
			lifetime_lease,
		}) {
			if priority == RequestPriority::High {
				self._registry.record_high_priority_finish(node_id);
				self._registry.finish_dma(node_id);
			}
			drop(request);
		}
	}

	pub fn request_dma_range(
		&self,
		node_id: NodeId,
		pointer: SvpPointer,
		extent_offset: u32,
		request_length: u32,
		priority: RequestPriority,
	) -> Result<(), String> {
		let lifetime_lease = self
			._registry
			.acquire_io_lifetime(node_id)
			.ok_or_else(|| "Resolver request targets a retired node".to_string())?;
		let request = SvpRequest {
			node_id,
			pointer,
			extent_offset,
			request_length,
			priority,
			scan_root: None,
			content_generation: self._registry.content_generation(node_id),
			hydration_buffer: None,
			lifetime_lease,
		};
		request_len(&request).map_err(str::to_string)?;
		if priority == RequestPriority::High {
			self._registry.record_high_priority_start(node_id);
		}
		if let Err(request) = self.request_queue.push(request) {
			if priority == RequestPriority::High {
				self._registry.record_high_priority_finish(node_id);
				self._registry.finish_dma(node_id);
			}
			drop(request);
			return Err("Resolver request queue is full".to_string());
		}
		Ok(())
	}

	pub(crate) fn hydrate_whole_leaf(
		&self,
		node_id: NodeId,
	) -> Result<WholeLeafHydration, WholeLeafHydrationError> {
		self.hydrate_whole_leaf_inner(node_id, None)
	}

	pub(crate) fn hydrate_whole_leaf_for_buffer(
		&self,
		node_id: NodeId,
		buffer_id: BufferId,
	) -> Result<WholeLeafHydration, WholeLeafHydrationError> {
		self.hydrate_whole_leaf_inner(node_id, Some(buffer_id))
	}

	fn hydrate_whole_leaf_inner(
		&self,
		node_id: NodeId,
		hydration_buffer: Option<BufferId>,
	) -> Result<WholeLeafHydration, WholeLeafHydrationError> {
		let lifetime_lease = self
			._registry
			.acquire_io_lifetime(node_id)
			.ok_or(WholeLeafHydrationError::InvalidRequest)?;
		if self._registry.whole_leaf_is_resident(node_id) {
			return Ok(WholeLeafHydration::Ready);
		}

		let generation = self._registry.content_generation(node_id);
		match self._registry.whole_leaf_read_outcome(node_id, generation) {
			Some(WholeLeafReadOutcome::InFlight) => return Ok(WholeLeafHydration::Pending),
			Some(WholeLeafReadOutcome::Failed) => return Ok(WholeLeafHydration::Failed),
			None => {}
		}

		let pointer = self
			._registry
			.node_span(node_id)
			.ok_or(WholeLeafHydrationError::InvalidRequest)?;
		let request = SvpRequest {
			node_id,
			pointer,
			extent_offset: 0,
			request_length: pointer.byte_length,
			priority: RequestPriority::High,
			scan_root: None,
			content_generation: generation,
			hydration_buffer,
			lifetime_lease,
		};
		request_len(&request).map_err(|_| WholeLeafHydrationError::InvalidRequest)?;

		if !self._registry.begin_dma(node_id) {
			// Another viewport request owns the node. Do not associate this
			// generation with that request: an edit may have bumped the generation
			// while an older read was still in flight. Its completion wakes the
			// engine, and the retry can then admit the current generation.
			return Ok(WholeLeafHydration::Pending);
		}
		match self._registry.begin_whole_leaf_read(node_id, generation) {
			WholeLeafReadOutcome::Failed => {
				self._registry.finish_dma(node_id);
				return Ok(WholeLeafHydration::Failed);
			}
			WholeLeafReadOutcome::InFlight => {}
		}
		self._registry.record_high_priority_start(node_id);
		if let Err(request) = self.request_queue.push(request) {
			self._registry.cancel_whole_leaf_read(node_id, generation);
			self._registry.record_high_priority_finish(node_id);
			self._registry.finish_dma(node_id);
			drop(request);
			return Err(WholeLeafHydrationError::QueueFull);
		}

		Ok(WholeLeafHydration::Pending)
	}

	#[allow(dead_code)] // Wired by the bounded-read API's explicit retry operation.
	pub(crate) fn retry_whole_leaf(&self, node_id: NodeId) {
		self._registry.retry_whole_leaf_read(node_id);
	}
}

fn default_backend_preference() -> BackendPreference {
	match std::env::var("BARYON_SVP_BACKEND") {
		Ok(value) if value.eq_ignore_ascii_case("blocking") => BackendPreference::Blocking,
		_ => BackendPreference::PlatformDefault,
	}
}

pub(super) struct CompletionMeta {
	pub(super) node_id: NodeId,
	pub(super) was_viewport: bool,
	pub(super) extent_offset: u32,
	pub(super) request_length: u32,
	pub(super) whole_node: bool,
	pub(super) scan_root: Option<NodeId>,
	pub(super) content_generation: u64,
	pub(super) hydration_buffer: Option<BufferId>,
	_lifetime_lease: IoLifetimeLease,
	#[cfg(target_os = "linux")]
	pub(super) buffer_index: usize,
}

impl CompletionMeta {
	pub(super) fn new(req: SvpRequest, buffer_index: usize) -> Self {
		#[cfg(not(target_os = "linux"))]
		let _ = buffer_index;

		Self {
			node_id: req.node_id,
			was_viewport: req.priority == RequestPriority::High,
			extent_offset: req.extent_offset,
			request_length: req.request_length,
			whole_node: req.extent_offset == 0 && req.request_length == req.pointer.byte_length,
			scan_root: req.scan_root,
			content_generation: req.content_generation,
			hydration_buffer: req.hydration_buffer,
			_lifetime_lease: req.lifetime_lease,
			#[cfg(target_os = "linux")]
			buffer_index,
		}
	}
}

pub(super) fn request_len(req: &SvpRequest) -> Result<u32, &'static str> {
	let end = req
		.extent_offset
		.checked_add(req.request_length)
		.ok_or("subrange request overflow")?;
	if end > req.pointer.byte_length {
		return Err("subrange request exceeds its physical extent");
	}
	if req.request_length as usize > RESOLVER_TRANSFER_SIZE {
		return Err("whole-node DMA request exceeds the resolver transfer buffer");
	}
	Ok(req.request_length)
}

pub(super) fn complete_read(
	registry: &Arc<UastRegistry>,
	queue: &SvpRequestQueue,
	meta: CompletionMeta,
	bytes: &[u8],
) -> bool {
	let newlines = memchr_iter(b'\n', bytes).count() as u32;
	let ends_with_newline = bytes.last() == Some(&b'\n');

	let metric_refresh = meta.whole_node
		&& registry.queue_metric_inflation(
			meta.node_id,
			newlines,
			ends_with_newline,
			meta.scan_root,
			meta.content_generation,
		);
	if meta.was_viewport {
		registry.record_high_priority_finish(meta.node_id);
	} else {
		registry
			.scanned_bytes
			.fetch_add(bytes.len() as u64, Ordering::Relaxed);
	}

	let needs_refresh = if meta.was_viewport {
		if meta.whole_node {
			let published = registry.publish_resolved_data(
				meta.node_id,
				meta.content_generation,
				bytes.to_vec(),
			);
			registry.finish_whole_leaf_read(meta.node_id, meta.content_generation, published);
		} else {
			registry.publish_partial_resident_range(
				meta.node_id,
				meta.content_generation,
				meta.extent_offset,
				bytes.to_vec(),
			);
		}
		true
	} else {
		metric_refresh
	};

	registry.finish_dma(meta.node_id);
	if let Some(root) = meta.scan_root {
		registry.finish_metric_scan_io(root);
	}
	pump_metric_scans(registry, queue, meta.scan_root);
	needs_refresh
}

pub(super) fn complete_failed(
	registry: &Arc<UastRegistry>,
	queue: &SvpRequestQueue,
	meta: CompletionMeta,
) -> bool {
	let needs_refresh = meta.was_viewport && meta.whole_node;
	if meta.was_viewport {
		registry.record_high_priority_finish(meta.node_id);
		if meta.whole_node {
			registry.finish_whole_leaf_read(meta.node_id, meta.content_generation, false);
		}
	}
	registry.finish_dma(meta.node_id);
	if let Some(root) = meta.scan_root {
		registry.finish_metric_scan_io(root);
		registry.record_metric_scan_failure(root, meta.node_id);
	}
	pump_metric_scans(registry, queue, meta.scan_root);
	needs_refresh
}

fn pump_metric_scans(
	registry: &Arc<UastRegistry>,
	queue: &SvpRequestQueue,
	preferred_root: Option<NodeId>,
) {
	if let Some(root) = preferred_root {
		request_next_scan(registry, queue, Some(root));
	}
	for root in registry.active_metric_scan_roots() {
		if Some(root) != preferred_root {
			request_next_scan(registry, queue, Some(root));
		}
	}
}

fn request_next_scan(
	registry: &Arc<UastRegistry>,
	queue: &SvpRequestQueue,
	scan_root: Option<NodeId>,
) {
	let Some(root) = scan_root else {
		return;
	};
	let Some(claim) = registry.claim_next_metric_scan(root) else {
		return;
	};
	let Some(lifetime_lease) = registry.acquire_io_lifetime(claim.node_id) else {
		registry.return_metric_scan_claim(root, claim);
		return;
	};
	let request = SvpRequest {
		node_id: claim.node_id,
		pointer: claim.pointer,
		extent_offset: 0,
		request_length: claim.pointer.byte_length,
		priority: RequestPriority::Low,
		scan_root: Some(root),
		content_generation: claim.content_generation,
		hydration_buffer: None,
		lifetime_lease,
	};
	if let Err(request) = queue.push(request) {
		registry.return_metric_scan_claim(root, claim);
		drop(request);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::backing_store::GeneratedBackingStore;
	use crate::uast::{SemanticKind, SpanMetrics};
	use std::time::{Duration, SystemTime, UNIX_EPOCH};

	fn published_nodes(count: usize) -> (Arc<UastRegistry>, Vec<NodeId>) {
		let registry = Arc::new(UastRegistry::new(count as u32));
		let mut reservation = registry
			.reserve_chunk(count as u32)
			.expect("fixture reservation");
		let nodes = (0..count)
			.map(|_| reservation.spawn_node(SemanticKind::Token, None, SpanMetrics::default()))
			.collect();
		drop(reservation);
		(registry, nodes)
	}

	#[test]
	fn completion_held_across_retirement_finishes_before_slot_reuse() {
		let (registry, nodes) = published_nodes(1);
		let old = nodes[0];
		let pointer = SvpPointer {
			lba: 0,
			byte_length: 4,
			device_id: 1,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[old.index()].get() = Some(pointer);
		}
		assert!(registry.begin_dma(old));
		let request = SvpRequest {
			node_id: old,
			pointer,
			extent_offset: 0,
			request_length: 4,
			priority: RequestPriority::High,
			scan_root: None,
			content_generation: registry.content_generation(old),
			hydration_buffer: None,
			lifetime_lease: registry.acquire_io_lifetime(old).expect("request lease"),
		};
		let meta = CompletionMeta::new(request, 0);

		registry.retire_orphaned_forest(
			[old],
			1,
			0,
			crate::ecs::registry::ReclaimFence::NeverPublished,
		);
		registry.reclaim_orphaned_node(old);
		assert_eq!(registry.stats().pending_reuse_slots, 1);
		assert!(
			registry.reserve_chunk(1).is_none(),
			"the old completion must keep the slot unavailable"
		);

		let queue = SvpRequestQueue::new(1);
		assert!(complete_failed(&registry, &queue, meta));
		assert_eq!(registry.stats().pending_reuse_slots, 0);
		let mut replacement = registry.reserve_chunk(1).expect("completion released slot");
		let new = replacement.spawn_node(
			SemanticKind::None,
			None,
			SpanMetrics {
				byte_length: 9,
				newlines: 2,
			},
		);
		assert_eq!(new.index(), old.index());
		assert_ne!(new, old);
		assert_eq!(registry.get_total_bytes(new), 9);
		assert!(
			registry.begin_dma(new),
			"old completion cleanup must not leave the replacement DMA flag set"
		);
		registry.finish_dma(new);
	}

	#[test]
	fn completion_batches_preserve_attribution_without_multiplying_legacy_refreshes() {
		let (legacy_tx, legacy_rx) = mpsc::channel();
		let legacy = ResolverNotifiers {
			target: ResolverNotificationTarget::RefreshOnly(legacy_tx),
		};
		legacy.notify_batch(&[None, Some(BufferId::new(1)), Some(BufferId::new(2))]);
		assert_eq!(legacy_rx.try_iter().count(), 1);

		let (attributed_tx, attributed_rx) = mpsc::channel();
		let attributed = ResolverNotifiers {
			target: ResolverNotificationTarget::Attributed(attributed_tx),
		};
		attributed.notify_batch(&[None, Some(BufferId::new(1)), Some(BufferId::new(2))]);
		assert_eq!(
			attributed_rx.try_iter().collect::<Vec<_>>(),
			vec![
				ResolverNotification::Refresh,
				ResolverNotification::HydrationCompleted(BufferId::new(1)),
				ResolverNotification::HydrationCompleted(BufferId::new(2)),
			]
		);
	}

	#[test]
	fn unregister_releases_registry_and_platform_backing_entries() {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock")
			.as_nanos();
		let path = std::env::temp_dir().join(format!(
			"baryon-resolver-unregister-{}-{nonce}",
			std::process::id()
		));
		std::fs::write(&path, b"backing").expect("write backing");
		let registry = Arc::new(UastRegistry::new(1));
		let (notify, _notifications) = mpsc::channel();
		let resolver = SvpResolver::new(registry, notify);

		resolver
			.register_device(37, &path)
			.expect("register backing");
		assert_eq!(resolver.registered_device_counts().0, 1);
		#[cfg(target_os = "linux")]
		assert_eq!(
			resolver.registered_device_counts().1,
			usize::from(matches!(&resolver.backend, ResolverBackend::LinuxUring(_))),
			"only the io_uring backend retains a second registered device entry"
		);
		#[cfg(not(target_os = "linux"))]
		assert_eq!(resolver.registered_device_counts().1, 0);

		resolver.unregister_device(37);
		assert_eq!(resolver.registered_device_counts(), (0, 0));
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn oversized_whole_node_request_is_a_hard_error() {
		let (registry, nodes) = published_nodes(1);
		let request = SvpRequest {
			node_id: nodes[0],
			pointer: SvpPointer {
				lba: 0,
				byte_length: RESOLVER_TRANSFER_SIZE as u32 + 1,
				device_id: 1,
				head_trim: 0,
			},
			extent_offset: 0,
			request_length: RESOLVER_TRANSFER_SIZE as u32 + 1,
			priority: RequestPriority::High,
			scan_root: None,
			content_generation: 0,
			hydration_buffer: None,
			lifetime_lease: registry.acquire_io_lifetime(nodes[0]).expect("live node"),
		};
		assert!(request_len(&request).is_err());
	}

	#[test]
	fn scan_lanes_cover_each_extent_once_without_overlap() {
		const EXTENTS: usize = 67;
		let (registry, nodes) = published_nodes(EXTENTS + 1);
		let root = nodes[0];
		for extent in 0..EXTENTS {
			let index = extent + 1;
			unsafe {
				*registry.spans[index].get() = Some(SvpPointer {
					lba: index as u64,
					byte_length: 1,
					device_id: 1,
					head_trim: 0,
				});
				(*registry.edges[index].get()).parent = Some(root);
			}
		}
		let leaves = nodes[1..].to_vec();
		registry.begin_metric_scan(root, leaves);
		let queue = SvpRequestQueue::new(EXTENTS * 2);
		for _ in 0..METRIC_SCAN_LANES.min(EXTENTS) {
			let claim = registry.claim_next_metric_scan(root).unwrap();
			assert!(
				queue
					.push(SvpRequest {
						node_id: claim.node_id,
						pointer: claim.pointer,
						extent_offset: 0,
						request_length: 1,
						priority: RequestPriority::Low,
						scan_root: Some(root),
						content_generation: claim.content_generation,
						hydration_buffer: None,
						lifetime_lease: registry
							.acquire_io_lifetime(claim.node_id)
							.expect("live scan node"),
					})
					.is_ok()
			);
		}

		let mut visits = [0u8; EXTENTS];
		while let Some(request) = queue.pop() {
			visits[request.node_id.index() - 1] += 1;
			registry.finish_metric_scan_io(root);
			request_next_scan(&registry, &queue, request.scan_root);
		}
		assert!(visits.iter().all(|&count| count == 1));
	}

	#[test]
	fn background_metric_scan_does_not_crowd_viewport_io_out_of_the_queue() {
		const EXTENTS: usize = METRIC_SCAN_LANES + 2;
		let (registry, nodes) = published_nodes(EXTENTS + 1);
		let root = nodes[0];
		for index in 1..=EXTENTS {
			unsafe {
				*registry.spans[index].get() = Some(SvpPointer {
					lba: index as u64,
					byte_length: 1,
					device_id: 1,
					head_trim: 0,
				});
				(*registry.edges[index].get()).parent = Some(root);
			}
		}
		registry.begin_metric_scan(root, nodes[1..].iter().copied());
		let queue = SvpRequestQueue::new(EXTENTS + 2);
		for _ in 0..METRIC_SCAN_LANES {
			request_next_scan(&registry, &queue, Some(root));
		}
		let viewport_node = nodes[EXTENTS];
		assert!(
			queue
				.push(SvpRequest {
					node_id: viewport_node,
					pointer: unsafe {
						(*registry.spans[viewport_node.index()].get()).expect("viewport span")
					},
					extent_offset: 0,
					request_length: 1,
					priority: RequestPriority::High,
					scan_root: None,
					content_generation: 0,
					hydration_buffer: None,
					lifetime_lease: registry
						.acquire_io_lifetime(viewport_node)
						.expect("live viewport node"),
				})
				.is_ok(),
			"viewport request fits queue"
		);

		// A full scan depth is queued ahead of the viewport request in wall-clock
		// order, so a plain FIFO would hand the backend every one of them first.
		assert_eq!(queue.len(), METRIC_SCAN_LANES + 1);

		let mut background_requests_ahead = 0usize;
		while let Some(request) = queue.pop() {
			if request.priority == RequestPriority::High {
				break;
			}
			background_requests_ahead += 1;
		}
		assert_eq!(
			background_requests_ahead, 0,
			"viewport I/O was handed to the backend behind {background_requests_ahead} \
			 line-count reads; scan depth must not delay the viewport"
		);
	}

	#[test]
	fn failed_scan_read_marks_count_incomplete_and_keeps_lane_moving() {
		const EXTENTS: usize = 5;
		let (registry, nodes) = published_nodes(EXTENTS + 1);
		let root = nodes[0];
		for index in 1..=EXTENTS {
			unsafe {
				*registry.spans[index].get() = Some(SvpPointer {
					lba: index as u64,
					byte_length: 1,
					device_id: 1,
					head_trim: 0,
				});
				(*registry.edges[index].get()).parent = Some(root);
			}
		}
		registry.begin_metric_scan(root, nodes[1..].iter().copied());
		let queue = SvpRequestQueue::new(EXTENTS * 2);
		request_next_scan(&registry, &queue, Some(root));

		let mut failed = 0usize;
		while let Some(request) = queue.pop() {
			failed += 1;
			complete_failed(&registry, &queue, CompletionMeta::new(request, 0));
		}

		assert_eq!(failed, EXTENTS);
		assert_eq!(registry.stats().pending_scan_blocks, 0);
		assert_eq!(registry.exact_line_count(root), None);
	}

	#[test]
	fn saturated_queue_returns_scan_claim_and_resumes_when_a_slot_opens() {
		let (registry, nodes) = published_nodes(3);
		let root = nodes[0];
		for index in 1..=2 {
			unsafe {
				*registry.spans[index].get() = Some(SvpPointer {
					lba: index as u64,
					byte_length: 1,
					device_id: 1,
					head_trim: 0,
				});
				(*registry.edges[index].get()).parent = Some(root);
			}
		}
		registry.begin_metric_scan(root, nodes[1..].iter().copied());
		let queue = SvpRequestQueue::new(1);
		assert!(
			queue
				.push(SvpRequest {
					node_id: root,
					pointer: SvpPointer {
						lba: 0,
						byte_length: 1,
						device_id: 1,
						head_trim: 0,
					},
					extent_offset: 0,
					request_length: 1,
					priority: RequestPriority::High,
					scan_root: None,
					content_generation: 0,
					hydration_buffer: None,
					lifetime_lease: registry.acquire_io_lifetime(root).expect("live root"),
				})
				.is_ok()
		);

		request_next_scan(&registry, &queue, Some(root));
		assert_eq!(queue.pop().unwrap().node_id, root);
		pump_metric_scans(&registry, &queue, None);
		let first = queue.pop().expect("returned claim is schedulable");
		assert_eq!(first.node_id, nodes[1]);
		complete_failed(&registry, &queue, CompletionMeta::new(first, 0));
		let second = queue.pop().expect("second scan block");
		assert_eq!(second.node_id, nodes[2]);
		complete_failed(&registry, &queue, CompletionMeta::new(second, 0));
		assert_eq!(registry.stats().pending_scan_blocks, 0);
	}

	#[test]
	fn stale_dma_completion_cannot_overwrite_edited_resident_data() {
		let (registry, nodes) = published_nodes(1);
		let node = nodes[0];
		let pointer = SvpPointer {
			lba: 0,
			byte_length: 6,
			device_id: 1,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[0].get() = Some(pointer);
		}
		let request = SvpRequest {
			node_id: node,
			pointer,
			extent_offset: 0,
			request_length: 6,
			priority: RequestPriority::High,
			scan_root: None,
			content_generation: registry.content_generation(node),
			hydration_buffer: None,
			lifetime_lease: registry.acquire_io_lifetime(node).expect("live node"),
		};
		let meta = CompletionMeta::new(request, 0);
		registry.bump_content_generation(node);
		registry.promote_hydrated_data(node);
		registry.replace_authoritative_data(node, b"edited".to_vec());

		let queue = SvpRequestQueue::new(1);
		assert!(complete_read(&registry, &queue, meta, b"oldold"));
		assert_eq!(
			unsafe { (&*registry.virtual_data[0].get()).as_deref() },
			Some(b"edited".as_slice())
		);
		assert!(registry.drain_metric_inflations().is_empty());
		assert!(!registry.metrics_inflated[0].load(Ordering::Acquire));
	}

	#[test]
	fn resolver_metric_results_are_applied_only_when_the_engine_drains_them() {
		let (registry, nodes) = published_nodes(2);
		let root = nodes[0];
		let leaf = nodes[1];
		let pointer = SvpPointer {
			lba: 0,
			byte_length: 2,
			device_id: 1,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[leaf.index()].get() = Some(pointer);
			(*registry.edges[leaf.index()].get()).parent = Some(root);
			(*registry.edges[root.index()].get()).first_child = Some(leaf);
			(*registry.metrics[root.index()].get()).byte_length = 2;
			(*registry.metrics[leaf.index()].get()).byte_length = 2;
		}
		registry.begin_metric_scan(root, [leaf]);
		let claim = registry.claim_next_metric_scan(root).unwrap();
		let request = SvpRequest {
			node_id: claim.node_id,
			pointer: claim.pointer,
			extent_offset: 0,
			request_length: 2,
			priority: RequestPriority::Low,
			scan_root: Some(root),
			content_generation: claim.content_generation,
			hydration_buffer: None,
			lifetime_lease: registry.acquire_io_lifetime(leaf).expect("live leaf"),
		};
		let queue = SvpRequestQueue::new(2);

		assert!(complete_read(
			&registry,
			&queue,
			CompletionMeta::new(request, 0),
			b"x\n",
		));
		assert_eq!(registry.get_total_newlines(root), 0);
		assert_eq!(registry.exact_line_count(root), None);

		assert_eq!(registry.drain_metric_inflations(), vec![root]);
		assert_eq!(registry.get_total_newlines(root), 1);
		assert_eq!(registry.exact_line_count(root), Some(2));
	}

	#[test]
	fn blocking_subrange_read_tracks_only_covered_bytes() {
		let (registry, nodes) = published_nodes(1);
		let node = nodes[0];
		let span = SvpPointer {
			lba: 0,
			byte_length: 32,
			device_id: 44,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[0].get() = Some(span);
		}
		let store = Arc::new(GeneratedBackingStore::new(32, |offset| offset as u8));
		registry.register_backing_store(44, store.clone());
		let (tx, rx) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), tx);
		registry.dma_in_flight[0].store(true, Ordering::Release);
		resolver
			.request_dma_range(node, span, 8, 4, RequestPriority::High)
			.expect("queue subrange");
		rx.recv_timeout(Duration::from_secs(2))
			.expect("subrange completion notification");

		assert_eq!(
			registry.read_partial_resident_range(node, 8, 4),
			Some(vec![8, 9, 10, 11])
		);
		assert_eq!(registry.read_partial_resident_range(node, 7, 2), None);
		assert!(unsafe { (&*registry.virtual_data[0].get()).is_none() });
		assert_eq!(store.bytes_read(), 4);
	}

	#[test]
	fn whole_leaf_hydration_publishes_bytes_and_wakes_the_engine() {
		let (registry, nodes) = published_nodes(1);
		let node = nodes[0];
		let span = SvpPointer {
			lba: 0,
			byte_length: 6,
			device_id: 45,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[0].get() = Some(span);
			(*registry.metrics[0].get()).byte_length = 6;
		}
		registry.register_backing_store(
			45,
			Arc::new(GeneratedBackingStore::new(6, |offset| b'a' + offset as u8)),
		);
		let (tx, rx) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), tx);

		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Pending)
		);
		rx.recv_timeout(Duration::from_secs(2))
			.expect("whole-leaf completion notification");
		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Ready)
		);
		assert_eq!(
			unsafe { (&*registry.virtual_data[0].get()).as_deref() },
			Some(b"abcdef".as_slice())
		);
	}

	#[test]
	fn failed_whole_leaf_hydration_is_sticky_until_explicit_retry() {
		let (registry, nodes) = published_nodes(1);
		let node = nodes[0];
		let span = SvpPointer {
			lba: 0,
			byte_length: 4,
			device_id: 99,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[0].get() = Some(span);
			(*registry.metrics[0].get()).byte_length = 4;
		}
		let (tx, rx) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), tx);

		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Pending)
		);
		rx.recv_timeout(Duration::from_secs(2))
			.expect("failed hydration must wake the engine");
		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Failed)
		);
		assert!(
			rx.recv_timeout(Duration::from_millis(20)).is_err(),
			"observing a recorded failure must not resubmit implicitly"
		);

		resolver.retry_whole_leaf(node);
		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Pending)
		);
		rx.recv_timeout(Duration::from_secs(2))
			.expect("explicit retry completion notification");
		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Failed)
		);
	}

	#[test]
	fn attributed_whole_leaf_success_and_failure_echo_the_admitting_buffer() {
		for (device_id, succeeds) in [(46_u16, true), (100_u16, false)] {
			let (registry, nodes) = published_nodes(1);
			let node = nodes[0];
			let span = SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id,
				head_trim: 0,
			};
			unsafe {
				*registry.spans[0].get() = Some(span);
				(*registry.metrics[0].get()).byte_length = 4;
			}
			if succeeds {
				registry.register_backing_store(
					device_id,
					Arc::new(GeneratedBackingStore::new(4, |offset| b'a' + offset as u8)),
				);
			}
			let (tx, rx) = mpsc::channel();
			let resolver = SvpResolver::new_blocking_with_notifications_for_tests(registry, tx);
			let buffer = BufferId::new(if succeeds { 7 } else { 8 });

			assert_eq!(
				resolver.hydrate_whole_leaf_for_buffer(node, buffer),
				Ok(WholeLeafHydration::Pending)
			);
			assert_eq!(
				rx.recv_timeout(Duration::from_secs(2)),
				Ok(ResolverNotification::HydrationCompleted(buffer))
			);
			assert_eq!(
				resolver.hydrate_whole_leaf_for_buffer(node, buffer),
				Ok(if succeeds {
					WholeLeafHydration::Ready
				} else {
					WholeLeafHydration::Failed
				})
			);
		}
	}

	#[test]
	fn new_generation_does_not_attach_to_an_older_in_flight_read() {
		let (registry, nodes) = published_nodes(1);
		let node = nodes[0];
		let span = SvpPointer {
			lba: 0,
			byte_length: 4,
			device_id: 1,
			head_trim: 0,
		};
		unsafe {
			*registry.spans[0].get() = Some(span);
			(*registry.metrics[0].get()).byte_length = 4;
		}
		let (tx, _rx) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), tx);

		registry.dma_in_flight[0].store(true, Ordering::Release);
		let current_generation = registry.bump_content_generation(node);
		assert_eq!(
			resolver.hydrate_whole_leaf(node),
			Ok(WholeLeafHydration::Pending)
		);
		assert_eq!(
			registry.whole_leaf_read_outcome(node, current_generation),
			None,
			"the current generation must wait for, not inherit, an older request"
		);
	}
}
