use crate::metrics::global as metrics;
use crate::schema::{
	CdcCursor, CdcRejectCode, Locality, LogicalSchemaFingerprint, TableObjectId, TenantId,
};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::interval;

pub const DEFAULT_MAX_CDC_LIVE_TAIL_BYTES_PER_LOCALITY: u64 = 64 * 1024 * 1024;
pub const DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS: u64 = 1000;

pub type FinalizationLogIndex = u64;

#[derive(Debug)]
pub struct CdcLiveEntry {
	pub cursor: CdcCursor,
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub record_batch: Arc<RecordBatch>,
	finalized: AtomicBool,
	estimated_bytes: u64,
}

impl CdcLiveEntry {
	pub fn new(
		cursor: CdcCursor,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		logical_schema_fingerprint: LogicalSchemaFingerprint,
		record_batch: Arc<RecordBatch>,
	) -> Self {
		let estimated_bytes = estimate_record_batch_bytes(&record_batch);
		Self {
			cursor,
			tenant_id,
			table_object_id,
			logical_schema_fingerprint,
			record_batch,
			finalized: AtomicBool::new(false),
			estimated_bytes,
		}
	}

	pub fn with_finalized(self, finalized: bool) -> Self {
		self.finalized.store(finalized, Ordering::Release);
		self
	}

	pub fn is_finalized(&self) -> bool {
		self.finalized.load(Ordering::Acquire)
	}

	pub fn mark_finalized(&self) {
		self.finalized.store(true, Ordering::Release);
	}

	pub fn estimated_bytes(&self) -> u64 {
		self.estimated_bytes
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatchupSnapshot {
	pub snapshot_hlc: crate::schema::Hlc,
}

impl CatchupSnapshot {
	pub const fn new(snapshot_hlc: crate::schema::Hlc) -> Self {
		Self { snapshot_hlc }
	}

	pub const fn ceiling(&self) -> CdcCursor {
		CdcCursor::ceiling_for_hlc(self.snapshot_hlc)
	}
}

#[derive(Debug)]
pub struct CdcLocalityFrontiers {
	inner: RwLock<HashMap<Locality, crate::schema::Hlc>>,
	advance_notify: Notify,
}

impl Default for CdcLocalityFrontiers {
	fn default() -> Self {
		Self {
			inner: RwLock::new(HashMap::new()),
			advance_notify: Notify::new(),
		}
	}
}

impl CdcLocalityFrontiers {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn advance(&self, locality_id: Locality, hlc: crate::schema::Hlc) -> crate::schema::Hlc {
		let (next, advanced) = {
			let mut inner = self
				.inner
				.write()
				.unwrap_or_else(|poisoned| poisoned.into_inner());
			let previous = inner.get(&locality_id).copied();
			let next = previous.map_or(hlc, |current| current.max(hlc));
			if previous != Some(next) {
				inner.insert(locality_id, next);
				(next, true)
			} else {
				(next, false)
			}
		};
		if advanced {
			self.advance_notify.notify_waiters();
		}
		next
	}

	pub async fn wait_for_advance(&self) {
		self.advance_notify.notified().await;
	}

	pub fn get(&self, locality_id: Locality) -> Option<crate::schema::Hlc> {
		self.inner
			.read()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.get(&locality_id)
			.copied()
	}

	pub fn min_for_localities<I>(&self, locality_ids: I) -> Option<crate::schema::Hlc>
	where
		I: IntoIterator<Item = Locality>,
	{
		let inner = self
			.inner
			.read()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut min_hlc: Option<crate::schema::Hlc> = None;
		let mut saw_locality = false;
		for locality_id in locality_ids {
			saw_locality = true;
			let hlc = *inner.get(&locality_id)?;
			min_hlc = Some(min_hlc.map_or(hlc, |current| current.min(hlc)));
		}
		saw_locality.then_some(min_hlc).flatten()
	}

	pub fn snapshot(&self) -> Vec<(Locality, crate::schema::Hlc)> {
		let mut snapshot: Vec<_> = self
			.inner
			.read()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.iter()
			.map(|(locality_id, hlc)| (*locality_id, *hlc))
			.collect();
		snapshot.sort_by_key(|(locality_id, _)| *locality_id);
		snapshot
	}
}

#[derive(Debug)]
struct CdcLiveTailRingInner {
	bytes_used: u64,
	entries: VecDeque<Arc<CdcLiveEntry>>,
	eviction_frontiers: HashMap<(TenantId, TableObjectId), CdcCursor>,
}

#[derive(Debug)]
pub struct CdcLiveTailRing {
	locality_id: Locality,
	bytes_cap: u64,
	inner: Mutex<CdcLiveTailRingInner>,
}

impl CdcLiveTailRing {
	pub fn new(locality_id: Locality, bytes_cap: u64) -> Self {
		Self {
			locality_id,
			bytes_cap,
			inner: Mutex::new(CdcLiveTailRingInner {
				bytes_used: 0,
				entries: VecDeque::new(),
				eviction_frontiers: HashMap::new(),
			}),
		}
	}

	pub fn locality_id(&self) -> Locality {
		self.locality_id
	}

	pub fn bytes_cap(&self) -> u64 {
		self.bytes_cap
	}

	pub async fn push(&self, entry: CdcLiveEntry) -> Vec<Arc<CdcLiveEntry>> {
		self.push_arc(Arc::new(entry)).await
	}

	pub async fn push_arc(&self, entry: Arc<CdcLiveEntry>) -> Vec<Arc<CdcLiveEntry>> {
		let mut inner = self.inner.lock().await;
		inner.bytes_used = inner.bytes_used.saturating_add(entry.estimated_bytes());
		inner.entries.push_back(entry);

		let mut evicted = Vec::new();
		while inner.bytes_used > self.bytes_cap {
			let Some(oldest) = inner.entries.pop_front() else {
				inner.bytes_used = 0;
				break;
			};
			inner.bytes_used = inner.bytes_used.saturating_sub(oldest.estimated_bytes());
			inner
				.eviction_frontiers
				.entry((oldest.tenant_id, oldest.table_object_id))
				.and_modify(|frontier| *frontier = (*frontier).max(oldest.cursor))
				.or_insert(oldest.cursor);
			evicted.push(oldest);
			metrics().observe_cdc_ring_overflow();
		}
		metrics().set_cdc_staged_entries_in_ring(inner.entries.len() as u64);
		evicted
	}

	pub async fn head_cursor(&self) -> Option<CdcCursor> {
		self.inner.lock().await.entries.front().map(|e| e.cursor)
	}

	pub async fn tail_cursor(&self) -> Option<CdcCursor> {
		self.inner.lock().await.entries.back().map(|e| e.cursor)
	}

	pub async fn len(&self) -> usize {
		self.inner.lock().await.entries.len()
	}

	pub async fn is_empty(&self) -> bool {
		self.len().await == 0
	}

	pub async fn bytes_used(&self) -> u64 {
		self.inner.lock().await.bytes_used
	}

	pub async fn mark_finalized(&self, cursor: CdcCursor) -> bool {
		self.mark_finalized_entry(cursor).await.is_some()
	}

	pub async fn mark_finalized_entry(&self, cursor: CdcCursor) -> Option<Arc<CdcLiveEntry>> {
		let inner = self.inner.lock().await;
		if let Some(entry) = inner.entries.iter().find(|entry| entry.cursor == cursor) {
			entry.mark_finalized();
			Some(entry.clone())
		} else {
			None
		}
	}

	pub async fn evict_cursor(&self, cursor: CdcCursor) -> Option<Arc<CdcLiveEntry>> {
		let mut inner = self.inner.lock().await;
		let index = inner
			.entries
			.iter()
			.position(|entry| entry.cursor == cursor)?;
		let entry = inner
			.entries
			.remove(index)
			.expect("cdc ring index from position must be valid");
		inner.bytes_used = inner.bytes_used.saturating_sub(entry.estimated_bytes());
		metrics().set_cdc_staged_entries_in_ring(inner.entries.len() as u64);
		Some(entry)
	}

	pub async fn evicted_cursor_frontier(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<CdcCursor> {
		self.inner
			.lock()
			.await
			.eviction_frontiers
			.get(&(tenant_id, table_object_id))
			.copied()
	}

	pub async fn eligible_entries_after(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		after: CdcCursor,
		safe_emission_hlc: crate::schema::Hlc,
		limit: usize,
	) -> Vec<Arc<CdcLiveEntry>> {
		let inner = self.inner.lock().await;
		let mut entries: Vec<_> = inner
			.entries
			.iter()
			.filter(|entry| {
				entry.tenant_id == tenant_id
					&& entry.table_object_id == table_object_id
					&& entry.cursor > after
					&& entry.cursor.row_hlc <= safe_emission_hlc
					&& entry.is_finalized()
			})
			.take(limit)
			.cloned()
			.collect();
		entries.sort_by_key(|entry| entry.cursor);
		entries
	}

	pub async fn open_time_ring_continuity_check(
		&self,
		snapshot: &CatchupSnapshot,
	) -> Result<(), CdcRejectCode> {
		let Some(head) = self.head_cursor().await else {
			return Ok(());
		};
		if head > snapshot.ceiling() {
			Err(CdcRejectCode::RingOverflowDuringCatchup)
		} else {
			Ok(())
		}
	}
}

#[derive(Debug)]
pub struct CdcFinalizationObserver {
	ring: Arc<CdcLiveTailRing>,
	pending_index: Mutex<HashMap<FinalizationLogIndex, Vec<CdcCursor>>>,
}

impl CdcFinalizationObserver {
	pub fn new(ring: Arc<CdcLiveTailRing>) -> Self {
		Self {
			ring,
			pending_index: Mutex::new(HashMap::new()),
		}
	}

	pub async fn stage(&self, finalization_log_index: FinalizationLogIndex, cursor: CdcCursor) {
		self.pending_index
			.lock()
			.await
			.entry(finalization_log_index)
			.or_default()
			.push(cursor);
	}

	pub async fn on_finalized(&self, finalization_log_index: FinalizationLogIndex) -> usize {
		let cursors = self
			.pending_index
			.lock()
			.await
			.remove(&finalization_log_index)
			.unwrap_or_default();
		let mut finalized = 0;
		for cursor in cursors {
			if self.ring.mark_finalized(cursor).await {
				finalized += 1;
			} else {
				metrics().observe_cdc_pending_finalization_evicted();
			}
		}
		finalized
	}

	pub async fn on_terminal_failure(&self, finalization_log_index: FinalizationLogIndex) -> usize {
		let cursors = self
			.pending_index
			.lock()
			.await
			.remove(&finalization_log_index)
			.unwrap_or_default();
		let mut evicted = 0;
		for cursor in cursors {
			let _ = self.ring.evict_cursor(cursor).await;
			metrics().observe_cdc_pending_finalization_evicted();
			evicted += 1;
		}
		evicted
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CdcLocalityHeartbeatWorker {
	pub locality_id: Locality,
	pub interval: Duration,
}

impl CdcLocalityHeartbeatWorker {
	pub fn new(locality_id: Locality, interval: Duration) -> Self {
		Self {
			locality_id,
			interval,
		}
	}

	pub async fn run<Q, QF, E, EF>(
		self,
		mut locality_quiet_since_last_finalization: Q,
		mut emit_heartbeat_finalization_marker: E,
	) -> std::io::Result<()>
	where
		Q: FnMut(Locality) -> QF,
		QF: Future<Output = bool>,
		E: FnMut(Locality) -> EF,
		EF: Future<Output = std::io::Result<()>>,
	{
		let mut ticker = interval(self.interval);
		loop {
			ticker.tick().await;
			if locality_quiet_since_last_finalization(self.locality_id).await {
				emit_heartbeat_finalization_marker(self.locality_id).await?;
			}
		}
	}
}

fn estimate_record_batch_bytes(batch: &RecordBatch) -> u64 {
	let rows = batch.num_rows() as u64;
	let columns = batch.num_columns() as u64;
	let floor = rows.saturating_mul(columns.max(1)).saturating_mul(8);
	u64::try_from(batch.get_array_memory_size())
		.unwrap_or(u64::MAX)
		.max(floor)
}
