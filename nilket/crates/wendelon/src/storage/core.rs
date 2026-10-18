//! Slab storage — append-only record log on local NVMe.
//!
//! This file owns the [`StorageEngine`] trait surface plus
//! [`LocalSlabStore`], a concrete implementation that lays down
//! records into an `O_DIRECT`-opened slab file via the dedicated
//! `io_uring` thread from [`crate::storage::io_uring`].
//!
//! ## On-disk layout
//!
//! One record is the concatenation:
//!
//! ```text
//! [SlabRecordHeader : 72B] [ HRFT frame bytes : frame_len ] [ zero pad -> 4K ]
//! ```
//!
//! The zero pad exists only because `O_DIRECT` requires every I/O
//! (offset, length, buffer pointer) to be page-aligned. The header's
//! `frame_len` is the source of truth for the record's payload size;
//! the pad is invisible to readers.
//!
//! ## Concurrency
//!
//! [`append`] runs under `&self` — multiple tasks may append in
//! parallel. Each call atomically reserves a disjoint byte range via
//! `next_offset.fetch_add`, then issues an independent write at that
//! offset. Because `O_DIRECT` writes at distinct, page-aligned
//! offsets never alias, no additional locking is required. The index
//! ([`DashMap`]) is independently lock-free.
//!
//! ## Multi-version index (ADR 013)
//!
//! Every record identity maps to a `Vec<RecordLocation>` sorted ascending by
//! [`HybridTimestamp`]. A fresh mutation pushes a new version; old
//! versions stay put so the read path can serve "AS OF" queries by
//! binary-searching for the newest version whose `hlc <=
//! as_of_hlc`. `lookup` returns the tail — the latest version — so
//! present-day reads pay the same O(1) cost they did before the
//! refactor. Retention is a separate knob: compaction does not drop
//! versions while ADR 013's `RETENTION_WINDOW` still covers them,
//! which is enforced by the background sweeper rather than this
//! file.
//!
//! ## What this file does NOT do yet
//!
//! * Slab rotation / sealing when the active slab hits a size cap.
//! * Crash recovery: scanning the file on open to rebuild
//!   [`index`](LocalSlabStore::index) and restore `next_offset`.
//! * Remote tier integration (S3 stubs, lazy migration).
//! * Cross-slab rotation / sealing policy (lives in
//!   [`SlabManager`](crate::storage::manager::SlabManager)).
//! * Remote tier integration (S3 stubs, lazy migration).
//!
//! [`append`]: LocalSlabStore::append

use crate::metrics::global as metrics;
use crate::schema::table::PlacementSemantics;
use crate::schema::{
	CounterCommitFrame, CounterTentativeFrame, HybridTimestamp, Identity, Locality, PartitionId,
	RecordRowIndex, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, SlabId, SlabRecordHeader, TenantId,
};
use crate::storage::io_uring::{AlignedBuf, DIRECT_IO_ALIGN};
use crate::storage::{AsyncFileOps, FileHandle, OpenFlags};
use arrow::array::{BooleanArray, FixedSizeBinaryArray, UInt64Array};
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::{Cursor, Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};
use tokio::time::Instant;

/// Slab record preamble magic = ASCII "SLAB".
const SLAB_RECORD_MAGIC: u32 = 0x534C4142;

/// A pointer to a record's location on disk.
///
/// Identity is the PHYSICAL record: two locations naming the same bytes are the
/// same location, whatever placement metadata each copy carries. Placement is
/// an attribute that rides along for route selection; including it in `Eq`/
/// `Hash` silently splits map keys between registration and reconstruction —
/// reference counts then add under one key and remove under another, and a
/// slab still holding live rows can be retired.
#[derive(Clone, Copy, Debug)]
pub struct RecordLocation {
	pub slab_id: SlabId,
	pub offset: u64,
	pub length: u32,
	pub hlc: HybridTimestamp,
	pub record_row_index: RecordRowIndex,
	pub is_tombstone: bool,
	/// ADR 0090: the placement semantics this record was written under. Route
	/// selection consults this BEFORE the slab is read — post-decode validation
	/// detects a bad destination too late. Loaded from the V5 persistent index
	/// on recovery; `RawLocality` for every record written before partitions.
	pub placement: PlacementSemantics,
	/// Set iff `placement` is `Partitioned`.
	pub partition: Option<PartitionId>,
}

impl PartialEq for RecordLocation {
	fn eq(&self, other: &Self) -> bool {
		self.slab_id == other.slab_id
			&& self.offset == other.offset
			&& self.length == other.length
			&& self.hlc == other.hlc
			&& self.record_row_index == other.record_row_index
			&& self.is_tombstone == other.is_tombstone
	}
}

impl Eq for RecordLocation {}

impl std::hash::Hash for RecordLocation {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.slab_id.hash(state);
		self.offset.hash(state);
		self.length.hash(state);
		self.hlc.hash(state);
		self.record_row_index.hash(state);
		self.is_tombstone.hash(state);
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MultiRowIndexEntry {
	pub key_identity: Identity,
	pub routing_locality: Locality,
	pub row_hlc: HybridTimestamp,
	pub record_row_index: RecordRowIndex,
	pub is_tombstone: bool,
	/// ADR 0090: the row's durable partition; `None` is a raw-locality row.
	pub partition: Option<PartitionId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredSlabEntry {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub routing_locality: Locality,
	pub routing_locality_trusted: bool,
	pub location: RecordLocation,
}

type TenantKey = (TenantId, Identity);

/// ADR 016 group-commit tuning knobs for slab durability.
///
/// The window shape is:
/// `clamp(inflight_writes * per_write_cost_estimate, floor, ceiling)`.
///
/// Defaults currently ship as:
/// - floor: 200 µs
/// - ceiling: 2 ms
/// - per-write estimate: 100 µs
///
/// The upper bound is therefore documented and enforced in code:
/// a normal data-path write waits at most 2 ms in the coalescing
/// queue before the slab issues `fdatasync`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupCommitConfig {
	pub floor: Duration,
	pub ceiling: Duration,
	pub per_write_cost_estimate: Duration,
}

impl Default for GroupCommitConfig {
	fn default() -> Self {
		Self {
			floor: Duration::from_micros(200),
			ceiling: Duration::from_millis(2),
			per_write_cost_estimate: Duration::from_micros(100),
		}
	}
}

impl GroupCommitConfig {
	fn window_for_pending(&self, pending_writes: usize) -> Duration {
		let scaled = self
			.per_write_cost_estimate
			.checked_mul(pending_writes.min(u32::MAX as usize) as u32)
			.unwrap_or(self.ceiling);
		scaled.clamp(self.floor, self.ceiling)
	}
}

struct GroupCommitCoordinator {
	config: GroupCommitConfig,
	state: Mutex<GroupCommitState>,
}

#[derive(Default)]
struct GroupCommitState {
	pending: Vec<oneshot::Sender<std::io::Result<()>>>,
	driver_running: bool,
}

/// The unified storage interface for Wendelon.
/// Abstracts between local NVMe slabs and remote object storage (S3).
#[async_trait]
pub trait StorageEngine: Send + Sync {
	/// Appends a raw HRFT frame to the current active slab.
	/// Returns the location of the record upon successful disk ack.
	async fn append(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation>;

	/// Reads a record from disk based on its location.
	async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>>;

	/// Looks up a key's location in the in-memory index.
	/// Returns the latest version, equivalent to
	/// [`Self::lookup_as_of`] with the sentinel zero HLC.
	fn lookup(&self, tenant_id: TenantId, key_identity: Identity) -> Option<RecordLocation>;

	/// Time-travel lookup (ADR 013). Returns the newest version
	/// whose `hlc <= as_of_hlc`. A zero-valued `as_of_hlc` is the
	/// sentinel for "no upper bound" and falls through to the same
	/// behaviour as [`Self::lookup`].
	fn lookup_as_of(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		as_of_hlc: HybridTimestamp,
	) -> Option<RecordLocation>;
}

/// A page-aligned byte range one slab has set aside for one framed record.
///
/// Admission to a slab and the byte reservation that admission is judged
/// against must be a single operation, or two writers can both pass the size
/// cap against one cursor. The manager therefore reserves under its lock and
/// hands the range to the writer as this token. It is deliberately not
/// `Clone`: [`LocalSlabStore::write_reserved`] consumes it, so a range is
/// written at most once. The slab id it carries lets the write refuse a token
/// taken on another slab.
#[derive(Debug)]
pub struct AppendReservation {
	slab_id: SlabId,
	offset: u64,
	payload_size: usize,
	padded_size: usize,
}

impl AppendReservation {
	pub(crate) fn offset(&self) -> u64 {
		self.offset
	}

	/// Bytes the slab's cursor advanced for this record: header, frame, and
	/// page padding.
	pub fn padded_size(&self) -> usize {
		self.padded_size
	}
}

/// A high-performance storage implementation using io_uring and O_DIRECT.
pub struct LocalSlabStore {
	/// In-memory multi-version index: Identity -> versions sorted
	/// ascending by HLC. The tail is the latest version; older
	/// entries are retained for time-travel reads (ADR 013).
	pub index: DashMap<TenantKey, Vec<RecordLocation>>,
	/// Counter eligibility observed only by local marker replay, before live tentative appends.
	pub(crate) recovered_counter_versions:
		Vec<(TenantId, Identity, super::counter::CounterVersion)>,
	/// V2 counter tentatives are paired across the manager's complete local slab set.
	pub(crate) recovered_counter_tentatives_v2: Vec<RecoveredCounterTentativeV2>,
	/// V2 commit markers are paired across the manager's complete local slab set.
	pub(crate) recovered_counter_markers_v2: Vec<RecoveredCounterMarkerV2>,
	pub(crate) recovered_non_counter_keys: Vec<TenantKey>,
	counter_records: std::sync::atomic::AtomicBool,
	pub(crate) recovered_counter_errors:
		Vec<(TenantId, Identity, super::counter::CounterVisibilityError)>,

	/// Identity of the active slab file. Written into every
	/// [`RecordLocation`] so reads can route to the right file once
	/// multiple slabs are live.
	slab_id: SlabId,

	/// Routing localities recovered from storage-only synthetic
	/// columns. Live writes publish locality through `SlabManager`;
	/// this map exists so restart recovery does not need table
	/// descriptor context to rebuild the manager's locality index.
	pub(crate) recovered_localities: DashMap<TenantKey, Locality>,

	/// Owning handle to the slab file.
	file: FileHandle,

	/// Data-path I/O seam over the underlying io_uring thread.
	file_ops: Arc<dyn AsyncFileOps>,

	/// Per-slab durability coalescer for ADR 016 group commit.
	group_commit: GroupCommitCoordinator,

	/// Monotonic write cursor into the slab file. Callers reserve
	/// disjoint byte ranges with `fetch_add`, so concurrent appends
	/// never collide.
	next_offset: AtomicU64,

	#[cfg(test)]
	fdatasync_calls: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RecoveredCounterTentativeV2 {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub location: RecordLocation,
	pub value: u64,
	pub origin: crate::schema::NodeId,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RecoveredCounterMarkerV2 {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub location: RecordLocation,
	pub tentative_offset: u64,
	pub origin: crate::schema::NodeId,
}

/// One validated counter-class frame observed by a read-only maintenance walk.
/// Ordinary frames are checked while walking but are not retained here.
#[derive(Clone, Debug)]
pub(crate) struct ScannedCounterFrame {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub location: RecordLocation,
	pub record_class: u32,
	pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CounterMaintenanceClass {
	TentativeV2,
	CommitMarkerV2,
}
impl CounterMaintenanceClass {
	fn record_class(self) -> u32 {
		match self {
			Self::TentativeV2 => crate::schema::record_class::COUNTER_TENTATIVE_V2,
			Self::CommitMarkerV2 => crate::schema::record_class::COUNTER_COMMIT_V2,
		}
	}
}

impl LocalSlabStore {
	/// Open (or create) a slab file at `path`, binding it to the
	/// supplied file-ops seam. The production manager passes an
	/// `io_uring`-backed implementation; restore and test flows may
	/// use a blocking adapter.
	///
	/// The file is opened with `O_DIRECT`, which is why every
	/// submission inside [`append`](Self::append) / [`read`](Self::read)
	/// uses [`AlignedBuf`] and rounds up to a page boundary.
	pub async fn open(
		path: impl AsRef<Path>,
		slab_id: SlabId,
		file_ops: Arc<dyn AsyncFileOps>,
		group_commit: GroupCommitConfig,
	) -> std::io::Result<Self> {
		Self::open_with_acknowledged(path, slab_id, file_ops, group_commit, None).await
	}

	/// Open a slab whose acknowledged record offsets are known from the
	/// durable index. Recovery resumes past anomalies at those offsets and
	/// leaves the file canonical; see [`recover_slab`] (ADR 0099).
	pub async fn open_with_acknowledged(
		path: impl AsRef<Path>,
		slab_id: SlabId,
		file_ops: Arc<dyn AsyncFileOps>,
		group_commit: GroupCommitConfig,
		acknowledged: Option<&BTreeSet<u64>>,
	) -> std::io::Result<Self> {
		let file = file_ops
			.open(
				path.as_ref(),
				OpenFlags {
					read: true,
					write: true,
					create: true,
					direct: true,
					..Default::default()
				},
			)
			.await?;

		// Scan any existing records so a restart resumes writing
		// past the last complete record and serves reads immediately.
		let (
			index,
			recovered_localities,
			write_cursor,
			recovered_counter_versions,
			recovered_counter_tentatives_v2,
			recovered_counter_markers_v2,
			recovered_non_counter_keys,
			recovered_counter_errors,
			has_counter_records,
		) = recover_slab(&file, slab_id, acknowledged)?;

		Ok(Self {
			index,
			recovered_counter_versions,
			recovered_counter_tentatives_v2,
			recovered_counter_markers_v2,
			recovered_non_counter_keys,
			recovered_counter_errors,
			counter_records: std::sync::atomic::AtomicBool::new(has_counter_records),
			recovered_localities,
			slab_id,
			file,
			file_ops,
			group_commit: GroupCommitCoordinator {
				config: group_commit,
				state: Mutex::new(GroupCommitState::default()),
			},
			next_offset: AtomicU64::new(write_cursor),
			#[cfg(test)]
			fdatasync_calls: AtomicU64::new(0),
		})
	}

	/// Current write cursor. Exposed for tests and recovery glue.
	/// Inspect the class under the location's checksum; payload shape is never authority.
	pub(crate) async fn counter_record_supported(
		&self,
		tenant: TenantId,
		key: Identity,
		location: RecordLocation,
	) -> std::io::Result<bool> {
		if location.slab_id != self.slab_id {
			return Err(super::counter::CounterFormatError::InvalidOriginFrame.io());
		}
		let buf = AlignedBuf::new(page_round_up(location.length as usize));
		let (read, buf) = self
			.file_ops
			.read_at(&self.file, location.offset, buf)
			.await?;
		let size = std::mem::size_of::<SlabRecordHeader>();
		if read < size {
			return Err(super::counter::CounterFormatError::InvalidOriginFrame.io());
		}
		let bytes = &buf.as_slice()[..read];
		let header: &SlabRecordHeader = bytemuck::from_bytes(&bytes[..size]);
		let end = size
			.checked_add(header.frame_len as usize)
			.filter(|end| *end <= read)
			.ok_or_else(|| super::counter::CounterFormatError::InvalidOriginFrame.io())?;
		if header.magic != SLAB_RECORD_MAGIC
			|| header.tenant_id != tenant
			|| header.key_identity != key
		{
			return Err(super::counter::CounterFormatError::InvalidOriginFrame.io());
		}
		verify_record_checksum(header, &bytes[size..end])?;
		match header.record_class {
			crate::schema::record_class::COUNTER_TENTATIVE => Ok(bytes[size..end].len() >= 8),
			crate::schema::record_class::COUNTER_TENTATIVE_V2 => {
				super::counter::decode_origin_frame(&bytes[size..end])?;
				Ok(true)
			}
			_ => Ok(false),
		}
	}

	pub(crate) fn refuse_counter_maintenance(&self) -> std::io::Result<()> {
		if self.counter_records.load(Ordering::Acquire) {
			return Err(super::counter::CounterFormatError::MaintenanceUnsupported.io());
		}
		Ok(())
	}

	/// Append one of the V2 counter frame kinds during a validated single-slab
	/// maintenance rewrite. This deliberately does not expose an arbitrary class
	/// parameter and does not index the frame; callers publish the tentative only
	/// when its source was indexed.
	pub(crate) async fn append_counter_maintenance_frame(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		class: CounterMaintenanceClass,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let expected = match class {
			CounterMaintenanceClass::TentativeV2 => 16,
			CounterMaintenanceClass::CommitMarkerV2 => 16,
		};
		if frame.len() != expected {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"V2 maintenance counter frame has an invalid payload length",
			));
		}
		super::counter::decode_origin_frame(frame)?;
		self.write_framed(tenant_id, key_identity, hlc, class.record_class(), frame)
			.await
	}

	/// Walk the complete physical slab without changing bytes, indexes, or recovery
	/// state. Every header, frame length, and checksum is validated before the next
	/// page is visited; only counter frames are retained in memory. Maintenance
	/// callers hold the slab's exclusive rewrite claim, so the write cursor is a
	/// stable upper bound for this walk.
	pub(crate) async fn scan_counter_frames(&self) -> std::io::Result<Vec<ScannedCounterFrame>> {
		let end = self.write_cursor();
		if end % DIRECT_IO_ALIGN as u64 != 0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"slab write cursor is not page aligned",
			));
		}
		let header_size = std::mem::size_of::<SlabRecordHeader>();
		let mut offset = 0_u64;
		let mut counters = Vec::new();
		while offset < end {
			let probe = AlignedBuf::new(DIRECT_IO_ALIGN);
			let (probe_read, probe) = self.file_ops.read_at(&self.file, offset, probe).await?;
			if probe_read < header_size {
				return Err(Error::new(
					ErrorKind::UnexpectedEof,
					"maintenance frame walk reached a short record header",
				));
			}
			let header: SlabRecordHeader =
				*bytemuck::from_bytes::<SlabRecordHeader>(&probe.as_slice()[..header_size]);
			if header.magic != SLAB_RECORD_MAGIC {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("maintenance frame walk found invalid magic at offset {offset}"),
				));
			}
			let payload_size = header_size
				.checked_add(header.frame_len as usize)
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame length overflow"))?;
			let padded_size = page_round_up(payload_size);
			let next = offset
				.checked_add(padded_size as u64)
				.filter(|next| *next <= end)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!("maintenance frame at offset {offset} exceeds the slab cursor"),
					)
				})?;
			let frame_bytes = if padded_size <= DIRECT_IO_ALIGN {
				probe.as_slice()[header_size..payload_size].to_vec()
			} else {
				let full = AlignedBuf::new(padded_size);
				let (read, full) = self.file_ops.read_at(&self.file, offset, full).await?;
				if read < payload_size {
					return Err(Error::new(
						ErrorKind::UnexpectedEof,
						"maintenance frame walk reached a short record payload",
					));
				}
				full.as_slice()[header_size..payload_size].to_vec()
			};
			verify_record_checksum(&header, &frame_bytes)?;
			if header.record_class != crate::schema::record_class::STANDARD {
				let location = RecordLocation {
					slab_id: self.slab_id,
					offset,
					length: payload_size as u32,
					hlc: header.hlc,
					record_row_index: RecordRowIndex::ZERO,
					is_tombstone: false,
					placement: PlacementSemantics::RawLocality,
					partition: None,
				};
				counters.push(ScannedCounterFrame {
					tenant_id: header.tenant_id,
					key_identity: header.key_identity,
					location,
					record_class: header.record_class,
					payload: frame_bytes,
				});
			}
			offset = next;
		}
		if self.write_cursor() != end {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"slab write cursor changed during maintenance frame walk",
			));
		}
		Ok(counters)
	}

	pub fn write_cursor(&self) -> u64 {
		self.next_offset.load(Ordering::Acquire)
	}

	/// Identity of the slab this store owns. Used by [`SlabManager`]
	/// to route reads after rotation.
	///
	/// [`SlabManager`]: crate::storage::manager::SlabManager
	pub fn slab_id(&self) -> SlabId {
		self.slab_id
	}

	/// Immediate durability barrier for administrative paths.
	///
	/// This bypasses ADR 016's coalescing window and issues a full
	/// `fsync(2)` immediately. The manager uses it for explicit
	/// flush-now boundaries such as migration / backup barriers.
	///
	/// ## Ordering contract (important)
	///
	/// Sync CQEs are **not** ordered against in-flight write CQEs
	/// in `io_uring`. Callers therefore must `.await` the
	/// [`append`](Self::append) futures they care about *before*
	/// calling `fsync` — `append` already awaits the write CQE, so
	/// any append whose future has resolved before the sync call is
	/// covered.
	pub async fn fsync(&self) -> std::io::Result<()> {
		self.file_ops.fsync(&self.file).await
	}

	/// ADR 016 data-path durability barrier.
	///
	/// Concurrent callers queue behind a single per-slab driver that
	/// waits for the adaptive coalescing window, then issues one
	/// `fdatasync(2)` for the whole batch. New callers that arrive
	/// during the open window can extend it up to the configured
	/// ceiling; callers that arrive after the flush starts form the
	/// next batch.
	pub async fn wait_durable_commit(&self) -> std::io::Result<()> {
		let (rx, run_driver) = {
			let mut state = self.group_commit.state.lock().await;
			let (tx, rx) = oneshot::channel();
			state.pending.push(tx);
			let run_driver = if state.driver_running {
				false
			} else {
				state.driver_running = true;
				true
			};
			(rx, run_driver)
		};

		if run_driver {
			self.run_group_commit_driver().await;
		}

		rx.await.map_err(|_| {
			Error::new(
				ErrorKind::BrokenPipe,
				"group-commit waiter dropped before durability result arrived",
			)
		})?
	}

	async fn run_group_commit_driver(&self) {
		loop {
			let batch_started = Instant::now();
			loop {
				let target = {
					let state = self.group_commit.state.lock().await;
					if state.pending.is_empty() {
						drop(state);
						let mut state = self.group_commit.state.lock().await;
						state.driver_running = false;
						return;
					}
					self.group_commit
						.config
						.window_for_pending(state.pending.len())
				};
				let elapsed = batch_started.elapsed();
				if elapsed >= target {
					metrics()
						.group_commit_window_seconds
						.observe_duration(elapsed);
					break;
				}
				tokio::time::sleep(target - elapsed).await;
			}

			let waiters = {
				let mut state = self.group_commit.state.lock().await;
				if state.pending.is_empty() {
					state.driver_running = false;
					return;
				}
				std::mem::take(&mut state.pending)
			};

			#[cfg(test)]
			self.fdatasync_calls.fetch_add(1, Ordering::Relaxed);

			let result = self.file_ops.fdatasync(&self.file).await;
			for waiter in waiters {
				let _ = waiter.send(clone_io_result(&result));
			}

			let mut state = self.group_commit.state.lock().await;
			if state.pending.is_empty() {
				state.driver_running = false;
				return;
			}
		}
	}

	/// Remove one exact record location from the slab-local index.
	///
	/// This is a best-effort in-memory rollback used when an append
	/// completed but the caller's required durability barrier failed.
	pub(crate) fn rollback_index_entry(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		let key = (tenant_id, key_identity);
		let should_remove_key = if let Some(mut versions) = self.index.get_mut(&key) {
			let removed = remove_version_exact(&mut versions, location);
			removed && versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			self.index.remove(&key);
		}
	}

	pub(crate) fn index_location(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.index
			.entry((tenant_id, key_identity))
			.and_modify(|versions| insert_version_sorted(versions, location))
			.or_insert_with(|| vec![location]);
	}

	/// Write a framed record of `record_class` at a freshly reserved,
	/// page-aligned offset and return its on-disk location **without** touching
	/// the in-memory index.
	///
	/// Ordinary [`append`](StorageEngine::append) layers an immediate
	/// [`index_location`](Self::index_location) on top of this with
	/// `record_class == STANDARD`, so blob/Arrow records are written and
	/// indexed exactly as before. ADR 075 counter tentatives instead stay
	/// unindexed until their commit marker lands — so an uncommitted add is
	/// invisible to reads with no finalization-aware read path, and recovery
	/// applies the same committed-only rule.
	pub(crate) async fn write_framed(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		record_class: u32,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let reservation = self.reserve_append(frame.len())?;
		self.write_reserved(
			reservation,
			tenant_id,
			key_identity,
			hlc,
			record_class,
			frame,
		)
		.await
	}

	/// The on-disk footprint of a frame: the header-inclusive payload size and
	/// the page-rounded size the slab actually reserves for it. Rotation
	/// decisions and reservations must agree on this number, so both read it
	/// from here.
	pub(crate) fn padded_record_size(frame_len: usize) -> std::io::Result<(usize, usize)> {
		let header_size = std::mem::size_of::<SlabRecordHeader>();
		let payload_size = header_size
			.checked_add(frame_len)
			.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "frame length overflows usize"))?;
		Ok((payload_size, page_round_up(payload_size)))
	}

	/// Reserve a disjoint, page-aligned byte range for a frame of `frame_len`
	/// bytes. The cursor advances here, so a caller that decides admission on
	/// the cursor and reserves in the same critical section sees every earlier
	/// reservation, including ones whose write has not landed yet.
	///
	/// A reservation that is never written leaves a zeroed hole exactly like a
	/// failed write does today: recovery stops at the first page without a
	/// record magic. Consume it with [`write_reserved`](Self::write_reserved)
	/// on the same task that took it.
	pub(crate) fn reserve_append(&self, frame_len: usize) -> std::io::Result<AppendReservation> {
		let (payload_size, padded_size) = Self::padded_record_size(frame_len)?;
		let offset = self
			.next_offset
			.fetch_add(padded_size as u64, Ordering::AcqRel);
		Ok(AppendReservation {
			slab_id: self.slab_id,
			offset,
			payload_size,
			padded_size,
		})
	}

	/// Write a STANDARD frame into `reservation` and index it, the reserved
	/// counterpart of [`append`](StorageEngine::append).
	pub(crate) async fn append_reserved(
		&self,
		reservation: AppendReservation,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		let location = self
			.write_reserved(
				reservation,
				tenant_id,
				key_identity,
				hlc,
				crate::schema::record_class::STANDARD,
				frame,
			)
			.await?;
		self.index_location(tenant_id, key_identity, location);
		Ok(location)
	}

	/// Write a framed record of `record_class` into a range this slab reserved
	/// for a frame of exactly this length. The reservation is consumed by value,
	/// so a range is written at most once; a reservation taken on another slab
	/// or for a different frame length is refused before any byte is written.
	pub(crate) async fn write_reserved(
		&self,
		reservation: AppendReservation,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		record_class: u32,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		// Conservative metadata: failed counter writes still refuse maintenance.
		if matches!(record_class, 1..=4) {
			self.counter_records.store(true, Ordering::Release);
		}
		let header_size = std::mem::size_of::<SlabRecordHeader>();
		let AppendReservation {
			slab_id,
			offset,
			payload_size,
			padded_size,
		} = reservation;
		if slab_id != self.slab_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"append reservation belongs to another slab",
			));
		}
		if payload_size != header_size + frame.len() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"append reservation was sized for a different frame",
			));
		}

		// Build the payload: header (checksum=0) || frame, then patch the
		// checksum over (header-with-zero-checksum || frame). `record_class`
		// occupies the slot that was `_pad_align` zero-fill, so a STANDARD
		// record is byte-for-byte identical to a pre-ADR-075 record.
		let mut buf = AlignedBuf::new(padded_size);
		{
			let dst = buf.as_mut_slice();
			let mut header = SlabRecordHeader {
				magic: SLAB_RECORD_MAGIC,
				record_class,
				tenant_id,
				key_identity,
				hlc,
				frame_len: frame.len() as u32,
				checksum: 0,
			};
			dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
			dst[header_size..header_size + frame.len()].copy_from_slice(frame);
			let checksum = compute_checksum(&dst[..header_size + frame.len()]);
			header.checksum = checksum;
			dst[..header_size].copy_from_slice(bytemuck::bytes_of(&header));
		}

		// Submit the write on its own task so that dropping this future
		// mid-flight cannot strand a partly written range: the task runs
		// to completion and, on a failure or short write, zeroes the whole
		// reservation before reporting (ADR 0099 decision 2). A range that
		// was never submitted is zero by construction.
		let file_ops = self.file_ops.clone();
		let file = self.file.clone();
		let slab_id = self.slab_id;
		let write = tokio::spawn(async move {
			let outcome = match file_ops.write_at(&file, offset, buf).await {
				Ok((written, _buf)) if written >= payload_size => return Ok(()),
				Ok((written, _buf)) => Err(Error::new(
					ErrorKind::WriteZero,
					format!("short write on slab append: {written} of {payload_size} bytes"),
				)),
				Err(err) => Err(err),
			};
			let zeros = AlignedBuf::new(padded_size);
			match file_ops.write_at(&file, offset, zeros).await {
				Ok((written, _)) if written >= padded_size => {}
				Ok((written, _)) => tracing::error!(
					slab_id = slab_id.get(),
					offset,
					padded_size,
					written,
					"failed to zero a failed append's reservation; the slab is no longer canonical"
				),
				Err(err) => tracing::error!(
					slab_id = slab_id.get(),
					offset,
					padded_size,
					error = %err,
					"failed to zero a failed append's reservation; the slab is no longer canonical"
				),
			}
			outcome
		});
		write.await.map_err(|err| {
			Error::new(ErrorKind::Other, format!("slab append task failed: {err}"))
		})??;

		Ok(RecordLocation {
			slab_id: self.slab_id,
			offset,
			length: payload_size as u32,
			hlc,
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		})
	}

	#[cfg(test)]
	pub(crate) fn fdatasync_call_count(&self) -> u64 {
		self.fdatasync_calls.load(Ordering::Relaxed)
	}
}

#[async_trait]
impl StorageEngine for LocalSlabStore {
	async fn append(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frame: &[u8],
	) -> std::io::Result<RecordLocation> {
		// Ordinary blob/Arrow record: write a STANDARD frame, then index it
		// immediately. `index_location` is the same atomic per-key
		// read-modify-write on the version chain as before, so concurrent
		// appends on different HLCs for the same key cannot interleave.
		let location = self
			.write_framed(
				tenant_id,
				key_identity,
				hlc,
				crate::schema::record_class::STANDARD,
				frame,
			)
			.await?;
		self.index_location(tenant_id, key_identity, location);
		Ok(location)
	}

	async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>> {
		if location.slab_id != self.slab_id {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"record lives in slab {} but this store serves slab {}",
					location.slab_id, self.slab_id
				),
			));
		}

		let padded_size = page_round_up(location.length as usize);
		let buf = AlignedBuf::new(padded_size);
		let (bytes_read, completion_buf) = self
			.file_ops
			.read_at(&self.file, location.offset, buf)
			.await?;

		let header_size = std::mem::size_of::<SlabRecordHeader>();
		if bytes_read < header_size {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"slab read returned fewer bytes than the record header",
			));
		}
		let bytes = &completion_buf.as_slice()[..bytes_read];
		let header: &SlabRecordHeader = bytemuck::from_bytes(&bytes[..header_size]);

		if header.magic != SLAB_RECORD_MAGIC {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"slab record magic mismatch",
			));
		}
		let frame_len = header.frame_len as usize;
		let end = header_size
			.checked_add(frame_len)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame_len overflows usize"))?;
		if end > bytes.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"slab record frame_len exceeds bytes read",
			));
		}
		verify_record_checksum(header, &bytes[header_size..end])?;

		Ok(bytes[header_size..end].to_vec())
	}

	fn lookup(&self, tenant_id: TenantId, key_identity: Identity) -> Option<RecordLocation> {
		self.index
			.get(&(tenant_id, key_identity))
			.and_then(|v| v.last().copied())
	}

	fn lookup_as_of(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		as_of_hlc: HybridTimestamp,
	) -> Option<RecordLocation> {
		let versions = self.index.get(&(tenant_id, key_identity))?;
		newest_version_as_of(versions.value(), as_of_hlc)
	}
}

/// Insert `loc` into a per-key version chain, keeping the vec
/// sorted ascending by HLC. Used on both the live append path and
/// by recovery / index-drain helpers; sharing the invariant in one
/// place means the rest of the file can rely on "tail = latest".
pub(crate) fn insert_version_sorted(versions: &mut Vec<RecordLocation>, loc: RecordLocation) {
	// `binary_search_by_key` returns `Err(pos)` when the key is
	// absent. On exact-HLC collisions we still insert (after the
	// existing entry) so replays and duplicate writes land
	// deterministically rather than getting silently deduped.
	let pos = match versions.binary_search_by_key(&loc.hlc, |l| l.hlc) {
		Ok(p) => p + 1,
		Err(p) => p,
	};
	versions.insert(pos, loc);
}

/// Remove one exact location from a per-key version chain.
///
/// Returns `true` when an element was removed. Used by the durable
/// append path to roll back the in-memory index if the post-append
/// flush fails.
pub(crate) fn remove_version_exact(
	versions: &mut Vec<RecordLocation>,
	loc: RecordLocation,
) -> bool {
	if let Some(pos) = versions.iter().position(|stored| *stored == loc) {
		versions.remove(pos);
		true
	} else {
		false
	}
}

/// Shared "AS OF" resolver over an HLC-sorted version chain. The
/// zero HLC is the wire sentinel for "no upper bound", so the
/// common case (no time-travel) returns the tail exactly like
/// [`StorageEngine::lookup`].
pub(crate) fn newest_version_as_of(
	versions: &[RecordLocation],
	as_of_hlc: HybridTimestamp,
) -> Option<RecordLocation> {
	if is_zero_hlc(as_of_hlc) {
		return versions.last().copied();
	}
	// `partition_point` gives the index of the first element with
	// `hlc > as_of_hlc`. Everything before it satisfies the
	// predicate, so the last one (index - 1) is the newest
	// version eligible for this `as_of`.
	let cutoff = versions.partition_point(|l| l.hlc <= as_of_hlc);
	if cutoff == 0 {
		None
	} else {
		Some(versions[cutoff - 1])
	}
}

/// Zero-HLC sentinel check. Kept as a helper so the meaning reads
/// the same at every call site instead of open-coding the field
/// compares.
#[inline]
pub(crate) fn is_zero_hlc(hlc: HybridTimestamp) -> bool {
	hlc.physical_ms == 0 && hlc.logical == 0 && hlc.node_id == 0
}

/// Round `n` up to the next multiple of [`DIRECT_IO_ALIGN`].
#[inline]
fn page_round_up(n: usize) -> usize {
	(n + DIRECT_IO_ALIGN - 1) & !(DIRECT_IO_ALIGN - 1)
}

// ============================================================
//  RECOVERY
//
//  Called once at [`LocalSlabStore::open`] time. Synchronous on
//  purpose — open is a startup path, not a steady-state path, and
//  a sync scan avoids threading `async` through the constructor.
//
//  Algorithm:
//    Start at offset 0. Records are page-padded so each record
//    begins on a `DIRECT_IO_ALIGN` boundary. Read the first page
//    at the current cursor — the 72-byte `SlabRecordHeader` sits
//    in its prefix. If magic matches, compute the padded size,
//    insert the record into the index, and advance. If magic does
//    not match, we have reached either the end of valid data or a
//    torn write; stop and report the cursor.
//
//  Recovery trusts magic + frame_len only long enough to size the
//  read. The real integrity decision is the current blake3-prefix
//  checksum over (header-with-zero-checksum || frame), which stops
//  the scan at the first torn or corrupted trailing record.
// ============================================================

/// Rebuild a slab's index from its file.
///
/// `acknowledged` is the set of record offsets in this slab that the node
/// acknowledged durable: each was fdatasynced and then recorded in the
/// durable index before the client heard back, so the bytes at such an
/// offset were persisted in full. With it, the walk resumes past any
/// anomaly at the next acknowledged offset and zeroes what it stepped over.
/// Without it, the first anomaly ends the walk (ADR 0099).
fn recover_slab(
	file: &FileHandle,
	slab_id: SlabId,
	acknowledged: Option<&BTreeSet<u64>>,
) -> std::io::Result<(
	DashMap<TenantKey, Vec<RecordLocation>>,
	DashMap<TenantKey, Locality>,
	u64,
	Vec<(TenantId, Identity, super::counter::CounterVersion)>,
	Vec<RecoveredCounterTentativeV2>,
	Vec<RecoveredCounterMarkerV2>,
	Vec<TenantKey>,
	Vec<(TenantId, Identity, super::counter::CounterVisibilityError)>,
	bool,
)> {
	use std::os::unix::fs::FileExt;

	let file = file.try_clone()?;
	let file_size = file.metadata()?.len();
	let index: DashMap<TenantKey, Vec<RecordLocation>> = DashMap::new();
	let recovered_localities: DashMap<TenantKey, Locality> = DashMap::new();
	let mut recovered_versions = HashMap::new();
	// ADR 075 counter recovery accumulators. Counter frames are NOT indexed
	// inline: a tentative value becomes visible only if a commit marker for
	// its op-id (its durable offset) is also present in the slab. We collect
	// both during the scan, then index committed tentatives after it — so an
	// orphan tentative (rolled back, or crashed before its marker) is never
	// resurrected. This is the in-process twin of the pure `recover_counter_index`
	// rule the storage `counter` module proves.
	let mut counter_tentatives: Vec<(
		TenantId,
		Identity,
		RecordLocation,
		u64,
		Option<crate::schema::NodeId>,
	)> = Vec::new();
	let mut recovered_counter_versions = Vec::new();
	let mut recovered_counter_tentatives_v2 = Vec::new();
	let mut recovered_counter_markers_v2 = Vec::new();
	let mut recovered_non_counter_keys = Vec::new();
	let mut committed_counter_ops: HashMap<
		(TenantId, Identity, u64),
		Vec<(Option<crate::schema::NodeId>, HybridTimestamp)>,
	> = HashMap::new();
	let mut recovered_counter_errors = Vec::new();
	let mut has_counter_records = false;
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	// One page is enough for the header probe — every record
	// starts on a page boundary and the header occupies only the
	// first 64 bytes. For longer records we allocate a full-size
	// buffer on demand and re-read so the checksum can cover the
	// complete frame.
	let mut probe = AlignedBuf::new(DIRECT_IO_ALIGN);

	let mut cursor: u64 = 0;
	// Where the next append resumes: the end of the last record that
	// validated. It trails `cursor` across interior holes so a hole at the
	// tail is reused rather than preserved (ADR 0099).
	let mut valid_end: u64 = 0;
	// Regions the walk stepped over on the oracle's word. They are
	// zeroed once the walk is done so the file carries only accepted
	// records and zero pages (ADR 0099 decision 1).
	let mut skipped: Vec<(u64, u64)> = Vec::new();
	while cursor + DIRECT_IO_ALIGN as u64 <= file_size {
		file.read_exact_at(probe.as_mut_slice(), cursor)?;
		// Copy the header out of the probe page so we can re-use
		// the buffer for the same-page frame read below.
		let header: SlabRecordHeader =
			*bytemuck::from_bytes::<SlabRecordHeader>(&probe.as_slice()[..header_size]);

		// Classify the page. Anything that is not a complete record
		// with a good checksum is an anomaly, whether it is a zero
		// page, a page with bytes but no magic, a record that claims
		// to run past the end of the file, or a torn record. What an
		// anomaly means depends on whether the caller can say where
		// the next acknowledged record starts.
		let validated = if header.magic != SLAB_RECORD_MAGIC {
			None
		} else {
			let payload_size = header_size
				.checked_add(header.frame_len as usize)
				.ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "recovered frame_len overflow")
				})?;
			let padded = page_round_up(payload_size) as u64;
			if cursor + padded > file_size {
				None
			} else {
				// Pull out the frame bytes. Small records are already in
				// `probe`; larger ones need a dedicated aligned read.
				let frame_bytes: Vec<u8> = if padded as usize <= DIRECT_IO_ALIGN {
					probe.as_slice()[header_size..payload_size].to_vec()
				} else {
					let mut full = AlignedBuf::new(padded as usize);
					file.read_exact_at(full.as_mut_slice(), cursor)?;
					full.as_slice()[header_size..payload_size].to_vec()
				};
				verify_record_checksum(&header, &frame_bytes)
					.ok()
					.map(|()| (payload_size, padded, frame_bytes))
			}
		};

		let Some((payload_size, padded, frame_bytes)) = validated else {
			let Some(acknowledged) = acknowledged else {
				// No oracle: the first anomaly is the end of valid
				// data, as before this record. A zero page is not
				// evidence of anything on its own.
				break;
			};
			// ADR 0099 decision 1: resume at the next offset the node
			// acknowledged durable. Everything in between was never
			// acknowledged and is zeroed below. No later offset means
			// this anomaly is the tail.
			let Some(next) = next_acknowledged_offset(acknowledged, cursor, file_size) else {
				break;
			};
			skipped.push((cursor, next));
			cursor = next;
			continue;
		};

		let base_location = RecordLocation {
			slab_id,
			offset: cursor,
			length: payload_size as u32,
			hlc: header.hlc,
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		};
		if matches!(header.record_class, 1..=4) {
			has_counter_records = true;
		}
		match header.record_class {
			crate::schema::record_class::STANDARD => {
				for entry in recover_entries_for_record(&header, base_location, &frame_bytes)? {
					observe_recovered_row_version(&mut recovered_versions, &entry);
					recovered_non_counter_keys.push((entry.tenant_id, entry.key_identity));
					index
						.entry((entry.tenant_id, entry.key_identity))
						.and_modify(|versions| insert_version_sorted(versions, entry.location))
						.or_insert_with(|| vec![entry.location]);
					recovered_localities.insert(
						(entry.tenant_id, entry.key_identity),
						entry.routing_locality,
					);
				}
			}
			crate::schema::record_class::COUNTER_TENTATIVE => {
				// op-id is the record's own offset. The value lives in the
				// frame and is read on demand; here we only need length
				// validation and the location to (maybe) index below.
				if frame_bytes.len() < std::mem::size_of::<CounterTentativeFrame>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"counter tentative frame shorter than its fixed value",
					));
				}
				counter_tentatives.push((
					header.tenant_id,
					header.key_identity,
					base_location,
					u64::from_le_bytes(
						frame_bytes[..8]
							.try_into()
							.expect("validated counter value length"),
					),
					None,
				));
			}
			crate::schema::record_class::COUNTER_COMMIT => {
				if frame_bytes.len() < std::mem::size_of::<CounterCommitFrame>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"counter commit marker shorter than its fixed op-id",
					));
				}
				let op_id = u64::from_le_bytes(frame_bytes[..8].try_into().map_err(|_| {
					Error::new(ErrorKind::InvalidData, "counter commit marker op-id slice")
				})?);
				committed_counter_ops
					.entry((header.tenant_id, header.key_identity, op_id))
					.or_default()
					.push((None, header.hlc));
			}
			crate::schema::record_class::COUNTER_TENTATIVE_V2
			| crate::schema::record_class::COUNTER_COMMIT_V2 => {
				match super::counter::decode_origin_frame(&frame_bytes) {
					Ok((value, origin))
						if header.record_class
							== crate::schema::record_class::COUNTER_TENTATIVE_V2 =>
					{
						recovered_counter_tentatives_v2.push(RecoveredCounterTentativeV2 {
							tenant_id: header.tenant_id,
							key_identity: header.key_identity,
							location: base_location,
							value,
							origin,
						});
					}
					Ok((tentative_offset, origin)) => {
						recovered_counter_markers_v2.push(RecoveredCounterMarkerV2 {
							tenant_id: header.tenant_id,
							key_identity: header.key_identity,
							location: base_location,
							tentative_offset,
							origin,
						});
					}
					Err(_) => recovered_counter_errors.push((
						header.tenant_id,
						header.key_identity,
						super::counter::CounterVisibilityError::Unsupported,
					)),
				}
			}
			other => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("slab record carries unknown record_class {other}"),
				));
			}
		}
		cursor += padded;
		valid_end = cursor;
	}

	// ADR 0099 decision 1: with an oracle, leave the file canonical. Every
	// region the walk stepped over, and whatever trails the last accepted
	// record, was never acknowledged; zero it so that a later reader of
	// these bytes can take "non-zero page" to mean "record".
	if acknowledged.is_some() {
		if valid_end < file_size {
			skipped.push((valid_end, file_size));
		}
		let mut zeroed = false;
		for (start, end) in skipped {
			if end <= start {
				continue;
			}
			let mut at = start;
			while at < end {
				let chunk = ((end - at) as usize).min(ZERO_FILL_CHUNK_BYTES);
				let zeros = AlignedBuf::new(page_round_up(chunk));
				file.write_all_at(&zeros.as_slice()[..chunk], at)?;
				at += chunk as u64;
			}
			zeroed = true;
		}
		if zeroed {
			file.sync_data()?;
		}
	}

	// Legacy classes 1/2 retain their original per-slab offset pairing. Pair
	// V2 frames locally first: an exact same-slab identity/offset pair is
	// authoritative within that slab. Only unmatched frames participate in
	// the manager's cross-slab identity join.
	type VersionIdentity = (TenantId, Identity, u64, u32, u32, u32);
	let version_identity =
		|tenant: TenantId, key: Identity, hlc: HybridTimestamp, origin: crate::schema::NodeId| {
			(
				tenant,
				key,
				hlc.physical_ms,
				hlc.logical,
				hlc.node_id,
				origin.id,
			)
		};
	let mut tentatives_by_identity: HashMap<VersionIdentity, Vec<RecoveredCounterTentativeV2>> =
		HashMap::new();
	for tentative in recovered_counter_tentatives_v2.drain(..) {
		tentatives_by_identity
			.entry(version_identity(
				tentative.tenant_id,
				tentative.key_identity,
				tentative.location.hlc,
				tentative.origin,
			))
			.or_default()
			.push(tentative);
	}
	let mut markers_by_identity: HashMap<VersionIdentity, Vec<RecoveredCounterMarkerV2>> =
		HashMap::new();
	for marker in recovered_counter_markers_v2.drain(..) {
		markers_by_identity
			.entry(version_identity(
				marker.tenant_id,
				marker.key_identity,
				marker.location.hlc,
				marker.origin,
			))
			.or_default()
			.push(marker);
	}
	for (identity, local_markers) in markers_by_identity {
		let Some(local_tentatives) = tentatives_by_identity.remove(&identity) else {
			recovered_counter_markers_v2.extend(local_markers);
			continue;
		};
		let mut paired_offsets = HashSet::new();
		let mut locally_paired = Vec::new();
		let mut offset_mismatch = false;
		for marker in local_markers {
			let Some(tentative) = local_tentatives
				.iter()
				.find(|tentative| tentative.location.offset == marker.tentative_offset)
			else {
				// An identity match inside this slab with a different offset is
				// corruption, not a candidate for the cross-slab join.
				offset_mismatch = true;
				break;
			};
			paired_offsets.insert(tentative.location.offset);
			locally_paired.push(*tentative);
		}
		let first_value = local_tentatives[0].value;
		let values_match = local_tentatives
			.iter()
			.all(|tentative| tentative.value == first_value);
		if offset_mismatch || !values_match {
			let (tenant_id, key_identity, ..) = identity;
			recovered_counter_errors.push((
				tenant_id,
				key_identity,
				super::counter::CounterVisibilityError::Conflict,
			));
			continue;
		}
		for tentative in locally_paired {
			recovered_counter_versions.push((
				tentative.tenant_id,
				tentative.key_identity,
				super::counter::CounterVersion {
					value: tentative.value,
					hlc: tentative.location.hlc,
					origin: Some(tentative.origin),
				},
			));
			index
				.entry((tentative.tenant_id, tentative.key_identity))
				.and_modify(|versions| insert_version_sorted(versions, tentative.location))
				.or_insert_with(|| vec![tentative.location]);
			recovered_localities.insert(
				(tentative.tenant_id, tentative.key_identity),
				Locality::from(tentative.key_identity),
			);
		}
		for tentative in local_tentatives {
			if !paired_offsets.contains(&tentative.location.offset) {
				recovered_counter_tentatives_v2.push(tentative);
			}
		}
	}
	for tentatives in tentatives_by_identity.into_values() {
		recovered_counter_tentatives_v2.extend(tentatives);
	}
	// A legacy marker/value cannot satisfy a V2 frame's origin-bound identity,
	// even when its slab offset or HLC happens to coincide. Refuse such mixed
	// same-slab pairings instead of accidentally consuming either frame.
	let mut conflicting_v2_tentatives = vec![false; recovered_counter_tentatives_v2.len()];
	let mut conflicting_v2_markers = vec![false; recovered_counter_markers_v2.len()];
	for (index, tentative) in recovered_counter_tentatives_v2.iter().enumerate() {
		if committed_counter_ops
			.get(&(
				tentative.tenant_id,
				tentative.key_identity,
				tentative.location.offset,
			))
			.is_some_and(|markers| {
				markers
					.iter()
					.any(|(_, hlc)| *hlc == tentative.location.hlc)
			}) {
			conflicting_v2_tentatives[index] = true;
			recovered_counter_errors.push((
				tentative.tenant_id,
				tentative.key_identity,
				super::counter::CounterVisibilityError::Conflict,
			));
		}
	}
	for (index, marker) in recovered_counter_markers_v2.iter().enumerate() {
		if counter_tentatives
			.iter()
			.any(|(tenant, key, location, _, _)| {
				*tenant == marker.tenant_id
					&& *key == marker.key_identity
					&& location.offset == marker.tentative_offset
					&& location.hlc == marker.location.hlc
			}) {
			conflicting_v2_markers[index] = true;
			recovered_counter_errors.push((
				marker.tenant_id,
				marker.key_identity,
				super::counter::CounterVisibilityError::Conflict,
			));
		}
	}
	for (tentative_index, tentative) in recovered_counter_tentatives_v2.iter().enumerate() {
		for (marker_index, marker) in recovered_counter_markers_v2.iter().enumerate() {
			if tentative.tenant_id == marker.tenant_id
				&& tentative.key_identity == marker.key_identity
				&& tentative.location.hlc == marker.location.hlc
				&& tentative.location.slab_id == marker.location.slab_id
				&& tentative.location.offset == marker.tentative_offset
				&& tentative.origin != marker.origin
			{
				conflicting_v2_tentatives[tentative_index] = true;
				conflicting_v2_markers[marker_index] = true;
				recovered_counter_errors.push((
					tentative.tenant_id,
					tentative.key_identity,
					super::counter::CounterVisibilityError::Conflict,
				));
			}
		}
	}
	recovered_counter_tentatives_v2 = std::mem::take(&mut recovered_counter_tentatives_v2)
		.into_iter()
		.enumerate()
		.filter_map(|(index, tentative)| (!conflicting_v2_tentatives[index]).then_some(tentative))
		.collect();
	recovered_counter_markers_v2 = std::mem::take(&mut recovered_counter_markers_v2)
		.into_iter()
		.enumerate()
		.filter_map(|(index, marker)| (!conflicting_v2_markers[index]).then_some(marker))
		.collect();
	for (tenant_id, key_identity, location, value, origin) in counter_tentatives {
		if let Some(markers) =
			committed_counter_ops.get(&(tenant_id, key_identity, location.offset))
		{
			if markers
				.iter()
				.any(|(marker_origin, _)| *marker_origin != origin)
			{
				recovered_counter_errors.push((
					tenant_id,
					key_identity,
					super::counter::CounterVisibilityError::Conflict,
				));
				continue;
			}
			recovered_counter_versions.push((
				tenant_id,
				key_identity,
				super::counter::CounterVersion {
					value,
					hlc: location.hlc,
					origin,
				},
			));
			index
				.entry((tenant_id, key_identity))
				.and_modify(|versions| insert_version_sorted(versions, location))
				.or_insert_with(|| vec![location]);
			recovered_localities.insert((tenant_id, key_identity), Locality::from(key_identity));
		}
	}

	validate_recovered_row_tombstone_chains(&recovered_versions)?;
	Ok((
		index,
		recovered_localities,
		valid_end,
		recovered_counter_versions,
		recovered_counter_tentatives_v2,
		recovered_counter_markers_v2,
		recovered_non_counter_keys,
		recovered_counter_errors,
		has_counter_records,
	))
}

/// Largest single write used to zero a skipped region at recovery.
const ZERO_FILL_CHUNK_BYTES: usize = 1 << 20;

/// The smallest acknowledged offset past `cursor` that a page can start at
/// inside `len` bytes, or `None` when the anomaly at `cursor` is the tail.
fn next_acknowledged_offset(acknowledged: &BTreeSet<u64>, cursor: u64, len: u64) -> Option<u64> {
	let next = *acknowledged.range(cursor + 1..).next()?;
	(next % DIRECT_IO_ALIGN as u64 == 0 && next + DIRECT_IO_ALIGN as u64 <= len).then_some(next)
}

/// Walk sealed slab bytes and return every record they hold.
///
/// `acknowledged` is the caller's evidence of where records start in this
/// slab: the offsets the manager's index holds for it, which for a tiered
/// slab were loaded from the durable rows. On an anomaly - a zero page, a
/// page with bytes but no magic, an overlong or torn record - the walk
/// resumes at the next acknowledged offset and stops if there is none. An
/// empty set is the strict walk that ends at the first anomaly. The
/// argument is mandatory so that no caller can walk bytes without stating
/// what it knows; there is no zero-page skip, because a zero page proves
/// nothing about the pages after it (ADR 0099 decision 3).
pub(crate) fn recover_slab_entries_from_blob(
	blob: &[u8],
	slab_id: SlabId,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<Vec<RecoveredSlabEntry>> {
	recover_slab_entries_from_blob_inner(blob, slab_id, acknowledged, false)
}

/// Secondary-index discovery only consumes ordinary table rows. A maintenance
/// replacement may also contain class-preserved counter frames, which have no
/// table attribution; skip those validated frames while retaining the default
/// fail-closed behavior for tiering and other blob-recovery callers.
pub(crate) fn recover_slab_entries_from_blob_skipping_counters(
	blob: &[u8],
	slab_id: SlabId,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<Vec<RecoveredSlabEntry>> {
	recover_slab_entries_from_blob_inner(blob, slab_id, acknowledged, true)
}

fn recover_slab_entries_from_blob_inner(
	blob: &[u8],
	slab_id: SlabId,
	acknowledged: &BTreeSet<u64>,
	skip_counter_frames: bool,
) -> std::io::Result<Vec<RecoveredSlabEntry>> {
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	let mut entries = Vec::new();
	let mut recovered_versions = HashMap::new();
	let mut cursor: usize = 0;

	while cursor + DIRECT_IO_ALIGN <= blob.len() {
		let header: SlabRecordHeader =
			*bytemuck::from_bytes::<SlabRecordHeader>(&blob[cursor..cursor + header_size]);

		let validated = if header.magic != SLAB_RECORD_MAGIC {
			None
		} else {
			let payload_size = header_size
				.checked_add(header.frame_len as usize)
				.ok_or_else(|| {
					Error::new(ErrorKind::InvalidData, "recovered frame_len overflow")
				})?;
			let padded = page_round_up(payload_size);
			if cursor + padded > blob.len() {
				None
			} else {
				let frame = &blob[cursor + header_size..cursor + payload_size];
				verify_record_checksum(&header, frame)
					.ok()
					.map(|()| (payload_size, padded, frame))
			}
		};

		let Some((payload_size, padded, frame)) = validated else {
			match next_acknowledged_offset(acknowledged, cursor as u64, blob.len() as u64) {
				Some(next) => {
					cursor = next as usize;
					continue;
				}
				None => break,
			}
		};

		let base_location = RecordLocation {
			slab_id,
			offset: cursor as u64,
			length: payload_size as u32,
			hlc: header.hlc,
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		};
		// ADR 075: the blob-tier recovery path does not (yet) apply the
		// counter tentative/commit rule — counters are not tiered in this cut.
		// Fail closed rather than mis-index a counter frame as a blob (which
		// would resurrect an orphan tentative). Tiering of counter slabs is a
		// later integration that must port the committed-only rule here.
		if matches!(header.record_class, 1..=4) {
			if skip_counter_frames {
				cursor += padded;
				continue;
			}
			return Err(super::counter::CounterFormatError::MaintenanceUnsupported.io());
		}
		if header.record_class != crate::schema::record_class::STANDARD {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"counter record encountered on the blob-tier recovery path (unsupported in this cut)",
			));
		}
		for entry in recover_entries_for_record(&header, base_location, frame)? {
			observe_recovered_row_version(&mut recovered_versions, &entry);
			entries.push(entry);
		}
		cursor += padded;
	}

	validate_recovered_row_tombstone_chains(&recovered_versions)?;
	Ok(entries)
}

fn observe_recovered_row_version(
	recovered_versions: &mut HashMap<TenantKey, Vec<RecordLocation>>,
	entry: &RecoveredSlabEntry,
) {
	recovered_versions
		.entry((entry.tenant_id, entry.key_identity))
		.and_modify(|versions| insert_version_sorted(versions, entry.location))
		.or_insert_with(|| vec![entry.location]);
}

fn validate_recovered_row_tombstone_chains(
	recovered_versions: &HashMap<TenantKey, Vec<RecordLocation>>,
) -> std::io::Result<()> {
	for versions in recovered_versions.values() {
		validate_row_tombstone_chain(versions)?;
	}
	Ok(())
}

pub(crate) fn validate_row_tombstone_chain(versions: &[RecordLocation]) -> std::io::Result<()> {
	if versions
		.windows(2)
		.any(|pair| pair[0].is_tombstone && pair[1].is_tombstone)
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"duplicate row tombstone recovered for the same tenant/key",
		));
	}
	Ok(())
}

fn recover_entries_for_record(
	header: &SlabRecordHeader,
	base_location: RecordLocation,
	frame: &[u8],
) -> std::io::Result<Vec<RecoveredSlabEntry>> {
	let Some(entries) = storage_multirow_index_entries_from_frame(frame)? else {
		return Ok(vec![RecoveredSlabEntry {
			tenant_id: header.tenant_id,
			key_identity: header.key_identity,
			routing_locality: Locality::from(header.key_identity),
			routing_locality_trusted: false,
			location: base_location,
		}]);
	};
	Ok(entries
		.into_iter()
		.map(|entry| RecoveredSlabEntry {
			tenant_id: header.tenant_id,
			key_identity: entry.key_identity,
			routing_locality: entry.routing_locality,
			routing_locality_trusted: true,
			location: RecordLocation {
				hlc: entry.row_hlc,
				record_row_index: entry.record_row_index,
				is_tombstone: entry.is_tombstone,
				// ADR 0090: recovery reads each row's placement from the frame.
				placement: if entry.partition.is_some() {
					PlacementSemantics::Partitioned
				} else {
					PlacementSemantics::RawLocality
				},
				partition: entry.partition,
				..base_location
			},
		})
		.collect())
}

pub(crate) fn storage_multirow_index_entries_from_frame(
	frame: &[u8],
) -> std::io::Result<Option<Vec<MultiRowIndexEntry>>> {
	if !has_plausible_arrow_stream_prefix(frame) {
		return Ok(None);
	}
	let batch = match decode_recovery_batch(frame) {
		Ok(batch) => batch,
		Err(_) => return Ok(None),
	};
	let has_hlc = batch.column_by_name(STORAGE_ROW_HLC_COLUMN).is_some();
	let has_identity = batch.column_by_name(STORAGE_ROW_KEY_ID_COLUMN).is_some();
	let has_locality = batch
		.column_by_name(STORAGE_ROW_ROUTING_LOCALITY_COLUMN)
		.is_some();
	let has_table_object_id = batch
		.column_by_name(STORAGE_ROW_TABLE_OBJECT_ID_COLUMN)
		.is_some();
	let has_tombstone = batch.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN).is_some();
	if !(has_hlc || has_identity || has_locality || has_table_object_id || has_tombstone) {
		return Ok(None);
	}
	if !(has_hlc && has_identity && has_locality && has_tombstone) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"multi-row storage record has an incomplete synthetic-column set",
		));
	}
	let row_hlcs = batch
		.column_by_name(STORAGE_ROW_HLC_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record has invalid __row_hlc__ column",
			)
		})?;
	let identities = batch
		.column_by_name(STORAGE_ROW_KEY_ID_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record has invalid __row_key_id__ column",
			)
		})?;
	let localities = batch
		.column_by_name(STORAGE_ROW_ROUTING_LOCALITY_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record has invalid __row_routing_locality__ column",
			)
		})?;
	let tombstones = batch
		.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN)
		.and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record has invalid __row_tombstone__ column",
			)
		})?;
	validate_recovered_row_tombstone_marker_shape(&batch, tombstones)?;
	// ADR 0090: per-row partition presence survives recovery; a null entry or
	// an absent column is a raw-locality row.
	let partitions = batch
		.column_by_name(crate::schema::STORAGE_ROW_PARTITION_COLUMN)
		.map(|array| {
			array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"multi-row storage record has invalid __row_partition__ column",
				)
			})
		})
		.transpose()?
		.map(|array| array.clone());
	let mut entries = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		let record_row_index = u32::try_from(row).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"multi-row storage record row index exceeds u32",
			)
		})?;
		entries.push(MultiRowIndexEntry {
			key_identity: Identity::new(fixed_size_binary_32(
				identities,
				row,
				STORAGE_ROW_KEY_ID_COLUMN,
			)?),
			routing_locality: Locality::new(localities.value(row)),
			row_hlc: decode_storage_hlc(fixed_size_binary_16(
				row_hlcs,
				row,
				STORAGE_ROW_HLC_COLUMN,
			)?)?,
			record_row_index: RecordRowIndex::new(record_row_index),
			is_tombstone: tombstones.value(row),
			partition: partitions
				.as_ref()
				.filter(|array| !arrow::array::Array::is_null(*array, row))
				.map(|array| PartitionId::new(array.value(row))),
		});
	}
	Ok(Some(entries))
}

fn validate_recovered_row_tombstone_marker_shape(
	batch: &arrow::record_batch::RecordBatch,
	tombstones: &BooleanArray,
) -> std::io::Result<()> {
	let has_tombstone_rows = (0..batch.num_rows()).any(|row| tombstones.value(row));
	if !has_tombstone_rows {
		return Ok(());
	}

	let schema = batch.schema();
	let user_columns = schema
		.fields()
		.iter()
		.filter(|field| {
			!matches!(
				field.name().as_str(),
				STORAGE_ROW_HLC_COLUMN
					| STORAGE_ROW_KEY_ID_COLUMN
					| STORAGE_ROW_ROUTING_LOCALITY_COLUMN
					| STORAGE_ROW_TABLE_OBJECT_ID_COLUMN
					| STORAGE_ROW_TOMBSTONE_COLUMN
			)
		})
		.map(|field| field.name().clone())
		.collect::<Vec<_>>();
	if !user_columns.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"row tombstone marker recovered with user columns: {}",
				user_columns.join(",")
			),
		));
	}

	let table_object_ids = batch
		.column_by_name(STORAGE_ROW_TABLE_OBJECT_ID_COLUMN)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"row tombstone marker missing __row_table_object_id__",
			)
		})?
		.as_any()
		.downcast_ref::<FixedSizeBinaryArray>()
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"row tombstone marker has invalid __row_table_object_id__ column",
			)
		})?;
	for row in 0..batch.num_rows() {
		if tombstones.value(row) {
			let _ =
				fixed_size_binary_16(table_object_ids, row, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN)?;
		}
	}
	Ok(())
}

fn has_plausible_arrow_stream_prefix(frame: &[u8]) -> bool {
	if frame.len() < 8 {
		return false;
	}
	let first = u32::from_le_bytes(frame[..4].try_into().expect("fixed width"));
	let (prefix_len, message_len) = if first == u32::MAX {
		let message_len = u32::from_le_bytes(frame[4..8].try_into().expect("fixed width"));
		(8_usize, message_len as usize)
	} else {
		(4_usize, first as usize)
	};
	message_len > 0 && message_len <= frame.len().saturating_sub(prefix_len)
}

fn decode_recovery_batch(frame: &[u8]) -> std::io::Result<arrow::record_batch::RecordBatch> {
	let materialized;
	let frame =
		if crate::storage::encoding::materialize::ipc_stream_contains_encoding_directory(frame) {
			materialized =
				crate::storage::encoding::materialize::materialize_encoding_ipc_stream(frame)?;
			materialized.as_slice()
		} else {
			frame
		};
	let mut reader = StreamReader::try_new(Cursor::new(frame), None)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("arrow stream open: {err}")))?;
	let mut batches = Vec::new();
	for next in reader.by_ref() {
		match next {
			Ok(batch) => batches.push(batch),
			Err(err) => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("arrow stream read: {err}"),
				));
			}
		}
	}
	match batches.as_slice() {
		[] => Err(Error::new(
			ErrorKind::InvalidData,
			"arrow stream contained no record batch",
		)),
		[batch] => Ok(batch.clone()),
		[first, ..] => arrow::compute::concat_batches(
			&first.schema(),
			&batches.iter().map(Clone::clone).collect::<Vec<_>>(),
		)
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream row-group concat: {err}"),
			)
		}),
	}
}

fn fixed_size_binary_16(
	array: &FixedSizeBinaryArray,
	row: usize,
	label: &str,
) -> std::io::Result<[u8; 16]> {
	let value = array.value(row);
	if value.len() != 16 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} width mismatch: expected=16 actual={}", value.len()),
		));
	}
	let mut out = [0_u8; 16];
	out.copy_from_slice(value);
	Ok(out)
}

fn fixed_size_binary_32(
	array: &FixedSizeBinaryArray,
	row: usize,
	label: &str,
) -> std::io::Result<[u8; 32]> {
	let value = array.value(row);
	if value.len() != 32 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} width mismatch: expected=32 actual={}", value.len()),
		));
	}
	let mut out = [0_u8; 32];
	out.copy_from_slice(value);
	Ok(out)
}

fn decode_storage_hlc(bytes: [u8; 16]) -> std::io::Result<HybridTimestamp> {
	Ok(HybridTimestamp {
		physical_ms: u64::from_le_bytes(bytes[0..8].try_into().expect("fixed width")),
		logical: u32::from_le_bytes(bytes[8..12].try_into().expect("fixed width")),
		node_id: u32::from_le_bytes(bytes[12..16].try_into().expect("fixed width")),
	})
}

// ============================================================
//  CHECKSUM
//
//  Torn-write detector. Defined as the first 4 bytes of a SHA-256
//  hash over (header-with-zero-checksum || frame-bytes), taken as
//  little-endian u32. Chosen to keep the checksum on the approved
//  hash family while preserving the existing on-disk field width
//  and torn-write semantics. If benchmarks ever show
//  this dominating append latency, the checksum primitive can be
//  swapped here without changing the rest of the read/write path.
// ============================================================

fn compute_checksum(bytes_header_then_frame: &[u8]) -> u32 {
	crate::hash::sha256::prefix_u32(bytes_header_then_frame)
}

fn clone_io_result(result: &std::io::Result<()>) -> std::io::Result<()> {
	match result {
		Ok(()) => Ok(()),
		Err(err) => Err(Error::new(err.kind(), err.to_string())),
	}
}

fn verify_record_checksum(header: &SlabRecordHeader, frame: &[u8]) -> std::io::Result<()> {
	let mut zeroed = *header;
	zeroed.checksum = 0;
	let header_bytes = bytemuck::bytes_of(&zeroed);

	// Fold header + frame into one contiguous slice for hashing.
	// The extra copy is fine for scaffold grade; a streaming
	// SHA-256 helper with two `update` calls would eliminate it.
	let mut buf = Vec::with_capacity(header_bytes.len() + frame.len());
	buf.extend_from_slice(header_bytes);
	buf.extend_from_slice(frame);

	let expected = compute_checksum(&buf);
	if expected != header.checksum {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"slab record checksum mismatch (torn write?)",
		));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::{AsyncFileOps, BlockingFileOps, FileHandle, OpenFlags};
	use arrow::array::{FixedSizeBinaryBuilder, UInt64Array, UInt64Builder};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;
	use async_trait::async_trait;
	use std::path::Path;
	use std::sync::{Arc, Mutex};
	use tokio::sync::Notify;

	fn hlc(ms: u64, logical: u32) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical,
			node_id: 0,
		}
	}

	fn loc(hlc: HybridTimestamp, offset: u64) -> RecordLocation {
		RecordLocation {
			slab_id: SlabId::ZERO,
			offset,
			length: 4096,
			hlc,
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		}
	}

	fn hlc_bytes(hlc: HybridTimestamp) -> [u8; 16] {
		let mut bytes = [0_u8; 16];
		bytes[..8].copy_from_slice(&hlc.physical_ms.to_le_bytes());
		bytes[8..12].copy_from_slice(&hlc.logical.to_le_bytes());
		bytes[12..16].copy_from_slice(&hlc.node_id.to_le_bytes());
		bytes
	}

	fn encode_batch(batch: &RecordBatch) -> Vec<u8> {
		crate::storage::ipc::encode_record_batch_bytes(
			batch,
			crate::schema::RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap()
	}

	fn multirow_storage_frame(
		row0: Identity,
		row1: Identity,
		hlc0: HybridTimestamp,
		hlc1: HybridTimestamp,
	) -> Vec<u8> {
		multirow_storage_frame_with_tombstones(row0, row1, hlc0, hlc1, false, false)
	}

	fn multirow_storage_frame_with_tombstones(
		row0: Identity,
		row1: Identity,
		hlc0: HybridTimestamp,
		hlc1: HybridTimestamp,
		tombstone0: bool,
		tombstone1: bool,
	) -> Vec<u8> {
		let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(2, 16);
		row_hlc.append_value(hlc_bytes(hlc0)).unwrap();
		row_hlc.append_value(hlc_bytes(hlc1)).unwrap();
		let mut row_id = FixedSizeBinaryBuilder::with_capacity(2, 32);
		row_id.append_value(row0.as_bytes()).unwrap();
		row_id.append_value(row1.as_bytes()).unwrap();
		let mut row_locality = UInt64Builder::with_capacity(2);
		row_locality.append_value(70);
		row_locality.append_value(80);
		let row_tombstone = BooleanArray::from(vec![tombstone0, tombstone1]);
		let schema = Arc::new(Schema::new(vec![
			Field::new("value", DataType::UInt64, false),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
			Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(UInt64Array::from(vec![1_u64, 2])),
				Arc::new(row_hlc.finish()),
				Arc::new(row_id.finish()),
				Arc::new(row_locality.finish()),
				Arc::new(row_tombstone),
			],
		)
		.unwrap();
		encode_batch(&batch)
	}

	fn row_tombstone_marker_frame(row: Identity, hlc: HybridTimestamp) -> Vec<u8> {
		row_tombstone_marker_frame_with_options(row, hlc, true, false)
	}

	fn row_tombstone_marker_frame_with_options(
		row: Identity,
		hlc: HybridTimestamp,
		include_table_object_id: bool,
		include_user_column: bool,
	) -> Vec<u8> {
		let mut fields = Vec::new();
		let mut columns = Vec::<Arc<dyn arrow::array::Array>>::new();
		if include_user_column {
			fields.push(Field::new("value", DataType::UInt64, false));
			columns.push(Arc::new(UInt64Array::from(vec![1_u64])));
		}

		let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(1, 16);
		row_hlc.append_value(hlc_bytes(hlc)).unwrap();
		let mut row_id = FixedSizeBinaryBuilder::with_capacity(1, 32);
		row_id.append_value(row.as_bytes()).unwrap();
		let mut row_locality = UInt64Builder::with_capacity(1);
		row_locality.append_value(70);

		fields.extend([
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
		]);
		columns.push(Arc::new(row_hlc.finish()));
		columns.push(Arc::new(row_id.finish()));
		columns.push(Arc::new(row_locality.finish()));

		if include_table_object_id {
			let mut row_table_object_id = FixedSizeBinaryBuilder::with_capacity(1, 16);
			row_table_object_id.append_value([0x50_u8; 16]).unwrap();
			fields.push(Field::new(
				STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
				DataType::FixedSizeBinary(16),
				false,
			));
			columns.push(Arc::new(row_table_object_id.finish()));
		}

		fields.push(Field::new(
			STORAGE_ROW_TOMBSTONE_COLUMN,
			DataType::Boolean,
			false,
		));
		columns.push(Arc::new(BooleanArray::from(vec![true])));

		let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
		encode_batch(&batch)
	}

	async fn recovery_error_for_single_frame(tag: &str, frame: &[u8]) -> std::io::Error {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let path = dir.join("slab_0000000000000000.wnd");
		let file_ops = Arc::new(BlockingFileOps);
		{
			let store = LocalSlabStore::open(
				&path,
				SlabId::ZERO,
				file_ops.clone(),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap();
			store
				.append(TenantId::new(7), Identity::new([9; 32]), hlc(99, 0), frame)
				.await
				.unwrap();
			store.fsync().await.unwrap();
		}

		match LocalSlabStore::open(
			&path,
			SlabId::ZERO,
			file_ops,
			GroupCommitConfig {
				floor: Duration::ZERO,
				ceiling: Duration::ZERO,
				per_write_cost_estimate: Duration::ZERO,
			},
		)
		.await
		{
			Ok(_) => panic!("invalid row tombstone marker must fail recovery"),
			Err(err) => err,
		}
	}

	#[test]
	fn insert_version_sorted_maintains_ascending_hlc_order() {
		let mut versions = Vec::new();
		insert_version_sorted(&mut versions, loc(hlc(30, 0), 2));
		insert_version_sorted(&mut versions, loc(hlc(10, 0), 0));
		insert_version_sorted(&mut versions, loc(hlc(20, 0), 1));
		insert_version_sorted(&mut versions, loc(hlc(40, 0), 3));

		let hlcs: Vec<u64> = versions.iter().map(|l| l.hlc.physical_ms).collect();
		assert_eq!(hlcs, vec![10, 20, 30, 40]);
	}

	#[test]
	fn insert_version_sorted_keeps_duplicate_hlcs_in_insertion_order() {
		// Two writes at the identical HLC (replay or cross-leader
		// race) must both land — the sweeper can resolve them
		// later, but the index must not silently drop one.
		let mut versions = Vec::new();
		insert_version_sorted(&mut versions, loc(hlc(10, 0), 0));
		insert_version_sorted(&mut versions, loc(hlc(10, 0), 1));
		insert_version_sorted(&mut versions, loc(hlc(10, 0), 2));

		assert_eq!(versions.len(), 3);
		assert_eq!(versions[0].offset, 0);
		assert_eq!(versions[2].offset, 2);
	}

	#[test]
	fn newest_version_as_of_with_zero_hlc_returns_latest() {
		let versions = vec![loc(hlc(10, 0), 0), loc(hlc(20, 0), 1), loc(hlc(30, 0), 2)];
		let latest = newest_version_as_of(&versions, hlc(0, 0)).unwrap();
		assert_eq!(latest.offset, 2);
	}

	#[test]
	fn newest_version_as_of_returns_version_at_or_before_bound() {
		let versions = vec![loc(hlc(10, 0), 0), loc(hlc(20, 0), 1), loc(hlc(30, 0), 2)];
		// Bound sits exactly on a version -> that version.
		assert_eq!(
			newest_version_as_of(&versions, hlc(20, 0)).unwrap().offset,
			1
		);
		// Bound between versions -> the older one.
		assert_eq!(
			newest_version_as_of(&versions, hlc(25, 0)).unwrap().offset,
			1
		);
		// Bound past all versions -> the latest.
		assert_eq!(
			newest_version_as_of(&versions, hlc(99, 0)).unwrap().offset,
			2
		);
	}

	#[test]
	fn newest_version_as_of_before_first_version_returns_none() {
		let versions = vec![loc(hlc(10, 0), 0), loc(hlc(20, 0), 1)];
		assert!(newest_version_as_of(&versions, hlc(5, 0)).is_none());
	}

	#[test]
	fn multirow_recovery_rejects_non_arrow_payload_prefixes() {
		assert!(!has_plausible_arrow_stream_prefix(b"v-old"));
		assert!(!has_plausible_arrow_stream_prefix(b"v-anchor"));
		assert!(
			storage_multirow_index_entries_from_frame(b"v-anchor")
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn group_commit_window_clamps_to_floor_and_ceiling() {
		let cfg = GroupCommitConfig {
			floor: Duration::from_micros(200),
			ceiling: Duration::from_millis(2),
			per_write_cost_estimate: Duration::from_micros(100),
		};

		assert_eq!(cfg.window_for_pending(1), Duration::from_micros(200));
		assert_eq!(cfg.window_for_pending(4), Duration::from_micros(400));
		assert_eq!(cfg.window_for_pending(64), Duration::from_millis(2));
	}

	#[tokio::test]
	async fn recovery_indexes_multirow_synthetic_aliases() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-multirow-recovery-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let path = dir.join("slab_0000000000000000.wnd");
		let file_ops = Arc::new(BlockingFileOps);
		let row0 = Identity::new([1; 32]);
		let row1 = Identity::new([2; 32]);
		let frame = multirow_storage_frame(row0, row1, hlc(10, 0), hlc(20, 3));
		{
			let store = LocalSlabStore::open(
				&path,
				SlabId::ZERO,
				file_ops.clone(),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap();
			store
				.append(TenantId::new(7), Identity::new([9; 32]), hlc(99, 0), &frame)
				.await
				.unwrap();
			store.fsync().await.unwrap();
		}

		let recovered = LocalSlabStore::open(
			&path,
			SlabId::ZERO,
			file_ops,
			GroupCommitConfig {
				floor: Duration::ZERO,
				ceiling: Duration::ZERO,
				per_write_cost_estimate: Duration::ZERO,
			},
		)
		.await
		.unwrap();
		assert!(
			recovered
				.lookup(TenantId::new(7), Identity::new([9; 32]))
				.is_none()
		);

		let loc0 = recovered.lookup(TenantId::new(7), row0).unwrap();
		assert_eq!(loc0.hlc, hlc(10, 0));
		assert_eq!(loc0.record_row_index, RecordRowIndex::ZERO);
		assert_eq!(
			recovered
				.recovered_localities
				.get(&(TenantId::new(7), row0))
				.map(|entry| *entry.value()),
			Some(Locality::new(70))
		);

		let loc1 = recovered.lookup(TenantId::new(7), row1).unwrap();
		assert_eq!(loc1.hlc, hlc(20, 3));
		assert_eq!(loc1.record_row_index, RecordRowIndex::new(1));
		assert_eq!(
			recovered
				.recovered_localities
				.get(&(TenantId::new(7), row1))
				.map(|entry| *entry.value()),
			Some(Locality::new(80))
		);
	}

	#[tokio::test]
	async fn recovery_allows_row_tombstone_after_resurrection_data() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-resurrected-row-tombstone-recovery-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let path = dir.join("slab_0000000000000000.wnd");
		let file_ops = Arc::new(BlockingFileOps);
		let row = Identity::new([6; 32]);
		let frames = [
			multirow_storage_frame(row, Identity::new([10; 32]), hlc(10, 0), hlc(10, 1)),
			row_tombstone_marker_frame(row, hlc(20, 0)),
			multirow_storage_frame(row, Identity::new([12; 32]), hlc(30, 0), hlc(30, 1)),
			row_tombstone_marker_frame(row, hlc(40, 0)),
		];
		{
			let store = LocalSlabStore::open(
				&path,
				SlabId::ZERO,
				file_ops.clone(),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap();
			for (index, frame) in frames.iter().enumerate() {
				store
					.append(
						TenantId::new(7),
						Identity::new([20 + index as u8; 32]),
						hlc(90 + index as u64, 0),
						frame,
					)
					.await
					.unwrap();
			}
			store.fsync().await.unwrap();
		}

		let recovered = LocalSlabStore::open(
			&path,
			SlabId::ZERO,
			file_ops,
			GroupCommitConfig {
				floor: Duration::ZERO,
				ceiling: Duration::ZERO,
				per_write_cost_estimate: Duration::ZERO,
			},
		)
		.await
		.unwrap();
		assert!(
			!recovered
				.lookup_as_of(TenantId::new(7), row, hlc(10, 0))
				.expect("initial data")
				.is_tombstone
		);
		assert!(
			recovered
				.lookup_as_of(TenantId::new(7), row, hlc(20, 0))
				.expect("first tombstone")
				.is_tombstone
		);
		assert!(
			!recovered
				.lookup_as_of(TenantId::new(7), row, hlc(30, 0))
				.expect("resurrection data")
				.is_tombstone
		);
		assert!(
			recovered
				.lookup(TenantId::new(7), row)
				.expect("second tombstone")
				.is_tombstone
		);
	}

	#[tokio::test]
	async fn recovery_fails_closed_on_duplicate_row_tombstones() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-duplicate-row-tombstone-recovery-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let path = dir.join("slab_0000000000000000.wnd");
		let file_ops = Arc::new(BlockingFileOps);
		let row = Identity::new([3; 32]);
		let frame_a = row_tombstone_marker_frame(row, hlc(10, 0));
		let frame_b = row_tombstone_marker_frame(row, hlc(20, 0));
		{
			let store = LocalSlabStore::open(
				&path,
				SlabId::ZERO,
				file_ops.clone(),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap();
			store
				.append(
					TenantId::new(7),
					Identity::new([9; 32]),
					hlc(99, 0),
					&frame_a,
				)
				.await
				.unwrap();
			store
				.append(
					TenantId::new(7),
					Identity::new([8; 32]),
					hlc(100, 0),
					&frame_b,
				)
				.await
				.unwrap();
			store.fsync().await.unwrap();
		}

		let recovered = LocalSlabStore::open(
			&path,
			SlabId::ZERO,
			file_ops,
			GroupCommitConfig {
				floor: Duration::ZERO,
				ceiling: Duration::ZERO,
				per_write_cost_estimate: Duration::ZERO,
			},
		)
		.await;
		let err = match recovered {
			Ok(_) => panic!("duplicate tombstones must fail recovery"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("duplicate row tombstone"));
	}

	#[tokio::test]
	async fn recovery_fails_closed_on_tombstone_marker_missing_table_object_id() {
		let frame = row_tombstone_marker_frame_with_options(
			Identity::new([4; 32]),
			hlc(10, 0),
			false,
			false,
		);
		let err =
			recovery_error_for_single_frame("missing-row-tombstone-table-object-id", &frame).await;

		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("missing __row_table_object_id__"));
	}

	#[tokio::test]
	async fn recovery_fails_closed_on_tombstone_marker_with_user_columns() {
		let frame =
			row_tombstone_marker_frame_with_options(Identity::new([5; 32]), hlc(10, 0), true, true);
		let err = recovery_error_for_single_frame("row-tombstone-user-columns", &frame).await;

		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("user columns"));
	}

	#[derive(Clone, Default)]
	struct RecordingFileOps {
		inner: BlockingFileOps,
		events: Arc<Mutex<Vec<&'static str>>>,
		fdatasync_started: Arc<Notify>,
		release_fdatasync: Arc<Notify>,
	}

	impl RecordingFileOps {
		fn new() -> Self {
			Self::default()
		}

		fn record(&self, event: &'static str) {
			self.events.lock().unwrap().push(event);
		}
	}

	#[async_trait]
	impl AsyncFileOps for RecordingFileOps {
		async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
			self.inner.open(path, flags).await
		}

		async fn read_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			len: usize,
		) -> std::io::Result<Vec<u8>> {
			self.inner.read_plain_at(handle, offset, len).await
		}

		async fn write_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			bytes: Vec<u8>,
		) -> std::io::Result<usize> {
			self.inner.write_plain_at(handle, offset, bytes).await
		}

		async fn read_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: AlignedBuf,
		) -> std::io::Result<(usize, AlignedBuf)> {
			self.inner.read_at(handle, offset, buf).await
		}

		async fn write_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: AlignedBuf,
		) -> std::io::Result<(usize, AlignedBuf)> {
			let result = self.inner.write_at(handle, offset, buf).await;
			if result.is_ok() {
				self.record("write_complete");
			}
			result
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fsync(handle).await
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.record("fdatasync_start");
			self.fdatasync_started.notify_waiters();
			self.release_fdatasync.notified().await;
			let result = self.inner.fdatasync(handle).await;
			if result.is_ok() {
				self.record("fdatasync_end");
			}
			result
		}

		async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
			self.inner.read_all(path).await
		}

		async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
			self.inner.write_all(path, bytes).await
		}

		async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
			self.inner.file_len(path).await
		}

		async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
			self.inner.set_len(handle, len).await
		}

		async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
			self.inner.create_dir_all(path).await
		}

		async fn read_dir(
			&self,
			path: &Path,
		) -> std::io::Result<Vec<crate::storage::DirectoryEntry>> {
			self.inner.read_dir(path).await
		}

		async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
			self.inner.fsync_dir(path).await
		}

		async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.rename(from, to).await
		}

		async fn link_no_replace(&self, from: &Path, to: &Path) -> std::io::Result<()> {
			self.inner.link_no_replace(from, to).await
		}

		async fn unlink(&self, path: &Path) -> std::io::Result<()> {
			self.inner.unlink(path).await
		}
	}

	#[tokio::test]
	async fn durable_commit_returns_after_fdatasync_completes() {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-group-commit-order-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let file_ops = Arc::new(RecordingFileOps::new());
		let store = Arc::new(
			LocalSlabStore::open(
				dir.join("slab_0000000000000000.wnd"),
				SlabId::ZERO,
				file_ops.clone(),
				GroupCommitConfig {
					floor: Duration::ZERO,
					ceiling: Duration::ZERO,
					per_write_cost_estimate: Duration::ZERO,
				},
			)
			.await
			.unwrap(),
		);
		let events = file_ops.events.clone();
		let append = {
			let store = store.clone();
			tokio::spawn(async move {
				store
					.append(
						TenantId::ZERO,
						Identity::new([7; 32]),
						hlc(10, 0),
						b"payload",
					)
					.await
					.unwrap();
				store.wait_durable_commit().await.unwrap();
				events.lock().unwrap().push("ack");
			})
		};

		file_ops.fdatasync_started.notified().await;
		tokio::time::sleep(Duration::from_millis(5)).await;
		assert!(
			!append.is_finished(),
			"durable append returned before fdatasync completed"
		);
		file_ops.release_fdatasync.notify_waiters();
		append.await.unwrap();

		let events = file_ops.events.lock().unwrap().clone();
		let fdatasync_end = events
			.iter()
			.position(|event| *event == "fdatasync_end")
			.expect("fdatasync completion must be recorded");
		let ack = events
			.iter()
			.position(|event| *event == "ack")
			.expect("ack completion must be recorded");
		assert!(fdatasync_end < ack, "ack must follow fdatasync completion");
	}
}
