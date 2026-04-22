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
//! [SlabRecordHeader : 64B] [ HRFT frame bytes : frame_len ] [ zero pad → 4K ]
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
use crate::schema::{HybridTimestamp, Identity, SlabId, SlabRecordHeader, TenantId};
use crate::storage::io_uring::{AlignedBuf, DIRECT_IO_ALIGN};
use crate::storage::{AsyncFileOps, FileHandle, OpenFlags};
use async_trait::async_trait;
use dashmap::DashMap;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};
use tokio::time::Instant;

/// Slab record preamble magic = ASCII "SLAB".
const SLAB_RECORD_MAGIC: u32 = 0x534C4142;

/// A pointer to a record's location on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecordLocation {
	pub slab_id: SlabId,
	pub offset: u64,
	pub length: u32,
	pub hlc: HybridTimestamp,
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

/// A high-performance storage implementation using io_uring and O_DIRECT.
pub struct LocalSlabStore {
	/// In-memory multi-version index: Identity -> versions sorted
	/// ascending by HLC. The tail is the latest version; older
	/// entries are retained for time-travel reads (ADR 013).
	pub index: DashMap<TenantKey, Vec<RecordLocation>>,

	/// Identity of the active slab file. Written into every
	/// [`RecordLocation`] so reads can route to the right file once
	/// multiple slabs are live.
	slab_id: SlabId,

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
		let (index, write_cursor) = recover_slab(&file, slab_id)?;

		Ok(Self {
			index,
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
		let header_size = std::mem::size_of::<SlabRecordHeader>();
		let payload_size = header_size
			.checked_add(frame.len())
			.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "frame length overflows usize"))?;
		let padded_size = page_round_up(payload_size);

		// Reserve a disjoint, page-aligned byte range.
		let offset = self
			.next_offset
			.fetch_add(padded_size as u64, Ordering::AcqRel);

		// Build the payload. `AlignedBuf::new` zero-initialises, so
		// the trailing pad is already correct.
		let mut buf = AlignedBuf::new(padded_size);
		{
			let dst = buf.as_mut_slice();
			// Write header first with checksum=0, then write the
			// frame bytes, then compute the checksum over
			// (header-with-zero-checksum || frame) and patch it
			// back into the header region. This keeps the
			// "checksum excludes itself" convention honest without
			// a second buffer.
			let mut header = SlabRecordHeader {
				magic: SLAB_RECORD_MAGIC,
				_pad_align: 0,
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

		// Submit and await the kernel CQE.
		let (written, _buf) = self.file_ops.write_at(&self.file, offset, buf).await?;
		if written < payload_size {
			return Err(Error::new(
				ErrorKind::WriteZero,
				"short write on slab append",
			));
		}

		let location = RecordLocation {
			slab_id: self.slab_id,
			offset,
			length: payload_size as u32,
			hlc,
		};
		// DashMap's `entry` API gives us an atomic read-modify-write
		// on the per-key version chain, so concurrent appends on
		// different HLCs for the same key cannot interleave their
		// insertions.
		self.index
			.entry((tenant_id, key_identity))
			.and_modify(|versions| insert_version_sorted(versions, location))
			.or_insert_with(|| vec![location]);
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
//    at the current cursor — the 64-byte `SlabRecordHeader` sits
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

fn recover_slab(
	file: &FileHandle,
	slab_id: SlabId,
) -> std::io::Result<(DashMap<TenantKey, Vec<RecordLocation>>, u64)> {
	use std::os::unix::fs::FileExt;

	let file = file.try_clone()?;
	let file_size = file.metadata()?.len();
	let index: DashMap<TenantKey, Vec<RecordLocation>> = DashMap::new();
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	// One page is enough for the header probe — every record
	// starts on a page boundary and the header occupies only the
	// first 64 bytes. For longer records we allocate a full-size
	// buffer on demand and re-read so the checksum can cover the
	// complete frame.
	let mut probe = AlignedBuf::new(DIRECT_IO_ALIGN);

	let mut cursor: u64 = 0;
	while cursor + DIRECT_IO_ALIGN as u64 <= file_size {
		file.read_exact_at(probe.as_mut_slice(), cursor)?;
		// Copy the header out of the probe page so we can re-use
		// the buffer for the same-page frame read below.
		let header: SlabRecordHeader =
			*bytemuck::from_bytes::<SlabRecordHeader>(&probe.as_slice()[..header_size]);

		if header.magic != SLAB_RECORD_MAGIC {
			// End of valid data: either never-written zeroes or a
			// torn trailing record.
			break;
		}

		let payload_size = header_size
			.checked_add(header.frame_len as usize)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "recovered frame_len overflow"))?;
		let padded = page_round_up(payload_size) as u64;

		// Defensive: stop if the record claims to extend past EOF.
		if cursor + padded > file_size {
			break;
		}

		// Pull out the frame bytes. Small records are already in
		// `probe`; larger ones need a dedicated aligned read.
		let frame_bytes: Vec<u8> = if padded as usize <= DIRECT_IO_ALIGN {
			probe.as_slice()[header_size..payload_size].to_vec()
		} else {
			let mut full = AlignedBuf::new(padded as usize);
			file.read_exact_at(full.as_mut_slice(), cursor)?;
			full.as_slice()[header_size..payload_size].to_vec()
		};

		if verify_record_checksum(&header, &frame_bytes).is_err() {
			// A header with the right magic but a broken checksum
			// signals a torn trailing write. Stop here rather than
			// trusting downstream records whose header might look
			// intact by coincidence.
			break;
		}

		let location = RecordLocation {
			slab_id,
			offset: cursor,
			length: payload_size as u32,
			hlc: header.hlc,
		};
		index
			.entry((header.tenant_id, header.key_identity))
			.and_modify(|versions| insert_version_sorted(versions, location))
			.or_insert_with(|| vec![location]);
		cursor += padded;
	}

	Ok((index, cursor))
}

pub(crate) fn recover_slab_entries_from_blob(
	blob: &[u8],
	slab_id: SlabId,
) -> std::io::Result<Vec<(TenantId, Identity, RecordLocation)>> {
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	let mut entries = Vec::new();
	let mut cursor: usize = 0;

	while cursor + DIRECT_IO_ALIGN <= blob.len() {
		let header: SlabRecordHeader =
			*bytemuck::from_bytes::<SlabRecordHeader>(&blob[cursor..cursor + header_size]);

		if header.magic != SLAB_RECORD_MAGIC {
			break;
		}

		let payload_size = header_size
			.checked_add(header.frame_len as usize)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "recovered frame_len overflow"))?;
		let padded = page_round_up(payload_size);

		if cursor + padded > blob.len() {
			break;
		}

		let frame = &blob[cursor + header_size..cursor + payload_size];
		if verify_record_checksum(&header, frame).is_err() {
			break;
		}

		let location = RecordLocation {
			slab_id,
			offset: cursor as u64,
			length: payload_size as u32,
			hlc: header.hlc,
		};
		entries.push((header.tenant_id, header.key_identity, location));
		cursor += padded;
	}

	Ok(entries)
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
