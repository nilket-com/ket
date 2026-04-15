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
//! [SlabRecordHeader : 40B] [ HRFT frame bytes : frame_len ] [ zero pad → 4K ]
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
//! Every key maps to a `Vec<RecordLocation>` sorted ascending by
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
//! * fsync / barrier discipline — the kernel's `O_DIRECT` gets the
//!   data to the device, but a group-commit `fsync` policy (ADR 009)
//!   is not yet implemented.
//!
//! [`append`]: LocalSlabStore::append

use crate::schema::{HybridTimestamp, SlabRecordHeader};
use crate::storage::io_uring::{AlignedBuf, IoOp, IoRingHandle, DIRECT_IO_ALIGN};
use async_trait::async_trait;
use dashmap::DashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Linux `O_DIRECT` flag. Defined inline to avoid pulling in `libc`
/// purely for one constant; the value is stable on Linux x86_64 /
/// aarch64 / riscv64 and matches `include/uapi/asm-generic/fcntl.h`.
const O_DIRECT: i32 = 0o0040000;

/// Slab record preamble magic = ASCII "SLAB".
const SLAB_RECORD_MAGIC: u32 = 0x534C4142;

/// A pointer to a record's location on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordLocation {
    pub slab_id: u64,
    pub offset: u64,
    pub length: u32,
    pub hlc: HybridTimestamp,
}

/// The unified storage interface for Wendelon.
/// Abstracts between local NVMe slabs and remote object storage (S3).
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Appends a raw HRFT frame to the current active slab.
    /// Returns the location of the record upon successful disk ack.
    async fn append(
        &self,
        key_hash: u64,
        hlc: HybridTimestamp,
        frame: &[u8],
    ) -> std::io::Result<RecordLocation>;

    /// Reads a record from disk based on its location.
    async fn read(&self, location: RecordLocation) -> std::io::Result<Vec<u8>>;

    /// Looks up a key's location in the in-memory index.
    /// Returns the latest version, equivalent to
    /// [`Self::lookup_as_of`] with the sentinel zero HLC.
    fn lookup(&self, key_hash: u64) -> Option<RecordLocation>;

    /// Time-travel lookup (ADR 013). Returns the newest version
    /// whose `hlc <= as_of_hlc`. A zero-valued `as_of_hlc` is the
    /// sentinel for "no upper bound" and falls through to the same
    /// behaviour as [`Self::lookup`].
    fn lookup_as_of(&self, key_hash: u64, as_of_hlc: HybridTimestamp) -> Option<RecordLocation>;
}

/// A high-performance storage implementation using io_uring and O_DIRECT.
pub struct LocalSlabStore {
    /// In-memory multi-version index: KeyHash -> versions sorted
    /// ascending by HLC. The tail is the latest version; older
    /// entries are retained for time-travel reads (ADR 013).
    pub index: DashMap<u64, Vec<RecordLocation>>,

    /// Identity of the active slab file. Written into every
    /// [`RecordLocation`] so reads can route to the right file once
    /// multiple slabs are live.
    slab_id: u64,

    /// Owning handle to the slab file. Keeps the kernel fd alive for
    /// as long as this store exists; `fd` below is a cached copy.
    #[allow(dead_code)]
    file: File,

    /// Cached raw fd for use with [`IoRequest`]. Valid for the
    /// lifetime of `self.file`.
    ///
    /// [`IoRequest`]: crate::storage::io_uring::IoRequest
    fd: RawFd,

    /// Submission handle for the dedicated `io_uring` thread.
    io: IoRingHandle,

    /// Monotonic write cursor into the slab file. Callers reserve
    /// disjoint byte ranges with `fetch_add`, so concurrent appends
    /// never collide.
    next_offset: AtomicU64,
}

impl LocalSlabStore {
    /// Open (or create) a slab file at `path`, binding it to the
    /// dedicated `io_uring` thread reachable through `io`.
    ///
    /// The file is opened with `O_DIRECT`, which is why every
    /// submission inside [`append`](Self::append) / [`read`](Self::read)
    /// uses [`AlignedBuf`] and rounds up to a page boundary.
    pub fn open(path: impl AsRef<Path>, slab_id: u64, io: IoRingHandle) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(O_DIRECT)
            .open(path.as_ref())?;
        let fd = file.as_raw_fd();

        // Scan any existing records so a restart resumes writing
        // past the last complete record and serves reads immediately.
        let (index, write_cursor) = recover_slab(&file, slab_id)?;

        Ok(Self {
            index,
            slab_id,
            file,
            fd,
            io,
            next_offset: AtomicU64::new(write_cursor),
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
    pub fn slab_id(&self) -> u64 {
        self.slab_id
    }

    /// Durability barrier: wait for every prior append on this
    /// store to land, then issue `fsync(2)` via the `io_uring`
    /// thread. Returning from `fsync().await` guarantees all
    /// previously-acked writes are persisted.
    ///
    /// ## Ordering contract (important)
    ///
    /// Fsync CQEs are **not** ordered against in-flight write CQEs
    /// in `io_uring`. Callers therefore must `.await` the
    /// [`append`](Self::append) futures they care about *before*
    /// calling `fsync` — `append` already awaits the write CQE, so
    /// any append whose future has resolved before the fsync call
    /// is covered. This is the natural shape for group-commit:
    /// fan-out appends in parallel, join them all, then a single
    /// `fsync` promotes the batch to durable.
    pub async fn fsync(&self) -> std::io::Result<()> {
        let reply = self.io.submit_fsync(self.fd).await?;
        let completion = reply
            .await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "io_uring reply channel closed"))?;
        if completion.result < 0 {
            return Err(Error::from_raw_os_error(-completion.result));
        }
        Ok(())
    }

    /// Remove one exact record location from the slab-local index.
    ///
    /// This is a best-effort in-memory rollback used when an append
    /// completed but the caller's required durability barrier failed.
    pub(crate) fn rollback_index_entry(&self, key_hash: u64, location: RecordLocation) {
        let should_remove_key = if let Some(mut versions) = self.index.get_mut(&key_hash) {
            let removed = remove_version_exact(&mut versions, location);
            removed && versions.is_empty()
        } else {
            false
        };
        if should_remove_key {
            self.index.remove(&key_hash);
        }
    }
}

#[async_trait]
impl StorageEngine for LocalSlabStore {
    async fn append(
        &self,
        key_hash: u64,
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
                key_hash,
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
        let reply = self.io.submit(IoOp::Write, self.fd, buf, offset).await?;
        let completion = reply
            .await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "io_uring reply channel closed"))?;

        if completion.result < 0 {
            return Err(Error::from_raw_os_error(-completion.result));
        }
        if (completion.result as usize) < payload_size {
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
            .entry(key_hash)
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
        let reply = self
            .io
            .submit(IoOp::Read, self.fd, buf, location.offset)
            .await?;
        let completion = reply
            .await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "io_uring reply channel closed"))?;

        if completion.result < 0 {
            return Err(Error::from_raw_os_error(-completion.result));
        }
        let bytes_read = completion.result as usize;

        let header_size = std::mem::size_of::<SlabRecordHeader>();
        if bytes_read < header_size {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "slab read returned fewer bytes than the record header",
            ));
        }
        let bytes = &completion.buf.as_slice()[..bytes_read];
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

    fn lookup(&self, key_hash: u64) -> Option<RecordLocation> {
        self.index.get(&key_hash).and_then(|v| v.last().copied())
    }

    fn lookup_as_of(&self, key_hash: u64, as_of_hlc: HybridTimestamp) -> Option<RecordLocation> {
        let versions = self.index.get(&key_hash)?;
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
//    at the current cursor — the 40-byte `SlabRecordHeader` sits
//    in its prefix. If magic matches, compute the padded size,
//    insert the record into the index, and advance. If magic does
//    not match, we have reached either the end of valid data or a
//    torn write; stop and report the cursor.
//
//  This trusts magic + frame_len on disk. A torn record whose
//  header was flushed but whose frame was not will leak an entry
//  into the index. That is acceptable for the scaffold: the real
//  durability barrier (ADR 009 group-commit fsync) will make this
//  impossible once it lands, and at that point a CRC over the
//  header+frame will replace the plain magic check.
// ============================================================

fn recover_slab(
    file: &File,
    slab_id: u64,
) -> std::io::Result<(DashMap<u64, Vec<RecordLocation>>, u64)> {
    use std::os::unix::fs::FileExt;

    let file_size = file.metadata()?.len();
    let index: DashMap<u64, Vec<RecordLocation>> = DashMap::new();
    let header_size = std::mem::size_of::<SlabRecordHeader>();
    // One page is enough for the header probe — every record
    // starts on a page boundary and the header occupies only the
    // first 40 bytes. For longer records we allocate a full-size
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
            .entry(header.key_hash)
            .and_modify(|versions| insert_version_sorted(versions, location))
            .or_insert_with(|| vec![location]);
        cursor += padded;
    }

    Ok((index, cursor))
}

// ============================================================
//  CHECKSUM
//
//  Torn-write detector. Defined as the first 4 bytes of a blake3
//  hash over (header-with-zero-checksum || frame-bytes), taken as
//  little-endian u32. Picked over a CRC32 because blake3 is
//  already a project dependency — avoiding a new crate beats the
//  marginal CPU cost of the stronger hash for a durability aid
//  that is not on the truly hottest path. If benchmarks ever show
//  this dominating append latency, swap in `crc32fast` here and
//  leave every other call site untouched.
// ============================================================

fn compute_checksum(bytes_header_then_frame: &[u8]) -> u32 {
    let hash = blake3::hash(bytes_header_then_frame);
    let bytes: [u8; 4] = hash.as_bytes()[..4]
        .try_into()
        .expect("blake3 output is 32 bytes");
    u32::from_le_bytes(bytes)
}

fn verify_record_checksum(header: &SlabRecordHeader, frame: &[u8]) -> std::io::Result<()> {
    let mut zeroed = *header;
    zeroed.checksum = 0;
    let header_bytes = bytemuck::bytes_of(&zeroed);

    // Fold header + frame into one contiguous slice for hashing.
    // The extra copy is fine for scaffold grade; a streaming
    // blake3::Hasher with two `update` calls would eliminate it.
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

    fn hlc(ms: u64, logical: u32) -> HybridTimestamp {
        HybridTimestamp {
            physical_ms: ms,
            logical,
            node_id: 0,
        }
    }

    fn loc(hlc: HybridTimestamp, offset: u64) -> RecordLocation {
        RecordLocation {
            slab_id: 0,
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
}
