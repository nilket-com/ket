//! Offline, read-only scan of a node's retained Control log for the Root2 compromise audit.
//!
//! The scan runs the store's own open-time readers (snapshot artifact validation, WAL segment
//! discovery and record decode, retained-log contiguity and the recovered committed pointer) over
//! a backend that refuses every mutating operation before it reaches the file system. It never
//! repairs, truncates or completes anything, takes no data-directory owner lock, and opens every
//! file read-only: a torn tail is measured, not cut. Corruption, a gap or an unexplained missing
//! prefix is the readers' own `InvalidData` refusal.
use super::{
	check_retained_log_contiguous, control_artifact_paths, load_snapshot_artifact, replay_wal,
	resolve_recovered_committed,
};
use crate::consensus::WendelonTypeConfig;
use crate::consensus::decision_mode::DecisionMode;
use crate::storage::io_uring::AlignedBuf;
use crate::storage::{AsyncFileOps, BlockingFileOps, DirectoryEntry, FileHandle, OpenFlags};
use async_trait::async_trait;
use openraft::{Entry, LogId};
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::path::Path;

/// What the scan read: the checksum-validated retained entries above the validated snapshot
/// boundary, that boundary, the local recovered committed pointer and the torn-tail length.
/// The committed pointer is this node's local knowledge, never a quorum-currentness claim.
pub(crate) struct ControlLogScan {
	pub(crate) entries: BTreeMap<u64, Entry<WendelonTypeConfig>>,
	pub(crate) snapshot_boundary: Option<LogId<u32>>,
	pub(crate) committed: Option<LogId<u32>>,
	pub(crate) torn_tail_bytes: u64,
}

/// Scans `data_dir`'s Control artifacts read-only. `InvalidData` is invalid history (checksum,
/// framing, gap or a prefix no snapshot explains); any other error kind is I/O.
pub(crate) async fn scan_control_log_read_only(data_dir: &Path) -> std::io::Result<ControlLogScan> {
	let ops = ReadOnlyFileOps;
	let (wal, _, snapshot_path) = control_artifact_paths(data_dir);
	// Offline tools read the today authority; a ParentDecisions store is refused.
	let snapshot = load_snapshot_artifact(&ops, &snapshot_path, DecisionMode::Today).await?;
	let snapshot_boundary = snapshot.as_ref().and_then(|loaded| loaded.last_included);
	let replay = replay_wal(&ops, &wal, snapshot.as_ref(), DecisionMode::Today).await?;
	check_retained_log_contiguous(
		&wal,
		snapshot_boundary,
		replay.legacy_start,
		&replay.entries,
	)?;
	let committed =
		resolve_recovered_committed(snapshot.as_ref(), &replay.entries, replay.checkpoint)?;
	// Replay stopped at the last validated record of the last segment; the rest is measured.
	let torn_tail_bytes = match (
		replay.last_segment_needs_truncate,
		replay.wal_segments.last(),
	) {
		(true, Some(last)) => ops
			.file_len(&last.path)
			.await?
			.unwrap_or(0)
			.saturating_sub(last.len_bytes),
		_ => 0,
	};
	Ok(ControlLogScan {
		entries: replay.entries,
		snapshot_boundary,
		committed,
		torn_tail_bytes,
	})
}

fn refused(op: &str) -> Error {
	Error::new(
		ErrorKind::PermissionDenied,
		format!("read-only Control log scan refuses {op}"),
	)
}

/// Reads delegate to the unowned blocking backend; every write, sync, create, rename, link,
/// unlink or truncate is refused here, so no shared reader can mutate the directory.
struct ReadOnlyFileOps;

#[async_trait]
impl AsyncFileOps for ReadOnlyFileOps {
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		if flags.write || flags.create || flags.create_new || flags.truncate {
			return Err(refused("a writable open"));
		}
		BlockingFileOps.open(path, flags).await
	}
	async fn read_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		len: usize,
	) -> std::io::Result<Vec<u8>> {
		BlockingFileOps.read_plain_at(handle, offset, len).await
	}
	async fn write_plain_at(&self, _: &FileHandle, _: u64, _: Vec<u8>) -> std::io::Result<usize> {
		Err(refused("a write"))
	}
	async fn read_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		BlockingFileOps.read_at(handle, offset, buf).await
	}
	async fn write_at(
		&self,
		_: &FileHandle,
		_: u64,
		_: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		Err(refused("a write"))
	}
	async fn fsync(&self, _: &FileHandle) -> std::io::Result<()> {
		Err(refused("a sync"))
	}
	async fn fdatasync(&self, _: &FileHandle) -> std::io::Result<()> {
		Err(refused("a sync"))
	}
	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		BlockingFileOps.read_all(path).await
	}
	async fn write_all(&self, _: &Path, _: &[u8]) -> std::io::Result<()> {
		Err(refused("a write"))
	}
	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
		BlockingFileOps.file_len(path).await
	}
	async fn set_len(&self, _: &FileHandle, _: u64) -> std::io::Result<()> {
		Err(refused("a truncate"))
	}
	async fn create_dir_all(&self, _: &Path) -> std::io::Result<()> {
		Err(refused("a directory create"))
	}
	async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>> {
		BlockingFileOps.read_dir(path).await
	}
	async fn fsync_dir(&self, _: &Path) -> std::io::Result<()> {
		Err(refused("a directory sync"))
	}
	async fn rename(&self, _: &Path, _: &Path) -> std::io::Result<()> {
		Err(refused("a rename"))
	}
	async fn link_no_replace(&self, _: &Path, _: &Path) -> std::io::Result<()> {
		Err(refused("a link"))
	}
	async fn unlink(&self, _: &Path) -> std::io::Result<()> {
		Err(refused("an unlink"))
	}
}

/// Test-only writer of real Control WAL artifacts, byte-for-byte the store's own record and
/// snapshot encodings, so the audit is exercised over files the production readers accept.
#[cfg(test)]
pub(crate) mod fixture {
	use crate::consensus::WendelonTypeConfig;
	use crate::consensus::raft_storage::{
		SmState, encode_snapshot, serialize_checkpoint_page, serialize_entry_page, wal_segment_path,
	};
	use openraft::{Entry, LogId};
	use std::path::{Path, PathBuf};

	/// Writes `entries` as one named segment starting at `start`, then, when given, a recovery
	/// checkpoint record committing `commit`. Returns the segment path.
	pub(crate) fn write_segment(
		dir: &Path,
		start: u64,
		entries: &[Entry<WendelonTypeConfig>],
		commit: Option<LogId<u32>>,
	) -> PathBuf {
		let path = wal_segment_path(&dir.join("control.wal"), start).unwrap();
		let mut bytes = Vec::new();
		for entry in entries {
			bytes.extend_from_slice(serialize_entry_page(entry).unwrap().as_slice());
		}
		if let Some(commit) = commit {
			let page =
				serialize_checkpoint_page(commit.index, 0, commit.index, commit.leader_id.term);
			bytes.extend_from_slice(page.as_slice());
		}
		std::fs::write(&path, bytes).unwrap();
		path
	}

	/// The padded length of one entry's WAL record, for offset arithmetic in damage tests.
	pub(crate) fn record_len(entry: &Entry<WendelonTypeConfig>) -> usize {
		serialize_entry_page(entry).unwrap().as_slice().len()
	}

	/// A validated snapshot artifact whose boundary is `last`.
	pub(crate) fn write_snapshot(dir: &Path, last: LogId<u32>) {
		let state = SmState {
			last_applied_log_id: Some(last),
			..SmState::default()
		};
		std::fs::write(dir.join("snapshot.wnd"), encode_snapshot(&state).unwrap()).unwrap();
	}
}
