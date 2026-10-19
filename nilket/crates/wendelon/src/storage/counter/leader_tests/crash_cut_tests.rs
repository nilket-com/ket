//! Crash-cut and failure tests for counter dedup finalization, driven through
//! the real `CounterLeader`, its recovery, and retries of the same write key.
//!
//! The persistence model is logical, not a physical power-cut experiment. It
//! keeps volatile file bytes apart from durable ones and volatile directory
//! entries apart from durable ones. A crash discards everything no barrier
//! made durable. A cut stops the world after a chosen persistence event or
//! participant marker: the leader's later file operations and participant
//! calls all fail, so the durable image is exactly the image at the cut.
use super::*;

use crate::storage::counter::applied_log::{self, AppliedLogFault};
use std::collections::{HashMap, HashSet};

const WITNESS_DIR: &str = "/crash-cut/tenant-7/counter-witness";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
	TempWriteAfterBytes,
	TempSync,
	WitnessRenameAfterMove,
	PrepareDirectorySync,
	WitnessProgressSync,
	WitnessProgressDirectorySync,
	/// The append fails with the frame's identifying prefix durable.
	AppliedAppendTorn,
	/// The append fails with only part of the length prefix durable.
	AppliedAppendTornUnnamed,
	AppliedDataSync,
	AppliedDirectorySync,
	WitnessUnlink,
	UnlinkDirectorySync,
	/// A compaction generation's `create_new` persists part of the file, then fails.
	GenerationCreateTorn,
	/// `sync_data` on a numbered generation fails.
	GenerationDataSync,
	/// The directory sync that makes a new generation's name durable fails.
	GenerationDirectorySync,
	/// Writing `CURRENT`'s temp file fails after its bytes landed.
	CurrentTempWrite,
	/// `sync_all` on `CURRENT`'s temp file fails.
	CurrentTempSync,
	/// The rename onto `CURRENT` fails after the move.
	CurrentRename,
	/// The directory sync after the `CURRENT` rename fails.
	CurrentDirectorySync,
	/// The rename of the poison onto generation 0 fails after the move.
	PoisonRename,
	/// The directory sync that retires older generations fails.
	RetireDirectorySync,
}

#[derive(Clone, Copy, Debug)]
enum Cut {
	/// Crash after the n-th (1-based) event since the cut was armed.
	Event(usize),
	/// Crash after the n-th event whose label starts with the prefix.
	Label(&'static str, usize),
}

#[derive(Default)]
struct ModelImage {
	volatile_files: HashMap<PathBuf, Vec<u8>>,
	durable_files: HashMap<PathBuf, Vec<u8>>,
	volatile_names: HashSet<PathBuf>,
	durable_names: HashSet<PathBuf>,
	volatile_dirs: HashSet<PathBuf>,
	durable_dirs: HashSet<PathBuf>,
	pending_renames: HashMap<PathBuf, PathBuf>,
	/// Renames a failed directory sync dropped: as after a real `fsync` error,
	/// a later successful sync does not persist them, so they revert at a crash.
	lost_renames: HashSet<PathBuf>,
	/// fsync-error semantics (opt-in): after a failed directory sync, later
	/// directory syncs report success but persist nothing until power loss.
	sticky_directory_errors: bool,
	directory_errored: bool,
	/// Armed one-shot faults; each fires once at its own operation.
	faults: Vec<Fault>,
	unlinked_witness: bool,
	/// A numbered generation was unlinked since the last crash.
	unlinked_generation: bool,
	/// Bytes returned by `read_file` (the recovery read).
	bytes_read: u64,
	cut: Option<Cut>,
	/// Notified after the n-th event since it was armed; the world goes on.
	signal: Option<(usize, Arc<tokio::sync::Notify>)>,
	events: usize,
	label_hits: usize,
	crashed: bool,
	/// False models the old code: sync calls persist nothing.
	barriers: bool,
	/// The kernel persists an unlink's directory entry at once.
	eager_unlink: bool,
	/// The kernel writes back everything at the first participant marker.
	writeback_at_first_marker: bool,
}

#[derive(Default)]
struct PersistenceModel {
	image: StdMutex<ModelImage>,
	trace: Arc<StdMutex<Vec<String>>>,
	/// Blocks one file operation, on its own (blocking-pool) thread, until released.
	park: StdMutex<Option<ModelPark>>,
}

/// Parks the `nth` file operation whose label starts with `prefix`.
struct ModelPark {
	prefix: &'static str,
	nth: usize,
	seen: usize,
	park: Arc<BlockingPark>,
}

/// A parked file operation: `reached` fires when it parks, and it resumes once
/// `release` is called. It parks before taking the model's image lock.
#[derive(Default)]
struct BlockingPark {
	reached: tokio::sync::Notify,
	released: StdMutex<bool>,
	wake: std::sync::Condvar,
}

impl BlockingPark {
	fn release(&self) {
		*self.released.lock().unwrap() = true;
		self.wake.notify_all();
	}

	fn wait(&self) {
		let mut released = self.released.lock().unwrap();
		while !*released {
			released = self.wake.wait(released).unwrap();
		}
	}
}

/// Releases a park when dropped, so a failing assertion unwinds instead of
/// leaving a blocking-pool thread parked (which would hang the runtime's
/// shutdown).
struct ReleaseOnDrop(Arc<BlockingPark>);

impl Drop for ReleaseOnDrop {
	fn drop(&mut self) {
		self.0.release();
	}
}

fn crashed_error() -> Error {
	Error::other("model process crashed")
}

fn is_witness(path: &Path) -> bool {
	path.file_name()
		.is_some_and(|name| name.to_string_lossy().starts_with("counter-witness-"))
}

fn is_temp(path: &Path) -> bool {
	path.file_name()
		.is_some_and(|name| name.to_string_lossy().starts_with(".tmp-"))
}

/// Any applied-log generation: `counter-applied.log` (generation 0) or a
/// numbered compaction generation `counter-applied.<n>`.
fn is_applied(path: &Path) -> bool {
	path.file_name()
		.is_some_and(|name| name == "counter-applied.log")
		|| is_generation(path)
}

/// A numbered compaction generation `counter-applied.<n>`.
fn is_generation(path: &Path) -> bool {
	path.file_name()
		.and_then(|name| name.to_str())
		.and_then(|name| name.strip_prefix("counter-applied."))
		.is_some_and(|number| !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()))
}

fn is_current(path: &Path) -> bool {
	path.file_name().is_some_and(|name| name == "CURRENT")
}

fn is_current_temp(path: &Path) -> bool {
	path.file_name().is_some_and(|name| name == ".current-tmp")
}

fn is_generation_zero(path: &Path) -> bool {
	path.file_name()
		.is_some_and(|name| name == "counter-applied.log")
}

impl PersistenceModel {
	fn new() -> Arc<Self> {
		let model = Self::default();
		{
			let mut image = model.image.lock().unwrap();
			let root = PathBuf::from("/");
			image.volatile_dirs.insert(root.clone());
			image.durable_dirs.insert(root);
			image.barriers = true;
		}
		Arc::new(model)
	}

	/// The old code's persistence: no barriers, and the kernel free to persist
	/// metadata before data (unlinks at once, other state at the first marker).
	fn without_barriers() -> Arc<Self> {
		let model = Self::new();
		{
			let mut image = model.image.lock().unwrap();
			image.barriers = false;
			image.eager_unlink = true;
			image.writeback_at_first_marker = true;
		}
		model
	}

	fn arm(&self, fault: Fault) {
		self.image.lock().unwrap().faults.push(fault);
	}

	fn arm_cut(&self, cut: Cut) {
		let mut image = self.image.lock().unwrap();
		image.cut = Some(cut);
		image.events = 0;
		image.label_hits = 0;
	}

	/// Notify `signal` after the n-th (1-based) event from now on, without a
	/// crash: the caller can drop an operation there while its work goes on.
	fn signal_after(&self, n: usize, signal: Arc<tokio::sync::Notify>) {
		let mut image = self.image.lock().unwrap();
		image.signal = Some((n, signal));
		image.events = 0;
	}

	/// Park the `nth` (1-based) file operation from now on whose label (as the
	/// trace spells it) starts with `prefix`.
	fn park_at(&self, prefix: &'static str, nth: usize) -> Arc<BlockingPark> {
		let park = Arc::new(BlockingPark::default());
		*self.park.lock().unwrap() = Some(ModelPark {
			prefix,
			nth,
			seen: 0,
			park: park.clone(),
		});
		park
	}

	fn park_if_armed(&self, operation: &str, path: &Path) {
		let label = format!("{operation}:{}", path.display());
		let parked = {
			let mut armed = self.park.lock().unwrap();
			let hit = armed.as_mut().is_some_and(|park| {
				label.starts_with(park.prefix) && {
					park.seen += 1;
					park.seen == park.nth
				}
			});
			if hit { armed.take() } else { None }
		};
		if let Some(parked) = parked {
			parked.park.reached.notify_one();
			parked.park.wait();
		}
	}

	/// The kernel writes back everything now, as it may at any time.
	fn writeback_now(&self) {
		Self::writeback_all(&mut self.image.lock().unwrap());
	}

	fn has_durable_witness_name(&self) -> bool {
		self.image
			.lock()
			.unwrap()
			.durable_names
			.iter()
			.any(|path| is_witness(path))
	}

	/// Count events from now on (for sizing a cut sweep).
	fn reset_events(&self) {
		self.image.lock().unwrap().events = 0;
	}

	fn events(&self) -> usize {
		self.image.lock().unwrap().events
	}

	fn trace(&self) -> Vec<String> {
		self.trace.lock().unwrap().clone()
	}

	fn live(image: &ModelImage) -> std::io::Result<()> {
		if image.crashed {
			Err(crashed_error())
		} else {
			Ok(())
		}
	}

	/// A mutating operation completed: trace it and apply the armed cut.
	fn completed(&self, image: &mut ModelImage, operation: &str, path: &Path) {
		let label = format!("{operation}:{}", path.display());
		image.events += 1;
		if image
			.signal
			.as_ref()
			.is_some_and(|(n, _)| *n == image.events)
		{
			if let Some((_, signal)) = image.signal.take() {
				signal.notify_one();
			}
		}
		match image.cut {
			Some(Cut::Event(n)) if image.events == n => image.crashed = true,
			Some(Cut::Label(prefix, n)) if label.starts_with(prefix) => {
				image.label_hits += 1;
				if image.label_hits == n {
					image.crashed = true;
				}
			}
			_ => (),
		}
		self.trace.lock().unwrap().push(label);
	}

	fn fail_if_armed(image: &mut ModelImage, fault: Fault) -> std::io::Result<()> {
		if let Some(index) = image.faults.iter().position(|armed| *armed == fault) {
			image.faults.remove(index);
			return Err(Error::other(format!(
				"injected persistence failure: {fault:?}"
			)));
		}
		Ok(())
	}

	/// Power loss: only what a barrier made durable survives.
	fn crash(&self) {
		let mut image = self.image.lock().unwrap();
		image.volatile_files = image.durable_files.clone();
		image.volatile_names = image.durable_names.clone();
		image.volatile_dirs = image.durable_dirs.clone();
		// A durable name whose bytes never reached a barrier is an empty file.
		let empty: Vec<PathBuf> = image
			.durable_names
			.iter()
			.filter(|name| {
				!image.durable_dirs.contains(*name) && !image.durable_files.contains_key(*name)
			})
			.cloned()
			.collect();
		for name in empty {
			image.volatile_files.insert(name, Vec::new());
		}
		image.pending_renames.clear();
		image.lost_renames.clear();
		image.directory_errored = false;
		image.unlinked_witness = false;
		image.unlinked_generation = false;
		image.faults.clear();
		image.cut = None;
		image.signal = None;
		image.crashed = false;
	}

	fn writeback_all(image: &mut ModelImage) {
		image.durable_files = image.volatile_files.clone();
		image.durable_names = image.volatile_names.clone();
		image.durable_dirs = image.volatile_dirs.clone();
		image.pending_renames.clear();
	}

	fn has_witness_name(&self) -> bool {
		self.image
			.lock()
			.unwrap()
			.volatile_names
			.iter()
			.any(|path| is_witness(path))
	}

	fn bytes(&self, path: &Path) -> Option<Vec<u8>> {
		let image = self.image.lock().unwrap();
		image
			.volatile_names
			.contains(path)
			.then(|| image.volatile_files.get(path).cloned())
			.flatten()
	}

	/// Replace a file's bytes and make them and its name durable, as an
	/// out-of-band writer (a fixture or a foreign process) would.
	fn put_durable(&self, path: &Path, bytes: Vec<u8>) {
		let mut image = self.image.lock().unwrap();
		image.volatile_names.insert(path.to_path_buf());
		image.durable_names.insert(path.to_path_buf());
		image
			.volatile_files
			.insert(path.to_path_buf(), bytes.clone());
		image.durable_files.insert(path.to_path_buf(), bytes);
	}

	/// A process restart without power loss: the process dies (at a cut, or
	/// here) but the kernel keeps its page cache, so every volatile file byte
	/// and directory entry survives, durable or not. Faults, cuts, parks and
	/// the stopped world are cleared; nothing is persisted or discarded.
	fn restart_process(&self) {
		let mut image = self.image.lock().unwrap();
		image.faults.clear();
		image.cut = None;
		image.signal = None;
		image.crashed = false;
		drop(image);
		*self.park.lock().unwrap() = None;
	}

	/// fsync-error semantics: a failed directory sync leaves the directory's
	/// later syncs persisting nothing (while reporting success) until a crash.
	fn sticky_directory_errors(&self) {
		self.image.lock().unwrap().sticky_directory_errors = true;
	}

	/// From now on the kernel persists an unlink's directory entry at once, as
	/// it may write back metadata at any time.
	fn eager_unlinks(&self) {
		self.image.lock().unwrap().eager_unlink = true;
	}

	/// Cut the power now: every later operation fails, as after a crash cut,
	/// until `crash` discards what no barrier made durable.
	fn halt(&self) {
		self.image.lock().unwrap().crashed = true;
	}

	/// Remove a file and its name durably, as an out-of-band actor would.
	fn remove_durable(&self, path: &Path) {
		let mut image = self.image.lock().unwrap();
		image.volatile_names.remove(path);
		image.durable_names.remove(path);
		image.volatile_files.remove(path);
		image.durable_files.remove(path);
	}

	/// Bytes `read_file` has returned since the counter was last reset.
	fn take_bytes_read(&self) -> u64 {
		std::mem::take(&mut self.image.lock().unwrap().bytes_read)
	}

	fn persist_dir_entries(image: &mut ModelImage, dir: &Path) {
		let renames: Vec<_> = image
			.pending_renames
			.iter()
			.filter(|(to, _)| to.parent() == Some(dir) && !image.lost_renames.contains(*to))
			.map(|(to, from)| (to.clone(), from.clone()))
			.collect();
		for (to, from) in renames {
			if let Some(bytes) = image.durable_files.get(&from).cloned() {
				image.durable_files.insert(to.clone(), bytes);
			}
			image.pending_renames.remove(&to);
		}
		let lost = image.lost_renames.clone();
		image
			.durable_names
			.retain(|entry| entry.parent() != Some(dir) || lost.contains(entry));
		let children: HashSet<PathBuf> = image
			.volatile_names
			.iter()
			.filter(|entry| entry.parent() == Some(dir) && !lost.contains(*entry))
			.cloned()
			.collect();
		image.durable_names.extend(children.iter().cloned());
		let durable_names = image.durable_names.clone();
		image
			.durable_dirs
			.retain(|entry| durable_names.contains(entry) || entry == Path::new("/"));
		let child_dirs: Vec<_> = children
			.into_iter()
			.filter(|entry| image.volatile_dirs.contains(entry))
			.collect();
		image.durable_dirs.extend(child_dirs);
		if image.volatile_dirs.contains(dir) {
			image.durable_dirs.insert(dir.to_path_buf());
		}
	}
}

impl CounterDurableFileOps for PersistenceModel {
	fn exists(&self, path: &Path) -> std::io::Result<bool> {
		let image = self.image.lock().unwrap();
		Self::live(&image)?;
		Ok(image.volatile_names.contains(path) || image.volatile_dirs.contains(path))
	}

	fn create_dir(&self, path: &Path) -> std::io::Result<()> {
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		if image.volatile_dirs.contains(path) || image.volatile_names.contains(path) {
			return Err(Error::new(ErrorKind::AlreadyExists, "model path exists"));
		}
		image.volatile_dirs.insert(path.to_path_buf());
		image.volatile_names.insert(path.to_path_buf());
		self.completed(&mut image, "create_dir", path);
		Ok(())
	}

	fn write_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		self.park_if_armed("write_file", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		image.volatile_names.insert(path.to_path_buf());
		image
			.volatile_files
			.insert(path.to_path_buf(), bytes.to_vec());
		if is_temp(path) {
			Self::fail_if_armed(&mut image, Fault::TempWriteAfterBytes)?;
		}
		if is_current_temp(path) {
			Self::fail_if_armed(&mut image, Fault::CurrentTempWrite)?;
		}
		self.completed(&mut image, "write_file", path);
		Ok(())
	}

	fn create_new_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		self.park_if_armed("create_new_file", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		if image.volatile_names.contains(path) || image.volatile_dirs.contains(path) {
			return Err(Error::new(ErrorKind::AlreadyExists, "model path exists"));
		}
		image.volatile_names.insert(path.to_path_buf());
		image
			.volatile_files
			.insert(path.to_path_buf(), bytes.to_vec());
		if is_generation(path) && image.faults.contains(&Fault::GenerationCreateTorn) {
			// The kernel persisted the name and part of the bytes before failing.
			image
				.faults
				.retain(|armed| *armed != Fault::GenerationCreateTorn);
			let partial = bytes[..bytes.len() / 2].to_vec();
			image
				.volatile_files
				.insert(path.to_path_buf(), partial.clone());
			image.durable_files.insert(path.to_path_buf(), partial);
			image.durable_names.insert(path.to_path_buf());
			return Err(Error::other("injected torn generation create"));
		}
		self.completed(&mut image, "create_new_file", path);
		Ok(())
	}

	fn append_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<bool> {
		self.park_if_armed("append_file", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		let created = !image.volatile_names.contains(path);
		image.volatile_names.insert(path.to_path_buf());
		if created {
			image.volatile_files.insert(path.to_path_buf(), Vec::new());
		}
		let before = image.volatile_files[path].len();
		image
			.volatile_files
			.get_mut(path)
			.unwrap()
			.extend_from_slice(bytes);
		let torn = [
			(
				Fault::AppliedAppendTorn,
				applied_log::identifying_prefix_len(bytes),
			),
			(Fault::AppliedAppendTornUnnamed, 2),
		]
		.into_iter()
		.find(|(fault, _)| is_applied(path) && image.faults.contains(fault));
		if let Some((fault, keep)) = torn {
			// The kernel persisted a prefix of the frame (and the name) before failing.
			image.faults.retain(|armed| *armed != fault);
			let partial = image.volatile_files[path][..before + keep].to_vec();
			image
				.volatile_files
				.insert(path.to_path_buf(), partial.clone());
			image.durable_files.insert(path.to_path_buf(), partial);
			image.durable_names.insert(path.to_path_buf());
			return Err(Error::other("injected torn applied append"));
		}
		self.completed(&mut image, "append_file", path);
		Ok(created)
	}

	fn file_len(&self, path: &Path) -> std::io::Result<u64> {
		let image = self.image.lock().unwrap();
		Self::live(&image)?;
		if !image.volatile_names.contains(path) {
			return Err(Error::new(ErrorKind::NotFound, "model file absent"));
		}
		Ok(image.volatile_files.get(path).map_or(0, Vec::len) as u64)
	}

	fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		if !image.volatile_names.contains(path) {
			return Err(Error::new(ErrorKind::NotFound, "model file absent"));
		}
		let bytes = image
			.volatile_files
			.get(path)
			.cloned()
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "model file has no bytes"))?;
		image.bytes_read += bytes.len() as u64;
		Ok(bytes)
	}

	fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>> {
		let image = self.image.lock().unwrap();
		Self::live(&image)?;
		if !image.volatile_dirs.contains(path) {
			return Err(Error::new(ErrorKind::NotFound, "model directory absent"));
		}
		let mut entries: Vec<_> = image
			.volatile_names
			.iter()
			.filter(|entry| entry.parent() == Some(path))
			.cloned()
			.collect();
		entries.sort();
		Ok(entries)
	}

	fn truncate(&self, path: &Path, len: u64) -> std::io::Result<()> {
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		let bytes = image
			.volatile_files
			.get_mut(path)
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "model file absent"))?;
		bytes.truncate(usize::try_from(len).unwrap());
		self.completed(&mut image, "truncate", path);
		Ok(())
	}

	fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		self.park_if_armed("rename", to);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		let bytes = image
			.volatile_files
			.remove(from)
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "model rename source absent"))?;
		image.volatile_names.remove(from);
		image.volatile_names.insert(to.to_path_buf());
		image.volatile_files.insert(to.to_path_buf(), bytes);
		image
			.pending_renames
			.insert(to.to_path_buf(), from.to_path_buf());
		if is_current(to) {
			Self::fail_if_armed(&mut image, Fault::CurrentRename)?;
		} else if is_generation_zero(to) {
			Self::fail_if_armed(&mut image, Fault::PoisonRename)?;
		} else {
			Self::fail_if_armed(&mut image, Fault::WitnessRenameAfterMove)?;
		}
		self.completed(&mut image, "rename", to);
		Ok(())
	}

	fn unlink(&self, path: &Path) -> std::io::Result<()> {
		self.park_if_armed("unlink", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		if !image.volatile_names.contains(path) {
			return Err(Error::new(ErrorKind::NotFound, "model file absent"));
		}
		if is_witness(path) {
			Self::fail_if_armed(&mut image, Fault::WitnessUnlink)?;
			image.unlinked_witness = true;
		}
		if is_generation(path) {
			image.unlinked_generation = true;
		}
		image.volatile_names.remove(path);
		image.volatile_files.remove(path);
		if image.eager_unlink {
			image.durable_names.remove(path);
		}
		self.completed(&mut image, "unlink", path);
		Ok(())
	}

	fn sync_all(&self, path: &Path) -> std::io::Result<()> {
		self.park_if_armed("sync_all", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		if is_temp(path) {
			let fault = if image.volatile_names.iter().any(|entry| is_witness(entry)) {
				Fault::WitnessProgressSync
			} else {
				Fault::TempSync
			};
			Self::fail_if_armed(&mut image, fault)?;
		}
		if is_current_temp(path) {
			Self::fail_if_armed(&mut image, Fault::CurrentTempSync)?;
		}
		let bytes = image
			.volatile_files
			.get(path)
			.cloned()
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "model file absent"))?;
		if image.barriers {
			image.durable_files.insert(path.to_path_buf(), bytes);
		}
		self.completed(&mut image, "sync_all", path);
		Ok(())
	}

	fn sync_data(&self, path: &Path) -> std::io::Result<()> {
		self.park_if_armed("sync_data", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		if is_generation(path) {
			Self::fail_if_armed(&mut image, Fault::GenerationDataSync)?;
		}
		if is_applied(path) {
			Self::fail_if_armed(&mut image, Fault::AppliedDataSync)?;
		}
		let bytes = image
			.volatile_files
			.get(path)
			.cloned()
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "model file absent"))?;
		if image.barriers {
			image.durable_files.insert(path.to_path_buf(), bytes);
		}
		self.completed(&mut image, "sync_data", path);
		Ok(())
	}

	fn sync_dir(&self, path: &Path) -> std::io::Result<()> {
		self.park_if_armed("sync_dir", path);
		let mut image = self.image.lock().unwrap();
		Self::live(&image)?;
		let applied = path.join("counter-applied.log");
		let has_witness = image
			.volatile_names
			.iter()
			.any(|entry| entry.parent() == Some(path) && is_witness(entry));
		let committed = self
			.trace
			.lock()
			.unwrap()
			.iter()
			.any(|event| event == "participant_commit");
		if has_witness && !committed && !image.volatile_names.contains(&applied) {
			Self::fail_if_armed(&mut image, Fault::PrepareDirectorySync)?;
		}
		if has_witness && committed {
			Self::fail_if_armed(&mut image, Fault::WitnessProgressDirectorySync)?;
		}
		if image.volatile_names.contains(&applied) && !image.durable_names.contains(&applied) {
			Self::fail_if_armed(&mut image, Fault::AppliedDirectorySync)?;
		}
		if image.unlinked_witness {
			Self::fail_if_armed(&mut image, Fault::UnlinkDirectorySync)?;
		}
		let current_pending = image.pending_renames.keys().any(|to| is_current(to));
		if current_pending {
			if let Err(error) = Self::fail_if_armed(&mut image, Fault::CurrentDirectorySync) {
				let lost: Vec<PathBuf> = image
					.pending_renames
					.keys()
					.filter(|to| is_current(to))
					.cloned()
					.collect();
				image.lost_renames.extend(lost);
				return Err(error);
			}
		} else if image
			.volatile_names
			.iter()
			.any(|entry| is_generation(entry) && !image.durable_names.contains(entry))
		{
			if let Err(error) = Self::fail_if_armed(&mut image, Fault::GenerationDirectorySync) {
				if image.sticky_directory_errors {
					image.directory_errored = true;
				}
				return Err(error);
			}
		}
		if image.unlinked_generation
			|| image
				.pending_renames
				.keys()
				.any(|to| is_generation_zero(to))
		{
			Self::fail_if_armed(&mut image, Fault::RetireDirectorySync)?;
		}
		if image.barriers && !image.directory_errored {
			Self::persist_dir_entries(&mut image, path);
			image.unlinked_generation = false;
		}
		self.completed(&mut image, "sync_dir", path);
		Ok(())
	}
}

impl ParticipantObserver for PersistenceModel {
	fn crashed(&self) -> bool {
		self.image.lock().unwrap().crashed
	}

	fn commit_started(&self) {
		self.trace
			.lock()
			.unwrap()
			.push("participant_commit".to_owned());
	}

	fn marker_durable(&self) {
		let mut image = self.image.lock().unwrap();
		if image.writeback_at_first_marker {
			image.writeback_at_first_marker = false;
			Self::writeback_all(&mut image);
		}
		self.completed(&mut image, "participant_marker", Path::new("participant"));
	}
}

fn model_store(model: &Arc<PersistenceModel>) -> DurableCounterWitnessStore {
	DurableCounterWitnessStore::open_with_file_ops(WITNESS_DIR, model.clone()).unwrap()
}

fn applied_log_path() -> PathBuf {
	Path::new(WITNESS_DIR).join("counter-applied.log")
}

/// Records currently in the log (identical duplicates folded).
fn applied_records(model: &Arc<PersistenceModel>) -> Vec<AppliedCounterAdd> {
	model_store(model).load_applied().unwrap()
}

/// Raw frames in the log for `write_key`, counting identical duplicates.
fn raw_frames_for(model: &Arc<PersistenceModel>, write_key: CounterWriteKey) -> usize {
	let Some(bytes) = model.bytes(&applied_log_path()) else {
		return 0;
	};
	let mut at = 0;
	let mut count = 0;
	while at < bytes.len() {
		let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
		let image = applied_log::parse(&bytes[at..at + 4 + len]).unwrap();
		count += image
			.records
			.iter()
			.filter(|record| record.write_key == write_key)
			.count();
		at += 4 + len;
	}
	count
}

fn applied_op(model: &Arc<PersistenceModel>, write_key: CounterWriteKey) -> u64 {
	applied_records(model)
		.into_iter()
		.find(|record| record.write_key == write_key)
		.expect("applied record")
		.op_id
}

/// Which durable representations of `write_key`'s outcome a crashed image holds.
fn durable_representations(
	model: &Arc<PersistenceModel>,
	write_key: CounterWriteKey,
) -> (Option<StoredWitness>, bool) {
	let store = model_store(model);
	let witness = store
		.load_all()
		.unwrap()
		.into_iter()
		.find(|witness| witness.write_key == write_key);
	let applied = store
		.load_applied()
		.unwrap()
		.iter()
		.any(|record| record.write_key == write_key);
	(witness, applied)
}

fn outcome(previous: u64, current: u64) -> CounterAddOutcome {
	CounterAddOutcome { previous, current }
}

/// Participants on real slabs, plus the persistence model for the leader.
struct Rig {
	model: Arc<PersistenceModel>,
	paths: Vec<PathBuf>,
	quorum: usize,
	/// The leader's wall clock; the system clock when `None`.
	clock: Option<Arc<TestClock>>,
}

impl Rig {
	fn new(model: Arc<PersistenceModel>, participants: usize) -> Self {
		let dir = slab_dir();
		Self {
			model,
			paths: (0..participants)
				.map(|i| dir.join(format!("participant-{i}.wnd")))
				.collect(),
			quorum: participants / 2 + 1,
			clock: None,
		}
	}

	/// A rig whose every leader runs on `clock`.
	fn on_clock(model: Arc<PersistenceModel>, participants: usize, clock: Arc<TestClock>) -> Self {
		Self {
			clock: Some(clock),
			..Self::new(model, participants)
		}
	}

	async fn participants(&self, observed: bool) -> Vec<TestParticipant> {
		let mut participants = Vec::new();
		for path in &self.paths {
			let store = open_store(path).await;
			participants.push(if observed {
				TestParticipant::over_observed(store, self.model.clone())
			} else {
				TestParticipant::over(store)
			});
		}
		participants
	}

	/// A fresh leader whose participants report to the model.
	async fn leader(&self) -> LeaderHarness {
		let participants = self.participants(true).await;
		LeaderHarness::new_on(
			participants,
			self.quorum,
			true,
			model_store(&self.model),
			self.clock.clone(),
		)
	}

	/// Power-cut the model, reopen every participant, and recover the leader.
	async fn restart(&self) -> LeaderHarness {
		self.model.crash();
		self.recover_in_place().await.unwrap()
	}

	/// Recover from the image as it stands (no new power cut), so a cut armed
	/// beforehand can land inside recovery itself.
	async fn recover_in_place(&self) -> std::io::Result<LeaderHarness> {
		let store =
			DurableCounterWitnessStore::open_with_file_ops(WITNESS_DIR, self.model.clone())?;
		self.recover_with(store).await
	}

	/// Recover from the image as it stands through `store` (a store whose
	/// recovery switches a test has set).
	async fn recover_with(
		&self,
		store: DurableCounterWitnessStore,
	) -> std::io::Result<LeaderHarness> {
		let participants = self.participants(true).await;
		LeaderHarness::recover_on(participants, self.quorum, true, store, self.clock.clone())
	}

	async fn participant_values(&self, key: &CounterKey) -> Vec<Option<u64>> {
		let mut values = Vec::new();
		for path in &self.paths {
			values.push(read_after_restart(path, key).await);
		}
		values
	}
}

/// After recovery from any cut: the original outcome replays, reconciliation
/// resolves it once, the op-id high-water stays above it, and a second
/// restart replays it again.
async fn assert_recovers_one_outcome(
	rig: &Rig,
	write_key: CounterWriteKey,
	key: &CounterKey,
	expected: CounterAddOutcome,
	context: &str,
) {
	let recovered = rig.restart().await;
	let retry = recovered.fetch_add(write_key, key, 5).await.unwrap();
	assert_eq!(retry.outcome(), expected, "{context}: retry outcome");
	recovered.reconcile().await;
	recovered.reconcile().await;
	assert_eq!(recovered.pending_witnesses(), 0, "{context}: resolved");
	assert_eq!(recovered.read(key).await.unwrap(), Some(expected.current));
	drop(recovered);
	for value in rig.participant_values(key).await {
		assert_eq!(
			value,
			Some(expected.current),
			"{context}: participant value"
		);
	}
	assert_eq!(
		raw_frames_for(&rig.model, write_key),
		1,
		"{context}: one frame"
	);

	let recovered = rig.restart().await;
	assert_eq!(
		recovered.fetch_add(write_key, key, 5).await.unwrap(),
		CounterAddResult::Replayed(expected),
		"{context}: replay after a second restart"
	);
	let next = wk(0xfeed);
	assert_eq!(
		recovered.fetch_add(next, key, 1).await.unwrap(),
		CounterAddResult::Applied(outcome(expected.current, expected.current + 1)),
		"{context}: the next add builds on the single outcome"
	);
	drop(recovered);
	assert!(
		applied_op(&rig.model, next) > applied_op(&rig.model, write_key),
		"{context}: op ids stay above every resolved op"
	);
	assert_eq!(
		raw_frames_for(&rig.model, write_key),
		1,
		"{context}: still one frame"
	);
}

// C1/C5: a failed prepare barrier rejects the add before any participant
// commit, withdraws the half-published witness, and leaves nothing a later
// directory sync or recovery could resurrect.
#[tokio::test]
async fn failed_prepare_sends_no_commit_and_leaves_no_witness() {
	for fault in [
		Fault::TempWriteAfterBytes,
		Fault::TempSync,
		Fault::WitnessRenameAfterMove,
		Fault::PrepareDirectorySync,
	] {
		let rig = Rig::new(PersistenceModel::new(), 3);
		let key = ckey(b"prepare");
		let write_key = wk(100 + fault as u64);
		{
			let leader = rig.leader().await;
			rig.model.arm(fault);
			assert_eq!(
				leader.fetch_add(write_key, &key, 5).await,
				Err(CounterLeaderError::Internal),
				"{fault:?}"
			);
			assert!(
				!rig.model
					.trace()
					.iter()
					.any(|event| event == "participant_commit"),
				"{fault:?}: a participant commit crossed a failed prepare barrier"
			);
			assert!(
				!rig.model.has_witness_name(),
				"{fault:?}: witness withdrawn"
			);
			// A later operation's directory barriers cannot resurrect it.
			leader.fetch_add(wk(1), &ckey(b"other"), 1).await.unwrap();
		}
		let recovered = rig.restart().await;
		assert_eq!(recovered.read(&key).await.unwrap(), None, "{fault:?}");
		assert_eq!(
			recovered.fetch_add(write_key, &key, 5).await.unwrap(),
			CounterAddResult::Applied(outcome(0, 5)),
			"{fault:?}: the rejected add was never applied"
		);
	}
}

// C1-C3 ordering, from one traced add over a newly created applied log.
#[tokio::test]
async fn trace_places_every_barrier_around_the_participant_commits() {
	let rig = Rig::new(PersistenceModel::new(), 3);
	let leader = rig.leader().await;
	leader
		.fetch_add(wk(180), &ckey(b"ordered"), 1)
		.await
		.unwrap();
	let trace = rig.model.trace();
	let find = |from: usize, predicate: &dyn Fn(&str) -> bool| {
		trace
			.iter()
			.enumerate()
			.skip(from)
			.find(|(_, event)| predicate(event))
			.map(|(index, _)| index)
			.unwrap_or_else(|| panic!("event after {from} not in {trace:#?}"))
	};
	let temp_write = find(0, &|e| e.starts_with("write_file:") && e.contains(".tmp-"));
	let temp_sync = find(temp_write, &|e| {
		e.starts_with("sync_all:") && e.contains(".tmp-")
	});
	let rename = find(temp_sync, &|e| {
		e.starts_with("rename:") && e.contains("counter-witness-")
	});
	let prepare_barrier = find(rename, &|e| e.starts_with("sync_dir:"));
	let first_commit = find(0, &|e| e == "participant_commit");
	let last_marker = trace
		.iter()
		.rposition(|e| e.starts_with("participant_marker"))
		.unwrap();
	let append = find(0, &|e| e.starts_with("append_file:"));
	let applied_sync = find(append, &|e| {
		e.starts_with("sync_data:") && e.ends_with("counter-applied.log")
	});
	let name_barrier = find(applied_sync, &|e| e.starts_with("sync_dir:"));
	let unlink = find(0, &|e| {
		e.starts_with("unlink:") && e.contains("counter-witness-")
	});
	let unlink_barrier = find(unlink, &|e| e.starts_with("sync_dir:"));
	assert!(temp_write < temp_sync && temp_sync < rename && rename < prepare_barrier);
	assert!(prepare_barrier < first_commit);
	assert!(last_marker < append && append < applied_sync && applied_sync < name_barrier);
	assert!(name_barrier < unlink && unlink < unlink_barrier);
	assert_eq!(unlink_barrier, trace.len() - 1);
}

// C2 (required before SOURCE): crash immediately after EACH participant's
// commit marker becomes durable, through the real leader path. The durable
// witness survives with at most the markers before the cut recorded, the
// participants after the cut have no marker, and recovery finishes the add once.
#[tokio::test]
async fn crash_after_each_participant_marker_recovers_one_outcome() {
	for marker in 1..=3 {
		let rig = Rig::new(PersistenceModel::new(), 3);
		let key = ckey(b"markers");
		let write_key = wk(400 + marker as u64);
		{
			let leader = rig.leader().await;
			rig.model.arm_cut(Cut::Label("participant_marker", marker));
			let _ = leader.fetch_add(write_key, &key, 5).await;
		}
		rig.model.crash();
		let (witness, applied) = durable_representations(&rig.model, write_key);
		let witness = witness.expect("the prepared witness is durable at every marker cut");
		assert!(!applied, "marker {marker}: nothing resolved yet");
		assert!(
			witness.committed.len() < marker,
			"marker {marker}: progress recorded past the cut"
		);
		let values = rig.participant_values(&key).await;
		let expected: Vec<_> = (0..3).map(|i| (i < marker).then_some(5)).collect();
		assert_eq!(
			values, expected,
			"marker {marker}: markers before the cut only"
		);
		assert_recovers_one_outcome(
			&rig,
			write_key,
			&key,
			outcome(0, 5),
			&format!("marker {marker}"),
		)
		.await;
	}
}

// C2/C3: the named cuts on either side of each resolution barrier, for a newly
// created and for an existing applied log. Each image holds a durable witness
// or a durable applied record (C3's corrected ordering), and recovers once.
#[tokio::test]
async fn named_resolution_cuts_keep_a_durable_representation() {
	// (label, occurrence, durable witness expected, durable applied expected)
	let new_log: &[(&str, usize, bool, bool)] = &[
		("append_file", 1, true, false),
		("sync_data", 1, true, false),
		("sync_dir", 5, true, true), // the new log's directory entry
		("unlink", 1, true, true),
		("sync_dir", 6, false, true), // the unlink's directory barrier
	];
	let existing_log: &[(&str, usize, bool, bool)] = &[
		("append_file", 1, true, false),
		("sync_data", 1, true, true),
		("unlink", 1, true, true),
		("sync_dir", 5, false, true),
	];
	for (existing, cuts) in [(false, new_log), (true, existing_log)] {
		for &(label, occurrence, witness_expected, applied_expected) in cuts {
			let context = format!("existing log {existing}, after {label} #{occurrence}");
			let rig = Rig::new(PersistenceModel::new(), 3);
			let key = ckey(b"named");
			let write_key = wk(500);
			{
				let leader = rig.leader().await;
				if existing {
					leader.fetch_add(wk(499), &ckey(b"prior"), 1).await.unwrap();
				}
				rig.model.arm_cut(Cut::Label(label, occurrence));
				let _ = leader.fetch_add(write_key, &key, 5).await;
			}
			rig.model.crash();
			let (witness, applied) = durable_representations(&rig.model, write_key);
			assert_eq!(witness.is_some(), witness_expected, "{context}: witness");
			assert_eq!(applied, applied_expected, "{context}: applied record");
			assert_recovers_one_outcome(&rig, write_key, &key, outcome(0, 5), &context).await;
		}
	}
}

// C1-C4 exhaustively: crash after EVERY persistence event and participant
// marker of an add (new and existing applied log). Before the prepare barrier
// recovery exposes no committed add; from the first marker on, the image holds
// a valid witness or applied record; every image recovers exactly one outcome
// with one frame and op ids above it, across repeated reconcile and restart.
#[tokio::test]
async fn every_crash_cut_preserves_exactly_one_outcome() {
	for existing in [false, true] {
		// Size the sweep and locate the barriers with an uncut run.
		let probe = Rig::new(PersistenceModel::new(), 3);
		let (total, prepare_barrier, first_marker) = {
			let leader = probe.leader().await;
			if existing {
				leader.fetch_add(wk(699), &ckey(b"prior"), 1).await.unwrap();
			}
			let skip = probe.model.trace().len();
			probe.model.reset_events();
			leader.fetch_add(wk(700), &ckey(b"sweep"), 5).await.unwrap();
			let trace = probe.model.trace()[skip..]
				.iter()
				.filter(|event| *event != "participant_commit")
				.cloned()
				.collect::<Vec<_>>();
			let position =
				|prefix: &str| trace.iter().position(|e| e.starts_with(prefix)).unwrap() + 1;
			(
				probe.model.events(),
				position("sync_dir:"),
				position("participant_marker"),
			)
		};
		assert!(
			total > 20,
			"the sweep covers the whole add ({total} events)"
		);
		for cut in 1..=total {
			let context = format!("existing log {existing}, cut after event {cut}/{total}");
			let rig = Rig::new(PersistenceModel::new(), 3);
			let key = ckey(b"sweep");
			let write_key = wk(700);
			{
				let leader = rig.leader().await;
				if existing {
					leader.fetch_add(wk(699), &ckey(b"prior"), 1).await.unwrap();
				}
				rig.model.arm_cut(Cut::Event(cut));
				let _ = leader.fetch_add(write_key, &key, 5).await;
			}
			rig.model.crash();
			let (witness, applied) = durable_representations(&rig.model, write_key);
			if cut >= first_marker {
				assert!(
					witness.is_some() || applied,
					"{context}: a committed add lost both representations"
				);
			}
			if cut < prepare_barrier {
				let recovered = rig.restart().await;
				assert_eq!(
					recovered.read(&key).await.unwrap(),
					None,
					"{context}: recovery exposed an add before its prepare barrier"
				);
			}
			assert_recovers_one_outcome(&rig, write_key, &key, outcome(0, 5), &context).await;
		}
	}
}

// C3 negative control: the old ordering (no barriers, so the kernel may
// persist the witness unlink while the applied append is lost) produces the
// forbidden image after a participant commit: neither representation, and a
// retry double-counts. The same add with the barriers recovers once.
#[tokio::test]
async fn old_ordering_admits_the_lost_both_image_and_double_counts() {
	let key = ckey(b"control");
	let write_key = wk(800);
	let rig = Rig::new(PersistenceModel::without_barriers(), 3);
	{
		let leader = rig.leader().await;
		assert_eq!(
			leader.fetch_add(write_key, &key, 5).await.unwrap(),
			CounterAddResult::Applied(outcome(0, 5))
		);
	}
	rig.model.crash();
	assert_eq!(rig.participant_values(&key).await, vec![Some(5); 3]);
	let (witness, applied) = durable_representations(&rig.model, write_key);
	assert!(witness.is_none() && !applied, "the forbidden image");
	let recovered = rig.restart().await;
	assert_eq!(
		recovered.fetch_add(write_key, &key, 5).await.unwrap(),
		CounterAddResult::Applied(outcome(5, 10)),
		"the old ordering re-applies a committed add"
	);

	let rig = Rig::new(PersistenceModel::new(), 3);
	{
		let leader = rig.leader().await;
		leader.fetch_add(write_key, &key, 5).await.unwrap();
	}
	assert_recovers_one_outcome(&rig, write_key, &key, outcome(0, 5), "with barriers").await;
}

// C5: a failure at each post-commit file or directory barrier still answers the
// add once and keeps a retryable representation. Failures that leave the log
// length unknown fail closed in-process (typed `Unsettled`) until recovery.
#[tokio::test]
async fn post_commit_failures_keep_a_retryable_outcome() {
	for fault in [
		Fault::WitnessProgressSync,
		Fault::WitnessProgressDirectorySync,
		Fault::AppliedAppendTorn,
		Fault::AppliedAppendTornUnnamed,
		Fault::AppliedDataSync,
		Fault::AppliedDirectorySync,
		Fault::WitnessUnlink,
		Fault::UnlinkDirectorySync,
	] {
		let rig = Rig::new(PersistenceModel::new(), 3);
		let key = ckey(b"failure");
		let write_key = wk(200 + fault as u64);
		{
			let leader = rig.leader().await;
			rig.model.arm(fault);
			assert_eq!(
				leader.fetch_add(write_key, &key, 5).await.unwrap(),
				CounterAddResult::Applied(outcome(0, 5)),
				"{fault:?}"
			);
			let unsettled = matches!(
				fault,
				Fault::AppliedAppendTorn | Fault::AppliedAppendTornUnnamed | Fault::AppliedDataSync
			);
			let pending = match fault {
				Fault::WitnessProgressSync | Fault::WitnessProgressDirectorySync => 0,
				_ => 1,
			};
			assert_eq!(leader.pending_witnesses(), pending, "{fault:?}");
			assert_eq!(
				leader.fetch_add(write_key, &key, 5).await.unwrap(),
				CounterAddResult::Replayed(outcome(0, 5)),
				"{fault:?}: in-process retry"
			);
			leader.reconcile().await;
			if unsettled {
				assert_eq!(leader.pending_witnesses(), 1, "{fault:?}: fails closed");
				let witness = leader.leader.witnesses.get(&1).unwrap().clone();
				let applied = AppliedCounterAdd {
					op_id: witness.op_id,
					write_key: witness.write_key,
					key: Some(witness.key.clone()),
					previous: witness.previous,
					current: witness.value,
				};
				let error = leader
					.leader
					.witness_store
					.0
					.append_applied(&applied, &witness)
					.unwrap_err();
				assert_eq!(
					error
						.get_ref()
						.and_then(|error| error.downcast_ref::<AppliedLogFault>()),
					Some(&AppliedLogFault::Unsettled),
					"{fault:?}"
				);
			} else {
				assert_eq!(leader.pending_witnesses(), 0, "{fault:?}: retry resolves");
			}
		}
		assert_recovers_one_outcome(&rig, write_key, &key, outcome(0, 5), &format!("{fault:?}"))
			.await;
	}
}

// §3: a torn final frame whose witness is gone fails closed and is not touched.
#[tokio::test]
async fn torn_tail_without_an_unresolved_witness_fails_closed_unchanged() {
	for named in [true, false] {
		let rig = Rig::new(PersistenceModel::new(), 1);
		{
			let leader = rig.leader().await;
			leader.fetch_add(wk(901), &ckey(b"torn"), 5).await.unwrap();
		}
		let log = applied_log_path();
		let mut bytes = rig.model.bytes(&log).unwrap();
		let frame = applied_log::encode_frame(&AppliedCounterAdd {
			op_id: 2,
			write_key: wk(902),
			key: Some(ckey(b"torn")),
			previous: 5,
			current: 6,
		})
		.unwrap();
		let keep = if named {
			applied_log::identifying_prefix_len(&frame)
		} else {
			2
		};
		bytes.extend_from_slice(&frame[..keep]);
		rig.model.put_durable(&log, bytes.clone());
		rig.model.crash();
		let participants = rig.participants(false).await;
		assert!(
			LeaderHarness::recover(participants, 1, true, model_store(&rig.model)).is_err(),
			"named {named}: a torn frame without its witness must fail closed"
		);
		assert_eq!(rig.model.bytes(&log).unwrap(), bytes, "named {named}");
	}
}

// D2: the store is the log's only writer, so a length it did not write is a
// foreign writer or corruption. It fails closed with a typed error, never
// re-reads, appends nothing, and keeps the witness.
#[tokio::test]
async fn foreign_applied_log_change_fails_closed_without_rereading() {
	for change in ["grown", "shrunk", "removed"] {
		let rig = Rig::new(PersistenceModel::new(), 3);
		let key = ckey(b"foreign");
		let leader = rig.leader().await;
		leader.fetch_add(wk(1), &key, 5).await.unwrap();
		let log = applied_log_path();
		let written = rig.model.bytes(&log).unwrap();
		let expected_len = written.len() as u64;
		let observed_len = match change {
			"grown" => {
				let mut grown = written.clone();
				grown.extend_from_slice(
					&applied_log::encode_frame(&AppliedCounterAdd {
						op_id: 9,
						write_key: wk(9),
						key: Some(key.clone()),
						previous: 5,
						current: 6,
					})
					.unwrap(),
				);
				rig.model.put_durable(&log, grown.clone());
				Some(grown.len() as u64)
			}
			"shrunk" => {
				rig.model
					.put_durable(&log, written[..written.len() - 1].to_vec());
				Some(expected_len - 1)
			}
			_ => {
				CounterDurableFileOps::unlink(rig.model.as_ref(), &log).unwrap();
				None
			}
		};
		let foreign = rig.model.bytes(&log);
		assert_eq!(
			leader.fetch_add(wk(2), &key, 1).await.unwrap(),
			CounterAddResult::Applied(outcome(5, 6)),
			"{change}: the add is witnessed"
		);
		assert_eq!(leader.pending_witnesses(), 1, "{change}: not resolved");
		assert_eq!(rig.model.bytes(&log), foreign, "{change}: nothing appended");
		assert!(rig.model.has_witness_name(), "{change}: witness kept");
		let witness = leader.leader.witnesses.iter().next().unwrap().clone();
		let applied = AppliedCounterAdd {
			op_id: witness.op_id,
			write_key: witness.write_key,
			key: Some(witness.key.clone()),
			previous: witness.previous,
			current: witness.value,
		};
		let error = leader
			.leader
			.witness_store
			.0
			.append_applied(&applied, &witness)
			.unwrap_err();
		assert_eq!(
			error
				.get_ref()
				.and_then(|error| error.downcast_ref::<AppliedLogFault>()),
			Some(&AppliedLogFault::Diverged {
				expected_len,
				observed_len
			}),
			"{change}"
		);
		assert_eq!(rig.model.bytes(&log), foreign, "{change}: still untouched");
	}
}

fn fixture_witness(op_id: u64, write_key: CounterWriteKey, key: &[u8]) -> StoredWitness {
	StoredWitness {
		body: CounterFinalizationWitness {
			op_id,
			write_key,
			key: ckey(key),
			value: 5,
			previous: 0,
			commit_hlc: h(op_id),
			accepted: vec![node_id(0)],
			committed: HashSet::from([node_id(0)]),
		},
		origin: Some(node_id(0)),
	}
}

// D1: unkeyed records written before the keyed frame are still read and seed
// the op-id high-water; new appends are keyed. An unkeyed record cannot show
// which add it records, so a request under its write key is refused even when
// it is a genuine retry (the documented compatibility limit).
#[tokio::test]
async fn unkeyed_records_are_read_but_refuse_their_write_keys_and_new_records_are_keyed() {
	let rig = Rig::new(PersistenceModel::new(), 1);
	model_store(&rig.model);
	let mut bytes = Vec::new();
	for (op_id, n, previous, current) in [(1, 11, 0, 5), (2, 12, 5, 7)] {
		bytes.extend(
			applied_log::encode_unkeyed_frame(&AppliedCounterAdd {
				op_id,
				write_key: wk(n),
				key: None,
				previous,
				current,
			})
			.unwrap(),
		);
	}
	rig.model.put_durable(&applied_log_path(), bytes);
	let recovered = rig.restart().await;
	let key = ckey(b"unkeyed");
	assert_eq!(
		recovered.fetch_add(wk(11), &key, 5).await,
		Err(CounterLeaderError::WriteKeyConflict)
	);
	assert_eq!(
		recovered.fetch_add(wk(12), &key, 2).await,
		Err(CounterLeaderError::WriteKeyConflict)
	);
	recovered.fetch_add(wk(13), &key, 1).await.unwrap();
	drop(recovered);
	let records = applied_records(&rig.model);
	let shapes: Vec<_> = records
		.iter()
		.map(|record| (record.op_id, record.key.clone()))
		.collect();
	assert_eq!(shapes, vec![(1, None), (2, None), (3, Some(key))]);
}

// D1 limit: an unkeyed record has no counter key, so it binds to a witness by
// op id, write key and outcome only.
#[tokio::test]
async fn unkeyed_record_binds_to_its_witness_without_a_key_comparison() {
	let rig = Rig::new(PersistenceModel::new(), 1);
	let store = model_store(&rig.model);
	let witness = fixture_witness(1, wk(21), b"a");
	store.write(&witness).unwrap();
	rig.model.put_durable(
		&applied_log_path(),
		applied_log::encode_unkeyed_frame(&AppliedCounterAdd {
			op_id: 1,
			write_key: wk(21),
			key: None,
			previous: 0,
			current: 5,
		})
		.unwrap(),
	);
	drop(store);
	let recovered = rig.restart().await;
	assert_eq!(
		recovered.fetch_add(wk(21), &ckey(b"a"), 5).await.unwrap(),
		CounterAddResult::Replayed(outcome(0, 5))
	);
}

// D1: a keyed record paired with a witness for another counter fails closed and
// preserves both files.
#[tokio::test]
async fn keyed_record_for_another_counter_fails_closed_and_keeps_files() {
	let rig = Rig::new(PersistenceModel::new(), 1);
	let store = model_store(&rig.model);
	let witness = fixture_witness(1, wk(31), b"a");
	store.write(&witness).unwrap();
	let log = applied_log_path();
	let frame = applied_log::encode_frame(&AppliedCounterAdd {
		op_id: 1,
		write_key: wk(31),
		key: Some(ckey(b"b")),
		previous: 0,
		current: 5,
	})
	.unwrap();
	rig.model.put_durable(&log, frame.clone());
	let witness_path = store.origin_path_for(1);
	let witness_bytes = rig.model.bytes(&witness_path).unwrap();
	drop(store);
	rig.model.crash();
	let participants = rig.participants(false).await;
	assert!(LeaderHarness::recover(participants, 1, true, model_store(&rig.model)).is_err());
	assert_eq!(rig.model.bytes(&log), Some(frame));
	assert_eq!(rig.model.bytes(&witness_path), Some(witness_bytes));
}

// C6: concurrent resolutions of distinct counters (a multi-threaded runtime,
// so appends really race) leave complete frames, each bound to its identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_resolutions_write_complete_identity_bound_frames() {
	let rig = Rig::new(PersistenceModel::new(), 3);
	let leader = Arc::new(rig.leader().await);
	let mut tasks = Vec::new();
	for n in 1..=16u64 {
		let leader = leader.clone();
		tasks.push(tokio::spawn(async move {
			let key = ckey(format!("concurrent-{n}").as_bytes());
			leader.fetch_add(wk(n), &key, n).await.unwrap()
		}));
	}
	for task in tasks {
		assert!(matches!(task.await.unwrap(), CounterAddResult::Applied(_)));
	}
	assert_eq!(leader.pending_witnesses(), 0);
	let mut observed: Vec<_> = applied_records(&rig.model)
		.into_iter()
		.map(|record| {
			(
				record.write_key,
				record.key,
				record.previous,
				record.current,
			)
		})
		.collect();
	observed.sort_by_key(|(write_key, ..)| *write_key);
	let expected: Vec<_> = (1..=16u64)
		.map(|n| {
			(
				wk(n),
				Some(ckey(format!("concurrent-{n}").as_bytes())),
				0,
				n,
			)
		})
		.collect();
	assert_eq!(observed, expected);
	for n in 1..=16 {
		assert_eq!(raw_frames_for(&rig.model, wk(n)), 1);
	}
}

// C6 on the production file ops: OS threads append to one real log at once;
// every frame is complete and decodes to the identity that wrote it.
#[test]
fn parallel_appends_to_a_real_log_never_interleave() {
	let dir = slab_dir().join("witnesses");
	let store = Arc::new(DurableCounterWitnessStore::open(&dir).unwrap());
	let threads = 8u64;
	let per_thread = 16u64;
	let witnesses: Vec<_> = (1..=threads * per_thread)
		.map(|op_id| fixture_witness(op_id, wk(op_id), format!("key-{op_id}").as_bytes()))
		.collect();
	for witness in &witnesses {
		store.write(witness).unwrap();
	}
	let witnesses = Arc::new(witnesses);
	let handles: Vec<_> = (0..threads)
		.map(|thread| {
			let store = store.clone();
			let witnesses = witnesses.clone();
			std::thread::spawn(move || {
				for index in 0..per_thread {
					let witness = &witnesses[(thread * per_thread + index) as usize];
					let applied = AppliedCounterAdd {
						op_id: witness.op_id,
						write_key: witness.write_key,
						key: Some(witness.key.clone()),
						previous: witness.previous,
						current: witness.value,
					};
					store.append_applied(&applied, witness).unwrap();
				}
			})
		})
		.collect();
	for handle in handles {
		handle.join().unwrap();
	}
	let bytes = std::fs::read(dir.join("counter-applied.log")).unwrap();
	let image = applied_log::parse(&bytes).unwrap();
	assert!(image.torn_tail.is_none());
	assert_eq!(image.records.len() as u64, threads * per_thread);
	for record in &image.records {
		let witness = &witnesses[(record.op_id - 1) as usize];
		assert!(applied_log::witness_matches_applied(witness, record));
		assert_eq!(record.key.as_ref(), Some(&witness.key));
	}
}

fn fault_of(error: &Error) -> Option<AppliedLogFault> {
	error
		.get_ref()
		.and_then(|error| error.downcast_ref::<AppliedLogFault>())
		.copied()
}

// H1: a failed prepare whose withdrawal also fails may leave a durable witness.
// The store latches unsettled: every later publish, rewrite and append on ANY
// key fails closed with a typed error. The one `Internal` answer is in doubt;
// after restart recovery may complete it, and a retry replays it once.
#[tokio::test]
async fn failed_prepare_whose_withdrawal_fails_latches_the_store() {
	let rig = Rig::new(PersistenceModel::new(), 3);
	let key = ckey(b"doubt");
	let write_key = wk(950);
	{
		let leader = rig.leader().await;
		rig.model.arm(Fault::PrepareDirectorySync);
		rig.model.arm(Fault::WitnessUnlink);
		assert_eq!(
			leader.fetch_add(write_key, &key, 5).await,
			Err(CounterLeaderError::Internal)
		);
		assert!(leader.leader.durability_unsettled());
		assert_eq!(
			leader.fetch_add(wk(951), &ckey(b"elsewhere"), 1).await,
			Err(CounterLeaderError::Internal),
			"the next add on any key fails closed"
		);
		let probe = fixture_witness(77, wk(952), b"probe");
		let store = &leader.leader.witness_store.0;
		assert_eq!(
			fault_of(&store.write(&probe).unwrap_err()),
			Some(AppliedLogFault::Unsettled)
		);
		assert_eq!(
			fault_of(&store.rewrite(&probe).unwrap_err()),
			Some(AppliedLogFault::Unsettled)
		);
		let applied = applied_log::record_for(&probe);
		assert_eq!(
			fault_of(&store.append_applied(&applied, &probe).unwrap_err()),
			Some(AppliedLogFault::Unsettled)
		);
	}
	let recovered = rig.restart().await;
	assert!(!recovered.leader.durability_unsettled(), "recovery resets");
	let retry = recovered.fetch_add(write_key, &key, 5).await.unwrap();
	assert_eq!(retry.outcome(), outcome(0, 5));
	recovered.reconcile().await;
	assert_eq!(recovered.pending_witnesses(), 0);
	assert_eq!(recovered.read(&key).await.unwrap(), Some(5));
	drop(recovered);
	let ops: HashSet<u64> = applied_records(&rig.model)
		.into_iter()
		.filter(|record| record.write_key == write_key)
		.map(|record| record.op_id)
		.collect();
	assert!(
		ops.len() <= 1,
		"at most one op id under the write key: {ops:?}"
	);
	assert!(raw_frames_for(&rig.model, write_key) <= 1);
}

// H2: a torn tail is bounded by the frame its owner would have written. A
// corrupt frame (a wrong shape tag, or a valid-looking prefix with an
// oversized length) ahead of valid frames, beside an unrelated or even a
// same-op pending witness, fails recovery closed and leaves every byte.
#[tokio::test]
async fn corrupt_frame_ahead_of_valid_frames_fails_closed_unchanged() {
	for bad_tag in [true, false] {
		let rig = Rig::new(PersistenceModel::new(), 1);
		let store = model_store(&rig.model);
		let pending = fixture_witness(3, wk(43), b"pending");
		store.write(&pending).unwrap();
		let frame = |op_id, n, key: &[u8], previous, current| {
			applied_log::encode_frame(&AppliedCounterAdd {
				op_id,
				write_key: wk(n),
				key: Some(ckey(key)),
				previous,
				current,
			})
			.unwrap()
		};
		let mut bytes = frame(1, 41, b"c", 0, 5);
		let mut corrupt = if bad_tag {
			frame(2, 42, b"c", 5, 6)
		} else {
			// The pending witness's own frame, so only the bound can reject it.
			frame(3, 43, b"pending", 0, 5)
		};
		let tail = frame(4, 44, b"c", 6, 7);
		let oversized = (corrupt.len() + tail.len()) as u32;
		corrupt[..4].copy_from_slice(&oversized.to_le_bytes());
		if bad_tag {
			corrupt[5] = 0x07;
		}
		bytes.extend_from_slice(&corrupt);
		bytes.extend_from_slice(&tail);
		let log = applied_log_path();
		rig.model.put_durable(&log, bytes.clone());
		let witness_path = store.origin_path_for(3);
		let witness_bytes = rig.model.bytes(&witness_path).unwrap();
		drop(store);
		let participants = rig.participants(false).await;
		assert!(
			LeaderHarness::recover(participants, 1, true, model_store(&rig.model)).is_err(),
			"bad tag {bad_tag}: recovery must fail closed"
		);
		assert_eq!(rig.model.bytes(&log), Some(bytes), "bad tag {bad_tag}");
		assert_eq!(rig.model.bytes(&witness_path), Some(witness_bytes));
	}
}

// H2: an unnamed tail is only one too short to hold its tag plus a complete
// op-id varint; a wrong tag or an overlong varint is corruption.
#[test]
fn torn_tails_name_their_operation_or_are_corruption() {
	let tail_of = |payload: &[u8]| {
		let mut bytes = 64u32.to_le_bytes().to_vec();
		bytes.extend_from_slice(payload);
		applied_log::parse(&bytes).map(|image| image.torn_tail.unwrap().op_id)
	};
	assert_eq!(tail_of(&[]).unwrap(), None);
	assert_eq!(tail_of(&[0]).unwrap(), None);
	assert_eq!(tail_of(&[0, 2]).unwrap(), None);
	assert_eq!(tail_of(&[0, 2, 0x80]).unwrap(), None);
	assert_eq!(tail_of(&[0, 2, 0x81, 0x01]).unwrap(), Some(129));
	assert_eq!(tail_of(&[0x05, 0xff]).unwrap(), Some(5));
	assert!(tail_of(&[0, 7, 1]).is_err(), "wrong shape tag");
	assert!(
		tail_of(&[
			0, 2, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80
		])
		.is_err()
	);
	assert_eq!(
		applied_log::parse(&[1, 0])
			.unwrap()
			.torn_tail
			.unwrap()
			.op_id,
		None
	);
}

// (f) One write key naming two operations is a typed identity conflict, in a
// new append and in recovery alike; both representations are kept.
#[tokio::test]
async fn one_write_key_for_two_operations_is_a_typed_identity_conflict() {
	let rig = Rig::new(PersistenceModel::new(), 1);
	let store = model_store(&rig.model);
	let first = fixture_witness(1, wk(61), b"a");
	store.write(&first).unwrap();
	store
		.append_applied(&applied_log::record_for(&first), &first)
		.unwrap();
	let second = fixture_witness(2, wk(61), b"b");
	store.write(&second).unwrap();
	let expected = AppliedLogFault::IdentityConflict {
		write_key: wk(61),
		recorded_op_id: 1,
		conflicting_op_id: 2,
	};
	let error = store
		.append_applied(&applied_log::record_for(&second), &second)
		.unwrap_err();
	assert_eq!(fault_of(&error), Some(expected));
	drop(store);
	rig.model.crash();
	let participants = rig.participants(false).await;
	let Err(error) = LeaderHarness::recover(participants, 1, true, model_store(&rig.model)) else {
		panic!("recovery must refuse a write key with two operations");
	};
	assert_eq!(fault_of(&error), Some(expected));
	assert_eq!(raw_frames_for(&rig.model, wk(61)), 1);
}

/// One add cut after `cut` events, then power-cut: the image recovery starts from.
async fn crashed_image(existing: bool, cut: usize) -> Rig {
	let rig = Rig::new(PersistenceModel::new(), 3);
	{
		let leader = rig.leader().await;
		if existing {
			leader.fetch_add(wk(699), &ckey(b"prior"), 1).await.unwrap();
		}
		rig.model.arm_cut(Cut::Event(cut));
		let _ = leader.fetch_add(wk(700), &ckey(b"sweep"), 5).await;
	}
	rig.model.crash();
	rig
}

/// What a restarted node does: recover, serve a retry, reconcile.
async fn recovery_work(rig: &Rig) {
	let Ok(recovered) = rig.recover_in_place().await else {
		return;
	};
	if let Ok(retry) = recovered.fetch_add(wk(700), &ckey(b"sweep"), 5).await {
		assert_eq!(retry.outcome(), outcome(0, 5), "a retry during recovery");
	}
	recovered.reconcile().await;
}

// C4 nested: crash after every event of recovery and reconciliation
// themselves (the torn-tail truncate and sync, the retried append, the
// re-driven markers, the unlink), from every crash image of an add. Each
// doubly crashed image still recovers exactly one outcome and one frame.
#[tokio::test]
async fn crash_cuts_inside_recovery_and_reconcile_recover_one_outcome() {
	for existing in [false, true] {
		let total = {
			let probe = Rig::new(PersistenceModel::new(), 3);
			let leader = probe.leader().await;
			if existing {
				leader.fetch_add(wk(699), &ckey(b"prior"), 1).await.unwrap();
			}
			probe.model.reset_events();
			leader.fetch_add(wk(700), &ckey(b"sweep"), 5).await.unwrap();
			probe.model.events()
		};
		let mut nested = 0;
		for outer in 1..=total {
			let inner_total = {
				let rig = crashed_image(existing, outer).await;
				rig.model.reset_events();
				recovery_work(&rig).await;
				rig.model.events()
			};
			for inner in 1..=inner_total {
				let context = format!(
					"existing log {existing}, add cut {outer}/{total}, recovery cut {inner}/{inner_total}"
				);
				let rig = crashed_image(existing, outer).await;
				rig.model.arm_cut(Cut::Event(inner));
				recovery_work(&rig).await;
				assert_recovers_one_outcome(
					&rig,
					wk(700),
					&ckey(b"sweep"),
					outcome(0, 5),
					&context,
				)
				.await;
				nested += 1;
			}
		}
		assert!(nested > total, "the nested sweep ran ({nested} cuts)");
	}
}

mod write_key_tests;

mod eligible_park_tests;

mod retention_tests;

mod compaction_tests;
