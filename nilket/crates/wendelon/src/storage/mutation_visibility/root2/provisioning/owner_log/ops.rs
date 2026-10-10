//! Owner I/O primitives and the owner directory's private steps. Nothing here is visible outside
//! `owner_log`; the eight exported methods live in the parent.
use super::super::super::codec::hash;
use super::super::super::types::Digest2;
use super::super::inventory::MAX_LOG_BYTES2;
use super::super::owner_core::{
	Classification2, MAX_EVIDENCE_BYTES2, MAX_REPAIR_ENTRIES2, classify2, decode_evidence2,
	evidence_name2, expected_repair_event2,
};
use super::{
	IoStep2, LOG, MARKER, MARKER_MAGIC, MarkerOutcome2, OwnerDir2, OwnerIoError2, REPAIRS,
	RepairOutcome2,
};
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt};
use std::path::Path;
pub(super) type Owned<T> = Result<T, OwnerIoError2>;

pub(super) fn io_err(step: IoStep2) -> impl Fn(io::Error) -> OwnerIoError2 {
	move |e| OwnerIoError2::Io {
		step,
		errno: e.raw_os_error(),
	}
}
// Every named step passes through here: the test fault plan may fail it, and a test crash stop
// may end the process image after it.
pub(super) fn run<T>(step: IoStep2, f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
	let _ = step;
	#[cfg(test)]
	super::seam::hit(step)?;
	let value = f()?;
	#[cfg(test)]
	super::seam::after(step);
	Ok(value)
}

pub(super) enum Mode {
	Read,
	ReadWrite,
	Create(u32),
}
// No symlink leaf (ELOOP), no blocking open of a FIFO; the caller then requires a regular file.
pub(super) fn open_file(path: &Path, mode: Mode) -> io::Result<File> {
	let mut o = OpenOptions::new();
	match mode {
		Mode::Read => o.read(true),
		Mode::ReadWrite => o.read(true).write(true),
		Mode::Create(perm) => o.read(true).write(true).create_new(true).mode(perm),
	};
	o.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
	o.open(path)
}
pub(super) fn regular(file: File, step: IoStep2) -> Owned<File> {
	let m = file.metadata().map_err(io_err(step))?;
	if !m.file_type().is_file() {
		return Err(OwnerIoError2::NotRegular { step });
	}
	Ok(file)
}
pub(super) fn open_dir(path: &Path) -> io::Result<File> {
	OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
		.open(path)
}
pub(super) fn checked_dir(dir: &File, step: IoStep2) -> Owned<(u64, u64)> {
	let m = dir.metadata().map_err(io_err(step))?;
	if !m.is_dir() {
		return Err(OwnerIoError2::NotRegular { step });
	}
	if m.mode() & 0o022 != 0 {
		return Err(OwnerIoError2::InsecureDir);
	}
	Ok((m.dev(), m.ino()))
}
// Requested storage is exactly the limit; the file's reported length is never trusted.
pub(super) fn bounded(mut file: File, limit: usize, step: IoStep2) -> Owned<Vec<u8>> {
	let mut buf = Vec::new();
	buf.try_reserve_exact(limit)
		.map_err(|_| OwnerIoError2::Io {
			step,
			errno: Some(libc::ENOMEM),
		})?;
	buf.resize(limit, 0);
	let mut n = 0;
	run(step, || Ok(())).map_err(io_err(step))?;
	while n < limit {
		match file.read(&mut buf[n..]) {
			Ok(0) => break,
			Ok(k) => n += k,
			Err(e) if e.kind() == ErrorKind::Interrupted => {}
			Err(e) => return Err(io_err(step)(e)),
		}
	}
	buf.truncate(n);
	Ok(buf)
}
pub(super) fn sha(bytes: &[u8]) -> Digest2 {
	hash(b"", bytes)
}
pub(super) fn export_name_ok(name: &str) -> bool {
	!name.is_empty()
		&& name.len() <= 64
		&& name
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
		&& !name.starts_with('.')
		&& !name.ends_with(".tmp")
}

pub(super) struct View {
	pub(super) log: Option<Vec<u8>>,
	pub(super) entries: Vec<(String, Vec<u8>)>,
	pub(super) classification: Classification2,
}
impl View {
	pub(super) fn bytes(&self) -> &[u8] {
		self.log.as_deref().unwrap_or(&[])
	}
	pub(super) fn rep_names(&self) -> Vec<String> {
		self.entries
			.iter()
			.filter(|(n, _)| n.ends_with(".rep"))
			.map(|(n, _)| n.clone())
			.collect()
	}
}

impl OwnerDir2 {
	pub(super) fn marker_stat(&self) -> Owned<Option<bool>> {
		match run(IoStep2::MarkerStat, || {
			fs::symlink_metadata(self.path.join(MARKER))
		}) {
			Ok(m) => Ok(Some(m.file_type().is_file())),
			Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
			Err(e) => Err(io_err(IoStep2::MarkerStat)(e)),
		}
	}
	pub(super) fn blocked(&self) -> Owned<()> {
		if let Some(step) = self.poisoned {
			return Err(OwnerIoError2::Poisoned { step });
		}
		match self.marker_stat()? {
			Some(_) => Err(OwnerIoError2::SyncFailurePending),
			None => Ok(()),
		}
	}
	// Latch first, then exactly one non-recursive marker attempt; the original step is reported.
	pub(super) fn synced(&mut self, step: IoStep2, r: io::Result<()>) -> Owned<()> {
		if r.is_ok() {
			return Ok(());
		}
		self.poisoned = Some(step);
		let marker = self.set_marker(step);
		Err(OwnerIoError2::SyncFailed { step, marker })
	}
	// Clear's own sync failures latch without a new marker attempt.
	pub(super) fn clear_synced(
		&mut self,
		step: IoStep2,
		r: io::Result<()>,
		marker_present: bool,
	) -> Owned<()> {
		if r.is_ok() {
			return Ok(());
		}
		self.poisoned = Some(step);
		Err(OwnerIoError2::SyncFailed {
			step,
			marker: if marker_present {
				MarkerOutcome2::AlreadyPresent
			} else {
				MarkerOutcome2::NotDurable
			},
		})
	}
	pub(super) fn set_marker(&self, step: IoStep2) -> MarkerOutcome2 {
		let path = self.path.join(MARKER);
		let file = match run(IoStep2::MarkerCreate, || {
			open_file(&path, Mode::Create(0o600))
		}) {
			Ok(f) => f,
			Err(e) if e.kind() == ErrorKind::AlreadyExists => {
				return MarkerOutcome2::AlreadyPresent;
			}
			Err(_) => return MarkerOutcome2::NotDurable,
		};
		let log_len = fs::symlink_metadata(self.path.join(LOG)).map_or(0, |m| m.len());
		let mut body = Vec::with_capacity(17);
		body.extend_from_slice(MARKER_MAGIC);
		body.push(step as u8);
		body.extend_from_slice(&log_len.to_le_bytes());
		if run(IoStep2::MarkerWrite, || (&file).write_all(&body)).is_err()
			|| run(IoStep2::MarkerSync, || file.sync_data()).is_err()
			|| run(IoStep2::MarkerDirSync, || self.dir.sync_all()).is_err()
		{
			return MarkerOutcome2::NotDurable;
		}
		MarkerOutcome2::Durable
	}
	pub(super) fn log_file(&self, mode: Mode) -> Owned<File> {
		let f = run(IoStep2::LogOpen, || open_file(&self.path.join(LOG), mode))
			.map_err(io_err(IoStep2::LogOpen))?;
		regular(f, IoStep2::LogOpen)
	}
	pub(super) fn log_for_write(&self, create: bool) -> Owned<File> {
		if !create {
			return self.log_file(Mode::ReadWrite);
		}
		let f = run(IoStep2::LogCreate, || {
			open_file(&self.path.join(LOG), Mode::Create(0o600))
		})
		.map_err(io_err(IoStep2::LogCreate))?;
		regular(f, IoStep2::LogCreate)
	}
	// None only for ENOENT; any other failure is an error, never an absent log.
	pub(super) fn read_log(&self) -> Owned<Option<Vec<u8>>> {
		let f = match run(IoStep2::LogOpen, || {
			open_file(&self.path.join(LOG), Mode::Read)
		}) {
			Ok(f) => regular(f, IoStep2::LogOpen)?,
			Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
			Err(e) => return Err(io_err(IoStep2::LogOpen)(e)),
		};
		bounded(f, MAX_LOG_BYTES2 + 1, IoStep2::LogRead).map(Some)
	}
	pub(super) fn repairs_dir(&self) -> Owned<File> {
		let d = run(IoStep2::RepairsOpen, || open_dir(&self.path.join(REPAIRS)))
			.map_err(io_err(IoStep2::RepairsOpen))?;
		checked_dir(&d, IoStep2::RepairsOpen)?;
		Ok(d)
	}
	pub(super) fn evidence_file(&self, name: &str) -> Owned<File> {
		let f = run(IoStep2::EvidenceRead, || {
			open_file(&self.path.join(REPAIRS).join(name), Mode::Read)
		})
		.map_err(io_err(IoStep2::EvidenceRead))?;
		regular(f, IoStep2::EvidenceRead)
	}
	// Bounded, sorted enumeration; tmp content is never read.
	pub(super) fn entries(&self) -> Owned<Vec<(String, Vec<u8>)>> {
		let path = self.path.join(REPAIRS);
		match run(IoStep2::RepairsOpen, || open_dir(&path)) {
			Ok(d) => checked_dir(&d, IoStep2::RepairsOpen).map(|_| ())?,
			Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
			Err(e) => return Err(io_err(IoStep2::RepairsOpen)(e)),
		}
		let listing = run(IoStep2::RepairsEnum, || fs::read_dir(&path))
			.map_err(io_err(IoStep2::RepairsEnum))?;
		let mut found: Vec<(Option<String>, bool)> = Vec::new();
		for entry in listing {
			if found.len() == MAX_REPAIR_ENTRIES2 {
				return Err(OwnerIoError2::Classified(Classification2::RepairDirFull));
			}
			let entry = entry.map_err(io_err(IoStep2::RepairsEnum))?;
			let is_file = entry
				.file_type()
				.map_err(io_err(IoStep2::RepairsEnum))?
				.is_file();
			found.push((entry.file_name().to_str().map(str::to_owned), is_file));
		}
		found.sort();
		let mut entries = Vec::with_capacity(found.len());
		for (index, (name, is_file)) in found.into_iter().enumerate() {
			let foreign = OwnerIoError2::Classified(Classification2::RepairDirForeign {
				index: index as u32,
			});
			let Some(name) = name.filter(|_| is_file) else {
				return Err(foreign);
			};
			let bytes = if name.ends_with(".rep") {
				bounded(
					self.evidence_file(&name)?,
					MAX_EVIDENCE_BYTES2 + 1,
					IoStep2::EvidenceRead,
				)?
			} else {
				Vec::new()
			};
			entries.push((name, bytes));
		}
		Ok(entries)
	}
	pub(super) fn view(&self) -> Owned<View> {
		let log = self.read_log()?;
		let entries = match self.entries() {
			Ok(e) => e,
			Err(OwnerIoError2::Classified(c)) => {
				return Ok(View {
					log,
					entries: Vec::new(),
					classification: c,
				});
			}
			Err(e) => return Err(e),
		};
		let refs: Vec<(&str, &[u8])> = entries
			.iter()
			.map(|(n, b)| (n.as_str(), b.as_slice()))
			.collect();
		let classification = classify2(log.as_deref().unwrap_or(&[]), &refs);
		Ok(View {
			log,
			entries,
			classification,
		})
	}
	pub(super) fn clean_view(&self) -> Owned<View> {
		let view = self.view()?;
		match view.classification {
			Classification2::Clean { .. } => Ok(view),
			other => Err(OwnerIoError2::Classified(other)),
		}
	}
	// Re-sync before a log-dependent success: the log (an absent log is a directory state), each
	// relied-on evidence file, `repairs/`, and the owner directory last.
	pub(super) fn resync(&mut self, log_present: bool, evidence: &[String]) -> Owned<()> {
		if log_present {
			let file = self.log_file(Mode::Read)?;
			let r = run(IoStep2::LogSync, || file.sync_data());
			self.synced(IoStep2::LogSync, r)?;
		}
		if !evidence.is_empty() {
			for name in evidence {
				let file = self.evidence_file(name)?;
				let r = run(IoStep2::EvidenceSync, || file.sync_data());
				self.synced(IoStep2::EvidenceSync, r)?;
			}
			let repairs = self.repairs_dir()?;
			let r = run(IoStep2::RepairsDirSync, || repairs.sync_all());
			self.synced(IoStep2::RepairsDirSync, r)?;
		}
		let r = run(IoStep2::OwnerDirSync, || self.dir.sync_all());
		self.synced(IoStep2::OwnerDirSync, r)
	}
	pub(super) fn ensure_repairs(&mut self) -> Owned<()> {
		let path = self.path.join(REPAIRS);
		match run(IoStep2::RepairsCreate, || {
			DirBuilder::new().mode(0o700).create(&path)
		}) {
			Ok(()) => {
				let r = run(IoStep2::OwnerDirSync, || self.dir.sync_all());
				self.synced(IoStep2::OwnerDirSync, r)
			}
			Err(e) if e.kind() == ErrorKind::AlreadyExists => self.repairs_dir().map(|_| ()),
			Err(e) => Err(io_err(IoStep2::RepairsCreate)(e)),
		}
	}
	// Exclusive publication: link(2) never replaces an existing name.
	pub(super) fn publish_evidence(&mut self, name: &str, bytes: &[u8]) -> Owned<()> {
		let repairs = self.path.join(REPAIRS);
		let tmp = repairs.join(format!("{name}.tmp"));
		let target = repairs.join(name);
		let file = match run(IoStep2::EvidenceTmpCreate, || {
			open_file(&tmp, Mode::Create(0o600))
		}) {
			Ok(f) => f,
			Err(e) if e.kind() == ErrorKind::AlreadyExists => {
				// A stale tmp is not evidence: one unlink, one directory sync, one retry.
				run(IoStep2::EvidenceTmpUnlink, || fs::remove_file(&tmp))
					.map_err(io_err(IoStep2::EvidenceTmpUnlink))?;
				let dir = self.repairs_dir()?;
				let r = run(IoStep2::RepairsDirSync, || dir.sync_all());
				self.synced(IoStep2::RepairsDirSync, r)?;
				run(IoStep2::EvidenceTmpCreate, || {
					open_file(&tmp, Mode::Create(0o600))
				})
				.map_err(io_err(IoStep2::EvidenceTmpCreate))?
			}
			Err(e) => return Err(io_err(IoStep2::EvidenceTmpCreate)(e)),
		};
		let file = regular(file, IoStep2::EvidenceTmpCreate)?;
		run(IoStep2::EvidenceWrite, || (&file).write_all(bytes))
			.map_err(io_err(IoStep2::EvidenceWrite))?;
		let r = run(IoStep2::EvidenceSync, || file.sync_data());
		self.synced(IoStep2::EvidenceSync, r)?;
		match run(IoStep2::EvidenceLink, || fs::hard_link(&tmp, &target)) {
			Ok(()) => {}
			Err(e) if e.kind() == ErrorKind::AlreadyExists => {
				let existing = bounded(
					self.evidence_file(name)?,
					MAX_EVIDENCE_BYTES2 + 1,
					IoStep2::EvidenceRead,
				)?;
				if existing != bytes {
					return Err(OwnerIoError2::EvidenceInvalid);
				}
			}
			Err(e) => return Err(io_err(IoStep2::EvidenceLink)(e)),
		}
		run(IoStep2::EvidenceTmpUnlink, || fs::remove_file(&tmp))
			.map_err(io_err(IoStep2::EvidenceTmpUnlink))?;
		let dir = self.repairs_dir()?;
		let r = run(IoStep2::RepairsDirSync, || dir.sync_all());
		self.synced(IoStep2::RepairsDirSync, r)?;
		let r = run(IoStep2::OwnerDirSync, || self.dir.sync_all());
		self.synced(IoStep2::OwnerDirSync, r)
	}
	// r7 §4.4 resume: re-sync the evidence, truncate, verify, append the expected Repair, verify.
	pub(super) fn resume(&mut self, context: &Digest2) -> Owned<RepairOutcome2> {
		let view = self.view()?;
		match view.classification {
			Classification2::Pending { context: c, .. } if c == *context => {}
			Classification2::Pending { .. } => return Err(OwnerIoError2::RepairContextMismatch),
			other => return Err(OwnerIoError2::Classified(other)),
		}
		let name = evidence_name2(context);
		let Some((_, bytes)) = view.entries.iter().find(|(n, _)| *n == name) else {
			return Err(OwnerIoError2::RepairContextMismatch);
		};
		let evidence =
			decode_evidence2(&name, bytes).map_err(|_| OwnerIoError2::EvidenceInvalid)?;
		let expected =
			expected_repair_event2(&evidence).map_err(|_| OwnerIoError2::EvidenceInvalid)?;
		self.resync(view.log.is_some(), std::slice::from_ref(&name))?;
		let file = self.log_for_write(view.log.is_none())?;
		run(IoStep2::Truncate, || file.set_len(evidence.boundary))
			.map_err(io_err(IoStep2::Truncate))?;
		let r = run(IoStep2::TruncateSync, || file.sync_data());
		self.synced(IoStep2::TruncateSync, r)?;
		let truncated = self.read_log()?.unwrap_or_default();
		if truncated.len() as u64 != evidence.boundary || sha(&truncated) != evidence.prefix_sha256
		{
			return Err(OwnerIoError2::RepairVerifyFailed);
		}
		run(IoStep2::LogWrite, || {
			file.write_all_at(&expected, evidence.boundary)
		})
		.map_err(io_err(IoStep2::LogWrite))?;
		let r = run(IoStep2::LogSync, || file.sync_data());
		self.synced(IoStep2::LogSync, r)?;
		let r = run(IoStep2::OwnerDirSync, || self.dir.sync_all());
		self.synced(IoStep2::OwnerDirSync, r)?;
		match self.view()?.classification {
			Classification2::Clean { last_seq } if last_seq == evidence.repair_seq => {
				Ok(RepairOutcome2::Repaired {
					context: *context,
					seq: evidence.repair_seq,
				})
			}
			_ => Err(OwnerIoError2::RepairVerifyFailed),
		}
	}
}
