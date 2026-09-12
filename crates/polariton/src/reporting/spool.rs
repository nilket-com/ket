//! Immutable segments; only publication, progress and retirement share a lock.
use super::*;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, Write};
const CAP: usize = 10_000;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Generation {
	pub id: String,
	pub install: String,
	pub destination: Option<Destination>,
}
#[derive(Serialize, Deserialize)]
struct Header {
	generation: String,
	pid: u32,
	start: String,
}
#[derive(Serialize, Deserialize)]
struct Progress {
	segment: String,
	generation: String,
	count: usize,
	checksum: String,
}
pub(super) struct Spool {
	root: PathBuf,
	pub generation: Generation,
	dropped: u64,
}
fn io(e: impl std::fmt::Display) -> String {
	e.to_string()
}
fn lock(root: &Path, wait: bool) -> Result<File, String> {
	let file = OpenOptions::new()
		.create(true)
		.truncate(false)
		.read(true)
		.write(true)
		.open(root.join("reporting.lock"))
		.map_err(io)?;
	if wait {
		file.lock().map_err(io)?;
	} else {
		file.try_lock()
			.map_err(|_| "reporting queue is owned by another drainer".to_string())?;
	}
	Ok(file)
}
#[cfg(unix)]
fn sync_dir(root: &Path) -> Result<(), String> {
	File::open(root).and_then(|f| f.sync_all()).map_err(io)
}
/// Windows has no directory fsync: opening a directory needs backup semantics
/// and flushing it needs write access, so the entry is left to the volume.
#[cfg(not(unix))]
fn sync_dir(_root: &Path) -> Result<(), String> {
	Ok(())
}
fn publish<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
	let temporary = path.with_extension(format!("tmp-{}", id()));
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&temporary)
		.map_err(io)?;
	file.write_all(&bytes(value)?).map_err(io)?;
	file.sync_all().map_err(io)?;
	#[cfg(test)]
	if path.extension().is_some_and(|e| e == "progress") {
		crash_gate("progress");
	}
	fs::rename(temporary, path).map_err(io)?;
	sync_dir(path.parent().unwrap())
}
fn generation(root: &Path) -> Result<Generation, String> {
	read(&root.join("generation.json"))
}
fn token(pid: u32) -> Option<String> {
	crate::process_identity::process_start(pid)
}
fn progress_path(segment: &Path) -> PathBuf {
	segment.with_extension("progress")
}
fn checksum(segment: &str, generation: &str, count: usize) -> String {
	format!(
		"{:x}",
		Sha256::digest(format!("{segment}\0{generation}\0{count}").as_bytes())
	)
}
fn count(segment: &Path, gen_id: &str, length: usize) -> usize {
	let Ok(p): Result<Progress, _> = read(&progress_path(segment)) else {
		return 0;
	};
	let name = segment.file_name().unwrap().to_string_lossy();
	if p.segment != name
		|| p.generation != gen_id
		|| p.count > length
		|| p.checksum != checksum(&p.segment, &p.generation, p.count)
	{
		0
	} else {
		p.count
	}
}
fn entries(root: &Path, extension: &str) -> Result<Vec<PathBuf>, String> {
	let mut paths = fs::read_dir(root)
		.map_err(io)?
		.filter_map(Result::ok)
		.map(|e| e.path())
		.filter(|p| p.extension().is_some_and(|e| e == extension))
		.collect::<Vec<_>>();
	paths.sort();
	Ok(paths)
}
fn header(path: &Path) -> Result<Header, String> {
	let mut reader = std::io::BufReader::new(File::open(path).map_err(io)?);
	let mut line = Vec::new();
	reader.read_until(b'\n', &mut line).map_err(io)?;
	super::decode(&line)
}
fn segment(path: &Path) -> Result<(Header, Vec<Report>), String> {
	let data = fs::read(path).map_err(io)?;
	let mut lines = data.split_inclusive(|b| *b == b'\n');
	let header = super::decode(lines.next().ok_or("empty segment")?)?;
	let mut reports = Vec::new();
	for line in lines {
		if !line.ends_with(b"\n") {
			break;
		}
		reports.push(super::decode(line)?);
	}
	Ok((header, reports))
}
fn remove(path: &Path) -> Result<(), String> {
	match fs::remove_file(path) {
		Ok(()) => Ok(()),
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(e) => Err(io(e)),
	}
}
fn retire(path: &Path) -> Result<(), String> {
	// Persist removal of the segment first; an orphan progress file is harmless.
	remove(path)?;
	sync_dir(path.parent().unwrap())?;
	remove(&progress_path(path))?;
	sync_dir(path.parent().unwrap())
}
impl Spool {
	pub fn open(root: PathBuf, destination: Destination) -> Result<Self, String> {
		fs::create_dir_all(&root).map_err(io)?;
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).map_err(io)?;
		}
		// Persist the newly created directory entries before acknowledging
		// any metadata or segment publication below them.
		for ancestor in root.ancestors().skip(1) {
			sync_dir(ancestor)?;
		}
		let _lock = lock(&root, true)?;
		let path = root.join("generation.json");
		let previous = if path.exists() {
			Some(generation(&root)?)
		} else {
			None
		};
		let current = match previous {
			Some(g) if g.destination.as_ref() == Some(&destination) => g,
			Some(g) if g.destination.is_some() => {
				return Err("disable reporting before changing its destination".into());
			}
			_ => {
				let g = Generation {
					id: id(),
					install: id(),
					destination: Some(destination),
				};
				publish(&path, &g)?;
				g
			}
		};
		Ok(Self {
			root,
			generation: current,
			dropped: 0,
		})
	}
	pub fn existing(root: PathBuf, destination: &Destination) -> Result<Self, String> {
		let generation = generation(&root)?;
		if generation.destination.as_ref() != Some(destination) {
			return Err("reporting generation or destination differs".into());
		}
		Ok(Self {
			root,
			generation,
			dropped: 0,
		})
	}
	pub fn current(&self) -> Result<bool, String> {
		Ok(generation(&self.root)? == self.generation && self.generation.destination.is_some())
	}
	pub fn persist(&mut self, run: &str, index: u64, reports: &[Report]) -> Result<u64, String> {
		// Lock creation too: an old process cannot create a file after disable cleanup.
		let path;
		let header = Header {
			generation: self.generation.id.clone(),
			pid: std::process::id(),
			start: token(std::process::id()).ok_or("cannot identify writer process")?,
		};
		let mut file = {
			let _lock = lock(&self.root, true)?;
			if !self.current()? {
				return Err("reporting was disabled".into());
			}
			let ordinal = entries(&self.root, "active")?
				.into_iter()
				.chain(entries(&self.root, "sealed")?)
				.filter_map(|p| {
					p.file_name()?
						.to_str()?
						.split('-')
						.next()?
						.parse::<u64>()
						.ok()
				})
				.max()
				.unwrap_or(0)
				.checked_add(1)
				.ok_or("segment ordinals exhausted")?;
			path = self
				.root
				.join(format!("{ordinal:020}-{run}-{index:020}.active"));
			let mut file = OpenOptions::new()
				.create_new(true)
				.write(true)
				.open(&path)
				.map_err(io)?;
			// A reader holding the lock sees either a complete header or a
			// dead creator's incomplete header; records follow only afterwards.
			if let Err(e) = file
				.write_all(&bytes(&header)?)
				.and_then(|_| file.write_all(b"\n"))
			{
				drop(file);
				remove(&path)?;
				return Err(io(e));
			}
			file
		};
		let write = (|| -> Result<(), String> {
			for report in reports {
				file.write_all(&bytes(report)?).map_err(io)?;
				file.write_all(b"\n").map_err(io)?;
				#[cfg(test)]
				crash_gate("append");
			}
			file.sync_data().map_err(io)
		})();
		if let Err(error) = write {
			drop(file);
			remove(&path)?;
			return Err(error);
		}
		drop(file);
		let _lock = lock(&self.root, true)?;
		if !self.current()? {
			remove(&path)?;
			return Err("reporting was disabled".into());
		}
		let dropped = self.admit(reports.len())?;
		fs::rename(&path, path.with_extension("sealed")).map_err(io)?;
		sync_dir(&self.root)?;
		Ok(dropped)
	}
	fn admit(&self, incoming: usize) -> Result<u64, String> {
		if incoming > CAP {
			return Err("segment exceeds queue limit".into());
		}
		let mut existing = Vec::new();
		for path in entries(&self.root, "sealed")? {
			let h = header(&path)?;
			if h.generation != self.generation.id {
				retire(&path)?;
				continue;
			}
			let (_, r) = segment(&path)?;
			let remaining = r.len() - count(&path, &h.generation, r.len());
			existing.push((path, remaining));
		}
		let mut size = incoming + existing.iter().map(|(_, n)| n).sum::<usize>();
		let mut dropped = 0;
		for (path, remaining) in existing {
			if size <= CAP {
				break;
			}
			retire(&path)?;
			size -= remaining;
			dropped += remaining as u64;
		}
		Ok(dropped)
	}
	fn recover(&mut self) -> Result<(), String> {
		for ext in ["sealed", "active"] {
			for path in entries(&self.root, ext)? {
				let header = match header(&path) {
					Ok(h) => h,
					Err(_) if ext == "active" => {
						retire(&path)?;
						continue;
					}
					Err(e) => return Err(e),
				};
				if header.generation != self.generation.id {
					retire(&path)?;
					continue;
				}
				if ext == "active" && token(header.pid).as_deref() != Some(&header.start) {
					// Only a dead writer's file can be truncated during recovery.
					let (_, reports) = segment(&path)?;
					let data = fs::read(&path).map_err(io)?;
					let end = data.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
					let file = OpenOptions::new().write(true).open(&path).map_err(io)?;
					file.set_len(end as u64).map_err(io)?;
					file.sync_data().map_err(io)?;
					self.dropped += self.admit(reports.len())? + u64::from(end < data.len());
					fs::rename(&path, path.with_extension("sealed")).map_err(io)?;
					sync_dir(&self.root)?;
				}
			}
		}
		Ok(())
	}
	pub fn take_dropped(&mut self) -> u64 {
		std::mem::take(&mut self.dropped)
	}
	pub fn drain(
		&mut self,
		mut send: impl FnMut(&Report) -> Result<(), String>,
	) -> Result<(), String> {
		{
			let _lock = lock(&self.root, false)?;
			if !self.current()? {
				return Err("reporting was disabled".into());
			}
			self.recover()?;
		}
		loop {
			// Release at each acknowledged boundary. Revocation and another
			// writer can acquire between attempts; never retain a stale cursor.
			let _lock = lock(&self.root, false)?;
			if !self.current()? {
				return Err("reporting was disabled".into());
			}
			let Some(path) = entries(&self.root, "sealed")?.into_iter().next() else {
				return Ok(());
			};
			let h = header(&path)?;
			if h.generation != self.generation.id {
				retire(&path)?;
				continue;
			}
			let (_, reports) = segment(&path)?;
			let i = count(&path, &h.generation, reports.len());
			if i == reports.len() {
				retire(&path)?;
				continue;
			}
			send(&reports[i])?;
			let name = path.file_name().unwrap().to_string_lossy().into_owned();
			let progress = Progress {
				checksum: checksum(&name, &h.generation, i + 1),
				segment: name,
				generation: h.generation,
				count: i + 1,
			};
			publish(&progress_path(&path), &progress)?;
			if i + 1 == reports.len() {
				retire(&path)?;
			}
		}
	}
}
pub(super) fn disable(
	root: &Path,
	mut send: impl FnMut(&Destination, &Report) -> Result<(), String>,
) -> Result<(), String> {
	let _lock = lock(root, true)?;
	let old = generation(root)?;
	if let Some(destination) = &old.destination {
		publish(
			&root.join("generation.json"),
			&Generation {
				id: id(),
				install: String::new(),
				destination: None,
			},
		)?;
		#[cfg(test)]
		crash_gate("disable");
		let report = Report {
			report_id: id(),
			install_id: old.install,
			run_id: id(),
			sequence: 0,
			journey_id: None,
			step: Step::ReportingDisabled,
			outcome: "ok".into(),
			reason: None,
			dropped: 0,
			platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
			app_version: env!("CARGO_PKG_VERSION").into(),
			at_ms: now(),
		};
		let _ = send(destination, &report);
	}
	for item in fs::read_dir(root).map_err(io)? {
		let path = item.map_err(io)?.path();
		if !matches!(
			path.file_name().and_then(|s| s.to_str()),
			Some("reporting.lock" | "generation.json")
		) {
			remove(&path)?;
		}
	}
	sync_dir(root)
}

#[cfg(test)]
fn crash_gate(phase: &str) {
	if std::env::var("POLARITON_0161_CRASH_PHASE").as_deref() != Ok(phase) {
		return;
	}
	let path = std::env::var_os("POLARITON_0161_CRASH_READY").unwrap();
	fs::write(path, phase).unwrap();
	loop {
		std::thread::park();
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	struct Scratch(PathBuf);
	impl Scratch {
		fn new() -> Self {
			Self(std::env::temp_dir().join(format!("polariton-report-test-{}", id())))
		}
	}
	impl Drop for Scratch {
		fn drop(&mut self) {
			fs::remove_dir_all(&self.0).ok();
		}
	}
	fn destination() -> Destination {
		Destination {
			endpoint: "127.0.0.1:1".into(),
			pki: PathBuf::from("test-only-pki"),
		}
	}
	fn report(spool: &Spool, sequence: u64) -> Report {
		Report {
			report_id: id(),
			install_id: spool.generation.install.clone(),
			run_id: "run".into(),
			sequence,
			journey_id: None,
			step: Step::RunOpened,
			outcome: "ok".into(),
			reason: None,
			dropped: 0,
			platform: "test".into(),
			app_version: "test".into(),
			at_ms: now(),
		}
	}
	#[test]
	fn unknown_ack_retries_identical_values_and_then_retires() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		let row = report(&spool, 0);
		spool.persist("run", 0, &[row.clone()]).unwrap();
		let mut rows = std::collections::HashMap::new();
		assert!(
			spool
				.drain(|r| {
					rows.insert(r.report_id.clone(), r.clone());
					Err("lost ack".into())
				})
				.is_err()
		);
		assert_eq!(entries(&root.0, "sealed").unwrap().len(), 1);
		spool
			.drain(|r| {
				assert_eq!(r, &row);
				rows.insert(r.report_id.clone(), r.clone());
				Ok(())
			})
			.unwrap();
		assert_eq!(rows.len(), 1);
		assert!(entries(&root.0, "sealed").unwrap().is_empty());
	}
	#[test]
	fn progress_is_bound_and_in_range_corruption_replays() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		let reports = (0..3).map(|i| report(&spool, i)).collect::<Vec<_>>();
		spool.persist("run", 0, &reports).unwrap();
		let path = entries(&root.0, "sealed").unwrap().remove(0);
		let name = path.file_name().unwrap().to_str().unwrap();
		let good = Progress {
			segment: name.into(),
			generation: spool.generation.id.clone(),
			count: 1,
			checksum: checksum(name, &spool.generation.id, 1),
		};
		publish(&progress_path(&path), &good).unwrap();
		assert_eq!(count(&path, &spool.generation.id, 3), 1);
		for bad in [
			Progress { count: 2, ..good },
			Progress {
				segment: "other".into(),
				generation: spool.generation.id.clone(),
				count: 2,
				checksum: checksum("other", &spool.generation.id, 2),
			},
		] {
			publish(&progress_path(&path), &bad).unwrap();
			assert_eq!(count(&path, &spool.generation.id, 3), 0);
		}
		fs::write(progress_path(&path), b"broken").unwrap();
		assert_eq!(count(&path, &spool.generation.id, 3), 0);
	}
	#[test]
	fn durable_revocation_before_cleanup_prevents_recovery_after_reenable() {
		let root = Scratch::new();
		let mut old = Spool::open(root.0.clone(), destination()).unwrap();
		old.persist("run", 0, &[report(&old, 0)]).unwrap();
		// Simulate a crash just after durable revocation, before unlinking.
		publish(
			&root.0.join("generation.json"),
			&Generation {
				id: id(),
				install: String::new(),
				destination: None,
			},
		)
		.unwrap();
		let mut fresh = Spool::open(root.0.clone(), destination()).unwrap();
		assert_ne!(old.generation.install, fresh.generation.install);
		assert!(old.persist("run", 1, &[report(&old, 1)]).is_err());
		fresh.drain(|_| panic!("revoked report sent")).unwrap();
		assert!(entries(&root.0, "sealed").unwrap().is_empty());
	}
	#[test]
	fn off_cleanup_keeps_stable_lock_and_does_not_repeat_final_report() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		spool.persist("run", 0, &[report(&spool, 0)]).unwrap();
		let mut finals = 0;
		disable(&root.0, |_, r| {
			assert_eq!(r.step, Step::ReportingDisabled);
			finals += 1;
			Ok(())
		})
		.unwrap();
		disable(&root.0, |_, _| panic!("off must stay silent")).unwrap();
		assert_eq!(finals, 1);
		assert_eq!(fs::read_dir(&root.0).unwrap().count(), 2);
	}
	#[test]
	fn two_drainers_cannot_own_one_directory() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		let _held = lock(&root.0, false).unwrap();
		assert!(spool.drain(|_| Ok(())).is_err());
	}
	#[test]
	fn dead_active_recovery_at_cap_counts_only_unacknowledged_evictions_and_torn_tail() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		for n in 0..50 {
			let reports = (0..200)
				.map(|i| report(&spool, n * 200 + i))
				.collect::<Vec<_>>();
			spool.persist("run", n, &reports).unwrap();
		}
		let oldest = entries(&root.0, "sealed").unwrap().remove(0);
		let name = oldest.file_name().unwrap().to_str().unwrap();
		publish(
			&progress_path(&oldest),
			&Progress {
				segment: name.into(),
				generation: spool.generation.id.clone(),
				count: 100,
				checksum: checksum(name, &spool.generation.id, 100),
			},
		)
		.unwrap();
		let active = root.0.join("99999999999999999999-dead.active");
		let header = Header {
			generation: spool.generation.id.clone(),
			pid: u32::MAX,
			start: "dead".into(),
		};
		let mut data = bytes(&header).unwrap();
		data.push(b'\n');
		for i in 0..200 {
			data.extend(bytes(&report(&spool, 10000 + i)).unwrap());
			data.push(b'\n');
		}
		data.extend(b"{torn");
		fs::write(&active, data).unwrap();
		let _held = lock(&root.0, true).unwrap();
		spool.recover().unwrap();
		assert_eq!(spool.take_dropped(), 101);
		assert!(!oldest.exists());
		let remaining: usize = entries(&root.0, "sealed")
			.unwrap()
			.iter()
			.map(|p| {
				let (h, r) = segment(p).unwrap();
				r.len() - count(p, &h.generation, r.len())
			})
			.sum();
		assert_eq!(remaining, CAP);
		assert!(!active.exists());
	}
	#[test]
	fn live_active_is_not_read_and_revoked_body_is_not_decoded() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		let active = root.0.join("live.active");
		let mut data = bytes(&Header {
			generation: spool.generation.id.clone(),
			pid: std::process::id(),
			start: token(std::process::id()).unwrap(),
		})
		.unwrap();
		data.extend(b"\ninvalid unfinished bytes");
		fs::write(&active, &data).unwrap();
		spool.drain(|_| panic!("active file sent")).unwrap();
		assert_eq!(fs::read(&active).unwrap(), data);
		let revoked = root.0.join("old.sealed");
		let mut data = bytes(&Header {
			generation: "revoked".into(),
			pid: std::process::id(),
			start: token(std::process::id()).unwrap(),
		})
		.unwrap();
		data.extend(b"\nnot a report\n");
		fs::write(&revoked, data).unwrap();
		spool.drain(|_| panic!("revoked file sent")).unwrap();
		assert!(!revoked.exists());
	}
	#[test]
	fn unpublished_progress_temporary_does_not_skip_a_report() {
		let root = Scratch::new();
		let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
		spool.persist("run", 0, &[report(&spool, 0)]).unwrap();
		let path = entries(&root.0, "sealed").unwrap().remove(0);
		let name = path.file_name().unwrap().to_str().unwrap();
		let temporary = progress_path(&path).with_extension("tmp-crash");
		fs::write(
			&temporary,
			bytes(&Progress {
				segment: name.into(),
				generation: spool.generation.id.clone(),
				count: 1,
				checksum: checksum(name, &spool.generation.id, 1),
			})
			.unwrap(),
		)
		.unwrap();
		let mut sent = 0;
		spool
			.drain(|_| {
				sent += 1;
				Ok(())
			})
			.unwrap();
		assert_eq!(sent, 1);
	}

	#[test]
	fn closed_report_schema_does_not_carry_error_text() {
		let root = Scratch::new();
		let spool = Spool::open(root.0.clone(), destination()).unwrap();
		let mut r = report(&spool, 0);
		r.reason = Some(Reason::FileNotFound);
		let sql = super::super::insert_sql(&r);
		assert!(sql.contains("file.not_found"));
		assert!(!sql.contains("test-only-pki"));
	}
	#[test]
	fn product_0161_crash_child() {
		let Ok(phase) = std::env::var("POLARITON_0161_CRASH_PHASE") else {
			return;
		};
		let root = PathBuf::from(std::env::var_os("POLARITON_0161_CRASH_ROOT").unwrap());
		let mut spool = Spool::open(root.clone(), destination()).unwrap();
		let r = report(&spool, 0);
		fs::write(root.join("expected-id"), &r.report_id).unwrap();
		#[cfg(unix)]
		if phase == "append-error" {
			// Confine the real I/O failure to this child process. The header fits,
			// the report batch does not: write_all must encounter EFBIG.
			unsafe {
				libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
				let limit = libc::rlimit {
					rlim_cur: 512,
					rlim_max: 512,
				};
				assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limit), 0);
			}
			assert!(spool.persist("child", 0, &[r.clone(), r]).is_err());
			assert!(entries(&root, "active").unwrap().is_empty());
			assert!(entries(&root, "sealed").unwrap().is_empty());
			return;
		}
		spool.persist("child", 0, &[r]).unwrap();
		match phase.as_str() {
			"progress" => spool.drain(|_| Ok(())).unwrap(),
			"disable" => disable(&root, |_, _| Ok(())).unwrap(),
			_ => panic!("append gate should already be waiting"),
		}
	}
	#[cfg(unix)]
	#[test]
	fn product_0161_failed_append_discards_the_unsynced_batch() {
		let root = Scratch::new();
		let status = std::process::Command::new(std::env::current_exe().unwrap())
			.args([
				"--exact",
				"reporting::spool::tests::product_0161_crash_child",
			])
			.env("POLARITON_0161_CRASH_PHASE", "append-error")
			.env("POLARITON_0161_CRASH_ROOT", &root.0)
			// The child's file-size limit must not apply to the parent ladder log.
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.unwrap();
		assert!(status.success());
	}
	#[test]
	fn product_0161_killed_writer_progress_and_revocation_recover_without_skips() {
		for phase in ["append", "progress", "disable"] {
			let root = Scratch::new();
			let ready = root.0.join("ready");
			let mut child = std::process::Command::new(std::env::current_exe().unwrap())
				.args([
					"--exact",
					"reporting::spool::tests::product_0161_crash_child",
				])
				.env("POLARITON_0161_CRASH_PHASE", phase)
				.env("POLARITON_0161_CRASH_ROOT", &root.0)
				.env("POLARITON_0161_CRASH_READY", &ready)
				.stdout(std::process::Stdio::null())
				.spawn()
				.unwrap();
			let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
			while !ready.exists() && std::time::Instant::now() < deadline {
				std::thread::sleep(std::time::Duration::from_millis(10));
			}
			if !ready.exists() {
				let _ = child.kill();
				panic!("child missed {phase} gate");
			}
			child.kill().unwrap();
			assert!(!child.wait().unwrap().success());
			let expected = fs::read_to_string(root.0.join("expected-id")).unwrap();
			let mut spool = Spool::open(root.0.clone(), destination()).unwrap();
			let mut sent = Vec::new();
			spool
				.drain(|r| {
					sent.push(r.report_id.clone());
					Ok(())
				})
				.unwrap();
			if phase == "disable" {
				assert!(sent.is_empty());
			} else {
				assert_eq!(sent, vec![expected]);
			}
		}
	}
}
