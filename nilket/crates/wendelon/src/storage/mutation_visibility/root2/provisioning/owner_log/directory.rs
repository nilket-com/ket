//! Immutable owner-directory format. Only a marked v2 directory opens; an unmarked one is
//! refused as `Unformatted` and never upgraded.
use super::*;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
const FORMAT: &str = "OWNER_FORMAT";
const TEMP: &str = "OWNER_FORMAT.tmp";
const FORMAT_BYTES: usize = 48;
fn marker_bytes() -> [u8; FORMAT_BYTES] {
	let mut b = [0; FORMAT_BYTES];
	b[..8].copy_from_slice(b"VISOWN02");
	b[8..10].copy_from_slice(&2u16.to_le_bytes());
	b[10..12].copy_from_slice(&512u16.to_le_bytes());
	let sum = sha(&b[..16]);
	b[16..].copy_from_slice(&sum);
	b
}
fn present(p: &Path) -> Owned<bool> {
	match fs::symlink_metadata(p) {
		Ok(_) => Ok(true),
		Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
		Err(e) => Err(io_err(IoStep2::LockOpen)(e)),
	}
}
fn magic(owner: &OwnerDir2) -> Owned<Option<Vec<u8>>> {
	let mut f = match open_file(&owner.path.join(LOG), Mode::Read) {
		Ok(f) => regular(f, IoStep2::LogOpen)?,
		Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
		Err(e) => return Err(io_err(IoStep2::LogOpen)(e)),
	};
	let mut b = vec![0; 8];
	let mut n = 0;
	while n < 8 {
		match f.read(&mut b[n..]) {
			Ok(0) => break,
			Ok(k) => n += k,
			Err(e) if e.kind() == ErrorKind::Interrupted => {}
			Err(e) => return Err(io_err(IoStep2::LogRead)(e)),
		}
	}
	b.truncate(n);
	Ok(Some(b))
}
impl OwnerDir2 {
	pub(in crate::storage::mutation_visibility::root2) fn open(path: &Path) -> Owned<Self> {
		let owner = Self::locked(path)?;
		if present(&owner.path.join(TEMP))? {
			return Err(OwnerIoError2::Incomplete);
		}
		if present(&owner.path.join(FORMAT))? {
			let f = open_file(&owner.path.join(FORMAT), Mode::Read)
				.map_err(|_| OwnerIoError2::MarkerCorrupt)?;
			let m = f.metadata().map_err(|_| OwnerIoError2::MarkerCorrupt)?;
			if !m.is_file() || m.mode() & 0o7777 != 0o600 {
				return Err(OwnerIoError2::MarkerCorrupt);
			}
			let bytes = ops::bounded(f, FORMAT_BYTES + 1, IoStep2::LogRead)?;
			if bytes != marker_bytes() {
				return Err(OwnerIoError2::MarkerCorrupt);
			}
			let Some(prefix) = magic(&owner)? else {
				return Err(OwnerIoError2::Incomplete);
			};
			// Empty or a partial v2 prefix can still be a legitimate repairable crash state.
			if prefix == b"VISIEV02" {
				return Err(OwnerIoError2::ForeignFormat);
			}
			return Ok(owner);
		}
		// No marker: an unmarked directory (any log, or none) is not a v2 directory.
		Err(OwnerIoError2::Unformatted)
	}
	// Only the known foreign framing is recognized here; unreadable/short probes defer to the
	// existing operation's typed read path.
	pub(super) fn check_framing(&self) -> Owned<()> {
		if let Ok(Some(p)) = magic(self)
			&& p == b"VISIEV02"
		{
			return Err(OwnerIoError2::ForeignFormat);
		}
		Ok(())
	}

	pub(in crate::storage::mutation_visibility::root2) fn create_v2(path: &Path) -> Owned<Self> {
		let mut owner = Self::locked(path)?;
		let entries = fs::read_dir(&owner.path).map_err(io_err(IoStep2::FormatTempCreate))?;
		for entry in entries {
			let entry = entry.map_err(io_err(IoStep2::FormatTempCreate))?;
			if entry.file_name() != LOCK {
				return Err(OwnerIoError2::CreationNotEmpty);
			}
		}
		let temp = owner.path.join(TEMP);
		let target = owner.path.join(FORMAT);
		let file = run(IoStep2::FormatTempCreate, || {
			open_file(&temp, Mode::Create(0o600))
		})
		.map_err(io_err(IoStep2::FormatTempCreate))?;
		let file = regular(file, IoStep2::FormatTempCreate)?;
		if file
			.metadata()
			.map_err(io_err(IoStep2::FormatTempCreate))?
			.mode() & 0o7777
			!= 0o600
		{
			return Err(OwnerIoError2::MarkerCorrupt);
		}
		run(IoStep2::FormatWrite, || (&file).write_all(&marker_bytes()))
			.map_err(io_err(IoStep2::FormatWrite))?;
		let result = run(IoStep2::FormatSync, || file.sync_all());
		owner.synced(IoStep2::FormatSync, result)?;
		// Same-directory hard-link then unlink is the contract's exclusive no-replace fallback.
		run(IoStep2::FormatPublish, || {
			fs::hard_link(&temp, &target)?;
			fs::remove_file(&temp)
		})
		.map_err(io_err(IoStep2::FormatPublish))?;
		let result = run(IoStep2::FormatDirSync, || owner.dir.sync_all());
		owner.synced(IoStep2::FormatDirSync, result)?;
		let log = run(IoStep2::FormatLogCreate, || {
			open_file(&owner.path.join(LOG), Mode::Create(0o600))
		})
		.map_err(io_err(IoStep2::FormatLogCreate))?;
		let log = regular(log, IoStep2::FormatLogCreate)?;
		let result = run(IoStep2::FormatLogSync, || log.sync_all());
		owner.synced(IoStep2::FormatLogSync, result)?;
		let result = run(IoStep2::FormatFinalSync, || owner.dir.sync_all());
		owner.synced(IoStep2::FormatFinalSync, result)?;
		Ok(owner)
	}
}
