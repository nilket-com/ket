//! The counter-specific persistence seam for the witness directory and the
//! applied log. Every byte write, rename, unlink and barrier the counter's
//! dedup authority depends on goes through it, so crash tests can model file
//! bytes and directory entries as separately durable, and inject failures.
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

pub(super) trait CounterDurableFileOps: Send + Sync {
	fn exists(&self, path: &Path) -> std::io::Result<bool>;
	fn create_dir(&self, path: &Path) -> std::io::Result<()>;
	/// Create or replace `path` with `bytes`. Not a durability barrier.
	fn write_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()>;
	/// Append `bytes`, returning whether this call created the file. Not a
	/// durability barrier.
	fn append_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<bool>;
	fn file_len(&self, path: &Path) -> std::io::Result<u64>;
	fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>>;
	fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>>;
	fn truncate(&self, path: &Path, len: u64) -> std::io::Result<()>;
	fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
	fn unlink(&self, path: &Path) -> std::io::Result<()>;
	/// `fsync(2)`: the file's bytes and metadata.
	fn sync_all(&self, path: &Path) -> std::io::Result<()>;
	/// `fdatasync(2)`: the file's bytes and the metadata needed to read them.
	fn sync_data(&self, path: &Path) -> std::io::Result<()>;
	/// `fsync(2)` on a directory: its entries (creations, renames, unlinks).
	fn sync_dir(&self, path: &Path) -> std::io::Result<()>;
}

pub(super) struct StdCounterDurableFileOps;

impl CounterDurableFileOps for StdCounterDurableFileOps {
	fn exists(&self, path: &Path) -> std::io::Result<bool> {
		match std::fs::symlink_metadata(path) {
			Ok(_) => Ok(true),
			Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
			Err(error) => Err(error),
		}
	}

	fn create_dir(&self, path: &Path) -> std::io::Result<()> {
		std::fs::create_dir(path)
	}

	fn write_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		let mut file = std::fs::File::create(path)?;
		file.write_all(bytes)
	}

	fn append_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<bool> {
		// `create_new` decides creation atomically, so the caller learns whether
		// the directory entry is new and still needs its own barrier.
		let (mut file, created) = match std::fs::OpenOptions::new()
			.append(true)
			.create_new(true)
			.open(path)
		{
			Ok(file) => (file, true),
			Err(error) if error.kind() == ErrorKind::AlreadyExists => {
				(std::fs::OpenOptions::new().append(true).open(path)?, false)
			}
			Err(error) => return Err(error),
		};
		file.write_all(bytes)?;
		Ok(created)
	}

	fn file_len(&self, path: &Path) -> std::io::Result<u64> {
		Ok(std::fs::metadata(path)?.len())
	}

	fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		std::fs::read(path)
	}

	fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>> {
		std::fs::read_dir(path)?
			.map(|entry| entry.map(|entry| entry.path()))
			.collect()
	}

	fn truncate(&self, path: &Path, len: u64) -> std::io::Result<()> {
		std::fs::OpenOptions::new()
			.write(true)
			.open(path)?
			.set_len(len)
	}

	fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		std::fs::rename(from, to)
	}

	fn unlink(&self, path: &Path) -> std::io::Result<()> {
		std::fs::remove_file(path)
	}

	fn sync_all(&self, path: &Path) -> std::io::Result<()> {
		std::fs::OpenOptions::new()
			.write(true)
			.open(path)?
			.sync_all()
	}

	fn sync_data(&self, path: &Path) -> std::io::Result<()> {
		std::fs::OpenOptions::new()
			.write(true)
			.open(path)?
			.sync_data()
	}

	fn sync_dir(&self, path: &Path) -> std::io::Result<()> {
		std::fs::File::open(path)?.sync_all()
	}
}

/// Create `dir` and any missing ancestors so that every new name is durable:
/// each created directory's parent is synced after the creation, and `dir`
/// itself is synced last, which also makes every name already inside it (for
/// example one a crashed process left unsynced) durable before first use.
pub(super) fn create_dir_durably(
	ops: &dyn CounterDurableFileOps,
	dir: &Path,
) -> std::io::Result<()> {
	let mut missing = Vec::new();
	let mut ancestor = dir;
	while !ops.exists(ancestor)? {
		missing.push(ancestor.to_path_buf());
		ancestor = ancestor.parent().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				"counter witness directory has no existing ancestor",
			)
		})?;
	}
	for child in missing.into_iter().rev() {
		match ops.create_dir(&child) {
			Ok(()) => (),
			Err(error) if error.kind() == ErrorKind::AlreadyExists && ops.exists(&child)? => (),
			Err(error) => return Err(error),
		}
		let parent = child.parent().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				"counter witness directory has no parent",
			)
		})?;
		ops.sync_dir(parent)?;
	}
	ops.sync_dir(dir)
}
