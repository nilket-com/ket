use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub(crate) struct AtomicWriteOptions {
	unix_mode: Option<u32>,
	preserve_existing_permissions: bool,
	require_existing_writable: bool,
	sync: bool,
}

impl AtomicWriteOptions {
	pub(crate) const fn config() -> Self {
		Self {
			unix_mode: None,
			preserve_existing_permissions: true,
			require_existing_writable: true,
			sync: true,
		}
	}

	const fn private_state() -> Self {
		Self {
			unix_mode: Some(0o600),
			preserve_existing_permissions: false,
			require_existing_writable: false,
			sync: false,
		}
	}
}

pub(crate) fn atomic_write(
	path: &Path,
	contents: &[u8],
	options: AtomicWriteOptions,
) -> std::io::Result<()> {
	let temp_path = atomic_temp_path(path);
	let existing_permissions = if options.preserve_existing_permissions {
		fs::metadata(path)
			.ok()
			.map(|metadata| metadata.permissions())
	} else {
		None
	};

	if options.require_existing_writable && path.exists() {
		// Renaming over a read-only file can succeed on Unix if the directory is writable.
		// Probe the target first to preserve the old in-place write semantics.
		OpenOptions::new().write(true).open(path)?;
	}

	let result = (|| {
		let mut file_options = OpenOptions::new();
		file_options.create(true).write(true).truncate(true);
		#[cfg(unix)]
		if let Some(mode) = existing_permissions
			.as_ref()
			.map(|permissions| permissions.mode() & 0o777)
			.or(options.unix_mode)
		{
			file_options.mode(mode);
		}

		{
			let mut file = file_options.open(&temp_path)?;
			file.write_all(contents)?;
			if options.sync {
				file.sync_all()?;
			}
		}

		if let Some(permissions) = existing_permissions {
			fs::set_permissions(&temp_path, permissions)?;
		}

		replace_file(&temp_path, path)?;
		if options.sync {
			sync_parent_dir(path)?;
		}
		Ok(())
	})();

	if result.is_err() {
		let _ = fs::remove_file(&temp_path);
	}
	result
}

pub(crate) fn atomic_temp_path(path: &Path) -> PathBuf {
	let file_name = path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("atomic");
	path.with_file_name(format!("{file_name}.tmp-{}", std::process::id()))
}

/// Atomically replace a private per-user state file.
///
/// New files receive mode `0600` on Unix. This is intended for frontend state such as command
/// history; durable editor settings use the engine's stricter synchronized write path.
pub fn write_private_state_atomically(path: &Path, contents: &[u8]) -> std::io::Result<()> {
	atomic_write(path, contents, AtomicWriteOptions::private_state())
}

#[cfg(not(windows))]
fn replace_file(temp_path: &Path, path: &Path) -> std::io::Result<()> {
	fs::rename(temp_path, path)
}

#[cfg(windows)]
fn replace_file(temp_path: &Path, path: &Path) -> std::io::Result<()> {
	use std::os::windows::ffi::OsStrExt;

	const MOVEFILE_REPLACE_EXISTING: u32 = 0x00000001;
	const MOVEFILE_WRITE_THROUGH: u32 = 0x00000008;

	unsafe extern "system" {
		fn MoveFileExW(
			lpExistingFileName: *const u16,
			lpNewFileName: *const u16,
			dwFlags: u32,
		) -> i32;
	}

	let source = path_to_wide(temp_path);
	let target = path_to_wide(path);
	let ok = unsafe {
		MoveFileExW(
			source.as_ptr(),
			target.as_ptr(),
			MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
		)
	};
	if ok == 0 {
		Err(std::io::Error::last_os_error())
	} else {
		Ok(())
	}
}

#[cfg(windows)]
fn path_to_wide(path: &Path) -> Vec<u16> {
	path.as_os_str().encode_wide().chain(Some(0)).collect()
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
	if let Some(parent) = path.parent() {
		fs::File::open(parent)?.sync_all()?;
	}
	Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> std::io::Result<()> {
	Ok(())
}
