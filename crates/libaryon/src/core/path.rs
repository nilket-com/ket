use std::path::{Path, PathBuf};

fn home_dir() -> Option<PathBuf> {
	if let Some(home) = std::env::var_os("HOME") {
		return Some(PathBuf::from(home));
	}
	if let Some(profile) = std::env::var_os("USERPROFILE") {
		return Some(PathBuf::from(profile));
	}
	let drive = std::env::var_os("HOMEDRIVE")?;
	let path = std::env::var_os("HOMEPATH")?;
	let mut combined = drive;
	combined.push(path);
	Some(PathBuf::from(combined))
}

/// Expands leading tildes (~) and handles relative paths.
pub fn expand_path(path: &str) -> PathBuf {
	if let Some(stripped) = path.strip_prefix("~/")
		&& let Some(home) = home_dir()
	{
		return home.join(stripped);
	}
	if cfg!(windows)
		&& let Some(stripped) = path.strip_prefix("~\\")
		&& let Some(home) = home_dir()
	{
		return home.join(stripped);
	}
	if path == "~"
		&& let Some(home) = home_dir()
	{
		return home;
	}

	// Canonicalize handles ".." and "." but requires the path to exist.
	// For simplicity in this prototype, we'll just use PathBuf::from
	// which handles ".." correctly during standard file operations.
	PathBuf::from(path)
}

/// `Path::canonicalize` for paths that are displayed or compared afterwards.
///
/// On Windows the standard call answers with a verbatim `\?\C:\...` path. That
/// spelling reads as noise in a status line and never equals the same location
/// resolved without the prefix, so the prefix comes back off here. Everywhere
/// else this is `Path::canonicalize`.
pub fn canonicalize(path: &Path) -> std::io::Result<PathBuf> {
	path.canonicalize().map(strip_verbatim_prefix)
}

#[cfg(not(windows))]
fn strip_verbatim_prefix(path: PathBuf) -> PathBuf {
	path
}

#[cfg(windows)]
fn strip_verbatim_prefix(path: PathBuf) -> PathBuf {
	use std::ffi::OsString;
	use std::path::{Component, Prefix};

	let mut components = path.components();
	let Some(Component::Prefix(prefix)) = components.next() else {
		return path;
	};
	let mut stripped = match prefix.kind() {
		Prefix::VerbatimDisk(disk) => PathBuf::from(format!("{}:\\", char::from(disk))),
		Prefix::VerbatimUNC(server, share) => {
			let mut root = OsString::from(r"\\");
			root.push(server);
			root.push(r"\");
			root.push(share);
			root.push(r"\");
			PathBuf::from(root)
		}
		_ => return path,
	};
	for component in components {
		if component != Component::RootDir {
			stripped.push(component.as_os_str());
		}
	}
	stripped
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn canonicalize_never_answers_with_a_verbatim_prefix() {
		let temp = std::env::temp_dir();
		let canonical = canonicalize(&temp).expect("temp dir canonicalizes");
		assert!(!canonical.to_string_lossy().starts_with(r"\\?\"));
		assert!(canonical.is_absolute());
	}

	#[cfg(windows)]
	#[test]
	fn verbatim_disk_and_unc_prefixes_are_stripped() {
		assert_eq!(
			strip_verbatim_prefix(PathBuf::from(r"\\?\C:\Users\em\file.rs")),
			PathBuf::from(r"C:\Users\em\file.rs")
		);
		assert_eq!(
			strip_verbatim_prefix(PathBuf::from(r"\\?\UNC\server\share\dir\file.rs")),
			PathBuf::from(r"\\server\share\dir\file.rs")
		);
		assert_eq!(
			strip_verbatim_prefix(PathBuf::from(r"C:\already\plain")),
			PathBuf::from(r"C:\already\plain")
		);
	}
}
