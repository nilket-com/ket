use std::path::PathBuf;

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
