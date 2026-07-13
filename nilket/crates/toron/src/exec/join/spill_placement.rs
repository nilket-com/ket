use std::ffi::OsString;
use std::path::PathBuf;

pub(in crate::exec) const JOIN_SPILL_DIR_ENV: &str = "TORON_JOIN_SPILL_DIR";

pub(in crate::exec) fn default_join_spill_dir() -> PathBuf {
	resolve_join_spill_dir(|| std::env::var_os(JOIN_SPILL_DIR_ENV))
}

fn resolve_join_spill_dir(env: impl FnOnce() -> Option<OsString>) -> PathBuf {
	match env() {
		Some(value) if !value.is_empty() => PathBuf::from(value),
		_ => legacy_join_spill_dir(),
	}
}

fn legacy_join_spill_dir() -> PathBuf {
	std::env::temp_dir().join("toron_join_spill")
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ExecutionContext;
	use std::sync::Arc;
	use wendelon::storage::BlockingFileOps;

	#[test]
	fn non_empty_environment_path_overrides_legacy_default() {
		let configured = PathBuf::from("configured/join-spill");
		assert_eq!(
			resolve_join_spill_dir(|| Some(configured.clone().into_os_string())),
			configured
		);
	}

	#[test]
	fn empty_environment_path_is_unset() {
		assert_eq!(
			resolve_join_spill_dir(|| Some(OsString::new())),
			legacy_join_spill_dir()
		);
	}

	#[test]
	fn missing_environment_path_preserves_legacy_default() {
		assert_eq!(resolve_join_spill_dir(|| None), legacy_join_spill_dir());
	}

	#[cfg(unix)]
	#[test]
	fn non_unicode_environment_path_round_trips_without_loss() {
		use std::os::unix::ffi::{OsStrExt, OsStringExt};

		let bytes = b"configured/join-spill-\xff".to_vec();
		let path = resolve_join_spill_dir(|| Some(OsString::from_vec(bytes.clone())));
		assert_eq!(path.as_os_str().as_bytes(), bytes);
	}

	#[test]
	fn builder_override_replaces_the_selected_default() {
		let configured = PathBuf::from("configured/default");
		let builder = PathBuf::from("builder/join-spill");
		let mut context = ExecutionContext::local_test();
		context.join_spill_dir = configured;

		let context = context.with_join_spill_store(Arc::new(BlockingFileOps), builder.clone());
		assert_eq!(context.join_spill_dir, builder);
	}
}
