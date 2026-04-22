#[cfg(feature = "fips")]
use std::io::Error;
#[cfg(feature = "fips")]
use std::sync::OnceLock;
#[cfg(feature = "fips")]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FipsBuildState {
	pub fips_mode: bool,
	pub self_test_last_ts: Option<u64>,
}

pub fn crypto_profile() -> &'static str {
	if cfg!(feature = "fips") {
		"fips"
	} else {
		"default"
	}
}

pub fn build_mode() -> &'static str {
	if cfg!(debug_assertions) {
		"debug"
	} else {
		"release"
	}
}

pub fn aws_lc_rs_version() -> &'static str {
	env!("WENDELON_AWS_LC_RS_VERSION")
}

#[cfg(feature = "fips")]
static FIPS_SELF_TEST_LAST_TS: OnceLock<u64> = OnceLock::new();

#[cfg(feature = "fips")]
pub fn initialize_fips_runtime() -> std::io::Result<FipsBuildState> {
	aws_lc_rs::try_fips_mode()
		.map_err(|err| Error::other(format!("aws-lc-rs FIPS mode unavailable: {err}")))?;
	let ts = *FIPS_SELF_TEST_LAST_TS.get_or_init(unix_timestamp_secs);
	Ok(FipsBuildState {
		fips_mode: true,
		self_test_last_ts: Some(ts),
	})
}

#[cfg(not(feature = "fips"))]
pub fn initialize_fips_runtime() -> std::io::Result<FipsBuildState> {
	Ok(FipsBuildState::default())
}

#[cfg(feature = "fips")]
pub fn current_fips_build_state() -> FipsBuildState {
	FipsBuildState {
		fips_mode: true,
		self_test_last_ts: FIPS_SELF_TEST_LAST_TS.get().copied(),
	}
}

#[cfg(not(feature = "fips"))]
pub fn current_fips_build_state() -> FipsBuildState {
	FipsBuildState::default()
}

#[cfg(feature = "fips")]
fn unix_timestamp_secs() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_secs()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn aws_lc_version_env_is_present() {
		assert!(!aws_lc_rs_version().is_empty());
	}
}
