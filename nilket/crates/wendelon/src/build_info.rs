#[cfg(any(test, feature = "fips"))]
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
#[cfg(any(test, feature = "fips"))]
use aws_lc_rs::digest::{SHA256, digest as sha256_digest};
#[cfg(any(test, feature = "fips"))]
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

#[cfg(any(test, feature = "fips"))]
const SHA256_ABC_KAT: [u8; 32] = [
	0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
	0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
];

#[cfg(any(test, feature = "fips"))]
const AES256_GCM_EMPTY_PLAINTEXT_KAT: [u8; 16] = [
	0x53, 0x0f, 0x8a, 0xfb, 0xc7, 0x45, 0x36, 0xb9, 0xa9, 0x63, 0xb4, 0xf1, 0xc4, 0xcb, 0x73, 0x8b,
];

#[cfg(feature = "fips")]
static FIPS_SELF_TEST_LAST_TS: OnceLock<u64> = OnceLock::new();

#[cfg(feature = "fips")]
pub fn initialize_fips_runtime() -> std::io::Result<FipsBuildState> {
	aws_lc_rs::try_fips_mode()
		.map_err(|err| Error::other(format!("aws-lc-rs FIPS mode unavailable: {err}")))?;
	run_fips_startup_known_answer_tests()?;
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

#[cfg(any(test, feature = "fips"))]
fn run_fips_startup_known_answer_tests() -> std::io::Result<()> {
	sha256_known_answer_test()?;
	aes256_gcm_known_answer_test()?;
	Ok(())
}

#[cfg(any(test, feature = "fips"))]
fn sha256_known_answer_test() -> std::io::Result<()> {
	let digest = sha256_digest(&SHA256, b"abc");
	if digest.as_ref() != SHA256_ABC_KAT.as_slice() {
		return Err(fips_self_test_error(
			"sha256_kat",
			"SHA-256 digest mismatch for test vector `abc`",
		));
	}
	Ok(())
}

#[cfg(any(test, feature = "fips"))]
fn aes256_gcm_known_answer_test() -> std::io::Result<()> {
	let key = LessSafeKey::new(
		UnboundKey::new(&AES_256_GCM, &[0u8; 32])
			.map_err(|_| fips_self_test_error("aes256_gcm_kat", "failed to construct key"))?,
	);
	let mut in_out = Vec::new();
	key.seal_in_place_append_tag(
		Nonce::assume_unique_for_key([0u8; 12]),
		Aad::from(&[][..]),
		&mut in_out,
	)
	.map_err(|_| fips_self_test_error("aes256_gcm_kat", "seal failed"))?;
	if in_out.as_slice() != AES256_GCM_EMPTY_PLAINTEXT_KAT.as_slice() {
		return Err(fips_self_test_error(
			"aes256_gcm_kat",
			"AES-256-GCM empty-plaintext tag mismatch",
		));
	}

	let key = LessSafeKey::new(
		UnboundKey::new(&AES_256_GCM, &[0u8; 32])
			.map_err(|_| fips_self_test_error("aes256_gcm_kat", "failed to construct key"))?,
	);
	let mut in_out = AES256_GCM_EMPTY_PLAINTEXT_KAT.to_vec();
	let plaintext = key
		.open_in_place(
			Nonce::assume_unique_for_key([0u8; 12]),
			Aad::from(&[][..]),
			&mut in_out,
		)
		.map_err(|_| fips_self_test_error("aes256_gcm_kat", "open failed"))?;
	if !plaintext.is_empty() {
		return Err(fips_self_test_error(
			"aes256_gcm_kat",
			"AES-256-GCM empty-plaintext open returned bytes",
		));
	}
	Ok(())
}

#[cfg(any(test, feature = "fips"))]
fn fips_self_test_error(stage: &str, detail: &str) -> Error {
	Error::other(format!(
		"FIPS startup self-test failed at {stage}: {detail}"
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::ErrorKind;

	#[test]
	fn aws_lc_version_env_is_present() {
		assert!(!aws_lc_rs_version().is_empty());
	}

	#[test]
	fn fips_startup_known_answer_tests_pass() {
		run_fips_startup_known_answer_tests().unwrap();
	}

	#[test]
	fn fips_self_test_error_is_terminal_io_error() {
		let err = fips_self_test_error("forced", "boom");
		assert_eq!(err.kind(), ErrorKind::Other);
		assert!(
			err.to_string()
				.contains("FIPS startup self-test failed at forced: boom")
		);
	}
}
