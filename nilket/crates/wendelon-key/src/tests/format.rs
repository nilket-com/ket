//! T-FMT and T-K3: the version-1 file is byte-exact, every malformed header is refused before any
//! derivation, every header byte is authenticated, and the derived-key check holds.
use super::support::*;
use crate::envelope::{self, FILE_BYTES, HEADER_BYTES, Header, NEW_LOG_N, NEW_P, State};
use crate::error::ToolError;
use crate::passphrase::normalize;
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use key_guard::custody::SecretBuf;

fn secret(s: &str) -> SecretBuf {
	let mut b = SecretBuf::with_capacity(s.len());
	for x in s.bytes() {
		b.push(x).unwrap();
	}
	normalize(&b).unwrap()
}
fn seed(bytes: [u8; 32]) -> SecretBuf {
	let mut b = SecretBuf::zeroed(32);
	b.as_mut_slice().copy_from_slice(&bytes);
	b
}
/// The golden header: a Bound recovery key at epoch 0 of the fixture context, a counter salt and
/// nonce, and a backup_of reference.
fn golden_header() -> Header {
	Header {
		state: State::Bound,
		log_n: NEW_LOG_N,
		p: NEW_P,
		salt: std::array::from_fn(|i| i as u8),
		nonce: std::array::from_fn(|i| 0x40 + i as u8),
		role: 3,
		epoch: 0,
		context: Some(context_bytes(0xa3)),
		key: public(&RECOVERY_SEED),
		backup_of: Some([0x77; 32]),
	}
}

/// The version-1 golden file of `golden_header` under `PRIMARY`, pinned by its SHA-256 and its
/// ciphertext; the header bytes are checked field by field below.
const GOLDEN_SHA256: &str = "173129f7ea3b186aec4b654264d5976eebc5f552f9079a3a3cd0e57b911b5ac3";

#[test]
fn the_golden_file_is_byte_exact_and_decrypts_to_its_seed() {
	let file = envelope::seal(&golden_header(), &seed(RECOVERY_SEED), &secret(PRIMARY)).unwrap();
	assert_eq!(file.len(), FILE_BYTES);
	assert_eq!(&file[0..8], b"WROLEKEY");
	assert_eq!(&file[8..16], &[1, 0, 2, 1, 1, 17, 8, 1]);
	assert_eq!(&file[16..48], &golden_header().salt);
	assert_eq!(&file[48..60], &golden_header().nonce);
	assert_eq!(file[60], 3);
	assert_eq!(&file[61..69], &0u64.to_le_bytes());
	assert_eq!(file[69], 1);
	assert_eq!(&file[70..258], &context_bytes(0xa3));
	assert_eq!(&file[258..290], &public(&RECOVERY_SEED));
	assert_eq!(file[290], 1);
	assert_eq!(&file[291..323], &[0x77; 32]);
	assert_eq!(hex(&envelope::file_digest(&file)), GOLDEN_SHA256);
	let (header, opened) = envelope::open("golden", &file, &secret(PRIMARY)).unwrap();
	assert_eq!(header, golden_header());
	assert_eq!(opened.as_slice(), &RECOVERY_SEED);
	// The identity digests are the contract's (the Bound domain over context, role, epoch, key).
	let mut pending = golden_header();
	pending.state = State::Pending;
	assert_ne!(pending.identity(), golden_header().identity());
	pending.context = None;
	let mut expected = b"WROLEKEY-PENDING\0".to_vec();
	expected.extend([3]);
	expected.extend(0u64.to_le_bytes());
	expected.extend([0]);
	expected.extend([0; 188]);
	expected.extend(public(&RECOVERY_SEED));
	assert_eq!(pending.identity(), envelope::file_digest(&expected));
}

#[test]
fn the_kdf_is_rfc_7914_scrypt() {
	// RFC 7914 §12, vector 1 (outside the file's bounds; this checks the primitive alone).
	let params = scrypt::Params::new(4, 1, 1, 64).unwrap();
	let mut out = [0_u8; 64];
	scrypt::scrypt(b"", b"", &params, &mut out).unwrap();
	assert_eq!(
		hex(&out),
		concat!(
			"77d6576238657b203b19ca42c18a0497f16b4844e3074ae8dfdffa3fede21442",
			"fcd0069ded0948f8326a753a0fc81f17e8d3e0fb2e0d3628cf35e20c38d18906"
		)
	);
}

fn reason(file: &[u8]) -> &'static str {
	match envelope::open("f", file, &secret(PRIMARY)) {
		Err(ToolError::Format { reason, .. }) => reason,
		other => panic!("expected a format refusal, got {:?}", other.map(|_| ())),
	}
}

#[test]
fn every_malformed_header_is_refused_before_derivation() {
	let good = envelope::seal(&golden_header(), &seed(RECOVERY_SEED), &secret(PRIMARY)).unwrap();
	let edit = |at: usize, v: u8| {
		let mut f = good.clone();
		f[at] = v;
		f
	};
	for (what, file, expected) in [
		("version + 1", edit(8, 2), "unsupported format version"),
		("state 0", edit(10, 0), "unknown state"),
		("state 3", edit(10, 3), "unknown state"),
		("provider 2", edit(11, 2), "unknown provider"),
		("kdf 2", edit(12, 2), "unknown key derivation"),
		(
			"log_n 16",
			edit(13, 16),
			"key derivation parameters out of bounds",
		),
		(
			"log_n 21",
			edit(13, 21),
			"key derivation parameters out of bounds",
		),
		(
			"r 9",
			edit(14, 9),
			"key derivation parameters out of bounds",
		),
		(
			"p 0",
			edit(15, 0),
			"key derivation parameters out of bounds",
		),
		(
			"p 5",
			edit(15, 5),
			"key derivation parameters out of bounds",
		),
		("role 0", edit(60, 0), "unknown role"),
		("role 4", edit(60, 4), "unknown role"),
		("context presence 2", edit(69, 2), "bad presence byte"),
		("backup presence 2", edit(290, 2), "bad presence byte"),
		("magic", edit(0, b'X'), "bad magic"),
	] {
		assert_eq!(reason(&file), expected, "{what}");
	}
	// A valid alternative value of a field passes every header rule, so only the associated data
	// (or, for p, the derivation it feeds) can refuse it.
	for (what, at, value) in [
		("state Bound to Pending", 10, 1),
		("role recovery to admin", 60, 1),
		("p 1 to 2", 15, 2),
	] {
		let f = edit(at, value);
		crate::envelope::Header::decode(&f).unwrap_or_else(|e| panic!("{what}: {e}"));
		assert!(
			matches!(
				envelope::open("f", &f, &secret(PRIMARY)),
				Err(ToolError::Decrypt { .. })
			),
			"{what}"
		);
	}
	// Absent fields are zero-filled, and a Bound file carries its context.
	let mut absent = good.clone();
	absent[290] = 0;
	assert_eq!(reason(&absent), "non-zero fill");
	let mut unbound = good.clone();
	unbound[69] = 0;
	unbound[70..258].fill(0);
	assert_eq!(reason(&unbound), "a Bound file without its context");
	let mut long = good.clone();
	long.push(0);
	assert_eq!(reason(&long), "not the version's exact length");
	assert_eq!(
		reason(&good[..FILE_BYTES - 1]),
		"not the version's exact length"
	);
	// Control: the unmodified file opens.
	envelope::open("f", &good, &secret(PRIMARY)).unwrap();
}

#[test]
fn flipping_any_header_byte_fails_and_the_unmodified_file_decrypts() {
	let good = envelope::seal(&golden_header(), &seed(RECOVERY_SEED), &secret(PRIMARY)).unwrap();
	let passphrase = secret(PRIMARY);
	envelope::open("f", &good, &passphrase).unwrap();
	let lanes = std::thread::available_parallelism()
		.map_or(4, |n| n.get())
		.min(8);
	let failures: Vec<usize> = std::thread::scope(|scope| {
		let handles: Vec<_> = (0..lanes)
			.map(|lane| {
				let good = &good;
				let passphrase = &passphrase;
				scope.spawn(move || {
					(lane..HEADER_BYTES)
						.step_by(lanes)
						.filter(|&at| {
							let mut f = good.clone();
							f[at] ^= 0xff;
							envelope::open("f", &f, passphrase).is_ok()
						})
						.collect::<Vec<_>>()
				})
			})
			.collect();
		handles
			.into_iter()
			.flat_map(|h| h.join().unwrap())
			.collect()
	});
	assert!(
		failures.is_empty(),
		"header bytes not authenticated: {failures:?}"
	);
	// The ciphertext and tag are authenticated too.
	for at in [HEADER_BYTES, FILE_BYTES - 1] {
		let mut f = good.clone();
		f[at] ^= 0x01;
		assert!(matches!(
			envelope::open("f", &f, &passphrase),
			Err(ToolError::Decrypt { .. })
		));
	}
}

#[test]
fn a_wrong_passphrase_and_a_seed_of_another_key_are_refused() {
	let good = envelope::seal(&golden_header(), &seed(RECOVERY_SEED), &secret(PRIMARY)).unwrap();
	assert!(matches!(
		envelope::open("f", &good, &secret(SECONDARY)),
		Err(ToolError::Decrypt { .. })
	));
	// `seal` itself refuses a seed that does not derive the header key.
	assert!(envelope::seal(&golden_header(), &seed(ADMIN_SEED), &secret(PRIMARY)).is_err());
	// A file built outside `seal`, encrypting another key's seed under a well-formed header,
	// decrypts but fails the derived-key check.
	let head = golden_header().encode();
	let params = scrypt::Params::new(NEW_LOG_N, 8, u32::from(NEW_P), 32).unwrap();
	let mut key = [0_u8; 32];
	scrypt::scrypt(
		secret(PRIMARY).as_slice(),
		&golden_header().salt,
		&params,
		&mut key,
	)
	.unwrap();
	let aead = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &key).unwrap());
	let mut body = ADMIN_SEED;
	let tag = aead
		.seal_in_place_separate_tag(
			Nonce::assume_unique_for_key(golden_header().nonce),
			Aad::from(&head[..]),
			&mut body,
		)
		.unwrap();
	let mut forged = head.to_vec();
	forged.extend(body);
	forged.extend(tag.as_ref());
	assert!(matches!(
		envelope::open("f", &forged, &secret(PRIMARY)),
		Err(ToolError::KeyMismatch { .. })
	));
}
