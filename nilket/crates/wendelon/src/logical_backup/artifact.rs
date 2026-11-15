//! ADR 0736: the sealed backup artifact.
//!
//! One fresh 256-bit data key per backup. It is wrapped under a key derived with scrypt from
//! an operator passphrase, at FIXED parameters (log_n 17, r 8, p 1: 128 MiB of working memory,
//! in the operator tool only); a wrap with other parameters is refused. Chunks and the
//! manifest are AES-256-GCM under the data key: chunk `i` (1-based) uses the 96-bit big-endian
//! counter `i` as its nonce and its own header as associated data; nonce 0 is the manifest's.
//! The key is never reused across backups, so the counter never repeats a nonce under a key.
//! Every length is bounded before anything of that length is allocated.

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use std::io::{Error, ErrorKind};
use zeroize::Zeroizing;

pub const WRAP_MAGIC: &[u8; 8] = b"KETBKWRP";
pub const WRAP_VERSION: u16 = 1;
pub const SCRYPT_LOG_N: u8 = 17;
pub const SCRYPT_R: u32 = 8;
pub const SCRYPT_P: u32 = 1;
/// magic, version, log_n, salt (32), wrapped key + tag (48).
pub const WRAP_BYTES: usize = 8 + 2 + 1 + 32 + 48;
/// The most chunks one backup holds.
pub const MAX_CHUNKS: u64 = 1 << 32;
/// The most plaintext bytes one chunk holds.
pub const MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// The most bytes a sealed manifest may have.
pub const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
const TAG: usize = 16;

fn invalid(message: &str) -> Error {
	Error::new(ErrorKind::InvalidData, message.to_owned())
}

/// The backup's data key.
pub struct DataKey(Zeroizing<[u8; 32]>);

impl DataKey {
	/// A fresh random key.
	pub fn generate() -> std::io::Result<Self> {
		let mut key = Zeroizing::new([0u8; 32]);
		aws_lc_rs::rand::fill(key.as_mut()).map_err(|_| Error::other("no randomness"))?;
		Ok(Self(key))
	}

	fn aead(&self) -> LessSafeKey {
		LessSafeKey::new(UnboundKey::new(&AES_256_GCM, self.0.as_ref()).expect("a 32-byte key"))
	}

	/// Wraps this key under `passphrase` with a fresh salt. The header is the associated data.
	pub fn wrap(&self, passphrase: &[u8]) -> std::io::Result<[u8; WRAP_BYTES]> {
		let mut salt = [0u8; 32];
		aws_lc_rs::rand::fill(&mut salt).map_err(|_| Error::other("no randomness"))?;
		let mut out = [0u8; WRAP_BYTES];
		out[..8].copy_from_slice(WRAP_MAGIC);
		out[8..10].copy_from_slice(&WRAP_VERSION.to_be_bytes());
		out[10] = SCRYPT_LOG_N;
		out[11..43].copy_from_slice(&salt);
		let kek = derive(passphrase, &salt)?;
		let mut sealed = self.0.to_vec();
		kek.seal_in_place_append_tag(
			Nonce::assume_unique_for_key([0u8; 12]),
			Aad::from(&out[..43]),
			&mut sealed,
		)
		.map_err(|_| Error::other("seal failed"))?;
		out[43..].copy_from_slice(&sealed);
		Ok(out)
	}

	/// Unwraps a wrapped key, refusing any header but this format's exact one.
	pub fn unwrap(wrapped: &[u8], passphrase: &[u8]) -> std::io::Result<Self> {
		if wrapped.len() != WRAP_BYTES
			|| &wrapped[..8] != WRAP_MAGIC
			|| wrapped[8..10] != WRAP_VERSION.to_be_bytes()
			|| wrapped[10] != SCRYPT_LOG_N
		{
			return Err(invalid("not a backup key wrap of this format and KDF"));
		}
		let salt: [u8; 32] = wrapped[11..43].try_into().expect("width");
		let kek = derive(passphrase, &salt)?;
		let mut sealed = wrapped[43..].to_vec();
		let opened = kek
			.open_in_place(
				Nonce::assume_unique_for_key([0u8; 12]),
				Aad::from(&wrapped[..43]),
				&mut sealed,
			)
			.map_err(|_| invalid("the passphrase does not open this backup"))?;
		let mut key = Zeroizing::new([0u8; 32]);
		key.copy_from_slice(opened);
		Ok(Self(key))
	}

	/// Seals chunk `ordinal` (1-based) with its header as associated data.
	pub fn seal_chunk(
		&self,
		ordinal: u64,
		header: &[u8],
		plaintext: &[u8],
	) -> std::io::Result<Vec<u8>> {
		if ordinal == 0 || ordinal > MAX_CHUNKS {
			return Err(invalid("chunk ordinal outside the admitted range"));
		}
		if plaintext.len() > MAX_CHUNK_BYTES {
			return Err(invalid("chunk too large"));
		}
		self.seal(ordinal, header, plaintext)
	}

	/// Opens chunk `ordinal`, which must have been sealed with exactly this header.
	pub fn open_chunk(
		&self,
		ordinal: u64,
		header: &[u8],
		sealed: &[u8],
	) -> std::io::Result<Vec<u8>> {
		if ordinal == 0 || ordinal > MAX_CHUNKS {
			return Err(invalid("chunk ordinal outside the admitted range"));
		}
		if sealed.len() > MAX_CHUNK_BYTES + TAG {
			return Err(invalid("chunk too large"));
		}
		self.open(ordinal, header, sealed)
	}

	/// Seals the manifest (nonce 0) with `header` (which names the backup) as associated data.
	pub fn seal_manifest(&self, header: &[u8], manifest: &[u8]) -> std::io::Result<Vec<u8>> {
		if manifest.len() + TAG > MAX_MANIFEST_BYTES {
			return Err(invalid("manifest too large"));
		}
		self.seal(0, header, manifest)
	}

	pub fn open_manifest(&self, header: &[u8], sealed: &[u8]) -> std::io::Result<Vec<u8>> {
		if sealed.len() > MAX_MANIFEST_BYTES {
			return Err(invalid("manifest too large"));
		}
		self.open(0, header, sealed)
	}

	fn seal(&self, counter: u64, aad: &[u8], plaintext: &[u8]) -> std::io::Result<Vec<u8>> {
		let mut out = plaintext.to_vec();
		self.aead()
			.seal_in_place_append_tag(nonce(counter), Aad::from(aad), &mut out)
			.map_err(|_| Error::other("seal failed"))?;
		Ok(out)
	}

	fn open(&self, counter: u64, aad: &[u8], sealed: &[u8]) -> std::io::Result<Vec<u8>> {
		let mut buffer = sealed.to_vec();
		let plain = self
			.aead()
			.open_in_place(nonce(counter), Aad::from(aad), &mut buffer)
			.map_err(|_| invalid("authentication failed"))?
			.len();
		buffer.truncate(plain);
		Ok(buffer)
	}
}

fn nonce(counter: u64) -> Nonce {
	let mut bytes = [0u8; 12];
	bytes[4..].copy_from_slice(&counter.to_be_bytes());
	Nonce::assume_unique_for_key(bytes)
}

fn derive(passphrase: &[u8], salt: &[u8; 32]) -> std::io::Result<LessSafeKey> {
	let params = scrypt::Params::new(SCRYPT_LOG_N, SCRYPT_R, SCRYPT_P, 32)
		.map_err(|_| Error::other("scrypt parameters"))?;
	let mut key = Zeroizing::new([0u8; 32]);
	scrypt::scrypt(passphrase, salt, &params, key.as_mut())
		.map_err(|_| Error::other("scrypt failed"))?;
	Ok(LessSafeKey::new(
		UnboundKey::new(&AES_256_GCM, key.as_ref()).expect("a 32-byte key"),
	))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_wrap_opens_only_with_its_passphrase_and_exact_header() {
		let key = DataKey::generate().unwrap();
		let wrapped = key.wrap(b"correct horse").unwrap();
		let again = DataKey::unwrap(&wrapped, b"correct horse").unwrap();
		assert_eq!(again.0.as_ref(), key.0.as_ref());
		assert!(DataKey::unwrap(&wrapped, b"wrong").is_err());
		for at in [0, 9, 10, 20, 50, WRAP_BYTES - 1] {
			let mut tampered = wrapped;
			tampered[at] ^= 1;
			assert!(
				DataKey::unwrap(&tampered, b"correct horse").is_err(),
				"{at}"
			);
		}
		assert!(DataKey::unwrap(&wrapped[..WRAP_BYTES - 1], b"correct horse").is_err());
	}

	#[test]
	fn chunks_open_only_at_their_ordinal_with_their_header() {
		let key = DataKey::generate().unwrap();
		let sealed = key.seal_chunk(3, b"backup/t/3", b"rows").unwrap();
		assert_eq!(key.open_chunk(3, b"backup/t/3", &sealed).unwrap(), b"rows");
		assert!(key.open_chunk(4, b"backup/t/3", &sealed).is_err(), "moved");
		assert!(
			key.open_chunk(3, b"backup/u/3", &sealed).is_err(),
			"relabelled"
		);
		let mut flipped = sealed.clone();
		flipped[0] ^= 1;
		assert!(key.open_chunk(3, b"backup/t/3", &flipped).is_err());
		assert!(
			key.seal_chunk(0, b"h", b"x").is_err(),
			"0 is the manifest's"
		);
		assert!(key.seal_chunk(MAX_CHUNKS + 1, b"h", b"x").is_err());
		assert!(
			key.seal_chunk(1, b"h", &vec![0; MAX_CHUNK_BYTES + 1])
				.is_err()
		);
		// The manifest's nonce never opens a chunk and the reverse.
		let manifest = key.seal_manifest(b"backup", b"m").unwrap();
		assert!(key.open_chunk(1, b"backup", &manifest).is_err());
		assert_eq!(key.open_manifest(b"backup", &manifest).unwrap(), b"m");
		assert!(key.open_manifest(b"other", &manifest).is_err());
		// Another backup's key opens nothing.
		let other = DataKey::generate().unwrap();
		assert!(other.open_chunk(3, b"backup/t/3", &sealed).is_err());
	}
}
