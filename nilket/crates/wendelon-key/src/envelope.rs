//! K3: the encrypted key file, version 1, exactly 371 bytes.
//!
//! | Offset | Field |
//! |---|---|
//! | 0..8 | `WROLEKEY` |
//! | 8..10 | version u16 LE = 1 |
//! | 10 | state: 1 Pending, 2 Bound |
//! | 11 | provider: 1 file |
//! | 12 | KDF: 1 scrypt |
//! | 13, 14, 15 | log_n (17..=20), r (8), p (1..=4) |
//! | 16..48 | salt |
//! | 48..60 | nonce |
//! | 60 | role: 1 admin, 2 governance, 3 recovery |
//! | 61..69 | epoch u64 LE (Pending: the intended epoch) |
//! | 69 | context present (0 or 1; a Bound file has one) |
//! | 70..258 | context, or zeros |
//! | 258..290 | public key |
//! | 290 | `backup_of` present (0 or 1) |
//! | 291..323 | `backup_of`, or zeros |
//! | 323..371 | AES-256-GCM over the 32-byte seed: ciphertext, then the 16-byte tag |
//!
//! The associated data is bytes 0..323 exactly as read, never a re-serialisation. A header outside
//! these rules is refused before any key derivation. The header is authenticated only by
//! decryption, after which the seed must derive the header's public key.
use crate::error::ToolError;
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::digest::{SHA256, digest};
use ed25519_dalek::SigningKey;
use key_guard::custody::SecretBuf;

pub(crate) const MAGIC: &[u8; 8] = b"WROLEKEY";
pub(crate) const VERSION: u16 = 1;
pub(crate) const HEADER_BYTES: usize = 323;
pub(crate) const FILE_BYTES: usize = 371;
pub(crate) const CONTEXT_BYTES: usize = 188;
/// The admitted file lengths for key-guard's exact-size open.
pub(crate) const ADMITTED: [u64; 1] = [FILE_BYTES as u64];
/// scrypt bounds (K3): r = 8, 17 <= log_n <= 20, 1 <= p <= 4.
pub(crate) const LOG_N: std::ops::RangeInclusive<u8> = 17..=20;
pub(crate) const R: u8 = 8;
pub(crate) const P: std::ops::RangeInclusive<u8> = 1..=4;
/// New files: 2^17 × 8 × 128 B = 128 MiB of scrypt working set, one lane.
pub(crate) const NEW_LOG_N: u8 = 17;
pub(crate) const NEW_P: u8 = 1;

const ID_DOMAIN: &[u8] = b"WROLEKEY-ID\0";
const PENDING_DOMAIN: &[u8] = b"WROLEKEY-PENDING\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
	Pending,
	Bound,
}

impl State {
	pub(crate) fn name(self) -> &'static str {
		match self {
			Self::Pending => "Pending",
			Self::Bound => "Bound",
		}
	}
}

/// Every header field. All of it is public: the seed is only ever in the ciphertext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Header {
	pub(crate) state: State,
	pub(crate) log_n: u8,
	pub(crate) p: u8,
	pub(crate) salt: [u8; 32],
	pub(crate) nonce: [u8; 12],
	/// 1 admin, 2 governance, 3 recovery.
	pub(crate) role: u8,
	/// The bound epoch, or a Pending file's intended epoch.
	pub(crate) epoch: u64,
	pub(crate) context: Option<[u8; CONTEXT_BYTES]>,
	pub(crate) key: [u8; 32],
	pub(crate) backup_of: Option<[u8; 32]>,
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
	let mut ctx = aws_lc_rs::digest::Context::new(&SHA256);
	for part in parts {
		ctx.update(part);
	}
	ctx.finish().as_ref().try_into().expect("sha256 width")
}

/// The display fingerprint of a public key: the first 16 hex digits of SHA-256(key). Never an
/// identity.
pub(crate) fn fingerprint(key: &[u8; 32]) -> String {
	hex(&digest(&SHA256, key).as_ref()[..8])
}

pub(crate) fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn file_digest(bytes: &[u8]) -> [u8; 32] {
	sha256(&[bytes])
}

impl Header {
	/// The identity digest (contract §2): role u8, epoch u64 LE, key 32 bytes.
	/// - Bound: SHA-256(`WROLEKEY-ID\0` ‖ context ‖ role ‖ epoch ‖ key).
	/// - Pending: SHA-256(`WROLEKEY-PENDING\0` ‖ role ‖ intended epoch ‖ has-context ‖ context or
	///   zeros ‖ key).
	pub(crate) fn identity(&self) -> [u8; 32] {
		let epoch = self.epoch.to_le_bytes();
		let context = self.context.unwrap_or([0; CONTEXT_BYTES]);
		match self.state {
			State::Bound => sha256(&[ID_DOMAIN, &context, &[self.role], &epoch, &self.key]),
			State::Pending => sha256(&[
				PENDING_DOMAIN,
				&[self.role],
				&epoch,
				&[u8::from(self.context.is_some())],
				&context,
				&self.key,
			]),
		}
	}

	pub(crate) fn encode(&self) -> [u8; HEADER_BYTES] {
		let mut b = [0_u8; HEADER_BYTES];
		b[0..8].copy_from_slice(MAGIC);
		b[8..10].copy_from_slice(&VERSION.to_le_bytes());
		b[10] = match self.state {
			State::Pending => 1,
			State::Bound => 2,
		};
		b[11] = 1;
		b[12] = 1;
		b[13] = self.log_n;
		b[14] = R;
		b[15] = self.p;
		b[16..48].copy_from_slice(&self.salt);
		b[48..60].copy_from_slice(&self.nonce);
		b[60] = self.role;
		b[61..69].copy_from_slice(&self.epoch.to_le_bytes());
		if let Some(context) = &self.context {
			b[69] = 1;
			b[70..258].copy_from_slice(context);
		}
		b[258..290].copy_from_slice(&self.key);
		if let Some(of) = &self.backup_of {
			b[290] = 1;
			b[291..323].copy_from_slice(of);
		}
		b
	}

	/// The header of `bytes`, which must be exactly one version-1 file. Every rule is checked
	/// here, before any derivation.
	pub(crate) fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
		if bytes.len() < 10 {
			return Err("truncated");
		}
		if &bytes[0..8] != MAGIC {
			return Err("bad magic");
		}
		if u16::from_le_bytes([bytes[8], bytes[9]]) != VERSION {
			return Err("unsupported format version");
		}
		if bytes.len() != FILE_BYTES {
			return Err("not the version's exact length");
		}
		let state = match bytes[10] {
			1 => State::Pending,
			2 => State::Bound,
			_ => return Err("unknown state"),
		};
		if bytes[11] != 1 {
			return Err("unknown provider");
		}
		if bytes[12] != 1 {
			return Err("unknown key derivation");
		}
		let (log_n, r, p) = (bytes[13], bytes[14], bytes[15]);
		if !LOG_N.contains(&log_n) || r != R || !P.contains(&p) {
			return Err("key derivation parameters out of bounds");
		}
		let role = bytes[60];
		if !(1..=3).contains(&role) {
			return Err("unknown role");
		}
		let context = optional::<CONTEXT_BYTES>(bytes[69], &bytes[70..258])?;
		if state == State::Bound && context.is_none() {
			return Err("a Bound file without its context");
		}
		let backup_of = optional::<32>(bytes[290], &bytes[291..323])?;
		Ok(Self {
			state,
			log_n,
			p,
			salt: bytes[16..48].try_into().expect("salt width"),
			nonce: bytes[48..60].try_into().expect("nonce width"),
			role,
			epoch: u64::from_le_bytes(bytes[61..69].try_into().expect("epoch width")),
			context,
			key: bytes[258..290].try_into().expect("key width"),
			backup_of,
		})
	}
}

/// A presence byte and its zero-filled field.
fn optional<const N: usize>(present: u8, field: &[u8]) -> Result<Option<[u8; N]>, &'static str> {
	match present {
		0 if field.iter().all(|b| *b == 0) => Ok(None),
		0 => Err("non-zero fill"),
		1 => Ok(Some(field.try_into().expect("field width"))),
		_ => Err("bad presence byte"),
	}
}

/// scrypt(passphrase, salt, log_n, 8, p) into a 32-byte secret buffer.
fn derive(passphrase: &SecretBuf, header: &Header) -> Result<SecretBuf, ToolError> {
	let params = scrypt::Params::new(header.log_n, u32::from(R), u32::from(header.p), 32)
		.map_err(|_| ToolError::Kdf)?;
	let mut key = SecretBuf::zeroed(32);
	scrypt::scrypt(
		passphrase.as_slice(),
		&header.salt,
		&params,
		key.as_mut_slice(),
	)
	.map_err(|_| ToolError::Kdf)?;
	Ok(key)
}

fn aead(key: &SecretBuf) -> Result<LessSafeKey, ToolError> {
	UnboundKey::new(&AES_256_GCM, key.as_slice())
		.map(LessSafeKey::new)
		.map_err(|_| ToolError::Seal)
}

/// The public key a seed derives.
pub(crate) fn public_key(seed: &SecretBuf) -> [u8; 32] {
	let seed: &[u8; 32] = seed.as_slice().try_into().expect("a 32-byte seed");
	SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

/// The whole file for `header` with `seed` encrypted under `passphrase`. The caller supplies a
/// fresh salt and nonce in `header` for every encryption.
pub(crate) fn seal(
	header: &Header,
	seed: &SecretBuf,
	passphrase: &SecretBuf,
) -> Result<Vec<u8>, ToolError> {
	if seed.len() != 32 || public_key(seed) != header.key {
		return Err(ToolError::Seal);
	}
	let head = header.encode();
	let key = aead(&derive(passphrase, header)?)?;
	let mut body = SecretBuf::zeroed(32);
	body.as_mut_slice().copy_from_slice(seed.as_slice());
	let tag = key
		.seal_in_place_separate_tag(
			Nonce::assume_unique_for_key(header.nonce),
			Aad::from(&head[..]),
			body.as_mut_slice(),
		)
		.map_err(|_| ToolError::Seal)?;
	let mut file = head.to_vec();
	file.extend_from_slice(body.as_slice());
	file.extend_from_slice(tag.as_ref());
	Ok(file)
}

/// The seed of `file` (named `name` in errors), after the header rules, decryption under
/// `passphrase` with the on-disk header as associated data, and the derived-key check.
pub(crate) fn open(
	name: &str,
	file: &[u8],
	passphrase: &SecretBuf,
) -> Result<(Header, SecretBuf), ToolError> {
	let header = Header::decode(file).map_err(|reason| ToolError::Format {
		name: name.to_string(),
		reason,
	})?;
	let key = aead(&derive(passphrase, &header)?)?;
	let mut body = SecretBuf::zeroed(48);
	body.as_mut_slice()
		.copy_from_slice(&file[HEADER_BYTES..FILE_BYTES]);
	let plain = key
		.open_in_place(
			Nonce::assume_unique_for_key(header.nonce),
			Aad::from(&file[..HEADER_BYTES]),
			body.as_mut_slice(),
		)
		.map_err(|_| ToolError::Decrypt {
			name: name.to_string(),
		})?;
	let mut seed = SecretBuf::zeroed(32);
	seed.as_mut_slice().copy_from_slice(&plain[..32]);
	if public_key(&seed) != header.key {
		return Err(ToolError::KeyMismatch {
			name: name.to_string(),
		});
	}
	Ok((header, seed))
}
