//! Encrypted at-rest storage primitives owned by libaryon.
//!
//! This module deliberately exposes encrypted documents as bytes, rather than
//! application-specific records. Frontends can build their own schemas without
//! gaining access to passphrases or derived keys.

use crate::core::write_private_state_atomically;
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
	XChaCha20Poly1305, XNonce,
	aead::{Aead, KeyInit, Payload},
};
use std::path::Path;
use zeroize::{Zeroize, Zeroizing};

const MAGIC: &[u8; 8] = b"BARYENC\0";
const VERSION: u8 = 1;
const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const KEY_LEN: usize = 32;
const HEADER_LEN: usize = MAGIC.len() + 1 + SALT_LEN + NONCE_LEN;

/// A session key derived from a passphrase. Its bytes are never exposed.
struct SessionKey(Zeroizing<[u8; KEY_LEN]>);

impl SessionKey {
	/// Derive a key for a newly encrypted file and return its randomly generated salt.
	pub fn new(passphrase: &mut Zeroizing<Vec<u8>>) -> Result<(Self, [u8; SALT_LEN]), String> {
		let salt = random_bytes::<SALT_LEN>();
		let key = derive_key(passphrase, &salt)?;
		passphrase.zeroize();
		Ok((Self(Zeroizing::new(key)), salt))
	}

	/// Derive a key for an existing encrypted file.
	pub fn for_salt(
		passphrase: &mut Zeroizing<Vec<u8>>,
		salt: &[u8; SALT_LEN],
	) -> Result<Self, String> {
		let key = derive_key(passphrase, salt)?;
		passphrase.zeroize();
		Ok(Self(Zeroizing::new(key)))
	}
}

/// Libaryon's in-memory authority for an unlocked encryption session.
///
/// Frontends deliberately cannot inspect the passphrase or derived keys. They
/// may only ask the session to open or write encrypted bytes. Dropping or
/// locking this value zeroizes the retained passphrase.
#[derive(Default)]
pub struct SecretSession {
	passphrase: Option<Zeroizing<Vec<u8>>>,
}

impl SecretSession {
	pub fn is_unlocked(&self) -> bool {
		self.passphrase.is_some()
	}

	/// Replace the session passphrase. The caller must pass ownership of the
	/// masked prompt bytes; no frontend-facing API can read them back.
	pub fn unlock(&mut self, passphrase: Zeroizing<Vec<u8>>) -> Result<(), String> {
		if passphrase.is_empty() {
			return Err("password cannot be empty".to_owned());
		}
		self.passphrase = Some(passphrase);
		Ok(())
	}

	pub fn lock(&mut self) {
		self.passphrase = None;
	}

	pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
		let mut passphrase = self.require_passphrase()?;
		let (key, salt) = SessionKey::new(&mut passphrase)?;
		seal(&key, &salt, plaintext)
	}

	pub fn open(&self, contents: &[u8]) -> Result<Zeroizing<Vec<u8>>, String> {
		let salt = file_salt(contents)?;
		let mut passphrase = self.require_passphrase()?;
		let key = SessionKey::for_salt(&mut passphrase, &salt)?;
		open(&key, contents)
	}

	pub fn write(&self, path: &Path, plaintext: &[u8]) -> Result<(), String> {
		let encrypted = self.seal(plaintext)?;
		write_private_state_atomically(path, &encrypted)
			.map_err(|error| format!("could not write encrypted file: {error}"))
	}

	fn require_passphrase(&self) -> Result<Zeroizing<Vec<u8>>, String> {
		self.passphrase
			.as_ref()
			.map(|passphrase| Zeroizing::new(passphrase.to_vec()))
			.ok_or_else(|| "encrypted files are locked; use :unlock".to_owned())
	}
}

fn derive_key(passphrase: &[u8], salt: &[u8; SALT_LEN]) -> Result<[u8; KEY_LEN], String> {
	let params = Params::new(65_536, 3, 1, Some(KEY_LEN))
		.map_err(|_| "invalid encryption parameters".to_owned())?;
	let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
	let mut key = [0_u8; KEY_LEN];
	argon
		.hash_password_into(passphrase, salt, &mut key)
		.map_err(|_| "could not derive encryption key".to_owned())?;
	Ok(key)
}

/// Encrypt plaintext to Baryon's versioned file format.
fn seal(key: &SessionKey, salt: &[u8; SALT_LEN], plaintext: &[u8]) -> Result<Vec<u8>, String> {
	let nonce = random_bytes::<NONCE_LEN>();
	let mut header = Vec::with_capacity(HEADER_LEN);
	header.extend_from_slice(MAGIC);
	header.push(VERSION);
	header.extend_from_slice(salt);
	header.extend_from_slice(&nonce);
	let cipher = XChaCha20Poly1305::new_from_slice(&key.0[..])
		.map_err(|_| "could not initialise encryption".to_owned())?;
	let ciphertext = cipher
		.encrypt(
			XNonce::from_slice(&nonce),
			Payload {
				msg: plaintext,
				aad: &header,
			},
		)
		.map_err(|_| "could not encrypt document".to_owned())?;
	header.extend_from_slice(&ciphertext);
	Ok(header)
}

/// Decrypt an encrypted Baryon document. Authentication failures intentionally
/// do not distinguish a wrong password from a tampered file.
fn open(key: &SessionKey, contents: &[u8]) -> Result<Zeroizing<Vec<u8>>, String> {
	if contents.len() < HEADER_LEN
		|| &contents[..MAGIC.len()] != MAGIC
		|| contents[MAGIC.len()] != VERSION
	{
		return Err("not a supported encrypted Baryon file".to_owned());
	}
	let nonce_offset = MAGIC.len() + 1 + SALT_LEN;
	let nonce = &contents[nonce_offset..HEADER_LEN];
	let cipher = XChaCha20Poly1305::new_from_slice(&key.0[..])
		.map_err(|_| "could not initialise encryption".to_owned())?;
	let plaintext = cipher
		.decrypt(
			XNonce::from_slice(nonce),
			Payload {
				msg: &contents[HEADER_LEN..],
				aad: &contents[..HEADER_LEN],
			},
		)
		.map_err(|_| "could not unlock encrypted file".to_owned())?;
	Ok(Zeroizing::new(plaintext))
}

/// Read the salt from a valid encrypted-file header before deriving its key.
pub fn file_salt(contents: &[u8]) -> Result<[u8; SALT_LEN], String> {
	if contents.len() < HEADER_LEN
		|| &contents[..MAGIC.len()] != MAGIC
		|| contents[MAGIC.len()] != VERSION
	{
		return Err("not a supported encrypted Baryon file".to_owned());
	}
	let mut salt = [0_u8; SALT_LEN];
	salt.copy_from_slice(&contents[MAGIC.len() + 1..MAGIC.len() + 1 + SALT_LEN]);
	Ok(salt)
}

/// Encrypt and atomically persist a document with owner-only permissions for
/// newly created files.
fn random_bytes<const N: usize>() -> [u8; N] {
	rand::random()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn round_trip_authenticates_the_header_and_payload() {
		let mut passphrase = Zeroizing::new(b"correct horse battery staple".to_vec());
		let (key, salt) = SessionKey::new(&mut passphrase).expect("key");
		assert!(passphrase.is_empty());
		let encrypted = seal(&key, &salt, b"select * from payroll").expect("seal");
		assert_eq!(file_salt(&encrypted).expect("salt"), salt);
		assert_eq!(
			open(&key, &encrypted).expect("open").as_slice(),
			b"select * from payroll"
		);
		let mut changed = encrypted;
		changed[0] ^= 1;
		assert!(open(&key, &changed).is_err());
	}

	#[test]
	fn session_holds_the_passphrase_without_exposing_it() {
		let mut session = SecretSession::default();
		session
			.unlock(Zeroizing::new(b"correct horse battery staple".to_vec()))
			.expect("unlock");
		let encrypted = session.seal(b"top secret").expect("seal");
		assert_eq!(
			session.open(&encrypted).expect("open").as_slice(),
			b"top secret"
		);
		session.lock();
		assert!(session.open(&encrypted).is_err());
	}
}
