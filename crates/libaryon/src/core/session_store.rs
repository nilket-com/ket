//! Durable session identity and atomic owner-partitioned snapshots.
//!
//! This module is deliberately a substrate. Constructing a [`SessionStore`] does not select,
//! create, or restore a session; callers must explicitly create or open one. The production
//! editor remains ephemeral until a later consumer chooses a saved session.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::str::FromStr;

const STORE_VERSION: u32 = 1;
const SESSION_ID_HEX_LEN: usize = 32;
const GENERATION_HEX_LEN: usize = 16;

/// Opaque identity for one continuing saved session.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId([u8; 16]);

impl SessionId {
	fn from_random_bytes(bytes: [u8; 16]) -> Option<Self> {
		(bytes != [0; 16]).then_some(Self(bytes))
	}

	fn mint() -> Self {
		loop {
			if let Some(id) = Self::from_random_bytes(rand::random()) {
				return id;
			}
		}
	}

	fn parse_canonical(value: &str) -> Result<Self, ParseSessionIdError> {
		if value.len() != SESSION_ID_HEX_LEN
			|| !value
				.bytes()
				.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
		{
			return Err(ParseSessionIdError);
		}

		let mut bytes = [0_u8; 16];
		for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
			bytes[index] = (decode_hex_nibble(pair[0]).ok_or(ParseSessionIdError)? << 4)
				| decode_hex_nibble(pair[1]).ok_or(ParseSessionIdError)?;
		}
		Self::from_random_bytes(bytes).ok_or(ParseSessionIdError)
	}

	fn as_hex(self) -> String {
		encode_hex(&self.0)
	}
}

impl fmt::Debug for SessionId {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_tuple("SessionId")
			.field(&self.as_hex())
			.finish()
	}
}

impl fmt::Display for SessionId {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(&self.as_hex())
	}
}

impl FromStr for SessionId {
	type Err = ParseSessionIdError;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		Self::parse_canonical(value)
	}
}

/// Refusal returned for a non-canonical or zero session identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseSessionIdError;

impl fmt::Display for ParseSessionIdError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("session id must be 32 lowercase hexadecimal digits and nonzero")
	}
}

impl std::error::Error for ParseSessionIdError {}

/// Opaque identity for one document inside a saved session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentId(NonZeroU64);

impl DocumentId {
	pub const fn get(self) -> u64 {
		self.0.get()
	}

	fn new(value: u64) -> Result<Self, SessionStoreError> {
		NonZeroU64::new(value)
			.map(Self)
			.ok_or_else(|| SessionStoreError::Corrupt("document id is zero".to_owned()))
	}
}

impl fmt::Display for DocumentId {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(formatter)
	}
}

/// Validated authority name for an opaque owner payload.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerId(String);

impl OwnerId {
	pub fn new(value: impl Into<String>) -> Result<Self, SessionStoreError> {
		let value = value.into();
		let mut bytes = value.bytes();
		let valid = value.len() <= 64
			&& matches!(bytes.next(), Some(b'a'..=b'z'))
			&& bytes.all(|byte| {
				byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
			});
		if !valid {
			return Err(SessionStoreError::InvalidOwnerId(value));
		}
		Ok(Self(value))
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	fn payload_name(&self) -> String {
		format!("{}.payload", self.0)
	}
}

impl fmt::Display for OwnerId {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(&self.0)
	}
}

impl FromStr for OwnerId {
	type Err = SessionStoreError;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		Self::new(value)
	}
}

/// Durable document identity plus its mutable path attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentIdentity {
	id: DocumentId,
	path: Option<PathBuf>,
}

impl DocumentIdentity {
	pub const fn id(&self) -> DocumentId {
		self.id
	}

	pub fn path(&self) -> Option<&Path> {
		self.path.as_deref()
	}
}

/// Validated committed base snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
	session_id: SessionId,
	generation: u64,
	next_document_id: u64,
	documents: BTreeMap<DocumentId, DocumentIdentity>,
}

impl SessionSnapshot {
	pub const fn session_id(&self) -> SessionId {
		self.session_id
	}

	pub const fn generation(&self) -> u64 {
		self.generation
	}

	pub fn documents(
		&self,
	) -> impl DoubleEndedIterator<Item = &DocumentIdentity> + ExactSizeIterator {
		self.documents.values()
	}

	pub fn document(&self, id: DocumentId) -> Option<&DocumentIdentity> {
		self.documents.get(&id)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnerPayload {
	schema_version: u32,
	bytes: Vec<u8>,
}

/// A durable-session store rooted below the head-neutral platform state path.
#[derive(Debug, Clone)]
pub struct SessionStore {
	root: PathBuf,
}

impl SessionStore {
	/// Resolve the default head-neutral durable-session root without creating it.
	pub fn platform_default() -> Result<Self, SessionStoreError> {
		if !cfg!(unix) {
			return Err(SessionStoreError::UnsupportedPlatform);
		}
		default_session_root()
			.map(|root| Self { root })
			.ok_or(SessionStoreError::StateRootUnavailable)
	}

	#[cfg(test)]
	fn at(root: PathBuf) -> Self {
		Self { root }
	}
}

/// Marker for a shared-lock opened session.
///
/// Mutating operations are absent from the read-only capability:
///
/// ```compile_fail
/// use libaryon::core::{ReadOnly, SavedSession};
///
/// fn cannot_commit(mut session: SavedSession<ReadOnly>) {
///     session.commit();
/// }
/// ```
#[derive(Debug)]
pub struct ReadOnly {
	_private: (),
}

/// Marker for an exclusive-lock opened session.
#[derive(Debug)]
pub struct Writable {
	_private: (),
}

mod sealed {
	pub trait AccessMode {}

	impl AccessMode for super::ReadOnly {}
	impl AccessMode for super::Writable {}
}

/// One opened saved session. Its private lock descriptor lives as long as this value.
#[derive(Debug)]
pub struct SavedSession<Mode: sealed::AccessMode> {
	snapshot: SessionSnapshot,
	owners: BTreeMap<OwnerId, OwnerPayload>,
	#[cfg(unix)]
	session_dir: std::fs::File,
	#[cfg(unix)]
	_lock: std::fs::File,
	publication_uncertain: bool,
	_mode: PhantomData<Mode>,
}

impl<Mode: sealed::AccessMode> SavedSession<Mode> {
	pub fn snapshot(&self) -> &SessionSnapshot {
		&self.snapshot
	}

	pub fn owner_payload(&self, owner: &OwnerId) -> Option<(u32, &[u8])> {
		self.owners
			.get(owner)
			.map(|payload| (payload.schema_version, payload.bytes.as_slice()))
	}
}

/// Durable-session refusal. Corruption never falls back to a guessed generation.
#[derive(Debug)]
pub enum SessionStoreError {
	UnsupportedPlatform,
	StateRootUnavailable,
	Contended(SessionId),
	InvalidOwnerId(String),
	DocumentIdExhausted,
	GenerationExhausted,
	PublicationUncertain,
	UnknownDocument(DocumentId),
	Corrupt(String),
	Io(io::Error),
}

impl fmt::Display for SessionStoreError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::UnsupportedPlatform => formatter.write_str("durable sessions require Unix flock"),
			Self::StateRootUnavailable => {
				formatter.write_str("durable session state root is unavailable")
			}
			Self::Contended(id) => {
				write!(formatter, "saved session {id} is already open incompatibly")
			}
			Self::InvalidOwnerId(id) => write!(formatter, "invalid owner id {id:?}"),
			Self::DocumentIdExhausted => formatter.write_str("document id space exhausted"),
			Self::GenerationExhausted => formatter.write_str("session generation space exhausted"),
			Self::PublicationUncertain => formatter.write_str(
				"CURRENT was replaced but directory synchronization failed; reopen the session",
			),
			Self::UnknownDocument(id) => write!(formatter, "unknown document {id}"),
			Self::Corrupt(reason) => write!(formatter, "corrupt saved session: {reason}"),
			Self::Io(error) => error.fmt(formatter),
		}
	}
}

impl std::error::Error for SessionStoreError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io(error) => Some(error),
			_ => None,
		}
	}
}

impl From<io::Error> for SessionStoreError {
	fn from(error: io::Error) -> Self {
		Self::Io(error)
	}
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskSnapshot {
	version: u32,
	session_id: String,
	generation: u64,
	next_document_id: u64,
	documents: Vec<DiskDocument>,
	owners: Vec<DiskOwner>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskDocument {
	id: u64,
	path_hex: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskOwner {
	owner: String,
	schema_version: u32,
	byte_len: u64,
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
	match byte {
		b'0'..=b'9' => Some(byte - b'0'),
		b'a'..=b'f' => Some(byte - b'a' + 10),
		_ => None,
	}
}

fn encode_hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut encoded = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		encoded.push(char::from(DIGITS[(byte >> 4) as usize]));
		encoded.push(char::from(DIGITS[(byte & 0x0f) as usize]));
	}
	encoded
}

fn decode_hex(value: &str, subject: &str) -> Result<Vec<u8>, SessionStoreError> {
	if !value.len().is_multiple_of(2)
		|| !value
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
	{
		return Err(SessionStoreError::Corrupt(format!(
			"{subject} is not canonical lowercase hexadecimal"
		)));
	}

	value
		.as_bytes()
		.chunks_exact(2)
		.map(|pair| {
			Ok((decode_hex_nibble(pair[0]).expect("validated hex") << 4)
				| decode_hex_nibble(pair[1]).expect("validated hex"))
		})
		.collect()
}

fn default_session_root() -> Option<PathBuf> {
	default_session_root_for(
		if cfg!(target_os = "macos") {
			SessionPlatform::Macos
		} else if cfg!(unix) {
			SessionPlatform::Xdg
		} else {
			return None;
		},
		std::env::var_os("XDG_STATE_HOME"),
		std::env::var_os("HOME"),
	)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionPlatform {
	Xdg,
	Macos,
}

fn default_session_root_for(
	platform: SessionPlatform,
	xdg_state_home: Option<OsString>,
	home: Option<OsString>,
) -> Option<PathBuf> {
	let non_empty =
		|value: Option<OsString>| value.filter(|value| !value.is_empty()).map(PathBuf::from);
	match platform {
		SessionPlatform::Xdg => non_empty(xdg_state_home)
			.map(|xdg| xdg.join("libaryon").join("sessions"))
			.or_else(|| {
				non_empty(home).map(|home| {
					home.join(".local")
						.join("state")
						.join("libaryon")
						.join("sessions")
				})
			}),
		SessionPlatform::Macos => non_empty(home).map(|home| {
			home.join("Library")
				.join("Application Support")
				.join("libaryon")
				.join("state")
				.join("sessions")
		}),
	}
}

#[cfg(unix)]
fn path_to_hex(path: &Path) -> String {
	use std::os::unix::ffi::OsStrExt;
	encode_hex(path.as_os_str().as_bytes())
}

#[cfg(unix)]
fn path_from_hex(value: &str) -> Result<PathBuf, SessionStoreError> {
	use std::os::unix::ffi::OsStringExt;
	Ok(PathBuf::from(OsString::from_vec(decode_hex(
		value, "path",
	)?)))
}

#[cfg(unix)]
mod unix {
	use super::*;
	use std::collections::BTreeSet;
	use std::ffi::{CStr, CString};
	use std::fs::{self, File};
	use std::io::{Read, Write};
	use std::os::fd::{AsRawFd, FromRawFd, RawFd};
	use std::os::unix::ffi::OsStrExt;
	use std::os::unix::fs::{DirBuilderExt, MetadataExt};

	const PRIVATE_DIR_MODE: u32 = 0o700;
	const PRIVATE_FILE_MODE: u32 = 0o600;
	const MAX_POINTER_BYTES: usize = 128;
	const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	enum LockMode {
		Shared,
		Exclusive,
	}

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub(super) enum CommitFault {
		None,
		BeforeGenerationRename,
		AfterGenerationRename,
		BeforeCurrentRename,
		AfterCurrentRename,
	}

	impl SessionStore {
		pub fn create(&self) -> Result<SavedSession<Writable>, SessionStoreError> {
			self.create_with_minter(SessionId::mint)
		}

		pub fn open_read_only(
			&self,
			id: SessionId,
		) -> Result<SavedSession<ReadOnly>, SessionStoreError> {
			let (session_dir, lock) = self.open_locked(id, LockMode::Shared)?;
			let (snapshot, owners) = load_snapshot(&session_dir, id)?;
			Ok(SavedSession {
				snapshot,
				owners,
				session_dir,
				_lock: lock,
				publication_uncertain: false,
				_mode: PhantomData,
			})
		}

		pub fn open_writable(
			&self,
			id: SessionId,
		) -> Result<SavedSession<Writable>, SessionStoreError> {
			let (session_dir, lock) = self.open_locked(id, LockMode::Exclusive)?;
			let (snapshot, owners) = load_snapshot(&session_dir, id)?;
			let mut session = SavedSession {
				snapshot,
				owners,
				session_dir,
				_lock: lock,
				publication_uncertain: false,
				_mode: PhantomData,
			};
			session.cleanup_unreferenced()?;
			Ok(session)
		}

		fn create_with_minter(
			&self,
			mut mint: impl FnMut() -> SessionId,
		) -> Result<SavedSession<Writable>, SessionStoreError> {
			let root = ensure_root(&self.root)?;
			loop {
				let id = mint();
				match mkdir_at(&root, &id.as_hex(), PRIVATE_DIR_MODE) {
					Ok(()) => {
						root.sync_all()?;
						let session_dir = open_dir_at(&root, &id.as_hex())?;
						validate_private_dir(&session_dir, "session directory")?;
						let lock = open_lock(&session_dir, true, LockMode::Exclusive)?;
						mkdir_at(&session_dir, "generations", PRIVATE_DIR_MODE)?;
						session_dir.sync_all()?;
						let mut session = SavedSession {
							snapshot: SessionSnapshot {
								session_id: id,
								generation: 0,
								next_document_id: 1,
								documents: BTreeMap::new(),
							},
							owners: BTreeMap::new(),
							session_dir,
							_lock: lock,
							publication_uncertain: false,
							_mode: PhantomData,
						};
						session.commit()?;
						return Ok(session);
					}
					Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
					Err(error) => return Err(error.into()),
				}
			}
		}

		fn open_locked(
			&self,
			id: SessionId,
			mode: LockMode,
		) -> Result<(File, File), SessionStoreError> {
			let root = open_existing_root(&self.root)?;
			let session_dir = open_dir_at(&root, &id.as_hex())?;
			validate_private_dir(&session_dir, "session directory")?;
			// PRODUCT-0059: the lock must precede even the first CURRENT read.
			let lock = open_lock(&session_dir, false, mode).map_err(|error| {
				if matches!(error.raw_os_error(), Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN)
				{
					SessionStoreError::Contended(id)
				} else {
					SessionStoreError::Io(error)
				}
			})?;
			Ok((session_dir, lock))
		}
	}

	impl SavedSession<Writable> {
		pub fn mint_document(
			&mut self,
			path: Option<PathBuf>,
		) -> Result<DocumentId, SessionStoreError> {
			if self.snapshot.next_document_id == u64::MAX {
				return Err(SessionStoreError::DocumentIdExhausted);
			}
			let id = DocumentId::new(self.snapshot.next_document_id)?;
			self.snapshot.next_document_id += 1;
			self.snapshot
				.documents
				.insert(id, DocumentIdentity { id, path });
			Ok(id)
		}

		pub fn update_document_path(
			&mut self,
			id: DocumentId,
			path: Option<PathBuf>,
		) -> Result<(), SessionStoreError> {
			let document = self
				.snapshot
				.documents
				.get_mut(&id)
				.ok_or(SessionStoreError::UnknownDocument(id))?;
			document.path = path;
			Ok(())
		}

		pub fn remove_document(&mut self, id: DocumentId) -> Result<(), SessionStoreError> {
			self.snapshot
				.documents
				.remove(&id)
				.map(|_| ())
				.ok_or(SessionStoreError::UnknownDocument(id))
		}

		pub fn set_owner_payload(&mut self, owner: OwnerId, schema_version: u32, bytes: Vec<u8>) {
			self.owners.insert(
				owner,
				OwnerPayload {
					schema_version,
					bytes,
				},
			);
		}

		pub fn commit(&mut self) -> Result<(), SessionStoreError> {
			self.commit_with_fault(CommitFault::None)
		}

		fn commit_with_fault(&mut self, fault: CommitFault) -> Result<(), SessionStoreError> {
			if self.publication_uncertain {
				return Err(SessionStoreError::PublicationUncertain);
			}
			let generation = self
				.snapshot
				.generation
				.checked_add(1)
				.ok_or(SessionStoreError::GenerationExhausted)?;
			self.cleanup_unreferenced()?;
			let generation_name = generation_name(generation);
			let generations_dir = open_dir_at(&self.session_dir, "generations")?;
			let temp_name = create_unique_temp_dir(&generations_dir, ".generation")?;
			let temp_dir = open_dir_at(&generations_dir, &temp_name)?;

			let disk = snapshot_to_disk(&self.snapshot, &self.owners, generation)?;
			let manifest = serde_json::to_vec_pretty(&disk)
				.map_err(|error| SessionStoreError::Corrupt(format!("serialize store: {error}")))?;
			write_new_file_at(&temp_dir, "store.json", &manifest)?;
			mkdir_at(&temp_dir, "owners", PRIVATE_DIR_MODE)?;
			let owners_dir = open_dir_at(&temp_dir, "owners")?;
			for (owner, payload) in &self.owners {
				write_new_file_at(&owners_dir, &owner.payload_name(), &payload.bytes)?;
			}
			owners_dir.sync_all()?;
			temp_dir.sync_all()?;

			if fault == CommitFault::BeforeGenerationRename {
				return Err(injected_fault("before generation rename"));
			}
			rename_at(
				&generations_dir,
				&temp_name,
				&generations_dir,
				&generation_name,
			)?;
			generations_dir.sync_all()?;
			if fault == CommitFault::AfterGenerationRename {
				return Err(injected_fault("after generation rename"));
			}

			let current_temp = unique_name(".CURRENT");
			write_new_file_at(
				&self.session_dir,
				&current_temp,
				format!("{generation_name}\n").as_bytes(),
			)?;
			if fault == CommitFault::BeforeCurrentRename {
				return Err(injected_fault("before CURRENT rename"));
			}
			rename_at(
				&self.session_dir,
				&current_temp,
				&self.session_dir,
				"CURRENT",
			)?;
			self.snapshot.generation = generation;
			if fault == CommitFault::AfterCurrentRename {
				self.publication_uncertain = true;
				return Err(injected_fault("after CURRENT rename"));
			}
			if let Err(error) = self.session_dir.sync_all() {
				self.publication_uncertain = true;
				return Err(error.into());
			}
			Ok(())
		}

		pub fn cleanup_unreferenced(&mut self) -> Result<(), SessionStoreError> {
			if self.publication_uncertain {
				return Err(SessionStoreError::PublicationUncertain);
			}
			let generations_dir = open_dir_at(&self.session_dir, "generations")?;
			let current = generation_name(self.snapshot.generation);
			for entry in list_dir(&generations_dir)? {
				if entry == "." || entry == ".." || entry == current {
					continue;
				}
				remove_entry_at(&generations_dir, &entry)?;
			}
			generations_dir.sync_all()?;

			for entry in list_dir(&self.session_dir)? {
				if entry.starts_with(".CURRENT.tmp-") {
					unlink_at(&self.session_dir, &entry, 0)?;
				}
			}
			self.session_dir.sync_all()?;
			Ok(())
		}
	}

	fn snapshot_to_disk(
		snapshot: &SessionSnapshot,
		owners: &BTreeMap<OwnerId, OwnerPayload>,
		generation: u64,
	) -> Result<DiskSnapshot, SessionStoreError> {
		if snapshot.next_document_id == 0
			|| snapshot
				.documents
				.keys()
				.any(|id| id.get() >= snapshot.next_document_id)
		{
			return Err(SessionStoreError::Corrupt(
				"next document id does not exceed every allocated id".to_owned(),
			));
		}
		Ok(DiskSnapshot {
			version: STORE_VERSION,
			session_id: snapshot.session_id.as_hex(),
			generation,
			next_document_id: snapshot.next_document_id,
			documents: snapshot
				.documents
				.values()
				.map(|document| DiskDocument {
					id: document.id.get(),
					path_hex: document.path.as_deref().map(path_to_hex),
				})
				.collect(),
			owners: owners
				.iter()
				.map(|(owner, payload)| DiskOwner {
					owner: owner.to_string(),
					schema_version: payload.schema_version,
					byte_len: u64::try_from(payload.bytes.len())
						.expect("usize payload length fits u64 on supported Unix"),
				})
				.collect(),
		})
	}

	fn load_snapshot(
		session_dir: &File,
		expected_session: SessionId,
	) -> Result<(SessionSnapshot, BTreeMap<OwnerId, OwnerPayload>), SessionStoreError> {
		let current = read_limited_file_at(session_dir, "CURRENT", MAX_POINTER_BYTES)?;
		let current = std::str::from_utf8(&current)
			.map_err(|_| SessionStoreError::Corrupt("CURRENT is not UTF-8".to_owned()))?;
		let generation = parse_current(current)?;
		let generation_name = generation_name(generation);
		let generations_dir = open_dir_at(session_dir, "generations")?;
		let generation_dir = open_dir_at(&generations_dir, &generation_name)?;
		let manifest = read_limited_file_at(&generation_dir, "store.json", MAX_MANIFEST_BYTES)?;
		let disk: DiskSnapshot = serde_json::from_slice(&manifest)
			.map_err(|error| SessionStoreError::Corrupt(format!("store.json: {error}")))?;

		if disk.version != STORE_VERSION {
			return Err(SessionStoreError::Corrupt(format!(
				"unsupported store version {}",
				disk.version
			)));
		}
		let session_id = SessionId::parse_canonical(&disk.session_id)
			.map_err(|error| SessionStoreError::Corrupt(error.to_string()))?;
		if session_id != expected_session {
			return Err(SessionStoreError::Corrupt(
				"manifest session id does not match its directory".to_owned(),
			));
		}
		if disk.generation != generation {
			return Err(SessionStoreError::Corrupt(
				"manifest generation does not match CURRENT".to_owned(),
			));
		}
		if disk.next_document_id == 0 {
			return Err(SessionStoreError::Corrupt(
				"next document id is zero".to_owned(),
			));
		}

		let mut documents = BTreeMap::new();
		for document in disk.documents {
			let id = DocumentId::new(document.id)?;
			if id.get() >= disk.next_document_id {
				return Err(SessionStoreError::Corrupt(format!(
					"document {id} is not below next document id"
				)));
			}
			let identity = DocumentIdentity {
				id,
				path: document
					.path_hex
					.as_deref()
					.map(path_from_hex)
					.transpose()?,
			};
			if documents.insert(id, identity).is_some() {
				return Err(SessionStoreError::Corrupt(format!(
					"duplicate document {id}"
				)));
			}
		}

		let owners_dir = open_dir_at(&generation_dir, "owners")?;
		let mut owners = BTreeMap::new();
		for owner in disk.owners {
			let id = OwnerId::new(owner.owner)
				.map_err(|error| SessionStoreError::Corrupt(error.to_string()))?;
			let bytes = read_file_at(&owners_dir, &id.payload_name())?;
			if bytes.len() as u64 != owner.byte_len {
				return Err(SessionStoreError::Corrupt(format!(
					"owner {id} payload length is {}, expected {}",
					bytes.len(),
					owner.byte_len
				)));
			}
			if owners
				.insert(
					id.clone(),
					OwnerPayload {
						schema_version: owner.schema_version,
						bytes,
					},
				)
				.is_some()
			{
				return Err(SessionStoreError::Corrupt(format!("duplicate owner {id}")));
			}
		}
		let expected_owner_files = owners
			.keys()
			.map(OwnerId::payload_name)
			.collect::<BTreeSet<_>>();
		let actual_owner_files = list_dir(&owners_dir)?
			.into_iter()
			.filter(|name| name != "." && name != "..")
			.collect::<BTreeSet<_>>();
		if actual_owner_files != expected_owner_files {
			return Err(SessionStoreError::Corrupt(
				"owner directory does not exactly match its descriptors".to_owned(),
			));
		}

		Ok((
			SessionSnapshot {
				session_id,
				generation,
				next_document_id: disk.next_document_id,
				documents,
			},
			owners,
		))
	}

	fn parse_current(value: &str) -> Result<u64, SessionStoreError> {
		if value.len() != GENERATION_HEX_LEN + 1 || !value.ends_with('\n') {
			return Err(SessionStoreError::Corrupt(
				"CURRENT must be 16 lowercase hexadecimal digits plus newline".to_owned(),
			));
		}
		let digits = &value[..GENERATION_HEX_LEN];
		if !digits
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
		{
			return Err(SessionStoreError::Corrupt(
				"CURRENT generation is not canonical lowercase hexadecimal".to_owned(),
			));
		}
		let generation = u64::from_str_radix(digits, 16)
			.map_err(|error| SessionStoreError::Corrupt(format!("CURRENT: {error}")))?;
		if generation == 0 {
			return Err(SessionStoreError::Corrupt(
				"CURRENT generation is zero".to_owned(),
			));
		}
		Ok(generation)
	}

	fn generation_name(generation: u64) -> String {
		format!("{generation:016x}")
	}

	fn injected_fault(stage: &str) -> SessionStoreError {
		SessionStoreError::Io(io::Error::other(format!("injected fault {stage}")))
	}

	fn ensure_root(path: &Path) -> Result<File, SessionStoreError> {
		let mut builder = fs::DirBuilder::new();
		builder.recursive(true).mode(PRIVATE_DIR_MODE);
		builder.create(path)?;
		let file = open_root_fd(path)?;
		let metadata = file.metadata()?;
		if !metadata.is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
			return Err(SessionStoreError::Io(io::Error::new(
				io::ErrorKind::PermissionDenied,
				"session root must be a directory owned by this user",
			)));
		}
		if unsafe { libc::fchmod(file.as_raw_fd(), PRIVATE_DIR_MODE as libc::mode_t) } != 0 {
			return Err(io::Error::last_os_error().into());
		}
		validate_private_dir(&file, "session root")?;
		Ok(file)
	}

	fn open_existing_root(path: &Path) -> Result<File, SessionStoreError> {
		let file = open_root_fd(path)?;
		validate_private_dir(&file, "session root")?;
		Ok(file)
	}

	fn open_root_fd(path: &Path) -> io::Result<File> {
		let path = c_path(path)?;
		let fd = unsafe {
			libc::open(
				path.as_ptr(),
				libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
			)
		};
		file_from_fd(fd)
	}

	fn open_lock(session_dir: &File, create: bool, mode: LockMode) -> io::Result<File> {
		let mut flags = match mode {
			LockMode::Shared => libc::O_RDONLY,
			LockMode::Exclusive => libc::O_RDWR,
		};
		if create {
			flags |= libc::O_CREAT | libc::O_EXCL;
		}
		let lock = open_file_at(session_dir, "session.lock", flags, PRIVATE_FILE_MODE)?;
		validate_private_file(&lock, "session.lock")?;
		let operation = match mode {
			LockMode::Shared => libc::LOCK_SH | libc::LOCK_NB,
			LockMode::Exclusive => libc::LOCK_EX | libc::LOCK_NB,
		};
		if unsafe { libc::flock(lock.as_raw_fd(), operation) } != 0 {
			return Err(io::Error::last_os_error());
		}
		Ok(lock)
	}

	fn open_dir_at(parent: &File, name: &str) -> io::Result<File> {
		let file = open_file_at(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
		if !file.metadata()?.is_dir() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("{name} is not a directory"),
			));
		}
		Ok(file)
	}

	fn open_file_at(parent: &File, name: &str, flags: i32, mode: u32) -> io::Result<File> {
		let name = c_name(name)?;
		let fd = unsafe {
			libc::openat(
				parent.as_raw_fd(),
				name.as_ptr(),
				flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
				mode as libc::mode_t,
			)
		};
		file_from_fd(fd)
	}

	fn file_from_fd(fd: RawFd) -> io::Result<File> {
		if fd < 0 {
			Err(io::Error::last_os_error())
		} else {
			Ok(unsafe { File::from_raw_fd(fd) })
		}
	}

	fn mkdir_at(parent: &File, name: &str, mode: u32) -> io::Result<()> {
		let name = c_name(name)?;
		if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), mode as libc::mode_t) } == 0 {
			Ok(())
		} else {
			Err(io::Error::last_os_error())
		}
	}

	fn rename_at(from_dir: &File, from: &str, to_dir: &File, to: &str) -> io::Result<()> {
		let from = c_name(from)?;
		let to = c_name(to)?;
		if unsafe {
			libc::renameat(
				from_dir.as_raw_fd(),
				from.as_ptr(),
				to_dir.as_raw_fd(),
				to.as_ptr(),
			)
		} == 0
		{
			Ok(())
		} else {
			Err(io::Error::last_os_error())
		}
	}

	fn unlink_at(parent: &File, name: &str, flags: i32) -> io::Result<()> {
		let name = c_name(name)?;
		if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), flags) } == 0 {
			Ok(())
		} else {
			Err(io::Error::last_os_error())
		}
	}

	fn write_new_file_at(parent: &File, name: &str, bytes: &[u8]) -> io::Result<()> {
		let mut file = open_file_at(
			parent,
			name,
			libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
			PRIVATE_FILE_MODE,
		)?;
		file.write_all(bytes)?;
		file.sync_all()?;
		Ok(())
	}

	fn read_file_at(parent: &File, name: &str) -> io::Result<Vec<u8>> {
		let mut file = open_file_at(parent, name, libc::O_RDONLY, 0)?;
		validate_private_file(&file, name)?;
		let mut bytes = Vec::new();
		file.read_to_end(&mut bytes)?;
		Ok(bytes)
	}

	fn read_limited_file_at(parent: &File, name: &str, limit: usize) -> io::Result<Vec<u8>> {
		let mut file = open_file_at(parent, name, libc::O_RDONLY, 0)?;
		validate_private_file(&file, name)?;
		let mut bytes = Vec::new();
		Read::by_ref(&mut file)
			.take((limit + 1) as u64)
			.read_to_end(&mut bytes)?;
		if bytes.len() > limit {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("{name} exceeds {limit} bytes"),
			));
		}
		Ok(bytes)
	}

	fn validate_private_dir(file: &File, subject: &str) -> io::Result<()> {
		let metadata = file.metadata()?;
		if !metadata.is_dir() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("{subject} is not a directory"),
			));
		}
		if metadata.uid() != unsafe { libc::geteuid() } {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{subject} is not owned by this user"),
			));
		}
		if metadata.mode() & 0o077 != 0 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{subject} grants group or world access"),
			));
		}
		Ok(())
	}

	fn validate_private_file(file: &File, subject: &str) -> io::Result<()> {
		let metadata = file.metadata()?;
		if !metadata.is_file() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("{subject} is not a regular file"),
			));
		}
		if metadata.uid() != unsafe { libc::geteuid() } || metadata.nlink() != 1 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{subject} has unsafe ownership or links"),
			));
		}
		if metadata.mode() & 0o077 != 0 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{subject} grants group or world access"),
			));
		}
		Ok(())
	}

	fn create_unique_temp_dir(parent: &File, prefix: &str) -> io::Result<String> {
		loop {
			let name = unique_name(prefix);
			match mkdir_at(parent, &name, PRIVATE_DIR_MODE) {
				Ok(()) => return Ok(name),
				Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
				Err(error) => return Err(error),
			}
		}
	}

	fn unique_name(prefix: &str) -> String {
		format!(
			"{prefix}.tmp-{}-{:016x}",
			std::process::id(),
			rand::random::<u64>()
		)
	}

	fn c_name(name: &str) -> io::Result<CString> {
		if name.is_empty() || name == "." || name == ".." || name.contains('/') {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"unsafe child name",
			));
		}
		CString::new(name)
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "child name contains NUL"))
	}

	fn c_path(path: &Path) -> io::Result<CString> {
		CString::new(path.as_os_str().as_bytes())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
	}

	fn list_dir(directory: &File) -> io::Result<Vec<String>> {
		let duplicated = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
		if duplicated < 0 {
			return Err(io::Error::last_os_error());
		}
		let stream = unsafe { libc::fdopendir(duplicated) };
		if stream.is_null() {
			unsafe { libc::close(duplicated) };
			return Err(io::Error::last_os_error());
		}

		let mut names = Vec::new();
		loop {
			set_errno(0);
			let entry = unsafe { libc::readdir(stream) };
			if entry.is_null() {
				let code = current_errno();
				unsafe { libc::closedir(stream) };
				if code == 0 {
					return Ok(names);
				}
				return Err(io::Error::from_raw_os_error(code));
			}
			let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
			let name = match name.to_str() {
				Ok(name) => name,
				Err(_) => {
					unsafe { libc::closedir(stream) };
					return Err(io::Error::new(
						io::ErrorKind::InvalidData,
						"directory entry is not UTF-8",
					));
				}
			};
			names.push(name.to_owned());
		}
	}

	#[cfg(any(target_os = "linux", target_os = "android"))]
	fn errno_pointer() -> *mut libc::c_int {
		unsafe { libc::__errno_location() }
	}

	#[cfg(any(
		target_os = "macos",
		target_os = "ios",
		target_os = "freebsd",
		target_os = "openbsd",
		target_os = "netbsd",
		target_os = "dragonfly"
	))]
	fn errno_pointer() -> *mut libc::c_int {
		unsafe { libc::__error() }
	}

	fn set_errno(value: libc::c_int) {
		unsafe { *errno_pointer() = value };
	}

	fn current_errno() -> libc::c_int {
		unsafe { *errno_pointer() }
	}

	fn remove_entry_at(parent: &File, name: &str) -> io::Result<()> {
		match open_dir_at(parent, name) {
			Ok(directory) => {
				for child in list_dir(&directory)? {
					if child != "." && child != ".." {
						remove_entry_at(&directory, &child)?;
					}
				}
				drop(directory);
				unlink_at(parent, name, libc::AT_REMOVEDIR)
			}
			Err(error) if error.raw_os_error() == Some(libc::ENOTDIR) => unlink_at(parent, name, 0),
			Err(error) => Err(error),
		}
	}

	#[cfg(test)]
	mod tests {
		use super::*;
		use crate::core::BufferId;
		use std::collections::{HashMap, VecDeque};
		use std::os::unix::ffi::OsStringExt;
		use std::os::unix::fs::{PermissionsExt, symlink};
		use std::process::{Child, Command, Stdio};
		use std::sync::{Mutex, MutexGuard};
		use std::thread;
		use std::time::{Duration, Instant};

		struct TempRoot(PathBuf);
		static SESSION_TEST_PROCESS_LOCK: Mutex<()> = Mutex::new(());

		fn serialize_process_tests() -> MutexGuard<'static, ()> {
			// O_CLOEXEC closes a descriptor at exec. Between fork and exec the child necessarily
			// shares its parent's open file descriptions, so unrelated test threads could observe a
			// correct transient contention. Keep this fixture family serial while its real lock
			// contenders remain separate processes.
			SESSION_TEST_PROCESS_LOCK.lock().expect("session test lock")
		}

		impl TempRoot {
			fn new(name: &str) -> Self {
				let path = std::env::temp_dir().join(format!(
					"libaryon-session-{}-{name}-{:016x}",
					std::process::id(),
					rand::random::<u64>()
				));
				Self(path)
			}

			fn store(&self) -> SessionStore {
				SessionStore::at(self.0.clone())
			}
		}

		impl Drop for TempRoot {
			fn drop(&mut self) {
				let _ = fs::remove_dir_all(&self.0);
			}
		}

		struct ChildGuard(Option<Child>);

		impl ChildGuard {
			fn kill_and_wait(&mut self) {
				if let Some(mut child) = self.0.take() {
					let _ = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGKILL) };
					let _ = child.wait();
				}
			}
		}

		impl Drop for ChildGuard {
			fn drop(&mut self) {
				self.kill_and_wait();
			}
		}

		fn known_session(byte: u8) -> SessionId {
			SessionId::from_random_bytes([byte; 16]).expect("nonzero session id")
		}

		fn session_path(root: &TempRoot, id: SessionId) -> PathBuf {
			root.0.join(id.to_string())
		}

		fn current_generation_path(root: &TempRoot, id: SessionId) -> PathBuf {
			let session = session_path(root, id);
			let current = fs::read_to_string(session.join("CURRENT")).unwrap();
			session.join("generations").join(current.trim())
		}

		fn read_tree(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
			fn visit(root: &Path, path: &Path, found: &mut BTreeMap<PathBuf, Vec<u8>>) {
				let mut entries = fs::read_dir(path)
					.unwrap()
					.map(|entry| entry.unwrap())
					.collect::<Vec<_>>();
				entries.sort_by_key(|entry| entry.file_name());
				for entry in entries {
					let kind = entry.file_type().unwrap();
					if kind.is_dir() {
						visit(root, &entry.path(), found);
					} else if kind.is_file() {
						found.insert(
							entry.path().strip_prefix(root).unwrap().to_owned(),
							fs::read(entry.path()).unwrap(),
						);
					}
				}
			}

			let mut found = BTreeMap::new();
			visit(path, path, &mut found);
			found
		}

		fn assert_private_tree(path: &Path) {
			let metadata = fs::symlink_metadata(path).unwrap();
			assert_eq!(
				metadata.permissions().mode() & 0o077,
				0,
				"unsafe mode at {}",
				path.display()
			);
			if metadata.is_dir() {
				for entry in fs::read_dir(path).unwrap() {
					assert_private_tree(&entry.unwrap().path());
				}
			}
		}

		#[test]
		fn product_0059_ids_paths_rebinding_and_owner_payloads_round_trip() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("round-trip");
			let store = root.store();
			let mut session = store.create().expect("create session");
			let session_id = session.snapshot().session_id();
			let lock_path = session_path(&root, session_id).join("session.lock");
			let lock_inode = fs::metadata(&lock_path).unwrap().ino();
			assert_eq!(session_id.to_string().len(), SESSION_ID_HEX_LEN);
			assert_eq!(
				session_id.to_string().parse::<SessionId>().unwrap(),
				session_id
			);

			let scratch = session.mint_document(None).expect("mint scratch");
			let removed = session
				.mint_document(Some(PathBuf::from("before.rn")))
				.expect("mint file");
			let non_utf8 =
				PathBuf::from(OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]));
			session
				.update_document_path(scratch, Some(non_utf8.clone()))
				.expect("save-as preserves identity");
			session.remove_document(removed).expect("remove document");
			let later = session.mint_document(None).expect("mint after removal");
			assert_eq!(scratch.get(), 1);
			assert_eq!(removed.get(), 2);
			assert_eq!(later.get(), 3, "removed document ids must not be reused");

			let polariton = OwnerId::new("polariton").unwrap();
			let opaque = vec![0, 0xff, b'\n', 7, 0x80];
			session.set_owner_payload(polariton.clone(), 9, opaque.clone());
			session.commit().expect("commit state");
			assert_eq!(session.snapshot().generation(), 2);
			session
				.cleanup_unreferenced()
				.expect("cleanup old generation");
			assert_eq!(fs::metadata(&lock_path).unwrap().ino(), lock_inode);
			assert_private_tree(&root.0);
			drop(session);

			let reopened = store.open_read_only(session_id).expect("reopen state");
			assert_eq!(reopened.snapshot().generation(), 2);
			assert_eq!(
				reopened.snapshot().document(scratch).unwrap().path(),
				Some(non_utf8.as_path())
			);
			assert!(reopened.snapshot().document(removed).is_none());
			assert_eq!(
				reopened.owner_payload(&polariton),
				Some((9, opaque.as_slice()))
			);

			let runtime_ids = [BufferId::new(91), BufferId::new(17)];
			let bindings: HashMap<DocumentId, BufferId> = reopened
				.snapshot()
				.documents()
				.rev()
				.zip(runtime_ids)
				.map(|(document, runtime)| (document.id(), runtime))
				.collect();
			assert_eq!(bindings[&scratch], BufferId::new(17));
			assert_eq!(bindings[&later], BufferId::new(91));

			let manifest_path = session_path(&root, session_id)
				.join("generations")
				.join("0000000000000002")
				.join("store.json");
			let manifest = fs::read_to_string(manifest_path).expect("read manifest");
			for runtime_name in ["BufferId", "PaneId", "ViewId"] {
				assert!(!manifest.contains(runtime_name));
			}

			drop(reopened);
			let mut separate = store.create().expect("create second session");
			let separate_id = separate.snapshot().session_id();
			let same_local = separate
				.mint_document(Some(non_utf8))
				.expect("mint in second session");
			assert_eq!(same_local, scratch);
			assert_ne!(
				separate_id, session_id,
				"session id disambiguates local document ids"
			);
		}

		#[test]
		fn product_0059_identity_and_owner_grammars_refuse_unsafe_spellings() {
			let _serial = serialize_process_tests();
			let canonical = known_session(0xab).to_string();
			assert_eq!(canonical, "abababababababababababababababab");
			for invalid in [
				"00000000000000000000000000000000",
				"ABABABABABABABABABABABABABABABAB",
				"abab",
				"abababababababababababababababab00",
				"../../../../../../../../etc/passwd",
				"abababababababababababababababa/",
			] {
				assert!(
					invalid.parse::<SessionId>().is_err(),
					"accepted {invalid:?}"
				);
			}
			for invalid in [
				"",
				"Polariton",
				".",
				"a/b",
				"a.b",
				"é",
				"a_very_long_owner_name_that_exceeds_the_sixty_four_character_limit_by_far",
			] {
				assert!(OwnerId::new(invalid).is_err(), "accepted {invalid:?}");
			}
			assert!(OwnerId::new("a").is_ok());
			assert!(OwnerId::new("polariton_v1").is_ok());
		}

		#[test]
		fn product_0059_state_roots_are_head_neutral_and_platform_exact() {
			let _serial = serialize_process_tests();
			assert_eq!(
				default_session_root_for(
					SessionPlatform::Xdg,
					Some(OsString::from("/state")),
					Some(OsString::from("/home/me")),
				),
				Some(PathBuf::from("/state/libaryon/sessions"))
			);
			assert_eq!(
				default_session_root_for(
					SessionPlatform::Xdg,
					Some(OsString::new()),
					Some(OsString::from("/home/me")),
				),
				Some(PathBuf::from("/home/me/.local/state/libaryon/sessions"))
			);
			assert_eq!(
				default_session_root_for(
					SessionPlatform::Macos,
					Some(OsString::from("/ignored")),
					Some(OsString::from("/Users/me")),
				),
				Some(PathBuf::from(
					"/Users/me/Library/Application Support/libaryon/state/sessions"
				))
			);
		}

		#[test]
		fn product_0059_mint_collision_retries_without_touching_the_existing_session() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("collision");
			let store = root.store();
			let existing_id = known_session(1);
			let existing = store
				.create_with_minter(|| existing_id)
				.expect("create fixed session");
			drop(existing);
			let current_path = session_path(&root, existing_id).join("CURRENT");
			let before = fs::read(&current_path).unwrap();

			let replacement_id = known_session(2);
			let mut candidates = VecDeque::from([existing_id, replacement_id]);
			let replacement = store
				.create_with_minter(|| candidates.pop_front().expect("candidate"))
				.expect("retry collision");
			assert_eq!(replacement.snapshot().session_id(), replacement_id);
			assert_eq!(fs::read(current_path).unwrap(), before);
		}

		#[test]
		fn product_0059_faults_never_publish_a_partial_generation() {
			let _serial = serialize_process_tests();
			for fault in [
				CommitFault::BeforeGenerationRename,
				CommitFault::AfterGenerationRename,
				CommitFault::BeforeCurrentRename,
			] {
				let root = TempRoot::new("fault");
				let store = root.store();
				let mut session = store.create().unwrap();
				let id = session
					.mint_document(Some(PathBuf::from("old.rn")))
					.unwrap();
				let owner = OwnerId::new("future_head").unwrap();
				session.set_owner_payload(owner.clone(), 3, vec![0, 0xff, 1]);
				session.commit().unwrap();
				let session_id = session.snapshot().session_id();
				let committed_generation = session.snapshot().generation();
				session
					.update_document_path(id, Some(PathBuf::from("new.rn")))
					.unwrap();
				session.set_owner_payload(owner.clone(), 4, b"new".to_vec());
				assert!(session.commit_with_fault(fault).is_err());
				drop(session);

				let reopened = store.open_read_only(session_id).unwrap();
				assert_eq!(reopened.snapshot().generation(), committed_generation);
				assert_eq!(
					reopened.snapshot().document(id).unwrap().path(),
					Some(Path::new("old.rn"))
				);
				assert_eq!(
					reopened.owner_payload(&owner),
					Some((3, [0, 0xff, 1].as_slice()))
				);
				drop(reopened);

				let _writable = store.open_writable(session_id).unwrap();
				let entries = fs::read_dir(session_path(&root, session_id).join("generations"))
					.unwrap()
					.map(|entry| entry.unwrap().file_name())
					.collect::<Vec<_>>();
				assert_eq!(
					entries,
					[OsString::from(generation_name(committed_generation))]
				);
			}
		}

		#[test]
		fn product_0059_retry_reclaims_an_unpublished_same_generation() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("retry");
			let store = root.store();
			let mut session = store.create().unwrap();
			let id = session
				.mint_document(Some(PathBuf::from("retry.rn")))
				.unwrap();
			assert!(
				session
					.commit_with_fault(CommitFault::AfterGenerationRename)
					.is_err()
			);
			session
				.commit()
				.expect("retry cleans unpublished generation");
			assert_eq!(session.snapshot().generation(), 2);
			assert_eq!(
				session.snapshot().document(id).unwrap().path(),
				Some(Path::new("retry.rn"))
			);
		}

		#[test]
		fn product_0059_failure_after_current_rename_exposes_the_new_complete_generation() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("post-current");
			let store = root.store();
			let mut session = store.create().unwrap();
			let session_id = session.snapshot().session_id();
			let id = session
				.mint_document(Some(PathBuf::from("published.rn")))
				.unwrap();
			assert!(
				session
					.commit_with_fault(CommitFault::AfterCurrentRename)
					.is_err()
			);
			assert_eq!(session.snapshot().generation(), 2);
			assert!(matches!(
				session.commit(),
				Err(SessionStoreError::PublicationUncertain)
			));
			drop(session);

			let reopened = store.open_read_only(session_id).unwrap();
			assert_eq!(reopened.snapshot().generation(), 2);
			assert_eq!(
				reopened.snapshot().document(id).unwrap().path(),
				Some(Path::new("published.rn"))
			);
		}

		#[test]
		fn product_0059_unknown_owner_survives_base_only_commit_byte_for_byte() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("owner-preservation");
			let store = root.store();
			let mut session = store.create().unwrap();
			let id = session.mint_document(None).unwrap();
			let owner = OwnerId::new("unknown_future").unwrap();
			let bytes = vec![0, 1, 0xff, 2, 0x80, 3];
			session.set_owner_payload(owner.clone(), 77, bytes.clone());
			session.commit().unwrap();
			let session_id = session.snapshot().session_id();
			drop(session);

			let mut reopened = store.open_writable(session_id).unwrap();
			reopened
				.update_document_path(id, Some(PathBuf::from("named.rn")))
				.unwrap();
			reopened.commit().unwrap();
			drop(reopened);

			let final_state = store.open_read_only(session_id).unwrap();
			assert_eq!(
				final_state.owner_payload(&owner),
				Some((77, bytes.as_slice()))
			);
		}

		#[test]
		fn product_0059_corruption_refuses_without_guessing_or_rewriting() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("corruption");
			let store = root.store();
			let mut session = store.create().unwrap();
			let owner = OwnerId::new("opaque").unwrap();
			session.set_owner_payload(owner.clone(), 4, vec![1, 2, 3, 4]);
			session.commit().unwrap();
			let id = session.snapshot().session_id();
			drop(session);

			let manifest_path = current_generation_path(&root, id).join("store.json");
			let original_manifest = fs::read(&manifest_path).unwrap();
			let mut value: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
			value
				.as_object_mut()
				.unwrap()
				.insert("future_base_field".to_owned(), 1.into());
			fs::write(&manifest_path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
			let before = read_tree(&session_path(&root, id));
			assert!(
				matches!(store.open_read_only(id), Err(SessionStoreError::Corrupt(reason)) if reason.contains("unknown field"))
			);
			assert_eq!(
				read_tree(&session_path(&root, id)),
				before,
				"failed open must not rewrite bytes"
			);

			fs::write(&manifest_path, &original_manifest).unwrap();
			let mut value: serde_json::Value = serde_json::from_slice(&original_manifest).unwrap();
			value["session_id"] = known_session(9).to_string().into();
			fs::write(&manifest_path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
			assert!(
				matches!(store.open_read_only(id), Err(SessionStoreError::Corrupt(reason)) if reason.contains("does not match"))
			);

			fs::write(&manifest_path, &original_manifest).unwrap();
			let payload = current_generation_path(&root, id)
				.join("owners")
				.join(owner.payload_name());
			fs::write(payload, [1, 2]).unwrap();
			assert!(
				matches!(store.open_read_only(id), Err(SessionStoreError::Corrupt(reason)) if reason.contains("payload length"))
			);

			let payload = current_generation_path(&root, id)
				.join("owners")
				.join(owner.payload_name());
			fs::write(&payload, [1, 2, 3, 4]).unwrap();
			fs::write(payload.with_file_name("unlisted.payload"), b"unlisted").unwrap();
			assert!(
				matches!(store.open_read_only(id), Err(SessionStoreError::Corrupt(reason)) if reason.contains("does not exactly match"))
			);
		}

		#[test]
		fn product_0059_no_follow_opens_keep_store_symlinks_inert() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("symlink");
			let store = root.store();
			let session = store.create().unwrap();
			let id = session.snapshot().session_id();
			drop(session);

			let outside = root.0.join("outside");
			fs::write(&outside, b"outside-control").unwrap();
			let current = session_path(&root, id).join("CURRENT");
			fs::remove_file(&current).unwrap();
			symlink(&outside, &current).unwrap();
			assert!(store.open_read_only(id).is_err());
			assert_eq!(fs::read(&outside).unwrap(), b"outside-control");

			let other = store.create().unwrap();
			let other_id = other.snapshot().session_id();
			drop(other);
			let lock = session_path(&root, other_id).join("session.lock");
			fs::remove_file(&lock).unwrap();
			symlink(&outside, &lock).unwrap();
			assert!(store.open_writable(other_id).is_err());
			assert_eq!(fs::read(&outside).unwrap(), b"outside-control");

			let third = store.create().unwrap();
			let third_id = third.snapshot().session_id();
			drop(third);
			let generation = current_generation_path(&root, third_id);
			let parked = generation.with_extension("parked");
			fs::rename(&generation, &parked).unwrap();
			let outside_dir = root.0.join("outside-dir");
			fs::create_dir(&outside_dir).unwrap();
			symlink(&outside_dir, &generation).unwrap();
			assert!(store.open_read_only(third_id).is_err());

			let mut fourth = store.create().unwrap();
			let fourth_id = fourth.snapshot().session_id();
			let owner = OwnerId::new("opaque").unwrap();
			fourth.set_owner_payload(owner.clone(), 1, b"inside".to_vec());
			fourth.commit().unwrap();
			drop(fourth);
			let payload = current_generation_path(&root, fourth_id)
				.join("owners")
				.join(owner.payload_name());
			fs::remove_file(&payload).unwrap();
			symlink(&outside, &payload).unwrap();
			assert!(store.open_read_only(fourth_id).is_err());
			assert_eq!(fs::read(&outside).unwrap(), b"outside-control");

			let redirected = root.0.join("redirected-root");
			let redirect_target = root.0.join("redirect-target");
			fs::create_dir(&redirect_target).unwrap();
			fs::set_permissions(&redirect_target, fs::Permissions::from_mode(0o755)).unwrap();
			symlink(&redirect_target, &redirected).unwrap();
			assert!(SessionStore::at(redirected).create().is_err());
			assert_eq!(
				fs::metadata(redirect_target).unwrap().permissions().mode() & 0o777,
				0o755
			);
		}

		#[test]
		fn product_0059_allocators_refuse_before_wrap_or_staging() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("exhaustion");
			let store = root.store();
			let mut session = store.create().unwrap();
			session.snapshot.next_document_id = u64::MAX;
			assert!(matches!(
				session.mint_document(None),
				Err(SessionStoreError::DocumentIdExhausted)
			));
			let before =
				fs::read(session_path(&root, session.snapshot().session_id()).join("CURRENT"))
					.unwrap();
			session.snapshot.generation = u64::MAX;
			assert!(matches!(
				session.commit(),
				Err(SessionStoreError::GenerationExhausted)
			));
			let after =
				fs::read(session_path(&root, session.snapshot().session_id()).join("CURRENT"))
					.unwrap();
			assert_eq!(before, after);
		}

		fn spawn_helper(
			root: &TempRoot,
			id: SessionId,
			role: &str,
			ready: &Path,
			pid: &Path,
		) -> ChildGuard {
			let child = Command::new(std::env::current_exe().expect("test binary"))
				.args([
					"--exact",
					"core::session_store::unix::tests::session_store_process_helper",
					"--nocapture",
				])
				.env("LIBARYON_SESSION_HELPER", role)
				.env("LIBARYON_SESSION_ROOT", &root.0)
				.env("LIBARYON_SESSION_ID", id.to_string())
				.env("LIBARYON_SESSION_READY", ready)
				.env("LIBARYON_SESSION_GRANDCHILD_PID", pid)
				.stdin(Stdio::null())
				.stdout(Stdio::null())
				.stderr(Stdio::null())
				.spawn()
				.expect("spawn session helper");
			ChildGuard(Some(child))
		}

		fn wait_for(path: &Path) {
			let deadline = Instant::now() + Duration::from_secs(10);
			while !path.exists() {
				assert!(
					Instant::now() < deadline,
					"timed out waiting for {}",
					path.display()
				);
				thread::sleep(Duration::from_millis(10));
			}
		}

		#[test]
		fn product_0059_lock_contention_precedes_current_and_readers_share() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("locking");
			let store = root.store();
			let session = store.create().unwrap();
			let id = session.snapshot().session_id();
			drop(session);

			let ready = root.0.join("exclusive-ready");
			let pid = root.0.join("unused-pid");
			let mut holder = spawn_helper(&root, id, "exclusive", &ready, &pid);
			wait_for(&ready);
			assert!(
				matches!(store.open_writable(id), Err(SessionStoreError::Contended(found)) if found == id)
			);
			assert!(
				matches!(store.open_read_only(id), Err(SessionStoreError::Contended(found)) if found == id)
			);

			let current_path = session_path(&root, id).join("CURRENT");
			let current = fs::read(&current_path).unwrap();
			fs::write(&current_path, b"broken\n").unwrap();
			assert!(
				matches!(store.open_writable(id), Err(SessionStoreError::Contended(found)) if found == id)
			);
			fs::write(&current_path, current).unwrap();
			holder.kill_and_wait();
			drop(
				store
					.open_writable(id)
					.expect("kernel releases lock on death"),
			);

			let ready_a = root.0.join("shared-a");
			let ready_b = root.0.join("shared-b");
			let mut reader_a = spawn_helper(&root, id, "shared", &ready_a, &pid);
			let mut reader_b = spawn_helper(&root, id, "shared", &ready_b, &pid);
			wait_for(&ready_a);
			wait_for(&ready_b);
			assert!(
				matches!(store.open_writable(id), Err(SessionStoreError::Contended(found)) if found == id)
			);
			drop(store.open_read_only(id).expect("shared locks coexist"));
			reader_a.kill_and_wait();
			reader_b.kill_and_wait();
		}

		#[test]
		fn product_0059_exec_child_cannot_become_a_phantom_lock_holder() {
			let _serial = serialize_process_tests();
			let root = TempRoot::new("cloexec");
			let store = root.store();
			let session = store.create().unwrap();
			let id = session.snapshot().session_id();
			let flags = unsafe { libc::fcntl(session._lock.as_raw_fd(), libc::F_GETFD) };
			assert_ne!(
				flags & libc::FD_CLOEXEC,
				0,
				"lock descriptor must be close-on-exec"
			);
			drop(session);

			let ready = root.0.join("grandchild-ready");
			let pid_path = root.0.join("grandchild-pid");
			let mut holder = spawn_helper(&root, id, "exclusive-grandchild", &ready, &pid_path);
			wait_for(&ready);
			let grandchild_pid: libc::pid_t =
				fs::read_to_string(&pid_path).unwrap().parse().unwrap();
			holder.kill_and_wait();
			let reopened = store
				.open_writable(id)
				.expect("exec child must not inherit lock");
			assert_eq!(
				unsafe { libc::kill(grandchild_pid, 0) },
				0,
				"grandchild control must still be alive"
			);
			drop(reopened);
			unsafe { libc::kill(grandchild_pid, libc::SIGKILL) };
		}

		#[test]
		fn session_store_process_helper() {
			let Ok(role) = std::env::var("LIBARYON_SESSION_HELPER") else {
				return;
			};
			let root =
				PathBuf::from(std::env::var_os("LIBARYON_SESSION_ROOT").expect("helper root"));
			let id = std::env::var("LIBARYON_SESSION_ID")
				.unwrap()
				.parse::<SessionId>()
				.unwrap();
			let ready = PathBuf::from(std::env::var_os("LIBARYON_SESSION_READY").unwrap());
			let pid_path =
				PathBuf::from(std::env::var_os("LIBARYON_SESSION_GRANDCHILD_PID").unwrap());
			let store = SessionStore::at(root);
			match role.as_str() {
				"exclusive" => {
					let _session = store.open_writable(id).unwrap();
					fs::write(ready, b"ready").unwrap();
					loop {
						thread::sleep(Duration::from_secs(60));
					}
				}
				"shared" => {
					let _session = store.open_read_only(id).unwrap();
					fs::write(ready, b"ready").unwrap();
					loop {
						thread::sleep(Duration::from_secs(60));
					}
				}
				"exclusive-grandchild" => {
					let _session = store.open_writable(id).unwrap();
					let child = Command::new("sleep").arg("60").spawn().unwrap();
					fs::write(pid_path, child.id().to_string()).unwrap();
					fs::write(ready, b"ready").unwrap();
					std::mem::forget(child);
					loop {
						thread::sleep(Duration::from_secs(60));
					}
				}
				other => panic!("unknown helper role {other}"),
			}
		}
	}
}

#[cfg(not(unix))]
impl SessionStore {
	pub fn create(&self) -> Result<SavedSession<Writable>, SessionStoreError> {
		Err(SessionStoreError::UnsupportedPlatform)
	}

	pub fn open_read_only(
		&self,
		_id: SessionId,
	) -> Result<SavedSession<ReadOnly>, SessionStoreError> {
		Err(SessionStoreError::UnsupportedPlatform)
	}

	pub fn open_writable(
		&self,
		_id: SessionId,
	) -> Result<SavedSession<Writable>, SessionStoreError> {
		Err(SessionStoreError::UnsupportedPlatform)
	}
}
