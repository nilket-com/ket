//! Strict owner payload for libaryon's durable document images.
//!
//! The generic session store publishes this byte string atomically with every
//! sibling owner. This module owns its schema and refuses malformed or partial
//! document sets before engine state is constructed.

use super::document::io::MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES;
use super::language::LanguageRegistry;
use super::session::{EditorSession, FileStamp, LanguageSelection, RestoredDocument};
use super::settings::EngineSettings;
use crate::core::{
	DocumentId, DocumentIdentity, OwnerId, SavedSession, SessionStoreError, Writable,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

#[allow(
	dead_code,
	reason = "consumed by the session-bound engine in the next implementation step"
)]
pub(crate) const DOCUMENT_OWNER_ID: &str = "libaryon_documents";
pub(crate) const DOCUMENT_OWNER_SCHEMA_VERSION: u32 = 1;
pub(crate) const DOCUMENT_CONTENT_BUDGET: u64 = MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES;
const MAGIC: &[u8; 8] = b"LBYDOCS1";
const HEADER_BYTES: usize = MAGIC.len() + size_of::<u64>();
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocumentCheckpoint {
	pub(crate) document_id: DocumentId,
	pub(crate) bytes: Vec<u8>,
	pub(crate) dirty: bool,
	pub(crate) virtual_path: Option<String>,
	pub(crate) language_selection: LanguageSelection,
	pub(crate) file_stamp: Option<FileStamp>,
}

#[derive(Debug)]
pub enum SessionCheckpointError {
	NotSaved,
	EngineStopped,
	Refused(String),
	Store(SessionStoreError),
}

impl std::fmt::Display for SessionCheckpointError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NotSaved => formatter.write_str("engine is not bound to a saved session"),
			Self::EngineStopped => {
				formatter.write_str("engine stopped before checkpoint completed")
			}
			Self::Refused(reason) => formatter.write_str(reason),
			Self::Store(error) => error.fmt(formatter),
		}
	}
}

impl std::error::Error for SessionCheckpointError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Store(error) => Some(error),
			_ => None,
		}
	}
}

impl From<SessionStoreError> for SessionCheckpointError {
	fn from(error: SessionStoreError) -> Self {
		Self::Store(error)
	}
}

#[derive(Debug)]
pub enum SessionRestoreError {
	InitialFileConflict,
	Refused(String),
	Store(SessionStoreError),
}

impl std::fmt::Display for SessionRestoreError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::InitialFileConflict => formatter.write_str(
				"saved-session restoration cannot be combined with EngineConfig::initial_file",
			),
			Self::Refused(reason) => formatter.write_str(reason),
			Self::Store(error) => error.fmt(formatter),
		}
	}
}

impl std::error::Error for SessionRestoreError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Store(error) => Some(error),
			_ => None,
		}
	}
}

impl From<SessionStoreError> for SessionRestoreError {
	fn from(error: SessionStoreError) -> Self {
		Self::Store(error)
	}
}

pub(crate) fn restore_saved_session(
	registry: &crate::ecs::UastRegistry,
	language_registry: &LanguageRegistry,
	settings: &EngineSettings,
	saved: SavedSession<Writable>,
) -> Result<EditorSession, SessionRestoreError> {
	restore_saved_session_with_budget(
		registry,
		language_registry,
		settings,
		saved,
		DOCUMENT_CONTENT_BUDGET,
	)
}

fn restore_saved_session_with_budget(
	registry: &crate::ecs::UastRegistry,
	language_registry: &LanguageRegistry,
	settings: &EngineSettings,
	saved: SavedSession<Writable>,
	content_budget: u64,
) -> Result<EditorSession, SessionRestoreError> {
	let base_documents: Vec<DocumentIdentity> = saved.snapshot().documents().cloned().collect();
	let paths: BTreeMap<DocumentId, Option<std::path::PathBuf>> = base_documents
		.iter()
		.map(|identity| (identity.id(), identity.path().map(Path::to_path_buf)))
		.collect();
	let owner = OwnerId::new(DOCUMENT_OWNER_ID)?;
	let checkpoints =
		decode_document_owner(base_documents, saved.owner_payload(&owner), content_budget)?;

	let mut restored = Vec::with_capacity(checkpoints.len());
	let mut warnings = Vec::new();
	let mut aggregate = 0_u64;
	for checkpoint in checkpoints {
		let file_path = paths.get(&checkpoint.document_id).cloned().ok_or_else(|| {
			SessionRestoreError::Refused("restored document path is missing".to_owned())
		})?;
		if file_path.as_ref().is_some_and(|path| !path.is_absolute()) {
			return Err(SessionRestoreError::Refused(format!(
				"saved document {} has a relative path; durable file paths must be absolute lexical paths",
				checkpoint.document_id
			)));
		}

		let mut bytes = checkpoint.bytes;
		let mut dirty = checkpoint.dirty;
		let mut current_stamp = checkpoint.file_stamp.clone();
		if let Some(path) = file_path.as_deref() {
			let metadata = std::fs::metadata(path);
			let observed_stamp = metadata.as_ref().ok().map(FileStamp::from_metadata);
			if dirty {
				if observed_stamp.as_ref() != checkpoint.file_stamp.as_ref() {
					warnings.push(format!(
						"{} changed or became unavailable; restored unsaved checkpoint bytes",
						path.to_string_lossy()
					));
				}
			} else {
				if let Ok(metadata) = &metadata {
					let projected = aggregate.checked_add(metadata.len()).ok_or_else(|| {
						SessionRestoreError::Refused(
							"aggregate document length overflow".to_owned(),
						)
					})?;
					if projected > content_budget {
						return Err(SessionRestoreError::Refused(format!(
							"restored document content is {projected} bytes (limit is {})",
							content_budget
						)));
					}
				}
				let disk = std::fs::read(path);
				match disk {
					Ok(disk) => {
						if observed_stamp.as_ref() != checkpoint.file_stamp.as_ref() {
							warnings.push(format!(
								"{} changed since the checkpoint; loaded current disk bytes",
								path.to_string_lossy()
							));
						}
						bytes = disk;
						current_stamp = observed_stamp;
					}
					Err(error) => {
						dirty = true;
						current_stamp = observed_stamp;
						warnings.push(format!(
							"{} could not be read ({error}); recovered checkpoint bytes and marked the document dirty",
							path.to_string_lossy()
						));
					}
				}
			}
		}

		let byte_len = u64::try_from(bytes.len()).map_err(|_| {
			SessionRestoreError::Refused("restored document length exceeds u64".to_owned())
		})?;
		aggregate = aggregate.checked_add(byte_len).ok_or_else(|| {
			SessionRestoreError::Refused("aggregate document length overflow".to_owned())
		})?;
		if aggregate > content_budget {
			return Err(SessionRestoreError::Refused(format!(
				"restored document content is {aggregate} bytes (limit is {content_budget})"
			)));
		}

		let active_language_id = match &checkpoint.language_selection {
			LanguageSelection::Auto => file_path
				.as_deref()
				.and_then(Path::extension)
				.and_then(|extension| extension.to_str())
				.and_then(|extension| language_registry.resolve_supported_language_id(extension)),
			LanguageSelection::ExplicitPlaintext => None,
			LanguageSelection::Explicit(provider) => {
				let resolved = language_registry.resolve_supported_language_id(provider);
				if resolved.is_none() {
					warnings.push(format!(
						"document {} requested unavailable language provider {provider:?}; restored as plaintext",
						checkpoint.document_id
					));
				}
				resolved
			}
		};
		restored.push(RestoredDocument {
			document_id: checkpoint.document_id,
			bytes,
			file_path,
			virtual_path: checkpoint.virtual_path,
			file_stamp: current_stamp,
			language_selection: checkpoint.language_selection,
			active_language_id,
			dirty,
		});
	}

	let startup_status = (!warnings.is_empty()).then(|| {
		format!(
			"Saved-session restoration warnings: {}",
			warnings.join("; ")
		)
	});
	EditorSession::new_restored(
		registry,
		settings.theme_name.clone(),
		settings.minimap_enabled,
		settings.wrap_enabled,
		settings.line_numbers_enabled,
		settings.relative_line_numbers_enabled,
		settings.visible_whitespace,
		settings.reload_enabled,
		settings.save_durability,
		settings.ui_colors,
		settings.language_themes.clone(),
		saved,
		restored,
		startup_status,
	)
	.map_err(SessionRestoreError::Refused)
}

pub(crate) fn checkpoint_documents(
	registry: &crate::ecs::UastRegistry,
	session: &mut super::session::EditorSession,
	content_budget: u64,
) -> Result<(), SessionCheckpointError> {
	checkpoint_documents_with_commit(registry, session, content_budget, |saved| saved.commit())
}

fn durable_document_candidate<'a>(
	session: &super::session::EditorSession,
	buffer_id: crate::core::BufferId,
	buffer: &'a super::session::Buffer,
) -> Option<&'a super::session::TextBuffer> {
	(buffer.role == super::session::BufferRole::Document
		&& !session.snapshot_partners.contains_key(&buffer_id))
	.then(|| buffer.text())
	.flatten()
}

fn checkpoint_documents_with_commit(
	registry: &crate::ecs::UastRegistry,
	session: &mut super::session::EditorSession,
	content_budget: u64,
	commit: impl FnOnce(&mut SavedSession<Writable>) -> Result<(), SessionStoreError>,
) -> Result<(), SessionCheckpointError> {
	let saved = session
		.saved_documents
		.as_ref()
		.ok_or(SessionCheckpointError::NotSaved)?;
	let mut candidates = Vec::new();
	let mut aggregate = 0_u64;
	for (buffer_id, buffer) in session.buffers.iter() {
		let Some(document) = durable_document_candidate(session, buffer_id, buffer) else {
			continue;
		};
		if document.encrypted_backing.is_some() {
			return Err(SessionCheckpointError::Refused(
				"Session checkpoint refused: encrypted plaintext cannot enter the unencrypted session store"
					.to_owned(),
			));
		}
		let document_id = saved.document_id(buffer_id).ok_or_else(|| {
			SessionCheckpointError::Refused(format!(
				"ordinary buffer {buffer_id} has no durable document identity"
			))
		})?;
		let byte_len = registry.get_total_bytes(document.root_id);
		aggregate = aggregate.checked_add(byte_len).ok_or_else(|| {
			SessionCheckpointError::Refused("aggregate document length overflow".to_owned())
		})?;
		if aggregate > content_budget {
			return Err(SessionCheckpointError::Refused(format!(
				"Session checkpoint requires {aggregate} bytes of document content (limit is {content_budget})"
			)));
		}
		candidates.push((document_id, document));
	}

	let mut checkpoints = Vec::with_capacity(candidates.len());
	for (document_id, document) in candidates {
		let bytes = super::document::io::read_document_snapshot(
			registry,
			document.root_id,
			document.backing_path.as_deref(),
		)
		.map_err(SessionCheckpointError::Refused)?;
		checkpoints.push(DocumentCheckpoint {
			document_id,
			bytes,
			dirty: document.ledger.is_dirty(),
			virtual_path: document.virtual_path.clone(),
			language_selection: document.language_selection.clone(),
			file_stamp: document.file_stamp.clone(),
		});
	}
	let payload = encode_document_owner(&checkpoints, content_budget)?;
	let owner = crate::core::OwnerId::new(DOCUMENT_OWNER_ID)?;
	let saved = session
		.saved_documents
		.as_mut()
		.expect("saved state cannot disappear during ordered capture");
	saved
		.session
		.set_owner_payload(owner, DOCUMENT_OWNER_SCHEMA_VERSION, payload);
	commit(&mut saved.session).map_err(|error| {
		if matches!(error, SessionStoreError::PublicationUncertain) {
			SessionCheckpointError::Refused(
				"Saved-session publication is uncertain; later edits are not session-checkpointable through this engine. Keep the live engine open to copy them, then drop and reopen the saved session."
					.to_owned(),
			)
		} else {
			SessionCheckpointError::Store(error)
		}
	})?;
	Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum CheckpointFault {
	BeforeCurrent,
	AfterCurrent,
}

#[cfg(test)]
pub(crate) fn checkpoint_documents_with_fault(
	registry: &crate::ecs::UastRegistry,
	session: &mut super::session::EditorSession,
	content_budget: u64,
	fault: CheckpointFault,
) -> Result<(), SessionCheckpointError> {
	checkpoint_documents_with_commit(registry, session, content_budget, |saved| match fault {
		CheckpointFault::BeforeCurrent => saved.commit_before_current_failure_for_test(),
		CheckpointFault::AfterCurrent => saved.commit_after_current_failure_for_test(),
	})
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
	documents: Vec<ManifestDocument>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestDocument {
	document_id: u64,
	byte_len: u64,
	dirty: bool,
	virtual_path: Option<String>,
	language_selection: ManifestLanguageSelection,
	file_stamp: Option<ManifestFileStamp>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "provider", rename_all = "snake_case")]
enum ManifestLanguageSelection {
	Auto,
	ExplicitPlaintext,
	Explicit(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFileStamp {
	byte_len: u64,
	modified_unix_seconds: Option<u64>,
	modified_subsec_nanos: Option<u32>,
}

pub(crate) fn encode_document_owner(
	documents: &[DocumentCheckpoint],
	content_budget: u64,
) -> Result<Vec<u8>, SessionStoreError> {
	let mut ordered: Vec<&DocumentCheckpoint> = documents.iter().collect();
	ordered.sort_by_key(|document| document.document_id);
	for pair in ordered.windows(2) {
		if pair[0].document_id == pair[1].document_id {
			return corrupt("duplicate document id");
		}
	}

	let mut aggregate = 0_u64;
	let mut manifest_documents = Vec::with_capacity(ordered.len());
	for document in &ordered {
		let byte_len = u64::try_from(document.bytes.len())
			.map_err(|_| corruption("document length exceeds u64"))?;
		aggregate = aggregate
			.checked_add(byte_len)
			.ok_or_else(|| corruption("aggregate document length overflow"))?;
		if aggregate > content_budget {
			return corrupt(format!(
				"aggregate document content is {aggregate} bytes (limit is {content_budget})"
			));
		}
		manifest_documents.push(ManifestDocument {
			document_id: document.document_id.get(),
			byte_len,
			dirty: document.dirty,
			virtual_path: document.virtual_path.clone(),
			language_selection: (&document.language_selection).into(),
			file_stamp: document.file_stamp.as_ref().map(Into::into),
		});
	}

	let manifest = serde_json::to_vec(&Manifest {
		documents: manifest_documents,
	})
	.map_err(|error| corruption(format!("serialize document manifest: {error}")))?;
	let manifest_len = u64::try_from(manifest.len())
		.map_err(|_| corruption("document manifest length exceeds u64"))?;
	if manifest_len > MAX_MANIFEST_BYTES {
		return corrupt(format!(
			"document manifest is {manifest_len} bytes (limit is {MAX_MANIFEST_BYTES})"
		));
	}
	let content_len = usize::try_from(aggregate)
		.map_err(|_| corruption("aggregate document content exceeds address space"))?;
	let capacity = HEADER_BYTES
		.checked_add(manifest.len())
		.and_then(|length| length.checked_add(content_len))
		.ok_or_else(|| corruption("document payload length overflow"))?;
	let mut payload = Vec::new();
	payload
		.try_reserve_exact(capacity)
		.map_err(|_| corruption("document payload allocation failed"))?;
	payload.extend_from_slice(MAGIC);
	payload.extend_from_slice(&manifest_len.to_le_bytes());
	payload.extend_from_slice(&manifest);
	for document in ordered {
		payload.extend_from_slice(&document.bytes);
	}
	Ok(payload)
}

pub(crate) fn decode_document_owner(
	base_documents: impl IntoIterator<Item = DocumentIdentity>,
	payload: Option<(u32, &[u8])>,
	content_budget: u64,
) -> Result<Vec<DocumentCheckpoint>, SessionStoreError> {
	let base: BTreeSet<DocumentId> = base_documents
		.into_iter()
		.map(|document| document.id())
		.collect();
	let Some((schema_version, payload)) = payload else {
		return if base.is_empty() {
			Ok(Vec::new())
		} else {
			corrupt("nonempty document identity set has no libaryon document payload")
		};
	};
	if schema_version != DOCUMENT_OWNER_SCHEMA_VERSION {
		return corrupt(format!(
			"unsupported libaryon document owner schema {schema_version}"
		));
	}
	decode_payload(&base, payload, content_budget)
}

fn decode_payload(
	base: &BTreeSet<DocumentId>,
	payload: &[u8],
	content_budget: u64,
) -> Result<Vec<DocumentCheckpoint>, SessionStoreError> {
	if payload.len() < HEADER_BYTES || &payload[..MAGIC.len()] != MAGIC {
		return corrupt("invalid libaryon document payload magic");
	}
	let manifest_len = u64::from_le_bytes(
		payload[MAGIC.len()..HEADER_BYTES]
			.try_into()
			.expect("header length is fixed"),
	);
	if manifest_len > MAX_MANIFEST_BYTES {
		return corrupt(format!(
			"document manifest is {manifest_len} bytes (limit is {MAX_MANIFEST_BYTES})"
		));
	}
	let manifest_len = usize::try_from(manifest_len)
		.map_err(|_| corruption("document manifest exceeds address space"))?;
	let manifest_end = HEADER_BYTES
		.checked_add(manifest_len)
		.ok_or_else(|| corruption("document manifest extent overflow"))?;
	let manifest_bytes = payload
		.get(HEADER_BYTES..manifest_end)
		.ok_or_else(|| corruption("truncated document manifest"))?;
	let manifest: Manifest = serde_json::from_slice(manifest_bytes)
		.map_err(|error| corruption(format!("invalid document manifest: {error}")))?;

	let mut seen = BTreeSet::new();
	let mut previous = None;
	let mut aggregate = 0_u64;
	for document in &manifest.documents {
		let id = DocumentId::new(document.document_id)?;
		if previous.is_some_and(|previous| id <= previous) {
			return corrupt("document ids are duplicate or out of ascending order");
		}
		previous = Some(id);
		seen.insert(id);
		aggregate = aggregate
			.checked_add(document.byte_len)
			.ok_or_else(|| corruption("aggregate document length overflow"))?;
		if aggregate > content_budget {
			return corrupt(format!(
				"aggregate document content is {aggregate} bytes (limit is {content_budget})"
			));
		}
	}
	if &seen != base {
		return corrupt("document payload id set does not equal the base snapshot");
	}
	let content_len = usize::try_from(aggregate)
		.map_err(|_| corruption("aggregate document content exceeds address space"))?;
	let expected_end = manifest_end
		.checked_add(content_len)
		.ok_or_else(|| corruption("document content extent overflow"))?;
	if expected_end != payload.len() {
		return if expected_end < payload.len() {
			corrupt("trailing bytes after document content extents")
		} else {
			corrupt("missing bytes from document content extents")
		};
	}

	let mut offset = manifest_end;
	let mut checkpoints = Vec::with_capacity(manifest.documents.len());
	for document in manifest.documents {
		let id = DocumentId::new(document.document_id)?;
		let byte_len = usize::try_from(document.byte_len)
			.map_err(|_| corruption("document extent exceeds address space"))?;
		let end = offset
			.checked_add(byte_len)
			.ok_or_else(|| corruption("document extent overflow"))?;
		let bytes = payload
			.get(offset..end)
			.ok_or_else(|| corruption("missing document content extent"))?
			.to_vec();
		offset = end;
		let language_selection = decode_language_selection(document.language_selection)?;
		let file_stamp = document.file_stamp.map(decode_file_stamp).transpose()?;
		checkpoints.push(DocumentCheckpoint {
			document_id: id,
			bytes,
			dirty: document.dirty,
			virtual_path: document.virtual_path,
			language_selection,
			file_stamp,
		});
	}
	Ok(checkpoints)
}

impl From<&LanguageSelection> for ManifestLanguageSelection {
	fn from(selection: &LanguageSelection) -> Self {
		match selection {
			LanguageSelection::Auto => Self::Auto,
			LanguageSelection::ExplicitPlaintext => Self::ExplicitPlaintext,
			LanguageSelection::Explicit(provider) => Self::Explicit(provider.clone()),
		}
	}
}

fn decode_language_selection(
	selection: ManifestLanguageSelection,
) -> Result<LanguageSelection, SessionStoreError> {
	match selection {
		ManifestLanguageSelection::Auto => Ok(LanguageSelection::Auto),
		ManifestLanguageSelection::ExplicitPlaintext => Ok(LanguageSelection::ExplicitPlaintext),
		ManifestLanguageSelection::Explicit(provider) if provider.is_empty() => {
			corrupt("explicit language provider id is empty")
		}
		ManifestLanguageSelection::Explicit(provider) => Ok(LanguageSelection::Explicit(provider)),
	}
}

impl From<&FileStamp> for ManifestFileStamp {
	fn from(stamp: &FileStamp) -> Self {
		let modified = stamp
			.modified
			.and_then(|time| time.duration_since(UNIX_EPOCH).ok());
		Self {
			byte_len: stamp.len,
			modified_unix_seconds: modified.map(|duration| duration.as_secs()),
			modified_subsec_nanos: modified.map(|duration| duration.subsec_nanos()),
		}
	}
}

fn decode_file_stamp(stamp: ManifestFileStamp) -> Result<FileStamp, SessionStoreError> {
	let modified = match (stamp.modified_unix_seconds, stamp.modified_subsec_nanos) {
		(None, None) => None,
		(Some(seconds), Some(nanos)) if nanos < 1_000_000_000 => {
			Some(UNIX_EPOCH + Duration::new(seconds, nanos))
		}
		(Some(_), Some(_)) => return corrupt("file stamp nanoseconds are out of range"),
		_ => return corrupt("file stamp time fields are not paired"),
	};
	Ok(FileStamp {
		len: stamp.byte_len,
		modified,
	})
}

fn corruption(reason: impl Into<String>) -> SessionStoreError {
	SessionStoreError::Corrupt(reason.into())
}

fn corrupt<T>(reason: impl Into<String>) -> Result<T, SessionStoreError> {
	Err(corruption(reason))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::SessionStore;
	use crate::engine::UiColorOverrides;
	use crate::engine::session::{EditorSession, LayoutNode};
	use crate::engine::settings::SaveDurability;
	use crate::engine::undo::HistoryDirection;
	use std::collections::BTreeMap;
	use std::path::PathBuf;
	fn identity(id: u64) -> DocumentIdentity {
		DocumentIdentity::new(DocumentId::new(id).unwrap(), None)
	}

	fn checkpoint(id: u64, bytes: Vec<u8>) -> DocumentCheckpoint {
		let even = id.is_multiple_of(2);
		DocumentCheckpoint {
			document_id: DocumentId::new(id).unwrap(),
			bytes,
			dirty: even,
			virtual_path: even.then(|| format!("untitled-{id}.bin")),
			language_selection: if even {
				LanguageSelection::Explicit("binary-test".to_owned())
			} else {
				LanguageSelection::ExplicitPlaintext
			},
			file_stamp: None,
		}
	}

	fn raw_payload(manifest: &[u8], content: &[u8]) -> Vec<u8> {
		let mut payload = Vec::new();
		payload.extend_from_slice(MAGIC);
		payload.extend_from_slice(&(manifest.len() as u64).to_le_bytes());
		payload.extend_from_slice(manifest);
		payload.extend_from_slice(content);
		payload
	}

	fn manifest_document(id: u64, byte_len: u64) -> serde_json::Value {
		serde_json::json!({
			"document_id": id,
			"byte_len": byte_len,
			"dirty": false,
			"virtual_path": null,
			"language_selection": { "kind": "auto" },
			"file_stamp": null,
		})
	}

	fn saved_editor(registry: &crate::ecs::UastRegistry) -> (EditorSession, PathBuf) {
		let root = std::env::temp_dir().join(format!(
			"libaryon-document-checkpoint-{}-{}",
			std::process::id(),
			rand::random::<u64>()
		));
		let saved = crate::core::SessionStore::at(root.clone())
			.create()
			.expect("create saved session");
		let mut editor = EditorSession::new(
			registry,
			"onedark".to_owned(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::Atomic,
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		editor
			.attach_new_saved_session(saved)
			.expect("attach saved session");
		(editor, root)
	}

	fn temp_root(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"libaryon-{name}-{}-{}",
			std::process::id(),
			rand::random::<u64>()
		))
	}

	fn stamp(path: &Path) -> FileStamp {
		FileStamp::from_metadata(&std::fs::metadata(path).expect("file metadata"))
	}

	fn checkpoint_for(
		id: DocumentId,
		bytes: &[u8],
		dirty: bool,
		stamp: Option<FileStamp>,
	) -> DocumentCheckpoint {
		DocumentCheckpoint {
			document_id: id,
			bytes: bytes.to_vec(),
			dirty,
			virtual_path: None,
			language_selection: LanguageSelection::Auto,
			file_stamp: stamp,
		}
	}

	#[test]
	fn product_0060_codec_round_trips_arbitrary_bytes_and_orders_ids() {
		let documents = vec![
			checkpoint(9, vec![b'"', b'\\', 0, 0xff]),
			checkpoint(2, vec![0xf0, 0x28, 0x8c, 0x28, 0]),
		];
		let payload = encode_document_owner(&documents, 64).expect("encode");
		let decoded = decode_document_owner(
			[identity(2), identity(9)],
			Some((DOCUMENT_OWNER_SCHEMA_VERSION, &payload)),
			64,
		)
		.expect("decode");
		assert_eq!(decoded, vec![documents[1].clone(), documents[0].clone()]);
	}

	#[test]
	fn product_0060_codec_budget_accepts_boundary_and_refuses_next_byte() {
		let at_boundary =
			encode_document_owner(&[checkpoint(1, vec![0; 8])], 8).expect("boundary accepted");
		assert!(
			decode_payload(
				&BTreeSet::from([DocumentId::new(1).unwrap()]),
				&at_boundary,
				8
			)
			.is_ok()
		);
		assert!(encode_document_owner(&[checkpoint(1, vec![0; 9])], 8).is_err());
		assert_eq!(DOCUMENT_CONTENT_BUDGET, 256 * 1024 * 1024);
	}

	#[test]
	fn product_0060_codec_refuses_malformed_extents_and_id_sets() {
		let payload = encode_document_owner(&[checkpoint(1, b"abc".to_vec())], 16).unwrap();
		let base = BTreeSet::from([DocumentId::new(1).unwrap()]);
		let mut bad_magic = payload.clone();
		bad_magic[0] ^= 1;
		assert!(decode_payload(&base, &bad_magic, 16).is_err());
		assert!(decode_payload(&base, &payload[..HEADER_BYTES + 1], 16).is_err());
		let mut trailing = payload.clone();
		trailing.push(0);
		assert!(decode_payload(&base, &trailing, 16).is_err());
		assert!(decode_payload(&base, &payload[..payload.len() - 1], 16).is_err());
		assert!(
			decode_payload(&BTreeSet::from([DocumentId::new(2).unwrap()]), &payload, 16).is_err()
		);

		let duplicate_manifest = serde_json::to_vec(&serde_json::json!({
			"documents": [manifest_document(1, 0), manifest_document(1, 0)],
		}))
		.unwrap();
		assert!(decode_payload(&base, &raw_payload(&duplicate_manifest, b""), 16).is_err());

		let out_of_order_manifest = serde_json::to_vec(&serde_json::json!({
			"documents": [manifest_document(2, 0), manifest_document(1, 0)],
		}))
		.unwrap();
		let two_ids = BTreeSet::from([DocumentId::new(1).unwrap(), DocumentId::new(2).unwrap()]);
		assert!(decode_payload(&two_ids, &raw_payload(&out_of_order_manifest, b""), 16).is_err());

		let overflow_manifest = serde_json::to_vec(&serde_json::json!({
			"documents": [manifest_document(1, u64::MAX), manifest_document(2, 1)],
		}))
		.unwrap();
		assert!(
			decode_payload(&two_ids, &raw_payload(&overflow_manifest, b""), u64::MAX,).is_err()
		);

		let unknown_field = br#"{"documents":[],"surprise":true}"#;
		assert!(decode_payload(&BTreeSet::new(), &raw_payload(unknown_field, b""), 0).is_err());
		assert!(decode_payload(&BTreeSet::new(), &raw_payload(&[0xff], b""), 0).is_err());
	}

	#[test]
	fn product_0060_empty_predecessor_and_unknown_schema_are_distinct() {
		assert!(decode_document_owner([], None, 0).unwrap().is_empty());
		assert!(decode_document_owner([identity(1)], None, 0).is_err());
		assert!(decode_document_owner([], Some((2, b"opaque")), 0).is_err());
	}

	#[test]
	fn product_0060_capture_publishes_exact_bytes_and_refuses_atomically() {
		let registry = crate::ecs::UastRegistry::new(64);
		let (mut editor, root) = saved_editor(&registry);
		let buffer_id = editor
			.layout
			.find_pane(editor.focused_pane_id)
			.unwrap()
			.active_tab()
			.buffer_id;
		let document = editor.buffers.get(buffer_id).unwrap().text().unwrap();
		let leaf = registry.get_first_child(document.root_id).unwrap();
		let exact = vec![0, b'"', b'\\', 0xff];
		registry.replace_virtual_document_data(document.root_id, leaf, exact.clone());

		assert!(checkpoint_documents(&registry, &mut editor, 3).is_err());
		let saved = editor.saved_documents.as_ref().unwrap();
		let owner = crate::core::OwnerId::new(DOCUMENT_OWNER_ID).unwrap();
		assert_eq!(saved.session.owner_payload(&owner), None);

		checkpoint_documents(&registry, &mut editor, 4).expect("checkpoint exact bytes");
		let saved = editor.saved_documents.as_ref().unwrap();
		let (schema, payload) = saved.session.owner_payload(&owner).unwrap();
		let committed_payload = payload.to_vec();
		let decoded = decode_document_owner(
			saved.session.snapshot().documents().cloned(),
			Some((schema, &committed_payload)),
			4,
		)
		.expect("decode committed capture");
		assert_eq!(decoded.len(), 1);
		assert_eq!(decoded[0].bytes, exact);

		editor
			.buffers
			.get_mut(buffer_id)
			.unwrap()
			.text_mut()
			.unwrap()
			.encrypted_backing = Some(PathBuf::from("secret.age"));
		let error = checkpoint_documents(&registry, &mut editor, 4).unwrap_err();
		assert!(error.to_string().contains("encrypted plaintext"));
		let preserved = editor
			.saved_documents
			.as_ref()
			.unwrap()
			.session
			.owner_payload(&owner)
			.unwrap()
			.1
			.to_vec();
		assert_eq!(preserved, committed_payload);
		drop(editor);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn product_0060_checkpoint_persists_only_positive_document_candidates() {
		use crate::engine::session::{BufferContent, SnapshotProvenance, TextBuffer};

		let registry = crate::ecs::UastRegistry::new(256);
		let (mut editor, root) = saved_editor(&registry);
		let ordinary_scratch = editor
			.layout
			.find_pane(editor.focused_pane_id)
			.unwrap()
			.active_tab()
			.buffer_id;

		let file_path = root.with_extension("ordinary.txt");
		std::fs::write(&file_path, b"ordinary file").unwrap();
		let (mut file_document, file_leaf) =
			TextBuffer::new_from_bytes(&registry, b"ordinary file".to_vec()).unwrap();
		file_document.file_path = Some(file_path.clone());
		file_document.file_stamp = Some(stamp(&file_path));
		let ordinary_file = editor.buffers.insert(BufferContent::Text(file_document));
		editor
			.saved_documents
			.as_mut()
			.unwrap()
			.mint(ordinary_file, Some(&file_path))
			.unwrap();
		let second_view = editor
			.create_text_view_for_buffer(&registry, ordinary_file, 80, 24)
			.unwrap();
		assert_eq!(second_view.buffer_id, ordinary_file);
		assert_eq!(
			registry.get_first_child(
				editor
					.buffers
					.get(ordinary_file)
					.unwrap()
					.text()
					.unwrap()
					.root_id
			),
			Some(file_leaf)
		);

		let (output, _) = editor
			.ensure_output_panel(&registry, "derived-output")
			.unwrap();
		let (snapshot, _) =
			TextBuffer::new_from_bytes(&registry, b"git snapshot".to_vec()).unwrap();
		let snapshot_id = editor.buffers.insert(BufferContent::Text(snapshot));
		editor.snapshot_partners.insert(
			snapshot_id,
			SnapshotProvenance::GitRevision {
				repo_root: root.clone(),
				rel_path: "ordinary.txt".to_owned(),
				revision: "HEAD".to_owned(),
				commit_oid: "a".repeat(40),
				blob_oid: "b".repeat(40),
			},
		);
		let (browser, _) = crate::engine::file_browser::create_file_browser_buffer(
			&registry,
			std::env::temp_dir(),
		)
		.unwrap();
		let browser_id = editor.buffers.insert(BufferContent::FileBrowser(browser));
		crate::engine::markdown_image_preview::open_markdown_image_preview(
			&registry,
			&mut editor,
			false,
		)
		.unwrap();
		crate::engine::uast_inspector::open_uast_inspector(&registry, &mut editor, None).unwrap();
		let host_view = editor.create_host_panel_view(
			crate::engine::HostPanelId::new(91),
			crate::engine::HostPanelDescriptor {
				kind: "test".to_owned(),
				title: "host".to_owned(),
			},
			80,
			24,
		);

		for excluded in [output, snapshot_id, browser_id, host_view.buffer_id] {
			assert!(
				durable_document_candidate(
					&editor,
					excluded,
					editor.buffers.get(excluded).unwrap()
				)
				.is_none()
			);
		}
		assert!(
			editor
				.buffers
				.iter()
				.filter(|(id, buffer)| durable_document_candidate(&editor, *id, buffer).is_some())
				.count() >= 2
		);
		checkpoint_documents(&registry, &mut editor, 1024).unwrap();
		let saved = editor.saved_documents.as_ref().unwrap();
		let owner = OwnerId::new(DOCUMENT_OWNER_ID).unwrap();
		let (schema, payload) = saved.session.owner_payload(&owner).unwrap();
		let decoded = decode_document_owner(
			saved.session.snapshot().documents().cloned(),
			Some((schema, payload)),
			1024,
		)
		.unwrap();
		assert_eq!(decoded.len(), 2);
		let durable_ids = decoded
			.iter()
			.map(|document| document.document_id)
			.collect::<BTreeSet<_>>();
		let bindings = editor.saved_documents.as_ref().unwrap();
		assert_eq!(
			durable_ids,
			BTreeSet::from([
				bindings.document_id(ordinary_scratch).unwrap(),
				bindings.document_id(ordinary_file).unwrap(),
			])
		);
		drop(editor);
		let _ = std::fs::remove_file(file_path);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn product_0060_restores_exact_dirty_bytes_and_id_order_without_undo_history() {
		let root = temp_root("document-restore-order");
		let store = SessionStore::at(root.clone());
		let mut saved = store.create().expect("create session");
		let first = saved.mint_document(None).expect("first id");
		let removed = saved.mint_document(None).expect("middle id");
		let last = saved.mint_document(None).expect("last id");
		saved.remove_document(removed).expect("remove middle id");
		let exact_first = vec![0, 0xff, b'\n'];
		let exact_last = vec![b'"', b'\\', 0xf8];
		let payload = encode_document_owner(
			&[
				checkpoint_for(last, &exact_last, true, None),
				checkpoint_for(first, &exact_first, true, None),
			],
			64,
		)
		.expect("owner payload");
		saved.set_owner_payload(
			OwnerId::new(DOCUMENT_OWNER_ID).unwrap(),
			DOCUMENT_OWNER_SCHEMA_VERSION,
			payload,
		);
		saved.commit().expect("commit image");

		let registry = crate::ecs::UastRegistry::new(64);
		let session = restore_saved_session(
			&registry,
			&crate::engine::host::build_language_registry(),
			&EngineSettings::default(),
			saved,
		)
		.expect("restore session");
		let LayoutNode::Leaf(leaf) = &session.layout else {
			panic!("fallback is one pane");
		};
		let saved_state = session.saved_documents.as_ref().unwrap();
		let ids = leaf
			.pane
			.tabs
			.iter()
			.map(|view| saved_state.document_id(view.buffer_id).unwrap())
			.collect::<Vec<_>>();
		assert_eq!(ids, vec![first, last]);
		assert_eq!(leaf.pane.active_tab_index, 0);
		for (id, expected) in [(first, exact_first), (last, exact_last)] {
			let buffer_id = saved_state.buffer_id(id).unwrap();
			let document = session.buffers.get(buffer_id).unwrap().text().unwrap();
			assert_eq!(
				super::super::document::io::read_document_snapshot(
					&registry,
					document.root_id,
					document.backing_path.as_deref(),
				)
				.unwrap(),
				expected
			);
			assert!(document.ledger.is_dirty());
			assert!(document.ledger.preview(HistoryDirection::Undo).is_none());
		}
		drop(session);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn product_0060_file_restore_matrix_reports_every_divergence() {
		let root = temp_root("document-restore-matrix");
		std::fs::create_dir_all(&root).unwrap();
		let unchanged = root.join("unchanged.txt");
		let changed_clean = root.join("changed-clean.txt");
		let changed_dirty = root.join("changed-dirty.txt");
		let missing_clean = root.join("missing-clean.txt");
		for (path, bytes) in [
			(&unchanged, b"same".as_slice()),
			(&changed_clean, b"clean-old".as_slice()),
			(&changed_dirty, b"dirty-disk-old".as_slice()),
			(&missing_clean, b"missing-old".as_slice()),
		] {
			std::fs::write(path, bytes).unwrap();
		}
		let store_root = root.join("sessions");
		let store = SessionStore::at(store_root);
		let mut saved = store.create().unwrap();
		let mut documents = Vec::new();
		for (path, bytes, dirty) in [
			(&unchanged, b"same".as_slice(), false),
			(&changed_clean, b"clean-old".as_slice(), false),
			(&changed_dirty, b"dirty-checkpoint".as_slice(), true),
			(&missing_clean, b"missing-old".as_slice(), false),
		] {
			let id = saved.mint_document(Some(path.clone())).unwrap();
			documents.push(checkpoint_for(id, bytes, dirty, Some(stamp(path))));
		}
		let payload = encode_document_owner(&documents, 1024).unwrap();
		saved.set_owner_payload(
			OwnerId::new(DOCUMENT_OWNER_ID).unwrap(),
			DOCUMENT_OWNER_SCHEMA_VERSION,
			payload,
		);
		saved.commit().unwrap();
		std::fs::write(&changed_clean, b"clean-current-longer").unwrap();
		std::fs::write(&changed_dirty, b"dirty-disk-current-longer").unwrap();
		std::fs::remove_file(&missing_clean).unwrap();

		let registry = crate::ecs::UastRegistry::new(128);
		let session = restore_saved_session(
			&registry,
			&crate::engine::host::build_language_registry(),
			&EngineSettings::default(),
			saved,
		)
		.unwrap();
		let saved_state = session.saved_documents.as_ref().unwrap();
		let expected = [
			(documents[0].document_id, b"same".as_slice(), false),
			(
				documents[1].document_id,
				b"clean-current-longer".as_slice(),
				false,
			),
			(
				documents[2].document_id,
				b"dirty-checkpoint".as_slice(),
				true,
			),
			(documents[3].document_id, b"missing-old".as_slice(), true),
		];
		for (id, bytes, dirty) in expected {
			let document = session
				.buffers
				.get(saved_state.buffer_id(id).unwrap())
				.unwrap()
				.text()
				.unwrap();
			assert_eq!(
				super::super::document::io::read_document_snapshot(
					&registry,
					document.root_id,
					document.backing_path.as_deref()
				)
				.unwrap(),
				bytes
			);
			assert_eq!(document.ledger.is_dirty(), dirty);
		}
		let warning = session.ui.status_message.as_deref().unwrap();
		assert!(warning.contains("changed-clean.txt"));
		assert!(warning.contains("changed-dirty.txt"));
		assert!(warning.contains("missing-clean.txt"));
		assert!(!warning.contains("unchanged.txt"));
		drop(session);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn product_0060_restore_refuses_a_clean_file_that_grew_past_the_budget() {
		let root = temp_root("document-restore-budget");
		std::fs::create_dir_all(&root).unwrap();
		let path = root.join("grew.txt");
		std::fs::write(&path, b"1234").unwrap();
		let store = SessionStore::at(root.join("sessions"));
		let mut saved = store.create().unwrap();
		let id = saved.mint_document(Some(path.clone())).unwrap();
		let payload =
			encode_document_owner(&[checkpoint_for(id, b"1234", false, Some(stamp(&path)))], 8)
				.unwrap();
		saved.set_owner_payload(
			OwnerId::new(DOCUMENT_OWNER_ID).unwrap(),
			DOCUMENT_OWNER_SCHEMA_VERSION,
			payload,
		);
		saved.commit().unwrap();
		std::fs::write(&path, b"123456789").unwrap();
		let registry = crate::ecs::UastRegistry::new(32);
		let error = restore_saved_session_with_budget(
			&registry,
			&crate::engine::host::build_language_registry(),
			&EngineSettings::default(),
			saved,
			8,
		)
		.err()
		.expect("growth is a structural capacity refusal");
		assert!(error.to_string().contains("9 bytes (limit is 8)"));
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn product_0060_language_selection_provenance_survives_unavailable_providers() {
		let root = temp_root("document-language-restore");
		std::fs::create_dir_all(&root).unwrap();
		let rust_path = root.join("automatic.rs");
		std::fs::write(&rust_path, b"fn automatic() {}\n").unwrap();
		let store = SessionStore::at(root.join("sessions"));
		let mut saved = store.create().unwrap();
		let automatic = saved.mint_document(Some(rust_path.clone())).unwrap();
		let explicit = saved.mint_document(None).unwrap();
		let plaintext = saved.mint_document(None).unwrap();
		let unavailable = saved.mint_document(None).unwrap();
		let documents = vec![
			DocumentCheckpoint {
				document_id: automatic,
				bytes: b"fn automatic() {}\n".to_vec(),
				dirty: false,
				virtual_path: None,
				language_selection: LanguageSelection::Auto,
				file_stamp: Some(stamp(&rust_path)),
			},
			DocumentCheckpoint {
				document_id: explicit,
				bytes: b"fn explicit() {}\n".to_vec(),
				dirty: true,
				virtual_path: Some("explicit.data".to_owned()),
				language_selection: LanguageSelection::Explicit("rust".to_owned()),
				file_stamp: None,
			},
			DocumentCheckpoint {
				document_id: plaintext,
				bytes: b"fn plaintext() {}\n".to_vec(),
				dirty: true,
				virtual_path: Some("plaintext.rs".to_owned()),
				language_selection: LanguageSelection::ExplicitPlaintext,
				file_stamp: None,
			},
			DocumentCheckpoint {
				document_id: unavailable,
				bytes: b"future provider bytes\n".to_vec(),
				dirty: true,
				virtual_path: Some("future.lang".to_owned()),
				language_selection: LanguageSelection::Explicit("future-provider".to_owned()),
				file_stamp: None,
			},
		];
		let payload = encode_document_owner(&documents, 1024).unwrap();
		saved.set_owner_payload(
			OwnerId::new(DOCUMENT_OWNER_ID).unwrap(),
			DOCUMENT_OWNER_SCHEMA_VERSION,
			payload,
		);
		saved.commit().unwrap();

		let registry = crate::ecs::UastRegistry::new(128);
		let mut session = restore_saved_session(
			&registry,
			&crate::engine::host::build_language_registry(),
			&EngineSettings::default(),
			saved,
		)
		.unwrap();
		let state = session.saved_documents.as_ref().unwrap();
		for (id, selection, active) in [
			(automatic, LanguageSelection::Auto, Some("rust")),
			(
				explicit,
				LanguageSelection::Explicit("rust".to_owned()),
				Some("rust"),
			),
			(plaintext, LanguageSelection::ExplicitPlaintext, None),
			(
				unavailable,
				LanguageSelection::Explicit("future-provider".to_owned()),
				None,
			),
		] {
			let document = session
				.buffers
				.get(state.buffer_id(id).unwrap())
				.unwrap()
				.text()
				.unwrap();
			assert_eq!(document.language_selection, selection);
			assert_eq!(document.active_language_id.map(|id| id.as_str()), active);
		}
		assert!(
			session
				.ui
				.status_message
				.as_deref()
				.unwrap()
				.contains("future-provider")
		);
		checkpoint_documents(&registry, &mut session, 1024).unwrap();
		let state = session.saved_documents.as_ref().unwrap();
		let owner = OwnerId::new(DOCUMENT_OWNER_ID).unwrap();
		let (schema, payload) = state.session.owner_payload(&owner).unwrap();
		let decoded = decode_document_owner(
			state.session.snapshot().documents().cloned(),
			Some((schema, payload)),
			1024,
		)
		.unwrap();
		assert_eq!(
			decoded
				.iter()
				.find(|document| document.document_id == unavailable)
				.unwrap()
				.language_selection,
			LanguageSelection::Explicit("future-provider".to_owned())
		);
		drop(session);
		let _ = std::fs::remove_dir_all(root);
	}
}
