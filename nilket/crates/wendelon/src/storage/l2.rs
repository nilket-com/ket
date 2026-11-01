//! L2 object storage interface (ADR 006 — Tiered Storage).
//!
//! The Background Sweeper streams sealed L1 slabs out to a cold
//! tier so NVMe can be reclaimed for fresh writes. This file owns
//! the trait the sweeper speaks against ([`ObjectStore`]) and a
//! local-filesystem implementation ([`LocalFsObjectStore`]) that
//! is good enough for bring-up, tests, and anyone running the
//! engine on a shared NFS / CephFS pool.
//!
//! ## Minimal surface by design
//!
//! The seam stays deliberately small, but W7 already needs a bit
//! more than blind whole-object reads. In addition to `put` /
//! `get` / `delete`, callers can now `head` objects, list them by
//! prefix, and issue byte-range reads. Multipart upload and richer
//! retention workflows are still deferred to follow-up work once
//! the replicated lifecycle is carried end-to-end.
//!
//! ## Why not the AWS SDK
//!
//! The AWS SDK pulls a small forest of dependencies and wants a
//! Tokio runtime handle at construction. Keeping the seam at a
//! tiny trait means remote backends can land behind the same call
//! sites with zero churn to the sweeper. The remote backend here
//! is built on OpenDAL so Wendelon can point the same tiering path
//! at RustFS in the pilot, or pivot later to S3-compatible, B2,
//! or GCS targets without rewriting storage call sites.
//!
//! ## URI shape
//!
//! Callers store whatever `put` returns and feed it back into
//! `get` / `delete`. The URI format is implementation-private —
//! `LocalFsObjectStore` uses `file://<absolute path>` so a
//! deployment that relocates the L2 root sees the bad paths fail
//! loudly rather than silently sliding into "some other file".

use crate::storage::{AsyncFileOps, BlockingFileOps, OpenFlags, write_file};
use async_trait::async_trait;
use bytes::Bytes;
use opendal::layers::RetryLayer;
use opendal::services::{B2, Gcs, S3};
use opendal::{ErrorKind as OpendalErrorKind, Operator};
use std::collections::{BTreeMap, VecDeque};
#[cfg(test)]
use std::fs;
use std::io::{Error, ErrorKind};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Metadata about an object stored in L2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMetadata {
	pub key: String,
	pub uri: String,
	pub size_bytes: u64,
}

/// The L2 storage seam.
///
/// Every method is `async` so remote HTTP object stores fit without
/// re-typing the trait. Implementations must be `Send + Sync` —
/// the sweeper clones them across tokio tasks.
#[async_trait]
pub trait ObjectStore: Send + Sync {
	/// Upload `bytes` under `key` (typically `slab_<hex>.wnd`) and
	/// return the URI that later `get` / `delete` calls can use to
	/// address it. Implementations should treat `put` as
	/// overwriting — the sweeper retries uploads, and the payload
	/// hash is the durability oracle.
	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String>;

	/// Resolve the final object URI before upload so callers can persist
	/// an intent before an upload may become externally visible.
	fn planned_upload_uri(&self, _key: &str) -> std::io::Result<String> {
		Err(Error::new(
			ErrorKind::Unsupported,
			"object store cannot determine upload URI",
		))
	}

	/// Fetch metadata for the object previously uploaded to `uri`.
	/// The returned `key` is always relative to the configured L2
	/// root / prefix so callers can safely compare it against local
	/// sweeper policy.
	async fn head(&self, uri: &str) -> std::io::Result<ObjectMetadata>;

	/// Fetch a half-open byte range `[start, end)` from the object
	/// previously uploaded to `uri`. Callers must supply a valid
	/// range; implementations reject inverted or oversized spans.
	async fn get_range(&self, uri: &str, range: Range<u64>) -> std::io::Result<Bytes>;

	/// Fetch the full object previously uploaded to `uri` as shared
	/// bytes so callers can cheaply fan the payload out to both the
	/// hot-read path and a rehydration install without cloning a
	/// second slab-sized buffer.
	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes>;

	/// Fetch the full object previously uploaded to `uri`. The
	/// sweeper uses this for its post-upload hash verification;
	/// the read path uses it for L1-evicted lookups.
	async fn get(&self, uri: &str) -> std::io::Result<Vec<u8>> {
		Ok(self.get_bytes(uri).await?.to_vec())
	}

	/// List objects whose keys start with `prefix`, recursively.
	/// Returned entries are relative to the configured store root.
	async fn list(&self, prefix: &str) -> std::io::Result<Vec<ObjectMetadata>>;

	/// Remove the object previously uploaded to `uri`. Not on the
	/// V1 sweeper's hot path (the L1 unlink is the primary GC
	/// event) but needed for test teardown and future retention
	/// workflows.
	async fn delete(&self, uri: &str) -> std::io::Result<()>;
}

/// Configured L2 backend for a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectStoreConfig {
	LocalFs { root: PathBuf },
	Remote(RemoteObjectStoreConfig),
}

impl ObjectStoreConfig {
	pub(crate) fn build_with_owner(
		&self,
		owner: crate::storage::ownership::DataDirectoryOwner,
	) -> std::io::Result<Arc<dyn ObjectStore>> {
		match self {
			Self::LocalFs { root } => Ok(Arc::new(LocalFsObjectStore::new_with_file_ops(
				root.clone(),
				Arc::new(BlockingFileOps::with_owner(owner)),
			)?)),
			Self::Remote(config) => Ok(Arc::new(RemoteObjectStore::new(config.clone())?)),
		}
	}
	pub fn build(&self) -> std::io::Result<Arc<dyn ObjectStore>> {
		match self {
			Self::LocalFs { root } => Ok(Arc::new(LocalFsObjectStore::new(root.clone())?)),
			Self::Remote(config) => Ok(Arc::new(RemoteObjectStore::new(config.clone())?)),
		}
	}
}

/// OpenDAL-backed remote-object-store config.
///
/// Persisted URIs use the provider-neutral `l2://bucket/prefix/key`
/// family so tier metadata does not hard-code a specific object-store
/// protocol into the Raft state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteObjectStoreConfig {
	pub bucket: String,
	/// Optional prefix inside the bucket.
	pub root: String,
	pub provider: RemoteObjectStoreProvider,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteObjectStoreProvider {
	S3Compatible(S3CompatibleProviderConfig),
	BackblazeB2(BackblazeB2ProviderConfig),
	GoogleCloudStorage(GoogleCloudStorageProviderConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3CompatibleProviderConfig {
	pub endpoint: String,
	pub region: String,
	pub access_key_id: Option<String>,
	pub secret_access_key: Option<String>,
	pub session_token: Option<String>,
	/// Allows unsigned requests for local harnesses or anonymous
	/// compatible stores.
	pub allow_anonymous: bool,
	pub virtual_host_style: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackblazeB2ProviderConfig {
	pub bucket_id: String,
	pub application_key_id: String,
	pub application_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoogleCloudStorageProviderConfig {
	pub endpoint: Option<String>,
	pub credential: Option<String>,
	pub credential_path: Option<String>,
	pub service_account: Option<String>,
	pub token: Option<String>,
	pub allow_anonymous: bool,
}

impl RemoteObjectStoreConfig {
	fn validate(&self) -> std::io::Result<()> {
		if self.bucket.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"remote object store bucket must not be empty",
			));
		}
		self.provider.validate()
	}
}

impl RemoteObjectStoreProvider {
	fn validate(&self) -> std::io::Result<()> {
		match self {
			Self::S3Compatible(provider) => {
				if provider.endpoint.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote S3-compatible endpoint must not be empty",
					));
				}
				if provider.region.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote S3-compatible region must not be empty",
					));
				}
				let has_access_key = provider.access_key_id.is_some();
				let has_secret = provider.secret_access_key.is_some();
				if has_access_key != has_secret {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote S3-compatible credentials must provide both access_key_id and secret_access_key",
					));
				}
			}
			Self::BackblazeB2(provider) => {
				if provider.bucket_id.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote B2 bucket_id must not be empty",
					));
				}
				if provider.application_key_id.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote B2 application_key_id must not be empty",
					));
				}
				if provider.application_key.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote B2 application_key must not be empty",
					));
				}
			}
			Self::GoogleCloudStorage(provider) => {
				if matches!(provider.endpoint.as_deref(), Some(endpoint) if endpoint.trim().is_empty())
				{
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote GCS endpoint must not be empty when set",
					));
				}
				if provider.credential.is_some() && provider.credential_path.is_some() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"remote GCS config may set either credential or credential_path, not both",
					));
				}
			}
		}
		Ok(())
	}
}

fn validate_object_key(key: &str) -> std::io::Result<()> {
	if key.is_empty() || Path::new(key).is_absolute() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("rejecting unsafe object key: {key}"),
		));
	}
	for component in Path::new(key).components() {
		match component {
			Component::CurDir | Component::Normal(_) => {}
			_ => {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("rejecting unsafe object key: {key}"),
				));
			}
		}
	}
	Ok(())
}

fn validate_object_prefix(prefix: &str) -> std::io::Result<()> {
	if Path::new(prefix).is_absolute() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("rejecting unsafe object prefix: {prefix}"),
		));
	}
	for component in Path::new(prefix).components() {
		match component {
			Component::CurDir | Component::Normal(_) => {}
			_ => {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!("rejecting unsafe object prefix: {prefix}"),
				));
			}
		}
	}
	Ok(())
}

fn checked_range_len(range: &Range<u64>) -> std::io::Result<usize> {
	let len = range.end.checked_sub(range.start).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("invalid byte range {}..{}", range.start, range.end),
		)
	})?;
	usize::try_from(len).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("byte range {}..{} is too large", range.start, range.end),
		)
	})
}

const REMOTE_OBJECT_URI_SCHEME: &str = "l2://";

fn normalize_prefix(prefix: &str) -> String {
	prefix.trim_matches('/').to_string()
}

#[derive(Debug)]
struct RemoteOperationError {
	context: &'static str,
	cause: opendal::Error,
}
impl std::fmt::Display for RemoteOperationError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "{}: {}", self.context, self.cause)
	}
}
impl std::error::Error for RemoteOperationError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		Some(&self.cause)
	}
}

/// Bounded provider categories survive RetryLayer's final Persistent status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RemoteAvailabilityFailure {
	Unexpected,
	RateLimited,
}

pub(crate) fn remote_availability_failure(error: &Error) -> Option<RemoteAvailabilityFailure> {
	let remote = error.get_ref()?.downcast_ref::<RemoteOperationError>()?;
	match remote.cause.kind() {
		OpendalErrorKind::Unexpected => Some(RemoteAvailabilityFailure::Unexpected),
		OpendalErrorKind::RateLimited => Some(RemoteAvailabilityFailure::RateLimited),
		_ => None,
	}
}

fn opendal_error(context: &'static str, err: opendal::Error) -> Error {
	let kind = match err.kind() {
		OpendalErrorKind::Unexpected | OpendalErrorKind::RateLimited => ErrorKind::WouldBlock,
		OpendalErrorKind::NotFound => ErrorKind::NotFound,
		OpendalErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
		OpendalErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
		OpendalErrorKind::IsADirectory
		| OpendalErrorKind::NotADirectory
		| OpendalErrorKind::ConfigInvalid
		| OpendalErrorKind::Unsupported => ErrorKind::InvalidInput,
		_ => ErrorKind::Other,
	};
	Error::new(
		kind,
		RemoteOperationError {
			context,
			cause: err,
		},
	)
}

// ============================================================
//  REMOTE BACKEND
// ============================================================

/// Remote [`ObjectStore`] backed by OpenDAL.
pub struct RemoteObjectStore {
	operator: Operator,
	bucket: String,
	root: String,
}

impl RemoteObjectStore {
	pub fn new(config: RemoteObjectStoreConfig) -> std::io::Result<Self> {
		config.validate()?;

		let root = normalize_prefix(&config.root);
		let root_path = (!root.is_empty()).then(|| format!("/{root}"));
		let operator = match &config.provider {
			RemoteObjectStoreProvider::S3Compatible(provider) => {
				let mut builder = S3::default()
					.bucket(&config.bucket)
					.endpoint(&provider.endpoint)
					.region(&provider.region);
				if let Some(root_path) = root_path.as_deref() {
					builder = builder.root(root_path);
				}
				if let Some(access_key_id) = provider.access_key_id.as_deref() {
					builder = builder.access_key_id(access_key_id);
				}
				if let Some(secret_access_key) = provider.secret_access_key.as_deref() {
					builder = builder.secret_access_key(secret_access_key);
				}
				if let Some(session_token) = provider.session_token.as_deref() {
					builder = builder.session_token(session_token);
				}
				if provider.allow_anonymous {
					builder = builder.allow_anonymous();
				}
				if provider.virtual_host_style {
					builder = builder.enable_virtual_host_style();
				}
				Operator::new(builder)
					.map_err(|err| opendal_error("failed to build remote object store", err))?
					.layer(
						RetryLayer::new()
							.with_jitter()
							.with_min_delay(Duration::from_millis(50))
							.with_max_delay(Duration::from_secs(1))
							.with_max_times(4),
					)
					.finish()
			}
			RemoteObjectStoreProvider::BackblazeB2(provider) => {
				let mut builder = B2::default()
					.bucket(&config.bucket)
					.bucket_id(&provider.bucket_id)
					.application_key_id(&provider.application_key_id)
					.application_key(&provider.application_key);
				if let Some(root_path) = root_path.as_deref() {
					builder = builder.root(root_path);
				}
				Operator::new(builder)
					.map_err(|err| opendal_error("failed to build remote object store", err))?
					.layer(
						RetryLayer::new()
							.with_jitter()
							.with_min_delay(Duration::from_millis(50))
							.with_max_delay(Duration::from_secs(1))
							.with_max_times(4),
					)
					.finish()
			}
			RemoteObjectStoreProvider::GoogleCloudStorage(provider) => {
				let mut builder = Gcs::default().bucket(&config.bucket);
				if let Some(root_path) = root_path.as_deref() {
					builder = builder.root(root_path);
				}
				if let Some(endpoint) = provider.endpoint.as_deref() {
					builder = builder.endpoint(endpoint);
				}
				if let Some(credential) = provider.credential.as_deref() {
					builder = builder.credential(credential);
				}
				if let Some(credential_path) = provider.credential_path.as_deref() {
					builder = builder.credential_path(credential_path);
				}
				if let Some(service_account) = provider.service_account.as_deref() {
					builder = builder.service_account(service_account);
				}
				if let Some(token) = provider.token.as_ref() {
					builder = builder.token(token.clone());
				}
				if provider.allow_anonymous {
					builder = builder.allow_anonymous();
				}
				Operator::new(builder)
					.map_err(|err| opendal_error("failed to build remote object store", err))?
					.layer(
						RetryLayer::new()
							.with_jitter()
							.with_min_delay(Duration::from_millis(50))
							.with_max_delay(Duration::from_secs(1))
							.with_max_times(4),
					)
					.finish()
			}
		};

		Ok(Self {
			operator,
			bucket: config.bucket,
			root,
		})
	}

	fn uri_for_key(&self, key: &str) -> String {
		if self.root.is_empty() {
			format!("{REMOTE_OBJECT_URI_SCHEME}{}/{}", self.bucket, key)
		} else {
			format!(
				"{REMOTE_OBJECT_URI_SCHEME}{}/{}/{}",
				self.bucket, self.root, key
			)
		}
	}

	fn key_from_uri(&self, uri: &str) -> std::io::Result<String> {
		let remainder = uri.strip_prefix(REMOTE_OBJECT_URI_SCHEME).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!(
					"RemoteObjectStore only speaks {REMOTE_OBJECT_URI_SCHEME} URIs, got: {uri}"
				),
			)
		})?;
		let (bucket, object_path) = remainder.split_once('/').ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("remote object URI is missing an object path: {uri}"),
			)
		})?;
		if bucket != self.bucket {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"remote object URI bucket {bucket} does not match configured bucket {}",
					self.bucket
				),
			));
		}
		let object_path = object_path.trim_matches('/');
		if object_path.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!("remote object URI is missing an object path: {uri}"),
			));
		}
		if self.root.is_empty() {
			validate_object_key(object_path)?;
			return Ok(object_path.to_string());
		}
		let prefix = format!("{}/", self.root);
		if !object_path.starts_with(&prefix) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"remote object URI path is outside configured root {}: {uri}",
					self.root
				),
			));
		}
		let key = &object_path[prefix.len()..];
		validate_object_key(key)?;
		Ok(key.to_string())
	}
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
	fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
		validate_object_key(key)?;
		Ok(self.uri_for_key(key))
	}

	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		validate_object_key(key)?;
		self.operator
			.write(key, bytes.to_vec())
			.await
			.map_err(|err| opendal_error("remote object put failed", err))?;
		Ok(self.uri_for_key(key))
	}

	async fn head(&self, uri: &str) -> std::io::Result<ObjectMetadata> {
		let key = self.key_from_uri(uri)?;
		let metadata = self
			.operator
			.stat(&key)
			.await
			.map_err(|err| opendal_error("remote object head failed", err))?;
		Ok(ObjectMetadata {
			uri: self.uri_for_key(&key),
			key,
			size_bytes: metadata.content_length(),
		})
	}

	async fn get_range(&self, uri: &str, range: Range<u64>) -> std::io::Result<Bytes> {
		let key = self.key_from_uri(uri)?;
		if checked_range_len(&range)? == 0 {
			return Ok(Bytes::new());
		}
		let buffer = self
			.operator
			.read_with(&key)
			.range(range.start..range.end)
			.await
			.map_err(|err| opendal_error("remote object range get failed", err))?;
		Ok(Bytes::from(buffer.to_vec()))
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let key = self.key_from_uri(uri)?;
		let buffer = self
			.operator
			.read(&key)
			.await
			.map_err(|err| opendal_error("remote object get failed", err))?;
		Ok(Bytes::from(buffer.to_vec()))
	}

	async fn list(&self, prefix: &str) -> std::io::Result<Vec<ObjectMetadata>> {
		validate_object_prefix(prefix)?;
		let entries = self
			.operator
			.list_with(prefix)
			.recursive(true)
			.await
			.map_err(|err| opendal_error("remote object list failed", err))?;
		let mut objects = entries
			.into_iter()
			.filter_map(|entry| {
				if !entry.metadata().is_file() {
					return None;
				}
				let key = entry.path().trim_matches('/').to_string();
				Some(ObjectMetadata {
					uri: self.uri_for_key(&key),
					key,
					size_bytes: entry.metadata().content_length(),
				})
			})
			.collect::<Vec<_>>();
		objects.sort_by(|left, right| left.key.cmp(&right.key));
		Ok(objects)
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		let key = self.key_from_uri(uri)?;
		self.operator
			.delete(&key)
			.await
			.map_err(|err| opendal_error("remote object delete failed", err))
	}
}

// ============================================================
//  LOCAL FS BACKEND
// ============================================================

/// Filesystem-backed [`ObjectStore`].
///
/// Writes are atomic via a `<name>.tmp` + rename, so a crash mid-
/// upload leaves at most a `.tmp` sidecar that the operator can
/// sweep. Reads are straight `fs::read` — the object store is not
/// expected to be a latency-critical path.
pub struct LocalFsObjectStore {
	root: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
}

impl LocalFsObjectStore {
	/// Bind the store to `root`. The directory must already exist;
	/// we refuse to create it so the caller keeps control over
	/// permissions, mount layout, and any cleanup policy.
	pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
		Self::new_with_file_ops(root, Arc::new(BlockingFileOps))
	}

	pub fn new_with_file_ops(
		root: impl Into<PathBuf>,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> std::io::Result<Self> {
		let root = root.into();
		if !root.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"LocalFsObjectStore root is not a directory: {}",
					root.display()
				),
			));
		}
		Ok(Self { root, file_ops })
	}

	/// Translate a "file://" URI back into a path, pinned under
	/// `root` so a malformed URI cannot reach outside the store.
	fn resolve(&self, uri: &str) -> std::io::Result<PathBuf> {
		let path = uri.strip_prefix("file://").ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("LocalFsObjectStore only speaks file:// URIs, got: {uri}"),
			)
		})?;
		let candidate = PathBuf::from(path);
		let root = self.root.canonicalize()?;
		let canonical = candidate.canonicalize()?;
		if !canonical.starts_with(&root) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!("URI resolves outside the object store root: {uri}"),
			));
		}
		Ok(canonical)
	}

	async fn fsync_parent_dir(&self, path: &Path) -> std::io::Result<()> {
		let parent = path.parent().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("path has no parent directory: {}", path.display()),
			)
		})?;
		self.file_ops.fsync_dir(parent).await
	}

	fn metadata_from_canonical_path(
		canonical_root: &Path,
		canonical_path: &Path,
		size_bytes: u64,
	) -> std::io::Result<ObjectMetadata> {
		if !canonical_path.starts_with(canonical_root) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"path resolves outside the object store root: {}",
					canonical_path.display()
				),
			));
		}
		let key = canonical_path
			.strip_prefix(canonical_root)
			.map_err(|err| Error::other(format!("failed to derive object key: {err}")))?
			.to_string_lossy()
			.replace('\\', "/");
		validate_object_key(&key)?;
		Ok(ObjectMetadata {
			uri: format!("file://{}", canonical_path.display()),
			key,
			size_bytes,
		})
	}
}

#[async_trait]
impl ObjectStore for LocalFsObjectStore {
	fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
		validate_object_key(key)?;
		let mut path = self.root.canonicalize()?;
		for component in Path::new(key).components() {
			if let Component::Normal(name) = component {
				path.push(name);
				match std::fs::symlink_metadata(&path) {
					Ok(meta) if meta.file_type().is_symlink() => {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"planned upload path contains a symlink",
						));
					}
					Ok(_) => {}
					Err(error) if error.kind() == ErrorKind::NotFound => {}
					Err(error) => return Err(error),
				}
			}
		}
		Ok(format!("file://{}", path.display()))
	}

	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		validate_object_key(key)?;
		let final_path = self.root.join(key);
		if let Some(parent) = final_path.parent() {
			self.file_ops.create_dir_all(parent).await?;
		}
		// Atomic rename: write to `<final>.tmp`, then rename. A
		// concurrent reader will either see the previous object
		// (if any) or the new one — never a partial upload.
		let mut tmp_path = final_path.clone();
		tmp_path.as_mut_os_string().push(".tmp");
		write_file(
			self.file_ops.as_ref(),
			&tmp_path,
			OpenFlags {
				write: true,
				create: true,
				truncate: true,
				..Default::default()
			},
			bytes,
		)
		.await?;
		self.file_ops.rename(&tmp_path, &final_path).await?;
		self.fsync_parent_dir(&final_path).await?;
		let canonical = final_path.canonicalize()?;
		Ok(format!("file://{}", canonical.display()))
	}

	async fn head(&self, uri: &str) -> std::io::Result<ObjectMetadata> {
		let path = self.resolve(uri)?;
		let size_bytes = self.file_ops.file_len(&path).await?.ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!("object disappeared before head completed: {uri}"),
			)
		})?;
		let canonical_root = self.root.canonicalize()?;
		Self::metadata_from_canonical_path(&canonical_root, &path, size_bytes)
	}

	async fn get_range(&self, uri: &str, range: Range<u64>) -> std::io::Result<Bytes> {
		let len = checked_range_len(&range)?;
		if len == 0 {
			return Ok(Bytes::new());
		}
		let path = self.resolve(uri)?;
		let handle = self
			.file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					..Default::default()
				},
			)
			.await?;
		Ok(Bytes::from(
			self.file_ops
				.read_plain_at(&handle, range.start, len)
				.await?,
		))
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let path = self.resolve(uri)?;
		Ok(Bytes::from(self.file_ops.read_all(&path).await?))
	}

	async fn list(&self, prefix: &str) -> std::io::Result<Vec<ObjectMetadata>> {
		validate_object_prefix(prefix)?;
		let canonical_root = self.root.canonicalize()?;
		let prefix = prefix.to_string();
		tokio::task::spawn_blocking(move || {
			let mut objects = Vec::new();
			let mut stack = vec![canonical_root.clone()];
			while let Some(dir) = stack.pop() {
				for entry in std::fs::read_dir(&dir)? {
					let entry = entry?;
					let file_type = entry.file_type()?;
					if file_type.is_dir() {
						let canonical_dir = entry.path().canonicalize()?;
						if canonical_dir.starts_with(&canonical_root) {
							stack.push(canonical_dir);
						}
						continue;
					}
					if !file_type.is_file() {
						continue;
					}
					let canonical_path = entry.path().canonicalize()?;
					if !canonical_path.starts_with(&canonical_root) {
						continue;
					}
					let object = Self::metadata_from_canonical_path(
						&canonical_root,
						&canonical_path,
						entry.metadata()?.len(),
					)?;
					if object.key.starts_with(&prefix) {
						objects.push(object);
					}
				}
			}
			objects.sort_by(|left, right| left.key.cmp(&right.key));
			Ok(objects)
		})
		.await
		.map_err(|err| Error::other(format!("local object list task failed: {err}")))?
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		let path = self.resolve(uri)?;
		self.file_ops.unlink(&path).await?;
		self.fsync_parent_dir(&path).await
	}
}

/// Operation names for [`MockObjectStore`] fault injection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MockObjectStoreOp {
	Put,
	Head,
	GetRange,
	GetBytes,
	List,
	Delete,
}

/// Per-operation latency configuration for [`MockObjectStore`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MockObjectStoreLatency {
	pub put: Duration,
	pub head: Duration,
	pub get_range: Duration,
	pub get_bytes: Duration,
	pub list: Duration,
	pub delete: Duration,
}

impl MockObjectStoreLatency {
	fn for_op(self, op: MockObjectStoreOp) -> Duration {
		match op {
			MockObjectStoreOp::Put => self.put,
			MockObjectStoreOp::Head => self.head,
			MockObjectStoreOp::GetRange => self.get_range,
			MockObjectStoreOp::GetBytes => self.get_bytes,
			MockObjectStoreOp::List => self.list,
			MockObjectStoreOp::Delete => self.delete,
		}
	}
}

/// Fault action for [`MockObjectStore`] operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MockObjectStoreFaultAction {
	Error { kind: ErrorKind, message: String },
}

/// One-shot or delayed fault injected into [`MockObjectStore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InjectedMockObjectStoreFault {
	op: MockObjectStoreOp,
	key: Option<String>,
	remaining_matches: usize,
	action: MockObjectStoreFaultAction,
}

impl InjectedMockObjectStoreFault {
	pub fn on(op: MockObjectStoreOp, action: MockObjectStoreFaultAction) -> Self {
		Self {
			op,
			key: None,
			remaining_matches: 0,
			action,
		}
	}

	pub fn for_key(mut self, key: impl Into<String>) -> Self {
		self.key = Some(key.into());
		self
	}

	pub fn after_matches(mut self, remaining_matches: usize) -> Self {
		self.remaining_matches = remaining_matches;
		self
	}

	fn matches(&self, op: MockObjectStoreOp, target: &str) -> bool {
		self.op == op && self.key.as_ref().map(|key| key == target).unwrap_or(true)
	}
}

/// Deterministic configuration for [`MockObjectStore`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MockObjectStoreConfig {
	pub latency: MockObjectStoreLatency,
	/// Number of `list` calls a `put` remains invisible for.
	pub list_visibility_delay: u64,
	/// Number of `list` calls a `delete` remains visible for.
	pub delete_visibility_delay: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MockCatalogEntry {
	size_bytes: u64,
	visible_after_list_epoch: u64,
	hidden_after_list_epoch: Option<u64>,
}

#[derive(Default)]
struct MockObjectStoreState {
	objects: BTreeMap<String, Bytes>,
	catalog: BTreeMap<String, MockCatalogEntry>,
	faults: VecDeque<InjectedMockObjectStoreFault>,
	list_epoch: u64,
}

/// In-memory [`ObjectStore`] for deterministic simulator and harness use.
///
/// `get`/`head` are strongly consistent. `list` can lag behind `put` and
/// `delete` according to the configured visibility delays.
pub struct MockObjectStore {
	config: MockObjectStoreConfig,
	state: Mutex<MockObjectStoreState>,
}

impl MockObjectStore {
	pub fn new(config: MockObjectStoreConfig) -> Self {
		Self {
			config,
			state: Mutex::new(MockObjectStoreState::default()),
		}
	}

	pub fn inject(&self, fault: InjectedMockObjectStoreFault) {
		self.state.lock().unwrap().faults.push_back(fault);
	}

	fn uri_for_key(&self, key: &str) -> String {
		format!("mock://store/{key}")
	}

	fn key_from_uri(&self, uri: &str) -> std::io::Result<String> {
		let key = uri.strip_prefix("mock://store/").ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("MockObjectStore only speaks mock://store URIs, got: {uri}"),
			)
		})?;
		validate_object_key(key)?;
		Ok(key.to_string())
	}

	async fn apply_latency(&self, op: MockObjectStoreOp) {
		let delay = self.config.latency.for_op(op);
		if !delay.is_zero() {
			tokio::time::sleep(delay).await;
		}
	}

	fn maybe_fail(&self, op: MockObjectStoreOp, target: &str) -> std::io::Result<()> {
		let mut state = self.state.lock().unwrap();
		let Some(idx) = state
			.faults
			.iter()
			.position(|fault| fault.matches(op, target))
		else {
			return Ok(());
		};
		let fault = state.faults.get_mut(idx).unwrap();
		if fault.remaining_matches > 0 {
			fault.remaining_matches -= 1;
			return Ok(());
		}
		match state.faults.remove(idx).unwrap().action {
			MockObjectStoreFaultAction::Error { kind, message } => Err(Error::new(kind, message)),
		}
	}

	fn not_found(kind: &str, target: &str) -> Error {
		Error::new(ErrorKind::NotFound, format!("{kind} not found: {target}"))
	}
}

impl Default for MockObjectStore {
	fn default() -> Self {
		Self::new(MockObjectStoreConfig::default())
	}
}

#[async_trait]
impl ObjectStore for MockObjectStore {
	fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
		validate_object_key(key)?;
		Ok(self.uri_for_key(key))
	}

	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		validate_object_key(key)?;
		self.apply_latency(MockObjectStoreOp::Put).await;
		self.maybe_fail(MockObjectStoreOp::Put, key)?;
		let mut state = self.state.lock().unwrap();
		let visible_after_list_epoch = state
			.list_epoch
			.saturating_add(self.config.list_visibility_delay)
			.saturating_add(1);
		state
			.objects
			.insert(key.to_string(), Bytes::copy_from_slice(bytes));
		state.catalog.insert(
			key.to_string(),
			MockCatalogEntry {
				size_bytes: bytes.len() as u64,
				visible_after_list_epoch,
				hidden_after_list_epoch: None,
			},
		);
		Ok(self.uri_for_key(key))
	}

	async fn head(&self, uri: &str) -> std::io::Result<ObjectMetadata> {
		let key = self.key_from_uri(uri)?;
		self.apply_latency(MockObjectStoreOp::Head).await;
		self.maybe_fail(MockObjectStoreOp::Head, &key)?;
		let state = self.state.lock().unwrap();
		let bytes = state
			.objects
			.get(&key)
			.ok_or_else(|| Self::not_found("object", &key))?;
		Ok(ObjectMetadata {
			key: key.clone(),
			uri: self.uri_for_key(&key),
			size_bytes: bytes.len() as u64,
		})
	}

	async fn get_range(&self, uri: &str, range: Range<u64>) -> std::io::Result<Bytes> {
		let key = self.key_from_uri(uri)?;
		let len = checked_range_len(&range)?;
		if len == 0 {
			return Ok(Bytes::new());
		}
		self.apply_latency(MockObjectStoreOp::GetRange).await;
		self.maybe_fail(MockObjectStoreOp::GetRange, &key)?;
		let state = self.state.lock().unwrap();
		let bytes = state
			.objects
			.get(&key)
			.ok_or_else(|| Self::not_found("object", &key))?;
		if range.end > bytes.len() as u64 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"byte range {}..{} exceeds object size {}",
					range.start,
					range.end,
					bytes.len()
				),
			));
		}
		Ok(bytes.slice(range.start as usize..range.end as usize))
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let key = self.key_from_uri(uri)?;
		self.apply_latency(MockObjectStoreOp::GetBytes).await;
		self.maybe_fail(MockObjectStoreOp::GetBytes, &key)?;
		let state = self.state.lock().unwrap();
		state
			.objects
			.get(&key)
			.cloned()
			.ok_or_else(|| Self::not_found("object", &key))
	}

	async fn list(&self, prefix: &str) -> std::io::Result<Vec<ObjectMetadata>> {
		validate_object_prefix(prefix)?;
		self.apply_latency(MockObjectStoreOp::List).await;
		self.maybe_fail(MockObjectStoreOp::List, prefix)?;
		let mut state = self.state.lock().unwrap();
		// Visibility delay is measured in successful `list` calls;
		// injected list faults do not advance the epoch.
		state.list_epoch = state.list_epoch.saturating_add(1);
		let list_epoch = state.list_epoch;
		let mut objects = state
			.catalog
			.iter()
			.filter_map(|(key, entry)| {
				if !key.starts_with(prefix) {
					return None;
				}
				if list_epoch < entry.visible_after_list_epoch {
					return None;
				}
				if entry
					.hidden_after_list_epoch
					.map(|hidden_after| list_epoch >= hidden_after)
					.unwrap_or(false)
				{
					return None;
				}
				Some(ObjectMetadata {
					key: key.clone(),
					uri: self.uri_for_key(key),
					size_bytes: entry.size_bytes,
				})
			})
			.collect::<Vec<_>>();
		objects.sort_by(|left, right| left.key.cmp(&right.key));
		Ok(objects)
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		let key = self.key_from_uri(uri)?;
		self.apply_latency(MockObjectStoreOp::Delete).await;
		self.maybe_fail(MockObjectStoreOp::Delete, &key)?;
		let mut state = self.state.lock().unwrap();
		if state.objects.remove(&key).is_none() {
			return Err(Self::not_found("object", &key));
		}
		let hidden_after_list_epoch = state
			.list_epoch
			.saturating_add(self.config.delete_visibility_delay)
			.saturating_add(1);
		if let Some(entry) = state.catalog.get_mut(&key) {
			entry.hidden_after_list_epoch = Some(hidden_after_list_epoch);
		}
		Ok(())
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;

	fn fresh_root() -> crate::test_support::TestDir {
		crate::test_support::TestDir::new("wendelon-l2")
	}

	#[tokio::test]
	async fn put_get_round_trips() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		let uri = store.put("slab_0001.wnd", b"hello wendelon").await.unwrap();
		assert!(uri.starts_with("file://"));
		let bytes = store.get(&uri).await.unwrap();
		assert_eq!(bytes, b"hello wendelon");
	}

	#[tokio::test]
	async fn planned_upload_uri_matches_the_uploaded_object() {
		let root = fresh_root();
		let local = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		let mock = MockObjectStore::default();
		for store in [&local as &dyn ObjectStore, &mock as &dyn ObjectStore] {
			let key = "planned/nested/object.wnd";
			let planned = store.planned_upload_uri(key).unwrap();
			assert_eq!(store.put(key, b"object").await.unwrap(), planned);
			assert_eq!(store.head(&planned).await.unwrap().size_bytes, 6);
		}
	}

	#[tokio::test]
	async fn head_reports_key_uri_and_size() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		let uri = store
			.put("sealed/slab_0001.wnd", b"hello wendelon")
			.await
			.unwrap();
		let meta = store.head(&uri).await.unwrap();
		assert_eq!(meta.key, "sealed/slab_0001.wnd");
		assert_eq!(meta.uri, uri);
		assert_eq!(meta.size_bytes, 14);
	}

	#[tokio::test]
	async fn get_range_reads_requested_span() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		let uri = store.put("slice.wnd", b"abcdefghij").await.unwrap();
		let slice = store.get_range(&uri, 2..7).await.unwrap();
		assert_eq!(slice.as_ref(), b"cdefg");
	}

	#[tokio::test]
	async fn list_returns_recursive_prefix_matches() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		store.put("sealed/slab_0001.wnd", b"aaa").await.unwrap();
		store
			.put("sealed/nested/slab_0002.wnd", b"bbbb")
			.await
			.unwrap();
		store.put("archive/slab_0003.wnd", b"cc").await.unwrap();

		let listed = store.list("sealed/").await.unwrap();
		assert_eq!(
			listed
				.iter()
				.map(|entry| entry.key.as_str())
				.collect::<Vec<_>>(),
			vec!["sealed/nested/slab_0002.wnd", "sealed/slab_0001.wnd"]
		);
		assert_eq!(listed[0].size_bytes, 4);
		assert_eq!(listed[1].size_bytes, 3);
	}

	#[tokio::test]
	async fn put_is_atomic_via_tmp_rename() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		let _uri = store.put("slab.wnd", b"a").await.unwrap();
		let _uri = store.put("slab.wnd", b"bb").await.unwrap();
		let tmp_leak = root.join("slab.wnd.tmp");
		assert!(!tmp_leak.exists(), "tmp rename residue leaked");
		let uri = format!("file://{}/slab.wnd", root.canonicalize().unwrap().display());
		assert_eq!(store.get(&uri).await.unwrap(), b"bb");
	}

	#[tokio::test]
	async fn delete_removes_object() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		let uri = store.put("gone.wnd", b"x").await.unwrap();
		store.delete(&uri).await.unwrap();
		assert!(store.get(&uri).await.is_err());
	}

	#[tokio::test]
	async fn rejects_traversal_keys() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		assert!(store.put("../evil", b"x").await.is_err());
		assert!(store.put("/etc/passwd", b"x").await.is_err());
		assert!(store.put("", b"x").await.is_err());
	}

	#[tokio::test]
	async fn rejects_uri_outside_root() {
		let root = fresh_root();
		let other = fresh_root();
		let store = LocalFsObjectStore::new(root.to_path_buf()).unwrap();
		// Point a well-formed file:// URI at a real file outside the root.
		let outside = other.join("foreign");
		fs::write(&outside, b"nope").unwrap();
		let uri = format!("file://{}", outside.canonicalize().unwrap().display());
		assert!(store.get(&uri).await.is_err());
	}

	#[tokio::test]
	async fn mock_store_round_trips_and_lists_with_visibility_delay() {
		let store = MockObjectStore::new(MockObjectStoreConfig {
			list_visibility_delay: 1,
			delete_visibility_delay: 1,
			..Default::default()
		});

		let uri = store.put("sealed/slab_0001.wnd", b"hello").await.unwrap();
		assert_eq!(store.get(&uri).await.unwrap(), b"hello");
		assert_eq!(store.head(&uri).await.unwrap().size_bytes, 5);

		assert!(
			store.list("sealed/").await.unwrap().is_empty(),
			"first list should still miss the delayed put"
		);
		assert_eq!(store.list("sealed/").await.unwrap().len(), 1);

		store.delete(&uri).await.unwrap();
		assert_eq!(
			store.get(&uri).await.unwrap_err().kind(),
			ErrorKind::NotFound
		);
		assert_eq!(store.list("sealed/").await.unwrap().len(), 1);
		assert!(
			store.list("sealed/").await.unwrap().is_empty(),
			"delete visibility delay should eventually expire"
		);
	}

	#[tokio::test]
	async fn mock_store_injected_fault_applies_once() {
		let store = MockObjectStore::default();
		let uri = store.put("faulty.wnd", b"abc").await.unwrap();
		store.inject(
			InjectedMockObjectStoreFault::on(
				MockObjectStoreOp::GetBytes,
				MockObjectStoreFaultAction::Error {
					kind: ErrorKind::TimedOut,
					message: "simulated get timeout".into(),
				},
			)
			.for_key("faulty.wnd"),
		);

		let err = store.get(&uri).await.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::TimedOut);
		assert_eq!(store.get(&uri).await.unwrap(), b"abc");
	}

	#[tokio::test]
	async fn mock_store_range_reads_validate_bounds() {
		let store = MockObjectStore::default();
		let uri = store.put("slice.wnd", b"abcdefghij").await.unwrap();
		assert_eq!(store.get_range(&uri, 2..6).await.unwrap().as_ref(), b"cdef");
		let err = store.get_range(&uri, 2..16).await.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput);
	}
}

#[cfg(test)]
mod remote_operation_error_controls {
	use super::*;

	#[test]
	fn remote_availability_failures_keep_a_typed_cause_and_retryable_kind() {
		for kind in [OpendalErrorKind::Unexpected, OpendalErrorKind::RateLimited] {
			let error = opendal_error(
				"remote read",
				opendal::Error::new(kind, "same opaque message").set_persistent(),
			);
			assert_eq!(
				error.kind(),
				ErrorKind::WouldBlock,
				"backend availability remains retryable after its retry layer stops"
			);
			assert!(
				error.get_ref().unwrap().source().is_some(),
				"the original provider error is retained as a typed cause"
			);
		}
	}

	#[test]
	fn remote_authorization_and_configuration_errors_keep_their_nonretryable_kind() {
		for (kind, expected) in [
			(
				OpendalErrorKind::PermissionDenied,
				ErrorKind::PermissionDenied,
			),
			(OpendalErrorKind::ConfigInvalid, ErrorKind::InvalidInput),
			(OpendalErrorKind::Unsupported, ErrorKind::InvalidInput),
		] {
			let error = opendal_error(
				"remote read",
				opendal::Error::new(kind, "same opaque message").set_persistent(),
			);
			assert_eq!(error.kind(), expected);
			assert!(
				error.get_ref().unwrap().source().is_some(),
				"classification never requires the error message"
			);
		}
	}
}

#[cfg(test)]
mod authority_error_policy_controls {
	use super::*;
	use crate::storage::manager::tier_authority::transient_tier_error;

	#[test]
	fn authority_retry_policy_uses_typed_remote_causes_and_keeps_opaque_failures_sticky() {
		for (provider, expected) in [
			(
				OpendalErrorKind::Unexpected,
				RemoteAvailabilityFailure::Unexpected,
			),
			(
				OpendalErrorKind::RateLimited,
				RemoteAvailabilityFailure::RateLimited,
			),
		] {
			let error = opendal_error(
				"remote read",
				opendal::Error::new(provider, "opaque").set_persistent(),
			);
			assert_eq!(remote_availability_failure(&error), Some(expected));
			assert!(transient_tier_error(error.kind()));
		}
		for kind in [
			OpendalErrorKind::PermissionDenied,
			OpendalErrorKind::ConfigInvalid,
			OpendalErrorKind::Unsupported,
		] {
			let error = opendal_error(
				"remote read",
				opendal::Error::new(kind, "timeout Unexpected retry"),
			);
			assert_eq!(remote_availability_failure(&error), None);
			assert!(!transient_tier_error(error.kind()));
		}
		for kind in [ErrorKind::Other, ErrorKind::InvalidData] {
			let error = Error::new(kind, "remote Unexpected timeout retry");
			assert_eq!(remote_availability_failure(&error), None);
			assert!(!transient_tier_error(error.kind()));
		}
		assert!(transient_tier_error(ErrorKind::NotFound));
	}
}
