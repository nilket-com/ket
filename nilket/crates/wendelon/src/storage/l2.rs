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
#[cfg(test)]
use std::fs;
use std::io::{Error, ErrorKind};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
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

fn opendal_error(context: &str, err: opendal::Error) -> Error {
	let kind = match err.kind() {
		OpendalErrorKind::NotFound => ErrorKind::NotFound,
		OpendalErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
		OpendalErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
		OpendalErrorKind::IsADirectory
		| OpendalErrorKind::NotADirectory
		| OpendalErrorKind::ConfigInvalid
		| OpendalErrorKind::Unsupported => ErrorKind::InvalidInput,
		_ => ErrorKind::Other,
	};
	Error::new(kind, format!("{context}: {err}"))
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

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;

	fn fresh_root() -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-l2-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		fs::create_dir_all(&dir).unwrap();
		dir
	}

	#[tokio::test]
	async fn put_get_round_trips() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(&root).unwrap();
		let uri = store.put("slab_0001.wnd", b"hello wendelon").await.unwrap();
		assert!(uri.starts_with("file://"));
		let bytes = store.get(&uri).await.unwrap();
		assert_eq!(bytes, b"hello wendelon");
	}

	#[tokio::test]
	async fn head_reports_key_uri_and_size() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(&root).unwrap();
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
		let store = LocalFsObjectStore::new(&root).unwrap();
		let uri = store.put("slice.wnd", b"abcdefghij").await.unwrap();
		let slice = store.get_range(&uri, 2..7).await.unwrap();
		assert_eq!(slice.as_ref(), b"cdefg");
	}

	#[tokio::test]
	async fn list_returns_recursive_prefix_matches() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(&root).unwrap();
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
		let store = LocalFsObjectStore::new(&root).unwrap();
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
		let store = LocalFsObjectStore::new(&root).unwrap();
		let uri = store.put("gone.wnd", b"x").await.unwrap();
		store.delete(&uri).await.unwrap();
		assert!(store.get(&uri).await.is_err());
	}

	#[tokio::test]
	async fn rejects_traversal_keys() {
		let root = fresh_root();
		let store = LocalFsObjectStore::new(&root).unwrap();
		assert!(store.put("../evil", b"x").await.is_err());
		assert!(store.put("/etc/passwd", b"x").await.is_err());
		assert!(store.put("", b"x").await.is_err());
	}

	#[tokio::test]
	async fn rejects_uri_outside_root() {
		let root = fresh_root();
		let other = fresh_root();
		let store = LocalFsObjectStore::new(&root).unwrap();
		// Point a well-formed file:// URI at a real file outside the root.
		let outside = other.join("foreign");
		fs::write(&outside, b"nope").unwrap();
		let uri = format!("file://{}", outside.canonicalize().unwrap().display());
		assert!(store.get(&uri).await.is_err());
	}
}
