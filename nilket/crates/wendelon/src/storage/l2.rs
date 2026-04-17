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
//! The trait is intentionally a three-verb shape — `put`, `get`,
//! `delete` — because V1's sweeper is not yet doing anything
//! fancier. Range-GETs (so L2 reads do not have to pull an entire
//! slab blob per record) and multi-part uploads are deferred to a
//! follow-up once the end-to-end control-ring pivot is proven.
//!
//! ## Why not the AWS SDK
//!
//! The AWS SDK pulls a small forest of dependencies and wants a
//! Tokio runtime handle at construction. Keeping the seam at a
//! tiny trait means remote backends can land behind the same call
//! sites with zero churn to the sweeper. The first remote backend
//! here is a generic object-store implementation over the
//! S3-compatible API surface, built on OpenDAL, so a node can push
//! sealed slabs to RustFS, MinIO, AWS S3, Google Cloud Storage's
//! XML API, Backblaze B2's S3 layer, Ceph RGW, and similar targets
//! without rewriting the tiering path.
//!
//! ## URI shape
//!
//! Callers store whatever `put` returns and feed it back into
//! `get` / `delete`. The URI format is implementation-private —
//! `LocalFsObjectStore` uses `file://<absolute path>` so a
//! deployment that relocates the L2 root sees the bad paths fail
//! loudly rather than silently sliding into "some other file".

use async_trait::async_trait;
use bytes::Bytes;
use opendal::layers::RetryLayer;
use opendal::services::S3;
use opendal::{ErrorKind as OpendalErrorKind, Operator};
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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

/// Remote object-store config.
///
/// The transport dialect is the widely implemented S3-compatible
/// surface, but the target can be any backend that speaks it.
/// Persisted URIs use the canonical `s3://bucket/prefix/key`
/// family so tier metadata remains endpoint-independent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteObjectStoreConfig {
	pub bucket: String,
	pub endpoint: String,
	pub region: String,
	/// Optional prefix inside the bucket.
	pub root: String,
	pub access_key_id: Option<String>,
	pub secret_access_key: Option<String>,
	pub session_token: Option<String>,
	/// Allows unsigned requests for local harnesses or anonymous
	/// compatible stores.
	pub allow_anonymous: bool,
	pub virtual_host_style: bool,
}

impl RemoteObjectStoreConfig {
	fn validate(&self) -> std::io::Result<()> {
		if self.bucket.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"remote object store bucket must not be empty",
			));
		}
		if self.endpoint.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"remote object store endpoint must not be empty",
			));
		}
		if self.region.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"remote object store region must not be empty",
			));
		}
		let has_access_key = self.access_key_id.is_some();
		let has_secret = self.secret_access_key.is_some();
		if has_access_key != has_secret {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"remote object store credentials must provide both access_key_id and secret_access_key",
			));
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
		let mut builder = S3::default()
			.bucket(&config.bucket)
			.endpoint(&config.endpoint)
			.region(&config.region);
		if !root.is_empty() {
			builder = builder.root(&format!("/{root}"));
		}
		if let Some(access_key_id) = config.access_key_id.as_deref() {
			builder = builder.access_key_id(access_key_id);
		}
		if let Some(secret_access_key) = config.secret_access_key.as_deref() {
			builder = builder.secret_access_key(secret_access_key);
		}
		if let Some(session_token) = config.session_token.as_deref() {
			builder = builder.session_token(session_token);
		}
		if config.allow_anonymous {
			builder = builder.allow_anonymous();
		}
		if config.virtual_host_style {
			builder = builder.enable_virtual_host_style();
		}

		let operator = Operator::new(builder)
			.map_err(|err| opendal_error("failed to build remote object store", err))?
			.layer(
				RetryLayer::new()
					.with_jitter()
					.with_min_delay(Duration::from_millis(50))
					.with_max_delay(Duration::from_secs(1))
					.with_max_times(4),
			)
			.finish();

		Ok(Self {
			operator,
			bucket: config.bucket,
			root,
		})
	}

	fn uri_for_key(&self, key: &str) -> String {
		if self.root.is_empty() {
			format!("s3://{}/{}", self.bucket, key)
		} else {
			format!("s3://{}/{}/{}", self.bucket, self.root, key)
		}
	}

	fn key_from_uri(&self, uri: &str) -> std::io::Result<String> {
		let remainder = uri.strip_prefix("s3://").ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("RemoteObjectStore only speaks s3:// URIs, got: {uri}"),
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

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let key = self.key_from_uri(uri)?;
		let buffer = self
			.operator
			.read(&key)
			.await
			.map_err(|err| opendal_error("remote object get failed", err))?;
		Ok(Bytes::from(buffer.to_vec()))
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
}

impl LocalFsObjectStore {
	/// Bind the store to `root`. The directory must already exist;
	/// we refuse to create it so the caller keeps control over
	/// permissions, mount layout, and any cleanup policy.
	pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
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
		Ok(Self { root })
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
}

#[async_trait]
impl ObjectStore for LocalFsObjectStore {
	async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
		validate_object_key(key)?;
		let final_path = self.root.join(key);
		if let Some(parent) = final_path.parent() {
			fs::create_dir_all(parent)?;
		}
		// Atomic rename: write to `<final>.tmp`, then rename. A
		// concurrent reader will either see the previous object
		// (if any) or the new one — never a partial upload.
		let mut tmp_path = final_path.clone();
		tmp_path.as_mut_os_string().push(".tmp");
		fs::write(&tmp_path, bytes)?;
		fs::rename(&tmp_path, &final_path)?;
		let canonical = final_path.canonicalize()?;
		Ok(format!("file://{}", canonical.display()))
	}

	async fn get_bytes(&self, uri: &str) -> std::io::Result<Bytes> {
		let path = self.resolve(uri)?;
		Ok(Bytes::from(fs::read(path)?))
	}

	async fn delete(&self, uri: &str) -> std::io::Result<()> {
		let path = self.resolve(uri)?;
		fs::remove_file(path)
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
