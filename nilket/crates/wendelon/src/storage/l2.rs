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
//! tiny trait means the day an S3 backend lands, it drops into the
//! same call sites with zero churn to the sweeper. Until then,
//! `LocalFsObjectStore` is the entire "backend" surface.
//!
//! ## URI shape
//!
//! Callers store whatever `put` returns and feed it back into
//! `get` / `delete`. The URI format is implementation-private —
//! `LocalFsObjectStore` uses `file://<absolute path>` so a
//! deployment that relocates the L2 root sees the bad paths fail
//! loudly rather than silently sliding into "some other file".

use async_trait::async_trait;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

/// The L2 storage seam.
///
/// Every method is `async` so S3-style HTTP backends fit without
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

    /// Fetch the full object previously uploaded to `uri`. The
    /// sweeper uses this for its post-upload hash verification;
    /// the read path uses it for L1-evicted lookups.
    async fn get(&self, uri: &str) -> std::io::Result<Vec<u8>>;

    /// Remove the object previously uploaded to `uri`. Not on the
    /// V1 sweeper's hot path (the L1 unlink is the primary GC
    /// event) but needed for test teardown and future retention
    /// workflows.
    async fn delete(&self, uri: &str) -> std::io::Result<()>;
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
                format!("LocalFsObjectStore root is not a directory: {}", root.display()),
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
        if key.is_empty() || key.contains("..") || Path::new(key).is_absolute() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("rejecting unsafe object key: {key}"),
            ));
        }
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

    async fn get(&self, uri: &str) -> std::io::Result<Vec<u8>> {
        let path = self.resolve(uri)?;
        fs::read(path)
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
