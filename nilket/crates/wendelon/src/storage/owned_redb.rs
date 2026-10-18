//! A redb database whose lifetime is bounded by data-directory ownership.
//!
//! redb's `Drop for Database` writes (an allocator-state commit and trim, then a
//! header flush). A store must therefore never let its `Database` drop after the
//! owner releases the directory lock. Every access, reads included, runs under
//! a write permit, and the registered closer drops the database on a joined
//! blocking thread inside the owner's close, before the lock is released.

use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex, OnceLock};

use redb::Database;

use crate::storage::ownership::{DataDirectoryOwner, StorageWorker, closed_error};

pub(crate) struct OwnedRedb {
	db: Arc<Mutex<Option<Arc<Database>>>>,
	owner: OnceLock<DataDirectoryOwner>,
	label: &'static str,
}

struct RedbCloser {
	db: Arc<Mutex<Option<Arc<Database>>>>,
	// The database's drop writes. Once started it runs to completion on its own
	// thread; a cancelled or timed-out close resumes waiting on the same drop
	// rather than treating it as done.
	dropping: Mutex<Option<Arc<crate::storage::ownership::WorkerJoins>>>,
	label: &'static str,
}

impl OwnedRedb {
	pub(crate) fn new(db: Database, label: &'static str) -> Self {
		Self {
			db: Arc::new(Mutex::new(Some(Arc::new(db)))),
			owner: OnceLock::new(),
			label,
		}
	}

	/// Bind this store to the node's owner and register its closer. Rebinding to
	/// the same owner is a no-op; binding to another owner generation is refused.
	pub(crate) fn bind_owner(&self, owner: DataDirectoryOwner) -> std::io::Result<()> {
		if let Some(existing) = self.owner.get() {
			return if existing.same_owner(&owner) {
				Ok(())
			} else {
				Err(Error::new(
					ErrorKind::InvalidInput,
					format!("{} belongs to another owner generation", self.label),
				))
			};
		}
		// Register the closer before binding: a store must never be bound to an
		// owner that cannot close it.
		owner.register_worker(Arc::new(RedbCloser {
			db: self.db.clone(),
			dropping: Mutex::new(None),
			label: self.label,
		}))?;
		self.owner.set(owner).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("{} was bound concurrently", self.label),
			)
		})
	}

	/// True once the owning close has taken the database away. A process-wide
	/// store registry must not hand such a store to a new owner.
	pub(crate) fn is_closed(&self) -> bool {
		self.db
			.lock()
			.expect("owned redb handle poisoned")
			.is_none()
	}

	/// Run one database operation on a blocking thread under a permit. The
	/// database handle is released before the permit, so a drained owner has no
	/// outstanding handle besides the closer's.
	pub(crate) async fn run<T, F>(&self, f: F) -> std::io::Result<T>
	where
		T: Send + 'static,
		F: FnOnce(&Database) -> std::io::Result<T> + Send + 'static,
	{
		let (db, permit) = self.access()?;
		let observation = crate::storage::finality_observation::current_work();
		tokio::task::spawn_blocking(move || {
			let _observation = observation;
			let result = f(&db);
			drop(db);
			drop(permit);
			result
		})
		.await
		.map_err(|err| Error::other(format!("blocking {} task failed: {err}", self.label)))?
	}

	/// Test-only direct handle. Never hold it across an owner close.
	#[cfg(test)]
	pub(crate) fn test_handle(&self) -> Arc<Database> {
		self.db
			.lock()
			.expect("owned redb handle poisoned")
			.clone()
			.expect("owned redb is open")
	}

	fn access(
		&self,
	) -> std::io::Result<(
		Arc<Database>,
		Option<crate::storage::ownership::WritePermit>,
	)> {
		// Permit first: once the owner is Closing no new access is admitted, so
		// the closer cannot race a fresh clone of the handle.
		let permit = self
			.owner
			.get()
			.map(|owner| owner.write_permit())
			.transpose()?;
		let db = self
			.db
			.lock()
			.expect("owned redb handle poisoned")
			.clone()
			.ok_or_else(closed_error)?;
		Ok((db, permit))
	}
}

#[async_trait::async_trait]
impl StorageWorker for RedbCloser {
	async fn close_and_join(&self) -> std::io::Result<()> {
		let pending = self
			.dropping
			.lock()
			.expect("owned redb closer poisoned")
			.clone();
		let joins = match pending {
			Some(joins) => joins,
			None => {
				let Some(db) = self.db.lock().expect("owned redb handle poisoned").take() else {
					return Ok(());
				};
				// The owner has drained every permit, so this must be the only
				// handle. Anything else means an access escaped the permit; fail
				// closed and keep the lock rather than let a later drop write unowned.
				let db = match Arc::try_unwrap(db) {
					Ok(db) => db,
					Err(db) => {
						*self.db.lock().expect("owned redb handle poisoned") = Some(db);
						return Err(Error::other(format!(
							"{} still has an outstanding handle at close",
							self.label
						)));
					}
				};
				let thread = std::thread::Builder::new()
					.name(format!("wendelon-close-{}", self.label.replace(' ', "-")))
					.spawn(move || drop(db))?;
				let joins = crate::storage::ownership::WorkerJoins::new(vec![thread]);
				*self.dropping.lock().expect("owned redb closer poisoned") = Some(joins.clone());
				joins
			}
		};
		joins
			.join()
			.await
			.map_err(|err| Error::other(format!("closing {} failed: {err}", self.label)))
	}
}

#[cfg(all(test, unix))]
mod tests {
	use super::*;
	use std::path::{Path, PathBuf};
	use std::time::Duration;

	struct TestDir(PathBuf);
	impl TestDir {
		fn new() -> Self {
			let path =
				std::env::temp_dir().join(format!("wendelon-owned-redb-{}", uuid::Uuid::new_v4()));
			std::fs::create_dir(&path).unwrap();
			Self(path)
		}
		fn path(&self) -> &Path {
			&self.0
		}
	}
	impl Drop for TestDir {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}

	const TABLE: redb::TableDefinition<'_, u64, u64> = redb::TableDefinition::new("t");

	fn file_state(path: &Path) -> (Vec<u8>, std::time::SystemTime) {
		(
			std::fs::read(path).unwrap(),
			std::fs::metadata(path).unwrap().modified().unwrap(),
		)
	}

	#[tokio::test]
	async fn database_is_dropped_inside_close_and_a_retained_store_never_writes_after() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let path = dir.path().join("store.redb");
		let store = OwnedRedb::new(redb::Database::create(&path).unwrap(), "test store");
		store.bind_owner(owner.clone()).unwrap();
		store
			.run(|db| {
				let txn = db.begin_write().map_err(std::io::Error::other)?;
				txn.open_table(TABLE)
					.map_err(std::io::Error::other)?
					.insert(1, 2)
					.map_err(std::io::Error::other)?;
				txn.commit().map_err(std::io::Error::other)
			})
			.await
			.unwrap();

		owner.close(Duration::from_secs(5)).await.unwrap();
		let after_close = file_state(&path);

		// The retained store is revoked: no access, and dropping it later
		// cannot reach redb's writing destructor.
		let error = store.run(|_| Ok(())).await.unwrap_err();
		assert!(
			error
				.get_ref()
				.is_some_and(|e| e.is::<crate::storage::ownership::Closed>())
		);
		drop(store);
		assert_eq!(
			file_state(&path),
			after_close,
			"no redb write after the owner closed"
		);
	}

	#[tokio::test]
	async fn an_escaped_handle_fails_close_and_keeps_the_lock() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let store = OwnedRedb::new(
			redb::Database::create(dir.path().join("store.redb")).unwrap(),
			"test store",
		);
		store.bind_owner(owner.clone()).unwrap();
		let escaped = store.test_handle();

		let error = owner.close(Duration::from_secs(5)).await.unwrap_err();
		assert!(
			error
				.get_ref()
				.is_some_and(|e| e.is::<crate::storage::ownership::ShutdownIncomplete>())
		);
		let busy = DataDirectoryOwner::acquire_existing(dir.path()).unwrap_err();
		assert!(
			busy.get_ref()
				.is_some_and(|e| e.is::<crate::storage::ownership::DataDirectoryBusy>()),
			"a failed close must keep the lock"
		);
		drop(escaped);
	}
}
