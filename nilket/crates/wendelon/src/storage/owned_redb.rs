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
	/// The closer registered with the owner, kept so this store can be closed
	/// before its owner through the same drop state the owner joins.
	closer: OnceLock<Arc<RedbCloser>>,
	label: &'static str,
}

struct RedbCloser {
	db: Arc<Mutex<Option<Arc<Database>>>>,
	// The database's drop writes. Once started it runs to completion on its own
	// thread; a cancelled or timed-out close resumes waiting on the same drop
	// rather than treating it as done.
	dropping: Mutex<Option<Arc<crate::storage::ownership::WorkerJoins>>>,
	/// ADR 0106 (wendelon): the store can be closed by its owner and by itself,
	/// concurrently. This gate makes "take the database, check it is the only
	/// handle, launch its drop, publish the pending join" one transition, so a
	/// concurrent or resumed close sees the live database, the same pending
	/// join, or a finished one; never a taken database with nothing to join.
	/// An async lock: a waiting close stays cancellable (a timed-out owner
	/// close fails and keeps the directory).
	gate: tokio::sync::Mutex<()>,
	/// Test-only: a close pauses inside the transition, after taking the
	/// database and before publishing its drop.
	#[cfg(test)]
	pause: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
	label: &'static str,
}

impl OwnedRedb {
	pub(crate) fn new(db: Database, label: &'static str) -> Self {
		Self {
			db: Arc::new(Mutex::new(Some(Arc::new(db)))),
			owner: OnceLock::new(),
			closer: OnceLock::new(),
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
		let closer = Arc::new(RedbCloser {
			db: self.db.clone(),
			dropping: Mutex::new(None),
			gate: tokio::sync::Mutex::new(()),
			#[cfg(test)]
			pause: Mutex::new(None),
			label: self.label,
		});
		owner.register_worker(closer.clone())?;
		// Set before the owner: a bound store always has its closer.
		let _ = self.closer.set(closer);
		self.owner.set(owner).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("{} was bound concurrently", self.label),
			)
		})
	}

	/// ADR 0106 (wendelon): closes this one store before its owner closes,
	/// through the closer the owner registered, so the owner's close shares
	/// its drop: a drop begun here and not joined (this wait cancelled) is
	/// joined by the owner's close before the lock is released. An operation
	/// still running holds a handle, so the close fails and the store stays
	/// open and protected; the caller retries after that work ends. An unbound
	/// store has no owner to account for it and is refused.
	pub(crate) async fn close_and_join(&self) -> std::io::Result<()> {
		let closer = self.closer.get().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("{} is not bound to an owner", self.label),
			)
		})?;
		closer.close_and_join().await
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
		let joins = {
			let _gate = self.gate.lock().await;
			let pending = self
				.dropping
				.lock()
				.expect("owned redb closer poisoned")
				.clone();
			match pending {
				Some(joins) => joins,
				None => {
					let Some(db) = self.db.lock().expect("owned redb handle poisoned").take()
					else {
						// Under the gate with nothing pending: this store never
						// held a database to drop.
						return Ok(());
					};
					#[cfg(test)]
					if let Some((reached, resume)) = self
						.pause
						.lock()
						.expect("owned redb closer poisoned")
						.take()
					{
						let _ = reached.send(());
						let _ = resume.recv();
					}
					// The owner has drained every permit, so this must be the only
					// handle. Anything else means an access escaped the permit; fail
					// closed and keep the lock rather than let a later drop write
					// unowned.
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
					// The drop runs on its own thread; a failed launch puts the
					// database back, live and protected, instead of dropping it
					// here.
					let cell = Arc::new(Mutex::new(Some(db)));
					let dropped = cell.clone();
					let launched = std::thread::Builder::new()
						.name(format!("wendelon-close-{}", self.label.replace(' ', "-")))
						.spawn(move || {
							drop(dropped.lock().expect("owned redb drop poisoned").take())
						});
					let thread = match launched {
						Ok(thread) => thread,
						Err(error) => {
							let db = cell.lock().expect("owned redb drop poisoned").take();
							if let Some(db) = db {
								*self.db.lock().expect("owned redb handle poisoned") =
									Some(Arc::new(db));
							}
							return Err(error);
						}
					};
					let joins = crate::storage::ownership::WorkerJoins::new(vec![thread]);
					*self.dropping.lock().expect("owned redb closer poisoned") =
						Some(joins.clone());
					joins
				}
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

	/// ADR 0106 (wendelon): a store closed before its owner. An operation still
	/// running (its waiter cancelled) holds a handle: the store's close fails
	/// and keeps it, and the owner's close cannot finish or release the lock.
	/// Once the work ends, the store closes, and the owner then closes and
	/// releases the directory.
	#[tokio::test]
	async fn a_store_closed_before_its_owner_waits_for_running_work() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let store = Arc::new(OwnedRedb::new(
			redb::Database::create(dir.path().join("store.redb")).unwrap(),
			"test store",
		));
		store.bind_owner(owner.clone()).unwrap();
		let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
		let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
		let running = store.clone();
		let waiter = tokio::spawn(async move {
			running
				.run(move |_db| {
					entered_tx.send(()).unwrap();
					release_rx.recv().unwrap();
					Ok(())
				})
				.await
		});
		tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
			.await
			.unwrap();
		// The waiter is cancelled; its blocking work keeps running.
		waiter.abort();
		let _ = waiter.await;

		let refused = store.close_and_join().await.unwrap_err();
		assert!(
			refused.to_string().contains("outstanding handle"),
			"{refused}"
		);
		assert!(!store.is_closed(), "a failed close keeps the store");
		assert!(
			owner.close(Duration::from_millis(200)).await.is_err(),
			"the owner cannot close while the work runs"
		);
		assert!(
			DataDirectoryOwner::acquire_existing(dir.path()).is_err(),
			"the lock is kept"
		);

		release_tx.send(()).unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
		loop {
			match store.close_and_join().await {
				Ok(()) => break,
				Err(_) if tokio::time::Instant::now() < deadline => {
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
				Err(error) => panic!("never closed: {error}"),
			}
		}
		assert!(store.is_closed());
		owner.close(Duration::from_secs(5)).await.unwrap();
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}

	#[tokio::test]
	async fn an_unbound_store_is_not_closed_before_an_owner() {
		let dir = TestDir::new();
		let store = OwnedRedb::new(
			redb::Database::create(dir.path().join("store.redb")).unwrap(),
			"test store",
		);
		assert!(store.close_and_join().await.is_err());
		assert!(!store.is_closed());
	}

	/// ADR 0106 (wendelon): two closes of one store at once, its own and its
	/// owner's. The first pauses after taking the database and before
	/// publishing its drop. The owner's close cannot report success, and a
	/// timed-out one keeps the lock; once the first resumes, both end on the
	/// SAME drop, and only then is the directory released.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_close_racing_the_owners_close_shares_one_drop() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let store = Arc::new(OwnedRedb::new(
			redb::Database::create(dir.path().join("store.redb")).unwrap(),
			"test store",
		));
		store.bind_owner(owner.clone()).unwrap();
		let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
		let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
		*store.closer.get().unwrap().pause.lock().unwrap() = Some((reached_tx, resume_rx));
		let first = {
			let store = store.clone();
			tokio::spawn(async move { store.close_and_join().await })
		};
		tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
			.await
			.unwrap();
		// The database is taken and its drop not yet published.
		assert!(
			owner.close(Duration::from_millis(200)).await.is_err(),
			"the owner's close cannot succeed meanwhile"
		);
		assert!(
			DataDirectoryOwner::acquire_existing(dir.path()).is_err(),
			"the lock is kept"
		);
		let second = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(10)).await })
		};
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert!(
			!second.is_finished(),
			"the owner's close waits for the same drop"
		);
		resume_tx.send(()).unwrap();
		first.await.unwrap().unwrap();
		second.await.unwrap().unwrap();
		assert!(store.is_closed());
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}
}
