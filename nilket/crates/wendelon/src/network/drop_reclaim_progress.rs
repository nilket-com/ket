//! ADR 0095 durable node-local bounded reclaim progress.

use crate::integration::compute::DropOperationId;
use crate::storage::manager::TableDropReclaimCursor;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use tokio::sync::Mutex;

const PROGRESS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("drop_reclaim_progress_v1");
const PROGRESS_FORMAT_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DropReclaimPhase {
	#[default]
	LocalL1,
	TieredL2,
	VerifyIndexes,
	Complete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DropReclaimProgress {
	pub format_version: u8,
	pub operation_id: DropOperationId,
	pub finalize_log_index: u64,
	pub owner_epoch: u64,
	pub phase: DropReclaimPhase,
	pub cursor: TableDropReclaimCursor,
	pub examined_total: u64,
	pub corruption_rebuild: bool,
}

impl DropReclaimProgress {
	pub(crate) fn start(
		operation_id: DropOperationId,
		finalize_log_index: u64,
		owner_epoch: u64,
	) -> Self {
		Self {
			format_version: PROGRESS_FORMAT_VERSION,
			operation_id,
			finalize_log_index,
			owner_epoch,
			phase: DropReclaimPhase::LocalL1,
			cursor: TableDropReclaimCursor::default(),
			examined_total: 0,
			corruption_rebuild: false,
		}
	}

	pub(crate) fn validate_identity(
		&self,
		operation_id: DropOperationId,
		finalize_log_index: u64,
	) -> std::io::Result<()> {
		if self.format_version != PROGRESS_FORMAT_VERSION {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"drop reclaim progress format version {} is unsupported",
					self.format_version
				),
			));
		}
		if self.operation_id != operation_id || self.finalize_log_index != finalize_log_index {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"drop reclaim progress identity disagrees with replicated debt",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DropReclaimProgressLoad {
	Absent(DropReclaimProgress),
	Present(DropReclaimProgress),
	CorruptReset(DropReclaimProgress),
}

impl DropReclaimProgressLoad {
	pub(crate) fn progress(self) -> DropReclaimProgress {
		match self {
			Self::Absent(progress) | Self::Present(progress) | Self::CorruptReset(progress) => {
				progress
			}
		}
	}
}

pub(crate) struct DropReclaimProgressStore {
	db: crate::storage::owned_redb::OwnedRedb,
	write_lock: Mutex<()>,
}

impl DropReclaimProgressStore {
	pub(crate) fn bind_owner(
		&self,
		owner: crate::storage::ownership::DataDirectoryOwner,
	) -> std::io::Result<()> {
		self.db.bind_owner(owner)
	}
	pub(crate) fn open(path: impl Into<PathBuf>) -> std::io::Result<Arc<Self>> {
		let path = path.into();
		let mut registry = stores()
			.lock()
			.expect("drop reclaim store registry poisoned");
		if let Some(existing) = registry
			.get(&path)
			.and_then(Weak::upgrade)
			.filter(|store| !store.db.is_closed())
		{
			return Ok(existing);
		}
		let db = Database::create(path.clone()).map_err(redb_to_io)?;
		let txn = db.begin_write().map_err(redb_to_io)?;
		{
			let _ = txn.open_table(PROGRESS).map_err(redb_to_io)?;
		}
		txn.commit().map_err(redb_to_io)?;
		let store = Arc::new(Self {
			db: crate::storage::owned_redb::OwnedRedb::new(db, "drop reclaim progress store"),
			write_lock: Mutex::new(()),
		});
		registry.insert(path, Arc::downgrade(&store));
		Ok(store)
	}

	pub(crate) async fn load_or_start(
		&self,
		operation_id: DropOperationId,
		finalize_log_index: u64,
		owner_epoch: u64,
	) -> std::io::Result<DropReclaimProgressLoad> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let key = operation_key(operation_id);
				let txn = db.begin_write().map_err(redb_to_io)?;
				let raw = {
					let table = txn.open_table(PROGRESS).map_err(redb_to_io)?;
					table
						.get(key.as_slice())
						.map_err(redb_to_io)?
						.map(|value| value.value().to_vec())
				};
				let outcome = match raw {
					None => DropReclaimProgressLoad::Absent(DropReclaimProgress::start(
						operation_id,
						finalize_log_index,
						owner_epoch,
					)),
					Some(bytes) => {
						match postcard::from_bytes::<DropReclaimProgress>(&bytes).and_then(
							|progress| {
								progress
									.validate_identity(operation_id, finalize_log_index)
									.map_err(|_| postcard::Error::DeserializeBadEncoding)?;
								Ok(progress)
							},
						) {
							Ok(progress) => DropReclaimProgressLoad::Present(progress),
							Err(error) => {
								let mut reset = DropReclaimProgress::start(
									operation_id,
									finalize_log_index,
									owner_epoch,
								);
								reset.corruption_rebuild = true;
								let encoded = postcard::to_stdvec(&reset).map_err(|encode| {
									Error::other(format!(
										"encode drop reclaim corruption marker: {encode}"
									))
								})?;
								{
									let mut table = txn.open_table(PROGRESS).map_err(redb_to_io)?;
									table
										.insert(key.as_slice(), encoded.as_slice())
										.map_err(redb_to_io)?;
								}
								tracing::error!(
									?operation_id,
									%error,
									"drop reclaim progress is corrupt; entering bounded reset-from-zero rebuild"
								);
								DropReclaimProgressLoad::CorruptReset(reset)
							}
						}
					}
				};
				txn.commit().map_err(redb_to_io)?;
				Ok(outcome)
			})
			.await
	}

	pub(crate) async fn store_current_epoch(
		&self,
		progress: DropReclaimProgress,
		expected_owner_epoch: u64,
	) -> std::io::Result<()> {
		if progress.owner_epoch != expected_owner_epoch {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"stale drop reclaim owner epoch cannot persist progress",
			));
		}
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let key = operation_key(progress.operation_id);
				let encoded = postcard::to_stdvec(&progress).map_err(|error| {
					Error::other(format!("encode drop reclaim progress: {error}"))
				})?;
				let txn = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = txn.open_table(PROGRESS).map_err(redb_to_io)?;
					table
						.insert(key.as_slice(), encoded.as_slice())
						.map_err(redb_to_io)?;
				}
				txn.commit().map_err(redb_to_io)
			})
			.await
	}

	#[cfg(test)]
	pub(crate) async fn inject_raw_for_test(
		&self,
		operation_id: DropOperationId,
		bytes: Vec<u8>,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let key = operation_key(operation_id);
				let txn = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = txn.open_table(PROGRESS).map_err(redb_to_io)?;
					table
						.insert(key.as_slice(), bytes.as_slice())
						.map_err(redb_to_io)?;
				}
				txn.commit().map_err(redb_to_io)
			})
			.await
	}
}

fn operation_key(operation_id: DropOperationId) -> [u8; 32] {
	let mut key = [0_u8; 32];
	key[..8].copy_from_slice(&operation_id.tenant_id.get().to_le_bytes());
	key[8..24].copy_from_slice(operation_id.table_object_id.as_bytes());
	key[24..].copy_from_slice(&operation_id.schema_generation.get().to_le_bytes());
	key
}

pub(crate) fn drop_reclaim_progress_path(dir: &Path) -> PathBuf {
	dir.join("drop_reclaim_progress.redb")
}

fn redb_to_io(error: impl std::fmt::Display) -> Error {
	Error::other(format!("drop reclaim progress store: {error}"))
}

fn stores() -> &'static StdMutex<HashMap<PathBuf, Weak<DropReclaimProgressStore>>> {
	static STORES: OnceLock<StdMutex<HashMap<PathBuf, Weak<DropReclaimProgressStore>>>> =
		OnceLock::new();
	STORES.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::integration::compute::SchemaGeneration;
	use crate::schema::{TableObjectId, TenantId};

	fn progress_path(tag: &str) -> (crate::test_support::TestDir, PathBuf) {
		let dir = crate::test_support::TestDir::new(&format!("wendelon-{tag}"));
		let path = dir.join("progress.redb");
		(dir, path)
	}

	fn operation() -> DropOperationId {
		DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([0x95; 16]),
			schema_generation: SchemaGeneration::new(4),
		}
	}

	#[tokio::test]
	async fn adr0095_absent_and_corrupt_progress_are_distinct_and_rebuild_is_explicit() {
		let (_dir, path) = progress_path("drop-progress-corrupt");
		let store = DropReclaimProgressStore::open(path).unwrap();
		let absent = store.load_or_start(operation(), 11, 7).await.unwrap();
		assert!(matches!(absent, DropReclaimProgressLoad::Absent(_)));
		store
			.inject_raw_for_test(operation(), vec![0xFF, 0x01])
			.await
			.unwrap();
		let corrupt = store.load_or_start(operation(), 11, 7).await.unwrap();
		assert!(matches!(corrupt, DropReclaimProgressLoad::CorruptReset(_)));
		assert!(corrupt.progress().corruption_rebuild);
	}

	#[tokio::test]
	async fn adr0095_stale_epoch_cannot_persist_progress() {
		let (_dir, path) = progress_path("drop-progress-epoch");
		let store = DropReclaimProgressStore::open(path).unwrap();
		let progress = DropReclaimProgress::start(operation(), 11, 7);
		let error = store.store_current_epoch(progress, 8).await.unwrap_err();
		assert_eq!(error.kind(), ErrorKind::PermissionDenied);
	}

	#[tokio::test]
	async fn adr0095_progress_reopens_and_old_epoch_cursor_remains_resumable() {
		let (_dir, path) = progress_path("drop-progress-reopen");
		let store = DropReclaimProgressStore::open(path.clone()).unwrap();
		let mut progress = DropReclaimProgress::start(operation(), 11, 7);
		progress.phase = DropReclaimPhase::TieredL2;
		progress.cursor = TableDropReclaimCursor {
			after_slab_id: Some(crate::schema::SlabId::new(19)),
			upper_bound_slab_id: Some(crate::schema::SlabId::new(40)),
		};
		progress.examined_total = 19;
		store.store_current_epoch(progress, 7).await.unwrap();
		drop(store);

		let reopened = DropReclaimProgressStore::open(path).unwrap();
		let loaded = reopened.load_or_start(operation(), 11, 8).await.unwrap();
		let loaded = match loaded {
			DropReclaimProgressLoad::Present(progress) => progress,
			other => panic!("durable progress must reopen as present, got {other:?}"),
		};
		assert_eq!(loaded.owner_epoch, 7, "reopen does not rewrite provenance");
		assert_eq!(loaded.phase, DropReclaimPhase::TieredL2);
		assert_eq!(
			loaded.cursor.after_slab_id,
			Some(crate::schema::SlabId::new(19))
		);
		assert_eq!(loaded.examined_total, 19);
	}
}
