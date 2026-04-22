use crate::consensus::TenantFinalization;
use crate::schema::ClientWriteId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use tokio::sync::Mutex;

const PENDING_FINALIZATIONS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("pending_finalizations_v1");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PendingFinalizationId([u8; 32]);

impl PendingFinalizationId {
	pub fn for_record(write_id: ClientWriteId, finalization: &TenantFinalization) -> Self {
		Self(crate::hash::sha256::digest_many(&[
			bytemuck::bytes_of(&write_id),
			bytemuck::bytes_of(&finalization.tenant_id),
			finalization.key_identity.as_bytes(),
			bytemuck::bytes_of(&finalization.marker),
			&finalization.charged_bytes.to_le_bytes(),
		]))
	}

	pub fn as_bytes(&self) -> &[u8; 32] {
		&self.0
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingFinalizationRecord {
	pub id: PendingFinalizationId,
	pub write_id: ClientWriteId,
	pub await_finalization: bool,
	pub finalization: TenantFinalization,
}

impl PendingFinalizationRecord {
	pub fn new(
		write_id: ClientWriteId,
		await_finalization: bool,
		finalization: TenantFinalization,
	) -> Self {
		Self {
			id: PendingFinalizationId::for_record(write_id, &finalization),
			write_id,
			await_finalization,
			finalization,
		}
	}
}

pub struct PendingFinalizationStore {
	db: Arc<Database>,
	write_lock: Mutex<()>,
}

impl PendingFinalizationStore {
	pub fn open(path: impl Into<PathBuf>) -> std::io::Result<Arc<Self>> {
		let path = path.into();
		if let Some(existing) = pending_store_registry()
			.lock()
			.expect("pending finalization registry poisoned")
			.get(&path)
			.and_then(Weak::upgrade)
		{
			return Ok(existing);
		}

		let db = Arc::new(Database::create(path.clone()).map_err(redb_to_io)?);
		let write_txn = db.begin_write().map_err(redb_to_io)?;
		{
			let _table = write_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
		}
		write_txn.commit().map_err(redb_to_io)?;
		let store = Arc::new(Self {
			db,
			write_lock: Mutex::new(()),
		});
		pending_store_registry()
			.lock()
			.expect("pending finalization registry poisoned")
			.insert(path, Arc::downgrade(&store));
		Ok(store)
	}

	pub async fn load_all(&self) -> std::io::Result<Vec<PendingFinalizationRecord>> {
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let table = read_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let mut out = Vec::new();
			for entry in table.iter().map_err(redb_to_io)? {
				let (_key, value) = entry.map_err(redb_to_io)?;
				out.push(postcard::from_bytes(value.value()).map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("decode pending finalization: {err}"),
					)
				})?);
			}
			Ok(out)
		})
		.await
	}

	pub async fn get(
		&self,
		id: PendingFinalizationId,
	) -> std::io::Result<Option<PendingFinalizationRecord>> {
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let table = read_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let Some(value) = table.get(&id.as_bytes()[..]).map_err(redb_to_io)? else {
				return Ok(None);
			};
			let record = postcard::from_bytes(value.value()).map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("decode pending finalization: {err}"),
				)
			})?;
			Ok(Some(record))
		})
		.await
	}

	pub async fn upsert(&self, record: PendingFinalizationRecord) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let encoded = postcard::to_stdvec(&record)
				.map_err(|err| Error::other(format!("encode pending finalization: {err}")))?;
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			{
				let mut table = write_txn
					.open_table(PENDING_FINALIZATIONS_TABLE)
					.map_err(redb_to_io)?;
				table
					.insert(&record.id.as_bytes()[..], encoded.as_slice())
					.map_err(redb_to_io)?;
			}
			write_txn.commit().map_err(redb_to_io)
		})
		.await
	}

	pub async fn remove(&self, id: PendingFinalizationId) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			{
				let mut table = write_txn
					.open_table(PENDING_FINALIZATIONS_TABLE)
					.map_err(redb_to_io)?;
				let _ = table.remove(&id.as_bytes()[..]).map_err(redb_to_io)?;
			}
			write_txn.commit().map_err(redb_to_io)
		})
		.await
	}
}

pub fn pending_finalizations_path(dir: &Path) -> PathBuf {
	dir.join("pending_finalizations.redb")
}

async fn spawn_blocking_io<T>(
	f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T>
where
	T: Send + 'static,
{
	tokio::task::spawn_blocking(f)
		.await
		.map_err(|err| Error::other(format!("blocking redb task failed: {err}")))?
}

fn redb_to_io(err: impl std::fmt::Display) -> std::io::Error {
	Error::other(format!("redb error: {err}"))
}

fn pending_store_registry() -> &'static StdMutex<HashMap<PathBuf, Weak<PendingFinalizationStore>>> {
	static REGISTRY: OnceLock<StdMutex<HashMap<PathBuf, Weak<PendingFinalizationStore>>>> =
		OnceLock::new();
	REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::TenantFinalization;
	use crate::schema::{
		ClientWriteId, DurabilityLevel, FinalizationMarker, HybridTimestamp, Identity, TenantId,
	};

	fn temp_path(tag: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"wendelon-pending-finalizations-{tag}-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		))
	}

	fn record() -> PendingFinalizationRecord {
		PendingFinalizationRecord::new(
			ClientWriteId {
				client_uuid: [7; 16],
				sequence_no: 42,
				idempotency_ttl_ms: 60_000,
				_pad: 0,
			},
			true,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: 9,
					commit_hlc: HybridTimestamp {
						physical_ms: 123,
						logical: 4,
						node_id: 8,
					},
					payload_hash: [0xAB; 32],
					e_safe_index: 0,
					durability: DurabilityLevel::QUORUM,
					_pad: [0; 7],
				},
				tenant_id: TenantId::new(11),
				key_identity: Identity::blake3_tenant(TenantId::new(11), b"pending-key"),
				charged_bytes: 8192,
			},
		)
	}

	#[tokio::test]
	async fn pending_store_roundtrips_records() {
		let store = PendingFinalizationStore::open(temp_path("roundtrip")).unwrap();
		let record = record();
		store.upsert(record.clone()).await.unwrap();
		assert_eq!(store.get(record.id).await.unwrap(), Some(record.clone()));
		assert_eq!(store.load_all().await.unwrap(), vec![record.clone()]);
		store.remove(record.id).await.unwrap();
		assert!(store.get(record.id).await.unwrap().is_none());
	}
}
