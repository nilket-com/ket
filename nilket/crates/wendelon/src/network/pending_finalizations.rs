use crate::consensus::TenantFinalization;
use crate::schema::ClientWriteId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use tokio::sync::Mutex;

const PENDING_FINALIZATIONS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("pending_finalizations_v1");
const PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("pending_finalizations_by_write_id_v1");
const WRITE_ID_KEY_LEN: usize = std::mem::size_of::<ClientWriteId>();

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

	fn from_slice(bytes: &[u8]) -> std::io::Result<Self> {
		if bytes.len() != 32 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"pending finalization id width mismatch: expected=32 actual={}",
					bytes.len()
				),
			));
		}
		let mut out = [0u8; 32];
		out.copy_from_slice(bytes);
		Ok(Self(out))
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
	count: AtomicU64,
	#[cfg(test)]
	fail_next_upsert: StdMutex<Option<String>>,
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
		{
			let _table = write_txn
				.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
				.map_err(redb_to_io)?;
		}
		let mut initial_count = 0u64;
		{
			let primary = write_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let mut by_write_id = write_txn
				.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
				.map_err(redb_to_io)?;
			// Self-heal existing databases and guarantee the
			// secondary index matches the authoritative primary
			// table on every open.
			by_write_id.retain(|_, _| false).map_err(redb_to_io)?;
			for entry in primary.iter().map_err(redb_to_io)? {
				let (_key, value) = entry.map_err(redb_to_io)?;
				let record = decode_pending_finalization_record(value.value())?;
				initial_count = initial_count.saturating_add(1);
				let write_key = encode_write_id_key(record.write_id);
				by_write_id
					.insert(write_key.as_slice(), &record.id.as_bytes()[..])
					.map_err(redb_to_io)?;
			}
		}
		write_txn.commit().map_err(redb_to_io)?;
		let store = Arc::new(Self {
			db,
			write_lock: Mutex::new(()),
			count: AtomicU64::new(initial_count),
			#[cfg(test)]
			fail_next_upsert: StdMutex::new(None),
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
			let record = decode_pending_finalization_record(value.value())?;
			Ok(Some(record))
		})
		.await
	}

	pub async fn find_by_write_id(
		&self,
		write_id: ClientWriteId,
	) -> std::io::Result<Option<PendingFinalizationRecord>> {
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let by_write_id = read_txn
				.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
				.map_err(redb_to_io)?;
			let primary = read_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let write_key = encode_write_id_key(write_id);
			let Some(id_bytes) = by_write_id.get(write_key.as_slice()).map_err(redb_to_io)? else {
				return Ok(None);
			};
			let pending_id = PendingFinalizationId::from_slice(id_bytes.value())?;
			let Some(value) = primary
				.get(&pending_id.as_bytes()[..])
				.map_err(redb_to_io)?
			else {
				return Ok(None);
			};
			let record = decode_pending_finalization_record(value.value())?;
			if record.write_id != write_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"pending-finalization secondary index returned a mismatched write_id",
				));
			}
			Ok(Some(record))
		})
		.await
	}

	pub async fn upsert(&self, record: PendingFinalizationRecord) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		#[cfg(test)]
		if let Some(message) = self
			.fail_next_upsert
			.lock()
			.expect("pending finalization failpoint poisoned")
			.take()
		{
			return Err(Error::other(format!(
				"injected pending finalization upsert failure: {message}"
			)));
		}
		let db = self.db.clone();
		let inserted_new = spawn_blocking_io(move || {
			let encoded = postcard::to_stdvec(&record)
				.map_err(|err| Error::other(format!("encode pending finalization: {err}")))?;
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			let inserted_new = {
				let mut table = write_txn
					.open_table(PENDING_FINALIZATIONS_TABLE)
					.map_err(redb_to_io)?;
				let existed = table
					.get(&record.id.as_bytes()[..])
					.map_err(redb_to_io)?
					.is_some();
				table
					.insert(&record.id.as_bytes()[..], encoded.as_slice())
					.map_err(redb_to_io)?;
				let mut by_write_id = write_txn
					.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
					.map_err(redb_to_io)?;
				let write_key = encode_write_id_key(record.write_id);
				by_write_id
					.insert(write_key.as_slice(), &record.id.as_bytes()[..])
					.map_err(redb_to_io)?;
				!existed
			};
			write_txn.commit().map_err(redb_to_io)?;
			Ok(inserted_new)
		})
		.await?;
		if inserted_new {
			self.count.fetch_add(1, Ordering::Relaxed);
		}
		Ok(())
	}

	pub async fn remove(&self, id: PendingFinalizationId) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		let removed = spawn_blocking_io(move || {
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			let removed_any = {
				let mut table = write_txn
					.open_table(PENDING_FINALIZATIONS_TABLE)
					.map_err(redb_to_io)?;
				let removed_record = match table.get(&id.as_bytes()[..]).map_err(redb_to_io)? {
					Some(value) => Some(decode_pending_finalization_record(value.value())?),
					None => None,
				};
				let removed_any = table
					.remove(&id.as_bytes()[..])
					.map_err(redb_to_io)?
					.is_some();
				let replacement_id = if let Some(record) = removed_record.as_ref() {
					let mut replacement = None;
					for entry in table.iter().map_err(redb_to_io)? {
						let (_key, value) = entry.map_err(redb_to_io)?;
						let candidate = decode_pending_finalization_record(value.value())?;
						if candidate.write_id == record.write_id {
							replacement = Some(candidate.id);
							break;
						}
					}
					replacement
				} else {
					None
				};
				if let Some(record) = removed_record {
					let mut by_write_id = write_txn
						.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
						.map_err(redb_to_io)?;
					let write_key = encode_write_id_key(record.write_id);
					let indexed_id =
						match by_write_id.get(write_key.as_slice()).map_err(redb_to_io)? {
							Some(value) => Some(PendingFinalizationId::from_slice(value.value())?),
							None => None,
						};
					if let Some(indexed_id) = indexed_id {
						if indexed_id == record.id {
							if let Some(replacement_id) = replacement_id {
								by_write_id
									.insert(write_key.as_slice(), &replacement_id.as_bytes()[..])
									.map_err(redb_to_io)?;
							} else {
								let _ = by_write_id
									.remove(write_key.as_slice())
									.map_err(redb_to_io)?;
							}
						}
					}
				}
				removed_any
			};
			write_txn.commit().map_err(redb_to_io)?;
			Ok(removed_any)
		})
		.await?;
		if removed {
			self.count.fetch_sub(1, Ordering::Relaxed);
		}
		Ok(())
	}

	pub fn count(&self) -> u64 {
		self.count.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub(crate) fn fail_next_upsert_for_test(&self, message: impl Into<String>) {
		*self
			.fail_next_upsert
			.lock()
			.expect("pending finalization failpoint poisoned") = Some(message.into());
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

fn decode_pending_finalization_record(bytes: &[u8]) -> std::io::Result<PendingFinalizationRecord> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("decode pending finalization: {err}"),
		)
	})
}

fn encode_write_id_key(write_id: ClientWriteId) -> [u8; WRITE_ID_KEY_LEN] {
	let mut out = [0u8; WRITE_ID_KEY_LEN];
	out.copy_from_slice(bytemuck::bytes_of(&write_id));
	out
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
		assert_eq!(store.count(), 0);
		store.upsert(record.clone()).await.unwrap();
		assert_eq!(store.count(), 1);
		assert_eq!(store.get(record.id).await.unwrap(), Some(record.clone()));
		assert_eq!(
			store.find_by_write_id(record.write_id).await.unwrap(),
			Some(record.clone())
		);
		assert_eq!(store.load_all().await.unwrap(), vec![record.clone()]);
		store.remove(record.id).await.unwrap();
		assert_eq!(store.count(), 0);
		assert!(store.get(record.id).await.unwrap().is_none());
		assert!(
			store
				.find_by_write_id(record.write_id)
				.await
				.unwrap()
				.is_none()
		);
	}

	#[tokio::test]
	async fn open_rebuilds_write_id_index_for_legacy_rows() {
		let path = temp_path("legacy-rebuild");
		let record = record();
		let db = redb::Database::create(path.clone()).unwrap();
		let write_txn = db.begin_write().unwrap();
		{
			let mut primary = write_txn.open_table(PENDING_FINALIZATIONS_TABLE).unwrap();
			let encoded = postcard::to_stdvec(&record).unwrap();
			primary
				.insert(&record.id.as_bytes()[..], encoded.as_slice())
				.unwrap();
		}
		write_txn.commit().unwrap();
		drop(db);

		let store = PendingFinalizationStore::open(path).unwrap();
		assert_eq!(store.count(), 1);
		assert_eq!(
			store.find_by_write_id(record.write_id).await.unwrap(),
			Some(record)
		);
	}

	#[tokio::test]
	async fn remove_repoints_write_id_index_when_alternate_record_remains() {
		let store = PendingFinalizationStore::open(temp_path("remove-repoint")).unwrap();
		let first = record();
		let second = PendingFinalizationRecord::new(
			first.write_id,
			true,
			TenantFinalization {
				marker: FinalizationMarker {
					log_index: 0,
					term: 0,
					epoch: 10,
					commit_hlc: HybridTimestamp {
						physical_ms: 456,
						logical: 5,
						node_id: 9,
					},
					payload_hash: [0xCD; 32],
					e_safe_index: 0,
					durability: DurabilityLevel::QUORUM,
					_pad: [0; 7],
				},
				tenant_id: TenantId::new(11),
				key_identity: Identity::blake3_tenant(TenantId::new(11), b"pending-key"),
				charged_bytes: 16_384,
			},
		);

		store.upsert(first.clone()).await.unwrap();
		store.upsert(second.clone()).await.unwrap();
		assert_eq!(
			store.find_by_write_id(first.write_id).await.unwrap(),
			Some(second.clone()),
			"newest record stays discoverable while both rows exist"
		);

		store.remove(second.id).await.unwrap();
		assert_eq!(
			store.find_by_write_id(first.write_id).await.unwrap(),
			Some(first),
			"removing the indexed row must fall back to an older row with the same write_id"
		);
	}
}
