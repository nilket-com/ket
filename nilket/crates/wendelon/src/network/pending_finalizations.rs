use crate::consensus::TenantFinalization;
use crate::schema::{ClientWriteId, PRE_VERSIONING_FORMAT};
use crate::storage::finality_observation::{
	FinalityObservationRegistry, Unavailable, WorkKind, WorkScope,
};
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
	TableDefinition::new("pending_finalizations_by_write_id_v2");
const PENDING_FINALIZATIONS_BY_WRITE_ID_LEGACY_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("pending_finalizations_by_write_id_v1");
// Sidecar rather than a field on `PendingFinalizationRecord`: postcard encodes that
// record positionally, so widening it would make pre-ADR-542 pending witnesses
// unreadable. Presence means the parent mutation is DELETE; the twelve bytes are its
// three exact row-count outcomes in little-endian order.
const PENDING_FINALIZATION_DELETE_COUNTS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("pending_finalization_delete_counts_v1");
// ADR 85 store-level shape marker. This store has no header to carve a version
// out of, so the marker is durable metadata of its own, read before any record
// body is decoded. An **absent** marker is legacy version zero — every store
// written before this gate reports it, and refusing them would strand working
// deployments to explain a failure they have not had.
const PENDING_FINALIZATION_META_TABLE: TableDefinition<&str, u8> =
	TableDefinition::new("pending_finalization_meta_v1");
const PENDING_FINALIZATION_FORMAT_KEY: &str = "format_version";
/// Shape of the `postcard(PendingFinalizationRecord)` values this build writes.
const PENDING_FINALIZATION_FORMAT_VERSION: u8 = 1;
// The local secondary index scopes canonical identities by tenant. Primary
// records and their serialized format stay unchanged; open rebuilds this index.
const WRITE_ID_KEY_LEN: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PendingFinalizationId([u8; 32]);

impl PendingFinalizationId {
	pub fn for_record(write_id: ClientWriteId, finalization: &TenantFinalization) -> Self {
		// Hash the canonical idempotency identity only; TTL / pad
		// are not part of `ClientWriteId` identity (see
		// `src/schema/core.rs` and the architecture guard).
		let canonical = write_id.canonical_idempotency_bytes();
		Self(crate::hash::sha256::digest_many(&[
			&canonical,
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
	observation: Arc<FinalityObservationRegistry>,
	db: Arc<Database>,
	/// Shape this store's record values are known to carry.
	/// [`PRE_VERSIONING_FORMAT`] means unverifiable, not old.
	format_version: u8,
	write_lock: Mutex<()>,
	count: AtomicU64,
	#[cfg(test)]
	commit_gate: StdMutex<Option<ObservationCommitGate>>,
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
		Self::open_observed(path, FinalityObservationRegistry::new())
	}

	pub(crate) fn open_observed(
		path: impl Into<PathBuf>,
		observation: Arc<FinalityObservationRegistry>,
	) -> std::io::Result<Arc<Self>> {
		let path = path.into();
		if let Some(existing) = pending_store_registry()
			.lock()
			.expect("pending finalization registry poisoned")
			.get(&path)
			.and_then(Weak::upgrade)
		{
			if !Arc::ptr_eq(&existing.observation, &observation) {
				existing
					.observation
					.unavailable(Unavailable::UnknownProducer);
				observation.unavailable(Unavailable::UnknownProducer);
			}
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
		{
			let _table = write_txn
				.open_table(PENDING_FINALIZATION_DELETE_COUNTS_TABLE)
				.map_err(redb_to_io)?;
		}
		// Resolved before the self-heal loop below, which decodes every primary
		// value. A store whose shape this build cannot read must be refused
		// before its records are interpreted, not after.
		let format_version = {
			let meta = write_txn
				.open_table(PENDING_FINALIZATION_META_TABLE)
				.map_err(redb_to_io)?;
			let declared = meta
				.get(PENDING_FINALIZATION_FORMAT_KEY)
				.map_err(redb_to_io)?
				.map(|value| value.value());
			match declared {
				Some(version) if version > PENDING_FINALIZATION_FORMAT_VERSION => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"pending finalization store {} declares format version {version}, \
							 newer than this build accepts (supported: {}..={}); the store is \
							 intact but written by a different build, not corrupt",
							path.display(),
							PRE_VERSIONING_FORMAT,
							PENDING_FINALIZATION_FORMAT_VERSION
						),
					));
				}
				Some(version) => version,
				None => PRE_VERSIONING_FORMAT,
			}
		};
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
				let record = decode_pending_finalization_record(value.value(), format_version)?;
				initial_count = initial_count.saturating_add(1);
				let write_key = encode_write_id_key(record.finalization.tenant_id, record.write_id);
				by_write_id
					.insert(write_key.as_slice(), &record.id.as_bytes()[..])
					.map_err(redb_to_io)?;
			}
		}
		// Rebuild and retire the old namespace atomically with the same transaction.
		// All table handles above have ended before this deletion.
		write_txn
			.delete_table(PENDING_FINALIZATIONS_BY_WRITE_ID_LEGACY_TABLE)
			.map_err(redb_to_io)?;
		// Stamp only a store that holds nothing. Marking a populated legacy store
		// would assert that records written by an older build match this one's
		// shape — a claim nothing here can support, and exactly the false
		// assurance this record exists to remove. A legacy store stays legacy.
		let format_version = if format_version == PRE_VERSIONING_FORMAT && initial_count == 0 {
			let mut meta = write_txn
				.open_table(PENDING_FINALIZATION_META_TABLE)
				.map_err(redb_to_io)?;
			meta.insert(
				PENDING_FINALIZATION_FORMAT_KEY,
				PENDING_FINALIZATION_FORMAT_VERSION,
			)
			.map_err(redb_to_io)?;
			PENDING_FINALIZATION_FORMAT_VERSION
		} else {
			format_version
		};
		write_txn.commit().map_err(redb_to_io)?;
		let store = Arc::new(Self {
			observation,
			db,
			format_version,
			write_lock: Mutex::new(()),
			count: AtomicU64::new(initial_count),
			#[cfg(test)]
			commit_gate: StdMutex::new(None),
			#[cfg(test)]
			fail_next_upsert: StdMutex::new(None),
		});
		pending_store_registry()
			.lock()
			.expect("pending finalization registry poisoned")
			.insert(path, Arc::downgrade(&store));
		Ok(store)
	}

	#[cfg(test)]
	pub(crate) fn test_commit_gate(
		&self,
		phase: u8,
	) -> (
		std::sync::mpsc::Receiver<()>,
		std::sync::mpsc::SyncSender<()>,
	) {
		let (entered, ready) = std::sync::mpsc::sync_channel(1);
		let (release, wait) = std::sync::mpsc::sync_channel(1);
		*self.commit_gate.lock().unwrap() = Some(ObservationCommitGate {
			phase,
			entered,
			release: wait,
		});
		(ready, release)
	}

	pub async fn load_all(&self) -> std::io::Result<Vec<PendingFinalizationRecord>> {
		let db = self.db.clone();
		let format_version = self.format_version;
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let table = read_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let mut out = Vec::new();
			for entry in table.iter().map_err(redb_to_io)? {
				let (_key, value) = entry.map_err(redb_to_io)?;
				out.push(decode_pending_finalization_record(
					value.value(),
					format_version,
				)?);
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
		let format_version = self.format_version;
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let table = read_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let Some(value) = table.get(&id.as_bytes()[..]).map_err(redb_to_io)? else {
				return Ok(None);
			};
			let record = decode_pending_finalization_record(value.value(), format_version)?;
			Ok(Some(record))
		})
		.await
	}

	pub async fn find_by_write_id(
		&self,
		tenant_id: crate::schema::TenantId,
		write_id: ClientWriteId,
	) -> std::io::Result<Option<PendingFinalizationRecord>> {
		let db = self.db.clone();
		let format_version = self.format_version;
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let by_write_id = read_txn
				.open_table(PENDING_FINALIZATIONS_BY_WRITE_ID_TABLE)
				.map_err(redb_to_io)?;
			let primary = read_txn
				.open_table(PENDING_FINALIZATIONS_TABLE)
				.map_err(redb_to_io)?;
			let write_key = encode_write_id_key(tenant_id, write_id);
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
			let record = decode_pending_finalization_record(value.value(), format_version)?;
			// Defensive consistency check: secondary index value
			// must point at a record whose `write_id` shares the
			// canonical idempotency identity. TTL / pad may drift
			// across retries (the index is keyed on canonical
			// bytes only), so do not compare the full struct.
			if record.finalization.tenant_id != tenant_id
				|| record.write_id.canonical_idempotency_bytes()
					!= write_id.canonical_idempotency_bytes()
			{
				return Err(Error::new(
					ErrorKind::InvalidData,
					"pending-finalization secondary index returned a mismatched tenant or write_id",
				));
			}
			Ok(Some(record))
		})
		.await
	}

	pub async fn upsert(&self, record: PendingFinalizationRecord) -> std::io::Result<()> {
		self.upsert_with_delete_counts(record, None).await
	}

	pub async fn upsert_with_delete_counts(
		&self,
		record: PendingFinalizationRecord,
		delete_counts: Option<[u32; 3]>,
	) -> std::io::Result<()> {
		let observation = self.observation.begin(
			WorkScope::Tenant(record.finalization.tenant_id),
			WorkKind::Persistence,
		);
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
		#[cfg(test)]
		let mut gate = self
			.commit_gate
			.lock()
			.expect("observation test gate poisoned")
			.take();
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
				let write_key = encode_write_id_key(record.finalization.tenant_id, record.write_id);
				by_write_id
					.insert(write_key.as_slice(), &record.id.as_bytes()[..])
					.map_err(redb_to_io)?;
				if let Some(delete_counts) = delete_counts {
					let encoded_counts = encode_delete_counts(delete_counts);
					let mut contexts = write_txn
						.open_table(PENDING_FINALIZATION_DELETE_COUNTS_TABLE)
						.map_err(redb_to_io)?;
					contexts
						.insert(&record.id.as_bytes()[..], encoded_counts.as_slice())
						.map_err(redb_to_io)?;
				}
				!existed
			};
			let shadow = observation.before_durable_commit(*record.id.as_bytes());
			#[cfg(test)]
			observation_commit_gate(&mut gate, 0);
			write_txn.commit().map_err(redb_to_io)?;
			#[cfg(test)]
			observation_commit_gate(&mut gate, 1);
			shadow.committed(true);
			#[cfg(test)]
			observation_commit_gate(&mut gate, 2);
			Ok(inserted_new)
		})
		.await?;
		if inserted_new {
			self.count.fetch_add(1, Ordering::Relaxed);
		}
		Ok(())
	}

	pub async fn delete_counts(
		&self,
		id: PendingFinalizationId,
	) -> std::io::Result<Option<[u32; 3]>> {
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let read_txn = db.begin_read().map_err(redb_to_io)?;
			let table = read_txn
				.open_table(PENDING_FINALIZATION_DELETE_COUNTS_TABLE)
				.map_err(redb_to_io)?;
			let Some(value) = table.get(&id.as_bytes()[..]).map_err(redb_to_io)? else {
				return Ok(None);
			};
			decode_delete_counts(value.value()).map(Some)
		})
		.await
	}

	pub async fn remove(&self, id: PendingFinalizationId) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		let format_version = self.format_version;
		let observation = self.observation.clone();
		let owner = observation.begin(WorkScope::Node, WorkKind::Persistence);
		#[cfg(test)]
		let mut gate = self
			.commit_gate
			.lock()
			.expect("observation test gate poisoned")
			.take();
		let removed = spawn_blocking_io(move || {
			let _owner = owner;
			let removed_tenant;
			let write_txn = db.begin_write().map_err(redb_to_io)?;
			let removed_any = {
				let mut table = write_txn
					.open_table(PENDING_FINALIZATIONS_TABLE)
					.map_err(redb_to_io)?;
				let removed_record = match table.get(&id.as_bytes()[..]).map_err(redb_to_io)? {
					Some(value) => Some(decode_pending_finalization_record(
						value.value(),
						format_version,
					)?),
					None => None,
				};
				removed_tenant = removed_record
					.as_ref()
					.map(|record| record.finalization.tenant_id);
				let removed_any = table
					.remove(&id.as_bytes()[..])
					.map_err(redb_to_io)?
					.is_some();
				let replacement_id = if let Some(record) = removed_record.as_ref() {
					let canonical = record.write_id.canonical_idempotency_bytes();
					let mut replacement = None;
					for entry in table.iter().map_err(redb_to_io)? {
						let (_key, value) = entry.map_err(redb_to_io)?;
						let candidate =
							decode_pending_finalization_record(value.value(), format_version)?;
						if candidate.finalization.tenant_id == record.finalization.tenant_id
							&& candidate.write_id.canonical_idempotency_bytes() == canonical
						{
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
					let write_key =
						encode_write_id_key(record.finalization.tenant_id, record.write_id);
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
				let mut contexts = write_txn
					.open_table(PENDING_FINALIZATION_DELETE_COUNTS_TABLE)
					.map_err(redb_to_io)?;
				let _ = contexts.remove(&id.as_bytes()[..]).map_err(redb_to_io)?;
				removed_any
			};
			let shadow = removed_tenant.map(|tenant| {
				observation
					.begin(WorkScope::Tenant(tenant), WorkKind::Persistence)
					.before_durable_commit(*id.as_bytes())
			});
			#[cfg(test)]
			observation_commit_gate(&mut gate, 0);
			write_txn.commit().map_err(redb_to_io)?;
			#[cfg(test)]
			observation_commit_gate(&mut gate, 1);
			if let Some(shadow) = shadow {
				shadow.committed(false);
			}
			#[cfg(test)]
			observation_commit_gate(&mut gate, 2);
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

#[cfg(test)]
struct ObservationCommitGate {
	phase: u8,
	entered: std::sync::mpsc::SyncSender<()>,
	release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
fn observation_commit_gate(gate: &mut Option<ObservationCommitGate>, phase: u8) {
	if gate.as_ref().is_some_and(|gate| gate.phase == phase) {
		let gate = gate.take().expect("matched gate");
		gate.entered.send(()).expect("fixture receiver alive");
		gate.release.recv().expect("fixture release alive");
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
	let observation = crate::storage::finality_observation::current_work();
	tokio::task::spawn_blocking(move || {
		let _observation = observation;
		f()
	})
	.await
	.map_err(|err| Error::other(format!("blocking redb task failed: {err}")))?
}

fn redb_to_io(err: impl std::fmt::Display) -> std::io::Error {
	Error::other(format!("redb error: {err}"))
}

fn decode_pending_finalization_record(
	bytes: &[u8],
	format_version: u8,
) -> std::io::Result<PendingFinalizationRecord> {
	postcard::from_bytes(bytes).map_err(|err| {
		// Postcard encodes this record positionally, so a widened writer makes an
		// intact value undecodable. For an unmarked store that is the likeliest
		// reading, and saying so is the whole point of the marker.
		let detail = if format_version == PRE_VERSIONING_FORMAT {
			format!(
				"decode pending finalization: {err}; this store carries no format marker, \
				 so its records predate format versioning — they were very likely written \
				 by a build with a different record shape, and the value itself is intact"
			)
		} else {
			format!("decode pending finalization: {err}")
		};
		Error::new(ErrorKind::InvalidData, detail)
	})
}

fn encode_write_id_key(
	tenant_id: crate::schema::TenantId,
	write_id: ClientWriteId,
) -> [u8; WRITE_ID_KEY_LEN] {
	let mut key = [0u8; WRITE_ID_KEY_LEN];
	key[..8].copy_from_slice(&tenant_id.get().to_be_bytes());
	key[8..].copy_from_slice(&write_id.canonical_idempotency_bytes());
	key
}

fn encode_delete_counts(counts: [u32; 3]) -> [u8; 12] {
	let mut encoded = [0u8; 12];
	for (slot, count) in encoded.chunks_exact_mut(4).zip(counts) {
		slot.copy_from_slice(&count.to_le_bytes());
	}
	encoded
}

fn decode_delete_counts(encoded: &[u8]) -> std::io::Result<[u32; 3]> {
	if encoded.len() != 12 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"pending finalization delete-count width mismatch: expected=12 actual={}",
				encoded.len()
			),
		));
	}
	let mut counts = [0u32; 3];
	for (count, slot) in counts.iter_mut().zip(encoded.chunks_exact(4)) {
		*count = u32::from_le_bytes(slot.try_into().expect("four-byte count chunk"));
	}
	Ok(counts)
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
				record_count: 1,
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
			store
				.find_by_write_id(record.finalization.tenant_id, record.write_id)
				.await
				.unwrap(),
			Some(record.clone())
		);
		assert_eq!(store.load_all().await.unwrap(), vec![record.clone()]);
		store.remove(record.id).await.unwrap();
		assert_eq!(store.count(), 0);
		assert!(store.get(record.id).await.unwrap().is_none());
		assert!(
			store
				.find_by_write_id(record.finalization.tenant_id, record.write_id)
				.await
				.unwrap()
				.is_none()
		);
	}

	#[tokio::test]
	async fn delete_context_is_a_durable_sidecar_and_reclaims_with_its_witness() {
		let path = temp_path("delete-context");
		let record = record();
		let counts = [7, 11, 13];
		{
			let store = PendingFinalizationStore::open(path.clone()).unwrap();
			store
				.upsert_with_delete_counts(record.clone(), Some(counts))
				.await
				.unwrap();
			assert_eq!(store.delete_counts(record.id).await.unwrap(), Some(counts));
		}

		let reopened = PendingFinalizationStore::open(path).unwrap();
		assert_eq!(
			reopened.delete_counts(record.id).await.unwrap(),
			Some(counts)
		);
		reopened.remove(record.id).await.unwrap();
		assert_eq!(reopened.delete_counts(record.id).await.unwrap(), None);
	}

	#[tokio::test]
	async fn for_record_ignores_idempotency_ttl_and_pad() {
		// Same canonical identity (`client_uuid`, `sequence_no`),
		// drifted TTL / pad — must yield the same id. Otherwise a
		// retry that beats the in-memory idempotency cache (post-
		// eviction or post-restart) would produce a fresh
		// finalization id and miss `find_by_write_id`'s replay
		// path. Pre-canonicalization (Phase 8 follow-up), the id
		// was hashed off the full 32-byte struct, folding TTL /
		// pad into the id and breaking retry convergence.
		let base = ClientWriteId {
			client_uuid: [9; 16],
			sequence_no: 7,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		};
		let drifted = ClientWriteId {
			idempotency_ttl_ms: 30_000,
			_pad: 0xDEAD_BEEF,
			..base
		};
		let finalization = record().finalization;
		assert_eq!(
			PendingFinalizationId::for_record(base, &finalization),
			PendingFinalizationId::for_record(drifted, &finalization),
			"PendingFinalizationId must depend only on the canonical \
			 idempotency identity (`client_uuid` + `sequence_no`)"
		);
	}

	#[tokio::test]
	async fn find_by_write_id_matches_across_idempotency_ttl_drift() {
		// Upsert a record with one TTL; look it up with a retry
		// that carries the same canonical identity but a drifted
		// TTL. The redb secondary index key shape must depend only
		// on canonical bytes.
		let store = PendingFinalizationStore::open(temp_path("ttl-drift")).unwrap();
		let record = record();
		store.upsert(record.clone()).await.unwrap();
		let drifted_write_id = ClientWriteId {
			idempotency_ttl_ms: 12_345,
			_pad: 0xAAAA,
			..record.write_id
		};
		assert_eq!(
			store
				.find_by_write_id(record.finalization.tenant_id, drifted_write_id)
				.await
				.unwrap(),
			Some(record),
			"find_by_write_id must match across drifted TTL"
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
			store
				.find_by_write_id(record.finalization.tenant_id, record.write_id)
				.await
				.unwrap(),
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
				record_count: 1,
			},
		);

		store.upsert(first.clone()).await.unwrap();
		store.upsert(second.clone()).await.unwrap();
		assert_eq!(
			store
				.find_by_write_id(first.finalization.tenant_id, first.write_id)
				.await
				.unwrap(),
			Some(second.clone()),
			"newest record stays discoverable while both rows exist"
		);

		store.remove(second.id).await.unwrap();
		assert_eq!(
			store
				.find_by_write_id(first.finalization.tenant_id, first.write_id)
				.await
				.unwrap(),
			Some(first),
			"removing the indexed row must fall back to an older row with the same write_id"
		);
	}

	/// Open a store, then drop the process-wide registry entry so the next open
	/// genuinely re-reads the file rather than returning the cached handle.
	async fn reopen_store(path: &PathBuf) -> std::io::Result<Arc<PendingFinalizationStore>> {
		pending_store_registry()
			.lock()
			.expect("pending finalization registry poisoned")
			.remove(path);
		PendingFinalizationStore::open(path.clone())
	}

	#[tokio::test]
	async fn adr0085_fresh_pending_store_declares_its_format_version() {
		let path = temp_path("format-fresh");
		let store = PendingFinalizationStore::open(path.clone()).unwrap();
		assert_eq!(store.format_version, PENDING_FINALIZATION_FORMAT_VERSION);
		assert_ne!(
			store.format_version, PRE_VERSIONING_FORMAT,
			"a store this build created is not legacy"
		);
		drop(store);
		let _ = std::fs::remove_file(&path);
	}

	#[tokio::test]
	async fn adr0085_populated_legacy_store_is_not_stamped_as_current() {
		// A store with records but no marker predates the gate. Stamping it would
		// assert its records match this build's shape — the exact false assurance
		// ADR 85 exists to remove.
		let path = temp_path("format-legacy");
		let store = PendingFinalizationStore::open(path.clone()).unwrap();
		store.upsert(record()).await.unwrap();
		{
			let write_txn = store.db.begin_write().unwrap();
			{
				let mut meta = write_txn
					.open_table(PENDING_FINALIZATION_META_TABLE)
					.unwrap();
				meta.remove(PENDING_FINALIZATION_FORMAT_KEY).unwrap();
			}
			write_txn.commit().unwrap();
		}
		drop(store);

		let reopened = reopen_store(&path).await.unwrap();
		assert_eq!(
			reopened.format_version, PRE_VERSIONING_FORMAT,
			"a populated store without a marker stays legacy"
		);
		assert_eq!(reopened.load_all().await.unwrap().len(), 1);
		drop(reopened);
		let _ = std::fs::remove_file(&path);
	}

	#[tokio::test]
	async fn adr0085_pending_store_from_a_newer_build_is_refused_by_name() {
		let path = temp_path("format-newer");
		let store = PendingFinalizationStore::open(path.clone()).unwrap();
		store.upsert(record()).await.unwrap();
		{
			let write_txn = store.db.begin_write().unwrap();
			{
				let mut meta = write_txn
					.open_table(PENDING_FINALIZATION_META_TABLE)
					.unwrap();
				meta.insert(PENDING_FINALIZATION_FORMAT_KEY, 9u8).unwrap();
			}
			write_txn.commit().unwrap();
		}
		drop(store);

		let text = match reopen_store(&path).await {
			Ok(_) => panic!("a store version this build does not know must be refused"),
			Err(error) => error.to_string(),
		};
		assert!(text.contains('9'), "the refusal names the version: {text}");
		assert!(
			text.contains("not corrupt"),
			"an intact store is not reported as damage: {text}"
		);
		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn adr0085_unmarked_store_explains_a_shape_mismatch_rather_than_reporting_damage() {
		let error = decode_pending_finalization_record(&[0x01, 0x02], PRE_VERSIONING_FORMAT)
			.expect_err("a truncated positional value must not decode");
		let text = error.to_string();
		assert!(
			text.contains("predate format versioning"),
			"an unmarked store explains itself: {text}"
		);
		assert!(
			text.contains("intact"),
			"the operator is told the value is sound: {text}"
		);

		let marked =
			decode_pending_finalization_record(&[0x01, 0x02], PENDING_FINALIZATION_FORMAT_VERSION)
				.expect_err("the same bytes still fail under a marked store");
		assert!(
			!marked.to_string().contains("predate format versioning"),
			"a marked store must not blame a format it has already declared: {marked}"
		);
	}
	fn p2b_gate(
		store: &PendingFinalizationStore,
		phase: u8,
	) -> (
		std::sync::mpsc::Receiver<()>,
		std::sync::mpsc::SyncSender<()>,
	) {
		let (entered, ready) = std::sync::mpsc::sync_channel(1);
		let (release, wait) = std::sync::mpsc::sync_channel(1);
		*store.commit_gate.lock().unwrap() = Some(ObservationCommitGate {
			phase,
			entered,
			release: wait,
		});
		(ready, release)
	}

	async fn p2b_entered(ready: std::sync::mpsc::Receiver<()>) {
		tokio::time::timeout(
			std::time::Duration::from_secs(10),
			tokio::task::spawn_blocking(move || ready.recv().unwrap()),
		)
		.await
		.unwrap()
		.unwrap();
	}

	async fn p2b_no_active(store: &PendingFinalizationStore) {
		tokio::time::timeout(std::time::Duration::from_secs(10), async {
			while store.observation.test_state().0 != 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn adr629_p2b_cancellation_before_spawn_has_no_durable_effect() {
		let store = PendingFinalizationStore::open(temp_path("p2b-before-spawn")).unwrap();
		let lock = store.write_lock.lock().await;
		let worker = store.clone();
		let job = tokio::spawn(async move { worker.upsert(record()).await });
		tokio::time::timeout(std::time::Duration::from_secs(10), async {
			while store.observation.test_state().0 == 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		job.abort();
		let _ = job.await;
		drop(lock);
		assert_eq!(store.observation.test_state(), (0, 0, None));
		assert!(store.load_all().await.unwrap().is_empty());
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn adr629_p2b_cancelled_upsert_job_keeps_shadow_while_legacy_gauge_skips_update() {
		let store = PendingFinalizationStore::open(temp_path("p2b-commit-cancel")).unwrap();
		let (ready, release) = p2b_gate(&store, 0);
		let worker = store.clone();
		let job = tokio::spawn(async move { worker.upsert(record()).await });
		p2b_entered(ready).await;
		assert_eq!(store.observation.test_state(), (1, 0, None));
		job.abort();
		let _ = job.await;
		assert_eq!(store.observation.test_state().0, 1);
		release.send(()).unwrap();
		p2b_no_active(&store).await;
		assert_eq!(store.load_all().await.unwrap(), vec![record()]);
		assert_eq!(
			store.count(),
			0,
			"characterized pre-existing post-await gauge, not fixed here"
		);
		assert_eq!(store.observation.test_state(), (0, 1, None));
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn adr629_p2b_cancelled_after_commit_stays_active_until_shadow_install() {
		let store = PendingFinalizationStore::open(temp_path("p2b-after-commit")).unwrap();
		let (ready, release) = p2b_gate(&store, 1);
		let worker = store.clone();
		let job = tokio::spawn(async move { worker.upsert(record()).await });
		p2b_entered(ready).await;
		job.abort();
		let _ = job.await;
		assert_eq!(store.load_all().await.unwrap(), vec![record()]);
		assert_eq!(store.observation.test_state(), (1, 0, None));
		release.send(()).unwrap();
		p2b_no_active(&store).await;
		assert_eq!(store.observation.test_state(), (0, 1, None));
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn adr629_p2b_actual_redb_late_delete_shadow_cannot_erase_a_newer_commit() {
		let store = PendingFinalizationStore::open(temp_path("p2b-late-delete")).unwrap();
		store.upsert(record()).await.unwrap();
		let (ready, release) = p2b_gate(&store, 1);
		let worker = store.clone();
		let id = record().id;
		let job = tokio::spawn(async move { worker.remove(id).await });
		p2b_entered(ready).await;
		job.abort();
		let _ = job.await;
		assert!(store.load_all().await.unwrap().is_empty());
		assert!(store.observation.test_state().0 > 0);
		store.upsert(record()).await.unwrap();
		assert_eq!(store.load_all().await.unwrap(), vec![record()]);
		release.send(()).unwrap();
		p2b_no_active(&store).await;
		assert_eq!(store.observation.test_state(), (0, 1, None));
		assert!(
			store.observation.collect(&[], || unreachable!()).is_err(),
			"not an enabled complete observer"
		);
	}
}

#[cfg(test)]
#[path = "pending_finalizations_tenant_tests.rs"]
mod tenant_index_tests;
