//! ADR 542 durable terminal mutation outcomes.
//!
//! Pending finalizations remain in their purpose-built store. This ledger begins at
//! the terminal/expiry boundary and retains the exact typed response so restart replay
//! does not depend on the process-lifetime DashMaps.

use crate::schema::{ClientWriteId, TenantId, WriteResponseMeta};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const OUTCOMES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("mutation_outcomes_v1");
const OUTCOME_EXPIRY: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("mutation_outcome_expiry_v1");
const EXPIRED: TableDefinition<&[u8], u64> = TableDefinition::new("mutation_expired_v1");
const WRITE_KEY_LEN: usize = 32;
pub(crate) const OUTCOME_CAPACITY: usize = 4_096;
pub(crate) const EXPIRED_TOMBSTONE_CAPACITY: usize = 4_096;
pub(crate) const EXPIRED_TOMBSTONE_HORIZON_MS: u64 = 15 * 60 * 1_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableMutationOutcome {
	pub(crate) write_id: ClientWriteId,
	pub(crate) tenant_id: TenantId,
	#[serde(with = "serde_bytes")]
	response: Vec<u8>,
	#[serde(default)]
	finalization: Option<Vec<u8>>,
	pub(crate) delete_counts: Option<[u32; 3]>,
	pub(crate) expires_at_ms: u64,
}

impl DurableMutationOutcome {
	pub(crate) fn new(
		write_id: ClientWriteId,
		tenant_id: TenantId,
		response: WriteResponseMeta,
		finalization: Option<crate::schema::FinalizationMarker>,
		delete_counts: Option<[u32; 3]>,
	) -> Self {
		let ttl = u64::from(write_id.idempotency_ttl_ms.clamp(1_000, 300_000));
		Self {
			write_id,
			tenant_id,
			response: bytemuck::bytes_of(&response).to_vec(),
			finalization: finalization.map(|marker| bytemuck::bytes_of(&marker).to_vec()),
			delete_counts,
			expires_at_ms: now_ms().saturating_add(ttl),
		}
	}

	pub(crate) fn finalization(
		&self,
	) -> std::io::Result<Option<crate::schema::FinalizationMarker>> {
		self.finalization
			.as_deref()
			.map(|bytes| {
				if bytes.len() != std::mem::size_of::<crate::schema::FinalizationMarker>() {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"durable finalization marker width mismatch",
					));
				}
				Ok(*bytemuck::from_bytes(bytes))
			})
			.transpose()
	}

	pub(crate) fn response(&self) -> std::io::Result<WriteResponseMeta> {
		if self.response.len() != std::mem::size_of::<WriteResponseMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"durable mutation response width mismatch: expected={} actual={}",
					std::mem::size_of::<WriteResponseMeta>(),
					self.response.len()
				),
			));
		}
		Ok(*bytemuck::from_bytes(&self.response))
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MutationResolution {
	Present(DurableMutationOutcome),
	Expired,
	Unknown,
}

pub(crate) struct MutationOutcomeStore {
	db: Arc<Database>,
	write_lock: Mutex<()>,
}

impl MutationOutcomeStore {
	pub(crate) fn open(path: impl Into<PathBuf>) -> std::io::Result<Arc<Self>> {
		let path = path.into();
		let mut registry = outcome_store_registry()
			.lock()
			.expect("mutation outcome registry poisoned");
		if let Some(existing) = registry.get(&path).and_then(Weak::upgrade) {
			return Ok(existing);
		}
		// Keep the registry lock through open/publication. Two concurrent node fixtures
		// may share a profile path; releasing it after the lookup lets both call redb
		// `Database::create`, and the loser reports "Database already open" despite the
		// process intentionally sharing one store.
		let db = Arc::new(Database::create(path.clone()).map_err(redb_to_io)?);
		let txn = db.begin_write().map_err(redb_to_io)?;
		{
			let _ = txn.open_table(OUTCOMES).map_err(redb_to_io)?;
			let _ = txn.open_table(OUTCOME_EXPIRY).map_err(redb_to_io)?;
			let _ = txn.open_table(EXPIRED).map_err(redb_to_io)?;
		}
		txn.commit().map_err(redb_to_io)?;
		let store = Arc::new(Self {
			db,
			write_lock: Mutex::new(()),
		});
		registry.insert(path, Arc::downgrade(&store));
		Ok(store)
	}

	pub(crate) async fn upsert(&self, outcome: DurableMutationOutcome) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let encoded = postcard::to_stdvec(&outcome)
				.map_err(|err| Error::other(format!("encode mutation outcome: {err}")))?;
			let txn = db.begin_write().map_err(redb_to_io)?;
			{
				let key = key(outcome.write_id, outcome.tenant_id);
				let mut outcomes = txn.open_table(OUTCOMES).map_err(redb_to_io)?;
				let mut expiry = txn.open_table(OUTCOME_EXPIRY).map_err(redb_to_io)?;
				if let Some(previous) = outcomes.get(key.as_slice()).map_err(redb_to_io)? {
					let previous = decode(previous.value())?;
					let previous_expiry = expiry_key(previous.expires_at_ms, key);
					let _ = expiry
						.remove(previous_expiry.as_slice())
						.map_err(redb_to_io)?;
				}
				outcomes
					.insert(key.as_slice(), encoded.as_slice())
					.map_err(redb_to_io)?;
				let outcome_expiry = expiry_key(outcome.expires_at_ms, key);
				let empty: &[u8] = &[];
				expiry
					.insert(outcome_expiry.as_slice(), empty)
					.map_err(redb_to_io)?;
				let mut expired = txn.open_table(EXPIRED).map_err(redb_to_io)?;
				let _ = expired.remove(key.as_slice()).map_err(redb_to_io)?;
				prune_outcomes(
					&mut outcomes,
					&mut expiry,
					&mut expired,
					now_ms(),
					OUTCOME_CAPACITY,
				)?;
				prune_expired(&mut expired, now_ms())?;
			}
			txn.commit().map_err(redb_to_io)
		})
		.await
	}

	pub(crate) async fn resolve(
		&self,
		write_id: ClientWriteId,
		tenant_id: TenantId,
	) -> std::io::Result<MutationResolution> {
		let _guard = self.write_lock.lock().await;
		let db = self.db.clone();
		spawn_blocking_io(move || {
			let now = now_ms();
			let key = key(write_id, tenant_id);
			let txn = db.begin_write().map_err(redb_to_io)?;
			let resolution = {
				let mut outcomes = txn.open_table(OUTCOMES).map_err(redb_to_io)?;
				let mut expiry = txn.open_table(OUTCOME_EXPIRY).map_err(redb_to_io)?;
				let decoded = outcomes
					.get(key.as_slice())
					.map_err(redb_to_io)?
					.map(|value| decode(value.value()))
					.transpose()?;
				match decoded {
					Some(outcome) if outcome.expires_at_ms > now => {
						MutationResolution::Present(outcome)
					}
					Some(_) => {
						let removed = outcomes
							.remove(key.as_slice())
							.map_err(redb_to_io)?
							.expect("decoded mutation outcome remains present");
						let removed = decode(removed.value())?;
						let expiry_key = expiry_key(removed.expires_at_ms, key);
						let _ = expiry.remove(expiry_key.as_slice()).map_err(redb_to_io)?;
						let mut expired = txn.open_table(EXPIRED).map_err(redb_to_io)?;
						expired.insert(key.as_slice(), now).map_err(redb_to_io)?;
						prune_expired(&mut expired, now)?;
						MutationResolution::Expired
					}
					None => {
						let mut expired = txn.open_table(EXPIRED).map_err(redb_to_io)?;
						prune_expired(&mut expired, now)?;
						if expired.get(key.as_slice()).map_err(redb_to_io)?.is_some() {
							MutationResolution::Expired
						} else if operation_identity_is_expired(write_id, now) {
							// Toron mutation identities use the SQL/Rune UUIDv7 query id.
							// Its issuance time is self-authenticating identity material, so
							// expiry remains provable even after bounded detail and tombstone
							// retention have both reclaimed their rows. Without this check an
							// old replay becomes indistinguishable from a new mutation at the
							// retention horizon and can double-apply.
							MutationResolution::Expired
						} else {
							MutationResolution::Unknown
						}
					}
				}
			};
			txn.commit().map_err(redb_to_io)?;
			Ok(resolution)
		})
		.await
	}
}

fn outcome_store_registry() -> &'static StdMutex<HashMap<PathBuf, Weak<MutationOutcomeStore>>> {
	static REGISTRY: OnceLock<StdMutex<HashMap<PathBuf, Weak<MutationOutcomeStore>>>> =
		OnceLock::new();
	REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn operation_identity_is_expired(write_id: ClientWriteId, now_ms: u64) -> bool {
	let Some(issued_at_ms) = uuid_v7_unix_millis(write_id.client_uuid) else {
		// Legacy SDK identities are UUIDv4 session ids. They retain their historical
		// cache semantics; ADR 542's permanent post-reclaim rejection applies to the
		// Toron SQL/Rune mutation identities, which are UUIDv7 by construction.
		return false;
	};
	let ttl = u64::from(write_id.idempotency_ttl_ms.clamp(1_000, 300_000));
	issued_at_ms.saturating_add(ttl) <= now_ms
}

fn uuid_v7_unix_millis(bytes: [u8; 16]) -> Option<u64> {
	if bytes[6] >> 4 != 7 || bytes[8] >> 6 != 2 {
		return None;
	}
	Some(
		(u64::from(bytes[0]) << 40)
			| (u64::from(bytes[1]) << 32)
			| (u64::from(bytes[2]) << 24)
			| (u64::from(bytes[3]) << 16)
			| (u64::from(bytes[4]) << 8)
			| u64::from(bytes[5]),
	)
}

fn prune_expired(table: &mut redb::Table<'_, &[u8], u64>, now: u64) -> std::io::Result<()> {
	let horizon = now.saturating_sub(EXPIRED_TOMBSTONE_HORIZON_MS);
	let mut rows = table
		.iter()
		.map_err(redb_to_io)?
		.map(|entry| {
			let (key, value) = entry.map_err(redb_to_io)?;
			Ok((key.value().to_vec(), value.value()))
		})
		.collect::<std::io::Result<Vec<_>>>()?;
	rows.sort_by_key(|(_, expired_at)| *expired_at);
	let capacity_drop = rows.len().saturating_sub(EXPIRED_TOMBSTONE_CAPACITY);
	for (index, (key, expired_at)) in rows.into_iter().enumerate() {
		if index < capacity_drop || expired_at <= horizon {
			let _ = table.remove(key.as_slice()).map_err(redb_to_io)?;
		}
	}
	Ok(())
}

fn prune_outcomes(
	outcomes: &mut redb::Table<'_, &[u8], &[u8]>,
	expiry: &mut redb::Table<'_, &[u8], &[u8]>,
	expired: &mut redb::Table<'_, &[u8], u64>,
	now: u64,
	capacity: usize,
) -> std::io::Result<()> {
	let mut capacity_drop = outcomes
		.len()
		.map_err(redb_to_io)?
		.saturating_sub(u64::try_from(capacity).unwrap_or(u64::MAX));
	loop {
		let Some((expiry_row, outcome_key, expires_at)) = oldest_outcome_expiry(expiry)? else {
			break;
		};
		if capacity_drop == 0 && expires_at > now {
			break;
		}
		let _ = expiry.remove(expiry_row.as_slice()).map_err(redb_to_io)?;
		let _ = outcomes
			.remove(outcome_key.as_slice())
			.map_err(redb_to_io)?;
		expired
			.insert(outcome_key.as_slice(), now)
			.map_err(redb_to_io)?;
		capacity_drop = capacity_drop.saturating_sub(1);
	}
	Ok(())
}

fn oldest_outcome_expiry(
	expiry: &redb::Table<'_, &[u8], &[u8]>,
) -> std::io::Result<Option<(Vec<u8>, [u8; WRITE_KEY_LEN], u64)>> {
	let mut rows = expiry.iter().map_err(redb_to_io)?;
	let Some(entry) = rows.next() else {
		return Ok(None);
	};
	let (key, _) = entry.map_err(redb_to_io)?;
	let encoded = key.value().to_vec();
	let (expires_at, outcome_key) = decode_expiry_key(&encoded)?;
	Ok(Some((encoded, outcome_key, expires_at)))
}

pub(crate) fn mutation_outcomes_path(dir: &Path) -> PathBuf {
	dir.join("mutation_outcomes.redb")
}

fn key(write_id: ClientWriteId, tenant_id: TenantId) -> [u8; WRITE_KEY_LEN] {
	let mut key = [0_u8; WRITE_KEY_LEN];
	key[..8].copy_from_slice(&tenant_id.get().to_le_bytes());
	key[8..].copy_from_slice(&write_id.canonical_idempotency_bytes());
	key
}

fn expiry_key(expires_at_ms: u64, outcome_key: [u8; WRITE_KEY_LEN]) -> [u8; 40] {
	let mut key = [0_u8; 40];
	key[..8].copy_from_slice(&expires_at_ms.to_be_bytes());
	key[8..].copy_from_slice(&outcome_key);
	key
}

fn decode_expiry_key(key: &[u8]) -> std::io::Result<(u64, [u8; WRITE_KEY_LEN])> {
	if key.len() != 40 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"mutation outcome expiry key width is {}, expected 40",
				key.len()
			),
		));
	}
	let expires_at_ms = u64::from_be_bytes(key[..8].try_into().expect("eight-byte expiry"));
	let mut outcome_key = [0_u8; WRITE_KEY_LEN];
	outcome_key.copy_from_slice(&key[8..]);
	Ok((expires_at_ms, outcome_key))
}

fn decode(bytes: &[u8]) -> std::io::Result<DurableMutationOutcome> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("decode mutation outcome: {err}"),
		)
	})
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

async fn spawn_blocking_io<T>(
	f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T>
where
	T: Send + 'static,
{
	tokio::task::spawn_blocking(f)
		.await
		.map_err(|err| Error::other(format!("blocking mutation outcome task failed: {err}")))?
}

fn redb_to_io(error: impl std::fmt::Display) -> std::io::Error {
	Error::other(format!("redb mutation outcome error: {error}"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		HybridTimestamp, MutationCommitKnowledge, MutationStopReason, NodeId, WriteStatus,
	};

	fn write_id(sequence_no: u64, ttl: u32) -> ClientWriteId {
		ClientWriteId {
			client_uuid: [7; 16],
			sequence_no,
			idempotency_ttl_ms: ttl,
			_pad: 0,
		}
	}

	fn v7_write_id(issued_at_ms: u64, ttl: u32) -> ClientWriteId {
		let mut client_uuid = [0_u8; 16];
		client_uuid[..6].copy_from_slice(&issued_at_ms.to_be_bytes()[2..]);
		client_uuid[6] = 0x70;
		client_uuid[8] = 0x80;
		ClientWriteId {
			client_uuid,
			sequence_no: 0,
			idempotency_ttl_ms: ttl,
			_pad: 0,
		}
	}

	fn response(id: ClientWriteId) -> WriteResponseMeta {
		WriteResponseMeta {
			status: WriteStatus::WRITE_DEADLINE_EXCEEDED,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch: 1,
			commit_hlc: HybridTimestamp {
				physical_ms: 1,
				logical: 0,
				node_id: 1,
			},
			leader_hint: NodeId {
				id: 1,
				_reserved: 0,
			},
			echoed_write_id: id,
			has_finalization: 0,
			_pad2: [0; 7],
		}
		.with_mutation_outcome(
			MutationCommitKnowledge::INDETERMINATE,
			MutationStopReason::DEADLINE,
		)
	}

	#[tokio::test]
	async fn durable_outcome_survives_reopen_with_both_axes() {
		let path = std::env::temp_dir().join(format!(
			"wendelon-mutation-outcomes-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		let id = write_id(1, 60_000);
		let store = MutationOutcomeStore::open(&path).unwrap();
		store
			.upsert(DurableMutationOutcome::new(
				id,
				TenantId::new(1),
				response(id),
				None,
				None,
			))
			.await
			.unwrap();
		drop(store);
		let reopened = MutationOutcomeStore::open(&path).unwrap();
		let MutationResolution::Present(outcome) =
			reopened.resolve(id, TenantId::new(1)).await.unwrap()
		else {
			panic!("outcome must survive restart");
		};
		let response = outcome.response().unwrap();
		assert_eq!(
			response.mutation_stop_reason(),
			MutationStopReason::DEADLINE
		);
		assert_eq!(
			response.mutation_commit_knowledge(),
			MutationCommitKnowledge::INDETERMINATE
		);
	}

	#[tokio::test]
	async fn reclaimed_v7_operation_never_becomes_a_new_mutation() {
		let path = std::env::temp_dir().join(format!(
			"wendelon-mutation-outcomes-expired-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		let now = now_ms();
		let id = v7_write_id(now.saturating_sub(2_000), 1_000);
		let store = MutationOutcomeStore::open(&path).unwrap();
		assert_eq!(
			store.resolve(id, TenantId::new(1)).await.unwrap(),
			MutationResolution::Expired,
			"bounded tombstone reclamation must never authorize an old UUIDv7 mutation"
		);
	}

	#[tokio::test]
	async fn tenant_scope_cannot_resolve_another_tenants_operation() {
		let path = std::env::temp_dir().join(format!(
			"wendelon-mutation-outcomes-tenant-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		let id = write_id(9, 60_000);
		let store = MutationOutcomeStore::open(&path).unwrap();
		store
			.upsert(DurableMutationOutcome::new(
				id,
				TenantId::new(1),
				response(id),
				None,
				None,
			))
			.await
			.unwrap();
		assert_eq!(
			store.resolve(id, TenantId::new(2)).await.unwrap(),
			MutationResolution::Unknown
		);
		store
			.upsert(DurableMutationOutcome::new(
				id,
				TenantId::new(2),
				response(id),
				None,
				Some([1, 2, 3]),
			))
			.await
			.unwrap();
		assert!(matches!(
			store.resolve(id, TenantId::new(1)).await.unwrap(),
			MutationResolution::Present(_)
		));
		assert!(matches!(
			store.resolve(id, TenantId::new(2)).await.unwrap(),
			MutationResolution::Present(_)
		));
	}

	#[test]
	fn terminal_capacity_evicts_oldest_into_expired_evidence() {
		let path = std::env::temp_dir().join(format!(
			"wendelon-mutation-outcomes-capacity-{}-{}.redb",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		let store = MutationOutcomeStore::open(path).unwrap();
		let tenant = TenantId::new(1);
		let txn = store.db.begin_write().unwrap();
		{
			let mut outcomes = txn.open_table(OUTCOMES).unwrap();
			let mut expiry = txn.open_table(OUTCOME_EXPIRY).unwrap();
			let mut expired = txn.open_table(EXPIRED).unwrap();
			for sequence in 0..3 {
				let id = write_id(sequence, 60_000);
				let mut outcome = DurableMutationOutcome::new(id, tenant, response(id), None, None);
				outcome.expires_at_ms = 10_000 + sequence;
				let encoded = postcard::to_stdvec(&outcome).unwrap();
				let key = key(id, tenant);
				outcomes.insert(key.as_slice(), encoded.as_slice()).unwrap();
				let expiry_key = expiry_key(outcome.expires_at_ms, key);
				let empty: &[u8] = &[];
				expiry.insert(expiry_key.as_slice(), empty).unwrap();
			}
			prune_outcomes(&mut outcomes, &mut expiry, &mut expired, 1, 2).unwrap();
			assert_eq!(outcomes.len().unwrap(), 2);
			assert!(
				expired
					.get(key(write_id(0, 60_000), tenant).as_slice())
					.unwrap()
					.is_some()
			);
		}
		txn.commit().unwrap();
	}
}
