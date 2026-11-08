//! ADR 0699 S3b: a replica's durable record of the row versions each parent
//! write owns.
//!
//! `P[rsv, ordinal]` is an immutable intent naming one child's exact versions,
//! with a state: `Intent` (rows may be partly appended), `Complete` (every
//! version is durable and verified) or `Refused` (the reducer recorded the
//! parent Refused; the versions stay tentative until S5 removes them). Each
//! named version also has an owner entry, so a version can belong to one
//! `(rsv, ordinal)` only. Intent and owners commit together.
//!
//! These tables are authority, not cache: the index rebuild at open never
//! touches them, and an initialized store missing them refuses to open. They
//! are created on first use, so a store on today's authority never has them.

// Reached through the S3b child-write pipeline, which S3c dispatches.
#![cfg_attr(not(test), allow(dead_code))]

use super::*;
use serde::{Deserialize, Serialize};

pub(super) const PARENT_INTENTS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("parent_intents_v1");
pub(super) const PARENT_ROW_OWNERS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("parent_row_owners_v1");
pub(super) const META_PARENT_INTENTS_INITIALIZED: &str = "parent_intents_initialized";

/// The largest manifest one intent may carry (rows of one child).
pub(crate) const MAX_INTENT_ROWS: usize = crate::consensus::decisions::MAX_ROWS_PER_CHILD as usize;

/// One version a parent owns: the table, the row's key identity, the parent's
/// commit HLC and a digest of the row's canonical bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OwnedVersion {
	pub table: TableObjectId,
	pub key_identity: [u8; 32],
	pub hlc: HybridTimestamp,
	pub row_digest: [u8; 32],
}

impl OwnedVersion {
	fn version(&self) -> (TableObjectId, [u8; 32], HybridTimestamp) {
		(self.table, self.key_identity, self.hlc)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentIntentState {
	Intent,
	Complete,
	Refused,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentIntent {
	pub tenant: TenantId,
	pub rsv: u64,
	pub ordinal: u16,
	pub claim_digest: [u8; 32],
	pub range_start: u64,
	pub range_end: u64,
	pub content: [u8; 32],
	/// Strictly increasing by version; one entry per version.
	pub manifest: Vec<OwnedVersion>,
	pub state: ParentIntentState,
}

impl ParentIntent {
	/// The parts fixed at intent: everything but the state.
	fn same_intent(&self, other: &Self) -> bool {
		self.tenant == other.tenant
			&& self.rsv == other.rsv
			&& self.ordinal == other.ordinal
			&& self.claim_digest == other.claim_digest
			&& self.range_start == other.range_start
			&& self.range_end == other.range_end
			&& self.content == other.content
			&& self.manifest == other.manifest
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentRefusal {
	/// A different intent already exists at `(rsv, ordinal)`.
	ConflictingIntent,
	/// A named version is owned by another `(rsv, ordinal)`.
	ForeignOwner,
	/// A named version already exists in the index with no owner.
	ForeignVersion,
	/// The manifest names one version twice, is unsorted, empty or oversized.
	MalformedManifest,
	/// The intent was refused by a recorded decision; nothing may be added.
	Refused,
	/// No intent exists at `(rsv, ordinal)`.
	Missing,
	/// The intent is not in a state that allows this transition.
	WrongState,
}

fn intent_key(rsv: u64, ordinal: u16) -> [u8; 10] {
	let mut key = [0u8; 10];
	key[..8].copy_from_slice(&rsv.to_be_bytes());
	key[8..].copy_from_slice(&ordinal.to_be_bytes());
	key
}

fn owner_key(tenant: TenantId, version: &OwnedVersion) -> [u8; 72] {
	let mut key = [0u8; 72];
	key[..8].copy_from_slice(&tenant.get().to_le_bytes());
	key[8..24].copy_from_slice(version.table.as_bytes());
	key[24..56].copy_from_slice(&version.key_identity);
	key[56..64].copy_from_slice(&version.hlc.physical_ms.to_le_bytes());
	key[64..68].copy_from_slice(&version.hlc.logical.to_le_bytes());
	key[68..72].copy_from_slice(&version.hlc.node_id.to_le_bytes());
	key
}

/// ADR 0699 S5a: the owner key of a version, for the base activation that
/// retires an erased parent's owners.
pub(super) fn owner_key_for_base(tenant: TenantId, version: &OwnedVersion) -> [u8; 72] {
	owner_key(tenant, version)
}

/// ADR 0699 S5a: a stored intent, for the base activation.
pub(super) fn decode_intent_for_base(bytes: &[u8]) -> std::io::Result<ParentIntent> {
	decode_intent(bytes)
}

fn owner_value(rsv: u64, ordinal: u16) -> [u8; 10] {
	intent_key(rsv, ordinal)
}

/// The record-table key prefix of every physical copy of a version.
fn record_prefix(tenant: TenantId, version: &OwnedVersion) -> [u8; 56] {
	let mut prefix = [0u8; 56];
	prefix[..8].copy_from_slice(&tenant.get().to_le_bytes());
	prefix[8..40].copy_from_slice(&version.key_identity);
	prefix[40..48].copy_from_slice(&version.hlc.physical_ms.to_le_bytes());
	prefix[48..52].copy_from_slice(&version.hlc.logical.to_le_bytes());
	prefix[52..56].copy_from_slice(&version.hlc.node_id.to_le_bytes());
	prefix
}

fn decode_intent(bytes: &[u8]) -> std::io::Result<ParentIntent> {
	postcard::from_bytes(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

fn encode_intent(intent: &ParentIntent) -> std::io::Result<Vec<u8>> {
	postcard::to_stdvec(intent).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

/// Strictly increasing by `(table, key, hlc)`, non-empty and bounded.
pub(crate) fn manifest_is_canonical(manifest: &[OwnedVersion]) -> bool {
	!manifest.is_empty()
		&& manifest.len() <= MAX_INTENT_ROWS
		&& manifest
			.windows(2)
			.all(|pair| pair[0].version() < pair[1].version())
}

/// ADR 0699 S5b: the owner value of an intent, for the activation that
/// records the evidence of its retired versions.
pub(super) fn owner_value_for_base(rsv: u64, ordinal: u16) -> [u8; 10] {
	owner_value(rsv, ordinal)
}

/// ADR 0699 S5b: the indexed copies of a version, for the debt sweep.
pub(super) fn record_copies_for_base(
	records: &impl ReadableTable<&'static [u8], &'static [u8]>,
	tenant: TenantId,
	version: &OwnedVersion,
) -> std::io::Result<usize> {
	record_copies(records, tenant, version)
}

/// Physical copies of `version` in the record table, by prefix.
fn record_copies(
	records: &impl ReadableTable<&'static [u8], &'static [u8]>,
	tenant: TenantId,
	version: &OwnedVersion,
) -> std::io::Result<usize> {
	let prefix = record_prefix(tenant, version);
	let mut low = [0u8; RECORD_KEY_LEN];
	let mut high = [0xFFu8; RECORD_KEY_LEN];
	low[..56].copy_from_slice(&prefix);
	high[..56].copy_from_slice(&prefix);
	let mut copies = 0;
	for row in records
		.range(low.as_slice()..=high.as_slice())
		.map_err(redb_to_io)?
	{
		row.map_err(redb_to_io)?;
		copies += 1;
	}
	Ok(copies)
}

impl PersistentLocalIndex {
	/// Records the intent and its owner entries in one transaction, or returns
	/// the existing intent's state if an identical one is already recorded.
	/// Every named version must be unowned and absent from the index, or owned
	/// by this `(rsv, ordinal)` already.
	pub(crate) async fn begin_parent_intent(
		&self,
		intent: ParentIntent,
	) -> std::io::Result<Result<ParentIntentState, IntentRefusal>> {
		if !manifest_is_canonical(&intent.manifest) || intent.state != ParentIntentState::Intent {
			return Ok(Err(IntentRefusal::MalformedManifest));
		}
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let key = intent_key(intent.rsv, intent.ordinal);
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					meta.insert(META_PARENT_INTENTS_INITIALIZED, &1u64)
						.map_err(redb_to_io)?;
				}
				let mut intents = tx.open_table(PARENT_INTENTS_TABLE).map_err(redb_to_io)?;
				if let Some(existing) = intents.get(key.as_slice()).map_err(redb_to_io)? {
					let existing = decode_intent(existing.value())?;
					return Ok(if !existing.same_intent(&intent) {
						Err(IntentRefusal::ConflictingIntent)
					} else if existing.state == ParentIntentState::Refused {
						Err(IntentRefusal::Refused)
					} else {
						Ok(existing.state)
					});
				}
				let mut owners = tx.open_table(PARENT_ROW_OWNERS_TABLE).map_err(redb_to_io)?;
				let records = tx.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				let mine = owner_value(intent.rsv, intent.ordinal);
				for version in &intent.manifest {
					let owner = owner_key(intent.tenant, version);
					if let Some(current) = owners.get(owner.as_slice()).map_err(redb_to_io)? {
						if current.value() != mine.as_slice() {
							return Ok(Err(IntentRefusal::ForeignOwner));
						}
						continue;
					}
					if record_copies(&records, intent.tenant, version)? > 0 {
						return Ok(Err(IntentRefusal::ForeignVersion));
					}
				}
				drop(records);
				for version in &intent.manifest {
					owners
						.insert(
							owner_key(intent.tenant, version).as_slice(),
							mine.as_slice(),
						)
						.map_err(redb_to_io)?;
				}
				intents
					.insert(key.as_slice(), encode_intent(&intent)?.as_slice())
					.map_err(redb_to_io)?;
				drop(owners);
				drop(intents);
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(ParentIntentState::Intent))
			})
			.await
	}

	/// The intent at `(rsv, ordinal)`, if any.
	pub(crate) async fn parent_intent(
		&self,
		rsv: u64,
		ordinal: u16,
	) -> std::io::Result<Option<ParentIntent>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let intents = match tx.open_table(PARENT_INTENTS_TABLE) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
					Err(error) => return Err(redb_to_io(error)),
				};
				intents
					.get(intent_key(rsv, ordinal).as_slice())
					.map_err(redb_to_io)?
					.map(|value| decode_intent(value.value()))
					.transpose()
			})
			.await
	}

	/// Moves the intent from one of `from` to `to`, in one transaction.
	pub(crate) async fn set_parent_intent_state(
		&self,
		rsv: u64,
		ordinal: u16,
		from: &'static [ParentIntentState],
		to: ParentIntentState,
	) -> std::io::Result<Result<(), IntentRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let key = intent_key(rsv, ordinal);
				{
					let mut intents = match tx.open_table(PARENT_INTENTS_TABLE) {
						Ok(table) => table,
						Err(error) => return Err(redb_to_io(error)),
					};
					let Some(existing) = intents.get(key.as_slice()).map_err(redb_to_io)? else {
						return Ok(Err(IntentRefusal::Missing));
					};
					let mut intent = decode_intent(existing.value())?;
					drop(existing);
					if intent.state == to {
						return Ok(Ok(()));
					}
					if !from.contains(&intent.state) {
						return Ok(Err(IntentRefusal::WrongState));
					}
					intent.state = to;
					intents
						.insert(key.as_slice(), encode_intent(&intent)?.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}

	/// For each manifest version, its number of physical copies in the index.
	pub(crate) async fn owned_version_copies(
		&self,
		tenant: TenantId,
		manifest: Vec<OwnedVersion>,
	) -> std::io::Result<Vec<usize>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let records = tx.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				manifest
					.iter()
					.map(|version| record_copies(&records, tenant, version))
					.collect()
			})
			.await
	}

	/// Every intent in one of `states`, by `(rsv, ordinal)`.
	pub(crate) async fn parent_intents_in(
		&self,
		states: &'static [ParentIntentState],
	) -> std::io::Result<Vec<(u64, u16)>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let intents = match tx.open_table(PARENT_INTENTS_TABLE) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
					Err(error) => return Err(redb_to_io(error)),
				};
				let mut found = Vec::new();
				for row in intents.iter().map_err(redb_to_io)? {
					let (_, value) = row.map_err(redb_to_io)?;
					let intent = decode_intent(value.value())?;
					if states.contains(&intent.state) {
						found.push((intent.rsv, intent.ordinal));
					}
				}
				Ok(found)
			})
			.await
	}

	/// Test-only: overwrite a version's owner entry, to model inconsistent
	/// ownership the verification must refuse.
	#[cfg(test)]
	pub(crate) async fn set_version_owner_for_test(
		&self,
		tenant: TenantId,
		version: OwnedVersion,
		owner: (u64, u16),
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut owners = tx.open_table(PARENT_ROW_OWNERS_TABLE).map_err(redb_to_io)?;
					owners
						.insert(
							owner_key(tenant, &version).as_slice(),
							owner_value(owner.0, owner.1).as_slice(),
						)
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// The owner recorded for a version, if any.
	pub(crate) async fn version_owner(
		&self,
		tenant: TenantId,
		version: OwnedVersion,
	) -> std::io::Result<Option<(u64, u16)>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let owners = match tx.open_table(PARENT_ROW_OWNERS_TABLE) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
					Err(error) => return Err(redb_to_io(error)),
				};
				Ok(owners
					.get(owner_key(tenant, &version).as_slice())
					.map_err(redb_to_io)?
					.map(|value| {
						let bytes = value.value();
						(
							u64::from_be_bytes(bytes[..8].try_into().expect("owner value")),
							u16::from_be_bytes(bytes[8..10].try_into().expect("owner value")),
						)
					}))
			})
			.await
	}
}

/// Run at open: an initialized store missing either table has lost authority.
pub(super) fn check_parent_intent_authority(tx: &redb::WriteTransaction) -> std::io::Result<()> {
	let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
	let initialized = meta
		.get(META_PARENT_INTENTS_INITIALIZED)
		.map_err(redb_to_io)?
		.is_some();
	if !initialized {
		return Ok(());
	}
	let tables = tx
		.list_tables()
		.map_err(redb_to_io)?
		.map(|table| table.name().to_owned())
		.collect::<HashSet<_>>();
	if tables.contains(PARENT_INTENTS_TABLE.name())
		&& tables.contains(PARENT_ROW_OWNERS_TABLE.name())
	{
		Ok(())
	} else {
		Err(Error::new(
			ErrorKind::InvalidData,
			"parent intent authority is missing",
		))
	}
}

#[cfg(test)]
#[path = "parent_intents_tests.rs"]
mod tests;
