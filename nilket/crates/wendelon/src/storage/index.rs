use crate::schema::table::PlacementSemantics;
use crate::schema::{
	ByteCount, CompactionLevelIndex, HybridTimestamp, Identity, IndexObjectId, Locality,
	LogicalSchemaFingerprint, ManifestEpoch, PartitionId, RecordRowIndex, RowCount, SlabId,
	SupersessionMetadata, TableObjectId, TenantId, TenantKekId, TombstoneMetadata,
};
use crate::storage::RecordLocation;
use crate::storage::lsm::{LsmRunSummary, SidecarCoverage, SlabState};
use crate::storage::manager::TieredSlabPointer;
use crate::storage::ownership::DataDirectoryOwner;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

const RECORDS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slab_records_v1");
/// V1 layout: `payload_hash[32] || l2_uri_utf8`. Older rows had no
/// `is_encrypted` byte. Read-only; one-shot migrated into V2 the
/// first time a process opens an index that still has V1 rows.
const TIERED_SLABS_TABLE_V1: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v1");
/// V2 layout: `payload_hash[32] || flags_byte || flags2_byte || [optional metadata] ||
/// l2_uri_utf8`.
///
/// Flags:
/// - bit 0 = `is_encrypted`
/// - bit 1 = `tenant_id` present (8 LE bytes)
/// - bit 2 = `tenant_kek_id` present (8 LE bytes)
/// - bit 3 = `stats_format_version` present (2 LE bytes)
/// - bit 4 = `stats_block_len` present (4 LE bytes)
/// - bit 5 = `stats_block_hash128` present (16 raw bytes)
/// - bit 6 = earliest tombstone HLC present (16 LE bytes)
/// - bit 7 = tombstone metadata is tracked
///
/// Flags2:
/// - bit 0 = earliest supersession HLC present (16 LE bytes)
/// - bit 1 = supersession metadata is tracked
///
const TIERED_SLABS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("tiered_slabs_v2");
const SECONDARY_INDEX_SEGMENTS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("secondary_index_segments_v1");
const LSM_RUNS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("lsm_runs_v1");
/// A committed Retire this store has yet to apply, because an unresolved install names the slab.
const DEFERRED_RETIREMENTS_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("deferred_retirements_v1");
const LSM_INSTALL_INTENTS_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("lsm_install_intents_v1");
const META_LSM_INSTALL_INTENTS_INITIALIZED: &str = "lsm_install_intents_initialized_v1";
const META_LSM_CERTIFICATES_INITIALIZED: &str = "lsm_certified_selections_initialized_v1";
const META_TIER_CERTIFICATES_INITIALIZED: &str = "tier_certified_selections_initialized_v1";

/// ADR 0097 decision 4: durable evidence of deterministic maintenance failures.
///
/// Every deterministic ATTEMPT is recorded, not only the final quarantine: the
/// attempt count is what bounds the work, and a restart that lost it would let an
/// OOM cycle rebuild the same working set forever - which is the incident this
/// record exists for. Keys are `MaintenanceWorkKey` encodings; values are opaque
/// here and decoded by the policy layer, so a decode failure is a policy decision
/// rather than an index one.
const MAINTENANCE_RECEIPTS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("maintenance_failure_receipts_v1");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("storage_index_meta_v1");
const SNAPSHOT_CLEANUP_INTENTS_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("snapshot_cleanup_intents_v1");
const META_SNAPSHOT_CLEANUP_INITIALIZED: &str = "snapshot_cleanup_intents_initialized";
const META_CLEANUP_AUTHORITY_GAPS: &str = "cleanup_authority_gaps_v1";
const META_CLEANUP_HISTORY_INCOMPLETE: &str = "cleanup_history_incomplete_v1";
const SNAPSHOT_RETIREMENTS_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("snapshot_retirements_v1");
const META_SNAPSHOT_RETIREMENTS_INITIALIZED: &str = "snapshot_retirements_initialized_v1";
const SNAPSHOT_RETIREMENT_STATE_TABLE: TableDefinition<&str, &[u8]> =
	TableDefinition::new("snapshot_retirement_state_v1");
const META_SNAPSHOT_RETIREMENT_EPOCH_FLOOR: &str = "snapshot_retirement_epoch_floor_v1";
const METADATA_PENDING_TABLE: TableDefinition<u64, &[u8]> =
	TableDefinition::new("metadata_pending_tokens_v1");
const META_METADATA_PENDING_INITIALIZED: &str = "metadata_pending_initialized_v1";
const META_METADATA_TOKEN_FLOOR: &str = "metadata_token_floor_v1";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingMetadataTokens {
	pub supersession: std::collections::BTreeSet<u64>,
	pub tombstone: std::collections::BTreeSet<u64>,
	pub degrade_applied: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MetadataKind {
	Supersession,
	Tombstone,
}

impl PendingMetadataTokens {
	pub(crate) fn degrade(&self, pointer: &mut TieredSlabPointer) {
		if !self.supersession.is_empty() {
			pointer.supersession_metadata = SupersessionMetadata::Untracked;
		}
		if !self.tombstone.is_empty() {
			pointer.tombstone_metadata = TombstoneMetadata::Untracked;
		}
	}
	fn is_empty(&self) -> bool {
		self.supersession.is_empty() && self.tombstone.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SnapshotRetirementMark {
	pub epoch: u64,
	pub snapshot_index: Option<u64>,
	pub pointer: TieredSlabPointer,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SnapshotRetirementState {
	epoch: u64,
	committed: Vec<SlabId>,
}

/// Apply surviving tokens to exactly the pointer published by this transaction.
fn apply_pending_metadata_to_pointer(
	tx: &redb::WriteTransaction,
	pointer: &mut TieredSlabPointer,
	completed: Option<(SlabId, MetadataKind, u64)>,
) -> std::io::Result<()> {
	let mut table = tx.open_table(METADATA_PENDING_TABLE).map_err(redb_to_io)?;
	let slab = pointer.slab_id.get();
	let pending = table
		.get(&slab)
		.map_err(redb_to_io)?
		.map(|value| {
			postcard::from_bytes::<PendingMetadataTokens>(value.value())
				.map_err(|e| Error::new(ErrorKind::InvalidData, e))
		})
		.transpose()?;
	if let Some(mut pending) = pending {
		if let Some((id, kind, token)) = completed.filter(|(id, _, _)| *id == pointer.slab_id) {
			let removed = match kind {
				MetadataKind::Supersession => pending.supersession.remove(&token),
				MetadataKind::Tombstone => pending.tombstone.remove(&token),
			};
			if !removed {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("metadata token {token} missing for slab {}", id.get()),
				));
			}
		}
		if pending.is_empty() {
			table.remove(&slab).map_err(redb_to_io)?;
		} else {
			pending.degrade(pointer);
			pending.degrade_applied = true;
			let value =
				postcard::to_stdvec(&pending).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
			table.insert(&slab, value.as_slice()).map_err(redb_to_io)?;
		}
	} else if completed.is_some_and(|(id, _, _)| id == pointer.slab_id) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"completed metadata token row is missing",
		));
	}
	Ok(())
}

fn note_metadata_degradation(
	before: &TieredSlabPointer,
	after: &TieredSlabPointer,
	counts: &mut (u64, u64),
) {
	if before.supersession_metadata.is_tracked() && after.supersession_metadata.is_untracked() {
		counts.0 += 1;
	}
	if before.tombstone_metadata.is_tracked() && after.tombstone_metadata.is_untracked() {
		counts.1 += 1;
	}
}
fn publish_metadata_degradation_counts(counts: (u64, u64)) {
	crate::metrics::global()
		.storage_supersession_metadata_degraded_total
		.add(counts.0);
	crate::metrics::global()
		.storage_tombstone_metadata_degraded_total
		.add(counts.1);
}

/// Durable retained-object accounting and publication evidence.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RetainedObjectIntent {
	pub bytes: Option<u64>,
	pub replacement_uri: Option<String>,
	pub replacement_bytes: Option<u64>,
	pub published_replacements: std::collections::BTreeSet<String>,
	pub retirement_source: Option<RetirementSource>,
	pub state: RetainedObjectIntentState,
}

impl RetainedObjectIntent {
	pub(crate) fn was_published(&self, replacement: &str) -> bool {
		self.published_replacements.contains(replacement)
			|| (self.state == RetainedObjectIntentState::ReplacementPublished
				&& self.replacement_uri.as_deref() == Some(replacement))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum RetirementSource {
	Snapshot(u64),
	Log(u64),
}

impl RetirementSource {
	pub(crate) fn index(self) -> u64 {
		match self {
			Self::Snapshot(index) | Self::Log(index) => index,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum RetainedObjectIntentState {
	Unresolved,
	ReplacementPublished,
	RetirementPublished,
	/// Fresh candidate; no committer call is permitted until this proof is cleared durably.
	CandidateNotSubmitted,
}

/// Record publication of predecessors and the retired object in one transaction.
/// A published retirement record with a source, and no tier pointer: what open reads as
/// `retired_tiered_slabs`.
fn retirement_evidence_stands(
	tx: &redb::WriteTransaction,
	slab_id: SlabId,
) -> std::io::Result<bool> {
	if tx
		.open_table(TIERED_SLABS_TABLE)
		.map_err(redb_to_io)?
		.get(&slab_id.get())
		.map_err(redb_to_io)?
		.is_some()
	{
		return Ok(false);
	}
	let cleanup = tx
		.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
		.map_err(redb_to_io)?;
	let prefix = slab_id.get().to_be_bytes();
	for row in cleanup.range(prefix.as_slice()..).map_err(redb_to_io)? {
		let (key, value) = row.map_err(redb_to_io)?;
		if !key.value().starts_with(&prefix) {
			break;
		}
		let intent: RetainedObjectIntent = postcard::from_bytes(value.value())
			.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if intent.state == RetainedObjectIntentState::RetirementPublished
			&& intent.retirement_source.is_some()
		{
			return Ok(true);
		}
	}
	Ok(false)
}

/// A retirement a log entry has decided, of exactly this generation, held back while an install
/// that names the slab is unresolved. Its rows, pointer and publication revision stay untouched,
/// so that install's evidence stays true whichever way it resolves.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DeferredRetirement {
	pub pointer: TieredSlabPointer,
	pub log_index: u64,
}

/// A retirement's row ends only with a retirement of the generation it names.
fn clear_deferred_retirement_of(
	tx: &redb::WriteTransaction,
	pointer: &TieredSlabPointer,
) -> std::io::Result<()> {
	let mut table = tx
		.open_table(DEFERRED_RETIREMENTS_TABLE)
		.map_err(redb_to_io)?;
	let same = table
		.get(&pointer.slab_id.get())
		.map_err(redb_to_io)?
		.map(|value| {
			postcard::from_bytes::<DeferredRetirement>(value.value())
				.map_err(|e| Error::new(ErrorKind::InvalidData, e))
		})
		.transpose()?
		.is_some_and(|row| row.pointer.same_object_generation(pointer));
	if same {
		table.remove(&pointer.slab_id.get()).map_err(redb_to_io)?;
	}
	Ok(())
}

/// A retry resolves only the intent covered by its authority: an older callback must leave a newer
/// snapshot or log retirement intact.
fn retirement_resolves_mark(
	mark: &SnapshotRetirementMark,
	last_control_log_index: Option<u64>,
	retirement_epoch: u64,
) -> bool {
	mark.epoch <= retirement_epoch
		&& match (last_control_log_index, mark.snapshot_index) {
			(Some(applied), Some(required)) => applied >= required,
			(_, None) => true,
			(None, Some(_)) => false,
		}
}

/// Remove a run row, keeping the manifest epoch it carried as a floor. Every removal comes here.
fn remove_lsm_run_row(
	tx: &redb::WriteTransaction,
	slab_id: SlabId,
) -> std::io::Result<Option<LsmRunSummary>> {
	let mut table = tx.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
	let removed = table
		.remove(&slab_id.get())
		.map_err(redb_to_io)?
		.map(|value| decode_lsm_run_summary(slab_id, value.value()))
		.transpose()?;
	drop(table);
	if let Some(run) = &removed {
		raise_lsm_manifest_epoch_floor(tx, run.manifest_epoch)?;
	}
	Ok(removed)
}

/// A reopen recovers the manifest epoch from its run rows and this floor, which only rises.
fn raise_lsm_manifest_epoch_floor(
	tx: &redb::WriteTransaction,
	epoch: crate::schema::ManifestEpoch,
) -> std::io::Result<()> {
	let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
	let floor = meta
		.get(META_LSM_MANIFEST_EPOCH_FLOOR)
		.map_err(redb_to_io)?
		.map(|value| value.value())
		.unwrap_or(0);
	if epoch.get() > floor {
		meta.insert(META_LSM_MANIFEST_EPOCH_FLOOR, &epoch.get())
			.map_err(redb_to_io)?;
	}
	Ok(())
}

/// Whether an unresolved install names this slab. Its commit still has to apply to that run, so
/// the run waits for the install to resolve.
fn install_intent_names_in<T: redb::ReadableTable<u64, &'static [u8]>>(
	intents: &T,
	slab_id: SlabId,
) -> std::io::Result<bool> {
	for row in intents.iter().map_err(redb_to_io)? {
		let (_, value) = row.map_err(redb_to_io)?;
		let intent: LsmInstallIntent =
			serde_json::from_slice(value.value()).map_err(Error::other)?;
		let commit = &intent.commit;
		if commit.slabs_to_retire.contains(&slab_id)
			|| commit.slabs_to_add.iter().any(|run| run.slab_id == slab_id)
		{
			return Ok(true);
		}
	}
	Ok(false)
}

/// A tier retirement is this slab's terminal state, so a Live run of it ends with the same
/// transaction: it has no backing left to compact, and GC has no file to claim. Any other state
/// means LSM already owns the slab, and keeps its own lifecycle; so does a run an unresolved
/// install names, until it resolves. True when this leaves no such run.
fn dispose_retired_live_run(tx: &redb::WriteTransaction, slab_id: SlabId) -> std::io::Result<bool> {
	if install_intent_names_in(
		&tx.open_table(LSM_INSTALL_INTENTS_TABLE)
			.map_err(redb_to_io)?,
		slab_id,
	)? {
		return Ok(false);
	}
	let live = {
		let table = tx.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
		let run = table
			.get(&slab_id.get())
			.map_err(redb_to_io)?
			.map(|value| decode_lsm_run_summary(slab_id, value.value()))
			.transpose()?;
		run.is_some_and(|run| run.state.is_live())
	};
	if live {
		remove_lsm_run_row(tx, slab_id)?;
	}
	Ok(true)
}

fn persist_retirement_records(
	tx: &redb::WriteTransaction,
	pointer: &TieredSlabPointer,
	known_bytes: Option<u64>,
	evidence: Vec<(String, RetainedObjectIntent)>,
	source: RetirementSource,
) -> std::io::Result<(RetainedObjectIntent, Vec<(String, RetainedObjectIntent)>)> {
	let mut table = tx
		.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
		.map_err(redb_to_io)?;
	let mut applied_evidence = Vec::new();
	for (prior_uri, proposed) in evidence {
		let mut prior_key = pointer.slab_id.get().to_be_bytes().to_vec();
		prior_key.extend_from_slice(prior_uri.as_bytes());
		let existing = table
			.get(prior_key.as_slice())
			.map_err(redb_to_io)?
			.map(|value| {
				postcard::from_bytes::<RetainedObjectIntent>(value.value())
					.map_err(|e| Error::new(ErrorKind::InvalidData, e))
			})
			.transpose()?;
		let mut prior = existing.unwrap_or(proposed);
		if !prior.was_published(&pointer.l2_uri) {
			if prior.state != RetainedObjectIntentState::Unresolved
				|| prior.replacement_uri.as_deref() != Some(pointer.l2_uri.as_str())
			{
				return Err(Error::new(
					ErrorKind::InvalidData,
					"retirement predecessor mismatch",
				));
			}
			prior.state = RetainedObjectIntentState::ReplacementPublished;
		}
		prior.published_replacements.insert(pointer.l2_uri.clone());
		let encoded =
			postcard::to_stdvec(&prior).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		table
			.insert(prior_key.as_slice(), encoded.as_slice())
			.map_err(redb_to_io)?;
		applied_evidence.push((prior_uri, prior));
	}
	let mut key = pointer.slab_id.get().to_be_bytes().to_vec();
	key.extend_from_slice(pointer.l2_uri.as_bytes());
	let existing = table
		.get(key.as_slice())
		.map_err(redb_to_io)?
		.map(|value| {
			postcard::from_bytes::<RetainedObjectIntent>(value.value())
				.map_err(|e| Error::new(ErrorKind::InvalidData, e))
		})
		.transpose()?;
	let mut retained = existing.unwrap_or(RetainedObjectIntent {
		bytes: known_bytes,
		replacement_uri: None,
		replacement_bytes: None,
		published_replacements: Default::default(),
		retirement_source: None,
		state: RetainedObjectIntentState::Unresolved,
	});
	let pending_other = retained.state == RetainedObjectIntentState::Unresolved
		&& retained
			.replacement_uri
			.as_deref()
			.is_some_and(|uri| uri != pointer.l2_uri);
	if !pending_other {
		retained.replacement_uri = None;
		retained.replacement_bytes = None;
		retained.state = RetainedObjectIntentState::RetirementPublished;
	}
	retained.retirement_source = Some(source);
	let encoded =
		postcard::to_stdvec(&retained).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
	table
		.insert(key.as_slice(), encoded.as_slice())
		.map_err(redb_to_io)?;
	Ok((retained, applied_evidence))
}

/// Limits applied while copying the receipt table out of redb. Supplied by the
/// policy layer, which owns the receipt schema and so knows the true maxima.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReceiptLoadBounds {
	pub(crate) max_rows: usize,
	pub(crate) max_key_bytes: usize,
	pub(crate) max_value_bytes: usize,
	pub(crate) max_total_bytes: usize,
}

fn receipt_load_exceeded(what: String) -> Error {
	Error::new(
		ErrorKind::InvalidData,
		format!("receipt load bound exceeded: {what}"),
	)
}

/// Test seam for the lost-history case: drop ONLY the receipt table from a closed
/// index, leaving the initialization marker and every other table intact. Lives
/// here so the table definition stays private to this module.
#[cfg(test)]
pub(crate) fn delete_maintenance_receipt_table_for_test(path: &Path) {
	let db = Database::create(path).unwrap();
	let write_txn = db.begin_write().unwrap();
	write_txn.delete_table(MAINTENANCE_RECEIPTS_TABLE).unwrap();
	write_txn.commit().unwrap();
}
const META_LAST_CONTROL_LOG_INDEX: &str = "last_control_log_index";
pub(crate) mod certified_base;
mod lsm_certificate;
pub(crate) use lsm_certificate::{LsmCertificate, intent_binding};
mod tier_certificate;
pub(crate) use op_state::CertificateRequirement;
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use tier_certificate::{
	CertifiedTierAllocation, CertifiedTierOrphan, CertifiedTierWork, TierCertificateBindsAnotherOp,
	TierCertifiedPostimage, TierCertifiedSelection, TierSelectionEvidence,
};
/// The store's incarnation, written once by fresh initialization and never
/// rewritten. Its absence on open means the store predates slab identity.
mod op_state;
pub(crate) mod parent_intents;
pub use op_state::OpTicket;
pub(crate) use op_state::{
	AbandonedOutput, GcFileIdentity, LoadedOp, MAX_RESERVED_OUTPUTS_PER_JOB, OpEvidence,
	OpOwnership, OpProgress, OpProgressUpdate, RejectedClaim, ReservationRefused, ReservedIdState,
	ReservedOp, TerminalBacklog, is_op_state_invalid,
};
#[cfg(test)]
pub(crate) use op_state::{
	OpHolderState, OpIntent, OpPhase, OpTxnCut, op_row_bytes_at, op_row_progress_offset,
	remove_op_state_meta_for_test, set_op_row_bytes_at, set_op_state_version_for_test,
};

const META_STORE_INCARNATION_HIGH: &str = "store_incarnation_high_v1";
const META_STORE_INCARNATION_LOW: &str = "store_incarnation_low_v1";
/// The exclusive bound on every slab id this store has issued. It is
/// committed before any id below it is used, so reopen never reissues one.
const META_SLAB_ID_HIGH_WATER: &str = "slab_id_high_water_v1";
/// The highest manifest epoch of any run row removed. Open reads the manifest epoch back from the
/// rows, so a removed row must not take the epoch it carried with it.
const META_LSM_MANIFEST_EPOCH_FLOOR: &str = "lsm_manifest_epoch_floor_v1";

pub(crate) use crate::consensus::StoreIncarnation;

#[derive(Debug)]
struct StoreIdentityMissing;

impl std::fmt::Display for StoreIdentityMissing {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(
			"store identity is missing: the store predates the slab identity format and cannot be opened; re-create it",
		)
	}
}

impl std::error::Error for StoreIdentityMissing {}

fn initializing_index_path(path: &Path) -> PathBuf {
	let mut name = path.file_name().unwrap_or_default().to_os_string();
	name.push(".initializing");
	path.with_file_name(name)
}

fn store_identity_missing() -> Error {
	Error::new(ErrorKind::InvalidData, StoreIdentityMissing)
}

/// An old-format store is refused, never opened in a degraded mode.
pub(crate) fn is_store_identity_missing(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|cause| cause.is::<StoreIdentityMissing>())
}

#[cfg(test)]
pub(crate) fn remove_store_identity_for_test(path: &Path) {
	let db = Database::create(path).unwrap();
	let write_txn = db.begin_write().unwrap();
	{
		let mut meta = write_txn.open_table(META_TABLE).unwrap();
		meta.remove(META_STORE_INCARNATION_HIGH).unwrap();
		meta.remove(META_STORE_INCARNATION_LOW).unwrap();
	}
	write_txn.commit().unwrap();
}

/// Test seam: an intent row that cannot be decoded, so the index and its H
/// open but the snapshot does not load.
#[cfg(test)]
pub(crate) fn insert_undecodable_lsm_intent_for_test(path: &Path) {
	let db = Database::create(path).unwrap();
	let write_txn = db.begin_write().unwrap();
	{
		let mut intents = write_txn.open_table(LSM_INSTALL_INTENTS_TABLE).unwrap();
		intents
			.insert(u64::MAX, b"not-an-intent".as_slice())
			.unwrap();
	}
	write_txn.commit().unwrap();
}

#[cfg(test)]
pub(crate) fn overwrite_slab_id_high_water_for_test(path: &Path, bound: u64) {
	let db = Database::create(path).unwrap();
	let write_txn = db.begin_write().unwrap();
	{
		let mut meta = write_txn.open_table(META_TABLE).unwrap();
		meta.insert(META_SLAB_ID_HIGH_WATER, &bound).unwrap();
	}
	write_txn.commit().unwrap();
}
/// ADR 0097 decision 4: set once, when the receipt table is first created.
///
/// Its whole purpose is to tell "this store has never had receipts" apart from
/// "this store had receipts and they are gone". Creating the table whenever it is
/// missing cannot distinguish those, and the second case must not present as an
/// empty history - that is precisely how a lost attempt budget would silently
/// reset.
const META_MAINTENANCE_RECEIPTS_INITIALIZED: &str = "maintenance_receipts_initialized_v1";

const RECORD_KEY_LEN: usize = 8 + 32 + 8 + 4 + 4 + 8 + 8;
const RECORD_VALUE_LEN_V1: usize = 4;
const RECORD_VALUE_LEN_V2: usize = 4 + 8;
const RECORD_VALUE_LEN_V3: usize = 4 + 8 + 4 + 4;
/// V4 adds the table-object identity that owned this published record
/// location.  The location key cannot be reverse-mapped to its table after
/// the slab is missing, so this attribution must travel with the durable
/// reference itself.
const RECORD_VALUE_LEN_V4: usize = 4 + 8 + 4 + 4 + 16;
/// ADR 0090 V5 appends the durable placement stamp and its partition. A value
/// shorter than this is an earlier version and decodes as `RawLocality`; a value
/// of this length with an unknown stamp is malformed and is refused, so "written
/// before partitions" and "corrupt" never share an outcome.
const RECORD_VALUE_LEN_V5: usize = 4 + 8 + 4 + 4 + 16 + 1 + 8;
const RECORD_VALUE_LEN: usize = RECORD_VALUE_LEN_V5;
const PLACEMENT_STAMP_RAW_LOCALITY: u8 = 0;
const PLACEMENT_STAMP_PARTITIONED: u8 = 1;
const RECORD_FLAG_IS_TOMBSTONE: u32 = 1 << 0;
const RECORD_FLAG_HAS_TABLE_OBJECT_ID: u32 = 1 << 1;
const TIERED_POINTER_HASH_LEN: usize = 32;
const TIERED_POINTER_FLAGS_LEN: usize = 2;
const TIERED_POINTER_PREFIX_LEN: usize = TIERED_POINTER_HASH_LEN + TIERED_POINTER_FLAGS_LEN;
const TIERED_FLAG_IS_ENCRYPTED: u8 = 0b0000_0001;
const TIERED_FLAG_HAS_TENANT_ID: u8 = 0b0000_0010;
const TIERED_FLAG_HAS_TENANT_KEK_ID: u8 = 0b0000_0100;
const TIERED_FLAG_HAS_STATS_FORMAT_VERSION: u8 = 0b0000_1000;
const TIERED_FLAG_HAS_STATS_BLOCK_LEN: u8 = 0b0001_0000;
const TIERED_FLAG_HAS_STATS_BLOCK_HASH128: u8 = 0b0010_0000;
const TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC: u8 = 0b0100_0000;
const TIERED_FLAG_TOMBSTONE_METADATA_TRACKED: u8 = 0b1000_0000;
const TIERED_FLAG_KNOWN_MASK: u8 = TIERED_FLAG_IS_ENCRYPTED
	| TIERED_FLAG_HAS_TENANT_ID
	| TIERED_FLAG_HAS_TENANT_KEK_ID
	| TIERED_FLAG_HAS_STATS_FORMAT_VERSION
	| TIERED_FLAG_HAS_STATS_BLOCK_LEN
	| TIERED_FLAG_HAS_STATS_BLOCK_HASH128
	| TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC
	| TIERED_FLAG_TOMBSTONE_METADATA_TRACKED;
const TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC: u8 = 0b0000_0001;
const TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED: u8 = 0b0000_0010;
const TIERED_FLAG2_KNOWN_MASK: u8 =
	TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC | TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED;
const SECONDARY_INDEX_SEGMENT_KEY_LEN: usize = 8 + 16 + 16 + 32 + 8;
const SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN: usize = 32 + 32 + 2 + 8 + 4 + 4;
const SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI: u32 = 1 << 0;
const SECONDARY_INDEX_SEGMENT_FLAG_KNOWN_MASK: u32 = SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI;
const LSM_RUN_VALUE_PREFIX_LEN: usize = 1 + 1 + 1 + 1 + 8 + 16 + 16 + 8 + 8 + 4 + 4 + 4 + 4 + 4;
const LSM_SLAB_STATE_LIVE: u8 = 0;
const LSM_SLAB_STATE_RETIRED: u8 = 1;
const LSM_SLAB_STATE_DELETE_PENDING: u8 = 2;
const LSM_SIDECAR_COVERAGE_NONE: u8 = 0;
const LSM_SIDECAR_COVERAGE_PARTIAL: u8 = 1;
const LSM_SIDECAR_COVERAGE_COMPLETE: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PersistentRecordRow {
	pub tenant_id: TenantId,
	pub key_identity: Identity,
	pub routing_locality: Locality,
	/// Present for typed table rows written after ADR 0539. Legacy and
	/// untyped KV rows deliberately remain unattributed rather than guessed.
	pub table_object_id: Option<TableObjectId>,
	/// ADR 0090: how this record resolves to a replica set. Absent stamp means
	/// [`PlacementSemantics::RawLocality`], decoded from a shorter value rather
	/// than inferred from a failure.
	pub placement: PlacementSemantics,
	/// Set only when `placement` is `Partitioned`.
	pub partition: Option<PartitionId>,
	pub location: RecordLocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PersistentSecondaryIndexSegmentKey {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub source_slab_id: SlabId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PersistentSecondaryIndexSegmentRow {
	pub key: PersistentSecondaryIndexSegmentKey,
	pub segment_path: String,
	pub l2_uri: Option<String>,
	pub payload_hash: [u8; 32],
	pub state_digest: [u8; 32],
	pub format_version: u16,
	pub entry_count: u64,
}

/// Identifies which authority can resolve an interrupted LSM install.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum LsmInstallKind {
	Local,
	Control,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum LsmInstallPhase {
	Prepared,
	OutputsPublished,
}

/// Durable evidence that an LSM install may have renamed output files.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LsmInstallIntent {
	pub kind: LsmInstallKind,
	pub phase: LsmInstallPhase,
	pub commit: crate::storage::lsm::CompactionCommit,
	pub commit_digest: [u8; 32],
	/// The operations this intent's work belongs to, linked in the transaction
	/// that allocated them (ADR 0697).
	pub ops: Vec<crate::consensus::slab_ops::OpKey>,
	pub inputs: Vec<LsmInputAuthority>,
	pub local_input_rows_digest: [u8; 32],
	pub local_output_rows_digest: [u8; 32],
	pub local_input_runs: Vec<LsmRunSummary>,
	pub local_input_files: Vec<LsmFileIdentity>,
	pub local_output_files: Vec<LsmFileIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LsmFileIdentity {
	pub slab_id: SlabId,
	pub len: u64,
	pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LsmInputAuthority {
	pub slab_id: SlabId,
	/// Counts publications in one open of this store, and restarts at the next:
	/// meaningful only beside the `opened` it was read under.
	pub revision: u64,
	pub tiered: Option<TieredSlabPointer>,
	/// Which open of this store read `revision`.
	pub opened: u64,
}

impl LsmInputAuthority {
	/// Whether this slab is unchanged since `recorded`. Within one open, exactly. A
	/// reopen lost the revisions before it, so what can be compared is the durable
	/// tier evidence, and no publication may have happened in this open.
	pub(crate) fn unchanged_since(&self, recorded: &Self) -> bool {
		if self.opened == recorded.opened {
			return self == recorded;
		}
		self.slab_id == recorded.slab_id && self.tiered == recorded.tiered && self.revision == 0
	}

	pub(crate) fn all_unchanged_since(current: &[Self], recorded: &[Self]) -> bool {
		current.len() == recorded.len()
			&& current
				.iter()
				.zip(recorded)
				.all(|(current, recorded)| current.unchanged_since(recorded))
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PersistentIndexSnapshot {
	/// Every durable record reference, including local L1 records.  Recovery
	/// still rebuilds the authoritative location index from slab files, but the
	/// durable rows carry attribution that cannot be reconstructed once a slab
	/// is missing.
	pub record_rows: Vec<PersistentRecordRow>,
	pub tiered_slabs: Vec<TieredSlabPointer>,
	/// Explicit durable retirements without a currently published tier pointer.
	pub retired_tiered_slabs: HashSet<SlabId>,
	pub tiered_records: Vec<PersistentRecordRow>,
	pub record_localities: Vec<(TenantId, Identity, Locality)>,
	pub secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
	pub lsm_runs: Vec<LsmRunSummary>,
	pub lsm_manifest_epoch_floor: crate::schema::ManifestEpoch,
	pub lsm_install_intents: Vec<LsmInstallIntent>,
	/// ADR 0703: certified installs' certificates, by job.
	pub lsm_certificates: Vec<(u64, LsmCertificate)>,
	pub last_control_log_index: Option<u64>,
}

impl PersistentIndexSnapshot {
	/// ADR 0703: the certificate of `intent`'s job, if any.
	pub(crate) fn lsm_certificate(&self, intent: &LsmInstallIntent) -> Option<&LsmCertificate> {
		let job = intent.commit.job_id.get();
		self.lsm_certificates
			.iter()
			.find(|(certified, _)| *certified == job)
			.map(|(_, certificate)| certificate)
	}
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PersistentIndexUpdate {
	pub lsm_intent_transition: Option<(LsmInstallIntent, LsmInstallIntent)>,
	pub put_records: Vec<PersistentRecordRow>,
	pub delete_records: Vec<PersistentRecordRow>,
	pub put_tiered_slabs: Vec<TieredSlabPointer>,
	/// A metadata operation clears only its own token in the pointer transaction.
	pub completed_metadata_token: Option<(SlabId, MetadataKind, u64)>,
	/// Only an actual committed tier publication can supersede a snapshot retirement.
	pub tier_republication_index: Option<u64>,
	pub delete_tiered_slab_ids: Vec<SlabId>,
	pub put_secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
	pub delete_secondary_index_segments: Vec<PersistentSecondaryIndexSegmentKey>,
	pub put_lsm_runs: Vec<LsmRunSummary>,
	pub delete_lsm_run_ids: Vec<SlabId>,
	/// A manifest epoch no run row carries, which a reopen must still recover.
	pub raise_lsm_manifest_epoch_floor: Option<crate::schema::ManifestEpoch>,
	/// One step of an operation's local progress, durable with this update.
	pub op_progress: Option<op_state::OpProgressUpdate>,
	/// The linked LSM intent whose local work this update finishes.
	pub complete_lsm_intent: Option<LsmInstallIntent>,
	/// Reservations this publication makes live slabs: their rows end here.
	pub promote_reservations: Vec<SlabId>,
	pub last_control_log_index: Option<u64>,
	/// A budget charge for the memory this update holds (ADR 0692 §7: the
	/// row copies of a tier publication). It moves with the update into the
	/// blocking worker and is released only when that worker drops it.
	pub retained_charge: Option<RetainedCharge>,
}

/// An opaque keep-alive for a charge that must live as long as the memory it
/// pays for, wherever that memory moves.
#[derive(Clone)]
pub(crate) struct RetainedCharge(
	#[allow(dead_code, reason = "a keep-alive: held, never read")]
	pub(crate)  std::sync::Arc<dyn std::any::Any + Send + Sync>,
);

impl std::fmt::Debug for RetainedCharge {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("RetainedCharge")
	}
}

pub(crate) struct PersistentLocalIndex {
	db: crate::storage::owned_redb::OwnedRedb,
	write_lock: Mutex<()>,
	/// Sequences a live workflow holds (`op_state::OpOwnership`).
	live_ops: std::sync::Arc<dashmap::DashSet<u64>>,
	/// Distinguishes this open of the store from any other, for `LsmInputAuthority`.
	open_nonce: u64,
	#[cfg(test)]
	#[allow(clippy::type_complexity)]
	op_after_commit_hold:
		std::sync::Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
	#[cfg(test)]
	op_txn_cut: std::sync::Mutex<Option<op_state::OpTxnCut>>,
	#[cfg(test)]
	op_txn_cut_skip: std::sync::Mutex<Option<(u32, op_state::OpTxnCut)>>,
	/// A cut for the next publication of runs, which an install with no outputs is: it
	/// carries no reservations and no progress for a cut on an operation's own step to find.
	#[cfg(test)]
	run_publication_cut: std::sync::Mutex<Option<op_state::OpTxnCut>>,
	#[cfg(test)]
	#[allow(clippy::type_complexity)]
	op_handoff_hold:
		std::sync::Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
	/// ADR 0097: false when this store previously held receipts and the table is
	/// now gone - lost history, not empty history.
	receipts_available: bool,
	/// False for the lifetime of this handle if initialized cleanup history is missing.
	cleanup_available: bool,
	persisted_cleanup_gaps: std::sync::Arc<CleanupGapContribution>,
	incarnation: StoreIncarnation,
	/// Test seam: make the next receipt mutations fail, so the policy's
	/// fail-closed paths can be exercised against a real store.
	#[cfg(test)]
	pub(crate) fail_receipt_mutations: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	pub(crate) fail_next_delta: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	pub(crate) fail_next_cleanup_insert: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	pub(crate) fail_cleanup_gap_before_commit: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	pub(crate) fail_next_retirement: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	pub(crate) fail_cleanup_after_provisional_remove: std::sync::atomic::AtomicBool,
	/// Test seam: hold the next guarded delta inside its blocking worker,
	/// before the worker consumes the update's row copies.
	#[cfg(test)]
	pub(crate) guarded_delta_before_rows_hold: std::sync::Mutex<
		Option<(
			std::sync::mpsc::SyncSender<()>,
			std::sync::mpsc::Receiver<()>,
		)>,
	>,
	#[cfg(test)]
	pub(crate) guarded_delta_before_commit_hold: std::sync::Mutex<
		Option<(
			std::sync::Arc<std::sync::Barrier>,
			std::sync::Arc<std::sync::Barrier>,
			bool,
		)>,
	>,
	#[cfg(test)]
	pub(crate) guarded_delta_commit_hold: std::sync::Mutex<
		Option<(
			std::sync::Arc<std::sync::Barrier>,
			std::sync::Arc<std::sync::Barrier>,
		)>,
	>,
	#[cfg(test)]
	pub(crate) fail_next_snapshot_retirement: std::sync::atomic::AtomicBool,
	#[cfg(test)]
	pub(crate) retirement_commit_hold: std::sync::Mutex<
		Option<(
			std::sync::Arc<std::sync::Barrier>,
			std::sync::Arc<std::sync::Barrier>,
		)>,
	>,
}

struct CleanupGapContribution {
	count: std::sync::atomic::AtomicU64,
	history_incomplete: bool,
}

impl Drop for CleanupGapContribution {
	fn drop(&mut self) {
		crate::metrics::global()
			.storage_cleanup_unrecorded_supersessions
			.sub(self.count.load(std::sync::atomic::Ordering::Relaxed));
		if self.history_incomplete {
			crate::metrics::global()
				.storage_cleanup_history_incomplete
				.sub(1);
		}
	}
}

impl PersistentLocalIndex {
	/// Allocate and durably record an operation before it can publish a row.
	pub(crate) async fn begin_metadata_token(
		&self,
		slab: SlabId,
		kind: MetadataKind,
	) -> std::io::Result<u64> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let token;
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let floor = meta
						.get(META_METADATA_TOKEN_FLOOR)
						.map_err(redb_to_io)?
						.map(|v| v.value())
						.unwrap_or(0);
					token = floor
						.checked_add(1)
						.ok_or_else(|| Error::other("metadata token counter exhausted"))?;
					meta.insert(META_METADATA_TOKEN_FLOOR, &token)
						.map_err(redb_to_io)?;
				}
				{
					let mut table = tx.open_table(METADATA_PENDING_TABLE).map_err(redb_to_io)?;
					let mut pending = table
						.get(&slab.get())
						.map_err(redb_to_io)?
						.map(|v| {
							postcard::from_bytes::<PendingMetadataTokens>(v.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))
						})
						.transpose()?
						.unwrap_or_default();
					match kind {
						MetadataKind::Supersession => {
							pending.supersession.insert(token);
						}
						MetadataKind::Tombstone => {
							pending.tombstone.insert(token);
						}
					}
					pending.degrade_applied = false;
					let value = postcard::to_stdvec(&pending)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					table
						.insert(&slab.get(), value.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)?;
				Ok(token)
			})
			.await
	}

	#[cfg(test)]
	pub(crate) async fn metadata_token_floor_for_test(&self) -> std::io::Result<u64> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let value = table
					.get(META_METADATA_TOKEN_FLOOR)
					.map_err(redb_to_io)?
					.map(|v| v.value())
					.unwrap_or(0);
				Ok(value)
			})
			.await
	}

	pub(crate) async fn load_pending_metadata_tokens(
		&self,
	) -> std::io::Result<Vec<(SlabId, PendingMetadataTokens)>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx.open_table(METADATA_PENDING_TABLE).map_err(redb_to_io)?;
				let mut rows = Vec::new();
				for row in table.iter().map_err(redb_to_io)? {
					let (key, value) = row.map_err(redb_to_io)?;
					let pending = postcard::from_bytes::<PendingMetadataTokens>(value.value())
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					rows.push((SlabId::new(key.value()), pending));
				}
				Ok(rows)
			})
			.await
	}

	/// On open, no prior writer is active. Degrade the current pointer and clear
	/// its surviving operation tokens atomically before exposing the index.
	pub(crate) async fn recover_metadata_tokens(&self) -> std::io::Result<usize> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let count;
				let mut degradation_counts = (0, 0);
				{
					let mut pending = tx.open_table(METADATA_PENDING_TABLE).map_err(redb_to_io)?;
					let rows = pending
						.iter()
						.map_err(redb_to_io)?
						.map(|row| {
							let (key, value) = row.map_err(redb_to_io)?;
							Ok((
								key.value(),
								postcard::from_bytes::<PendingMetadataTokens>(value.value())
									.map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
							))
						})
						.collect::<std::io::Result<Vec<_>>>()?;
					count = rows.len();
					let mut pointers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					for (slab, tokens) in rows {
						let pointer = pointers
							.get(&slab)
							.map_err(redb_to_io)?
							.map(|value| decode_tiered_pointer(SlabId::new(slab), value.value()))
							.transpose()?;
						if let Some(mut pointer) = pointer {
							let before = pointer.clone();
							tokens.degrade(&mut pointer);
							note_metadata_degradation(&before, &pointer, &mut degradation_counts);
							let value = encode_tiered_pointer(&pointer);
							pointers
								.insert(&slab, value.as_slice())
								.map_err(redb_to_io)?;
							pending.remove(&slab).map_err(redb_to_io)?;
						}
					}
				}
				tx.commit().map_err(redb_to_io)?;
				publish_metadata_degradation_counts(degradation_counts);
				Ok(count)
			})
			.await
	}

	pub(crate) fn cleanup_authority_available(&self) -> bool {
		self.cleanup_available
	}

	fn require_cleanup_authority(&self) -> std::io::Result<()> {
		if self.cleanup_available {
			Ok(())
		} else {
			Err(Error::new(
				ErrorKind::WouldBlock,
				"cleanup authority unavailable until restart",
			))
		}
	}

	#[cfg(test)]
	pub(crate) async fn cleanup_authority_gap_count(&self) -> std::io::Result<u64> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let count = meta
					.get(META_CLEANUP_AUTHORITY_GAPS)
					.map_err(redb_to_io)?
					.map(|v| v.value())
					.unwrap_or(0);
				Ok(count)
			})
			.await
	}

	/// Drop the rows a test's simulated crash never made durable.
	#[cfg(test)]
	pub(crate) async fn forget_record_rows_for_test(
		&self,
		forget: impl Fn(&PersistentRecordRow) -> bool,
	) -> std::io::Result<()> {
		let snapshot = self.load_snapshot().await?;
		let rows = snapshot
			.record_rows
			.into_iter()
			.chain(snapshot.tiered_records)
			.filter(|row| !forget(row))
			.collect();
		self.rebuild(rows, snapshot.tiered_slabs, snapshot.last_control_log_index)
			.await
	}

	pub(crate) async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
		Self::open_with_owner(path, None).await
	}

	pub(crate) async fn open_with_owner(
		path: impl Into<PathBuf>,
		owner: Option<DataDirectoryOwner>,
	) -> std::io::Result<Self> {
		let path = path.into();
		let mut delay = std::time::Duration::from_millis(200);
		loop {
			match Self::open_index(path.clone(), owner.clone(), None).await {
				Err(error)
					if error
						.get_ref()
						.and_then(|cause| cause.downcast_ref::<redb::DatabaseError>())
						.is_some_and(|cause| {
							matches!(cause, redb::DatabaseError::DatabaseAlreadyOpen)
						}) =>
				{
					// No database or directory write permit crosses this backoff.
					tracing::warn!(path = %path.display(), readiness = "Pending", "storage index is held by a previous handle; retrying open");
					crate::storage::manager::tier_authority::observe_tier_authority_retry(&error);
					let delay =
						crate::storage::manager::tier_authority::next_transient_retry(&mut delay);
					tokio::time::sleep(delay).await;
				}
				result => return result,
			}
		}
	}

	#[cfg(test)]
	pub(crate) fn hold_database_for_test(&self) -> std::sync::Arc<Database> {
		self.db.test_handle()
	}

	#[cfg(test)]
	pub(crate) async fn initialize_new(
		path: impl Into<PathBuf>,
		owner: Option<DataDirectoryOwner>,
	) -> std::io::Result<Self> {
		Self::initialize_new_with_high_water(path, owner, 0).await
	}

	/// Create the index with its identity and an initial slab id high-water
	/// in one transaction. The file is built under a temporary name and
	/// linked into place only after that commit, so a crash leaves either no
	/// index or a complete one.
	pub(crate) async fn initialize_new_with_high_water(
		path: impl Into<PathBuf>,
		owner: Option<DataDirectoryOwner>,
		initial_high_water: u64,
	) -> std::io::Result<Self> {
		Self::open_index(path, owner, Some(initial_high_water)).await
	}

	async fn open_index(
		path: impl Into<PathBuf>,
		owner: Option<DataDirectoryOwner>,
		initial_high_water: Option<u64>,
	) -> std::io::Result<Self> {
		let initialize_new = initial_high_water.is_some();
		let path = path.into();
		if let Some(owner) = &owner {
			owner.ensure_contains(&path)?;
		}
		// Create, initialize, wrap and bind under one open permit. A failed
		// initialization or a refused bind drops the raw database inside this
		// closure, while the permit still holds the owner open.
		let bind_owner = owner.clone();
		let opened =
			spawn_blocking_owned_io(owner, move || {
				let staging = initializing_index_path(&path);
				let db = if initialize_new {
					// An explicit fresh initialization can never open or replace an
					// existing authority file: it is built under a staging name and
					// linked into place without clobbering (below). A staging file
					// left by a crash was never linked, so it is not authority.
					if path.exists() {
						return Err(Error::new(
							ErrorKind::AlreadyExists,
							format!("storage index already exists: {}", path.display()),
						));
					}
					match std::fs::remove_file(&staging) {
						Ok(()) => {}
						Err(error) if error.kind() == ErrorKind::NotFound => {}
						Err(error) => return Err(error),
					}
					let file = std::fs::OpenOptions::new()
						.read(true)
						.write(true)
						.create_new(true)
						.open(&staging)?;
					Database::builder().create_file(file).map_err(redb_to_io)?
				} else {
					Database::open(&path).map_err(|error| match error {
						redb::DatabaseError::DatabaseAlreadyOpen => {
							Error::new(ErrorKind::WouldBlock, error)
						}
						other => redb_to_io(other),
					})?
				};
				let write_txn = db.begin_write().map_err(redb_to_io)?;
				{
					let _table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				}
				{
					let _table = write_txn
						.open_table(TIERED_SLABS_TABLE)
						.map_err(redb_to_io)?;
				}
				{
					let _table = write_txn
						.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
						.map_err(redb_to_io)?;
				}
				{
					let _table = write_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
					let _table = write_txn
						.open_table(DEFERRED_RETIREMENTS_TABLE)
						.map_err(redb_to_io)?;
				}
				{
					let _table = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
				}
				// Keep the rest of the index usable when cleanup history alone is missing.
				let cleanup_ok = {
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_SNAPSHOT_CLEANUP_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let present = write_txn
						.list_tables()
						.map_err(redb_to_io)?
						.any(|table| table.name() == SNAPSHOT_CLEANUP_INTENTS_TABLE.name());
					if initialized && !present {
						meta.insert(META_CLEANUP_HISTORY_INCOMPLETE, &1u64)
							.map_err(redb_to_io)?;
					}
					if !initialized {
						let _table = write_txn
							.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
							.map_err(redb_to_io)?;
						meta.insert(META_SNAPSHOT_CLEANUP_INITIALIZED, &1u64)
							.map_err(redb_to_io)?;
					}
					!initialized || present
				};
				{
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_SNAPSHOT_RETIREMENTS_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let tables = write_txn
						.list_tables()
						.map_err(redb_to_io)?
						.map(|table| table.name().to_owned())
						.collect::<HashSet<_>>();
					let present = tables.contains(SNAPSHOT_RETIREMENTS_TABLE.name())
						&& tables.contains(SNAPSHOT_RETIREMENT_STATE_TABLE.name());
					if initialized && !present {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"snapshot retirement authority is missing",
						));
					}
					if !initialized {
						let _table = write_txn
							.open_table(SNAPSHOT_RETIREMENTS_TABLE)
							.map_err(redb_to_io)?;
						let _state = write_txn
							.open_table(SNAPSHOT_RETIREMENT_STATE_TABLE)
							.map_err(redb_to_io)?;
						meta.insert(META_SNAPSHOT_RETIREMENTS_INITIALIZED, &1u64)
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_METADATA_PENDING_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let present = write_txn
						.list_tables()
						.map_err(redb_to_io)?
						.any(|table| table.name() == METADATA_PENDING_TABLE.name());
					if initialized && !present {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"pending metadata authority is missing",
						));
					}
					if !initialized {
						let _table = write_txn
							.open_table(METADATA_PENDING_TABLE)
							.map_err(redb_to_io)?;
						meta.insert(META_METADATA_PENDING_INITIALIZED, &1u64)
							.map_err(redb_to_io)?;
					}
				}
				// ADR 0699 S3b: parent intents are created on first use; once
				// initialized, a missing table refuses the open.
				parent_intents::check_parent_intent_authority(&write_txn)?;
				// Create only on first upgrade. A missing previously initialized table
				// is lost authority, not evidence that no submission is outstanding.
				{
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_LSM_INSTALL_INTENTS_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let present = write_txn
						.list_tables()
						.map_err(redb_to_io)?
						.any(|table| table.name() == LSM_INSTALL_INTENTS_TABLE.name());
					if initialized && !present {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"LSM install intent authority is missing",
						));
					}
					if !initialized {
						let _table = write_txn
							.open_table(LSM_INSTALL_INTENTS_TABLE)
							.map_err(redb_to_io)?;
						meta.insert(META_LSM_INSTALL_INTENTS_INITIALIZED, &1u64)
							.map_err(redb_to_io)?;
					}
				}
				// ADR 0703: the same for certified installs' certificates.
				{
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_LSM_CERTIFICATES_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let present = write_txn.list_tables().map_err(redb_to_io)?.any(|table| {
						table.name() == lsm_certificate::LSM_CERTIFICATES_TABLE.name()
					});
					if initialized && !present {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"LSM certificate authority is missing",
						));
					}
					if !initialized {
						let _table = write_txn
							.open_table(lsm_certificate::LSM_CERTIFICATES_TABLE)
							.map_err(redb_to_io)?;
						meta.insert(META_LSM_CERTIFICATES_INITIALIZED, &1u64)
							.map_err(redb_to_io)?;
					}
				}
				// ADR 0704: the same for certified tiered operations' certificates.
				{
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_TIER_CERTIFICATES_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let present = write_txn.list_tables().map_err(redb_to_io)?.any(|table| {
						table.name() == tier_certificate::TIER_CERTIFICATES_TABLE.name()
					});
					if initialized && !present {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"tier certificate authority is missing",
						));
					}
					if !initialized {
						let _table = write_txn
							.open_table(tier_certificate::TIER_CERTIFICATES_TABLE)
							.map_err(redb_to_io)?;
						meta.insert(META_TIER_CERTIFICATES_INITIALIZED, &1u64)
							.map_err(redb_to_io)?;
					}
				}
				// ADR 0097: create the receipt table ONLY on a store that has never had
				// one, and record that fact durably in the same transaction. On a store
				// whose marker is set, a missing table means history was lost, and the
				// caller is told so rather than handed an empty one.
				let receipts_ok = {
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let initialized = meta
						.get(META_MAINTENANCE_RECEIPTS_INITIALIZED)
						.map_err(redb_to_io)?
						.is_some();
					let table_present = write_txn
						.list_tables()
						.map_err(redb_to_io)?
						.any(|handle| handle.name() == MAINTENANCE_RECEIPTS_TABLE.name());
					match (initialized, table_present) {
						(true, true) => true,
						(true, false) => false,
						(false, _) => {
							let _table = write_txn
								.open_table(MAINTENANCE_RECEIPTS_TABLE)
								.map_err(redb_to_io)?;
							meta.insert(META_MAINTENANCE_RECEIPTS_INITIALIZED, &1u64)
								.map_err(redb_to_io)?;
							true
						}
					}
				};
				// One-shot v1 → v2 migration. Every v1 row becomes a v2
				// row with `is_encrypted = false`; the rewrite sweeper
				// will then GET, encrypt, and stamp the truth back. v1
				// is left in place but unused — operators can drop it
				// in a later release once every cluster has cycled.
				let mut migrated_keys: Vec<u64> = Vec::new();
				{
					let v1 = write_txn
						.open_table(TIERED_SLABS_TABLE_V1)
						.map_err(redb_to_io)?;
					let mut v2 = write_txn
						.open_table(TIERED_SLABS_TABLE)
						.map_err(redb_to_io)?;
					for entry in v1.iter().map_err(redb_to_io)? {
						let (slab_id_key, value) = entry.map_err(redb_to_io)?;
						let slab_id = SlabId::new(slab_id_key.value());
						if v2.get(&slab_id.get()).map_err(redb_to_io)?.is_some() {
							continue;
						}
						let pointer = decode_tiered_pointer_v1(slab_id, value.value())?;
						let encoded = encode_tiered_pointer(&pointer);
						v2.insert(&slab_id.get(), encoded.as_slice())
							.map_err(redb_to_io)?;
						migrated_keys.push(slab_id.get());
					}
				}
				if !migrated_keys.is_empty() {
					let mut v1 = write_txn
						.open_table(TIERED_SLABS_TABLE_V1)
						.map_err(redb_to_io)?;
					for key in migrated_keys {
						let _ = v1.remove(&key).map_err(redb_to_io)?;
					}
				}
				let initial_gaps = {
					let meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let value = meta
						.get(META_CLEANUP_AUTHORITY_GAPS)
						.map_err(redb_to_io)?
						.map(|v| v.value())
						.unwrap_or(0);
					value
				};
				let history_incomplete = {
					let meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let missing = meta
						.get(META_CLEANUP_HISTORY_INCOMPLETE)
						.map_err(redb_to_io)?
						.is_some();
					missing
				};
				let incarnation = {
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					if initialize_new {
						let incarnation = StoreIncarnation::fresh();
						meta.insert(
							META_STORE_INCARNATION_HIGH,
							&((incarnation.get() >> 64) as u64),
						)
						.map_err(redb_to_io)?;
						meta.insert(META_STORE_INCARNATION_LOW, &(incarnation.get() as u64))
							.map_err(redb_to_io)?;
						meta.insert(
							META_SLAB_ID_HIGH_WATER,
							&initial_high_water.unwrap_or_default(),
						)
						.map_err(redb_to_io)?;
						incarnation
					} else {
						let read = |key| {
							meta.get(key)
								.map_err(redb_to_io)
								.map(|value| value.map(|value| value.value()))
						};
						let (Some(high), Some(low), Some(_)) = (
							read(META_STORE_INCARNATION_HIGH)?,
							read(META_STORE_INCARNATION_LOW)?,
							read(META_SLAB_ID_HIGH_WATER)?,
						) else {
							return Err(store_identity_missing());
						};
						let incarnation =
							StoreIncarnation::from_raw((u128::from(high) << 64) | u128::from(low));
						if incarnation.get() == 0 {
							return Err(store_identity_missing());
						}
						incarnation
					}
				};
				if initialize_new {
					op_state::initialize_op_state(&write_txn)?;
				} else {
					op_state::validate_op_state(&write_txn)?;
				}
				write_txn.commit().map_err(redb_to_io)?;
				if initialize_new {
					// hard_link refuses an existing target, so this never replaces
					// an index that appeared meanwhile.
					std::fs::hard_link(&staging, &path)?;
					std::fs::remove_file(&staging)?;
					if let Some(parent) = path.parent() {
						std::fs::File::open(parent)?.sync_all()?;
					}
				}
				let db = crate::storage::owned_redb::OwnedRedb::new(db, "storage index");
				if let Some(owner) = bind_owner {
					db.bind_owner(owner)?;
				}
				Ok((
					db,
					receipts_ok,
					cleanup_ok,
					initial_gaps,
					history_incomplete,
					incarnation,
				))
			})
			.await?;
		let (db, receipts_ok, cleanup_ok, initial_gaps, history_incomplete, incarnation) = opened;
		if history_incomplete {
			crate::metrics::global()
				.storage_cleanup_history_incomplete
				.add(1);
		}
		crate::metrics::global()
			.storage_cleanup_unrecorded_supersessions
			.add(initial_gaps);
		Ok(Self {
			db,
			write_lock: Mutex::new(()),
			live_ops: std::sync::Arc::new(dashmap::DashSet::new()),
			open_nonce: rand::random::<u64>(),
			#[cfg(test)]
			op_after_commit_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			op_txn_cut: std::sync::Mutex::new(None),
			#[cfg(test)]
			op_txn_cut_skip: std::sync::Mutex::new(None),
			#[cfg(test)]
			run_publication_cut: std::sync::Mutex::new(None),
			#[cfg(test)]
			op_handoff_hold: std::sync::Mutex::new(None),
			receipts_available: receipts_ok,
			cleanup_available: cleanup_ok,
			persisted_cleanup_gaps: std::sync::Arc::new(CleanupGapContribution {
				count: std::sync::atomic::AtomicU64::new(initial_gaps),
				history_incomplete,
			}),
			incarnation,
			#[cfg(test)]
			fail_receipt_mutations: Default::default(),
			#[cfg(test)]
			fail_next_delta: Default::default(),
			#[cfg(test)]
			fail_next_cleanup_insert: Default::default(),
			#[cfg(test)]
			fail_cleanup_gap_before_commit: Default::default(),
			#[cfg(test)]
			fail_next_retirement: Default::default(),
			#[cfg(test)]
			fail_cleanup_after_provisional_remove: Default::default(),
			#[cfg(test)]
			guarded_delta_before_rows_hold: Default::default(),
			#[cfg(test)]
			guarded_delta_before_commit_hold: Default::default(),
			#[cfg(test)]
			guarded_delta_commit_hold: Default::default(),
			#[cfg(test)]
			fail_next_snapshot_retirement: std::sync::atomic::AtomicBool::new(false),
			#[cfg(test)]
			retirement_commit_hold: std::sync::Mutex::new(None),
		})
	}

	/// ADR 0097 decision 4: whether this store's receipt history is trustworthy.
	///
	/// `false` means the store has held receipts before and the table is now
	/// missing. The index still serves everything else; only maintenance admission
	/// is affected, because an empty history here is indistinguishable from a lost
	/// one and would silently reset an attempt budget.
	pub(crate) fn receipts_available(&self) -> bool {
		self.receipts_available
	}

	/// Read the exact durable record key for a publication pre/post comparison.
	pub(crate) async fn record_row_at_key(
		&self,
		row: PersistentRecordRow,
	) -> std::io::Result<Option<PersistentRecordRow>> {
		self.db
			.run(move |db| {
				let txn = db.begin_read().map_err(redb_to_io)?;
				let table = txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				let key = encode_record_key(row);
				let value = table.get(key.as_slice()).map_err(redb_to_io)?;
				value
					.map(|value| decode_record_row(&key, value.value()))
					.transpose()
			})
			.await
	}

	pub(crate) async fn load_snapshot(&self) -> std::io::Result<PersistentIndexSnapshot> {
		let cleanup_available = self.cleanup_available;
		self.db
			.run(move |db| {
				let read_txn = db.begin_read().map_err(redb_to_io)?;
				let tiered_table = read_txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?;
				let mut tiered_slabs = Vec::new();
				let mut tiered_ids = HashSet::new();
				for entry in tiered_table.iter().map_err(redb_to_io)? {
					let (slab_id, value) = entry.map_err(redb_to_io)?;
					let pointer =
						decode_tiered_pointer(SlabId::new(slab_id.value()), value.value())?;
					tiered_ids.insert(pointer.slab_id);
					tiered_slabs.push(pointer);
				}

				let records_table = read_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				let mut retired_tiered_slabs = HashSet::new();
				if cleanup_available {
					let cleanup = read_txn
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					for row in cleanup.iter().map_err(redb_to_io)? {
						let (key, value) = row.map_err(redb_to_io)?;
						if key.value().len() <= 8 {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"invalid retirement record key",
							));
						}
						let slab =
							SlabId::new(u64::from_be_bytes(key.value()[..8].try_into().unwrap()));
						let intent: RetainedObjectIntent = postcard::from_bytes(value.value())
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
						if intent.state == RetainedObjectIntentState::RetirementPublished
							&& intent.retirement_source.is_some()
							&& !tiered_ids.contains(&slab)
						{
							retired_tiered_slabs.insert(slab);
						}
					}
				}
				let mut record_rows = Vec::new();
				let mut tiered_records = Vec::new();
				let mut locality_by_key: HashMap<
					(TenantId, Identity),
					(Locality, HybridTimestamp),
				> = HashMap::new();
				for entry in records_table.iter().map_err(redb_to_io)? {
					let (key, value) = entry.map_err(redb_to_io)?;
					let row = decode_record_row(key.value(), value.value())?;
					record_rows.push(row);
					locality_by_key
						.entry((row.tenant_id, row.key_identity))
						.and_modify(|(locality, hlc)| {
							if row.location.hlc >= *hlc {
								*locality = row.routing_locality;
								*hlc = row.location.hlc;
							}
						})
						.or_insert((row.routing_locality, row.location.hlc));
					if tiered_ids.contains(&row.location.slab_id) {
						tiered_records.push(row);
					}
				}
				let mut record_localities = locality_by_key
					.into_iter()
					.map(|((tenant_id, key_identity), (locality, _))| {
						(tenant_id, key_identity, locality)
					})
					.collect::<Vec<_>>();
				record_localities
					.sort_by_key(|(tenant_id, key_identity, _)| (tenant_id.get(), *key_identity));
				let secondary_index_segments_table = read_txn
					.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
					.map_err(redb_to_io)?;
				let mut secondary_index_segments = Vec::new();
				for entry in secondary_index_segments_table.iter().map_err(redb_to_io)? {
					let (key, value) = entry.map_err(redb_to_io)?;
					secondary_index_segments.push(decode_secondary_index_segment_row(
						key.value(),
						value.value(),
					)?);
				}
				let lsm_runs_table = read_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
				let mut lsm_runs = Vec::new();
				for entry in lsm_runs_table.iter().map_err(redb_to_io)? {
					let (slab_id, value) = entry.map_err(redb_to_io)?;
					lsm_runs.push(decode_lsm_run_summary(
						SlabId::new(slab_id.value()),
						value.value(),
					)?);
				}
				let intent_table = read_txn
					.open_table(LSM_INSTALL_INTENTS_TABLE)
					.map_err(redb_to_io)?;
				let mut lsm_install_intents = Vec::new();
				for entry in intent_table.iter().map_err(redb_to_io)? {
					let (key, value) = entry.map_err(redb_to_io)?;
					let intent: LsmInstallIntent =
						serde_json::from_slice(value.value()).map_err(|error| {
							std::io::Error::new(std::io::ErrorKind::InvalidData, error)
						})?;
					if intent.commit.job_id.get() != key.value() {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"LSM intent job key mismatch",
						));
					}
					let bytes =
						serde_json::to_vec(&intent.commit).map_err(std::io::Error::other)?;
					if *blake3::hash(&bytes).as_bytes() != intent.commit_digest {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"LSM intent commit digest mismatch",
						));
					}
					lsm_install_intents.push(intent);
				}
				let lsm_certificates = lsm_certificate::read_certificates(
					&read_txn
						.open_table(lsm_certificate::LSM_CERTIFICATES_TABLE)
						.map_err(redb_to_io)?,
				)?;
				let meta_table = read_txn.open_table(META_TABLE).map_err(redb_to_io)?;
				let last_control_log_index = meta_table
					.get(META_LAST_CONTROL_LOG_INDEX)
					.map_err(redb_to_io)?
					.map(|value| value.value());
				let lsm_manifest_epoch_floor = crate::schema::ManifestEpoch::new(
					meta_table
						.get(META_LSM_MANIFEST_EPOCH_FLOOR)
						.map_err(redb_to_io)?
						.map(|value| value.value())
						.unwrap_or(0),
				);

				Ok(PersistentIndexSnapshot {
					record_rows,
					tiered_slabs,
					retired_tiered_slabs,
					tiered_records,
					record_localities,
					secondary_index_segments,
					lsm_runs,
					lsm_manifest_epoch_floor,
					lsm_install_intents,
					lsm_certificates,
					last_control_log_index,
				})
			})
			.await
	}

	pub(crate) async fn put_lsm_install_intent(
		&self,
		intent: LsmInstallIntent,
	) -> std::io::Result<()> {
		let commit_bytes = serde_json::to_vec(&intent.commit).map_err(std::io::Error::other)?;
		if *blake3::hash(&commit_bytes).as_bytes() != intent.commit_digest {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"LSM intent digest mismatch",
			));
		}
		let job = intent.commit.job_id.get();
		let bytes = serde_json::to_vec(&intent).map_err(std::io::Error::other)?;
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(LSM_INSTALL_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					if let Some(existing) = table.get(&job).map_err(redb_to_io)? {
						if existing.value() != bytes.as_slice() {
							return Err(std::io::Error::new(
								std::io::ErrorKind::AlreadyExists,
								"different LSM intent for job",
							));
						}
					} else {
						table.insert(&job, bytes.as_slice()).map_err(redb_to_io)?;
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	pub(crate) async fn clear_lsm_install_intent(
		&self,
		intent: LsmInstallIntent,
	) -> std::io::Result<()> {
		let job = intent.commit.job_id.get();
		let bytes = serde_json::to_vec(&intent).map_err(std::io::Error::other)?;
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(LSM_INSTALL_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let matches = table
						.get(&job)
						.map_err(redb_to_io)?
						.is_some_and(|existing| existing.value() == bytes.as_slice());
					if !matches {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"LSM intent changed or disappeared",
						));
					}
					table.remove(&job).map_err(redb_to_io)?;
				}
				lsm_certificate::remove_certificate_of(&tx, &intent)?;
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	pub(crate) async fn rebuild(
		&self,
		rows: Vec<PersistentRecordRow>,
		tiered_slabs: Vec<TieredSlabPointer>,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let secondary_index_segments = self
			.load_snapshot()
			.await
			.map(|snapshot| snapshot.secondary_index_segments)
			.unwrap_or_default();
		let lsm_runs = self
			.load_snapshot()
			.await
			.map(|snapshot| snapshot.lsm_runs)
			.unwrap_or_default();
		self.rebuild_with_secondary_index_segments_and_lsm_runs(
			rows,
			tiered_slabs,
			secondary_index_segments,
			lsm_runs,
			last_control_log_index,
		)
		.await
	}

	#[cfg(test)]
	pub(crate) async fn rebuild_with_secondary_index_segments(
		&self,
		rows: Vec<PersistentRecordRow>,
		tiered_slabs: Vec<TieredSlabPointer>,
		secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let lsm_runs = self
			.load_snapshot()
			.await
			.map(|snapshot| snapshot.lsm_runs)
			.unwrap_or_default();
		self.rebuild_with_secondary_index_segments_and_lsm_runs(
			rows,
			tiered_slabs,
			secondary_index_segments,
			lsm_runs,
			last_control_log_index,
		)
		.await
	}

	pub(crate) async fn rebuild_with_secondary_index_segments_and_lsm_runs(
		&self,
		rows: Vec<PersistentRecordRow>,
		tiered_slabs: Vec<TieredSlabPointer>,
		secondary_index_segments: Vec<PersistentSecondaryIndexSegmentRow>,
		lsm_runs: Vec<LsmRunSummary>,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let write_txn = db.begin_write().map_err(redb_to_io)?;
				let previous_pointers = {
					let table = write_txn
						.open_table(TIERED_SLABS_TABLE)
						.map_err(redb_to_io)?;
					let mut old = HashMap::new();
					for row in table.iter().map_err(redb_to_io)? {
						let (key, value) = row.map_err(redb_to_io)?;
						let slab = SlabId::new(key.value());
						old.insert(slab, decode_tiered_pointer(slab, value.value())?);
					}
					old
				};
				let mut degradation_counts = (0, 0);
				let _ = write_txn.delete_table(RECORDS_TABLE);
				let _ = write_txn.delete_table(TIERED_SLABS_TABLE);
				let _ = write_txn.delete_table(SECONDARY_INDEX_SEGMENTS_TABLE);
				let _ = write_txn.delete_table(LSM_RUNS_TABLE);
				{
					let mut table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
					for row in rows {
						let key = encode_record_key(row);
						let value = encode_record_value(row);
						table
							.insert(key.as_slice(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut table = write_txn
						.open_table(TIERED_SLABS_TABLE)
						.map_err(redb_to_io)?;
					for mut pointer in tiered_slabs {
						apply_pending_metadata_to_pointer(&write_txn, &mut pointer, None)?;
						if let Some(previous) = previous_pointers.get(&pointer.slab_id) {
							note_metadata_degradation(previous, &pointer, &mut degradation_counts);
						}
						let value = encode_tiered_pointer(&pointer);
						table
							.insert(&pointer.slab_id.get(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut table = write_txn
						.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
						.map_err(redb_to_io)?;
					for row in secondary_index_segments {
						let key = encode_secondary_index_segment_key(row.key);
						let value = encode_secondary_index_segment_value(&row);
						table
							.insert(key.as_slice(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut table = write_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
					for run in lsm_runs {
						let value = encode_lsm_run_summary(&run)?;
						table
							.insert(&run.slab_id.get(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				if let Some(index) = last_control_log_index {
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					meta.insert(META_LAST_CONTROL_LOG_INDEX, &index)
						.map_err(redb_to_io)?;
				}
				write_txn.commit().map_err(redb_to_io)?;
				publish_metadata_degradation_counts(degradation_counts);
				Ok(())
			})
			.await
	}

	pub(crate) async fn apply(&self, update: PersistentIndexUpdate) -> std::io::Result<()> {
		self.apply_with_publication_guard(update, None).await
	}

	/// The blocking redb closure may outlive its awaiting caller. The optional
	/// gate is owned by the actual transaction until it finishes.
	pub(crate) async fn apply_with_publication_guard(
		&self,
		update: PersistentIndexUpdate,
		publication: Option<std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>>,
	) -> std::io::Result<()> {
		self.apply_with_publication_guard_inner(update, publication, None)
			.await
	}

	pub(crate) async fn apply_with_control_lsm_completion(
		&self,
		update: PersistentIndexUpdate,
		publication: Option<std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>>,
		completion: crate::storage::manager::AcceptedControlLsmCompletion,
	) -> std::io::Result<()> {
		self.apply_with_publication_guard_inner(update, publication, Some(completion))
			.await
	}

	async fn apply_with_publication_guard_inner(
		&self,
		mut update: PersistentIndexUpdate,
		publication: Option<std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>>,
		completion: Option<crate::storage::manager::AcceptedControlLsmCompletion>,
	) -> std::io::Result<()> {
		if let Some((before, after)) = &update.lsm_intent_transition {
			let mut expected = before.clone();
			expected.phase = LsmInstallPhase::OutputsPublished;
			if before.kind != LsmInstallKind::Local
				|| before.phase != LsmInstallPhase::Prepared
				|| expected != *after
			{
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"invalid LSM intent phase transition",
				));
			}
		}
		if let Some(completion) = completion {
			if update.lsm_intent_transition.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"two LSM phase transitions in one update",
				));
			}
			let (before, after) = completion.into_transition();
			let inputs = before
				.inputs
				.iter()
				.map(|input| input.slab_id)
				.collect::<HashSet<_>>();
			let outputs = before
				.commit
				.slabs_to_add
				.iter()
				.map(|run| run.slab_id)
				.collect::<HashSet<_>>();
			let mut expected = before.clone();
			expected.phase = LsmInstallPhase::OutputsPublished;
			if before.kind != LsmInstallKind::Control
				|| before.phase != LsmInstallPhase::Prepared
				|| expected != after
				|| before.local_input_runs.is_empty()
				|| before.local_input_rows_digest == [0; 32]
				|| before.local_output_rows_digest == [0; 32]
				|| !before.commit.slabs_to_mark_delete_pending.is_empty()
				|| !before.commit.slabs_to_remove.is_empty()
				|| !before.commit.sidecar_swaps.is_empty()
				|| before
					.local_input_runs
					.iter()
					.chain(&before.commit.slabs_to_add)
					.any(|run| run.sidecar_coverage != crate::storage::SidecarCoverage::None)
				|| update
					.put_records
					.iter()
					.any(|row| !outputs.contains(&row.location.slab_id))
				|| update
					.delete_records
					.iter()
					.any(|row| !inputs.contains(&row.location.slab_id))
				|| lsm_rows_digest(update.put_records.iter().copied())
					!= before.local_output_rows_digest
				|| lsm_rows_digest(update.delete_records.iter().copied())
					!= before.local_input_rows_digest
			{
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"invalid completed control LSM publication",
				));
			}
			update.lsm_intent_transition = Some((before, after));
		}

		#[cfg(test)]
		if self
			.fail_next_delta
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return Err(Error::other("injected delta failure"));
		}
		let _guard = self.write_lock.lock().await;
		let cleanup_missing = !self.cleanup_available;
		let persisted_cleanup_gaps = self.persisted_cleanup_gaps.clone();
		#[cfg(test)]
		let fail_cleanup_gap = self
			.fail_cleanup_gap_before_commit
			.swap(false, std::sync::atomic::Ordering::AcqRel);
		#[cfg(test)]
		let commit_hold = self.guarded_delta_commit_hold.lock().unwrap().clone();
		#[cfg(test)]
		let before_commit_hold = self.guarded_delta_before_commit_hold.lock().unwrap().take();
		#[cfg(test)]
		let before_rows_hold = self.guarded_delta_before_rows_hold.lock().unwrap().take();
		#[cfg(test)]
		let progress_cut = (update.op_progress.is_some() || !update.promote_reservations.is_empty())
			.then(|| self.take_op_txn_cut())
			.flatten()
			.or_else(|| {
				(!update.put_lsm_runs.is_empty())
					.then(|| self.run_publication_cut.lock().unwrap().take())
					.flatten()
			});
		self.db
			.run(move |db| {
				let _publication = publication;
				// Bind the charge by name: a closure captures only the fields it
				// uses, and the row copies below must stay charged until they drop.
				let _retained_charge = update.retained_charge;
				#[cfg(test)]
				if let Some((entered, release)) = before_rows_hold {
					// A test that already gave up drops both ends; never wait on it.
					if entered.send(()).is_ok() {
						let _ = release.recv();
					}
				}
				let write_txn = db.begin_write().map_err(redb_to_io)?;
				let mut degradation_counts = (0, 0);
				let mut missing_cleanup_supersessions = Vec::new();
				{
					let mut table = write_txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
					for row in update.delete_records {
						let key = encode_record_key(row);
						let _ = table.remove(key.as_slice()).map_err(redb_to_io)?;
					}
					for row in update.put_records {
						let key = encode_record_key(row);
						let value = encode_record_value(row);
						table
							.insert(key.as_slice(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut table = write_txn
						.open_table(TIERED_SLABS_TABLE)
						.map_err(redb_to_io)?;
					for slab_id in update.delete_tiered_slab_ids {
						let _ = table.remove(&slab_id.get()).map_err(redb_to_io)?;
					}
					for mut pointer in update.put_tiered_slabs {
						let previous = table.get(&pointer.slab_id.get()).map_err(redb_to_io)?.map(|v| decode_tiered_pointer(pointer.slab_id, v.value())).transpose()?;
						apply_pending_metadata_to_pointer(&write_txn, &mut pointer, update.completed_metadata_token)?;
						if let Some(previous) = &previous { note_metadata_degradation(previous, &pointer, &mut degradation_counts); }
						if let Some(control_index) = update.tier_republication_index {
							let mut marks = write_txn.open_table(SNAPSHOT_RETIREMENTS_TABLE).map_err(redb_to_io)?;
							let mark = marks.get(&pointer.slab_id.get()).map_err(redb_to_io)?
								.map(|value| postcard::from_bytes::<SnapshotRetirementMark>(value.value()).map_err(|e| Error::new(ErrorKind::InvalidData, e))).transpose()?;
							if mark.is_some_and(|mark| mark.snapshot_index.is_none_or(|snapshot| control_index > snapshot)) {
								marks.remove(&pointer.slab_id.get()).map_err(redb_to_io)?;
							}
						}
						if let Some(control_index) = update.tier_republication_index.filter(|_| !cleanup_missing) {
							let mut cleanup = write_txn.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE).map_err(redb_to_io)?;
							let mut key = pointer.slab_id.get().to_be_bytes().to_vec(); key.extend_from_slice(pointer.l2_uri.as_bytes());
							let prior = cleanup.get(key.as_slice()).map_err(redb_to_io)?.map(|v| postcard::from_bytes::<RetainedObjectIntent>(v.value()).map_err(|e| Error::new(ErrorKind::InvalidData, e))).transpose()?;
							if let Some(mut prior) = prior.filter(|p| p.retirement_source.is_some_and(|source| control_index > source.index())) {
								prior.retirement_source = None;
								if prior.state == RetainedObjectIntentState::RetirementPublished { prior.state = RetainedObjectIntentState::ReplacementPublished; }
								let encoded = postcard::to_stdvec(&prior).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
								cleanup.insert(key.as_slice(), encoded.as_slice()).map_err(redb_to_io)?;
							}
						}
						if cleanup_missing {
							let old = table.get(&pointer.slab_id.get()).map_err(redb_to_io)?
								.map(|value| decode_tiered_pointer(pointer.slab_id, value.value())).transpose()?;
							if let Some(old) = old.filter(|old| old.l2_uri != pointer.l2_uri) {
								missing_cleanup_supersessions.push((pointer.slab_id, old.l2_uri));
							}
						}
						let value = encode_tiered_pointer(&pointer);
						table
							.insert(&pointer.slab_id.get(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut table = write_txn
						.open_table(SECONDARY_INDEX_SEGMENTS_TABLE)
						.map_err(redb_to_io)?;
					for key in update.delete_secondary_index_segments {
						let encoded = encode_secondary_index_segment_key(key);
						let _ = table.remove(encoded.as_slice()).map_err(redb_to_io)?;
					}
					for row in update.put_secondary_index_segments {
						let key = encode_secondary_index_segment_key(row.key);
						let value = encode_secondary_index_segment_value(&row);
						table
							.insert(key.as_slice(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					if let Some(progress) = &update.op_progress {
						op_state::apply_op_progress(&write_txn, progress)?;
					}
					if let Some(intent) = &update.complete_lsm_intent {
						op_state::complete_linked_lsm_intent(&write_txn, intent)?;
					}
					op_state::promote_reservations(&write_txn, &update.promote_reservations)?;
					for slab_id in update.delete_lsm_run_ids {
						remove_lsm_run_row(&write_txn, slab_id)?;
					}
					if let Some(epoch) = update.raise_lsm_manifest_epoch_floor {
						raise_lsm_manifest_epoch_floor(&write_txn, epoch)?;
					}
					let mut table = write_txn.open_table(LSM_RUNS_TABLE).map_err(redb_to_io)?;
					for run in update.put_lsm_runs {
						let value = encode_lsm_run_summary(&run)?;
						table
							.insert(&run.slab_id.get(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				if let Some((before, after)) = update.lsm_intent_transition {
					let key = before.commit.job_id.get();
					let old = serde_json::to_vec(&before).map_err(Error::other)?;
					let next = serde_json::to_vec(&after).map_err(Error::other)?;
					let mut table = write_txn
						.open_table(LSM_INSTALL_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let matches = table
						.get(&key)
						.map_err(redb_to_io)?
						.is_some_and(|value| value.value() == old.as_slice());
					if !matches {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"LSM intent changed before output publication",
						));
					}
					table.insert(&key, next.as_slice()).map_err(redb_to_io)?;
				}
				if let Some(index) = update.last_control_log_index {
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let current = meta
						.get(META_LAST_CONTROL_LOG_INDEX)
						.map_err(redb_to_io)?
						.map(|value| value.value())
						.unwrap_or(0);
					if index >= current {
						meta.insert(META_LAST_CONTROL_LOG_INDEX, &index)
							.map_err(redb_to_io)?;
					}
				}
				if !missing_cleanup_supersessions.is_empty() {
					let mut meta = write_txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let count = meta.get(META_CLEANUP_AUTHORITY_GAPS).map_err(redb_to_io)?.map(|v| v.value()).unwrap_or(0);
					let next = count.checked_add(missing_cleanup_supersessions.len() as u64).ok_or_else(|| Error::other("cleanup gap counter overflow"))?;
					meta.insert(META_CLEANUP_AUTHORITY_GAPS, &next).map_err(redb_to_io)?;
				}
				#[cfg(test)]
				if fail_cleanup_gap && !missing_cleanup_supersessions.is_empty() {
					return Err(Error::other("injected cleanup gap transaction failure"));
				}
				#[cfg(test)]
				if let Some((entered, release, fail)) = before_commit_hold {
					entered.wait();
					release.wait();
					if fail { return Err(Error::other("injected delta failure before commit")); }
				}
				#[cfg(test)]
				if progress_cut == Some(op_state::OpTxnCut::BeforeCommit) {
					return Err(Error::other("injected fault before a progress commit"));
				}
				write_txn.commit().map_err(redb_to_io)?;
				#[cfg(test)]
				if progress_cut == Some(op_state::OpTxnCut::AfterCommit) {
					return Err(Error::other("injected loss of a progress commit's result"));
				}
				publish_metadata_degradation_counts(degradation_counts);
				let gap_count = missing_cleanup_supersessions.len() as u64;
				persisted_cleanup_gaps.count.fetch_add(gap_count, std::sync::atomic::Ordering::Relaxed);
				crate::metrics::global().storage_cleanup_unrecorded_supersessions.add(gap_count);
				for (slab_id, old_uri) in missing_cleanup_supersessions {
					tracing::warn!(slab_id = slab_id.get(), old_uri = %old_uri, "committed supersession missing cleanup accounting");
					crate::metrics::global().storage_cleanup_unrecorded_supersessions_total.inc();
				}
				#[cfg(test)]
				if let Some((entered, release)) = commit_hold {
					entered.wait();
					release.wait();
				}
				Ok(())
			})
			.await
	}

	/// Remove one slab and its pointer in one checked transaction.
	/// Hold a decided retirement back, durably, before its local step is acknowledged. The pointer
	/// must still be this generation: a retirement of any other has nothing to wait for here.
	pub(crate) async fn defer_retirement(
		&self,
		deferred: DeferredRetirement,
		gate: std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let _gate = gate;
				let txn = db.begin_write().map_err(redb_to_io)?;
				let slab_id = deferred.pointer.slab_id;
				let same_generation = txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?
					.get(&slab_id.get())
					.map_err(redb_to_io)?
					.map(|value| decode_tiered_pointer(slab_id, value.value()))
					.transpose()?
					.is_some_and(|pointer| pointer.same_object_generation(&deferred.pointer));
				if !same_generation {
					return Err(Error::new(
						ErrorKind::WouldBlock,
						"tier pointer changed before its retirement was deferred",
					));
				}
				let bytes = postcard::to_stdvec(&deferred)
					.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
				txn.open_table(DEFERRED_RETIREMENTS_TABLE)
					.map_err(redb_to_io)?
					.insert(&slab_id.get(), bytes.as_slice())
					.map_err(redb_to_io)?;
				txn.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Whether this exact held retirement is still to apply: its row as read, and a pointer of its
	/// generation. Anything else keeps the obligation and its fence. Every retirement that takes
	/// effect clears a row of its own generation in the same transaction, and nothing durable can
	/// prove after the fact that some other route retired exactly this generation.
	pub(crate) async fn deferred_retirement_is_owed(
		&self,
		row: DeferredRetirement,
	) -> std::io::Result<bool> {
		self.db
			.run(move |db| {
				let txn = db.begin_read().map_err(redb_to_io)?;
				let slab_id = row.pointer.slab_id;
				let saved = txn
					.open_table(DEFERRED_RETIREMENTS_TABLE)
					.map_err(redb_to_io)?
					.get(&slab_id.get())
					.map_err(redb_to_io)?
					.map(|value| {
						postcard::from_bytes::<DeferredRetirement>(value.value())
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))
					})
					.transpose()?;
				if saved.as_ref() != Some(&row) {
					return Ok(false);
				}
				let pointer = txn
					.open_table(TIERED_SLABS_TABLE)
					.map_err(redb_to_io)?
					.get(&slab_id.get())
					.map_err(redb_to_io)?
					.map(|value| decode_tiered_pointer(slab_id, value.value()))
					.transpose()?;
				Ok(pointer.is_some_and(|pointer| pointer.same_object_generation(&row.pointer)))
			})
			.await
	}

	/// Whether a durable snapshot mark of this generation stands that a retirement with this
	/// authority would not resolve. Deleting the pointer then would strand the mark, and its fence,
	/// for good: the mark's own retry must take it instead.
	pub(crate) async fn outstanding_mark_blocks_retirement(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		retirement_epoch: u64,
	) -> std::io::Result<bool> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let marks = tx
					.open_table(SNAPSHOT_RETIREMENTS_TABLE)
					.map_err(redb_to_io)?;
				let mark = marks
					.get(&pointer.slab_id.get())
					.map_err(redb_to_io)?
					.map(|v| {
						postcard::from_bytes::<SnapshotRetirementMark>(v.value())
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))
					})
					.transpose()?;
				Ok(mark.is_some_and(|mark| {
					pointer.same_object_generation(&mark.pointer)
						&& !retirement_resolves_mark(
							&mark,
							last_control_log_index,
							retirement_epoch,
						)
				}))
			})
			.await
	}

	pub(crate) async fn load_deferred_retirements(
		&self,
	) -> std::io::Result<Vec<DeferredRetirement>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx
					.open_table(DEFERRED_RETIREMENTS_TABLE)
					.map_err(redb_to_io)?;
				let mut rows = Vec::new();
				for row in table.iter().map_err(redb_to_io)? {
					let (key, value) = row.map_err(redb_to_io)?;
					let deferred: DeferredRetirement = postcard::from_bytes(value.value())
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					if deferred.pointer.slab_id.get() != key.value() {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"deferred retirement key mismatch",
						));
					}
					rows.push(deferred);
				}
				Ok(rows)
			})
			.await
	}

	/// A Live run left by a retirement from before retirements disposed of their run. Only exact
	/// evidence authorizes this: a published retirement record with a source and no tier pointer,
	/// never a missing file. True when that evidence stands, so memory may follow.
	pub(crate) async fn dispose_retirement_remnant(
		&self,
		slab_id: SlabId,
		gate: std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	) -> std::io::Result<bool> {
		if !self.cleanup_available {
			return Ok(false);
		}
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let _gate = gate;
				let txn = db.begin_write().map_err(redb_to_io)?;
				if !retirement_evidence_stands(&txn, slab_id)? {
					return Ok(false);
				}
				let disposed = dispose_retired_live_run(&txn, slab_id)?;
				txn.commit().map_err(redb_to_io)?;
				Ok(disposed)
			})
			.await
	}

	/// The record a retirement writes, on its own.
	#[cfg(test)]
	pub(crate) async fn record_retirement_for_test(
		&self,
		pointer: TieredSlabPointer,
		index: u64,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let txn = db.begin_write().map_err(redb_to_io)?;
				persist_retirement_records(
					&txn,
					&pointer,
					None,
					Vec::new(),
					RetirementSource::Log(index),
				)?;
				txn.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Whether an unresolved install names this slab, as the disposal in a retirement reads it.
	pub(crate) async fn install_intent_names(&self, slab_id: SlabId) -> std::io::Result<bool> {
		self.db
			.run(move |db| {
				let read = db.begin_read().map_err(redb_to_io)?;
				install_intent_names_in(
					&read
						.open_table(LSM_INSTALL_INTENTS_TABLE)
						.map_err(redb_to_io)?,
					slab_id,
				)
			})
			.await
	}

	/// The open-time form of `dispose_retirement_remnant`: all of them, in one transaction.
	pub(crate) async fn dispose_retirement_remnants(
		&self,
		slabs: Vec<SlabId>,
	) -> std::io::Result<Vec<SlabId>> {
		if !self.cleanup_available || slabs.is_empty() {
			return Ok(Vec::new());
		}
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let txn = db.begin_write().map_err(redb_to_io)?;
				let mut disposed = Vec::new();
				for slab_id in slabs {
					if retirement_evidence_stands(&txn, slab_id)?
						&& dispose_retired_live_run(&txn, slab_id)?
					{
						disposed.push(slab_id);
					}
				}
				txn.commit().map_err(redb_to_io)?;
				Ok(disposed)
			})
			.await
	}

	pub(crate) async fn retire_tiered_slab_checked(
		&self,
		slab_id: SlabId,
		expected: Option<TieredSlabPointer>,
		known_bytes: Option<u64>,
		evidence: Vec<(String, RetainedObjectIntent)>,
		last_control_log_index: Option<u64>,
		retirement_epoch: u64,
		gate: std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	) -> std::io::Result<(
		Option<(RetainedObjectIntent, Vec<(String, RetainedObjectIntent)>)>,
		Option<SnapshotRetirementMark>,
	)> {
		#[cfg(test)]
		if self
			.fail_next_retirement
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return Err(Error::other("injected retirement transaction failure"));
		}
		#[cfg(test)]
		let retirement_commit_hold = self.retirement_commit_hold.lock().unwrap().take();
		let _guard = self.write_lock.lock().await;
		let cleanup_missing = !self.cleanup_available;
		let persisted_cleanup_gaps = self.persisted_cleanup_gaps.clone();
		self.db
			.run(move |db| {
				let _gate = gate;
				let txn = db.begin_write().map_err(redb_to_io)?;
				{
					let mut tiers = txn.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					let actual = tiers
						.get(&slab_id.get())
						.map_err(redb_to_io)?
						.map(|value| decode_tiered_pointer(slab_id, value.value()))
						.transpose()?;
					// Advisory metadata may differ from the caller's mirror while a
					// metadata token is pending; only the object generation matters.
					let same_generation = match (&actual, &expected) {
						(Some(actual), Some(expected)) => actual.same_object_generation(expected),
						(None, None) => true,
						_ => false,
					};
					if !same_generation {
						return Err(Error::new(
							ErrorKind::WouldBlock,
							"tier pointer changed before retirement",
						));
					}
					tiers.remove(&slab_id.get()).map_err(redb_to_io)?;
				}
				{
					let mut records = txn.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
					let mut keys = Vec::new();
					for entry in records.iter().map_err(redb_to_io)? {
						let (key, value) = entry.map_err(redb_to_io)?;
						if decode_record_row(key.value(), value.value())?
							.location
							.slab_id == slab_id
						{
							keys.push(key.value().to_vec());
						}
					}
					for key in keys {
						records.remove(key.as_slice()).map_err(redb_to_io)?;
					}
				}
				dispose_retired_live_run(&txn, slab_id)?;
				if let Some(pointer) = &expected {
					clear_deferred_retirement_of(&txn, pointer)?;
				}
				if let Some(index) = last_control_log_index {
					let mut meta = txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let current = meta
						.get(META_LAST_CONTROL_LOG_INDEX)
						.map_err(redb_to_io)?
						.map(|value| value.value())
						.unwrap_or(0);
					if index >= current {
						meta.insert(META_LAST_CONTROL_LOG_INDEX, &index)
							.map_err(redb_to_io)?;
					}
				}
				// A retry resolves only the intent covered by its authority. An
				// older callback must leave a newer snapshot/log retirement intact.
				let resolved_mark = {
					let mut marks = txn.open_table(SNAPSHOT_RETIREMENTS_TABLE).map_err(redb_to_io)?;
					let mark = marks.get(&slab_id.get()).map_err(redb_to_io)?
						.map(|v| postcard::from_bytes::<SnapshotRetirementMark>(v.value())
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))).transpose()?;
					let resolved = mark.filter(|mark| {
						expected
							.as_ref()
							.is_some_and(|pointer| pointer.same_object_generation(&mark.pointer))
							&& retirement_resolves_mark(mark, last_control_log_index, retirement_epoch)
					});
					if resolved.is_some() { marks.remove(&slab_id.get()).map_err(redb_to_io)?; }
					resolved
				};
				let retirement_record = if !cleanup_missing {
					match (expected.as_ref(), last_control_log_index) {
						(Some(pointer), Some(index)) => Some(persist_retirement_records(&txn, pointer, known_bytes, evidence, RetirementSource::Log(index))?),
						_ => None,
					}
				} else { None };
				// A missing source index is also missing cleanup accounting, even
				// when the receipt tables themselves are available.
				let accounting_missing = expected.is_some() && retirement_record.is_none();
				if accounting_missing {
					let mut meta = txn.open_table(META_TABLE).map_err(redb_to_io)?;
					let current = meta.get(META_CLEANUP_AUTHORITY_GAPS).map_err(redb_to_io)?.map(|v| v.value()).unwrap_or(0);
					let next = current.checked_add(1).ok_or_else(|| Error::other("cleanup gap counter overflow"))?;
					meta.insert(META_CLEANUP_AUTHORITY_GAPS, &next).map_err(redb_to_io)?;
				}
				let result = txn.commit().map_err(redb_to_io);
				if result.is_ok() && accounting_missing {
					if let Some(old) = expected {
						persisted_cleanup_gaps.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
						crate::metrics::global().storage_cleanup_unrecorded_supersessions.add(1);
						tracing::warn!(slab_id = slab_id.get(), old_uri = %old.l2_uri, "committed retirement missing cleanup accounting");
						crate::metrics::global().storage_cleanup_unrecorded_supersessions_total.inc();
					}
				}
				#[cfg(test)]
				if let Some((entered, release)) = retirement_commit_hold {
					entered.wait();
					release.wait();
				}
				result?;
				Ok((retirement_record, resolved_mark))
			})
			.await
	}

	#[cfg(test)]
	pub(crate) async fn remove_cleanup_table_for_test(&self) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				tx.delete_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
					.map_err(redb_to_io)?;
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	#[cfg(test)]
	pub(crate) async fn restore_cleanup_table_for_test(&self) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let _table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	pub(crate) fn store_incarnation(&self) -> StoreIncarnation {
		self.incarnation
	}

	/// The committed exclusive bound on issued slab ids.
	pub(crate) async fn slab_id_high_water(&self) -> std::io::Result<u64> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				meta.get(META_SLAB_ID_HIGH_WATER)
					.map_err(redb_to_io)?
					.map(|value| value.value())
					.ok_or_else(store_identity_missing)
			})
			.await
	}

	/// Commit a raised slab id bound. It returns only after the commit is
	/// durable; a failure exposes no id above the previous bound.
	/// Which open of this store this is.
	pub(crate) fn open_nonce(&self) -> u64 {
		self.open_nonce
	}

	pub(crate) async fn persist_slab_id_high_water(&self, bound: u64) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let current = meta
						.get(META_SLAB_ID_HIGH_WATER)
						.map_err(redb_to_io)?
						.map(|value| value.value())
						.ok_or_else(store_identity_missing)?;
					if bound < current {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							format!("slab id high-water only rises: {bound} < {current}"),
						));
					}
					meta.insert(META_SLAB_ID_HIGH_WATER, &bound)
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	pub(crate) async fn snapshot_retirement_epoch_floor(&self) -> std::io::Result<u64> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let value = meta
					.get(META_SNAPSHOT_RETIREMENT_EPOCH_FLOOR)
					.map_err(redb_to_io)?
					.map(|v| v.value())
					.unwrap_or(0);
				Ok(value)
			})
			.await
	}

	/// Snapshot write-gate caller records the complete new retirement set before releasing the gate.
	pub(crate) async fn stage_snapshot_retirement_marks(
		&self,
		marks: Vec<SnapshotRetirementMark>,
		committed: std::collections::HashSet<SlabId>,
		snapshot_epoch: u64,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
	) -> std::io::Result<Vec<TieredSlabPointer>> {
		self.stage_tier_comparison(
			None,
			marks,
			committed,
			snapshot_epoch,
			current_epoch,
			epoch_gate,
		)
		.await
	}

	/// The receipt and committed-set comparison are committed in the same redb transaction.
	pub(crate) async fn stage_tier_comparison(
		&self,
		replacement_applied: Option<(u64, Vec<TieredSlabPointer>)>,
		marks: Vec<SnapshotRetirementMark>,
		committed: std::collections::HashSet<SlabId>,
		snapshot_epoch: u64,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
	) -> std::io::Result<Vec<TieredSlabPointer>> {
		self.stage_tier_comparison_inner(
			replacement_applied,
			Vec::new(),
			None,
			marks,
			committed,
			snapshot_epoch,
			current_epoch,
			epoch_gate,
		)
		.await
	}

	/// Accepted-snapshot deferral is the sole issuer. The caller retains the
	/// fresh canonical read lease and index write gate until pending registration.
	/// Only exact absent canonical pointers are staged; records are untouched.
	pub(crate) async fn stage_snapshot_pending_tiers(
		&self,
		staged: Vec<TieredSlabPointer>,
		canonical: Vec<TieredSlabPointer>,
		snapshot_epoch: u64,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
		gate: std::sync::Arc<tokio::sync::OwnedRwLockWriteGuard<()>>,
	) -> std::io::Result<()> {
		let mut ids = std::collections::HashSet::new();
		if canonical.iter().any(|pointer| !ids.insert(pointer.slab_id)) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"duplicate canonical snapshot tier",
			));
		}
		ids.clear();
		if staged
			.iter()
			.any(|pointer| !ids.insert(pointer.slab_id) || !canonical.contains(pointer))
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"snapshot staging pointer differs from canonical set",
			));
		}
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let _gate = gate;
				let _epoch = epoch_gate.read().unwrap_or_else(|e| e.into_inner());
				if current_epoch.load(std::sync::atomic::Ordering::Acquire) != snapshot_epoch {
					return Err(Error::new(
						ErrorKind::Interrupted,
						"snapshot staging epoch superseded",
					));
				}
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					for pointer in staged {
						let actual = tiers
							.get(&pointer.slab_id.get())
							.map_err(redb_to_io)?
							.map(|value| decode_tiered_pointer(pointer.slab_id, value.value()))
							.transpose()?;
						match actual {
							Some(actual) if actual != pointer => {
								return Err(Error::new(
									ErrorKind::InvalidData,
									"snapshot staging found a different durable pointer",
								));
							}
							Some(_) => {}
							None => {
								let value = encode_tiered_pointer(&pointer);
								tiers
									.insert(&pointer.slab_id.get(), value.as_slice())
									.map_err(redb_to_io)?;
							}
						}
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// The sole issuer is the manager's complete confirmed replacement replay.
	/// Explicit exact callback evidence may stage new absent canonical rows.
	pub(crate) async fn stage_replacement_tier_comparison(
		&self,
		replacement_applied: (u64, Vec<TieredSlabPointer>),
		replay_staged: Vec<TieredSlabPointer>,
		marks: Vec<SnapshotRetirementMark>,
		committed: std::collections::HashSet<SlabId>,
		snapshot_epoch: u64,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
	) -> std::io::Result<Vec<TieredSlabPointer>> {
		self.stage_tier_comparison_inner(
			Some(replacement_applied),
			replay_staged,
			None,
			marks,
			committed,
			snapshot_epoch,
			current_epoch,
			epoch_gate,
		)
		.await
	}

	/// Sole issuer: complete snapshot comparison with exact older pending flights.
	pub(crate) async fn stage_snapshot_tier_comparison_with_pending_retirements(
		&self,
		snapshot_index: u64,
		pending_retirements: Vec<(TieredSlabPointer, u64)>,
		marks: Vec<SnapshotRetirementMark>,
		committed: std::collections::HashSet<SlabId>,
		snapshot_epoch: u64,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
	) -> std::io::Result<Vec<TieredSlabPointer>> {
		self.stage_tier_comparison_inner(
			None,
			Vec::new(),
			Some((snapshot_index, pending_retirements)),
			marks,
			committed,
			snapshot_epoch,
			current_epoch,
			epoch_gate,
		)
		.await
	}

	async fn stage_tier_comparison_inner(
		&self,
		replacement_applied: Option<(u64, Vec<TieredSlabPointer>)>,
		replay_staged: Vec<TieredSlabPointer>,
		pending_retirement_staged: Option<(u64, Vec<(TieredSlabPointer, u64)>)>,
		marks: Vec<SnapshotRetirementMark>,
		committed: std::collections::HashSet<SlabId>,
		snapshot_epoch: u64,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
	) -> std::io::Result<Vec<TieredSlabPointer>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let _epoch_guard = epoch_gate.read().unwrap_or_else(|e| e.into_inner());
				if current_epoch.load(std::sync::atomic::Ordering::Acquire) != snapshot_epoch {
					return Err(Error::new(
						ErrorKind::Interrupted,
						"snapshot retirement epoch superseded",
					));
				}
				let tx = db.begin_write().map_err(redb_to_io)?;
				if let Some((snapshot, pending)) = pending_retirement_staged {
					if replacement_applied.is_some() {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"pending retirement requires complete snapshot proof",
						));
					}
					let mut tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					for (pointer, old) in pending {
						if old >= snapshot
							|| committed.contains(&pointer.slab_id)
							|| !marks.iter().any(|mark| {
								mark.pointer == pointer
									&& mark.snapshot_index == Some(snapshot)
									&& mark.epoch == snapshot_epoch
							}) {
							return Err(Error::new(
								ErrorKind::InvalidInput,
								"unjustified pending retirement input",
							));
						}
						let actual = tiers
							.get(&pointer.slab_id.get())
							.map_err(redb_to_io)?
							.map(|value| decode_tiered_pointer(pointer.slab_id, value.value()))
							.transpose()?;
						if let Some(actual) = actual {
							if !actual.same_object_generation(&pointer) {
								return Err(Error::new(
									ErrorKind::WouldBlock,
									"pending retirement durable generation changed",
								));
							}
						} else {
							let value = encode_tiered_pointer(&pointer);
							tiers
								.insert(&pointer.slab_id.get(), value.as_slice())
								.map_err(redb_to_io)?;
						}
					}
				}
				if !replay_staged.is_empty() {
					let expected = replacement_applied.as_ref().ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidInput,
							"replay staging requires replacement proof",
						)
					})?;
					let mut tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					for pointer in replay_staged {
						if !expected.1.contains(&pointer) {
							return Err(Error::new(
								ErrorKind::InvalidInput,
								"replay staging pointer differs from canonical set",
							));
						}
						let absent = tiers
							.get(&pointer.slab_id.get())
							.map_err(redb_to_io)?
							.is_none();
						if absent {
							let value = encode_tiered_pointer(&pointer);
							tiers
								.insert(&pointer.slab_id.get(), value.as_slice())
								.map_err(redb_to_io)?;
						}
					}
				}
				if let Some((_, expected)) = &replacement_applied {
					let tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					for pointer in expected {
						if tiers
							.get(&pointer.slab_id.get())
							.map_err(redb_to_io)?
							.is_none()
						{
							return Err(Error::new(
								ErrorKind::WouldBlock,
								"replacement comparison is missing an expected committed pointer",
							));
						}
					}
					for row in tiers.iter().map_err(redb_to_io)? {
						let (id, value) = row.map_err(redb_to_io)?;
						let slab = SlabId::new(id.value());
						let mut actual = decode_tiered_pointer(slab, value.value())?;
						let Some(pointer) = expected.iter().find(|p| p.slab_id == slab) else {
							return Err(Error::new(
								ErrorKind::WouldBlock,
								"replacement comparison found an absent committed slab",
							));
						};
						actual.tombstone_metadata = pointer.tombstone_metadata;
						actual.supersession_metadata = pointer.supersession_metadata;
						if &actual != pointer {
							return Err(Error::new(
								ErrorKind::WouldBlock,
								"replacement comparison found a changed committed pointer",
							));
						}
					}
				}
				let mut committed_ids = committed.iter().copied().collect::<Vec<_>>();
				committed_ids.sort();
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let floor = meta
						.get(META_SNAPSHOT_RETIREMENT_EPOCH_FLOOR)
						.map_err(redb_to_io)?
						.map(|v| v.value())
						.unwrap_or(0);
					if snapshot_epoch < floor {
						return Err(Error::new(
							ErrorKind::Interrupted,
							"snapshot retirement epoch is stale",
						));
					}
					meta.insert(META_SNAPSHOT_RETIREMENT_EPOCH_FLOOR, &snapshot_epoch)
						.map_err(redb_to_io)?;
					if let Some((applied, _)) = &replacement_applied {
						meta.insert("replacement_compared_applied_v1", applied)
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut state = tx
						.open_table(SNAPSHOT_RETIREMENT_STATE_TABLE)
						.map_err(redb_to_io)?;
					let encoded = postcard::to_stdvec(&SnapshotRetirementState {
						epoch: snapshot_epoch,
						committed: committed_ids,
					})
					.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					state
						.insert("current", encoded.as_slice())
						.map_err(redb_to_io)?;
				}
				let mut degraded_pointers = Vec::new();
				let mut degradation_counts = (0, 0);
				{
					let mut tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
					let mut table = tx
						.open_table(SNAPSHOT_RETIREMENTS_TABLE)
						.map_err(redb_to_io)?;
					for slab in &committed {
						table.remove(&slab.get()).map_err(redb_to_io)?;
						let old = tiers
							.get(&slab.get())
							.map_err(redb_to_io)?
							.map(|v| decode_tiered_pointer(*slab, v.value()))
							.transpose()?;
						if let Some(mut pointer) = old {
							let before = pointer.clone();
							apply_pending_metadata_to_pointer(&tx, &mut pointer, None)?;
							if pointer != before {
								note_metadata_degradation(
									&before,
									&pointer,
									&mut degradation_counts,
								);
								let value = encode_tiered_pointer(&pointer);
								tiers
									.insert(&slab.get(), value.as_slice())
									.map_err(redb_to_io)?;
								degraded_pointers.push(pointer);
							}
						}
					}
					for mark in marks {
						let id = mark.pointer.slab_id;
						let actual = tiers
							.get(&id.get())
							.map_err(redb_to_io)?
							.map(|v| decode_tiered_pointer(id, v.value()))
							.transpose()?;
						if actual.as_ref() != Some(&mark.pointer) {
							return Err(Error::new(
								ErrorKind::WouldBlock,
								"snapshot retirement pointer changed",
							));
						}
						let value = postcard::to_stdvec(&mark)
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
						table
							.insert(&id.get(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				tx.commit().map_err(redb_to_io)?;
				publish_metadata_degradation_counts(degradation_counts);
				Ok(degraded_pointers)
			})
			.await
	}

	/// Persist a gate-timeout intent without claiming a complete snapshot set.
	/// Its only clearers are a checked retirement or a newer verified comparison.
	pub(crate) async fn stage_deferred_retirement(
		&self,
		mark: SnapshotRetirementMark,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
	) -> std::io::Result<Option<SnapshotRetirementMark>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let _epoch_guard = epoch_gate.read().unwrap_or_else(|e| e.into_inner());
				if current_epoch.load(std::sync::atomic::Ordering::Acquire) != mark.epoch {
					return Ok(None);
				}
				let tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
				let actual = tiers
					.get(&mark.pointer.slab_id.get())
					.map_err(redb_to_io)?
					.map(|v| decode_tiered_pointer(mark.pointer.slab_id, v.value()))
					.transpose()?;
				if actual.as_ref() != Some(&mark.pointer) {
					return Ok(None);
				}
				drop(tiers);
				{
					let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
					let floor = meta
						.get(META_SNAPSHOT_RETIREMENT_EPOCH_FLOOR)
						.map_err(redb_to_io)?
						.map(|value| value.value())
						.unwrap_or(0);
					meta.insert(META_SNAPSHOT_RETIREMENT_EPOCH_FLOOR, &floor.max(mark.epoch))
						.map_err(redb_to_io)?;
				}
				let saved = {
					let mut marks = tx
						.open_table(SNAPSHOT_RETIREMENTS_TABLE)
						.map_err(redb_to_io)?;
					let previous = marks
						.get(&mark.pointer.slab_id.get())
						.map_err(redb_to_io)?
						.map(|v| {
							postcard::from_bytes::<SnapshotRetirementMark>(v.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))
						})
						.transpose()?;
					if let Some(previous) = previous.filter(|previous| {
						previous.epoch > mark.epoch
							|| (previous.epoch == mark.epoch
								&& previous.snapshot_index >= mark.snapshot_index)
					}) {
						previous
					} else {
						let encoded = postcard::to_stdvec(&mark)
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
						marks
							.insert(&mark.pointer.slab_id.get(), encoded.as_slice())
							.map_err(redb_to_io)?;
						mark
					}
				};
				tx.commit().map_err(redb_to_io)?;
				Ok(Some(saved))
			})
			.await
	}

	pub(crate) async fn load_snapshot_retirement_marks(
		&self,
	) -> std::io::Result<Vec<SnapshotRetirementMark>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx
					.open_table(SNAPSHOT_RETIREMENTS_TABLE)
					.map_err(redb_to_io)?;
				let mut marks = Vec::new();
				for row in table.iter().map_err(redb_to_io)? {
					let (key, value) = row.map_err(redb_to_io)?;
					let mark: SnapshotRetirementMark = postcard::from_bytes(value.value())
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					if mark.pointer.slab_id.get() != key.value() {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"snapshot retirement key mismatch",
						));
					}
					marks.push(mark);
				}
				Ok(marks)
			})
			.await
	}

	/// Apply an already-published snapshot retirement under its tier lock and index gate.
	/// None means the captured authority is obsolete and no persistent effect occurred.
	pub(crate) async fn apply_snapshot_retirement(
		&self,
		mark: SnapshotRetirementMark,
		known_bytes: Option<u64>,
		evidence: Vec<(String, RetainedObjectIntent)>,
		current_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
		epoch_gate: std::sync::Arc<std::sync::RwLock<()>>,
		publication_gate: std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	) -> std::io::Result<
		Option<(
			Option<RetainedObjectIntent>,
			Vec<(String, RetainedObjectIntent)>,
		)>,
	> {
		#[cfg(test)]
		if self
			.fail_next_snapshot_retirement
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return Err(Error::other("injected snapshot retirement failure"));
		}
		let _guard = self.write_lock.lock().await;
		let cleanup_available = self.cleanup_available;
		let gap_contribution = self.persisted_cleanup_gaps.clone();
		#[cfg(test)]
		let fail_cleanup_gap = self
			.fail_cleanup_gap_before_commit
			.swap(false, std::sync::atomic::Ordering::AcqRel);
		self.db.run(move |db| {
			let _publication_gate = publication_gate;
			let _epoch_guard = epoch_gate.read().unwrap_or_else(|e| e.into_inner());
			if current_epoch.load(std::sync::atomic::Ordering::Acquire) != mark.epoch { return Ok(None); }
			let tx = db.begin_write().map_err(redb_to_io)?;
			{
				let state = tx.open_table(SNAPSHOT_RETIREMENT_STATE_TABLE).map_err(redb_to_io)?;
				let saved = state.get("current").map_err(redb_to_io)?.map(|value| postcard::from_bytes::<SnapshotRetirementState>(value.value()).map_err(|e| Error::new(ErrorKind::InvalidData, e))).transpose()?;
				if !saved.is_some_and(|saved| saved.epoch == mark.epoch && !saved.committed.contains(&mark.pointer.slab_id)) { return Ok(None); }
			}
			{
				let mut marks = tx.open_table(SNAPSHOT_RETIREMENTS_TABLE).map_err(redb_to_io)?;
				let saved = marks.get(&mark.pointer.slab_id.get()).map_err(redb_to_io)?.map(|value| postcard::from_bytes::<SnapshotRetirementMark>(value.value()).map_err(|e| Error::new(ErrorKind::InvalidData, e))).transpose()?;
				if saved.as_ref() != Some(&mark) { return Ok(None); }
				let mut tiers = tx.open_table(TIERED_SLABS_TABLE).map_err(redb_to_io)?;
				let actual = tiers.get(&mark.pointer.slab_id.get()).map_err(redb_to_io)?.map(|v| decode_tiered_pointer(mark.pointer.slab_id, v.value())).transpose()?;
				if actual.as_ref().is_none_or(|pointer| !pointer.same_object_generation(&mark.pointer)) { return Ok(None); }
				tiers.remove(&mark.pointer.slab_id.get()).map_err(redb_to_io)?;
				marks.remove(&mark.pointer.slab_id.get()).map_err(redb_to_io)?;
			}
			{
				let mut records = tx.open_table(RECORDS_TABLE).map_err(redb_to_io)?;
				let mut keys = Vec::new();
				for row in records.iter().map_err(redb_to_io)? {
					let (key, value) = row.map_err(redb_to_io)?;
					if decode_record_row(key.value(), value.value())?.location.slab_id == mark.pointer.slab_id { keys.push(key.value().to_vec()); }
				}
				for key in keys { records.remove(key.as_slice()).map_err(redb_to_io)?; }
			}
			dispose_retired_live_run(&tx, mark.pointer.slab_id)?;
			clear_deferred_retirement_of(&tx, &mark.pointer)?;
			let (retained, applied_evidence) = if cleanup_available {
				let retirement_index = mark.snapshot_index.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "snapshot retirement lacks an authority index"))?;
				let (record, evidence) = persist_retirement_records(&tx, &mark.pointer, known_bytes, evidence, RetirementSource::Snapshot(retirement_index))?;
				(Some(record), evidence)
			} else {
				let mut meta = tx.open_table(META_TABLE).map_err(redb_to_io)?;
				let current = meta.get(META_CLEANUP_AUTHORITY_GAPS).map_err(redb_to_io)?.map(|v| v.value()).unwrap_or(0);
				let next = current.checked_add(1).ok_or_else(|| Error::other("cleanup gap counter overflow"))?;
				meta.insert(META_CLEANUP_AUTHORITY_GAPS, &next).map_err(redb_to_io)?;
				(None, Vec::new())
			};
			#[cfg(test)]
			if fail_cleanup_gap && !cleanup_available { return Err(Error::other("injected snapshot retirement gap failure")); }
			tx.commit().map_err(redb_to_io)?;
			if !cleanup_available {
				gap_contribution.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				crate::metrics::global().storage_cleanup_unrecorded_supersessions.add(1);
				crate::metrics::global().storage_cleanup_unrecorded_supersessions_total.inc();
				tracing::warn!(slab_id = mark.pointer.slab_id.get(), old_uri = %mark.pointer.l2_uri, "committed snapshot retirement missing cleanup accounting");
			}
			Ok(Some((retained, applied_evidence)))
		}).await
	}

	/// Persist one unresolved snapshot-raced cleanup, idempotently by slab and URI.
	pub(crate) async fn put_snapshot_cleanup_intent(
		&self,
		slab_id: SlabId,
		uri: String,
		intent: RetainedObjectIntent,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		#[cfg(test)]
		if self
			.fail_next_cleanup_insert
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return Err(Error::other("injected cleanup insert failure"));
		}
		let encoded =
			postcard::to_stdvec(&intent).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		let mut key = slab_id.get().to_be_bytes().to_vec();
		key.extend_from_slice(uri.as_bytes());
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let existing = table
						.get(key.as_slice())
						.map_err(redb_to_io)?
						.map(|value| value.value().to_vec());
					match existing {
						Some(value) if value != encoded => {
							let previous: RetainedObjectIntent = postcard::from_bytes(&value)
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
							// A published ambiguous upload can later be superseded.
							// Transition its existing row without adding a second count.
							if previous.state != RetainedObjectIntentState::ReplacementPublished
								|| previous.replacement_uri.as_deref() != Some(uri.as_str())
								|| previous.bytes != intent.bytes
								|| intent.state != RetainedObjectIntentState::Unresolved
								|| intent.replacement_uri.is_none()
							{
								return Err(Error::new(
									ErrorKind::InvalidData,
									"conflicting cleanup intent",
								));
							}
							let mut next = intent.clone();
							next.published_replacements
								.extend(previous.published_replacements);
							next.published_replacements.extend(previous.replacement_uri);
							let merged = postcard::to_stdvec(&next)
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
							table
								.insert(key.as_slice(), merged.as_slice())
								.map_err(redb_to_io)?;
						}
						Some(_) => {}
						None => {
							table
								.insert(key.as_slice(), encoded.as_slice())
								.map_err(redb_to_io)?;
						}
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Stage the old object and its provisional replacement together.
	pub(crate) async fn put_cleanup_intent_pair(
		&self,
		slab_id: SlabId,
		old_uri: String,
		old: RetainedObjectIntent,
		provisional: Option<(String, RetainedObjectIntent)>,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let mut entries = vec![(old_uri, old)];
		if let Some(provisional) = provisional {
			entries.push(provisional);
		}
		let entries = entries
			.into_iter()
			.map(|(uri, intent)| {
				let mut key = slab_id.get().to_be_bytes().to_vec();
				key.extend_from_slice(uri.as_bytes());
				let value = postcard::to_stdvec(&intent)
					.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
				Ok((key, value))
			})
			.collect::<std::io::Result<Vec<_>>>()?;
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					for (key, value) in entries {
						let prior = table
							.get(key.as_slice())
							.map_err(redb_to_io)?
							.map(|v| v.value().to_vec());
						match prior {
							Some(prior) if prior != value => {
								let prior: RetainedObjectIntent = postcard::from_bytes(&prior)
									.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
								let next: RetainedObjectIntent = postcard::from_bytes(&value)
									.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
								if !matches!(
									prior.state,
									RetainedObjectIntentState::ReplacementPublished
										| RetainedObjectIntentState::RetirementPublished
								) || next.state != RetainedObjectIntentState::Unresolved
									|| prior.bytes != next.bytes
									|| !prior
										.published_replacements
										.is_subset(&next.published_replacements)
									|| prior
										.replacement_uri
										.as_ref()
										.is_some_and(|u| !next.published_replacements.contains(u))
									|| next
										.replacement_uri
										.as_ref()
										.is_none_or(|u| next.published_replacements.contains(u))
								{
									return Err(Error::new(
										ErrorKind::InvalidData,
										"conflicting cleanup intent",
									));
								}
								table
									.insert(key.as_slice(), value.as_slice())
									.map_err(redb_to_io)?;
							}
							Some(_) => {}
							None => {
								table
									.insert(key.as_slice(), value.as_slice())
									.map_err(redb_to_io)?;
							}
						}
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Record publication and its accounting in one durable transaction.
	pub(crate) async fn mark_cleanup_replacement_published(
		&self,
		slab_id: SlabId,
		uri: String,
		replacement: String,
		bytes: Option<u64>,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let mut key = slab_id.get().to_be_bytes().to_vec();
		key.extend_from_slice(uri.as_bytes());
		#[cfg(test)]
		let fail_after_remove = self
			.fail_cleanup_after_provisional_remove
			.swap(false, std::sync::atomic::Ordering::AcqRel);
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let previous = table
						.get(key.as_slice())
						.map_err(redb_to_io)?
						.map(|value| {
							postcard::from_bytes::<RetainedObjectIntent>(value.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))
						})
						.transpose()?;
					if let Some(ref old) = previous {
						if old.replacement_uri.as_ref() != Some(&replacement) {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"cleanup replacement mismatch",
							));
						}
					}
					let mut replacement_bytes =
						previous.as_ref().and_then(|old| old.replacement_bytes);
					if uri != replacement {
						let mut provisional_key = slab_id.get().to_be_bytes().to_vec();
						provisional_key.extend_from_slice(replacement.as_bytes());
						let provisional = table
							.get(provisional_key.as_slice())
							.map_err(redb_to_io)?
							.map(|value| {
								postcard::from_bytes::<RetainedObjectIntent>(value.value())
									.map_err(|e| Error::new(ErrorKind::InvalidData, e))
							})
							.transpose()?;
						if let Some(provisional) = provisional.filter(|p| {
							matches!(
								p.state,
								RetainedObjectIntentState::Unresolved
									| RetainedObjectIntentState::CandidateNotSubmitted
							) && p.replacement_uri.as_deref() == Some(replacement.as_str())
						}) {
							replacement_bytes = replacement_bytes.or(provisional.bytes);
							table
								.remove(provisional_key.as_slice())
								.map_err(redb_to_io)?;
						}
					}
					#[cfg(test)]
					if fail_after_remove {
						return Err(Error::other("injected cleanup transaction failure"));
					}
					let mut published_replacements = previous
						.as_ref()
						.map(|old| old.published_replacements.clone())
						.unwrap_or_default();
					published_replacements.insert(replacement.clone());
					let retirement_source = previous.as_ref().and_then(|old| old.retirement_source);
					let intent = RetainedObjectIntent {
						bytes: previous.map_or(bytes, |old| old.bytes),
						replacement_uri: Some(replacement),
						replacement_bytes,
						published_replacements,
						retirement_source,
						state: RetainedObjectIntentState::ReplacementPublished,
					};
					let value = postcard::to_stdvec(&intent)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					table
						.insert(key.as_slice(), value.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Stage a snapshot-superseded live object without remote I/O.
	/// If its prior publication has an unresolved predecessor, record that
	/// evidence in the same transaction as the new retained record.
	pub(crate) async fn stage_snapshot_retained_object(
		&self,
		slab_id: SlabId,
		uri: String,
		retained: RetainedObjectIntent,
		evidence: Vec<(String, RetainedObjectIntent)>,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let mut key = slab_id.get().to_be_bytes().to_vec();
		key.extend_from_slice(uri.as_bytes());
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let existing = table
						.get(key.as_slice())
						.map_err(redb_to_io)?
						.map(|v| {
							postcard::from_bytes::<RetainedObjectIntent>(v.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))
						})
						.transpose()?;
					let mut write_retained = true;
					if let Some(prior) = existing {
						let provisional = matches!(
							prior.state,
							RetainedObjectIntentState::Unresolved
								| RetainedObjectIntentState::CandidateNotSubmitted
						) && prior.replacement_uri.as_deref()
							== Some(uri.as_str());
						if prior == retained {
							write_retained = false;
						} else if !provisional {
							let valid_next = prior.state
								== RetainedObjectIntentState::ReplacementPublished
								&& retained.state == RetainedObjectIntentState::Unresolved
								&& prior.bytes == retained.bytes
								&& prior
									.published_replacements
									.is_subset(&retained.published_replacements)
								&& prior.replacement_uri.as_ref().is_none_or(|uri| {
									retained.published_replacements.contains(uri)
								});
							if !valid_next {
								return Err(Error::new(
									ErrorKind::InvalidData,
									"conflicting snapshot retained record",
								));
							}
						}
					}
					for (predecessor, proposed) in evidence {
						let mut predecessor_key = slab_id.get().to_be_bytes().to_vec();
						predecessor_key.extend_from_slice(predecessor.as_bytes());
						let prior = table
							.get(predecessor_key.as_slice())
							.map_err(redb_to_io)?
							.map(|v| {
								postcard::from_bytes::<RetainedObjectIntent>(v.value())
									.map_err(|e| Error::new(ErrorKind::InvalidData, e))
							})
							.transpose()?;
						if prior
							.as_ref()
							.is_some_and(|p| p.replacement_uri.as_deref() != Some(uri.as_str()))
						{
							return Err(Error::new(
								ErrorKind::InvalidData,
								"snapshot predecessor replacement mismatch",
							));
						}
						if !prior.as_ref().is_some_and(|p| {
							p.state == RetainedObjectIntentState::ReplacementPublished
						}) {
							let mut published = prior.unwrap_or(proposed);
							published.published_replacements.insert(uri.clone());
							published.state = RetainedObjectIntentState::ReplacementPublished;
							let value = postcard::to_stdvec(&published)
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
							table
								.insert(predecessor_key.as_slice(), value.as_slice())
								.map_err(redb_to_io)?;
						}
					}
					if write_retained {
						let value = postcard::to_stdvec(&retained)
							.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
						table
							.insert(key.as_slice(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// The caller either has not invoked the committer yet, or carries B1N's
	/// pre-client_write refusal proof. A returned Raft error is insufficient.
	pub(crate) async fn set_tier_candidate_proposal_state(
		&self,
		slab_id: SlabId,
		uri: String,
		not_submitted: bool,
		bytes: Option<u64>,
	) -> std::io::Result<RetainedObjectIntent> {
		self.require_cleanup_authority()?;
		let mut key = slab_id.get().to_be_bytes().to_vec();
		key.extend_from_slice(uri.as_bytes());
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let next = {
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let prior = table
						.get(key.as_slice())
						.map_err(redb_to_io)?
						.map(|v| {
							postcard::from_bytes::<RetainedObjectIntent>(v.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))
						})
						.transpose()?;
					let mut next = match prior {
						Some(prior)
							if matches!(
								prior.state,
								RetainedObjectIntentState::Unresolved
									| RetainedObjectIntentState::CandidateNotSubmitted
							) && (not_submitted
								|| prior.state
									== RetainedObjectIntentState::CandidateNotSubmitted)
								&& prior.replacement_uri.as_deref() == Some(uri.as_str())
								&& prior.published_replacements.is_empty()
								&& prior.retirement_source.is_none() =>
						{
							prior
						}
						None if not_submitted => RetainedObjectIntent {
							bytes,
							replacement_uri: Some(uri.clone()),
							replacement_bytes: None,
							published_replacements: Default::default(),
							retirement_source: None,
							state: RetainedObjectIntentState::CandidateNotSubmitted,
						},
						_ => {
							return Err(Error::new(
								ErrorKind::WouldBlock,
								"tier candidate is not a fresh provisional",
							));
						}
					};
					next.state = if not_submitted {
						RetainedObjectIntentState::CandidateNotSubmitted
					} else {
						RetainedObjectIntentState::Unresolved
					};
					let encoded = postcard::to_stdvec(&next)
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					table
						.insert(key.as_slice(), encoded.as_slice())
						.map_err(redb_to_io)?;
					next
				};
				tx.commit().map_err(redb_to_io)?;
				Ok(next)
			})
			.await
	}

	/// Finish or idempotently observe our atomic rollback after deleting only
	/// the never-submitted fresh candidate. Refresh the caller's stale parents
	/// after cancellation without overwriting newer publication history.
	pub(crate) async fn clear_unsubmitted_tier_candidate(
		&self,
		slab_id: SlabId,
		uri: String,
		expected_parents: Vec<String>,
	) -> std::io::Result<Vec<(String, Option<RetainedObjectIntent>)>> {
		self.require_cleanup_authority()?;
		let prefix = slab_id.get().to_be_bytes();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let changes = {
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let mut records =
						std::collections::BTreeMap::<String, RetainedObjectIntent>::new();
					for row in table.iter().map_err(redb_to_io)? {
						let (key, value) = row.map_err(redb_to_io)?;
						if key.value().starts_with(&prefix) {
							let name = std::str::from_utf8(&key.value()[8..])
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))?
								.to_owned();
							let record = postcard::from_bytes(value.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
							records.insert(name, record);
						}
					}
					let valid = match records.get(&uri) {
						Some(r) => {
							r.state == RetainedObjectIntentState::CandidateNotSubmitted
								&& r.replacement_uri.as_deref() == Some(uri.as_str())
								&& r.published_replacements.is_empty()
								&& r.retirement_source.is_none()
						}
						// The only other clearer consumes publication evidence and
						// leaves its parent edge. No candidate and no such edge is
						// the completed atomic rollback, not an absence-only proof.
						None => !records
							.values()
							.any(|r| r.replacement_uri.as_deref() == Some(uri.as_str())),
					};
					if !valid || records.values().any(|r| r.was_published(&uri)) {
						return Err(Error::new(
							ErrorKind::WouldBlock,
							"candidate submission/publication proof changed",
						));
					}
					let mut affected = expected_parents
						.into_iter()
						.collect::<std::collections::BTreeSet<_>>();
					affected.insert(uri.clone());
					records.remove(&uri);
					let parents = records
						.iter()
						.filter(|(_, r)| {
							r.state == RetainedObjectIntentState::Unresolved
								&& r.replacement_uri.as_deref() == Some(uri.as_str())
						})
						.map(|(name, _)| name.clone())
						.collect::<Vec<_>>();
					for name in parents {
						affected.insert(name.clone());
						let record = records.get_mut(&name).unwrap();
						if record.published_replacements.is_empty()
							&& record.retirement_source.is_none()
						{
							records.remove(&name);
						} else {
							record.replacement_uri = None;
							record.replacement_bytes = None;
							record.state = if record.retirement_source.is_some() {
								RetainedObjectIntentState::RetirementPublished
							} else {
								RetainedObjectIntentState::ReplacementPublished
							};
						}
					}
					let mut changes = Vec::new();
					for name in affected {
						let mut key = prefix.to_vec();
						key.extend_from_slice(name.as_bytes());
						let record = records.get(&name).cloned();
						match &record {
							Some(record) => {
								let bytes = postcard::to_stdvec(record)
									.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
								table
									.insert(key.as_slice(), bytes.as_slice())
									.map_err(redb_to_io)?;
							}
							None => {
								table.remove(key.as_slice()).map_err(redb_to_io)?;
							}
						}
						changes.push((name, record));
					}
					changes
				};
				tx.commit().map_err(redb_to_io)?;
				Ok(changes)
			})
			.await
	}

	/// Remove a first-tier provisional object once it is known live. A
	/// replacement owned by an old-object intent must resolve with that row.
	pub(crate) async fn remove_standalone_live_provisional(
		&self,
		slab_id: SlabId,
		uri: String,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let mut key = slab_id.get().to_be_bytes().to_vec();
		key.extend_from_slice(uri.as_bytes());
		let prefix = slab_id.get().to_be_bytes();
		let _write = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx
						.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					for row in table.iter().map_err(redb_to_io)? {
						let (other_key, value) = row.map_err(redb_to_io)?;
						if other_key.value().starts_with(&prefix)
							&& other_key.value() != key.as_slice()
						{
							let record: RetainedObjectIntent = postcard::from_bytes(value.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
							if record.replacement_uri.as_deref() == Some(uri.as_str()) {
								return Err(Error::new(
									ErrorKind::WouldBlock,
									"provisional belongs to an old-object intent",
								));
							}
						}
					}
					let prior = table
						.get(key.as_slice())
						.map_err(redb_to_io)?
						.map(|v| {
							postcard::from_bytes::<RetainedObjectIntent>(v.value())
								.map_err(|e| Error::new(ErrorKind::InvalidData, e))
						})
						.transpose()?;
					if let Some(prior) = prior {
						if !matches!(
							prior.state,
							RetainedObjectIntentState::Unresolved
								| RetainedObjectIntentState::CandidateNotSubmitted
						) || prior.replacement_uri.as_deref() != Some(uri.as_str())
						{
							return Err(Error::new(
								ErrorKind::InvalidData,
								"not a standalone provisional record",
							));
						}
						table.remove(key.as_slice()).map_err(redb_to_io)?;
					}
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	pub(crate) async fn load_snapshot_cleanup_intents(
		&self,
	) -> std::io::Result<Vec<(SlabId, String, RetainedObjectIntent)>> {
		self.require_cleanup_authority()?;
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx
					.open_table(SNAPSHOT_CLEANUP_INTENTS_TABLE)
					.map_err(redb_to_io)?;
				let mut records = Vec::new();
				for row in table.iter().map_err(redb_to_io)? {
					let (key, bytes) = row.map_err(redb_to_io)?;
					let key = key.value();
					if key.len() <= 8 {
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"invalid snapshot cleanup key",
						));
					}
					let slab_id = SlabId::new(u64::from_be_bytes(key[..8].try_into().unwrap()));
					let uri = std::str::from_utf8(&key[8..])
						.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
						.to_owned();
					let intent = postcard::from_bytes(bytes.value())
						.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
					records.push((slab_id, uri, intent));
				}
				Ok(records)
			})
			.await
	}

	/// ADR 0097: every receipt in the table, as raw key/value bytes.
	///
	/// Decoding is the policy layer's job. Returning raw pairs keeps the failure
	/// boundary honest: an unreadable TABLE is an index error, while an
	/// undecodable RECEIPT is a policy error, and only the policy layer can decide
	/// that the right response to the latter is to refuse maintenance rather than
	/// to continue with a silently shorter history.
	///
	/// The copy is BOUNDED while iterating, not after: a corrupted or indefinitely
	/// accumulated table must fail closed before it can exhaust memory, or the
	/// policy layer never gets its chance to refuse. Every bound is checked
	/// against the borrowed entry before any byte of it is copied. The bounds come
	/// from the caller because the value's maximum is fixed by the receipt schema,
	/// which this layer deliberately does not know.
	pub(crate) async fn load_maintenance_receipts(
		&self,
		bounds: ReceiptLoadBounds,
	) -> std::io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
		self.db
			.run(move |db| {
				let read_txn = db.begin_read().map_err(redb_to_io)?;
				let table = read_txn
					.open_table(MAINTENANCE_RECEIPTS_TABLE)
					.map_err(redb_to_io)?;
				let mut out = Vec::new();
				let mut total_bytes = 0usize;
				for entry in table.iter().map_err(redb_to_io)? {
					let (key, value) = entry.map_err(redb_to_io)?;
					let (key, value) = (key.value(), value.value());
					if out.len() >= bounds.max_rows {
						return Err(receipt_load_exceeded(format!(
							"more than {} maintenance receipts",
							bounds.max_rows
						)));
					}
					if key.len() > bounds.max_key_bytes {
						return Err(receipt_load_exceeded(format!(
							"a maintenance receipt key of {} bytes exceeds {}",
							key.len(),
							bounds.max_key_bytes
						)));
					}
					if value.len() > bounds.max_value_bytes {
						return Err(receipt_load_exceeded(format!(
							"a maintenance receipt value of {} bytes exceeds {}",
							value.len(),
							bounds.max_value_bytes
						)));
					}
					total_bytes = total_bytes
						.checked_add(key.len())
						.and_then(|total| total.checked_add(value.len()))
						.filter(|total| *total <= bounds.max_total_bytes)
						.ok_or_else(|| {
							receipt_load_exceeded(format!(
								"maintenance receipts exceed {} bytes in total",
								bounds.max_total_bytes
							))
						})?;
					out.push((key.to_vec(), value.to_vec()));
				}
				Ok(out)
			})
			.await
	}

	/// Durably record one receipt. The caller commits this BEFORE exposing the
	/// cooldown or quarantine it describes, so a crash between the two leaves a
	/// receipt that over-counts rather than a bound that under-counts.
	pub(crate) async fn put_maintenance_receipt(
		&self,
		key: Vec<u8>,
		value: Vec<u8>,
	) -> std::io::Result<()> {
		// Serialize with index rebuilds and with each other: a receipt write that
		// interleaved with a rebuild could land in a table the rebuild then replaces.
		let _guard = self.write_lock.lock().await;
		#[cfg(test)]
		if self
			.fail_receipt_mutations
			.load(std::sync::atomic::Ordering::SeqCst)
		{
			return Err(Error::other("injected receipt store failure"));
		}
		self.db
			.run(move |db| {
				let write_txn = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = write_txn
						.open_table(MAINTENANCE_RECEIPTS_TABLE)
						.map_err(redb_to_io)?;
					table
						.insert(key.as_slice(), value.as_slice())
						.map_err(redb_to_io)?;
				}
				write_txn.commit().map_err(redb_to_io)?;
				Ok(())
			})
			.await
	}

	/// Remove receipts. Used by success (every origin for one slab set) and by the
	/// explicit requeue. Durable removal happens FIRST; in-memory state is cleared
	/// only after this returns, so a failure here leaves the quarantine effective.
	pub(crate) async fn delete_maintenance_receipts(
		&self,
		keys: Vec<Vec<u8>>,
	) -> std::io::Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		let _guard = self.write_lock.lock().await;
		#[cfg(test)]
		if self
			.fail_receipt_mutations
			.load(std::sync::atomic::Ordering::SeqCst)
		{
			return Err(Error::other("injected receipt store failure"));
		}
		self.db
			.run(move |db| {
				let write_txn = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = write_txn
						.open_table(MAINTENANCE_RECEIPTS_TABLE)
						.map_err(redb_to_io)?;
					for key in &keys {
						table.remove(key.as_slice()).map_err(redb_to_io)?;
					}
				}
				write_txn.commit().map_err(redb_to_io)?;
				Ok(())
			})
			.await
	}

	#[cfg(test)]
	pub(crate) async fn upsert_record(&self, row: PersistentRecordRow) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			put_records: vec![row],
			..Default::default()
		})
		.await
	}

	#[cfg(test)]
	pub(crate) async fn upsert_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			put_tiered_slabs: vec![pointer],
			last_control_log_index,
			..Default::default()
		})
		.await
	}

	#[cfg(test)]
	pub(crate) async fn upsert_secondary_index_segment(
		&self,
		row: PersistentSecondaryIndexSegmentRow,
	) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			put_secondary_index_segments: vec![row],
			..Default::default()
		})
		.await
	}

	#[cfg(test)]
	pub(crate) async fn delete_secondary_index_segment(
		&self,
		key: PersistentSecondaryIndexSegmentKey,
	) -> std::io::Result<()> {
		self.apply(PersistentIndexUpdate {
			delete_secondary_index_segments: vec![key],
			..Default::default()
		})
		.await
	}
}

pub(crate) fn persistent_index_path(dir: &Path) -> PathBuf {
	dir.join("storage_index.redb")
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

async fn spawn_blocking_owned_io<T>(
	owner: Option<DataDirectoryOwner>,
	f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T>
where
	T: Send + 'static,
{
	let permit = owner
		.as_ref()
		.map(DataDirectoryOwner::write_permit)
		.transpose()?;
	spawn_blocking_io(move || {
		let _owner = owner;
		let _permit = permit;
		f()
	})
	.await
}

fn redb_to_io(err: impl std::fmt::Display) -> std::io::Error {
	Error::other(format!("redb error: {err}"))
}

/// Stable digest of durable row keys and values for an LSM input set.
pub(crate) fn lsm_rows_digest(rows: impl IntoIterator<Item = PersistentRecordRow>) -> [u8; 32] {
	let mut rows = rows
		.into_iter()
		.map(|row| (encode_record_key(row), encode_record_value(row)))
		.collect::<Vec<_>>();
	rows.sort_unstable();
	let mut digest = blake3::Hasher::new();
	for (key, value) in rows {
		digest.update(&key);
		digest.update(&value);
	}
	*digest.finalize().as_bytes()
}

pub(crate) fn lsm_input_rows_digest(
	snapshot: &PersistentIndexSnapshot,
	inputs: &std::collections::HashSet<SlabId>,
) -> [u8; 32] {
	lsm_rows_digest(
		snapshot
			.record_rows
			.iter()
			.filter(|row| inputs.contains(&row.location.slab_id))
			.copied(),
	)
}

fn encode_record_key(row: PersistentRecordRow) -> [u8; RECORD_KEY_LEN] {
	let mut bytes = [0u8; RECORD_KEY_LEN];
	let mut cursor = 0;
	bytes[cursor..cursor + 8].copy_from_slice(&row.tenant_id.get().to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 32].copy_from_slice(row.key_identity.as_bytes());
	cursor += 32;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.hlc.physical_ms.to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 4].copy_from_slice(&row.location.hlc.logical.to_le_bytes());
	cursor += 4;
	bytes[cursor..cursor + 4].copy_from_slice(&row.location.hlc.node_id.to_le_bytes());
	cursor += 4;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.slab_id.get().to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 8].copy_from_slice(&row.location.offset.to_le_bytes());
	bytes
}

fn encode_record_value(row: PersistentRecordRow) -> [u8; RECORD_VALUE_LEN] {
	let mut bytes = [0u8; RECORD_VALUE_LEN];
	bytes[..4].copy_from_slice(&row.location.length.to_le_bytes());
	bytes[4..12].copy_from_slice(&row.routing_locality.get().to_le_bytes());
	bytes[12..16].copy_from_slice(&row.location.record_row_index.get().to_le_bytes());
	let mut flags = 0_u32;
	if row.location.is_tombstone {
		flags |= RECORD_FLAG_IS_TOMBSTONE;
	}
	if row.table_object_id.is_some() {
		flags |= RECORD_FLAG_HAS_TABLE_OBJECT_ID;
	}
	bytes[16..20].copy_from_slice(&flags.to_le_bytes());
	if let Some(table_object_id) = row.table_object_id {
		bytes[20..36].copy_from_slice(table_object_id.as_bytes());
	}
	bytes[36] = match row.placement {
		PlacementSemantics::RawLocality => PLACEMENT_STAMP_RAW_LOCALITY,
		PlacementSemantics::Partitioned => PLACEMENT_STAMP_PARTITIONED,
	};
	let partition = row.partition.map_or(0, |partition| partition.get());
	bytes[37..45].copy_from_slice(&partition.to_le_bytes());
	bytes
}

fn decode_record_row(key: &[u8], value: &[u8]) -> std::io::Result<PersistentRecordRow> {
	if key.len() != RECORD_KEY_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"persistent record key length mismatch: expected {RECORD_KEY_LEN}, got {}",
				key.len()
			),
		));
	}
	if value.len() != RECORD_VALUE_LEN
		&& value.len() != RECORD_VALUE_LEN_V4
		&& value.len() != RECORD_VALUE_LEN_V3
		&& value.len() != RECORD_VALUE_LEN_V2
		&& value.len() != RECORD_VALUE_LEN_V1
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"persistent record value length mismatch: expected {RECORD_VALUE_LEN_V1}, {RECORD_VALUE_LEN_V2}, {RECORD_VALUE_LEN_V3}, {RECORD_VALUE_LEN_V4}, or {RECORD_VALUE_LEN}, got {}",
				value.len()
			),
		));
	}

	let mut cursor = 0;
	let tenant_id = TenantId::new(read_u64(key, &mut cursor)?);
	let key_identity = Identity::new(read_array_32(key, &mut cursor)?);
	let physical_ms = read_u64(key, &mut cursor)?;
	let logical = read_u32(key, &mut cursor)?;
	let node_id = read_u32(key, &mut cursor)?;
	let slab_id = read_u64(key, &mut cursor)?;
	let offset = read_u64(key, &mut cursor)?;
	let length = u32::from_le_bytes(value[..4].try_into().expect("validated above"));
	let routing_locality = if value.len() >= RECORD_VALUE_LEN_V2 {
		Locality::new(u64::from_le_bytes(
			value[4..12].try_into().expect("validated above"),
		))
	} else {
		Locality::from(key_identity)
	};
	let record_row_index = if value.len() >= RECORD_VALUE_LEN_V3 {
		RecordRowIndex::new(u32::from_le_bytes(
			value[12..16].try_into().expect("validated above"),
		))
	} else {
		RecordRowIndex::ZERO
	};
	let flags = if value.len() >= RECORD_VALUE_LEN_V3 {
		u32::from_le_bytes(value[16..20].try_into().expect("validated above"))
	} else {
		0
	};
	if flags & !(RECORD_FLAG_IS_TOMBSTONE | RECORD_FLAG_HAS_TABLE_OBJECT_ID) != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("persistent record row has unknown flags 0x{flags:08x}"),
		));
	}
	let table_object_id = if flags & RECORD_FLAG_HAS_TABLE_OBJECT_ID != 0 {
		if value.len() < RECORD_VALUE_LEN_V4 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"persistent record row declares a table object id without V4 storage",
			));
		}
		Some(TableObjectId::new(
			value[20..36].try_into().expect("validated V4 record value"),
		))
	} else {
		None
	};

	// ADR 0090: a value shorter than V5 predates partitions and is legacy by
	// length. A V5-length value carries an explicit stamp, and an unrecognized
	// stamp is malformed rather than quietly legacy — absence and damage must
	// not share an outcome.
	let (placement, partition) = if value.len() >= RECORD_VALUE_LEN_V5 {
		let stamp = value[36];
		match stamp {
			PLACEMENT_STAMP_RAW_LOCALITY => (PlacementSemantics::RawLocality, None),
			PLACEMENT_STAMP_PARTITIONED => (
				PlacementSemantics::Partitioned,
				Some(PartitionId::new(u64::from_le_bytes(
					value[37..45].try_into().expect("validated V5 record value"),
				))),
			),
			other => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"persistent record row has unknown placement stamp {other}; \
						 a V5 value with an unrecognized stamp is malformed, not legacy"
					),
				));
			}
		}
	} else {
		(PlacementSemantics::RawLocality, None)
	};

	Ok(PersistentRecordRow {
		tenant_id,
		key_identity,
		routing_locality,
		table_object_id,
		placement,
		partition,
		location: RecordLocation {
			placement,
			partition,
			slab_id: SlabId::new(slab_id),
			offset,
			length,
			hlc: HybridTimestamp {
				physical_ms,
				logical,
				node_id,
			},
			record_row_index,
			is_tombstone: flags & RECORD_FLAG_IS_TOMBSTONE != 0,
		},
	})
}

fn encode_secondary_index_segment_key(
	key: PersistentSecondaryIndexSegmentKey,
) -> [u8; SECONDARY_INDEX_SEGMENT_KEY_LEN] {
	let mut bytes = [0u8; SECONDARY_INDEX_SEGMENT_KEY_LEN];
	let mut cursor = 0;
	bytes[cursor..cursor + 8].copy_from_slice(&key.tenant_id.get().to_le_bytes());
	cursor += 8;
	bytes[cursor..cursor + 16].copy_from_slice(key.table_object_id.as_bytes());
	cursor += 16;
	bytes[cursor..cursor + 16].copy_from_slice(key.index_object_id.as_bytes());
	cursor += 16;
	bytes[cursor..cursor + 32].copy_from_slice(key.logical_schema_fingerprint.as_bytes());
	cursor += 32;
	bytes[cursor..cursor + 8].copy_from_slice(&key.source_slab_id.get().to_le_bytes());
	bytes
}

fn encode_secondary_index_segment_value(row: &PersistentSecondaryIndexSegmentRow) -> Vec<u8> {
	let segment_path = row.segment_path.as_bytes();
	let l2_uri = row.l2_uri.as_deref().unwrap_or("").as_bytes();
	let mut flags = 0_u32;
	if row.l2_uri.is_some() {
		flags |= SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI;
	}
	let mut bytes = Vec::with_capacity(
		SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN + segment_path.len() + l2_uri.len(),
	);
	bytes.extend_from_slice(&row.payload_hash);
	bytes.extend_from_slice(&row.state_digest);
	bytes.extend_from_slice(&row.format_version.to_le_bytes());
	bytes.extend_from_slice(&row.entry_count.to_le_bytes());
	bytes.extend_from_slice(&(segment_path.len() as u32).to_le_bytes());
	bytes.extend_from_slice(&flags.to_le_bytes());
	bytes.extend_from_slice(segment_path);
	bytes.extend_from_slice(l2_uri);
	bytes
}

fn decode_secondary_index_segment_row(
	key: &[u8],
	value: &[u8],
) -> std::io::Result<PersistentSecondaryIndexSegmentRow> {
	if key.len() != SECONDARY_INDEX_SEGMENT_KEY_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"secondary index segment key length mismatch: expected {SECONDARY_INDEX_SEGMENT_KEY_LEN}, got {}",
				key.len()
			),
		));
	}
	if value.len() < SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"secondary index segment value too short: expected at least {SECONDARY_INDEX_SEGMENT_VALUE_PREFIX_LEN}, got {}",
				value.len()
			),
		));
	}

	let mut key_cursor = 0;
	let tenant_id = TenantId::new(read_u64(key, &mut key_cursor)?);
	let table_object_id = TableObjectId::new(read_array_16(key, &mut key_cursor)?);
	let index_object_id = IndexObjectId::new(read_array_16(key, &mut key_cursor)?);
	let logical_schema_fingerprint =
		LogicalSchemaFingerprint::from_bytes(read_array_32(key, &mut key_cursor)?);
	let source_slab_id = SlabId::new(read_u64(key, &mut key_cursor)?);

	let mut value_cursor = 0;
	let payload_hash = read_array_32(value, &mut value_cursor)?;
	let state_digest = read_array_32(value, &mut value_cursor)?;
	let format_version = read_u16(value, &mut value_cursor)?;
	let entry_count = read_u64(value, &mut value_cursor)?;
	let segment_path_len = read_u32(value, &mut value_cursor)? as usize;
	let flags = read_u32(value, &mut value_cursor)?;
	if flags & !SECONDARY_INDEX_SEGMENT_FLAG_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("secondary index segment row has unknown flags 0x{flags:08x}"),
		));
	}
	let segment_path_end = value_cursor.checked_add(segment_path_len).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"secondary index path length overflows",
		)
	})?;
	let segment_path_bytes = value.get(value_cursor..segment_path_end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"secondary index segment value truncated while reading segment_path",
		)
	})?;
	let segment_path = std::str::from_utf8(segment_path_bytes)
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("segment_path utf8: {err}")))?
		.to_string();
	value_cursor = segment_path_end;
	let remaining = &value[value_cursor..];
	let l2_uri = if flags & SECONDARY_INDEX_SEGMENT_FLAG_HAS_L2_URI != 0 {
		Some(
			std::str::from_utf8(remaining)
				.map_err(|err| Error::new(ErrorKind::InvalidData, format!("l2_uri utf8: {err}")))?
				.to_string(),
		)
	} else if remaining.is_empty() {
		None
	} else {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"secondary index segment row has trailing bytes without l2_uri flag",
		));
	};

	Ok(PersistentSecondaryIndexSegmentRow {
		key: PersistentSecondaryIndexSegmentKey {
			tenant_id,
			table_object_id,
			index_object_id,
			logical_schema_fingerprint,
			source_slab_id,
		},
		segment_path,
		l2_uri,
		payload_hash,
		state_digest,
		format_version,
		entry_count,
	})
}

fn encode_lsm_run_summary(run: &LsmRunSummary) -> std::io::Result<Vec<u8>> {
	let mut bytes = Vec::with_capacity(LSM_RUN_VALUE_PREFIX_LEN);
	bytes.push(run.compaction_level.get());
	bytes.push(encode_lsm_slab_state_tag(&run.state));
	bytes.push(encode_lsm_sidecar_coverage(run.sidecar_coverage));
	bytes.push(0);
	bytes.extend_from_slice(&run.manifest_epoch.get().to_le_bytes());
	encode_hlc(&mut bytes, run.min_row_hlc);
	encode_hlc(&mut bytes, run.max_row_hlc);
	bytes.extend_from_slice(&run.size_bytes.get().to_le_bytes());
	bytes.extend_from_slice(&run.row_count.get().to_le_bytes());
	bytes.extend_from_slice(&run.retention_debt_score.to_le_bytes());
	bytes.extend_from_slice(&run.tombstone_debt_score.to_le_bytes());
	bytes.extend_from_slice(&run.index_coverage_debt_score.to_le_bytes());
	bytes.extend_from_slice(&run.layout_debt_score.to_le_bytes());
	let successors = match &run.state {
		SlabState::Retired { successors } => successors.as_slice(),
		_ => &[],
	};
	let successor_count = u32::try_from(successors.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"LSM run successor count does not fit in u32",
		)
	})?;
	bytes.extend_from_slice(&successor_count.to_le_bytes());
	for successor in successors {
		bytes.extend_from_slice(&successor.get().to_le_bytes());
	}
	Ok(bytes)
}

fn decode_lsm_run_summary(slab_id: SlabId, value: &[u8]) -> std::io::Result<LsmRunSummary> {
	if value.len() < LSM_RUN_VALUE_PREFIX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"LSM run value too short: expected at least {LSM_RUN_VALUE_PREFIX_LEN}, got {}",
				value.len()
			),
		));
	}
	let mut cursor = 0usize;
	let compaction_level = read_u8(value, &mut cursor).and_then(|raw| {
		CompactionLevelIndex::try_new(raw).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("LSM run has invalid compaction level {raw}"),
			)
		})
	})?;
	let state_tag = read_u8(value, &mut cursor)?;
	let sidecar_coverage = decode_lsm_sidecar_coverage(read_u8(value, &mut cursor)?)?;
	let reserved = read_u8(value, &mut cursor)?;
	if reserved != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("LSM run reserved byte is non-zero: {reserved}"),
		));
	}
	let manifest_epoch = ManifestEpoch::new(read_u64(value, &mut cursor)?);
	let min_row_hlc = read_hlc(value, &mut cursor)?;
	let max_row_hlc = read_hlc(value, &mut cursor)?;
	let size_bytes = ByteCount::new(read_u64(value, &mut cursor)?);
	let row_count = RowCount::new(read_u64(value, &mut cursor)?);
	let retention_debt_score = read_u32(value, &mut cursor)?;
	let tombstone_debt_score = read_u32(value, &mut cursor)?;
	let index_coverage_debt_score = read_u32(value, &mut cursor)?;
	let layout_debt_score = read_u32(value, &mut cursor)?;
	let successor_count = read_u32(value, &mut cursor)? as usize;
	let successor_bytes = successor_count
		.checked_mul(8)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "LSM successor byte count overflows"))?;
	if value.len() != cursor + successor_bytes {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LSM run value has truncated or trailing successor bytes",
		));
	}
	let mut successors = Vec::with_capacity(successor_count);
	for _ in 0..successor_count {
		successors.push(SlabId::new(read_u64(value, &mut cursor)?));
	}
	let state = match state_tag {
		LSM_SLAB_STATE_LIVE if successors.is_empty() => SlabState::Live,
		LSM_SLAB_STATE_RETIRED => SlabState::Retired { successors },
		LSM_SLAB_STATE_DELETE_PENDING if successors.is_empty() => SlabState::DeletePending,
		_ => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("LSM run has invalid state tag/successor combination {state_tag}"),
			));
		}
	};
	Ok(LsmRunSummary {
		slab_id,
		compaction_level,
		state,
		min_row_hlc,
		max_row_hlc,
		size_bytes,
		row_count,
		sidecar_coverage,
		manifest_epoch,
		retention_debt_score,
		tombstone_debt_score,
		index_coverage_debt_score,
		layout_debt_score,
	})
}

fn encode_hlc(bytes: &mut Vec<u8>, hlc: HybridTimestamp) {
	bytes.extend_from_slice(&hlc.physical_ms.to_le_bytes());
	bytes.extend_from_slice(&hlc.logical.to_le_bytes());
	bytes.extend_from_slice(&hlc.node_id.to_le_bytes());
}

fn read_hlc(bytes: &[u8], cursor: &mut usize) -> std::io::Result<HybridTimestamp> {
	Ok(HybridTimestamp {
		physical_ms: read_u64(bytes, cursor)?,
		logical: read_u32(bytes, cursor)?,
		node_id: read_u32(bytes, cursor)?,
	})
}

fn encode_lsm_slab_state_tag(state: &SlabState) -> u8 {
	match state {
		SlabState::Live => LSM_SLAB_STATE_LIVE,
		SlabState::Retired { .. } => LSM_SLAB_STATE_RETIRED,
		SlabState::DeletePending => LSM_SLAB_STATE_DELETE_PENDING,
	}
}

fn encode_lsm_sidecar_coverage(coverage: SidecarCoverage) -> u8 {
	match coverage {
		SidecarCoverage::None => LSM_SIDECAR_COVERAGE_NONE,
		SidecarCoverage::Partial => LSM_SIDECAR_COVERAGE_PARTIAL,
		SidecarCoverage::Complete => LSM_SIDECAR_COVERAGE_COMPLETE,
	}
}

fn decode_lsm_sidecar_coverage(raw: u8) -> std::io::Result<SidecarCoverage> {
	match raw {
		LSM_SIDECAR_COVERAGE_NONE => Ok(SidecarCoverage::None),
		LSM_SIDECAR_COVERAGE_PARTIAL => Ok(SidecarCoverage::Partial),
		LSM_SIDECAR_COVERAGE_COMPLETE => Ok(SidecarCoverage::Complete),
		_ => Err(Error::new(
			ErrorKind::InvalidData,
			format!("LSM run has invalid sidecar coverage {raw}"),
		)),
	}
}

fn encode_tiered_pointer(pointer: &TieredSlabPointer) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(TIERED_POINTER_PREFIX_LEN + pointer.l2_uri.len());
	bytes.extend_from_slice(&pointer.payload_hash);
	let mut flags = 0u8;
	let mut flags2 = 0u8;
	if pointer.is_encrypted {
		flags |= TIERED_FLAG_IS_ENCRYPTED;
	}
	if pointer.tenant_id.is_some() {
		flags |= TIERED_FLAG_HAS_TENANT_ID;
	}
	if pointer.tenant_kek_id.is_some() {
		flags |= TIERED_FLAG_HAS_TENANT_KEK_ID;
	}
	if pointer.stats_format_version.is_some() {
		flags |= TIERED_FLAG_HAS_STATS_FORMAT_VERSION;
	}
	if pointer.stats_block_len.is_some() {
		flags |= TIERED_FLAG_HAS_STATS_BLOCK_LEN;
	}
	if pointer.stats_block_hash128.is_some() {
		flags |= TIERED_FLAG_HAS_STATS_BLOCK_HASH128;
	}
	match pointer.tombstone_metadata {
		TombstoneMetadata::Untracked => {}
		TombstoneMetadata::None => {
			flags |= TIERED_FLAG_TOMBSTONE_METADATA_TRACKED;
		}
		TombstoneMetadata::Earliest(_) => {
			flags |= TIERED_FLAG_TOMBSTONE_METADATA_TRACKED | TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC;
		}
	}
	match pointer.supersession_metadata {
		SupersessionMetadata::Untracked => {}
		SupersessionMetadata::None => {
			flags2 |= TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED;
		}
		SupersessionMetadata::Earliest(_) => {
			flags2 |= TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED
				| TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC;
		}
	}
	bytes.push(flags);
	bytes.push(flags2);
	if let Some(tenant_id) = pointer.tenant_id {
		bytes.extend_from_slice(&tenant_id.get().to_le_bytes());
	}
	if let Some(tenant_kek_id) = pointer.tenant_kek_id {
		bytes.extend_from_slice(&tenant_kek_id.get().to_le_bytes());
	}
	if let Some(stats_format_version) = pointer.stats_format_version {
		bytes.extend_from_slice(&stats_format_version.to_le_bytes());
	}
	if let Some(stats_block_len) = pointer.stats_block_len {
		bytes.extend_from_slice(&stats_block_len.to_le_bytes());
	}
	if let Some(stats_block_hash128) = pointer.stats_block_hash128 {
		bytes.extend_from_slice(&stats_block_hash128);
	}
	if let TombstoneMetadata::Earliest(tombstone_metadata) = pointer.tombstone_metadata {
		bytes.extend_from_slice(&tombstone_metadata.physical_ms.to_le_bytes());
		bytes.extend_from_slice(&tombstone_metadata.logical.to_le_bytes());
		bytes.extend_from_slice(&tombstone_metadata.node_id.to_le_bytes());
	}
	if let SupersessionMetadata::Earliest(supersession_metadata) = pointer.supersession_metadata {
		bytes.extend_from_slice(&supersession_metadata.physical_ms.to_le_bytes());
		bytes.extend_from_slice(&supersession_metadata.logical.to_le_bytes());
		bytes.extend_from_slice(&supersession_metadata.node_id.to_le_bytes());
	}
	bytes.extend_from_slice(pointer.l2_uri.as_bytes());
	bytes
}

fn decode_tiered_pointer(slab_id: SlabId, value: &[u8]) -> std::io::Result<TieredSlabPointer> {
	if value.len() < TIERED_POINTER_PREFIX_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"tiered slab value too short: expected at least {TIERED_POINTER_PREFIX_LEN}, got {}",
				value.len()
			),
		));
	}
	let mut payload_hash = [0u8; TIERED_POINTER_HASH_LEN];
	payload_hash.copy_from_slice(&value[..TIERED_POINTER_HASH_LEN]);
	let flags = value[TIERED_POINTER_HASH_LEN];
	let flags2 = value[TIERED_POINTER_HASH_LEN + 1];
	if flags & !TIERED_FLAG_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("tiered slab value has unknown flags 0x{flags:02x}"),
		));
	}
	if flags2 & !TIERED_FLAG2_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("tiered slab value has unknown flags2 0x{flags2:02x}"),
		));
	}
	let is_encrypted = (flags & TIERED_FLAG_IS_ENCRYPTED) != 0;
	let mut cursor = TIERED_POINTER_PREFIX_LEN;
	let tenant_id = if (flags & TIERED_FLAG_HAS_TENANT_ID) != 0 {
		let end = cursor.saturating_add(8);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding tenant_id",
			)
		})?;
		cursor = end;
		Some(TenantId::new(u64::from_le_bytes(
			slot.try_into().expect("fixed width"),
		)))
	} else {
		None
	};
	let tenant_kek_id = if (flags & TIERED_FLAG_HAS_TENANT_KEK_ID) != 0 {
		let end = cursor.saturating_add(8);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding tenant_kek_id",
			)
		})?;
		cursor = end;
		Some(
			TenantKekId::try_from(u64::from_le_bytes(slot.try_into().expect("fixed width")))
				.map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("invalid tiered slab tenant_kek_id: {err}"),
					)
				})?,
		)
	} else {
		None
	};
	let stats_format_version = if (flags & TIERED_FLAG_HAS_STATS_FORMAT_VERSION) != 0 {
		let end = cursor.saturating_add(2);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding stats_format_version",
			)
		})?;
		cursor = end;
		Some(u16::from_le_bytes(slot.try_into().expect("fixed width")))
	} else {
		None
	};
	let stats_block_len = if (flags & TIERED_FLAG_HAS_STATS_BLOCK_LEN) != 0 {
		let end = cursor.saturating_add(4);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding stats_block_len",
			)
		})?;
		cursor = end;
		Some(u32::from_le_bytes(slot.try_into().expect("fixed width")))
	} else {
		None
	};
	let stats_block_hash128 = if (flags & TIERED_FLAG_HAS_STATS_BLOCK_HASH128) != 0 {
		let end = cursor.saturating_add(16);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding stats_block_hash128",
			)
		})?;
		cursor = end;
		let mut hash = [0_u8; 16];
		hash.copy_from_slice(slot);
		Some(hash)
	} else {
		None
	};
	let tombstone_metadata = if (flags & TIERED_FLAG_HAS_FIRST_TOMBSTONE_HLC) != 0 {
		let end = cursor.saturating_add(16);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding tombstone_metadata",
			)
		})?;
		cursor = end;
		TombstoneMetadata::Earliest(HybridTimestamp {
			physical_ms: u64::from_le_bytes(slot[0..8].try_into().expect("fixed width")),
			logical: u32::from_le_bytes(slot[8..12].try_into().expect("fixed width")),
			node_id: u32::from_le_bytes(slot[12..16].try_into().expect("fixed width")),
		})
	} else if (flags & TIERED_FLAG_TOMBSTONE_METADATA_TRACKED) != 0 {
		TombstoneMetadata::None
	} else {
		TombstoneMetadata::Untracked
	};
	let supersession_metadata = if (flags2 & TIERED_FLAG2_HAS_FIRST_SUPERSESSION_HLC) != 0 {
		let end = cursor.saturating_add(16);
		let slot = value.get(cursor..end).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"tiered slab value truncated while decoding supersession_metadata",
			)
		})?;
		cursor = end;
		SupersessionMetadata::Earliest(HybridTimestamp {
			physical_ms: u64::from_le_bytes(slot[0..8].try_into().expect("fixed width")),
			logical: u32::from_le_bytes(slot[8..12].try_into().expect("fixed width")),
			node_id: u32::from_le_bytes(slot[12..16].try_into().expect("fixed width")),
		})
	} else if (flags2 & TIERED_FLAG2_SUPERSESSION_METADATA_TRACKED) != 0 {
		SupersessionMetadata::None
	} else {
		SupersessionMetadata::Untracked
	};
	let l2_uri = std::str::from_utf8(&value[cursor..])
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("tiered slab URI is not utf8: {err}"),
			)
		})?
		.to_string();
	Ok(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash,
		is_encrypted,
		tenant_id,
		tenant_kek_id,
		stats_format_version,
		stats_block_len,
		stats_block_hash128,
		tombstone_metadata,
		supersession_metadata,
	})
}

/// Decode a v1 tiered pointer record. v1 lacked the
/// `flags_byte`, so any pointer recovered from v1 defaults to
/// `is_encrypted = false`. The plaintext-rewrite sweeper will
/// GET each one, encrypt it, and stamp `is_encrypted = true`
/// back through the v2 table on the next pass — converging on the
/// steady-state short-circuit.
fn decode_tiered_pointer_v1(slab_id: SlabId, value: &[u8]) -> std::io::Result<TieredSlabPointer> {
	if value.len() < TIERED_POINTER_HASH_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"v1 tiered slab value too short: expected at least {TIERED_POINTER_HASH_LEN}, got {}",
				value.len()
			),
		));
	}
	let mut payload_hash = [0u8; TIERED_POINTER_HASH_LEN];
	payload_hash.copy_from_slice(&value[..TIERED_POINTER_HASH_LEN]);
	let l2_uri = std::str::from_utf8(&value[TIERED_POINTER_HASH_LEN..])
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("v1 tiered slab URI is not utf8: {err}"),
			)
		})?
		.to_string();
	Ok(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash,
		is_encrypted: false,
		tenant_id: None,
		tenant_kek_id: None,
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::Untracked,
		supersession_metadata: SupersessionMetadata::Untracked,
	})
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u64> {
	let end = cursor.saturating_add(8);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u64",
		)
	})?;
	*cursor = end;
	Ok(u64::from_le_bytes(slice.try_into().expect("fixed width")))
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u16> {
	let end = cursor.saturating_add(2);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u16",
		)
	})?;
	*cursor = end;
	Ok(u16::from_le_bytes(slice.try_into().expect("fixed width")))
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u8> {
	let byte = bytes.get(*cursor).copied().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u8",
		)
	})?;
	*cursor = (*cursor).saturating_add(1);
	Ok(byte)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> std::io::Result<u32> {
	let end = cursor.saturating_add(4);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading u32",
		)
	})?;
	*cursor = end;
	Ok(u32::from_le_bytes(slice.try_into().expect("fixed width")))
}

fn read_array_16(bytes: &[u8], cursor: &mut usize) -> std::io::Result<[u8; 16]> {
	let end = cursor.saturating_add(16);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading 16 bytes",
		)
	})?;
	*cursor = end;
	Ok(slice.try_into().expect("fixed width"))
}

fn read_array_32(bytes: &[u8], cursor: &mut usize) -> std::io::Result<[u8; 32]> {
	let end = cursor.saturating_add(32);
	let slice = bytes.get(*cursor..end).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			"persistent index row truncated while reading identity bytes",
		)
	})?;
	*cursor = end;
	Ok(slice.try_into().expect("fixed width"))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn metadata_tokens_are_grouped_and_counter_survives_reopen() {
		let (_dir, path) = temp_path("metadata-tokens");
		let index = PersistentLocalIndex::initialize_new(path.clone(), None)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(3),
			l2_uri: "tier/token".into(),
			payload_hash: [7; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		index
			.apply(PersistentIndexUpdate {
				put_tiered_slabs: vec![pointer.clone()],
				..Default::default()
			})
			.await
			.unwrap();
		let before_supersession = crate::metrics::global()
			.storage_supersession_metadata_degraded_total
			.get();
		let before_tombstone = crate::metrics::global()
			.storage_tombstone_metadata_degraded_total
			.get();
		let mut last = 0;
		for _ in 0..100 {
			last = index
				.begin_metadata_token(pointer.slab_id, MetadataKind::Supersession)
				.await
				.unwrap();
		}
		let tombstone = index
			.begin_metadata_token(pointer.slab_id, MetadataKind::Tombstone)
			.await
			.unwrap();
		assert!(tombstone > last);
		let count = index
			.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = tx.open_table(METADATA_PENDING_TABLE).map_err(redb_to_io)?;
				Ok(table.iter().map_err(redb_to_io)?.count())
			})
			.await
			.unwrap();
		assert_eq!(count, 1);
		drop(index);
		let index = PersistentLocalIndex::open(path).await.unwrap();
		assert_eq!(index.recover_metadata_tokens().await.unwrap(), 1);
		assert_eq!(
			crate::metrics::global()
				.storage_supersession_metadata_degraded_total
				.get(),
			before_supersession + 1
		);
		assert_eq!(
			crate::metrics::global()
				.storage_tombstone_metadata_degraded_total
				.get(),
			before_tombstone + 1
		);
		let recovered = index
			.load_snapshot()
			.await
			.unwrap()
			.tiered_slabs
			.into_iter()
			.find(|p| p.slab_id == pointer.slab_id)
			.unwrap();
		assert!(recovered.supersession_metadata.is_untracked());
		assert!(recovered.tombstone_metadata.is_untracked());
		assert!(
			index
				.begin_metadata_token(pointer.slab_id, MetadataKind::Supersession)
				.await
				.unwrap() > tombstone
		);
	}

	#[tokio::test]
	async fn metadata_pointer_publication_clears_only_its_own_token() {
		let (_dir, path) = temp_path("metadata-token-publication");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(4),
			l2_uri: "tier/token".into(),
			payload_hash: [7; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let first = index
			.begin_metadata_token(pointer.slab_id, MetadataKind::Supersession)
			.await
			.unwrap();
		let second = index
			.begin_metadata_token(pointer.slab_id, MetadataKind::Supersession)
			.await
			.unwrap();
		index
			.apply(PersistentIndexUpdate {
				put_tiered_slabs: vec![pointer.clone()],
				..Default::default()
			})
			.await
			.unwrap();
		assert!(
			index.load_snapshot().await.unwrap().tiered_slabs[0]
				.supersession_metadata
				.is_untracked(),
			"a publishing transaction must not re-track while tokens exist"
		);
		assert_eq!(
			index.load_pending_metadata_tokens().await.unwrap()[0]
				.1
				.supersession
				.len(),
			2
		);
		index
			.apply(PersistentIndexUpdate {
				put_tiered_slabs: vec![pointer.clone()],
				completed_metadata_token: Some((
					pointer.slab_id,
					MetadataKind::Supersession,
					first,
				)),
				..Default::default()
			})
			.await
			.unwrap();
		let saved = index.load_snapshot().await.unwrap().tiered_slabs.remove(0);
		assert!(saved.supersession_metadata.is_untracked());
		let rows = index.load_pending_metadata_tokens().await.unwrap();
		assert_eq!(rows.len(), 1);
		assert_eq!(
			rows[0].1.supersession.iter().copied().collect::<Vec<_>>(),
			vec![second]
		);
		index
			.apply(PersistentIndexUpdate {
				put_tiered_slabs: vec![saved],
				completed_metadata_token: Some((
					pointer.slab_id,
					MetadataKind::Supersession,
					second,
				)),
				..Default::default()
			})
			.await
			.unwrap();
		assert!(
			index
				.load_pending_metadata_tokens()
				.await
				.unwrap()
				.is_empty()
		);
		assert!(
			index.load_snapshot().await.unwrap().tiered_slabs[0]
				.supersession_metadata
				.is_untracked()
		);
	}

	fn generous_bounds() -> ReceiptLoadBounds {
		ReceiptLoadBounds {
			max_rows: 1 << 16,
			max_key_bytes: 1 << 16,
			max_value_bytes: 1 << 16,
			max_total_bytes: 1 << 24,
		}
	}

	/// A fresh index path inside its own scratch dir; hold the guard while the index
	/// is open or reopened.
	fn temp_path(tag: &str) -> (crate::test_support::TestDir, PathBuf) {
		let dir = crate::test_support::TestDir::new(&format!("wendelon-redb-{tag}"));
		let path = dir.join("index.redb");
		(dir, path)
	}

	fn row() -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id: TenantId::new(7),
			key_identity: Identity::blake3(b"row-key"),
			routing_locality: Locality::new(0x8877),
			table_object_id: None,
			placement: PlacementSemantics::RawLocality,
			partition: None,
			location: RecordLocation {
				slab_id: SlabId::new(9),
				offset: 128,
				length: 4096,
				record_row_index: RecordRowIndex::new(7),
				hlc: HybridTimestamp {
					physical_ms: 55,
					logical: 3,
					node_id: 11,
				},
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
		}
	}

	fn secondary_index_segment_row() -> PersistentSecondaryIndexSegmentRow {
		PersistentSecondaryIndexSegmentRow {
			key: PersistentSecondaryIndexSegmentKey {
				tenant_id: TenantId::new(7),
				table_object_id: TableObjectId::new([0x10; 16]),
				index_object_id: IndexObjectId::new([0x52; 16]),
				logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([0x51; 32]),
				source_slab_id: SlabId::new(41),
			},
			segment_path: "secondary_index/7/table/index/0000000000000029.idx".to_owned(),
			l2_uri: Some("file:///tmp/secondary-index/0000000000000029.idx".to_owned()),
			payload_hash: [0xAB; 32],
			state_digest: [0xCD; 32],
			format_version: crate::storage::SECONDARY_INDEX_SEGMENT_FORMAT_VERSION,
			entry_count: 17,
		}
	}

	fn lsm_run() -> LsmRunSummary {
		LsmRunSummary {
			slab_id: SlabId::new(51),
			compaction_level: CompactionLevelIndex::L1,
			state: SlabState::Retired {
				successors: vec![SlabId::new(71), SlabId::new(72)],
			},
			min_row_hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 1,
				node_id: 2,
			},
			max_row_hlc: HybridTimestamp {
				physical_ms: 20,
				logical: 3,
				node_id: 4,
			},
			size_bytes: ByteCount::new(8192),
			row_count: RowCount::new(7),
			sidecar_coverage: SidecarCoverage::Complete,
			manifest_epoch: ManifestEpoch::new(9),
			retention_debt_score: 1,
			tombstone_debt_score: 2,
			index_coverage_debt_score: 3,
			layout_debt_score: 4,
		}
	}

	#[test]
	fn record_row_roundtrip_preserves_every_field() {
		let row = row();
		let key = encode_record_key(row);
		let value = encode_record_value(row);
		let decoded = decode_record_row(&key, &value).unwrap();
		assert_eq!(decoded, row);
	}

	#[test]
	fn secondary_index_segment_row_roundtrip_preserves_every_field() {
		let row = secondary_index_segment_row();
		let key = encode_secondary_index_segment_key(row.key);
		let value = encode_secondary_index_segment_value(&row);
		let decoded = decode_secondary_index_segment_row(&key, &value).unwrap();
		assert_eq!(decoded, row);
	}

	#[test]
	fn lsm_run_summary_roundtrip_preserves_every_field() {
		let run = lsm_run();
		let value = encode_lsm_run_summary(&run).unwrap();
		let decoded = decode_lsm_run_summary(run.slab_id, &value).unwrap();
		assert_eq!(decoded, run);
	}

	#[test]
	fn secondary_index_segment_row_rejects_trailing_l2_without_flag() {
		let mut row = secondary_index_segment_row();
		row.l2_uri = None;
		let key = encode_secondary_index_segment_key(row.key);
		let mut value = encode_secondary_index_segment_value(&row);
		value.extend_from_slice(b"unexpected");
		let err = decode_secondary_index_segment_row(&key, &value).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(err.to_string().contains("trailing bytes"));
	}

	#[test]
	fn record_row_value_encoding_matches_row_index_golden_bytes() {
		let value = encode_record_value(row());
		assert_eq!(value.len(), RECORD_VALUE_LEN);
		assert_eq!(
			value,
			[
				0x00, 0x10, 0x00, 0x00, // length = 4096
				0x77, 0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // locality = 0x8877
				0x07, 0x00, 0x00, 0x00, // record_row_index = 7
				0x00, 0x00, 0x00, 0x00, // flags
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // table object id absent
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// ADR 0090 V5 tail: placement stamp then partition.
				0x00, // stamp = RawLocality
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // partition absent
			]
		);
	}

	#[test]
	fn record_row_value_roundtrips_tombstone_flag() {
		let mut row = row();
		row.location.is_tombstone = true;
		let value = encode_record_value(row);
		assert_eq!(
			u32::from_le_bytes(value[16..20].try_into().unwrap()) & RECORD_FLAG_IS_TOMBSTONE,
			RECORD_FLAG_IS_TOMBSTONE
		);
		assert_eq!(
			decode_record_row(&encode_record_key(row), &value).unwrap(),
			row
		);
	}

	#[test]
	fn record_row_value_roundtrips_table_object_id() {
		let mut row = row();
		row.table_object_id = Some(TableObjectId::new([0x53; 16]));
		let value = encode_record_value(row);
		assert_eq!(
			u32::from_le_bytes(value[16..20].try_into().unwrap()) & RECORD_FLAG_HAS_TABLE_OBJECT_ID,
			RECORD_FLAG_HAS_TABLE_OBJECT_ID
		);
		assert_eq!(&value[20..36], &[0x53; 16]);
		assert_eq!(
			decode_record_row(&encode_record_key(row), &value).unwrap(),
			row
		);
	}

	#[test]
	fn legacy_v3_record_row_decodes_without_table_attribution() {
		let row = row();
		let key = encode_record_key(row);
		let v4 = encode_record_value(row);
		let decoded = decode_record_row(&key, &v4[..RECORD_VALUE_LEN_V3]).unwrap();
		assert_eq!(decoded.table_object_id, None);
		assert_eq!(decoded.location, row.location);
	}

	#[test]
	fn tiered_pointer_roundtrip_preserves_every_field() {
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(3),
			l2_uri: "file:///tmp/example".into(),
			payload_hash: [0xAB; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(9)),
			tenant_kek_id: Some(TenantKekId::try_from(11).unwrap()),
			stats_format_version: Some(1),
			stats_block_len: Some(128),
			stats_block_hash128: Some([0x5A; 16]),
			tombstone_metadata: TombstoneMetadata::Earliest(HybridTimestamp {
				physical_ms: 99,
				logical: 1,
				node_id: 2,
			}),
			supersession_metadata: SupersessionMetadata::Earliest(HybridTimestamp {
				physical_ms: 199,
				logical: 2,
				node_id: 3,
			}),
		};
		let encoded = encode_tiered_pointer(&pointer);
		let decoded = decode_tiered_pointer(pointer.slab_id, &encoded).unwrap();
		assert_eq!(decoded, pointer);

		for tombstone_metadata in [
			TombstoneMetadata::Untracked,
			TombstoneMetadata::None,
			TombstoneMetadata::Earliest(HybridTimestamp {
				physical_ms: 100,
				logical: 2,
				node_id: 3,
			}),
		] {
			let pointer = TieredSlabPointer {
				tombstone_metadata,
				..pointer.clone()
			};
			let decoded =
				decode_tiered_pointer(pointer.slab_id, &encode_tiered_pointer(&pointer)).unwrap();
			assert_eq!(decoded.tombstone_metadata, tombstone_metadata);
		}
		for supersession_metadata in [
			SupersessionMetadata::Untracked,
			SupersessionMetadata::None,
			SupersessionMetadata::Earliest(HybridTimestamp {
				physical_ms: 200,
				logical: 3,
				node_id: 4,
			}),
		] {
			let pointer = TieredSlabPointer {
				supersession_metadata,
				..pointer.clone()
			};
			let decoded =
				decode_tiered_pointer(pointer.slab_id, &encode_tiered_pointer(&pointer)).unwrap();
			assert_eq!(decoded.supersession_metadata, supersession_metadata);
		}
	}

	#[test]
	fn tiered_pointer_v1_decode_defaults_to_plaintext() {
		let mut v1 = Vec::new();
		v1.extend_from_slice(&[0xCD; 32]);
		v1.extend_from_slice(b"file:///tmp/legacy");
		let decoded = decode_tiered_pointer_v1(SlabId::new(7), &v1).unwrap();
		assert!(!decoded.is_encrypted);
		assert_eq!(decoded.payload_hash, [0xCD; 32]);
		assert_eq!(decoded.l2_uri, "file:///tmp/legacy");
		assert_eq!(decoded.stats_format_version, None);
		assert_eq!(decoded.stats_block_len, None);
		assert_eq!(decoded.stats_block_hash128, None);
		assert_eq!(decoded.tombstone_metadata, TombstoneMetadata::Untracked);
		assert_eq!(
			decoded.supersession_metadata,
			SupersessionMetadata::Untracked
		);
	}

	#[tokio::test]
	async fn secondary_index_segments_persist_across_snapshot_and_delete() {
		let (_dir, path) = temp_path("secondary-index-segment");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let row = secondary_index_segment_row();
		index
			.upsert_secondary_index_segment(row.clone())
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.secondary_index_segments, vec![row.clone()]);

		index.delete_secondary_index_segment(row.key).await.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert!(snapshot.secondary_index_segments.is_empty());
	}

	#[tokio::test]
	async fn lsm_install_intent_survives_reopen_and_requires_exact_clear() {
		let (_dir, path) = temp_path("lsm-install-intent");
		let index = PersistentLocalIndex::initialize_new(path.clone(), None)
			.await
			.unwrap();
		let commit = crate::storage::lsm::CompactionCommit::gc_remove(
			crate::consensus::OwnerRef::for_test(1, 1),
			crate::schema::CompactionJobId::new(5),
			ManifestEpoch::new(3),
			vec![SlabId::new(51)],
		);
		let commit_digest = *blake3::hash(&serde_json::to_vec(&commit).unwrap()).as_bytes();
		let intent = LsmInstallIntent {
			kind: LsmInstallKind::Control,
			phase: LsmInstallPhase::Prepared,
			local_input_rows_digest: [0; 32],
			local_output_rows_digest: [0; 32],
			local_input_runs: Vec::new(),
			local_input_files: Vec::new(),
			local_output_files: Vec::new(),
			commit,
			commit_digest,
			ops: Vec::new(),
			inputs: vec![LsmInputAuthority {
				slab_id: SlabId::new(51),
				revision: 7,
				opened: 0,
				tiered: None,
			}],
		};
		index.put_lsm_install_intent(intent.clone()).await.unwrap();
		index.put_lsm_install_intent(intent.clone()).await.unwrap();
		index.rebuild(Vec::new(), Vec::new(), None).await.unwrap();
		drop(index);
		let index = PersistentLocalIndex::open(path).await.unwrap();
		assert_eq!(
			index.load_snapshot().await.unwrap().lsm_install_intents,
			vec![intent.clone()]
		);
		let mut stale = intent.clone();
		stale.inputs[0].revision += 1;
		assert!(index.clear_lsm_install_intent(stale).await.is_err());
		assert_eq!(
			index.load_snapshot().await.unwrap().lsm_install_intents,
			vec![intent.clone()]
		);
		index.clear_lsm_install_intent(intent).await.unwrap();
		assert!(
			index
				.load_snapshot()
				.await
				.unwrap()
				.lsm_install_intents
				.is_empty()
		);
	}

	#[tokio::test]
	async fn lost_lsm_install_intent_table_is_not_treated_as_empty() {
		let (_dir, path) = temp_path("lsm-install-intent-lost");
		let index = PersistentLocalIndex::initialize_new(path.clone(), None)
			.await
			.unwrap();
		drop(index);
		{
			let db = Database::create(&path).unwrap();
			let tx = db.begin_write().unwrap();
			tx.delete_table(LSM_INSTALL_INTENTS_TABLE).unwrap();
			tx.commit().unwrap();
		}
		assert!(PersistentLocalIndex::open(path).await.is_err());
	}

	#[tokio::test]
	async fn cancelled_guarded_delta_keeps_gate_until_blocking_commit_finishes() {
		let (_dir, path) = temp_path("guarded-redb-delta");
		let index = std::sync::Arc::new(
			PersistentLocalIndex::initialize_new(path, None)
				.await
				.unwrap(),
		);
		let gate = std::sync::Arc::new(tokio::sync::RwLock::new(()));
		let read = std::sync::Arc::new(gate.clone().read_owned().await);
		let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
		let release = std::sync::Arc::new(std::sync::Barrier::new(2));
		*index.guarded_delta_commit_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		let expected = row();
		let task = {
			let index = index.clone();
			let expected = expected.clone();
			tokio::spawn(async move {
				index
					.apply_with_publication_guard(
						PersistentIndexUpdate {
							put_records: vec![expected],
							..Default::default()
						},
						Some(read),
					)
					.await
			})
		};
		tokio::task::spawn_blocking(move || entered.wait())
			.await
			.unwrap();
		task.abort();
		assert!(task.await.unwrap_err().is_cancelled());
		let writer = gate.clone();
		let mut exclusive = tokio::spawn(async move { writer.write_owned().await });
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(200), &mut exclusive)
				.await
				.is_err()
		);
		tokio::task::spawn_blocking(move || release.wait())
			.await
			.unwrap();
		drop(exclusive.await.unwrap());
		assert!(
			index
				.load_snapshot()
				.await
				.unwrap()
				.record_rows
				.contains(&expected)
		);
	}

	#[tokio::test]
	async fn lsm_runs_persist_across_snapshot_and_delete() {
		let (_dir, path) = temp_path("lsm-runs");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let run = lsm_run();
		index
			.apply(PersistentIndexUpdate {
				put_lsm_runs: vec![run.clone()],
				..Default::default()
			})
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.lsm_runs, vec![run.clone()]);

		index
			.apply(PersistentIndexUpdate {
				delete_lsm_run_ids: vec![run.slab_id],
				..Default::default()
			})
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert!(snapshot.lsm_runs.is_empty());
	}

	#[tokio::test]
	async fn rebuild_preserves_existing_secondary_index_segments() {
		let (_dir, path) = temp_path("secondary-index-preserve");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let segment = secondary_index_segment_row();
		index
			.rebuild_with_secondary_index_segments(
				Vec::new(),
				Vec::new(),
				vec![segment.clone()],
				None,
			)
			.await
			.unwrap();
		index
			.rebuild(vec![row()], Vec::new(), Some(33))
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.secondary_index_segments, vec![segment]);
		assert_eq!(snapshot.last_control_log_index, Some(33));
	}

	#[tokio::test]
	async fn snapshot_only_surfaces_tiered_records() {
		let (_dir, path) = temp_path("snapshot");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let tiered_pointer = TieredSlabPointer {
			slab_id: SlabId::new(41),
			l2_uri: "file:///tmp/tiered".into(),
			payload_hash: [0x11; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(5)),
			tenant_kek_id: Some(TenantKekId::try_from(15).unwrap()),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let local_row = PersistentRecordRow {
			location: RecordLocation {
				slab_id: SlabId::new(7),
				..row().location
			},
			..row()
		};
		let tiered_row = PersistentRecordRow {
			location: RecordLocation {
				slab_id: tiered_pointer.slab_id,
				..row().location
			},
			..row()
		};
		index
			.rebuild(
				vec![local_row, tiered_row],
				vec![tiered_pointer.clone()],
				None,
			)
			.await
			.unwrap();

		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.tiered_slabs, vec![tiered_pointer]);
		assert_eq!(snapshot.tiered_records, vec![tiered_row]);
		assert_eq!(
			snapshot.record_localities,
			vec![(row().tenant_id, row().key_identity, row().routing_locality)]
		);
	}

	#[tokio::test]
	async fn tiered_update_stamps_monotonic_control_log_index() {
		let (_dir, path) = temp_path("watermark");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(9),
			l2_uri: "file:///tmp/tiered-watermark".into(),
			payload_hash: [0x22; 32],
			is_encrypted: true,
			tenant_id: Some(TenantId::new(6)),
			tenant_kek_id: Some(TenantKekId::try_from(16).unwrap()),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};

		index
			.upsert_tiered_slab(pointer.clone(), Some(12))
			.await
			.unwrap();
		let first = index.load_snapshot().await.unwrap();
		assert_eq!(first.tiered_slabs, vec![pointer.clone()]);
		assert_eq!(first.last_control_log_index, Some(12));

		index
			.upsert_tiered_slab(pointer.clone(), Some(7))
			.await
			.unwrap();
		let second = index.load_snapshot().await.unwrap();
		assert_eq!(second.last_control_log_index, Some(12));

		index.upsert_tiered_slab(pointer, Some(19)).await.unwrap();
		let third = index.load_snapshot().await.unwrap();
		assert_eq!(third.last_control_log_index, Some(19));
	}

	/// ADR 0097 decision 4: a store that HAS held receipts and no longer has the
	/// table must report its history as unavailable - while the rest of the index
	/// keeps serving.
	///
	/// This is the distinction the marker exists for. Creating the table whenever
	/// it is missing would present lost history as an empty one, which silently
	/// resets an attempt budget on exactly the restart where the budget matters.
	/// A rebuild replaces the index tables and leaves the receipt table alone: the
	/// attempt budget is not derivable from slab bytes, so a rebuild must not
	/// reset it.
	#[tokio::test]
	async fn a_rebuild_leaves_maintenance_receipts_untouched() {
		let (_dir, path) = temp_path("adr0097-receipt-rebuild");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		index
			.put_maintenance_receipt(vec![0, 11, 0, 0, 0, 0, 0, 0, 0, 5], vec![1, 2, 3])
			.await
			.unwrap();
		index
			.rebuild_with_secondary_index_segments_and_lsm_runs(
				vec![row()],
				Vec::new(),
				Vec::new(),
				Vec::new(),
				None,
			)
			.await
			.unwrap();
		assert!(index.receipts_available());
		assert_eq!(
			index
				.load_maintenance_receipts(generous_bounds())
				.await
				.unwrap(),
			vec![(vec![0, 11, 0, 0, 0, 0, 0, 0, 0, 5], vec![1, 2, 3])]
		);
	}

	/// Every bound refuses BEFORE copying the offending row, and the error is data
	/// corruption rather than an I/O failure, so recovery treats it as history it
	/// cannot trust.
	#[tokio::test]
	async fn receipt_load_refuses_rows_over_its_bounds() {
		let (_dir, path) = temp_path("adr0097-receipt-bounds");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		for slab in 1..=3u8 {
			index
				.put_maintenance_receipt(vec![0, 11, 0, 0, 0, 0, 0, 0, 0, slab], vec![1; 8])
				.await
				.unwrap();
		}
		let within = generous_bounds();
		assert_eq!(
			index.load_maintenance_receipts(within).await.unwrap().len(),
			3
		);

		let cases = [
			(
				"rows",
				ReceiptLoadBounds {
					max_rows: 2,
					..within
				},
			),
			(
				"key bytes",
				ReceiptLoadBounds {
					max_key_bytes: 9,
					..within
				},
			),
			(
				"value bytes",
				ReceiptLoadBounds {
					max_value_bytes: 7,
					..within
				},
			),
			(
				"total bytes",
				ReceiptLoadBounds {
					max_total_bytes: 3 * 18 - 1,
					..within
				},
			),
		];
		for (what, bounds) in cases {
			let error = index
				.load_maintenance_receipts(bounds)
				.await
				.expect_err(what);
			assert_eq!(error.kind(), ErrorKind::InvalidData, "{what}");
			assert!(
				error.to_string().contains("receipt load bound exceeded"),
				"{what}: {error}"
			);
		}
		// Exactly at the total is admitted: the bound is inclusive.
		let exact = ReceiptLoadBounds {
			max_total_bytes: 3 * 18,
			..within
		};
		assert_eq!(
			index.load_maintenance_receipts(exact).await.unwrap().len(),
			3
		);
	}

	#[tokio::test]
	async fn a_deleted_receipt_table_disables_receipts_but_not_the_index() {
		let (_dir, path) = temp_path("adr0097-receipt-marker");

		// A store with ordinary index history AND a receipt.
		let index = PersistentLocalIndex::initialize_new(path.clone(), None)
			.await
			.unwrap();
		assert!(
			index.receipts_available(),
			"a fresh store initializes its receipt table"
		);
		index.upsert_record(row()).await.unwrap();
		index
			.put_maintenance_receipt(vec![0, 11, 0, 0, 0, 0, 0, 0, 0, 5], vec![1, 2, 3])
			.await
			.unwrap();
		assert_eq!(
			index
				.load_maintenance_receipts(generous_bounds())
				.await
				.unwrap()
				.len(),
			1
		);
		drop(index);

		// Lose ONLY the receipt table, leaving every other table intact.
		delete_maintenance_receipt_table_for_test(&path);

		let reopened = PersistentLocalIndex::open(path.clone()).await.unwrap();
		assert!(
			!reopened.receipts_available(),
			"the marker survives, so a missing table is LOST history, not empty history"
		);
		// Serving is unaffected: the ordinary snapshot still reads back.
		let snapshot = reopened.load_snapshot().await.unwrap();
		assert_eq!(
			snapshot.record_rows.len(),
			1,
			"index recovery is preserved while maintenance is disabled"
		);

		let _ = std::fs::remove_file(&path);
	}

	/// The complementary case: a store that has never held receipts initializes
	/// them, which must NOT be mistaken for lost history.
	#[tokio::test]
	async fn a_fresh_store_initializes_receipts_and_reopens_available() {
		let (_dir, path) = temp_path("adr0097-receipt-fresh");
		let index = PersistentLocalIndex::initialize_new(path.clone(), None)
			.await
			.unwrap();
		assert!(index.receipts_available());
		assert!(
			index
				.load_maintenance_receipts(generous_bounds())
				.await
				.unwrap()
				.is_empty()
		);
		drop(index);

		let reopened = PersistentLocalIndex::open(path.clone()).await.unwrap();
		assert!(
			reopened.receipts_available(),
			"an empty-but-present table stays available across reopen"
		);
		assert!(
			reopened
				.load_maintenance_receipts(generous_bounds())
				.await
				.unwrap()
				.is_empty()
		);

		let _ = std::fs::remove_file(&path);
	}
	#[tokio::test]
	async fn lsm_output_and_intent_phase_commit_atomically_and_clear_exactly() {
		let (_dir, path) = temp_path("lsm-phase-atomic");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let commit = crate::storage::lsm::CompactionCommit::gc_remove(
			crate::consensus::OwnerRef::for_test(1, 1),
			crate::schema::CompactionJobId::new(55),
			ManifestEpoch::new(3),
			vec![SlabId::new(51)],
		);
		let intent = LsmInstallIntent {
			kind: LsmInstallKind::Local,
			phase: LsmInstallPhase::Prepared,
			commit_digest: *blake3::hash(&serde_json::to_vec(&commit).unwrap()).as_bytes(),
			ops: Vec::new(),
			commit,
			inputs: Vec::new(),
			local_input_rows_digest: [0; 32],
			local_output_rows_digest: [0; 32],
			local_input_runs: Vec::new(),
			local_input_files: Vec::new(),
			local_output_files: Vec::new(),
		};
		index.put_lsm_install_intent(intent.clone()).await.unwrap();
		let mut published = intent.clone();
		published.phase = LsmInstallPhase::OutputsPublished;
		let mut wrong = intent.clone();
		wrong.local_input_rows_digest[0] = 1;
		let mut wrong_published = wrong.clone();
		wrong_published.phase = LsmInstallPhase::OutputsPublished;
		assert!(
			index
				.apply(PersistentIndexUpdate {
					lsm_intent_transition: Some((wrong, wrong_published)),
					put_records: vec![row()],
					..Default::default()
				})
				.await
				.is_err()
		);
		let snapshot = index.load_snapshot().await.unwrap();
		assert!(snapshot.record_rows.is_empty());
		assert_eq!(snapshot.lsm_install_intents, vec![intent.clone()]);
		index
			.apply(PersistentIndexUpdate {
				lsm_intent_transition: Some((intent.clone(), published.clone())),
				put_records: vec![row()],
				..Default::default()
			})
			.await
			.unwrap();
		let snapshot = index.load_snapshot().await.unwrap();
		assert_eq!(snapshot.record_rows, vec![row()]);
		assert_eq!(snapshot.lsm_install_intents, vec![published.clone()]);
		assert!(index.clear_lsm_install_intent(intent).await.is_err());
		index.clear_lsm_install_intent(published).await.unwrap();
	}
}

/// ADR 0090: the length-dispatched compatibility ladder. A value shorter than V5
/// predates partitions and decodes as legacy; a V5-length value carries an
/// explicit stamp, and an unrecognized stamp is malformed. "Written before
/// partitions" and "corrupt" must never resolve to the same outcome.
#[cfg(test)]
mod adr0090_record_value_versions {
	use super::*;

	fn row() -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id: TenantId::new(7),
			key_identity: Identity::new([3; 32]),
			routing_locality: Locality::new(0x00AB_0000_0000_1234),
			table_object_id: Some(TableObjectId::new([9; 16])),
			placement: PlacementSemantics::RawLocality,
			partition: None,
			location: RecordLocation {
				slab_id: SlabId::new(11),
				offset: 4096,
				length: 512,
				hlc: HybridTimestamp {
					physical_ms: 100,
					logical: 1,
					node_id: 2,
				},
				record_row_index: RecordRowIndex::new(3),
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
		}
	}

	#[test]
	fn adr0090_v5_round_trips_both_placements() {
		for (placement, partition) in [
			(PlacementSemantics::RawLocality, None),
			(
				PlacementSemantics::Partitioned,
				Some(PartitionId::new(0x00AB_0000_0000_0000)),
			),
			// Partition zero is a legitimate partition, not an absent one.
			(PlacementSemantics::Partitioned, Some(PartitionId::new(0))),
		] {
			let mut original = row();
			original.placement = placement;
			original.partition = partition;
			let key = encode_record_key(original);
			let value = encode_record_value(original);
			assert_eq!(value.len(), RECORD_VALUE_LEN_V5);
			let decoded = decode_record_row(&key, &value).expect("V5 decodes");
			assert_eq!(decoded.placement, placement);
			assert_eq!(decoded.partition, partition);
			assert_eq!(decoded.routing_locality, original.routing_locality);
			assert_eq!(decoded.table_object_id, original.table_object_id);
		}
	}

	#[test]
	fn adr0090_values_written_before_v5_decode_as_legacy() {
		let original = row();
		let key = encode_record_key(original);
		let full = encode_record_value(original);
		// Genuine earlier artifacts: the same value truncated to each prior
		// version's length, which is exactly what an older build wrote.
		for (len, label) in [
			(RECORD_VALUE_LEN_V4, "V4"),
			(RECORD_VALUE_LEN_V3, "V3"),
			(RECORD_VALUE_LEN_V2, "V2"),
			(RECORD_VALUE_LEN_V1, "V1"),
		] {
			let mut truncated = full[..len].to_vec();
			if len < RECORD_VALUE_LEN_V4 && len >= RECORD_VALUE_LEN_V3 {
				// A pre-V4 writer never set the table-object flag.
				let mut flags = u32::from_le_bytes(truncated[16..20].try_into().unwrap());
				flags &= !RECORD_FLAG_HAS_TABLE_OBJECT_ID;
				truncated[16..20].copy_from_slice(&flags.to_le_bytes());
			}
			let decoded = decode_record_row(&key, &truncated)
				.unwrap_or_else(|err| panic!("{label} value must still decode: {err}"));
			assert_eq!(
				decoded.placement,
				PlacementSemantics::RawLocality,
				"{label} predates partitions and is legacy"
			);
			assert_eq!(decoded.partition, None, "{label} carries no partition");
		}
	}

	/// The discriminating control: a V5-length value whose stamp is unrecognized
	/// is malformed. An implementation that fell back to legacy on an unknown
	/// stamp would pass every other test here and silently misroute a record
	/// whose placement it did not understand.
	#[test]
	fn adr0090_a_v5_value_with_an_unknown_stamp_is_malformed_not_legacy() {
		let original = row();
		let key = encode_record_key(original);
		let mut value = encode_record_value(original).to_vec();
		assert_eq!(value.len(), RECORD_VALUE_LEN_V5);
		for stamp in [2u8, 7, 200, u8::MAX] {
			value[36] = stamp;
			let err =
				decode_record_row(&key, &value).expect_err("an unknown V5 stamp must be refused");
			assert!(
				err.to_string().contains("unknown placement stamp"),
				"refusal must name the cause, got: {err}"
			);
		}
	}

	#[test]
	fn adr0090_an_unrecognized_length_is_still_refused() {
		let original = row();
		let key = encode_record_key(original);
		let full = encode_record_value(original);
		for len in [0usize, 5, 21, 37, 44] {
			assert!(
				decode_record_row(&key, &full[..len.min(full.len())]).is_err(),
				"length {len} is not a known version and must be refused"
			);
		}
	}
	#[tokio::test]
	async fn snapshot_retirement_marks_survive_reopen_and_conflicts_roll_back() {
		let dir = crate::test_support::TestDir::new("snapshot-retirement-marks");
		let path = dir.join("index.redb");
		let index = PersistentLocalIndex::initialize_new(path.clone(), None)
			.await
			.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: SlabId::new(3),
			l2_uri: "tier/old".into(),
			payload_hash: [7; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		index
			.apply(PersistentIndexUpdate {
				put_tiered_slabs: vec![pointer.clone()],
				..Default::default()
			})
			.await
			.unwrap();
		let epoch = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(7));
		let gate = std::sync::Arc::new(std::sync::RwLock::new(()));
		let mark = SnapshotRetirementMark {
			epoch: 7,
			snapshot_index: Some(50),
			pointer: pointer.clone(),
		};
		index
			.stage_snapshot_retirement_marks(
				vec![mark.clone()],
				HashSet::new(),
				7,
				epoch.clone(),
				gate.clone(),
			)
			.await
			.unwrap();
		drop(index);
		let index = PersistentLocalIndex::open(path).await.unwrap();
		assert_eq!(
			index.load_snapshot_retirement_marks().await.unwrap(),
			vec![mark.clone()]
		);
		let mut stale = mark.clone();
		stale.pointer.l2_uri = "tier/not-current".into();
		assert!(
			index
				.stage_snapshot_retirement_marks(
					vec![stale],
					HashSet::new(),
					7,
					epoch.clone(),
					gate.clone()
				)
				.await
				.is_err()
		);
		assert_eq!(
			index.load_snapshot_retirement_marks().await.unwrap(),
			vec![mark.clone()]
		);
		assert_eq!(index.snapshot_retirement_epoch_floor().await.unwrap(), 7);
		epoch.store(8, std::sync::atomic::Ordering::Release);
		let error = index
			.stage_snapshot_retirement_marks(
				vec![mark.clone()],
				HashSet::new(),
				7,
				epoch.clone(),
				gate.clone(),
			)
			.await
			.unwrap_err();
		assert_eq!(error.kind(), ErrorKind::Interrupted);
		assert_eq!(
			index.load_snapshot_retirement_marks().await.unwrap(),
			vec![mark]
		);
		index
			.stage_snapshot_retirement_marks(
				vec![],
				HashSet::from([pointer.slab_id]),
				8,
				epoch.clone(),
				gate.clone(),
			)
			.await
			.unwrap();
		assert!(
			index
				.load_snapshot_retirement_marks()
				.await
				.unwrap()
				.is_empty()
		);
		assert_eq!(index.snapshot_retirement_epoch_floor().await.unwrap(), 8);
	}
	#[tokio::test]
	async fn b2_aborted_candidate_keeps_all_prior_publication_history() {
		let _dir = crate::test_support::TestDir::new("b2-history");
		let path = _dir.join("index.redb");
		let index = PersistentLocalIndex::initialize_new(path, None)
			.await
			.unwrap();
		let slab = SlabId::new(7);
		let old = RetainedObjectIntent {
			bytes: Some(100),
			replacement_uri: Some("new".into()),
			replacement_bytes: Some(200),
			published_replacements: std::collections::BTreeSet::from(["prior".into()]),
			retirement_source: Some(RetirementSource::Log(11)),
			state: RetainedObjectIntentState::Unresolved,
		};
		let candidate = RetainedObjectIntent {
			bytes: Some(200),
			replacement_uri: Some("new".into()),
			replacement_bytes: None,
			published_replacements: Default::default(),
			retirement_source: None,
			state: RetainedObjectIntentState::CandidateNotSubmitted,
		};
		index
			.put_cleanup_intent_pair(slab, "old".into(), old, Some(("new".into(), candidate)))
			.await
			.unwrap();
		index
			.clear_unsubmitted_tier_candidate(slab, "new".into(), vec!["old".into()])
			.await
			.unwrap();
		let records = index.load_snapshot_cleanup_intents().await.unwrap();
		assert_eq!(records.len(), 1);
		let (saved_slab, uri, saved) = &records[0];
		assert_eq!(*saved_slab, slab);
		assert_eq!(uri, "old");
		assert!(saved.was_published("prior"));
		assert!(
			!saved.was_published("new"),
			"aborted replacement is not publication evidence"
		);
		assert_eq!(saved.retirement_source, Some(RetirementSource::Log(11)));
		assert_eq!(saved.bytes, Some(100));
		assert_eq!(saved.replacement_uri, None);
		assert_eq!(saved.replacement_bytes, None);
	}
}
