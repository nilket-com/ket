//! ADR 0699 S5a: a store's certified base inventory.
//!
//! `base_active_v1` is the positive inventory of the current generation: one
//! entry per key, the key's certified winning version (a winning tombstone
//! included). `base_staged_v1` is the candidate for exactly one pending
//! target, durable before the store reports it. Activation, after the
//! generation commits, is one transaction: the staged candidate, checked
//! against the committed certificate, becomes active, and the intents and
//! owner entries of the erased parents are retired with it. An unowned
//! version is never a member; only an entry here is.
//!
//! Fencing: a candidate is staged only on the active predecessor it was built
//! from, and never replaces a candidate for a later target. A candidate stays
//! until a later target supersedes it or it is activated.

#![cfg_attr(not(test), allow(dead_code))]

use super::*;
use crate::consensus::decisions::generations::{ChunkSum, chunk_sums_of};
use serde::{Deserialize, Serialize};

pub(super) const BASE_ACTIVE_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("base_active_v1");
pub(super) const BASE_STAGED_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("base_staged_v1");
pub(super) const BASE_META_TABLE: TableDefinition<&str, &[u8]> =
	TableDefinition::new("base_meta_v1");
/// ADR 0106 (wendelon): a replacement's recovered base pages, staged before
/// one publication. Separate from the generation's own staging.
pub(super) const BASE_RECOVERY_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("base_recovery_v1");
/// ADR 0699 S5b: positive evidence that a version is obsolete, written only by
/// the activation that made it so.
pub(super) const CLEANUP_DEBT_TABLE: TableDefinition<&[u8], &[u8]> =
	TableDefinition::new("cleanup_debt_v1");

/// One version's identity: tenant, table, key and full HLC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionKey {
	pub tenant: TenantId,
	pub table: TableObjectId,
	pub key_identity: [u8; 32],
	pub hlc: HybridTimestamp,
}

/// A stored version as the index knows it: tenant, key identity and full
/// HLC. A row identity is derived from its table, so this names one version;
/// the table comes from positive evidence (the debt record).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionRef {
	pub tenant: TenantId,
	pub key_identity: [u8; 32],
	pub hlc: HybridTimestamp,
}

impl VersionRef {
	/// The debt-table key prefix of every table's debt for this version.
	fn debt_prefix(&self) -> [u8; 56] {
		let mut prefix = [0u8; 56];
		prefix[..8].copy_from_slice(&self.tenant.get().to_le_bytes());
		prefix[8..40].copy_from_slice(&self.key_identity);
		prefix[40..48].copy_from_slice(&self.hlc.physical_ms.to_le_bytes());
		prefix[48..52].copy_from_slice(&self.hlc.logical.to_le_bytes());
		prefix[52..56].copy_from_slice(&self.hlc.node_id.to_le_bytes());
		prefix
	}
}

impl VersionKey {
	pub fn version_ref(&self) -> VersionRef {
		VersionRef {
			tenant: self.tenant,
			key_identity: self.key_identity,
			hlc: self.hlc,
		}
	}

	/// The owner-table key of this version.
	fn owner_bytes(&self) -> [u8; 72] {
		super::parent_intents::owner_key_for_base(self.tenant, &self.as_owned([0; 32]))
	}

	/// The debt-table key: tenant, key, HLC, then table, so one version's debt
	/// is found by prefix.
	fn debt_bytes(&self) -> [u8; 72] {
		let mut key = [0u8; 72];
		key[..56].copy_from_slice(&self.version_ref().debt_prefix());
		key[56..].copy_from_slice(self.table.as_bytes());
		key
	}

	fn from_debt_bytes(bytes: &[u8]) -> std::io::Result<Self> {
		let bytes: &[u8; 72] = bytes
			.try_into()
			.map_err(|_| Error::new(ErrorKind::InvalidData, "a debt key is not 72 bytes"))?;
		fn take<const N: usize>(bytes: &[u8; 72], at: usize) -> [u8; N] {
			bytes[at..at + N].try_into().expect("in range")
		}
		Ok(Self {
			tenant: TenantId::new(u64::from_le_bytes(take(bytes, 0))),
			key_identity: take(bytes, 8),
			hlc: HybridTimestamp {
				physical_ms: u64::from_le_bytes(take(bytes, 40)),
				logical: u32::from_le_bytes(take(bytes, 48)),
				node_id: u32::from_le_bytes(take(bytes, 52)),
			},
			table: TableObjectId::new(take(bytes, 56)),
		})
	}

	pub fn as_owned(&self, row_digest: [u8; 32]) -> super::parent_intents::OwnedVersion {
		super::parent_intents::OwnedVersion {
			table: self.table,
			key_identity: self.key_identity,
			hlc: self.hlc,
			row_digest,
		}
	}
}

/// Why a version is obsolete, with the evidence that identifies its content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebtRecord {
	/// The generation whose activation recorded it.
	pub gid: u64,
	/// The retired content's digest, from the manifest or the base entry.
	pub row_digest: [u8; 32],
	pub source: DebtSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DebtSource {
	/// A version of an erased parent that is not its key's new base entry; the
	/// intent `(rsv, ordinal)` owned it when activation retired it. Its kind
	/// follows from the version and its table's schema.
	RetiredIntent { rsv: u64, ordinal: u16 },
	/// A previous base entry displaced by another version of its key.
	DisplacedBase { tombstone: bool },
}

/// What the survivor rule reads for one stored version: its debt (with the
/// table the debt names), that table's active base entry for the key, and
/// whether an owner entry names it. Debt under more than one table is
/// ambiguous and protects the version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionFacts {
	pub debt: Option<(VersionKey, DebtRecord)>,
	pub ambiguous: bool,
	pub base: Option<BaseEntry>,
	pub owned: bool,
}

/// Facts read for exactly `candidates` in ONE index read transaction. Only
/// [`PersistentLocalIndex::version_facts`] constructs it, so a decision can
/// never pair facts from two reads, or facts read for other versions.
#[derive(Clone, Debug)]
pub struct CoherentFacts {
	active_gid: u64,
	candidates: Vec<VersionRef>,
	facts: Vec<VersionFacts>,
}

#[cfg(test)]
impl CoherentFacts {
	/// Test-only: facts as one read would return them, for a pure decision.
	pub(crate) fn for_test(
		active_gid: u64,
		candidates: Vec<VersionRef>,
		facts: Vec<VersionFacts>,
	) -> Self {
		Self {
			active_gid,
			candidates,
			facts,
		}
	}
}

impl CoherentFacts {
	/// The active generation the transaction saw.
	pub fn active_gid(&self) -> u64 {
		self.active_gid
	}

	/// The versions these facts were read for, in order.
	pub fn candidates(&self) -> &[VersionRef] {
		&self.candidates
	}

	/// The facts of `candidates()[at]`.
	pub fn get(&self, at: usize) -> &VersionFacts {
		&self.facts[at]
	}
}
const BASE_META_KEY: &str = "meta";
/// DC-269: the installed certificate's cut ([`InstalledCut`]).
const BASE_CUT_KEY: &str = "installed_cut";
/// ADR 0106 (wendelon): the certificate a recovery is installing.
const BASE_RECOVERY_KEY: &str = "recovery";
/// ADR 0106 (wendelon): the exact recovery statement, saved before first sent.
const RECOVERY_SUBMISSION_KEY: &str = "recovery_submission";

/// ADR 0106 (wendelon): a replacement's recovery statement as it will be (or
/// was) submitted: saved once, before the first send, and never re-sampled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySubmission {
	pub token: u128,
	pub r_new: u32,
	pub birth: [u8; 16],
	pub store: crate::consensus::StoreIncarnation,
	pub gid: u64,
	pub target_digest: Option<[u8; 32]>,
	pub k: u64,
}

/// ADR 0106 (wendelon): why a recovered base page or publication was refused.
/// Each refusal changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstallRefusal {
	/// The store already holds a base, a generation's staging, or active rows:
	/// only a pristine promoted store installs a recovered base.
	NotPristine,
	/// A recovery for another certificate is staged here.
	OtherCertificate,
	/// A page names a key already staged with different content.
	ConflictingEntry,
	/// The staged entries do not hash to the certificate's chunks.
	SumsMismatch,
}

/// A key's certified version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseEntry {
	pub tenant: TenantId,
	pub table: TableObjectId,
	pub key_identity: [u8; 32],
	pub hlc: HybridTimestamp,
	pub row_digest: [u8; 32],
	pub tombstone: bool,
	/// The version's ring position; its high byte is its chunk.
	pub position: u64,
}

impl BaseEntry {
	pub(crate) fn key(&self) -> [u8; 56] {
		base_key(self.tenant, self.table, &self.key_identity)
	}

	pub fn chunk(&self) -> usize {
		(self.position >> 56) as usize
	}

	/// The survivor's digest preimage: fixed-width fields, every one bound.
	pub fn preimage(&self) -> Vec<u8> {
		let mut preimage = Vec::with_capacity(8 + 16 + 32 + 16 + 32 + 1);
		preimage.extend_from_slice(&self.tenant.get().to_le_bytes());
		preimage.extend_from_slice(self.table.as_bytes());
		preimage.extend_from_slice(&self.key_identity);
		preimage.extend_from_slice(&self.hlc.physical_ms.to_le_bytes());
		preimage.extend_from_slice(&self.hlc.logical.to_le_bytes());
		preimage.extend_from_slice(&self.hlc.node_id.to_le_bytes());
		preimage.extend_from_slice(&self.row_digest);
		preimage.push(u8::from(self.tombstone));
		preimage
	}
}

fn base_key(tenant: TenantId, table: TableObjectId, key_identity: &[u8; 32]) -> [u8; 56] {
	let mut key = [0u8; 56];
	key[..8].copy_from_slice(&tenant.get().to_be_bytes());
	key[8..24].copy_from_slice(table.as_bytes());
	key[24..].copy_from_slice(key_identity);
	key
}

/// Every chunk's count and root over `entries`, empty chunks included.
pub fn chunk_sums<'a>(entries: impl IntoIterator<Item = &'a BaseEntry>) -> Vec<ChunkSum> {
	chunk_sums_of(
		entries
			.into_iter()
			.map(|entry| (entry.chunk(), entry.preimage())),
	)
}

/// The staged candidate for one target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedCandidate {
	pub gid: u64,
	pub target_digest: [u8; 32],
	pub predecessor_gid: u64,
	pub chunks: Vec<ChunkSum>,
}

/// The base's durable metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseMeta {
	pub active_gid: u64,
	pub active_chunks: Vec<ChunkSum>,
	pub staged: Option<StagedCandidate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageRefusal {
	/// The active base is not the predecessor the candidate was built from.
	PredecessorMoved,
	/// A candidate for a later target is staged: this worker is stale.
	Superseded,
	/// A different candidate for the same target is already staged.
	Conflicting,
	/// Two entries for one key.
	DuplicateKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivateRefusal {
	/// No staged candidate matches the committed certificate: behind boundary.
	NoMatchingCandidate,
	/// The staged rows do not hash to the certificate's chunks (damaged or
	/// changed staged evidence): behind boundary, nothing installed.
	StagedRowsMismatch,
	/// DC-272: an intent this activation would retire belongs to a producer
	/// stripe its fence does not hold (one begun after the fence's read).
	/// Nothing changed; retryable.
	ProducerUnfenced,
}

/// What activation checks the staged candidate against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Activation {
	pub gid: u64,
	pub target_digest: [u8; 32],
	pub predecessor_gid: u64,
	pub chunks: Vec<ChunkSum>,
	/// Every parent reserved at or before this was erased: its intents and
	/// owner entries are retired with the activation.
	pub t: u64,
}

/// DC-269: the cut of the certificate whose base is installed here. Every
/// parent reserved at or before `t` was erased by that certificate, so no intent
/// may be begun for one. It is written in the same transaction that installs the
/// base (activation or recovered publication). A base installed without it (by
/// an earlier build) is unconfirmed: no intent is admitted until a
/// certificate-aware step confirms or writes it from the trusted certificate of
/// the installed generation and chunks ([`PersistentLocalIndex::confirm_installed_cut`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledCut {
	pub gid: u64,
	pub t: u64,
}

/// DC-269: the installed cut's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutState {
	/// Nothing installed (generation 0): nothing was erased.
	Genesis,
	/// The installed generation's own cut.
	Current(InstalledCut),
	/// A generation is installed, with no cut or an earlier generation's (an
	/// earlier build installed it): no intent is admitted until it is confirmed.
	Unconfirmed,
}

/// DC-269: what [`PersistentLocalIndex::confirm_installed_cut`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutConfirmation {
	/// Another generation is installed: nothing was checked or written.
	NotInstalled,
	/// The generation is installed with other chunks: nothing was written.
	OtherBase,
	/// The installed cut equals the certificate's.
	Confirmed,
	/// The unconfirmed cut was written from the certificate.
	Upgraded,
}

/// DC-269: the cut's state against `active_gid`. A cut at genesis or for a later
/// generation than the installed one is damage, refused.
fn read_cut(
	table: &impl ReadableTable<&'static str, &'static [u8]>,
	active_gid: u64,
) -> std::io::Result<CutState> {
	let cut: Option<InstalledCut> = match table.get(BASE_CUT_KEY).map_err(redb_to_io)? {
		None => None,
		Some(bytes) => Some(
			postcard::from_bytes(bytes.value())
				.map_err(|error| Error::new(ErrorKind::InvalidData, error))?,
		),
	};
	match cut {
		None if active_gid == 0 => Ok(CutState::Genesis),
		Some(cut) if active_gid != 0 && cut.gid == active_gid => Ok(CutState::Current(cut)),
		None => Ok(CutState::Unconfirmed),
		Some(cut) if active_gid != 0 && cut.gid < active_gid => Ok(CutState::Unconfirmed),
		Some(cut) => Err(Error::new(
			ErrorKind::InvalidData,
			format!("the installed base (generation {active_gid}) has the installed cut {cut:?}"),
		)),
	}
}

/// DC-269: the admission read, in the transaction that would begin an intent:
/// the installed generation alone (the first field of the stored metadata, a
/// varint) and the small cut row; never the chunk vectors.
pub(super) fn admission_cut_in(tx: &redb::WriteTransaction) -> std::io::Result<CutState> {
	let table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
	let active_gid = match table.get(BASE_META_KEY).map_err(redb_to_io)? {
		None => 0,
		Some(bytes) => {
			postcard::take_from_bytes::<u64>(bytes.value())
				.map_err(|error| Error::new(ErrorKind::InvalidData, error))?
				.0
		}
	};
	read_cut(&table, active_gid)
}

/// DC-269: records `cut` with the base installed in the same transaction.
fn write_cut(
	table: &mut redb::Table<&'static str, &'static [u8]>,
	cut: InstalledCut,
) -> std::io::Result<()> {
	table
		.insert(BASE_CUT_KEY, encode(&cut)?.as_slice())
		.map_err(redb_to_io)?;
	Ok(())
}

/// DC-269: an installed generation's cut against a trusted `t` for it: equal
/// confirms; unconfirmed is written; another is refused (fail closed).
fn settle_cut(
	table: &mut redb::Table<&'static str, &'static [u8]>,
	gid: u64,
	t: u64,
) -> std::io::Result<CutConfirmation> {
	match read_cut(table, gid)? {
		CutState::Current(cut) if cut.t == t => Ok(CutConfirmation::Confirmed),
		CutState::Current(cut) => Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"generation {gid} is installed with the installed cut t {}, not the certificate's {t}",
				cut.t
			),
		)),
		CutState::Unconfirmed => {
			write_cut(table, InstalledCut { gid, t })?;
			Ok(CutConfirmation::Upgraded)
		}
		CutState::Genesis => Ok(CutConfirmation::NotInstalled),
	}
}

/// ADR 0703: the active generation, read in the caller's write transaction.
pub(super) fn active_gid_in(tx: &redb::WriteTransaction) -> std::io::Result<u64> {
	Ok(read_meta(&tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?)?.active_gid)
}

/// The base metadata. DC-269: every read also checks the installed cut against
/// it, so an inconsistent store fails closed wherever its base is consulted.
fn read_meta(table: &impl ReadableTable<&'static str, &'static [u8]>) -> std::io::Result<BaseMeta> {
	let meta: BaseMeta = match table.get(BASE_META_KEY).map_err(redb_to_io)? {
		None => BaseMeta {
			active_gid: 0,
			active_chunks: chunk_sums(std::iter::empty()),
			staged: None,
		},
		Some(bytes) => postcard::from_bytes(bytes.value())
			.map_err(|error| Error::new(ErrorKind::InvalidData, error))?,
	};
	read_cut(table, meta.active_gid)?;
	Ok(meta)
}

fn encode<T: Serialize>(value: &T) -> std::io::Result<Vec<u8>> {
	postcard::to_stdvec(value).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

fn decode_entry(bytes: &[u8]) -> std::io::Result<BaseEntry> {
	postcard::from_bytes(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

impl PersistentLocalIndex {
	pub(crate) async fn base_meta(&self) -> std::io::Result<BaseMeta> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				match tx.open_table(BASE_META_TABLE) {
					Ok(table) => read_meta(&table),
					Err(redb::TableError::TableDoesNotExist(_)) => Ok(BaseMeta {
						active_gid: 0,
						active_chunks: chunk_sums(std::iter::empty()),
						staged: None,
					}),
					Err(error) => Err(redb_to_io(error)),
				}
			})
			.await
	}

	/// ADR 0106 (wendelon): the saved recovery statement, if any.
	pub(crate) async fn recovery_submission(&self) -> std::io::Result<Option<RecoverySubmission>> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = match tx.open_table(BASE_META_TABLE) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
					Err(error) => return Err(redb_to_io(error)),
				};
				match table.get(RECOVERY_SUBMISSION_KEY).map_err(redb_to_io)? {
					None => Ok(None),
					Some(bytes) => postcard::from_bytes(bytes.value())
						.map(Some)
						.map_err(|error| Error::new(ErrorKind::InvalidData, error)),
				}
			})
			.await
	}

	/// ADR 0106 (wendelon): saves `submission` once. Saving it again is a
	/// no-op; saving another is refused (`Ok(false)`) and changes nothing.
	pub(crate) async fn save_recovery_submission(
		&self,
		submission: RecoverySubmission,
	) -> std::io::Result<bool> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let mut table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				if let Some(bytes) = table.get(RECOVERY_SUBMISSION_KEY).map_err(redb_to_io)? {
					let saved: RecoverySubmission = postcard::from_bytes(bytes.value())
						.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
					return Ok(saved == submission);
				}
				table
					.insert(RECOVERY_SUBMISSION_KEY, encode(&submission)?.as_slice())
					.map_err(redb_to_io)?;
				drop(table);
				tx.commit().map_err(redb_to_io)?;
				Ok(true)
			})
			.await
	}

	/// ADR 0106 (wendelon): up to `limit` active entries after `after`, in key
	/// order: one page of the certified base for a recovering replacement.
	pub(crate) async fn base_page(
		&self,
		after: Option<[u8; 56]>,
		limit: usize,
	) -> std::io::Result<Vec<BaseEntry>> {
		self.entry_page(BASE_ACTIVE_TABLE, after, limit).await
	}

	/// ADR 0106 (wendelon): up to `limit` staged recovered entries after the
	/// key `after`, in key order (the order a source's pages walk).
	pub(crate) async fn recovered_page(
		&self,
		after: Option<[u8; 56]>,
		limit: usize,
	) -> std::io::Result<Vec<BaseEntry>> {
		self.entry_page(BASE_RECOVERY_TABLE, after, limit).await
	}

	async fn entry_page(
		&self,
		definition: redb::TableDefinition<'static, &'static [u8], &'static [u8]>,
		after: Option<[u8; 56]>,
		limit: usize,
	) -> std::io::Result<Vec<BaseEntry>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let table = match tx.open_table(definition) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
					Err(error) => return Err(redb_to_io(error)),
				};
				let rows = match &after {
					Some(after) => table
						.range::<&[u8]>((
							std::ops::Bound::Excluded(after.as_slice()),
							std::ops::Bound::Unbounded,
						))
						.map_err(redb_to_io)?,
					None => table.iter().map_err(redb_to_io)?,
				};
				let mut page = Vec::new();
				for row in rows.take(limit) {
					let (_, value) = row.map_err(redb_to_io)?;
					page.push(decode_entry(value.value())?);
				}
				Ok(page)
			})
			.await
	}

	/// Every active entry, or every staged one.
	pub(crate) async fn base_entries(&self, staged: bool) -> std::io::Result<Vec<BaseEntry>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let definition = if staged {
					BASE_STAGED_TABLE
				} else {
					BASE_ACTIVE_TABLE
				};
				let table = match tx.open_table(definition) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
					Err(error) => return Err(redb_to_io(error)),
				};
				let mut entries = Vec::new();
				for row in table.iter().map_err(redb_to_io)? {
					let (_, value) = row.map_err(redb_to_io)?;
					entries.push(decode_entry(value.value())?);
				}
				Ok(entries)
			})
			.await
	}

	/// The active entry of one key.
	pub(crate) async fn base_entry(
		&self,
		tenant: TenantId,
		table: TableObjectId,
		key_identity: [u8; 32],
	) -> std::io::Result<Option<BaseEntry>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let active = match tx.open_table(BASE_ACTIVE_TABLE) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
					Err(error) => return Err(redb_to_io(error)),
				};
				let key = base_key(tenant, table, &key_identity);
				active
					.get(key.as_slice())
					.map_err(redb_to_io)?
					.map(|value| decode_entry(value.value()))
					.transpose()
			})
			.await
	}

	/// Stages the complete candidate for one target, durably, in one
	/// transaction, fenced against the active predecessor and later targets.
	pub(crate) async fn stage_base(
		&self,
		candidate: StagedCandidate,
		entries: Vec<BaseEntry>,
	) -> std::io::Result<Result<(), StageRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let mut meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				let mut meta = read_meta(&meta_table)?;
				if meta.active_gid != candidate.predecessor_gid {
					return Ok(Err(StageRefusal::PredecessorMoved));
				}
				match &meta.staged {
					Some(staged) if staged.gid > candidate.gid => {
						return Ok(Err(StageRefusal::Superseded));
					}
					Some(staged) if staged.gid == candidate.gid => {
						return Ok(if *staged == candidate {
							Ok(())
						} else {
							Err(StageRefusal::Conflicting)
						});
					}
					_ => {}
				}
				let mut staged = tx.open_table(BASE_STAGED_TABLE).map_err(redb_to_io)?;
				staged.retain(|_, _| false).map_err(redb_to_io)?;
				for entry in &entries {
					if staged
						.insert(entry.key().as_slice(), encode(entry)?.as_slice())
						.map_err(redb_to_io)?
						.is_some()
					{
						return Ok(Err(StageRefusal::DuplicateKey));
					}
				}
				drop(staged);
				meta.staged = Some(candidate);
				meta_table
					.insert(BASE_META_KEY, encode(&meta)?.as_slice())
					.map_err(redb_to_io)?;
				drop(meta_table);
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}

	/// ADR 0106 (wendelon): stages one verified page of a recovered base for
	/// `certificate` (its gid, digest, predecessor and chunks), on a pristine
	/// store only. The page's rows must already be durable in this store's
	/// slabs. A key staged again with equal content is accepted; with other
	/// content, refused. The ordinary generation staging is not touched.
	pub(crate) async fn stage_recovered_page(
		&self,
		certificate: StagedCandidate,
		entries: Vec<BaseEntry>,
	) -> std::io::Result<Result<(), InstallRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let mut meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				let meta = read_meta(&meta_table)?;
				let active_empty = tx
					.open_table(BASE_ACTIVE_TABLE)
					.map_err(redb_to_io)?
					.first()
					.map_err(redb_to_io)?
					.is_none();
				if meta.active_gid != 0 || meta.staged.is_some() || !active_empty {
					return Ok(Err(InstallRefusal::NotPristine));
				}
				match meta_table.get(BASE_RECOVERY_KEY).map_err(redb_to_io)? {
					Some(bytes) => {
						let staged: StagedCandidate = postcard::from_bytes(bytes.value())
							.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
						if staged != certificate {
							return Ok(Err(InstallRefusal::OtherCertificate));
						}
					}
					None => {}
				}
				let mut recovery = tx.open_table(BASE_RECOVERY_TABLE).map_err(redb_to_io)?;
				for entry in &entries {
					let encoded = encode(entry)?;
					if let Some(previous) = recovery
						.insert(entry.key().as_slice(), encoded.as_slice())
						.map_err(redb_to_io)?
						&& previous.value() != encoded.as_slice()
					{
						return Ok(Err(InstallRefusal::ConflictingEntry));
					}
				}
				drop(recovery);
				meta_table
					.insert(BASE_RECOVERY_KEY, encode(&certificate)?.as_slice())
					.map_err(redb_to_io)?;
				drop(meta_table);
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}

	/// ADR 0106 (wendelon): whether the staged recovered entries for
	/// `certificate` hash exactly to its chunks (counts included), streamed.
	pub(crate) async fn recovered_staging_matches(
		&self,
		certificate: StagedCandidate,
	) -> std::io::Result<bool> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let meta = match tx.open_table(BASE_META_TABLE) {
					Ok(table) => table,
					Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
					Err(error) => return Err(redb_to_io(error)),
				};
				let staged: Option<StagedCandidate> =
					match meta.get(BASE_RECOVERY_KEY).map_err(redb_to_io)? {
						Some(bytes) => Some(
							postcard::from_bytes(bytes.value())
								.map_err(|error| Error::new(ErrorKind::InvalidData, error))?,
						),
						None => None,
					};
				if staged.as_ref() != Some(&certificate) {
					return Ok(false);
				}
				let mut sums = crate::consensus::decisions::generations::ChunkAccumulator::new();
				match tx.open_table(BASE_RECOVERY_TABLE) {
					Ok(table) => {
						for row in table.iter().map_err(redb_to_io)? {
							let (_, value) = row.map_err(redb_to_io)?;
							let entry = decode_entry(value.value())?;
							sums.add(entry.chunk(), &entry.preimage());
						}
					}
					Err(redb::TableError::TableDoesNotExist(_)) => {}
					Err(error) => return Err(redb_to_io(error)),
				}
				Ok(sums.finish() == certificate.chunks)
			})
			.await
	}

	/// ADR 0106 (wendelon): discards the staged recovered entries and their
	/// certificate, on a pristine store only: an untrusted attempt's metadata,
	/// before any of its rows was stored. Nothing published is touched.
	pub(crate) async fn discard_recovered_staging(
		&self,
	) -> std::io::Result<Result<(), InstallRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let mut meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				let meta = read_meta(&meta_table)?;
				let active_empty = tx
					.open_table(BASE_ACTIVE_TABLE)
					.map_err(redb_to_io)?
					.first()
					.map_err(redb_to_io)?
					.is_none();
				if meta.active_gid != 0 || meta.staged.is_some() || !active_empty {
					return Ok(Err(InstallRefusal::NotPristine));
				}
				tx.open_table(BASE_RECOVERY_TABLE)
					.map_err(redb_to_io)?
					.retain(|_, _| false)
					.map_err(redb_to_io)?;
				meta_table.remove(BASE_RECOVERY_KEY).map_err(redb_to_io)?;
				drop(meta_table);
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}

	/// ADR 0106 (wendelon): publishes the staged recovered base as this store's
	/// active base at `certificate`'s gid, in one transaction, only if the
	/// staged entries hash to its chunks (recomputed as they are read, never
	/// trusted from the pages). The recovery staging is cleared in the same
	/// transaction. Publishing an already published certificate is answered
	/// as done.
	///
	/// DC-269: `t` is the trusted certificate's cut, installed with the base.
	pub(crate) async fn publish_recovered(
		&self,
		certificate: StagedCandidate,
		t: u64,
	) -> std::io::Result<Result<(), InstallRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let mut meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				let meta = read_meta(&meta_table)?;
				if meta.active_gid == certificate.gid
					&& meta.active_chunks == certificate.chunks
					&& meta_table
						.get(BASE_RECOVERY_KEY)
						.map_err(redb_to_io)?
						.is_none()
				{
					// DC-269: the installed cut is the certificate's (written if
					// unconfirmed), or the repeat is refused.
					if settle_cut(&mut meta_table, meta.active_gid, t)? == CutConfirmation::Upgraded
					{
						drop(meta_table);
						tx.commit().map_err(redb_to_io)?;
					}
					return Ok(Ok(()));
				}
				let mut active = tx.open_table(BASE_ACTIVE_TABLE).map_err(redb_to_io)?;
				if meta.active_gid != 0
					|| meta.staged.is_some()
					|| active.first().map_err(redb_to_io)?.is_some()
				{
					return Ok(Err(InstallRefusal::NotPristine));
				}
				match meta_table.get(BASE_RECOVERY_KEY).map_err(redb_to_io)? {
					Some(bytes) => {
						let staged: StagedCandidate = postcard::from_bytes(bytes.value())
							.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
						if staged != certificate {
							return Ok(Err(InstallRefusal::OtherCertificate));
						}
					}
					None => return Ok(Err(InstallRefusal::OtherCertificate)),
				}
				let mut recovery = tx.open_table(BASE_RECOVERY_TABLE).map_err(redb_to_io)?;
				let mut decode_error = None;
				let sums =
					chunk_sums_of(recovery.iter().map_err(redb_to_io)?.filter_map(
						|row| match row {
							Ok((_, value)) => match decode_entry(value.value()) {
								Ok(entry) => Some((entry.chunk(), entry.preimage())),
								Err(error) => {
									decode_error.get_or_insert(error);
									None
								}
							},
							Err(error) => {
								decode_error.get_or_insert(redb_to_io(error));
								None
							}
						},
					));
				if let Some(error) = decode_error {
					return Err(error);
				}
				if sums != certificate.chunks {
					return Ok(Err(InstallRefusal::SumsMismatch));
				}
				for row in recovery.iter().map_err(redb_to_io)? {
					let (key, value) = row.map_err(redb_to_io)?;
					active
						.insert(key.value(), value.value())
						.map_err(redb_to_io)?;
				}
				recovery.retain(|_, _| false).map_err(redb_to_io)?;
				drop(recovery);
				drop(active);
				meta_table.remove(BASE_RECOVERY_KEY).map_err(redb_to_io)?;
				let published = BaseMeta {
					active_gid: certificate.gid,
					active_chunks: certificate.chunks,
					staged: None,
				};
				meta_table
					.insert(BASE_META_KEY, encode(&published)?.as_slice())
					.map_err(redb_to_io)?;
				write_cut(
					&mut meta_table,
					InstalledCut {
						gid: certificate.gid,
						t,
					},
				)?;
				drop(meta_table);
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}

	/// DC-269: the certificate-aware check of an already installed generation,
	/// in one transaction. With `gid` and `chunks` installed, the installed cut
	/// must equal the trusted certificate's `t`: equal confirms, unconfirmed is
	/// written from it, another fails closed (`InvalidData`). Another installed
	/// generation, or other chunks, are reported and nothing is written.
	pub(crate) async fn confirm_installed_cut(
		&self,
		gid: u64,
		chunks: Vec<ChunkSum>,
		t: u64,
	) -> std::io::Result<CutConfirmation> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let mut meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				let meta = read_meta(&meta_table)?;
				if gid == 0 || meta.active_gid != gid {
					return Ok(CutConfirmation::NotInstalled);
				}
				if meta.active_chunks != chunks {
					return Ok(CutConfirmation::OtherBase);
				}
				let confirmation = settle_cut(&mut meta_table, gid, t)?;
				if confirmation == CutConfirmation::Upgraded {
					drop(meta_table);
					tx.commit().map_err(redb_to_io)?;
				}
				Ok(confirmation)
			})
			.await
	}

	/// DC-269 (test only): replaces or removes the installed cut row, as an
	/// earlier build or damage would leave it.
	#[cfg(test)]
	pub(crate) async fn set_installed_cut_for_test(
		&self,
		cut: Option<InstalledCut>,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
					match cut {
						Some(cut) => write_cut(&mut table, cut)?,
						None => {
							table.remove(BASE_CUT_KEY).map_err(redb_to_io)?;
						}
					}
				}
				tx.commit().map_err(redb_to_io)?;
				Ok(())
			})
			.await
	}

	/// DC-269 (test only): replaces the installed generation's recorded chunks,
	/// as damage would leave them; the rows are not touched.
	#[cfg(test)]
	pub(crate) async fn set_active_chunks_for_test(
		&self,
		chunks: Vec<ChunkSum>,
	) -> std::io::Result<()> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
					let mut meta = read_meta(&table)?;
					meta.active_chunks = chunks;
					table
						.insert(BASE_META_KEY, encode(&meta)?.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)?;
				Ok(())
			})
			.await
	}

	/// DC-269: the installed cut's state, read with the metadata it is checked
	/// against.
	pub(crate) async fn installed_cut(&self) -> std::io::Result<CutState> {
		self.db
			.run(|db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				match tx.open_table(BASE_META_TABLE) {
					Ok(table) => read_cut(&table, read_meta(&table)?.active_gid),
					Err(redb::TableError::TableDoesNotExist(_)) => Ok(CutState::Genesis),
					Err(error) => Err(redb_to_io(error)),
				}
			})
			.await
	}

	/// Rewrites the staged rows of the already staged candidate `candidate`
	/// (equal metadata required) with `entries`, which must hash to its
	/// chunks: a recovered candidate whose rebuild on this start matches its
	/// recorded evidence gets exactly that inventory back. Anything else is
	/// refused and changes nothing; a reported candidate is never replaced by
	/// different evidence.
	pub(crate) async fn restage_base(
		&self,
		candidate: StagedCandidate,
		entries: Vec<BaseEntry>,
	) -> std::io::Result<Result<(), StageRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				if chunk_sums(&entries) != candidate.chunks {
					return Ok(Err(StageRefusal::Conflicting));
				}
				let tx = db.begin_write().map_err(redb_to_io)?;
				let meta = {
					let meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
					read_meta(&meta_table)?
				};
				if meta.staged.as_ref() != Some(&candidate) {
					return Ok(Err(StageRefusal::Conflicting));
				}
				{
					let mut staged = tx.open_table(BASE_STAGED_TABLE).map_err(redb_to_io)?;
					staged.retain(|_, _| false).map_err(redb_to_io)?;
					for entry in &entries {
						if staged
							.insert(entry.key().as_slice(), encode(entry)?.as_slice())
							.map_err(redb_to_io)?
							.is_some()
						{
							return Ok(Err(StageRefusal::DuplicateKey));
						}
					}
				}
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}

	/// Whether the staged rows hash to the staged candidate's chunks.
	pub(crate) async fn staged_rows_match(&self) -> std::io::Result<bool> {
		let meta = self.base_meta().await?;
		let Some(staged) = meta.staged else {
			return Ok(false);
		};
		Ok(chunk_sums(&self.base_entries(true).await?) == staged.chunks)
	}

	/// Activates the staged candidate of a committed generation, in one
	/// transaction: the candidate must match the certificate exactly and sit
	/// on the active predecessor; it replaces the active base; the intents and
	/// owner entries of every parent reserved at or before `t` are retired.
	/// Idempotent once the generation is active.
	///
	/// DC-272: `fence` holds the producer stripes of the intents this activation
	/// retires. It moves into the operation that commits or aborts, so those
	/// stripes stay held until the transaction ends, even if this call's caller
	/// is cancelled. If any recorded intent at or below `t` belongs to a stripe
	/// the fence does not hold, nothing changes (`ProducerUnfenced`).
	pub(crate) async fn activate_base(
		&self,
		activation: Activation,
		fence: crate::network::data_server::child_writes::ProducerFence,
	) -> std::io::Result<Result<(), ActivateRefusal>> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let fence = fence;
				let tx = db.begin_write().map_err(redb_to_io)?;
				// Every intent this activation could retire has its producer
				// fenced, or nothing changes. Checked in this transaction.
				{
					let intents = tx
						.open_table(super::parent_intents::PARENT_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					for row in intents.iter().map_err(redb_to_io)? {
						let (key, _) = row.map_err(redb_to_io)?;
						let rsv =
							u64::from_be_bytes(key.value()[..8].try_into().expect("intent key"));
						if rsv > activation.t {
							break;
						}
						if !fence.covers(rsv) {
							return Ok(Err(ActivateRefusal::ProducerUnfenced));
						}
					}
				}
				let mut meta_table = tx.open_table(BASE_META_TABLE).map_err(redb_to_io)?;
				let mut meta = read_meta(&meta_table)?;
				if meta.active_gid == activation.gid {
					// DC-269: the repeat is done only for the certificate's own base
					// and cut: other chunks, or another cut, fail closed and write
					// nothing; an unconfirmed cut is written from the certificate.
					if meta.active_chunks != activation.chunks {
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"generation {} is installed with chunks other than its certificate's",
								activation.gid
							),
						));
					}
					if settle_cut(&mut meta_table, meta.active_gid, activation.t)?
						== CutConfirmation::Upgraded
					{
						drop(meta_table);
						tx.commit().map_err(redb_to_io)?;
					}
					return Ok(Ok(()));
				}
				let matches = meta.staged.as_ref().is_some_and(|staged| {
					staged.gid == activation.gid
						&& staged.target_digest == activation.target_digest
						&& staged.predecessor_gid == activation.predecessor_gid
						&& staged.chunks == activation.chunks
				}) && meta.active_gid == activation.predecessor_gid;
				if !matches {
					return Ok(Err(ActivateRefusal::NoMatchingCandidate));
				}
				let staged_rows: Vec<(Vec<u8>, Vec<u8>)> = {
					let staged = tx.open_table(BASE_STAGED_TABLE).map_err(redb_to_io)?;
					let mut rows = Vec::new();
					for row in staged.iter().map_err(redb_to_io)? {
						let (key, value) = row.map_err(redb_to_io)?;
						rows.push((key.value().to_vec(), value.value().to_vec()));
					}
					rows
				};
				// The rows installed are exactly the certified inventory: they
				// must hash to the certificate's chunks, not only the metadata.
				let entries = staged_rows
					.iter()
					.map(|(_, value)| decode_entry(value))
					.collect::<std::io::Result<Vec<_>>>()?;
				if chunk_sums(&entries) != activation.chunks {
					return Ok(Err(ActivateRefusal::StagedRowsMismatch));
				}
				// ADR 0699 S5b: the previous base, whose displaced entries
				// become debt.
				let displaced: Vec<BaseEntry> = {
					let active = tx.open_table(BASE_ACTIVE_TABLE).map_err(redb_to_io)?;
					let mut previous = Vec::new();
					for row in active.iter().map_err(redb_to_io)? {
						let (_, value) = row.map_err(redb_to_io)?;
						previous.push(decode_entry(value.value())?);
					}
					previous
				};
				{
					let mut active = tx.open_table(BASE_ACTIVE_TABLE).map_err(redb_to_io)?;
					active.retain(|_, _| false).map_err(redb_to_io)?;
					for (key, value) in &staged_rows {
						active
							.insert(key.as_slice(), value.as_slice())
							.map_err(redb_to_io)?;
					}
				}
				{
					let mut staged = tx.open_table(BASE_STAGED_TABLE).map_err(redb_to_io)?;
					staged.retain(|_, _| false).map_err(redb_to_io)?;
				}
				// ADR 0699 S5b: the new base's entries by key, for the debt the
				// activation records below.
				let new_base: std::collections::HashMap<[u8; 56], BaseEntry> =
					entries.iter().map(|entry| (entry.key(), *entry)).collect();
				let mut debt = tx.open_table(CLEANUP_DEBT_TABLE).map_err(redb_to_io)?;
				let mut record_debt =
					|key: VersionKey, record: DebtRecord| -> std::io::Result<()> {
						debt.insert(key.debt_bytes().as_slice(), encode(&record)?.as_slice())
							.map_err(redb_to_io)?;
						Ok(())
					};
				// Each displaced previous base entry is obsolete.
				for entry in &displaced {
					let replaced = new_base
						.get(&entry.key())
						.is_none_or(|new| new.hlc != entry.hlc || new.tombstone != entry.tombstone);
					if replaced {
						record_debt(
							VersionKey {
								tenant: entry.tenant,
								table: entry.table,
								key_identity: entry.key_identity,
								hlc: entry.hlc,
							},
							DebtRecord {
								gid: activation.gid,
								row_digest: entry.row_digest,
								source: DebtSource::DisplacedBase {
									tombstone: entry.tombstone,
								},
							},
						)?;
					}
				}
				// The erased parents' intents and owners retire with activation.
				{
					let mut intents = tx
						.open_table(super::parent_intents::PARENT_INTENTS_TABLE)
						.map_err(redb_to_io)?;
					let mut retired = Vec::new();
					for row in intents.iter().map_err(redb_to_io)? {
						let (key, value) = row.map_err(redb_to_io)?;
						let rsv =
							u64::from_be_bytes(key.value()[..8].try_into().expect("intent key"));
						if rsv <= activation.t {
							retired.push((key.value().to_vec(), value.value().to_vec()));
						}
					}
					let mut owners = tx
						.open_table(super::parent_intents::PARENT_ROW_OWNERS_TABLE)
						.map_err(redb_to_io)?;
					for (key, value) in &retired {
						let intent = super::parent_intents::decode_intent_for_base(value)?;
						let mine =
							super::parent_intents::owner_value_for_base(intent.rsv, intent.ordinal);
						for version in &intent.manifest {
							let owner =
								super::parent_intents::owner_key_for_base(intent.tenant, version);
							// ADR 0699 S5b: a version is listed as debt only when
							// this intent still owns it and it is not its key's new
							// base entry (an equal HLC protects either kind).
							let owned_here = owners
								.get(owner.as_slice())
								.map_err(redb_to_io)?
								.is_some_and(|value| value.value() == mine.as_slice());
							let winner = new_base
								.get(&base_key(
									intent.tenant,
									version.table,
									&version.key_identity,
								))
								.is_some_and(|entry| entry.hlc == version.hlc);
							if owned_here && !winner {
								record_debt(
									VersionKey {
										tenant: intent.tenant,
										table: version.table,
										key_identity: version.key_identity,
										hlc: version.hlc,
									},
									DebtRecord {
										gid: activation.gid,
										row_digest: version.row_digest,
										source: DebtSource::RetiredIntent {
											rsv: intent.rsv,
											ordinal: intent.ordinal,
										},
									},
								)?;
							}
							// Only this intent's own owner entry retires with it;
							// another producer's ownership stays.
							if owned_here {
								owners.remove(owner.as_slice()).map_err(redb_to_io)?;
							}
						}
						intents.remove(key.as_slice()).map_err(redb_to_io)?;
					}
				}
				drop(debt);
				meta.active_gid = activation.gid;
				meta.active_chunks = activation.chunks;
				meta.staged = None;
				meta_table
					.insert(BASE_META_KEY, encode(&meta)?.as_slice())
					.map_err(redb_to_io)?;
				write_cut(
					&mut meta_table,
					InstalledCut {
						gid: activation.gid,
						t: activation.t,
					},
				)?;
				drop(meta_table);
				tx.commit().map_err(redb_to_io)?;
				Ok(Ok(()))
			})
			.await
	}
}

/// ADR 0699 S5b: the cleanup debt.
impl PersistentLocalIndex {
	/// The survivor rule's facts for `candidates`, read in ONE transaction:
	/// the active generation, and per candidate its debt record, its key's
	/// active base entry and whether an owner entry names it. A missing table
	/// reads as empty; any other error fails the whole read.
	pub(crate) async fn version_facts(
		&self,
		candidates: Vec<VersionRef>,
	) -> std::io::Result<CoherentFacts> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let active_gid = match tx.open_table(BASE_META_TABLE) {
					Ok(table) => read_meta(&table)?.active_gid,
					Err(redb::TableError::TableDoesNotExist(_)) => 0,
					Err(error) => return Err(redb_to_io(error)),
				};
				let debt = optional_table(&tx, CLEANUP_DEBT_TABLE)?;
				let base = optional_table(&tx, BASE_ACTIVE_TABLE)?;
				let owners = optional_table(&tx, super::parent_intents::PARENT_ROW_OWNERS_TABLE)?;
				let mut facts = Vec::with_capacity(candidates.len());
				for candidate in &candidates {
					let mut listed = Vec::new();
					if let Some(table) = &debt {
						let prefix = candidate.debt_prefix();
						let mut low = [0u8; 72];
						let mut high = [0xFFu8; 72];
						low[..56].copy_from_slice(&prefix);
						high[..56].copy_from_slice(&prefix);
						for row in table
							.range(low.as_slice()..=high.as_slice())
							.map_err(redb_to_io)?
						{
							let (key, value) = row.map_err(redb_to_io)?;
							listed.push((
								VersionKey::from_debt_bytes(key.value())?,
								decode_debt(value.value())?,
							));
						}
					}
					let ambiguous = listed.len() > 1;
					let debt = if ambiguous { None } else { listed.pop() };
					let (base, owned) = match &debt {
						Some((key, _)) => {
							let base = match &base {
								Some(table) => match table
									.get(
										base_key(key.tenant, key.table, &key.key_identity)
											.as_slice(),
									)
									.map_err(redb_to_io)?
								{
									Some(value) => Some(decode_entry(value.value())?),
									None => None,
								},
								None => None,
							};
							let owned = match &owners {
								Some(table) => table
									.get(key.owner_bytes().as_slice())
									.map_err(redb_to_io)?
									.is_some(),
								None => false,
							};
							(base, owned)
						}
						None => (None, false),
					};
					facts.push(VersionFacts {
						debt,
						ambiguous,
						base,
						owned,
					});
				}
				Ok(CoherentFacts {
					active_gid,
					candidates,
					facts,
				})
			})
			.await
	}

	/// Up to `limit` debt entries after `after`, in key order.
	pub(crate) async fn debt_page(
		&self,
		after: Option<VersionKey>,
		limit: usize,
	) -> std::io::Result<Vec<(VersionKey, DebtRecord)>> {
		self.db
			.run(move |db| {
				let tx = db.begin_read().map_err(redb_to_io)?;
				let Some(debt) = optional_table(&tx, CLEANUP_DEBT_TABLE)? else {
					return Ok(Vec::new());
				};
				let low = after.map(|key| key.debt_bytes());
				let mut page = Vec::new();
				let rows = match &low {
					Some(low) => debt.range::<&[u8]>((
						std::ops::Bound::Excluded(low.as_slice()),
						std::ops::Bound::Unbounded,
					)),
					None => debt.range::<&[u8]>(..),
				}
				.map_err(redb_to_io)?;
				for row in rows.take(limit) {
					let (key, value) = row.map_err(redb_to_io)?;
					page.push((
						VersionKey::from_debt_bytes(key.value())?,
						decode_debt(value.value())?,
					));
				}
				Ok(page)
			})
			.await
	}

	/// Retires one debt entry iff, in one write transaction, it is still
	/// exactly `expected` (never a newer incarnation), no active base entry is
	/// at its HLC, no owner names it and the index has no copy of it. Returns
	/// whether it was retired.
	pub(crate) async fn retire_debt(
		&self,
		key: VersionKey,
		expected: DebtRecord,
	) -> std::io::Result<bool> {
		let _guard = self.write_lock.lock().await;
		self.db
			.run(move |db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				let retired = {
					let mut debt = tx.open_table(CLEANUP_DEBT_TABLE).map_err(redb_to_io)?;
					let exact = match debt.get(key.debt_bytes().as_slice()).map_err(redb_to_io)? {
						Some(value) => decode_debt(value.value())? == expected,
						None => false,
					};
					let protected_by_base = tx
						.open_table(BASE_ACTIVE_TABLE)
						.map_err(redb_to_io)?
						.get(base_key(key.tenant, key.table, &key.key_identity).as_slice())
						.map_err(redb_to_io)?
						.map(|value| decode_entry(value.value()))
						.transpose()?
						.is_some_and(|entry| entry.hlc == key.hlc);
					let owned = tx
						.open_table(super::parent_intents::PARENT_ROW_OWNERS_TABLE)
						.map_err(redb_to_io)?
						.get(key.owner_bytes().as_slice())
						.map_err(redb_to_io)?
						.is_some();
					let copies = super::parent_intents::record_copies_for_base(
						&tx.open_table(RECORDS_TABLE).map_err(redb_to_io)?,
						key.tenant,
						&key.as_owned(expected.row_digest),
					)?;
					let retire = exact && !protected_by_base && !owned && copies == 0;
					if retire {
						debt.remove(key.debt_bytes().as_slice())
							.map_err(redb_to_io)?;
					}
					retire
				};
				// Nothing retired: abort, so a sweep over debt that still has
				// copies writes (and syncs) nothing.
				if retired {
					tx.commit().map_err(redb_to_io)?;
				} else {
					tx.abort().map_err(redb_to_io)?;
				}
				Ok(retired)
			})
			.await
	}
}

fn decode_debt(bytes: &[u8]) -> std::io::Result<DebtRecord> {
	postcard::from_bytes(bytes).map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

/// A table of a read transaction, or `None` if it was never created.
fn optional_table<K: redb::Key + 'static, V: redb::Value + 'static>(
	tx: &redb::ReadTransaction,
	definition: TableDefinition<K, V>,
) -> std::io::Result<Option<redb::ReadOnlyTable<K, V>>> {
	match tx.open_table(definition) {
		Ok(table) => Ok(Some(table)),
		Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
		Err(error) => Err(redb_to_io(error)),
	}
}

#[cfg(test)]
impl PersistentLocalIndex {
	/// Test-only: remove one staged row and leave the metadata, as damage to
	/// the staged evidence would.
	pub(crate) async fn drop_staged_row_for_test(&self) -> std::io::Result<()> {
		self.db
			.run(|db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut staged = tx.open_table(BASE_STAGED_TABLE).map_err(redb_to_io)?;
					let key = {
						let mut rows = staged.iter().map_err(redb_to_io)?;
						let (key, _) = rows
							.next()
							.ok_or_else(|| Error::other("no staged row"))?
							.map_err(redb_to_io)?;
						key.value().to_vec()
					};
					staged.remove(key.as_slice()).map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}

	/// Test-only: alter one active entry's row digest and leave the metadata,
	/// as physical damage to the base would.
	pub(crate) async fn tamper_base_entry_for_test(&self) -> std::io::Result<()> {
		self.db
			.run(|db| {
				let tx = db.begin_write().map_err(redb_to_io)?;
				{
					let mut active = tx.open_table(BASE_ACTIVE_TABLE).map_err(redb_to_io)?;
					let (key, mut entry) = {
						let mut rows = active.iter().map_err(redb_to_io)?;
						let (key, value) = rows
							.next()
							.ok_or_else(|| Error::other("no base entry"))?
							.map_err(redb_to_io)?;
						(key.value().to_vec(), decode_entry(value.value())?)
					};
					entry.row_digest[0] ^= 0xFF;
					active
						.insert(key.as_slice(), encode(&entry)?.as_slice())
						.map_err(redb_to_io)?;
				}
				tx.commit().map_err(redb_to_io)
			})
			.await
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::decisions::generations::GENERATION_CHUNKS;
	use crate::storage::index::parent_intents::{OwnedVersion, ParentIntent, ParentIntentState};

	const TENANT: TenantId = TenantId(7);

	fn entry(key: u8, at: u64, chunk: u8) -> BaseEntry {
		BaseEntry {
			tenant: TENANT,
			table: TableObjectId::new([3; 16]),
			key_identity: [key; 32],
			hlc: HybridTimestamp {
				physical_ms: at,
				logical: 0,
				node_id: 1,
			},
			row_digest: [key ^ 0x5A; 32],
			tombstone: false,
			position: u64::from(chunk) << 56,
		}
	}

	fn candidate(gid: u64, predecessor_gid: u64, entries: &[BaseEntry]) -> StagedCandidate {
		StagedCandidate {
			gid,
			target_digest: [gid as u8; 32],
			predecessor_gid,
			chunks: chunk_sums(entries),
		}
	}

	async fn open(tag: &str) -> (crate::test_support::TestDir, PersistentLocalIndex) {
		let dir = crate::test_support::TestDir::new(&format!("wendelon-certified-base-{tag}"));
		let index = PersistentLocalIndex::initialize_new(dir.join("index.redb"), None)
			.await
			.unwrap();
		(dir, index)
	}

	/// CODEX-4: staged rows changed under unchanged metadata are never
	/// installed; a recovered candidate's rows are restored only by a rebuild
	/// that hashes to its staged evidence.
	#[tokio::test]
	async fn changed_staged_rows_never_activate_and_restore_only_from_matching_evidence() {
		let (_dir, index) = open("staged-rows").await;
		let entries = vec![entry(1, 5, 0), entry(2, 6, 7)];
		let staged = candidate(1, 0, &entries);
		index
			.stage_base(staged.clone(), entries.clone())
			.await
			.unwrap()
			.unwrap();
		assert!(index.staged_rows_match().await.unwrap());
		index.drop_staged_row_for_test().await.unwrap();
		assert!(!index.staged_rows_match().await.unwrap());
		let activation = Activation {
			gid: 1,
			target_digest: staged.target_digest,
			predecessor_gid: 0,
			chunks: staged.chunks.clone(),
			t: 8,
		};
		let before = index.base_meta().await.unwrap();
		assert_eq!(
			index
				.activate_base(
					activation.clone(),
					crate::network::data_server::child_writes::ProducerFence::uncontended_for_test(
					)
					.await
				)
				.await
				.unwrap(),
			Err(ActivateRefusal::StagedRowsMismatch)
		);
		assert_eq!(
			index.base_meta().await.unwrap(),
			before,
			"nothing installed"
		);
		assert!(index.base_entries(false).await.unwrap().is_empty());
		// A rebuild with other evidence is refused and changes nothing.
		assert_eq!(
			index
				.restage_base(staged.clone(), vec![entry(1, 5, 0)])
				.await
				.unwrap(),
			Err(StageRefusal::Conflicting)
		);
		assert!(!index.staged_rows_match().await.unwrap());
		// A rebuild that reproduces the staged evidence restores its rows.
		index
			.restage_base(staged.clone(), entries.clone())
			.await
			.unwrap()
			.unwrap();
		assert!(index.staged_rows_match().await.unwrap());
		index
			.activate_base(
				activation,
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(index.base_entries(false).await.unwrap().len(), 2);
	}

	fn owned(key: u8, at: u64, digest: u8) -> OwnedVersion {
		OwnedVersion {
			table: TableObjectId::new([3; 16]),
			key_identity: [key; 32],
			hlc: HybridTimestamp {
				physical_ms: at,
				logical: 0,
				node_id: 1,
			},
			row_digest: [digest; 32],
		}
	}

	fn version_key(version: &OwnedVersion) -> VersionKey {
		VersionKey {
			tenant: TENANT,
			table: version.table,
			key_identity: version.key_identity,
			hlc: version.hlc,
		}
	}

	async fn begin(index: &PersistentLocalIndex, rsv: u64, manifest: Vec<OwnedVersion>) {
		index
			.begin_parent_intent(ParentIntent {
				tenant: TENANT,
				rsv,
				ordinal: 0,
				claim_digest: [rsv as u8; 32],
				range_start: 0,
				range_end: u64::MAX,
				content: [9; 32],
				manifest,
				state: ParentIntentState::Intent,
			})
			.await
			.unwrap()
			.unwrap();
	}

	async fn activate(
		index: &PersistentLocalIndex,
		gid: u64,
		predecessor: u64,
		t: u64,
		entries: Vec<BaseEntry>,
	) {
		let staged = candidate(gid, predecessor, &entries);
		index
			.stage_base(staged.clone(), entries)
			.await
			.unwrap()
			.unwrap();
		index
			.activate_base(
				Activation {
					gid,
					target_digest: staged.target_digest,
					predecessor_gid: predecessor,
					chunks: staged.chunks,
					t,
				},
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.unwrap()
			.unwrap();
	}

	/// ADR 0699 S5b: activation records, in its own transaction, positive debt
	/// for exactly the versions it makes obsolete; facts come from one read;
	/// debt retires only from its exact record with no owner, base entry or
	/// copy; a stale debt version owned again is protected.
	#[tokio::test]
	async fn activation_records_exactly_the_obsolete_versions_as_debt() {
		let (_dir, index) = open("debt").await;
		// Generation 1: key 3 at 1 in the base.
		let old = entry(3, 1, 0);
		activate(&index, 1, 0, 0, vec![old]).await;
		assert!(index.debt_page(None, 100).await.unwrap().is_empty());
		// Retained parents: rsv 5 owns key 1 (the next winner) and key 2 (a
		// loser); rsv 7 named key 4, whose owner entry now names another
		// producer.
		let winner = owned(1, 5, 0x11);
		let loser = owned(2, 5, 0x22);
		let never_appended = owned(5, 5, 0x55);
		begin(&index, 5, vec![winner, loser, never_appended]).await;
		let foreign = owned(4, 7, 0x44);
		begin(&index, 7, vec![foreign]).await;
		index
			.set_version_owner_for_test(TENANT, foreign, (99, 0))
			.await
			.unwrap();
		// Generation 2 (T = 8): key 1 at 5 wins, key 3 moves to 6.
		let mut new_winner = entry(1, 5, 0);
		new_winner.row_digest = winner.row_digest;
		activate(&index, 2, 1, 8, vec![new_winner, entry(3, 6, 0)]).await;
		let debt = index.debt_page(None, 100).await.unwrap();
		let listed: Vec<(VersionKey, DebtSource)> = debt
			.iter()
			.map(|(key, record)| (*key, record.source))
			.collect();
		let displaced = VersionKey {
			tenant: TENANT,
			table: old.table,
			key_identity: old.key_identity,
			hlc: old.hlc,
		};
		assert_eq!(
			listed.len(),
			3,
			"only the losers and the displaced entry: {listed:?}"
		);
		assert!(listed.contains(&(
			version_key(&never_appended),
			DebtSource::RetiredIntent { rsv: 5, ordinal: 0 }
		)));
		assert!(listed.contains(&(
			version_key(&loser),
			DebtSource::RetiredIntent { rsv: 5, ordinal: 0 }
		)));
		assert!(listed.contains(&(displaced, DebtSource::DisplacedBase { tombstone: false })));
		assert!(
			debt.iter().all(|(_, record)| record.gid == 2),
			"recorded by generation 2"
		);
		// A retry of the activation invents nothing.
		activate_again(&index, 2).await;
		assert_eq!(index.debt_page(None, 100).await.unwrap(), debt);

		// One coherent read of the facts.
		let facts = index
			.version_facts(vec![
				version_key(&winner).version_ref(),
				version_key(&loser).version_ref(),
			])
			.await
			.unwrap();
		assert_eq!(facts.active_gid(), 2);
		assert!(facts.get(0).debt.is_none(), "the winner is not debt");
		assert!(facts.get(1).debt.is_some() && !facts.get(1).owned);
		assert_eq!(
			facts.get(1).debt.as_ref().map(|(key, _)| key.table),
			Some(loser.table),
			"the debt names the version's table"
		);
		assert_eq!(
			index.version_owner(TENANT, foreign).await.unwrap(),
			Some((99, 0)),
			"another producer's ownership survives the activation"
		);

		// Retirement: only the exact record, never an owned version.
		let (loser_key, loser_debt) = debt
			.iter()
			.find(|(key, _)| *key == version_key(&loser))
			.cloned()
			.unwrap();
		let mut wrong = loser_debt.clone();
		wrong.gid = 3;
		assert!(!index.retire_debt(loser_key, wrong).await.unwrap());
		// The loser's identity owned again by a new parent: protected.
		begin(&index, 20, vec![loser]).await;
		assert!(
			!index
				.retire_debt(loser_key, loser_debt.clone())
				.await
				.unwrap()
		);
		assert!(
			index
				.version_facts(vec![loser_key.version_ref()])
				.await
				.unwrap()
				.get(0)
				.owned
		);
		// The displaced entry: no copy, no owner, base at another HLC.
		let (displaced_key, displaced_debt) = debt
			.iter()
			.find(|(key, _)| *key == displaced)
			.cloned()
			.unwrap();
		assert!(
			index
				.retire_debt(displaced_key, displaced_debt)
				.await
				.unwrap()
		);
		// A retired intent's version that was never appended: no copy, so it
		// retires without any rewrite.
		let (never_key, never_debt) = debt
			.iter()
			.find(|(key, _)| *key == version_key(&never_appended))
			.cloned()
			.unwrap();
		assert!(index.retire_debt(never_key, never_debt).await.unwrap());
		assert_eq!(
			index.debt_page(None, 100).await.unwrap().len(),
			1,
			"only the re-owned loser's debt stays"
		);
	}

	async fn activate_again(index: &PersistentLocalIndex, gid: u64) {
		let meta = index.base_meta().await.unwrap();
		index
			.activate_base(
				Activation {
					gid,
					target_digest: [gid as u8; 32],
					predecessor_gid: gid - 1,
					chunks: meta.active_chunks,
					t: 8,
				},
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.unwrap()
			.unwrap();
	}

	#[test]
	fn chunk_sums_cover_every_chunk_and_depend_on_content() {
		let empty = chunk_sums(std::iter::empty());
		assert_eq!(empty.len(), GENERATION_CHUNKS);
		let one = chunk_sums(&[entry(1, 10, 4)]);
		assert_eq!(one[4].count, 1);
		assert_ne!(one[4].root, empty[4].root);
		assert_eq!(one[5], empty[5], "other chunks are the empty chunk");
		let mut other = entry(1, 10, 4);
		other.tombstone = true;
		assert_ne!(
			chunk_sums(&[other])[4].root,
			one[4].root,
			"the tombstone flag is bound"
		);
	}

	#[tokio::test]
	async fn staging_is_fenced_by_predecessor_and_by_later_targets() {
		let (_dir, index) = open("fence").await;
		let entries = vec![entry(1, 10, 0), entry(2, 11, 1)];
		// Built on a predecessor that is not active: refused.
		assert_eq!(
			index
				.stage_base(candidate(2, 1, &entries), entries.clone())
				.await
				.unwrap(),
			Err(StageRefusal::PredecessorMoved)
		);
		// Target 3 staged first; a late worker for abandoned target 2 is refused.
		index
			.stage_base(candidate(3, 0, &entries), entries.clone())
			.await
			.unwrap()
			.unwrap();
		assert_eq!(
			index
				.stage_base(candidate(2, 0, &entries), entries.clone())
				.await
				.unwrap(),
			Err(StageRefusal::Superseded)
		);
		// The same candidate again is idempotent; a different one conflicts.
		index
			.stage_base(candidate(3, 0, &entries), entries.clone())
			.await
			.unwrap()
			.unwrap();
		let fewer = vec![entry(1, 10, 0)];
		assert_eq!(
			index
				.stage_base(candidate(3, 0, &fewer), fewer)
				.await
				.unwrap(),
			Err(StageRefusal::Conflicting)
		);
		assert_eq!(index.base_meta().await.unwrap().staged.unwrap().gid, 3);
		assert_eq!(index.base_entries(true).await.unwrap().len(), 2);
	}

	#[tokio::test]
	async fn activation_swaps_in_the_matching_candidate_and_retires_erased_intents() {
		let (_dir, index) = open("activate").await;
		// An intent at rsv 5 (erased by T = 8) and one at rsv 9 (retained).
		for (rsv, key) in [(5u64, 1u8), (9, 2)] {
			let version = OwnedVersion {
				table: TableObjectId::new([3; 16]),
				key_identity: [key; 32],
				hlc: HybridTimestamp {
					physical_ms: rsv,
					logical: 0,
					node_id: 1,
				},
				row_digest: [key; 32],
			};
			let intent = ParentIntent {
				tenant: TENANT,
				rsv,
				ordinal: 0,
				claim_digest: [rsv as u8; 32],
				range_start: 0,
				range_end: u64::MAX,
				content: [9; 32],
				manifest: vec![version],
				state: ParentIntentState::Intent,
			};
			index.begin_parent_intent(intent).await.unwrap().unwrap();
		}
		let entries = vec![entry(1, 5, 0)];
		let staged = candidate(1, 0, &entries);
		index
			.stage_base(staged.clone(), entries.clone())
			.await
			.unwrap()
			.unwrap();
		let activation = |chunks: Vec<ChunkSum>| Activation {
			gid: 1,
			target_digest: staged.target_digest,
			predecessor_gid: 0,
			chunks,
			t: 8,
		};
		// A certificate the candidate does not match: refused, nothing changes.
		assert_eq!(
			index
				.activate_base(
					activation(chunk_sums(std::iter::empty())),
					crate::network::data_server::child_writes::ProducerFence::uncontended_for_test(
					)
					.await
				)
				.await
				.unwrap(),
			Err(ActivateRefusal::NoMatchingCandidate)
		);
		assert!(index.parent_intent(5, 0).await.unwrap().is_some());
		// The matching certificate: active is the candidate; rsv 5 retires.
		index
			.activate_base(
				activation(staged.chunks.clone()),
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.unwrap()
			.unwrap();
		let meta = index.base_meta().await.unwrap();
		assert_eq!(meta.active_gid, 1);
		assert!(meta.staged.is_none());
		assert_eq!(index.base_entries(false).await.unwrap(), entries);
		assert!(index.base_entries(true).await.unwrap().is_empty());
		assert!(index.parent_intent(5, 0).await.unwrap().is_none());
		assert!(index.parent_intent(9, 0).await.unwrap().is_some());
		// Idempotent once active.
		index
			.activate_base(
				activation(staged.chunks.clone()),
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.unwrap()
			.unwrap();
	}

	// ---- DC-269: the installed cut ----

	fn cut_version(key: u8) -> OwnedVersion {
		OwnedVersion {
			table: TableObjectId::new([3; 16]),
			key_identity: [key; 32],
			hlc: HybridTimestamp {
				physical_ms: 100 + u64::from(key),
				logical: 0,
				node_id: 1,
			},
			row_digest: [key; 32],
		}
	}

	async fn try_begin(
		index: &PersistentLocalIndex,
		rsv: u64,
		key: u8,
	) -> Result<ParentIntentState, crate::storage::index::parent_intents::IntentRefusal> {
		index
			.begin_parent_intent(ParentIntent {
				tenant: TENANT,
				rsv,
				ordinal: 0,
				claim_digest: [rsv as u8; 32],
				range_start: 0,
				range_end: u64::MAX,
				content: [9; 32],
				manifest: vec![cut_version(key)],
				state: ParentIntentState::Intent,
			})
			.await
			.unwrap()
	}

	/// Refused at and below the cut, with nothing recorded; admitted above it.
	async fn assert_cut(index: &PersistentLocalIndex, t: u64, key: u8, admit: u64) {
		assert!(admit > t);
		use crate::storage::index::parent_intents::IntentRefusal::ErasedByInstalledCut;
		for rsv in [t - 1, t] {
			assert_eq!(
				try_begin(index, rsv, key).await,
				Err(ErasedByInstalledCut),
				"rsv {rsv}"
			);
			assert!(index.parent_intent(rsv, 0).await.unwrap().is_none());
		}
		assert_eq!(
			index.version_owner(TENANT, cut_version(key)).await.unwrap(),
			None
		);
		assert_eq!(
			try_begin(index, admit, key).await,
			Ok(ParentIntentState::Intent)
		);
		assert_eq!(
			index.version_owner(TENANT, cut_version(key)).await.unwrap(),
			Some((admit, 0))
		);
	}

	/// DC-269: an activated generation installs its certificate's cut with the
	/// base; an intent at or below it is refused, above it admitted; the cut
	/// survives a real reopen; re-activating the same generation with another cut
	/// fails closed.
	#[tokio::test]
	async fn an_activated_cut_refuses_erased_intents_across_a_reopen() {
		let (dir, index) = open("installed-cut").await;
		// Genesis has no cut: any reservation may begin.
		assert_eq!(try_begin(&index, 3, 1).await, Ok(ParentIntentState::Intent));
		activate(&index, 1, 0, 10, Vec::new()).await;
		assert!(
			index.parent_intent(3, 0).await.unwrap().is_none(),
			"retired"
		);
		assert_cut(&index, 10, 2, 11).await;
		let error = index
			.activate_base(
				Activation {
					gid: 1,
					target_digest: [1; 32],
					predecessor_gid: 0,
					chunks: chunk_sums(std::iter::empty()),
					t: 9,
				},
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.err()
			.expect("another cut for the installed generation fails closed");
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		drop(index);
		let index = PersistentLocalIndex::open(dir.join("index.redb"))
			.await
			.unwrap();
		assert_cut(&index, 10, 3, 12).await;
		// The next generation raises the cut.
		activate(&index, 2, 1, 20, Vec::new()).await;
		assert_cut(&index, 20, 4, 21).await;
	}

	/// DC-269: a recovered base is published with the trusted certificate's cut,
	/// atomically; the repeat with the same cut is done, with another fails
	/// closed; the cut survives a real reopen.
	#[tokio::test]
	async fn a_recovered_base_publishes_its_cut_across_a_reopen() {
		let (dir, index) = open("recovered-cut").await;
		let certificate = candidate(4, 3, &[]);
		assert_eq!(
			index
				.stage_recovered_page(certificate.clone(), Vec::new())
				.await
				.unwrap(),
			Ok(())
		);
		assert_eq!(
			index
				.publish_recovered(certificate.clone(), 7)
				.await
				.unwrap(),
			Ok(())
		);
		assert_eq!(index.base_meta().await.unwrap().active_gid, 4);
		assert_cut(&index, 7, 1, 8).await;
		assert_eq!(
			index
				.publish_recovered(certificate.clone(), 7)
				.await
				.unwrap(),
			Ok(())
		);
		let error = index.publish_recovered(certificate, 6).await.err().unwrap();
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		drop(index);
		let index = PersistentLocalIndex::open(dir.join("index.redb"))
			.await
			.unwrap();
		assert_cut(&index, 7, 2, 9).await;
	}

	/// DC-269: the compatibility boundary. A base installed by an earlier build
	/// has no cut: the store opens (its clean record and data are untouched), no
	/// intent is admitted until the trusted certificate of the installed
	/// generation and chunks confirms the cut, which is then written; other
	/// chunks or another generation write nothing; a cut other than the
	/// certificate's fails closed. A stale cut (an earlier generation's, left by an
	/// earlier build's activation) is unconfirmed the same way.
	#[tokio::test]
	async fn an_unconfirmed_installed_cut_admits_nothing_until_its_certificate_confirms_it() {
		use crate::storage::index::parent_intents::IntentRefusal::{
			ErasedByInstalledCut, InstalledCutUnconfirmed,
		};
		let (dir, mut index) = open("cut-upgrade").await;
		activate(&index, 1, 0, 5, Vec::new()).await;
		activate(&index, 2, 1, 9, Vec::new()).await;
		let chunks = index.base_meta().await.unwrap().active_chunks;
		for (earlier, left) in [
			("missing", None),
			("stale", Some(InstalledCut { gid: 1, t: 5 })),
		] {
			index.set_installed_cut_for_test(left).await.unwrap();
			drop(index);
			// The earlier build's store opens.
			let reopened = PersistentLocalIndex::open(dir.join("index.redb"))
				.await
				.unwrap();
			assert_eq!(
				reopened.installed_cut().await.unwrap(),
				CutState::Unconfirmed,
				"{earlier}"
			);
			for rsv in [3, 9, 50] {
				assert_eq!(
					try_begin(&reopened, rsv, 7).await,
					Err(InstalledCutUnconfirmed),
					"{earlier}: rsv {rsv}"
				);
				assert!(reopened.parent_intent(rsv, 0).await.unwrap().is_none());
			}
			assert_eq!(
				reopened
					.version_owner(TENANT, cut_version(7))
					.await
					.unwrap(),
				None
			);
			// Another generation or other chunks write nothing.
			assert_eq!(
				reopened
					.confirm_installed_cut(1, chunks.clone(), 5)
					.await
					.unwrap(),
				CutConfirmation::NotInstalled
			);
			assert_eq!(
				reopened
					.confirm_installed_cut(2, chunk_sums(&[entry(1, 1, 0)]), 9)
					.await
					.unwrap(),
				CutConfirmation::OtherBase
			);
			assert_eq!(
				reopened.installed_cut().await.unwrap(),
				CutState::Unconfirmed
			);
			// The trusted certificate of the installed generation writes it.
			assert_eq!(
				reopened
					.confirm_installed_cut(2, chunks.clone(), 9)
					.await
					.unwrap(),
				CutConfirmation::Upgraded,
				"{earlier}"
			);
			assert_eq!(
				reopened.installed_cut().await.unwrap(),
				CutState::Current(InstalledCut { gid: 2, t: 9 })
			);
			assert_eq!(try_begin(&reopened, 9, 7).await, Err(ErasedByInstalledCut));
			assert_eq!(
				reopened
					.confirm_installed_cut(2, chunks.clone(), 9)
					.await
					.unwrap(),
				CutConfirmation::Confirmed
			);
			// The same generation and chunks with another t fail closed.
			let error = reopened
				.confirm_installed_cut(2, chunks.clone(), 8)
				.await
				.err()
				.unwrap();
			assert_eq!(error.kind(), ErrorKind::InvalidData, "{earlier}");
			index = reopened;
		}
		// A cut for a later generation than the installed one is damage: every
		// read of the base and every begin fail closed.
		index
			.set_installed_cut_for_test(Some(InstalledCut { gid: 3, t: 12 }))
			.await
			.unwrap();
		assert_eq!(
			index.base_meta().await.err().unwrap().kind(),
			ErrorKind::InvalidData
		);
		assert_eq!(
			index
				.begin_parent_intent(ParentIntent {
					tenant: TENANT,
					rsv: 40,
					ordinal: 0,
					claim_digest: [40; 32],
					range_start: 0,
					range_end: u64::MAX,
					content: [9; 32],
					manifest: vec![cut_version(8)],
					state: ParentIntentState::Intent,
				})
				.await
				.err()
				.unwrap()
				.kind(),
			ErrorKind::InvalidData
		);
		// Genesis has no cut and admits.
		let (_genesis_dir, genesis) = open("cut-genesis").await;
		assert_eq!(genesis.installed_cut().await.unwrap(), CutState::Genesis);
		assert_eq!(
			try_begin(&genesis, 1, 1).await,
			Ok(ParentIntentState::Intent)
		);
	}

	/// DC-269 r2: the same generation with other chunks is not the certificate's
	/// base: the repeat activation fails closed, confirmation reports it, and
	/// neither changes a row or backfills the cut.
	#[tokio::test]
	async fn the_same_generation_with_other_chunks_fails_closed_and_writes_nothing() {
		let (_dir, index) = open("cut-other-chunks").await;
		let entries = vec![entry(1, 5, 0)];
		activate(&index, 1, 0, 6, entries.clone()).await;
		index.set_installed_cut_for_test(None).await.unwrap();
		let before = index.base_entries(false).await.unwrap();
		let other = chunk_sums(&[entry(2, 5, 0)]);
		let error = index
			.activate_base(
				Activation {
					gid: 1,
					target_digest: [1; 32],
					predecessor_gid: 0,
					chunks: other.clone(),
					t: 6,
				},
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.err()
			.expect("other chunks fail closed");
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		assert_eq!(
			index.confirm_installed_cut(1, other, 6).await.unwrap(),
			CutConfirmation::OtherBase
		);
		assert_eq!(
			index.base_entries(false).await.unwrap(),
			before,
			"rows unchanged"
		);
		assert_eq!(
			index.base_meta().await.unwrap().active_chunks,
			chunk_sums(&entries)
		);
		assert_eq!(
			index.installed_cut().await.unwrap(),
			CutState::Unconfirmed,
			"no backfill"
		);
		// The repeat with the certificate's own chunks writes its cut.
		index
			.activate_base(
				Activation {
					gid: 1,
					target_digest: [1; 32],
					predecessor_gid: 0,
					chunks: chunk_sums(&entries),
					t: 6,
				},
				crate::network::data_server::child_writes::ProducerFence::uncontended_for_test()
					.await,
			)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(
			index.installed_cut().await.unwrap(),
			CutState::Current(InstalledCut { gid: 1, t: 6 })
		);
	}

	/// DC-272: an intent begun after the fence's read, on a stripe the fence does
	/// not hold, refuses the activation with nothing changed (base, staging,
	/// intents, owners); a fresh read and fence then activate and retire both.
	#[tokio::test]
	async fn an_intent_begun_after_the_fence_read_refuses_activation_until_refenced() {
		use crate::network::data_server::child_writes::ParentLocks;
		let (_dir, index) = open("producer-fence").await;
		let locks = ParentLocks::new();
		let stripes = locks.stripe_count();
		assert_ne!(3 % stripes, 4 % stripes, "premise: distinct stripes");
		begin(&index, 3, vec![cut_version(30)]).await;
		let staged = candidate(1, 0, &[]);
		index
			.stage_base(staged.clone(), Vec::new())
			.await
			.unwrap()
			.unwrap();
		let activation = Activation {
			gid: 1,
			target_digest: staged.target_digest,
			predecessor_gid: 0,
			chunks: staged.chunks.clone(),
			t: 10,
		};
		let wanted = index.intent_stripes_through(10, stripes).await.unwrap();
		assert_eq!(wanted.iter().filter(|stripe| **stripe).count(), 1);
		// Begun after the read, at or below t, on another stripe.
		begin(&index, 4, vec![cut_version(40)]).await;
		let fence = locks.fence(&wanted).await;
		assert_eq!(
			index
				.activate_base(activation.clone(), fence)
				.await
				.unwrap(),
			Err(ActivateRefusal::ProducerUnfenced)
		);
		let meta = index.base_meta().await.unwrap();
		assert_eq!(
			(meta.active_gid, meta.staged.as_ref()),
			(0, Some(&staged)),
			"unchanged"
		);
		for (rsv, key) in [(3, 30), (4, 40)] {
			assert!(
				index.parent_intent(rsv, 0).await.unwrap().is_some(),
				"intent {rsv} kept"
			);
			assert_eq!(
				index.version_owner(TENANT, cut_version(key)).await.unwrap(),
				Some((rsv, 0))
			);
		}
		assert_eq!(index.installed_cut().await.unwrap(), CutState::Genesis);
		// A fresh read and fence cover both.
		let wanted = index.intent_stripes_through(10, stripes).await.unwrap();
		assert_eq!(wanted.iter().filter(|stripe| **stripe).count(), 2);
		let fence = locks.fence(&wanted).await;
		index
			.activate_base(activation, fence)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(index.base_meta().await.unwrap().active_gid, 1);
		for rsv in [3, 4] {
			assert!(
				index.parent_intent(rsv, 0).await.unwrap().is_none(),
				"intent {rsv} retired"
			);
		}
	}
}
