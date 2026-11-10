//! ADR 0704: certified cleanup of this leader's own active tiered slabs.
//!
//! A certified tier job's operation is allocated only in ParentDecisions mode
//! and only on a store whose op intents and tier certificates agreed at open.
//! Any disagreement found at open fences certified tiered cleanup for the life
//! of this manager: the store opens and reads are unaffected, nothing is cleared
//! automatically, and every later certified allocation is refused, typed.
//!
//! One step takes one job: it picks one of this leader's tiered slabs whose
//! local pointer is the committed one, claims it (the slab-rewrite exclusive
//! claim) and reads its published object, decides once (0703's rule, through
//! the data server), rewrites with exactly the selection reclaimed, and
//! proposes one certified operation: a `Tier` replacing the source with the
//! survivors, or a `Retire` when none survive. Old objects are retained.

use super::*;
use crate::consensus::slab_ops::{OpKey, TierGeneration};
use crate::storage::index::certified_base::VersionRef;
use crate::storage::index::{
	CertifiedTierAllocation, CertifiedTierOrphan, CertifiedTierWork, TierSelectionEvidence,
};
use crate::storage::manager::certified_lsm::{
	CertifiedSelection, DropSummary, FrameTable, canonical_drop, frame_tables_of,
};

/// Why certified tiered cleanup is fenced on this store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedTierFence {
	/// Op intents and tier certificates disagreed at open.
	Orphans(Vec<CertifiedTierOrphan>),
	/// Open could not read them: no durable index, or the read failed.
	Unverified,
}

/// A certified tiered allocation this store refuses before writing anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedTierRefusal {
	/// The store is not in ParentDecisions mode.
	NotInMode,
	/// Certified tiered cleanup is fenced on this store.
	Fenced(CertifiedTierFence),
	/// This node may not propose: it does not lead.
	NotLeader,
	/// No canonical tier state is bound: an isolated store never cleans certified.
	NoCanonicalSource,
	/// The object store could not serve the claimed slab's published object.
	ObjectStoreUnavailable,
	/// The records a rewrite would reclaim are not exactly the selection's
	/// occurrences among the slab's durable rows.
	ReclaimedNotSelection,
}

impl std::fmt::Display for CertifiedTierRefusal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NotInMode => {
				f.write_str("certified tiered cleanup runs only in ParentDecisions mode")
			}
			Self::Fenced(fence) => write!(f, "certified tiered cleanup is fenced: {fence:?}"),
			Self::NotLeader => f.write_str("certified tiered cleanup runs only on the leader"),
			Self::NoCanonicalSource => {
				f.write_str("certified tiered cleanup needs the canonical tier state")
			}
			Self::ObjectStoreUnavailable => {
				f.write_str("the object store cannot serve the claimed tiered slab")
			}
			Self::ReclaimedNotSelection => {
				f.write_str("the reclaimed records are not exactly the certified selection")
			}
		}
	}
}

impl std::error::Error for CertifiedTierRefusal {}

/// The refusal an allocation or step error carries, if it is one.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn certified_tier_refusal(error: &Error) -> Option<&CertifiedTierRefusal> {
	error.get_ref()?.downcast_ref::<CertifiedTierRefusal>()
}

/// What open finds: `None` only when every intent and certificate agree.
pub(super) async fn certified_tier_fence_at_open(
	index: Option<&PersistentLocalIndex>,
) -> Option<CertifiedTierFence> {
	let Some(index) = index else {
		return Some(CertifiedTierFence::Unverified);
	};
	match index.certified_tier_orphans().await {
		Ok(orphans) if orphans.is_empty() => None,
		Ok(orphans) => {
			tracing::error!(
				?orphans,
				"op intents and tier certificates disagree; certified tiered cleanup stays fenced"
			);
			Some(CertifiedTierFence::Orphans(orphans))
		}
		Err(error) => {
			tracing::error!(
				%error,
				"tier certificates are unreadable; certified tiered cleanup stays fenced"
			);
			Some(CertifiedTierFence::Unverified)
		}
	}
}

impl SlabManager {
	/// Why certified tiered cleanup is fenced on this store, if it is.
	pub(crate) fn certified_tier_cleanup_fenced(&self) -> Option<&CertifiedTierFence> {
		self.certified_tier_fence.as_ref()
	}

	/// A certified tier job's one operation and its certificate, in one
	/// transaction under the selection's generation. Refused, typed, outside
	/// ParentDecisions mode or on a fenced store; a moved generation writes nothing.
	pub(crate) async fn allocate_certified_tier_op(
		&self,
		work: CertifiedTierWork,
		evidence: TierSelectionEvidence,
	) -> std::io::Result<CertifiedTierAllocation> {
		if !self.versions_contained() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				CertifiedTierRefusal::NotInMode,
			));
		}
		if let Some(fence) = self.certified_tier_cleanup_fenced() {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				CertifiedTierRefusal::Fenced(fence.clone()),
			));
		}
		self.require_op_index()?
			.allocate_certified_tier_op(work, evidence)
			.await
	}
}

/// Where a test ends a certified tier step, as a crash would: nothing after it
/// runs, not even the step's own cleanup.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedTierCut {
	/// A rewrite's candidate is uploaded and read back; nothing is allocated.
	AfterUpload,
	/// The intent and certificate are durable; nothing is submitted.
	BeforeSubmit,
	/// The ring decided it Applied; this store has not delivered it locally.
	AfterAcceptance,
}

#[cfg(test)]
#[derive(Debug)]
struct CertifiedTierCrash(CertifiedTierCut);

#[cfg(test)]
impl std::fmt::Display for CertifiedTierCrash {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "certified tier step cut at {:?}", self.0)
	}
}

#[cfg(test)]
impl std::error::Error for CertifiedTierCrash {}

#[cfg(test)]
impl SlabManager {
	/// Test-only: ends the next certified tier step at `cut`.
	pub(crate) fn cut_next_certified_tier_step(&self, cut: CertifiedTierCut) {
		*self
			.continuity_test_holds
			.certified_tier_cut
			.lock()
			.unwrap() = Some(cut);
	}

	fn certified_tier_cut(&self, at: CertifiedTierCut) -> std::io::Result<()> {
		let mut cut = self
			.continuity_test_holds
			.certified_tier_cut
			.lock()
			.unwrap();
		if *cut == Some(at) {
			*cut = None;
			return Err(Error::other(CertifiedTierCrash(at)));
		}
		Ok(())
	}
}

/// ADR 0704 §5: the historical objects the certified path replaced (the sources
/// of its applied rewrites and retirements). They are retained, never indexed
/// again and never deleted here; reported for tests only, with no capacity claim.
#[cfg(test)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CertifiedRetainedObjects {
	pub count: usize,
	pub bytes: u64,
	pub uris: Vec<String>,
}

#[cfg(test)]
impl SlabManager {
	fn note_certified_retained_object(&self, uri: &str, bytes: u64) {
		self.continuity_test_holds
			.certified_retained
			.lock()
			.unwrap()
			.push((uri.to_owned(), bytes));
	}

	/// Test-only: what this manager's certified path retained since it opened.
	pub(crate) fn certified_retained_objects_for_test(&self) -> CertifiedRetainedObjects {
		let retained = self
			.continuity_test_holds
			.certified_retained
			.lock()
			.unwrap();
		CertifiedRetainedObjects {
			count: retained.len(),
			bytes: retained.iter().map(|(_, bytes)| bytes).sum(),
			uris: retained.iter().map(|(uri, _)| uri.clone()).collect(),
		}
	}
}

#[cfg(test)]
fn is_certified_tier_crash(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<CertifiedTierCrash>())
}

fn refused(refusal: CertifiedTierRefusal) -> Error {
	let kind = match refusal {
		CertifiedTierRefusal::ObjectStoreUnavailable | CertifiedTierRefusal::NotLeader => {
			ErrorKind::WouldBlock
		}
		CertifiedTierRefusal::ReclaimedNotSelection => ErrorKind::InvalidData,
		_ => ErrorKind::PermissionDenied,
	};
	Error::new(kind, refusal)
}

/// What one certified tier step did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CertifiedTierStep {
	/// No debt anywhere, or no eligible slab.
	Idle,
	/// The selection names nothing: nothing written, uploaded or proposed.
	NothingSelected(SlabId),
	/// The active generation moved before allocation: nothing allocated. A
	/// rewrite's uploaded candidate is retained, never referenced.
	GenerationMoved {
		slab: SlabId,
		candidate: Option<String>,
	},
	/// A certified rewrite, decided Applied.
	Rewritten {
		slab: SlabId,
		op: OpKey,
		candidate: String,
	},
	/// A certified retirement, decided Applied.
	Retired { slab: SlabId, op: OpKey },
}

/// Decides a claimed job's selection: the data server, which holds the table
/// descriptors that content evidence needs.
#[async_trait::async_trait]
pub(crate) trait CertifiedTierDecider: Send + Sync {
	async fn decide(&self, job: &TierCertifiedJob) -> std::io::Result<CertifiedSelection>;
}

/// One claimed tiered slab awaiting its certified selection. The slab-rewrite
/// exclusive claim is held until the job is finished or dropped.
pub(crate) struct TierCertifiedJob {
	slab: crate::consensus::SlabRef,
	source: TieredSlabPointer,
	source_generation: TierGeneration,
	source_bytes: u64,
	/// The slab's durable rows when claimed, and their digest.
	source_rows: Vec<PersistentRecordRow>,
	source_rows_digest: [u8; 32],
	fingerprint: [u8; 32],
	plaintext: Zeroizing<Vec<u8>>,
	/// The published object's validated layout: `plaintext` is its bytes.
	plaintext_layout: crate::storage::tier_object_layout::TierObjectLayout,
	/// One per indexed record of the published object, sorted by offset.
	records: Vec<CompactionRecord>,
	/// Each record's version entry and what its frame says of its table.
	entries: Vec<crate::storage::VersionEntry>,
	frame_tables: Vec<FrameTable>,
	_claim: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl TierCertifiedJob {
	/// The distinct versions of the slab's records, sorted: what the coherent
	/// facts must be read for.
	pub(crate) fn versions(&self) -> Vec<VersionRef> {
		let mut versions: Vec<VersionRef> = self
			.entries
			.iter()
			.map(|entry| VersionRef {
				tenant: entry.tenant_id,
				key_identity: *entry.row_key_id.as_bytes(),
				hlc: entry.row_hlc,
			})
			.collect();
		versions.sort_unstable();
		versions.dedup();
		versions
	}

	pub(crate) fn slab_id(&self) -> SlabId {
		self.slab.local_id()
	}

	pub(super) fn entries(&self) -> &[crate::storage::VersionEntry] {
		&self.entries
	}

	pub(super) fn frame_tables(&self) -> &[FrameTable] {
		&self.frame_tables
	}

	pub(super) fn fingerprint(&self) -> [u8; 32] {
		self.fingerprint
	}

	pub(super) fn source_rows_digest(&self) -> [u8; 32] {
		self.source_rows_digest
	}
}

/// The job a selection is decided for: the owner, incarnation and slab, the
/// source generation, and the slab's durable rows digest.
fn tier_job_fingerprint(
	slab: crate::consensus::SlabRef,
	source: &TierGeneration,
	source_rows_digest: &[u8; 32],
) -> std::io::Result<[u8; 32]> {
	let mut hasher = blake3::Hasher::new();
	hasher.update(b"tier-certified-job-v1");
	hasher.update(&postcard::to_stdvec(&slab).map_err(Error::other)?);
	hasher.update(&postcard::to_stdvec(source).map_err(Error::other)?);
	hasher.update(source_rows_digest);
	Ok(*hasher.finalize().as_bytes())
}

type MemberKey = crate::storage::manager::certified_lsm::CertifiedMemberKey;

fn record_member(record: &CompactionRecord) -> MemberKey {
	(
		record.tenant_id,
		record.key_identity,
		record.old_location.hlc,
		record.old_location.is_tombstone,
	)
}

fn row_member(row: &PersistentRecordRow) -> MemberKey {
	(
		row.tenant_id,
		row.key_identity,
		row.location.hlc,
		row.location.is_tombstone,
	)
}

/// The certified drop of a rewrite: the reclaimed records must be exactly the
/// selection's occurrences among the slab's durable rows (the same multiset,
/// member by member), and a dropped row's durable attribution, if any, its
/// member's table. Anything else refuses, typed.
pub(super) fn certified_tier_drop(
	records: &[CompactionRecord],
	source_rows: &[PersistentRecordRow],
	selection: &CertifiedSelection,
) -> std::io::Result<DropSummary> {
	let dropped: Vec<PersistentRecordRow> = source_rows
		.iter()
		.filter(|row| selection.member_table_of_row(row).is_some())
		.copied()
		.collect();
	if dropped.iter().any(|row| {
		row.table_object_id
			.is_some_and(|table| Some(table) != selection.member_table_of_row(row))
	}) {
		return Err(refused(CertifiedTierRefusal::ReclaimedNotSelection));
	}
	let mut durable: BTreeMap<MemberKey, u64> = BTreeMap::new();
	for row in &dropped {
		*durable.entry(row_member(row)).or_default() += 1;
	}
	let mut reclaimed: BTreeMap<MemberKey, u64> = BTreeMap::new();
	for record in records.iter().filter(|record| record.reclaim) {
		*reclaimed.entry(record_member(record)).or_default() += 1;
	}
	if durable != reclaimed {
		return Err(refused(CertifiedTierRefusal::ReclaimedNotSelection));
	}
	canonical_drop(&dropped, |row| selection.member_table_of_row(row))
}

impl SlabManager {
	/// ADR 0704: one certified tier job, if one is admissible: pick, claim,
	/// decide once, rewrite, propose. Refused, typed, outside ParentDecisions
	/// mode, on a fenced store, off the leader, and with no canonical state.
	pub(crate) async fn run_certified_tier_step(
		&self,
		committer: &dyn TierCommitter,
		decider: &dyn CertifiedTierDecider,
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<CertifiedTierStep> {
		if !self.versions_contained() {
			return Err(refused(CertifiedTierRefusal::NotInMode));
		}
		if let Some(fence) = self.certified_tier_cleanup_fenced() {
			return Err(refused(CertifiedTierRefusal::Fenced(fence.clone())));
		}
		if !committer.should_propose() {
			return Err(refused(CertifiedTierRefusal::NotLeader));
		}
		if self.canonical_tier_source.get().is_none() {
			return Err(refused(CertifiedTierRefusal::NoCanonicalSource));
		}
		let _permit = crate::governance::authorize_autonomous_action(
			crate::governance::AutonomousActionSurface::L2RetentionRewrite,
		);
		let index = self.require_op_index()?;
		// No debt at all: no version anywhere can be selected.
		if index.debt_page(None, 1).await?.is_empty() {
			return Ok(CertifiedTierStep::Idle);
		}
		let Some(slab_id) = self.next_certified_tier_slab().await? else {
			return Ok(CertifiedTierStep::Idle);
		};
		let Some(job) = self.claim_tiered_for_certified_cleanup(slab_id).await? else {
			return Ok(CertifiedTierStep::Idle);
		};
		let selection = decider.decide(&job).await?;
		self.finish_certified_tier(job, selection, committer, table_schemas)
			.await
	}

	/// The slab's published pointer when a certified job may take it: owned by
	/// this store's current incarnation, its committed pointer present and the
	/// local one the same generation, and nothing pending, deferred, unverified,
	/// retired or unresolved for it. Read under one canonical lease.
	async fn certified_tier_source(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<Option<TieredSlabPointer>> {
		let (current, committed) = self.committed_certified_tier(slab_id).await?;
		let Some(committed) = committed.filter(|_| current) else {
			return Ok(None);
		};
		let Some(local) = self.tier_pointer(slab_id).await else {
			return Ok(None);
		};
		if TierGeneration::from(&committed) != TierGeneration::from(&local)
			|| self.tier_authority.pending.contains_key(&slab_id)
			|| self.unapplied_tier_pointers.contains_key(&slab_id)
			|| self.unverified_tier_slabs.contains(&slab_id)
			|| self.retired_tier_slabs.contains(&slab_id)
			|| self.unresolved_tier_cleanup.contains(slab_id)
		{
			return Ok(None);
		}
		Ok(Some(local))
	}

	/// The committed tier record of this store's `slab_id`, and whether this
	/// store is its node's current incarnation: one read of one canonical lease,
	/// held for nothing else (no object I/O, no lock).
	async fn committed_certified_tier(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<(bool, Option<TieredSlab>)> {
		let owner = self.owner_ref()?;
		let Some(lease) = self.canonical_tier_lease().await? else {
			return Err(refused(CertifiedTierRefusal::NoCanonicalSource));
		};
		Ok((
			lease.owner_is_current(owner),
			lease.committed_tier(owner.slab(slab_id)),
		))
	}

	/// The next eligible tiered slab after the last one visited, wrapping, in
	/// slab order.
	async fn next_certified_tier_slab(&self) -> std::io::Result<Option<SlabId>> {
		let mut slabs: Vec<SlabId> = self
			.list_tiered_slabs()
			.await
			.into_iter()
			.map(|pointer| pointer.slab_id)
			.collect();
		slabs.sort_unstable();
		slabs.dedup();
		let last = self.certified_tier_cursor.load(Ordering::Acquire);
		let (after, before): (Vec<SlabId>, Vec<SlabId>) =
			slabs.into_iter().partition(|slab| slab.get() > last);
		for slab_id in after.into_iter().chain(before) {
			if self.certified_tier_source(slab_id).await?.is_some() {
				self.certified_tier_cursor
					.store(slab_id.get(), Ordering::Release);
				return Ok(Some(slab_id));
			}
		}
		Ok(None)
	}

	/// Claims one tiered slab (the slab-rewrite exclusive claim), records its
	/// source generation and durable rows digest, and reads and validates its
	/// published object into records exactly as the retention rewrite does,
	/// each with its frame's table stamp and durable attribution. `None` when
	/// it is no longer eligible. An object store that cannot serve the object
	/// refuses, typed.
	pub(crate) async fn claim_tiered_for_certified_cleanup(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<Option<TierCertifiedJob>> {
		self.require_tier_authority_ready()?;
		let claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
		let Some(source) = self.certified_tier_source(slab_id).await? else {
			return Ok(None);
		};
		let slab = self.owner_ref()?.slab(slab_id);
		let source_generation = TierGeneration::from(&source);
		let source_rows: Vec<PersistentRecordRow> = self
			.persistent_record_rows_snapshot()
			.into_iter()
			.filter(|row| row.location.slab_id == slab_id)
			.collect();
		let source_rows_digest =
			crate::storage::index::lsm_rows_digest(source_rows.iter().copied());
		let Some(l2) = self.config.l2.clone() else {
			return Err(refused(CertifiedTierRefusal::ObjectStoreUnavailable));
		};
		let old_blob = match l2.get(&source.l2_uri).await {
			Ok(blob) => blob,
			Err(error) => {
				tracing::warn!(
					slab_id = slab_id.get(),
					l2_uri = %source.l2_uri,
					%error,
					"certified tier claim: the object store cannot serve the published object"
				);
				return Err(refused(CertifiedTierRefusal::ObjectStoreUnavailable));
			}
		};
		validate_tiered_blob(&source, &old_blob)?;
		let acknowledged = self.acknowledged_offsets_in_slab(slab_id);
		let decoded = decode_tiered_blob(
			self.config.crypto.as_ref(),
			&source,
			&old_blob,
			TierObjectEvidence::Indexed(&acknowledged),
		)?;
		let source_layout = decoded.layout;
		let plaintext_bytes = Zeroizing::new(decoded.bytes);
		let plaintext =
			crate::storage::tier_object_layout::SlabView::new(&plaintext_bytes, source_layout)?;
		let mut records = recover_slab_entries_from_blob(plaintext, &acknowledged)?
			.into_iter()
			.map(|entry| CompactionRecord {
				tenant_id: entry.tenant_id,
				key_identity: entry.key_identity,
				routing_locality: self.recovered_entry_routing_locality(&entry),
				old_location: entry.location,
				reclaim: false,
				retention_ambiguous: false,
				retention_lag_seconds: 0,
			})
			.collect::<Vec<_>>();
		records.sort_by_key(|record| record.old_location.offset);
		// One read and one decode per physical frame: its records share it.
		let mut frame_tables = vec![FrameTable::Untyped; records.len()];
		for indices in physical_compaction_group_indices(&records).values() {
			let Some(first) = indices.first().copied() else {
				continue;
			};
			let frame = Zeroizing::new(extract_record_from_blob(
				plaintext,
				crate::storage::core::FrameLocation::of_point_read(records[first].old_location),
			)?);
			let rows: Vec<RecordRowIndex> = indices
				.iter()
				.map(|index| records[*index].old_location.record_row_index)
				.collect();
			for (index, table) in indices.iter().zip(frame_tables_of(&frame, &rows)) {
				frame_tables[*index] = table;
			}
		}
		let mut entries = Vec::with_capacity(records.len());
		for record in &records {
			entries.push(crate::storage::VersionEntry {
				tenant_id: record.tenant_id,
				table_object_id: self.published_table_object_id(record.old_location)?,
				row_key_id: record.key_identity,
				row_hlc: record.old_location.hlc,
				record_row_index: record.old_location.record_row_index,
				location: record.old_location,
				// The rule reads a lifecycle only to keep migrated-range markers,
				// which no tiered record carries.
				lifecycle: crate::storage::VersionLifecycle::AmbiguousTable,
			});
		}
		let fingerprint = tier_job_fingerprint(slab, &source_generation, &source_rows_digest)?;
		Ok(Some(TierCertifiedJob {
			slab,
			source,
			source_generation,
			source_bytes: old_blob.len() as u64,
			source_rows,
			source_rows_digest,
			fingerprint,
			plaintext: plaintext_bytes,
			plaintext_layout: source_layout,
			records,
			entries,
			frame_tables,
			_claim: claim,
		}))
	}

	/// Rewrites the claimed slab with exactly `selection` reclaimed and
	/// proposes the one certified operation.
	pub(crate) async fn finish_certified_tier(
		&self,
		job: TierCertifiedJob,
		selection: CertifiedSelection,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<CertifiedTierStep> {
		if selection.job_fingerprint() != job.fingerprint
			|| selection.input_rows_digest() != job.source_rows_digest
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"a certified selection applies only to the job it was decided for",
			));
		}
		let slab_id = job.slab_id();
		if selection.is_empty() {
			return Ok(CertifiedTierStep::NothingSelected(slab_id));
		}
		// Reclaimed from the selection only: never retention, table-drop or
		// containment rules. Conflicting versions stay protected.
		let mut records = job.records.clone();
		for (record, entry) in records.iter_mut().zip(&job.entries) {
			record.reclaim = selection.selects(entry);
		}
		self.keep_conflicting_versions(&mut records);
		let drop = certified_tier_drop(&records, &job.source_rows, &selection)?;
		if drop.drop_count == 0 {
			return Ok(CertifiedTierStep::NothingSelected(slab_id));
		}
		let evidence = TierSelectionEvidence {
			job_fingerprint: job.fingerprint,
			active_gid: selection.active_gid(),
			source_generation: job.source_generation.clone(),
			source_rows_digest: job.source_rows_digest,
			drop_digest: drop.drop_digest,
			drop_rows_digest: drop.drop_rows_digest,
			drop_count: drop.drop_count,
		};
		if records.iter().all(|record| record.reclaim) {
			return self
				.propose_certified_retirement(&job, evidence, committer)
				.await;
		}
		self.propose_certified_rewrite(&job, &records, evidence, committer, table_schemas)
			.await
	}

	/// No record survives: one certified `Retire` of the source generation,
	/// through the ordinary retirement's staging and submission.
	async fn propose_certified_retirement(
		&self,
		job: &TierCertifiedJob,
		evidence: TierSelectionEvidence,
		committer: &dyn TierCommitter,
	) -> std::io::Result<CertifiedTierStep> {
		let slab_id = job.slab_id();
		committer.preflight(job.slab.owner()).await?;
		self.require_op_index()?.ensure_op_room(1).await?;
		self.stage_table_drop_l2_cleanup(slab_id, &job.source.l2_uri, Some(job.source_bytes), None)
			.await?;
		let observed_epoch = self.capture_snapshot_epoch();
		let ticket = match self
			.allocate_certified_tier_op(CertifiedTierWork::Retire { slab: job.slab }, evidence)
			.await?
		{
			CertifiedTierAllocation::GenerationMoved => {
				return Ok(CertifiedTierStep::GenerationMoved {
					slab: slab_id,
					candidate: None,
				});
			}
			CertifiedTierAllocation::Allocated(allocation) => allocation.first,
		};
		let op = ticket.key();
		#[cfg(test)]
		self.certified_tier_cut(CertifiedTierCut::BeforeSubmit)?;
		crate::storage::sweeper::submit_tier_retirement(self, committer, &ticket).await?;
		#[cfg(test)]
		self.certified_tier_cut(CertifiedTierCut::AfterAcceptance)?;
		self.retire_tiered_slab_at_snapshot_epoch(slab_id, observed_epoch)
			.await?;
		#[cfg(test)]
		self.note_certified_retained_object(&job.source.l2_uri, job.source_bytes);
		if Self::retain_superseded_l2_object(slab_id, &job.source.l2_uri, "superseded tier object")
			.is_ok()
		{
			self.clear_retired_tiered_cleanup(slab_id);
		}
		Ok(CertifiedTierStep::Retired { slab: slab_id, op })
	}

	/// Survivors: a fresh `certified/` object, uploaded and read back, its
	/// postimage rows digest the delivery's own derivation over the read-back
	/// object, then one certified `Tier` replacing the source.
	async fn propose_certified_rewrite(
		&self,
		job: &TierCertifiedJob,
		records: &[CompactionRecord],
		evidence: TierSelectionEvidence,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<CertifiedTierStep> {
		let slab_id = job.slab_id();
		let owner = job.slab.owner();
		let l2 = self
			.config
			.l2
			.clone()
			.ok_or_else(|| refused(CertifiedTierRefusal::ObjectStoreUnavailable))?;
		let replacement = self
			.build_tiered_replacement(
				&job.source,
				crate::storage::tier_object_layout::SlabView::new(
					&job.plaintext,
					job.plaintext_layout,
				)?,
				records,
				table_schemas,
				job.source_bytes,
			)
			.await?;
		let key = format!(
			"certified/{:08x}-{:032x}/slab_{:016x}_{}.wnd",
			owner.node(),
			owner.incarnation().get(),
			slab_id,
			uuid::Uuid::new_v4().simple()
		);
		let length = replacement.encrypted_bytes.len() as u64;
		let planned_uri = l2.planned_upload_uri(&key)?;
		self.stage_table_drop_l2_cleanup(
			slab_id,
			&job.source.l2_uri,
			Some(job.source_bytes),
			Some((&planned_uri, length)),
		)
		.await?;
		let new_uri = l2.put(&key, &replacement.encrypted_bytes).await?;
		if new_uri != planned_uri {
			self.persist_ambiguous_upload_intent(slab_id, &new_uri, length)
				.await?;
			return Err(Error::new(
				ErrorKind::InvalidData,
				"object store returned a different upload URI",
			));
		}
		let fetched = l2.get(&new_uri).await?;
		if crate::hash::content::digest(&fetched) != replacement.payload_hash {
			// Never referenced, and retained: no physical delete here.
			self.record_tier_candidate_not_submitted(slab_id, &new_uri, length)
				.await?;
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("certified rewrite of slab {slab_id}: the read-back object differs"),
			));
		}
		let footer = self
			.encrypted_tiered_footer_summary(&fetched)?
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!("certified rewrite of slab {slab_id} has no encrypted footer"),
				)
			})?;
		let tier = TieredSlab {
			slab: job.slab,
			l2_uri: new_uri.clone(),
			payload_hash: replacement.payload_hash,
			is_encrypted: true,
			tenant_id: Some(footer.tenant_id),
			tenant_kek_id: Some(footer.tenant_kek_id),
			stats_format_version: footer
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: footer.stats_locator.map(|locator| locator.stats_block_len),
			stats_block_hash128: footer
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: replacement.tombstone_metadata,
			supersession_metadata: replacement.supersession_metadata,
			certified: None,
		};
		// The delivery's own rows derivation, over the object as read back.
		let postimage_rows_digest = {
			let candidate = TieredSlabPointer::from_ring(&tier);
			let plan = self
				.build_tiered_row_refresh_plan(&candidate, Some(&job.source))
				.await?;
			crate::storage::index::lsm_rows_digest(plan.new_rows.iter().copied())
		};
		#[cfg(test)]
		self.certified_tier_cut(CertifiedTierCut::AfterUpload)?;
		self.prepare_tier_candidate_proposal(slab_id, &new_uri)
			.await?;
		let submitted = async {
			committer.preflight(owner).await?;
			let proposal = self
				.reserve_tier_history(tier.clone())
				.await
				.map_err(|refusal| {
					Error::new(
						refusal.kind(),
						crate::storage::sweeper::TierCommitNotSubmitted(format!(
							"tier history not reserved: {refusal}"
						)),
					)
				})?;
			match self
				.allocate_certified_tier_op(
					CertifiedTierWork::Rewrite {
						tier: tier.clone(),
						postimage_rows_digest,
					},
					evidence,
				)
				.await?
			{
				// The reservation is released with its proposal.
				CertifiedTierAllocation::GenerationMoved => Ok(None),
				CertifiedTierAllocation::Allocated(allocation) => {
					let op = allocation.first.key();
					#[cfg(test)]
					self.certified_tier_cut(CertifiedTierCut::BeforeSubmit)?;
					crate::storage::sweeper::submit_tier_proposal(
						self,
						committer,
						proposal,
						allocation.first,
					)
					.await?;
					Ok(Some(op))
				}
			}
		}
		.await;
		let op = match submitted {
			Ok(Some(op)) => op,
			Ok(None) => {
				self.record_tier_candidate_not_submitted(slab_id, &new_uri, length)
					.await?;
				return Ok(CertifiedTierStep::GenerationMoved {
					slab: slab_id,
					candidate: Some(new_uri),
				});
			}
			Err(error) => {
				#[cfg(test)]
				if is_certified_tier_crash(&error) {
					return Err(error);
				}
				self.retain_failed_certified_candidate(slab_id, &new_uri, length, &error)
					.await?;
				return Err(error);
			}
		};
		#[cfg(test)]
		self.certified_tier_cut(CertifiedTierCut::AfterAcceptance)?;
		// Applied: the existing delivery path, unchanged.
		let observed_epoch = self.capture_snapshot_epoch();
		let (_, committed) = self.committed_certified_tier(slab_id).await?;
		if let Some(pointer) = committed.map(|tier| TieredSlabPointer::from_ring(&tier)) {
			self.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
				.await?;
		}
		self.record_cleanup_replacement_published(slab_id, &job.source.l2_uri, &new_uri)
			.await?;
		#[cfg(test)]
		self.note_certified_retained_object(&job.source.l2_uri, job.source_bytes);
		if Self::retain_superseded_l2_object(slab_id, &job.source.l2_uri, "superseded tier object")
			.is_ok()
		{
			self.clear_retired_tiered_cleanup(slab_id);
		}
		Ok(CertifiedTierStep::Rewritten {
			slab: slab_id,
			op,
			candidate: new_uri,
		})
	}

	/// A candidate whose proposal failed after upload, as the ordinary rewrite
	/// keeps one: preserved if committed, recorded not submitted on a definite
	/// refusal, otherwise an ambiguous upload. Never deleted here.
	async fn retain_failed_certified_candidate(
		&self,
		slab_id: SlabId,
		uri: &str,
		bytes: u64,
		error: &Error,
	) -> std::io::Result<()> {
		let (_, committed) = self.committed_certified_tier(slab_id).await?;
		if committed.is_some_and(|tier| tier.l2_uri == uri) {
			return Ok(());
		}
		if crate::storage::sweeper::tier_proposal_not_submitted(error) {
			return self
				.record_tier_candidate_not_submitted(slab_id, uri, bytes)
				.await;
		}
		self.persist_ambiguous_upload_intent(slab_id, uri, bytes)
			.await?;
		let _ = Self::retain_superseded_l2_object(slab_id, uri, "commit result ambiguous");
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::manager::certified_lsm::CertifiedSelection;

	fn location(offset: u64, hlc: u64) -> RecordLocation {
		RecordLocation {
			slab_id: SlabId::new(7),
			offset,
			length: 10,
			hlc: HybridTimestamp {
				physical_ms: hlc,
				logical: 0,
				node_id: 0,
			},
			record_row_index: RecordRowIndex::default(),
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		}
	}

	fn record(offset: u64, hlc: u64, reclaim: bool) -> CompactionRecord {
		CompactionRecord {
			tenant_id: TenantId::ZERO,
			key_identity: Identity::new([offset as u8; 32]),
			routing_locality: Locality::new(0),
			old_location: location(offset, hlc),
			reclaim,
			retention_ambiguous: false,
			retention_lag_seconds: 0,
		}
	}

	fn row(record: &CompactionRecord) -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id: record.tenant_id,
			key_identity: record.key_identity,
			routing_locality: record.routing_locality,
			table_object_id: None,
			placement: PlacementSemantics::RawLocality,
			partition: None,
			location: record.old_location,
		}
	}

	/// ADR 0704 §1: the reclaimed records must be exactly the selection's
	/// occurrence multiset among the slab's durable rows; any difference
	/// (a selected copy kept, an extra durable copy, a copy reclaimed outside
	/// the selection, a dropped row attributed to another table) refuses.
	#[test]
	fn a_reclaimed_set_that_differs_from_the_selection_refuses() {
		let table = TableObjectId::new([9; 16]);
		let (selected, kept) = (record(1, 10, true), record(2, 20, false));
		let selection =
			CertifiedSelection::for_test([(record_member(&selected), table)].into_iter().collect());
		let rows = vec![row(&selected), row(&kept)];
		let exact = certified_tier_drop(&[selected, kept], &rows, &selection).unwrap();
		assert_eq!(exact.drop_count, 1, "premise: the exact set is the drop");
		let is_refusal = |result: std::io::Result<DropSummary>| {
			result.is_err_and(|error| {
				certified_tier_refusal(&error) == Some(&CertifiedTierRefusal::ReclaimedNotSelection)
			})
		};
		// A selected copy the rewrite keeps.
		let unreclaimed = record(1, 10, false);
		assert!(is_refusal(certified_tier_drop(
			&[unreclaimed, kept],
			&rows,
			&selection
		)));
		// A second durable copy of the selected version the records lack.
		let mut extra = rows.clone();
		extra.push(row(&selected));
		assert!(is_refusal(certified_tier_drop(
			&[selected, kept],
			&extra,
			&selection
		)));
		// A record reclaimed outside the selection.
		let outside = record(2, 20, true);
		assert!(is_refusal(certified_tier_drop(
			&[selected, outside],
			&rows,
			&selection
		)));
		// A dropped row attributed to another table.
		let mut attributed = rows.clone();
		attributed[0].table_object_id = Some(TableObjectId::new([8; 16]));
		assert!(is_refusal(certified_tier_drop(
			&[selected, kept],
			&attributed,
			&selection
		)));
	}
}
