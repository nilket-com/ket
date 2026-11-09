//! ADR 0703: LSM compaction that elides exactly one certified selection.
//!
//! In ParentDecisions mode the ordinary LSM picker stays off. The only LSM
//! path is this one: the leader claims a plan, reads the lifecycle-resolved
//! entries of its inputs, decides once (S5b's coherent facts and content
//! evidence for exactly those versions) which versions the job elides, and
//! runs the ordinary control-ring job with that selection as its plan's
//! elision. Install binds a certificate of the exact drop multiset to the
//! generation the selection was decided under, and recovery accepts the
//! install only with that certificate.

use super::secondary_index_segments::MaintenanceClaims;
use super::*;
use crate::storage::index::certified_base::{CoherentFacts, VersionRef};
use crate::storage::index::{
	LsmCertificate, LsmInstallIntent, PersistentIndexSnapshot, PersistentRecordRow,
};
use crate::storage::lsm::{PlanElision, SurvivorOrDrop};
use crate::storage::lsm_worker::LsmCompactionWorker;
use crate::storage::manager::certified_cleanup::ContentEvidence;
use crate::storage::slab_pins::SlabPinGuard;

/// The membership key of a certified version: (tenant, key, HLC, tombstone).
pub(crate) type CertifiedMemberKey = (TenantId, Identity, HybridTimestamp, bool);

/// ADR 0703: the versions one LSM job may elide, decided once from one coherent
/// index read and content evidence for exactly this job's inputs. Its fields
/// are private to this module: only [`CertifiedSelection::decide`] builds one,
/// and nothing changes it after; other modules only read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CertifiedSelection {
	/// Each member's table: the one its debt record names.
	members: BTreeMap<CertifiedMemberKey, TableObjectId>,
	/// The active generation the facts were read under.
	active_gid: u64,
	/// The job and exact inputs it was decided for.
	job_fingerprint: [u8; 32],
	/// The claimed inputs' durable rows digest (`lsm_input_rows_digest`).
	input_rows_digest: [u8; 32],
}

impl CertifiedSelection {
	/// Whether the selection names this record's version.
	pub(crate) fn selects(&self, record: &VersionEntry) -> bool {
		self.members.contains_key(&member_key(record))
	}

	/// The generation the facts were read under.
	pub(crate) fn active_gid(&self) -> u64 {
		self.active_gid
	}

	/// The job and exact inputs it was decided for.
	pub(crate) fn job_fingerprint(&self) -> [u8; 32] {
		self.job_fingerprint
	}

	/// How many distinct versions it names.
	pub fn len(&self) -> usize {
		self.members.len()
	}

	/// Whether it names none.
	pub fn is_empty(&self) -> bool {
		self.members.is_empty()
	}
}

/// What a copy's own frame says about its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameTable {
	/// Not a multi-row frame, or one with no table column: no stamp.
	Untyped,
	/// The table the frame stamps for this row.
	Stamped(TableObjectId),
	/// A multi-row frame whose batch or table column does not decode. Never
	/// read as untyped: the version is kept.
	Malformed,
}

/// The table `frame` stamps for row `row`. Only a frame that is not multi-row,
/// or carries no table column, is untyped; a decoding error is malformed.
pub(crate) fn frame_table_of(frame: &[u8], row: RecordRowIndex) -> FrameTable {
	match storage_multirow_index_entries_from_frame(frame) {
		Ok(None) => FrameTable::Untyped,
		Ok(Some(_)) => match decode_record_batch(frame) {
			Ok(batch) => match storage_row_table_object_id_at(&batch, row) {
				Ok(Some(table)) => FrameTable::Stamped(table),
				Ok(None) => FrameTable::Untyped,
				Err(_) => FrameTable::Malformed,
			},
			Err(_) => FrameTable::Malformed,
		},
		Err(_) => FrameTable::Malformed,
	}
}

/// A claimed LSM job awaiting its certified selection. Its pins and claims
/// are held until it is finished or dropped.
pub(crate) struct LsmCertifiedJob {
	plan: CompactionPlan,
	_pins: SlabPinGuard,
	_claims: MaintenanceClaims,
	/// The inputs' lifecycle-resolved entries.
	entries: Vec<VersionEntry>,
	/// What each entry's own frame says about its table.
	frame_tables: Vec<FrameTable>,
	input_rows_digest: [u8; 32],
	fingerprint: [u8; 32],
}

impl LsmCertifiedJob {
	/// The distinct versions of the job's inputs, sorted: what the coherent
	/// facts must be read for.
	pub(crate) fn versions(&self) -> Vec<VersionRef> {
		let mut versions: Vec<VersionRef> = self.entries.iter().map(version_ref).collect();
		versions.sort_unstable();
		versions.dedup();
		versions
	}
}

fn version_ref(entry: &VersionEntry) -> VersionRef {
	VersionRef {
		tenant: entry.tenant_id,
		key_identity: *entry.row_key_id.as_bytes(),
		hlc: entry.row_hlc,
	}
}

fn member_key(entry: &VersionEntry) -> CertifiedMemberKey {
	(
		entry.tenant_id,
		entry.row_key_id,
		entry.row_hlc,
		entry.location.is_tombstone,
	)
}

fn row_member_key(row: &PersistentRecordRow) -> CertifiedMemberKey {
	(
		row.tenant_id,
		row.key_identity,
		row.location.hlc,
		row.location.is_tombstone,
	)
}

/// The job and exact inputs a selection is decided for: its id, its
/// manifest epoch, its sorted input slabs and their durable rows digest.
fn job_fingerprint(
	job_id: CompactionJobId,
	manifest_epoch_pre: ManifestEpoch,
	inputs: impl IntoIterator<Item = SlabId>,
	input_rows_digest: &[u8; 32],
) -> [u8; 32] {
	let mut inputs: Vec<SlabId> = inputs.into_iter().collect();
	inputs.sort_unstable();
	inputs.dedup();
	let mut hasher = blake3::Hasher::new();
	hasher.update(b"lsm-certified-job-v1");
	hasher.update(&job_id.get().to_le_bytes());
	hasher.update(&manifest_epoch_pre.get().to_le_bytes());
	for slab in inputs {
		hasher.update(&slab.get().to_le_bytes());
	}
	hasher.update(input_rows_digest);
	*hasher.finalize().as_bytes()
}

/// Recovery: the certificate an install must carry. A certificate must bind
/// this exact install and job; a contained store's install must have one.
pub(super) fn certificate_of_install<'a>(
	intent: &LsmInstallIntent,
	durable: &'a PersistentIndexSnapshot,
	contained: bool,
) -> Result<Option<&'a LsmCertificate>, &'static str> {
	let Some(certificate) = durable.lsm_certificate(intent) else {
		// A compaction install retires its inputs; GC marks and removals never
		// do, never carry a certificate, and keep their own recovery.
		return if contained && !intent.commit.slabs_to_retire.is_empty() {
			Err("a contained store's install has no certificate")
		} else {
			Ok(None)
		};
	};
	if intent.kind != crate::storage::index::LsmInstallKind::Control {
		return Err("a certified install is only ever a control install");
	}
	if certificate.intent_binding != crate::storage::index::intent_binding(intent) {
		return Err("its certificate binds another install");
	}
	let fingerprint = job_fingerprint(
		intent.commit.job_id,
		intent.commit.manifest_epoch_pre,
		intent.inputs.iter().map(|input| input.slab_id),
		&intent.local_input_rows_digest,
	);
	if certificate.job_fingerprint != fingerprint {
		return Err("its certificate is for another job or other inputs");
	}
	Ok(Some(certificate))
}

fn row_version_ref(row: &PersistentRecordRow) -> VersionRef {
	VersionRef {
		tenant: row.tenant_id,
		key_identity: *row.key_identity.as_bytes(),
		hlc: row.location.hlc,
	}
}

/// Recovery: the tables of the residual rows, from their durable debt
/// records (one unambiguous table each), never from a catalog. Debt is
/// retired only once no copy is left, and the residual rows are copies, so
/// their records stand. A version without one is left out and refuses.
pub(super) async fn residual_tables(
	index: &crate::storage::index::PersistentLocalIndex,
	residual: &[PersistentRecordRow],
) -> std::io::Result<BTreeMap<VersionRef, TableObjectId>> {
	let mut versions: Vec<VersionRef> = residual.iter().map(row_version_ref).collect();
	versions.sort_unstable();
	versions.dedup();
	let facts = index.version_facts(versions).await?;
	let mut tables = BTreeMap::new();
	for (at, version) in facts.candidates().iter().enumerate() {
		let fact = facts.get(at);
		if let (Some((key, _)), false) = (&fact.debt, fact.ambiguous) {
			tables.insert(*version, key.table);
		}
	}
	Ok(tables)
}

/// Recovery: the residual input rows are exactly the certificate's drop.
pub(super) fn residual_matches_certificate(
	residual: &[PersistentRecordRow],
	tables: &BTreeMap<VersionRef, TableObjectId>,
	certificate: &LsmCertificate,
) -> Result<(), &'static str> {
	let table_of = |row: &PersistentRecordRow| tables.get(&row_version_ref(row)).copied();
	let Ok(summary) = canonical_drop(residual, table_of) else {
		return Err("a dropped row has no durable identity");
	};
	if summary.drop_digest != certificate.drop_digest
		|| summary.drop_rows_digest != certificate.drop_rows_digest
		|| summary.drop_count != certificate.drop_count
	{
		return Err("its dropped rows are not its certificate's");
	}
	Ok(())
}

impl CertifiedSelection {
	/// The versions `job` elides (ADR 0703, S5b's local rule applied to every
	/// copy). A version is a member iff every copy of it among the job's
	/// inputs:
	/// - has facts listing positive debt under exactly one table, not
	///   ambiguous, with no owner and no active base entry at its HLC;
	/// - has a frame that is well formed, and a table that its frame stamps
	///   or its durable row attributes, if any, equal to the debt's (an
	///   untyped copy takes the debt's table);
	/// - has content evidence for that debt record and its kind;
	/// - is not a migrated-range marker;
	/// and no copy of the same (tenant, key, HLC) has the other kind. There is
	/// no tombstone-coverage override: a certified drop is proved by its own
	/// debt and evidence. Facts read for other versions are refused outright.
	pub(crate) fn decide(
		job: &LsmCertifiedJob,
		facts: &CoherentFacts,
		evidence: &[ContentEvidence],
	) -> std::io::Result<Self> {
		if facts.candidates() != job.versions().as_slice() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"the facts were not read for this job's versions",
			));
		}
		let members = decide_members(&job.entries, &job.frame_tables, facts, evidence);
		Ok(Self {
			members,
			active_gid: facts.active_gid(),
			job_fingerprint: job.fingerprint,
			input_rows_digest: job.input_rows_digest,
		})
	}
}

/// The members of a selection, from the entries of a job (with their frame
/// tables), facts read for exactly their versions, and content evidence. See
/// [`CertifiedSelection::decide`] for the rule.
fn decide_members(
	entries: &[VersionEntry],
	frame_tables: &[FrameTable],
	facts: &CoherentFacts,
	evidence: &[ContentEvidence],
) -> BTreeMap<CertifiedMemberKey, TableObjectId> {
	// Per member: its table while every copy passes, None once one fails.
	let mut verdicts: BTreeMap<CertifiedMemberKey, Option<TableObjectId>> = BTreeMap::new();
	let mut kinds: BTreeMap<VersionRef, (bool, bool)> = BTreeMap::new();
	for (entry, frame_table) in entries.iter().zip(frame_tables) {
		let reference = version_ref(entry);
		let seen = kinds.entry(reference).or_insert((false, false));
		if entry.location.is_tombstone {
			seen.1 = true;
		} else {
			seen.0 = true;
		}
		let passes = || -> Option<TableObjectId> {
			if entry.lifecycle == VersionLifecycle::MigratedRangeTombstone {
				return None;
			}
			let at = facts.candidates().binary_search(&reference).ok()?;
			let fact = facts.get(at);
			let (key, debt) = fact.debt.as_ref()?;
			if fact.ambiguous
				|| fact.owned
				|| fact.base.as_ref().is_some_and(|base| base.hlc == key.hlc)
			{
				return None;
			}
			// A table the frame stamps, or the durable row attributes, must be
			// the debt's. An untyped row (ParentDecisions writes its rows so)
			// takes the debt's, which its content evidence checks against that
			// table's descriptor, as S5b's local rule does.
			if match frame_table {
				FrameTable::Malformed => true,
				FrameTable::Stamped(table) => *table != key.table,
				FrameTable::Untyped => false,
			} || entry
				.table_object_id
				.is_some_and(|table| table != key.table)
			{
				return None;
			}
			evidence
				.iter()
				.any(|evidence| evidence.matches(*key, debt, entry.location.is_tombstone))
				.then_some(key.table)
		};
		let verdict = passes();
		verdicts
			.entry(member_key(entry))
			.and_modify(|current| {
				if *current != verdict {
					*current = None;
				}
			})
			.or_insert(verdict);
	}
	verdicts
		.into_iter()
		.filter_map(|(member, table)| {
			let reference = VersionRef {
				tenant: member.0,
				key_identity: *member.1.as_bytes(),
				hlc: member.2,
			};
			// A conflicting pair: both kinds at one version stay.
			let conflicting = kinds
				.get(&reference)
				.is_some_and(|(row, tombstone)| *row && *tombstone);
			table.filter(|_| !conflicting).map(|table| (member, table))
		})
		.collect()
}

/// The canonical drop multiset of a certified install: one deterministic
/// encoding, shared by the install and by recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DropSummary {
	/// blake3 over the sorted (tenant, table, key, HLC, kind, count) of each
	/// dropped member.
	pub drop_digest: [u8; 32],
	/// `lsm_rows_digest` of the dropped durable rows.
	pub drop_rows_digest: [u8; 32],
	pub drop_count: u64,
}

/// The canonical drop of `rows`, the dropped durable input rows, each with
/// the table its durable debt record names (`table_of`). A row whose table
/// cannot be established has no identity and refuses.
pub(crate) fn canonical_drop(
	rows: &[PersistentRecordRow],
	table_of: impl Fn(&PersistentRecordRow) -> Option<TableObjectId>,
) -> std::io::Result<DropSummary> {
	let mut members: BTreeMap<(TenantId, TableObjectId, Identity, HybridTimestamp, bool), u64> =
		BTreeMap::new();
	for row in rows {
		let table = table_of(row).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"a dropped LSM row has no established table identity",
			)
		})?;
		*members
			.entry((
				row.tenant_id,
				table,
				row.key_identity,
				row.location.hlc,
				row.location.is_tombstone,
			))
			.or_default() += 1;
	}
	let sorted: Vec<_> = members.into_iter().collect();
	let encoded = postcard::to_allocvec(&sorted).map_err(Error::other)?;
	let mut hasher = blake3::Hasher::new();
	hasher.update(b"lsm-canonical-drop-v1");
	hasher.update(&encoded);
	Ok(DropSummary {
		drop_digest: *hasher.finalize().as_bytes(),
		drop_rows_digest: crate::storage::index::lsm_rows_digest(rows.iter().copied()),
		drop_count: rows.len() as u64,
	})
}

/// At intent capture: the durable input rows the selection names, checked
/// against the job's inputs and the drop decisions it ran, as one canonical
/// drop.
pub(super) fn certified_drop_at_capture(
	selection: &CertifiedSelection,
	snapshot: &PersistentIndexSnapshot,
	inputs: &HashSet<SlabId>,
	input_digest: [u8; 32],
	decisions: &[SurvivorOrDrop],
) -> Result<DropSummary, CompactionFailure> {
	if selection.input_rows_digest != input_digest {
		return Err(CompactionFailure::ManifestRace);
	}
	let dropped: Vec<PersistentRecordRow> = snapshot
		.record_rows
		.iter()
		.filter(|row| inputs.contains(&row.location.slab_id))
		.filter(|row| selection.members.contains_key(&row_member_key(row)))
		.copied()
		.collect();
	// A dropped row's durable attribution, if it has one, is its member's table.
	if dropped.iter().any(|row| {
		row.table_object_id.is_some_and(|table| {
			Some(table) != selection.members.get(&row_member_key(row)).copied()
		})
	}) {
		return Err(CompactionFailure::VisibleDigestInvariance);
	}
	// The same multiset the job's decisions dropped, member by member.
	let mut durable: BTreeMap<CertifiedMemberKey, u64> = BTreeMap::new();
	for row in &dropped {
		*durable.entry(row_member_key(row)).or_default() += 1;
	}
	let mut decided: BTreeMap<CertifiedMemberKey, u64> = BTreeMap::new();
	for decision in decisions {
		if let SurvivorOrDrop::Drop { record, .. } = decision {
			*decided.entry(member_key(record)).or_default() += 1;
		}
	}
	if durable != decided {
		return Err(CompactionFailure::VisibleDigestInvariance);
	}
	Ok(canonical_drop(&dropped, |row| {
		selection.members.get(&row_member_key(row)).copied()
	})?)
}

/// Recovery: the input rows no output consumes, by a consuming multiset
/// match on (tenant, key, HLC, kind). An output with nothing left to consume,
/// or a residual identity that an output also holds (a member is all or
/// nothing), refuses.
pub(super) fn residual_after_outputs(
	inputs: &[PersistentRecordRow],
	outputs: &[CertifiedMemberKey],
) -> Result<Vec<PersistentRecordRow>, &'static str> {
	let mut remaining: BTreeMap<CertifiedMemberKey, Vec<PersistentRecordRow>> = BTreeMap::new();
	for row in inputs {
		remaining.entry(row_member_key(row)).or_default().push(*row);
	}
	let mut produced: BTreeSet<CertifiedMemberKey> = BTreeSet::new();
	for output in outputs {
		let Some(copies) = remaining.get_mut(output) else {
			return Err("an output row has no input row");
		};
		if copies.pop().is_none() {
			return Err("an output row has no input row left");
		}
		produced.insert(*output);
	}
	let mut residual = Vec::new();
	for (member, copies) in remaining {
		if copies.is_empty() {
			continue;
		}
		if produced.contains(&member) {
			return Err("a version both survives and is dropped");
		}
		residual.extend(copies);
	}
	Ok(residual)
}

/// Decides a claimed job's selection: the data server, which holds the
/// table descriptors that content evidence needs.
#[async_trait::async_trait]
pub(crate) trait CertifiedLsmDecider: Send + Sync {
	async fn decide(&self, job: &LsmCertifiedJob) -> std::io::Result<CertifiedSelection>;
}

impl SlabManager {
	/// ADR 0703: one certified LSM job, if one is admissible: pick, claim,
	/// decide once, run. Only a contained (ParentDecisions) store runs it;
	/// tiered inputs stay contained.
	pub(crate) async fn run_certified_lsm_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		decider: &dyn CertifiedLsmDecider,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		if !self.versions_contained() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"certified LSM runs only in a contained store",
			)
			.into());
		}
		if self.certified_lsm_fenced.load(Ordering::Acquire) {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"an orphan LSM certificate fences certified LSM on this store",
			)
			.into());
		}
		let tiered: HashSet<SlabId> = {
			let state = self.state.lock().await;
			state.tiered.iter().map(|entry| *entry.key()).collect()
		};
		let Some(plan) = self.pick_lsm_plan_excluding(worker, now, &tiered) else {
			return Ok(None);
		};
		self.execute_lsm_plan_under_policy(plan, |plan| async move {
			let started = Instant::now();
			let source_level = plan.source_level;
			let input_slabs = plan.touched_slabs().len() as u64;
			let result = async {
				let job = self
					.claim_lsm_for_certified_elision(worker, plan, table_schemas)
					.await?;
				let selection = decider.decide(&job).await?;
				self.finish_certified_lsm(
					worker,
					job,
					selection,
					committer,
					SecondaryIndexCompactionContext {
						table_schemas,
						secondary_indexes,
					},
				)
				.await
			}
			.await;
			self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
			result
		})
		.await
	}

	/// Takes the plan's pins and claims, revalidates it, refuses counter and
	/// tiered inputs, and reads its inputs' entries and frame tables.
	async fn claim_lsm_for_certified_elision(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		table_schemas: &TableSchemaCatalog,
	) -> Result<LsmCertifiedJob, CompactionFailure> {
		let pins = worker.acquire_plan_pins(&plan)?;
		let claims = self.acquire_maintenance_claims(&plan.touched_slabs()).await;
		self.revalidate_lsm_plan_after_claims(&plan)?;
		self.refuse_lsm_counter_inputs(&plan).await?;
		{
			let state = self.state.lock().await;
			if plan
				.touched_slabs()
				.iter()
				.any(|slab| state.tiered.contains_key(slab))
			{
				return Err(CompactionFailure::ManifestRace);
			}
		}
		let entries = self
			.lsm_version_entries_for_plan_with_table_schemas(&plan, table_schemas)
			.await?;
		let mut frame_tables = Vec::with_capacity(entries.len());
		for entry in &entries {
			let frame = self
				.read_with_reference_origin(entry.location, PublishedReferenceOrigin::Compaction)
				.await?;
			frame_tables.push(frame_table_of(&frame, entry.location.record_row_index));
		}
		let touched: HashSet<SlabId> = plan.touched_slabs().into_iter().collect();
		let input_rows_digest = crate::storage::index::lsm_rows_digest(
			self.persistent_record_rows_snapshot()
				.into_iter()
				.filter(|row| touched.contains(&row.location.slab_id)),
		);
		let fingerprint = job_fingerprint(
			plan.job_id,
			plan.manifest_epoch_pre,
			plan.touched_slabs(),
			&input_rows_digest,
		);
		Ok(LsmCertifiedJob {
			plan,
			_pins: pins,
			_claims: claims,
			entries,
			frame_tables,
			input_rows_digest,
			fingerprint,
		})
	}

	/// Runs the claimed job with its selection as the plan's elision.
	async fn finish_certified_lsm(
		&self,
		worker: &LsmCompactionWorker,
		job: LsmCertifiedJob,
		selection: CertifiedSelection,
		committer: &dyn TierCommitter,
		context: SecondaryIndexCompactionContext<'_>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		if selection.job_fingerprint != job.fingerprint
			|| selection.input_rows_digest != job.input_rows_digest
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"a certified selection applies only to the job it was decided for",
			)
			.into());
		}
		let LsmCertifiedJob {
			mut plan,
			_pins,
			_claims,
			..
		} = job;
		plan.elision = PlanElision::Certified(Arc::new(selection));
		self.run_claimed_lsm_via_control_ring(worker, plan, committer, Some(context))
			.await
	}
}

#[cfg(test)]
impl SlabManager {
	/// Test-only: one certified job over `plan` (no picker): claim, decide,
	/// run, exactly as the tick does.
	pub(crate) async fn run_certified_lsm_plan_for_test(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		decider: &dyn CertifiedLsmDecider,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		assert!(self.versions_contained(), "certified LSM is contained-only");
		let job = self
			.claim_lsm_for_certified_elision(worker, plan, table_schemas)
			.await?;
		let selection = decider.decide(&job).await?;
		self.finish_certified_lsm(
			worker,
			job,
			selection,
			committer,
			SecondaryIndexCompactionContext {
				table_schemas,
				secondary_indexes,
			},
		)
		.await
	}

	/// Test-only: whether an orphan certificate fenced certified LSM at open.
	pub(crate) fn certified_lsm_fenced_for_test(&self) -> bool {
		self.certified_lsm_fenced.load(Ordering::Acquire)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::table::PlacementSemantics;
	use crate::storage::DropReason;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn key(byte: u8) -> Identity {
		Identity::new([byte; 32])
	}

	const TABLE: TableObjectId = TableObjectId::new([0x73; 16]);

	fn location(slab: u64, offset: u64, ms: u64, tombstone: bool) -> RecordLocation {
		RecordLocation {
			slab_id: SlabId::new(slab),
			offset,
			length: 10,
			hlc: hlc(ms),
			record_row_index: RecordRowIndex::new(0),
			is_tombstone: tombstone,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		}
	}

	fn entry(
		byte: u8,
		ms: u64,
		slab: u64,
		tombstone: bool,
		lifecycle: VersionLifecycle,
	) -> VersionEntry {
		VersionEntry {
			tenant_id: TenantId::ZERO,
			table_object_id: Some(TABLE),
			row_key_id: key(byte),
			row_hlc: hlc(ms),
			record_row_index: RecordRowIndex::new(0),
			location: location(slab, ms, ms, tombstone),
			lifecycle,
		}
	}

	fn row(byte: u8, ms: u64, slab: u64, tombstone: bool) -> PersistentRecordRow {
		PersistentRecordRow {
			tenant_id: TenantId::ZERO,
			key_identity: key(byte),
			routing_locality: Locality::new(0),
			table_object_id: Some(TABLE),
			placement: PlacementSemantics::RawLocality,
			partition: None,
			location: location(slab, ms, ms, tombstone),
		}
	}

	fn selection(members: &[(u8, u64, bool)]) -> CertifiedSelection {
		CertifiedSelection {
			members: members
				.iter()
				.map(|(byte, ms, tombstone)| {
					((TenantId::ZERO, key(*byte), hlc(*ms), *tombstone), TABLE)
				})
				.collect(),
			active_gid: 1,
			job_fingerprint: [1; 32],
			input_rows_digest: [2; 32],
		}
	}

	/// ADR 0703: under a certified selection exactly the selected records drop,
	/// whatever their lifecycle; the floor rule never runs, so a record that
	/// floor elision would drop survives when it is not selected.
	#[test]
	fn a_certified_elision_drops_exactly_the_selected_records() {
		let records = vec![
			entry(1, 10, 1, false, VersionLifecycle::PrimaryKey),
			entry(1, 20, 1, false, VersionLifecycle::PrimaryKey),
			entry(2, 10, 1, false, VersionLifecycle::AppendOnly),
			entry(3, 10, 1, true, VersionLifecycle::AppendOnlyContentHash),
			entry(4, 10, 1, false, VersionLifecycle::AmbiguousTable),
			entry(5, 10, 1, true, VersionLifecycle::MigratedRangeTombstone),
		];
		// Under a floor above everything, today's rule drops several of them.
		let floor = apply_elision(records.clone(), &PlanElision::RetentionFloor(hlc(1_000)));
		assert!(
			floor
				.iter()
				.filter(|decision| matches!(decision, SurvivorOrDrop::Drop { .. }))
				.count() > 1,
			"premise: the floor rule would drop more"
		);
		let certified = apply_elision(
			records,
			&PlanElision::Certified(Arc::new(selection(&[(1, 10, false), (3, 10, true)]))),
		);
		let dropped: Vec<_> = certified
			.iter()
			.filter_map(|decision| match decision {
				SurvivorOrDrop::Drop { record, reason } => {
					assert_eq!(*reason, DropReason::Certified);
					Some((record.row_key_id, record.row_hlc.physical_ms))
				}
				SurvivorOrDrop::Survivor(_) => None,
			})
			.collect();
		assert_eq!(dropped, vec![(key(1), 10), (key(3), 10)]);
		// A selection names a kind: the same version's other kind is not selected.
		let other_kind = apply_elision(
			[entry(1, 10, 1, true, VersionLifecycle::PrimaryKey)],
			&PlanElision::Certified(Arc::new(selection(&[(1, 10, false)]))),
		);
		assert!(matches!(other_kind[0], SurvivorOrDrop::Survivor(_)));
	}

	fn typed(_: &PersistentRecordRow) -> Option<TableObjectId> {
		Some(TABLE)
	}

	/// ADR 0703 R1: two identical copies of one selected version count twice;
	/// a row whose table cannot be established refuses; the logical digest
	/// names the version and its multiplicity, the physical digest each copy.
	#[test]
	fn the_canonical_drop_counts_every_copy_and_refuses_a_missing_identity() {
		let once = canonical_drop(&[row(1, 10, 1, false)], typed).unwrap();
		let twice = canonical_drop(&[row(1, 10, 1, false), row(1, 10, 2, false)], typed).unwrap();
		assert_eq!(once.drop_count, 1);
		assert_eq!(twice.drop_count, 2);
		assert_ne!(once.drop_digest, twice.drop_digest, "multiplicity counts");
		assert_ne!(once.drop_rows_digest, twice.drop_rows_digest);
		// Order does not matter: one deterministic encoding.
		assert_eq!(
			twice,
			canonical_drop(&[row(1, 10, 2, false), row(1, 10, 1, false)], typed).unwrap()
		);
		// The kind and the table are part of the identity.
		assert_ne!(
			once.drop_digest,
			canonical_drop(&[row(1, 10, 1, true)], typed)
				.unwrap()
				.drop_digest
		);
		assert_ne!(
			once.drop_digest,
			canonical_drop(&[row(1, 10, 1, false)], |_| Some(TableObjectId::new(
				[0x74; 16]
			)))
			.unwrap()
			.drop_digest
		);
		assert_eq!(
			canonical_drop(&[row(1, 10, 1, false)], |_| None)
				.unwrap_err()
				.kind(),
			ErrorKind::InvalidData
		);
	}

	/// ADR 0703 R1: recovery's consuming match. Every output consumes one input
	/// copy; the residual is every copy of the dropped versions; an output with
	/// nothing to consume, or a version both kept and dropped, refuses.
	#[test]
	fn recovery_consumes_one_input_copy_per_output() {
		let kept = |byte: u8, ms: u64| (TenantId::ZERO, key(byte), hlc(ms), false);
		let inputs = [
			row(1, 10, 1, false),
			row(1, 10, 2, false),
			row(2, 20, 1, false),
			row(2, 20, 2, false),
		];
		// Version 2's two copies both survive; version 1's both drop.
		let residual = residual_after_outputs(&inputs, &[kept(2, 20), kept(2, 20)]).unwrap();
		assert_eq!(residual.len(), 2);
		assert!(residual.iter().all(|row| row.key_identity == key(1)));
		assert_eq!(
			canonical_drop(&residual, typed).unwrap(),
			canonical_drop(&inputs[..2], typed).unwrap()
		);
		// A third output of version 2 has no input left.
		assert!(residual_after_outputs(&inputs, &[kept(2, 20), kept(2, 20), kept(2, 20)]).is_err());
		// One copy of version 1 kept and one dropped: refused.
		assert!(residual_after_outputs(&inputs, &[kept(1, 10), kept(2, 20), kept(2, 20)]).is_err());
		// An output no input names.
		assert!(residual_after_outputs(&inputs, &[kept(9, 90)]).is_err());
	}

	/// ADR 0703: the selection keeps every protected class and selects a
	/// version only when every copy of it in the job passes. Selected: a typed
	/// version with two identical copies, an untyped row (as ParentDecisions
	/// writes them) whose table is its debt's, and a debt tombstone next to an
	/// untyped row. Everything else stays.
	#[test]
	fn the_selection_keeps_every_protected_class() {
		use crate::storage::index::certified_base::{
			BaseEntry, DebtRecord, DebtSource, VersionFacts, VersionKey,
		};
		use crate::storage::manager::certified_cleanup::StoredCopy;
		const OTHER: TableObjectId = TableObjectId::new([0x74; 16]);
		use VersionLifecycle::{AmbiguousTable, MigratedRangeTombstone, PrimaryKey};
		// (key, ms, slab, tombstone, lifecycle, frame table, durable attribution)
		type Row = (
			u8,
			u64,
			u64,
			bool,
			VersionLifecycle,
			FrameTable,
			Option<TableObjectId>,
		);
		let (t, o) = (Some(TABLE), Some(OTHER));
		let (ft, fo, untyped, malformed) = (
			FrameTable::Stamped(TABLE),
			FrameTable::Stamped(OTHER),
			FrameTable::Untyped,
			FrameTable::Malformed,
		);
		let rows: Vec<Row> = vec![
			(1, 10, 1, false, PrimaryKey, ft, t), // selected, copy one
			(1, 10, 2, false, PrimaryKey, ft, t), // selected, copy two
			(2, 10, 1, false, PrimaryKey, ft, t), // owned
			(3, 10, 1, false, PrimaryKey, ft, t), // the base entry
			(4, 10, 1, false, PrimaryKey, ft, t), // ambiguous facts
			(5, 10, 1, false, PrimaryKey, ft, t), // content mismatch
			(6, 10, 1, false, AmbiguousTable, untyped, None), // untyped: selected
			(7, 10, 1, false, PrimaryKey, fo, t), // the frame disagrees
			(8, 10, 1, false, PrimaryKey, ft, t), // debt names another table
			(9, 10, 1, false, AmbiguousTable, untyped, o), // attribution disagrees
			(10, 10, 1, false, PrimaryKey, ft, t), // a conflicting pair
			(10, 10, 2, true, PrimaryKey, ft, t),
			(11, 10, 1, false, PrimaryKey, ft, t), // one copy fails
			(11, 10, 2, false, PrimaryKey, fo, t),
			(12, 10, 1, false, PrimaryKey, ft, t),             // no debt
			(13, 10, 1, true, MigratedRangeTombstone, ft, t),  // a migrated-range marker
			(14, 10, 1, true, PrimaryKey, ft, t),              // debt tombstone: selected
			(14, 20, 1, false, AmbiguousTable, untyped, None), // its untyped successor
			(15, 10, 1, false, AmbiguousTable, malformed, None), // malformed, never untyped
		];
		let entries: Vec<VersionEntry> = rows
			.iter()
			.map(|(byte, ms, slab, tombstone, lifecycle, _, attribution)| {
				let mut entry = entry(*byte, *ms, *slab, *tombstone, *lifecycle);
				entry.location.offset = *slab * 1_000 + *ms;
				entry.table_object_id = *attribution;
				entry
			})
			.collect();
		let frame_tables: Vec<FrameTable> = rows.iter().map(|row| row.5).collect();
		let mut candidates: Vec<VersionRef> = entries.iter().map(version_ref).collect();
		candidates.sort_unstable();
		candidates.dedup();
		let debt = |byte: u8| DebtRecord {
			gid: 1,
			row_digest: [byte; 32],
			source: DebtSource::RetiredIntent { rsv: 5, ordinal: 0 },
		};
		let mut evidence = Vec::new();
		let facts: Vec<VersionFacts> = candidates
			.iter()
			.map(|reference| {
				let byte = reference.key_identity[0];
				let table = if byte == 8 { OTHER } else { TABLE };
				let key = VersionKey {
					tenant: reference.tenant,
					table,
					key_identity: reference.key_identity,
					hlc: reference.hlc,
				};
				let listed = byte != 12 && !(byte == 14 && reference.hlc == hlc(20));
				let tombstone = matches!(byte, 13 | 14);
				if listed && byte != 5 {
					evidence.extend(ContentEvidence::check(
						key,
						&debt(byte),
						tombstone,
						&[StoredCopy {
							tombstone,
							row_digest: (!tombstone).then_some([byte; 32]),
						}],
					));
				}
				VersionFacts {
					debt: listed.then(|| (key, debt(byte))),
					ambiguous: byte == 4,
					base: (byte == 3).then(|| BaseEntry {
						tenant: reference.tenant,
						table,
						key_identity: reference.key_identity,
						hlc: reference.hlc,
						row_digest: [byte; 32],
						tombstone: false,
						position: 0,
					}),
					owned: byte == 2,
				}
			})
			.collect();
		let facts = CoherentFacts::for_test(1, candidates, facts);
		let members = decide_members(&entries, &frame_tables, &facts, &evidence);
		assert_eq!(
			members.keys().copied().collect::<Vec<_>>(),
			vec![
				(TenantId::ZERO, key(1), hlc(10), false),
				(TenantId::ZERO, key(6), hlc(10), false),
				(TenantId::ZERO, key(14), hlc(10), true),
			],
			"exactly the versions whose every copy passes"
		);
		assert!(
			members.values().all(|table| *table == TABLE),
			"the debt's table"
		);
		// Their copies drop (two, one and one); nothing else does.
		let selection = CertifiedSelection {
			members,
			active_gid: 1,
			job_fingerprint: [1; 32],
			input_rows_digest: [2; 32],
		};
		let dropped = apply_elision(entries, &PlanElision::Certified(Arc::new(selection)))
			.into_iter()
			.filter(|decision| matches!(decision, SurvivorOrDrop::Drop { .. }))
			.count();
		assert_eq!(dropped, 4);
	}

	/// ADR 0703: the certificate matches only its exact residual, with the
	/// tables the durable debt names.
	#[test]
	fn a_residual_matches_only_its_certificate() {
		let dropped = [row(1, 10, 1, false), row(1, 10, 2, false)];
		let summary = canonical_drop(&dropped, typed).unwrap();
		let certificate = LsmCertificate {
			job_fingerprint: [1; 32],
			active_gid: 1,
			drop_digest: summary.drop_digest,
			drop_rows_digest: summary.drop_rows_digest,
			drop_count: summary.drop_count,
			intent_binding: [3; 32],
		};
		let tables: BTreeMap<VersionRef, TableObjectId> = dropped
			.iter()
			.map(|row| (row_version_ref(row), TABLE))
			.collect();
		assert!(residual_matches_certificate(&dropped, &tables, &certificate).is_ok());
		assert!(residual_matches_certificate(&dropped[..1], &tables, &certificate).is_err());
		assert!(residual_matches_certificate(&[], &tables, &certificate).is_err());
		// A residual version whose debt record is gone has no identity.
		assert!(residual_matches_certificate(&dropped, &BTreeMap::new(), &certificate).is_err());
	}
}
