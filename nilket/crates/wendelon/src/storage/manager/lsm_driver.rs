use super::*;
use crate::storage::maintenance_policy::{DeterministicOrigin, deterministic_failure};

/// The version a caller expects a frame to hold, beyond its location.
#[derive(Clone, Copy, Debug)]
pub(super) struct ExpectedVersion {
	pub(super) tenant_id: Option<TenantId>,
	pub(super) key_identity: Option<Identity>,
}

/// A frame read for `target` must hold `target`'s version at its row. The
/// version is decoded, never assumed: a multi-row frame's row at the target
/// index, or a single-row frame's record header, gives its key and HLC; the
/// header gives its tenant. The HLC must be the target's, and the tenant and
/// key the caller's when it names them. A frame whose rows cannot be decoded
/// is an error.
pub(super) fn verify_frame_names_version(
	header: &SlabRecordHeader,
	frame: &[u8],
	target: RecordLocation,
	expected: ExpectedVersion,
) -> std::io::Result<()> {
	let superseded = || stale_location_error(target.slab_id, StaleLocationReason::Superseded);
	let (key_identity, hlc) = match storage_multirow_index_entries_from_frame(frame)? {
		Some(entries) => {
			let entry = entries
				.iter()
				.find(|entry| entry.record_row_index == target.record_row_index)
				.ok_or_else(superseded)?;
			(entry.key_identity, entry.row_hlc)
		}
		None if target.record_row_index == RecordRowIndex::ZERO => {
			(header.key_identity, header.hlc)
		}
		None => return Err(superseded()),
	};
	if hlc != target.hlc
		|| expected
			.tenant_id
			.is_some_and(|tenant_id| tenant_id != header.tenant_id)
		|| expected
			.key_identity
			.is_some_and(|expected| expected != key_identity)
	{
		return Err(superseded());
	}
	Ok(())
}

pub(super) fn lsm_run_summary_from_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	slab_id: SlabId,
	size_bytes: u64,
	manifest_epoch: ManifestEpoch,
) -> LsmRunSummary {
	let mut min_row_hlc: Option<HybridTimestamp> = None;
	let mut max_row_hlc: Option<HybridTimestamp> = None;
	let mut row_count = 0_u64;

	for entry in key_index.iter() {
		for location in entry.value() {
			if location.slab_id != slab_id {
				continue;
			}
			min_row_hlc = Some(
				min_row_hlc
					.map(|current| current.min(location.hlc))
					.unwrap_or(location.hlc),
			);
			max_row_hlc = Some(
				max_row_hlc
					.map(|current| current.max(location.hlc))
					.unwrap_or(location.hlc),
			);
			row_count = row_count.saturating_add(1);
		}
	}

	LsmRunSummary::new(
		slab_id,
		CompactionLevelIndex::L0,
		min_row_hlc.unwrap_or_else(zero_hlc),
		max_row_hlc.unwrap_or_else(zero_hlc),
		ByteCount::new(size_bytes),
		RowCount::new(row_count),
		manifest_epoch,
	)
}

pub(super) fn refresh_lsm_level_metrics(snapshot: &ManifestSnapshot) {
	for compaction_level in CompactionLevelIndex::ALL {
		metrics().set_lsm_level_summary(
			compaction_level,
			u64::from(snapshot.compaction_level_slab_count(compaction_level)),
			snapshot.compaction_level_total_bytes(compaction_level),
		);
	}
}

pub(super) fn remove_non_live_lsm_rows_from_index(
	key_index: &DashMap<TenantKey, Vec<RecordLocation>>,
	runs: &[LsmRunSummary],
) {
	let non_live_slabs = runs
		.iter()
		.filter(|run| !run.state.is_live())
		.map(|run| run.slab_id)
		.collect::<HashSet<_>>();
	if non_live_slabs.is_empty() {
		return;
	}
	let keys = key_index
		.iter()
		.map(|entry| *entry.key())
		.collect::<Vec<_>>();
	for key in keys {
		let should_remove_key = if let Some(mut versions) = key_index.get_mut(&key) {
			versions.retain(|location| !non_live_slabs.contains(&location.slab_id));
			versions.is_empty()
		} else {
			false
		};
		if should_remove_key {
			key_index.remove(&key);
		}
	}
}

pub(super) fn lsm_compaction_result_from_failure(err: &CompactionFailure) -> LsmCompactionResult {
	match err {
		CompactionFailure::ManifestRace => LsmCompactionResult::ManifestRace,
		CompactionFailure::CounterMaintenanceSkipped => LsmCompactionResult::Aborted,
		CompactionFailure::SidecarDigestMismatch => LsmCompactionResult::SidecarDigestMismatch,
		CompactionFailure::VisibleDigestInvariance => LsmCompactionResult::VisibleDigestInvariance,
		CompactionFailure::BackfillRaceLost | CompactionFailure::IoError(_) => {
			LsmCompactionResult::Aborted
		}
	}
}

pub(super) struct LsmInstalledCompactionOutputs {
	pub(super) installed: Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>,
	pub(super) removed_rows: Vec<PersistentRecordRow>,
	pub(super) inserted_rows: Vec<PersistentRecordRow>,
	pub(super) relocations: Vec<LsmRowRelocation>,
}

/// One surviving LSM input row and the output location that replaces it.
pub(super) struct LsmRowRelocation {
	pub(super) tenant_id: TenantId,
	pub(super) key_identity: Identity,
	pub(super) routing_locality: Locality,
	pub(super) table_object_id: Option<TableObjectId>,
	pub(super) old: RecordLocation,
	pub(super) new: RecordLocation,
}

#[async_trait]
impl LsmCompactionJobRunner for SlabManager {
	async fn run_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		SlabManager::run_lsm_compaction_once(self, worker, plan).await
	}
}

#[async_trait]
impl LsmCompactionDriver for SlabManager {
	async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		SlabManager::run_lsm_compaction_tick(self, worker, now).await
	}
}

#[async_trait]
impl LsmGcDriver for SlabManager {
	async fn run_lsm_gc_tick(&self, worker: &LsmGcWorker) -> std::io::Result<Vec<SlabId>> {
		SlabManager::sweep_lsm_delete_pending_once(self, worker).await
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct LsmSourceFrameKey {
	pub(super) slab_id: SlabId,
	pub(super) offset: u64,
	pub(super) length: u32,
}

impl From<RecordLocation> for LsmSourceFrameKey {
	fn from(location: RecordLocation) -> Self {
		Self {
			slab_id: location.slab_id,
			offset: location.offset,
			length: location.length,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// ADR 0097: one physical frame re-encoded to hold exactly a run.s planned rows,
/// with its index entries renumbered densely over them.
pub(super) struct LsmRewrittenMultiRowFrame {
	pub(super) frame: Vec<u8>,
	pub(super) entries: Vec<MultiRowIndexEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LsmMultiRowSourceFrame {
	pub(super) frame: Arc<Vec<u8>>,
	pub(super) entries: Vec<MultiRowIndexEntry>,
}

/// ADR 0097: identity match that does NOT compare `record_row_index`. A rewritten
/// frame renumbers its rows densely over the survivors, so the planned row and the
/// entry that now carries it agree on key, HLC and tombstone but not on position.
pub(super) fn lsm_multirow_index_entry_matches_version_ignoring_position(
	entry: &MultiRowIndexEntry,
	version: &VersionEntry,
) -> bool {
	entry.key_identity == version.row_key_id
		&& entry.row_hlc == version.row_hlc
		&& entry.is_tombstone == version.location.is_tombstone
}

pub(super) fn lsm_multirow_index_entry_matches_version(
	entry: &MultiRowIndexEntry,
	version: &VersionEntry,
) -> bool {
	entry.key_identity == version.row_key_id
		&& entry.row_hlc == version.row_hlc
		&& entry.record_row_index == version.record_row_index
		&& entry.is_tombstone == version.location.is_tombstone
}

pub(super) fn lsm_multirow_index_entry_for_version<'a>(
	entries: &'a [MultiRowIndexEntry],
	version: &VersionEntry,
) -> Option<&'a MultiRowIndexEntry> {
	entries
		.iter()
		.find(|entry| lsm_multirow_index_entry_matches_version(entry, version))
}

pub(super) fn validate_lsm_multirow_source_entry(
	version: &VersionEntry,
	entry: &MultiRowIndexEntry,
) -> std::io::Result<()> {
	if lsm_multirow_index_entry_matches_version(entry, version) {
		return Ok(());
	}
	Err(Error::new(
		ErrorKind::InvalidData,
		"LSM multi-row source entry does not match planned survivor",
	))
}

/// Hash a sealed input or staged output in bounded chunks so recovery can
/// distinguish the file captured by an intent from an unrelated same-id file.
pub(super) async fn lsm_file_identity(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
	slab_id: SlabId,
) -> std::io::Result<crate::storage::index::LsmFileIdentity> {
	let len = file_ops
		.file_len(path)
		.await?
		.ok_or_else(|| Error::new(ErrorKind::NotFound, "LSM intent file missing"))?;
	let handle = file_ops
		.open(
			path,
			OpenFlags {
				read: true,
				..Default::default()
			},
		)
		.await?;
	let mut digest = blake3::Hasher::new();
	let mut offset = 0_u64;
	while offset < len {
		let wanted = (len - offset).min(1024 * 1024) as usize;
		let chunk = file_ops.read_plain_at(&handle, offset, wanted).await?;
		if chunk.is_empty() || chunk.len() > wanted {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"LSM intent file changed while hashing",
			));
		}
		digest.update(&chunk);
		offset += chunk.len() as u64;
	}
	if file_ops.file_len(path).await? != Some(len) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"LSM intent file length changed while hashing",
		));
	}
	Ok(crate::storage::index::LsmFileIdentity {
		slab_id,
		len,
		digest: *digest.finalize().as_bytes(),
	})
}

/// Process-local capability minted only after an accepted control compaction.
/// It is not serializable or clonable; raw index updates still accept only Local.
pub(crate) struct AcceptedControlLsmCompletion {
	before: crate::storage::index::LsmInstallIntent,
}
impl AcceptedControlLsmCompletion {
	fn new_after_acceptance(
		intent: &crate::storage::index::LsmInstallIntent,
		no_sidecars: bool,
	) -> Option<Self> {
		(no_sidecars
			&& intent.kind == crate::storage::index::LsmInstallKind::Control
			&& intent.phase == crate::storage::index::LsmInstallPhase::Prepared
			&& !intent.local_input_runs.is_empty()
			&& intent.commit.manifest_epoch_pre != ManifestEpoch::MAX
			&& intent.commit.slabs_to_mark_delete_pending.is_empty()
			&& intent.commit.slabs_to_remove.is_empty()
			&& intent.commit.sidecar_swaps.is_empty()
			&& intent
				.local_input_runs
				.iter()
				.chain(&intent.commit.slabs_to_add)
				.all(|run| run.sidecar_coverage == crate::storage::SidecarCoverage::None))
		.then(|| Self {
			before: intent.clone(),
		})
	}
	fn published_intent(&self) -> crate::storage::index::LsmInstallIntent {
		let mut after = self.before.clone();
		after.phase = crate::storage::index::LsmInstallPhase::OutputsPublished;
		after
	}
	pub(crate) fn into_transition(
		self,
	) -> (
		crate::storage::index::LsmInstallIntent,
		crate::storage::index::LsmInstallIntent,
	) {
		let after = self.published_intent();
		(self.before, after)
	}
}

/// A guarded stage refusal is definite only before any output is renamed.
/// Every later failure is uncertain and keeps its intent fenced.
#[derive(Debug)]
pub(super) enum LsmStageFailure {
	BeforeRename(CompactionFailure),
	Uncertain(CompactionFailure),
}
impl From<CompactionFailure> for LsmStageFailure {
	fn from(failure: CompactionFailure) -> Self {
		Self::Uncertain(failure)
	}
}
impl From<std::io::Error> for LsmStageFailure {
	fn from(error: std::io::Error) -> Self {
		Self::Uncertain(error.into())
	}
}

pub(super) struct UnresolvedControlIntentMetric {
	legacy: bool,
}
impl UnresolvedControlIntentMetric {
	fn new(legacy: bool) -> Self {
		metrics().storage_lsm_control_intents_unresolved.add(1);
		if legacy {
			metrics()
				.storage_lsm_control_intents_legacy_unresolved
				.add(1);
		}
		Self { legacy }
	}
}
impl Drop for UnresolvedControlIntentMetric {
	fn drop(&mut self) {
		metrics().storage_lsm_control_intents_unresolved.sub(1);
		if self.legacy {
			metrics()
				.storage_lsm_control_intents_legacy_unresolved
				.sub(1);
		}
	}
}

/// Empty slab scope is unknown and fences every slab. On restart, or
/// when output keys are not cheaply knowable, private cross-slab apply also
/// remains fenced until the intent resolves.
pub(super) struct UnresolvedLsmScope {
	pub(super) control_legacy: Option<bool>,
	pub(super) control_metric: Option<UnresolvedControlIntentMetric>,
	pub(super) slabs: HashSet<SlabId>,
	pub(super) keys_unknown: bool,
	pub(super) in_flight: bool,
	pub(super) owner_token: Arc<()>,
}

pub(super) fn lsm_intent_scope(
	intent: &crate::storage::index::LsmInstallIntent,
	recovering: bool,
) -> UnresolvedLsmScope {
	let slabs = intent
		.inputs
		.iter()
		.map(|input| input.slab_id)
		.chain(intent.commit.slabs_to_add.iter().map(|run| run.slab_id))
		.chain(intent.commit.slabs_to_retire.iter().copied())
		.chain(intent.commit.slabs_to_mark_delete_pending.iter().copied())
		.chain(intent.commit.slabs_to_remove.iter().copied())
		.chain(
			intent
				.commit
				.sidecar_swaps
				.iter()
				.flat_map(|swap| [swap.source_slab_id, swap.output_slab_id]),
		)
		.collect();
	let control_legacy = (intent.kind == crate::storage::index::LsmInstallKind::Control).then_some(
		intent.local_input_runs.is_empty()
			|| intent.local_input_rows_digest == [0; 32]
			|| intent.local_output_rows_digest == [0; 32],
	);
	UnresolvedLsmScope {
		control_metric: if recovering {
			control_legacy.map(UnresolvedControlIntentMetric::new)
		} else {
			None
		},
		control_legacy,
		slabs,
		in_flight: !recovering,
		owner_token: Arc::new(()),
		keys_unknown: recovering
			|| !intent.commit.slabs_to_add.is_empty()
			|| !intent.local_output_files.is_empty()
			|| !intent.commit.sidecar_swaps.is_empty(),
	}
}

/// A live install owns its registered intent. Cancellation, panic or an
/// uncertain return leaves the intent orphaned and fences public admission.
pub(super) struct LsmIntentOwner<'a> {
	manager: &'a SlabManager,
	job_id: CompactionJobId,
	owner_token: Arc<()>,
	completed: bool,
}
impl LsmIntentOwner<'_> {
	fn complete(mut self) {
		self.completed = true;
	}
}
impl Drop for LsmIntentOwner<'_> {
	fn drop(&mut self) {
		if self.completed {
			return;
		}
		if let Some(mut scope) = self.manager.lsm_install_unresolved.get_mut(&self.job_id) {
			if !Arc::ptr_eq(&scope.owner_token, &self.owner_token) {
				return;
			}
			scope.in_flight = false;
			if scope.control_metric.is_none() {
				scope.control_metric = scope.control_legacy.map(UnresolvedControlIntentMetric::new);
			}
			drop(scope);
			self.manager.tier_authority.notify_publication_change();
		}
	}
}

#[cfg(test)]
pub(super) type ControlLsmInstallHold = std::sync::Mutex<
	Option<(
		ControlLsmInstallCut,
		Arc<tokio::sync::Semaphore>,
		Arc<tokio::sync::Semaphore>,
	)>,
>;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ControlLsmInstallCut {
	ObserveAll,
	BeforeRename,
	AfterRename,
	AfterAcceptance,
	AfterPublication,
}

impl SlabManager {
	/// This manager's share of the process-wide control-intent gauges, as
	/// (unresolved, legacy). Tests in one process share the gauges themselves.
	#[cfg(test)]
	pub(super) fn control_lsm_intent_metrics_for_test(&self) -> (usize, usize) {
		self.lsm_install_unresolved
			.iter()
			.filter_map(|scope| scope.control_metric.as_ref().map(|metric| metric.legacy))
			.fold((0, 0), |(all, legacy), is_legacy| {
				(all + 1, legacy + usize::from(is_legacy))
			})
	}

	#[cfg(test)]
	async fn pause_control_lsm_install_for_test(&self, cut: ControlLsmInstallCut) {
		let hold = self
			.lsm_control_install_hold
			.lock()
			.unwrap()
			.as_ref()
			.filter(|(expected, _, _)| {
				*expected == cut || *expected == ControlLsmInstallCut::ObserveAll
			})
			.cloned();
		if let Some((expected, entered, release)) = hold {
			entered.add_permits(1);
			// ObserveAll counts each reached cut without pausing it.
			if expected == ControlLsmInstallCut::ObserveAll {
				return;
			}
			if let Ok(permit) = release.acquire().await {
				permit.forget();
			}
		}
	}

	pub(super) async fn register_lsm_install_intent(
		&self,
		intent: &crate::storage::index::LsmInstallIntent,
	) -> Option<LsmIntentOwner<'_>> {
		// A private tier publication holds a read guard through its final
		// key-disjoint check and memory publication. Registering a new intent
		// cannot race between that check and the resulting key edits.
		let _scope = self.lsm_intent_scope_gate.write().await;
		match self.lsm_install_unresolved.entry(intent.commit.job_id) {
			dashmap::mapref::entry::Entry::Vacant(entry) => {
				let scope = lsm_intent_scope(intent, false);
				let owner_token = scope.owner_token.clone();
				entry.insert(scope);
				Some(LsmIntentOwner {
					manager: self,
					job_id: intent.commit.job_id,
					owner_token,
					completed: false,
				})
			}
			dashmap::mapref::entry::Entry::Occupied(_) => None,
		}
	}

	/// Nothing was renamed or proposed: clear exactly this durable intent.
	async fn clear_refused_lsm_intent(
		&self,
		index: &PersistentLocalIndex,
		intent: &crate::storage::index::LsmInstallIntent,
	) -> std::io::Result<()> {
		index.clear_lsm_install_intent(intent.clone()).await?;
		self.clear_registered_lsm_intent(intent.commit.job_id).await;
		Ok(())
	}

	pub(super) async fn clear_registered_lsm_intent(&self, job_id: CompactionJobId) {
		let _scope = self.lsm_intent_scope_gate.write().await;
		self.lsm_install_unresolved.remove(&job_id);
	}

	pub(super) fn has_orphaned_lsm_intents(&self) -> bool {
		self.lsm_install_unresolved
			.iter()
			.any(|entry| !entry.value().in_flight)
	}

	pub(super) fn lsm_intent_names_slab(&self, slab_id: SlabId) -> bool {
		self.lsm_install_unresolved.iter().any(|intent| {
			intent.value().slabs.is_empty() || intent.value().slabs.contains(&slab_id)
		})
	}

	pub(super) fn lsm_intent_keys_unknown(&self) -> bool {
		self.lsm_install_unresolved
			.iter()
			.any(|intent| intent.value().keys_unknown)
	}
}

/// Resolve only local intents whose complete file evidence proves a single
/// outcome. Every ambiguous or partial state leaves the intent in place.
pub(super) async fn reconcile_local_lsm_intent(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	index: &PersistentLocalIndex,
	snapshot: &crate::storage::index::PersistentIndexSnapshot,
	intent: &crate::storage::index::LsmInstallIntent,
) -> std::io::Result<bool> {
	use crate::storage::index::{LsmInstallKind, LsmInstallPhase};
	if intent.kind != LsmInstallKind::Local {
		return Ok(false);
	}
	let unique = |ids: Vec<SlabId>| {
		let count = ids.len();
		let set = ids.into_iter().collect::<HashSet<_>>();
		(set.len() == count).then_some(set)
	};
	let Some(inputs) = unique(intent.inputs.iter().map(|entry| entry.slab_id).collect()) else {
		return Ok(false);
	};
	let Some(local_inputs) = unique(
		intent
			.local_input_files
			.iter()
			.map(|entry| entry.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(expected_local_inputs) = unique(
		intent
			.inputs
			.iter()
			.filter(|entry| entry.tiered.is_none())
			.map(|entry| entry.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(outputs) = unique(
		intent
			.local_output_files
			.iter()
			.map(|entry| entry.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(expected_outputs) = unique(
		intent
			.commit
			.slabs_to_add
			.iter()
			.map(|entry| entry.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(expected_retirements) = unique(intent.commit.slabs_to_retire.clone()) else {
		return Ok(false);
	};
	let Some(input_run_ids) = unique(
		intent
			.local_input_runs
			.iter()
			.map(|run| run.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	if snapshot
		.secondary_index_segments
		.iter()
		.any(|segment| inputs.contains(&segment.key.source_slab_id))
	{
		return Ok(false);
	}
	if inputs.is_empty()
		|| inputs != expected_retirements
		|| input_run_ids != inputs
		|| intent
			.local_input_runs
			.iter()
			.any(|run| !run.state.is_live())
		|| local_inputs != expected_local_inputs
		|| outputs != expected_outputs
		|| !inputs.is_disjoint(&outputs)
	{
		return Ok(false);
	}
	let published = intent
		.local_output_files
		.iter()
		.filter(|file| {
			snapshot
				.lsm_runs
				.iter()
				.any(|run| run.slab_id == file.slab_id && run.state.is_live())
		})
		.count();
	let recorded = intent
		.local_output_files
		.iter()
		.filter(|file| {
			snapshot
				.lsm_runs
				.iter()
				.any(|run| run.slab_id == file.slab_id)
		})
		.count();
	if intent.phase == LsmInstallPhase::OutputsPublished {
		if published != intent.local_output_files.len() || recorded != published {
			return Ok(false);
		}
		let post_epoch = intent.commit.manifest_epoch_pre.saturating_add(1);
		let successors = intent
			.commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		for output in &intent.commit.slabs_to_add {
			let mut expected = output.clone();
			expected.state = SlabState::Live;
			expected.manifest_epoch = post_epoch;
			if !snapshot.lsm_runs.contains(&expected) {
				return Ok(false);
			}
		}
		if crate::storage::index::lsm_input_rows_digest(snapshot, &outputs)
			!= intent.local_output_rows_digest
		{
			return Ok(false);
		}
		for file in &intent.local_output_files {
			if lsm_file_identity(file_ops, &slab_path(dir, file.slab_id), file.slab_id).await?
				!= *file
			{
				return Ok(false);
			}
		}
		let mut retired_runs = intent.local_input_runs.clone();
		for run in &mut retired_runs {
			run.state = SlabState::Retired {
				successors: successors.clone(),
			};
			run.manifest_epoch = post_epoch;
		}
		let all_live = intent
			.local_input_runs
			.iter()
			.all(|run| snapshot.lsm_runs.contains(run));
		let all_retired = retired_runs
			.iter()
			.all(|run| snapshot.lsm_runs.contains(run));
		if all_live {
			if crate::storage::index::lsm_input_rows_digest(snapshot, &inputs)
				!= intent.local_input_rows_digest
			{
				return Ok(false);
			}
			for input in &intent.inputs {
				if let Some(pointer) = &input.tiered {
					if !snapshot.tiered_slabs.contains(pointer) {
						return Ok(false);
					}
				}
			}
			for file in &intent.local_input_files {
				if lsm_file_identity(file_ops, &slab_path(dir, file.slab_id), file.slab_id).await?
					!= *file
				{
					return Ok(false);
				}
			}
			let delete_records = snapshot
				.record_rows
				.iter()
				.filter(|row| inputs.contains(&row.location.slab_id))
				.copied()
				.collect();
			index
				.apply(PersistentIndexUpdate {
					delete_records,
					put_lsm_runs: retired_runs,
					..Default::default()
				})
				.await?;
		} else if !all_retired
			|| snapshot
				.record_rows
				.iter()
				.any(|row| inputs.contains(&row.location.slab_id))
		{
			return Ok(false);
		}
		index.clear_lsm_install_intent(intent.clone()).await?;
		return Ok(true);
	}
	if intent.phase != LsmInstallPhase::Prepared
		|| recorded != 0
		|| snapshot
			.record_rows
			.iter()
			.any(|row| outputs.contains(&row.location.slab_id))
		|| snapshot
			.tiered_slabs
			.iter()
			.any(|pointer| outputs.contains(&pointer.slab_id))
		|| crate::storage::index::lsm_input_rows_digest(snapshot, &inputs)
			!= intent.local_input_rows_digest
	{
		return Ok(false);
	}
	if !intent
		.local_input_runs
		.iter()
		.all(|run| snapshot.lsm_runs.contains(run))
	{
		return Ok(false);
	}
	for input in &intent.inputs {
		if let Some(pointer) = &input.tiered {
			if !snapshot.tiered_slabs.contains(pointer) {
				return Ok(false);
			}
		}
	}
	for file in &intent.local_input_files {
		if lsm_file_identity(file_ops, &slab_path(dir, file.slab_id), file.slab_id).await? != *file
		{
			return Ok(false);
		}
	}
	for file in &intent.local_output_files {
		let final_path = slab_path(dir, file.slab_id);
		let temp_path = lsm_compaction_tmp_path(dir, intent.commit.job_id, file.slab_id);
		let quarantine = dir.join(format!(
			"lsm-quarantine-{:016x}-slab_{:016x}.wnd",
			intent.commit.job_id, file.slab_id
		));
		let mut sources = Vec::new();
		for path in [&final_path, &temp_path] {
			if file_ops.file_len(path).await?.is_some() {
				sources.push(path);
			}
		}
		if sources.len() > 1 {
			return Ok(false);
		}
		let quarantine_exists = file_ops.file_len(&quarantine).await?.is_some();
		if quarantine_exists
			&& lsm_file_identity(file_ops, &quarantine, file.slab_id).await? != *file
		{
			return Ok(false);
		}
		if let Some(source) = sources.first() {
			if lsm_file_identity(file_ops, source, file.slab_id).await? != *file {
				return Ok(false);
			}
			if !quarantine_exists {
				file_ops.link_no_replace(source, &quarantine).await?;
				file_ops.fsync_dir(dir).await?;
			}
			file_ops.unlink(source).await?;
			file_ops.fsync_dir(dir).await?;
		} else if !quarantine_exists {
			return Ok(false);
		}
	}
	index.clear_lsm_install_intent(intent.clone()).await?;
	Ok(true)
}

/// A Control intent is not a proposal-outcome oracle. Only a local transaction
/// published after acceptance, with an exact completed postimage, may resolve.
pub(super) async fn reconcile_completed_control_lsm_intent(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	index: &PersistentLocalIndex,
	snapshot: &crate::storage::index::PersistentIndexSnapshot,
	intent: &crate::storage::index::LsmInstallIntent,
) -> std::io::Result<bool> {
	use crate::storage::index::{LsmInstallKind, LsmInstallPhase};
	if intent.kind != LsmInstallKind::Control
		|| intent.phase != LsmInstallPhase::OutputsPublished
		|| intent.local_input_rows_digest == [0; 32]
		|| intent.local_output_rows_digest == [0; 32]
		|| !intent.commit.slabs_to_mark_delete_pending.is_empty()
		|| !intent.commit.slabs_to_remove.is_empty()
		|| !intent.commit.sidecar_swaps.is_empty()
	{
		return Ok(false);
	}
	let commit_bytes = serde_json::to_vec(&intent.commit).map_err(Error::other)?;
	if *blake3::hash(&commit_bytes).as_bytes() != intent.commit_digest {
		return Ok(false);
	}
	let unique = |ids: Vec<SlabId>| {
		let len = ids.len();
		let ids = ids.into_iter().collect::<HashSet<_>>();
		(ids.len() == len).then_some(ids)
	};
	let Some(inputs) = unique(intent.inputs.iter().map(|input| input.slab_id).collect()) else {
		return Ok(false);
	};
	let Some(retirements) = unique(intent.commit.slabs_to_retire.clone()) else {
		return Ok(false);
	};
	let Some(input_runs) = unique(
		intent
			.local_input_runs
			.iter()
			.map(|run| run.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(outputs) = unique(
		intent
			.commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(output_files) = unique(
		intent
			.local_output_files
			.iter()
			.map(|file| file.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(local_inputs) = unique(
		intent
			.inputs
			.iter()
			.filter(|input| input.tiered.is_none())
			.map(|input| input.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	let Some(input_files) = unique(
		intent
			.local_input_files
			.iter()
			.map(|file| file.slab_id)
			.collect(),
	) else {
		return Ok(false);
	};
	if inputs.is_empty()
		|| inputs != retirements
		|| inputs != input_runs
		|| outputs != output_files
		|| local_inputs != input_files
		|| !inputs.is_disjoint(&outputs)
		|| intent.local_input_runs.iter().any(|run| {
			!run.state.is_live() || run.sidecar_coverage != crate::storage::SidecarCoverage::None
		}) || intent
		.commit
		.slabs_to_add
		.iter()
		.any(|run| run.sidecar_coverage != crate::storage::SidecarCoverage::None)
		|| snapshot.secondary_index_segments.iter().any(|row| {
			inputs.contains(&row.key.source_slab_id) || outputs.contains(&row.key.source_slab_id)
		}) || snapshot
		.record_rows
		.iter()
		.any(|row| inputs.contains(&row.location.slab_id))
		|| snapshot
			.tiered_slabs
			.iter()
			.any(|pointer| outputs.contains(&pointer.slab_id))
	{
		return Ok(false);
	}
	let Some(post_epoch) = intent
		.commit
		.manifest_epoch_pre
		.get()
		.checked_add(1)
		.map(ManifestEpoch::new)
	else {
		return Ok(false);
	};
	let successors = intent
		.commit
		.slabs_to_add
		.iter()
		.map(|run| run.slab_id)
		.collect::<Vec<_>>();
	for original in &intent.local_input_runs {
		let mut retired = original.clone();
		retired.state = SlabState::Retired {
			successors: successors.clone(),
		};
		retired.manifest_epoch = post_epoch;
		if !snapshot.lsm_runs.contains(&retired) {
			return Ok(false);
		}
	}
	for original in &intent.commit.slabs_to_add {
		let mut published = original.clone();
		published.state = SlabState::Live;
		published.manifest_epoch = post_epoch;
		if !snapshot.lsm_runs.contains(&published) {
			return Ok(false);
		}
	}
	if crate::storage::index::lsm_input_rows_digest(snapshot, &outputs)
		!= intent.local_output_rows_digest
	{
		return Ok(false);
	}
	for file in &intent.local_output_files {
		if lsm_file_identity(file_ops, &slab_path(dir, file.slab_id), file.slab_id).await? != *file
		{
			return Ok(false);
		}
	}
	// No row or file repair, no rollback, no deletion. The completed local
	// transaction is already present; clear only this exact saved intent.
	index.clear_lsm_install_intent(intent.clone()).await?;
	Ok(true)
}

pub(super) fn lsm_compaction_tmp_path(
	dir: &Path,
	job_id: CompactionJobId,
	slab_id: SlabId,
) -> PathBuf {
	dir.join(format!(
		"compaction-temp-{job_id:016x}-slab_{slab_id:016x}.wnd"
	))
}

pub(super) fn is_lsm_compaction_tmp_filename(name: &str) -> bool {
	name.starts_with("compaction-temp-")
}

pub(super) async fn cleanup_lsm_compaction_tmp_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
) -> std::io::Result<()> {
	let mut removed = 0usize;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		let Some(name) = entry.file_name.to_str() else {
			continue;
		};
		if !is_lsm_compaction_tmp_filename(name) {
			continue;
		}
		unlink_local_file(file_ops, &dir.join(name)).await?;
		removed = removed.saturating_add(1);
	}
	if removed > 0 {
		tracing::warn!(
			dir = %dir.display(),
			removed_tmp_files = removed,
			"removed stale LSM compaction temp files during open"
		);
	}
	Ok(())
}

impl SlabManager {
	pub async fn reserve_lsm_output_slab_ids(&self, count: usize) -> std::io::Result<Vec<SlabId>> {
		let count = u64::try_from(count).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation count does not fit in u64",
			)
		})?;
		let mut state = self.state.lock().await;
		let start = state.next_slab_id.get();
		let end = start.checked_add(count).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"LSM output slab reservation would overflow SlabId",
			)
		})?;
		if end > start {
			self.cover_slab_ids_through(&mut state, SlabId::new(end - 1))
				.await?;
		}
		state.advance_slab_allocator(SlabId::new(end))?;
		Ok((start..end).map(SlabId::new).collect())
	}

	pub fn apply_lsm_manifest_compaction_commit(
		&self,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let post_epoch = manifest.apply_compaction_commit(commit)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		self.apply_lsm_pin_state_for_manifest_commit(commit);
		refresh_lsm_level_metrics(&snapshot);
		Ok(post_epoch)
	}

	pub(super) fn apply_lsm_pin_state_for_manifest_commit(&self, commit: &CompactionCommit) {
		for slab_id in &commit.slabs_to_mark_delete_pending {
			self.slab_pins.force_delete_pending(*slab_id);
		}
		for slab_id in &commit.slabs_to_remove {
			self.slab_pins.clear_delete_pending(*slab_id);
		}
	}

	pub fn apply_lsm_manifest_planned_compaction_commit(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		let post_epoch = manifest.apply_planned_compaction_commit(plan, commit)?;
		let snapshot = manifest.snapshot();
		drop(manifest);
		self.apply_lsm_pin_state_for_manifest_commit(commit);
		refresh_lsm_level_metrics(&snapshot);
		Ok(post_epoch)
	}

	async fn lock_lsm_slabs(
		&self,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
	) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
		let mut slabs = plan.touched_slabs();
		slabs.extend(written.iter().map(|run| run.slab_id));
		slabs.sort_unstable();
		slabs.dedup();
		let mut guards = Vec::with_capacity(slabs.len());
		for slab_id in slabs {
			guards.push(self.tier_publication_lock(slab_id).lock_owned().await);
		}
		guards
	}

	async fn begin_lsm_index_publication(
		&self,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
	) -> std::io::Result<IndexPublication> {
		if self.persistent_index.is_none() {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for LSM install",
			));
		}
		let mut slabs = plan.touched_slabs();
		slabs.extend(written.iter().map(|run| run.slab_id));
		self.begin_index_publication(slabs).await
	}

	pub async fn install_lsm_compaction_temp_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
	) -> Result<ManifestEpoch, CompactionFailure> {
		self.require_tier_authority_ready()?;
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let _locks = self.lock_lsm_slabs(plan, written).await;
		let expected = prepared
			.input_authority
			.as_ref()
			.ok_or(CompactionFailure::ManifestRace)?;
		if self
			.lsm_input_authority_locked(&plan.touched_slabs())
			.await? != *expected
		{
			return Err(CompactionFailure::ManifestRace);
		}
		self.preflight_lsm_compaction_outputs(plan, prepared, written, commit, false)?;
		// The read guard protects the snapshot; no orphan mark exists until the first durable mutation can be submitted.
		let gate = Arc::new(self.persistent_index_gate.clone().read_owned().await);
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for local LSM intent",
			)
		})?;
		let mut snapshot = index.load_snapshot().await?;
		let input_ids = expected
			.iter()
			.map(|input| input.slab_id)
			.collect::<HashSet<_>>();
		let output_ids = written
			.iter()
			.map(|run| run.slab_id)
			.collect::<HashSet<_>>();
		if snapshot
			.secondary_index_segments
			.iter()
			.any(|segment| input_ids.contains(&segment.key.source_slab_id))
		{
			return Err(CompactionFailure::SidecarDigestMismatch);
		}
		if output_ids.len() != written.len()
			|| !input_ids.is_disjoint(&output_ids)
			|| snapshot
				.lsm_runs
				.iter()
				.any(|run| output_ids.contains(&run.slab_id))
			|| snapshot
				.record_rows
				.iter()
				.any(|row| output_ids.contains(&row.location.slab_id))
			|| snapshot
				.tiered_slabs
				.iter()
				.any(|pointer| output_ids.contains(&pointer.slab_id))
		{
			return Err(CompactionFailure::ManifestRace);
		}
		// Sealing registers L0 runs in memory; materialize only these locked
		// inputs when their durable summaries do not exist yet. Do not overwrite
		// a conflicting durable run or synthesize unrelated slab authority.
		let live_manifest = self.compaction_manifest_snapshot();
		if live_manifest.manifest_epoch != commit.manifest_epoch_pre {
			return Err(CompactionFailure::ManifestRace);
		}
		let local_input_runs = live_manifest
			.runs()
			.iter()
			.filter(|run| input_ids.contains(&run.slab_id))
			.cloned()
			.collect::<Vec<_>>();
		if local_input_runs.len() != input_ids.len()
			|| local_input_runs.iter().any(|run| !run.state.is_live())
		{
			return Err(CompactionFailure::ManifestRace);
		}
		let mut missing = Vec::new();
		for run in &local_input_runs {
			match snapshot
				.lsm_runs
				.iter()
				.find(|stored| stored.slab_id == run.slab_id)
			{
				Some(stored) if stored != run => return Err(CompactionFailure::ManifestRace),
				Some(_) => {}
				None => missing.push(run.clone()),
			}
		}
		let mut publication_slabs = plan.touched_slabs();
		publication_slabs.extend(written.iter().map(|run| run.slab_id));
		if !missing.is_empty() {
			// The transaction only mirrors runs already live in memory. Once it
			// commits, nothing of it is left to publish, so a later refusal or
			// error in this install cannot orphan it.
			let publication =
				self.begin_index_publication_with_gate(publication_slabs.clone(), gate.clone())?;
			index
				.apply_with_publication_guard(
					PersistentIndexUpdate {
						put_lsm_runs: missing,
						..Default::default()
					},
					Some(publication.gate.clone()),
				)
				.await?;
			publication.complete();
			snapshot = index.load_snapshot().await?;
		}
		let local_input_rows_digest =
			crate::storage::index::lsm_input_rows_digest(&snapshot, &input_ids);
		let live_rows = self
			.persistent_record_rows_snapshot()
			.into_iter()
			.filter(|row| input_ids.contains(&row.location.slab_id));
		if crate::storage::index::lsm_rows_digest(live_rows) != local_input_rows_digest {
			return Err(CompactionFailure::ManifestRace);
		}
		let current_manifest = self.compaction_manifest_snapshot();
		if current_manifest.manifest_epoch != commit.manifest_epoch_pre
			|| !local_input_runs
				.iter()
				.all(|run| current_manifest.runs().contains(run))
		{
			return Err(CompactionFailure::ManifestRace);
		}
		let local_output_rows_digest = crate::storage::index::lsm_rows_digest(
			self.lsm_inserted_persistent_rows(prepared, written),
		);
		let mut local_input_files = Vec::new();
		for input in expected {
			if input.tiered.is_none() {
				local_input_files.push(
					lsm_file_identity(
						self.file_ops.as_ref(),
						&slab_path(&self.config.dir, input.slab_id),
						input.slab_id,
					)
					.await?,
				);
			}
		}
		let mut local_output_files = Vec::new();
		for output in written {
			local_output_files.push(
				lsm_file_identity(self.file_ops.as_ref(), &output.temp_path, output.slab_id)
					.await?,
			);
		}
		let intent = crate::storage::index::LsmInstallIntent {
			kind: crate::storage::index::LsmInstallKind::Local,
			phase: crate::storage::index::LsmInstallPhase::Prepared,
			local_input_rows_digest,
			local_output_rows_digest,
			local_input_runs,
			local_input_files,
			local_output_files,
			commit: commit.clone(),
			commit_digest: *blake3::hash(&serde_json::to_vec(commit).map_err(Error::other)?)
				.as_bytes(),
			ops: Vec::new(),
			inputs: expected.clone(),
		};
		// Register while no new mark is held across the scope-lock await. Mark
		// only immediately before the intent submission, after all
		// deterministic preflight and file checks.
		let Some(_intent_owner) = self.register_lsm_install_intent(&intent).await else {
			return Err(CompactionFailure::ManifestRace);
		};
		let publication = match self.begin_index_publication_with_gate(publication_slabs, gate) {
			Ok(publication) => publication,
			Err(error) => {
				self.clear_registered_lsm_intent(commit.job_id).await;
				_intent_owner.complete();
				return Err(error.into());
			}
		};
		self.tier_authority.notify_publication_change();
		index.put_lsm_install_intent(intent.clone()).await?;
		#[cfg(test)]
		if self.lsm_fail_before_rename.swap(false, Ordering::AcqRel) {
			return Err(Error::other("injected local LSM pre-rename failure").into());
		}
		let staged = match self
			.stage_lsm_compaction_outputs_locked(plan, prepared, written, commit, false, true)
			.await
		{
			Ok(staged) => staged,
			Err(LsmStageFailure::BeforeRename(failure)) => {
				self.clear_refused_lsm_intent(index, &intent).await?;
				_intent_owner.complete();
				publication.complete();
				return Err(failure);
			}
			Err(LsmStageFailure::Uncertain(failure)) => return Err(failure),
		};
		#[cfg(test)]
		{
			let hold = self.lsm_after_rename_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		for slab_id in plan.touched_slabs() {
			self.bump_slab_publication_revision(slab_id)?;
		}
		// Outputs reach durable authority before any input is retired. Public
		// admission remains fenced by the intent throughout the two phases.
		let post_epoch = commit.manifest_epoch_pre.saturating_add(1);
		let mut output_runs = commit.slabs_to_add.clone();
		for run in &mut output_runs {
			run.state = SlabState::Live;
			run.manifest_epoch = post_epoch;
		}
		let mut published_intent = intent.clone();
		published_intent.phase = crate::storage::index::LsmInstallPhase::OutputsPublished;
		index
			.apply_with_publication_guard(
				PersistentIndexUpdate {
					lsm_intent_transition: Some((intent.clone(), published_intent.clone())),
					put_records: staged.inserted_rows.clone(),
					put_lsm_runs: output_runs,
					..Default::default()
				},
				Some(publication.gate.clone()),
			)
			.await?;
		#[cfg(test)]
		{
			let hold = self.lsm_after_output_publish_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		let post_epoch = self.apply_lsm_manifest_planned_compaction_commit(plan, commit)?;
		let post_epoch = self
			.complete_lsm_compaction_install(staged, post_epoch, publication, None)
			.await?;
		#[cfg(test)]
		{
			let hold = self.lsm_after_install_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		index.clear_lsm_install_intent(published_intent).await?;
		self.clear_registered_lsm_intent(commit.job_id).await;
		#[cfg(test)]
		{
			let hold = self.lsm_after_intent_clear_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		_intent_owner.complete();
		self.tier_authority.notify_publication_change();
		Ok(post_epoch)
	}

	pub async fn install_lsm_compaction_temp_outputs_via_control_ring(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		committer: &dyn TierCommitter,
	) -> Result<ManifestEpoch, CompactionFailure> {
		self.install_lsm_compaction_temp_outputs_via_control_ring_with_context(
			plan, prepared, written, commit, committer, None,
		)
		.await
	}

	pub(super) async fn install_lsm_compaction_temp_outputs_via_control_ring_with_context(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<ManifestEpoch, CompactionFailure> {
		self.require_tier_authority_ready()?;
		committer.preflight(commit.owner).await?;
		#[cfg(test)]
		let precheck = !self
			.continuity_test_holds
			.skip_op_room_precheck
			.load(std::sync::atomic::Ordering::Acquire);
		#[cfg(not(test))]
		let precheck = true;
		if precheck {
			self.require_op_index()?.ensure_op_room(1).await?;
		}
		self.preflight_lsm_compaction_outputs(
			plan,
			prepared,
			written,
			commit,
			secondary_index_context.is_some(),
		)?;
		// Sidecars can be prepared from durable temporary output before the
		// intent fences public source reads and before any output rename.
		let prepared_sidecars = self
			.prepare_lsm_secondary_index_segments_for_compaction(
				plan,
				written,
				secondary_index_context,
				true,
			)
			.await?;
		self.verify_lsm_prepared_inputs(plan, prepared).await?;
		let intent = self
			.capture_control_lsm_intent(
				plan,
				prepared,
				written,
				commit,
				secondary_index_context.is_some(),
			)
			.await?;
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for LSM intent",
			)
		})?;
		let Some(_intent_owner) = self.register_lsm_install_intent(&intent).await else {
			return Err(CompactionFailure::ManifestRace);
		};
		self.tier_authority.notify_publication_change();
		// Mark before entering OwnedRedb: its blocking transaction may outlive a
		// cancelled caller. Any uncertain result retains the mark and evidence.
		// The operation and its intent commit together, so neither exists without
		// the other (ADR 0697).
		let allocation = match index
			.allocate_ops(
				vec![crate::consensus::slab_ops::SlabCommand::Compaction(
					commit.clone(),
				)],
				Some(intent),
			)
			.await
		{
			Ok(allocation) => allocation,
			// Proven: nothing was written. Only the owned runtime scope goes, so
			// the refused producer leaves no fence on reserved work. Any other
			// result may have committed and keeps the fence.
			Err(error) if crate::storage::sweeper::op_window_refused(&error) => {
				self.clear_registered_lsm_intent(commit.job_id).await;
				_intent_owner.complete();
				self.tier_authority.notify_publication_change();
				return Err(error.into());
			}
			Err(error) => return Err(error.into()),
		};
		let ticket = allocation.first;
		let intent = allocation
			.linked
			.ok_or_else(|| Error::other("an allocated LSM operation returns its intent"))?;

		#[cfg(test)]
		self.pause_control_lsm_install_for_test(ControlLsmInstallCut::BeforeRename)
			.await;
		let staged = match self
			.stage_lsm_compaction_outputs(
				plan,
				prepared,
				written,
				commit,
				secondary_index_context.is_some(),
				true,
			)
			.await
		{
			Ok(staged) => staged,
			Err(LsmStageFailure::BeforeRename(failure)) => {
				self.cancel_unsubmitted_lsm_op(index, committer, &ticket, &intent)
					.await?;
				_intent_owner.complete();
				return Err(failure);
			}
			Err(LsmStageFailure::Uncertain(failure)) => return Err(failure),
		};
		#[cfg(test)]
		{
			let hold = self.lsm_after_rename_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		#[cfg(test)]
		self.pause_control_lsm_install_for_test(ControlLsmInstallCut::AfterRename)
			.await;
		let receipt = committer.submit_op(&ticket).await?;
		if let crate::storage::sweeper::VerifiedReceipt::Rejected { reason, .. } = receipt {
			{
				let _maintenance = self
					.maintenance_publication_gate
					.clone()
					.write_owned()
					.await;
				let _locks = self.lock_lsm_slabs(plan, written).await;
				{
					let state = self.state.lock().await;
					if written.iter().any(|run| {
						state.tiered.contains_key(&run.slab_id)
							|| state.sealed.contains_key(&run.slab_id)
							|| state.active.slab_id() == run.slab_id
					}) {
						return Err(CompactionFailure::ManifestRace);
					}
				}
				// The sole submission definitely refused. Keep the intent until
				// every installed output is durably removed; cancellation or an
				// unlink error therefore leaves evidence and readiness fenced.
				for (run, _) in &staged.installed {
					let path = slab_path(&self.config.dir, run.slab_id);
					unlink_local_file(self.file_ops.as_ref(), &path).await?;
					fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
				}
				index
					.hand_off_op(ticket.key(), ticket.digest(), Some(intent))
					.await?;
				self.clear_registered_lsm_intent(commit.job_id).await;
				_intent_owner.complete();
				self.tier_authority.notify_publication_change();
			}
			return Err(Error::new(
				ErrorKind::InvalidData,
				crate::storage::sweeper::LsmCommitRejected(format!("{reason:?}")),
			)
			.into());
		}
		#[cfg(test)]
		self.pause_control_lsm_install_for_test(ControlLsmInstallCut::AfterAcceptance)
			.await;
		let completion = AcceptedControlLsmCompletion::new_after_acceptance(
			&intent,
			prepared_sidecars.is_none(),
		);
		let completed_intent = completion
			.as_ref()
			.map(AcceptedControlLsmCompletion::published_intent)
			.unwrap_or_else(|| intent.clone());
		let post_epoch = self
			.lsm_committed_manifest_epoch(commit)
			.ok_or(CompactionFailure::ManifestRace)?;
		// No tier lock spans the control await. All input and output locks are
		// reacquired in order before comparing the captured authority.
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let _locks = self.lock_lsm_slabs(plan, written).await;
		let expected = prepared
			.input_authority
			.as_ref()
			.ok_or(CompactionFailure::ManifestRace)?;
		if self
			.lsm_input_authority_locked(&plan.touched_slabs())
			.await? != *expected
		{
			return Err(CompactionFailure::ManifestRace);
		}
		let publication = self.begin_lsm_index_publication(plan, written).await?;
		for slab_id in plan.touched_slabs() {
			self.bump_slab_publication_revision(slab_id)?;
		}
		let post_epoch = self
			.complete_lsm_compaction_install(staged, post_epoch, publication, completion)
			.await?;
		self.publish_prepared_lsm_secondary_index_segments_locked(prepared_sidecars)
			.await;
		if self.index_authority_unavailable.load(Ordering::Acquire)
			|| self.has_orphaned_index_publications()
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"LSM sidecar publication remains unresolved",
			)
			.into());
		}
		#[cfg(test)]
		{
			self.pause_control_lsm_install_for_test(ControlLsmInstallCut::AfterPublication)
				.await;
			// Preserve the baseline control's exact post-install cut.
			let hold = self.lsm_after_install_hold.lock().unwrap().clone();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				let _ = release.acquire().await;
			}
		}
		index
			.hand_off_op(ticket.key(), ticket.digest(), Some(completed_intent))
			.await?;
		self.clear_registered_lsm_intent(commit.job_id).await;
		_intent_owner.complete();
		self.tier_authority.notify_publication_change();
		Ok(post_epoch)
	}

	fn preflight_lsm_compaction_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<(), CompactionFailure> {
		self.validate_lsm_written_outputs(plan, prepared, written, commit)?;
		// A rewrite never drops a copy of a conflicting version: its evidence
		// must survive for every later read to refuse.
		if prepared.decisions.iter().any(|decision| {
			matches!(
				decision,
				crate::storage::SurvivorOrDrop::Drop { record, .. }
					if self.conflicted_locations.contains(&record.location)
			)
		}) {
			return Err(CompactionFailure::IoError(stale_location_error(
				plan.touched_slabs()
					.first()
					.copied()
					.unwrap_or(SlabId::ZERO),
				StaleLocationReason::ConflictingVersion,
			)));
		}
		self.validate_lsm_sidecar_swap_readiness(plan, commit, allow_sidecar_covered_inputs)?;
		{
			let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
			manifest.validate_compaction_commit_against_plan(plan, commit)?;
		}

		Ok(())
	}

	/// Capture a Control intent's local evidence under the rename guards.
	async fn capture_control_lsm_intent(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<crate::storage::index::LsmInstallIntent, CompactionFailure> {
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let _locks = self.lock_lsm_slabs(plan, written).await;
		let inputs = prepared
			.input_authority
			.as_ref()
			.ok_or(CompactionFailure::ManifestRace)?;
		if self
			.lsm_input_authority_locked(&plan.touched_slabs())
			.await? != *inputs
		{
			return Err(CompactionFailure::ManifestRace);
		}
		self.preflight_lsm_compaction_outputs(
			plan,
			prepared,
			written,
			commit,
			allow_sidecar_covered_inputs,
		)?;
		let input_ids = inputs
			.iter()
			.map(|input| input.slab_id)
			.collect::<HashSet<_>>();
		let manifest = self.compaction_manifest_snapshot();
		let local_input_runs = manifest
			.runs()
			.iter()
			.filter(|run| input_ids.contains(&run.slab_id))
			.cloned()
			.collect::<Vec<_>>();
		if manifest.manifest_epoch != commit.manifest_epoch_pre
			|| local_input_runs.len() != input_ids.len()
			|| local_input_runs.iter().any(|run| !run.state.is_live())
		{
			return Err(CompactionFailure::ManifestRace);
		}
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for control LSM evidence",
			)
		})?;
		let gate = self.persistent_index_gate.clone().read_owned().await;
		let snapshot = index.load_snapshot().await?;
		let input_digest = crate::storage::index::lsm_input_rows_digest(&snapshot, &input_ids);
		let local_rows = self
			.persistent_record_rows_snapshot()
			.into_iter()
			.filter(|row| input_ids.contains(&row.location.slab_id));
		if crate::storage::index::lsm_rows_digest(local_rows) != input_digest {
			return Err(CompactionFailure::ManifestRace);
		}
		let mut local_input_files = Vec::new();
		for input in inputs {
			if input.tiered.is_none() {
				local_input_files.push(
					lsm_file_identity(
						self.file_ops.as_ref(),
						&slab_path(&self.config.dir, input.slab_id),
						input.slab_id,
					)
					.await?,
				);
			}
		}
		let mut local_output_files = Vec::new();
		for output in written {
			local_output_files.push(
				lsm_file_identity(self.file_ops.as_ref(), &output.temp_path, output.slab_id)
					.await?,
			);
		}
		drop(gate);
		Ok(crate::storage::index::LsmInstallIntent {
			kind: crate::storage::index::LsmInstallKind::Control,
			phase: crate::storage::index::LsmInstallPhase::Prepared,
			local_input_rows_digest: input_digest,
			local_output_rows_digest: crate::storage::index::lsm_rows_digest(
				self.lsm_inserted_persistent_rows(prepared, written),
			),
			local_input_runs,
			local_input_files,
			local_output_files,
			commit: commit.clone(),
			commit_digest: *blake3::hash(&serde_json::to_vec(commit).map_err(Error::other)?)
				.as_bytes(),
			ops: Vec::new(),
			inputs: inputs.clone(),
		})
	}

	/// Guard output rename against concurrent maintenance and tier publication.
	/// The control caller releases these guards before submitting its Raft write.
	pub(super) async fn stage_lsm_compaction_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
		keep_intent_outputs: bool,
	) -> Result<LsmInstalledCompactionOutputs, LsmStageFailure> {
		let _maintenance = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let _locks = self.lock_lsm_slabs(plan, written).await;
		let expected = prepared
			.input_authority
			.as_ref()
			.ok_or(LsmStageFailure::BeforeRename(
				CompactionFailure::ManifestRace,
			))?;
		if self
			.lsm_input_authority_locked(&plan.touched_slabs())
			.await
			.map_err(LsmStageFailure::BeforeRename)?
			!= *expected
		{
			return Err(LsmStageFailure::BeforeRename(
				CompactionFailure::ManifestRace,
			));
		}
		self.stage_lsm_compaction_outputs_locked(
			plan,
			prepared,
			written,
			commit,
			allow_sidecar_covered_inputs,
			keep_intent_outputs,
		)
		.await
	}

	/// Caller holds the maintenance gate and sorted input/output publication locks.
	async fn stage_lsm_compaction_outputs_locked(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
		keep_intent_outputs: bool,
	) -> Result<LsmInstalledCompactionOutputs, LsmStageFailure> {
		self.preflight_lsm_compaction_outputs(
			plan,
			prepared,
			written,
			commit,
			allow_sidecar_covered_inputs,
		)
		.map_err(LsmStageFailure::BeforeRename)?;

		let installed = self
			.install_lsm_output_files(written, keep_intent_outputs)
			.await?;
		let max_output_slab_id = installed.iter().map(|(run, _)| run.slab_id).max();

		let removed_rows = self.lsm_removed_persistent_rows(prepared);
		let inserted_rows = self.lsm_inserted_persistent_rows(prepared, written);
		let relocations = self.lsm_row_relocations(prepared, written);

		{
			let state = self.state.lock().await;
			for (run, _) in &installed {
				if state.sealed.contains_key(&run.slab_id)
					|| state.tiered.contains_key(&run.slab_id)
				{
					drop(state);
					if !keep_intent_outputs {
						self.cleanup_lsm_installed_outputs(&installed).await;
					}
					return Err(CompactionFailure::ManifestRace.into());
				}
			}
		}
		if let Some(max_output_slab_id) = max_output_slab_id {
			let mut state = self.state.lock().await;
			if let Err(err) = self
				.advance_active_after_lsm_outputs(&mut state, max_output_slab_id)
				.await
			{
				drop(state);
				if !keep_intent_outputs {
					self.cleanup_lsm_installed_outputs(&installed).await;
				}
				return Err(err.into());
			}
		}

		Ok(LsmInstalledCompactionOutputs {
			installed,
			removed_rows,
			inserted_rows,
			relocations,
		})
	}

	pub(super) async fn complete_lsm_compaction_install(
		&self,
		staged: LsmInstalledCompactionOutputs,
		post_epoch: ManifestEpoch,
		publication: IndexPublication,
		completion: Option<AcceptedControlLsmCompletion>,
	) -> Result<ManifestEpoch, CompactionFailure> {
		let lsm_runs = self.compaction_manifest_snapshot().runs().to_vec();
		let LsmInstalledCompactionOutputs {
			installed,
			removed_rows,
			inserted_rows,
			relocations,
		} = staged;

		// One relocation per key: every surviving input row is replaced by its
		// output in the same key critical section; dropped input rows are
		// removed there too. Readers of the index never see a surviving key
		// absent, or one version twice.
		let mut by_key: BTreeMap<TenantKey, KeyRelocation> = BTreeMap::new();
		let paired_sources = relocations
			.iter()
			.map(|relocation| relocation.old)
			.collect::<HashSet<_>>();
		for relocation in &relocations {
			by_key
				.entry((relocation.tenant_id, relocation.key_identity))
				.or_insert_with(|| KeyRelocation::new(relocation.routing_locality, None))
				.pairs
				.push(RelocationPair {
					old: relocation.old,
					new: relocation.new,
					table_object_id: relocation.table_object_id,
				});
		}
		for row in &removed_rows {
			if !paired_sources.contains(&row.location) {
				by_key
					.entry((row.tenant_id, row.key_identity))
					.or_insert_with(|| KeyRelocation::new(row.routing_locality, None))
					.removals
					.push(row.location);
			}
		}
		let mut refused_outputs = HashSet::new();
		{
			let state = self.state.lock().await;
			for (run, slab) in installed {
				state.install_sealed(&self.tier_budget, run.slab_id, slab);
			}
			let mut relocated_any = false;
			for ((tenant_id, key_identity), relocation) in &by_key {
				let mut scratch = RelocationScratch::for_relocation(relocation);
				for (_, new) in self.relocate_indexed_locations(
					*tenant_id,
					*key_identity,
					relocation,
					&mut scratch,
				) {
					refused_outputs.insert(*new);
				}
				if !relocated_any {
					relocated_any = true;
					#[cfg(test)]
					self.index_relocation_pause_point().await;
				}
			}
		}
		if !refused_outputs.is_empty() {
			// The durable intent retains both generations until recovery can
			// establish the exact source and output state.
			return Err(CompactionFailure::ManifestRace);
		}
		// The durable mirror never names an output the live index refused.
		let inserted_rows = inserted_rows
			.into_iter()
			.filter(|row| !refused_outputs.contains(&row.location))
			.collect::<Vec<_>>();

		let update = PersistentIndexUpdate {
			delete_records: removed_rows,
			put_records: inserted_rows,
			put_lsm_runs: lsm_runs,
			..Default::default()
		};
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		if let Some(completion) = completion {
			index
				.apply_with_control_lsm_completion(
					update,
					Some(publication.gate.clone()),
					completion,
				)
				.await?;
		} else {
			index
				.apply_with_publication_guard(update, Some(publication.gate.clone()))
				.await?;
		}
		publication.complete();
		Ok(post_epoch)
	}

	pub(super) fn lsm_committed_manifest_epoch(
		&self,
		commit: &CompactionCommit,
	) -> Option<ManifestEpoch> {
		let snapshot = self.compaction_manifest_snapshot();
		let successors = commit
			.slabs_to_add
			.iter()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		for slab_id in &commit.slabs_to_retire {
			let run = snapshot.run(*slab_id)?;
			if !matches!(&run.state, crate::storage::SlabState::Retired { successors: existing } if existing == &successors)
			{
				return None;
			}
		}
		for expected in &commit.slabs_to_add {
			let run = snapshot.run(expected.slab_id)?;
			if !run.state.is_live()
				|| run.compaction_level != expected.compaction_level
				|| run.min_row_hlc != expected.min_row_hlc
				|| run.max_row_hlc != expected.max_row_hlc
				|| run.size_bytes != expected.size_bytes
				|| run.row_count != expected.row_count
				|| run.sidecar_coverage != expected.sidecar_coverage
			{
				return None;
			}
		}
		Some(snapshot.manifest_epoch)
	}

	pub async fn run_lsm_compaction_once(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let started = Instant::now();
		let source_level = plan.source_level;
		let input_slabs = plan.touched_slabs().len() as u64;
		let result = self.run_lsm_compaction_once_inner(worker, plan).await;
		self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
		result
	}

	pub async fn run_lsm_compaction_once_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		self.run_lsm_compaction_once_via_control_ring_with_context(worker, plan, committer, None)
			.await
	}

	pub(super) async fn run_lsm_compaction_once_via_control_ring_with_context(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let started = Instant::now();
		let source_level = plan.source_level;
		let input_slabs = plan.touched_slabs().len() as u64;
		let result = self
			.run_lsm_compaction_once_inner_via_control_ring(
				worker,
				plan,
				committer,
				secondary_index_context,
			)
			.await;
		self.observe_lsm_compaction_result(source_level, input_slabs, started, &result);
		result
	}

	pub(super) fn observe_lsm_compaction_result(
		&self,
		source_level: CompactionLevelIndex,
		input_slabs: u64,
		started: Instant,
		result: &Result<crate::storage::lsm::CompactionOutcome, CompactionFailure>,
	) {
		match result {
			Ok(outcome) => metrics().observe_lsm_compaction(
				source_level,
				LsmCompactionResult::Ok,
				input_slabs,
				outcome.bytes_out,
				started.elapsed(),
			),
			Err(err) => {
				let metric_result = lsm_compaction_result_from_failure(err);
				if matches!(metric_result, LsmCompactionResult::VisibleDigestInvariance) {
					metrics().observe_lsm_digest_invariance_violation(
						LsmDigestInvariantKind::VisibleDomain,
					);
				}
				metrics().observe_lsm_compaction(
					source_level,
					metric_result,
					input_slabs,
					0,
					started.elapsed(),
				);
			}
		}
	}

	pub fn pick_lsm_compaction_plan(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Option<CompactionPlan> {
		// A maintenance rewrite holds this only across its durable
		// publication/install handoff. Returning no plan while that handoff is
		// in flight is preferable to planning the old Live run from the
		// pre-D3 in-memory manifest.
		if self
			.maintenance_publication_restart_required
			.load(Ordering::Acquire)
		{
			return None;
		}
		let _publication_gate = self.maintenance_publication_gate.try_read().ok()?;
		let snapshot = self.compaction_manifest_snapshot();
		// ADR 0097 decision 4: eligibility is applied DURING the search. Rejecting
		// after the choice let one quarantined slab suppress every other level's
		// work; a bounded backoff on one input must not stall all compaction.
		// One clock sample for the whole search, so every candidate is judged
		// against the same deadline boundary; admission re-samples under its lock.
		let policy = &self.maintenance_policy;
		let Some(now_ms) = policy.now_ms() else {
			policy.disable("policy clock unavailable at selection");
			return None;
		};
		let live_l0 = snapshot
			.slabs_at_compaction_level(CompactionLevelIndex::L0)
			.into_iter()
			.map(|run| run.slab_id)
			.collect::<HashSet<_>>();
		self.counter_deferred_l0_slabs
			.retain(|slab_id| live_l0.contains(slab_id));
		crate::storage::lsm::pick_compaction_where_excluding(
			&snapshot,
			&worker.config(),
			now,
			|slab_id| self.counter_deferred_l0_slabs.contains(&slab_id),
			|plan| {
				let touched = plan.touched_slabs();
				!touched
					.iter()
					.any(|slab_id| self.is_compaction_slab_quarantined(*slab_id))
					&& policy.is_eligible(MaintenanceOperation::LsmCompaction, &touched, now_ms)
			},
		)
	}

	pub async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.execute_lsm_plan_under_policy(plan, |plan| self.run_lsm_compaction_once(worker, plan))
			.await
	}

	pub async fn run_lsm_compaction_tick_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.execute_lsm_plan_under_policy(plan, |plan| {
			self.run_lsm_compaction_once_via_control_ring(worker, plan, committer)
		})
		.await
	}

	pub async fn run_lsm_compaction_tick_via_control_ring_with_catalogs(
		&self,
		worker: &LsmCompactionWorker,
		now: HybridTimestamp,
		committer: &dyn TierCommitter,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure> {
		let Some(plan) = self.pick_lsm_compaction_plan(worker, now) else {
			return Ok(None);
		};
		self.execute_lsm_plan_under_policy(plan, |plan| {
			self.run_lsm_compaction_once_via_control_ring_with_context(
				worker,
				plan,
				committer,
				Some(SecondaryIndexCompactionContext {
					table_schemas,
					secondary_indexes,
				}),
			)
		})
		.await
	}

	/// ADR 0097 decision 4: every tick funnels its chosen plan through the policy.
	///
	/// The touched set is captured BEFORE the plan is consumed by the run, and the
	/// reservation it names is held across the run. A refusal is not a failure: the
	/// tick simply had no admissible work.
	async fn execute_lsm_plan_under_policy<Fut>(
		&self,
		plan: CompactionPlan,
		run: impl FnOnce(CompactionPlan) -> Fut,
	) -> Result<Option<crate::storage::lsm::CompactionOutcome>, CompactionFailure>
	where
		Fut: std::future::Future<
				Output = Result<crate::storage::lsm::CompactionOutcome, CompactionFailure>,
			>,
	{
		let touched = plan.touched_slabs();
		match self
			.maintenance_policy
			.execute(
				MaintenanceOperation::LsmCompaction,
				&touched,
				classify_compaction_failure,
				// A committed compaction outcome IS a published rewrite.
				|_| SuccessDisposition::Rewrote,
				|| run(plan),
			)
			.await
		{
			MaintenanceAttempt::Skipped(refusal) => {
				tracing::debug!(
					slabs = ?touched,
					?refusal,
					pending_counter_frames = self.counter_pending_frame_count(),
					"LSM compaction plan not admitted this tick"
				);
				Ok(None)
			}
			MaintenanceAttempt::Completed(outcome) => Ok(Some(outcome)),
			MaintenanceAttempt::Failed(err) => Err(err),
		}
	}

	/// ADR 0097: a plan is chosen before its claims are held, so between the pick
	/// and the claim another maintenance job may have rewritten or retired one of
	/// its slabs. Re-check presence once the claims are in hand.
	///
	/// Deliberately a presence check, not `manifest_epoch` equality: any unrelated
	/// compaction anywhere bumps the epoch, and failing on that would abandon
	/// plans that are still perfectly valid. What the claim protects is that these
	/// slabs stop moving; what this checks is that they were still there when it
	/// started protecting them.
	fn revalidate_lsm_plan_after_claims(
		&self,
		plan: &CompactionPlan,
	) -> Result<(), CompactionFailure> {
		let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
		for slab_id in plan.touched_slabs() {
			// Presence alone is not enough: a RETIRED run stays in the manifest, so
			// `get_run(..).is_some()` would accept a slab another job has already
			// replaced. The plan is only still valid if every slab it names is Live
			// and still at the level the plan chose it from.
			let Some(run) = manifest.get_run(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			if !run.state.is_live() {
				return Err(CompactionFailure::ManifestRace);
			}
			let planned_level = if plan.input_slabs.contains(&slab_id) {
				plan.source_level
			} else {
				plan.target_level
			};
			if run.compaction_level != planned_level {
				return Err(CompactionFailure::ManifestRace);
			}
		}
		Ok(())
	}

	/// Multi-source LSM rewriting is still classless. Inspect every touched local
	/// slab after exclusive claims and plan revalidation, but before output ids or
	/// temporary files are reserved. A counter-bearing (or unavailable) backing
	/// gets a typed retryable deferral; the original plan and source bytes stay
	/// untouched.
	async fn refuse_lsm_counter_inputs(
		&self,
		plan: &CompactionPlan,
	) -> Result<(), CompactionFailure> {
		for slab_id in plan.touched_slabs() {
			if self.counter_slab_has_pending_frames(slab_id) {
				self.note_lsm_counter_deferral(plan, slab_id);
				return Err(CompactionFailure::CounterMaintenanceSkipped);
			}
			match self.resolve_backing(slab_id).await {
				SlabBacking::Local(slab) => {
					let scanned = match slab.scan_counter_frames().await {
						Ok(scanned) => scanned,
						Err(_error) if self.counter_slab_has_pending_frames(slab_id) => {
							self.note_lsm_counter_deferral(plan, slab_id);
							return Err(CompactionFailure::CounterMaintenanceSkipped);
						}
						Err(error) => return Err(error.into()),
					};
					if self.counter_slab_has_pending_frames(slab_id) || !scanned.is_empty() {
						self.note_lsm_counter_deferral(plan, slab_id);
						return Err(CompactionFailure::CounterMaintenanceSkipped);
					}
				}
				// Blob-tier recovery rejects record classes 1..=4 in
				// recover_slab_entries_from_blob, so an accepted L2 slab is counter-free.
				SlabBacking::L2(_) => {}
				SlabBacking::Missing => return Err(CompactionFailure::ManifestRace),
			}
		}
		Ok(())
	}

	fn note_lsm_counter_deferral(&self, plan: &CompactionPlan, slab_id: SlabId) {
		if plan.source_level == CompactionLevelIndex::L0 && plan.input_slabs.contains(&slab_id) {
			self.counter_deferred_l0_slabs.insert(slab_id);
		}
		tracing::debug!(
			slab = %slab_id,
			deferred_l0 = plan.source_level == CompactionLevelIndex::L0,
			pending_counter_frames = self.counter_pending_frame_count(),
			"LSM counter-bearing source deferred"
		);
	}

	pub(super) async fn run_lsm_compaction_once_inner(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let _pins = worker.acquire_plan_pins(&plan)?;
		// ADR 0097: the pins above only stop the slabs being DELETED; they are
		// shared, so another maintenance job could still rewrite one underneath
		// this read. Take the exclusive claims over every touched slab before any
		// read and hold them through publication, then revalidate - the plan was
		// picked before the claims existed.
		let _claims = self.acquire_maintenance_claims(&plan.touched_slabs()).await;
		self.revalidate_lsm_plan_after_claims(&plan)?;
		self.refuse_lsm_counter_inputs(&plan).await?;
		let prepared = self.prepare_lsm_compaction_outputs(worker, &plan).await?;
		let source_frames = self.read_lsm_single_row_source_frames(&plan).await?;
		self.verify_lsm_prepared_inputs(&plan, &prepared).await?;
		let written = self
			.write_lsm_compaction_temp_outputs(&plan, &prepared, &source_frames)
			.await?;
		let digest_summary = match self
			.lsm_single_row_bridge_digest_summary(&plan, &prepared, &written, &source_frames)
			.await
		{
			Ok(digest_summary) => digest_summary,
			Err(err) => {
				self.cleanup_lsm_written_outputs(&written).await;
				return Err(err);
			}
		};
		let commit = worker.build_written_compaction_commit(
			self.owner_ref()?,
			&plan,
			&written,
			digest_summary,
		);
		let post_epoch = match self
			.install_lsm_compaction_temp_outputs(&plan, &prepared, &written, &commit)
			.await
		{
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				if !self.lsm_install_unresolved.contains_key(&plan.job_id) {
					self.cleanup_lsm_written_outputs(&written).await;
				}
				return Err(err);
			}
		};
		Ok(worker.compaction_outcome_from_commit(&plan, &prepared, &commit, post_epoch))
	}

	pub(super) async fn run_lsm_compaction_once_inner_via_control_ring(
		&self,
		worker: &LsmCompactionWorker,
		plan: CompactionPlan,
		committer: &dyn TierCommitter,
		secondary_index_context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<crate::storage::lsm::CompactionOutcome, CompactionFailure> {
		let _pins = worker.acquire_plan_pins(&plan)?;
		// ADR 0097: exclusive claims over every touched slab, held for the whole
		// job. See the sibling entry point above for why the shared pins are not
		// enough on their own.
		let _claims = self.acquire_maintenance_claims(&plan.touched_slabs()).await;
		self.revalidate_lsm_plan_after_claims(&plan)?;
		self.refuse_lsm_counter_inputs(&plan).await?;
		let table_schemas = secondary_index_context
			.as_ref()
			.map(|context| context.table_schemas);
		let prepared = match secondary_index_context {
			Some(context) => {
				self.prepare_lsm_compaction_outputs_with_table_schemas(
					worker,
					&plan,
					context.table_schemas,
				)
				.await?
			}
			None => self.prepare_lsm_compaction_outputs(worker, &plan).await?,
		};
		let source_frames = self.read_lsm_single_row_source_frames(&plan).await?;
		self.verify_lsm_prepared_inputs(&plan, &prepared).await?;
		let written = self
			.write_lsm_compaction_temp_outputs_with_table_schemas(
				&plan,
				&prepared,
				&source_frames,
				table_schemas,
			)
			.await?;
		let digest_summary = match self
			.lsm_single_row_bridge_digest_summary(&plan, &prepared, &written, &source_frames)
			.await
		{
			Ok(digest_summary) => digest_summary,
			Err(err) => {
				self.cleanup_lsm_written_outputs(&written).await;
				return Err(err);
			}
		};
		let commit = worker.build_written_compaction_commit(
			self.owner_ref()?,
			&plan,
			&written,
			digest_summary,
		);
		let post_epoch = match self
			.install_lsm_compaction_temp_outputs_via_control_ring_with_context(
				&plan,
				&prepared,
				&written,
				&commit,
				committer,
				secondary_index_context,
			)
			.await
		{
			Ok(post_epoch) => post_epoch,
			Err(err) => {
				if !self.lsm_install_unresolved.contains_key(&plan.job_id) {
					self.cleanup_lsm_written_outputs(&written).await;
				}
				return Err(err);
			}
		};
		Ok(worker.compaction_outcome_from_commit(&plan, &prepared, &commit, post_epoch))
	}

	/// Verify the bytes actually written to each temp output. Frames are read
	/// once per physical record, and only one decoded source/output pair is
	/// retained at a time so multi-row output does not multiply frame memory.
	async fn verify_lsm_written_rows(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		source_frames: &[LsmSourceFrame],
	) -> Result<(), CompactionFailure> {
		let sources = source_frames
			.iter()
			.map(|source| {
				(
					LsmSourceFrameKey::from(source.entry.location),
					source.frame.clone(),
				)
			})
			.collect::<HashMap<_, _>>();
		if prepared.output_runs.len() != written.len() {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		for (planned, output) in prepared.output_runs.iter().zip(written) {
			if planned.slab_id != output.slab_id || planned.rows.len() != output.locations.len() {
				return Err(CompactionFailure::VisibleDigestInvariance);
			}
			let file = self
				.file_ops
				.open(
					&output.temp_path,
					OpenFlags {
						read: true,
						direct: true,
						..Default::default()
					},
				)
				.await?;
			let mut groups: BTreeMap<(u64, u32), Vec<usize>> = BTreeMap::new();
			for (index, location) in output.locations.iter().enumerate() {
				groups
					.entry((location.offset, location.length))
					.or_default()
					.push(index);
			}
			for indices in groups.values() {
				let first = indices[0];
				let source_key = LsmSourceFrameKey::from(planned.rows[first].location);
				let source = sources
					.get(&source_key)
					.ok_or(CompactionFailure::VisibleDigestInvariance)?;
				let (header, frame) = crate::storage::core::read_verified_slab_frame(
					self.file_ops.as_ref(),
					&file,
					output.locations[first],
				)
				.await?;
				let source_entries = storage_multirow_index_entries_from_frame(source)?;
				let output_entries = storage_multirow_index_entries_from_frame(&frame)?;
				match (source_entries, output_entries) {
					(Some(source_entries), Some(output_entries)) => {
						let source_batch = decode_record_batch(source)?;
						let output_batch = decode_record_batch(&frame)?;
						for &index in indices {
							let record = &planned.rows[index];
							let location = output.locations[index];
							if LsmSourceFrameKey::from(record.location) != source_key
								|| header.tenant_id != record.tenant_id
								|| location.slab_id != output.slab_id
								|| location.hlc != record.row_hlc
								|| location.is_tombstone != record.location.is_tombstone
								|| location.placement != record.location.placement
								|| location.partition != record.location.partition
							{
								return Err(CompactionFailure::VisibleDigestInvariance);
							}
							let old = source_entries
								.iter()
								.find(|entry| entry.record_row_index == record.record_row_index)
								.ok_or(CompactionFailure::VisibleDigestInvariance)?;
							let new = output_entries
								.iter()
								.find(|entry| entry.record_row_index == location.record_row_index)
								.ok_or(CompactionFailure::VisibleDigestInvariance)?;
							if !lsm_multirow_index_entry_matches_version(old, record)
								|| !lsm_multirow_index_entry_matches_version_ignoring_position(
									new, record,
								) || old.routing_locality != new.routing_locality
								|| old.partition != new.partition
								|| storage_row_table_object_id_at(
									&source_batch,
									record.record_row_index,
								)? != storage_row_table_object_id_at(
									&output_batch,
									location.record_row_index,
								)? || materialize_record_location_batch(
								&source_batch,
								record.record_row_index,
							)? != materialize_record_location_batch(
								&output_batch,
								location.record_row_index,
							)? {
								return Err(CompactionFailure::VisibleDigestInvariance);
							}
						}
					}
					(None, None) => {
						if indices.len() != 1 {
							return Err(CompactionFailure::VisibleDigestInvariance);
						}
						let record = &planned.rows[first];
						let location = output.locations[first];
						if header.tenant_id != record.tenant_id
							|| header.key_identity != record.row_key_id
							|| header.hlc != record.row_hlc
							|| location.slab_id != output.slab_id
							|| location.hlc != record.row_hlc
							|| location.is_tombstone != record.location.is_tombstone
							|| location.placement != record.location.placement
							|| location.partition != record.location.partition
							|| source.as_slice() != frame.as_slice()
						{
							return Err(CompactionFailure::VisibleDigestInvariance);
						}
					}
					_ => return Err(CompactionFailure::VisibleDigestInvariance),
				}
			}
		}
		Ok(())
	}

	pub(super) async fn lsm_single_row_bridge_digest_summary(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		source_frames: &[LsmSourceFrame],
	) -> Result<CompactionDigestSummary, CompactionFailure> {
		let survivors = survivor_records(&prepared.decisions);
		let output_rows = prepared
			.output_runs
			.iter()
			.flat_map(|run| run.rows.iter().cloned())
			.collect::<Vec<_>>();
		let output_location_count = written.iter().map(|run| run.locations.len()).sum::<usize>();
		if survivors != output_rows || output_rows.len() != output_location_count {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		// Independently derive the visible domain from the original input and
		// the actual output using the very same floor/predicate as the planner.
		// Keep the simple above-floor invariant explicit: no planner decision
		// may claim that a row visible at or above the floor can be dropped.
		if prepared.decisions.iter().any(|decision| {
			matches!(decision,
			crate::storage::SurvivorOrDrop::Drop { record, .. }
			if record.row_hlc >= plan.retention_floor_snapshot)
		}) {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		let input_rows = prepared
			.decisions
			.iter()
			.map(|decision| match decision {
				crate::storage::SurvivorOrDrop::Survivor(record)
				| crate::storage::SurvivorOrDrop::Drop { record, .. } => record.clone(),
			})
			.collect::<Vec<_>>();
		// Source frames are read independently from every row in the touched
		// slabs. Require one decision per source before trusting those decisions
		// as the input domain; lifecycle is assigned later by schema resolution.
		let mut source_rows = HashMap::with_capacity(source_frames.len());
		for source in source_frames {
			if source_rows
				.insert(source.entry.location, &source.entry)
				.is_some()
			{
				return Err(CompactionFailure::VisibleDigestInvariance);
			}
		}
		for record in &input_rows {
			let Some(source) = source_rows.remove(&record.location) else {
				return Err(CompactionFailure::VisibleDigestInvariance);
			};
			let mut resolved = source.clone();
			resolved.lifecycle = record.lifecycle;
			if resolved != *record
				|| resolved.location.placement != record.location.placement
				|| resolved.location.partition != record.location.partition
			{
				return Err(CompactionFailure::VisibleDigestInvariance);
			}
		}
		if !source_rows.is_empty() {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		// A tombstone paired with an ambiguous or different exact lifecycle
		// cannot prove which versions it covers. Preserve that entire row group.
		// Migrated-range markers have independent elision semantics and are not
		// ordinary row tombstones; they do not participate in this test.
		let mut group_evidence = BTreeMap::new();
		for record in &input_rows {
			if record.lifecycle == VersionLifecycle::MigratedRangeTombstone {
				continue;
			}
			let evidence = group_evidence
				.entry((record.tenant_id, record.row_key_id))
				.or_insert((None, false, false));
			evidence.2 |= record.lifecycle == VersionLifecycle::AmbiguousTable
				|| evidence.0.is_some_and(|first| first != record.lifecycle);
			evidence.0.get_or_insert(record.lifecycle);
			evidence.1 |= record.location.is_tombstone;
		}
		let mut pre_decisions = apply_elision(input_rows, plan.retention_floor_snapshot);
		for decision in &mut pre_decisions {
			if let crate::storage::SurvivorOrDrop::Drop { record, .. } = decision {
				if record.lifecycle != VersionLifecycle::MigratedRangeTombstone
					&& group_evidence
						.get(&(record.tenant_id, record.row_key_id))
						.is_some_and(|e| e.1 && e.2)
				{
					*decision = crate::storage::SurvivorOrDrop::Survivor(record.clone());
				}
			}
		}
		let pre_visible = survivor_records(&pre_decisions);
		let mut post_decisions = apply_elision(output_rows, plan.retention_floor_snapshot);
		for decision in &mut post_decisions {
			if let crate::storage::SurvivorOrDrop::Drop { record, .. } = decision {
				if record.lifecycle != VersionLifecycle::MigratedRangeTombstone
					&& group_evidence
						.get(&(record.tenant_id, record.row_key_id))
						.is_some_and(|e| e.1 && e.2)
				{
					*decision = crate::storage::SurvivorOrDrop::Survivor(record.clone());
				}
			}
		}
		let post_visible = survivor_records(&post_decisions);
		if pre_visible != post_visible {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		self.verify_lsm_written_rows(prepared, written, source_frames)
			.await?;
		let table_by_location = self.lsm_source_frame_table_map(source_frames)?;
		let pre_rows = pre_visible
			.iter()
			.map(|record| self.lsm_visible_domain_row_for_record(record, &table_by_location))
			.collect::<Result<Vec<_>, _>>()?;
		let post_rows = post_visible
			.iter()
			.map(|record| self.lsm_visible_domain_row_for_record(record, &table_by_location))
			.collect::<Result<Vec<_>, _>>()?;
		let digest_summary = compaction_visible_domain_summary(pre_rows, post_rows);
		if digest_summary.visible_pre != digest_summary.visible_post {
			return Err(CompactionFailure::VisibleDigestInvariance);
		}
		Ok(digest_summary)
	}

	pub(super) fn lsm_source_frame_table_map(
		&self,
		source_frames: &[LsmSourceFrame],
	) -> Result<HashMap<RecordLocation, TableObjectId>, CompactionFailure> {
		let mut table_by_location = HashMap::with_capacity(source_frames.len());
		for source in source_frames {
			table_by_location.insert(
				source.entry.location,
				self.lsm_source_frame_table_object_id(source)?,
			);
		}
		Ok(table_by_location)
	}

	pub(super) fn lsm_source_frame_table_object_id(
		&self,
		source: &LsmSourceFrame,
	) -> Result<TableObjectId, CompactionFailure> {
		let batch = match decode_record_batch(&source.frame) {
			Ok(batch) => batch,
			Err(_) => return Ok(TableObjectId::DEFAULT_KV),
		};
		match storage_row_table_object_id_at(&batch, source.entry.record_row_index)? {
			Some(table_object_id) => Ok(table_object_id),
			None => Ok(TableObjectId::DEFAULT_KV),
		}
	}

	pub(super) fn lsm_visible_domain_row_for_record(
		&self,
		record: &VersionEntry,
		table_by_location: &HashMap<RecordLocation, TableObjectId>,
	) -> Result<VisibleDomainRow, CompactionFailure> {
		let Some(table_object_id) = table_by_location.get(&record.location).copied() else {
			return Err(CompactionFailure::VisibleDigestInvariance);
		};
		Ok(VisibleDomainRow {
			tenant_id: record.tenant_id,
			table_object_id,
			row_key_id: record.row_key_id,
			row_hlc: record.row_hlc,
			is_tombstone: record.location.is_tombstone,
		})
	}

	pub(super) fn validate_lsm_written_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
		commit: &CompactionCommit,
	) -> Result<(), CompactionFailure> {
		if prepared.output_runs.len() != written.len()
			|| written.len() != commit.slabs_to_add.len()
			|| commit.job_id != plan.job_id
			|| commit.manifest_epoch_pre != plan.manifest_epoch_pre
			|| commit.has_lsm_gc_lifecycle_fields()
		{
			return Err(CompactionFailure::ManifestRace);
		}
		for ((planned, written), committed) in prepared
			.output_runs
			.iter()
			.zip(written)
			.zip(&commit.slabs_to_add)
		{
			if planned.slab_id != written.slab_id
				|| planned.slab_id != committed.slab_id
				|| planned.compaction_level != plan.target_level
				|| written.compaction_level != plan.target_level
				|| committed.compaction_level != plan.target_level
				|| planned.rows.len() != written.locations.len()
				|| committed.row_count != written.summary.row_count
				|| committed.size_bytes != written.summary.size_bytes
				|| committed.min_row_hlc != written.summary.min_row_hlc
				|| committed.max_row_hlc != written.summary.max_row_hlc
			{
				return Err(CompactionFailure::ManifestRace);
			}
		}
		Ok(())
	}

	pub(super) fn validate_lsm_sidecar_swap_readiness(
		&self,
		plan: &CompactionPlan,
		commit: &CompactionCommit,
		allow_sidecar_covered_inputs: bool,
	) -> Result<(), CompactionFailure> {
		if !commit.sidecar_swaps.is_empty() {
			return Err(CompactionFailure::SidecarDigestMismatch);
		}
		let manifest = recover_rwlock_read(&self.compaction_manifest, "compaction_manifest");
		for slab_id in plan.touched_slabs() {
			let Some(run) = manifest.get_run(slab_id) else {
				return Err(CompactionFailure::ManifestRace);
			};
			let has_sidecar_segments = !self
				.list_secondary_index_segments_for_source_slab(slab_id)
				.is_empty();
			if !allow_sidecar_covered_inputs
				&& (run.sidecar_coverage != crate::storage::SidecarCoverage::None
					|| has_sidecar_segments)
			{
				return Err(CompactionFailure::SidecarDigestMismatch);
			}
		}
		Ok(())
	}

	pub(super) async fn install_lsm_output_files(
		&self,
		written: &[LsmWrittenOutputRun],
		keep_intent_outputs: bool,
	) -> std::io::Result<Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>> {
		let mut final_paths = Vec::with_capacity(written.len());
		for run in written {
			let final_path = slab_path(&self.config.dir, run.slab_id);
			if self.file_ops.file_len(&final_path).await?.is_some() {
				return Err(Error::new(
					ErrorKind::AlreadyExists,
					format!(
						"LSM output slab path already exists: {}",
						final_path.display()
					),
				));
			}
			final_paths.push(final_path);
		}

		let mut installed_paths = Vec::with_capacity(written.len());
		let install_result: std::io::Result<Vec<(LsmWrittenOutputRun, Arc<LocalSlabStore>)>> =
			async {
				let mut installed = Vec::with_capacity(written.len());
				for (run, final_path) in written.iter().zip(&final_paths) {
					self.file_ops.rename(&run.temp_path, final_path).await?;
					fsync_parent_dir(self.file_ops.as_ref(), final_path).await?;
					installed_paths.push(final_path.clone());
					let slab = Arc::new(
						LocalSlabStore::open(
							final_path,
							run.slab_id,
							self.file_ops.clone(),
							self.config.group_commit.clone(),
						)
						.await?,
					);
					installed.push((run.clone(), slab));
				}
				Ok(installed)
			}
			.await;

		match install_result {
			Ok(installed) => Ok(installed),
			Err(err) => {
				if !keep_intent_outputs {
					for path in installed_paths {
						let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
					}
				}
				Err(err)
			}
		}
	}

	pub(super) async fn cleanup_lsm_written_outputs(&self, written: &[LsmWrittenOutputRun]) {
		for run in written {
			let _ = unlink_local_file(self.file_ops.as_ref(), &run.temp_path).await;
		}
	}

	pub(super) async fn cleanup_lsm_installed_outputs(
		&self,
		installed: &[(LsmWrittenOutputRun, Arc<LocalSlabStore>)],
	) {
		for (run, _) in installed {
			let final_path = slab_path(&self.config.dir, run.slab_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &final_path).await;
		}
	}

	pub(super) fn lsm_removed_persistent_rows(
		&self,
		prepared: &LsmPreparedOutputs,
	) -> Vec<PersistentRecordRow> {
		let mut rows = prepared
			.decisions
			.iter()
			.map(|decision| match decision {
				crate::storage::SurvivorOrDrop::Survivor(record) => record,
				crate::storage::SurvivorOrDrop::Drop { record, .. } => record,
			})
			.map(|record| PersistentRecordRow {
				tenant_id: record.tenant_id,
				key_identity: record.row_key_id,
				routing_locality: self
					.routing_locality_for_key(record.tenant_id, record.row_key_id),
				table_object_id: record.table_object_id,
				// ADR 0090: the row's durable placement travels with its
				// location; a maintenance rewrite must never downgrade it.
				placement: record.location.placement,
				partition: record.location.partition,
				location: record.location,
			})
			.collect::<Vec<_>>();
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	/// The exact `(source, output)` pair of every surviving row, in the same
	/// planned-row/written-location correspondence as the inserted rows.
	pub(super) fn lsm_row_relocations(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
	) -> Vec<LsmRowRelocation> {
		let mut relocations = Vec::new();
		for (planned, written) in prepared.output_runs.iter().zip(written) {
			for (record, location) in planned.rows.iter().zip(&written.locations) {
				relocations.push(LsmRowRelocation {
					tenant_id: record.tenant_id,
					key_identity: record.row_key_id,
					routing_locality: self
						.routing_locality_for_key(record.tenant_id, record.row_key_id),
					table_object_id: record.table_object_id,
					old: record.location,
					new: RecordLocation {
						placement: record.location.placement,
						partition: record.location.partition,
						..*location
					},
				});
			}
		}
		relocations
	}

	pub(super) fn lsm_inserted_persistent_rows(
		&self,
		prepared: &LsmPreparedOutputs,
		written: &[LsmWrittenOutputRun],
	) -> Vec<PersistentRecordRow> {
		let mut rows = Vec::new();
		for (planned, written) in prepared.output_runs.iter().zip(written) {
			for (record, location) in planned.rows.iter().zip(&written.locations) {
				rows.push(PersistentRecordRow {
					tenant_id: record.tenant_id,
					key_identity: record.row_key_id,
					routing_locality: self
						.routing_locality_for_key(record.tenant_id, record.row_key_id),
					table_object_id: record.table_object_id,
					// ADR 0090: the LSM output re-homes the bytes, not the
					// placement; carry it from the planned row into the new
					// location so the rewrite cannot downgrade it.
					placement: record.location.placement,
					partition: record.location.partition,
					location: RecordLocation {
						placement: record.location.placement,
						partition: record.location.partition,
						..*location
					},
				});
			}
		}
		rows.sort_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		rows
	}

	pub(super) async fn advance_active_after_lsm_outputs(
		&self,
		state: &mut ManagerState,
		max_output_slab_id: SlabId,
	) -> std::io::Result<()> {
		if state.active.slab_id() > max_output_slab_id {
			return Ok(());
		}
		let next_id = if state.next_slab_id <= max_output_slab_id {
			max_output_slab_id.saturating_add(1)
		} else {
			state.next_slab_id
		};
		let next_end = self.cover_slab_ids_through(state, next_id).await?;
		let next_path = slab_path(&self.config.dir, next_id);
		let next_slab = Arc::new(
			LocalSlabStore::open(
				&next_path,
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let sealed_slab = std::mem::replace(&mut state.active, next_slab);
		let sealed_id = sealed_slab.slab_id();
		let sealed_size = sealed_slab.write_cursor();
		if sealed_size == 0 {
			// LSM output installation can advance past an active slab that has
			// never received a write. It is not a manifest run and therefore
			// cannot be retired by LSM GC; remove the file at the producer so
			// empty maintenance rounds do not accumulate forever.
			if let Err(error) = unlink_local_file(
				self.file_ops.as_ref(),
				&slab_path(&self.config.dir, sealed_id),
			)
			.await
			{
				// Keep the state coherent after the active replacement if the
				// producer cannot remove the orphan. The caller sees the error and
				// cleans up newly installed output files, but does not retry this
				// unlink; retaining the slab keeps it visible to this process's
				// recovery state and leaves the old one-file leak explicit.
				state.install_sealed(&self.tier_budget, sealed_id, sealed_slab);
				state.advance_slab_allocator(next_end)?;
				return Err(error);
			}
		} else {
			state.install_sealed(&self.tier_budget, sealed_id, sealed_slab);
			self.record_l0_compaction_run(sealed_id, sealed_size);
		}
		state.advance_slab_allocator(next_end)?;
		Ok(())
	}

	pub fn lsm_version_entries_for_plan(
		&self,
		plan: &CompactionPlan,
	) -> std::io::Result<Vec<VersionEntry>> {
		self.require_tier_authority_ready()?;
		let touched_slabs = plan.touched_slabs().into_iter().collect::<BTreeSet<_>>();
		let mut entries = Vec::new();
		for entry in self.key_index.iter() {
			let (tenant_id, row_key_id) = *entry.key();
			for location in entry.value().iter().copied() {
				if !touched_slabs.contains(&location.slab_id) {
					continue;
				}
				entries.push(VersionEntry {
					tenant_id,
					table_object_id: self.published_table_object_id(location)?,
					row_key_id,
					row_hlc: location.hlc,
					record_row_index: location.record_row_index,
					location,
					lifecycle: VersionLifecycle::AmbiguousTable,
				});
			}
		}
		entries.sort_by_key(VersionEntry::merge_key);
		Ok(entries)
	}

	pub(super) async fn lsm_version_entries_for_plan_with_table_schemas(
		&self,
		plan: &CompactionPlan,
		table_schemas: &TableSchemaCatalog,
	) -> Result<Vec<VersionEntry>, CompactionFailure> {
		let mut entries = self.lsm_version_entries_for_plan(plan)?;
		for entry in &mut entries {
			entry.lifecycle = self
				.lsm_version_lifecycle_for_entry(entry, table_schemas)
				.await?;
		}
		Ok(entries)
	}

	pub(super) async fn lsm_version_lifecycle_for_entry(
		&self,
		entry: &VersionEntry,
		table_schemas: &TableSchemaCatalog,
	) -> Result<VersionLifecycle, CompactionFailure> {
		let frame = match self
			.read_with_reference_origin(entry.location, PublishedReferenceOrigin::Compaction)
			.await
		{
			Ok(frame) => frame,
			Err(err) if err.kind() == ErrorKind::InvalidData => {
				return Ok(VersionLifecycle::AmbiguousTable);
			}
			Err(err) => return Err(err.into()),
		};
		let batch = match decode_record_batch(&frame) {
			Ok(batch) => batch,
			Err(_) => return Ok(VersionLifecycle::AmbiguousTable),
		};
		let Some(table_object_id) =
			storage_row_table_object_id_at(&batch, entry.location.record_row_index)?
		else {
			return Ok(VersionLifecycle::AmbiguousTable);
		};
		let Some(table) = table_schemas.latest_by_id(entry.tenant_id, table_object_id) else {
			return Ok(VersionLifecycle::AmbiguousTable);
		};
		if entry.location.is_tombstone {
			// A marker has no user columns, so descriptor matching cannot classify
			// it. Require the indexed identity and table attribution to agree with
			// the validated marker frame before assigning an exact lifecycle.
			if entry.table_object_id != Some(table_object_id)
				|| entry.row_hlc != entry.location.hlc
				|| entry.record_row_index != entry.location.record_row_index
			{
				return Ok(VersionLifecycle::AmbiguousTable);
			}
			let verified = match self
				.read_version(entry.tenant_id, entry.row_key_id, entry.location)
				.await
			{
				Ok(frame) => frame,
				Err(err) if err.kind() == ErrorKind::InvalidData => {
					return Ok(VersionLifecycle::AmbiguousTable);
				}
				Err(err) => return Err(err.into()),
			};
			if verified != frame {
				return Ok(VersionLifecycle::AmbiguousTable);
			}
			let Some(entries) = storage_multirow_index_entries_from_frame(&verified)? else {
				return Ok(VersionLifecycle::AmbiguousTable);
			};
			let Some(marker) = lsm_multirow_index_entry_for_version(&entries, entry) else {
				return Ok(VersionLifecycle::AmbiguousTable);
			};
			let placement = if marker.partition.is_some() {
				PlacementSemantics::Partitioned
			} else {
				PlacementSemantics::RawLocality
			};
			if marker.routing_locality
				!= self.routing_locality_for_key(entry.tenant_id, entry.row_key_id)
				|| marker.partition != entry.location.partition
				|| placement != entry.location.placement
			{
				return Ok(VersionLifecycle::AmbiguousTable);
			}
			return Ok(match table.row_identity_policy {
				RowIdentityPolicy::PrimaryKey(_) => VersionLifecycle::PrimaryKey,
				RowIdentityPolicy::AppendOnly => VersionLifecycle::AppendOnly,
				RowIdentityPolicy::AppendOnlyContentHash => VersionLifecycle::AppendOnlyContentHash,
			});
		}
		let row_batch = materialize_record_location_batch(&batch, entry.location.record_row_index)?;
		let routing_locality = self.routing_locality_for_key(entry.tenant_id, entry.row_key_id);
		if !descriptor_matches_tiered_row(
			table,
			entry.tenant_id,
			entry.row_key_id,
			routing_locality,
			Some(table_object_id),
			&row_batch,
		)? {
			return Ok(VersionLifecycle::AmbiguousTable);
		}
		Ok(match table.row_identity_policy {
			RowIdentityPolicy::PrimaryKey(_) => VersionLifecycle::PrimaryKey,
			RowIdentityPolicy::AppendOnly => VersionLifecycle::AppendOnly,
			RowIdentityPolicy::AppendOnlyContentHash => VersionLifecycle::AppendOnlyContentHash,
		})
	}

	/// The caller holds the maintenance gate followed by every input's tier
	/// lock in ascending order. Active and non-quiescent local slabs cannot be
	/// captured: a reservation might still publish after our snapshot.
	async fn lsm_input_authority_locked(
		&self,
		slabs: &[SlabId],
	) -> Result<Vec<crate::storage::index::LsmInputAuthority>, CompactionFailure> {
		let state = self.state.lock().await;
		let mut inputs = Vec::with_capacity(slabs.len());
		for &slab_id in slabs {
			if self.tier_authority.pending.contains_key(&slab_id)
				|| self.unverified_tier_slabs.contains(&slab_id)
				|| self.unapplied_tier_pointers.contains_key(&slab_id)
				|| self
					.pending_index_publications
					.iter()
					.any(|entry| entry.key().0 == slab_id)
			{
				return Err(CompactionFailure::ManifestRace);
			}
			let tiered = state
				.tiered
				.get(&slab_id)
				.map(|entry| entry.value().clone());
			if tiered.is_none()
				&& !state
					.sealed
					.get(&slab_id)
					.is_some_and(|slab| slab.manager_appends_quiescent())
			{
				return Err(CompactionFailure::ManifestRace);
			}
			let revision = self
				.slab_publication_revisions
				.get(&slab_id)
				.map(|entry| *entry.value())
				.unwrap_or(0);
			inputs.push(crate::storage::index::LsmInputAuthority {
				slab_id,
				revision,
				tiered,
			});
		}
		Ok(inputs)
	}

	async fn capture_lsm_input_authority(
		&self,
		plan: &CompactionPlan,
	) -> Result<Vec<crate::storage::index::LsmInputAuthority>, CompactionFailure> {
		let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
		let slabs = plan.touched_slabs();
		let mut locks = Vec::with_capacity(slabs.len());
		for &slab_id in &slabs {
			locks.push(self.tier_publication_lock(slab_id).lock_owned().await);
		}
		self.lsm_input_authority_locked(&slabs).await
	}

	async fn verify_lsm_prepared_inputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
	) -> Result<(), CompactionFailure> {
		let Some(expected) = &prepared.input_authority else {
			return Err(CompactionFailure::ManifestRace);
		};
		if self.capture_lsm_input_authority(plan).await? != *expected {
			return Err(CompactionFailure::ManifestRace);
		}
		Ok(())
	}

	#[cfg(test)]
	async fn pause_lsm_input_capture_for_test(&self) {
		let hold = self.lsm_capture_hold.lock().unwrap().clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub async fn prepare_lsm_compaction_outputs(
		&self,
		worker: &LsmCompactionWorker,
		plan: &CompactionPlan,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let authority = self.capture_lsm_input_authority(plan).await?;
		#[cfg(test)]
		self.pause_lsm_input_capture_for_test().await;
		let entries = self.lsm_version_entries_for_plan(plan)?;
		let target_size = worker
			.config()
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(entries.clone(), plan.retention_floor_snapshot);
		let output_run_count =
			required_output_run_count(survivor_records(&decisions), target_size)?;
		let output_slab_ids = self.reserve_lsm_output_slab_ids(output_run_count).await?;
		let mut prepared = worker.plan_survivor_outputs(plan, entries, output_slab_ids)?;
		if self.capture_lsm_input_authority(plan).await? != authority {
			return Err(CompactionFailure::ManifestRace);
		}
		prepared.input_authority = Some(authority);
		Ok(prepared)
	}

	pub(super) async fn prepare_lsm_compaction_outputs_with_table_schemas(
		&self,
		worker: &LsmCompactionWorker,
		plan: &CompactionPlan,
		table_schemas: &TableSchemaCatalog,
	) -> Result<LsmPreparedOutputs, CompactionFailure> {
		let authority = self.capture_lsm_input_authority(plan).await?;
		#[cfg(test)]
		self.pause_lsm_input_capture_for_test().await;
		let entries = self
			.lsm_version_entries_for_plan_with_table_schemas(plan, table_schemas)
			.await?;
		let target_size = worker
			.config()
			.output_run_target_bytes(plan.target_level)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"invalid compaction output target level",
				)
			})?;
		let decisions = apply_elision(entries.clone(), plan.retention_floor_snapshot);
		let output_run_count =
			required_output_run_count(survivor_records(&decisions), target_size)?;
		let output_slab_ids = self.reserve_lsm_output_slab_ids(output_run_count).await?;
		let mut prepared = worker.plan_survivor_outputs(plan, entries, output_slab_ids)?;
		if self.capture_lsm_input_authority(plan).await? != authority {
			return Err(CompactionFailure::ManifestRace);
		}
		prepared.input_authority = Some(authority);
		Ok(prepared)
	}

	/// `missing_origin` labels the retained integrity evidence when the
	/// backing is absent: a compaction source read is not a client read, and
	/// conflating them poisons the missing-published-reference signal.
	pub(super) async fn read_with_reference_origin(
		&self,
		location: RecordLocation,
		missing_origin: PublishedReferenceOrigin,
	) -> std::io::Result<Vec<u8>> {
		self.read_expected_version(location, missing_origin, None)
			.await
	}

	/// Read the version `(tenant_id, key_identity)` holds at `location`: the
	/// frame's decoded tenant, key and HLC must be that version's, on every
	/// backing, or the read refuses typed.
	pub(crate) async fn read_version(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		self.read_expected_version(
			location,
			PublishedReferenceOrigin::Read,
			Some((tenant_id, key_identity)),
		)
		.await
	}

	/// A tiered slab's frame is always verified against its location's version
	/// (and its pointer's tenant); any frame is verified against the caller's
	/// version when the caller names it.
	async fn read_expected_version(
		&self,
		location: RecordLocation,
		missing_origin: PublishedReferenceOrigin,
		named: Option<(TenantId, Identity)>,
	) -> std::io::Result<Vec<u8>> {
		self.read_expected_version_with_header(location, missing_origin, named)
			.await
			.map(|(_, frame)| frame)
	}

	/// Recovery obtains the checksum-verified header from the same pinned
	/// backing as the frame. Payload bytes never establish the record class.
	pub(super) async fn read_recovery_frame(
		&self,
		location: RecordLocation,
	) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
		let (backing, target, _pin) = self.recovery_read_target(location).await?;
		match backing {
			SlabBacking::Local(slab) => slab.read_with_header(target).await,
			SlabBacking::L2(_) => Err(Error::new(
				ErrorKind::InvalidData,
				"tier conflict recovery requires the owned verification flight",
			)),
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::NotFound,
				"recovery location has no local backing",
			)),
		}
	}

	async fn read_expected_version_with_header(
		&self,
		location: RecordLocation,
		missing_origin: PublishedReferenceOrigin,
		named: Option<(TenantId, Identity)>,
	) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
		// A generation's object can be deleted after a later publication while
		// a read that selected it is in flight; resolve once more from the
		// original location, never from the stale target.
		let mut attempts = 0;
		loop {
			attempts += 1;
			let ReadTarget {
				backing,
				location: target,
				tiered,
				_pin,
			} = self.resolve_read_target(location, true).await?;
			let expected = ExpectedVersion {
				tenant_id: named.map(|(tenant_id, _)| tenant_id),
				key_identity: named.map(|(_, key_identity)| key_identity),
			};
			let verify = tiered || named.is_some();
			match backing {
				SlabBacking::Local(slab) => {
					let started = Instant::now();
					let result = slab.read_with_header(target).await;
					metrics()
						.l1_read_latency_seconds
						.observe_duration(started.elapsed());
					let (header, frame) = result?;
					if verify {
						verify_frame_names_version(&header, &frame, target, expected)?;
					}
					return Ok((header, frame));
				}
				SlabBacking::L2(pointer) => {
					let started = Instant::now();
					let result = self.read_from_l2(&pointer, target).await;
					metrics()
						.l2_range_read_latency_seconds
						.observe_duration(started.elapsed());
					match result {
						Err(error) if error.kind() == ErrorKind::NotFound && attempts == 1 => {
							continue;
						}
						Ok((header, frame)) => {
							let expected = ExpectedVersion {
								tenant_id: expected.tenant_id.or(pointer.tenant_id),
								..expected
							};
							verify_frame_names_version(&header, &frame, target, expected)?;
							return Ok((header, frame));
						}
						Err(error) => return Err(error),
					}
				}
				SlabBacking::Missing => {
					return Err(Error::new(ErrorKind::NotFound, {
						self.record_missing_published_references_for_slab(
							target.slab_id,
							missing_origin,
						);
						format!("no slab with id {} under this manager", target.slab_id)
					}));
				}
			}
		}
	}

	pub async fn read_lsm_single_row_source_frames(
		&self,
		plan: &CompactionPlan,
	) -> std::io::Result<Vec<LsmSourceFrame>> {
		let entries = self.lsm_version_entries_for_plan(plan)?;
		let mut frames = Vec::with_capacity(entries.len());
		// ADR 0097: one read per PHYSICAL frame, shared by every row that resolves
		// into it. Reading per row re-read and re-retained the whole containing
		// frame once per row of a multi-row frame; the plan can name thousands of
		// rows across far fewer frames.
		let mut frames_by_key: HashMap<LsmSourceFrameKey, Arc<Vec<u8>>> = HashMap::new();
		for entry in entries {
			let key = LsmSourceFrameKey::from(entry.location);
			if let Some(frame) = frames_by_key.get(&key) {
				frames.push(LsmSourceFrame {
					entry,
					frame: Arc::clone(frame),
				});
				continue;
			}
			let frame = self
				.read_with_reference_origin(entry.location, PublishedReferenceOrigin::Compaction)
				.await
				.map_err(|err| {
					Error::new(
						err.kind(),
						format!(
							"LSM compaction source read failed at slab {} offset {} \
							 (plan touches slabs {:?}): {err}",
							entry.location.slab_id.get(),
							entry.location.offset,
							plan.touched_slabs(),
						),
					)
				})?;
			let frame = Arc::new(frame);
			frames_by_key.insert(key, Arc::clone(&frame));
			frames.push(LsmSourceFrame { entry, frame });
		}
		Ok(frames)
	}

	/// ADR 0097: re-encode one physical frame so it holds exactly the rows the run
	/// planned, delegating to the same `compact_physical_record_frame` the
	/// tombstone and retention rewrites use. That path already preserves key,
	/// routing locality, HLC, tombstone flag, durable placement, partition and
	/// per-row table attribution, and it renumbers `record_row_index` densely over
	/// the survivors - so the caller must match planned rows to new entries by
	/// identity rather than by their old index.
	///
	/// The source frame is not touched: the rewrite is appended to the temp output
	/// and the original stays addressable until installation commits.
	fn rewrite_lsm_multirow_partial_survivors(
		&self,
		tenant_id: TenantId,
		key: &LsmSourceFrameKey,
		source: &LsmMultiRowSourceFrame,
		run_rows_for_source: &[(usize, &VersionEntry)],
		table_schemas: Option<&TableSchemaCatalog>,
	) -> std::io::Result<LsmRewrittenMultiRowFrame> {
		let records = source
			.entries
			.iter()
			.map(|entry| {
				// ADR 0090: the row's durable placement is derived from its own
				// partition, never inherited from a neighbour in the same frame.
				let placement = if entry.partition.is_some() {
					PlacementSemantics::Partitioned
				} else {
					PlacementSemantics::RawLocality
				};
				let survives = run_rows_for_source.iter().any(|(_, candidate)| {
					lsm_multirow_index_entry_matches_version(entry, candidate)
				});
				CompactionRecord {
					tenant_id,
					key_identity: entry.key_identity,
					routing_locality: entry.routing_locality,
					old_location: RecordLocation {
						slab_id: key.slab_id,
						offset: key.offset,
						length: key.length,
						hlc: entry.row_hlc,
						record_row_index: entry.record_row_index,
						is_tombstone: entry.is_tombstone,
						placement,
						partition: entry.partition,
					},
					reclaim: !survives,
					retention_ambiguous: false,
					retention_lag_seconds: 0,
				}
			})
			.collect::<Vec<_>>();

		let compacted = compact_physical_record_frame(
			&source.frame,
			&records,
			table_schemas,
			&self.config.row_group_writer,
			&self.config.encoding_writer,
			&self.config.l2_chunk_layout,
		)?
		.ok_or_else(|| {
			// Unreachable from here: the caller only rewrites a frame that at least
			// one planned row resolves into, so the survivor set is never empty.
			// Recorded rather than assumed, because an empty rewrite would silently
			// drop every row of the frame.
			deterministic_failure(
				ErrorKind::InvalidData,
				DeterministicOrigin::LsmRewriteProducedNoSurvivors,
				"LSM multi-row rewrite produced no surviving rows",
			)
		})?;

		let entries =
			storage_multirow_index_entries_from_frame(&compacted.frame)?.ok_or_else(|| {
				deterministic_failure(
					ErrorKind::InvalidData,
					DeterministicOrigin::LsmRewriteFrameNotMultiRow,
					"LSM multi-row rewrite produced a frame without multi-row entries",
				)
			})?;
		// The caller maps planned rows to these entries by (key, HLC, tombstone),
		// because the rewrite renumbers positions. A first match is only sound if
		// that tuple is unique: two entries sharing it would let one planned row
		// silently take the other row.s location.
		let mut seen = HashSet::with_capacity(entries.len());
		for entry in &entries {
			if !seen.insert((entry.key_identity, entry.row_hlc, entry.is_tombstone)) {
				return Err(deterministic_failure(
					ErrorKind::InvalidData,
					DeterministicOrigin::LsmRewriteDuplicateRowIdentity,
					"LSM multi-row rewrite produced two rows with one identity; \
					 position-independent mapping would be ambiguous",
				));
			}
		}
		if entries.len() != run_rows_for_source.len() {
			return Err(deterministic_failure(
				ErrorKind::InvalidData,
				DeterministicOrigin::LsmRewriteKeptWrongRowCount,
				format!(
					"LSM multi-row rewrite kept {} rows, run planned {}",
					entries.len(),
					run_rows_for_source.len()
				),
			));
		}
		Ok(LsmRewrittenMultiRowFrame {
			frame: compacted.frame,
			entries,
		})
	}

	pub async fn write_lsm_compaction_temp_outputs(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		source_frames: &[LsmSourceFrame],
	) -> std::io::Result<Vec<LsmWrittenOutputRun>> {
		self.write_lsm_compaction_temp_outputs_with_table_schemas(
			plan,
			prepared,
			source_frames,
			None,
		)
		.await
	}

	/// ADR 0097: the partial-survivor rewrite re-encodes a frame, so it needs the
	/// schema catalog the row-group descriptor is chosen from. `None` keeps the
	/// uncompressed-policy path, which is what a caller without a catalog had
	/// before this parameter existed.
	pub async fn write_lsm_compaction_temp_outputs_with_table_schemas(
		&self,
		plan: &CompactionPlan,
		prepared: &LsmPreparedOutputs,
		source_frames: &[LsmSourceFrame],
		table_schemas: Option<&TableSchemaCatalog>,
	) -> std::io::Result<Vec<LsmWrittenOutputRun>> {
		let mut source_frames_by_location = HashMap::with_capacity(source_frames.len());
		let mut multirow_source_frames_by_key = HashMap::new();
		for source in source_frames {
			// ADR 0097: rows sharing a physical frame share its buffer, so the frame is
			// decoded once per frame rather than once per row. `Arc::ptr_eq` below is
			// the same-buffer fast path; the byte comparison stays for the case the
			// type still permits - two buffers claiming one physical location.
			let key = LsmSourceFrameKey::from(source.entry.location);
			if let Some(existing) = multirow_source_frames_by_key.get(&key) {
				let existing: &LsmMultiRowSourceFrame = existing;
				if Arc::ptr_eq(&existing.frame, &source.frame) {
					validate_lsm_multirow_source_entry(
						&source.entry,
						lsm_multirow_index_entry_for_version(&existing.entries, &source.entry)
							.ok_or_else(|| {
								deterministic_failure(
									ErrorKind::InvalidData,
									DeterministicOrigin::LsmSourceFrameMissingPlannedSurvivor,
									"LSM source frame does not contain planned multi-row survivor",
								)
							})?,
					)?;
					continue;
				}
			}
			if let Some(entries) = storage_multirow_index_entries_from_frame(&source.frame)? {
				let Some(matching_entry) =
					lsm_multirow_index_entry_for_version(&entries, &source.entry)
				else {
					return Err(deterministic_failure(
						ErrorKind::InvalidData,
						DeterministicOrigin::LsmSourceFrameMissingPlannedSurvivor,
						"LSM source frame does not contain planned multi-row survivor",
					));
				};
				validate_lsm_multirow_source_entry(&source.entry, matching_entry)?;
				let key = LsmSourceFrameKey::from(source.entry.location);
				match multirow_source_frames_by_key.entry(key) {
					std::collections::hash_map::Entry::Vacant(entry) => {
						entry.insert(LsmMultiRowSourceFrame {
							frame: Arc::clone(&source.frame),
							entries,
						});
					}
					std::collections::hash_map::Entry::Occupied(entry) => {
						let existing = entry.get();
						if existing.frame != source.frame || existing.entries != entries {
							return Err(deterministic_failure(
								ErrorKind::InvalidData,
								DeterministicOrigin::LsmInconsistentSourceFrames,
								"inconsistent LSM source frames for the same physical record",
							));
						}
					}
				}
			} else if source_frames_by_location
				.insert(source.entry.location, source)
				.is_some()
			{
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"duplicate LSM source frame location",
				));
			}
		}

		let mut written: Vec<LsmWrittenOutputRun> = Vec::with_capacity(prepared.output_runs.len());
		for run in &prepared.output_runs {
			if run.compaction_level != plan.target_level {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"planned LSM output run target level does not match compaction plan",
				));
			}

			let temp_path = lsm_compaction_tmp_path(&self.config.dir, plan.job_id, run.slab_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &temp_path).await;
			let output = match LocalSlabStore::open(
				&temp_path,
				run.slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await
			{
				Ok(output) => Arc::new(output),
				Err(err) => {
					for previous in &written {
						let _ =
							unlink_local_file(self.file_ops.as_ref(), &previous.temp_path).await;
					}
					return Err(err);
				}
			};
			let write_result: std::io::Result<Vec<RecordLocation>> = async {
				let mut locations = vec![None; run.rows.len()];
				let mut written_multirow_keys = HashSet::new();
				for (row_index, row) in run.rows.iter().enumerate() {
					let multirow_key = LsmSourceFrameKey::from(row.location);
					if let Some(source) = multirow_source_frames_by_key.get(&multirow_key) {
						if !written_multirow_keys.insert(multirow_key) {
							continue;
						}
						let run_rows_for_source = run
							.rows
							.iter()
							.enumerate()
							.filter(|(_, candidate)| {
								LsmSourceFrameKey::from(candidate.location) == multirow_key
							})
							.collect::<Vec<_>>();
						// ADR 0097: a frame whose surviving rows are fewer than the rows it
						// holds is rewritten to contain exactly the survivors, rather than
						// refused after the whole working set has been built. Retention
						// dropping some rows of a frame is the ordinary case here; the
						// planner already guarantees a run never holds only PART of a
						// frame's survivors, so a short run row set means rows were
						// dropped, not split across runs.
						let rewritten = if run_rows_for_source.len() == source.entries.len() {
							None
						} else {
							Some(self.rewrite_lsm_multirow_partial_survivors(
								row.tenant_id,
								&multirow_key,
								source,
								&run_rows_for_source,
								table_schemas,
							)?)
						};
						let (frame_bytes, index_entries) = match &rewritten {
							Some(rewritten) => {
								(rewritten.frame.as_slice(), rewritten.entries.as_slice())
							}
							None => (source.frame.as_slice(), source.entries.as_slice()),
						};
						// A rewritten frame renumbers its rows, so position is not a
						// stable identifier across the rewrite; an untouched frame keeps
						// its positions and is held to the stricter match.
						let rows_match = |entry: &MultiRowIndexEntry, candidate: &VersionEntry| {
							if rewritten.is_some() {
								lsm_multirow_index_entry_matches_version_ignoring_position(
									entry, candidate,
								)
							} else {
								lsm_multirow_index_entry_matches_version(entry, candidate)
							}
						};
						// Validate once and carry the exact matched source row into the
						// output location, preserving the single deterministic origin.
						let matched_rows =
							index_entries
								.iter()
								.map(|entry| {
									let mut matches = run_rows_for_source
										.iter()
										.filter(|(_, candidate)| rows_match(entry, candidate))
										.map(|(_, candidate)| *candidate);
									let first = matches.next();
									match first {
								Some(candidate) if matches.all(|other|
									other.location.placement == candidate.location.placement
										&& other.location.partition == candidate.location.partition) => Ok(candidate),
								_ => Err(deterministic_failure(
									ErrorKind::InvalidData,
									DeterministicOrigin::LsmRewrittenFrameCarriesUnplannedRow,
									"LSM output row lacks an unambiguous planned placement",
								)),
							}
								})
								.collect::<std::io::Result<Vec<_>>>()?;
						let Some(primary) = index_entries.first() else {
							return Err(deterministic_failure(
								ErrorKind::InvalidData,
								DeterministicOrigin::LsmMultiRowSourceFrameHasNoEntries,
								"LSM multi-row source frame has no index entries",
							));
						};
						let record_hlc = index_entries
							.iter()
							.map(|entry| entry.row_hlc)
							.max()
							.unwrap_or(primary.row_hlc);
						let base_location = output
							.append(row.tenant_id, primary.key_identity, record_hlc, frame_bytes)
							.await?;
						output.rollback_index_entry(
							row.tenant_id,
							primary.key_identity,
							base_location,
						);
						let mut locations_by_row_index = HashMap::new();
						for (entry, candidate) in index_entries.iter().zip(&matched_rows) {
							let new_location = RecordLocation {
								hlc: entry.row_hlc,
								record_row_index: entry.record_row_index,
								is_tombstone: entry.is_tombstone,
								placement: candidate.location.placement,
								partition: candidate.location.partition,
								..base_location
							};
							output.index_location(row.tenant_id, entry.key_identity, new_location);
							locations_by_row_index.insert(entry.record_row_index, new_location);
						}
						for (position, candidate) in run_rows_for_source {
							// A rewritten frame renumbers its rows, so a planned row is
							// matched by identity and HLC rather than by its old index.
							let Some(new_location) = index_entries
								.iter()
								.find(|entry| rows_match(entry, candidate))
								.and_then(|entry| {
									locations_by_row_index.get(&entry.record_row_index).copied()
								})
							else {
								return Err(deterministic_failure(
									ErrorKind::InvalidData,
									DeterministicOrigin::LsmOutputLocationMissingPlannedRow,
									"LSM multi-row output location missing planned row",
								));
							};
							locations[position] = Some(new_location);
						}
						continue;
					}

					let source = source_frames_by_location
						.get(&row.location)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidInput,
								"missing LSM source frame for planned survivor",
							)
						})?;
					if source.entry != *row {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"LSM source frame entry does not match planned survivor",
						));
					}
					if storage_multirow_index_entries_from_frame(&source.frame)?.is_some() {
						return Err(deterministic_failure(
							ErrorKind::Unsupported,
							DeterministicOrigin::LsmWriterCannotSplitMultiRowFrame,
							"LSM temp-output writer does not yet split multi-row frames",
						));
					}
					let base_location = output
						.append(row.tenant_id, row.row_key_id, row.row_hlc, &source.frame)
						.await?;
					output.rollback_index_entry(row.tenant_id, row.row_key_id, base_location);
					let new_location = RecordLocation {
						hlc: row.row_hlc,
						record_row_index: row.record_row_index,
						is_tombstone: row.location.is_tombstone,
						placement: row.location.placement,
						partition: row.location.partition,
						..base_location
					};
					output.index_location(row.tenant_id, row.row_key_id, new_location);
					locations[row_index] = Some(new_location);
				}
				let locations = locations
					.into_iter()
					.collect::<Option<Vec<_>>>()
					.ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"LSM output writer did not materialize every planned survivor",
						)
					})?;
				output.fsync().await?;
				fsync_parent_dir(self.file_ops.as_ref(), &temp_path).await?;
				Ok(locations)
			}
			.await;

			let locations = match write_result {
				Ok(locations) => locations,
				Err(err) => {
					let _ = unlink_local_file(self.file_ops.as_ref(), &temp_path).await;
					for previous in &written {
						let _ =
							unlink_local_file(self.file_ops.as_ref(), &previous.temp_path).await;
					}
					return Err(err);
				}
			};
			let mut summary = run.summary(plan.manifest_epoch_pre);
			summary.size_bytes = ByteCount::new(output.write_cursor());
			written.push(LsmWrittenOutputRun {
				slab_id: run.slab_id,
				compaction_level: run.compaction_level,
				temp_path,
				summary,
				locations,
			});
		}
		Ok(written)
	}

	pub async fn sweep_lsm_delete_pending_once(
		&self,
		gc_worker: &LsmGcWorker,
	) -> std::io::Result<Vec<SlabId>> {
		self.require_tier_authority_ready()?;
		// Capture retired candidates, claim rewrites first (the same order as
		// compaction), then serialize their manifest transition under tier locks.
		let mut retired = self
			.compaction_manifest_snapshot()
			.retired_runs()
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		retired.sort_unstable();
		retired.dedup();
		{
			let mut claims = Vec::with_capacity(retired.len());
			for &slab_id in &retired {
				claims.push(self.acquire_slab_rewrite_exclusive_claim(slab_id).await);
			}
			let _maintenance = self
				.maintenance_publication_gate
				.clone()
				.write_owned()
				.await;
			let mut locks = Vec::with_capacity(retired.len());
			for &slab_id in &retired {
				locks.push(self.tier_publication_lock(slab_id).lock_owned().await);
			}
			let mut unreferenced = BTreeSet::new();
			for &slab_id in &retired {
				if !self.counter_slab_has_pending_frames(slab_id)
					&& self.lsm_input_authority_locked(&[slab_id]).await.is_ok()
					&& self.record_rows_for_slab(slab_id)?.is_empty()
				{
					unreferenced.insert(slab_id);
				}
			}
			let before =
				recover_rwlock_read(&self.compaction_manifest, "compaction_manifest").clone();
			let mut updated = before.clone();
			let index = self.persistent_index.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::WouldBlock,
					"persistent index unavailable for LSM GC",
				)
			})?;
			let publication = self.begin_index_publication(retired.clone()).await?;
			let outcome = gc_worker
				.transition_retired_to_delete_pending_if(&mut updated, |slab_id| {
					unreferenced.contains(&slab_id)
				});
			if !outcome.marked_delete_pending.is_empty() {
				for &slab_id in &outcome.marked_delete_pending {
					self.bump_slab_publication_revision(slab_id)?;
				}
				let runs = outcome
					.marked_delete_pending
					.iter()
					.filter_map(|slab_id| updated.get_run(*slab_id).cloned())
					.collect();
				index
					.apply_with_publication_guard(
						PersistentIndexUpdate {
							put_lsm_runs: runs,
							..Default::default()
						},
						Some(publication.gate.clone()),
					)
					.await?;
				let mut manifest =
					recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
				let current = manifest.snapshot();
				let original = before.snapshot();
				if current.manifest_epoch != original.manifest_epoch
					|| current.runs() != original.runs()
				{
					return Err(Error::new(
						ErrorKind::WouldBlock,
						"LSM manifest changed during GC marking",
					));
				}
				*manifest = updated;
				let snapshot = manifest.snapshot();
				drop(manifest);
				refresh_lsm_level_metrics(&snapshot);
			}
			publication.complete();
		}

		let mut candidates = self
			.compaction_manifest_snapshot()
			.delete_pending_runs()
			.cloned()
			.collect::<Vec<_>>();
		candidates.sort_unstable_by_key(|run| run.slab_id);
		let mut deleted = Vec::new();
		for expected in candidates {
			let slab_id = expected.slab_id;
			if self.is_compaction_slab_quarantined(slab_id) {
				continue;
			}
			let _slab_rewrite_claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
			let _maintenance = self
				.maintenance_publication_gate
				.clone()
				.write_owned()
				.await;
			let tier_lock = self.tier_publication_lock(slab_id);
			let _tier_guard = tier_lock.lock().await;
			let still_pending = self
				.compaction_manifest_snapshot()
				.delete_pending_runs()
				.any(|run| *run == expected);
			if !still_pending
				|| self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
			{
				continue;
			}
			{
				let state = self.state.lock().await;
				if state
					.sealed
					.get(&slab_id)
					.is_some_and(|slab| !slab.manager_appends_quiescent())
				{
					continue;
				}
			}
			let index = self.persistent_index.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::WouldBlock,
					"persistent index unavailable for LSM GC",
				)
			})?;
			// Sidecar rows are retired below under this same X lock. A live
			// record reference, however, forbids releasing the slab.
			let snapshot = index.load_snapshot().await?;
			if snapshot
				.record_rows
				.iter()
				.chain(&snapshot.tiered_records)
				.any(|row| row.location.slab_id == slab_id)
			{
				self.quarantined_compaction_slabs.insert(slab_id);
				continue;
			}
			for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
				self.retire_secondary_index_segment_for_replaced_slab_locked(&info)
					.await?;
			}
			if !self.can_retire_local_slab(slab_id).await? {
				continue;
			}
			self.bump_slab_publication_revision(slab_id)?;
			if self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"LSM GC eligibility changed before unlink",
				));
			}
			let publication = self.begin_index_publication(vec![slab_id]).await?;
			self.gc_unlink_owned_slab_file(slab_id).await?;
			self.redirects.remove_touching(&self.tier_budget, slab_id);
			self.record_disposal_evidence(slab_id).await;
			index
				.apply_with_publication_guard(
					PersistentIndexUpdate {
						delete_lsm_run_ids: vec![slab_id],
						..Default::default()
					},
					Some(publication.gate.clone()),
				)
				.await?;
			let snapshot = {
				let mut manifest =
					recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
				gc_worker.complete_delete(&mut manifest, slab_id);
				manifest.snapshot()
			};
			refresh_lsm_level_metrics(&snapshot);
			#[cfg(test)]
			self.pause_gc_before_clear_for_test().await;
			publication.complete();
			#[cfg(test)]
			self.pause_gc_after_clear_for_test().await;
			deleted.push(slab_id);
		}
		Ok(deleted)
	}

	pub async fn sweep_lsm_delete_pending_once_via_control_ring(
		&self,
		gc_worker: &LsmGcWorker,
		committer: &dyn TierCommitter,
	) -> std::io::Result<Vec<SlabId>> {
		self.require_tier_authority_ready()?;
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for LSM GC",
			)
		})?;
		let mut retired = self
			.compaction_manifest_snapshot()
			.retired_runs()
			.cloned()
			.collect::<Vec<_>>();
		retired.sort_unstable_by_key(|run| run.slab_id);
		// A full window refuses new marks and new removals, and never keeps this
		// sweep from the removals already reserved.
		let mut backlog = None;
		for expected_run in retired {
			let slab_id = expected_run.slab_id;
			// No lock spans this control-ring await, and nothing is allocated
			// or marked for an owner that cannot propose.
			committer.preflight(self.owner_ref()?).await?;
			if let Err(error) = index.ensure_op_room(2).await {
				if !crate::storage::sweeper::op_window_refused(&error) {
					return Err(error);
				}
				backlog = Some(error);
				break;
			}
			let expected = {
				let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
				let tier_lock = self.tier_publication_lock(slab_id);
				let _tier_guard = tier_lock.lock().await;
				if !self
					.compaction_manifest_snapshot()
					.retired_runs()
					.any(|run| *run == expected_run)
					|| self.slab_pins.pin_count(slab_id) != 0
					|| self.counter_slab_has_pending_frames(slab_id)
					|| !self.record_rows_for_slab(slab_id)?.is_empty()
				{
					continue;
				}
				let expected = match self.lsm_input_authority_locked(&[slab_id]).await {
					Ok(authority) => authority,
					Err(_) => continue,
				};
				if !self.slab_pins.mark_delete_pending(slab_id) {
					continue;
				}
				expected
			};
			let snapshot = self.compaction_manifest_snapshot();
			let commit = CompactionCommit::gc_mark_delete_pending(
				self.owner_ref()?,
				Self::lsm_gc_job_id(snapshot.manifest_epoch, 0xD1, &[slab_id]),
				snapshot.manifest_epoch,
				vec![slab_id],
			);
			// The removal is built whole now and reserved with the mark, so it
			// never needs a fresh sequence (ADR 0697 §5.2).
			let remove = crate::consensus::slab_ops::GcRemove {
				owner: commit.owner,
				job_id: Self::lsm_gc_job_id(snapshot.manifest_epoch, 0xD2, &[slab_id]),
				slab: slab_id,
				expected_run: crate::consensus::slab_ops::RunIdentity::of(&expected_run),
			};
			let digest =
				*blake3::hash(&serde_json::to_vec(&commit).map_err(Error::other)?).as_bytes();
			let intent = crate::storage::index::LsmInstallIntent {
				kind: crate::storage::index::LsmInstallKind::Control,
				phase: crate::storage::index::LsmInstallPhase::Prepared,
				local_input_rows_digest: [0; 32],
				local_output_rows_digest: [0; 32],
				local_input_runs: Vec::new(),
				local_input_files: Vec::new(),
				local_output_files: Vec::new(),
				commit: commit.clone(),
				commit_digest: digest,
				ops: Vec::new(),
				inputs: expected.clone(),
			};
			let Some(_intent_owner) = self.register_lsm_install_intent(&intent).await else {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"LSM GC intent already unresolved",
				));
			};
			self.tier_authority.notify_publication_change();
			let allocation = match index
				.allocate_ops(
					vec![
						crate::consensus::slab_ops::SlabCommand::Compaction(commit.clone()),
						crate::consensus::slab_ops::SlabCommand::GcRemove(remove),
					],
					Some(intent),
				)
				.await
			{
				Ok(allocation) => allocation,
				// Nothing was written: the window is full, and this run is
				// not marked. Only new work is refused.
				Err(error) if crate::storage::sweeper::op_window_refused(&error) => {
					self.slab_pins.clear_delete_pending(slab_id);
					self.clear_registered_lsm_intent(commit.job_id).await;
					_intent_owner.complete();
					self.tier_authority.notify_publication_change();
					backlog = Some(error);
					break;
				}
				Err(error) => return Err(error),
			};
			let removal = allocation
				.later
				.first()
				.map(|reserved| reserved.key())
				.ok_or_else(|| Error::other("a GC mark reserves its removal"))?;
			let ticket = allocation.first;
			let intent = allocation
				.linked
				.ok_or_else(|| Error::other("an allocated GC operation returns its intent"))?;
			Self::require_gc_applied(committer.submit_op(&ticket).await?)?;
			let _claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
			let _maintenance = self
				.maintenance_publication_gate
				.clone()
				.write_owned()
				.await;
			let tier_lock = self.tier_publication_lock(slab_id);
			let _tier_guard = tier_lock.lock().await;
			let snapshot = self.compaction_manifest_snapshot();
			let actual = snapshot.run(slab_id).cloned();
			let exact = actual.as_ref().is_some_and(|run| {
				let mut expected_after = expected_run.clone();
				expected_after.state = SlabState::DeletePending;
				expected_after.manifest_epoch = commit.manifest_epoch_pre.saturating_add(1);
				*run == expected_after
			});
			if !exact
				|| self
					.lsm_input_authority_locked(&[slab_id])
					.await
					.ok()
					.as_ref() != Some(&expected)
				|| self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
				|| !self.record_rows_for_slab_for_recovery(slab_id).is_empty()
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"committed LSM GC mark changed; intent remains fenced",
				));
			}
			self.bump_slab_publication_revision(slab_id)?;
			let publication = self.begin_index_publication(vec![slab_id]).await?;
			index
				.apply_with_publication_guard(
					PersistentIndexUpdate {
						put_lsm_runs: vec![actual.unwrap()],
						..Default::default()
					},
					Some(publication.gate.clone()),
				)
				.await?;
			gc_worker.observe_manifest_snapshot(&snapshot);
			publication.complete();
			// The mark's own revision change is in this authority; anything after it
			// is not, and the removal refuses it.
			let left_for_removal = self
				.lsm_input_authority_locked(&[slab_id])
				.await
				.map_err(|error| Error::other(error.to_string()))?;
			index
				.hand_off_gc_mark(&ticket, intent, removal, left_for_removal)
				.await?;
			self.clear_registered_lsm_intent(commit.job_id).await;
			_intent_owner.complete();
			self.tier_authority.notify_publication_change();
		}

		let mut candidates = self
			.compaction_manifest_snapshot()
			.delete_pending_runs()
			.cloned()
			.collect::<Vec<_>>();
		candidates.sort_unstable_by_key(|run| run.slab_id);
		let mut deleted = Vec::new();
		for expected_run in candidates {
			let slab_id = expected_run.slab_id;
			if self.is_compaction_slab_quarantined(slab_id) {
				continue;
			}
			#[cfg(test)]
			if self
				.continuity_test_holds
				.gc_remove_held
				.load(std::sync::atomic::Ordering::Acquire)
			{
				continue;
			}
			// No lock spans this control-ring await, and nothing is allocated
			// or marked for an owner that cannot propose.
			committer.preflight(self.owner_ref()?).await?;
			let expected = {
				let _maintenance = self.maintenance_publication_gate.clone().read_owned().await;
				let tier_lock = self.tier_publication_lock(slab_id);
				let _tier_guard = tier_lock.lock().await;
				if !self
					.compaction_manifest_snapshot()
					.delete_pending_runs()
					.any(|run| *run == expected_run)
					|| self.slab_pins.pin_count(slab_id) != 0
					|| self.counter_slab_has_pending_frames(slab_id)
					|| !self.record_rows_for_slab(slab_id)?.is_empty()
				{
					continue;
				}
				match self.lsm_input_authority_locked(&[slab_id]).await {
					Ok(authority) => authority,
					Err(_) => continue,
				}
			};
			// The removal reserved with this run's mark. A run marked by another
			// path has none, and takes a new operation under the window's bound.
			let reserved = match self
				.gc_remove_reservation_or_allocate(index, &expected_run)
				.await
			{
				Ok(reserved) => reserved,
				Err(error) if crate::storage::sweeper::op_window_refused(&error) => {
					backlog = Some(error);
					continue;
				}
				Err(error) => return Err(error),
			};
			// Its mark has not finished; its intent is what fences this run.
			if reserved.awaits_its_workflow() {
				continue;
			}
			// The authority this reservation was made for, or nothing: a different
			// object is never adopted as the same operation.
			if reserved
				.expected_inputs
				.as_ref()
				.is_some_and(|inputs| *inputs != expected)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"a reserved GC removal's input authority changed; it remains fenced",
				));
			}
			let snapshot = self.compaction_manifest_snapshot();
			let commit = CompactionCommit::gc_remove(
				self.owner_ref()?,
				reserved.job_id,
				snapshot.manifest_epoch,
				vec![slab_id],
			);
			let digest =
				*blake3::hash(&serde_json::to_vec(&commit).map_err(Error::other)?).as_bytes();
			let intent = crate::storage::index::LsmInstallIntent {
				kind: crate::storage::index::LsmInstallKind::Control,
				phase: crate::storage::index::LsmInstallPhase::Prepared,
				local_input_rows_digest: [0; 32],
				local_output_rows_digest: [0; 32],
				local_input_runs: Vec::new(),
				local_input_files: Vec::new(),
				local_output_files: Vec::new(),
				commit: commit.clone(),
				commit_digest: digest,
				ops: Vec::new(),
				inputs: expected.clone(),
			};
			let Some(_intent_owner) = self.register_lsm_install_intent(&intent).await else {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"LSM GC intent already unresolved",
				));
			};
			self.tier_authority.notify_publication_change();
			let mut intent = intent;
			intent.ops = vec![reserved.key()];
			index.put_lsm_install_intent(intent.clone()).await?;
			let ticket = reserved.into_ticket(index).await?;
			// No maintenance, tier or rewrite lock spans the control-ring await.
			Self::require_gc_applied(committer.submit_op(&ticket).await?)?;
			let _claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
			let _maintenance = self
				.maintenance_publication_gate
				.clone()
				.write_owned()
				.await;
			let tier_lock = self.tier_publication_lock(slab_id);
			let _tier_guard = tier_lock.lock().await;
			if self
				.lsm_input_authority_locked(&[slab_id])
				.await
				.ok()
				.as_ref() != Some(&expected)
				|| self.compaction_manifest_snapshot().run(slab_id).is_some()
				|| self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
				|| !self.slab_pins.mark_delete_pending(slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"committed LSM GC authority changed; intent remains fenced",
				));
			}
			let durable = index.load_snapshot().await?;
			if durable
				.record_rows
				.iter()
				.chain(&durable.tiered_records)
				.any(|row| row.location.slab_id == slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"committed LSM GC has live record references",
				));
			}
			for info in self.list_secondary_index_segments_for_source_slab(slab_id) {
				self.retire_secondary_index_segment_for_replaced_slab_locked(&info)
					.await?;
			}
			let durable = index.load_snapshot().await?;
			if durable
				.secondary_index_segments
				.iter()
				.any(|row| row.key.source_slab_id == slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"committed LSM GC has sidecar references",
				));
			}
			self.bump_slab_publication_revision(slab_id)?;
			if self.slab_pins.pin_count(slab_id) != 0
				|| self.counter_slab_has_pending_frames(slab_id)
			{
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"LSM GC eligibility changed before unlink",
				));
			}
			self.gc_unlink_owned_slab_file(slab_id).await?;
			self.redirects.remove_touching(&self.tier_budget, slab_id);
			self.record_disposal_evidence(slab_id).await;
			let publication = self.begin_index_publication(vec![slab_id]).await?;
			index
				.apply_with_publication_guard(
					PersistentIndexUpdate {
						delete_lsm_run_ids: vec![slab_id],
						..Default::default()
					},
					Some(publication.gate.clone()),
				)
				.await?;
			gc_worker.observe_manifest_snapshot(&self.compaction_manifest_snapshot());
			#[cfg(test)]
			self.pause_gc_before_clear_for_test().await;
			publication.complete();
			index
				.hand_off_op(ticket.key(), ticket.digest(), Some(intent))
				.await?;
			self.clear_registered_lsm_intent(commit.job_id).await;
			_intent_owner.complete();
			self.slab_pins.clear_delete_pending(slab_id);
			self.tier_authority.notify_publication_change();
			#[cfg(test)]
			self.pause_gc_after_clear_for_test().await;
			deleted.push(slab_id);
		}
		match backlog {
			Some(error) if deleted.is_empty() => Err(error),
			_ => Ok(deleted),
		}
	}

	pub(super) fn lsm_gc_job_id(
		manifest_epoch: ManifestEpoch,
		discriminator: u64,
		slab_ids: &[SlabId],
	) -> CompactionJobId {
		let mut value = 0xA55A_0550_0000_0000_u64 ^ discriminator ^ manifest_epoch.get();
		for slab_id in slab_ids {
			value = value.rotate_left(7) ^ slab_id.get();
		}
		CompactionJobId::new(value)
	}

	pub(super) fn pin_lsm_read_location(
		&self,
		location: RecordLocation,
	) -> std::io::Result<SlabPinGuard> {
		if self
			.maintenance_publication_restart_required
			.load(Ordering::Acquire)
		{
			return Err(Error::new(
				ErrorKind::Other,
				"maintenance publication requires restart reconciliation before reads may resume",
			));
		}
		self.slab_pins.acquire(&[location.slab_id]).map_err(|_| {
			Error::new(
				ErrorKind::NotFound,
				format!("slab {} is delete-pending for LSM GC", location.slab_id),
			)
		})
	}
}
