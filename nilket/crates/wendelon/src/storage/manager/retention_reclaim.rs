use super::*;
use crate::storage::index::{RetainedObjectIntent, RetainedObjectIntentState};

/// A nonleader defers L2 debt without uploading, proposing, or completing it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableDropReclaimStatus {
	#[default]
	Progress,
	DeferredNotLeader,
}

/// Outcome of one explicit table-drop reclaim pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableDropReclaimReport {
	pub status: TableDropReclaimStatus,
	pub compacted_slabs: usize,
	pub reclaimed_records: usize,
	pub reclaimed_bytes: u64,
}

type DeferredDropKey = (TenantId, TableObjectId, u64);

type CleanupWorkKey = (SlabId, u8, String);
enum CleanupWork {
	Candidate(SlabId, String),
	Intent(SlabId, String),
	Legacy(SlabId),
}

#[derive(Default)]
pub(crate) struct CleanupReconcileReport {
	examined: usize,
	fully_scanned: bool,
	has_pending: bool,
	deferred_slabs: BTreeSet<SlabId>,
	first_error: Option<Error>,
}
impl CleanupReconcileReport {
	fn legacy_result(self) -> std::io::Result<(usize, bool)> {
		match self.first_error {
			Some(error) => Err(error),
			// Pending work can arrive after this scan selected its entries. It
			// prevents completion without inventing an error for unvisited work.
			None => Ok((self.examined, self.fully_scanned && !self.has_pending)),
		}
	}
}
#[derive(Default)]
pub(super) struct CleanupScanSchedule {
	after: std::sync::Mutex<Option<CleanupWorkKey>>,
	drops: dashmap::DashMap<DeferredDropKey, Arc<tokio::sync::Mutex<TableDropSliceScan>>>,
}
struct TableDropSliceScan {
	after: Option<SlabId>,
	watermark: Option<SlabId>,
	upper: Option<SlabId>,
	blocked: Option<SlabId>,
	cleanup_next: bool,
}
impl TableDropSliceScan {
	fn start(cursor: TableDropReclaimCursor, upper: Option<SlabId>) -> Self {
		Self {
			after: cursor.after_slab_id,
			watermark: cursor.after_slab_id,
			upper,
			blocked: None,
			cleanup_next: true,
		}
	}
}

/// Volatile set of operation debts deferred by local role. Existing replicated
/// debt and durable reclaim progress reconstruct it on the first recovery tick.
#[derive(Default)]
pub(super) struct DeferredTableDropL2(
	std::sync::Mutex<BTreeSet<DeferredDropKey>>,
	#[cfg(test)] std::sync::Mutex<Option<&'static crate::metrics::Gauge>>,
);

impl DeferredTableDropL2 {
	fn gauge(&self) -> &'static crate::metrics::Gauge {
		#[cfg(test)]
		if let Some(gauge) = *self.1.lock().unwrap() {
			return gauge;
		}
		&metrics().storage_table_drop_l2_deferred_debts
	}

	fn update(&self, f: impl FnOnce(&mut BTreeSet<DeferredDropKey>)) {
		let mut debts = self.0.lock().unwrap();
		let before = debts.len();
		f(&mut debts);
		let after = debts.len();
		let gauge = self.gauge();
		if after > before {
			gauge.add((after - before) as u64);
		} else {
			gauge.sub((before - after) as u64);
		}
	}
}
impl Drop for DeferredTableDropL2 {
	fn drop(&mut self) {
		let count = self.0.get_mut().unwrap().len() as u64;
		self.gauge().sub(count);
	}
}

/// ADR 0098 decision 2: what one policed scan of a sealed slab established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SealedSlabScan {
	/// The claim found the slab pinned, retired, or replaced: nothing was
	/// scanned and nothing is known about its contents.
	Unclaimed,
	/// The scan found work and could not do it this tick - a pin gate or a
	/// quarantined run stood in the way. The work is still there.
	Deferred,
	/// The scan found nothing to rewrite.
	Clean,
}

impl SealedSlabScan {
	pub(super) fn after_scan(records: &[CompactionRecord]) -> Self {
		if records.iter().any(|record| record.reclaim) {
			Self::Deferred
		} else {
			Self::Clean
		}
	}

	fn disposition(self, compaction: Option<&CompactionOutcome>) -> SuccessDisposition {
		match (self, compaction) {
			(_, Some(_)) => SuccessDisposition::Rewrote,
			(Self::Clean, None) => SuccessDisposition::Neutral,
			(Self::Unclaimed | Self::Deferred, None) => SuccessDisposition::Unclaimed,
		}
	}
}

#[derive(Clone, Copy)]
pub(super) struct RetentionSlabScanOutcome {
	pub(super) scan: SealedSlabScan,
	pub(super) compaction: Option<CompactionOutcome>,
	pub(super) retention_stats: RetentionCompactionStats,
}

impl RetentionSlabScanOutcome {
	pub(super) fn disposition(&self) -> SuccessDisposition {
		self.scan.disposition(self.compaction.as_ref())
	}
}

/// One tombstone reclaim attempt over one sealed slab.
#[derive(Clone, Copy)]
pub(super) struct TombstoneSlabScanOutcome {
	pub(super) scan: SealedSlabScan,
	pub(super) compaction: Option<CompactionOutcome>,
}

impl TombstoneSlabScanOutcome {
	pub(super) fn disposition(&self) -> SuccessDisposition {
		self.scan.disposition(self.compaction.as_ref())
	}
}

pub(super) fn retention_rewrite_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.retention.tmp", slab_id))
}

pub(super) fn table_drop_rewrite_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.table_drop.tmp", slab_id))
}

const TABLE_DROP_L2_CLEANUP_PREFIX: &str = "table_drop_l2_cleanup_";
const TABLE_DROP_L2_CLEANUP_SUFFIX: &str = ".pending";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingTableDropL2Cleanup {
	format_version: u8,
	slab_id: SlabId,
	old_l2_uri: String,
	replacement_l2_uri: Option<String>,
	state: CleanupIntentState,
	retained_bytes: u64,
}

/// A failed response never establishes that a publication cannot still apply.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum CleanupIntentState {
	Unresolved,
}

/// One manager's contribution to process gauges, restored before open returns.
#[derive(Default)]
pub(super) struct UnresolvedTierCleanup {
	intents: std::sync::Mutex<CleanupIntentUnion>,
	missing_authority: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct CleanupIntentUnion {
	legacy: BTreeMap<SlabId, (String, Option<String>, u64)>,
	snapshot: BTreeMap<(SlabId, String), RetainedObjectIntent>,
	pending: BTreeSet<(SlabId, String)>,
}

impl CleanupIntentUnion {
	fn totals(&self) -> (u64, u64, u64) {
		let mut count = self.legacy.len() as u64;
		let mut bytes: u64 = self.legacy.values().map(|(_, _, bytes)| *bytes).sum();
		let mut unknown = 0u64;
		for ((slab, uri), size) in &self.snapshot {
			let duplicate = self.legacy.get(slab).is_some_and(|(old, replacement, _)| {
				uri == old || replacement.as_ref() == Some(uri)
			});
			if !duplicate {
				count = count.saturating_add(1);
				if let Some(size) = size.bytes {
					bytes = bytes.saturating_add(size);
				} else {
					unknown = unknown.saturating_add(1);
				}
			}
		}
		(count, bytes, unknown)
	}
}

impl UnresolvedTierCleanup {
	fn mark_missing_authority(&self) {
		if !self
			.missing_authority
			.swap(true, std::sync::atomic::Ordering::AcqRel)
		{
			metrics().storage_cleanup_authority_missing.add(1);
		}
	}

	#[cfg(test)]
	pub(super) fn test_record_count(&self) -> usize {
		self.intents.lock().unwrap().snapshot.len()
	}

	#[cfg(test)]
	pub(super) fn test_totals(&self) -> (u64, u64) {
		let totals = self.intents.lock().unwrap().totals();
		(totals.0, totals.1)
	}

	#[cfg(test)]
	pub(super) fn test_accounting(&self) -> (u64, u64, u64) {
		self.intents.lock().unwrap().totals()
	}

	fn update(&self, f: impl FnOnce(&mut CleanupIntentUnion)) {
		let mut intents = self.intents.lock().unwrap();
		let before = intents.totals();
		let records_before = intents.snapshot.len() as u64;
		f(&mut intents);
		let after = intents.totals();
		let records_after = intents.snapshot.len() as u64;
		let m = metrics();
		if records_after >= records_before {
			m.storage_tier_cleanup_records
				.add(records_after - records_before);
		} else {
			m.storage_tier_cleanup_records
				.sub(records_before - records_after);
		}
		if after.0 >= before.0 {
			m.storage_unresolved_tier_cleanup_intents
				.add(after.0 - before.0);
		} else {
			m.storage_unresolved_tier_cleanup_intents
				.sub(before.0 - after.0);
		}
		if after.1 >= before.1 {
			m.storage_unresolved_tier_cleanup_bytes
				.add(after.1 - before.1);
		} else {
			m.storage_unresolved_tier_cleanup_bytes
				.sub(before.1 - after.1);
		}
		if after.2 >= before.2 {
			m.storage_unresolved_tier_cleanup_unknown_size
				.add(after.2 - before.2);
		} else {
			m.storage_unresolved_tier_cleanup_unknown_size
				.sub(before.2 - after.2);
		}
	}

	fn insert(&self, slab_id: SlabId, uri: String, replacement: Option<String>, bytes: u64) {
		self.update(|intents| {
			intents.legacy.insert(slab_id, (uri, replacement, bytes));
		});
	}

	fn begin_snapshot(&self, slab_id: SlabId, uri: &str) {
		self.intents
			.lock()
			.unwrap()
			.pending
			.insert((slab_id, uri.to_owned()));
	}

	fn finish_snapshot(
		&self,
		slab_id: SlabId,
		uri: String,
		intent: RetainedObjectIntent,
		committed: bool,
	) {
		self.update(|intents| {
			intents
				.snapshot
				.entry((slab_id, uri.clone()))
				.or_insert(intent);
			if committed {
				intents.pending.remove(&(slab_id, uri));
			}
		});
	}

	fn insert_snapshot(&self, slab_id: SlabId, uri: String, intent: RetainedObjectIntent) {
		self.update(|intents| {
			intents.snapshot.insert((slab_id, uri), intent);
		});
	}

	fn publish_replacement(
		&self,
		slab_id: SlabId,
		old: String,
		replacement: &str,
		intent: RetainedObjectIntent,
	) {
		self.update(|intents| {
			let mut intent = intent;
			intent.published_replacements.insert(replacement.to_owned());
			if let Some(prior) = intents.snapshot.get(&(slab_id, old.clone())) {
				intent.bytes = prior.bytes;
				intent.replacement_bytes = prior.replacement_bytes;
				intent.retirement_source = intent.retirement_source.or(prior.retirement_source);
				intent
					.published_replacements
					.extend(prior.published_replacements.iter().cloned());
				if prior.state == RetainedObjectIntentState::ReplacementPublished {
					intent
						.published_replacements
						.extend(prior.replacement_uri.iter().cloned());
				}
			}
			let key = (slab_id, replacement.to_owned());
			if old != replacement {
				if let Some(provisional) = intents.snapshot.get(&key).filter(|p| {
					matches!(
						p.state,
						RetainedObjectIntentState::Unresolved
							| RetainedObjectIntentState::CandidateNotSubmitted
					) && p.replacement_uri.as_deref() == Some(replacement)
				}) {
					intent.replacement_bytes = intent.replacement_bytes.or(provisional.bytes);
					intents.snapshot.remove(&key);
				}
			}
			intents.snapshot.insert((slab_id, old), intent);
		});
	}

	fn unresolved_slabs(&self) -> BTreeSet<SlabId> {
		let slabs = {
			let intents = self.intents.lock().unwrap();
			intents
				.legacy
				.keys()
				.copied()
				.chain(intents.snapshot.keys().map(|(slab, _)| *slab))
				.chain(intents.pending.iter().map(|(slab, _)| *slab))
				.collect::<BTreeSet<_>>()
		};
		slabs
			.into_iter()
			.filter(|slab| self.contains(*slab))
			.collect()
	}

	fn contains(&self, slab_id: SlabId) -> bool {
		let intents = self.intents.lock().unwrap();
		let unresolved_legacy =
			intents
				.legacy
				.get(&slab_id)
				.is_some_and(|(old, replacement, _)| {
					!intents
						.snapshot
						.get(&(slab_id, old.clone()))
						.is_some_and(|record| {
							replacement
								.as_deref()
								.is_some_and(|uri| record.was_published(uri))
						})
				});
		unresolved_legacy
			|| intents.snapshot.iter().any(|((slab, _), record)| {
				*slab == slab_id
					&& matches!(
						record.state,
						RetainedObjectIntentState::Unresolved
							| RetainedObjectIntentState::CandidateNotSubmitted
					)
			}) || intents.pending.iter().any(|(slab, _)| *slab == slab_id)
	}

	fn published(&self, slab_id: SlabId, uri: &str, replacement: &str) -> bool {
		self.intents
			.lock()
			.unwrap()
			.snapshot
			.get(&(slab_id, uri.to_owned()))
			.is_some_and(|record| record.was_published(replacement))
	}
}

struct PendingSnapshotCleanup<'a> {
	cleanup: &'a UnresolvedTierCleanup,
	slab_id: SlabId,
	uri: String,
	intent: RetainedObjectIntent,
	finished: bool,
}

impl Drop for PendingSnapshotCleanup<'_> {
	fn drop(&mut self) {
		if !self.finished {
			self.cleanup.finish_snapshot(
				self.slab_id,
				self.uri.clone(),
				self.intent.clone(),
				false,
			);
		}
	}
}

impl Drop for UnresolvedTierCleanup {
	fn drop(&mut self) {
		if *self.missing_authority.get_mut() {
			metrics().storage_cleanup_authority_missing.sub(1);
		}
		let intents = self.intents.get_mut().unwrap();
		let totals = intents.totals();
		metrics()
			.storage_tier_cleanup_records
			.sub(intents.snapshot.len() as u64);
		metrics().release_storage_unresolved_tier_cleanup(totals.0, totals.1);
		metrics()
			.storage_unresolved_tier_cleanup_unknown_size
			.sub(totals.2);
	}
}

pub(super) fn table_drop_l2_cleanup_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!(
		"{TABLE_DROP_L2_CLEANUP_PREFIX}{:016x}{TABLE_DROP_L2_CLEANUP_SUFFIX}",
		slab_id
	))
}

fn parse_table_drop_l2_cleanup_filename(name: &str) -> Option<SlabId> {
	let raw = name
		.strip_prefix(TABLE_DROP_L2_CLEANUP_PREFIX)?
		.strip_suffix(TABLE_DROP_L2_CLEANUP_SUFFIX)?;
	(raw.len() == 16)
		.then(|| u64::from_str_radix(raw, 16).ok().map(SlabId::new))
		.flatten()
}

pub(super) fn parse_mvcc_rewrite_tmp_filename(name: &str) -> Option<SlabId> {
	let rest = name.strip_prefix("slab_")?;
	let rest = rest
		.strip_suffix(".wnd.retention.tmp")
		.or_else(|| rest.strip_suffix(".wnd.table_drop.tmp"))?;
	if rest.len() != 16 {
		return None;
	}
	u64::from_str_radix(rest, 16).ok().map(SlabId::new)
}

pub(super) async fn cleanup_retention_rewrite_tmp_files(
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
		if parse_mvcc_rewrite_tmp_filename(name).is_none() {
			continue;
		}
		unlink_local_file(file_ops, &dir.join(name)).await?;
		removed = removed.saturating_add(1);
	}
	if removed > 0 {
		tracing::warn!(
			dir = %dir.display(),
			removed_tmp_files = removed,
			"removed stale tiered retention rewrite temp files during open"
		);
	}
	Ok(())
}

impl SlabManager {
	pub(crate) fn retain_table_drop_scan_operations(
		&self,
		active: &BTreeSet<(TenantId, TableObjectId, u64)>,
	) {
		self.cleanup_scan_schedule
			.drops
			.retain(|operation, _| active.contains(operation));
	}

	pub(crate) async fn prepare_tier_candidate_proposal(
		&self,
		slab_id: SlabId,
		uri: &str,
	) -> std::io::Result<()> {
		self.set_tier_candidate_proposal_state(slab_id, uri, false, None)
			.await
	}

	/// Only pre-invocation code or B1N's definite refusal may call this.
	pub(crate) async fn record_tier_candidate_not_submitted(
		&self,
		slab_id: SlabId,
		uri: &str,
		bytes: u64,
	) -> std::io::Result<()> {
		self.set_tier_candidate_proposal_state(slab_id, uri, true, Some(bytes))
			.await
	}

	async fn set_tier_candidate_proposal_state(
		&self,
		slab_id: SlabId,
		uri: &str,
		not_submitted: bool,
		bytes: Option<u64>,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let _claim = self.cleanup_claim(slab_id).lock_owned().await;
		self.unresolved_tier_cleanup.begin_snapshot(slab_id, uri);
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		let record = index
			.set_tier_candidate_proposal_state(slab_id, uri.to_owned(), not_submitted, bytes)
			.await?;
		self.unresolved_tier_cleanup.update(|intents| {
			intents.snapshot.insert((slab_id, uri.to_owned()), record);
			intents.pending.remove(&(slab_id, uri.to_owned()));
		});
		Ok(())
	}

	pub(crate) async fn discard_unsubmitted_tier_candidate(
		&self,
		slab_id: SlabId,
		uri: &str,
		object_store: &dyn ObjectStore,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let _claim = self.cleanup_claim(slab_id).lock_owned().await;
		let expected = {
			let intents = self.unresolved_tier_cleanup.intents.lock().unwrap();
			if intents.pending.contains(&(slab_id, uri.to_owned()))
				|| !intents
					.snapshot
					.get(&(slab_id, uri.to_owned()))
					.is_some_and(|r| {
						r.state == RetainedObjectIntentState::CandidateNotSubmitted
							&& r.replacement_uri.as_deref() == Some(uri)
							&& r.published_replacements.is_empty()
							&& r.retirement_source.is_none()
					}) || intents
				.snapshot
				.iter()
				.any(|((slab, _), r)| *slab == slab_id && r.was_published(uri))
			{
				return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
			}
			intents
				.snapshot
				.iter()
				.filter(|((slab, name), record)| {
					*slab == slab_id
						&& (name == uri || record.replacement_uri.as_deref() == Some(uri))
				})
				.map(|((_, name), record)| (name.clone(), record.clone()))
				.collect::<BTreeMap<_, _>>()
		};
		if self
			.tier_pointer(slab_id)
			.await
			.is_some_and(|p| p.l2_uri == uri)
			|| self
				.unapplied_tier_pointer(slab_id)
				.is_some_and(|p| p.l2_uri == uri)
		{
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		match object_store.delete(uri).await {
			Ok(()) => {}
			Err(error) if error.kind() == ErrorKind::NotFound => {}
			Err(error) => return Err(error),
		}
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		let changes = index
			.clear_unsubmitted_tier_candidate(
				slab_id,
				uri.to_owned(),
				expected
					.keys()
					.filter(|name| name.as_str() != uri)
					.cloned()
					.collect(),
			)
			.await?;
		#[cfg(test)]
		self.candidate_after_clear_pause_point().await;
		self.unresolved_tier_cleanup.update(|intents| {
			for (name, record) in changes {
				let key = (slab_id, name.clone());
				if intents.snapshot.get(&key) != expected.get(&name) {
					continue;
				}
				intents.pending.remove(&key);
				match record {
					Some(record) => {
						intents.snapshot.insert(key, record);
					}
					None => {
						intents.snapshot.remove(&key);
					}
				}
			}
		});
		Ok(())
	}

	pub(crate) fn set_table_drop_l2_deferred(
		&self,
		tenant: TenantId,
		table: TableObjectId,
		generation: u64,
		deferred: bool,
	) {
		self.deferred_table_drop_l2.update(|debts| {
			let key = (tenant, table, generation);
			if deferred {
				debts.insert(key);
			} else {
				debts.remove(&key);
			}
		});
	}

	pub(crate) fn replace_table_drop_l2_deferred(
		&self,
		active: &BTreeSet<(TenantId, TableObjectId, u64)>,
	) {
		self.deferred_table_drop_l2
			.update(|debts| *debts = active.clone());
	}

	#[cfg(test)]
	pub(crate) fn bind_table_drop_l2_gauge_for_test(&self, gauge: &'static crate::metrics::Gauge) {
		assert!(self.deferred_table_drop_l2.0.lock().unwrap().is_empty());
		*self.deferred_table_drop_l2.1.lock().unwrap() = Some(gauge);
	}

	#[cfg(test)]
	pub(crate) fn table_drop_l2_deferred_count(&self) -> usize {
		self.deferred_table_drop_l2.0.lock().unwrap().len()
	}

	#[cfg(test)]
	pub(crate) fn retained_cleanup_test_accounting(&self) -> (u64, u64, u64) {
		self.unresolved_tier_cleanup.test_accounting()
	}

	#[cfg(test)]
	pub(crate) fn retained_cleanup_test_crypto(&self) -> Arc<CryptoManager> {
		self.config.crypto.clone()
	}

	pub(super) fn cleanup_claim(&self, slab_id: SlabId) -> Arc<tokio::sync::Mutex<()>> {
		self.cleanup_claims
			.entry(slab_id)
			.or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
			.clone()
	}

	pub(super) fn note_tier_republished(&self, slab_id: SlabId, uri: &str, index: u64) {
		self.unresolved_tier_cleanup.update(|intents| {
			if let Some(record) = intents.snapshot.get_mut(&(slab_id, uri.to_owned())) {
				if record
					.retirement_source
					.is_some_and(|source| index > source.index())
				{
					record.retirement_source = None;
					if record.state == RetainedObjectIntentState::RetirementPublished {
						record.state = RetainedObjectIntentState::ReplacementPublished;
					}
				}
			}
		});
	}

	pub(super) fn snapshot_retained_size_hint(&self, slab_id: SlabId, uri: &str) -> Option<u64> {
		let intents = self.unresolved_tier_cleanup.intents.lock().unwrap();
		intents
			.snapshot
			.get(&(slab_id, uri.to_owned()))
			.and_then(|record| record.bytes)
			.or_else(|| {
				intents
					.snapshot
					.iter()
					.filter(|((slab, prior_uri), record)| {
						*slab == slab_id
							&& prior_uri != uri && record.replacement_uri.as_deref() == Some(uri)
					})
					.find_map(|(_, record)| record.replacement_bytes)
			})
	}

	pub(super) fn snapshot_retirement_evidence(
		&self,
		slab_id: SlabId,
		uri: &str,
	) -> Vec<(String, RetainedObjectIntent)> {
		let intents = self.unresolved_tier_cleanup.intents.lock().unwrap();
		let mut evidence = intents
			.snapshot
			.iter()
			.filter(|((slab, prior), record)| {
				*slab == slab_id && prior != uri && record.replacement_uri.as_deref() == Some(uri)
			})
			.map(|((_, prior), record)| (prior.clone(), record.clone()))
			.collect::<Vec<_>>();
		if let Some((prior, replacement, bytes)) = intents.legacy.get(&slab_id) {
			if replacement.as_deref() == Some(uri) && !evidence.iter().any(|(key, _)| key == prior)
			{
				evidence.push((
					prior.clone(),
					RetainedObjectIntent {
						bytes: Some(*bytes),
						replacement_uri: replacement.clone(),
						replacement_bytes: None,
						published_replacements: Default::default(),
						retirement_source: None,
						state: RetainedObjectIntentState::ReplacementPublished,
					},
				));
			}
		}
		evidence
	}

	pub(super) fn note_snapshot_retirement_applied(
		&self,
		slab_id: SlabId,
		uri: String,
		intent: RetainedObjectIntent,
		evidence: Vec<(String, RetainedObjectIntent)>,
	) {
		self.unresolved_tier_cleanup.update(|intents| {
			for (prior, record) in evidence {
				intents.snapshot.insert((slab_id, prior), record);
			}
			intents.snapshot.insert((slab_id, uri), intent);
		});
	}

	pub(crate) fn require_cleanup_authority(&self) -> std::io::Result<()> {
		if self
			.persistent_index
			.as_ref()
			.is_some_and(|index| index.cleanup_authority_available())
		{
			Ok(())
		} else {
			Err(Error::new(
				ErrorKind::WouldBlock,
				"cleanup authority unavailable until restart",
			))
		}
	}

	pub(super) async fn load_unresolved_tier_cleanup(&self) -> std::io::Result<()> {
		for entry in self.file_ops.read_dir(&self.config.dir).await? {
			if !entry.is_file {
				continue;
			}
			let Some(slab_id) = entry
				.file_name
				.to_str()
				.and_then(parse_table_drop_l2_cleanup_filename)
			else {
				continue;
			};
			let marker = self.read_table_drop_cleanup_intent(slab_id).await?;
			self.unresolved_tier_cleanup.insert(
				slab_id,
				marker.old_l2_uri,
				marker.replacement_l2_uri,
				marker.retained_bytes,
			);
		}
		if let Some(index) = &self.persistent_index {
			if !index.cleanup_authority_available() {
				self.unresolved_tier_cleanup.mark_missing_authority();
				return Ok(());
			}
			for (slab_id, uri, intent) in index.load_snapshot_cleanup_intents().await? {
				self.unresolved_tier_cleanup
					.insert_snapshot(slab_id, uri, intent);
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub(crate) async fn persist_snapshot_cleanup_intent(
		&self,
		slab_id: SlabId,
		uri: &str,
		bytes: u64,
	) -> std::io::Result<()> {
		self.persist_retained_intent(slab_id, uri, bytes, None)
			.await
	}

	/// An ambiguous upload is unresolved until that exact URI is observed live.
	pub(crate) async fn persist_ambiguous_upload_intent(
		&self,
		slab_id: SlabId,
		uri: &str,
		bytes: u64,
	) -> std::io::Result<()> {
		self.persist_retained_intent(slab_id, uri, bytes, Some(uri.to_owned()))
			.await
	}

	async fn persist_retained_intent(
		&self,
		slab_id: SlabId,
		uri: &str,
		bytes: u64,
		replacement_uri: Option<String>,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		let _claim = self.cleanup_claim(slab_id).lock_owned().await;
		let (existing, pending) = {
			let intents = self.unresolved_tier_cleanup.intents.lock().unwrap();
			let key = (slab_id, uri.to_owned());
			(
				intents.snapshot.get(&key).cloned(),
				intents.pending.contains(&key),
			)
		};
		if let Some(existing) = &existing {
			if replacement_uri.is_some() && existing.replacement_uri != replacement_uri {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"conflicting retained-object intent",
				));
			}
			if !pending {
				return Ok(());
			}
		}
		// A failed or cancelled write leaves conservative memory and a pending
		// fence. Retry the exact durable intent before reporting success.
		let intent = existing.unwrap_or(RetainedObjectIntent {
			bytes: Some(bytes),
			replacement_uri,
			replacement_bytes: None,
			published_replacements: Default::default(),
			retirement_source: None,
			state: RetainedObjectIntentState::Unresolved,
		});
		self.unresolved_tier_cleanup.begin_snapshot(slab_id, uri);
		let mut pending = PendingSnapshotCleanup {
			cleanup: &self.unresolved_tier_cleanup,
			slab_id,
			uri: uri.to_owned(),
			intent: intent.clone(),
			finished: false,
		};
		let result = match &self.persistent_index {
			Some(index) => {
				index
					.put_snapshot_cleanup_intent(slab_id, uri.to_owned(), intent.clone())
					.await
			}
			None => Err(Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for cleanup intent",
			)),
		};
		self.unresolved_tier_cleanup.finish_snapshot(
			slab_id,
			uri.to_owned(),
			intent,
			result.is_ok(),
		);
		pending.finished = true;
		result
	}

	async fn read_table_drop_cleanup_intent(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<PendingTableDropL2Cleanup> {
		let path = table_drop_l2_cleanup_path(&self.config.dir, slab_id);
		let bytes = self.file_ops.read_all(&path).await?;
		let marker: PendingTableDropL2Cleanup = postcard::from_bytes(&bytes).map_err(|error| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid tier cleanup intent: {error}"),
			)
		})?;
		if marker.format_version != 2 || marker.slab_id != slab_id {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"tier cleanup intent identity or format mismatch",
			));
		}
		Ok(marker)
	}

	#[cfg(test)]
	pub(crate) async fn persist_table_drop_l2_cleanup(
		&self,
		slab_id: SlabId,
		old_l2_uri: &str,
		replacement_l2_uri: Option<&str>,
	) -> std::io::Result<()> {
		let store = self
			.config
			.l2
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "cleanup intent requires L2"))?;
		let replacement = match replacement_l2_uri {
			Some(uri) => Some((uri, store.head(uri).await?.size_bytes)),
			None => None,
		};
		let old_bytes = store.head(old_l2_uri).await?.size_bytes;
		self.stage_table_drop_l2_cleanup(slab_id, old_l2_uri, Some(old_bytes), replacement)
			.await
	}

	/// Durably stage both objects before the replacement upload begins.
	pub(crate) async fn stage_table_drop_l2_cleanup(
		&self,
		slab_id: SlabId,
		old_l2_uri: &str,
		old_bytes: Option<u64>,
		replacement: Option<(&str, u64)>,
	) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		if self.unresolved_tier_cleanup.contains(slab_id) {
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		let replacement_l2_uri = replacement.map(|(uri, _)| uri);
		let mut provisional =
			replacement
				.filter(|(uri, _)| *uri != old_l2_uri)
				.map(|(uri, bytes)| {
					(
						uri.to_owned(),
						RetainedObjectIntent {
							bytes: Some(bytes),
							replacement_uri: Some(uri.to_owned()),
							replacement_bytes: None,
							published_replacements: Default::default(),
							retirement_source: None,
							state: RetainedObjectIntentState::CandidateNotSubmitted,
						},
					)
				});
		let mut intent = RetainedObjectIntent {
			bytes: old_bytes,
			replacement_uri: replacement_l2_uri.map(str::to_owned),
			replacement_bytes: replacement.map(|(_, bytes)| bytes),
			published_replacements: Default::default(),
			retirement_source: None,
			state: RetainedObjectIntentState::Unresolved,
		};
		let _claim = self.cleanup_claim(slab_id).lock_owned().await;
		if self.unresolved_tier_cleanup.contains(slab_id) {
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		if let Some(prior) = self
			.unresolved_tier_cleanup
			.intents
			.lock()
			.unwrap()
			.snapshot
			.get(&(slab_id, old_l2_uri.to_owned()))
			.cloned()
		{
			if matches!(
				prior.state,
				RetainedObjectIntentState::ReplacementPublished
					| RetainedObjectIntentState::RetirementPublished
			) {
				intent.bytes = prior.bytes;
				intent.retirement_source = prior.retirement_source;
				intent.published_replacements = prior.published_replacements;
				intent.published_replacements.extend(prior.replacement_uri);
			}
		}
		if let Some((uri, requested)) = &provisional {
			let prior = self
				.unresolved_tier_cleanup
				.intents
				.lock()
				.unwrap()
				.snapshot
				.get(&(slab_id, uri.clone()))
				.cloned();
			if let Some(prior) = prior {
				if !matches!(
					prior.state,
					RetainedObjectIntentState::ReplacementPublished
						| RetainedObjectIntentState::RetirementPublished
				) || prior
					.bytes
					.zip(requested.bytes)
					.is_some_and(|(a, b)| a != b)
				{
					return Err(Error::new(
						ErrorKind::InvalidData,
						"conflicting replacement history",
					));
				}
				// This URI is already counted by its historical record. Preserve it.
				provisional = None;
			}
		}
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for cleanup intent",
			)
		})?;
		self.unresolved_tier_cleanup
			.begin_snapshot(slab_id, old_l2_uri);
		if let Some((uri, _)) = &provisional {
			self.unresolved_tier_cleanup.begin_snapshot(slab_id, uri);
		}
		index
			.put_cleanup_intent_pair(
				slab_id,
				old_l2_uri.to_owned(),
				intent.clone(),
				provisional.clone(),
			)
			.await?;
		self.unresolved_tier_cleanup.update(|intents| {
			intents
				.snapshot
				.insert((slab_id, old_l2_uri.to_owned()), intent);
			intents.pending.remove(&(slab_id, old_l2_uri.to_owned()));
			if let Some((uri, item)) = provisional {
				intents.pending.remove(&(slab_id, uri.clone()));
				intents.snapshot.insert((slab_id, uri), item);
			}
		});
		Ok(())
	}

	pub(crate) async fn record_cleanup_replacement_published(
		&self,
		slab_id: SlabId,
		old_uri: &str,
		replacement: &str,
	) -> std::io::Result<()> {
		let _claim = self.cleanup_claim(slab_id).lock_owned().await;
		if self
			.unresolved_tier_cleanup
			.published(slab_id, old_uri, replacement)
		{
			return Ok(());
		}
		if self.unapplied_tier_pointer(slab_id).is_some()
			|| self
				.tier_pointer(slab_id)
				.await
				.as_ref()
				.map(|p| p.l2_uri.as_str())
				!= Some(replacement)
		{
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		let intent = self
			.unresolved_tier_cleanup
			.intents
			.lock()
			.unwrap()
			.snapshot
			.get(&(slab_id, old_uri.to_owned()))
			.cloned()
			.ok_or_else(|| Error::new(ErrorKind::NotFound, "cleanup intent missing"))?;
		if intent.replacement_uri.as_deref() != Some(replacement) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"cleanup replacement mismatch",
			));
		}
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for cleanup intent",
			)
		})?;
		index
			.mark_cleanup_replacement_published(
				slab_id,
				old_uri.to_owned(),
				replacement.to_owned(),
				intent.bytes,
			)
			.await?;
		self.unresolved_tier_cleanup.publish_replacement(
			slab_id,
			old_uri.to_owned(),
			replacement,
			RetainedObjectIntent {
				state: RetainedObjectIntentState::ReplacementPublished,
				..intent
			},
		);
		Ok(())
	}

	/// Preserve the local generation a snapshot is about to supersede. Sizes
	/// come only from records already held here; unknown stays unknown.
	pub(crate) async fn record_retained_pointer_transition(
		&self,
		old: &TieredSlabPointer,
		replacement: Option<&TieredSlabPointer>,
		locally_published: bool,
	) -> std::io::Result<()> {
		if self
			.persistent_index
			.as_ref()
			.is_some_and(|index| !index.cleanup_authority_available())
		{
			// The committed pointer transaction records this accounting gap.
			return Ok(());
		}
		if replacement.is_some_and(|pointer| old.l2_uri == pointer.l2_uri) {
			return Ok(());
		}
		let slab_id = old.slab_id;
		let _claim = self.cleanup_claim(slab_id).lock_owned().await;
		let (known_bytes, evidence, existing_retained) = {
			let intents = self.unresolved_tier_cleanup.intents.lock().unwrap();
			let provisional = intents
				.snapshot
				.get(&(slab_id, old.l2_uri.clone()))
				.filter(|p| {
					matches!(
						p.state,
						RetainedObjectIntentState::Unresolved
							| RetainedObjectIntentState::CandidateNotSubmitted
					) && p.replacement_uri.as_deref() == Some(old.l2_uri.as_str())
				});
			let existing_retained = intents
				.snapshot
				.get(&(slab_id, old.l2_uri.clone()))
				.filter(|p| {
					!(matches!(
						p.state,
						RetainedObjectIntentState::Unresolved
							| RetainedObjectIntentState::CandidateNotSubmitted
					) && p.replacement_uri.as_deref() == Some(old.l2_uri.as_str()))
				})
				.cloned();
			let mut predecessors = intents
				.snapshot
				.iter()
				.filter(|((slab, uri), record)| {
					*slab == slab_id
						&& uri != &old.l2_uri
						&& record.replacement_uri.as_deref() == Some(old.l2_uri.as_str())
				})
				.map(|((_, uri), record)| (uri.clone(), record.clone()))
				.collect::<Vec<_>>();
			let known = provisional
				.and_then(|p| p.bytes)
				.or_else(|| predecessors.iter().find_map(|(_, p)| p.replacement_bytes));
			if locally_published {
				if let Some((uri, next, bytes)) = intents.legacy.get(&slab_id) {
					if next.as_deref() == Some(old.l2_uri.as_str())
						&& !predecessors.iter().any(|(key, _)| key == uri)
					{
						predecessors.push((
							uri.clone(),
							RetainedObjectIntent {
								bytes: Some(*bytes),
								replacement_uri: next.clone(),
								replacement_bytes: None,
								published_replacements: Default::default(),
								retirement_source: None,
								state: RetainedObjectIntentState::ReplacementPublished,
							},
						));
					}
				}
			} else {
				predecessors.clear();
			}
			let evidence = predecessors;
			(known, evidence, existing_retained)
		};
		let retained = match existing_retained {
			Some(mut prior)
				if prior.state == RetainedObjectIntentState::ReplacementPublished
					&& replacement.is_some_and(|pointer| !prior.was_published(&pointer.l2_uri)) =>
			{
				prior
					.published_replacements
					.extend(prior.replacement_uri.take());
				prior.replacement_uri = replacement.map(|pointer| pointer.l2_uri.clone());
				prior.replacement_bytes = None;
				prior.state = RetainedObjectIntentState::Unresolved;
				prior
			}
			Some(prior) => prior,
			None => RetainedObjectIntent {
				bytes: known_bytes,
				replacement_uri: replacement.map(|pointer| pointer.l2_uri.clone()),
				replacement_bytes: None,
				published_replacements: Default::default(),
				retirement_source: None,
				state: RetainedObjectIntentState::Unresolved,
			},
		};
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for snapshot retained object",
			)
		})?;
		self.unresolved_tier_cleanup
			.begin_snapshot(slab_id, &old.l2_uri);
		index
			.stage_snapshot_retained_object(
				slab_id,
				old.l2_uri.clone(),
				retained.clone(),
				evidence.clone(),
			)
			.await?;
		self.unresolved_tier_cleanup.update(|intents| {
			for (uri, mut prior) in evidence {
				prior.published_replacements.insert(old.l2_uri.clone());
				prior.state = RetainedObjectIntentState::ReplacementPublished;
				intents.snapshot.insert((slab_id, uri), prior);
			}
			intents.pending.remove(&(slab_id, old.l2_uri.clone()));
			intents
				.snapshot
				.insert((slab_id, old.l2_uri.clone()), retained);
		});
		Ok(())
	}

	pub(crate) fn refuse_unresolved_cleanup(&self, slab_id: SlabId) -> std::io::Result<()> {
		if self.unresolved_tier_cleanup.contains(slab_id) {
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		Ok(())
	}

	pub(crate) async fn reconcile_table_drop_l2_cleanup_markers(
		&self,
		object_store: &dyn ObjectStore,
		examined_budget: usize,
	) -> std::io::Result<(usize, bool)> {
		self.reconcile_table_drop_l2_cleanup_impl(object_store, examined_budget, false)
			.await?
			.legacy_result()
	}
	pub(crate) async fn reconcile_unresolved_table_drop_cleanup(
		&self,
		object_store: &dyn ObjectStore,
		examined_budget: usize,
	) -> std::io::Result<CleanupReconcileReport> {
		self.reconcile_table_drop_l2_cleanup_impl(object_store, examined_budget, true)
			.await
	}
	async fn reconcile_table_drop_l2_cleanup_impl(
		&self,
		object_store: &dyn ObjectStore,
		examined_budget: usize,
		unresolved_only: bool,
	) -> std::io::Result<CleanupReconcileReport> {
		self.require_cleanup_authority()?;
		// Directory discovery is global authority: a failure here still stops
		// initiative. Errors while processing one known slab are deferred below.
		let mut markers = self
			.file_ops
			.read_dir(&self.config.dir)
			.await?
			.into_iter()
			.filter_map(|entry| {
				entry
					.is_file
					.then(|| {
						entry
							.file_name
							.to_str()
							.and_then(parse_table_drop_l2_cleanup_filename)
					})
					.flatten()
			})
			.collect::<Vec<_>>();
		let live_uris = self
			.list_tiered_slabs()
			.await
			.into_iter()
			.map(|p| (p.slab_id, p.l2_uri))
			.collect::<BTreeMap<_, _>>();
		let mut work = Vec::new();
		{
			let intents = self.unresolved_tier_cleanup.intents.lock().unwrap();
			let candidates = intents
				.snapshot
				.iter()
				.filter(|(key, record)| {
					record.state == RetainedObjectIntentState::CandidateNotSubmitted
						&& live_uris.get(&key.0) != Some(&key.1)
						&& !intents.pending.contains(*key)
				})
				.map(|(key, _)| key.clone())
				.collect::<BTreeSet<_>>();
			for (slab, uri) in &candidates {
				work.push((
					(*slab, 0, uri.clone()),
					CleanupWork::Candidate(*slab, uri.clone()),
				));
			}
			for ((slab, uri), record) in &intents.snapshot {
				let provisional = record.replacement_uri.as_deref() == Some(uri.as_str());
				let owned = intents
					.legacy
					.get(slab)
					.is_some_and(|(_, replacement, _)| replacement.as_ref() == Some(uri))
					|| intents
						.snapshot
						.iter()
						.any(|((other_slab, other_uri), other)| {
							other_slab == slab
								&& other_uri != uri && other.replacement_uri.as_ref() == Some(uri)
						});
				let candidate_owned = candidates.contains(&(*slab, uri.clone()))
					|| record.replacement_uri.as_ref().is_some_and(|replacement| {
						candidates.contains(&(*slab, replacement.clone()))
					});
				if matches!(
					record.state,
					RetainedObjectIntentState::Unresolved
						| RetainedObjectIntentState::CandidateNotSubmitted
				) && !candidate_owned
					&& (!provisional || !owned)
					&& !intents
						.legacy
						.get(slab)
						.is_some_and(|(old, replacement, _)| {
							old == uri && record.replacement_uri == *replacement
						}) {
					work.push((
						(*slab, 1, uri.clone()),
						CleanupWork::Intent(*slab, uri.clone()),
					));
				}
			}
			if unresolved_only {
				markers.retain(|slab| {
					!intents
						.legacy
						.get(slab)
						.is_some_and(|(old, replacement, _)| {
							intents
								.snapshot
								.get(&(*slab, old.clone()))
								.is_some_and(|record| {
									replacement
										.as_deref()
										.is_some_and(|uri| record.was_published(uri))
								})
						})
				});
			}
		}
		for slab in markers {
			work.push(((slab, 2, String::new()), CleanupWork::Legacy(slab)));
		}
		work.sort_by(|a, b| a.0.cmp(&b.0));
		if let Some(after) = self.cleanup_scan_schedule.after.lock().unwrap().as_ref() {
			let split = work.partition_point(|(key, _)| key <= after);
			work.rotate_left(split);
		}
		let total = work.len();
		let mut report = CleanupReconcileReport {
			fully_scanned: total <= examined_budget,
			..Default::default()
		};
		for (key, item) in work.into_iter().take(examined_budget) {
			report.examined += 1;
			let slab = key.0;
			let outcome = self.reconcile_cleanup_work(item, object_store).await;
			*self.cleanup_scan_schedule.after.lock().unwrap() = Some(key);
			if let Err(error) = outcome {
				tracing::warn!(slab_id=slab.get(),error=%error,"cleanup deferred for this slab; continuing the scan");
				report.deferred_slabs.insert(slab);
				if report.first_error.is_none() {
					report.first_error = Some(error);
				}
			}
		}
		let pending = self.unresolved_tier_cleanup.unresolved_slabs();
		report.has_pending = !pending.is_empty();
		report.deferred_slabs.extend(pending);
		Ok(report)
	}
	async fn reconcile_cleanup_work(
		&self,
		work: CleanupWork,
		object_store: &dyn ObjectStore,
	) -> std::io::Result<()> {
		match work {
			CleanupWork::Candidate(slab, uri) => {
				#[cfg(test)]
				self.candidate_reconcile_before_rewrite_pause_point().await;
				let _rewrite = self.acquire_slab_rewrite_exclusive_claim(slab).await;
				let still_unsubmitted = self
					.unresolved_tier_cleanup
					.intents
					.lock()
					.unwrap()
					.snapshot
					.get(&(slab, uri.clone()))
					.is_some_and(|record| {
						record.state == RetainedObjectIntentState::CandidateNotSubmitted
					});
				if !still_unsubmitted
					|| self
						.tier_pointer(slab)
						.await
						.is_some_and(|p| p.l2_uri == uri)
				{
					return Ok(());
				}
				self.discard_unsubmitted_tier_candidate(slab, &uri, object_store)
					.await
			}
			CleanupWork::Intent(slab_id, uri) => {
				let _claim = self.cleanup_claim(slab_id).lock_owned().await;
				let Some(intent) = self
					.unresolved_tier_cleanup
					.intents
					.lock()
					.unwrap()
					.snapshot
					.get(&(slab_id, uri.clone()))
					.cloned()
				else {
					return Ok(());
				};
				if !matches!(
					intent.state,
					RetainedObjectIntentState::Unresolved
						| RetainedObjectIntentState::CandidateNotSubmitted
				) {
					return Ok(());
				}
				let Some(replacement) = intent.replacement_uri.as_deref() else {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				};
				if self
					.unresolved_tier_cleanup
					.published(slab_id, &uri, replacement)
				{
					return Ok(());
				}
				if self.unapplied_tier_pointer(slab_id).is_some()
					|| self
						.tier_pointer(slab_id)
						.await
						.as_ref()
						.map(|p| p.l2_uri.as_str())
						!= Some(replacement)
				{
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				}
				let index = self.persistent_index.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::WouldBlock,
						"persistent index unavailable for cleanup intent",
					)
				})?;
				if uri == replacement {
					index
						.remove_standalone_live_provisional(slab_id, uri.clone())
						.await?;
					self.unresolved_tier_cleanup.update(|intents| {
						intents.snapshot.remove(&(slab_id, uri));
					});
					return Ok(());
				}
				index
					.mark_cleanup_replacement_published(
						slab_id,
						uri.clone(),
						replacement.to_owned(),
						intent.bytes,
					)
					.await?;
				self.unresolved_tier_cleanup.publish_replacement(
					slab_id,
					uri,
					replacement,
					RetainedObjectIntent {
						state: RetainedObjectIntentState::ReplacementPublished,
						..intent.clone()
					},
				);
				Ok(())
			}
			CleanupWork::Legacy(slab_id) => {
				let _claim = self.cleanup_claim(slab_id).lock_owned().await;
				#[cfg(test)]
				self.cleanup_reconcile_claim_pause_point().await;
				let marker = self.read_table_drop_cleanup_intent(slab_id).await?;
				let Some(replacement) = marker.replacement_l2_uri.as_deref() else {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				};
				if self
					.unresolved_tier_cleanup
					.published(slab_id, &marker.old_l2_uri, replacement)
				{
					return Ok(());
				}
				if self.unapplied_tier_pointer(slab_id).is_some()
					|| self
						.tier_pointer(slab_id)
						.await
						.as_ref()
						.map(|p| p.l2_uri.as_str())
						!= Some(replacement)
				{
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				}
				let index = self.persistent_index.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::WouldBlock,
						"persistent index unavailable for cleanup intent",
					)
				})?;
				index
					.mark_cleanup_replacement_published(
						slab_id,
						marker.old_l2_uri.clone(),
						replacement.to_owned(),
						Some(marker.retained_bytes),
					)
					.await?;
				self.unresolved_tier_cleanup.publish_replacement(
					slab_id,
					marker.old_l2_uri,
					replacement,
					RetainedObjectIntent {
						bytes: Some(marker.retained_bytes),
						replacement_uri: Some(replacement.to_owned()),
						replacement_bytes: None,
						published_replacements: Default::default(),
						retirement_source: None,
						state: RetainedObjectIntentState::ReplacementPublished,
					},
				);
				Ok(())
			}
		}
	}

	pub async fn reclaim_table_drop_storage_slice(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		cursor: TableDropReclaimCursor,
		examined_budget: usize,
	) -> std::io::Result<TableDropReclaimSliceReport> {
		self.require_tier_authority_ready()?;
		descriptor.validate()?;
		if descriptor.table_object_id == TableObjectId::DEFAULT_KV || examined_budget == 0 {
			return Ok(TableDropReclaimSliceReport {
				cursor_next: cursor,
				done: descriptor.table_object_id == TableObjectId::DEFAULT_KV,
				..TableDropReclaimSliceReport::default()
			});
		}
		let _ = self.seal_active_for_table_drop_reclaim().await?;
		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};
		let upper_bound = cursor
			.upper_bound_slab_id
			.or_else(|| sealed.last().map(|(slab_id, _)| *slab_id));
		let mut report = TableDropReclaimSliceReport {
			cursor_next: TableDropReclaimCursor {
				after_slab_id: cursor.after_slab_id,
				upper_bound_slab_id: upper_bound,
			},
			..TableDropReclaimSliceReport::default()
		};
		let candidates = sealed.into_iter().filter(|(slab_id, _)| {
			cursor.after_slab_id.is_none_or(|after| *slab_id > after)
				&& upper_bound.is_none_or(|upper| *slab_id <= upper)
		});
		for (slab_id, _stale_handle) in candidates.take(examined_budget) {
			report.examined += 1;
			report.cursor_next.after_slab_id = Some(slab_id);
			if let Some(outcome) = self
				.reclaim_one_sealed_table_drop_slab(
					slab_id,
					tenant_id,
					descriptor,
					table_schemas,
					secondary_indexes,
				)
				.await?
			{
				report.rewritten += 1;
				report.reclaimed_records = report
					.reclaimed_records
					.saturating_add(outcome.reclaimed_records);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		report.done = match upper_bound {
			None => true,
			Some(upper) => report
				.cursor_next
				.after_slab_id
				.is_some_and(|after| after >= upper),
		};
		Ok(report)
	}

	pub async fn reclaim_table_drop_tiered_storage_slice(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		committer: &dyn TierCommitter,
		cursor: TableDropReclaimCursor,
		examined_budget: usize,
	) -> std::io::Result<TableDropReclaimSliceReport> {
		// The coordinator can ask a follower participant to reclaim its local
		// debt. Its L2 phase remains pending until this node may propose.
		if !committer.should_propose() {
			self.set_table_drop_l2_deferred(
				tenant_id,
				descriptor.table_object_id,
				descriptor.schema_generation,
				true,
			);
			return Ok(TableDropReclaimSliceReport {
				status: TableDropReclaimStatus::DeferredNotLeader,
				cursor_next: cursor,
				..TableDropReclaimSliceReport::default()
			});
		}
		self.set_table_drop_l2_deferred(
			tenant_id,
			descriptor.table_object_id,
			descriptor.schema_generation,
			false,
		);

		self.require_cleanup_authority()?;
		self.require_tier_authority_ready()?;
		descriptor.validate()?;
		if descriptor.table_object_id == TableObjectId::DEFAULT_KV || examined_budget == 0 {
			return Ok(TableDropReclaimSliceReport {
				cursor_next: cursor,
				done: descriptor.table_object_id == TableObjectId::DEFAULT_KV,
				..TableDropReclaimSliceReport::default()
			});
		}
		let mut pointers = self.list_tiered_slabs().await;
		pointers.sort_by_key(|pointer| pointer.slab_id);
		let upper_bound = cursor
			.upper_bound_slab_id
			.or_else(|| pointers.last().map(|pointer| pointer.slab_id));
		let mut report = TableDropReclaimSliceReport {
			cursor_next: TableDropReclaimCursor {
				after_slab_id: cursor.after_slab_id,
				upper_bound_slab_id: upper_bound,
			},
			..TableDropReclaimSliceReport::default()
		};
		if pointers.is_empty() && self.config.l2.is_none() {
			report.done = true;
			return Ok(report);
		}
		let object_store = self.config.l2.clone().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				"table drop tiered reclaim requires an L2 object store",
			)
		})?;
		let operation = (
			tenant_id,
			descriptor.table_object_id,
			descriptor.schema_generation,
		);
		let state = self
			.cleanup_scan_schedule
			.drops
			.entry(operation)
			.or_insert_with(|| {
				Arc::new(tokio::sync::Mutex::new(TableDropSliceScan::start(
					cursor,
					upper_bound,
				)))
			})
			.clone();
		let mut scan = state.lock_owned().await;
		let still_blocked = scan.blocked.is_some_and(|slab| {
			self.unresolved_tier_cleanup.contains(slab)
				|| self.refuse_proposal_over_unapplied_pointer(slab).is_err()
		});
		if scan.watermark != cursor.after_slab_id
			|| scan.upper != upper_bound
			|| (scan.blocked.is_some() && !still_blocked && scan.cleanup_next)
		{
			*scan = TableDropSliceScan::start(cursor, upper_bound);
		}
		let cleanup_budget = if scan.cleanup_next {
			if examined_budget == 1 {
				1
			} else {
				examined_budget / 2 + examined_budget % 2
			}
		} else {
			0
		};
		let cleanup = self
			.reconcile_unresolved_table_drop_cleanup(object_store.as_ref(), cleanup_budget)
			.await?;
		report.examined = cleanup.examined;
		let cleanup_done =
			cleanup.fully_scanned && !cleanup.has_pending && cleanup.first_error.is_none();
		let cleanup_deferred = cleanup.deferred_slabs.clone();
		if cleanup.examined > 0 || cleanup.has_pending {
			scan.cleanup_next = !scan.cleanup_next;
		} else {
			scan.cleanup_next = true;
		}
		for slab in cleanup.deferred_slabs {
			if cursor.after_slab_id.is_none_or(|after| slab > after)
				&& upper_bound.is_none_or(|upper| slab <= upper)
			{
				scan.blocked = Some(scan.blocked.map_or(slab, |old| old.min(slab)));
			}
		}
		let pointer_ids = pointers.iter().map(|p| p.slab_id).collect::<Vec<_>>();
		let after = scan.after;
		let candidates = pointers.into_iter().filter(|pointer| {
			after.is_none_or(|after| pointer.slab_id > after)
				&& upper_bound.is_none_or(|upper| pointer.slab_id <= upper)
		});
		let mut pointer_end = true;

		for pointer in candidates {
			if report.examined == examined_budget {
				pointer_end = false;
				break;
			}
			scan.after = Some(pointer.slab_id);
			if scan.blocked == Some(pointer.slab_id)
				|| cleanup_deferred.contains(&pointer.slab_id)
				|| self.unresolved_tier_cleanup.contains(pointer.slab_id)
				|| self
					.refuse_proposal_over_unapplied_pointer(pointer.slab_id)
					.is_err()
			{
				scan.blocked = Some(
					scan.blocked
						.map_or(pointer.slab_id, |old| old.min(pointer.slab_id)),
				);
				continue;
			}
			let _slab_rewrite_claim = self
				.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
				.await;
			report.examined += 1;
			report.cursor_next.after_slab_id = Some(pointer.slab_id);
			if cleanup_deferred.contains(&pointer.slab_id)
				|| self
					.refuse_proposal_over_unapplied_pointer(pointer.slab_id)
					.is_err() || self.unresolved_tier_cleanup.contains(pointer.slab_id)
			{
				scan.blocked = Some(
					scan.blocked
						.map_or(pointer.slab_id, |old| old.min(pointer.slab_id)),
				);
				continue;
			}
			let previous_blocked = scan.blocked;
			// A failed attempt must not publish a cursor receipt past this slab.
			scan.blocked =
				Some(previous_blocked.map_or(pointer.slab_id, |old| old.min(pointer.slab_id)));
			let Some(prepared) = self
				.prepare_tiered_table_drop_rewrite(&pointer, tenant_id, descriptor, table_schemas)
				.await?
			else {
				scan.blocked = previous_blocked;
				continue;
			};
			match prepared {
				PreparedTieredTableDropRewrite::Rewrite {
					slab_id,
					old_l2_uri,
					old_bytes,
					encrypted_bytes,
					payload_hash,
					tombstone_metadata,
					supersession_metadata,
					reclaimed_records,
					reclaimed_bytes,
				} => {
					let key = self.table_drop_rewritten_key(slab_id);
					let planned_uri = object_store.planned_upload_uri(&key)?;
					self.stage_table_drop_l2_cleanup(
						slab_id,
						&old_l2_uri,
						Some(old_bytes),
						Some((&planned_uri, encrypted_bytes.len() as u64)),
					)
					.await?;
					let new_uri = object_store.put(&key, &encrypted_bytes).await?;
					if new_uri != planned_uri {
						self.persist_ambiguous_upload_intent(
							slab_id,
							&new_uri,
							encrypted_bytes.len() as u64,
						)
						.await?;
						return Err(Error::new(
							ErrorKind::InvalidData,
							"object store returned a different upload URI",
						));
					}
					#[cfg(test)]
					self.table_drop_after_upload_pause_point().await;
					let fetched = object_store.get(&new_uri).await?;
					let fetched_hash = crate::hash::content::digest(&fetched);
					if fetched_hash != payload_hash {
						if let Err(cleanup_error) = self
							.discard_unsubmitted_tier_candidate(
								slab_id,
								&new_uri,
								object_store.as_ref(),
							)
							.await
						{
							tracing::warn!(error=%cleanup_error,"unsubmitted table-drop candidate awaits cleanup");
						}
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"table drop rewrite hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
								slab_id,
								&payload_hash[..4],
								&fetched_hash[..4],
							),
						));
					}
					let footer = self
						.encrypted_tiered_footer_summary(&encrypted_bytes)?
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								format!(
									"table drop rewrite for slab {} did not produce an encrypted footer",
									slab_id
								),
							)
						})?;
					let (stats_format_version, stats_block_len, stats_block_hash128) =
						stats_advisory_fields(footer.stats_locator);
					let tier = TieredSlab {
						slab_id,
						l2_uri: new_uri.clone(),
						payload_hash,
						is_encrypted: true,
						tenant_id: Some(footer.tenant_id),
						tenant_kek_id: Some(footer.tenant_kek_id),
						stats_format_version,
						stats_block_len,
						stats_block_hash128,
						tombstone_metadata,
						supersession_metadata,
					};
					self.prepare_tier_candidate_proposal(slab_id, &new_uri)
						.await?;
					if let Err(error) =
						crate::storage::sweeper::propose_tier_slab(self, committer, tier).await
					{
						if crate::storage::sweeper::tier_proposal_not_submitted(&error) {
							self.record_tier_candidate_not_submitted(
								slab_id,
								&new_uri,
								encrypted_bytes.len() as u64,
							)
							.await?;
							return Err(error);
						}

						if self
							.tier_pointer(slab_id)
							.await
							.as_ref()
							.map(|pointer| pointer.l2_uri.as_str())
							!= Some(new_uri.as_str())
						{
							self.persist_ambiguous_upload_intent(
								slab_id,
								&new_uri,
								encrypted_bytes.len() as u64,
							)
							.await?;
						}
						// Commit errors are ambiguous: the Raft apply may still have won.
						// Keep both objects and the durable marker; the next slice resolves
						// which URI is authoritative before deleting either old data.
						return Err(error);
					}
					// The old object may still serve this node if it failed to publish
					// the committed pointer: it is kept, and the refusal is typed.
					self.refuse_proposal_over_unapplied_pointer(slab_id)?;
					if old_l2_uri != new_uri {
						Self::retain_superseded_l2_object(slab_id, &old_l2_uri, "tier rewrite")?;
					}
					self.record_cleanup_replacement_published(slab_id, &old_l2_uri, &new_uri)
						.await?;
					self.clear_retired_tiered_cleanup(slab_id);
					report.rewritten += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
				PreparedTieredTableDropRewrite::Retire {
					slab_id,
					old_l2_uri,
					old_bytes,
					reclaimed_records,
					reclaimed_bytes,
				} => {
					self.stage_table_drop_l2_cleanup(slab_id, &old_l2_uri, Some(old_bytes), None)
						.await?;
					if let Err(error) = committer.retire_tiered_slab(slab_id).await {
						// As above, a transport error does not prove the retirement failed.
						// Reconciliation compares the durable pointer with the marker.
						return Err(error);
					}
					Self::retain_superseded_l2_object(slab_id, &old_l2_uri, "tier retirement")?;
					self.clear_retired_tiered_cleanup(slab_id);
					report.rewritten += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
			}
			scan.blocked = previous_blocked;
		}
		if let Some(blocked) = scan.blocked {
			report.cursor_next.after_slab_id = pointer_ids
				.iter()
				.copied()
				.filter(|id| *id < blocked && cursor.after_slab_id.is_none_or(|after| *id > after))
				.max()
				.or(cursor.after_slab_id);
		} else {
			report.cursor_next.after_slab_id = scan.after;
		}
		report.done = pointer_end && cleanup_done && scan.blocked.is_none();
		scan.watermark = report.cursor_next.after_slab_id;
		if pointer_end {
			scan.after = report.cursor_next.after_slab_id;
		}
		if report.done {
			self.cleanup_scan_schedule.drops.remove(&operation);
		}

		Ok(report)
	}

	pub async fn reclaim_tombstoned_ranges(
		&self,
		tombstones: &[MigratedRangeTombstone],
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		max_slabs: usize,
	) -> std::io::Result<TombstoneCompactionReport> {
		self.require_tier_authority_ready()?;
		if tombstones.is_empty() {
			return Ok(TombstoneCompactionReport::default());
		}
		for tombstone in tombstones {
			self.record_migrated_range_tombstone(tombstone, None)
				.await?;
		}
		if max_slabs == 0 {
			return Ok(TombstoneCompactionReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = TombstoneCompactionReport::default();
		// ADR 0098 decision 1: the budget is on attempts that completed, not on
		// rewrites. A pass that finds nothing to rewrite still decodes every slab
		// it attempts, so an unbudgeted pass decodes the whole node every tick.
		let mut attempted_slabs = 0_usize;
		// ADR 0097: the handle is deliberately unused - each path refetches the
		// current slab after it acquires ownership.
		for (slab_id, _stale_handle) in sealed {
			if attempted_slabs >= max_slabs {
				break;
			}
			// ADR 0097 decision 4: one slab is one policed attempt, as for retention.
			// A refusal skips the slab this tick and costs no budget; a slab that
			// was scanned and needed no rewrite cools down (ADR 0098 decision 2).
			let attempt = self
				.maintenance_policy
				.execute(
					MaintenanceOperation::TombstoneReclaim,
					&[slab_id],
					classify_io_error,
					TombstoneSlabScanOutcome::disposition,
					|| {
						self.compact_one_sealed_slab(
							slab_id,
							tombstones,
							SecondaryIndexCompactionContext {
								table_schemas,
								secondary_indexes,
							},
						)
					},
				)
				.await;
			let scan = match attempt {
				MaintenanceAttempt::Skipped(refusal) => {
					tracing::debug!(
						slab_id = slab_id.get(),
						?refusal,
						"tombstone reclaim of slab not admitted this tick"
					);
					continue;
				}
				MaintenanceAttempt::Completed(scan) => scan,
				MaintenanceAttempt::Failed(err) => return Err(err),
			};
			attempted_slabs += 1;
			if let Some(outcome) = scan.compaction {
				report.compacted_slabs += 1;
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn prune_versions_older_than(
		&self,
		retention_floor: HybridTimestamp,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
		self.require_tier_authority_ready()?;
		if max_slabs == 0 {
			return Ok(RetentionPruningReport::default());
		}

		// ADR 0097: enumerate IDS only. Cloning handles here would hand each pass a
		// slab image chosen before it waits for ownership; the claim path refetches
		// the current one after acquiring, so a stale handle has no reason to exist.
		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| *entry.key())
				.collect::<Vec<_>>();
			slabs.sort_unstable();
			slabs
		};

		let mut report = RetentionPruningReport::default();
		for slab_id in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.prune_one_sealed_slab(
					slab_id,
					retention_floor,
					SecondaryIndexCompactionContext {
						table_schemas,
						secondary_indexes,
					},
				)
				.await?
			{
				report.compacted_slabs += 1;
				report.pruned_versions = report
					.pruned_versions
					.saturating_add(outcome.reclaimed_records);
				report.pruned_data_rows = report
					.pruned_data_rows
					.saturating_add(outcome.retention_stats.pruned_data_rows);
				report.pruned_tombstone_rows = report
					.pruned_tombstone_rows
					.saturating_add(outcome.retention_stats.pruned_tombstone_rows);
				report.pair_drops = report
					.pair_drops
					.saturating_add(outcome.retention_stats.pair_drops);
				report.ambiguous_rows_retained = report
					.ambiguous_rows_retained
					.saturating_add(outcome.retention_stats.ambiguous_rows_retained);
				report.max_lag_seconds = report
					.max_lag_seconds
					.max(outcome.retention_stats.max_lag_seconds);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn prune_versions_for_retention(
		&self,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		now_ms: u64,
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
		self.require_tier_authority_ready()?;
		if max_slabs == 0 {
			return Ok(RetentionPruningReport::default());
		}

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = RetentionPruningReport::default();
		// ADR 0098 decision 1: the budget is on attempts that completed, not on
		// rewrites. This is the loop the local deployment pinned a core on: with a
		// rewrite budget, a node with nothing to prune decoded every sealed slab
		// every tick.
		let mut attempted_slabs = 0_usize;
		// ADR 0097: the handle is deliberately unused - each path refetches the
		// current slab after it acquires ownership.
		for (slab_id, _stale_handle) in sealed {
			if attempted_slabs >= max_slabs {
				break;
			}
			// ADR 0097 decision 4: one slab is one policed attempt. A refusal skips the
			// slab this tick and costs no budget; a failure is recorded, then
			// propagates as before. A scan that pruned nothing rewrote nothing and
			// proves nothing about a receipt; it cools the slab down (ADR 0098).
			let attempt = self
				.maintenance_policy
				.execute(
					MaintenanceOperation::RetentionPrune,
					&[slab_id],
					classify_io_error,
					RetentionSlabScanOutcome::disposition,
					|| {
						self.prune_one_sealed_slab_for_retention(
							slab_id,
							cluster_policy,
							table_lifecycle,
							table_schemas,
							secondary_indexes,
							now_ms,
						)
					},
				)
				.await;
			let scan = match attempt {
				MaintenanceAttempt::Skipped(refusal) => {
					tracing::debug!(
						slab_id = slab_id.get(),
						?refusal,
						"retention pruning of slab not admitted this tick"
					);
					continue;
				}
				MaintenanceAttempt::Completed(scan) => scan,
				MaintenanceAttempt::Failed(err) => return Err(err),
			};
			attempted_slabs += 1;
			report.pruned_data_rows = report
				.pruned_data_rows
				.saturating_add(scan.retention_stats.pruned_data_rows);
			report.pruned_tombstone_rows = report
				.pruned_tombstone_rows
				.saturating_add(scan.retention_stats.pruned_tombstone_rows);
			report.pair_drops = report
				.pair_drops
				.saturating_add(scan.retention_stats.pair_drops);
			report.ambiguous_rows_retained = report
				.ambiguous_rows_retained
				.saturating_add(scan.retention_stats.ambiguous_rows_retained);
			report.max_lag_seconds = report
				.max_lag_seconds
				.max(scan.retention_stats.max_lag_seconds);
			if let Some(outcome) = scan.compaction {
				report.compacted_slabs += 1;
				report.pruned_versions = report
					.pruned_versions
					.saturating_add(outcome.reclaimed_records);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn reclaim_table_drop_storage(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		max_slabs: usize,
	) -> std::io::Result<TableDropReclaimReport> {
		self.require_tier_authority_ready()?;
		descriptor.validate()?;
		if descriptor.table_object_id == TableObjectId::DEFAULT_KV || max_slabs == 0 {
			return Ok(TableDropReclaimReport::default());
		}
		let _ = self.seal_active_for_table_drop_reclaim().await?;

		let sealed = {
			let state = self.state.lock().await;
			let mut slabs = state
				.sealed
				.iter()
				.map(|entry| (*entry.key(), entry.value().clone()))
				.collect::<Vec<_>>();
			slabs.sort_by_key(|(slab_id, _)| *slab_id);
			slabs
		};

		let mut report = TableDropReclaimReport::default();
		// ADR 0097: the handle is deliberately unused - each path refetches the
		// current slab after it acquires ownership.
		for (slab_id, _stale_handle) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.reclaim_one_sealed_table_drop_slab(
					slab_id,
					tenant_id,
					descriptor,
					table_schemas,
					secondary_indexes,
				)
				.await?
			{
				report.compacted_slabs += 1;
				report.reclaimed_records = report
					.reclaimed_records
					.saturating_add(outcome.reclaimed_records);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub async fn reclaim_table_drop_tiered_storage(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		committer: &dyn TierCommitter,
		max_slabs: usize,
	) -> std::io::Result<TableDropReclaimReport> {
		// The coordinator can ask a follower participant to reclaim its local
		// debt. Its L2 phase remains pending until this node may propose.
		if !committer.should_propose() {
			self.set_table_drop_l2_deferred(
				tenant_id,
				descriptor.table_object_id,
				descriptor.schema_generation,
				true,
			);
			return Ok(TableDropReclaimReport {
				status: TableDropReclaimStatus::DeferredNotLeader,
				..TableDropReclaimReport::default()
			});
		}
		self.set_table_drop_l2_deferred(
			tenant_id,
			descriptor.table_object_id,
			descriptor.schema_generation,
			false,
		);

		self.require_cleanup_authority()?;
		self.require_tier_authority_ready()?;
		descriptor.validate()?;
		if descriptor.table_object_id == TableObjectId::DEFAULT_KV || max_slabs == 0 {
			return Ok(TableDropReclaimReport::default());
		}
		let pointers = self.list_tiered_slabs().await;
		if pointers.is_empty() {
			return Ok(TableDropReclaimReport::default());
		}
		let object_store = self.config.l2.clone().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				"table drop tiered reclaim requires an L2 object store",
			)
		})?;

		let mut report = TableDropReclaimReport::default();
		for pointer in pointers {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			let _slab_rewrite_claim = self
				.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
				.await;
			if self
				.refuse_proposal_over_unapplied_pointer(pointer.slab_id)
				.is_err() || self.unresolved_tier_cleanup.contains(pointer.slab_id)
			{
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					"table-drop slab deferred while cleanup remains unresolved"
				);
				continue;
			}
			let Some(prepared) = self
				.prepare_tiered_table_drop_rewrite(&pointer, tenant_id, descriptor, table_schemas)
				.await?
			else {
				continue;
			};
			match prepared {
				PreparedTieredTableDropRewrite::Rewrite {
					slab_id,
					old_l2_uri,
					old_bytes,
					encrypted_bytes,
					payload_hash,
					tombstone_metadata,
					supersession_metadata,
					reclaimed_records,
					reclaimed_bytes,
				} => {
					let key = self.table_drop_rewritten_key(slab_id);
					let planned_uri = object_store.planned_upload_uri(&key)?;
					self.stage_table_drop_l2_cleanup(
						slab_id,
						&old_l2_uri,
						Some(old_bytes),
						Some((&planned_uri, encrypted_bytes.len() as u64)),
					)
					.await?;
					let new_uri = object_store.put(&key, &encrypted_bytes).await?;
					if new_uri != planned_uri {
						self.persist_ambiguous_upload_intent(
							slab_id,
							&new_uri,
							encrypted_bytes.len() as u64,
						)
						.await?;
						return Err(Error::new(
							ErrorKind::InvalidData,
							"object store returned a different upload URI",
						));
					}
					#[cfg(test)]
					self.table_drop_after_upload_pause_point().await;
					let fetched = object_store.get(&new_uri).await?;
					let fetched_hash = crate::hash::content::digest(&fetched);
					if fetched_hash != payload_hash {
						if let Err(cleanup_error) = self
							.discard_unsubmitted_tier_candidate(
								slab_id,
								&new_uri,
								object_store.as_ref(),
							)
							.await
						{
							tracing::warn!(error=%cleanup_error,"unsubmitted table-drop candidate awaits cleanup");
						}
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"table drop rewrite hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
								slab_id,
								&payload_hash[..4],
								&fetched_hash[..4],
							),
						));
					}
					let footer = self
						.encrypted_tiered_footer_summary(&encrypted_bytes)?
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								format!(
									"table drop rewrite for slab {} did not produce an encrypted footer",
									slab_id
								),
							)
						})?;
					let (stats_format_version, stats_block_len, stats_block_hash128) =
						stats_advisory_fields(footer.stats_locator);
					let tier = TieredSlab {
						slab_id,
						l2_uri: new_uri.clone(),
						payload_hash,
						is_encrypted: true,
						tenant_id: Some(footer.tenant_id),
						tenant_kek_id: Some(footer.tenant_kek_id),
						stats_format_version,
						stats_block_len,
						stats_block_hash128,
						tombstone_metadata,
						supersession_metadata,
					};
					// A lost response is not proof of a failed proposal. The durable
					// unresolved intent keeps both objects, even across restart.
					self.prepare_tier_candidate_proposal(slab_id, &new_uri)
						.await?;
					if let Err(error) =
						crate::storage::sweeper::propose_tier_slab(self, committer, tier).await
					{
						if crate::storage::sweeper::tier_proposal_not_submitted(&error) {
							self.record_tier_candidate_not_submitted(
								slab_id,
								&new_uri,
								encrypted_bytes.len() as u64,
							)
							.await?;
							return Err(error);
						}

						if self
							.tier_pointer(slab_id)
							.await
							.as_ref()
							.map(|pointer| pointer.l2_uri.as_str())
							!= Some(new_uri.as_str())
						{
							self.persist_ambiguous_upload_intent(
								slab_id,
								&new_uri,
								encrypted_bytes.len() as u64,
							)
							.await?;
						}
						return Err(error);
					}
					// The old object may still serve this node if it failed to publish
					// the committed pointer: it is kept, and the refusal is typed.
					self.refuse_proposal_over_unapplied_pointer(slab_id)?;
					if old_l2_uri != new_uri {
						Self::retain_superseded_l2_object(slab_id, &old_l2_uri, "tier rewrite")?;
					}
					self.record_cleanup_replacement_published(slab_id, &old_l2_uri, &new_uri)
						.await?;
					self.clear_retired_tiered_cleanup(slab_id);
					report.compacted_slabs += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
				PreparedTieredTableDropRewrite::Retire {
					slab_id,
					old_l2_uri,
					old_bytes,
					reclaimed_records,
					reclaimed_bytes,
				} => {
					self.stage_table_drop_l2_cleanup(slab_id, &old_l2_uri, Some(old_bytes), None)
						.await?;
					committer.retire_tiered_slab(slab_id).await?;
					Self::retain_superseded_l2_object(slab_id, &old_l2_uri, "tier retirement")?;
					self.clear_retired_tiered_cleanup(slab_id);
					report.compacted_slabs += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
			}
		}
		Ok(report)
	}

	/// A proposer never plans from a generation the cluster has moved past, and
	/// never deletes the object a node that failed to publish still serves.
	fn refuse_proposal_over_unapplied_pointer(&self, slab_id: SlabId) -> std::io::Result<()> {
		match self.unapplied_tier_pointer(slab_id) {
			Some(_) => Err(stale_location_error(slab_id, StaleLocationReason::Fenced)),
			None => Ok(()),
		}
	}

	pub(super) fn table_drop_rewritten_key(&self, slab_id: SlabId) -> String {
		format!(
			"table_drop/slab_{:016x}_{}.wnd",
			slab_id,
			uuid::Uuid::new_v4().simple()
		)
	}

	pub(super) async fn seal_active_for_table_drop_reclaim(
		&self,
	) -> std::io::Result<Option<SlabId>> {
		let mut state = self.state.lock().await;
		if state.active.write_cursor() == 0 {
			return Ok(None);
		}
		state.active.fsync().await?;
		let sealed = state.active.clone();
		let sealed_id = sealed.slab_id();
		let sealed_size = sealed.write_cursor();
		let next_id = state.next_slab_id;
		let next_end = self.cover_slab_ids_through(&mut state, next_id).await?;
		let next = Arc::new(
			LocalSlabStore::open(
				slab_path(&self.config.dir, next_id),
				next_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		state.active = next;
		state.install_sealed(&self.tier_budget, sealed_id, sealed);
		state.advance_slab_allocator(next_end)?;
		self.record_l0_compaction_run(sealed_id, sealed_size);
		Ok(Some(sealed_id))
	}

	pub(super) async fn reclaim_one_sealed_table_drop_slab(
		&self,
		slab_id: SlabId,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> std::io::Result<Option<CompactionOutcome>> {
		// ADR 0097: own the slab BEFORE scanning it, then refetch it. Scanning
		// unowned let this job plan from bytes another maintenance job had already
		// replaced; and the handle it was called with predates the wait for
		// ownership, so after waiting it can name a superseded image.
		let Some((claims, slab)) = self.claim_sealed_slab_for_maintenance(slab_id).await else {
			return Ok(None);
		};
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (record_tenant_id, key_identity) = *entry.key();
				let routing_locality =
					self.routing_locality_for_key(record_tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id: record_tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: false,
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		self.mark_records_for_table_drop(&slab, &mut records, tenant_id, descriptor)
			.await?;
		records.sort_by_key(|record| record.old_location.offset);
		self.compact_sealed_slab_records(
			slab_id,
			slab,
			&records,
			SecondaryIndexCompactionContext {
				table_schemas,
				secondary_indexes,
			},
			&claims,
		)
		.await
	}

	pub(super) async fn prune_one_sealed_slab(
		&self,
		slab_id: SlabId,
		retention_floor: HybridTimestamp,
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Option<CompactionOutcome>> {
		// ADR 0097: own the slab BEFORE scanning it, then refetch it. Scanning
		// unowned let this job plan from bytes another maintenance job had already
		// replaced; and the handle it was called with predates the wait for
		// ownership, so after waiting it can name a superseded image.
		let Some((claims, slab)) = self.claim_sealed_slab_for_maintenance(slab_id).await else {
			return Ok(None);
		};
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: self.is_retention_prunable(
							tenant_id,
							key_identity,
							*loc,
							retention_floor,
						),
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		Self::mark_retention_prunable_row_tombstone_pairs(&mut records, retention_floor);
		records.sort_by_key(|record| record.old_location.offset);

		self.compact_sealed_slab_records(slab_id, slab, &records, secondary_index_context, &claims)
			.await
	}

	pub(super) async fn mark_records_for_table_drop(
		&self,
		slab: &Arc<LocalSlabStore>,
		records: &mut [CompactionRecord],
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		let groups = physical_compaction_group_indices(records);
		for indices in groups.values() {
			let Some(first_idx) = indices.first().copied() else {
				continue;
			};
			let frame = slab.read(records[first_idx].old_location).await?;
			self.mark_record_group_for_table_drop(&frame, indices, records, tenant_id, descriptor)?;
		}
		Ok(())
	}

	pub(super) fn mark_records_for_table_drop_from_blob(
		&self,
		plaintext: &[u8],
		records: &mut [CompactionRecord],
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		let groups = physical_compaction_group_indices(records);
		for indices in groups.values() {
			let Some(first_idx) = indices.first().copied() else {
				continue;
			};
			let frame = extract_record_from_blob(plaintext, records[first_idx].old_location)?;
			self.mark_record_group_for_table_drop(&frame, indices, records, tenant_id, descriptor)?;
		}
		Ok(())
	}

	pub(super) fn mark_record_group_for_table_drop(
		&self,
		frame: &[u8],
		indices: &[usize],
		records: &mut [CompactionRecord],
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<()> {
		if storage_multirow_index_entries_from_frame(frame)?.is_some() {
			let batch = decode_record_batch(frame)?;
			for idx in indices {
				let record = records[*idx];
				if record.tenant_id != tenant_id {
					continue;
				}
				let stored_table_object_id =
					storage_row_table_object_id_at(&batch, record.old_location.record_row_index)?;
				if stored_table_object_id == Some(descriptor.table_object_id) {
					records[*idx].reclaim = true;
				}
			}
			return Ok(());
		}

		let Ok(batch) = decode_record_batch(frame) else {
			return Ok(());
		};
		for idx in indices {
			let record = records[*idx];
			if record.tenant_id != tenant_id {
				continue;
			}
			let Ok(row) =
				materialize_record_location_batch(&batch, record.old_location.record_row_index)
			else {
				continue;
			};
			if descriptor_matches_tiered_row(
				descriptor,
				record.tenant_id,
				record.key_identity,
				record.routing_locality,
				None,
				&row,
			)? {
				records[*idx].reclaim = true;
			}
		}
		Ok(())
	}

	pub(super) async fn prune_one_sealed_slab_for_retention(
		&self,
		slab_id: SlabId,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		now_ms: u64,
	) -> std::io::Result<RetentionSlabScanOutcome> {
		// ADR 0097: own the slab BEFORE scanning it. This is the retention path
		// the live node was looping on: it scanned unowned, then waited behind
		// whatever job held the slab, then rewrote from a stale scan.
		// Refetch after claiming: the `slab` argument was chosen before this job
		// waited for ownership and may be a superseded image. A slab that is no
		// longer sealed was retired by the job this one waited for, so there is
		// nothing left to scan and the pass reports an empty scan rather than a
		// rewrite of bytes that are gone.
		let Some((claims, slab)) = self.claim_sealed_slab_for_maintenance(slab_id).await else {
			return Ok(RetentionSlabScanOutcome {
				scan: SealedSlabScan::Unclaimed,
				compaction: None,
				retention_stats: RetentionCompactionStats::default(),
			});
		};
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality,
						old_location: *loc,
						reclaim: false,
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		self.mark_records_for_table_retention(
			&slab,
			&mut records,
			cluster_policy,
			table_lifecycle,
			table_schemas,
			now_ms,
		)
		.await?;
		records.sort_by_key(|record| record.old_location.offset);
		let retention_stats = Self::retention_compaction_stats(&records);
		let scan = SealedSlabScan::after_scan(&records);
		let compaction = self
			.compact_sealed_slab_records(
				slab_id,
				slab,
				&records,
				SecondaryIndexCompactionContext {
					table_schemas,
					secondary_indexes,
				},
				&claims,
			)
			.await?;

		Ok(RetentionSlabScanOutcome {
			scan,
			compaction,
			retention_stats,
		})
	}

	pub(super) fn is_retention_prunable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		retention_floor: HybridTimestamp,
	) -> bool {
		let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
			return false;
		};
		let older_versions = versions.partition_point(|stored| stored.hlc < retention_floor);
		let prune_prefix_len = older_versions.saturating_sub(1);
		prune_prefix_len > 0
			&& versions
				.iter()
				.take(prune_prefix_len)
				.any(|stored| *stored == location)
	}

	pub(super) async fn mark_records_for_table_retention(
		&self,
		slab: &Arc<LocalSlabStore>,
		records: &mut [CompactionRecord],
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		now_ms: u64,
	) -> std::io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		let global_floor = cluster_retention_floor(cluster_policy, now_ms);
		let groups = physical_compaction_group_indices(records);
		for indices in groups.values() {
			let Some(first_idx) = indices.first().copied() else {
				continue;
			};
			let first = records[first_idx];
			let frame = slab.read(first.old_location).await?;
			self.mark_record_group_for_table_retention(
				&frame,
				indices,
				records,
				cluster_policy,
				table_lifecycle,
				table_schemas,
				now_ms,
				global_floor,
			)?;
		}
		Ok(())
	}

	pub(super) fn mark_records_for_table_retention_from_blob(
		&self,
		plaintext: &[u8],
		records: &mut [CompactionRecord],
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		now_ms: u64,
	) -> std::io::Result<()> {
		if records.is_empty() {
			return Ok(());
		}
		let global_floor = cluster_retention_floor(cluster_policy, now_ms);
		let groups = physical_compaction_group_indices(records);
		for indices in groups.values() {
			let Some(first_idx) = indices.first().copied() else {
				continue;
			};
			let first = records[first_idx];
			let frame = extract_record_from_blob(plaintext, first.old_location)?;
			self.mark_record_group_for_table_retention(
				&frame,
				indices,
				records,
				cluster_policy,
				table_lifecycle,
				table_schemas,
				now_ms,
				global_floor,
			)?;
		}
		Ok(())
	}

	pub(super) fn mark_record_group_for_table_retention(
		&self,
		frame: &[u8],
		indices: &[usize],
		records: &mut [CompactionRecord],
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		now_ms: u64,
		global_floor: Option<HybridTimestamp>,
	) -> std::io::Result<()> {
		if storage_multirow_index_entries_from_frame(frame)?.is_none() {
			if let Some(floor) = global_floor {
				for idx in indices {
					let record = records[*idx];
					records[*idx].reclaim = self.is_retention_prunable(
						record.tenant_id,
						record.key_identity,
						record.old_location,
						floor,
					);
					Self::record_retention_lag_if_retained(&mut records[*idx], floor, now_ms);
				}
			}
			return Ok(());
		}

		let batch = decode_record_batch(frame)?;
		if !has_storage_synthetic_columns(&batch) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"multi-row retention record decoded without synthetic columns",
			));
		}
		for idx in indices {
			let record = records[*idx];
			let stored_table_object_id =
				storage_row_table_object_id_at(&batch, record.old_location.record_row_index)?;
			let Some(table_object_id) = stored_table_object_id else {
				records[*idx].retention_ambiguous = true;
				records[*idx].reclaim = false;
				continue;
			};
			let Some(descriptor) = table_schemas.latest_by_id(record.tenant_id, table_object_id)
			else {
				records[*idx].retention_ambiguous = true;
				records[*idx].reclaim = false;
				continue;
			};
			match descriptor.row_identity_policy {
				RowIdentityPolicy::PrimaryKey(_) => {
					if let Some(floor) = global_floor {
						records[*idx].reclaim = self.is_retention_prunable(
							record.tenant_id,
							record.key_identity,
							record.old_location,
							floor,
						);
						Self::record_retention_lag_if_retained(&mut records[*idx], floor, now_ms);
						if !records[*idx].reclaim
							&& record.old_location.is_tombstone
							&& record.old_location.hlc < floor
						{
							metrics()
								.observe_storage_pk_delete_tombstone_boundary_versions_retained(
									record.tenant_id,
									table_object_id,
									1,
								);
						}
					}
				}
				RowIdentityPolicy::AppendOnly | RowIdentityPolicy::AppendOnlyContentHash => {
					let effective = effective_table_retention(
						&cluster_policy,
						table_lifecycle.retention_policy(record.tenant_id, table_object_id),
						now_ms,
					);
					records[*idx].reclaim = effective.floor.is_some_and(|floor| {
						self.is_append_only_retention_prunable(
							record.tenant_id,
							record.key_identity,
							record.old_location,
							floor,
						)
					});
					if let Some(floor) = effective.floor {
						Self::record_retention_lag_if_retained(&mut records[*idx], floor, now_ms);
					}
				}
			}
		}
		Ok(())
	}

	pub(super) fn record_retention_lag_if_retained(
		record: &mut CompactionRecord,
		floor: HybridTimestamp,
		now_ms: u64,
	) {
		if !record.reclaim && record.old_location.hlc < floor {
			record.retention_lag_seconds =
				now_ms.saturating_sub(record.old_location.hlc.physical_ms) / 1000;
		}
	}

	pub(super) fn is_append_only_retention_prunable(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		table_floor: HybridTimestamp,
	) -> bool {
		if location.hlc >= table_floor {
			return false;
		}
		if location.is_tombstone {
			return true;
		}
		let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
			return false;
		};
		match versions
			.iter()
			.find(|stored| stored.hlc > location.hlc && stored.is_tombstone)
		{
			Some(tombstone) if tombstone.hlc >= table_floor => false,
			_ => true,
		}
	}

	pub(super) fn mark_retention_prunable_row_tombstone_pairs(
		records: &mut [CompactionRecord],
		retention_floor: HybridTimestamp,
	) {
		if records.is_empty() {
			return;
		}
		let mut ordered = (0..records.len()).collect::<Vec<_>>();
		ordered.sort_by_key(|idx| {
			let record = records[*idx];
			(
				record.tenant_id,
				record.key_identity,
				record.old_location.hlc,
				record.old_location.offset,
				record.old_location.record_row_index,
			)
		});
		let mut prior_data_by_key: HashMap<TenantKey, usize> = HashMap::new();
		for idx in ordered {
			let record = records[idx];
			let key = (record.tenant_id, record.key_identity);
			if record.old_location.is_tombstone {
				if record.old_location.hlc < retention_floor
					&& let Some(data_idx) = prior_data_by_key.get(&key).copied()
				{
					records[data_idx].reclaim = true;
					records[idx].reclaim = true;
				}
				continue;
			}
			prior_data_by_key.insert(key, idx);
		}
	}

	pub(super) fn retention_compaction_stats(
		records: &[CompactionRecord],
	) -> RetentionCompactionStats {
		let mut stats = RetentionCompactionStats::default();
		let mut reclaimed_data_by_key: HashSet<TenantKey> = HashSet::new();
		let mut reclaimed_tombstones_by_key: HashSet<TenantKey> = HashSet::new();
		for record in records {
			if record.retention_ambiguous && !record.reclaim {
				stats.ambiguous_rows_retained = stats.ambiguous_rows_retained.saturating_add(1);
			}
			if !record.reclaim {
				stats.max_lag_seconds = stats.max_lag_seconds.max(record.retention_lag_seconds);
				continue;
			}
			if record.old_location.is_tombstone {
				stats.pruned_tombstone_rows = stats.pruned_tombstone_rows.saturating_add(1);
				reclaimed_tombstones_by_key.insert((record.tenant_id, record.key_identity));
			} else {
				stats.pruned_data_rows = stats.pruned_data_rows.saturating_add(1);
				reclaimed_data_by_key.insert((record.tenant_id, record.key_identity));
			}
		}
		stats.pair_drops = reclaimed_tombstones_by_key
			.intersection(&reclaimed_data_by_key)
			.count();
		stats
	}
}

#[cfg(test)]
mod cleanup_union_controls {
	use super::*;

	#[test]
	fn unknown_sizes_are_counted_and_legacy_duplicates_are_not_double_counted() {
		let slab = SlabId::new(12);
		let mut union = CleanupIntentUnion::default();
		union.snapshot.insert(
			(slab, "old".into()),
			RetainedObjectIntent {
				bytes: None,
				replacement_uri: None,
				replacement_bytes: None,
				published_replacements: Default::default(),
				retirement_source: None,
				state: RetainedObjectIntentState::Unresolved,
			},
		);
		union.snapshot.insert(
			(slab, "other".into()),
			RetainedObjectIntent {
				bytes: Some(7),
				replacement_uri: None,
				replacement_bytes: None,
				published_replacements: Default::default(),
				retirement_source: None,
				state: RetainedObjectIntentState::Unresolved,
			},
		);
		assert_eq!(union.totals(), (2, 7, 1));
		union.legacy.insert(slab, ("old".into(), None, 100));
		assert_eq!(union.totals(), (2, 107, 0));
	}

	#[test]
	fn legacy_old_uri_dedup_keeps_legacy_combined_bytes() {
		let slab = SlabId::new(11);
		let mut union = CleanupIntentUnion::default();
		union.legacy.insert(slab, ("old".into(), None, 100));
		union.snapshot.insert(
			(slab, "old".into()),
			RetainedObjectIntent {
				bytes: Some(200),
				replacement_uri: None,
				replacement_bytes: None,
				published_replacements: Default::default(),
				retirement_source: None,
				state: RetainedObjectIntentState::Unresolved,
			},
		);
		union.snapshot.insert(
			(slab, "other".into()),
			RetainedObjectIntent {
				bytes: Some(7),
				replacement_uri: None,
				replacement_bytes: None,
				published_replacements: Default::default(),
				retirement_source: None,
				state: RetainedObjectIntentState::Unresolved,
			},
		);
		assert_eq!(union.totals(), (2, 107, 0));
	}
}
