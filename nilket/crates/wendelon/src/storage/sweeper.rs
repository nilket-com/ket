//! Lazy tiering Background Sweeper (ADR 006 / Target #3).
//!
//! [`SlabSweeper`] is a single tokio task that periodically asks
//! the [`SlabManager`] for sealed slabs, streams each one out to
//! an [`ObjectStore`] (L2), verifies the payload hash, commits a
//! [`WendelonRequest::TierSlab`] through the Control Ring, and —
//! only after the local state machine has applied the commit —
//! returns so the L1 file can be unlinked.
//!
//! The same task also rewrites legacy plaintext L2 objects into
//! the ADR 022 encrypted object layout. Those rewrites are bounded
//! per pass
//! so an operator can pause or throttle the migration independently
//! of fresh tier uploads.
//!
//! ## Two-phase garbage collection
//!
//! The strict invariant is: **the L1 slab file must remain
//! readable until every replica's state machine has committed the
//! tier pivot.** This file enforces that by:
//!
//! 1. Reading the sealed L1 blob (L1 still authoritative).
//! 2. Uploading it to L2.
//! 3. Round-trip fetching and hashing the uploaded bytes (paranoia
//!    against a broken backend).
//! 4. Calling `raft.client_write(TierSlab(..))`. openraft only
//!    returns after the entry has been committed *and* applied
//!    locally, so by the time we see `Ok(..)` every replica has
//!    observed the pivot and our own `WendelonStateMachine` has
//!    fired its [`TierObserver`] — which is what actually unlinks
//!    the L1 file through [`SlabManager::apply_tier_slab`].
//!
//! There is deliberately **no direct unlink call** in this file.
//! The apply path is the single source of truth for "this slab
//! has left L1"; driving the unlink from here would race apply
//! and open a window where a read sees neither the L1 file nor
//! the `tiered` map entry. Routing every unlink through the SM
//! apply observer collapses that window.
//!
//! ## Rate limiting
//!
//! The sweeper's config caps:
//!  * `scan_interval`: how often the task scans for new sealed
//!    slabs. Too-small values starve the write path on a busy
//!    cluster; the default of 5s is a compromise.
//!  * `max_upload_bytes_per_sec`: a token bucket applied to the
//!    L2 upload so the sweeper never saturates the NIC. This caps
//!    PUT traffic only; rewrite-path GET bandwidth is
//!    not throttled. The bucket is refilled on a 100ms tick.
//!  * `max_plaintext_rewrites_per_pass`: how many legacy plaintext
//!    L2 slabs are re-encrypted per scan. Setting it to 0 pauses
//!    the migration without disabling fresh tier uploads.
//!  * `max_dek_rewraps_per_pass`: how many encrypted L2 slabs
//!    sealed under stale tenant KEKs are re-emitted per scan.
//!    Setting it to 0 pauses the background re-wrap path without
//!    disabling fresh tier uploads or plaintext migration.
//!
//! ## Leader-only uploads
//!
//! Every replica runs its own sweeper, but only the Raft leader
//! has a functioning `client_write`. Followers call
//! `client_write` and openraft returns `ForwardToLeader` — the
//! sweeper catches that error and quietly skips ahead to the next
//! sealed slab rather than spamming uploads that will never
//! commit. The upload cost is therefore paid exactly once per
//! slab even though the sweeper itself runs everywhere.

use crate::audit::{AuditAuthority, AuditEvent, AuditOperation, AuditSink, NoopAuditSink};
use crate::consensus::monitor::{PhysicalClock, SystemClock};
use crate::consensus::{
	MigratedRangeTombstone, RetireTieredSlab, TieredSlab, WendelonRaft, WendelonRequest,
	WendelonStateMachine,
};
use crate::governance::{AutonomousActionSurface, authorize_autonomous_action};
use crate::metrics::global as metrics;
use crate::schema::{HybridTimestamp, NodeId, SlabId, TenantId, TenantKekId};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::MvccRetentionPolicy;
use crate::storage::l2::ObjectStore;
use crate::storage::manager::{
	PreparedTieredRetentionRewrite, RetentionCompactionStats, SealedSlabInfo, SlabManager,
	StaleLocationReason, TierProposal, TieredSlabPointer, stale_location_error,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use zeroize::Zeroizing;

fn stats_advisory_fields(
	locator: Option<crate::crypto::TieredStatsBlockLocator>,
) -> (Option<u16>, Option<u32>, Option<[u8; 16]>) {
	match locator {
		Some(locator) => (
			Some(locator.stats_format_version),
			Some(locator.stats_block_len),
			Some(locator.stats_block_hash128),
		),
		None => (None, None, None),
	}
}

/// Knobs for the background sweeper. Defaults aim for "runs on a
/// laptop without destroying your write throughput" — production
/// deployments will want to set `max_upload_bytes_per_sec` from
/// the NIC capacity and the per-node upload budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SweeperConfig {
	/// How often the sweeper checks for new sealed slabs.
	pub scan_interval: Duration,
	/// Only slabs at or above this size are eligible. Set to 0 to
	/// tier every sealed slab — useful for tests; production
	/// normally wants this non-zero so small gap-fill slabs are
	/// not shipped off NVMe.
	pub min_slab_bytes: u64,
	/// Upload rate limit in bytes per second. `None` disables the
	/// limiter. Applied per sweeper instance, not per slab, and it
	/// throttles PUT traffic only.
	pub max_upload_bytes_per_sec: Option<u64>,
	/// Object-key prefix. URIs returned by the object store are
	/// `<prefix>slab_<hex>.wnd`; operators may set this to a
	/// namespace / cluster identifier so multiple Wendelon
	/// deployments can share a bucket.
	pub key_prefix: String,
	/// Maximum number of legacy plaintext L2 slabs to rewrite into
	/// ciphertext during one scan. `0` pauses the rewrite path.
	pub max_plaintext_rewrites_per_pass: usize,
	/// Maximum number of encrypted L2 slabs to re-wrap under the
	/// active tenant KEK during one scan. `0` pauses the re-wrap
	/// path.
	pub max_dek_rewraps_per_pass: usize,
}

impl Default for SweeperConfig {
	fn default() -> Self {
		Self {
			scan_interval: Duration::from_secs(5),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: String::new(),
			max_plaintext_rewrites_per_pass: 1,
			max_dek_rewraps_per_pass: 1,
		}
	}
}

const RETENTION_TIERED_REWRITE_MAX_PER_PASS: usize = 1;
const SUPERSESSION_METADATA_BACKFILL_MAX_PER_PASS: usize = 64;

/// The sweeper task and everything it needs to do its job.
///
/// Cloneable — internally every long-lived piece is an `Arc`, so
/// tests can pass the same `SlabSweeper` around without rebuilding
/// it.
#[derive(Clone)]
pub struct SlabSweeper {
	slab: Arc<SlabManager>,
	object_store: Arc<dyn ObjectStore>,
	raft: Arc<dyn TierCommitter>,
	state_machine: WendelonStateMachine,
	config: SweeperConfig,
	mvcc_retention_policy: MvccRetentionPolicy,
	clock: Arc<dyn PhysicalClock>,
	local_node: NodeId,
	audit_sink: Arc<dyn AuditSink>,
	/// Fires to nudge the sweeper to scan immediately instead of
	/// waiting for the next interval. Used by tests and by the
	/// slab rotation path to hand-off a freshly-sealed slab.
	wakeup: Arc<Notify>,
	#[cfg(test)]
	old_cleanup_hold:
		Arc<std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>>,
	#[cfg(test)]
	reconcile_snapshot_hold:
		Arc<std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>>,
}

/// The sweeper's view of the Raft handle.
///
/// Abstracted behind a trait so tests can drive the tier path
/// without standing up an openraft cluster. The production impl
/// is a thin wrapper over [`WendelonRaft`]; see [`RaftCommitter`].
#[async_trait]
pub trait TierCommitter: Send + Sync {
	/// Advisory local-leadership gate for autonomous work. A true result is
	/// permission to attempt a proposal, never proof of apply or cleanup.
	/// Committers without a role view retain their existing attempt behavior.
	fn should_propose(&self) -> bool {
		true
	}

	/// Submit a TierSlab entry. Returns `Ok(())` only after the
	/// entry has been committed *and* applied locally, so the
	/// caller can treat a successful return as "L1 file has been
	/// unlinked via the SM apply observer". An error means the
	/// sweeper should skip this slab and try again on the next
	/// scan — the L1 file stays put.
	///
	/// The proposal carries the history its publication reserved (ADR 0692
	/// §7); a committer resolves it through [`TierProposal::begin_submission`].
	async fn commit_tier_slab(&self, proposal: TierProposal) -> std::io::Result<()>;

	/// Submit a RetireTieredSlab entry. Returns `Ok(())` only after
	/// the local state machine has applied the retirement.
	async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()>;

	/// Submit an ADR 055 LSM CompactionCommit entry. Returns
	/// `Ok(())` only after the local state machine has applied the
	/// commit and fired the local storage observer.
	async fn commit_lsm_compaction(
		&self,
		commit: crate::storage::CompactionCommit,
	) -> std::io::Result<()> {
		let _ = commit;
		Err(std::io::Error::new(
			std::io::ErrorKind::Unsupported,
			"LSM compaction commits are not supported by this committer",
		))
	}
}

/// The sole submitted LSM command was applied by the state machine and
/// explicitly rejected. Transport failures are not this definite outcome.
#[derive(Debug)]
pub(crate) struct LsmCommitRejected(pub(crate) String);
impl std::fmt::Display for LsmCommitRejected {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "LSM commit rejected: {}", self.0)
	}
}
impl std::error::Error for LsmCommitRejected {}

/// This command was refused before client_write was invoked. A later Raft
/// error, including ForwardToLeader, never carries this proof.
#[derive(Debug)]
struct TierCommitNotSubmitted(String);
impl std::fmt::Display for TierCommitNotSubmitted {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.0)
	}
}
impl std::error::Error for TierCommitNotSubmitted {}
pub(crate) fn tier_proposal_not_submitted(error: &std::io::Error) -> bool {
	error
		.get_ref()
		.and_then(|inner| inner.downcast_ref::<TierCommitNotSubmitted>())
		.is_some()
}

/// Reserve `tier`'s history and propose it. A refused reservation proposes
/// nothing, so it is reported as a definite not-submitted outcome and the
/// caller's existing cleanup of an unsubmitted upload applies unchanged.
pub(crate) async fn propose_tier_slab(
	slab: &SlabManager,
	committer: &dyn TierCommitter,
	tier: TieredSlab,
) -> std::io::Result<()> {
	match slab.reserve_tier_history(tier).await {
		Ok(proposal) => committer.commit_tier_slab(proposal).await,
		Err(refusal) => {
			crate::metrics::global().observe_tier_rewrite_refusal(
				crate::metrics::TierRewriteRefusalReason::NodeHistoryBudget,
			);
			Err(std::io::Error::new(
				refusal.kind(),
				TierCommitNotSubmitted(format!("tier history not reserved: {refusal}")),
			))
		}
	}
}

/// Production [`TierCommitter`] backed by an openraft [`WendelonRaft`].
///
/// `client_write` is resolved on the leader and blocks until the
/// local state machine has applied the entry, which is exactly the
/// synchronisation the sweeper needs. Local role refusal before submission
/// carries a definite not-submitted proof. Any error returned by client_write,
/// including ForwardToLeader, remains ambiguous and retains uploaded bytes.
pub struct RaftCommitter {
	raft: WendelonRaft,
	#[cfg(test)]
	submit_without_role_check: bool,
}

impl RaftCommitter {
	pub fn new(raft: WendelonRaft) -> Self {
		Self {
			raft,
			#[cfg(test)]
			submit_without_role_check: false,
		}
	}
	fn require_submission_role(&self) -> std::io::Result<()> {
		let allowed = self.should_propose();
		#[cfg(test)]
		let allowed = allowed || self.submit_without_role_check;
		if allowed {
			Ok(())
		} else {
			Err(std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				TierCommitNotSubmitted(
					"local node is not leader; client_write was not invoked".into(),
				),
			))
		}
	}
}

#[async_trait]
impl TierCommitter for RaftCommitter {
	fn should_propose(&self) -> bool {
		self.raft.metrics().borrow().state == openraft::ServerState::Leader
	}

	async fn commit_tier_slab(&self, proposal: TierProposal) -> std::io::Result<()> {
		// Dropping the proposal on this refusal releases its reservation.
		self.require_submission_role()?;
		let tier = proposal.tier().clone();
		let submission = proposal.begin_submission().map_err(|refusal| {
			std::io::Error::new(
				refusal.kind(),
				TierCommitNotSubmitted(format!("tier history no longer reserved: {refusal}")),
			)
		})?;
		match self
			.raft
			.client_write(WendelonRequest::TierSlab(tier))
			.await
		{
			Ok(_) => {
				submission.applied();
				Ok(())
			}
			Err(openraft::error::RaftError::APIError(
				openraft::error::ClientWriteError::ForwardToLeader(error),
			)) => Err(std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				format!("tier proposal outcome ambiguous after client_write: {error}"),
			)),
			Err(e) => Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				format!("TierSlab commit failed: {e}"),
			)),
		}
	}

	async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()> {
		self.require_submission_role()?;
		match self
			.raft
			.client_write(WendelonRequest::RetireTieredSlab(RetireTieredSlab {
				slab_id,
			}))
			.await
		{
			Ok(_) => Ok(()),
			Err(openraft::error::RaftError::APIError(
				openraft::error::ClientWriteError::ForwardToLeader(error),
			)) => Err(std::io::Error::new(
				std::io::ErrorKind::WouldBlock,
				format!("tier proposal outcome ambiguous after client_write: {error}"),
			)),
			Err(e) => Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				format!("RetireTieredSlab commit failed: {e}"),
			)),
		}
	}

	async fn commit_lsm_compaction(
		&self,
		commit: crate::storage::CompactionCommit,
	) -> std::io::Result<()> {
		match self
			.raft
			.client_write(WendelonRequest::CompactionCommit(commit))
			.await
		{
			Ok(response) => match response.data {
				crate::consensus::WendelonResponse::Success => Ok(()),
				crate::consensus::WendelonResponse::Error(message) => Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					LsmCommitRejected(message),
				)),
				other => Err(std::io::Error::other(format!(
					"unexpected LSM response: {other:?}"
				))),
			},
			Err(e) => Err(std::io::Error::other(format!(
				"CompactionCommit commit failed: {e}"
			))),
		}
	}
}

impl SlabSweeper {
	pub fn new(
		slab: Arc<SlabManager>,
		object_store: Arc<dyn ObjectStore>,
		raft: Arc<dyn TierCommitter>,
		state_machine: WendelonStateMachine,
		config: SweeperConfig,
	) -> Self {
		Self {
			slab,
			object_store,
			raft,
			state_machine,
			config,
			mvcc_retention_policy: MvccRetentionPolicy::Disabled,
			clock: Arc::new(SystemClock),
			local_node: NodeId {
				id: 0,
				_reserved: 0,
			},
			audit_sink: Arc::new(NoopAuditSink),
			wakeup: Arc::new(Notify::new()),
			#[cfg(test)]
			old_cleanup_hold: Arc::new(std::sync::Mutex::new(None)),
			#[cfg(test)]
			reconcile_snapshot_hold: Arc::new(std::sync::Mutex::new(None)),
		}
	}

	pub fn with_mvcc_retention_policy(mut self, retention_policy: MvccRetentionPolicy) -> Self {
		self.mvcc_retention_policy = retention_policy;
		self
	}

	pub fn with_mvcc_retention_window(mut self, retention_window: Option<Duration>) -> Self {
		self.mvcc_retention_policy = MvccRetentionPolicy::from_optional_window(retention_window);
		self
	}

	pub fn with_clock(mut self, clock: Arc<dyn PhysicalClock>) -> Self {
		self.clock = clock;
		self
	}

	pub fn with_audit_sink(mut self, local_node: NodeId, audit_sink: Arc<dyn AuditSink>) -> Self {
		self.local_node = local_node;
		self.audit_sink = audit_sink;
		self
	}

	/// Prod the sweeper to scan right now, skipping the remaining
	/// wait. Safe to call from any task; multiple concurrent
	/// wakeups coalesce.
	pub fn wake(&self) {
		self.wakeup.notify_one();
	}

	/// Spawn the background loop. The returned [`JoinHandle`]
	/// completes only when `wakeup.notify_waiters()` is called on
	/// an explicitly-cancelled sweeper or the task panics.
	pub fn spawn(self) -> JoinHandle<()> {
		tokio::spawn(self.run())
	}

	/// Execute a single sweep pass and return. Separated from
	/// [`Self::run`] so tests can step through the sweeper
	/// without racing a tokio interval.
	pub async fn sweep_once(&self) -> std::io::Result<SweepReport> {
		self.slab.require_cleanup_authority()?;
		metrics().storage_tiering_passes_total.inc();
		let mut report = SweepReport::default();
		let reconcile = self.reconcile_tiered_runtime_state_once().await?;
		report.failed += reconcile.failed;
		if let Err(error) = self
			.slab
			.reconcile_table_drop_l2_cleanup_markers(self.object_store.as_ref(), 64)
			.await
		{
			tracing::warn!(error = %error, "retained cleanup intent is awaiting publication");
		}
		// Followers still mirror committed state above, but do not initiate
		// new tier/retention commands or pay for uploads they cannot publish.
		if !self.raft.should_propose() {
			metrics()
				.storage_tiering_failures_total
				.add(report.failed as u64);
			return Ok(report);
		}
		let supersession_backfill = self
			.backfill_supersession_metadata_once(SUPERSESSION_METADATA_BACKFILL_MAX_PER_PASS)
			.await?;
		report.supersession_metadata_backfilled_tiered = supersession_backfill.backfilled;
		report.supersession_metadata_backfill_failures = supersession_backfill.failed;
		report.failed += supersession_backfill.failed;
		let candidates = self.slab.list_sealed_slabs().await;
		for candidate in candidates {
			if candidate.size_bytes < self.config.min_slab_bytes {
				report.skipped_too_small += 1;
				continue;
			}
			if self.slab.is_tiered(candidate.slab_id).await {
				// Raced with another replica's commit — local SM
				// already applied the pivot, nothing to do.
				report.skipped_already_tiered += 1;
				continue;
			}
			match self.tier_one_slab(&candidate).await {
				Ok(()) => report.tiered += 1,
				Err(e) => {
					tracing::warn!(
						slab_id = candidate.slab_id.get(),
						error = %e,
						"sweeper pass failed on slab; will retry on next scan"
					);
					report.failed += 1;
				}
			}
		}
		let rewritten = self
			.rewrite_plaintext_tiered_slabs_once(self.config.max_plaintext_rewrites_per_pass)
			.await?;
		report.rewritten_plaintext_tiered = rewritten.rewritten;
		report.skipped_already_encrypted_tiered = rewritten.skipped_already_encrypted;
		report.plaintext_rewrite_failures = rewritten.failed;
		report.failed += rewritten.failed;
		let rewrapped = self
			.rewrap_tiered_slabs_once(self.config.max_dek_rewraps_per_pass)
			.await?;
		report.rewrapped_tiered_deks = rewrapped.rewrapped;
		report.skipped_current_tenant_kek_tiered = rewrapped.skipped_already_current;
		report.backfilled_tenant_kek_metadata_tiered = rewrapped.backfilled_metadata;
		report.dek_rewrap_failures = rewrapped.failed;
		report.failed += rewrapped.failed;
		let retention_rewrites = self
			.rewrite_tiered_slabs_for_retention_once(RETENTION_TIERED_REWRITE_MAX_PER_PASS)
			.await?;
		report.retention_rewritten_tiered = retention_rewrites.rewritten;
		report.retention_pruned_versions = retention_rewrites.pruned_versions;
		report.retention_reclaimed_bytes = retention_rewrites.reclaimed_bytes;
		report.retention_rewrite_failures = retention_rewrites.failed;
		report.failed += retention_rewrites.failed;
		if retention_rewrites.pruned_versions > 0
			|| retention_rewrites.max_lag_seconds > 0
			|| retention_rewrites.ambiguous_rows_retained > 0
		{
			metrics().observe_storage_table_retention_l2_prune(
				retention_rewrites.pruned_data_rows as u64,
				retention_rewrites.pruned_tombstone_rows as u64,
				retention_rewrites.reclaimed_bytes,
				retention_rewrites.pair_drops as u64,
				retention_rewrites.ambiguous_rows_retained as u64,
				retention_rewrites.max_lag_seconds,
			);
		}
		let reclaim = self.reclaim_tombstoned_ranges_once(1).await?;
		report.reclaimed_slabs = reclaim.compacted_slabs;
		report.reclaimed_bytes = reclaim.reclaimed_bytes;
		let rehydrated = self.reclaim_rehydrated_slabs_once().await?;
		report.rehydrated_reclaimed = rehydrated.reclaimed;
		report.skipped_rehydration_guard = rehydrated.skipped_guard;
		metrics()
			.storage_tiered_slabs_total
			.add(report.tiered as u64);
		metrics()
			.storage_tiering_failures_total
			.add(report.failed as u64);
		metrics()
			.storage_tiering_skipped_small_total
			.add(report.skipped_too_small as u64);
		metrics()
			.storage_tiering_skipped_already_tiered_total
			.add(report.skipped_already_tiered as u64);
		metrics()
			.storage_rehydration_guard_skips_total
			.add(report.skipped_rehydration_guard as u64);
		metrics()
			.storage_rehydrated_reclaims_total
			.add(report.rehydrated_reclaimed as u64);
		metrics()
			.storage_migration_reclaimed_slabs_total
			.add(report.reclaimed_slabs as u64);
		metrics()
			.storage_migration_reclaimed_bytes_total
			.add(report.reclaimed_bytes);
		Ok(report)
	}

	/// Drive a single slab through the full two-phase pipeline.
	async fn tier_one_slab(&self, info: &SealedSlabInfo) -> std::io::Result<()> {
		let _permit = authorize_autonomous_action(AutonomousActionSurface::L2TieringUpload);
		self.refuse_proposal_over_unpublished_commit(info.slab_id, None)
			.await?;
		// Phase 1: snapshot L1 bytes. `read_sealed_raw` fails
		// loudly if the slab has been rotated away or already
		// tiered — either is a sign the caller's info went stale,
		// and re-listing on the next pass will converge.
		let plaintext = Zeroizing::new(self.slab.read_sealed_raw(info.slab_id).await?);
		let encrypted = self
			.encrypt_tiered_object_blob_with_current_stats(info.slab_id, plaintext.as_ref())
			.await?;
		let expected_hash = crate::hash::content::digest(&encrypted.bytes);

		// Phase 2: upload to L2, applying the rate limit.
		self.rate_limit_upload(encrypted.bytes.len()).await;
		// Every upload attempt writes a fresh object: a published generation is
		// immutable, and a retried attempt can never overwrite an object a
		// committed pointer names.
		let key = format!(
			"{}slab_{:016x}_{}.wnd",
			self.config.key_prefix,
			info.slab_id,
			uuid::Uuid::new_v4().simple()
		);
		let upload_started = tokio::time::Instant::now();
		let l2_uri = self.object_store.put(&key, &encrypted.bytes).await?;
		metrics()
			.l2_upload_latency_seconds
			.observe_duration(upload_started.elapsed());

		// Phase 3: round-trip verification. A mismatch here means
		// the object store acked a write but returned a different
		// blob — we refuse to pivot on corrupt data. The L1 file
		// is still intact; the sweeper will retry next pass.
		let fetched = self.object_store.get(&l2_uri).await?;
		let fetched_hash = crate::hash::content::digest(&fetched);
		if fetched_hash != expected_hash {
			// Best-effort cleanup of the bad object. Ignoring the
			// error here is intentional: we already know the
			// backend is misbehaving.
			let _ = self.object_store.delete(&l2_uri).await;
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"L2 round-trip hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
					info.slab_id,
					&expected_hash[..4],
					&fetched_hash[..4],
				),
			));
		}

		// Phase 4: commit through Raft. Only returns Ok after the
		// local SM has applied the entry and fired the tier
		// observer — which is what actually unlinks the L1 file
		// via `SlabManager::apply_tier_slab`. Readers that
		// captured an `Arc<LocalSlabStore>` *before* the apply
		// keep their open fd (Linux unlink semantics).
		let (stats_format_version, stats_block_len, stats_block_hash128) =
			stats_advisory_fields(encrypted.stats_locator);
		let tier = TieredSlab {
			slab_id: info.slab_id,
			l2_uri: l2_uri.clone(),
			payload_hash: expected_hash,
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version,
			stats_block_len,
			stats_block_hash128,
			tombstone_metadata: self.slab.slab_row_tombstone_metadata(info.slab_id)?,
			supersession_metadata: self.slab.slab_supersession_metadata(info.slab_id)?,
		};
		if let Err(error) = self
			.slab
			.confirm_tier_candidate_quiescent(info.slab_id)
			.await
		{
			if let Err(cleanup_error) = self.delete_object_if_present(&l2_uri).await {
				tracing::warn!(error = %cleanup_error, "failed to remove unsubmitted tier upload");
			}
			return Err(error);
		}
		if let Err(error) = propose_tier_slab(&self.slab, self.raft.as_ref(), tier).await {
			let not_applied = error
				.get_ref()
				.and_then(|inner| inner.downcast_ref::<TierCommitNotSubmitted>())
				.is_some();
			let observed_live = self
				.sm_tier_pointer(info.slab_id)
				.await
				.is_some_and(|pointer| pointer.l2_uri == l2_uri);
			if not_applied && !observed_live {
				// This first upload has no prior cleanup marker. The URI is fresh
				// and client_write was never invoked, so it cannot become live later.
				if let Err(cleanup_error) = self.delete_object_if_present(&l2_uri).await {
					// Do not lose accounting when the backend refuses deletion.
					// Existing staged/retry cleanup remains conservative.
					self.cleanup_uploaded_object_after_failed_commit(
						info.slab_id,
						&l2_uri,
						encrypted.bytes.len() as u64,
						&error,
					)
					.await?;
					return Err(cleanup_error);
				}
			} else {
				self.cleanup_uploaded_object_after_failed_commit(
					info.slab_id,
					&l2_uri,
					encrypted.bytes.len() as u64,
					&error,
				)
				.await?;
			}
			return Err(error);
		}

		// Defensive: if the SM has an observer wired in, apply is
		// already done. If it does not (e.g., a test wiring), the
		// sweeper still owns the invariant — call through the
		// manager's apply_tier_slab directly. This is idempotent
		// on the real path because the observer already moved the
		// slab out of `sealed`.
		let observed_epoch = self.slab.capture_snapshot_epoch();
		if let Some(pointer) = self.sm_tier_pointer(info.slab_id).await {
			self.slab
				.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
				.await?;
		}

		Ok(())
	}

	async fn encrypt_tiered_object_blob_with_current_stats(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
	) -> std::io::Result<crate::crypto::EncryptedTieredSlab> {
		let snapshot = self.state_machine.state_snapshot().await;
		self.slab.encrypt_tiered_object_blob_with_table_stats(
			slab_id,
			plaintext_blob,
			&snapshot.table_schemas,
		)
	}

	async fn rewrite_plaintext_tiered_slabs_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<PlaintextRewriteReport> {
		let mut report = PlaintextRewriteReport::default();
		if max_slabs == 0 {
			return Ok(report);
		}
		for pointer in self.slab.list_tiered_slabs().await {
			if report.rewritten >= max_slabs {
				break;
			}
			// Steady-state short-circuit: a previous successful
			// rewrite (or fresh-encrypt tiering) stamped
			// `is_encrypted = true` on the SM pointer. Skip without
			// the GET so the rewrite sweeper costs O(0) once the
			// fleet is fully ciphertext.
			if pointer.is_encrypted {
				report.skipped_already_encrypted += 1;
				continue;
			}
			match self.rewrite_one_plaintext_tiered_slab(&pointer).await {
				Ok(PlaintextRewriteOutcome::AlreadyEncrypted) => {
					report.skipped_already_encrypted += 1;
				}
				Ok(PlaintextRewriteOutcome::Rewritten) => {
					report.rewritten += 1;
				}
				Err(error) => {
					tracing::warn!(
						slab_id = pointer.slab_id.get(),
						l2_uri = %pointer.l2_uri,
						error = %error,
						"plaintext L2 rewrite failed; will retry on next scan"
					);
					report.failed += 1;
				}
			}
		}
		Ok(report)
	}

	async fn rewrite_one_plaintext_tiered_slab(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<PlaintextRewriteOutcome> {
		let _slab_rewrite_claim = self
			.slab
			.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
			.await;
		self.refuse_proposal_over_unpublished_commit(pointer.slab_id, Some(pointer))
			.await?;
		let blob = self.object_store.get_bytes(&pointer.l2_uri).await?;
		let actual_hash = crate::hash::content::digest(blob.as_ref());
		if actual_hash != pointer.payload_hash {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"L2 payload hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
					pointer.slab_id,
					&pointer.payload_hash[..4],
					&actual_hash[..4],
				),
			));
		}
		if self
			.slab
			.encrypted_tiered_footer_summary(blob.as_ref())?
			.is_some()
		{
			return Ok(PlaintextRewriteOutcome::AlreadyEncrypted);
		}

		let encrypted = self
			.encrypt_tiered_object_blob_with_current_stats(pointer.slab_id, blob.as_ref())
			.await?;
		let expected_hash = crate::hash::content::digest(&encrypted.bytes);
		// Per-attempt suffix so a leader-handoff retry never races
		// the same key as an earlier in-flight upload. The earlier
		// attempt either committed (we never reach this branch on a
		// committed slab — `is_encrypted` short-circuits in
		// `rewrite_one_plaintext_tiered_slab`) or failed and was
		// best-effort deleted; either way, a fresh key keeps the two
		// attempts independent.
		let key = self.rewritten_key(pointer.slab_id);
		let planned_uri = self.object_store.planned_upload_uri(&key)?;
		self.slab
			.stage_table_drop_l2_cleanup(
				pointer.slab_id,
				&pointer.l2_uri,
				Some(blob.len() as u64),
				Some((&planned_uri, encrypted.bytes.len() as u64)),
			)
			.await?;
		self.rate_limit_upload(encrypted.bytes.len()).await;
		let new_uri = self.object_store.put(&key, &encrypted.bytes).await?;
		if new_uri != planned_uri {
			self.slab
				.persist_ambiguous_upload_intent(
					pointer.slab_id,
					&new_uri,
					encrypted.bytes.len() as u64,
				)
				.await?;
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"object store returned a different upload URI",
			));
		}
		let fetched = self.object_store.get(&new_uri).await?;
		let fetched_hash = crate::hash::content::digest(&fetched);
		if fetched_hash != expected_hash {
			if let Err(cleanup_error) = self
				.slab
				.discard_unsubmitted_tier_candidate(
					pointer.slab_id,
					&new_uri,
					self.object_store.as_ref(),
				)
				.await
			{
				tracing::warn!(error=%cleanup_error,"unsubmitted rewrite candidate awaits cleanup");
			}
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"rewritten ciphertext hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
					pointer.slab_id,
					&expected_hash[..4],
					&fetched_hash[..4],
				),
			));
		}

		let (stats_format_version, stats_block_len, stats_block_hash128) =
			stats_advisory_fields(encrypted.stats_locator);
		let tier = TieredSlab {
			slab_id: pointer.slab_id,
			l2_uri: new_uri.clone(),
			payload_hash: expected_hash,
			is_encrypted: true,
			tenant_id: Some(encrypted.tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version,
			stats_block_len,
			stats_block_hash128,
			tombstone_metadata: pointer.tombstone_metadata,
			supersession_metadata: pointer.supersession_metadata,
		};
		self.slab
			.prepare_tier_candidate_proposal(pointer.slab_id, &new_uri)
			.await?;
		if let Err(error) = propose_tier_slab(&self.slab, self.raft.as_ref(), tier).await {
			self.cleanup_uploaded_object_after_failed_commit(
				pointer.slab_id,
				&new_uri,
				encrypted.bytes.len() as u64,
				&error,
			)
			.await?;
			return Err(error);
		}

		let observed_epoch = self.slab.capture_snapshot_epoch();
		if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
			self.slab
				.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
				.await?;
		}

		#[cfg(test)]
		{
			let hold = self.old_cleanup_hold.lock().unwrap().take();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				if let Ok(permit) = release.acquire().await {
					permit.forget();
				}
			}
		}
		self.slab
			.record_cleanup_replacement_published(pointer.slab_id, &pointer.l2_uri, &new_uri)
			.await?;
		if pointer.l2_uri != new_uri {
			if let Err(error) = SlabManager::retain_superseded_l2_object(
				pointer.slab_id,
				&pointer.l2_uri,
				"superseded tier object",
			) {
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					l2_uri = %pointer.l2_uri,
					error = %error,
					"ciphertext rewrite committed but legacy plaintext object cleanup failed"
				);
			} else {
				self.slab.clear_retired_tiered_cleanup(pointer.slab_id);
			}
		} else {
			self.slab.clear_retired_tiered_cleanup(pointer.slab_id);
		}
		tracing::info!(
			slab_id = pointer.slab_id.get(),
			old_l2_uri = %pointer.l2_uri,
			new_l2_uri = %new_uri,
			"rewrote plaintext L2 slab into ADR 022 ciphertext"
		);
		Ok(PlaintextRewriteOutcome::Rewritten)
	}

	async fn rewrap_tiered_slabs_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<TenantKekRewrapReport> {
		let mut report = TenantKekRewrapReport::default();
		if max_slabs == 0 {
			return Ok(report);
		}
		for pointer in self.slab.list_tiered_slabs().await {
			if report.rewrapped + report.backfilled_metadata >= max_slabs {
				break;
			}
			match self.rewrap_one_tiered_slab(&pointer).await {
				Ok(TenantKekRewrapOutcome::AlreadyCurrent) => {
					report.skipped_already_current += 1;
				}
				Ok(TenantKekRewrapOutcome::MetadataBackfilled) => {
					report.backfilled_metadata += 1;
				}
				Ok(TenantKekRewrapOutcome::Rewrapped) => {
					report.rewrapped += 1;
				}
				Err(error) => {
					tracing::warn!(
						slab_id = pointer.slab_id.get(),
						l2_uri = %pointer.l2_uri,
						error = %error,
						"tenant KEK re-wrap failed; will retry on next scan"
					);
					report.failed += 1;
				}
			}
		}
		Ok(report)
	}

	async fn rewrap_one_tiered_slab(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<TenantKekRewrapOutcome> {
		// Candidates were selected before ownership. Serialize with retirement
		// and row rewrites, then validate both local and committed publication.
		let _slab_rewrite_claim = self
			.slab
			.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
			.await;
		if self.slab.tier_pointer(pointer.slab_id).await.as_ref() != Some(pointer)
			|| self.sm_tier_pointer(pointer.slab_id).await.as_ref() != Some(pointer)
		{
			return Ok(TenantKekRewrapOutcome::AlreadyCurrent);
		}

		self.refuse_proposal_over_unpublished_commit(pointer.slab_id, Some(pointer))
			.await?;
		if let (Some(tenant_id), Some(pointer_tenant_kek_id)) =
			(pointer.tenant_id, pointer.tenant_kek_id)
		{
			let active_tenant_kek_id = self.slab.active_tenant_kek_id(tenant_id)?;
			if pointer_tenant_kek_id == active_tenant_kek_id {
				return Ok(TenantKekRewrapOutcome::AlreadyCurrent);
			}
		}
		let blob = self.object_store.get_bytes(&pointer.l2_uri).await?;
		let Some(footer) = self.slab.encrypted_tiered_footer_summary(blob.as_ref())? else {
			return Ok(TenantKekRewrapOutcome::AlreadyCurrent);
		};
		let active_tenant_kek_id = self.slab.active_tenant_kek_id(footer.tenant_id)?;
		if footer.tenant_kek_id == active_tenant_kek_id
			&& pointer.tenant_id == Some(footer.tenant_id)
			&& pointer.tenant_kek_id == Some(footer.tenant_kek_id)
		{
			return Ok(TenantKekRewrapOutcome::AlreadyCurrent);
		}
		if footer.tenant_kek_id == active_tenant_kek_id {
			let (stats_format_version, stats_block_len, stats_block_hash128) =
				stats_advisory_fields(footer.stats_locator);
			let tier = TieredSlab {
				slab_id: pointer.slab_id,
				l2_uri: pointer.l2_uri.clone(),
				payload_hash: pointer.payload_hash,
				is_encrypted: true,
				tenant_id: Some(footer.tenant_id),
				tenant_kek_id: Some(footer.tenant_kek_id),
				stats_format_version,
				stats_block_len,
				stats_block_hash128,
				tombstone_metadata: pointer.tombstone_metadata,
				supersession_metadata: pointer.supersession_metadata,
			};
			if let Err(error) = propose_tier_slab(&self.slab, self.raft.as_ref(), tier).await {
				return Err(error);
			}
			let observed_epoch = self.slab.capture_snapshot_epoch();
			if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
				self.slab
					.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
					.await?;
			}
			return Ok(TenantKekRewrapOutcome::MetadataBackfilled);
		}
		let plaintext = Zeroizing::new(
			self.slab
				.decode_tiered_object_blob(pointer, blob.as_ref())?,
		);
		let encrypted = self
			.encrypt_tiered_object_blob_with_current_stats(pointer.slab_id, plaintext.as_ref())
			.await?;
		if encrypted.tenant_kek_id != active_tenant_kek_id {
			return Err(std::io::Error::other(format!(
				"tenant KEK re-wrap for slab {} expected active tenant_kek_id {}, got {}",
				pointer.slab_id, active_tenant_kek_id, encrypted.tenant_kek_id
			)));
		}
		let expected_hash = crate::hash::content::digest(&encrypted.bytes);
		let key = self.rewritten_key(pointer.slab_id);
		let planned_uri = self.object_store.planned_upload_uri(&key)?;
		self.slab
			.stage_table_drop_l2_cleanup(
				pointer.slab_id,
				&pointer.l2_uri,
				Some(blob.len() as u64),
				Some((&planned_uri, encrypted.bytes.len() as u64)),
			)
			.await?;
		self.rate_limit_upload(encrypted.bytes.len()).await;
		let new_uri = self.object_store.put(&key, &encrypted.bytes).await?;
		if new_uri != planned_uri {
			self.slab
				.persist_ambiguous_upload_intent(
					pointer.slab_id,
					&new_uri,
					encrypted.bytes.len() as u64,
				)
				.await?;
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"object store returned a different upload URI",
			));
		}
		let fetched = self.object_store.get(&new_uri).await?;
		let fetched_hash = crate::hash::content::digest(&fetched);
		if fetched_hash != expected_hash {
			if let Err(cleanup_error) = self
				.slab
				.discard_unsubmitted_tier_candidate(
					pointer.slab_id,
					&new_uri,
					self.object_store.as_ref(),
				)
				.await
			{
				tracing::warn!(error=%cleanup_error,"unsubmitted rewrite candidate awaits cleanup");
			}
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"tenant KEK re-wrap hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
					pointer.slab_id,
					&expected_hash[..4],
					&fetched_hash[..4],
				),
			));
		}

		let (stats_format_version, stats_block_len, stats_block_hash128) =
			stats_advisory_fields(encrypted.stats_locator);
		let tier = TieredSlab {
			slab_id: pointer.slab_id,
			l2_uri: new_uri.clone(),
			payload_hash: expected_hash,
			is_encrypted: true,
			tenant_id: Some(footer.tenant_id),
			tenant_kek_id: Some(active_tenant_kek_id),
			stats_format_version,
			stats_block_len,
			stats_block_hash128,
			tombstone_metadata: pointer.tombstone_metadata,
			supersession_metadata: pointer.supersession_metadata,
		};
		self.slab
			.prepare_tier_candidate_proposal(pointer.slab_id, &new_uri)
			.await?;
		if let Err(error) = propose_tier_slab(&self.slab, self.raft.as_ref(), tier).await {
			self.cleanup_uploaded_object_after_failed_commit(
				pointer.slab_id,
				&new_uri,
				encrypted.bytes.len() as u64,
				&error,
			)
			.await?;
			return Err(error);
		}

		let observed_epoch = self.slab.capture_snapshot_epoch();
		if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
			self.slab
				.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
				.await?;
		}

		self.slab
			.record_cleanup_replacement_published(pointer.slab_id, &pointer.l2_uri, &new_uri)
			.await?;
		if pointer.l2_uri != new_uri {
			if let Err(error) = SlabManager::retain_superseded_l2_object(
				pointer.slab_id,
				&pointer.l2_uri,
				"superseded tier object",
			) {
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					l2_uri = %pointer.l2_uri,
					error = %error,
					"tenant KEK re-wrap committed but old object cleanup failed"
				);
			} else {
				self.slab.clear_retired_tiered_cleanup(pointer.slab_id);
			}
		} else {
			self.slab.clear_retired_tiered_cleanup(pointer.slab_id);
		}
		tracing::info!(
			slab_id = pointer.slab_id.get(),
			tenant_id = footer.tenant_id.get(),
			old_tenant_kek_id = footer.tenant_kek_id.get(),
			new_tenant_kek_id = active_tenant_kek_id.get(),
			old_l2_uri = %pointer.l2_uri,
			new_l2_uri = %new_uri,
			"re-wrapped tiered slab under active tenant KEK"
		);
		self.emit_tenant_kek_rewrap_audit(
			pointer.slab_id,
			footer.tenant_id,
			footer.tenant_kek_id,
			active_tenant_kek_id,
			&pointer.l2_uri,
			&new_uri,
		);
		Ok(TenantKekRewrapOutcome::Rewrapped)
	}

	fn mvcc_retention_floor_hlc_at(&self, now_ms: u64) -> Option<HybridTimestamp> {
		let window = self.mvcc_retention_policy.window()?;
		let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
		Some(HybridTimestamp {
			physical_ms: now_ms.saturating_sub(window_ms),
			logical: 0,
			node_id: 0,
		})
	}

	async fn rewrite_tiered_slabs_for_retention_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<TieredRetentionRewriteReport> {
		let mut report = TieredRetentionRewriteReport::default();
		if max_slabs == 0 {
			return Ok(report);
		}
		let now_ms = self.clock.now_ms();
		let audit_floor = self
			.mvcc_retention_floor_hlc_at(now_ms)
			.unwrap_or(HybridTimestamp {
				physical_ms: now_ms,
				logical: 0,
				node_id: 0,
			});
		for pointer in self.slab.list_tiered_slabs().await {
			if report.rewritten >= max_slabs {
				break;
			}
			match self
				.rewrite_one_tiered_slab_for_retention(&pointer, now_ms, audit_floor)
				.await
			{
				Ok(Some(outcome)) => {
					if outcome.mutated_slab {
						report.rewritten += 1;
					}
					report.pruned_versions += outcome.pruned_versions;
					report.pruned_data_rows += outcome.retention_stats.pruned_data_rows;
					report.pruned_tombstone_rows += outcome.retention_stats.pruned_tombstone_rows;
					report.pair_drops += outcome.retention_stats.pair_drops;
					report.ambiguous_rows_retained +=
						outcome.retention_stats.ambiguous_rows_retained;
					report.max_lag_seconds = report
						.max_lag_seconds
						.max(outcome.retention_stats.max_lag_seconds);
					report.reclaimed_bytes += outcome.reclaimed_bytes;
				}
				Ok(None) => {}
				Err(error) => {
					tracing::warn!(
						slab_id = pointer.slab_id.get(),
						l2_uri = %pointer.l2_uri,
						error = %error,
						"tiered MVCC retention rewrite failed; will retry on next scan"
					);
					report.failed += 1;
				}
			}
		}
		Ok(report)
	}

	async fn rewrite_one_tiered_slab_for_retention(
		&self,
		pointer: &TieredSlabPointer,
		now_ms: u64,
		audit_floor: HybridTimestamp,
	) -> std::io::Result<Option<PreparedTieredRetentionOutcome>> {
		let _permit = authorize_autonomous_action(AutonomousActionSurface::L2RetentionRewrite);
		let _slab_rewrite_claim = self
			.slab
			.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
			.await;
		self.refuse_proposal_over_unpublished_commit(pointer.slab_id, Some(pointer))
			.await?;
		let snapshot = self.state_machine.state_snapshot().await;
		let Some(prepared) = self
			.slab
			.prepare_tiered_table_retention_rewrite(
				pointer,
				self.mvcc_retention_policy,
				&snapshot.table_lifecycle,
				&snapshot.table_schemas,
				now_ms,
			)
			.await?
		else {
			return Ok(None);
		};
		match prepared {
			PreparedTieredRetentionRewrite::Rewrite {
				slab_id,
				old_l2_uri,
				old_bytes,
				encrypted_bytes,
				payload_hash,
				tombstone_metadata,
				supersession_metadata,
				pruned_versions,
				retention_stats,
				reclaimed_bytes,
			} => {
				let key = self.retention_rewritten_key(slab_id);
				let planned_uri = self.object_store.planned_upload_uri(&key)?;
				self.slab
					.stage_table_drop_l2_cleanup(
						slab_id,
						&old_l2_uri,
						Some(old_bytes),
						Some((&planned_uri, encrypted_bytes.len() as u64)),
					)
					.await?;
				self.rate_limit_upload(encrypted_bytes.len()).await;
				let new_uri = self.object_store.put(&key, &encrypted_bytes).await?;
				if new_uri != planned_uri {
					self.slab
						.persist_ambiguous_upload_intent(
							slab_id,
							&new_uri,
							encrypted_bytes.len() as u64,
						)
						.await?;
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"object store returned a different upload URI",
					));
				}
				let fetched = self.object_store.get(&new_uri).await?;
				let fetched_hash = crate::hash::content::digest(&fetched);
				if fetched_hash != payload_hash {
					if let Err(cleanup_error) = self
						.slab
						.discard_unsubmitted_tier_candidate(
							slab_id,
							&new_uri,
							self.object_store.as_ref(),
						)
						.await
					{
						tracing::warn!(error=%cleanup_error,"unsubmitted rewrite candidate awaits cleanup");
					}
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!(
							"retention rewrite hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
							slab_id,
							&payload_hash[..4],
							&fetched_hash[..4],
						),
					));
				}
				let footer = self
					.slab
					.encrypted_tiered_footer_summary(&encrypted_bytes)?
					.ok_or_else(|| {
						std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							format!(
								"retention rewrite for slab {} did not produce an encrypted footer",
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
				self.slab
					.prepare_tier_candidate_proposal(slab_id, &new_uri)
					.await?;
				if let Err(error) = propose_tier_slab(&self.slab, self.raft.as_ref(), tier).await {
					self.cleanup_uploaded_object_after_failed_commit(
						slab_id,
						&new_uri,
						encrypted_bytes.len() as u64,
						&error,
					)
					.await?;
					return Err(error);
				}

				let observed_epoch = self.slab.capture_snapshot_epoch();
				if let Some(pointer) = self.sm_tier_pointer(slab_id).await {
					self.slab
						.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
						.await?;
				}
				self.slab
					.record_cleanup_replacement_published(slab_id, &old_l2_uri, &new_uri)
					.await?;
				if old_l2_uri != new_uri {
					if let Err(error) = SlabManager::retain_superseded_l2_object(
						slab_id,
						&old_l2_uri,
						"superseded tier object",
					) {
						tracing::warn!(
							slab_id = slab_id.get(),
							l2_uri = %old_l2_uri,
							error = %error,
							"tiered retention rewrite committed but old object cleanup failed"
						);
					} else {
						self.slab.clear_retired_tiered_cleanup(slab_id);
					}
				} else {
					self.slab.clear_retired_tiered_cleanup(slab_id);
				}
				tracing::info!(
					slab_id = slab_id.get(),
					old_l2_uri = %old_l2_uri,
					new_l2_uri = %new_uri,
					pruned_versions,
					reclaimed_bytes,
					"rewrote tiered slab for MVCC retention"
				);
				self.emit_retention_pruned_audit(
					slab_id,
					audit_floor,
					pruned_versions,
					reclaimed_bytes,
					&old_l2_uri,
					Some(&new_uri),
					"rewrite",
				);
				Ok(Some(PreparedTieredRetentionOutcome {
					mutated_slab: true,
					pruned_versions,
					retention_stats,
					reclaimed_bytes,
				}))
			}
			PreparedTieredRetentionRewrite::Retire {
				slab_id,
				old_l2_uri,
				old_bytes,
				pruned_versions,
				retention_stats,
				reclaimed_bytes,
			} => {
				self.slab
					.stage_table_drop_l2_cleanup(slab_id, &old_l2_uri, Some(old_bytes), None)
					.await?;
				let observed_epoch = self.slab.capture_snapshot_epoch();
				self.raft.retire_tiered_slab(slab_id).await?;
				self.slab
					.retire_tiered_slab_at_snapshot_epoch(slab_id, observed_epoch)
					.await?;
				if let Err(error) = SlabManager::retain_superseded_l2_object(
					slab_id,
					&old_l2_uri,
					"superseded tier object",
				) {
					tracing::warn!(
						slab_id = slab_id.get(),
						l2_uri = %old_l2_uri,
						error = %error,
						"tiered retention retirement committed but old object cleanup failed"
					);
				} else {
					self.slab.clear_retired_tiered_cleanup(slab_id);
				}
				tracing::info!(
					slab_id = slab_id.get(),
					old_l2_uri = %old_l2_uri,
					pruned_versions,
					reclaimed_bytes,
					"retired fully-pruned tiered slab for MVCC retention"
				);
				self.emit_retention_pruned_audit(
					slab_id,
					audit_floor,
					pruned_versions,
					reclaimed_bytes,
					&old_l2_uri,
					None,
					"retire",
				);
				Ok(Some(PreparedTieredRetentionOutcome {
					mutated_slab: true,
					pruned_versions,
					retention_stats,
					reclaimed_bytes,
				}))
			}
			PreparedTieredRetentionRewrite::Observe { retention_stats } => {
				Ok(Some(PreparedTieredRetentionOutcome {
					mutated_slab: false,
					pruned_versions: 0,
					retention_stats,
					reclaimed_bytes: 0,
				}))
			}
		}
	}

	fn emit_retention_pruned_audit(
		&self,
		slab_id: SlabId,
		retention_floor: HybridTimestamp,
		pruned_versions: usize,
		reclaimed_bytes: u64,
		old_l2_uri: &str,
		new_l2_uri: Option<&str>,
		rewrite_action: &'static str,
	) {
		let mut message = format!(
			"retention_floor_ms={} slab_id={} rewrite_action={} pruned_versions={} reclaimed_bytes={} old_l2_uri={}",
			retention_floor.physical_ms,
			slab_id.get(),
			rewrite_action,
			pruned_versions,
			reclaimed_bytes,
			old_l2_uri,
		);
		if let Some(new_l2_uri) = new_l2_uri {
			message.push_str(" new_l2_uri=");
			message.push_str(new_l2_uri);
		}
		let mut audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::MvccSnapshotPruned,
			self.local_node,
			message,
		)
		.with_parameter("retention_floor_ms", retention_floor.physical_ms)
		.with_parameter("slab_id", slab_id.get())
		.with_parameter("rewrite_action", rewrite_action)
		.with_parameter("pruned_versions", pruned_versions as u64)
		.with_parameter("reclaimed_bytes", reclaimed_bytes)
		.with_parameter("old_l2_uri", old_l2_uri);
		if let Some(new_l2_uri) = new_l2_uri {
			audit = audit.with_parameter("new_l2_uri", new_l2_uri);
		}
		if let Err(err) = self.audit_sink.emit(audit) {
			tracing::warn!(
				slab_id = slab_id.get(),
				error = %err,
				"tiered MVCC retention prune audit emission failed"
			);
		}
	}

	fn emit_tenant_kek_rewrap_audit(
		&self,
		slab_id: SlabId,
		tenant_id: TenantId,
		old_tenant_kek_id: TenantKekId,
		new_tenant_kek_id: TenantKekId,
		old_l2_uri: &str,
		new_l2_uri: &str,
	) {
		let message = format!(
			"slab_id={} tenant_id={} old_tenant_kek_id={} new_tenant_kek_id={} old_l2_uri={} new_l2_uri={}",
			slab_id.get(),
			tenant_id.get(),
			old_tenant_kek_id.get(),
			new_tenant_kek_id.get(),
			old_l2_uri,
			new_l2_uri,
		);
		let audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoSlabDekRewrapped,
			self.local_node,
			message,
		)
		.with_tenant(tenant_id)
		.with_parameter("slab_id", slab_id.get())
		.with_parameter("tenant_id", tenant_id.get())
		.with_parameter("old_tenant_kek_id", old_tenant_kek_id.get())
		.with_parameter("new_tenant_kek_id", new_tenant_kek_id.get())
		.with_parameter("old_l2_uri", old_l2_uri)
		.with_parameter("new_l2_uri", new_l2_uri);
		if let Err(err) = self.audit_sink.emit(audit) {
			tracing::warn!(
				slab_id = slab_id.get(),
				error = %err,
				"tenant KEK re-wrap audit emission failed"
			);
		}
	}

	/// Pull the state machine's authoritative tier pointer for a
	/// slab (post-apply). Returns `None` if for some reason the
	/// apply didn't land — which we treat as a terminal failure
	/// upstream.
	async fn sm_tier_pointer(&self, slab_id: SlabId) -> Option<TieredSlabPointer> {
		self.state_machine
			.tiered_slab(slab_id)
			.await
			.map(|t| TieredSlabPointer {
				slab_id: t.slab_id,
				l2_uri: t.l2_uri.clone(),
				payload_hash: t.payload_hash,
				is_encrypted: t.is_encrypted,
				tenant_id: t.tenant_id,
				tenant_kek_id: t.tenant_kek_id,
				stats_format_version: t.stats_format_version,
				stats_block_len: t.stats_block_len,
				stats_block_hash128: t.stats_block_hash128,
				tombstone_metadata: t.tombstone_metadata,
				supersession_metadata: t.supersession_metadata,
			})
	}
	fn try_sm_tier_pointer(&self, slab_id: SlabId) -> std::io::Result<Option<TieredSlabPointer>> {
		Ok(self
			.state_machine
			.try_tiered_slab(slab_id)?
			.map(|t| TieredSlabPointer {
				slab_id: t.slab_id,
				l2_uri: t.l2_uri.clone(),
				payload_hash: t.payload_hash,
				is_encrypted: t.is_encrypted,
				tenant_id: t.tenant_id,
				tenant_kek_id: t.tenant_kek_id,
				stats_format_version: t.stats_format_version,
				stats_block_len: t.stats_block_len,
				stats_block_hash128: t.stats_block_hash128,
				tombstone_metadata: t.tombstone_metadata,
				supersession_metadata: t.supersession_metadata,
			}))
	}

	async fn sm_tier_pointers(&self) -> Vec<TieredSlabPointer> {
		let state = self.state_machine.state_snapshot().await;
		let mut pointers = state
			.tiered_slabs
			.values()
			.map(|tier| TieredSlabPointer {
				slab_id: tier.slab_id,
				l2_uri: tier.l2_uri.clone(),
				payload_hash: tier.payload_hash,
				is_encrypted: tier.is_encrypted,
				tenant_id: tier.tenant_id,
				tenant_kek_id: tier.tenant_kek_id,
				stats_format_version: tier.stats_format_version,
				stats_block_len: tier.stats_block_len,
				stats_block_hash128: tier.stats_block_hash128,
				tombstone_metadata: tier.tombstone_metadata,
				supersession_metadata: tier.supersession_metadata,
			})
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// A proposal plans from this node's published generation. When the
	/// cluster has committed a pointer the node has not published, planning
	/// from the older generation would propose, and upload, over it again; the
	/// proposer refuses typed instead and uploads nothing.
	async fn refuse_proposal_over_unpublished_commit(
		&self,
		slab_id: SlabId,
		published: Option<&TieredSlabPointer>,
	) -> std::io::Result<()> {
		self.slab.refuse_unresolved_cleanup(slab_id)?;
		let committed = self.sm_tier_pointer(slab_id).await;
		if committed.is_some() && committed.as_ref() != published
			|| self.slab.unapplied_tier_pointer(slab_id).is_some()
		{
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		Ok(())
	}

	async fn delete_object_if_present(&self, uri: &str) -> std::io::Result<()> {
		match self.object_store.delete(uri).await {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error),
		}
	}

	async fn cleanup_uploaded_object_after_failed_commit(
		&self,
		slab_id: SlabId,
		new_uri: &str,
		bytes: u64,
		error: &std::io::Error,
	) -> std::io::Result<()> {
		if self
			.sm_tier_pointer(slab_id)
			.await
			.is_some_and(|pointer| pointer.l2_uri == new_uri)
		{
			tracing::warn!(
				slab_id = slab_id.get(),
				l2_uri = %new_uri,
				"tier commit returned error after state machine already pointed at uploaded object; preserving object"
			);
			return Ok(());
		}
		if tier_proposal_not_submitted(error) {
			return self
				.slab
				.record_tier_candidate_not_submitted(slab_id, new_uri, bytes)
				.await;
		}

		self.slab
			.persist_ambiguous_upload_intent(slab_id, new_uri, bytes)
			.await?;
		let _ =
			SlabManager::retain_superseded_l2_object(slab_id, new_uri, "commit result ambiguous");
		Ok(())
	}

	async fn persist_retained_object(&self, pointer: &TieredSlabPointer) -> std::io::Result<()> {
		self.slab
			.record_retained_pointer_transition(pointer, None, true)
			.await
	}

	async fn reconcile_tiered_runtime_state_once(
		&self,
	) -> std::io::Result<TieredStateReconcileReport> {
		let mut report = TieredStateReconcileReport::default();
		let observed_epoch = self.slab.capture_snapshot_epoch();
		let authoritative = self.sm_tier_pointers().await;
		#[cfg(test)]
		{
			let hold = self.reconcile_snapshot_hold.lock().unwrap().take();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				if let Ok(permit) = release.acquire().await {
					permit.forget();
				}
			}
		}
		let local = self.slab.list_tiered_slabs().await;
		let candidates = authoritative
			.iter()
			.chain(&local)
			.map(|p| p.slab_id)
			.collect::<std::collections::BTreeSet<_>>();
		for slab_id in candidates {
			let _slab_rewrite_claim = self
				.slab
				.acquire_slab_rewrite_exclusive_claim(slab_id)
				.await;
			let retired = match self
				.slab
				.reconcile_tier_slab_at_snapshot_epoch(slab_id, observed_epoch, || {
					std::future::ready(self.try_sm_tier_pointer(slab_id))
				})
				.await
			{
				Ok((changed, retired)) => {
					if changed {
						report.reconciled += 1;
					}
					retired
				}
				Err(error) => {
					report.failed += 1;
					tracing::warn!(slab_id = slab_id.get(), error = %error, "failed to reconcile local tier state from current committed authority");
					continue;
				}
			};
			if let Some(pointer) = retired {
				if let Err(error) = SlabManager::retain_superseded_l2_object(
					slab_id,
					&pointer.l2_uri,
					"superseded tier object",
				) {
					report.failed += 1;
					tracing::warn!(slab_id = slab_id.get(), error = %error, "failed to retain retired L2 object during committed-state reconciliation");
					continue;
				}
				self.slab.clear_retired_tiered_cleanup(slab_id);
			}
		}

		for pointer in self.slab.list_retired_tiered_cleanup_candidates().await {
			if let Err(error) = self.persist_retained_object(&pointer).await {
				report.failed += 1;
				tracing::warn!(slab_id = pointer.slab_id.get(), error = %error, "failed to record retained tier object");
				continue;
			}
			if let Err(error) = self
				.slab
				.retry_retired_tiered_local_cleanup(pointer.slab_id)
				.await
			{
				report.failed += 1;
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					error = %error,
					"failed to retry retired local slab cleanup"
				);
				continue;
			}
			if let Err(error) = SlabManager::retain_superseded_l2_object(
				pointer.slab_id,
				&pointer.l2_uri,
				"superseded tier object",
			) {
				report.failed += 1;
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					l2_uri = %pointer.l2_uri,
					error = %error,
					"failed to retry retired tiered object cleanup"
				);
				continue;
			}
			self.slab.clear_retired_tiered_cleanup(pointer.slab_id);
			report.reconciled += 1;
		}

		Ok(report)
	}

	async fn backfill_supersession_metadata_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<SupersessionMetadataBackfillReport> {
		let mut report = SupersessionMetadataBackfillReport::default();
		if max_slabs == 0 {
			return Ok(report);
		}
		let mut attempted = 0;
		for pointer in self.slab.list_tiered_slabs().await {
			if !pointer.supersession_metadata.is_untracked() {
				continue;
			}
			if attempted >= max_slabs {
				break;
			}
			attempted += 1;
			match self
				.backfill_one_supersession_metadata_pointer(&pointer)
				.await
			{
				Ok(changed) => {
					if changed {
						report.backfilled += 1;
					}
				}
				Err(error) => {
					report.failed += 1;
					tracing::warn!(
						slab_id = pointer.slab_id.get(),
						l2_uri = %pointer.l2_uri,
						error = %error,
						"tiered supersession metadata backfill failed; will retry on next scan"
					);
				}
			}
		}
		Ok(report)
	}

	async fn backfill_one_supersession_metadata_pointer(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<bool> {
		// Candidates were selected before ownership. Serialize with retirement
		// and row rewrites, then validate both local and committed publication.
		let _slab_rewrite_claim = self
			.slab
			.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
			.await;
		if self.slab.tier_pointer(pointer.slab_id).await.as_ref() != Some(pointer)
			|| self.sm_tier_pointer(pointer.slab_id).await.as_ref() != Some(pointer)
		{
			return Ok(false);
		}

		if !pointer.supersession_metadata.is_untracked()
			|| self.slab.pending_metadata_requires_exact(pointer.slab_id)
		{
			return Ok(false);
		}
		self.refuse_proposal_over_unpublished_commit(pointer.slab_id, Some(pointer))
			.await?;
		let supersession_metadata = self.slab.slab_supersession_metadata(pointer.slab_id)?;
		let tier = TieredSlab {
			slab_id: pointer.slab_id,
			l2_uri: pointer.l2_uri.clone(),
			payload_hash: pointer.payload_hash,
			is_encrypted: pointer.is_encrypted,
			tenant_id: pointer.tenant_id,
			tenant_kek_id: pointer.tenant_kek_id,
			stats_format_version: pointer.stats_format_version,
			stats_block_len: pointer.stats_block_len,
			stats_block_hash128: pointer.stats_block_hash128,
			tombstone_metadata: pointer.tombstone_metadata,
			supersession_metadata,
		};
		propose_tier_slab(&self.slab, self.raft.as_ref(), tier).await?;
		let observed_epoch = self.slab.capture_snapshot_epoch();
		if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
			self.slab
				.apply_tier_slab_at_snapshot_epoch(pointer, observed_epoch)
				.await?;
		}
		tracing::info!(
			slab_id = pointer.slab_id.get(),
			supersession_metadata = ?supersession_metadata,
			"backfilled tiered supersession metadata"
		);
		Ok(true)
	}

	async fn reclaim_tombstoned_ranges_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<crate::storage::manager::TombstoneCompactionReport> {
		let _permit =
			authorize_autonomous_action(AutonomousActionSurface::MigrationTombstoneReclaim);
		let state = self.state_machine.state_snapshot().await;
		self.slab
			.reclaim_tombstoned_ranges(
				&state.tombstones,
				&state.table_schemas,
				&state.secondary_indexes,
				max_slabs,
			)
			.await
	}

	async fn reclaim_rehydrated_slabs_once(&self) -> std::io::Result<RehydrationSweepReport> {
		let _permit = authorize_autonomous_action(AutonomousActionSurface::L1RehydratedSlabReclaim);
		let mut report = RehydrationSweepReport::default();
		let now = Instant::now();
		for candidate in self.slab.list_rehydrated_slabs().await {
			if candidate.keep_local_until > now {
				report.skipped_guard += 1;
				continue;
			}
			if self.slab.drop_rehydrated_slab(candidate.slab_id).await? {
				report.reclaimed += 1;
			}
		}
		Ok(report)
	}

	/// Per-upload pacing tied to `max_upload_bytes_per_sec`.
	///
	/// Each upload sleeps for `bytes / cap` seconds so the sweeper's
	/// average outbound rate stays at or below the configured cap.
	/// There is deliberately no clamp to `scan_interval` here: a
	/// 32 GiB slab at a 100 MiB/s cap should sleep for the full ~5
	/// minutes the cap implies, not bypass the limit by clamping to
	/// one scan interval. Operators who want larger slabs to upload
	/// faster should raise `max_upload_bytes_per_sec`, not rely on
	/// the sweeper to silently exceed it.
	async fn rate_limit_upload(&self, bytes: usize) {
		let Some(cap) = self.config.max_upload_bytes_per_sec else {
			return;
		};
		if cap == 0 {
			return;
		}
		let millis = ((bytes as u128) * 1000) / (cap as u128).max(1);
		if millis > 0 {
			sleep(Duration::from_millis(millis.min(u64::MAX as u128) as u64)).await;
		}
	}

	fn rewritten_key(&self, slab_id: SlabId) -> String {
		format!(
			"{}crypto/slab_{:016x}_{}.wnd",
			self.config.key_prefix,
			slab_id,
			uuid::Uuid::new_v4().simple()
		)
	}

	fn retention_rewritten_key(&self, slab_id: SlabId) -> String {
		format!(
			"{}retention/slab_{:016x}_{}.wnd",
			self.config.key_prefix,
			slab_id,
			uuid::Uuid::new_v4().simple()
		)
	}

	/// Background loop shape: sleep until the next scan deadline
	/// *or* a wakeup signal, then run one `sweep_once`. Shutdown
	/// happens by dropping every clone of `self.wakeup` (the
	/// runtime will no longer deliver notifies and the loop exits
	/// naturally at the next cycle).
	async fn run(self) {
		let mut next_deadline = Instant::now() + self.config.scan_interval;
		loop {
			let now = Instant::now();
			let remaining = next_deadline.saturating_duration_since(now);
			tokio::select! {
				_ = sleep(remaining) => {},
				_ = self.wakeup.notified() => {},
			}
			if let Err(e) = self.sweep_once().await {
				metrics().storage_tiering_pass_errors_total.inc();
				tracing::warn!(error = %e, "sweeper pass returned error");
			}
			next_deadline = Instant::now() + self.config.scan_interval;
		}
	}
}

/// Per-pass bookkeeping. Returned from [`SlabSweeper::sweep_once`]
/// for test assertions and for operators who want to wire the
/// counts into Prometheus.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
	pub tiered: usize,
	pub failed: usize,
	pub skipped_too_small: usize,
	pub skipped_already_tiered: usize,
	pub rewritten_plaintext_tiered: usize,
	pub skipped_already_encrypted_tiered: usize,
	pub plaintext_rewrite_failures: usize,
	pub rewrapped_tiered_deks: usize,
	pub skipped_current_tenant_kek_tiered: usize,
	pub backfilled_tenant_kek_metadata_tiered: usize,
	pub dek_rewrap_failures: usize,
	pub retention_rewritten_tiered: usize,
	pub retention_pruned_versions: usize,
	pub retention_reclaimed_bytes: u64,
	pub retention_rewrite_failures: usize,
	pub supersession_metadata_backfilled_tiered: usize,
	pub supersession_metadata_backfill_failures: usize,
	pub skipped_rehydration_guard: usize,
	pub rehydrated_reclaimed: usize,
	pub reclaimed_slabs: usize,
	pub reclaimed_bytes: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PlaintextRewriteReport {
	rewritten: usize,
	skipped_already_encrypted: usize,
	failed: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TenantKekRewrapReport {
	rewrapped: usize,
	skipped_already_current: usize,
	backfilled_metadata: usize,
	failed: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TieredStateReconcileReport {
	reconciled: usize,
	failed: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SupersessionMetadataBackfillReport {
	backfilled: usize,
	failed: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TieredRetentionRewriteReport {
	rewritten: usize,
	pruned_versions: usize,
	pruned_data_rows: usize,
	pruned_tombstone_rows: usize,
	pair_drops: usize,
	ambiguous_rows_retained: usize,
	max_lag_seconds: u64,
	reclaimed_bytes: u64,
	failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedTieredRetentionOutcome {
	mutated_slab: bool,
	pruned_versions: usize,
	retention_stats: RetentionCompactionStats,
	reclaimed_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaintextRewriteOutcome {
	AlreadyEncrypted,
	Rewritten,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TenantKekRewrapOutcome {
	AlreadyCurrent,
	MetadataBackfilled,
	Rewrapped,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RehydrationSweepReport {
	reclaimed: usize,
	skipped_guard: usize,
}

// ============================================================
//  TIER OBSERVER BRIDGE
// ============================================================

use crate::consensus::TierObserver;

/// Glue that lets a [`SlabManager`] be installed as the state
/// machine's [`TierObserver`]. Holds an `Arc<SlabManager>` so the
/// SM apply loop can fire filesystem work without owning the
/// manager's lifetime.
pub struct ManagerTierObserver {
	slab: Arc<SlabManager>,
	snapshot_source: std::sync::OnceLock<crate::consensus::TierSnapshotSource>,
}

impl ManagerTierObserver {
	pub fn new(slab: Arc<SlabManager>) -> Self {
		Self {
			slab,
			snapshot_source: std::sync::OnceLock::new(),
		}
	}
}

#[async_trait]
impl TierObserver for ManagerTierObserver {
	fn bind_tier_snapshot_source(&self, source: crate::consensus::TierSnapshotSource) {
		self.slab.bind_canonical_tier_source(source.clone());
		let _ = self.snapshot_source.set(source);
	}

	fn before_tier_snapshot_install(&self) -> std::io::Result<u64> {
		self.slab.begin_tier_snapshot_install()
	}

	async fn on_tier_snapshot_installed(
		&self,
		tiers: &[TieredSlab],
		index: Option<u64>,
		snapshot_epoch: u64,
	) -> std::io::Result<()> {
		let canonical = match self.snapshot_source.get() {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		let pointers = tiers
			.iter()
			.map(|tier| TieredSlabPointer {
				slab_id: tier.slab_id,
				l2_uri: tier.l2_uri.clone(),
				payload_hash: tier.payload_hash,
				is_encrypted: tier.is_encrypted,
				tenant_id: tier.tenant_id,
				tenant_kek_id: tier.tenant_kek_id,
				stats_format_version: tier.stats_format_version,
				stats_block_len: tier.stats_block_len,
				stats_block_hash128: tier.stats_block_hash128,
				tombstone_metadata: tier.tombstone_metadata,
				supersession_metadata: tier.supersession_metadata,
			})
			.collect();
		match self
			.slab
			.finish_tier_snapshot_install_at_epoch(pointers, index, snapshot_epoch)
			.await
		{
			Ok(()) => Ok(()),
			Err(error) => {
				if self.slab.is_snapshot_append_deferral(&error) {
					let canonical = canonical.as_ref().ok_or_else(|| {
						std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"snapshot source missing",
						)
					})?;
					self.slab
						.stage_deferred_snapshot_tiers(canonical, index, snapshot_epoch)
						.await?;
				}
				if let Some(source) = self.snapshot_source.get()
					&& self.slab.defer_tier_snapshot_reconciliation(
						snapshot_epoch,
						source.clone(),
						&error,
					)? {
					return Ok(());
				}
				Err(error)
			}
		}
	}

	fn before_slab_retired(&self, slab_id: SlabId, applied_log_index: u64) {
		if !self.slab.observer_is_superseded(Some(applied_log_index)) {
			self.slab
				.fence_unapplied_tier_retirement(slab_id, Some(applied_log_index));
		}
	}

	async fn on_slab_tiered(
		&self,
		tier: &TieredSlab,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		self.slab
			.apply_tier_slab_committed(
				TieredSlabPointer {
					slab_id: tier.slab_id,
					l2_uri: tier.l2_uri.clone(),
					payload_hash: tier.payload_hash,
					is_encrypted: tier.is_encrypted,
					tenant_id: tier.tenant_id,
					tenant_kek_id: tier.tenant_kek_id,
					stats_format_version: tier.stats_format_version,
					stats_block_len: tier.stats_block_len,
					stats_block_hash128: tier.stats_block_hash128,
					tombstone_metadata: tier.tombstone_metadata,
					supersession_metadata: tier.supersession_metadata,
				},
				Some(applied_log_index),
			)
			.await
	}

	async fn on_slab_retired(
		&self,
		slab_id: SlabId,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		let result = self
			.slab
			.retire_tiered_slab_committed(slab_id, Some(applied_log_index))
			.await
			.map(|_| ());
		if result.is_ok() && !self.slab.observer_is_superseded(Some(applied_log_index)) {
			self.slab
				.clear_unapplied_tier_retirement(slab_id, Some(applied_log_index));
		}
		result
	}

	async fn on_migrated_range_tombstone(
		&self,
		tombstone: &MigratedRangeTombstone,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		self.slab
			.record_migrated_range_tombstone(tombstone, Some(applied_log_index))
			.await
			.map(|_| ())
	}

	async fn on_lsm_compaction_committed(
		&self,
		commit: &crate::storage::CompactionCommit,
		applied_log_index: u64,
	) -> std::io::Result<()> {
		self.slab
			.observe_lsm_compaction_commit(commit, applied_log_index)
			.await
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::audit::InMemoryAuditSink;
	use crate::consensus::monitor::PhysicalClock;
	use crate::crypto::CryptoManager;
	use crate::schema::table::{
		RecordBodyCompressionPolicy, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
		STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
		STORAGE_ROW_TOMBSTONE_COLUMN, TableObjectId,
	};
	use crate::schema::{Identity, Locality};
	use crate::storage::core::storage_multirow_index_entries_from_frame;
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::l2::LocalFsObjectStore;
	use crate::storage::manager::SlabManagerConfig;
	use arrow::array::{Array, BooleanArray, FixedSizeBinaryBuilder, UInt64Array, UInt64Builder};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;
	use std::time::{SystemTime, UNIX_EPOCH};
	use tokio::sync::Mutex as AsyncMutex;

	/// Test-only committer that skips openraft entirely. Records
	/// the commit in the provided SM so the sweeper's observer
	/// path still fires.
	struct DirectApplyCommitter {
		observer: Arc<dyn TierObserver>,
		state_machine: Option<WendelonStateMachine>,
		/// When `Some`, every call to `commit_tier_slab` returns
		/// an error cloned from this template. Models a persistent
		/// "leader unreachable" scenario.
		persistent_err: AsyncMutex<Option<std::io::ErrorKind>>,
		/// When `Some`, the next `commit_tier_slab` first seeds the
		/// state machine with the committed pointer and then returns
		/// an error. Models an ambiguous post-commit timeout.
		commit_applied_err: AsyncMutex<Option<std::io::ErrorKind>>,
	}

	impl DirectApplyCommitter {
		fn ok(sm: WendelonStateMachine, observer: Arc<dyn TierObserver>) -> Self {
			Self {
				observer,
				state_machine: Some(sm),
				persistent_err: AsyncMutex::new(None),
				commit_applied_err: AsyncMutex::new(None),
			}
		}

		fn commit_applied_then_err(
			sm: WendelonStateMachine,
			observer: Arc<dyn TierObserver>,
			kind: std::io::ErrorKind,
		) -> Self {
			Self {
				observer,
				state_machine: Some(sm),
				persistent_err: AsyncMutex::new(None),
				commit_applied_err: AsyncMutex::new(Some(kind)),
			}
		}
	}

	#[async_trait]
	impl TierCommitter for DirectApplyCommitter {
		async fn commit_tier_slab(
			&self,
			proposal: crate::storage::TierProposal,
		) -> std::io::Result<()> {
			let tier = proposal.tier().clone();
			// Modelled like RaftCommitter: submitted before the commit, applied
			// only on success; a failure leaves the outcome unknown.
			let submission = proposal.begin_submission()?;
			if let Some(kind) = *self.persistent_err.lock().await {
				return Err(std::io::Error::new(kind, "simulated commit failure"));
			}
			if let Some(kind) = self.commit_applied_err.lock().await.take() {
				if let Some(sm) = &self.state_machine {
					sm.seed_tiered_slab_for_test(tier).await;
				}
				return Err(std::io::Error::new(
					kind,
					"simulated post-commit ambiguous failure",
				));
			}
			if let Some(sm) = &self.state_machine {
				sm.seed_tiered_slab_for_test(tier.clone()).await;
			}
			self.observer.on_slab_tiered(&tier, 0).await?;
			submission.applied();
			Ok(())
		}

		async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()> {
			if let Some(kind) = *self.persistent_err.lock().await {
				return Err(std::io::Error::new(kind, "simulated commit failure"));
			}
			if let Some(sm) = &self.state_machine {
				sm.retire_tiered_slab_for_test(slab_id).await;
			}
			self.observer.on_slab_retired(slab_id, 0).await?;
			Ok(())
		}
	}

	struct FixedClock {
		now_ms: u64,
	}

	impl PhysicalClock for FixedClock {
		fn now_ms(&self) -> u64 {
			self.now_ms
		}
	}

	fn fresh_dir(tag: &str) -> crate::test_support::TestDir {
		crate::test_support::TestDir::new(&format!("wendelon-sweeper-{tag}"))
	}

	async fn seed_state_machine_pointer(sm: &WendelonStateMachine, pointer: &TieredSlabPointer) {
		sm.seed_tiered_slab_for_test(TieredSlab {
			slab_id: pointer.slab_id,
			l2_uri: pointer.l2_uri.clone(),
			payload_hash: pointer.payload_hash,
			is_encrypted: pointer.is_encrypted,
			tenant_id: pointer.tenant_id,
			tenant_kek_id: pointer.tenant_kek_id,
			stats_format_version: pointer.stats_format_version,
			stats_block_len: pointer.stats_block_len,
			stats_block_hash128: pointer.stats_block_hash128,
			tombstone_metadata: pointer.tombstone_metadata,
			supersession_metadata: pointer.supersession_metadata,
		})
		.await;
	}

	fn hlc() -> crate::schema::HybridTimestamp {
		crate::schema::HybridTimestamp {
			physical_ms: 1,
			logical: 0,
			node_id: 1,
		}
	}

	fn hlc_at(physical_ms: u64) -> crate::schema::HybridTimestamp {
		crate::schema::HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn id(locality: u64) -> crate::schema::Identity {
		let mut bytes = [0u8; 32];
		bytes[..8].copy_from_slice(&locality.to_le_bytes());
		crate::schema::Identity::new(bytes)
	}

	fn tenant() -> crate::schema::TenantId {
		crate::schema::TenantId::ZERO
	}

	fn node_id(id: u32) -> crate::schema::NodeId {
		crate::schema::NodeId { id, _reserved: 0 }
	}

	fn encode_test_batch(batch: &RecordBatch) -> Vec<u8> {
		crate::storage::ipc::encode_record_batch_bytes(
			batch,
			RecordBodyCompressionPolicy::Uncompressed,
		)
		.unwrap()
	}

	fn test_hlc_storage_bytes(hlc: HybridTimestamp) -> [u8; 16] {
		let mut bytes = [0_u8; 16];
		bytes[..8].copy_from_slice(&hlc.physical_ms.to_le_bytes());
		bytes[8..12].copy_from_slice(&hlc.logical.to_le_bytes());
		bytes[12..16].copy_from_slice(&hlc.node_id.to_le_bytes());
		bytes
	}

	fn encode_test_multirow_storage_frame(
		rows: &[(Identity, Locality, HybridTimestamp, u64, bool)],
	) -> Vec<u8> {
		let mut row_hlc = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
		let mut row_identity = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
		let mut row_locality = UInt64Builder::with_capacity(rows.len());
		for (identity, locality, hlc, _, _) in rows {
			row_hlc.append_value(test_hlc_storage_bytes(*hlc)).unwrap();
			row_identity.append_value(identity.as_bytes()).unwrap();
			row_locality.append_value(locality.get());
		}
		let row_tombstone = BooleanArray::from(
			rows.iter()
				.map(|(_, _, _, _, value)| *value)
				.collect::<Vec<_>>(),
		);
		let schema = Arc::new(Schema::new(vec![
			Field::new("value", DataType::UInt64, false),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
			Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(UInt64Array::from(
					rows.iter()
						.map(|(_, _, _, value, _)| *value)
						.collect::<Vec<_>>(),
				)) as Arc<dyn Array>,
				Arc::new(row_hlc.finish()) as Arc<dyn Array>,
				Arc::new(row_identity.finish()) as Arc<dyn Array>,
				Arc::new(row_locality.finish()) as Arc<dyn Array>,
				Arc::new(row_tombstone) as Arc<dyn Array>,
			],
		)
		.unwrap();
		encode_test_batch(&batch)
	}

	fn encode_test_row_tombstone_marker_frame(
		table_object_id: TableObjectId,
		key_identity: Identity,
		routing_locality: Locality,
		row_hlc: HybridTimestamp,
	) -> Vec<u8> {
		let mut row_hlc_builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
		row_hlc_builder
			.append_value(test_hlc_storage_bytes(row_hlc))
			.unwrap();
		let mut row_identity = FixedSizeBinaryBuilder::with_capacity(1, 32);
		row_identity.append_value(key_identity.as_bytes()).unwrap();
		let mut row_locality = UInt64Builder::with_capacity(1);
		row_locality.append_value(routing_locality.get());
		let mut row_table_object_id = FixedSizeBinaryBuilder::with_capacity(1, 16);
		row_table_object_id
			.append_value(table_object_id.as_bytes())
			.unwrap();
		let schema = Arc::new(Schema::new(vec![
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
			Field::new(
				STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
				DataType::FixedSizeBinary(16),
				false,
			),
			Field::new(STORAGE_ROW_TOMBSTONE_COLUMN, DataType::Boolean, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(row_hlc_builder.finish()) as Arc<dyn Array>,
				Arc::new(row_identity.finish()) as Arc<dyn Array>,
				Arc::new(row_locality.finish()) as Arc<dyn Array>,
				Arc::new(row_table_object_id.finish()) as Arc<dyn Array>,
				Arc::new(BooleanArray::from(vec![true])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		encode_test_batch(&batch)
	}

	fn test_crypto() -> Arc<CryptoManager> {
		let crypto = Arc::new(CryptoManager::new_dev());
		crypto.install_local_only_for_tests(tenant()).unwrap();
		crypto
	}

	async fn build_manager_with_sealed_slabs(
		l2: Option<Arc<dyn ObjectStore>>,
	) -> (crate::test_support::TestDir, Arc<SlabManager>) {
		use crate::storage::core::StorageEngine;
		let dir = fresh_dir("l1");
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: dir.to_path_buf(),
			// Tiny cap so two appends produce two sealed slabs.
			max_slab_bytes: 1,
			io,
			l2,
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		// Each append rotates because the write_cursor crosses
		// `max_slab_bytes` on the first record.
		manager
			.append(tenant(), id(1), hlc(), b"hello-1")
			.await
			.unwrap();
		manager
			.append(tenant(), id(2), hlc(), b"hello-2")
			.await
			.unwrap();
		manager
			.append(tenant(), id(3), hlc(), b"hello-3")
			.await
			.unwrap();
		(dir, manager)
	}

	#[tokio::test]
	async fn sweeper_tiers_sealed_slabs_and_unlinks_l1() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		let sealed_before = manager.list_sealed_slabs().await;
		assert!(
			!sealed_before.is_empty(),
			"precondition: manager should have sealed slabs"
		);

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));

		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, sealed_before.len());
		assert_eq!(report.failed, 0);

		// L1 files for every sealed slab should be gone.
		for info in &sealed_before {
			let path = l1_dir.join(format!("slab_{:016x}.wnd", info.slab_id));
			assert!(!path.exists(), "L1 file still present: {}", path.display());
		}
		// Manager's tiered count matches.
		assert_eq!(manager.tiered_count().await, sealed_before.len());
		// Sealed count is zero — they all moved to tiered.
		assert_eq!(manager.sealed_count().await, 0);
	}

	#[tokio::test]
	async fn sweeper_tiers_row_tombstone_slab_with_metadata() {
		let l1_dir = fresh_dir("row-tombstone-tier-l1");
		let l2_root = fresh_dir("row-tombstone-tier-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();

		let key = id(0x5050);
		let locality = Locality::new(0x5050);
		let data_hlc = hlc_at(10);
		let tombstone_hlc = hlc_at(20);
		let data_frame =
			encode_test_multirow_storage_frame(&[(key, locality, data_hlc, 100, false)]);
		let data_entries = storage_multirow_index_entries_from_frame(&data_frame)
			.unwrap()
			.expect("data entry");
		manager
			.append_durable_multirow_with_locality(tenant(), &data_frame, &data_entries)
			.await
			.unwrap();
		let tombstone_frame = encode_test_row_tombstone_marker_frame(
			TableObjectId::new([0x50; 16]),
			key,
			locality,
			tombstone_hlc,
		);
		let tombstone_entries = storage_multirow_index_entries_from_frame(&tombstone_frame)
			.unwrap()
			.expect("tombstone entry");
		manager
			.append_durable_multirow_with_locality(tenant(), &tombstone_frame, &tombstone_entries)
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		assert_eq!(
			manager.slab_row_tombstone_metadata(sealed_id).unwrap(),
			TombstoneMetadata::Earliest(tombstone_hlc)
		);

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm.clone(),
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, 1);
		assert_eq!(report.failed, 0);
		assert_eq!(manager.sealed_count().await, 0);

		let local_pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_eq!(
			local_pointer.tombstone_metadata,
			TombstoneMetadata::Earliest(tombstone_hlc)
		);
		let snapshot = sm.state_snapshot().await;
		let committed_pointer = snapshot
			.tiered_slabs
			.get(&sealed_id)
			.expect("state machine committed tier pointer");
		assert_eq!(
			committed_pointer.tombstone_metadata,
			TombstoneMetadata::Earliest(tombstone_hlc)
		);
	}

	#[tokio::test]
	async fn read_pivots_to_l2_after_tiering() {
		use crate::storage::core::StorageEngine;

		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		// Record the location of key=1 before sweeping so we can
		// verify the read path pivots without a re-lookup.
		let loc1 = manager
			.lookup(tenant(), id(1))
			.unwrap()
			.expect("key present");
		let original = manager.read(loc1).await.expect("pre-tier read ok");

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: String::new(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);
		sweeper.sweep_once().await.unwrap();
		assert!(manager.is_tiered(loc1.slab_id).await);

		// Same location, but now served from L2.
		let via_l2 = manager.read(loc1).await.expect("post-tier read ok");
		assert_eq!(via_l2, original);
	}

	#[tokio::test]
	async fn sweeper_skips_slabs_under_min_size() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				// Each sealed slab is ~4K on disk; set the floor
				// absurdly high so nothing qualifies.
				min_slab_bytes: u64::MAX,
				max_upload_bytes_per_sec: None,
				key_prefix: String::new(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, 0);
		assert!(report.skipped_too_small > 0);
		assert_eq!(manager.tiered_count().await, 0);
	}

	#[tokio::test]
	async fn failed_commit_leaves_l1_intact() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let sealed_before = manager.list_sealed_slabs().await;

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer_inner = Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		*committer_inner.persistent_err.lock().await = Some(std::io::ErrorKind::ConnectionRefused);
		let committer: Arc<dyn TierCommitter> = committer_inner.clone();

		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: String::new(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.tiered, 0);
		assert!(report.failed > 0);
		// L1 still intact for every sealed slab.
		for info in &sealed_before {
			let path = l1_dir.join(format!("slab_{:016x}.wnd", info.slab_id));
			assert!(path.exists(), "L1 file missing: {}", path.display());
		}
		assert_eq!(manager.tiered_count().await, 0);
	}

	async fn a2_same_layout_crypto_reconcile_control(rewrap: bool) {
		let _ = tracing_subscriber::fmt().with_test_writer().try_init();
		use crate::storage::core::StorageEngine;
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let l2_dir = fresh_dir("a2-reconcile-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let old_location = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let old_raw = manager.read_sealed_raw(old_location.slab_id).await.unwrap();
		let crypto = manager.retained_cleanup_test_crypto();
		let old_blob = if rewrap {
			manager
				.encrypt_tiered_object_blob(old_location.slab_id, &old_raw)
				.unwrap()
				.bytes
		} else {
			old_raw.clone()
		};
		let old_footer = manager.encrypted_tiered_footer_summary(&old_blob).unwrap();
		let old = TieredSlabPointer {
			slab_id: old_location.slab_id,
			l2_uri: l2.put("a2/old", &old_blob).await.unwrap(),
			payload_hash: crate::hash::content::digest(&old_blob),
			is_encrypted: rewrap,
			tenant_id: old_footer.as_ref().map(|_| tenant()),
			tenant_kek_id: old_footer.as_ref().map(|f| f.tenant_kek_id),
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		assert_eq!(manager.read(old_location).await.unwrap(), b"hello-1");
		if rewrap {
			let rotation = crypto.prepare_rotation(tenant()).unwrap();
			crypto.apply_rotation(&rotation).unwrap();
		}
		let encrypted = manager
			.encrypt_tiered_object_blob(old.slab_id, &old_raw)
			.unwrap();
		let mut current = old.clone();
		current.l2_uri = l2
			.put("a2/new-same-layout", &encrypted.bytes)
			.await
			.unwrap();
		current.payload_hash = crate::hash::content::digest(&encrypted.bytes);
		current.is_encrypted = true;
		current.tenant_id = Some(tenant());
		current.tenant_kek_id = Some(encrypted.tenant_kek_id);
		current.stats_format_version = encrypted.stats_locator.map(|l| l.stats_format_version);
		current.stats_block_len = encrypted.stats_locator.map(|l| l.stats_block_len);
		current.stats_block_hash128 = encrypted.stats_locator.map(|l| l.stats_block_hash128);
		assert_eq!(
			manager
				.decode_tiered_object_blob(&current, &encrypted.bytes)
				.unwrap(),
			old_raw,
			"crypto rewrite preserves exact row layout"
		);
		if rewrap {
			assert_ne!(current.tenant_kek_id, old.tenant_kek_id);
		}
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sm = WendelonStateMachine::new().with_tier_observer(observer.clone());
		seed_state_machine_pointer(&sm, &old).await;
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm.clone(),
			SweeperConfig::default(),
		);
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*sweeper.reconcile_snapshot_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		let epoch = manager.capture_snapshot_epoch();
		let task = tokio::spawn(async move { sweeper.reconcile_tiered_runtime_state_once().await });
		tokio::time::timeout(Duration::from_secs(10), entered.acquire())
			.await
			.unwrap()
			.unwrap()
			.forget();
		let mut target = sm.clone();
		RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 702),
				payload: EntryPayload::Normal(crate::consensus::WendelonRequest::TierSlab(
					TieredSlab {
						slab_id: current.slab_id,
						l2_uri: current.l2_uri.clone(),
						payload_hash: current.payload_hash,
						is_encrypted: current.is_encrypted,
						tenant_id: current.tenant_id,
						tenant_kek_id: current.tenant_kek_id,
						stats_format_version: current.stats_format_version,
						stats_block_len: current.stats_block_len,
						stats_block_hash128: current.stats_block_hash128,
						tombstone_metadata: current.tombstone_metadata,
						supersession_metadata: current.supersession_metadata,
					},
				)),
			}],
		)
		.await
		.unwrap();
		assert_eq!(
			manager.capture_snapshot_epoch(),
			epoch,
			"this is log apply, not snapshot fencing"
		);
		assert_eq!(
			manager.tier_pointer(current.slab_id).await,
			Some(current.clone())
		);
		let live = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		assert_eq!(manager.read(live).await.unwrap(), b"hello-1");
		assert!(manager.require_cleanup_authority().is_ok());
		release.add_permits(1);
		let report = task.await.unwrap().unwrap();
		let actual = manager.tier_pointer(current.slab_id).await;
		eprintln!(
			"same-layout rewrap={rewrap}, report={report:?}, published={actual:?}, old={old:?}, current={current:?}"
		);
		assert_eq!(
			actual,
			Some(current),
			"A2 stale reconcile must preserve the newer same-layout crypto generation"
		);
	}

	#[tokio::test]
	async fn a2_reconcile_never_rolls_back_same_layout_tenant_kek_rewrap() {
		a2_same_layout_crypto_reconcile_control(true).await;
	}

	#[tokio::test]
	async fn a2_reconcile_never_rolls_back_plaintext_to_ciphertext_rewrite() {
		a2_same_layout_crypto_reconcile_control(false).await;
	}

	#[tokio::test]
	async fn a2_reconcile_captured_before_log_apply_never_restores_pruned_rows() {
		let _ = tracing_subscriber::fmt().with_test_writer().try_init();
		use crate::storage::core::StorageEngine;
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let l2_dir = fresh_dir("a2-reconcile-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let old_location = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let old_raw = manager.read_sealed_raw(old_location.slab_id).await.unwrap();
		let old = TieredSlabPointer {
			slab_id: old_location.slab_id,
			l2_uri: l2.put("a2/old", &old_raw).await.unwrap(),
			payload_hash: crate::hash::content::digest(&old_raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		assert_eq!(manager.read(old_location).await.unwrap(), b"hello-1");
		// The newer generation has a valid remaining row and prunes key1.
		let rewritten = fresh_dir("a2-pruned-generation");
		let new_path = rewritten.join("new.wnd");
		let new_slab = crate::storage::core::LocalSlabStore::open(
			&new_path,
			old.slab_id,
			Arc::new(crate::storage::file_ops::BlockingFileOps),
			crate::storage::GroupCommitConfig::default(),
		)
		.await
		.unwrap();
		let survivor = new_slab
			.append(tenant(), id(9), hlc(), b"survivor")
			.await
			.unwrap();
		new_slab.fsync().await.unwrap();
		let new_raw = std::fs::read(new_path).unwrap();
		// A compacted generation is produced encrypted, described by its own
		// append's offset, as the production rewrites produce it.
		let new_object = manager
			.encrypt_tiered_blob_with_evidence(
				old.slab_id,
				&new_raw,
				&std::collections::BTreeSet::from([survivor.offset]),
				None,
			)
			.unwrap();
		let mut current = old.clone();
		current.l2_uri = l2.put("a2/new-pruned", &new_object.bytes).await.unwrap();
		current.payload_hash = crate::hash::content::digest(&new_object.bytes);
		current.is_encrypted = true;
		current.tenant_id = Some(tenant());
		current.tenant_kek_id = Some(new_object.tenant_kek_id);
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sm = WendelonStateMachine::new().with_tier_observer(observer.clone());
		seed_state_machine_pointer(&sm, &old).await;
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm.clone(),
			SweeperConfig::default(),
		);
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*sweeper.reconcile_snapshot_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		let epoch = manager.capture_snapshot_epoch();
		let task = tokio::spawn(async move { sweeper.reconcile_tiered_runtime_state_once().await });
		tokio::time::timeout(Duration::from_secs(10), entered.acquire())
			.await
			.unwrap()
			.unwrap()
			.forget();
		let mut target = sm.clone();
		RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 702),
				payload: EntryPayload::Normal(crate::consensus::WendelonRequest::TierSlab(
					TieredSlab {
						slab_id: current.slab_id,
						l2_uri: current.l2_uri.clone(),
						payload_hash: current.payload_hash,
						is_encrypted: current.is_encrypted,
						tenant_id: current.tenant_id,
						tenant_kek_id: current.tenant_kek_id,
						stats_format_version: current.stats_format_version,
						stats_block_len: current.stats_block_len,
						stats_block_hash128: current.stats_block_hash128,
						tombstone_metadata: current.tombstone_metadata,
						supersession_metadata: current.supersession_metadata,
					},
				)),
			}],
		)
		.await
		.unwrap();
		assert_eq!(
			manager.capture_snapshot_epoch(),
			epoch,
			"this is log apply, not snapshot fencing"
		);
		assert_eq!(
			manager.tier_pointer(current.slab_id).await,
			Some(current.clone())
		);
		if let Ok(Some(location)) = manager.lookup(tenant(), id(1)) {
			assert!(
				manager.read(location).await.is_err(),
				"new generation must not serve key1 before stale reconcile resumes"
			);
		}
		let survivor = manager.lookup(tenant(), id(9)).unwrap().unwrap();
		assert_eq!(manager.read(survivor).await.unwrap(), b"survivor");
		eprintln!(
			"A2 diagnostics before stale reconcile: cleanup={:?}, unapplied={:?}",
			manager.require_cleanup_authority(),
			manager.unapplied_tier_pointer(current.slab_id)
		);
		release.add_permits(1);
		let report = task.await.unwrap().unwrap();
		eprintln!(
			"A2 diagnostics after stale reconcile: report={report:?}, cleanup={:?}, unapplied={:?}, survivor_lookup={:?}",
			manager.require_cleanup_authority(),
			manager.unapplied_tier_pointer(current.slab_id),
			manager.lookup(tenant(), id(9))
		);
		if let Ok(Some(location)) = manager.lookup(tenant(), id(1)) {
			if let Ok(bytes) = manager.read(location).await {
				panic!(
					"A2 stale reconcile resurrected a committed-pruned row: {bytes:?}; report={report:?}"
				);
			}
		}
		assert_eq!(
			manager.tier_pointer(current.slab_id).await,
			Some(current),
			"A2 stale reconcile must not revert the newer committed pointer"
		);
	}

	#[tokio::test]
	async fn a3_reconcile_captured_before_first_log_tier_never_retires_a_live_slab() {
		use crate::storage::core::StorageEngine;
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let l2_dir = fresh_dir("a3-reconcile-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let location = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		assert_eq!(manager.read(location).await.unwrap(), b"hello-1");
		let raw = manager.read_sealed_raw(location.slab_id).await.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: location.slab_id,
			l2_uri: l2.put("a3/new-live", &raw).await.unwrap(),
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sm = WendelonStateMachine::new().with_tier_observer(observer.clone());
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm.clone(),
			SweeperConfig::default(),
		);
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*sweeper.reconcile_snapshot_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		let epoch = manager.capture_snapshot_epoch();
		let task = tokio::spawn(async move { sweeper.reconcile_tiered_runtime_state_once().await });
		tokio::time::timeout(Duration::from_secs(10), entered.acquire())
			.await
			.unwrap()
			.unwrap()
			.forget();
		let mut target = sm.clone();
		RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 703),
				payload: EntryPayload::Normal(crate::consensus::WendelonRequest::TierSlab(
					TieredSlab {
						slab_id: pointer.slab_id,
						l2_uri: pointer.l2_uri.clone(),
						payload_hash: pointer.payload_hash,
						is_encrypted: pointer.is_encrypted,
						tenant_id: pointer.tenant_id,
						tenant_kek_id: pointer.tenant_kek_id,
						stats_format_version: pointer.stats_format_version,
						stats_block_len: pointer.stats_block_len,
						stats_block_hash128: pointer.stats_block_hash128,
						tombstone_metadata: pointer.tombstone_metadata,
						supersession_metadata: pointer.supersession_metadata,
					},
				)),
			}],
		)
		.await
		.unwrap();
		assert_eq!(
			manager.capture_snapshot_epoch(),
			epoch,
			"log apply does not bump snapshot epoch"
		);
		assert_eq!(
			manager.tier_pointer(pointer.slab_id).await,
			Some(pointer.clone())
		);
		let before = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		assert_eq!(manager.read(before).await.unwrap(), b"hello-1");
		release.add_permits(1);
		let report = task.await.unwrap().unwrap();
		assert_eq!(
			sm.state_snapshot()
				.await
				.tiered_slabs
				.get(&pointer.slab_id)
				.map(|p| p.l2_uri.clone()),
			Some(pointer.l2_uri.clone()),
			"the state machine still names the live slab"
		);
		let after = manager.lookup(tenant(), id(1));
		assert!(
			matches!(&after, Ok(Some(_))),
			"A3 stale reconcile removed an acknowledged live row: {after:?}; report={report:?}"
		);
		assert_eq!(
			manager.read(after.unwrap().unwrap()).await.unwrap(),
			b"hello-1"
		);
		assert_eq!(manager.tier_pointer(pointer.slab_id).await, Some(pointer));
	}

	struct A4PausedLogCommitter {
		sm: WendelonStateMachine,
		entered: Arc<tokio::sync::Semaphore>,
		release: Arc<tokio::sync::Semaphore>,
		index: Arc<std::sync::atomic::AtomicU64>,
	}

	async fn a4_apply_control_request(
		sm: &WendelonStateMachine,
		index: &std::sync::atomic::AtomicU64,
		request: crate::consensus::WendelonRequest,
	) -> std::io::Result<()> {
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let mut target = sm.clone();
		RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(
					CommittedLeaderId::new(3, 1),
					index.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
				),
				payload: EntryPayload::Normal(request),
			}],
		)
		.await
		.map_err(|e| std::io::Error::other(e.to_string()))?;
		Ok(())
	}

	#[async_trait]
	impl TierCommitter for A4PausedLogCommitter {
		async fn commit_tier_slab(
			&self,
			proposal: crate::storage::TierProposal,
		) -> std::io::Result<()> {
			let tier = proposal.tier().clone();
			self.entered.add_permits(1);
			self.release.acquire().await.unwrap().forget();
			a4_apply_control_request(
				&self.sm,
				&self.index,
				crate::consensus::WendelonRequest::TierSlab(tier),
			)
			.await
		}
		async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()> {
			a4_apply_control_request(
				&self.sm,
				&self.index,
				crate::consensus::WendelonRequest::RetireTieredSlab(RetireTieredSlab { slab_id }),
			)
			.await
		}
	}

	async fn a4_retirement_during_pointer_maintenance_control(
		backfill: bool,
		retired_before_start: bool,
	) {
		use crate::storage::core::StorageEngine;
		use std::future::Future;
		use std::task::Poll;
		let l2_dir = fresh_dir("a4-retirement-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_dir).unwrap());
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let location = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(location.slab_id).await.unwrap();
		let encrypted = manager
			.encrypt_tiered_object_blob(location.slab_id, &raw)
			.unwrap();
		let old = TieredSlabPointer {
			slab_id: location.slab_id,
			l2_uri: l2.put("a4/old", &encrypted.bytes).await.unwrap(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted.stats_locator.map(|l| l.stats_format_version),
			stats_block_len: encrypted.stats_locator.map(|l| l.stats_block_len),
			stats_block_hash128: encrypted.stats_locator.map(|l| l.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: if backfill {
				SupersessionMetadata::Untracked
			} else {
				SupersessionMetadata::None
			},
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		assert_eq!(manager.read(location).await.unwrap(), b"hello-1");
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sm = WendelonStateMachine::new().with_tier_observer(observer);
		seed_state_machine_pointer(&sm, &old).await;
		if !backfill {
			let crypto = manager.retained_cleanup_test_crypto();
			let rotation = crypto.prepare_rotation(tenant()).unwrap();
			crypto.apply_rotation(&rotation).unwrap();
			assert_ne!(
				manager.active_tenant_kek_id(tenant()).unwrap(),
				encrypted.tenant_kek_id
			);
		}
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		let index = Arc::new(std::sync::atomic::AtomicU64::new(900));
		let committer: Arc<dyn TierCommitter> = Arc::new(A4PausedLogCommitter {
			sm: sm.clone(),
			entered: entered.clone(),
			release: release.clone(),
			index: index.clone(),
		});
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm.clone(),
			SweeperConfig::default(),
		);
		let slab_id = old.slab_id;
		if retired_before_start {
			let claim = manager.acquire_slab_rewrite_exclusive_claim(slab_id).await;
			a4_apply_control_request(
				&sm,
				&index,
				crate::consensus::WendelonRequest::RetireTieredSlab(RetireTieredSlab { slab_id }),
			)
			.await
			.unwrap();
			drop(claim);
			release.add_permits(1);
		}
		let operation = tokio::spawn(async move {
			if backfill {
				sweeper
					.backfill_one_supersession_metadata_pointer(&old)
					.await
					.map(|_| ())
			} else {
				sweeper.rewrap_one_tiered_slab(&old).await.map(|_| ())
			}
		});
		let result;
		if retired_before_start {
			result = operation.await.unwrap();
		} else {
			tokio::time::timeout(Duration::from_secs(10), entered.acquire())
				.await
				.unwrap()
				.unwrap()
				.forget();
			// Poll the real competing claim once. A granted claim lets retirement
			// commit now; a queued claim lets maintenance finish before retirement.
			// Neither order depends on a sleep or on a scheduling guess.
			let mut retiring_claim =
				Box::pin(manager.acquire_slab_rewrite_exclusive_claim(slab_id));
			let first_poll =
				std::future::poll_fn(|cx| Poll::Ready(retiring_claim.as_mut().poll(cx))).await;
			match first_poll {
				Poll::Ready(claim) => {
					a4_apply_control_request(
						&sm,
						&index,
						crate::consensus::WendelonRequest::RetireTieredSlab(RetireTieredSlab {
							slab_id,
						}),
					)
					.await
					.unwrap();
					assert!(
						!sm.state_snapshot()
							.await
							.tiered_slabs
							.contains_key(&slab_id),
						"retirement must be applied before stale maintenance resumes"
					);
					drop(claim);
					release.add_permits(1);
					result = operation.await.unwrap();
				}
				Poll::Pending => {
					release.add_permits(1);
					result = operation.await.unwrap();
					let claim = retiring_claim.await;
					a4_apply_control_request(
						&sm,
						&index,
						crate::consensus::WendelonRequest::RetireTieredSlab(RetireTieredSlab {
							slab_id,
						}),
					)
					.await
					.unwrap();
					drop(claim);
				}
			}
		}

		assert!(
			!sm.state_snapshot()
				.await
				.tiered_slabs
				.contains_key(&slab_id),
			"A4 stale maintenance resurrected a retired slab; backfill={backfill}; maintenance={result:?}"
		);
		if let Ok(Some(location)) = manager.lookup(tenant(), id(1)) {
			assert!(
				manager.read(location).await.is_err(),
				"a retired slab must never serve its old row"
			);
		}
	}

	#[tokio::test]
	async fn a4_tenant_kek_rewrap_never_resurrects_a_concurrently_retired_slab() {
		a4_retirement_during_pointer_maintenance_control(false, false).await;
	}

	#[tokio::test]
	async fn a4_supersession_backfill_never_resurrects_a_concurrently_retired_slab() {
		a4_retirement_during_pointer_maintenance_control(true, false).await;
	}

	#[tokio::test]
	async fn a4_supersession_backfill_skips_a_candidate_retired_before_it_claims() {
		a4_retirement_during_pointer_maintenance_control(true, true).await;
	}

	#[tokio::test]
	async fn apply_tier_slab_is_idempotent() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2)).await;
		let sealed = manager.list_sealed_slabs().await;
		let info = sealed.first().unwrap().clone();

		let ptr = TieredSlabPointer {
			slab_id: info.slab_id,
			l2_uri: "file:///dev/null".into(),
			payload_hash: [0; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(ptr.clone()).await.unwrap();
		// Second apply must not fail even though sealed no longer
		// contains the slab.
		manager.apply_tier_slab(ptr).await.unwrap();
		assert!(manager.is_tiered(info.slab_id).await);
	}

	#[tokio::test]
	async fn constructed_absent_tier_timeout_must_fence_stale_read() {
		use crate::storage::core::StorageEngine;
		let l2_root = fresh_dir("dc112-retire-timeout-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let uri = l2.put("dc112-retire-timeout/old", &raw).await.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: uri,
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(pointer).await.unwrap();
		let before = manager
			.read(manager.lookup(tenant(), id(1)).unwrap().unwrap())
			.await
			.unwrap();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		// Explicitly constructed discrepancy; this is not a production-reachability claim.
		let sm = WendelonStateMachine::new().with_tier_observer(observer.clone());
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper =
			SlabSweeper::new(manager.clone(), l2, committer, sm, SweeperConfig::default());
		let gate = manager.hold_index_write_gate_for_test().await;
		let report = tokio::time::timeout(
			Duration::from_secs(35),
			sweeper.reconcile_tiered_runtime_state_once(),
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(report.failed, 1);
		let lookup = manager.lookup(tenant(), id(1));
		let read = match &lookup {
			Ok(Some(location)) => {
				Some(tokio::time::timeout(Duration::from_secs(5), manager.read(*location)).await)
			}
			_ => None,
		};
		drop(gate);
		assert!(
			lookup.is_err(),
			"constructed absent-tier timeout must fence: lookup={lookup:?}, read={read:?}, expected_before={before:?}"
		);
		let retry = sweeper.reconcile_tiered_runtime_state_once().await.unwrap();
		assert_eq!(retry.failed, 0);
		assert_eq!(retry.reconciled, 1);
		assert!(manager.lookup(tenant(), id(1)).unwrap().is_none());
		assert!(manager.tier_authority_readiness_reason().is_none());
	}

	#[tokio::test]
	async fn sweeper_reconcile_rejects_a_pointer_captured_before_a_snapshot() {
		use crate::storage::core::StorageEngine;
		use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

		let l2_root = fresh_dir("dc112-sweeper-snapshot-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let hash = crate::hash::content::digest(&raw);
		let old_uri = l2.put("dc112-sweeper-snapshot/old", &raw).await.unwrap();
		let new_uri = l2.put("dc112-sweeper-snapshot/new", &raw).await.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: old_uri,
			payload_hash: hash,
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		let mut current_pointer = old_pointer.clone();
		current_pointer.l2_uri = new_uri;
		manager
			.apply_tier_slab(current_pointer.clone())
			.await
			.unwrap();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sm = WendelonStateMachine::new().with_tier_observer(observer.clone());
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm.clone(),
			SweeperConfig::default(),
		);
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*sweeper.reconcile_snapshot_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		let task = tokio::spawn(async move { sweeper.reconcile_tiered_runtime_state_once().await });
		tokio::time::timeout(Duration::from_secs(60), entered.acquire())
			.await
			.unwrap()
			.unwrap()
			.forget();
		let mut source = WendelonStateMachine::new();
		seed_state_machine_pointer(&source, &current_pointer).await;
		RaftStateMachine::apply(
			&mut source,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 702),
				payload: EntryPayload::Blank,
			}],
		)
		.await
		.unwrap();
		let mut builder = RaftStateMachine::get_snapshot_builder(&mut source).await;
		let built = builder.build_snapshot().await.unwrap();
		let mut target = sm.clone();
		RaftStateMachine::install_snapshot(&mut target, &built.meta, built.snapshot)
			.await
			.unwrap();
		release.add_permits(1);
		let _report = task.await.unwrap().unwrap();
		assert_eq!(
			manager.tier_pointer(loc.slab_id).await,
			Some(current_pointer),
			"a stale sweeper snapshot cannot overwrite newer committed tier authority"
		);
		let location = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		assert_eq!(manager.read(location).await.unwrap(), b"hello-1");
		assert!(
			manager.unapplied_tier_pointer(loc.slab_id).is_none(),
			"fresh reconciliation must not install a stale-pointer fence"
		);
	}

	#[tokio::test]
	async fn snapshot_during_old_object_cleanup_retains_the_old_object() {
		use crate::storage::core::StorageEngine;
		use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

		let l2_root = fresh_dir("snapshot-cleanup-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let uri = l2.put("snapshot-cleanup/old", &raw).await.unwrap();
		let old = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: uri.clone(),
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sm = WendelonStateMachine::new().with_tier_observer(observer.clone());
		seed_state_machine_pointer(&sm, &old).await;
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm.clone(),
			SweeperConfig::default(),
		);
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*sweeper.old_cleanup_hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		let task =
			tokio::spawn(async move { sweeper.rewrite_one_plaintext_tiered_slab(&old).await });
		tokio::time::timeout(Duration::from_secs(60), entered.acquire())
			.await
			.unwrap()
			.unwrap()
			.forget();
		let replacement = manager.tier_pointer(loc.slab_id).await.unwrap();
		assert_ne!(replacement.l2_uri, uri);
		let replacement_bytes = l2.head(&replacement.l2_uri).await.unwrap().size_bytes;
		let mut source = WendelonStateMachine::new();
		let old_pointer = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: uri.clone(),
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		seed_state_machine_pointer(&source, &old_pointer).await;
		RaftStateMachine::apply(
			&mut source,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 708),
				payload: EntryPayload::Blank,
			}],
		)
		.await
		.unwrap();
		let mut builder = RaftStateMachine::get_snapshot_builder(&mut source).await;
		let built = builder.build_snapshot().await.unwrap();
		let mut target = sm.clone();
		RaftStateMachine::install_snapshot(&mut target, &built.meta, built.snapshot)
			.await
			.unwrap();
		release.add_permits(1);
		let _result = task.await.unwrap();
		assert!(
			l2.get(&uri).await.is_ok(),
			"a snapshot-raced cleanup must retain the old object"
		);
		tokio::time::timeout(Duration::from_secs(5), manager.wait_tier_authority_ready())
			.await
			.unwrap()
			.unwrap();
		assert_eq!(manager.tier_pointer(loc.slab_id).await.unwrap().l2_uri, uri);
		let before_reopen = manager.retained_cleanup_test_accounting();
		assert_eq!(
			before_reopen,
			(2, raw.len() as u64 + replacement_bytes, 0),
			"the snapshot-superseded replacement must be durably accounted"
		);
		manager.fsync_all_local_slabs().await.unwrap();
		let crypto = manager.retained_cleanup_test_crypto();
		drop(target);
		drop(sm);
		let old_manager = Arc::downgrade(&manager);
		drop(manager);
		tokio::time::timeout(Duration::from_secs(5), async {
			while old_manager.upgrade().is_some() {
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("manager owners must release before reopen");
		let reopened = SlabManager::open(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: Default::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: Default::default(),
			crypto,
		})
		.await
		.unwrap();
		assert_eq!(reopened.retained_cleanup_test_accounting(), before_reopen);
	}

	struct NoHeadStore(Arc<dyn ObjectStore>);
	#[async_trait::async_trait]
	impl ObjectStore for NoHeadStore {
		fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
			self.0.planned_upload_uri(key)
		}
		async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
			self.0.put(key, bytes).await
		}
		async fn head(&self, _uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
			panic!("snapshot must not call HEAD")
		}
		async fn get_range(
			&self,
			uri: &str,
			range: std::ops::Range<u64>,
		) -> std::io::Result<bytes::Bytes> {
			self.0.get_range(uri, range).await
		}
		async fn get_bytes(&self, uri: &str) -> std::io::Result<bytes::Bytes> {
			self.0.get_bytes(uri).await
		}
		async fn list(
			&self,
			prefix: &str,
		) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
			self.0.list(prefix).await
		}
		async fn delete(&self, uri: &str) -> std::io::Result<()> {
			self.0.delete(uri).await
		}
	}

	#[tokio::test]
	async fn snapshot_records_unknown_size_without_object_store_head() {
		use crate::storage::core::StorageEngine;
		use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let root = fresh_dir("snapshot-no-head-l2");
		let inner: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*root).unwrap());
		let l2: Arc<dyn ObjectStore> = Arc::new(NoHeadStore(inner));
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let old_uri = l2.put("snapshot-no-head/old.wnd", &raw).await.unwrap();
		let new_uri = l2.put("snapshot-no-head/new.wnd", &raw).await.unwrap();
		let old = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		let mut replacement = old;
		replacement.l2_uri = new_uri;
		let mut source = WendelonStateMachine::new();
		seed_state_machine_pointer(&source, &replacement).await;
		RaftStateMachine::apply(
			&mut source,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 807),
				payload: EntryPayload::Blank,
			}],
		)
		.await
		.unwrap();
		let mut builder = RaftStateMachine::get_snapshot_builder(&mut source).await;
		let built = builder.build_snapshot().await.unwrap();
		let mut target = WendelonStateMachine::new()
			.with_tier_observer(Arc::new(ManagerTierObserver::new(manager.clone())));
		RaftStateMachine::install_snapshot(&mut target, &built.meta, built.snapshot)
			.await
			.unwrap();
		tokio::time::timeout(Duration::from_secs(5), manager.wait_tier_authority_ready())
			.await
			.unwrap()
			.unwrap();
		assert_eq!(manager.tier_pointer(loc.slab_id).await, Some(replacement));
		assert_eq!(manager.retained_cleanup_test_accounting(), (1, 0, 1));
	}

	#[tokio::test]
	async fn state_reconciliation_records_displaced_object_with_unknown_size() {
		use crate::storage::core::StorageEngine;
		let l2_root = fresh_dir("reconcile-retained-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let old_uri = l2.put("reconcile/old.wnd", &raw).await.unwrap();
		let new_uri = l2.put("reconcile/new.wnd", &raw).await.unwrap();
		let old = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: old_uri.clone(),
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		let mut replacement = old;
		replacement.l2_uri = new_uri;
		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &replacement).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig::default(),
		);
		let report = sweeper.reconcile_tiered_runtime_state_once().await.unwrap();
		assert_eq!(report.failed, 0);
		assert_eq!(manager.tier_pointer(loc.slab_id).await, Some(replacement));
		assert_eq!(manager.retained_cleanup_test_accounting(), (1, 0, 1));
		assert_eq!(l2.get(&old_uri).await.unwrap(), raw);
	}

	#[tokio::test]
	async fn sweeper_rewrites_plaintext_tiered_slabs_to_ciphertext() {
		use crate::storage::core::StorageEngine;

		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_l1_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager
			.lookup(tenant(), id(1))
			.unwrap()
			.expect("key present");
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let old_hash = crate::hash::content::digest(&raw);
		let old_uri = l2
			.put(&format!("legacy/slab_{:016x}.wnd", loc.slab_id), &raw)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: old_uri.clone(),
			payload_hash: old_hash,
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();
		assert_eq!(manager.read(loc).await.unwrap(), b"hello-1");

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);

		let first = sweeper.sweep_once().await.unwrap();
		assert_eq!(first.rewritten_plaintext_tiered, 1);
		assert_eq!(first.plaintext_rewrite_failures, 0);

		let new_pointer = manager.tier_pointer(loc.slab_id).await.unwrap();
		assert_ne!(new_pointer.l2_uri, old_uri);
		let new_blob = l2.get(&new_pointer.l2_uri).await.unwrap();
		let footer = manager
			.encrypted_tiered_footer_summary(&new_blob)
			.unwrap()
			.expect("rewritten object must be encrypted");
		assert_eq!(footer.slab_id, loc.slab_id);
		assert!(
			!new_blob
				.windows(b"hello-1".len())
				.any(|window| window == b"hello-1"),
			"ciphertext object leaked plaintext payload"
		);
		assert_eq!(manager.read(loc).await.unwrap(), b"hello-1");
		assert!(
			l2.get(&old_uri).await.is_ok(),
			"legacy plaintext object is retained after rewrite until owner-scoped retirement authority"
		);

		let second = sweeper.sweep_once().await.unwrap();
		assert_eq!(second.rewritten_plaintext_tiered, 0);
		assert!(second.skipped_already_encrypted_tiered >= 1);
	}

	#[tokio::test]
	async fn sweeper_rewraps_tiered_slabs_to_the_active_tenant_kek() {
		use crate::storage::core::StorageEngine;

		let l1_dir = fresh_dir("tenant-kek-rewrap-l1");
		let l2_root = fresh_dir("tenant-kek-rewrap-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: crypto.clone(),
		})
		.await
		.unwrap();

		let old_loc = manager
			.append(tenant(), id(0x5150), hlc_at(10), b"before-rotation")
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		assert_eq!(old_loc.slab_id, sealed_id);

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let audit_sink = Arc::new(InMemoryAuditSink::new());
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_audit_sink(node_id(7), audit_sink.clone());

		let first = sweeper.sweep_once().await.unwrap();
		assert_eq!(first.tiered, 1);
		let old_pointer = manager.tier_pointer(sealed_id).await.unwrap();
		let old_blob = l2.get(&old_pointer.l2_uri).await.unwrap();
		let old_footer = manager
			.encrypted_tiered_footer_summary(&old_blob)
			.unwrap()
			.expect("initial tiered object must be encrypted");
		assert_eq!(manager.read(old_loc).await.unwrap(), b"before-rotation");

		let rotation = crypto.prepare_rotation(tenant()).unwrap();
		let rotated_tenant_kek_id = rotation.tenant_kek_id;
		crypto.apply_rotation(&rotation).unwrap();

		let second = sweeper.sweep_once().await.unwrap();
		assert_eq!(second.rewrapped_tiered_deks, 1);
		assert_eq!(second.dek_rewrap_failures, 0);
		let new_pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_ne!(new_pointer.l2_uri, old_pointer.l2_uri);
		let new_blob = l2.get(&new_pointer.l2_uri).await.unwrap();
		let new_footer = manager
			.encrypted_tiered_footer_summary(&new_blob)
			.unwrap()
			.expect("re-wrapped tiered object must remain encrypted");
		assert_eq!(new_footer.tenant_kek_id, rotated_tenant_kek_id);
		assert_ne!(new_footer.tenant_kek_id, old_footer.tenant_kek_id);
		assert!(
			l2.get(&old_pointer.l2_uri).await.is_ok(),
			"old tenant-KEK object is retained after re-wrap until owner-scoped retirement authority"
		);
		assert_eq!(manager.read(old_loc).await.unwrap(), b"before-rotation");

		let events = audit_sink.events();
		assert_eq!(events.len(), 1);
		assert_eq!(events[0].event_type(), "crypto.slab_dek_rewrapped");
		assert!(events[0].message.contains(&format!(
			"old_tenant_kek_id={}",
			old_footer.tenant_kek_id.get()
		)));
		assert!(events[0].message.contains(&format!(
			"new_tenant_kek_id={}",
			rotated_tenant_kek_id.get()
		)));
	}

	#[tokio::test]
	async fn sweeper_backfills_missing_tenant_kek_pointer_metadata_without_rewrap() {
		use crate::storage::core::StorageEngine;

		let l1_dir = fresh_dir("tenant-kek-backfill-l1");
		let l2_root = fresh_dir("tenant-kek-backfill-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();

		let old_loc = manager
			.append(tenant(), id(0x5151), hlc_at(10), b"metadata-backfill")
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		assert_eq!(old_loc.slab_id, sealed_id);

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let audit_sink = Arc::new(InMemoryAuditSink::new());
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm.clone(),
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_audit_sink(node_id(8), audit_sink.clone());

		let first = sweeper.sweep_once().await.unwrap();
		assert_eq!(first.tiered, 1);

		let current_pointer = manager.tier_pointer(sealed_id).await.unwrap();
		let current_blob = l2.get(&current_pointer.l2_uri).await.unwrap();
		let current_footer = manager
			.encrypted_tiered_footer_summary(&current_blob)
			.unwrap()
			.expect("tiered object must be encrypted");

		let metadata_missing_pointer = TieredSlabPointer {
			slab_id: current_pointer.slab_id,
			l2_uri: current_pointer.l2_uri.clone(),
			payload_hash: current_pointer.payload_hash,
			is_encrypted: current_pointer.is_encrypted,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: current_pointer.tombstone_metadata,
			supersession_metadata: current_pointer.supersession_metadata,
		};
		manager
			.apply_tier_slab(metadata_missing_pointer.clone())
			.await
			.unwrap();
		seed_state_machine_pointer(&sm, &metadata_missing_pointer).await;

		let second = sweeper.sweep_once().await.unwrap();
		assert_eq!(second.rewrapped_tiered_deks, 0);
		assert_eq!(second.backfilled_tenant_kek_metadata_tiered, 1);
		assert_eq!(second.skipped_current_tenant_kek_tiered, 0);
		assert_eq!(second.dek_rewrap_failures, 0);
		assert!(audit_sink.events().is_empty());

		let refreshed_pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_eq!(refreshed_pointer.l2_uri, current_pointer.l2_uri);
		assert_eq!(refreshed_pointer.tenant_id, Some(current_footer.tenant_id));
		assert_eq!(
			refreshed_pointer.tenant_kek_id,
			Some(current_footer.tenant_kek_id)
		);

		let third = sweeper.sweep_once().await.unwrap();
		assert_eq!(third.rewrapped_tiered_deks, 0);
		assert_eq!(third.backfilled_tenant_kek_metadata_tiered, 0);
		assert!(third.skipped_current_tenant_kek_tiered >= 1);
	}

	#[tokio::test]
	async fn sweeper_backfills_untracked_supersession_metadata() {
		use crate::storage::core::StorageEngine;

		let l1_dir = fresh_dir("supersession-backfill-l1");
		let l2_root = fresh_dir("supersession-backfill-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();

		let key = id(0x4242);
		let old_hlc = hlc_at(10);
		let superseding_hlc = hlc_at(20);
		let old_loc = manager
			.append_durable(tenant(), key, old_hlc, b"old")
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		assert_eq!(old_loc.slab_id, sealed_id);
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::Untracked,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();
		manager
			.append_durable(tenant(), key, superseding_hlc, b"new")
			.await
			.unwrap();
		assert_eq!(
			manager
				.tier_pointer(sealed_id)
				.await
				.unwrap()
				.supersession_metadata,
			SupersessionMetadata::Untracked
		);

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);

		let pause = manager.pause_append_metadata_observed_once();
		let pending_append = {
			let manager = manager.clone();
			tokio::spawn(async move {
				manager
					.append_durable(tenant(), key, hlc_at(15), b"new")
					.await
			})
		};
		tokio::time::timeout(Duration::from_secs(60), pause.reached.notified())
			.await
			.unwrap();
		let current = manager.tier_pointer(sealed_id).await.unwrap();
		assert!(
			!sweeper
				.backfill_one_supersession_metadata_pointer(&current)
				.await
				.unwrap(),
			"pending metadata tokens prevent re-tracking"
		);
		pause.resume.notify_one();
		pending_append.await.unwrap().unwrap();
		let report = sweeper.sweep_once().await.unwrap();

		assert_eq!(report.supersession_metadata_backfilled_tiered, 1);
		assert_eq!(report.supersession_metadata_backfill_failures, 0);
		assert_eq!(
			manager
				.tier_pointer(sealed_id)
				.await
				.unwrap()
				.supersession_metadata,
			SupersessionMetadata::Earliest(hlc_at(15))
		);
		assert_eq!(manager.read(old_loc).await.unwrap(), b"old");
		assert_eq!(
			manager
				.read(manager.lookup(tenant(), key).unwrap().expect("latest row"))
				.await
				.unwrap(),
			b"new"
		);
	}

	#[tokio::test]
	async fn sweeper_rewrites_tiered_slabs_for_mvcc_retention() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-l1");
		let l2_root = fresh_dir("retention-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let key = id(9);
		let older = hlc_at(now_ms.saturating_sub(50_000));
		let anchor = hlc_at(now_ms.saturating_sub(20_000));
		let latest = hlc_at(now_ms.saturating_sub(5_000));
		manager
			.append(tenant(), key, older, b"v-old")
			.await
			.unwrap();
		manager
			.append(tenant(), key, anchor, b"v-anchor")
			.await
			.unwrap();
		manager
			.append(tenant(), key, latest, b"v-latest")
			.await
			.unwrap();
		let pre_rewrite_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(12_000)))
			.unwrap()
			.expect("anchor retained before rewrite");
		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_hash = crate::hash::content::digest(&encrypted.bytes);
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let tombstone_metadata = TombstoneMetadata::Earliest(hlc_at(now_ms.saturating_sub(1_000)));
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri.clone(),
			payload_hash: old_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let audit_sink = Arc::new(InMemoryAuditSink::new());
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_audit_sink(node_id(1), audit_sink.clone())
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewritten_tiered, 1);
		assert_eq!(report.retention_pruned_versions, 1);
		assert!(report.retention_reclaimed_bytes > 0);

		let new_pointer = manager.tier_pointer(sealed_id).await.unwrap();
		assert_ne!(new_pointer.l2_uri, old_uri);
		assert_eq!(new_pointer.tombstone_metadata, tombstone_metadata);
		assert!(
			l2.get(&old_uri).await.is_ok(),
			"old tiered object is retained after retention rewrite until owner-scoped retirement authority"
		);

		let retained_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(12_000)))
			.unwrap()
			.expect("retained-window anchor should remain");
		assert_eq!(manager.read(retained_anchor).await.unwrap(), b"v-anchor");
		assert_eq!(
			manager.read(pre_rewrite_anchor).await.unwrap(),
			b"v-anchor",
			"stale pre-rewrite locations should relocate to the rewritten object"
		);
		assert!(
			manager
				.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(30_000)))
				.unwrap()
				.is_none(),
			"versions older than the retention floor should disappear from the tiered index"
		);
		let retained_latest = manager
			.lookup(tenant(), key)
			.unwrap()
			.expect("latest survives");
		assert_eq!(manager.read(retained_latest).await.unwrap(), b"v-latest");
		let events = audit_sink.events();
		assert_eq!(events.len(), 1);
		assert_eq!(events[0].event_type(), "mvcc.snapshot_pruned");
		assert!(events[0].message.contains("rewrite_action=rewrite"));
		assert!(events[0].message.contains("pruned_versions=1"));
	}

	#[tokio::test]
	async fn sweeper_retires_fully_pruned_tiered_slabs_for_mvcc_retention() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-retire-l1");
		let l2_root = fresh_dir("retention-retire-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let key = id(17);
		let older = hlc_at(now_ms.saturating_sub(50_000));
		let anchor = hlc_at(now_ms.saturating_sub(20_000));
		let latest = hlc_at(now_ms.saturating_sub(5_000));
		let older_loc = manager
			.append(tenant(), key, older, b"v-old")
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		assert_eq!(older_loc.slab_id, sealed_id);
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_hash = crate::hash::content::digest(&encrypted.bytes);
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri.clone(),
			payload_hash: old_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();
		manager
			.append(tenant(), key, anchor, b"v-anchor")
			.await
			.unwrap();
		manager
			.append(tenant(), key, latest, b"v-latest")
			.await
			.unwrap();

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let audit_sink = Arc::new(InMemoryAuditSink::new());
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_audit_sink(node_id(1), audit_sink.clone())
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewritten_tiered, 1);
		assert_eq!(report.retention_pruned_versions, 1);
		assert!(report.retention_reclaimed_bytes > 0);
		assert!(
			manager.tier_pointer(sealed_id).await.is_none(),
			"fully pruned slab should retire its tier pointer"
		);
		assert!(
			l2.get(&old_uri).await.is_ok(),
			"retired tiered object is retained after retention retirement until owner-scoped retirement authority"
		);

		let retained_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(12_000)))
			.unwrap()
			.expect("retained-window anchor should remain");
		assert_eq!(manager.read(retained_anchor).await.unwrap(), b"v-anchor");
		assert!(
			manager
				.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(30_000)))
				.unwrap()
				.is_none(),
			"fully pruned slab should disappear from historical lookups"
		);
		let retained_latest = manager
			.lookup(tenant(), key)
			.unwrap()
			.expect("latest survives");
		assert_eq!(manager.read(retained_latest).await.unwrap(), b"v-latest");
		assert!(
			manager.read(older_loc).await.is_err(),
			"retired slab should not keep serving stale pre-retention locations"
		);
		let events = audit_sink.events();
		assert_eq!(events.len(), 1);
		assert_eq!(events[0].event_type(), "mvcc.snapshot_pruned");
		assert!(events[0].message.contains("rewrite_action=retire"));
		assert!(events[0].message.contains("pruned_versions=1"));
	}

	#[tokio::test]
	async fn sweeper_retention_rewrite_preserves_relocated_reads_for_duplicate_hlcs() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-duplicate-hlc-l1");
		let l2_root = fresh_dir("retention-duplicate-hlc-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let key = id(27);
		let duplicate_hlc = hlc_at(now_ms.saturating_sub(50_000));
		let latest = hlc_at(now_ms.saturating_sub(5_000));
		// Two byte-identical copies of one version (a replica retry or a leader
		// replay); a different payload at one HLC is a conflict, controlled by
		// `sweeper_retention_rewrite_keeps_conflicting_copies_refused_through_relocation`.
		manager
			.append(tenant(), key, duplicate_hlc, b"v-old")
			.await
			.unwrap();
		let later_copy = manager
			.append(tenant(), key, duplicate_hlc, b"v-old")
			.await
			.unwrap();
		manager
			.append(tenant(), key, latest, b"v-latest")
			.await
			.unwrap();
		let pre_rewrite_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(10_000)))
			.unwrap()
			.expect("latest duplicate before floor should be retained");
		assert_eq!(
			pre_rewrite_anchor, later_copy,
			"lookup_as_of should prefer the later duplicate at the same HLC"
		);
		assert_eq!(manager.read(pre_rewrite_anchor).await.unwrap(), b"v-old");

		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_hash = crate::hash::content::digest(&encrypted.bytes);
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri,
			payload_hash: old_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewritten_tiered, 1);
		assert_eq!(report.retention_pruned_versions, 1);

		let retained_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(10_000)))
			.unwrap()
			.expect("retained duplicate should remain visible");
		assert_eq!(manager.read(retained_anchor).await.unwrap(), b"v-old");
		assert_eq!(
			manager.read(pre_rewrite_anchor).await.unwrap(),
			b"v-old",
			"stale duplicate-HLC location should relocate to the surviving rewritten row"
		);
	}

	#[tokio::test]
	async fn sweeper_retention_rewrite_indexes_each_surviving_copy_once() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-surviving-copy-l1");
		let l2_root = fresh_dir("retention-surviving-copy-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let key = id(27);
		let duplicate_hlc = hlc_at(now_ms.saturating_sub(50_000));
		let latest = hlc_at(now_ms.saturating_sub(5_000));
		// Two byte-identical copies of one version (a replica retry or a leader
		// replay); a different payload at one HLC is a conflict, controlled by
		// `sweeper_retention_rewrite_keeps_conflicting_copies_refused_through_relocation`.
		manager
			.append(tenant(), key, duplicate_hlc, b"v-old")
			.await
			.unwrap();
		let later_copy = manager
			.append(tenant(), key, duplicate_hlc, b"v-old")
			.await
			.unwrap();
		manager
			.append(tenant(), key, latest, b"v-latest")
			.await
			.unwrap();
		let pre_rewrite_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(10_000)))
			.unwrap()
			.expect("latest duplicate before floor should be retained");
		assert_eq!(
			pre_rewrite_anchor, later_copy,
			"lookup_as_of should prefer the later duplicate at the same HLC"
		);
		assert_eq!(manager.read(pre_rewrite_anchor).await.unwrap(), b"v-old");

		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_hash = crate::hash::content::digest(&encrypted.bytes);
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri,
			payload_hash: old_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewritten_tiered, 1);
		assert_eq!(report.retention_pruned_versions, 1);

		// Each surviving copy is indexed once: exactly one location per
		// surviving version, and no two index entries name one physical row.
		let mut locations = manager
			.scan_all_versions(Locality::ZERO, Locality::MAX, hlc_at(0))
			.unwrap()
			.into_iter()
			.filter(|(_, scanned, _, _)| *scanned == key)
			.map(|(_, _, _, location)| location)
			.collect::<Vec<_>>();
		locations.sort_by_key(|location| (location.hlc, location.offset));
		assert_eq!(
			locations
				.iter()
				.map(|location| location.hlc)
				.collect::<Vec<_>>(),
			vec![duplicate_hlc, latest],
			"one copy of the duplicated version survives, indexed once: {locations:?}"
		);
		let mut values = Vec::new();
		for location in &locations {
			values.push(manager.read(*location).await.unwrap());
		}
		assert_eq!(values, vec![b"v-old".to_vec(), b"v-latest".to_vec()]);
		let retained_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(10_000)))
			.unwrap()
			.expect("retained duplicate should remain visible");
		assert_eq!(manager.read(retained_anchor).await.unwrap(), b"v-old");
		assert_eq!(
			manager.read(pre_rewrite_anchor).await.unwrap(),
			b"v-old",
			"stale duplicate-HLC location should relocate to the surviving rewritten row"
		);
	}

	#[tokio::test]
	async fn sweeper_retention_rewrite_keeps_conflicting_copies_refused_through_relocation() {
		use crate::storage::RecordLocation;
		use crate::storage::core::StorageEngine;
		use crate::storage::manager::{StaleLocationReason, stale_location};

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-conflicting-hlc-l1");
		let l2_root = fresh_dir("retention-conflicting-hlc-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (key, other) = (id(28), id(29));
		let duplicate_hlc = hlc_at(now_ms.saturating_sub(50_000));
		let latest = hlc_at(now_ms.saturating_sub(5_000));
		// Two different payloads at one (tenant, key, hlc), written below the
		// manager as a store from before the producer check holds them, plus a
		// prunable older version of another key so the rewrite moves rows.
		{
			let slab = crate::storage::core::LocalSlabStore::open(
				l1_dir.join(format!("slab_{:016x}.wnd", 0)),
				SlabId::ZERO,
				Arc::new(crate::storage::file_ops::BlockingFileOps),
				crate::storage::GroupCommitConfig::default(),
			)
			.await
			.unwrap();
			slab.append(
				tenant(),
				other,
				hlc_at(now_ms.saturating_sub(60_000)),
				b"o-old",
			)
			.await
			.unwrap();
			slab.append(tenant(), key, duplicate_hlc, b"v-old-a")
				.await
				.unwrap();
			slab.append(tenant(), key, duplicate_hlc, b"v-old-b")
				.await
				.unwrap();
			slab.append(
				tenant(),
				other,
				hlc_at(now_ms.saturating_sub(40_000)),
				b"o-new",
			)
			.await
			.unwrap();
			slab.append(tenant(), key, latest, b"v-latest")
				.await
				.unwrap();
			slab.fsync().await.unwrap();
		}
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let copies_of = |manager: &SlabManager| {
			let mut copies = manager
				.scan_all_versions(Locality::ZERO, Locality::MAX, hlc_at(0))
				.unwrap()
				.into_iter()
				.filter(|(_, scanned, _, location)| {
					*scanned == key && location.hlc == duplicate_hlc
				})
				.map(|(_, _, _, location)| location)
				.collect::<Vec<_>>();
			copies.sort_by_key(|location| location.offset);
			copies
		};
		async fn assert_conflict_refused(
			manager: &SlabManager,
			location: RecordLocation,
			what: &str,
		) {
			match manager.read(location).await {
				Ok(bytes) => panic!("{what}: a conflicted copy was served: {bytes:?}"),
				Err(error) => assert_eq!(
					stale_location(&error).map(|refusal| refusal.reason),
					Some(StaleLocationReason::ConflictingVersion),
					"{what}: {error}"
				),
			}
		}
		let before = copies_of(&manager);
		assert_eq!(
			before.len(),
			2,
			"premise: both conflicting copies were recovered"
		);
		for location in &before {
			assert_conflict_refused(&manager, *location, "before the rewrite").await;
		}

		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();
		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			committer,
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(
			report.retention_rewritten_tiered, 1,
			"premise: the rewrite ran"
		);
		assert_eq!(
			report.retention_pruned_versions, 1,
			"only the other key's older version is pruned; no conflicted copy is"
		);

		let after = copies_of(&manager);
		assert_eq!(after.len(), 2, "the rewrite dropped a conflicted copy");
		assert_ne!(after, before, "premise: the rewrite moved the copies");
		for location in &after {
			assert_conflict_refused(&manager, *location, "after the rewrite").await;
		}
		for location in &before {
			assert_conflict_refused(&manager, *location, "a pre-rewrite location").await;
		}
		let newest = manager.lookup(tenant(), key).unwrap().unwrap();
		assert_eq!(manager.read(newest).await.unwrap(), b"v-latest");
		let survivors = manager
			.scan_all_versions(Locality::ZERO, Locality::MAX, hlc_at(0))
			.unwrap()
			.into_iter()
			.filter(|(_, scanned, _, _)| *scanned == other)
			.map(|(_, _, _, location)| location)
			.collect::<Vec<_>>();
		assert_eq!(
			survivors.len(),
			1,
			"the other key keeps exactly its newest version"
		);
		assert_eq!(manager.read(survivors[0]).await.unwrap(), b"o-new");
	}

	/// A manager over a fresh L1 directory and a local object store.
	async fn open_retention_manager(
		tag: &str,
	) -> (
		[crate::test_support::TestDir; 2],
		Arc<SlabManager>,
		Arc<dyn ObjectStore>,
	) {
		let l1_dir = fresh_dir(&format!("{tag}-l1"));
		let l2_root = fresh_dir(&format!("{tag}-l2"));
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		([l1_dir, l2_root], manager, l2)
	}

	/// Seal the active slab and publish it as a tiered slab, on the node and in
	/// the state machine.
	async fn tier_active_slab(
		manager: &Arc<SlabManager>,
		l2: &Arc<dyn ObjectStore>,
		sm: &WendelonStateMachine,
		object: &str,
	) -> TieredSlabPointer {
		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let uri = l2.put(object, &encrypted.bytes).await.unwrap();
		let pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: uri,
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(pointer.clone()).await.unwrap();
		seed_state_machine_pointer(sm, &pointer).await;
		pointer
	}

	/// A sweeper whose committer applies through the manager's tier observer.
	fn retention_sweeper(
		manager: &Arc<SlabManager>,
		l2: &Arc<dyn ObjectStore>,
		sm: &WendelonStateMachine,
		now_ms: u64,
	) -> SlabSweeper {
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm.clone(),
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_mvcc_retention_window(Some(Duration::from_secs(15)))
	}

	async fn l2_object_names(l2: &Arc<dyn ObjectStore>) -> std::collections::BTreeSet<String> {
		l2.list("")
			.await
			.unwrap()
			.into_iter()
			.map(|object| object.key)
			.collect()
	}

	#[tokio::test]
	async fn a_rewrite_moving_another_key_into_a_pruned_location_is_refused_before_upload() {
		use crate::storage::core::StorageEngine;
		use crate::storage::manager::{StaleLocation, StaleLocationReason, stale_location};

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let (_dirs, manager, l2) = open_retention_manager("retention-slot-reuse").await;
		let (a, b) = (id(0x9D01), id(0x9D02));
		let batch_hlc = hlc_at(now_ms.saturating_sub(50_000));
		// a and b were written in one batch: one HLC, equal row sizes. a's newer
		// version makes the batch's a row prunable; b's row is b's only version.
		manager
			.append(tenant(), a, batch_hlc, b"a-old")
			.await
			.unwrap();
		manager
			.append(tenant(), b, batch_hlc, b"b-old")
			.await
			.unwrap();
		manager
			.append(tenant(), a, hlc_at(now_ms.saturating_sub(40_000)), b"a-new")
			.await
			.unwrap();
		let held_a = manager
			.lookup_as_of(tenant(), a, hlc_at(now_ms.saturating_sub(45_000)))
			.unwrap()
			.unwrap();
		let held_b = manager.lookup(tenant(), b).unwrap().unwrap();
		assert_eq!(
			(held_a.hlc, held_a.length, held_a.record_row_index),
			(held_b.hlc, held_b.length, held_b.record_row_index),
			"premise: one HLC, one row size"
		);
		let sm = WendelonStateMachine::new();
		let pointer = tier_active_slab(&manager, &l2, &sm, "tier/retention-slot-reuse.wnd").await;
		let sweeper = retention_sweeper(&manager, &l2, &sm, now_ms);
		let objects = l2_object_names(&l2).await;

		// Pruning a's batch row moves b's row into its old physical location;
		// the rewrite is refused in preparation, before any upload or commit.
		let refused = sweeper
			.rewrite_one_tiered_slab_for_retention(
				&pointer,
				now_ms,
				hlc_at(now_ms.saturating_sub(15_000)),
			)
			.await
			.expect_err("a rewrite whose old and new meanings of one location differ");
		assert_eq!(
			stale_location(&refused),
			Some(StaleLocation {
				slab_id: pointer.slab_id,
				reason: StaleLocationReason::LayoutCollision,
			}),
			"{refused}"
		);
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewritten_tiered, 0);
		assert_eq!(report.retention_rewrite_failures, 1);
		assert_eq!(
			l2_object_names(&l2).await,
			objects,
			"a refused rewrite uploads nothing"
		);
		assert_eq!(
			sweeper.sm_tier_pointer(pointer.slab_id).await,
			Some(pointer.clone()),
			"a refused rewrite commits nothing"
		);
		assert_eq!(
			manager.tier_pointer(pointer.slab_id).await,
			Some(pointer.clone())
		);
		assert_eq!(manager.read(held_a).await.unwrap(), b"a-old");
		assert_eq!(manager.read(held_b).await.unwrap(), b"b-old");
		let latest_a = manager.lookup(tenant(), a).unwrap().unwrap();
		assert_eq!(manager.read(latest_a).await.unwrap(), b"a-new");
		let latest_b = manager.lookup(tenant(), b).unwrap().unwrap();
		assert_eq!(manager.read(latest_b).await.unwrap(), b"b-old");
	}

	#[tokio::test]
	async fn a_retention_rewrite_of_conflicting_copies_publishes_once_and_never_repeats() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-conflicting-once-l1");
		let l2_root = fresh_dir("retention-conflicting-once-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (key, other) = (id(28), id(29));
		let duplicate_hlc = hlc_at(now_ms.saturating_sub(50_000));
		// The conflicted-copies premise, below the manager: two payloads at one
		// (tenant, key, hlc), and a prunable older version of another key that
		// moves the copies, so one copy lands on the other's old location.
		{
			let slab = crate::storage::core::LocalSlabStore::open(
				l1_dir.join(format!("slab_{:016x}.wnd", 0)),
				SlabId::ZERO,
				Arc::new(crate::storage::file_ops::BlockingFileOps),
				crate::storage::GroupCommitConfig::default(),
			)
			.await
			.unwrap();
			slab.append(
				tenant(),
				other,
				hlc_at(now_ms.saturating_sub(60_000)),
				b"o-old",
			)
			.await
			.unwrap();
			slab.append(tenant(), key, duplicate_hlc, b"v-old-a")
				.await
				.unwrap();
			slab.append(tenant(), key, duplicate_hlc, b"v-old-b")
				.await
				.unwrap();
			slab.append(
				tenant(),
				other,
				hlc_at(now_ms.saturating_sub(40_000)),
				b"o-new",
			)
			.await
			.unwrap();
			slab.fsync().await.unwrap();
		}
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let sm = WendelonStateMachine::new();
		let pointer =
			tier_active_slab(&manager, &l2, &sm, "tier/retention-conflicting-once.wnd").await;
		let sweeper = retention_sweeper(&manager, &l2, &sm, now_ms);
		let mut uploaded = None;
		for sweep in 1..=3 {
			sweeper.sweep_once().await.unwrap();
			assert_eq!(
				sweeper.sm_tier_pointer(pointer.slab_id).await,
				manager.tier_pointer(pointer.slab_id).await,
				"sweep {sweep}: the node publishes every pointer the cluster committed"
			);
			let objects = l2_object_names(&l2).await;
			match &uploaded {
				None => uploaded = Some(objects),
				Some(first) => assert_eq!(
					&objects, first,
					"sweep {sweep}: the rewrite is published once and never uploaded again"
				),
			}
		}
		assert_ne!(
			manager.tier_pointer(pointer.slab_id).await,
			Some(pointer),
			"premise: the rewrite was published"
		);
	}

	#[tokio::test]
	async fn a_committed_pointer_the_node_cannot_apply_is_never_reproposed() {
		use crate::storage::core::StorageEngine;
		use crate::storage::manager::{StaleLocation, StaleLocationReason, stale_location};

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let (_dirs, manager, l2) = open_retention_manager("retention-unapplied").await;
		let key = id(0x9E01);
		manager
			.append(
				tenant(),
				key,
				hlc_at(now_ms.saturating_sub(50_000)),
				b"k-old",
			)
			.await
			.unwrap();
		manager
			.append(
				tenant(),
				key,
				hlc_at(now_ms.saturating_sub(40_000)),
				b"k-new",
			)
			.await
			.unwrap();
		let sm = WendelonStateMachine::new();
		let pointer = tier_active_slab(&manager, &l2, &sm, "tier/retention-unapplied.wnd").await;
		// The cluster committed a generation this node cannot apply: its object
		// is not in this node's store.
		let unapplied = TieredSlabPointer {
			l2_uri: pointer
				.l2_uri
				.replace("retention-unapplied.wnd", "unapplied-missing.wnd"),
			payload_hash: [0x5A; 32],
			..pointer.clone()
		};
		seed_state_machine_pointer(&sm, &unapplied).await;
		let sweeper = retention_sweeper(&manager, &l2, &sm, now_ms);
		let objects = l2_object_names(&l2).await;

		// A proposer never plans from a generation the cluster has moved past:
		// it refuses typed and uploads nothing.
		let refused = sweeper
			.rewrite_one_tiered_slab_for_retention(
				&pointer,
				now_ms,
				hlc_at(now_ms.saturating_sub(15_000)),
			)
			.await
			.expect_err("a proposal over a committed pointer this node has not applied");
		assert_eq!(
			stale_location(&refused),
			Some(StaleLocation {
				slab_id: pointer.slab_id,
				reason: StaleLocationReason::Fenced,
			}),
			"{refused}"
		);
		for sweep in 1..=2 {
			let refused = sweeper.sweep_once().await.unwrap_err();
			assert_eq!(
				stale_location(&refused).map(|reason| reason.reason),
				Some(StaleLocationReason::Fenced)
			);
			assert_eq!(
				l2_object_names(&l2).await,
				objects,
				"sweep {sweep}: nothing is uploaded over an unapplied committed pointer"
			);
			assert_eq!(
				sweeper.sm_tier_pointer(pointer.slab_id).await,
				Some(unapplied.clone()),
				"sweep {sweep}: nothing is committed over it"
			);
			assert_eq!(
				manager.tier_pointer(pointer.slab_id).await,
				Some(pointer.clone()),
				"sweep {sweep}: the node keeps its last published generation"
			);
		}
		let refused = manager.lookup(tenant(), key).unwrap_err();
		assert_eq!(
			stale_location(&refused).map(|reason| reason.reason),
			Some(StaleLocationReason::Fenced)
		);
	}

	#[tokio::test]
	async fn sweeper_preserves_uploaded_rewrite_object_when_commit_result_is_ambiguous() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-ambiguous-commit-l1");
		let l2_root = fresh_dir("retention-ambiguous-commit-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let key = id(33);
		manager
			.append(
				tenant(),
				key,
				hlc_at(now_ms.saturating_sub(50_000)),
				b"v-old",
			)
			.await
			.unwrap();
		manager
			.append(
				tenant(),
				key,
				hlc_at(now_ms.saturating_sub(20_000)),
				b"v-anchor",
			)
			.await
			.unwrap();
		manager
			.append(
				tenant(),
				key,
				hlc_at(now_ms.saturating_sub(5_000)),
				b"v-latest",
			)
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let old_hash = crate::hash::content::digest(&encrypted.bytes);
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		let old_pointer = TieredSlabPointer {
			slab_id: sealed_id,
			l2_uri: old_uri.clone(),
			payload_hash: old_hash,
			is_encrypted: true,
			tenant_id: Some(tenant()),
			tenant_kek_id: Some(
				manager
					.encrypted_tiered_footer_summary(&encrypted.bytes)
					.unwrap()
					.unwrap()
					.tenant_kek_id,
			),
			stats_format_version: encrypted
				.stats_locator
				.map(|locator| locator.stats_format_version),
			stats_block_len: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_len),
			stats_block_hash128: encrypted
				.stats_locator
				.map(|locator| locator.stats_block_hash128),
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old_pointer.clone()).await.unwrap();

		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old_pointer).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let ambiguous_committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::commit_applied_then_err(
				sm.clone(),
				observer.clone(),
				std::io::ErrorKind::TimedOut,
			));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			ambiguous_committer,
			sm.clone(),
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));

		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewrite_failures, 1);
		let committed_uri = sm
			.state_snapshot()
			.await
			.tiered_slabs
			.get(&sealed_id)
			.expect("state machine should still commit the rewritten pointer")
			.l2_uri
			.clone();
		assert_ne!(committed_uri, old_uri);
		assert!(
			l2.get(&committed_uri).await.is_ok(),
			"ambiguous commit errors must not delete the object the state machine committed"
		);
		assert_eq!(
			manager
				.tier_pointer(sealed_id)
				.await
				.expect("local manager still points at the pre-reconcile URI")
				.l2_uri,
			old_uri
		);

		let converging_sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer)),
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.with_clock(Arc::new(FixedClock { now_ms }))
		.with_mvcc_retention_window(Some(Duration::from_secs(15)));
		let report = converging_sweeper.sweep_once().await.unwrap();
		assert_eq!(report.retention_rewrite_failures, 0);
		assert_eq!(
			manager
				.tier_pointer(sealed_id)
				.await
				.expect("next sweep should reconcile local state to the committed pointer")
				.l2_uri,
			committed_uri
		);
	}

	#[tokio::test]
	async fn sweeper_retries_pending_retired_cleanup_candidates() {
		use crate::storage::core::StorageEngine;

		let l1_dir = fresh_dir("retired-cleanup-retry-l1");
		let l2_root = fresh_dir("retired-cleanup-retry-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io,
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		let key = id(41);
		manager
			.append(tenant(), key, hlc_at(10), b"v-old")
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		let raw = manager.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = manager.encrypt_tiered_object_blob(sealed_id, &raw).unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let old_uri = l2
			.put(&format!("tier/slab_{sealed_id:016x}.wnd"), &encrypted.bytes)
			.await
			.unwrap();
		manager
			.apply_tier_slab(TieredSlabPointer {
				slab_id: sealed_id,
				l2_uri: old_uri.clone(),
				payload_hash,
				is_encrypted: true,
				tenant_id: Some(tenant()),
				tenant_kek_id: Some(
					manager
						.encrypted_tiered_footer_summary(&encrypted.bytes)
						.unwrap()
						.unwrap()
						.tenant_kek_id,
				),
				stats_format_version: encrypted
					.stats_locator
					.map(|locator| locator.stats_format_version),
				stats_block_len: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_len),
				stats_block_hash128: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_hash128),
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			})
			.await
			.unwrap();
		assert!(
			manager
				.retire_tiered_slab_committed(sealed_id, None)
				.await
				.unwrap()
		);
		assert!(
			manager.tier_pointer(sealed_id).await.is_none(),
			"local manager already removed the tier pointer"
		);
		assert_eq!(
			manager.list_retired_tiered_cleanup_candidates().await.len(),
			1
		);
		assert!(
			l2.get(&old_uri).await.is_ok(),
			"pending cleanup candidate should still have its old L2 object before retry"
		);

		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer)),
			sm,
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);
		let report = sweeper.sweep_once().await.unwrap();
		assert_eq!(report.failed, 0);
		assert!(
			manager
				.list_retired_tiered_cleanup_candidates()
				.await
				.is_empty(),
			"next sweep should drain the pending retired cleanup marker"
		);
		assert!(
			l2.get(&old_uri).await.is_ok(),
			"cleanup retry retains the stale retired L2 object until owner-scoped retirement authority"
		);
	}

	#[tokio::test]
	async fn a_retried_first_tier_upload_never_overwrites_a_committed_object() {
		use crate::storage::core::StorageEngine;

		let l1_dir = fresh_dir("first-tier-retry-l1");
		let l2_root = fresh_dir("first-tier-retry-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		manager
			.append(tenant(), id(41), hlc(), b"first-tier")
			.await
			.unwrap();
		let sealed_id = manager.force_seal_active_for_test().await;
		let config = || SweeperConfig {
			scan_interval: Duration::from_millis(10),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: "tier/".into(),
			max_plaintext_rewrites_per_pass: 1,
			max_dek_rewraps_per_pass: 1,
		};
		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		// The first attempt's commit is applied by the state machine, but the
		// sweeper sees an error, so the local slab stays sealed.
		let ambiguous: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::commit_applied_then_err(
				sm.clone(),
				observer.clone(),
				std::io::ErrorKind::TimedOut,
			));
		let _ = SlabSweeper::new(manager.clone(), l2.clone(), ambiguous, sm.clone(), config())
			.sweep_once()
			.await;
		let committed = sm
			.state_snapshot()
			.await
			.tiered_slabs
			.get(&sealed_id)
			.expect("premise: the first attempt's pointer is committed")
			.clone();
		assert!(
			manager.tier_pointer(sealed_id).await.is_none(),
			"premise: the local slab is still sealed after the ambiguous commit"
		);
		// A retry of the first tiering (the next pass).
		let retry: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer.clone()));
		let _ = SlabSweeper::new(manager.clone(), l2.clone(), retry, sm.clone(), config())
			.sweep_once()
			.await;
		let object = l2
			.get(&committed.l2_uri)
			.await
			.expect("the committed object must still exist");
		assert_eq!(
			crate::hash::content::digest(&object),
			committed.payload_hash,
			"a retried first-tier upload overwrote the object a committed pointer names"
		);
	}

	#[tokio::test]
	async fn the_sweeper_never_tiers_a_counter_bearing_slab() {
		use crate::schema::{HybridTimestamp, NodeId};
		use crate::storage::core::StorageEngine;

		let l1_dir = fresh_dir("sweeper-counter-slab-l1");
		let l2_root = fresh_dir("sweeper-counter-slab-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let manager = SlabManager::initialize_new_store(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: l1_dir.to_path_buf(),
			max_slab_bytes: 1 << 20,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(l2.clone()),
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: crate::storage::GroupCommitConfig::default(),
			crypto: test_crypto(),
		})
		.await
		.unwrap();
		// Two committed counter frames from two origins at one op id.
		let key = id(61);
		let op = HybridTimestamp {
			physical_ms: 11,
			logical: 0,
			node_id: 0,
		};
		let mut locations = Vec::new();
		for (value, origin) in [(5, 1), (9, 2)] {
			let origin = NodeId {
				id: origin,
				_reserved: 0,
			};
			let location = manager
				.counter_append_tentative(tenant(), key, op, value, origin)
				.await
				.unwrap();
			manager
				.counter_append_commit_marker(tenant(), key, op, location.offset, origin)
				.await
				.unwrap();
			manager.counter_commit_published(tenant(), key, location, origin);
			locations.push(location);
		}
		let mut before = Vec::new();
		for location in &locations {
			let frame = manager.read(*location).await.unwrap();
			before.push(crate::storage::counter::decode_origin_frame(&frame).unwrap());
		}
		let sealed_id = manager.force_seal_active_for_test().await;
		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer: Arc<dyn TierCommitter> =
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let report = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm.clone(),
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "tier/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		)
		.sweep_once()
		.await;
		if let Ok(report) = &report {
			assert_eq!(
				report.tiered, 0,
				"the sweeper tiered a counter-bearing slab"
			);
		}
		assert!(
			manager.tier_pointer(sealed_id).await.is_none()
				&& !sm
					.state_snapshot()
					.await
					.tiered_slabs
					.contains_key(&sealed_id),
			"a counter-bearing slab got a tier pointer through the sweeper"
		);
		assert!(
			l1_dir.join(format!("slab_{:016x}.wnd", sealed_id)).exists(),
			"the counter-bearing slab's local file must stay"
		);
		let mut after = Vec::new();
		for location in &locations {
			let frame = manager.read(*location).await.unwrap();
			after.push(crate::storage::counter::decode_origin_frame(&frame).unwrap());
		}
		assert_eq!(after, before, "counter values and origins stay exact");
	}
	struct B1CountingStore {
		inner: Arc<dyn ObjectStore>,
		puts: std::sync::atomic::AtomicUsize,
	}
	#[async_trait]
	impl ObjectStore for B1CountingStore {
		async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
			self.puts.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
			self.inner.put(key, bytes).await
		}
		fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
			self.inner.planned_upload_uri(key)
		}
		async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
			self.inner.head(uri).await
		}
		async fn get_range(
			&self,
			uri: &str,
			range: std::ops::Range<u64>,
		) -> std::io::Result<bytes::Bytes> {
			self.inner.get_range(uri, range).await
		}
		async fn get_bytes(&self, uri: &str) -> std::io::Result<bytes::Bytes> {
			self.inner.get_bytes(uri).await
		}
		async fn list(
			&self,
			prefix: &str,
		) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
			self.inner.list(prefix).await
		}
		async fn delete(&self, uri: &str) -> std::io::Result<()> {
			self.inner.delete(uri).await
		}
	}

	async fn b1_real_follower(
		dir: &std::path::Path,
	) -> (
		WendelonRaft,
		crate::consensus::WendelonLogStore,
		WendelonStateMachine,
	) {
		use openraft::raft::AppendEntriesRequest;
		use openraft::{
			BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote,
		};
		use std::collections::{BTreeMap, BTreeSet};
		let store =
			crate::consensus::WendelonLogStore::open_for_retention_test(dir, 64 * 4096, None)
				.await
				.unwrap();
		let sm = store.recovered_state_machine().await.unwrap();
		let network = crate::consensus::WendelonRaftNetworkFactory::new(
			Arc::new(crate::node::NoDialTransportProvider),
			crate::network::pool::BufPool::new(8, 1 << 20),
		);
		let config = Arc::new(
			crate::consensus::retention::ControlRetention::default()
				.openraft_config("wendelon")
				.validate()
				.unwrap(),
		);
		let raft = WendelonRaft::new(1, config, network, store.clone(), sm.clone())
			.await
			.unwrap();
		let membership = Membership::new(
			vec![BTreeSet::from([1_u32, 2_u32])],
			BTreeMap::from([
				(
					1_u32,
					BasicNode {
						addr: "127.0.0.1:0".into(),
					},
				),
				(
					2_u32,
					BasicNode {
						addr: "127.0.0.1:0".into(),
					},
				),
			]),
		);
		let log_id = LogId::new(CommittedLeaderId::new(1, 2), 0);
		raft.append_entries(AppendEntriesRequest {
			vote: Vote::new_committed(1, 2),
			prev_log_id: None,
			entries: vec![Entry {
				log_id,
				payload: EntryPayload::Membership(membership),
			}],
			leader_commit: Some(log_id),
		})
		.await
		.unwrap();
		raft.wait(Some(Duration::from_secs(60)))
			.current_leader(2, "remote leader established")
			.await
			.unwrap();
		assert_ne!(raft.metrics().borrow().state, openraft::ServerState::Leader);
		// Node 1 cannot gain a quorum through NoDialTransportProvider, even if
		// an election timer fires. No timeout or election configuration is widened.
		(raft, store, sm)
	}

	fn b1_config() -> SweeperConfig {
		SweeperConfig {
			scan_interval: Duration::from_millis(10),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: "b1/".into(),
			max_plaintext_rewrites_per_pass: 0,
			max_dek_rewraps_per_pass: 0,
		}
	}

	#[tokio::test]
	async fn b1_real_follower_does_not_upload_or_fence_its_sealed_rows() {
		use crate::storage::core::StorageEngine;
		let wal = fresh_dir("b1-follower-wal");
		let l2_root = fresh_dir("b1-follower-l2");
		let store = Arc::new(B1CountingStore {
			inner: Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap()),
			puts: Default::default(),
		});
		let l2: Arc<dyn ObjectStore> = store.clone();
		let (_l1, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		assert!(!manager.list_sealed_slabs().await.is_empty());
		let (raft, _log, sm) = b1_real_follower(&wal).await;
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			Arc::new(RaftCommitter::new(raft.clone())),
			sm,
			b1_config(),
		);
		let outcome = sweeper.sweep_once().await;
		let readable = manager.lookup(tenant(), id(1));
		let puts = store.puts.load(std::sync::atomic::Ordering::Acquire);
		raft.shutdown().await.unwrap();
		assert_eq!(
			puts, 0,
			"B1 a real follower must skip uploads before the object-store put"
		);
		assert!(
			outcome.is_ok(),
			"nonleader skip must not poison a pass: {outcome:?}"
		);
		assert!(
			readable.unwrap().is_some(),
			"a skipped nonleader pass cannot fence an acknowledged row"
		);
	}

	#[tokio::test]
	async fn b1_real_follower_commit_is_a_typed_retryable_refusal() {
		let wal = fresh_dir("b1-follower-error");
		let (raft, _log, _sm) = b1_real_follower(&wal).await;
		let before = raft.metrics().borrow().last_log_index;
		let tier = TieredSlab {
			slab_id: SlabId::new(71),
			l2_uri: "b1/unsubmitted".into(),
			payload_hash: [7; 32],
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: crate::schema::TombstoneMetadata::None,
			supersession_metadata: crate::schema::SupersessionMetadata::None,
		};
		let error = tokio::time::timeout(
			Duration::from_secs(60),
			RaftCommitter::new(raft.clone())
				.commit_tier_slab(crate::storage::TierProposal::unreserved_for_test(tier)),
		)
		.await
		.unwrap()
		.unwrap_err();
		let after = raft.metrics().borrow().last_log_index;
		raft.shutdown().await.unwrap();
		assert_eq!(
			before, after,
			"the real follower did not append this command"
		);
		assert_eq!(
			error.kind(),
			std::io::ErrorKind::WouldBlock,
			"B1 ForwardToLeader must remain a distinguishable retryable refusal"
		);
	}

	#[tokio::test]
	async fn b1_untyped_would_block_is_still_an_ambiguous_upload_outcome() {
		use crate::storage::core::StorageEngine;
		let l2_root = fresh_dir("b1-unknown-wouldblock-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap());
		let (_dir, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let original = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let sm = WendelonStateMachine::new();
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer = Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		*committer.persistent_err.lock().await = Some(std::io::ErrorKind::WouldBlock);
		let sweeper = SlabSweeper::new(manager.clone(), l2.clone(), committer, sm, b1_config());
		let _ = sweeper.sweep_once().await;
		assert!(
			!l2.list("b1/").await.unwrap().is_empty(),
			"unknown outcome must retain uploaded bytes"
		);
		let error = sweeper
			.refuse_proposal_over_unpublished_commit(original.slab_id, None)
			.await
			.expect_err("unknown proposal outcome must keep the maintenance fence");
		assert_eq!(
			crate::storage::manager::stale_location(&error)
				.unwrap()
				.reason,
			StaleLocationReason::Fenced
		);
		assert!(
			manager.lookup(tenant(), id(1)).unwrap().is_some(),
			"the maintenance fence must not be confused with a point-read refusal"
		);
	}
	// Keep the real proposal path but deliberately provide no leadership hint.
	// This models a proposal rejected after an earlier permissive hint.
	struct B1UnhintedRaftCommitter(RaftCommitter);
	#[async_trait]
	impl TierCommitter for B1UnhintedRaftCommitter {
		async fn commit_tier_slab(
			&self,
			proposal: crate::storage::TierProposal,
		) -> std::io::Result<()> {
			self.0.commit_tier_slab(proposal).await
		}
		async fn retire_tiered_slab(&self, slab: SlabId) -> std::io::Result<()> {
			self.0.retire_tiered_slab(slab).await
		}
	}

	#[tokio::test]
	async fn b1_definitely_rejected_uploaded_candidate_is_not_left_unresolved() {
		let wal = fresh_dir("b1-rejected-upload-wal");
		let l2_root = fresh_dir("b1-rejected-upload-l2");
		let store = Arc::new(B1CountingStore {
			inner: Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap()),
			puts: Default::default(),
		});
		let l2: Arc<dyn ObjectStore> = store.clone();
		let (_l1, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let sealed = manager.list_sealed_slabs().await;
		let (raft, _log, sm) = b1_real_follower(&wal).await;
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			Arc::new(B1UnhintedRaftCommitter(RaftCommitter::new(raft.clone()))),
			sm,
			b1_config(),
		);
		let _ = sweeper.sweep_once().await;
		let puts = store.puts.load(std::sync::atomic::Ordering::Acquire);
		let remaining = l2.list("b1/").await.unwrap();
		let pending = sealed
			.iter()
			.filter(|info| manager.refuse_unresolved_cleanup(info.slab_id).is_err())
			.count();
		raft.shutdown().await.unwrap();
		assert_eq!(
			puts,
			sealed.len(),
			"every candidate actually reached the upload/proposal cut"
		);
		assert!(
			remaining.is_empty(),
			"B1 an actual ForwardToLeader refusal must not retain a fresh unpublished upload"
		);
		assert_eq!(
			pending, 0,
			"a definite proposal refusal must not create a permanent maintenance fence"
		);
	}
	#[tokio::test]
	async fn b1n_post_invocation_forward_retains_upload() {
		let wal = fresh_dir("b1n-post-invocation-wal");
		let l2_root = fresh_dir("b1n-post-invocation-l2");
		let store = Arc::new(B1CountingStore {
			inner: Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap()),
			puts: Default::default(),
		});
		let l2: Arc<dyn ObjectStore> = store.clone();
		let (_l1, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let sealed = manager.list_sealed_slabs().await;
		let (raft, _log, sm) = b1_real_follower(&wal).await;
		let mut production = RaftCommitter::new(raft.clone());
		// Models the earlier role check passing, followed by role loss. The
		// real client_write still executes and returns its own Forward error.
		production.submit_without_role_check = true;
		assert!(production.submit_without_role_check);
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			Arc::new(B1UnhintedRaftCommitter(production)),
			sm,
			b1_config(),
		);
		let _ = sweeper.sweep_once().await;
		let puts = store.puts.load(std::sync::atomic::Ordering::Acquire);
		let remaining = l2.list("b1/").await.unwrap();
		let pending = sealed
			.iter()
			.filter(|info| manager.refuse_unresolved_cleanup(info.slab_id).is_err())
			.count();
		raft.shutdown().await.unwrap();
		assert_eq!(
			puts,
			sealed.len(),
			"every candidate actually reached the upload/proposal cut"
		);
		assert!(
			!remaining.is_empty(),
			"B1n post-invocation Forward is ambiguous: retain the upload"
		);
		assert!(
			pending > 0,
			"post-invocation ambiguity must retain its cleanup fence"
		);
	}
	#[tokio::test]
	async fn b1n_follower_pre_invocation_refusal_is_definite() {
		let wal = fresh_dir("b1n-pre-invocation");
		let (raft, _log, _sm) = b1_real_follower(&wal).await;
		let error = RaftCommitter::new(raft.clone())
			.retire_tiered_slab(SlabId::new(71))
			.await
			.unwrap_err();
		raft.shutdown().await.unwrap();
		assert!(
			error
				.get_ref()
				.and_then(|e| e.downcast_ref::<TierCommitNotSubmitted>())
				.is_some(),
			"refusal before invoking client_write must be definite"
		);
	}
	// Faults are armed after the old object is installed. They affect only the
	// replacement before its first possible proposal, never the live old URI.
	struct B2FaultStore {
		inner: Arc<dyn ObjectStore>,
		fault: std::sync::atomic::AtomicU8,
		failed_deletes: std::sync::atomic::AtomicUsize,
	}
	#[async_trait]
	impl ObjectStore for B2FaultStore {
		async fn put(&self, key: &str, bytes: &[u8]) -> std::io::Result<String> {
			let uri = self.inner.put(key, bytes).await?;
			if self.fault.load(std::sync::atomic::Ordering::Acquire) == 1 {
				return Err(std::io::Error::new(
					std::io::ErrorKind::ConnectionReset,
					"B2 put failed after storing replacement",
				));
			}
			Ok(uri)
		}
		fn planned_upload_uri(&self, key: &str) -> std::io::Result<String> {
			self.inner.planned_upload_uri(key)
		}
		async fn head(&self, uri: &str) -> std::io::Result<crate::storage::l2::ObjectMetadata> {
			self.inner.head(uri).await
		}
		async fn get_range(
			&self,
			uri: &str,
			range: std::ops::Range<u64>,
		) -> std::io::Result<bytes::Bytes> {
			self.inner.get_range(uri, range).await
		}
		async fn get_bytes(&self, uri: &str) -> std::io::Result<bytes::Bytes> {
			self.inner.get_bytes(uri).await
		}
		async fn get(&self, uri: &str) -> std::io::Result<Vec<u8>> {
			if uri.ends_with("/old.wnd") {
				return self.inner.get(uri).await;
			}
			match self.fault.load(std::sync::atomic::Ordering::Acquire) {
				2 => Err(std::io::Error::new(
					std::io::ErrorKind::ConnectionReset,
					"B2 replacement verification read failed",
				)),
				3 => Ok(b"B2 corrupted verification response".to_vec()),
				_ => self.inner.get(uri).await,
			}
		}
		async fn list(
			&self,
			prefix: &str,
		) -> std::io::Result<Vec<crate::storage::l2::ObjectMetadata>> {
			self.inner.list(prefix).await
		}
		async fn delete(&self, uri: &str) -> std::io::Result<()> {
			if self.fault.load(std::sync::atomic::Ordering::Acquire) == 3 {
				self.failed_deletes
					.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
				return Err(std::io::Error::new(
					std::io::ErrorKind::PermissionDenied,
					"B2 temporary delete refusal",
				));
			}
			self.inner.delete(uri).await
		}
	}
	async fn b2_preproposal_fault_retries(fault: u8) {
		use crate::storage::core::StorageEngine;
		let l2_root = fresh_dir("b2-preproposal-l2");
		let store = Arc::new(B2FaultStore {
			inner: Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap()),
			fault: Default::default(),
			failed_deletes: Default::default(),
		});
		let l2: Arc<dyn ObjectStore> = store.clone();
		let (_l1, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let old_uri = l2.put("b2/old.wnd", &raw).await.unwrap();
		let old = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: old_uri.clone(),
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let committer = Arc::new(DirectApplyCommitter::ok(sm.clone(), observer));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2.clone(),
			committer,
			sm,
			SweeperConfig {
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "b2/".into(),
				..SweeperConfig::default()
			},
		);
		store
			.fault
			.store(fault, std::sync::atomic::Ordering::Release);
		let error = sweeper
			.rewrite_one_plaintext_tiered_slab(&old)
			.await
			.expect_err("armed preproposal fault must execute");
		assert_eq!(
			error.kind(),
			if fault == 3 {
				std::io::ErrorKind::InvalidData
			} else {
				std::io::ErrorKind::ConnectionReset
			}
		);
		if fault == 3 {
			assert!(
				store
					.failed_deletes
					.load(std::sync::atomic::Ordering::Acquire)
					> 0,
				"failed delete premise"
			);
		}
		assert_eq!(manager.tier_pointer(loc.slab_id).await, Some(old.clone()));
		assert_eq!(manager.read(loc).await.unwrap(), b"hello-1");
		assert_eq!(
			store.inner.get(&old_uri).await.unwrap(),
			raw,
			"old committed bytes are retained"
		);
		store.fault.store(0, std::sync::atomic::Ordering::Release);
		let _ = manager
			.reconcile_table_drop_l2_cleanup_markers(&*l2, usize::MAX)
			.await;
		let retry = sweeper.rewrite_one_plaintext_tiered_slab(&old).await;
		assert!(
			retry.is_ok(),
			"B2 preproposal failure must allow a later replacement: {retry:?}"
		);
		assert_ne!(
			manager.tier_pointer(loc.slab_id).await.unwrap().l2_uri,
			old_uri
		);
		assert_eq!(manager.read(loc).await.unwrap(), b"hello-1");
	}
	#[tokio::test]
	async fn b2_put_failure_before_proposal_can_retry() {
		b2_preproposal_fault_retries(1).await;
	}
	#[tokio::test]
	async fn b2_verification_read_failure_before_proposal_can_retry() {
		b2_preproposal_fault_retries(2).await;
	}
	#[tokio::test]
	async fn b2_hash_mismatch_with_failed_delete_can_retry() {
		b2_preproposal_fault_retries(3).await;
	}
	async fn b2_failed_delete_fixture() -> (
		[crate::test_support::TestDir; 2],
		Arc<SlabManager>,
		Arc<B2FaultStore>,
		TieredSlabPointer,
	) {
		use crate::storage::core::StorageEngine;
		let l2_root = fresh_dir("b2-catchup-l2");
		let store = Arc::new(B2FaultStore {
			inner: Arc::new(LocalFsObjectStore::new(&*l2_root).unwrap()),
			fault: Default::default(),
			failed_deletes: Default::default(),
		});
		let l2: Arc<dyn ObjectStore> = store.clone();
		let (l1, manager) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		let raw = manager.read_sealed_raw(loc.slab_id).await.unwrap();
		let old_uri = l2.put("b2/old.wnd", &raw).await.unwrap();
		let old = TieredSlabPointer {
			slab_id: loc.slab_id,
			l2_uri: old_uri,
			payload_hash: crate::hash::content::digest(&raw),
			is_encrypted: false,
			tenant_id: None,
			tenant_kek_id: None,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			tombstone_metadata: TombstoneMetadata::None,
			supersession_metadata: SupersessionMetadata::None,
		};
		manager.apply_tier_slab(old.clone()).await.unwrap();
		let sm = WendelonStateMachine::new();
		seed_state_machine_pointer(&sm, &old).await;
		let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
		let sweeper = SlabSweeper::new(
			manager.clone(),
			l2,
			Arc::new(DirectApplyCommitter::ok(sm.clone(), observer)),
			sm,
			SweeperConfig {
				max_upload_bytes_per_sec: None,
				key_prefix: "b2/".into(),
				..SweeperConfig::default()
			},
		);
		store.fault.store(3, std::sync::atomic::Ordering::Release);
		let error = sweeper
			.rewrite_one_plaintext_tiered_slab(&old)
			.await
			.unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
		assert!(
			store
				.failed_deletes
				.load(std::sync::atomic::Ordering::Acquire)
				> 0
		);
		assert!(manager.refuse_unresolved_cleanup(old.slab_id).is_err());
		assert_eq!(manager.retained_cleanup_test_accounting().0, 2);
		drop(sweeper);
		([l1, l2_root], manager, store, old)
	}
	async fn b2_assert_resolver_clears(
		manager: &SlabManager,
		store: &B2FaultStore,
		old: &TieredSlabPointer,
	) {
		use crate::storage::core::StorageEngine;
		store.fault.store(0, std::sync::atomic::Ordering::Release);
		manager
			.reconcile_table_drop_l2_cleanup_markers(store, usize::MAX)
			.await
			.unwrap();
		assert!(
			manager.refuse_unresolved_cleanup(old.slab_id).is_ok(),
			"unsubmitted proof must eventually clear the local fence"
		);
		assert_eq!(manager.retained_cleanup_test_accounting(), (0, 0, 0));
		assert_eq!(manager.tier_pointer(old.slab_id).await, Some(old.clone()));
		let loc = manager.lookup(tenant(), id(1)).unwrap().unwrap();
		assert_eq!(manager.read(loc).await.unwrap(), b"hello-1");
		let objects = store.inner.list("b2/").await.unwrap();
		assert_eq!(objects.len(), 1, "only the committed old object remains");
		assert_eq!(objects[0].uri, old.l2_uri);
	}
	#[tokio::test]
	async fn b2_failed_delete_reopens_with_proof_and_eventually_clears() {
		let (dirs, manager, store, old) = b2_failed_delete_fixture().await;
		let error = manager
			.reconcile_table_drop_l2_cleanup_markers(&*store, usize::MAX)
			.await
			.unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
		manager.fsync_all_local_slabs().await.unwrap();
		let crypto = manager.retained_cleanup_test_crypto();
		drop(manager);
		let keysets = crypto.tenant_keysets_snapshot().unwrap();
		crypto
			.replace_keysets_from_authoritative_snapshot(&keysets)
			.unwrap();
		let reopened = SlabManager::open(SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: dirs[0].to_path_buf(),
			max_slab_bytes: 1,
			io: spawn_io_thread(64).unwrap(),
			l2: Some(store.clone()),
			rehydration: Default::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: Default::default(),
			crypto,
		})
		.await
		.unwrap();
		reopened.wait_tier_authority_ready().await.unwrap();
		assert!(
			reopened.refuse_unresolved_cleanup(old.slab_id).is_err(),
			"reopen retains the debt until backend deletion succeeds"
		);
		b2_assert_resolver_clears(&reopened, &store, &old).await;
	}
	#[tokio::test]
	async fn b2_unsubmitted_proof_survives_snapshot_catchup_and_clears() {
		use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let (_dirs, manager, store, old) = b2_failed_delete_fixture().await;
		let mut source = WendelonStateMachine::new();
		seed_state_machine_pointer(&source, &old).await;
		RaftStateMachine::apply(
			&mut source,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(3, 1), 201),
				payload: EntryPayload::Blank,
			}],
		)
		.await
		.unwrap();
		let mut builder = RaftStateMachine::get_snapshot_builder(&mut source).await;
		let snapshot = builder.build_snapshot().await.unwrap();
		let mut target = WendelonStateMachine::new()
			.with_tier_observer(Arc::new(ManagerTierObserver::new(manager.clone())));
		RaftStateMachine::install_snapshot(&mut target, &snapshot.meta, snapshot.snapshot)
			.await
			.unwrap();
		manager.wait_tier_authority_ready().await.unwrap();
		b2_assert_resolver_clears(&manager, &store, &old).await;
	}
	#[tokio::test]
	async fn b2_unsubmitted_proof_survives_log_apply_and_clears() {
		use openraft::storage::RaftStateMachine;
		use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
		let (_dirs, manager, store, old) = b2_failed_delete_fixture().await;
		let mut target = WendelonStateMachine::new()
			.with_tier_observer(Arc::new(ManagerTierObserver::new(manager.clone())));
		RaftStateMachine::apply(
			&mut target,
			[Entry {
				log_id: LogId::new(CommittedLeaderId::new(4, 1), 202),
				payload: EntryPayload::Normal(crate::consensus::WendelonRequest::TierSlab(
					TieredSlab {
						slab_id: old.slab_id,
						l2_uri: old.l2_uri.clone(),
						payload_hash: old.payload_hash,
						is_encrypted: false,
						tenant_id: None,
						tenant_kek_id: None,
						stats_format_version: None,
						stats_block_len: None,
						stats_block_hash128: None,
						tombstone_metadata: old.tombstone_metadata,
						supersession_metadata: old.supersession_metadata,
					},
				)),
			}],
		)
		.await
		.unwrap();
		b2_assert_resolver_clears(&manager, &store, &old).await;
	}

	#[tokio::test]
	async fn b2_reconcile_waits_for_active_rewrite_claim() {
		let (_dirs, manager, store, old) = b2_failed_delete_fixture().await;
		let claim = manager
			.acquire_slab_rewrite_exclusive_claim(old.slab_id)
			.await;
		let pause = manager.pause_candidate_reconcile_before_rewrite_once();
		let mut task = {
			let manager = manager.clone();
			let store = store.clone();
			tokio::spawn(async move {
				manager
					.reconcile_table_drop_l2_cleanup_markers(&*store, 1)
					.await
			})
		};
		tokio::time::timeout(Duration::from_secs(60), pause.reached.notified())
			.await
			.unwrap();
		pause.resume.notify_one();
		let early = tokio::time::timeout(Duration::from_millis(100), &mut task).await;
		let waited = early.is_err();
		drop(claim);
		let result = match early {
			Ok(result) => result,
			Err(_) => task.await,
		}
		.unwrap();
		assert_eq!(
			result.unwrap_err().kind(),
			std::io::ErrorKind::PermissionDenied
		);
		assert!(
			waited,
			"B2 candidate recovery must wait for an active upload/rewrite claim"
		);
	}

	#[tokio::test]
	async fn b2_retry_after_cancelled_durable_clear_clears_the_memory_fence() {
		let (_dirs, manager, store, old) = b2_failed_delete_fixture().await;
		store.fault.store(0, std::sync::atomic::Ordering::Release);
		let pause = manager.pause_candidate_after_clear_once();
		let task = {
			let manager = manager.clone();
			let store = store.clone();
			tokio::spawn(async move {
				manager
					.reconcile_table_drop_l2_cleanup_markers(&*store, usize::MAX)
					.await
			})
		};
		tokio::time::timeout(Duration::from_secs(60), pause.reached.notified())
			.await
			.unwrap();
		task.abort();
		assert!(task.await.unwrap_err().is_cancelled());
		assert!(
			manager.refuse_unresolved_cleanup(old.slab_id).is_err(),
			"cancelled mirror update keeps a conservative fence"
		);
		b2_assert_resolver_clears(&manager, &store, &old).await;
	}
}
