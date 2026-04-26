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
use crate::schema::{HybridTimestamp, NodeId, SlabId, TenantId, TenantKekId, TombstoneMetadata};
use crate::storage::MvccRetentionPolicy;
use crate::storage::l2::ObjectStore;
use crate::storage::manager::{
	PreparedTieredRetentionRewrite, SealedSlabInfo, SlabManager, TieredSlabPointer,
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
}

/// The sweeper's view of the Raft handle.
///
/// Abstracted behind a trait so tests can drive the tier path
/// without standing up an openraft cluster. The production impl
/// is a thin wrapper over [`WendelonRaft`]; see [`RaftCommitter`].
#[async_trait]
pub trait TierCommitter: Send + Sync {
	/// Submit a TierSlab entry. Returns `Ok(())` only after the
	/// entry has been committed *and* applied locally, so the
	/// caller can treat a successful return as "L1 file has been
	/// unlinked via the SM apply observer". An error means the
	/// sweeper should skip this slab and try again on the next
	/// scan — the L1 file stays put.
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()>;

	/// Submit a RetireTieredSlab entry. Returns `Ok(())` only after
	/// the local state machine has applied the retirement.
	async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()>;
}

/// Production [`TierCommitter`] backed by an openraft [`WendelonRaft`].
///
/// `client_write` is resolved on the leader and blocks until the
/// local state machine has applied the entry, which is exactly the
/// synchronisation the sweeper needs. On a follower, openraft
/// returns `ForwardToLeader`, which we surface as a non-fatal
/// `WouldBlock` so the sweeper can skip this pass without tearing
/// down.
pub struct RaftCommitter {
	raft: WendelonRaft,
}

impl RaftCommitter {
	pub fn new(raft: WendelonRaft) -> Self {
		Self { raft }
	}
}

#[async_trait]
impl TierCommitter for RaftCommitter {
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
		match self
			.raft
			.client_write(WendelonRequest::TierSlab(tier))
			.await
		{
			Ok(_) => Ok(()),
			Err(e) => Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				format!("TierSlab commit failed: {e}"),
			)),
		}
	}

	async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<()> {
		match self
			.raft
			.client_write(WendelonRequest::RetireTieredSlab(RetireTieredSlab {
				slab_id,
			}))
			.await
		{
			Ok(_) => Ok(()),
			Err(e) => Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				format!("RetireTieredSlab commit failed: {e}"),
			)),
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
		metrics().storage_tiering_passes_total.inc();
		let mut report = SweepReport::default();
		let reconcile = self.reconcile_tiered_runtime_state_once().await?;
		report.failed += reconcile.failed;
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
		let key = format!("{}slab_{:016x}.wnd", self.config.key_prefix, info.slab_id);
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
			tombstone_metadata: TombstoneMetadata::None,
		};
		if let Err(error) = self.raft.commit_tier_slab(tier).await {
			self.cleanup_uploaded_object_after_failed_commit(info.slab_id, &l2_uri)
				.await;
			return Err(error);
		}

		// Defensive: if the SM has an observer wired in, apply is
		// already done. If it does not (e.g., a test wiring), the
		// sweeper still owns the invariant — call through the
		// manager's apply_tier_slab directly. This is idempotent
		// on the real path because the observer already moved the
		// slab out of `sealed`.
		if let Some(pointer) = self.sm_tier_pointer(info.slab_id).await {
			self.slab.apply_tier_slab(pointer).await?;
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
		self.rate_limit_upload(encrypted.bytes.len()).await;
		let new_uri = self.object_store.put(&key, &encrypted.bytes).await?;
		let fetched = self.object_store.get(&new_uri).await?;
		let fetched_hash = crate::hash::content::digest(&fetched);
		if fetched_hash != expected_hash {
			let _ = self.delete_object_if_present(&new_uri).await;
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
		};
		if let Err(error) = self.raft.commit_tier_slab(tier).await {
			self.cleanup_uploaded_object_after_failed_commit(pointer.slab_id, &new_uri)
				.await;
			return Err(error);
		}

		if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
			self.slab.apply_tier_slab(pointer).await?;
		}

		if pointer.l2_uri != new_uri {
			if let Err(error) = self.delete_object_if_present(&pointer.l2_uri).await {
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
			};
			if let Err(error) = self.raft.commit_tier_slab(tier).await {
				return Err(error);
			}
			if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
				self.slab.apply_tier_slab(pointer).await?;
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
		self.rate_limit_upload(encrypted.bytes.len()).await;
		let new_uri = self.object_store.put(&key, &encrypted.bytes).await?;
		let fetched = self.object_store.get(&new_uri).await?;
		let fetched_hash = crate::hash::content::digest(&fetched);
		if fetched_hash != expected_hash {
			let _ = self.delete_object_if_present(&new_uri).await;
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
		};
		if let Err(error) = self.raft.commit_tier_slab(tier).await {
			self.cleanup_uploaded_object_after_failed_commit(pointer.slab_id, &new_uri)
				.await;
			return Err(error);
		}

		if let Some(pointer) = self.sm_tier_pointer(pointer.slab_id).await {
			self.slab.apply_tier_slab(pointer).await?;
		}

		if pointer.l2_uri != new_uri {
			if let Err(error) = self.delete_object_if_present(&pointer.l2_uri).await {
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

	fn mvcc_retention_floor_hlc(&self) -> Option<HybridTimestamp> {
		let window = self.mvcc_retention_policy.window()?;
		let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
		Some(HybridTimestamp {
			physical_ms: self.clock.now_ms().saturating_sub(window_ms),
			logical: 0,
			node_id: 0,
		})
	}

	async fn rewrite_tiered_slabs_for_retention_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<TieredRetentionRewriteReport> {
		let mut report = TieredRetentionRewriteReport::default();
		let Some(retention_floor) = self.mvcc_retention_floor_hlc() else {
			return Ok(report);
		};
		if max_slabs == 0 {
			return Ok(report);
		}
		for pointer in self.slab.list_tiered_slabs().await {
			if report.rewritten >= max_slabs {
				break;
			}
			match self
				.rewrite_one_tiered_slab_for_retention(&pointer, retention_floor)
				.await
			{
				Ok(Some(outcome)) => {
					report.rewritten += 1;
					report.pruned_versions += outcome.pruned_versions;
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
		retention_floor: HybridTimestamp,
	) -> std::io::Result<Option<PreparedTieredRetentionOutcome>> {
		let _permit = authorize_autonomous_action(AutonomousActionSurface::L2RetentionRewrite);
		let snapshot = self.state_machine.state_snapshot().await;
		let Some(prepared) = self
			.slab
			.prepare_tiered_retention_rewrite(pointer, retention_floor, &snapshot.table_schemas)
			.await?
		else {
			return Ok(None);
		};
		match prepared {
			PreparedTieredRetentionRewrite::Rewrite {
				slab_id,
				old_l2_uri,
				encrypted_bytes,
				payload_hash,
				tombstone_metadata,
				pruned_versions,
				reclaimed_bytes,
			} => {
				let key = self.retention_rewritten_key(slab_id);
				self.rate_limit_upload(encrypted_bytes.len()).await;
				let new_uri = self.object_store.put(&key, &encrypted_bytes).await?;
				let fetched = self.object_store.get(&new_uri).await?;
				let fetched_hash = crate::hash::content::digest(&fetched);
				if fetched_hash != payload_hash {
					let _ = self.delete_object_if_present(&new_uri).await;
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
				};
				if let Err(error) = self.raft.commit_tier_slab(tier).await {
					self.cleanup_uploaded_object_after_failed_commit(slab_id, &new_uri)
						.await;
					return Err(error);
				}

				if let Some(pointer) = self.sm_tier_pointer(slab_id).await {
					self.slab.apply_tier_slab(pointer).await?;
				}
				if old_l2_uri != new_uri {
					if let Err(error) = self.delete_object_if_present(&old_l2_uri).await {
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
					retention_floor,
					pruned_versions,
					reclaimed_bytes,
					&old_l2_uri,
					Some(&new_uri),
					"rewrite",
				);
				Ok(Some(PreparedTieredRetentionOutcome {
					pruned_versions,
					reclaimed_bytes,
				}))
			}
			PreparedTieredRetentionRewrite::Retire {
				slab_id,
				old_l2_uri,
				pruned_versions,
				reclaimed_bytes,
			} => {
				self.raft.retire_tiered_slab(slab_id).await?;
				self.slab.retire_tiered_slab(slab_id).await?;
				if let Err(error) = self.delete_object_if_present(&old_l2_uri).await {
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
					retention_floor,
					pruned_versions,
					reclaimed_bytes,
					&old_l2_uri,
					None,
					"retire",
				);
				Ok(Some(PreparedTieredRetentionOutcome {
					pruned_versions,
					reclaimed_bytes,
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
		let state = self.state_machine.state_snapshot().await;
		state.tiered_slabs.get(&slab_id).map(|t| TieredSlabPointer {
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
		})
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
			})
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	async fn delete_object_if_present(&self, uri: &str) -> std::io::Result<()> {
		match self.object_store.delete(uri).await {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error),
		}
	}

	async fn cleanup_uploaded_object_after_failed_commit(&self, slab_id: SlabId, new_uri: &str) {
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
			return;
		}
		if let Err(error) = self.delete_object_if_present(new_uri).await {
			tracing::warn!(
				slab_id = slab_id.get(),
				l2_uri = %new_uri,
				error = %error,
				"failed to clean up uncommitted uploaded tiered object"
			);
		}
	}

	async fn reconcile_tiered_runtime_state_once(
		&self,
	) -> std::io::Result<TieredStateReconcileReport> {
		let mut report = TieredStateReconcileReport::default();
		let authoritative = self.sm_tier_pointers().await;
		let authoritative_by_slab = authoritative
			.iter()
			.map(|pointer| (pointer.slab_id, pointer.clone()))
			.collect::<std::collections::HashMap<_, _>>();
		let local = self.slab.list_tiered_slabs().await;
		let local_by_slab = local
			.iter()
			.map(|pointer| (pointer.slab_id, pointer.clone()))
			.collect::<std::collections::HashMap<_, _>>();

		for pointer in authoritative {
			if local_by_slab.get(&pointer.slab_id) == Some(&pointer) {
				continue;
			}
			match self.slab.reconcile_tiered_slab(pointer.clone(), None).await {
				Ok(()) => report.reconciled += 1,
				Err(error) => {
					report.failed += 1;
					tracing::warn!(
						slab_id = pointer.slab_id.get(),
						l2_uri = %pointer.l2_uri,
						error = %error,
						"failed to reconcile local tier state from committed state machine pointer"
					);
				}
			}
		}

		for pointer in local {
			if authoritative_by_slab.contains_key(&pointer.slab_id) {
				continue;
			}
			match self.slab.retire_tiered_slab(pointer.slab_id).await {
				Ok(changed) => {
					if changed {
						report.reconciled += 1;
					}
				}
				Err(error) => {
					report.failed += 1;
					tracing::warn!(
						slab_id = pointer.slab_id.get(),
						l2_uri = %pointer.l2_uri,
						error = %error,
						"failed to retire local tier pointer absent from committed state machine"
					);
					continue;
				}
			}
			if let Err(error) = self.delete_object_if_present(&pointer.l2_uri).await {
				report.failed += 1;
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					l2_uri = %pointer.l2_uri,
					error = %error,
					"failed to delete retired L2 object during committed-state reconciliation"
				);
				continue;
			}
			self.slab.clear_retired_tiered_cleanup(pointer.slab_id);
		}

		for pointer in self.slab.list_retired_tiered_cleanup_candidates().await {
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
			if let Err(error) = self.delete_object_if_present(&pointer.l2_uri).await {
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

	async fn reclaim_tombstoned_ranges_once(
		&self,
		max_slabs: usize,
	) -> std::io::Result<crate::storage::manager::TombstoneCompactionReport> {
		let _permit =
			authorize_autonomous_action(AutonomousActionSurface::MigrationTombstoneReclaim);
		let state = self.state_machine.state_snapshot().await;
		self.slab
			.reclaim_tombstoned_ranges(&state.tombstones, max_slabs)
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
struct TieredRetentionRewriteReport {
	rewritten: usize,
	pruned_versions: usize,
	reclaimed_bytes: u64,
	failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedTieredRetentionOutcome {
	pruned_versions: usize,
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
}

impl ManagerTierObserver {
	pub fn new(slab: Arc<SlabManager>) -> Self {
		Self { slab }
	}
}

#[async_trait]
impl TierObserver for ManagerTierObserver {
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
		self.slab
			.retire_tiered_slab_committed(slab_id, Some(applied_log_index))
			.await
			.map(|_| ())
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
	use crate::storage::io_uring::spawn_io_thread;
	use crate::storage::l2::LocalFsObjectStore;
	use crate::storage::manager::SlabManagerConfig;
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
		async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
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

	fn fresh_dir(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-sweeper-{tag}-{}-{}",
			std::process::id(),
			uuid::Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir
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

	fn test_crypto() -> Arc<CryptoManager> {
		let crypto = Arc::new(CryptoManager::new_dev());
		crypto.install_local_only_for_tests(tenant()).unwrap();
		crypto
	}

	async fn build_manager_with_sealed_slabs(
		l2: Option<Arc<dyn ObjectStore>>,
	) -> (Arc<SlabManager>, std::path::PathBuf) {
		use crate::storage::core::StorageEngine;
		let dir = fresh_dir("l1");
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				// Tiny cap so two appends produce two sealed slabs.
				max_slab_bytes: 1,
				io,
				l2,
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
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
		(manager, dir)
	}

	#[tokio::test]
	async fn sweeper_tiers_sealed_slabs_and_unlinks_l1() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

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
	async fn read_pivots_to_l2_after_tiering() {
		use crate::storage::core::StorageEngine;

		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

		// Record the location of key=1 before sweeping so we can
		// verify the read path pivots without a re-lookup.
		let loc1 = manager.lookup(tenant(), id(1)).expect("key present");
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;

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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
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

	#[tokio::test]
	async fn apply_tier_slab_is_idempotent() {
		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2)).await;
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
		};
		manager.apply_tier_slab(ptr.clone()).await.unwrap();
		// Second apply must not fail even though sealed no longer
		// contains the slab.
		manager.apply_tier_slab(ptr).await.unwrap();
		assert!(manager.is_tiered(info.slab_id).await);
	}

	#[tokio::test]
	async fn sweeper_rewrites_plaintext_tiered_slabs_to_ciphertext() {
		use crate::storage::core::StorageEngine;

		let l2_root = fresh_dir("l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let (manager, _l1_dir) = build_manager_with_sealed_slabs(Some(l2.clone())).await;
		let loc = manager.lookup(tenant(), id(1)).expect("key present");
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
			l2.get(&old_uri).await.is_err(),
			"legacy plaintext object should be deleted after rewrite"
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let crypto = test_crypto();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: crypto.clone(),
			})
			.await
			.unwrap(),
		);

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
			l2.get(&old_pointer.l2_uri).await.is_err(),
			"old tenant-KEK object should be deleted after re-wrap"
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);

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
	async fn sweeper_rewrites_tiered_slabs_for_mvcc_retention() {
		use crate::storage::core::StorageEngine;

		let now_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		let l1_dir = fresh_dir("retention-l1");
		let l2_root = fresh_dir("retention-l2");
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
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
			l2.get(&old_uri).await.is_err(),
			"old tiered object should be deleted after retention rewrite"
		);

		let retained_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(12_000)))
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
				.is_none(),
			"versions older than the retention floor should disappear from the tiered index"
		);
		let retained_latest = manager.lookup(tenant(), key).expect("latest survives");
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
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
			l2.get(&old_uri).await.is_err(),
			"retired tiered object should be deleted after retention retirement"
		);

		let retained_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(12_000)))
			.expect("retained-window anchor should remain");
		assert_eq!(manager.read(retained_anchor).await.unwrap(), b"v-anchor");
		assert!(
			manager
				.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(30_000)))
				.is_none(),
			"fully pruned slab should disappear from historical lookups"
		);
		let retained_latest = manager.lookup(tenant(), key).expect("latest survives");
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		let key = id(27);
		let duplicate_hlc = hlc_at(now_ms.saturating_sub(50_000));
		let latest = hlc_at(now_ms.saturating_sub(5_000));
		manager
			.append(tenant(), key, duplicate_hlc, b"v-old-a")
			.await
			.unwrap();
		manager
			.append(tenant(), key, duplicate_hlc, b"v-old-b")
			.await
			.unwrap();
		manager
			.append(tenant(), key, latest, b"v-latest")
			.await
			.unwrap();
		let pre_rewrite_anchor = manager
			.lookup_as_of(tenant(), key, hlc_at(now_ms.saturating_sub(10_000)))
			.expect("latest duplicate before floor should be retained");
		assert_eq!(
			manager.read(pre_rewrite_anchor).await.unwrap(),
			b"v-old-b",
			"lookup_as_of should prefer the later duplicate at the same HLC"
		);

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
			.expect("retained duplicate should remain visible");
		assert_eq!(manager.read(retained_anchor).await.unwrap(), b"v-old-b");
		assert_eq!(
			manager.read(pre_rewrite_anchor).await.unwrap(),
			b"v-old-b",
			"stale duplicate-HLC location should relocate to the surviving rewritten row"
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
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
		let l2: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::new(&l2_root).unwrap());
		let io = spawn_io_thread(64).unwrap();
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: l1_dir,
				max_slab_bytes: 1 << 20,
				io,
				l2: Some(l2.clone()),
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
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
			l2.get(&old_uri).await.is_err(),
			"cleanup retry should delete the stale retired L2 object"
		);
	}
}
