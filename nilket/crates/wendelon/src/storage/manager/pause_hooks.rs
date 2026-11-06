//! Test-only pause points that let controls observe the index between
//! the steps of a relocation, a removal, a publication or a read.
use super::*;

/// Every armed pause slot of one manager.
#[derive(Default)]
pub(crate) struct TestPauses {
	pub(super) candidate_after_clear_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) candidate_reconcile_before_rewrite_pause:
		std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) index_relocation_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) empty_key_removal_pause: std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	pub(super) empty_key_locality_removal_pause: std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	pub(super) tier_publication_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) append_metadata_observed_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) metadata_token_batch_pause: std::sync::Mutex<Option<MetadataTokenBatchPause>>,
	pub(super) metadata_token_before_allocation_pause:
		std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) metadata_token_committed_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) metadata_pointer_committed_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) table_drop_after_upload_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) cleanup_reconcile_claim_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) tier_observer_before_gate_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) snapshot_before_marker_clear_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) snapshot_after_retirement_mark_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) snapshot_before_retirement_apply_pause:
		std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) read_backing_selection_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) rehydrated_copy_eviction_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
	pub(super) empty_reference_slab_removal_pause: std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	pub(super) reference_reassignment_pause: std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	pub(super) append_reference_publication_pause: std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	pub(super) empty_secondary_reference_slab_removal_pause:
		std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	pub(super) migration_after_same_hlc_check_pause: std::sync::Mutex<Option<IndexRelocationPause>>,
}

/// Handles for a one-shot pause inside an install's in-memory index
/// relocation. The install notifies `reached` and waits for `resume`.
#[derive(Clone)]
pub(crate) struct IndexRelocationPause {
	pub(crate) reached: Arc<tokio::sync::Notify>,
	pub(crate) resume: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
pub(crate) struct MetadataTokenBatchPause {
	pub(crate) reached: Arc<tokio::sync::Barrier>,
	pub(crate) resume: Arc<tokio::sync::Barrier>,
}

/// Handles for a one-shot synchronous pause after the last version of a key
/// was removed and before the key itself is removed. The paused thread waits
/// on `reached` and then on `resume`.
#[derive(Clone)]
pub(crate) struct EmptyKeyRemovalPause {
	pub(crate) reached: Arc<std::sync::Barrier>,
	pub(crate) resume: Arc<std::sync::Barrier>,
}

impl SlabManager {
	/// Refuse every append from the `(skip+1)`th on at its head, as readiness would, until cleared.
	pub(crate) fn refuse_append_after_for_test(&self, skip: u64) {
		self.continuity_test_holds
			.append_refusal_after
			.store(skip + 1, Ordering::Release);
	}

	pub(super) fn note_append_refusal_for_test(&self) {
		let holds = &self.continuity_test_holds;
		holds.append_refusals_seen.fetch_add(1, Ordering::AcqRel);
		holds.append_refusal_seen.notify_waiters();
	}

	/// Until an append has actually returned the proof-carrying refusal.
	pub(crate) async fn append_refusal_for_test(&self) {
		let holds = &self.continuity_test_holds;
		loop {
			let seen = holds.append_refusal_seen.notified();
			tokio::pin!(seen);
			seen.as_mut().enable();
			if holds.append_refusals_seen.load(Ordering::Acquire) > 0 {
				return;
			}
			seen.await;
		}
	}

	pub(crate) fn clear_append_refusal_for_test(&self) {
		self.continuity_test_holds
			.append_refusal_after
			.store(0, Ordering::Release);
	}

	/// The next version check raises this fence, once: after its append's head admitted it.
	pub(crate) fn fence_at_next_version_check_for_test(&self, slab_id: SlabId) {
		*self
			.continuity_test_holds
			.fence_at_version_check
			.lock()
			.unwrap() = Some(slab_id);
	}

	pub(super) fn raise_fence_at_version_check_for_test(&self) {
		let slab = self
			.continuity_test_holds
			.fence_at_version_check
			.lock()
			.unwrap()
			.take();
		if let Some(slab) = slab {
			self.raise_retirement_fence_for_test(slab);
		}
	}

	/// Raise and clear the real store-wide fence a tier retirement holds.
	pub(crate) fn raise_retirement_fence_for_test(&self, slab_id: SlabId) {
		self.tier_authority.mark_retirement(slab_id, Some(1));
	}

	pub(crate) fn clear_retirement_fence_for_test(&self, slab_id: SlabId) {
		self.tier_authority.clear_retirement(slab_id, Some(1));
	}

	/// A refusal that does not clear by itself, as an unresolved operation's fence is.
	pub(crate) fn fence_unresolved_op_for_test(&self) {
		self.tier_authority
			.unresolved_op_fence
			.store(true, Ordering::Release);
	}

	pub(super) fn take_append_refusal_for_test(&self) -> bool {
		self.continuity_test_holds
			.append_refusal_after
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
				(left > 1).then(|| left - 1)
			})
			.is_err_and(|left| left == 1)
	}

	/// Shared acceptance assertions for a fully applied retirement. Pending
	/// retirement controls keep their separate fence/drain assertions.
	pub(crate) async fn assert_retirement_completed_for_test(
		&self,
		tenant: TenantId,
		key: Identity,
		old_location: RecordLocation,
		uri: &str,
	) {
		use crate::storage::core::StorageEngine;
		use crate::storage::index::RetainedObjectIntentState;
		tokio::time::timeout(Duration::from_secs(60), self.wait_tier_authority_ready())
			.await
			.unwrap()
			.unwrap();
		assert!(
			self.lookup(tenant, key).unwrap().is_none(),
			"retired row must be absent"
		);
		assert!(
			self.read(old_location).await.is_err(),
			"an old location must not serve retired data"
		);
		assert!(
			self.scan(tenant, Locality::new(0), Locality::new(u64::MAX))
				.unwrap()
				.iter()
				.all(|(_, location)| location.slab_id != old_location.slab_id),
			"scan must not serve any row from the retired slab"
		);
		let records = self
			.persistent_index
			.as_ref()
			.unwrap()
			.load_snapshot_cleanup_intents()
			.await
			.unwrap();
		let record = records
			.iter()
			.find(|(slab, retained_uri, _)| *slab == old_location.slab_id && retained_uri == uri)
			.expect("durable retirement evidence");
		assert_eq!(
			record.2.state,
			RetainedObjectIntentState::RetirementPublished
		);
		assert!(record.2.retirement_source.is_some());
		assert!(
			self.config.l2.as_ref().unwrap().get(uri).await.is_ok(),
			"old object retained"
		);
		assert!(
			self.tier_authority_readiness_reason().is_none(),
			"completed retirement reaches Ready"
		);
	}

	pub(crate) async fn hold_index_write_gate_for_test(
		&self,
	) -> tokio::sync::OwnedRwLockWriteGuard<()> {
		self.persistent_index_gate.clone().write_owned().await
	}

	/// Arm a one-shot pause inside the next install's index relocation.
	pub(crate) fn pause_index_relocation_once(&self) -> IndexRelocationPause {
		let pause = IndexRelocationPause {
			reached: Arc::new(tokio::sync::Notify::new()),
			resume: Arc::new(tokio::sync::Notify::new()),
		};
		*self.test_pauses.index_relocation_pause.lock().unwrap() = Some(pause.clone());
		pause
	}

	pub(super) async fn index_relocation_pause_point(&self) {
		let pause = self
			.test_pauses
			.index_relocation_pause
			.lock()
			.unwrap()
			.take();
		if let Some(pause) = pause {
			pause.reached.notify_one();
			pause.resume.notified().await;
		}
	}

	/// Arm a one-shot pause before the next empty key is removed.
	pub(crate) fn pause_before_empty_key_removal_once(&self) -> EmptyKeyRemovalPause {
		let pause = EmptyKeyRemovalPause {
			reached: Arc::new(std::sync::Barrier::new(2)),
			resume: Arc::new(std::sync::Barrier::new(2)),
		};
		*self.test_pauses.empty_key_removal_pause.lock().unwrap() = Some(pause.clone());
		pause
	}

	pub(super) fn empty_key_removal_pause_point(&self) {
		let pause = self
			.test_pauses
			.empty_key_removal_pause
			.lock()
			.unwrap()
			.take();
		if let Some(pause) = pause {
			pause.reached.wait();
			pause.resume.wait();
		}
	}

	/// Arm a one-shot pause after the next empty key is removed and before its
	/// locality membership is removed.
	pub(crate) fn pause_after_empty_key_removal_once(&self) -> EmptyKeyRemovalPause {
		let pause = EmptyKeyRemovalPause {
			reached: Arc::new(std::sync::Barrier::new(2)),
			resume: Arc::new(std::sync::Barrier::new(2)),
		};
		*self
			.test_pauses
			.empty_key_locality_removal_pause
			.lock()
			.unwrap() = Some(pause.clone());
		pause
	}

	pub(super) fn arm_async_pause(
		slot: &std::sync::Mutex<Option<IndexRelocationPause>>,
	) -> IndexRelocationPause {
		let pause = IndexRelocationPause {
			reached: Arc::new(tokio::sync::Notify::new()),
			resume: Arc::new(tokio::sync::Notify::new()),
		};
		*slot.lock().unwrap() = Some(pause.clone());
		pause
	}

	pub(super) async fn fire_async_pause(slot: &std::sync::Mutex<Option<IndexRelocationPause>>) {
		let pause = slot.lock().unwrap().take();
		if let Some(pause) = pause {
			pause.reached.notify_one();
			pause.resume.notified().await;
		}
	}

	pub(crate) fn pause_snapshot_before_retirement_apply_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.snapshot_before_retirement_apply_pause)
	}

	pub(super) async fn snapshot_before_retirement_apply_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.snapshot_before_retirement_apply_pause).await;
	}

	pub(crate) fn pause_snapshot_after_retirement_mark_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.snapshot_after_retirement_mark_pause)
	}

	pub(super) async fn snapshot_after_retirement_mark_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.snapshot_after_retirement_mark_pause).await;
	}

	pub(crate) fn pause_snapshot_before_marker_clear_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.snapshot_before_marker_clear_pause)
	}

	#[allow(dead_code)] // Used by the snapshot-comparison fix in the following commit.
	pub(super) async fn snapshot_before_marker_clear_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.snapshot_before_marker_clear_pause).await;
	}

	pub(crate) fn pause_candidate_after_clear_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.candidate_after_clear_pause)
	}
	pub(super) async fn candidate_after_clear_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.candidate_after_clear_pause).await;
	}

	pub(crate) fn pause_candidate_reconcile_before_rewrite_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.candidate_reconcile_before_rewrite_pause)
	}
	pub(super) async fn candidate_reconcile_before_rewrite_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.candidate_reconcile_before_rewrite_pause).await;
	}

	pub(crate) fn pause_cleanup_reconcile_claim_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.cleanup_reconcile_claim_pause)
	}
	pub(super) async fn cleanup_reconcile_claim_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.cleanup_reconcile_claim_pause).await;
	}

	pub(crate) fn pause_table_drop_after_upload_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.table_drop_after_upload_pause)
	}

	pub(super) async fn table_drop_after_upload_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.table_drop_after_upload_pause).await;
	}

	pub(crate) fn pause_metadata_token_before_allocation_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.metadata_token_before_allocation_pause)
	}
	pub(super) async fn metadata_token_before_allocation_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.metadata_token_before_allocation_pause).await;
	}

	pub(crate) fn pause_metadata_token_committed_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.metadata_token_committed_pause)
	}
	pub(super) async fn metadata_token_committed_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.metadata_token_committed_pause).await;
	}
	pub(crate) fn pause_metadata_pointer_committed_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.metadata_pointer_committed_pause)
	}
	pub(super) async fn metadata_pointer_committed_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.metadata_pointer_committed_pause).await;
	}

	pub(crate) fn pause_metadata_tokens(&self, writers: usize) -> MetadataTokenBatchPause {
		let pause = MetadataTokenBatchPause {
			reached: Arc::new(tokio::sync::Barrier::new(writers + 1)),
			resume: Arc::new(tokio::sync::Barrier::new(writers + 1)),
		};
		*self.test_pauses.metadata_token_batch_pause.lock().unwrap() = Some(pause.clone());
		pause
	}
	pub(super) async fn metadata_token_batch_pause_point(&self) {
		let pause = self
			.test_pauses
			.metadata_token_batch_pause
			.lock()
			.unwrap()
			.clone();
		if let Some(pause) = pause {
			pause.reached.wait().await;
			pause.resume.wait().await;
		}
	}

	pub(crate) fn pause_append_metadata_observed_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.append_metadata_observed_pause)
	}

	pub(super) async fn append_metadata_observed_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.append_metadata_observed_pause).await;
	}

	pub(crate) fn pause_tier_observer_before_gate_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.tier_observer_before_gate_pause)
	}

	pub(super) async fn tier_observer_before_gate_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.tier_observer_before_gate_pause).await;
	}

	/// Arm a one-shot pause in the next migration ingest, after it found no
	/// same-HLC copy and before it appends.
	pub(crate) fn pause_migration_after_same_hlc_check_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.migration_after_same_hlc_check_pause)
	}

	pub(super) async fn migration_after_same_hlc_check_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.migration_after_same_hlc_check_pause).await;
	}

	/// Arm a one-shot pause in the next tier publication, after its redirects
	/// and before its pointer is installed.
	pub(crate) fn pause_tier_publication_before_pointer_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.tier_publication_pause)
	}

	pub(super) async fn tier_publication_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.tier_publication_pause).await;
	}

	/// Arm a one-shot pause in the next read, after its location is resolved
	/// and before its backing is selected.
	pub(crate) fn pause_read_before_backing_selection_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.read_backing_selection_pause)
	}

	pub(super) async fn read_backing_selection_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.read_backing_selection_pause).await;
	}

	/// Arm a one-shot pause in the next rehydrated-copy eviction, after the
	/// copy left `state` and before its file is unlinked.
	pub(crate) fn pause_rehydrated_copy_eviction_before_unlink_once(&self) -> IndexRelocationPause {
		Self::arm_async_pause(&self.test_pauses.rehydrated_copy_eviction_pause)
	}

	pub(super) async fn rehydrated_copy_eviction_pause_point(&self) {
		Self::fire_async_pause(&self.test_pauses.rehydrated_copy_eviction_pause).await;
	}

	/// Arm a one-shot pause before the next empty reference slab entry is removed.
	pub(crate) fn pause_before_empty_reference_slab_removal_once(&self) -> EmptyKeyRemovalPause {
		let pause = EmptyKeyRemovalPause {
			reached: Arc::new(std::sync::Barrier::new(2)),
			resume: Arc::new(std::sync::Barrier::new(2)),
		};
		*self
			.test_pauses
			.empty_reference_slab_removal_pause
			.lock()
			.unwrap() = Some(pause.clone());
		pause
	}

	pub(super) fn empty_reference_slab_removal_pause_point(&self) {
		let pause = self
			.test_pauses
			.empty_reference_slab_removal_pause
			.lock()
			.unwrap()
			.take();
		if let Some(pause) = pause {
			pause.reached.wait();
			pause.resume.wait();
		}
	}

	pub(super) fn arm_sync_pause(
		slot: &std::sync::Mutex<Option<EmptyKeyRemovalPause>>,
	) -> EmptyKeyRemovalPause {
		let pause = EmptyKeyRemovalPause {
			reached: Arc::new(std::sync::Barrier::new(2)),
			resume: Arc::new(std::sync::Barrier::new(2)),
		};
		*slot.lock().unwrap() = Some(pause.clone());
		pause
	}

	pub(super) fn fire_sync_pause(slot: &std::sync::Mutex<Option<EmptyKeyRemovalPause>>) {
		let pause = slot.lock().unwrap().take();
		if let Some(pause) = pause {
			pause.reached.wait();
			pause.resume.wait();
		}
	}

	pub(crate) fn pause_inside_reference_reassignment_once(&self) -> EmptyKeyRemovalPause {
		Self::arm_sync_pause(&self.test_pauses.reference_reassignment_pause)
	}

	pub(super) fn reference_reassignment_pause_point(&self) {
		Self::fire_sync_pause(&self.test_pauses.reference_reassignment_pause);
	}

	pub(crate) fn pause_before_append_reference_publication_once(&self) -> EmptyKeyRemovalPause {
		Self::arm_sync_pause(&self.test_pauses.append_reference_publication_pause)
	}

	pub(super) fn append_reference_publication_pause_point(&self) {
		Self::fire_sync_pause(&self.test_pauses.append_reference_publication_pause);
	}

	pub(crate) fn pause_before_empty_secondary_reference_slab_removal_once(
		&self,
	) -> EmptyKeyRemovalPause {
		Self::arm_sync_pause(
			&self
				.test_pauses
				.empty_secondary_reference_slab_removal_pause,
		)
	}

	pub(super) fn empty_secondary_reference_slab_removal_pause_point(&self) {
		Self::fire_sync_pause(
			&self
				.test_pauses
				.empty_secondary_reference_slab_removal_pause,
		);
	}

	pub(super) fn empty_key_locality_removal_pause_point(&self) {
		let pause = self
			.test_pauses
			.empty_key_locality_removal_pause
			.lock()
			.unwrap()
			.take();
		if let Some(pause) = pause {
			pause.reached.wait();
			pause.resume.wait();
		}
	}
}

#[derive(Default)]
pub(super) struct TestHolds {
	pub(super) retirement_snapshot_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) rebuild_snapshot_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// At a GC removal's disposal entry: the intent and the file identity are
	/// captured, and no exclusion, authority check or local effect has happened.
	/// After a reservation cleanup's file was validated: stage 1 before its disposal,
	/// stage 2 before its purge.
	pub(super) cleanup_validated_hold:
		std::sync::Mutex<Option<(u8, Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Holds a deferred open before it starts, and fails one attempt of it.
	pub(super) deferred_open_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) deferred_open_fail_once: std::sync::atomic::AtomicBool,
	/// Parks a rename in its own task, as a late worker would be.
	pub(super) lsm_rename_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Parks a tier delivery after its evidence is prepared, before its authority.
	pub(super) tier_delivery_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Fails the next read of what a recovered publication left.
	pub(super) fail_publication_readback: std::sync::atomic::AtomicBool,
	pub(super) append_refusal_after: std::sync::atomic::AtomicU64,
	pub(super) fence_at_version_check: std::sync::Mutex<Option<SlabId>>,
	/// Appends that returned the proof-carrying refusal, and a wake-up for whoever awaits one.
	pub(super) append_refusals_seen: std::sync::atomic::AtomicU64,
	pub(super) append_refusal_seen: tokio::sync::Notify,
	/// Parks a deferred open at one named stage, for a process death there.
	pub(super) deferred_open_stage_hold:
		std::sync::Mutex<Option<(u8, Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) gc_disposal_entry_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) gc_before_unlink_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) gc_after_unlink_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) gc_after_fsync_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Pauses an operation's handoff inside its canonical lease.
	pub(super) op_handoff_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Reaches the allocation's own window check, as a lost race with another
	/// producer would.
	pub(super) skip_op_room_precheck: std::sync::atomic::AtomicBool,
	/// Keeps the GC sweep from reaching a removal, as a blocker would.
	pub(super) gc_remove_held: std::sync::atomic::AtomicBool,
	pub(super) gc_before_clear_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	pub(super) gc_after_clear_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}

impl SlabManager {
	pub(super) async fn pause_gc_after_clear_for_test(&self) {
		let hold = self
			.continuity_test_holds
			.gc_after_clear_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_op_handoff_for_test(&self) {
		let hold = self
			.continuity_test_holds
			.op_handoff_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_cleanup_validated_for_test(&self, stage: u8) {
		let hold = self
			.continuity_test_holds
			.cleanup_validated_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((wanted, entered, release)) = hold
			&& wanted == stage
		{
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_deferred_open_stage_for_test(&self, stage: u8) {
		let hold = self
			.continuity_test_holds
			.deferred_open_stage_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((at, entered, release)) = hold
			&& at == stage
		{
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_deferred_open_for_test(&self) -> std::io::Result<()> {
		let hold = self
			.continuity_test_holds
			.deferred_open_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
		if self
			.continuity_test_holds
			.deferred_open_fail_once
			.swap(false, std::sync::atomic::Ordering::AcqRel)
		{
			return Err(std::io::Error::other("injected: a deferred open failed"));
		}
		Ok(())
	}

	pub(super) async fn pause_gc_disposal_entry_for_test(&self) {
		let hold = self
			.continuity_test_holds
			.gc_disposal_entry_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_gc_before_clear_for_test(&self) {
		let hold = self
			.continuity_test_holds
			.gc_before_clear_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_rebuild_snapshot_for_test(&self) {
		let hold = self
			.continuity_test_holds
			.rebuild_snapshot_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	pub(super) async fn pause_retirement_snapshot_for_test(&self) {
		let hold = self
			.continuity_test_holds
			.retirement_snapshot_hold
			.lock()
			.unwrap()
			.clone();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}
}

/// ADR 0090 route-gate hook: install an arbitrary version so route tests can
/// construct placement states the write path cannot produce yet. The location
/// may point at nothing — route selection must decide before any slab byte is
/// touched, which is exactly what the tests prove.
#[cfg(test)]
impl SlabManager {
	pub(crate) fn install_version_for_route_test(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
	) {
		self.key_index
			.entry((tenant_id, key_identity))
			.or_default()
			.push(location);
	}
}
