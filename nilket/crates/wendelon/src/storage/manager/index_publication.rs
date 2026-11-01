//! Persistent index publication and snapshot coordination.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IndexPublicationState {
	InFlight,
	Orphaned,
}

/// Only a single durable row append currently has an in-place proof. All other
/// publication kinds retain their orphan fence until manager reopen.
#[derive(Clone, Copy, Debug)]
pub(super) struct SingleAppendEvidence {
	pub(super) row: PersistentRecordRow,
	pub(super) pre: Option<PersistentRecordRow>,
	pub(super) frame_digest: [u8; 32],
}

/// An owner records its orphaned publication on every non-successful exit.
/// The gate is also cloned into any blocking redb transaction, which may
/// outlive this owner after cancellation.
pub(super) struct IndexPublication {
	marks: Arc<DashMap<(SlabId, u64), IndexPublicationState>>,
	orphaned: Arc<std::sync::atomic::AtomicUsize>,
	evidence: Arc<DashMap<(SlabId, u64), SingleAppendEvidence>>,
	slabs: Vec<SlabId>,
	revision: u64,
	pub(super) gate: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	authority: Arc<tier_authority::TierAuthority>,
	completed: bool,
}
impl IndexPublication {
	pub(super) fn complete(mut self) {
		for &slab in &self.slabs {
			self.marks.remove(&(slab, self.revision));
			self.evidence.remove(&(slab, self.revision));
		}
		self.completed = true;
		self.authority.notify_publication_change();
		if self.orphaned.load(Ordering::Acquire) != 0 {
			self.authority.schedule_index_reconciliation();
		}
	}
}
impl Drop for IndexPublication {
	fn drop(&mut self) {
		if !self.completed {
			for &slab in &self.slabs {
				if let Some(mut mark) = self.marks.get_mut(&(slab, self.revision)) {
					if *mark == IndexPublicationState::InFlight {
						self.orphaned.fetch_add(1, Ordering::AcqRel);
						*mark = IndexPublicationState::Orphaned;
					}
				}
			}
			self.authority.notify_publication_change();
			if self
				.slabs
				.iter()
				.any(|slab| self.evidence.contains_key(&(*slab, self.revision)))
			{
				self.authority.schedule_index_reconciliation();
			}
		}
	}
}

impl SlabManager {
	pub(super) fn has_orphaned_index_publications(&self) -> bool {
		self.orphaned_index_publications.load(Ordering::Acquire) != 0
	}

	pub(super) async fn begin_index_publication(
		&self,
		slabs: Vec<SlabId>,
	) -> std::io::Result<IndexPublication> {
		let gate = Arc::new(self.persistent_index_gate.clone().read_owned().await);
		self.begin_index_publication_with_gate(slabs, gate)
	}

	pub(super) fn begin_index_publication_with_gate(
		&self,
		mut slabs: Vec<SlabId>,
		gate: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	) -> std::io::Result<IndexPublication> {
		slabs.sort_unstable();
		slabs.dedup();
		let revision = self
			.index_publication_revision
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
				value.checked_add(1)
			})
			.map_err(|_| Error::other("persistent index revision exhausted"))?;
		for &slab in &slabs {
			self.pending_index_publications
				.insert((slab, revision), IndexPublicationState::InFlight);
		}
		Ok(IndexPublication {
			marks: self.pending_index_publications.clone(),
			orphaned: self.orphaned_index_publications.clone(),
			evidence: self.single_append_publication_evidence.clone(),
			slabs,
			revision,
			gate,
			authority: self.tier_authority.clone(),
			completed: false,
		})
	}

	/// Capture the exact pre-state before submitting the single-row transaction.
	pub(super) async fn begin_single_append_publication(
		&self,
		row: PersistentRecordRow,
		frame: &[u8],
	) -> std::io::Result<IndexPublication> {
		let publication = self
			.begin_index_publication(vec![row.location.slab_id])
			.await?;
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		let pre = index.record_row_at_key(row).await?;
		// A reserved new physical location must not overwrite a previous row.
		if pre.is_some() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"append record key already exists",
			));
		}
		publication.evidence.insert(
			(row.location.slab_id, publication.revision),
			SingleAppendEvidence {
				row,
				pre,
				frame_digest: crate::hash::content::digest(frame),
			},
		);
		Ok(publication)
	}

	/// Reconcile only verified post-commit single-row appends. Unknown kinds,
	/// mismatches and pre-commit outcomes stay fenced for restart recovery.
	#[cfg(test)]
	pub(super) async fn reconcile_single_append_publications(&self) -> std::io::Result<usize> {
		let authority = self.tier_authority.clone();
		let weak = authority.manager_weak();
		let epoch = authority.reconcile_epoch();
		let mut total = 0;
		for attempt in 0..4 {
			let (completed, retry) =
				Self::reconcile_single_append_publications_once(&weak, &authority, epoch).await?;
			total += completed;
			if !retry {
				break;
			}
			tokio::time::sleep(Duration::from_millis(20 << attempt)).await;
		}
		Ok(total)
	}

	pub(super) async fn reconcile_single_append_publications_once(
		weak: &std::sync::Weak<SlabManager>,
		authority: &Arc<tier_authority::TierAuthority>,
		epoch: u64,
	) -> std::io::Result<(usize, bool)> {
		let Some(manager) = weak.upgrade() else {
			return Ok((0, false));
		};
		if !authority.current(epoch) {
			return Ok((0, false));
		}
		let gate = manager.persistent_index_gate.clone();
		drop(manager);
		// A detached redb closure owns a read guard. This bounded writer drain
		// proves earlier closures finished; Tokio queues later readers behind it
		// for at most the timeout on this recovery attempt.
		let drain = match tokio::time::timeout(Duration::from_millis(250), gate.write_owned()).await
		{
			Ok(guard) => guard,
			Err(_) => return Ok((0, true)),
		};
		drop(drain);
		let Some(manager) = weak.upgrade() else {
			return Ok((0, false));
		};
		let candidates = manager
			.single_append_publication_evidence
			.iter()
			.map(|entry| (*entry.key(), *entry.value()))
			.collect::<Vec<_>>();
		drop(manager);
		let mut reconciled = 0;
		let mut retry = false;
		for (mark, evidence) in candidates {
			if !authority.current(epoch) {
				break;
			}
			let Some(manager) = weak.upgrade() else {
				break;
			};
			let tier_lock = manager.tier_publication_lock(mark.0);
			let gate = manager.persistent_index_gate.clone();
			drop(manager);
			let _tier_guard = match tokio::time::timeout(
				Duration::from_millis(250),
				tier_lock.lock_owned(),
			)
			.await
			{
				Ok(guard) => guard,
				Err(_) => {
					retry = true;
					continue;
				}
			};
			let _gate =
				match tokio::time::timeout(Duration::from_millis(250), gate.read_owned()).await {
					Ok(guard) => guard,
					Err(_) => {
						retry = true;
						continue;
					}
				};
			let Some(manager) = weak.upgrade() else {
				break;
			};
			if !authority.current(epoch)
				|| manager
					.pending_index_publications
					.get(&mark)
					.map(|state| *state.value())
					!= Some(IndexPublicationState::Orphaned)
				|| !manager.lsm_install_unresolved.is_empty()
			{
				continue;
			}
			let other_states = manager
				.pending_index_publications
				.iter()
				.filter(|entry| entry.key().0 == mark.0 && *entry.key() != mark)
				.map(|entry| *entry.value())
				.collect::<Vec<_>>();
			if other_states
				.iter()
				.any(|state| *state == IndexPublicationState::Orphaned)
			{
				continue;
			}
			if !other_states.is_empty() {
				retry = true;
				continue;
			}
			let Some(index) = manager.persistent_index.clone() else {
				continue;
			};
			let slab = {
				let Ok(state) = manager.state.try_lock() else {
					retry = true;
					continue;
				};
				if state.tiered.contains_key(&mark.0) {
					None
				} else if state.active.slab_id() == mark.0 {
					Some(state.active.clone())
				} else {
					state.sealed.get(&mark.0).map(|slab| slab.value().clone())
				}
			};
			drop(manager);
			let Some(slab) = slab else {
				continue;
			};
			let Some(actual) = index.record_row_at_key(evidence.row).await? else {
				// PRE does not prove the private slab index is unchanged.
				continue;
			};
			if evidence.pre.is_some()
				|| actual != evidence.row
				|| actual.location.placement != evidence.row.location.placement
				|| actual.location.partition != evidence.row.location.partition
			{
				continue;
			}
			let (header, frame) = slab.read_with_header(evidence.row.location).await?;
			if storage_multirow_index_entries_from_frame(&frame)?.is_some()
				|| header.record_class != crate::schema::record_class::STANDARD
				|| lsm_driver::verify_frame_names_version(
					&header,
					&frame,
					evidence.row.location,
					lsm_driver::ExpectedVersion {
						tenant_id: Some(evidence.row.tenant_id),
						key_identity: Some(evidence.row.key_identity),
					},
				)
				.is_err() || crate::hash::content::digest(&frame) != evidence.frame_digest
			{
				continue;
			}
			#[cfg(test)]
			{
				let hold = authority
					.reconcile_before_mutation_hold
					.lock()
					.unwrap()
					.clone();
				if let Some((entered, release)) = hold {
					entered.add_permits(1);
					let _ = release.acquire().await;
				}
			}
			let Some(manager) = weak.upgrade() else {
				break;
			};
			let completed = authority
				.reconcile_if_current(epoch, || {
					// Close takes the same authority lock before changing epoch or
					// closed. Compatibility and both mutations are one closed-checked
					// unit with no await between them.
					if manager
						.pending_index_publications
						.get(&mark)
						.map(|state| *state.value())
						!= Some(IndexPublicationState::Orphaned)
					{
						return false;
					}
					let key = (evidence.row.tenant_id, evidence.row.key_identity);
					let mut locality_index =
						recover_rwlock_write(&manager.locality_index, "locality_index");
					if manager
						.key_localities
						.get(&key)
						.is_some_and(|locality| *locality != evidence.row.routing_locality)
					{
						return false;
					}
					match manager.key_index.entry(key) {
						dashmap::mapref::entry::Entry::Vacant(entry) => {
							if slab
								.ensure_index_location(
									evidence.row.tenant_id,
									evidence.row.key_identity,
									evidence.row.location,
								)
								.is_err()
							{
								return false;
							}
							entry.insert(vec![evidence.row.location]);
						}
						dashmap::mapref::entry::Entry::Occupied(entry) => {
							let versions = entry.get();
							if versions.len() != 1
								|| versions[0] != evidence.row.location
								|| versions[0].placement != evidence.row.location.placement
								|| versions[0].partition != evidence.row.location.partition
							{
								return false;
							}
							if slab
								.ensure_index_location(
									evidence.row.tenant_id,
									evidence.row.key_identity,
									evidence.row.location,
								)
								.is_err()
							{
								return false;
							}
						}
					}
					manager
						.key_localities
						.insert(key, evidence.row.routing_locality);
					locality_index
						.entry(evidence.row.tenant_id)
						.or_default()
						.insert((evidence.row.routing_locality, evidence.row.key_identity));
					if manager
						.pending_index_publications
						.remove_if(&mark, |_, state| *state == IndexPublicationState::Orphaned)
						.is_some()
					{
						manager.single_append_publication_evidence.remove(&mark);
						manager
							.orphaned_index_publications
							.fetch_sub(1, Ordering::AcqRel);
						manager.tier_authority.notify_publication_change();
						true
					} else {
						false
					}
				})
				.unwrap_or(false);
			if completed {
				reconciled += 1;
			}
		}
		Ok((reconciled, retry))
	}

	pub(super) fn persistent_record_rows_snapshot(&self) -> Vec<PersistentRecordRow> {
		let mut rows = Vec::new();
		for entry in self.key_index.iter() {
			let (tenant_id, key_identity) = *entry.key();
			for location in entry.value().iter() {
				rows.push(self.persistent_record_row(tenant_id, key_identity, *location));
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

	pub(super) fn has_rows_for_slab(&self, slab_id: SlabId) -> bool {
		self.key_index.iter().any(|entry| {
			entry
				.value()
				.iter()
				.any(|location| location.slab_id == slab_id)
		})
	}

	pub(super) async fn tiered_pointers_snapshot(&self) -> Vec<TieredSlabPointer> {
		let state = self.state.lock().await;
		let mut tiered = state
			.tiered
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		tiered.sort_by_key(|pointer| pointer.slab_id);
		tiered
	}

	pub(super) async fn rebuild_persistent_index(&self) -> std::io::Result<()> {
		let Some(index) = &self.persistent_index else {
			return Ok(());
		};
		let _gate = self.persistent_index_gate.clone().write_owned().await;
		if !self.lsm_install_unresolved.is_empty() || !self.pending_index_publications.is_empty() {
			return Err(stale_location_error(
				SlabId::ZERO,
				StaleLocationReason::Fenced,
			));
		}
		let rows = self.persistent_record_rows_snapshot();
		let tiered = self.tiered_pointers_snapshot().await;
		index.rebuild(rows, tiered, None).await
	}

	pub(super) async fn mirror_persistent_update(
		&self,
		update: PersistentIndexUpdate,
		operation: &'static str,
	) {
		let Some(index) = &self.persistent_index else {
			return;
		};
		if let Err(err) = index.apply(update.clone()).await {
			tracing::warn!(
				error = %err,
				dir = %self.config.dir.display(),
				operation,
				"redb delta update failed; rebuilding from authoritative storage state"
			);
			if !self.lsm_install_unresolved.is_empty() {
				self.index_authority_unavailable
					.store(true, Ordering::Release);
				tracing::warn!(
					"redb delta failed during an unresolved LSM install; fencing instead of rebuilding under tier locks"
				);
				return;
			}
			let _index_guard = self.persistent_index_gate.clone().write_owned().await;
			if !self.lsm_install_unresolved.is_empty()
				|| !self.pending_index_publications.is_empty()
			{
				tracing::warn!("persistent index rebuild fenced by unresolved publication");
				return;
			}
			let rows = self.persistent_record_rows_snapshot();
			let tiered = self.tiered_pointers_snapshot().await;
			#[cfg(test)]
			self.pause_rebuild_snapshot_for_test().await;
			if let Err(rebuild_err) = index
				.rebuild(rows, tiered, update.last_control_log_index)
				.await
			{
				tracing::warn!(
					error = %rebuild_err,
					dir = %self.config.dir.display(),
					operation,
					"failed to rebuild redb storage index snapshot"
				);
			}
		}
	}

	/// A local slab may be unlinked only after the durable index agrees that
	/// no published record or secondary-index source still names it. Runtime
	/// `key_index` alone is insufficient: it can already have advanced past a
	/// source while a failed mirror leaves recovery pointing at that source.
	///
	/// A mismatch preserves the slab, quarantines further retirement work for
	/// it, and leaves repair to an explicit future operator decision.
	pub(super) async fn can_retire_local_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		self.require_tier_authority_ready()?;
		let Some(index) = &self.persistent_index else {
			return Ok(true);
		};
		let snapshot = index.load_snapshot().await?;
		let record_references = snapshot
			.record_rows
			.iter()
			.chain(snapshot.tiered_records.iter())
			.filter(|row| row.location.slab_id == slab_id)
			.count();
		let secondary_references = snapshot
			.secondary_index_segments
			.iter()
			.filter(|row| row.key.source_slab_id == slab_id)
			.count();
		if record_references == 0 && secondary_references == 0 {
			return Ok(true);
		}

		self.quarantined_compaction_slabs.insert(slab_id);
		tracing::error!(
			slab_id = slab_id.get(),
			record_references,
			secondary_references,
			"refusing to retire slab with durable published references"
		);
		Ok(false)
	}

	pub(super) async fn persist_record_row(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		location: RecordLocation,
		publication: &IndexPublication,
	) -> std::io::Result<()> {
		let row = self.persistent_record_row(tenant_id, key_identity, location);
		self.persist_record_rows_strict(vec![row], publication)
			.await
	}

	pub(super) async fn persist_record_row_strict(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		location: RecordLocation,
		publication: &IndexPublication,
	) -> std::io::Result<()> {
		let row = PersistentRecordRow {
			tenant_id,
			key_identity,
			routing_locality,
			table_object_id: None,
			placement: location.placement,
			partition: location.partition,
			location,
		};
		self.persist_record_rows_strict(vec![row], publication)
			.await
	}

	pub(super) async fn persist_record_rows_strict(
		&self,
		rows: Vec<PersistentRecordRow>,
		publication: &IndexPublication,
	) -> std::io::Result<()> {
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		index
			.apply_with_publication_guard(
				PersistentIndexUpdate {
					put_records: rows,
					..Default::default()
				},
				Some(publication.gate.clone()),
			)
			.await
	}

	#[cfg(test)]
	pub(super) async fn persist_tiered_pointer_strict(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		let publication = self.begin_index_publication(vec![pointer.slab_id]).await?;
		self.persist_tiered_pointer_guarded(pointer, last_control_log_index, None, &publication)
			.await?;
		publication.complete();
		Ok(())
	}

	pub(super) async fn persist_tiered_pointer_guarded(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		tier_republication_index: Option<u64>,
		publication: &IndexPublication,
	) -> std::io::Result<()> {
		self.persist_tiered_pointer_with_metadata_token(
			pointer,
			last_control_log_index,
			tier_republication_index,
			None,
			publication,
		)
		.await
	}

	pub(super) async fn persist_tiered_pointer_with_metadata_token(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		tier_republication_index: Option<u64>,
		completed_metadata_token: Option<(SlabId, crate::storage::index::MetadataKind, u64)>,
		publication: &IndexPublication,
	) -> std::io::Result<()> {
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		index
			.apply_with_publication_guard(
				PersistentIndexUpdate {
					put_tiered_slabs: vec![pointer],
					completed_metadata_token,
					tier_republication_index,
					last_control_log_index,
					..Default::default()
				},
				Some(publication.gate.clone()),
			)
			.await
	}

	#[cfg(test)]
	pub(super) async fn delete_tiered_slab_strict(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<Option<IndexPublication>> {
		let gate = Arc::new(self.persistent_index_gate.clone().read_owned().await);
		self.delete_tiered_slab_strict_with_gate(slab_id, last_control_log_index, gate)
			.await
	}

	pub(super) async fn delete_tiered_slab_strict_with_gate(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
		gate: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
	) -> std::io::Result<Option<IndexPublication>> {
		let Some(index) = &self.persistent_index else {
			return Ok(None);
		};
		let publication = self.begin_index_publication_with_gate(vec![slab_id], gate)?;
		let expected = self
			.state
			.lock()
			.await
			.tiered
			.get(&slab_id)
			.map(|entry| entry.value().clone());
		let known_bytes = expected
			.as_ref()
			.and_then(|pointer| self.snapshot_retained_size_hint(slab_id, &pointer.l2_uri));
		let evidence = expected
			.as_ref()
			.map(|pointer| self.snapshot_retirement_evidence(slab_id, &pointer.l2_uri))
			.unwrap_or_default();
		#[cfg(test)]
		self.pause_retirement_snapshot_for_test().await;
		let retired_uri = expected.as_ref().map(|pointer| pointer.l2_uri.clone());
		let (retirement, resolved_mark) = index
			.retire_tiered_slab_checked(
				slab_id,
				expected,
				known_bytes,
				evidence,
				last_control_log_index,
				self.capture_snapshot_epoch(),
				publication.gate.clone(),
			)
			.await?;
		if let Some(mark) = resolved_mark {
			self.tier_authority
				.clear_snapshot_retirement(slab_id, mark.epoch);
		}
		if let Some((retained, evidence)) = retirement {
			self.note_snapshot_retirement_applied(
				slab_id,
				retired_uri.expect("retirement pointer"),
				retained,
				evidence,
			);
		}
		Ok(Some(publication))
	}
}
