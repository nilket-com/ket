//! Durable, conservative tier metadata observations.
use super::*;

impl SlabManager {
	pub(super) fn latest_location_before(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
	) -> Option<RecordLocation> {
		self.key_index
			.get(&(tenant_id, key_identity))
			.and_then(|versions| {
				versions
					.iter()
					.filter(|location| location.hlc < hlc)
					.max_by_key(|location| location.hlc)
					.copied()
			})
	}

	pub(super) async fn record_supersession_for_slab_at_epoch(
		&self,
		slab_id: SlabId,
		hlc: HybridTimestamp,
		index: Option<u64>,
		epoch: u64,
		token: Option<u64>,
	) -> std::io::Result<bool> {
		let kind = crate::storage::index::MetadataKind::Supersession;
		let result = self
			.update_tier_pointer_metadata_with_token(
				slab_id,
				index,
				Some(epoch),
				token.map(|token| (kind, token)),
				|pointer| pointer.record_supersession_hlc(hlc),
			)
			.await;
		if !matches!(result, Ok(true)) {
			if let Some(token) = token {
				self.queue_completed_metadata_retry(slab_id, kind, token);
			}
		}
		result
	}

	pub(super) async fn record_row_tombstone_for_slab_at_epoch(
		&self,
		slab_id: SlabId,
		hlc: HybridTimestamp,
		index: Option<u64>,
		epoch: u64,
		token: Option<u64>,
	) -> std::io::Result<bool> {
		let kind = crate::storage::index::MetadataKind::Tombstone;
		let result = self
			.update_tier_pointer_metadata_with_token(
				slab_id,
				index,
				Some(epoch),
				token.map(|token| (kind, token)),
				|pointer| pointer.record_tombstone_hlc(hlc),
			)
			.await;
		if !matches!(result, Ok(true)) {
			if let Some(token) = token {
				self.queue_completed_metadata_retry(slab_id, kind, token);
			}
		}
		result
	}

	/// Reserve durable conservative metadata before a row can become visible.
	pub(super) async fn begin_pending_metadata_operation(
		&self,
		slab_id: SlabId,
		kind: crate::storage::index::MetadataKind,
	) -> std::io::Result<Option<u64>> {
		if self.tier_pointer(slab_id).await.is_none()
			&& !self.tier_authority.pending.contains_key(&slab_id)
		{
			return Ok(None);
		}
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for metadata intent",
			)
		})?;
		#[cfg(test)]
		self.metadata_token_before_allocation_pause_point().await;
		let token = index.begin_metadata_token(slab_id, kind).await?;
		#[cfg(test)]
		self.metadata_token_committed_pause_point().await;
		let mut entry = self.pending_metadata_tokens.entry(slab_id).or_default();
		match kind {
			crate::storage::index::MetadataKind::Supersession => {
				entry.supersession.insert(token);
			}
			crate::storage::index::MetadataKind::Tombstone => {
				entry.tombstone.insert(token);
			}
		}
		drop(entry);
		#[cfg(test)]
		self.metadata_token_batch_pause_point().await;
		Ok(Some(token))
	}

	#[cfg(test)]
	pub(crate) async fn stage_pending_metadata_for_test(
		&self,
		slab_id: SlabId,
		kind: crate::storage::index::MetadataKind,
	) -> std::io::Result<Option<u64>> {
		self.begin_pending_metadata_operation(slab_id, kind).await
	}

	fn finish_pending_metadata_operation(
		&self,
		slab_id: SlabId,
		kind: crate::storage::index::MetadataKind,
		token: u64,
	) {
		if let dashmap::mapref::entry::Entry::Occupied(mut entry) =
			self.pending_metadata_tokens.entry(slab_id)
		{
			let pending = entry.get_mut();
			match kind {
				crate::storage::index::MetadataKind::Supersession => {
					pending.supersession.remove(&token);
				}
				crate::storage::index::MetadataKind::Tombstone => {
					pending.tombstone.remove(&token);
				}
			}
			if pending.supersession.is_empty() && pending.tombstone.is_empty() {
				entry.remove();
			}
		}
	}

	fn queue_completed_metadata_retry(
		&self,
		slab_id: SlabId,
		kind: crate::storage::index::MetadataKind,
		token: u64,
	) {
		self.completed_metadata_retries
			.insert((slab_id, token), kind);
		self.tier_authority.wake_completed_metadata();
	}

	pub(super) async fn retry_completed_metadata_operations(&self) -> bool {
		let pending = self
			.completed_metadata_retries
			.iter()
			.map(|entry| (*entry.key(), *entry.value()))
			.collect::<Vec<_>>();
		let mut retry_needed = false;
		for ((slab, token), kind) in pending {
			if self.tier_pointer(slab).await.is_none()
				&& !self.tier_authority.pending.contains_key(&slab)
			{
				// No pointer exists to update. Keep the durable token; a later tier
				// publication or reopen will make progress without periodic polling.
				continue;
			}
			let result = self
				.update_tier_pointer_metadata_with_token(
					slab,
					None,
					None,
					Some((kind, token)),
					|pointer| match kind {
						crate::storage::index::MetadataKind::Supersession => {
							pointer.supersession_metadata = SupersessionMetadata::Untracked
						}
						crate::storage::index::MetadataKind::Tombstone => {
							pointer.tombstone_metadata = TombstoneMetadata::Untracked
						}
					},
				)
				.await;
			if matches!(result, Ok(true)) {
				self.completed_metadata_retries.remove(&(slab, token));
			} else {
				retry_needed = true;
			}
		}
		retry_needed
	}

	/// Control observers update authority even while data answers are fenced.
	/// The publication lock excludes a late install of older metadata.
	pub(super) async fn update_tier_pointer_metadata(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
		observed_epoch: Option<u64>,
		update: impl FnOnce(&mut TieredSlabPointer) + Send,
	) -> std::io::Result<bool> {
		self.update_tier_pointer_metadata_with_token(
			slab_id,
			last_control_log_index,
			observed_epoch,
			None,
			update,
		)
		.await
	}

	async fn update_tier_pointer_metadata_with_token(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
		observed_epoch: Option<u64>,
		metadata_token: Option<(crate::storage::index::MetadataKind, u64)>,
		update: impl FnOnce(&mut TieredSlabPointer) + Send,
	) -> std::io::Result<bool> {
		if metadata_token.is_some() && self.snapshot_install_in_flight() {
			return Ok(false);
		}
		let control_epoch = observed_epoch.unwrap_or_else(|| self.capture_snapshot_epoch());
		let lock = self.tier_publication_lock(slab_id);
		let _publication = lock.lock().await;
		let pending = self
			.tier_authority
			.pending
			.get(&slab_id)
			.map(|entry| entry.value().clone());
		if last_control_log_index
			.zip(
				pending
					.as_ref()
					.and_then(|entry| entry.last_control_log_index),
			)
			.is_some_and(|(incoming, known)| incoming < known)
		{
			return Ok(false);
		}

		let pointer = if let Some(pending) = &pending {
			Some(pending.pointer.clone())
		} else {
			self.state
				.lock()
				.await
				.tiered
				.get(&slab_id)
				.map(|entry| entry.value().clone())
		};
		let Some(mut pointer) = pointer else {
			return Ok(false);
		};
		let before = pointer.clone();
		update(&mut pointer);
		if pointer == before && metadata_token.is_none() {
			return Ok(false);
		}
		let publication = if metadata_token.is_some() {
			let Ok(gate) = self.persistent_index_gate.clone().try_read_owned() else {
				return Ok(false);
			};
			self.begin_index_publication_with_gate(vec![slab_id], Arc::new(gate))?
		} else {
			self.begin_index_publication(vec![slab_id]).await?
		};
		if self.observer_is_superseded(last_control_log_index) {
			publication.complete();
			return Ok(false);
		}
		if ((last_control_log_index.is_none() || observed_epoch.is_some())
			&& !self.snapshot_epoch_is_current(control_epoch))
			|| self.snapshot_install_in_flight()
		{
			if let Some((kind, _)) = metadata_token {
				if self.snapshot_install_in_flight() {
					publication.complete();
					return Ok(false);
				}
				match kind {
					crate::storage::index::MetadataKind::Supersession => {
						pointer.supersession_metadata = SupersessionMetadata::Untracked
					}
					crate::storage::index::MetadataKind::Tombstone => {
						pointer.tombstone_metadata = TombstoneMetadata::Untracked
					}
				}
			} else {
				// An obsolete local observation cannot become unapplied committed
				// authority over the newer snapshot. A log callback still defers.
				if last_control_log_index.is_some() {
					self.unapplied_tier_pointers.insert(
						slab_id,
						(pointer.clone(), Some(StaleLocationReason::Fenced), None),
					);
				}
				publication.complete();
				return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
			}
		}
		if let Some((kind, token)) = metadata_token {
			if let Some(other) = self.pending_metadata_tokens.get(&slab_id) {
				if other.supersession.iter().any(|t| {
					!matches!(kind, crate::storage::index::MetadataKind::Supersession)
						|| *t != token
				}) {
					pointer.supersession_metadata = SupersessionMetadata::Untracked;
				}
				if other.tombstone.iter().any(|t| {
					!matches!(kind, crate::storage::index::MetadataKind::Tombstone) || *t != token
				}) {
					pointer.tombstone_metadata = TombstoneMetadata::Untracked;
				}
			}
		}
		if let Err(error) = self.bump_slab_publication_revision(slab_id) {
			// No durable write has been invoked; this refusal is complete.
			publication.complete();
			return Err(error);
		}
		if pending.is_some() {
			// Invalidate before the durable await. Cancellation must not leave a
			// running flight with permission to overwrite this committed update.
			if let Err(error) =
				self.update_pending_tier_authority(pointer.clone(), last_control_log_index)
			{
				publication.complete();
				return Err(error);
			}
		} else {
			self.state
				.lock()
				.await
				.tiered
				.insert(slab_id, pointer.clone());
		}
		// The pointer and its applied-control index are one durable update;
		// this does not claim the unverified row mirror is complete.
		self.persist_tiered_pointer_with_metadata_token(
			pointer.clone(),
			last_control_log_index,
			None,
			metadata_token.map(|(kind, token)| (slab_id, kind, token)),
			&publication,
		)
		.await?;
		#[cfg(test)]
		if metadata_token.is_some() {
			self.metadata_pointer_committed_pause_point().await;
		}
		if let Some((kind, token)) = metadata_token {
			self.finish_pending_metadata_operation(slab_id, kind, token);
		}
		publication.complete();
		self.clear_tiered_stats_cache_for_slab(slab_id);
		Ok(true)
	}

	pub(super) async fn record_migrated_range_tombstone_impl(
		&self,
		tombstone: &MigratedRangeTombstone,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<usize> {
		let control_epoch = self.capture_snapshot_epoch();
		let affected = if self.tier_authority_is_ready() {
			self.migrated_tombstone_affected_tiered_slabs(tombstone)
		} else {
			// There is no complete range census before verification. Metadata
			// here only disables slab-wide shortcuts; the actual range predicate
			// still filters rows. Tier slabs are immutable, so conservative marking
			// cannot hide unrelated rows or change with a later append.
			self.tier_authority_snapshot()
				.await
				.into_iter()
				.map(|entry| entry.pointer.slab_id)
				.collect()
		};
		let mut changed = 0;
		for slab_id in affected {
			if !self.snapshot_epoch_is_current(control_epoch)
				&& self.observer_is_superseded(last_control_log_index)
			{
				return Ok(changed);
			}
			if !self.snapshot_epoch_is_current(control_epoch) && last_control_log_index.is_none() {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"unindexed migrated tombstone observation became stale",
				));
			}
			let token = if last_control_log_index.is_some() {
				self.begin_pending_metadata_operation(
					slab_id,
					crate::storage::index::MetadataKind::Tombstone,
				)
				.await?
			} else {
				None
			};
			#[cfg(test)]
			self.append_metadata_observed_pause_point().await;
			changed += usize::from(
				self.record_row_tombstone_for_slab_at_epoch(
					slab_id,
					tombstone.completed_at_hlc,
					last_control_log_index,
					control_epoch,
					token,
				)
				.await?,
			);
		}
		if !self.snapshot_epoch_is_current(control_epoch)
			&& last_control_log_index.is_none()
			&& !self.observer_is_superseded(last_control_log_index)
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"unindexed migrated tombstone observation became stale",
			));
		}
		Ok(changed)
	}
}
