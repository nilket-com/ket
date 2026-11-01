//! Logical version identity, by record class.
//!
//! An ordinary row version is named by `(tenant, key, hlc)`; a counter
//! tentative by `(tenant, key, hlc, origin)`. Byte-identical copies are the
//! version. A different payload under the same identity is a conflict: a
//! producer refuses it, recovery records every conflicting copy so reads refuse
//! it, and rewrites never prune or merge it. Persisted carriers of a physical
//! location (secondary-index segments) are resolved by identity, never by the
//! saved offset.
use super::tiered_io::TieredRowRefreshPlan;
use super::*;

/// The payload a producer is about to publish at an existing identity.
pub(super) enum IncomingVersion<'a> {
	/// A single-record frame.
	Record(&'a [u8]),
	/// One row of a multi-row frame.
	Row { batch: &'a RecordBatch, row: usize },
	/// A counter tentative, identified with its origin.
	Counter { value: u64, origin: NodeId },
}

/// Whether row `left_row` of `left` and row `right_row` of `right` carry the
/// same uniquely named columns and equal row values, independent of column order.
pub(super) fn same_row(
	left: &RecordBatch,
	left_row: usize,
	right: &RecordBatch,
	right_row: usize,
) -> bool {
	if left_row >= left.num_rows() || right_row >= right.num_rows() {
		return false;
	}
	let left_schema = left.schema();
	let right_schema = right.schema();
	let left_fields = left_schema
		.fields()
		.iter()
		.map(|field| field.name())
		.collect::<std::collections::BTreeSet<_>>();
	let right_fields = right_schema
		.fields()
		.iter()
		.map(|field| field.name())
		.collect::<std::collections::BTreeSet<_>>();
	if left_fields != right_fields
		|| left_fields.len() != left.num_columns()
		|| right_fields.len() != right.num_columns()
	{
		return false;
	}
	right_schema
		.fields()
		.iter()
		.enumerate()
		.all(|(index, field)| {
			left.column_by_name(field.name()).is_some_and(|stored| {
				stored.data_type() == field.data_type()
					&& stored.slice(left_row, 1).as_ref()
						== right.column(index).slice(right_row, 1).as_ref()
			})
		})
}

impl SlabManager {
	/// A rewrite keeps every copy of a conflicting version.
	pub(super) fn keep_conflicting_versions(
		&self,
		records: &mut [super::compaction::CompactionRecord],
	) {
		for record in records.iter_mut() {
			if record.reclaim && self.conflicted_locations.contains(&record.old_location) {
				record.reclaim = false;
			}
		}
	}

	/// Every indexed location of `(tenant, key)` at exactly `hlc`.
	pub(super) fn same_hlc_locations(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
	) -> std::io::Result<Vec<RecordLocation>> {
		self.require_tier_authority_ready()?;
		Ok(self
			.key_index
			.get(&(tenant_id, key_identity))
			.map(|chain| {
				chain
					.iter()
					.filter(|location| location.hlc == hlc)
					.copied()
					.collect()
			})
			.unwrap_or_default())
	}

	/// Refuse a payload that differs from an indexed version with the same
	/// identity. Returns how many same-HLC locations were checked, for the
	/// re-check at index time.
	pub(super) async fn refuse_conflicting_version(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		incoming: IncomingVersion<'_>,
	) -> std::io::Result<usize> {
		self.require_tier_authority_ready()?;
		let candidates = self.same_hlc_locations(tenant_id, key_identity, hlc)?;
		for candidate in &candidates {
			let stored = self.read(*candidate).await?;
			let same = match &incoming {
				IncomingVersion::Counter { value, origin } => {
					match super::super::counter::decode_origin_frame(&stored) {
						// Another origin is another counter version.
						Ok((stored_value, stored_origin)) if stored_origin == *origin => {
							stored_value == *value
						}
						Ok(_) => true,
						Err(_) => false,
					}
				}
				IncomingVersion::Record(bytes) => stored.as_slice() == *bytes,
				IncomingVersion::Row { batch, row } => match decode_record_batch(&stored) {
					Ok(stored_batch) => same_row(
						&stored_batch,
						candidate.record_row_index.get() as usize,
						batch,
						*row,
					),
					Err(_) => false,
				},
			};
			if !same {
				metrics().observe_storage_version_conflict_refused();
				return Err(stale_location_error(
					candidate.slab_id,
					StaleLocationReason::ConflictingVersion,
				));
			}
		}
		Ok(candidates.len())
	}

	/// Index an appended version, unless a same-HLC location appeared after the
	/// producer's check: under the key's guard, the number of same-HLC
	/// locations must still equal what the check compared against.
	pub(super) fn index_checked_version(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		location: RecordLocation,
		checked_same_hlc: usize,
	) -> std::io::Result<()> {
		let mut raced = false;
		self.key_index
			.entry((tenant_id, key_identity))
			.and_modify(|versions| {
				let same_hlc = versions
					.iter()
					.filter(|stored| stored.hlc == location.hlc)
					.count();
				if same_hlc > checked_same_hlc {
					raced = true;
				} else {
					insert_version_sorted(versions, location);
				}
			})
			.or_insert_with(|| vec![location]);
		if raced {
			metrics().observe_storage_version_conflict_refused();
			return Err(stale_location_error(
				location.slab_id,
				StaleLocationReason::ConflictingVersion,
			));
		}
		self.insert_locality_membership_with_locality(tenant_id, key_identity, routing_locality);
		Ok(())
	}

	/// At open: compare every group of same-HLC copies by class identity and
	/// record each copy of a conflicting identity. Rebuilt at every open from
	/// slab bytes and tiered rows, so nothing is persisted. An unreadable copy
	/// leaves the identity unresolved; it is never omitted from the comparison.
	pub(super) fn version_conflict_groups_for_recovery(&self) -> Vec<Vec<RecordLocation>> {
		self.key_index
			.iter()
			.flat_map(|entry| {
				let mut by_hlc: BTreeMap<HybridTimestamp, Vec<RecordLocation>> = BTreeMap::new();
				for location in entry.value() {
					by_hlc.entry(location.hlc).or_default().push(*location);
				}
				by_hlc
					.into_values()
					.filter(|group| group.len() > 1)
					.collect::<Vec<_>>()
			})
			.collect::<Vec<_>>()
	}

	pub(super) async fn recover_version_conflicts(&self) -> std::io::Result<()> {
		for group in self.version_conflict_groups_for_recovery() {
			let mut payloads = Vec::with_capacity(group.len());
			for location in &group {
				match self.read_recovery_frame(*location).await {
					Ok((header, bytes)) => payloads.push((*location, header, bytes)),
					Err(error) if error.kind() == ErrorKind::Unsupported => return Err(error),
					Err(_) => {
						return Err(stale_location_error(
							location.slab_id,
							StaleLocationReason::AmbiguousVersion,
						));
					}
				}
			}
			self.record_recovered_version_conflicts(&group, payloads)?;
		}
		Ok(())
	}

	pub(super) fn record_recovered_version_conflicts(
		&self,
		group: &[RecordLocation],
		payloads: Vec<(RecordLocation, SlabRecordHeader, Vec<u8>)>,
	) -> std::io::Result<()> {
		// Class is part of the version's meaning, not a namespace that
		// permits two meanings at one (tenant, key, HLC). Once classes
		// agree, only a counter's origin divides that identity further.
		if let Some((_, first, _)) = payloads.first()
			&& payloads
				.iter()
				.any(|(_, header, _)| header.record_class != first.record_class)
		{
			metrics().observe_storage_version_conflict_recovered(group.len() as u64);
			for location in group {
				self.conflicted_locations.insert(*location);
			}
			return Ok(());
		}
		// Group by class identity: counter tentatives by origin, everything
		// else by the triple alone.
		let mut identities: BTreeMap<Option<u32>, Vec<(RecordLocation, Vec<u8>)>> = BTreeMap::new();
		for (location, header, bytes) in payloads {
			let origin = match header.record_class {
				crate::schema::record_class::STANDARD => None,
				crate::schema::record_class::COUNTER_TENTATIVE_V2 => {
					Some(super::super::counter::decode_origin_frame(&bytes)?.1.id)
				}
				_ => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"an indexed version has a non-version record class",
					));
				}
			};
			identities
				.entry(origin)
				.or_default()
				.push((location, bytes));
		}
		for copies in identities.into_values() {
			let Some((first_location, first)) = copies.first() else {
				continue;
			};
			let first_batch = decode_record_batch(first).ok();
			let conflicting = copies.iter().skip(1).any(|(location, bytes)| {
				match (&first_batch, decode_record_batch(bytes).ok()) {
					(Some(first_batch), Some(batch)) => !same_row(
						&batch,
						location.record_row_index.get() as usize,
						first_batch,
						first_location.record_row_index.get() as usize,
					),
					_ => bytes != first,
				}
			});
			if conflicting {
				metrics().observe_storage_version_conflict_recovered(copies.len() as u64);
				for (location, _) in &copies {
					self.conflicted_locations.insert(*location);
				}
			}
		}
		Ok(())
	}

	/// The current location of a persisted carrier's version: resolved by
	/// `(tenant, key, hlc)` against the key's chain. The saved location is used
	/// only to name the slab in a refusal.
	pub(crate) fn resolve_logical_version(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		saved: RecordLocation,
	) -> std::io::Result<RecordLocation> {
		self.require_tier_authority_ready()?;
		let candidates = self.same_hlc_locations(tenant_id, key_identity, hlc)?;
		let Some(current) = candidates.last().copied() else {
			return Err(stale_location_error(
				saved.slab_id,
				StaleLocationReason::VersionGone,
			));
		};
		if candidates
			.iter()
			.any(|location| self.conflicted_locations.contains(location))
		{
			return Err(stale_location_error(
				saved.slab_id,
				StaleLocationReason::ConflictingVersion,
			));
		}
		Ok(current)
	}

	/// Resolve secondary-index lookup rows to their versions' current locations.
	pub(crate) fn resolve_secondary_index_rows(
		&self,
		tenant_id: TenantId,
		rows: &[crate::storage::SecondaryIndexLookupRow],
	) -> std::io::Result<Vec<crate::storage::SecondaryIndexLookupRow>> {
		self.require_tier_authority_ready()?;
		rows.iter()
			.map(|row| {
				let mut resolved = row.clone();
				resolved.record_location = self.resolve_logical_version(
					tenant_id,
					row.row_key_id,
					row.row_hlc,
					row.record_location,
				)?;
				Ok(resolved)
			})
			.collect()
	}

	/// Persist a tier pointer with its row delta in one transaction, publish
	/// the next generation history with the pointer, and relocate the index.
	/// The caller holds the slab's tier publication lock.
	pub(super) async fn publish_tier_generation(
		&self,
		pointer: &TieredSlabPointer,
		plan: &TieredRowRefreshPlan,
		last_control_log_index: Option<u64>,
		authority_epoch: u64,
	) -> std::io::Result<()> {
		let _cleanup_guard = self.cleanup_claim(pointer.slab_id).lock_owned().await;
		if let Some(error) = self.tier_authority.integrity_error() {
			return Err(error);
		}
		let _scope = self.lsm_intent_scope_gate.read().await;
		let touched = plan
			.old_rows
			.iter()
			.chain(&plan.new_rows)
			.cloned()
			.collect::<Vec<_>>();
		let own_dirty = self
			.pending_index_publications
			.iter()
			.any(|entry| entry.key().0 == pointer.slab_id);
		if self.index_authority_unavailable.load(Ordering::Acquire)
			|| own_dirty
			|| self
				.check_tier_publication_lsm_scope(pointer.slab_id, &touched)
				.await
				.is_err()
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				StaleLocation {
					slab_id: pointer.slab_id,
					reason: StaleLocationReason::Fenced,
				},
			));
		}
		let (current, base) = {
			let state = self.state.lock().await;
			(
				state
					.tier_layouts
					.get(&pointer.slab_id)
					.map(|layout| layout.value().clone()),
				self.tier_publication_base(&state, pointer.slab_id),
			)
		};
		let next = Arc::new(self.build_observed_tier_layout(
			&current.unwrap_or_default(),
			pointer,
			&base,
			plan,
		)?);
		self.bump_slab_publication_revision(pointer.slab_id)?;
		let publication = self.begin_index_publication(vec![pointer.slab_id]).await?;
		if !self.tier_authority.current(authority_epoch) {
			publication.complete();
			return Err(stale_location_error(
				pointer.slab_id,
				StaleLocationReason::Fenced,
			));
		}
		let index = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?;
		index
			.apply_with_publication_guard(
				PersistentIndexUpdate {
					put_tiered_slabs: vec![pointer.clone()],
					tier_republication_index: last_control_log_index,
					delete_records: plan.old_rows.clone(),
					put_records: plan.new_rows.clone(),
					last_control_log_index,
					..Default::default()
				},
				Some(publication.gate.clone()),
			)
			.await?;
		{
			let state = self.state.lock().await;
			state.tiered.insert(pointer.slab_id, pointer.clone());
			state.tier_layouts.insert(pointer.slab_id, next);
		}
		// The durable transaction succeeded. Mirror it under the publication
		// gate even if newer authority invalidated this verification flight.
		self.apply_tiered_row_refresh_plan_locked(plan.clone(), false)
			.await;
		// These mirrors are bounded by the committed index; they cannot
		// clear authority from a newer snapshot while this flight finishes.
		if let Some(index) = last_control_log_index {
			self.tier_authority
				.cancel_snapshot_retirement_before_control(pointer.slab_id, index);
			if self.require_cleanup_authority().is_ok() {
				self.note_tier_republished(pointer.slab_id, &pointer.l2_uri, index);
			}
		}
		publication.complete();
		if !self.tier_authority.current(authority_epoch) {
			return Err(stale_location_error(
				pointer.slab_id,
				StaleLocationReason::Fenced,
			));
		}
		Ok(())
	}
}

/// ADR 0090: placement resolution helpers that live outside the storage-engine
/// trait surface.
impl SlabManager {
	/// ADR 0090: the placement authority for one row version. Durable carriers
	/// that predate placement — the V1 secondary-index segment format — decode
	/// their locations as raw-locality, so a reader holding such a location
	/// must resolve the version's true placement from the key chain rather
	/// than trusting the segment's reconstruction.
	pub(crate) fn version_placement_at(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
	) -> std::io::Result<Option<(PlacementSemantics, Option<crate::schema::PartitionId>)>> {
		self.require_tier_authority_ready()?;
		let Some(versions) = self.key_index.get(&(tenant_id, key_identity)) else {
			return Ok(None);
		};
		Ok(versions
			.value()
			.iter()
			.find(|location| location.hlc == hlc)
			.map(|location| (location.placement, location.partition)))
	}
}

impl SlabManager {
	/// Idempotent migration ingest (Plan 019).
	///
	/// Distinct from [`StorageEngine::append`] so the correctness
	/// boundary for lazy migration lives at a dedicated API rather
	/// than piggybacking on the hot write path:
	///
	/// * **Fast path** — if a version with the same
	///   `(key_identity, hlc)`
	///   already exists locally, read its frame, blake3 it, and
	///   compare to `payload_hash`. A match is a no-op and returns
	///   [`MigrationIngestOutcome::Duplicate`]. A mismatch is a
	///   fatal invariant violation: two nodes claim different bytes
	///   for the same MVCC slot, which would make MVCC history
	///   non-deterministic if silently accepted.
	/// * **Slow path** — otherwise append the frame through the
	///   same slab machinery the hot write path uses and return
	///   [`MigrationIngestOutcome::Inserted`]. The append is indexed only
	///   if no same-HLC version appeared after the check, as for every
	///   producer.
	///
	/// The `migration_id` argument is currently only used for
	/// diagnostic framing in the mismatch error — retained so the
	/// signature stays stable once the migration registry and
	/// sweeper surfaces wire it through.
	pub async fn append_migration(
		&self,
		migration_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		payload_hash: [u8; 32],
		frame: &[u8],
	) -> std::io::Result<MigrationIngestOutcome> {
		self.require_tier_authority_ready()?;
		self.append_migration_with_locality(
			migration_id,
			tenant_id,
			key_identity,
			Locality::from(key_identity),
			hlc,
			payload_hash,
			frame,
		)
		.await
	}

	/// Idempotent migration ingest for records whose routing
	/// locality is not the identity projection. Table-spread rows
	/// use this path so range migration preserves their table arc
	/// placement across replica-set movement.
	pub async fn append_migration_with_locality(
		&self,
		migration_id: u64,
		tenant_id: TenantId,
		key_identity: Identity,
		routing_locality: Locality,
		hlc: HybridTimestamp,
		payload_hash: [u8; 32],
		frame: &[u8],
	) -> std::io::Result<MigrationIngestOutcome> {
		self.require_tier_authority_ready()?;
		self.counter_unsupported.insert((tenant_id, key_identity));
		self.counter_eligible.refuse(
			&(tenant_id, key_identity),
			super::counter::CounterVisibilityError::Unsupported,
		);
		// Fast-path collision check before touching disk: walk the
		// key's existing version chain under a short-lived read
		// guard and look for an exact HLC match. DashMap's read
		// guard is cheap and the chain is O(versions-per-key),
		// which is bounded by retention.
		// Every same-HLC location is compared, not only the first: the version
		// is a duplicate only if each existing copy is byte-identical.
		let existing = self.same_hlc_locations(tenant_id, key_identity, hlc)?;
		let mut stored_hash = payload_hash;
		for location in &existing {
			let stored = self.read(*location).await?;
			let hash = crate::hash::content::digest(&stored);
			if hash != payload_hash {
				stored_hash = hash;
				break;
			}
		}
		if let Some(location) = existing.first().copied() {
			if stored_hash == payload_hash {
				self.insert_locality_membership_with_locality(
					tenant_id,
					key_identity,
					routing_locality,
				);
				let publication = self.begin_index_publication(vec![location.slab_id]).await?;
				self.persist_record_row_strict(
					tenant_id,
					key_identity,
					routing_locality,
					location,
					&publication,
				)
				.await?;
				publication.complete();
				return Ok(MigrationIngestOutcome::Duplicate(location));
			}
			// Different bytes claim the same MVCC slot. Refuse
			// loudly — accepting either side would make historical
			// reads depend on which replica answered the pull.
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"append_migration payload_hash mismatch: migration_id={migration_id} \
                     key_identity={key_identity} hlc={hlc:?} stored_hash={stored_hash:?} \
                     incoming_hash={payload_hash:?}"
				),
			));
		}
		#[cfg(test)]
		self.migration_after_same_hlc_check_pause_point().await;

		if let Some(entries) = storage_multirow_index_entries_from_frame(frame)? {
			let Some(requested_entry) = entries
				.iter()
				.find(|entry| entry.key_identity == key_identity && entry.row_hlc == hlc)
				.copied()
			else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"multi-row migration frame does not contain the requested row identity/HLC",
				));
			};
			if requested_entry.routing_locality != routing_locality {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"multi-row migration frame routing locality mismatch",
				));
			}
			let base_location = self
				.append_durable_multirow_with_locality(tenant_id, frame, &entries)
				.await?;
			return Ok(MigrationIngestOutcome::Inserted(RecordLocation {
				hlc: requested_entry.row_hlc,
				record_row_index: requested_entry.record_row_index,
				is_tombstone: requested_entry.is_tombstone,
				..base_location
			}));
		}

		// No prior version at this exact HLC — append to the same
		// slab machinery as hot writes, but publish the caller's
		// routing locality instead of deriving it from identity.
		let (slab, location, _append_guard) = self
			.append_on_target(tenant_id, key_identity, hlc, frame)
			.await?;
		let publication = self.begin_index_publication(vec![location.slab_id]).await?;
		if let Err(err) = self
			.persist_record_row_strict(
				tenant_id,
				key_identity,
				routing_locality,
				location,
				&publication,
			)
			.await
		{
			slab.rollback_index_entry(tenant_id, key_identity, location);
			return Err(err);
		}
		// The same-HLC check above ran before the append awaited: a version
		// published at this slot since then is a conflict, as for any producer.
		if let Err(err) = self.index_checked_version(
			tenant_id,
			key_identity,
			routing_locality,
			location,
			existing.len(),
		) {
			slab.rollback_index_entry(tenant_id, key_identity, location);
			return Err(err);
		}
		publication.complete();
		Ok(MigrationIngestOutcome::Inserted(location))
	}
}
