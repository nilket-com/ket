//! Counter eligibility and class-aware migration admission.
use super::*;

impl SlabManager {
	/// Eligibility is independent of the mixed live tentative index.
	pub(crate) fn counter_eligible_cells(
		&self,
	) -> Arc<super::counter::visibility::EligibleCells<(TenantId, Identity)>> {
		self.counter_eligible.clone()
	}
	pub(crate) async fn counter_eligible_supported(
		&self,
		tenant: TenantId,
		key: Identity,
	) -> std::io::Result<bool> {
		if self.counter_unsupported.contains(&(tenant, key)) {
			return Ok(false);
		}
		let locations = self
			.key_index
			.get(&(tenant, key))
			.map(|v| v.clone())
			.unwrap_or_default();
		for location in locations {
			match self.resolve_backing(location.slab_id).await {
				SlabBacking::Local(slab) => {
					if !slab.counter_record_supported(tenant, key, location).await? {
						return Ok(false);
					}
				}
				SlabBacking::L2(_) | SlabBacking::Missing => return Ok(false),
			}
		}
		Ok(true)
	}

	/// Counter frames cannot enter the classless raw migration envelope.
	pub(crate) async fn refuse_counter_migration(&self, slab_id: SlabId) -> std::io::Result<()> {
		match self.resolve_backing(slab_id).await {
			SlabBacking::Local(slab) => slab.refuse_counter_maintenance(),
			SlabBacking::L2(pointer) => {
				// This is the same class-aware walk used to admit blob-tier bytes.
				let bytes = self.tiered_plaintext_from_l2(&pointer).await?;
				super::core::recover_slab_entries_from_blob(
					&bytes,
					slab_id,
					&self.acknowledged_offsets_in_slab(slab_id),
				)?;
				Ok(())
			}
			SlabBacking::Missing => Err(std::io::Error::new(
				std::io::ErrorKind::NotFound,
				"migration source slab missing",
			)),
		}
	}
	pub(super) fn recover_counter_eligible(
		active: &Arc<LocalSlabStore>,
		sealed: &DashMap<SlabId, Arc<LocalSlabStore>>,
		counter_unsupported: &DashSet<(TenantId, Identity)>,
		key_index: &DashMap<(TenantId, Identity), Vec<RecordLocation>>,
	) -> Arc<super::counter::visibility::EligibleCells<(TenantId, Identity)>> {
		let mut counter_slabs = vec![active.clone()];
		counter_slabs.extend(sealed.iter().map(|entry| entry.value().clone()));
		Self::recover_counter_eligible_from_slabs(&counter_slabs, counter_unsupported, key_index)
	}

	pub(super) fn recover_counter_eligible_from_slabs(
		counter_slabs: &[Arc<LocalSlabStore>],
		counter_unsupported: &DashSet<(TenantId, Identity)>,
		key_index: &DashMap<(TenantId, Identity), Vec<RecordLocation>>,
	) -> Arc<super::counter::visibility::EligibleCells<(TenantId, Identity)>> {
		let counter_eligible = Arc::new(super::counter::visibility::EligibleCells::default());
		for slab in counter_slabs {
			for &(tenant, key, version) in &slab.recovered_counter_versions {
				counter_eligible.recover(&(tenant, key), version);
			}
			for &key in &slab.recovered_non_counter_keys {
				counter_unsupported.insert(key);
			}
			for &(tenant, key, error) in &slab.recovered_counter_errors {
				counter_eligible.refuse(&(tenant, key), error);
			}
		}

		// V2 markers and tentatives use the complete version identity, not a
		// slab-local byte offset. Keep the slab handles with each candidate so
		// only a successfully paired tentative is added to the live indexes.
		type VersionIdentity = (TenantId, Identity, u64, u32, u32, u32);
		let identity = |tenant: TenantId,
		                key: Identity,
		                hlc: crate::schema::HybridTimestamp,
		                origin: crate::schema::NodeId| {
			(
				tenant,
				key,
				hlc.physical_ms,
				hlc.logical,
				hlc.node_id,
				origin.id,
			)
		};
		let mut tentatives: HashMap<
			VersionIdentity,
			Vec<(
				Arc<LocalSlabStore>,
				crate::storage::core::RecoveredCounterTentativeV2,
			)>,
		> = HashMap::new();
		let mut markers: HashMap<
			VersionIdentity,
			Vec<crate::storage::core::RecoveredCounterMarkerV2>,
		> = HashMap::new();
		for slab in counter_slabs {
			for &candidate in &slab.recovered_counter_tentatives_v2 {
				tentatives
					.entry(identity(
						candidate.tenant_id,
						candidate.key_identity,
						candidate.location.hlc,
						candidate.origin,
					))
					.or_default()
					.push((slab.clone(), candidate));
			}
			for &candidate in &slab.recovered_counter_markers_v2 {
				markers
					.entry(identity(
						candidate.tenant_id,
						candidate.key_identity,
						candidate.location.hlc,
						candidate.origin,
					))
					.or_default()
					.push(candidate);
			}
		}
		for (version_identity, version_markers) in markers {
			let (tenant, key, physical_ms, logical, node_id, origin_id) = version_identity;
			let hlc = crate::schema::HybridTimestamp {
				physical_ms,
				logical,
				node_id,
			};
			let origin = crate::schema::NodeId {
				id: origin_id,
				_reserved: 0,
			};
			let already_committed =
				match counter_eligible.committed_identity(&(tenant, key), hlc, origin) {
					Ok(value) => value,
					Err(error) => {
						counter_eligible.refuse(&(tenant, key), error);
						continue;
					}
				};
			let Some(version_tentatives) = tentatives.get(&version_identity) else {
				// A marker duplicated on another slab is idempotent when this
				// exact version identity was already recovered from a local pair.
				if already_committed.is_some() {
					continue;
				}
				counter_eligible.refuse(
					&(tenant, key),
					super::counter::CounterVisibilityError::Conflict,
				);
				continue;
			};
			let first = version_tentatives[0].1;
			let identity_conflict = version_tentatives
				.iter()
				.any(|(_, tentative)| tentative.value != first.value)
				|| already_committed.is_some_and(|value| value != first.value);
			let offset_conflict = version_markers.iter().any(|marker| {
				let same_slab = version_tentatives
					.iter()
					.filter(|(_, tentative)| tentative.location.slab_id == marker.location.slab_id)
					.map(|(_, tentative)| tentative.location.offset)
					.collect::<Vec<_>>();
				!same_slab.is_empty() && !same_slab.contains(&marker.tentative_offset)
			});
			if identity_conflict || offset_conflict {
				let (tenant, key, ..) = version_identity;
				counter_eligible.refuse(
					&(tenant, key),
					super::counter::CounterVisibilityError::Conflict,
				);
				continue;
			}
			let version = super::counter::CounterVersion {
				value: first.value,
				hlc: first.location.hlc,
				origin: Some(first.origin),
			};
			for (slab, tentative) in version_tentatives {
				slab.index_location(
					tentative.tenant_id,
					tentative.key_identity,
					tentative.location,
				);
				key_index
					.entry((tentative.tenant_id, tentative.key_identity))
					.and_modify(|versions| insert_version_sorted(versions, tentative.location))
					.or_insert_with(|| vec![tentative.location]);
				counter_eligible.recover(&(tentative.tenant_id, tentative.key_identity), version);
			}
		}
		for key in counter_unsupported.iter() {
			counter_eligible.refuse(
				key.key(),
				super::counter::CounterVisibilityError::Unsupported,
			);
		}
		counter_eligible
	}
}
