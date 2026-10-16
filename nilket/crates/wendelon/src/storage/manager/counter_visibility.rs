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
	) -> Arc<super::counter::visibility::EligibleCells<(TenantId, Identity)>> {
		let counter_eligible = Arc::new(super::counter::visibility::EligibleCells::default());
		let mut counter_slabs = vec![active.clone()];
		counter_slabs.extend(sealed.iter().map(|entry| entry.value().clone()));
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
		for key in counter_unsupported.iter() {
			counter_eligible.refuse(
				key.key(),
				super::counter::CounterVisibilityError::Unsupported,
			);
		}
		counter_eligible
	}
}
