use super::*;

#[derive(Clone, Copy)]
pub(super) struct RetentionSlabScanOutcome {
	pub(super) compaction: Option<CompactionOutcome>,
	pub(super) retention_stats: RetentionCompactionStats,
}

pub(super) fn retention_rewrite_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.retention.tmp", slab_id))
}

pub(super) fn table_drop_rewrite_tmp_path(dir: &Path, slab_id: SlabId) -> PathBuf {
	dir.join(format!("slab_{:016x}.wnd.table_drop.tmp", slab_id))
}

pub(super) fn parse_mvcc_rewrite_tmp_filename(name: &str) -> Option<SlabId> {
	let rest = name
		.strip_prefix("slab_")?
		.strip_suffix(".wnd.retention.tmp")?;
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
	pub async fn reclaim_tombstoned_ranges(
		&self,
		tombstones: &[MigratedRangeTombstone],
		max_slabs: usize,
	) -> std::io::Result<TombstoneCompactionReport> {
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
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.compact_one_sealed_slab(slab_id, slab, tombstones)
				.await?
			{
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
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
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
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.prune_one_sealed_slab(slab_id, slab, retention_floor)
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
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			let scan = self
				.prune_one_sealed_slab_for_retention(
					slab_id,
					slab,
					cluster_policy,
					table_lifecycle,
					table_schemas,
					secondary_indexes,
					now_ms,
				)
				.await?;
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
		for (slab_id, slab) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.reclaim_one_sealed_table_drop_slab(
					slab_id,
					slab,
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
					encrypted_bytes,
					payload_hash,
					tombstone_metadata,
					supersession_metadata,
					reclaimed_records,
					reclaimed_bytes,
				} => {
					let key = self.table_drop_rewritten_key(slab_id);
					let new_uri = object_store.put(&key, &encrypted_bytes).await?;
					let fetched = object_store.get(&new_uri).await?;
					let fetched_hash = crate::hash::content::digest(&fetched);
					if fetched_hash != payload_hash {
						let _ = Self::delete_l2_object_if_present(object_store.as_ref(), &new_uri)
							.await;
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
					if let Err(error) = committer.commit_tier_slab(tier).await {
						if !self
							.tier_pointer(slab_id)
							.await
							.is_some_and(|pointer| pointer.l2_uri == new_uri)
						{
							let _ =
								Self::delete_l2_object_if_present(object_store.as_ref(), &new_uri)
									.await;
						}
						return Err(error);
					}
					if old_l2_uri != new_uri {
						if let Err(error) =
							Self::delete_l2_object_if_present(object_store.as_ref(), &old_l2_uri)
								.await
						{
							tracing::warn!(
								slab_id = slab_id.get(),
								l2_uri = %old_l2_uri,
								error = %error,
								"table drop tiered rewrite committed but old object cleanup failed"
							);
						} else {
							self.clear_retired_tiered_cleanup(slab_id);
						}
					} else {
						self.clear_retired_tiered_cleanup(slab_id);
					}
					report.compacted_slabs += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
				PreparedTieredTableDropRewrite::Retire {
					slab_id,
					old_l2_uri,
					reclaimed_records,
					reclaimed_bytes,
				} => {
					committer.retire_tiered_slab(slab_id).await?;
					if let Err(error) =
						Self::delete_l2_object_if_present(object_store.as_ref(), &old_l2_uri).await
					{
						tracing::warn!(
							slab_id = slab_id.get(),
							l2_uri = %old_l2_uri,
							error = %error,
							"table drop tiered retirement committed but old object cleanup failed"
						);
					} else {
						self.clear_retired_tiered_cleanup(slab_id);
					}
					report.compacted_slabs += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
			}
		}
		Ok(report)
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
		state.sealed.insert(sealed_id, sealed);
		state.next_slab_id = next_id.saturating_add(1);
		self.record_l0_compaction_run(sealed_id, sealed_size);
		Ok(Some(sealed_id))
	}

	pub(super) async fn reclaim_one_sealed_table_drop_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
	) -> std::io::Result<Option<CompactionOutcome>> {
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
			Some(SecondaryIndexCompactionContext {
				table_schemas,
				secondary_indexes,
			}),
		)
		.await
	}

	pub(super) async fn prune_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		retention_floor: HybridTimestamp,
	) -> std::io::Result<Option<CompactionOutcome>> {
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

		self.compact_sealed_slab_records(slab_id, slab, &records, None)
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
		slab: Arc<LocalSlabStore>,
		cluster_policy: MvccRetentionPolicy,
		table_lifecycle: &TableLifecycleCatalog,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		now_ms: u64,
	) -> std::io::Result<RetentionSlabScanOutcome> {
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
		let compaction = self
			.compact_sealed_slab_records(
				slab_id,
				slab,
				&records,
				Some(SecondaryIndexCompactionContext {
					table_schemas,
					secondary_indexes,
				}),
			)
			.await?;

		Ok(RetentionSlabScanOutcome {
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
