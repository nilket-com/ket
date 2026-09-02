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

const TABLE_DROP_L2_CLEANUP_PREFIX: &str = "table_drop_l2_cleanup_";
const TABLE_DROP_L2_CLEANUP_SUFFIX: &str = ".pending";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingTableDropL2Cleanup {
	format_version: u8,
	slab_id: SlabId,
	old_l2_uri: String,
	replacement_l2_uri: Option<String>,
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
	pub(super) async fn persist_table_drop_l2_cleanup(
		&self,
		slab_id: SlabId,
		old_l2_uri: &str,
		replacement_l2_uri: Option<&str>,
	) -> std::io::Result<()> {
		let marker = PendingTableDropL2Cleanup {
			format_version: 1,
			slab_id,
			old_l2_uri: old_l2_uri.to_string(),
			replacement_l2_uri: replacement_l2_uri.map(str::to_string),
		};
		let bytes = postcard::to_stdvec(&marker).map_err(|error| {
			Error::new(
				ErrorKind::InvalidData,
				format!("encode table-drop L2 cleanup marker: {error}"),
			)
		})?;
		let path = table_drop_l2_cleanup_path(&self.config.dir, slab_id);
		write_file(
			self.file_ops.as_ref(),
			&path,
			OpenFlags {
				write: true,
				create: true,
				truncate: true,
				..OpenFlags::default()
			},
			&bytes,
		)
		.await?;
		self.file_ops.fsync_dir(&self.config.dir).await
	}

	pub(super) async fn reconcile_table_drop_l2_cleanup_markers(
		&self,
		object_store: &dyn ObjectStore,
		examined_budget: usize,
	) -> std::io::Result<(usize, bool)> {
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
		markers.sort_unstable();
		let total = markers.len();
		let mut examined = 0usize;
		for slab_id in markers.into_iter().take(examined_budget) {
			examined += 1;
			let path = table_drop_l2_cleanup_path(&self.config.dir, slab_id);
			let bytes = self.file_ops.read_all(&path).await?;
			let marker: PendingTableDropL2Cleanup =
				postcard::from_bytes(&bytes).map_err(|error| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"table-drop L2 cleanup marker for slab {slab_id} is corrupt: {error}"
						),
					)
				})?;
			if marker.format_version != 1 || marker.slab_id != slab_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"table-drop L2 cleanup marker for slab {slab_id} has unsupported identity or format"
					),
				));
			}
			let current_uri = self
				.tier_pointer(slab_id)
				.await
				.map(|pointer| pointer.l2_uri);
			if current_uri.as_deref() != Some(marker.old_l2_uri.as_str()) {
				Self::delete_l2_object_if_present(object_store, &marker.old_l2_uri).await?;
			} else if let Some(replacement_uri) = marker.replacement_l2_uri.as_deref() {
				Self::delete_l2_object_if_present(object_store, replacement_uri).await?;
			}
			// If the old pointer is still authoritative, the replacement commit did
			// not become visible and the next bounded pass will retry it. Otherwise
			// the old object is gone before this durable intent is removed.
			unlink_local_file(self.file_ops.as_ref(), &path).await?;
		}
		Ok((examined, examined == total))
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
		let (cleanup_examined, cleanup_done) = self
			.reconcile_table_drop_l2_cleanup_markers(object_store.as_ref(), examined_budget)
			.await?;
		report.examined = cleanup_examined;
		if !cleanup_done || cleanup_examined == examined_budget {
			return Ok(report);
		}
		if pointers.is_empty() {
			report.done = true;
			return Ok(report);
		}
		let pointer_budget = examined_budget - cleanup_examined;
		let candidates = pointers.into_iter().filter(|pointer| {
			cursor
				.after_slab_id
				.is_none_or(|after| pointer.slab_id > after)
				&& upper_bound.is_none_or(|upper| pointer.slab_id <= upper)
		});
		for pointer in candidates.take(pointer_budget) {
			let _slab_rewrite_claim = self
				.acquire_slab_rewrite_exclusive_claim(pointer.slab_id)
				.await;
			report.examined += 1;
			report.cursor_next.after_slab_id = Some(pointer.slab_id);
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
					self.persist_table_drop_l2_cleanup(slab_id, &old_l2_uri, Some(&new_uri))
						.await?;
					let cleanup_marker = table_drop_l2_cleanup_path(&self.config.dir, slab_id);
					if let Err(error) = committer.commit_tier_slab(tier).await {
						// Commit errors are ambiguous: the Raft apply may still have won.
						// Keep both objects and the durable marker; the next slice resolves
						// which URI is authoritative before deleting either old data.
						return Err(error);
					}
					if old_l2_uri != new_uri {
						Self::delete_l2_object_if_present(object_store.as_ref(), &old_l2_uri)
							.await?;
					}
					unlink_local_file(self.file_ops.as_ref(), &cleanup_marker).await?;
					self.clear_retired_tiered_cleanup(slab_id);
					report.rewritten += 1;
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
					self.persist_table_drop_l2_cleanup(slab_id, &old_l2_uri, None)
						.await?;
					let cleanup_marker = table_drop_l2_cleanup_path(&self.config.dir, slab_id);
					if let Err(error) = committer.retire_tiered_slab(slab_id).await {
						// As above, a transport error does not prove the retirement failed.
						// Reconciliation compares the durable pointer with the marker.
						return Err(error);
					}
					Self::delete_l2_object_if_present(object_store.as_ref(), &old_l2_uri).await?;
					unlink_local_file(self.file_ops.as_ref(), &cleanup_marker).await?;
					self.clear_retired_tiered_cleanup(slab_id);
					report.rewritten += 1;
					report.reclaimed_records =
						report.reclaimed_records.saturating_add(reclaimed_records);
					report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reclaimed_bytes);
				}
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

	pub async fn reclaim_tombstoned_ranges(
		&self,
		tombstones: &[MigratedRangeTombstone],
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
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
		// ADR 0097: the handle is deliberately unused - each path refetches the
		// current slab after it acquires ownership.
		for (slab_id, _stale_handle) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.compact_one_sealed_slab(
					slab_id,
					tombstones,
					SecondaryIndexCompactionContext {
						table_schemas,
						secondary_indexes,
					},
				)
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
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		max_slabs: usize,
	) -> std::io::Result<RetentionPruningReport> {
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
		// ADR 0097: the handle is deliberately unused - each path refetches the
		// current slab after it acquires ownership.
		for (slab_id, _stale_handle) in sealed {
			if report.compacted_slabs >= max_slabs {
				break;
			}
			let scan = self
				.prune_one_sealed_slab_for_retention(
					slab_id,
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
