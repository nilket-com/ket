use super::*;

#[derive(Clone, Copy)]
pub(super) struct CompactionRecord {
	pub(super) tenant_id: TenantId,
	pub(super) key_identity: Identity,
	pub(super) routing_locality: Locality,
	pub(super) old_location: RecordLocation,
	pub(super) reclaim: bool,
	pub(super) retention_ambiguous: bool,
	pub(super) retention_lag_seconds: u64,
}

#[derive(Clone)]
pub(super) struct ReclusterLogicalRow {
	pub(super) tenant_id: TenantId,
	pub(super) key_identity: Identity,
	pub(super) routing_locality: Locality,
	pub(super) old_location: RecordLocation,
	pub(super) row_hlc: HybridTimestamp,
	pub(super) batch: RecordBatch,
}

pub(super) struct ReclusterGroupPlan {
	pub(super) frame: Vec<u8>,
	pub(super) records: Vec<CompactionRecord>,
	pub(super) matching_rows: Vec<ReclusterLogicalRow>,
	pub(super) nonmatching_records: Vec<CompactionRecord>,
}

pub(super) struct CompactedFrame {
	pub(super) tenant_id: TenantId,
	pub(super) key_identity: Identity,
	pub(super) hlc: HybridTimestamp,
	pub(super) frame: Vec<u8>,
	pub(super) was_multirow: bool,
	pub(super) rows: Vec<CompactedFrameRow>,
}

#[derive(Clone, Copy)]
pub(super) struct CompactedFrameRow {
	pub(super) tenant_id: TenantId,
	pub(super) key_identity: Identity,
	pub(super) routing_locality: Locality,
	pub(super) old_location: RecordLocation,
	pub(super) row_hlc: HybridTimestamp,
	pub(super) record_row_index: RecordRowIndex,
}

#[derive(Clone, Copy)]
pub(super) struct RelocatedRecord {
	pub(super) tenant_id: TenantId,
	pub(super) key_identity: Identity,
	pub(super) routing_locality: Locality,
	pub(super) old_location: RecordLocation,
	pub(super) new_location: RecordLocation,
}

#[derive(Clone, Copy)]
pub(super) struct CompactionOutcome {
	pub(super) reclaimed_bytes: u64,
	pub(super) reclaimed_records: usize,
	pub(super) retention_stats: RetentionCompactionStats,
}

#[derive(Clone, Copy)]
pub(super) struct LayoutReclusterOutcome {
	pub(super) reclaimed_bytes: u64,
	pub(super) rewritten_records: usize,
	pub(super) relocated_rows: usize,
}

pub(super) fn physical_compaction_groups(
	records: &[CompactionRecord],
) -> BTreeMap<(u64, u32), Vec<CompactionRecord>> {
	let mut groups = BTreeMap::new();
	for record in records {
		groups
			.entry((record.old_location.offset, record.old_location.length))
			.or_insert_with(Vec::new)
			.push(*record);
	}
	groups
}

pub(super) fn physical_compaction_group_indices(
	records: &[CompactionRecord],
) -> BTreeMap<(u64, u32), Vec<usize>> {
	let mut groups = BTreeMap::new();
	for (idx, record) in records.iter().enumerate() {
		groups
			.entry((record.old_location.offset, record.old_location.length))
			.or_insert_with(Vec::new)
			.push(idx);
	}
	groups
}

pub(super) async fn append_existing_frame_to_replacement(
	replacement: &Arc<LocalSlabStore>,
	frame: &[u8],
	records: &[CompactionRecord],
	relocations: &mut Vec<RelocatedRecord>,
) -> std::io::Result<()> {
	let Some(first) = records.first().copied() else {
		return Ok(());
	};
	let hlc = records
		.iter()
		.map(|record| record.old_location.hlc)
		.max()
		.unwrap_or(first.old_location.hlc);
	let base_location = replacement
		.append(first.tenant_id, first.key_identity, hlc, frame)
		.await?;
	replacement.rollback_index_entry(first.tenant_id, first.key_identity, base_location);
	for record in records {
		let new_location = RecordLocation {
			hlc: record.old_location.hlc,
			record_row_index: record.old_location.record_row_index,
			is_tombstone: record.old_location.is_tombstone,
			// ADR 0090: compaction re-homes the bytes, never the placement.
			placement: record.old_location.placement,
			partition: record.old_location.partition,
			..base_location
		};
		replacement.index_location(record.tenant_id, record.key_identity, new_location);
		relocations.push(RelocatedRecord {
			tenant_id: record.tenant_id,
			key_identity: record.key_identity,
			routing_locality: record.routing_locality,
			old_location: record.old_location,
			new_location,
		});
	}
	Ok(())
}

pub(super) async fn append_compacted_frame_to_replacement(
	replacement: &Arc<LocalSlabStore>,
	compacted: CompactedFrame,
	relocations: &mut Vec<RelocatedRecord>,
) -> std::io::Result<()> {
	let base_location = replacement
		.append(
			compacted.tenant_id,
			compacted.key_identity,
			compacted.hlc,
			&compacted.frame,
		)
		.await?;
	replacement.rollback_index_entry(compacted.tenant_id, compacted.key_identity, base_location);
	for row in &compacted.rows {
		let new_location = RecordLocation {
			hlc: row.row_hlc,
			record_row_index: row.record_row_index,
			is_tombstone: row.old_location.is_tombstone,
			// ADR 0090: compaction re-homes the bytes, never the placement.
			placement: row.old_location.placement,
			partition: row.old_location.partition,
			..base_location
		};
		replacement.index_location(row.tenant_id, row.key_identity, new_location);
		relocations.push(RelocatedRecord {
			tenant_id: row.tenant_id,
			key_identity: row.key_identity,
			routing_locality: row.routing_locality,
			old_location: row.old_location,
			new_location,
		});
	}
	Ok(())
}

pub(super) fn compact_physical_record_frame(
	frame: &[u8],
	records: &[CompactionRecord],
	table_schemas: Option<&TableSchemaCatalog>,
	row_group_config: &crate::storage::row_group::RowGroupWriterConfig,
	encoding_config: &EncodingWriterConfig,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<Option<CompactedFrame>> {
	let mut survivors = records
		.iter()
		.copied()
		.filter(|record| !record.reclaim)
		.collect::<Vec<_>>();
	if survivors.is_empty() {
		return Ok(None);
	}

	if storage_multirow_index_entries_from_frame(frame)?.is_none() {
		if records.len() != 1 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"single-row storage record has multiple compaction index entries",
			));
		}
		let record = survivors[0];
		return Ok(Some(CompactedFrame {
			tenant_id: record.tenant_id,
			key_identity: record.key_identity,
			hlc: record.old_location.hlc,
			frame: frame.to_vec(),
			was_multirow: false,
			rows: vec![CompactedFrameRow {
				tenant_id: record.tenant_id,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				old_location: record.old_location,
				row_hlc: record.old_location.hlc,
				record_row_index: RecordRowIndex::ZERO,
			}],
		}));
	}

	let batch = decode_record_batch(frame)?;
	if !has_storage_synthetic_columns(&batch) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"multi-row storage record decoded without synthetic columns",
		));
	}
	survivors.sort_by_key(|record| {
		(
			record.routing_locality,
			record.key_identity,
			record.old_location.hlc,
			record.old_location.record_row_index,
		)
	});
	// ADR 045 Phase 8: preserve the per-row `__row_table_object_id__`
	// stamp through compaction. AppendOnly rows must keep their
	// table-isolation binding; PK rows have `None`.
	let source_metadata = crate::storage::multirow::storage_rows_from_batch(&batch)?;
	let descriptor = compaction_row_group_descriptor(
		table_schemas,
		row_group_config,
		&survivors,
		&source_metadata,
	);
	let source_rows = survivors
		.iter()
		.map(|record| {
			let row_index = record.old_location.record_row_index.get() as usize;
			let table_object_id = source_metadata
				.get(row_index)
				.and_then(|meta| meta.table_object_id);
			MultiRowSourceRow {
				source_row_index: row_index,
				key_identity: record.key_identity,
				routing_locality: record.routing_locality,
				row_hlc: record.old_location.hlc,
				is_tombstone: record.old_location.is_tombstone,
				// ADR 0090: compaction preserves each row's durable placement.
				partition: source_metadata
					.get(row_index)
					.and_then(|meta| meta.partition),
				table_object_id,
			}
		})
		.collect::<Vec<_>>();
	let policy = crate::storage::ipc::record_batch_compression_policy_from_ipc(frame)?;
	let encoded = match descriptor {
		Some(descriptor) => {
			encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config(
				&batch,
				&source_rows,
				descriptor,
				policy,
				row_group_config,
				encoding_config,
				chunk_layout_config,
			)?
		}
		None => encode_multirow_from_survivors_with_policy(&batch, &source_rows, policy)?,
	};
	let entries = encoded.rows;
	if entries.len() != survivors.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"rewritten multi-row storage record entry count mismatch: expected={} actual={}",
				survivors.len(),
				entries.len()
			),
		));
	}

	let primary = entries[0];
	let hlc = encoded.max_row_hlc;
	let mut rows = Vec::with_capacity(entries.len());
	for (record, entry) in survivors.iter().zip(entries.iter()) {
		if record.key_identity != entry.key_identity
			|| record.routing_locality != entry.routing_locality
			|| record.old_location.hlc != entry.row_hlc
			|| record.old_location.is_tombstone != entry.is_tombstone
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"rewritten multi-row storage metadata does not match surviving row",
			));
		}
		rows.push(CompactedFrameRow {
			tenant_id: record.tenant_id,
			key_identity: entry.key_identity,
			routing_locality: entry.routing_locality,
			old_location: record.old_location,
			row_hlc: entry.row_hlc,
			record_row_index: entry.record_row_index,
		});
	}
	Ok(Some(CompactedFrame {
		tenant_id: survivors[0].tenant_id,
		key_identity: primary.key_identity,
		hlc,
		frame: encoded.frame,
		was_multirow: true,
		rows,
	}))
}

pub(super) fn compaction_row_group_descriptor<'a>(
	table_schemas: Option<&'a TableSchemaCatalog>,
	row_group_config: &crate::storage::row_group::RowGroupWriterConfig,
	survivors: &[CompactionRecord],
	source_metadata: &[StorageRowMetadata],
) -> Option<&'a TableSchemaDescriptor> {
	if !row_group_config.enabled {
		return None;
	}
	let table_schemas = table_schemas?;
	let first = survivors.first()?;
	let mut table_object_id = None;
	for record in survivors {
		if record.tenant_id != first.tenant_id {
			return None;
		}
		let metadata = source_metadata.get(record.old_location.record_row_index.get() as usize)?;
		match (table_object_id, metadata.table_object_id) {
			(None, Some(value)) => table_object_id = Some(value),
			(Some(expected), Some(value)) if expected == value => {}
			_ => return None,
		}
	}
	let descriptor = table_schemas.latest_by_id(first.tenant_id, table_object_id?)?;
	descriptor
		.record_layout_policy
		.is_multi_row()
		.then_some(descriptor)
}

impl SlabManager {
	pub async fn recluster_sealed_table_layout(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		max_slabs: usize,
	) -> std::io::Result<LayoutReclusterReport> {
		descriptor.validate()?;
		if max_slabs == 0 {
			return Ok(LayoutReclusterReport::default());
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

		let mut report = LayoutReclusterReport::default();
		for (slab_id, slab) in sealed {
			if report.rewritten_slabs >= max_slabs {
				break;
			}
			if let Some(outcome) = self
				.recluster_one_sealed_slab(
					slab_id,
					slab,
					tenant_id,
					descriptor,
					SecondaryIndexCompactionContext {
						table_schemas,
						secondary_indexes,
					},
				)
				.await?
			{
				report.rewritten_slabs += 1;
				report.rewritten_records = report
					.rewritten_records
					.saturating_add(outcome.rewritten_records);
				report.relocated_rows =
					report.relocated_rows.saturating_add(outcome.relocated_rows);
				report.reclaimed_bytes = report
					.reclaimed_bytes
					.saturating_add(outcome.reclaimed_bytes);
			}
		}
		Ok(report)
	}

	pub(super) async fn compact_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		tombstones: &[MigratedRangeTombstone],
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Option<CompactionOutcome>> {
		let mut records: Vec<CompactionRecord> = slab
			.index
			.iter()
			.flat_map(|entry| {
				let (tenant_id, key_identity) = *entry.key();
				let locality = self.routing_locality_for_key(tenant_id, key_identity);
				entry
					.value()
					.iter()
					.map(move |loc| CompactionRecord {
						tenant_id,
						key_identity,
						routing_locality: locality,
						old_location: *loc,
						reclaim: tombstones.iter().any(|tombstone| {
							locality.get() >= tombstone.range_start
								&& locality.get() <= tombstone.range_end
								&& loc.hlc >= tombstone.migration_hlc_floor
						}),
						retention_ambiguous: false,
						retention_lag_seconds: 0,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		records.sort_by_key(|record| record.old_location.offset);

		self.compact_sealed_slab_records(slab_id, slab, &records, secondary_index_context)
			.await
	}

	pub(super) async fn recluster_one_sealed_slab(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Option<LayoutReclusterOutcome>> {
		if self.is_compaction_slab_quarantined(slab_id) {
			return Ok(None);
		}
		let _slab_rewrite_claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;
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
		if records.is_empty() {
			return Ok(None);
		}
		records.sort_by_key(|record| {
			(
				record.old_location.offset,
				record.old_location.record_row_index,
			)
		});

		let target_is_multirow = descriptor.record_layout_policy.is_multi_row();
		let mut needs_recluster = false;
		let mut group_plans = Vec::new();
		for group in physical_compaction_groups(&records).values() {
			let Some(first) = group.first().copied() else {
				continue;
			};
			let frame = slab.read(first.old_location).await?;
			let source_is_multirow = storage_multirow_index_entries_from_frame(&frame)?.is_some();
			let decoded = match decode_record_batch(&frame) {
				Ok(batch) => Some(batch),
				Err(err) if source_is_multirow => return Err(err),
				Err(_) => None,
			};
			let source_compression_policy = match decoded.as_ref() {
				Some(_) => Some(record_batch_compression_policy_from_ipc(&frame)?),
				None => None,
			};
			let mut matching_rows = Vec::new();
			let mut nonmatching_records = Vec::new();
			for record in group {
				let matches_descriptor = match decoded.as_ref() {
					Some(_) if record.tenant_id != tenant_id => false,
					Some(batch) => {
						// Phase 8.5: malformed
						// `__row_table_object_id__` is storage
						// corruption, not "treat as PK".
						let stored_table_object_id =
							crate::storage::multirow::storage_row_table_object_id_at(
								batch,
								record.old_location.record_row_index,
							)?;
						let row_batch = materialize_record_location_batch(
							batch,
							record.old_location.record_row_index,
						)?;
						if descriptor_matches_tiered_row(
							descriptor,
							tenant_id,
							record.key_identity,
							record.routing_locality,
							stored_table_object_id,
							&row_batch,
						)? {
							matching_rows.push(ReclusterLogicalRow {
								tenant_id: record.tenant_id,
								key_identity: record.key_identity,
								routing_locality: record.routing_locality,
								old_location: record.old_location,
								row_hlc: record.old_location.hlc,
								batch: row_batch,
							});
							true
						} else {
							false
						}
					}
					None => false,
				};
				if !matches_descriptor {
					nonmatching_records.push(*record);
				}
			}
			if !matching_rows.is_empty() {
				let codec_mismatch = match source_compression_policy {
					Some(policy) => policy != descriptor.record_body_compression_policy,
					None => false,
				};
				if source_is_multirow != target_is_multirow || codec_mismatch {
					needs_recluster = true;
				}
			}
			group_plans.push(ReclusterGroupPlan {
				frame,
				records: group.clone(),
				matching_rows,
				nonmatching_records,
			});
		}

		if !needs_recluster {
			return Ok(None);
		}

		let old_bytes = slab.write_cursor();
		let replacement_id = self.reserve_slab_id().await;
		let replacement_path = slab_path(&self.config.dir, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let mut relocations = Vec::with_capacity(records.len());
		let mut rows_to_recluster = Vec::new();
		let mut rewritten_records = 0_usize;
		let mut multirow_recompact_count = 0_u64;

		for plan in group_plans {
			if plan.matching_rows.is_empty() {
				append_existing_frame_to_replacement(
					&replacement,
					&plan.frame,
					&plan.records,
					&mut relocations,
				)
				.await?;
				rewritten_records = rewritten_records.saturating_add(1);
				continue;
			}

			let matching_locations = plan
				.matching_rows
				.iter()
				.map(|row| row.old_location)
				.collect::<HashSet<_>>();
			rows_to_recluster.extend(plan.matching_rows);
			if plan.nonmatching_records.is_empty() {
				continue;
			}

			let partial = plan
				.records
				.iter()
				.map(|record| CompactionRecord {
					reclaim: matching_locations.contains(&record.old_location),
					..*record
				})
				.collect::<Vec<_>>();
			if let Some(compacted) = compact_physical_record_frame(
				&plan.frame,
				&partial,
				None,
				&self.config.row_group_writer,
				&self.config.encoding_writer,
				&self.config.l2_chunk_layout,
			)? {
				if compacted.was_multirow {
					multirow_recompact_count = multirow_recompact_count.saturating_add(1);
				}
				append_compacted_frame_to_replacement(&replacement, compacted, &mut relocations)
					.await?;
				rewritten_records = rewritten_records.saturating_add(1);
			}
		}

		match &descriptor.record_layout_policy {
			RecordLayoutPolicy::SingleRow => {
				rows_to_recluster.sort_by_key(|row| {
					(
						row.routing_locality,
						row.key_identity,
						row.row_hlc,
						row.old_location.offset,
						row.old_location.record_row_index,
					)
				});
				for row in &rows_to_recluster {
					let frame = encode_batch_with_policy(
						&row.batch,
						descriptor.record_body_compression_policy,
					)?
					.bytes;
					let new_location = replacement
						.append(row.tenant_id, row.key_identity, row.row_hlc, &frame)
						.await?;
					relocations.push(RelocatedRecord {
						tenant_id: row.tenant_id,
						key_identity: row.key_identity,
						routing_locality: row.routing_locality,
						old_location: row.old_location,
						new_location,
					});
					rewritten_records = rewritten_records.saturating_add(1);
				}
			}
			RecordLayoutPolicy::MultiRow(layout) => {
				let batches = rows_to_recluster
					.iter()
					.map(|row| row.batch.clone())
					.collect::<Vec<_>>();
				if !batches.is_empty() {
					let schema = batches[0].schema();
					let batch = concat_batches(&schema, &batches).map_err(|err| {
						Error::new(
							ErrorKind::InvalidData,
							format!("layout recluster row concat failed: {err}"),
						)
					})?;
					// ADR 045 Phase 8: reclustering re-stamps the
					// table-isolation column from the descriptor — the
					// descriptor is authoritative for the rows being
					// reclustered because reclustering operates
					// per-descriptor.
					let table_object_id_stamp = Some(descriptor.table_object_id);
					let source_rows = rows_to_recluster
						.iter()
						.enumerate()
						.map(|(source_row_index, row)| MultiRowSourceRow {
							source_row_index,
							key_identity: row.key_identity,
							routing_locality: row.routing_locality,
							row_hlc: row.row_hlc,
							is_tombstone: row.old_location.is_tombstone,
							partition: row.old_location.partition,
							table_object_id: table_object_id_stamp,
						})
						.collect::<Vec<_>>();
					for packed in
						pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config(
							*layout,
							&batch,
							&source_rows,
							Some(descriptor),
							descriptor.record_body_compression_policy,
							&self.config.row_group_writer,
							&self.config.encoding_writer,
							&self.config.l2_chunk_layout,
						)? {
						let Some(primary) = packed.encoded.rows.first().copied() else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"layout recluster produced an empty multi-row frame",
							));
						};
						let base_location = replacement
							.append(
								tenant_id,
								primary.key_identity,
								packed.encoded.max_row_hlc,
								&packed.encoded.frame,
							)
							.await?;
						replacement.rollback_index_entry(
							tenant_id,
							primary.key_identity,
							base_location,
						);
						for (source_row, encoded_row) in
							packed.source_rows.iter().zip(packed.encoded.rows.iter())
						{
							let old_row = rows_to_recluster
								.get(source_row.source_row_index)
								.ok_or_else(|| {
									Error::new(
										ErrorKind::InvalidData,
										"layout recluster source row index out of bounds",
									)
								})?;
							if old_row.key_identity != encoded_row.key_identity
								|| old_row.routing_locality != encoded_row.routing_locality
								|| old_row.row_hlc != encoded_row.row_hlc
								|| old_row.old_location.is_tombstone != encoded_row.is_tombstone
							{
								return Err(Error::new(
									ErrorKind::InvalidData,
									"layout recluster storage metadata does not match source row",
								));
							}
							let new_location = RecordLocation {
								hlc: encoded_row.row_hlc,
								record_row_index: encoded_row.record_row_index,
								is_tombstone: encoded_row.is_tombstone,
								// ADR 0090: the reclustered frame's per-row
								// partition is the durable placement.
								placement: if encoded_row.partition.is_some() {
									crate::schema::table::PlacementSemantics::Partitioned
								} else {
									crate::schema::table::PlacementSemantics::RawLocality
								},
								partition: encoded_row.partition,
								..base_location
							};
							replacement.index_location(
								tenant_id,
								encoded_row.key_identity,
								new_location,
							);
							relocations.push(RelocatedRecord {
								tenant_id,
								key_identity: encoded_row.key_identity,
								routing_locality: encoded_row.routing_locality,
								old_location: old_row.old_location,
								new_location,
							});
						}
						rewritten_records = rewritten_records.saturating_add(1);
						multirow_recompact_count = multirow_recompact_count.saturating_add(1);
					}
				}
			}
		}

		if relocations.len() != records.len() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"layout recluster relocation count mismatch: old_rows={} new_rows={}",
					records.len(),
					relocations.len()
				),
			));
		}
		replacement.fsync().await?;
		let new_bytes = replacement.write_cursor();
		// Enumerated before install so the descriptors reflect what the retiring slab
		// actually published.
		let old_secondary_index_descriptors = self
			.secondary_index_compaction_descriptors_for_source_slab(
				slab_id,
				secondary_index_context,
			)
			.await?;
		if !self
			.finish_compaction_with_replacement(
				slab_id,
				replacement_id,
				replacement.clone(),
				&records,
				&relocations,
			)
			.await?
		{
			let path = slab_path(&self.config.dir, replacement_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
			return Ok(None);
		}

		match self
			.reconcile_secondary_index_segments_after_compaction(
				slab_id,
				Some(replacement_id),
				secondary_index_context,
				old_secondary_index_descriptors,
			)
			.await
		{
			Ok(_proof) => {
				let old_path = slab_path(&self.config.dir, slab_id);
				unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
			}
			Err(err) => {
				self.quarantine_compaction_slab_for_reconciliation(slab_id, &err);
			}
		}
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}

		Ok(Some(LayoutReclusterOutcome {
			reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
			rewritten_records,
			relocated_rows: relocations.len(),
		}))
	}

	pub(super) async fn compact_sealed_slab_records(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Option<CompactionOutcome>> {
		if records.is_empty() || records.iter().all(|record| !record.reclaim) {
			return Ok(None);
		}
		// Quarantined work is not retried at every tick; repair is an explicit
		// future operator decision.
		if self.is_compaction_slab_quarantined(slab_id) {
			return Ok(None);
		}
		let _slab_rewrite_claim = self.acquire_slab_rewrite_exclusive_claim(slab_id).await;

		let old_bytes = slab.write_cursor();
		let reclaimed_records = records.iter().filter(|record| record.reclaim).count();
		let retention_stats = Self::retention_compaction_stats(records);
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		// Enumerated before any mutation so the descriptors reflect the state the
		// retiring slab actually published. Failure fails the compaction, untouched.
		let old_secondary_index_descriptors = self
			.secondary_index_compaction_descriptors_for_source_slab(
				slab_id,
				secondary_index_context,
			)
			.await?;
		if survivors == 0 {
			// Validate before destroying: reconciliation proof precedes the record
			// rollback, persistent deletion and unlink inside the finish path.
			if let Err(err) = self
				.reconcile_secondary_index_segments_after_compaction(
					slab_id,
					None,
					secondary_index_context,
					old_secondary_index_descriptors,
				)
				.await
			{
				self.quarantine_compaction_slab_for_reconciliation(slab_id, &err);
				return Ok(None);
			}
			self.finish_compaction_without_replacement(slab_id, &records)
				.await?;
			return Ok(Some(CompactionOutcome {
				reclaimed_bytes: old_bytes,
				reclaimed_records,
				retention_stats,
			}));
		}

		let replacement_id = self.reserve_slab_id().await;
		let replacement_path = slab_path(&self.config.dir, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let mut relocations = Vec::with_capacity(survivors);
		let physical_groups = physical_compaction_groups(records);
		let mut multirow_recompact_count = 0_u64;
		for group in physical_groups.values() {
			let Some(first) = group.first().copied() else {
				continue;
			};
			let frame = slab.read(first.old_location).await?;
			let Some(compacted) = compact_physical_record_frame(
				&frame,
				group,
				None,
				&self.config.row_group_writer,
				&self.config.encoding_writer,
				&self.config.l2_chunk_layout,
			)?
			else {
				continue;
			};
			if compacted.was_multirow {
				multirow_recompact_count = multirow_recompact_count.saturating_add(1);
			}
			let base_location = replacement
				.append(
					compacted.tenant_id,
					compacted.key_identity,
					compacted.hlc,
					&compacted.frame,
				)
				.await?;
			replacement.rollback_index_entry(
				compacted.tenant_id,
				compacted.key_identity,
				base_location,
			);
			for row in &compacted.rows {
				let new_location = RecordLocation {
					hlc: row.row_hlc,
					record_row_index: row.record_row_index,
					is_tombstone: row.old_location.is_tombstone,
					// ADR 0090: compaction re-homes the bytes, never the placement.
					placement: row.old_location.placement,
					partition: row.old_location.partition,
					..base_location
				};
				replacement.index_location(row.tenant_id, row.key_identity, new_location);
				relocations.push(RelocatedRecord {
					tenant_id: row.tenant_id,
					key_identity: row.key_identity,
					routing_locality: row.routing_locality,
					old_location: row.old_location,
					new_location,
				});
			}
		}
		replacement.fsync().await?;
		let new_bytes = replacement.write_cursor();

		if !self
			.finish_compaction_with_replacement(
				slab_id,
				replacement_id,
				replacement.clone(),
				&records,
				&relocations,
			)
			.await?
		{
			let path = slab_path(&self.config.dir, replacement_id);
			let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
			return Ok(None);
		}

		match self
			.reconcile_secondary_index_segments_after_compaction(
				slab_id,
				Some(replacement_id),
				secondary_index_context,
				old_secondary_index_descriptors,
			)
			.await
		{
			Ok(_proof) => {
				let old_path = slab_path(&self.config.dir, slab_id);
				unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
			}
			Err(err) => {
				// The replacement is installed and owns the records; the old file is
				// preserved on disk as evidence, never republished. Containment, not
				// repair.
				self.quarantine_compaction_slab_for_reconciliation(slab_id, &err);
			}
		}
		if multirow_recompact_count > 0 {
			metrics().observe_storage_multirow_recompact(multirow_recompact_count);
		}

		Ok(Some(CompactionOutcome {
			reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
			reclaimed_records,
			retention_stats,
		}))
	}

	pub(super) async fn finish_compaction_without_replacement(
		&self,
		slab_id: SlabId,
		records: &[CompactionRecord],
	) -> std::io::Result<()> {
		let removed = {
			let state = self.state.lock().await;
			state.sealed.remove(&slab_id)
		};
		let Some((_, _removed)) = removed else {
			return Ok(());
		};
		for record in records {
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		self.delete_persistent_records(records).await;
		// No negative reference scan here: this path runs only after
		// `reconcile_secondary_index_segments_after_compaction` returned proof. The
		// scan-based `can_retire_local_slab` guard remains on the sweep paths, where
		// no reconciliation proof exists.
		let old_path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &old_path).await?;
		Ok(())
	}

	pub(super) async fn finish_compaction_with_replacement(
		&self,
		old_slab_id: SlabId,
		new_slab_id: SlabId,
		replacement: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		relocations: &[RelocatedRecord],
	) -> std::io::Result<bool> {
		let state = self.state.lock().await;
		if !state.sealed.contains_key(&old_slab_id) || state.tiered.contains_key(&old_slab_id) {
			return Ok(false);
		}
		let relocation_tables = relocations
			.iter()
			.filter_map(|relocation| {
				self.published_table_object_id(relocation.old_location)
					.map(|table_object_id| (relocation.old_location, table_object_id))
			})
			.collect::<HashMap<_, _>>();
		state.sealed.remove(&old_slab_id);
		state.sealed.insert(new_slab_id, replacement);
		for record in records {
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		for relocation in relocations {
			self.index_appended_location_with_locality(
				relocation.tenant_id,
				relocation.key_identity,
				relocation.routing_locality,
				relocation.new_location,
			);
			self.relocated_reads
				.insert(relocation.old_location, relocation.new_location);
			if let Some(table_object_id) = relocation_tables.get(&relocation.old_location).copied()
			{
				self.add_published_table_reference(
					relocation.tenant_id,
					table_object_id,
					relocation.new_location,
				);
			}
		}
		drop(state);
		self.replace_persistent_records(records, relocations, &relocation_tables)
			.await;
		Ok(true)
	}
}
