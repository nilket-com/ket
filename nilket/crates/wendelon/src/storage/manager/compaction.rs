use super::retention_reclaim::{SealedSlabScan, TombstoneSlabScanOutcome};
use super::secondary_index_segments::MaintenanceClaims;
use super::*;
use crate::storage::maintenance_policy::{DeterministicOrigin, deterministic_failure};

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

type CounterVersionIdentity = (TenantId, Identity, u64, u32, u32, u32);

#[derive(Clone)]
struct CounterRewriteFrame {
	physical: core::ScannedCounterFrame,
	value: u64,
	origin: crate::schema::NodeId,
	marker_tentative_offset: Option<u64>,
	indexed: bool,
}

#[derive(Default)]
struct CounterRewritePlan {
	frames: Vec<CounterRewriteFrame>,
	pending: bool,
}

#[derive(Clone, Copy)]
pub(super) struct RelocatedCounterFrame {
	old_location: RecordLocation,
	new_location: RecordLocation,
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	indexed: bool,
}

fn counter_version_identity(
	tenant_id: TenantId,
	key_identity: Identity,
	hlc: HybridTimestamp,
	origin: crate::schema::NodeId,
) -> CounterVersionIdentity {
	(
		tenant_id,
		key_identity,
		hlc.physical_ms,
		hlc.logical,
		hlc.node_id,
		origin.id,
	)
}

fn counter_frame_is_tentative(frame: &core::ScannedCounterFrame) -> bool {
	frame.record_class == crate::schema::record_class::COUNTER_TENTATIVE_V2
}

fn counter_frame_is_marker(frame: &core::ScannedCounterFrame) -> bool {
	frame.record_class == crate::schema::record_class::COUNTER_COMMIT_V2
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
	async fn plan_counter_rewrite(
		&self,
		slab: &Arc<LocalSlabStore>,
	) -> std::io::Result<CounterRewritePlan> {
		// A counter reservation advances the physical cursor before its async write
		// completes. The manager registers its pending identity under the same state
		// lock as that reservation, so defer before reading a possible zero/partial
		// reservation rather than treating it as corrupt slab bytes.
		if self.counter_slab_has_pending_frames(slab.slab_id()) {
			return Ok(CounterRewritePlan {
				frames: Vec::new(),
				pending: true,
			});
		}
		let scanned = match slab.scan_counter_frames().await {
			Ok(scanned) => scanned,
			Err(_error) if self.counter_slab_has_pending_frames(slab.slab_id()) => {
				return Ok(CounterRewritePlan {
					frames: Vec::new(),
					pending: true,
				});
			}
			Err(error) => return Err(error),
		};
		if self.counter_slab_has_pending_frames(slab.slab_id()) {
			return Ok(CounterRewritePlan {
				frames: Vec::new(),
				pending: true,
			});
		}
		let mut plan = CounterRewritePlan::default();
		let mut tentatives_by_identity: HashMap<CounterVersionIdentity, Vec<usize>> =
			HashMap::new();
		let mut markers_by_identity: HashMap<CounterVersionIdentity, Vec<usize>> = HashMap::new();
		for physical in scanned {
			if matches!(
				physical.record_class,
				crate::schema::record_class::COUNTER_TENTATIVE
					| crate::schema::record_class::COUNTER_COMMIT
			) {
				return Err(super::counter::CounterFormatError::MaintenanceUnsupported.io());
			}
			let (value, origin) = match physical.record_class {
				crate::schema::record_class::COUNTER_TENTATIVE_V2
				| crate::schema::record_class::COUNTER_COMMIT_V2 => {
					super::counter::decode_origin_frame(&physical.payload)?
				}
				other => {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!("maintenance frame walk found unsupported record class {other}"),
					));
				}
			};
			let identity = counter_version_identity(
				physical.tenant_id,
				physical.key_identity,
				physical.location.hlc,
				origin,
			);
			let indexed = counter_frame_is_tentative(&physical)
				&& self
					.key_index
					.get(&(physical.tenant_id, physical.key_identity))
					.is_some_and(|locations| locations.iter().any(|loc| *loc == physical.location));
			let marker_tentative_offset = counter_frame_is_marker(&physical).then_some(value);
			let index = plan.frames.len();
			if counter_frame_is_tentative(&physical) {
				tentatives_by_identity
					.entry(identity)
					.or_default()
					.push(index);
			} else {
				markers_by_identity.entry(identity).or_default().push(index);
			}
			plan.pending |=
				counter_frame_is_tentative(&physical) && self.counter_frame_is_pending(&physical);
			plan.frames.push(CounterRewriteFrame {
				physical,
				value,
				origin,
				marker_tentative_offset,
				indexed,
			});
		}

		// Same-slab V2 pairs retain the recovery reader's strict offset check. The
		// only permitted offset change is the bounded remap made while writing the
		// already-validated pair to this one replacement slab.
		for (identity, marker_indices) in &markers_by_identity {
			let local_tentatives = tentatives_by_identity
				.get(identity)
				.cloned()
				.unwrap_or_default();
			if local_tentatives.is_empty() {
				let first = &plan.frames[marker_indices[0]];
				let committed = self
					.counter_eligible
					.committed_identity(
						&(first.physical.tenant_id, first.physical.key_identity),
						first.physical.location.hlc,
						first.origin,
					)
					.map_err(super::counter::CounterVisibilityError::io)?;
				if committed.is_none() {
					return Err(super::counter::CounterVisibilityError::Conflict.io());
				}
				// This marker pairs with a tentative in another slab. Its stored
				// offset is intentionally copied verbatim; cross-slab pairing is by
				// version identity, not by a coincident offset in this rewrite.
				for marker_index in marker_indices {
					plan.frames[*marker_index].marker_tentative_offset = None;
				}
				continue;
			}
			let first_value = plan.frames[local_tentatives[0]].value;
			if local_tentatives
				.iter()
				.any(|index| plan.frames[*index].value != first_value)
			{
				return Err(super::counter::CounterVisibilityError::Conflict.io());
			}
			let committed = self
				.counter_eligible
				.committed_identity(
					&(
						plan.frames[local_tentatives[0]].physical.tenant_id,
						plan.frames[local_tentatives[0]].physical.key_identity,
					),
					plan.frames[local_tentatives[0]].physical.location.hlc,
					plan.frames[local_tentatives[0]].origin,
				)
				.map_err(super::counter::CounterVisibilityError::io)?;
			if committed.is_some_and(|value| value != first_value) {
				return Err(super::counter::CounterVisibilityError::Conflict.io());
			}
			for marker_index in marker_indices {
				let marker = &plan.frames[*marker_index];
				let marker_offset = marker.marker_tentative_offset.unwrap_or(u64::MAX);
				let paired = local_tentatives.iter().any(|tentative_index| {
					plan.frames[*tentative_index].physical.location.offset == marker_offset
				});
				if !paired {
					return Err(super::counter::CounterVisibilityError::Conflict.io());
				}
				// Only a validated pair physically present in this source slab may
				// have its offset remapped in the replacement.
				plan.frames[*marker_index].marker_tentative_offset = Some(marker_offset);
			}
		}
		for tentative_indices in tentatives_by_identity.values() {
			let first = &plan.frames[tentative_indices[0]];
			if tentative_indices
				.iter()
				.any(|index| plan.frames[*index].value != first.value)
			{
				return Err(super::counter::CounterVisibilityError::Conflict.io());
			}
			let committed = self
				.counter_eligible
				.committed_identity(
					&(first.physical.tenant_id, first.physical.key_identity),
					first.physical.location.hlc,
					first.origin,
				)
				.map_err(super::counter::CounterVisibilityError::io)?;
			if committed.is_some_and(|value| value != first.value) {
				return Err(super::counter::CounterVisibilityError::Conflict.io());
			}
		}
		Ok(plan)
	}

	fn ordinary_compaction_records(
		records: &[CompactionRecord],
		counter_plan: &CounterRewritePlan,
	) -> Vec<CompactionRecord> {
		let counter_locations = counter_plan
			.frames
			.iter()
			.map(|frame| frame.physical.location)
			.collect::<HashSet<_>>();
		records
			.iter()
			.copied()
			.filter(|record| !counter_locations.contains(&record.old_location))
			.collect()
	}

	async fn append_counter_rewrite_frames(
		&self,
		replacement: &Arc<LocalSlabStore>,
		plan: &CounterRewritePlan,
	) -> std::io::Result<Vec<RelocatedCounterFrame>> {
		let mut relocated = Vec::with_capacity(plan.frames.len());
		let mut new_tentative_offsets = HashMap::new();
		for frame in plan
			.frames
			.iter()
			.filter(|frame| counter_frame_is_tentative(&frame.physical))
		{
			let new_location = replacement
				.append_counter_maintenance_frame(
					frame.physical.tenant_id,
					frame.physical.key_identity,
					frame.physical.location.hlc,
					core::CounterMaintenanceClass::TentativeV2,
					&frame.physical.payload,
				)
				.await?;
			let identity = counter_version_identity(
				frame.physical.tenant_id,
				frame.physical.key_identity,
				frame.physical.location.hlc,
				frame.origin,
			);
			new_tentative_offsets.insert(
				(identity, frame.physical.location.offset),
				new_location.offset,
			);
			if frame.indexed {
				replacement.index_location(
					frame.physical.tenant_id,
					frame.physical.key_identity,
					new_location,
				);
			}
			relocated.push(RelocatedCounterFrame {
				old_location: frame.physical.location,
				new_location,
				tenant_id: frame.physical.tenant_id,
				key_identity: frame.physical.key_identity,
				routing_locality: self.routing_locality_for_key(
					frame.physical.tenant_id,
					frame.physical.key_identity,
				),
				indexed: frame.indexed,
			});
		}
		for frame in plan
			.frames
			.iter()
			.filter(|frame| counter_frame_is_marker(&frame.physical))
		{
			let mut payload = frame.physical.payload.clone();
			if let Some(old_tentative_offset) = frame.marker_tentative_offset {
				let identity = counter_version_identity(
					frame.physical.tenant_id,
					frame.physical.key_identity,
					frame.physical.location.hlc,
					frame.origin,
				);
				let new_offset = new_tentative_offsets
					.get(&(identity, old_tentative_offset))
					.copied()
					.ok_or_else(|| super::counter::CounterVisibilityError::Conflict.io())?;
				payload[..8].copy_from_slice(&new_offset.to_le_bytes());
			}
			let new_location = replacement
				.append_counter_maintenance_frame(
					frame.physical.tenant_id,
					frame.physical.key_identity,
					frame.physical.location.hlc,
					core::CounterMaintenanceClass::CommitMarkerV2,
					&payload,
				)
				.await?;
			relocated.push(RelocatedCounterFrame {
				old_location: frame.physical.location,
				new_location,
				tenant_id: frame.physical.tenant_id,
				key_identity: frame.physical.key_identity,
				routing_locality: self.routing_locality_for_key(
					frame.physical.tenant_id,
					frame.physical.key_identity,
				),
				indexed: false,
			});
		}
		Ok(relocated)
	}

	pub async fn recluster_sealed_table_layout(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		table_schemas: &TableSchemaCatalog,
		secondary_indexes: &SecondaryIndexCatalog,
		max_slabs: usize,
	) -> std::io::Result<LayoutReclusterReport> {
		self.require_tier_authority_ready()?;
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
		tombstones: &[MigratedRangeTombstone],
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<TombstoneSlabScanOutcome> {
		self.require_tier_authority_ready()?;
		// ADR 0097: own the slab BEFORE scanning it, then refetch it. Scanning
		// unowned let this job plan from bytes another maintenance job had already
		// replaced; and the handle it was called with predates the wait for
		// ownership, so after waiting it can name a superseded image.
		let Some((claims, slab)) = self.claim_sealed_slab_for_maintenance(slab_id).await else {
			return Ok(TombstoneSlabScanOutcome {
				scan: SealedSlabScan::Unclaimed,
				compaction: None,
			});
		};
		let counter_plan = self.plan_counter_rewrite(&slab).await?;
		if counter_plan.pending {
			return Ok(TombstoneSlabScanOutcome {
				scan: SealedSlabScan::Deferred,
				compaction: None,
			});
		}
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
		records = Self::ordinary_compaction_records(&records, &counter_plan);
		records.sort_by_key(|record| record.old_location.offset);

		let scan = SealedSlabScan::after_scan(&records);
		let compaction = self
			.compact_sealed_slab_records_planned(
				slab_id,
				slab,
				&records,
				secondary_index_context,
				&claims,
				&counter_plan,
			)
			.await?;
		Ok(TombstoneSlabScanOutcome { scan, compaction })
	}

	pub(super) async fn recluster_one_sealed_slab(
		&self,
		slab_id: SlabId,
		// Unused: `recluster_one_sealed_slab` refetches after claiming.
		_stale_handle: Arc<LocalSlabStore>,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Option<LayoutReclusterOutcome>> {
		self.require_tier_authority_ready()?;
		if self.is_compaction_slab_quarantined(slab_id) {
			return Ok(None);
		}
		if self
			.compaction_manifest_snapshot()
			.run(slab_id)
			.is_some_and(|run| !run.state.is_live())
		{
			return Ok(None);
		}
		// ADR 0097: own the slab before scanning it, like every other rewrite path,
		// and refetch it afterwards - the handle this was called with predates the
		// wait for ownership. Recluster reads the whole slab index below.
		let Some((_claims, slab)) = self.claim_sealed_slab_for_maintenance(slab_id).await else {
			return Ok(None);
		};
		let counter_plan = self.plan_counter_rewrite(&slab).await?;
		if counter_plan.pending {
			return Ok(None);
		}
		self.ensure_live_lsm_run_for_maintenance(slab_id, slab.write_cursor());
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
		records = Self::ordinary_compaction_records(&records, &counter_plan);
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
		let replacement_path =
			maintenance_staged_slab_path(&self.config.dir, slab_id, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let counter_relocations = self
			.append_counter_rewrite_frames(&replacement, &counter_plan)
			.await?;
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
				&counter_relocations,
				&replacement_path,
			)
			.await?
		{
			let _ = unlink_local_file(self.file_ops.as_ref(), &replacement_path).await;
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
			Ok(_proof) => {}
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

	/// ADR 0097: `claims` is the caller's proof that it already owns `slab_id`.
	/// The claim used to be taken here, which meant the caller had scanned the
	/// slab unowned and could arrive with a plan built from bytes another job had
	/// already replaced. Ownership is now hoisted above the scan by every caller.
	pub(super) async fn compact_sealed_slab_records(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
		claims: &MaintenanceClaims,
	) -> std::io::Result<Option<CompactionOutcome>> {
		self.require_tier_authority_ready()?;
		let counter_plan = self.plan_counter_rewrite(&slab).await?;
		self.compact_sealed_slab_records_planned(
			slab_id,
			slab,
			records,
			secondary_index_context,
			claims,
			&counter_plan,
		)
		.await
	}

	async fn compact_sealed_slab_records_planned(
		&self,
		slab_id: SlabId,
		slab: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		secondary_index_context: SecondaryIndexCompactionContext<'_>,
		claims: &MaintenanceClaims,
		counter_plan: &CounterRewritePlan,
	) -> std::io::Result<Option<CompactionOutcome>> {
		if !claims.covers(slab_id) {
			return Err(deterministic_failure(
				ErrorKind::InvalidInput,
				DeterministicOrigin::MaintenanceRewriteWithoutClaim,
				format!(
					"maintenance rewrite of slab {slab_id} without holding its claim; \
					 the job owns {:?}",
					claims.slabs()
				),
			));
		}
		if counter_plan.pending {
			return Ok(None);
		}
		let mut records = Self::ordinary_compaction_records(records, counter_plan);
		self.keep_conflicting_versions(&mut records);
		if records.is_empty() && counter_plan.frames.is_empty()
			|| records.iter().all(|record| !record.reclaim) && counter_plan.frames.is_empty()
		{
			return Ok(None);
		}
		// Quarantined work is not retried at every tick; repair is an explicit
		// future operator decision.
		if self.is_compaction_slab_quarantined(slab_id) {
			return Ok(None);
		}
		if self
			.compaction_manifest_snapshot()
			.run(slab_id)
			.is_some_and(|run| !run.state.is_live())
		{
			return Ok(None);
		}
		// Ownership is the caller's, proven by `claims` above.
		self.ensure_live_lsm_run_for_maintenance(slab_id, slab.write_cursor());
		let old_bytes = slab.write_cursor();
		let reclaimed_records = records.iter().filter(|record| record.reclaim).count();
		let retention_stats = Self::retention_compaction_stats(&records);
		let survivors = records.iter().filter(|record| !record.reclaim).count();
		// Enumerated before any mutation so the descriptors reflect the state the
		// retiring slab actually published. Failure fails the compaction, untouched.
		let old_secondary_index_descriptors = self
			.secondary_index_compaction_descriptors_for_source_slab(
				slab_id,
				secondary_index_context,
			)
			.await?;
		if survivors == 0 && counter_plan.frames.is_empty() {
			// A fully-dead slab has no legitimate reader after the admission
			// tombstone. Take the pin gate only at the first irreversible step;
			// holding it across a survivor rewrite would incorrectly reject
			// readers of live co-resident rows.
			let Some(_delete_pending) = self.slab_pins.mark_delete_pending_guarded(slab_id) else {
				return Ok(None);
			};
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
		let replacement_path =
			maintenance_staged_slab_path(&self.config.dir, slab_id, replacement_id);
		let replacement = Arc::new(
			LocalSlabStore::open(
				&replacement_path,
				replacement_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		let counter_relocations = self
			.append_counter_rewrite_frames(&replacement, counter_plan)
			.await?;
		let mut relocations = Vec::with_capacity(survivors);
		let physical_groups = physical_compaction_groups(&records);
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
				&counter_relocations,
				&replacement_path,
			)
			.await?
		{
			let _ = unlink_local_file(self.file_ops.as_ref(), &replacement_path).await;
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
			Ok(_proof) => {}
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
		let _publication_gate = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		{
			let state = self.state.lock().await;
			if !state
				.sealed
				.get(&slab_id)
				.is_some_and(|slab| slab.manager_appends_quiescent())
				|| state.tiered.contains_key(&slab_id)
			{
				return Ok(());
			}
		}
		self.bump_slab_publication_revision(slab_id)?;
		let deleted_rows = records
			.iter()
			.map(|record| {
				self.persistent_record_row(
					record.tenant_id,
					record.key_identity,
					record.old_location,
				)
			})
			.collect::<Vec<_>>();
		let lsm_runs = self.maintenance_manifest_preview(slab_id, None)?;
		let index_publication = self.begin_index_publication(vec![slab_id]).await?;
		// D2′: records and the old run's Retired disposition become durable in
		// one transaction before the in-memory index or backing changes.
		self.persist_maintenance_publication(
			PersistentIndexUpdate {
				delete_records: deleted_rows,
				put_lsm_runs: lsm_runs,
				..Default::default()
			},
			&index_publication,
		)
		.await?;
		self.apply_maintenance_manifest_publication(slab_id, None)?;
		{
			let state = self.state.lock().await;
			if state.sealed.remove(&slab_id).is_some() {
				self.release_slab_file(slab_id);
			}
		}
		for record in records {
			self.rollback_indexed_location(
				record.tenant_id,
				record.key_identity,
				record.old_location,
			);
		}
		index_publication.complete();
		// No negative reference scan here: this path runs only after
		// `reconcile_secondary_index_segments_after_compaction` returned proof. The
		// scan-based `can_retire_local_slab` guard remains on the sweep paths, where
		// no reconciliation proof exists.
		self.unlink_released_slab_file(slab_id).await?;
		Ok(())
	}

	pub(super) async fn finish_compaction_with_replacement(
		&self,
		old_slab_id: SlabId,
		new_slab_id: SlabId,
		replacement: Arc<LocalSlabStore>,
		records: &[CompactionRecord],
		relocations: &[RelocatedRecord],
		counter_relocations: &[RelocatedCounterFrame],
		staged_path: &Path,
	) -> std::io::Result<bool> {
		let publication_gate = self
			.maintenance_publication_gate
			.clone()
			.write_owned()
			.await;
		let tier_lock = self.tier_publication_lock(old_slab_id);
		let _tier_guard = tier_lock.lock().await;
		{
			let state = self.state.lock().await;
			if !state
				.sealed
				.get(&old_slab_id)
				.is_some_and(|slab| slab.manager_appends_quiescent())
				|| state.tiered.contains_key(&old_slab_id)
			{
				return Ok(false);
			}
		}
		self.bump_slab_publication_revision(old_slab_id)?;
		let survivor_count = records.iter().filter(|record| !record.reclaim).count();
		if relocations.len() != survivor_count
			|| relocations.iter().any(|relocation| {
				records
					.iter()
					.any(|record| record.reclaim && record.old_location == relocation.old_location)
			}) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"maintenance rewrite relocation set does not equal the declared survivor set",
			));
		}
		if counter_relocations.iter().any(|relocation| {
			relocation.old_location.slab_id != old_slab_id
				|| relocation.new_location.slab_id != new_slab_id
		}) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"counter maintenance relocation escaped its source/replacement slab pair",
			));
		}
		let mut relocation_tables = HashMap::new();
		for relocation in relocations {
			if let Some(table_object_id) =
				self.published_table_object_id(relocation.old_location)?
			{
				relocation_tables.insert(relocation.old_location, table_object_id);
			}
		}

		let mut removed_rows = records
			.iter()
			.map(|record| {
				self.persistent_record_row(
					record.tenant_id,
					record.key_identity,
					record.old_location,
				)
			})
			.collect::<Vec<_>>();
		removed_rows.extend(
			counter_relocations
				.iter()
				.filter(|relocation| relocation.indexed)
				.map(|relocation| {
					self.persistent_record_row(
						relocation.tenant_id,
						relocation.key_identity,
						relocation.old_location,
					)
				}),
		);
		let mut inserted_rows = relocations
			.iter()
			.map(|relocation| PersistentRecordRow {
				tenant_id: relocation.tenant_id,
				key_identity: relocation.key_identity,
				routing_locality: relocation.routing_locality,
				table_object_id: relocation_tables.get(&relocation.old_location).copied(),
				placement: relocation.new_location.placement,
				partition: relocation.new_location.partition,
				location: relocation.new_location,
			})
			.collect::<Vec<_>>();
		inserted_rows.extend(
			counter_relocations
				.iter()
				.filter(|relocation| relocation.indexed)
				.map(|relocation| PersistentRecordRow {
					tenant_id: relocation.tenant_id,
					key_identity: relocation.key_identity,
					routing_locality: relocation.routing_locality,
					table_object_id: None,
					placement: relocation.new_location.placement,
					partition: relocation.new_location.partition,
					location: relocation.new_location,
				}),
		);
		let min_hlc = relocations
			.iter()
			.map(|relocation| relocation.new_location.hlc)
			.chain(
				counter_relocations
					.iter()
					.map(|relocation| relocation.new_location.hlc),
			)
			.min();
		let max_hlc = relocations
			.iter()
			.map(|relocation| relocation.new_location.hlc)
			.chain(
				counter_relocations
					.iter()
					.map(|relocation| relocation.new_location.hlc),
			)
			.max();
		let old_run = self
			.compaction_manifest_snapshot()
			.run(old_slab_id)
			.cloned()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!("maintenance source run {old_slab_id} is absent"),
				)
			})?;
		let replacement_run = LsmRunSummary::new(
			new_slab_id,
			old_run.compaction_level,
			min_hlc.unwrap_or_else(zero_hlc),
			max_hlc.unwrap_or_else(zero_hlc),
			ByteCount::new(replacement.write_cursor()),
			RowCount::new(
				(relocations.len()
					+ counter_relocations
						.iter()
						.filter(|relocation| relocation.indexed)
						.count()) as u64,
			),
			old_run.manifest_epoch,
		);
		let lsm_runs =
			self.maintenance_manifest_preview(old_slab_id, Some(replacement_run.clone()))?;

		let mut by_key: BTreeMap<TenantKey, KeyRelocation> = BTreeMap::new();
		let mut relocated_sources = HashSet::new();
		for relocation in relocations {
			relocated_sources.insert(relocation.old_location);
			by_key
				.entry((relocation.tenant_id, relocation.key_identity))
				.or_insert_with(|| KeyRelocation::new(relocation.routing_locality, true))
				.pairs
				.push(RelocationPair {
					old: relocation.old_location,
					new: relocation.new_location,
					table_object_id: relocation_tables.get(&relocation.old_location).copied(),
				});
		}
		for relocation in counter_relocations
			.iter()
			.filter(|relocation| relocation.indexed)
		{
			relocated_sources.insert(relocation.old_location);
			by_key
				.entry((relocation.tenant_id, relocation.key_identity))
				.or_insert_with(|| KeyRelocation::new(relocation.routing_locality, true))
				.pairs
				.push(RelocationPair {
					old: relocation.old_location,
					new: relocation.new_location,
					table_object_id: None,
				});
		}
		for record in records {
			if !relocated_sources.contains(&record.old_location) {
				by_key
					.entry((record.tenant_id, record.key_identity))
					.or_insert_with(|| KeyRelocation::new(record.routing_locality, true))
					.removals
					.push(record.old_location);
			}
		}
		// The maintenance and source-tier guards exclude production removals
		// through D3. Refuse a stale survivor set before D2: a published output
		// must have at least one live source, including coalesced physical copies.
		for (key, relocation) in &by_key {
			let versions = self.key_index.get(key);
			let available = relocation
				.pairs
				.iter()
				.filter(|pair| {
					versions
						.as_ref()
						.is_some_and(|chain| chain.contains(&pair.old))
				})
				.map(|pair| pair.new)
				.collect::<HashSet<_>>();
			if relocation
				.pairs
				.iter()
				.any(|pair| !available.contains(&pair.new))
			{
				return Ok(false);
			}
		}

		let index_publication = self
			.begin_index_publication(vec![old_slab_id, new_slab_id])
			.await?;
		// D2: make the record relocation and manifest disposition durable before
		// a final-path file exists or any live lookup starts using the replacement.
		let published_lsm_runs = lsm_runs.clone();
		self.persist_maintenance_publication(
			PersistentIndexUpdate {
				delete_records: removed_rows,
				put_records: inserted_rows,
				put_lsm_runs: lsm_runs,
				..Default::default()
			},
			&index_publication,
		)
		.await?;

		// D1′: live installation invokes the same S1 reconciler startup uses.
		// Only a durable publication is ever allowed to occupy the normal
		// discovery namespace.
		let expected_staged_path =
			maintenance_staged_slab_path(&self.config.dir, old_slab_id, new_slab_id);
		if staged_path != expected_staged_path
			|| self.file_ops.file_len(staged_path).await?.is_none()
		{
			let err = Error::new(
				ErrorKind::NotFound,
				format!(
					"committed maintenance publication {old_slab_id}->{new_slab_id} lost its staged replacement before D1′"
				),
			);
			self.enter_maintenance_publication_fail_stop(publication_gate);
			return Err(err);
		}
		if let Err(err) = reconcile_staged_maintenance_publications(
			self.file_ops.as_ref(),
			&self.config.dir,
			Some(&published_lsm_runs),
			Some((old_slab_id, new_slab_id)),
		)
		.await
		{
			self.enter_maintenance_publication_fail_stop(publication_gate);
			return Err(err);
		}

		// D3: update the in-memory manifest and record maps while the planner gate
		// excludes a plan derived from the pre-publication Live run. Keep the old
		// backing sealed for stale readers; Phase D owns its eventual disposal.
		self.apply_maintenance_manifest_publication(old_slab_id, Some(replacement_run))?;
		// One relocation per key: every survivor (and indexed counter frame) is
		// replaced by its new location, and reclaimed records are removed, in a
		// single key critical section. A stale holder of an old location follows
		// the published redirect, or reads the retained old backing.
		let state = self.state.lock().await;
		state.sealed.insert(new_slab_id, replacement);
		let mut refused_rows = Vec::new();
		let mut relocated_any = false;
		for ((tenant_id, key_identity), relocation) in &by_key {
			for (_, new) in self.relocate_indexed_locations(*tenant_id, *key_identity, relocation) {
				refused_rows.push(self.persistent_record_row(*tenant_id, *key_identity, new));
			}
			if !relocated_any {
				relocated_any = true;
				#[cfg(test)]
				self.index_relocation_pause_point().await;
			}
		}
		drop(state);
		if !refused_rows.is_empty() {
			// D2 is already durable. A violated source-stability invariant must
			// retain the gate; deleting outputs in a second transaction would
			// create a crash window and could delete a still-live shared output.
			self.enter_maintenance_publication_fail_stop(publication_gate);
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"maintenance relocation refused after durable publication; restart required",
			));
		}
		index_publication.complete();
		Ok(true)
	}
}
