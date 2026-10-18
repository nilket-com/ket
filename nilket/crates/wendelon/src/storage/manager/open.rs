//! Slab manager initialization, carved without changing recovery order.
use super::*;

impl SlabManager {
	pub(super) async fn open_shared(
		mut config: SlabManagerConfig,
		owner: Option<DataDirectoryOwner>,
		#[cfg(feature = "test-support")] file_ops_override: Option<Arc<dyn AsyncFileOps>>,
	) -> std::io::Result<Self> {
		if let Some(owner) = &owner {
			owner.ensure_contains(&config.dir)?;
			config.io = config
				.io
				.clone()
				.try_with_data_directory_owner(owner.clone())?;
		}
		config.rehydration.validate()?;
		config.l2_column_fetch.validate()?;
		config.row_group_writer.validate()?;
		config.l2_row_group_fetch.validate()?;
		config.encoding_writer.validate()?;
		config.l2_chunk_layout.validate()?;
		config.l2_encoding_fetch.validate()?;
		config.encoding_compute.validate()?;
		if !config.dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"SlabManager directory does not exist: {}",
					config.dir.display()
				),
			));
		}

		#[cfg(not(feature = "test-support"))]
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(IoUringFileOps::with_isolated_blocking(
			config.io.clone(),
			"wendelon-fs-meta-slab",
			BlockingIoClass::SlabMetadata,
		)?);
		#[cfg(feature = "test-support")]
		let file_ops: Arc<dyn AsyncFileOps> = match file_ops_override {
			Some(file_ops) => match &owner {
				Some(owner) => file_ops.bind_directory_owner(owner.clone())?,
				None => file_ops,
			},
			None => Arc::new(IoUringFileOps::with_isolated_blocking(
				config.io.clone(),
				"wendelon-fs-meta-slab",
				BlockingIoClass::SlabMetadata,
			)?),
		};
		// Staged maintenance replacements are intentionally invisible to normal
		// slab discovery. Reconcile them before every discovery/allocator step so
		// a committed S1 publication reaches its final path and an S0 file cannot
		// reserve an id by accident on a rebuilt manager.
		let persistent_index = match PersistentLocalIndex::open_with_owner(
			persistent_index_path(&config.dir),
			owner.clone(),
		)
		.await
		{
			Ok(index) => Some(Arc::new(index)),
			Err(err) => {
				tracing::warn!(
					error = %err,
					dir = %config.dir.display(),
					"failed to open redb storage index; continuing with in-memory recovery"
				);
				None
			}
		};
		// ADR 0099: the durable rows double as slab recovery's boundary
		// oracle. A row exists only for a record that was fdatasynced and
		// then acknowledged, so its offset is a place the walk may resume
		// after an anomaly. Without redb there is no oracle and recovery
		// stops at the first anomaly.
		let mut acknowledged_by_slab: Option<BTreeMap<SlabId, BTreeSet<u64>>> = None;
		let staged_runs = match &persistent_index {
			Some(index) => match index.load_snapshot().await {
				Ok(snapshot) => {
					let mut by_slab: BTreeMap<SlabId, BTreeSet<u64>> = BTreeMap::new();
					for row in &snapshot.record_rows {
						by_slab
							.entry(row.location.slab_id)
							.or_default()
							.insert(row.location.offset);
					}
					acknowledged_by_slab = Some(by_slab);
					Some(snapshot.lsm_runs)
				}
				Err(err) => {
					tracing::warn!(
						error = %err,
						dir = %config.dir.display(),
						"failed to load redb before staged maintenance reconciliation; treating staged files as uninstalled"
					);
					None
				}
			},
			None => None,
		};
		let acknowledged_for = |id: SlabId| -> Option<BTreeSet<u64>> {
			acknowledged_by_slab
				.as_ref()
				.map(|by_slab| by_slab.get(&id).cloned().unwrap_or_default())
		};
		reconcile_staged_maintenance_publications(
			file_ops.as_ref(),
			&config.dir,
			staged_runs.as_deref(),
			None,
		)
		.await?;
		cleanup_retention_rewrite_tmp_files(file_ops.as_ref(), &config.dir).await?;
		cleanup_lsm_compaction_tmp_files(file_ops.as_ref(), &config.dir).await?;
		let mut existing_ids = discover_slab_ids(file_ops.as_ref(), &config.dir).await?;
		existing_ids.sort_unstable();

		let key_index: DashMap<TenantKey, Vec<RecordLocation>> = DashMap::new();
		let table_by_location: DashMap<RecordLocation, (TenantId, TableObjectId)> = DashMap::new();
		let table_references_by_slab: DashMap<SlabId, BTreeMap<(TenantId, TableObjectId), usize>> =
			DashMap::new();
		let secondary_index_table_references_by_slab: DashMap<
			SlabId,
			BTreeMap<(TenantId, TableObjectId), usize>,
		> = DashMap::new();
		let sealed: DashMap<SlabId, Arc<LocalSlabStore>> = DashMap::new();
		let tiered: DashMap<SlabId, TieredSlabPointer> = DashMap::new();
		let rehydrated: DashMap<SlabId, RehydratedSlabState> = DashMap::new();
		let secondary_index_segments: DashMap<
			PersistentSecondaryIndexSegmentKey,
			PersistentSecondaryIndexSegmentRow,
		> = DashMap::new();
		let secondary_index_coverage: DashMap<
			SecondaryIndexCoverageKey,
			SecondaryIndexCoverageTracker,
		> = DashMap::new();

		let mut recovered_key_localities: Vec<(TenantId, Identity, Locality)> = Vec::new();
		let (active, next_slab_id) = if existing_ids.is_empty() {
			let id = SlabId::ZERO;
			let slab = Arc::new(
				LocalSlabStore::open_with_acknowledged(
					slab_path(&config.dir, id),
					id,
					file_ops.clone(),
					config.group_commit.clone(),
					acknowledged_for(id).as_ref(),
				)
				.await?,
			);
			drain_slab_index(&slab, &key_index, &mut recovered_key_localities);
			(slab, id.saturating_add(1))
		} else {
			// Split: everything except the max becomes sealed, the
			// max becomes active. Zero-length sealed files are not opened:
			// they cannot contain records, and retaining them would recreate
			// the descriptor exhaustion that this recovery path bounds.
			let (&active_id, sealed_ids) = existing_ids.split_last().expect("non-empty");
			let active_path = slab_path(&config.dir, active_id);
			let active_len = file_ops.file_len(&active_path).await?.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"active slab disappeared during recovery: {}",
						active_path.display()
					),
				)
			})?;
			let mut non_empty_sealed_ids = Vec::with_capacity(sealed_ids.len());
			for &id in sealed_ids {
				let path = slab_path(&config.dir, id);
				match file_ops.file_len(&path).await? {
					Some(0) => continue,
					Some(_) => non_empty_sealed_ids.push(id),
					None => {
						return Err(Error::new(
							ErrorKind::NotFound,
							format!("slab disappeared during recovery: {}", path.display()),
						));
					}
				}
			}
			let non_empty_count = non_empty_sealed_ids.len() + usize::from(active_len > 0);
			let open_count = non_empty_sealed_ids.len() + 1;
			if open_count > MAX_RECOVERY_OPEN_SLABS {
				return Err(Error::new(
					ErrorKind::Other,
					format!(
						"bounded slab recovery refused: total slab files={}, non-empty slab files={}, slabs to open={}, bound={}; preserve the active slab and use the documented recovery path",
						existing_ids.len(),
						non_empty_count,
						open_count,
						MAX_RECOVERY_OPEN_SLABS,
					),
				));
			}
			// Drain into key_index in ascending id order so later writes
			// shadow earlier ones.
			for &id in &non_empty_sealed_ids {
				let slab = Arc::new(
					LocalSlabStore::open_with_acknowledged(
						slab_path(&config.dir, id),
						id,
						file_ops.clone(),
						config.group_commit.clone(),
						acknowledged_for(id).as_ref(),
					)
					.await?,
				);
				drain_slab_index(&slab, &key_index, &mut recovered_key_localities);
				sealed.insert(id, slab);
			}
			let active = Arc::new(
				LocalSlabStore::open_with_acknowledged(
					active_path,
					active_id,
					file_ops.clone(),
					config.group_commit.clone(),
					acknowledged_for(active_id).as_ref(),
				)
				.await?,
			);
			drain_slab_index(&active, &key_index, &mut recovered_key_localities);
			(active, active_id.saturating_add(1))
		};

		let counter_unsupported = DashSet::new();
		let mut persistent_key_localities: Vec<(TenantId, Identity, Locality)> = Vec::new();
		let mut recovered_lsm_runs: Vec<LsmRunSummary> = Vec::new();
		if let Some(index) = &persistent_index {
			match index.load_snapshot().await {
				Ok(snapshot) => {
					for row in snapshot.record_rows {
						if let Some(table_object_id) = row.table_object_id {
							table_by_location
								.insert(row.location, (row.tenant_id, table_object_id));
							*table_references_by_slab
								.entry(row.location.slab_id)
								.or_default()
								.entry((row.tenant_id, table_object_id))
								.or_default() += 1;
						}
					}
					for pointer in snapshot.tiered_slabs {
						tiered.insert(pointer.slab_id, pointer);
					}
					for row in snapshot.tiered_records {
						counter_unsupported.insert((row.tenant_id, row.key_identity));
						key_index
							.entry((row.tenant_id, row.key_identity))
							.and_modify(|versions| insert_version_sorted(versions, row.location))
							.or_insert_with(|| vec![row.location]);
						persistent_key_localities.push((
							row.tenant_id,
							row.key_identity,
							row.routing_locality,
						));
					}
					for row in snapshot.secondary_index_segments {
						*secondary_index_table_references_by_slab
							.entry(row.key.source_slab_id)
							.or_default()
							.entry((row.key.tenant_id, row.key.table_object_id))
							.or_default() += 1;
						let coverage_key = secondary_index_coverage_key_from_persistent(&row.key);
						secondary_index_coverage
							.entry(coverage_key)
							.or_insert_with(|| SecondaryIndexCoverageTracker::new(coverage_key))
							.mark_covered(row.key.source_slab_id);
						secondary_index_segments.insert(row.key, row);
					}
					persistent_key_localities.extend(snapshot.record_localities);
					recovered_lsm_runs = snapshot.lsm_runs;
				}
				Err(err) => {
					tracing::warn!(
						error = %err,
						dir = %config.dir.display(),
						"failed to load tiered rows from redb; local slab scan remains authoritative"
					);
				}
			}
		}
		let counter_eligible =
			Self::recover_counter_eligible(&active, &sealed, &counter_unsupported, &key_index);
		remove_non_live_lsm_rows_from_index(&key_index, &recovered_lsm_runs);
		let key_localities = build_key_localities(&key_index);
		for (tenant_id, key_identity, locality) in recovered_key_localities {
			if key_index.contains_key(&(tenant_id, key_identity)) {
				key_localities.insert((tenant_id, key_identity), locality);
			}
		}
		for (tenant_id, key_identity, locality) in persistent_key_localities {
			if key_index.contains_key(&(tenant_id, key_identity)) {
				key_localities.insert((tenant_id, key_identity), locality);
			}
		}
		for entry in key_index.iter() {
			validate_row_tombstone_chain(entry.value())?;
		}
		let locality_index = build_locality_index(&key_localities);
		let recovered_manifest_epoch = recovered_lsm_runs
			.iter()
			.map(|run| run.manifest_epoch)
			.max()
			.unwrap_or(ManifestEpoch::ZERO);
		let mut compaction_manifest = if recovered_lsm_runs.is_empty() {
			CompactionManifest::new(ManifestEpoch::ZERO)
		} else {
			CompactionManifest::from_recovered_runs(recovered_manifest_epoch, recovered_lsm_runs)
		};
		for entry in sealed.iter() {
			if compaction_manifest.get_run(*entry.key()).is_none() {
				compaction_manifest.insert_live_run(lsm_run_summary_from_index(
					&key_index,
					*entry.key(),
					entry.value().write_cursor(),
					compaction_manifest.manifest_epoch(),
				));
			}
		}

		// ADR 0097 decision 4: admission truth is hydrated ONCE, before the manager
		// exists, from the same index handle serving uses. `None` is not empty
		// history - it is the absence of a record, and no record means no maintenance.
		let maintenance_policy = match &persistent_index {
			Some(index) => {
				MaintenancePolicyInner::load(Arc::clone(index), Arc::new(SystemMaintenanceClock))
					.await
			}
			None => MaintenancePolicyInner::disabled(
				"no persistent index: maintenance receipts have nowhere to live",
			),
		};
		let manager = Self {
			_directory_owner: owner,
			finality_observation:
				crate::storage::finality_observation::FinalityObservationRegistry::new(),
			config,
			file_ops,
			state: Mutex::new(ManagerState {
				active,
				sealed,
				tiered,
				rehydrated,
				hot_read_counts: HashMap::new(),
				rehydrating: HashSet::new(),
				next_slab_id,
			}),
			compaction_manifest: RwLock::new(compaction_manifest),
			slab_pins: Arc::new(SlabPinTable::default()),
			counter_eligible,
			counter_unsupported,
			counter_pending_frames: DashSet::new(),
			counter_deferred_l0_slabs: DashSet::new(),
			key_index,
			locality_index: RwLock::new(locality_index),
			key_localities,
			table_by_location,
			table_references_by_slab,
			secondary_index_table_references_by_slab,
			integrity_failures: DashMap::new(),
			quarantined_compaction_slabs: DashSet::new(),
			relocated_reads: DashMap::new(),
			persistent_index,
			maintenance_policy,
			retired_tiered_cleanup: DashMap::new(),
			secondary_index_segments,
			secondary_index_coverage,
			secondary_index_admission_gate: Arc::new(AsyncRwLock::new(())),
			maintenance_publication_gate: Arc::new(AsyncRwLock::new(())),
			maintenance_publication_restart_required: AtomicBool::new(false),
			maintenance_publication_fail_stop: std::sync::Mutex::new(None),
			secondary_index_declaration_cuts: DashSet::new(),
			slab_rewrite_claims: DashMap::new(),
			tiered_stats_cache: DashMap::new(),
		};
		manager
			.detect_missing_published_references_on_recovery()
			.await;
		let snapshot = manager.compaction_manifest_snapshot();
		refresh_lsm_level_metrics(&snapshot);
		if manager.integrity_failures.is_empty() {
			manager.rebuild_persistent_index().await;
		} else {
			tracing::error!(
				fenced_table_count = manager.integrity_failures.len(),
				"published reference integrity failure detected during recovery; preserving durable index evidence"
			);
		}
		Ok(manager)
	}
}
