//! Slab manager initialization, carved without changing recovery order.
use super::*;

/// Explicit capability, minted only after a restore intent and control replay agree.
pub(crate) struct IndexRecoveryAuthority {
	owner: DataDirectoryOwner,
}
impl IndexRecoveryAuthority {
	pub(crate) fn verified_restore(owner: DataDirectoryOwner) -> Self {
		Self { owner }
	}
	fn check(&self, owner: &DataDirectoryOwner, dir: &std::path::Path) -> std::io::Result<()> {
		if !self.owner.same_owner(owner) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"index recovery authority belongs to another owner",
			));
		}
		owner.ensure_path(dir)
	}
}

/// Durable rows recovery could not find in the recovered index. Opening would
/// serve those keys without them and the rebuild would erase their rows, so
/// the open refuses. The diagnostic carries no row key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnaccountedDurableVersions {
	pub versions: usize,
	pub slabs: BTreeSet<SlabId>,
}

impl std::fmt::Display for UnaccountedDurableVersions {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			formatter,
			"{} durable record versions in slabs {:?} were not recovered; recovery refused",
			self.versions, self.slabs
		)
	}
}
impl std::error::Error for UnaccountedDurableVersions {}

/// Each durable version with the table it is attributed to, if any.
type DurableVersions = HashMap<(TenantId, Identity, RecordLocation), Option<TableObjectId>>;

/// A typed row the recovered index lacks is accounted for only by its table's
/// published-reference fence: that fence refuses every read of the table and
/// keeps the durable index from being rebuilt. Untyped rows have no such
/// fence, so for them the open itself refuses.
pub(super) fn refuse_unaccounted_durable_versions(
	manager: &SlabManager,
	mut durable_versions: DurableVersions,
) -> std::io::Result<()> {
	for entry in manager.key_index.iter() {
		let (tenant_id, key_identity) = *entry.key();
		for location in entry.value() {
			durable_versions.remove(&(tenant_id, key_identity, *location));
		}
	}
	durable_versions.retain(|&(tenant_id, _, _), table_object_id| {
		table_object_id.is_none_or(|table_object_id| {
			manager
				.table_integrity_failure(tenant_id, table_object_id)
				.is_none()
		})
	});
	if durable_versions.is_empty() {
		return Ok(());
	}
	let slabs = durable_versions
		.keys()
		.map(|(_, _, location)| location.slab_id)
		.collect();
	Err(Error::new(
		ErrorKind::InvalidData,
		UnaccountedDurableVersions {
			versions: durable_versions.len(),
			slabs,
		},
	))
}

/// How `open_shared` treats the durable index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StoreOpen {
	/// Open an existing store; its identity and high-water must be present.
	Existing,
	/// Create a store in a directory with no slab evidence.
	Fresh,
	/// Create the index over slab files a verified restore laid down; they are
	/// adopted as issued in the index's creating transaction.
	VerifiedRestore,
}

impl SlabManager {
	pub(crate) async fn initialize_recovered_store_with_owner(
		config: SlabManagerConfig,
		owner: DataDirectoryOwner,
		authority: &IndexRecoveryAuthority,
	) -> std::io::Result<Arc<Self>> {
		authority.check(&owner, &config.dir)?;
		Self::open_shared(
			config,
			Some(owner),
			crate::metrics::global(),
			StoreOpen::VerifiedRestore,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	#[cfg(feature = "test-support")]
	pub(crate) async fn initialize_recovered_store_with_file_ops_and_owner(
		config: SlabManagerConfig,
		file_ops: Arc<dyn AsyncFileOps>,
		owner: DataDirectoryOwner,
		authority: &IndexRecoveryAuthority,
	) -> std::io::Result<Arc<Self>> {
		authority.check(&owner, &config.dir)?;
		Self::open_shared(
			config,
			Some(owner),
			crate::metrics::global(),
			StoreOpen::VerifiedRestore,
			Some(file_ops),
		)
		.await
	}

	/// Test fixtures that lay raw slab files down before the index exists
	/// adopt them explicitly, as a verified restore would.
	#[cfg(test)]
	pub(crate) async fn initialize_store_adopting_existing_slabs_for_test(
		config: SlabManagerConfig,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			crate::metrics::global(),
			StoreOpen::VerifiedRestore,
			None,
		)
		.await
	}

	/// Explicitly initialize a new store; the caller must establish Fresh.
	#[cfg(any(test, feature = "test-support"))]
	pub async fn initialize_new_store(config: SlabManagerConfig) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			crate::metrics::global(),
			StoreOpen::Fresh,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	/// Initialize a new owner-bound store after the node establishes Fresh.
	pub async fn initialize_new_store_with_owner(
		config: SlabManagerConfig,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			Some(owner),
			crate::metrics::global(),
			StoreOpen::Fresh,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	#[cfg(any(test, feature = "test-support"))]
	pub(crate) async fn initialize_new_store_with_file_ops_and_owner(
		config: SlabManagerConfig,
		file_ops: Arc<dyn AsyncFileOps>,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			Some(owner),
			crate::metrics::global(),
			StoreOpen::Fresh,
			Some(file_ops),
		)
		.await
	}

	#[cfg(any(test, feature = "test-support"))]
	pub async fn initialize_new_store_with_file_ops(
		config: SlabManagerConfig,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			crate::metrics::global(),
			StoreOpen::Fresh,
			Some(file_ops),
		)
		.await
	}

	#[cfg(test)]
	pub(crate) async fn initialize_new_store_with_maintenance_metrics(
		config: SlabManagerConfig,
		metrics: &'static crate::metrics::MetricsRegistry,
	) -> std::io::Result<Arc<Self>> {
		Self::open_shared(
			config,
			None,
			metrics,
			StoreOpen::Fresh,
			#[cfg(any(test, feature = "test-support"))]
			None,
		)
		.await
	}

	pub(super) async fn open_shared(
		mut config: SlabManagerConfig,
		owner: Option<DataDirectoryOwner>,
		maintenance_metrics: &'static crate::metrics::MetricsRegistry,
		mode: StoreOpen,
		#[cfg(any(test, feature = "test-support"))] file_ops_override: Option<
			Arc<dyn AsyncFileOps>,
		>,
	) -> std::io::Result<Arc<Self>> {
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

		#[cfg(not(any(test, feature = "test-support")))]
		let file_ops: Arc<dyn AsyncFileOps> = Arc::new(IoUringFileOps::with_isolated_blocking(
			config.io.clone(),
			"wendelon-fs-meta-slab",
			BlockingIoClass::SlabMetadata,
		)?);
		#[cfg(any(test, feature = "test-support"))]
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
		let awaiting_cluster = file_ops
			.file_len(&config.dir.join(AWAITING_CLUSTER_MARKER))
			.await?
			.is_some();
		let index_path = persistent_index_path(&config.dir);
		// An existing store whose index cannot be opened still starts fenced
		// (reads and writes refused until a reopen with the index). Without
		// identity and high-water nothing may be issued or created, so that
		// degraded open only opens existing slab files, without repair. An
		// old-format store (no identity) is refused outright.
		let index = match mode {
			StoreOpen::Existing => {
				match PersistentLocalIndex::open_with_owner(index_path, owner.clone()).await {
					Ok(index) => Some(index),
					Err(err)
						if crate::storage::index::is_store_identity_missing(&err)
							|| crate::storage::index::is_op_state_invalid(&err) =>
					{
						return Err(err);
					}
					Err(err) => {
						tracing::warn!(
							error = %err,
							dir = %config.dir.display(),
							"failed to open redb storage index; opening existing slabs fenced, without issuing ids"
						);
						None
					}
				}
			}
			StoreOpen::Fresh | StoreOpen::VerifiedRestore => {
				// Read-only census before the index exists. Only a verified
				// restore adopts slab files already present, and it records them
				// in the same transaction that writes the identity.
				let floor =
					slab_ids::slab_id_evidence_floor(file_ops.as_ref(), &config.dir, None).await?;
				if mode == StoreOpen::Fresh && floor > SlabId::ZERO {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!(
							"fresh store initialization found existing slab evidence up to id {} in {}; only a verified restore adopts existing slab files",
							floor.get() - 1,
							config.dir.display()
						),
					));
				}
				Some(
					PersistentLocalIndex::initialize_new_with_high_water(
						index_path,
						owner.clone(),
						floor.get(),
					)
					.await?,
				)
			}
		};
		// Validate H against the store's unmodified evidence before any
		// recovery step below may rename, unlink, clear or zero-fill it. Own
		// evidence at or above H means an id was used without being made
		// durable: open fails closed and never repairs H upward.
		let validated_high_water = match &index {
			None => None,
			Some(index) => Some({
				let high_water = SlabId::new(index.slab_id_high_water().await?);
				// The census must read the durable evidence it validates
				// against: a snapshot that fails to load is refused here, never
				// treated as "no durable evidence" by a store that can issue ids.
				let census = index.load_snapshot().await?;
				let floor =
					slab_ids::slab_id_evidence_floor(file_ops.as_ref(), &config.dir, Some(&census))
						.await?;
				if high_water < floor {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"slab id high-water {} is below this store's own evidence floor {}",
							high_water.get(),
							floor.get()
						),
					));
				}
				tracing::info!(
					dir = %config.dir.display(),
					incarnation = %format_args!("{:032x}", index.store_incarnation().get()),
					slab_id_high_water = high_water.get(),
					"opened store"
				);
				high_water
			}),
		};
		let persistent_index = index.map(Arc::new);
		let pending_metadata_tokens = DashMap::new();
		if let Some(index) = &persistent_index {
			index.recover_metadata_tokens().await?;
			for (slab, tokens) in index.load_pending_metadata_tokens().await? {
				pending_metadata_tokens.insert(slab, tokens);
			}
		}
		// One redb snapshot serves every open-time decision, so the tier
		// authority that classifies slab files is the one the index loads.
		let mut snapshot = match &persistent_index {
			Some(index) => match index.load_snapshot().await {
				Ok(snapshot) => Some(snapshot),
				Err(err) => {
					tracing::warn!(
						error = %err,
						dir = %config.dir.display(),
						"failed to load redb at open; staged files are treated as uninstalled and the local slab scan remains authoritative"
					);
					None
				}
			},
			None => None,
		};
		// ADR 0703: the durable mode obligation, known before any install is
		// reconciled. A marked (ParentDecisions) store installs only certified
		// compactions; an unreadable marker refuses the open.
		let contained = crate::consensus::decision_mode::store_is_marked(&config.dir)?;
		// Local intent recovery runs before filename discovery. It removes an
		// intent only after proving its input/output files and preserving any
		// unpublished output under a non-discoverable quarantine name.
		if !awaiting_cluster && let (Some(index), Some(initial)) = (&persistent_index, &snapshot) {
			let intents = initial.lsm_install_intents.clone();
			let mut changed = false;
			let touched = intents
				.iter()
				.map(|intent| {
					intent
						.inputs
						.iter()
						.map(|input| input.slab_id)
						.chain(intent.commit.slabs_to_add.iter().map(|run| run.slab_id))
						.collect::<HashSet<_>>()
				})
				.collect::<Vec<_>>();
			for (position, intent) in intents.iter().enumerate() {
				if touched.iter().enumerate().any(|(other, slabs)| {
					other != position && !slabs.is_disjoint(&touched[position])
				}) {
					continue;
				}
				let result = match intent.kind {
					crate::storage::index::LsmInstallKind::Local => {
						lsm_driver::reconcile_local_lsm_intent(
							file_ops.as_ref(),
							&config.dir,
							index,
							initial,
							intent,
							contained,
						)
						.await
					}
					// ADR 0697: an intent linked to an operation is not recovered
					// from local evidence alone. It stays fenced until recovery
					// holds that operation's receipt and current authority.
					crate::storage::index::LsmInstallKind::Control if !intent.ops.is_empty() => {
						Ok(false)
					}
					crate::storage::index::LsmInstallKind::Control => {
						lsm_driver::reconcile_completed_control_lsm_intent(
							file_ops.as_ref(),
							&config.dir,
							index,
							initial,
							intent,
							contained,
						)
						.await
					}
				};
				match result {
					Ok(resolved) => changed |= resolved,
					Err(error) => {
						tracing::warn!(error = %error, job_id = ?intent.commit.job_id, "LSM intent remains fenced during recovery")
					}
				}
			}
			if changed {
				snapshot = match index.load_snapshot().await {
					Ok(reloaded) => Some(reloaded),
					Err(error) => {
						tracing::warn!(error = %error, "failed to reload index after LSM intent recovery");
						None
					}
				};
			}
		}
		let retained_secondary_sidecars = Arc::new(RetainedSecondarySidecars::default());
		if let Some(snapshot) = &snapshot {
			let referenced = snapshot
				.secondary_index_segments
				.iter()
				.map(|row| PathBuf::from(&row.segment_path))
				.collect();
			for (path, bytes) in secondary_index_cleanup::scan_retained_secondary_files(
				&config.dir,
				owner.clone(),
				referenced,
			)
			.await?
			{
				retained_secondary_sidecars.record(path, bytes);
			}
		}
		let index_authority_unavailable = snapshot.is_none();
		let lsm_install_intents = DashMap::new();
		if let Some(snapshot) = &snapshot {
			for intent in &snapshot.lsm_install_intents {
				lsm_install_intents.insert(
					intent.commit.job_id,
					lsm_driver::lsm_intent_scope(intent, true),
				);
			}
		}
		let lsm_install_unresolved = !lsm_install_intents.is_empty();
		// ADR 0703: a certificate whose install intent is gone is not reused or
		// cleared here; it fences certified LSM on this store until an operator
		// resolves it.
		let orphan_lsm_certificates = snapshot
			.as_ref()
			.map(|snapshot| {
				snapshot
					.lsm_certificates
					.iter()
					.filter(|(job, _)| {
						!snapshot
							.lsm_install_intents
							.iter()
							.any(|intent| intent.commit.job_id.get() == *job)
					})
					.map(|(job, _)| *job)
					.collect::<Vec<_>>()
			})
			.unwrap_or_default();
		if !orphan_lsm_certificates.is_empty() {
			tracing::error!(
				jobs = ?orphan_lsm_certificates,
				"LSM certificates without their install intents; certified LSM stays fenced"
			);
		}
		// ADR 0099: the durable rows double as slab recovery's boundary
		// oracle. A row exists only for a record that was fdatasynced and
		// then acknowledged, so its offset is a place the walk may resume
		// after an anomaly. Without redb there is no oracle and recovery
		// stops at the first anomaly.
		let acknowledged_by_slab: Option<BTreeMap<SlabId, BTreeSet<u64>>> =
			snapshot.as_ref().map(|snapshot| {
				let mut by_slab: BTreeMap<SlabId, BTreeSet<u64>> = BTreeMap::new();
				for row in &snapshot.record_rows {
					by_slab
						.entry(row.location.slab_id)
						.or_default()
						.insert(row.location.offset);
				}
				by_slab
			});
		let acknowledged_for = |id: SlabId| -> Option<BTreeSet<u64>> {
			acknowledged_by_slab
				.as_ref()
				.map(|by_slab| by_slab.get(&id).cloned().unwrap_or_default())
		};
		if !index_authority_unavailable && !lsm_install_unresolved && !awaiting_cluster {
			reconcile_staged_maintenance_publications(
				file_ops.as_ref(),
				&config.dir,
				snapshot
					.as_ref()
					.map(|snapshot| snapshot.lsm_runs.as_slice()),
				None,
			)
			.await?;
			cleanup_retention_rewrite_tmp_files(file_ops.as_ref(), &config.dir).await?;
			cleanup_lsm_compaction_tmp_files(file_ops.as_ref(), &config.dir).await?;
			// The manager is not published and no sidecar producer exists yet.
			secondary_index_cleanup::cleanup_secondary_index_tmp_files(&config.dir, owner.clone())
				.await?;
		}
		let mut discovered_ids = discover_slab_ids(file_ops.as_ref(), &config.dir).await?;
		discovered_ids.sort_unstable();
		// Unresolved output files are evidence, not authoritative rows. Keep
		// their names below the allocator floor, but never open or index them.
		let unresolved_outputs = snapshot
			.as_ref()
			.map(|snapshot| {
				snapshot
					.lsm_install_intents
					.iter()
					.flat_map(|intent| {
						intent
							.commit
							.slabs_to_add
							.iter()
							.map(|run| run.slab_id)
							.chain(intent.local_output_files.iter().map(|file| file.slab_id))
							.chain(
								intent
									.commit
									.sidecar_swaps
									.iter()
									.map(|swap| swap.output_slab_id),
							)
					})
					.collect::<BTreeSet<_>>()
			})
			.unwrap_or_default();
		// A reservation row means no durable publication has made this id a slab, whatever
		// is at its path, and its publication ends the row in one transaction. Opening a file
		// there would index an unpublished output, and recovery can rewrite it, destroying
		// the identity its cleanup is bound to. So it is evidence too.
		let unpublished_reservations = match &persistent_index {
			Some(index) => index
				.load_reserved_ids()
				.await?
				.into_iter()
				.map(|(id, _)| id)
				.collect::<BTreeSet<_>>(),
			None => BTreeSet::new(),
		};
		// A discovered id with a committed tier pointer is quarantined before
		// any file is opened: it serves only through its tier generation. Every
		// slab id redb names, and every discovered id, stays below the
		// allocator floor, so no new slab reuses one.
		let tier_pointer_ids = snapshot
			.as_ref()
			.map(|snapshot| {
				snapshot
					.tiered_slabs
					.iter()
					.map(|pointer| pointer.slab_id)
					.collect::<BTreeSet<_>>()
			})
			.unwrap_or_default();
		// A leftover file after a checked retirement is evidence, even though
		// its pointer is gone. Bare absence still recovers unknown local slabs.
		let retired_tier_ids = snapshot
			.as_ref()
			.map(|snapshot| {
				snapshot
					.retired_tiered_slabs
					.iter()
					.copied()
					.collect::<BTreeSet<_>>()
			})
			.unwrap_or_default();
		let allocator_floor = snapshot
			.as_ref()
			.map(|snapshot| {
				snapshot
					.record_rows
					.iter()
					.chain(snapshot.tiered_records.iter())
					.map(|row| row.location.slab_id)
					.chain(snapshot.lsm_runs.iter().map(|run| run.slab_id))
					.chain(
						snapshot
							.secondary_index_segments
							.iter()
							.map(|row| row.key.source_slab_id),
					)
					.max()
			})
			.into_iter()
			.flatten()
			.chain(tier_pointer_ids.iter().copied())
			.chain(retired_tier_ids.iter().copied())
			.chain(unresolved_outputs.iter().copied())
			.chain(discovered_ids.iter().copied())
			.max()
			.map(|id| {
				id.get()
					.checked_add(1)
					.map(SlabId::new)
					.ok_or_else(slab_ids_exhausted)
			})
			.transpose()?
			.unwrap_or(SlabId::ZERO);
		let highest_discovered_is_quarantined = discovered_ids.last().is_some_and(|id| {
			tier_pointer_ids.contains(id)
				|| retired_tier_ids.contains(id)
				|| unresolved_outputs.contains(id)
				|| unpublished_reservations.contains(id)
		});
		discovered_ids.retain(|id| {
			!unresolved_outputs.contains(id) && !unpublished_reservations.contains(id)
		});
		let (existing_ids, quarantined_slab_files) =
			slab_file_quarantine::classify_discovered_slab_files(
				file_ops.as_ref(),
				&config.dir,
				discovered_ids,
				&tier_pointer_ids,
				&retired_tier_ids,
			)
			.await?;

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
		// Split: everything except the max becomes sealed, the max becomes
		// active. When there is no local file, or the highest file is
		// quarantined, every local file is sealed and the active slab is a fresh
		// id at the floor. Zero-length sealed files are not opened: they cannot
		// contain records, and retaining them would recreate the descriptor
		// exhaustion that this recovery path bounds.
		let (active_id, sealed_ids) = match existing_ids.split_last() {
			Some((&active_id, sealed_ids)) if !highest_discovered_is_quarantined => {
				(Some(active_id), sealed_ids)
			}
			_ => (None, existing_ids.as_slice()),
		};
		let active_len = match active_id {
			Some(active_id) => {
				let active_path = slab_path(&config.dir, active_id);
				file_ops.file_len(&active_path).await?.ok_or_else(|| {
					Error::new(
						ErrorKind::NotFound,
						format!(
							"active slab disappeared during recovery: {}",
							active_path.display()
						),
					)
				})?
			}
			None => 0,
		};
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
				LocalSlabStore::open_recovered(
					slab_path(&config.dir, id),
					id,
					file_ops.clone(),
					config.group_commit.clone(),
					acknowledged_for(id).as_ref(),
					// A store without its durable index opens only existing slabs.
					persistent_index.is_some(),
				)
				.await?,
			);
			drain_slab_index(&slab, &key_index, &mut recovered_key_localities);
			sealed.insert(id, slab);
		}
		// Recovery only renames or removes evidence counted by the census
		// above, so the validated H still covers the floor here.
		if let Some(high_water) = validated_high_water
			&& high_water < allocator_floor
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"slab id high-water {} is below this store's recovered evidence floor {}",
					high_water.get(),
					allocator_floor.get()
				),
			));
		}
		// A fresh active slab starts at H, never below it, and H covers it
		// before its file exists. Without H there is no fresh active slab.
		let active_id = match (active_id, validated_high_water) {
			(Some(active_id), _) => active_id,
			(None, Some(high_water)) => high_water,
			(None, None) => {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"store in {} has no usable durable index and no existing active slab; no slab is created without its identity and high-water",
						config.dir.display()
					),
				));
			}
		};
		let active_end = active_id
			.get()
			.checked_add(1)
			.map(SlabId::new)
			.ok_or_else(slab_ids_exhausted)?;
		let durable_slab_id_bound = match (&persistent_index, validated_high_water) {
			(Some(index), Some(high_water)) if active_end > high_water => {
				let bound = active_end
					.get()
					.checked_add(SLAB_ID_LEASE)
					.ok_or_else(slab_ids_exhausted)?;
				index.persist_slab_id_high_water(bound).await?;
				SlabId::new(bound)
			}
			(_, high_water) => high_water.unwrap_or(SlabId::ZERO),
		};
		let active = Arc::new(
			LocalSlabStore::open_recovered(
				slab_path(&config.dir, active_id),
				active_id,
				file_ops.clone(),
				config.group_commit.clone(),
				acknowledged_for(active_id).as_ref(),
				// A store without its durable index opens only existing slabs.
				persistent_index.is_some(),
			)
			.await?,
		);
		drain_slab_index(&active, &key_index, &mut recovered_key_localities);
		// Every id below the validated H may have been issued before this open.
		let next_slab_id = active_end
			.max(allocator_floor)
			.max(validated_high_water.unwrap_or(SlabId::ZERO));

		// Every durable row names a version the node acknowledged. Recovery
		// replaces the durable index from memory, so each one must still be
		// held there; unresolved install outputs are evidence, not rows.
		let durable_versions: Option<DurableVersions> = snapshot.as_ref().map(|snapshot| {
			snapshot
				.record_rows
				.iter()
				.chain(snapshot.tiered_records.iter())
				.filter(|row| !unresolved_outputs.contains(&row.location.slab_id))
				.map(|row| {
					(
						(row.tenant_id, row.key_identity, row.location),
						row.table_object_id,
					)
				})
				.collect()
		});
		let counter_unsupported = DashSet::new();
		let mut persistent_key_localities: Vec<(TenantId, Identity, Locality)> = Vec::new();
		let mut recovered_lsm_runs: Vec<LsmRunSummary> = Vec::new();
		let mut lsm_manifest_epoch_floor = ManifestEpoch::ZERO;
		let recovered_control_index = snapshot
			.as_ref()
			.and_then(|snapshot| snapshot.last_control_log_index);
		if let Some(snapshot) = snapshot {
			for row in snapshot.record_rows {
				if unresolved_outputs.contains(&row.location.slab_id) {
					continue;
				}
				if let Some(table_object_id) = row.table_object_id {
					table_by_location.insert(row.location, (row.tenant_id, table_object_id));
					*table_references_by_slab
						.entry(row.location.slab_id)
						.or_default()
						.entry((row.tenant_id, table_object_id))
						.or_default() += 1;
				}
			}
			for pointer in snapshot.tiered_slabs {
				if !unresolved_outputs.contains(&pointer.slab_id) {
					tiered.insert(pointer.slab_id, pointer);
				}
			}
			for row in snapshot.tiered_records {
				if unresolved_outputs.contains(&row.location.slab_id) {
					continue;
				}
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
				// An unresolved output is held back, so a reference to it is no missing slab:
				// a deferred open counts it when it loads that output.
				if !unresolved_outputs.contains(&row.key.source_slab_id) {
					*secondary_index_table_references_by_slab
						.entry(row.key.source_slab_id)
						.or_default()
						.entry((row.key.tenant_id, row.key.table_object_id))
						.or_default() += 1;
				}
				let coverage_key = secondary_index_coverage_key_from_persistent(&row.key);
				secondary_index_coverage
					.entry(coverage_key)
					.or_insert_with(|| SecondaryIndexCoverageTracker::new(coverage_key))
					.mark_covered(row.key.source_slab_id);
				secondary_index_segments.insert(row.key, row);
			}
			persistent_key_localities.extend(snapshot.record_localities);
			recovered_lsm_runs = snapshot.lsm_runs;
			lsm_manifest_epoch_floor = snapshot.lsm_manifest_epoch_floor;
			if let Some(index) = &persistent_index {
				// Runs from before retirements disposed of theirs. A published retirement
				// authorizes this, never a missing file, and a run an unresolved install
				// names stays for that install to resolve.
				let named = snapshot
					.lsm_install_intents
					.iter()
					.flat_map(|intent| {
						let commit = &intent.commit;
						commit
							.slabs_to_retire
							.iter()
							.copied()
							.chain(commit.slabs_to_add.iter().map(|run| run.slab_id))
					})
					.collect::<BTreeSet<_>>();
				let remnants = recovered_lsm_runs
					.iter()
					.filter(|run| {
						run.state.is_live()
							&& snapshot.retired_tiered_slabs.contains(&run.slab_id)
							&& !named.contains(&run.slab_id)
					})
					.map(|run| run.slab_id)
					.collect::<Vec<_>>();
				let disposed = index.dispose_retirement_remnants(remnants).await?;
				if !disposed.is_empty() {
					tracing::warn!(
						runs = disposed.len(),
						"removed Live runs of slabs whose tier retirement left them behind"
					);
				}
				for run in &recovered_lsm_runs {
					if disposed.contains(&run.slab_id) {
						lsm_manifest_epoch_floor = lsm_manifest_epoch_floor.max(run.manifest_epoch);
					}
				}
				recovered_lsm_runs.retain(|run| !disposed.contains(&run.slab_id));
			}
		}
		let counter_eligible =
			Self::recover_counter_eligible(&active, &sealed, &counter_unsupported, &key_index);
		// An unpaired recovered tentative may still be named by an external
		// witness. Keep its slab ineligible until reconciliation resolves it.
		let counter_pending_frames = DashSet::new();
		let counter_pending_guards = CounterPendingGuards::default();
		let mut recovered_counter_slabs = vec![active.clone()];
		recovered_counter_slabs.extend(sealed.iter().map(|entry| entry.value().clone()));
		for slab in recovered_counter_slabs {
			Self::note_recovered_counter_tentatives(
				&slab,
				&counter_pending_frames,
				&counter_pending_guards,
			)?;
		}
		if !index_authority_unavailable && !lsm_install_unresolved {
			remove_non_live_lsm_rows_from_index(&key_index, &recovered_lsm_runs);
		}
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
			.unwrap_or(ManifestEpoch::ZERO)
			.max(lsm_manifest_epoch_floor);
		let mut compaction_manifest = if recovered_lsm_runs.is_empty() {
			CompactionManifest::new(recovered_manifest_epoch)
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
		// A run nothing backs fails every plan that names it. With no retirement record to
		// authorize removing it, it stays, and this says so.
		let backless = compaction_manifest
			.snapshot()
			.runs()
			.iter()
			.filter(|run| {
				run.state.is_live()
					&& !sealed.contains_key(&run.slab_id)
					&& active.slab_id() != run.slab_id
					&& !tier_pointer_ids.contains(&run.slab_id)
			})
			.map(|run| run.slab_id)
			.collect::<Vec<_>>();
		if !backless.is_empty() {
			tracing::warn!(
				runs = backless.len(),
				first = ?backless.first(),
				"Live runs with no backing and no retirement evidence block LSM compaction"
			);
		}

		// ADR 0097 decision 4: admission truth is hydrated ONCE, before the manager
		// exists, from the same index handle serving uses. `None` is not empty
		// history - it is the absence of a record, and no record means no maintenance.
		let maintenance_policy = match &persistent_index {
			Some(index) => {
				MaintenancePolicyInner::load(
					Arc::clone(index),
					Arc::new(SystemMaintenanceClock),
					maintenance_metrics,
				)
				.await
			}
			None => MaintenancePolicyInner::disabled(
				"no persistent index: maintenance receipts have nowhere to live",
				maintenance_metrics,
			),
		};
		let snapshot_epoch_initial = match &persistent_index {
			Some(index) => index
				.snapshot_retirement_epoch_floor()
				.await?
				.checked_add(1)
				.ok_or_else(|| Error::other("snapshot epoch exhausted"))?,
			None => 1,
		};
		let tier_budget =
			super::tier_budget::TierMemoryBudget::new(config.tier_history_budget_bytes);
		#[cfg(feature = "test-support")]
		crate::metrics::global().set_tier_measurement_source(std::sync::Arc::downgrade(
			&(tier_budget.clone() as std::sync::Arc<dyn crate::metrics::TierMeasurementSource>),
		));
		let tier_tokens = super::tier_tokens::TierTokenRegistry::new(&tier_budget)?;
		let manager = Arc::new(Self {
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
				durable_slab_id_bound,
				reservation_in_flight_bound: SlabId::new(0),
				tier_layouts: DashMap::new(),
				disposal_evidence: Default::default(),
			}),
			compaction_manifest: RwLock::new(compaction_manifest),
			slab_pins: Arc::new(SlabPinTable::default()),
			counter_eligible,
			counter_unsupported,
			counter_pending_frames,
			counter_pending_guards,
			slab_publication_revisions: DashMap::new(),
			counter_deferred_l0_slabs: DashSet::new(),
			key_index,
			locality_index: RwLock::new(locality_index),
			key_localities,
			table_by_location,
			table_references_by_slab,
			secondary_index_table_references_by_slab,
			integrity_failures: DashMap::new(),
			quarantined_compaction_slabs: DashSet::new(),
			redirects: Default::default(),
			persistent_index,
			pending_metadata_tokens,
			completed_metadata_retries: DashMap::new(),
			persistent_index_gate: Arc::new(tokio::sync::RwLock::new(())),
			index_authority_unavailable: AtomicBool::new(index_authority_unavailable),
			control_node: std::sync::OnceLock::new(),
			owner_authority_fence: std::sync::OnceLock::new(),
			lsm_install_unresolved: lsm_install_intents,
			lsm_intent_scope_gate: Arc::new(tokio::sync::RwLock::new(())),
			pending_index_publications: Arc::new(DashMap::new()),
			single_append_publication_evidence: Arc::new(DashMap::new()),
			index_publication_revision: std::sync::atomic::AtomicU64::new(1),
			orphaned_index_publications: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
			maintenance_policy,
			retired_tiered_cleanup: DashMap::new(),
			secondary_index_segments,
			secondary_index_coverage,
			secondary_index_admission_gate: Arc::new(AsyncRwLock::new(())),
			lsm_manifest_transition_gate: AsyncRwLock::new(()),
			maintenance_publication_gate: Arc::new(AsyncRwLock::new(())),
			maintenance_publication_restart_required: AtomicBool::new(false),
			maintenance_publication_fail_stop: std::sync::Mutex::new(None),
			secondary_index_declaration_cuts: DashSet::new(),
			retained_secondary_sidecars,
			secondary_index_persist_stripes: (0..crate::storage::manager::secondary_index_segments::SECONDARY_INDEX_PERSIST_STRIPES)
				.map(|_| tokio::sync::RwLock::new(()))
				.collect(),
			slab_rewrite_claims: DashMap::new(),
			tiered_stats_cache: DashMap::new(),
			tier_publication_locks: DashMap::new(),
			cleanup_claims: DashMap::new(),
			conflicted_locations: DashSet::new(),
			versions_contained: AtomicBool::new(contained),
			certified_lsm_fenced: AtomicBool::new(!orphan_lsm_certificates.is_empty()),
			unverified_tier_slabs: DashSet::new(),
			tier_authority: tier_authority::TierAuthority::new(),
			installed_snapshot_index: std::sync::RwLock::new(None),
			snapshot_epoch: Arc::new(std::sync::atomic::AtomicU64::new(snapshot_epoch_initial)),
			snapshot_epoch_gate: Arc::new(std::sync::RwLock::new(())),
			awaiting_cluster: AtomicBool::new(awaiting_cluster),
			retired_tier_slabs: DashSet::new(),
			tier_budget: tier_budget.clone(),
			tier_tokens,
			unapplied_tier_pointers: DashMap::new(),
			canonical_tier_source: std::sync::OnceLock::new(),
			#[cfg(test)]
			replay_probe: Default::default(),
			#[cfg(test)]
			counter_rollback_fail_once: std::sync::atomic::AtomicBool::new(false),
			#[cfg(test)]
			lsm_control_install_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			lsm_after_rename_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			lsm_fail_before_rename: std::sync::atomic::AtomicBool::new(false),
			#[cfg(test)]
			lsm_after_output_publish_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			lsm_after_install_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			lsm_after_intent_clear_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			lsm_capture_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			counter_rollback_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			continuity_test_holds: Default::default(),
			unresolved_tier_cleanup: Default::default(),
			deferred_table_drop_l2: Default::default(),
			cleanup_scan_schedule: Default::default(),
			quarantined_slab_files,
			released_slab_files: DashSet::new(),
			live_reservations: Arc::new(DashSet::new()),
			#[cfg(test)]
			test_pauses: Default::default(),
		});
		{
			// Every tiered slab is verified against its object before its first
			// serve in this process.
			let state = manager.state.lock().await;
			for entry in state.tiered.iter() {
				manager.unverified_tier_slabs.insert(*entry.key());
				manager.tier_authority.pending.insert(
					*entry.key(),
					tier_authority::PendingTierAuthority {
						pointer: entry.value().clone(),
						last_control_log_index: recovered_control_index,
					},
				);
			}
		}
		manager
			.detect_missing_published_references_on_recovery()
			.await;
		manager.load_unresolved_tier_cleanup().await?;
		if let Some(index) = &manager.persistent_index {
			for deferred in index.load_deferred_retirements().await? {
				manager
					.tier_authority
					.note_deferred_retirement(deferred.pointer.slab_id, deferred.log_index);
			}
			for mark in index.load_snapshot_retirement_marks().await? {
				manager.tier_authority.mark_snapshot_retirement(
					mark.pointer.slab_id,
					mark.epoch,
					mark.snapshot_index,
				);
			}
		}
		if let Some(durable_versions) = durable_versions {
			refuse_unaccounted_durable_versions(&manager, durable_versions)?;
		}
		manager.tier_authority.initialize(&manager)?;
		if manager.tier_authority.pending.is_empty() {
			manager.recover_version_conflicts().await?;
			manager.tier_authority.local_ready();
		} else {
			manager.tier_authority.start();
		}
		let snapshot = manager.compaction_manifest_snapshot();
		refresh_lsm_level_metrics(&snapshot);
		if !awaiting_cluster
			&& !index_authority_unavailable
			&& manager.integrity_failures.is_empty()
			&& manager.tier_authority.pending.is_empty()
			&& !lsm_install_unresolved
		{
			manager.rebuild_persistent_index().await?;
		} else {
			tracing::error!(
				fenced_table_count = manager.integrity_failures.len(),
				"published reference integrity failure detected during recovery; preserving durable index evidence"
			);
		}
		// Before anyone holds this manager: what the checks above skipped for an intent is
		// still owed when that intent resolves.
		if lsm_install_unresolved && !awaiting_cluster && !index_authority_unavailable {
			manager
				.tier_authority
				.deferred_open
				.store(true, Ordering::Release);
		}
		manager.fence_unresolved_ops_at_open().await?;
		Ok(manager)
	}
}
