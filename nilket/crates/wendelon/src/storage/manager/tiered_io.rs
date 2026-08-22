use super::*;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct TieredRowRefreshPlan {
	pub(super) old_rows: Vec<PersistentRecordRow>,
	pub(super) new_rows: Vec<PersistentRecordRow>,
	pub(super) relocated_reads: Vec<(RecordLocation, RecordLocation)>,
}

#[derive(Clone)]
pub(super) struct RehydratedSlabState {
	pub(super) slab: Arc<LocalSlabStore>,
	pub(super) keep_local_until: Instant,
}

/// Authoritative location for a slab, as observed by the read
/// path. Keeping it an enum means the "is the file still here?"
/// question is answered with the same lock acquisition that
/// resolves the backing.
pub(super) enum SlabBacking {
	Local(Arc<LocalSlabStore>),
	L2(TieredSlabPointer),
	Missing,
}

pub(super) fn validate_tiered_blob(
	pointer: &TieredSlabPointer,
	blob: &[u8],
) -> std::io::Result<()> {
	let actual = crate::hash::content::digest(blob);
	if actual != pointer.payload_hash {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"L2 payload hash mismatch for slab {}: expected {:02x?}, got {:02x?}",
				pointer.slab_id,
				&pointer.payload_hash[..4],
				&actual[..4],
			),
		));
	}
	Ok(())
}

pub(super) fn decode_tiered_blob(
	crypto: &CryptoManager,
	pointer: &TieredSlabPointer,
	blob: &[u8],
) -> std::io::Result<Vec<u8>> {
	match crypto.encrypted_footer_summary(blob)? {
		Some(FooterSummary {
			slab_id, tenant_id, ..
		}) => {
			if slab_id != pointer.slab_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"encrypted tiered slab footer references slab {} but metadata expects {}",
						slab_id, pointer.slab_id
					),
				));
			}
			let DecryptedTieredSlab { plaintext, .. } =
				crypto.decrypt_tiered_slab(pointer.slab_id, blob)?;
			let payload_tenant_id = single_tenant_id_from_plain_slab(&plaintext, pointer.slab_id)?;
			if payload_tenant_id != tenant_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"encrypted tiered slab footer tenant {} does not match payload tenant {} for slab {}",
						tenant_id.get(),
						payload_tenant_id.get(),
						pointer.slab_id,
					),
				));
			}
			Ok(plaintext)
		}
		None => Ok(blob.to_vec()),
	}
}

pub(super) fn validate_tiered_record_header_tenant(
	blob: &[u8],
	expected_tenant_id: TenantId,
	slab_id: SlabId,
) -> std::io::Result<()> {
	let header_size = std::mem::size_of::<SlabRecordHeader>();
	if blob.len() < header_size {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"L2 record range shorter than record header",
		));
	}
	let header = bytemuck::pod_read_unaligned::<SlabRecordHeader>(&blob[..header_size]);
	if header.tenant_id != expected_tenant_id {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"encrypted tiered slab footer tenant {} does not match record header tenant {} for slab {}",
				expected_tenant_id.get(),
				header.tenant_id.get(),
				slab_id,
			),
		));
	}
	Ok(())
}

pub(super) fn extract_record_from_blob(
	blob: &[u8],
	location: RecordLocation,
) -> std::io::Result<Vec<u8>> {
	let offset = location.offset as usize;
	let length = location.length as usize;
	let header_size = std::mem::size_of::<crate::schema::SlabRecordHeader>();
	if blob.len() < offset + header_size {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"L2 blob shorter than record header",
		));
	}
	let header = bytemuck::pod_read_unaligned::<crate::schema::SlabRecordHeader>(
		&blob[offset..offset + header_size],
	);
	let frame_len = header.frame_len as usize;
	let end = offset
		.checked_add(header_size)
		.and_then(|h| h.checked_add(frame_len))
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame_len overflows usize"))?;
	if end > blob.len() || header_size + frame_len > length {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"L2 record extends past downloaded blob",
		));
	}
	Ok(blob[offset + header_size..end].to_vec())
}

pub(super) async fn fetch_tiered_ciphertext_runs(
	l2: Arc<dyn ObjectStore>,
	pointer: &TieredSlabPointer,
	plan: &TieredMultiRangeReadPlan,
	max_parallel_range_gets: usize,
) -> std::io::Result<Vec<Bytes>> {
	fetch_l2_ranges_bounded(
		l2,
		pointer.l2_uri.clone(),
		plan.ciphertext_ranges_in_object(),
		max_parallel_range_gets,
	)
	.await
}

pub(super) async fn fetch_l2_ranges_bounded(
	l2: Arc<dyn ObjectStore>,
	uri: String,
	ranges: Vec<std::ops::Range<u64>>,
	max_parallel_range_gets: usize,
) -> std::io::Result<Vec<Bytes>> {
	if ranges.is_empty() {
		return Ok(Vec::new());
	}
	let max_parallel = max_parallel_range_gets.max(1).min(ranges.len());
	if max_parallel == 1 || ranges.len() == 1 {
		let mut out = Vec::with_capacity(ranges.len());
		for range in ranges {
			out.push(l2.get_range(&uri, range).await?);
		}
		return Ok(out);
	}

	let mut out = vec![None; ranges.len()];
	let mut next = 0usize;
	let mut join = JoinSet::new();
	while next < ranges.len() || !join.is_empty() {
		while next < ranges.len() && join.len() < max_parallel {
			let l2 = l2.clone();
			let uri = uri.clone();
			let range = ranges[next].clone();
			let index = next;
			join.spawn(async move {
				let bytes = l2.get_range(&uri, range).await?;
				Ok::<_, std::io::Error>((index, bytes))
			});
			next += 1;
		}
		let Some(joined) = join.join_next().await else {
			continue;
		};
		match joined {
			Ok(Ok((index, bytes))) => out[index] = Some(bytes),
			Ok(Err(err)) => return Err(err),
			Err(err) => {
				return Err(Error::other(format!(
					"L2 range fetch task failed before returning ciphertext: {err}"
				)));
			}
		}
	}
	out.into_iter()
		.enumerate()
		.map(|(index, bytes)| {
			bytes.ok_or_else(|| Error::other(format!("L2 range fetch {index} did not complete")))
		})
		.collect()
}

pub(super) fn descriptor_matches_tiered_row(
	descriptor: &TableSchemaDescriptor,
	tenant_id: TenantId,
	key_identity: Identity,
	routing_locality: Locality,
	stored_table_object_id: Option<TableObjectId>,
	batch: &RecordBatch,
) -> std::io::Result<bool> {
	if batch.num_rows() != 1 || batch.schema().as_ref() != &descriptor.arrow_schema()? {
		return Ok(false);
	}
	match &descriptor.row_identity_policy {
		// PK-keyed: re-derive identity / locality from the user
		// row and compare to the stored synthetic columns. Phase 8
		// also stamps `__row_table_object_id__` on multi-row PK
		// rows; when present, it gives us an early fail before
		// the (more expensive) PK re-derivation.
		RowIdentityPolicy::PrimaryKey(_) => {
			if let Some(stored) = stored_table_object_id
				&& stored != descriptor.table_object_id
			{
				return Ok(false);
			}
			let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			table_meta.tenant_id = tenant_id;
			table_meta.table_object_id = descriptor.table_object_id;
			table_meta.schema_generation = descriptor.schema_generation;
			table_meta.row_count = 1;
			let zero_hlc = HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			};
			let admission = match validate_table_write_request_batch(
				&table_meta,
				batch,
				descriptor,
				WriterNodeId::ZERO,
				zero_hlc,
			) {
				Ok(admission) => admission,
				Err(_) => return Ok(false),
			};
			Ok(admission.key_identities.as_slice() == [key_identity]
				&& admission.localities.as_slice() == [routing_locality])
		}
		// AppendOnly: identity is engine-synthesized at write
		// time from `(writer_node_id, commit_hlc,
		// intra_batch_index)` (not recoverable from the user
		// batch alone) and locality is request-level (depends
		// on `parent_write_id`). Phase 8 binds rows back to
		// their descriptor via `__row_table_object_id__` —
		// verify equality with `descriptor.table_object_id` so
		// table-spread prefix collisions across same-tenant
		// AppendOnly tables cannot leak rows.
		RowIdentityPolicy::AppendOnly => {
			let _ = (tenant_id, key_identity, routing_locality);
			Ok(stored_table_object_id == Some(descriptor.table_object_id))
		}
		RowIdentityPolicy::AppendOnlyContentHash => {
			if stored_table_object_id != Some(descriptor.table_object_id) {
				return Ok(false);
			}
			let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
			table_meta.tenant_id = tenant_id;
			table_meta.table_object_id = descriptor.table_object_id;
			table_meta.schema_generation = descriptor.schema_generation;
			table_meta.row_count = 1;
			let zero_hlc = HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			};
			let admission = match validate_table_write_request_batch(
				&table_meta,
				batch,
				descriptor,
				WriterNodeId::ZERO,
				zero_hlc,
			) {
				Ok(admission) => admission,
				Err(_) => return Ok(false),
			};
			Ok(admission.key_identities.as_slice() == [key_identity]
				&& admission.localities.as_slice() == [routing_locality])
		}
	}
}

pub(super) fn build_tiered_object_metadata(
	blob: &[u8],
	slab_id: SlabId,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<TieredSlabObjectMetadata> {
	let entries = recover_slab_entries_from_blob(blob, slab_id)?;
	let Some(first_entry) = entries.first().copied() else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!(
				"sealed slab {} does not contain any committed records; tiered encryption requires non-empty single-tenant slabs",
				slab_id
			),
		));
	};
	let tenant_id = first_entry.tenant_id;
	let has_row_tombstone_records = entries.iter().any(|entry| entry.location.is_tombstone);
	let mut offset_index = Vec::with_capacity(entries.len());
	for entry in entries {
		let other_tenant_id = entry.tenant_id;
		let key_identity = entry.key_identity;
		let location = entry.location;
		if other_tenant_id != tenant_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"sealed slab {} contains multiple tenants ({} and {}); tiered encryption requires single-tenant slabs until W5 lands tenant-scoped active slabs",
					slab_id,
					tenant_id.get(),
					other_tenant_id.get(),
				),
			));
		}
		offset_index.push(TieredOffsetIndexEntry {
			key_locator: IdentityPrefix::from(key_identity),
			byte_offset: location.offset,
			byte_length: location.length,
			flags: 0,
		});
	}
	Ok(TieredSlabObjectMetadata {
		tenant_id,
		created_hlc: first_entry.location.hlc.physical_ms,
		chunk_size_bytes: choose_tiered_object_chunk_size(blob, slab_id, chunk_layout_config)?,
		offset_index,
		column_stats_block: None,
		has_row_tombstone_records,
	})
}

pub(super) fn choose_tiered_object_chunk_size(
	blob: &[u8],
	slab_id: SlabId,
	chunk_layout_config: &L2ChunkLayoutConfig,
) -> std::io::Result<crate::schema::ChunkSizeBytes> {
	if !chunk_layout_config.enabled {
		return Ok(SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE);
	}
	let directories = encoding_directories_from_slab_blob(blob, slab_id)?;
	Ok(choose_chunk_size_bytes_for_directories(
		directories.iter(),
		chunk_layout_config,
	))
}

impl SlabManager {
	/// Encode a sealed slab into the ADR 022 L2 object layout.
	/// The current write path still assumes every sealed slab is
	/// single-tenant, so the footer and per-chunk AAD stay slab-scoped.
	pub fn encrypt_tiered_object_blob(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
	) -> std::io::Result<EncryptedTieredSlab> {
		let metadata =
			build_tiered_object_metadata(plaintext_blob, slab_id, &self.config.l2_chunk_layout)?;
		self.config
			.crypto
			.encrypt_tiered_slab_object(slab_id, &metadata, plaintext_blob)
	}

	pub fn encrypt_tiered_object_blob_with_table_stats(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<EncryptedTieredSlab> {
		let mut metadata =
			build_tiered_object_metadata(plaintext_blob, slab_id, &self.config.l2_chunk_layout)?;
		metadata.column_stats_block = self.build_tiered_column_stats_block(
			plaintext_blob,
			slab_id,
			metadata.tenant_id,
			table_schemas,
		)?;
		self.config
			.crypto
			.encrypt_tiered_slab_object(slab_id, &metadata, plaintext_blob)
	}

	/// Detect whether a tiered object already carries an encrypted
	/// footer. Callers use this to distinguish legacy plaintext L2
	/// objects from ciphertext ones during migration.
	pub fn encrypted_tiered_footer_summary(
		&self,
		blob: &[u8],
	) -> std::io::Result<Option<FooterSummary>> {
		self.config.crypto.encrypted_footer_summary(blob)
	}

	/// Move a slab's backing from the sealed L1 set to the tiered
	/// set. On return:
	///
	/// * The slab is no longer in `sealed` — future
	///   [`StorageEngine::read`]s route to L2 via the stored URI.
	/// * The on-disk `slab_<id>.wnd` file has been unlinked.
	/// * Any read that grabbed an `Arc<LocalSlabStore>` *before*
	///   this call still holds its own reference, so its open fd
	///   survives the unlink (Linux semantics) — zero downtime
	///   for readers mid-flight.
	pub async fn apply_tier_slab(&self, pointer: TieredSlabPointer) -> std::io::Result<()> {
		self.apply_tier_slab_committed(pointer, None).await
	}

	pub async fn apply_tier_slab_committed(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		// This path is intentionally idempotent for the same pointer. The
		// sweeper may fall back to calling it after Raft apply has already
		// fired the tier observer, so a second application of an identical
		// pointer must not rebuild row state or mutate relocated reads again.
		let old_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&pointer.slab_id)
				.map(|entry| entry.value().clone())
		};
		let refresh_plan = if old_pointer.as_ref().is_some_and(|old| old != &pointer)
			|| !self.has_rows_for_slab(pointer.slab_id)
		{
			Some(self.build_tiered_row_refresh_plan_from_l2(&pointer).await?)
		} else {
			None
		};
		#[cfg(debug_assertions)]
		if old_pointer.as_ref().is_some_and(|old| old == &pointer) {
			debug_assert!(
				refresh_plan.is_none(),
				"identical tier pointer replay must remain a no-op; the sweeper may reapply after the observer already committed local state"
			);
		}
		self.persist_tiered_pointer_strict(pointer.clone(), last_control_log_index)
			.await?;
		if let Some(plan) = &refresh_plan {
			for (old_location, new_location) in &plan.relocated_reads {
				self.relocated_reads.insert(*old_location, *new_location);
			}
		}
		if old_pointer
			.as_ref()
			.map(|old| old != &pointer)
			.unwrap_or(true)
		{
			self.clear_tiered_stats_cache_for_slab(pointer.slab_id);
		}
		{
			let mut state = self.state.lock().await;
			let _old = state.sealed.remove(&pointer.slab_id);
			let _old = state.rehydrated.remove(&pointer.slab_id);
			state.tiered.insert(pointer.slab_id, pointer.clone());
			state.hot_read_counts.remove(&pointer.slab_id);
			state.rehydrating.remove(&pointer.slab_id);
		}
		self.retired_tiered_cleanup.remove(&pointer.slab_id);
		if let Some(plan) = refresh_plan {
			self.apply_tiered_row_refresh_plan(plan).await;
		}

		// Unlink the L1 file. Open fds held by in-flight reads
		// continue to work; new reads find the entry in `tiered`
		// and go to L2.
		let path = slab_path(&self.config.dir, pointer.slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(())
	}

	pub async fn retire_tiered_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		self.retire_tiered_slab_committed(slab_id, None).await
	}

	pub async fn retire_tiered_slab_committed(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<bool> {
		let rows = self.record_rows_for_slab(slab_id);
		let retired_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&slab_id)
				.map(|entry| entry.value().clone())
		};
		let should_remove = {
			let state = self.state.lock().await;
			retired_pointer.is_some() || state.rehydrated.contains_key(&slab_id) || !rows.is_empty()
		};
		if !should_remove {
			return Ok(false);
		}
		if let Some(pointer) = retired_pointer.clone() {
			self.retired_tiered_cleanup.insert(slab_id, pointer);
		}
		self.delete_tiered_slab_strict(slab_id, last_control_log_index)
			.await?;
		{
			let mut state = self.state.lock().await;
			let _old = state.sealed.remove(&slab_id);
			let _old = state.rehydrated.remove(&slab_id);
			let _old = state.tiered.remove(&slab_id);
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
		}
		for row in &rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		self.relocated_reads.retain(|old_location, new_location| {
			old_location.slab_id != slab_id && new_location.slab_id != slab_id
		});
		self.clear_tiered_stats_cache_for_slab(slab_id);
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(true)
	}

	/// Converge one recovered tiered slab into local state.
	///
	/// When the redb mirror is unavailable or missing rows, the
	/// manager rebuilds the per-record index directly from the
	/// authoritative L2 blob before installing the tier pointer.
	pub async fn reconcile_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		self.apply_tier_slab_committed(pointer, last_control_log_index)
			.await
	}

	/// Whether a slab has been pivoted to L2. Cheap — only hits
	/// the tiered DashMap.
	pub async fn is_tiered(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.tiered.contains_key(&slab_id)
	}

	/// Whether a tiered slab currently also has a rehydrated local
	/// copy in L1.
	pub async fn is_rehydrated(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.rehydrated.contains_key(&slab_id)
	}

	/// Lookup the L2 pointer for a tiered slab.
	pub async fn tier_pointer(&self, slab_id: SlabId) -> Option<TieredSlabPointer> {
		self.state
			.lock()
			.await
			.tiered
			.get(&slab_id)
			.map(|p| p.clone())
	}

	/// Enumerate the current tiered-slab pointers held in local
	/// runtime state. The returned list is sorted by slab id so
	/// background sweeps behave deterministically.
	pub async fn list_tiered_slabs(&self) -> Vec<TieredSlabPointer> {
		let mut pointers = self
			.state
			.lock()
			.await
			.tiered
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// Restore-time validation pass for replicated tier pointers.
	///
	/// Confirms that every committed tier reference is resolvable
	/// through the configured L2 store before the node advertises
	/// ready. This is intentionally lighter than a full object hash
	/// re-read: H2 only needs to prove the referenced objects exist
	/// and are reachable from the restore environment.
	pub async fn validate_tiered_references(&self) -> std::io::Result<()> {
		let pointers = self.list_tiered_slabs().await;
		if pointers.is_empty() {
			return Ok(());
		}
		let l2 = self.config.l2.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"restore validation requires an L2 object store when tiered slab references are present",
			)
		})?;
		for pointer in &pointers {
			let metadata = l2.head(&pointer.l2_uri).await.map_err(|err| {
				Error::other(format!(
					"restore validation failed for tiered slab {} at {}: {err}",
					pointer.slab_id, pointer.l2_uri
				))
			})?;
			if metadata.size_bytes == 0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"restore validation found zero-length tiered slab {} at {}",
						pointer.slab_id, pointer.l2_uri
					),
				));
			}
		}
		Ok(())
	}

	/// Tier retirements that committed in the SM but still need
	/// best-effort local cleanup retried.
	pub async fn list_retired_tiered_cleanup_candidates(&self) -> Vec<TieredSlabPointer> {
		let mut pointers = self
			.retired_tiered_cleanup
			.iter()
			.map(|entry| entry.value().clone())
			.collect::<Vec<_>>();
		pointers.sort_by_key(|pointer| pointer.slab_id);
		pointers
	}

	/// Retry only the local-file cleanup for a retired tiered slab.
	pub async fn retry_retired_tiered_local_cleanup(&self, slab_id: SlabId) -> std::io::Result<()> {
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await
	}

	/// Mark one retired tiered slab cleanup candidate complete.
	pub fn clear_retired_tiered_cleanup(&self, slab_id: SlabId) {
		self.retired_tiered_cleanup.remove(&slab_id);
	}

	/// Best-effort reclaim of one rehydrated local cache copy. The
	/// authoritative L2 pointer remains intact; only the temporary
	/// L1 file is removed.
	pub async fn drop_rehydrated_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		let removed = {
			let state = self.state.lock().await;
			if state.rehydrating.contains(&slab_id) {
				return Ok(false);
			}
			state.rehydrated.remove(&slab_id).map(|(_, state)| state)
		};
		let Some(_state) = removed else {
			return Ok(false);
		};
		let path = slab_path(&self.config.dir, slab_id);
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		Ok(true)
	}

	pub(super) async fn tiered_plaintext_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let l2 = self.config.l2.clone().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"cannot recover tiered slab {} without an L2 object store",
					pointer.slab_id
				),
			)
		})?;
		let blob = l2.get(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, &blob)?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, &blob)
	}

	pub(super) async fn load_tiered_rows_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		let plaintext = self.tiered_plaintext_from_l2(pointer).await?;
		recover_slab_entries_from_blob(&plaintext, pointer.slab_id)?
			.into_iter()
			.map(|entry| {
				let frame = extract_record_from_blob(&plaintext, entry.location)?;
				let table_object_id = decode_record_batch(&frame)
					.ok()
					.map(|batch| {
						storage_row_table_object_id_at(&batch, entry.location.record_row_index)
					})
					.transpose()?
					.flatten();
				Ok(PersistentRecordRow {
					tenant_id: entry.tenant_id,
					key_identity: entry.key_identity,
					routing_locality: self.recovered_entry_routing_locality(&entry),
					table_object_id,
					// ADR 0090: recovery read the row's placement from the frame;
					// re-tiering must not downgrade it.
					placement: entry.location.placement,
					partition: entry.location.partition,
					location: entry.location,
				})
			})
			.collect()
	}

	pub(super) async fn build_tiered_row_refresh_plan_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<TieredRowRefreshPlan> {
		let new_rows = self.load_tiered_rows_from_l2(pointer).await?;
		let old_rows = self.record_rows_for_slab(pointer.slab_id);
		let mut old_by_slot = HashMap::with_capacity(old_rows.len());
		for row in &old_rows {
			old_by_slot
				.entry((row.tenant_id, row.key_identity, row.location.hlc))
				.or_insert_with(Vec::new)
				.push(row.location);
		}
		let mut new_by_slot = HashMap::with_capacity(new_rows.len());
		for row in &new_rows {
			new_by_slot
				.entry((row.tenant_id, row.key_identity, row.location.hlc))
				.or_insert_with(Vec::new)
				.push(row.location);
		}
		let mut relocated_reads = Vec::new();
		for (slot, old_locations) in old_by_slot {
			let Some(new_locations) = new_by_slot.remove(&slot) else {
				continue;
			};
			// Retention rewrites preserve surviving rows in append/offset order but
			// may prune a prefix of older duplicates that share the same HLC. Align
			// from the tail so the newest duplicate survives the relocation map.
			let mut slot_relocations = old_locations
				.into_iter()
				.rev()
				.zip(new_locations.into_iter().rev())
				.filter_map(|(old_location, new_location)| {
					(old_location != new_location).then_some((old_location, new_location))
				})
				.collect::<Vec<_>>();
			slot_relocations.reverse();
			relocated_reads.extend(slot_relocations);
		}
		relocated_reads
			.sort_by_key(|(old_location, _)| (old_location.slab_id, old_location.offset));
		Ok(TieredRowRefreshPlan {
			old_rows,
			new_rows,
			relocated_reads,
		})
	}

	pub(super) async fn apply_tiered_row_refresh_plan(&self, plan: TieredRowRefreshPlan) {
		if plan.old_rows == plan.new_rows {
			return;
		}
		for row in &plan.old_rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		for row in &plan.new_rows {
			self.key_index
				.entry((row.tenant_id, row.key_identity))
				.and_modify(|versions| {
					if !versions.contains(&row.location) {
						insert_version_sorted(versions, row.location);
					}
				})
				.or_insert_with(|| vec![row.location]);
			self.insert_locality_membership_with_locality(
				row.tenant_id,
				row.key_identity,
				row.routing_locality,
			);
			if let Some(table_object_id) = row.table_object_id {
				self.add_published_table_reference(row.tenant_id, table_object_id, row.location);
			}
		}
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: plan.old_rows,
				put_records: plan.new_rows,
				..Default::default()
			},
			"tiered-row-refresh",
		)
		.await;
	}

	/// Fetch a single record from L2, then decide whether that L2
	/// read was hot enough to justify rehydrating the whole slab
	/// back into local L1 storage.
	pub(super) async fn read_from_l2(
		&self,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		let l2 = self.config.l2.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"slab {} is tiered but no ObjectStore configured on this manager",
					pointer.slab_id
				),
			)
		})?;
		let mut plaintext_for_rehydration = None;
		let frame = if pointer.is_encrypted {
			self.read_encrypted_record_from_l2(l2.as_ref(), pointer, location)
				.await?
		} else {
			let plaintext = self
				.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
				.await?;
			let frame = extract_record_from_blob(&plaintext, location)?;
			plaintext_for_rehydration = Some(plaintext);
			frame
		};
		if self.note_l2_read(pointer.slab_id).await {
			let rehydrate_result = async {
				let plaintext = match plaintext_for_rehydration.take() {
					Some(plaintext) => plaintext,
					None => {
						self.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
							.await?
					}
				};
				self.rehydrate_tiered_slab(pointer.clone(), bytes::Bytes::from(plaintext))
					.await
			}
			.await;
			if let Err(err) = rehydrate_result {
				tracing::warn!(
					slab_id = pointer.slab_id.get(),
					error = %err,
					"rehydration failed; continuing to serve slab from L2"
				);
			}
		}
		if let Ok(policy) = record_batch_compression_policy_from_ipc(&frame) {
			metrics().observe_storage_compression_l2_record_body_fetch(policy, frame.len() as u64);
		}
		Ok(frame)
	}

	pub(super) async fn fetch_full_tiered_plaintext(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let blob = l2.get_bytes(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, blob.as_ref())?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, blob.as_ref())
	}

	pub(super) async fn read_encrypted_record_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let plaintext_start = usize::try_from(location.offset).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!("record offset {} does not fit into usize", location.offset),
			)
		})?;
		let plaintext_end_u64 = location
			.offset
			.checked_add(location.length as u64)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"record range overflows u64 for slab {} at offset {}",
						pointer.slab_id, location.offset,
					),
				)
			})?;
		let plaintext_end = usize::try_from(plaintext_end_u64).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!("record end {} does not fit into usize", plaintext_end_u64),
			)
		})?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};
		let plan = self.config.crypto.plan_tiered_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			plaintext_start..plaintext_end,
		)?;
		let footer = plan.footer();
		let ciphertext = l2
			.get_range(&pointer.l2_uri, plan.ciphertext_range_in_object())
			.await?;
		let record_bytes = plan.decrypt(ciphertext.as_ref())?;
		validate_tiered_record_header_tenant(&record_bytes, footer.tenant_id, pointer.slab_id)?;
		extract_record_from_blob(
			&record_bytes,
			RecordLocation {
				offset: 0,
				..location
			},
		)
	}

	pub(super) async fn note_l2_read(&self, slab_id: SlabId) -> bool {
		let promote_after = self.config.rehydration.promote_after_l2_reads;
		if promote_after == 0 {
			return false;
		}
		let mut state = self.state.lock().await;
		if state.rehydrated.contains_key(&slab_id) || state.rehydrating.contains(&slab_id) {
			return false;
		}
		let hits = state.hot_read_counts.entry(slab_id).or_insert(0);
		*hits = hits.saturating_add(1);
		if *hits < promote_after {
			return false;
		}
		state.rehydrating.insert(slab_id);
		true
	}

	pub(super) async fn rehydrate_tiered_slab(
		&self,
		pointer: TieredSlabPointer,
		blob: Bytes,
	) -> std::io::Result<()> {
		let result = self
			.install_rehydrated_local_copy(pointer.slab_id, blob.as_ref())
			.await;
		let mut state = self.state.lock().await;
		state.rehydrating.remove(&pointer.slab_id);
		match result {
			Ok(installed) => {
				state.hot_read_counts.remove(&pointer.slab_id);
				state.rehydrated.insert(pointer.slab_id, installed);
				Ok(())
			}
			Err(err) => {
				let retry_floor = self
					.config
					.rehydration
					.promote_after_l2_reads
					.saturating_sub(1);
				if retry_floor == 0 {
					state.hot_read_counts.remove(&pointer.slab_id);
				} else {
					state.hot_read_counts.insert(pointer.slab_id, retry_floor);
				}
				Err(err)
			}
		}
	}

	pub(super) async fn install_rehydrated_local_copy(
		&self,
		slab_id: SlabId,
		blob: &[u8],
	) -> std::io::Result<RehydratedSlabState> {
		let path = slab_path(&self.config.dir, slab_id);
		let tmp_path = rehydrate_tmp_path(&self.config.dir, slab_id);
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		self.file_ops.write_all(&tmp_path, blob).await?;
		let tmp = self
			.file_ops
			.open(
				&tmp_path,
				OpenFlags {
					read: true,
					write: true,
					create: false,
					create_new: false,
					truncate: false,
					direct: false,
				},
			)
			.await?;
		self.file_ops.fsync(&tmp).await?;
		self.file_ops.rename(&tmp_path, &path).await?;
		fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
		let slab = Arc::new(
			LocalSlabStore::open(
				&path,
				slab_id,
				self.file_ops.clone(),
				self.config.group_commit.clone(),
			)
			.await?,
		);
		Ok(RehydratedSlabState {
			slab,
			keep_local_until: Instant::now() + self.config.rehydration.min_local_residency,
		})
	}
}
