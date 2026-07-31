use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexSegmentInfo {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub source_slab_id: SlabId,
	pub segment_path: String,
	pub l2_uri: Option<String>,
	pub payload_hash: [u8; 32],
	pub state_digest: [u8; 32],
	pub format_version: u16,
	pub entry_count: u64,
}

impl SecondaryIndexSegmentInfo {
	fn key(&self) -> PersistentSecondaryIndexSegmentKey {
		PersistentSecondaryIndexSegmentKey {
			tenant_id: self.tenant_id,
			table_object_id: self.table_object_id,
			index_object_id: self.index_object_id,
			logical_schema_fingerprint: self.logical_schema_fingerprint,
			source_slab_id: self.source_slab_id,
		}
	}

	pub fn coverage_key(&self) -> SecondaryIndexCoverageKey {
		SecondaryIndexCoverageKey {
			tenant_id: self.tenant_id,
			table_object_id: self.table_object_id,
			index_object_id: self.index_object_id,
			logical_schema_fingerprint: self.logical_schema_fingerprint,
		}
	}
}

impl From<PersistentSecondaryIndexSegmentRow> for SecondaryIndexSegmentInfo {
	fn from(row: PersistentSecondaryIndexSegmentRow) -> Self {
		Self {
			tenant_id: row.key.tenant_id,
			table_object_id: row.key.table_object_id,
			index_object_id: row.key.index_object_id,
			logical_schema_fingerprint: row.key.logical_schema_fingerprint,
			source_slab_id: row.key.source_slab_id,
			segment_path: row.segment_path,
			l2_uri: row.l2_uri,
			payload_hash: row.payload_hash,
			state_digest: row.state_digest,
			format_version: row.format_version,
			entry_count: row.entry_count,
		}
	}
}

impl From<SecondaryIndexSegmentInfo> for PersistentSecondaryIndexSegmentRow {
	fn from(info: SecondaryIndexSegmentInfo) -> Self {
		Self {
			key: info.key(),
			segment_path: info.segment_path,
			l2_uri: info.l2_uri,
			payload_hash: info.payload_hash,
			state_digest: info.state_digest,
			format_version: info.format_version,
			entry_count: info.entry_count,
		}
	}
}

pub(super) fn secondary_index_coverage_key_from_persistent(
	key: &PersistentSecondaryIndexSegmentKey,
) -> SecondaryIndexCoverageKey {
	SecondaryIndexCoverageKey {
		tenant_id: key.tenant_id,
		table_object_id: key.table_object_id,
		index_object_id: key.index_object_id,
		logical_schema_fingerprint: key.logical_schema_fingerprint,
	}
}

pub(super) fn secondary_index_backfill_reason_from_io(
	err: &std::io::Error,
) -> SecondaryIndexBackfillErrorReason {
	match err.kind() {
		ErrorKind::NotFound => SecondaryIndexBackfillErrorReason::MissingSegment,
		ErrorKind::InvalidData | ErrorKind::UnexpectedEof => {
			SecondaryIndexBackfillErrorReason::CorruptSegment
		}
		_ => SecondaryIndexBackfillErrorReason::EncodeFailure,
	}
}

#[derive(Clone, Copy)]
pub(super) struct SecondaryIndexCompactionContext<'a> {
	pub(super) table_schemas: &'a TableSchemaCatalog,
	pub(super) secondary_indexes: &'a SecondaryIndexCatalog,
}

pub(super) struct LsmPreparedSecondaryIndexSegments {
	pub(super) old_sidecars: Vec<SecondaryIndexSegmentInfo>,
	pub(super) old_descriptors: Vec<(SlabId, SecondaryIndexCoverageKey)>,
	pub(super) output_segments: Vec<LsmPreparedSecondaryIndexSegment>,
}

pub(super) struct LsmPreparedSecondaryIndexSegment {
	pub(super) key: SecondaryIndexCoverageKey,
	pub(super) output_slab_id: SlabId,
	pub(super) index_object_id: IndexObjectId,
	pub(super) segment: SecondaryIndexSegment,
}

pub(super) fn secondary_index_sidecar_path(dir: &Path, relative: &str) -> std::io::Result<PathBuf> {
	let relative_path = Path::new(relative);
	if relative_path.is_absolute()
		|| relative_path.components().any(|component| {
			matches!(
				component,
				std::path::Component::ParentDir
					| std::path::Component::RootDir
					| std::path::Component::Prefix(_)
			)
		}) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("secondary index sidecar path is not relative-safe: {relative}"),
		));
	}
	Ok(dir.join(relative_path))
}

pub(super) fn secondary_index_sidecar_tmp_path(path: &Path) -> PathBuf {
	let mut tmp = path.as_os_str().to_os_string();
	tmp.push(".tmp");
	PathBuf::from(tmp)
}

impl SlabManager {
	pub async fn ensure_secondary_index_coverage(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		max_buffer_bytes: usize,
	) -> std::io::Result<SecondaryIndexBackfillPlan> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let (active_slab_id, historical_slabs) = self.secondary_index_source_slabs().await;
		let mut tracker = self
			.secondary_index_coverage
			.get(&key)
			.map(|entry| entry.value().clone())
			.unwrap_or_else(|| SecondaryIndexCoverageTracker::new(key));

		for row in self.secondary_index_segments.iter() {
			if secondary_index_coverage_key_from_persistent(&row.key) == key {
				tracker.mark_covered(row.key.source_slab_id);
			}
		}
		if tracker.active_buffer().is_none() {
			tracker.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				active_slab_id,
				max_buffer_bytes,
			))?;
		}

		let active_buffer_slab = tracker
			.active_buffer()
			.map(SecondaryIndexActiveBuffer::source_slab_id);
		let uncovered_slabs = tracker
			.uncovered_slabs()
			.map(|(slab_id, _error)| slab_id)
			.collect::<BTreeSet<_>>();
		let candidates = historical_slabs
			.into_iter()
			.filter(|slab_id| !tracker.is_covered(*slab_id))
			.filter(|slab_id| Some(*slab_id) != active_buffer_slab)
			.filter(|slab_id| !uncovered_slabs.contains(slab_id))
			.map(|source_slab_id| SecondaryIndexBackfillCandidate {
				key,
				source_slab_id,
			})
			.collect::<Vec<_>>();
		let snapshot = tracker.snapshot();
		self.secondary_index_coverage.insert(key, tracker);
		Ok(SecondaryIndexBackfillPlan {
			snapshot,
			candidates,
		})
	}

	pub fn secondary_index_coverage_snapshot(
		&self,
		key: SecondaryIndexCoverageKey,
	) -> Option<SecondaryIndexCoverageSnapshot> {
		self.secondary_index_coverage
			.get(&key)
			.map(|entry| entry.value().snapshot())
	}

	pub async fn secondary_index_tail_source_slabs(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
	) -> BTreeSet<SlabId> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let (active_slab_id, historical_slabs) = self.secondary_index_source_slabs().await;
		let tracker = self
			.secondary_index_coverage
			.get(&key)
			.map(|entry| entry.value().clone());
		let covered_slabs = tracker
			.as_ref()
			.map(|tracker| tracker.covered_slabs().collect::<BTreeSet<_>>())
			.unwrap_or_default();
		let mut tail = BTreeSet::new();
		// The active slab can receive more writes after any buffered
		// entries are sealed, so it remains a scan tail until rotation.
		tail.insert(active_slab_id);
		for source_slab_id in historical_slabs {
			if !covered_slabs.contains(&source_slab_id) {
				tail.insert(source_slab_id);
			}
		}
		if let Some(tracker) = tracker {
			if let Some(buffer) = tracker.active_buffer() {
				tail.insert(buffer.source_slab_id());
			}
			tail.extend(
				tracker
					.uncovered_slabs()
					.map(|(source_slab_id, _)| source_slab_id),
			);
		}
		tail
	}

	pub fn list_secondary_index_coverage_snapshots(&self) -> Vec<SecondaryIndexCoverageSnapshot> {
		let mut out = self
			.secondary_index_coverage
			.iter()
			.map(|entry| entry.value().snapshot())
			.collect::<Vec<_>>();
		out.sort_by_key(|snapshot| {
			(
				snapshot.key.tenant_id,
				snapshot.key.table_object_id,
				snapshot.key.index_object_id,
			)
		});
		out
	}

	pub fn append_secondary_index_active_buffer_batch(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		batch: &RecordBatch,
		base_location: RecordLocation,
		max_buffer_bytes: usize,
	) -> std::io::Result<usize> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		if tracker
			.active_buffer()
			.is_some_and(|buffer| buffer.source_slab_id() != base_location.slab_id)
		{
			let old_slab_id = tracker
				.active_buffer()
				.expect("checked Some above")
				.source_slab_id();
			tracker.mark_uncovered(
				old_slab_id,
				SecondaryIndexBackfillError::new(
					SecondaryIndexBackfillErrorReason::SealFailure,
					"active buffer moved to a different slab before seal",
				),
			);
		}
		if tracker.active_buffer().is_none() {
			tracker.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				base_location.slab_id,
				max_buffer_bytes,
			))?;
		}
		let result = tracker
			.active_buffer_mut()
			.expect("active buffer installed above")
			.append_storage_batch(descriptor, batch, base_location);
		if let Err(err) = result {
			let failure = tracker
				.active_buffer()
				.and_then(|buffer| buffer.failure().cloned())
				.unwrap_or_else(|| {
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::EncodeFailure,
						err.to_string(),
					)
				});
			metrics().observe_storage_secondary_index_active_buffer_failure(
				tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				failure.reason.label(),
			);
			tracker.mark_uncovered(base_location.slab_id, failure);
			return Err(err);
		}
		result
	}

	pub fn append_secondary_index_active_buffer_entries(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		entries: Vec<SecondaryIndexEntry>,
		source_slab_id: SlabId,
		max_buffer_bytes: usize,
	) -> std::io::Result<usize> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		if tracker
			.active_buffer()
			.is_some_and(|buffer| buffer.source_slab_id() != source_slab_id)
		{
			let old_slab_id = tracker
				.active_buffer()
				.expect("checked Some above")
				.source_slab_id();
			tracker.mark_uncovered(
				old_slab_id,
				SecondaryIndexBackfillError::new(
					SecondaryIndexBackfillErrorReason::SealFailure,
					"active buffer moved to a different slab before seal",
				),
			);
		}
		if tracker.active_buffer().is_none() {
			tracker.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				source_slab_id,
				max_buffer_bytes,
			))?;
		}
		let result = tracker
			.active_buffer_mut()
			.expect("active buffer installed above")
			.append_entries(descriptor, entries, source_slab_id);
		if let Err(err) = result {
			let failure = tracker
				.active_buffer()
				.and_then(|buffer| buffer.failure().cloned())
				.unwrap_or_else(|| {
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::EncodeFailure,
						err.to_string(),
					)
				});
			metrics().observe_storage_secondary_index_active_buffer_failure(
				tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				failure.reason.label(),
			);
			tracker.mark_uncovered(source_slab_id, failure);
			return Err(err);
		}
		result
	}

	pub async fn seal_secondary_index_active_buffer(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
	) -> std::io::Result<Option<SecondaryIndexSegmentInfo>> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let buffer = {
			let mut tracker = self
				.secondary_index_coverage
				.entry(key)
				.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
			tracker.take_active_buffer()
		};
		let Some(buffer) = buffer else {
			return Ok(None);
		};
		let source_slab_id = buffer.source_slab_id();
		let segment = match buffer.seal(descriptor) {
			Ok(segment) => segment,
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::SealFailure,
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_active_buffer_failure(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					SecondaryIndexBackfillErrorReason::SealFailure.label(),
				);
				return Err(err);
			}
		};
		match self.persist_secondary_index_segment(&segment).await {
			Ok(info) => Ok(Some(info)),
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_segment_failed(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					SecondaryIndexBackfillErrorReason::PersistFailure.label(),
				);
				Err(err)
			}
		}
	}

	pub async fn seal_secondary_index_active_buffer_if_source_changed(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		next_source_slab_id: SlabId,
	) -> std::io::Result<Option<SecondaryIndexSegmentInfo>> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let key = SecondaryIndexCoverageKey::from(identity);
		let should_seal = self
			.secondary_index_coverage
			.get(&key)
			.and_then(|tracker| {
				tracker
					.value()
					.active_buffer()
					.map(|buffer| buffer.source_slab_id() != next_source_slab_id)
			})
			.unwrap_or(false);
		if should_seal {
			self.seal_secondary_index_active_buffer(tenant_id, descriptor)
				.await
		} else {
			Ok(None)
		}
	}

	pub async fn backfill_secondary_index_source_slab(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		self.backfill_secondary_index_source_slab_inner(
			tenant_id,
			table,
			descriptor,
			source_slab_id,
		)
		.await
	}

	pub async fn backfill_secondary_index_candidate(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		candidate: &SecondaryIndexBackfillCandidate,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		let expected_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor),
		);
		if candidate.key != expected_key {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index backfill candidate does not match descriptor identity",
			));
		}
		self.backfill_secondary_index_source_slab_inner(
			tenant_id,
			table,
			descriptor,
			candidate.source_slab_id,
		)
		.await
	}

	pub async fn run_secondary_index_backfill_once(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		max_slabs: usize,
		max_buffer_bytes: usize,
	) -> std::io::Result<SecondaryIndexBackfillTickReport> {
		let before = self
			.ensure_secondary_index_coverage(tenant_id, descriptor, max_buffer_bytes)
			.await?;
		let mut completed = Vec::new();
		let mut failures = Vec::new();

		for candidate in before.candidates.iter().take(max_slabs) {
			match self
				.backfill_secondary_index_candidate(tenant_id, table, descriptor, candidate)
				.await
			{
				Ok(info) => completed.push(SecondaryIndexBackfillSuccess {
					candidate: candidate.clone(),
					entry_count: info.entry_count,
				}),
				Err(err) => failures.push(SecondaryIndexBackfillFailure {
					candidate: candidate.clone(),
					error: SecondaryIndexBackfillError::new(
						secondary_index_backfill_reason_from_io(&err),
						err.to_string(),
					),
				}),
			}
		}

		let after = self
			.ensure_secondary_index_coverage(tenant_id, descriptor, max_buffer_bytes)
			.await?;
		let attempted = completed.len() + failures.len();
		let historical_complete =
			after.candidates.is_empty() && after.snapshot.uncovered_slabs.is_empty();

		Ok(SecondaryIndexBackfillTickReport {
			before,
			after,
			attempted,
			completed,
			failures,
			historical_complete,
		})
	}

	pub(super) async fn backfill_secondary_index_source_slab_inner(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		let started_at = Instant::now();
		descriptor.validate_against_table(table)?;
		let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
			tenant_id, descriptor,
		));
		let source_slab_lease = self
			.acquire_secondary_index_source_slab_read_lease(source_slab_id)
			.await
			.map_err(|err| {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						secondary_index_backfill_reason_from_io(&err),
						err.to_string(),
					),
				);
				err
			})?;
		let segment = match self
			.build_secondary_index_segment_for_source_slab(
				tenant_id,
				table,
				descriptor,
				source_slab_id,
			)
			.await
		{
			Ok(segment) => segment,
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						secondary_index_backfill_reason_from_io(&err),
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_segment_failed(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					secondary_index_backfill_reason_from_io(&err).label(),
				);
				return Err(err);
			}
		};
		if let Err(err) = self
			.ensure_secondary_index_source_slab_available(source_slab_id)
			.await
		{
			self.mark_secondary_index_uncovered(
				key,
				source_slab_id,
				SecondaryIndexBackfillError::new(
					secondary_index_backfill_reason_from_io(&err),
					err.to_string(),
				),
			);
			metrics().observe_storage_secondary_index_segment_failed(
				tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
				secondary_index_backfill_reason_from_io(&err).label(),
			);
			return Err(err);
		}
		let result = match self.persist_secondary_index_segment(&segment).await {
			Ok(info) => {
				metrics().observe_storage_secondary_index_backfill_seconds(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					started_at.elapsed(),
				);
				Ok(info)
			}
			Err(err) => {
				self.mark_secondary_index_uncovered(
					key,
					source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				metrics().observe_storage_secondary_index_segment_failed(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					SecondaryIndexBackfillErrorReason::PersistFailure.label(),
				);
				Err(err)
			}
		};
		// Held until after sidecar persistence so compaction cannot
		// retire the source slab underneath a committed sidecar.
		// Explicitly name the guard for the ADR 052 read-lease grep.
		drop(source_slab_lease);
		result
	}

	pub fn mark_secondary_index_backfill_uncovered(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
		error: SecondaryIndexBackfillError,
	) {
		self.mark_secondary_index_uncovered(key, source_slab_id, error);
	}

	pub async fn persist_secondary_index_segment(
		&self,
		segment: &SecondaryIndexSegment,
	) -> std::io::Result<SecondaryIndexSegmentInfo> {
		let identity = segment.identity();
		let segment_path = secondary_index_segment_storage_path(
			segment.header.tenant_id,
			segment.header.table_object_id,
			segment.header.index_object_id,
			segment.header.source_slab_id,
		);
		let info = SecondaryIndexSegmentInfo {
			tenant_id: segment.header.tenant_id,
			table_object_id: segment.header.table_object_id,
			index_object_id: segment.header.index_object_id,
			logical_schema_fingerprint: identity.logical_schema_fingerprint,
			source_slab_id: segment.header.source_slab_id,
			segment_path,
			l2_uri: None,
			payload_hash: segment.header.payload_hash,
			state_digest: *segment.header.state_digest.as_bytes(),
			format_version: segment.header.format_version,
			entry_count: segment.header.entry_count,
		};
		let row = PersistentSecondaryIndexSegmentRow::from(info.clone());
		let encoded = segment.encode()?;
		let path = secondary_index_sidecar_path(&self.config.dir, &info.segment_path)?;
		let tmp_path = secondary_index_sidecar_tmp_path(&path);
		if let Some(parent) = path.parent() {
			self.file_ops.create_dir_all(parent).await?;
		}
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		self.file_ops.write_all(&tmp_path, &encoded).await?;
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
		if let Some(index) = &self.persistent_index {
			if let Err(err) = index.upsert_secondary_index_segment(row.clone()).await {
				let _ = unlink_local_file(self.file_ops.as_ref(), &path).await;
				return Err(err);
			}
		}
		self.secondary_index_segments.insert(row.key, row);
		self.mark_secondary_index_covered(info.coverage_key(), info.source_slab_id);
		metrics().observe_storage_secondary_index_segment_built(
			info.tenant_id,
			info.table_object_id,
			info.index_object_id,
			info.entry_count,
		);
		Ok(info)
	}

	pub async fn load_secondary_index_segment(
		&self,
		key: &SecondaryIndexSegmentInfo,
	) -> std::io::Result<Option<SecondaryIndexSegment>> {
		let Some(row) = self
			.secondary_index_segments
			.get(&key.key())
			.map(|entry| entry.value().clone())
		else {
			return Ok(None);
		};
		let path = secondary_index_sidecar_path(&self.config.dir, &row.segment_path)?;
		let bytes = match self.file_ops.read_all(&path).await {
			Ok(bytes) => bytes,
			Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err),
		};
		match SecondaryIndexSegment::decode(&bytes) {
			Ok(segment) => Ok(Some(segment)),
			Err(err) => {
				metrics().observe_storage_secondary_index_corruption_failure(
					row.key.tenant_id,
					row.key.table_object_id,
					row.key.index_object_id,
				);
				self.mark_secondary_index_uncovered(
					secondary_index_coverage_key_from_persistent(&row.key),
					row.key.source_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::CorruptSegment,
						err.to_string(),
					),
				);
				Err(err)
			}
		}
	}

	pub async fn delete_secondary_index_segment(
		&self,
		key: &SecondaryIndexSegmentInfo,
	) -> std::io::Result<bool> {
		let Some(row) = self
			.secondary_index_segments
			.get(&key.key())
			.map(|entry| entry.value().clone())
		else {
			return Ok(false);
		};
		if let Some(index) = &self.persistent_index {
			index.delete_secondary_index_segment(row.key).await?;
		}
		let path = secondary_index_sidecar_path(&self.config.dir, &row.segment_path)?;
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		self.secondary_index_segments.remove(&row.key);
		self.mark_secondary_index_uncovered(
			secondary_index_coverage_key_from_persistent(&row.key),
			row.key.source_slab_id,
			SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::MissingSegment,
				"secondary index sidecar segment deleted",
			),
		);
		Ok(true)
	}

	pub async fn gc_secondary_index_segments_for_dropping_index(
		&self,
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
	) -> std::io::Result<usize> {
		let started_at = Instant::now();
		let segments = self.list_secondary_index_segments(
			tenant_id,
			descriptor.table_object_id,
			descriptor.index_object_id,
			descriptor.logical_schema_fingerprint,
		);
		let mut removed = 0usize;
		for info in segments {
			if self
				.retire_secondary_index_segment_for_replaced_slab(&info)
				.await?
			{
				removed = removed.saturating_add(1);
			}
		}
		let coverage_key = SecondaryIndexCoverageKey::from(
			SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor),
		);
		self.secondary_index_coverage.remove(&coverage_key);
		metrics().set_storage_secondary_index_uncovered_slabs(
			tenant_id,
			descriptor.table_object_id,
			descriptor.index_object_id,
			0,
		);
		metrics().observe_storage_secondary_index_drop_gc_seconds(
			tenant_id,
			descriptor.table_object_id,
			descriptor.index_object_id,
			started_at.elapsed(),
		);
		Ok(removed)
	}

	pub fn list_secondary_index_segments(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		index_object_id: IndexObjectId,
		logical_schema_fingerprint: LogicalSchemaFingerprint,
	) -> Vec<SecondaryIndexSegmentInfo> {
		let mut out = self
			.secondary_index_segments
			.iter()
			.filter_map(|entry| {
				let key = entry.key();
				(key.tenant_id == tenant_id
					&& key.table_object_id == table_object_id
					&& key.index_object_id == index_object_id
					&& key.logical_schema_fingerprint == logical_schema_fingerprint)
					.then(|| SecondaryIndexSegmentInfo::from(entry.value().clone()))
			})
			.collect::<Vec<_>>();
		out.sort_by_key(|info| info.source_slab_id);
		out
	}

	pub(super) fn list_secondary_index_segments_for_source_slab(
		&self,
		source_slab_id: SlabId,
	) -> Vec<SecondaryIndexSegmentInfo> {
		let mut out = self
			.secondary_index_segments
			.iter()
			.filter_map(|entry| {
				(entry.key().source_slab_id == source_slab_id)
					.then(|| SecondaryIndexSegmentInfo::from(entry.value().clone()))
			})
			.collect::<Vec<_>>();
		out.sort_by(|left, right| {
			(left.tenant_id, left.table_object_id, left.index_object_id)
				.cmp(&(
					right.tenant_id,
					right.table_object_id,
					right.index_object_id,
				))
				.then_with(|| {
					left.logical_schema_fingerprint
						.as_bytes()
						.cmp(right.logical_schema_fingerprint.as_bytes())
				})
		});
		out
	}

	pub(super) fn forget_secondary_index_source_slab(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
	) {
		if let Some(mut tracker) = self.secondary_index_coverage.get_mut(&key) {
			tracker.forget_slab(source_slab_id);
			let uncovered = tracker.uncovered_slabs().count() as u64;
			drop(tracker);
			self.observe_secondary_index_uncovered_slabs(key, uncovered);
		}
	}

	pub(super) async fn retire_secondary_index_segment_for_replaced_slab(
		&self,
		info: &SecondaryIndexSegmentInfo,
	) -> std::io::Result<bool> {
		let Some(row) = self
			.secondary_index_segments
			.get(&info.key())
			.map(|entry| entry.value().clone())
		else {
			self.forget_secondary_index_source_slab(info.coverage_key(), info.source_slab_id);
			return Ok(false);
		};
		if let Some(index) = &self.persistent_index {
			index.delete_secondary_index_segment(row.key).await?;
		}
		let path = secondary_index_sidecar_path(&self.config.dir, &row.segment_path)?;
		unlink_local_file(self.file_ops.as_ref(), &path).await?;
		self.secondary_index_segments.remove(&row.key);
		self.forget_secondary_index_source_slab(info.coverage_key(), info.source_slab_id);
		Ok(true)
	}

	pub(super) fn secondary_index_table_ids_from_plaintext(
		&self,
		source_slab_id: SlabId,
		plaintext: &[u8],
	) -> std::io::Result<BTreeSet<(TenantId, TableObjectId)>> {
		let mut table_ids = BTreeSet::new();
		for recovered in recover_slab_entries_from_blob(plaintext, source_slab_id)? {
			let frame = match extract_record_from_blob(plaintext, recovered.location) {
				Ok(frame) => frame,
				Err(_) => continue,
			};
			let storage_batch = match decode_record_batch(&frame) {
				Ok(batch) => batch,
				Err(_) => continue,
			};
			if let Some(table_object_id) =
				storage_row_table_object_id_at(&storage_batch, recovered.location.record_row_index)?
			{
				table_ids.insert((recovered.tenant_id, table_object_id));
			}
		}
		Ok(table_ids)
	}

	pub(super) async fn secondary_index_compaction_descriptors_for_source_slab(
		&self,
		source_slab_id: SlabId,
		context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Vec<(TenantId, TableSchemaDescriptor, SecondaryIndexDescriptor)>> {
		let plaintext = self
			.secondary_index_source_slab_plaintext(source_slab_id)
			.await?;
		self.secondary_index_compaction_descriptors_for_plaintext(
			source_slab_id,
			&plaintext,
			context,
		)
	}

	pub(super) fn secondary_index_compaction_descriptors_for_plaintext(
		&self,
		source_slab_id: SlabId,
		plaintext: &[u8],
		context: SecondaryIndexCompactionContext<'_>,
	) -> std::io::Result<Vec<(TenantId, TableSchemaDescriptor, SecondaryIndexDescriptor)>> {
		let mut descriptors = Vec::new();
		for (tenant_id, table_object_id) in
			self.secondary_index_table_ids_from_plaintext(source_slab_id, plaintext)?
		{
			for row in context
				.secondary_indexes
				.for_table(tenant_id, table_object_id)
			{
				if !matches!(
					row.state,
					SecondaryIndexState::Building | SecondaryIndexState::Ready
				) {
					continue;
				}
				let Some(table) = context.table_schemas.get(
					tenant_id,
					row.descriptor.table_object_id,
					row.descriptor.table_schema_generation,
				) else {
					continue;
				};
				if row.descriptor.validate_against_table(table).is_ok() {
					descriptors.push((tenant_id, table.clone(), row.descriptor.clone()));
				}
			}
		}
		descriptors.sort_by_key(|(tenant_id, _table, descriptor)| {
			(
				*tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
			)
		});
		descriptors.dedup_by_key(|(tenant_id, _table, descriptor)| {
			(
				*tenant_id,
				descriptor.table_object_id,
				descriptor.index_object_id,
			)
		});
		Ok(descriptors)
	}

	pub(super) async fn reconcile_secondary_index_segments_after_compaction(
		&self,
		old_slab_id: SlabId,
		new_slab_id: Option<SlabId>,
		context: Option<SecondaryIndexCompactionContext<'_>>,
		old_descriptors: Vec<(TenantId, TableSchemaDescriptor, SecondaryIndexDescriptor)>,
	) -> usize {
		let Some(context) = context else {
			return 0;
		};
		let mut retired_keys = HashSet::new();
		for old_info in self.list_secondary_index_segments_for_source_slab(old_slab_id) {
			let Some(row) = context
				.secondary_indexes
				.get(old_info.tenant_id, old_info.index_object_id)
			else {
				continue;
			};
			if !matches!(
				row.state,
				SecondaryIndexState::Building | SecondaryIndexState::Ready
			) {
				continue;
			}
			match self
				.retire_secondary_index_segment_for_replaced_slab(&old_info)
				.await
			{
				Ok(_) => {
					retired_keys.insert(old_info.coverage_key());
				}
				Err(err) => {
					tracing::warn!(
						old_slab_id = old_slab_id.get(),
						index_object_id = ?old_info.index_object_id,
						error = %err,
						"failed to retire old secondary index sidecar after compaction"
					);
				}
			}
		}
		for (tenant_id, _table, descriptor) in old_descriptors {
			let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
				tenant_id,
				&descriptor,
			));
			if retired_keys.insert(key) {
				self.forget_secondary_index_source_slab(key, old_slab_id);
			}
		}

		let Some(new_slab_id) = new_slab_id else {
			return 0;
		};
		let descriptors = match self
			.secondary_index_compaction_descriptors_for_source_slab(new_slab_id, context)
			.await
		{
			Ok(descriptors) => descriptors,
			Err(err) => {
				tracing::warn!(
					old_slab_id = old_slab_id.get(),
					new_slab_id = new_slab_id.get(),
					error = %err,
					"failed to enumerate secondary index descriptors for compacted slab"
				);
				return 0;
			}
		};

		let mut rebuilt = 0usize;
		for (tenant_id, table, descriptor) in descriptors {
			let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
				tenant_id,
				&descriptor,
			));
			if retired_keys.insert(key) {
				self.forget_secondary_index_source_slab(key, old_slab_id);
			}
			let old_info = self
				.list_secondary_index_segments(
					tenant_id,
					descriptor.table_object_id,
					descriptor.index_object_id,
					descriptor.logical_schema_fingerprint,
				)
				.into_iter()
				.find(|info| info.source_slab_id == old_slab_id);
			if let Some(old_info) = old_info
				&& let Err(err) = self
					.retire_secondary_index_segment_for_replaced_slab(&old_info)
					.await
			{
				tracing::warn!(
					old_slab_id = old_slab_id.get(),
					index_object_id = ?descriptor.index_object_id,
					error = %err,
					"failed to retire old secondary index sidecar before rebuild"
				);
			}
			let segment = match self
				.build_secondary_index_segment_for_source_slab(
					tenant_id,
					&table,
					&descriptor,
					new_slab_id,
				)
				.await
			{
				Ok(segment) => segment,
				Err(err) => {
					self.mark_secondary_index_uncovered(
						key,
						new_slab_id,
						SecondaryIndexBackfillError::new(
							secondary_index_backfill_reason_from_io(&err),
							err.to_string(),
						),
					);
					tracing::warn!(
						new_slab_id = new_slab_id.get(),
						index_object_id = ?descriptor.index_object_id,
						error = %err,
						"failed to rebuild secondary index sidecar after compaction"
					);
					continue;
				}
			};
			if let Err(err) = self.persist_secondary_index_segment(&segment).await {
				self.mark_secondary_index_uncovered(
					key,
					new_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				tracing::warn!(
					new_slab_id = new_slab_id.get(),
					index_object_id = ?descriptor.index_object_id,
					error = %err,
					"failed to persist rebuilt secondary index sidecar after compaction"
				);
				continue;
			}
			rebuilt = rebuilt.saturating_add(1);
		}
		rebuilt
	}

	pub(super) async fn prepare_lsm_secondary_index_segments_for_compaction(
		&self,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
		context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> Result<Option<LsmPreparedSecondaryIndexSegments>, CompactionFailure> {
		let Some(context) = context else {
			return Ok(None);
		};

		let mut old_sidecars = Vec::new();
		let mut old_descriptors = Vec::new();
		for old_slab_id in plan.touched_slabs() {
			for old_info in self.list_secondary_index_segments_for_source_slab(old_slab_id) {
				old_sidecars.push(old_info);
			}
			for (tenant_id, _table, descriptor) in self
				.secondary_index_compaction_descriptors_for_source_slab(old_slab_id, context)
				.await?
			{
				old_descriptors.push((
					old_slab_id,
					SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
						tenant_id,
						&descriptor,
					)),
				));
			}
		}

		let mut output_segments = Vec::new();
		for run in written {
			let path = slab_path(&self.config.dir, run.slab_id);
			let plaintext = self.file_ops.read_all(&path).await?;
			for (tenant_id, table, descriptor) in self
				.secondary_index_compaction_descriptors_for_plaintext(
					run.slab_id,
					&plaintext,
					context,
				)? {
				let key = SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(
					tenant_id,
					&descriptor,
				));
				let segment = self.build_secondary_index_segment_from_plaintext(
					tenant_id,
					&table,
					&descriptor,
					run.slab_id,
					&plaintext,
				)?;
				self.verify_lsm_secondary_index_segment_cold_rebuild(
					&segment,
					tenant_id,
					&table,
					&descriptor,
					run.slab_id,
					&plaintext,
				)?;
				output_segments.push(LsmPreparedSecondaryIndexSegment {
					key,
					output_slab_id: run.slab_id,
					index_object_id: descriptor.index_object_id,
					segment,
				});
			}
		}

		Ok(Some(LsmPreparedSecondaryIndexSegments {
			old_sidecars,
			old_descriptors,
			output_segments,
		}))
	}

	pub(super) fn verify_lsm_secondary_index_segment_cold_rebuild(
		&self,
		segment: &SecondaryIndexSegment,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
		plaintext: &[u8],
	) -> Result<(), CompactionFailure> {
		let cold_rebuild = self.build_secondary_index_segment_from_plaintext(
			tenant_id,
			table,
			descriptor,
			source_slab_id,
			plaintext,
		)?;
		if &cold_rebuild != segment {
			return Err(CompactionFailure::SidecarDigestMismatch);
		}
		Ok(())
	}

	pub(super) async fn publish_prepared_lsm_secondary_index_segments(
		&self,
		prepared: Option<LsmPreparedSecondaryIndexSegments>,
	) -> usize {
		let Some(prepared) = prepared else {
			return 0;
		};
		let mut coverage_by_slab: BTreeMap<SlabId, (usize, usize)> = BTreeMap::new();

		for old_info in prepared.old_sidecars {
			if let Err(err) = self
				.retire_secondary_index_segment_for_replaced_slab(&old_info)
				.await
			{
				tracing::warn!(
					old_slab_id = old_info.source_slab_id.get(),
					index_object_id = ?old_info.index_object_id,
					error = %err,
					"failed to retire old secondary index sidecar after LSM compaction"
				);
			}
		}
		for (old_slab_id, key) in prepared.old_descriptors {
			self.forget_secondary_index_source_slab(key, old_slab_id);
		}

		let mut rebuilt = 0usize;
		for prepared_segment in prepared.output_segments {
			let coverage = coverage_by_slab
				.entry(prepared_segment.output_slab_id)
				.or_default();
			coverage.0 = coverage.0.saturating_add(1);
			if let Err(err) = self
				.persist_secondary_index_segment(&prepared_segment.segment)
				.await
			{
				self.mark_secondary_index_uncovered(
					prepared_segment.key,
					prepared_segment.output_slab_id,
					SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::PersistFailure,
						err.to_string(),
					),
				);
				tracing::warn!(
					output_slab_id = prepared_segment.output_slab_id.get(),
					index_object_id = ?prepared_segment.index_object_id,
					error = %err,
					"failed to persist prepared secondary index sidecar after LSM compaction"
				);
				continue;
			}
			let coverage = coverage_by_slab
				.entry(prepared_segment.output_slab_id)
				.or_default();
			coverage.1 = coverage.1.saturating_add(1);
			rebuilt = rebuilt.saturating_add(1);
		}
		self.update_lsm_sidecar_coverage_after_publish(coverage_by_slab)
			.await;
		rebuilt
	}

	pub(super) async fn update_lsm_sidecar_coverage_after_publish(
		&self,
		coverage_by_slab: BTreeMap<SlabId, (usize, usize)>,
	) {
		let updates = coverage_by_slab
			.into_iter()
			.filter_map(|(slab_id, (attempted, succeeded))| {
				if attempted == 0 {
					return None;
				}
				let coverage = if attempted == succeeded {
					crate::storage::SidecarCoverage::Complete
				} else {
					crate::storage::SidecarCoverage::Partial
				};
				Some((slab_id, coverage))
			})
			.collect::<Vec<_>>();
		if updates.is_empty() {
			return;
		}

		let lsm_runs = {
			let mut manifest =
				recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
			for (slab_id, coverage) in updates {
				let Some(run) = manifest.get_run(slab_id).cloned() else {
					continue;
				};
				if !run.state.is_live() || run.sidecar_coverage == coverage {
					continue;
				}
				let mut updated = run;
				updated.sidecar_coverage = coverage;
				manifest.insert_live_run(updated);
			}
			let snapshot = manifest.snapshot();
			drop(manifest);
			refresh_lsm_level_metrics(&snapshot);
			snapshot.runs().to_vec()
		};
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				put_lsm_runs: lsm_runs,
				..Default::default()
			},
			"lsm-sidecar-coverage-update",
		)
		.await;
	}

	#[allow(dead_code)]
	pub(super) async fn reconcile_lsm_secondary_index_segments_after_compaction(
		&self,
		plan: &CompactionPlan,
		written: &[LsmWrittenOutputRun],
		context: Option<SecondaryIndexCompactionContext<'_>>,
	) -> usize {
		match self
			.prepare_lsm_secondary_index_segments_for_compaction(plan, written, context)
			.await
		{
			Ok(prepared) => {
				self.publish_prepared_lsm_secondary_index_segments(prepared)
					.await
			}
			Err(err) => {
				tracing::warn!(
					error = %err,
					"failed to prepare secondary index sidecars after LSM compaction"
				);
				0
			}
		}
	}

	pub(super) async fn secondary_index_source_slabs(&self) -> (SlabId, Vec<SlabId>) {
		let state = self.state.lock().await;
		let active_slab_id = state.active.slab_id();
		let mut historical = state
			.sealed
			.iter()
			.map(|entry| *entry.key())
			.chain(state.tiered.iter().map(|entry| *entry.key()))
			.collect::<Vec<_>>();
		historical.sort_unstable();
		historical.dedup();
		(active_slab_id, historical)
	}

	pub(super) fn secondary_index_source_slab_lease(
		&self,
		source_slab_id: SlabId,
	) -> Arc<AsyncRwLock<()>> {
		self.secondary_index_source_slab_leases
			.entry(source_slab_id)
			.or_insert_with(|| Arc::new(AsyncRwLock::new(())))
			.clone()
	}

	pub(super) async fn acquire_secondary_index_source_slab_read_lease(
		&self,
		source_slab_id: SlabId,
	) -> std::io::Result<OwnedRwLockReadGuard<()>> {
		let source_slab_lease = self.secondary_index_source_slab_lease(source_slab_id);
		let guard = source_slab_lease.read_owned().await;
		self.ensure_secondary_index_source_slab_available(source_slab_id)
			.await?;
		Ok(guard)
	}

	pub(super) async fn acquire_secondary_index_source_slab_compaction_lease(
		&self,
		source_slab_id: SlabId,
	) -> OwnedRwLockWriteGuard<()> {
		self.secondary_index_source_slab_lease(source_slab_id)
			.write_owned()
			.await
	}

	pub(super) async fn ensure_secondary_index_source_slab_available(
		&self,
		source_slab_id: SlabId,
	) -> std::io::Result<()> {
		let state = self.state.lock().await;
		if state.active.slab_id() == source_slab_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index backfill source slab is still active",
			));
		}
		if state.sealed.contains_key(&source_slab_id) || state.tiered.contains_key(&source_slab_id)
		{
			return Ok(());
		}
		Err(Error::new(
			ErrorKind::NotFound,
			format!("secondary index backfill source slab {source_slab_id} not found"),
		))
	}

	pub(super) async fn build_secondary_index_segment_for_source_slab(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
	) -> std::io::Result<SecondaryIndexSegment> {
		let plaintext = self
			.secondary_index_source_slab_plaintext(source_slab_id)
			.await?;
		self.build_secondary_index_segment_from_plaintext(
			tenant_id,
			table,
			descriptor,
			source_slab_id,
			&plaintext,
		)
	}

	pub(super) fn build_secondary_index_segment_from_plaintext(
		&self,
		tenant_id: TenantId,
		table: &TableSchemaDescriptor,
		descriptor: &SecondaryIndexDescriptor,
		source_slab_id: SlabId,
		plaintext: &[u8],
	) -> std::io::Result<SecondaryIndexSegment> {
		let identity = SecondaryIndexIdentity::from_descriptor(tenant_id, descriptor);
		let mut index_entries = Vec::new();
		for recovered in recover_slab_entries_from_blob(plaintext, source_slab_id)? {
			if recovered.tenant_id != tenant_id || recovered.location.is_tombstone {
				continue;
			}
			let frame = extract_record_from_blob(plaintext, recovered.location)?;
			let storage_batch = match decode_record_batch(&frame) {
				Ok(batch) => batch,
				Err(_) => continue,
			};
			let stored_table_object_id = storage_row_table_object_id_at(
				&storage_batch,
				recovered.location.record_row_index,
			)?;
			let materialized = match materialize_record_location_batch(
				&storage_batch,
				recovered.location.record_row_index,
			) {
				Ok(batch) if batch.num_rows() == 1 => batch,
				Ok(_) => continue,
				Err(err) => return Err(err),
			};
			let routing_locality = self.recovered_entry_routing_locality(&recovered);
			if !descriptor_matches_tiered_row(
				table,
				tenant_id,
				recovered.key_identity,
				routing_locality,
				stored_table_object_id,
				&materialized,
			)? {
				continue;
			}
			let Some(encoded_key) = encode_secondary_index_key(&materialized, 0, descriptor)?
			else {
				continue;
			};
			index_entries.push(SecondaryIndexEntry {
				encoded_key,
				row_key_id: recovered.key_identity,
				row_hlc: recovered.location.hlc,
				record_location: RecordLocation {
					is_tombstone: false,
					..recovered.location
				},
			});
		}
		SecondaryIndexSegment::build(
			identity,
			source_slab_id,
			descriptor.table_schema_generation,
			descriptor.key.arrow_type_tag,
			index_entries,
		)
	}

	pub(super) async fn secondary_index_source_slab_plaintext(
		&self,
		source_slab_id: SlabId,
	) -> std::io::Result<Vec<u8>> {
		let tier_pointer = {
			let state = self.state.lock().await;
			if state.active.slab_id() == source_slab_id {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"secondary index backfill source slab is still active",
				));
			}
			if state.sealed.contains_key(&source_slab_id) {
				None
			} else if let Some(pointer) = state.tiered.get(&source_slab_id) {
				Some(pointer.clone())
			} else {
				return Err(Error::new(
					ErrorKind::NotFound,
					format!("secondary index backfill source slab {source_slab_id} not found"),
				));
			}
		};
		if let Some(pointer) = tier_pointer {
			self.tiered_plaintext_from_l2(&pointer).await
		} else {
			self.read_sealed_raw(source_slab_id).await
		}
	}

	pub(super) fn mark_secondary_index_covered(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
	) {
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		tracker.mark_covered(source_slab_id);
		let uncovered = tracker.uncovered_slabs().count() as u64;
		drop(tracker);
		self.observe_secondary_index_uncovered_slabs(key, uncovered);
	}

	pub(super) fn mark_secondary_index_uncovered(
		&self,
		key: SecondaryIndexCoverageKey,
		source_slab_id: SlabId,
		error: SecondaryIndexBackfillError,
	) {
		let mut tracker = self
			.secondary_index_coverage
			.entry(key)
			.or_insert_with(|| SecondaryIndexCoverageTracker::new(key));
		tracker.mark_uncovered(source_slab_id, error);
		let uncovered = tracker.uncovered_slabs().count() as u64;
		drop(tracker);
		self.observe_secondary_index_uncovered_slabs(key, uncovered);
	}

	pub(super) fn observe_secondary_index_uncovered_slabs(
		&self,
		key: SecondaryIndexCoverageKey,
		uncovered: u64,
	) {
		metrics().set_storage_secondary_index_uncovered_slabs(
			key.tenant_id,
			key.table_object_id,
			key.index_object_id,
			uncovered,
		);
	}
}
