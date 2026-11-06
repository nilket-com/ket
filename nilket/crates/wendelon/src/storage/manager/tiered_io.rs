use super::tier_deferral::{DeferralOutcome, FenceCause, TierDeferral, TierReplay, fenced_error};
use super::tier_plan::tier_row_owner;
use super::*;
use crate::storage::io_uring::DIRECT_IO_ALIGN;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct TieredRowRefreshPlan {
	/// The pointer whose object the plan was built from; the plan applies only
	/// while that pointer is the slab's published generation.
	pub(super) pointer: Option<TieredSlabPointer>,
	pub(super) old_rows: Vec<PersistentRecordRow>,
	pub(super) new_rows: Vec<PersistentRecordRow>,
	pub(super) relocated_reads: Vec<(RecordLocation, RecordLocation)>,
	/// The node budget charge for building and applying this plan.
	pub(super) scratch: super::tier_budget::PlanScratch,
}

impl TieredRowRefreshPlan {
	/// The copies of the plan's rows (old, then new) a publication hands its
	/// index transaction.
	pub(super) fn try_copy_rows(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<(Vec<PersistentRecordRow>, Vec<PersistentRecordRow>)> {
		Ok((
			covered_row_copy(slab_id, &[&self.old_rows])?,
			covered_row_copy(slab_id, &[&self.new_rows])?,
		))
	}

	/// Every old and new row, for the publication's LSM scope check.
	pub(super) fn try_touched_rows(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		covered_row_copy(slab_id, &[&self.old_rows, &self.new_rows])
	}
}

/// `parts` copied into exactly sized storage, or a typed scratch refusal: the
/// plan's charge paid for this copy (`reserve_tier_plan_scratch`).
fn covered_row_copy(
	slab_id: SlabId,
	parts: &[&[PersistentRecordRow]],
) -> std::io::Result<Vec<PersistentRecordRow>> {
	use super::tier_budget::{TierHistoryShortfall, tier_history_refusal};
	let short = || tier_history_refusal(slab_id, TierHistoryShortfall::Scratch);
	let len = parts
		.iter()
		.try_fold(0usize, |len, part| len.checked_add(part.len()))
		.ok_or_else(short)?;
	let mut copy = Vec::new();
	copy.try_reserve_exact(len).map_err(|_| short())?;
	for part in parts {
		copy.extend_from_slice(part);
	}
	Ok(copy)
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

/// Which generation a tier object's bytes are, and so which record offsets
/// describe them. SlabId alone does not say: a replacement keeps the slab id
/// and moves its records.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TierObjectEvidence<'a> {
	/// The object is the generation the manager's index describes: the
	/// published generation, the source of a rewrite, or a first upload of the
	/// indexed local slab. The index's offsets are independent evidence that
	/// every acknowledged record is present; the object's own offset index,
	/// written by whichever build produced it, never replaces them.
	Indexed(&'a BTreeSet<u64>),
	/// An object whose layout the index does not describe: a replacement not
	/// yet published, or committed authority staged over unrelated local
	/// evidence (bootstrap or snapshot recovery), not necessarily a rewrite.
	/// Its own authenticated offset index is the evidence; a plaintext object
	/// has none and is refused.
	Replacement,
}

/// The evidence for decoding `pointer`'s object, given the slab's published
/// generation. The index describes the object when it is the published object
/// (advisory tombstone or supersession metadata aside), or when its payload
/// hash equals the published object's, so its verified bytes are identical
/// even at another location. With nothing published, a live publication is a
/// first upload of the indexed local slab, while staged committed authority
/// (bootstrap or snapshot recovery) is described by its own layout.
pub(super) fn tier_object_evidence<'a>(
	published: Option<&TieredSlabPointer>,
	pointer: &TieredSlabPointer,
	acknowledged: &'a BTreeSet<u64>,
	staged_authority: bool,
) -> TierObjectEvidence<'a> {
	match published {
		Some(published)
			if published.same_object_generation(pointer)
				|| (published.slab_id == pointer.slab_id
					&& published.payload_hash == pointer.payload_hash) =>
		{
			TierObjectEvidence::Indexed(acknowledged)
		}
		None if !staged_authority => TierObjectEvidence::Indexed(acknowledged),
		_ => TierObjectEvidence::Replacement,
	}
}

/// A tier object's plaintext with the record offsets that describe exactly
/// these bytes; every walk of the bytes uses these offsets.
pub(crate) struct TieredPlaintext {
	pub(crate) bytes: Vec<u8>,
	pub(crate) record_offsets: BTreeSet<u64>,
}

/// The tenant check walks the decrypted bytes with the generation's record
/// offsets (ADR 0099), so it can resume past a leading hole.
pub(super) fn decode_tiered_blob(
	crypto: &CryptoManager,
	pointer: &TieredSlabPointer,
	blob: &[u8],
	evidence: TierObjectEvidence<'_>,
) -> std::io::Result<TieredPlaintext> {
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
			let DecryptedTieredSlab {
				plaintext,
				record_offsets: object_offsets,
				..
			} = crypto.decrypt_tiered_slab(pointer.slab_id, blob)?;
			let record_offsets = match evidence {
				TierObjectEvidence::Indexed(acknowledged) => acknowledged.clone(),
				TierObjectEvidence::Replacement => object_offsets,
			};
			let payload_tenant_id =
				single_tenant_id_from_plain_slab(&plaintext, pointer.slab_id, &record_offsets)?;
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
			Ok(TieredPlaintext {
				bytes: plaintext,
				record_offsets,
			})
		}
		None => match evidence {
			TierObjectEvidence::Indexed(acknowledged) => Ok(TieredPlaintext {
				bytes: blob.to_vec(),
				record_offsets: acknowledged.clone(),
			}),
			TierObjectEvidence::Replacement => Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"replacement tier object for slab {} is plaintext: it carries no authenticated record layout",
					pointer.slab_id
				),
			)),
		},
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
	location: crate::storage::core::FrameLocation,
) -> std::io::Result<Vec<u8>> {
	extract_record_and_header_from_blob(blob, location).map(|(_, frame)| frame)
}

/// The record header and frame at `location` in a plaintext slab blob.
pub(super) fn extract_record_and_header_from_blob(
	blob: &[u8],
	location: crate::storage::core::FrameLocation,
) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
	let location = location.record();
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
	Ok((header, blob[offset + header_size..end].to_vec()))
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
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<TieredSlabObjectMetadata> {
	let entries = recover_slab_entries_from_blob(blob, slab_id, acknowledged)?;
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
		chunk_size_bytes: choose_tiered_object_chunk_size(
			blob,
			slab_id,
			chunk_layout_config,
			acknowledged,
		)?,
		offset_index,
		column_stats_block: None,
		has_row_tombstone_records,
	})
}

pub(super) fn choose_tiered_object_chunk_size(
	blob: &[u8],
	slab_id: SlabId,
	chunk_layout_config: &L2ChunkLayoutConfig,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<crate::schema::ChunkSizeBytes> {
	if !chunk_layout_config.enabled {
		return Ok(SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE);
	}
	let directories = encoding_directories_from_slab_blob(blob, slab_id, acknowledged)?;
	Ok(choose_chunk_size_bytes_for_directories(
		directories.iter(),
		chunk_layout_config,
	))
}

impl SlabManager {
	/// Encode a sealed slab into the ADR 022 L2 object layout.
	/// The current write path still assumes every sealed slab is
	/// single-tenant, so the footer and per-chunk AAD stay slab-scoped.
	/// `plaintext_blob` must be the indexed sealed slab itself: the index's
	/// offsets for the slab are its record evidence.
	pub fn encrypt_tiered_object_blob(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
	) -> std::io::Result<EncryptedTieredSlab> {
		let acknowledged = self.acknowledged_offsets_in_slab(slab_id);
		self.encrypt_tiered_blob_with_evidence(slab_id, plaintext_blob, &acknowledged, None)
	}

	/// As `encrypt_tiered_object_blob`, with column stats.
	pub fn encrypt_tiered_object_blob_with_table_stats(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
		table_schemas: &TableSchemaCatalog,
	) -> std::io::Result<EncryptedTieredSlab> {
		let acknowledged = self.acknowledged_offsets_in_slab(slab_id);
		self.encrypt_tiered_blob_with_evidence(
			slab_id,
			plaintext_blob,
			&acknowledged,
			Some(table_schemas),
		)
	}

	/// Every walk of `plaintext_blob` (offset index, chunk layout, column
	/// stats) uses `acknowledged`, the record offsets in exactly these bytes.
	/// A replacement written for the same slab id passes the offsets of its
	/// own appends: the index still describes the old layout.
	pub(crate) fn encrypt_tiered_blob_with_evidence(
		&self,
		slab_id: SlabId,
		plaintext_blob: &[u8],
		acknowledged: &BTreeSet<u64>,
		table_schemas: Option<&TableSchemaCatalog>,
	) -> std::io::Result<EncryptedTieredSlab> {
		self.require_tier_authority_ready()?;
		let mut metadata = build_tiered_object_metadata(
			plaintext_blob,
			slab_id,
			&self.config.l2_chunk_layout,
			acknowledged,
		)?;
		if let Some(table_schemas) = table_schemas {
			metadata.column_stats_block = self.build_tiered_column_stats_block(
				plaintext_blob,
				slab_id,
				metadata.tenant_id,
				table_schemas,
				acknowledged,
			)?;
		}
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
		self.apply_tier_slab_with_epoch(pointer, last_control_log_index, None)
			.await
	}

	/// Re-read control authority after acquiring the same slab lock as log
	/// observers. The discovery snapshot only selects work; it cannot authorize
	/// a pointer replacement or retirement. The callback drops its SM read guard
	/// before any filesystem work. Return whether local state changed and the
	/// retired pointer whose existing cleanup path must be completed.
	pub(crate) async fn reconcile_tier_slab_at_snapshot_epoch<F, Fut>(
		&self,
		slab_id: SlabId,
		observed_epoch: u64,
		authoritative: F,
	) -> std::io::Result<(bool, Option<TieredSlabPointer>)>
	where
		F: FnOnce() -> Fut,
		Fut: std::future::Future<Output = std::io::Result<Option<TieredSlabPointer>>>,
	{
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		let pointer = authoritative().await?;
		let local = self.tier_pointer(slab_id).await;
		let matching_deferral = pointer.as_ref().is_some_and(|pointer| {
			self.unapplied_tier_pointers
				.get(&slab_id)
				.is_some_and(|entry| entry.value().fences(pointer))
		});
		if pointer == local && !matching_deferral {
			return Ok((false, None));
		}
		match pointer {
			Some(pointer) => {
				if let Some(old) = local.as_ref().filter(|old| old.l2_uri != pointer.l2_uri) {
					self.record_retained_pointer_transition(old, Some(&pointer), true)
						.await?;
				}
				// A replayable fence of exactly this pointer is resolved by
				// replaying its own callback, as the authority worker does.
				let replayable = self
					.unapplied_tier_pointers
					.get(&slab_id)
					.is_some_and(|entry| {
						entry.value().fences(&pointer) && entry.value().replayable_index().is_some()
					});
				if replayable {
					return match self
						.replay_tier_deferral_under_publication_lock(
							slab_id,
							Some(&pointer),
							observed_epoch,
						)
						.await
					{
						TierReplay::Replayed(result) => result.map(|()| (true, None)),
						TierReplay::Superseded => Ok((true, None)),
						TierReplay::NotReplayable
						| TierReplay::Unchanged
						| TierReplay::KeptFenced => Ok((false, None)),
					};
				}
				self.apply_tier_slab_under_publication_lock(pointer, None, observed_epoch)
					.await?;
				Ok((true, None))
			}
			None => {
				let Some(old) = local else {
					return Ok((false, None));
				};
				self.record_retained_pointer_transition(&old, None, true)
					.await?;
				let changed = self
					.retire_tiered_slab_under_publication_lock(slab_id, None, observed_epoch)
					.await?;
				Ok((changed, changed.then_some(old)))
			}
		}
	}

	pub(crate) async fn apply_tier_slab_at_snapshot_epoch(
		&self,
		pointer: TieredSlabPointer,
		epoch: u64,
	) -> std::io::Result<()> {
		self.apply_tier_slab_with_epoch(pointer, None, Some(epoch))
			.await
	}

	async fn apply_tier_slab_with_epoch(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		observed_epoch: Option<u64>,
	) -> std::io::Result<()> {
		if last_control_log_index.is_none() {
			self.require_cleanup_authority()?;
		}
		// This path is intentionally idempotent for the same pointer. The
		// sweeper may fall back to calling it after Raft apply has already
		// fired the tier observer, so a second application of an identical
		// pointer must not rebuild row state or mutate relocated reads again.
		let control_epoch = observed_epoch.unwrap_or_else(|| self.capture_snapshot_epoch());
		let tier_lock = self.tier_publication_lock(pointer.slab_id);
		let _tier_guard = tier_lock.lock().await;
		self.apply_tier_slab_under_publication_lock(pointer, last_control_log_index, control_epoch)
			.await
	}

	/// What delivering a committed pointer needs from object storage, gathered with no
	/// authority held: object storage can stall, and a state machine must not wait on it.
	/// A slab with no rows has normally lost them, and they come back from its object. A retired
	/// run has none because compaction gave them to its successors: restoring them would add a
	/// second version of each key, naming a slab a reopen drops from its index. Every route that
	/// derives rows from an object asks this, publication and post-reopen verification alike.
	pub(super) fn run_gave_its_rows_to_successors(&self, slab_id: SlabId) -> bool {
		!self.has_rows_for_slab(slab_id)
			&& self
				.compaction_manifest_snapshot()
				.run(slab_id)
				.is_some_and(|run| !run.state.is_live())
	}

	/// Whether this delivery moves or restores rows. A run that gave its rows away publishes its
	/// pointer alone.
	fn tier_delivery_needs_row_plan(
		&self,
		pointer: &TieredSlabPointer,
		old_pointer: Option<&TieredSlabPointer>,
	) -> bool {
		if self.run_gave_its_rows_to_successors(pointer.slab_id) {
			return false;
		}
		old_pointer.is_some_and(|old| old != pointer) || !self.has_rows_for_slab(pointer.slab_id)
	}

	pub(super) async fn prepare_tier_delivery(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<PreparedTierDelivery> {
		let old_pointer = self.tier_pointer(pointer.slab_id).await;
		let plan = if self.tier_delivery_needs_row_plan(pointer, old_pointer.as_ref()) {
			Some(
				self.build_tiered_row_refresh_plan(pointer, old_pointer.as_ref())
					.await?,
			)
		} else {
			None
		};
		Ok(PreparedTierDelivery { old_pointer, plan })
	}

	/// Publish `pointer` from what was prepared. Local files and redb only: if the published
	/// generation moved since it was prepared, this refuses rather than going to fetch.
	pub(super) async fn apply_prepared_tier_delivery(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		prepared: PreparedTierDelivery,
	) -> std::io::Result<()> {
		let control_epoch = self.capture_snapshot_epoch();
		let tier_lock = self.tier_publication_lock(pointer.slab_id);
		let _tier_guard = tier_lock.lock().await;
		self.apply_tier_slab_under_publication_lock_prepared(
			pointer,
			last_control_log_index,
			control_epoch,
			false,
			Some(prepared),
		)
		.await
	}

	/// Caller owns this slab's tier publication lock, including authority comparison.
	async fn apply_tier_slab_under_publication_lock(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		control_epoch: u64,
	) -> std::io::Result<()> {
		self.apply_tier_slab_under_publication_lock_as(
			pointer,
			last_control_log_index,
			control_epoch,
			false,
		)
		.await
	}

	/// `apply_tier_slab_under_publication_lock`. A `replay` of a deferred
	/// `Some(index)` callback (`tier_deferral`) also requires, after every
	/// wait that precedes its publication, that `control_epoch` is current
	/// and that the committed pointer still is `pointer`; otherwise it refuses
	/// typed and leaves the deferral exactly as it was.
	pub(super) async fn apply_tier_slab_under_publication_lock_as(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		control_epoch: u64,
		replay: bool,
	) -> std::io::Result<()> {
		self.apply_tier_slab_under_publication_lock_prepared(
			pointer,
			last_control_log_index,
			control_epoch,
			replay,
			None,
		)
		.await
	}

	async fn apply_tier_slab_under_publication_lock_prepared(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		control_epoch: u64,
		replay: bool,
		prepared: Option<PreparedTierDelivery>,
	) -> std::io::Result<()> {
		if last_control_log_index.is_none() {
			self.require_cleanup_authority()?;
		}
		if let Some(error) = self.tier_authority.integrity_error() {
			return Err(error);
		}
		let _cleanup_guard = self.cleanup_claim(pointer.slab_id).lock_owned().await;
		#[cfg(test)]
		self.tier_observer_before_gate_pause_point().await;
		let gate = match tokio::time::timeout(
			Duration::from_secs(30),
			self.persistent_index_gate.clone().read_owned(),
		)
		.await
		{
			Ok(gate) => Arc::new(gate),
			Err(_) => {
				self.record_tier_deferral(
					pointer.slab_id,
					TierDeferral::fenced(
						pointer.clone(),
						FenceCause::IndexGate,
						last_control_log_index,
					),
				);
				return Err(fenced_error(
					ErrorKind::Other,
					pointer.slab_id,
					FenceCause::IndexGate,
				));
			}
		};
		if self.observer_is_superseded(last_control_log_index) {
			return Ok(());
		}
		if (last_control_log_index.is_none() || replay)
			&& !self.snapshot_epoch_is_current(control_epoch)
		{
			// A newer comparison owns this generation. Refusing this obsolete
			// local completion must not install committed-pointer authority.
			return Err(stale_location_error(
				pointer.slab_id,
				StaleLocationReason::Fenced,
			));
		}
		if replay && !self.canonical_tier_pointer_is(&pointer) {
			// A newer committed state owns the slab; its own callback resolves it.
			return Err(stale_location_error(
				pointer.slab_id,
				StaleLocationReason::Fenced,
			));
		}
		if self.snapshot_install_in_flight() {
			let own_dirty = self
				.pending_index_publications
				.iter()
				.any(|entry| entry.key().0 == pointer.slab_id);
			if last_control_log_index.is_some()
				&& !self.retired_tier_slabs.contains(&pointer.slab_id)
				&& !self.index_authority_unavailable.load(Ordering::Acquire)
				&& !own_dirty
				&& !self.lsm_intent_names_slab(pointer.slab_id)
				&& !self.lsm_intent_keys_unknown()
				&& !(self.tier_authority.pending.contains_key(&pointer.slab_id)
					&& !self.lsm_install_unresolved.is_empty())
			{
				let floor = pointer.slab_id;
				return self
					.stage_tier_authority_under_publication_lock(
						pointer,
						last_control_log_index,
						floor,
						true,
					)
					.await;
			}
			self.record_tier_deferral(
				pointer.slab_id,
				TierDeferral::fenced(
					pointer.clone(),
					FenceCause::SnapshotInstall,
					last_control_log_index,
				),
			);
			return Err(fenced_error(
				ErrorKind::Other,
				pointer.slab_id,
				FenceCause::SnapshotInstall,
			));
		}
		if self.retired_tier_slabs.contains(&pointer.slab_id) {
			tracing::warn!(
				slab_id = pointer.slab_id.get(),
				"tier publication for a slab retired in this process is stale; ignored"
			);
			crate::metrics::global().observe_tier_rewrite_refusal(
				crate::metrics::TierRewriteRefusalReason::SlabRetired,
			);
			return Ok(());
		}
		self.bump_slab_publication_revision(pointer.slab_id)?;
		if self.lsm_intent_names_slab(pointer.slab_id)
			|| self.lsm_intent_keys_unknown()
			|| (self.tier_authority.pending.contains_key(&pointer.slab_id)
				&& !self.lsm_install_unresolved.is_empty())
		{
			let error = fenced_error(ErrorKind::Other, pointer.slab_id, FenceCause::LsmScope);
			self.record_tier_deferral(
				pointer.slab_id,
				TierDeferral::fenced(pointer, FenceCause::LsmScope, last_control_log_index),
			);
			return Err(error);
		}
		if self.is_awaiting_cluster() && last_control_log_index.is_some() {
			let own_dirty = self
				.pending_index_publications
				.iter()
				.any(|entry| entry.key().0 == pointer.slab_id);
			let incompatible_refusal = self
				.unapplied_tier_pointers
				.get(&pointer.slab_id)
				.is_some_and(|entry| !entry.value().fences(&pointer));
			if self.index_authority_unavailable.load(Ordering::Acquire)
				|| own_dirty || incompatible_refusal
			{
				return Err(stale_location_error(
					pointer.slab_id,
					StaleLocationReason::Fenced,
				));
			}
			// Replay registers committed authority while the replacement marker
			// still closes all data answers. Only the complete confirmed prefix
			// may clear that marker; registration performs no physical publication.
			let floor = pointer.slab_id;
			return self
				.stage_tier_authority_under_publication_lock(
					pointer,
					last_control_log_index,
					floor,
					false,
				)
				.await;
		}
		if self.tier_authority.pending.contains_key(&pointer.slab_id) {
			if last_control_log_index.is_none() {
				// Local sweep/reconcile callers report completed publication, not
				// registration. A matching committed flight may still resume.
				self.resume_matching_tier_deferral(&pointer);
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"tier verification is pending",
				));
			}
			let result =
				self.update_pending_tier_authority(pointer.clone(), last_control_log_index);
			if result.is_ok() {
				self.resume_matching_tier_deferral(&pointer);
			}
			return result;
		}
		let old_pointer = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&pointer.slab_id)
				.map(|entry| entry.value().clone())
		};
		// Structural refusals remain typed and cached. A Fenced dependency
		// can finish, so a retry must recheck all normal publication guards.
		if let Some(entry) = self.unapplied_tier_pointers.get(&pointer.slab_id)
			&& entry.value().pointer == pointer
			&& let DeferralOutcome::Refused(reason) = entry.value().outcome
		{
			return Err(stale_location_error(pointer.slab_id, reason));
		}
		let already_published = old_pointer.as_ref() == Some(&pointer);
		let result = match self.require_private_tier_apply_authority(pointer.slab_id) {
			Ok(()) => {
				crate::metrics::global().tier_observer_publications.inc();
				self.publish_committed_tier_pointer(
					&pointer,
					old_pointer,
					last_control_log_index,
					gate,
					prepared,
				)
				.await
			}
			Err(error) => Err(error),
		};
		match &result {
			Ok(()) => {
				if let Some(index) = last_control_log_index {
					self.tier_authority
						.cancel_snapshot_retirement_before_control(pointer.slab_id, index);
					if self.require_cleanup_authority().is_ok() {
						self.note_tier_republished(pointer.slab_id, &pointer.l2_uri, index);
					}
				}
				self.unapplied_tier_pointers.remove(&pointer.slab_id);
			}
			Err(_) if already_published => {}
			Err(error) => {
				self.record_tier_deferral(
					pointer.slab_id,
					TierDeferral {
						pointer: pointer.clone(),
						outcome: DeferralOutcome::of(error),
						provenance: last_control_log_index,
					},
				);
			}
		}
		result
	}

	/// The committed pointer this node has not published, if any. A proposer
	/// never plans from a generation the cluster has moved past.
	pub(crate) fn unapplied_tier_pointer(&self, slab_id: SlabId) -> Option<TieredSlabPointer> {
		self.unapplied_tier_pointers
			.get(&slab_id)
			.map(|entry| entry.value().pointer.clone())
	}

	/// Caller holds the LSM scope read gate through this check and publication.
	pub(super) async fn check_tier_publication_lsm_scope(
		&self,
		slab_id: SlabId,
		touched: &[PersistentRecordRow],
	) -> std::io::Result<()> {
		if self.lsm_intent_names_slab(slab_id)
			|| self.lsm_intent_keys_unknown()
			|| (self.tier_authority.pending.contains_key(&slab_id)
				&& !self.lsm_install_unresolved.is_empty())
		{
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		// Named GC inputs may contain physical keys no longer in the aggregate
		// index. Tier-only, missing, or counter-bearing sources cannot cheaply
		// establish complete key disjointness and are refused conservatively.
		let mut named_slabs = HashSet::new();
		for intent in &self.lsm_install_unresolved {
			if intent.value().slabs.is_empty() || intent.value().keys_unknown {
				return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
			}
			named_slabs.extend(intent.value().slabs.iter().copied());
		}
		let sources = if named_slabs.is_empty() {
			Vec::new()
		} else {
			let state = self.state.lock().await;
			let mut sources = Vec::with_capacity(named_slabs.len());
			for slab_id in named_slabs {
				if state.tiered.contains_key(&slab_id) {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				}
				let Some(slab) = state
					.sealed
					.get(&slab_id)
					.map(|entry| entry.value().clone())
				else {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				};
				if slab.refuse_counter_maintenance().is_err() || !slab.manager_appends_quiescent() {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				}
				sources.push(slab);
			}
			sources
		};
		// A private in-memory index may omit a rolled-back physical frame.
		// Prove the complete physical key set from bounded owned-file reads;
		// incomplete/corrupt/large evidence is refused, never treated as empty.
		const MAX_SCOPE_BYTES: usize = 8 * 1024 * 1024;
		const MAX_SCOPE_SLABS: usize = 32;
		if sources.len() > MAX_SCOPE_SLABS {
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		let mut remaining = MAX_SCOPE_BYTES;
		let mut named_keys = HashSet::new();
		for slab in sources {
			let blob = slab
				.read_scope_blob(remaining)
				.await
				.map_err(|_| stale_location_error(slab_id, StaleLocationReason::Fenced))?;
			remaining -= blob.len();
			let entries =
				recover_slab_entries_from_blob(&blob, slab.slab_id(), &BTreeSet::new())
					.map_err(|_| stale_location_error(slab_id, StaleLocationReason::Fenced))?;
			let mut frames = entries
				.iter()
				.map(|entry| (entry.location.offset, entry.location.length))
				.collect::<Vec<_>>();
			frames.sort_unstable();
			frames.dedup();
			let mut cursor = 0usize;
			for (offset, length) in frames {
				if usize::try_from(offset).ok() != Some(cursor) {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				}
				let Some(padded) = (length as usize)
					.checked_add(DIRECT_IO_ALIGN - 1)
					.map(|value| value & !(DIRECT_IO_ALIGN - 1))
				else {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				};
				let Some(next) = cursor.checked_add(padded) else {
					return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
				};
				cursor = next;
			}
			if cursor != blob.len() {
				return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
			}
			named_keys.extend(
				entries
					.into_iter()
					.map(|entry| (entry.tenant_id, entry.key_identity)),
			);
		}
		for row in touched {
			let key = (row.tenant_id, row.key_identity);
			if named_keys.contains(&key)
				|| self.key_index.get(&key).is_some_and(|locations| {
					locations
						.iter()
						.any(|location| self.lsm_intent_names_slab(location.slab_id))
				}) {
				return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
			}
		}
		Ok(())
	}

	async fn publish_committed_tier_pointer(
		&self,
		pointer: &TieredSlabPointer,
		old_pointer: Option<TieredSlabPointer>,
		last_control_log_index: Option<u64>,
		gate: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
		prepared: Option<PreparedTierDelivery>,
	) -> std::io::Result<()> {
		let pointer = pointer.clone();
		let needs_plan = self.tier_delivery_needs_row_plan(&pointer, old_pointer.as_ref());
		let refresh_plan = match prepared {
			Some(prepared) => {
				if prepared.old_pointer != old_pointer || prepared.plan.is_some() != needs_plan {
					return Err(Error::new(
						ErrorKind::WouldBlock,
						"the published tier generation moved since this delivery was prepared",
					));
				}
				prepared.plan
			}
			None if needs_plan => Some(
				self.build_tiered_row_refresh_plan(&pointer, old_pointer.as_ref())
					.await
					.map_err(|error| self.observed_publication_error(pointer.slab_id, error))?,
			),
			None => None,
		};
		#[cfg(debug_assertions)]
		if old_pointer.as_ref().is_some_and(|old| old == &pointer) {
			debug_assert!(
				refresh_plan.is_none(),
				"identical tier pointer replay must remain a no-op; the sweeper may reapply after the observer already committed local state"
			);
		}
		// The next generation's history is built before anything is published;
		// a layout that would make an old location name another version is
		// refused before the pointer or any row changes.
		let next_layout = match &refresh_plan {
			Some(plan) => {
				let (current, base) = {
					let state = self.state.lock().await;
					(
						state
							.tier_layouts
							.get(&pointer.slab_id)
							.map(|layout| layout.value().clone()),
						self.tier_publication_base(&state, pointer.slab_id),
					)
				};
				let (layout, token) = self.build_observed_tier_layout(
					&current.unwrap_or_default(),
					&pointer,
					&base,
					plan,
					last_control_log_index.is_some(),
				)?;
				Some((Arc::new(layout), token))
			}
			None => None,
		};
		let (next_layout, applied_token) = match next_layout {
			Some((layout, token)) => (Some(layout), token),
			None => (None, None),
		};
		// Every covered allocation of the row refresh happens here, before the
		// durable boundary; after it the refresh only applies.
		let prepared_refresh = match &refresh_plan {
			Some(plan) => Some(
				self.prepare_tiered_row_refresh(plan)
					.map_err(|error| self.observed_publication_error(pointer.slab_id, error))?,
			),
			None => None,
		};
		let row_copies = match &refresh_plan {
			Some(plan) => Some(
				plan.try_copy_rows(pointer.slab_id)
					.map_err(|error| self.observed_publication_error(pointer.slab_id, error))?,
			),
			None => None,
		};
		// Hold the scope guard from the final key check through publication, so
		// a new LSM intent cannot appear between the check and key-chain edits.
		let _scope = self.lsm_intent_scope_gate.read().await;
		self.require_private_tier_apply_authority(pointer.slab_id)?;
		let touched = match &refresh_plan {
			Some(plan) => plan
				.try_touched_rows(pointer.slab_id)
				.map_err(|error| self.observed_publication_error(pointer.slab_id, error))?,
			None => self.record_rows_for_slab_for_recovery(pointer.slab_id),
		};
		self.check_tier_publication_lsm_scope(pointer.slab_id, &touched)
			.await?;
		// The pointer and its row delta become durable in one transaction, so a
		// crash can never leave the new pointer with the previous rows.
		let publication = self.begin_index_publication_with_gate(vec![pointer.slab_id], gate)?;
		match (&refresh_plan, row_copies, &self.persistent_index) {
			(Some(plan), Some((delete_records, put_records)), Some(index)) => {
				index
					.apply_with_publication_guard(
						PersistentIndexUpdate {
							put_tiered_slabs: vec![pointer.clone()],
							tier_republication_index: last_control_log_index,
							delete_records,
							put_records,
							retained_charge: plan.scratch.retain(),
							last_control_log_index,
							..Default::default()
						},
						Some(publication.gate.clone()),
					)
					.await?;
			}
			_ => {
				self.persist_tiered_pointer_guarded(
					pointer.clone(),
					last_control_log_index,
					last_control_log_index,
					&publication,
				)
				.await?;
			}
		}
		if old_pointer
			.as_ref()
			.map(|old| old != &pointer)
			.unwrap_or(true)
		{
			self.clear_tiered_stats_cache_for_slab(pointer.slab_id);
		}
		#[cfg(test)]
		self.tier_publication_pause_point().await;
		{
			// The pointer and its generation history are published together.
			let mut state = self.state.lock().await;
			let released_sealed = state.sealed.remove(&pointer.slab_id).is_some();
			let released_copy = state.rehydrated.remove(&pointer.slab_id).is_some();
			if released_sealed || released_copy {
				self.release_slab_file(pointer.slab_id);
			}
			state.install_tiered(&self.tier_budget, pointer.slab_id, pointer.clone());
			if let Some(layout) = next_layout {
				state.install_tier_layout(&self.tier_budget, pointer.slab_id, layout);
			}
			state.hot_read_counts.remove(&pointer.slab_id);
			state.rehydrating.remove(&pointer.slab_id);
		}
		// The applied entry is installed: the proposal its token reserved for
		// is resolved. Any earlier failure or cancellation left the token held.
		if let Some(token) = applied_token {
			self.tier_tokens.consume(token);
		}
		self.retired_tiered_cleanup.remove(&pointer.slab_id);
		if let (Some(plan), Some(prepared)) = (&refresh_plan, prepared_refresh) {
			self.apply_prepared_tiered_row_refresh(plan, prepared, false)
				.await;
		}

		publication.complete();

		// Unlink the L1 file this process released (the sealed slab or its
		// own copy), never a file it does not own. Open fds held by in-flight
		// reads continue to work; new reads find the entry in `tiered` and go
		// to L2.
		self.unlink_released_slab_file(pointer.slab_id).await?;
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
		self.retire_tiered_slab_with_epoch(slab_id, last_control_log_index, None)
			.await
	}

	pub(crate) async fn retire_tiered_slab_at_snapshot_epoch(
		&self,
		slab_id: SlabId,
		epoch: u64,
	) -> std::io::Result<bool> {
		self.retire_tiered_slab_with_epoch(slab_id, None, Some(epoch))
			.await
	}

	/// Snapshot and indexed retirement take the same private row snapshot.
	fn record_rows_for_tier_retirement(&self, slab_id: SlabId) -> Vec<PersistentRecordRow> {
		self.record_rows_for_slab_for_recovery(slab_id)
	}

	async fn retire_tiered_slab_with_epoch(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
		observed_epoch: Option<u64>,
	) -> std::io::Result<bool> {
		if last_control_log_index.is_none() {
			self.require_cleanup_authority()?;
		}
		let control_epoch = observed_epoch.unwrap_or_else(|| self.capture_snapshot_epoch());
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		self.retire_tiered_slab_under_publication_lock(
			slab_id,
			last_control_log_index,
			control_epoch,
		)
		.await
	}

	/// Caller owns this slab's tier publication lock, including authority comparison.
	async fn retire_tiered_slab_under_publication_lock(
		&self,
		slab_id: SlabId,
		last_control_log_index: Option<u64>,
		control_epoch: u64,
	) -> std::io::Result<bool> {
		if last_control_log_index.is_none() {
			self.require_cleanup_authority()?;
		}
		let _cleanup_guard = self.cleanup_claim(slab_id).lock_owned().await;
		let gate = match tokio::time::timeout(
			Duration::from_secs(30),
			self.persistent_index_gate.clone().read_owned(),
		)
		.await
		{
			Ok(gate) => Arc::new(gate),
			Err(_) => {
				// The read gate cannot be acquired, so pointer removal is not yet
				// safe. Persist only this slab's intent, without inventing a complete
				// snapshot set. Reopen restores the same scoped read fence.
				if !self.observer_is_superseded(last_control_log_index)
					&& self.snapshot_epoch_is_current(control_epoch)
				{
					let pointer = self
						.state
						.lock()
						.await
						.tiered
						.get(&slab_id)
						.map(|entry| entry.value().clone());
					if let Some(pointer) = pointer {
						let mark = crate::storage::index::SnapshotRetirementMark {
							epoch: control_epoch,
							snapshot_index: last_control_log_index,
							pointer,
						};
						self.tier_authority.mark_snapshot_retirement(
							slab_id,
							control_epoch,
							last_control_log_index,
						);
						if let Some(index) = &self.persistent_index {
							match index
								.stage_deferred_retirement(
									mark,
									self.snapshot_epoch.clone(),
									self.snapshot_epoch_gate.clone(),
								)
								.await
							{
								Ok(Some(mark)) => self.tier_authority.mark_snapshot_retirement(
									slab_id,
									mark.epoch,
									mark.snapshot_index,
								),
								Ok(None) => self
									.tier_authority
									.clear_snapshot_retirement(slab_id, control_epoch), // A newer comparison owns this pointer.
								Err(error) => {
									return Err(error);
								}
							}
						} else {
							self.fence_unapplied_tier_retirement(slab_id, last_control_log_index);
						}
					}
				}
				return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
			}
		};
		if self.observer_is_superseded(last_control_log_index) {
			return Ok(false);
		}
		if last_control_log_index.is_none() && !self.snapshot_epoch_is_current(control_epoch) {
			// The newer snapshot owns this decision. An obsolete local completion
			// must not create or overwrite its retirement fence.
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		self.require_private_retirement_authority(slab_id, last_control_log_index.is_some())?;
		let rows = self.record_rows_for_tier_retirement(slab_id);
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
			// A current callback for a retirement already applied: all that can be left is a run
			// from before retirements disposed of theirs, and only its evidence authorizes that.
			if last_control_log_index.is_some()
				&& let Some(index) = &self.persistent_index
				&& self
					.compaction_manifest_snapshot()
					.run(slab_id)
					.is_some_and(|run| run.state.is_live())
			{
				let publication = self.begin_index_publication_with_gate(vec![slab_id], gate)?;
				if index
					.dispose_retirement_remnant(slab_id, publication.gate.clone())
					.await?
				{
					self.drop_live_run(slab_id);
				}
				publication.complete();
			}
			return Ok(false);
		}
		self.bump_slab_publication_revision(slab_id)?;
		if let Some(pointer) = retired_pointer.clone() {
			self.retired_tiered_cleanup.insert(slab_id, pointer);
		}
		let publication = self
			.delete_tiered_slab_strict_with_gate(slab_id, last_control_log_index, gate)
			.await?;
		{
			let mut state = self.state.lock().await;
			let released_sealed = state.sealed.remove(&slab_id).is_some();
			let released_copy = state.rehydrated.remove(&slab_id).is_some();
			if released_sealed || released_copy {
				self.release_slab_file(slab_id);
			}
			let _old = state.tiered.remove(&slab_id);
			let _old = state.remove_tier_layout(&self.tier_budget, slab_id);
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
		}
		self.drop_retired_live_run(slab_id).await;
		self.retired_tier_slabs.insert(slab_id);
		self.unapplied_tier_pointers.remove(&slab_id);
		for row in &rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		self.redirects.remove_touching(&self.tier_budget, slab_id);
		self.record_disposal_evidence(slab_id).await;
		self.clear_tiered_stats_cache_for_slab(slab_id);
		if let Some(publication) = publication {
			publication.complete();
		}
		self.unlink_released_slab_file(slab_id).await?;
		Ok(true)
	}

	/// Memory's half of a retirement's run disposal. A run with no durable row, as a sealed L0 may
	/// be, goes too. An unresolved install's run waits, as in the durable half; an index that
	/// cannot say leaves it waiting too, and a later plan or reopen finishes it.
	async fn drop_retired_live_run(&self, slab_id: SlabId) {
		if let Some(index) = &self.persistent_index {
			match index.install_intent_names(slab_id).await {
				Ok(false) => {}
				Ok(true) => return,
				Err(error) => {
					tracing::warn!(%error, slab_id = slab_id.get(), "retired run left Live");
					return;
				}
			}
		}
		self.drop_live_run(slab_id);
	}

	/// Any state but Live keeps its own lifecycle, as in the durable half.
	pub(super) fn drop_live_run(&self, slab_id: SlabId) {
		let mut manifest = recover_rwlock_write(&self.compaction_manifest, "compaction_manifest");
		if !manifest
			.get_run(slab_id)
			.is_some_and(|run| run.state.is_live())
		{
			return;
		}
		manifest.remove_run(slab_id);
		let snapshot = manifest.snapshot();
		drop(manifest);
		refresh_lsm_level_metrics(&snapshot);
	}

	/// Apply a snapshot-authorized retirement, or leave it for a newer snapshot.
	pub(super) async fn apply_snapshot_retirement(
		&self,
		mark: crate::storage::index::SnapshotRetirementMark,
	) -> std::io::Result<bool> {
		let slab_id = mark.pointer.slab_id;
		let tier_lock = self.tier_publication_lock(slab_id);
		let _tier_guard = tier_lock.lock().await;
		let _cleanup_guard = self.cleanup_claim(slab_id).lock_owned().await;
		let gate = Arc::new(self.persistent_index_gate.clone().read_owned().await);
		let local = self
			.state
			.lock()
			.await
			.tiered
			.get(&slab_id)
			.map(|entry| entry.value().clone());
		let matching_pending = self
			.tier_authority
			.pending
			.get(&slab_id)
			.is_some_and(|entry| entry.pointer.same_object_generation(&mark.pointer));
		match local.as_ref() {
			Some(pointer) if !pointer.same_object_generation(&mark.pointer) => return Ok(false),
			None if !matching_pending => return Ok(false),
			_ => {}
		}
		if self
			.tier_authority
			.pending
			.get(&slab_id)
			.is_some_and(|pending| {
				!pending
					.value()
					.pointer
					.same_object_generation(&mark.pointer)
			}) {
			return Ok(false);
		}
		// A fenced deferral of another object generation from a callback newer
		// than this mark is newer committed authority, exactly as a pending
		// entry is: decline, so its replay publishes and supersedes the mark
		// durably. This only declines a destructive step; it cancels nothing.
		if self
			.unapplied_tier_pointers
			.get(&slab_id)
			.is_some_and(|entry| {
				let deferral = entry.value();
				deferral.is_fenced()
					&& !deferral.pointer.same_object_generation(&mark.pointer)
					&& deferral
						.provenance
						.is_some_and(|index| mark.snapshot_index.is_none_or(|mark| index > mark))
			}) {
			return Ok(false);
		}
		let rows = self.record_rows_for_tier_retirement(slab_id);
		let index = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for snapshot retirement",
			)
		})?;
		let known_bytes = self.snapshot_retained_size_hint(slab_id, &mark.pointer.l2_uri);
		let evidence = self.snapshot_retirement_evidence(slab_id, &mark.pointer.l2_uri);
		let Some((retained, evidence)) = index
			.apply_snapshot_retirement(
				mark.clone(),
				known_bytes,
				evidence,
				self.snapshot_epoch.clone(),
				self.snapshot_epoch_gate.clone(),
				gate.clone(),
			)
			.await?
		else {
			return Ok(false);
		};
		{
			let mut state = self.state.lock().await;
			let released_sealed = state.sealed.remove(&slab_id).is_some();
			let released_copy = state.rehydrated.remove(&slab_id).is_some();
			if released_sealed || released_copy {
				self.release_slab_file(slab_id);
			}
			state.tiered.remove(&slab_id);
			state.remove_tier_layout(&self.tier_budget, slab_id);
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
		}
		self.drop_retired_live_run(slab_id).await;
		self.retired_tier_slabs.insert(slab_id);
		self.unapplied_tier_pointers.remove(&slab_id);
		self.unverified_tier_slabs.remove(&slab_id);
		self.tier_authority
			.pending
			.remove_if(&slab_id, |_, pending| {
				pending.pointer.same_object_generation(&mark.pointer)
			});
		for row in &rows {
			self.rollback_indexed_location(row.tenant_id, row.key_identity, row.location);
		}
		self.redirects.remove_touching(&self.tier_budget, slab_id);
		self.record_disposal_evidence(slab_id).await;
		self.clear_tiered_stats_cache_for_slab(slab_id);
		if let Some(intent) = retained {
			self.note_snapshot_retirement_applied(
				slab_id,
				mark.pointer.l2_uri.clone(),
				intent,
				evidence,
			);
		}
		self.tier_authority
			.clear_snapshot_retirement(slab_id, mark.epoch);
		self.retired_tiered_cleanup
			.insert(slab_id, mark.pointer.clone());
		self.unlink_released_slab_file(slab_id).await?;
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
	/// A surviving metadata operation requires exact reads until resolved.
	pub(crate) fn pending_metadata_requires_exact(&self, slab_id: SlabId) -> bool {
		self.pending_metadata_tokens.contains_key(&slab_id)
	}

	/// A pending metadata operation requires the aggregate's exact row path.
	pub(crate) async fn tier_pointer_for_aggregate(
		&self,
		slab_id: SlabId,
	) -> Option<TieredSlabPointer> {
		let pointer = self.tier_pointer(slab_id).await?;
		(!self.pending_metadata_requires_exact(slab_id)).then_some(pointer)
	}

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
			let metadata = l2.head(&pointer.l2_uri).await.inspect_err(|error| {
				tracing::warn!(slab_id = pointer.slab_id.get(), l2_uri = %pointer.l2_uri, %error, "restore tier reference is unavailable");
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

	/// Retry only the local-file cleanup for a retired tiered slab: an unlink
	/// the retirement released and did not finish. The candidate alone never
	/// authorizes removing whatever file is at the slab's path.
	pub async fn retry_retired_tiered_local_cleanup(&self, slab_id: SlabId) -> std::io::Result<()> {
		self.require_cleanup_authority()?;
		self.unlink_released_slab_file(slab_id).await
	}

	/// Mark one retired tiered slab cleanup candidate complete.
	pub fn clear_retired_tiered_cleanup(&self, slab_id: SlabId) {
		self.retired_tiered_cleanup.remove(&slab_id);
	}

	/// Best-effort reclaim of one rehydrated local cache copy. The
	/// authoritative L2 pointer remains intact; only the temporary
	/// L1 file is removed. An earlier eviction that released its copy but did
	/// not finish the unlink is finished here; only a copy this process
	/// installed is ever removed.
	pub async fn drop_rehydrated_slab(&self, slab_id: SlabId) -> std::io::Result<bool> {
		let removed = {
			let state = self.state.lock().await;
			if state.rehydrating.contains(&slab_id) {
				return Ok(false);
			}
			let removed = state.rehydrated.remove(&slab_id).is_some();
			if removed {
				self.release_slab_file(slab_id);
			}
			removed
		};
		#[cfg(test)]
		self.rehydrated_copy_eviction_pause_point().await;
		self.unlink_released_slab_file(slab_id).await?;
		Ok(removed)
	}

	/// The plaintext of `pointer`'s object, which must be the generation the
	/// index describes (see `TierObjectEvidence::Indexed`).
	pub(super) async fn tiered_plaintext_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let acknowledged = self.acknowledged_offsets_in_slab(pointer.slab_id);
		Ok(self
			.tiered_object_from_l2(pointer, TierObjectEvidence::Indexed(&acknowledged))
			.await?
			.bytes)
	}

	/// `pointer`'s object decoded as the stated generation.
	pub(super) async fn tiered_object_from_l2(
		&self,
		pointer: &TieredSlabPointer,
		evidence: TierObjectEvidence<'_>,
	) -> std::io::Result<TieredPlaintext> {
		let l2 = self.config.l2.clone().ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!(
					"cannot recover tiered slab {} without an L2 object store",
					pointer.slab_id
				),
			)
		})?;
		let blob = l2
			.get(&pointer.l2_uri)
			.await
			.map_err(super::tier_deferral::l2_unavailable)?;
		validate_tiered_blob(pointer, &blob)?;
		decode_tiered_blob(self.config.crypto.as_ref(), pointer, &blob, evidence)
	}

	#[cfg(test)]
	pub(super) async fn load_tiered_rows_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		// As publication decides.
		let acknowledged = self.acknowledged_offsets_in_slab(pointer.slab_id);
		let published = self.tier_pointer(pointer.slab_id).await;
		let evidence = tier_object_evidence(published.as_ref(), pointer, &acknowledged, false);
		let plaintext = self.tiered_object_from_l2(pointer, evidence).await?;
		self.tiered_rows_from_plaintext_with_evidence(
			&plaintext.bytes,
			pointer.slab_id,
			&plaintext.record_offsets,
		)
	}

	/// The rows in `plaintext`, walked with `acknowledged`, the record offsets
	/// in exactly these bytes. Unbounded: production plans use `tiered_rows_within`.
	#[cfg(test)]
	pub(super) fn tiered_rows_from_plaintext_with_evidence(
		&self,
		plaintext: &[u8],
		slab_id: SlabId,
		acknowledged: &BTreeSet<u64>,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		let entries = recover_slab_entries_from_blob(plaintext, slab_id, acknowledged)?;
		let mut rows = Vec::with_capacity(entries.len());
		for frame in crate::storage::core::recovered_frames(&entries) {
			self.persistent_rows_from_recovered_frame(plaintext, frame, &mut rows)?;
		}
		Ok(rows)
	}

	/// The index rows of one recovered frame of `plaintext`, decoded once. This runs on the Raft
	/// apply path. Core recovery already classified synthetic rows: opaque blob bytes must never be
	/// probed as an Arrow length-prefixed stream, so only a trusted row asks for the decode.
	fn persistent_rows_from_recovered_frame(
		&self,
		plaintext: &[u8],
		frame: crate::storage::core::RecoveredFrame<'_>,
		rows: &mut Vec<PersistentRecordRow>,
	) -> std::io::Result<()> {
		let bytes = extract_record_from_blob(plaintext, frame.location())?;
		let batch = if frame
			.entries()
			.iter()
			.any(|entry| entry.routing_locality_trusted)
		{
			Some(decode_record_batch(&bytes)?)
		} else {
			None
		};
		for entry in frame.entries().iter().copied() {
			let table_object_id = match &batch {
				Some(batch) if entry.routing_locality_trusted => {
					storage_row_table_object_id_at(batch, entry.location.record_row_index)?
				}
				_ => None,
			};
			rows.push(PersistentRecordRow {
				tenant_id: entry.tenant_id,
				key_identity: entry.key_identity,
				routing_locality: self.recovered_entry_routing_locality(&entry),
				table_object_id,
				// ADR 0090: recovery read the row's placement from the frame;
				// re-tiering must not downgrade it.
				placement: entry.location.placement,
				partition: entry.location.partition,
				location: entry.location,
			});
		}
		Ok(())
	}

	/// The verifier already fetched and authenticated this object without
	/// retaining the manager over remote IO. Its private row snapshot is not
	/// an answer or a maintenance emptiness proof.
	pub(super) fn build_tiered_row_refresh_plan_from_plaintext(
		&self,
		pointer: &TieredSlabPointer,
		plaintext: &TieredPlaintext,
	) -> std::io::Result<TieredRowRefreshPlan> {
		let scratch = self.reserve_tier_plan_scratch(
			pointer.slab_id,
			&plaintext.bytes,
			&plaintext.record_offsets,
		)?;
		let new_rows = if self.run_gave_its_rows_to_successors(pointer.slab_id) {
			Vec::new()
		} else {
			self.tiered_rows_within(
				&plaintext.bytes,
				pointer.slab_id,
				&plaintext.record_offsets,
				&scratch,
			)?
		};
		let old_rows = self.record_rows_for_slab_for_recovery_within(pointer.slab_id, &scratch)?;
		let relocated_reads = self.pair_tier_rows(
			pointer.slab_id,
			&old_rows,
			&new_rows,
			None,
			&plaintext.bytes,
		)?;
		Ok(TieredRowRefreshPlan {
			pointer: Some(pointer.clone()),
			old_rows,
			new_rows,
			relocated_reads,
			scratch: scratch.charge,
		})
	}

	/// Reserve, before any of it is allocated, the memory a publication plan
	/// over `plaintext` needs (ADR 0692 §7): the bounded recovery walk, the
	/// new and old row vectors with the copies made of them for the index
	/// transaction and the apply, and the pairing scratch. The counts come
	/// from the count-only walk and a count over the index.
	pub(super) fn reserve_tier_plan_scratch(
		&self,
		slab_id: SlabId,
		plaintext: &[u8],
		acknowledged: &BTreeSet<u64>,
	) -> std::io::Result<TierPlanScratch> {
		use super::tier_budget::{
			PlanScratch, TierBudgetKind, TierHistoryShortfall, checked_sum, tier_history_refusal,
			vec_bytes,
		};
		let new_bound =
			crate::storage::core::count_slab_entries_in_blob(plaintext, slab_id, acknowledged)?;
		let old_bound = self.count_record_rows_for_slab(slab_id);
		let short = || tier_history_refusal(slab_id, TierHistoryShortfall::Scratch);
		// Rows: the plan's vectors, the index transaction's copies of them
		// (`try_copy_rows`), and the LSM scope check's copy of both
		// (`try_touched_rows`); pairs: the plan's (pairing's own buffers are in
		// `tier_pairing_scratch_bytes`, the refresh's in
		// `tier_refresh_prepare_bytes`).
		let rows = |count| vec_bytes::<PersistentRecordRow>(count)?.checked_mul(3);
		let pairs = vec_bytes::<(RecordLocation, RecordLocation)>(old_bound);
		let bytes = checked_sum(&[
			crate::storage::core::bounded_recovery_bytes(new_bound),
			rows(new_bound.entries),
			rows(old_bound),
			pairs,
			super::tier_plan::tier_pairing_scratch_bytes(old_bound, new_bound.entries),
			tier_refresh_prepare_bytes(old_bound, new_bound.entries),
		])
		.ok_or_else(short)?;
		let charge = self
			.tier_budget
			.try_reserve(TierBudgetKind::Scratch, bytes)
			.ok_or_else(short)?;
		Ok(TierPlanScratch {
			new_bound,
			old_bound,
			charge: PlanScratch::new(charge),
		})
	}

	/// `tiered_rows_from_plaintext_with_evidence` within `scratch`'s bound.
	pub(super) fn tiered_rows_within(
		&self,
		plaintext: &[u8],
		slab_id: SlabId,
		acknowledged: &BTreeSet<u64>,
		scratch: &TierPlanScratch,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		let entries = crate::storage::core::recover_slab_entries_from_blob_bounded(
			plaintext,
			slab_id,
			acknowledged,
			scratch.new_bound,
		)
		.map_err(|error| stale_plan_if_over_bound(slab_id, error))?;
		let mut rows = Vec::new();
		rows.try_reserve_exact(entries.len()).map_err(|_| {
			super::tier_budget::tier_history_refusal(
				slab_id,
				super::tier_budget::TierHistoryShortfall::Scratch,
			)
		})?;
		for frame in crate::storage::core::recovered_frames(&entries) {
			self.persistent_rows_from_recovered_frame(plaintext, frame, &mut rows)?;
		}
		Ok(rows)
	}

	/// `record_rows_for_slab` (readiness checked) within `scratch`'s bound.
	pub(super) fn record_rows_for_slab_within_checked(
		&self,
		slab_id: SlabId,
		scratch: &TierPlanScratch,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		self.require_tier_authority_ready()?;
		self.record_rows_for_slab_for_recovery_within(slab_id, scratch)
	}

	/// The slab's indexed rows, as `record_rows_for_slab_for_recovery` lists
	/// them, within `scratch`'s bound.
	pub(super) fn record_rows_for_slab_for_recovery_within(
		&self,
		slab_id: SlabId,
		scratch: &TierPlanScratch,
	) -> std::io::Result<Vec<PersistentRecordRow>> {
		let mut rows = Vec::new();
		rows.try_reserve_exact(scratch.old_bound)
			.map_err(|_| Error::new(ErrorKind::OutOfMemory, "tier plan rows"))?;
		for entry in self.key_index.iter() {
			let (tenant_id, key_identity) = *entry.key();
			for location in entry.value().iter() {
				if location.slab_id == slab_id {
					if rows.len() == scratch.old_bound {
						return Err(super::tier_budget::tier_history_refusal(
							slab_id,
							super::tier_budget::TierHistoryShortfall::StalePlan,
						));
					}
					rows.push(self.persistent_record_row(tenant_id, key_identity, *location));
				}
			}
		}
		// Rows equal in this key are one location of one key, so they are equal
		// rows and the unstable sort, which needs no buffer, is exact.
		rows.sort_unstable_by_key(|row| {
			(
				row.location.slab_id,
				row.tenant_id,
				row.key_identity,
				row.location.hlc,
				row.location.offset,
				row.location.record_row_index,
			)
		});
		Ok(rows)
	}

	/// The plan that moves the slab's indexed rows to `pointer`'s object, with
	/// the manager's published generation as the one it replaces.
	#[cfg(test)]
	pub(super) async fn build_tiered_row_refresh_plan_from_l2(
		&self,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<TieredRowRefreshPlan> {
		let published = self.tier_pointer(pointer.slab_id).await;
		self.build_tiered_row_refresh_plan(pointer, published.as_ref())
			.await
	}

	/// The plan that moves the slab's indexed rows to `pointer`'s object. Rows
	/// pair by logical owner (`tier_plan`); a conflicting owner whose copies
	/// move pairs by payload, read from the published generation `published`.
	pub(super) async fn build_tiered_row_refresh_plan(
		&self,
		pointer: &TieredSlabPointer,
		published: Option<&TieredSlabPointer>,
	) -> std::io::Result<TieredRowRefreshPlan> {
		// A pointer to a different object than the published generation is a
		// replacement: the index still describes the published one. Otherwise
		// the object is the generation the index describes (a first upload of
		// the indexed local slab, or the same object with only advisory
		// tombstone or supersession metadata changed).
		let acknowledged = self.acknowledged_offsets_in_slab(pointer.slab_id);
		let evidence = tier_object_evidence(published, pointer, &acknowledged, false);
		let TieredPlaintext {
			bytes: new_plaintext,
			record_offsets: new_offsets,
		} = self.tiered_object_from_l2(pointer, evidence).await?;
		let scratch =
			self.reserve_tier_plan_scratch(pointer.slab_id, &new_plaintext, &new_offsets)?;
		let new_rows =
			self.tiered_rows_within(&new_plaintext, pointer.slab_id, &new_offsets, &scratch)?;
		// The committed observer holds this slab's tier lock and has checked
		// its private authority; unrelated intents fence public reads only.
		let old_rows = self.record_rows_for_slab_for_recovery_within(pointer.slab_id, &scratch)?;
		let old_plaintext = match published {
			Some(published)
				if self.tier_pairing_needs_old_payloads(
					pointer.slab_id,
					&old_rows,
					&new_rows,
				)? =>
			{
				Some(self.tiered_plaintext_from_l2(published).await?)
			}
			_ => None,
		};
		let relocated_reads = self.pair_tier_rows(
			pointer.slab_id,
			&old_rows,
			&new_rows,
			old_plaintext.as_deref(),
			&new_plaintext,
		)?;
		Ok(TieredRowRefreshPlan {
			pointer: Some(pointer.clone()),
			old_rows,
			new_rows,
			relocated_reads,
			scratch: scratch.charge,
		})
	}

	/// Apply a refresh plan outside a publication: only while the plan's
	/// pointer is still the slab's published generation and the slab was not
	/// retired. A stale plan is refused whole, never resurrected. Production
	/// plans are built and applied inside `apply_tier_slab_committed` under the
	/// tier publication lock, so only controls apply a plan this way.
	#[cfg(test)]
	pub(super) async fn apply_tiered_row_refresh_plan(&self, plan: TieredRowRefreshPlan) {
		let Some(pointer) = plan.pointer.clone() else {
			return;
		};
		let tier_lock = self.tier_publication_lock(pointer.slab_id);
		let _tier_guard = tier_lock.lock().await;
		let current = {
			let state = self.state.lock().await;
			state
				.tiered
				.get(&pointer.slab_id)
				.is_some_and(|published| published.value() == &pointer)
		};
		if !current || self.retired_tier_slabs.contains(&pointer.slab_id) {
			tracing::warn!(
				slab_id = pointer.slab_id.get(),
				"stale tier row refresh plan refused"
			);
			return;
		}
		self.apply_tiered_row_refresh_plan_locked(plan, true).await;
	}

	/// Apply a refresh plan whose pointer the caller has published under the
	/// slab's tier publication lock.
	#[cfg(test)]
	pub(super) async fn apply_tiered_row_refresh_plan_locked(
		&self,
		plan: TieredRowRefreshPlan,
		mirror: bool,
	) {
		let prepared = self
			.prepare_tiered_row_refresh(&plan)
			.expect("a test-only refresh plan's relocations fit its scratch");
		self.apply_prepared_tiered_row_refresh(&plan, prepared, mirror)
			.await;
	}

	/// Build, before any publication boundary, every per-key relocation a
	/// refresh plan applies: fallibly, in containers sized exactly from counts
	/// and covered by the plan's scratch reservation (ADR 0692 §7), so the
	/// apply after the pointer and rows are published allocates no covered
	/// memory. Its effect is the one `apply_tiered_row_refresh_plan_locked`
	/// always had: rows match by owner; each surviving pair relocates; other
	/// old rows are removed and other new rows inserted, per key in key
	/// order, each key's routing locality taken from its first entry.
	pub(super) fn prepare_tiered_row_refresh(
		&self,
		plan: &TieredRowRefreshPlan,
	) -> std::io::Result<PreparedTierRefresh> {
		use super::tier_budget::{bounded_set_insert, try_bounded_map, try_bounded_set};
		if plan.old_rows == plan.new_rows {
			return Ok(PreparedTierRefresh::default());
		}
		use super::tier_budget::{TierHistoryShortfall, tier_history_refusal};
		let slab_id = plan
			.pointer
			.as_ref()
			.map_or(SlabId::ZERO, |pointer| pointer.slab_id);
		let unallocated = |_: hashbrown::TryReserveError| {
			tier_history_refusal(slab_id, TierHistoryShortfall::Scratch)
		};
		let unallocated_vec = |_: std::collections::TryReserveError| {
			tier_history_refusal(slab_id, TierHistoryShortfall::Scratch)
		};
		let stale = |_| tier_history_refusal(slab_id, TierHistoryShortfall::StalePlan);
		let index_by_location = |rows: &[PersistentRecordRow]| {
			let mut map = try_bounded_map(rows.len()).map_err(unallocated)?;
			for (index, row) in rows.iter().enumerate() {
				super::tier_budget::bounded_insert(&mut map, rows.len(), row.location, index)
					.map_err(stale)?;
			}
			Ok::<_, Error>(map)
		};
		// Later duplicates win, as `collect::<HashMap<_, _>>` had them.
		let old_by_location = index_by_location(&plan.old_rows)?;
		let new_by_location = index_by_location(&plan.new_rows)?;
		let same_owner = |old: &PersistentRecordRow, new: &PersistentRecordRow| {
			tier_row_owner(old) == tier_row_owner(new)
		};
		// (key, push order, entry): the old per-key `BTreeMap` built in loop
		// order, flattened. Push order is unique, so the unstable sort is exact.
		let mut entries = Vec::new();
		entries
			.try_reserve_exact(
				plan.relocated_reads.len() + plan.old_rows.len() + plan.new_rows.len(),
			)
			.map_err(unallocated_vec)?;
		let mut paired_old = try_bounded_set(plan.relocated_reads.len()).map_err(unallocated)?;
		let mut paired_new = try_bounded_set(plan.relocated_reads.len()).map_err(unallocated)?;
		for (old, new) in &plan.relocated_reads {
			let (Some(old_row), Some(new_row)) = (
				old_by_location.get(old).map(|index| &plan.old_rows[*index]),
				new_by_location.get(new).map(|index| &plan.new_rows[*index]),
			) else {
				continue;
			};
			if !same_owner(old_row, new_row) {
				continue;
			}
			bounded_set_insert(&mut paired_old, plan.relocated_reads.len(), *old).map_err(stale)?;
			bounded_set_insert(&mut paired_new, plan.relocated_reads.len(), *new).map_err(stale)?;
			let order = entries.len();
			entries.push((
				(old_row.tenant_id, old_row.key_identity),
				order,
				RefreshEntry::Pair {
					locality: new_row.routing_locality,
					pair: RelocationPair {
						old: *old,
						new: *new,
						table_object_id: new_row.table_object_id,
					},
				},
			));
		}
		let kept =
			|row: &PersistentRecordRow,
			 others: &[PersistentRecordRow],
			 by_location: &super::tier_budget::BudgetedMap<RecordLocation, usize>| {
				by_location
					.get(&row.location)
					.is_some_and(|index| same_owner(row, &others[*index]))
			};
		for row in &plan.old_rows {
			if !paired_old.contains(&row.location) && !kept(row, &plan.new_rows, &new_by_location) {
				let order = entries.len();
				entries.push((
					(row.tenant_id, row.key_identity),
					order,
					RefreshEntry::Removal {
						locality: row.routing_locality,
						location: row.location,
					},
				));
			}
		}
		for row in &plan.new_rows {
			if !paired_new.contains(&row.location) && !kept(row, &plan.old_rows, &old_by_location) {
				let order = entries.len();
				entries.push((
					(row.tenant_id, row.key_identity),
					order,
					RefreshEntry::Insertion {
						locality: row.routing_locality,
						insertion: RelocationInsertion {
							location: row.location,
							table_object_id: row.table_object_id,
						},
					},
				));
			}
		}
		entries.sort_unstable_by_key(|(key, order, _)| (*key, *order));
		let keys = entries.chunk_by(|left, right| left.0 == right.0).count();
		let mut relocations = Vec::new();
		relocations
			.try_reserve_exact(keys)
			.map_err(unallocated_vec)?;
		for run in entries.chunk_by(|left, right| left.0 == right.0) {
			let count =
				|wanted: fn(&RefreshEntry) -> bool| run.iter().filter(|e| wanted(&e.2)).count();
			let mut relocation = KeyRelocation::new(run[0].2.locality(), None);
			relocation
				.pairs
				.try_reserve_exact(count(|e| matches!(e, RefreshEntry::Pair { .. })))
				.map_err(unallocated_vec)?;
			relocation
				.removals
				.try_reserve_exact(count(|e| matches!(e, RefreshEntry::Removal { .. })))
				.map_err(unallocated_vec)?;
			relocation
				.insertions
				.try_reserve_exact(count(|e| matches!(e, RefreshEntry::Insertion { .. })))
				.map_err(unallocated_vec)?;
			for (_, _, entry) in run {
				match entry {
					RefreshEntry::Pair { pair, .. } => relocation.pairs.push(*pair),
					RefreshEntry::Removal { location, .. } => relocation.removals.push(*location),
					RefreshEntry::Insertion { insertion, .. } => {
						relocation.insertions.push(*insertion)
					}
				}
			}
			relocations.push((run[0].0, relocation));
		}
		let (pairs, removals) =
			relocations
				.iter()
				.fold((0, 0), |(pairs, removals), (_, relocation)| {
					(
						pairs.max(relocation.pairs.len()),
						removals.max(relocation.removals.len()),
					)
				});
		let scratch = super::relocation::RelocationScratch::try_for_counts(pairs, removals)
			.ok_or_else(|| tier_history_refusal(slab_id, TierHistoryShortfall::Scratch))?;
		Ok(PreparedTierRefresh {
			relocations,
			scratch,
		})
	}

	/// Apply relocations prepared by `prepare_tiered_row_refresh` for `plan`.
	pub(super) async fn apply_prepared_tiered_row_refresh(
		&self,
		plan: &TieredRowRefreshPlan,
		prepared: PreparedTierRefresh,
		mirror: bool,
	) {
		if plan.old_rows == plan.new_rows {
			return;
		}
		let PreparedTierRefresh {
			relocations: by_key,
			mut scratch,
		} = prepared;
		let mut relocated_any = false;
		for ((tenant_id, key_identity), relocation) in &by_key {
			// `scratch` was sized for the largest of these relocations.
			self.relocate_indexed_locations(*tenant_id, *key_identity, relocation, &mut scratch);
			if !relocated_any {
				relocated_any = true;
				#[cfg(test)]
				self.index_relocation_pause_point().await;
			}
		}
		// The refreshed object is the authority for each row's routing locality.
		for row in &plan.new_rows {
			self.insert_locality_membership_with_locality(
				row.tenant_id,
				row.key_identity,
				row.routing_locality,
			);
		}
		if !mirror {
			return;
		}
		// Test-only mirror path (`apply_tiered_row_refresh_plan`).
		self.mirror_persistent_update(
			PersistentIndexUpdate {
				delete_records: plan.old_rows.clone(),
				put_records: plan.new_rows.clone(),
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
	) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
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
		let (header, frame) = if pointer.is_encrypted {
			self.read_encrypted_record_and_header_from_l2(l2.as_ref(), pointer, location)
				.await?
		} else {
			let plaintext = self
				.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
				.await?;
			let record = extract_record_and_header_from_blob(
				&plaintext,
				crate::storage::core::FrameLocation::of_point_read(location),
			)?;
			plaintext_for_rehydration = Some(plaintext);
			record
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
		Ok((header, frame))
	}

	pub(super) async fn fetch_full_tiered_plaintext(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
	) -> std::io::Result<Vec<u8>> {
		let blob = l2.get_bytes(&pointer.l2_uri).await?;
		validate_tiered_blob(pointer, blob.as_ref())?;
		// Reads serve the published generation, which the index describes.
		let acknowledged = self.acknowledged_offsets_in_slab(pointer.slab_id);
		Ok(decode_tiered_blob(
			self.config.crypto.as_ref(),
			pointer,
			blob.as_ref(),
			TierObjectEvidence::Indexed(&acknowledged),
		)?
		.bytes)
	}

	pub(super) async fn read_encrypted_record_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<Vec<u8>> {
		self.read_encrypted_record_and_header_from_l2(l2, pointer, location)
			.await
			.map(|(_, frame)| frame)
	}

	pub(super) async fn read_encrypted_record_and_header_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
	) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
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
		extract_record_and_header_from_blob(
			&record_bytes,
			crate::storage::core::FrameLocation::of_point_read(RecordLocation {
				offset: 0,
				..location
			}),
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
		// The slab's path holds a quarantined file: a copy would have to
		// replace evidence, so the slab keeps serving from verified L2.
		if self.quarantined_slab_files.contains(slab_id) {
			state.hot_read_counts.remove(&slab_id);
			self.quarantined_slab_files.note_rehydration_skipped();
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
		// A copy is installed only for the generation it was made from.
		let still_published = state
			.tiered
			.get(&pointer.slab_id)
			.is_some_and(|published| published.value() == &pointer);
		match result {
			Ok(_) if !still_published => {
				// The copy this call installed belongs to a superseded
				// generation: it is never tracked, and its file is removed
				// rather than left at the slab's path.
				self.release_slab_file(pointer.slab_id);
				drop(state);
				self.unlink_released_slab_file(pointer.slab_id).await
			}
			Ok(installed) => {
				state.hot_read_counts.remove(&pointer.slab_id);
				state.install_rehydrated(&self.tier_budget, pointer.slab_id, installed);
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
		if self.quarantined_slab_files.contains(slab_id) {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!("slab {slab_id}'s path holds a quarantined file; it is never replaced"),
			));
		}
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
		// The copy is published only where no file exists: whatever is already
		// at the slab's path is not this copy's to replace.
		let linked = self.file_ops.link_no_replace(&tmp_path, &path).await;
		let _ = unlink_local_file(self.file_ops.as_ref(), &tmp_path).await;
		linked?;
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

impl SlabManager {
	/// ADR 0099: the offsets this manager's index holds for `slab_id`, the
	/// evidence a sealed-bytes walk needs to resume past an anomaly. For a
	/// tiered slab these were loaded from the durable rows; for a local
	/// sealed slab they came from open-time recovery, which itself used the
	/// durable rows, plus live appends since. A slab the index knows nothing
	/// about yields the empty set, which is the strict walk.
	pub(crate) fn acknowledged_offsets_in_slab(&self, slab_id: SlabId) -> BTreeSet<u64> {
		self.key_index
			.iter()
			.flat_map(|entry| {
				entry
					.value()
					.iter()
					.filter(|location| location.slab_id == slab_id)
					.map(|location| location.offset)
					.collect::<Vec<_>>()
			})
			.collect()
	}

	/// The same evidence for every slab at once, for callers that walk many.
	pub(crate) fn acknowledged_offsets_by_slab(&self) -> BTreeMap<SlabId, BTreeSet<u64>> {
		let mut by_slab: BTreeMap<SlabId, BTreeSet<u64>> = BTreeMap::new();
		for entry in self.key_index.iter() {
			for location in entry.value().iter() {
				by_slab
					.entry(location.slab_id)
					.or_default()
					.insert(location.offset);
			}
		}
		by_slab
	}
}

/// A publication plan's counted bounds and its reserved scratch charge.
pub(super) struct TierPlanScratch {
	pub(super) new_bound: crate::storage::core::SlabEntryBound,
	pub(super) old_bound: usize,
	pub(super) charge: super::tier_budget::PlanScratch,
}

/// A bounded walk that outgrew its count means the plan's inputs changed or
/// the count was wrong: refuse the plan as stale rather than grow.
fn stale_plan_if_over_bound(slab_id: SlabId, error: Error) -> Error {
	let over = error
		.get_ref()
		.is_some_and(|inner| inner.is::<crate::storage::core::EntryBoundExceeded>());
	if over {
		super::tier_budget::tier_history_refusal(
			slab_id,
			super::tier_budget::TierHistoryShortfall::StalePlan,
		)
	} else {
		error
	}
}

/// The per-key relocations of one refresh plan, built before publication.
#[derive(Debug, Default)]
pub(super) struct PreparedTierRefresh {
	relocations: Vec<(TenantKey, KeyRelocation)>,
	/// The relocation buffers, sized for the largest per-key relocation.
	scratch: super::relocation::RelocationScratch,
}

#[cfg(test)]
impl PreparedTierRefresh {
	pub(super) fn relocations_for_test(&self) -> &[(TenantKey, KeyRelocation)] {
		&self.relocations
	}
}

/// One flattened per-key change of a refresh plan.
#[derive(Clone, Copy, Debug)]
enum RefreshEntry {
	Pair {
		locality: Locality,
		pair: RelocationPair,
	},
	Removal {
		locality: Locality,
		location: RecordLocation,
	},
	Insertion {
		locality: Locality,
		insertion: RelocationInsertion,
	},
}

impl RefreshEntry {
	fn locality(&self) -> Locality {
		match self {
			Self::Pair { locality, .. }
			| Self::Removal { locality, .. }
			| Self::Insertion { locality, .. } => *locality,
		}
	}
}

/// Bytes `prepare_tiered_row_refresh` allocates for a plan of `old` rows,
/// `new` rows and at most `old` pairs.
pub(super) fn tier_refresh_prepare_bytes(old: usize, new: usize) -> Option<u64> {
	use super::tier_budget::{checked_sum, hash_table_bytes_bound, vec_bytes};
	let entries = old.checked_mul(2)?.checked_add(new)?;
	checked_sum(&[
		hash_table_bytes_bound::<(RecordLocation, usize)>(old),
		hash_table_bytes_bound::<(RecordLocation, usize)>(new),
		hash_table_bytes_bound::<RecordLocation>(old),
		hash_table_bytes_bound::<RecordLocation>(old),
		vec_bytes::<(TenantKey, usize, RefreshEntry)>(entries),
		vec_bytes::<(TenantKey, KeyRelocation)>(old.checked_add(new)?),
		// Each entry lands in exactly one per-key vector.
		vec_bytes::<RelocationPair>(old),
		vec_bytes::<RecordLocation>(old),
		vec_bytes::<RelocationInsertion>(new),
		// The apply's relocation buffers: no key has more pairs or removals
		// than the plan has old rows, whatever its version chain holds.
		super::relocation::RelocationScratch::bytes_bound(old, old),
	])
}

/// What a tier delivery fetched before it held any authority.
pub(super) struct PreparedTierDelivery {
	old_pointer: Option<TieredSlabPointer>,
	plan: Option<TieredRowRefreshPlan>,
}
