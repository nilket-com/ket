use crate::schema::{
	IndexObjectId, LogicalSchemaFingerprint, SecondaryIndexDescriptor, SlabId, TableObjectId,
	TenantId,
};
use crate::storage::core::RecordLocation;
use crate::storage::secondary_index::{
	SecondaryIndexEntry, SecondaryIndexIdentity, secondary_index_entries_from_storage_batch,
};
use crate::storage::secondary_index_segment::SecondaryIndexSegment;
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{Error, ErrorKind};
use std::mem::size_of;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecondaryIndexBackfillErrorReason {
	CompactionRace,
	EncodeFailure,
	ActiveBufferOverflow,
	SealFailure,
	PersistFailure,
	CorruptSegment,
	MissingSegment,
}

impl SecondaryIndexBackfillErrorReason {
	pub fn label(self) -> &'static str {
		match self {
			Self::CompactionRace => "compaction_race",
			Self::EncodeFailure => "encode_failure",
			Self::ActiveBufferOverflow => "active_buffer_overflow",
			Self::SealFailure => "seal_failure",
			Self::PersistFailure => "persist_failure",
			Self::CorruptSegment => "corrupt_segment",
			Self::MissingSegment => "missing_segment",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexBackfillError {
	pub reason: SecondaryIndexBackfillErrorReason,
	pub message: String,
}

impl SecondaryIndexBackfillError {
	pub fn new(reason: SecondaryIndexBackfillErrorReason, message: impl Into<String>) -> Self {
		Self {
			reason,
			message: message.into(),
		}
	}
}

impl fmt::Display for SecondaryIndexBackfillError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}: {}", self.reason.label(), self.message)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexActiveBufferSnapshot {
	pub source_slab_id: SlabId,
	pub entry_count: usize,
	pub buffered_bytes: usize,
	pub max_buffer_bytes: usize,
	pub failure: Option<SecondaryIndexBackfillError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SecondaryIndexCoverageKey {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
}

impl From<SecondaryIndexIdentity> for SecondaryIndexCoverageKey {
	fn from(identity: SecondaryIndexIdentity) -> Self {
		Self {
			tenant_id: identity.tenant_id,
			table_object_id: identity.table_object_id,
			index_object_id: identity.index_object_id,
			logical_schema_fingerprint: identity.logical_schema_fingerprint,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCoverageSnapshot {
	pub key: SecondaryIndexCoverageKey,
	pub covered_slabs: Vec<SlabId>,
	pub active_buffer: Option<SecondaryIndexActiveBufferSnapshot>,
	pub uncovered_slabs: Vec<(SlabId, SecondaryIndexBackfillError)>,
	pub last_backfill_error: Option<SecondaryIndexBackfillError>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexBackfillCandidate {
	pub key: SecondaryIndexCoverageKey,
	pub source_slab_id: SlabId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexBackfillPlan {
	pub snapshot: SecondaryIndexCoverageSnapshot,
	pub candidates: Vec<SecondaryIndexBackfillCandidate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexBackfillSuccess {
	pub candidate: SecondaryIndexBackfillCandidate,
	pub entry_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexBackfillFailure {
	pub candidate: SecondaryIndexBackfillCandidate,
	pub error: SecondaryIndexBackfillError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexBackfillTickReport {
	pub before: SecondaryIndexBackfillPlan,
	pub after: SecondaryIndexBackfillPlan,
	pub attempted: usize,
	pub completed: Vec<SecondaryIndexBackfillSuccess>,
	pub failures: Vec<SecondaryIndexBackfillFailure>,
	pub historical_complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexActiveBuffer {
	identity: SecondaryIndexIdentity,
	source_slab_id: SlabId,
	max_buffer_bytes: usize,
	buffered_bytes: usize,
	entries: Vec<SecondaryIndexEntry>,
	failure: Option<SecondaryIndexBackfillError>,
}

impl SecondaryIndexActiveBuffer {
	pub fn new(
		identity: SecondaryIndexIdentity,
		source_slab_id: SlabId,
		max_buffer_bytes: usize,
	) -> Self {
		Self {
			identity,
			source_slab_id,
			max_buffer_bytes,
			buffered_bytes: 0,
			entries: Vec::new(),
			failure: None,
		}
	}

	pub fn identity(&self) -> SecondaryIndexIdentity {
		self.identity
	}

	pub fn source_slab_id(&self) -> SlabId {
		self.source_slab_id
	}

	pub fn entry_count(&self) -> usize {
		self.entries.len()
	}

	pub fn buffered_bytes(&self) -> usize {
		self.buffered_bytes
	}

	pub fn max_buffer_bytes(&self) -> usize {
		self.max_buffer_bytes
	}

	pub fn failure(&self) -> Option<&SecondaryIndexBackfillError> {
		self.failure.as_ref()
	}

	pub fn entries(&self) -> &[SecondaryIndexEntry] {
		&self.entries
	}

	pub fn append_storage_batch(
		&mut self,
		descriptor: &SecondaryIndexDescriptor,
		batch: &RecordBatch,
		base_location: RecordLocation,
	) -> std::io::Result<usize> {
		if let Some(failure) = &self.failure {
			return Err(Error::new(
				ErrorKind::Other,
				format!("secondary index active buffer already failed: {failure}"),
			));
		}
		validate_descriptor_identity(descriptor, self.identity)?;
		if base_location.slab_id != self.source_slab_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index active buffer source slab mismatch",
			));
		}
		let entries =
			match secondary_index_entries_from_storage_batch(descriptor, batch, base_location) {
				Ok(entries) => entries,
				Err(err) => {
					self.fail(SecondaryIndexBackfillError::new(
						SecondaryIndexBackfillErrorReason::EncodeFailure,
						err.to_string(),
					));
					return Err(err);
				}
			};
		let added_bytes = entries.iter().map(estimated_entry_bytes).sum::<usize>();
		if self.buffered_bytes.saturating_add(added_bytes) > self.max_buffer_bytes {
			let failure = SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::ActiveBufferOverflow,
				format!(
					"secondary index active buffer exceeded {} bytes",
					self.max_buffer_bytes
				),
			);
			self.fail(failure.clone());
			return Err(Error::new(ErrorKind::Other, failure.to_string()));
		}
		let appended = entries.len();
		self.buffered_bytes += added_bytes;
		self.entries.extend(entries);
		Ok(appended)
	}

	pub fn append_entries(
		&mut self,
		descriptor: &SecondaryIndexDescriptor,
		entries: Vec<SecondaryIndexEntry>,
		source_slab_id: SlabId,
	) -> std::io::Result<usize> {
		if let Some(failure) = &self.failure {
			return Err(Error::new(
				ErrorKind::Other,
				format!("secondary index active buffer already failed: {failure}"),
			));
		}
		validate_descriptor_identity(descriptor, self.identity)?;
		if source_slab_id != self.source_slab_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index active buffer source slab mismatch",
			));
		}
		let added_bytes = entries.iter().map(estimated_entry_bytes).sum::<usize>();
		if self.buffered_bytes.saturating_add(added_bytes) > self.max_buffer_bytes {
			let failure = SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::ActiveBufferOverflow,
				format!(
					"secondary index active buffer exceeded {} bytes",
					self.max_buffer_bytes
				),
			);
			self.fail(failure.clone());
			return Err(Error::new(ErrorKind::Other, failure.to_string()));
		}
		let appended = entries.len();
		self.buffered_bytes += added_bytes;
		self.entries.extend(entries);
		Ok(appended)
	}

	pub fn seal(
		self,
		descriptor: &SecondaryIndexDescriptor,
	) -> std::io::Result<SecondaryIndexSegment> {
		if let Some(failure) = self.failure {
			return Err(Error::new(
				ErrorKind::Other,
				format!("secondary index active buffer cannot seal after failure: {failure}"),
			));
		}
		validate_descriptor_identity(descriptor, self.identity)?;
		SecondaryIndexSegment::build(
			self.identity,
			self.source_slab_id,
			descriptor.table_schema_generation,
			descriptor.key.arrow_type_tag,
			self.entries,
		)
	}

	fn fail(&mut self, failure: SecondaryIndexBackfillError) {
		self.entries.clear();
		self.buffered_bytes = 0;
		self.failure = Some(failure);
	}

	fn snapshot(&self) -> SecondaryIndexActiveBufferSnapshot {
		SecondaryIndexActiveBufferSnapshot {
			source_slab_id: self.source_slab_id,
			entry_count: self.entries.len(),
			buffered_bytes: self.buffered_bytes,
			max_buffer_bytes: self.max_buffer_bytes,
			failure: self.failure.clone(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCoverageTracker {
	key: SecondaryIndexCoverageKey,
	covered_slabs: BTreeSet<SlabId>,
	active_slab_buffer: Option<SecondaryIndexActiveBuffer>,
	uncovered_slabs: BTreeMap<SlabId, SecondaryIndexBackfillError>,
	last_backfill_error: Option<SecondaryIndexBackfillError>,
}

impl SecondaryIndexCoverageTracker {
	pub fn new(key: SecondaryIndexCoverageKey) -> Self {
		Self {
			key,
			covered_slabs: BTreeSet::new(),
			active_slab_buffer: None,
			uncovered_slabs: BTreeMap::new(),
			last_backfill_error: None,
		}
	}

	pub fn key(&self) -> SecondaryIndexCoverageKey {
		self.key
	}

	pub fn active_buffer(&self) -> Option<&SecondaryIndexActiveBuffer> {
		self.active_slab_buffer.as_ref()
	}

	pub fn active_buffer_mut(&mut self) -> Option<&mut SecondaryIndexActiveBuffer> {
		self.active_slab_buffer.as_mut()
	}

	pub fn take_active_buffer(&mut self) -> Option<SecondaryIndexActiveBuffer> {
		self.active_slab_buffer.take()
	}

	pub fn mark_active(&mut self, buffer: SecondaryIndexActiveBuffer) -> std::io::Result<()> {
		if SecondaryIndexCoverageKey::from(buffer.identity()) != self.key {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index coverage tracker identity mismatch",
			));
		}
		self.covered_slabs.remove(&buffer.source_slab_id());
		self.uncovered_slabs.remove(&buffer.source_slab_id());
		self.active_slab_buffer = Some(buffer);
		Ok(())
	}

	pub fn mark_covered(&mut self, source_slab_id: SlabId) {
		self.covered_slabs.insert(source_slab_id);
		self.uncovered_slabs.remove(&source_slab_id);
		if self
			.active_slab_buffer
			.as_ref()
			.is_some_and(|buffer| buffer.source_slab_id() == source_slab_id)
		{
			self.active_slab_buffer = None;
		}
	}

	pub fn mark_uncovered(&mut self, source_slab_id: SlabId, error: SecondaryIndexBackfillError) {
		self.covered_slabs.remove(&source_slab_id);
		if self
			.active_slab_buffer
			.as_ref()
			.is_some_and(|buffer| buffer.source_slab_id() == source_slab_id)
		{
			self.active_slab_buffer = None;
		}
		self.last_backfill_error = Some(error.clone());
		self.uncovered_slabs.insert(source_slab_id, error);
	}

	pub fn forget_slab(&mut self, source_slab_id: SlabId) {
		self.covered_slabs.remove(&source_slab_id);
		self.uncovered_slabs.remove(&source_slab_id);
		if self
			.active_slab_buffer
			.as_ref()
			.is_some_and(|buffer| buffer.source_slab_id() == source_slab_id)
		{
			self.active_slab_buffer = None;
		}
	}

	pub fn is_covered(&self, source_slab_id: SlabId) -> bool {
		self.covered_slabs.contains(&source_slab_id)
	}

	pub fn covered_slabs(&self) -> impl Iterator<Item = SlabId> + '_ {
		self.covered_slabs.iter().copied()
	}

	pub fn uncovered_slabs(
		&self,
	) -> impl Iterator<Item = (SlabId, &SecondaryIndexBackfillError)> + '_ {
		self.uncovered_slabs
			.iter()
			.map(|(slab_id, error)| (*slab_id, error))
	}

	pub fn snapshot(&self) -> SecondaryIndexCoverageSnapshot {
		SecondaryIndexCoverageSnapshot {
			key: self.key,
			covered_slabs: self.covered_slabs.iter().copied().collect(),
			active_buffer: self
				.active_slab_buffer
				.as_ref()
				.map(SecondaryIndexActiveBuffer::snapshot),
			uncovered_slabs: self
				.uncovered_slabs
				.iter()
				.map(|(slab_id, error)| (*slab_id, error.clone()))
				.collect(),
			last_backfill_error: self.last_backfill_error.clone(),
		}
	}
}

fn validate_descriptor_identity(
	descriptor: &SecondaryIndexDescriptor,
	identity: SecondaryIndexIdentity,
) -> std::io::Result<()> {
	if descriptor.table_object_id != identity.table_object_id
		|| descriptor.index_object_id != identity.index_object_id
		|| descriptor.logical_schema_fingerprint != identity.logical_schema_fingerprint
		|| descriptor.key.column_ordinal != identity.column_ordinal
	{
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"secondary index descriptor does not match buffer identity",
		));
	}
	Ok(())
}

fn estimated_entry_bytes(entry: &SecondaryIndexEntry) -> usize {
	size_of::<SecondaryIndexEntry>() + entry.encoded_key.len()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		HybridTimestamp, Identity, IndexArrowTypeTag, IndexKeyEncoding, IndexObjectId, Locality,
		LogicalSchemaFingerprint, RecordRowIndex, SecondaryIndexKeyDescriptor, TableObjectId,
		TableWriteAdmission, TenantId,
	};
	use crate::storage::append_storage_synthetic_columns;
	use arrow::array::{Int64Array, UInt64Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	fn hlc(value: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: value,
			logical: 0,
			node_id: 1,
		}
	}

	fn descriptor() -> SecondaryIndexDescriptor {
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x52; 16]),
			table_object_id: TableObjectId::new([0x10; 16]),
			namespace: "analytics".to_owned(),
			index_name: "events_age_idx".to_owned(),
			table_schema_generation: 4,
			logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([0x51; 32]),
			key: SecondaryIndexKeyDescriptor {
				column_name: "age".to_owned(),
				column_ordinal: 1,
				arrow_type_tag: IndexArrowTypeTag::Int64,
				encoding: IndexKeyEncoding::OrderedScalar,
			},
		}
	}

	fn segment_identity(descriptor: &SecondaryIndexDescriptor) -> SecondaryIndexIdentity {
		SecondaryIndexIdentity {
			tenant_id: TenantId::new(7),
			table_object_id: descriptor.table_object_id,
			index_object_id: descriptor.index_object_id,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			column_ordinal: descriptor.key.column_ordinal,
		}
	}

	fn location(slab_id: SlabId, offset: u64, row_hlc: HybridTimestamp) -> RecordLocation {
		RecordLocation {
			slab_id,
			offset,
			length: 256,
			hlc: row_hlc,
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
		}
	}

	fn batch(ages: Vec<Option<i64>>, row_hlc: HybridTimestamp, identity_byte: u8) -> RecordBatch {
		let ids = UInt64Array::from_iter_values(0..ages.len() as u64);
		let ages = Int64Array::from(ages);
		let batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::UInt64, false),
				Field::new("age", DataType::Int64, true),
			])),
			vec![Arc::new(ids), Arc::new(ages)],
		)
		.unwrap();
		append_storage_synthetic_columns(
			&batch,
			&(0..batch.num_rows()).collect::<Vec<_>>(),
			&TableWriteAdmission {
				canonical_primary_keys: vec![vec![identity_byte]; batch.num_rows()],
				key_identities: vec![Identity::new([identity_byte; 32]); batch.num_rows()],
				localities: vec![Locality::new(11); batch.num_rows()],
			},
			row_hlc,
			Some(TableObjectId::new([0x10; 16])),
		)
		.unwrap()
	}

	#[test]
	fn active_buffer_emits_unchanged_value_versions_and_seals_segment() {
		let descriptor = descriptor();
		let identity = segment_identity(&descriptor);
		let source_slab_id = SlabId::new(41);
		let mut buffer = SecondaryIndexActiveBuffer::new(identity, source_slab_id, 4096);
		assert_eq!(
			buffer
				.append_storage_batch(
					&descriptor,
					&batch(vec![Some(42)], hlc(10), 1),
					location(source_slab_id, 100, hlc(10)),
				)
				.unwrap(),
			1
		);
		assert_eq!(
			buffer
				.append_storage_batch(
					&descriptor,
					&batch(vec![Some(42)], hlc(20), 1),
					location(source_slab_id, 200, hlc(20)),
				)
				.unwrap(),
			1
		);
		let segment = buffer.seal(&descriptor).unwrap();
		assert_eq!(segment.header.source_slab_id, source_slab_id);
		assert_eq!(segment.header.entry_count, 2);
		assert_eq!(
			segment.entries[0].encoded_key,
			segment.entries[1].encoded_key
		);
		assert_eq!(segment.entries[0].row_hlc, hlc(10));
		assert_eq!(segment.entries[1].row_hlc, hlc(20));
		segment.validate().unwrap();
	}

	#[test]
	fn active_buffer_skips_null_indexed_values() {
		let descriptor = descriptor();
		let identity = segment_identity(&descriptor);
		let source_slab_id = SlabId::new(41);
		let mut buffer = SecondaryIndexActiveBuffer::new(identity, source_slab_id, 4096);
		let appended = buffer
			.append_storage_batch(
				&descriptor,
				&batch(vec![None, Some(7)], hlc(10), 2),
				location(source_slab_id, 100, hlc(10)),
			)
			.unwrap();
		assert_eq!(appended, 1);
		assert_eq!(buffer.entry_count(), 1);
	}

	#[test]
	fn active_buffer_overflow_drops_entries_and_records_failure() {
		let descriptor = descriptor();
		let identity = segment_identity(&descriptor);
		let source_slab_id = SlabId::new(41);
		let mut buffer = SecondaryIndexActiveBuffer::new(identity, source_slab_id, 1);
		let err = buffer
			.append_storage_batch(
				&descriptor,
				&batch(vec![Some(7)], hlc(10), 2),
				location(source_slab_id, 100, hlc(10)),
			)
			.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::Other);
		assert_eq!(buffer.entry_count(), 0);
		assert_eq!(buffer.buffered_bytes(), 0);
		assert_eq!(
			buffer.failure().map(|failure| failure.reason),
			Some(SecondaryIndexBackfillErrorReason::ActiveBufferOverflow)
		);
	}

	#[test]
	fn coverage_tracker_moves_slabs_between_active_uncovered_and_covered() {
		let descriptor = descriptor();
		let identity = segment_identity(&descriptor);
		let source_slab_id = SlabId::new(41);
		let mut tracker =
			SecondaryIndexCoverageTracker::new(SecondaryIndexCoverageKey::from(identity));
		tracker
			.mark_active(SecondaryIndexActiveBuffer::new(
				identity,
				source_slab_id,
				4096,
			))
			.unwrap();
		assert_eq!(
			tracker.snapshot().active_buffer.unwrap().source_slab_id,
			source_slab_id
		);
		tracker.mark_uncovered(
			source_slab_id,
			SecondaryIndexBackfillError::new(
				SecondaryIndexBackfillErrorReason::CompactionRace,
				"source slab compacted during backfill",
			),
		);
		let snapshot = tracker.snapshot();
		assert!(snapshot.active_buffer.is_none());
		assert_eq!(snapshot.uncovered_slabs.len(), 1);
		assert_eq!(
			snapshot.last_backfill_error.unwrap().reason,
			SecondaryIndexBackfillErrorReason::CompactionRace
		);
		tracker.mark_covered(source_slab_id);
		assert!(tracker.is_covered(source_slab_id));
		assert!(tracker.snapshot().uncovered_slabs.is_empty());
	}
}
