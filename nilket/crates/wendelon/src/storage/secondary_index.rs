use crate::schema::pbc::{
	ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, ColumnPredicateV1, Program,
};
use crate::schema::table::TableSchemaDescriptor;
use crate::schema::{
	HybridTimestamp, Identity, IndexArrowTypeTag, IndexObjectId, LogicalSchemaFingerprint,
	SecondaryIndexCatalog, SecondaryIndexDescriptor, SecondaryIndexState, TableObjectId, TenantId,
};
use crate::storage::RecordLocation;
use crate::storage::multirow::{
	has_storage_synthetic_columns, materialize_record_location_batch, storage_rows_from_batch,
};
use arrow::array::{
	Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Int8Array, Int16Array, Int32Array,
	Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
	TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
	UInt16Array, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::io::{Error, ErrorKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexEntry {
	pub encoded_key: Vec<u8>,
	pub row_key_id: Identity,
	pub row_hlc: HybridTimestamp,
	pub record_location: RecordLocation,
}

impl SecondaryIndexEntry {
	pub fn logical_cmp(&self, other: &Self) -> Ordering {
		self.encoded_key
			.cmp(&other.encoded_key)
			.then_with(|| self.row_key_id.cmp(&other.row_key_id))
			.then_with(|| self.row_hlc.cmp(&other.row_hlc))
			.then_with(|| {
				self.record_location
					.slab_id
					.cmp(&other.record_location.slab_id)
			})
			.then_with(|| {
				self.record_location
					.offset
					.cmp(&other.record_location.offset)
			})
			.then_with(|| {
				self.record_location
					.record_row_index
					.cmp(&other.record_location.record_row_index)
			})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SecondaryIndexIdentity {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub column_ordinal: u32,
}

impl SecondaryIndexIdentity {
	pub fn from_descriptor(tenant_id: TenantId, descriptor: &SecondaryIndexDescriptor) -> Self {
		Self {
			tenant_id,
			table_object_id: descriptor.table_object_id,
			index_object_id: descriptor.index_object_id,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			column_ordinal: descriptor.key.column_ordinal,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecondaryIndexCandidateRejectReason {
	DescriptorMismatch,
	NotVisibleAsOf,
	RowMetadataMismatch,
	Tombstone,
	NullIndexedValue,
	IndexedValueMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecondaryIndexCandidateValidation {
	Accepted,
	Rejected(SecondaryIndexCandidateRejectReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecondaryIndexScanPredicate {
	Equal(Vec<u8>),
	Range {
		lower: Option<SecondaryIndexRangeBound>,
		upper: Option<SecondaryIndexRangeBound>,
	},
	InList(Vec<Vec<u8>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexRangeBound {
	pub encoded_key: Vec<u8>,
	pub inclusive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexBound {
	pub encoded_key: Vec<u8>,
	pub inclusive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexQuery {
	PointEqual {
		encoded_key: Vec<u8>,
	},
	Range {
		lower: Option<IndexBound>,
		upper: Option<IndexBound>,
	},
	InList {
		encoded_keys: Vec<Vec<u8>>,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableLookupByIndexRequest {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub query: IndexQuery,
	pub as_of_hlc: HybridTimestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexAccessPlan {
	pub descriptor: SecondaryIndexDescriptor,
	pub query: IndexQuery,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SecondaryIndexLookupStats {
	pub scanned_segments: u64,
	pub candidate_count: u64,
	pub visible_count: u64,
	pub superseded_count: u64,
	pub stale_indexed_value_count: u64,
	pub rejected_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexLookupRow {
	pub row_key_id: Identity,
	pub row_hlc: HybridTimestamp,
	pub record_location: RecordLocation,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SecondaryIndexLookupReport {
	pub rows: Vec<SecondaryIndexLookupRow>,
	pub stats: SecondaryIndexLookupStats,
}

impl From<SecondaryIndexRangeBound> for IndexBound {
	fn from(bound: SecondaryIndexRangeBound) -> Self {
		Self {
			encoded_key: bound.encoded_key,
			inclusive: bound.inclusive,
		}
	}
}

impl From<SecondaryIndexScanPredicate> for IndexQuery {
	fn from(predicate: SecondaryIndexScanPredicate) -> Self {
		match predicate {
			SecondaryIndexScanPredicate::Equal(encoded_key) => Self::PointEqual { encoded_key },
			SecondaryIndexScanPredicate::Range { lower, upper } => Self::Range {
				lower: lower.map(Into::into),
				upper: upper.map(Into::into),
			},
			SecondaryIndexScanPredicate::InList(mut encoded_keys) => {
				encoded_keys.sort();
				encoded_keys.dedup();
				Self::InList { encoded_keys }
			}
		}
	}
}

impl TableLookupByIndexRequest {
	pub fn from_descriptor(
		tenant_id: TenantId,
		descriptor: &SecondaryIndexDescriptor,
		query: IndexQuery,
		as_of_hlc: HybridTimestamp,
	) -> Self {
		Self {
			tenant_id,
			table_object_id: descriptor.table_object_id,
			index_object_id: descriptor.index_object_id,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			query,
			as_of_hlc,
		}
	}
}

pub fn secondary_index_predicate_from_pbc(
	descriptor: &SecondaryIndexDescriptor,
	predicates: &[ColumnPredicateV1],
) -> std::io::Result<Option<SecondaryIndexScanPredicate>> {
	let column_ordinal = descriptor.key.column_ordinal as usize;
	let mut equal = None;
	let mut in_list = None;
	let mut lower: Option<SecondaryIndexRangeBound> = None;
	let mut upper: Option<SecondaryIndexRangeBound> = None;

	for predicate in predicates
		.iter()
		.filter(|predicate| predicate.column_index == column_ordinal)
	{
		match predicate.comparison {
			ColumnPredicateComparisonV1::Eq => {
				let Some(encoded) = encode_secondary_index_literal(
					descriptor.key.arrow_type_tag,
					&predicate.literal,
				)?
				else {
					return Ok(None);
				};
				equal.get_or_insert(encoded);
			}
			ColumnPredicateComparisonV1::InList => {
				let ColumnPredicateLiteralV1::List(items) = &predicate.literal else {
					return Ok(None);
				};
				let mut encoded_items = Vec::with_capacity(items.len());
				for item in items {
					let Some(encoded) =
						encode_secondary_index_literal(descriptor.key.arrow_type_tag, item)?
					else {
						return Ok(None);
					};
					encoded_items.push(encoded);
				}
				encoded_items.sort();
				encoded_items.dedup();
				in_list.get_or_insert(encoded_items);
			}
			ColumnPredicateComparisonV1::Gt | ColumnPredicateComparisonV1::Gte => {
				let Some(encoded) = encode_secondary_index_literal(
					descriptor.key.arrow_type_tag,
					&predicate.literal,
				)?
				else {
					return Ok(None);
				};
				let bound = SecondaryIndexRangeBound {
					encoded_key: encoded,
					inclusive: predicate.comparison == ColumnPredicateComparisonV1::Gte,
				};
				if lower
					.as_ref()
					.is_none_or(|current| range_lower_bound_cmp(&bound, current).is_gt())
				{
					lower = Some(bound);
				}
			}
			ColumnPredicateComparisonV1::Lt | ColumnPredicateComparisonV1::Lte => {
				let Some(encoded) = encode_secondary_index_literal(
					descriptor.key.arrow_type_tag,
					&predicate.literal,
				)?
				else {
					return Ok(None);
				};
				let bound = SecondaryIndexRangeBound {
					encoded_key: encoded,
					inclusive: predicate.comparison == ColumnPredicateComparisonV1::Lte,
				};
				if upper
					.as_ref()
					.is_none_or(|current| range_upper_bound_cmp(&bound, current).is_lt())
				{
					upper = Some(bound);
				}
			}
		}
	}

	if let Some(encoded) = equal {
		return Ok(Some(SecondaryIndexScanPredicate::Equal(encoded)));
	}
	if let Some(items) = in_list {
		return Ok(Some(SecondaryIndexScanPredicate::InList(items)));
	}
	if lower.is_some() || upper.is_some() {
		return Ok(Some(SecondaryIndexScanPredicate::Range { lower, upper }));
	}
	Ok(None)
}

pub fn secondary_index_query_from_pbc(
	descriptor: &SecondaryIndexDescriptor,
	predicates: &[ColumnPredicateV1],
) -> std::io::Result<Option<IndexQuery>> {
	secondary_index_predicate_from_pbc(descriptor, predicates)
		.map(|predicate| predicate.map(Into::into))
}

pub fn secondary_index_access_plan_from_program(
	catalog: &SecondaryIndexCatalog,
	tenant_id: TenantId,
	table: &TableSchemaDescriptor,
	program: &Program,
) -> std::io::Result<Option<SecondaryIndexAccessPlan>> {
	let Some(predicates) = program.conjunctive_column_predicates() else {
		return Ok(None);
	};
	secondary_index_access_plan_from_predicates(catalog, tenant_id, table, &predicates)
}

pub fn secondary_index_access_plan_from_predicates(
	catalog: &SecondaryIndexCatalog,
	tenant_id: TenantId,
	table: &TableSchemaDescriptor,
	predicates: &[ColumnPredicateV1],
) -> std::io::Result<Option<SecondaryIndexAccessPlan>> {
	for row in catalog.for_table(tenant_id, table.table_object_id) {
		if row.state != SecondaryIndexState::Ready {
			continue;
		}
		if row.descriptor.table_schema_generation != table.schema_generation
			|| row.descriptor.logical_schema_fingerprint != table.logical_schema_fingerprint
		{
			continue;
		}
		row.descriptor.validate_against_table(table)?;
		let Some(query) = secondary_index_query_from_pbc(&row.descriptor, predicates)? else {
			continue;
		};
		return Ok(Some(SecondaryIndexAccessPlan {
			descriptor: row.descriptor.clone(),
			query,
		}));
	}
	Ok(None)
}

pub fn secondary_index_entry_matches_query(
	entry: &SecondaryIndexEntry,
	query: &IndexQuery,
) -> bool {
	match query {
		IndexQuery::PointEqual { encoded_key } => entry.encoded_key == *encoded_key,
		IndexQuery::Range { lower, upper } => {
			encoded_key_satisfies_lower(&entry.encoded_key, lower.as_ref())
				&& encoded_key_satisfies_upper(&entry.encoded_key, upper.as_ref())
		}
		IndexQuery::InList { encoded_keys } => encoded_keys
			.binary_search_by(|probe| probe.as_slice().cmp(entry.encoded_key.as_slice()))
			.is_ok(),
	}
}

pub fn secondary_index_key_range_intersects_query(
	entry_count: u64,
	min_encoded_key: &[u8],
	max_encoded_key: &[u8],
	query: &IndexQuery,
) -> bool {
	if entry_count == 0 {
		return false;
	}
	match query {
		IndexQuery::PointEqual { encoded_key } => {
			min_encoded_key <= encoded_key.as_slice() && encoded_key.as_slice() <= max_encoded_key
		}
		IndexQuery::Range { lower, upper } => {
			if let Some(upper) = upper {
				match upper.encoded_key.as_slice().cmp(min_encoded_key) {
					Ordering::Less => return false,
					Ordering::Equal if !upper.inclusive => return false,
					_ => {}
				}
			}
			if let Some(lower) = lower {
				match lower.encoded_key.as_slice().cmp(max_encoded_key) {
					Ordering::Greater => return false,
					Ordering::Equal if !lower.inclusive => return false,
					_ => {}
				}
			}
			true
		}
		IndexQuery::InList { encoded_keys } => encoded_keys.iter().any(|encoded_key| {
			min_encoded_key <= encoded_key.as_slice() && encoded_key.as_slice() <= max_encoded_key
		}),
	}
}

/// Revalidates an index sidecar candidate after the caller has fetched the row
/// version selected by primary-key MVCC lookup.
///
/// This function owns the descriptor/sidecar binding check, visibility and
/// tombstone checks against `visible_as_of_location`, storage-row metadata
/// confirmation, and indexed-value re-encoding. The caller still owns the row
/// fetch itself and any final PBC/predicate recheck over the projected query
/// result.
pub fn revalidate_secondary_index_candidate(
	descriptor: &SecondaryIndexDescriptor,
	segment_identity: SecondaryIndexIdentity,
	entry: &SecondaryIndexEntry,
	fetched_batch: &RecordBatch,
	visible_as_of_location: Option<RecordLocation>,
) -> std::io::Result<SecondaryIndexCandidateValidation> {
	if segment_identity.table_object_id != descriptor.table_object_id
		|| segment_identity.index_object_id != descriptor.index_object_id
		|| segment_identity.logical_schema_fingerprint != descriptor.logical_schema_fingerprint
		|| segment_identity.column_ordinal != descriptor.key.column_ordinal
	{
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::DescriptorMismatch,
		));
	}
	let Some(visible_location) = visible_as_of_location else {
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::NotVisibleAsOf,
		));
	};
	if visible_location.is_tombstone || visible_location.hlc != entry.row_hlc {
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::NotVisibleAsOf,
		));
	}
	if entry.record_location.is_tombstone {
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::Tombstone,
		));
	}
	if has_storage_synthetic_columns(fetched_batch) {
		let rows = storage_rows_from_batch(fetched_batch)?;
		let Some(row) = rows.get(entry.record_location.record_row_index.get() as usize) else {
			return Ok(SecondaryIndexCandidateValidation::Rejected(
				SecondaryIndexCandidateRejectReason::RowMetadataMismatch,
			));
		};
		if row.key_identity != entry.row_key_id || row.row_hlc != entry.row_hlc {
			return Ok(SecondaryIndexCandidateValidation::Rejected(
				SecondaryIndexCandidateRejectReason::RowMetadataMismatch,
			));
		}
		if row.is_tombstone {
			return Ok(SecondaryIndexCandidateValidation::Rejected(
				SecondaryIndexCandidateRejectReason::Tombstone,
			));
		}
	} else if entry.record_location.record_row_index != crate::schema::RecordRowIndex::ZERO
		|| entry.record_location.hlc != entry.row_hlc
	{
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::RowMetadataMismatch,
		));
	}

	let materialized =
		materialize_record_location_batch(fetched_batch, entry.record_location.record_row_index)?;
	let encoded = encode_secondary_index_key(&materialized, 0, descriptor)?;
	let Some(encoded) = encoded else {
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::NullIndexedValue,
		));
	};
	if encoded != entry.encoded_key {
		return Ok(SecondaryIndexCandidateValidation::Rejected(
			SecondaryIndexCandidateRejectReason::IndexedValueMismatch,
		));
	}
	Ok(SecondaryIndexCandidateValidation::Accepted)
}

pub fn secondary_index_entries_from_storage_batch(
	descriptor: &SecondaryIndexDescriptor,
	batch: &RecordBatch,
	base_location: RecordLocation,
) -> std::io::Result<Vec<SecondaryIndexEntry>> {
	let rows = storage_rows_from_batch(batch)?;
	let mut entries = Vec::new();
	for row in rows {
		if row.is_tombstone {
			continue;
		}
		let row_index = row.record_row_index.get() as usize;
		if row_index >= batch.num_rows() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"secondary index row {} is outside storage batch with {} rows",
					row_index,
					batch.num_rows()
				),
			));
		}
		let Some(encoded_key) = encode_secondary_index_key(batch, row_index, descriptor)? else {
			continue;
		};
		entries.push(SecondaryIndexEntry {
			encoded_key,
			row_key_id: row.key_identity,
			row_hlc: row.row_hlc,
			record_location: RecordLocation {
				hlc: row.row_hlc,
				record_row_index: row.record_row_index,
				is_tombstone: false,
				..base_location
			},
		});
	}
	Ok(entries)
}

pub fn secondary_index_entry_from_materialized_row(
	descriptor: &SecondaryIndexDescriptor,
	batch: &RecordBatch,
	row: usize,
	row_key_id: crate::schema::Identity,
	row_hlc: crate::schema::HybridTimestamp,
	record_location: RecordLocation,
) -> std::io::Result<Option<SecondaryIndexEntry>> {
	if record_location.is_tombstone {
		return Ok(None);
	}
	let Some(encoded_key) = encode_secondary_index_key(batch, row, descriptor)? else {
		return Ok(None);
	};
	Ok(Some(SecondaryIndexEntry {
		encoded_key,
		row_key_id,
		row_hlc,
		record_location: RecordLocation {
			hlc: row_hlc,
			is_tombstone: false,
			..record_location
		},
	}))
}

pub fn encode_secondary_index_key(
	batch: &RecordBatch,
	row: usize,
	descriptor: &SecondaryIndexDescriptor,
) -> std::io::Result<Option<Vec<u8>>> {
	let column = batch
		.columns()
		.get(descriptor.key.column_ordinal as usize)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"secondary index column_ordinal is outside the RecordBatch",
			)
		})?
		.as_ref();
	encode_secondary_index_key_from_array(column, row, descriptor.key.arrow_type_tag)
}

pub fn encode_secondary_index_key_from_array(
	array: &dyn Array,
	row: usize,
	tag: IndexArrowTypeTag,
) -> std::io::Result<Option<Vec<u8>>> {
	if row >= array.len() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"secondary index row is outside the Arrow array",
		));
	}
	if array.is_null(row) {
		return Ok(None);
	}
	match tag {
		IndexArrowTypeTag::Boolean => {
			let array = downcast_array::<BooleanArray>(array, tag)?;
			Ok(Some(vec![u8::from(array.value(row))]))
		}
		IndexArrowTypeTag::Int8 => {
			encode_signed(array, row, tag, |a: &Int8Array, row| a.value(row) as i64, 1)
		}
		IndexArrowTypeTag::Int16 => encode_signed(
			array,
			row,
			tag,
			|a: &Int16Array, row| a.value(row) as i64,
			2,
		),
		IndexArrowTypeTag::Int32 => encode_signed(
			array,
			row,
			tag,
			|a: &Int32Array, row| a.value(row) as i64,
			4,
		),
		IndexArrowTypeTag::Int64 => {
			encode_signed(array, row, tag, |a: &Int64Array, row| a.value(row), 8)
		}
		IndexArrowTypeTag::UInt8 => encode_unsigned(
			array,
			row,
			tag,
			|a: &UInt8Array, row| a.value(row) as u64,
			1,
		),
		IndexArrowTypeTag::UInt16 => encode_unsigned(
			array,
			row,
			tag,
			|a: &UInt16Array, row| a.value(row) as u64,
			2,
		),
		IndexArrowTypeTag::UInt32 => encode_unsigned(
			array,
			row,
			tag,
			|a: &UInt32Array, row| a.value(row) as u64,
			4,
		),
		IndexArrowTypeTag::UInt64 => {
			encode_unsigned(array, row, tag, |a: &UInt64Array, row| a.value(row), 8)
		}
		IndexArrowTypeTag::Date32 => encode_signed(
			array,
			row,
			tag,
			|a: &Date32Array, row| a.value(row) as i64,
			4,
		),
		IndexArrowTypeTag::Date64 => {
			encode_signed(array, row, tag, |a: &Date64Array, row| a.value(row), 8)
		}
		IndexArrowTypeTag::TimestampSecond => encode_signed(
			array,
			row,
			tag,
			|a: &TimestampSecondArray, row| a.value(row),
			8,
		),
		IndexArrowTypeTag::TimestampMillisecond => encode_signed(
			array,
			row,
			tag,
			|a: &TimestampMillisecondArray, row| a.value(row),
			8,
		),
		IndexArrowTypeTag::TimestampMicrosecond => encode_signed(
			array,
			row,
			tag,
			|a: &TimestampMicrosecondArray, row| a.value(row),
			8,
		),
		IndexArrowTypeTag::TimestampNanosecond => encode_signed(
			array,
			row,
			tag,
			|a: &TimestampNanosecondArray, row| a.value(row),
			8,
		),
		IndexArrowTypeTag::Utf8 => {
			let array = downcast_array::<StringArray>(array, tag)?;
			Ok(Some(array.value(row).as_bytes().to_vec()))
		}
		IndexArrowTypeTag::LargeUtf8 => {
			let array = downcast_array::<LargeStringArray>(array, tag)?;
			Ok(Some(array.value(row).as_bytes().to_vec()))
		}
		IndexArrowTypeTag::Binary => {
			let array = downcast_array::<BinaryArray>(array, tag)?;
			Ok(Some(array.value(row).to_vec()))
		}
		IndexArrowTypeTag::LargeBinary => {
			let array = downcast_array::<LargeBinaryArray>(array, tag)?;
			Ok(Some(array.value(row).to_vec()))
		}
	}
}

fn downcast_array<'a, T: 'static>(
	array: &'a dyn Array,
	tag: IndexArrowTypeTag,
) -> std::io::Result<&'a T> {
	array.as_any().downcast_ref::<T>().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!("secondary index Arrow array does not match {tag:?}"),
		)
	})
}

fn encode_unsigned<T: 'static>(
	array: &dyn Array,
	row: usize,
	tag: IndexArrowTypeTag,
	value: impl FnOnce(&T, usize) -> u64,
	width: usize,
) -> std::io::Result<Option<Vec<u8>>> {
	let array = downcast_array::<T>(array, tag)?;
	Ok(Some(encode_unsigned_value(value(array, row), width)?))
}

fn encode_signed<T: 'static>(
	array: &dyn Array,
	row: usize,
	tag: IndexArrowTypeTag,
	value: impl FnOnce(&T, usize) -> i64,
	width: usize,
) -> std::io::Result<Option<Vec<u8>>> {
	let raw = value(downcast_array::<T>(array, tag)?, row);
	Ok(Some(encode_signed_value(raw, width)?))
}

fn encode_secondary_index_literal(
	tag: IndexArrowTypeTag,
	literal: &ColumnPredicateLiteralV1,
) -> std::io::Result<Option<Vec<u8>>> {
	match (tag, literal) {
		(IndexArrowTypeTag::Int8, ColumnPredicateLiteralV1::Int(value)) => {
			Ok((*value >= i8::MIN as i64 && *value <= i8::MAX as i64)
				.then(|| encode_signed_value(*value, 1))
				.transpose()?)
		}
		(IndexArrowTypeTag::Int16, ColumnPredicateLiteralV1::Int(value)) => {
			Ok((*value >= i16::MIN as i64 && *value <= i16::MAX as i64)
				.then(|| encode_signed_value(*value, 2))
				.transpose()?)
		}
		(
			IndexArrowTypeTag::Int32 | IndexArrowTypeTag::Date32,
			ColumnPredicateLiteralV1::Int(value),
		) => Ok((*value >= i32::MIN as i64 && *value <= i32::MAX as i64)
			.then(|| encode_signed_value(*value, 4))
			.transpose()?),
		(
			IndexArrowTypeTag::Int64 | IndexArrowTypeTag::Date64,
			ColumnPredicateLiteralV1::Int(value),
		) => Ok(Some(encode_signed_value(*value, 8)?)),
		(IndexArrowTypeTag::UInt8, ColumnPredicateLiteralV1::Int(value)) => Ok((*value >= 0
			&& *value <= u8::MAX as i64)
			.then(|| encode_unsigned_value(*value as u64, 1))
			.transpose()?),
		(IndexArrowTypeTag::UInt16, ColumnPredicateLiteralV1::Int(value)) => Ok((*value >= 0
			&& *value <= u16::MAX as i64)
			.then(|| encode_unsigned_value(*value as u64, 2))
			.transpose()?),
		(IndexArrowTypeTag::UInt32, ColumnPredicateLiteralV1::Int(value)) => Ok((*value >= 0
			&& *value <= u32::MAX as i64)
			.then(|| encode_unsigned_value(*value as u64, 4))
			.transpose()?),
		(IndexArrowTypeTag::UInt64, ColumnPredicateLiteralV1::Int(value)) => Ok((*value >= 0)
			.then(|| encode_unsigned_value(*value as u64, 8))
			.transpose()?),
		(
			IndexArrowTypeTag::TimestampSecond
			| IndexArrowTypeTag::TimestampMillisecond
			| IndexArrowTypeTag::TimestampMicrosecond
			| IndexArrowTypeTag::TimestampNanosecond,
			ColumnPredicateLiteralV1::Ts(value),
		) => Ok(Some(encode_signed_value(*value, 8)?)),
		(
			IndexArrowTypeTag::Utf8
			| IndexArrowTypeTag::LargeUtf8
			| IndexArrowTypeTag::Binary
			| IndexArrowTypeTag::LargeBinary,
			ColumnPredicateLiteralV1::Str(value),
		) => Ok(Some(value.clone())),
		_ => Ok(None),
	}
}

fn encode_unsigned_value(raw: u64, width: usize) -> std::io::Result<Vec<u8>> {
	if !matches!(width, 1 | 2 | 4 | 8) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"secondary index unsigned width must be 1, 2, 4, or 8 bytes",
		));
	}
	let bytes = raw.to_be_bytes();
	Ok(bytes[8 - width..].to_vec())
}

fn encode_signed_value(raw: i64, width: usize) -> std::io::Result<Vec<u8>> {
	let flipped = match width {
		1 => ((raw as i8) as u8 ^ 0x80) as u64,
		2 => ((raw as i16) as u16 ^ 0x8000) as u64,
		4 => ((raw as i32) as u32 ^ 0x8000_0000) as u64,
		8 => (raw as u64) ^ 0x8000_0000_0000_0000,
		_ => {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"secondary index signed width must be 1, 2, 4, or 8 bytes",
			));
		}
	};
	let bytes = flipped.to_be_bytes();
	Ok(bytes[8 - width..].to_vec())
}

fn range_lower_bound_cmp(
	left: &SecondaryIndexRangeBound,
	right: &SecondaryIndexRangeBound,
) -> Ordering {
	left.encoded_key
		.cmp(&right.encoded_key)
		.then_with(|| left.inclusive.cmp(&right.inclusive).reverse())
}

fn range_upper_bound_cmp(
	left: &SecondaryIndexRangeBound,
	right: &SecondaryIndexRangeBound,
) -> Ordering {
	left.encoded_key
		.cmp(&right.encoded_key)
		.then_with(|| left.inclusive.cmp(&right.inclusive))
}

fn encoded_key_satisfies_lower(encoded_key: &[u8], lower: Option<&IndexBound>) -> bool {
	let Some(lower) = lower else {
		return true;
	};
	match encoded_key.cmp(lower.encoded_key.as_slice()) {
		Ordering::Less => false,
		Ordering::Equal => lower.inclusive,
		Ordering::Greater => true,
	}
}

fn encoded_key_satisfies_upper(encoded_key: &[u8], upper: Option<&IndexBound>) -> bool {
	let Some(upper) = upper else {
		return true;
	};
	match encoded_key.cmp(upper.encoded_key.as_slice()) {
		Ordering::Less => true,
		Ordering::Equal => upper.inclusive,
		Ordering::Greater => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::pbc::{
		ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, ColumnPredicateV1, OP_EQ,
		OP_PUSH_COL, OP_PUSH_INT, Program,
	};
	use crate::schema::{
		IndexArrowTypeTag, IndexKeyEncoding, Locality, LocalityPolicy, PrimaryKeyDescriptorV1,
		RecordRowIndex, RowIdentityPolicy, SchemaEvolutionPolicyV1, SecondaryIndexKeyDescriptor,
		TableObjectId, TableSchemaCatalogV1, TableSchemaDescriptor, TableWriteAdmission,
	};
	use crate::storage::append_storage_synthetic_columns;
	use arrow::array::{Int32Array, Int64Array, UInt64Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;

	fn hlc(value: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: value,
			logical: 0,
			node_id: 1,
		}
	}

	fn table_with_generation(schema_generation: u64) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([0x10; 16]),
			schema_generation,
			"analytics",
			"events",
			&Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("age", DataType::Int64, true),
			]),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn table() -> TableSchemaDescriptor {
		table_with_generation(1)
	}

	fn descriptor() -> SecondaryIndexDescriptor {
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x52; 16]),
			table_object_id: TableObjectId::new([0x10; 16]),
			namespace: "analytics".to_owned(),
			index_name: "events_age_idx".to_owned(),
			table_schema_generation: 1,
			logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([0x51; 32]),
			key: SecondaryIndexKeyDescriptor {
				column_name: "age".to_owned(),
				column_ordinal: 1,
				arrow_type_tag: IndexArrowTypeTag::Int64,
				encoding: IndexKeyEncoding::OrderedScalar,
			},
		}
	}

	fn descriptor_for_table(table: &TableSchemaDescriptor) -> SecondaryIndexDescriptor {
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x52; 16]),
			table_object_id: table.table_object_id,
			namespace: table.namespace.clone(),
			index_name: "events_age_idx".to_owned(),
			table_schema_generation: table.schema_generation,
			logical_schema_fingerprint: table.logical_schema_fingerprint,
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

	fn encoded_i64(value: i64) -> Vec<u8> {
		((value as u64) ^ 0x8000_0000_0000_0000)
			.to_be_bytes()
			.to_vec()
	}

	fn pbc_eq_age(value: i64) -> Program {
		let mut bytecode = Vec::new();
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&1_u32.to_le_bytes());
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&value.to_le_bytes());
		bytecode.push(OP_EQ);
		Program::compile(&bytecode).unwrap()
	}

	fn location(record_row_index: RecordRowIndex, row_hlc: HybridTimestamp) -> RecordLocation {
		RecordLocation {
			slab_id: crate::schema::SlabId::new(9),
			offset: 128,
			length: 512,
			hlc: row_hlc,
			record_row_index,
			is_tombstone: false,
		}
	}

	fn indexed_batch(ages: Vec<Option<i64>>, row_hlc: HybridTimestamp) -> RecordBatch {
		let ids = Int64Array::from_iter_values(0..ages.len() as i64);
		let ages = Int64Array::from(ages);
		let batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("age", DataType::Int64, true),
			])),
			vec![Arc::new(ids), Arc::new(ages)],
		)
		.unwrap();
		append_storage_synthetic_columns(
			&batch,
			&(0..batch.num_rows()).collect::<Vec<_>>(),
			&TableWriteAdmission {
				canonical_primary_keys: vec![vec![1], vec![2]],
				key_identities: vec![Identity::new([1; 32]), Identity::new([2; 32])],
				localities: vec![Locality::new(11), Locality::new(12)],
			},
			row_hlc,
			Some(TableObjectId::new([0x10; 16])),
		)
		.unwrap()
	}

	#[test]
	fn signed_integer_encoding_preserves_numeric_order() {
		let array = Int32Array::from(vec![-2, -1, 0, 1, 2]);
		let encoded = (0..array.len())
			.map(|row| {
				encode_secondary_index_key_from_array(&array, row, IndexArrowTypeTag::Int32)
					.unwrap()
					.unwrap()
			})
			.collect::<Vec<_>>();
		let mut sorted = encoded.clone();
		sorted.sort();
		assert_eq!(encoded, sorted);
		assert_eq!(encoded[0], vec![0x7f, 0xff, 0xff, 0xfe]);
		assert_eq!(encoded[2], vec![0x80, 0x00, 0x00, 0x00]);
	}

	#[test]
	fn null_indexed_value_emits_no_key() {
		let array = UInt64Array::from(vec![Some(7), None]);
		assert_eq!(
			encode_secondary_index_key_from_array(&array, 0, IndexArrowTypeTag::UInt64)
				.unwrap()
				.unwrap(),
			7_u64.to_be_bytes().to_vec()
		);
		assert!(
			encode_secondary_index_key_from_array(&array, 1, IndexArrowTypeTag::UInt64)
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn pbc_equality_predicate_extracts_encoded_index_key() {
		let descriptor = descriptor();
		let plan = secondary_index_predicate_from_pbc(
			&descriptor,
			&[ColumnPredicateV1 {
				column_index: 1,
				comparison: ColumnPredicateComparisonV1::Eq,
				literal: ColumnPredicateLiteralV1::Int(42),
			}],
		)
		.unwrap();
		assert_eq!(
			plan,
			Some(SecondaryIndexScanPredicate::Equal(
				((42_i64 as u64) ^ 0x8000_0000_0000_0000)
					.to_be_bytes()
					.to_vec()
			))
		);
	}

	#[test]
	fn pbc_range_predicates_tighten_to_encoded_bounds() {
		let descriptor = descriptor();
		let plan = secondary_index_predicate_from_pbc(
			&descriptor,
			&[
				ColumnPredicateV1 {
					column_index: 1,
					comparison: ColumnPredicateComparisonV1::Gte,
					literal: ColumnPredicateLiteralV1::Int(10),
				},
				ColumnPredicateV1 {
					column_index: 1,
					comparison: ColumnPredicateComparisonV1::Lt,
					literal: ColumnPredicateLiteralV1::Int(20),
				},
			],
		)
		.unwrap();
		assert_eq!(
			plan,
			Some(SecondaryIndexScanPredicate::Range {
				lower: Some(SecondaryIndexRangeBound {
					encoded_key: ((10_i64 as u64) ^ 0x8000_0000_0000_0000)
						.to_be_bytes()
						.to_vec(),
					inclusive: true,
				}),
				upper: Some(SecondaryIndexRangeBound {
					encoded_key: ((20_i64 as u64) ^ 0x8000_0000_0000_0000)
						.to_be_bytes()
						.to_vec(),
					inclusive: false,
				}),
			})
		);
	}

	#[test]
	fn pbc_in_list_predicate_deduplicates_encoded_keys() {
		let mut descriptor = descriptor();
		descriptor.key.arrow_type_tag = IndexArrowTypeTag::Utf8;
		let plan = secondary_index_predicate_from_pbc(
			&descriptor,
			&[ColumnPredicateV1 {
				column_index: 1,
				comparison: ColumnPredicateComparisonV1::InList,
				literal: ColumnPredicateLiteralV1::List(vec![
					ColumnPredicateLiteralV1::Str(b"beta".to_vec()),
					ColumnPredicateLiteralV1::Str(b"alpha".to_vec()),
					ColumnPredicateLiteralV1::Str(b"alpha".to_vec()),
				]),
			}],
		)
		.unwrap();
		assert_eq!(
			plan,
			Some(SecondaryIndexScanPredicate::InList(vec![
				b"alpha".to_vec(),
				b"beta".to_vec()
			]))
		);
	}

	#[test]
	fn access_plan_uses_ready_index_from_pbc_program() {
		let tenant_id = TenantId::new(7);
		let table = table();
		let descriptor = descriptor_for_table(&table);
		let mut tables = TableSchemaCatalogV1::default();
		tables.upsert(tenant_id, table.clone()).unwrap();
		let mut catalog = SecondaryIndexCatalog::default();
		catalog
			.create(tenant_id, descriptor.clone(), &tables)
			.unwrap();
		let program = pbc_eq_age(42);

		assert_eq!(
			secondary_index_access_plan_from_program(&catalog, tenant_id, &table, &program)
				.unwrap(),
			None
		);

		catalog
			.mark_backfill_complete(tenant_id, descriptor.index_object_id)
			.unwrap();
		let plan = secondary_index_access_plan_from_program(&catalog, tenant_id, &table, &program)
			.unwrap()
			.unwrap();
		assert_eq!(plan.descriptor, descriptor);
		assert_eq!(
			plan.query,
			IndexQuery::PointEqual {
				encoded_key: encoded_i64(42),
			}
		);
	}

	#[test]
	fn access_plan_skips_stale_and_dropping_indexes() {
		let tenant_id = TenantId::new(7);
		let table = table();
		let descriptor = descriptor_for_table(&table);
		let mut tables = TableSchemaCatalogV1::default();
		tables.upsert(tenant_id, table.clone()).unwrap();
		let mut catalog = SecondaryIndexCatalog::default();
		catalog
			.create(tenant_id, descriptor.clone(), &tables)
			.unwrap();
		catalog
			.mark_backfill_complete(tenant_id, descriptor.index_object_id)
			.unwrap();
		let program = pbc_eq_age(42);

		let stale_table = table_with_generation(2);
		assert_eq!(
			secondary_index_access_plan_from_program(&catalog, tenant_id, &stale_table, &program)
				.unwrap(),
			None
		);

		catalog
			.mark_dropping(tenant_id, descriptor.index_object_id)
			.unwrap();
		assert_eq!(
			secondary_index_access_plan_from_program(&catalog, tenant_id, &table, &program)
				.unwrap(),
			None
		);
	}

	#[test]
	fn index_query_matches_entries_and_segment_key_ranges() {
		let row_hlc = hlc(10);
		let entry_10 = SecondaryIndexEntry {
			encoded_key: ((10_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec(),
			row_key_id: Identity::new([1; 32]),
			row_hlc,
			record_location: location(RecordRowIndex::ZERO, row_hlc),
		};
		let entry_20 = SecondaryIndexEntry {
			encoded_key: ((20_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec(),
			row_key_id: Identity::new([2; 32]),
			row_hlc,
			record_location: location(RecordRowIndex::ZERO, row_hlc),
		};
		let range = IndexQuery::Range {
			lower: Some(IndexBound {
				encoded_key: entry_10.encoded_key.clone(),
				inclusive: true,
			}),
			upper: Some(IndexBound {
				encoded_key: entry_20.encoded_key.clone(),
				inclusive: false,
			}),
		};
		assert!(secondary_index_entry_matches_query(&entry_10, &range));
		assert!(!secondary_index_entry_matches_query(&entry_20, &range));
		assert!(secondary_index_key_range_intersects_query(
			2,
			&entry_10.encoded_key,
			&entry_20.encoded_key,
			&range
		));

		let point = IndexQuery::PointEqual {
			encoded_key: ((30_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec(),
		};
		assert!(!secondary_index_key_range_intersects_query(
			2,
			&entry_10.encoded_key,
			&entry_20.encoded_key,
			&point
		));
	}

	#[test]
	fn pbc_unsupported_literal_routes_to_scan() {
		let descriptor = descriptor();
		let plan = secondary_index_predicate_from_pbc(
			&descriptor,
			&[ColumnPredicateV1 {
				column_index: 1,
				comparison: ColumnPredicateComparisonV1::Eq,
				literal: ColumnPredicateLiteralV1::Float(42.0),
			}],
		)
		.unwrap();
		assert_eq!(plan, None);
	}

	#[test]
	fn candidate_revalidation_accepts_visible_matching_version_entry() {
		let descriptor = descriptor();
		let row_hlc = hlc(10);
		let batch = indexed_batch(vec![Some(41), Some(42)], row_hlc);
		let record_location = location(RecordRowIndex::new(1), row_hlc);
		let entry = SecondaryIndexEntry {
			encoded_key: encode_signed(
				batch.column(1).as_ref(),
				1,
				IndexArrowTypeTag::Int64,
				|array: &Int64Array, row| array.value(row),
				8,
			)
			.unwrap()
			.unwrap(),
			row_key_id: Identity::new([2; 32]),
			row_hlc,
			record_location,
		};

		let result = revalidate_secondary_index_candidate(
			&descriptor,
			segment_identity(&descriptor),
			&entry,
			&batch,
			Some(record_location),
		)
		.unwrap();
		assert_eq!(result, SecondaryIndexCandidateValidation::Accepted);
	}

	#[test]
	fn candidate_revalidation_rejects_stale_indexed_value() {
		let descriptor = descriptor();
		let row_hlc = hlc(10);
		let batch = indexed_batch(vec![Some(41), Some(42)], row_hlc);
		let record_location = location(RecordRowIndex::new(1), row_hlc);
		let entry = SecondaryIndexEntry {
			encoded_key: ((41_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec(),
			row_key_id: Identity::new([2; 32]),
			row_hlc,
			record_location,
		};

		let result = revalidate_secondary_index_candidate(
			&descriptor,
			segment_identity(&descriptor),
			&entry,
			&batch,
			Some(record_location),
		)
		.unwrap();
		assert_eq!(
			result,
			SecondaryIndexCandidateValidation::Rejected(
				SecondaryIndexCandidateRejectReason::IndexedValueMismatch
			)
		);
	}

	#[test]
	fn candidate_revalidation_rejects_row_metadata_mismatch() {
		let descriptor = descriptor();
		let row_hlc = hlc(10);
		let batch = indexed_batch(vec![Some(41), Some(42)], row_hlc);
		let record_location = location(RecordRowIndex::new(1), row_hlc);
		let entry = SecondaryIndexEntry {
			encoded_key: ((42_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec(),
			row_key_id: Identity::new([9; 32]),
			row_hlc,
			record_location,
		};

		let result = revalidate_secondary_index_candidate(
			&descriptor,
			segment_identity(&descriptor),
			&entry,
			&batch,
			Some(record_location),
		)
		.unwrap();
		assert_eq!(
			result,
			SecondaryIndexCandidateValidation::Rejected(
				SecondaryIndexCandidateRejectReason::RowMetadataMismatch
			)
		);
	}

	#[test]
	fn candidate_revalidation_rejects_null_indexed_value() {
		let descriptor = descriptor();
		let row_hlc = hlc(10);
		let batch = indexed_batch(vec![Some(41), None], row_hlc);
		let record_location = location(RecordRowIndex::new(1), row_hlc);
		let entry = SecondaryIndexEntry {
			encoded_key: vec![0],
			row_key_id: Identity::new([2; 32]),
			row_hlc,
			record_location,
		};

		let result = revalidate_secondary_index_candidate(
			&descriptor,
			segment_identity(&descriptor),
			&entry,
			&batch,
			Some(record_location),
		)
		.unwrap();
		assert_eq!(
			result,
			SecondaryIndexCandidateValidation::Rejected(
				SecondaryIndexCandidateRejectReason::NullIndexedValue
			)
		);
	}

	#[test]
	fn storage_batch_entry_builder_emits_non_null_data_rows_only() {
		let descriptor = descriptor();
		let row_hlc = hlc(10);
		let batch = indexed_batch(vec![Some(41), None], row_hlc);
		let entries = secondary_index_entries_from_storage_batch(
			&descriptor,
			&batch,
			location(RecordRowIndex::ZERO, row_hlc),
		)
		.unwrap();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].row_key_id, Identity::new([1; 32]));
		assert_eq!(entries[0].row_hlc, row_hlc);
		assert_eq!(
			entries[0].record_location.record_row_index,
			RecordRowIndex::ZERO
		);
		assert_eq!(
			entries[0].encoded_key,
			((41_i64 as u64) ^ 0x8000_0000_0000_0000)
				.to_be_bytes()
				.to_vec()
		);
	}
}
