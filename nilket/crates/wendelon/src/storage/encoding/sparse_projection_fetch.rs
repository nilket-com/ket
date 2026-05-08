use crate::schema::{
	EncodingPageIndex, IpcBufferIndex, IpcColumnOrdinal, RowGroupIndex, RowGroupRowCount,
};
use crate::storage::columnar::{ColumnFetchRequirement, RecordBufferDirectory};
use crate::storage::encoding::materialize::find_buffer_encoding;
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::page_materialize::{
	PageMaterializeColumn, PageMaterializeFetch, encoded_page_body_range,
	fixed_width_non_null_value_column, fixed_width_value_bytes,
	required_projected_physical_ordinals,
};
use crate::storage::encoding::{
	BufferEncoding, EncodingCascade, EncodingDirectory, EncodingPageDescriptor, EncodingRejectCode,
	L2EncodingFetchConfig,
};
use crate::storage::row_group::{
	L2RowGroupFetchConfig, PhaseBOutcome, RecordBodyByteRange, RecordPageDirectory,
};
use arrow::array::{Array, BooleanArray};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const MAX_SPARSE_RANGES_PER_BUFFER: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowToPageMap {
	pub pages: Vec<EncodingPageDescriptor>,
	pub row_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ActivePageSet {
	None,
	All,
	Sparse(Vec<EncodingPageIndex>),
}

#[derive(Clone, Debug)]
pub(crate) struct SparseProjectionFetchPlan {
	pub selected_groups: Vec<RowGroupIndex>,
	pub group_pages: Vec<SparseProjectionFetchGroup>,
	pub columns: Vec<PageMaterializeColumn>,
	pub fetches: Vec<PageMaterializeFetch>,
	pub selected_body_ranges: Vec<RecordBodyByteRange>,
}

#[derive(Clone, Debug)]
pub(crate) struct SparseProjectionFetchGroup {
	pub row_group_index: RowGroupIndex,
	pub selected_pages: Vec<EncodingPageDescriptor>,
}

#[derive(Clone, Debug)]
pub(crate) enum SparseProjectionFetchDecision {
	Sparse(SparseProjectionFetchPlan),
	Decline(SparseProjectionFetchDecline),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SparseProjectionFetchDecline {
	AllRowsMask,
	MissingExactCocdMask,
	DensePredicateBatchPresent,
	CascadeNotPageEmitting,
	CascadeNotProjectionEligible,
	NullableProjectedColumn,
	VariableWidthProjectedColumn,
	PageGeometryMismatchAcrossColumns,
	ByteEstimateNotSparse,
	SimdPageCapExceeded,
	RangeCapExceeded,
	PerBufferFragmentationCapExceeded,
	SelectedByteFractionExceeded,
	Disabled,
}

pub(crate) struct SparseProjectionFetchInput<'a> {
	pub record_directory: &'a RecordBufferDirectory,
	pub page_directory: &'a RecordPageDirectory,
	pub encoding_directory: &'a EncodingDirectory,
	pub requirement: &'a ColumnFetchRequirement,
	pub predicate_outcome: &'a PhaseBOutcome,
	pub selected_groups: &'a [RowGroupIndex],
	pub dense_body_ranges: &'a [RecordBodyByteRange],
	pub row_group_config: &'a L2RowGroupFetchConfig,
	pub encoding_config: &'a L2EncodingFetchConfig,
	pub dense_predicate_batches_present: bool,
}

impl RowToPageMap {
	pub(crate) fn from_buffer_encoding(
		encoding: &BufferEncoding,
	) -> Result<Self, EncodingRejectCode> {
		let pages = expand_page_descriptors(encoding)?;
		Ok(Self {
			row_count: encoding.decoded_value_count.get(),
			pages,
		})
	}

	pub(crate) fn page_for_row(&self, row: u32) -> Option<EncodingPageDescriptor> {
		self.pages.iter().copied().find(|page| {
			let start = page.first_value_index.get();
			let end = start.saturating_add(page.value_count.get());
			row >= start && row < end
		})
	}
}

pub(crate) fn decide_sparse_projection_fetch(
	input: SparseProjectionFetchInput<'_>,
) -> SparseProjectionFetchDecision {
	if !input.row_group_config.enabled || !input.encoding_config.enabled {
		return SparseProjectionFetchDecision::Decline(SparseProjectionFetchDecline::Disabled);
	}
	if input.dense_predicate_batches_present {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::DensePredicateBatchPresent,
		);
	}
	let Some(physical_ordinals) =
		required_projected_physical_ordinals(input.record_directory, input.requirement)
	else {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::VariableWidthProjectedColumn,
		);
	};
	if physical_ordinals.is_empty() {
		return SparseProjectionFetchDecision::Sparse(SparseProjectionFetchPlan {
			selected_groups: input.selected_groups.to_vec(),
			group_pages: Vec::new(),
			columns: Vec::new(),
			fetches: Vec::new(),
			selected_body_ranges: Vec::new(),
		});
	}

	let mut active_masks = BTreeMap::<RowGroupIndex, &'_ BooleanArray>::new();
	for group_index in input.selected_groups {
		let Some(mask) = input.predicate_outcome.survival_masks.get(group_index) else {
			return SparseProjectionFetchDecision::Decline(
				SparseProjectionFetchDecline::MissingExactCocdMask,
			);
		};
		if mask.true_count() == 0 {
			continue;
		}
		if mask.null_count() == 0 && mask.true_count() == mask.len() {
			return SparseProjectionFetchDecision::Decline(
				SparseProjectionFetchDecline::AllRowsMask,
			);
		}
		active_masks.insert(*group_index, mask);
	}
	if active_masks.is_empty() {
		return SparseProjectionFetchDecision::Sparse(SparseProjectionFetchPlan {
			selected_groups: Vec::new(),
			group_pages: Vec::new(),
			columns: Vec::new(),
			fetches: Vec::new(),
			selected_body_ranges: Vec::new(),
		});
	}

	let mut columns = Vec::new();
	for physical_ordinal in physical_ordinals {
		let column = match projection_column_candidate(input.record_directory, physical_ordinal) {
			Ok(column) => column,
			Err(reason) => return SparseProjectionFetchDecision::Decline(reason),
		};
		columns.push(column);
	}

	let mut selected_groups = Vec::new();
	let mut group_pages = Vec::new();
	let mut fetches = Vec::new();
	let mut per_buffer_ranges =
		BTreeMap::<(RowGroupIndex, IpcColumnOrdinal, IpcBufferIndex), usize>::new();
	for (group_index, mask) in active_masks {
		let Some(group) = input
			.page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == group_index)
		else {
			return SparseProjectionFetchDecision::Decline(
				SparseProjectionFetchDecline::PageGeometryMismatchAcrossColumns,
			);
		};
		let mut expected_pages: Option<Vec<EncodingPageDescriptor>> = None;
		let mut column_pages = Vec::new();
		for column in &columns {
			let Some((buffer_page, encoding, cascade, pages)) =
				projection_column_pages(&input, group_index, group.row_count, column, mask)
			else {
				return SparseProjectionFetchDecision::Decline(
					SparseProjectionFetchDecline::CascadeNotPageEmitting,
				);
			};
			if !projection_cascade_eligible(cascade) {
				return SparseProjectionFetchDecision::Decline(
					SparseProjectionFetchDecline::CascadeNotProjectionEligible,
				);
			}
			if let Some(expected) = &expected_pages {
				if !same_page_geometry(expected, &pages) {
					return SparseProjectionFetchDecision::Decline(
						SparseProjectionFetchDecline::PageGeometryMismatchAcrossColumns,
					);
				}
			} else {
				expected_pages = Some(pages.clone());
			}
			column_pages.push((column, buffer_page, encoding, cascade, pages));
		}
		let pages = expected_pages.unwrap_or_default();
		if pages.is_empty() {
			continue;
		}
		selected_groups.push(group_index);
		group_pages.push(SparseProjectionFetchGroup {
			row_group_index: group_index,
			selected_pages: pages.clone(),
		});
		for (column, buffer_page, encoding, cascade, pages) in column_pages {
			let per_buffer_key = (
				group_index,
				buffer_page.ipc_column_ordinal,
				buffer_page.ipc_buffer_ordinal,
			);
			let counter = per_buffer_ranges.entry(per_buffer_key).or_insert(0);
			*counter = counter.saturating_add(pages.len());
			if *counter > MAX_SPARSE_RANGES_PER_BUFFER {
				return SparseProjectionFetchDecision::Decline(
					SparseProjectionFetchDecline::PerBufferFragmentationCapExceeded,
				);
			}
			for page in pages {
				fetches.push(PageMaterializeFetch {
					key: crate::storage::encoding::page_materialize::PageMaterializeBufferKey {
						row_group_index: group_index,
						ipc_column_ordinal: buffer_page.ipc_column_ordinal,
						ipc_buffer_ordinal: buffer_page.ipc_buffer_ordinal,
						page_index: page.page_index.get(),
					},
					body_range: encoded_page_body_range(buffer_page.range, encoding, page),
					cascade: cascade.clone(),
					page,
					bytes_per_value: column.bytes_per_value,
					data_type: column.data_type.clone(),
				});
			}
		}
	}

	if fetches.len() > input.encoding_config.max_simd_pages_per_record.get() as usize {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::SimdPageCapExceeded,
		);
	}
	let mut selected_body_ranges = fetches
		.iter()
		.map(|fetch| fetch.body_range)
		.collect::<Vec<_>>();
	if coalesce_record_body_ranges(&mut selected_body_ranges).is_err() {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::PageGeometryMismatchAcrossColumns,
		);
	}
	if selected_body_ranges.len() > input.row_group_config.max_ranges_per_record.get() as usize {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::RangeCapExceeded,
		);
	}
	let sparse_bytes = selected_body_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	let selected_byte_scaled = sparse_bytes.saturating_mul(u64::from(
		crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
	));
	let allowed_byte_scaled = input
		.record_directory
		.record_plaintext_range
		.len
		.saturating_mul(u64::from(
			input.row_group_config.max_selected_byte_fraction_micros,
		));
	if selected_byte_scaled > allowed_byte_scaled {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::SelectedByteFractionExceeded,
		);
	}
	let dense_bytes = input
		.dense_body_ranges
		.iter()
		.map(|range| range.len.get())
		.sum::<u64>();
	if sparse_bytes >= dense_bytes {
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::ByteEstimateNotSparse,
		);
	}
	SparseProjectionFetchDecision::Sparse(SparseProjectionFetchPlan {
		selected_groups,
		group_pages,
		columns,
		fetches,
		selected_body_ranges,
	})
}

fn projection_column_pages<'a>(
	input: &'a SparseProjectionFetchInput<'_>,
	group_index: RowGroupIndex,
	group_row_count: RowGroupRowCount,
	column: &PageMaterializeColumn,
	mask: &BooleanArray,
) -> Option<(
	&'a crate::storage::row_group::BufferPageDirectoryEntry,
	&'a BufferEncoding,
	&'a EncodingCascade,
	Vec<EncodingPageDescriptor>,
)> {
	let buffer_page = input.page_directory.buffer_pages.iter().find(|page| {
		page.row_group_index == group_index
			&& page.ipc_column_ordinal == IpcColumnOrdinal::new(column.physical_ordinal as u32)
			&& page.ipc_buffer_ordinal == column.value_buffer_ordinal
			&& page.range.len.get() > 0
	})?;
	let encoding = find_buffer_encoding(
		input.encoding_directory,
		group_index,
		column.value_buffer_ordinal,
		buffer_page.range,
	)?;
	if encoding.decoded_value_count.get() != group_row_count.get()
		|| encoding.page_end_offsets.is_empty()
	{
		return None;
	}
	let cascade = input
		.encoding_directory
		.cascades
		.get(encoding.cascade_index.get() as usize)?;
	if !cascade.emits_pages_in_v1() {
		return None;
	}
	let map = RowToPageMap::from_buffer_encoding(encoding).ok()?;
	let active = mask_to_active_pages(mask, &map)?;
	let ActivePageSet::Sparse(page_indices) = active else {
		return None;
	};
	let mut pages = Vec::with_capacity(page_indices.len());
	for page_index in page_indices {
		pages.push(
			map.pages
				.iter()
				.copied()
				.find(|page| page.page_index == page_index)?,
		);
	}
	Some((buffer_page, encoding, cascade, pages))
}

fn projection_column_candidate(
	record_directory: &RecordBufferDirectory,
	physical_ordinal: usize,
) -> Result<PageMaterializeColumn, SparseProjectionFetchDecline> {
	let Some(field) = record_directory.ipc_schema.fields().get(physical_ordinal) else {
		return Err(SparseProjectionFetchDecline::PageGeometryMismatchAcrossColumns);
	};
	if record_directory
		.column_null_counts
		.get(physical_ordinal)
		.copied()
		.unwrap_or(usize::MAX)
		!= 0
	{
		return Err(SparseProjectionFetchDecline::NullableProjectedColumn);
	}
	if fixed_width_value_bytes(field.data_type()).is_none() {
		return Err(SparseProjectionFetchDecline::VariableWidthProjectedColumn);
	}
	fixed_width_non_null_value_column(record_directory, physical_ordinal)
		.ok_or(SparseProjectionFetchDecline::PageGeometryMismatchAcrossColumns)
}

pub(crate) fn mask_to_active_pages(
	mask: &BooleanArray,
	map: &RowToPageMap,
) -> Option<ActivePageSet> {
	if mask.len() != map.row_count as usize {
		return None;
	}
	if mask.true_count() == 0 {
		return Some(ActivePageSet::None);
	}
	if mask.null_count() == 0 && mask.true_count() == mask.len() {
		return Some(ActivePageSet::All);
	}
	let mut pages = BTreeSet::new();
	for row in 0..mask.len() {
		if mask.is_valid(row) && mask.value(row) {
			pages.insert(map.page_for_row(row as u32)?.page_index);
		}
	}
	if pages.is_empty() {
		Some(ActivePageSet::None)
	} else {
		Some(ActivePageSet::Sparse(pages.into_iter().collect()))
	}
}

fn same_page_geometry(left: &[EncodingPageDescriptor], right: &[EncodingPageDescriptor]) -> bool {
	left.len() == right.len()
		&& left.iter().zip(right.iter()).all(|(left, right)| {
			left.page_index == right.page_index
				&& left.first_value_index == right.first_value_index
				&& left.value_count == right.value_count
		})
}

fn projection_cascade_eligible(cascade: &EncodingCascade) -> bool {
	match cascade {
		EncodingCascade::BitPacked { .. } => true,
		EncodingCascade::Dictionary { inner, .. } => matches!(
			inner.as_ref(),
			crate::storage::encoding::EncodingLeafCascade::Plain
				| crate::storage::encoding::EncodingLeafCascade::BitPacked { .. }
		),
		_ => false,
	}
}

fn coalesce_record_body_ranges(ranges: &mut Vec<RecordBodyByteRange>) -> std::io::Result<()> {
	ranges.sort_by_key(|range| range.start.get());
	let mut coalesced = Vec::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		if range.len.get() == 0 {
			continue;
		}
		let Some(last): Option<&mut RecordBodyByteRange> = coalesced.last_mut() else {
			coalesced.push(range);
			continue;
		};
		let last_end = last.end()?;
		let range_end = range.end()?;
		if range.start.get() <= last_end {
			last.len =
				crate::schema::RecordBodyByteLen::new(range_end.max(last_end) - last.start.get());
		} else {
			coalesced.push(range);
		}
	}
	*ranges = coalesced;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		BaseValue, BitsPerValue, BufferValueCount, EncodingCascadeIndex, EncodingPageByteOffset,
		EncodingPageCount, EncryptedChunkCount, IpcBufferIndex, IpcBufferLength, IpcBufferOffset,
		LogicalSchemaFingerprint, RecordBodyByteLen, RecordBodyByteOffset,
		RecordBodyCompressionPolicy, RecordRowIndex, RowCount, RunCount, SimdPageValueCount,
		SlabId,
	};
	use crate::storage::columnar::{
		ColumnBufferRange, ColumnOrdinalSet, IpcBufferRange, PlaintextByteRange, TieredRecordId,
	};
	use crate::storage::encoding::{EncodedBufferByteRange, EncodingLeafCascade};
	use crate::storage::row_group::{BufferPageDirectoryEntry, RowGroupDirectoryEntry};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::HashMap;

	const VALUE_BUFFER_BODY_START: u64 = 256;

	struct PlannerFixture {
		record_directory: RecordBufferDirectory,
		page_directory: RecordPageDirectory,
		encoding_directory: EncodingDirectory,
		row_group_config: L2RowGroupFetchConfig,
		encoding_config: L2EncodingFetchConfig,
	}

	impl PlannerFixture {
		fn new(
			row_count: u32,
			page_value_count: u32,
			page_byte_len: u32,
			column_null_count: usize,
		) -> Self {
			Self::new_with_data_type(
				row_count,
				page_value_count,
				page_byte_len,
				column_null_count,
				DataType::Int64,
			)
		}

		fn new_with_data_type(
			row_count: u32,
			page_value_count: u32,
			page_byte_len: u32,
			column_null_count: usize,
			data_type: DataType,
		) -> Self {
			let page_count = row_count.div_ceil(page_value_count);
			let total_encoded_bytes = u64::from(page_count.saturating_mul(page_byte_len));
			let record_id = TieredRecordId {
				slab_id: SlabId::new(60),
				record_offset: RecordBodyByteOffset::ZERO,
			};
			let schema = Schema::new(vec![Field::new("value", data_type, column_null_count != 0)]);
			let fingerprint = LogicalSchemaFingerprint::from_bytes([0x60; 32]);
			let record_directory = RecordBufferDirectory {
				record_id,
				record_plaintext_range: PlaintextByteRange {
					start: 0,
					len: 8 * 1024,
				},
				row_count: row_count as usize,
				record_body_compression: RecordBodyCompressionPolicy::Uncompressed,
				ipc_schema: schema,
				buffers: vec![
					IpcBufferRange {
						offset_in_record: IpcBufferOffset::ZERO,
						length: IpcBufferLength::ZERO,
					},
					IpcBufferRange {
						offset_in_record: IpcBufferOffset::new(VALUE_BUFFER_BODY_START),
						length: IpcBufferLength::new(total_encoded_bytes),
					},
				],
				column_to_buffers: vec![ColumnBufferRange {
					first_buffer_ordinal: IpcBufferIndex::ZERO,
					buffer_count: 2,
				}],
				column_null_counts: vec![column_null_count],
				storage_column_ordinals: BTreeMap::new(),
				logical_schema_fingerprint: fingerprint,
				page_directory: None,
				encoding_directory: None,
			};
			let page_directory = RecordPageDirectory {
				record_id,
				row_count: RowCount::new(u64::from(row_count)),
				row_groups: vec![RowGroupDirectoryEntry {
					row_group_index: RowGroupIndex::ZERO,
					first_record_row_index: RecordRowIndex::ZERO,
					row_count: RowGroupRowCount::new(row_count),
					metadata_range: RecordBodyByteRange {
						start: RecordBodyByteOffset::ZERO,
						len: RecordBodyByteLen::new(64),
					},
					body_range: RecordBodyByteRange {
						start: RecordBodyByteOffset::ZERO,
						len: RecordBodyByteLen::new(8 * 1024),
					},
				}],
				buffer_pages: vec![BufferPageDirectoryEntry {
					row_group_index: RowGroupIndex::ZERO,
					ipc_column_ordinal: IpcColumnOrdinal::ZERO,
					ipc_buffer_ordinal: IpcBufferIndex::new(1),
					range: RecordBodyByteRange {
						start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START),
						len: RecordBodyByteLen::new(total_encoded_bytes),
					},
				}],
				row_group_stats: Vec::new(),
				logical_schema_fingerprint: fingerprint,
			};
			let page_end_offsets = (1..=page_count)
				.map(|page| EncodingPageByteOffset::new(page.saturating_mul(page_byte_len)))
				.collect::<Vec<_>>();
			let encoding_directory = EncodingDirectory {
				record_id,
				row_count: RowCount::new(u64::from(row_count)),
				logical_schema_fingerprint: fingerprint,
				target_simd_page_values: SimdPageValueCount::new(page_value_count),
				cascades: vec![EncodingCascade::BitPacked {
					bits: BitsPerValue::new(8),
					base: BaseValue::new(0),
				}],
				buffers: vec![BufferEncoding {
					row_group_index: RowGroupIndex::ZERO,
					ipc_column_ordinal: IpcColumnOrdinal::ZERO,
					ipc_buffer_ordinal: IpcBufferIndex::new(1),
					cascade_index: EncodingCascadeIndex::ZERO,
					decoded_byte_len: RecordBodyByteLen::new(u64::from(row_count) * 8),
					decoded_value_count: BufferValueCount::new(row_count),
					encoded_range: EncodedBufferByteRange {
						start: EncodingPageByteOffset::ZERO,
						len: RecordBodyByteLen::new(total_encoded_bytes),
					},
					page_value_count: SimdPageValueCount::new(page_value_count),
					page_end_offsets,
				}],
			};
			Self {
				record_directory,
				page_directory,
				encoding_directory,
				row_group_config: L2RowGroupFetchConfig {
					enabled: true,
					max_ranges_per_record: EncryptedChunkCount::new(64),
					..L2RowGroupFetchConfig::default()
				},
				encoding_config: L2EncodingFetchConfig {
					enabled: true,
					max_simd_pages_per_record: EncodingPageCount::new(64),
					..L2EncodingFetchConfig::default()
				},
			}
		}

		fn decide(
			&self,
			requirement: &ColumnFetchRequirement,
			outcome: &PhaseBOutcome,
			dense_ranges: &[RecordBodyByteRange],
		) -> SparseProjectionFetchDecision {
			self.decide_with_dense_predicate_batches(requirement, outcome, dense_ranges, false)
		}

		fn decide_with_dense_predicate_batches(
			&self,
			requirement: &ColumnFetchRequirement,
			outcome: &PhaseBOutcome,
			dense_ranges: &[RecordBodyByteRange],
			dense_predicate_batches_present: bool,
		) -> SparseProjectionFetchDecision {
			decide_sparse_projection_fetch(SparseProjectionFetchInput {
				record_directory: &self.record_directory,
				page_directory: &self.page_directory,
				encoding_directory: &self.encoding_directory,
				requirement,
				predicate_outcome: outcome,
				selected_groups: &[RowGroupIndex::ZERO],
				dense_body_ranges: dense_ranges,
				row_group_config: &self.row_group_config,
				encoding_config: &self.encoding_config,
				dense_predicate_batches_present,
			})
		}
	}

	fn encoding(row_count: u32, page_values: u32, ends: &[u32]) -> BufferEncoding {
		BufferEncoding {
			row_group_index: RowGroupIndex::ZERO,
			ipc_column_ordinal: IpcColumnOrdinal::ZERO,
			ipc_buffer_ordinal: crate::schema::IpcBufferIndex::ZERO,
			cascade_index: crate::schema::EncodingCascadeIndex::ZERO,
			decoded_byte_len: RecordBodyByteLen::new(row_count as u64),
			decoded_value_count: BufferValueCount::new(row_count),
			encoded_range: EncodedBufferByteRange {
				start: EncodingPageByteOffset::ZERO,
				len: RecordBodyByteLen::new(*ends.last().unwrap_or(&0) as u64),
			},
			page_value_count: SimdPageValueCount::new(page_values),
			page_end_offsets: ends
				.iter()
				.copied()
				.map(EncodingPageByteOffset::new)
				.collect(),
		}
	}

	#[test]
	fn row_to_page_map_handles_partial_last_page() {
		let map = RowToPageMap::from_buffer_encoding(&encoding(10, 4, &[4, 8, 10])).unwrap();
		assert_eq!(map.page_for_row(0).unwrap().page_index.get(), 0);
		assert_eq!(map.page_for_row(4).unwrap().page_index.get(), 1);
		assert_eq!(map.page_for_row(8).unwrap().page_index.get(), 2);
		assert!(map.page_for_row(10).is_none());
	}

	#[test]
	fn boolean_mask_maps_to_active_pages() {
		let map = RowToPageMap::from_buffer_encoding(&encoding(10, 4, &[4, 8, 10])).unwrap();
		let mask = BooleanArray::from(vec![
			false, true, false, false, false, true, false, false, true, false,
		]);
		let ActivePageSet::Sparse(pages) = mask_to_active_pages(&mask, &map).unwrap() else {
			panic!("expected sparse pages");
		};
		assert_eq!(
			pages.into_iter().map(|page| page.get()).collect::<Vec<_>>(),
			vec![0, 1, 2]
		);
	}

	#[test]
	fn empty_boolean_mask_maps_to_none() {
		let map = RowToPageMap::from_buffer_encoding(&encoding(10, 4, &[4, 8, 10])).unwrap();
		let mask = BooleanArray::from(vec![false; 10]);
		assert_eq!(mask_to_active_pages(&mask, &map), Some(ActivePageSet::None));
	}

	fn output_requirement() -> ColumnFetchRequirement {
		ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([0]),
			..ColumnFetchRequirement::default()
		}
	}

	fn outcome(mask: BooleanArray) -> PhaseBOutcome {
		PhaseBOutcome {
			survival_masks: HashMap::from([(RowGroupIndex::ZERO, mask)]),
			..PhaseBOutcome::default()
		}
	}

	fn dense_range() -> RecordBodyByteRange {
		RecordBodyByteRange {
			start: RecordBodyByteOffset::ZERO,
			len: RecordBodyByteLen::new(4 * 1024),
		}
	}

	fn tiny_dense_range() -> RecordBodyByteRange {
		RecordBodyByteRange {
			start: RecordBodyByteOffset::ZERO,
			len: RecordBodyByteLen::new(1),
		}
	}

	fn assert_decline(
		decision: SparseProjectionFetchDecision,
		expected: SparseProjectionFetchDecline,
	) {
		let SparseProjectionFetchDecision::Decline(actual) = decision else {
			panic!("expected decline {expected:?}, got {decision:?}");
		};
		assert_eq!(actual, expected);
	}

	#[test]
	fn none_mask_short_circuits_before_projection_column_eligibility() {
		let fixture = PlannerFixture::new(16, 4, 32, 1);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from(vec![false; 16])),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected empty sparse plan, got {decision:?}");
		};
		assert!(plan.selected_groups.is_empty());
		assert!(plan.group_pages.is_empty());
		assert!(plan.fetches.is_empty());
		assert!(plan.selected_body_ranges.is_empty());
	}

	#[test]
	fn none_mask_short_circuits_before_non_page_emitting_cascade() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 0);
		fixture.encoding_directory.cascades[0] = EncodingCascade::Plain;
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from(vec![false; 16])),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected empty sparse plan, got {decision:?}");
		};
		assert!(plan.selected_groups.is_empty());
		assert!(plan.fetches.is_empty());
		assert!(plan.selected_body_ranges.is_empty());
	}

	#[test]
	fn all_rows_mask_declines_sparse_projection() {
		let fixture = PlannerFixture::new(16, 4, 32, 0);
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from(vec![true; 16])),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::AllRowsMask,
		);
	}

	#[test]
	fn missing_exact_mask_declines_sparse_projection() {
		let fixture = PlannerFixture::new(16, 4, 32, 0);
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&PhaseBOutcome::default(),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::MissingExactCocdMask,
		);
	}

	#[test]
	fn dense_predicate_batch_declines_sparse_projection() {
		let fixture = PlannerFixture::new(16, 4, 32, 0);
		assert_decline(
			fixture.decide_with_dense_predicate_batches(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
				true,
			),
			SparseProjectionFetchDecline::DensePredicateBatchPresent,
		);
	}

	#[test]
	fn disabled_row_group_or_encoding_fetch_declines_sparse_projection() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 0);
		fixture.row_group_config.enabled = false;
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::Disabled,
		);
	}

	#[test]
	fn nullable_active_projection_column_declines_sparse_projection() {
		let fixture = PlannerFixture::new(16, 4, 32, 1);
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::NullableProjectedColumn,
		);
	}

	#[test]
	fn variable_width_active_projection_column_declines_sparse_projection() {
		let fixture = PlannerFixture::new_with_data_type(16, 4, 32, 0, DataType::Utf8);
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::VariableWidthProjectedColumn,
		);
	}

	#[test]
	fn rle_projection_default_off_until_phase7_evidence() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 0);
		fixture.encoding_directory.cascades[0] = EncodingCascade::Rle {
			run_count: RunCount::new(1),
			inner: Box::new(EncodingLeafCascade::Plain),
		};
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::CascadeNotProjectionEligible,
		);
	}

	#[test]
	fn clustered_mask_selects_only_touched_projection_pages() {
		let fixture = PlannerFixture::new(16, 4, 32, 0);
		let mut mask = vec![false; 16];
		mask[5] = true;
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from(mask)),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected sparse projection plan, got {decision:?}");
		};
		assert_eq!(plan.selected_groups, vec![RowGroupIndex::ZERO]);
		assert_eq!(plan.group_pages.len(), 1);
		assert_eq!(plan.group_pages[0].selected_pages.len(), 1);
		assert_eq!(plan.group_pages[0].selected_pages[0].page_index.get(), 1);
		assert_eq!(plan.fetches.len(), 1);
		assert_eq!(plan.selected_body_ranges.len(), 1);
		assert_eq!(
			plan.selected_body_ranges[0],
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START + 32),
				len: RecordBodyByteLen::new(32),
			}
		);
	}

	#[test]
	fn sparse_byte_estimate_must_beat_dense_projection_bytes() {
		let fixture = PlannerFixture::new(16, 4, 32, 0);
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[tiny_dense_range()],
			),
			SparseProjectionFetchDecline::ByteEstimateNotSparse,
		);
	}

	#[test]
	fn selected_simd_page_cap_declines_sparse_projection() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 0);
		fixture.encoding_config.max_simd_pages_per_record = EncodingPageCount::new(1);
		let mut mask = vec![false; 16];
		mask[0] = true;
		mask[4] = true;
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from(mask)),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::SimdPageCapExceeded,
		);
	}

	#[test]
	fn coalesced_range_cap_declines_sparse_projection() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 0);
		fixture.row_group_config.max_ranges_per_record = EncryptedChunkCount::new(1);
		let mut mask = vec![false; 16];
		mask[0] = true;
		mask[8] = true;
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from(mask)),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::RangeCapExceeded,
		);
	}

	#[test]
	fn selected_byte_fraction_cap_declines_sparse_projection() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 0);
		fixture.row_group_config.max_selected_byte_fraction_micros = 1;
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::SelectedByteFractionExceeded,
		);
	}

	#[test]
	fn fragmented_mask_declines_before_tiered_range_planning() {
		let fixture = PlannerFixture::new(10, 1, 8, 0);
		let mut mask = vec![true; 10];
		mask[9] = false;
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from(mask)),
			&[dense_range()],
		);
		assert!(matches!(
			decision,
			SparseProjectionFetchDecision::Decline(
				SparseProjectionFetchDecline::PerBufferFragmentationCapExceeded
			)
		));
	}
}
