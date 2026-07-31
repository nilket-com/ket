use crate::schema::{
	EncodingPageIndex, IpcBufferIndex, IpcColumnOrdinal, RowGroupIndex, RowGroupRowCount,
};
use crate::storage::columnar::{ColumnFetchRequirement, RecordBufferDirectory};
use crate::storage::encoding::materialize::{
	encoded_validity_cascade_supported, find_buffer_encoding,
};
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::page_materialize::{
	PageMaterializeBufferKey, PageMaterializeColumn, PageMaterializeFetch,
	PageMaterializeValidityFetch, PageMaterializeValiditySource, encoded_page_body_range,
	fixed_width_sparse_projection_column, fixed_width_value_bytes,
	required_projected_physical_ordinals, validity_bitmap_body_range_for_page,
};
use crate::storage::encoding::{
	BufferEncoding, EncodingCascade, EncodingDirectory, EncodingPageDescriptor, EncodingRejectCode,
	L2EncodingFetchConfig,
};
use crate::storage::row_group::{
	L2RowGroupFetchConfig, PhaseBOutcome, RecordBodyByteRange, RecordPageDirectory,
};
use arrow::array::{Array, BooleanArray};
use arrow::datatypes::{DataType, TimeUnit};
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
	pub validity_fetches: Vec<PageMaterializeValidityFetch>,
	pub value_page_dispositions: BTreeMap<PageMaterializeBufferKey, SelectedPageDisposition>,
	pub all_valid_nullable_pages: BTreeSet<PageMaterializeBufferKey>,
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
	CascadeNotPageEmitting,
	CascadeNotProjectionEligible,
	EncodedValidityBufferUnsupported,
	ValidityBufferRangeMissing,
	NullableTypeNotInCatalog,
	VariableWidthProjectedColumn,
	PageGeometryMismatchAcrossColumns,
	ByteEstimateNotSparse,
	SimdPageCapExceeded,
	RangeCapExceeded,
	PerBufferFragmentationCapExceeded,
	SelectedByteFractionExceeded,
	Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SelectedPageDisposition {
	Materialize,
	AllNullElided {
		page_row_count: u32,
		bytes_per_value: usize,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AllNullElisionDeclineReason {
	NotNullable,
	ColumnHasNoNulls,
	DataTypeNotInCatalog,
	MissingPageStats,
	PageStatsNotAllNull,
	CorruptPageDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EncodedValidityDeclineReason {
	UnsupportedCascade,
	MissingPageMetadata,
	PageGeometryMismatch,
	RowGroupRowCountMismatch,
	DecodedByteLenMismatch,
	CorruptPageDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EncodedValiditySparseEligibility {
	Eligible,
	Decline(EncodedValidityDeclineReason),
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
			validity_fetches: Vec::new(),
			value_page_dispositions: BTreeMap::new(),
			all_valid_nullable_pages: BTreeSet::new(),
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
			validity_fetches: Vec::new(),
			value_page_dispositions: BTreeMap::new(),
			all_valid_nullable_pages: BTreeSet::new(),
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
	let mut validity_fetches = Vec::new();
	let mut value_page_dispositions = BTreeMap::new();
	let mut all_valid_nullable_pages = BTreeSet::new();
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
			let may_have_nulls = column.nullable
				&& row_group_column_may_have_nulls(
					input.record_directory,
					input.page_directory,
					group_index,
					column,
				);
			let mut materialize_pages = Vec::with_capacity(pages.len());
			for page in pages {
				let value_key = PageMaterializeBufferKey {
					row_group_index: group_index,
					ipc_column_ordinal: buffer_page.ipc_column_ordinal,
					ipc_buffer_ordinal: buffer_page.ipc_buffer_ordinal,
					page_index: page.page_index.get(),
				};
				if all_null_elision_decline_reason(column, may_have_nulls, encoding, page).is_none()
				{
					value_page_dispositions.insert(
						value_key,
						SelectedPageDisposition::AllNullElided {
							page_row_count: page.value_count.get(),
							bytes_per_value: column.bytes_per_value,
						},
					);
				} else {
					materialize_pages.push(page);
				}
			}
			if column.nullable {
				if may_have_nulls {
					if materialize_pages.is_empty() {
						continue;
					}
					let validity_page = input
						.page_directory
						.buffer_pages
						.iter()
						.find(|page| {
							page.row_group_index == group_index
								&& page.ipc_column_ordinal
									== IpcColumnOrdinal::new(column.physical_ordinal as u32)
								&& page.ipc_buffer_ordinal == column.null_buffer_ordinal
								&& page.range.len.get() > 0
						})
						.ok_or(SparseProjectionFetchDecline::ValidityBufferRangeMissing);
					let validity_page = match validity_page {
						Ok(page) => page,
						Err(reason) => return SparseProjectionFetchDecision::Decline(reason),
					};
					let per_buffer_key = (
						group_index,
						validity_page.ipc_column_ordinal,
						validity_page.ipc_buffer_ordinal,
					);
					let counter = per_buffer_ranges.entry(per_buffer_key).or_insert(0);
					*counter = counter.saturating_add(materialize_pages.len());
					if *counter > MAX_SPARSE_RANGES_PER_BUFFER {
						return SparseProjectionFetchDecision::Decline(
							SparseProjectionFetchDecline::PerBufferFragmentationCapExceeded,
						);
					}
					let validity_encoding = find_buffer_encoding(
						input.encoding_directory,
						group_index,
						column.null_buffer_ordinal,
						validity_page.range,
					);
					if let Some(validity_encoding) = validity_encoding {
						let Some(validity_cascade) = input
							.encoding_directory
							.cascades
							.get(validity_encoding.cascade_index.get() as usize)
						else {
							return SparseProjectionFetchDecision::Decline(
								SparseProjectionFetchDecline::EncodedValidityBufferUnsupported,
							);
						};
						match encoded_validity_sparse_eligibility(
							validity_encoding,
							validity_cascade,
							&materialize_pages,
							group.row_count,
						) {
							EncodedValiditySparseEligibility::Eligible => {}
							EncodedValiditySparseEligibility::Decline(_) => {
								return SparseProjectionFetchDecision::Decline(
									SparseProjectionFetchDecline::EncodedValidityBufferUnsupported,
								);
							}
						}
						let validity_pages = match expand_page_descriptors(validity_encoding) {
							Ok(pages) => pages,
							Err(_) => {
								return SparseProjectionFetchDecision::Decline(
									SparseProjectionFetchDecline::EncodedValidityBufferUnsupported,
								);
							}
						};
						for page in &materialize_pages {
							let Some(validity_descriptor) = validity_pages
								.iter()
								.copied()
								.find(|candidate| candidate.page_index == page.page_index)
							else {
								return SparseProjectionFetchDecision::Decline(
									SparseProjectionFetchDecline::EncodedValidityBufferUnsupported,
								);
							};
							validity_fetches.push(PageMaterializeValidityFetch {
								key: PageMaterializeBufferKey {
									row_group_index: group_index,
									ipc_column_ordinal: validity_page.ipc_column_ordinal,
									ipc_buffer_ordinal: validity_page.ipc_buffer_ordinal,
									page_index: validity_descriptor.page_index.get(),
								},
								body_range: encoded_page_body_range(
									validity_page.range,
									validity_encoding,
									validity_descriptor,
								),
								page: validity_descriptor,
								source: PageMaterializeValiditySource::Encoded {
									cascade: validity_encoding.cascade_index,
								},
								page_row_count: RowGroupRowCount::new(
									validity_descriptor.value_count.get(),
								),
							});
						}
					} else {
						for page in &materialize_pages {
							let (body_range, bit_offset) = match validity_bitmap_body_range_for_page(
								validity_page.range,
								*page,
							) {
								Ok(range) => range,
								Err(_) => {
									return SparseProjectionFetchDecision::Decline(
										SparseProjectionFetchDecline::ValidityBufferRangeMissing,
									);
								}
							};
							validity_fetches.push(PageMaterializeValidityFetch {
								key: PageMaterializeBufferKey {
									row_group_index: group_index,
									ipc_column_ordinal: validity_page.ipc_column_ordinal,
									ipc_buffer_ordinal: validity_page.ipc_buffer_ordinal,
									page_index: page.page_index.get(),
								},
								body_range,
								page: *page,
								source: PageMaterializeValiditySource::Raw { bit_offset },
								page_row_count: RowGroupRowCount::new(page.value_count.get()),
							});
						}
					}
				} else {
					for page in &materialize_pages {
						all_valid_nullable_pages.insert(PageMaterializeBufferKey {
							row_group_index: group_index,
							ipc_column_ordinal: IpcColumnOrdinal::new(
								column.physical_ordinal as u32,
							),
							ipc_buffer_ordinal: column.null_buffer_ordinal,
							page_index: page.page_index.get(),
						});
					}
				}
			}
			let per_buffer_key = (
				group_index,
				buffer_page.ipc_column_ordinal,
				buffer_page.ipc_buffer_ordinal,
			);
			let counter = per_buffer_ranges.entry(per_buffer_key).or_insert(0);
			*counter = counter.saturating_add(materialize_pages.len());
			if *counter > MAX_SPARSE_RANGES_PER_BUFFER {
				return SparseProjectionFetchDecision::Decline(
					SparseProjectionFetchDecline::PerBufferFragmentationCapExceeded,
				);
			}
			for page in materialize_pages {
				let key = PageMaterializeBufferKey {
					row_group_index: group_index,
					ipc_column_ordinal: buffer_page.ipc_column_ordinal,
					ipc_buffer_ordinal: buffer_page.ipc_buffer_ordinal,
					page_index: page.page_index.get(),
				};
				value_page_dispositions
					.entry(key)
					.or_insert(SelectedPageDisposition::Materialize);
				fetches.push(PageMaterializeFetch {
					key,
					body_range: encoded_page_body_range(buffer_page.range, encoding, page),
					cascade: cascade.clone(),
					page,
					bytes_per_value: column.bytes_per_value,
					data_type: column.data_type.clone(),
				});
			}
		}
	}

	if fetches.len().saturating_add(validity_fetches.len())
		> input.encoding_config.max_simd_pages_per_record.get() as usize
	{
		return SparseProjectionFetchDecision::Decline(
			SparseProjectionFetchDecline::SimdPageCapExceeded,
		);
	}
	let mut selected_body_ranges = fetches
		.iter()
		.map(|fetch| fetch.body_range)
		.collect::<Vec<_>>();
	selected_body_ranges.extend(validity_fetches.iter().map(|fetch| fetch.body_range));
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
		validity_fetches,
		value_page_dispositions,
		all_valid_nullable_pages,
		selected_body_ranges,
	})
}

pub(crate) fn value_page_is_metadata_all_null(
	value_buf: &BufferEncoding,
	page: EncodingPageDescriptor,
) -> bool {
	all_null_metadata_decline_reason(value_buf, page).is_none()
}

fn all_null_elision_decline_reason(
	column: &PageMaterializeColumn,
	column_may_have_nulls: bool,
	value_buf: &BufferEncoding,
	page: EncodingPageDescriptor,
) -> Option<AllNullElisionDeclineReason> {
	if !column.nullable {
		return Some(AllNullElisionDeclineReason::NotNullable);
	}
	if !column_may_have_nulls {
		return Some(AllNullElisionDeclineReason::ColumnHasNoNulls);
	}
	if !all_null_elision_data_type_eligible(&column.data_type) {
		return Some(AllNullElisionDeclineReason::DataTypeNotInCatalog);
	}
	if value_page_is_metadata_all_null(value_buf, page) {
		None
	} else {
		all_null_metadata_decline_reason(value_buf, page)
	}
}

fn all_null_metadata_decline_reason(
	value_buf: &BufferEncoding,
	page: EncodingPageDescriptor,
) -> Option<AllNullElisionDeclineReason> {
	let page_index = page.page_index.get() as usize;
	let Some(stats) = value_buf.page_stats.get(page_index) else {
		return Some(AllNullElisionDeclineReason::MissingPageStats);
	};
	if page.value_count.get() == 0 {
		return Some(AllNullElisionDeclineReason::CorruptPageDescriptor);
	}
	let Some(page_value_end) = page
		.first_value_index
		.get()
		.checked_add(page.value_count.get())
	else {
		return Some(AllNullElisionDeclineReason::CorruptPageDescriptor);
	};
	if page_value_end > value_buf.decoded_value_count.get() {
		return Some(AllNullElisionDeclineReason::CorruptPageDescriptor);
	}
	let Ok(page_byte_end) = page.byte_range.end() else {
		return Some(AllNullElisionDeclineReason::CorruptPageDescriptor);
	};
	if page_byte_end > value_buf.encoded_range.len.get() {
		return Some(AllNullElisionDeclineReason::CorruptPageDescriptor);
	}
	if stats.null_count.get() != page.value_count.get() {
		return Some(AllNullElisionDeclineReason::PageStatsNotAllNull);
	}
	None
}

fn row_group_column_may_have_nulls(
	record_directory: &RecordBufferDirectory,
	page_directory: &RecordPageDirectory,
	group_index: RowGroupIndex,
	column: &PageMaterializeColumn,
) -> bool {
	let ipc_column_ordinal = IpcColumnOrdinal::new(column.physical_ordinal as u32);
	if let Some(stat) = page_directory.row_group_stats.iter().find(|stat| {
		stat.row_group_index == group_index && stat.ipc_column_ordinal == ipc_column_ordinal
	}) {
		return stat.null_count.get() != 0;
	}
	record_directory
		.column_null_counts
		.get(column.physical_ordinal)
		.copied()
		.unwrap_or(usize::MAX)
		!= 0
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
	if fixed_width_value_bytes(field.data_type()).is_none() {
		return Err(SparseProjectionFetchDecline::VariableWidthProjectedColumn);
	}
	if record_directory
		.column_null_counts
		.get(physical_ordinal)
		.is_none()
	{
		return Err(SparseProjectionFetchDecline::PageGeometryMismatchAcrossColumns);
	}
	fixed_width_sparse_projection_column(record_directory, physical_ordinal)
		.ok_or(SparseProjectionFetchDecline::NullableTypeNotInCatalog)
}

fn all_null_elision_data_type_eligible(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Date32
			| DataType::Date64
			| DataType::Timestamp(
				TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
				None,
			)
	)
}

pub(crate) fn encoded_validity_sparse_eligibility(
	validity_buf: &BufferEncoding,
	validity_cascade: &EncodingCascade,
	selected_value_pages: &[EncodingPageDescriptor],
	row_group_row_count: RowGroupRowCount,
) -> EncodedValiditySparseEligibility {
	if !encoded_validity_cascade_supported(validity_cascade) {
		return EncodedValiditySparseEligibility::Decline(
			EncodedValidityDeclineReason::UnsupportedCascade,
		);
	}
	if validity_buf.page_end_offsets.is_empty() {
		return EncodedValiditySparseEligibility::Decline(
			EncodedValidityDeclineReason::MissingPageMetadata,
		);
	}
	if validity_buf.decoded_value_count.get() != row_group_row_count.get() {
		return EncodedValiditySparseEligibility::Decline(
			EncodedValidityDeclineReason::RowGroupRowCountMismatch,
		);
	}
	if validity_buf.decoded_byte_len.get()
		!= u64::from(validity_buf.decoded_value_count.get()).div_ceil(8)
	{
		return EncodedValiditySparseEligibility::Decline(
			EncodedValidityDeclineReason::DecodedByteLenMismatch,
		);
	}
	let validity_pages = match expand_page_descriptors(validity_buf) {
		Ok(pages) => pages,
		Err(_) => {
			return EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::CorruptPageDescriptor,
			);
		}
	};
	for selected in selected_value_pages {
		let Some(validity_page) = validity_pages
			.iter()
			.find(|page| page.page_index == selected.page_index)
		else {
			return EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::PageGeometryMismatch,
			);
		};
		if validity_page.first_value_index != selected.first_value_index
			|| validity_page.value_count != selected.value_count
		{
			return EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::PageGeometryMismatch,
			);
		}
	}
	EncodedValiditySparseEligibility::Eligible
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
		BaseValue, BitsPerValue, BufferValueCount, BufferValueIndex, EncodingCascadeIndex,
		EncodingPageByteOffset, EncodingPageCount, EncryptedChunkCount, IpcBufferIndex,
		IpcBufferLength, IpcBufferOffset, LogicalSchemaFingerprint, PageNullCount,
		RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy, RecordRowIndex,
		RowCount, RunCount, SimdPageValueCount, SlabId,
	};
	use crate::storage::columnar::{
		ColumnBufferRange, ColumnOrdinalSet, IpcBufferRange, PlaintextByteRange, TieredRecordId,
	};
	use crate::storage::encoding::{
		EncodedBufferByteRange, EncodingLeafCascade, EncodingPageStats,
	};
	use crate::storage::row_group::{
		BufferPageDirectoryEntry, RowGroupColumnStats, RowGroupDirectoryEntry,
	};
	use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
	use std::collections::HashMap;

	const NULL_BUFFER_BODY_START: u64 = 128;
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
			Self::new_with_data_type_and_nullable(
				row_count,
				page_value_count,
				page_byte_len,
				column_null_count,
				data_type,
				column_null_count != 0,
			)
		}

		fn new_with_data_type_and_nullable(
			row_count: u32,
			page_value_count: u32,
			page_byte_len: u32,
			column_null_count: usize,
			data_type: DataType,
			field_nullable: bool,
		) -> Self {
			let page_count = row_count.div_ceil(page_value_count);
			let total_encoded_bytes = u64::from(page_count.saturating_mul(page_byte_len));
			let null_buffer_bytes = if column_null_count == 0 {
				0
			} else {
				u64::from(row_count).div_ceil(8)
			};
			let record_id = TieredRecordId {
				slab_id: SlabId::new(60),
				record_offset: RecordBodyByteOffset::ZERO,
			};
			let schema = Schema::new(vec![Field::new("value", data_type, field_nullable)]);
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
						offset_in_record: if null_buffer_bytes == 0 {
							IpcBufferOffset::ZERO
						} else {
							IpcBufferOffset::new(NULL_BUFFER_BODY_START)
						},
						length: IpcBufferLength::new(null_buffer_bytes),
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
				buffer_pages: vec![
					BufferPageDirectoryEntry {
						row_group_index: RowGroupIndex::ZERO,
						ipc_column_ordinal: IpcColumnOrdinal::ZERO,
						ipc_buffer_ordinal: IpcBufferIndex::ZERO,
						range: RecordBodyByteRange {
							start: if null_buffer_bytes == 0 {
								RecordBodyByteOffset::ZERO
							} else {
								RecordBodyByteOffset::new(NULL_BUFFER_BODY_START)
							},
							len: RecordBodyByteLen::new(null_buffer_bytes),
						},
					},
					BufferPageDirectoryEntry {
						row_group_index: RowGroupIndex::ZERO,
						ipc_column_ordinal: IpcColumnOrdinal::ZERO,
						ipc_buffer_ordinal: IpcBufferIndex::new(1),
						range: RecordBodyByteRange {
							start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START),
							len: RecordBodyByteLen::new(total_encoded_bytes),
						},
					},
				],
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
					page_stats: Vec::new(),
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
			self.decide_for_groups(requirement, outcome, dense_ranges, &[RowGroupIndex::ZERO])
		}

		fn decide_for_groups(
			&self,
			requirement: &ColumnFetchRequirement,
			outcome: &PhaseBOutcome,
			dense_ranges: &[RecordBodyByteRange],
			selected_groups: &[RowGroupIndex],
		) -> SparseProjectionFetchDecision {
			decide_sparse_projection_fetch(SparseProjectionFetchInput {
				record_directory: &self.record_directory,
				page_directory: &self.page_directory,
				encoding_directory: &self.encoding_directory,
				requirement,
				predicate_outcome: outcome,
				selected_groups,
				dense_body_ranges: dense_ranges,
				row_group_config: &self.row_group_config,
				encoding_config: &self.encoding_config,
			})
		}

		fn set_value_page_null_counts(&mut self, counts: &[u32]) {
			self.encoding_directory.buffers[0].page_stats = counts
				.iter()
				.copied()
				.map(|null_count| EncodingPageStats {
					null_count: PageNullCount::new(null_count),
					min: None,
					max: None,
				})
				.collect();
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
			page_stats: Vec::new(),
		}
	}

	fn encoded_validity_buffer(row_group_index: RowGroupIndex, row_count: u32) -> BufferEncoding {
		BufferEncoding {
			row_group_index,
			ipc_column_ordinal: IpcColumnOrdinal::ZERO,
			ipc_buffer_ordinal: IpcBufferIndex::ZERO,
			cascade_index: EncodingCascadeIndex::ZERO,
			decoded_byte_len: RecordBodyByteLen::new(u64::from(row_count).div_ceil(8)),
			decoded_value_count: BufferValueCount::new(row_count),
			encoded_range: EncodedBufferByteRange {
				start: EncodingPageByteOffset::ZERO,
				len: RecordBodyByteLen::new(u64::from(row_count).div_ceil(8)),
			},
			page_value_count: SimdPageValueCount::new(4),
			page_end_offsets: Vec::new(),
			page_stats: Vec::new(),
		}
	}

	fn encoded_validity_buffer_with_pages(
		row_group_index: RowGroupIndex,
		row_count: u32,
		page_value_count: u32,
		cascade_index: EncodingCascadeIndex,
		page_end_offsets: &[u32],
	) -> BufferEncoding {
		BufferEncoding {
			row_group_index,
			ipc_column_ordinal: IpcColumnOrdinal::ZERO,
			ipc_buffer_ordinal: IpcBufferIndex::ZERO,
			cascade_index,
			decoded_byte_len: RecordBodyByteLen::new(u64::from(row_count).div_ceil(8)),
			decoded_value_count: BufferValueCount::new(row_count),
			encoded_range: EncodedBufferByteRange {
				start: EncodingPageByteOffset::ZERO,
				len: RecordBodyByteLen::new(u64::from(*page_end_offsets.last().unwrap_or(&0))),
			},
			page_value_count: SimdPageValueCount::new(page_value_count),
			page_end_offsets: page_end_offsets
				.iter()
				.copied()
				.map(EncodingPageByteOffset::new)
				.collect(),
			page_stats: Vec::new(),
		}
	}

	fn value_page_key(page_index: u32) -> PageMaterializeBufferKey {
		PageMaterializeBufferKey {
			row_group_index: RowGroupIndex::ZERO,
			ipc_column_ordinal: IpcColumnOrdinal::ZERO,
			ipc_buffer_ordinal: IpcBufferIndex::new(1),
			page_index,
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
	fn value_page_is_metadata_all_null_accepts_exact_page_null_count() {
		let mut value_buf = encoding(8, 4, &[32, 64]);
		value_buf.page_stats = vec![
			EncodingPageStats {
				null_count: PageNullCount::new(4),
				min: None,
				max: None,
			},
			EncodingPageStats {
				null_count: PageNullCount::new(3),
				min: None,
				max: None,
			},
		];
		let pages = expand_page_descriptors(&value_buf).unwrap();
		assert!(value_page_is_metadata_all_null(&value_buf, pages[0]));
		assert!(!value_page_is_metadata_all_null(&value_buf, pages[1]));
	}

	#[test]
	fn value_page_is_metadata_all_null_fails_closed_on_missing_or_malformed_metadata() {
		let mut value_buf = encoding(8, 4, &[32, 64]);
		let pages = expand_page_descriptors(&value_buf).unwrap();
		assert!(!value_page_is_metadata_all_null(&value_buf, pages[0]));

		value_buf.page_stats = vec![EncodingPageStats {
			null_count: PageNullCount::new(4),
			min: None,
			max: None,
		}];
		let mut out_of_bounds = pages[0];
		out_of_bounds.page_index = EncodingPageIndex::new(9);
		assert!(!value_page_is_metadata_all_null(&value_buf, out_of_bounds));

		let mut bad_row_span = pages[0];
		bad_row_span.first_value_index = BufferValueIndex::new(7);
		bad_row_span.value_count = BufferValueCount::new(4);
		assert!(!value_page_is_metadata_all_null(&value_buf, bad_row_span));

		let mut bad_byte_span = pages[0];
		bad_byte_span.byte_range.len = RecordBodyByteLen::new(65);
		assert!(!value_page_is_metadata_all_null(&value_buf, bad_byte_span));
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

	fn assert_nullable_exact_mask_engages_sparse_projection(
		mask: BooleanArray,
	) -> SparseProjectionFetchPlan {
		let fixture = PlannerFixture::new(16, 4, 32, 1);
		let decision = fixture.decide(&output_requirement(), &outcome(mask), &[dense_range()]);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected nullable sparse projection plan, got {decision:?}");
		};
		assert!(plan.columns[0].nullable);
		assert!(!plan.fetches.is_empty());
		assert_eq!(plan.fetches.len(), plan.validity_fetches.len());
		assert!(plan.all_valid_nullable_pages.is_empty());
		plan
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
		assert!(plan.validity_fetches.is_empty());
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
		assert!(plan.validity_fetches.is_empty());
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
	fn dense_predicate_batch_does_not_decline_sparse_projection() {
		let fixture = PlannerFixture::new(16, 4, 32, 0);
		let mut outcome = outcome(BooleanArray::from_iter((0..16).map(|row| row == 1)));
		outcome.decoded_predicate_batches.insert(
			RowGroupIndex::ZERO,
			arrow::record_batch::RecordBatch::try_new(
				std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
					arrow::datatypes::Field::new(
						"predicate",
						arrow::datatypes::DataType::Int64,
						false,
					),
				])),
				vec![std::sync::Arc::new(
					arrow::array::Int64Array::from_iter_values(0..16),
				)],
			)
			.unwrap(),
		);
		let SparseProjectionFetchDecision::Sparse(plan) =
			fixture.decide(&output_requirement(), &outcome, &[dense_range()])
		else {
			panic!("dense predicate batch must not decline sparse projection");
		};
		assert_eq!(plan.selected_groups, vec![RowGroupIndex::ZERO]);
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
	fn adr063_is_null_exact_mask_drives_nullable_sparse_projection() {
		let plan = assert_nullable_exact_mask_engages_sparse_projection(BooleanArray::from_iter(
			(0..16).map(|row| matches!(row, 1 | 9)),
		));
		assert_eq!(plan.group_pages[0].selected_pages.len(), 2);
	}

	#[test]
	fn adr063_is_not_null_exact_mask_drives_nullable_sparse_projection() {
		let plan = assert_nullable_exact_mask_engages_sparse_projection(BooleanArray::from_iter(
			(0..16).map(|row| row % 4 == 0),
		));
		assert_eq!(plan.group_pages[0].selected_pages.len(), 4);
	}

	#[test]
	fn adr063_nullable_value_comparison_exact_mask_drives_sparse_projection() {
		let plan = assert_nullable_exact_mask_engages_sparse_projection(BooleanArray::from_iter(
			(0..16).map(|row| row > 10),
		));
		assert_eq!(plan.group_pages[0].selected_pages.len(), 2);
	}

	#[test]
	fn nullable_active_projection_column_fetches_page_validity_bitmap() {
		let fixture = PlannerFixture::new(16, 4, 32, 1);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected nullable sparse projection plan, got {decision:?}");
		};
		assert!(plan.columns[0].nullable);
		assert_eq!(plan.fetches.len(), 1);
		assert_eq!(plan.validity_fetches.len(), 1);
		assert!(plan.all_valid_nullable_pages.is_empty());
		assert_eq!(
			plan.validity_fetches[0].body_range,
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(NULL_BUFFER_BODY_START),
				len: RecordBodyByteLen::new(1),
			}
		);
		assert!(matches!(
			plan.validity_fetches[0].source,
			PageMaterializeValiditySource::Raw { bit_offset: 0 }
		));
		assert_eq!(
			plan.validity_fetches[0].page_row_count,
			RowGroupRowCount::new(4)
		);
		assert_eq!(plan.selected_body_ranges.len(), 2);
		assert_eq!(
			plan.selected_body_ranges,
			vec![
				RecordBodyByteRange {
					start: RecordBodyByteOffset::new(NULL_BUFFER_BODY_START),
					len: RecordBodyByteLen::new(1),
				},
				RecordBodyByteRange {
					start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START),
					len: RecordBodyByteLen::new(32),
				},
			]
		);
	}

	#[test]
	fn nullable_all_null_page_elides_value_and_validity_fetches() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 4);
		fixture.set_value_page_null_counts(&[4, 0, 0, 0]);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected all-null sparse projection plan, got {decision:?}");
		};
		assert!(plan.fetches.is_empty());
		assert!(plan.validity_fetches.is_empty());
		assert!(plan.selected_body_ranges.is_empty());
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(0)),
			Some(&SelectedPageDisposition::AllNullElided {
				page_row_count: 4,
				bytes_per_value: 8,
			})
		);
	}

	#[test]
	fn nullable_all_null_elision_preserves_materialized_neighbor_pages() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 4);
		fixture.set_value_page_null_counts(&[4, 0, 0, 0]);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter(
				(0..16).map(|row| matches!(row, 1 | 5)),
			)),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected mixed all-null sparse projection plan, got {decision:?}");
		};
		assert_eq!(plan.fetches.len(), 1);
		assert_eq!(plan.fetches[0].key, value_page_key(1));
		assert_eq!(plan.validity_fetches.len(), 1);
		assert_eq!(plan.validity_fetches[0].key.page_index, 1);
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(0)),
			Some(&SelectedPageDisposition::AllNullElided {
				page_row_count: 4,
				bytes_per_value: 8,
			})
		);
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(1)),
			Some(&SelectedPageDisposition::Materialize)
		);
		assert_eq!(
			plan.selected_body_ranges,
			vec![
				RecordBodyByteRange {
					start: RecordBodyByteOffset::new(NULL_BUFFER_BODY_START),
					len: RecordBodyByteLen::new(1),
				},
				RecordBodyByteRange {
					start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START + 32),
					len: RecordBodyByteLen::new(32),
				},
			]
		);
	}

	#[test]
	fn missing_page_stats_keeps_nullable_sparse_projection_on_fetch_path() {
		let fixture = PlannerFixture::new(16, 4, 32, 4);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected nullable sparse projection plan, got {decision:?}");
		};
		assert_eq!(plan.fetches.len(), 1);
		assert_eq!(plan.validity_fetches.len(), 1);
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(0)),
			Some(&SelectedPageDisposition::Materialize)
		);
	}

	#[test]
	fn all_null_elision_catalog_includes_int32_and_timestamp_millisecond() {
		for data_type in [
			DataType::Int32,
			DataType::Timestamp(TimeUnit::Millisecond, None),
		] {
			let mut fixture = PlannerFixture::new_with_data_type(16, 4, 16, 4, data_type.clone());
			fixture.set_value_page_null_counts(&[4, 0, 0, 0]);
			let decision = fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			);
			let SparseProjectionFetchDecision::Sparse(plan) = decision else {
				panic!(
					"expected all-null sparse projection plan for {data_type:?}, got {decision:?}"
				);
			};
			assert!(plan.fetches.is_empty(), "{data_type:?}");
			assert!(plan.validity_fetches.is_empty(), "{data_type:?}");
			assert!(matches!(
				plan.value_page_dispositions.get(&value_page_key(0)),
				Some(SelectedPageDisposition::AllNullElided { .. })
			));
		}
	}

	#[test]
	fn all_null_elision_catalog_excludes_float64_without_declining_sparse_projection() {
		let mut fixture = PlannerFixture::new_with_data_type(16, 4, 32, 4, DataType::Float64);
		fixture.set_value_page_null_counts(&[4, 0, 0, 0]);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected sparse projection plan, got {decision:?}");
		};
		assert_eq!(plan.fetches.len(), 1);
		assert_eq!(plan.validity_fetches.len(), 1);
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(0)),
			Some(&SelectedPageDisposition::Materialize)
		);
	}

	#[test]
	fn boolean_is_not_sparse_projection_value_column_catalog_v1() {
		let fixture = PlannerFixture::new_with_data_type(16, 4, 32, 4, DataType::Boolean);
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
	fn all_valid_nullable_field_ignores_all_null_page_stats() {
		let mut fixture =
			PlannerFixture::new_with_data_type_and_nullable(16, 4, 32, 0, DataType::Int64, true);
		fixture.set_value_page_null_counts(&[4, 0, 0, 0]);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected sparse projection plan, got {decision:?}");
		};
		assert_eq!(plan.fetches.len(), 1);
		assert!(plan.validity_fetches.is_empty());
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(0)),
			Some(&SelectedPageDisposition::Materialize)
		);
	}

	#[test]
	fn schema_non_nullable_column_ignores_all_null_page_stats() {
		let mut fixture =
			PlannerFixture::new_with_data_type_and_nullable(16, 4, 32, 0, DataType::Int64, false);
		fixture.set_value_page_null_counts(&[4, 0, 0, 0]);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected sparse projection plan, got {decision:?}");
		};
		assert!(!plan.columns[0].nullable);
		assert_eq!(plan.fetches.len(), 1);
		assert!(plan.validity_fetches.is_empty());
		assert_eq!(
			plan.value_page_dispositions.get(&value_page_key(0)),
			Some(&SelectedPageDisposition::Materialize)
		);
	}

	#[test]
	fn nullable_row_group_zero_null_count_skips_validity_fetch_when_record_has_nulls() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 1);
		let group_one = RowGroupIndex::new(1);
		fixture
			.page_directory
			.row_groups
			.push(RowGroupDirectoryEntry {
				row_group_index: group_one,
				first_record_row_index: RecordRowIndex::new(16),
				row_count: RowGroupRowCount::new(16),
				metadata_range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(512),
					len: RecordBodyByteLen::new(64),
				},
				body_range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(512),
					len: RecordBodyByteLen::new(8 * 1024),
				},
			});
		fixture.page_directory.row_group_stats = vec![
			RowGroupColumnStats {
				row_group_index: RowGroupIndex::ZERO,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				null_count: RowCount::new(0),
				min: None,
				max: None,
			},
			RowGroupColumnStats {
				row_group_index: group_one,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				null_count: RowCount::new(1),
				min: None,
				max: None,
			},
		];

		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected nullable all-valid row-group sparse plan, got {decision:?}");
		};
		assert!(plan.columns[0].nullable);
		assert_eq!(plan.fetches.len(), 1);
		assert!(plan.validity_fetches.is_empty());
		assert_eq!(plan.all_valid_nullable_pages.len(), 1);
		assert!(
			plan.all_valid_nullable_pages
				.contains(&PageMaterializeBufferKey {
					row_group_index: RowGroupIndex::ZERO,
					ipc_column_ordinal: IpcColumnOrdinal::ZERO,
					ipc_buffer_ordinal: IpcBufferIndex::ZERO,
					page_index: 0,
				})
		);
		assert_eq!(
			plan.selected_body_ranges,
			vec![RecordBodyByteRange {
				start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START),
				len: RecordBodyByteLen::new(32),
			}]
		);
	}

	#[test]
	fn nullable_field_with_zero_null_count_uses_all_valid_sparse_path() {
		let fixture =
			PlannerFixture::new_with_data_type_and_nullable(16, 4, 32, 0, DataType::Int64, true);
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected all-valid sparse projection plan, got {decision:?}");
		};
		assert!(!plan.columns[0].nullable);
		assert_eq!(plan.fetches.len(), 1);
		assert!(plan.validity_fetches.is_empty());
		assert!(plan.all_valid_nullable_pages.is_empty());
		assert_eq!(
			plan.selected_body_ranges,
			vec![RecordBodyByteRange {
				start: RecordBodyByteOffset::new(VALUE_BUFFER_BODY_START),
				len: RecordBodyByteLen::new(32),
			}]
		);
	}

	#[test]
	fn encoded_validity_buffer_declines_sparse_projection() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 1);
		fixture
			.encoding_directory
			.buffers
			.push(encoded_validity_buffer(RowGroupIndex::ZERO, 16));
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::EncodedValidityBufferUnsupported,
		);
	}

	#[test]
	fn encoded_validity_sparse_eligibility_reports_decline_reasons() {
		let supported_cascade = EncodingCascade::Rle {
			run_count: RunCount::new(4),
			inner: Box::new(EncodingLeafCascade::Plain),
		};
		let selected_pages =
			expand_page_descriptors(&encoding(4096, 1024, &[128, 256, 384, 512])).unwrap();
		let valid_buffer = encoded_validity_buffer_with_pages(
			RowGroupIndex::ZERO,
			4096,
			1024,
			EncodingCascadeIndex::new(1),
			&[5, 10, 15, 20],
		);

		assert_eq!(
			encoded_validity_sparse_eligibility(
				&valid_buffer,
				&EncodingCascade::Plain,
				&selected_pages,
				RowGroupRowCount::new(4096),
			),
			EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::UnsupportedCascade
			)
		);

		let missing_pages = encoded_validity_buffer(RowGroupIndex::ZERO, 4096);
		assert_eq!(
			encoded_validity_sparse_eligibility(
				&missing_pages,
				&supported_cascade,
				&selected_pages,
				RowGroupRowCount::new(4096),
			),
			EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::MissingPageMetadata
			)
		);

		assert_eq!(
			encoded_validity_sparse_eligibility(
				&valid_buffer,
				&supported_cascade,
				&selected_pages,
				RowGroupRowCount::new(4095),
			),
			EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::RowGroupRowCountMismatch
			)
		);

		let mut decoded_len_mismatch = valid_buffer.clone();
		decoded_len_mismatch.decoded_byte_len = RecordBodyByteLen::new(999);
		assert_eq!(
			encoded_validity_sparse_eligibility(
				&decoded_len_mismatch,
				&supported_cascade,
				&selected_pages,
				RowGroupRowCount::new(4096),
			),
			EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::DecodedByteLenMismatch
			)
		);

		let mut corrupt_descriptor = valid_buffer.clone();
		corrupt_descriptor.page_end_offsets = vec![
			EncodingPageByteOffset::new(5),
			EncodingPageByteOffset::new(4),
		];
		corrupt_descriptor.encoded_range.len = RecordBodyByteLen::new(4);
		assert_eq!(
			encoded_validity_sparse_eligibility(
				&corrupt_descriptor,
				&supported_cascade,
				&selected_pages,
				RowGroupRowCount::new(4096),
			),
			EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::CorruptPageDescriptor
			)
		);

		let mismatched_selected_pages =
			expand_page_descriptors(&encoding(4096, 2048, &[256, 512])).unwrap();
		assert_eq!(
			encoded_validity_sparse_eligibility(
				&valid_buffer,
				&supported_cascade,
				&mismatched_selected_pages,
				RowGroupRowCount::new(4096),
			),
			EncodedValiditySparseEligibility::Decline(
				EncodedValidityDeclineReason::PageGeometryMismatch
			)
		);

		assert_eq!(
			encoded_validity_sparse_eligibility(
				&valid_buffer,
				&supported_cascade,
				&selected_pages,
				RowGroupRowCount::new(4096),
			),
			EncodedValiditySparseEligibility::Eligible
		);
	}

	#[test]
	fn rle_plain_encoded_validity_buffer_fetches_selected_validity_page() {
		let mut fixture = PlannerFixture::new(4096, 1024, 32, 1);
		fixture
			.encoding_directory
			.cascades
			.push(EncodingCascade::Rle {
				run_count: RunCount::new(4),
				inner: Box::new(EncodingLeafCascade::Plain),
			});
		fixture
			.encoding_directory
			.buffers
			.push(encoded_validity_buffer_with_pages(
				RowGroupIndex::ZERO,
				4096,
				1024,
				EncodingCascadeIndex::new(1),
				&[5, 10, 15, 20],
			));
		let decision = fixture.decide(
			&output_requirement(),
			&outcome(BooleanArray::from_iter((0..4096).map(|row| row == 1))),
			&[dense_range()],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected encoded-validity sparse projection plan, got {decision:?}");
		};
		assert_eq!(plan.validity_fetches.len(), 1);
		assert_eq!(
			plan.validity_fetches[0].body_range,
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(NULL_BUFFER_BODY_START),
				len: RecordBodyByteLen::new(5),
			}
		);
		assert!(matches!(
			plan.validity_fetches[0].source,
			PageMaterializeValiditySource::Encoded {
				cascade: EncodingCascadeIndex(1)
			}
		));
		assert_eq!(
			plan.validity_fetches[0].page_row_count,
			RowGroupRowCount::new(1024)
		);
	}

	#[test]
	fn nullable_projection_declines_when_validity_range_is_missing() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 1);
		fixture
			.page_directory
			.buffer_pages
			.retain(|page| page.ipc_buffer_ordinal != IpcBufferIndex::ZERO);
		assert_decline(
			fixture.decide(
				&output_requirement(),
				&outcome(BooleanArray::from_iter((0..16).map(|row| row == 1))),
				&[dense_range()],
			),
			SparseProjectionFetchDecline::ValidityBufferRangeMissing,
		);
	}

	#[test]
	fn encoded_validity_detection_for_one_row_group_does_not_poison_another() {
		let mut fixture = PlannerFixture::new(16, 4, 32, 1);
		let group_one = RowGroupIndex::new(1);
		let group_one_null_start = 512;
		let group_one_value_start = 640;
		fixture
			.page_directory
			.row_groups
			.push(RowGroupDirectoryEntry {
				row_group_index: group_one,
				first_record_row_index: RecordRowIndex::new(16),
				row_count: RowGroupRowCount::new(16),
				metadata_range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(448),
					len: RecordBodyByteLen::new(64),
				},
				body_range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(448),
					len: RecordBodyByteLen::new(8 * 1024),
				},
			});
		fixture
			.page_directory
			.buffer_pages
			.push(BufferPageDirectoryEntry {
				row_group_index: group_one,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				ipc_buffer_ordinal: IpcBufferIndex::ZERO,
				range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(group_one_null_start),
					len: RecordBodyByteLen::new(2),
				},
			});
		fixture
			.page_directory
			.buffer_pages
			.push(BufferPageDirectoryEntry {
				row_group_index: group_one,
				ipc_column_ordinal: IpcColumnOrdinal::ZERO,
				ipc_buffer_ordinal: IpcBufferIndex::new(1),
				range: RecordBodyByteRange {
					start: RecordBodyByteOffset::new(group_one_value_start),
					len: RecordBodyByteLen::new(128),
				},
			});
		let mut group_one_value_encoding = fixture.encoding_directory.buffers[0].clone();
		group_one_value_encoding.row_group_index = group_one;
		fixture
			.encoding_directory
			.buffers
			.push(group_one_value_encoding);
		fixture
			.encoding_directory
			.buffers
			.push(encoded_validity_buffer(RowGroupIndex::ZERO, 16));

		let outcome = PhaseBOutcome {
			survival_masks: HashMap::from([(
				group_one,
				BooleanArray::from_iter((0..16).map(|row| row == 1)),
			)]),
			..PhaseBOutcome::default()
		};
		let decision = fixture.decide_for_groups(
			&output_requirement(),
			&outcome,
			&[dense_range()],
			&[group_one],
		);
		let SparseProjectionFetchDecision::Sparse(plan) = decision else {
			panic!("expected row-group-local raw validity path, got {decision:?}");
		};
		assert_eq!(plan.selected_groups, vec![group_one]);
		assert_eq!(plan.validity_fetches.len(), 1);
		assert_eq!(
			plan.validity_fetches[0].body_range,
			RecordBodyByteRange {
				start: RecordBodyByteOffset::new(group_one_null_start),
				len: RecordBodyByteLen::new(1),
			}
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
		assert!(plan.validity_fetches.is_empty());
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
