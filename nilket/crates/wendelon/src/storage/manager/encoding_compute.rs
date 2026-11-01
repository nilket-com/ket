use super::*;
use std::collections::BTreeSet;

pub(crate) struct ProjectedL2RecordRead {
	pub(crate) projected: ProjectedRecordBatch,
	pub(crate) projected_first_record_row_index: RecordRowIndex,
	pub(crate) indexed_rows_materialized: bool,
	pub(crate) encoding_materialize_audit: Vec<EncodingMaterializeAuditSample>,
	#[allow(dead_code)]
	pub(crate) strategy: L2FetchStrategy,
}

pub(crate) struct EncodedAggregateL2RecordRead {
	pub(crate) partials: Vec<AggregatePartial>,
	pub(crate) rows_admitted: u64,
	#[allow(dead_code)]
	pub(crate) strategy: L2FetchStrategy,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EncodingMaterializeAuditSample {
	pub(crate) cascade: &'static str,
	pub(crate) buffer_bytes_in: u64,
	pub(crate) buffer_bytes_out: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EncodingIndexedRowPage {
	pub(super) page_key: PageMaterializeKey,
	pub(super) row_offset_in_page: usize,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingIndexedPagePlan {
	pub(super) selected_groups: Vec<RowGroupIndex>,
	pub(super) row_pages: Vec<EncodingIndexedRowPage>,
	pub(super) columns: Vec<PageMaterializeColumn>,
	pub(super) fetches: Vec<PageMaterializeFetch>,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputePhaseBRead {
	pub(super) outcome: crate::storage::row_group::PhaseBOutcome,
	pub(super) phase_b_fetch: crate::storage::row_group::RowGroupRangeFetchPlan,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputePhaseBPlan {
	pub(super) node: EncodingComputePhaseBNode,
	pub(super) leaves: Vec<EncodingComputePhaseBLeaf>,
	pub(super) fetches: Vec<EncodingComputePhaseBFetch>,
	pub(super) record_body_ranges: Vec<crate::storage::row_group::RecordBodyByteRange>,
	pub(super) body_ranges: Vec<TieredPlaintextRange>,
	pub(super) record_ranges: Vec<TieredPlaintextRange>,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingPredicatePagePlan {
	pub(super) node: EncodingComputePhaseBNode,
	pub(super) leaves: Vec<EncodingComputePhaseBLeaf>,
	pub(super) decisions: Vec<EncodingPredicatePageDecision>,
	pub(super) fetches: Vec<EncodingPredicatePageFetch>,
	pub(super) record_body_ranges: Vec<crate::storage::row_group::RecordBodyByteRange>,
	pub(super) body_ranges: Vec<TieredPlaintextRange>,
	pub(super) record_ranges: Vec<TieredPlaintextRange>,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingPredicatePageDecision {
	pub(super) row_group_index: RowGroupIndex,
	pub(super) leaf_index: usize,
	pub(super) first_row: u32,
	pub(super) row_count: u32,
	pub(super) classification: EncodingPredicatePageDecisionKind,
}

#[derive(Clone, Debug)]
pub(super) enum EncodingPredicatePageDecisionKind {
	Disjoint,
	ExactMatch,
	Maybe { fetch_index: usize },
}

#[derive(Clone, Debug)]
pub(super) struct EncodingPredicatePageFetch {
	pub(super) body_range: crate::storage::row_group::RecordBodyByteRange,
	pub(super) encoding: BufferEncoding,
	pub(super) cascade: EncodingCascade,
	pub(super) data_type: DataType,
	pub(super) atom: PredicateAtom,
}

#[derive(Clone, Debug)]
pub(super) enum EncodingComputePhaseBNode {
	Leaf(usize),
	And(Vec<EncodingComputePhaseBNode>),
	Or(Vec<EncodingComputePhaseBNode>),
	Not(Box<EncodingComputePhaseBNode>),
}

#[derive(Clone, Debug)]
pub(super) enum EncodingComputePhaseBLeaf {
	Encoded {
		column_index: usize,
		physical_ordinal: usize,
		value_buffer_ordinal: IpcBufferIndex,
		offsets_buffer_ordinal: Option<IpcBufferIndex>,
		atom: PredicateAtom,
		data_type: DataType,
	},
	Validity {
		physical_ordinal: usize,
		selected_when_no_nulls: bool,
	},
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputePhaseBFetch {
	pub(super) leaf_index: usize,
	pub(super) row_group_index: RowGroupIndex,
	pub(super) body_range: crate::storage::row_group::RecordBodyByteRange,
	pub(super) kind: EncodingComputePhaseBFetchKind,
}

#[derive(Clone, Debug)]
pub(super) enum EncodingComputePhaseBFetchKind {
	Encoded {
		encoding: BufferEncoding,
		cascade: EncodingCascade,
	},
	FsstValues {
		encoding: BufferEncoding,
		cascade: EncodingCascade,
	},
	FsstOffsets {
		encoding: Option<BufferEncoding>,
		cascade: Option<EncodingCascade>,
	},
	Validity {
		encoding: Option<BufferEncoding>,
		cascade: Option<EncodingCascade>,
	},
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputeFsstValueBytes {
	pub(super) bytes: Vec<u8>,
	pub(super) encoding: BufferEncoding,
	pub(super) cascade: EncodingCascade,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputeFsstOffsetBytes {
	pub(super) bytes: Vec<u8>,
	pub(super) encoding: Option<BufferEncoding>,
	pub(super) cascade: Option<EncodingCascade>,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputeValidityFetch {
	pub(super) body_range: crate::storage::row_group::RecordBodyByteRange,
	pub(super) encoding: Option<BufferEncoding>,
	pub(super) cascade: Option<EncodingCascade>,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputeAggregateFetch {
	pub(super) column: u16,
	pub(super) value_body_range: Option<crate::storage::row_group::RecordBodyByteRange>,
	pub(super) validity_fetch: Option<EncodingComputeValidityFetch>,
	pub(super) encoding: Option<BufferEncoding>,
	pub(super) cascade: Option<EncodingCascade>,
	pub(super) data_type: DataType,
}

#[derive(Clone, Debug)]
pub(super) struct EncodingComputeAggregatePlan {
	pub(super) fetches: Vec<EncodingComputeAggregateFetch>,
	pub(super) record_body_ranges: Vec<crate::storage::row_group::RecordBodyByteRange>,
	pub(super) body_ranges: Vec<TieredPlaintextRange>,
	pub(super) record_ranges: Vec<TieredPlaintextRange>,
	pub(super) pages_fetched: u64,
}

pub(super) fn decode_full_projected_l2_record(
	slab_id: SlabId,
	location: RecordLocation,
	descriptor: &TableSchemaDescriptor,
	requirement: &ColumnFetchRequirement,
	frame: &[u8],
) -> std::io::Result<ProjectedRecordBatch> {
	let directory = parse_record_buffer_directory(
		TieredRecordId {
			slab_id,
			record_offset: RecordBodyByteOffset::new(location.offset),
		},
		PlaintextByteRange {
			start: 0,
			len: frame.len() as u64,
		},
		frame,
		descriptor,
	)
	.map_err(columnar_io_error)?;
	if directory.page_directory.is_some() {
		let batch = crate::storage::multirow::decode_batch(frame)?;
		return crate::storage::row_group::project_record_batch_for_requirement(
			&batch,
			requirement,
		);
	}
	decode_projected_record_batch(frame, &directory, requirement).map_err(arrow_io_error)
}

pub(super) fn indexed_record_fetch_key(location: RecordLocation) -> (SlabId, u64, u32, bool) {
	(
		location.slab_id,
		location.offset,
		location.length,
		location.is_tombstone,
	)
}

pub(super) fn take_projected_indexed_rows(
	read: &ProjectedL2RecordRead,
	record_rows: &[RecordRowIndex],
) -> std::io::Result<ProjectedRecordBatch> {
	if record_rows.is_empty() {
		return read.projected.take_rows(&[]);
	}
	if let L2FetchStrategy::RowGroup(plan) = &read.strategy {
		if plan.surviving_groups.is_empty() {
			return Ok(read.projected.slice(0, 0));
		}
		let mut group_offsets = HashMap::new();
		let mut projected_offset = 0usize;
		for group_index in &plan.surviving_groups {
			let group = plan
				.directory
				.row_groups
				.iter()
				.find(|group| group.row_group_index == *group_index)
				.copied()
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"row-group strategy selected missing group {}",
							group_index.get()
						),
					)
				})?;
			group_offsets.insert(*group_index, (projected_offset, group));
			projected_offset = projected_offset.saturating_add(group.row_count.get() as usize);
		}
		let mut offsets = Vec::with_capacity(record_rows.len());
		for row in record_rows {
			let group = crate::storage::row_group::row_group_for_record_row(&plan.directory, *row)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"indexed record row {} is not covered by row-group directory",
							row.get()
						),
					)
				})?;
			let Some((base_offset, selected_group)) = group_offsets.get(&group.row_group_index)
			else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"indexed record row {} maps to row group {} that was not fetched",
						row.get(),
						group.row_group_index.get()
					),
				));
			};
			let within_group = row
				.get()
				.checked_sub(selected_group.first_record_row_index.get())
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"indexed record row {} precedes row group {} base {}",
							row.get(),
							selected_group.row_group_index.get(),
							selected_group.first_record_row_index.get()
						),
					)
				})? as usize;
			offsets.push(base_offset.saturating_add(within_group));
		}
		return read.projected.take_rows(&offsets);
	}
	let base = read.projected_first_record_row_index.get();
	let offsets = record_rows
		.iter()
		.map(|row| {
			row.get().checked_sub(base).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!(
						"indexed record row {} precedes projected base {}",
						row.get(),
						base
					),
				)
			})
		})
		.map(|result| result.map(|offset| offset as usize))
		.collect::<std::io::Result<Vec<_>>>()?;
	read.projected.take_rows(&offsets)
}

pub(super) fn build_encoding_compute_phase_b_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	encoded_predicate: &EncodedPredicateNode,
	output_columns: &ColumnOrdinalSet,
	selected_groups: &[RowGroupIndex],
	predicate_fetched_groups: &[RowGroupIndex],
	config: &EncodingComputeConfig,
	header_size: u64,
) -> Result<EncodingComputePhaseBPlan, EncodingComputeFallbackReason> {
	let mut builder = EncodingComputePhaseBPlanBuilder {
		record_directory,
		leaves: Vec::new(),
	};
	let node = builder.build_node(encoded_predicate)?;
	let leaves = builder.leaves;
	let predicate_fetched = predicate_fetched_groups
		.iter()
		.copied()
		.collect::<BTreeSet<_>>();
	let mut fetches = Vec::new();
	let mut encoded_pages = 0_u64;
	for group_index in selected_groups {
		if !predicate_fetched.contains(group_index) {
			continue;
		}
		let group = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == *group_index)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if group.row_count.get() < config.min_buffer_rows_for_encoded_eval.get() {
			return Err(EncodingComputeFallbackReason::DenseSelection);
		}
		for (leaf_index, leaf) in leaves.iter().enumerate() {
			match leaf {
				EncodingComputePhaseBLeaf::Encoded {
					atom,
					column_index,
					data_type,
					physical_ordinal,
					value_buffer_ordinal,
					offsets_buffer_ordinal,
					..
				} => {
					let page = page_directory
						.buffer_pages
						.iter()
						.find(|page| {
							page.row_group_index == *group_index
								&& page.ipc_column_ordinal
									== IpcColumnOrdinal::new(*physical_ordinal as u32)
								&& page.ipc_buffer_ordinal == *value_buffer_ordinal
								&& page.range.len.get() > 0
						})
						.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
					let encoding = find_buffer_encoding(
						encoding_directory,
						*group_index,
						*value_buffer_ordinal,
						page.range,
					)
					.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
					if encoding.decoded_value_count.get() != group.row_count.get() {
						return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					}
					let cascade = encoding_directory
						.cascades
						.get(encoding.cascade_index.get() as usize)
						.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
					let evaluator = evaluator_kind_for_cascade(cascade)
						.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
					if matches!(
						evaluator,
						crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstWholeBuffer
							| crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstRowSafe
					) && output_columns.contains(*column_index as u32)
					{
						return Err(EncodingComputeFallbackReason::UnsupportedCascade);
					}
					if !encoding_compute_predicate_data_type_supported_for_evaluator(
						data_type, evaluator,
					) {
						return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					}
					if !config.predicate_evaluator_enabled(evaluator) {
						return Err(EncodingComputeFallbackReason::UnsupportedCascade);
					}
					let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type) else {
						return Err(EncodingComputeFallbackReason::UnsupportedProgram);
					};
					if matches!(
						native_evaluation_eligibility(cascade, atom, &coercion),
						NativeEvaluationEligibility::DecodeScalarFallback
					) {
						return Err(EncodingComputeFallbackReason::UnsupportedCascade);
					}
					encoded_pages =
						encoded_pages.saturating_add(encoding.page_end_offsets.len().max(1) as u64);
					if encoded_pages > u64::from(config.max_encoded_pages_per_record.get()) {
						return Err(EncodingComputeFallbackReason::TooManyPages);
					}
					if matches!(
						evaluator,
						crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstWholeBuffer
							| crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstRowSafe
					) {
						let offsets_buffer_ordinal = offsets_buffer_ordinal
							.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
						let offsets_page = page_directory
							.buffer_pages
							.iter()
							.find(|page| {
								page.row_group_index == *group_index
									&& page.ipc_column_ordinal
										== IpcColumnOrdinal::new(*physical_ordinal as u32)
									&& page.ipc_buffer_ordinal == offsets_buffer_ordinal
									&& page.range.len.get() > 0
							})
							.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
						let offsets_encoding = find_buffer_encoding(
							encoding_directory,
							*group_index,
							offsets_buffer_ordinal,
							offsets_page.range,
						);
						let (offsets_body_range, offsets_encoding, offsets_cascade) =
							match offsets_encoding {
								Some(offsets_encoding) => {
									let offsets_cascade = encoding_directory
										.cascades
										.get(offsets_encoding.cascade_index.get() as usize)
										.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
									(
										encoding_compute_fetch_range_for_buffer_page(
											offsets_page.range,
											offsets_encoding,
										)?,
										Some(offsets_encoding.clone()),
										Some(offsets_cascade.clone()),
									)
								}
								None => (offsets_page.range, None, None),
							};
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: encoding_compute_fetch_range_for_buffer_page(
								page.range, encoding,
							)?,
							kind: EncodingComputePhaseBFetchKind::FsstValues {
								encoding: encoding.clone(),
								cascade: cascade.clone(),
							},
						});
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: offsets_body_range,
							kind: EncodingComputePhaseBFetchKind::FsstOffsets {
								encoding: offsets_encoding,
								cascade: offsets_cascade,
							},
						});
					} else {
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: encoding_compute_fetch_range_for_buffer_page(
								page.range, encoding,
							)?,
							kind: EncodingComputePhaseBFetchKind::Encoded {
								encoding: encoding.clone(),
								cascade: cascade.clone(),
							},
						});
					}
					if let Some(validity_fetch) = encoding_compute_validity_page_for_row_group(
						record_directory,
						page_directory,
						encoding_directory,
						*group_index,
						*physical_ordinal,
					)? {
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: validity_fetch.body_range,
							kind: EncodingComputePhaseBFetchKind::Validity {
								encoding: validity_fetch.encoding,
								cascade: validity_fetch.cascade,
							},
						});
					}
				}
				EncodingComputePhaseBLeaf::Validity {
					physical_ordinal, ..
				} => {
					if let Some(validity_fetch) = encoding_compute_validity_page_for_row_group(
						record_directory,
						page_directory,
						encoding_directory,
						*group_index,
						*physical_ordinal,
					)? {
						fetches.push(EncodingComputePhaseBFetch {
							leaf_index,
							row_group_index: *group_index,
							body_range: validity_fetch.body_range,
							kind: EncodingComputePhaseBFetchKind::Validity {
								encoding: validity_fetch.encoding,
								cascade: validity_fetch.cascade,
							},
						});
					}
				}
			}
		}
	}
	let mut record_body_ranges = fetches
		.iter()
		.map(|fetch| fetch.body_range)
		.collect::<Vec<_>>();
	coalesce_manager_record_body_ranges(&mut record_body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let body_ranges = record_body_ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let record_ranges = record_body_ranges_to_record_ranges(header_size, &body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(EncodingComputePhaseBPlan {
		node,
		leaves,
		fetches,
		record_body_ranges,
		body_ranges,
		record_ranges,
	})
}

pub(super) fn build_encoding_predicate_page_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	encoded_predicate: &EncodedPredicateNode,
	_output_columns: &ColumnOrdinalSet,
	selected_groups: &[RowGroupIndex],
	predicate_fetched_groups: &[RowGroupIndex],
	config: &EncodingComputeConfig,
	header_size: u64,
) -> Result<EncodingPredicatePagePlan, EncodingComputeFallbackReason> {
	let mut builder = EncodingComputePhaseBPlanBuilder {
		record_directory,
		leaves: Vec::new(),
	};
	let node = builder.build_node(encoded_predicate)?;
	let leaves = builder.leaves;
	let predicate_fetched = predicate_fetched_groups
		.iter()
		.copied()
		.collect::<BTreeSet<_>>();
	let mut decisions = Vec::new();
	let mut fetches = Vec::new();
	let mut selected_value_bytes = 0_u64;
	let mut dense_value_bytes = 0_u64;
	let mut encoded_pages = 0_u64;
	for group_index in selected_groups {
		if !predicate_fetched.contains(group_index) {
			continue;
		}
		let group = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == *group_index)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if group.row_count.get() < config.min_buffer_rows_for_encoded_eval.get() {
			return Err(EncodingComputeFallbackReason::DenseSelection);
		}
		for (leaf_index, leaf) in leaves.iter().enumerate() {
			match leaf {
				EncodingComputePhaseBLeaf::Encoded {
					atom,
					column_index,
					data_type,
					..
				} => {
					let (physical_ordinal, value_buffer_ordinal) =
						encoding_compute_value_buffer(record_directory, *column_index)?;
					build_encoding_predicate_page_leaf_decisions(
						page_directory,
						encoding_directory,
						group,
						leaf_index,
						physical_ordinal,
						value_buffer_ordinal,
						data_type,
						atom,
						config,
						&mut decisions,
						&mut fetches,
						&mut selected_value_bytes,
						&mut dense_value_bytes,
						&mut encoded_pages,
					)?;
				}
				EncodingComputePhaseBLeaf::Validity {
					physical_ordinal,
					selected_when_no_nulls,
				} => {
					let mapping = record_directory
						.column_to_buffers
						.get(*physical_ordinal)
						.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
					if mapping.buffer_count != 2 {
						return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					}
					let value_buffer_ordinal = IpcBufferIndex::new(
						mapping
							.first_buffer_ordinal
							.get()
							.checked_add(1)
							.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?,
					);
					let field = record_directory
						.ipc_schema
						.fields()
						.get(*physical_ordinal)
						.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
					let atom = if *selected_when_no_nulls {
						PredicateAtom::is_not_null()
					} else {
						PredicateAtom::is_null()
					};
					build_encoding_predicate_page_leaf_decisions(
						page_directory,
						encoding_directory,
						group,
						leaf_index,
						*physical_ordinal,
						value_buffer_ordinal,
						field.data_type(),
						&atom,
						config,
						&mut decisions,
						&mut fetches,
						&mut selected_value_bytes,
						&mut dense_value_bytes,
						&mut encoded_pages,
					)?;
				}
			}
		}
	}
	if selected_value_bytes >= dense_value_bytes {
		return Err(EncodingComputeFallbackReason::DenseSelection);
	}
	let mut record_body_ranges = fetches
		.iter()
		.map(|fetch| fetch.body_range)
		.collect::<Vec<_>>();
	coalesce_manager_record_body_ranges(&mut record_body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let body_ranges = record_body_ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let record_ranges = record_body_ranges_to_record_ranges(header_size, &body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(EncodingPredicatePagePlan {
		node,
		leaves,
		decisions,
		fetches,
		record_body_ranges,
		body_ranges,
		record_ranges,
	})
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_encoding_predicate_page_leaf_decisions(
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	group: &crate::storage::row_group::RowGroupDirectoryEntry,
	leaf_index: usize,
	physical_ordinal: usize,
	value_buffer_ordinal: IpcBufferIndex,
	data_type: &DataType,
	atom: &PredicateAtom,
	config: &EncodingComputeConfig,
	decisions: &mut Vec<EncodingPredicatePageDecision>,
	fetches: &mut Vec<EncodingPredicatePageFetch>,
	selected_value_bytes: &mut u64,
	dense_value_bytes: &mut u64,
	encoded_pages: &mut u64,
) -> Result<(), EncodingComputeFallbackReason> {
	let page = page_directory
		.buffer_pages
		.iter()
		.find(|page| {
			page.row_group_index == group.row_group_index
				&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
				&& page.ipc_buffer_ordinal == value_buffer_ordinal
				&& page.range.len.get() > 0
		})
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let encoding = find_buffer_encoding(
		encoding_directory,
		group.row_group_index,
		value_buffer_ordinal,
		page.range,
	)
	.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
	if encoding.decoded_value_count.get() != group.row_count.get()
		|| encoding.page_stats.is_empty()
		|| encoding.page_stats.len() != encoding.page_end_offsets.len()
	{
		return Err(EncodingComputeFallbackReason::UnsupportedCascade);
	}
	let cascade = encoding_directory
		.cascades
		.get(encoding.cascade_index.get() as usize)
		.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
	if !page_stats_cascade_eligible(cascade) {
		return Err(EncodingComputeFallbackReason::UnsupportedCascade);
	}
	let evaluator = evaluator_kind_for_cascade(cascade)
		.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
	if !config.predicate_evaluator_enabled(evaluator) {
		return Err(EncodingComputeFallbackReason::UnsupportedCascade);
	}
	if !matches!(
		atom.op,
		crate::storage::encoding::compute::PredicateAtomOp::IsNull
			| crate::storage::encoding::compute::PredicateAtomOp::IsNotNull
	) {
		let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type) else {
			return Err(EncodingComputeFallbackReason::UnsupportedProgram);
		};
		if matches!(
			native_evaluation_eligibility(cascade, atom, &coercion),
			NativeEvaluationEligibility::DecodeScalarFallback
		) {
			return Err(EncodingComputeFallbackReason::UnsupportedCascade);
		}
	}
	*dense_value_bytes = dense_value_bytes.saturating_add(
		encoding_compute_fetch_range_for_buffer_page(page.range, encoding)?
			.len
			.get(),
	);
	let pages = expand_page_descriptors(encoding)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let bytes_per_value = decoded_bytes_per_encoded_value(encoding).ok();
	for (page_descriptor, stat) in pages.iter().zip(&encoding.page_stats) {
		let classification =
			classify_page_stats_for_atom(stat, page_descriptor.value_count.get(), data_type, atom)
				.ok_or(EncodingComputeFallbackReason::UnsupportedProgram)?;
		let first_row = page_descriptor.first_value_index.get();
		let row_count = page_descriptor.value_count.get();
		match classification {
			PagePredicateClassification::Disjoint => {
				decisions.push(EncodingPredicatePageDecision {
					row_group_index: group.row_group_index,
					leaf_index,
					first_row,
					row_count,
					classification: EncodingPredicatePageDecisionKind::Disjoint,
				});
			}
			PagePredicateClassification::ExactMatch => {
				decisions.push(EncodingPredicatePageDecision {
					row_group_index: group.row_group_index,
					leaf_index,
					first_row,
					row_count,
					classification: EncodingPredicatePageDecisionKind::ExactMatch,
				});
			}
			PagePredicateClassification::Maybe => {
				if stat.null_count.get() != 0
					|| matches!(
						atom.op,
						crate::storage::encoding::compute::PredicateAtomOp::IsNull
							| crate::storage::encoding::compute::PredicateAtomOp::IsNotNull
					) {
					return Err(EncodingComputeFallbackReason::UnsupportedCascade);
				}
				let Some(bytes_per_value) = bytes_per_value else {
					return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
				};
				*encoded_pages = encoded_pages.saturating_add(1);
				if *encoded_pages > u64::from(config.max_encoded_pages_per_record.get()) {
					return Err(EncodingComputeFallbackReason::TooManyPages);
				}
				let body_range = encoding_compute_fetch_range_for_encoded_page(
					page.range,
					encoding,
					page_descriptor.byte_range,
				)?;
				*selected_value_bytes = selected_value_bytes.saturating_add(body_range.len.get());
				let fetch_index = fetches.len();
				fetches.push(EncodingPredicatePageFetch {
					body_range,
					encoding: encoding_for_predicate_page(
						encoding,
						row_count,
						bytes_per_value,
						page_descriptor.byte_range.len,
						stat.clone(),
					)?,
					cascade: cascade.clone(),
					data_type: data_type.clone(),
					atom: atom.clone(),
				});
				decisions.push(EncodingPredicatePageDecision {
					row_group_index: group.row_group_index,
					leaf_index,
					first_row,
					row_count,
					classification: EncodingPredicatePageDecisionKind::Maybe { fetch_index },
				});
			}
		}
	}
	Ok(())
}

pub(super) fn decoded_bytes_per_encoded_value(
	encoding: &BufferEncoding,
) -> Result<usize, EncodingComputeFallbackReason> {
	let values = usize::try_from(encoding.decoded_value_count.get())
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let bytes = usize::try_from(encoding.decoded_byte_len.get())
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if values == 0 || bytes == 0 || bytes % values != 0 {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	Ok(bytes / values)
}

pub(super) fn encoding_compute_fetch_range_for_encoded_page(
	buffer_range: crate::storage::row_group::RecordBodyByteRange,
	encoding: &BufferEncoding,
	page_range: EncodedBufferByteRange,
) -> Result<crate::storage::row_group::RecordBodyByteRange, EncodingComputeFallbackReason> {
	let relative_start = u64::from(encoding.encoded_range.start.get())
		.checked_add(u64::from(page_range.start.get()))
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let relative_end = relative_start
		.checked_add(page_range.len.get())
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let encoded_end = encoding
		.encoded_range
		.end()
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if relative_end > encoded_end || encoded_end > buffer_range.len.get() {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	let start = buffer_range
		.start
		.get()
		.checked_add(relative_start)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(crate::storage::row_group::RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: page_range.len,
	})
}

pub(super) fn encoding_for_predicate_page(
	encoding: &BufferEncoding,
	row_count: u32,
	bytes_per_value: usize,
	encoded_len: RecordBodyByteLen,
	stat: crate::storage::encoding::EncodingPageStats,
) -> Result<BufferEncoding, EncodingComputeFallbackReason> {
	let decoded_byte_len = u64::from(row_count)
		.checked_mul(
			u64::try_from(bytes_per_value)
				.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?,
		)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let encoded_len_u32 = u32::try_from(encoded_len.get())
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(BufferEncoding {
		row_group_index: encoding.row_group_index,
		ipc_column_ordinal: encoding.ipc_column_ordinal,
		ipc_buffer_ordinal: encoding.ipc_buffer_ordinal,
		cascade_index: encoding.cascade_index,
		decoded_byte_len: RecordBodyByteLen::new(decoded_byte_len),
		decoded_value_count: BufferValueCount::new(row_count),
		encoded_range: EncodedBufferByteRange {
			start: EncodingPageByteOffset::ZERO,
			len: encoded_len,
		},
		page_value_count: SimdPageValueCount::new(row_count),
		page_end_offsets: vec![EncodingPageByteOffset::new(encoded_len_u32)],
		page_stats: vec![stat],
	})
}

pub(super) fn build_encoding_compute_aggregate_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	request: &AggregateRequest,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	config: &EncodingComputeConfig,
	header_size: u64,
) -> Result<EncodingComputeAggregatePlan, EncodingComputeFallbackReason> {
	let mut fetches = Vec::new();
	let mut fetched_columns = BTreeSet::new();
	let mut pages_fetched = 0_u64;
	for expression in &request.aggregates {
		let Some(kind) = aggregate_kind_for_tag(expression.tag) else {
			return Err(EncodingComputeFallbackReason::AggregateUnsupported);
		};
		if !config.aggregate_kernel_enabled(kind) {
			return Err(EncodingComputeFallbackReason::AggregateUnsupported);
		}
		let function = expression
			.function()
			.map_err(|_| EncodingComputeFallbackReason::AggregateUnsupported)?;
		let Some(column) = function.column() else {
			continue;
		};
		if fetched_columns.contains(&column) {
			continue;
		}
		let physical_ordinal =
			encoding_compute_physical_user_column_ordinal(record_directory, column as usize)?;
		let validity_fetch = encoding_compute_validity_page_for_row_group(
			record_directory,
			page_directory,
			encoding_directory,
			group.row_group_index,
			physical_ordinal,
		)?;
		if validity_fetch.is_some() {
			pages_fetched = pages_fetched.saturating_add(1);
			if pages_fetched > u64::from(config.max_encoded_pages_per_record.get()) {
				return Err(EncodingComputeFallbackReason::TooManyPages);
			}
		}
		let field = record_directory
			.ipc_schema
			.fields()
			.get(physical_ordinal)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if matches!(
			function,
			crate::schema::AggregateFunction::CountNonNull { .. }
		) {
			fetches.push(EncodingComputeAggregateFetch {
				column,
				value_body_range: None,
				validity_fetch,
				encoding: None,
				cascade: None,
				data_type: field.data_type().clone(),
			});
			fetched_columns.insert(column);
			continue;
		}
		let (_, value_buffer_ordinal) =
			encoding_compute_value_buffer(record_directory, column as usize)?;
		let page = page_directory
			.buffer_pages
			.iter()
			.find(|page| {
				page.row_group_index == group.row_group_index
					&& page.ipc_column_ordinal == IpcColumnOrdinal::new(physical_ordinal as u32)
					&& page.ipc_buffer_ordinal == value_buffer_ordinal
					&& page.range.len.get() > 0
			})
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		let encoding = find_buffer_encoding(
			encoding_directory,
			group.row_group_index,
			value_buffer_ordinal,
			page.range,
		)
		.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
		if encoding.decoded_value_count.get() != group.row_count.get() {
			return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
		}
		let cascade = encoding_directory
			.cascades
			.get(encoding.cascade_index.get() as usize)
			.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
		if evaluator_kind_for_cascade(cascade).is_none() {
			return Err(EncodingComputeFallbackReason::UnsupportedCascade);
		}
		if !native_aggregate_cascade_eligible(&function, field.data_type(), Some(cascade)) {
			return Err(EncodingComputeFallbackReason::AggregateUnsupported);
		}
		pages_fetched = pages_fetched.saturating_add(encoding.page_end_offsets.len().max(1) as u64);
		if pages_fetched > u64::from(config.max_encoded_pages_per_record.get()) {
			return Err(EncodingComputeFallbackReason::TooManyPages);
		}
		fetches.push(EncodingComputeAggregateFetch {
			column,
			value_body_range: Some(encoding_compute_fetch_range_for_buffer_page(
				page.range, encoding,
			)?),
			validity_fetch,
			encoding: Some(encoding.clone()),
			cascade: Some(cascade.clone()),
			data_type: field.data_type().clone(),
		});
		fetched_columns.insert(column);
	}
	let mut record_body_ranges = fetches
		.iter()
		.flat_map(|fetch| {
			[
				fetch.value_body_range,
				fetch
					.validity_fetch
					.as_ref()
					.map(|validity| validity.body_range),
			]
		})
		.flatten()
		.collect::<Vec<_>>();
	coalesce_manager_record_body_ranges(&mut record_body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let body_ranges = record_body_ranges
		.iter()
		.map(|range| TieredPlaintextRange::new(range.start, range.len))
		.collect::<Vec<_>>();
	let record_ranges = record_body_ranges_to_record_ranges(header_size, &body_ranges)
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(EncodingComputeAggregatePlan {
		fetches,
		record_body_ranges,
		body_ranges,
		record_ranges,
		pages_fetched,
	})
}

pub(super) fn encoding_compute_row_group_column_null_count(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	physical_ordinal: usize,
) -> Option<RowCount> {
	let physical = IpcColumnOrdinal::new(physical_ordinal as u32);
	if let Some(stat) = page_directory.row_group_stats.iter().find(|stat| {
		stat.row_group_index == group.row_group_index && stat.ipc_column_ordinal == physical
	}) {
		return Some(stat.null_count);
	}
	let record_nulls = record_directory
		.column_null_counts
		.get(physical_ordinal)
		.copied()?;
	if record_nulls == 0 {
		return Some(RowCount::new(0));
	}
	let record_nulls = u64::try_from(record_nulls).ok()?;
	if record_nulls == page_directory.row_count.get() {
		return Some(RowCount::new(u64::from(group.row_count.get())));
	}
	None
}

pub(super) fn encoding_compute_row_group_column_is_non_null(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	physical_ordinal: usize,
) -> bool {
	encoding_compute_row_group_column_null_count(
		record_directory,
		page_directory,
		group,
		physical_ordinal,
	)
	.is_some_and(|count| count.get() == 0)
}

pub(super) fn encoding_compute_validity_page_for_row_group(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	row_group_index: RowGroupIndex,
	physical_ordinal: usize,
) -> Result<Option<EncodingComputeValidityFetch>, EncodingComputeFallbackReason> {
	let mapping = record_directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if !matches!(mapping.buffer_count, 2 | 3) {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	if page_directory
		.row_groups
		.iter()
		.find(|group| group.row_group_index == row_group_index)
		.is_some_and(|group| {
			encoding_compute_row_group_column_is_non_null(
				record_directory,
				page_directory,
				*group,
				physical_ordinal,
			)
		}) {
		return Ok(None);
	}
	let physical = IpcColumnOrdinal::new(physical_ordinal as u32);
	let validity_buffer_ordinal = mapping.first_buffer_ordinal;
	let Some(page) = page_directory.buffer_pages.iter().find(|page| {
		page.row_group_index == row_group_index
			&& page.ipc_column_ordinal == physical
			&& page.ipc_buffer_ordinal == validity_buffer_ordinal
	}) else {
		let known_non_null = page_directory
			.row_groups
			.iter()
			.find(|group| group.row_group_index == row_group_index)
			.is_some_and(|group| {
				encoding_compute_row_group_column_is_non_null(
					record_directory,
					page_directory,
					*group,
					physical_ordinal,
				)
			});
		return if known_non_null {
			Ok(None)
		} else {
			Err(EncodingComputeFallbackReason::UnsupportedArrowLayout)
		};
	};
	if page.range.len.get() == 0 {
		Ok(None)
	} else {
		let encoding = find_buffer_encoding(
			encoding_directory,
			row_group_index,
			validity_buffer_ordinal,
			page.range,
		);
		let (encoding, cascade) = match encoding {
			Some(encoding) => {
				let cascade = encoding_directory
					.cascades
					.get(encoding.cascade_index.get() as usize)
					.ok_or(EncodingComputeFallbackReason::UnsupportedCascade)?;
				if !encoded_validity_cascade_supported(cascade) {
					return Err(EncodingComputeFallbackReason::UnsupportedCascade);
				}
				(Some(encoding.clone()), Some(cascade.clone()))
			}
			None => (None, None),
		};
		let body_range = match encoding.as_ref() {
			Some(encoding) => encoding_compute_fetch_range_for_buffer_page(page.range, encoding)?,
			None => page.range,
		};
		Ok(Some(EncodingComputeValidityFetch {
			body_range,
			encoding,
			cascade,
		}))
	}
}

pub(super) fn encoding_compute_single_row_mask(
	rows: RowCount,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	record_row_index: RecordRowIndex,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let offset = record_row_index
		.get()
		.checked_sub(group.first_record_row_index.get())
		.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
	if offset >= group.row_count.get() {
		return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
	}
	let mut values = vec![
		false;
		usize::try_from(rows.get())
			.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?
	];
	let offset =
		usize::try_from(offset).map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
	values[offset] = true;
	Ok(EncodedRowMask::from_bool_slice(&values))
}

pub(super) fn encoding_compute_aggregate_kernels_enabled(
	request: &AggregateRequest,
	config: &EncodingComputeConfig,
) -> bool {
	request.aggregates.iter().all(|expression| {
		aggregate_kind_for_tag(expression.tag)
			.is_some_and(|kind| config.aggregate_kernel_enabled(kind))
	})
}

pub(super) struct EncodingComputePhaseBPlanBuilder<'a> {
	pub(super) record_directory: &'a RecordBufferDirectory,
	pub(super) leaves: Vec<EncodingComputePhaseBLeaf>,
}

impl EncodingComputePhaseBPlanBuilder<'_> {
	fn build_node(
		&mut self,
		node: &EncodedPredicateNode,
	) -> Result<EncodingComputePhaseBNode, EncodingComputeFallbackReason> {
		match node {
			EncodedPredicateNode::Comparison(comparison) => {
				self.push_encoded_leaf(comparison.column.column_index, comparison.atom.clone())
			}
			EncodedPredicateNode::InList(in_list) => {
				self.push_encoded_leaf(in_list.column.column_index, in_list.atom.clone())
			}
			EncodedPredicateNode::IsNull(column) => {
				self.push_trivial_null_leaf(column.column_index, false)
			}
			EncodedPredicateNode::IsNotNull(column) => {
				self.push_trivial_null_leaf(column.column_index, true)
			}
			EncodedPredicateNode::And(branches) => {
				if branches.is_empty() {
					return Err(EncodingComputeFallbackReason::UnsupportedProgram);
				}
				let nodes = branches
					.iter()
					.map(|branch| self.build_node(branch))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(EncodingComputePhaseBNode::And(nodes))
			}
			EncodedPredicateNode::Or(branches) => {
				if branches.is_empty() {
					return Err(EncodingComputeFallbackReason::UnsupportedProgram);
				}
				let nodes = branches
					.iter()
					.map(|branch| self.build_node(branch))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(EncodingComputePhaseBNode::Or(nodes))
			}
			EncodedPredicateNode::Not(branch) => Ok(EncodingComputePhaseBNode::Not(Box::new(
				self.build_node(branch)?,
			))),
		}
	}

	fn push_encoded_leaf(
		&mut self,
		column_index: usize,
		atom: PredicateAtom,
	) -> Result<EncodingComputePhaseBNode, EncodingComputeFallbackReason> {
		let buffers = encoding_compute_column_buffers(self.record_directory, column_index)?;
		let field = self
			.record_directory
			.ipc_schema
			.fields()
			.get(buffers.physical_ordinal)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
		if !encoding_compute_predicate_data_type_candidate_supported(field.data_type()) {
			return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
		}
		if coerce_literal(atom.literal.as_ref(), field.data_type()).is_none() {
			return Err(EncodingComputeFallbackReason::UnsupportedProgram);
		}
		let leaf_index = self.leaves.len();
		self.leaves.push(EncodingComputePhaseBLeaf::Encoded {
			column_index,
			physical_ordinal: buffers.physical_ordinal,
			value_buffer_ordinal: buffers.value_buffer_ordinal,
			offsets_buffer_ordinal: buffers.offsets_buffer_ordinal,
			atom,
			data_type: field.data_type().clone(),
		});
		Ok(EncodingComputePhaseBNode::Leaf(leaf_index))
	}

	fn push_trivial_null_leaf(
		&mut self,
		column_index: usize,
		selected_when_no_nulls: bool,
	) -> Result<EncodingComputePhaseBNode, EncodingComputeFallbackReason> {
		let physical_ordinal =
			encoding_compute_physical_user_column_ordinal(self.record_directory, column_index)?;
		let leaf_index = self.leaves.len();
		self.leaves.push(EncodingComputePhaseBLeaf::Validity {
			physical_ordinal,
			selected_when_no_nulls,
		});
		Ok(EncodingComputePhaseBNode::Leaf(leaf_index))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EncodingComputeColumnBuffers {
	pub(super) physical_ordinal: usize,
	pub(super) value_buffer_ordinal: IpcBufferIndex,
	pub(super) offsets_buffer_ordinal: Option<IpcBufferIndex>,
}

pub(super) fn encoding_compute_physical_user_column_ordinal(
	directory: &RecordBufferDirectory,
	descriptor_ordinal: usize,
) -> Result<usize, EncodingComputeFallbackReason> {
	let user_physical_ordinals = directory
		.ipc_schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(physical, field)| {
			StorageColumn::from_name(field.name())
				.is_none()
				.then_some(physical)
		})
		.collect::<Vec<_>>();
	user_physical_ordinals
		.get(descriptor_ordinal)
		.copied()
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)
}

pub(super) fn encoding_compute_value_buffer(
	directory: &RecordBufferDirectory,
	descriptor_ordinal: usize,
) -> Result<(usize, IpcBufferIndex), EncodingComputeFallbackReason> {
	let buffers = encoding_compute_column_buffers(directory, descriptor_ordinal)?;
	Ok((buffers.physical_ordinal, buffers.value_buffer_ordinal))
}

pub(super) fn encoding_compute_column_buffers(
	directory: &RecordBufferDirectory,
	descriptor_ordinal: usize,
) -> Result<EncodingComputeColumnBuffers, EncodingComputeFallbackReason> {
	let physical_ordinal =
		encoding_compute_physical_user_column_ordinal(directory, descriptor_ordinal)?;
	let mapping = directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	let first = mapping.first_buffer_ordinal.get();
	let (offsets_buffer_ordinal, value_offset) = match mapping.buffer_count {
		2 => (None, 1),
		3 => (
			Some(IpcBufferIndex::new(first.checked_add(1).ok_or(
				EncodingComputeFallbackReason::UnsupportedArrowLayout,
			)?)),
			2,
		),
		_ => return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout),
	};
	let value_buffer_ordinal = IpcBufferIndex::new(
		first
			.checked_add(value_offset)
			.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?,
	);
	Ok(EncodingComputeColumnBuffers {
		physical_ordinal,
		value_buffer_ordinal,
		offsets_buffer_ordinal,
	})
}

pub(super) fn encoding_compute_predicate_data_type_candidate_supported(
	data_type: &DataType,
) -> bool {
	encoding_compute_predicate_data_type_supported(data_type)
		|| encoding_compute_fsst_predicate_data_type_supported(data_type)
}

pub(super) fn encoding_compute_predicate_data_type_supported_for_evaluator(
	data_type: &DataType,
	evaluator: crate::storage::encoding::compute::EncodingComputeEvaluatorKind,
) -> bool {
	if matches!(
		evaluator,
		crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstWholeBuffer
			| crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstRowSafe
	) {
		return encoding_compute_fsst_predicate_data_type_supported(data_type);
	}
	encoding_compute_fixed_width_predicate_data_type_supported(data_type)
}

pub(super) fn encoding_compute_predicate_data_type_supported(data_type: &DataType) -> bool {
	encoding_compute_fixed_width_predicate_data_type_supported(data_type)
}

pub(super) fn encoding_compute_fixed_width_predicate_data_type_supported(
	data_type: &DataType,
) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Float32
			| DataType::Float64
			| DataType::Date32
			| DataType::Date64
			| DataType::Time32(_)
			| DataType::Time64(_)
			| DataType::Timestamp(_, _)
			| DataType::Duration(_)
			| DataType::FixedSizeBinary(_)
	)
}

pub(super) fn encoding_compute_fsst_predicate_data_type_supported(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
	)
}

pub(super) fn encoding_compute_fetch_range_for_buffer_page(
	buffer_range: crate::storage::row_group::RecordBodyByteRange,
	encoding: &BufferEncoding,
) -> Result<crate::storage::row_group::RecordBodyByteRange, EncodingComputeFallbackReason> {
	let encoded_start = u64::from(encoding.encoded_range.start.get());
	let encoded_end = encoding
		.encoded_range
		.end()
		.map_err(|_| EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	if encoded_end > buffer_range.len.get() {
		return Err(EncodingComputeFallbackReason::UnsupportedArrowLayout);
	}
	let start = buffer_range
		.start
		.get()
		.checked_add(encoded_start)
		.ok_or(EncodingComputeFallbackReason::UnsupportedArrowLayout)?;
	Ok(crate::storage::row_group::RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: encoding.encoded_range.len,
	})
}

pub(super) fn encoding_compute_mask_selectivity_allowed(
	mask: &EncodedRowMask,
	config: &EncodingComputeConfig,
) -> Result<bool, EncodingComputeRejectCode> {
	let rows = mask.rows().get();
	let selected = mask.popcount()?.get();
	let micros = if rows == 0 {
		0
	} else {
		selected
			.saturating_mul(1_000_000)
			.checked_div(rows)
			.unwrap_or(1_000_000)
			.min(1_000_000) as u32
	};
	Ok(encoded_leaf_selectivity_allowed(
		SelectivityFractionMicros::new(micros),
		config,
	))
}

pub(super) fn encoding_compute_eval_node(
	node: &EncodingComputePhaseBNode,
	leaf_masks: &[EncodedRowMask],
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	match node {
		EncodingComputePhaseBNode::Leaf(index) => leaf_masks
			.get(*index)
			.cloned()
			.ok_or(EncodingComputeRejectCode::MaskLengthMismatch),
		EncodingComputePhaseBNode::And(branches) => {
			let mut iter = branches.iter();
			let first = iter
				.next()
				.ok_or(EncodingComputeRejectCode::MaskLengthMismatch)?;
			let mut out = encoding_compute_eval_node(first, leaf_masks)?;
			for branch in iter {
				out = out.and(&encoding_compute_eval_node(branch, leaf_masks)?)?;
			}
			Ok(out)
		}
		EncodingComputePhaseBNode::Or(branches) => {
			let mut iter = branches.iter();
			let first = iter
				.next()
				.ok_or(EncodingComputeRejectCode::MaskLengthMismatch)?;
			let mut out = encoding_compute_eval_node(first, leaf_masks)?;
			for branch in iter {
				out = out.or(&encoding_compute_eval_node(branch, leaf_masks)?)?;
			}
			Ok(out)
		}
		EncodingComputePhaseBNode::Not(branch) => {
			encoding_compute_eval_node(branch, leaf_masks)?.not()
		}
	}
}

pub(super) fn encoding_compute_invariant_error(code: EncodingComputeRejectCode) -> std::io::Error {
	metrics().observe_storage_l2_encoding_compute_invariant_violation(code.as_str());
	Error::new(
		ErrorKind::InvalidData,
		format!("Encoding compute invariant violation: {code:?}"),
	)
}

pub(super) fn phase_b_point_read_survives(
	phase_b: &crate::storage::row_group::PhaseBOutcome,
	group: crate::storage::row_group::RowGroupDirectoryEntry,
	record_row_index: RecordRowIndex,
) -> std::io::Result<bool> {
	let survival_mask = phase_b
		.survival_masks
		.get(&group.row_group_index)
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"Phase B survival mask missing for target row group {}",
					group.row_group_index.get()
				),
			)
		})?;
	let row_offset = record_row_index
		.get()
		.checked_sub(group.first_record_row_index.get())
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record row index {} precedes row group {} base {}",
					record_row_index.get(),
					group.row_group_index.get(),
					group.first_record_row_index.get()
				),
			)
		})? as usize;
	if row_offset >= survival_mask.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"record row index {} maps outside Phase B mask length {} for row group {}",
				record_row_index.get(),
				survival_mask.len(),
				group.row_group_index.get()
			),
		));
	}
	Ok(survival_mask.is_valid(row_offset) && survival_mask.value(row_offset))
}

pub(super) fn build_encoding_indexed_page_plan(
	record_directory: &RecordBufferDirectory,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	encoding_directory: &EncodingDirectory,
	requirement: &ColumnFetchRequirement,
	selected_record_rows: &[RecordRowIndex],
	selected_groups: &[RowGroupIndex],
) -> Result<Option<EncodingIndexedPagePlan>, EncodingFallbackReason> {
	let physical_ordinals = required_projected_physical_ordinals(record_directory, requirement)
		.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
	if physical_ordinals.is_empty() {
		return Ok(Some(EncodingIndexedPagePlan {
			selected_groups: selected_groups.to_vec(),
			row_pages: selected_record_rows
				.iter()
				.map(|row| {
					let group =
						crate::storage::row_group::row_group_for_record_row(page_directory, *row)
							.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
					Ok(EncodingIndexedRowPage {
						page_key: PageMaterializeKey {
							row_group_index: group.row_group_index,
							page_index: 0,
						},
						row_offset_in_page: 0,
					})
				})
				.collect::<Result<Vec<_>, _>>()?,
			columns: Vec::new(),
			fetches: Vec::new(),
		}));
	}

	let mut columns = Vec::with_capacity(physical_ordinals.len());
	for physical_ordinal in physical_ordinals {
		let column = fixed_width_encoding_indexed_value_column(record_directory, physical_ordinal)
			.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
		columns.push(column);
	}

	let first_column = columns
		.first()
		.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
	let mut row_pages = Vec::with_capacity(selected_record_rows.len());
	let mut fetches_by_key = BTreeMap::<PageMaterializeBufferKey, PageMaterializeFetch>::new();
	for row in selected_record_rows {
		let group = crate::storage::row_group::row_group_for_record_row(page_directory, *row)
			.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
		let row_within_group = row
			.get()
			.checked_sub(group.first_record_row_index.get())
			.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
		let mut page_key = None;
		for column in &columns {
			let column_page = page_directory
				.buffer_pages
				.iter()
				.find(|page| {
					page.row_group_index == group.row_group_index
						&& page.ipc_column_ordinal
							== IpcColumnOrdinal::new(column.physical_ordinal as u32)
						&& page.ipc_buffer_ordinal == column.value_buffer_ordinal
						&& page.range.len.get() > 0
				})
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
			let encoding = encoding_directory
				.buffers
				.iter()
				.find(|encoding| {
					encoding.row_group_index == group.row_group_index
						&& encoding.ipc_column_ordinal == column_page.ipc_column_ordinal
						&& encoding.ipc_buffer_ordinal == column.value_buffer_ordinal
						&& !encoding.page_end_offsets.is_empty()
				})
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
			let pages = expand_page_descriptors(encoding)
				.map_err(|_| EncodingFallbackReason::UnsupportedArrowLayout)?;
			let page = pages
				.into_iter()
				.find(|page| {
					let start = page.first_value_index.get();
					let end = start.saturating_add(page.value_count.get());
					row_within_group >= start && row_within_group < end
				})
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?;
			let candidate_page_key = PageMaterializeKey {
				row_group_index: group.row_group_index,
				page_index: page.page_index.get(),
			};
			if column.physical_ordinal == first_column.physical_ordinal {
				let row_offset = row_within_group
					.checked_sub(page.first_value_index.get())
					.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?
					as usize;
				page_key = Some((candidate_page_key, row_offset));
			} else if Some(candidate_page_key) != page_key.map(|(key, _)| key) {
				return Err(EncodingFallbackReason::UnsupportedArrowLayout);
			}
			let cascade = encoding_directory
				.cascades
				.get(encoding.cascade_index.get() as usize)
				.ok_or(EncodingFallbackReason::UnsupportedArrowLayout)?
				.clone();
			let fetch_key = PageMaterializeBufferKey {
				row_group_index: group.row_group_index,
				ipc_column_ordinal: column_page.ipc_column_ordinal,
				ipc_buffer_ordinal: column.value_buffer_ordinal,
				page_index: page.page_index.get(),
			};
			fetches_by_key.entry(fetch_key).or_insert_with(|| {
				metrics()
					.observe_storage_l2_encoding_simd_pages_fetched(cascade.kind().as_str(), 1);
				PageMaterializeFetch {
					key: fetch_key,
					body_range: encoded_page_body_range(column_page.range, encoding, page),
					cascade,
					page,
					bytes_per_value: column.bytes_per_value,
					data_type: column.data_type.clone(),
				}
			});
		}
		let Some((page_key, row_offset_in_page)) = page_key else {
			return Err(EncodingFallbackReason::UnsupportedArrowLayout);
		};
		row_pages.push(EncodingIndexedRowPage {
			page_key,
			row_offset_in_page,
		});
	}

	let fetches = fetches_by_key.into_values().collect::<Vec<_>>();
	if fetches.is_empty() && !columns.is_empty() {
		return Ok(None);
	}
	Ok(Some(EncodingIndexedPagePlan {
		selected_groups: selected_groups.to_vec(),
		row_pages,
		columns,
		fetches,
	}))
}

pub(super) fn encoding_materialize_audit_for_indexed_plan(
	plan: &EncodingIndexedPagePlan,
) -> Vec<EncodingMaterializeAuditSample> {
	let mut by_cascade = BTreeMap::<&'static str, (u64, u64)>::new();
	for fetch in &plan.fetches {
		let decoded =
			u64::from(fetch.page.value_count.get()).saturating_mul(fetch.bytes_per_value as u64);
		let entry = by_cascade
			.entry(fetch.cascade.kind().as_str())
			.or_insert((0, 0));
		entry.0 = entry.0.saturating_add(fetch.body_range.len.get());
		entry.1 = entry.1.saturating_add(decoded);
	}
	by_cascade
		.into_iter()
		.map(
			|(cascade, (buffer_bytes_in, buffer_bytes_out))| EncodingMaterializeAuditSample {
				cascade,
				buffer_bytes_in,
				buffer_bytes_out,
			},
		)
		.collect()
}

#[derive(Clone, Debug)]
pub(super) struct SparseProjectionPageBatch {
	pub(super) row_group_index: RowGroupIndex,
	pub(super) page: crate::storage::encoding::EncodingPageDescriptor,
	pub(super) local_mask: BooleanArray,
	pub(super) batch: ProjectedRecordBatch,
}

pub(super) fn materialize_sparse_projection_pages<B: AsRef<[u8]>>(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plan: &SparseProjectionFetchPlan,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
	predicate_outcome: &crate::storage::row_group::PhaseBOutcome,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let page_batches = materialize_sparse_projection_page_batches(
		record_directory,
		requirement,
		plan,
		selected_ranges,
		plaintext_extracts,
		predicate_outcome,
	)?;
	concat_sparse_projection_page_batches(record_directory, requirement, page_batches)
}

pub(super) fn materialize_sparse_projection_page_batches<B: AsRef<[u8]>>(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plan: &SparseProjectionFetchPlan,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
	predicate_outcome: &crate::storage::row_group::PhaseBOutcome,
) -> Result<Vec<SparseProjectionPageBatch>, arrow::error::ArrowError> {
	if plan.selected_groups.is_empty() {
		return Ok(Vec::new());
	}
	let mut decoded_pages = BTreeMap::<PageMaterializeBufferKey, Vec<u8>>::new();
	for fetch in &plan.fetches {
		let encoded =
			extract_encoded_page_bytes(fetch.body_range, selected_ranges, plaintext_extracts)?;
		let decoded = decode_page_fetch(fetch, &encoded)?;
		decoded_pages.insert(fetch.key, decoded);
	}
	let mut decoded_validity_pages =
		BTreeMap::<PageMaterializeBufferKey, PageValidityBitmap>::new();
	for fetch in &plan.validity_fetches {
		if fetch.page.value_count.get() != fetch.page_row_count.get() {
			return Err(arrow::error::ArrowError::SchemaError(format!(
				"sparse projection validity fetch page row count {} disagrees with page descriptor {}",
				fetch.page_row_count.get(),
				fetch.page.value_count.get()
			)));
		}
		let fetched =
			extract_encoded_page_bytes(fetch.body_range, selected_ranges, plaintext_extracts)?;
		let validity = match fetch.source {
			PageMaterializeValiditySource::Raw { bit_offset } => {
				rebase_validity_bitmap(&fetched, bit_offset, fetch.page_row_count.get() as usize)?
			}
			PageMaterializeValiditySource::Encoded { cascade } => {
				let _ = cascade;
				decode_rle_validity_page(&fetched, fetch.page_row_count.get() as usize)?
			}
		};
		decoded_validity_pages.insert(fetch.key, validity);
	}

	let mut filtered = Vec::new();
	for group in &plan.group_pages {
		let mask = predicate_outcome
			.survival_masks
			.get(&group.row_group_index)
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(format!(
					"Sparse projection mask missing for row group {}",
					group.row_group_index.get()
				))
			})?;
		for page in &group.selected_pages {
			let page_key = PageMaterializeKey {
				row_group_index: group.row_group_index,
				page_index: page.page_index.get(),
			};
			let start = page.first_value_index.get() as usize;
			let len = page.value_count.get() as usize;
			if start.checked_add(len).is_none_or(|end| end > mask.len()) {
				return Err(arrow::error::ArrowError::SchemaError(format!(
					"Sparse projection page mask range {}..{} exceeds mask length {}",
					start,
					start.saturating_add(len),
					mask.len()
				)));
			}
			let local_mask = arrow::array::BooleanArray::from_iter(
				(start..start + len).map(|row| Some(mask.is_valid(row) && mask.value(row))),
			);
			if local_mask.true_count() == 0 {
				continue;
			}
			synthesize_all_null_projection_page(
				plan,
				page_key,
				len,
				&mut decoded_pages,
				&mut decoded_validity_pages,
			)?;
			let page_batch = projected_page_batch_from_decoded_buffers(
				record_directory,
				requirement,
				&plan.columns,
				page_key,
				len,
				&decoded_pages,
				&decoded_validity_pages,
				&plan.all_valid_nullable_pages,
			)?;
			if local_mask.null_count() == 0 && local_mask.true_count() == local_mask.len() {
				filtered.push(SparseProjectionPageBatch {
					row_group_index: group.row_group_index,
					page: *page,
					local_mask,
					batch: page_batch,
				});
				continue;
			}
			let batch = arrow::compute::filter_record_batch(&page_batch.batch, &local_mask)?;
			filtered.push(SparseProjectionPageBatch {
				row_group_index: group.row_group_index,
				page: *page,
				local_mask,
				batch: ProjectedRecordBatch {
					batch,
					ordinal_mapping: page_batch.ordinal_mapping,
					storage_mapping: page_batch.storage_mapping,
				},
			});
		}
	}
	Ok(filtered)
}

pub(super) fn synthesize_all_null_projection_page(
	plan: &SparseProjectionFetchPlan,
	page_key: PageMaterializeKey,
	page_row_count: usize,
	decoded_pages: &mut BTreeMap<PageMaterializeBufferKey, Vec<u8>>,
	decoded_validity_pages: &mut BTreeMap<PageMaterializeBufferKey, PageValidityBitmap>,
) -> Result<(), arrow::error::ArrowError> {
	for column in &plan.columns {
		let value_key = PageMaterializeBufferKey {
			row_group_index: page_key.row_group_index,
			ipc_column_ordinal: IpcColumnOrdinal::new(column.physical_ordinal as u32),
			ipc_buffer_ordinal: column.value_buffer_ordinal,
			page_index: page_key.page_index,
		};
		let value_source = match plan.value_page_dispositions.get(&value_key) {
			Some(SelectedPageDisposition::AllNullElided {
				page_row_count: disposition_page_row_count,
				bytes_per_value,
			}) => {
				if !column.nullable {
					return Err(arrow::error::ArrowError::SchemaError(
						"all-null synthesized page requested for non-nullable column".into(),
					));
				}
				if *disposition_page_row_count as usize != page_row_count {
					return Err(arrow::error::ArrowError::SchemaError(format!(
						"all-null synthesized page row count {} disagrees with descriptor {}",
						disposition_page_row_count, page_row_count
					)));
				}
				if *bytes_per_value != column.bytes_per_value {
					return Err(arrow::error::ArrowError::SchemaError(format!(
						"all-null synthesized bytes_per_value {} disagrees with column {}",
						bytes_per_value, column.bytes_per_value
					)));
				}
				PageMaterializeValueSource::AllNullSynthesized
			}
			_ => PageMaterializeValueSource::Decoded,
		};
		match value_source {
			PageMaterializeValueSource::Decoded => {}
			PageMaterializeValueSource::AllNullSynthesized => {
				let (value_bytes, validity) =
					synthesize_all_null_page_buffers(page_row_count, column.bytes_per_value)?;
				decoded_pages.insert(value_key, value_bytes);
				decoded_validity_pages.insert(
					PageMaterializeBufferKey {
						row_group_index: page_key.row_group_index,
						ipc_column_ordinal: IpcColumnOrdinal::new(column.physical_ordinal as u32),
						ipc_buffer_ordinal: column.null_buffer_ordinal,
						page_index: page_key.page_index,
					},
					validity,
				);
			}
		}
	}
	Ok(())
}

pub(super) fn concat_sparse_projection_page_batches(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	page_batches: Vec<SparseProjectionPageBatch>,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	if page_batches.is_empty() {
		return empty_projected_batch_for_requirement(record_directory, requirement, 0);
	}
	concat_projected_record_batches(
		page_batches
			.into_iter()
			.map(|page_batch| page_batch.batch)
			.collect(),
	)
}

pub(super) fn materialize_encoding_indexed_pages<B: AsRef<[u8]>>(
	record_directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plan: &EncodingIndexedPagePlan,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
	selected_row_count: usize,
	descriptor: &TableSchemaDescriptor,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	if plan.columns.is_empty() {
		return projected_batch_for_empty_indexed_rows(
			record_directory,
			selected_row_count,
			descriptor,
		);
	}
	let mut decoded_pages = BTreeMap::<PageMaterializeBufferKey, Vec<u8>>::new();
	for fetch in &plan.fetches {
		let encoded =
			extract_encoded_page_bytes(fetch.body_range, selected_ranges, plaintext_extracts)?;
		let decoded = decode_page_fetch(fetch, &encoded)?;
		decoded_pages.insert(fetch.key, decoded);
	}
	let decoded_validity_pages = BTreeMap::<PageMaterializeBufferKey, PageValidityBitmap>::new();
	let all_valid_nullable_pages = BTreeSet::<PageMaterializeBufferKey>::new();

	let mut page_batches = BTreeMap::<PageMaterializeKey, ProjectedRecordBatch>::new();
	for page_key in plan.row_pages.iter().map(|row| row.page_key) {
		if page_batches.contains_key(&page_key) {
			continue;
		}
		let page_fetch = plan
			.fetches
			.iter()
			.find(|fetch| {
				fetch.key.row_group_index == page_key.row_group_index
					&& fetch.key.page_index == page_key.page_index
			})
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed page batch missing fetch".into(),
				)
			})?;
		let page_batch = projected_page_batch_from_decoded_buffers(
			record_directory,
			requirement,
			&plan.columns,
			page_key,
			page_fetch.page.value_count.get() as usize,
			&decoded_pages,
			&decoded_validity_pages,
			&all_valid_nullable_pages,
		)?;
		page_batches.insert(page_key, page_batch);
	}

	let mut rows = Vec::with_capacity(plan.row_pages.len());
	for row_page in &plan.row_pages {
		let page_batch = page_batches.get(&row_page.page_key).ok_or_else(|| {
			arrow::error::ArrowError::SchemaError(
				"Encoding indexed page batch missing for selected row".into(),
			)
		})?;
		rows.push(
			page_batch
				.take_rows(&[row_page.row_offset_in_page])
				.map_err(|err| arrow::error::ArrowError::ExternalError(Box::new(err)))?,
		);
	}
	concat_projected_record_batches(rows)
}

pub(super) fn projected_batch_for_empty_indexed_rows(
	record_directory: &RecordBufferDirectory,
	row_count: usize,
	descriptor: &TableSchemaDescriptor,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
	let batch = arrow::record_batch::RecordBatch::try_new_with_options(
		schema,
		Vec::new(),
		&arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
	)?;
	let descriptor_len = descriptor
		.arrow_schema()
		.map(|schema| schema.fields().len())
		.unwrap_or_else(|_| record_directory.ipc_schema.fields().len());
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::new(vec![None; descriptor_len]),
		storage_mapping: BTreeMap::new(),
	})
}

pub(super) fn concat_projected_record_batches(
	batches: Vec<ProjectedRecordBatch>,
) -> Result<ProjectedRecordBatch, arrow::error::ArrowError> {
	let Some(first) = batches.first() else {
		return Err(arrow::error::ArrowError::InvalidArgumentError(
			"Encoding indexed page materialization produced no batches".into(),
		));
	};
	let schema = first.batch.schema();
	let raw = batches
		.iter()
		.map(|batch| batch.batch.clone())
		.collect::<Vec<_>>();
	let batch = arrow::compute::concat_batches(&schema, &raw)?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: first.ordinal_mapping.clone(),
		storage_mapping: first.storage_mapping.clone(),
	})
}

pub(super) fn extract_record_body_bytes<B: AsRef<[u8]>>(
	range: crate::storage::row_group::RecordBodyByteRange,
	selected_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
	if selected_ranges.len() != plaintext_extracts.len() {
		return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
			"Encoding indexed page expected {} plaintext extracts, got {}",
			selected_ranges.len(),
			plaintext_extracts.len()
		)));
	}
	let start = range.start.get();
	let end = range
		.end()
		.map_err(|err| arrow::error::ArrowError::SchemaError(err.to_string()))?;
	for (candidate, extract) in selected_ranges.iter().zip(plaintext_extracts.iter()) {
		let candidate_start = candidate.start.get();
		let candidate_end = candidate_start
			.checked_add(candidate.len.get())
			.ok_or_else(|| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed plaintext range overflow".into(),
				)
			})?;
		if start >= candidate_start && end <= candidate_end {
			let relative_start = usize::try_from(start - candidate_start).map_err(|_| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed relative page start exceeds usize".into(),
				)
			})?;
			let relative_end = usize::try_from(end - candidate_start).map_err(|_| {
				arrow::error::ArrowError::SchemaError(
					"Encoding indexed relative page end exceeds usize".into(),
				)
			})?;
			let extract = extract.as_ref();
			if relative_end > extract.len() {
				return Err(arrow::error::ArrowError::SchemaError(
					"Encoding indexed page range exceeds plaintext extract".into(),
				));
			}
			return Ok(extract[relative_start..relative_end].to_vec());
		}
	}
	Err(arrow::error::ArrowError::SchemaError(
		"Encoding indexed page range was not fetched".into(),
	))
}

pub(super) fn coalesce_manager_record_body_ranges(
	ranges: &mut Vec<crate::storage::row_group::RecordBodyByteRange>,
) -> std::io::Result<()> {
	ranges.sort_by_key(|range| range.start.get());
	let mut coalesced = Vec::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		if range.len.get() == 0 {
			continue;
		}
		let Some(last): Option<&mut crate::storage::row_group::RecordBodyByteRange> =
			coalesced.last_mut()
		else {
			coalesced.push(range);
			continue;
		};
		let last_end = last.end()?;
		let range_end = range.end()?;
		if range.start.get() <= last_end {
			last.len = RecordBodyByteLen::new(range_end.max(last_end) - last.start.get());
		} else {
			coalesced.push(range);
		}
	}
	*ranges = coalesced;
	Ok(())
}

pub(super) fn encoding_compute_decode_error(code: EncodingRejectCode) -> std::io::Error {
	metrics().observe_storage_l2_encoding_invariant_violation(code.as_str());
	Error::new(
		ErrorKind::InvalidData,
		format!("Encoding compute validity decode rejected: {code:?}"),
	)
}

pub(super) fn record_body_location(location: RecordLocation) -> std::io::Result<RecordLocation> {
	let header_size = slab_record_header_size_u64()?;
	let body_offset = location.offset.checked_add(header_size).ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"record body offset overflows u64 for slab {} at offset {}",
				location.slab_id, location.offset
			),
		)
	})?;
	let body_len = u64::from(location.length)
		.checked_sub(header_size)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "record is shorter than slab header"))?;
	let body_len = u32::try_from(body_len).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record body length {body_len} exceeds u32::MAX"),
		)
	})?;
	Ok(RecordLocation {
		offset: body_offset,
		length: body_len,
		..location
	})
}

pub(super) fn record_plaintext_range_for_l2(
	location: RecordLocation,
) -> std::io::Result<std::ops::Range<usize>> {
	let start = usize::try_from(location.offset).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record offset {} does not fit into usize", location.offset),
		)
	})?;
	let end_u64 = location
		.offset
		.checked_add(u64::from(location.length))
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record range overflows u64 for slab {} at offset {}",
					location.slab_id, location.offset
				),
			)
		})?;
	let end = usize::try_from(end_u64).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record end {end_u64} does not fit into usize"),
		)
	})?;
	Ok(start..end)
}

pub(super) fn record_body_ranges_to_record_ranges(
	header_size: u64,
	body_ranges: &[TieredPlaintextRange],
) -> std::io::Result<Vec<TieredPlaintextRange>> {
	body_ranges
		.iter()
		.map(|range| {
			let start = header_size.checked_add(range.start.get()).ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "record body range start overflow")
			})?;
			Ok(TieredPlaintextRange::new(
				RecordBodyByteOffset::new(start),
				range.len,
			))
		})
		.collect()
}

pub(super) fn pick_point_row_group_fetch_strategy(
	config: &crate::storage::row_group::L2RowGroupFetchConfig,
	record_location: &RecordLocation,
	page_directory: &crate::storage::row_group::RecordPageDirectory,
	plan: crate::storage::row_group::RowGroupFetchPlan,
) -> L2FetchStrategy {
	if !config.enabled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
	}
	if plan.surviving_groups.is_empty() {
		return L2FetchStrategy::RowGroup(plan);
	}
	let selected_group_scaled = (plan.surviving_groups.len() as u64).saturating_mul(u64::from(
		crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
	));
	let allowed_group_scaled = (page_directory.row_groups.len() as u64)
		.saturating_mul(u64::from(config.max_selected_row_group_fraction_micros));
	if selected_group_scaled > allowed_group_scaled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRowGroups);
	}
	let selected_ciphertext_bytes = plan
		.phase_b_fetch
		.as_ref()
		.map(|phase_b| phase_b.fetched_ciphertext_bytes)
		.unwrap_or(0)
		.saturating_add(plan.phase_c_fetch.fetched_ciphertext_bytes);
	let selected_byte_scaled = selected_ciphertext_bytes.saturating_mul(u64::from(
		crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR,
	));
	let allowed_byte_scaled = u64::from(record_location.length)
		.saturating_mul(u64::from(config.max_selected_byte_fraction_micros));
	if selected_byte_scaled > allowed_byte_scaled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyBytes);
	}
	let selected_ranges = plan
		.phase_b_fetch
		.as_ref()
		.map(|phase_b| phase_b.chunk_runs.len())
		.unwrap_or(0)
		.saturating_add(plan.phase_c_fetch.chunk_runs.len());
	if selected_ranges > config.max_ranges_per_record.get() as usize {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRanges);
	}
	L2FetchStrategy::RowGroup(plan)
}

pub(super) fn observe_l2_row_group_fetch_strategy(
	metrics: &crate::metrics::MetricsRegistry,
	strategy: &L2FetchStrategy,
	planning_duration: Duration,
) {
	metrics.observe_storage_l2_row_group_fetch_plan(planning_duration);
	match strategy {
		L2FetchStrategy::RowGroup(plan) => {
			metrics.observe_storage_l2_row_group_fetch_request("row_group");
			let kept = plan.surviving_groups.len() as u64;
			let total = plan.directory.row_groups.len() as u64;
			if kept == 0 {
				metrics.observe_storage_l2_row_group_fetch_row_groups("no_survivors", total);
			} else {
				metrics.observe_storage_l2_row_group_fetch_row_groups("keep", kept);
				metrics.observe_storage_l2_row_group_fetch_row_groups(
					"predicate_fetch",
					plan.predicate_fetched_groups.len() as u64,
				);
				metrics.observe_storage_l2_row_group_fetch_row_groups(
					"skip",
					total.saturating_sub(kept),
				);
			}
			metrics.observe_storage_l2_row_group_fetch_bytes(
				"phase_b",
				plan.phase_b_fetch
					.as_ref()
					.map(|phase_b| phase_b.fetched_ciphertext_bytes)
					.unwrap_or(0),
			);
			metrics.observe_storage_l2_row_group_fetch_bytes(
				"phase_c",
				plan.phase_c_fetch.fetched_ciphertext_bytes,
			);
			metrics.observe_storage_l2_row_group_fetch_selected_ranges(
				plan.phase_c_fetch.chunk_runs.len().saturating_add(
					plan.phase_b_fetch
						.as_ref()
						.map(|phase_b| phase_b.chunk_runs.len())
						.unwrap_or(0),
				) as u64,
			);
			let selected_plaintext_bytes = plan
				.phase_c_fetch
				.selected_plaintext_ranges
				.iter()
				.map(|range| range.len.get())
				.sum::<u64>();
			let total_row_group_bytes = plan
				.directory
				.row_groups
				.iter()
				.map(|group| {
					group
						.metadata_range
						.len
						.get()
						.saturating_add(group.body_range.len.get())
				})
				.sum::<u64>();
			metrics.observe_storage_l2_row_group_fetch_bytes(
				"avoided",
				total_row_group_bytes.saturating_sub(selected_plaintext_bytes),
			);
		}
		L2FetchStrategy::Fallback(reason) => {
			metrics.observe_storage_l2_row_group_fetch_request("whole_record");
			metrics.observe_storage_l2_row_group_fetch_fallback(reason.as_str());
		}
		L2FetchStrategy::Columnar(_) => {
			metrics.observe_storage_l2_row_group_fetch_request("columnar_buffer");
		}
		L2FetchStrategy::WholeRecord => {
			metrics.observe_storage_l2_row_group_fetch_request("whole_record");
		}
	}
}

pub(super) fn encoding_directories_from_slab_blob(
	blob: &[u8],
	slab_id: SlabId,
	acknowledged: &BTreeSet<u64>,
) -> std::io::Result<Vec<EncodingDirectory>> {
	let mut out = Vec::new();
	let mut seen_records = BTreeSet::new();
	for entry in recover_slab_entries_from_blob(blob, slab_id, acknowledged)? {
		if !seen_records.insert((entry.location.offset, entry.location.length)) {
			continue;
		}
		let frame = extract_record_from_blob(blob, entry.location)?;
		if !ipc_stream_contains_encoding_directory(&frame) {
			continue;
		}
		crate::storage::ipc::validate_ipc_stream_envelope(&frame)?;
		let reader = StreamReader::try_new(Cursor::new(frame), None).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow stream open for chunk-size selection: {err}"),
			)
		})?;
		let schema = reader.schema();
		let Some(value) = schema.metadata().get(ENCODING_DIRECTORY_METADATA_KEY) else {
			continue;
		};
		let directory =
			crate::storage::encoding::directory::deserialize_encoding_directory_value(value)
				.map_err(|code| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"Encoding directory rejected during chunk-size selection: {code:?}"
						),
					)
				})?;
		out.push(directory);
	}
	Ok(out)
}

impl SlabManager {
	pub(super) async fn try_read_encoding_indexed_pages_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		whole_record_range: std::ops::Range<usize>,
		body_location: &RecordLocation,
		header_size: u64,
		record_directory: &RecordBufferDirectory,
		page_directory: &crate::storage::row_group::RecordPageDirectory,
		descriptor: &TableSchemaDescriptor,
		requirement: &ColumnFetchRequirement,
		selected_record_rows: &[RecordRowIndex],
		selected_groups: &[RowGroupIndex],
		planning_started: Instant,
	) -> std::io::Result<Option<ProjectedL2RecordRead>> {
		let Some(encoding_directory) = record_directory.encoding_directory.as_ref() else {
			return Ok(None);
		};
		if selected_record_rows.is_empty() || !self.config.l2_encoding_fetch.enabled {
			return Ok(None);
		}
		let plan = match build_encoding_indexed_page_plan(
			record_directory,
			page_directory,
			encoding_directory,
			requirement,
			selected_record_rows,
			selected_groups,
		) {
			Ok(Some(plan)) => plan,
			Ok(None) => return Ok(None),
			Err(reason) => {
				metrics().observe_storage_l2_encoding_fallback(reason.as_str());
				return Ok(None);
			}
		};
		if plan.fetches.len()
			> self
				.config
				.l2_encoding_fetch
				.max_simd_pages_per_record
				.get() as usize
		{
			metrics().observe_storage_l2_encoding_fallback(
				EncodingFallbackReason::TooManyPages.as_str(),
			);
			return Ok(None);
		}

		let mut selected_body_byte_ranges = plan
			.fetches
			.iter()
			.map(|fetch| fetch.body_range)
			.collect::<Vec<_>>();
		coalesce_manager_record_body_ranges(&mut selected_body_byte_ranges)?;
		let selected_body_ranges = selected_body_byte_ranges
			.iter()
			.map(|range| TieredPlaintextRange::new(range.start, range.len))
			.collect::<Vec<_>>();
		let selected_record_ranges =
			record_body_ranges_to_record_ranges(header_size, &selected_body_ranges)?;
		let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			object_size_bytes,
			tail_window,
			additional_metadata,
			whole_record_range,
			&selected_record_ranges,
		)?;
		let phase_c_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
			&tiered_plan,
			&selected_body_byte_ranges,
		);
		let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
			record_id: page_directory.record_id,
			directory: page_directory.clone(),
			surviving_groups: plan.selected_groups.clone(),
			predicate_fetched_groups: Vec::new(),
			phase_b_fetch: None,
			phase_c_fetch,
			logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
		};
		let strategy = pick_point_row_group_fetch_strategy(
			&self.config.l2_row_group_fetch,
			body_location,
			page_directory,
			row_group_plan,
		);
		let L2FetchStrategy::RowGroup(row_group_plan) = strategy else {
			metrics().observe_storage_l2_encoding_fallback(
				EncodingFallbackReason::DenseSelection.as_str(),
			);
			return Ok(None);
		};
		observe_l2_row_group_fetch_strategy(
			metrics(),
			&L2FetchStrategy::RowGroup(row_group_plan.clone()),
			planning_started.elapsed(),
		);

		let plaintext_extracts = if selected_record_ranges.is_empty() {
			Vec::new()
		} else {
			let ciphertext_runs = fetch_tiered_ciphertext_runs(
				l2,
				pointer,
				&tiered_plan,
				self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
			)
			.await?;
			tiered_plan.decrypt(&ciphertext_runs)?
		};
		let projected = materialize_encoding_indexed_pages(
			record_directory,
			requirement,
			&plan,
			&selected_body_ranges,
			&plaintext_extracts,
			selected_record_rows.len(),
			descriptor,
		)
		.map_err(arrow_io_error)?;
		Ok(Some(ProjectedL2RecordRead {
			projected,
			projected_first_record_row_index: RecordRowIndex::ZERO,
			indexed_rows_materialized: true,
			encoding_materialize_audit: encoding_materialize_audit_for_indexed_plan(&plan),
			strategy: L2FetchStrategy::RowGroup(row_group_plan),
		}))
	}

	pub(super) async fn read_encoded_aggregate_record_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptor,
		request: &AggregateRequest,
		predicate: &crate::schema::pbc::Program,
	) -> std::io::Result<Option<EncodedAggregateL2RecordRead>> {
		let observe_fallback = |reason: EncodingComputeFallbackReason| {
			observe_encoding_compute_aggregate_request(request, "dense_fallback", reason.as_str());
		};
		if !pointer.is_encrypted {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		}
		let body_location = record_body_location(location)?;
		let whole_record_range = record_plaintext_range_for_l2(location)?;
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};

		let mut probe = initial_metadata_probe_plan(&self.config.l2_column_fetch, &body_location)
			.map_err(columnar_io_error)?;
		let header_size = slab_record_header_size_u64()?;
		let directory = loop {
			let prefix_len = header_size
				.checked_add(probe.window_bytes.get())
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "metadata prefix overflow"))?;
			let prefix_range = TieredPlaintextRange::new(
				RecordBodyByteOffset::new(0),
				RecordBodyByteLen::try_new(prefix_len).map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("metadata prefix length rejected: {err}"),
					)
				})?,
			);
			let prefix_plan = self.config.crypto.plan_tiered_multi_range_read(
				pointer.slab_id,
				metadata.size_bytes,
				tail_window.as_ref(),
				additional_metadata.as_deref(),
				whole_record_range.clone(),
				&[prefix_range],
			)?;
			let prefix_ciphertext = fetch_tiered_ciphertext_runs(
				l2.clone(),
				pointer,
				&prefix_plan,
				self.config.l2_column_fetch.max_parallel_range_gets.get() as usize,
			)
			.await?;
			let prefix_plaintext = prefix_plan.decrypt(&prefix_ciphertext)?;
			let Some(prefix_plaintext) = prefix_plaintext.first() else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"metadata prefix decrypt returned no plaintext",
				));
			};
			let header_size_usize = usize::try_from(header_size).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"slab record header size exceeds usize",
				)
			})?;
			if prefix_plaintext.len() < header_size_usize {
				return Err(Error::new(
					ErrorKind::UnexpectedEof,
					"metadata prefix shorter than slab record header",
				));
			}
			validate_tiered_record_header_tenant(
				&prefix_plaintext[..header_size_usize],
				prefix_plan.footer().tenant_id,
				pointer.slab_id,
			)?;
			let metadata_window_bytes = &prefix_plaintext[header_size_usize..];
			match parse_record_buffer_directory(
				TieredRecordId {
					slab_id: pointer.slab_id,
					record_offset: RecordBodyByteOffset::new(location.offset),
				},
				PlaintextByteRange {
					start: 0,
					len: u64::from(body_location.length),
				},
				metadata_window_bytes,
				descriptor,
			) {
				Ok(directory) => break directory,
				Err(crate::storage::columnar::ColumnFetchError::MetadataTruncated {
					needed,
					..
				}) => {
					let next = next_metadata_probe_plan(
						&self.config.l2_column_fetch,
						&body_location,
						probe.window_bytes,
						needed,
					)
					.map_err(columnar_io_error)?;
					match next {
						Some(next) => probe = next,
						None => {
							observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
							return Ok(None);
						}
					}
				}
				Err(crate::storage::columnar::ColumnFetchError::UnsupportedArrowLayout) => {
					observe_fallback(EncodingComputeFallbackReason::UnsupportedArrowLayout);
					return Ok(None);
				}
				Err(err) => return Err(columnar_io_error(err)),
			}
		};

		let Some(page_directory) = directory.page_directory.as_ref() else {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		};
		let Some(encoding_directory) = directory.encoding_directory.as_ref() else {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		};
		let Some(group) = crate::storage::row_group::row_group_for_record_row(
			page_directory,
			location.record_row_index,
		) else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"record row index {} is not covered by row-group directory",
					location.record_row_index.get()
				),
			));
		};
		let row_count = RowCount::new(u64::from(group.row_count.get()));
		let mut selected_mask =
			encoding_compute_single_row_mask(row_count, group, location.record_row_index)
				.map_err(encoding_compute_invariant_error)?;
		let mut predicate_fetched_groups = Vec::new();
		let mut phase_b_fetch = None;
		let predicate_tree = predicate.predicate_tree();
		if !matches!(
			predicate_tree,
			crate::schema::pbc::PredicateTree::AlwaysTrue
		) {
			let Some(_) = encoded_predicate_node_from_tree(&predicate_tree) else {
				observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
				return Ok(None);
			};
			let selected_groups = [group.row_group_index];
			predicate_fetched_groups.push(group.row_group_index);
			let output_columns = ColumnOrdinalSet::new();
			let Some(encoded_phase_b) = self
				.try_read_encoding_compute_phase_b_from_l2(
					l2.clone(),
					pointer,
					metadata.size_bytes,
					tail_window.as_ref(),
					additional_metadata.as_deref(),
					whole_record_range.clone(),
					header_size,
					&directory,
					page_directory,
					predicate,
					&output_columns,
					&selected_groups,
					&predicate_fetched_groups,
				)
				.await?
			else {
				return Ok(None);
			};
			let mask = encoded_phase_b
				.outcome
				.survival_masks
				.get(&group.row_group_index)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"Encoding compute aggregate mask missing for row group {}",
							group.row_group_index.get()
						),
					)
				})?;
			let predicate_mask = EncodedRowMask::from_arrow_boolean_array(mask);
			selected_mask = selected_mask
				.and(&predicate_mask)
				.map_err(encoding_compute_invariant_error)?;
			phase_b_fetch = Some(encoded_phase_b.phase_b_fetch);
		}

		let aggregate_plan = match build_encoding_compute_aggregate_plan(
			&directory,
			page_directory,
			encoding_directory,
			request,
			group,
			&self.config.encoding_compute,
			header_size,
		) {
			Ok(plan) => plan,
			Err(reason) => {
				observe_fallback(reason);
				return Ok(None);
			}
		};
		let aggregate_tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			whole_record_range,
			&aggregate_plan.record_ranges,
		)?;
		let phase_c_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
			&aggregate_tiered_plan,
			&aggregate_plan.record_body_ranges,
		);
		let plaintext = if aggregate_plan.record_ranges.is_empty() {
			Vec::new()
		} else {
			let ciphertext = fetch_tiered_ciphertext_runs(
				l2,
				pointer,
				&aggregate_tiered_plan,
				self.config
					.l2_row_group_fetch
					.max_parallel_range_gets
					.get()
					.max(1) as usize,
			)
			.await?;
			aggregate_tiered_plan.decrypt(&ciphertext)?
		};
		let mut context = EncodedAggregateContext::new(row_count);
		populate_encoding_compute_non_null_counts(
			&mut context,
			&directory,
			page_directory,
			group,
			request,
		);
		for fetch in &aggregate_plan.fetches {
			if let Some(validity_fetch) = &fetch.validity_fetch {
				let validity = extract_record_body_bytes(
					validity_fetch.body_range,
					&aggregate_plan.body_ranges,
					&plaintext,
				)
				.map_err(arrow_io_error)?;
				let validity = if let Some(encoding) = &validity_fetch.encoding {
					let cascade = validity_fetch.cascade.as_ref().ok_or_else(|| {
						Error::new(
							ErrorKind::InvalidData,
							"Encoding compute aggregate validity fetch missing cascade",
						)
					})?;
					decode_encoded_validity_buffer(
						&validity,
						cascade,
						encoding,
						group.row_count.get() as usize,
					)
					.map_err(encoding_compute_decode_error)?
				} else {
					validity
				};
				let validity_mask = evaluate_validity_bitmap(Some(&validity), row_count, false)
					.map_err(encoding_compute_invariant_error)?;
				context = context
					.with_validity_mask(fetch.column, validity_mask)
					.map_err(encoding_compute_invariant_error)?;
			}
			if let Some(value_body_range) = fetch.value_body_range {
				let encoded = extract_record_body_bytes(
					value_body_range,
					&aggregate_plan.body_ranges,
					&plaintext,
				)
				.map_err(arrow_io_error)?;
				let encoding = fetch.encoding.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"Encoding compute aggregate value fetch missing encoding",
					)
				})?;
				let cascade = fetch.cascade.as_ref().ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"Encoding compute aggregate value fetch missing cascade",
					)
				})?;
				context = context
					.with_native_encoded_column(
						fetch.column,
						fetch.data_type.clone(),
						&encoded,
						encoding,
						cascade,
					)
					.map_err(encoding_compute_invariant_error)?;
			}
		}
		let Some(partials) =
			try_fold_encoded_aggregate_request(request, &context, Some(&selected_mask))
				.map_err(encoding_compute_invariant_error)?
		else {
			observe_fallback(EncodingComputeFallbackReason::AggregateUnsupported);
			return Ok(None);
		};
		let rows_admitted = selected_mask
			.popcount()
			.map_err(encoding_compute_invariant_error)?
			.get();
		observe_encoding_compute_aggregate_request(request, "exact", "none");
		if aggregate_plan.pages_fetched > 0 {
			metrics().observe_storage_l2_encoding_compute_pages_fetched(
				"selected",
				aggregate_plan.pages_fetched,
			);
		}
		let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
			record_id: page_directory.record_id,
			directory: page_directory.clone(),
			surviving_groups: vec![group.row_group_index],
			predicate_fetched_groups,
			phase_b_fetch,
			phase_c_fetch,
			logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
		};
		Ok(Some(EncodedAggregateL2RecordRead {
			partials,
			rows_admitted,
			strategy: L2FetchStrategy::RowGroup(row_group_plan),
		}))
	}

	pub(super) async fn try_read_encoding_compute_phase_b_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		object_size_bytes: u64,
		tail_window: &[u8],
		additional_metadata: Option<&[u8]>,
		whole_record_range: std::ops::Range<usize>,
		header_size: u64,
		record_directory: &RecordBufferDirectory,
		page_directory: &crate::storage::row_group::RecordPageDirectory,
		predicate: &crate::schema::pbc::Program,
		output_columns: &ColumnOrdinalSet,
		selected_groups: &[RowGroupIndex],
		predicate_fetched_groups: &[RowGroupIndex],
	) -> std::io::Result<Option<EncodingComputePhaseBRead>> {
		if predicate_fetched_groups.is_empty() {
			return Ok(None);
		}
		let observe_fallback = |reason: EncodingComputeFallbackReason| {
			metrics().observe_storage_l2_encoding_compute_plan("dense_fallback", reason.as_str());
		};
		if !self.config.encoding_compute.enabled {
			observe_fallback(EncodingComputeFallbackReason::ReaderDisabled);
			return Ok(None);
		}
		let Some(encoding_directory) = record_directory.encoding_directory.as_ref() else {
			observe_fallback(EncodingComputeFallbackReason::DirectoryMissing);
			return Ok(None);
		};
		let Some(encoded_predicate) = encoded_predicate_node_from_tree(&predicate.predicate_tree())
		else {
			observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
			return Ok(None);
		};
		let observe_page_fallback = |reason: EncodingComputeFallbackReason| {
			metrics().observe_storage_l2_encoding_compute_plan("page_fallback", reason.as_str());
		};
		match build_encoding_predicate_page_plan(
			record_directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			output_columns,
			selected_groups,
			predicate_fetched_groups,
			&self.config.encoding_compute,
			header_size,
		) {
			Ok(plan) => {
				let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
					pointer.slab_id,
					object_size_bytes,
					tail_window,
					additional_metadata,
					whole_record_range,
					&plan.record_ranges,
				)?;
				let phase_b_fetch =
					crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
						&tiered_plan,
						&plan.record_body_ranges,
					);
				let plaintext = if plan.record_ranges.is_empty() {
					Vec::new()
				} else {
					let ciphertext = fetch_tiered_ciphertext_runs(
						l2.clone(),
						pointer,
						&tiered_plan,
						self.config
							.l2_row_group_fetch
							.max_parallel_range_gets
							.get()
							.max(1) as usize,
					)
					.await?;
					tiered_plan.decrypt(&ciphertext)?
				};
				let mut outcome = crate::storage::row_group::PhaseBOutcome::default();
				let mut total_mask_bytes = 0_u64;
				let mut rows_scanned = 0_u64;
				let mut rows_selected = 0_u64;
				let predicate_fetched = predicate_fetched_groups
					.iter()
					.copied()
					.collect::<BTreeSet<_>>();
				for group_index in selected_groups {
					if !predicate_fetched.contains(group_index) {
						continue;
					}
					let group = page_directory
						.row_groups
						.iter()
						.find(|group| group.row_group_index == *group_index)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								format!(
									"Encoding predicate page selected row group {} missing",
									group_index.get()
								),
							)
						})?;
					let row_count = RowCount::new(u64::from(group.row_count.get()));
					let mut leaf_slices =
						vec![Vec::<PredicatePageMaskSlice>::new(); plan.leaves.len()];
					for decision in plan
						.decisions
						.iter()
						.filter(|decision| decision.row_group_index == *group_index)
					{
						let leaf = leaf_slices.get_mut(decision.leaf_index).ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"Encoding predicate page decision leaf out of range",
							)
						})?;
						match decision.classification {
							EncodingPredicatePageDecisionKind::Disjoint => {
								leaf.push(PredicatePageMaskSlice::Disjoint {
									first_row: RecordRowIndex::new(decision.first_row),
									row_count: RowCount::new(u64::from(decision.row_count)),
								});
							}
							EncodingPredicatePageDecisionKind::ExactMatch => {
								leaf.push(PredicatePageMaskSlice::ExactMatch {
									first_row: RecordRowIndex::new(decision.first_row),
									row_count: RowCount::new(u64::from(decision.row_count)),
								});
							}
							EncodingPredicatePageDecisionKind::Maybe { fetch_index } => {
								let fetch = plan.fetches.get(fetch_index).ok_or_else(|| {
									Error::new(
										ErrorKind::InvalidData,
										"Encoding predicate page fetch index out of range",
									)
								})?;
								let fetched = match extract_record_body_bytes(
									fetch.body_range,
									&plan.body_ranges,
									&plaintext,
								) {
									Ok(encoded) => encoded,
									Err(_) => {
										observe_page_fallback(
											EncodingComputeFallbackReason::UnsupportedArrowLayout,
										);
										return Ok(None);
									}
								};
								let Some(coercion) =
									coerce_literal(fetch.atom.literal.as_ref(), &fetch.data_type)
								else {
									observe_page_fallback(
										EncodingComputeFallbackReason::UnsupportedProgram,
									);
									return Ok(None);
								};
								let evaluator = evaluator_kind_for_cascade(&fetch.cascade)
									.ok_or_else(|| {
										Error::new(
											ErrorKind::InvalidData,
											"Encoding predicate page fetch carried unsupported cascade",
										)
									})?;
								let started = Instant::now();
								let mask = match evaluate_encoded_predicate(
									&fetched,
									&fetch.encoding,
									&fetch.cascade,
									&fetch.atom,
									&coercion,
								) {
									Ok(mask) => mask,
									Err(_) => {
										observe_page_fallback(
											EncodingComputeFallbackReason::UnsupportedCascade,
										);
										return Ok(None);
									}
								};
								metrics().observe_storage_l2_encoding_compute_kernel(
									evaluator.as_str(),
									fetch.cascade.kind().as_str(),
									started.elapsed(),
								);
								if mask.rows() != RowCount::new(u64::from(decision.row_count)) {
									return Err(encoding_compute_invariant_error(
										EncodingComputeRejectCode::MaskLengthMismatch,
									));
								}
								leaf.push(PredicatePageMaskSlice::Maybe {
									first_row: RecordRowIndex::new(decision.first_row),
									mask,
								});
							}
						}
					}
					let mut leaf_masks = Vec::with_capacity(plan.leaves.len());
					for slices in leaf_slices {
						let mask = stitch_predicate_page_mask(&PredicatePageFetchPlan {
							row_count,
							slices,
						})
						.map_err(encoding_compute_invariant_error)?;
						if !encoding_compute_mask_selectivity_allowed(
							&mask,
							&self.config.encoding_compute,
						)
						.map_err(encoding_compute_invariant_error)?
						{
							observe_page_fallback(EncodingComputeFallbackReason::DenseSelection);
							return Ok(None);
						}
						leaf_masks.push(mask);
					}
					let mask = encoding_compute_eval_node(&plan.node, &leaf_masks)
						.map_err(encoding_compute_invariant_error)?;
					let selected = mask
						.popcount()
						.map_err(encoding_compute_invariant_error)?
						.get();
					total_mask_bytes = total_mask_bytes.saturating_add(mask.byte_len() as u64);
					if total_mask_bytes
						> self.config.encoding_compute.max_mask_bytes_per_record.get()
					{
						observe_page_fallback(EncodingComputeFallbackReason::TooManyMaskBytes);
						return Ok(None);
					}
					rows_scanned = rows_scanned.saturating_add(row_count.get());
					rows_selected = rows_selected.saturating_add(selected);
					let boolean_mask = mask
						.to_arrow_boolean_array()
						.map_err(encoding_compute_invariant_error)?;
					outcome.survival_masks.insert(*group_index, boolean_mask);
				}
				metrics().observe_storage_l2_encoding_compute_plan("page_exact", "none");
				metrics().observe_storage_l2_encoding_compute_rows("scanned", rows_scanned);
				metrics().observe_storage_l2_encoding_compute_rows("selected", rows_selected);
				metrics().observe_storage_l2_encoding_compute_rows(
					"avoided",
					rows_scanned.saturating_sub(rows_selected),
				);
				metrics().observe_storage_l2_encoding_compute_mask_bytes(total_mask_bytes);
				metrics().observe_storage_l2_encoding_compute_pages_fetched(
					"selected",
					plan.fetches.len() as u64,
				);
				return Ok(Some(EncodingComputePhaseBRead {
					outcome,
					phase_b_fetch,
				}));
			}
			Err(reason) => observe_page_fallback(reason),
		}
		let plan = match build_encoding_compute_phase_b_plan(
			record_directory,
			page_directory,
			encoding_directory,
			&encoded_predicate,
			output_columns,
			selected_groups,
			predicate_fetched_groups,
			&self.config.encoding_compute,
			header_size,
		) {
			Ok(plan) => plan,
			Err(reason) => {
				observe_fallback(reason);
				return Ok(None);
			}
		};
		let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			object_size_bytes,
			tail_window,
			additional_metadata,
			whole_record_range,
			&plan.record_ranges,
		)?;
		let phase_b_fetch = crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
			&tiered_plan,
			&plan.record_body_ranges,
		);
		let plaintext = if plan.record_ranges.is_empty() {
			Vec::new()
		} else {
			let ciphertext = fetch_tiered_ciphertext_runs(
				l2,
				pointer,
				&tiered_plan,
				self.config
					.l2_row_group_fetch
					.max_parallel_range_gets
					.get()
					.max(1) as usize,
			)
			.await?;
			tiered_plan.decrypt(&ciphertext)?
		};
		let mut outcome = crate::storage::row_group::PhaseBOutcome::default();
		let mut total_mask_bytes = 0_u64;
		let mut rows_scanned = 0_u64;
		let mut rows_selected = 0_u64;
		let predicate_fetched = predicate_fetched_groups
			.iter()
			.copied()
			.collect::<BTreeSet<_>>();
		for group_index in selected_groups {
			if !predicate_fetched.contains(group_index) {
				continue;
			}
			let group = page_directory
				.row_groups
				.iter()
				.find(|group| group.row_group_index == *group_index)
				.ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"Encoding compute selected row group {} missing",
							group_index.get()
						),
					)
				})?;
			let row_count = RowCount::new(u64::from(group.row_count.get()));
			let mut value_masks = vec![None::<EncodedRowMask>; plan.leaves.len()];
			let mut validity_masks = vec![None::<EncodedRowMask>; plan.leaves.len()];
			let mut fsst_values = vec![None::<EncodingComputeFsstValueBytes>; plan.leaves.len()];
			let mut fsst_offsets = vec![None::<EncodingComputeFsstOffsetBytes>; plan.leaves.len()];
			for fetch in plan
				.fetches
				.iter()
				.filter(|fetch| fetch.row_group_index == *group_index)
			{
				let fetched = match extract_record_body_bytes(
					fetch.body_range,
					&plan.body_ranges,
					&plaintext,
				) {
					Ok(encoded) => encoded,
					Err(_) => {
						observe_fallback(EncodingComputeFallbackReason::UnsupportedArrowLayout);
						return Ok(None);
					}
				};
				match &fetch.kind {
					EncodingComputePhaseBFetchKind::Encoded { encoding, cascade } => {
						let EncodingComputePhaseBLeaf::Encoded {
							atom, data_type, ..
						} = &plan.leaves[fetch.leaf_index]
						else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"Encoding compute value fetch targeted non-value leaf",
							));
						};
						let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type)
						else {
							observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
							return Ok(None);
						};
						let evaluator = evaluator_kind_for_cascade(cascade).ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"Encoding compute fetch carried unsupported cascade",
							)
						})?;
						let started = Instant::now();
						let mask = match evaluate_encoded_predicate(
							&fetched, encoding, cascade, atom, &coercion,
						) {
							Ok(mask) => mask,
							Err(_) => {
								observe_fallback(EncodingComputeFallbackReason::UnsupportedCascade);
								return Ok(None);
							}
						};
						metrics().observe_storage_l2_encoding_compute_kernel(
							evaluator.as_str(),
							cascade.kind().as_str(),
							started.elapsed(),
						);
						value_masks[fetch.leaf_index] = Some(mask);
					}
					EncodingComputePhaseBFetchKind::FsstValues { encoding, cascade } => {
						fsst_values[fetch.leaf_index] = Some(EncodingComputeFsstValueBytes {
							bytes: fetched,
							encoding: encoding.clone(),
							cascade: cascade.clone(),
						});
					}
					EncodingComputePhaseBFetchKind::FsstOffsets { encoding, cascade } => {
						fsst_offsets[fetch.leaf_index] = Some(EncodingComputeFsstOffsetBytes {
							bytes: fetched,
							encoding: encoding.clone(),
							cascade: cascade.clone(),
						});
					}
					EncodingComputePhaseBFetchKind::Validity { encoding, cascade } => {
						let match_nulls = match &plan.leaves[fetch.leaf_index] {
							EncodingComputePhaseBLeaf::Encoded { .. } => false,
							EncodingComputePhaseBLeaf::Validity {
								selected_when_no_nulls,
								..
							} => !*selected_when_no_nulls,
						};
						let validity = if let Some(encoding) = encoding {
							let cascade = cascade.as_ref().ok_or_else(|| {
								Error::new(
									ErrorKind::InvalidData,
									"Encoding compute validity fetch missing cascade",
								)
							})?;
							decode_encoded_validity_buffer(
								&fetched,
								cascade,
								encoding,
								group.row_count.get() as usize,
							)
							.map_err(encoding_compute_decode_error)?
						} else {
							fetched
						};
						let mask =
							evaluate_validity_bitmap(Some(&validity), row_count, match_nulls)
								.map_err(encoding_compute_invariant_error)?;
						validity_masks[fetch.leaf_index] = Some(mask);
					}
				}
			}
			for (leaf_index, value) in fsst_values.iter_mut().enumerate() {
				let Some(value) = value.take() else {
					continue;
				};
				let offsets = fsst_offsets[leaf_index].take().ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"Encoding compute FSST leaf missing offsets fetch",
					)
				})?;
				let EncodingComputePhaseBLeaf::Encoded {
					atom, data_type, ..
				} = &plan.leaves[leaf_index]
				else {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"Encoding compute FSST fetch targeted non-value leaf",
					));
				};
				let Some(coercion) = coerce_literal(atom.literal.as_ref(), data_type) else {
					observe_fallback(EncodingComputeFallbackReason::UnsupportedProgram);
					return Ok(None);
				};
				let evaluator = evaluator_kind_for_cascade(&value.cascade).ok_or_else(|| {
					Error::new(
						ErrorKind::InvalidData,
						"Encoding compute FSST fetch carried unsupported cascade",
					)
				})?;
				let started = Instant::now();
				let mask = if evaluator
					== crate::storage::encoding::compute::EncodingComputeEvaluatorKind::FsstRowSafe
				{
					evaluate_fsst_row_safe_predicate(
						&value.bytes,
						&value.encoding,
						&offsets.bytes,
						offsets.encoding.as_ref(),
						offsets.cascade.as_ref(),
						data_type,
						atom,
						&coercion,
					)
				} else {
					evaluate_fsst_predicate(
						&value.bytes,
						&value.encoding,
						&offsets.bytes,
						offsets.encoding.as_ref(),
						offsets.cascade.as_ref(),
						data_type,
						atom,
						&coercion,
					)
				}
				.map_err(encoding_compute_invariant_error)?;
				metrics().observe_storage_l2_encoding_compute_kernel(
					evaluator.as_str(),
					value.cascade.kind().as_str(),
					started.elapsed(),
				);
				value_masks[leaf_index] = Some(mask);
			}
			let mut leaf_masks = Vec::with_capacity(plan.leaves.len());
			for (leaf_index, leaf) in plan.leaves.iter().enumerate() {
				let mask = match leaf {
					EncodingComputePhaseBLeaf::Encoded { .. } => {
						let Some(value_mask) = value_masks[leaf_index].take() else {
							return Err(Error::new(
								ErrorKind::InvalidData,
								"Encoding compute encoded leaf missing value mask",
							));
						};
						if let Some(validity_mask) = validity_masks[leaf_index].take() {
							value_mask
								.and(&validity_mask)
								.map_err(encoding_compute_invariant_error)?
						} else {
							value_mask
						}
					}
					EncodingComputePhaseBLeaf::Validity {
						selected_when_no_nulls,
						..
					} => validity_masks[leaf_index].take().unwrap_or_else(|| {
						if *selected_when_no_nulls {
							EncodedRowMask::All { rows: row_count }
						} else {
							EncodedRowMask::None { rows: row_count }
						}
					}),
				};
				if !encoding_compute_mask_selectivity_allowed(&mask, &self.config.encoding_compute)
					.map_err(encoding_compute_invariant_error)?
				{
					observe_fallback(EncodingComputeFallbackReason::DenseSelection);
					return Ok(None);
				}
				leaf_masks.push(mask);
			}
			let mask = encoding_compute_eval_node(&plan.node, &leaf_masks)
				.map_err(encoding_compute_invariant_error)?;
			let selected = mask
				.popcount()
				.map_err(encoding_compute_invariant_error)?
				.get();
			total_mask_bytes = total_mask_bytes.saturating_add(mask.byte_len() as u64);
			if total_mask_bytes > self.config.encoding_compute.max_mask_bytes_per_record.get() {
				observe_fallback(EncodingComputeFallbackReason::TooManyMaskBytes);
				return Ok(None);
			}
			rows_scanned = rows_scanned.saturating_add(row_count.get());
			rows_selected = rows_selected.saturating_add(selected);
			let boolean_mask = mask
				.to_arrow_boolean_array()
				.map_err(encoding_compute_invariant_error)?;
			outcome.survival_masks.insert(*group_index, boolean_mask);
		}
		let pages_fetched = plan
			.fetches
			.iter()
			.map(|fetch| match &fetch.kind {
				EncodingComputePhaseBFetchKind::Encoded { encoding, .. } => {
					encoding.page_end_offsets.len().max(1) as u64
				}
				EncodingComputePhaseBFetchKind::FsstValues { encoding, .. } => {
					encoding.page_end_offsets.len().max(1) as u64
				}
				EncodingComputePhaseBFetchKind::FsstOffsets { .. } => 1,
				EncodingComputePhaseBFetchKind::Validity { .. } => 1,
			})
			.sum::<u64>();
		metrics().observe_storage_l2_encoding_compute_plan("exact", "none");
		metrics().observe_storage_l2_encoding_compute_rows("scanned", rows_scanned);
		metrics().observe_storage_l2_encoding_compute_rows("selected", rows_selected);
		metrics().observe_storage_l2_encoding_compute_rows(
			"avoided",
			rows_scanned.saturating_sub(rows_selected),
		);
		metrics().observe_storage_l2_encoding_compute_mask_bytes(total_mask_bytes);
		metrics().observe_storage_l2_encoding_compute_pages_fetched("selected", pages_fetched);
		Ok(Some(EncodingComputePhaseBRead {
			outcome,
			phase_b_fetch,
		}))
	}

	pub(super) async fn read_projected_record_from_l2(
		&self,
		l2: Arc<dyn ObjectStore>,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptor,
		requirement: &ColumnFetchRequirement,
		predicate: Option<&crate::schema::pbc::Program>,
		selected_record_rows: Option<&[RecordRowIndex]>,
	) -> std::io::Result<ProjectedL2RecordRead> {
		if !pointer.is_encrypted {
			let plaintext = self
				.fetch_full_tiered_plaintext(l2.as_ref(), pointer)
				.await?;
			let frame = extract_record_from_blob(&plaintext, location)?;
			let projected = decode_full_projected_l2_record(
				pointer.slab_id,
				location,
				descriptor,
				requirement,
				&frame,
			)?;
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: RecordRowIndex::ZERO,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy: L2FetchStrategy::WholeRecord,
			});
		}

		let planning_started = Instant::now();
		let body_location = record_body_location(location)?;
		let whole_record_range = record_plaintext_range_for_l2(location)?;
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let additional_metadata_range = self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		)?;
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};

		let pre_probe = pick_l2_fetch_strategy(
			&self.config.l2_column_fetch,
			descriptor,
			&body_location,
			requirement,
			None,
			None,
			false,
		);
		if !matches!(
			pre_probe,
			L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap)
		) && !self.config.l2_row_group_fetch.enabled
		{
			observe_l2_fetch_strategy(metrics(), &pre_probe, planning_started.elapsed());
			let projected = self
				.read_full_projected_record_from_l2(
					l2.as_ref(),
					pointer,
					location,
					descriptor,
					requirement,
				)
				.await?;
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: RecordRowIndex::ZERO,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy: pre_probe,
			});
		}

		let mut probe = initial_metadata_probe_plan(&self.config.l2_column_fetch, &body_location)
			.map_err(columnar_io_error)?;
		let header_size = slab_record_header_size_u64()?;
		let directory = loop {
			let prefix_len = header_size
				.checked_add(probe.window_bytes.get())
				.ok_or_else(|| Error::new(ErrorKind::InvalidData, "metadata prefix overflow"))?;
			let prefix_range = TieredPlaintextRange::new(
				RecordBodyByteOffset::new(0),
				RecordBodyByteLen::try_new(prefix_len).map_err(|err| {
					Error::new(
						ErrorKind::InvalidData,
						format!("metadata prefix length rejected: {err}"),
					)
				})?,
			);
			let prefix_plan = self.config.crypto.plan_tiered_multi_range_read(
				pointer.slab_id,
				metadata.size_bytes,
				tail_window.as_ref(),
				additional_metadata.as_deref(),
				whole_record_range.clone(),
				&[prefix_range],
			)?;
			let prefix_ciphertext = fetch_tiered_ciphertext_runs(
				l2.clone(),
				pointer,
				&prefix_plan,
				self.config.l2_column_fetch.max_parallel_range_gets.get() as usize,
			)
			.await?;
			let prefix_plaintext = prefix_plan.decrypt(&prefix_ciphertext)?;
			let Some(prefix_plaintext) = prefix_plaintext.first() else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"metadata prefix decrypt returned no plaintext",
				));
			};
			let header_size = usize::try_from(header_size).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"slab record header size exceeds usize",
				)
			})?;
			if prefix_plaintext.len() < header_size {
				return Err(Error::new(
					ErrorKind::UnexpectedEof,
					"metadata prefix shorter than slab record header",
				));
			}
			validate_tiered_record_header_tenant(
				&prefix_plaintext[..header_size],
				prefix_plan.footer().tenant_id,
				pointer.slab_id,
			)?;
			let metadata_window_bytes = &prefix_plaintext[header_size..];
			match parse_record_buffer_directory(
				TieredRecordId {
					slab_id: pointer.slab_id,
					record_offset: RecordBodyByteOffset::new(location.offset),
				},
				PlaintextByteRange {
					start: 0,
					len: u64::from(body_location.length),
				},
				metadata_window_bytes,
				descriptor,
			) {
				Ok(directory) => break directory,
				Err(crate::storage::columnar::ColumnFetchError::MetadataTruncated {
					needed,
					..
				}) => {
					let next = next_metadata_probe_plan(
						&self.config.l2_column_fetch,
						&body_location,
						probe.window_bytes,
						needed,
					)
					.map_err(columnar_io_error)?;
					match next {
						Some(next) => probe = next,
						None => {
							let strategy =
								L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap);
							observe_l2_fetch_strategy(
								metrics(),
								&strategy,
								planning_started.elapsed(),
							);
							let projected = self
								.read_full_projected_record_from_l2(
									l2.as_ref(),
									pointer,
									location,
									descriptor,
									requirement,
								)
								.await?;
							return Ok(ProjectedL2RecordRead {
								projected,
								projected_first_record_row_index: RecordRowIndex::ZERO,
								indexed_rows_materialized: false,
								encoding_materialize_audit: Vec::new(),
								strategy,
							});
						}
					}
				}
				Err(crate::storage::columnar::ColumnFetchError::UnsupportedArrowLayout) => {
					let strategy =
						L2FetchStrategy::Fallback(ColumnarFallbackReason::UnsupportedArrowLayout);
					observe_l2_fetch_strategy(metrics(), &strategy, planning_started.elapsed());
					let projected = self
						.read_full_projected_record_from_l2(
							l2.as_ref(),
							pointer,
							location,
							descriptor,
							requirement,
						)
						.await?;
					return Ok(ProjectedL2RecordRead {
						projected,
						projected_first_record_row_index: RecordRowIndex::ZERO,
						indexed_rows_materialized: false,
						encoding_materialize_audit: Vec::new(),
						strategy,
					});
				}
				Err(err) => return Err(columnar_io_error(err)),
			}
		};

		if let Some(page_directory) = directory.page_directory.clone() {
			if directory.encoding_directory.is_some() && !self.config.l2_encoding_fetch.enabled {
				metrics().observe_storage_l2_encoding_fallback("reader_disabled");
				let strategy = L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
				observe_l2_row_group_fetch_strategy(
					metrics(),
					&strategy,
					planning_started.elapsed(),
				);
				let projected = self
					.read_full_projected_record_from_l2(
						l2.as_ref(),
						pointer,
						location,
						descriptor,
						requirement,
					)
					.await?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy,
				});
			}
			if !self.config.l2_row_group_fetch.enabled {
				let strategy = L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
				observe_l2_row_group_fetch_strategy(
					metrics(),
					&strategy,
					planning_started.elapsed(),
				);
				let projected = self
					.read_full_projected_record_from_l2(
						l2.as_ref(),
						pointer,
						location,
						descriptor,
						requirement,
					)
					.await?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy,
				});
			}
			let Some(group) = crate::storage::row_group::row_group_for_record_row(
				&page_directory,
				location.record_row_index,
			) else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"record row index {} is not covered by row-group directory",
						location.record_row_index.get()
					),
				));
			};
			let selected_groups = match selected_record_rows {
				Some(rows) if !rows.is_empty() => {
					crate::storage::row_group::row_groups_for_record_rows(&page_directory, rows)?
				}
				_ => vec![group.row_group_index],
			};
			if predicate.is_none() {
				if let Some(rows) = selected_record_rows {
					if let Some(read) = self
						.try_read_encoding_indexed_pages_from_l2(
							l2.clone(),
							pointer,
							metadata.size_bytes,
							tail_window.as_ref(),
							additional_metadata.as_deref(),
							whole_record_range.clone(),
							&body_location,
							header_size,
							&directory,
							&page_directory,
							descriptor,
							requirement,
							rows,
							selected_groups.as_slice(),
							planning_started,
						)
						.await?
					{
						return Ok(read);
					}
				}
			}
			let point_read = selected_record_rows.is_none();
			let mut predicate_fetched_groups = Vec::new();
			let mut predicate_fetch = None;
			let mut predicate_fetched_requirement = ColumnFetchRequirement::default();
			let mut predicate_projection_by_group = HashMap::new();
			let mut predicate_outcome = crate::storage::row_group::PhaseBOutcome::default();
			let mut selected_groups = selected_groups;
			if let Some(predicate) = predicate {
				let predicate_kind = crate::storage::row_group::classify_pbc_for_row_group_pruning(
					predicate,
					&directory.ipc_schema,
				);
				let classifications = crate::storage::row_group::classify_row_groups(
					&page_directory,
					&predicate_kind,
					&self.config.l2_row_group_fetch,
				)
				.into_iter()
				.collect::<HashMap<_, _>>();
				selected_groups.retain(|group_index| {
					classifications
						.get(group_index)
						.copied()
						.unwrap_or(crate::storage::row_group::RowGroupClassification::Keep)
						!= crate::storage::row_group::RowGroupClassification::Skip
				});
				predicate_fetched_groups = selected_groups
					.iter()
					.copied()
					.filter(|group_index| {
						classifications
							.get(group_index)
							.copied()
							.unwrap_or(crate::storage::row_group::RowGroupClassification::Keep)
							== crate::storage::row_group::RowGroupClassification::NeedsPredicateFetch
					})
					.collect();
				if selected_groups.is_empty() {
					let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
						record_id: page_directory.record_id,
						directory: page_directory.clone(),
						surviving_groups: Vec::new(),
						predicate_fetched_groups: Vec::new(),
						phase_b_fetch: None,
						phase_c_fetch: crate::storage::row_group::RowGroupRangeFetchPlan::default(),
						logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
					};
					let strategy = L2FetchStrategy::RowGroup(row_group_plan);
					observe_l2_row_group_fetch_strategy(
						metrics(),
						&strategy,
						planning_started.elapsed(),
					);
					let projected = crate::storage::row_group::materialize_row_groups_from_batches(
						&[],
						&page_directory,
						requirement,
						&[],
					)?;
					return Ok(ProjectedL2RecordRead {
						projected,
						projected_first_record_row_index: group.first_record_row_index,
						indexed_rows_materialized: false,
						encoding_materialize_audit: Vec::new(),
						strategy,
					});
				}
				if !predicate_fetched_groups.is_empty() {
					if let Some(encoded_predicate_read) = self
						.try_read_encoding_compute_phase_b_from_l2(
							l2.clone(),
							pointer,
							metadata.size_bytes,
							tail_window.as_ref(),
							additional_metadata.as_deref(),
							whole_record_range.clone(),
							header_size,
							&directory,
							&page_directory,
							predicate,
							&requirement.output_columns,
							&selected_groups,
							&predicate_fetched_groups,
						)
						.await?
					{
						predicate_fetch = Some(encoded_predicate_read.phase_b_fetch);
						predicate_outcome = encoded_predicate_read.outcome;
						predicate_fetched_requirement = ColumnFetchRequirement {
							predicate_columns: ColumnOrdinalSet::from_columns(
								requirement
									.predicate_columns
									.iter()
									.filter(|column| !requirement.output_columns.contains(*column)),
							),
							..ColumnFetchRequirement::default()
						};
						let predicate_fetched_set = predicate_fetched_groups
							.iter()
							.copied()
							.collect::<BTreeSet<_>>();
						if point_read
							&& predicate_fetched_set.contains(&group.row_group_index)
							&& !phase_b_point_read_survives(
								&predicate_outcome,
								group,
								location.record_row_index,
							)? {
							let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
								record_id: page_directory.record_id,
								directory: page_directory.clone(),
								surviving_groups: Vec::new(),
								predicate_fetched_groups,
								phase_b_fetch: predicate_fetch,
								phase_c_fetch:
									crate::storage::row_group::RowGroupRangeFetchPlan::default(),
								logical_schema_fingerprint: page_directory
									.logical_schema_fingerprint,
							};
							let strategy = L2FetchStrategy::RowGroup(row_group_plan);
							observe_l2_row_group_fetch_strategy(
								metrics(),
								&strategy,
								planning_started.elapsed(),
							);
							let projected =
								crate::storage::row_group::materialize_row_groups_from_batches(
									&[],
									&page_directory,
									requirement,
									&[],
								)?;
							return Ok(ProjectedL2RecordRead {
								projected,
								projected_first_record_row_index: group.first_record_row_index,
								indexed_rows_materialized: false,
								encoding_materialize_audit: Vec::new(),
								strategy,
							});
						}
					}
					if predicate_fetch.is_none() {
						let predicate_fetch_requirement = ColumnFetchRequirement {
							predicate_columns: requirement.predicate_columns.clone(),
							storage_columns: requirement.storage_columns.clone(),
							..ColumnFetchRequirement::default()
						};
						if !predicate_fetch_requirement.user_visible().is_empty()
							|| !predicate_fetch_requirement.storage_columns.is_empty()
						{
							let predicate_body_byte_ranges =
								crate::storage::row_group::selected_row_group_record_body_ranges(
									&directory,
									&page_directory,
									&predicate_fetch_requirement,
									&selected_groups,
								)
								.map_err(columnar_io_error)?;
							let predicate_body_ranges = predicate_body_byte_ranges
								.iter()
								.map(|range| TieredPlaintextRange::new(range.start, range.len))
								.collect::<Vec<_>>();
							let predicate_record_ranges = record_body_ranges_to_record_ranges(
								header_size,
								&predicate_body_ranges,
							)?;
							let predicate_tiered_plan =
								self.config.crypto.plan_tiered_multi_range_read(
									pointer.slab_id,
									metadata.size_bytes,
									tail_window.as_ref(),
									additional_metadata.as_deref(),
									whole_record_range.clone(),
									&predicate_record_ranges,
								)?;
							predicate_fetch = Some(
								crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
									&predicate_tiered_plan,
									&predicate_body_byte_ranges,
								),
							);
							let predicate_ciphertext = fetch_tiered_ciphertext_runs(
								l2.clone(),
								pointer,
								&predicate_tiered_plan,
								self.config.l2_row_group_fetch.max_parallel_range_gets.get()
									as usize,
							)
							.await?;
							let predicate_plaintext =
								predicate_tiered_plan.decrypt(&predicate_ciphertext)?;
							let user_column_count = descriptor.arrow_schema()?.fields().len();
							let predicate_fetched_set = predicate_fetched_groups
								.iter()
								.copied()
								.collect::<BTreeSet<_>>();
							for group_index in &selected_groups {
								let predicate_projection =
							crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
								&directory,
								&page_directory,
								&predicate_fetch_requirement,
								&predicate_body_ranges,
								&predicate_plaintext,
									&[*group_index],
							)
							.map_err(arrow_io_error)?;
								if predicate_fetched_set.contains(group_index) {
									let survival_mask = survival_mask_projected_record_batch(
										&predicate_projection,
										predicate,
										user_column_count,
									)?;
									predicate_outcome
										.decoded_predicate_batches
										.insert(*group_index, predicate_projection.batch.clone());
									predicate_outcome
										.survival_masks
										.insert(*group_index, survival_mask);
								}
								predicate_projection_by_group
									.insert(*group_index, predicate_projection);
							}
							if point_read && predicate_fetched_set.contains(&group.row_group_index)
							{
								let survival_mask = predicate_outcome
									.survival_masks
									.get(&group.row_group_index)
									.ok_or_else(|| {
										Error::new(
											ErrorKind::InvalidData,
											format!(
												"predicate survival mask missing for target row group {}",
												group.row_group_index.get()
											),
										)
									})?;
								let row_offset = location
									.record_row_index
									.get()
									.checked_sub(group.first_record_row_index.get())
									.ok_or_else(|| {
										Error::new(
											ErrorKind::InvalidData,
											format!(
												"record row index {} precedes row group {} base {}",
												location.record_row_index.get(),
												group.row_group_index.get(),
												group.first_record_row_index.get()
											),
										)
									})? as usize;
								if row_offset >= survival_mask.len() {
									return Err(Error::new(
										ErrorKind::InvalidData,
										format!(
											"record row index {} maps outside predicate survival mask length {} for row group {}",
											location.record_row_index.get(),
											survival_mask.len(),
											group.row_group_index.get()
										),
									));
								}
								if !survival_mask.is_valid(row_offset)
									|| !survival_mask.value(row_offset)
								{
									let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
									record_id: page_directory.record_id,
									directory: page_directory.clone(),
									surviving_groups: Vec::new(),
									predicate_fetched_groups,
									phase_b_fetch: predicate_fetch,
									phase_c_fetch:
										crate::storage::row_group::RowGroupRangeFetchPlan::default(),
									logical_schema_fingerprint: page_directory
										.logical_schema_fingerprint,
								};
									let strategy = L2FetchStrategy::RowGroup(row_group_plan);
									observe_l2_row_group_fetch_strategy(
										metrics(),
										&strategy,
										planning_started.elapsed(),
									);
									let projected =
									crate::storage::row_group::materialize_row_groups_from_batches(
										&[],
										&page_directory,
										requirement,
										&[],
									)?;
									return Ok(ProjectedL2RecordRead {
										projected,
										projected_first_record_row_index: group
											.first_record_row_index,
										indexed_rows_materialized: false,
										encoding_materialize_audit: Vec::new(),
										strategy,
									});
								}
							}
							predicate_fetched_requirement = predicate_fetch_requirement;
						}
					}
				}
			}
			let projection_fetch_requirement = if predicate_fetch.is_some() {
				requirement.excluding(&predicate_fetched_requirement)
			} else {
				requirement.clone()
			};
			let projection_fetch_requirement_empty =
				projection_fetch_requirement.user_visible().is_empty()
					&& projection_fetch_requirement.storage_columns.is_empty();
			let predicate_output_overlap = predicate_fetched_requirement
				.predicate_columns
				.iter()
				.any(|column| requirement.output_columns.contains(column));
			let dense_projection_body_byte_ranges = if projection_fetch_requirement_empty {
				Vec::new()
			} else {
				crate::storage::row_group::selected_row_group_record_body_ranges(
					&directory,
					&page_directory,
					&projection_fetch_requirement,
					&selected_groups,
				)
				.map_err(columnar_io_error)?
			};
			let mut sparse_projection_plan = None;
			let mut projection_groups = selected_groups.clone();
			let selected_body_byte_ranges = if !point_read
				&& !projection_fetch_requirement_empty
				&& !predicate_outcome.survival_masks.is_empty()
			{
				if let Some(encoding_directory) = directory.encoding_directory.as_ref() {
					match decide_sparse_projection_fetch(SparseProjectionFetchInput {
						record_directory: &directory,
						page_directory: &page_directory,
						encoding_directory,
						requirement: &projection_fetch_requirement,
						predicate_outcome: &predicate_outcome,
						selected_groups: &selected_groups,
						dense_body_ranges: &dense_projection_body_byte_ranges,
						row_group_config: &self.config.l2_row_group_fetch,
						encoding_config: &self.config.l2_encoding_fetch,
					}) {
						SparseProjectionFetchDecision::Sparse(plan) => {
							projection_groups = plan.selected_groups.clone();
							let ranges = plan.selected_body_ranges.clone();
							sparse_projection_plan = Some(plan);
							ranges
						}
						SparseProjectionFetchDecision::Decline(reason) => {
							let _ = reason;
							dense_projection_body_byte_ranges.clone()
						}
					}
				} else {
					dense_projection_body_byte_ranges.clone()
				}
			} else {
				dense_projection_body_byte_ranges.clone()
			};
			let selected_body_ranges = selected_body_byte_ranges
				.iter()
				.map(|range| TieredPlaintextRange::new(range.start, range.len))
				.collect::<Vec<_>>();
			let selected_record_ranges =
				record_body_ranges_to_record_ranges(header_size, &selected_body_ranges)?;
			let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
				pointer.slab_id,
				metadata.size_bytes,
				tail_window.as_ref(),
				additional_metadata.as_deref(),
				whole_record_range.clone(),
				&selected_record_ranges,
			)?;
			let projection_fetch =
				crate::storage::row_group::row_group_range_fetch_plan_from_tiered(
					&tiered_plan,
					&selected_body_byte_ranges,
				);
			let row_group_plan = crate::storage::row_group::RowGroupFetchPlan {
				record_id: page_directory.record_id,
				directory: page_directory.clone(),
				surviving_groups: projection_groups.to_vec(),
				predicate_fetched_groups,
				phase_b_fetch: predicate_fetch,
				phase_c_fetch: projection_fetch,
				logical_schema_fingerprint: page_directory.logical_schema_fingerprint,
			};
			let strategy = pick_point_row_group_fetch_strategy(
				&self.config.l2_row_group_fetch,
				&body_location,
				&page_directory,
				row_group_plan,
			);
			observe_l2_row_group_fetch_strategy(metrics(), &strategy, planning_started.elapsed());
			let L2FetchStrategy::RowGroup(row_group_plan) = strategy else {
				let projected = self
					.read_full_projected_record_from_l2(
						l2.as_ref(),
						pointer,
						location,
						descriptor,
						requirement,
					)
					.await?;
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: RecordRowIndex::ZERO,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy,
				});
			};
			if let Some(sparse_projection_plan) = sparse_projection_plan.as_ref() {
				for fetch in &sparse_projection_plan.fetches {
					metrics().observe_storage_l2_encoding_simd_pages_fetched(
						fetch.cascade.kind().as_str(),
						1,
					);
				}
				let plaintext_extracts = if selected_record_ranges.is_empty() {
					Vec::new()
				} else {
					let ciphertext_runs = fetch_tiered_ciphertext_runs(
						l2,
						pointer,
						&tiered_plan,
						self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
					)
					.await?;
					tiered_plan.decrypt(&ciphertext_runs)?
				};
				let projected = if predicate_projection_by_group.is_empty() {
					materialize_sparse_projection_pages(
						&directory,
						&projection_fetch_requirement,
						sparse_projection_plan,
						&selected_body_ranges,
						&plaintext_extracts,
						&predicate_outcome,
					)
					.map_err(arrow_io_error)?
				} else {
					let sparse_page_batches = materialize_sparse_projection_page_batches(
						&directory,
						&projection_fetch_requirement,
						sparse_projection_plan,
						&selected_body_ranges,
						&plaintext_extracts,
						&predicate_outcome,
					)
					.map_err(arrow_io_error)?;
					if !predicate_output_overlap {
						concat_sparse_projection_page_batches(
							&directory,
							&projection_fetch_requirement,
							sparse_page_batches,
						)
						.map_err(arrow_io_error)?
					} else {
						let mut zipped_pages = Vec::new();
						for page_batch in sparse_page_batches {
							let Some(predicate_projection) =
								predicate_projection_by_group.get(&page_batch.row_group_index)
							else {
								return Err(Error::new(
									ErrorKind::InvalidData,
									format!(
										"predicate projection batch missing for sparse projection row group {}",
										page_batch.row_group_index.get()
									),
								));
							};
							let predicate_page = filtered_predicate_page_batch(
								predicate_projection,
								page_batch.page,
								&page_batch.local_mask,
							)
							.map_err(arrow_io_error)?;
							if predicate_page.batch.num_rows() != page_batch.batch.batch.num_rows()
							{
								return Err(Error::new(
									ErrorKind::InvalidData,
									format!(
										"predicate page row count {} does not match sparse projection row count {}",
										predicate_page.batch.num_rows(),
										page_batch.batch.batch.num_rows()
									),
								));
							}
							zipped_pages.push(
								crate::storage::row_group::zip_predicate_and_projection_columns(
									&predicate_page,
									&page_batch.batch,
									requirement,
								)?,
							);
						}
						if zipped_pages.is_empty() {
							empty_projected_batch_for_requirement(&directory, requirement, 0)
								.map_err(arrow_io_error)?
						} else {
							concat_projected_record_batches(zipped_pages).map_err(arrow_io_error)?
						}
					}
				};
				return Ok(ProjectedL2RecordRead {
					projected,
					projected_first_record_row_index: group.first_record_row_index,
					indexed_rows_materialized: false,
					encoding_materialize_audit: Vec::new(),
					strategy: L2FetchStrategy::RowGroup(row_group_plan),
				});
			}
			let projected = if predicate_projection_by_group.is_empty() {
				if predicate_outcome.survival_masks.is_empty() || point_read {
					if projection_fetch_requirement_empty {
						crate::storage::row_group::empty_projected_row_group(
							group.row_count.get() as usize
						)?
					} else {
						let ciphertext_runs = fetch_tiered_ciphertext_runs(
							l2,
							pointer,
							&tiered_plan,
							self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
						)
						.await?;
						let plaintext_extracts = tiered_plan.decrypt(&ciphertext_runs)?;
						crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
							&directory,
							&page_directory,
							&projection_fetch_requirement,
							&selected_body_ranges,
							&plaintext_extracts,
							&selected_groups,
						)
						.map_err(arrow_io_error)?
					}
				} else {
					let plaintext_extracts = if projection_fetch_requirement_empty {
						Vec::new()
					} else {
						let ciphertext_runs = fetch_tiered_ciphertext_runs(
							l2,
							pointer,
							&tiered_plan,
							self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
						)
						.await?;
						tiered_plan.decrypt(&ciphertext_runs)?
					};
					let mut projected_groups = Vec::new();
					for group_index in &selected_groups {
						let group_entry = page_directory
							.row_groups
							.iter()
							.find(|candidate| candidate.row_group_index == *group_index)
							.ok_or_else(|| {
								Error::new(
									ErrorKind::InvalidData,
									format!(
										"selected row group {} missing from page directory",
										group_index.get()
									),
								)
							})?;
						let projected = if projection_fetch_requirement_empty {
							crate::storage::row_group::empty_projected_row_group(
								group_entry.row_count.get() as usize,
							)?
						} else {
							crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
								&directory,
								&page_directory,
								&projection_fetch_requirement,
								&selected_body_ranges,
								&plaintext_extracts,
								&[*group_index],
							)
							.map_err(arrow_io_error)?
						};
						projected_groups.push((*group_index, projected));
					}
					crate::storage::row_group::apply_phase_b_masks_to_projected_row_groups(
						projected_groups,
						&predicate_outcome,
						row_group_plan.predicate_fetched_groups.as_slice(),
						&page_directory,
					)?
				}
			} else {
				let plaintext_extracts = if projection_fetch_requirement_empty {
					Vec::new()
				} else {
					let ciphertext_runs = fetch_tiered_ciphertext_runs(
						l2,
						pointer,
						&tiered_plan,
						self.config.l2_row_group_fetch.max_parallel_range_gets.get() as usize,
					)
					.await?;
					tiered_plan.decrypt(&ciphertext_runs)?
				};
				let mut projected_groups = Vec::new();
				for group_index in &selected_groups {
					let group_entry = page_directory
						.row_groups
						.iter()
						.find(|candidate| candidate.row_group_index == *group_index)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								format!(
									"selected row group {} missing from page directory",
									group_index.get()
								),
							)
						})?;
					let projection_projected = if projection_fetch_requirement_empty {
						crate::storage::row_group::empty_projected_row_group(
							group_entry.row_count.get() as usize,
						)?
					} else {
						crate::storage::row_group::decode_projected_row_groups_from_plaintext_ranges(
							&directory,
							&page_directory,
							&projection_fetch_requirement,
							&selected_body_ranges,
							&plaintext_extracts,
							&[*group_index],
						)
						.map_err(arrow_io_error)?
					};
					let Some(predicate_projection) = predicate_projection_by_group.get(group_index)
					else {
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"predicate projection batch missing for selected row group {}",
								group_index.get()
							),
						));
					};
					let zipped = crate::storage::row_group::zip_predicate_and_projection_columns(
						predicate_projection,
						&projection_projected,
						requirement,
					)?;
					projected_groups.push((*group_index, zipped));
				}
				let mask_groups = if point_read {
					&[][..]
				} else {
					row_group_plan.predicate_fetched_groups.as_slice()
				};
				crate::storage::row_group::apply_phase_b_masks_to_projected_row_groups(
					projected_groups,
					&predicate_outcome,
					mask_groups,
					&page_directory,
				)?
			};
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: group.first_record_row_index,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy: L2FetchStrategy::RowGroup(row_group_plan),
			});
		}

		let selected_body_ranges =
			selected_record_body_ranges(&directory, requirement).map_err(columnar_io_error)?;
		let selected_record_ranges =
			record_body_ranges_to_record_ranges(header_size, &selected_body_ranges)?;
		let tiered_plan = self.config.crypto.plan_tiered_multi_range_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
			whole_record_range,
			&selected_record_ranges,
		)?;
		let candidate_plan = match build_column_fetch_plan(&directory, requirement, &tiered_plan) {
			Ok(plan) => Some(plan),
			Err(crate::storage::columnar::ColumnFetchError::UnsupportedArrowLayout) => None,
			Err(err) => return Err(columnar_io_error(err)),
		};
		let strategy = pick_l2_fetch_strategy(
			&self.config.l2_column_fetch,
			descriptor,
			&body_location,
			requirement,
			Some(&directory),
			candidate_plan,
			false,
		);
		observe_l2_fetch_strategy(metrics(), &strategy, planning_started.elapsed());
		if !matches!(strategy, L2FetchStrategy::Columnar(_)) {
			let projected = self
				.read_full_projected_record_from_l2(
					l2.as_ref(),
					pointer,
					location,
					descriptor,
					requirement,
				)
				.await?;
			return Ok(ProjectedL2RecordRead {
				projected,
				projected_first_record_row_index: RecordRowIndex::ZERO,
				indexed_rows_materialized: false,
				encoding_materialize_audit: Vec::new(),
				strategy,
			});
		}

		let ciphertext_runs = fetch_tiered_ciphertext_runs(
			l2,
			pointer,
			&tiered_plan,
			self.config.l2_column_fetch.max_parallel_range_gets.get() as usize,
		)
		.await?;
		let plaintext_extracts = tiered_plan.decrypt(&ciphertext_runs)?;
		let projected = decode_projected_record_batch_from_plaintext_ranges(
			&directory,
			requirement,
			&selected_body_ranges,
			&plaintext_extracts,
		)
		.map_err(arrow_io_error)?;
		Ok(ProjectedL2RecordRead {
			projected,
			projected_first_record_row_index: RecordRowIndex::ZERO,
			indexed_rows_materialized: false,
			encoding_materialize_audit: Vec::new(),
			strategy,
		})
	}

	pub(super) async fn read_full_projected_record_from_l2(
		&self,
		l2: &dyn ObjectStore,
		pointer: &TieredSlabPointer,
		location: RecordLocation,
		descriptor: &TableSchemaDescriptor,
		requirement: &ColumnFetchRequirement,
	) -> std::io::Result<ProjectedRecordBatch> {
		let frame = if pointer.is_encrypted {
			self.read_encrypted_record_from_l2(l2, pointer, location)
				.await?
		} else {
			let plaintext = self.fetch_full_tiered_plaintext(l2, pointer).await?;
			extract_record_from_blob(&plaintext, location)?
		};
		decode_full_projected_l2_record(pointer.slab_id, location, descriptor, requirement, &frame)
	}
}
