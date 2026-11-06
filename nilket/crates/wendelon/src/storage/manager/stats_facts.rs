use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct NdvColumnCandidate {
	pub(super) ordinal: u16,
	pub(super) small_set_max_values: Option<u32>,
	pub(super) small_set_max_bytes: Option<u32>,
	pub(super) hll_enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RangeColumnCandidate {
	pub(super) ordinal: u16,
	pub(super) arrow_type_tag: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct HeavyHitterColumnCandidate {
	pub(super) ordinal: u16,
	pub(super) arrow_type_tag: u8,
	pub(super) max_entries: u16,
	pub(super) max_bytes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ColumnGroupTopKCandidate {
	pub(super) column_ordinals: Vec<u16>,
	pub(super) arrow_type_tags: Vec<u8>,
	pub(super) max_entries: u16,
	pub(super) max_bytes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ColumnGroupNdvCandidate {
	pub(super) column_ordinals: Vec<u16>,
	pub(super) arrow_type_tags: Vec<u8>,
	pub(super) precision: HllPrecision,
	pub(super) exact_max_distinct_values: u32,
	pub(super) exact_max_bytes: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RangeDistributionColumnCandidate {
	pub(super) ordinal: u16,
	pub(super) arrow_type_tag: u8,
}

#[derive(Clone, Debug)]
pub(super) enum TieredStatsContributor {
	Matching(ParsedColumnStatsBlock),
	OtherTable,
	Missing,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct TieredStatsCacheKey {
	pub(super) slab_id: SlabId,
	pub(super) table_object_id: TableObjectId,
	pub(super) schema_generation: u64,
	pub(super) logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub(super) stats_format_version: u16,
	pub(super) stats_block_len: u32,
	pub(super) stats_block_hash128: [u8; 16],
}

pub(super) fn ndv_column_candidates(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<BTreeMap<u16, NdvColumnCandidate>> {
	let schema = descriptor.arrow_schema()?;
	let mut candidates = BTreeMap::new();
	for spec in &descriptor.stats_policy.small_sets {
		if let Some(ordinal) = schema
			.fields()
			.iter()
			.position(|field| field.name() == &spec.column_name)
		{
			let entry = candidates
				.entry(ordinal as u16)
				.or_insert(NdvColumnCandidate {
					ordinal: ordinal as u16,
					small_set_max_values: None,
					small_set_max_bytes: None,
					hll_enabled: false,
				});
			entry.small_set_max_values = Some(spec.max_distinct_values.get());
			entry.small_set_max_bytes = Some(spec.max_bytes.get());
		}
	}
	for spec in &descriptor.stats_policy.hll_distincts {
		if let Some(ordinal) = schema
			.fields()
			.iter()
			.position(|field| field.name() == &spec.column_name)
		{
			let entry = candidates
				.entry(ordinal as u16)
				.or_insert(NdvColumnCandidate {
					ordinal: ordinal as u16,
					small_set_max_values: None,
					small_set_max_bytes: None,
					hll_enabled: false,
				});
			entry.hll_enabled = true;
		}
	}
	Ok(candidates)
}

pub(super) fn range_column_candidates(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<BTreeMap<u16, RangeColumnCandidate>> {
	let schema = descriptor.arrow_schema()?;
	let mut candidates = BTreeMap::new();
	for (ordinal, field) in schema.fields().iter().enumerate() {
		let Some(arrow_type_tag) = arrow_stats_type_tag(field.data_type()) else {
			continue;
		};
		if range_fact_tag_supported(arrow_type_tag) {
			candidates.insert(
				ordinal as u16,
				RangeColumnCandidate {
					ordinal: ordinal as u16,
					arrow_type_tag,
				},
			);
		}
	}
	Ok(candidates)
}

pub(super) fn heavy_hitter_column_candidates(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<BTreeMap<u16, HeavyHitterColumnCandidate>> {
	let schema = descriptor.arrow_schema()?;
	let mut candidates = BTreeMap::new();
	for spec in &descriptor.stats_policy.heavy_hitters {
		if let Some((ordinal, field)) = schema
			.fields()
			.iter()
			.enumerate()
			.find(|(_, field)| field.name() == &spec.column_name)
		{
			let Some(arrow_type_tag) = arrow_stats_type_tag(field.data_type()) else {
				continue;
			};
			candidates.insert(
				ordinal as u16,
				HeavyHitterColumnCandidate {
					ordinal: ordinal as u16,
					arrow_type_tag,
					max_entries: spec.max_entries,
					max_bytes: spec.max_bytes,
				},
			);
		}
	}
	Ok(candidates)
}

pub(super) fn column_group_top_k_candidates(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<BTreeMap<u16, ColumnGroupTopKCandidate>> {
	let schema = descriptor.arrow_schema()?;
	let mut candidates = BTreeMap::new();
	for spec in &descriptor.stats_policy.column_group_top_k {
		let mut column_ordinals = Vec::with_capacity(spec.column_names.len());
		let mut arrow_type_tags = Vec::with_capacity(spec.column_names.len());
		for column_name in &spec.column_names {
			let Some((ordinal, field)) = schema
				.fields()
				.iter()
				.enumerate()
				.find(|(_, field)| field.name() == column_name)
			else {
				continue;
			};
			let Some(arrow_type_tag) = arrow_stats_type_tag(field.data_type()) else {
				continue;
			};
			column_ordinals.push(ordinal as u16);
			arrow_type_tags.push(arrow_type_tag);
		}
		if column_ordinals.len() != spec.column_names.len() || column_ordinals.len() < 2 {
			continue;
		}
		let Some(anchor_ordinal) = column_ordinals.iter().min().copied() else {
			continue;
		};
		candidates.insert(
			anchor_ordinal,
			ColumnGroupTopKCandidate {
				column_ordinals,
				arrow_type_tags,
				max_entries: spec.max_entries,
				max_bytes: spec.max_bytes,
			},
		);
	}
	Ok(candidates)
}

pub(super) fn column_group_ndv_candidates(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<BTreeMap<u16, ColumnGroupNdvCandidate>> {
	let schema = descriptor.arrow_schema()?;
	let mut candidates = BTreeMap::new();
	for spec in &descriptor.stats_policy.column_group_ndv {
		let mut column_ordinals = Vec::with_capacity(spec.column_names.len());
		let mut arrow_type_tags = Vec::with_capacity(spec.column_names.len());
		for column_name in &spec.column_names {
			let Some((ordinal, field)) = schema
				.fields()
				.iter()
				.enumerate()
				.find(|(_, field)| field.name() == column_name)
			else {
				continue;
			};
			let Some(arrow_type_tag) = arrow_stats_type_tag(field.data_type()) else {
				continue;
			};
			column_ordinals.push(ordinal as u16);
			arrow_type_tags.push(arrow_type_tag);
		}
		if column_ordinals.len() != spec.column_names.len() || column_ordinals.len() < 2 {
			continue;
		}
		let Some(anchor_ordinal) = column_ordinals.iter().min().copied() else {
			continue;
		};
		candidates.insert(
			anchor_ordinal,
			ColumnGroupNdvCandidate {
				column_ordinals,
				arrow_type_tags,
				precision: spec.precision,
				exact_max_distinct_values: spec.exact_max_distinct_values.get(),
				exact_max_bytes: spec.exact_max_bytes.get(),
			},
		);
	}
	Ok(candidates)
}

pub(super) fn range_distribution_column_candidates(
	descriptor: &TableSchemaDescriptor,
) -> std::io::Result<BTreeMap<u16, RangeDistributionColumnCandidate>> {
	let schema = descriptor.arrow_schema()?;
	let mut candidates = BTreeMap::new();
	for spec in &descriptor.stats_policy.range_distributions {
		if let Some((ordinal, field)) = schema
			.fields()
			.iter()
			.enumerate()
			.find(|(_, field)| field.name() == &spec.column_name)
		{
			let Some(arrow_type_tag) = arrow_stats_type_tag(field.data_type()) else {
				continue;
			};
			if range_distribution_value_width(arrow_type_tag).is_none() {
				continue;
			}
			candidates.insert(
				ordinal as u16,
				RangeDistributionColumnCandidate {
					ordinal: ordinal as u16,
					arrow_type_tag,
				},
			);
		}
	}
	Ok(candidates)
}

pub(super) fn range_distribution_value_width(arrow_type_tag: u8) -> Option<usize> {
	match arrow_type_tag {
		ARROW_STATS_TAG_INT32 | ARROW_STATS_TAG_FLOAT32 => Some(4),
		ARROW_STATS_TAG_INT64 | ARROW_STATS_TAG_FLOAT64 => Some(8),
		_ => None,
	}
}

pub(super) fn ndv_fact_for_column(
	candidate: &NdvColumnCandidate,
	blocks: &[ParsedColumnStatsBlock],
) -> std::io::Result<Option<NdvFact>> {
	let mut exact_values = BTreeSet::<Vec<u8>>::new();
	let mut exact_complete =
		candidate.small_set_max_values.is_some() && candidate.small_set_max_bytes.is_some();
	let mut merged_hll: Option<HllRegisters> = None;
	let mut hll_complete = candidate.hll_enabled;

	for block in blocks {
		let Some(entry) = block
			.columns
			.iter()
			.find(|entry| entry.column_ordinal == candidate.ordinal)
		else {
			return Ok(None);
		};
		let payload = decode_column_stats_payload(entry)?;
		if exact_complete {
			if let Some(values) = payload.small_set.as_ref() {
				for value in values {
					exact_values.insert(value.clone());
				}
				let value_count = u32::try_from(exact_values.len()).unwrap_or(u32::MAX);
				let byte_count = exact_values.iter().try_fold(0_u32, |sum, value| {
					let len = u32::try_from(value.len()).map_err(|_| ())?;
					sum.checked_add(len).ok_or(())
				});
				let value_over_cap = match candidate.small_set_max_values {
					Some(cap) => value_count > cap,
					None => true,
				};
				let bytes_over_cap = match (byte_count, candidate.small_set_max_bytes) {
					(Ok(bytes), Some(cap)) => bytes > cap,
					_ => true,
				};
				if value_over_cap || bytes_over_cap {
					exact_complete = false;
				}
			} else {
				exact_complete = false;
			}
		}
		if hll_complete {
			if let Some(hll) = payload.hll_distinct.as_ref() {
				let registers = HllPrecision::new(hll.precision).and_then(|precision| {
					HllRegisters::from_registers(precision, hll.registers.clone())
				});
				match (merged_hll.as_mut(), registers) {
					(Some(merged), Ok(registers))
						if merged.precision() == registers.precision() =>
					{
						merged.merge(&registers)?;
					}
					(None, Ok(registers)) => {
						merged_hll = Some(registers);
					}
					_ => {
						hll_complete = false;
					}
				}
			} else {
				hll_complete = false;
			}
		}
	}

	if exact_complete {
		let count = u64::try_from(exact_values.len()).unwrap_or(u64::MAX);
		return Ok(Some(NdvFact::Exact { count }));
	}
	if hll_complete {
		if let Some(merged_hll) = merged_hll {
			let estimate = merged_hll.estimate()?.round();
			let estimate = if estimate.is_sign_negative() {
				0
			} else if estimate > u64::MAX as f64 {
				u64::MAX
			} else {
				estimate as u64
			};
			let register_count = merged_hll.registers().len() as f64;
			let rel_err_ppm = ((1.04 / register_count.sqrt()) * 1_000_000.0).ceil();
			let rel_err_ppm = if rel_err_ppm.is_sign_negative() {
				0
			} else if rel_err_ppm > u32::MAX as f64 {
				u32::MAX
			} else {
				rel_err_ppm as u32
			};
			return Ok(Some(NdvFact::Approx {
				estimate,
				rel_err_ppm,
			}));
		}
	}
	Ok(None)
}

pub(super) fn range_fact_for_column(
	candidate: &RangeColumnCandidate,
	blocks: &[ParsedColumnStatsBlock],
) -> std::io::Result<Option<ColumnRangeFact>> {
	let mut null_count = 0_u64;
	let mut non_null_count = 0_u64;
	let mut min: Option<Vec<u8>> = None;
	let mut max: Option<Vec<u8>> = None;

	for block in blocks {
		let Some(entry) = block
			.columns
			.iter()
			.find(|entry| entry.column_ordinal == candidate.ordinal)
		else {
			return Ok(None);
		};
		if entry.arrow_type_tag != candidate.arrow_type_tag {
			return Ok(None);
		}
		let payload = decode_column_stats_payload(entry)?;
		let Some(block_null_count) = payload.null_count else {
			return Ok(None);
		};
		let Some(block_non_null_count) = payload.non_null_count else {
			return Ok(None);
		};
		null_count = null_count
			.checked_add(block_null_count)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "range fact null_count overflow"))?;
		non_null_count = non_null_count
			.checked_add(block_non_null_count)
			.ok_or_else(|| {
				Error::new(ErrorKind::InvalidData, "range fact non_null_count overflow")
			})?;
		if block_non_null_count == 0 {
			continue;
		}
		if entry.stat_flags & (COLUMN_STAT_MIN_TRUNCATED | COLUMN_STAT_MAX_TRUNCATED) != 0 {
			return Ok(None);
		}
		let (Some(block_min), Some(block_max)) = (payload.min, payload.max) else {
			return Ok(None);
		};
		if min
			.as_deref()
			.map(|current| block_min.as_slice() < current)
			.unwrap_or(true)
		{
			min = Some(block_min);
		}
		if max
			.as_deref()
			.map(|current| block_max.as_slice() > current)
			.unwrap_or(true)
		{
			max = Some(block_max);
		}
	}

	if non_null_count == 0 {
		min = None;
		max = None;
	} else if min.is_none() || max.is_none() {
		return Ok(None);
	}

	Ok(Some(ColumnRangeFact {
		column_ordinal: candidate.ordinal,
		arrow_type_tag: candidate.arrow_type_tag,
		null_count,
		non_null_count,
		min,
		max,
	}))
}

pub(super) fn range_distribution_fact_for_column(
	_candidate: &RangeDistributionColumnCandidate,
	_blocks: &[ParsedColumnStatsBlock],
) -> std::io::Result<Option<ColumnRangeDistributionFact>> {
	// ADR 0083: a v1 flat payload discarded the weights needed to interpret
	// its retained values. Absence is supported; manufacturing equal weights is
	// not. CSB v2 will restore this fact class behind its fleet fence.
	Ok(None)
}

pub(super) fn heavy_hitter_fact_for_column(
	candidate: &HeavyHitterColumnCandidate,
	blocks: &[ParsedColumnStatsBlock],
) -> std::io::Result<Option<ColumnHeavyHitterFact>> {
	let mut contributors = Vec::with_capacity(blocks.len());
	let mut candidate_keys = BTreeSet::<Vec<u8>>::new();
	for block in blocks {
		let Some(entry) = block
			.columns
			.iter()
			.find(|entry| entry.column_ordinal == candidate.ordinal)
		else {
			return Ok(None);
		};
		if entry.arrow_type_tag != candidate.arrow_type_tag {
			return Ok(None);
		}
		let payload = decode_column_stats_payload(entry)?;
		let Some(heavy_hitters) = payload.heavy_hitters else {
			return Ok(None);
		};
		for entry in &heavy_hitters.entries {
			candidate_keys.insert(entry.value_key.clone());
		}
		contributors.push(heavy_hitters);
	}

	let mut exact_entries = Vec::new();
	for key in candidate_keys {
		let mut frequency = 0_u64;
		let mut known = true;
		for contributor in &contributors {
			match contributor
				.entries
				.binary_search_by(|entry| entry.value_key.as_slice().cmp(key.as_slice()))
			{
				Ok(index) => {
					frequency = frequency
						.checked_add(contributor.entries[index].frequency)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"heavy-hitter fact frequency overflow",
							)
						})?;
				}
				Err(_) if contributor.complete => {}
				Err(_) => {
					known = false;
					break;
				}
			}
		}
		if known && frequency != 0 {
			exact_entries.push(ColumnHeavyHitterEntry {
				value_key: key,
				frequency,
			});
		}
	}

	exact_entries.sort_by(|left, right| {
		right
			.frequency
			.cmp(&left.frequency)
			.then_with(|| left.value_key.cmp(&right.value_key))
	});
	let max_entries = usize::from(candidate.max_entries);
	let max_bytes = candidate.max_bytes as usize;
	let mut selected = Vec::new();
	for entry in exact_entries {
		if selected.len() >= max_entries {
			break;
		}
		selected.push(entry);
		if heavy_hitter_fact_entries_encoded_len(&selected)? > max_bytes {
			selected.pop();
			break;
		}
	}
	if selected.is_empty() {
		return Ok(None);
	}
	selected.sort_by(|left, right| left.value_key.cmp(&right.value_key));
	Ok(Some(ColumnHeavyHitterFact {
		column_ordinal: candidate.ordinal,
		arrow_type_tag: candidate.arrow_type_tag,
		entries: selected,
	}))
}

pub(super) fn heavy_hitter_fact_entries_encoded_len(
	entries: &[ColumnHeavyHitterEntry],
) -> std::io::Result<usize> {
	entries.iter().try_fold(1_usize + 4, |bytes, entry| {
		bytes
			.checked_add(4)
			.and_then(|bytes| bytes.checked_add(entry.value_key.len()))
			.and_then(|bytes| bytes.checked_add(8))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "heavy-hitter fact byte overflow"))
	})
}

pub(super) fn column_group_top_k_fact_for_group(
	candidate: &ColumnGroupTopKCandidate,
	blocks: &[ParsedColumnStatsBlock],
) -> std::io::Result<Option<ColumnGroupTopKFact>> {
	let Some(anchor_ordinal) = candidate.column_ordinals.iter().min().copied() else {
		return Ok(None);
	};
	let anchor_tag = candidate
		.column_ordinals
		.iter()
		.position(|ordinal| *ordinal == anchor_ordinal)
		.and_then(|index| candidate.arrow_type_tags.get(index))
		.copied()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "group top-k anchor tag missing"))?;
	let mut contributors = Vec::with_capacity(blocks.len());
	let mut candidate_keys = BTreeSet::<Vec<Vec<u8>>>::new();
	for block in blocks {
		let Some(entry) = block
			.columns
			.iter()
			.find(|entry| entry.column_ordinal == anchor_ordinal)
		else {
			return Ok(None);
		};
		if entry.arrow_type_tag != anchor_tag {
			return Ok(None);
		}
		let payload = decode_column_stats_payload(entry)?;
		let Some(group_top_k) = payload.group_top_k else {
			return Ok(None);
		};
		if group_top_k.column_ordinals != candidate.column_ordinals
			|| group_top_k.arrow_type_tags != candidate.arrow_type_tags
		{
			return Ok(None);
		}
		for entry in &group_top_k.entries {
			candidate_keys.insert(entry.value_keys.clone());
		}
		contributors.push(group_top_k);
	}

	let mut exact_entries = Vec::new();
	for key in candidate_keys {
		let mut frequency = 0_u64;
		let mut known = true;
		for contributor in &contributors {
			match contributor
				.entries
				.binary_search_by(|entry| entry.value_keys.cmp(&key))
			{
				Ok(index) => {
					frequency = frequency
						.checked_add(contributor.entries[index].frequency)
						.ok_or_else(|| {
							Error::new(
								ErrorKind::InvalidData,
								"group top-k fact frequency overflow",
							)
						})?;
				}
				Err(_) if contributor.complete => {}
				Err(_) => {
					known = false;
					break;
				}
			}
		}
		if known && frequency != 0 {
			exact_entries.push(ColumnGroupTopKEntry {
				value_keys: key,
				frequency,
			});
		}
	}

	exact_entries.sort_by(|left, right| {
		right
			.frequency
			.cmp(&left.frequency)
			.then_with(|| left.value_keys.cmp(&right.value_keys))
	});
	let max_entries = usize::from(candidate.max_entries);
	let max_bytes = candidate.max_bytes as usize;
	let mut selected = Vec::new();
	for entry in exact_entries {
		if selected.len() >= max_entries {
			break;
		}
		selected.push(entry);
		let payload = ColumnStatsGroupTopKPayload {
			column_ordinals: candidate.column_ordinals.clone(),
			arrow_type_tags: candidate.arrow_type_tags.clone(),
			complete: false,
			entries: selected
				.iter()
				.map(|entry| ColumnStatsGroupTopKEntry {
					value_keys: entry.value_keys.clone(),
					frequency: entry.frequency,
				})
				.collect(),
		};
		if group_top_k_payload_encoded_len(&payload)? > max_bytes {
			selected.pop();
			break;
		}
	}
	if selected.is_empty() {
		return Ok(None);
	}
	selected.sort_by(|left, right| left.value_keys.cmp(&right.value_keys));
	Ok(Some(ColumnGroupTopKFact {
		column_ordinals: candidate.column_ordinals.clone(),
		arrow_type_tags: candidate.arrow_type_tags.clone(),
		entries: selected,
	}))
}

pub(super) fn column_group_ndv_fact_for_group(
	candidate: &ColumnGroupNdvCandidate,
	blocks: &[ParsedColumnStatsBlock],
) -> std::io::Result<Option<ColumnGroupNdvFact>> {
	let Some(anchor_ordinal) = candidate.column_ordinals.iter().min().copied() else {
		return Ok(None);
	};
	let anchor_tag = candidate
		.column_ordinals
		.iter()
		.position(|ordinal| *ordinal == anchor_ordinal)
		.and_then(|index| candidate.arrow_type_tags.get(index))
		.copied()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "group NDV anchor tag missing"))?;
	let mut exact_values = BTreeSet::<Vec<u8>>::new();
	let mut exact_complete = true;
	let mut merged_hll: Option<HllRegisters> = None;
	for block in blocks {
		let Some(entry) = block
			.columns
			.iter()
			.find(|entry| entry.column_ordinal == anchor_ordinal)
		else {
			return Ok(None);
		};
		if entry.arrow_type_tag != anchor_tag {
			return Ok(None);
		}
		let payload = decode_column_stats_payload(entry)?;
		let Some(group_ndv) = payload.group_ndv else {
			return Ok(None);
		};
		if group_ndv.column_ordinals != candidate.column_ordinals
			|| group_ndv.arrow_type_tags != candidate.arrow_type_tags
			|| group_ndv.hll_distinct.precision != candidate.precision.get()
		{
			return Ok(None);
		}
		if exact_complete {
			if let Some(values) = group_ndv.exact_values.as_ref() {
				for value in values {
					exact_values.insert(value.clone());
				}
				if group_ndv_exact_set_exceeds_caps(
					&exact_values,
					candidate.exact_max_distinct_values,
					candidate.exact_max_bytes,
				)? {
					exact_complete = false;
					exact_values.clear();
				}
			} else {
				exact_complete = false;
				exact_values.clear();
			}
		}
		let registers = HllPrecision::new(group_ndv.hll_distinct.precision).and_then(|precision| {
			HllRegisters::from_registers(precision, group_ndv.hll_distinct.registers.clone())
		});
		match (merged_hll.as_mut(), registers) {
			(Some(merged), Ok(registers)) if merged.precision() == registers.precision() => {
				merged.merge(&registers)?;
			}
			(None, Ok(registers)) => {
				merged_hll = Some(registers);
			}
			_ => return Ok(None),
		}
	}

	let distinct = if exact_complete {
		let count = u64::try_from(exact_values.len()).unwrap_or(u64::MAX);
		if count == 0 {
			return Ok(None);
		}
		NdvFact::Exact { count }
	} else {
		let Some(merged_hll) = merged_hll else {
			return Ok(None);
		};
		let estimate = merged_hll.estimate()?.round();
		let estimate = if estimate.is_sign_negative() {
			0
		} else if estimate > u64::MAX as f64 {
			u64::MAX
		} else {
			estimate as u64
		};
		if estimate == 0 {
			return Ok(None);
		}
		let register_count = merged_hll.registers().len() as f64;
		let rel_err_ppm = ((1.04 / register_count.sqrt()) * 1_000_000.0).ceil();
		let rel_err_ppm = if rel_err_ppm.is_sign_negative() {
			0
		} else if rel_err_ppm > u32::MAX as f64 {
			u32::MAX
		} else {
			rel_err_ppm as u32
		};
		NdvFact::Approx {
			estimate,
			rel_err_ppm,
		}
	};
	Ok(Some(ColumnGroupNdvFact {
		column_ordinals: candidate.column_ordinals.clone(),
		distinct,
	}))
}

pub(super) fn group_ndv_exact_set_exceeds_caps(
	values: &BTreeSet<Vec<u8>>,
	max_distinct_values: u32,
	max_bytes: u32,
) -> std::io::Result<bool> {
	let value_count = u32::try_from(values.len()).unwrap_or(u32::MAX);
	if value_count > max_distinct_values {
		return Ok(true);
	}
	let byte_count = values.iter().try_fold(0_u32, |sum, value| {
		let len = u32::try_from(value.len()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"group NDV exact value length exceeds u32::MAX",
			)
		})?;
		sum.checked_add(len).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"group NDV exact byte count overflow",
			)
		})
	})?;
	Ok(byte_count > max_bytes)
}

pub(super) fn row_upper_bound_for_blocks(blocks: &[ParsedColumnStatsBlock]) -> u64 {
	blocks.iter().fold(0_u64, |rows, block| {
		rows.saturating_add(block.header.row_count)
	})
}

pub(super) fn tiered_stats_cache_key(
	pointer: &TieredSlabPointer,
	descriptor: &TableSchemaDescriptor,
) -> Option<TieredStatsCacheKey> {
	Some(TieredStatsCacheKey {
		slab_id: pointer.slab_id,
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		stats_format_version: pointer.stats_format_version?,
		stats_block_len: pointer.stats_block_len?,
		stats_block_hash128: pointer.stats_block_hash128?,
	})
}

impl SlabManager {
	pub(super) async fn load_tiered_optimizer_stats_contributor(
		&self,
		pointer: &TieredSlabPointer,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<TieredStatsContributor> {
		metrics().pbc_slab_stats_consulted_total.inc();
		let Some(cache_key) = tiered_stats_cache_key(pointer, descriptor) else {
			metrics().pbc_slab_stats_missing_total.inc();
			return Ok(TieredStatsContributor::Missing);
		};
		if let Some(entry) = self.tiered_stats_cache.get(&cache_key) {
			metrics().pbc_slab_stats_cache_hits_total.inc();
			return Ok(TieredStatsContributor::Matching(entry.value().clone()));
		}
		let Some(l2) = self.config.l2.as_ref() else {
			metrics().pbc_slab_stats_missing_total.inc();
			return Ok(TieredStatsContributor::Missing);
		};
		metrics().pbc_slab_stats_cache_misses_total.inc();
		let metadata = l2.head(&pointer.l2_uri).await?;
		let tail_start = metadata
			.size_bytes
			.saturating_sub(self.config.rehydration.tail_read_window_bytes);
		let tail_window = l2
			.get_range(&pointer.l2_uri, tail_start..metadata.size_bytes)
			.await?;
		let additional_metadata_range = match self.config.crypto.additional_tiered_metadata_range(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
		) {
			Ok(range) => range,
			Err(error) => {
				metrics().pbc_slab_stats_decrypt_failed_total.inc();
				return Err(error);
			}
		};
		let additional_metadata = match additional_metadata_range {
			Some(range) => Some(l2.get_range(&pointer.l2_uri, range).await?),
			None => None,
		};
		let Some(plan) = self.config.crypto.plan_tiered_stats_block_read(
			pointer.slab_id,
			metadata.size_bytes,
			tail_window.as_ref(),
			additional_metadata.as_deref(),
		)?
		else {
			metrics().pbc_slab_stats_missing_total.inc();
			return Ok(TieredStatsContributor::Missing);
		};
		let locator = plan.locator();
		if pointer.stats_format_version != Some(locator.stats_format_version)
			|| pointer.stats_block_len != Some(locator.stats_block_len)
			|| pointer.stats_block_hash128 != Some(locator.stats_block_hash128)
		{
			metrics().pbc_slab_stats_pointer_drift_total.inc();
			return Ok(TieredStatsContributor::Missing);
		}
		let stats_ciphertext = l2
			.get_range(&pointer.l2_uri, plan.stats_range_in_object())
			.await?;
		let stats = plan.decrypt(stats_ciphertext.as_ref(), descriptor.schema_generation)?;
		let parsed = parse_column_stats_block(&stats.plaintext)?;
		let parsed_identity = ColumnStatsDescriptorIdentity::from_header(&parsed.header);
		let descriptor_identity = ColumnStatsDescriptorIdentity::from_descriptor(descriptor);
		if parsed_identity == descriptor_identity {
			self.tiered_stats_cache.insert(cache_key, parsed.clone());
			return Ok(TieredStatsContributor::Matching(parsed));
		}
		if parsed.header.table_object_id != descriptor.table_object_id {
			return Ok(TieredStatsContributor::OtherTable);
		}
		if parsed.header.schema_generation == descriptor.schema_generation
			&& parsed.header.logical_schema_fingerprint != descriptor.logical_schema_fingerprint
		{
			if let Some(tenant_id) = pointer.tenant_id {
				metrics().observe_storage_csb_logical_fingerprint_mismatch(
					tenant_id,
					descriptor.table_object_id,
				);
				metrics()
					.observe_storage_csb_pre_adr_051_cliff(tenant_id, descriptor.table_object_id);
			}
		}
		metrics().pbc_slab_stats_pointer_drift_total.inc();
		Ok(TieredStatsContributor::Missing)
	}

	pub async fn table_optimizer_facts(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
	) -> std::io::Result<Option<TableOptimizerFacts>> {
		self.require_tier_authority_ready()?;
		let ndv_candidates = ndv_column_candidates(descriptor)?;
		let range_candidates = range_column_candidates(descriptor)?;
		let heavy_hitter_candidates = heavy_hitter_column_candidates(descriptor)?;
		let range_distribution_candidates = range_distribution_column_candidates(descriptor)?;
		let column_group_top_k_candidates = column_group_top_k_candidates(descriptor)?;
		let column_group_ndv_candidates = column_group_ndv_candidates(descriptor)?;
		if self
			.has_uncovered_local_rows_for_descriptor(tenant_id, descriptor)
			.await?
		{
			return Ok(None);
		}

		let producer_epoch = self.compaction_manifest_snapshot().manifest_epoch.get();
		let mut blocks = Vec::new();
		for pointer in self.list_tiered_slabs().await {
			if let Some(pointer_tenant_id) = pointer.tenant_id
				&& pointer_tenant_id != tenant_id
			{
				continue;
			}
			match self
				.load_tiered_optimizer_stats_contributor(&pointer, descriptor)
				.await
				.unwrap_or(TieredStatsContributor::Missing)
			{
				TieredStatsContributor::Matching(block) => blocks.push(block),
				TieredStatsContributor::OtherTable => continue,
				TieredStatsContributor::Missing => return Ok(None),
			}
		}
		let row_upper_bound = row_upper_bound_for_blocks(&blocks);

		let mut column_ndv = Vec::new();
		for candidate in ndv_candidates.values() {
			if let Some(distinct) = ndv_fact_for_column(candidate, &blocks)? {
				column_ndv.push(ColumnNdvFact {
					column_ordinal: candidate.ordinal,
					distinct,
				});
			}
		}
		let mut column_range = Vec::new();
		for candidate in range_candidates.values() {
			if let Some(range) = range_fact_for_column(candidate, &blocks)? {
				column_range.push(range);
			}
		}
		let mut column_heavy_hitters = Vec::new();
		for candidate in heavy_hitter_candidates.values() {
			if let Some(fact) = heavy_hitter_fact_for_column(candidate, &blocks)? {
				column_heavy_hitters.push(fact);
			}
		}
		let mut column_group_top_k = Vec::new();
		for candidate in column_group_top_k_candidates.values() {
			if let Some(fact) = column_group_top_k_fact_for_group(candidate, &blocks)? {
				column_group_top_k.push(fact);
			}
		}
		let mut column_group_ndv = Vec::new();
		for candidate in column_group_ndv_candidates.values() {
			if let Some(fact) = column_group_ndv_fact_for_group(candidate, &blocks)? {
				column_group_ndv.push(fact);
			}
		}
		let mut column_range_distributions = Vec::new();
		for candidate in range_distribution_candidates.values() {
			if let Some(fact) = range_distribution_fact_for_column(candidate, &blocks)? {
				column_range_distributions.push(fact);
			}
		}
		Ok(Some(TableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch,
			row_upper_bound: Some(row_upper_bound),
			column_ndv,
			column_range,
			column_range_distributions,
			column_heavy_hitters,
			column_group_top_k,
			column_group_ndv,
		}))
	}

	pub(super) fn build_tiered_column_stats_block(
		&self,
		blob: &[u8],
		slab_id: SlabId,
		tenant_id: TenantId,
		table_schemas: &TableSchemaCatalog,
		acknowledged: &BTreeSet<u64>,
	) -> std::io::Result<Option<TieredColumnStatsBlock>> {
		self.require_tier_authority_ready()?;
		let descriptors = table_schemas
			.descriptors_for_tenant(tenant_id)
			.into_iter()
			.filter(|descriptor| descriptor.table_object_id != TableObjectId::DEFAULT_KV)
			.collect::<Vec<_>>();
		if descriptors.is_empty() {
			return Ok(None);
		}
		let entries = recover_slab_entries_from_blob(blob, slab_id, acknowledged)?;
		let mut candidates: Option<Vec<TableSchemaDescriptor>> = None;
		let mut saw_unmatched_row = false;
		let mut row_batches = Vec::with_capacity(entries.len());
		// A multirow frame holds many logical entries, in a row. Each decodes to the same batch,
		// and the row slices share its buffers, so the frame is decoded once however many rows
		// it holds. A frame that does not decode stays that way for all of them.
		let mut decoded_frame = None;

		for entry in entries {
			let record_tenant_id = entry.tenant_id;
			let key_identity = entry.key_identity;
			let routing_locality = self.recovered_entry_routing_locality(&entry);
			let location = entry.location;
			if record_tenant_id != tenant_id {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"ADR 039 typed-table stats build saw tenant {} in slab {} after metadata chose tenant {}",
						record_tenant_id.get(),
						slab_id,
						tenant_id.get(),
					),
				));
			}
			if !matches!(&decoded_frame, Some((offset, _)) if *offset == location.offset) {
				let frame = extract_record_from_blob(blob, location)?;
				decoded_frame = Some((location.offset, decode_record_batch(&frame).ok()));
			}
			let decoded = decoded_frame
				.as_ref()
				.and_then(|(_, decoded)| decoded.as_ref());
			let (batch, stored_table_object_id) = match decoded {
				Some(decoded) => {
					// Phase 8.5: surface malformed
					// `__row_table_object_id__` as storage corruption
					// rather than silently treating it as PK. `Ok(None)`
					// means the column is absent (PK row); only fail
					// when the column is present but unreadable.
					let table_object_id = crate::storage::multirow::storage_row_table_object_id_at(
						decoded,
						location.record_row_index,
					)?;
					match materialize_record_location_batch(decoded, location.record_row_index) {
						Ok(batch) if batch.num_rows() == 1 => (batch, table_object_id),
						_ => {
							saw_unmatched_row = true;
							continue;
						}
					}
				}
				None => {
					saw_unmatched_row = true;
					continue;
				}
			};
			let had_candidates = candidates.is_some();
			let matching = match candidates.take() {
				Some(existing) => existing,
				None => descriptors.clone(),
			}
			.into_iter()
			.filter_map(|descriptor| {
				match descriptor_matches_tiered_row(
					&descriptor,
					tenant_id,
					key_identity,
					routing_locality,
					stored_table_object_id,
					&batch,
				) {
					Ok(true) => Some(descriptor),
					Ok(false) | Err(_) => None,
				}
			})
			.collect::<Vec<_>>();
			if matching.is_empty() {
				if had_candidates {
					return Err(Error::new(
						ErrorKind::InvalidData,
						format!(
							"ADR 039 typed-table slab {slab_id} contains a row outside the matched table descriptor; stats are mandatory for typed-table v2 slabs"
						),
					));
				}
				saw_unmatched_row = true;
				continue;
			}
			candidates = Some(matching);
			row_batches.push(batch);
		}

		let Some(mut candidates) = candidates else {
			return Ok(None);
		};
		if saw_unmatched_row {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 039 typed-table slab {slab_id} also contains rows that cannot be represented by the matched table descriptor; stats are mandatory for typed-table v2 slabs"
				),
			));
		}
		if candidates.len() != 1 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 039 typed-table slab {slab_id} matches {} table descriptors; cannot choose a mandatory CSB descriptor",
					candidates.len()
				),
			));
		}
		if row_batches.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"ADR 039 typed-table slab {slab_id} matched a descriptor but no row batches"
				),
			));
		}
		let descriptor = candidates.remove(0);
		let schema = row_batches[0].schema();
		let batch = concat_batches(&schema, &row_batches).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("ADR 039 CSB row concat failed: {err}"),
			)
		})?;
		let max_stats_ciphertext_bytes = max_column_stats_ciphertext_bytes_for_slab_payload(
			usize::try_from(self.config.max_slab_bytes).unwrap_or(usize::MAX),
		);
		let plaintext = build_column_stats_block_with_ciphertext_budget(
			&descriptor,
			&batch,
			crate::storage::stats::COLUMN_STATS_DEFAULT_PREFIX_BYTES,
			max_stats_ciphertext_bytes,
		)?;
		let parsed = parse_column_stats_block(&plaintext)?;
		if parsed.header.column_block_count == 0 {
			return Ok(None);
		}
		Ok(Some(TieredColumnStatsBlock::plaintext(
			plaintext,
			descriptor.schema_generation,
			parsed.header.column_block_count,
			locator_flags_for_parsed_column_stats_block(&parsed),
		)))
	}

	pub(super) fn clear_tiered_stats_cache_for_slab(&self, slab_id: SlabId) {
		self.tiered_stats_cache
			.retain(|key, _stats| key.slab_id != slab_id);
	}
}
