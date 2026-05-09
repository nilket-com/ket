use crate::schema::pbc::ColumnPredicateLiteralV1;
use crate::schema::{RecordRowIndex, RowCount};
use crate::storage::encoding::EncodingPageStats;
use crate::storage::encoding::compute::EncodedRowMask;
use crate::storage::encoding::compute::{PredicateAtom, PredicateAtomOp};
use crate::storage::stats::{
	arrow_stats_type_tag, encode_bool_stat_value, encode_signed_stat_value,
	encode_unsigned_stat_value,
};
use arrow::datatypes::{DataType, TimeUnit};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PagePredicateClassification {
	Disjoint,
	ExactMatch,
	Maybe,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PredicatePageFetchPlan {
	pub(crate) row_count: RowCount,
	pub(crate) slices: Vec<PredicatePageMaskSlice>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PredicatePageMaskSlice {
	Disjoint {
		first_row: RecordRowIndex,
		row_count: RowCount,
	},
	ExactMatch {
		first_row: RecordRowIndex,
		row_count: RowCount,
	},
	Maybe {
		first_row: RecordRowIndex,
		mask: EncodedRowMask,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum PredicatePageFetchDecline {
	MissingPageStats,
	UnsupportedPredicate,
	UnsupportedCascade,
	PageGeometryMismatchAcrossPredicateColumns,
	NotCheaperThanDense,
	FragmentationCapExceeded,
	MaxRangesExceeded,
	MaxSimdPagesExceeded,
}

pub(crate) fn stitch_predicate_page_mask(
	plan: &PredicatePageFetchPlan,
) -> Result<EncodedRowMask, crate::storage::encoding::compute::EncodingComputeRejectCode> {
	let rows = usize::try_from(plan.row_count.get()).map_err(|_| {
		crate::storage::encoding::compute::EncodingComputeRejectCode::SelectionRangeOverflow
	})?;
	let mut values = vec![false; rows];
	for slice in &plan.slices {
		match slice {
			PredicatePageMaskSlice::Disjoint {
				first_row,
				row_count,
			} => {
				let (start, end) = slice_bounds(*first_row, *row_count, rows)?;
				values[start..end].fill(false);
			}
			PredicatePageMaskSlice::ExactMatch {
				first_row,
				row_count,
			} => {
				let (start, end) = slice_bounds(*first_row, *row_count, rows)?;
				values[start..end].fill(true);
			}
			PredicatePageMaskSlice::Maybe { first_row, mask } => {
				let page_values = mask.to_bool_vec()?;
				let row_count = RowCount::new(page_values.len() as u64);
				let (start, end) = slice_bounds(*first_row, row_count, rows)?;
				values[start..end].copy_from_slice(&page_values);
			}
		}
	}
	Ok(EncodedRowMask::from_bool_slice(&values))
}

pub fn classify_page_stats_for_atom(
	stat: &EncodingPageStats,
	page_rows: u32,
	data_type: &DataType,
	atom: &PredicateAtom,
) -> Option<PagePredicateClassification> {
	match atom.op {
		PredicateAtomOp::IsNull => Some(classify_null_page(stat, page_rows, true)),
		PredicateAtomOp::IsNotNull => Some(classify_null_page(stat, page_rows, false)),
		PredicateAtomOp::Eq
		| PredicateAtomOp::Neq
		| PredicateAtomOp::Gt
		| PredicateAtomOp::Gte
		| PredicateAtomOp::Lt
		| PredicateAtomOp::Lte => {
			let literal = atom.literal.as_ref()?;
			let literal = literal_to_canonical_bytes(data_type, literal)?;
			classify_value_page(stat, page_rows, data_type, atom.op, literal.as_slice())
		}
		PredicateAtomOp::InList => None,
	}
}

fn classify_null_page(
	stat: &EncodingPageStats,
	page_rows: u32,
	match_nulls: bool,
) -> PagePredicateClassification {
	let nulls = stat.null_count.get();
	if match_nulls {
		if nulls == 0 {
			PagePredicateClassification::Disjoint
		} else if nulls == page_rows {
			PagePredicateClassification::ExactMatch
		} else {
			PagePredicateClassification::Maybe
		}
	} else if nulls == 0 {
		PagePredicateClassification::ExactMatch
	} else if nulls == page_rows {
		PagePredicateClassification::Disjoint
	} else {
		PagePredicateClassification::Maybe
	}
}

fn classify_value_page(
	stat: &EncodingPageStats,
	page_rows: u32,
	data_type: &DataType,
	op: PredicateAtomOp,
	literal: &[u8],
) -> Option<PagePredicateClassification> {
	if stat.null_count.get() >= page_rows {
		return Some(PagePredicateClassification::Disjoint);
	}
	let tag = arrow_stats_type_tag(data_type)?;
	let min = stat.min.as_ref()?;
	let max = stat.max.as_ref()?;
	if min.arrow_type_tag != tag || max.arrow_type_tag != tag || min.truncated || max.truncated {
		return None;
	}
	let min = min.bytes.as_slice();
	let max = max.bytes.as_slice();
	let has_nulls = stat.null_count.get() != 0;
	let disjoint = match op {
		PredicateAtomOp::Eq => literal < min || literal > max,
		PredicateAtomOp::Neq => min == literal && max == literal,
		PredicateAtomOp::Gt => max <= literal,
		PredicateAtomOp::Gte => max < literal,
		PredicateAtomOp::Lt => min >= literal,
		PredicateAtomOp::Lte => min > literal,
		_ => return None,
	};
	if disjoint {
		return Some(PagePredicateClassification::Disjoint);
	}
	let exact = !has_nulls
		&& match op {
			PredicateAtomOp::Eq => min == literal && max == literal,
			PredicateAtomOp::Neq => literal < min || literal > max,
			PredicateAtomOp::Gt => min > literal,
			PredicateAtomOp::Gte => min >= literal,
			PredicateAtomOp::Lt => max < literal,
			PredicateAtomOp::Lte => max <= literal,
			_ => false,
		};
	if exact {
		Some(PagePredicateClassification::ExactMatch)
	} else {
		Some(PagePredicateClassification::Maybe)
	}
}

fn literal_to_canonical_bytes(
	data_type: &DataType,
	literal: &ColumnPredicateLiteralV1,
) -> Option<Vec<u8>> {
	match (data_type, literal) {
		(DataType::Int8, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 1).ok()
		}
		(DataType::Int16, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 2).ok()
		}
		(DataType::Int32, ColumnPredicateLiteralV1::Int(value))
		| (DataType::Date32, ColumnPredicateLiteralV1::Int(value)) => {
			encode_signed_stat_value(*value, 4).ok()
		}
		(DataType::Int64, ColumnPredicateLiteralV1::Int(value))
		| (DataType::Date64, ColumnPredicateLiteralV1::Int(value))
		| (
			DataType::Timestamp(TimeUnit::Millisecond, None),
			ColumnPredicateLiteralV1::Int(value),
		)
		| (
			DataType::Timestamp(TimeUnit::Microsecond, None),
			ColumnPredicateLiteralV1::Int(value),
		)
		| (DataType::Timestamp(TimeUnit::Nanosecond, None), ColumnPredicateLiteralV1::Int(value))
		| (DataType::Timestamp(TimeUnit::Millisecond, None), ColumnPredicateLiteralV1::Ts(value))
		| (DataType::Timestamp(TimeUnit::Microsecond, None), ColumnPredicateLiteralV1::Ts(value))
		| (DataType::Timestamp(TimeUnit::Nanosecond, None), ColumnPredicateLiteralV1::Ts(value)) => {
			encode_signed_stat_value(*value, 8).ok()
		}
		(DataType::UInt8, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 1).ok()
		}
		(DataType::UInt16, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 2).ok()
		}
		(DataType::UInt32, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 4).ok()
		}
		(DataType::UInt64, ColumnPredicateLiteralV1::Int(value)) => {
			encode_unsigned_stat_value(u64::try_from(*value).ok()?, 8).ok()
		}
		(DataType::Boolean, ColumnPredicateLiteralV1::Int(value)) => match *value {
			0 => Some(encode_bool_stat_value(false).to_vec()),
			1 => Some(encode_bool_stat_value(true).to_vec()),
			_ => None,
		},
		_ => None,
	}
}

fn slice_bounds(
	first_row: RecordRowIndex,
	row_count: RowCount,
	total_rows: usize,
) -> Result<(usize, usize), crate::storage::encoding::compute::EncodingComputeRejectCode> {
	let start = usize::try_from(first_row.get()).map_err(|_| {
		crate::storage::encoding::compute::EncodingComputeRejectCode::SelectionRangeOverflow
	})?;
	let len = usize::try_from(row_count.get()).map_err(|_| {
		crate::storage::encoding::compute::EncodingComputeRejectCode::SelectionRangeOverflow
	})?;
	let end = start.checked_add(len).ok_or(
		crate::storage::encoding::compute::EncodingComputeRejectCode::SelectionRangeOverflow,
	)?;
	if end > total_rows {
		return Err(
			crate::storage::encoding::compute::EncodingComputeRejectCode::SelectionRangeOverflow,
		);
	}
	Ok((start, end))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::PageNullCount;
	use crate::storage::encoding::EncodingPageStats;
	use crate::storage::stats::{ARROW_STATS_TAG_INT64, CanonicalScalar};

	fn int64_stat(min: i64, max: i64, nulls: u32) -> EncodingPageStats {
		EncodingPageStats {
			null_count: PageNullCount::new(nulls),
			min: Some(CanonicalScalar {
				arrow_type_tag: ARROW_STATS_TAG_INT64,
				bytes: encode_signed_stat_value(min, 8).unwrap(),
				truncated: false,
			}),
			max: Some(CanonicalScalar {
				arrow_type_tag: ARROW_STATS_TAG_INT64,
				bytes: encode_signed_stat_value(max, 8).unwrap(),
				truncated: false,
			}),
		}
	}

	fn atom(op: PredicateAtomOp, value: i64) -> PredicateAtom {
		PredicateAtom {
			op,
			literal: Some(ColumnPredicateLiteralV1::Int(value)),
		}
	}

	#[test]
	fn equality_classifies_disjoint_exact_and_maybe() {
		assert_eq!(
			classify_page_stats_for_atom(
				&int64_stat(10, 20, 0),
				8,
				&DataType::Int64,
				&atom(PredicateAtomOp::Eq, 5),
			),
			Some(PagePredicateClassification::Disjoint)
		);
		assert_eq!(
			classify_page_stats_for_atom(
				&int64_stat(7, 7, 0),
				8,
				&DataType::Int64,
				&atom(PredicateAtomOp::Eq, 7),
			),
			Some(PagePredicateClassification::ExactMatch)
		);
		assert_eq!(
			classify_page_stats_for_atom(
				&int64_stat(1, 9, 0),
				8,
				&DataType::Int64,
				&atom(PredicateAtomOp::Eq, 7),
			),
			Some(PagePredicateClassification::Maybe)
		);
	}

	#[test]
	fn null_predicates_are_null_count_conservative() {
		let mixed = int64_stat(1, 9, 2);
		assert_eq!(
			classify_page_stats_for_atom(&mixed, 8, &DataType::Int64, &PredicateAtom::is_null(),),
			Some(PagePredicateClassification::Maybe)
		);
		let non_null = int64_stat(1, 9, 0);
		assert_eq!(
			classify_page_stats_for_atom(
				&non_null,
				8,
				&DataType::Int64,
				&PredicateAtom::is_not_null(),
			),
			Some(PagePredicateClassification::ExactMatch)
		);
	}

	#[test]
	fn range_and_inequality_classify_boundaries() {
		assert_eq!(
			classify_page_stats_for_atom(
				&int64_stat(10, 20, 0),
				8,
				&DataType::Int64,
				&atom(PredicateAtomOp::Lt, 10),
			),
			Some(PagePredicateClassification::Disjoint)
		);
		assert_eq!(
			classify_page_stats_for_atom(
				&int64_stat(10, 20, 0),
				8,
				&DataType::Int64,
				&atom(PredicateAtomOp::Lte, 20),
			),
			Some(PagePredicateClassification::ExactMatch)
		);
		assert_eq!(
			classify_page_stats_for_atom(
				&int64_stat(10, 20, 3),
				8,
				&DataType::Int64,
				&atom(PredicateAtomOp::Gte, 10),
			),
			Some(PagePredicateClassification::Maybe)
		);
	}

	#[test]
	fn stitch_predicate_page_mask_combines_disjoint_exact_and_maybe_slices() {
		let plan = PredicatePageFetchPlan {
			row_count: RowCount::new(8),
			slices: vec![
				PredicatePageMaskSlice::Disjoint {
					first_row: RecordRowIndex::new(0),
					row_count: RowCount::new(2),
				},
				PredicatePageMaskSlice::ExactMatch {
					first_row: RecordRowIndex::new(2),
					row_count: RowCount::new(3),
				},
				PredicatePageMaskSlice::Maybe {
					first_row: RecordRowIndex::new(5),
					mask: EncodedRowMask::from_bool_slice(&[true, false, true]),
				},
			],
		};
		let mask = stitch_predicate_page_mask(&plan).unwrap();
		assert_eq!(
			mask.to_bool_vec().unwrap(),
			vec![false, false, true, true, true, true, false, true]
		);
	}

	#[test]
	fn private_decline_catalog_stays_closed_to_module() {
		let declines = [
			PredicatePageFetchDecline::MissingPageStats,
			PredicatePageFetchDecline::UnsupportedPredicate,
			PredicatePageFetchDecline::UnsupportedCascade,
			PredicatePageFetchDecline::PageGeometryMismatchAcrossPredicateColumns,
			PredicatePageFetchDecline::NotCheaperThanDense,
			PredicatePageFetchDecline::FragmentationCapExceeded,
			PredicatePageFetchDecline::MaxRangesExceeded,
			PredicatePageFetchDecline::MaxSimdPagesExceeded,
		];
		assert_eq!(declines.len(), 8);
	}
}
