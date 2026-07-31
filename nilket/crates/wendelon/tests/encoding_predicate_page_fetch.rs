use arrow::datatypes::DataType;
use wendelon::schema::PageNullCount;
use wendelon::schema::pbc::ColumnPredicateLiteral;
use wendelon::storage::encoding::predicate_page_fetch::{
	PagePredicateClassification, classify_page_stats_for_atom,
};
use wendelon::storage::encoding::{EncodingPageStats, PredicateAtom, PredicateAtomOp};
use wendelon::storage::stats::{ARROW_STATS_TAG_INT64, CanonicalScalar, encode_signed_stat_value};

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

fn int64_atom(op: PredicateAtomOp, value: i64) -> PredicateAtom {
	PredicateAtom {
		op,
		literal: Some(ColumnPredicateLiteral::Int(value)),
	}
}

#[test]
fn equality_classifies_pages_as_disjoint_exact_or_maybe() {
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(10, 20, 0),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Eq, 5),
		),
		Some(PagePredicateClassification::Disjoint)
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(7, 7, 0),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Eq, 7),
		),
		Some(PagePredicateClassification::ExactMatch)
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(1, 9, 0),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Eq, 7),
		),
		Some(PagePredicateClassification::Maybe)
	);
}

#[test]
fn nulls_prevent_exact_value_matches_but_support_null_predicates() {
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(7, 7, 1),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Eq, 7),
		),
		Some(PagePredicateClassification::Maybe)
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(7, 7, 0),
			8,
			&DataType::Int64,
			&PredicateAtom::is_not_null(),
		),
		Some(PagePredicateClassification::ExactMatch)
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(7, 7, 8),
			8,
			&DataType::Int64,
			&PredicateAtom::is_null(),
		),
		Some(PagePredicateClassification::ExactMatch)
	);
}

#[test]
fn range_predicates_use_min_max_conservatively() {
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(10, 20, 0),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Lt, 10),
		),
		Some(PagePredicateClassification::Disjoint)
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(10, 20, 0),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Lte, 20),
		),
		Some(PagePredicateClassification::ExactMatch)
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(10, 20, 3),
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Gte, 10),
		),
		Some(PagePredicateClassification::Maybe)
	);
}

#[test]
fn unsupported_or_truncated_value_stats_decline_classification() {
	let mut stat = int64_stat(1, 9, 0);
	stat.min.as_mut().unwrap().truncated = true;
	assert_eq!(
		classify_page_stats_for_atom(
			&stat,
			8,
			&DataType::Int64,
			&int64_atom(PredicateAtomOp::Eq, 7),
		),
		None
	);
	assert_eq!(
		classify_page_stats_for_atom(
			&int64_stat(1, 9, 0),
			8,
			&DataType::Utf8,
			&int64_atom(PredicateAtomOp::Eq, 7),
		),
		None
	);
}
