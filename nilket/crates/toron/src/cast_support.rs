//! The explicit-cast support table (ADR 564).
//!
//! One declaration classifies every castable type-family pair along two
//! dimensions: whether the conversion can fail, and who may construct it. The
//! SQL binder admits a pair only when a user may write it; the executor
//! conformance tests cover every supported pair regardless, since a
//! binder-synthesized cast still executes. Before this table, the surface was
//! three hand-kept lists whose agreement was documented nowhere — and they had
//! already drifted.

use arrow::datatypes::DataType;

/// A castable type family. `Decimal128` is one family, not one entry per
/// `(precision, scale)`; the executors handle the instance parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastFamily {
	Int32,
	Int64,
	Float32,
	Float64,
	Decimal128,
	Utf8,
	LargeUtf8,
	Date32,
	Timestamp,
}

impl CastFamily {
	#[cfg(test)]
	pub(crate) const ALL: &'static [Self] = &[
		Self::Int32,
		Self::Int64,
		Self::Float32,
		Self::Float64,
		Self::Decimal128,
		Self::Utf8,
		Self::LargeUtf8,
		Self::Date32,
		Self::Timestamp,
	];
}

pub(crate) fn cast_family(data_type: &DataType) -> Option<CastFamily> {
	match data_type {
		DataType::Int32 => Some(CastFamily::Int32),
		DataType::Int64 => Some(CastFamily::Int64),
		DataType::Float32 => Some(CastFamily::Float32),
		DataType::Float64 => Some(CastFamily::Float64),
		DataType::Decimal128(_, _) => Some(CastFamily::Decimal128),
		DataType::Utf8 => Some(CastFamily::Utf8),
		DataType::LargeUtf8 => Some(CastFamily::LargeUtf8),
		DataType::Date32 => Some(CastFamily::Date32),
		DataType::Timestamp(_, _) => Some(CastFamily::Timestamp),
		_ => None,
	}
}

/// Whether a conversion can fail on a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastSupport {
	/// Every value converts. `TRY_CAST` is identical by inheritance.
	Infallible,
	/// A value can fail: strict `CAST` raises naming it; `TRY_CAST` yields
	/// NULL for it.
	Fallible,
}

/// Who may construct the cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastAdmission {
	/// A user may write it as `CAST`/`TRY_CAST`.
	SqlCast,
	/// Only the binder's own coercions construct it — the decimal arithmetic
	/// arms ADR 263 synthesizes. Admitting these to user SQL would be a new
	/// decision; deleting them would break arithmetic.
	SynthesizedOnly,
}

/// The single source of truth. `None` is unsupported; identity pairs
/// short-circuit before any lookup and are not listed.
pub(crate) fn explicit_cast_rule(
	source: CastFamily,
	target: CastFamily,
) -> Option<(CastSupport, CastAdmission)> {
	use CastAdmission::{SqlCast, SynthesizedOnly};
	use CastFamily as F;
	use CastSupport::{Fallible, Infallible};
	match (source, target) {
		// Numeric widenings never fail.
		(F::Int32, F::Int64)
		| (F::Int32, F::Float32)
		| (F::Int32, F::Float64)
		| (F::Int64, F::Float32)
		| (F::Int64, F::Float64)
		| (F::Float32, F::Float64) => Some((Infallible, SqlCast)),
		// Numeric narrowings fail on out-of-range or fractional values.
		(F::Int64, F::Int32)
		| (F::Float32, F::Int32)
		| (F::Float32, F::Int64)
		| (F::Float64, F::Int32)
		| (F::Float64, F::Int64)
		| (F::Float64, F::Float32) => Some((Fallible, SqlCast)),
		// ADR 263's decimal coercions: binder-synthesized for arithmetic,
		// deliberately not user-writable.
		(F::Int32, F::Decimal128) | (F::Int64, F::Decimal128) | (F::Decimal128, F::Decimal128) => {
			Some((Fallible, SynthesizedOnly))
		}
		// ADR 564: text parses to numerics per row; either string width.
		(F::Utf8 | F::LargeUtf8, F::Int32 | F::Int64 | F::Float32 | F::Float64) => {
			Some((Fallible, SqlCast))
		}
		// ADR 564: text parses to temporals through the ADR 558 kernels, so
		// the two spellings are one node and cannot diverge.
		(F::Utf8 | F::LargeUtf8, F::Timestamp) | (F::Utf8 | F::LargeUtf8, F::Date32) => {
			Some((Fallible, SqlCast))
		}
		// ADR 564: rendering to text is canonical and never fails.
		(
			F::Int32
			| F::Int64
			| F::Float32
			| F::Float64
			| F::Decimal128
			| F::Date32
			| F::Timestamp,
			F::Utf8,
		) => Some((Infallible, SqlCast)),
		_ => None,
	}
}

#[cfg(test)]
mod adr564_table_tests {
	use super::*;

	/// The synthesized decimal pairs must never be admitted to user SQL, and
	/// every user-writable pair must be declared — the two populations the
	/// admission dimension exists to separate.
	#[test]
	fn synthesized_pairs_are_not_sql_admissible() {
		for (source, target) in [
			(CastFamily::Int32, CastFamily::Decimal128),
			(CastFamily::Int64, CastFamily::Decimal128),
			(CastFamily::Decimal128, CastFamily::Decimal128),
		] {
			assert_eq!(
				explicit_cast_rule(source, target),
				Some((CastSupport::Fallible, CastAdmission::SynthesizedOnly))
			);
		}
	}

	/// Rendering to text is total; parsing from text is not. The table must
	/// say so for every string pair, or an executor could disagree silently.
	#[test]
	fn string_pairs_declare_their_fallibility() {
		for source in [CastFamily::Utf8, CastFamily::LargeUtf8] {
			for target in [
				CastFamily::Int32,
				CastFamily::Int64,
				CastFamily::Float32,
				CastFamily::Float64,
				CastFamily::Date32,
				CastFamily::Timestamp,
			] {
				assert_eq!(
					explicit_cast_rule(source, target),
					Some((CastSupport::Fallible, CastAdmission::SqlCast)),
					"{source:?} -> {target:?}"
				);
			}
		}
		for source in [
			CastFamily::Int32,
			CastFamily::Int64,
			CastFamily::Float32,
			CastFamily::Float64,
			CastFamily::Decimal128,
			CastFamily::Date32,
			CastFamily::Timestamp,
		] {
			assert_eq!(
				explicit_cast_rule(source, CastFamily::Utf8),
				Some((CastSupport::Infallible, CastAdmission::SqlCast)),
				"{source:?} -> Utf8"
			);
		}
	}

	/// LargeUtf8 is a source family only in this cut: no pair targets it, so a
	/// rendering cannot silently widen.
	#[test]
	fn large_utf8_is_not_a_target() {
		for source in CastFamily::ALL {
			assert_eq!(explicit_cast_rule(*source, CastFamily::LargeUtf8), None);
		}
	}
}
