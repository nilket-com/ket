//! ADR 0168: a SQL cell's answer on the receipt, bounded by the encoded
//! size it will occupy in a live event. Formatting proceeds column by
//! column and cell by cell against a byte budget and stops when the budget
//! is spent; no string proportional to the relation is ever built.

use crate::driver::{ResultColumn, ResultSummary, TruncationReason};
use crate::result_artifact::ResultArtifact;
use polars::frame::DataFrame;

/// Rows the preview holds at most.
pub(crate) const PREVIEW_ROWS: usize = 20;
/// Bytes the encoded `result` may occupy, envelope included. A live event
/// line is `live::MAX_LINE` (64 KiB) and also carries the command echo and
/// the notebook description, so the answer takes a quarter at most.
pub(crate) const RESULT_BUDGET_BYTES: usize = 16 * 1024;
/// The smallest cut a value is worth: below this many bytes of remaining
/// budget the row ends instead.
const MIN_VALUE_BYTES: usize = 8;

/// Encoded size of a JSON string for `text`, quotes included: the escaping
/// rules of RFC 8259 as the live encoder applies them (the quote and the
/// backslash escaped, control characters as two-character escapes or
/// `\uXXXX`, everything else as its UTF-8 bytes).
fn encoded_len(text: &str) -> usize {
	2 + text
		.chars()
		.map(|c| match c {
			'"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
			c if (c as u32) < 0x20 => 6,
			c => c.len_utf8(),
		})
		.sum::<usize>()
}

/// Cut `text` so that its encoded form fits `budget` bytes, at a character
/// boundary. Escaping can lengthen a character, so the cut is verified.
fn cut_to_fit(text: &str, budget: usize) -> String {
	let mut cut: String = text.chars().take(budget.saturating_sub(2)).collect();
	while !cut.is_empty() && encoded_len(&cut) > budget {
		cut.pop();
	}
	cut
}

/// The summary of `artifact`, or `None` when the projection cannot be
/// built (the receipt then carries no answer; the failure is the cell's).
pub(crate) fn summarize(artifact: &ResultArtifact) -> Option<ResultSummary> {
	let frame = artifact.projection().ok()?;
	Some(summarize_frame(&frame, artifact.height()))
}

/// `frame` is the projection; `rows` the relation's height (the frame's
/// height for a whole projection).
pub(crate) fn summarize_frame(frame: &DataFrame, rows: usize) -> ResultSummary {
	// Envelope: the field names and punctuation of the summary itself.
	let mut remaining = RESULT_BUDGET_BYTES.saturating_sub(
		r#"{"columns":[],"rows":,"preview":[],"truncated":false,"truncation_reasons":[]}"#.len()
			+ 20,
	);
	let mut reasons = Vec::new();
	let mut columns = Vec::new();
	for column in frame.columns() {
		let entry = ResultColumn {
			name: column.name().to_string(),
			dtype: column.dtype().to_string(),
		};
		let cost =
			r#"{"name":,"dtype":},"#.len() + encoded_len(&entry.name) + encoded_len(&entry.dtype);
		if cost > remaining {
			reasons.push(TruncationReason::Columns);
			break;
		}
		remaining -= cost;
		columns.push(entry);
	}
	let mut preview = Vec::new();
	if !reasons.contains(&TruncationReason::Columns) {
		let width = columns.len();
		let shown = frame.height().min(PREVIEW_ROWS);
		let mut value_cut = false;
		'rows: for row in 0..shown {
			let mut cells = Vec::with_capacity(width);
			let mut row_cost = 3; // "[]," around the row
			for column in frame.columns().iter().take(width) {
				let cell = crate::gui::table::cell_text(column, row);
				let cost = match &cell {
					None => 5,
					Some(text) => encoded_len(text) + 1,
				};
				if row_cost + cost > remaining {
					// A single value may be cut to fit; a row that cannot even
					// hold a cut value ends the preview here.
					let left = remaining.saturating_sub(row_cost + 1);
					match cell {
						Some(text) if left >= MIN_VALUE_BYTES => {
							cells.push(Some(cut_to_fit(&text, left)));
							value_cut = true;
							// The rest of this row cannot be shown; stop after it.
							preview.push(cells);
							reasons.push(TruncationReason::Bytes);
							break 'rows;
						}
						_ => {
							reasons.push(TruncationReason::Bytes);
							break 'rows;
						}
					}
				}
				row_cost += cost;
				cells.push(cell);
			}
			remaining -= row_cost;
			preview.push(cells);
		}
		if value_cut {
			reasons.push(TruncationReason::Value);
		}
		if rows > preview.len() && !reasons.contains(&TruncationReason::Bytes) {
			reasons.push(TruncationReason::Rows);
		}
	}
	ResultSummary {
		columns,
		rows: rows as u64,
		truncated: !reasons.is_empty(),
		truncation_reasons: reasons,
		preview,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::prelude::*;

	fn encoded(summary: &ResultSummary) -> usize {
		crate::live::encode(summary).unwrap().len()
	}

	#[test]
	fn the_length_rule_matches_the_live_encoder() {
		for text in [
			"plain",
			"quote\"d",
			"back\\slash",
			"tab\tnew\nline",
			"\u{1}ctl",
			"\u{e9}\u{f1}\u{65e5}\u{672c}",
		] {
			assert_eq!(
				encoded_len(text),
				crate::live::encode(&text).unwrap().len() - 1,
				"{text:?}"
			);
		}
	}

	#[test]
	fn a_small_result_is_whole_and_nullness_is_data() {
		let frame = DataFrame::new(
			2,
			vec![
				Column::new("x".into(), [Some(1_i64), None]),
				Column::new("s".into(), [Some("null"), None]),
			],
		)
		.unwrap();
		let summary = summarize_frame(&frame, 2);
		assert_eq!(summary.columns.len(), 2);
		assert_eq!(summary.columns[0].name, "x");
		assert_eq!(summary.rows, 2);
		assert_eq!(
			summary.preview,
			vec![
				vec![Some("1".to_owned()), Some("null".to_owned())],
				vec![None, None]
			]
		);
		assert!(!summary.truncated && summary.truncation_reasons.is_empty());
	}

	#[test]
	fn the_row_bound_names_itself_and_an_empty_relation_keeps_its_columns() {
		let frame = DataFrame::new(
			1000,
			vec![Column::new("n".into(), (0..1000_i64).collect::<Vec<_>>())],
		)
		.unwrap();
		let summary = summarize_frame(&frame, 1000);
		assert_eq!(summary.rows, 1000);
		assert_eq!(summary.preview.len(), PREVIEW_ROWS);
		assert_eq!(summary.truncation_reasons, vec![TruncationReason::Rows]);
		let empty = DataFrame::new(0, vec![Column::new("x".into(), Vec::<i64>::new())]).unwrap();
		let summary = summarize_frame(&empty, 0);
		assert_eq!(summary.columns.len(), 1);
		assert_eq!(summary.rows, 0);
		assert!(summary.preview.is_empty() && !summary.truncated);
	}

	#[test]
	fn every_bound_keeps_the_encoding_under_budget() {
		// Wide: the schema fits, twenty rows do not.
		let wide = DataFrame::new(
			20,
			(0..300)
				.map(|i| Column::new(format!("c{i}").into(), vec![Some("x".repeat(60)); 20]))
				.collect(),
		)
		.unwrap();
		let summary = summarize_frame(&wide, 20);
		assert!(
			summary
				.truncation_reasons
				.contains(&TruncationReason::Bytes),
			"{:?}",
			summary.truncation_reasons
		);
		assert!(
			encoded(&summary) <= RESULT_BUDGET_BYTES,
			"{}",
			encoded(&summary)
		);
		// Schema alone exceeds the budget: leading columns, no preview.
		let many = DataFrame::new(
			1,
			(0..2000)
				.map(|i| Column::new(format!("column_number_{i}").into(), [1_i64]))
				.collect(),
		)
		.unwrap();
		let summary = summarize_frame(&many, 1);
		assert_eq!(summary.truncation_reasons, vec![TruncationReason::Columns]);
		assert!(
			summary.preview.is_empty()
				&& summary.columns.len() < 2000
				&& !summary.columns.is_empty()
		);
		assert!(encoded(&summary) <= RESULT_BUDGET_BYTES);
		// One enormous value is cut at a character boundary.
		let big = DataFrame::new(
			1,
			vec![Column::new("big".into(), [Some("\u{e9}".repeat(40_000))])],
		)
		.unwrap();
		let summary = summarize_frame(&big, 1);
		assert!(
			summary
				.truncation_reasons
				.contains(&TruncationReason::Value)
		);
		let cell = summary.preview[0][0].as_deref().unwrap();
		assert!(cell.chars().all(|c| c == '\u{e9}') && !cell.is_empty());
		assert!(encoded(&summary) <= RESULT_BUDGET_BYTES);
	}
}
