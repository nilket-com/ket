use crate::model::{Corpus, RankedSymbol};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
struct LineScore {
	score: i64,
	col_byte: usize,
}

pub fn rank(query: &str, corpus: &Corpus) -> Vec<RankedSymbol> {
	let mut best = BTreeMap::<usize, i64>::new();
	for (file_index, file) in corpus.files.iter().enumerate() {
		let candidates = corpus
			.symbols
			.iter()
			.enumerate()
			.filter(|(_, symbol)| symbol.file_index == file_index)
			.collect::<Vec<_>>();
		let mut line_start = 0usize;
		for segment in file.text.split_inclusive('\n') {
			let without_newline = segment.strip_suffix('\n').unwrap_or(segment);
			let line = without_newline
				.strip_suffix('\r')
				.unwrap_or(without_newline);
			if let Some(score) = score_line(query, line) {
				let hit = line_start.saturating_add(score.col_byte);
				let owner = candidates
					.iter()
					.filter(|(_, symbol)| {
						symbol.id.start as usize <= hit && hit < symbol.id.end as usize
					})
					.min_by_key(|(_, symbol)| symbol.id.end.saturating_sub(symbol.id.start));
				if let Some((symbol_index, _)) = owner {
					best.entry(*symbol_index)
						.and_modify(|current| *current = (*current).max(score.score))
						.or_insert(score.score);
				}
			}
			line_start = line_start.saturating_add(segment.len());
		}
	}
	let mut ranked = best
		.into_iter()
		.map(|(symbol_index, score)| {
			let symbol = &corpus.symbols[symbol_index];
			RankedSymbol {
				id: symbol.id.clone(),
				name: symbol.name.clone(),
				score: score as f64,
			}
		})
		.collect::<Vec<_>>();
	ranked.sort_by(|left, right| {
		right
			.score
			.total_cmp(&left.score)
			.then_with(|| left.id.cmp(&right.id))
	});
	ranked
}

fn score_line(query: &str, line: &str) -> Option<LineScore> {
	if query.is_empty() {
		return None;
	}
	let case_sensitive = query.chars().any(char::is_uppercase);
	let query_chars = query.chars().collect::<Vec<_>>();
	let line_chars = line.char_indices().collect::<Vec<_>>();

	if query_chars.len() <= line_chars.len() {
		for start in 0..=line_chars.len() - query_chars.len() {
			if query_chars.iter().enumerate().all(|(offset, query_ch)| {
				chars_equal(*query_ch, line_chars[start + offset].1, case_sensitive)
			}) {
				let col = line_chars[start].0;
				return Some(LineScore {
					score: 3_000_000i64.saturating_sub(col as i64),
					col_byte: col,
				});
			}
		}
	}

	if let Some((col, gap)) = subsequence(&query_chars, &line_chars, case_sensitive, true) {
		return Some(LineScore {
			score: 2_000_000i64
				.saturating_sub((gap as i64).saturating_mul(8))
				.saturating_sub(col as i64),
			col_byte: col,
		});
	}
	let (col, gap) = subsequence(&query_chars, &line_chars, case_sensitive, false)?;
	Some(LineScore {
		score: 1_000_000i64
			.saturating_sub((gap as i64).saturating_mul(8))
			.saturating_sub(col as i64),
		col_byte: col,
	})
}

fn subsequence(
	query: &[char],
	line: &[(usize, char)],
	case_sensitive: bool,
	boundaries_only: bool,
) -> Option<(usize, usize)> {
	let mut cursor = 0usize;
	let mut first = None;
	let mut previous = None;
	let mut gap = 0usize;
	for query_ch in query {
		let found = (cursor..line.len()).find(|index| {
			chars_equal(*query_ch, line[*index].1, case_sensitive)
				&& (!boundaries_only || is_word_boundary(line, *index))
		})?;
		first.get_or_insert(line[found].0);
		if let Some(previous) = previous {
			gap = gap.saturating_add(found.saturating_sub(previous + 1));
		}
		previous = Some(found);
		cursor = found + 1;
	}
	Some((first?, gap))
}

fn chars_equal(query: char, candidate: char, case_sensitive: bool) -> bool {
	if case_sensitive {
		query == candidate
	} else {
		query.to_lowercase().eq(candidate.to_lowercase())
	}
}

fn is_word_boundary(line: &[(usize, char)], index: usize) -> bool {
	index == 0
		|| !line[index - 1].1.is_alphanumeric()
		|| line[index - 1].1.is_lowercase() && line[index].1.is_uppercase()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::corpus::extract_file_symbols;
	use crate::model::{ResolutionStats, SourceFile};
	use std::path::PathBuf;

	#[test]
	fn scorer_matches_the_shipped_exact_boundary_and_subsequence_tiers() {
		let exact = score_line("render_frame", "fn render_frame()").unwrap();
		let boundary = score_line("rf", "fn render_frame()").unwrap();
		let plain = score_line("rndr", "fn render_frame()").unwrap();
		assert!(exact.score > boundary.score);
		assert!(boundary.score > plain.score);
	}

	#[test]
	fn raw_line_hits_map_to_the_innermost_containing_function() {
		let text = "fn outer() { fn inner() { supervisor(); } }";
		let symbols = extract_file_symbols("src/test.rs", text);
		let corpus = Corpus {
			root: PathBuf::new(),
			files: vec![SourceFile {
				relative_path: "src/test.rs".to_string(),
				canonical_path: PathBuf::new(),
				text: text.to_string(),
			}],
			symbols,
			resolution: ResolutionStats::default(),
			resolution_backend: None,
			resolution_error: None,
		};
		let ranked = rank("supervisor", &corpus);
		assert_eq!(
			ranked.first().map(|symbol| symbol.name.as_str()),
			Some("inner")
		);
	}
}
