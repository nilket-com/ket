//! Markdown table column widths, in characters.
//!
//! How wide each column of a Markdown table should be is a property of what
//! its cells hold, not of the surface that draws them: a terminal rasterizer
//! and a GUI layout both ask the same question, so the answer lives here
//! (ADR 0012 libaryon). Everything is counted in characters; a head converts
//! its pane to characters and the chosen widths back to its own units.
//!
//! The word model the choice scores with is here too, because scoring must
//! break lines exactly as a head draws them: [`wrap_inline_words`] returns the
//! lines, [`inline_line_count`] counts them without allocating.

use crate::svp::markdown_blocks::{MarkdownInlineStyle, MarkdownInlineText};

/// What one table column's cells ask of its width.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ColumnDemand {
	/// Characters in the column's longest cell: the width at which nothing in
	/// it wraps or clips (its max-content width).
	longest_cell: usize,
	/// Characters in the column's longest visible token, a run with no
	/// whitespace, counted across inline style boundaries (`foo**bar**baz` is
	/// one token): the narrowest width that breaks no token (min-content).
	longest_token: usize,
	/// Characters in all the column's cells: how much text a width serves.
	volume: usize,
}

/// One pass over every cell, header included; nothing is allocated per cell.
fn column_demands(rows: &[Vec<MarkdownInlineText>], columns: usize) -> Vec<ColumnDemand> {
	let mut demands = vec![ColumnDemand::default(); columns];
	for row in rows {
		for (demand, cell) in demands.iter_mut().zip(row) {
			let mut chars = 0usize;
			let mut token = 0usize;
			for ch in cell.spans().iter().flat_map(|span| span.text.chars()) {
				chars += 1;
				if ch.is_whitespace() {
					token = 0;
				} else {
					token += 1;
					demand.longest_token = demand.longest_token.max(token);
				}
			}
			demand.longest_cell = demand.longest_cell.max(chars);
			demand.volume += chars;
		}
	}
	demands
}

/// Character widths for a table's columns and the gutter between them, for a
/// head that spaces columns with characters (Baryon's grid).
///
/// The gutter is 2 characters, then 1, then 0 as the space shrinks below
/// `columns + 2 * (columns - 1)` and `columns + (columns - 1)`; the rest is
/// shared by [`table_content_widths`]. Every column is at least one character
/// wide, so a table with more columns than the space has characters is wider
/// than the space and is clipped.
pub fn table_column_widths(
	rows: &[Vec<MarkdownInlineText>],
	available_chars: usize,
) -> (Vec<usize>, usize) {
	let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
	if columns == 0 {
		return (Vec::new(), 0);
	}
	let available_chars = available_chars.max(1);
	let gutter: usize = if columns == 1 {
		0
	} else if available_chars >= columns.saturating_add((columns - 1).saturating_mul(2)) {
		2
	} else if available_chars >= columns.saturating_add(columns - 1) {
		1
	} else {
		0
	};
	let content_chars =
		available_chars.saturating_sub(gutter.saturating_mul(columns.saturating_sub(1)));
	(table_content_widths(rows, content_chars), gutter)
}

/// Character widths for a table's columns, fitted to what the cells contain,
/// sharing `content_chars` of text space with no gaps between columns (a head
/// that spaces columns itself, with padding, calls this directly).
///
/// 1. **Everything fits:** each column gets its longest cell and the surplus is
///    spread evenly (ties to the larger demand, then the earlier column), so a
///    table that fits keeps its balanced look and nothing wraps.
/// 2. **Short columns whole, long columns wrap:** otherwise the candidates
///    keep the `k` shortest columns at their longest cell, for every `k` that
///    leaves the rest a character each, and fit the remaining columns around
///    them ([`fit_long_columns`]), once protecting their tokens and once by
///    text volume alone. The candidate with the lowest [`table_wrap_cost`]
///    wins; among equals, the one keeping more columns whole, then the one
///    protecting tokens. So identifiers, statuses, dates and numbers stay on
///    one line and the long prose column takes the wrapping, unless keeping
///    them whole would starve the prose into a taller table.
///
/// One width per column of the widest row; every width is at least one, so
/// less space than columns yields a table wider than the space.
pub fn table_content_widths(rows: &[Vec<MarkdownInlineText>], content_chars: usize) -> Vec<usize> {
	const MIN_CELL_WIDTH: usize = 4;

	let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
	if columns == 0 {
		return Vec::new();
	}
	let content_space = content_chars.max(columns);
	let min_width = MIN_CELL_WIDTH.min(content_space / columns).max(1);
	let demands = column_demands(rows, columns);
	let full = demands
		.iter()
		.map(|demand| demand.longest_cell.max(min_width))
		.collect::<Vec<_>>();

	let full_total = full.iter().sum::<usize>();
	if full_total <= content_space {
		let mut widths = full.clone();
		let extra = content_space - full_total;
		for width in &mut widths {
			*width += extra / columns;
		}
		let mut order = (0..columns).collect::<Vec<_>>();
		order.sort_by_key(|&column| std::cmp::Reverse(full[column]));
		for &column in order.iter().take(extra % columns) {
			widths[column] += 1;
		}
		return widths;
	}

	// Short columns first: the candidates keep the k shortest columns whole
	// (ties to the earlier column) and fit the rest around them, scored by
	// their real wrapped cost. Nothing here depends on the pane's width except
	// the space itself, so a wider pane never makes a candidate costlier.
	let longest = demands
		.iter()
		.map(|demand| demand.longest_cell.max(1))
		.collect::<Vec<_>>();
	let widest = longest.iter().copied().max().unwrap_or(1);
	let short = |column: usize| longest[column].saturating_mul(2) <= widest;
	let mut by_length = (0..columns).collect::<Vec<_>>();
	by_length.sort_by_key(|&column| (longest[column], column));
	type CandidateKey = (usize, std::cmp::Reverse<usize>, std::cmp::Reverse<bool>);
	let mut best: Option<(CandidateKey, Vec<usize>)> = None;
	for whole in 0..columns {
		let kept = &by_length[..whole];
		let kept_width = kept.iter().map(|&column| longest[column]).sum::<usize>();
		let open = &by_length[whole..];
		if kept_width + open.len() > content_space {
			break;
		}
		for protect_tokens in [true, false] {
			let mut widths = vec![0usize; columns];
			for &column in kept {
				widths[column] = longest[column];
			}
			fit_long_columns(
				&mut widths,
				open,
				content_space - kept_width,
				&demands,
				protect_tokens,
			);
			let cost = table_wrap_cost(rows, &widths, open, short);
			// Among equal costs: more columns whole, then tokens protected.
			let key = (
				cost,
				std::cmp::Reverse(whole),
				std::cmp::Reverse(protect_tokens),
			);
			if best.as_ref().is_none_or(|(best_key, _)| key < *best_key) {
				best = Some((key, widths));
			}
		}
	}
	best.map(|(_, widths)| widths)
		.expect("keeping no column whole is always feasible: content_space >= columns")
}

/// A long column's cells read acceptably from this many characters.
const READABLE_CELL_WIDTH: usize = 4;
/// Tokens up to this long are kept whole when there is room; a longer one (a
/// URL, a hash) breaks rather than claiming the table.
const PROTECTED_TOKEN_CHARS: usize = 40;

/// The width below which a long column would break one of its tokens.
fn token_floor(demand: &ColumnDemand) -> usize {
	let longest = demand.longest_cell.max(1);
	demand
		.longest_token
		.min(PROTECTED_TOKEN_CHARS)
		.clamp(READABLE_CELL_WIDTH.min(longest), longest)
}

/// Fits the `open` (long) columns into `space`, one character at a time.
///
/// Every column starts at one character. Each further character goes to the
/// column with the highest priority, never past its longest cell:
///
/// 1. a column narrower than [`READABLE_CELL_WIDTH`];
/// 2. with `protect_tokens`, then a column narrower than its longest token
///    ([`token_floor`]);
/// 3. then the column where it saves the most estimated lines, `volume /
///    width`, i.e. the largest `volume / (width * (width + 1))`.
///
/// Within a tier the larger saving wins, then the earlier column. The order
/// does not depend on `space`, so the fit for one more character is the fit
/// for this one plus a character: a wider pane only ever widens a column. A
/// space larger than every open column's longest cell leaves the rest to the
/// earliest open column.
fn fit_long_columns(
	widths: &mut [usize],
	open: &[usize],
	space: usize,
	demands: &[ColumnDemand],
	protect_tokens: bool,
) {
	let mut open = open.to_vec();
	open.sort_unstable();
	for &column in &open {
		widths[column] = 1;
	}
	let mut extra = space.saturating_sub(open.len());
	while extra > 0 {
		let priority = |column: usize| {
			let width = widths[column];
			let demand = &demands[column];
			let tier = if width < READABLE_CELL_WIDTH {
				2u8
			} else if protect_tokens && width < token_floor(demand) {
				1
			} else {
				0
			};
			(
				tier,
				demand.volume.max(1) as u128,
				(width as u128) * (width as u128 + 1),
			)
		};
		let target = open
			.iter()
			.copied()
			.filter(|&column| widths[column] < demands[column].longest_cell.max(1))
			.reduce(|best, column| {
				let (best_tier, best_volume, best_cost) = priority(best);
				let (tier, volume, cost) = priority(column);
				// Compare volume / cost fractions without dividing.
				let better = tier > best_tier
					|| (tier == best_tier && volume * best_cost > best_volume * cost);
				if better { column } else { best }
			})
			.or_else(|| open.first().copied());
		let Some(column) = target else {
			break;
		};
		widths[column] += 1;
		extra -= 1;
	}
}

/// What a candidate layout costs, in text lines with wrap on: per row, its
/// rendered height plus the row's worst blemish, the larger of
///
/// - the lines beyond its first that a cell of a *short* column (one whose
///   longest cell is at most half the table's longest cell) wraps onto, and
/// - the breaks forced inside a cell's tokens (runs with no whitespace).
///
/// A wrapped status, or an identifier cut in pieces, reads worse than the
/// same line of wrapped prose, so its row pays for it twice: keeping one whole
/// is worth a line of height. The charge is per row and only for the worst
/// cell (its full value, which can be several lines), so many blemishes in one
/// row never outweigh the room the prose needs. That is a readability
/// heuristic, not a measure: one badly split cell and several equally badly
/// split cells cost a row the same, and which columns count as short follows
/// the table's longest cell, so editing one outlier cell can reclassify them.
/// Only the `open` columns can wrap; the others show their longest cell whole.
fn table_wrap_cost(
	rows: &[Vec<MarkdownInlineText>],
	widths: &[usize],
	open: &[usize],
	short: impl Fn(usize) -> bool,
) -> usize {
	rows.iter()
		.map(|row| {
			let mut tallest = 1;
			let mut blemish = 0;
			for &column in open {
				let Some(cell) = row.get(column) else {
					continue;
				};
				let width = widths[column].max(1);
				let lines = inline_line_count(cell, width);
				tallest = tallest.max(lines);
				if short(column) {
					blemish = blemish.max(lines.saturating_sub(1));
				}
				blemish = blemish.max(forced_token_breaks(cell, width));
			}
			tallest + blemish
		})
		.sum()
}

/// Breaks a column `width` characters wide forces inside the cell's tokens
/// (runs with no whitespace, counted across inline style boundaries), as
/// `ceil(token / width) - 1` each. A proxy: a token made of styled fragments
/// can draw on more rows than that (fragments of 4 + 4 + 4 at width 6 draw on
/// three rows; the proxy counts one break).
fn forced_token_breaks(cell: &MarkdownInlineText, width: usize) -> usize {
	let width = width.max(1);
	let mut breaks = 0;
	let mut token = 0usize;
	for ch in cell.spans().iter().flat_map(|span| span.text.chars()) {
		if ch.is_whitespace() {
			breaks += token.saturating_sub(1) / width;
			token = 0;
		} else {
			token += 1;
		}
	}
	breaks + token.saturating_sub(1) / width
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkdownInlineWord {
	pub style: MarkdownInlineStyle,
	pub text: String,
	pub leading_space: bool,
}

pub fn inline_words(text: &MarkdownInlineText) -> Vec<MarkdownInlineWord> {
	let mut words = Vec::new();
	let mut pending_space = false;
	for span in text.spans() {
		let mut word = String::new();
		for ch in span.text.chars() {
			if ch.is_whitespace() {
				if !word.is_empty() {
					words.push(MarkdownInlineWord {
						style: span.style,
						text: std::mem::take(&mut word),
						leading_space: pending_space && !words.is_empty(),
					});
				}
				pending_space = !words.is_empty();
				continue;
			}
			word.push(ch);
		}
		if !word.is_empty() {
			words.push(MarkdownInlineWord {
				style: span.style,
				text: word,
				leading_space: pending_space && !words.is_empty(),
			});
			pending_space = false;
		}
	}
	words
}

/// Flows inline words into lines of at most `max_chars` characters; a word
/// longer than a line breaks on character boundaries. This is the line
/// breaking [`draw_wrapped_inline_text`] applies to prose, returned as data so
/// a table can measure a wrapped cell with the same lines it draws.
pub fn wrap_inline_words(
	text: &MarkdownInlineText,
	max_chars: usize,
) -> Vec<Vec<MarkdownInlineWord>> {
	let max_chars = max_chars.max(1);
	let mut lines = Vec::new();
	let mut line = Vec::new();
	let mut line_len = 0usize;
	for word in inline_words(text) {
		let style = word.style;
		let mut remaining = word.text;
		let mut leading_space = word.leading_space;
		while !remaining.is_empty() {
			let remaining_len = remaining.chars().count();
			let space_len = usize::from(leading_space && line_len > 0);
			if line_len > 0 && line_len + space_len + remaining_len > max_chars {
				lines.push(std::mem::take(&mut line));
				line_len = 0;
				leading_space = false;
				continue;
			}
			// An empty line takes the whole width; a non-empty one reached
			// here only because the word fits after it.
			if remaining_len <= max_chars.saturating_sub(line_len + space_len) {
				line_len += space_len + remaining_len;
				line.push(MarkdownInlineWord {
					style,
					text: remaining,
					leading_space: space_len > 0,
				});
				break;
			}
			line.push(MarkdownInlineWord {
				style,
				text: take_chars(&remaining, max_chars),
				leading_space: false,
			});
			lines.push(std::mem::take(&mut line));
			remaining = skip_chars(&remaining, max_chars);
			leading_space = false;
		}
	}
	if !line.is_empty() {
		lines.push(line);
	}
	lines
}

/// Calls `each(length, leading_space)` for every word [`inline_words`] would
/// produce, in order, without building them.
fn for_each_inline_word_len(text: &MarkdownInlineText, mut each: impl FnMut(usize, bool)) {
	let mut pending_space = false;
	let mut any = false;
	for span in text.spans() {
		let mut len = 0usize;
		for ch in span.text.chars() {
			if ch.is_whitespace() {
				if len > 0 {
					each(len, pending_space && any);
					any = true;
					len = 0;
				}
				pending_space = any;
				continue;
			}
			len += 1;
		}
		if len > 0 {
			each(len, pending_space && any);
			any = true;
			pending_space = false;
		}
	}
}

/// How many lines [`wrap_inline_words`] produces for `text` at `max_chars`,
/// step for step the same breaking, without allocating the lines. Layout
/// measures every table on every render; only drawing needs the words.
pub fn inline_line_count(text: &MarkdownInlineText, max_chars: usize) -> usize {
	let max_chars = max_chars.max(1);
	let mut lines = 0usize;
	let mut line_len = 0usize;
	for_each_inline_word_len(text, |len, leading_space| {
		let mut remaining = len;
		let mut leading_space = leading_space;
		while remaining > 0 {
			let space_len = usize::from(leading_space && line_len > 0);
			if line_len > 0 && line_len + space_len + remaining > max_chars {
				lines += 1;
				line_len = 0;
				leading_space = false;
				continue;
			}
			if remaining <= max_chars.saturating_sub(line_len + space_len) {
				line_len += space_len + remaining;
				break;
			}
			// Only an empty line reaches here: a full-width chunk is its own line.
			lines += 1;
			remaining -= max_chars;
			leading_space = false;
		}
	});
	lines + usize::from(line_len > 0)
}

fn take_chars(text: &str, count: usize) -> String {
	text.chars().take(count).collect()
}

fn skip_chars(text: &str, count: usize) -> String {
	text.chars().skip(count).collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::svp::markdown_blocks::{MarkdownBlock, parse_markdown_blocks};

	fn inline_text(text: &str) -> MarkdownInlineText {
		let mut inline = MarkdownInlineText::new();
		inline.push(MarkdownInlineStyle::TEXT, text);
		inline
	}

	fn line_texts(lines: &[Vec<MarkdownInlineWord>]) -> Vec<String> {
		lines
			.iter()
			.map(|line| {
				line.iter()
					.enumerate()
					.map(|(idx, word)| {
						if idx > 0 && word.leading_space {
							format!(" {}", word.text)
						} else {
							word.text.clone()
						}
					})
					.collect::<String>()
			})
			.collect()
	}

	#[test]
	fn table_columns_allocate_width_from_content_demand() {
		let rows = vec![
			vec![inline_text("ID"), inline_text("Description")],
			vec![
				inline_text("1"),
				inline_text("This column needs substantially more room than the ID"),
			],
		];
		let (widths, gutter) = table_column_widths(&rows, 60);

		assert_eq!(gutter, 2);
		assert_eq!(widths.iter().sum::<usize>() + gutter, 60);
		assert!(widths[1] >= widths[0].saturating_mul(2));
		assert!(widths[0] >= 4);
	}

	#[test]
	fn table_columns_stay_balanced_for_similar_content() {
		let rows = vec![vec![inline_text("left"), inline_text("rght")]];
		let (widths, gutter) = table_column_widths(&rows, 60);

		assert_eq!(widths, vec![29, 29]);
		assert_eq!(gutter, 2);
	}

	#[test]
	fn table_columns_bound_outliers_and_compress_to_available_width() {
		let rows = vec![vec![
			inline_text("customer key"),
			inline_text("order status"),
			inline_text("ship date"),
			inline_text(&"comment ".repeat(100)),
		]];
		let (widths, gutter) = table_column_widths(&rows, 80);

		assert_eq!(widths.iter().sum::<usize>() + gutter * 3, 80);
		assert!(widths.iter().all(|width| *width >= 4));
		assert!(widths[3] > widths[0]);
		assert!(widths[0] > 4, "the outlier must not starve other columns");
	}

	#[test]
	fn inline_words_preserve_adjacent_span_boundaries_without_extra_spaces() {
		let mut text = MarkdownInlineText::new();
		text.push(MarkdownInlineStyle::TEXT, "a");
		text.push(MarkdownInlineStyle::CODE, "b");
		text.push(MarkdownInlineStyle::TEXT, " c");

		assert_eq!(
			inline_words(&text),
			vec![
				MarkdownInlineWord {
					style: MarkdownInlineStyle::TEXT,
					text: "a".to_string(),
					leading_space: false,
				},
				MarkdownInlineWord {
					style: MarkdownInlineStyle::CODE,
					text: "b".to_string(),
					leading_space: false,
				},
				MarkdownInlineWord {
					style: MarkdownInlineStyle::TEXT,
					text: "c".to_string(),
					leading_space: true,
				}
			]
		);
	}

	#[test]
	fn wrapped_inline_words_flow_and_break_unbreakable_words() {
		let mut text = MarkdownInlineText::new();
		text.push(MarkdownInlineStyle::TEXT, "alpha beta ");
		text.push(MarkdownInlineStyle::CODE, "gammadeltaepsilon");
		let lines = wrap_inline_words(&text, 10);
		assert_eq!(
			line_texts(&lines),
			vec!["alpha beta", "gammadelta", "epsilon"]
		);
		assert_eq!(lines[1][0].style, MarkdownInlineStyle::CODE);
		assert!(wrap_inline_words(&MarkdownInlineText::new(), 10).is_empty());
		// A one-character column still progresses one character per line.
		assert_eq!(line_texts(&wrap_inline_words(&text, 0)).len(), 26);
	}

	/// The allocator before content fitting, kept as the corpus probe's baseline.
	fn table_column_widths_previous(
		rows: &[Vec<MarkdownInlineText>],
		available_chars: usize,
	) -> (Vec<usize>, usize) {
		const MIN_CELL_WIDTH: usize = 4;

		let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
		if columns == 0 {
			return (Vec::new(), 0);
		}
		let available_chars = available_chars.max(1);
		let gutter: usize = if columns == 1 {
			0
		} else if available_chars >= columns.saturating_add((columns - 1).saturating_mul(2)) {
			2
		} else if available_chars >= columns.saturating_add(columns - 1) {
			1
		} else {
			0
		};
		let content_space = available_chars
			.saturating_sub(gutter.saturating_mul(columns.saturating_sub(1)))
			.max(columns);
		let min_width = MIN_CELL_WIDTH.min(content_space / columns).max(1);
		// A single outlier should influence the table without consuming nearly all
		// of it. Extra space is distributed separately after measuring demand.
		let demand_cap = content_space.div_ceil(2).max(min_width);
		let desired = (0..columns)
			.map(|column| {
				rows.iter()
					.filter_map(|row| row.get(column))
					.map(MarkdownInlineText::char_count)
					.max()
					.unwrap_or(0)
					.max(min_width)
					.min(demand_cap)
			})
			.collect::<Vec<_>>();
		let desired_total = desired.iter().copied().sum::<usize>();

		if desired_total <= content_space {
			let mut widths = desired.clone();
			let extra = content_space - desired_total;
			let shared = extra / columns;
			for width in &mut widths {
				*width = width.saturating_add(shared);
			}
			let mut order = (0..columns).collect::<Vec<_>>();
			order.sort_unstable_by_key(|&column| std::cmp::Reverse(desired[column]));
			for &column in order.iter().take(extra % columns) {
				widths[column] = widths[column].saturating_add(1);
			}
			return (widths, gutter);
		}

		let mut widths = vec![min_width; columns];
		let remaining = content_space.saturating_sub(min_width.saturating_mul(columns));
		let weights = desired
			.iter()
			.map(|width| width.saturating_sub(min_width))
			.collect::<Vec<_>>();
		let weight_total = weights.iter().copied().sum::<usize>();
		if remaining == 0 || weight_total == 0 {
			return (widths, gutter);
		}

		let mut assigned = 0usize;
		let mut remainders = Vec::with_capacity(columns);
		for (column, &weight) in weights.iter().enumerate() {
			let scaled = (remaining as u128) * (weight as u128);
			let share = (scaled / weight_total as u128) as usize;
			widths[column] = widths[column].saturating_add(share);
			assigned = assigned.saturating_add(share);
			remainders.push((scaled % weight_total as u128, desired[column], column));
		}
		remainders.sort_unstable_by_key(|&(remainder, demand, _)| {
			(std::cmp::Reverse(remainder), std::cmp::Reverse(demand))
		});
		for &(_, _, column) in remainders.iter().take(remaining.saturating_sub(assigned)) {
			widths[column] = widths[column].saturating_add(1);
		}

		(widths, gutter)
	}

	const ATOMIC_OPS_TABLE: &str = "| Atomic op | Returns | Does |\n|---|---|---|\n| `update(cells, id, f)` | `Ok(result)` or `Err(reason)` | Applies `f` to the named cells at one instant, or changes nothing if `f` refuses |\n| `merge(cell, id, delta)` | nothing | Folds a commutative delta into one cell; never refuses |\n| `read(cells, at)` | states | Returns one or many cells as of one version; latest by default |\n| `await(cell, pred)` | state | Returns once `pred` holds for the cell |\n| `send` (returned by `f`) | n/a | Delivers a message exactly once, as an `update` or `merge` at another cell |\n";

	fn table_rows(source: &str) -> Vec<Vec<MarkdownInlineText>> {
		parse_markdown_blocks(source)
			.into_iter()
			.find_map(|block| match block {
				MarkdownBlock::Table { rows, .. } => Some(rows),
				_ => None,
			})
			.expect("a table")
	}

	#[test]
	fn short_columns_stay_whole_and_the_long_prose_column_wraps() {
		// The operator's example: the two short columns read best unwrapped,
		// and the long "Does" column is the one that should wrap. At the
		// width of the operator's screenshot (about 94 columns) the previous
		// allocator gave "Returns" 24 characters for a 25-character cell.
		let rows = table_rows(ATOMIC_OPS_TABLE);
		let height = |widths: &[usize]| score_table_widths(&rows, widths).height_lines;
		for available in 60..=200 {
			let (widths, gutter) = table_column_widths(&rows, available);
			assert_eq!(
				widths.iter().sum::<usize>() + gutter * 2,
				available,
				"the table uses the pane at {available}"
			);
			// Keeping a short column whole is worth at most one line.
			let previous = table_column_widths_previous(&rows, available).0;
			assert!(
				height(&widths) <= height(&previous) + 1,
				"{available}: {widths:?} vs previous {previous:?}"
			);
			let longest_does = rows.iter().map(|row| row[2].char_count()).max().unwrap();
			if available >= 88 {
				if widths[2] < longest_does {
					assert_eq!(&widths[..2], &[22, 25], "only Does wraps at {available}");
				}
				for row in &rows {
					for column in 0..2 {
						assert_eq!(wrap_inline_words(&row[column], widths[column]).len(), 1);
					}
				}
			}
		}
	}

	/// One 30-character identifier in the first column's first row, beside
	/// `prose_rows` rows of 39-character prose: 40 content characters.
	fn identifier_beside_prose(prose_rows: usize) -> Vec<Vec<MarkdownInlineText>> {
		let prose = "abcd ".repeat(8).trim_end().to_string();
		let mut rows = vec![vec![
			inline_text(&format!("{} 123456789", "x".repeat(30))),
			inline_text(&prose),
		]];
		for _ in 1..prose_rows {
			rows.push(vec![inline_text(""), inline_text(&prose)]);
		}
		rows
	}

	#[test]
	fn a_token_is_kept_whole_when_its_neighbours_do_not_need_the_room() {
		// The same 30-character identifier beside 25 rows of short cells:
		// they are whole at 9 characters, and the identifier keeps the rest.
		let mut rows = vec![vec![
			inline_text(&format!("{} 123456789", "x".repeat(30))),
			inline_text("word word"),
		]];
		for _ in 1..25 {
			rows.push(vec![inline_text(""), inline_text("word word")]);
		}
		assert_eq!(table_column_widths(&rows, 42), (vec![31, 9], 2));
	}

	#[test]
	fn a_token_breaks_when_keeping_it_whole_would_double_the_table() {
		// Codex's example (longest tokens [30, 5], volumes [40, 1000]):
		// keeping the identifier whole makes every one of 25 prose rows four
		// lines tall (100 lines); breaking it once makes them two.
		let rows = identifier_beside_prose(25);
		let (widths, gutter) = table_column_widths(&rows, 42);
		assert_eq!(widths.iter().sum::<usize>() + gutter, 42);
		assert!(widths[0] < 30, "{widths:?}");
		let height = score_table_widths(&rows, &widths).height_lines;
		let protected = score_table_widths(&rows, &[30, 10]).height_lines;
		assert!(height * 3 < protected * 2, "{height} vs {protected}");
	}

	#[test]
	fn a_giant_token_breaks_rather_than_starving_its_neighbours() {
		let rows = vec![vec![
			inline_text(&format!("https://example.com/{}", "a".repeat(200))),
			inline_text(&"word ".repeat(40)),
		]];
		let (widths, gutter) = table_column_widths(&rows, 62);
		assert_eq!(widths.iter().sum::<usize>() + gutter, 62);
		assert!(
			widths[1] >= 20,
			"the prose column keeps a fair share: {widths:?}"
		);
	}

	#[test]
	fn tiny_panes_keep_every_column_one_character_wide() {
		let rows = vec![vec![
			inline_text("alpha"),
			inline_text("beta"),
			inline_text("gamma"),
		]];
		for available in [0, 1, 2] {
			assert_eq!(table_column_widths(&rows, available), (vec![1, 1, 1], 0));
		}
		// Gutters shrink before columns do.
		assert_eq!(table_column_widths(&rows, 7).1, 2);
		assert_eq!(table_column_widths(&rows, 6).1, 1);
		assert_eq!(table_column_widths(&rows, 5).1, 1);
		assert_eq!(table_column_widths(&rows, 4).1, 0);
		for available in 3..=40 {
			let (widths, gutter) = table_column_widths(&rows, available);
			assert!(widths.iter().all(|&width| width >= 1));
			assert_eq!(widths.iter().sum::<usize>() + gutter * 2, available);
		}
	}

	#[test]
	fn inline_line_count_matches_the_wrapped_lines() {
		let mut text = MarkdownInlineText::new();
		text.push(MarkdownInlineStyle::TEXT, "alpha beta ");
		text.push(MarkdownInlineStyle::CODE, "gammadeltaepsilon");
		text.push(MarkdownInlineStyle::TEXT, "zeta ");
		text.push(MarkdownInlineStyle::CODE, " eta");
		text.push(MarkdownInlineStyle::TEXT, "  theta   iota");
		for width in 0..=40 {
			assert_eq!(
				inline_line_count(&text, width),
				wrap_inline_words(&text, width).len(),
				"width {width}"
			);
		}
		assert_eq!(inline_line_count(&MarkdownInlineText::new(), 10), 0);
	}

	#[test]
	fn a_wider_gutter_can_cost_more_on_a_tiny_pane() {
		// The fit never shrinks a column when the content space grows, so the
		// chosen cost cannot rise with it; but the gutter widens with the pane
		// (0, then 1, then 2 characters) and takes content space away when it
		// does. That only happens below `columns + 2 * (columns - 1)`.
		let rows = vec![
			vec![inline_text("aaa"), inline_text(""), inline_text("")],
			vec![inline_text(""), inline_text("aaa"), inline_text("")],
			vec![inline_text(""), inline_text(""), inline_text("aaa")],
		];
		let all = [0, 1, 2];
		let cost = |available: usize| {
			let (widths, gutter) = table_column_widths(&rows, available);
			(
				widths.clone(),
				gutter,
				table_wrap_cost(&rows, &widths, &all, |_| false),
			)
		};
		assert_eq!(cost(4), (vec![2, 1, 1], 0, 13));
		assert_eq!(cost(5), (vec![1, 1, 1], 1, 15));
		let mut previous = cost(7).2;
		for available in 8..=40 {
			let current = cost(available).2;
			assert!(current <= previous, "cost rose at {available}");
			previous = current;
		}
	}

	/// Allocation time on wide, long tables, in whatever profile this runs.
	/// Evidence, not a gate.
	#[test]
	fn content_widths_are_the_column_widths_without_their_gutters() {
		let tables = [
			table_rows(ATOMIC_OPS_TABLE),
			identifier_beside_prose(3),
			identifier_beside_prose(12),
		];
		for rows in &tables {
			let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
			for available in 0..=200 {
				let (widths, gutter) = table_column_widths(rows, available);
				let content = available.max(1).saturating_sub(gutter * (columns - 1));
				assert_eq!(
					table_content_widths(rows, content),
					widths,
					"at {available}"
				);
			}
		}
	}

	#[test]
	fn content_widths_hold_at_zero_and_tiny_budgets() {
		let rows = table_rows(ATOMIC_OPS_TABLE);
		for content in 0..=3 {
			assert_eq!(
				table_content_widths(&rows, content),
				vec![1, 1, 1],
				"at {content}"
			);
		}
		let at_five = table_content_widths(&rows, 5);
		assert_eq!(at_five.len(), 3);
		assert_eq!(at_five.iter().sum::<usize>(), 5);
		assert!(at_five.iter().all(|&width| width >= 1));
		assert!(table_content_widths(&[], 80).is_empty());
		assert!(table_content_widths(&[Vec::new(), Vec::new()], 80).is_empty());
		assert_eq!(table_column_widths(&[], 80), (Vec::new(), 0));
	}

	#[test]
	fn missing_cells_count_as_empty() {
		// Ragged rows: the widest row sets the columns; a missing cell asks
		// for nothing, exactly as an empty one does.
		let ragged = vec![
			vec![
				inline_text("name"),
				inline_text("status"),
				inline_text("note"),
			],
			vec![inline_text("alpha")],
			vec![inline_text("beta"), inline_text("ok")],
		];
		let padded = vec![
			vec![
				inline_text("name"),
				inline_text("status"),
				inline_text("note"),
			],
			vec![inline_text("alpha"), inline_text(""), inline_text("")],
			vec![inline_text("beta"), inline_text("ok"), inline_text("")],
		];
		for content in 0..=40 {
			let widths = table_content_widths(&ragged, content);
			assert_eq!(widths.len(), 3);
			assert_eq!(
				widths,
				table_content_widths(&padded, content),
				"at {content}"
			);
			assert_eq!(
				table_column_widths(&ragged, content),
				table_column_widths(&padded, content)
			);
		}
	}

	#[test]
	#[ignore = "timing probe"]
	fn table_width_stress_probe() {
		for (columns, rows, cell_chars) in
			[(8, 200, 120), (16, 200, 200), (32, 100, 200), (48, 50, 400)]
		{
			let cell = |row: usize, column: usize| {
				let mut text = String::new();
				let mut index = 0;
				while text.len() < cell_chars {
					text.push_str(&format!(
						"w{}x{} ",
						(row * 7 + column * 3 + index) % 97,
						index % 13
					));
					index += 1;
				}
				inline_text(text.trim_end())
			};
			let table = (0..rows)
				.map(|row| {
					(0..columns)
						.map(|column| cell(row, column))
						.collect::<Vec<_>>()
				})
				.collect::<Vec<_>>();
			for available in [80usize, 140, 240] {
				let started = std::time::Instant::now();
				let (widths, _) = table_column_widths(&table, available);
				let allocation = started.elapsed();
				let started = std::time::Instant::now();
				let lines = table_height_by_count(&table, &widths);
				let measure = started.elapsed();
				println!(
					"STRESS columns={columns} rows={rows} cell_chars={cell_chars} available={available} allocate={:.2}ms measure={:.2}ms height={lines}",
					allocation.as_secs_f64() * 1000.0,
					measure.as_secs_f64() * 1000.0,
				);
			}
		}
	}

	fn table_height_by_count(rows: &[Vec<MarkdownInlineText>], widths: &[usize]) -> usize {
		rows.iter()
			.map(|row| {
				row.iter()
					.zip(widths)
					.map(|(cell, &width)| inline_line_count(cell, width))
					.max()
					.unwrap_or(0)
					.max(1)
			})
			.sum()
	}

	/// What one column-width choice costs one table.
	#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
	struct TableWidthScore {
		/// Rendered rows of text with wrap on: per row, its tallest cell.
		height_lines: usize,
		/// Cells of at most 16 characters that wrap.
		short_cell_wraps: usize,
		/// Visible tokens (no whitespace, across style boundaries) longer
		/// than their column, and the forced breaks inside them.
		broken_tokens: usize,
		token_breaks: usize,
		/// With wrap off: cells cut short, and the characters lost.
		clipped_cells: usize,
		clipped_chars: usize,
	}

	impl std::ops::AddAssign for TableWidthScore {
		fn add_assign(&mut self, other: Self) {
			self.height_lines += other.height_lines;
			self.short_cell_wraps += other.short_cell_wraps;
			self.broken_tokens += other.broken_tokens;
			self.token_breaks += other.token_breaks;
			self.clipped_cells += other.clipped_cells;
			self.clipped_chars += other.clipped_chars;
		}
	}

	type WidthAlgorithm = fn(&[Vec<MarkdownInlineText>], usize) -> (Vec<usize>, usize);

	fn visible_token_lengths(cell: &MarkdownInlineText) -> Vec<usize> {
		let mut tokens = Vec::new();
		let mut token = 0usize;
		for ch in cell.spans().iter().flat_map(|span| span.text.chars()) {
			if ch.is_whitespace() {
				if token > 0 {
					tokens.push(token);
				}
				token = 0;
			} else {
				token += 1;
			}
		}
		if token > 0 {
			tokens.push(token);
		}
		tokens
	}

	fn score_table_widths(rows: &[Vec<MarkdownInlineText>], widths: &[usize]) -> TableWidthScore {
		let mut score = TableWidthScore::default();
		for row in rows {
			let mut row_lines = 1;
			for (cell, &width) in row.iter().zip(widths) {
				let chars = cell.char_count();
				let lines = wrap_inline_words(cell, width).len();
				row_lines = row_lines.max(lines);
				if chars <= 16 && lines > 1 {
					score.short_cell_wraps += 1;
				}
				for token in visible_token_lengths(cell) {
					if token > width {
						score.broken_tokens += 1;
						score.token_breaks += token.div_ceil(width.max(1)) - 1;
					}
				}
				if chars > width {
					score.clipped_cells += 1;
					score.clipped_chars += chars - width.saturating_sub(1);
				}
			}
			score.height_lines += row_lines;
		}
		score
	}

	fn percentile(sorted: &[u128], fraction: f64) -> u128 {
		if sorted.is_empty() {
			return 0;
		}
		sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
	}

	/// Scores table column-width algorithms over every Markdown table in the
	/// files listed (one path per line) in `BARYON_TABLE_CORPUS`, against the
	/// first algorithm as baseline. Evidence, not a gate: run it by hand and
	/// keep its output out of git. Allocation is timed alone (the scoring is
	/// not), per table, in whatever profile the probe was built with.
	#[test]
	#[ignore = "corpus probe; needs BARYON_TABLE_CORPUS"]
	fn table_width_corpus_probe() {
		let list =
			std::env::var("BARYON_TABLE_CORPUS").expect("BARYON_TABLE_CORPUS lists the files");
		let paths = std::fs::read_to_string(&list)
			.expect("corpus list")
			.lines()
			.map(str::to_string)
			.collect::<Vec<_>>();
		let mut unreadable = Vec::new();
		let mut tables = Vec::new();
		for (file, path) in paths.iter().enumerate() {
			let Ok(source) = std::fs::read_to_string(path) else {
				unreadable.push(path.clone());
				continue;
			};
			let mut index = 0;
			for block in parse_markdown_blocks(&source) {
				if let MarkdownBlock::Table { rows, .. } = block
					&& !rows.is_empty()
				{
					tables.push((file, index, rows));
					index += 1;
				}
			}
		}
		println!(
			"files={} unreadable={} {unreadable:?} tables={}",
			paths.len(),
			unreadable.len(),
			tables.len()
		);
		let algorithms: &[(&str, WidthAlgorithm)] = &[
			("previous", table_column_widths_previous),
			("fitted", table_column_widths),
		];
		for (name, algorithm) in algorithms {
			// One more character of pane should never make a table taller.
			let mut steps = 0usize;
			let mut taller = 0usize;
			let mut costlier = 0usize;
			let mut worst = (0usize, String::new());
			for (file, index, rows) in &tables {
				let height = |available: usize| {
					score_table_widths(rows, &algorithm(rows, available).0).height_lines
				};
				// The allocator's own objective, with every column open (a
				// column kept whole neither wraps nor breaks a token).
				let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
				let all = (0..columns).collect::<Vec<_>>();
				let widest = column_demands(rows, columns)
					.iter()
					.map(|demand| demand.longest_cell.max(1))
					.max()
					.unwrap_or(1);
				let longest = column_demands(rows, columns);
				let cost = |available: usize| {
					table_wrap_cost(rows, &algorithm(rows, available).0, &all, |column| {
						longest[column].longest_cell.max(1) * 2 <= widest
					})
				};
				let mut cost_before = cost(60);
				for available in 61..=140 {
					let cost_after = cost(available);
					if cost_after > cost_before {
						costlier += 1;
					}
					cost_before = cost_after;
				}
				let mut before = height(60);
				for available in 61..=140 {
					let after = height(available);
					steps += 1;
					if after > before {
						taller += 1;
						if after - before > worst.0 {
							worst = (
								after - before,
								format!("{}#table{index} at {available}", paths[*file]),
							);
						}
					}
					before = after;
				}
			}
			println!(
				"monotone {name:<8} widening steps={steps} taller={taller} costlier={costlier} worst=+{} {}",
				worst.0, worst.1
			);
		}
		for available in [60usize, 80, 100, 140] {
			let mut baseline = Vec::new();
			for (position, (name, algorithm)) in algorithms.iter().enumerate() {
				let mut total = TableWidthScore::default();
				let mut nanos = Vec::with_capacity(tables.len());
				let mut per_file = vec![0u128; paths.len()];
				let mut scores = Vec::with_capacity(tables.len());
				if position == 0 {
					// The counter that layout and the cost use must break
					// exactly as the drawn lines do, on every corpus cell.
					for (_, _, rows) in &tables {
						for cell in rows.iter().flatten() {
							for width in [1, 2, 3, 5, 8, 13, 21, 34, available] {
								assert_eq!(
									inline_line_count(cell, width),
									wrap_inline_words(cell, width).len()
								);
							}
						}
					}
					// The gap-free entry point is the gridded one minus its gutters.
					for (_, _, rows) in &tables {
						let (widths, gutter) = table_column_widths(rows, available);
						let columns = widths.len();
						let content = available.saturating_sub(gutter * columns.saturating_sub(1));
						assert_eq!(table_content_widths(rows, content), widths);
					}
				}
				for (file, _, rows) in &tables {
					let started = std::time::Instant::now();
					let (widths, gutter) = algorithm(rows, available);
					let elapsed = started.elapsed().as_nanos();
					nanos.push(elapsed);
					per_file[*file] += elapsed;
					let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
					assert_eq!(widths.len(), columns, "{name}");
					assert!(widths.iter().all(|&width| width >= 1), "{name}: {widths:?}");
					assert!(
						widths.iter().sum::<usize>() + gutter * columns.saturating_sub(1)
							<= available.max(columns),
						"{name} overflows {available}: {widths:?} gutter {gutter}"
					);
					let score = score_table_widths(rows, &widths);
					total += score;
					scores.push(score);
				}
				nanos.sort_unstable();
				let (largest_file, largest_nanos) = per_file
					.iter()
					.enumerate()
					.max_by_key(|(_, nanos)| **nanos)
					.map(|(file, nanos)| (paths[file].as_str(), *nanos))
					.unwrap_or(("", 0));
				println!(
					"width={available:>3} {name:<8} height={:>6} short_wraps={:>5} broken_tokens={:>5} token_breaks={:>6} clipped_cells={:>5} clipped_chars={:>7} alloc_us p50={:.1} p95={:.1} max={:.1} largest_doc={:.1}us {}",
					total.height_lines,
					total.short_cell_wraps,
					total.broken_tokens,
					total.token_breaks,
					total.clipped_cells,
					total.clipped_chars,
					percentile(&nanos, 0.5) as f64 / 1000.0,
					percentile(&nanos, 0.95) as f64 / 1000.0,
					nanos.last().copied().unwrap_or(0) as f64 / 1000.0,
					largest_nanos as f64 / 1000.0,
					largest_file.rsplit('/').next().unwrap_or(largest_file),
				);
				if position == 0 {
					baseline = scores;
					continue;
				}
				type Metric = fn(&TableWidthScore) -> usize;
				let metrics: [(&str, Metric); 4] = [
					("height", |score| score.height_lines),
					("short_wraps", |score| score.short_cell_wraps),
					("token_breaks", |score| score.token_breaks),
					("clipped_chars", |score| score.clipped_chars),
				];
				for (metric, value) in metrics {
					let (mut better, mut equal, mut worse) = (0, 0, 0);
					let mut worst = (0usize, None);
					for (table, (before, after)) in baseline.iter().zip(&scores).enumerate() {
						let (before, after) = (value(before), value(after));
						match after.cmp(&before) {
							std::cmp::Ordering::Less => better += 1,
							std::cmp::Ordering::Equal => equal += 1,
							std::cmp::Ordering::Greater => {
								worse += 1;
								if after - before > worst.0 {
									worst = (after - before, Some(table));
								}
							}
						}
					}
					let worst_at = worst.1.map(|table| {
						let (file, index, _) = &tables[table];
						format!("+{} at {}#table{index}", worst.0, paths[*file])
					});
					println!(
						"           {metric:<13} vs {}: better={better:>4} equal={equal:>4} worse={worse:>4} worst={}",
						algorithms[0].0,
						worst_at.unwrap_or_else(|| "-".to_string()),
					);
				}
			}
		}
	}
}
