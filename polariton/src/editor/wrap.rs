use crate::editor::coords::{CharIdx, LineIdx};

#[derive(Debug, Clone, Copy)]
pub struct VisualLine {
	pub doc_line: LineIdx,
	/// Character offset within the document line where this visual line starts.
	pub col_start: CharIdx,
	/// Character offset within the document line where this visual line ends (exclusive).
	pub col_end: CharIdx,
	/// Whether this is the first visual line of the doc line (for line number display).
	pub is_first: bool,
}

/// Configuration for line wrapping.
#[derive(Debug, Clone, Copy)]
pub struct WrapConfig {
	pub enabled: bool,
	/// Maximum number of columns before wrapping.
	pub wrap_col: CharIdx,
}

impl Default for WrapConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			wrap_col: CharIdx(120),
		}
	}
}

/// Compute visual lines for the entire document.
pub fn compute_visual_lines(
	line_count: LineIdx,
	line_text: &dyn Fn(LineIdx) -> String,
	is_hidden: &dyn Fn(LineIdx) -> bool,
	config: &WrapConfig,
) -> Vec<VisualLine> {
	let mut visual = Vec::new();

	for doc_line_idx in 0..*line_count {
		let doc_line = LineIdx(doc_line_idx);
		if is_hidden(doc_line) {
			continue;
		}

		let text = line_text(doc_line);
		let chars: Vec<char> = text.chars().collect();
		let line_len = CharIdx(chars.len());
		if !config.enabled || line_len <= config.wrap_col {
			visual.push(VisualLine {
				doc_line,
				col_start: CharIdx(0),
				col_end: line_len,
				is_first: true,
			});
		} else {
			// Wrap at word boundaries when possible
			let mut col = CharIdx(0);
			let mut first = true;
			while col < line_len {
				let remaining = *line_len - *col;
				let chunk_end = if remaining <= *config.wrap_col {
					line_len
				} else {
					// Try to find a good break point (space, comma, paren)
					let max_end = *col + *config.wrap_col;
					CharIdx(
						( *col..max_end )
							.rev()
							.find(|&idx| matches!(chars[idx], ' ' | ',' | '(' | ')'))
							.map(|idx| idx + 1)
							.unwrap_or(max_end)
					)
				};

				visual.push(VisualLine {
					doc_line,
					col_start: col,
					col_end: chunk_end,
					is_first: first,
				});
				first = false;
				col = chunk_end;
			}
			// Handle empty lines
			if *col == 0 {
				visual.push(VisualLine {
					doc_line,
					col_start: CharIdx(0),
					col_end: CharIdx(0),
					is_first: true,
				});
			}
		}
	}

	visual
}
