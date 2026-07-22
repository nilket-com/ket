use std::sync::Arc;

use crate::core::DocByte;
use crate::engine::language::{LanguageProvider, SemanticOutput};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};

pub struct FsProvider;

impl LanguageProvider for FsProvider {
	fn language_id(&self) -> &'static str {
		"fs"
	}

	fn label(&self) -> &'static str {
		"Files"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&[]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		highlight_fs(viewport_bytes, offset_map)
	}

	fn semantic_reactor(
		&self,
		_full_source: Arc<String>,
		_global_offset: DocByte,
		_file_path: &str,
	) -> SemanticOutput {
		SemanticOutput::default()
	}
}

fn global_offset(local: usize, offset_map: &[(usize, DocByte)]) -> DocByte {
	let index = offset_map.partition_point(|(start, _)| *start <= local);
	let (base_local, base_global) = offset_map
		.get(index.saturating_sub(1))
		.copied()
		.unwrap_or((0, DocByte::ZERO));
	base_global.saturating_add(local.saturating_sub(base_local) as u64)
}

fn push_span(
	out: &mut Vec<HighlightSpan>,
	local_start: usize,
	local_end: usize,
	category: TokenCategory,
	offset_map: &[(usize, DocByte)],
) {
	if local_start >= local_end {
		return;
	}
	out.push(HighlightSpan::new(
		global_offset(local_start, offset_map),
		global_offset(local_end, offset_map),
		category,
	));
}

fn highlight_fs(viewport_bytes: &[u8], offset_map: &[(usize, DocByte)]) -> Vec<HighlightSpan> {
	let mut out = Vec::new();
	let mut line_start = 0usize;

	while line_start <= viewport_bytes.len() {
		let line_end = viewport_bytes[line_start..]
			.iter()
			.position(|&byte| byte == b'\n')
			.map(|offset| line_start + offset)
			.unwrap_or(viewport_bytes.len());
		let line = &viewport_bytes[line_start..line_end];
		let indent = line
			.iter()
			.position(|&byte| byte != b' ')
			.unwrap_or(line.len());
		if indent < line.len() {
			match &line[indent..] {
				rest if rest.starts_with("▶ ".as_bytes()) || rest.starts_with("▼ ".as_bytes()) =>
				{
					push_span(
						&mut out,
						line_start + indent,
						line_start + indent + "▶".len(),
						TokenCategory::Punctuation,
						offset_map,
					);
					let name_start = line_start + indent + "▶ ".len();
					let name_end = line_end;
					push_span(
						&mut out,
						name_start,
						name_end,
						TokenCategory::Module,
						offset_map,
					);
				}
				_ if indent >= 2 => {
					push_span(
						&mut out,
						line_start + indent,
						line_end,
						TokenCategory::Variable,
						offset_map,
					);
				}
				_ => {}
			}
		}

		if line_end == viewport_bytes.len() {
			break;
		}
		line_start = line_end + 1;
	}

	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lexical_baseline_highlights_directory_rows() {
		let provider = FsProvider;
		let bytes = "▶ src/\n  main.rs".as_bytes();
		let spans = provider.lexical_baseline(bytes, &[(0, DocByte::ZERO)]);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Punctuation
					&& span.start == DocByte::ZERO)
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Module)
		);
		assert!(
			spans
				.iter()
				.any(|span| span.category == TokenCategory::Variable)
		);
	}
}
