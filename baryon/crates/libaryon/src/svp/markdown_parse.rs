use crate::core::DocByte;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkdownTokenKind {
	HeadingMarker,
	HeadingText,
	Fence,
	FenceInfo,
	CodeBlockText,
	InlineCode,
	LinkLabel,
	LinkDestination,
	ListMarker,
	BlockQuoteMarker,
	HorizontalRule,
	HtmlComment,
	Emphasis,
	Punctuation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkdownToken {
	pub kind: MarkdownTokenKind,
	pub start: usize,
	pub end: usize,
}

#[derive(Debug, Clone, Copy)]
struct FenceState {
	marker: u8,
	len: usize,
}

pub fn tokenize(src: &[u8]) -> Vec<MarkdownToken> {
	let mut out = Vec::new();
	let mut line_start = 0usize;
	let mut fence: Option<FenceState> = None;
	let mut html_comment = false;

	while line_start <= src.len() {
		let line_end = src[line_start..]
			.iter()
			.position(|&byte| byte == b'\n')
			.map(|offset| line_start + offset)
			.unwrap_or(src.len());

		scan_line(
			src,
			line_start,
			line_end,
			&mut fence,
			&mut html_comment,
			&mut out,
		);

		if line_end == src.len() {
			break;
		}
		line_start = line_end + 1;
	}

	out
}

fn scan_line(
	src: &[u8],
	line_start: usize,
	line_end: usize,
	fence: &mut Option<FenceState>,
	html_comment: &mut bool,
	out: &mut Vec<MarkdownToken>,
) {
	if line_start >= line_end {
		return;
	}

	if *html_comment {
		scan_inline(src, line_start, line_end, out, html_comment);
		return;
	}

	if let Some(state) = *fence {
		if let Some((marker_start, marker_end, None)) =
			fence_at_line_start(src, line_start, line_end)
			&& src[marker_start] == state.marker
			&& marker_end.saturating_sub(marker_start) >= state.len
			&& trailing_space_only(src, marker_end, line_end)
		{
			push(out, MarkdownTokenKind::Fence, marker_start, marker_end);
			*fence = None;
			return;
		}

		push(out, MarkdownTokenKind::CodeBlockText, line_start, line_end);
		return;
	}

	if let Some((marker_start, marker_end, info_start)) =
		fence_at_line_start(src, line_start, line_end)
	{
		push(out, MarkdownTokenKind::Fence, marker_start, marker_end);
		if let Some(info_start) = info_start {
			let info_end = trim_ascii_end(src, info_start, line_end);
			push(out, MarkdownTokenKind::FenceInfo, info_start, info_end);
		}
		*fence = Some(FenceState {
			marker: src[marker_start],
			len: marker_end.saturating_sub(marker_start),
		});
		return;
	}

	let indent = leading_spaces(src, line_start, line_end);
	if indent <= 3 {
		let content_start = line_start + indent;
		if let Some((marker_end, text_start)) = heading_at(src, content_start, line_end) {
			push(
				out,
				MarkdownTokenKind::HeadingMarker,
				content_start,
				marker_end,
			);
			push(
				out,
				MarkdownTokenKind::HeadingText,
				text_start,
				trim_ascii_end(src, text_start, line_end),
			);
			scan_inline(src, text_start, line_end, out, html_comment);
			return;
		}

		if is_horizontal_rule(src, content_start, line_end) {
			push(
				out,
				MarkdownTokenKind::HorizontalRule,
				content_start,
				line_end,
			);
			return;
		}

		if content_start < line_end && src[content_start] == b'>' {
			let marker_end =
				if content_start + 1 < line_end && matches!(src[content_start + 1], b' ' | b'\t') {
					content_start + 2
				} else {
					content_start + 1
				};
			push(
				out,
				MarkdownTokenKind::BlockQuoteMarker,
				content_start,
				marker_end,
			);
		}
	}

	if let Some((marker_start, marker_end)) = list_marker(src, line_start, line_end) {
		push(out, MarkdownTokenKind::ListMarker, marker_start, marker_end);
	}

	scan_inline(src, line_start, line_end, out, html_comment);
}

fn scan_inline(
	src: &[u8],
	start: usize,
	end: usize,
	out: &mut Vec<MarkdownToken>,
	html_comment: &mut bool,
) {
	let mut pos = start;
	while pos < end {
		if *html_comment {
			let (comment_end, closed) = find_bytes(src, pos, end, b"-->")
				.map(|idx| (idx + 3, true))
				.unwrap_or((end, false));
			push(out, MarkdownTokenKind::HtmlComment, pos, comment_end);
			*html_comment = !closed;
			pos = comment_end;
			continue;
		}

		if starts_with(src, pos, end, b"<!--") {
			let (comment_end, closed) = find_bytes(src, pos + 4, end, b"-->")
				.map(|idx| (idx + 3, true))
				.unwrap_or((end, false));
			push(out, MarkdownTokenKind::HtmlComment, pos, comment_end);
			*html_comment = !closed;
			pos = comment_end;
			continue;
		}

		if src[pos] == b'`' {
			let tick_len = count_run(src, pos, end, b'`');
			let mut search = pos + tick_len;
			let mut close = None;
			while search < end {
				if src[search] == b'`' && count_run(src, search, end, b'`') >= tick_len {
					close = Some(search);
					break;
				}
				search += 1;
			}
			let token_end = close
				.map(|close| close + tick_len)
				.unwrap_or(pos + tick_len);
			push(out, MarkdownTokenKind::InlineCode, pos, token_end);
			pos = token_end;
			continue;
		}

		if src[pos] == b'!' && pos + 1 < end && src[pos + 1] == b'[' {
			if let Some(next) = scan_link(src, pos, end, true, out) {
				pos = next;
				continue;
			}
		} else if src[pos] == b'['
			&& let Some(next) = scan_link(src, pos, end, false, out)
		{
			pos = next;
			continue;
		}

		if matches!(src[pos], b'*' | b'_') {
			let marker = src[pos];
			let run = count_run(src, pos, end, marker).min(3);
			push(out, MarkdownTokenKind::Emphasis, pos, pos + run);
			pos += run;
			continue;
		}

		pos += 1;
	}
}

fn scan_link(
	src: &[u8],
	start: usize,
	end: usize,
	image: bool,
	out: &mut Vec<MarkdownToken>,
) -> Option<usize> {
	let bracket_start = if image { start + 1 } else { start };
	let label_start = bracket_start + 1;
	let label_end = find_byte(src, label_start, end, b']')?;
	if label_end + 1 >= end || src[label_end + 1] != b'(' {
		return None;
	}
	let destination_start = label_end + 2;
	let destination_end = find_byte(src, destination_start, end, b')')?;

	if image {
		push(out, MarkdownTokenKind::Punctuation, start, start + 1);
	}
	push(
		out,
		MarkdownTokenKind::Punctuation,
		bracket_start,
		bracket_start + 1,
	);
	push(out, MarkdownTokenKind::LinkLabel, label_start, label_end);
	push(
		out,
		MarkdownTokenKind::Punctuation,
		label_end,
		label_end + 2,
	);
	push(
		out,
		MarkdownTokenKind::LinkDestination,
		destination_start,
		destination_end,
	);
	push(
		out,
		MarkdownTokenKind::Punctuation,
		destination_end,
		destination_end + 1,
	);
	Some(destination_end + 1)
}

fn fence_at_line_start(
	src: &[u8],
	line_start: usize,
	line_end: usize,
) -> Option<(usize, usize, Option<usize>)> {
	let indent = leading_spaces(src, line_start, line_end);
	if indent > 3 {
		return None;
	}
	let marker_start = line_start + indent;
	if marker_start >= line_end || !matches!(src[marker_start], b'`' | b'~') {
		return None;
	}
	let marker = src[marker_start];
	let marker_len = count_run(src, marker_start, line_end, marker);
	if marker_len < 3 {
		return None;
	}
	let marker_end = marker_start + marker_len;
	let info_start = skip_ascii_space(src, marker_end, line_end);
	let info_start = if info_start < line_end {
		Some(info_start)
	} else {
		None
	};
	Some((marker_start, marker_end, info_start))
}

fn heading_at(src: &[u8], start: usize, end: usize) -> Option<(usize, usize)> {
	if start >= end || src[start] != b'#' {
		return None;
	}
	let marker_len = count_run(src, start, end, b'#');
	if marker_len == 0 || marker_len > 6 {
		return None;
	}
	let marker_end = start + marker_len;
	if marker_end < end && !matches!(src[marker_end], b' ' | b'\t') {
		return None;
	}
	Some((marker_end, skip_ascii_space(src, marker_end, end)))
}

fn list_marker(src: &[u8], line_start: usize, line_end: usize) -> Option<(usize, usize)> {
	let indent = leading_spaces(src, line_start, line_end);
	let marker_start = line_start + indent;
	if marker_start >= line_end {
		return None;
	}

	match src[marker_start] {
		b'-' | b'*' | b'+' => {
			let marker_end = marker_start + 1;
			if marker_end == line_end || matches!(src[marker_end], b' ' | b'\t') {
				Some((marker_start, marker_end))
			} else {
				None
			}
		}
		b'0'..=b'9' => {
			let mut pos = marker_start;
			while pos < line_end && src[pos].is_ascii_digit() {
				pos += 1;
			}
			if pos < line_end && matches!(src[pos], b'.' | b')') {
				let marker_end = pos + 1;
				if marker_end == line_end || matches!(src[marker_end], b' ' | b'\t') {
					return Some((marker_start, marker_end));
				}
			}
			None
		}
		_ => None,
	}
}

fn is_horizontal_rule(src: &[u8], start: usize, end: usize) -> bool {
	let mut marker = None;
	let mut count = 0usize;
	let mut pos = start;
	while pos < end {
		let byte = src[pos];
		if matches!(byte, b' ' | b'\t') {
			pos += 1;
			continue;
		}
		if !matches!(byte, b'-' | b'*' | b'_') {
			return false;
		}
		match marker {
			Some(existing) if existing != byte => return false,
			None => marker = Some(byte),
			_ => {}
		}
		count += 1;
		pos += 1;
	}
	count >= 3
}

fn push(out: &mut Vec<MarkdownToken>, kind: MarkdownTokenKind, start: usize, end: usize) {
	if start < end {
		out.push(MarkdownToken { kind, start, end });
	}
}

fn starts_with(src: &[u8], start: usize, end: usize, needle: &[u8]) -> bool {
	start + needle.len() <= end && &src[start..start + needle.len()] == needle
}

fn find_byte(src: &[u8], start: usize, end: usize, needle: u8) -> Option<usize> {
	src[start..end]
		.iter()
		.position(|&byte| byte == needle)
		.map(|offset| start + offset)
}

fn find_bytes(src: &[u8], start: usize, end: usize, needle: &[u8]) -> Option<usize> {
	if needle.is_empty() || start >= end || needle.len() > end.saturating_sub(start) {
		return None;
	}
	(start..=end - needle.len()).find(|&idx| &src[idx..idx + needle.len()] == needle)
}

fn count_run(src: &[u8], start: usize, end: usize, marker: u8) -> usize {
	let mut pos = start;
	while pos < end && src[pos] == marker {
		pos += 1;
	}
	pos - start
}

fn leading_spaces(src: &[u8], start: usize, end: usize) -> usize {
	let mut pos = start;
	while pos < end && src[pos] == b' ' {
		pos += 1;
	}
	pos - start
}

fn skip_ascii_space(src: &[u8], start: usize, end: usize) -> usize {
	let mut pos = start;
	while pos < end && matches!(src[pos], b' ' | b'\t') {
		pos += 1;
	}
	pos
}

fn trim_ascii_end(src: &[u8], start: usize, end: usize) -> usize {
	let mut pos = end;
	while pos > start && matches!(src[pos - 1], b' ' | b'\t' | b'\r') {
		pos -= 1;
	}
	pos
}

fn trailing_space_only(src: &[u8], start: usize, end: usize) -> bool {
	src[start..end]
		.iter()
		.all(|byte| matches!(*byte, b' ' | b'\t' | b'\r'))
}

pub fn translate_offset(local: usize, map: &[(usize, DocByte)]) -> DocByte {
	if map.is_empty() {
		return DocByte::new(local as u64);
	}
	let idx = map
		.partition_point(|&(viewport_idx, _)| viewport_idx <= local)
		.saturating_sub(1);
	let (viewport_idx, base) = map[idx];
	base.saturating_add(local.saturating_sub(viewport_idx) as u64)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn kinds(src: &[u8]) -> Vec<MarkdownTokenKind> {
		tokenize(src).into_iter().map(|token| token.kind).collect()
	}

	#[test]
	fn tokenizes_heading() {
		assert_eq!(
			kinds(b"## Title\n"),
			vec![
				MarkdownTokenKind::HeadingMarker,
				MarkdownTokenKind::HeadingText
			]
		);
	}

	#[test]
	fn tokenizes_fenced_code_body() {
		let kinds = kinds(b"```rust\nlet x = 1;\n```\n");
		assert!(kinds.contains(&MarkdownTokenKind::Fence));
		assert!(kinds.contains(&MarkdownTokenKind::FenceInfo));
		assert!(kinds.contains(&MarkdownTokenKind::CodeBlockText));
	}

	#[test]
	fn tokenizes_links() {
		let kinds = kinds(b"[Baryon](https://example.com)\n");
		assert!(kinds.contains(&MarkdownTokenKind::LinkLabel));
		assert!(kinds.contains(&MarkdownTokenKind::LinkDestination));
	}

	#[test]
	fn tokenizes_unordered_and_ordered_list_markers() {
		let tokens = tokenize(b"- item\n12. item\n");
		assert!(tokens.iter().any(|token| {
			token.kind == MarkdownTokenKind::ListMarker && token.start == 0 && token.end == 1
		}));
		assert!(tokens.iter().any(|token| {
			token.kind == MarkdownTokenKind::ListMarker && token.start == 7 && token.end == 10
		}));
	}

	#[test]
	fn tokenizes_multiline_html_comment() {
		let kinds = kinds(b"<!-- one\ntwo -->\n# Title\n");
		let comments = kinds
			.iter()
			.filter(|kind| **kind == MarkdownTokenKind::HtmlComment)
			.count();
		assert_eq!(comments, 2);
		assert!(kinds.contains(&MarkdownTokenKind::HeadingText));
	}

	#[test]
	fn open_html_comment_takes_precedence_over_fence_markers() {
		let kinds = kinds(b"<!-- one\n```rust\ntwo -->\n");
		assert!(!kinds.contains(&MarkdownTokenKind::Fence));
		assert!(
			kinds
				.iter()
				.all(|kind| *kind == MarkdownTokenKind::HtmlComment)
		);
	}
}
