// ─── Vim mode ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum VimMode {
	Off,
	Normal,
	Insert,
	Visual,
	VisualLine,
	VisualBlock,
	Command,
}

// ─── Repeatable edit record (for dot-repeat) ──────────────────────────────────

#[derive(Clone, Debug)]
pub enum NormalEdit {
	/// d/y + motion (e.g. dw, d$)
	OperatorMotion {
		op: char,
		motion: String,
		count: usize,
	},
	/// c + motion: cut + re-insert last_insert_text on replay
	ChangeMotion { motion: String, count: usize },
	/// dd / cc / yy
	LineOp { op: char, count: usize },
	/// x — delete char forward
	DeleteChar { count: usize },
	/// X — delete char backward
	BackspaceChar { count: usize },
	/// ~ — toggle case
	ToggleCase { count: usize },
	/// r<c> — replace char
	ReplaceChar { ch: char, count: usize },
}

// ─── :substitute parser ────────────────────────────────────────────────────────

pub fn parse_substitute(
	cmd: &str,
	current_line: usize,
	last_line: usize,
) -> Option<(usize, usize, String, String, bool, bool)> {
	let mut i = 0;
	let bytes = cmd.as_bytes();
	while i < bytes.len() && matches!(bytes[i], b'0'..=b'9' | b'%' | b'.' | b'$' | b',') {
		i += 1;
	}
	let range_str = &cmd[..i];
	if bytes.get(i) != Some(&b's') {
		return None;
	}
	i += 1;
	let sep = *bytes.get(i)? as char;
	i += 1;
	let rest = &cmd[i..];
	let sep_str = sep.to_string();
	let mut parts = rest.splitn(3, sep_str.as_str());
	let pattern = parts.next().unwrap_or("");
	let replacement = parts.next().unwrap_or("");
	let flags = parts.next().unwrap_or("");
	if pattern.is_empty() {
		return None;
	}
	let (first, last) = parse_vim_range(range_str, current_line, last_line);
	let global = flags.contains('g');
	let icase = flags.contains('i');
	Some((
		first,
		last,
		pattern.to_string(),
		replacement.to_string(),
		global,
		icase,
	))
}

fn parse_vim_range(range: &str, current: usize, last: usize) -> (usize, usize) {
	match range.trim() {
		"" | "." => (current, current),
		"%" => (0, last),
		"$" => (last, last),
		s => {
			if let Some((a, b)) = s.split_once(',') {
				(
					parse_line_addr(a, current, last),
					parse_line_addr(b, current, last),
				)
			} else {
				let n = parse_line_addr(s, current, last);
				(n, n)
			}
		}
	}
}

fn parse_line_addr(s: &str, current: usize, last: usize) -> usize {
	match s.trim() {
		"." => current,
		"$" => last,
		n => n
			.parse::<usize>()
			.map(|n| n.saturating_sub(1).min(last))
			.unwrap_or(current),
	}
}
