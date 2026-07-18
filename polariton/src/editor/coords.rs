use std::cmp::Ordering;

pub const TAB_WIDTH: usize = 4;

macro_rules! impl_index_behavior {
	($name:ident) => {
		impl std::ops::Deref for $name {
			type Target = usize;
			fn deref(&self) -> &Self::Target {
				&self.0
			}
		}

		impl std::ops::Add<usize> for $name {
			type Output = Self;
			fn add(self, rhs: usize) -> Self::Output {
				$name(self.0 + rhs)
			}
		}

		impl std::ops::AddAssign<usize> for $name {
			fn add_assign(&mut self, rhs: usize) {
				self.0 += rhs;
			}
		}

		impl std::ops::Sub<usize> for $name {
			type Output = Self;
			fn sub(self, rhs: usize) -> Self::Output {
				$name(self.0.saturating_sub(rhs))
			}
		}

		impl std::ops::SubAssign<usize> for $name {
			fn sub_assign(&mut self, rhs: usize) {
				self.0 = self.0.saturating_sub(rhs);
			}
		}

		impl std::ops::Sub<Self> for $name {
			type Output = usize;
			fn sub(self, rhs: Self) -> Self::Output {
				self.0.saturating_sub(rhs.0)
			}
		}

		impl std::ops::Add<Self> for $name {
			type Output = Self;
			fn add(self, rhs: Self) -> Self::Output {
				$name(self.0 + rhs.0)
			}
		}

		impl $name {
			pub fn saturating_sub<T: Into<usize>>(self, rhs: T) -> Self {
				$name(self.0.saturating_sub(rhs.into()))
			}
			pub fn min<T: Into<Self>>(self, other: T) -> Self {
				$name(self.0.min(other.into().0))
			}
			pub fn max<T: Into<Self>>(self, other: T) -> Self {
				$name(self.0.max(other.into().0))
			}
		}

		impl std::fmt::Display for $name {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				write!(f, "{}", self.0)
			}
		}

		impl From<usize> for $name {
			fn from(v: usize) -> Self {
				Self(v)
			}
		}

		impl From<$name> for usize {
			fn from(v: $name) -> Self {
				v.0
			}
		}
	};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LineIdx(pub usize);
impl_index_behavior!(LineIdx);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CharIdx(pub usize);
impl_index_behavior!(CharIdx);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ByteIdx(pub usize);
impl_index_behavior!(ByteIdx);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct VisualCol(pub usize);
impl_index_behavior!(VisualCol);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CursorPos {
	pub line: LineIdx,
	pub col: CharIdx,
}

impl CursorPos {
	pub fn new(line: impl Into<LineIdx>, col: impl Into<CharIdx>) -> Self {
		Self {
			line: line.into(),
			col: col.into(),
		}
	}

	pub fn zero() -> Self {
		Self::default()
	}
}

impl PartialOrd for CursorPos {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for CursorPos {
	fn cmp(&self, other: &Self) -> Ordering {
		self.line.cmp(&other.line).then(self.col.cmp(&other.col))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Selection {
	pub anchor: CursorPos,
	pub head: CursorPos,
}

impl Selection {
	pub fn caret(p: CursorPos) -> Self {
		Self { anchor: p, head: p }
	}

	pub fn is_caret(&self) -> bool {
		self.anchor == self.head
	}

	pub fn ordered(&self) -> (CursorPos, CursorPos) {
		if self.anchor <= self.head {
			(self.anchor, self.head)
		} else {
			(self.head, self.anchor)
		}
	}
}

pub mod line {
	use super::{ByteIdx, CharIdx, VisualCol, TAB_WIDTH};

	pub fn visual_col_of(line: &str, logical_col: CharIdx) -> VisualCol {
		let mut vcol = 0usize;
		for (i, ch) in line.chars().enumerate() {
			if i >= *logical_col {
				break;
			}
			if ch == '\t' {
				vcol = (vcol / TAB_WIDTH + 1) * TAB_WIDTH;
			} else {
				vcol += 1;
			}
		}
		VisualCol(vcol)
	}

	pub fn logical_col_of(line: &str, target_vcol: VisualCol) -> CharIdx {
		let mut vcol = 0usize;
		for (i, ch) in line.chars().enumerate() {
			if vcol >= *target_vcol {
				return CharIdx(i);
			}
			if ch == '\t' {
				let next = (vcol / TAB_WIDTH + 1) * TAB_WIDTH;
				if *target_vcol < next {
					return CharIdx(i);
				}
				vcol = next;
			} else {
				vcol += 1;
			}
		}
		CharIdx(line.chars().count())
	}

	pub fn chars_with_vcols(line: &str) -> impl Iterator<Item = (char, VisualCol)> + '_ {
		let mut vcol = 0usize;
		line.chars().map(move |ch| {
			let start = vcol;
			vcol = if ch == '\t' {
				(vcol / TAB_WIDTH + 1) * TAB_WIDTH
			} else {
				vcol + 1
			};
			(ch, VisualCol(start))
		})
	}

	pub fn slice_chars(text: &str, start_col: CharIdx, end_col: CharIdx) -> String {
		text.chars()
			.skip(*start_col)
			.take((*end_col).saturating_sub(*start_col))
			.collect()
	}

	pub fn char_to_byte_idx(text: &str, char_idx: CharIdx) -> ByteIdx {
		ByteIdx(
			text.char_indices()
				.nth(*char_idx)
				.map(|(idx, _)| idx)
				.unwrap_or(text.len()),
		)
	}

	pub fn byte_to_char_idx(text: &str, byte_idx: ByteIdx) -> CharIdx {
		let mut char_count = 0;
		for (i, _) in text.char_indices() {
			if i >= *byte_idx {
				return CharIdx(char_count);
			}
			char_count += 1;
		}
		CharIdx(char_count)
	}
}

pub mod document {
	use super::{line, ByteIdx, CharIdx, CursorPos, LineIdx};
	use ropey::Rope;
	use tree_sitter::Point;

	pub fn clamp_pos(rope: &Rope, p: CursorPos) -> CursorPos {
		let line_count = rope.len_lines().max(1);
		let line = (*p.line).min(line_count.saturating_sub(1));
		let col = (*p.col).min(*line_len(rope, LineIdx(line)));
		CursorPos::new(LineIdx(line), CharIdx(col))
	}

	pub fn pos_to_char(rope: &Rope, p: CursorPos) -> usize {
		let clamped = clamp_pos(rope, p);
		rope.line_to_char(*clamped.line) + *clamped.col
	}

	pub fn byte_to_char_col(rope: &Rope, byte: usize) -> (LineIdx, CharIdx) {
		let char_idx = rope.byte_to_char(byte);
		let line = rope.char_to_line(char_idx);
		let col = char_idx - rope.line_to_char(line);
		(LineIdx(line), CharIdx(col))
	}

	pub fn point_to_char_pos<F>(rope: &Rope, point: Point, mut line_text: F) -> CursorPos
	where
		F: FnMut(LineIdx) -> String,
	{
		let line_count = rope.len_lines().max(1);
		if point.row >= line_count {
			return CursorPos::new(LineIdx(line_count.saturating_sub(1)), CharIdx(0));
		}
		let line = LineIdx(point.row);
		let text = line_text(line);
		let byte_col = ByteIdx(point.column.min(text.len()));
		CursorPos::new(line, line::byte_to_char_idx(&text, byte_col))
	}

	pub fn line_len(rope: &Rope, line: LineIdx) -> CharIdx {
		if *line >= rope.len_lines() {
			return CharIdx(0);
		}
		let text: String = rope.line(*line).chars().collect();
		CharIdx(
			text.trim_end_matches('\n')
				.trim_end_matches('\r')
				.chars()
				.count(),
		)
	}
}
