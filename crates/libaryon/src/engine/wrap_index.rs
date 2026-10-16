//! Incremental soft wrap for lines too long to wrap whole (ADR 0009).
//!
//! [`WrapScanner`] reproduces `layout::compute_line_wrap` exactly while
//! consuming a line in chunks, so a long line can be wrapped one extent at a
//! time. A row start is emitted only once no later byte can move it: at a
//! word's start, when that word is known to overrun the row, and at each hard
//! break inside such a word. An emitted start is never retracted.
//!
//! The scanner holds back the last display cell between chunks, because the
//! next chunk may extend its grapheme. A held cell is capped at
//! [`MAX_HELD_CELL_BYTES`]: past it, the cell is taken as complete at a scalar
//! boundary. So a single grapheme longer than that (only pathological text has
//! one) may wrap differently from `compute_line_wrap`, and in exchange the
//! scanner's working memory is bounded.
//!
//! Rows made of trailing whitespace at the end of the line are known only when
//! the line ends. They are emitted then without a byte offset; their bytes lie
//! in the whitespace run that [`WrapScanner::trailing_whitespace_start`] names.

use crate::core::text::{DisplayCellKind, advance_display_col, visit_display_cells};
use crate::core::{DisplayOffset, DocLine, StateId, TAB_SIZE, TopologyRev};
use crate::ecs::NodeId;

/// Bytes of one display cell the scanner holds back before taking it as
/// complete. Bounds a scanner's carry, and so its working memory.
pub(crate) const MAX_HELD_CELL_BYTES: usize = 64 * 1024;

/// One wrapped row's start within its line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowStart {
	pub(crate) col: DisplayOffset,
	/// Line-relative byte of the cell holding `col`. A start inside a wide cell
	/// or a tab names that cell. `None` only for trailing-whitespace rows.
	pub(crate) byte: Option<u64>,
}

/// A position the scanner has passed: line-relative byte and display offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanPoint {
	pub(crate) byte: u64,
	pub(crate) col: DisplayOffset,
}

#[derive(Debug, Clone)]
pub(crate) struct WrapScanner {
	width: u64,
	visual_col: u64,
	row_start: u64,
	/// The word being read, if any.
	word_start: Option<ScanPoint>,
	/// The whitespace run since the last word, if any.
	whitespace_start: Option<ScanPoint>,
	/// Line-relative bytes fully scanned: everything before `carry`.
	consumed: u64,
	/// Bytes held back because a later chunk can extend them: the last display
	/// cell (a grapheme may continue) and an incomplete UTF-8 sequence.
	carry: Vec<u8>,
	ended: bool,
}

impl WrapScanner {
	/// A scanner at the start of a line. Row 0 starts at offset 0, byte 0, and
	/// is not emitted.
	pub(crate) fn new(text_width: u16) -> Self {
		Self {
			width: u64::from(text_width),
			visual_col: 0,
			row_start: 0,
			word_start: None,
			whitespace_start: None,
			consumed: 0,
			carry: Vec::new(),
			ended: false,
		}
	}

	/// Scan the next bytes of the line, appending each newly determined row
	/// start to `rows`. A newline ends the line; bytes after it are ignored.
	pub(crate) fn feed(&mut self, chunk: &[u8], rows: &mut Vec<RowStart>) {
		// A zero-width text area has one row and no width, as in
		// `compute_line_wrap`.
		if self.ended || chunk.is_empty() || self.width == 0 {
			return;
		}
		let mut buffer = std::mem::take(&mut self.carry);
		buffer.extend_from_slice(chunk);
		self.scan(&buffer, false, rows);
	}

	/// End the line, appending the remaining row starts. Returns the line's
	/// display width.
	pub(crate) fn finish(mut self, rows: &mut Vec<RowStart>) -> DisplayOffset {
		if self.width == 0 {
			return DisplayOffset::ZERO;
		}
		let carry = std::mem::take(&mut self.carry);
		if !self.ended {
			self.scan(&carry, true, rows);
		}
		// Whitespace-only lines and long trailing whitespace still need
		// reachable continuation rows.
		while self.visual_col.saturating_sub(self.row_start) > self.width {
			self.row_start = self.row_start.saturating_add(self.width);
			rows.push(RowStart {
				col: DisplayOffset::new(self.row_start),
				byte: None,
			});
		}
		DisplayOffset::new(self.visual_col)
	}

	/// Where the scan stands: everything before this point is final.
	pub(crate) fn scanned(&self) -> ScanPoint {
		ScanPoint {
			byte: self.consumed,
			col: DisplayOffset::new(self.visual_col),
		}
	}

	/// Line-relative bytes fed so far, including held-back ones: where the next
	/// chunk starts.
	pub(crate) fn fed(&self) -> u64 {
		self.consumed + self.carry.len() as u64
	}

	/// Whether a newline ended the line inside a fed chunk.
	pub(crate) fn ended(&self) -> bool {
		self.ended
	}

	/// Start of the whitespace run since the last word, which holds any
	/// trailing-whitespace rows [`Self::finish`] emits.
	pub(crate) fn trailing_whitespace_start(&self) -> Option<ScanPoint> {
		self.whitespace_start
	}

	fn scan(&mut self, buffer: &[u8], last: bool, rows: &mut Vec<RowStart>) {
		let end = if last {
			buffer.len()
		} else {
			buffer.len() - incomplete_utf8_tail(buffer)
		};
		let scanned = &buffer[..end];
		// Unless the line ends here, hold back the final cell: the next chunk
		// may extend its grapheme.
		let mut cells = Vec::new();
		visit_display_cells(scanned, |cell| {
			cells.push(cell);
			true
		});
		let held_cell_bytes = cells.last().map_or(0, |cell| end - cell.byte_start);
		let keep = if last || held_cell_bytes > MAX_HELD_CELL_BYTES {
			cells.len()
		} else {
			cells.len().saturating_sub(1)
		};
		let mut held_from = if keep < cells.len() {
			cells[keep].byte_start
		} else {
			end
		};
		for cell in &cells[..keep] {
			if cell.kind == DisplayCellKind::Newline {
				self.ended = true;
				held_from = buffer.len();
				break;
			}
			let byte = self.consumed + cell.byte_start as u64;
			let before = ScanPoint {
				byte,
				col: DisplayOffset::new(self.visual_col),
			};
			let is_whitespace = cell.kind == DisplayCellKind::Tab
				|| std::str::from_utf8(&scanned[cell.byte_start..cell.byte_end])
					.ok()
					.is_some_and(|text| text.chars().all(char::is_whitespace));
			if is_whitespace {
				self.word_start = None;
				self.whitespace_start.get_or_insert(before);
			} else {
				self.whitespace_start = None;
				self.word_start.get_or_insert(before);
			}
			// Display offsets past u32 advance without the u32 helper's limit.
			let advanced = advance_display_col(
				(self.visual_col % u64::from(TAB_SIZE)) as u32,
				*cell,
				TAB_SIZE,
			);
			let step = u64::from(advanced) - self.visual_col % u64::from(TAB_SIZE);
			self.visual_col = self.visual_col.saturating_add(step);
			if !is_whitespace {
				self.break_overrunning_word(byte, rows);
			}
		}
		self.consumed += held_from as u64;
		if !self.ended {
			self.carry = buffer[held_from..].to_vec();
		}
	}

	/// The current word reaches `visual_col`. Once it overruns the row, emit
	/// the breaks `compute_line_wrap` would make at the word's end: they are
	/// already determined, because the word can only get longer.
	fn break_overrunning_word(&mut self, cell_byte: u64, rows: &mut Vec<RowStart>) {
		if self.visual_col.saturating_sub(self.row_start) <= self.width {
			return;
		}
		let word = self.word_start.expect("an overrunning word has a start");
		let word_col = word.col.get();
		if word_col > self.row_start {
			self.row_start = word_col;
			rows.push(RowStart {
				col: word.col,
				byte: Some(word.byte),
			});
		}
		// Each further break falls inside the current cell: the row could not
		// overrun before it.
		while self.visual_col.saturating_sub(self.row_start) > self.width {
			self.row_start = self.row_start.saturating_add(self.width);
			rows.push(RowStart {
				col: DisplayOffset::new(self.row_start),
				byte: Some(cell_byte),
			});
		}
	}
}

/// A line's bytes as the wrap index reads them. Offsets are line-relative.
pub(crate) trait LineSource {
	/// Up to `max` bytes from `offset`, stopping at the line's end. `Bytes` is
	/// never empty: a source with nothing at `offset` returns `End`.
	fn read(&self, offset: u64, max: usize) -> LineRead;
}

pub(crate) enum LineRead {
	Bytes(Vec<u8>),
	/// The bytes at `offset` are not resident; hydrating this node makes them so.
	Pending(NodeId),
	/// `offset` is at or past the line's end.
	End,
}

/// What a lookup is for. A new `state` or `topology` discards every line; lines
/// at different text widths coexist, because render adjusts the width while
/// fitting the gutter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WrapKey {
	pub(crate) state: StateId,
	pub(crate) topology: TopologyRev,
	pub(crate) text_width: u16,
}

/// Where a lookup stands. `Unknown` asks the caller to refresh again, after
/// hydrating `pending` when given: each call reads a bounded number of bytes,
/// and each `Unknown` without `pending` has made progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowLookup {
	Known(RowStart),
	/// The line has fewer rows than requested; it has exactly `rows`.
	PastEnd {
		rows: u64,
	},
	Unknown {
		scanned_rows: u64,
		pending: Option<NodeId>,
	},
}

/// The row holding a display column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColLookup {
	/// `col` is the requested column, clamped to the line's width once known.
	Known {
		row: u64,
		start: RowStart,
		col: DisplayOffset,
	},
	Unknown {
		pending: Option<NodeId>,
	},
}

/// The byte of a cell found by walking the line's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ByteLookup {
	Known(u64),
	Unknown { pending: Option<NodeId> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowCount {
	Exact(u64),
	/// At least this many; the line is not scanned to its end yet.
	AtLeast {
		rows: u64,
		pending: Option<NodeId>,
	},
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WrapIndexLimits {
	/// Bytes one frame may read across every lookup, scanning or replaying:
	/// ADR 0007's per-line discovery bound. [`WrapIndex::begin_frame`]
	/// restores it.
	pub(crate) read_budget: usize,
	/// Bytes of input between checkpoints.
	pub(crate) checkpoint_bytes: u64,
	/// Memory one line's checkpoints and recent rows may hold. Over it,
	/// every other checkpoint is dropped; a checkpoint larger than the cap on
	/// its own is not kept.
	pub(crate) line_bytes: usize,
	/// Lines indexed before the least recently used is dropped.
	pub(crate) max_lines: usize,
}

impl Default for WrapIndexLimits {
	fn default() -> Self {
		Self {
			read_budget: crate::svp::ingest::COLD_INGEST_EXTENT_SIZE,
			checkpoint_bytes: 64 * 1024,
			// A checkpoint without carry is about 100 bytes: 4 MiB keeps
			// checkpoints about 5 MiB apart on a 200 GiB line.
			line_bytes: 4 * 1024 * 1024,
			max_lines: 8,
		}
	}
}

/// A scanner snapshot, the number of rows emitted before it, and the start
/// of the last of them: a column lookup may land in that row.
#[derive(Clone)]
struct Checkpoint {
	rows_before: u64,
	last_start: RowStart,
	scanner: WrapScanner,
}

impl Checkpoint {
	/// Heap bytes beyond the checkpoint's slot in its vector.
	fn carry_bytes(&self) -> usize {
		self.scanner.carry.capacity()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
	Row(u64),
	/// The row holding this display offset.
	Col(u64),
}

/// A replay toward `target` from a checkpoint, resumed across calls.
struct Replay {
	target: Target,
	scanner: WrapScanner,
	next_row: u64,
	/// The latest start passed: the answer for a column lookup.
	last: (u64, RowStart),
}

enum ReplayOutcome {
	Row(RowStart),
	Col { row: u64, start: RowStart },
	PastEnd { rows: u64 },
	Unknown { pending: Option<NodeId> },
}

struct LineWrap {
	line: DocLine,
	text_width: u16,
	/// Ascending by `rows_before`; the first is the line start.
	checkpoints: Vec<Checkpoint>,
	/// Sum of the checkpoints' carry bytes.
	checkpoint_carry: usize,
	/// The scan's leading edge, the rows it has emitted (row 0 included), and
	/// the start of the last of them.
	frontier: WrapScanner,
	frontier_rows: u64,
	frontier_last: RowStart,
	/// Exact once the line's end is scanned, with the line's display width
	/// and the start of its trailing whitespace: the window origin for rows
	/// without a byte of their own.
	total_rows: Option<u64>,
	visual_width: Option<u64>,
	trailing_whitespace: Option<ScanPoint>,
	/// How far a walk through the trailing whitespace has got, so a long run
	/// is walked across frames rather than restarted.
	whitespace_walk: Option<ScanPoint>,
	/// Starts emitted by the latest scan or replay, for cheap nearby lookups.
	recent: Vec<(u64, RowStart)>,
	replay: Option<Replay>,
}

const ROW_ZERO: RowStart = RowStart {
	col: DisplayOffset::ZERO,
	byte: Some(0),
};

/// Sub-chunk the scan feeds between checkpoint opportunities.
const FEED_STEP: usize = 4 * 1024;

/// Recent row starts kept per line: a fixed share of the line's memory.
const RECENT_ROWS: usize = 4 * 1024;

const RECENT_BYTES: usize = RECENT_ROWS * std::mem::size_of::<(u64, RowStart)>();

/// Working memory a line may hold beyond `line_bytes`: the frontier's and a
/// replay's held cells, each at most a held cell plus one sub-chunk, plus an
/// incomplete UTF-8 tail.
#[cfg(test)]
pub(crate) const WORKING_BYTES: usize = 2 * (MAX_HELD_CELL_BYTES + FEED_STEP + 4);

/// One read for the scan or a replay. `None` stops it: the line ended or
/// `Pending` names what to hydrate.
enum Step {
	Read(Vec<u8>),
	End,
	Pending(NodeId),
}

fn step(source: &dyn LineSource, offset: u64, max: usize) -> Step {
	match source.read(offset, max) {
		LineRead::Bytes(bytes) if !bytes.is_empty() => Step::Read(bytes),
		// An empty read cannot make progress; the contract makes it the end.
		LineRead::Bytes(_) | LineRead::End => Step::End,
		LineRead::Pending(node) => Step::Pending(node),
	}
}

impl LineWrap {
	fn new(line: DocLine, text_width: u16) -> Self {
		let start = Checkpoint {
			rows_before: 1,
			last_start: ROW_ZERO,
			scanner: WrapScanner::new(text_width),
		};
		Self {
			line,
			text_width,
			checkpoint_carry: start.carry_bytes(),
			frontier: start.scanner.clone(),
			checkpoints: vec![start],
			frontier_rows: 1,
			frontier_last: ROW_ZERO,
			total_rows: None,
			visual_width: None,
			trailing_whitespace: None,
			whitespace_walk: None,
			recent: vec![(0, ROW_ZERO)],
			replay: None,
		}
	}

	#[cfg(test)]
	fn held_bytes(&self) -> usize {
		let replay = self
			.replay
			.as_ref()
			.map_or(0, |replay| replay.scanner.carry.capacity());
		self.checkpoint_memory()
			+ self.recent.capacity() * std::mem::size_of::<(u64, RowStart)>()
			+ self.frontier.carry.capacity()
			+ replay
	}

	/// Advance the frontier, spending at most `budget` bytes from the frame's
	/// remainder.
	fn advance(
		&mut self,
		source: &dyn LineSource,
		limits: &WrapIndexLimits,
		budget: &mut usize,
	) -> Option<NodeId> {
		let mut rows = Vec::new();
		while self.total_rows.is_none() && *budget > 0 {
			match step(source, self.frontier.fed(), (*budget).min(FEED_STEP)) {
				Step::Pending(node) => return Some(node),
				Step::End => self.finish_frontier(&mut rows),
				Step::Read(bytes) => {
					*budget -= bytes.len().min(*budget);
					self.frontier.feed(&bytes, &mut rows);
					self.record(&mut rows);
					if self.frontier.ended() {
						self.finish_frontier(&mut rows);
					} else {
						self.checkpoint(limits);
					}
				}
			}
		}
		None
	}

	fn finish_frontier(&mut self, rows: &mut Vec<RowStart>) {
		let scanner = std::mem::replace(&mut self.frontier, WrapScanner::new(0));
		self.trailing_whitespace = scanner.trailing_whitespace_start();
		self.visual_width = Some(scanner.finish(rows).get());
		self.record(rows);
		self.total_rows = Some(self.frontier_rows);
	}

	fn record(&mut self, rows: &mut Vec<RowStart>) {
		for start in rows.drain(..) {
			self.push_recent(self.frontier_rows, start);
			self.frontier_last = start;
			self.frontier_rows += 1;
		}
	}

	fn push_recent(&mut self, row: u64, start: RowStart) {
		if self.recent.len() == RECENT_ROWS {
			self.recent.drain(..RECENT_ROWS / 2);
		}
		self.recent.push((row, start));
	}

	fn checkpoint(&mut self, limits: &WrapIndexLimits) {
		let last = self
			.checkpoints
			.last()
			.expect("a line has its start checkpoint");
		if self.frontier.fed() < last.scanner.fed() + limits.checkpoint_bytes {
			return;
		}
		let checkpoint = Checkpoint {
			rows_before: self.frontier_rows,
			last_start: self.frontier_last,
			scanner: self.frontier.clone(),
		};
		let budget = limits.line_bytes.saturating_sub(RECENT_BYTES);
		if std::mem::size_of::<Checkpoint>() + checkpoint.carry_bytes() > budget / 2 {
			// A huge held-back cell; replay from an earlier checkpoint instead.
			return;
		}
		self.checkpoint_carry += checkpoint.carry_bytes();
		self.checkpoints.push(checkpoint);
		if self.checkpoint_memory() <= budget {
			return;
		}
		while self.checkpoint_memory() > budget && self.checkpoints.len() > 2 {
			// Keep the line start and every other checkpoint: memory stays
			// bounded and a replay covers at most twice as far.
			let mut index = 0;
			self.checkpoints.retain(|_| {
				index += 1;
				index % 2 == 1
			});
			self.checkpoints.shrink_to_fit();
			self.checkpoint_carry = self.checkpoints.iter().map(Checkpoint::carry_bytes).sum();
		}
	}

	/// Checkpoint memory as allocated: vector slots plus carry.
	fn checkpoint_memory(&self) -> usize {
		self.checkpoints.capacity() * std::mem::size_of::<Checkpoint>() + self.checkpoint_carry
	}

	/// Replay toward `target`, which the frontier has already passed. Resumes a
	/// replay already heading there; spends at most `budget`.
	fn replay(
		&mut self,
		target: Target,
		source: &dyn LineSource,
		budget: &mut usize,
	) -> ReplayOutcome {
		// A recent hit leaves any replay under way in place for later targets.
		if let Target::Row(row) = target
			&& let Some(&(_, start)) = self.recent.iter().find(|(recent, _)| *recent == row)
		{
			return ReplayOutcome::Row(start);
		}
		// A column whose row and the next row are both recent needs no replay.
		if let Target::Col(col) = target
			&& let Some(&(row, start)) = self
				.recent
				.iter()
				.filter(|(_, start)| start.col.get() <= col)
				.max_by_key(|(row, _)| *row)
			&& (self.total_rows == Some(row + 1)
				|| self
					.recent
					.iter()
					.any(|(next, next_start)| *next == row + 1 && next_start.col.get() > col))
		{
			return ReplayOutcome::Col { row, start };
		}
		// A replay already under way serves any target at or ahead of where it
		// stands, so neighbouring rows continue it instead of restarting.
		let resumable = self.replay.as_ref().is_some_and(|replay| match target {
			Target::Row(row) => row >= replay.next_row,
			Target::Col(col) => col >= replay.last.1.col.get(),
		});
		if let Some(replay) = self.replay.as_mut().filter(|_| resumable) {
			replay.target = target;
		} else {
			let at = match target {
				Target::Row(row) => self
					.checkpoints
					.partition_point(|checkpoint| checkpoint.rows_before <= row),
				Target::Col(col) => self
					.checkpoints
					.partition_point(|checkpoint| checkpoint.scanner.scanned().col.get() <= col),
			}
			.saturating_sub(1);
			let checkpoint = &self.checkpoints[at];
			self.replay = Some(Replay {
				target,
				scanner: checkpoint.scanner.clone(),
				next_row: checkpoint.rows_before,
				last: (checkpoint.rows_before - 1, checkpoint.last_start),
			});
		}
		let mut replay = self.replay.take().expect("a replay was just placed");
		let mut rows = Vec::new();
		loop {
			if *budget == 0 {
				self.replay = Some(replay);
				return ReplayOutcome::Unknown { pending: None };
			}
			let finished = match step(source, replay.scanner.fed(), (*budget).min(FEED_STEP)) {
				Step::Pending(node) => {
					self.replay = Some(replay);
					return ReplayOutcome::Unknown {
						pending: Some(node),
					};
				}
				Step::End => {
					std::mem::replace(&mut replay.scanner, WrapScanner::new(0)).finish(&mut rows);
					true
				}
				Step::Read(bytes) => {
					*budget -= bytes.len().min(*budget);
					replay.scanner.feed(&bytes, &mut rows);
					if replay.scanner.ended() {
						std::mem::replace(&mut replay.scanner, WrapScanner::new(0))
							.finish(&mut rows);
						true
					} else {
						false
					}
				}
			};
			let mut found = None;
			for start in rows.drain(..) {
				match target {
					Target::Row(row) if replay.next_row == row => {
						found = Some(ReplayOutcome::Row(start))
					}
					Target::Col(col) if found.is_none() && start.col.get() > col => {
						let (row, start) = replay.last;
						found = Some(ReplayOutcome::Col { row, start });
					}
					_ => {}
				}
				self.push_recent(replay.next_row, start);
				replay.last = (replay.next_row, start);
				replay.next_row += 1;
			}
			if let Some(outcome) = found {
				// Keep the replay: the next rows usually continue from here.
				self.replay = Some(replay);
				return outcome;
			}
			if finished {
				return match target {
					Target::Row(_) => ReplayOutcome::PastEnd {
						rows: replay.next_row,
					},
					// The column is in the line's last row.
					Target::Col(_) => {
						let (row, start) = replay.last;
						ReplayOutcome::Col { row, start }
					}
				};
			}
		}
	}
}

/// Wrap rows of long lines, scanned lazily and kept within fixed limits.
pub(crate) struct WrapIndex {
	/// The `(state, topology)` the lines are valid for.
	valid_for: Option<(StateId, TopologyRev)>,
	/// Most recently used last.
	lines: Vec<LineWrap>,
	limits: WrapIndexLimits,
	/// Bytes this frame may still read.
	frame_budget: usize,
}

impl Default for WrapIndex {
	fn default() -> Self {
		Self::with_limits(WrapIndexLimits::default())
	}
}

impl WrapIndex {
	pub(crate) fn with_limits(limits: WrapIndexLimits) -> Self {
		assert!(
			limits.read_budget > 0
				&& limits.checkpoint_bytes > 0
				&& limits.line_bytes > RECENT_BYTES
				&& limits.max_lines > 0,
			"wrap index limits must allow progress: {limits:?}"
		);
		Self {
			valid_for: None,
			lines: Vec::new(),
			limits,
			frame_budget: limits.read_budget,
		}
	}

	/// Start a frame: every lookup until the next call shares one read budget,
	/// however many layout passes consult the same line.
	pub(crate) fn begin_frame(&mut self) {
		self.frame_budget = self.limits.read_budget;
	}

	/// The start of wrapped `row` of `line`.
	pub(crate) fn row_start(
		&mut self,
		key: WrapKey,
		line: DocLine,
		row: u64,
		source: &dyn LineSource,
	) -> RowLookup {
		if row == 0 {
			return RowLookup::Known(ROW_ZERO);
		}
		let limits = self.limits;
		let mut budget = self.frame_budget;
		let entry = Self::entry(&mut self.lines, &mut self.valid_for, &limits, key, line);
		let mut pending = None;
		if row >= entry.frontier_rows && entry.total_rows.is_none() {
			pending = entry.advance(source, &limits, &mut budget);
		}
		let lookup = match entry.total_rows {
			Some(rows) if row >= rows => RowLookup::PastEnd { rows },
			_ if row >= entry.frontier_rows => RowLookup::Unknown {
				scanned_rows: entry.frontier_rows,
				pending,
			},
			_ => match entry.replay(Target::Row(row), source, &mut budget) {
				ReplayOutcome::Row(start) => RowLookup::Known(start),
				ReplayOutcome::PastEnd { rows } => RowLookup::PastEnd { rows },
				ReplayOutcome::Unknown { pending } => RowLookup::Unknown {
					scanned_rows: entry.frontier_rows,
					pending,
				},
				ReplayOutcome::Col { .. } => unreachable!("a row replay finds a row"),
			},
		};
		self.frame_budget = budget;
		lookup
	}

	/// The row of `line` holding display offset `col`.
	pub(crate) fn row_for_col(
		&mut self,
		key: WrapKey,
		line: DocLine,
		col: DisplayOffset,
		source: &dyn LineSource,
	) -> ColLookup {
		// The line's first column is in its first row: no replay can be needed.
		if col == DisplayOffset::ZERO {
			return ColLookup::Known {
				row: 0,
				start: ROW_ZERO,
				col,
			};
		}
		let limits = self.limits;
		let mut budget = self.frame_budget;
		let entry = Self::entry(&mut self.lines, &mut self.valid_for, &limits, key, line);
		// Past the line's end a column sits in its last row, at its end.
		let col = entry
			.visual_width
			.map_or(col.get(), |width| col.get().min(width));
		let beyond_frontier = |entry: &LineWrap| {
			entry.total_rows.is_none() && entry.frontier.scanned().col.get() <= col
		};
		let lookup = if beyond_frontier(entry) {
			match entry.advance(source, &limits, &mut budget) {
				Some(node) => ColLookup::Unknown {
					pending: Some(node),
				},
				None if beyond_frontier(entry) => ColLookup::Unknown { pending: None },
				None => {
					let col = entry.visual_width.map_or(col, |width| col.min(width));
					Self::replay_col(entry, col, source, &mut budget)
				}
			}
		} else {
			Self::replay_col(entry, col, source, &mut budget)
		};
		self.frame_budget = budget;
		lookup
	}

	fn replay_col(
		entry: &mut LineWrap,
		col: u64,
		source: &dyn LineSource,
		budget: &mut usize,
	) -> ColLookup {
		match entry.replay(Target::Col(col), source, budget) {
			ReplayOutcome::Col { row, start } => ColLookup::Known {
				row,
				start,
				col: DisplayOffset::new(col),
			},
			ReplayOutcome::Unknown { pending } => ColLookup::Unknown { pending },
			ReplayOutcome::Row(_) | ReplayOutcome::PastEnd { .. } => {
				unreachable!("a column replay finds a column's row")
			}
		}
	}

	/// Where a window must start to show a row that has no byte of its own:
	/// the start of the line's trailing whitespace, once its end is scanned.
	pub(crate) fn trailing_whitespace_origin(
		&self,
		key: WrapKey,
		line: DocLine,
	) -> Option<ScanPoint> {
		if self.valid_for != Some((key.state, key.topology)) {
			return None;
		}
		self.lines
			.iter()
			.find(|entry| entry.line == line && entry.text_width == key.text_width)
			.and_then(|entry| entry.trailing_whitespace)
	}

	/// The byte of the cell holding display offset `col` within `line`'s
	/// trailing whitespace: where a row without a byte of its own starts. The
	/// walk from the run's start spends the frame budget and resumes on a
	/// later call.
	pub(crate) fn trailing_whitespace_byte(
		&mut self,
		key: WrapKey,
		line: DocLine,
		col: DisplayOffset,
		source: &dyn LineSource,
	) -> ByteLookup {
		let limits = self.limits;
		let mut budget = self.frame_budget;
		let entry = Self::entry(&mut self.lines, &mut self.valid_for, &limits, key, line);
		let Some(run) = entry.trailing_whitespace else {
			return ByteLookup::Unknown { pending: None };
		};
		let target = col.get();
		let mut at = match entry.whitespace_walk {
			Some(walk) if walk.col.get() <= target => walk,
			_ => run,
		};
		let lookup = loop {
			if budget == 0 {
				break ByteLookup::Unknown { pending: None };
			}
			let bytes = match step(source, at.byte, budget.min(FEED_STEP)) {
				Step::Pending(node) => {
					break ByteLookup::Unknown {
						pending: Some(node),
					};
				}
				// A column past the line's end is at its end.
				Step::End => break ByteLookup::Known(at.byte),
				Step::Read(bytes) => bytes,
			};
			budget -= bytes.len().min(budget);
			let tail = incomplete_utf8_tail(&bytes);
			let usable = if tail == bytes.len() {
				bytes.len()
			} else {
				bytes.len() - tail
			};
			let mut column = at.col.get();
			let mut consumed = 0usize;
			let mut found = None;
			visit_display_cells(&bytes[..usable], |cell| {
				let next = advance_u64(column, cell);
				if target < next {
					found = Some(at.byte + cell.byte_start as u64);
					return false;
				}
				column = next;
				consumed = cell.byte_end;
				true
			});
			if let Some(byte) = found {
				break ByteLookup::Known(byte);
			}
			at = ScanPoint {
				byte: at.byte + consumed as u64,
				col: DisplayOffset::new(column),
			};
		};
		entry.whitespace_walk = Some(at);
		self.frame_budget = budget;
		lookup
	}

	/// How many wrapped rows `line` has, as far as it is scanned.
	pub(crate) fn row_count(
		&mut self,
		key: WrapKey,
		line: DocLine,
		source: &dyn LineSource,
	) -> RowCount {
		let limits = self.limits;
		let mut budget = self.frame_budget;
		let entry = Self::entry(&mut self.lines, &mut self.valid_for, &limits, key, line);
		let pending = if entry.total_rows.is_none() {
			entry.advance(source, &limits, &mut budget)
		} else {
			None
		};
		let count = match entry.total_rows {
			Some(rows) => RowCount::Exact(rows),
			None => RowCount::AtLeast {
				rows: entry.frontier_rows,
				pending,
			},
		};
		self.frame_budget = budget;
		count
	}

	/// How far the scan of `line` at `text_width` has read, before it ends.
	#[cfg(test)]
	pub(crate) fn fed_bytes(&self, line: DocLine, text_width: u16) -> Option<u64> {
		self.lines
			.iter()
			.find(|entry| entry.line == line && entry.text_width == text_width)
			.map(|entry| entry.frontier.fed())
	}

	/// Memory held across every line: checkpoints, recent rows, and the live
	/// frontier and replay scanners. Per line it stays within
	/// `line_bytes + WORKING_BYTES`.
	#[cfg(test)]
	pub(crate) fn held_bytes(&self) -> usize {
		self.lines.iter().map(LineWrap::held_bytes).sum()
	}

	fn entry<'a>(
		lines: &'a mut Vec<LineWrap>,
		valid_for: &mut Option<(StateId, TopologyRev)>,
		limits: &WrapIndexLimits,
		key: WrapKey,
		line: DocLine,
	) -> &'a mut LineWrap {
		if *valid_for != Some((key.state, key.topology)) {
			*valid_for = Some((key.state, key.topology));
			lines.clear();
		}
		if let Some(at) = lines
			.iter()
			.position(|entry| entry.line == line && entry.text_width == key.text_width)
		{
			let entry = lines.remove(at);
			lines.push(entry);
		} else {
			if lines.len() == limits.max_lines {
				lines.remove(0);
			}
			lines.push(LineWrap::new(line, key.text_width));
		}
		lines.last_mut().expect("the entry was just placed last")
	}
}

/// `advance_display_col` for offsets past `u32`: the tab rule applies to the
/// column modulo the tab size.
fn advance_u64(column: u64, cell: crate::core::text::DisplayCell) -> u64 {
	let phase = (column % u64::from(TAB_SIZE)) as u32;
	let step = u64::from(advance_display_col(phase, cell, TAB_SIZE)) - u64::from(phase);
	column.saturating_add(step)
}

/// Length of a UTF-8 sequence cut off at the end of `bytes`, which a later
/// chunk may complete.
fn incomplete_utf8_tail(bytes: &[u8]) -> usize {
	for back in 1..=bytes.len().min(3) {
		let byte = bytes[bytes.len() - back];
		if byte & 0b1100_0000 == 0b1000_0000 {
			continue;
		}
		let needed = match byte {
			0xF0..=0xF7 => 4,
			0xE0..=0xEF => 3,
			0xC2..=0xDF => 2,
			_ => return 0,
		};
		return if needed > back { back } else { 0 };
	}
	0
}

#[cfg(test)]
mod tests {
	use super::{
		LineRead, LineSource, RowCount, RowLookup, RowStart, ScanPoint, WrapIndex, WrapIndexLimits,
		WrapKey, WrapScanner,
	};
	use crate::core::text::{advance_display_col, visit_display_cells};
	use crate::core::{DisplayOffset, DocLine, StateId, TAB_SIZE, TopologyRev};
	use crate::ecs::NodeId;
	use std::cell::RefCell;
	use std::ops::Range;

	/// Deterministic generator, so a failure reproduces from its seed.
	struct XorShift(u64);

	impl XorShift {
		fn next(&mut self) -> u64 {
			self.0 ^= self.0 << 13;
			self.0 ^= self.0 >> 7;
			self.0 ^= self.0 << 17;
			self.0
		}

		fn below(&mut self, bound: usize) -> usize {
			(self.next() % bound as u64) as usize
		}
	}

	const PIECES: &[&[u8]] = &[
		b"a",
		b"word",
		b"extraordinarily",
		b" ",
		b"  ",
		b"\t",
		"日本".as_bytes(),
		"e\u{301}".as_bytes(),
		"\u{1F468}\u{200D}\u{1F469}".as_bytes(),
		"\u{3000}".as_bytes(),
		&[0xFF],
		&[0xE6, 0x97],
		b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
	];

	fn line(seed: u64) -> Vec<u8> {
		let mut rng = XorShift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
		let mut line = Vec::new();
		for _ in 0..rng.below(60) {
			line.extend_from_slice(PIECES[rng.below(PIECES.len())]);
		}
		line
	}

	fn scan_in_chunks(line: &[u8], width: u16, seed: u64) -> (Vec<RowStart>, DisplayOffset) {
		let mut rng = XorShift(seed | 1);
		let mut scanner = WrapScanner::new(width);
		let mut rows = vec![RowStart {
			col: DisplayOffset::ZERO,
			byte: Some(0),
		}];
		let mut at = 0;
		while at < line.len() {
			let take = (1 + rng.below(9)).min(line.len() - at);
			scanner.feed(&line[at..at + take], &mut rows);
			at += take;
		}
		let width = scanner.finish(&mut rows);
		(rows, width)
	}

	/// The cell starting at `byte` and the display range it covers.
	fn cell_span(line: &[u8], byte: usize) -> Option<(u64, u64)> {
		let mut col = 0u32;
		let mut span = None;
		visit_display_cells(line, |cell| {
			let next = advance_display_col(col, cell, TAB_SIZE);
			if cell.byte_start == byte {
				span = Some((u64::from(col), u64::from(next)));
				return false;
			}
			col = next;
			true
		});
		span
	}

	#[test]
	fn chunked_scan_wraps_exactly_like_the_whole_line() {
		for seed in 0..600u64 {
			let line = line(seed);
			for width in [0u16, 1, 2, 3, 4, 7, 10, 80] {
				let whole = crate::engine::layout::wrap_starts_for_test(&line, width);
				let (rows, visual_width) = scan_in_chunks(&line, width, seed ^ u64::from(width));
				let cols: Vec<u64> = rows.iter().map(|row| row.col.get()).collect();
				assert_eq!(
					cols,
					whole.0,
					"seed {seed} width {width} line {:?}",
					String::from_utf8_lossy(&line)
				);
				assert_eq!(visual_width.get(), u64::from(whole.1));
				for row in &rows {
					// Row 0 of an empty line starts at its end, where no cell is.
					let Some(byte) = row.byte.filter(|&byte| (byte as usize) < line.len()) else {
						continue;
					};
					let (start, end) = cell_span(&line, byte as usize)
						.unwrap_or_else(|| panic!("seed {seed}: row byte {byte} starts no cell"));
					let col = row.col.get();
					assert!(
						start <= col && (col == start || col < end),
						"seed {seed} width {width}: row at {col} named the cell spanning {start}..{end}"
					);
				}
			}
		}
	}

	#[test]
	fn the_scan_point_and_trailing_whitespace_run_are_reported() {
		let mut scanner = WrapScanner::new(4);
		let mut rows = Vec::new();
		scanner.feed(b"ab\t  ", &mut rows);
		// The last cell is held back: a later chunk could extend it.
		assert_eq!(
			scanner.scanned(),
			ScanPoint {
				byte: 4,
				col: DisplayOffset::new(5),
			}
		);
		assert_eq!(
			scanner.trailing_whitespace_start(),
			Some(ScanPoint {
				byte: 2,
				col: DisplayOffset::new(2),
			})
		);
		// `x` ends the run once it is scanned; `y` is now the held cell.
		scanner.feed(b"xy", &mut rows);
		assert_eq!(scanner.trailing_whitespace_start(), None);
		assert_eq!(scanner.finish(&mut rows), DisplayOffset::new(8));
	}

	#[test]
	fn a_newline_ends_the_line() {
		let mut scanner = WrapScanner::new(4);
		let mut rows = Vec::new();
		scanner.feed(b"abc\nzzzzzzzzzzzz", &mut rows);
		assert_eq!(scanner.finish(&mut rows), DisplayOffset::new(3));
		assert!(rows.is_empty());
	}

	#[test]
	fn offsets_pass_u32_without_overflow() {
		let mut scanner = WrapScanner::new(100);
		scanner.visual_col = u64::from(u32::MAX) - 2;
		scanner.row_start = scanner.visual_col;
		let mut rows = Vec::new();
		scanner.feed(&[b'x'; 200], &mut rows);
		let width = scanner.finish(&mut rows);
		assert_eq!(width.get(), u64::from(u32::MAX) - 2 + 200);
		assert_eq!(rows[0].col.get(), u64::from(u32::MAX) - 2 + 100);
		assert!(rows[0].col.to_doc_display_col().is_none());
	}

	/// A line whose bytes in `unloaded` are not resident until hydrated.
	/// `read` records the bytes handed out since the last `take_read`.
	struct FakeLine {
		bytes: Vec<u8>,
		unloaded: RefCell<Vec<Range<u64>>>,
		read: std::cell::Cell<usize>,
	}

	impl FakeLine {
		fn new(bytes: Vec<u8>) -> Self {
			Self {
				bytes,
				unloaded: RefCell::new(Vec::new()),
				read: std::cell::Cell::new(0),
			}
		}

		fn take_read(&self) -> usize {
			self.read.replace(0)
		}
	}

	const UNLOADED_NODE: usize = 7;

	impl LineSource for FakeLine {
		fn read(&self, offset: u64, max: usize) -> LineRead {
			if offset >= self.bytes.len() as u64 {
				return LineRead::End;
			}
			let unloaded = self.unloaded.borrow();
			if unloaded.iter().any(|range| range.contains(&offset)) {
				return LineRead::Pending(NodeId::from_index(UNLOADED_NODE));
			}
			let next_unloaded = unloaded
				.iter()
				.map(|range| range.start)
				.filter(|&start| start > offset)
				.min()
				.unwrap_or(u64::MAX);
			let end = (offset + max as u64)
				.min(self.bytes.len() as u64)
				.min(next_unloaded);
			self.read.set(self.read.get() + (end - offset) as usize);
			LineRead::Bytes(self.bytes[offset as usize..end as usize].to_vec())
		}
	}

	fn long_line(target: usize) -> Vec<u8> {
		let mut rng = XorShift(0x5EED);
		let mut bytes = Vec::new();
		while bytes.len() < target {
			bytes.extend_from_slice(PIECES[rng.below(PIECES.len())]);
		}
		bytes
	}

	/// Room for about eight checkpoints per line, so thinning happens.
	fn tight_limits() -> WrapIndexLimits {
		WrapIndexLimits {
			read_budget: 8 * 1024,
			checkpoint_bytes: 2 * 1024,
			line_bytes: super::RECENT_BYTES + 8 * 2 * std::mem::size_of::<super::Checkpoint>(),
			max_lines: 2,
		}
	}

	/// Look `row` up, calling again while each call makes progress.
	fn settle(
		index: &mut WrapIndex,
		key: WrapKey,
		line: DocLine,
		row: u64,
		source: &FakeLine,
	) -> RowLookup {
		source.take_read();
		for _ in 0..10_000 {
			index.begin_frame();
			let lookup = index.row_start(key, line, row, source);
			assert!(
				source.take_read() <= index.limits.read_budget,
				"one frame read more than its budget"
			);
			match lookup {
				RowLookup::Unknown { pending: None, .. } => continue,
				settled => return settled,
			}
		}
		panic!("row {row} never settled");
	}

	const KEY: WrapKey = WrapKey {
		state: StateId::new(1),
		topology: TopologyRev::new(1),
		text_width: 10,
	};

	fn scan_to_end(index: &mut WrapIndex, key: WrapKey, line: DocLine, source: &FakeLine) -> u64 {
		for _ in 0..10_000 {
			index.begin_frame();
			if let RowCount::Exact(rows) = index.row_count(key, line, source) {
				return rows;
			}
		}
		panic!("the line never finished scanning");
	}

	#[test]
	fn lookups_after_bounded_scans_match_the_whole_line_wrap() {
		let source = FakeLine::new(long_line(200 * 1024));
		let (whole, _) = crate::engine::layout::wrap_starts_for_test(&source.bytes, KEY.text_width);
		let mut index = WrapIndex::with_limits(tight_limits());
		let line = DocLine::new(3);

		// One call reads at most one budget: the first count is a lower bound.
		assert!(matches!(
			index.row_count(KEY, line, &source),
			RowCount::AtLeast { pending: None, .. }
		));
		assert_eq!(
			scan_to_end(&mut index, KEY, line, &source),
			whole.len() as u64
		);

		// Sequential and scattered rows, through thinned checkpoints.
		let mut rng = XorShift(42);
		let rows: Vec<u64> = (0..200u64)
			.chain((0..200).map(|_| rng.below(whole.len()) as u64))
			.collect();
		for row in rows {
			let lookup = settle(&mut index, KEY, line, row, &source);
			let RowLookup::Known(start) = lookup else {
				panic!("row {row} of a scanned line: {lookup:?}");
			};
			assert_eq!(start.col.get(), whole[row as usize], "row {row}");
		}
		assert_eq!(
			index.row_start(KEY, line, whole.len() as u64, &source),
			RowLookup::PastEnd {
				rows: whole.len() as u64
			}
		);
		assert!(
			index.held_bytes() <= tight_limits().line_bytes,
			"held {} bytes over a {}-byte line cap",
			index.held_bytes(),
			tight_limits().line_bytes
		);
	}

	/// "start ", then one grapheme of `marks` combining accents (2 bytes each).
	fn grapheme_line(marks: usize) -> Vec<u8> {
		let mut bytes = b"start e".to_vec();
		for _ in 0..marks {
			bytes.extend_from_slice("\u{301}".as_bytes());
		}
		bytes.extend_from_slice(b" end of the line");
		bytes
	}

	/// Scan to the end, asserting the memory bound after every call: the peak
	/// is while a long grapheme is still held back.
	fn scan_within_bound(index: &mut WrapIndex, source: &FakeLine) -> u64 {
		let bound = tight_limits().line_bytes + super::WORKING_BYTES;
		for _ in 0..10_000 {
			index.begin_frame();
			let count = index.row_count(KEY, DocLine::ZERO, source);
			assert!(
				index.held_bytes() <= bound,
				"held {} bytes over the {bound}-byte line bound",
				index.held_bytes()
			);
			if let RowCount::Exact(rows) = count {
				return rows;
			}
		}
		panic!("the line never finished scanning");
	}

	#[test]
	fn a_long_grapheme_under_the_cap_wraps_exactly_within_the_memory_bound() {
		let source = FakeLine::new(grapheme_line(20_000));
		let (whole, _) = crate::engine::layout::wrap_starts_for_test(&source.bytes, KEY.text_width);
		let mut index = WrapIndex::with_limits(tight_limits());
		assert_eq!(scan_within_bound(&mut index, &source), whole.len() as u64);
		for row in 0..whole.len() as u64 {
			assert!(matches!(
				settle(&mut index, KEY, DocLine::ZERO, row, &source),
				RowLookup::Known(start) if start.col.get() == whole[row as usize]
			));
			assert!(index.held_bytes() <= tight_limits().line_bytes + super::WORKING_BYTES);
		}
	}

	#[test]
	fn a_grapheme_over_the_cap_is_split_and_memory_stays_bounded() {
		// 600 KB in one grapheme, more than the whole line bound: past
		// MAX_HELD_CELL_BYTES the scanner takes the held part as complete, the
		// documented deviation from whole-line wrap.
		let source = FakeLine::new(grapheme_line(300_000));
		assert!(source.bytes.len() > tight_limits().line_bytes + super::WORKING_BYTES);
		let mut index = WrapIndex::with_limits(tight_limits());
		let rows = scan_within_bound(&mut index, &source);
		assert!(rows >= 2, "the line still wraps");
	}

	#[test]
	fn an_empty_read_ends_the_line() {
		struct Empty;
		impl LineSource for Empty {
			fn read(&self, _: u64, _: usize) -> LineRead {
				LineRead::Bytes(Vec::new())
			}
		}
		let mut index = WrapIndex::default();
		assert_eq!(
			index.row_count(KEY, DocLine::ZERO, &Empty),
			RowCount::Exact(1)
		);
	}

	#[test]
	#[should_panic(expected = "wrap index limits must allow progress")]
	fn zero_limits_are_rejected() {
		WrapIndex::with_limits(WrapIndexLimits {
			read_budget: 0,
			..WrapIndexLimits::default()
		});
	}

	#[test]
	fn an_unloaded_range_pauses_the_scan_until_hydrated() {
		let source = FakeLine::new(long_line(64 * 1024));
		source.unloaded.borrow_mut().push(20_000..30_000);
		let mut index = WrapIndex::with_limits(WrapIndexLimits {
			read_budget: 64 * 1024,
			..tight_limits()
		});
		let line = DocLine::ZERO;
		let RowCount::AtLeast { rows, pending } = index.row_count(KEY, line, &source) else {
			panic!("the scan must stop at the unloaded range");
		};
		assert_eq!(pending, Some(NodeId::from_index(UNLOADED_NODE)));
		assert!(rows > 1, "rows before the unloaded range are known");
		// A row past the scanned prefix is unknown, and names what to hydrate.
		assert!(matches!(
			index.row_start(KEY, line, rows + 100, &source),
			RowLookup::Unknown {
				pending: Some(_),
				..
			}
		));

		source.unloaded.borrow_mut().clear();
		let (whole, _) = crate::engine::layout::wrap_starts_for_test(&source.bytes, KEY.text_width);
		assert_eq!(
			scan_to_end(&mut index, KEY, line, &source),
			whole.len() as u64
		);
	}

	#[test]
	fn a_new_key_or_an_evicted_line_starts_over() {
		let source = FakeLine::new(long_line(16 * 1024));
		let mut index = WrapIndex::with_limits(tight_limits());
		let first = DocLine::new(1);
		scan_to_end(&mut index, KEY, first, &source);
		assert!(matches!(
			index.row_count(KEY, first, &source),
			RowCount::Exact(_)
		));

		let edited = WrapKey {
			state: StateId::new(2),
			..KEY
		};
		assert!(matches!(
			index.row_count(edited, first, &source),
			RowCount::AtLeast { .. }
		));

		// Two lines fit; a third evicts the least recently used.
		scan_to_end(&mut index, edited, first, &source);
		scan_to_end(&mut index, edited, DocLine::new(2), &source);
		scan_to_end(&mut index, edited, DocLine::new(3), &source);
		assert!(matches!(
			index.row_count(edited, first, &source),
			RowCount::AtLeast { .. }
		));
	}

	/// The row `compute_line_wrap`'s starts put `col` in.
	fn whole_row_for_col(whole: &[u64], col: u64) -> u64 {
		(whole.partition_point(|&start| start <= col) - 1) as u64
	}

	fn settle_col(
		index: &mut WrapIndex,
		line: DocLine,
		col: u64,
		source: &FakeLine,
	) -> (u64, RowStart) {
		for _ in 0..10_000 {
			index.begin_frame();
			match index.row_for_col(KEY, line, DisplayOffset::new(col), source) {
				super::ColLookup::Known { row, start, .. } => return (row, start),
				super::ColLookup::Unknown { pending: None } => continue,
				other => panic!("column {col}: {other:?}"),
			}
		}
		panic!("column {col} never settled");
	}

	#[test]
	fn a_column_maps_to_the_row_the_whole_line_wrap_gives_it() {
		let source = FakeLine::new(long_line(64 * 1024));
		let (whole, width) =
			crate::engine::layout::wrap_starts_for_test(&source.bytes, KEY.text_width);
		let mut index = WrapIndex::with_limits(tight_limits());
		let line = DocLine::ZERO;
		// Before and after the scan completes, and past the line's end.
		let mut rng = XorShift(7);
		let cols: Vec<u64> = (0..200)
			.map(|_| rng.below(u64::from(width) as usize + 50) as u64)
			.chain([0, u64::from(width), u64::from(width) + 1000])
			.collect();
		for (pass, &col) in cols.iter().enumerate() {
			if pass == 100 {
				scan_to_end(&mut index, KEY, line, &source);
			}
			let (row, start) = settle_col(&mut index, line, col, &source);
			assert_eq!(row, whole_row_for_col(&whole, col), "column {col}");
			assert_eq!(start.col.get(), whole[row as usize], "column {col}");
		}
	}

	#[test]
	fn lookups_in_one_frame_share_one_read_budget() {
		let source = FakeLine::new(long_line(256 * 1024));
		let mut index = WrapIndex::with_limits(tight_limits());
		let line = DocLine::new(4);
		index.begin_frame();
		source.take_read();
		// Several layout passes ask about the same line within one frame.
		let _ = index.row_count(KEY, line, &source);
		let _ = index.row_start(KEY, line, 5_000, &source);
		let _ = index.row_for_col(KEY, line, DisplayOffset::new(40_000), &source);
		let _ = index.row_count(KEY, line, &source);
		assert!(
			source.take_read() <= tight_limits().read_budget,
			"one frame read more than one budget"
		);
		// The next frame may read again.
		index.begin_frame();
		let _ = index.row_count(KEY, line, &source);
		assert!(source.take_read() > 0);
	}

	#[test]
	fn widths_of_one_line_coexist() {
		let source = FakeLine::new(long_line(4 * 1024));
		let mut index = WrapIndex::with_limits(tight_limits());
		let line = DocLine::ZERO;
		let narrow = WrapKey {
			text_width: 7,
			..KEY
		};
		scan_to_end(&mut index, KEY, line, &source);
		scan_to_end(&mut index, narrow, line, &source);
		source.take_read();
		index.begin_frame();
		assert!(matches!(
			index.row_count(KEY, line, &source),
			RowCount::Exact(_)
		));
		assert!(matches!(
			index.row_count(narrow, line, &source),
			RowCount::Exact(_)
		));
		assert_eq!(source.take_read(), 0, "both widths stayed indexed");
	}
}
