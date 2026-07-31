use super::RepoSearchMode;
use super::request::{InternalEvent, InternalSender};
use crate::core::{BufferId, DocByte, DocDisplayCol, DocLine, PaneId, StateId, ViewId};
use crate::ecs::NodeId;
use regex_automata::meta::Regex;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

pub(crate) const SEARCH_DEBOUNCE: Duration = Duration::from_millis(75);
pub(crate) const MAX_RESULTS_PER_FILE: usize = 50;
pub(crate) const MAX_SEARCH_RESULTS: usize = 1_000;
pub(crate) const MAX_CORPUS_FILES: usize = 100_000;
pub(crate) const FALLBACK_WALK_ENTRIES: usize = 150_000;
const STREAM_FILE_INTERVAL: usize = 128;
const STREAM_TIME_INTERVAL: Duration = Duration::from_millis(40);
const LINE_TEXT_CHARS: usize = 240;
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchSourceStamp {
	pub(crate) buffer_id: BufferId,
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveFileSnapshot {
	pub(crate) path: PathBuf,
	/// `None` marks an open file excluded by the per-file size gate. Keeping
	/// the path in the map prevents the worker from incorrectly reading its
	/// stale on-disk copy.
	pub(crate) bytes: Option<Vec<u8>>,
	pub(crate) stamp: SearchSourceStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PickerOrigin {
	pub(crate) pane_id: PaneId,
	pub(crate) view_id: ViewId,
	pub(crate) buffer_id: BufferId,
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) cursor: DocByte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepoSearchMatch {
	pub(crate) path: PathBuf,
	pub(crate) display_path: String,
	pub(crate) line: DocLine,
	pub(crate) col: DocDisplayCol,
	pub(crate) byte: DocByte,
	pub(crate) line_text: String,
	pub(crate) score: i64,
	pub(crate) line_chars: usize,
	pub(crate) path_depth: usize,
	pub(crate) source_stamp: Option<SearchSourceStamp>,
}

#[derive(Debug)]
pub(crate) struct RepoSearchBatch {
	pub(crate) generation: u64,
	pub(crate) results: Vec<RepoSearchMatch>,
	pub(crate) matched: u64,
	pub(crate) truncated: bool,
	pub(crate) searching: bool,
	pub(crate) files_walked: u64,
	pub(crate) error: Option<String>,
}

pub(crate) struct RepoSearchRequest {
	pub(crate) generation: u64,
	pub(crate) mode: RepoSearchMode,
	pub(crate) query: String,
	pub(crate) anchor: PathBuf,
	pub(crate) live_files: Vec<LiveFileSnapshot>,
	pub(crate) cancel: Arc<AtomicBool>,
	pub(crate) files_walked: Arc<AtomicU64>,
}

#[derive(Debug)]
pub(crate) struct PickerState {
	pub(crate) mode: RepoSearchMode,
	pub(crate) query: String,
	pub(crate) query_cursor: usize,
	pub(crate) selected: usize,
	pub(crate) generation: u64,
	pub(crate) results: Vec<RepoSearchMatch>,
	pub(crate) matched: u64,
	pub(crate) truncated: bool,
	pub(crate) searching: bool,
	pub(crate) files_walked: u64,
	pub(crate) error: Option<String>,
	pub(crate) origin: PickerOrigin,
	pub(crate) cancel: Option<Arc<AtomicBool>>,
}

impl PickerState {
	pub(crate) fn new(mode: RepoSearchMode, query: String, origin: PickerOrigin) -> Self {
		let query_cursor = query.len();
		Self {
			mode,
			query,
			query_cursor,
			selected: 0,
			generation: 0,
			results: Vec::new(),
			matched: 0,
			truncated: false,
			searching: false,
			files_walked: 0,
			error: None,
			origin,
			cancel: None,
		}
	}

	pub(crate) fn cancel_active(&mut self) {
		if let Some(cancel) = self.cancel.take() {
			cancel.store(true, AtomicOrdering::Release);
		}
		self.searching = false;
	}

	pub(crate) fn begin_generation(&mut self, generation: u64) {
		self.cancel_active();
		self.generation = generation;
		self.selected = 0;
		self.results.clear();
		self.matched = 0;
		self.truncated = false;
		self.files_walked = 0;
		self.error = None;
		self.searching = !self.query.is_empty();
	}

	pub(crate) fn apply_batch(&mut self, batch: RepoSearchBatch) -> bool {
		if batch.generation != self.generation {
			return false;
		}
		self.results = batch.results;
		self.matched = batch.matched;
		self.truncated = batch.truncated;
		self.searching = batch.searching;
		self.files_walked = batch.files_walked;
		self.error = batch.error;
		self.selected = self.selected.min(self.results.len().saturating_sub(1));
		true
	}

	pub(crate) fn move_selection(&mut self, delta: i32) {
		if self.results.is_empty() {
			self.selected = 0;
			return;
		}
		let len = self.results.len() as i64;
		self.selected = (self.selected as i64 + i64::from(delta)).rem_euclid(len) as usize;
	}
}

impl Drop for PickerState {
	fn drop(&mut self) {
		self.cancel_active();
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MatchTier {
	Subsequence = 1,
	BoundarySubsequence = 2,
	Exact = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LineScore {
	tier: MatchTier,
	score: i64,
	col_byte: usize,
}

enum LineMatcher {
	Fuzzy(String),
	Literal(Vec<u8>),
	Regex(Regex),
}

impl LineMatcher {
	fn compile(mode: RepoSearchMode, query: &str) -> Result<Self, String> {
		match mode {
			RepoSearchMode::Fuzzy => Ok(Self::Fuzzy(query.to_string())),
			RepoSearchMode::Literal => Ok(Self::Literal(query.as_bytes().to_vec())),
			RepoSearchMode::Regex => Regex::new(query).map(Self::Regex).map_err(|error| {
				let detail = error
					.to_string()
					.split_whitespace()
					.collect::<Vec<_>>()
					.join(" ");
				format!("Invalid regular expression: {detail}")
			}),
		}
	}

	fn score(&self, line: &str) -> Option<LineScore> {
		match self {
			Self::Fuzzy(query) => score_line(query, line),
			Self::Literal(query) => {
				let col = memchr::memmem::find(line.as_bytes(), query)?;
				Some(exact_line_score(col))
			}
			Self::Regex(regex) => regex
				.find(line.as_bytes())
				.map(|matched| exact_line_score(matched.start())),
		}
	}
}

fn exact_line_score(col_byte: usize) -> LineScore {
	LineScore {
		tier: MatchTier::Exact,
		score: 3_000_000i64.saturating_sub(col_byte as i64),
		col_byte,
	}
}

pub(crate) fn spawn_search_task(
	request: RepoSearchRequest,
	internal: InternalSender,
) -> Result<(), String> {
	std::thread::Builder::new()
		.name(format!("repo-search-{}", request.generation))
		.spawn(move || run_search_task(request, internal))
		.map(|_| ())
		.map_err(|error| format!("Unable to start repository search: {error}"))
}

pub(crate) fn spawn_search_debounce(
	generation: u64,
	cancel: Arc<AtomicBool>,
	internal: InternalSender,
) -> Result<(), String> {
	std::thread::Builder::new()
		.name(format!("repo-search-debounce-{generation}"))
		.spawn(move || {
			std::thread::sleep(SEARCH_DEBOUNCE);
			if !cancel.load(AtomicOrdering::Acquire) {
				let _ = internal.send(InternalEvent::RepoSearchReady(generation));
			}
		})
		.map(|_| ())
		.map_err(|error| format!("Unable to schedule repository search: {error}"))
}

fn run_search_task(request: RepoSearchRequest, internal: InternalSender) {
	if request.cancel.load(AtomicOrdering::Acquire) {
		return;
	}
	let matcher = match LineMatcher::compile(request.mode, &request.query) {
		Ok(matcher) => matcher,
		Err(error) => {
			let _ = internal.send(InternalEvent::RepoSearchBatch(Box::new(RepoSearchBatch {
				generation: request.generation,
				results: Vec::new(),
				matched: 0,
				truncated: false,
				searching: false,
				files_walked: 0,
				error: Some(error),
			})));
			return;
		}
	};

	let (root, files, corpus_truncated) = corpus_files(&request.anchor);
	let live_files = request
		.live_files
		.into_iter()
		.map(|snapshot| (snapshot.path.clone(), snapshot))
		.collect::<HashMap<_, _>>();
	let mut results = Vec::new();
	let mut matched = 0u64;
	let mut truncated = corpus_truncated;
	let mut last_stream = Instant::now();

	for (file_index, relative) in files.into_iter().enumerate() {
		if request.cancel.load(AtomicOrdering::Acquire) {
			return;
		}
		request.files_walked.fetch_add(1, AtomicOrdering::AcqRel);
		let path = root.join(&relative);
		let canonical = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
		let live = live_files.get(&canonical);
		let bytes = match live {
			Some(snapshot) => {
				let Some(bytes) = snapshot.bytes.as_deref() else {
					continue;
				};
				bytes
			}
			None => {
				let Ok(metadata) = std::fs::metadata(&path) else {
					continue;
				};
				if metadata.len() >= super::MAX_SEMANTIC_BYTES {
					continue;
				}
				let Ok(owned) = std::fs::read(&path) else {
					continue;
				};
				if is_binary(&owned) {
					continue;
				}
				search_file(
					&matcher,
					&relative,
					&path,
					&owned,
					None,
					&request.cancel,
					&mut results,
					&mut matched,
					&mut truncated,
				);
				if should_stream(file_index, last_stream) {
					sort_and_cap(&mut results, &mut truncated);
					let _ = internal.try_send(InternalEvent::RepoSearchBatch(Box::new(
						RepoSearchBatch {
							generation: request.generation,
							results: results.clone(),
							matched,
							truncated,
							searching: true,
							files_walked: request.files_walked.load(AtomicOrdering::Acquire),
							error: None,
						},
					)));
					last_stream = Instant::now();
				}
				continue;
			}
		};
		if bytes.len() as u64 >= super::MAX_SEMANTIC_BYTES || is_binary(bytes) {
			continue;
		}
		search_file(
			&matcher,
			&relative,
			&path,
			bytes,
			live.map(|snapshot| snapshot.stamp),
			&request.cancel,
			&mut results,
			&mut matched,
			&mut truncated,
		);
		if should_stream(file_index, last_stream) {
			sort_and_cap(&mut results, &mut truncated);
			let _ = internal.try_send(InternalEvent::RepoSearchBatch(Box::new(RepoSearchBatch {
				generation: request.generation,
				results: results.clone(),
				matched,
				truncated,
				searching: true,
				files_walked: request.files_walked.load(AtomicOrdering::Acquire),
				error: None,
			})));
			last_stream = Instant::now();
		}
	}

	if request.cancel.load(AtomicOrdering::Acquire) {
		return;
	}
	sort_and_cap(&mut results, &mut truncated);
	let _ = internal.send(InternalEvent::RepoSearchBatch(Box::new(RepoSearchBatch {
		generation: request.generation,
		results,
		matched,
		truncated,
		searching: false,
		files_walked: request.files_walked.load(AtomicOrdering::Acquire),
		error: None,
	})));
}

fn should_stream(file_index: usize, last_stream: Instant) -> bool {
	(file_index + 1).is_multiple_of(STREAM_FILE_INTERVAL)
		|| last_stream.elapsed() >= STREAM_TIME_INTERVAL
}

#[allow(clippy::too_many_arguments)]
fn search_file(
	matcher: &LineMatcher,
	relative: &Path,
	path: &Path,
	bytes: &[u8],
	source_stamp: Option<SearchSourceStamp>,
	cancel: &AtomicBool,
	results: &mut Vec<RepoSearchMatch>,
	matched: &mut u64,
	truncated: &mut bool,
) {
	let Ok(text) = std::str::from_utf8(bytes) else {
		return;
	};
	let display_path = relative.to_string_lossy().replace('\\', "/");
	let path_depth = relative.components().count();
	let mut byte_start = 0usize;
	let mut stored_for_file = 0usize;
	for (line_number, segment) in text.split_inclusive('\n').enumerate() {
		if line_number.is_multiple_of(128) && cancel.load(AtomicOrdering::Acquire) {
			return;
		}
		let without_newline = segment.strip_suffix('\n').unwrap_or(segment);
		let line = without_newline
			.strip_suffix('\r')
			.unwrap_or(without_newline);
		if let Some(line_score) = matcher.score(line) {
			*matched = matched.saturating_add(1);
			if stored_for_file < MAX_RESULTS_PER_FILE {
				results.push(RepoSearchMatch {
					path: path.to_path_buf(),
					display_path: display_path.clone(),
					line: DocLine::new(line_number as u64),
					col: display_col(line, line_score.col_byte),
					byte: DocByte::new((byte_start + line_score.col_byte) as u64),
					line_text: capped_line_text(line),
					score: line_score.score,
					line_chars: line.chars().count(),
					path_depth,
					source_stamp,
				});
				stored_for_file += 1;
			} else {
				*truncated = true;
			}
		}
		byte_start = byte_start.saturating_add(segment.len());
	}
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
					tier: MatchTier::Exact,
					score: 3_000_000i64.saturating_sub(col as i64),
					col_byte: col,
				});
			}
		}
	}

	if let Some((col, gap)) = subsequence(&query_chars, &line_chars, case_sensitive, true) {
		return Some(LineScore {
			tier: MatchTier::BoundarySubsequence,
			score: 2_000_000i64
				.saturating_sub((gap as i64).saturating_mul(8))
				.saturating_sub(col as i64),
			col_byte: col,
		});
	}
	let (col, gap) = subsequence(&query_chars, &line_chars, case_sensitive, false)?;
	Some(LineScore {
		tier: MatchTier::Subsequence,
		score: 1_000_000i64
			.saturating_sub((gap as i64).saturating_mul(8))
			.saturating_sub(col as i64),
		col_byte: col,
	})
}

pub(crate) fn query_column(
	mode: RepoSearchMode,
	query: &str,
	line: &str,
) -> Result<Option<(usize, DocDisplayCol)>, String> {
	let matcher = LineMatcher::compile(mode, query)?;
	Ok(matcher
		.score(line)
		.map(|score| (score.col_byte, display_col(line, score.col_byte))))
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

fn is_word_boundary(line: &[(usize, char)], index: usize) -> bool {
	index == 0 || !line[index - 1].1.is_alphanumeric()
}

fn chars_equal(query: char, candidate: char, case_sensitive: bool) -> bool {
	if case_sensitive {
		query == candidate
	} else {
		query.to_lowercase().eq(candidate.to_lowercase())
	}
}

fn result_order(left: &RepoSearchMatch, right: &RepoSearchMatch) -> Ordering {
	right
		.score
		.cmp(&left.score)
		.then_with(|| left.line_chars.cmp(&right.line_chars))
		.then_with(|| left.path_depth.cmp(&right.path_depth))
		.then_with(|| left.display_path.cmp(&right.display_path))
		.then_with(|| left.line.cmp(&right.line))
		.then_with(|| left.col.cmp(&right.col))
}

fn sort_and_cap(results: &mut Vec<RepoSearchMatch>, truncated: &mut bool) {
	results.sort_by(result_order);
	if results.len() > MAX_SEARCH_RESULTS {
		results.truncate(MAX_SEARCH_RESULTS);
		*truncated = true;
	}
}

fn display_col(line: &str, byte: usize) -> DocDisplayCol {
	let mut col = 0u32;
	for ch in line[..byte.min(line.len())].chars() {
		if ch == '\t' {
			col = (col / crate::core::TAB_SIZE + 1) * crate::core::TAB_SIZE;
		} else {
			col = col.saturating_add(1);
		}
	}
	DocDisplayCol::new(col)
}

fn capped_line_text(line: &str) -> String {
	let trimmed = line.trim();
	let mut text = trimmed.chars().take(LINE_TEXT_CHARS).collect::<String>();
	if trimmed.chars().count() > LINE_TEXT_CHARS {
		text.push('…');
	}
	text
}

fn is_binary(bytes: &[u8]) -> bool {
	bytes.iter().take(BINARY_SNIFF_BYTES).any(|byte| *byte == 0)
}

fn corpus_files(anchor: &Path) -> (PathBuf, Vec<PathBuf>, bool) {
	if let Some(corpus) = git_corpus(anchor) {
		return corpus;
	}
	fallback_corpus(anchor)
}

fn git_corpus(anchor: &Path) -> Option<(PathBuf, Vec<PathBuf>, bool)> {
	let directory = if anchor.is_dir() {
		anchor
	} else {
		anchor.parent().unwrap_or(anchor)
	};
	let root_output = Command::new("git")
		.arg("-C")
		.arg(directory)
		.args(["rev-parse", "--show-toplevel"])
		.output()
		.ok()?;
	if !root_output.status.success() {
		return None;
	}
	let root = PathBuf::from(
		String::from_utf8(root_output.stdout)
			.ok()?
			.trim_end_matches(['\r', '\n']),
	);
	let files_output = Command::new("git")
		.arg("-C")
		.arg(&root)
		.args(["ls-files", "-z"])
		.output()
		.ok()?;
	if !files_output.status.success() {
		return None;
	}
	let mut truncated = false;
	let mut files = Vec::new();
	for path in files_output.stdout.split(|byte| *byte == 0) {
		if path.is_empty() {
			continue;
		}
		if files.len() >= MAX_CORPUS_FILES {
			truncated = true;
			break;
		}
		files.push(PathBuf::from(String::from_utf8_lossy(path).into_owned()));
	}
	files.sort();
	Some((root, files, truncated))
}

fn fallback_corpus(anchor: &Path) -> (PathBuf, Vec<PathBuf>, bool) {
	let root = if anchor.is_dir() {
		anchor.to_path_buf()
	} else {
		anchor
			.parent()
			.unwrap_or_else(|| Path::new("."))
			.to_path_buf()
	};
	let mut stack = vec![root.clone()];
	let mut files = Vec::new();
	let mut visited = 0usize;
	let mut truncated = false;
	while let Some(directory) = stack.pop() {
		let Ok(entries) = std::fs::read_dir(&directory) else {
			continue;
		};
		let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
		entries.sort_by_key(std::fs::DirEntry::file_name);
		for entry in entries.into_iter().rev() {
			visited += 1;
			if visited > FALLBACK_WALK_ENTRIES || files.len() >= MAX_CORPUS_FILES {
				truncated = true;
				break;
			}
			let path = entry.path();
			let Ok(kind) = entry.file_type() else {
				continue;
			};
			if kind.is_symlink() {
				continue;
			}
			if kind.is_dir() {
				if matches!(entry.file_name().to_str(), Some(".git" | "target")) {
					continue;
				}
				stack.push(path);
			} else if kind.is_file() {
				files.push(path.strip_prefix(&root).unwrap_or(&path).to_path_buf());
			}
		}
		if truncated {
			break;
		}
	}
	files.sort();
	(root, files, truncated)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::BTreeSet;
	use std::sync::atomic::AtomicBool;

	#[test]
	fn scorer_orders_exact_boundary_and_plain_subsequences() {
		let exact = score_line("render_frame", "fn render_frame()").expect("exact");
		let boundary = score_line("rf", "fn render_frame()").expect("boundary");
		let plain = score_line("rndr", "fn render_frame()").expect("plain");
		assert_eq!(exact.tier, MatchTier::Exact);
		assert_eq!(boundary.tier, MatchTier::BoundarySubsequence);
		assert_eq!(plain.tier, MatchTier::Subsequence);
		assert!(exact.score > boundary.score);
		assert!(boundary.score > plain.score);
	}

	#[test]
	fn scorer_uses_smartcase_and_prefers_earlier_exact_columns() {
		assert!(score_line("frame", "FRAME").is_some());
		assert!(score_line("Frame", "FRAME").is_none());
		let early = score_line("frame", "frame later").expect("early");
		let late = score_line("frame", "later frame").expect("late");
		assert!(early.score > late.score);
	}

	#[test]
	fn result_ties_are_deterministic() {
		let make = |path: &str, line_chars, depth| RepoSearchMatch {
			path: PathBuf::from(path),
			display_path: path.to_string(),
			line: DocLine::ZERO,
			col: DocDisplayCol::ZERO,
			byte: DocByte::ZERO,
			line_text: String::new(),
			score: 3_000_000,
			line_chars,
			path_depth: depth,
			source_stamp: None,
		};
		let mut results = vec![
			make("z/deep.rs", 10, 2),
			make("b.rs", 10, 1),
			make("a.rs", 10, 1),
			make("long.rs", 20, 1),
		];
		let mut truncated = false;
		sort_and_cap(&mut results, &mut truncated);
		assert_eq!(
			results
				.iter()
				.map(|result| result.display_path.as_str())
				.collect::<Vec<_>>(),
			["a.rs", "b.rs", "z/deep.rs", "long.rs"]
		);
	}

	#[test]
	fn binary_sniff_detects_nul_without_rejecting_utf8() {
		assert!(is_binary(b"abc\0def"));
		assert!(!is_binary("café".as_bytes()));
	}

	#[test]
	fn an_older_batch_cannot_land_after_a_new_query_generation() {
		let mut picker = PickerState::new(
			RepoSearchMode::Fuzzy,
			"first".to_string(),
			PickerOrigin {
				pane_id: PaneId::new(1),
				view_id: ViewId::new(1),
				buffer_id: BufferId::new(1),
				root: NodeId::from_index(0),
				state_id: StateId::new(1),
				cursor: DocByte::ZERO,
			},
		);
		picker.begin_generation(41);
		picker.begin_generation(42);
		assert!(!picker.apply_batch(RepoSearchBatch {
			generation: 41,
			results: Vec::new(),
			matched: 99,
			truncated: true,
			searching: false,
			files_walked: 99,
			error: None,
		}));
		assert_eq!(picker.generation, 42);
		assert_eq!(picker.matched, 0);
	}

	#[test]
	fn live_bytes_are_the_search_and_jump_coordinate_source() {
		let cancel = AtomicBool::new(false);
		let mut results = Vec::new();
		let mut matched = 0;
		let mut truncated = false;
		let matcher = LineMatcher::compile(RepoSearchMode::Fuzzy, "live_value").expect("matcher");
		search_file(
			&matcher,
			Path::new("src/live.rs"),
			Path::new("/repo/src/live.rs"),
			b"let live_value = 7;\n",
			None,
			&cancel,
			&mut results,
			&mut matched,
			&mut truncated,
		);
		assert_eq!(matched, 1);
		assert_eq!(results[0].byte, DocByte::new(4));
		assert_eq!(
			query_column(RepoSearchMode::Fuzzy, "live_value", "let stale_value = 7;"),
			Ok(None),
			"acceptance must reject a result after its current line stops matching"
		);
	}

	#[test]
	fn literal_and_regex_modes_interpret_metacharacters_honestly() {
		let literal = LineMatcher::compile(RepoSearchMode::Literal, "render.*frame")
			.expect("literal matcher");
		assert!(literal.score("render.*frame").is_some());
		assert!(literal.score("render_one_frame").is_none());

		let regex =
			LineMatcher::compile(RepoSearchMode::Regex, "render.*frame").expect("regex matcher");
		assert!(regex.score("render_one_frame").is_some());
		assert!(regex.score("RENDER_ONE_FRAME").is_none());
		let insensitive =
			LineMatcher::compile(RepoSearchMode::Regex, "(?i)render.*frame").expect("flags");
		assert!(insensitive.score("RENDER_ONE_FRAME").is_some());
	}

	#[test]
	fn literal_and_regex_columns_remain_utf8_bytes_and_tab_display_columns() {
		for mode in [RepoSearchMode::Literal, RepoSearchMode::Regex] {
			assert_eq!(
				query_column(mode, "β", "\tαβ"),
				Ok(Some((3, DocDisplayCol::new(5))))
			);
		}
	}

	#[test]
	fn invalid_regex_is_a_recoverable_final_batch() {
		let (_, _, internal, requests) = super::super::request::channel();
		run_search_task(
			RepoSearchRequest {
				generation: 12,
				mode: RepoSearchMode::Regex,
				query: "[".to_string(),
				anchor: PathBuf::from("."),
				live_files: Vec::new(),
				cancel: Arc::new(AtomicBool::new(false)),
				files_walked: Arc::new(AtomicU64::new(0)),
			},
			internal,
		);
		let request = requests.recv().expect("regex error");
		let super::super::request::EngineRequest::Internal(InternalEvent::RepoSearchBatch(batch)) =
			request
		else {
			panic!("expected regex error batch");
		};
		assert_eq!(batch.generation, 12);
		assert!(!batch.searching);
		assert!(batch.results.is_empty());
		assert!(
			batch
				.error
				.as_deref()
				.is_some_and(|error| error.contains("Invalid"))
		);
	}

	#[test]
	fn superseded_work_cancels_before_walking_the_corpus() {
		let (_, _, internal, requests) = super::super::request::channel();
		let cancel = Arc::new(AtomicBool::new(true));
		spawn_search_debounce(7, cancel, internal).expect("debounce");
		std::thread::sleep(SEARCH_DEBOUNCE + Duration::from_millis(30));
		assert!(requests.try_recv().is_err());
	}

	#[test]
	fn composed_fixture_covers_every_literal_row_while_ranking_exact_over_fuzzy() {
		let cancel = AtomicBool::new(false);
		let files = [
			("src/a.rs", "fn render_frame() {}\nlet r_f = 1;\n"),
			(
				"src/b.rs",
				"render_frame();\n// render something fragmentary\n",
			),
		];
		let mut results = Vec::new();
		let mut matched = 0;
		let mut truncated = false;
		let matcher = LineMatcher::compile(RepoSearchMode::Fuzzy, "render_frame").expect("matcher");
		for (path, text) in files {
			search_file(
				&matcher,
				Path::new(path),
				Path::new(path),
				text.as_bytes(),
				None,
				&cancel,
				&mut results,
				&mut matched,
				&mut truncated,
			);
		}
		sort_and_cap(&mut results, &mut truncated);
		let literal = BTreeSet::from([
			("src/a.rs".to_string(), 0u64),
			("src/b.rs".to_string(), 0u64),
		]);
		let found = results
			.iter()
			.map(|result| (result.display_path.clone(), result.line.get()))
			.collect::<BTreeSet<_>>();
		assert!(literal.is_subset(&found));
		assert!(
			results
				.iter()
				.take(2)
				.all(|result| result.score > 2_000_000)
		);
	}

	#[test]
	fn fallback_worker_excludes_binary_and_oversized_files() {
		let directory = std::env::temp_dir().join(format!(
			"libaryon-fuzzy-fallback-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.expect("clock")
				.as_nanos()
		));
		std::fs::create_dir_all(&directory).expect("fixture directory");
		std::fs::write(directory.join("text.txt"), b"find_the_supervisor\n").expect("text");
		std::fs::write(directory.join("binary.bin"), b"find_the_supervisor\0hidden")
			.expect("binary");
		let oversized = std::fs::File::create(directory.join("oversized.txt")).expect("large");
		oversized
			.set_len(super::super::MAX_SEMANTIC_BYTES)
			.expect("size");
		drop(oversized);

		let (_, _, internal, requests) = super::super::request::channel();
		spawn_search_task(
			RepoSearchRequest {
				generation: 11,
				mode: RepoSearchMode::Fuzzy,
				query: "find_the_supervisor".to_string(),
				anchor: directory.clone(),
				live_files: Vec::new(),
				cancel: Arc::new(AtomicBool::new(false)),
				files_walked: Arc::new(AtomicU64::new(0)),
			},
			internal,
		)
		.expect("worker");
		let deadline = Instant::now() + Duration::from_secs(2);
		let final_batch = loop {
			let request = requests
				.recv_timeout(Duration::from_millis(100))
				.expect("search result");
			if let super::super::request::EngineRequest::Internal(InternalEvent::RepoSearchBatch(
				batch,
			)) = request && !batch.searching
			{
				break batch;
			}
			assert!(Instant::now() < deadline, "final batch timed out");
		};
		assert_eq!(final_batch.results.len(), 1);
		assert_eq!(final_batch.results[0].display_path, "text.txt");
		std::fs::remove_dir_all(directory).expect("cleanup");
	}

	#[test]
	#[ignore = "repository acceptance fixture; shells out to git and scans the worktree"]
	fn render_frame_repo_search_covers_the_literal_oracle() {
		let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
		let (root, files, corpus_truncated) = git_corpus(manifest).expect("Git worktree");
		assert!(!corpus_truncated, "fixture corpus unexpectedly truncated");
		let cancel = AtomicBool::new(false);
		let mut results = Vec::new();
		let mut matched = 0;
		let mut truncated = false;
		let mut literal = BTreeSet::new();
		let matcher = LineMatcher::compile(RepoSearchMode::Fuzzy, "render_frame").expect("matcher");
		for relative in files {
			let path = root.join(&relative);
			let Ok(bytes) = std::fs::read(&path) else {
				continue;
			};
			if bytes.len() as u64 >= super::super::MAX_SEMANTIC_BYTES || is_binary(&bytes) {
				continue;
			}
			if let Ok(text) = std::str::from_utf8(&bytes) {
				for (line, text) in text.lines().enumerate() {
					if text.contains("render_frame") {
						literal
							.insert((relative.to_string_lossy().replace('\\', "/"), line as u64));
					}
				}
			}
			search_file(
				&matcher,
				&relative,
				&path,
				&bytes,
				None,
				&cancel,
				&mut results,
				&mut matched,
				&mut truncated,
			);
		}
		sort_and_cap(&mut results, &mut truncated);
		let found = results
			.iter()
			.map(|result| (result.display_path.clone(), result.line.get()))
			.collect::<BTreeSet<_>>();
		assert!(
			literal.is_subset(&found),
			"literal rows absent from fuzzy result: {:?}",
			literal.difference(&found).collect::<Vec<_>>()
		);
	}
}
