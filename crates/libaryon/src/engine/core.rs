use crate::core::{DocByte, DocDisplayCol, DocLine, DocLineCount, StateId, TAB_SIZE};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::language::LanguageRegistry;
use crate::engine::settings::SaveDurability;
use crate::engine::ui_colors::UiColorOverrides;
use crate::engine::undo::TextDelta;
use crate::svp::SvpResolver;
use crate::svp::semantic::SemanticReactor;
use crate::uast::Viewport;
use regex_automata::meta::Regex;
use regex_automata::util::syntax;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

use super::event_subscribers::{SubscriberDispatcher, UnavailableSubscriberDispatcher};
use super::index::SumTree;
use super::jobs::{JobDispatcher, UnavailableJobDispatcher};
use super::minimap::next_registry_topology_revision;
use super::protocol::{CommandRange, SearchMatch};
use super::request::{EngineRequest, InternalSender};
use super::runtime_registry::{
	DynamicActionExecutor, DynamicActionOutcome, UnavailableActionExecutor,
};
use super::session::{CachedSumTree, EditorSession};
use super::support::line_byte_range;

pub(crate) const FILE_DEVICE_ID: u16 = 0x42;
pub(crate) const MAX_SEMANTIC_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_MINIMAP_TEXT_BYTES: u64 = 16 * 1024 * 1024;

#[cfg(test)]
type SubscriberSetup = Box<dyn FnOnce(&mut EditorSession) + Send>;

pub(super) fn advance_col(b: u8, line: &mut DocLine, col: &mut DocDisplayCol) {
	if b == b'\n' {
		*line += DocLineCount::new(1);
		*col = DocDisplayCol::ZERO;
	} else if b == b'\t' {
		*col = col
			.checked_add(TAB_SIZE - (col.get() % TAB_SIZE))
			.expect("DocDisplayCol addition overflow");
	} else {
		*col = col.checked_add(1).expect("DocDisplayCol addition overflow");
	}
}

pub(super) fn resolve_byte_range(
	range: &CommandRange,
	doc: &[u8],
	cursor_line: DocLine,
) -> Option<(usize, usize)> {
	match range {
		CommandRange::WholeFile => None,
		CommandRange::CurrentLine => Some(line_byte_range(doc, cursor_line, cursor_line)),
		CommandRange::SingleLine(n) => Some(line_byte_range(doc, *n, *n)),
		CommandRange::LineRange(a, b) => Some(line_byte_range(doc, *a, *b)),
		CommandRange::Visual {
			start_byte,
			end_byte,
		} => {
			let a = start_byte.get() as usize;
			let b = end_byte.get() as usize;
			let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
			let lo = lo.min(doc.len());
			let hi = hi.min(doc.len());
			let line_start = doc[..lo]
				.iter()
				.rposition(|&byte| byte == b'\n')
				.map(|p| p + 1)
				.unwrap_or(0);
			let line_end = doc[hi..]
				.iter()
				.position(|&byte| byte == b'\n')
				.map(|p| hi + p + 1)
				.unwrap_or(doc.len());
			Some((line_start, line_end))
		}
	}
}

pub(super) fn build_regex(pattern: &str, case_insensitive: bool) -> Result<Regex, String> {
	Regex::builder()
		.syntax(syntax::Config::new().case_insensitive(case_insensitive))
		.build(pattern)
		.map_err(|e| format!("Invalid regex: {}", e))
}

pub(super) fn find_all_matches(doc_bytes: &[u8], re: &Regex) -> Vec<SearchMatch> {
	let mut matches = Vec::new();
	let mut line = DocLine::ZERO;
	let mut col = DocDisplayCol::ZERO;
	let mut prev_pos = 0usize;

	for m in re.find_iter(doc_bytes) {
		for &b in &doc_bytes[prev_pos..m.start()] {
			advance_col(b, &mut line, &mut col);
		}
		let match_len = m.end() - m.start();
		matches.push(SearchMatch {
			line,
			col,
			byte_len: match_len,
		});
		for &b in &doc_bytes[m.start()..m.end()] {
			advance_col(b, &mut line, &mut col);
		}
		prev_pos = m.end();
	}
	matches
}

pub(super) fn substitute_text_delta(
	doc: &[u8],
	re: &Regex,
	replacement: &str,
	byte_range: Option<(usize, usize)>,
) -> Option<(TextDelta, usize)> {
	let (start, end) = byte_range.unwrap_or((0, doc.len()));
	let slice = &doc[start..end];
	let mut first_match_start = None;
	let mut last_match_end = 0usize;
	let mut previous_end = 0usize;
	let mut replacement_bytes = Vec::with_capacity(slice.len());
	let mut match_count = 0usize;

	for m in re.find_iter(slice) {
		let rel_start = m.start();
		let rel_end = m.end();
		if first_match_start.is_some() {
			replacement_bytes.extend_from_slice(&slice[previous_end..rel_start]);
		} else {
			first_match_start = Some(start + rel_start);
		}
		replacement_bytes.extend_from_slice(replacement.as_bytes());
		previous_end = rel_end;
		last_match_end = start + rel_end;
		match_count += 1;
	}

	let first_match_start = first_match_start?;
	Some((
		TextDelta {
			global_byte_offset: DocByte::new(first_match_start as u64),
			deleted_bytes: doc[first_match_start..last_match_end].to_vec(),
			inserted_bytes: replacement_bytes,
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		},
		match_count,
	))
}

pub(crate) fn refresh_sum_tree<'a>(
	cache: &'a mut Option<CachedSumTree>,
	registry: &UastRegistry,
	root: NodeId,
	state_id: StateId,
) -> &'a SumTree {
	let observed_total_lines = registry.get_total_newlines(root);
	let topology_revision = next_registry_topology_revision(registry);
	let total_lines = if registry.exact_line_count(root).is_none() {
		cache
			.as_ref()
			.filter(|cached| cached.root == root && cached.topology_revision == topology_revision)
			.map_or(observed_total_lines, |cached| cached.total_lines)
	} else {
		observed_total_lines
	};
	let total_bytes = registry.get_total_bytes(root);
	let can_retain_cold_tree = cache
		.as_ref()
		.is_some_and(|cached| cached.root == root && cached.tree.has_cold_extents());
	let needs_rebuild = match cache {
		Some(cached) => {
			!can_retain_cold_tree
				&& (cached.root != root
					|| cached.state_id != state_id
					|| cached.total_lines != total_lines
					|| cached.total_bytes != total_bytes
					|| cached.topology_revision != topology_revision)
		}
		None => true,
	};

	if needs_rebuild {
		registry
			.sum_tree_rebuilds
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		*cache = Some(CachedSumTree {
			root,
			state_id,
			total_lines,
			total_bytes,
			topology_revision,
			tree: SumTree::build_from_registry(registry, root),
		});
	} else if let Some(cached) = cache.as_mut() {
		// A tree with cold extents is only a sentinel that makes layout use the
		// production sparse coordinate index. Its per-extent entries are not
		// queried, so edits and root-child splices can update the cheap cache
		// metadata without rebuilding an O(extents) structure per keystroke.
		cached.state_id = state_id;
		cached.total_lines = total_lines;
		cached.total_bytes = total_bytes;
		cached.topology_revision = topology_revision;
	}

	&cache.as_ref().expect("sum tree must be cached").tree
}

pub(crate) struct Engine {
	pub(super) registry: Arc<UastRegistry>,
	pub(super) resolver: Arc<SvpResolver>,
	pub(super) rx_request: mpsc::Receiver<EngineRequest>,
	pub(super) tx_internal: InternalSender,
	pub(super) tx_view: mpsc::Sender<Viewport>,
	/// Ordered, non-coalescing frontend stream; absent when no host attached.
	pub(super) host_events: Option<super::host_event::HostEventSender>,
	pub(super) reactor: SemanticReactor,
	pub(super) lang_registry: Arc<LanguageRegistry>,
	pub(super) initial_theme_name: String,
	pub(super) initial_minimap_enabled: bool,
	pub(super) initial_wrap_enabled: bool,
	pub(super) initial_line_numbers_enabled: bool,
	pub(super) initial_relative_line_numbers_enabled: bool,
	pub(super) initial_visible_whitespace: bool,
	pub(super) initial_reload_enabled: bool,
	pub(super) initial_save_durability: SaveDurability,
	pub(super) initial_ui_colors: UiColorOverrides,
	pub(super) initial_language_themes: BTreeMap<String, String>,
	pub(super) initial_file: Option<String>,
	pub(super) rune_host: Option<super::rune_host::RuneHostConfig>,
	pub(super) terminate: Arc<AtomicBool>,
	pub(super) dynamic_action_executor:
		Box<dyn DynamicActionExecutor<EditorSession, Output = DynamicActionOutcome> + Send>,
	pub(super) subscriber_dispatcher: Box<dyn SubscriberDispatcher<EditorSession> + Send>,
	pub(super) job_dispatcher: Box<dyn JobDispatcher<EditorSession> + Send>,
	#[cfg(test)]
	pub(super) event_tap: Option<mpsc::Sender<super::event_bus::EngineEvent>>,
	#[cfg(test)]
	pub(super) subscriber_setup: Option<SubscriberSetup>,
}

impl Engine {
	pub(crate) fn new(
		registry: Arc<UastRegistry>,
		resolver: Arc<SvpResolver>,
		rx_request: mpsc::Receiver<EngineRequest>,
		tx_internal: InternalSender,
		tx_view: mpsc::Sender<Viewport>,
		lang_registry: Arc<LanguageRegistry>,
		initial_theme_name: String,
		initial_minimap_enabled: bool,
		initial_wrap_enabled: bool,
		initial_line_numbers_enabled: bool,
		initial_relative_line_numbers_enabled: bool,
		initial_visible_whitespace: bool,
		initial_reload_enabled: bool,
		initial_save_durability: SaveDurability,
		initial_ui_colors: UiColorOverrides,
		initial_language_themes: BTreeMap<String, String>,
	) -> Self {
		let reactor = SemanticReactor::new(tx_internal.clone(), lang_registry.clone());
		Self {
			registry,
			resolver,
			rx_request,
			tx_internal,
			tx_view,
			host_events: None,
			reactor,
			lang_registry,
			initial_theme_name,
			initial_minimap_enabled,
			initial_wrap_enabled,
			initial_line_numbers_enabled,
			initial_relative_line_numbers_enabled,
			initial_visible_whitespace,
			initial_reload_enabled,
			initial_save_durability,
			initial_ui_colors,
			initial_language_themes,
			initial_file: None,
			rune_host: None,
			terminate: Arc::new(AtomicBool::new(false)),
			dynamic_action_executor: Box::new(UnavailableActionExecutor),
			subscriber_dispatcher: Box::new(UnavailableSubscriberDispatcher),
			job_dispatcher: Box::new(UnavailableJobDispatcher),
			#[cfg(test)]
			event_tap: None,
			#[cfg(test)]
			subscriber_setup: None,
		}
	}

	#[cfg(test)]
	pub(crate) fn with_event_tap(
		mut self,
		tap: mpsc::Sender<super::event_bus::EngineEvent>,
	) -> Self {
		self.event_tap = Some(tap);
		self
	}

	#[cfg(test)]
	pub(crate) fn with_subscriber_setup(
		mut self,
		setup: impl FnOnce(&mut EditorSession) + Send + 'static,
	) -> Self {
		self.subscriber_setup = Some(Box::new(setup));
		self
	}

	pub(crate) fn with_initial_file(mut self, initial_file: Option<String>) -> Self {
		self.initial_file = initial_file;
		self
	}

	pub(crate) fn with_host_events(
		mut self,
		host_events: super::host_event::HostEventSender,
	) -> Self {
		self.host_events = Some(host_events);
		self
	}

	/// Emit an ordered frontend event. A departed frontend is an ordinary
	/// shutdown condition, so delivery failure is deliberately ignored.
	#[allow(dead_code)]
	pub(super) fn emit_host_event(&self, event: super::host_event::HostEvent) {
		if let Some(sender) = self.host_events.as_ref() {
			let _ = sender.send(event);
		}
	}

	pub(crate) fn with_rune_host_config(
		mut self,
		config: Option<super::rune_host::RuneHostConfig>,
	) -> Self {
		self.rune_host = config;
		self
	}

	/// Install the language-neutral executor used by resolved runtime actions.
	/// The Rune lifecycle stage supplies the first production implementation.
	#[allow(dead_code)]
	pub(crate) fn with_dynamic_action_executor<E>(mut self, executor: E) -> Self
	where
		E: DynamicActionExecutor<EditorSession, Output = DynamicActionOutcome> + Send + 'static,
	{
		self.dynamic_action_executor = Box::new(executor);
		self
	}

	#[allow(dead_code)]
	pub(crate) fn with_subscriber_dispatcher<D>(mut self, dispatcher: D) -> Self
	where
		D: SubscriberDispatcher<EditorSession> + Send + 'static,
	{
		self.subscriber_dispatcher = Box::new(dispatcher);
		self
	}

	#[allow(dead_code)]
	pub(crate) fn with_job_dispatcher<D>(mut self, dispatcher: D) -> Self
	where
		D: JobDispatcher<EditorSession> + Send + 'static,
	{
		self.job_dispatcher = Box::new(dispatcher);
		self
	}

	pub(crate) fn termination_handle(&self) -> Arc<AtomicBool> {
		self.terminate.clone()
	}

	pub(crate) fn run(self) {
		super::runtime::run_engine(self);
	}
}

#[cfg(test)]
mod tests {
	use super::{refresh_sum_tree, resolve_byte_range, substitute_text_delta};
	use crate::core::{DocByte, DocDisplayCol, DocLine, StateId};
	use crate::ecs::UastRegistry;
	use crate::engine::protocol::CommandRange;
	use crate::engine::support::line_end_visual_col;
	use crate::engine::test_support::build_grafted_rust_document;
	use crate::svp::SvpPointer;
	use crate::uast::{SemanticKind, SpanMetrics};
	use crate::uast::{UastMutation, UastProjection};
	use regex_automata::meta::Regex;

	#[test]
	fn visual_range_snaps_mid_line_bytes_outward_to_full_lines() {
		// Three 5-byte lines: "aaaa\nbbbb\ncccc\n". Bytes 2..7 start inside
		// line 0 and end inside line 1, so the snapped range must cover the
		// whole of both lines including their trailing newlines.
		let doc = b"aaaa\nbbbb\ncccc\n";
		let range = CommandRange::Visual {
			start_byte: DocByte::new(2),
			end_byte: DocByte::new(7),
		};
		let snapped = resolve_byte_range(&range, doc, DocLine::ZERO);
		assert_eq!(snapped, Some((0, 10)));
		assert_eq!(&doc[0..10], b"aaaa\nbbbb\n");
	}

	#[test]
	fn visual_range_accepts_reversed_byte_pair() {
		// Selection built from a cursor that moved backward from the anchor.
		let doc = b"alpha\nbeta\ngamma\n";
		let range = CommandRange::Visual {
			start_byte: DocByte::new(13),
			end_byte: DocByte::new(1),
		};
		let snapped = resolve_byte_range(&range, doc, DocLine::ZERO);
		assert_eq!(snapped, Some((0, 17)));
	}

	#[test]
	fn grafted_rust_cursor_lookup_can_reach_mid_line_columns() {
		let source = include_str!("structural_graft.rs");
		let (registry, root) = build_grafted_rust_document(source);

		let target_line = DocLine::new(
			source
				.lines()
				.position(|line| line.contains("let mut result = Vec::new();"))
				.expect("target code line should exist") as u64,
		);
		let target_col = DocDisplayCol::new(20);

		let target =
			registry.find_node_at_line_col(root, &crate::uast::NoFolds, target_line, target_col);

		assert_eq!(target.display_col, target_col);
	}

	#[test]
	fn grafted_main_rs_cursor_lookup_reaches_past_path_prefix() {
		let source = include_str!("fixtures/main.rs");
		let (registry, root) = build_grafted_rust_document(source);

		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(1),
			DocDisplayCol::new(12),
		);

		assert_eq!(target.display_col, DocDisplayCol::new(12));
	}

	#[test]
	fn grafted_clipboard_cursor_lookup_clamps_blank_space_on_signature_line() {
		let source = include_str!("clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);
		let line = source.lines().nth(7).expect("line 8 should exist");
		let expected_end = line_end_visual_col(line.as_bytes(), DocLine::ZERO);

		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(7),
			DocDisplayCol::new(37),
		);

		assert_eq!(target.display_col, expected_end);
	}

	#[test]
	fn grafted_clipboard_cursor_lookup_reaches_mid_expression_columns() {
		let source = include_str!("clipboard.rs");
		let (registry, root) = build_grafted_rust_document(source);

		let target = registry.find_node_at_line_col(
			root,
			&crate::uast::NoFolds,
			DocLine::new(9),
			DocDisplayCol::new(30),
		);

		assert_eq!(target.display_col, DocDisplayCol::new(30));
	}

	#[test]
	fn residency_churn_does_not_rebuild_the_cached_sum_tree() {
		let registry = UastRegistry::new(2);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 1,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 1,
			},
		);
		chunk.append_local_child(root, leaf);

		let mut cache = None;
		refresh_sum_tree(&mut cache, &registry, root, StateId::ZERO);
		let initial_rebuilds = registry.stats().sum_tree_rebuilds;
		registry.hot_swap_virtual_data(leaf, b"a\nb\n".to_vec());
		refresh_sum_tree(&mut cache, &registry, root, StateId::ZERO);
		registry.drop_resident_data(leaf);
		refresh_sum_tree(&mut cache, &registry, root, StateId::ZERO);

		assert_eq!(registry.stats().sum_tree_rebuilds, initial_rebuilds);
	}

	#[test]
	fn cold_document_keystroke_does_not_rebuild_the_extent_wide_sum_tree() {
		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 1024 * 1024,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 1024 * 1024,
				device_id: 1,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 1024 * 1024,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);

		let mut cache = None;
		refresh_sum_tree(&mut cache, &registry, root, StateId::ZERO);
		assert!(cache.as_ref().expect("cold cache").tree.has_cold_extents());
		let initial_rebuilds = registry.stats().sum_tree_rebuilds;
		registry.hot_swap_virtual_data(leaf, vec![b'x'; 1024 * 1024]);
		registry.metrics_inflated[leaf.index()].store(true, std::sync::atomic::Ordering::Release);
		registry.set_terminal_newline(leaf, false);
		let (cursor_node, cursor_offset) = registry.insert_text(leaf, 1024 * 1024, b"!");
		assert_ne!(cursor_node, leaf);
		assert_eq!(cursor_offset, 1);

		refresh_sum_tree(&mut cache, &registry, root, StateId::new(1));

		assert_eq!(
			registry.stats().sum_tree_rebuilds,
			initial_rebuilds,
			"cold layout uses the coordinate index, so a keystroke must not walk every extent"
		);
	}

	#[test]
	fn bulk_substitute_delta_collapses_multiple_matches_into_one_edit() {
		let doc = b"alpha beta gamma beta omega";
		let re = Regex::new("beta").expect("regex");
		let (delta, count) =
			substitute_text_delta(doc, &re, "BETA", None).expect("expected substitutions");

		assert_eq!(count, 2);
		assert_eq!(delta.global_byte_offset, DocByte::new(6));
		assert_eq!(delta.deleted_bytes, b"beta gamma beta");
		assert_eq!(delta.inserted_bytes, b"BETA gamma BETA");
	}

	#[test]
	fn bulk_substitute_preserves_non_utf8_bytes_between_matches() {
		let doc = b"beta\xffbeta";
		let re = Regex::new("beta").expect("regex");
		let (delta, count) =
			substitute_text_delta(doc, &re, "X", None).expect("expected substitutions");

		assert_eq!(count, 2);
		assert_eq!(delta.deleted_bytes, doc);
		assert_eq!(delta.inserted_bytes, b"X\xffX");
	}
}
