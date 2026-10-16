use super::clipboard::ClipboardHandle;
use super::document::cursor::line_col_from_doc_byte_sparse;
use super::event_bus::{EngineEvent, EngineEventQueue};
use super::event_subscribers::{BaselineBuilder, BaselineError, SubscriberRuntime};
use super::folding::{
	FoldEdit, FoldSet, FoldSnapshot, snap_line_to_visible_boundary, unfold_ancestor_chain,
};
use super::index::SumTree;
use super::input::InputRuntime;
use super::jobs::{JobRuntime, JobSpawnError, JobSpec};
use super::layout::{CaretIndex, CaretLookup, GutterNumbering};
use super::minimap::MinimapSnapshot;
use super::protocol::{
	CommandRange, EditorMode, HostPanelGroupLayout, HostPanelGroupMember, HostPanelGroupShape,
	PaneDividerReservation, PaneDropEdge, PaneFocusDirection, PaneResizeDirection, SearchMatch,
	SplitDirection, SubstituteFlags, UastInspectorMode, VisualKind,
};
use super::runtime_registry::{RegistrationBatch, RegistryError, RuntimeRegistries};
use super::shadow::ShadowSession;
use super::undo::{GenerationTarget, UndoLedger};
use crate::core::{
	BufferId, DEFAULT_VIEWPORT_LINES, DEFAULT_VIEWPORT_WIDTH, DisplayRow, DocByte, DocDisplayCol,
	DocLine, DocumentId, JobId, PaneCol, PaneId, PaneRect, PaneRow, RegistryOwnerId, RegistryRev,
	RequestId, STATUS_BAR_ROWS, SavedSession, ScreenCol, ScreenRow, StateId, TopologyRev, ViewId,
	ViewportId, Writable,
};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::Theme;
use crate::engine::host_event::{
	HostPanelDescriptor, HostPanelGroupId, HostPanelId, PanelRevision,
};
use crate::engine::language::{AnalysisDepth, FILE_BROWSER_THEME_ID};
use crate::engine::settings::{EngineSettings, SaveDurability};
use crate::engine::ui_colors::UiColorOverrides;
use crate::svp::BackingIdentity;
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;
use crate::uast::UastProjection;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub(crate) struct CachedSumTree {
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) total_lines: u64,
	pub(crate) total_bytes: u64,
	pub(crate) topology_revision: u64,
	pub(crate) tree: SumTree,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FileBrowserRowKind {
	Directory { expanded: bool },
	File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileBrowserRow {
	pub(crate) path: PathBuf,
	pub(crate) depth: u16,
	pub(crate) kind: FileBrowserRowKind,
}

#[derive(Debug)]
pub(crate) struct FileBrowserState {
	pub(crate) root_path: PathBuf,
	pub(crate) expanded_dirs: HashSet<PathBuf>,
	pub(crate) visible_rows: Vec<FileBrowserRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct MarkdownImagePreviewState {
	pub(crate) source_buffer_id: BufferId,
	pub(crate) source_view_id: ViewId,
	pub(crate) serif: bool,
	pub(crate) source_path: Option<String>,
	pub(crate) source_state_id: StateId,
	pub(crate) source_text: String,
	pub(crate) source_highlights: Vec<crate::svp::rust_highlight::HighlightSpan>,
	pub(crate) source_theme_colors:
		[Option<crate::core::Rgb>; crate::svp::rust_highlight::CATEGORY_COUNT],
	pub(crate) source_semantic_revision: u64,
	pub(crate) source_cells: Vec<crate::uast::projection::MarkdownCellSnapshot>,
	pub(crate) source_scroll_line: DocLine,
	pub(crate) source_viewport_line_count: u32,
	/// A pinned preview renders a fixed in-memory document (e.g. :help): it is
	/// never refreshed from its source pane and handles scrolling itself.
	pub(crate) pinned: bool,
}

pub(crate) struct ConfirmState {
	pub(crate) replacement: String,
	pub(crate) replacements_done: u32,
	pub(crate) total_matches: u32,
	pub(crate) flags: SubstituteFlags,
	pub(crate) range: CommandRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UastInspectorStamp {
	pub(crate) source_buffer_id: BufferId,
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) topology_rev: TopologyRev,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UastInspectorNodeKey {
	pub(crate) start_byte: u64,
	pub(crate) end_byte: u64,
	pub(crate) provider_kind: crate::uast::ProviderKindId,
	pub(crate) ancestry: Vec<crate::uast::ProviderKindId>,
}

#[derive(Debug, Clone)]
pub(crate) struct UastInspectorRow {
	pub(crate) line: DocLine,
	pub(crate) line_start_byte: DocByte,
	pub(crate) node_id: NodeId,
	pub(crate) start_byte: DocByte,
	pub(crate) end_byte: DocByte,
	pub(crate) provider_kind: crate::uast::ProviderKindId,
	pub(crate) ancestry: Vec<crate::uast::ProviderKindId>,
	pub(crate) depth: u16,
	pub(crate) child_count: usize,
	pub(crate) contains_cursor: bool,
	pub(crate) stamp: UastInspectorStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UastIndexInspectorTarget {
	RootExtent { node_id: NodeId, rank: usize },
	SumTreeEntry { rank: usize, leaf: u32, slot: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct UastIndexInspectorRow {
	pub(crate) line: DocLine,
	pub(crate) line_start_byte: DocByte,
	pub(crate) start_byte: DocByte,
	pub(crate) end_byte: DocByte,
	pub(crate) contains_cursor: bool,
	pub(crate) stamp: UastInspectorStamp,
	pub(crate) index_revision: u64,
	pub(crate) target: UastIndexInspectorTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UastSumTreeCacheKey {
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) total_lines: u64,
	pub(crate) total_bytes: u64,
	pub(crate) topology_revision: u64,
}

impl From<&CachedSumTree> for UastSumTreeCacheKey {
	fn from(cache: &CachedSumTree) -> Self {
		Self {
			root: cache.root,
			state_id: cache.state_id,
			total_lines: cache.total_lines,
			total_bytes: cache.total_bytes,
			topology_revision: cache.topology_revision,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UastIndexRefreshIdentity {
	Treap {
		revision: u64,
	},
	SumTree {
		cache: Option<UastSumTreeCacheKey>,
		rebuilds: u64,
	},
}

impl UastInspectorRow {
	pub(crate) fn key(&self) -> UastInspectorNodeKey {
		UastInspectorNodeKey {
			start_byte: self.start_byte.get(),
			end_byte: self.end_byte.get(),
			provider_kind: self.provider_kind,
			ancestry: self.ancestry.clone(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UastInspectorRefreshKey {
	pub(crate) source_buffer_id: BufferId,
	pub(crate) source_view_id: Option<ViewId>,
	pub(crate) source_name: String,
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) topology_rev: TopologyRev,
	pub(crate) applied_structural_state_id: Option<StateId>,
	pub(crate) pending_semantic: bool,
	pub(crate) structural_issue: Option<StructuralIssue>,
	pub(crate) active_language_id: Option<super::language::LanguageId>,
	pub(crate) cursor_byte: Option<DocByte>,
	pub(crate) mode: UastInspectorMode,
	pub(crate) fold_revision: u64,
	pub(crate) follow_cursor: bool,
	pub(crate) owner_accounting: UastOwnerAccounting,
	pub(crate) pane_width: u16,
	pub(crate) pane_lines: u32,
	pub(crate) snapshot_node_budget: usize,
	pub(crate) registry_revision: u64,
	pub(crate) reclaim_pending_heads: usize,
	pub(crate) index_identity: Option<UastIndexRefreshIdentity>,
}

#[derive(Debug)]
pub(crate) struct UastInspectorPanel {
	pub(crate) source_buffer_id: BufferId,
	pub(crate) source_view_id: Option<ViewId>,
	pub(crate) mode: UastInspectorMode,
	pub(crate) source_root: Option<NodeId>,
	pub(crate) source_state_id: Option<StateId>,
	pub(crate) source_topology_rev: Option<TopologyRev>,
	pub(crate) rows: Vec<UastInspectorRow>,
	pub(crate) index_rows: Vec<UastIndexInspectorRow>,
	pub(crate) folded: HashSet<UastInspectorNodeKey>,
	pub(crate) fold_revision: u64,
	pub(crate) follow_cursor: bool,
	pub(crate) last_refresh_key: Option<UastInspectorRefreshKey>,
	pub(crate) graph: Option<crate::uast::UastGraphSnapshot>,
	pub(crate) rendered_text: Vec<u8>,
	pub(crate) snapshot_node_budget: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UastOwnerAccounting {
	pub(crate) allocated_nodes: u64,
	pub(crate) retired_nodes: u64,
	pub(crate) current_live_nodes: u64,
	pub(crate) parked_nodes: u64,
	pub(crate) payload_added_bytes: u64,
	pub(crate) payload_removed_bytes: u64,
	pub(crate) current_live_payload_bytes: u64,
	pub(crate) parked_payload_bytes: u64,
	pub(crate) root_generations: u32,
}

impl UastOwnerAccounting {
	pub(crate) const fn new(initial_nodes: u64) -> Self {
		Self {
			allocated_nodes: initial_nodes,
			retired_nodes: 0,
			current_live_nodes: initial_nodes,
			parked_nodes: 0,
			payload_added_bytes: 0,
			payload_removed_bytes: 0,
			current_live_payload_bytes: 0,
			parked_payload_bytes: 0,
			root_generations: 1,
		}
	}

	fn assert_invariant(&self) {
		assert_eq!(
			self.allocated_nodes
				.checked_sub(self.retired_nodes)
				.expect("UAST owner retirement exceeds allocation"),
			self.current_live_nodes
				.checked_add(self.parked_nodes)
				.expect("UAST owner live + parked overflow"),
			"UAST owner allocation algebra diverged"
		);
		assert_eq!(
			self.payload_added_bytes
				.checked_sub(self.payload_removed_bytes)
				.expect("UAST owner payload removal exceeds additions"),
			self.current_live_payload_bytes
				.checked_add(self.parked_payload_bytes)
				.expect("UAST owner live + parked payload overflow"),
			"UAST owner payload algebra diverged"
		);
	}

	fn record_transaction(
		&mut self,
		allocated: u64,
		retired: u64,
		payload_added: u64,
		payload_physically_removed: u64,
		payload_orphaned: u64,
	) {
		let available = self
			.current_live_nodes
			.checked_add(allocated)
			.expect("UAST owner live allocation overflow");
		assert!(
			retired <= available,
			"UAST owner retirement exceeds its live and newly allocated nodes"
		);
		self.allocated_nodes = self
			.allocated_nodes
			.checked_add(allocated)
			.expect("UAST owner allocated counter overflow");
		self.retired_nodes = self
			.retired_nodes
			.checked_add(retired)
			.expect("UAST owner retired counter overflow");
		self.current_live_nodes = available - retired;
		self.record_payload_transaction(
			payload_added,
			payload_physically_removed,
			payload_orphaned,
		);
		self.assert_invariant();
	}

	pub(crate) fn record_payload_transaction(
		&mut self,
		added: u64,
		physically_removed: u64,
		orphaned: u64,
	) {
		let available = self
			.current_live_payload_bytes
			.checked_add(added)
			.expect("UAST owner live payload addition overflow");
		let removed = physically_removed
			.checked_add(orphaned)
			.expect("UAST owner payload removal transaction overflow");
		assert!(
			removed <= available,
			"UAST owner payload removal exceeds its live and newly added bytes"
		);
		self.payload_added_bytes = self
			.payload_added_bytes
			.checked_add(added)
			.expect("UAST owner payload-added counter overflow");
		self.payload_removed_bytes = self
			.payload_removed_bytes
			.checked_add(removed)
			.expect("UAST owner payload-removed counter overflow");
		self.current_live_payload_bytes = available - removed;
		self.assert_invariant();
	}

	fn record_parked_retirement(&mut self, retired: u64, payload_retired: u64) {
		assert!(
			retired <= self.parked_nodes,
			"UAST parked retirement exceeds parked nodes"
		);
		self.parked_nodes -= retired;
		self.retired_nodes = self
			.retired_nodes
			.checked_add(retired)
			.expect("UAST owner retired counter overflow");
		assert!(
			payload_retired <= self.parked_payload_bytes,
			"UAST parked payload retirement exceeds parked bytes"
		);
		self.parked_payload_bytes -= payload_retired;
		self.payload_removed_bytes = self
			.payload_removed_bytes
			.checked_add(payload_retired)
			.expect("UAST owner payload-removed counter overflow");
		self.assert_invariant();
	}

	fn record_root_transition(
		&mut self,
		allocated: u64,
		ordinary_retired: u64,
		payload_added: u64,
		payload_physically_removed: u64,
		ordinary_orphaned_payload: u64,
		transition: RootTransition,
	) {
		assert_eq!(
			ordinary_retired, 0,
			"root replacement mixed ordinary retirement with a typed transition"
		);
		assert_eq!(
			ordinary_orphaned_payload, 0,
			"root replacement mixed ordinary payload retirement with a typed transition"
		);
		self.allocated_nodes = self
			.allocated_nodes
			.checked_add(allocated)
			.expect("UAST owner allocated counter overflow");
		self.payload_added_bytes = self
			.payload_added_bytes
			.checked_add(payload_added)
			.expect("UAST owner payload-added counter overflow");
		self.payload_removed_bytes = self
			.payload_removed_bytes
			.checked_add(payload_physically_removed)
			.expect("UAST owner payload-removed counter overflow");
		let incoming_payload = payload_added
			.checked_sub(payload_physically_removed)
			.expect("root replacement physically removed more payload than it added");
		let old_live = self.current_live_nodes;
		let old_live_payload = self.current_live_payload_bytes;
		match transition {
			RootTransition::Retire => {
				self.retired_nodes = self
					.retired_nodes
					.checked_add(old_live)
					.expect("UAST owner retired counter overflow");
				self.current_live_nodes = allocated;
				self.payload_removed_bytes = self
					.payload_removed_bytes
					.checked_add(old_live_payload)
					.expect("UAST owner payload-removed counter overflow");
				self.current_live_payload_bytes = incoming_payload;
			}
			RootTransition::Park {
				revived_live: None,
				revived_payload: None,
			} => {
				self.parked_nodes = self
					.parked_nodes
					.checked_add(old_live)
					.expect("UAST owner parked counter overflow");
				self.current_live_nodes = allocated;
				self.parked_payload_bytes = self
					.parked_payload_bytes
					.checked_add(old_live_payload)
					.expect("UAST owner parked-payload counter overflow");
				self.current_live_payload_bytes = incoming_payload;
			}
			RootTransition::Park {
				revived_live: Some(revived),
				revived_payload: Some(revived_payload),
			} => {
				assert_eq!(
					allocated, 0,
					"reviving a parked generation unexpectedly allocated nodes"
				);
				assert_eq!(
					payload_added, 0,
					"reviving a parked generation unexpectedly added authoritative payload"
				);
				assert_eq!(
					payload_physically_removed, 0,
					"reviving a parked generation unexpectedly removed authoritative payload"
				);
				assert!(
					revived <= self.parked_nodes,
					"revived generation exceeds parked nodes"
				);
				self.parked_nodes = self
					.parked_nodes
					.checked_add(old_live)
					.and_then(|parked| parked.checked_sub(revived))
					.expect("UAST parked transition overflow/underflow");
				self.current_live_nodes = revived;
				assert!(
					revived_payload <= self.parked_payload_bytes,
					"revived generation exceeds parked payload bytes"
				);
				self.parked_payload_bytes = self
					.parked_payload_bytes
					.checked_add(old_live_payload)
					.and_then(|parked| parked.checked_sub(revived_payload))
					.expect("UAST parked-payload transition overflow/underflow");
				self.current_live_payload_bytes = revived_payload;
			}
			RootTransition::Park { .. } => {
				panic!("parked node and payload revival must be both present or both absent")
			}
		}
		self.root_generations = self
			.root_generations
			.checked_add(1)
			.expect("UAST owner root generation overflow");
		self.assert_invariant();
	}
}

pub(crate) const GENERATION_PARK_CAP: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootTransition {
	Retire,
	Park {
		revived_live: Option<u64>,
		revived_payload: Option<u64>,
	},
}

#[derive(Debug)]
pub(crate) struct ParkedGenerationRoot {
	pub(crate) target: GenerationTarget,
	pub(crate) root: NodeId,
	pub(crate) device_id: Option<u16>,
	pub(crate) live_nodes: u64,
	pub(crate) payload_bytes: u64,
	pub(crate) file_identity: BackingIdentity,
}

#[derive(Debug, Default)]
pub(crate) struct ParkedGenerationCache {
	entries: VecDeque<ParkedGenerationRoot>,
}

impl ParkedGenerationCache {
	pub(crate) fn take(&mut self, target: &GenerationTarget) -> Option<ParkedGenerationRoot> {
		let index = self.entries.iter().position(|entry| {
			entry.target.generation_id == target.generation_id && entry.target.path == target.path
		})?;
		self.entries.remove(index)
	}

	pub(crate) fn restore(&mut self, entry: ParkedGenerationRoot) {
		assert!(
			self.entries.len() < GENERATION_PARK_CAP,
			"restoring a removed parked generation must preserve cache capacity"
		);
		self.entries.push_back(entry);
	}

	pub(crate) fn insert(&mut self, entry: ParkedGenerationRoot) -> Option<ParkedGenerationRoot> {
		assert!(
			self.entries.iter().all(|cached| {
				cached.target.generation_id != entry.target.generation_id
					|| cached.target.path != entry.target.path
			}),
			"active generation must not already be parked"
		);
		self.entries.push_back(entry);
		(self.entries.len() > GENERATION_PARK_CAP)
			.then(|| self.entries.pop_front().expect("overflow has an LRU entry"))
	}

	pub(crate) fn remove_targets(
		&mut self,
		targets: &[GenerationTarget],
	) -> Vec<ParkedGenerationRoot> {
		let mut removed = Vec::new();
		self.entries.retain(|entry| {
			let matches = targets.iter().any(|target| {
				target.generation_id == entry.target.generation_id
					&& target.path == entry.target.path
			});
			if matches {
				removed.push(ParkedGenerationRoot {
					target: entry.target.clone(),
					root: entry.root,
					device_id: entry.device_id,
					live_nodes: entry.live_nodes,
					payload_bytes: entry.payload_bytes,
					file_identity: entry.file_identity,
				});
			}
			!matches
		});
		removed
	}

	pub(crate) fn drain(&mut self) -> Vec<ParkedGenerationRoot> {
		self.entries.drain(..).collect()
	}

	pub(crate) fn live_nodes(&self) -> u64 {
		self.entries.iter().fold(0u64, |total, entry| {
			total
				.checked_add(entry.live_nodes)
				.expect("parked generation live-node sum overflow")
		})
	}

	pub(crate) fn payload_bytes(&self) -> u64 {
		self.entries.iter().fold(0u64, |total, entry| {
			total
				.checked_add(entry.payload_bytes)
				.expect("parked generation payload-byte sum overflow")
		})
	}

	#[cfg(test)]
	pub(crate) fn entries(&self) -> &VecDeque<ParkedGenerationRoot> {
		&self.entries
	}
}

pub(crate) struct TextBuffer {
	pub(crate) root_id: NodeId,
	pub(crate) uast_accounting: UastOwnerAccounting,
	pub(crate) parked_generations: ParkedGenerationCache,
	pub(crate) pending_root_transition: Option<RootTransition>,
	pub(crate) pending_parked_retirements: u64,
	pub(crate) pending_parked_payload_retirements: u64,
	pub(crate) topology_rev: TopologyRev,
	pub(crate) file_path: Option<PathBuf>,
	/// Encrypted-at-rest backing owned by libaryon. This is intentionally
	/// distinct from `file_path`: the latter always means plaintext I/O.
	pub(crate) encrypted_backing: Option<PathBuf>,
	/// File-path-shaped identity for an unsaved in-memory document (e.g.
	/// `statement.sql`). Feeds language services that expect a path without
	/// claiming persistence: `file_path` stays `None` and writes still refuse.
	pub(crate) virtual_path: Option<String>,
	canonical_path: Option<CanonicalPath>,
	pub(crate) backing_path: Option<PathBuf>,
	/// Resolver device owned by the current reachable document root.
	///
	/// Replaced roots remain allocated in the registry, but are unreachable and
	/// must never be read after this device is retired.
	pub(crate) backing_device_id: Option<u16>,
	pub(crate) file_stamp: Option<FileStamp>,
	pub(crate) active_language_id: Option<super::language::LanguageId>,
	pub(crate) language_selection: LanguageSelection,
	pub(crate) shadow: Option<ShadowSession>,
	pub(crate) ledger: UndoLedger,
	pub(crate) search_matches: Vec<SearchMatch>,
	search_revision: u64,
	search_pattern_revision: u64,
	pub(crate) navigation_anchors: super::navigation::NavigationAnchorStore,
	pub(crate) semantic: Box<SemanticState>,
}

/// Authored provenance for a document's resolved language.
///
/// `active_language_id` is deliberately derived runtime state. This value is
/// the authority that can cross a saved-session boundary without freezing
/// extension-based detection or losing an explicit plaintext choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LanguageSelection {
	Auto,
	ExplicitPlaintext,
	Explicit(String),
}

pub(crate) struct RestoredDocument {
	pub(crate) document_id: DocumentId,
	pub(crate) bytes: Vec<u8>,
	pub(crate) file_path: Option<PathBuf>,
	pub(crate) virtual_path: Option<String>,
	pub(crate) file_stamp: Option<FileStamp>,
	pub(crate) language_selection: LanguageSelection,
	pub(crate) active_language_id: Option<super::language::LanguageId>,
	pub(crate) dirty: bool,
}

impl TextBuffer {
	pub(crate) fn canonical_path(&self) -> Option<&CanonicalPath> {
		self.canonical_path.as_ref()
	}

	pub(crate) fn new_unnamed(registry: &UastRegistry) -> Result<(Self, NodeId), String> {
		Self::new_from_bytes(registry, Vec::new())
	}

	pub(crate) fn new_from_bytes(
		registry: &UastRegistry,
		bytes: Vec<u8>,
	) -> Result<(Self, NodeId), String> {
		let payload_bytes = bytes.len() as u64;
		let metrics = crate::uast::SpanMetrics {
			byte_length: bytes.len() as u64,
			newlines: bytes.iter().filter(|byte| **byte == b'\n').count() as u64,
		};
		let mut chunk = registry
			.reserve_chunk(2)
			.ok_or_else(|| registry.capacity_refusal_message("Document creation", 2))?;
		let root = chunk.spawn_node(crate::uast::SemanticKind::RelationalTable, None, metrics);
		let leaf = chunk.spawn_node(crate::uast::SemanticKind::Token, None, metrics);
		chunk.append_local_child(root, leaf);
		registry.replace_authoritative_data(leaf, bytes);
		let mut uast_accounting = UastOwnerAccounting::new(2);
		uast_accounting.record_payload_transaction(payload_bytes, 0, 0);
		Ok((
			Self {
				root_id: root,
				uast_accounting,
				parked_generations: ParkedGenerationCache::default(),
				pending_root_transition: None,
				pending_parked_retirements: 0,
				pending_parked_payload_retirements: 0,
				topology_rev: TopologyRev::ZERO,
				file_path: None,
				encrypted_backing: None,
				virtual_path: None,
				canonical_path: None,
				backing_path: None,
				backing_device_id: None,
				file_stamp: None,
				active_language_id: None,
				language_selection: LanguageSelection::Auto,
				shadow: None,
				ledger: UndoLedger::new(),
				search_matches: Vec::new(),
				search_revision: 0,
				search_pattern_revision: 0,
				navigation_anchors: super::navigation::NavigationAnchorStore::default(),
				semantic: Box::new(SemanticState::new()),
			},
			leaf,
		))
	}

	pub(crate) fn bump_topology_rev(&mut self) {
		self.topology_rev = self.topology_rev.next();
	}

	pub(crate) fn bump_search_revision(&mut self) {
		self.search_revision = self.search_revision.wrapping_add(1);
	}

	pub(crate) fn sync_search_pattern_revision(&mut self, revision: u64) {
		if self.search_pattern_revision == revision {
			return;
		}
		self.search_matches.clear();
		self.bump_search_revision();
		self.search_pattern_revision = revision;
	}

	pub(crate) fn search_matches_are_current(&self, revision: u64) -> bool {
		self.search_pattern_revision == revision
	}
}

pub(crate) struct FileBrowserBuffer {
	pub(crate) root_id: NodeId,
	pub(crate) uast_accounting: UastOwnerAccounting,
	pub(crate) topology_rev: TopologyRev,
	pub(crate) file_path: String,
	pub(crate) state: FileBrowserState,
	pub(crate) cached_sum_tree: Option<CachedSumTree>,
}

impl FileBrowserBuffer {
	pub(crate) fn bump_topology_rev(&mut self) {
		self.topology_rev = self.topology_rev.next();
	}
}

pub(crate) struct MarkdownPreviewBuffer {
	pub(crate) title: String,
	pub(crate) state: MarkdownImagePreviewState,
}

/// A pane whose pixels the frontend draws. The engine owns its identity,
/// geometry, focus, and tab membership, and knows nothing about its contents:
/// no host data is representable here, only presentation metadata.
pub(crate) struct HostPanelBuffer {
	pub(crate) panel_id: HostPanelId,
	pub(crate) descriptor: HostPanelDescriptor,
	pub(crate) revision: PanelRevision,
}

#[allow(
	clippy::large_enum_variant,
	reason = "text buffers own the hot document state; boxing only this variant would add indirection to every edit for a cold-path generation cache"
)]
pub(crate) enum BufferContent {
	Text(TextBuffer),
	FileBrowser(FileBrowserBuffer),
	MarkdownPreview(MarkdownPreviewBuffer),
	HostPanel(HostPanelBuffer),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferRole {
	Document,
	OutputPanel,
}

/// Saved-session authority plus the runtime/durable identity bijection.
///
/// The opened writable handle remains private to the engine for its entire
/// lifetime. Mutations update its staged snapshot immediately; publication is
/// still explicit and atomic at checkpoint time.
pub(crate) struct SavedDocumentState {
	pub(crate) session: SavedSession<Writable>,
	by_document: BTreeMap<DocumentId, BufferId>,
	by_buffer: HashMap<BufferId, DocumentId>,
}

impl SavedDocumentState {
	pub(crate) fn new(session: SavedSession<Writable>) -> Self {
		Self {
			session,
			by_document: BTreeMap::new(),
			by_buffer: HashMap::new(),
		}
	}

	pub(crate) fn bind_restored(
		&mut self,
		document_id: DocumentId,
		buffer_id: BufferId,
	) -> Result<(), String> {
		if self.by_document.contains_key(&document_id) || self.by_buffer.contains_key(&buffer_id) {
			return Err("saved document identity is already bound".to_owned());
		}
		self.by_document.insert(document_id, buffer_id);
		self.by_buffer.insert(buffer_id, document_id);
		Ok(())
	}

	pub(crate) fn mint(
		&mut self,
		buffer_id: BufferId,
		path: Option<&Path>,
	) -> Result<DocumentId, String> {
		if self.by_buffer.contains_key(&buffer_id) {
			return Err("runtime buffer already has a durable document identity".to_owned());
		}
		let path = path.map(absolute_lexical_path).transpose()?;
		let document_id = self
			.session
			.mint_document(path)
			.map_err(|error| error.to_string())?;
		self.bind_restored(document_id, buffer_id)?;
		Ok(document_id)
	}

	pub(crate) fn document_id(&self, buffer_id: BufferId) -> Option<DocumentId> {
		self.by_buffer.get(&buffer_id).copied()
	}

	pub(crate) fn buffer_id(&self, document_id: DocumentId) -> Option<BufferId> {
		self.by_document.get(&document_id).copied()
	}

	fn document_ids(&self) -> BTreeSet<DocumentId> {
		self.by_document.keys().copied().collect()
	}

	pub(crate) fn update_path(
		&mut self,
		buffer_id: BufferId,
		path: Option<&Path>,
	) -> Result<(), String> {
		let document_id = self
			.document_id(buffer_id)
			.ok_or_else(|| "ordinary saved document has no durable identity".to_owned())?;
		let path = path.map(absolute_lexical_path).transpose()?;
		self.session
			.update_document_path(document_id, path)
			.map_err(|error| error.to_string())
	}

	pub(crate) fn remove(&mut self, buffer_id: BufferId) -> Result<Option<DocumentId>, String> {
		let Some(document_id) = self.by_buffer.remove(&buffer_id) else {
			return Ok(None);
		};
		let removed = self.by_document.remove(&document_id);
		assert_eq!(
			removed,
			Some(buffer_id),
			"saved document bijection diverged"
		);
		self.session
			.remove_document(document_id)
			.map_err(|error| error.to_string())?;
		Ok(Some(document_id))
	}
}

pub(crate) fn absolute_lexical_path(path: &Path) -> Result<PathBuf, String> {
	if path.is_absolute() {
		Ok(path.to_path_buf())
	} else {
		std::env::current_dir()
			.map(|current| current.join(path))
			.map_err(|error| format!("Current directory unavailable: {error}"))
	}
}

pub(crate) struct Buffer {
	pub(crate) id: BufferId,
	pub(crate) content: BufferContent,
	pub(crate) role: BufferRole,
}

impl Buffer {
	pub(crate) fn is_file_browser(&self) -> bool {
		matches!(self.content, BufferContent::FileBrowser(_))
	}

	pub(crate) fn is_markdown_preview(&self) -> bool {
		matches!(self.content, BufferContent::MarkdownPreview(_))
	}

	#[allow(dead_code, reason = "used by host-panel input routing, next cut")]
	pub(crate) fn is_host_panel(&self) -> bool {
		matches!(self.content, BufferContent::HostPanel(_))
	}

	pub(crate) fn is_output_panel(&self) -> bool {
		self.role == BufferRole::OutputPanel
	}

	pub(crate) fn is_dirty(&self) -> bool {
		self.text().is_some_and(|buffer| buffer.ledger.is_dirty())
	}

	pub(crate) fn root_id(&self) -> Option<NodeId> {
		match &self.content {
			BufferContent::Text(buffer) => Some(buffer.root_id),
			BufferContent::FileBrowser(buffer) => Some(buffer.root_id),
			BufferContent::MarkdownPreview(_) | BufferContent::HostPanel(_) => None,
		}
	}

	pub(crate) fn file_path(&self) -> Option<&str> {
		match &self.content {
			BufferContent::Text(buffer) => buffer.file_path.as_deref().and_then(Path::to_str),
			BufferContent::FileBrowser(buffer) => Some(&buffer.file_path),
			BufferContent::MarkdownPreview(buffer) => Some(&buffer.title),
			// A host panel is never file-backed: it has a title, not a path.
			BufferContent::HostPanel(_) => None,
		}
	}

	pub(crate) fn text(&self) -> Option<&TextBuffer> {
		match &self.content {
			BufferContent::Text(buffer) => Some(buffer),
			BufferContent::FileBrowser(_)
			| BufferContent::MarkdownPreview(_)
			| BufferContent::HostPanel(_) => None,
		}
	}

	pub(crate) fn text_mut(&mut self) -> Option<&mut TextBuffer> {
		match &mut self.content {
			BufferContent::Text(buffer) => Some(buffer),
			BufferContent::FileBrowser(_)
			| BufferContent::MarkdownPreview(_)
			| BufferContent::HostPanel(_) => None,
		}
	}

	pub(crate) fn file_browser(&self) -> Option<&FileBrowserBuffer> {
		match &self.content {
			BufferContent::FileBrowser(buffer) => Some(buffer),
			BufferContent::Text(_)
			| BufferContent::MarkdownPreview(_)
			| BufferContent::HostPanel(_) => None,
		}
	}

	pub(crate) fn markdown_preview(&self) -> Option<&MarkdownPreviewBuffer> {
		match &self.content {
			BufferContent::MarkdownPreview(buffer) => Some(buffer),
			BufferContent::Text(_)
			| BufferContent::FileBrowser(_)
			| BufferContent::HostPanel(_) => None,
		}
	}

	pub(crate) fn host_panel(&self) -> Option<&HostPanelBuffer> {
		match &self.content {
			BufferContent::HostPanel(buffer) => Some(buffer),
			BufferContent::Text(_)
			| BufferContent::FileBrowser(_)
			| BufferContent::MarkdownPreview(_) => None,
		}
	}

	pub(crate) fn host_panel_mut(&mut self) -> Option<&mut HostPanelBuffer> {
		match &mut self.content {
			BufferContent::HostPanel(buffer) => Some(buffer),
			BufferContent::Text(_)
			| BufferContent::FileBrowser(_)
			| BufferContent::MarkdownPreview(_) => None,
		}
	}
}

pub(crate) struct BufferStore {
	next_buffer_id: BufferId,
	by_id: HashMap<BufferId, Buffer>,
	by_path: HashMap<CanonicalPath, BufferId>,
}

/// Stable path identity for live writable buffers.
///
/// Unlike `std::fs::canonicalize`, this also names targets that do not exist:
/// the longest existing ancestor is canonicalized and the missing suffix is
/// appended lexically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalPath(PathBuf);

impl CanonicalPath {
	pub(crate) fn resolve(path: impl AsRef<Path>) -> Result<Self, String> {
		let path = path.as_ref();
		let absolute = if path.is_absolute() {
			path.to_path_buf()
		} else {
			std::env::current_dir()
				.map_err(|err| format!("Unable to resolve current directory: {err}"))?
				.join(path)
		};
		Ok(Self(resolve_path_components(&absolute, 0)?))
	}

	pub(crate) fn as_path(&self) -> &Path {
		&self.0
	}
}

fn resolve_path_components(path: &Path, symlink_depth: u8) -> Result<PathBuf, String> {
	if symlink_depth >= 40 {
		return Err(format!(
			"Unable to resolve {}: too many symbolic links",
			path.display()
		));
	}
	let mut resolved = PathBuf::new();
	for component in path.components() {
		match component {
			std::path::Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
			std::path::Component::RootDir => resolved.push(component.as_os_str()),
			std::path::Component::CurDir => {}
			std::path::Component::ParentDir => {
				resolved.pop();
			}
			std::path::Component::Normal(name) => {
				let candidate = resolved.join(name);
				resolved = match std::fs::canonicalize(&candidate) {
					Ok(canonical) => canonical,
					Err(_)
						if std::fs::symlink_metadata(&candidate)
							.is_ok_and(|metadata| metadata.file_type().is_symlink()) =>
					{
						let target = std::fs::read_link(&candidate).map_err(|err| {
							format!("Unable to resolve {}: {err}", candidate.display())
						})?;
						let target = if target.is_absolute() {
							target
						} else {
							resolved.join(target)
						};
						resolve_path_components(&target, symlink_depth + 1)?
					}
					Err(_) => candidate,
				};
			}
		}
	}
	Ok(resolved)
}

pub(crate) struct PathReservation {
	buffer_id: BufferId,
	path: CanonicalPath,
}

impl BufferStore {
	pub(crate) fn new() -> Self {
		Self {
			next_buffer_id: BufferId::new(1),
			by_id: HashMap::new(),
			by_path: HashMap::new(),
		}
	}

	pub(crate) fn insert(&mut self, content: BufferContent) -> BufferId {
		self.insert_with_role(content, BufferRole::Document)
	}

	pub(crate) fn next_buffer_id(&self) -> BufferId {
		self.next_buffer_id
	}

	pub(crate) fn insert_with_role(
		&mut self,
		content: BufferContent,
		role: BufferRole,
	) -> BufferId {
		let id = self.next_buffer_id;
		self.next_buffer_id = self.next_buffer_id.next();
		let previous = self.by_id.insert(id, Buffer { id, content, role });
		assert!(previous.is_none(), "fresh BufferId must not collide");
		id
	}

	pub(crate) fn get(&self, id: BufferId) -> Option<&Buffer> {
		self.by_id.get(&id)
	}

	pub(crate) fn get_mut(&mut self, id: BufferId) -> Option<&mut Buffer> {
		self.by_id.get_mut(&id)
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = (BufferId, &Buffer)> {
		self.by_id.iter().map(|(id, buffer)| (*id, buffer))
	}

	/// Mutate a text buffer/view pair before the view enters the live layout.
	///
	/// The callback prevents a raw mutable buffer reference from escaping. Live
	/// pairs must instead use `EditorSession::mutate_buffer`, the stage-7 fan-out
	/// seam.
	pub(crate) fn with_detached_text_mut<R>(
		&mut self,
		id: BufferId,
		view: &mut View,
		mutate: impl FnOnce(&mut TextEditorMut<'_>) -> R,
	) -> Option<R> {
		let buffer = self.get_mut(id)?;
		let mut editor = detached_text_editor_mut(buffer, view)?;
		Some(mutate(&mut editor))
	}

	/// Mutate a file-browser buffer/view pair before the view enters the layout.
	pub(crate) fn with_detached_file_browser_mut<R>(
		&mut self,
		id: BufferId,
		view: &mut View,
		mutate: impl FnOnce(&mut FileBrowserEditorMut<'_>) -> R,
	) -> Option<R> {
		let buffer = self.get_mut(id)?;
		let mut editor = detached_file_browser_editor_mut(buffer, view)?;
		Some(mutate(&mut editor))
	}

	pub(crate) fn buffer_for_path(&self, path: &CanonicalPath) -> Option<BufferId> {
		self.by_path.get(path).copied()
	}

	pub(crate) fn reserve_path(
		&self,
		buffer_id: BufferId,
		path: CanonicalPath,
	) -> Result<PathReservation, String> {
		if let Some(owner) = self.by_path.get(&path)
			&& *owner != buffer_id
		{
			return Err("File is already open in another buffer".to_string());
		}
		Ok(PathReservation { buffer_id, path })
	}

	pub(crate) fn commit_path(&mut self, reservation: PathReservation) {
		let PathReservation { buffer_id, path } = reservation;
		let buffer = self
			.by_id
			.get_mut(&buffer_id)
			.expect("path reservation must name a live buffer");
		let document = buffer
			.text_mut()
			.expect("only text buffers can own canonical paths");
		if let Some(previous) = document.canonical_path.replace(path.clone()) {
			self.by_path.remove(&previous);
		}
		let previous = self.by_path.insert(path, buffer_id);
		assert!(
			previous.is_none() || previous == Some(buffer_id),
			"reserved canonical path must remain available until commit"
		);
	}

	/// Drop a buffer's canonical-path claim after its document stops
	/// representing that file (e.g. `ReplaceDocument`), so the path can be
	/// reopened into another buffer.
	/// Find the buffer backing a host panel identity, if one is live.
	pub(crate) fn host_panel_buffer_id(&self, panel_id: HostPanelId) -> Option<BufferId> {
		self.by_id
			.iter()
			.find(|(_, buffer)| {
				buffer
					.host_panel()
					.is_some_and(|panel| panel.panel_id == panel_id)
			})
			.map(|(id, _)| *id)
	}

	pub(crate) fn detach_path(&mut self, buffer_id: BufferId) {
		let Some(buffer) = self.by_id.get_mut(&buffer_id) else {
			return;
		};
		let Some(document) = buffer.text_mut() else {
			return;
		};
		if let Some(path) = document.canonical_path.take()
			&& self.by_path.get(&path) == Some(&buffer_id)
		{
			self.by_path.remove(&path);
		}
	}

	pub(crate) fn remove(&mut self, id: BufferId) -> Option<Buffer> {
		let buffer = self.by_id.remove(&id)?;
		if let BufferContent::Text(document) = &buffer.content
			&& let Some(path) = &document.canonical_path
			&& self.by_path.get(path) == Some(&id)
		{
			self.by_path.remove(path);
		}
		Some(buffer)
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.by_id.len()
	}

	#[cfg(test)]
	pub(crate) fn contains(&self, id: BufferId) -> bool {
		self.by_id.contains_key(&id)
	}

	#[cfg(test)]
	pub(crate) fn next_id(&self) -> BufferId {
		self.next_buffer_id
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Affinity {
	Before,
	After,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TextAnchor {
	pub(crate) byte: DocByte,
	pub(crate) affinity: Affinity,
}

impl TextAnchor {
	pub(crate) const fn new(byte: DocByte, affinity: Affinity) -> Self {
		Self { byte, affinity }
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CaretNodeCache {
	pub(crate) node: NodeId,
	pub(crate) offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CaretLineCache {
	pub(crate) line: DocLine,
	pub(crate) col: DocDisplayCol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CaretCache {
	node: Option<CaretNodeCache>,
	node_rev: TopologyRev,
	line: Option<CaretLineCache>,
	line_rev: StateId,
}

impl CaretCache {
	const EMPTY: Self = Self {
		node: None,
		node_rev: TopologyRev::ZERO,
		line: None,
		line_rev: StateId::ZERO,
	};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingCaretTarget {
	pub(crate) line: DocLine,
	pub(crate) col: DocDisplayCol,
	pub(crate) affinity: Affinity,
	pub(crate) reveal_hidden: bool,
	pub(crate) required_node: Option<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Caret {
	anchor: TextAnchor,
	pub(crate) preferred_col: DocDisplayCol,
	cache: CaretCache,
	pending_target: Option<PendingCaretTarget>,
}

impl Caret {
	pub(crate) const fn at_start() -> Self {
		Self {
			anchor: TextAnchor::new(DocByte::ZERO, Affinity::After),
			preferred_col: DocDisplayCol::ZERO,
			cache: CaretCache::EMPTY,
			pending_target: None,
		}
	}

	pub(crate) const fn byte(&self) -> DocByte {
		self.anchor.byte
	}

	pub(crate) const fn affinity(&self) -> Affinity {
		self.anchor.affinity
	}

	pub(crate) const fn node_cache(&self) -> Option<CaretNodeCache> {
		self.cache.node
	}

	pub(crate) const fn line_cache(&self) -> Option<CaretLineCache> {
		self.cache.line
	}

	pub(crate) const fn pending_target(&self) -> Option<PendingCaretTarget> {
		self.pending_target
	}

	pub(crate) fn abs_line(&self) -> DocLine {
		self.pending_target.map_or_else(
			|| self.cache.line.map_or(DocLine::ZERO, |line| line.line),
			|target| target.line,
		)
	}

	pub(crate) fn abs_col(&self) -> DocDisplayCol {
		self.pending_target.map_or_else(
			|| self.cache.line.map_or(DocDisplayCol::ZERO, |line| line.col),
			|target| target.col,
		)
	}

	pub(crate) fn set_anchor(&mut self, anchor: TextAnchor) {
		self.anchor = anchor;
		self.cache = CaretCache::EMPTY;
		self.pending_target = None;
	}

	pub(crate) fn set_byte(&mut self, byte: DocByte, affinity: Affinity) {
		self.set_anchor(TextAnchor::new(byte, affinity));
	}

	pub(crate) fn set_pending_target(
		&mut self,
		line: DocLine,
		col: DocDisplayCol,
		affinity: Affinity,
		reveal_hidden: bool,
		required_node: Option<NodeId>,
	) {
		self.pending_target = Some(PendingCaretTarget {
			line,
			col,
			affinity,
			reveal_hidden,
			required_node,
		});
	}

	pub(crate) fn resolve(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		folds: &mut FoldSet,
		topology_rev: TopologyRev,
		state_id: StateId,
	) -> bool {
		self.resolve_indexed(registry, root, folds, topology_rev, state_id, None)
	}

	/// [`Self::resolve`], taking lines on the wrap index's path through it
	/// (ADR 0010): a pending target or a stale line cache on such a line is
	/// settled from the index, never from the line's start.
	pub(crate) fn resolve_indexed(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		folds: &mut FoldSet,
		topology_rev: TopologyRev,
		state_id: StateId,
		indexed: Option<CaretIndex<'_>>,
	) -> bool {
		if let Some(pending) = self.pending_target
			&& let Some(placed) = self.place_indexed(
				registry,
				root,
				folds,
				topology_rev,
				state_id,
				pending.line,
				pending.col,
				pending.affinity,
				pending.reveal_hidden,
				indexed,
			) {
			if !placed {
				return false;
			}
		} else if let Some(mut pending) = self.pending_target {
			let lookup = if pending.reveal_hidden {
				let raw = registry.find_node_at_line_col_raw(root, pending.line, pending.col);
				if let Some(target) = raw.ready() {
					unfold_ancestor_chain(registry, folds, target.node_id);
					registry.find_node_at_line_col(root, folds, pending.line, pending.col)
				} else {
					raw
				}
			} else {
				registry.find_node_at_line_col(root, folds, pending.line, pending.col)
			};
			let Some(target) = lookup.ready() else {
				pending.required_node = lookup.required_node();
				self.pending_target = Some(pending);
				return false;
			};
			let byte = registry.doc_byte_for_node_offset(root, target.node_id, target.node_byte);
			self.anchor = TextAnchor::new(byte, pending.affinity);
			let line = if pending.reveal_hidden {
				pending.line
			} else {
				snap_line_to_visible_boundary(registry, root, folds, pending.line)
			};
			self.cache = CaretCache {
				node: Some(CaretNodeCache {
					node: target.node_id,
					offset: target.node_byte.get(),
				}),
				node_rev: topology_rev,
				line: Some(CaretLineCache {
					line,
					col: target.display_col,
				}),
				line_rev: state_id,
			};
			self.pending_target = None;
		}
		let total = registry.get_total_bytes(root);
		if self.anchor.byte.get() > total {
			self.set_byte(DocByte::new(total), self.anchor.affinity);
		}
		if self.cache.node_rev != topology_rev {
			self.cache.node = None;
		}
		if self.cache.line_rev != state_id {
			self.cache.line = None;
		}
		if self.cache.node.is_none() {
			let total = registry.get_total_bytes(root);
			let lookup_byte = match self.anchor.affinity {
				Affinity::Before if self.anchor.byte > DocByte::ZERO => {
					self.anchor.byte.saturating_sub(1)
				}
				Affinity::After if self.anchor.byte.get() == total && total > 0 => {
					self.anchor.byte.saturating_sub(1)
				}
				_ => self.anchor.byte,
			};
			let target = registry.find_node_at_doc_byte_raw(root, lookup_byte);
			let offset = match self.anchor.affinity {
				Affinity::Before if self.anchor.byte > DocByte::ZERO => {
					target.node_byte.get().saturating_add(1)
				}
				Affinity::After if self.anchor.byte.get() == total && total > 0 => {
					target.node_byte.get().saturating_add(1)
				}
				_ => target.node_byte.get(),
			};
			self.cache.node = Some(CaretNodeCache {
				node: target.node_id,
				offset,
			});
			self.cache.node_rev = topology_rev;
		}
		if self.cache.line.is_none() {
			let found = match indexed
				.and_then(|indexed| indexed.line_col_for_byte(registry, root, self.anchor.byte))
			{
				Some(CaretLookup::Known(found)) => Some(found),
				Some(CaretLookup::Pending) => None,
				None => line_col_from_doc_byte_sparse(registry, root, self.anchor.byte).ok(),
			};
			if let Some((line, col)) = found {
				self.cache.line = Some(CaretLineCache { line, col });
				self.cache.line_rev = state_id;
			}
		}
		self.cache.node.is_some() && self.cache.line.is_some()
	}

	/// Place at `line` and `col` through the index when the line is on its
	/// path and visible. `None`: not such a line, so the caller takes today's
	/// path. `Some(false)`: indexed but pending, with the target kept.
	#[allow(clippy::too_many_arguments)]
	fn place_indexed(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		folds: &mut FoldSet,
		topology_rev: TopologyRev,
		state_id: StateId,
		line: DocLine,
		col: DocDisplayCol,
		affinity: Affinity,
		reveal_hidden: bool,
		indexed: Option<CaretIndex<'_>>,
	) -> Option<bool> {
		// A hidden line keeps today's fold handling.
		let indexed = indexed
			.filter(|_| snap_line_to_visible_boundary(registry, root, folds, line) == line)?;
		match indexed.byte_for_line_col(registry, root, line, col)? {
			CaretLookup::Known((byte, col)) => {
				let target = registry.find_node_at_doc_byte_raw(root, byte);
				self.install_resolved(
					TextAnchor::new(byte, affinity),
					self.preferred_col,
					CaretNodeCache {
						node: target.node_id,
						offset: target.node_byte.get(),
					},
					CaretLineCache { line, col },
					topology_rev,
					state_id,
				);
				Some(true)
			}
			CaretLookup::Pending => {
				self.set_pending_target(line, col, affinity, reveal_hidden, None);
				Some(false)
			}
		}
	}

	/// Place the caret at a position whose line and column are already known,
	/// such as a row start from the wrap index (ADR 0009). Unlike
	/// [`Self::place_at_line_col`] this reads nothing from the line's start:
	/// the node comes from a metrics-only descent.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn place_at_known(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		topology_rev: TopologyRev,
		state_id: StateId,
		byte: DocByte,
		line: DocLine,
		col: DocDisplayCol,
	) {
		let target = registry.find_node_at_doc_byte_raw(root, byte);
		self.anchor = TextAnchor::new(byte, Affinity::After);
		self.pending_target = None;
		self.cache = CaretCache {
			node: Some(CaretNodeCache {
				node: target.node_id,
				offset: target.node_byte.get(),
			}),
			node_rev: topology_rev,
			line: Some(CaretLineCache { line, col }),
			line_rev: state_id,
		};
	}

	pub(crate) fn place_at_line_col(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		folds: &mut FoldSet,
		topology_rev: TopologyRev,
		state_id: StateId,
		target_line: DocLine,
		target_col: DocDisplayCol,
		reveal_hidden: bool,
		affinity: Affinity,
	) -> bool {
		self.place_at_line_col_indexed(
			registry,
			root,
			folds,
			topology_rev,
			state_id,
			target_line,
			target_col,
			reveal_hidden,
			affinity,
			None,
		)
	}

	/// [`Self::place_at_line_col`], taking a line on the wrap index's path
	/// through it (ADR 0010).
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn place_at_line_col_indexed(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		folds: &mut FoldSet,
		topology_rev: TopologyRev,
		state_id: StateId,
		target_line: DocLine,
		target_col: DocDisplayCol,
		reveal_hidden: bool,
		affinity: Affinity,
		indexed: Option<CaretIndex<'_>>,
	) -> bool {
		if let Some(placed) = self.place_indexed(
			registry,
			root,
			folds,
			topology_rev,
			state_id,
			target_line,
			target_col,
			affinity,
			reveal_hidden,
			indexed,
		) {
			return placed;
		}
		let lookup = if reveal_hidden {
			let raw = registry.find_node_at_line_col_raw(root, target_line, target_col);
			let Some(raw_target) = raw.ready() else {
				self.set_pending_target(
					target_line,
					target_col,
					affinity,
					reveal_hidden,
					raw.required_node(),
				);
				return false;
			};
			unfold_ancestor_chain(registry, folds, raw_target.node_id);
			registry.find_node_at_line_col(root, folds, target_line, target_col)
		} else {
			registry.find_node_at_line_col(root, folds, target_line, target_col)
		};
		let Some(target) = lookup.ready() else {
			self.set_pending_target(
				target_line,
				target_col,
				affinity,
				reveal_hidden,
				lookup.required_node(),
			);
			return false;
		};
		let byte = registry.doc_byte_for_node_offset(root, target.node_id, target.node_byte);
		let line = if reveal_hidden {
			target_line
		} else {
			snap_line_to_visible_boundary(registry, root, folds, target_line)
		};
		self.install_resolved(
			TextAnchor::new(byte, affinity),
			self.preferred_col,
			CaretNodeCache {
				node: target.node_id,
				offset: target.node_byte.get(),
			},
			CaretLineCache {
				line,
				col: target.display_col,
			},
			topology_rev,
			state_id,
		);
		true
	}

	pub(crate) fn install_resolved(
		&mut self,
		anchor: TextAnchor,
		preferred_col: DocDisplayCol,
		node: CaretNodeCache,
		line: CaretLineCache,
		topology_rev: TopologyRev,
		state_id: StateId,
	) {
		self.anchor = anchor;
		self.preferred_col = preferred_col;
		self.cache = CaretCache {
			node: Some(node),
			node_rev: topology_rev,
			line: Some(line),
			line_rev: state_id,
		};
		self.pending_target = None;
	}

	pub(crate) fn reset_to_start(
		&mut self,
		node: NodeId,
		topology_rev: TopologyRev,
		state_id: StateId,
	) {
		self.install_resolved(
			TextAnchor::new(DocByte::ZERO, Affinity::After),
			DocDisplayCol::ZERO,
			CaretNodeCache { node, offset: 0 },
			CaretLineCache {
				line: DocLine::ZERO,
				col: DocDisplayCol::ZERO,
			},
			topology_rev,
			state_id,
		);
	}

	fn without_cache(self) -> Self {
		Self {
			cache: CaretCache::EMPTY,
			..self
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorSet {
	primary: Caret,
	secondary: Vec<Caret>,
}

pub(crate) const MAX_CURSORS: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CursorSnapshot {
	Points {
		primary: Caret,
		secondary: Vec<Caret>,
	},
	Visual {
		caret: Caret,
		anchor: TextAnchor,
		kind: VisualKind,
	},
}

impl CursorSnapshot {
	pub(crate) fn single(byte: DocByte) -> Self {
		Self::from_bytes(byte, Vec::new())
	}

	pub(crate) fn from_bytes(primary: DocByte, secondary: Vec<DocByte>) -> Self {
		let caret = |byte| Caret {
			anchor: TextAnchor::new(byte, Affinity::After),
			preferred_col: DocDisplayCol::ZERO,
			cache: CaretCache::EMPTY,
			pending_target: None,
		};
		Self::Points {
			primary: caret(primary),
			secondary: secondary.into_iter().map(caret).collect(),
		}
	}

	pub(crate) fn primary(&self) -> Caret {
		match self {
			Self::Points { primary, .. } => *primary,
			Self::Visual { caret, .. } => *caret,
		}
	}
}

#[cfg(test)]
fn caret_from_doc_byte(
	registry: &UastRegistry,
	root: NodeId,
	byte: DocByte,
) -> Result<Caret, String> {
	let mut caret = Caret::at_start();
	let mut folds = FoldSet::default();
	caret.set_byte(
		DocByte::new(byte.get().min(registry.get_total_bytes(root))),
		Affinity::After,
	);
	if !caret.resolve(registry, root, &mut folds, TopologyRev::ZERO, StateId::ZERO) {
		return Err("Caret coordinate is still loading".to_string());
	}
	caret.preferred_col = caret.abs_col();
	Ok(caret)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddCaretResult {
	Added,
	Promoted,
	Unchanged,
	AtLimit,
}

impl CursorSet {
	pub(crate) const fn single(primary: Caret) -> Self {
		Self {
			primary,
			secondary: Vec::new(),
		}
	}

	pub(crate) const fn primary(&self) -> &Caret {
		&self.primary
	}

	pub(crate) fn primary_mut(&mut self) -> &mut Caret {
		&mut self.primary
	}

	pub(crate) fn secondary(&self) -> &[Caret] {
		&self.secondary
	}

	pub(crate) fn all(&self) -> impl Iterator<Item = &Caret> {
		std::iter::once(&self.primary).chain(self.secondary.iter())
	}

	pub(crate) fn all_mut(&mut self) -> impl Iterator<Item = &mut Caret> {
		std::iter::once(&mut self.primary).chain(self.secondary.iter_mut())
	}

	pub(crate) fn len(&self) -> usize {
		1 + self.secondary.len()
	}

	pub(crate) fn snapshot(&self) -> CursorSnapshot {
		CursorSnapshot::Points {
			primary: self.primary.without_cache(),
			secondary: self
				.secondary
				.iter()
				.copied()
				.map(Caret::without_cache)
				.collect(),
		}
	}

	pub(crate) fn add_or_promote(
		&mut self,
		registry: &UastRegistry,
		root: NodeId,
		caret: Caret,
	) -> AddCaretResult {
		let target_byte = caret.byte();
		let primary_byte = self.primary.byte();
		if target_byte == primary_byte {
			return AddCaretResult::Unchanged;
		}

		if let Some(index) = self
			.secondary
			.iter()
			.position(|candidate| candidate.byte() == target_byte)
		{
			let promoted = self.secondary.remove(index);
			self.secondary.push(self.primary);
			self.primary = promoted;
			self.normalize(registry, root);
			return AddCaretResult::Promoted;
		}

		if self.len() >= MAX_CURSORS {
			return AddCaretResult::AtLimit;
		}
		self.secondary.push(self.primary);
		self.primary = caret;
		self.normalize(registry, root);
		AddCaretResult::Added
	}

	pub(crate) fn add_pending(&mut self, caret: Caret) -> AddCaretResult {
		if self.all().any(
			|candidate| match (candidate.pending_target(), caret.pending_target()) {
				(Some(left), Some(right)) => (left.line, left.col) == (right.line, right.col),
				(None, None) => candidate.anchor.byte == caret.anchor.byte,
				_ => false,
			},
		) {
			return AddCaretResult::Unchanged;
		}
		if self.len() >= MAX_CURSORS {
			return AddCaretResult::AtLimit;
		}
		self.secondary.push(self.primary);
		self.primary = caret;
		AddCaretResult::Added
	}

	pub(crate) fn normalize(&mut self, _registry: &UastRegistry, _root: NodeId) {
		let primary_byte = self.primary.byte();
		let mut seen = HashSet::new();
		self.secondary
			.retain(|caret| caret.byte() != primary_byte && seen.insert(caret.byte()));
	}

	pub(crate) fn collapse(&mut self) {
		self.secondary.clear();
	}

	pub(crate) fn sync_preferred_col(&mut self) {
		for caret in self.all_mut() {
			if let Some(line) = caret.line_cache() {
				caret.preferred_col = line.col;
			}
		}
	}

	pub(crate) fn from_snapshot(primary: Caret, secondary: Vec<Caret>) -> Self {
		Self {
			primary: primary.without_cache(),
			secondary: secondary.into_iter().map(Caret::without_cache).collect(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CursorMode {
	Points(CursorSet),
	Visual {
		caret: Caret,
		anchor: TextAnchor,
		kind: VisualKind,
	},
}

pub(crate) struct CursorState {
	mode: CursorMode,
}

impl CursorState {
	pub(crate) const fn single(caret: Caret) -> Self {
		Self {
			mode: CursorMode::Points(CursorSet::single(caret)),
		}
	}

	pub(crate) fn points(&self) -> Option<&CursorSet> {
		match &self.mode {
			CursorMode::Points(cursors) => Some(cursors),
			CursorMode::Visual { .. } => None,
		}
	}

	pub(crate) fn points_mut(&mut self) -> Option<&mut CursorSet> {
		match &mut self.mode {
			CursorMode::Points(cursors) => Some(cursors),
			CursorMode::Visual { .. } => None,
		}
	}

	pub(crate) fn primary(&self) -> &Caret {
		match &self.mode {
			CursorMode::Points(cursors) => cursors.primary(),
			CursorMode::Visual { caret, .. } => caret,
		}
	}

	pub(crate) fn primary_mut(&mut self) -> &mut Caret {
		match &mut self.mode {
			CursorMode::Points(cursors) => cursors.primary_mut(),
			CursorMode::Visual { caret, .. } => caret,
		}
	}

	pub(crate) fn visit_carets_mut(&mut self, mut visit: impl FnMut(&mut Caret)) {
		match &mut self.mode {
			CursorMode::Points(cursors) => {
				for caret in cursors.all_mut() {
					visit(caret);
				}
			}
			CursorMode::Visual { caret, .. } => visit(caret),
		}
	}

	pub(crate) fn active_visual(&self) -> Option<(DocByte, VisualKind)> {
		match self.mode {
			CursorMode::Points(_) => None,
			CursorMode::Visual { anchor, kind, .. } => Some((anchor.byte, kind)),
		}
	}

	pub(crate) fn set_active_visual(&mut self, active: Option<(DocByte, VisualKind)>) {
		let caret = *self.primary();
		self.mode = match (active, &self.mode) {
			(Some((anchor, kind)), _) => CursorMode::Visual {
				caret,
				anchor: TextAnchor::new(anchor, Affinity::Before),
				kind,
			},
			(None, CursorMode::Points(_)) => return,
			(None, CursorMode::Visual { .. }) => CursorMode::Points(CursorSet::single(caret)),
		};
	}

	pub(crate) fn collapse(&mut self) {
		if let CursorMode::Points(cursors) = &mut self.mode {
			cursors.collapse();
		}
	}

	pub(crate) fn sync_preferred_col(&mut self) {
		match &mut self.mode {
			CursorMode::Points(cursors) => cursors.sync_preferred_col(),
			CursorMode::Visual { caret, .. } => {
				if let Some(line) = caret.line_cache() {
					caret.preferred_col = line.col;
				}
			}
		}
	}

	pub(crate) fn snapshot(&self) -> CursorSnapshot {
		match &self.mode {
			CursorMode::Points(cursors) => cursors.snapshot(),
			CursorMode::Visual {
				caret,
				anchor,
				kind,
			} => CursorSnapshot::Visual {
				caret: caret.without_cache(),
				anchor: *anchor,
				kind: *kind,
			},
		}
	}

	pub(crate) fn restore_snapshot(&mut self, snapshot: CursorSnapshot) {
		self.mode = match snapshot {
			CursorSnapshot::Points { primary, secondary } => {
				CursorMode::Points(CursorSet::from_snapshot(primary, secondary))
			}
			CursorSnapshot::Visual {
				caret,
				anchor,
				kind,
			} => CursorMode::Visual {
				caret: caret.without_cache(),
				anchor,
				kind,
			},
		};
	}

	pub(crate) fn clamp_anchors(&mut self, total_bytes: u64) {
		self.visit_carets_mut(|caret| {
			caret.set_byte(
				DocByte::new(caret.byte().get().min(total_bytes)),
				caret.affinity(),
			);
		});
		if let CursorMode::Visual { anchor, .. } = &mut self.mode {
			anchor.byte = DocByte::new(anchor.byte.get().min(total_bytes));
		}
	}

	pub(crate) fn rebase_after_edits(&mut self, edits: &[FoldEdit]) {
		self.visit_carets_mut(|caret| {
			let anchor = rebase_text_anchor(TextAnchor::new(caret.byte(), caret.affinity()), edits);
			caret.set_byte(anchor.byte, anchor.affinity);
		});
		if let CursorMode::Visual { anchor, .. } = &mut self.mode {
			*anchor = rebase_text_anchor(*anchor, edits);
		}
	}

	pub(crate) fn len(&self) -> usize {
		match &self.mode {
			CursorMode::Points(cursors) => cursors.len(),
			CursorMode::Visual { .. } => 1,
		}
	}
}

pub(crate) fn rebase_text_anchor(anchor: TextAnchor, edits: &[FoldEdit]) -> TextAnchor {
	let original = anchor.byte;
	let affinity = anchor.affinity;
	let mut ordered = edits.to_vec();
	ordered.sort_unstable_by_key(|edit| edit.start);
	let mut shift = 0i128;
	for edit in ordered {
		let start = edit.start.get();
		let end = start.saturating_add(edit.deleted_len);
		if original.get() < start {
			break;
		}
		if edit.deleted_len == 0 {
			if original.get() > start
				|| (original.get() == start && matches!(affinity, Affinity::After))
			{
				shift += i128::from(edit.inserted_len);
			}
			continue;
		}
		if original.get() >= end {
			shift += i128::from(edit.inserted_len) - i128::from(edit.deleted_len);
			continue;
		}
		let replacement_offset = if matches!(affinity, Affinity::After) {
			edit.inserted_len
		} else {
			0
		};
		return TextAnchor::new(
			DocByte::new(
				((i128::from(start) + shift).max(0) as u64).saturating_add(replacement_offset),
			),
			affinity,
		);
	}
	TextAnchor::new(
		DocByte::new((i128::from(original.get()) + shift).max(0) as u64),
		affinity,
	)
}

impl std::ops::Deref for CursorState {
	type Target = Caret;

	fn deref(&self) -> &Self::Target {
		self.primary()
	}
}

impl std::ops::DerefMut for CursorState {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.primary_mut()
	}
}

pub(crate) struct CommonViewState {
	pub(crate) scroll_y: u64,
	pub(crate) scroll_row_offset: DisplayRow,
	pub(crate) viewport_width: u16,
	pub(crate) viewport_lines: u32,
	pub(crate) wrap_enabled: bool,
	pub(crate) minimap_enabled: bool,
	pub(crate) line_numbers_enabled: bool,
	pub(crate) relative_line_numbers_enabled: bool,
	pub(crate) visible_whitespace: bool,
	pub(crate) theme_override: Option<String>,
	pub(crate) current_theme_name: String,
	pub(crate) current_theme: Theme,
	pub(crate) background_hex: Option<String>,
	pub(crate) cached_minimap: Option<MinimapSnapshot>,
	pub(crate) last_minimap_state: Option<StateId>,
	pub(crate) last_minimap_semantic_revision: u64,
	pub(crate) last_minimap_total_lines: u64,
	pub(crate) last_minimap_path: Option<PathBuf>,
	/// Soft-wrap rows of this view's long lines (ADR 0009). A `RefCell`
	/// because layout reads the view through shared references.
	pub(crate) wrap_index: std::cell::RefCell<super::wrap_index::WrapIndex>,
	/// Work a command could not finish, which render carries on (ADR 0010).
	pub(crate) carried: std::cell::RefCell<super::layout::CarriedWork>,
}

impl CommonViewState {
	fn gutter_numbering(&self) -> Option<GutterNumbering> {
		match (
			self.line_numbers_enabled,
			self.relative_line_numbers_enabled,
		) {
			(false, false) => None,
			(true, false) => Some(GutterNumbering::Absolute),
			(false, true) => Some(GutterNumbering::Relative),
			(true, true) => Some(GutterNumbering::Hybrid),
		}
	}

	pub(crate) fn reset_minimap_cache(&mut self) {
		self.cached_minimap = None;
		self.last_minimap_state = None;
		self.last_minimap_semantic_revision = u64::MAX;
		self.last_minimap_total_lines = u64::MAX;
		self.last_minimap_path = None;
	}
}

pub(crate) struct UiState {
	pub(crate) status_message: Option<String>,
	/// ADR 0001 stage 4 warning; allocation is monotonic until stage 5, so
	/// crossing the threshold can occur at most once per session.
	pub(crate) capacity_warning_shown: bool,
	/// One-shot `QueryDocumentText` reply, drained into the next viewport.
	pub(crate) document_text_response: Option<String>,
	/// Buffer that produced `document_text_response`.
	pub(crate) document_text_response_buffer_id: Option<BufferId>,
	/// State read with `document_text_response`.
	pub(crate) document_text_response_state_id: Option<StateId>,
	pub(crate) busy: bool,
	pub(crate) pending_quit: bool,
	pub(crate) mode_override: Option<EditorMode>,
	pub(crate) yank_flash: Option<(DocByte, DocByte)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StructuralIssueKind {
	Unavailable,
	Rejected,
	CapacityRefused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StructuralIssue {
	pub(crate) state_id: StateId,
	pub(crate) kind: StructuralIssueKind,
}

pub(crate) struct SemanticState {
	pub(crate) highlights: Vec<HighlightSpan>,
	pub(crate) diagnostics: Vec<DiagnosticSpan>,
	pub(crate) inline_annotations: Vec<super::language::InlineAnnotation>,
	pub(crate) last_semantic_state: Option<StateId>,
	/// Content state whose structural topology is currently installed.
	/// Unlike `last_semantic_state`, this advances only after a graft succeeds.
	pub(crate) applied_structural_state_id: Option<StateId>,
	/// Most recent accepted semantic response that could not install topology.
	pub(crate) structural_issue: Option<StructuralIssue>,
	pub(crate) last_semantic_len: usize,
	pub(crate) last_semantic_path: Option<String>,
	pub(crate) last_analysis_depth: Option<AnalysisDepth>,
	pub(crate) pending_semantic_request_id: Option<RequestId>,
	pub(crate) cached_sum_tree: Option<CachedSumTree>,
	pub(crate) view_revision: u64,
}

impl SemanticState {
	fn new() -> Self {
		Self {
			highlights: Vec::new(),
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
			last_semantic_state: None,
			applied_structural_state_id: None,
			structural_issue: None,
			last_semantic_len: usize::MAX,
			last_semantic_path: None,
			last_analysis_depth: None,
			pending_semantic_request_id: None,
			cached_sum_tree: None,
			view_revision: 0,
		}
	}

	pub(crate) fn reset_pipeline(&mut self) {
		self.highlights.clear();
		self.diagnostics.clear();
		self.inline_annotations.clear();
		self.last_semantic_state = None;
		self.applied_structural_state_id = None;
		self.structural_issue = None;
		self.last_semantic_len = usize::MAX;
		self.last_semantic_path = None;
		self.last_analysis_depth = None;
		self.pending_semantic_request_id = None;
		self.cached_sum_tree = None;
		self.view_revision = self.view_revision.wrapping_add(1);
	}
}

pub(crate) struct SettingsPersistent {
	pub(crate) theme_name: String,
	pub(crate) minimap_enabled: bool,
	pub(crate) wrap_enabled: bool,
	pub(crate) line_numbers_enabled: bool,
	pub(crate) relative_line_numbers_enabled: bool,
	pub(crate) visible_whitespace: bool,
	pub(crate) reload_enabled: bool,
	pub(crate) save_durability: SaveDurability,
	pub(crate) ui_colors: UiColorOverrides,
	pub(crate) language_themes: BTreeMap<String, String>,
}

impl SettingsPersistent {
	pub(crate) fn builtin_defaults() -> Self {
		let EngineSettings {
			theme_name,
			minimap_enabled,
			wrap_enabled,
			line_numbers_enabled,
			relative_line_numbers_enabled,
			visible_whitespace,
			reload_enabled,
			save_durability,
			ui_colors,
			language_themes,
		} = EngineSettings::default();
		Self {
			theme_name,
			minimap_enabled,
			wrap_enabled,
			line_numbers_enabled,
			relative_line_numbers_enabled,
			visible_whitespace,
			reload_enabled,
			save_durability,
			ui_colors,
			language_themes,
		}
	}

	fn resolved_theme(&self) -> (String, Theme) {
		match Theme::try_new(&self.theme_name) {
			Ok(theme) => (self.theme_name.clone(), theme),
			Err(_) => ("onedark".to_string(), Theme::try_new("onedark").unwrap()),
		}
	}
}

fn new_common_view_state(
	persistent_settings: &SettingsPersistent,
	viewport_width: u16,
	viewport_lines: u32,
) -> CommonViewState {
	let (current_theme_name, current_theme) = persistent_settings.resolved_theme();
	CommonViewState {
		scroll_y: 0,
		scroll_row_offset: DisplayRow::ZERO,
		viewport_width,
		viewport_lines,
		wrap_enabled: persistent_settings.wrap_enabled,
		minimap_enabled: persistent_settings.minimap_enabled,
		line_numbers_enabled: persistent_settings.line_numbers_enabled,
		relative_line_numbers_enabled: persistent_settings.relative_line_numbers_enabled,
		visible_whitespace: persistent_settings.visible_whitespace,
		theme_override: None,
		current_theme_name,
		current_theme,
		background_hex: None,
		cached_minimap: None,
		last_minimap_state: None,
		last_minimap_semantic_revision: u64::MAX,
		last_minimap_total_lines: u64::MAX,
		last_minimap_path: None,
		wrap_index: std::cell::RefCell::default(),
		carried: std::cell::RefCell::default(),
	}
}

pub(crate) fn resolve_theme_name(
	active_language_id: Option<super::language::LanguageId>,
	theme_override: Option<&str>,
	settings: &SettingsPersistent,
) -> String {
	if let Some(theme_override) = theme_override {
		return theme_override.to_string();
	}
	if let Some(language_theme) = active_language_id
		.and_then(|language_id| settings.language_themes.get(language_id.as_str()))
	{
		return language_theme.clone();
	}
	settings.theme_name.clone()
}

pub(crate) fn resolve_file_browser_theme_name(
	theme_override: Option<&str>,
	settings: &SettingsPersistent,
) -> String {
	theme_override
		.map(str::to_owned)
		.or_else(|| settings.language_themes.get(FILE_BROWSER_THEME_ID).cloned())
		.unwrap_or_else(|| settings.theme_name.clone())
}

pub(crate) struct TextViewState {
	pub(crate) folds: FoldSet,
	pub(crate) common: CommonViewState,
}

impl TextViewState {
	pub(crate) fn gutter_numbering(&self) -> Option<GutterNumbering> {
		self.common.gutter_numbering()
	}
}

impl std::ops::Deref for TextViewState {
	type Target = CommonViewState;

	fn deref(&self) -> &Self::Target {
		&self.common
	}
}

impl std::ops::DerefMut for TextViewState {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.common
	}
}

pub(crate) struct TextView {
	pub(crate) cursor: CursorState,
	pub(crate) state: TextViewState,
	pub(crate) match_index: Option<usize>,
	pub(crate) match_info: Option<String>,
}

pub(crate) struct FileBrowserView {
	pub(crate) cursor: CursorState,
	pub(crate) state: TextViewState,
}

pub(crate) struct MarkdownPreviewView {
	pub(crate) state: CommonViewState,
}

/// View-local state for a host panel: geometry only, since the frontend owns
/// everything drawn inside it.
pub(crate) struct HostPanelView {
	pub(crate) state: CommonViewState,
}

pub(crate) enum ViewContent {
	Text(TextView),
	FileBrowser(FileBrowserView),
	MarkdownPreview(MarkdownPreviewView),
	HostPanel(HostPanelView),
}

pub(crate) struct View {
	pub(crate) id: ViewId,
	pub(crate) buffer_id: BufferId,
	pub(crate) content: ViewContent,
}

impl View {
	pub(crate) fn new_text(
		id: ViewId,
		buffer_id: BufferId,
		initial_leaf: NodeId,
		persistent_settings: &SettingsPersistent,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Self {
		let mut cursor = CursorState::single(Caret::at_start());
		cursor
			.primary_mut()
			.reset_to_start(initial_leaf, TopologyRev::ZERO, StateId::ZERO);

		Self {
			id,
			buffer_id,
			content: ViewContent::Text(TextView {
				cursor,
				state: TextViewState {
					folds: FoldSet::default(),
					common: new_common_view_state(
						persistent_settings,
						viewport_width,
						viewport_lines,
					),
				},
				match_index: None,
				match_info: None,
			}),
		}
	}

	pub(crate) fn new_file_browser(
		id: ViewId,
		buffer_id: BufferId,
		initial_leaf: NodeId,
		persistent_settings: &SettingsPersistent,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Self {
		let mut cursor = CursorState::single(Caret::at_start());
		cursor
			.primary_mut()
			.reset_to_start(initial_leaf, TopologyRev::ZERO, StateId::ZERO);
		let mut common = new_common_view_state(persistent_settings, viewport_width, viewport_lines);
		let theme_name = resolve_file_browser_theme_name(None, persistent_settings);
		if theme_name != common.current_theme_name
			&& let Ok(theme) = Theme::try_new(&theme_name)
		{
			common.current_theme_name = theme_name;
			common.current_theme = theme;
		}
		Self {
			id,
			buffer_id,
			content: ViewContent::FileBrowser(FileBrowserView {
				cursor,
				state: TextViewState {
					folds: FoldSet::default(),
					common,
				},
			}),
		}
	}

	pub(crate) fn new_markdown_preview(
		id: ViewId,
		buffer_id: BufferId,
		persistent_settings: &SettingsPersistent,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Self {
		Self {
			id,
			buffer_id,
			content: ViewContent::MarkdownPreview(MarkdownPreviewView {
				state: new_common_view_state(persistent_settings, viewport_width, viewport_lines),
			}),
		}
	}

	pub(crate) fn new_host_panel(
		id: ViewId,
		buffer_id: BufferId,
		persistent_settings: &SettingsPersistent,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Self {
		Self {
			id,
			buffer_id,
			content: ViewContent::HostPanel(HostPanelView {
				state: new_common_view_state(persistent_settings, viewport_width, viewport_lines),
			}),
		}
	}

	#[allow(dead_code, reason = "used by host-panel input routing, next cut")]
	pub(crate) fn is_host_panel(&self) -> bool {
		matches!(self.content, ViewContent::HostPanel(_))
	}

	pub(crate) fn is_file_browser(&self) -> bool {
		matches!(self.content, ViewContent::FileBrowser(_))
	}

	pub(crate) fn is_markdown_preview(&self) -> bool {
		matches!(self.content, ViewContent::MarkdownPreview(_))
	}

	pub(crate) fn common(&self) -> &CommonViewState {
		match &self.content {
			ViewContent::Text(view) => &view.state.common,
			ViewContent::FileBrowser(view) => &view.state.common,
			ViewContent::MarkdownPreview(view) => &view.state,
			ViewContent::HostPanel(view) => &view.state,
		}
	}

	pub(crate) fn common_mut(&mut self) -> &mut CommonViewState {
		match &mut self.content {
			ViewContent::Text(view) => &mut view.state.common,
			ViewContent::FileBrowser(view) => &mut view.state.common,
			ViewContent::MarkdownPreview(view) => &mut view.state,
			ViewContent::HostPanel(view) => &mut view.state,
		}
	}

	pub(crate) fn reset_minimap_cache(&mut self) {
		self.common_mut().reset_minimap_cache();
	}

	pub(crate) fn retarget_text_buffer(
		&mut self,
		buffer_id: BufferId,
		initial_leaf: NodeId,
		topology_rev: TopologyRev,
		state_id: StateId,
	) {
		let ViewContent::Text(text_view) = &mut self.content else {
			panic!("only a text view can be retargeted to a text buffer");
		};
		let mut cursor = CursorState::single(Caret::at_start());
		cursor
			.primary_mut()
			.reset_to_start(initial_leaf, topology_rev, state_id);
		text_view.cursor = cursor;
		text_view.state.folds.clear();
		text_view.state.common.scroll_y = 0;
		text_view.state.common.scroll_row_offset = DisplayRow::ZERO;
		text_view.state.common.reset_minimap_cache();
		text_view.match_index = None;
		text_view.match_info = None;
		self.buffer_id = buffer_id;
	}
}

pub(crate) struct TextEditor<'a> {
	pub(crate) document: &'a TextBuffer,
	pub(crate) cursor: &'a CursorState,
	pub(crate) view: &'a TextViewState,
}

pub(crate) struct TextEditorMut<'a> {
	pub(crate) document: &'a mut TextBuffer,
	pub(crate) cursor: &'a mut CursorState,
	pub(crate) view: &'a mut TextViewState,
	pub(crate) match_index: &'a mut Option<usize>,
	pub(crate) match_info: &'a mut Option<String>,
}

impl TextEditorMut<'_> {
	pub(crate) fn gutter_numbering(&self) -> Option<GutterNumbering> {
		self.view.gutter_numbering()
	}

	pub(crate) fn reset_pipeline(&mut self) {
		self.document.semantic.reset_pipeline();
		self.view.common.reset_minimap_cache();
	}
}

pub(crate) struct FileBrowserEditorMut<'a> {
	pub(crate) document: &'a mut FileBrowserBuffer,
	pub(crate) cursor: &'a mut CursorState,
	pub(crate) view: &'a mut TextViewState,
}

pub(crate) struct MarkdownPreviewEditorMut<'a> {
	pub(crate) document: &'a mut MarkdownPreviewBuffer,
	pub(crate) view: &'a mut CommonViewState,
}

/// Capability-typed synthetic-buffer mutation target. Text buffers use the
/// fan-out-aware `mutate_text_buffer` boundary below; synthetic buffers share
/// their buffer state while each view retains its own local state.
pub(crate) enum BufferMutationTarget<'a> {
	FileBrowser(FileBrowserEditorMut<'a>),
	MarkdownPreview(MarkdownPreviewEditorMut<'a>),
}

struct SharedTextViewSnapshot {
	view_id: ViewId,
	folds: FoldSnapshot,
}

pub(crate) fn text_editor<'a>(buffer: &'a Buffer, view: &'a View) -> Option<TextEditor<'a>> {
	if buffer.id != view.buffer_id {
		return None;
	}
	let (BufferContent::Text(document), ViewContent::Text(text_view)) =
		(&buffer.content, &view.content)
	else {
		return None;
	};
	Some(TextEditor {
		document,
		cursor: &text_view.cursor,
		view: &text_view.state,
	})
}

/// Pair a buffer with a view while the view is still detached from the live
/// layout. Once attached, callers must use `EditorSession::mutate_buffer`.
fn detached_text_editor_mut<'a>(
	buffer: &'a mut Buffer,
	view: &'a mut View,
) -> Option<TextEditorMut<'a>> {
	if buffer.id != view.buffer_id {
		return None;
	}
	let (BufferContent::Text(document), ViewContent::Text(text_view)) =
		(&mut buffer.content, &mut view.content)
	else {
		return None;
	};
	Some(TextEditorMut {
		cursor: &mut text_view.cursor,
		view: &mut text_view.state,
		match_index: &mut text_view.match_index,
		match_info: &mut text_view.match_info,
		document,
	})
}

/// Pair a synthetic buffer with a view before the view enters the live layout.
fn detached_file_browser_editor_mut<'a>(
	buffer: &'a mut Buffer,
	view: &'a mut View,
) -> Option<FileBrowserEditorMut<'a>> {
	if buffer.id != view.buffer_id {
		return None;
	}
	let (BufferContent::FileBrowser(document), ViewContent::FileBrowser(browser_view)) =
		(&mut buffer.content, &mut view.content)
	else {
		return None;
	};
	Some(FileBrowserEditorMut {
		document,
		cursor: &mut browser_view.cursor,
		view: &mut browser_view.state,
	})
}

pub(crate) fn with_text_buffer_mut<R>(
	registry: &UastRegistry,
	buffers: &mut BufferStore,
	layout: &mut LayoutNode,
	events: &mut EngineEventQueue,
	view_id: ViewId,
	mutate: impl FnOnce(&mut TextEditorMut<'_>) -> R,
) -> Option<R> {
	EditorSession::mutate_text_buffer(registry, buffers, layout, events, view_id, mutate)
}

pub(crate) fn with_file_browser_buffer_mut<R>(
	buffers: &mut BufferStore,
	layout: &mut LayoutNode,
	view_id: ViewId,
	mutate: impl FnOnce(&mut FileBrowserEditorMut<'_>) -> R,
) -> Option<R> {
	EditorSession::mutate_buffer(buffers, layout, view_id, |target| match target {
		BufferMutationTarget::FileBrowser(mut editor) => Some(mutate(&mut editor)),
		BufferMutationTarget::MarkdownPreview(_) => None,
	})
	.flatten()
}

pub(crate) fn with_markdown_preview_buffer_mut<R>(
	buffers: &mut BufferStore,
	layout: &mut LayoutNode,
	view_id: ViewId,
	mutate: impl FnOnce(&mut MarkdownPreviewEditorMut<'_>) -> R,
) -> Option<R> {
	EditorSession::mutate_buffer(buffers, layout, view_id, |target| match target {
		BufferMutationTarget::MarkdownPreview(mut editor) => Some(mutate(&mut editor)),
		BufferMutationTarget::FileBrowser(_) => None,
	})
	.flatten()
}

#[cfg(test)]
pub(crate) struct TestTextEditor {
	pub(crate) buffer: Buffer,
	pub(crate) view: View,
}

#[cfg(test)]
impl TestTextEditor {
	pub(crate) fn new(
		registry: &UastRegistry,
		settings: &SettingsPersistent,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Self {
		let (text, leaf) = TextBuffer::new_unnamed(registry).expect("test document capacity");
		let buffer_id = BufferId::new(1);
		Self {
			buffer: Buffer {
				id: buffer_id,
				content: BufferContent::Text(text),
				role: BufferRole::Document,
			},
			view: View::new_text(
				ViewId::new(1),
				buffer_id,
				leaf,
				settings,
				viewport_width,
				viewport_lines,
			),
		}
	}

	pub(crate) fn from_existing_text(
		root: NodeId,
		leaf: NodeId,
		settings: &SettingsPersistent,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Self {
		let buffer_id = BufferId::new(1);
		Self {
			buffer: Buffer {
				id: buffer_id,
				content: BufferContent::Text(TextBuffer {
					root_id: root,
					uast_accounting: UastOwnerAccounting::new(2),
					parked_generations: ParkedGenerationCache::default(),
					pending_root_transition: None,
					pending_parked_retirements: 0,
					pending_parked_payload_retirements: 0,
					topology_rev: TopologyRev::ZERO,
					file_path: None,
					encrypted_backing: None,
					virtual_path: None,
					canonical_path: None,
					backing_path: None,
					backing_device_id: None,
					file_stamp: None,
					active_language_id: None,
					language_selection: LanguageSelection::Auto,
					shadow: None,
					ledger: UndoLedger::new(),
					search_matches: Vec::new(),
					search_revision: 0,
					search_pattern_revision: 0,
					navigation_anchors: super::navigation::NavigationAnchorStore::default(),
					semantic: Box::new(SemanticState::new()),
				}),
				role: BufferRole::Document,
			},
			view: View::new_text(
				ViewId::new(1),
				buffer_id,
				leaf,
				settings,
				viewport_width,
				viewport_lines,
			),
		}
	}

	pub(crate) fn editor_mut(&mut self) -> TextEditorMut<'_> {
		detached_text_editor_mut(&mut self.buffer, &mut self.view).expect("text test fixture")
	}
}

pub(crate) struct EditorPane {
	pub(crate) tabs: Vec<View>,
	pub(crate) active_tab_index: usize,
}

impl EditorPane {
	pub(crate) fn from_view(view: View) -> Self {
		Self {
			tabs: vec![view],
			active_tab_index: 0,
		}
	}

	pub(crate) fn new_empty(
		id: ViewId,
		buffer_id: BufferId,
		initial_leaf: NodeId,
		persistent_settings: &SettingsPersistent,
	) -> Self {
		Self {
			tabs: vec![View::new_text(
				id,
				buffer_id,
				initial_leaf,
				persistent_settings,
				DEFAULT_VIEWPORT_WIDTH,
				DEFAULT_VIEWPORT_LINES,
			)],
			active_tab_index: 0,
		}
	}

	pub(crate) fn active_tab(&self) -> &View {
		&self.tabs[self.active_tab_index]
	}

	pub(crate) fn active_tab_mut(&mut self) -> &mut View {
		&mut self.tabs[self.active_tab_index]
	}

	pub(crate) fn activate_view(&mut self, view_id: ViewId) -> bool {
		let Some(index) = self.tabs.iter().position(|tab| tab.id == view_id) else {
			return false;
		};
		self.active_tab_index = index;
		true
	}
}

pub(crate) struct LayoutLeaf {
	pub(crate) group_root: Option<crate::engine::HostPanelGroupId>,
	pub(crate) pane_id: PaneId,
	pub(crate) pane: EditorPane,
}

pub(crate) enum LayoutNode {
	Split {
		group_root: Option<crate::engine::HostPanelGroupId>,
		direction: SplitDirection,
		children: Vec<LayoutNode>,
		ratios: Vec<u32>,
	},
	Leaf(LayoutLeaf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PaneLayout {
	pub(crate) pane_id: PaneId,
	pub(crate) rect: PaneRect,
}

const DEFAULT_SPLIT_RATIO: u32 = 8;
const MIN_PANE_EXTENT: u32 = 2;

fn split_divider_cells(reservation: PaneDividerReservation, direction: SplitDirection) -> u32 {
	reservation.cells(direction)
}

fn balanced_tile_columns(member_count: usize) -> usize {
	let mut columns = 1usize;
	while columns.saturating_mul(columns) < member_count.max(1) {
		columns = columns.saturating_add(1);
	}
	columns
}

fn host_panel_tile_minimum(member_count: usize, reservation: PaneDividerReservation) -> (u32, u32) {
	if member_count == 0 {
		return (0, 0);
	}
	let columns = balanced_tile_columns(member_count);
	let rows = member_count.div_ceil(columns);
	let width = MIN_PANE_EXTENT
		.saturating_mul(columns as u32)
		.saturating_add(
			reservation
				.columns
				.saturating_mul(columns.saturating_sub(1) as u32),
		);
	let height = MIN_PANE_EXTENT.saturating_mul(rows as u32).saturating_add(
		reservation
			.rows
			.saturating_mul(rows.saturating_sub(1) as u32),
	);
	(width, height)
}

fn build_host_panel_group_tile(
	group_id: HostPanelGroupId,
	members: Vec<(PaneId, EditorPane, HostPanelGroupShape)>,
	layout: HostPanelGroupLayout,
) -> LayoutNode {
	assert!(!members.is_empty(), "a materialized group is non-empty");
	match layout {
		HostPanelGroupLayout::RowMajorTile => {}
	}
	let columns = balanced_tile_columns(members.len());
	let mut rows = Vec::new();
	let mut members = members.into_iter();
	loop {
		let row_members = members.by_ref().take(columns).collect::<Vec<_>>();
		if row_members.is_empty() {
			break;
		}
		let mut ratios = Vec::with_capacity(row_members.len());
		let mut children = Vec::with_capacity(row_members.len());
		for (pane_id, pane, shape) in row_members {
			ratios.push(match shape {
				HostPanelGroupShape::Compact => 1,
				HostPanelGroupShape::Expansive => 3,
			});
			children.push(LayoutNode::Leaf(LayoutLeaf {
				group_root: None,
				pane_id,
				pane,
			}));
		}
		rows.push(if children.len() == 1 {
			children.pop().expect("one row child")
		} else {
			LayoutNode::Split {
				group_root: None,
				direction: SplitDirection::Vertical,
				children,
				ratios,
			}
		});
	}
	let mut root = if rows.len() == 1 {
		rows.pop().expect("one tile row")
	} else {
		let row_count = rows.len();
		LayoutNode::Split {
			group_root: None,
			direction: SplitDirection::Horizontal,
			children: rows,
			ratios: vec![DEFAULT_SPLIT_RATIO; row_count],
		}
	};
	root.set_group_root(Some(group_id));
	root
}

fn build_plain_pane_branch(mut panes: Vec<(PaneId, EditorPane)>) -> Option<LayoutNode> {
	if panes.is_empty() {
		return None;
	}
	if panes.len() == 1 {
		let (pane_id, pane) = panes.pop().expect("one plain pane");
		return Some(LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id,
			pane,
		}));
	}
	let count = panes.len();
	Some(LayoutNode::Split {
		group_root: None,
		direction: SplitDirection::Horizontal,
		children: panes
			.into_iter()
			.map(|(pane_id, pane)| {
				LayoutNode::Leaf(LayoutLeaf {
					group_root: None,
					pane_id,
					pane,
				})
			})
			.collect(),
		ratios: vec![DEFAULT_SPLIT_RATIO; count],
	})
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PaneResizeResult {
	Resized,
	NoMatchingSplit,
	AtLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResizeLeafResult {
	NotFound,
	Resized,
	NoMatchingAncestor,
	AtLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupRootEditResult {
	NotFound,
	Replaced,
	RemoveSelf,
}

impl LayoutNode {
	fn group_root(&self) -> Option<HostPanelGroupId> {
		match self {
			Self::Split { group_root, .. } => *group_root,
			Self::Leaf(leaf) => leaf.group_root,
		}
	}

	fn set_group_root(&mut self, value: Option<HostPanelGroupId>) {
		match self {
			Self::Split { group_root, .. } => *group_root = value,
			Self::Leaf(leaf) => leaf.group_root = value,
		}
	}

	fn find_group_root(&self, target: HostPanelGroupId) -> Option<&Self> {
		if self.group_root() == Some(target) {
			return Some(self);
		}
		match self {
			Self::Split { children, .. } => children
				.iter()
				.find_map(|child| child.find_group_root(target)),
			Self::Leaf(_) => None,
		}
	}

	fn find_group_root_mut(&mut self, target: HostPanelGroupId) -> Option<&mut Self> {
		if self.group_root() == Some(target) {
			return Some(self);
		}
		match self {
			Self::Split { children, .. } => children
				.iter_mut()
				.find_map(|child| child.find_group_root_mut(target)),
			Self::Leaf(_) => None,
		}
	}

	fn group_containing_pane(&self, target: PaneId) -> Option<HostPanelGroupId> {
		if !self.contains_pane(target) {
			return None;
		}
		if let Some(group_id) = self.group_root() {
			return Some(group_id);
		}
		match self {
			Self::Split { children, .. } => children
				.iter()
				.find_map(|child| child.group_containing_pane(target)),
			Self::Leaf(_) => None,
		}
	}

	fn structure(&self) -> String {
		fn write(node: &LayoutNode, out: &mut String) {
			match node {
				LayoutNode::Leaf(leaf) => {
					out.push_str("L:");
					out.push_str(&leaf.pane_id.get().to_string());
				}
				LayoutNode::Split {
					direction,
					children,
					ratios,
					..
				} => {
					out.push_str(match direction {
						SplitDirection::Horizontal => "H[",
						SplitDirection::Vertical => "V[",
					});
					for ratio in ratios {
						out.push_str(&ratio.to_string());
						out.push(',');
					}
					out.push_str("]{");
					for child in children {
						write(child, out);
						out.push(';');
					}
					out.push('}');
				}
			}
		}

		let mut structure = String::new();
		write(self, &mut structure);
		structure
	}

	fn group_structure(&self, target: HostPanelGroupId) -> Option<String> {
		self.find_group_root(target).map(Self::structure)
	}

	fn rebuild_group_root(
		&mut self,
		target: HostPanelGroupId,
		rebuild: &mut Option<impl FnOnce(LayoutNode) -> LayoutNode>,
	) -> bool {
		if self.group_root() == Some(target) {
			let placeholder = LayoutNode::Split {
				group_root: None,
				direction: SplitDirection::Vertical,
				children: Vec::new(),
				ratios: Vec::new(),
			};
			let old = std::mem::replace(self, placeholder);
			*self = rebuild
				.take()
				.expect("a group root rebuild runs exactly once")(old);
			return true;
		}
		match self {
			Self::Split { children, .. } => children
				.iter_mut()
				.any(|child| child.rebuild_group_root(target, rebuild)),
			Self::Leaf(_) => false,
		}
	}

	fn reconcile_group_root(
		&mut self,
		target: HostPanelGroupId,
		reconcile: &mut Option<impl FnOnce(LayoutNode) -> Option<LayoutNode>>,
	) -> GroupRootEditResult {
		if self.group_root() == Some(target) {
			let placeholder = LayoutNode::Split {
				group_root: None,
				direction: SplitDirection::Vertical,
				children: Vec::new(),
				ratios: Vec::new(),
			};
			let old = std::mem::replace(self, placeholder);
			return if let Some(replacement) =
				reconcile
					.take()
					.expect("a group reconciliation runs exactly once")(old)
			{
				*self = replacement;
				GroupRootEditResult::Replaced
			} else {
				GroupRootEditResult::RemoveSelf
			};
		}

		let Self::Split {
			group_root,
			children,
			ratios,
			..
		} = self
		else {
			return GroupRootEditResult::NotFound;
		};
		let inherited_group = *group_root;
		let mut index = 0usize;
		while index < children.len() {
			match children[index].reconcile_group_root(target, reconcile) {
				GroupRootEditResult::NotFound => index += 1,
				GroupRootEditResult::Replaced => return GroupRootEditResult::Replaced,
				GroupRootEditResult::RemoveSelf => {
					children.remove(index);
					ratios.remove(index);
					if children.len() == 1 {
						let mut remaining = children.remove(0);
						if let Some(group_id) = inherited_group {
							debug_assert!(remaining.group_root().is_none());
							remaining.set_group_root(Some(group_id));
						}
						*self = remaining;
					}
					return GroupRootEditResult::Replaced;
				}
			}
		}
		GroupRootEditResult::NotFound
	}

	fn into_leaf_panes(self, out: &mut Vec<(PaneId, EditorPane)>) {
		match self {
			Self::Leaf(leaf) => out.push((leaf.pane_id, leaf.pane)),
			Self::Split { children, .. } => {
				for child in children {
					child.into_leaf_panes(out);
				}
			}
		}
	}

	fn contains_group_root(&self, target: HostPanelGroupId) -> bool {
		self.find_group_root(target).is_some()
	}

	fn can_host_group_subtree(&self, target: PaneId) -> bool {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => leaf.group_root.is_none(),
			Self::Leaf(_) => false,
			Self::Split { children, .. } => children
				.iter()
				.any(|child| child.can_host_group_subtree(target)),
		}
	}

	fn count_group_roots(&self, target: HostPanelGroupId) -> usize {
		let own = usize::from(self.group_root() == Some(target));
		match self {
			Self::Split { children, .. } => {
				own + children
					.iter()
					.map(|child| child.count_group_roots(target))
					.sum::<usize>()
			}
			Self::Leaf(_) => own,
		}
	}

	pub(crate) fn find_pane(&self, target: PaneId) -> Option<&EditorPane> {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => Some(&leaf.pane),
			Self::Leaf(_) => None,
			Self::Split { children, .. } => {
				children.iter().find_map(|child| child.find_pane(target))
			}
		}
	}

	pub(crate) fn find_pane_mut(&mut self, target: PaneId) -> Option<&mut EditorPane> {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => Some(&mut leaf.pane),
			Self::Leaf(_) => None,
			Self::Split { children, .. } => children
				.iter_mut()
				.find_map(|child| child.find_pane_mut(target)),
		}
	}

	fn take_leaf_contents(&mut self, target: PaneId) -> Option<EditorPane> {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => Some(std::mem::replace(
				&mut leaf.pane,
				EditorPane {
					tabs: Vec::new(),
					active_tab_index: 0,
				},
			)),
			Self::Leaf(_) => None,
			Self::Split { children, .. } => children
				.iter_mut()
				.find_map(|child| child.take_leaf_contents(target)),
		}
	}

	fn set_leaf_contents(&mut self, target: PaneId, pane: EditorPane) -> bool {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => {
				leaf.pane = pane;
				true
			}
			Self::Leaf(_) => false,
			Self::Split { children, .. } => {
				for child in children {
					if child.contains_pane(target) {
						return child.set_leaf_contents(target, pane);
					}
				}
				false
			}
		}
	}

	pub(crate) fn find_view(&self, target: ViewId) -> Option<&View> {
		match self {
			Self::Leaf(leaf) => leaf.pane.tabs.iter().find(|tab| tab.id == target),
			Self::Split { children, .. } => {
				children.iter().find_map(|child| child.find_view(target))
			}
		}
	}

	pub(crate) fn find_view_mut(&mut self, target: ViewId) -> Option<&mut View> {
		match self {
			Self::Leaf(leaf) => leaf.pane.tabs.iter_mut().find(|tab| tab.id == target),
			Self::Split { children, .. } => children
				.iter_mut()
				.find_map(|child| child.find_view_mut(target)),
		}
	}

	pub(crate) fn pane_id_for_view(&self, target: ViewId) -> Option<PaneId> {
		match self {
			Self::Leaf(leaf) => leaf
				.pane
				.tabs
				.iter()
				.any(|tab| tab.id == target)
				.then_some(leaf.pane_id),
			Self::Split { children, .. } => children
				.iter()
				.find_map(|child| child.pane_id_for_view(target)),
		}
	}

	pub(crate) fn view_id_for_buffer(&self, target: BufferId) -> Option<ViewId> {
		match self {
			Self::Leaf(leaf) => leaf
				.pane
				.tabs
				.iter()
				.find_map(|view| (view.buffer_id == target).then_some(view.id)),
			Self::Split { children, .. } => children
				.iter()
				.find_map(|child| child.view_id_for_buffer(target)),
		}
	}

	pub(crate) fn split_leaf_with_new_pane_order(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		new_pane_id: PaneId,
		new_pane: EditorPane,
		new_first: bool,
	) -> bool {
		let mut new_pane = Some(new_pane);
		self.split_leaf_inner_with_order(target, direction, new_pane_id, &mut new_pane, new_first)
	}

	/// Insert an already-existing pane beside `target`. Unlike a normal split,
	/// this preserves the pane identity and all of its tabs/view state.
	pub(crate) fn split_leaf_with_existing_pane_order(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		pane_id: PaneId,
		pane: EditorPane,
		new_first: bool,
	) -> bool {
		let mut pane = Some(pane);
		self.split_leaf_inner_with_order(target, direction, pane_id, &mut pane, new_first)
	}

	/// Insert a fully-built subtree beside one existing leaf with one mutation.
	/// Used by atomic host-panel group placement so no temporary split chain is
	/// ever projected.
	pub(crate) fn split_leaf_with_subtree(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		subtree: LayoutNode,
		new_first: bool,
	) -> bool {
		let mut subtree = Some(subtree);
		self.split_leaf_with_subtree_inner(target, direction, &mut subtree, new_first)
	}

	fn split_leaf_with_subtree_inner(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		subtree: &mut Option<LayoutNode>,
		new_first: bool,
	) -> bool {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => {
				if leaf.group_root.is_some() {
					return false;
				}
				let old_leaf = match std::mem::replace(
					self,
					Self::Split {
						group_root: None,
						direction,
						children: Vec::new(),
						ratios: Vec::new(),
					},
				) {
					Self::Leaf(leaf) => Self::Leaf(leaf),
					Self::Split { .. } => unreachable!("matched a leaf"),
				};
				let subtree = subtree
					.take()
					.expect("subtree target consumes the candidate exactly once");
				let children = if new_first {
					vec![subtree, old_leaf]
				} else {
					vec![old_leaf, subtree]
				};
				*self = Self::Split {
					group_root: None,
					direction,
					children,
					ratios: vec![DEFAULT_SPLIT_RATIO, DEFAULT_SPLIT_RATIO],
				};
				true
			}
			Self::Leaf(_) => false,
			Self::Split { children, .. } => children.iter_mut().any(|child| {
				child.split_leaf_with_subtree_inner(target, direction, subtree, new_first)
			}),
		}
	}

	/// Exchange the contents of two leaf positions while leaving the layout
	/// shape intact. Pane ids name positions in the tree; views/buffers (and
	/// therefore host-panel identities) travel with the dragged content.
	pub(crate) fn swap_leaf_contents(&mut self, first: PaneId, second: PaneId) -> bool {
		if first == second || !self.contains_pane(first) || !self.contains_pane(second) {
			return false;
		}
		let Some(first_pane) = self.take_leaf_contents(first) else {
			return false;
		};
		let Some(second_pane) = self.take_leaf_contents(second) else {
			return false;
		};
		let first_set = self.set_leaf_contents(first, second_pane);
		let second_set = self.set_leaf_contents(second, first_pane);
		debug_assert!(
			first_set && second_set,
			"prechecked leaves must still exist"
		);
		first_set && second_set
	}

	pub(crate) fn for_each_doctab_mut(&mut self, visit: &mut impl FnMut(&mut View)) {
		match self {
			Self::Leaf(leaf) => {
				for tab in &mut leaf.pane.tabs {
					visit(tab);
				}
			}
			Self::Split { children, .. } => {
				for child in children {
					child.for_each_doctab_mut(visit);
				}
			}
		}
	}

	pub(crate) fn for_each_doctab(&self, visit: &mut impl FnMut(&View)) {
		match self {
			Self::Leaf(leaf) => {
				for tab in &leaf.pane.tabs {
					visit(tab);
				}
			}
			Self::Split { children, .. } => {
				for child in children {
					child.for_each_doctab(visit);
				}
			}
		}
	}

	pub(crate) fn try_for_each_doctab<E>(
		&self,
		visit: &mut impl FnMut(&View) -> Result<(), E>,
	) -> Result<(), E> {
		match self {
			Self::Leaf(leaf) => {
				for tab in &leaf.pane.tabs {
					visit(tab)?;
				}
			}
			Self::Split { children, .. } => {
				for child in children {
					child.try_for_each_doctab(visit)?;
				}
			}
		}
		Ok(())
	}

	pub(crate) fn pane_count(&self) -> usize {
		match self {
			Self::Leaf(_) => 1,
			Self::Split { children, .. } => children.iter().map(Self::pane_count).sum(),
		}
	}

	pub(crate) fn collect_pane_ids(&self, out: &mut Vec<PaneId>) {
		match self {
			Self::Leaf(leaf) => out.push(leaf.pane_id),
			Self::Split { children, .. } => {
				for child in children {
					child.collect_pane_ids(out);
				}
			}
		}
	}

	pub(crate) fn collect_view_ids(&self, out: &mut Vec<ViewId>) {
		match self {
			Self::Leaf(leaf) => out.extend(leaf.pane.tabs.iter().map(|view| view.id)),
			Self::Split { children, .. } => {
				for child in children {
					child.collect_view_ids(out);
				}
			}
		}
	}

	/// Count matching views in layout order while materializing at most `maximum` ids.
	pub(crate) fn collect_view_ids_for_buffer(
		&self,
		buffer_id: BufferId,
		out: &mut Vec<ViewId>,
		maximum: usize,
	) -> usize {
		match self {
			Self::Leaf(leaf) => {
				let mut count = 0usize;
				for view in &leaf.pane.tabs {
					if view.buffer_id == buffer_id {
						count = count.saturating_add(1);
						if out.len() < maximum {
							out.push(view.id);
						}
					}
				}
				count
			}
			Self::Split { children, .. } => {
				let mut count = 0usize;
				for child in children {
					count = count
						.saturating_add(child.collect_view_ids_for_buffer(buffer_id, out, maximum));
				}
				count
			}
		}
	}

	pub(crate) fn view_count_for_buffer(&self, buffer_id: BufferId) -> usize {
		match self {
			Self::Leaf(leaf) => leaf
				.pane
				.tabs
				.iter()
				.filter(|view| view.buffer_id == buffer_id)
				.count(),
			Self::Split { children, .. } => children
				.iter()
				.map(|child| child.view_count_for_buffer(buffer_id))
				.sum(),
		}
	}

	pub(crate) fn first_leaf_id(&self) -> Option<PaneId> {
		match self {
			Self::Leaf(leaf) => Some(leaf.pane_id),
			Self::Split { children, .. } => children.iter().find_map(Self::first_leaf_id),
		}
	}

	pub(crate) fn split_leaf(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		new_pane_id: PaneId,
		new_pane: EditorPane,
	) -> bool {
		self.split_leaf_with_new_pane_order(target, direction, new_pane_id, new_pane, false)
	}

	fn split_leaf_inner_with_order(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		new_pane_id: PaneId,
		new_pane: &mut Option<EditorPane>,
		new_first: bool,
	) -> bool {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => {
				let group_root = leaf.group_root;
				let mut old_leaf = match std::mem::replace(
					self,
					Self::Split {
						group_root,
						direction,
						children: Vec::new(),
						ratios: Vec::new(),
					},
				) {
					Self::Leaf(leaf) => leaf,
					Self::Split { .. } => unreachable!("replaced a leaf with a split"),
				};
				old_leaf.group_root = None;
				let new_leaf = Self::Leaf(LayoutLeaf {
					group_root: None,
					pane_id: new_pane_id,
					pane: new_pane
						.take()
						.expect("split target must consume the new pane exactly once"),
				});
				let old_leaf = Self::Leaf(old_leaf);
				let children = if new_first {
					vec![new_leaf, old_leaf]
				} else {
					vec![old_leaf, new_leaf]
				};
				*self = Self::Split {
					group_root,
					direction,
					children,
					ratios: vec![DEFAULT_SPLIT_RATIO, DEFAULT_SPLIT_RATIO],
				};
				true
			}
			Self::Leaf(_) => false,
			Self::Split { children, .. } => children.iter_mut().any(|child| {
				child.split_leaf_inner_with_order(
					target,
					direction,
					new_pane_id,
					new_pane,
					new_first,
				)
			}),
		}
	}

	pub(crate) fn resize_leaf(
		&mut self,
		target: PaneId,
		axis: SplitDirection,
		direction: PaneResizeDirection,
		rect: PaneRect,
		reservation: PaneDividerReservation,
	) -> PaneResizeResult {
		match self.resize_leaf_inner(target, axis, direction, rect, reservation) {
			ResizeLeafResult::Resized => PaneResizeResult::Resized,
			ResizeLeafResult::NotFound | ResizeLeafResult::NoMatchingAncestor => {
				PaneResizeResult::NoMatchingSplit
			}
			ResizeLeafResult::AtLimit => PaneResizeResult::AtLimit,
		}
	}

	pub(crate) fn resize_boundary(
		&mut self,
		before: PaneId,
		after: PaneId,
		axis: SplitDirection,
		target: u32,
		rect: PaneRect,
		reservation: PaneDividerReservation,
	) -> bool {
		self.resize_boundary_inner(before, after, axis, target, rect, reservation)
	}

	fn contains_pane(&self, target: PaneId) -> bool {
		match self {
			Self::Leaf(leaf) => leaf.pane_id == target,
			Self::Split { children, .. } => {
				children.iter().any(|child| child.contains_pane(target))
			}
		}
	}

	fn minimum_extent(&self, axis: SplitDirection, reservation: PaneDividerReservation) -> u32 {
		match self {
			Self::Leaf(_) => MIN_PANE_EXTENT,
			Self::Split {
				direction,
				children,
				..
			} if *direction == axis => {
				let child_extent = children.iter().fold(0u32, |extent, child| {
					extent.saturating_add(child.minimum_extent(axis, reservation))
				});
				child_extent.saturating_add(
					split_divider_cells(reservation, *direction)
						.saturating_mul(children.len().saturating_sub(1) as u32),
				)
			}
			Self::Split { children, .. } => children
				.iter()
				.map(|child| child.minimum_extent(axis, reservation))
				.max()
				.unwrap_or(MIN_PANE_EXTENT),
		}
	}

	fn resize_boundary_inner(
		&mut self,
		before: PaneId,
		after: PaneId,
		axis: SplitDirection,
		target: u32,
		rect: PaneRect,
		reservation: PaneDividerReservation,
	) -> bool {
		let Self::Split {
			direction,
			children,
			ratios,
			..
		} = self
		else {
			return false;
		};
		let Some(before_index) = children
			.iter()
			.position(|child| child.contains_pane(before))
		else {
			return false;
		};
		let Some(after_index) = children.iter().position(|child| child.contains_pane(after)) else {
			return false;
		};

		let mut child_rects =
			split_child_rects(rect, *direction, ratios, children.len(), reservation);

		if before_index != after_index {
			if *direction != axis || after_index != before_index.saturating_add(1) {
				return false;
			}
			let child_extent = child_rects
				.iter()
				.map(|rect| rect_extent(*rect, *direction))
				.sum();
			let minimum_extents = children
				.iter()
				.map(|child| child.minimum_extent(*direction, reservation))
				.collect::<Vec<_>>();
			let mut resized = false;
			loop {
				let boundary = rect_boundary(child_rects[before_index], *direction);
				if boundary == target {
					return resized;
				}
				if !resize_split_boundary_ratios(
					ratios,
					before_index,
					boundary < target,
					child_extent,
					&minimum_extents,
				) {
					return resized;
				}
				resized = true;
				child_rects =
					split_child_rects(rect, *direction, ratios, children.len(), reservation);
			}
		}

		children[before_index].resize_boundary_inner(
			before,
			after,
			axis,
			target,
			child_rects[before_index],
			reservation,
		)
	}

	fn resize_leaf_inner(
		&mut self,
		target: PaneId,
		axis: SplitDirection,
		direction: PaneResizeDirection,
		rect: PaneRect,
		reservation: PaneDividerReservation,
	) -> ResizeLeafResult {
		match self {
			Self::Leaf(leaf) if leaf.pane_id == target => ResizeLeafResult::NoMatchingAncestor,
			Self::Leaf(_) => ResizeLeafResult::NotFound,
			Self::Split {
				direction: split_direction,
				children,
				ratios,
				..
			} => {
				let split_direction = *split_direction;
				if children.is_empty() {
					return ResizeLeafResult::NotFound;
				}

				let child_rects =
					split_child_rects(rect, split_direction, ratios, children.len(), reservation);
				let child_extent = child_rects
					.iter()
					.map(|rect| rect_extent(*rect, split_direction))
					.sum();
				let minimum_extents = children
					.iter()
					.map(|child| child.minimum_extent(split_direction, reservation))
					.collect::<Vec<_>>();

				for (child_index, (child, child_rect)) in
					children.iter_mut().zip(child_rects).enumerate()
				{
					let child_result =
						child.resize_leaf_inner(target, axis, direction, child_rect, reservation);
					match child_result {
						ResizeLeafResult::NotFound => {}
						ResizeLeafResult::Resized => return ResizeLeafResult::Resized,
						ResizeLeafResult::AtLimit => return ResizeLeafResult::AtLimit,
						ResizeLeafResult::NoMatchingAncestor => {
							if split_direction == axis {
								return if resize_split_ratios(
									ratios,
									child_index,
									direction,
									child_extent,
									&minimum_extents,
								) {
									ResizeLeafResult::Resized
								} else {
									ResizeLeafResult::AtLimit
								};
							}
							return ResizeLeafResult::NoMatchingAncestor;
						}
					}
				}
				ResizeLeafResult::NotFound
			}
		}
	}

	pub(crate) fn take_leaf(&mut self, target: PaneId) -> Option<EditorPane> {
		match self {
			Self::Leaf(_) => None,
			Self::Split {
				group_root,
				children,
				ratios,
				..
			} => {
				let collapsing_group = *group_root;
				let mut child_index = 0usize;
				while child_index < children.len() {
					let remove_child = matches!(&children[child_index], Self::Leaf(leaf) if leaf.pane_id == target);
					let removed_pane = if remove_child {
						let removed = children.remove(child_index);
						ratios.remove(child_index);
						let Self::Leaf(leaf) = removed else {
							unreachable!("direct pane child must be a leaf")
						};
						Some(leaf.pane)
					} else {
						children[child_index].take_leaf(target)
					};
					if let Some(removed_pane) = removed_pane {
						if children.len() == 1 {
							let mut remaining = children.remove(0);
							if let Some(group_id) = collapsing_group {
								debug_assert!(
									remaining.group_root().is_none(),
									"nested group roots are not an automatic layout"
								);
								remaining.set_group_root(Some(group_id));
							}
							*self = remaining;
						}
						return Some(removed_pane);
					}
					child_index += 1;
				}
				None
			}
		}
	}

	pub(crate) fn collect_pane_layouts(
		&self,
		rect: PaneRect,
		out: &mut Vec<PaneLayout>,
		reservation: PaneDividerReservation,
	) {
		match self {
			Self::Leaf(leaf) => out.push(PaneLayout {
				pane_id: leaf.pane_id,
				rect,
			}),
			Self::Split {
				direction,
				children,
				ratios,
				..
			} => {
				if children.is_empty() {
					return;
				}

				let child_rects =
					split_child_rects(rect, *direction, ratios, children.len(), reservation);
				for (child, child_rect) in children.iter().zip(child_rects) {
					child.collect_pane_layouts(child_rect, out, reservation);
				}
			}
		}
	}
}

fn rect_extent(rect: PaneRect, direction: SplitDirection) -> u32 {
	match direction {
		SplitDirection::Horizontal => rect.height.get(),
		SplitDirection::Vertical => rect.width.get(),
	}
}

fn rect_boundary(rect: PaneRect, direction: SplitDirection) -> u32 {
	match direction {
		SplitDirection::Horizontal => rect.bottom().get(),
		SplitDirection::Vertical => rect.right().get(),
	}
}

fn split_child_rects(
	rect: PaneRect,
	direction: SplitDirection,
	ratios: &[u32],
	child_count: usize,
	reservation: PaneDividerReservation,
) -> Vec<PaneRect> {
	if child_count == 0 {
		return Vec::new();
	}

	let divider_cells = split_divider_cells(reservation, direction);
	let divider_total = divider_cells.saturating_mul(child_count.saturating_sub(1) as u32);
	let child_extent = rect_extent(rect, direction).saturating_sub(divider_total);
	let child_sizes = split_extent(child_extent, ratios, child_count);
	let mut next_col = rect.screen_col;
	let mut next_row = rect.screen_row;
	child_sizes
		.into_iter()
		.map(|child_size| {
			let child_rect = match direction {
				SplitDirection::Horizontal => PaneRect::new(
					rect.screen_col,
					next_row,
					rect.width,
					PaneRow::new(child_size),
				),
				SplitDirection::Vertical => PaneRect::new(
					next_col,
					rect.screen_row,
					PaneCol::new(child_size),
					rect.height,
				),
			};
			match direction {
				SplitDirection::Horizontal => {
					next_row = ScreenRow::new(
						next_row
							.get()
							.saturating_add(child_size)
							.saturating_add(divider_cells),
					);
				}
				SplitDirection::Vertical => {
					next_col = ScreenCol::new(
						next_col
							.get()
							.saturating_add(child_size)
							.saturating_add(divider_cells),
					);
				}
			}
			child_rect
		})
		.collect()
}

fn split_extent(total_extent: u32, ratios: &[u32], child_count: usize) -> Vec<u32> {
	let mut sizes = Vec::with_capacity(child_count);
	let mut used = 0u32;
	let total_ratio = ratios.iter().copied().sum::<u32>().max(child_count as u32);

	for index in 0..child_count {
		let size = if index + 1 == child_count {
			total_extent.saturating_sub(used)
		} else {
			total_extent.saturating_mul(*ratios.get(index).unwrap_or(&1)) / total_ratio
		};
		sizes.push(size);
		used = used.saturating_add(size);
	}

	sizes
}

fn resize_split_ratios(
	ratios: &mut [u32],
	focused_index: usize,
	direction: PaneResizeDirection,
	total_extent: u32,
	minimum_extents: &[u32],
) -> bool {
	if ratios.len() <= 1 || focused_index >= ratios.len() || total_extent == 0 {
		return false;
	}

	let mut candidate = ratios.to_vec();
	ensure_resize_resolution(&mut candidate, total_extent);
	let original_focus_size =
		split_extent(total_extent, &candidate, candidate.len())[focused_index];

	let changed = match direction {
		PaneResizeDirection::Increase => transfer_ratio_to_focused_until_size_change(
			&mut candidate,
			focused_index,
			total_extent,
			original_focus_size,
			minimum_extents,
		),
		PaneResizeDirection::Decrease => transfer_ratio_from_focused_until_size_change(
			&mut candidate,
			focused_index,
			total_extent,
			original_focus_size,
			minimum_extents,
		),
	};

	if changed {
		ratios.copy_from_slice(&candidate);
	}

	changed
}

fn resize_split_boundary_ratios(
	ratios: &mut [u32],
	before_index: usize,
	move_forward: bool,
	total_extent: u32,
	minimum_extents: &[u32],
) -> bool {
	let after_index = before_index.saturating_add(1);
	if after_index >= ratios.len() || total_extent == 0 {
		return false;
	}

	let mut candidate = ratios.to_vec();
	ensure_resize_resolution(&mut candidate, total_extent);
	let boundary_extent = |values: &[u32]| {
		split_extent(total_extent, values, values.len())
			.into_iter()
			.take(after_index)
			.sum::<u32>()
	};
	let original_boundary = boundary_extent(&candidate);
	let (recipient, donor) = if move_forward {
		(before_index, after_index)
	} else {
		(after_index, before_index)
	};

	loop {
		let child_sizes = split_extent(total_extent, &candidate, candidate.len());
		if candidate[donor] <= 1
			|| child_sizes[donor]
				<= minimum_extents
					.get(donor)
					.copied()
					.unwrap_or(MIN_PANE_EXTENT)
		{
			return false;
		}
		candidate[donor] -= 1;
		candidate[recipient] += 1;
		let new_boundary = boundary_extent(&candidate);
		if (move_forward && new_boundary > original_boundary)
			|| (!move_forward && new_boundary < original_boundary)
		{
			ratios.copy_from_slice(&candidate);
			return true;
		}
	}
}

fn ensure_resize_resolution(ratios: &mut [u32], total_extent: u32) {
	let total_ratio = ratios.iter().copied().sum::<u32>().max(ratios.len() as u32);
	let target_ratio = total_extent.max(ratios.len() as u32);
	if total_ratio >= target_ratio {
		return;
	}

	let scale = target_ratio.div_ceil(total_ratio).max(1);
	for ratio in ratios {
		*ratio = ratio.saturating_mul(scale);
	}
}

fn transfer_ratio_to_focused_until_size_change(
	ratios: &mut [u32],
	focused_index: usize,
	total_extent: u32,
	original_focus_size: u32,
	minimum_extents: &[u32],
) -> bool {
	loop {
		let child_sizes = split_extent(total_extent, ratios, ratios.len());
		let Some((donor_index, _)) = ratios
			.iter()
			.enumerate()
			.filter(|(index, ratio)| {
				*index != focused_index
					&& **ratio > 1 && child_sizes[*index]
					> minimum_extents
						.get(*index)
						.copied()
						.unwrap_or(MIN_PANE_EXTENT)
			})
			.max_by_key(|(_, ratio)| **ratio)
		else {
			return false;
		};

		ratios[donor_index] -= 1;
		ratios[focused_index] += 1;

		let new_focus_size = split_extent(total_extent, ratios, ratios.len())[focused_index];
		if new_focus_size > original_focus_size {
			return true;
		}
	}
}

fn transfer_ratio_from_focused_until_size_change(
	ratios: &mut [u32],
	focused_index: usize,
	total_extent: u32,
	original_focus_size: u32,
	minimum_extents: &[u32],
) -> bool {
	loop {
		let child_sizes = split_extent(total_extent, ratios, ratios.len());
		if ratios[focused_index] <= 1
			|| child_sizes[focused_index]
				<= minimum_extents
					.get(focused_index)
					.copied()
					.unwrap_or(MIN_PANE_EXTENT)
		{
			return false;
		}

		let Some((recipient_index, _)) = ratios
			.iter()
			.enumerate()
			.filter(|(index, _)| *index != focused_index)
			.min_by_key(|(_, ratio)| **ratio)
		else {
			return false;
		};

		ratios[focused_index] -= 1;
		ratios[recipient_index] += 1;

		let new_focus_size = split_extent(total_extent, ratios, ratios.len())[focused_index];
		if new_focus_size < original_focus_size {
			return true;
		}
	}
}

fn spans_overlap(a_start: u32, a_end: u32, b_start: u32, b_end: u32) -> bool {
	a_start < b_end && b_start < a_end
}

fn pane_midpoint_col(rect: PaneRect) -> u32 {
	rect.screen_col.get().saturating_add(rect.width.get() / 2)
}

fn pane_midpoint_row(rect: PaneRect) -> u32 {
	rect.screen_row.get().saturating_add(rect.height.get() / 2)
}

fn better_focus_candidate(
	current: Option<(u32, u32, u32, PaneId)>,
	candidate: (u32, u32, u32, PaneId),
) -> Option<(u32, u32, u32, PaneId)> {
	match current {
		Some(existing) if existing <= candidate => Some(existing),
		_ => Some(candidate),
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterKind {
	CharWise,
	LineWise,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegisterValue {
	pub(crate) text: String,
	pub(crate) kind: RegisterKind,
}

pub(crate) struct ClipboardSession {
	pub(crate) registers: HashMap<char, RegisterValue>,
	pub(crate) clipboard: ClipboardHandle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileStamp {
	pub(crate) len: u64,
	pub(crate) modified: Option<SystemTime>,
}

impl FileStamp {
	pub(crate) fn from_metadata(metadata: &std::fs::Metadata) -> Self {
		Self {
			len: metadata.len(),
			modified: metadata.modified().ok(),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingReload {
	pub(crate) pane_id: PaneId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiffKind {
	Added,
	Removed,
	Modified,
	Moved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiffRole {
	Before,
	After,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntraLineDiffKind {
	Added,
	Removed,
	Modified,
}

// Sub-line token-level change produced by Layer 3 of the diff engine. `line` and
// `col_*` are coordinates on the pane that owns this DiffBlock; `partner_*`
// mirror the same change on the opposite pane so we can swap sides cheaply when
// building the partner's DiffBlock copy. Columns are already tab-expanded so
// the renderer can map them directly to PaneCol without re-scanning bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntraLineDiff {
	pub(crate) line: DocLine,
	pub(crate) col_start: u64,
	pub(crate) col_end: u64,
	pub(crate) partner_line: DocLine,
	pub(crate) partner_col_start: u64,
	pub(crate) partner_col_end: u64,
	pub(crate) kind: IntraLineDiffKind,
}

impl IntraLineDiff {
	pub(crate) fn swap_sides(self) -> Self {
		Self {
			line: self.partner_line,
			col_start: self.partner_col_start,
			col_end: self.partner_col_end,
			partner_line: self.line,
			partner_col_start: self.col_start,
			partner_col_end: self.col_end,
			kind: self.kind,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiffBlock {
	pub(crate) range: (DocLine, DocLine),
	pub(crate) kind: DiffKind,
	pub(crate) partner_range: (DocLine, DocLine),
	pub(crate) intra_line_diffs: Option<Vec<IntraLineDiff>>,
}

pub(crate) struct DiffPair {
	pub(crate) partner_pane_id: PaneId,
	/// Chronological role of the pane that owns this entry. Geometry may move;
	/// the comparison remains Before -> After.
	pub(crate) role: DiffRole,
	pub(crate) blocks: Vec<DiffBlock>,
}

/// Where a snapshot diff partner came from.
///
/// A snapshot partner is an in-memory virtual document diffed against a live
/// buffer. The diff engine itself is source-agnostic — it only ever sees two
/// document roots — so this type, not the engine, is where acquisition
/// sources accumulate. Git history is the first provider; the working index,
/// point-in-time copies of arbitrary files, or remote artifacts are all
/// future variants rather than new diff machinery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotProvenance {
	GitRevision {
		repo_root: PathBuf,
		rel_path: String,
		/// The revision as the user requested it (`HEAD~3`, a branch, a
		/// tag): the display label, not the identity.
		revision: String,
		/// Immutable identity, pinned at acquisition. A symbolic ref moving
		/// afterwards never changes what an open snapshot is: these OIDs are
		/// the foundation for caching, deduplication, and timeline movement.
		commit_oid: String,
		blob_oid: String,
	},
}

impl SnapshotProvenance {
	pub(crate) fn analysis_depth(&self) -> AnalysisDepth {
		match self {
			Self::GitRevision { .. } => AnalysisDepth::TopologyOnly,
		}
	}

	/// Tab title for a snapshot pane, e.g. `main.rs@HEAD~3`.
	pub(crate) fn title(&self) -> String {
		match self {
			Self::GitRevision {
				rel_path, revision, ..
			} => {
				let name = rel_path.rsplit('/').next().unwrap_or(rel_path);
				format!("{name}@{revision}")
			}
		}
	}
}

/// Attached text-object state captured around one high-level lifecycle
/// operation. Detached construction is intentionally absent.
pub(crate) struct TextLifecycleSnapshot {
	buffers: BTreeMap<BufferId, StateId>,
	views: Vec<(ViewId, BufferId)>,
	view_buffers: BTreeMap<ViewId, BufferId>,
	focused: Option<ViewId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostPanelGroupLayoutState {
	Automatic,
	Manual,
}

pub(crate) struct HostPanelGroupState {
	pub(crate) producer_buffer_id: Option<BufferId>,
	/// True only for a pre-start PRODUCT-0061 restoration that has not yet
	/// rebound frontend producer meaning. Ordinary producer release must not
	/// reopen that one-shot capability.
	pub(crate) reattachable_after_restore: bool,
	pub(crate) layout_state: HostPanelGroupLayoutState,
	pub(crate) current_members: Vec<HostPanelGroupMember>,
	pub(crate) retained_members: HashMap<HostPanelId, HostPanelGroupMember>,
	pub(crate) detached_members: HashSet<HostPanelId>,
	pub(crate) latest_valid_anchor: Option<PaneId>,
}

pub(crate) struct EditorSession {
	pub(crate) input: InputRuntime,
	/// The sole session owner of passphrases used by encrypted files and opaque
	/// application records. Frontends never receive this value.
	pub(crate) secrets: crate::secrets::SecretSession,
	pub(crate) events: EngineEventQueue,
	pub(crate) subscribers: SubscriberRuntime,
	pub(crate) jobs: JobRuntime,
	// Per-session authority for runtime owners and the effective command,
	// option, mapping, and event-subscriber catalogs.
	pub(crate) runtime_registries: RuntimeRegistries,
	pub(crate) viewport_id: ViewportId,
	pub(crate) hit_test_fingerprint: Option<u64>,
	/// Runtime presentation profile supplied by the active frontend. This is
	/// deliberately absent from the saved layout owner.
	pub(crate) divider_reservation: PaneDividerReservation,
	pub(crate) buffers: BufferStore,
	pub(crate) saved_documents: Option<SavedDocumentState>,
	pub(crate) application_owner: Option<super::session_checkpoint::ApplicationOwnerId>,
	pub(crate) layout: LayoutNode,
	pub(crate) host_panel_groups: HashMap<HostPanelGroupId, HostPanelGroupState>,
	pub(crate) focused_pane_id: PaneId,
	pub(crate) browser_target_pane_id: Option<PaneId>,
	pub(crate) diff_pairs: HashMap<PaneId, DiffPair>,
	pub(crate) terminal_width: ScreenCol,
	pub(crate) terminal_height: ScreenRow,
	pub(crate) clipboard: ClipboardSession,
	pub(crate) search_pattern: Option<String>,
	pub(crate) search_case_insensitive: bool,
	pub(crate) search_pattern_revision: u64,
	pub(crate) search_confirm: Option<ConfirmState>,
	pub(crate) pending_reload: Option<PendingReload>,
	pub(crate) git_branch_cache: super::git::GitBranchCache,
	pub(crate) ui: UiState,
	pub(crate) persistent_settings: SettingsPersistent,
	pub(crate) next_pane_id: PaneId,
	pub(crate) next_view_id: ViewId,
	/// Ordered frontend events queued since the last drain.
	pending_host_events: Vec<crate::engine::HostEvent>,
	pub(crate) next_semantic_request_id: RequestId,
	pub(crate) pending_navigation: Option<super::navigation::NavigationStamp>,
	pub(crate) jump_history: super::navigation::JumpHistory,
	pub(crate) picker: Option<super::fuzzy_search::PickerState>,
	/// Session-monotonic picker work identity. A closed picker may still have
	/// a queued worker response, so reopening must never reuse its generation.
	pub(crate) next_picker_generation: u64,
	pub(crate) next_dynamic_device_id: u16,
	pub(crate) output_panels: HashMap<String, BufferId>,
	pub(crate) output_panel_leaves: HashMap<BufferId, NodeId>,
	pub(crate) uast_inspectors: HashMap<BufferId, UastInspectorPanel>,
	pub(crate) reclaim_queue: super::reclaim::ReclaimQueue,
	pub(crate) git_status_job: Option<JobId>,
	pub(crate) git_status_refresh_pending: bool,
	pub(crate) git_status_branch: Option<String>,
	pub(crate) git_status_repo_root: Option<PathBuf>,
	pub(crate) git_refresh_mode: super::protocol::GitRefreshMode,
	/// The single in-flight snapshot fetch (currently `:panediff <REV>`), if
	/// any.
	pub(crate) snapshot_fetch_job: Option<JobId>,
	/// Provenance of snapshot partner buffers, e.g. the Git revision a
	/// `:panediff <REV>` pane was cut from. Drives tab titles and, later,
	/// history navigation. Entries are removed when the buffer is released.
	pub(crate) snapshot_partners: HashMap<BufferId, SnapshotProvenance>,
}

pub(crate) const DEFAULT_GIT_REFRESH_MODE: super::protocol::GitRefreshMode =
	super::protocol::GitRefreshMode::Interval(super::protocol::DEFAULT_GIT_REFRESH_SECONDS);

impl EditorSession {
	pub(crate) fn set_divider_reservation(&mut self, reservation: PaneDividerReservation) {
		if self.divider_reservation != reservation {
			self.divider_reservation = reservation;
			self.hit_test_fingerprint = None;
		}
	}

	/// Project exactly the shared identities PRODUCT-0061 compares at a coupled
	/// checkpoint. The absence of placement and relationship data is
	/// load-bearing: adding any would exceed the three-set witness's license.
	pub(crate) fn shared_identity_census(
		&self,
	) -> Option<crate::uast::projection::SharedIdentityCensus> {
		let saved = self.saved_documents.as_ref()?;
		let mut host_panels = BTreeSet::new();
		self.layout.for_each_doctab(&mut |view| {
			if let Some(panel) = self
				.buffers
				.get(view.buffer_id)
				.and_then(Buffer::host_panel)
			{
				host_panels.insert(panel.panel_id);
			}
		});
		Some(crate::uast::projection::SharedIdentityCensus {
			documents: saved.document_ids(),
			host_panels,
			host_panel_groups: self.host_panel_groups.keys().copied().collect(),
		})
	}

	pub(crate) fn attach_new_saved_session(
		&mut self,
		session: SavedSession<Writable>,
	) -> Result<(), String> {
		if self.saved_documents.is_some() {
			return Err("editor already owns a saved session".to_owned());
		}
		let mut saved = SavedDocumentState::new(session);
		for (buffer_id, buffer) in self.buffers.iter() {
			if buffer.role == BufferRole::Document
				&& !self.snapshot_partners.contains_key(&buffer_id)
				&& let Some(document) = buffer.text()
			{
				saved.mint(buffer_id, document.file_path.as_deref())?;
			}
		}
		self.saved_documents = Some(saved);
		Ok(())
	}

	#[allow(dead_code)] // first production caller lands in the Rune jobs binding
	pub(crate) fn spawn_job(
		&mut self,
		owner: RegistryOwnerId,
		spec: JobSpec,
	) -> Result<crate::core::JobId, JobSpawnError> {
		let live = self.runtime_registries.owner_is_live(owner);
		self.jobs.spawn(owner, live, spec)
	}

	pub(crate) fn remove_runtime_owner(
		&mut self,
		owner: RegistryOwnerId,
	) -> Result<RegistryRev, RegistryError> {
		let revision = self.runtime_registries.remove_owner(owner)?;
		self.jobs.cancel_owner(owner);
		Ok(revision)
	}

	pub(crate) fn replace_runtime_owner_generation(
		&mut self,
		retired_owner: RegistryOwnerId,
		batch: RegistrationBatch,
	) -> Result<RegistryRev, RegistryError> {
		let revision = self
			.runtime_registries
			.replace_owner_generation(retired_owner, batch)?;
		self.jobs.cancel_owner(retired_owner);
		Ok(revision)
	}

	pub(crate) fn contribute_subscriber_baseline(
		&self,
		builder: &mut BaselineBuilder,
	) -> Result<(), BaselineError> {
		self.layout.try_for_each_doctab(&mut |view| {
			let Some(document) = self.buffers.get(view.buffer_id).and_then(Buffer::text) else {
				return Ok(());
			};
			builder.push_buffer(view.buffer_id, document.ledger.current_state_id)?;
			builder.push_view(view.id, view.buffer_id)
		})?;
		let focused = self
			.layout
			.find_pane(self.focused_pane_id)
			.and_then(|pane| {
				let view = pane.active_tab();
				self.buffers
					.get(view.buffer_id)
					.and_then(Buffer::text)
					.map(|_| view.id)
			});
		builder.set_focused(focused);
		Ok(())
	}

	pub(crate) fn capture_text_lifecycle(&self) -> TextLifecycleSnapshot {
		let mut views = Vec::new();
		let mut view_buffers = BTreeMap::new();
		let mut buffers = BTreeMap::new();
		self.layout.for_each_doctab(&mut |view| {
			let Some(document) = self.buffers.get(view.buffer_id).and_then(Buffer::text) else {
				return;
			};
			views.push((view.id, view.buffer_id));
			view_buffers.insert(view.id, view.buffer_id);
			buffers
				.entry(view.buffer_id)
				.or_insert(document.ledger.current_state_id);
		});
		let focused = self
			.layout
			.find_pane(self.focused_pane_id)
			.and_then(|pane| {
				let view = pane.active_tab();
				self.buffers
					.get(view.buffer_id)
					.and_then(Buffer::text)
					.map(|_| view.id)
			});
		TextLifecycleSnapshot {
			buffers,
			views,
			view_buffers,
			focused,
		}
	}

	/// Publish lifecycle facts after a high-level operation has fully committed.
	/// Internal insert-then-rollback construction therefore produces no event.
	pub(crate) fn emit_text_lifecycle_since(&mut self, before: TextLifecycleSnapshot) {
		let after = self.capture_text_lifecycle();

		for (&buffer, &state) in &after.buffers {
			if !before.buffers.contains_key(&buffer) {
				self.events
					.push(EngineEvent::BufferOpened { buffer, state });
			}
		}
		for &(view, buffer) in &after.views {
			match before.view_buffers.get(&view).copied() {
				None => self.events.push(EngineEvent::ViewCreated { view, buffer }),
				Some(old_buffer) if old_buffer != buffer => {
					self.events.push(EngineEvent::ViewRetargeted {
						view,
						old_buffer,
						new_buffer: buffer,
					});
				}
				Some(_) => {}
			}
		}
		if before.focused != after.focused {
			self.events.push(EngineEvent::FocusedTextViewChanged {
				from: before.focused,
				to: after.focused,
			});
		}
		for &(view, buffer) in &before.views {
			if !after.view_buffers.contains_key(&view) {
				self.events.push(EngineEvent::ViewClosed { view, buffer });
			}
		}
		for &buffer in before.buffers.keys() {
			if !after.buffers.contains_key(&buffer) {
				self.events.push(EngineEvent::BufferClosed { buffer });
			}
		}
	}

	#[cfg(test)]
	pub(crate) fn assert_unique_text_node_ownership(&self, registry: &UastRegistry) {
		let live = self.capture_text_lifecycle();
		let mut owner_by_node = HashMap::<NodeId, BufferId>::new();
		for &buffer in live.buffers.keys() {
			let root = self
				.buffers
				.get(buffer)
				.and_then(Buffer::text)
				.expect("captured text buffer remains live")
				.root_id;
			let mut stack = vec![root];
			while let Some(node) = stack.pop() {
				if let Some(previous) = owner_by_node.insert(node, buffer) {
					assert_eq!(
						previous, buffer,
						"live NodeId {node:?} is reachable from two BufferIds"
					);
					continue;
				}
				let mut child = registry.get_first_child(node);
				while let Some(current) = child {
					stack.push(current);
					child = registry.get_next_sibling(current);
				}
			}
		}
	}

	/// Mutate buffer truth through one acting text view, then fan the resulting
	/// content/topology change out to every other attached view.
	pub(crate) fn mutate_text_buffer<R>(
		registry: &UastRegistry,
		buffers: &mut BufferStore,
		layout: &mut LayoutNode,
		events: &mut EngineEventQueue,
		acting_view_id: ViewId,
		mutate: impl FnOnce(&mut TextEditorMut<'_>) -> R,
	) -> Option<R> {
		let buffer_id = layout.find_view(acting_view_id)?.buffer_id;
		let document = buffers.get(buffer_id)?.text()?;
		let root_before = document.root_id;
		let accounting_before = document.uast_accounting;
		let allocated_before = registry.allocation_event_count();
		let retired_before = registry.retired_node_count();
		let payload_before = registry.authoritative_payload_snapshot();
		let state_before = document.ledger.current_state_id;
		let topology_before = document.topology_rev;
		let search_before = document.search_revision;
		let semantic_before = document.semantic.view_revision;

		let mut view_ids = Vec::new();
		layout.collect_view_ids(&mut view_ids);
		let snapshots = view_ids
			.into_iter()
			.filter(|view_id| *view_id != acting_view_id)
			.filter_map(|view_id| {
				let view = layout.find_view(view_id)?;
				if view.buffer_id != buffer_id {
					return None;
				}
				let ViewContent::Text(text_view) = &view.content else {
					panic!("view content must match its text buffer");
				};
				Some(SharedTextViewSnapshot {
					view_id,
					folds: text_view.state.folds.snapshot(registry, root_before),
				})
			})
			.collect::<Vec<_>>();

		let result = {
			let buffer = buffers.get_mut(buffer_id)?;
			let view = layout.find_view_mut(acting_view_id)?;
			let mut editor = detached_text_editor_mut(buffer, view)?;
			mutate(&mut editor)
		};

		let document = buffers
			.get_mut(buffer_id)
			.and_then(Buffer::text_mut)
			.expect("mutated text buffer must remain live");
		let root_after = document.root_id;
		let allocated_after = registry.allocation_event_count();
		let closure_retired_after = registry.retired_node_count();
		let payload_after = registry.authoritative_payload_snapshot();
		let allocated_delta = allocated_after.saturating_sub(allocated_before);
		let closure_retired_delta = closure_retired_after.saturating_sub(retired_before);
		let payload_added_delta = payload_after
			.added
			.checked_sub(payload_before.added)
			.expect("authoritative payload-added counter regressed");
		let payload_physically_removed_delta = payload_after
			.removed
			.checked_sub(payload_before.removed)
			.expect("authoritative payload-removed counter regressed");
		let orphaned_payload_delta = payload_after
			.orphaned_added
			.checked_sub(payload_before.orphaned_added)
			.expect("orphaned payload counter regressed");
		let parked_retired_delta = std::mem::take(&mut document.pending_parked_retirements);
		let parked_payload_retired_delta =
			std::mem::take(&mut document.pending_parked_payload_retirements);
		assert!(
			parked_retired_delta <= closure_retired_delta,
			"typed parked retirement exceeds registry retirement traffic"
		);
		let ordinary_retired_delta = closure_retired_delta - parked_retired_delta;
		assert!(
			parked_payload_retired_delta <= orphaned_payload_delta,
			"typed parked payload retirement exceeds orphaned payload traffic"
		);
		let ordinary_orphaned_payload = orphaned_payload_delta - parked_payload_retired_delta;
		document
			.uast_accounting
			.record_parked_retirement(parked_retired_delta, parked_payload_retired_delta);
		if root_before != root_after {
			let transition = document
				.pending_root_transition
				.take()
				.unwrap_or(RootTransition::Retire);
			if transition == RootTransition::Retire {
				registry.retire_orphaned_forest(
					[root_before],
					accounting_before.current_live_nodes,
					accounting_before.current_live_payload_bytes,
					crate::ecs::registry::ReclaimFence::AfterPublication,
				);
			}
			document.uast_accounting.record_root_transition(
				allocated_delta,
				ordinary_retired_delta,
				payload_added_delta,
				payload_physically_removed_delta,
				ordinary_orphaned_payload,
				transition,
			);
		} else {
			assert!(
				document.pending_root_transition.take().is_none(),
				"root transition recorded without replacing the root"
			);
			document.uast_accounting.record_transaction(
				allocated_delta,
				ordinary_retired_delta,
				payload_added_delta,
				payload_physically_removed_delta,
				ordinary_orphaned_payload,
			);
		}
		let state_after = document.ledger.current_state_id;
		let topology_after = document.topology_rev;
		let search_invalidated_by_content = (root_before != root_after
			|| state_before != state_after)
			&& document.search_revision == search_before;
		if search_invalidated_by_content {
			document.search_matches.clear();
			document.bump_search_revision();
		}
		let search_changed = document.search_revision != search_before;
		let semantic_changed = document.semantic.view_revision != semantic_before;
		let transition_deltas = (root_before == root_after && state_before != state_after)
			.then(|| {
				document
					.ledger
					.deltas_for_transition(state_before, state_after)
			})
			.flatten();
		let edits = transition_deltas
			.as_ref()
			.map(|deltas| super::document::edit::fold_edits_for_deltas(deltas));
		let root_replaced =
			root_before != root_after || (state_before != state_after && edits.is_none());
		let topology_changed = topology_before != topology_after;
		if root_replaced {
			document.navigation_anchors.invalidate_all();
		} else if let Some(edits) = &edits {
			document
				.navigation_anchors
				.rebase_after_edits(root_after, state_after, edits);
		}
		let total_bytes = registry.get_total_bytes(root_after);
		let total_lines = registry.get_total_newlines(root_after);
		for snapshot in snapshots {
			let view = layout
				.find_view_mut(snapshot.view_id)
				.expect("snapshotted shared view must remain attached during mutation");
			let ViewContent::Text(text_view) = &mut view.content else {
				panic!("view content must match its text buffer");
			};
			if root_replaced {
				text_view.cursor.clamp_anchors(total_bytes);
				text_view.state.folds.clear();
			} else if let Some(edits) = &edits {
				text_view.cursor.rebase_after_edits(edits);
				text_view.cursor.clamp_anchors(total_bytes);
				if let Some(points) = text_view.cursor.points_mut() {
					points.normalize(registry, root_after);
				}
				text_view.state.folds.restore_after_edits(
					registry,
					root_after,
					snapshot.folds,
					edits,
				);
			} else if topology_changed {
				text_view
					.state
					.folds
					.restore_exact(registry, root_after, snapshot.folds);
			}

			if root_replaced || edits.is_some() || search_changed {
				text_view.match_index = None;
				text_view.match_info = None;
			}
			if text_view.state.common.scroll_y > total_lines {
				text_view.state.common.scroll_y = total_lines;
				text_view.state.common.scroll_row_offset = DisplayRow::ZERO;
			}
			if root_replaced
				|| edits.is_some()
				|| topology_changed
				|| search_changed
				|| semantic_changed
			{
				text_view.state.common.reset_minimap_cache();
			}
		}

		let acting = layout
			.find_view_mut(acting_view_id)
			.expect("acting view must remain attached during mutation");
		let ViewContent::Text(acting) = &mut acting.content else {
			panic!("view content must match its text buffer");
		};
		if search_invalidated_by_content {
			acting.match_index = None;
			acting.match_info = None;
		}
		if acting.state.common.scroll_y > total_lines {
			acting.state.common.scroll_y = total_lines;
			acting.state.common.scroll_row_offset = DisplayRow::ZERO;
		}
		if root_replaced
			|| edits.is_some()
			|| topology_changed
			|| search_changed
			|| semantic_changed
		{
			acting.state.common.reset_minimap_cache();
		}

		if root_replaced {
			events.push(EngineEvent::BufferReplaced {
				buffer: buffer_id,
				old_state: state_before,
				new_state: state_after,
			});
		} else if state_before != state_after {
			let deltas = transition_deltas
				.as_ref()
				.expect("ordinary state transition must retain its ledger deltas");
			let (deleted_bytes, inserted_bytes) =
				deltas
					.iter()
					.fold((0_u64, 0_u64), |(deleted, inserted), delta| {
						(
							deleted.saturating_add(delta.deleted_bytes.len() as u64),
							inserted.saturating_add(delta.inserted_bytes.len() as u64),
						)
					});
			events.push(EngineEvent::BufferEdited {
				buffer: buffer_id,
				acting_view: acting_view_id,
				old_state: state_before,
				new_state: state_after,
				delta_count: deltas.len() as u64,
				deleted_bytes,
				inserted_bytes,
			});
		}

		Some(result)
	}

	/// Open the mutation boundary for a synthetic buffer/view pair. Synthetic
	/// buffers may share their buffer state; text buffers use `mutate_text_buffer`
	/// to fan out document changes to their attached views.
	pub(crate) fn mutate_buffer<R>(
		buffers: &mut BufferStore,
		layout: &mut LayoutNode,
		acting_view_id: ViewId,
		mutate: impl FnOnce(BufferMutationTarget<'_>) -> R,
	) -> Option<R> {
		let buffer_id = layout.find_view(acting_view_id)?.buffer_id;
		let buffer = buffers.get_mut(buffer_id)?;
		let view = layout.find_view_mut(acting_view_id)?;
		assert_eq!(
			buffer.id, view.buffer_id,
			"view must reference the buffer selected by its BufferId"
		);
		let target = match (&mut buffer.content, &mut view.content) {
			(BufferContent::Text(_), ViewContent::Text(_)) => return None,
			(BufferContent::FileBrowser(document), ViewContent::FileBrowser(browser_view)) => {
				BufferMutationTarget::FileBrowser(FileBrowserEditorMut {
					document,
					cursor: &mut browser_view.cursor,
					view: &mut browser_view.state,
				})
			}
			(
				BufferContent::MarkdownPreview(document),
				ViewContent::MarkdownPreview(preview_view),
			) => BufferMutationTarget::MarkdownPreview(MarkdownPreviewEditorMut {
				document,
				view: &mut preview_view.state,
			}),
			// Host panels have no engine-owned mutable document state. Callers
			// probing synthetic-buffer capabilities must treat them as inert,
			// rather than assuming every non-text buffer is browser/preview data.
			(BufferContent::HostPanel(_), ViewContent::HostPanel(_)) => return None,
			_ => panic!("view content must match its buffer content"),
		};
		Some(mutate(target))
	}

	pub(crate) fn new(
		registry: &UastRegistry,
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
		startup_status: Option<String>,
	) -> Self {
		let persistent_settings = SettingsPersistent {
			theme_name: initial_theme_name,
			minimap_enabled: initial_minimap_enabled,
			wrap_enabled: initial_wrap_enabled,
			line_numbers_enabled: initial_line_numbers_enabled,
			relative_line_numbers_enabled: initial_relative_line_numbers_enabled,
			visible_whitespace: initial_visible_whitespace,
			reload_enabled: initial_reload_enabled,
			save_durability: initial_save_durability,
			ui_colors: initial_ui_colors,
			language_themes: initial_language_themes,
		};
		let focused_pane_id = PaneId::new(1);
		let initial_view_id = ViewId::new(1);
		let mut buffers = BufferStore::new();
		let (initial_buffer, initial_leaf) = TextBuffer::new_unnamed(registry)
			.expect("failed to create initial unnamed text buffer");
		let initial_buffer_id = buffers.insert(BufferContent::Text(initial_buffer));
		let layout = LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id: focused_pane_id,
			pane: EditorPane::new_empty(
				initial_view_id,
				initial_buffer_id,
				initial_leaf,
				&persistent_settings,
			),
		});
		Self::from_workspace(
			persistent_settings,
			buffers,
			None,
			layout,
			initial_view_id.next(),
			startup_status,
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new_restored(
		registry: &UastRegistry,
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
		saved_session: SavedSession<Writable>,
		mut documents: Vec<RestoredDocument>,
		startup_status: Option<String>,
	) -> Result<Self, String> {
		if documents.is_empty() {
			let mut session = Self::new(
				registry,
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
				startup_status,
			);
			session.attach_new_saved_session(saved_session)?;
			return Ok(session);
		}

		documents.sort_by_key(|document| document.document_id);
		let persistent_settings = SettingsPersistent {
			theme_name: initial_theme_name,
			minimap_enabled: initial_minimap_enabled,
			wrap_enabled: initial_wrap_enabled,
			line_numbers_enabled: initial_line_numbers_enabled,
			relative_line_numbers_enabled: initial_relative_line_numbers_enabled,
			visible_whitespace: initial_visible_whitespace,
			reload_enabled: initial_reload_enabled,
			save_durability: initial_save_durability,
			ui_colors: initial_ui_colors,
			language_themes: initial_language_themes,
		};
		let pane_id = PaneId::new(1);
		let mut next_view_id = ViewId::new(1);
		let mut buffers = BufferStore::new();
		let mut saved_documents = SavedDocumentState::new(saved_session);
		let mut tabs = Vec::with_capacity(documents.len());

		for restored in documents {
			let (mut document, initial_leaf) =
				TextBuffer::new_from_bytes(registry, restored.bytes)?;
			document.file_path = restored.file_path;
			document.virtual_path = restored.virtual_path;
			document.file_stamp = restored.file_stamp;
			document.language_selection = restored.language_selection;
			document.active_language_id = restored.active_language_id;
			if restored.dirty {
				document.ledger.restore_dirty_baseline();
			} else {
				document.ledger.clear();
			}
			let canonical_path = document
				.file_path
				.as_deref()
				.map(CanonicalPath::resolve)
				.transpose()?;
			let buffer_id = buffers.insert(BufferContent::Text(document));
			if let Some(path) = canonical_path {
				let reservation = buffers.reserve_path(buffer_id, path)?;
				buffers.commit_path(reservation);
			}
			saved_documents.bind_restored(restored.document_id, buffer_id)?;
			tabs.push(View::new_text(
				next_view_id,
				buffer_id,
				initial_leaf,
				&persistent_settings,
				DEFAULT_VIEWPORT_WIDTH,
				DEFAULT_VIEWPORT_LINES,
			));
			next_view_id = next_view_id.next();
		}

		let layout = LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id,
			pane: EditorPane {
				tabs,
				active_tab_index: 0,
			},
		});
		Ok(Self::from_workspace(
			persistent_settings,
			buffers,
			Some(saved_documents),
			layout,
			next_view_id,
			startup_status,
		))
	}

	fn from_workspace(
		persistent_settings: SettingsPersistent,
		buffers: BufferStore,
		saved_documents: Option<SavedDocumentState>,
		layout: LayoutNode,
		next_view_id: ViewId,
		startup_status: Option<String>,
	) -> Self {
		let focused_pane_id = PaneId::new(1);

		Self {
			input: InputRuntime::new(),
			secrets: crate::secrets::SecretSession::default(),
			events: EngineEventQueue::default(),
			subscribers: SubscriberRuntime::default(),
			jobs: JobRuntime::new(),
			runtime_registries: RuntimeRegistries::new(),
			viewport_id: ViewportId::new(1),
			hit_test_fingerprint: None,
			divider_reservation: PaneDividerReservation::default(),
			buffers,
			saved_documents,
			application_owner: None,
			layout,
			host_panel_groups: HashMap::new(),
			focused_pane_id,
			browser_target_pane_id: Some(focused_pane_id),
			diff_pairs: HashMap::new(),
			terminal_width: ScreenCol::new(DEFAULT_VIEWPORT_WIDTH as u32),
			terminal_height: ScreenRow::new(
				DEFAULT_VIEWPORT_LINES.saturating_add(STATUS_BAR_ROWS as u32),
			),
			clipboard: ClipboardSession {
				registers: HashMap::new(),
				clipboard: ClipboardHandle::new(),
			},
			search_pattern: None,
			search_case_insensitive: false,
			search_pattern_revision: 0,
			search_confirm: None,
			pending_reload: None,
			git_branch_cache: super::git::GitBranchCache::default(),
			ui: UiState {
				status_message: startup_status,
				capacity_warning_shown: false,
				document_text_response: None,
				document_text_response_buffer_id: None,
				document_text_response_state_id: None,
				busy: false,
				pending_quit: false,
				mode_override: None,
				yank_flash: None,
			},
			persistent_settings,
			next_pane_id: PaneId::new(2),
			next_view_id,
			pending_host_events: Vec::new(),
			next_semantic_request_id: RequestId::new(1),
			pending_navigation: None,
			jump_history: super::navigation::JumpHistory::default(),
			picker: None,
			next_picker_generation: 1,
			next_dynamic_device_id: super::core::FILE_DEVICE_ID + 1,
			output_panels: HashMap::new(),
			output_panel_leaves: HashMap::new(),
			uast_inspectors: HashMap::new(),
			reclaim_queue: super::reclaim::ReclaimQueue::default(),
			git_status_job: None,
			git_status_refresh_pending: false,
			git_status_branch: None,
			git_status_repo_root: None,
			git_refresh_mode: DEFAULT_GIT_REFRESH_MODE,
			snapshot_fetch_job: None,
			snapshot_partners: HashMap::new(),
		}
	}

	/// Reserve a session-unique view identity. IDs are monotonic and never
	/// returned to the allocator when a view closes.
	pub(crate) fn allocate_view_id(&mut self) -> ViewId {
		let id = self.next_view_id;
		self.next_view_id = self.next_view_id.next();
		id
	}

	/// Create the buffer and view for a host-rendered panel. The engine holds
	/// only presentation metadata; the frontend owns whatever is drawn inside.
	pub(crate) fn create_host_panel_view(
		&mut self,
		panel_id: HostPanelId,
		descriptor: HostPanelDescriptor,
		viewport_width: u16,
		viewport_lines: u32,
	) -> View {
		let buffer_id = self
			.buffers
			.insert(BufferContent::HostPanel(HostPanelBuffer {
				panel_id,
				descriptor,
				revision: PanelRevision::new(0),
			}));
		let view_id = self.allocate_view_id();
		View::new_host_panel(
			view_id,
			buffer_id,
			&self.persistent_settings,
			viewport_width,
			viewport_lines,
		)
	}

	fn create_host_panel_view_for_buffer(
		&mut self,
		buffer_id: BufferId,
		viewport_width: u16,
		viewport_lines: u32,
	) -> View {
		let view_id = self.allocate_view_id();
		View::new_host_panel(
			view_id,
			buffer_id,
			&self.persistent_settings,
			viewport_width,
			viewport_lines,
		)
	}

	fn panel_id_in_pane(&self, pane: &EditorPane) -> Option<HostPanelId> {
		pane.tabs.iter().find_map(|view| {
			self.buffers
				.get(view.buffer_id)
				.and_then(|buffer| buffer.host_panel())
				.map(|panel| panel.panel_id)
		})
	}

	fn pane_id_for_panel(&self, panel_id: HostPanelId) -> Option<PaneId> {
		let buffer_id = self.host_panel_buffer_id(panel_id)?;
		self.layout
			.view_id_for_buffer(buffer_id)
			.and_then(|view_id| self.layout.pane_id_for_view(view_id))
	}

	fn group_owning_panel(&self, panel_id: HostPanelId) -> Option<HostPanelGroupId> {
		self.host_panel_groups.iter().find_map(|(group_id, group)| {
			(group
				.current_members
				.iter()
				.any(|member| member.panel_id == panel_id)
				|| group.retained_members.contains_key(&panel_id)
				|| group.detached_members.contains(&panel_id))
			.then_some(*group_id)
		})
	}

	fn clear_empty_group_root(&mut self, group_id: HostPanelGroupId) {
		let Some(group) = self.host_panel_groups.get(&group_id) else {
			return;
		};
		let has_attached_member = group
			.current_members
			.iter()
			.map(|member| member.panel_id)
			.chain(group.retained_members.keys().copied())
			.filter(|panel_id| !group.detached_members.contains(panel_id))
			.any(|panel_id| {
				self.pane_id_for_panel(panel_id).is_some_and(|pane_id| {
					self.layout.group_containing_pane(pane_id) == Some(group_id)
				})
			});
		if !has_attached_member && let Some(root) = self.layout.find_group_root_mut(group_id) {
			root.set_group_root(None);
		}
	}

	fn group_dimensions(&self, group: &HostPanelGroupState) -> (u16, u32) {
		group
			.latest_valid_anchor
			.and_then(|pane_id| self.layout.find_pane(pane_id))
			.or_else(|| {
				group
					.current_members
					.iter()
					.find_map(|member| self.pane_id_for_panel(member.panel_id))
					.and_then(|pane_id| self.layout.find_pane(pane_id))
			})
			.map(|pane| {
				let common = pane.active_tab().common();
				(common.viewport_width, common.viewport_lines)
			})
			.unwrap_or((DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES))
	}

	fn make_group_member_pane(
		&mut self,
		member: &HostPanelGroupMember,
		dimensions: (u16, u32),
	) -> (PaneId, EditorPane, HostPanelGroupShape) {
		let view = if let Some(buffer_id) = self.host_panel_buffer_id(member.panel_id) {
			self.create_host_panel_view_for_buffer(buffer_id, dimensions.0, dimensions.1)
		} else {
			self.create_host_panel_view(
				member.panel_id,
				HostPanelDescriptor {
					kind: member.kind.clone(),
					title: member.title.clone(),
				},
				dimensions.0,
				dimensions.1,
			)
		};
		let pane_id = self.next_pane_id;
		self.next_pane_id = self.next_pane_id.next();
		(pane_id, EditorPane::from_view(view), member.shape)
	}

	/// Buffer backing a panel identity. Disposal is refcounted like buffer
	/// release: the frontend may only drop backing state once no workspace
	/// view references this buffer.
	pub(crate) fn host_panel_buffer_id(&self, panel_id: HostPanelId) -> Option<BufferId> {
		self.buffers.host_panel_buffer_id(panel_id)
	}

	fn refresh_host_panel_descriptor(&mut self, member: &HostPanelGroupMember) -> bool {
		let Some(buffer_id) = self.host_panel_buffer_id(member.panel_id) else {
			return false;
		};
		let Some(panel) = self
			.buffers
			.get_mut(buffer_id)
			.and_then(Buffer::host_panel_mut)
		else {
			return false;
		};
		panel.descriptor.kind.clone_from(&member.kind);
		panel.descriptor.title.clone_from(&member.title);
		true
	}

	/// Establish one complete host-panel group without projecting intermediate
	/// split geometry. Returns backing buffers that could not be placed and must
	/// be released by the runtime.
	pub(crate) fn ensure_host_panel_group(
		&mut self,
		group_id: HostPanelGroupId,
		anchor: Option<PaneId>,
		members: Vec<HostPanelGroupMember>,
		layout: HostPanelGroupLayout,
	) -> Vec<BufferId> {
		if members.is_empty() && !self.host_panel_groups.contains_key(&group_id) {
			return Vec::new();
		}

		if let Some(mut group) = self.host_panel_groups.remove(&group_id) {
			if group.producer_buffer_id.is_none() {
				if !group.reattachable_after_restore {
					self.host_panel_groups.insert(group_id, group);
					return Vec::new();
				}
				let valid_anchor =
					anchor.filter(|pane_id| self.layout.find_pane(*pane_id).is_some());
				let producer = valid_anchor.and_then(|pane_id| {
					self.layout
						.find_pane(pane_id)
						.map(|pane| pane.active_tab().buffer_id)
				});
				let Some(producer) = producer else {
					self.host_panel_groups.insert(group_id, group);
					return Vec::new();
				};
				group.producer_buffer_id = Some(producer);
				group.reattachable_after_restore = false;
				group.latest_valid_anchor = valid_anchor;
				for member in &members {
					self.refresh_host_panel_descriptor(member);
				}
				// PRODUCT-0061 restores manual placement as settled workspace
				// truth. Reattaching producer meaning must not reinterpret that
				// placement as a request to tile it again.
				if group.layout_state == HostPanelGroupLayoutState::Manual {
					self.host_panel_groups.insert(group_id, group);
					return Vec::new();
				}
			} else if let Some(anchor) = anchor
				&& self.layout.find_pane(anchor).is_some_and(|pane| {
					Some(pane.active_tab().buffer_id) != group.producer_buffer_id
				}) {
				self.host_panel_groups.insert(group_id, group);
				return Vec::new();
			}
			let mut unique = HashSet::new();
			let conflicts = members.iter().any(|member| !unique.insert(member.panel_id))
				|| self.host_panel_groups.values().any(|other| {
					other.current_members.iter().any(|existing| {
						members
							.iter()
							.any(|member| member.panel_id == existing.panel_id)
					})
				});
			if conflicts {
				self.host_panel_groups.insert(group_id, group);
				return Vec::new();
			}
			for member in &members {
				self.refresh_host_panel_descriptor(member);
			}
			if anchor.is_some_and(|pane_id| self.layout.find_pane(pane_id).is_some()) {
				group.latest_valid_anchor = anchor;
			}

			let requested = members
				.iter()
				.map(|member| member.panel_id)
				.collect::<HashSet<_>>();
			for old in &group.current_members {
				if !requested.contains(&old.panel_id)
					&& !group.detached_members.contains(&old.panel_id)
					&& self.pane_id_for_panel(old.panel_id).is_some()
				{
					group.retained_members.insert(old.panel_id, old.clone());
				}
			}
			for member in &members {
				group.retained_members.remove(&member.panel_id);
			}

			let dimensions = self.group_dimensions(&group);
			let mut missing = Vec::new();
			for member in &members {
				if self.pane_id_for_panel(member.panel_id).is_none() {
					missing.push((
						member.clone(),
						self.make_group_member_pane(member, dimensions),
					));
				}
			}
			group.current_members = members;

			if missing.is_empty() {
				self.host_panel_groups.insert(group_id, group);
				return Vec::new();
			}

			let has_root = self.layout.contains_group_root(group_id);
			if has_root && group.layout_state == HostPanelGroupLayoutState::Automatic {
				let current_order = group
					.current_members
					.iter()
					.filter(|member| !group.detached_members.contains(&member.panel_id))
					.map(|member| (member.panel_id, member.shape))
					.collect::<Vec<_>>();
				let retained_shapes = group
					.retained_members
					.iter()
					.map(|(panel_id, member)| (*panel_id, member.shape))
					.collect::<HashMap<_, _>>();
				let panel_by_buffer = group
					.current_members
					.iter()
					.chain(group.retained_members.values())
					.filter_map(|member| {
						self.host_panel_buffer_id(member.panel_id)
							.map(|buffer_id| (buffer_id, member.panel_id))
					})
					.collect::<HashMap<_, _>>();
				let mut created = missing
					.into_iter()
					.map(|(member, pane)| (member.panel_id, pane))
					.collect::<HashMap<_, _>>();
				let mut rebuild = Some(move |old: LayoutNode| {
					let mut old_panes = Vec::new();
					old.into_leaf_panes(&mut old_panes);
					let mut existing = HashMap::new();
					let mut retained_in_layout_order = Vec::new();
					for (pane_id, pane) in old_panes {
						let panel_id = pane
							.tabs
							.iter()
							.find_map(|view| panel_by_buffer.get(&view.buffer_id).copied());
						if let Some(panel_id) = panel_id {
							existing.insert(panel_id, (pane_id, pane));
							retained_in_layout_order.push(panel_id);
						}
					}
					let mut tiled = Vec::new();
					let mut used = HashSet::new();
					for (panel_id, shape) in &current_order {
						if let Some((pane_id, pane)) = existing.remove(panel_id).or_else(|| {
							created
								.remove(panel_id)
								.map(|(pane_id, pane, _)| (pane_id, pane))
						}) {
							tiled.push((pane_id, pane, *shape));
							used.insert(*panel_id);
						}
					}
					for panel_id in retained_in_layout_order {
						if used.contains(&panel_id) {
							continue;
						}
						if let Some((pane_id, pane)) = existing.remove(&panel_id) {
							tiled.push((
								pane_id,
								pane,
								retained_shapes
									.get(&panel_id)
									.copied()
									.unwrap_or(HostPanelGroupShape::Compact),
							));
						}
					}
					build_host_panel_group_tile(group_id, tiled, HostPanelGroupLayout::RowMajorTile)
				});
				let rebuilt = self.layout.rebuild_group_root(group_id, &mut rebuild);
				debug_assert!(rebuilt);
			} else if has_root {
				let branch = build_host_panel_group_tile(
					group_id,
					missing
						.into_iter()
						.map(|(_, (pane_id, pane, shape))| (pane_id, pane, shape))
						.collect(),
					HostPanelGroupLayout::RowMajorTile,
				);
				let mut append = Some(move |mut old: LayoutNode| {
					old.set_group_root(None);
					let mut branch = branch;
					branch.set_group_root(None);
					LayoutNode::Split {
						group_root: Some(group_id),
						direction: SplitDirection::Horizontal,
						children: vec![old, branch],
						ratios: vec![DEFAULT_SPLIT_RATIO, DEFAULT_SPLIT_RATIO],
					}
				});
				let appended = self.layout.rebuild_group_root(group_id, &mut append);
				debug_assert!(appended);
			} else {
				let target = group.latest_valid_anchor.filter(|pane_id| {
					group.detached_members.is_empty()
						&& self.layout.can_host_group_subtree(*pane_id)
				});
				if let Some(target) = target {
					let tile = build_host_panel_group_tile(
						group_id,
						missing
							.into_iter()
							.map(|(_, (pane_id, pane, shape))| (pane_id, pane, shape))
							.collect(),
						HostPanelGroupLayout::RowMajorTile,
					);
					if self.layout.split_leaf_with_subtree(
						target,
						SplitDirection::Vertical,
						tile,
						false,
					) {
						self.focus_pane(target);
					}
				} else {
					let target = group
						.latest_valid_anchor
						.filter(|pane_id| self.layout.find_pane(*pane_id).is_some())
						.unwrap_or(self.focused_pane_id);
					for (member, (_, pane, _)) in missing.into_iter().rev() {
						if self
							.split_pane_with_new_pane_order(
								target,
								SplitDirection::Horizontal,
								pane,
								false,
							)
							.is_some()
						{
							group.detached_members.insert(member.panel_id);
						}
					}
					self.focus_pane(target);
				}
			}
			self.host_panel_groups.insert(group_id, group);
			debug_assert!(self.layout.count_group_roots(group_id) <= 1);
			return Vec::new();
		}

		let mut unique = HashSet::new();
		let conflicts = members.iter().any(|member| !unique.insert(member.panel_id))
			|| self.host_panel_groups.values().any(|group| {
				group.current_members.iter().any(|existing| {
					members
						.iter()
						.any(|member| member.panel_id == existing.panel_id)
				})
			});
		if !conflicts {
			for member in &members {
				self.refresh_host_panel_descriptor(member);
			}
		}

		let valid_anchor = anchor.filter(|pane_id| self.layout.find_pane(*pane_id).is_some());
		let producer_buffer_id = valid_anchor.and_then(|pane_id| {
			self.layout
				.find_pane(pane_id)
				.map(|pane| pane.active_tab().buffer_id)
		});
		let dimensions = valid_anchor
			.and_then(|pane_id| self.layout.find_pane(pane_id))
			.map(|pane| {
				let common = pane.active_tab().common();
				(common.viewport_width, common.viewport_lines)
			})
			.unwrap_or((DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES));

		let existing = members.iter().any(|member| {
			self.host_panel_buffer_id(member.panel_id)
				.and_then(|buffer_id| self.layout.view_id_for_buffer(buffer_id))
				.is_some()
		});
		let mut created = Vec::new();
		for member in &members {
			if self.host_panel_buffer_id(member.panel_id).is_some() {
				continue;
			}
			let view = self.create_host_panel_view(
				member.panel_id,
				HostPanelDescriptor {
					kind: member.kind.clone(),
					title: member.title.clone(),
				},
				dimensions.0,
				dimensions.1,
			);
			let pane_id = self.next_pane_id;
			self.next_pane_id = self.next_pane_id.next();
			created.push((
				member.panel_id,
				view.buffer_id,
				pane_id,
				EditorPane::from_view(view),
				member.shape,
			));
		}

		let minimum = host_panel_tile_minimum(created.len(), self.divider_reservation);
		let fits = valid_anchor
			.and_then(|pane_id| {
				self.pane_layouts()
					.into_iter()
					.find(|pane| pane.pane_id == pane_id)
			})
			.is_some_and(|anchor_layout| {
				let width = anchor_layout
					.rect
					.width
					.get()
					.saturating_sub(self.divider_reservation.columns)
					/ 2;
				let height = anchor_layout.rect.height.get();
				minimum.0 <= width && minimum.1 <= height
			});
		let candidate_allowed = !conflicts
			&& !existing
			&& valid_anchor.is_some_and(|pane_id| self.layout.can_host_group_subtree(pane_id))
			&& fits;

		let materialized = if candidate_allowed {
			let anchor = valid_anchor.expect("fit candidate has an anchor");
			let candidate = build_host_panel_group_tile(
				group_id,
				created
					.drain(..)
					.map(|(_, _, pane_id, pane, shape)| (pane_id, pane, shape))
					.collect(),
				layout,
			);
			let inserted = self.layout.split_leaf_with_subtree(
				anchor,
				SplitDirection::Vertical,
				candidate,
				false,
			);
			if inserted {
				self.focus_pane(anchor);
			}
			inserted
		} else {
			false
		};

		let mut unplaced = Vec::new();
		let mut detached_members = HashSet::new();
		if !materialized {
			let target = valid_anchor.unwrap_or(self.focused_pane_id);
			for (panel_id, buffer_id, _, pane, _) in created.into_iter().rev() {
				if self
					.split_pane_with_new_pane_order(target, SplitDirection::Horizontal, pane, false)
					.is_none()
				{
					unplaced.push(buffer_id);
				} else {
					detached_members.insert(panel_id);
				}
			}
			self.focus_pane(target);
			for member in &members {
				if self.host_panel_buffer_id(member.panel_id).is_some() {
					detached_members.insert(member.panel_id);
				}
			}
		}

		self.host_panel_groups.insert(
			group_id,
			HostPanelGroupState {
				producer_buffer_id,
				reattachable_after_restore: false,
				layout_state: HostPanelGroupLayoutState::Automatic,
				current_members: members,
				retained_members: HashMap::new(),
				detached_members,
				latest_valid_anchor: valid_anchor,
			},
		);
		debug_assert!(self.layout.count_group_roots(group_id) <= 1);
		unplaced
	}

	/// Reconcile one producer-owned group to its latest successful membership.
	/// Returns orphan backing buffers whose removed workspace views may now be
	/// released through the ordinary host-panel lifecycle.
	pub(crate) fn reset_host_panel_group(&mut self, group_id: HostPanelGroupId) -> Vec<BufferId> {
		let Some(mut group) = self.host_panel_groups.remove(&group_id) else {
			return Vec::new();
		};
		if group.producer_buffer_id.is_none() {
			self.host_panel_groups.insert(group_id, group);
			return Vec::new();
		}

		let current_ids = group
			.current_members
			.iter()
			.map(|member| member.panel_id)
			.collect::<HashSet<_>>();
		let has_root = self.layout.contains_group_root(group_id);
		let current_panes = group
			.current_members
			.iter()
			.filter_map(|member| self.pane_id_for_panel(member.panel_id))
			.collect::<HashSet<_>>();
		let attached_current = group.current_members.iter().all(|member| {
			!group.detached_members.contains(&member.panel_id)
				&& self
					.pane_id_for_panel(member.panel_id)
					.is_some_and(|pane_id| {
						self.layout.group_containing_pane(pane_id) == Some(group_id)
					})
		});
		let no_foreign = self
			.layout
			.find_group_root(group_id)
			.is_some_and(|root| root.pane_count() == current_panes.len());
		if group.layout_state == HostPanelGroupLayoutState::Automatic
			&& group.retained_members.is_empty()
			&& group.detached_members.is_empty()
			&& attached_current
			&& no_foreign
		{
			self.host_panel_groups.insert(group_id, group);
			return Vec::new();
		}

		if !has_root && !group.current_members.is_empty() {
			let Some(anchor) = group
				.latest_valid_anchor
				.filter(|pane_id| self.layout.can_host_group_subtree(*pane_id))
			else {
				self.host_panel_groups.insert(group_id, group);
				return Vec::new();
			};
			let minimum =
				host_panel_tile_minimum(group.current_members.len(), self.divider_reservation);
			let fits = self
				.pane_layouts()
				.into_iter()
				.find(|layout| layout.pane_id == anchor)
				.is_some_and(|anchor_layout| {
					let width = anchor_layout
						.rect
						.width
						.get()
						.saturating_sub(self.divider_reservation.columns)
						/ 2;
					minimum.0 <= width && minimum.1 <= anchor_layout.rect.height.get()
				});
			if !fits {
				self.host_panel_groups.insert(group_id, group);
				return Vec::new();
			}
		}

		let dimensions = self.group_dimensions(&group);
		let panel_by_buffer = group
			.current_members
			.iter()
			.chain(group.retained_members.values())
			.filter_map(|member| {
				self.host_panel_buffer_id(member.panel_id)
					.map(|buffer_id| (buffer_id, member.panel_id))
			})
			.collect::<HashMap<_, _>>();
		let buffer_by_panel = panel_by_buffer
			.iter()
			.map(|(buffer_id, panel_id)| (*panel_id, *buffer_id))
			.collect::<HashMap<_, _>>();
		let orphan_ids = group
			.retained_members
			.keys()
			.copied()
			.chain(
				group
					.detached_members
					.iter()
					.copied()
					.filter(|panel_id| !current_ids.contains(panel_id)),
			)
			.collect::<HashSet<_>>();

		let mut current_panes_by_panel = HashMap::new();
		let mut removed_buffers = Vec::new();
		for member in &group.current_members {
			let Some(pane_id) = self.pane_id_for_panel(member.panel_id) else {
				continue;
			};
			if self.layout.group_containing_pane(pane_id) == Some(group_id) {
				continue;
			}
			if let Some(pane) = self.layout.take_leaf(pane_id) {
				current_panes_by_panel.insert(member.panel_id, (pane_id, pane));
			}
		}
		for panel_id in &orphan_ids {
			let Some(pane_id) = self.pane_id_for_panel(*panel_id) else {
				continue;
			};
			if self.layout.group_containing_pane(pane_id) == Some(group_id) {
				continue;
			}
			if self.layout.take_leaf(pane_id).is_some()
				&& let Some(buffer_id) = self.host_panel_buffer_id(*panel_id)
			{
				removed_buffers.push(buffer_id);
			}
		}

		let mut foreign_panes = Vec::new();
		if has_root {
			let mut reconcile = Some(|old: LayoutNode| {
				let mut panes = Vec::new();
				old.into_leaf_panes(&mut panes);
				for (pane_id, pane) in panes {
					let panel_id = pane
						.tabs
						.iter()
						.find_map(|view| panel_by_buffer.get(&view.buffer_id).copied());
					match panel_id {
						Some(panel_id) if current_ids.contains(&panel_id) => {
							current_panes_by_panel.insert(panel_id, (pane_id, pane));
						}
						Some(panel_id) if orphan_ids.contains(&panel_id) => {
							if let Some(buffer_id) = buffer_by_panel.get(&panel_id) {
								removed_buffers.push(*buffer_id);
							}
						}
						_ => foreign_panes.push((pane_id, pane)),
					}
				}
				None
			});
			let edited = self.layout.reconcile_group_root(group_id, &mut reconcile);
			debug_assert_eq!(edited, GroupRootEditResult::Replaced);
		}

		for member in &group.current_members {
			if let std::collections::hash_map::Entry::Vacant(slot) =
				current_panes_by_panel.entry(member.panel_id)
			{
				let (pane_id, pane, _) = self.make_group_member_pane(member, dimensions);
				slot.insert((pane_id, pane));
			}
		}
		let tiled = group
			.current_members
			.iter()
			.filter_map(|member| {
				current_panes_by_panel
					.remove(&member.panel_id)
					.map(|(pane_id, pane)| (pane_id, pane, member.shape))
			})
			.collect::<Vec<_>>();
		let tile = (!tiled.is_empty()).then(|| {
			build_host_panel_group_tile(group_id, tiled, HostPanelGroupLayout::RowMajorTile)
		});
		let foreign = build_plain_pane_branch(foreign_panes);
		let replacement = match (tile, foreign) {
			(Some(tile), Some(foreign)) => Some(LayoutNode::Split {
				group_root: None,
				direction: SplitDirection::Vertical,
				children: vec![tile, foreign],
				ratios: vec![DEFAULT_SPLIT_RATIO, DEFAULT_SPLIT_RATIO],
			}),
			(Some(tile), None) => Some(tile),
			(None, Some(foreign)) => Some(foreign),
			(None, None) => None,
		};

		if has_root {
			if let Some(replacement) = replacement {
				let anchor = group
					.latest_valid_anchor
					.filter(|pane_id| self.layout.find_pane(*pane_id).is_some())
					.unwrap_or(self.focused_pane_id);
				let inserted = self.layout.split_leaf_with_subtree(
					anchor,
					SplitDirection::Vertical,
					replacement,
					false,
				);
				debug_assert!(inserted);
			}
		} else if let Some(replacement) = replacement {
			let anchor = group.latest_valid_anchor.expect("reset preflighted anchor");
			let inserted = self.layout.split_leaf_with_subtree(
				anchor,
				SplitDirection::Vertical,
				replacement,
				false,
			);
			debug_assert!(inserted);
		}

		group.layout_state = HostPanelGroupLayoutState::Automatic;
		group.retained_members.clear();
		group.detached_members.clear();
		self.host_panel_groups.insert(group_id, group);
		debug_assert!(self.layout.count_group_roots(group_id) <= 1);
		removed_buffers
	}

	pub(crate) fn host_panel_group_producer_released(&mut self, buffer_id: BufferId) {
		for group in self.host_panel_groups.values_mut() {
			if group.producer_buffer_id == Some(buffer_id) {
				group.producer_buffer_id = None;
			}
		}
	}

	pub(crate) fn collect_unreachable_host_panel_groups(&mut self) {
		let unreachable = self
			.host_panel_groups
			.iter()
			.filter_map(|(group_id, group)| {
				if group.producer_buffer_id.is_some() {
					return None;
				}
				let has_placement = group
					.current_members
					.iter()
					.map(|member| member.panel_id)
					.chain(group.retained_members.keys().copied())
					.chain(group.detached_members.iter().copied())
					.any(|panel_id| self.pane_id_for_panel(panel_id).is_some());
				(!has_placement).then_some(*group_id)
			})
			.collect::<Vec<_>>();
		for group_id in unreachable {
			if let Some(root) = self.layout.find_group_root_mut(group_id) {
				root.set_group_root(None);
			}
			self.host_panel_groups.remove(&group_id);
		}
	}

	/// Queue an ordered frontend event. Accumulated on the session so producers
	/// scattered through command dispatch and buffer release do not each need
	/// the event sender threaded into them; the run loop drains in order.
	pub(crate) fn queue_host_event(&mut self, event: crate::engine::HostEvent) {
		self.pending_host_events.push(event);
	}

	pub(crate) fn take_host_events(&mut self) -> Vec<crate::engine::HostEvent> {
		std::mem::take(&mut self.pending_host_events)
	}

	pub(crate) fn create_unnamed_text_view(
		&mut self,
		registry: &UastRegistry,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Result<View, String> {
		let pending_buffer_id = self.buffers.next_buffer_id();
		if let Some(saved) = self.saved_documents.as_mut() {
			saved.mint(pending_buffer_id, None)?;
		}
		let (buffer, leaf) = match TextBuffer::new_unnamed(registry) {
			Ok(created) => created,
			Err(error) => {
				if let Some(saved) = self.saved_documents.as_mut() {
					let _ = saved.remove(pending_buffer_id);
				}
				return Err(error);
			}
		};
		let buffer_id = self.buffers.insert(BufferContent::Text(buffer));
		assert_eq!(buffer_id, pending_buffer_id, "reserved BufferId changed");
		let view_id = self.allocate_view_id();
		Ok(View::new_text(
			view_id,
			buffer_id,
			leaf,
			&self.persistent_settings,
			viewport_width,
			viewport_lines,
		))
	}

	pub(crate) fn create_text_view_for_buffer(
		&mut self,
		registry: &UastRegistry,
		buffer_id: BufferId,
		viewport_width: u16,
		viewport_lines: u32,
	) -> Result<View, String> {
		let document = self
			.buffers
			.get(buffer_id)
			.and_then(Buffer::text)
			.ok_or_else(|| "Buffer is not a text buffer".to_string())?;
		let root = document.root_id;
		let topology_rev = document.topology_rev;
		let state_id = document.ledger.current_state_id;
		let initial_leaf = registry
			.find_node_at_doc_byte_raw(root, DocByte::ZERO)
			.node_id;
		let view_id = self.allocate_view_id();
		let mut view = View::new_text(
			view_id,
			buffer_id,
			initial_leaf,
			&self.persistent_settings,
			viewport_width,
			viewport_lines,
		);
		view.retarget_text_buffer(buffer_id, initial_leaf, topology_rev, state_id);
		Ok(view)
	}

	pub(crate) fn ensure_output_panel(
		&mut self,
		registry: &UastRegistry,
		name: &str,
	) -> Result<(BufferId, bool), String> {
		self.ensure_output_panel_split(registry, name, SplitDirection::Horizontal)
	}

	pub(crate) fn ensure_output_panel_split(
		&mut self,
		registry: &UastRegistry,
		name: &str,
		direction: SplitDirection,
	) -> Result<(BufferId, bool), String> {
		let buffer_id = if let Some(&buffer_id) = self.output_panels.get(name) {
			if self.buffers.get(buffer_id).is_some() {
				buffer_id
			} else {
				self.output_panels.remove(name);
				self.create_output_panel_buffer(registry, name)?
			}
		} else {
			self.create_output_panel_buffer(registry, name)?
		};

		if self.layout.view_id_for_buffer(buffer_id).is_some() {
			return Ok((buffer_id, false));
		}

		let requester = self.focused_pane_id;
		let view = self.create_text_view_for_buffer(
			registry,
			buffer_id,
			DEFAULT_VIEWPORT_WIDTH,
			DEFAULT_VIEWPORT_LINES,
		)?;
		let pane = EditorPane::from_view(view);
		if self
			.split_pane_with_new_pane_order(requester, direction, pane, false)
			.is_none()
		{
			return Err("Unable to open output panel".to_string());
		}
		self.focused_pane_id = requester;
		Ok((buffer_id, true))
	}

	fn create_output_panel_buffer(
		&mut self,
		registry: &UastRegistry,
		name: &str,
	) -> Result<BufferId, String> {
		let (document, leaf) = TextBuffer::new_unnamed(registry)?;
		let buffer_id = self
			.buffers
			.insert_with_role(BufferContent::Text(document), BufferRole::OutputPanel);
		self.output_panels.insert(name.to_string(), buffer_id);
		self.output_panel_leaves.insert(buffer_id, leaf);
		Ok(buffer_id)
	}

	pub(crate) fn replace_output_panel(
		&mut self,
		registry: &UastRegistry,
		buffer_id: BufferId,
		text: &[u8],
		highlights: Vec<HighlightSpan>,
	) -> Result<(), String> {
		let mut attached_view_ids = Vec::new();
		self.layout
			.collect_view_ids_for_buffer(buffer_id, &mut attached_view_ids, usize::MAX);
		let old_view_state = attached_view_ids
			.into_iter()
			.filter_map(|view_id| {
				self.layout.find_view(view_id).map(|view| {
					let cursor = match &view.content {
						ViewContent::Text(text) => Some(text.cursor.primary().byte()),
						_ => None,
					};
					(view_id, view.common().scroll_y, cursor)
				})
			})
			.collect::<Vec<_>>();
		let document = self
			.buffers
			.get_mut(buffer_id)
			.filter(|buffer| buffer.is_output_panel())
			.and_then(Buffer::text_mut)
			.ok_or_else(|| "Output panel buffer is no longer available".to_string())?;
		let root = document.root_id;
		let leaf = *self
			.output_panel_leaves
			.get(&buffer_id)
			.ok_or_else(|| "Output panel leaf is no longer available".to_string())?;
		let payload_before = registry.authoritative_payload_snapshot();
		registry.replace_virtual_document_data(root, leaf, text.to_vec());
		let payload_after = registry.authoritative_payload_snapshot();
		document.uast_accounting.record_payload_transaction(
			payload_after
				.added
				.checked_sub(payload_before.added)
				.expect("output-panel payload-added counter regressed"),
			payload_after
				.removed
				.checked_sub(payload_before.removed)
				.expect("output-panel payload-removed counter regressed"),
			0,
		);
		document.topology_rev = document.topology_rev.next();
		document.file_path = None;
		document.canonical_path = None;
		document.backing_path = None;
		document.backing_device_id = None;
		document.file_stamp = None;
		document.active_language_id = None;
		document.shadow = None;
		document.ledger.clear();
		document.search_matches.clear();
		document.bump_search_revision();
		// reset_pipeline clears producer spans as part of the generic semantic
		// invalidation; restore the producer-owned spans afterward.
		let panel_highlights = highlights;
		document.semantic.reset_pipeline();
		document.semantic.highlights = panel_highlights;

		let mut view_ids = Vec::new();
		self.layout
			.collect_view_ids_for_buffer(buffer_id, &mut view_ids, usize::MAX);
		for view_id in view_ids {
			if let Some(view) = self.layout.find_view_mut(view_id) {
				view.retarget_text_buffer(
					buffer_id,
					leaf,
					document.topology_rev,
					document.ledger.current_state_id,
				);
				if let Some((_, scroll_y, cursor)) =
					old_view_state.iter().find(|(id, _, _)| *id == view_id)
				{
					view.common_mut().scroll_y =
						(*scroll_y).min(registry.get_total_newlines(root).saturating_sub(1));
					if let (Some(cursor), ViewContent::Text(text)) = (cursor, &mut view.content) {
						text.cursor.primary_mut().set_byte(
							DocByte::new(cursor.get().min(registry.get_total_bytes(root))),
							Affinity::After,
						);
					}
				}
			}
		}
		Ok(())
	}

	pub(crate) fn editor_rect(&self) -> PaneRect {
		PaneRect::new(
			ScreenCol::ZERO,
			ScreenRow::ZERO,
			PaneCol::new(self.terminal_width.get()),
			PaneRow::new(
				self.terminal_height
					.get()
					.saturating_sub(STATUS_BAR_ROWS as u32),
			),
		)
	}

	pub(crate) fn pane_layouts(&self) -> Vec<PaneLayout> {
		let mut layouts = Vec::new();
		self.layout.collect_pane_layouts(
			self.editor_rect(),
			&mut layouts,
			self.divider_reservation,
		);
		layouts
	}

	pub(crate) fn pane_ids(&self) -> Vec<PaneId> {
		let mut ids = Vec::new();
		self.layout.collect_pane_ids(&mut ids);
		ids
	}

	pub(crate) fn browser_pane_id(&self) -> Option<PaneId> {
		self.pane_ids().into_iter().find(|pane_id| {
			self.layout
				.find_pane(*pane_id)
				.is_some_and(|pane| pane.active_tab().is_file_browser())
		})
	}

	pub(crate) fn focus_pane(&mut self, pane_id: PaneId) -> bool {
		if self.layout.find_pane(pane_id).is_some() {
			if self.is_navigation_target_pane(pane_id) {
				self.browser_target_pane_id = Some(pane_id);
			}
			self.focused_pane_id = pane_id;
			true
		} else {
			false
		}
	}

	/// Last focused editable pane. Navigation panes (file browser and output
	/// panels) deliberately do not replace it, so they can open entries where
	/// the user was reading before navigating.
	pub(crate) fn navigation_target_pane_id(&self) -> Option<PaneId> {
		let pane_id = self.browser_target_pane_id?;
		self.is_navigation_target_pane(pane_id).then_some(pane_id)
	}

	fn is_navigation_target_pane(&self, pane_id: PaneId) -> bool {
		let Some(pane) = self.layout.find_pane(pane_id) else {
			return false;
		};
		let Some(buffer) = self.buffers.get(pane.active_tab().buffer_id) else {
			return false;
		};
		!buffer.is_output_panel() && !buffer.is_file_browser() && !buffer.is_markdown_preview()
	}

	pub(crate) fn focus_pane_in_direction(&mut self, direction: PaneFocusDirection) -> bool {
		let layouts = self.pane_layouts();
		let Some(focused_rect) = layouts
			.iter()
			.find(|layout| layout.pane_id == self.focused_pane_id)
			.map(|layout| layout.rect)
		else {
			return false;
		};

		let mut best = None;
		for layout in layouts {
			if layout.pane_id == self.focused_pane_id {
				continue;
			}

			let candidate_rect = layout.rect;
			let candidate = match direction {
				PaneFocusDirection::Left
					if candidate_rect.right().get() <= focused_rect.screen_col.get() =>
				{
					(
						u32::from(!spans_overlap(
							candidate_rect.screen_row.get(),
							candidate_rect.bottom().get(),
							focused_rect.screen_row.get(),
							focused_rect.bottom().get(),
						)),
						focused_rect
							.screen_col
							.get()
							.saturating_sub(candidate_rect.right().get()),
						pane_midpoint_row(candidate_rect).abs_diff(pane_midpoint_row(focused_rect)),
						layout.pane_id,
					)
				}
				PaneFocusDirection::Right
					if candidate_rect.screen_col.get() >= focused_rect.right().get() =>
				{
					(
						u32::from(!spans_overlap(
							candidate_rect.screen_row.get(),
							candidate_rect.bottom().get(),
							focused_rect.screen_row.get(),
							focused_rect.bottom().get(),
						)),
						candidate_rect
							.screen_col
							.get()
							.saturating_sub(focused_rect.right().get()),
						pane_midpoint_row(candidate_rect).abs_diff(pane_midpoint_row(focused_rect)),
						layout.pane_id,
					)
				}
				PaneFocusDirection::Up
					if candidate_rect.bottom().get() <= focused_rect.screen_row.get() =>
				{
					(
						u32::from(!spans_overlap(
							candidate_rect.screen_col.get(),
							candidate_rect.right().get(),
							focused_rect.screen_col.get(),
							focused_rect.right().get(),
						)),
						focused_rect
							.screen_row
							.get()
							.saturating_sub(candidate_rect.bottom().get()),
						pane_midpoint_col(candidate_rect).abs_diff(pane_midpoint_col(focused_rect)),
						layout.pane_id,
					)
				}
				PaneFocusDirection::Down
					if candidate_rect.screen_row.get() >= focused_rect.bottom().get() =>
				{
					(
						u32::from(!spans_overlap(
							candidate_rect.screen_col.get(),
							candidate_rect.right().get(),
							focused_rect.screen_col.get(),
							focused_rect.right().get(),
						)),
						candidate_rect
							.screen_row
							.get()
							.saturating_sub(focused_rect.bottom().get()),
						pane_midpoint_col(candidate_rect).abs_diff(pane_midpoint_col(focused_rect)),
						layout.pane_id,
					)
				}
				_ => continue,
			};
			best = better_focus_candidate(best, candidate);
		}

		if let Some((_, _, _, pane_id)) = best {
			self.focus_pane(pane_id)
		} else {
			false
		}
	}

	pub(crate) fn split_focused_pane(
		&mut self,
		registry: &UastRegistry,
		direction: SplitDirection,
	) -> bool {
		let destination_group = self.layout.group_containing_pane(self.focused_pane_id);
		let new_pane_id = self.next_pane_id;
		self.next_pane_id = self.next_pane_id.next();
		let current_buffer_id = self
			.layout
			.find_pane(self.focused_pane_id)
			.expect("focused pane must exist")
			.active_tab()
			.buffer_id;
		let view = if self
			.buffers
			.get(current_buffer_id)
			.is_some_and(|buffer| buffer.text().is_some())
		{
			self.create_text_view_for_buffer(
				registry,
				current_buffer_id,
				DEFAULT_VIEWPORT_WIDTH,
				DEFAULT_VIEWPORT_LINES,
			)
		} else {
			self.create_unnamed_text_view(registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
		};
		let Ok(view) = view else {
			return false;
		};
		let buffer_id = view.buffer_id;
		let new_pane = EditorPane::from_view(view);
		if self
			.layout
			.split_leaf(self.focused_pane_id, direction, new_pane_id, new_pane)
		{
			if let Some(group_id) = destination_group {
				self.mark_host_panel_group_manual(group_id);
			}
			let focused = self.focus_pane(new_pane_id);
			debug_assert!(focused);
			true
		} else {
			if self.layout.view_count_for_buffer(buffer_id) == 0 {
				self.buffers.remove(buffer_id);
			}
			false
		}
	}

	pub(crate) fn split_pane_with_new_pane_order(
		&mut self,
		target: PaneId,
		direction: SplitDirection,
		new_pane: EditorPane,
		new_first: bool,
	) -> Option<PaneId> {
		let destination_group = self.layout.group_containing_pane(target);
		let new_pane_id = self.next_pane_id;
		self.next_pane_id = self.next_pane_id.next();
		if self.layout.split_leaf_with_new_pane_order(
			target,
			direction,
			new_pane_id,
			new_pane,
			new_first,
		) {
			if let Some(group_id) = destination_group {
				self.mark_host_panel_group_manual(group_id);
			}
			let focused = self.focus_pane(new_pane_id);
			debug_assert!(focused);
			Some(new_pane_id)
		} else {
			None
		}
	}

	pub(crate) fn open_file_browser_sidebar(&mut self, browser_pane: EditorPane) -> Option<PaneId> {
		if let Some(existing) = self.browser_pane_id() {
			self.focus_pane(existing);
			return Some(existing);
		}
		if self
			.layout
			.find_pane(self.focused_pane_id)
			.is_some_and(|pane| !pane.active_tab().is_file_browser())
		{
			self.browser_target_pane_id = Some(self.focused_pane_id);
		}

		let pane_id = self.split_pane_with_new_pane_order(
			self.focused_pane_id,
			SplitDirection::Vertical,
			browser_pane,
			true,
		)?;

		const FILE_BROWSER_MAX_WIDTH: u32 = 20;
		while let Some(current_width) = self
			.pane_layouts()
			.into_iter()
			.find(|layout| layout.pane_id == pane_id)
			.map(|layout| layout.rect.width.get())
		{
			if current_width <= FILE_BROWSER_MAX_WIDTH {
				break;
			}
			if self.resize_focused_pane(SplitDirection::Vertical, PaneResizeDirection::Decrease)
				!= PaneResizeResult::Resized
			{
				break;
			}
		}

		Some(pane_id)
	}

	pub(crate) fn resize_focused_pane(
		&mut self,
		axis: SplitDirection,
		direction: PaneResizeDirection,
	) -> PaneResizeResult {
		let group_id = self.layout.group_containing_pane(self.focused_pane_id);
		let before = group_id.and_then(|group_id| self.layout.group_structure(group_id));
		let result = self.layout.resize_leaf(
			self.focused_pane_id,
			axis,
			direction,
			self.editor_rect(),
			self.divider_reservation,
		);
		if result == PaneResizeResult::Resized
			&& group_id.is_some_and(|group_id| before != self.layout.group_structure(group_id))
		{
			self.mark_host_panel_group_manual(group_id.expect("changed group"));
		}
		result
	}

	pub(crate) fn resize_pane_boundary(
		&mut self,
		before: PaneId,
		after: PaneId,
		axis: SplitDirection,
		target: u32,
	) -> bool {
		let before_group = self.layout.group_containing_pane(before);
		let internal_group = before_group
			.filter(|group_id| self.layout.group_containing_pane(after) == Some(*group_id));
		let resized = self.layout.resize_boundary(
			before,
			after,
			axis,
			target,
			self.editor_rect(),
			self.divider_reservation,
		);
		if resized && let Some(group_id) = internal_group {
			self.mark_host_panel_group_manual(group_id);
		}
		resized
	}

	pub(crate) fn mark_host_panel_group_manual_for_pane(&mut self, pane_id: PaneId) {
		if let Some(group_id) = self.layout.group_containing_pane(pane_id) {
			self.mark_host_panel_group_manual(group_id);
		}
	}

	fn mark_host_panel_group_manual(&mut self, group_id: HostPanelGroupId) {
		if let Some(group) = self.host_panel_groups.get_mut(&group_id) {
			group.layout_state = HostPanelGroupLayoutState::Manual;
		}
	}

	/// Move an existing pane by exchanging it with a target or by inserting it
	/// beside that target. This is deliberately a session operation: GUI hosts
	/// describe a drop, while the engine alone mutates the split tree.
	pub(crate) fn move_pane(&mut self, source: PaneId, target: PaneId, edge: PaneDropEdge) -> bool {
		if source == target
			|| !self.layout.contains_pane(source)
			|| !self.layout.contains_pane(target)
		{
			return false;
		}
		let source_group = self.layout.group_containing_pane(source);
		let destination_group = self.layout.group_containing_pane(target);
		let source_panel = self
			.layout
			.find_pane(source)
			.and_then(|pane| self.panel_id_in_pane(pane));
		let owner_group = source_panel.and_then(|panel_id| self.group_owning_panel(panel_id));
		let moved = match edge {
			PaneDropEdge::Center => self.layout.swap_leaf_contents(source, target),
			edge => {
				let (direction, new_first) = match edge {
					PaneDropEdge::Left => (SplitDirection::Vertical, true),
					PaneDropEdge::Right => (SplitDirection::Vertical, false),
					PaneDropEdge::Top => (SplitDirection::Horizontal, true),
					PaneDropEdge::Bottom => (SplitDirection::Horizontal, false),
					PaneDropEdge::Center => unreachable!("handled above"),
				};
				let Some(pane) = self.layout.take_leaf(source) else {
					return false;
				};
				self.layout
					.split_leaf_with_existing_pane_order(target, direction, source, pane, new_first)
			}
		};
		if moved {
			if let Some(group_id) = destination_group {
				self.mark_host_panel_group_manual(group_id);
			}
			if let (Some(panel_id), Some(owner_group)) = (source_panel, owner_group)
				&& source_group == Some(owner_group)
				&& destination_group != Some(owner_group)
			{
				if let Some(group) = self.host_panel_groups.get_mut(&owner_group) {
					group.detached_members.insert(panel_id);
				}
				self.clear_empty_group_root(owner_group);
			}
			// Center swaps content into the target position; edge drops preserve
			// the source id. Focusing the target for center drops keeps the
			// dragged content visibly active in both cases.
			self.focused_pane_id = if edge == PaneDropEdge::Center {
				target
			} else {
				source
			};
		}
		moved
	}

	pub(crate) fn close_focused_pane(&mut self) -> Option<EditorPane> {
		if self.layout.pane_count() <= 1 {
			return None;
		}

		let focus_before_close = self.focused_pane_id;
		self.mark_host_panel_group_manual_for_pane(focus_before_close);
		let next_focus = self
			.pane_ids()
			.into_iter()
			.find(|pane_id| *pane_id != focus_before_close);
		if let Some(removed_pane) = self.layout.take_leaf(focus_before_close) {
			self.diff_pairs.remove(&focus_before_close);
			self.diff_pairs
				.retain(|_, pair| pair.partner_pane_id != focus_before_close);
			self.focused_pane_id = next_focus
				.filter(|pane_id| self.layout.find_pane(*pane_id).is_some())
				.or_else(|| self.layout.first_leaf_id())
				.unwrap_or(focus_before_close);
			Some(removed_pane)
		} else {
			None
		}
	}

	pub(crate) fn update_terminal_size(&mut self, width: u16, height: u16) {
		self.terminal_width = ScreenCol::new(width as u32);
		self.terminal_height = ScreenRow::new(height as u32);
	}
}

#[cfg(test)]
mod tests {
	use super::{
		AddCaretResult, Affinity, BufferContent, CanonicalPath, CursorMode, CursorSet, CursorState,
		DiffPair, DiffRole, EditorPane, EditorSession, GENERATION_PARK_CAP,
		HostPanelGroupLayoutState, LayoutLeaf, LayoutNode, MAX_CURSORS, ParkedGenerationCache,
		ParkedGenerationRoot, RootTransition, SettingsPersistent, SnapshotProvenance, TextAnchor,
		TextBuffer, UastOwnerAccounting, View, caret_from_doc_byte, host_panel_tile_minimum,
		resolve_theme_name, split_child_rects, with_file_browser_buffer_mut, with_text_buffer_mut,
	};
	use crate::core::{
		BufferId, DEFAULT_VIEWPORT_LINES, DEFAULT_VIEWPORT_WIDTH, DocByte, DocDisplayCol, DocLine,
		PaneCol, PaneRect, PaneRow, RegistryOwnerId, ScreenCol, ScreenRow, StateId, TopologyRev,
		ViewId,
	};
	use crate::ecs::UastRegistry;
	#[cfg(unix)]
	use crate::engine::PaneResizeDirection;
	use crate::engine::event_bus::EngineEvent;
	use crate::engine::event_subscribers::BaselineBuilder;
	use crate::engine::file_browser::{create_file_browser_buffer, initialize_file_browser};
	use crate::engine::folding::FoldSet;
	use crate::engine::jobs::{JobSpawnError, JobSpec};
	use crate::engine::runtime_registry::{ActionLease, RegistrationBatch};
	use crate::engine::test_support::{build_document, build_split_virtual_document};
	use crate::engine::{
		HostPanelDescriptor, HostPanelGroupId, HostPanelGroupLayout, HostPanelGroupMember,
		HostPanelGroupShape, HostPanelId, PaneDividerReservation, PaneDropEdge,
	};
	use crate::engine::{SplitDirection, UiColorOverrides, VisualKind};
	use crate::uast::UastMutation;
	use std::collections::BTreeMap;
	use std::ffi::OsString;
	use std::path::PathBuf;
	use std::time::Duration;

	use crate::engine::settings::SaveDurability;
	use crate::engine::undo::{HistoryDirection, HistoryReplay, TextDelta};

	fn settings_with_language_themes(
		language_themes: BTreeMap<String, String>,
	) -> SettingsPersistent {
		SettingsPersistent {
			theme_name: "onedark".to_string(),
			minimap_enabled: true,
			wrap_enabled: true,
			line_numbers_enabled: true,
			relative_line_numbers_enabled: false,
			visible_whitespace: true,
			reload_enabled: true,
			save_durability: SaveDurability::default(),
			ui_colors: UiColorOverrides::default(),
			language_themes,
		}
	}

	fn test_editor_session(registry: &UastRegistry) -> EditorSession {
		EditorSession::new(
			registry,
			"onedark".to_string(),
			true,
			true,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		)
	}

	#[cfg(unix)]
	#[test]
	fn product_0061_logical_layout_is_head_neutral_before_codec() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let root = std::env::temp_dir().join(format!(
			"libaryon-layout-head-neutral-{}-{}",
			std::process::id(),
			rand::random::<u64>()
		));
		let saved = crate::core::SessionStore::at(root.clone())
			.create()
			.unwrap();
		session.attach_new_saved_session(saved).unwrap();
		session.update_terminal_size(120, 40);
		assert!(session.split_focused_pane(&registry, SplitDirection::Horizontal));
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		assert_eq!(
			session.resize_focused_pane(SplitDirection::Vertical, PaneResizeDirection::Increase,),
			super::PaneResizeResult::Resized,
			"the oracle needs unequal integer weights, not a symmetric tree"
		);

		let logical_at_small_viewport = session.layout.structure().into_bytes();
		let checkpoint_at_small_viewport =
			crate::engine::layout_checkpoint::capture_layout(&session).unwrap();
		let document_ids = session
			.saved_documents
			.as_ref()
			.unwrap()
			.session
			.snapshot()
			.documents()
			.map(crate::core::DocumentIdentity::id)
			.collect();
		let owner_at_small_viewport = crate::engine::layout_checkpoint::encode_layout(
			&checkpoint_at_small_viewport,
			&document_ids,
			crate::engine::layout_checkpoint::LAYOUT_OWNER_BUDGET,
		)
		.unwrap();
		let rects_at_small_viewport = session.pane_layouts();
		assert_eq!(rects_at_small_viewport.len(), 3);

		session.update_terminal_size(360, 120);
		let logical_at_large_viewport = session.layout.structure().into_bytes();
		let checkpoint_at_large_viewport =
			crate::engine::layout_checkpoint::capture_layout(&session).unwrap();
		let owner_at_large_viewport = crate::engine::layout_checkpoint::encode_layout(
			&checkpoint_at_large_viewport,
			&document_ids,
			crate::engine::layout_checkpoint::LAYOUT_OWNER_BUDGET,
		)
		.unwrap();
		let rects_at_large_viewport = session.pane_layouts();
		session.set_divider_reservation(PaneDividerReservation::NONE);
		let checkpoint_at_zero_reservation =
			crate::engine::layout_checkpoint::capture_layout(&session).unwrap();
		let owner_at_zero_reservation = crate::engine::layout_checkpoint::encode_layout(
			&checkpoint_at_zero_reservation,
			&document_ids,
			crate::engine::layout_checkpoint::LAYOUT_OWNER_BUDGET,
		)
		.unwrap();
		let rects_at_zero_reservation = session.pane_layouts();

		assert_eq!(
			logical_at_large_viewport, logical_at_small_viewport,
			"logical split direction and integer weights must not depend on viewport geometry"
		);
		assert_ne!(
			rects_at_large_viewport, rects_at_small_viewport,
			"the rendered PaneRect control must change when the viewport is 3x larger"
		);
		assert_eq!(
			owner_at_large_viewport, owner_at_small_viewport,
			"production libaryon_layout bytes must be head-neutral across a 3x viewport resize"
		);
		assert_eq!(
			owner_at_zero_reservation, owner_at_small_viewport,
			"PRODUCT-0074's runtime divider profile must never enter libaryon_layout bytes"
		);
		assert_ne!(
			rects_at_zero_reservation, rects_at_large_viewport,
			"the control proves the same logical tree derives head-specific rectangles"
		);
		assert!(
			String::from_utf8(logical_at_large_viewport)
				.unwrap()
				.contains("H["),
			"the fixture must contain the outer horizontal split"
		);
		drop(session);
		let _ = std::fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[test]
	fn product_0061_restored_manual_group_reattaches_without_retiling() {
		use crate::engine::layout_checkpoint::{
			CheckpointDirection, CheckpointGroupLayoutState, CheckpointGroupShape, DurableTab,
			HostPanelCheckpoint, HostPanelGroupCheckpoint, HostPanelGroupMemberCheckpoint,
			LayoutCheckpoint, LayoutCheckpointNode, LeafPath, restore_layout,
		};

		let root = std::env::temp_dir().join(format!(
			"libaryon-restored-group-{}-{}",
			std::process::id(),
			rand::random::<u64>()
		));
		let saved = crate::core::SessionStore::at(root.clone())
			.create()
			.unwrap();
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.attach_new_saved_session(saved).unwrap();
		let producer_buffer = session
			.layout
			.find_pane(session.focused_pane_id)
			.unwrap()
			.active_tab()
			.buffer_id;
		let document_id = session
			.saved_documents
			.as_ref()
			.unwrap()
			.document_id(producer_buffer)
			.unwrap();
		let group_id = HostPanelGroupId::new(17);
		let first = HostPanelId::new(11);
		let detached = HostPanelId::new(12);
		let checkpoint = LayoutCheckpoint {
			root: Some(LayoutCheckpointNode::Split {
				group_root: None,
				direction: CheckpointDirection::Vertical,
				ratios: vec![5, 3, 2],
				children: vec![
					LayoutCheckpointNode::Leaf {
						group_root: None,
						tabs: vec![DurableTab::Document {
							document_id: document_id.get(),
						}],
						active_tab: 0,
					},
					LayoutCheckpointNode::Leaf {
						group_root: Some(group_id.get()),
						tabs: vec![DurableTab::HostPanel {
							panel_id: first.get(),
						}],
						active_tab: 0,
					},
					LayoutCheckpointNode::Leaf {
						group_root: None,
						tabs: vec![
							DurableTab::HostPanel {
								panel_id: first.get(),
							},
							DurableTab::HostPanel {
								panel_id: detached.get(),
							},
						],
						active_tab: 1,
					},
				],
			}),
			focused_leaf: Some(LeafPath(vec![0])),
			panels: vec![
				HostPanelCheckpoint {
					panel_id: first.get(),
					kind: "plot".to_owned(),
					title: "same title".to_owned(),
				},
				HostPanelCheckpoint {
					panel_id: detached.get(),
					kind: "plot".to_owned(),
					title: "same title".to_owned(),
				},
			],
			groups: vec![HostPanelGroupCheckpoint {
				group_id: group_id.get(),
				layout_state: CheckpointGroupLayoutState::Manual,
				current_members: vec![
					HostPanelGroupMemberCheckpoint {
						panel_id: first.get(),
						shape: CheckpointGroupShape::Compact,
					},
					HostPanelGroupMemberCheckpoint {
						panel_id: detached.get(),
						shape: CheckpointGroupShape::Expansive,
					},
				],
				retained_members: vec![],
				detached_members: vec![detached.get()],
				latest_anchor: Some(LeafPath(vec![0])),
			}],
		};
		let expected_checkpoint = checkpoint.clone();
		assert!(restore_layout(&registry, &mut session, checkpoint).unwrap());
		assert_eq!(
			crate::engine::layout_checkpoint::capture_layout(&session).unwrap(),
			expected_checkpoint,
			"PRODUCT-0061 logical meaning must round-trip independently of fresh runtime IDs"
		);
		let restored_events = session.take_host_events();
		assert_eq!(
			restored_events.len(),
			2,
			"a panel present in two views emits one PanelRestored per identity, not per occurrence"
		);
		let first_buffer = session.host_panel_buffer_id(first).unwrap();
		assert_eq!(
			session.layout.view_count_for_buffer(first_buffer),
			2,
			"duplicate panel occurrences share one restored backing buffer"
		);
		assert_ne!(
			first, detached,
			"identical descriptors do not collapse identity"
		);
		let before = session.layout.structure();
		let anchor = session.focused_pane_id;
		let members = vec![
			product_0015_member(first.get(), HostPanelGroupShape::Compact),
			product_0015_member(detached.get(), HostPanelGroupShape::Expansive),
		];
		assert!(
			session
				.ensure_host_panel_group(
					group_id,
					Some(anchor),
					members,
					HostPanelGroupLayout::RowMajorTile,
				)
				.is_empty()
		);
		let group = &session.host_panel_groups[&group_id];
		assert_eq!(group.producer_buffer_id, Some(producer_buffer));
		assert_eq!(group.layout_state, HostPanelGroupLayoutState::Manual);
		assert!(group.detached_members.contains(&detached));
		assert_eq!(
			session.layout.structure(),
			before,
			"PRODUCT-0061 producer reattachment must not retile restored manual placement"
		);
		drop(session);
		let _ = std::fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[test]
	fn product_0061_capture_filters_transient_tabs_and_phantom_leaves() {
		use crate::engine::layout_checkpoint::{DurableTab, LayoutCheckpointNode, capture_layout};

		let root = std::env::temp_dir().join(format!(
			"libaryon-layout-filter-{}-{}",
			std::process::id(),
			rand::random::<u64>()
		));
		let saved = crate::core::SessionStore::at(root.clone())
			.create()
			.unwrap();
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.attach_new_saved_session(saved).unwrap();
		let source = session.focused_pane_id;
		let panel_id = HostPanelId::new(55);
		let panel = session.create_host_panel_view(
			panel_id,
			HostPanelDescriptor {
				kind: "fixture".to_owned(),
				title: "durable panel".to_owned(),
			},
			DEFAULT_VIEWPORT_WIDTH,
			DEFAULT_VIEWPORT_LINES,
		);
		assert!(
			session
				.split_pane_with_new_pane_order(
					source,
					SplitDirection::Vertical,
					EditorPane::from_view(panel),
					false,
				)
				.is_some()
		);
		let (output_buffer, _) = session
			.ensure_output_panel(&registry, "transient-output")
			.expect("output pane");
		let output_pane = session
			.layout
			.view_id_for_buffer(output_buffer)
			.and_then(|view| session.layout.pane_id_for_view(view))
			.expect("output placement");
		session.focused_pane_id = output_pane;
		let checkpoint = capture_layout(&session).unwrap();
		let root_node = checkpoint.root.as_ref().expect("durable remainder");
		let LayoutCheckpointNode::Split {
			children, ratios, ..
		} = root_node
		else {
			panic!("document and host panel remain as a split")
		};
		assert_eq!(children.len(), 2, "transient-only leaf is removed");
		assert_eq!(ratios.len(), 2, "removed leaf also removes its ratio");
		assert!(
			checkpoint.focused_leaf.is_some(),
			"focus on a removed transient leaf deterministically rehomes"
		);
		let mut tabs = Vec::new();
		for child in children {
			let LayoutCheckpointNode::Leaf {
				tabs: child_tabs, ..
			} = child
			else {
				panic!("fixture children are leaves")
			};
			tabs.extend(child_tabs.iter());
		}
		assert_eq!(tabs.len(), 2);
		assert!(
			tabs.iter()
				.any(|tab| matches!(tab, DurableTab::Document { .. }))
		);
		assert!(tabs.iter().any(|tab| {
			matches!(tab, DurableTab::HostPanel { panel_id: value } if *value == panel_id.get())
		}));
		assert_eq!(checkpoint.panels.len(), 1);

		let transient = session
			.create_text_view_for_buffer(
				&registry,
				output_buffer,
				DEFAULT_VIEWPORT_WIDTH,
				DEFAULT_VIEWPORT_LINES,
			)
			.unwrap();
		session.layout = LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id: session.next_pane_id,
			pane: EditorPane::from_view(transient),
		});
		session.focused_pane_id = session.next_pane_id;
		let empty = capture_layout(&session).unwrap();
		assert!(empty.root.is_none());
		assert!(empty.focused_leaf.is_none());
		assert!(empty.panels.is_empty());
		drop(session);
		let _ = std::fs::remove_dir_all(root);
	}

	#[cfg(unix)]
	#[test]
	fn product_0060_saved_document_bindings_are_bijective_and_never_reuse_ids() {
		let root = std::env::temp_dir().join(format!(
			"libaryon-document-bindings-{}-{}",
			std::process::id(),
			rand::random::<u64>()
		));
		let saved = crate::core::SessionStore::at(root.clone())
			.create()
			.expect("create saved session");
		let registry = UastRegistry::new(128);
		let mut editor = test_editor_session(&registry);
		let initial_buffer = editor
			.layout
			.find_pane(editor.focused_pane_id)
			.unwrap()
			.active_tab()
			.buffer_id;
		editor
			.attach_new_saved_session(saved)
			.expect("attach saved session");
		let initial_document = editor
			.saved_documents
			.as_ref()
			.and_then(|saved| saved.document_id(initial_buffer))
			.expect("initial document id");
		assert_eq!(initial_document.get(), 1);
		assert_eq!(
			editor
				.saved_documents
				.as_ref()
				.and_then(|saved| saved.buffer_id(initial_document)),
			Some(initial_buffer)
		);

		let second_view = editor
			.create_text_view_for_buffer(&registry, initial_buffer, 80, 24)
			.expect("second view");
		assert_eq!(second_view.buffer_id, initial_buffer);
		assert_eq!(
			editor
				.saved_documents
				.as_ref()
				.unwrap()
				.session
				.snapshot()
				.documents()
				.count(),
			1,
			"a second view must not mint a second document"
		);

		let later = editor
			.create_unnamed_text_view(&registry, 80, 24)
			.expect("later document");
		let later_document = editor
			.saved_documents
			.as_ref()
			.and_then(|saved| saved.document_id(later.buffer_id))
			.expect("later document id");
		assert_eq!(later_document.get(), 2);
		let lexical = PathBuf::from("symlink-shaped/../kept.txt");
		editor
			.saved_documents
			.as_mut()
			.unwrap()
			.update_path(later.buffer_id, Some(&lexical))
			.expect("update exact path");
		let expected = std::env::current_dir().unwrap().join(&lexical);
		assert_eq!(
			editor
				.saved_documents
				.as_ref()
				.unwrap()
				.session
				.snapshot()
				.document(later_document)
				.unwrap()
				.path(),
			Some(expected.as_path()),
			"absolute lexical identity must retain parent components"
		);

		editor
			.saved_documents
			.as_mut()
			.unwrap()
			.remove(later.buffer_id)
			.expect("remove binding");
		let newest = editor
			.create_unnamed_text_view(&registry, 80, 24)
			.expect("newest document");
		assert_eq!(
			editor
				.saved_documents
				.as_ref()
				.and_then(|saved| saved.document_id(newest.buffer_id))
				.unwrap()
				.get(),
			3,
			"removed durable ids must never be reused"
		);
		drop(editor);
		let _ = std::fs::remove_dir_all(root);
	}

	fn product_0015_members(base: u64) -> Vec<HostPanelGroupMember> {
		(0..4)
			.map(|offset| HostPanelGroupMember {
				panel_id: HostPanelId::new(base + offset),
				kind: "view".to_owned(),
				title: format!("view-{offset}"),
				shape: if offset % 2 == 0 {
					HostPanelGroupShape::Compact
				} else {
					HostPanelGroupShape::Expansive
				},
			})
			.collect()
	}

	fn product_0015_member(panel_id: u64, shape: HostPanelGroupShape) -> HostPanelGroupMember {
		HostPanelGroupMember {
			panel_id: HostPanelId::new(panel_id),
			kind: "view".to_owned(),
			title: format!("view-{panel_id}"),
			shape,
		}
	}

	fn product_0016_descriptor(
		session: &EditorSession,
		panel_id: HostPanelId,
	) -> HostPanelDescriptor {
		let buffer_id = session
			.host_panel_buffer_id(panel_id)
			.expect("host panel buffer");
		session
			.buffers
			.get(buffer_id)
			.and_then(super::Buffer::host_panel)
			.expect("host panel descriptor")
			.descriptor
			.clone()
	}

	#[test]
	fn product_0016_repeated_group_refreshes_descriptors_without_layout_mutation() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(200, 70);
		let group_id = HostPanelGroupId::new(16);
		let anchor = session.focused_pane_id;
		let mut members = product_0015_members(1_600);
		assert!(
			session
				.ensure_host_panel_group(
					group_id,
					Some(anchor),
					members.clone(),
					HostPanelGroupLayout::RowMajorTile,
				)
				.is_empty()
		);

		let shared_panel = members[0].panel_id;
		let shared_buffer = session.host_panel_buffer_id(shared_panel).unwrap();
		let shared_pane = session.pane_id_for_panel(shared_panel).unwrap();
		let second_reference = session.create_host_panel_view_for_buffer(
			shared_buffer,
			DEFAULT_VIEWPORT_WIDTH,
			DEFAULT_VIEWPORT_LINES,
		);
		session
			.layout
			.find_pane_mut(shared_pane)
			.unwrap()
			.tabs
			.push(second_reference);

		let layout = session.layout.structure();
		let group_structure = session.layout.group_structure(group_id).unwrap();
		let pane_layouts = session.pane_layouts();
		let focus = session.focused_pane_id;
		let tab_state = session
			.pane_ids()
			.into_iter()
			.map(|pane_id| {
				let pane = session.layout.find_pane(pane_id).unwrap();
				(
					pane_id,
					pane.active_tab_index,
					pane.tabs.iter().map(|view| view.id).collect::<Vec<_>>(),
				)
			})
			.collect::<Vec<_>>();
		let panel_buffers = members
			.iter()
			.map(|member| {
				(
					member.panel_id,
					session.host_panel_buffer_id(member.panel_id).unwrap(),
				)
			})
			.collect::<Vec<_>>();

		for (index, member) in members.iter_mut().enumerate() {
			member.kind = format!("kind-{index}");
			member.title = format!("renamed-{index}");
		}
		assert!(
			session
				.ensure_host_panel_group(
					group_id,
					Some(anchor),
					members.clone(),
					HostPanelGroupLayout::RowMajorTile,
				)
				.is_empty()
		);

		for (index, member) in members.iter().enumerate() {
			assert_eq!(
				product_0016_descriptor(&session, member.panel_id),
				HostPanelDescriptor {
					kind: format!("kind-{index}"),
					title: format!("renamed-{index}"),
				}
			);
		}
		assert_eq!(session.layout.structure(), layout);
		assert_eq!(
			session.layout.group_structure(group_id).unwrap(),
			group_structure
		);
		assert_eq!(session.pane_layouts(), pane_layouts);
		assert_eq!(session.focused_pane_id, focus);
		assert_eq!(
			session
				.pane_ids()
				.into_iter()
				.map(|pane_id| {
					let pane = session.layout.find_pane(pane_id).unwrap();
					(
						pane_id,
						pane.active_tab_index,
						pane.tabs.iter().map(|view| view.id).collect::<Vec<_>>(),
					)
				})
				.collect::<Vec<_>>(),
			tab_state
		);
		assert_eq!(
			members
				.iter()
				.map(|member| {
					(
						member.panel_id,
						session.host_panel_buffer_id(member.panel_id).unwrap(),
					)
				})
				.collect::<Vec<_>>(),
			panel_buffers
		);
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Automatic
		);

		session.mark_host_panel_group_manual(group_id);
		let manual_structure = session.layout.structure();
		members[0].title = "manual-title".to_owned();
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(
			product_0016_descriptor(&session, shared_panel).title,
			"manual-title"
		);
		assert_eq!(session.layout.structure(), manual_structure);
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Manual
		);

		let retained = members.remove(1);
		let retained_descriptor = product_0016_descriptor(&session, retained.panel_id);
		for member in &mut members {
			member.title.push_str("-current");
		}
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members,
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(
			product_0016_descriptor(&session, retained.panel_id),
			retained_descriptor
		);
		assert!(
			session.host_panel_groups[&group_id]
				.retained_members
				.contains_key(&retained.panel_id)
		);
	}

	#[test]
	fn product_0015_group_root_is_unique_and_decline_leaves_no_partial_tag() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let group_id = HostPanelGroupId::new(1);
		let anchor = session.focused_pane_id;
		let unplaced = session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			product_0015_members(100),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert!(unplaced.is_empty());
		assert_eq!(session.layout.count_group_roots(group_id), 1);

		let mut declined = test_editor_session(&registry);
		declined.update_terminal_size(8, 6);
		let group_id = HostPanelGroupId::new(2);
		let anchor = declined.focused_pane_id;
		let unplaced = declined.ensure_host_panel_group(
			group_id,
			Some(anchor),
			product_0015_members(200),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert!(unplaced.is_empty());
		assert_eq!(declined.layout.count_group_roots(group_id), 0);
		assert_eq!(
			declined
				.host_panel_groups
				.get(&group_id)
				.expect("declined group state")
				.detached_members
				.len(),
			4
		);
	}

	#[test]
	fn product_0074_group_admission_and_reset_follow_the_reservation_profile() {
		let registry = UastRegistry::new(512);
		let members = product_0015_members(7400);

		let mut default = test_editor_session(&registry);
		default.update_terminal_size(16, 6);
		let default_anchor = default.focused_pane_id;
		default.ensure_host_panel_group(
			HostPanelGroupId::new(7401),
			Some(default_anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(
			default
				.layout
				.count_group_roots(HostPanelGroupId::new(7401)),
			0,
			"the four-column default refuses this deliberately marginal anchor"
		);

		let mut compact = test_editor_session(&registry);
		compact.set_divider_reservation(PaneDividerReservation::NONE);
		compact.update_terminal_size(16, 6);
		let compact_anchor = compact.focused_pane_id;
		compact.ensure_host_panel_group(
			HostPanelGroupId::new(7402),
			Some(compact_anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(
			compact
				.layout
				.count_group_roots(HostPanelGroupId::new(7402)),
			1,
			"zero reservation admits the same group through ensure_host_panel_group"
		);

		fn reset_probe(
			registry: &UastRegistry,
			reservation: PaneDividerReservation,
			group_id: HostPanelGroupId,
			base: u64,
		) -> usize {
			let mut session = test_editor_session(registry);
			session.set_divider_reservation(reservation);
			session.update_terminal_size(100, 20);
			let anchor = session.focused_pane_id;
			session.ensure_host_panel_group(
				group_id,
				Some(anchor),
				product_0015_members(base),
				HostPanelGroupLayout::RowMajorTile,
			);
			let root = session
				.layout
				.find_group_root(group_id)
				.expect("wide fixture materializes");
			assert_eq!(
				root.minimum_extent(SplitDirection::Vertical, reservation),
				if reservation == PaneDividerReservation::NONE {
					4
				} else {
					8
				},
				"minimum width consumes the same reservation as rectangle splitting"
			);
			assert_eq!(
				root.minimum_extent(SplitDirection::Horizontal, reservation),
				if reservation == PaneDividerReservation::NONE {
					4
				} else {
					5
				},
				"minimum height consumes the same reservation as rectangle splitting"
			);
			session
				.layout
				.find_group_root_mut(group_id)
				.expect("wide fixture materializes")
				.set_group_root(None);
			session.update_terminal_size(16, 6);
			let group = &session.host_panel_groups[&group_id];
			assert!(group.producer_buffer_id.is_some());
			assert_eq!(group.latest_valid_anchor, Some(anchor));
			assert!(session.layout.can_host_group_subtree(anchor));
			let anchor_rect = session
				.pane_layouts()
				.into_iter()
				.find(|layout| layout.pane_id == anchor)
				.expect("source anchor remains after clearing the group tag")
				.rect;
			let minimum = host_panel_tile_minimum(4, reservation);
			let available = anchor_rect.width.get().saturating_sub(reservation.columns) / 2;
			assert_eq!(
				minimum.0 <= available,
				reservation == PaneDividerReservation::NONE
			);
			assert!(session.reset_host_panel_group(group_id).is_empty());
			session.layout.count_group_roots(group_id)
		}

		assert_eq!(
			reset_probe(
				&registry,
				PaneDividerReservation::TERMINAL,
				HostPanelGroupId::new(7403),
				7500,
			),
			0,
			"the default reset path refuses the marginal anchor"
		);
		assert_eq!(
			reset_probe(
				&registry,
				PaneDividerReservation::NONE,
				HostPanelGroupId::new(7404),
				7600,
			),
			1,
			"zero reservation converges through reset_host_panel_group"
		);
	}

	#[test]
	fn product_0015_only_internal_geometry_changes_enter_manual() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(180, 60);
		let group_id = HostPanelGroupId::new(3);
		let anchor = session.focused_pane_id;
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			product_0015_members(300),
			HostPanelGroupLayout::RowMajorTile,
		);
		let first = session.pane_id_for_panel(HostPanelId::new(300)).unwrap();
		let second = session.pane_id_for_panel(HostPanelId::new(301)).unwrap();
		let boundary = session
			.pane_layouts()
			.into_iter()
			.find(|layout| layout.pane_id == first)
			.unwrap()
			.rect
			.right()
			.get();
		assert!(session.resize_pane_boundary(
			first,
			second,
			SplitDirection::Vertical,
			boundary.saturating_add(2),
		));
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Manual
		);
		let manual = session.layout.group_structure(group_id).unwrap();
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			product_0015_members(300),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(session.layout.group_structure(group_id).unwrap(), manual);

		session
			.host_panel_groups
			.get_mut(&group_id)
			.unwrap()
			.layout_state = HostPanelGroupLayoutState::Automatic;
		let first_rect = session
			.pane_layouts()
			.into_iter()
			.find(|layout| layout.pane_id == first)
			.unwrap()
			.rect;
		assert!(session.resize_pane_boundary(
			anchor,
			first,
			SplitDirection::Vertical,
			first_rect.screen_col.get().saturating_add(2),
		));
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Automatic
		);
		session.update_terminal_size(200, 70);
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Automatic
		);
	}

	#[test]
	fn product_0015_automatic_membership_retiles_only_for_addition() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(200, 70);
		let group_id = HostPanelGroupId::new(4);
		let anchor = session.focused_pane_id;
		let mut members = product_0015_members(400);
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		let four = session.layout.group_structure(group_id).unwrap();
		members.push(product_0015_member(404, HostPanelGroupShape::Expansive));
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_ne!(session.layout.group_structure(group_id).unwrap(), four);
		assert!(session.pane_id_for_panel(HostPanelId::new(404)).is_some());

		let before_omit = session.layout.group_structure(group_id).unwrap();
		let retained_pane = session.pane_id_for_panel(HostPanelId::new(401)).unwrap();
		members.retain(|member| member.panel_id != HostPanelId::new(401));
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(
			session.layout.group_structure(group_id).unwrap(),
			before_omit
		);
		assert_eq!(
			session.pane_id_for_panel(HostPanelId::new(401)),
			Some(retained_pane)
		);
		assert!(
			session.host_panel_groups[&group_id]
				.retained_members
				.contains_key(&HostPanelId::new(401))
		);

		members.insert(1, product_0015_member(401, HostPanelGroupShape::Expansive));
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members,
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(
			session.layout.group_structure(group_id).unwrap(),
			before_omit
		);
		assert_eq!(
			session.pane_id_for_panel(HostPanelId::new(401)),
			Some(retained_pane)
		);
		assert!(
			session.host_panel_groups[&group_id]
				.retained_members
				.is_empty()
		);
	}

	#[test]
	fn product_0015_manual_add_and_reopen_wrap_the_untouched_subtree() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(200, 70);
		let group_id = HostPanelGroupId::new(5);
		let anchor = session.focused_pane_id;
		let mut members = product_0015_members(500);
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		session.mark_host_panel_group_manual(group_id);
		let old_subtree = session
			.layout
			.find_group_root(group_id)
			.unwrap()
			.structure();
		members.push(product_0015_member(504, HostPanelGroupShape::Compact));
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		let root = session.layout.find_group_root(group_id).unwrap();
		let LayoutNode::Split { children, .. } = root else {
			panic!("manual append wraps the group root");
		};
		assert_eq!(children[0].structure(), old_subtree);

		let closed = session.pane_id_for_panel(HostPanelId::new(502)).unwrap();
		session.focus_pane(closed);
		let surviving = session
			.pane_id_for_panel(HostPanelId::new(500))
			.expect("surviving pane");
		assert!(session.close_focused_pane().is_some());
		assert!(session.pane_id_for_panel(HostPanelId::new(502)).is_none());
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members,
			HostPanelGroupLayout::RowMajorTile,
		);
		assert!(session.pane_id_for_panel(HostPanelId::new(502)).is_some());
		assert_eq!(
			session.pane_id_for_panel(HostPanelId::new(500)),
			Some(surviving)
		);
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Manual
		);
	}

	#[test]
	fn product_0015_detachment_survives_ensure_and_reset_reclaims_across_groups() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(240, 80);
		let first_anchor = session.focused_pane_id;
		assert!(session.split_focused_pane(&registry, SplitDirection::Horizontal));
		let second_anchor = session.focused_pane_id;
		session.focus_pane(first_anchor);
		let first_group = HostPanelGroupId::new(6);
		let second_group = HostPanelGroupId::new(7);
		session.ensure_host_panel_group(
			first_group,
			Some(first_anchor),
			product_0015_members(600),
			HostPanelGroupLayout::RowMajorTile,
		);
		session.ensure_host_panel_group(
			second_group,
			Some(second_anchor),
			product_0015_members(700),
			HostPanelGroupLayout::RowMajorTile,
		);

		let moved = session.pane_id_for_panel(HostPanelId::new(600)).unwrap();
		let destination = session.pane_id_for_panel(HostPanelId::new(700)).unwrap();
		assert!(session.move_pane(moved, destination, PaneDropEdge::Bottom));
		assert!(
			session.host_panel_groups[&first_group]
				.detached_members
				.contains(&HostPanelId::new(600))
		);
		assert_eq!(
			session.host_panel_groups[&first_group].layout_state,
			HostPanelGroupLayoutState::Automatic,
			"detachment alone does not dirty the owner"
		);
		assert_eq!(
			session.host_panel_groups[&second_group].layout_state,
			HostPanelGroupLayoutState::Manual,
			"the destination geometry was edited"
		);
		let external_structure = session.layout.structure();
		session.ensure_host_panel_group(
			first_group,
			Some(first_anchor),
			product_0015_members(600),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert_eq!(session.layout.structure(), external_structure);

		assert!(session.reset_host_panel_group(first_group).is_empty());
		let reclaimed = session.pane_id_for_panel(HostPanelId::new(600)).unwrap();
		assert_eq!(
			session.layout.group_containing_pane(reclaimed),
			Some(first_group)
		);
		assert!(
			!session.host_panel_groups[&first_group]
				.detached_members
				.contains(&HostPanelId::new(600))
		);
		assert!(
			!session.host_panel_groups[&second_group]
				.current_members
				.iter()
				.any(|member| member.panel_id == HostPanelId::new(600))
		);
		assert_eq!(session.layout.count_group_roots(first_group), 1);
		assert_eq!(session.layout.count_group_roots(second_group), 1);
	}

	#[test]
	fn product_0015_reset_reconciles_retained_missing_and_foreign_once() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(220, 80);
		let group_id = HostPanelGroupId::new(8);
		let anchor = session.focused_pane_id;
		let mut members = product_0015_members(800);
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);

		members.retain(|member| member.panel_id != HostPanelId::new(803));
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		let detached = session.pane_id_for_panel(HostPanelId::new(800)).unwrap();
		assert!(session.move_pane(detached, anchor, PaneDropEdge::Bottom));
		let missing = session.pane_id_for_panel(HostPanelId::new(801)).unwrap();
		session.focus_pane(missing);
		assert!(session.close_focused_pane().is_some());

		let foreign_view = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.unwrap();
		let foreign_buffer = foreign_view.buffer_id;
		let group_target = session.pane_id_for_panel(HostPanelId::new(802)).unwrap();
		let foreign_pane = session
			.split_pane_with_new_pane_order(
				group_target,
				SplitDirection::Horizontal,
				EditorPane::from_view(foreign_view),
				false,
			)
			.unwrap();

		let removed = session.reset_host_panel_group(group_id);
		assert!(
			removed.contains(
				&session
					.host_panel_buffer_id(HostPanelId::new(803))
					.expect("retained buffer survives until runtime release")
			)
		);
		for panel_id in [800, 801, 802] {
			let pane_id = session
				.pane_id_for_panel(HostPanelId::new(panel_id))
				.unwrap();
			assert_eq!(
				session.layout.group_containing_pane(pane_id),
				Some(group_id)
			);
		}
		assert!(session.pane_id_for_panel(HostPanelId::new(803)).is_none());
		assert!(session.layout.find_pane(foreign_pane).is_some());
		assert_eq!(
			session
				.layout
				.find_pane(foreign_pane)
				.unwrap()
				.active_tab()
				.buffer_id,
			foreign_buffer
		);
		assert_eq!(
			session.host_panel_groups[&group_id].layout_state,
			HostPanelGroupLayoutState::Automatic
		);
		assert!(
			session.host_panel_groups[&group_id]
				.retained_members
				.is_empty()
		);
		assert!(
			session.host_panel_groups[&group_id]
				.detached_members
				.is_empty()
		);

		let layout = session.layout.structure();
		let focus = session.focused_pane_id;
		assert!(session.reset_host_panel_group(group_id).is_empty());
		assert_eq!(session.layout.structure(), layout);
		assert_eq!(session.focused_pane_id, focus);
	}

	#[test]
	fn product_0015_actual_producer_release_retires_only_after_last_placement() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		session.update_terminal_size(180, 60);
		let group_id = HostPanelGroupId::new(9);
		let anchor = session.focused_pane_id;
		let producer = session
			.layout
			.find_pane(anchor)
			.unwrap()
			.active_tab()
			.buffer_id;
		let members = product_0015_members(900);
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			members.clone(),
			HostPanelGroupLayout::RowMajorTile,
		);
		session.host_panel_group_producer_released(producer);
		assert!(
			session.host_panel_groups[&group_id]
				.producer_buffer_id
				.is_none()
		);
		let before = session.layout.structure();
		session.ensure_host_panel_group(
			group_id,
			Some(anchor),
			Vec::new(),
			HostPanelGroupLayout::RowMajorTile,
		);
		assert!(session.reset_host_panel_group(group_id).is_empty());
		assert_eq!(session.layout.structure(), before);

		for member in &members {
			let pane_id = session.pane_id_for_panel(member.panel_id).unwrap();
			session.focus_pane(pane_id);
			assert!(session.close_focused_pane().is_some());
			session.collect_unreachable_host_panel_groups();
			if member.panel_id != members.last().unwrap().panel_id {
				assert!(session.host_panel_groups.contains_key(&group_id));
			}
		}
		assert!(!session.host_panel_groups.contains_key(&group_id));
		assert_eq!(session.layout.count_group_roots(group_id), 0);
	}

	fn exact_reachable_nodes(registry: &UastRegistry, root: crate::ecs::NodeId) -> u64 {
		let mut count = 0u64;
		let mut stack = vec![root];
		while let Some(node) = stack.pop() {
			count += 1;
			let mut child = registry.get_first_child(node);
			while let Some(current) = child {
				stack.push(current);
				child = registry.get_next_sibling(current);
			}
		}
		count
	}

	fn exact_reachable_authoritative_payload(
		registry: &UastRegistry,
		root: crate::ecs::NodeId,
	) -> u64 {
		let mut bytes = 0u64;
		let mut stack = vec![root];
		while let Some(node) = stack.pop() {
			bytes = bytes
				.checked_add(registry.authoritative_payload_len(node))
				.expect("reachable authoritative payload overflow");
			let mut child = registry.get_first_child(node);
			while let Some(current) = child {
				stack.push(current);
				child = registry.get_next_sibling(current);
			}
		}
		bytes
	}

	#[test]
	fn uast_owner_accounting_preserves_its_live_node_algebra() {
		let mut accounting = UastOwnerAccounting::new(2);
		accounting.record_transaction(3, 1, 0, 0, 0);
		assert_eq!(
			accounting,
			UastOwnerAccounting {
				allocated_nodes: 5,
				retired_nodes: 1,
				current_live_nodes: 4,
				parked_nodes: 0,
				payload_added_bytes: 0,
				payload_removed_bytes: 0,
				current_live_payload_bytes: 0,
				parked_payload_bytes: 0,
				root_generations: 1,
			}
		);

		accounting.record_root_transition(6, 0, 0, 0, 0, RootTransition::Retire);
		assert_eq!(accounting.allocated_nodes, 11);
		assert_eq!(accounting.retired_nodes, 5);
		assert_eq!(accounting.current_live_nodes, 6);
		assert_eq!(accounting.parked_nodes, 0);
		assert_eq!(accounting.root_generations, 2);

		accounting.record_root_transition(
			3,
			0,
			0,
			0,
			0,
			RootTransition::Park {
				revived_live: None,
				revived_payload: None,
			},
		);
		assert_eq!(accounting.allocated_nodes, 14);
		assert_eq!(accounting.current_live_nodes, 3);
		assert_eq!(accounting.parked_nodes, 6);

		accounting.record_root_transition(
			0,
			0,
			0,
			0,
			0,
			RootTransition::Park {
				revived_live: Some(6),
				revived_payload: Some(0),
			},
		);
		assert_eq!(accounting.current_live_nodes, 6);
		assert_eq!(accounting.parked_nodes, 3);
		assert_eq!(accounting.root_generations, 4);

		accounting.record_parked_retirement(3, 0);
		assert_eq!(accounting.retired_nodes, 8);
		assert_eq!(accounting.current_live_nodes, 6);
		assert_eq!(accounting.parked_nodes, 0);
		accounting.assert_invariant();
	}

	#[test]
	fn uast_owner_accounting_shape_excludes_hydrated_residency() {
		// Keep this exhaustive: owner payload is authoritative-only. Hydrated
		// residency remains a registry-global diagnostic under ADR 0001.
		let UastOwnerAccounting {
			allocated_nodes: _,
			retired_nodes: _,
			current_live_nodes: _,
			parked_nodes: _,
			payload_added_bytes: _,
			payload_removed_bytes: _,
			current_live_payload_bytes: _,
			parked_payload_bytes: _,
			root_generations: _,
		} = UastOwnerAccounting::new(2);
	}

	#[test]
	fn uast_owner_payload_accounting_preserves_live_parked_and_retired_algebra() {
		let mut accounting = UastOwnerAccounting::new(2);
		accounting.record_payload_transaction(10, 0, 0);
		accounting.record_payload_transaction(0, 3, 0);
		assert_eq!(accounting.current_live_payload_bytes, 7);

		accounting.record_root_transition(
			3,
			0,
			4,
			0,
			0,
			RootTransition::Park {
				revived_live: None,
				revived_payload: None,
			},
		);
		assert_eq!(accounting.current_live_payload_bytes, 4);
		assert_eq!(accounting.parked_payload_bytes, 7);

		accounting.record_root_transition(
			0,
			0,
			0,
			0,
			0,
			RootTransition::Park {
				revived_live: Some(2),
				revived_payload: Some(7),
			},
		);
		assert_eq!(accounting.current_live_payload_bytes, 7);
		assert_eq!(accounting.parked_payload_bytes, 4);

		accounting.record_parked_retirement(3, 4);
		assert_eq!(accounting.payload_added_bytes, 14);
		assert_eq!(accounting.payload_removed_bytes, 7);
		assert_eq!(accounting.current_live_payload_bytes, 7);
		assert_eq!(accounting.parked_payload_bytes, 0);

		accounting.record_root_transition(2, 0, 2, 0, 0, RootTransition::Retire);
		assert_eq!(accounting.payload_added_bytes, 16);
		assert_eq!(accounting.payload_removed_bytes, 14);
		assert_eq!(accounting.current_live_payload_bytes, 2);
		assert_eq!(accounting.parked_payload_bytes, 0);
		accounting.assert_invariant();
	}

	#[test]
	#[should_panic(expected = "payload removal exceeds")]
	fn uast_owner_payload_accounting_rejects_underflow() {
		let mut accounting = UastOwnerAccounting::new(2);
		accounting.record_payload_transaction(2, 3, 0);
	}

	#[test]
	#[should_panic(expected = "UAST owner retirement exceeds")]
	fn uast_owner_accounting_rejects_retirement_underflow() {
		let mut accounting = UastOwnerAccounting::new(2);
		accounting.record_transaction(0, 3, 0, 0, 0);
	}

	#[test]
	#[should_panic(expected = "parked retirement exceeds")]
	fn uast_owner_accounting_rejects_parked_retirement_underflow() {
		let mut accounting = UastOwnerAccounting::new(2);
		accounting.record_parked_retirement(1, 0);
	}

	#[test]
	fn parked_generation_cache_evicts_and_touches_in_lru_order() {
		let entry = |generation_id: u64| ParkedGenerationRoot {
			target: crate::engine::undo::GenerationTarget {
				generation_id,
				path: format!("/generation/{generation_id}"),
				byte_len: generation_id,
			},
			root: crate::ecs::NodeId::from_index(generation_id as usize),
			device_id: Some(generation_id as u16),
			live_nodes: generation_id,
			payload_bytes: generation_id,
			file_identity: crate::svp::BackingIdentity {
				length: generation_id,
				modified_nanos: generation_id as u128,
				file_id: Some(generation_id as u128),
			},
		};
		let mut cache = ParkedGenerationCache::default();
		for generation_id in 0..GENERATION_PARK_CAP as u64 {
			assert!(cache.insert(entry(generation_id)).is_none());
		}
		let touched_target = cache.entries()[1].target.clone();
		let touched = cache.take(&touched_target).expect("cached target");
		cache.restore(touched);

		let evicted = cache
			.insert(entry(GENERATION_PARK_CAP as u64))
			.expect("capacity overflow must evict");
		assert_eq!(evicted.target.generation_id, 0);
		assert_eq!(
			cache
				.entries()
				.iter()
				.map(|entry| entry.target.generation_id)
				.collect::<Vec<_>>(),
			vec![2, 3, 1, 4]
		);
	}

	#[test]
	fn graft_accounting_retires_descendants_but_keeps_the_root_live() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("focused pane")
			.active_tab()
			.id;
		let buffer_id = session.layout.find_view(view_id).unwrap().buffer_id;
		let green = crate::engine::language::GreenNode {
			kind: crate::uast::SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: 0,
			children: Vec::new(),
		};

		let initial = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		assert_eq!(
			initial.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, initial.root_id)
		);

		for expected_retired in 1..=2 {
			with_text_buffer_mut(
				&registry,
				&mut session.buffers,
				&mut session.layout,
				&mut session.events,
				view_id,
				|editor| {
					let retired_descendants = editor
						.document
						.uast_accounting
						.current_live_nodes
						.saturating_sub(1);
					crate::engine::structural_graft::apply_graft(
						&registry,
						editor.document.root_id,
						&green,
						b"",
					)
					.expect("empty graft is byte-identical");
					registry.note_nodes_retired(retired_descendants);
					editor.document.bump_topology_rev();
				},
			)
			.expect("graft through text mutation boundary");

			let document = session
				.buffers
				.get(buffer_id)
				.and_then(|buffer| buffer.text())
				.expect("text buffer");
			assert_eq!(
				document.uast_accounting.current_live_nodes,
				exact_reachable_nodes(&registry, document.root_id)
			);
			assert_eq!(document.uast_accounting.current_live_nodes, 2);
			assert_eq!(document.uast_accounting.retired_nodes, expected_retired);
			assert_eq!(document.uast_accounting.root_generations, 1);
		}

		let before = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.unwrap()
			.uast_accounting;
		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|editor| {
				let mismatched = crate::engine::language::GreenNode {
					byte_len: 1,
					..green.clone()
				};
				assert!(
					crate::engine::structural_graft::apply_graft(
						&registry,
						editor.document.root_id,
						&mismatched,
						b"x",
					)
					.is_err()
				);
			},
		);
		let after = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.unwrap()
			.uast_accounting;
		assert_eq!(after, before, "rejected graft must not move accounting");
	}

	#[test]
	fn repeated_grafts_reconcile_live_and_orphaned_authoritative_payload() {
		const SOURCE: &[u8] = b"fn measured() {}\n";
		const GRAFTS: u64 = 3;
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("focused pane")
			.active_tab()
			.id;
		let buffer_id = session.layout.find_view(view_id).unwrap().buffer_id;

		let mut chunk = registry.reserve_chunk(2).expect("physical fixture");
		let metrics = crate::uast::SpanMetrics {
			byte_length: SOURCE.len() as u64,
			newlines: 1,
		};
		let root = chunk.spawn_node(crate::uast::SemanticKind::RelationalTable, None, metrics);
		let leaf = chunk.spawn_node(
			crate::uast::SemanticKind::Token,
			Some(crate::svp::SvpPointer {
				lba: 0,
				byte_length: SOURCE.len() as u32,
				device_id: 77,
				head_trim: 0,
			}),
			metrics,
		);
		chunk.append_local_child(root, leaf);
		{
			let document = session
				.buffers
				.get_mut(buffer_id)
				.and_then(|buffer| buffer.text_mut())
				.expect("text buffer");
			registry.note_nodes_retired(document.uast_accounting.current_live_nodes);
			document.root_id = root;
			document.uast_accounting = UastOwnerAccounting::new(2);
		}
		let baseline = registry.authoritative_payload_snapshot();
		let green = crate::engine::language::GreenNode {
			kind: crate::uast::SemanticKind::RelationalTable,
			provider_kind: crate::uast::ProviderKindId::NONE,
			byte_len: SOURCE.len() as u32,
			children: Vec::new(),
		};

		for _ in 0..GRAFTS {
			with_text_buffer_mut(
				&registry,
				&mut session.buffers,
				&mut session.layout,
				&mut session.events,
				view_id,
				|editor| {
					let retired_nodes = editor
						.document
						.uast_accounting
						.current_live_nodes
						.saturating_sub(1);
					let retired_payload =
						editor.document.uast_accounting.current_live_payload_bytes;
					let graft = crate::engine::structural_graft::apply_graft_tracked(
						&registry,
						editor.document.root_id,
						&green,
						SOURCE,
					)
					.expect("graft is byte-identical");
					registry.retire_orphaned_forest(
						graft.retired_heads,
						retired_nodes,
						retired_payload,
						crate::ecs::registry::ReclaimFence::AfterPublication,
					);
					editor.document.bump_topology_rev();
				},
			)
			.expect("graft through owner boundary");
		}

		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		assert_eq!(
			document.uast_accounting.current_live_payload_bytes,
			SOURCE.len() as u64
		);
		assert_eq!(
			document.uast_accounting.current_live_payload_bytes,
			exact_reachable_authoritative_payload(&registry, document.root_id)
		);
		let payload = registry.authoritative_payload_snapshot();
		let orphaned_delta = payload.orphaned_added - baseline.orphaned_added;
		// Stage-3b baseline: the cold physical tree contributes no payload.
		// The first graft becomes live; each later graft leaves one exact source
		// copy orphaned.
		assert_eq!(orphaned_delta, (GRAFTS - 1) * SOURCE.len() as u64);
		assert_eq!(
			payload.retained() - baseline.retained(),
			document.uast_accounting.current_live_payload_bytes + orphaned_delta
		);

		session.reclaim_queue.collect_registry_notices(&registry);
		assert_eq!(
			registry.authoritative_payload_snapshot().orphaned(),
			orphaned_delta,
			"retirement frame must retain quarantined payload"
		);
		session.reclaim_queue.finish_frame();
		let live_payload = document.uast_accounting.current_live_payload_bytes;
		while session.reclaim_queue.has_eligible_work() {
			let step = session.reclaim_queue.drain_eligible(&registry, 1);
			assert!(
				step.visited_nodes <= 1,
				"reclaim step exceeded its node budget"
			);
			let partial = registry.authoritative_payload_snapshot();
			assert_eq!(
				partial.retained() - baseline.retained(),
				live_payload + (partial.orphaned() - baseline.orphaned()),
				"retained payload must reconcile after every partial reclaim step"
			);
		}
		let reclaimed = registry.authoritative_payload_snapshot();
		assert_eq!(reclaimed.orphaned() - baseline.orphaned(), 0);
		assert_eq!(
			reclaimed.orphaned_removed - baseline.orphaned_removed,
			(GRAFTS - 1) * SOURCE.len() as u64
		);
		assert_eq!(reclaimed.retained() - baseline.retained(), live_payload);

		let slot_high_water = registry.stats().allocated_nodes;
		let allocation_events = registry.allocation_event_count();
		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|editor| {
				let retired_nodes = editor
					.document
					.uast_accounting
					.current_live_nodes
					.saturating_sub(1);
				let retired_payload = editor.document.uast_accounting.current_live_payload_bytes;
				let graft = crate::engine::structural_graft::apply_graft_tracked(
					&registry,
					editor.document.root_id,
					&green,
					SOURCE,
				)
				.expect("recycled graft is byte-identical");
				registry.retire_orphaned_forest(
					graft.retired_heads,
					retired_nodes,
					retired_payload,
					crate::ecs::registry::ReclaimFence::AfterPublication,
				);
				editor.document.bump_topology_rev();
			},
		)
		.expect("graft through owner boundary");

		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		assert_eq!(
			registry.stats().allocated_nodes,
			slot_high_water,
			"recycled graft must hold the slot high-water flat"
		);
		assert_eq!(
			registry.allocation_event_count(),
			allocation_events + 1,
			"recycled publication remains an owner allocation event"
		);
		assert_eq!(
			document.uast_accounting.allocated_nodes - document.uast_accounting.retired_nodes,
			document.uast_accounting.current_live_nodes + document.uast_accounting.parked_nodes,
			"owner lifetime algebra must survive recycled allocation"
		);
		assert_eq!(
			document.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, document.root_id),
			"owner live nodes must reconcile after recycled allocation"
		);
	}

	#[test]
	fn abandoned_candidate_allocations_retire_without_becoming_live() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.unwrap()
			.active_tab()
			.id;
		let buffer_id = session.layout.find_view(view_id).unwrap().buffer_id;

		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|_| {
				let mut candidate = registry.reserve_chunk(2).expect("candidate nodes");
				let root = candidate.spawn_node(
					crate::uast::SemanticKind::RelationalTable,
					None,
					crate::uast::SpanMetrics {
						byte_length: 4,
						newlines: 0,
					},
				);
				let leaf = candidate.spawn_node(
					crate::uast::SemanticKind::Token,
					None,
					crate::uast::SpanMetrics {
						byte_length: 4,
						newlines: 0,
					},
				);
				candidate.append_local_child(root, leaf);
				registry.replace_authoritative_data(leaf, b"lost".to_vec());
				registry.retire_orphaned_forest(
					[root],
					2,
					4,
					crate::ecs::registry::ReclaimFence::NeverPublished,
				);
			},
		);

		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.unwrap();
		assert_eq!(document.uast_accounting.allocated_nodes, 4);
		assert_eq!(document.uast_accounting.retired_nodes, 2);
		assert_eq!(document.uast_accounting.current_live_nodes, 2);
		assert_eq!(document.uast_accounting.payload_added_bytes, 4);
		assert_eq!(document.uast_accounting.payload_removed_bytes, 4);
		assert_eq!(document.uast_accounting.current_live_payload_bytes, 0);
		assert_eq!(registry.stats().orphaned_payload_bytes, 4);
		assert_eq!(
			document.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, document.root_id)
		);

		session.reclaim_queue.collect_registry_notices(&registry);
		let reclaimed = session
			.reclaim_queue
			.drain_eligible(&registry, crate::engine::reclaim::RECLAIM_NODES_PER_FRAME);
		assert_eq!(reclaimed.visited_nodes, 2);
		assert_eq!(reclaimed.authoritative_bytes, 4);
		assert_eq!(registry.stats().orphaned_payload_bytes, 0);
	}

	#[test]
	fn snapshot_titles_pair_the_basename_with_the_requested_revision() {
		let provenance = SnapshotProvenance::GitRevision {
			repo_root: PathBuf::from("/repo"),
			rel_path: "src/main.rs".to_string(),
			revision: "HEAD~3".to_string(),
			commit_oid: "c".repeat(40),
			blob_oid: "b".repeat(40),
		};
		// The user-facing label stays symbolic; the OIDs are identity, not
		// display.
		assert_eq!(provenance.title(), "main.rs@HEAD~3");
		let provenance = SnapshotProvenance::GitRevision {
			repo_root: PathBuf::from("/repo"),
			rel_path: "README.md".to_string(),
			revision: "main".to_string(),
			commit_oid: "c".repeat(40),
			blob_oid: "b".repeat(40),
		};
		assert_eq!(provenance.title(), "README.md@main");
	}

	#[test]
	fn synthetic_buffer_mutation_allows_multiple_attached_views() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let (browser, leaf) =
			create_file_browser_buffer(&registry, PathBuf::from(".")).expect("browser buffer");
		let buffer_id = session.buffers.insert(BufferContent::FileBrowser(browser));
		let view_ids = [ViewId::new(100), ViewId::new(101), ViewId::new(102)];
		let tabs = view_ids
			.into_iter()
			.map(|view_id| {
				View::new_file_browser(
					view_id,
					buffer_id,
					leaf,
					&session.persistent_settings,
					DEFAULT_VIEWPORT_WIDTH,
					DEFAULT_VIEWPORT_LINES,
				)
			})
			.collect();
		session.layout = LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id: session.focused_pane_id,
			pane: EditorPane {
				tabs,
				active_tab_index: 0,
			},
		});

		let mutated = with_file_browser_buffer_mut(
			&mut session.buffers,
			&mut session.layout,
			view_ids[0],
			|editor| editor.view.scroll_y = 7,
		);

		assert_eq!(mutated, Some(()));
		assert_eq!(
			session
				.layout
				.find_view(view_ids[1])
				.expect("second browser view")
				.common()
				.scroll_y,
			0,
			"synthetic views retain independent viewport state"
		);
	}

	#[test]
	fn file_browser_payload_has_an_exact_synthetic_owner() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let root_path =
			std::env::temp_dir().join(format!("baryon-payload-browser-{}", std::process::id()));
		std::fs::create_dir_all(&root_path).expect("browser fixture directory");
		std::fs::write(root_path.join("owned.txt"), b"x").expect("browser fixture file");
		let (browser, leaf) =
			create_file_browser_buffer(&registry, root_path.clone()).expect("browser buffer");
		let root = browser.root_id;
		let buffer_id = session.buffers.insert(BufferContent::FileBrowser(browser));
		let view_id = ViewId::new(100);
		session.layout = LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id: session.focused_pane_id,
			pane: EditorPane::from_view(View::new_file_browser(
				view_id,
				buffer_id,
				leaf,
				&session.persistent_settings,
				DEFAULT_VIEWPORT_WIDTH,
				DEFAULT_VIEWPORT_LINES,
			)),
		});

		with_file_browser_buffer_mut(
			&mut session.buffers,
			&mut session.layout,
			view_id,
			|editor| initialize_file_browser(&registry, editor, root_path.clone()),
		)
		.expect("browser mutation boundary")
		.expect("browser rebuild");

		let browser = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.file_browser())
			.expect("browser buffer");
		assert!(browser.uast_accounting.current_live_payload_bytes > 0);
		assert_eq!(
			browser.uast_accounting.current_live_payload_bytes,
			exact_reachable_authoritative_payload(&registry, root)
		);
		assert_eq!(
			registry.authoritative_payload_snapshot().retained(),
			browser.uast_accounting.current_live_payload_bytes
		);
		let _ = std::fs::remove_dir_all(root_path);
	}

	#[test]
	fn output_panel_replacement_updates_its_payload_owner_without_allocating_nodes() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let (buffer_id, _) = session
			.ensure_output_panel(&registry, "payload-test")
			.expect("output panel");
		let allocated = registry.allocated_node_count();
		session
			.replace_output_panel(&registry, buffer_id, b"one\n", Vec::new())
			.expect("first output");
		session
			.replace_output_panel(&registry, buffer_id, b"two\nthree\n", Vec::new())
			.expect("replacement output");

		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("output text buffer");
		assert_eq!(registry.allocated_node_count(), allocated);
		assert_eq!(document.uast_accounting.current_live_payload_bytes, 10);
		assert_eq!(
			document.uast_accounting.current_live_payload_bytes,
			exact_reachable_authoritative_payload(&registry, document.root_id)
		);
		assert_eq!(document.uast_accounting.payload_added_bytes, 10);
		assert_eq!(document.uast_accounting.payload_removed_bytes, 0);
	}

	#[test]
	fn owner_payload_tracks_typing_rebind_hydration_and_promotion() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("focused pane")
			.active_tab()
			.id;
		let buffer_id = session.layout.find_view(view_id).unwrap().buffer_id;
		let root = session.buffers.get(buffer_id).unwrap().root_id().unwrap();
		let leaf = registry.get_first_child(root).expect("flat document leaf");

		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|_| {
				registry.insert_text(leaf, 0, b"abcdef");
			},
		);
		let document = session.buffers.get(buffer_id).unwrap().text().unwrap();
		assert_eq!(document.uast_accounting.current_live_payload_bytes, 6);

		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|_| {
				crate::engine::document::io::rebind_document_spans_to_saved_file(
					&registry, root, 9,
				);
			},
		);
		let after_rebind = session
			.buffers
			.get(buffer_id)
			.unwrap()
			.text()
			.unwrap()
			.uast_accounting;
		assert_eq!(after_rebind.current_live_payload_bytes, 0);
		assert_eq!(after_rebind.payload_removed_bytes, 6);

		let counters_before_hydration = registry.authoritative_payload_snapshot();
		registry.publish_hydrated_data(leaf, b"abcdef".to_vec());
		assert_eq!(
			registry.authoritative_payload_snapshot(),
			counters_before_hydration
		);

		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|_| {
				registry.insert_text(leaf, 6, b"!");
			},
		);
		let document = session.buffers.get(buffer_id).unwrap().text().unwrap();
		assert_eq!(document.uast_accounting.current_live_payload_bytes, 7);
		assert_eq!(
			document.uast_accounting.current_live_payload_bytes,
			exact_reachable_authoritative_payload(&registry, root)
		);
		assert_eq!(document.uast_accounting.payload_added_bytes, 13);
		assert_eq!(document.uast_accounting.payload_removed_bytes, 6);
	}

	#[test]
	fn synthetic_fold_materialization_reconciles_replaced_endpoint_payload() {
		const SOURCE: &[u8] =
			b"fn main() {\n    if true {\n        alpha();\n        beta();\n    }\n    gamma();\n}\n";
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("focused pane")
			.active_tab()
			.id;
		let buffer_id = session.layout.find_view(view_id).unwrap().buffer_id;
		let root = session.buffers.get(buffer_id).unwrap().root_id().unwrap();
		let leaf = registry.get_first_child(root).expect("flat document leaf");
		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|_| {
				registry.insert_text(leaf, 0, SOURCE);
			},
		);
		let orphaned_before = registry.stats().orphaned_payload_bytes;
		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|editor| {
				let fold = crate::engine::folding::materialize_fold_boundary_at_cursor(
					&registry,
					root,
					DocLine::new(2),
					DocDisplayCol::new(8),
				)
				.expect("nested block fold");
				editor.view.folds.insert(fold);
				editor.document.bump_topology_rev();
			},
		);

		let document = session.buffers.get(buffer_id).unwrap().text().unwrap();
		assert_eq!(
			document.uast_accounting.current_live_payload_bytes,
			SOURCE.len() as u64
		);
		assert_eq!(
			document.uast_accounting.current_live_payload_bytes,
			exact_reachable_authoritative_payload(&registry, root)
		);
		assert_eq!(
			document.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, root)
		);
		assert_eq!(
			registry.stats().orphaned_payload_bytes - orphaned_before,
			SOURCE.len() as u64,
			"partial materialization replaces and retires the original flat leaf"
		);
	}

	#[test]
	fn synthetic_mutation_ignores_host_panels() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view = session.create_host_panel_view(
			HostPanelId::new(1),
			HostPanelDescriptor {
				kind: "result-table".to_string(),
				title: "Results".to_string(),
			},
			DEFAULT_VIEWPORT_WIDTH,
			DEFAULT_VIEWPORT_LINES,
		);
		let view_id = view.id;
		session.layout = LayoutNode::Leaf(LayoutLeaf {
			group_root: None,
			pane_id: session.focused_pane_id,
			pane: EditorPane::from_view(view),
		});

		let result = with_file_browser_buffer_mut(
			&mut session.buffers,
			&mut session.layout,
			view_id,
			|_| unreachable!("host panels must not expose browser mutation"),
		);
		assert!(
			result.is_none(),
			"host panels are inert at synthetic mutation boundaries"
		);
	}

	#[test]
	fn runtime_registry_authority_is_session_local() {
		let registry = UastRegistry::new(256);
		let mut first = test_editor_session(&registry);
		let mut second = test_editor_session(&registry);

		assert_eq!(
			first
				.runtime_registries
				.allocate_owner()
				.expect("first owner"),
			RegistryOwnerId::new(1)
		);
		assert_eq!(first.runtime_registries.summary().owner_count, 1);
		assert_eq!(second.runtime_registries.summary().owner_count, 0);
		assert_eq!(
			second
				.runtime_registries
				.allocate_owner()
				.expect("second owner"),
			RegistryOwnerId::new(1),
			"each session owns an independent monotonic owner space"
		);
	}

	#[derive(Debug, Clone)]
	struct TestJobAction;

	fn sleeping_job(owner: RegistryOwnerId) -> JobSpec {
		JobSpec {
			program: PathBuf::from("/bin/sh"),
			args: vec![OsString::from("-c"), OsString::from("sleep 5")],
			cwd: None,
			stdin: None,
			timeout: Duration::from_secs(5),
			completion: Some(ActionLease::new(owner, TestJobAction)),
		}
	}

	#[test]
	fn owner_replacement_retires_registry_before_logically_cancelling_jobs() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let (_, _, internal, _requests) = crate::engine::request::channel();
		session.jobs.attach_internal_sender(internal);
		let retired = session.runtime_registries.allocate_owner().unwrap();
		session
			.runtime_registries
			.commit_owner_batch(RegistrationBatch::new(retired))
			.unwrap();
		let job = session.spawn_job(retired, sleeping_job(retired)).unwrap();
		assert!(session.jobs.is_running_unfixed(job));

		let replacement = session.runtime_registries.allocate_owner().unwrap();
		session
			.replace_runtime_owner_generation(retired, RegistrationBatch::new(replacement))
			.unwrap();
		assert!(!session.runtime_registries.owner_is_live(retired));
		assert!(session.runtime_registries.owner_is_live(replacement));
		assert!(session.jobs.is_logically_cancelled(job));
		let fresh = session
			.spawn_job(replacement, sleeping_job(replacement))
			.expect("replacement generation can spawn fresh jobs");
		assert!(session.jobs.is_running_unfixed(fresh));
	}

	#[test]
	fn owner_removal_retires_registry_before_logically_cancelling_jobs() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let (_, _, internal, _requests) = crate::engine::request::channel();
		session.jobs.attach_internal_sender(internal);
		let owner = session.runtime_registries.allocate_owner().unwrap();
		session
			.runtime_registries
			.commit_owner_batch(RegistrationBatch::new(owner))
			.unwrap();
		let job = session.spawn_job(owner, sleeping_job(owner)).unwrap();

		session.remove_runtime_owner(owner).unwrap();
		assert!(!session.runtime_registries.owner_is_live(owner));
		assert!(session.jobs.is_logically_cancelled(job));
		assert_eq!(
			session.spawn_job(owner, sleeping_job(owner)),
			Err(JobSpawnError::RetiredOwner(owner))
		);
	}

	#[test]
	fn failed_owner_replacement_leaves_jobs_running() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let (_, _, internal, _requests) = crate::engine::request::channel();
		session.jobs.attach_internal_sender(internal);
		let retired = session.runtime_registries.allocate_owner().unwrap();
		session
			.runtime_registries
			.commit_owner_batch(RegistrationBatch::new(retired))
			.unwrap();
		let job = session.spawn_job(retired, sleeping_job(retired)).unwrap();
		let unknown = RegistryOwnerId::new(999);
		let error = session
			.replace_runtime_owner_generation(retired, RegistrationBatch::new(unknown))
			.unwrap_err();
		assert!(matches!(
			error,
			crate::engine::runtime_registry::RegistryError::UnknownOwner(_)
		));
		assert!(session.runtime_registries.owner_is_live(retired));
		assert!(session.jobs.is_running_unfixed(job));
		assert_eq!(
			session.spawn_job(unknown, sleeping_job(unknown)),
			Err(JobSpawnError::RetiredOwner(unknown))
		);
	}

	#[test]
	fn subscriber_baseline_is_the_text_lifecycle_projection() {
		let registry = UastRegistry::new(256);
		let session = test_editor_session(&registry);
		let lifecycle = session.capture_text_lifecycle();
		let mut builder = BaselineBuilder::new();
		session
			.contribute_subscriber_baseline(&mut builder)
			.expect("session baseline contribution");
		let baseline = builder.finish().expect("subscriber baseline");

		assert_eq!(
			baseline
				.buffers
				.iter()
				.map(|entry| (entry.buffer, entry.state))
				.collect::<Vec<_>>(),
			lifecycle.buffers.into_iter().collect::<Vec<_>>()
		);
		assert_eq!(
			baseline
				.views
				.iter()
				.map(|entry| (entry.view, entry.buffer))
				.collect::<Vec<_>>(),
			lifecycle.views
		);
		assert_eq!(baseline.focused, lifecycle.focused);
		assert!(baseline.sources.is_empty());
	}

	#[test]
	fn canonical_path_resolves_a_missing_target_through_its_existing_ancestor() {
		let root = crate::engine::test_support::temp_test_path("canonical-missing-parent");
		std::fs::create_dir_all(&root).expect("create canonical-path fixture");
		let direct = CanonicalPath::resolve(root.join("target.txt")).expect("direct identity");
		let alias = CanonicalPath::resolve(root.join("missing").join("..").join("target.txt"))
			.expect("missing-parent alias identity");
		assert_eq!(alias, direct);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn lifecycle_observation_ignores_detached_rollback_and_orders_successful_attach() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);

		let before = session.capture_text_lifecycle();
		let discarded = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.expect("detached view");
		session.buffers.remove(discarded.buffer_id);
		session.emit_text_lifecycle_since(before);
		assert!(session.events.take_pass().is_empty());

		let before = session.capture_text_lifecycle();
		let attached = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.expect("attached view");
		let view = attached.id;
		let buffer = attached.buffer_id;
		let state = session
			.buffers
			.get(buffer)
			.and_then(super::Buffer::text)
			.expect("text buffer")
			.ledger
			.current_state_id;
		let pane = session
			.layout
			.find_pane_mut(session.focused_pane_id)
			.unwrap();
		pane.tabs.push(attached);
		pane.active_tab_index = pane.tabs.len() - 1;
		session.emit_text_lifecycle_since(before);
		assert_eq!(
			session.events.take_pass().into_iter().collect::<Vec<_>>(),
			vec![
				EngineEvent::BufferOpened { buffer, state },
				EngineEvent::ViewCreated { view, buffer },
				EngineEvent::FocusedTextViewChanged {
					from: Some(ViewId::new(1)),
					to: Some(view),
				},
			]
		);
	}

	#[test]
	fn lifecycle_observation_orders_retarget_and_focused_close() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let original_view = ViewId::new(1);
		let original_buffer = BufferId::new(1);

		let before = session.capture_text_lifecycle();
		let detached = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.expect("retarget buffer");
		let target_buffer = detached.buffer_id;
		let target_document = session
			.buffers
			.get(target_buffer)
			.and_then(super::Buffer::text)
			.expect("target text buffer");
		let target_root = target_document.root_id;
		let target_state = target_document.ledger.current_state_id;
		let target_topology = target_document.topology_rev;
		let target_leaf = registry
			.find_node_at_doc_byte_raw(target_root, DocByte::ZERO)
			.node_id;
		session
			.layout
			.find_view_mut(original_view)
			.unwrap()
			.retarget_text_buffer(target_buffer, target_leaf, target_topology, target_state);
		session.buffers.remove(original_buffer);
		session.emit_text_lifecycle_since(before);
		assert_eq!(
			session.events.take_pass().into_iter().collect::<Vec<_>>(),
			vec![
				EngineEvent::BufferOpened {
					buffer: target_buffer,
					state: target_state,
				},
				EngineEvent::ViewRetargeted {
					view: original_view,
					old_buffer: original_buffer,
					new_buffer: target_buffer,
				},
				EngineEvent::BufferClosed {
					buffer: original_buffer,
				},
			]
		);

		let before = session.capture_text_lifecycle();
		let second = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.expect("second focused view");
		let second_view = second.id;
		let second_buffer = second.buffer_id;
		let pane = session
			.layout
			.find_pane_mut(session.focused_pane_id)
			.unwrap();
		pane.tabs.push(second);
		pane.active_tab_index = pane.tabs.len() - 1;
		session.emit_text_lifecycle_since(before);
		session.events.take_pass();

		let before = session.capture_text_lifecycle();
		let pane = session
			.layout
			.find_pane_mut(session.focused_pane_id)
			.unwrap();
		let removed = pane.tabs.pop().expect("focused tab");
		pane.active_tab_index = 0;
		assert_eq!(removed.id, second_view);
		session.buffers.remove(second_buffer);
		session.emit_text_lifecycle_since(before);
		assert_eq!(
			session.events.take_pass().into_iter().collect::<Vec<_>>(),
			vec![
				EngineEvent::FocusedTextViewChanged {
					from: Some(second_view),
					to: Some(original_view),
				},
				EngineEvent::ViewClosed {
					view: second_view,
					buffer: second_buffer,
				},
				EngineEvent::BufferClosed {
					buffer: second_buffer,
				},
			]
		);
	}

	#[test]
	fn lifecycle_observation_keeps_committed_focus_round_trips_lossless() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let left_pane = session.focused_pane_id;
		let left_view = ViewId::new(1);
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		let right_pane = session.focused_pane_id;
		let right_view = session
			.layout
			.find_pane(right_pane)
			.expect("right pane")
			.active_tab()
			.id;

		let before = session.capture_text_lifecycle();
		assert!(session.focus_pane(left_pane));
		session.emit_text_lifecycle_since(before);
		let before = session.capture_text_lifecycle();
		assert!(session.focus_pane(right_pane));
		session.emit_text_lifecycle_since(before);

		assert_eq!(
			session.events.take_pass().into_iter().collect::<Vec<_>>(),
			vec![
				EngineEvent::FocusedTextViewChanged {
					from: Some(right_view),
					to: Some(left_view),
				},
				EngineEvent::FocusedTextViewChanged {
					from: Some(left_view),
					to: Some(right_view),
				},
			]
		);
	}

	#[test]
	fn moving_panes_swaps_contents_or_reinserts_the_existing_leaf() {
		use super::PaneDropEdge;

		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let left = session.focused_pane_id;
		let left_view = session
			.layout
			.find_pane(left)
			.expect("left pane")
			.active_tab()
			.id;
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		let right = session.focused_pane_id;
		let right_view = session
			.layout
			.find_pane(right)
			.expect("right pane")
			.active_tab()
			.id;

		assert!(session.move_pane(right, left, PaneDropEdge::Center));
		assert_eq!(session.focused_pane_id, left);
		assert_eq!(
			session.layout.find_pane(left).unwrap().active_tab().id,
			right_view
		);
		assert_eq!(
			session.layout.find_pane(right).unwrap().active_tab().id,
			left_view
		);

		// Edge drops preserve the source pane id (and all content it owns), but
		// replace the target's old split relationship with a new sibling split.
		assert!(session.move_pane(right, left, PaneDropEdge::Top));
		assert_eq!(session.focused_pane_id, right);
		assert_eq!(
			session.layout.find_pane(right).unwrap().active_tab().id,
			left_view
		);
		assert_eq!(
			session.layout.find_pane(left).unwrap().active_tab().id,
			right_view
		);
		assert_eq!(session.layout.pane_count(), 2);
	}

	#[test]
	fn pane_close_reports_every_view_in_layout_order_before_the_last_buffer() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		let pane_id = session.focused_pane_id;
		let shared_view = session
			.layout
			.find_pane(pane_id)
			.expect("split pane")
			.active_tab()
			.id;
		let extra = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.expect("second tab");
		let extra_view = extra.id;
		let extra_buffer = extra.buffer_id;
		let pane = session.layout.find_pane_mut(pane_id).expect("split pane");
		pane.tabs.push(extra);
		pane.active_tab_index = pane.tabs.len() - 1;

		let before = session.capture_text_lifecycle();
		let removed = session.close_focused_pane().expect("close split pane");
		assert_eq!(removed.tabs.len(), 2);
		session.buffers.remove(extra_buffer);
		session.emit_text_lifecycle_since(before);

		assert_eq!(
			session.events.take_pass().into_iter().collect::<Vec<_>>(),
			vec![
				EngineEvent::FocusedTextViewChanged {
					from: Some(extra_view),
					to: Some(ViewId::new(1)),
				},
				EngineEvent::ViewClosed {
					view: shared_view,
					buffer: BufferId::new(1),
				},
				EngineEvent::ViewClosed {
					view: extra_view,
					buffer: extra_buffer,
				},
				EngineEvent::BufferClosed {
					buffer: extra_buffer,
				},
			]
		);
	}

	#[test]
	fn live_nodes_have_one_buffer_owner_even_with_shared_views_and_path_aliases() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		session.assert_unique_text_node_ownership(&registry);

		let extra = session
			.create_unnamed_text_view(&registry, DEFAULT_VIEWPORT_WIDTH, DEFAULT_VIEWPORT_LINES)
			.expect("independent buffer");
		let extra_buffer = extra.buffer_id;
		session
			.layout
			.find_pane_mut(session.focused_pane_id)
			.unwrap()
			.tabs
			.push(extra);
		session.assert_unique_text_node_ownership(&registry);

		let path = CanonicalPath::resolve(crate::engine::test_support::temp_test_path(
			"node-owner-dedup.txt",
		))
		.expect("canonical path");
		let reservation = session
			.buffers
			.reserve_path(extra_buffer, path.clone())
			.expect("first path owner");
		session.buffers.commit_path(reservation);
		assert_eq!(session.buffers.buffer_for_path(&path), Some(extra_buffer));
		assert!(
			session
				.buffers
				.reserve_path(BufferId::new(1), path)
				.is_err()
		);
	}

	#[cfg(unix)]
	#[test]
	fn canonical_path_resolves_a_dangling_symlink_to_its_missing_target() {
		use std::os::unix::fs::symlink;

		let root = crate::engine::test_support::temp_test_path("canonical-dangling-link");
		std::fs::create_dir_all(&root).expect("create symlink fixture");
		symlink("target.txt", root.join("alias.txt")).expect("create dangling symlink");
		let target = CanonicalPath::resolve(root.join("target.txt")).expect("target identity");
		let alias = CanonicalPath::resolve(root.join("alias.txt")).expect("symlink identity");
		assert_eq!(alias, target);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn closing_a_pane_prunes_both_sides_of_its_diff_pair() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let left = session.focused_pane_id;
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		let right = session.focused_pane_id;
		session.diff_pairs.insert(
			left,
			DiffPair {
				partner_pane_id: right,
				role: DiffRole::Before,
				blocks: Vec::new(),
			},
		);
		session.diff_pairs.insert(
			right,
			DiffPair {
				partner_pane_id: left,
				role: DiffRole::After,
				blocks: Vec::new(),
			},
		);

		assert!(session.close_focused_pane().is_some());

		assert!(session.layout.find_pane(right).is_none());
		assert!(session.diff_pairs.is_empty());
	}

	#[test]
	fn view_ids_are_unique_stable_and_never_reused_after_close() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let initial_pane = session.focused_pane_id;
		let initial_view = session
			.layout
			.find_pane(initial_pane)
			.expect("initial pane")
			.active_tab()
			.id;
		assert_eq!(initial_view, ViewId::new(1));
		let initial_buffer = session.layout.find_view(initial_view).unwrap().buffer_id;
		assert_eq!(initial_buffer, BufferId::new(1));
		assert_eq!(session.buffers.len(), 1);

		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		let second_pane = session.focused_pane_id;
		let second_view = session
			.layout
			.find_pane(second_pane)
			.expect("second pane")
			.active_tab()
			.id;
		assert_eq!(second_view, ViewId::new(2));
		let second_buffer = session.layout.find_view(second_view).unwrap().buffer_id;
		assert_eq!(second_buffer, initial_buffer);
		let mut shared_views = Vec::new();
		let shared_count = session.layout.collect_view_ids_for_buffer(
			initial_buffer,
			&mut shared_views,
			usize::MAX,
		);
		assert_eq!(shared_count, 2);
		assert_eq!(shared_views, vec![initial_view, second_view]);
		let mut capped_views = Vec::new();
		let shared_count =
			session
				.layout
				.collect_view_ids_for_buffer(initial_buffer, &mut capped_views, 1);
		assert_eq!(shared_count, 2);
		assert_eq!(capped_views, vec![initial_view]);
		assert_eq!(session.buffers.next_id(), BufferId::new(2));
		assert_eq!(
			session.layout.find_view(initial_view).map(|tab| tab.id),
			Some(initial_view)
		);
		assert_eq!(
			session.layout.pane_id_for_view(second_view),
			Some(second_pane)
		);

		let removed = session.close_focused_pane().expect("close second pane");
		assert_eq!(removed.active_tab().id, second_view);
		assert_eq!(removed.active_tab().buffer_id, second_buffer);
		assert!(session.buffers.contains(second_buffer));
		assert!(session.layout.find_view(second_view).is_none());
		shared_views.clear();
		let shared_count = session.layout.collect_view_ids_for_buffer(
			initial_buffer,
			&mut shared_views,
			usize::MAX,
		);
		assert_eq!(shared_count, 1);
		assert_eq!(shared_views, vec![initial_view]);

		assert!(session.split_focused_pane(&registry, SplitDirection::Horizontal));
		let replacement_view = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("replacement pane")
			.active_tab()
			.id;
		assert_eq!(replacement_view, ViewId::new(3));
		let replacement_buffer = session
			.layout
			.find_view(replacement_view)
			.unwrap()
			.buffer_id;
		assert_eq!(replacement_buffer, initial_buffer);
		assert_ne!(replacement_view, second_view);
		assert_eq!(
			session.layout.find_view(initial_view).map(|tab| tab.id),
			Some(initial_view)
		);
	}

	#[test]
	fn shared_view_anchor_rebases_at_the_mutation_boundary() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let pane_id = session.focused_pane_id;
		let (buffer_id, acting_view_id) = {
			let view = session.layout.find_pane(pane_id).unwrap().active_tab();
			(view.buffer_id, view.id)
		};
		let navigation_anchor = {
			let document = session
				.buffers
				.get_mut(buffer_id)
				.and_then(|buffer| buffer.text_mut())
				.expect("text buffer");
			document.navigation_anchors.insert(
				document.root_id,
				document.ledger.current_state_id,
				DocByte::ZERO,
			)
		};
		let mut second_view = session
			.create_text_view_for_buffer(
				&registry,
				buffer_id,
				DEFAULT_VIEWPORT_WIDTH,
				DEFAULT_VIEWPORT_LINES,
			)
			.expect("shared view");
		let super::ViewContent::Text(text_view) = &mut second_view.content else {
			unreachable!();
		};
		text_view
			.cursor
			.primary_mut()
			.set_byte(DocByte::ZERO, Affinity::After);
		let second_view_id = second_view.id;
		session
			.layout
			.find_pane_mut(pane_id)
			.unwrap()
			.tabs
			.push(second_view);

		let result = with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			acting_view_id,
			|editor| {
				let mut root = Some(editor.document.root_id);
				let delta = TextDelta {
					global_byte_offset: DocByte::ZERO,
					deleted_bytes: Vec::new(),
					inserted_bytes: "x".as_bytes().to_vec(),
					state_before: StateId::ZERO,
					state_after: StateId::ZERO,
				};
				let snapshot = editor.cursor.snapshot();
				crate::engine::document::edit::apply_deltas_to_document_internal(
					&registry,
					&mut root,
					&mut editor.view.folds,
					&mut editor.document.ledger,
					&mut editor.document.semantic.highlights,
					&mut editor.document.semantic.diagnostics,
					vec![delta.clone()],
					false,
				)?;
				editor.document.ledger.push_group_with_cursors(
					vec![delta],
					snapshot.clone(),
					snapshot,
				);
				Err::<(), String>("operation failed after commit".to_string())
			},
		)
		.expect("acting text view");
		assert_eq!(result, Err("operation failed after commit".to_string()));
		let first_event = session.events.take_pass().pop_front().expect("edit event");
		assert!(matches!(
			first_event,
			EngineEvent::BufferEdited {
				buffer,
				acting_view,
				delta_count: 1,
				deleted_bytes: 0,
				inserted_bytes: 1,
				..
			} if buffer == buffer_id && acting_view == acting_view_id
		));
		let second = session.layout.find_view(second_view_id).unwrap();
		let super::ViewContent::Text(second) = &second.content else {
			unreachable!();
		};
		assert_eq!(second.cursor.byte(), DocByte::new(1));
		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		assert_eq!(
			document
				.navigation_anchors
				.get(navigation_anchor)
				.expect("rebased navigation anchor")
				.anchor
				.byte,
			DocByte::new(1)
		);
		assert_eq!(
			document.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, document.root_id),
			"insert allocation must be attributed to the owning buffer"
		);

		let undone = with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			acting_view_id,
			|editor| {
				let Some(HistoryReplay::Text { deltas, .. }) =
					editor.document.ledger.preview(HistoryDirection::Undo)
				else {
					panic!("text undo must be available");
				};
				let mut root = Some(editor.document.root_id);
				crate::engine::document::edit::apply_deltas_to_document_internal(
					&registry,
					&mut root,
					&mut editor.view.folds,
					&mut editor.document.ledger,
					&mut editor.document.semantic.highlights,
					&mut editor.document.semantic.diagnostics,
					deltas,
					false,
				)
				.expect("undo commit");
				editor.document.ledger.commit(HistoryDirection::Undo);
			},
		);
		assert_eq!(undone, Some(()));
		assert!(matches!(
			session.events.take_pass().pop_front(),
			Some(EngineEvent::BufferEdited {
				buffer,
				acting_view,
				delta_count: 1,
				deleted_bytes: 1,
				inserted_bytes: 0,
				..
			}) if buffer == buffer_id && acting_view == acting_view_id
		));
		let second = session.layout.find_view(second_view_id).unwrap();
		let super::ViewContent::Text(second) = &second.content else {
			unreachable!();
		};
		assert_eq!(second.cursor.byte(), DocByte::ZERO);
		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		assert_eq!(
			document
				.navigation_anchors
				.get(navigation_anchor)
				.expect("undo-rebased navigation anchor")
				.anchor
				.byte,
			DocByte::ZERO
		);
		assert_eq!(
			document.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, document.root_id),
			"undo pruning must retire nodes from the owning buffer"
		);
	}

	#[test]
	fn root_replacement_invalidates_navigation_anchors_at_the_mutation_boundary() {
		let registry = UastRegistry::new(256);
		let mut session = test_editor_session(&registry);
		let view_id = session
			.layout
			.find_pane(session.focused_pane_id)
			.expect("focused pane")
			.active_tab()
			.id;
		let buffer_id = session
			.layout
			.find_view(view_id)
			.expect("text view")
			.buffer_id;
		let anchor_id = {
			let document = session
				.buffers
				.get_mut(buffer_id)
				.and_then(|buffer| buffer.text_mut())
				.expect("text buffer");
			document.navigation_anchors.insert(
				document.root_id,
				document.ledger.current_state_id,
				DocByte::new(99),
			)
		};
		with_text_buffer_mut(
			&registry,
			&mut session.buffers,
			&mut session.layout,
			&mut session.events,
			view_id,
			|editor| {
				let (replacement, _) =
					TextBuffer::new_unnamed(&registry).expect("replacement root");
				editor.document.root_id = replacement.root_id;
			},
		)
		.expect("replace root");

		let document = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		assert_eq!(document.navigation_anchors.get(anchor_id), None);
		assert_eq!(document.uast_accounting.allocated_nodes, 4);
		assert_eq!(document.uast_accounting.retired_nodes, 2);
		assert_eq!(document.uast_accounting.current_live_nodes, 2);
		assert_eq!(document.uast_accounting.root_generations, 2);
		assert_eq!(
			document.uast_accounting.current_live_nodes,
			exact_reachable_nodes(&registry, document.root_id)
		);
	}

	#[test]
	fn product_0074_default_child_rects_bank_the_4_by_1_reservation() {
		let parent = PaneRect::new(
			ScreenCol::new(10),
			ScreenRow::new(5),
			PaneCol::new(100),
			PaneRow::new(41),
		);
		let vertical = split_child_rects(
			parent,
			SplitDirection::Vertical,
			&[1, 1],
			2,
			PaneDividerReservation::TERMINAL,
		);
		assert_eq!(vertical[0].screen_col, ScreenCol::new(10));
		assert_eq!(vertical[0].width, PaneCol::new(48));
		assert_eq!(vertical[1].screen_col, ScreenCol::new(62));
		assert_eq!(vertical[1].width, PaneCol::new(48));

		let horizontal = split_child_rects(
			parent,
			SplitDirection::Horizontal,
			&[1, 1],
			2,
			PaneDividerReservation::TERMINAL,
		);
		assert_eq!(horizontal[0].screen_row, ScreenRow::new(5));
		assert_eq!(horizontal[0].height, PaneRow::new(20));
		assert_eq!(horizontal[1].screen_row, ScreenRow::new(26));
		assert_eq!(horizontal[1].height, PaneRow::new(20));
	}

	#[test]
	fn product_0074_zero_reservation_tiles_in_both_axes() {
		let parent = PaneRect::new(
			ScreenCol::new(10),
			ScreenRow::new(5),
			PaneCol::new(100),
			PaneRow::new(41),
		);
		let vertical = split_child_rects(
			parent,
			SplitDirection::Vertical,
			&[1, 1],
			2,
			PaneDividerReservation::NONE,
		);
		assert_eq!(vertical[0].screen_col, ScreenCol::new(10));
		assert_eq!(vertical[0].width, PaneCol::new(50));
		assert_eq!(vertical[1].screen_col, vertical[0].right());
		assert_eq!(vertical[1].width, PaneCol::new(50));

		let horizontal = split_child_rects(
			parent,
			SplitDirection::Horizontal,
			&[1, 1],
			2,
			PaneDividerReservation::NONE,
		);
		assert_eq!(horizontal[0].screen_row, ScreenRow::new(5));
		assert_eq!(horizontal[0].height, PaneRow::new(20));
		assert_eq!(horizontal[1].screen_row, horizontal[0].bottom());
		assert_eq!(horizontal[1].height, PaneRow::new(21));
	}

	#[test]
	fn resolve_theme_uses_override_before_language_theme() {
		let settings = settings_with_language_themes(BTreeMap::from([(
			"rust".to_string(),
			"glowmax".to_string(),
		)]));

		assert_eq!(
			resolve_theme_name(
				Some(crate::engine::language::LanguageId::from_registered("rust")),
				Some("turbo"),
				&settings
			),
			"turbo"
		);
	}

	#[test]
	fn resolve_theme_uses_language_theme_before_global_theme() {
		let settings = settings_with_language_themes(BTreeMap::from([(
			"rust".to_string(),
			"glowmax".to_string(),
		)]));

		assert_eq!(
			resolve_theme_name(
				Some(crate::engine::language::LanguageId::from_registered("rust")),
				None,
				&settings
			),
			"glowmax"
		);
	}

	#[test]
	fn resolve_theme_uses_global_theme_without_language_match() {
		let settings = settings_with_language_themes(BTreeMap::from([(
			"rust".to_string(),
			"glowmax".to_string(),
		)]));

		assert_eq!(
			resolve_theme_name(
				Some(crate::engine::language::LanguageId::from_registered(
					"python"
				)),
				None,
				&settings
			),
			"onedark"
		);
	}

	#[test]
	fn resolve_theme_uses_global_theme_without_language() {
		let settings = settings_with_language_themes(BTreeMap::new());

		assert_eq!(resolve_theme_name(None, None, &settings), "onedark");
	}

	#[test]
	fn cursor_set_promotes_existing_positions_without_duplicates() {
		let (registry, root) = build_document("abcdef");
		let primary = caret_from_doc_byte(&registry, root, DocByte::new(1)).expect("primary");
		let mut cursors = CursorSet::single(primary);
		let second = caret_from_doc_byte(&registry, root, DocByte::new(4)).expect("second");
		assert_eq!(
			cursors.add_or_promote(&registry, root, second),
			AddCaretResult::Added
		);
		assert_eq!(cursors.len(), 2);
		assert_eq!(
			cursors.add_or_promote(&registry, root, primary),
			AddCaretResult::Promoted
		);
		assert_eq!(cursors.len(), 2);
		assert_eq!(
			cursors.add_or_promote(&registry, root, primary),
			AddCaretResult::Unchanged
		);
	}

	#[test]
	fn leaf_boundary_resolution_honors_anchor_affinity() {
		let (registry, root) = build_split_virtual_document(&["ab", "cd"]);
		let mut folds = FoldSet::default();
		let first = registry.get_first_child(root).expect("first leaf");
		let second = registry.get_next_sibling(first).expect("second leaf");

		let mut before = super::Caret::at_start();
		before.set_anchor(TextAnchor::new(DocByte::new(2), Affinity::Before));
		assert!(before.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::ZERO,
			StateId::ZERO,
		));
		assert_eq!(before.node_cache().expect("node").node, first);
		assert_eq!(before.node_cache().expect("node").offset, 2);

		let mut after = super::Caret::at_start();
		after.set_anchor(TextAnchor::new(DocByte::new(2), Affinity::After));
		assert!(after.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::ZERO,
			StateId::ZERO,
		));
		assert_eq!(after.node_cache().expect("node").node, second);
		assert_eq!(after.node_cache().expect("node").offset, 0);
		assert_eq!(before.line_cache(), after.line_cache());
	}

	#[test]
	fn anchor_writes_and_revisions_invalidate_only_their_owned_cache() {
		let (registry, root) = build_document("alpha\nbeta\n");
		let mut folds = FoldSet::default();
		let mut caret = caret_from_doc_byte(&registry, root, DocByte::new(2)).expect("caret");
		assert!(caret.node_cache().is_some());
		assert!(caret.line_cache().is_some());

		caret.set_anchor(TextAnchor::new(DocByte::new(7), Affinity::After));
		assert!(caret.node_cache().is_none());
		assert!(caret.line_cache().is_none());
		assert!(caret.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::new(1),
			StateId::new(1),
		));
		let line_at_anchor = caret.line_cache();

		assert!(caret.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::new(2),
			StateId::new(1),
		));
		assert_eq!(caret.cache.node_rev, TopologyRev::new(2));
		assert_eq!(caret.cache.line_rev, StateId::new(1));
		assert_eq!(caret.line_cache(), line_at_anchor);

		assert!(caret.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::new(2),
			StateId::new(2),
		));
		assert_eq!(caret.cache.line_rev, StateId::new(2));
	}

	#[test]
	fn pending_target_is_the_rendered_coordinate_without_rewriting_the_anchor() {
		let (registry, root) = build_document("alpha\nbeta\n");
		let mut caret = caret_from_doc_byte(&registry, root, DocByte::new(2)).expect("caret");
		let anchor = caret.anchor;
		let cached_line = caret.line_cache();

		caret.set_pending_target(
			DocLine::new(40_000),
			DocDisplayCol::new(3),
			Affinity::After,
			false,
			None,
		);

		assert_eq!(caret.anchor, anchor);
		assert_eq!(caret.line_cache(), cached_line);
		assert_eq!(caret.abs_line(), DocLine::new(40_000));
		assert_eq!(caret.abs_col(), DocDisplayCol::new(3));
	}

	#[test]
	fn pending_target_preserves_requested_affinity_when_it_resolves() {
		let (registry, root) = build_document("alpha\nbeta\n");
		let mut folds = FoldSet::default();
		let mut caret = caret_from_doc_byte(&registry, root, DocByte::ZERO).expect("caret");

		caret.set_pending_target(
			DocLine::new(1),
			DocDisplayCol::new(2),
			Affinity::Before,
			false,
			None,
		);

		assert!(caret.resolve(
			&registry,
			root,
			&mut folds,
			TopologyRev::ZERO,
			StateId::ZERO,
		));
		assert_eq!(caret.byte(), DocByte::new(8));
		assert_eq!(caret.affinity(), Affinity::Before);
	}

	#[test]
	fn snapshots_preserve_mode_affinity_preferred_columns_and_secondary_order() {
		let (registry, root) = build_document("abcdef");
		let mut primary = caret_from_doc_byte(&registry, root, DocByte::new(1)).expect("primary");
		primary.set_anchor(TextAnchor::new(primary.byte(), Affinity::Before));
		primary.preferred_col = DocDisplayCol::new(9);
		let mut later = caret_from_doc_byte(&registry, root, DocByte::new(4)).expect("later");
		later.preferred_col = DocDisplayCol::new(7);
		let mut earlier = caret_from_doc_byte(&registry, root, DocByte::new(2)).expect("earlier");
		earlier.set_anchor(TextAnchor::new(earlier.byte(), Affinity::Before));
		earlier.preferred_col = DocDisplayCol::new(5);
		let mut state = CursorState {
			mode: CursorMode::Points(CursorSet {
				primary,
				secondary: vec![later, earlier],
			}),
		};

		let points = state.snapshot();
		state.restore_snapshot(points.clone());
		assert_eq!(state.snapshot(), points);
		let CursorMode::Points(cursors) = &state.mode else {
			panic!("points mode must survive");
		};
		assert_eq!(cursors.primary().affinity(), Affinity::Before);
		assert_eq!(cursors.primary().preferred_col, DocDisplayCol::new(9));
		assert_eq!(cursors.secondary()[0].byte(), DocByte::new(4));
		assert_eq!(cursors.secondary()[1].byte(), DocByte::new(2));

		state.set_active_visual(Some((DocByte::ZERO, VisualKind::Char)));
		let visual = state.snapshot();
		state.restore_snapshot(visual.clone());
		assert_eq!(state.snapshot(), visual);
		let CursorMode::Visual { anchor, kind, .. } = state.mode else {
			panic!("visual mode must survive");
		};
		assert_eq!(anchor.affinity, Affinity::Before);
		assert_eq!(kind, VisualKind::Char);
	}

	#[test]
	fn normalization_deduplicates_opposite_affinities_at_one_byte() {
		let (registry, root) = build_document("abc");
		let mut primary = caret_from_doc_byte(&registry, root, DocByte::new(1)).expect("primary");
		primary.set_anchor(TextAnchor::new(primary.byte(), Affinity::Before));
		let secondary = caret_from_doc_byte(&registry, root, DocByte::new(1)).expect("secondary");
		let mut cursors = CursorSet {
			primary,
			secondary: vec![secondary],
		};

		cursors.normalize(&registry, root);

		assert_eq!(cursors.len(), 1);
		assert_eq!(cursors.primary().affinity(), Affinity::Before);
	}

	#[test]
	fn sync_preferred_col_updates_every_caret() {
		let (registry, root) = build_document("abcdef");
		let primary = caret_from_doc_byte(&registry, root, DocByte::new(1)).expect("primary");
		let secondary = caret_from_doc_byte(&registry, root, DocByte::new(4)).expect("secondary");
		let mut cursors = CursorSet {
			primary,
			secondary: vec![secondary],
		};
		for caret in cursors.all_mut() {
			caret.preferred_col = crate::core::DocDisplayCol::ZERO;
		}

		cursors.sync_preferred_col();

		assert!(
			cursors
				.all()
				.all(|caret| caret.preferred_col == caret.abs_col())
		);
	}

	#[test]
	fn cursor_set_limit_rejects_without_partial_mutation() {
		let text = "x".repeat(MAX_CURSORS + 1);
		let (registry, root) = build_document(&text);
		let primary = caret_from_doc_byte(&registry, root, DocByte::ZERO).expect("primary");
		let secondary = (1..MAX_CURSORS)
			.map(|offset| {
				caret_from_doc_byte(&registry, root, DocByte::new(offset as u64)).expect("caret")
			})
			.collect();
		let mut cursors = CursorSet { primary, secondary };
		let extra =
			caret_from_doc_byte(&registry, root, DocByte::new(MAX_CURSORS as u64)).expect("extra");
		assert_eq!(
			cursors.add_or_promote(&registry, root, extra),
			AddCaretResult::AtLimit
		);
		assert_eq!(cursors.len(), MAX_CURSORS);
		assert_eq!(cursors.primary(), &primary);
	}

	#[test]
	fn entering_visual_mode_consumes_secondary_cursors() {
		let (registry, root) = build_document("abcdef");
		let primary = caret_from_doc_byte(&registry, root, DocByte::new(1)).expect("primary");
		let secondary = caret_from_doc_byte(&registry, root, DocByte::new(4)).expect("secondary");
		let mut state = CursorState::single(primary);
		state
			.points_mut()
			.expect("point mode")
			.add_or_promote(&registry, root, secondary);
		assert_eq!(state.len(), 2);

		state.set_active_visual(Some((DocByte::new(1), crate::engine::VisualKind::Char)));
		assert!(matches!(state.mode, CursorMode::Visual { .. }));
		assert_eq!(state.len(), 1);

		state.set_active_visual(None);
		assert!(matches!(state.mode, CursorMode::Points(_)));
		assert_eq!(state.len(), 1);
	}

	#[test]
	fn unnamed_document_refuses_capacity_with_an_honest_remedy() {
		let registry = UastRegistry::new(1);
		let error = match TextBuffer::new_unnamed(&registry) {
			Ok(_) => panic!("two-node document cannot fit one remaining slot"),
			Err(error) => error,
		};
		assert!(error.contains("Document creation refused"));
		assert!(error.contains("UAST registry"));
		assert!(error.contains("Wait for reclamation"));
		assert!(error.contains("restart the editor"));
		assert_eq!(registry.stats().allocated_nodes, 0);
	}
}

#[cfg(test)]
mod known_position_tests {
	use super::{Affinity, Caret};
	use crate::core::{DocByte, DocDisplayCol, DocLine, StateId, TopologyRev};
	use crate::ecs::UastRegistry;
	use crate::engine::folding::FoldSet;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;

	/// One line in two leaves: the first is not loaded, so nothing can read the
	/// line from its start.
	fn line_with_unloaded_head() -> (UastRegistry, crate::ecs::NodeId) {
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(3).expect("OOM");
		let metrics = |byte_length| SpanMetrics {
			byte_length,
			newlines: 0,
		};
		let root = chunk.spawn_node(SemanticKind::RelationalTable, None, metrics(8));
		let head = chunk.spawn_node(SemanticKind::Token, None, metrics(4));
		let tail = chunk.spawn_node(SemanticKind::Token, None, metrics(4));
		chunk.append_local_child(root, head);
		chunk.append_local_child(root, tail);
		unsafe {
			*registry.virtual_data[tail.index()].get() = Some(b"zzzz".to_vec());
		}
		drop(chunk);
		(registry, root)
	}

	#[test]
	fn a_known_position_places_the_caret_without_reading_the_line_start() {
		let (registry, root) = line_with_unloaded_head();
		let (topology, state) = (TopologyRev::new(1), StateId::new(1));
		let mut folds = FoldSet::default();

		// Placing by column must walk the line from its start, which it cannot.
		let mut walked = Caret::at_start();
		assert!(!walked.place_at_line_col(
			&registry,
			root,
			&mut folds,
			topology,
			state,
			DocLine::ZERO,
			DocDisplayCol::new(6),
			false,
			Affinity::After,
		));

		let mut known = Caret::at_start();
		known.place_at_known(
			&registry,
			root,
			topology,
			state,
			DocByte::new(6),
			DocLine::ZERO,
			DocDisplayCol::new(6),
		);
		assert!(known.resolve(&registry, root, &mut folds, topology, state));
		assert_eq!(known.byte(), DocByte::new(6));
		assert_eq!(known.abs_line(), DocLine::ZERO);
		assert_eq!(known.abs_col(), DocDisplayCol::new(6));
	}
}
