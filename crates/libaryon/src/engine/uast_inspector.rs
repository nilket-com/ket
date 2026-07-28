use super::core::MAX_SEMANTIC_BYTES;
use super::language::{LanguageProvider, LanguageRegistry};
use super::protocol::UastInspectorMode;
use super::session::{
	Affinity, EditorSession, UastInspectorPanel, UastInspectorRow, ViewContent,
};
use crate::core::{BufferId, DocByte, DocLine, ViewId};
use crate::ecs::{NodeId, UastRegistry};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use crate::uast::{
	ProviderKindId, SemanticKind, UastGraphNode, UastGraphSnapshot, UastInspectorOptions,
	UastInspectorSnapshot, UastMetricCertainty, UastStorageKind,
};
use std::collections::{HashMap, HashSet};

const INSPECTOR_NODE_BUDGET: usize = 2_048;
const INSPECTOR_DEPTH_BUDGET: u16 = 32;

#[derive(Debug, Clone, Copy)]
struct SourceIdentity {
	view_id: ViewId,
	root: NodeId,
	state_id: crate::core::StateId,
	topology_rev: crate::core::TopologyRev,
	applied_structural_state_id: Option<crate::core::StateId>,
	pending_semantic: bool,
	active_language_id: Option<super::language::LanguageId>,
	cursor_byte: Option<DocByte>,
}

pub(crate) fn open_uast_inspector(
	registry: &UastRegistry,
	session: &mut EditorSession,
	requested_mode: Option<UastInspectorMode>,
) -> Result<(), String> {
	let (focused_buffer_id, source_view_id) = {
		let focused_view = session
			.layout
			.find_pane(session.focused_pane_id)
			.ok_or_else(|| "Focused pane is no longer available".to_string())?
			.active_tab();
		(focused_view.buffer_id, focused_view.id)
	};

	if let Some(panel) = session.uast_inspectors.get_mut(&focused_buffer_id) {
		if let Some(mode) = requested_mode {
			panel.mode = mode;
			panel.rendered_text.clear();
		}
		return Ok(());
	}

	session
		.buffers
		.get(focused_buffer_id)
		.filter(|buffer| !buffer.is_output_panel())
		.and_then(|buffer| buffer.text())
		.ok_or_else(|| "The focused pane is not an inspectable source document".to_string())?;

	let panel_name = format!("uast:{}", focused_buffer_id.get());
	let (panel_buffer_id, _) = session.ensure_output_panel(registry, &panel_name)?;
	let mode = requested_mode.unwrap_or(UastInspectorMode::Semantic);
	session.uast_inspectors.insert(
		panel_buffer_id,
		UastInspectorPanel {
			source_buffer_id: focused_buffer_id,
			source_view_id,
			mode,
			source_root: None,
			source_state_id: None,
			source_topology_rev: None,
			rows: Vec::new(),
			graph: None,
			rendered_text: Vec::new(),
		},
	);
	session.ui.status_message = Some(format!("UAST inspector opened ({})", mode_name(mode)));
	Ok(())
}

pub(crate) fn refresh_uast_inspectors(
	registry: &UastRegistry,
	lang_registry: &LanguageRegistry,
	session: &mut EditorSession,
) {
	session
		.uast_inspectors
		.retain(|buffer_id, _| session.buffers.get(*buffer_id).is_some());
	let panel_ids = session.uast_inspectors.keys().copied().collect::<Vec<_>>();

	for panel_buffer_id in panel_ids {
		let Some((source_buffer_id, mut source_view_id, mode)) = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.map(|panel| {
				(
					panel.source_buffer_id,
					panel.source_view_id,
					panel.mode,
				)
			})
		else {
			continue;
		};

		if !session
			.layout
			.find_view(source_view_id)
			.is_some_and(|view| view.buffer_id == source_buffer_id)
			&& let Some(replacement) = session.layout.view_id_for_buffer(source_buffer_id)
		{
			source_view_id = replacement;
			if let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id) {
				panel.source_view_id = replacement;
			}
		}

		let cursor_byte = session
			.layout
			.find_view(source_view_id)
			.filter(|view| view.buffer_id == source_buffer_id)
			.and_then(|view| match &view.content {
				ViewContent::Text(text) => Some(text.cursor.primary().byte()),
				_ => None,
			});
		let Some((identity, source_name)) = session
			.buffers
			.get(source_buffer_id)
			.and_then(|buffer| buffer.text())
			.map(|document| {
				(
					SourceIdentity {
						view_id: source_view_id,
						root: document.root_id,
						state_id: document.ledger.current_state_id,
						topology_rev: document.topology_rev,
						applied_structural_state_id: document
							.semantic
							.applied_structural_state_id,
						pending_semantic: document
							.semantic
							.pending_semantic_request_id
							.is_some(),
						active_language_id: document.active_language_id,
						cursor_byte,
					},
					document
						.file_path
						.as_deref()
						.or(document.virtual_path.as_deref())
						.unwrap_or("[No Name]")
						.to_owned(),
				)
			})
		else {
			if let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id) {
				panel.graph = None;
			}
			replace_if_changed(
				registry,
				session,
				panel_buffer_id,
				b"UAST inspector detached: the source buffer has closed.\n".to_vec(),
				Vec::new(),
				Vec::new(),
			);
			continue;
		};

		let snapshot = registry.inspect_uast(
			identity.root,
			UastInspectorOptions {
				max_nodes: INSPECTOR_NODE_BUDGET,
				max_depth: INSPECTOR_DEPTH_BUDGET,
				cursor_byte: identity.cursor_byte.map(DocByte::get),
				..UastInspectorOptions::default()
			},
		);
		let provider = identity
			.active_language_id
			.and_then(|language_id| lang_registry.get_provider(language_id));
		let rendered = render_snapshot(&snapshot, identity, &source_name, mode, provider);
		if let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id) {
			panel.source_root = Some(identity.root);
			panel.source_state_id = Some(identity.state_id);
			panel.source_topology_rev = Some(identity.topology_rev);
			panel.graph = rendered.graph.clone();
		}
		replace_if_changed(
			registry,
			session,
			panel_buffer_id,
			rendered.text,
			rendered.highlights,
			rendered.rows,
		);
	}
}

pub(crate) fn activate_selected_row(
	registry: &UastRegistry,
	session: &mut EditorSession,
) -> Result<(), String> {
	let (panel_buffer_id, selected_line) = {
		let view = session
			.layout
			.find_pane(session.focused_pane_id)
			.ok_or_else(|| "Focused pane is no longer available".to_string())?
			.active_tab();
		let line = match &view.content {
			ViewContent::Text(text) => text.cursor.primary().abs_line(),
			_ => return Err("The focused pane is not a UAST inspector".to_string()),
		};
		(view.buffer_id, line)
	};
	let (source_buffer_id, source_view_id, stamped_state, row) = {
		let panel = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.ok_or_else(|| "The focused pane is not a UAST inspector".to_string())?;
		let row = panel
			.rows
			.iter()
			.find(|row| row.line == selected_line)
			.cloned()
			.ok_or_else(|| "Select a UAST node row before jumping".to_string())?;
		(
			panel.source_buffer_id,
			panel.source_view_id,
			panel.source_state_id,
			row,
		)
	};
	jump_to_row(
		registry,
		session,
		source_buffer_id,
		source_view_id,
		stamped_state,
		row,
	)
}

pub(crate) fn activate_graph_node(
	registry: &UastRegistry,
	session: &mut EditorSession,
	inspector_view_id: ViewId,
	node_id: u32,
) -> Result<(), String> {
	let panel_buffer_id = session
		.layout
		.find_view(inspector_view_id)
		.map(|view| view.buffer_id)
		.ok_or_else(|| "The UAST graph view has closed".to_string())?;
	let (source_buffer_id, source_view_id, stamped_state, row) = {
		let panel = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.ok_or_else(|| "The graph no longer belongs to a UAST inspector".to_string())?;
		let row = panel
			.rows
			.iter()
			.find(|row| row.node_id.index() + 1 == node_id as usize)
			.cloned()
			.ok_or_else(|| "The selected graph node is no longer in the snapshot".to_string())?;
		(
			panel.source_buffer_id,
			panel.source_view_id,
			panel.source_state_id,
			row,
		)
	};
	jump_to_row(
		registry,
		session,
		source_buffer_id,
		source_view_id,
		stamped_state,
		row,
	)
}

fn jump_to_row(
	registry: &UastRegistry,
	session: &mut EditorSession,
	source_buffer_id: BufferId,
	mut source_view_id: ViewId,
	stamped_state: Option<crate::core::StateId>,
	row: UastInspectorRow,
) -> Result<(), String> {
	let (current_state, root) = session
		.buffers
		.get(source_buffer_id)
		.and_then(|buffer| buffer.text())
		.map(|document| (document.ledger.current_state_id, document.root_id))
		.ok_or_else(|| "The inspected source buffer has closed".to_string())?;
	if stamped_state != Some(current_state) {
		return Err("The source changed; the stale inspector row was not followed".to_string());
	}
	if !session
		.layout
		.find_view(source_view_id)
		.is_some_and(|view| view.buffer_id == source_buffer_id)
	{
		source_view_id = session
			.layout
			.view_id_for_buffer(source_buffer_id)
			.ok_or_else(|| "The source has no open view to jump to".to_string())?;
	}
	let target = DocByte::new(row.start_byte.get().min(registry.get_total_bytes(root)));
	{
		let source_view = session
			.layout
			.find_view_mut(source_view_id)
			.ok_or_else(|| "The source view has closed".to_string())?;
		let ViewContent::Text(source_text) = &mut source_view.content else {
			return Err("The source view is no longer textual".to_string());
		};
		source_text
			.cursor
			.primary_mut()
			.set_byte(target, Affinity::After);
	}
	if let Some(pane_id) = session.layout.pane_id_for_view(source_view_id) {
		session.focused_pane_id = pane_id;
	}
	session.ui.status_message = Some(format!(
		"UAST #{} [{}..{}) → byte {}",
		row.node_id.index() + 1,
		row.start_byte,
		row.end_byte,
		target
	));
	Ok(())
}

pub(crate) fn export_dot(
	registry: &UastRegistry,
	lang_registry: &LanguageRegistry,
	session: &mut EditorSession,
	path: Option<&str>,
) -> Result<(), String> {
	let focused_buffer_id = session
		.layout
		.find_pane(session.focused_pane_id)
		.ok_or_else(|| "Focused pane is no longer available".to_string())?
		.active_tab()
		.buffer_id;
	let source_buffer_id = session
		.uast_inspectors
		.get(&focused_buffer_id)
		.map_or(focused_buffer_id, |panel| panel.source_buffer_id);
	let (root, active_language_id, source_name) = session
		.buffers
		.get(source_buffer_id)
		.and_then(|buffer| buffer.text())
		.map(|document| {
			(
				document.root_id,
				document.active_language_id,
				document
					.file_path
					.as_deref()
					.or(document.virtual_path.as_deref())
					.unwrap_or("[No Name]")
					.to_owned(),
			)
		})
		.ok_or_else(|| "The focused pane is not an inspectable source document".to_string())?;
	let snapshot = registry.inspect_uast(
		root,
		UastInspectorOptions {
			max_nodes: INSPECTOR_NODE_BUDGET,
			max_depth: INSPECTOR_DEPTH_BUDGET,
			snippet_bytes: 0,
			..UastInspectorOptions::default()
		},
	);
	let provider =
		active_language_id.and_then(|language_id| lang_registry.get_provider(language_id));
	let dot = render_dot(&snapshot, &source_name, provider);

	if let Some(path) = path {
		crate::core::atomic_file::atomic_write(
			std::path::Path::new(path),
			dot.as_bytes(),
			crate::core::atomic_file::AtomicWriteOptions::config(),
		)
		.map_err(|error| format!("Unable to write {path}: {error}"))?;
		session.ui.status_message = Some(format!("Wrote UAST DOT to {path}"));
		return Ok(());
	}

	let panel_name = format!("uast-dot:{}", source_buffer_id.get());
	let (panel_buffer_id, _) = session.ensure_output_panel(registry, &panel_name)?;
	session.replace_output_panel(registry, panel_buffer_id, dot.as_bytes(), Vec::new())?;
	session.ui.status_message = Some("UAST DOT opened".to_string());
	Ok(())
}

fn render_dot(
	snapshot: &UastInspectorSnapshot,
	source_name: &str,
	provider: Option<&dyn LanguageProvider>,
) -> String {
	let mut dot = String::from("digraph uast {\n");
	dot.push_str("  graph [rankdir=TB, bgcolor=\"transparent\"];\n");
	dot.push_str("  node [shape=box, fontname=\"monospace\"];\n");
	dot.push_str(&format!(
		"  label=\"{} · reachable {}{} / allocated {}\";\n",
		dot_escape(source_name),
		snapshot.reachable_visited,
		if snapshot.reachable_complete {
			""
		} else {
			"+ (truncated)"
		},
		snapshot.allocated_nodes,
	));
	for node in &snapshot.nodes {
		let label = provider
			.and_then(|provider| provider.structural_kind_name(node.provider_kind))
			.unwrap_or_else(|| semantic_name(node.semantic_kind).to_owned());
		dot.push_str(&format!(
			"  n{} [label=\"{}\\n#{} [{}..{})\\n{} · {}\"];\n",
			node.node_id.index() + 1,
			dot_escape(&label),
			node.node_id.index() + 1,
			node.start_byte,
			node.end_byte,
			semantic_name(node.semantic_kind),
			storage_name(node.storage),
		));
		if let Some(parent) = node.parent {
			dot.push_str(&format!(
				"  n{} -> n{};\n",
				parent.index() + 1,
				node.node_id.index() + 1
			));
		}
	}
	dot.push_str("}\n");
	dot
}

fn dot_escape(text: &str) -> String {
	text.replace('\\', "\\\\")
		.replace('"', "\\\"")
		.replace('\n', "\\n")
}

struct RenderedInspector {
	text: Vec<u8>,
	highlights: Vec<HighlightSpan>,
	rows: Vec<UastInspectorRow>,
	graph: Option<UastGraphSnapshot>,
}

fn render_snapshot(
	snapshot: &UastInspectorSnapshot,
	identity: SourceIdentity,
	source_name: &str,
	mode: UastInspectorMode,
	provider: Option<&dyn LanguageProvider>,
) -> RenderedInspector {
	let semantic_current = identity.applied_structural_state_id == Some(identity.state_id);
	let freshness = if semantic_current {
		"current"
	} else if snapshot.total_bytes == 0 || snapshot.total_bytes >= MAX_SEMANTIC_BYTES {
		"unavailable"
	} else if identity.pending_semantic {
		"pending"
	} else if identity.applied_structural_state_id.is_some() {
		"stale"
	} else {
		"raw"
	};
	let reachable = if snapshot.reachable_complete {
		snapshot.reachable_visited.to_string()
	} else {
		format!(">={} (truncated)", snapshot.reachable_visited)
	};
	let reachable_resident = if snapshot.reachable_complete {
		snapshot.reachable_resident_bytes.to_string()
	} else {
		format!(">={}", snapshot.reachable_resident_bytes)
	};
	let mut text = format!(
		"UAST {source_name} · {} · {freshness}\nstate={} applied={} topology={} root=#{}\nreachable={} allocated={} reachable-resident={} B total={} B view={}\n\n",
		mode_name(mode),
		identity.state_id,
		identity
			.applied_structural_state_id
			.map_or_else(|| "none".to_string(), |state| state.to_string()),
		identity.topology_rev,
		snapshot.root.index() + 1,
		reachable,
		snapshot.allocated_nodes,
		reachable_resident,
		snapshot.total_bytes,
		identity.view_id,
	);
	let mut highlights = vec![HighlightSpan::new(
		DocByte::ZERO,
		DocByte::new(4),
		TokenCategory::Keyword,
	)];
	let mut rows = Vec::new();
	let mut effective_depths = HashMap::<NodeId, u16>::new();
	let mut ancestry = HashMap::<NodeId, Vec<ProviderKindId>>::new();
	let mut nearest_visible = HashMap::<NodeId, Option<NodeId>>::new();
	let mut graph_nodes = Vec::new();
	let semantic_has_labels = semantic_current
		&& snapshot
			.nodes
			.iter()
			.any(|node| node.provider_kind != ProviderKindId::NONE);

	for node in &snapshot.nodes {
		let parent_depth = node
			.parent
			.and_then(|parent| effective_depths.get(&parent).copied());
		let display_depth = parent_depth.map_or(0, |depth| depth.saturating_add(1));
		let mut node_ancestry = node
			.parent
			.and_then(|parent| ancestry.get(&parent).cloned())
			.unwrap_or_default();
		if node.provider_kind != ProviderKindId::NONE {
			node_ancestry.push(node.provider_kind);
		}

		let semantic_mode = mode != UastInspectorMode::Raw;
		let graph_parent = node
			.parent
			.and_then(|parent| nearest_visible.get(&parent).copied().flatten());
		let collapse_group = semantic_mode && node.semantic_kind == SemanticKind::Group;
		let hide_token = semantic_mode
			&& semantic_has_labels
			&& node.semantic_kind == SemanticKind::Token
			&& node.child_count == 0;
		if collapse_group {
			effective_depths.insert(
				node.node_id,
				parent_depth.unwrap_or(display_depth.saturating_sub(1)),
			);
			ancestry.insert(node.node_id, node_ancestry);
			nearest_visible.insert(node.node_id, graph_parent);
			continue;
		}
		effective_depths.insert(node.node_id, display_depth);
		ancestry.insert(node.node_id, node_ancestry.clone());
		if hide_token {
			nearest_visible.insert(node.node_id, graph_parent);
			continue;
		}
		nearest_visible.insert(node.node_id, Some(node.node_id));

		let line = DocLine::new(4 + rows.len() as u64);
		let row_start = text.len();
		text.push_str(if node.contains_cursor { "> " } else { "  " });
		text.push_str(&"  ".repeat(display_depth as usize));
		let label_start = text.len();
		let label = provider
			.and_then(|provider| provider.structural_kind_name(node.provider_kind))
			.unwrap_or_else(|| semantic_name(node.semantic_kind).to_owned());
		graph_nodes.push(UastGraphNode {
			id: (node.node_id.index() + 1) as u32,
			parent: graph_parent.map(|parent| (parent.index() + 1) as u32),
			depth: display_depth,
			label: label.clone(),
			semantic_kind: node.semantic_kind,
			storage: node.storage,
			start_byte: node.start_byte,
			end_byte: node.end_byte,
			selected: node.contains_cursor,
		});
		text.push_str(&label);
		let label_end = text.len();
		text.push_str(&format!(
			" · {} · #{} · [{}..{}) · nl={}{} · children={}{} · {}",
			semantic_name(node.semantic_kind),
			node.node_id.index() + 1,
			node.start_byte,
			node.end_byte,
			node.newlines,
			certainty_suffix(node.newline_certainty),
			node.child_count,
			if node.child_count_exact { "" } else { "+" },
			storage_name(node.storage),
		));
		if let Some(snippet) = node.resident_snippet.as_deref()
			&& !snippet.is_empty()
		{
			text.push_str(" · “");
			text.push_str(&printable_snippet(snippet));
			text.push('”');
		}
		text.push('\n');
		highlights.push(HighlightSpan::new(
			DocByte::new(label_start as u64),
			DocByte::new(label_end as u64),
			TokenCategory::Type,
		));
		highlights.push(HighlightSpan::new(
			DocByte::new(row_start as u64),
			DocByte::new((row_start + 1) as u64),
			TokenCategory::Keyword,
		));
		rows.push(UastInspectorRow {
			line,
			node_id: node.node_id,
			start_byte: DocByte::new(node.start_byte),
			end_byte: DocByte::new(node.end_byte),
			provider_kind: node.provider_kind,
			ancestry: node_ancestry,
		});
	}

	RenderedInspector {
		text: text.into_bytes(),
		highlights,
		rows,
		graph: (mode == UastInspectorMode::Graph).then(|| UastGraphSnapshot {
			nodes: scope_graph_nodes(graph_nodes),
			source_truncated: !snapshot.reachable_complete,
		}),
	}
}

fn scope_graph_nodes(nodes: Vec<UastGraphNode>) -> Vec<UastGraphNode> {
	const GRAPH_NODE_BUDGET: usize = 96;
	const SUBTREE_DEPTH: u16 = 4;
	if nodes.len() <= GRAPH_NODE_BUDGET {
		return nodes;
	}
	let by_id = nodes
		.iter()
		.enumerate()
		.map(|(index, node)| (node.id, index))
		.collect::<HashMap<_, _>>();
	let selected = nodes
		.iter()
		.filter(|node| node.selected)
		.max_by_key(|node| node.depth)
		.map(|node| node.id)
		.unwrap_or(nodes[0].id);
	let selected_depth = by_id
		.get(&selected)
		.map_or(0, |index| nodes[*index].depth);
	let mut retained = HashSet::new();
	let mut ancestor = Some(selected);
	while let Some(id) = ancestor {
		if !retained.insert(id) {
			break;
		}
		ancestor = by_id
			.get(&id)
			.and_then(|index| nodes[*index].parent);
	}
	for node in &nodes {
		if retained.len() >= GRAPH_NODE_BUDGET {
			break;
		}
		if node.depth < selected_depth
			|| node.depth > selected_depth.saturating_add(SUBTREE_DEPTH)
		{
			continue;
		}
		let mut candidate = Some(node.id);
		while let Some(id) = candidate {
			if id == selected {
				retained.insert(node.id);
				break;
			}
			candidate = by_id
				.get(&id)
				.and_then(|index| nodes[*index].parent);
		}
	}
	nodes
		.into_iter()
		.filter(|node| retained.contains(&node.id))
		.collect()
}

fn replace_if_changed(
	registry: &UastRegistry,
	session: &mut EditorSession,
	panel_buffer_id: BufferId,
	text: Vec<u8>,
	highlights: Vec<HighlightSpan>,
	rows: Vec<UastInspectorRow>,
) {
	let unchanged = session
		.uast_inspectors
		.get(&panel_buffer_id)
		.is_some_and(|panel| panel.rendered_text == text);
	if unchanged {
		return;
	}
	if session
		.replace_output_panel(registry, panel_buffer_id, &text, highlights)
		.is_ok()
		&& let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id)
	{
		panel.rendered_text = text;
		panel.rows = rows;
	}
}

fn mode_name(mode: UastInspectorMode) -> &'static str {
	match mode {
		UastInspectorMode::Semantic => "semantic",
		UastInspectorMode::Raw => "raw",
		UastInspectorMode::Graph => "graph",
	}
}

fn semantic_name(kind: SemanticKind) -> &'static str {
	match kind {
		SemanticKind::RelationalTable => "table",
		SemanticKind::RelationalRow => "row",
		SemanticKind::Group => "group",
		SemanticKind::Token => "token",
	}
}

fn storage_name(storage: UastStorageKind) -> &'static str {
	match storage {
		UastStorageKind::Branch => "branch",
		UastStorageKind::PhysicalCold => "physical:cold",
		UastStorageKind::PhysicalPartial => "physical:partial",
		UastStorageKind::PhysicalResident => "physical:resident",
		UastStorageKind::Virtual => "virtual",
	}
}

fn certainty_suffix(certainty: UastMetricCertainty) -> &'static str {
	match certainty {
		UastMetricCertainty::Unknown => "?",
		UastMetricCertainty::Partial => "~",
		UastMetricCertainty::Exact => "",
	}
}

fn printable_snippet(bytes: &[u8]) -> String {
	let mut output = String::new();
	for &byte in bytes {
		match byte {
			b'\n' => output.push_str("\\n"),
			b'\r' => output.push_str("\\r"),
			b'\t' => output.push_str("\\t"),
			b'\\' => output.push_str("\\\\"),
			0x20..=0x7e => output.push(char::from(byte)),
			_ => output.push('·'),
		}
	}
	output
}
