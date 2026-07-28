use super::core::MAX_SEMANTIC_BYTES;
use super::language::{LanguageProvider, LanguageRegistry};
use super::protocol::UastInspectorMode;
use super::session::{
	Affinity, EditorSession, StructuralIssue, StructuralIssueKind, UastInspectorNodeKey,
	UastInspectorPanel, UastInspectorRefreshKey, UastInspectorRow, UastInspectorStamp, ViewContent,
};
use crate::core::{BufferId, DocByte, DocLine, ViewId};
use crate::ecs::{NodeId, UastRegistry};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use crate::uast::{
	ProviderKindId, SemanticKind, UastGraphNode, UastGraphSelection, UastGraphSnapshot,
	UastGraphStamp, UastInspectorOptions, UastInspectorSnapshot, UastMetricCertainty,
	UastStorageKind,
};
use std::collections::{HashMap, HashSet};

const INSPECTOR_NODE_BUDGET: usize = 2_048;
const INSPECTOR_DEPTH_BUDGET: u16 = 32;
const INSPECTOR_MIN_NODE_BUDGET: usize = 64;
const INSPECTOR_VIEWPORT_OVERSCAN: usize = 4;
const INSPECTOR_HEADER_LINES: u64 = 5;

#[derive(Debug, Clone, Copy)]
struct SourceIdentity {
	view_id: Option<ViewId>,
	root: NodeId,
	state_id: crate::core::StateId,
	topology_rev: crate::core::TopologyRev,
	applied_structural_state_id: Option<crate::core::StateId>,
	pending_semantic: bool,
	structural_issue: Option<StructuralIssue>,
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
			panel.last_refresh_key = None;
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
	let existing_panel_id = session
		.uast_inspectors
		.iter()
		.find_map(|(panel_buffer_id, panel)| {
			(panel.source_buffer_id == focused_buffer_id).then_some(*panel_buffer_id)
		});
	let (panel_buffer_id, _) = session.ensure_output_panel(registry, &panel_name)?;
	let mode = requested_mode.unwrap_or(UastInspectorMode::Semantic);
	if existing_panel_id == Some(panel_buffer_id)
		&& let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id)
	{
		panel.source_view_id = Some(source_view_id);
		if requested_mode.is_some() {
			panel.mode = mode;
		}
		panel.rendered_text.clear();
		panel.last_refresh_key = None;
		session.ui.status_message = Some(format!(
			"UAST inspector retargeted ({})",
			mode_name(panel.mode)
		));
		return Ok(());
	}
	session.uast_inspectors.insert(
		panel_buffer_id,
		UastInspectorPanel {
			source_buffer_id: focused_buffer_id,
			source_view_id: Some(source_view_id),
			mode,
			source_root: None,
			source_state_id: None,
			source_topology_rev: None,
			rows: Vec::new(),
			folded: HashSet::new(),
			fold_revision: 0,
			follow_cursor: true,
			last_refresh_key: None,
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
			.map(|panel| (panel.source_buffer_id, panel.source_view_id, panel.mode))
		else {
			continue;
		};

		let focused_source_view = session
			.layout
			.find_pane(session.focused_pane_id)
			.map(|pane| pane.active_tab())
			.filter(|view| view.buffer_id == source_buffer_id)
			.map(|view| view.id);
		if let Some(focused) = focused_source_view {
			source_view_id = Some(focused);
		} else if !source_view_id.is_some_and(|view_id| {
			session
				.layout
				.find_view(view_id)
				.is_some_and(|view| view.buffer_id == source_buffer_id)
		}) {
			source_view_id = session.layout.view_id_for_buffer(source_buffer_id);
		}
		if session
			.uast_inspectors
			.get(&panel_buffer_id)
			.is_some_and(|panel| panel.source_view_id != source_view_id)
			&& let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id)
		{
			panel.source_view_id = source_view_id;
			panel.rendered_text.clear();
			panel.last_refresh_key = None;
		}

		let cursor_byte = source_view_id
			.and_then(|view_id| session.layout.find_view(view_id))
			.filter(|view| view.buffer_id == source_buffer_id)
			.and_then(|view| match &view.content {
				ViewContent::Text(text) => Some(text.cursor.primary().byte()),
				_ => None,
			});
		let (viewport_width, viewport_lines, inspector_scroll) = session
			.layout
			.view_id_for_buffer(panel_buffer_id)
			.and_then(|view_id| session.layout.find_view(view_id))
			.map(|view| {
				(
					view.common().viewport_width,
					view.common().viewport_lines,
					view.common().scroll_y,
				)
			})
			.unwrap_or((80, 24, 0));
		let viewport_budget = usize::try_from(inspector_scroll)
			.unwrap_or(usize::MAX)
			.saturating_add(
				usize::try_from(viewport_lines)
					.unwrap_or(usize::MAX)
					.saturating_mul(INSPECTOR_VIEWPORT_OVERSCAN),
			)
			.clamp(INSPECTOR_MIN_NODE_BUDGET, INSPECTOR_NODE_BUDGET);
		let Some((mut identity, source_name)) = session
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
						applied_structural_state_id: document.semantic.applied_structural_state_id,
						pending_semantic: document.semantic.pending_semantic_request_id.is_some(),
						structural_issue: document.semantic.structural_issue,
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
			replace_detached(
				registry,
				session,
				panel_buffer_id,
				b"UAST inspector detached: the source buffer has closed.\n".to_vec(),
			);
			continue;
		};

		let stamp = UastInspectorStamp {
			source_buffer_id,
			root: identity.root,
			state_id: identity.state_id,
			topology_rev: identity.topology_rev,
		};
		if let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id)
			&& (panel
				.source_state_id
				.is_some_and(|state| state != identity.state_id)
				|| panel.source_root.is_some_and(|root| root != identity.root))
		{
			panel.folded.clear();
			panel.fold_revision = panel.fold_revision.wrapping_add(1);
		}
		let folded = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.map(|panel| panel.folded.clone())
			.unwrap_or_default();
		let fold_revision = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.map_or(0, |panel| panel.fold_revision);
		let follow_cursor = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.is_none_or(|panel| panel.follow_cursor);
		if !follow_cursor {
			// Keep a manually selected node in scope without letting movement
			// in the paired source view recenter or invalidate the inspector.
			identity.cursor_byte = selected_inspector_source_byte(session, panel_buffer_id, stamp);
		}
		let refresh_key = UastInspectorRefreshKey {
			source_buffer_id,
			source_view_id: identity.view_id,
			source_name: source_name.clone(),
			root: identity.root,
			state_id: identity.state_id,
			topology_rev: identity.topology_rev,
			applied_structural_state_id: identity.applied_structural_state_id,
			pending_semantic: identity.pending_semantic,
			structural_issue: identity.structural_issue,
			active_language_id: identity.active_language_id,
			cursor_byte: identity.cursor_byte,
			mode,
			fold_revision,
			follow_cursor,
			pane_width: viewport_width,
			pane_lines: viewport_lines,
			pane_scroll: inspector_scroll,
			registry_revision: registry.inspector_revision(),
		};
		if session
			.uast_inspectors
			.get(&panel_buffer_id)
			.is_some_and(|panel| {
				panel.last_refresh_key.as_ref() == Some(&refresh_key)
					&& !panel.rendered_text.is_empty()
			}) {
			continue;
		}
		let snapshot = registry.inspect_uast(
			identity.root,
			UastInspectorOptions {
				max_nodes: viewport_budget,
				max_depth: INSPECTOR_DEPTH_BUDGET,
				cursor_byte: identity.cursor_byte.map(DocByte::get),
				..UastInspectorOptions::default()
			},
		);
		let provider = identity
			.active_language_id
			.and_then(|language_id| lang_registry.get_provider(language_id));
		let rendered = render_snapshot(
			&snapshot,
			identity,
			&source_name,
			mode,
			provider,
			stamp,
			&folded,
			follow_cursor,
			fold_revision,
			(viewport_width, viewport_lines, inspector_scroll),
		);
		let selected = selected_row_after_refresh(
			session,
			panel_buffer_id,
			&rendered.rows,
			stamp,
			follow_cursor,
		);
		replace_if_changed(
			registry,
			session,
			panel_buffer_id,
			rendered.text,
			rendered.highlights,
			rendered.rows,
			rendered.graph,
			stamp,
			selected,
			refresh_key,
		);
	}
}

fn selected_inspector_source_byte(
	session: &EditorSession,
	panel_buffer_id: BufferId,
	stamp: UastInspectorStamp,
) -> Option<DocByte> {
	let selected_line = session
		.layout
		.view_id_for_buffer(panel_buffer_id)
		.and_then(|view_id| session.layout.find_view(view_id))
		.and_then(|view| match &view.content {
			ViewContent::Text(text) => Some(text.cursor.primary().abs_line()),
			_ => None,
		})?;
	session
		.uast_inspectors
		.get(&panel_buffer_id)?
		.rows
		.iter()
		.find(|row| {
			row.line == selected_line
				&& row.stamp.source_buffer_id == stamp.source_buffer_id
				&& row.stamp.root == stamp.root
				&& row.stamp.state_id == stamp.state_id
		})
		.map(|row| row.start_byte)
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
	let (source_view_id, row) = {
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
		(panel.source_view_id, row)
	};
	jump_to_row(registry, session, source_view_id, row)
}

pub(crate) fn toggle_selected_fold(session: &mut EditorSession) -> Result<(), String> {
	let (panel_buffer_id, selected_line) = focused_inspector_line(session)?;
	let row = session
		.uast_inspectors
		.get(&panel_buffer_id)
		.and_then(|panel| panel.rows.iter().find(|row| row.line == selected_line))
		.cloned()
		.ok_or_else(|| "Select a UAST node row before folding".to_string())?;
	if row.child_count == 0 {
		return Err("The selected UAST node has no children to fold".to_string());
	}
	let key = row.key();
	let panel = session
		.uast_inspectors
		.get_mut(&panel_buffer_id)
		.ok_or_else(|| "The focused pane is not a UAST inspector".to_string())?;
	let folded = if panel.folded.remove(&key) {
		false
	} else {
		panel.folded.insert(key);
		true
	};
	panel.fold_revision = panel.fold_revision.wrapping_add(1);
	panel.rendered_text.clear();
	panel.last_refresh_key = None;
	session.ui.status_message = Some(if folded {
		"UAST node folded".to_string()
	} else {
		"UAST node expanded".to_string()
	});
	Ok(())
}

pub(crate) fn toggle_follow_cursor(session: &mut EditorSession) -> Result<(), String> {
	let (panel_buffer_id, _) = focused_inspector_line(session)?;
	let panel = session
		.uast_inspectors
		.get_mut(&panel_buffer_id)
		.ok_or_else(|| "The focused pane is not a UAST inspector".to_string())?;
	panel.follow_cursor = !panel.follow_cursor;
	panel.rendered_text.clear();
	panel.last_refresh_key = None;
	session.ui.status_message = Some(format!(
		"UAST cursor follow {}",
		if panel.follow_cursor { "on" } else { "off" }
	));
	Ok(())
}

fn focused_inspector_line(session: &EditorSession) -> Result<(BufferId, DocLine), String> {
	let view = session
		.layout
		.find_pane(session.focused_pane_id)
		.ok_or_else(|| "Focused pane is no longer available".to_string())?
		.active_tab();
	let line = match &view.content {
		ViewContent::Text(text) => text.cursor.primary().abs_line(),
		_ => return Err("The focused pane is not a UAST inspector".to_string()),
	};
	if !session.uast_inspectors.contains_key(&view.buffer_id) {
		return Err("The focused pane is not a UAST inspector".to_string());
	}
	Ok((view.buffer_id, line))
}

pub(crate) fn activate_graph_node(
	registry: &UastRegistry,
	session: &mut EditorSession,
	inspector_view_id: ViewId,
	selection: UastGraphSelection,
) -> Result<(), String> {
	let panel_buffer_id = session
		.layout
		.find_view(inspector_view_id)
		.map(|view| view.buffer_id)
		.ok_or_else(|| "The UAST graph view has closed".to_string())?;
	let (source_view_id, row) = {
		let panel = session
			.uast_inspectors
			.get(&panel_buffer_id)
			.ok_or_else(|| "The graph no longer belongs to a UAST inspector".to_string())?;
		let expected_root = usize::try_from(selection.stamp.root_id)
			.ok()
			.and_then(|id| id.checked_sub(1))
			.map(NodeId::from_index)
			.ok_or_else(|| "The graph carries an invalid root identity".to_string())?;
		let selected_node = usize::try_from(selection.node_id)
			.ok()
			.and_then(|id| id.checked_sub(1))
			.map(NodeId::from_index)
			.ok_or_else(|| "The graph carries an invalid node identity".to_string())?;
		if panel.source_buffer_id != selection.stamp.source_buffer_id {
			return Err("The graph no longer belongs to this source buffer".to_string());
		}
		let selection_stamp = UastInspectorStamp {
			source_buffer_id: selection.stamp.source_buffer_id,
			root: expected_root,
			state_id: selection.stamp.state_id,
			topology_rev: selection.stamp.topology_rev,
		};
		let row = if panel.source_topology_rev == Some(selection.stamp.topology_rev) {
			panel
				.rows
				.iter()
				.find(|row| row.node_id == selected_node)
				.cloned()
		} else {
			panel
				.rows
				.iter()
				.filter(|row| {
					row.start_byte.get() == selection.start_byte
						&& row.end_byte.get() == selection.end_byte
						&& row.provider_kind == selection.provider_kind
						&& row.ancestry == selection.ancestry
				})
				.max_by_key(|row| row.depth)
				.cloned()
		}
		.unwrap_or(UastInspectorRow {
			line: DocLine::ZERO,
			line_start_byte: DocByte::ZERO,
			node_id: selected_node,
			start_byte: DocByte::new(selection.start_byte),
			end_byte: DocByte::new(selection.end_byte),
			provider_kind: selection.provider_kind,
			ancestry: selection.ancestry,
			depth: 0,
			child_count: 0,
			contains_cursor: false,
			stamp: selection_stamp,
		});
		(panel.source_view_id, row)
	};
	jump_to_row(registry, session, source_view_id, row)
}

fn jump_to_row(
	registry: &UastRegistry,
	session: &mut EditorSession,
	mut source_view_id: Option<ViewId>,
	row: UastInspectorRow,
) -> Result<(), String> {
	let source_buffer_id = row.stamp.source_buffer_id;
	let (current_state, current_root) = session
		.buffers
		.get(source_buffer_id)
		.and_then(|buffer| buffer.text())
		.map(|document| (document.ledger.current_state_id, document.root_id))
		.ok_or_else(|| "The inspected source buffer has closed".to_string())?;
	if row.stamp.state_id != current_state || row.stamp.root != current_root {
		return Err("The source changed; the stale inspector row was not followed".to_string());
	}
	if !source_view_id.is_some_and(|view_id| {
		session
			.layout
			.find_view(view_id)
			.is_some_and(|view| view.buffer_id == source_buffer_id)
	}) {
		source_view_id = session.layout.view_id_for_buffer(source_buffer_id);
	}
	let source_view_id =
		source_view_id.ok_or_else(|| "The source has no open view to jump to".to_string())?;
	// A same-content graft may replace every descendant NodeId, but its source
	// ranges remain exact. TopologyRev therefore gates node identity only; this
	// source-range jump is valid whenever StateId and root still match.
	let source_len = registry.get_total_bytes(current_root);
	if row.start_byte > row.end_byte || row.end_byte.get() > source_len {
		return Err(
			"The inspector row range is no longer valid; refresh before jumping".to_string(),
		);
	}
	let target = row.start_byte;
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
	stamp: UastInspectorStamp,
	folded: &HashSet<UastInspectorNodeKey>,
	follow_cursor: bool,
	fold_revision: u64,
	pane_geometry: (u16, u32, u64),
) -> RenderedInspector {
	let semantic_current = identity.applied_structural_state_id == Some(identity.state_id);
	let issue = identity
		.structural_issue
		.filter(|issue| issue.state_id == identity.state_id);
	let semantic_status = if identity.pending_semantic {
		"reparse pending".to_string()
	} else if issue.is_some_and(|issue| issue.kind == StructuralIssueKind::Rejected) {
		"graft rejected".to_string()
	} else if issue.is_some_and(|issue| issue.kind == StructuralIssueKind::Unavailable) {
		"provider topology unavailable".to_string()
	} else if semantic_current {
		"current".to_string()
	} else if snapshot.total_bytes >= MAX_SEMANTIC_BYTES {
		format!("unavailable (>={} MiB)", MAX_SEMANTIC_BYTES / (1024 * 1024))
	} else if identity.active_language_id.is_none() {
		"unavailable (no provider)".to_string()
	} else if identity.applied_structural_state_id.is_some() {
		"edited topology · last-graft labels".to_string()
	} else if snapshot.total_bytes == 0 {
		"flat empty".to_string()
	} else {
		"flat · never grafted".to_string()
	};
	let topology_regime = if semantic_current {
		"semantic graft"
	} else if identity.applied_structural_state_id.is_some() {
		"live edited registry"
	} else if snapshot.nodes.iter().any(|node| {
		matches!(
			node.storage,
			UastStorageKind::PhysicalCold
				| UastStorageKind::PhysicalPartial
				| UastStorageKind::PhysicalResident
		)
	}) {
		"sparse physical"
	} else {
		"flat storage"
	};
	let scope = if snapshot.cursor_scoped {
		"cursor"
	} else {
		"root"
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
	let (pane_width, pane_lines, pane_scroll) = pane_geometry;
	let mut text = format!(
		"UAST {source_name} · {}\ntopology: {topology_regime} · semantic: {semantic_status} · scope: {scope} · follow: {}\nstate={} applied={} topology-rev={} root=#{} view={}\nreachable={} allocated={} reachable-resident={} B total={} B pane={}x{} scroll={} fold-rev={}\nEnter jump · Space fold · f follow\n",
		mode_name(mode),
		if follow_cursor { "on" } else { "off" },
		identity.state_id,
		identity
			.applied_structural_state_id
			.map_or_else(|| "none".to_string(), |state| state.to_string()),
		identity.topology_rev,
		snapshot.root.index() + 1,
		identity
			.view_id
			.map_or_else(|| "none".to_string(), |view| view.to_string()),
		reachable,
		snapshot.allocated_nodes,
		reachable_resident,
		snapshot.total_bytes,
		pane_width,
		pane_lines,
		pane_scroll,
		fold_revision,
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
	let mut folded_depth = None::<u16>;
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
		if let Some(depth) = folded_depth {
			if node.depth > depth {
				effective_depths.insert(node.node_id, display_depth);
				ancestry.insert(node.node_id, node_ancestry);
				nearest_visible.insert(node.node_id, None);
				continue;
			}
			folded_depth = None;
		}

		let semantic_mode = mode != UastInspectorMode::Raw;
		let graph_parent = node
			.parent
			.and_then(|parent| nearest_visible.get(&parent).copied().flatten());
		let collapse_group = semantic_mode && node.semantic_kind == SemanticKind::Group;
		let hide_trivia = semantic_mode
			&& node.child_count == 0
			&& provider
				.is_some_and(|provider| provider.structural_kind_is_trivia(node.provider_kind));
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
		if hide_trivia {
			nearest_visible.insert(node.node_id, graph_parent);
			continue;
		}
		nearest_visible.insert(node.node_id, Some(node.node_id));

		let key = UastInspectorNodeKey {
			start_byte: node.start_byte,
			end_byte: node.end_byte,
			provider_kind: node.provider_kind,
			ancestry: node_ancestry.clone(),
		};
		let is_folded = node.child_count > 0 && folded.contains(&key);
		let line = DocLine::new(INSPECTOR_HEADER_LINES + rows.len() as u64);
		let row_start = text.len();
		text.push_str(if node.contains_cursor { "> " } else { "  " });
		text.push_str(if node.child_count == 0 {
			"  "
		} else if is_folded {
			"▸ "
		} else {
			"▾ "
		});
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
			provider_kind: node.provider_kind,
			ancestry: node_ancestry.clone(),
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
			line_start_byte: DocByte::new(row_start as u64),
			node_id: node.node_id,
			start_byte: DocByte::new(node.start_byte),
			end_byte: DocByte::new(node.end_byte),
			provider_kind: node.provider_kind,
			ancestry: node_ancestry,
			depth: display_depth,
			child_count: node.child_count,
			contains_cursor: node.contains_cursor,
			stamp,
		});
		if is_folded {
			folded_depth = Some(node.depth);
		}
	}

	RenderedInspector {
		text: text.into_bytes(),
		highlights,
		rows,
		graph: (mode == UastInspectorMode::Graph).then(|| UastGraphSnapshot {
			stamp: UastGraphStamp {
				source_buffer_id: stamp.source_buffer_id,
				root_id: (stamp.root.index() + 1) as u32,
				state_id: stamp.state_id,
				topology_rev: stamp.topology_rev,
			},
			nodes: scope_graph_nodes(graph_nodes),
			source_truncated: !snapshot.reachable_complete,
		}),
	}
}

fn scope_graph_nodes(nodes: Vec<UastGraphNode>) -> Vec<UastGraphNode> {
	const GRAPH_NODE_BUDGET: usize = 96;
	const SUBTREE_DEPTH: u16 = 4;
	if nodes.is_empty() {
		return nodes;
	}
	let by_id = nodes
		.iter()
		.enumerate()
		.map(|(index, node)| (node.id, index))
		.collect::<HashMap<_, _>>();
	let branch_ids = nodes
		.iter()
		.filter_map(|node| node.parent)
		.collect::<HashSet<_>>();
	let selected = nodes
		.iter()
		.filter(|node| node.selected && branch_ids.contains(&node.id))
		.max_by_key(|node| node.depth)
		.map(|node| node.id)
		.or_else(|| {
			nodes
				.iter()
				.filter(|node| node.selected)
				.max_by_key(|node| node.depth)
				.map(|node| node.id)
		})
		.unwrap_or(nodes[0].id);
	let selected_depth = by_id.get(&selected).map_or(0, |index| nodes[*index].depth);
	let mut retained = HashSet::new();
	let mut ancestor = Some(selected);
	while let Some(id) = ancestor {
		if !retained.insert(id) {
			break;
		}
		ancestor = by_id.get(&id).and_then(|index| nodes[*index].parent);
	}
	for node in &nodes {
		if retained.len() >= GRAPH_NODE_BUDGET {
			break;
		}
		if node.depth < selected_depth || node.depth > selected_depth.saturating_add(SUBTREE_DEPTH)
		{
			continue;
		}
		let mut candidate = Some(node.id);
		while let Some(id) = candidate {
			if id == selected {
				retained.insert(node.id);
				break;
			}
			candidate = by_id.get(&id).and_then(|index| nodes[*index].parent);
		}
	}
	nodes
		.into_iter()
		.filter(|node| retained.contains(&node.id))
		.collect()
}

fn selected_row_after_refresh(
	session: &EditorSession,
	panel_buffer_id: BufferId,
	rows: &[UastInspectorRow],
	stamp: UastInspectorStamp,
	follow_cursor: bool,
) -> Option<UastInspectorRow> {
	let cursor_row = || {
		rows.iter()
			.filter(|row| row.contains_cursor)
			.max_by_key(|row| row.depth)
			.cloned()
	};
	if follow_cursor {
		return cursor_row().or_else(|| rows.first().cloned());
	}

	let selected_line = session
		.layout
		.view_id_for_buffer(panel_buffer_id)
		.and_then(|view_id| session.layout.find_view(view_id))
		.and_then(|view| match &view.content {
			ViewContent::Text(text) => Some(text.cursor.primary().abs_line()),
			_ => None,
		});
	let old_row = selected_line.and_then(|line| {
		session
			.uast_inspectors
			.get(&panel_buffer_id)
			.and_then(|panel| panel.rows.iter().find(|row| row.line == line))
	});
	remap_selected_row(old_row, rows, stamp)
		.or_else(cursor_row)
		.or_else(|| rows.first().cloned())
}

fn remap_selected_row(
	old_row: Option<&UastInspectorRow>,
	rows: &[UastInspectorRow],
	stamp: UastInspectorStamp,
) -> Option<UastInspectorRow> {
	old_row.and_then(|old| {
		if old.stamp.source_buffer_id != stamp.source_buffer_id
			|| old.stamp.root != stamp.root
			|| old.stamp.state_id != stamp.state_id
		{
			return None;
		}
		if old.stamp.topology_rev == stamp.topology_rev {
			return rows.iter().find(|row| row.node_id == old.node_id).cloned();
		}

		// Same bytes, new graft generation: range alone is ambiguous because
		// nested nodes often share extents. Match provider identity and its
		// ancestry, then deliberately choose the deepest remaining candidate.
		rows.iter()
			.filter(|row| {
				row.start_byte == old.start_byte
					&& row.end_byte == old.end_byte
					&& row.provider_kind == old.provider_kind
					&& row.ancestry == old.ancestry
			})
			.max_by_key(|row| row.depth)
			.cloned()
	})
}

fn replace_detached(
	registry: &UastRegistry,
	session: &mut EditorSession,
	panel_buffer_id: BufferId,
	text: Vec<u8>,
) {
	let unchanged = session
		.uast_inspectors
		.get(&panel_buffer_id)
		.is_some_and(|panel| panel.rendered_text == text);
	if !unchanged
		&& session
			.replace_output_panel(registry, panel_buffer_id, &text, Vec::new())
			.is_err()
	{
		return;
	}
	if let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id) {
		panel.source_view_id = None;
		panel.source_root = None;
		panel.source_state_id = None;
		panel.source_topology_rev = None;
		panel.rows.clear();
		panel.graph = None;
		panel.last_refresh_key = None;
		panel.rendered_text = text;
	}
}

fn replace_if_changed(
	registry: &UastRegistry,
	session: &mut EditorSession,
	panel_buffer_id: BufferId,
	text: Vec<u8>,
	highlights: Vec<HighlightSpan>,
	rows: Vec<UastInspectorRow>,
	graph: Option<UastGraphSnapshot>,
	stamp: UastInspectorStamp,
	selected: Option<UastInspectorRow>,
	refresh_key: UastInspectorRefreshKey,
) {
	let unchanged = session
		.uast_inspectors
		.get(&panel_buffer_id)
		.is_some_and(|panel| panel.rendered_text == text);
	if !unchanged
		&& session
			.replace_output_panel(registry, panel_buffer_id, &text, highlights)
			.is_err()
	{
		return;
	}
	if let Some(panel) = session.uast_inspectors.get_mut(&panel_buffer_id) {
		panel.source_root = Some(stamp.root);
		panel.source_state_id = Some(stamp.state_id);
		panel.source_topology_rev = Some(stamp.topology_rev);
		panel.graph = graph;
		panel.last_refresh_key = Some(refresh_key);
		panel.rendered_text = text;
		panel.rows = rows;
	}
	if let Some(selected) = selected {
		let mut view_ids = Vec::new();
		session
			.layout
			.collect_view_ids_for_buffer(panel_buffer_id, &mut view_ids, usize::MAX);
		for view_id in view_ids {
			let Some(view) = session.layout.find_view_mut(view_id) else {
				continue;
			};
			if let ViewContent::Text(text) = &mut view.content {
				text.cursor
					.primary_mut()
					.set_byte(selected.line_start_byte, Affinity::After);
			}
		}
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::{StateId, TopologyRev};
	use crate::uast::UastInspectorNode;

	fn stamp(root: NodeId, state: StateId, topology_rev: TopologyRev) -> UastInspectorStamp {
		UastInspectorStamp {
			source_buffer_id: BufferId::new(1),
			root,
			state_id: state,
			topology_rev,
		}
	}

	fn row(
		node_id: NodeId,
		depth: u16,
		start_byte: u64,
		end_byte: u64,
		provider_kind: ProviderKindId,
		ancestry: Vec<ProviderKindId>,
		stamp: UastInspectorStamp,
	) -> UastInspectorRow {
		UastInspectorRow {
			line: DocLine::new(u64::from(depth)),
			line_start_byte: DocByte::new(u64::from(depth)),
			node_id,
			start_byte: DocByte::new(start_byte),
			end_byte: DocByte::new(end_byte),
			provider_kind,
			ancestry,
			depth,
			child_count: 1,
			contains_cursor: false,
			stamp,
		}
	}

	#[test]
	fn selection_remap_uses_full_identity_and_prefers_the_deepest_match() {
		let root = NodeId::from_index(0);
		let old_stamp = stamp(root, StateId::ZERO, TopologyRev::ZERO);
		let new_stamp = stamp(root, StateId::ZERO, TopologyRev::new(1));
		let kind = ProviderKindId::new(7);
		let ancestry = vec![ProviderKindId::new(1), kind];
		let old = row(
			NodeId::from_index(1),
			1,
			4,
			12,
			kind,
			ancestry.clone(),
			old_stamp,
		);
		let shallow = row(
			NodeId::from_index(2),
			1,
			4,
			12,
			kind,
			ancestry.clone(),
			new_stamp,
		);
		let deep = row(NodeId::from_index(3), 3, 4, 12, kind, ancestry, new_stamp);

		let remapped = remap_selected_row(Some(&old), &[shallow, deep.clone()], new_stamp)
			.expect("same-content graft remaps");
		assert_eq!(remapped.node_id, deep.node_id);

		let changed_state = stamp(root, StateId::new(1), TopologyRev::new(2));
		assert!(remap_selected_row(Some(&old), &[deep], changed_state).is_none());
	}

	fn snapshot_node(
		node_id: NodeId,
		parent: Option<NodeId>,
		depth: u16,
		start_byte: u64,
		end_byte: u64,
		kind: SemanticKind,
		provider_kind: ProviderKindId,
		child_count: usize,
	) -> UastInspectorNode {
		UastInspectorNode {
			node_id,
			parent,
			depth,
			start_byte,
			end_byte,
			semantic_kind: kind,
			provider_kind,
			child_count,
			child_count_exact: true,
			newlines: 0,
			newline_certainty: UastMetricCertainty::Exact,
			storage: if child_count > 0 {
				UastStorageKind::Branch
			} else {
				UastStorageKind::Virtual
			},
			resident_bytes: 0,
			resident_snippet: None,
			contains_cursor: false,
		}
	}

	#[test]
	fn folded_nodes_hide_descendants_and_rejected_grafts_are_explicit() {
		let root = NodeId::from_index(0);
		let branch = NodeId::from_index(1);
		let leaf = NodeId::from_index(2);
		let sibling = NodeId::from_index(3);
		let root_kind = ProviderKindId::new(1);
		let branch_kind = ProviderKindId::new(2);
		let state = StateId::ZERO;
		let snapshot = UastInspectorSnapshot {
			root,
			total_bytes: 20,
			allocated_nodes: 4,
			reachable_visited: 4,
			reachable_complete: true,
			reachable_resident_bytes: 0,
			cursor_scoped: false,
			nodes: vec![
				snapshot_node(
					root,
					None,
					0,
					0,
					20,
					SemanticKind::RelationalTable,
					root_kind,
					2,
				),
				snapshot_node(
					branch,
					Some(root),
					1,
					0,
					10,
					SemanticKind::RelationalTable,
					branch_kind,
					1,
				),
				snapshot_node(
					leaf,
					Some(branch),
					2,
					0,
					10,
					SemanticKind::Token,
					ProviderKindId::new(3),
					0,
				),
				snapshot_node(
					sibling,
					Some(root),
					1,
					10,
					20,
					SemanticKind::RelationalRow,
					ProviderKindId::new(4),
					0,
				),
			],
		};
		let mut folded = HashSet::new();
		folded.insert(UastInspectorNodeKey {
			start_byte: 0,
			end_byte: 10,
			provider_kind: branch_kind,
			ancestry: vec![root_kind, branch_kind],
		});
		let identity = SourceIdentity {
			view_id: None,
			root,
			state_id: state,
			topology_rev: TopologyRev::ZERO,
			applied_structural_state_id: None,
			pending_semantic: false,
			structural_issue: Some(StructuralIssue {
				state_id: state,
				kind: StructuralIssueKind::Rejected,
			}),
			active_language_id: None,
			cursor_byte: None,
		};
		let rendered = render_snapshot(
			&snapshot,
			identity,
			"broken.rs",
			UastInspectorMode::Raw,
			None,
			stamp(root, state, TopologyRev::ZERO),
			&folded,
			false,
			1,
			(80, 24, 0),
		);

		assert_eq!(
			rendered
				.rows
				.iter()
				.map(|row| row.node_id)
				.collect::<Vec<_>>(),
			vec![root, branch, sibling]
		);
		let text = String::from_utf8(rendered.text).expect("UTF-8 inspector");
		assert!(text.contains("semantic: graft rejected"));
		assert!(text.contains("▸ "));
	}

	#[test]
	fn graph_scope_keeps_the_cursor_branch_and_drops_unrelated_siblings() {
		let nodes = vec![
			UastGraphNode {
				id: 1,
				parent: None,
				depth: 0,
				label: "root".to_string(),
				semantic_kind: SemanticKind::RelationalTable,
				storage: UastStorageKind::Branch,
				start_byte: 0,
				end_byte: 20,
				provider_kind: ProviderKindId::new(1),
				ancestry: vec![ProviderKindId::new(1)],
				selected: true,
			},
			UastGraphNode {
				id: 2,
				parent: Some(1),
				depth: 1,
				label: "branch".to_string(),
				semantic_kind: SemanticKind::RelationalTable,
				storage: UastStorageKind::Branch,
				start_byte: 0,
				end_byte: 10,
				provider_kind: ProviderKindId::new(2),
				ancestry: vec![ProviderKindId::new(1), ProviderKindId::new(2)],
				selected: true,
			},
			UastGraphNode {
				id: 3,
				parent: Some(2),
				depth: 2,
				label: "leaf".to_string(),
				semantic_kind: SemanticKind::Token,
				storage: UastStorageKind::Virtual,
				start_byte: 0,
				end_byte: 10,
				provider_kind: ProviderKindId::new(3),
				ancestry: vec![
					ProviderKindId::new(1),
					ProviderKindId::new(2),
					ProviderKindId::new(3),
				],
				selected: true,
			},
			UastGraphNode {
				id: 4,
				parent: Some(1),
				depth: 1,
				label: "unrelated".to_string(),
				semantic_kind: SemanticKind::RelationalRow,
				storage: UastStorageKind::Virtual,
				start_byte: 10,
				end_byte: 20,
				provider_kind: ProviderKindId::new(4),
				ancestry: vec![ProviderKindId::new(1), ProviderKindId::new(4)],
				selected: false,
			},
		];
		assert_eq!(
			scope_graph_nodes(nodes)
				.into_iter()
				.map(|node| node.id)
				.collect::<Vec<_>>(),
			vec![1, 2, 3]
		);
	}
}
