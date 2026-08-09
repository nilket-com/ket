//! Strict, head-neutral workspace layout checkpoints.
//!
//! Runtime pane/view/buffer identities and rendered rectangles are deliberately
//! absent. The codec names ordinary documents by `DocumentId` and host-owned
//! panels/groups by their frontend-authored opaque identities.

use super::session::{
	Buffer, BufferContent, EditorPane, EditorSession, HostPanelBuffer, HostPanelGroupLayoutState,
	HostPanelGroupState, LayoutLeaf, LayoutNode, SavedDocumentState, View,
};
use super::{
	HostPanelDescriptor, HostPanelGroupId, HostPanelGroupMember, HostPanelGroupShape, HostPanelId,
	PanelRevision, SplitDirection,
};
use crate::core::{BufferId, DEFAULT_VIEWPORT_LINES, DEFAULT_VIEWPORT_WIDTH, DocumentId, PaneId};
use crate::ecs::UastRegistry;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(crate) const LAYOUT_OWNER_ID: &str = "libaryon_layout";
pub(crate) const LAYOUT_OWNER_SCHEMA_VERSION: u32 = 1;
pub(crate) const LAYOUT_OWNER_BUDGET: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LayoutCheckpoint {
	pub(crate) root: Option<LayoutCheckpointNode>,
	pub(crate) focused_leaf: Option<LeafPath>,
	pub(crate) panels: Vec<HostPanelCheckpoint>,
	pub(crate) groups: Vec<HostPanelGroupCheckpoint>,
}

impl LayoutCheckpoint {
	pub(crate) fn panel_ids(&self) -> BTreeSet<HostPanelId> {
		self.panels
			.iter()
			.map(|panel| HostPanelId::new(panel.panel_id))
			.collect()
	}

	pub(crate) fn group_ids(&self) -> BTreeSet<HostPanelGroupId> {
		self.groups
			.iter()
			.map(|group| HostPanelGroupId::new(group.group_id))
			.collect()
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LayoutCheckpointNode {
	Split {
		group_root: Option<u64>,
		direction: CheckpointDirection,
		children: Vec<LayoutCheckpointNode>,
		ratios: Vec<u32>,
	},
	Leaf {
		group_root: Option<u64>,
		tabs: Vec<DurableTab>,
		active_tab: usize,
	},
}

impl LayoutCheckpointNode {
	fn group_root(&self) -> Option<u64> {
		match self {
			Self::Split { group_root, .. } | Self::Leaf { group_root, .. } => *group_root,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointDirection {
	Horizontal,
	Vertical,
}

impl From<SplitDirection> for CheckpointDirection {
	fn from(direction: SplitDirection) -> Self {
		match direction {
			SplitDirection::Horizontal => Self::Horizontal,
			SplitDirection::Vertical => Self::Vertical,
		}
	}
}

impl From<CheckpointDirection> for SplitDirection {
	fn from(direction: CheckpointDirection) -> Self {
		match direction {
			CheckpointDirection::Horizontal => Self::Horizontal,
			CheckpointDirection::Vertical => Self::Vertical,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DurableTab {
	Document { document_id: u64 },
	HostPanel { panel_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostPanelCheckpoint {
	pub(crate) panel_id: u64,
	pub(crate) kind: String,
	pub(crate) title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostPanelGroupCheckpoint {
	pub(crate) group_id: u64,
	pub(crate) layout_state: CheckpointGroupLayoutState,
	pub(crate) current_members: Vec<HostPanelGroupMemberCheckpoint>,
	pub(crate) retained_members: Vec<HostPanelGroupMemberCheckpoint>,
	pub(crate) detached_members: Vec<u64>,
	pub(crate) latest_anchor: Option<LeafPath>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointGroupLayoutState {
	Automatic,
	Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostPanelGroupMemberCheckpoint {
	pub(crate) panel_id: u64,
	pub(crate) shape: CheckpointGroupShape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CheckpointGroupShape {
	Compact,
	Expansive,
}

impl From<HostPanelGroupShape> for CheckpointGroupShape {
	fn from(shape: HostPanelGroupShape) -> Self {
		match shape {
			HostPanelGroupShape::Compact => Self::Compact,
			HostPanelGroupShape::Expansive => Self::Expansive,
		}
	}
}

impl From<CheckpointGroupShape> for HostPanelGroupShape {
	fn from(shape: CheckpointGroupShape) -> Self {
		match shape {
			CheckpointGroupShape::Compact => Self::Compact,
			CheckpointGroupShape::Expansive => Self::Expansive,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct LeafPath(pub(crate) Vec<u32>);

enum CapturedNode {
	Split {
		group_root: Option<u64>,
		direction: CheckpointDirection,
		children: Vec<CapturedNode>,
		ratios: Vec<u32>,
	},
	Leaf {
		group_root: Option<u64>,
		source_pane: PaneId,
		tabs: Vec<DurableTab>,
		active_tab: usize,
	},
}

impl CapturedNode {
	fn set_group_root(&mut self, group: Option<u64>) -> Result<(), String> {
		let slot = match self {
			Self::Split { group_root, .. } | Self::Leaf { group_root, .. } => group_root,
		};
		if slot.is_some() && *slot != group {
			return Err("canonicalization would merge two host-panel group roots".to_owned());
		}
		*slot = group;
		Ok(())
	}

	fn collect_paths(&self, path: &mut Vec<u32>, out: &mut HashMap<PaneId, LeafPath>) {
		match self {
			Self::Leaf { source_pane, .. } => {
				out.insert(*source_pane, LeafPath(path.clone()));
			}
			Self::Split { children, .. } => {
				for (index, child) in children.iter().enumerate() {
					path.push(index as u32);
					child.collect_paths(path, out);
					path.pop();
				}
			}
		}
	}

	fn into_checkpoint(self) -> LayoutCheckpointNode {
		match self {
			Self::Split {
				group_root,
				direction,
				children,
				ratios,
			} => LayoutCheckpointNode::Split {
				group_root,
				direction,
				children: children
					.into_iter()
					.map(CapturedNode::into_checkpoint)
					.collect(),
				ratios,
			},
			Self::Leaf {
				group_root,
				tabs,
				active_tab,
				..
			} => LayoutCheckpointNode::Leaf {
				group_root,
				tabs,
				active_tab,
			},
		}
	}
}

pub(crate) fn capture_layout(session: &EditorSession) -> Result<LayoutCheckpoint, String> {
	let saved = session
		.saved_documents
		.as_ref()
		.ok_or_else(|| "engine is not bound to a saved session".to_owned())?;
	let mut panels = BTreeMap::<u64, HostPanelCheckpoint>::new();
	let root = capture_node(&session.layout, &session.buffers, saved, &mut panels)?;

	let mut pane_paths = HashMap::new();
	if let Some(root) = &root {
		root.collect_paths(&mut Vec::new(), &mut pane_paths);
	}
	let focused_leaf = pane_paths
		.get(&session.focused_pane_id)
		.cloned()
		.or_else(|| {
			let mut paths = pane_paths.values().cloned().collect::<Vec<_>>();
			paths.sort();
			paths.into_iter().next()
		});
	let panel_ids = panels
		.keys()
		.copied()
		.map(HostPanelId::new)
		.collect::<BTreeSet<_>>();
	let mut groups = Vec::new();
	for (group_id, group) in &session.host_panel_groups {
		let current_members = group
			.current_members
			.iter()
			.filter(|member| panel_ids.contains(&member.panel_id))
			.map(|member| HostPanelGroupMemberCheckpoint {
				panel_id: member.panel_id.get(),
				shape: member.shape.into(),
			})
			.collect::<Vec<_>>();
		let mut retained_members = group
			.retained_members
			.values()
			.filter(|member| panel_ids.contains(&member.panel_id))
			.map(|member| HostPanelGroupMemberCheckpoint {
				panel_id: member.panel_id.get(),
				shape: member.shape.into(),
			})
			.collect::<Vec<_>>();
		retained_members.sort_by_key(|member| member.panel_id);
		let mut detached_members = group
			.detached_members
			.iter()
			.filter(|panel_id| panel_ids.contains(panel_id))
			.map(|panel_id| panel_id.get())
			.collect::<Vec<_>>();
		detached_members.sort_unstable();
		// Preserve an empty, still-live group identity. Polariton's semantic
		// producer map is append-only under PRODUCT-0061; omitting the engine's
		// corresponding group merely because its current view set is empty would
		// make the exact group census spuriously stale.
		groups.push(HostPanelGroupCheckpoint {
			group_id: group_id.get(),
			layout_state: match group.layout_state {
				HostPanelGroupLayoutState::Automatic => CheckpointGroupLayoutState::Automatic,
				HostPanelGroupLayoutState::Manual => CheckpointGroupLayoutState::Manual,
			},
			current_members,
			retained_members,
			detached_members,
			latest_anchor: group
				.latest_valid_anchor
				.and_then(|pane_id| pane_paths.get(&pane_id).cloned()),
		});
	}
	groups.sort_by_key(|group| group.group_id);

	let checkpoint = LayoutCheckpoint {
		root: root.map(CapturedNode::into_checkpoint),
		focused_leaf,
		panels: panels.into_values().collect(),
		groups,
	};
	validate_layout(&checkpoint, &saved_document_ids(saved))?;
	Ok(checkpoint)
}

/// Replace PRODUCT-0060's one-pane fallback with a validated logical layout.
///
/// Runtime identities are allocated here rather than decoded. Ordinary
/// document buffers already exist and are joined through `DocumentId`; each
/// host panel gets one new backing buffer shared by all of its restored views.
pub(crate) fn restore_layout(
	registry: &UastRegistry,
	session: &mut EditorSession,
	checkpoint: LayoutCheckpoint,
) -> Result<bool, String> {
	let Some(root) = checkpoint.root else {
		return Ok(false);
	};
	let focused_path = checkpoint
		.focused_leaf
		.clone()
		.expect("validated nonempty layout has a focused leaf");
	let descriptors = checkpoint
		.panels
		.iter()
		.map(|panel| {
			(
				HostPanelId::new(panel.panel_id),
				HostPanelDescriptor {
					kind: panel.kind.clone(),
					title: panel.title.clone(),
				},
			)
		})
		.collect::<BTreeMap<_, _>>();
	let mut panel_buffers = BTreeMap::<HostPanelId, BufferId>::new();
	for (panel_id, descriptor) in &descriptors {
		let buffer_id = session
			.buffers
			.insert(BufferContent::HostPanel(HostPanelBuffer {
				panel_id: *panel_id,
				descriptor: descriptor.clone(),
				revision: PanelRevision::new(0),
			}));
		panel_buffers.insert(*panel_id, buffer_id);
		session.queue_host_event(crate::engine::HostEvent::PanelRestored {
			panel_id: *panel_id,
			descriptor: descriptor.clone(),
		});
	}

	let mut restored_paths = BTreeMap::<LeafPath, PaneId>::new();
	let restored_root = restore_node(
		registry,
		session,
		&root,
		&panel_buffers,
		&mut Vec::new(),
		&mut restored_paths,
	)?;
	let focused_pane_id = restored_paths
		.get(&focused_path)
		.copied()
		.ok_or_else(|| "focused leaf disappeared while restoring layout".to_owned())?;
	session.layout = restored_root;
	session.focused_pane_id = focused_pane_id;
	session.browser_target_pane_id = Some(focused_pane_id);

	let mut groups = HashMap::new();
	for group in checkpoint.groups {
		let group_id = HostPanelGroupId::new(group.group_id);
		let current_members = group
			.current_members
			.into_iter()
			.map(|member| restore_member(member, &descriptors))
			.collect::<Result<Vec<_>, _>>()?;
		let retained_members = group
			.retained_members
			.into_iter()
			.map(|member| {
				let member = restore_member(member, &descriptors)?;
				Ok((member.panel_id, member))
			})
			.collect::<Result<HashMap<_, _>, String>>()?;
		let detached_members = group
			.detached_members
			.into_iter()
			.map(HostPanelId::new)
			.collect();
		let latest_valid_anchor = group
			.latest_anchor
			.as_ref()
			.and_then(|path| restored_paths.get(path).copied());
		groups.insert(
			group_id,
			HostPanelGroupState {
				producer_buffer_id: None,
				reattachable_after_restore: true,
				layout_state: match group.layout_state {
					CheckpointGroupLayoutState::Automatic => HostPanelGroupLayoutState::Automatic,
					CheckpointGroupLayoutState::Manual => HostPanelGroupLayoutState::Manual,
				},
				current_members,
				retained_members,
				detached_members,
				latest_valid_anchor,
			},
		);
	}
	session.host_panel_groups = groups;
	Ok(true)
}

fn restore_node(
	registry: &UastRegistry,
	session: &mut EditorSession,
	node: &LayoutCheckpointNode,
	panel_buffers: &BTreeMap<HostPanelId, BufferId>,
	path: &mut Vec<u32>,
	paths: &mut BTreeMap<LeafPath, PaneId>,
) -> Result<LayoutNode, String> {
	match node {
		LayoutCheckpointNode::Split {
			group_root,
			direction,
			children,
			ratios,
		} => {
			let mut restored = Vec::with_capacity(children.len());
			for (index, child) in children.iter().enumerate() {
				path.push(index as u32);
				restored.push(restore_node(
					registry,
					session,
					child,
					panel_buffers,
					path,
					paths,
				)?);
				path.pop();
			}
			Ok(LayoutNode::Split {
				group_root: group_root.map(HostPanelGroupId::new),
				direction: (*direction).into(),
				children: restored,
				ratios: ratios.clone(),
			})
		}
		LayoutCheckpointNode::Leaf {
			group_root,
			tabs,
			active_tab,
		} => {
			let pane_id = session.next_pane_id;
			session.next_pane_id = session.next_pane_id.next();
			let mut restored_tabs = Vec::with_capacity(tabs.len());
			for tab in tabs {
				let view = match tab {
					DurableTab::Document { document_id } => {
						let document_id = DocumentId::new(*document_id)
							.expect("validated document identity is nonzero");
						let buffer_id = session
							.saved_documents
							.as_ref()
							.and_then(|saved| saved.buffer_id(document_id))
							.ok_or_else(|| "restored layout document is not bound".to_owned())?;
						session.create_text_view_for_buffer(
							registry,
							buffer_id,
							DEFAULT_VIEWPORT_WIDTH,
							DEFAULT_VIEWPORT_LINES,
						)?
					}
					DurableTab::HostPanel { panel_id } => {
						let panel_id = HostPanelId::new(*panel_id);
						let buffer_id = *panel_buffers
							.get(&panel_id)
							.ok_or_else(|| "restored panel descriptor is missing".to_owned())?;
						let view_id = session.allocate_view_id();
						View::new_host_panel(
							view_id,
							buffer_id,
							&session.persistent_settings,
							DEFAULT_VIEWPORT_WIDTH,
							DEFAULT_VIEWPORT_LINES,
						)
					}
				};
				restored_tabs.push(view);
			}
			paths.insert(LeafPath(path.clone()), pane_id);
			Ok(LayoutNode::Leaf(LayoutLeaf {
				group_root: group_root.map(HostPanelGroupId::new),
				pane_id,
				pane: EditorPane {
					tabs: restored_tabs,
					active_tab_index: *active_tab,
				},
			}))
		}
	}
}

fn restore_member(
	member: HostPanelGroupMemberCheckpoint,
	descriptors: &BTreeMap<HostPanelId, HostPanelDescriptor>,
) -> Result<HostPanelGroupMember, String> {
	let panel_id = HostPanelId::new(member.panel_id);
	let descriptor = descriptors
		.get(&panel_id)
		.ok_or_else(|| "restored group member descriptor is missing".to_owned())?;
	Ok(HostPanelGroupMember {
		panel_id,
		kind: descriptor.kind.clone(),
		title: descriptor.title.clone(),
		shape: member.shape.into(),
	})
}

fn saved_document_ids(saved: &SavedDocumentState) -> BTreeSet<DocumentId> {
	saved
		.session
		.snapshot()
		.documents()
		.map(|doc| doc.id())
		.collect()
}

fn capture_node(
	node: &LayoutNode,
	buffers: &super::session::BufferStore,
	saved: &SavedDocumentState,
	panels: &mut BTreeMap<u64, HostPanelCheckpoint>,
) -> Result<Option<CapturedNode>, String> {
	match node {
		LayoutNode::Leaf(leaf) => {
			let mut tabs = Vec::new();
			let mut active_tab = None;
			for (source_index, view) in leaf.pane.tabs.iter().enumerate() {
				let Some(buffer) = buffers.get(view.buffer_id) else {
					return Err("layout view references a missing buffer".to_owned());
				};
				let durable = durable_tab(buffer, saved, panels)?;
				if let Some(durable) = durable {
					if source_index == leaf.pane.active_tab_index {
						active_tab = Some(tabs.len());
					}
					tabs.push(durable);
				}
			}
			if tabs.is_empty() {
				return Ok(None);
			}
			Ok(Some(CapturedNode::Leaf {
				group_root: leaf.group_root.map(HostPanelGroupId::get),
				source_pane: leaf.pane_id,
				tabs,
				active_tab: active_tab.unwrap_or(0),
			}))
		}
		LayoutNode::Split {
			group_root,
			direction,
			children,
			ratios,
		} => {
			if children.len() != ratios.len() {
				return Err("live layout child/ratio length mismatch".to_owned());
			}
			let mut captured_children = Vec::new();
			let mut captured_ratios = Vec::new();
			for (child, ratio) in children.iter().zip(ratios) {
				if let Some(child) = capture_node(child, buffers, saved, panels)? {
					captured_children.push(child);
					captured_ratios.push(*ratio);
				}
			}
			match captured_children.len() {
				0 => Ok(None),
				1 => {
					let mut child = captured_children.pop().expect("one captured child");
					if group_root.is_some() {
						child.set_group_root(group_root.map(HostPanelGroupId::get))?;
					}
					Ok(Some(child))
				}
				_ => Ok(Some(CapturedNode::Split {
					group_root: group_root.map(HostPanelGroupId::get),
					direction: (*direction).into(),
					children: captured_children,
					ratios: captured_ratios,
				})),
			}
		}
	}
}

fn durable_tab(
	buffer: &Buffer,
	saved: &SavedDocumentState,
	panels: &mut BTreeMap<u64, HostPanelCheckpoint>,
) -> Result<Option<DurableTab>, String> {
	if let Some(document_id) = saved.document_id(buffer.id) {
		return Ok(Some(DurableTab::Document {
			document_id: document_id.get(),
		}));
	}
	let Some(panel) = buffer.host_panel() else {
		return Ok(None);
	};
	let checkpoint = HostPanelCheckpoint {
		panel_id: panel.panel_id.get(),
		kind: panel.descriptor.kind.clone(),
		title: panel.descriptor.title.clone(),
	};
	if let Some(previous) = panels.insert(checkpoint.panel_id, checkpoint.clone())
		&& previous != checkpoint
	{
		return Err("one host panel identity has inconsistent descriptors".to_owned());
	}
	Ok(Some(DurableTab::HostPanel {
		panel_id: panel.panel_id.get(),
	}))
}

pub(crate) fn encode_layout(
	checkpoint: &LayoutCheckpoint,
	document_ids: &BTreeSet<DocumentId>,
	budget: u64,
) -> Result<Vec<u8>, String> {
	validate_layout(checkpoint, document_ids)?;
	let bytes = serde_json::to_vec(checkpoint)
		.map_err(|error| format!("could not encode logical layout: {error}"))?;
	if bytes.len() as u64 > budget {
		return Err(format!(
			"logical layout is {} bytes (limit is {budget})",
			bytes.len()
		));
	}
	Ok(bytes)
}

pub(crate) fn decode_layout(
	payload: &[u8],
	document_ids: &BTreeSet<DocumentId>,
	budget: u64,
) -> Result<LayoutCheckpoint, String> {
	if payload.len() as u64 > budget {
		return Err(format!(
			"logical layout is {} bytes (limit is {budget})",
			payload.len()
		));
	}
	let mut deserializer = serde_json::Deserializer::from_slice(payload);
	let checkpoint = LayoutCheckpoint::deserialize(&mut deserializer)
		.map_err(|error| format!("invalid logical layout: {error}"))?;
	deserializer
		.end()
		.map_err(|error| format!("trailing bytes after logical layout: {error}"))?;
	validate_layout(&checkpoint, document_ids)?;
	Ok(checkpoint)
}

fn validate_layout(
	checkpoint: &LayoutCheckpoint,
	document_ids: &BTreeSet<DocumentId>,
) -> Result<(), String> {
	let mut panels = BTreeSet::new();
	for panel in &checkpoint.panels {
		if panel.panel_id == 0 {
			return Err("host panel id is zero".to_owned());
		}
		if !panels.insert(panel.panel_id) {
			return Err("duplicate host panel descriptor".to_owned());
		}
	}

	let mut referenced_panels = BTreeSet::new();
	let mut group_root_counts = BTreeMap::<u64, usize>::new();
	if let Some(root) = &checkpoint.root {
		validate_node(
			root,
			document_ids,
			&panels,
			&mut referenced_panels,
			&mut group_root_counts,
		)?;
		let focused = checkpoint
			.focused_leaf
			.as_ref()
			.ok_or_else(|| "nonempty layout has no focused leaf".to_owned())?;
		if resolve_leaf(root, focused).is_none() {
			return Err("focused leaf path does not resolve".to_owned());
		}
	} else if checkpoint.focused_leaf.is_some() {
		return Err("empty layout has a focused leaf".to_owned());
	}
	if panels != referenced_panels {
		return Err("host panel descriptor set does not equal layout references".to_owned());
	}

	let mut groups = BTreeSet::new();
	for group in &checkpoint.groups {
		if group.group_id == 0 {
			return Err("host panel group id is zero".to_owned());
		}
		if !groups.insert(group.group_id) {
			return Err("duplicate host panel group".to_owned());
		}
		let roots = group_root_counts.get(&group.group_id).copied().unwrap_or(0);
		if roots > 1 {
			return Err("host panel group has multiple structural roots".to_owned());
		}
		if let Some(anchor) = &group.latest_anchor
			&& checkpoint
				.root
				.as_ref()
				.and_then(|root| resolve_leaf(root, anchor))
				.is_none()
		{
			return Err("host panel group anchor does not resolve".to_owned());
		}
		let mut current = BTreeSet::new();
		for member in &group.current_members {
			validate_member(member, &panels, &mut current, "current")?;
		}
		let mut retained = BTreeSet::new();
		for member in &group.retained_members {
			validate_member(member, &panels, &mut retained, "retained")?;
		}
		if current.iter().any(|id| retained.contains(id)) {
			return Err("current and retained host panel members overlap".to_owned());
		}
		let mut detached = BTreeSet::new();
		for panel_id in &group.detached_members {
			if *panel_id == 0 || !panels.contains(panel_id) {
				return Err("detached host panel member is missing".to_owned());
			}
			if !detached.insert(*panel_id) {
				return Err("duplicate detached host panel member".to_owned());
			}
			if !current.contains(panel_id) && !retained.contains(panel_id) {
				return Err("detached panel is neither current nor retained".to_owned());
			}
		}
	}
	for group_id in group_root_counts.keys() {
		if !groups.contains(group_id) {
			return Err("layout group root has no group record".to_owned());
		}
	}
	Ok(())
}

fn validate_member(
	member: &HostPanelGroupMemberCheckpoint,
	panels: &BTreeSet<u64>,
	seen: &mut BTreeSet<u64>,
	class: &str,
) -> Result<(), String> {
	if member.panel_id == 0 || !panels.contains(&member.panel_id) {
		return Err(format!("{class} host panel member is missing"));
	}
	if !seen.insert(member.panel_id) {
		return Err(format!("duplicate {class} host panel member"));
	}
	Ok(())
}

fn validate_node(
	node: &LayoutCheckpointNode,
	document_ids: &BTreeSet<DocumentId>,
	panels: &BTreeSet<u64>,
	referenced_panels: &mut BTreeSet<u64>,
	group_roots: &mut BTreeMap<u64, usize>,
) -> Result<(), String> {
	if let Some(group_id) = node.group_root() {
		if group_id == 0 {
			return Err("layout group root id is zero".to_owned());
		}
		*group_roots.entry(group_id).or_default() += 1;
	}
	match node {
		LayoutCheckpointNode::Split {
			children, ratios, ..
		} => {
			if children.len() < 2 {
				return Err("canonical split has fewer than two children".to_owned());
			}
			if children.len() != ratios.len() {
				return Err("layout child/ratio length mismatch".to_owned());
			}
			if ratios.contains(&0) {
				return Err("layout ratio is zero".to_owned());
			}
			for child in children {
				validate_node(child, document_ids, panels, referenced_panels, group_roots)?;
			}
		}
		LayoutCheckpointNode::Leaf {
			tabs, active_tab, ..
		} => {
			if tabs.is_empty() {
				return Err("canonical layout leaf is empty".to_owned());
			}
			if *active_tab >= tabs.len() {
				return Err("active tab index is out of bounds".to_owned());
			}
			for tab in tabs {
				match tab {
					DurableTab::Document { document_id } => {
						let id = DocumentId::new(*document_id)
							.map_err(|_| "document tab id is zero".to_owned())?;
						if !document_ids.contains(&id) {
							return Err("document tab references a missing document".to_owned());
						}
					}
					DurableTab::HostPanel { panel_id } => {
						if *panel_id == 0 || !panels.contains(panel_id) {
							return Err("host panel tab references a missing panel".to_owned());
						}
						referenced_panels.insert(*panel_id);
					}
				}
			}
		}
	}
	Ok(())
}

fn resolve_leaf<'a>(
	root: &'a LayoutCheckpointNode,
	path: &LeafPath,
) -> Option<&'a LayoutCheckpointNode> {
	let mut node = root;
	for index in &path.0 {
		let LayoutCheckpointNode::Split { children, .. } = node else {
			return None;
		};
		node = children.get(*index as usize)?;
	}
	matches!(node, LayoutCheckpointNode::Leaf { .. }).then_some(node)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn document(value: u64) -> DocumentId {
		DocumentId::new(value).unwrap()
	}

	fn fixture() -> (LayoutCheckpoint, BTreeSet<DocumentId>) {
		let documents = BTreeSet::from([document(4), document(9)]);
		let checkpoint = LayoutCheckpoint {
			root: Some(LayoutCheckpointNode::Split {
				group_root: None,
				direction: CheckpointDirection::Vertical,
				ratios: vec![3, 7],
				children: vec![
					LayoutCheckpointNode::Leaf {
						group_root: None,
						tabs: vec![
							DurableTab::Document { document_id: 4 },
							DurableTab::HostPanel { panel_id: 11 },
						],
						active_tab: 1,
					},
					LayoutCheckpointNode::Leaf {
						group_root: Some(17),
						tabs: vec![DurableTab::Document { document_id: 9 }],
						active_tab: 0,
					},
				],
			}),
			focused_leaf: Some(LeafPath(vec![1])),
			panels: vec![HostPanelCheckpoint {
				panel_id: 11,
				kind: "same".to_owned(),
				title: "same".to_owned(),
			}],
			groups: vec![HostPanelGroupCheckpoint {
				group_id: 17,
				layout_state: CheckpointGroupLayoutState::Manual,
				current_members: vec![HostPanelGroupMemberCheckpoint {
					panel_id: 11,
					shape: CheckpointGroupShape::Expansive,
				}],
				retained_members: vec![],
				detached_members: vec![11],
				latest_anchor: Some(LeafPath(vec![0])),
			}],
		};
		(checkpoint, documents)
	}

	#[test]
	fn product_0061_layout_codec_is_strict_deterministic_and_bounded() {
		let (checkpoint, documents) = fixture();
		let bytes = encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap();
		assert_eq!(
			decode_layout(&bytes, &documents, LAYOUT_OWNER_BUDGET).unwrap(),
			checkpoint
		);
		assert_eq!(
			encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap(),
			bytes,
			"logical layout encoding must be deterministic"
		);
		let mut trailing = bytes.clone();
		trailing.extend_from_slice(b"{}");
		assert!(
			decode_layout(&trailing, &documents, LAYOUT_OWNER_BUDGET)
				.unwrap_err()
				.contains("trailing")
		);
		assert!(decode_layout(&bytes, &documents, bytes.len() as u64 - 1).is_err());
		assert_eq!(LAYOUT_OWNER_BUDGET, 16 * 1024 * 1024);
	}

	#[test]
	fn product_0061_layout_codec_refuses_structural_mismatches() {
		let (mut checkpoint, documents) = fixture();
		let Some(LayoutCheckpointNode::Split { ratios, .. }) = &mut checkpoint.root else {
			unreachable!()
		};
		ratios.pop();
		assert_eq!(
			encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap_err(),
			"layout child/ratio length mismatch"
		);

		let (mut checkpoint, documents) = fixture();
		checkpoint.panels.push(checkpoint.panels[0].clone());
		assert_eq!(
			encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap_err(),
			"duplicate host panel descriptor"
		);

		let (mut checkpoint, documents) = fixture();
		checkpoint.focused_leaf = Some(LeafPath(vec![7]));
		assert_eq!(
			encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap_err(),
			"focused leaf path does not resolve"
		);

		let (mut checkpoint, documents) = fixture();
		checkpoint.groups.clear();
		assert_eq!(
			encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap_err(),
			"layout group root has no group record"
		);

		let (checkpoint, mut documents) = fixture();
		documents.remove(&document(9));
		assert_eq!(
			encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap_err(),
			"document tab references a missing document"
		);
	}

	#[test]
	fn product_0061_layout_codec_refuses_unknown_fields() {
		let (checkpoint, documents) = fixture();
		let bytes = encode_layout(&checkpoint, &documents, LAYOUT_OWNER_BUDGET).unwrap();
		let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
		value
			.as_object_mut()
			.unwrap()
			.insert("future".to_owned(), serde_json::json!(true));
		let bytes = serde_json::to_vec(&value).unwrap();
		assert!(
			decode_layout(&bytes, &documents, LAYOUT_OWNER_BUDGET)
				.unwrap_err()
				.contains("unknown field")
		);
	}
}
