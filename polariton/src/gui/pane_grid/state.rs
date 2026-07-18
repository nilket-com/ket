use iced::{Point, Size};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PaneId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Axis {
	Horizontal,
	Vertical,
}

impl Axis {
	pub fn is_horizontal(self) -> bool {
		self == Axis::Horizontal
	}

	pub fn split_size(self, size: Size) -> (f32, f32) {
		match self {
			Axis::Horizontal => (size.width, size.height),
			Axis::Vertical => (size.height, size.width),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollapseState {
	Expanded,
	FirstCollapsed,
	SecondCollapsed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitId(pub usize);

#[derive(Debug, Clone)]
pub struct Split {
	pub axis: Axis,
	pub ratio: f32,
	pub saved_ratio: f32,
	pub collapse: CollapseState,
	pub first: NodeId,
	pub second: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeId {
	Split(SplitId),
	Pane(PaneId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaximizeState {
	None,
	Maximized(PaneId),
}

#[derive(Debug, Clone, Copy)]
pub struct PaneDrag {
	pub pane: PaneId,
	pub origin: Point,
	pub current: Point,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropEdge {
	Top,
	Bottom,
	Left,
	Right,
	Center,
}

#[derive(Debug, Clone, Copy)]
pub struct DropTarget {
	pub pane: PaneId,
	pub edge: DropEdge,
}

#[derive(Debug, Clone)]
pub enum Action {
	DragStarted(SplitId),
	DragMoved(SplitId, f32),
	DragEnded,
	CollapseFirst(SplitId),
	CollapseSecond(SplitId),
	Expand(SplitId),
	Maximize(PaneId),
	PaneDragStarted(PaneId, Point),
	PaneDragMoved(Point),
	PaneDragDropped(DropTarget),
	PaneDragCancelled,
}

#[derive(Debug, Clone)]
pub struct State {
	panes: Vec<PaneId>,
	splits: Vec<(SplitId, Split)>,
	root: Option<NodeId>,
	next_pane_id: usize,
	next_split_id: usize,
	maximize: MaximizeState,
	dragging: Option<SplitId>,
	pane_dragging: Option<PaneDrag>,
}

impl State {
	pub fn new() -> Self {
		Self {
			panes: Vec::new(),
			splits: Vec::new(),
			root: None,
			next_pane_id: 0,
			next_split_id: 0,
			maximize: MaximizeState::None,
			dragging: None,
			pane_dragging: None,
		}
	}

	pub fn with_initial_pane() -> (Self, PaneId) {
		let mut state = Self::new();
		let pane = state.alloc_pane();
		state.root = Some(NodeId::Pane(pane));
		(state, pane)
	}

	fn alloc_pane(&mut self) -> PaneId {
		let id = PaneId(self.next_pane_id);
		self.next_pane_id += 1;
		self.panes.push(id);
		id
	}

	fn alloc_split(&mut self, split: Split) -> SplitId {
		let id = SplitId(self.next_split_id);
		self.next_split_id += 1;
		self.splits.push((id, split));
		id
	}

	pub fn split(&mut self, axis: Axis, pane: PaneId) -> Option<(SplitId, PaneId)> {
		let new_pane = self.alloc_pane();
		let node = self.find_node(NodeId::Pane(pane))?;

		let split = Split {
			axis,
			ratio: 0.5,
			saved_ratio: 0.5,
			collapse: CollapseState::Expanded,
			first: NodeId::Pane(pane),
			second: NodeId::Pane(new_pane),
		};

		let split_id = self.alloc_split(split);
		let split_node = NodeId::Split(split_id);

		if self.root == Some(node) {
			self.root = Some(split_node);
		} else {
			self.replace_node(node, split_node);
		}

		Some((split_id, new_pane))
	}

	fn find_node(&self, target: NodeId) -> Option<NodeId> {
		if self.root == Some(target) {
			return Some(target);
		}
		for (_, split) in &self.splits {
			if split.first == target || split.second == target {
				return Some(target);
			}
		}
		None
	}

	fn replace_node(&mut self, old: NodeId, new: NodeId) {
		if self.root == Some(old) {
			self.root = Some(new);
			return;
		}
		for (_, split) in &mut self.splits {
			if split.first == old {
				split.first = new;
				return;
			}
			if split.second == old {
				split.second = new;
				return;
			}
		}
	}

	pub fn get_split(&self, id: SplitId) -> Option<&Split> {
		self.splits.iter().find(|(sid, _)| *sid == id).map(|(_, s)| s)
	}

	pub fn get_split_mut(&mut self, id: SplitId) -> Option<&mut Split> {
		self.splits.iter_mut().find(|(sid, _)| *sid == id).map(|(_, s)| s)
	}

	pub fn root(&self) -> Option<NodeId> {
		self.root
	}

	pub fn panes(&self) -> &[PaneId] {
		&self.panes
	}

	pub fn maximize(&self) -> MaximizeState {
		self.maximize
	}

	pub fn toggle_maximize(&mut self, pane: PaneId) {
		self.maximize = match self.maximize {
			MaximizeState::Maximized(p) if p == pane => MaximizeState::None,
			_ => MaximizeState::Maximized(pane),
		};
	}

	pub fn restore_maximize(&mut self) {
		self.maximize = MaximizeState::None;
	}

	pub fn collapse_first(&mut self, split_id: SplitId) {
		if let Some(split) = self.get_split_mut(split_id) {
			if split.collapse == CollapseState::Expanded {
				split.saved_ratio = split.ratio;
			}
			split.collapse = CollapseState::FirstCollapsed;
			split.ratio = 0.0;
		}
	}

	pub fn collapse_second(&mut self, split_id: SplitId) {
		if let Some(split) = self.get_split_mut(split_id) {
			if split.collapse == CollapseState::Expanded {
				split.saved_ratio = split.ratio;
			}
			split.collapse = CollapseState::SecondCollapsed;
			split.ratio = 1.0;
		}
	}

	pub fn expand(&mut self, split_id: SplitId) {
		if let Some(split) = self.get_split_mut(split_id) {
			split.ratio = split.saved_ratio;
			split.collapse = CollapseState::Expanded;
		}
	}

	pub fn set_dragging(&mut self, split: Option<SplitId>) {
		self.dragging = split;
	}

	pub fn dragging(&self) -> Option<SplitId> {
		self.dragging
	}

	pub fn resize(&mut self, split_id: SplitId, ratio: f32) {
		if let Some(split) = self.get_split_mut(split_id)
		&& split.collapse == CollapseState::Expanded {
			split.ratio = ratio.clamp(0.05, 0.95);
			split.saved_ratio = split.ratio;
		}
	}

	pub fn pane_dragging(&self) -> Option<PaneDrag> {
		self.pane_dragging
	}

	pub fn set_pane_dragging(&mut self, drag: PaneDrag) {
		self.pane_dragging = Some(drag);
	}

	pub fn update_pane_drag_position(&mut self, pos: Point) {
		if let Some(ref mut drag) = self.pane_dragging {
			drag.current = pos;
		}
	}

	pub fn clear_pane_dragging(&mut self) {
		self.pane_dragging = None;
	}

	pub fn swap_panes(&mut self, a: PaneId, b: PaneId) {
		let node_a = NodeId::Pane(a);
		let node_b = NodeId::Pane(b);
		if self.root == Some(node_a) {
			self.root = Some(node_b);
		} else if self.root == Some(node_b) {
			self.root = Some(node_a);
		}
		for (_, split) in &mut self.splits {
			let first_is_a = split.first == node_a;
			let first_is_b = split.first == node_b;
			let second_is_a = split.second == node_a;
			let second_is_b = split.second == node_b;
			if first_is_a { split.first = node_b; }
			else if first_is_b { split.first = node_a; }
			if second_is_a { split.second = node_b; }
			else if second_is_b { split.second = node_a; }
		}
	}

	pub fn detach_pane(&mut self, pane: PaneId) -> bool {
		let pane_node = NodeId::Pane(pane);
		let found = self.splits.iter()
			.find(|(_, s)| s.first == pane_node || s.second == pane_node)
			.map(|(id, s)| {
				let sibling = if s.first == pane_node { s.second } else { s.first };
				(*id, sibling)
			});
		let (parent_id, sibling) = match found {
			Some(v) => v,
			None => return false,
		};
		let parent_node = NodeId::Split(parent_id);
		self.replace_node(parent_node, sibling);
		self.splits.retain(|(id, _)| *id != parent_id);
		true
	}

	pub fn insert_by_split(&mut self, pane: PaneId, target: PaneId, axis: Axis, before: bool) {
		let target_node = NodeId::Pane(target);
		let pane_node = NodeId::Pane(pane);
		let (first, second) = if before {
			(pane_node, target_node)
		} else {
			(target_node, pane_node)
		};
		let split = Split {
			axis,
			ratio: 0.5,
			saved_ratio: 0.5,
			collapse: CollapseState::Expanded,
			first,
			second,
		};
		let split_id = self.alloc_split(split);
		self.replace_node(target_node, NodeId::Split(split_id));
	}

	pub fn apply(&mut self, action: Action) {
		match action {
			Action::DragStarted(split_id) => {
				self.set_dragging(Some(split_id));
			}
			Action::DragMoved(split_id, ratio) => {
				self.resize(split_id, ratio);
			}
			Action::DragEnded => {
				self.set_dragging(None);
			}
			Action::CollapseFirst(split_id) => {
				self.collapse_first(split_id);
			}
			Action::CollapseSecond(split_id) => {
				self.collapse_second(split_id);
			}
			Action::Expand(split_id) => {
				self.expand(split_id);
			}
			Action::Maximize(pane) => {
				self.toggle_maximize(pane);
			}
			Action::PaneDragStarted(pane, origin) => {
				self.set_pane_dragging(PaneDrag {
					pane,
					origin,
					current: origin,
				});
			}
			Action::PaneDragMoved(pos) => {
				self.update_pane_drag_position(pos);
			}
			Action::PaneDragDropped(target) => {
				if let Some(drag) = self.pane_dragging() {
					let source = drag.pane;
					let dest = target.pane;
					match target.edge {
						DropEdge::Center => {
							self.swap_panes(source, dest);
						}
						DropEdge::Left => {
							if self.detach_pane(source) {
								self.insert_by_split(source, dest, Axis::Horizontal, true);
							}
						}
						DropEdge::Right => {
							if self.detach_pane(source) {
								self.insert_by_split(source, dest, Axis::Horizontal, false);
							}
						}
						DropEdge::Top => {
							if self.detach_pane(source) {
								self.insert_by_split(source, dest, Axis::Vertical, true);
							}
						}
						DropEdge::Bottom => {
							if self.detach_pane(source) {
								self.insert_by_split(source, dest, Axis::Vertical, false);
							}
						}
					}
				}
				self.clear_pane_dragging();
			}
			Action::PaneDragCancelled => {
				self.clear_pane_dragging();
			}
		}
	}
}
