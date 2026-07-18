use std::time::Instant;

use iced::keyboard::{self, key};

use super::geometry::{root_by_id, submenu_items};
use super::{MenuItem, MenuRoot, MenuState};

#[derive(Debug, Default)]
pub(crate) struct WidgetState {
	pub keyboard_navigation: bool,
	pub focus_root: Option<String>,
	pub focus_path: Vec<String>,
	pub pending_close_at: Option<Instant>,
}

impl WidgetState {
	pub fn clear(&mut self) {
		self.keyboard_navigation = false;
		self.focus_root = None;
		self.focus_path.clear();
		self.pending_close_at = None;
	}

	pub fn cancel_pending_close(&mut self) -> bool {
		self.pending_close_at.take().is_some()
	}

	pub fn sync(&mut self, roots: &[MenuRoot], menu_state: &MenuState) {
		let Some(open_root) = menu_state.open_root() else {
			self.clear();
			return;
		};

		self.focus_root = Some(open_root.to_owned());

		let Some(root) = root_by_id(roots, open_root) else {
			self.clear();
			return;
		};

		let visible_depths = menu_state.open_path().len() + 1;
		self.focus_path.truncate(visible_depths);

		let mut items = root.items.as_slice();

		for depth in 0..visible_depths {
			let fallback = first_selectable(items);
			let focused = self
				.focus_path
				.get(depth)
				.map(String::as_str)
				.filter(|id| selectable_item(items, id).is_some())
				.or(fallback);

			let Some(focused) = focused else {
				self.focus_path.truncate(depth);
				return;
			};

			if depth < self.focus_path.len() {
				self.focus_path[depth] = focused.to_owned();
			} else {
				self.focus_path.push(focused.to_owned());
			}

			if let Some(submenu_id) = menu_state.open_path().get(depth) {
				let Some(next_items) = submenu_items(items, submenu_id) else {
					self.focus_path.truncate(depth + 1);
					return;
				};

				items = next_items;
			}
		}
	}

	pub fn focus_root_panel(&mut self, roots: &[MenuRoot], root_id: &str) {
		self.keyboard_navigation = true;
		self.focus_root = Some(root_id.to_owned());
		self.focus_path.clear();

		if let Some(root) = root_by_id(roots, root_id)
			&& let Some(first) = first_selectable(&root.items)
		{
			self.focus_path.push(first.to_owned());
		}
	}

	pub fn focus_current_panel(
		&mut self,
		roots: &[MenuRoot],
		menu_state: &MenuState,
		direction: MoveDirection,
	) -> bool {
		self.sync(roots, menu_state);

		let Some((depth, items)) = focused_panel_items(roots, menu_state, self) else {
			return false;
		};

		let current = self.focus_path.get(depth).map(String::as_str);
		let next = match (current, direction) {
			(Some(current), MoveDirection::Next) => next_selectable(items, current),
			(Some(current), MoveDirection::Previous) => previous_selectable(items, current),
			(None, _) => first_selectable(items),
		};

		let Some(next) = next else {
			return false;
		};

		self.keyboard_navigation = true;

		if depth < self.focus_path.len() {
			self.focus_path[depth] = next.to_owned();
			self.focus_path.truncate(depth + 1);
		} else {
			self.focus_path.push(next.to_owned());
		}

		true
	}

	pub fn focus_submenu(&mut self, items: &[MenuItem], depth: usize, id: &str) -> bool {
		let Some(children) = submenu_items(items, id) else {
			return false;
		};

		self.keyboard_navigation = true;

		if depth < self.focus_path.len() {
			self.focus_path[depth] = id.to_owned();
			self.focus_path.truncate(depth + 1);
		} else {
			self.focus_path.push(id.to_owned());
		}

		if let Some(first) = first_selectable(children) {
			self.focus_path.push(first.to_owned());
		}

		true
	}

	pub fn focused(&self, id: &str, depth: usize) -> bool {
		self.keyboard_navigation && self.focus_path.get(depth).is_some_and(|focused| focused == id)
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum MoveDirection {
	Next,
	Previous,
}

pub(crate) fn panel_items<'a>(
	roots: &'a [MenuRoot],
	state: &'a MenuState,
	depth: usize,
) -> Option<&'a [MenuItem]> {
	let root = root_by_id(roots, state.open_root()?)?;
	let mut items = root.items.as_slice();

	for submenu_id in state.open_path().iter().take(depth) {
		items = submenu_items(items, submenu_id)?;
	}

	Some(items)
}

pub(crate) fn focused_panel_items<'a>(
	roots: &'a [MenuRoot],
	state: &'a MenuState,
	widget_state: &WidgetState,
) -> Option<(usize, &'a [MenuItem])> {
	let depth = widget_state.focus_path.len().checked_sub(1)?;
	panel_items(roots, state, depth).map(|items| (depth, items))
}

pub(crate) fn selectable_item<'a>(items: &'a [MenuItem], id: &str) -> Option<&'a MenuItem> {
	items.iter().find(|item| match item {
		MenuItem::Action { id: item_id, .. } | MenuItem::Submenu { id: item_id, .. } => {
			*item_id == id
		}
		MenuItem::Separator => false,
	})
}

pub(crate) fn adjacent_root<'a>(
	roots: &'a [MenuRoot],
	current: &str,
	offset: isize,
) -> Option<&'a MenuRoot> {
	let index = roots.iter().position(|root| root.id == current)?;
	let len = roots.len() as isize;
	let next_index = (index as isize + offset).rem_euclid(len) as usize;
	roots.get(next_index)
}

pub(crate) fn is_menu_activation(key: &key::Key<&str>, modifiers: keyboard::Modifiers) -> bool {
	modifiers.command()
		&& modifiers.shift()
		&& !modifiers.alt()
		&& matches!(key, key::Key::Character("m" | "M"))
}

pub(crate) fn navigation_direction(key: &key::Key<&str>, shift: bool) -> Option<MoveDirection> {
	match key {
		key::Key::Named(key::Named::ArrowDown) => Some(MoveDirection::Next),
		key::Key::Named(key::Named::ArrowUp) => Some(MoveDirection::Previous),
		key::Key::Named(key::Named::Tab) if shift => Some(MoveDirection::Previous),
		key::Key::Named(key::Named::Tab) => Some(MoveDirection::Next),
		_ => None,
	}
}

fn first_selectable(items: &[MenuItem]) -> Option<&str> {
	items.iter().find_map(item_id)
}

fn next_selectable<'a>(items: &'a [MenuItem], current: &str) -> Option<&'a str> {
	cycle_selectable(items, current, 1)
}

fn previous_selectable<'a>(items: &'a [MenuItem], current: &str) -> Option<&'a str> {
	cycle_selectable(items, current, -1)
}

fn cycle_selectable<'a>(
	items: &'a [MenuItem],
	current: &str,
	step: isize,
) -> Option<&'a str> {
	let ids: Vec<_> = items.iter().filter_map(item_id).collect();
	let current_index = ids.iter().position(|id| *id == current)?;

	if ids.is_empty() {
		return None;
	}

	let len = ids.len() as isize;
	let next_index = (current_index as isize + step).rem_euclid(len) as usize;
	ids.get(next_index).copied()
}

fn item_id(item: &MenuItem) -> Option<&str> {
	match item {
		MenuItem::Action { id, .. } | MenuItem::Submenu { id, .. } => Some(id.as_str()),
		MenuItem::Separator => None,
	}
}
