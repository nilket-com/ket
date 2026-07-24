use super::Frontend;
use crossterm::event::{self, MouseButton, MouseEventKind};
use libaryon::core::{
	CursorPosition, DOCTAB_BAR_ROWS, DocDisplayCol, DocLine, PaneId, PaneRect, SPLIT_DIVIDER_COLS,
	SPLIT_DIVIDER_ROWS, ScreenCol, ScreenRow, ViewportId,
};
use libaryon::engine::{
	EditorCommand, MinimapSnapshot, MouseIntent, SplitDirection, minimap_click_viewport_start_line,
	viewport_geometry_for_viewport_with_numbering,
};
use libaryon::uast::projection::{DocTabSummary, PaneViewport, Viewport};
use ratatui::backend::Backend;
use ratatui::layout::Rect;
use std::io;
use std::time::{Duration, Instant};

const DOUBLE_CLICK_INTERVAL: Duration = Duration::from_millis(500);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PaneDivider {
	before: PaneId,
	after: PaneId,
	axis: SplitDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PaneResizeDrag {
	divider: PaneDivider,
	grab_offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TabHit {
	Title(usize),
	Close(usize),
}

/// Mirrors `draw_tab_bar`: each tab ends in a three-column ` x ` close target,
/// followed by its one-column separator.
fn tab_hit_test(tabs: &[DocTabSummary], column: u16, width: u16) -> Option<TabHit> {
	let mut x = 0usize;
	let click = usize::from(column);
	let max_x = usize::from(width);
	for (index, tab) in tabs.iter().enumerate() {
		if x >= max_x {
			break;
		}
		let title_width = 1 + tab.title.chars().count() + usize::from(tab.is_dirty) * 2;
		let close_start = x.saturating_add(title_width);
		let tab_end = close_start.saturating_add(3).min(max_x);
		if click >= x && click < tab_end {
			return Some(if click >= close_start {
				TabHit::Close(index)
			} else {
				TabHit::Title(index)
			});
		}
		x = close_start.saturating_add(3);
		if x < max_x {
			x += 1;
		}
	}
	None
}

fn is_browser_double_click(
	previous: Option<(PaneId, DocLine, Instant)>,
	pane_id: PaneId,
	line: DocLine,
	now: Instant,
) -> bool {
	previous.is_some_and(|(previous_pane, previous_line, previous_at)| {
		previous_pane == pane_id
			&& previous_line == line
			&& now.saturating_duration_since(previous_at) <= DOUBLE_CLICK_INTERVAL
	})
}

fn pane_under_mouse(view: &Viewport, column: u16, row: u16) -> Option<&PaneViewport> {
	let screen_col = ScreenCol::new(column as u32);
	let screen_row = ScreenRow::new(row as u32);
	view.panes
		.iter()
		.find(|pane| pane.rect.contains(screen_col, screen_row))
}

fn divider_under_mouse(view: &Viewport, column: u16, row: u16) -> Option<PaneDivider> {
	let rects = view
		.panes
		.iter()
		.map(|pane| (pane.pane_id, pane.rect))
		.collect::<Vec<_>>();
	divider_under_mouse_in_rects(&rects, column, row)
}

fn divider_position_at_drag_start(view: &Viewport, divider: PaneDivider) -> Option<u32> {
	let before = view.pane(divider.before)?;
	let after = view.pane(divider.after)?;
	let position = match divider.axis {
		SplitDirection::Vertical
			if before.rect.right().get().saturating_add(SPLIT_DIVIDER_COLS)
				== after.rect.screen_col.get() =>
		{
			before.rect.right().get()
		}
		SplitDirection::Horizontal
			if before
				.rect
				.bottom()
				.get()
				.saturating_add(SPLIT_DIVIDER_ROWS)
				== after.rect.screen_row.get() =>
		{
			before.rect.bottom().get()
		}
		_ => return None,
	};
	Some(position)
}

fn mouse_position_for_axis(column: u16, row: u16, axis: SplitDirection) -> u32 {
	u32::from(match axis {
		SplitDirection::Vertical => column,
		SplitDirection::Horizontal => row,
	})
}

fn pane_resize_target(drag: PaneResizeDrag, mouse_position: u32) -> u32 {
	mouse_position.saturating_sub(drag.grab_offset)
}

fn divider_under_mouse_in_rects(
	rects: &[(PaneId, PaneRect)],
	column: u16,
	row: u16,
) -> Option<PaneDivider> {
	let column = u32::from(column);
	let row = u32::from(row);
	let spans_row = |rect: PaneRect| row >= rect.screen_row.get() && row < rect.bottom().get();
	let left = rects
		.iter()
		.filter(|(_, rect)| spans_row(*rect) && rect.right().get() <= column)
		.max_by_key(|(_, rect)| rect.right().get());
	let right = rects
		.iter()
		.filter(|(_, rect)| spans_row(*rect) && rect.screen_col.get() > column)
		.min_by_key(|(_, rect)| rect.screen_col.get());
	if let (Some((before, left)), Some((after, right))) = (left, right) {
		let gap = right.screen_col.get().saturating_sub(left.right().get());
		if gap == SPLIT_DIVIDER_COLS {
			return Some(PaneDivider {
				before: *before,
				after: *after,
				axis: SplitDirection::Vertical,
			});
		}
	}

	let spans_column =
		|rect: PaneRect| column >= rect.screen_col.get() && column < rect.right().get();
	let above = rects
		.iter()
		.filter(|(_, rect)| spans_column(*rect) && rect.bottom().get() <= row)
		.max_by_key(|(_, rect)| rect.bottom().get());
	let below = rects
		.iter()
		.filter(|(_, rect)| spans_column(*rect) && rect.screen_row.get() > row)
		.min_by_key(|(_, rect)| rect.screen_row.get());
	if let (Some((before, above)), Some((after, below))) = (above, below) {
		let gap = below.screen_row.get().saturating_sub(above.bottom().get());
		if gap == SPLIT_DIVIDER_ROWS {
			return Some(PaneDivider {
				before: *before,
				after: *after,
				axis: SplitDirection::Horizontal,
			});
		}
	}

	None
}

fn minimap_click_content_height(full_content_height: u32, preview_row_count: Option<usize>) -> u32 {
	if full_content_height == 0 {
		return 0;
	}
	match preview_row_count {
		Some(0) | None => full_content_height,
		Some(rows) => {
			let rows = rows.min(u32::MAX as usize) as u32;
			full_content_height.min(rows).max(1)
		}
	}
}

fn cursor_click_command(
	viewport_id: ViewportId,
	modifiers: event::KeyModifiers,
	target: CursorPosition,
) -> EditorCommand {
	EditorCommand::MouseIntent {
		viewport_id,
		intent: MouseIntent::ClickCursor {
			target,
			alt: modifiers.contains(event::KeyModifiers::ALT),
		},
	}
}

fn text_click_target(
	visible_rows: &[libaryon::engine::VisibleRow],
	scroll_y: u64,
	local_text_row: u16,
	text_col: u16,
) -> CursorPosition {
	let row = visible_rows
		.get(local_text_row as usize)
		.copied()
		.unwrap_or_else(|| libaryon::engine::VisibleRow {
			line: DocLine::new(scroll_y.saturating_add(u64::from(local_text_row))),
			start_col: DocDisplayCol::ZERO,
		});
	let target_col = row
		.start_col
		.checked_add(u32::from(text_col))
		.expect("DocDisplayCol addition overflow");
	CursorPosition::new(row.line, target_col)
}

impl<B: Backend + io::Write> Frontend<B> {
	pub(super) fn handle_mouse(&mut self, mouse: event::MouseEvent) {
		match mouse.kind {
			MouseEventKind::ScrollUp => {
				if let Some(view) = &self.current_viewport
					&& let Some(pane) = pane_under_mouse(view, mouse.column, mouse.row)
				{
					self.send_command(EditorCommand::MouseIntent {
						viewport_id: view.viewport_id,
						intent: MouseIntent::FocusPane(pane.pane_id),
					});
				}
				if let Some(viewport_id) =
					self.current_viewport.as_ref().map(|view| view.viewport_id)
				{
					self.send_command(EditorCommand::MouseIntent {
						viewport_id,
						intent: MouseIntent::ScrollViewport(-3),
					});
				} else {
					self.send_command(EditorCommand::ScrollViewport(-3));
				}
				self.needs_redraw = true;
			}
			MouseEventKind::ScrollDown => {
				if let Some(view) = &self.current_viewport
					&& let Some(pane) = pane_under_mouse(view, mouse.column, mouse.row)
				{
					self.send_command(EditorCommand::MouseIntent {
						viewport_id: view.viewport_id,
						intent: MouseIntent::FocusPane(pane.pane_id),
					});
				}
				if let Some(viewport_id) =
					self.current_viewport.as_ref().map(|view| view.viewport_id)
				{
					self.send_command(EditorCommand::MouseIntent {
						viewport_id,
						intent: MouseIntent::ScrollViewport(3),
					});
				} else {
					self.send_command(EditorCommand::ScrollViewport(3));
				}
				self.needs_redraw = true;
			}
			MouseEventKind::Down(MouseButton::Left) => {
				let previous_browser_click = self.last_browser_click.take();
				let view = match &self.current_viewport {
					Some(v) => v,
					None => return,
				};
				if let Some(divider) = divider_under_mouse(view, mouse.column, mouse.row) {
					let Some(divider_position) = divider_position_at_drag_start(view, divider)
					else {
						return;
					};
					let mouse_position =
						mouse_position_for_axis(mouse.column, mouse.row, divider.axis);
					self.pane_resize_drag = Some(PaneResizeDrag {
						divider,
						grab_offset: mouse_position.saturating_sub(divider_position),
					});
					self.status_message = None;
					self.needs_redraw = true;
					return;
				}
				self.pane_resize_drag = None;
				let pane = match pane_under_mouse(view, mouse.column, mouse.row) {
					Some(pane) => pane,
					None => return,
				};
				self.send_command(EditorCommand::MouseIntent {
					viewport_id: view.viewport_id,
					intent: MouseIntent::FocusPane(pane.pane_id),
				});

				let pane_top = pane.rect.screen_row.get() as u16;
				let pane_left = pane.rect.screen_col.get() as u16;
				let text_area_top = pane_top.saturating_add(DOCTAB_BAR_ROWS);
				if mouse.row < text_area_top {
					let local_tab_col = mouse.column.saturating_sub(pane_left);
					if let Some(hit) =
						tab_hit_test(&pane.tabs, local_tab_col, pane.rect.width.get() as u16)
					{
						let tab = &pane.tabs[match hit {
							TabHit::Title(index) | TabHit::Close(index) => index,
						}];
						let intent = match hit {
							TabHit::Title(_) => MouseIntent::ActivateDocTab {
								pane_id: pane.pane_id,
								view_id: tab.view_id,
							},
							TabHit::Close(_) => MouseIntent::CloseDocTab {
								pane_id: pane.pane_id,
								view_id: tab.view_id,
							},
						};
						self.send_command(EditorCommand::MouseIntent {
							viewport_id: view.viewport_id,
							intent,
						});
					}
					self.needs_redraw = true;
					return;
				}
				let local_text_row = mouse.row.saturating_sub(text_area_top);
				let geometry = viewport_geometry_for_viewport_with_numbering(
					pane.viewport_start_line,
					pane.viewport_line_count,
					pane.max_doc_line,
					pane.rect.width.get() as u16,
					pane.minimap.is_some(),
					pane.gutter_numbering,
				);
				let gutter_width = geometry.gutter_width;
				let text_right = pane_left
					.saturating_add(gutter_width)
					.saturating_add(geometry.text_width);
				let click_x = mouse.column;
				if click_x < pane_left.saturating_add(gutter_width) {
					return;
				}
				if click_x >= text_right {
					if geometry.minimap_width > 0 && pane.minimap.is_some() {
						let minimap_x = text_right.saturating_add(geometry.separator_width);
						let minimap_right = minimap_x.saturating_add(geometry.minimap_width);
						if click_x >= minimap_x && click_x < minimap_right {
							let content_height = pane
								.rect
								.height
								.get()
								.saturating_sub(u32::from(DOCTAB_BAR_ROWS));
							let preview_row_count =
								pane.minimap.as_ref().and_then(|minimap| match minimap {
									MinimapSnapshot::Preview(snapshot) => Some(snapshot.rows.len()),
									MinimapSnapshot::Overview(_)
									| MinimapSnapshot::ByteFallback(_) => None,
								});
							let minimap_area = Rect {
								x: minimap_x,
								y: text_area_top,
								width: geometry.minimap_width,
								height: content_height.min(u32::from(u16::MAX)) as u16,
							};
							let image_content_height = preview_row_count.and_then(|rows| {
								self.minimap.current_preview_content_height(
									pane.view_id,
									minimap_area,
									rows,
								)
							});
							let content_height = image_content_height.unwrap_or_else(|| {
								minimap_click_content_height(content_height, preview_row_count)
							});
							if let Some(target) = minimap_click_viewport_start_line(
								u32::from(local_text_row),
								pane.max_doc_line,
								content_height,
								pane.viewport_line_count,
							) {
								self.send_command(EditorCommand::MouseIntent {
									viewport_id: view.viewport_id,
									intent: MouseIntent::ScrollViewportToLine(target),
								});
								self.needs_redraw = true;
							}
						}
					}
					return;
				}

				let target = text_click_target(
					&pane.visible_rows,
					pane.scroll_y,
					local_text_row,
					click_x.saturating_sub(pane_left + gutter_width),
				);
				let pane_id = pane.pane_id;
				let is_file_browser = pane.is_file_browser;
				let is_git_status_panel = pane.is_git_status_panel;
				if is_file_browser {
					let now = Instant::now();
					let double_click =
						is_browser_double_click(previous_browser_click, pane_id, target.line, now);
					if !double_click {
						self.last_browser_click = Some((pane_id, target.line, now));
					}
					self.send_command(EditorCommand::MouseIntent {
						viewport_id: view.viewport_id,
						intent: MouseIntent::ClickBrowserEntry {
							line: target.line,
							double_click,
						},
					});
				} else if is_git_status_panel {
					let now = Instant::now();
					let double_click =
						is_browser_double_click(previous_browser_click, pane_id, target.line, now);
					if double_click {
						self.send_command(EditorCommand::MouseIntent {
							viewport_id: view.viewport_id,
							intent: MouseIntent::OpenGitStatusEntry { line: target.line },
						});
					} else {
						self.last_browser_click = Some((pane_id, target.line, now));
						self.send_command(cursor_click_command(
							view.viewport_id,
							mouse.modifiers,
							target,
						));
					}
				} else {
					self.send_command(cursor_click_command(
						view.viewport_id,
						mouse.modifiers,
						target,
					));
				}
				self.needs_redraw = true;
			}
			MouseEventKind::Drag(MouseButton::Left) => {
				if let Some(drag) = self.pane_resize_drag {
					let mouse_position =
						mouse_position_for_axis(mouse.column, mouse.row, drag.divider.axis);
					self.send_command(EditorCommand::ResizePaneBoundary {
						before: drag.divider.before,
						after: drag.divider.after,
						axis: drag.divider.axis,
						target: pane_resize_target(drag, mouse_position),
					});
				}
			}
			MouseEventKind::Up(MouseButton::Left) => {
				self.pane_resize_drag = None;
			}
			_ => {}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{
		DOUBLE_CLICK_INTERVAL, PaneDivider, PaneResizeDrag, TabHit, cursor_click_command,
		divider_under_mouse_in_rects, is_browser_double_click, minimap_click_content_height,
		minimap_click_viewport_start_line, pane_resize_target, tab_hit_test, text_click_target,
	};
	use crossterm::event::KeyModifiers;
	use libaryon::core::{
		CursorPosition, DocDisplayCol, DocLine, PaneCol, PaneId, PaneRect, PaneRow, ScreenCol,
		ScreenRow, ViewportId,
	};
	use libaryon::engine::{EditorCommand, MouseIntent, SplitDirection, VisibleRow};
	use libaryon::uast::projection::DocTabSummary;
	use std::time::{Duration, Instant};

	fn rect(x: u32, y: u32, width: u32, height: u32) -> PaneRect {
		PaneRect::new(
			ScreenCol::new(x),
			ScreenRow::new(y),
			PaneCol::new(width),
			PaneRow::new(height),
		)
	}

	fn tab(view_id: u64, title: &str, is_dirty: bool) -> DocTabSummary {
		DocTabSummary {
			buffer_id: libaryon::core::BufferId::new(view_id),
			view_id: libaryon::core::ViewId::new(view_id),
			title: title.to_string(),
			is_dirty,
		}
	}

	#[test]
	fn tab_title_and_spaced_close_targets_are_distinct() {
		let tabs = [tab(1, "one", false), tab(2, "two", true)];
		assert_eq!(tab_hit_test(&tabs, 0, 30), Some(TabHit::Title(0)));
		assert_eq!(tab_hit_test(&tabs, 3, 30), Some(TabHit::Title(0)));
		assert_eq!(tab_hit_test(&tabs, 4, 30), Some(TabHit::Close(0)));
		assert_eq!(tab_hit_test(&tabs, 6, 30), Some(TabHit::Close(0)));
		assert_eq!(tab_hit_test(&tabs, 7, 30), None);
		assert_eq!(tab_hit_test(&tabs, 8, 30), Some(TabHit::Title(1)));
		assert_eq!(tab_hit_test(&tabs, 13, 30), Some(TabHit::Title(1)));
		assert_eq!(tab_hit_test(&tabs, 14, 30), Some(TabHit::Close(1)));
	}

	#[test]
	fn vertical_divider_hit_test_returns_its_neighboring_panes() {
		let panes = [
			(PaneId::new(1), rect(0, 0, 18, 30)),
			(PaneId::new(2), rect(22, 0, 58, 30)),
		];
		assert_eq!(
			divider_under_mouse_in_rects(&panes, 20, 10),
			Some(PaneDivider {
				before: PaneId::new(1),
				after: PaneId::new(2),
				axis: SplitDirection::Vertical,
			})
		);
		assert_eq!(divider_under_mouse_in_rects(&panes, 17, 10), None);
		assert_eq!(divider_under_mouse_in_rects(&panes, 22, 10), None);
	}

	#[test]
	fn horizontal_divider_hit_test_returns_its_neighboring_panes() {
		let panes = [
			(PaneId::new(3), rect(0, 0, 80, 12)),
			(PaneId::new(4), rect(0, 13, 80, 17)),
		];
		assert_eq!(
			divider_under_mouse_in_rects(&panes, 40, 12),
			Some(PaneDivider {
				before: PaneId::new(3),
				after: PaneId::new(4),
				axis: SplitDirection::Horizontal,
			})
		);
	}

	#[test]
	fn absolute_drag_targets_do_not_depend_on_viewport_updates() {
		let drag = PaneResizeDrag {
			divider: PaneDivider {
				before: PaneId::new(1),
				after: PaneId::new(2),
				axis: SplitDirection::Vertical,
			},
			grab_offset: 2,
		};

		assert_eq!(pane_resize_target(drag, 51), 49);
		assert_eq!(pane_resize_target(drag, 52), 50);
		assert_eq!(pane_resize_target(drag, 53), 51);
		assert_eq!(pane_resize_target(drag, 53), 51);
		assert_eq!(pane_resize_target(drag, 1), 0);
	}

	#[test]
	fn browser_double_click_requires_the_same_pane_and_line_within_the_interval() {
		let pane = PaneId::new(3);
		let line = DocLine::new(7);
		let first = Instant::now();
		let previous = Some((pane, line, first));
		assert!(is_browser_double_click(
			previous,
			pane,
			line,
			first + DOUBLE_CLICK_INTERVAL,
		));
		assert!(!is_browser_double_click(
			previous,
			pane,
			line.saturating_add(1),
			first + Duration::from_millis(1),
		));
		assert!(!is_browser_double_click(
			previous,
			pane.next(),
			line,
			first + Duration::from_millis(1),
		));
		assert!(!is_browser_double_click(
			previous,
			pane,
			line,
			first + DOUBLE_CLICK_INTERVAL + Duration::from_millis(1),
		));
	}

	#[test]
	fn alt_click_preserves_hit_test_revision_and_intent() {
		let target = CursorPosition::new(DocLine::new(4), DocDisplayCol::new(7));
		let viewport_id = ViewportId::new(9);
		assert!(matches!(
			cursor_click_command(viewport_id, KeyModifiers::ALT, target),
			EditorCommand::MouseIntent {
				viewport_id: actual_revision,
				intent: MouseIntent::ClickCursor {
					target: actual,
					alt: true,
				},
			} if actual_revision == viewport_id && actual == target
		));
	}

	#[test]
	fn wrapped_text_click_uses_the_visible_rows_start_column() {
		let rows = [
			VisibleRow {
				line: DocLine::new(7),
				start_col: DocDisplayCol::ZERO,
			},
			VisibleRow {
				line: DocLine::new(7),
				start_col: DocDisplayCol::new(10),
			},
		];
		assert_eq!(
			text_click_target(&rows, 7, 1, 3),
			CursorPosition::new(DocLine::new(7), DocDisplayCol::new(13))
		);
		assert_eq!(
			text_click_target(&rows, 7, 4, 2),
			CursorPosition::new(DocLine::new(11), DocDisplayCol::new(2))
		);
	}

	#[test]
	fn minimap_click_at_top_targets_document_start() {
		assert_eq!(
			minimap_click_viewport_start_line(0, DocLine::new(1000), 40, 40).expect("target"),
			DocLine::ZERO
		);
	}

	#[test]
	fn minimap_click_at_bottom_requests_a_centered_start_near_document_end() {
		// The engine clamps this further so the final viewport does not extend past EOF.
		assert_eq!(
			minimap_click_viewport_start_line(39, DocLine::new(1000), 40, 40).expect("target"),
			DocLine::new(980)
		);
	}

	#[test]
	fn minimap_click_centers_the_interpolated_line() {
		let target =
			minimap_click_viewport_start_line(20, DocLine::new(1000), 40, 40).expect("target");
		assert_eq!(target, DocLine::new(492));
	}

	#[test]
	fn minimap_click_short_preview_uses_occupied_rows() {
		let content_height = minimap_click_content_height(40, Some(10));
		assert_eq!(content_height, 10);
		assert_eq!(
			minimap_click_viewport_start_line(5, DocLine::new(10), content_height, 4)
				.expect("target"),
			DocLine::new(3)
		);
	}

	#[test]
	fn minimap_click_below_short_preview_requests_centered_start_near_document_end() {
		let content_height = minimap_click_content_height(40, Some(10));
		assert_eq!(
			minimap_click_viewport_start_line(35, DocLine::new(10), content_height, 4)
				.expect("target"),
			DocLine::new(8)
		);
	}

	#[test]
	fn minimap_click_preview_taller_than_area_uses_full_height() {
		assert_eq!(minimap_click_content_height(40, Some(100)), 40);
	}

	#[test]
	fn minimap_click_image_preview_uses_rendered_rows() {
		assert_eq!(
			minimap_click_viewport_start_line(3, DocLine::new(100), 5, 4).expect("target"),
			DocLine::new(73)
		);
		assert_eq!(
			minimap_click_viewport_start_line(4, DocLine::new(100), 5, 4).expect("target"),
			DocLine::new(98)
		);
	}

	#[test]
	fn minimap_click_targets_single_line_document() {
		assert_eq!(
			minimap_click_viewport_start_line(0, DocLine::ZERO, 40, 40).expect("target"),
			DocLine::ZERO
		);
	}

	#[test]
	fn minimap_click_returns_none_for_empty_content() {
		assert!(minimap_click_viewport_start_line(0, DocLine::new(100), 0, 40).is_none());
	}
}
