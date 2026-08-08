#[path = "../../src/coordinate.rs"]
mod coordinate;

use coordinate::{Pt, Rect, Widget, Window};

fn widget_cursor_against_window_rect(rect: Rect<Window>, cursor: Pt<Widget>) -> bool {
	rect.contains(cursor)
}

fn main() {
	let _ = widget_cursor_against_window_rect;
}
