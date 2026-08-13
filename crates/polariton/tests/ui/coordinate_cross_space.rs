#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/coordinate.rs"]
mod coordinate;

use coordinate::{Cell, Plot, Pt, Rect, Widget};

fn cell_cursor_against_widget_rect(rect: Rect<Widget>, cursor: Pt<Cell>) -> bool {
	rect.contains(cursor)
}

fn plot_cursor_against_widget_rect(rect: Rect<Widget>, cursor: Pt<Plot>) -> bool {
	rect.contains(cursor)
}

fn main() {
	let _ = cell_cursor_against_widget_rect;
	let _ = plot_cursor_against_widget_rect;
}
