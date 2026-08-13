#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/coordinate.rs"]
mod coordinate;

use coordinate::{Pt, Rect, Widget};

fn contains(rect: Rect<Widget>, cursor: Pt<Widget>) -> bool {
	rect.contains(cursor)
}

fn main() {
	let _ = contains;
}
