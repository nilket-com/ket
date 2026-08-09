use libaryon::core::{ReadOnly, SavedSession};

fn exercise(session: &mut SavedSession<ReadOnly>) {
	let _ = session.cleanup_unreferenced();
}

fn main() {
	let _ = exercise;
}
