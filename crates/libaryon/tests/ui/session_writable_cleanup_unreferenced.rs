use libaryon::core::{SavedSession, Writable};

fn exercise(session: &mut SavedSession<Writable>) {
	let _ = session.cleanup_unreferenced();
}

fn main() {
	let _ = exercise;
}
