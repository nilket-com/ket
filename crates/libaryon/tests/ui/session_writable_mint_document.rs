use libaryon::core::{SavedSession, Writable};

fn exercise(session: &mut SavedSession<Writable>) {
	let _ = session.mint_document(None);
}

fn main() {
	let _ = exercise;
}
