use libaryon::core::{ReadOnly, SavedSession};

fn exercise(session: &mut SavedSession<ReadOnly>) {
	let _ = session.mint_document(None);
}

fn main() {
	let _ = exercise;
}
