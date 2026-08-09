use libaryon::core::{DocumentId, ReadOnly, SavedSession};

fn exercise(session: &mut SavedSession<ReadOnly>, document: DocumentId) {
	let _ = session.remove_document(document);
}

fn main() {
	let _ = exercise;
}
