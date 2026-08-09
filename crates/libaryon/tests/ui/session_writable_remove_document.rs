use libaryon::core::{DocumentId, SavedSession, Writable};

fn exercise(session: &mut SavedSession<Writable>, document: DocumentId) {
	let _ = session.remove_document(document);
}

fn main() {
	let _ = exercise;
}
