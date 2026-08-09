use libaryon::core::{DocumentId, SavedSession, Writable};
use std::path::PathBuf;

fn exercise(session: &mut SavedSession<Writable>, document: DocumentId, path: PathBuf) {
	let _ = session.update_document_path(document, Some(path));
}

fn main() {
	let _ = exercise;
}
