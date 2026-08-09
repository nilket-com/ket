use libaryon::core::{DocumentId, ReadOnly, SavedSession};
use std::path::PathBuf;

fn exercise(session: &mut SavedSession<ReadOnly>, document: DocumentId, path: PathBuf) {
	let _ = session.update_document_path(document, Some(path));
}

fn main() {
	let _ = exercise;
}
