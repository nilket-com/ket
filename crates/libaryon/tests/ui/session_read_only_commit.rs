use libaryon::core::{ReadOnly, SavedSession};

fn exercise(session: &mut SavedSession<ReadOnly>) {
	let _ = session.commit();
}

fn main() {
	let _ = exercise;
}
