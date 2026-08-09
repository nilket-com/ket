use libaryon::core::{SavedSession, Writable};

fn exercise(session: &mut SavedSession<Writable>) {
	let _ = session.commit();
}

fn main() {
	let _ = exercise;
}
