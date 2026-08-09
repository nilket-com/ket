use libaryon::core::{OwnerId, SavedSession, Writable};

fn exercise(session: &mut SavedSession<Writable>, owner: OwnerId) {
	session.set_owner_payload(owner, 1, vec![1, 2, 3]);
}

fn main() {
	let _ = exercise;
}
