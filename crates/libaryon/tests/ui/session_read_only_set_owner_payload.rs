use libaryon::core::{OwnerId, ReadOnly, SavedSession};

fn exercise(session: &mut SavedSession<ReadOnly>, owner: OwnerId) {
	session.set_owner_payload(owner, 1, vec![1, 2, 3]);
}

fn main() {
	let _ = exercise;
}
