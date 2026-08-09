use libaryon::core::{ReadOnly, SavedSession};
use libaryon::engine::{EngineConfig, EngineHandle};

fn exercise(session: SavedSession<ReadOnly>) {
    let _ = EngineHandle::spawn_saved(EngineConfig::default(), session);
}

fn main() {
    let _ = exercise;
}
