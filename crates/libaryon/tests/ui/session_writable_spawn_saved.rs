use libaryon::core::{SavedSession, Writable};
use libaryon::engine::{EngineConfig, EngineHandle};

fn exercise(session: SavedSession<Writable>) {
    let _ = EngineHandle::spawn_saved(EngineConfig::default(), session);
}

fn main() {
    let _ = exercise;
}
