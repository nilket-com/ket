//! Per-manager accounting for unresolved counter slabs and retained sidecars.
use super::*;

/// Counts each held slab once per manager, regardless of its number of frames.
#[derive(Default)]
pub(super) struct CounterPendingGuardState {
	entries: std::collections::HashMap<PendingCounterFrame, Arc<AppendInFlight>>,
	pub(super) per_slab: std::collections::HashMap<SlabId, usize>,
}
#[derive(Default)]
pub(super) struct CounterPendingGuards {
	pub(super) state: std::sync::Mutex<CounterPendingGuardState>,
}
impl CounterPendingGuards {
	pub(super) fn insert(&self, key: PendingCounterFrame, guard: Arc<AppendInFlight>) {
		let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
		if state.entries.insert(key, guard).is_none() {
			let count = state.per_slab.entry(key.6).or_default();
			if *count == 0 {
				metrics().storage_counter_unresolved_slabs.add(1);
			}
			*count += 1;
		}
	}
	fn decrement_slab(state: &mut CounterPendingGuardState, slab_id: SlabId) {
		let count = state
			.per_slab
			.get_mut(&slab_id)
			.expect("counter slab guard count");
		*count -= 1;
		if *count == 0 {
			state.per_slab.remove(&slab_id);
			metrics().storage_counter_unresolved_slabs.sub(1);
		}
	}
	pub(super) fn remove(&self, key: &PendingCounterFrame) {
		let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
		if state.entries.remove(key).is_some() {
			Self::decrement_slab(&mut state, key.6);
		}
	}
	pub(super) fn retain(
		&self,
		mut f: impl FnMut(&PendingCounterFrame, &mut Arc<AppendInFlight>) -> bool,
	) {
		let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
		let mut removed = Vec::new();
		state.entries.retain(|key, value| {
			let keep = f(key, value);
			if !keep {
				removed.push(key.6);
			}
			keep
		});
		for slab_id in removed {
			Self::decrement_slab(&mut state, slab_id);
		}
	}
}
impl Drop for CounterPendingGuards {
	fn drop(&mut self) {
		let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
		metrics()
			.storage_counter_unresolved_slabs
			.sub(state.per_slab.len() as u64);
	}
}
/// Per-manager contribution to process-wide retained-sidecar gauges.
#[derive(Default)]
pub(super) struct RetainedSecondarySidecars {
	files: std::sync::Mutex<std::collections::HashMap<PathBuf, u64>>,
}
impl RetainedSecondarySidecars {
	pub(super) fn record(&self, path: PathBuf, bytes: u64) {
		let mut files = self.files.lock().unwrap_or_else(|e| e.into_inner());
		if let std::collections::hash_map::Entry::Vacant(entry) = files.entry(path) {
			entry.insert(bytes);
			metrics().storage_retained_secondary_sidecar_files.add(1);
			metrics()
				.storage_retained_secondary_sidecar_bytes
				.add(bytes);
		}
	}
}
impl Drop for RetainedSecondarySidecars {
	fn drop(&mut self) {
		let files = self.files.lock().unwrap_or_else(|e| e.into_inner());
		metrics()
			.storage_retained_secondary_sidecar_files
			.sub(files.len() as u64);
		metrics()
			.storage_retained_secondary_sidecar_bytes
			.sub(files.values().copied().fold(0_u64, u64::saturating_add));
	}
}
