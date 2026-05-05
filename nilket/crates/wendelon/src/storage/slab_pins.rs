use crate::schema::SlabId;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
	SlabAlreadyDeletePending { slab_id: SlabId },
}

#[derive(Debug, Default)]
struct PinRecord {
	pin_count: u64,
	delete_pending: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SlabPinTable {
	inner: Arc<Mutex<BTreeMap<SlabId, PinRecord>>>,
}

impl SlabPinTable {
	pub fn acquire(&self, slabs: &[SlabId]) -> Result<SlabPinGuard, AcquireError> {
		let mut unique = BTreeSet::new();
		unique.extend(slabs.iter().copied());

		let mut inner = self.inner.lock().expect("slab pin table mutex poisoned");
		if let Some(slab_id) = unique.iter().copied().find(|slab_id| {
			inner
				.get(slab_id)
				.is_some_and(|record| record.delete_pending)
		}) {
			return Err(AcquireError::SlabAlreadyDeletePending { slab_id });
		}

		for slab_id in &unique {
			inner.entry(*slab_id).or_default().pin_count += 1;
		}

		Ok(SlabPinGuard {
			inner: self.inner.clone(),
			slabs: unique.into_iter().collect(),
		})
	}

	pub fn pin_count(&self, slab_id: SlabId) -> u64 {
		self.inner
			.lock()
			.expect("slab pin table mutex poisoned")
			.get(&slab_id)
			.map(|record| record.pin_count)
			.unwrap_or(0)
	}

	pub fn mark_delete_pending(&self, slab_id: SlabId) -> bool {
		let mut inner = self.inner.lock().expect("slab pin table mutex poisoned");
		let record = inner.entry(slab_id).or_default();
		if record.pin_count > 0 {
			return false;
		}
		record.delete_pending = true;
		true
	}

	pub fn force_delete_pending(&self, slab_id: SlabId) {
		let mut inner = self.inner.lock().expect("slab pin table mutex poisoned");
		inner.entry(slab_id).or_default().delete_pending = true;
	}

	pub fn clear_delete_pending(&self, slab_id: SlabId) {
		if let Some(record) = self
			.inner
			.lock()
			.expect("slab pin table mutex poisoned")
			.get_mut(&slab_id)
		{
			record.delete_pending = false;
		}
	}

	pub fn is_delete_pending(&self, slab_id: SlabId) -> bool {
		self.inner
			.lock()
			.expect("slab pin table mutex poisoned")
			.get(&slab_id)
			.is_some_and(|record| record.delete_pending)
	}
}

#[derive(Debug)]
pub struct SlabPinGuard {
	inner: Arc<Mutex<BTreeMap<SlabId, PinRecord>>>,
	slabs: Vec<SlabId>,
}

impl SlabPinGuard {
	pub fn slabs(&self) -> &[SlabId] {
		&self.slabs
	}
}

impl Drop for SlabPinGuard {
	fn drop(&mut self) {
		let mut inner = self.inner.lock().expect("slab pin table mutex poisoned");
		for slab_id in &self.slabs {
			let Some(record) = inner.get_mut(slab_id) else {
				continue;
			};
			record.pin_count = record.pin_count.saturating_sub(1);
			if record.pin_count == 0 && !record.delete_pending {
				inner.remove(slab_id);
			}
		}
	}
}
