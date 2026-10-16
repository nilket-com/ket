//! Eligible counter values, separate from the raw tentative index. No wire or disk format.
use crate::schema::{HybridTimestamp, NodeId};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterVisibilityError {
	Conflict,
	Poisoned,
	Unsupported,
}
impl std::fmt::Display for CounterVisibilityError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "counter eligibility {self:?}")
	}
}
impl std::error::Error for CounterVisibilityError {}
impl CounterVisibilityError {
	pub(crate) fn io(self) -> std::io::Error {
		std::io::Error::other(self)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterVersion {
	pub value: u64,
	pub hlc: HybridTimestamp,
	/// Legacy storage has no persisted origin. Unknown-origin ties retain F-CTR-1b.
	pub origin: Option<NodeId>,
}
impl CounterVersion {
	fn stamp(self) -> (u64, u32, u32) {
		(self.hlc.physical_ms, self.hlc.logical, self.hlc.node_id)
	}
	fn order(self) -> ((u64, u32, u32), Option<u32>) {
		(self.stamp(), self.origin.map(|origin| origin.id))
	}
}

pub(crate) fn winner(
	a: Option<CounterVersion>,
	b: Option<CounterVersion>,
) -> Result<Option<CounterVersion>, CounterVisibilityError> {
	match (a, b) {
		(None, other) | (other, None) => Ok(other),
		(Some(a), Some(b)) if a.stamp() == b.stamp() => match (a.origin, b.origin) {
			(Some(x), Some(y)) if x.id != y.id => Ok(Some(if x.id > y.id { a } else { b })),
			_ if a.value != b.value => Err(CounterVisibilityError::Conflict),
			_ => Ok(Some(CounterVersion {
				origin: a.origin.or(b.origin),
				..a
			})),
		},
		(Some(a), Some(b)) => Ok(Some(if a.order() > b.order() { a } else { b })),
	}
}
struct Seen {
	version: CounterVersion,
	pending: usize,
	published: bool,
}
#[derive(Default)]
struct Cell {
	version: Option<CounterVersion>,
	seen: BTreeMap<((u64, u32, u32), Option<u32>), Seen>,
	error: Option<CounterVisibilityError>,
}
impl Cell {
	fn prune(&mut self) {
		let floor = self.version.map(CounterVersion::stamp);
		self.seen.retain(|_, seen| {
			seen.pending > 0
				|| (seen.published && floor.is_some_and(|floor| seen.version.stamp() >= floor))
		});
	}
}
/// Owns one reference to a collision reservation before durable publication.
pub(crate) struct Reservation {
	cell: Arc<Mutex<Cell>>,
	version: CounterVersion,
	pending: bool,
}
impl Reservation {
	pub(crate) fn publish(mut self) -> Result<(), CounterVisibilityError> {
		let mut cell = self
			.cell
			.lock()
			.map_err(|_| CounterVisibilityError::Poisoned)?;
		if cell.error.is_some() {
			cell.error = Some(CounterVisibilityError::Poisoned);
			return Err(CounterVisibilityError::Poisoned);
		}
		let version = match winner(cell.version, Some(self.version)) {
			Ok(version) => version,
			Err(_) => {
				cell.error = Some(CounterVisibilityError::Poisoned);
				return Err(CounterVisibilityError::Poisoned);
			}
		};
		let Some(seen) = cell.seen.get_mut(&self.version.order()) else {
			cell.error = Some(CounterVisibilityError::Poisoned);
			return Err(CounterVisibilityError::Poisoned);
		};
		seen.pending -= 1;
		seen.published = true;
		self.pending = false;
		cell.version = version;
		cell.prune();
		Ok(())
	}
}
impl Drop for Reservation {
	fn drop(&mut self) {
		if !self.pending {
			return;
		}
		if let Ok(mut cell) = self.cell.lock() {
			if let Some(seen) = cell.seen.get_mut(&self.version.order()) {
				seen.pending -= 1;
			}
			cell.prune();
		}
	}
}
pub(crate) struct EligibleCells<K>(Mutex<HashMap<K, Arc<Mutex<Cell>>>>);
impl<K> Default for EligibleCells<K> {
	fn default() -> Self {
		Self(Mutex::new(HashMap::new()))
	}
}
impl<K: Eq + Hash + Clone> EligibleCells<K> {
	fn cell(&self, key: &K) -> Result<Arc<Mutex<Cell>>, CounterVisibilityError> {
		let mut cells = self
			.0
			.lock()
			.map_err(|_| CounterVisibilityError::Poisoned)?;
		Ok(cells.entry(key.clone()).or_default().clone())
	}
	pub(crate) fn read(&self, key: &K) -> Result<Option<CounterVersion>, CounterVisibilityError> {
		let cell = self
			.0
			.lock()
			.map_err(|_| CounterVisibilityError::Poisoned)?
			.get(key)
			.cloned();
		let Some(cell) = cell else {
			return Ok(None);
		};
		let cell = cell.lock().map_err(|_| CounterVisibilityError::Poisoned)?;
		match cell.error {
			Some(error) => Err(error),
			None => Ok(cell.version),
		}
	}
	pub(crate) fn prepare(
		&self,
		key: &K,
		version: CounterVersion,
	) -> Result<Reservation, CounterVisibilityError> {
		let cell = self.cell(key)?;
		{
			let mut c = cell.lock().map_err(|_| CounterVisibilityError::Poisoned)?;
			if let Some(error) = c.error {
				return Err(error);
			}
			// Known origins order independently. Every unknown-origin tie must still
			// satisfy the legacy equal-value rule among retained versions. Historical
			// conflicts wholly below the current winner are deliberately not retained.
			let collision = c
				.seen
				.values()
				.filter(|seen| seen.version.stamp() == version.stamp())
				.find_map(|seen| winner(Some(seen.version), Some(version)).err());
			if let Some(error) = collision {
				c.error = Some(error);
				return Err(error);
			}
			// Collision reservation, not visibility. A failed pre-witness write never publishes it.
			let seen = c.seen.entry(version.order()).or_insert(Seen {
				version,
				pending: 0,
				published: false,
			});
			seen.pending = seen
				.pending
				.checked_add(1)
				.ok_or(CounterVisibilityError::Poisoned)?;
		}
		Ok(Reservation {
			cell,
			version,
			pending: true,
		})
	}
	#[cfg(test)]
	pub(super) fn seen_len(&self, key: &K) -> usize {
		self.cell(key).unwrap().lock().unwrap().seen.len()
	}
	pub(crate) fn refuse(&self, key: &K, error: CounterVisibilityError) {
		if let Ok(cell) = self.cell(key) {
			if let Ok(mut cell) = cell.lock() {
				cell.error = Some(error);
			}
		}
	}
	pub(crate) fn recover(&self, key: &K, version: CounterVersion) {
		// Conflicts poison this key rather than preventing unrelated keys from opening.
		if let Ok(reservation) = self.prepare(key, version) {
			let _ = reservation.publish();
		}
	}
}
