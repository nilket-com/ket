use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::distributed::identity::FrameByteLen;

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ByteCount(pub u64);

impl ByteCount {
	pub const ZERO: Self = Self(0);

	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub fn from_usize(value: usize) -> Self {
		Self(u64::try_from(value).expect("usize byte count fits u64"))
	}

	pub const fn get(self) -> u64 {
		self.0
	}

	pub const fn saturating_sub(self, other: Self) -> Self {
		Self(self.0.saturating_sub(other.0))
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct DurationMillis(pub u32);

impl DurationMillis {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetDimension {
	Memory,
	ShuffleBytes,
	SpillBytes,
	BytesInFlightPerQuery,
	BytesInFlightPerWorkerStream,
	FrameSize,
}

impl BudgetDimension {
	pub const fn retryable(self) -> bool {
		matches!(self, Self::Memory | Self::ShuffleBytes)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBudget {
	pub bytes_in_flight_per_query: ByteCount,
	pub bytes_in_flight_per_worker_stream: ByteCount,
	pub memory_per_task: ByteCount,
	pub shuffle_bytes_published_per_task: ByteCount,
	pub spill_bytes_reserved_per_query: ByteCount,
	pub max_arrow_ipc_frame: FrameByteLen,
	pub cancellation_grace: DurationMillis,
}

impl QueryBudget {
	pub const fn test_default() -> Self {
		Self {
			bytes_in_flight_per_query: ByteCount::new(64 * 1024 * 1024),
			bytes_in_flight_per_worker_stream: ByteCount::new(16 * 1024 * 1024),
			memory_per_task: ByteCount::new(64 * 1024 * 1024),
			shuffle_bytes_published_per_task: ByteCount::new(64 * 1024 * 1024),
			spill_bytes_reserved_per_query: ByteCount::new(512 * 1024 * 1024),
			max_arrow_ipc_frame: FrameByteLen::from_u32(16 * 1024 * 1024),
			cancellation_grace: DurationMillis::new(1_000),
		}
	}
}

impl Default for QueryBudget {
	fn default() -> Self {
		Self::test_default()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBudget {
	pub memory: ByteCount,
	pub shuffle_bytes: ByteCount,
	pub max_arrow_ipc_frame: FrameByteLen,
}

impl TaskBudget {
	pub const fn from_query_budget(query: QueryBudget) -> Self {
		Self {
			memory: query.memory_per_task,
			shuffle_bytes: query.shuffle_bytes_published_per_task,
			max_arrow_ipc_frame: query.max_arrow_ipc_frame,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetRejection {
	pub dimension: BudgetDimension,
	pub requested: ByteCount,
	pub available: ByteCount,
}

#[derive(Debug, Clone)]
pub struct BudgetTracker {
	inner: Arc<Mutex<BudgetState>>,
}

#[derive(Debug)]
struct BudgetState {
	budget: QueryBudget,
	memory_used: ByteCount,
	shuffle_used: ByteCount,
	spill_used: ByteCount,
	bytes_in_flight_per_query_used: ByteCount,
	bytes_in_flight_per_worker_stream_used: ByteCount,
}

impl BudgetTracker {
	pub fn new(budget: QueryBudget) -> Self {
		Self {
			inner: Arc::new(Mutex::new(BudgetState {
				budget,
				memory_used: ByteCount::ZERO,
				shuffle_used: ByteCount::ZERO,
				spill_used: ByteCount::ZERO,
				bytes_in_flight_per_query_used: ByteCount::ZERO,
				bytes_in_flight_per_worker_stream_used: ByteCount::ZERO,
			})),
		}
	}

	pub fn reserve(
		&self,
		dimension: BudgetDimension,
		amount: ByteCount,
	) -> Result<BudgetReservation, BudgetRejection> {
		let mut state = self.inner.lock().expect("budget tracker poisoned");
		let used = state.used_for(dimension);
		let limit = state.limit_for(dimension);
		let available = limit.saturating_sub(used);
		if amount > available {
			return Err(BudgetRejection {
				dimension,
				requested: amount,
				available,
			});
		}

		state.add_used(dimension, amount);
		Ok(BudgetReservation {
			tracker: self.clone(),
			dimension,
			amount,
			released: false,
		})
	}

	pub fn used(&self, dimension: BudgetDimension) -> ByteCount {
		self.inner
			.lock()
			.expect("budget tracker poisoned")
			.used_for(dimension)
	}

	fn release(&self, dimension: BudgetDimension, amount: ByteCount) {
		self.inner
			.lock()
			.expect("budget tracker poisoned")
			.sub_used(dimension, amount);
	}
}

#[derive(Debug)]
pub struct BudgetReservation {
	tracker: BudgetTracker,
	dimension: BudgetDimension,
	amount: ByteCount,
	released: bool,
}

impl BudgetReservation {
	pub fn release(mut self) {
		self.release_once();
	}

	fn release_once(&mut self) {
		if !self.released {
			self.tracker.release(self.dimension, self.amount);
			self.released = true;
		}
	}
}

impl Drop for BudgetReservation {
	fn drop(&mut self) {
		self.release_once();
	}
}

impl BudgetState {
	fn limit_for(&self, dimension: BudgetDimension) -> ByteCount {
		match dimension {
			BudgetDimension::Memory => self.budget.memory_per_task,
			BudgetDimension::ShuffleBytes => self.budget.shuffle_bytes_published_per_task,
			BudgetDimension::SpillBytes => self.budget.spill_bytes_reserved_per_query,
			BudgetDimension::BytesInFlightPerQuery => self.budget.bytes_in_flight_per_query,
			BudgetDimension::BytesInFlightPerWorkerStream => {
				self.budget.bytes_in_flight_per_worker_stream
			}
			BudgetDimension::FrameSize => {
				ByteCount::new(u64::from(self.budget.max_arrow_ipc_frame.get()))
			}
		}
	}

	fn used_for(&self, dimension: BudgetDimension) -> ByteCount {
		match dimension {
			BudgetDimension::Memory => self.memory_used,
			BudgetDimension::ShuffleBytes => self.shuffle_used,
			BudgetDimension::SpillBytes => self.spill_used,
			BudgetDimension::BytesInFlightPerQuery => self.bytes_in_flight_per_query_used,
			BudgetDimension::BytesInFlightPerWorkerStream => {
				self.bytes_in_flight_per_worker_stream_used
			}
			BudgetDimension::FrameSize => ByteCount::ZERO,
		}
	}

	fn add_used(&mut self, dimension: BudgetDimension, amount: ByteCount) {
		if dimension == BudgetDimension::FrameSize {
			return;
		}
		let target = self.used_mut(dimension);
		*target = ByteCount::new(target.get() + amount.get());
	}

	fn sub_used(&mut self, dimension: BudgetDimension, amount: ByteCount) {
		if dimension == BudgetDimension::FrameSize {
			return;
		}
		let target = self.used_mut(dimension);
		*target = target.saturating_sub(amount);
	}

	fn used_mut(&mut self, dimension: BudgetDimension) -> &mut ByteCount {
		match dimension {
			BudgetDimension::Memory => &mut self.memory_used,
			BudgetDimension::ShuffleBytes => &mut self.shuffle_used,
			BudgetDimension::SpillBytes => &mut self.spill_used,
			BudgetDimension::BytesInFlightPerQuery => &mut self.bytes_in_flight_per_query_used,
			BudgetDimension::BytesInFlightPerWorkerStream => {
				&mut self.bytes_in_flight_per_worker_stream_used
			}
			BudgetDimension::FrameSize => unreachable!("FrameSize is not a retained reservation"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reservation_releases_on_drop() {
		let tracker = BudgetTracker::new(QueryBudget {
			memory_per_task: ByteCount::new(10),
			..QueryBudget::test_default()
		});

		let reservation = tracker
			.reserve(BudgetDimension::Memory, ByteCount::new(7))
			.expect("reserve");
		assert_eq!(tracker.used(BudgetDimension::Memory), ByteCount::new(7));

		drop(reservation);

		assert_eq!(tracker.used(BudgetDimension::Memory), ByteCount::ZERO);
	}

	#[test]
	fn reservation_rejects_over_limit() {
		let tracker = BudgetTracker::new(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(5),
			..QueryBudget::test_default()
		});

		let rejection = tracker
			.reserve(BudgetDimension::ShuffleBytes, ByteCount::new(6))
			.expect_err("over limit");

		assert_eq!(
			rejection,
			BudgetRejection {
				dimension: BudgetDimension::ShuffleBytes,
				requested: ByteCount::new(6),
				available: ByteCount::new(5),
			}
		);
	}
}
