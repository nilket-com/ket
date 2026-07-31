use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::distributed::budget::{ByteCount, DurationMillis};
use crate::distributed::identity::{LeaseEpoch, WorkerId};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerEndpoint(pub String);

impl WorkerEndpoint {
	pub fn new(value: impl Into<String>) -> Self {
		Self(value.into())
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn into_inner(self) -> String {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SimdLaneCount(pub u16);

impl SimdLaneCount {
	pub const fn new(value: u16) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u16 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ActiveTaskCount(pub u32);

impl ActiveTaskCount {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct UnixMillis(pub u64);

impl UnixMillis {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
	pub advertised_memory: ByteCount,
	pub simd_width: SimdLaneCount,
	pub spill_capacity_reserved: ByteCount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerResourceState {
	pub in_flight: ByteCount,
	pub active_tasks: ActiveTaskCount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
	pub last_heartbeat: UnixMillis,
	pub epoch: LeaseEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerStatus {
	Live,
	Suspect,
	Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerEntry {
	pub worker_id: WorkerId,
	pub endpoint: WorkerEndpoint,
	pub lease: WorkerLease,
	pub capabilities: WorkerCapabilities,
	pub resources: WorkerResourceState,
	pub status: WorkerStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlacementHints;

pub trait WorkerRegistry: Send + Sync {
	fn list(&self) -> Vec<WorkerEntry>;
	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry>;
	fn select_for_attempt(&self, hints: &PlacementHints) -> Option<WorkerId>;
	fn mark_lost(&self, worker_id: WorkerId);
	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease);
	fn expire_heartbeats(
		&self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerId>;
}

impl<Registry> WorkerRegistry for Arc<Registry>
where
	Registry: WorkerRegistry + ?Sized,
{
	fn list(&self) -> Vec<WorkerEntry> {
		self.as_ref().list()
	}

	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry> {
		self.as_ref().resolve(worker_id)
	}

	fn select_for_attempt(&self, hints: &PlacementHints) -> Option<WorkerId> {
		self.as_ref().select_for_attempt(hints)
	}

	fn mark_lost(&self, worker_id: WorkerId) {
		self.as_ref().mark_lost(worker_id);
	}

	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease) {
		self.as_ref().heartbeat(worker_id, lease);
	}

	fn expire_heartbeats(
		&self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerId> {
		self.as_ref()
			.expire_heartbeats(now, suspect_after, lost_after)
	}
}

#[derive(Debug)]
pub struct StaticWorkerRegistry {
	workers: RwLock<HashMap<WorkerId, WorkerEntry>>,
}

impl StaticWorkerRegistry {
	pub fn new(workers: impl IntoIterator<Item = WorkerEntry>) -> Self {
		Self {
			workers: RwLock::new(
				workers
					.into_iter()
					.map(|worker| (worker.worker_id, worker))
					.collect(),
			),
		}
	}
}

impl WorkerRegistry for StaticWorkerRegistry {
	fn list(&self) -> Vec<WorkerEntry> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.values()
			.cloned()
			.collect()
	}

	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.get(&worker_id)
			.cloned()
	}

	fn select_for_attempt(&self, _hints: &PlacementHints) -> Option<WorkerId> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.values()
			.filter(|worker| worker.status == WorkerStatus::Live)
			.min_by_key(|worker| (worker.resources.active_tasks, worker.resources.in_flight))
			.map(|worker| worker.worker_id)
	}

	fn mark_lost(&self, worker_id: WorkerId) {
		if let Some(worker) = self
			.workers
			.write()
			.expect("worker registry poisoned")
			.get_mut(&worker_id)
		{
			worker.status = WorkerStatus::Lost;
			worker.lease.epoch = worker.lease.epoch.next();
		}
	}

	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease) {
		if let Some(worker) = self
			.workers
			.write()
			.expect("worker registry poisoned")
			.get_mut(&worker_id)
		{
			worker.lease = lease;
			if worker.status != WorkerStatus::Lost {
				worker.status = WorkerStatus::Live;
			}
		}
	}

	fn expire_heartbeats(
		&self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerId> {
		let mut lost_workers = Vec::new();
		for worker in self
			.workers
			.write()
			.expect("worker registry poisoned")
			.values_mut()
		{
			if worker.status == WorkerStatus::Lost {
				continue;
			}

			let age = now.get().saturating_sub(worker.lease.last_heartbeat.get());
			if age >= u64::from(lost_after.get()) {
				worker.status = WorkerStatus::Lost;
				worker.lease.epoch = worker.lease.epoch.next();
				lost_workers.push(worker.worker_id);
			} else if age >= u64::from(suspect_after.get()) {
				worker.status = WorkerStatus::Suspect;
			}
		}

		lost_workers
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn worker(
		worker_id: WorkerId,
		active_tasks: ActiveTaskCount,
		in_flight: ByteCount,
		status: WorkerStatus,
	) -> WorkerEntry {
		WorkerEntry {
			worker_id,
			endpoint: WorkerEndpoint::new("127.0.0.1:9000"),
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(10),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(4096),
			},
			resources: WorkerResourceState {
				in_flight,
				active_tasks,
			},
			status,
		}
	}

	#[test]
	fn static_registry_selects_least_loaded_live_worker() {
		let busy = worker(
			WorkerId::from_u128(1),
			ActiveTaskCount::new(4),
			ByteCount::new(1),
			WorkerStatus::Live,
		);
		let idle = worker(
			WorkerId::from_u128(2),
			ActiveTaskCount::new(1),
			ByteCount::new(64),
			WorkerStatus::Live,
		);
		let lost = worker(
			WorkerId::from_u128(3),
			ActiveTaskCount::new(0),
			ByteCount::ZERO,
			WorkerStatus::Lost,
		);
		let registry = StaticWorkerRegistry::new([busy, idle.clone(), lost]);

		assert_eq!(
			registry.select_for_attempt(&PlacementHints),
			Some(idle.worker_id)
		);
	}

	#[test]
	fn mark_lost_bumps_lease_epoch() {
		let worker_id = WorkerId::from_u128(1);
		let registry = StaticWorkerRegistry::new([worker(
			worker_id,
			ActiveTaskCount::default(),
			ByteCount::ZERO,
			WorkerStatus::Live,
		)]);

		registry.mark_lost(worker_id);

		let entry = registry.resolve(worker_id).expect("worker exists");
		assert_eq!(entry.status, WorkerStatus::Lost);
		assert_eq!(entry.lease.epoch, LeaseEpoch::new(2));
	}

	#[test]
	fn heartbeat_expiry_marks_suspect_then_lost_without_selecting_lost_workers() {
		let worker_id = WorkerId::from_u128(1);
		let registry = StaticWorkerRegistry::new([worker(
			worker_id,
			ActiveTaskCount::default(),
			ByteCount::ZERO,
			WorkerStatus::Live,
		)]);

		assert!(
			registry
				.expire_heartbeats(
					UnixMillis::new(50),
					DurationMillis::new(100),
					DurationMillis::new(500),
				)
				.is_empty()
		);
		assert_eq!(
			registry.resolve(worker_id).expect("worker").status,
			WorkerStatus::Live
		);

		assert!(
			registry
				.expire_heartbeats(
					UnixMillis::new(150),
					DurationMillis::new(100),
					DurationMillis::new(500),
				)
				.is_empty()
		);
		assert_eq!(
			registry.resolve(worker_id).expect("worker").status,
			WorkerStatus::Suspect
		);

		let lost = registry.expire_heartbeats(
			UnixMillis::new(600),
			DurationMillis::new(100),
			DurationMillis::new(500),
		);
		assert_eq!(lost, vec![worker_id]);
		let entry = registry.resolve(worker_id).expect("worker");
		assert_eq!(entry.status, WorkerStatus::Lost);
		assert_eq!(entry.lease.epoch, LeaseEpoch::new(2));
		assert_eq!(registry.select_for_attempt(&PlacementHints), None);
	}
}
