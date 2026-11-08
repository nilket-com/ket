use crate::consensus::monitor::PhysicalClock;
use crate::schema::{HybridTimestamp, NodeId};
use std::sync::{Arc, Mutex};

/// Shared Hybrid Logical Clock driver.
///
/// The driver is the single monotonic timestamp authority for one
/// node. Callers either mint a fresh local timestamp or merge a
/// remote/client timestamp into the local timeline; both paths keep
/// the returned HLC strictly monotonic even if the wall clock steps
/// backward.
pub struct HybridLogicalClockDriver {
	local_node: NodeId,
	clock: Arc<dyn PhysicalClock>,
	last: Mutex<HybridTimestamp>,
}

impl HybridLogicalClockDriver {
	pub fn new(local_node: NodeId, clock: Arc<dyn PhysicalClock>) -> Self {
		Self {
			local_node,
			clock,
			last: Mutex::new(HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: local_node.id,
			}),
		}
	}

	pub fn local_node(&self) -> NodeId {
		self.local_node
	}

	pub fn current(&self) -> HybridTimestamp {
		*self.last.lock().expect("hlc mutex poisoned")
	}

	pub fn now(&self) -> HybridTimestamp {
		self.merge(HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		})
	}

	/// Test-only: forget every timestamp issued so far, as a node whose clock
	/// is behind a version another clock wrote.
	#[cfg(test)]
	pub(crate) fn rewind_for_test(&self) {
		*self.last.lock().expect("hlc mutex poisoned") = HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		};
	}

	pub fn merge(&self, remote: HybridTimestamp) -> HybridTimestamp {
		let now_hlc = HybridTimestamp {
			physical_ms: self.clock.now_ms(),
			logical: 0,
			node_id: self.local_node.id,
		};
		let mut guard = self.last.lock().expect("hlc mutex poisoned");
		let base = (*guard).max(now_hlc).max(remote);
		let next = if base.logical == u32::MAX {
			HybridTimestamp {
				physical_ms: base.physical_ms.saturating_add(1),
				logical: 0,
				node_id: self.local_node.id,
			}
		} else {
			HybridTimestamp {
				physical_ms: base.physical_ms,
				logical: base.logical + 1,
				node_id: self.local_node.id,
			}
		};
		*guard = next;
		next
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::monitor::FakeClock;

	fn nid(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	#[test]
	fn now_is_monotonic_when_wall_clock_stalls() {
		let clock = Arc::new(FakeClock::new(1_000));
		let hlc = HybridLogicalClockDriver::new(nid(7), clock.clone());
		let first = hlc.now();
		let second = hlc.now();
		assert!(second > first);
		assert_eq!(second.physical_ms, first.physical_ms);
		assert_eq!(second.logical, first.logical + 1);
	}

	#[test]
	fn merge_remote_future_advances_physical_component() {
		let clock = Arc::new(FakeClock::new(1_000));
		let hlc = HybridLogicalClockDriver::new(nid(7), clock.clone());
		let merged = hlc.merge(HybridTimestamp {
			physical_ms: 5_000,
			logical: 4,
			node_id: 99,
		});
		assert_eq!(merged.physical_ms, 5_000);
		assert_eq!(merged.logical, 5);
		assert_eq!(merged.node_id, 7);
	}

	#[test]
	fn logical_overflow_rolls_physical_component() {
		let clock = Arc::new(FakeClock::new(42));
		let hlc = HybridLogicalClockDriver::new(nid(7), clock.clone());
		{
			let mut guard = hlc.last.lock().unwrap();
			*guard = HybridTimestamp {
				physical_ms: 42,
				logical: u32::MAX,
				node_id: 7,
			};
		}
		let next = hlc.now();
		assert_eq!(next.physical_ms, 43);
		assert_eq!(next.logical, 0);
		assert_eq!(next.node_id, 7);
	}
}
