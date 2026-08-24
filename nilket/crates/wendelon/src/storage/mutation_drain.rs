//! ADR 0095 node-local mutation drain authority.

use crate::integration::compute::DropOperationId;
use crate::schema::{TableObjectId, TenantId};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Notify;

type ObjectKey = (TenantId, TableObjectId);

#[derive(Default)]
struct ObjectDrain {
	closed_by: Option<DropOperationId>,
	in_flight_by_generation: BTreeMap<u64, usize>,
}

impl ObjectDrain {
	fn in_flight(&self) -> usize {
		self.in_flight_by_generation.values().sum()
	}
}

#[derive(Default)]
struct RegistryState {
	startup_reconciled: bool,
	objects: BTreeMap<ObjectKey, ObjectDrain>,
}

/// One process-local authority shared by Raft apply and every local data path.
///
/// The mutex is the lifecycle linearization point. A mutation increments while
/// holding it; committed DROP prepare closes the object while holding the same
/// mutex. Therefore an admission is either counted before closure or refused
/// after closure—there is no admit-after-check window.
pub(crate) struct MutationDrainRegistry {
	state: Mutex<RegistryState>,
	drained: Notify,
}

impl MutationDrainRegistry {
	pub(crate) fn fresh_reconciled() -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RegistryState {
				startup_reconciled: true,
				objects: BTreeMap::new(),
			}),
			drained: Notify::new(),
		})
	}

	pub(crate) fn recovering() -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RegistryState::default()),
			drained: Notify::new(),
		})
	}

	pub(crate) fn complete_startup_reconciliation(&self) {
		self.state
			.lock()
			.expect("mutation drain registry poisoned")
			.startup_reconciled = true;
		self.drained.notify_waiters();
	}

	pub(crate) fn admit(
		self: &Arc<Self>,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		schema_generation: u64,
	) -> Result<MutationDrainGuard, MutationDrainError> {
		let key = (tenant_id, table_object_id);
		let mut state = self.state.lock().expect("mutation drain registry poisoned");
		if !state.startup_reconciled {
			return Err(MutationDrainError::StartupReconciliationPending);
		}
		let object = state.objects.entry(key).or_default();
		if let Some(operation_id) = object.closed_by {
			return Err(MutationDrainError::ObjectClosed(operation_id));
		}
		*object
			.in_flight_by_generation
			.entry(schema_generation)
			.or_default() += 1;
		drop(state);
		Ok(MutationDrainGuard {
			_inner: Arc::new(MutationDrainGuardInner {
				registry: Arc::downgrade(self),
				key,
				schema_generation,
			}),
		})
	}

	/// Close from committed state-machine apply. Replays of the same operation
	/// are idempotent; a conflicting operation is a state-machine invariant
	/// violation and is surfaced to tests through the returned error.
	pub(crate) fn close_committed(
		&self,
		operation_id: DropOperationId,
	) -> Result<(), MutationDrainError> {
		let key = (operation_id.tenant_id, operation_id.table_object_id);
		let mut state = self.state.lock().expect("mutation drain registry poisoned");
		let object = state.objects.entry(key).or_default();
		match object.closed_by {
			None => object.closed_by = Some(operation_id),
			Some(existing) if existing == operation_id => {}
			Some(existing) => {
				return Err(MutationDrainError::ConflictingClosure {
					existing,
					observed: operation_id,
				});
			}
		}
		drop(state);
		self.drained.notify_waiters();
		Ok(())
	}

	pub(crate) async fn wait_until_drained(
		&self,
		operation_id: DropOperationId,
	) -> Result<(), MutationDrainError> {
		loop {
			let notified = self.drained.notified();
			{
				let state = self.state.lock().expect("mutation drain registry poisoned");
				if !state.startup_reconciled {
					// Wait for startup reconciliation to register every resumable
					// byte-mutating task before interpreting zero as drained.
				} else {
					let key = (operation_id.tenant_id, operation_id.table_object_id);
					let object = state
						.objects
						.get(&key)
						.ok_or(MutationDrainError::ObjectNotClosed)?;
					if object.closed_by != Some(operation_id) {
						return Err(MutationDrainError::ObjectNotClosed);
					}
					if object.in_flight() == 0 {
						return Ok(());
					}
				}
			}
			notified.await;
		}
	}

	#[cfg(test)]
	pub(crate) fn in_flight_for(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> usize {
		self.state
			.lock()
			.expect("mutation drain registry poisoned")
			.objects
			.get(&(tenant_id, table_object_id))
			.map(ObjectDrain::in_flight)
			.unwrap_or(0)
	}

	fn release(&self, key: ObjectKey, schema_generation: u64) {
		let mut state = self.state.lock().expect("mutation drain registry poisoned");
		let object = state
			.objects
			.get_mut(&key)
			.expect("a mutation guard must retain its object entry");
		let count = object
			.in_flight_by_generation
			.get_mut(&schema_generation)
			.expect("a mutation guard must retain its generation entry");
		*count = count
			.checked_sub(1)
			.expect("mutation drain count underflow");
		if *count == 0 {
			object.in_flight_by_generation.remove(&schema_generation);
		}
		drop(state);
		self.drained.notify_waiters();
	}
}

/// Cloneable because a leader request can spawn durable local work. The
/// registry count is released only when the final clone disappears.
#[derive(Clone)]
pub(crate) struct MutationDrainGuard {
	_inner: Arc<MutationDrainGuardInner>,
}

struct MutationDrainGuardInner {
	registry: Weak<MutationDrainRegistry>,
	key: ObjectKey,
	schema_generation: u64,
}

impl Drop for MutationDrainGuardInner {
	fn drop(&mut self) {
		if let Some(registry) = self.registry.upgrade() {
			registry.release(self.key, self.schema_generation);
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationDrainError {
	StartupReconciliationPending,
	ObjectClosed(DropOperationId),
	ObjectNotClosed,
	ConflictingClosure {
		existing: DropOperationId,
		observed: DropOperationId,
	},
}

impl fmt::Display for MutationDrainError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::StartupReconciliationPending => {
				write!(f, "mutation admission waits for startup reconciliation")
			}
			Self::ObjectClosed(operation_id) => write!(
				f,
				"table object is closed by drop operation {operation_id:?}"
			),
			Self::ObjectNotClosed => write!(f, "table object is not closed for mutation drain"),
			Self::ConflictingClosure { existing, observed } => write!(
				f,
				"mutation drain closure conflict: existing {existing:?}, observed {observed:?}"
			),
		}
	}
}

impl std::error::Error for MutationDrainError {}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::integration::compute::SchemaGeneration;

	fn operation() -> DropOperationId {
		DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([9; 16]),
			schema_generation: SchemaGeneration::new(3),
		}
	}

	#[tokio::test]
	async fn adr0095_close_waits_for_preexisting_work_and_refuses_later_admission() {
		let registry = MutationDrainRegistry::fresh_reconciled();
		let operation_id = operation();
		let guard = registry
			.admit(operation_id.tenant_id, operation_id.table_object_id, 3)
			.unwrap();
		assert_eq!(
			registry.in_flight_for(operation_id.tenant_id, operation_id.table_object_id),
			1
		);
		registry.close_committed(operation_id).unwrap();
		assert!(matches!(
			registry.admit(operation_id.tenant_id, operation_id.table_object_id, 3),
			Err(MutationDrainError::ObjectClosed(existing)) if existing == operation_id
		));

		let waiting = {
			let registry = registry.clone();
			tokio::spawn(async move { registry.wait_until_drained(operation_id).await })
		};
		tokio::task::yield_now().await;
		assert!(!waiting.is_finished());
		drop(guard);
		waiting.await.unwrap().unwrap();
	}

	#[tokio::test]
	async fn adr0095_startup_barrier_never_interprets_unreconciled_zero_as_drained() {
		let registry = MutationDrainRegistry::recovering();
		let operation_id = operation();
		registry.close_committed(operation_id).unwrap();
		let waiting = {
			let registry = registry.clone();
			tokio::spawn(async move { registry.wait_until_drained(operation_id).await })
		};
		tokio::task::yield_now().await;
		assert!(!waiting.is_finished());
		registry.complete_startup_reconciliation();
		waiting.await.unwrap().unwrap();
	}

	#[test]
	#[ignore = "ADR 0095 implementation-receipt timing sample"]
	fn adr0095_mutation_admission_hot_path_receipt() {
		const ITERATIONS: u32 = 100_000;
		let registry = MutationDrainRegistry::fresh_reconciled();
		let operation_id = operation();
		let started = std::time::Instant::now();
		for _ in 0..ITERATIONS {
			let guard = registry
				.admit(operation_id.tenant_id, operation_id.table_object_id, 3)
				.unwrap();
			drop(guard);
		}
		let elapsed = started.elapsed();
		assert_eq!(
			registry.in_flight_for(operation_id.tenant_id, operation_id.table_object_id),
			0
		);
		eprintln!(
			"ADR0095 mutation admission receipt: iterations={ITERATIONS} elapsed_us={} ns_per_admit_release={}",
			elapsed.as_micros(),
			elapsed.as_nanos() / u128::from(ITERATIONS)
		);
	}
}
