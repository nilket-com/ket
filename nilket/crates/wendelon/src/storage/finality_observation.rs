//! ADR 629 P2 slice A: observation authority, not a mutation admission fence.
//!
//! This core is deliberately not attached to production mutation/metadata paths
//! yet. Coverage starts unavailable; later slices must reconcile every component.
//! The mutex is never held while calling external code, taking another lock or
//! awaiting. Work owners must keep clones inside detached and blocking jobs.
//! Nothing here cancels, refuses, retries or changes an ordinary mutation.
#![allow(dead_code)] // Dormant until the separately reviewed coverage slices.

use crate::schema::{TableObjectId, TenantId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

pub(crate) const MAX_OBSERVATION_SCOPES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TableScope {
	pub tenant: TenantId,
	pub object: TableObjectId,
	pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WorkScope {
	Node,
	Tenant(TenantId),
	Table(TableScope),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WorkKind {
	Mutation,
	Persistence,
	Finalization,
	Recovery,
	Metadata,
	StoragePublication,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CoverageComponent {
	MutationAdmission,
	DurableObligations,
	Recovery,
	Metadata,
	StoragePublication,
}

const REQUIRED_COMPONENTS: [CoverageComponent; 5] = [
	CoverageComponent::MutationAdmission,
	CoverageComponent::DurableObligations,
	CoverageComponent::Recovery,
	CoverageComponent::Metadata,
	CoverageComponent::StoragePublication,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unavailable {
	IncompleteCoverage,
	UnknownProducer,
	UnreadableState,
	SequenceExhausted,
	AccountingFailure,
	PoisonedAuthority,
	InvalidScope,
	ActivityDuringCollect,
	MetadataScopeMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScopeMetadata {
	pub scope: TableScope,
	pub descriptor_fingerprint: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetadataIdentity {
	pub membership_epoch: u64,
	pub placement_epoch: u64,
	pub scopes: Vec<ScopeMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Observation {
	pub incarnation: [u8; 16],
	pub sequence: u64,
	pub scopes: Vec<TableScope>,
	/// Internal conservative node-wide totals. Wire projection must not expose
	/// another tenant's identities or detailed work through these diagnostics.
	pub active: u64,
	pub durable: u64,
	pub metadata: MetadataIdentity,
}

impl Observation {
	/// Caller must additionally prove first/all-reads/last causal bracketing and
	/// the complete required-node roster. This method alone is NOT a cut.
	pub(crate) fn unchanged_and_zero(&self, other: &Self) -> bool {
		self.active == 0
			&& self.durable == 0
			&& other.active == 0
			&& other.durable == 0
			&& self == other
	}
}

#[derive(Default)]
struct State {
	sequence: u64,
	sticky: Option<Unavailable>,
	coverage: BTreeSet<CoverageComponent>,
	active: BTreeMap<(WorkScope, WorkKind), u64>,
	durable: BTreeMap<[u8; 32], WorkScope>,
}

impl State {
	fn advance(&mut self) {
		match self.sequence.checked_add(1) {
			Some(next) => self.sequence = next,
			None => self.fail(Unavailable::SequenceExhausted),
		}
	}

	fn fail(&mut self, reason: Unavailable) {
		if self.sticky.is_none() {
			self.sticky = Some(reason);
		}
	}

	fn totals(&self) -> Result<(u64, u64), Unavailable> {
		let active = self.active.values().try_fold(0u64, |sum, count| {
			sum.checked_add(*count)
				.ok_or(Unavailable::AccountingFailure)
		})?;
		let durable =
			u64::try_from(self.durable.len()).map_err(|_| Unavailable::AccountingFailure)?;
		Ok((active, durable))
	}

	fn available(&self) -> Result<(), Unavailable> {
		if let Some(reason) = self.sticky {
			return Err(reason);
		}
		if REQUIRED_COMPONENTS
			.iter()
			.any(|part| !self.coverage.contains(part))
		{
			return Err(Unavailable::IncompleteCoverage);
		}
		Ok(())
	}
}

pub(crate) struct FinalityObservationRegistry {
	incarnation: [u8; 16],
	state: Mutex<State>,
}

impl FinalityObservationRegistry {
	pub(crate) fn new() -> Arc<Self> {
		Arc::new(Self {
			incarnation: *Uuid::new_v4().as_bytes(),
			state: Mutex::new(State::default()),
		})
	}

	fn lock(&self) -> MutexGuard<'_, State> {
		match self.state.lock() {
			Ok(state) => state,
			Err(poisoned) => {
				let mut state = poisoned.into_inner();
				state.fail(Unavailable::PoisonedAuthority);
				state
			}
		}
	}

	/// Later slices may call only after that component's actual authorities and
	/// durable initialization are complete, never based on a zero metric.
	pub(crate) fn reconciled(&self, component: CoverageComponent) {
		let mut state = self.lock();
		if state.coverage.insert(component) {
			state.advance();
		}
	}

	pub(crate) fn invalidate_component(&self, component: CoverageComponent) {
		let mut state = self.lock();
		if state.coverage.remove(&component) {
			state.advance();
		}
	}

	/// An unmapped producer or indeterminate instrument state stays unavailable
	/// for this incarnation. It must not fail the underlying mutation.
	pub(crate) fn unavailable(&self, reason: Unavailable) {
		let mut state = self.lock();
		state.fail(reason);
		state.advance();
	}

	/// Register BEFORE the owned work can publish, and clone before detaching.
	/// Registration never refuses an ordinary mutation, including on overflow.
	pub(crate) fn begin(self: &Arc<Self>, scope: WorkScope, kind: WorkKind) -> WorkToken {
		let mut state = self.lock();
		state.advance();
		let key = (scope, kind);
		let count = state.active.entry(key).or_default();
		let counted = if let Some(next) = count.checked_add(1) {
			*count = next;
			true
		} else {
			state.fail(Unavailable::AccountingFailure);
			false
		};
		drop(state);
		WorkToken {
			inner: Arc::new(TokenInner {
				registry: self.clone(),
				scope,
				kind,
				counted,
			}),
		}
	}

	/// Collect a report without holding the authority while invoking metadata
	/// readers. An intervening admission/publication/recovery invalidates it.
	/// Metadata-changing work must itself hold a token across its publication.
	pub(crate) fn collect(
		&self,
		scopes: &[TableScope],
		read_metadata: impl FnOnce() -> Result<MetadataIdentity, Unavailable>,
	) -> Result<Observation, Unavailable> {
		if scopes.is_empty()
			|| scopes.len() > MAX_OBSERVATION_SCOPES
			|| scopes.iter().any(|scope| scope.generation == 0)
			|| scopes.windows(2).any(|pair| pair[0] >= pair[1])
		{
			return Err(Unavailable::InvalidScope);
		}
		let sequence = {
			let state = self.lock();
			state.available()?;
			state.sequence
		};
		let metadata = read_metadata()?;
		if metadata
			.scopes
			.iter()
			.map(|item| item.scope)
			.collect::<Vec<_>>()
			!= scopes
		{
			return Err(Unavailable::MetadataScopeMismatch);
		}
		let state = self.lock();
		state.available()?;
		if state.sequence != sequence {
			return Err(Unavailable::ActivityDuringCollect);
		}
		let (active, durable) = state.totals()?;
		Ok(Observation {
			incarnation: self.incarnation,
			sequence,
			scopes: scopes.to_vec(),
			active,
			durable,
			metadata,
		})
	}
}

#[derive(Clone)]
pub(crate) struct WorkToken {
	inner: Arc<TokenInner>,
}

impl WorkToken {
	/// Call only after the authoritative pending transaction committed. The
	/// active owner stays counted throughout, including caller cancellation.
	pub(crate) fn install_durable(&self, id: [u8; 32]) {
		let mut state = self.inner.registry.lock();
		match state.durable.get(&id) {
			Some(scope) if *scope == self.inner.scope => {}
			Some(_) => state.fail(Unavailable::AccountingFailure),
			None => {
				state.durable.insert(id, self.inner.scope);
				state.advance();
			}
		}
	}

	/// Call only after successful authoritative deletion. Missing or foreign
	/// debt is an instrument defect, never a successful zero observation.
	pub(crate) fn complete_durable(&self, id: [u8; 32]) {
		let mut state = self.inner.registry.lock();
		match state.durable.get(&id) {
			Some(scope) if *scope == self.inner.scope => {
				state.durable.remove(&id);
				state.advance();
			}
			_ => state.fail(Unavailable::AccountingFailure),
		}
	}
}

struct TokenInner {
	registry: Arc<FinalityObservationRegistry>,
	scope: WorkScope,
	kind: WorkKind,
	counted: bool,
}

impl Drop for TokenInner {
	fn drop(&mut self) {
		let mut state = self.registry.lock();
		if self.counted {
			let key = (self.scope, self.kind);
			match state
				.active
				.get_mut(&key)
				.and_then(|count| count.checked_sub(1))
			{
				Some(0) => {
					state.active.remove(&key);
				}
				Some(next) => {
					state.active.insert(key, next);
				}
				None => state.fail(Unavailable::AccountingFailure),
			}
		}
		state.advance();
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::{Barrier, mpsc};
	use std::thread;

	fn scope() -> TableScope {
		TableScope {
			tenant: TenantId::new(7),
			object: TableObjectId::new([1; 16]),
			generation: 1,
		}
	}

	fn ready() -> Arc<FinalityObservationRegistry> {
		let registry = FinalityObservationRegistry::new();
		for component in REQUIRED_COMPONENTS {
			registry.reconciled(component);
		}
		registry
	}

	fn metadata() -> MetadataIdentity {
		MetadataIdentity {
			membership_epoch: 1,
			placement_epoch: 1,
			scopes: vec![ScopeMetadata {
				scope: scope(),
				descriptor_fingerprint: [9; 32],
			}],
		}
	}

	fn observe(registry: &FinalityObservationRegistry) -> Result<Observation, Unavailable> {
		registry.collect(&[scope()], || Ok(metadata()))
	}

	#[test]
	fn adr629_p2a_startup_is_unavailable_until_every_component_is_reconciled() {
		let registry = FinalityObservationRegistry::new();
		for component in REQUIRED_COMPONENTS {
			assert_eq!(observe(&registry), Err(Unavailable::IncompleteCoverage));
			registry.reconciled(component);
		}
		assert_eq!(observe(&registry).unwrap().active, 0);
		registry.invalidate_component(CoverageComponent::Recovery);
		assert_eq!(observe(&registry), Err(Unavailable::IncompleteCoverage));
	}

	#[test]
	fn adr629_p2a_zero_to_zero_work_changes_sequence_and_clones_hold_the_obligation() {
		let registry = ready();
		let before = observe(&registry).unwrap();
		let token = registry.begin(WorkScope::Table(scope()), WorkKind::Mutation);
		let detached = token.clone();
		drop(token);
		assert_eq!(observe(&registry).unwrap().active, 1);
		drop(detached);
		let after = observe(&registry).unwrap();
		assert_eq!(after.active, 0);
		assert!(after.sequence > before.sequence);
		assert!(!before.unchanged_and_zero(&after));
		assert!(after.unchanged_and_zero(&observe(&registry).unwrap()));
	}

	#[test]
	fn adr629_p2a_persistent_handoff_never_reports_zero_and_delete_requires_an_owner() {
		let registry = ready();
		let token = registry.begin(WorkScope::Table(scope()), WorkKind::Persistence);
		assert_eq!(observe(&registry).unwrap().active, 1);
		token.install_durable([8; 32]);
		token.install_durable([8; 32]); // identical registration is idempotent
		let overlap = observe(&registry).unwrap();
		assert_eq!((overlap.active, overlap.durable), (1, 1));
		drop(token);
		let pending = observe(&registry).unwrap();
		assert_eq!((pending.active, pending.durable), (0, 1));
		let replay = registry.begin(WorkScope::Table(scope()), WorkKind::Finalization);
		replay.complete_durable([8; 32]);
		assert_eq!(observe(&registry).unwrap().active, 1);
		drop(replay);
		assert_eq!(observe(&registry).unwrap().durable, 0);
	}

	#[test]
	fn adr629_p2a_foreign_or_missing_durable_debt_withholds_instead_of_manufacturing_zero() {
		for missing in [false, true] {
			let registry = ready();
			let original = registry.begin(WorkScope::Table(scope()), WorkKind::Persistence);
			original.install_durable([8; 32]);
			let foreign =
				registry.begin(WorkScope::Tenant(TenantId::new(8)), WorkKind::Finalization);
			foreign.complete_durable(if missing { [0; 32] } else { [8; 32] });
			assert_eq!(observe(&registry), Err(Unavailable::AccountingFailure));
		}
	}

	#[test]
	fn adr629_p2a_conservative_node_work_blocks_an_unrelated_table_zero() {
		let registry = ready();
		let unrelated = registry.begin(WorkScope::Tenant(TenantId::new(999)), WorkKind::Mutation);
		let report = observe(&registry).unwrap();
		assert_eq!(report.active, 1);
		assert!(!report.unchanged_and_zero(&report));
		drop(unrelated);
	}

	#[test]
	fn adr629_p2a_snapshot_racing_admission_observes_the_held_work() {
		let registry = ready();
		let enter = Arc::new(Barrier::new(2));
		let release = Arc::new(Barrier::new(2));
		let worker_registry = registry.clone();
		let worker_enter = enter.clone();
		let worker_release = release.clone();
		let task = thread::spawn(move || {
			let token = worker_registry.begin(WorkScope::Table(scope()), WorkKind::Mutation);
			worker_enter.wait();
			worker_release.wait();
			drop(token);
		});
		enter.wait();
		assert_eq!(observe(&registry).unwrap().active, 1);
		release.wait();
		task.join().unwrap();
		assert_eq!(observe(&registry).unwrap().active, 0);
	}

	#[test]
	fn adr629_p2a_metadata_collect_holds_no_authority_lock_and_rejects_intervening_work() {
		let registry = ready();
		let (entered_tx, entered_rx) = mpsc::channel();
		let (resume_tx, resume_rx) = mpsc::channel();
		let reading_registry = registry.clone();
		let task = thread::spawn(move || {
			reading_registry.collect(&[scope()], || {
				assert!(reading_registry.state.try_lock().is_ok());
				let external_metadata = Mutex::new(metadata());
				let metadata_guard = external_metadata.lock().unwrap();
				entered_tx.send(()).unwrap();
				resume_rx.recv().unwrap();
				Ok(metadata_guard.clone())
			})
		});
		entered_rx.recv().unwrap();
		let token = registry.begin(WorkScope::Node, WorkKind::Metadata);
		drop(token);
		resume_tx.send(()).unwrap();
		assert_eq!(
			task.join().unwrap(),
			Err(Unavailable::ActivityDuringCollect)
		);
	}

	#[test]
	fn adr629_p2a_incarnation_and_metadata_changes_invalidate_comparisons() {
		let first = ready();
		let second = ready();
		let before = observe(&first).unwrap();
		let restart = observe(&second).unwrap();
		assert_eq!(before.sequence, restart.sequence);
		assert_ne!(before.incarnation, restart.incarnation);
		assert!(!before.unchanged_and_zero(&restart));
		let changed = first
			.collect(&[scope()], || {
				let mut value = metadata();
				value.placement_epoch += 1;
				Ok(value)
			})
			.unwrap();
		assert!(!before.unchanged_and_zero(&changed));
	}

	#[test]
	fn adr629_p2a_unknown_unreadable_and_exhausted_authority_never_refuse_underlying_work() {
		for reason in [Unavailable::UnknownProducer, Unavailable::UnreadableState] {
			let registry = ready();
			registry.unavailable(reason);
			let token = registry.begin(WorkScope::Node, WorkKind::Mutation);
			drop(token);
			assert_eq!(observe(&registry), Err(reason));
		}
		let registry = ready();
		registry.lock().sequence = u64::MAX;
		let token = registry.begin(WorkScope::Node, WorkKind::Mutation);
		drop(token);
		assert_eq!(observe(&registry), Err(Unavailable::SequenceExhausted));
	}

	#[test]
	fn adr629_p2a_poison_withholds_but_token_drop_and_registration_do_not_panic() {
		let registry = ready();
		let worker_registry = registry.clone();
		assert!(
			thread::spawn(move || {
				let _held = worker_registry.state.lock().unwrap();
				panic!("intentional authority poison");
			})
			.join()
			.is_err()
		);
		let token = registry.begin(WorkScope::Node, WorkKind::Mutation);
		drop(token);
		assert_eq!(observe(&registry), Err(Unavailable::PoisonedAuthority));
	}

	#[tokio::test]
	async fn adr629_p2a_running_blocking_job_retains_ownership_after_caller_handle_is_dropped() {
		let registry = ready();
		let owner = registry.begin(WorkScope::Table(scope()), WorkKind::Persistence);
		let detached = owner.clone();
		let (entered_tx, entered_rx) = mpsc::channel();
		let (resume_tx, resume_rx) = mpsc::channel();
		let (done_tx, done_rx) = mpsc::channel();
		let handle = tokio::task::spawn_blocking(move || {
			entered_tx.send(()).unwrap();
			resume_rx.recv().unwrap();
			detached.install_durable([6; 32]);
			drop(detached);
			done_tx.send(()).unwrap();
		});
		entered_rx.recv().unwrap();
		drop(owner);
		drop(handle); // caller ownership ends, the blocking job does not
		assert_eq!(observe(&registry).unwrap().active, 1);
		resume_tx.send(()).unwrap();
		done_rx.recv().unwrap();
		let after = observe(&registry).unwrap();
		assert_eq!((after.active, after.durable), (0, 1));
	}

	#[test]
	fn adr629_p2a_count_overflow_and_underflow_are_sticky_unavailable_not_panics() {
		let key = (WorkScope::Node, WorkKind::Mutation);
		let registry = ready();
		registry.lock().active.insert(key, u64::MAX);
		let token = registry.begin(key.0, key.1);
		drop(token);
		assert_eq!(observe(&registry), Err(Unavailable::AccountingFailure));
		let registry = ready();
		let token = registry.begin(key.0, key.1);
		registry.lock().active.remove(&key); // inject a broken instrument invariant
		drop(token);
		assert_eq!(observe(&registry), Err(Unavailable::AccountingFailure));
	}

	#[test]
	fn adr629_p2a_conflicting_durable_registration_preserves_original_debt_and_withholds() {
		let registry = ready();
		let first = registry.begin(WorkScope::Table(scope()), WorkKind::Persistence);
		first.install_durable([7; 32]);
		let other = registry.begin(WorkScope::Node, WorkKind::Persistence);
		other.install_durable([7; 32]);
		assert_eq!(
			registry.lock().durable.get(&[7; 32]),
			Some(&WorkScope::Table(scope()))
		);
		assert_eq!(observe(&registry), Err(Unavailable::AccountingFailure));
	}

	#[test]
	fn adr629_p2a_authority_lock_orders_admission_before_a_later_zero_comparison() {
		let registry = ready();
		let before = observe(&registry).unwrap();
		let held = registry.lock();
		let worker_registry = registry.clone();
		let (blocked_tx, blocked_rx) = mpsc::channel();
		let (entered_tx, entered_rx) = mpsc::channel();
		let (resume_tx, resume_rx) = mpsc::channel();
		let task = thread::spawn(move || {
			assert!(worker_registry.state.try_lock().is_err());
			blocked_tx.send(()).unwrap();
			let token = worker_registry.begin(WorkScope::Node, WorkKind::Mutation);
			entered_tx.send(()).unwrap();
			resume_rx.recv().unwrap();
			drop(token);
		});
		blocked_rx.recv().unwrap();
		assert_eq!(held.sequence, before.sequence);
		drop(held);
		entered_rx.recv().unwrap();
		assert_eq!(observe(&registry).unwrap().active, 1);
		resume_tx.send(()).unwrap();
		task.join().unwrap();
		assert!(!before.unchanged_and_zero(&observe(&registry).unwrap()));
	}

	#[test]
	fn adr629_p2a_scope_validation_and_metadata_read_failure_withhold() {
		let registry = ready();
		for scopes in [vec![], vec![scope(), scope()], vec![scope(); 17]] {
			assert_eq!(
				registry.collect(&scopes, || Ok(metadata())),
				Err(Unavailable::InvalidScope)
			);
		}
		let mut zero = scope();
		zero.generation = 0;
		assert_eq!(
			registry.collect(&[zero], || Ok(metadata())),
			Err(Unavailable::InvalidScope)
		);
		assert_eq!(
			registry.collect(&[scope()], || Err(Unavailable::UnreadableState)),
			Err(Unavailable::UnreadableState)
		);
		assert_eq!(
			registry.collect(&[scope()], || {
				let mut value = metadata();
				value.scopes.clear();
				Ok(value)
			}),
			Err(Unavailable::MetadataScopeMismatch)
		);
	}
}
