use crate::integration::compute::DropOperationId;
use crate::schema::{HybridTimestamp, NodeId, TableObjectId, TenantId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

pub const MIN_RETENTION_WINDOW_MS: u64 = 60_000;
pub const MAX_RETENTION_WINDOW_MS: u64 = 100 * 365 * 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct RetentionWindowMillis(u64);

impl RetentionWindowMillis {
	pub fn try_new(value: u64) -> Result<Self, RetentionWindowError> {
		let window = Self(value);
		window.validate()?;
		Ok(window)
	}

	pub fn try_from_duration(duration: Duration) -> Result<Self, RetentionWindowError> {
		let value = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
		Self::try_new(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}

	pub fn validate(self) -> Result<(), RetentionWindowError> {
		if self.0 < MIN_RETENTION_WINDOW_MS || self.0 > MAX_RETENTION_WINDOW_MS {
			return Err(RetentionWindowError { value: self.0 });
		}
		Ok(())
	}
}

impl Serialize for RetentionWindowMillis {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for RetentionWindowMillis {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let value = u64::deserialize(deserializer)?;
		Self::try_new(value).map_err(serde::de::Error::custom)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionWindowError {
	value: u64,
}

impl RetentionWindowError {
	pub const fn value(self) -> u64 {
		self.value
	}
}

impl fmt::Display for RetentionWindowError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"RETENTION_REJECTED_OUT_OF_RANGE: retention window {}ms is outside {}..={}",
			self.value, MIN_RETENTION_WINDOW_MS, MAX_RETENTION_WINDOW_MS
		)
	}
}

impl std::error::Error for RetentionWindowError {}

#[derive(
	Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PolicyGeneration(u64);

impl PolicyGeneration {
	pub const ZERO: Self = Self(0);

	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}

	pub const fn next(self) -> Self {
		Self(self.0.saturating_add(1))
	}
}

impl fmt::Display for PolicyGeneration {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableRetentionPolicy {
	InheritCluster,
	Disabled,
	FixedWindow(RetentionWindowMillis),
}

impl TableRetentionPolicy {
	pub const fn is_table_override_disallowed_for_pk(self) -> bool {
		matches!(self, Self::Disabled | Self::FixedWindow(_))
	}

	pub fn validate(self) -> Result<(), RetentionWindowError> {
		match self {
			Self::FixedWindow(window) => window.validate(),
			Self::InheritCluster | Self::Disabled => Ok(()),
		}
	}
}

impl Default for TableRetentionPolicy {
	fn default() -> Self {
		Self::InheritCluster
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableLifecycleKey {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
}

impl TableLifecycleKey {
	pub const fn new(tenant_id: TenantId, table_object_id: TableObjectId) -> Self {
		Self {
			tenant_id,
			table_object_id,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableLifecyclePolicy {
	pub policy_generation: PolicyGeneration,
	pub retention: TableRetentionPolicy,
	pub updated_at_hlc: HybridTimestamp,
}

impl TableLifecyclePolicy {
	pub const fn inherit_cluster(updated_at_hlc: HybridTimestamp) -> Self {
		Self {
			policy_generation: PolicyGeneration::ZERO,
			retention: TableRetentionPolicy::InheritCluster,
			updated_at_hlc,
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableLifecycleCatalog {
	by_id: BTreeMap<TableLifecycleKey, TableLifecyclePolicy>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropFence {
	pub operation_id: DropOperationId,
	pub fence_log_index: u64,
	pub reclaim_completion_log_index: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropFenceCatalog {
	#[serde(default)]
	by_id: BTreeMap<TableLifecycleKey, TableDropFence>,
}

/// ADR 0093: durable proof that one derived DROP operation completed.
///
/// The fence proves that a drop is in flight; this outcome proves that it
/// finished. Finalize moves a table from the former catalog to this one in a
/// single deterministic state-machine apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropOutcome {
	pub operation_id: DropOperationId,
	pub finalize_log_index: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropOutcomeCatalog {
	by_id: BTreeMap<TableLifecycleKey, TableDropOutcome>,
}

/// ADR 0095: admission-authoritative state after logical DROP finalizes and
/// before physical reclaim settles.
///
/// This replaces the prepare fence without opening an object-id admission
/// gap. The descriptor remains available to reclaim machinery while every
/// data-path admission site treats this tombstone exactly like the fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropTombstone {
	pub operation_id: DropOperationId,
	pub fence_log_index: u64,
	pub finalize_log_index: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropTombstoneCatalog {
	by_id: BTreeMap<TableLifecycleKey, TableDropTombstone>,
}

/// Deterministically ordered ADR 0095 erasure participants.
///
/// `NodeId` does not expose an ordering contract, so construction canonicalizes
/// by the full wire identity `(id, _reserved)`. Decode does not silently repair
/// malformed state: consensus apply calls [`Self::validate`] before trusting a
/// stamped set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropParticipantSet {
	nodes: Vec<NodeId>,
}

impl TableDropParticipantSet {
	pub fn new(nodes: impl IntoIterator<Item = NodeId>) -> Self {
		let mut keyed = BTreeSet::new();
		for node in nodes {
			keyed.insert((node.id, node._reserved));
		}
		Self {
			nodes: keyed
				.into_iter()
				.map(|(id, reserved)| NodeId {
					id,
					_reserved: reserved,
				})
				.collect(),
		}
	}

	pub fn validate(&self) -> Result<(), TableDropLifecycleError> {
		if self.nodes.is_empty() {
			return Err(TableDropLifecycleError::EmptyParticipantSet);
		}
		let canonical = Self::new(self.nodes.iter().copied());
		if canonical != *self {
			return Err(TableDropLifecycleError::NonCanonicalParticipantSet);
		}
		Ok(())
	}

	pub fn contains(&self, node: NodeId) -> bool {
		self.nodes.iter().any(|candidate| *candidate == node)
	}

	pub fn iter(&self) -> impl ExactSizeIterator<Item = NodeId> + '_ {
		self.nodes.iter().copied()
	}

	pub fn len(&self) -> usize {
		self.nodes.len()
	}

	pub fn is_empty(&self) -> bool {
		self.nodes.is_empty()
	}
}

/// Replicated ADR 0095 physical-erasure obligation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropReclaimDebt {
	pub operation_id: DropOperationId,
	pub fence_log_index: u64,
	pub finalize_log_index: u64,
	pub ownership_epoch: u64,
	pub participants: TableDropParticipantSet,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDropReclaimDebtCatalog {
	by_id: BTreeMap<TableLifecycleKey, TableDropReclaimDebt>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableLifecyclePolicyCounts {
	pub inherit_cluster: usize,
	pub disabled: usize,
	pub fixed_window: usize,
}

impl TableLifecycleCatalog {
	pub fn get(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableLifecyclePolicy> {
		self.by_id
			.get(&TableLifecycleKey::new(tenant_id, table_object_id))
	}

	pub fn retention_policy(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableRetentionPolicy> {
		self.get(tenant_id, table_object_id)
			.map(|policy| &policy.retention)
	}

	pub fn current_generation(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> PolicyGeneration {
		self.get(tenant_id, table_object_id)
			.map(|policy| policy.policy_generation)
			.unwrap_or(PolicyGeneration::ZERO)
	}

	pub fn ensure_inherit_cluster(
		&mut self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		updated_at_hlc: HybridTimestamp,
	) {
		self.by_id
			.entry(TableLifecycleKey::new(tenant_id, table_object_id))
			.or_insert_with(|| TableLifecyclePolicy::inherit_cluster(updated_at_hlc));
	}

	pub fn set_next_generation(
		&mut self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		expected_generation: PolicyGeneration,
		retention: TableRetentionPolicy,
		updated_at_hlc: HybridTimestamp,
	) -> Result<TableLifecyclePolicy, TableLifecycleSetError> {
		retention
			.validate()
			.map_err(TableLifecycleSetError::OutOfRange)?;
		let current = self.current_generation(tenant_id, table_object_id);
		if current != expected_generation {
			return Err(TableLifecycleSetError::StaleGeneration {
				expected_generation,
				current,
			});
		}
		let next = TableLifecyclePolicy {
			policy_generation: current.next(),
			retention,
			updated_at_hlc,
		};
		self.by_id
			.insert(TableLifecycleKey::new(tenant_id, table_object_id), next);
		Ok(next)
	}

	pub fn remove(&mut self, tenant_id: TenantId, table_object_id: TableObjectId) -> bool {
		self.by_id
			.remove(&TableLifecycleKey::new(tenant_id, table_object_id))
			.is_some()
	}

	pub fn len(&self) -> usize {
		self.by_id.len()
	}

	pub fn is_empty(&self) -> bool {
		self.by_id.is_empty()
	}

	/// Whether any table carries a retention window of its own. Only such a table can have a
	/// retention floor when the cluster policy is disabled.
	pub fn has_fixed_window(&self) -> bool {
		self.by_id
			.values()
			.any(|policy| matches!(policy.retention, TableRetentionPolicy::FixedWindow(_)))
	}

	pub fn policy_counts(&self) -> TableLifecyclePolicyCounts {
		let mut counts = TableLifecyclePolicyCounts::default();
		for policy in self.by_id.values() {
			match policy.retention {
				TableRetentionPolicy::InheritCluster => counts.inherit_cluster += 1,
				TableRetentionPolicy::Disabled => counts.disabled += 1,
				TableRetentionPolicy::FixedWindow(_) => counts.fixed_window += 1,
			}
		}
		counts
	}
}

impl TableDropFenceCatalog {
	pub fn iter(&self) -> impl Iterator<Item = &TableDropFence> {
		self.by_id.values()
	}

	pub fn get(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableDropFence> {
		self.by_id
			.get(&TableLifecycleKey::new(tenant_id, table_object_id))
	}

	pub fn is_fenced(&self, tenant_id: TenantId, table_object_id: TableObjectId) -> bool {
		self.get(tenant_id, table_object_id).is_some()
	}

	pub fn prepare(
		&mut self,
		operation_id: DropOperationId,
		fence_log_index: u64,
	) -> Result<TableDropFence, TableDropFenceError> {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		if let Some(existing) = self.by_id.get(&key).copied() {
			if existing.operation_id == operation_id {
				return Ok(existing);
			}
			return Err(TableDropFenceError::AlreadyDropping {
				existing_operation_id: existing.operation_id,
			});
		}
		let fence = TableDropFence {
			operation_id,
			fence_log_index,
			reclaim_completion_log_index: None,
		};
		self.by_id.insert(key, fence);
		Ok(fence)
	}

	pub fn complete_reclaim(
		&mut self,
		operation_id: DropOperationId,
		fence_log_index: u64,
		reclaim_completion_log_index: u64,
	) -> Result<TableDropFence, TableDropFenceError> {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		let Some(fence) = self.by_id.get_mut(&key) else {
			return Err(TableDropFenceError::MissingFence);
		};
		if fence.operation_id != operation_id {
			return Err(TableDropFenceError::AlreadyDropping {
				existing_operation_id: fence.operation_id,
			});
		}
		if fence.fence_log_index != fence_log_index {
			return Err(TableDropFenceError::FenceLogIndexMismatch {
				expected: fence.fence_log_index,
				observed: fence_log_index,
			});
		}
		if let Some(existing) = fence.reclaim_completion_log_index {
			if existing != reclaim_completion_log_index {
				return Err(TableDropFenceError::ReclaimCompletionLogIndexMismatch {
					expected: existing,
					observed: reclaim_completion_log_index,
				});
			}
			return Ok(*fence);
		}
		fence.reclaim_completion_log_index = Some(reclaim_completion_log_index);
		Ok(*fence)
	}

	pub fn validate_finalize(
		&self,
		operation_id: DropOperationId,
		fence_log_index: u64,
		reclaim_completion_log_index: u64,
	) -> Result<TableDropFence, TableDropFenceError> {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		let Some(fence) = self.by_id.get(&key).copied() else {
			return Err(TableDropFenceError::MissingFence);
		};
		if fence.operation_id != operation_id {
			return Err(TableDropFenceError::AlreadyDropping {
				existing_operation_id: fence.operation_id,
			});
		}
		if fence.fence_log_index != fence_log_index {
			return Err(TableDropFenceError::FenceLogIndexMismatch {
				expected: fence.fence_log_index,
				observed: fence_log_index,
			});
		}
		if fence.reclaim_completion_log_index != Some(reclaim_completion_log_index) {
			return Err(TableDropFenceError::ReclaimCompletionLogIndexMismatch {
				expected: fence.reclaim_completion_log_index.unwrap_or(0),
				observed: reclaim_completion_log_index,
			});
		}
		Ok(fence)
	}

	pub fn validate_logical_finalize(
		&self,
		operation_id: DropOperationId,
		fence_log_index: u64,
	) -> Result<TableDropFence, TableDropFenceError> {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		let Some(fence) = self.by_id.get(&key).copied() else {
			return Err(TableDropFenceError::MissingFence);
		};
		if fence.operation_id != operation_id {
			return Err(TableDropFenceError::AlreadyDropping {
				existing_operation_id: fence.operation_id,
			});
		}
		if fence.fence_log_index != fence_log_index {
			return Err(TableDropFenceError::FenceLogIndexMismatch {
				expected: fence.fence_log_index,
				observed: fence_log_index,
			});
		}
		Ok(fence)
	}

	/// Remove a fence only after [`Self::validate_finalize`] succeeded in the
	/// same exclusive state-machine apply. This seam is deliberately
	/// infallible: all contradictions belong to the prevalidation phase.
	pub fn remove_validated(&mut self, operation_id: DropOperationId) {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		let removed = self.by_id.remove(&key);
		debug_assert_eq!(removed.map(|fence| fence.operation_id), Some(operation_id));
	}
}

impl TableDropOutcomeCatalog {
	pub fn get(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableDropOutcome> {
		self.by_id
			.get(&TableLifecycleKey::new(tenant_id, table_object_id))
	}

	pub fn validate_record(&self, outcome: TableDropOutcome) -> Result<(), TableDropOutcomeError> {
		let key = TableLifecycleKey::new(
			outcome.operation_id.tenant_id,
			outcome.operation_id.table_object_id,
		);
		match self.by_id.get(&key).copied() {
			None => Ok(()),
			Some(existing) if existing == outcome => Ok(()),
			Some(existing) => Err(TableDropOutcomeError::Conflict {
				existing,
				observed: outcome,
			}),
		}
	}

	/// Insert an outcome only after [`Self::validate_record`] succeeded in the
	/// same exclusive state-machine apply.
	pub fn insert_validated(&mut self, outcome: TableDropOutcome) {
		let key = TableLifecycleKey::new(
			outcome.operation_id.tenant_id,
			outcome.operation_id.table_object_id,
		);
		let previous = self.by_id.insert(key, outcome);
		debug_assert!(previous.is_none() || previous == Some(outcome));
	}

	pub fn len(&self) -> usize {
		self.by_id.len()
	}

	pub fn is_empty(&self) -> bool {
		self.by_id.is_empty()
	}
}

impl TableDropTombstoneCatalog {
	pub fn iter(&self) -> impl Iterator<Item = &TableDropTombstone> {
		self.by_id.values()
	}

	pub fn get(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableDropTombstone> {
		self.by_id
			.get(&TableLifecycleKey::new(tenant_id, table_object_id))
	}

	pub fn contains(&self, tenant_id: TenantId, table_object_id: TableObjectId) -> bool {
		self.get(tenant_id, table_object_id).is_some()
	}

	pub fn validate_insert(
		&self,
		tombstone: TableDropTombstone,
	) -> Result<(), TableDropLifecycleError> {
		let key = TableLifecycleKey::new(
			tombstone.operation_id.tenant_id,
			tombstone.operation_id.table_object_id,
		);
		match self.by_id.get(&key).copied() {
			None => Ok(()),
			Some(existing) if existing == tombstone => Ok(()),
			Some(_) => Err(TableDropLifecycleError::TombstoneConflict),
		}
	}

	pub fn insert_validated(&mut self, tombstone: TableDropTombstone) {
		let key = TableLifecycleKey::new(
			tombstone.operation_id.tenant_id,
			tombstone.operation_id.table_object_id,
		);
		let previous = self.by_id.insert(key, tombstone);
		debug_assert!(previous.is_none() || previous == Some(tombstone));
	}

	pub fn remove_validated(&mut self, operation_id: DropOperationId) {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		let removed = self.by_id.remove(&key);
		debug_assert_eq!(removed.map(|row| row.operation_id), Some(operation_id));
	}

	pub fn len(&self) -> usize {
		self.by_id.len()
	}

	pub fn is_empty(&self) -> bool {
		self.by_id.is_empty()
	}
}

impl TableDropReclaimDebtCatalog {
	pub fn get(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableDropReclaimDebt> {
		self.by_id
			.get(&TableLifecycleKey::new(tenant_id, table_object_id))
	}

	pub fn validate_insert(
		&self,
		debt: &TableDropReclaimDebt,
	) -> Result<(), TableDropLifecycleError> {
		debt.participants.validate()?;
		let key = TableLifecycleKey::new(
			debt.operation_id.tenant_id,
			debt.operation_id.table_object_id,
		);
		match self.by_id.get(&key) {
			None => Ok(()),
			Some(existing) if existing == debt => Ok(()),
			Some(_) => Err(TableDropLifecycleError::DebtConflict),
		}
	}

	pub fn insert_validated(&mut self, debt: TableDropReclaimDebt) {
		let key = TableLifecycleKey::new(
			debt.operation_id.tenant_id,
			debt.operation_id.table_object_id,
		);
		let previous = self.by_id.insert(key, debt.clone());
		debug_assert!(previous.is_none() || previous == Some(debt));
	}

	pub fn remove_validated(&mut self, operation_id: DropOperationId) {
		let key = TableLifecycleKey::new(operation_id.tenant_id, operation_id.table_object_id);
		let removed = self.by_id.remove(&key);
		debug_assert_eq!(removed.map(|row| row.operation_id), Some(operation_id));
	}

	pub fn iter(&self) -> impl Iterator<Item = &TableDropReclaimDebt> {
		self.by_id.values()
	}

	pub fn len(&self) -> usize {
		self.by_id.len()
	}

	pub fn is_empty(&self) -> bool {
		self.by_id.is_empty()
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableDropLifecycleError {
	EmptyParticipantSet,
	NonCanonicalParticipantSet,
	TombstoneConflict,
	DebtConflict,
}

impl fmt::Display for TableDropLifecycleError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::EmptyParticipantSet => {
				write!(f, "table drop reclaim participant set is empty")
			}
			Self::NonCanonicalParticipantSet => write!(
				f,
				"table drop reclaim participant set is not sorted and unique"
			),
			Self::TombstoneConflict => {
				write!(f, "table drop tombstone conflicts with durable state")
			}
			Self::DebtConflict => write!(f, "table drop reclaim debt conflicts with durable state"),
		}
	}
}

impl std::error::Error for TableDropLifecycleError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableDropOutcomeError {
	Conflict {
		existing: TableDropOutcome,
		observed: TableDropOutcome,
	},
}

impl fmt::Display for TableDropOutcomeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Conflict { existing, observed } => write!(
				f,
				"table drop outcome conflict: existing operation_id {:?} at finalize log {}, observed operation_id {:?} at finalize log {}",
				existing.operation_id,
				existing.finalize_log_index,
				observed.operation_id,
				observed.finalize_log_index,
			),
		}
	}
}

impl std::error::Error for TableDropOutcomeError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableDropFenceError {
	AlreadyDropping {
		existing_operation_id: DropOperationId,
	},
	MissingFence,
	FenceLogIndexMismatch {
		expected: u64,
		observed: u64,
	},
	ReclaimCompletionLogIndexMismatch {
		expected: u64,
		observed: u64,
	},
}

impl fmt::Display for TableDropFenceError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::AlreadyDropping {
				existing_operation_id,
			} => write!(
				f,
				"table is already dropping under operation_id {existing_operation_id:?}"
			),
			Self::MissingFence => write!(f, "table drop fence is missing"),
			Self::FenceLogIndexMismatch { expected, observed } => write!(
				f,
				"table drop fence_log_index mismatch: expected {expected}, observed {observed}"
			),
			Self::ReclaimCompletionLogIndexMismatch { expected, observed } => write!(
				f,
				"table drop reclaim_completion_log_index mismatch: expected {expected}, observed {observed}"
			),
		}
	}
}

impl std::error::Error for TableDropFenceError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableLifecycleSetError {
	OutOfRange(RetentionWindowError),
	StaleGeneration {
		expected_generation: PolicyGeneration,
		current: PolicyGeneration,
	},
}

impl fmt::Display for TableLifecycleSetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::OutOfRange(err) => err.fmt(f),
			Self::StaleGeneration {
				expected_generation,
				current,
			} => write!(
				f,
				"stale policy_generation: expected {}, current {}",
				expected_generation, current
			),
		}
	}
}

impl std::error::Error for TableLifecycleSetError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct RetentionRejectCode(pub u16);

impl RetentionRejectCode {
	pub const RETENTION_REJECTED_OUT_OF_RANGE: Self = Self(0x3C01);
	pub const RETENTION_REJECTED_PK_DISALLOWED: Self = Self(0x3C02);
	pub const RETENTION_FLOOR_EXCEEDED: Self = Self(0x3C03);

	pub const fn name(self) -> &'static str {
		match self.0 {
			0x3C01 => "RETENTION_REJECTED_OUT_OF_RANGE",
			0x3C02 => "RETENTION_REJECTED_PK_DISALLOWED",
			0x3C03 => "RETENTION_FLOOR_EXCEEDED",
			_ => "RETENTION_REJECT_UNKNOWN",
		}
	}
}

impl fmt::Display for RetentionRejectCode {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.name())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 0,
		}
	}

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn drop_operation() -> DropOperationId {
		DropOperationId {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([0x95; 16]),
			schema_generation: crate::integration::compute::SchemaGeneration::new(3),
		}
	}

	#[test]
	fn retention_window_bounds_are_enforced() {
		assert!(RetentionWindowMillis::try_new(MIN_RETENTION_WINDOW_MS).is_ok());
		assert!(RetentionWindowMillis::try_new(MAX_RETENTION_WINDOW_MS).is_ok());
		assert!(RetentionWindowMillis::try_new(MIN_RETENTION_WINDOW_MS - 1).is_err());
		assert!(RetentionWindowMillis::try_new(MAX_RETENTION_WINDOW_MS + 1).is_err());
	}

	#[test]
	fn catalog_defaults_missing_generation_to_zero_and_advances_on_set() {
		let tenant = TenantId::new(7);
		let table = TableObjectId::new([9; 16]);
		let mut catalog = TableLifecycleCatalog::default();
		assert_eq!(
			catalog.current_generation(tenant, table),
			PolicyGeneration::ZERO
		);

		let policy = TableRetentionPolicy::FixedWindow(
			RetentionWindowMillis::try_new(MIN_RETENTION_WINDOW_MS).unwrap(),
		);
		let stored = catalog
			.set_next_generation(tenant, table, PolicyGeneration::ZERO, policy, hlc(10))
			.unwrap();
		assert_eq!(stored.policy_generation, PolicyGeneration::new(1));
		assert_eq!(catalog.retention_policy(tenant, table), Some(&policy));
	}

	#[test]
	fn adr0095_participants_are_canonical_and_malformed_state_is_refused() {
		let participants = TableDropParticipantSet::new([node(9), node(2), node(9), node(4)]);
		assert_eq!(
			participants.iter().map(|node| node.id).collect::<Vec<_>>(),
			[2, 4, 9]
		);
		assert!(participants.validate().is_ok());

		let malformed = TableDropParticipantSet {
			nodes: vec![node(4), node(2)],
		};
		assert_eq!(
			malformed.validate(),
			Err(TableDropLifecycleError::NonCanonicalParticipantSet)
		);
		assert_eq!(
			TableDropParticipantSet::default().validate(),
			Err(TableDropLifecycleError::EmptyParticipantSet)
		);
	}

	#[test]
	fn adr0095_tombstone_and_debt_catalogs_refuse_conflicting_replay() {
		let operation_id = drop_operation();
		let tombstone = TableDropTombstone {
			operation_id,
			fence_log_index: 10,
			finalize_log_index: 11,
		};
		let debt = TableDropReclaimDebt {
			operation_id,
			fence_log_index: 10,
			finalize_log_index: 11,
			ownership_epoch: 5,
			participants: TableDropParticipantSet::new([node(2), node(4)]),
		};

		let mut tombstones = TableDropTombstoneCatalog::default();
		tombstones.validate_insert(tombstone).unwrap();
		tombstones.insert_validated(tombstone);
		assert!(tombstones.validate_insert(tombstone).is_ok());
		assert_eq!(
			tombstones.validate_insert(TableDropTombstone {
				finalize_log_index: 12,
				..tombstone
			}),
			Err(TableDropLifecycleError::TombstoneConflict)
		);

		let mut debts = TableDropReclaimDebtCatalog::default();
		debts.validate_insert(&debt).unwrap();
		debts.insert_validated(debt.clone());
		assert!(debts.validate_insert(&debt).is_ok());
		let mut conflict = debt.clone();
		conflict.ownership_epoch += 1;
		assert_eq!(
			debts.validate_insert(&conflict),
			Err(TableDropLifecycleError::DebtConflict)
		);
	}
}
