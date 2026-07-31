use crate::integration::compute::DropOperationId;
use crate::schema::{HybridTimestamp, TableObjectId, TenantId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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

	pub fn finalize(
		&mut self,
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
		self.by_id.remove(&key);
		Ok(fence)
	}
}

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
}
