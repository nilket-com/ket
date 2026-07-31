use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use crate::distributed::budget::TenantBudgetPolicy;
use crate::storage::TenantId;

#[derive(Clone, Debug)]
pub struct SqlConnectionLimiter {
	inner: Arc<SqlConnectionLimiterInner>,
}

#[derive(Debug)]
struct SqlConnectionLimiterInner {
	max_total: Option<NonZeroU32>,
	tenant_policy: TenantBudgetPolicy,
	state: Mutex<SqlConnectionState>,
}

#[derive(Debug, Default)]
struct SqlConnectionState {
	global_active: u32,
	global_rejections: u64,
	tenant_active: HashMap<TenantConnectionKey, u32>,
	tenant_rejections: HashMap<TenantConnectionKey, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLimitExceeded;

#[must_use]
pub struct Tier1Permit {
	limiter: Option<SqlConnectionLimiter>,
}

#[must_use]
pub struct Tier2Permit {
	limiter: Option<SqlConnectionLimiter>,
	key: TenantConnectionKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalSqlConnectionMetrics {
	pub active: u64,
	pub rejections: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantSqlConnectionMetricSlot {
	pub label: String,
	pub active: u64,
	pub rejections: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TenantConnectionKey {
	Configured(TenantId),
	Other,
}

impl SqlConnectionLimiter {
	pub fn new(max_total: Option<NonZeroU32>, tenant_policy: TenantBudgetPolicy) -> Self {
		Self {
			inner: Arc::new(SqlConnectionLimiterInner {
				max_total,
				tenant_policy,
				state: Mutex::new(SqlConnectionState::default()),
			}),
		}
	}

	pub fn try_acquire_tier1(&self) -> Result<Tier1Permit, ConnectionLimitExceeded> {
		let Some(max_total) = self.inner.max_total else {
			return Ok(Tier1Permit { limiter: None });
		};
		let mut state = self.inner.state.lock().expect("SQL connection state lock");
		if state.global_active >= max_total.get() {
			state.global_rejections = state.global_rejections.saturating_add(1);
			return Err(ConnectionLimitExceeded);
		}
		state.global_active = state.global_active.saturating_add(1);
		Ok(Tier1Permit {
			limiter: Some(self.clone()),
		})
	}

	pub fn try_acquire_tier2(
		&self,
		tenant_id: TenantId,
	) -> Result<Tier2Permit, ConnectionLimitExceeded> {
		let budget = self.inner.tenant_policy.budget_for(tenant_id);
		let Some(max_connections) = budget.max_concurrent_connections else {
			return Ok(Tier2Permit {
				limiter: None,
				key: self.tenant_connection_key(tenant_id),
			});
		};
		let key = self.tenant_connection_key(tenant_id);
		let mut state = self.inner.state.lock().expect("SQL connection state lock");
		let active = state.tenant_active.get(&key).copied().unwrap_or(0);
		if active >= max_connections.get() {
			let rejections = state.tenant_rejections.entry(key).or_insert(0);
			*rejections = rejections.saturating_add(1);
			return Err(ConnectionLimitExceeded);
		}
		state.tenant_active.insert(key, active.saturating_add(1));
		Ok(Tier2Permit {
			limiter: Some(self.clone()),
			key,
		})
	}

	pub fn global_metrics(&self) -> GlobalSqlConnectionMetrics {
		let state = self.inner.state.lock().expect("SQL connection state lock");
		GlobalSqlConnectionMetrics {
			active: u64::from(state.global_active),
			rejections: state.global_rejections,
		}
	}

	pub fn tenant_metric_slots(&self) -> Vec<TenantSqlConnectionMetricSlot> {
		let state = self.inner.state.lock().expect("SQL connection state lock");
		let mut slots = Vec::new();
		for tenant_id in self.inner.tenant_policy.configured_tenant_ids() {
			let key = TenantConnectionKey::Configured(tenant_id);
			slots.push(TenantSqlConnectionMetricSlot {
				label: tenant_id.0.to_string(),
				active: u64::from(state.tenant_active.get(&key).copied().unwrap_or(0)),
				rejections: state.tenant_rejections.get(&key).copied().unwrap_or(0),
			});
		}
		let other_key = TenantConnectionKey::Other;
		slots.push(TenantSqlConnectionMetricSlot {
			label: "other".to_string(),
			active: u64::from(state.tenant_active.get(&other_key).copied().unwrap_or(0)),
			rejections: state
				.tenant_rejections
				.get(&other_key)
				.copied()
				.unwrap_or(0),
		});
		slots
	}

	fn tenant_connection_key(&self, tenant_id: TenantId) -> TenantConnectionKey {
		if self.inner.tenant_policy.is_configured_tenant(tenant_id) {
			TenantConnectionKey::Configured(tenant_id)
		} else {
			TenantConnectionKey::Other
		}
	}

	fn release_tier1(&self) {
		let mut state = self.inner.state.lock().expect("SQL connection state lock");
		state.global_active = state.global_active.saturating_sub(1);
	}

	fn release_tier2(&self, key: TenantConnectionKey) {
		let mut state = self.inner.state.lock().expect("SQL connection state lock");
		if let Some(active) = state.tenant_active.get_mut(&key) {
			*active = active.saturating_sub(1);
			if *active == 0 {
				state.tenant_active.remove(&key);
			}
		}
	}
}

impl Drop for Tier1Permit {
	fn drop(&mut self) {
		if let Some(limiter) = &self.limiter {
			limiter.release_tier1();
		}
	}
}

impl Drop for Tier2Permit {
	fn drop(&mut self) {
		if let Some(limiter) = &self.limiter {
			limiter.release_tier2(self.key);
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::num::NonZeroU32;

	use super::*;
	use crate::distributed::budget::{ByteCount, TenantBudget};

	fn budget(max_concurrent_connections: Option<u32>) -> TenantBudget {
		let mut budget = TenantBudget::new(ByteCount::new(8), ByteCount::new(32));
		budget.max_concurrent_connections = max_concurrent_connections.and_then(NonZeroU32::new);
		budget
	}

	fn limiter(
		max_total: Option<u32>,
		default_connections: Option<u32>,
		per_tenant_connections: Option<u32>,
	) -> SqlConnectionLimiter {
		let mut per_tenant = HashMap::new();
		if per_tenant_connections.is_some() {
			per_tenant.insert(TenantId(7), budget(per_tenant_connections));
		}
		SqlConnectionLimiter::new(
			max_total.and_then(NonZeroU32::new),
			TenantBudgetPolicy::new(budget(default_connections), per_tenant),
		)
	}

	#[test]
	fn default_disabled_connection_caps_preserve_current_connection_behavior() {
		let limiter = limiter(None, None, None);

		let _tier1_a = limiter.try_acquire_tier1().expect("disabled global");
		let _tier1_b = limiter.try_acquire_tier1().expect("disabled global");
		let _tenant_a = limiter
			.try_acquire_tier2(TenantId(7))
			.expect("disabled tenant");
		let _tenant_b = limiter
			.try_acquire_tier2(TenantId(9))
			.expect("disabled other");

		assert_eq!(limiter.global_metrics().active, 0);
		assert_eq!(limiter.global_metrics().rejections, 0);
		for slot in limiter.tenant_metric_slots() {
			assert_eq!(slot.active, 0);
			assert_eq!(slot.rejections, 0);
		}
	}

	#[test]
	fn global_sql_connection_cap_refuses_excess_incoming_before_handshake() {
		let limiter = limiter(Some(1), None, None);
		let permit = limiter.try_acquire_tier1().expect("first global permit");

		assert!(limiter.try_acquire_tier1().is_err());
		assert_eq!(limiter.global_metrics().active, 1);
		assert_eq!(limiter.global_metrics().rejections, 1);

		drop(permit);
		assert_eq!(limiter.global_metrics().active, 0);
		assert!(limiter.try_acquire_tier1().is_ok());
	}

	#[test]
	fn remote_sql_tenant_connection_cap_closes_excess_after_san_resolution() {
		let limiter = limiter(None, Some(1), Some(1));
		let permit = limiter
			.try_acquire_tier2(TenantId(7))
			.expect("first tenant permit");

		assert!(limiter.try_acquire_tier2(TenantId(7)).is_err());
		let slot = limiter
			.tenant_metric_slots()
			.into_iter()
			.find(|slot| slot.label == "7")
			.expect("configured tenant slot");
		assert_eq!(slot.active, 1);
		assert_eq!(slot.rejections, 1);

		drop(permit);
		assert!(limiter.try_acquire_tier2(TenantId(7)).is_ok());
	}

	#[test]
	fn loopback_sql_tenant_connection_cap_uses_config_tenant() {
		let limiter = limiter(None, Some(1), Some(1));
		let _permit = limiter
			.try_acquire_tier2(TenantId(7))
			.expect("loopback config tenant permit");

		let slot = limiter
			.tenant_metric_slots()
			.into_iter()
			.find(|slot| slot.label == "7")
			.expect("configured tenant slot");
		assert_eq!(slot.active, 1);
	}

	#[test]
	fn unconfigured_tenants_share_other_sql_connection_cap() {
		let limiter = limiter(None, Some(1), None);
		let _permit = limiter
			.try_acquire_tier2(TenantId(8))
			.expect("first other tenant permit");

		assert!(limiter.try_acquire_tier2(TenantId(9)).is_err());
		let slot = limiter
			.tenant_metric_slots()
			.into_iter()
			.find(|slot| slot.label == "other")
			.expect("other slot");
		assert_eq!(slot.active, 1);
		assert_eq!(slot.rejections, 1);
	}

	#[test]
	fn configured_tenant_connection_cap_isolated_from_other() {
		let limiter = limiter(None, Some(1), Some(1));
		let _configured = limiter
			.try_acquire_tier2(TenantId(7))
			.expect("configured permit");
		let _other = limiter
			.try_acquire_tier2(TenantId(9))
			.expect("other permit");

		assert!(limiter.try_acquire_tier2(TenantId(7)).is_err());
		assert!(limiter.try_acquire_tier2(TenantId(10)).is_err());
	}

	#[test]
	fn connection_permits_release_after_normal_close() {
		let limiter = limiter(Some(1), Some(1), Some(1));
		{
			let _tier1 = limiter.try_acquire_tier1().expect("tier1");
			let _tier2 = limiter.try_acquire_tier2(TenantId(7)).expect("tier2");
			assert_eq!(limiter.global_metrics().active, 1);
			assert_eq!(
				limiter
					.tenant_metric_slots()
					.into_iter()
					.find(|slot| slot.label == "7")
					.expect("tenant slot")
					.active,
				1
			);
		}

		assert_eq!(limiter.global_metrics().active, 0);
		assert_eq!(
			limiter
				.tenant_metric_slots()
				.into_iter()
				.find(|slot| slot.label == "7")
				.expect("tenant slot")
				.active,
			0
		);
	}

	#[test]
	fn global_connection_metrics_are_aggregate() {
		let limiter = limiter(Some(1), None, None);
		let _permit = limiter.try_acquire_tier1().expect("first permit");
		assert!(limiter.try_acquire_tier1().is_err());

		assert_eq!(
			limiter.global_metrics(),
			GlobalSqlConnectionMetrics {
				active: 1,
				rejections: 1,
			}
		);
	}

	#[test]
	fn tenant_connection_metrics_are_configured_plus_other() {
		let limiter = limiter(None, Some(1), Some(1));
		let _configured = limiter.try_acquire_tier2(TenantId(7)).expect("tenant 7");
		let _other = limiter.try_acquire_tier2(TenantId(9)).expect("other");
		assert!(limiter.try_acquire_tier2(TenantId(8)).is_err());

		let slots = limiter.tenant_metric_slots();
		assert_eq!(
			slots
				.iter()
				.map(|slot| slot.label.as_str())
				.collect::<Vec<_>>(),
			vec!["7", "other"]
		);
		assert!(!slots.iter().any(|slot| slot.label == "9"));
	}
}
