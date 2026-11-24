//! A member's tenant admission policy: the cache of each tenant's lifecycle state, quota and
//! bytes, its refresh from the applied state, and the read and write admissions that consult
//! it. Wendelon ADR 0129: a tenant absent from the cache is looked up in the member's applied
//! state before it is refused.
//!
//! The server's handle on the cache and the reservation a write holds stay with the server's
//! other state in the root; the cache's types and the mechanism are here.

use super::*;

/// One tenant's admission policy as this member last read it. `used_bytes` is the applied
/// used bytes at that read plus what this member's committed writes have added since.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CachedTenantPolicy {
	pub(super) state: TenantLifecycleState,
	pub(super) quota_bytes: u64,
	pub(super) used_bytes: u64,
	pub(super) replication_factor: u32,
	pub(super) mean_write_size_bytes: u64,
	/// Wendelon ADR 0129: the applied log index this member's state stood at when the entry
	/// was read from it. An entry is never replaced by one read at an earlier index.
	pub(super) applied_index: Option<u64>,
}

impl Default for CachedTenantPolicy {
	fn default() -> Self {
		Self {
			state: TenantLifecycleState::Creating,
			quota_bytes: 0,
			used_bytes: 0,
			replication_factor: 0,
			mean_write_size_bytes: 1,
			applied_index: None,
		}
	}
}

#[derive(Debug, Default)]
pub(super) struct TenantPolicyCacheState {
	pub(super) policies: HashMap<TenantId, CachedTenantPolicy>,
	pub(super) last_refresh: Option<Instant>,
}

impl WendelonDataServer {
	pub async fn sync_tenant_policy_cache_once(&self) -> usize {
		let snapshot = self.state_machine.state_snapshot().await;
		let applied_index = snapshot.last_applied_log_id.map(|id| id.index);
		let mut guard = recover_rwlock_write(&self.tenant_policy_cache, "tenant_policy_cache");
		let mut next = HashMap::with_capacity(snapshot.tenants.len());
		for (tenant_id, record) in snapshot.tenants {
			let mean_write_size_bytes = guard
				.policies
				.get(&tenant_id)
				.map(|cached| cached.mean_write_size_bytes)
				.unwrap_or(1)
				.max(1);
			next.insert(
				tenant_id,
				CachedTenantPolicy {
					state: record.state,
					quota_bytes: record.quota_bytes,
					used_bytes: record.used_bytes,
					replication_factor: record.replication_factor,
					mean_write_size_bytes,
					applied_index,
				},
			);
		}
		// Wendelon ADR 0129: an entry admitted from a later applied state than this snapshot's
		// (a miss answered while this refresh waited for its snapshot) stays as it is, with
		// its local charge; this snapshot is the older view of that tenant.
		for (tenant_id, cached) in &guard.policies {
			if cached.applied_index > applied_index {
				next.insert(*tenant_id, *cached);
			}
		}
		guard.policies = next;
		guard.last_refresh = Some(Instant::now());
		guard.policies.len()
	}

	pub fn spawn_tenant_policy_reconciler(self: Arc<Self>, tick: Duration) -> JoinHandle<()> {
		tokio::spawn(async move {
			let _ = self.sync_tenant_policy_cache_once().await;
			let mut ticker = tokio::time::interval(tick);
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				ticker.tick().await;
				let _ = self.sync_tenant_policy_cache_once().await;
			}
		})
	}

	pub(super) async fn ensure_tenant_policy_fresh(&self) {
		let refresh_needed = {
			let guard = recover_rwlock_read(&self.tenant_policy_cache, "tenant_policy_cache");
			guard
				.last_refresh
				.is_none_or(|last| last.elapsed() >= self.tenant_cache_refresh_interval)
		};
		if refresh_needed {
			let _ = self.sync_tenant_policy_cache_once().await;
		}
	}

	/// The tenant's admission policy on this member.
	///
	/// Wendelon ADR 0129: a miss in the cache is not an answer while the cache may be older
	/// than this member's applied state (it is refreshed at an interval). On a miss the one
	/// tenant is looked up in the applied state, under its read lock and without cloning it,
	/// and its entry is placed in the cache **under that same lock**:
	/// - absent there, the tenant is unknown to this member: `None`, and nothing is kept;
	/// - present, and the cache still without it: the entry is placed from what the applied
	///   state holds at that moment, and answers this and later calls;
	/// - present, and the cache now with it (another miss placed it meanwhile): that entry is
	///   the answer, as on a hit, with whatever this member has charged to it.
	///
	/// Because the read and the placing are one step under the applied state's lock, no two
	/// misses hold different views of one tenant: an entry is placed once, from the applied
	/// bytes, and is only added to afterwards. Lock order: the applied state's read lock, then
	/// the cache's lock, held for the map operation only; nothing takes them the other way.
	///
	/// A hit is answered from the cache alone, as before.
	pub(super) async fn cached_tenant_policy(
		&self,
		tenant_id: TenantId,
	) -> Option<CachedTenantPolicy> {
		self.ensure_tenant_policy_fresh().await;
		let cached = recover_rwlock_read(&self.tenant_policy_cache, "tenant_policy_cache")
			.policies
			.get(&tenant_id)
			.copied();
		if cached.is_some() {
			return cached;
		}
		#[cfg(test)]
		miss_pause::missed(self.local_node.id, tenant_id).await;
		self.state_machine
			.read_state(|state| {
				let record = state.tenants.get(&tenant_id)?;
				let mut guard =
					recover_rwlock_write(&self.tenant_policy_cache, "tenant_policy_cache");
				Some(
					*guard
						.policies
						.entry(tenant_id)
						.or_insert(CachedTenantPolicy {
							state: record.state,
							quota_bytes: record.quota_bytes,
							used_bytes: record.used_bytes,
							replication_factor: record.replication_factor,
							mean_write_size_bytes: 1,
							applied_index: state.last_applied_log_id.map(|id| id.index),
						}),
				)
			})
			.await
	}

	pub(super) async fn enforce_write_policy(
		&self,
		tenant_id: TenantId,
		estimated_bytes: u64,
	) -> Result<TenantWriteReservation, WriteStatus> {
		let Some(policy) = self.cached_tenant_policy(tenant_id).await else {
			return Err(WriteStatus::TENANT_UNAVAILABLE);
		};
		match policy.state {
			TenantLifecycleState::Active => {}
			TenantLifecycleState::Suspended => return Err(WriteStatus::TENANT_SUSPENDED),
			TenantLifecycleState::Creating
			| TenantLifecycleState::Deleting
			| TenantLifecycleState::Deleted => return Err(WriteStatus::TENANT_UNAVAILABLE),
		}
		if policy.used_bytes >= policy.quota_bytes {
			return Err(WriteStatus::QUOTA_EXCEEDED);
		}

		let inflight = self
			.tenant_inflight
			.get(&tenant_id)
			.map(|entry| *entry.value())
			.unwrap_or_default();
		if inflight.count >= DEFAULT_TENANT_MAX_INFLIGHT_WRITES {
			return Err(WriteStatus::QUOTA_EXCEEDED);
		}
		let mean_write_size = policy.mean_write_size_bytes.max(estimated_bytes).max(1);
		let overshoot_budget =
			(DEFAULT_TENANT_MAX_INFLIGHT_WRITES as u64).saturating_mul(mean_write_size);
		let projected = policy
			.used_bytes
			.saturating_add(inflight.bytes)
			.saturating_add(estimated_bytes);
		if projected > policy.quota_bytes.saturating_add(overshoot_budget) {
			return Err(WriteStatus::QUOTA_EXCEEDED);
		}

		self.tenant_inflight
			.entry(tenant_id)
			.and_modify(|entry| {
				entry.count = entry.count.saturating_add(1);
				entry.bytes = entry.bytes.saturating_add(estimated_bytes);
			})
			.or_insert(TenantInflight {
				count: 1,
				bytes: estimated_bytes,
			});
		Ok(TenantWriteReservation {
			inflight: self.tenant_inflight.clone(),
			tenant_id,
			bytes: estimated_bytes,
			active: true,
		})
	}

	pub(super) async fn enforce_read_policy(&self, tenant_id: TenantId) -> Result<(), ReadStatus> {
		let Some(policy) = self.cached_tenant_policy(tenant_id).await else {
			return Err(ReadStatus::TENANT_UNAVAILABLE);
		};
		if policy.state.allows_reads() {
			return Ok(());
		}
		Err(ReadStatus::TENANT_UNAVAILABLE)
	}

	pub(super) fn note_committed_write(&self, tenant_id: TenantId, charged_bytes: u64) {
		let mut guard = recover_rwlock_write(&self.tenant_policy_cache, "tenant_policy_cache");
		let Some(policy) = guard.policies.get_mut(&tenant_id) else {
			return;
		};
		// Conservatively anticipate the next reconciler sweep so a
		// freshly-committed local write tightens quota admission
		// immediately instead of waiting for the authoritative cache
		// refresh.
		policy.used_bytes = policy.used_bytes.saturating_add(charged_bytes);
		policy.mean_write_size_bytes = if policy.mean_write_size_bytes == 0 {
			charged_bytes.max(1)
		} else {
			((policy.mean_write_size_bytes.saturating_mul(7)).saturating_add(charged_bytes)) / 8
		}
		.max(1);
	}
}

/// Test only: holds a request that has just missed the cache for one tenant on one member,
/// before it reads the applied state, so a control can order misses and a write exactly.
#[cfg(test)]
pub(super) mod miss_pause {
	use crate::schema::TenantId;
	use std::sync::Mutex;
	use tokio::sync::{mpsc, oneshot};

	type Arrivals = mpsc::UnboundedSender<oneshot::Sender<()>>;
	static ARMED: Mutex<Option<(u32, TenantId, Arrivals)>> = Mutex::new(None);

	/// Arms the pause: each miss for `tenant` on `node` then sends its resume handle, in the
	/// order the misses arrived, and waits for it.
	pub(crate) fn arm(node: u32, tenant: TenantId) -> mpsc::UnboundedReceiver<oneshot::Sender<()>> {
		let (arrivals, arrived) = mpsc::unbounded_channel();
		*ARMED.lock().unwrap() = Some((node, tenant, arrivals));
		arrived
	}

	pub(crate) fn disarm() {
		*ARMED.lock().unwrap() = None;
	}

	pub(super) async fn missed(node: u32, tenant: TenantId) {
		let resume = {
			let armed = ARMED.lock().unwrap();
			match &*armed {
				Some((at, of, arrivals)) if *at == node && *of == tenant => {
					let (resume, resumed) = oneshot::channel();
					arrivals.send(resume).ok().map(|()| resumed)
				}
				_ => None,
			}
		};
		if let Some(resumed) = resume {
			let _ = resumed.await;
		}
	}
}
