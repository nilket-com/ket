//! The backup roster: what one settled cut of the replicated state holds that a
//! restore must reproduce, or the named reason the whole backup is refused.
//!
//! [`roster_at`] is a pure function of the applied state. The leader answers
//! the operator's roster request with it under the backup hold; the store-side
//! KV check (the certified base) is separate, because only a store has a base.

use crate::consensus::core::{MigrationState, TenantLifecycleState};
use crate::consensus::decisions::{ParentNamespace, ParentStatus};
use crate::consensus::raft_storage::SmState;
use crate::integration::compute::LogicalTypeDefinition;
use crate::schema::{
	SecondaryIndexDescriptor, SecondaryIndexState, TableObjectId, TableRetentionPolicy,
	TableSchemaDescriptor, TenantId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One tenant's settings a restore establishes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterTenant {
	pub tenant_id: TenantId,
	pub quota_bytes: u64,
	pub replication_factor: u32,
}

/// One live table: its latest descriptor and its exact retention policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterTable {
	pub tenant_id: TenantId,
	pub descriptor: TableSchemaDescriptor,
	pub retention: TableRetentionPolicy,
}

/// Everything a restore must reproduce, read at one settled cut under one hold.
///
/// Order is the restore's creation order: tenants ascending; per tenant, logical
/// types by ascending id (a fresh catalog assigns the same ids in that order, and
/// a delta unit precedes the point unit naming it), then tables, then indexes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRoster {
	/// The hold this roster was read under: (`id`, `entered_at`).
	pub hold: (u128, u64),
	/// The applied index the roster was read at (R).
	pub cut: u64,
	pub tenants: Vec<RosterTenant>,
	pub logical_types: Vec<(TenantId, LogicalTypeDefinition)>,
	pub tables: Vec<RosterTable>,
	pub indexes: Vec<(TenantId, SecondaryIndexDescriptor)>,
}

/// Why no roster is given at this cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RosterRefusal {
	/// The state is not in ParentDecisions mode.
	NotParentDecisions,
	/// The named hold is not the current hold.
	HoldMismatch { current: Option<(u128, u64)> },
	/// Not settled yet, and the hold does not block what settles it: ask again.
	Unsettled { why: String },
	/// Not settled, and what would settle it cannot happen under the hold (or
	/// in this mode): the backup is refused now.
	LifecycleNotSettled { what: String },
	/// A table has a retention floor.
	RetentionActive {
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	},
	/// A setting the target cannot reproduce through a supported route.
	SettingNotReproducible { what: String },
	/// KV data exists (a retained KV parent, any status, or a KV base entry).
	KvDataPresent,
	/// The store-side KV check could not conclude under one read authority.
	KvCheckInconclusive { why: String },
}

/// The roster at the state's applied index, for the hold `token`.
pub(crate) fn roster_at(
	state: &SmState,
	token: (u128, u64),
) -> Result<BackupRoster, RosterRefusal> {
	let decisions = state
		.decisions
		.as_ref()
		.ok_or(RosterRefusal::NotParentDecisions)?;
	let current = decisions.backup_hold();
	if current != Some(token) {
		return Err(RosterRefusal::HoldMismatch { current });
	}
	let cut = state.last_applied_log_id.map_or(0, |id| id.index);

	// Decision bodies: no KV parent at all; no parent still Reserved (its
	// Finalize/Resolve/Abort is admitted under the hold, so it can settle).
	let mut reserved = 0usize;
	for body in decisions.bodies().values() {
		if body.claim.key.namespace == ParentNamespace::Kv {
			return Err(RosterRefusal::KvDataPresent);
		}
		// Its status in this state, whatever index the state was read at.
		if matches!(body.status_at(u64::MAX), Some(ParentStatus::Reserved)) {
			reserved += 1;
		}
	}

	// Lifecycles the hold or this mode keeps from completing: refused now.
	let mut tenants = Vec::new();
	let mut tenant_ids: Vec<_> = state.tenants.keys().copied().collect();
	tenant_ids.sort();
	for tenant_id in tenant_ids {
		let record = &state.tenants[&tenant_id];
		if record.state != TenantLifecycleState::Active {
			return Err(RosterRefusal::LifecycleNotSettled {
				what: format!("tenant {} is {:?}", tenant_id.get(), record.state),
			});
		}
		tenants.push(RosterTenant {
			tenant_id,
			quota_bytes: record.quota_bytes,
			replication_factor: record.replication_factor,
		});
	}
	if let Some(entry) = state
		.migrations
		.values()
		.find(|entry| entry.state != MigrationState::Completed)
	{
		return Err(RosterRefusal::LifecycleNotSettled {
			what: format!("migration {} is {:?}", entry.migration_id, entry.state),
		});
	}
	// Wendelon ADR 0118 D5: a proved, logically finalized drop is skipped; any
	// other drop state refuses, as before.
	let finalized = finalized_drops(state, cut)?;
	// Nothing outside the tenant roster: every catalog object's tenant is a
	// live tenant, so iterating tenants below sees every object.
	let known: BTreeSet<_> = tenants.iter().map(|tenant| tenant.tenant_id).collect();
	if !state.table_schemas.tenant_ids().is_subset(&known)
		|| !state.logical_types.tenant_ids().is_subset(&known)
		|| state
			.secondary_indexes
			.rows()
			.iter()
			.any(|(tenant, _)| !known.contains(tenant))
	{
		return Err(RosterRefusal::LifecycleNotSettled {
			what: "a catalog object belongs to no live tenant".into(),
		});
	}
	if !state.ctas_sagas.is_empty() {
		return Err(RosterRefusal::LifecycleNotSettled {
			what: "a create-table-as-select operation is recorded".into(),
		});
	}

	// Tables: every live descriptor is the latest one bound to its name.
	let mut logical_types = Vec::new();
	let mut tables = Vec::new();
	let mut indexes = Vec::new();
	let mut unsettled_index = None;
	for tenant in &tenants {
		let tenant_id = tenant.tenant_id;
		for definition in state.logical_types.definitions_for_tenant(tenant_id) {
			logical_types.push((tenant_id, definition));
		}
		let mut named = state.table_schemas.latest_descriptors_for_tenant(tenant_id);
		named.sort_by_key(|descriptor| descriptor.table_object_id);
		let named_ids: BTreeSet<_> = named.iter().map(|d| d.table_object_id).collect();
		let live_ids: BTreeSet<_> = state
			.table_schemas
			.descriptors_for_tenant(tenant_id)
			.into_iter()
			.map(|descriptor| descriptor.table_object_id)
			// A proved finalized drop's descriptor stays, unbound: it is gone.
			.filter(|table| !finalized.contains(&(tenant_id, *table)))
			.collect();
		if named_ids != live_ids {
			return Err(RosterRefusal::LifecycleNotSettled {
				what: format!(
					"tenant {} has a table object no name is bound to",
					tenant_id.get()
				),
			});
		}
		for descriptor in named {
			let table_object_id = descriptor.table_object_id;
			if state
				.table_schemas
				.is_provisional(tenant_id, table_object_id)
			{
				return Err(RosterRefusal::LifecycleNotSettled {
					what: "a provisional table exists".into(),
				});
			}
			let retention = match state
				.table_lifecycle
				.retention_policy(tenant_id, table_object_id)
			{
				Some(TableRetentionPolicy::InheritCluster) => TableRetentionPolicy::InheritCluster,
				Some(TableRetentionPolicy::FixedWindow(_)) => {
					return Err(RosterRefusal::RetentionActive {
						tenant_id,
						table_object_id,
					});
				}
				// A table create establishes InheritCluster, and the setter is
				// refused in this mode: no route reproduces anything else.
				other => {
					return Err(RosterRefusal::SettingNotReproducible {
						what: format!("table retention policy {other:?}"),
					});
				}
			};
			tables.push(RosterTable {
				tenant_id,
				descriptor,
				retention,
			});
		}
		let mut rows = state.secondary_indexes.for_tenant(tenant_id);
		rows.sort_by_key(|row| row.descriptor.index_object_id);
		for row in rows {
			match row.state {
				SecondaryIndexState::Ready => {}
				// Completed by replica lifecycle updates, which the hold admits.
				SecondaryIndexState::Building | SecondaryIndexState::Dropping => {
					unsettled_index.get_or_insert_with(|| {
						format!("index {} is {:?}", row.descriptor.index_name, row.state)
					});
					continue;
				}
			}
			let table = state
				.table_schemas
				.latest_by_id(tenant_id, row.descriptor.table_object_id)
				.ok_or_else(|| RosterRefusal::LifecycleNotSettled {
					what: format!("index {} names no live table", row.descriptor.index_name),
				})?;
			// A create is validated against the table's latest generation; an
			// index made before an alter cannot be created again as it is.
			if row.descriptor.validate_against_table(table).is_err() {
				return Err(RosterRefusal::SettingNotReproducible {
					what: format!(
						"index {} was built against an earlier table generation",
						row.descriptor.index_name
					),
				});
			}
			indexes.push((tenant_id, row.descriptor.clone()));
		}
	}

	if reserved > 0 {
		return Err(RosterRefusal::Unsettled {
			why: format!("{reserved} parent(s) still reserved"),
		});
	}
	if let Some(why) = unsettled_index {
		return Err(RosterRefusal::Unsettled { why });
	}
	Ok(BackupRoster {
		hold: token,
		cut,
		tenants,
		logical_types,
		tables,
		indexes,
	})
}

/// Wendelon ADR 0118 D5: the tables whose drop is proved logically finalized at
/// `cut`. Any fence, and any outcome, tombstone or reclaim debt whose table is not
/// proved, refuses the roster: each of the three is a candidate.
fn finalized_drops(
	state: &SmState,
	cut: u64,
) -> Result<BTreeSet<(TenantId, TableObjectId)>, RosterRefusal> {
	let in_progress = || RosterRefusal::LifecycleNotSettled {
		what: "a table drop is in progress".into(),
	};
	if state.table_drop_fences.iter().next().is_some() {
		return Err(in_progress());
	}
	let keys: BTreeSet<_> = state
		.table_drop_outcomes
		.iter()
		.map(|outcome| outcome.operation_id)
		.chain(
			state
				.table_drop_tombstones
				.iter()
				.map(|tombstone| tombstone.operation_id),
		)
		.chain(
			state
				.table_drop_reclaim_debts
				.iter()
				.map(|debt| debt.operation_id),
		)
		.map(|operation| (operation.tenant_id, operation.table_object_id))
		.collect();
	for &(tenant_id, table_object_id) in &keys {
		if !proved_finalized(state, tenant_id, table_object_id, cut) {
			return Err(in_progress());
		}
	}
	Ok(keys)
}

/// Wendelon ADR 0118 D5: `(tenant, table)`'s drop is logically finalized at or
/// before `cut`, exactly: its permanent outcome, tombstone and reclaim debt name
/// one operation (tenant, table and schema generation) and one finalize index,
/// the tombstone's and the debt's fence index agree, the descriptor at that
/// generation is present with no name bound to it, and nothing else of the
/// table remains: no lifecycle record, no materialization certificate, no
/// secondary index (live or dropping). A fence is checked by the caller.
fn proved_finalized(
	state: &SmState,
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	cut: u64,
) -> bool {
	let (Some(outcome), Some(tombstone), Some(debt)) = (
		state.table_drop_outcomes.get(tenant_id, table_object_id),
		state.table_drop_tombstones.get(tenant_id, table_object_id),
		state
			.table_drop_reclaim_debts
			.get(tenant_id, table_object_id),
	) else {
		return false;
	};
	let operation = outcome.operation_id;
	let finalize = outcome.finalize_log_index;
	let records_agree = operation.tenant_id == tenant_id
		&& operation.table_object_id == table_object_id
		&& tombstone.operation_id == operation
		&& debt.operation_id == operation
		&& tombstone.finalize_log_index == finalize
		&& debt.finalize_log_index == finalize
		&& tombstone.fence_log_index == debt.fence_log_index
		&& finalize <= cut;
	let unbound_descriptor = state
		.table_schemas
		.latest_by_id(tenant_id, table_object_id)
		.is_some_and(|descriptor| {
			descriptor.schema_generation == operation.schema_generation.get()
				&& state
					.table_schemas
					.latest_by_name(
						tenant_id,
						descriptor.namespace.as_str(),
						descriptor.table_name.as_str(),
					)
					.is_none_or(|bound| bound.table_object_id != table_object_id)
		});
	let nothing_else_remains = state
		.table_lifecycle
		.get(tenant_id, table_object_id)
		.is_none()
		&& state
			.materialization_certificates
			.get(tenant_id, table_object_id)
			.is_none()
		&& !state
			.secondary_indexes
			.for_tenant(tenant_id)
			.iter()
			.any(|row| row.descriptor.table_object_id == table_object_id);
	records_agree && unbound_descriptor && nothing_else_remains
}
