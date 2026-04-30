use crate::schema::{
	HybridTimestamp, Identity, RowIdentityPolicy, TableSchemaDescriptorV1, TenantId,
};
use crate::storage::core::{RecordLocation, StorageEngine};
use crate::storage::manager::SlabManager;
use crate::storage::retention::{EffectiveRetention, row_visible_under_retention};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PkAdmissionOutcome {
	InsertedNewVisible { latest_was_tombstone: bool },
	UpsertedAlreadyVisible,
	DeletedSupersedingVisible,
	DeleteAbsorbedNotVisible { latest_was_tombstone: bool },
}

pub(crate) fn row_visible_for_admission(
	descriptor: &TableSchemaDescriptorV1,
	tenant_id: TenantId,
	key_identity: Identity,
	admission_hlc: HybridTimestamp,
	effective_retention: &EffectiveRetention,
	row_index: &SlabManager,
) -> bool {
	match descriptor.row_identity_policy {
		RowIdentityPolicy::PrimaryKey(_) => row_index
			.lookup_as_of(tenant_id, key_identity, admission_hlc)
			.is_some_and(|location| !location.is_tombstone),
		RowIdentityPolicy::AppendOnly | RowIdentityPolicy::AppendOnlyContentHash => row_index
			.lookup(tenant_id, key_identity)
			.is_some_and(|location| {
				!location.is_tombstone
					&& row_visible_under_retention(location.hlc, effective_retention)
			}),
	}
}

pub(crate) fn pk_delete_outcome_for_latest(
	latest_as_of_delete: Option<RecordLocation>,
) -> PkAdmissionOutcome {
	match latest_as_of_delete {
		Some(location) if location.is_tombstone => PkAdmissionOutcome::DeleteAbsorbedNotVisible {
			latest_was_tombstone: true,
		},
		Some(_) => PkAdmissionOutcome::DeletedSupersedingVisible,
		None => PkAdmissionOutcome::DeleteAbsorbedNotVisible {
			latest_was_tombstone: false,
		},
	}
}

pub(crate) fn pk_write_outcome_for_latest(
	latest_as_of_write: Option<RecordLocation>,
) -> PkAdmissionOutcome {
	match latest_as_of_write {
		Some(location) if !location.is_tombstone => PkAdmissionOutcome::UpsertedAlreadyVisible,
		Some(_) => PkAdmissionOutcome::InsertedNewVisible {
			latest_was_tombstone: true,
		},
		None => PkAdmissionOutcome::InsertedNewVisible {
			latest_was_tombstone: false,
		},
	}
}

pub(crate) fn admit_pk_write_row(
	descriptor: &TableSchemaDescriptorV1,
	tenant_id: TenantId,
	key_identity: Identity,
	admission_hlc: HybridTimestamp,
	row_index: &SlabManager,
) -> PkAdmissionOutcome {
	match descriptor.row_identity_policy {
		RowIdentityPolicy::PrimaryKey(_) => pk_write_outcome_for_latest(row_index.lookup_as_of(
			tenant_id,
			key_identity,
			admission_hlc,
		)),
		RowIdentityPolicy::AppendOnly | RowIdentityPolicy::AppendOnlyContentHash => {
			unreachable!("PK write admission only accepts primary-key row identity policy")
		}
	}
}

pub(crate) fn admit_pk_delete_row(
	descriptor: &TableSchemaDescriptorV1,
	tenant_id: TenantId,
	key_identity: Identity,
	delete_hlc: HybridTimestamp,
	effective_retention: &EffectiveRetention,
	row_index: &SlabManager,
) -> PkAdmissionOutcome {
	if row_visible_for_admission(
		descriptor,
		tenant_id,
		key_identity,
		delete_hlc,
		effective_retention,
		row_index,
	) {
		return PkAdmissionOutcome::DeletedSupersedingVisible;
	}

	pk_delete_outcome_for_latest(row_index.lookup_as_of(tenant_id, key_identity, delete_hlc))
}
