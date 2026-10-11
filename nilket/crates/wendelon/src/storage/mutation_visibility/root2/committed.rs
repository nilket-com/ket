//! Dormant committed-entry adapter for the classes that exist today: Blank, Membership and every
//! non-Root2 request. Position, version-1 identity and class are all derived from the real
//! openraft entry; no caller supplies any of them. Whether an entry is committed is not decided
//! here: that is the fact a `CommittedSource2` stands for, and no runtime code can mint one until
//! a reviewed integration reads the retained Control log from genesis under a separately trusted
//! committed bound. This authenticates neither commitment nor history.
use crate::consensus::{WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::entry_identity::{
	EntryIdentityError2, EntryPosition2, blank_entry_identity2, membership_entry_identity2,
	normal_entry_identity2,
};
use crate::storage::mutation_visibility::root2::origin::EntryClass2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use openraft::{Entry, EntryPayload};

/// Capability: the entries passed with it were read, in index order, from the committed retained
/// Control log. Private fields and a test-only mint: unreachable at runtime until reviewed.
pub(in crate::storage::mutation_visibility::root2) struct CommittedSource2 {
	_scope: (),
}
impl CommittedSource2 {
	#[cfg(test)]
	pub(in crate::storage::mutation_visibility::root2) fn synthetic() -> Self {
		Self { _scope: () }
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AdapterError2 {
	Identity(EntryIdentityError2),
}

/// The committed entry for `entry`. Ordering, positivity, genesis and re-scan rules stay with the
/// consumer (`advance_pre_root2`).
pub(in crate::storage::mutation_visibility::root2) fn committed_entry2(
	source: &CommittedSource2,
	entry: &Entry<WendelonTypeConfig>,
) -> Result<CommittedEntry2, AdapterError2> {
	let position = EntryPosition2 {
		term: entry.log_id.leader_id.term,
		node: entry.log_id.leader_id.node_id,
		index: entry.log_id.index,
	};
	let (identity, class) = match &entry.payload {
		EntryPayload::Blank => (blank_entry_identity2(position), EntryClass2::Blank),
		EntryPayload::Membership(m) => (
			membership_entry_identity2(position, m).map_err(AdapterError2::Identity)?,
			EntryClass2::Membership,
		),
		EntryPayload::Normal(r) => (
			normal_entry_identity2(position, r).map_err(AdapterError2::Identity)?,
			request_class(r),
		),
	};
	Ok(CommittedEntry2::from_source(
		source, position, identity, class,
	))
}

// Every variant by name and no wildcard: a new request variant (a Root2 carrier included) fails
// to compile here until its class is decided.
fn request_class(request: &WendelonRequest) -> EntryClass2 {
	match request {
		WendelonRequest::Finalize(_)
		| WendelonRequest::AcquireLease(_)
		| WendelonRequest::ReleaseLease(_)
		| WendelonRequest::AdvanceEpoch(_)
		| WendelonRequest::SchemaUpdate(_)
		| WendelonRequest::TierSlab(_)
		| WendelonRequest::RetireTieredSlab(_)
		| WendelonRequest::CompactionCommit(_)
		| WendelonRequest::SeedMigration(_)
		| WendelonRequest::MigrationComplete(_)
		| WendelonRequest::CreateTenant(_)
		| WendelonRequest::UpdateTenantQuota(_)
		| WendelonRequest::SuspendTenant(_)
		| WendelonRequest::ResumeTenant(_)
		| WendelonRequest::DeleteTenant(_)
		| WendelonRequest::InstallTenantKekVersion(_)
		| WendelonRequest::RetireTenantKekVersion(_)
		| WendelonRequest::RevokeActorKey(_)
		| WendelonRequest::SetTableRetentionPolicy(_)
		| WendelonRequest::TableDropPrepare(_)
		| WendelonRequest::TableReclaimCompletion(_)
		| WendelonRequest::TableDropFinalize(_)
		| WendelonRequest::CtasSaga(_)
		| WendelonRequest::ReplayLegacyCtasRowDispatched(_)
		| WendelonRequest::TableDropLogicalFinalize(_)
		| WendelonRequest::TableDropSettlement(_) => EntryClass2::NonRoot2,
	}
}
