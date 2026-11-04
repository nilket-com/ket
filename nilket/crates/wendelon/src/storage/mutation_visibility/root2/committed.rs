//! Dormant committed-entry adapter. Blank, Membership and legacy requests retain their classes;
//! a carrier supplies one decoded command, its envelope context and its borrowed sidecar.
//! Position, version-1 identity and class are all derived from the real
//! openraft entry; no caller supplies any of them. Whether an entry is committed is not decided
//! here: that is the fact a `CommittedSource2` stands for. The private dormant replay alone can
//! mint one inside a lexical run and use it only on validated acquired batches (and the initial
//! validated boundary). No production caller starts that replay. This adapter authenticates
//! neither commitment nor history.
use crate::consensus::{WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::entry_identity::{
	EntryIdentityError2, EntryPosition2, blank_entry_identity2, membership_entry_identity2,
	normal_entry_identity2,
};
use crate::storage::mutation_visibility::root2::origin::EntryClass2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use openraft::{Entry, EntryPayload};

/// Structural sidecar bound only; the dormant governance verifier remains unwired.
pub(in crate::storage::mutation_visibility::root2) const GOVERNANCE_SIDECAR_BYTES2: usize = 296;

/// Capability: the entries passed with it were read, in index order, from the committed retained
/// Control log. Private fields; the runtime mint is restricted to the private dormant replay.
pub(in crate::storage::mutation_visibility::root2) struct CommittedSource2 {
	_scope: (),
}
impl CommittedSource2 {
	pub(in crate::storage::mutation_visibility::root2) fn acquired(
		_: &crate::storage::mutation_visibility::root2::replay::PrefixRun2,
	) -> Self {
		Self { _scope: () }
	}

	#[cfg(test)]
	pub(in crate::storage::mutation_visibility::root2) fn synthetic() -> Self {
		Self { _scope: () }
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AdapterError2 {
	Identity(EntryIdentityError2),
}

/// One decoded command and the sidecar borrowed from that same immutable request.
pub(in crate::storage::mutation_visibility::root2) struct CarrierCommand2<'e> {
	generation: [u8; 16],
	cluster: [u8; 16],
	command: Command2,
	governance: Option<&'e [u8]>,
}
impl<'e> CarrierCommand2<'e> {
	pub(in crate::storage::mutation_visibility::root2) fn into_parts(
		self,
	) -> ([u8; 16], [u8; 16], Command2, Option<&'e [u8]>) {
		(self.generation, self.cluster, self.command, self.governance)
	}
}
/// Private replay adaptation result, including ordinary entries and malformed raw carriers.
/// No constructor escapes the one capability-gated adapter.
pub(in crate::storage::mutation_visibility::root2) struct AdaptedEntry2<'e> {
	committed: CommittedEntry2,
	raw_carrier: bool,
	carrier: Option<CarrierCommand2<'e>>,
}
impl<'e> AdaptedEntry2<'e> {
	pub(in crate::storage::mutation_visibility::root2) fn committed(&self) -> &CommittedEntry2 {
		&self.committed
	}
	pub(in crate::storage::mutation_visibility::root2) fn is_carrier(&self) -> bool {
		self.raw_carrier
	}
	pub(in crate::storage::mutation_visibility::root2) fn into_parts(
		self,
	) -> (CommittedEntry2, bool, Option<CarrierCommand2<'e>>) {
		(self.committed, self.raw_carrier, self.carrier)
	}
	#[cfg(test)]
	pub(in crate::storage::mutation_visibility::root2) fn into_committed(self) -> CommittedEntry2 {
		self.committed
	}
}

/// The committed entry for `entry`. Ordering, positivity, genesis and re-scan rules stay with the
/// consumer (`advance_pre_root2`).
pub(in crate::storage::mutation_visibility::root2) fn committed_entry2<'e>(
	source: &CommittedSource2,
	entry: &'e Entry<WendelonTypeConfig>,
) -> Result<AdaptedEntry2<'e>, AdapterError2> {
	let position = EntryPosition2 {
		term: entry.log_id.leader_id.term,
		node: entry.log_id.leader_id.node_id,
		index: entry.log_id.index,
	};
	let (identity, class, carrier) = match &entry.payload {
		EntryPayload::Blank => (blank_entry_identity2(position), EntryClass2::Blank, None),
		EntryPayload::Membership(m) => (
			membership_entry_identity2(position, m).map_err(AdapterError2::Identity)?,
			EntryClass2::Membership,
			None,
		),
		EntryPayload::Normal(r) => {
			let identity = normal_entry_identity2(position, r).map_err(AdapterError2::Identity)?;
			let (class, carrier) = request_class(r);
			(identity, class, carrier)
		}
	};
	let raw_carrier = matches!(
		&entry.payload,
		EntryPayload::Normal(WendelonRequest::Root2Command(_))
	);
	#[cfg(test)]
	let class = {
		if raw_carrier {
			class
		} else {
			fixture::class(position, identity).unwrap_or(class)
		}
	};
	Ok(AdaptedEntry2 {
		committed: CommittedEntry2::from_source(source, position, identity, class),
		raw_carrier,
		carrier,
	})
}

// Every variant by name and no wildcard: a new request variant (a Root2 carrier included) fails
// to compile here until its class is decided.
fn request_class(request: &WendelonRequest) -> (EntryClass2, Option<CarrierCommand2<'_>>) {
	match request {
		WendelonRequest::Root2Command(carrier) => {
			let Ok((generation, cluster, command)) = Command2::decode(carrier.command()) else {
				return (EntryClass2::MalformedRoot2, None);
			};
			let governance = carrier.governance_bytes();
			if governance.is_some_and(|bytes| {
				bytes.len() != GOVERNANCE_SIDECAR_BYTES2
					&& bytes.len() != super::role_command::ROLE_PROOF_BYTES3
			}) {
				return (EntryClass2::MalformedRoot2, None);
			}
			let Ok(digest) = command.digest(generation, cluster) else {
				return (EntryClass2::MalformedRoot2, None);
			};
			let class = if matches!(command, Command2::Bootstrap { .. }) {
				EntryClass2::Bootstrap(digest)
			} else {
				EntryClass2::OtherRoot2(digest)
			};
			(
				class,
				Some(CarrierCommand2 {
					generation,
					cluster,
					command,
					governance,
				}),
			)
		}
		WendelonRequest::Finalize(_)
		| WendelonRequest::AcquireLease(_)
		| WendelonRequest::ReleaseLease(_)
		| WendelonRequest::AdvanceEpoch(_)
		| WendelonRequest::SchemaUpdate(_)
		| WendelonRequest::TierSlab(_)
		| WendelonRequest::RetireTieredSlab(_)
		| WendelonRequest::CompactionCommit(_)
		| WendelonRequest::RegisterIncarnation(_)
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
		| WendelonRequest::TableDropSettlement(_) => (EntryClass2::NonRoot2, None),
	}
}

#[cfg(test)]
pub(in crate::storage::mutation_visibility::root2) mod fixture;
