//! Pure governance authorization from the exact pre-entry root and pinned administrator.
//! Structural/install failures refuse without a token; authorization failures are consumed denials.
use crate::storage::mutation_visibility::root2::admin_install::{admin_namespace2, install_admin2};
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::governance_command::{
	GOVERNANCE_COMMAND_BYTES2, GovernanceAction2, GovernanceCommandError2,
	check_governance_command2, parse_governance_command2,
};
use crate::storage::mutation_visibility::root2::lease_payload::{
	LeasePayloadError2, verify_issuer_signature2,
};
use crate::storage::mutation_visibility::root2::origin::{
	MutationAuthorityState2, root_and_origin2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::PinnedBoundTuple2;
use crate::storage::mutation_visibility::root2::types::Digest2;
use crate::storage::mutation_visibility::root2::verification::VerifyError2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Denial2 {
	Envelope(GovernanceCommandError2),
	Scope,
	IssuerMissing,
	IssuerRevoked,
	IssuerKey,
	IssuerSignature,
}

pub(in crate::storage::mutation_visibility::root2) fn verify_governance2(
	state: &MutationAuthorityState2,
	pin: &PinnedBoundTuple2,
	command: &Command2,
	governance: Option<&[u8]>,
	digest: Digest2,
) -> Result<bool, VerifyError2> {
	governance_failure2(state, pin, command, governance, digest).map(|failure| failure.is_none())
}

// Kept private: first-cause evidence is available only to this module's nested tests.
fn governance_failure2(
	state: &MutationAuthorityState2,
	pin: &PinnedBoundTuple2,
	command: &Command2,
	governance: Option<&[u8]>,
	digest: Digest2,
) -> Result<Option<Denial2>, VerifyError2> {
	let (root, _) = root_and_origin2(state).ok_or(VerifyError2::StateKind)?;
	let action = match command {
		Command2::EnrollSelector(_) => GovernanceAction2::EnrollSelector,
		Command2::EnrollAuthority { .. } => GovernanceAction2::EnrollAuthority,
		Command2::RevokeAuthority(_) => GovernanceAction2::RevokeAuthority,
		Command2::Grant { .. } => GovernanceAction2::Grant,
		Command2::Release { .. } => GovernanceAction2::Release,
		Command2::Cut { .. } => GovernanceAction2::Cut,
		Command2::Resolve { .. } => GovernanceAction2::Resolve,
		Command2::EnrollLeaseIssuer { .. } => GovernanceAction2::EnrollLeaseIssuer,
		Command2::RevokeLeaseIssuer { .. } => GovernanceAction2::RevokeLeaseIssuer,
		Command2::Bootstrap { .. }
		| Command2::AdmissionPermission(_)
		| Command2::Reserve(_)
		| Command2::Finalize { .. }
		| Command2::Cover { .. }
		| Command2::RotateAdmin(_)
		| Command2::RotateGovernance(_)
		| Command2::RotateRecovery(_)
		| Command2::Freeze(_)
		| Command2::Unfreeze(_)
		| Command2::RetireRoot
		| Command2::EpochAdmissionPermission(_) => return Err(VerifyError2::StateKind),
	};
	let bytes = governance.ok_or(VerifyError2::MissingSidecar)?;
	let role_proof = bytes.len() == super::role_command::ROLE_PROOF_BYTES3;
	if role_proof {
		super::role_command::parse(bytes).map_err(VerifyError2::Role)?;
	} else {
		if bytes.len() != GOVERNANCE_COMMAND_BYTES2 {
			return Err(VerifyError2::SidecarLength);
		}
		parse_governance_command2(bytes).map_err(VerifyError2::Governance)?;
	}
	let admin = install_admin2(state, pin).map_err(VerifyError2::Install)?;
	if role_proof {
		if !super::admin_install::admin_governance2(&admin)
			.is_some_and(|r| r.actions() & action.bit() != 0)
			|| !super::role_command::verify(&admin, bytes, digest, [2, 0])
				.map_err(VerifyError2::Role)?
		{
			return Ok(Some(Denial2::Envelope(GovernanceCommandError2::Signature)));
		}
	} else if let Err(error) = check_governance_command2(&admin, action, bytes, digest) {
		return Ok(Some(Denial2::Envelope(error)));
	}
	let namespace = admin_namespace2(&admin);
	let in_scope = match command {
		Command2::EnrollSelector(binding) => {
			binding.tenant == namespace.tenant && binding.generation == root.state.generation
		}
		Command2::Resolve { identity, .. } => {
			identity.generation == root.state.generation
				&& identity.tenant == namespace.tenant
				&& identity.scope == namespace.scope
				&& identity.table == namespace.table
				&& identity.schema_generation == namespace.schema
		}
		Command2::EnrollAuthority { .. }
		| Command2::RevokeAuthority(_)
		| Command2::Grant { .. }
		| Command2::Release { .. }
		| Command2::Cut { .. }
		| Command2::EnrollLeaseIssuer { .. }
		| Command2::RevokeLeaseIssuer { .. } => true,
		Command2::Bootstrap { .. }
		| Command2::AdmissionPermission(_)
		| Command2::Reserve(_)
		| Command2::Finalize { .. }
		| Command2::Cover { .. }
		| Command2::RotateAdmin(_)
		| Command2::RotateGovernance(_)
		| Command2::RotateRecovery(_)
		| Command2::Freeze(_)
		| Command2::Unfreeze(_)
		| Command2::RetireRoot
		| Command2::EpochAdmissionPermission(_) => return Err(VerifyError2::StateKind),
	};
	if !in_scope {
		return Ok(Some(Denial2::Scope));
	}
	if let Command2::Grant { lease, .. } = command {
		let Some(issuer) = root.state.issuers.get(&lease.issuer.to_be_bytes()) else {
			return Ok(Some(Denial2::IssuerMissing));
		};
		if issuer.revoked.is_some() {
			return Ok(Some(Denial2::IssuerRevoked));
		}
		match verify_issuer_signature2(&issuer.key, lease) {
			Ok(()) => {}
			Err(LeasePayloadError2::UnsupportedEndian) => return Err(VerifyError2::IssuerPayload),
			Err(LeasePayloadError2::Key) => return Ok(Some(Denial2::IssuerKey)),
			Err(LeasePayloadError2::Signature) => return Ok(Some(Denial2::IssuerSignature)),
		}
	}
	Ok(None)
}

#[cfg(test)]
#[path = "tests/governance_verification_private.rs"]
mod tests;
