//! Parse-only terminal dispatch cut. No installed view, role epoch or signature evaluation.
use super::{command::Command2, verification::VerifyError2 as E};
pub(super) fn check(command: &Command2, sidecar: Option<&[u8]>) -> Result<(), E> {
	match command {
		Command2::Bootstrap { .. } | Command2::Cover { .. } => Err(E::StateKind),
		Command2::AdmissionPermission(_)
		| Command2::EpochAdmissionPermission(_)
		| Command2::Reserve(_)
		| Command2::Finalize { .. } => {
			if sidecar.is_some() {
				Err(E::ExtraSidecar)
			} else {
				Ok(())
			}
		}
		Command2::RotateAdmin(_)
		| Command2::RotateGovernance(_)
		| Command2::RotateRecovery(_)
		| Command2::Freeze(_)
		| Command2::Unfreeze(_)
		| Command2::RetireRoot => {
			super::role_command::parse(sidecar.ok_or(E::MissingSidecar)?).map_err(E::Role)
		}
		Command2::EnrollSelector(_)
		| Command2::EnrollAuthority { .. }
		| Command2::RevokeAuthority(_)
		| Command2::Grant { .. }
		| Command2::Release { .. }
		| Command2::Cut { .. }
		| Command2::Resolve { .. }
		| Command2::EnrollLeaseIssuer { .. }
		| Command2::RevokeLeaseIssuer { .. } => {
			let bytes = sidecar.ok_or(E::MissingSidecar)?;
			if bytes.len() == super::role_command::ROLE_PROOF_BYTES3 {
				super::role_command::parse(bytes).map_err(E::Role)
			} else if bytes.len() == super::governance_command::GOVERNANCE_COMMAND_BYTES2 {
				super::governance_command::parse_governance_command2(bytes)
					.map(|_| ())
					.map_err(E::Governance)
			} else {
				Err(E::SidecarLength)
			}
		}
	}
}
