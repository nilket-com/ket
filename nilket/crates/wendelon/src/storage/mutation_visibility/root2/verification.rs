//! Stage C, narrow slice: the dormant command-verification producer. For Bootstrap (by the pinned
//! tuple), AdmissionPermission (the administrator statement), Reserve (the claim's admission
//! signature) and Finalize (the holder attestation) it computes a `VerifiedCommand2` from the exact
//! pre-entry state and hands it at once to the one matching consumer; the token never leaves
//! `advance_verified2`. The nine governance kinds use a structurally checked envelope and the
//! pre-entry administrator/issuer state; Cover remains not applicable here.
//!
//! Structural verification and command authorization only: nothing here authenticates that an
//! entry is committed or that a state is current. Authorization is applied outside the reducer, so
//! an invalid or revoked proof is a consumed `Denied(8)` even where the reducer alone would have
//! recorded a closure-driven refusal or a terminal replay; that limitation is deliberate and
//! recorded, not closed.
use crate::storage::mutation_visibility::MutationIdentity;
use crate::storage::mutation_visibility::root2::admin_install::{InstallError2, install_admin2};
use crate::storage::mutation_visibility::root2::admin_statement::{
	ADMIN_STATEMENT_BYTES2, check_admin_statement2,
};
use crate::storage::mutation_visibility::root2::auth::signature::{
	verify_admission_signature2, verify_holder_signature2,
};
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::governance_command::GovernanceCommandError2;
use crate::storage::mutation_visibility::root2::governance_verification::verify_governance2;
use crate::storage::mutation_visibility::root2::origin::{
	EntryClass2, MutationAuthorityState2, OriginView2, advance_admission2, advance_root2,
	bootstrap2, origin_view2, root_and_origin2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::{
	PinnedBoundTuple2, tuple_manifest_len2, tuple_manifest_pin2, tuple_record2,
};
use crate::storage::mutation_visibility::root2::state::Root2;
use crate::storage::mutation_visibility::root2::trust::{
	CommittedEntry2, TrustedEntry2, VerifiedCommand2,
};
use crate::storage::mutation_visibility::root2::types::{
	ClaimProjection2, Digest2, HolderAttestation2, Log2, RootError2,
};
use crate::storage::mutation_visibility::root2::validation::authority_key;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum UnsupportedKind2 {
	EnrollSelector,
	EnrollAuthority,
	RevokeAuthority,
	EnrollLeaseIssuer,
	RevokeLeaseIssuer,
	Grant,
	Release,
	Cut,
	Resolve,
}

/// Refusals: no token was made and the state is unchanged. A refused entry is not evaluable by
/// this slice; a caller must treat it as not ready, never skip it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum VerifyError2 {
	NotApplicable,
	EnvelopeContext,
	ExtraSidecar,
	MissingSidecar,
	SidecarLength,
	Governance(GovernanceCommandError2),
	Role(super::role_command::RoleProofError3),
	Install(InstallError2),
	IssuerPayload,
	Unsupported(UnsupportedKind2),
	DigestMismatch,
	CommittedMismatch,
	StateKind,
	RootMismatch,
	Consumer(RootError2),
}

/// Verify `command` against the exact pre-entry `state` (and `pin`), then consume the token at
/// once in the one matching consumer. Order: kind, digest, committed binding (rooted commands;
/// Bootstrap's is `bootstrap2`'s), state kind, root (generation, cluster), extra sidecar,
/// verification, consumer. The replay checks the retained envelope context before trusted bind;
/// governance structure/install refusals precede authorization and token creation.
pub(in crate::storage::mutation_visibility::root2) fn advance_verified2(
	state: &MutationAuthorityState2,
	pin: &PinnedBoundTuple2,
	committed: &CommittedEntry2,
	entry: &TrustedEntry2,
	command: &Command2,
	governance: Option<&[u8]>,
) -> Result<(MutationAuthorityState2, ApplyResult2), VerifyError2> {
	use VerifyError2 as E;
	kind(command)?;
	let (g, c, l, fp, d) = entry.fields();
	if command.digest(g, c).map_err(E::Consumer)? != d {
		return Err(E::DigestMismatch);
	}
	let bootstrap = matches!(command, Command2::Bootstrap { .. });
	if !bootstrap {
		let (position, identity, class) = committed.entry_fields();
		if class != EntryClass2::OtherRoot2(d)
			|| (position.term, position.node, position.index) != (l.term, l.node, l.index)
			|| identity != fp
		{
			return Err(E::CommittedMismatch);
		}
	}
	match (bootstrap, origin_view2(state)) {
		(true, OriginView2::PreRoot { .. }) => {}
		(false, OriginView2::Root) => {
			let (root, _) = root_and_origin2(state).ok_or(E::StateKind)?;
			if (g, c) != (root.state.generation, root.state.cluster) {
				return Err(E::RootMismatch);
			}
		}
		_ => return Err(E::StateKind),
	}
	if governance.is_some()
		&& matches!(
			command,
			Command2::Bootstrap { .. }
				| Command2::AdmissionPermission(_)
				| Command2::EpochAdmissionPermission(_)
				| Command2::Reserve(_)
				| Command2::Finalize { .. }
		) {
		return Err(E::ExtraSidecar);
	}
	let token = verify_command2(state, pin, entry, command, governance)?;
	match command {
		Command2::Bootstrap { .. } => bootstrap2(state, committed, entry, &token, command),
		Command2::AdmissionPermission(_) | Command2::EpochAdmissionPermission(_) => {
			advance_admission2(state, pin, entry, &token, command)
		}
		_ => advance_root2(state, entry, &token, command),
	}
	.map_err(E::Consumer)
}

/// The dispatch census: exhaustive, no wildcard.
fn kind(command: &Command2) -> Result<(), VerifyError2> {
	use VerifyError2 as E;
	match command {
		Command2::RotateAdmin(_)
		| Command2::RotateGovernance(_)
		| Command2::RotateRecovery(_)
		| Command2::EpochAdmissionPermission(_)
		| Command2::Bootstrap { .. }
		| Command2::AdmissionPermission(_)
		| Command2::Reserve(_)
		| Command2::Finalize { .. } => Ok(()),
		Command2::Cover { .. } => Err(E::NotApplicable),
		Command2::EnrollSelector(_)
		| Command2::EnrollAuthority { .. }
		| Command2::RevokeAuthority(_)
		| Command2::EnrollLeaseIssuer { .. }
		| Command2::RevokeLeaseIssuer { .. }
		| Command2::Grant { .. }
		| Command2::Release { .. }
		| Command2::Cut { .. }
		| Command2::Resolve { .. } => Ok(()),
	}
}

/// The only producer of a production token; its one caller is `advance_verified2`.
fn verify_command2(
	state: &MutationAuthorityState2,
	pin: &PinnedBoundTuple2,
	entry: &TrustedEntry2,
	command: &Command2,
	governance: Option<&[u8]>,
) -> Result<VerifiedCommand2, VerifyError2> {
	let (g, c, _, _, d) = entry.fields();
	let mut role_seed = None;
	let (authorized, evidence) = match (command, origin_view2(state)) {
		(Command2::Bootstrap { .. }, OriginView2::PreRoot { genesis }) => {
			(bootstrap_authorized(genesis, pin, entry, command), 0)
		}
		(Command2::AdmissionPermission(bytes), OriginView2::Root) => {
			(statement_authorized(state, pin, bytes), 0)
		}
		(Command2::EpochAdmissionPermission(bytes), OriginView2::Root) => {
			let admin = install_admin2(state, pin).map_err(VerifyError2::Install)?;
			(
				super::admin_statement::check_epoch_statement3(&admin, bytes).is_ok(),
				0,
			)
		}
		(
			Command2::RotateAdmin(_) | Command2::RotateGovernance(_) | Command2::RotateRecovery(_),
			OriginView2::Root,
		) => {
			let bytes = governance.ok_or(VerifyError2::MissingSidecar)?;
			super::role_command::parse(bytes).map_err(VerifyError2::Role)?;
			let admin = install_admin2(state, pin).map_err(VerifyError2::Install)?;
			let expected = if matches!(command, Command2::RotateRecovery(_)) {
				[1, 2]
			} else {
				[3, 0]
			};
			let ok = super::role_command::verify(&admin, bytes, d, expected)
				.map_err(VerifyError2::Role)?;
			if ok {
				let keys =
					super::admin_install::original_roles3(&admin).ok_or(VerifyError2::StateKind)?;
				let (covered, fp, digest) =
					super::admin_install::admin_cut3(&admin).ok_or(VerifyError2::StateKind)?;
				role_seed = Some((keys, covered, fp, digest));
			}
			(ok, 0)
		}
		(Command2::Reserve(claim), OriginView2::Root) => {
			let (root, _) = root_and_origin2(state).ok_or(VerifyError2::StateKind)?;
			(reserve_authorized(root, claim, c), 0)
		}
		(
			Command2::Finalize {
				identity,
				digest,
				reservation,
				attestation,
			},
			OriginView2::Root,
		) => {
			let (root, _) = root_and_origin2(state).ok_or(VerifyError2::StateKind)?;
			let ok = finalize_authorized(root, identity, *digest, *reservation, attestation, c);
			(ok, if ok { 1 } else { 2 })
		}
		(
			Command2::EnrollSelector(_)
			| Command2::EnrollAuthority { .. }
			| Command2::RevokeAuthority(_)
			| Command2::Grant { .. }
			| Command2::Release { .. }
			| Command2::Cut { .. }
			| Command2::Resolve { .. }
			| Command2::EnrollLeaseIssuer { .. }
			| Command2::RevokeLeaseIssuer { .. },
			OriginView2::Root,
		) => (verify_governance2(state, pin, command, governance, d)?, 0),
		_ => return Err(VerifyError2::StateKind),
	};
	Ok(VerifiedCommand2::verified2(
		g, c, d, authorized, None, evidence, role_seed,
	))
}

/// Every pinned Bootstrap anchor equals the entry and command, plus the commitment rule
/// `install_admin2` later enforces on the root. The pin is the owner's authority; no signature.
fn bootstrap_authorized(
	genesis: Digest2,
	pin: &PinnedBoundTuple2,
	entry: &TrustedEntry2,
	command: &Command2,
) -> bool {
	let Command2::Bootstrap {
		epoch,
		control,
		length,
	} = command
	else {
		return false;
	};
	let (g, c, l, fp, d) = entry.fields();
	let r = tuple_record2(pin).identity;
	(r.generation, r.cluster, r.bootstrap) == (g, c, l)
		&& (r.bootstrap_fingerprint, r.bootstrap_command) == (fp, d)
		&& r.initial_epoch == *epoch
		&& r.genesis_fingerprint == genesis
		&& *control == tuple_manifest_pin2(pin)
		&& *length == tuple_manifest_len2(pin)
}

/// The statement checks under the administrator installed from the same pin the gate uses.
fn statement_authorized(
	state: &MutationAuthorityState2,
	pin: &PinnedBoundTuple2,
	bytes: &[u8; ADMIN_STATEMENT_BYTES2],
) -> bool {
	install_admin2(state, pin).is_ok_and(|admin| check_admin_statement2(&admin, bytes).is_ok())
}

/// The claim's authority is enrolled and not revoked in the pre-entry state, and its key verifies
/// the admission signature.
fn reserve_authorized(root: &Root2, claim: &ClaimProjection2, cluster: [u8; 16]) -> bool {
	let Ok(key) = authority_key(claim.authority) else {
		return false;
	};
	root.state
		.authorities
		.get(&key)
		.filter(|a| a.revoked.is_none())
		.is_some_and(|a| verify_admission_signature2(claim, cluster, &a.public_key).is_ok())
}

/// The reserved parent matches, its authority is enrolled and not revoked, and that key verifies
/// the holder attestation over the parent's claim.
fn finalize_authorized(
	root: &Root2,
	identity: &MutationIdentity,
	digest: Digest2,
	reservation: Log2,
	attestation: &HolderAttestation2,
	cluster: [u8; 16],
) -> bool {
	let Ok(parent_key) = identity.encode() else {
		return false;
	};
	let Some(parent) = root.state.parents.get(&parent_key) else {
		return false;
	};
	if parent.claim.digest().ok() != Some(digest) || parent.reservation != reservation {
		return false;
	}
	let Ok(key) = authority_key(parent.claim.authority) else {
		return false;
	};
	root.state
		.authorities
		.get(&key)
		.filter(|a| a.revoked.is_none())
		.is_some_and(|a| {
			verify_holder_signature2(&parent.claim, cluster, attestation, &a.public_key).is_ok()
		})
}
