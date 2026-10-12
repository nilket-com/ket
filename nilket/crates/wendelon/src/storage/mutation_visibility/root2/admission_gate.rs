//! Dormant Admission issuer gate. The one path that applies an Admission permission command: the
//! installed administrator is bound to the WHOLE context of this same rooted state before replay,
//! the exact carried statement is authenticated against it, and only the checked action and key
//! reach the registry transitions. `Root2::reduce` keeps refusing the command.
//!
//! Transition preconditions: the root was built only by validated decode or by reduction, so its
//! permission map, authorities and derived summary are semantically valid; `l` is the next
//! committed index (steps 6-7), strictly above every existing coordinate; and the key's namespace
//! generation is the root's (step 5 plus the statement's context and scope checks). No full
//! validation runs per transition. No production caller exists until reviewed integration.
use crate::storage::mutation_visibility::root2::admin_install::{
	AdminContext2, InstalledAdmin2, admin_context2, admin_manifest_len2,
};
use crate::storage::mutation_visibility::root2::admin_statement::{
	AdminAction2, CheckedStatement2, check_admin_statement2, checked_action2, checked_key2,
};
use crate::storage::mutation_visibility::root2::admission_registry::{
	PERMISSION_SECTION_BYTES2, PermissionLimits2, PermissionOutcome2, PermissionRecord2,
	RegistryError2, encode_permission_key2, grant_permission2, revoke_permission2,
};
use crate::storage::mutation_visibility::root2::codec::valid_log;
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::incremental::{cached_capacity, update};
use crate::storage::mutation_visibility::root2::origin::{
	MutationAuthorityState2, root_and_origin2,
};
use crate::storage::mutation_visibility::root2::state::Root2;
use crate::storage::mutation_visibility::root2::trust::{TrustedEntry2, VerifiedCommand2};
use crate::storage::mutation_visibility::root2::types::{Log2, Result2, RootError2};
use crate::storage::mutation_visibility::root2::validation::authority_key;

fn denied(reason: u16, reference: Option<Log2>) -> ApplyResult2 {
	ApplyResult2::Denied { reason, reference }
}
// Arithmetic stays arithmetic; any other registry error means a transition precondition failed.
fn registry_error(e: RegistryError2) -> RootError2 {
	match e {
		RegistryError2::Overflow => RootError2::Overflow,
		_ => RootError2::RootInvariant,
	}
}
impl Root2 {
	/// Order: command, state, log, digest and verified match, root generation/cluster, WHOLE
	/// administrator context, replay, gap, authorization and statement (consumed denials), the
	/// checked transition, then the existing accounting.
	pub(in crate::storage::mutation_visibility::root2) fn reduce_admission(
		state: &MutationAuthorityState2,
		admin: &InstalledAdmin2,
		entry: &TrustedEntry2,
		verified: &VerifiedCommand2,
		command: &Command2,
	) -> Result2<(Root2, ApplyResult2)> {
		// 0-1. Only this command, only a rooted state.
		let Command2::AdmissionPermission(bytes) = command else {
			return Err(RootError2::TrustMismatch);
		};
		let Some((root, origin)) = root_and_origin2(state) else {
			return Err(RootError2::TrustMismatch);
		};
		let s = &root.state;
		// 2-4. The entry, its digest and the root it names.
		let (g, c, l, fp, d) = entry.fields();
		valid_log(l)?;
		let digest = command.digest(g, c)?;
		if digest != d || !verified.matches(g, c, d) {
			return Err(RootError2::TrustMismatch);
		}
		if (g, c) != (s.generation, s.cluster) {
			return Err(RootError2::TrustMismatch);
		}
		// 5. The administrator must describe this exact rooted origin; built here, never supplied.
		let expected = AdminContext2 {
			generation: s.generation,
			cluster: s.cluster,
			installed_at: s.bootstrap,
			bootstrap_fingerprint: origin.bootstrap_fingerprint,
			bootstrap_command: origin.bootstrap_command,
			genesis: origin.genesis,
			initial_epoch: s.initial_epoch,
			manifest_pin: s.control_digest,
		};
		// The installed recorded length, which install bound to its root's commitment.
		if s.control_len != admin_manifest_len2(admin) || admin_context2(admin) != expected {
			return Err(RootError2::TrustMismatch);
		}
		// 6-7. Replay and gap, only after the administrator is bound.
		if l.index == s.covered.index {
			if (l, fp, d) != (s.covered, s.fingerprint, s.command_digest) {
				return Err(RootError2::TrustMismatch);
			}
			return Ok((root.clone(), ApplyResult2::AlreadyProcessed));
		}
		if s.covered.index.checked_add(1) != Some(l.index) {
			return Err(RootError2::LineageGap);
		}
		// 8-9. Authorization and statement failures are consumed denials.
		let mut next = root.clone();
		let mut result = if !verified.authorized() {
			denied(8, None)
		} else {
			match check_admin_statement2(admin, bytes) {
				Err(_) => denied(8, None),
				Ok(checked) => apply_admission(&mut next, &checked, l)?,
			}
		};
		// 10-12. The existing accounting, limit fallback and cached capacity.
		next.state.covered = l;
		next.state.fingerprint = fp;
		next.state.command_digest = d;
		match update(s, &mut next.state, command) {
			Ok(()) => {}
			Err(RootError2::Limit) => {
				next = root.clone();
				next.state.covered = l;
				next.state.fingerprint = fp;
				next.state.command_digest = d;
				result = denied(11, None);
			}
			Err(e) => return Err(e),
		}
		cached_capacity(&next.state, next.limits)?;
		Ok((next, result))
	}
}
/// The action and key come only from the checked statement.
fn apply_admission(
	root: &mut Root2,
	checked: &CheckedStatement2,
	l: Log2,
) -> Result2<ApplyResult2> {
	let action = checked_action2(checked);
	let key = checked_key2(checked);
	let limits = PermissionLimits2 {
		count: root.limits.permissions,
		section: PERMISSION_SECTION_BYTES2,
		registry: root.limits.registry,
	};
	let s = &mut root.state;
	let (map, outcome, _) = match action {
		AdminAction2::Grant => grant_permission2(
			&s.permissions,
			s.authorities.get(&authority_key(key.authority)?),
			&key,
			l,
			s.derived.usage.registry,
			limits,
		),
		AdminAction2::Revoke => revoke_permission2(&s.permissions, &key, l),
	}
	.map_err(registry_error)?;
	s.permissions = map;
	let permission = |outcome: u8, r: PermissionRecord2| -> Result2<ApplyResult2> {
		Ok(ApplyResult2::Permission {
			outcome,
			key: encode_permission_key2(&r.key).map_err(registry_error)?,
			enrolled: r.enrolled,
			revoked: r.revoked,
		})
	};
	match outcome {
		PermissionOutcome2::Granted(r) => permission(1, r),
		PermissionOutcome2::GrantDuplicate(r) => permission(2, r),
		PermissionOutcome2::Revoked(r) => permission(3, r),
		PermissionOutcome2::RevokeDuplicate(r) => permission(4, r),
		PermissionOutcome2::UnknownAuthority | PermissionOutcome2::UnknownPermission => {
			Ok(denied(9, None))
		}
		PermissionOutcome2::AuthorityRevoked(x) => Ok(denied(16, Some(x))),
		PermissionOutcome2::Capacity => Ok(denied(11, None)),
	}
}
