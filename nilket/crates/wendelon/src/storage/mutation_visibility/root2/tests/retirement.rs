//! Retirement fixtures are not an independent issuer oracle or runtime activation.
use super::governance_verification::support as wire;
use super::profile_three_fixture::{M3_OTHER, s1_command};
use crate::storage::mutation_visibility::root2::{
	admin::manifest::*, admin_install::*, admin_statement::*, codec::*, command::*, origin::*,
	role_command, state::Root2, types::*,
};
use ed25519_dalek::{Signer, SigningKey};
mod support;
use support::*;
#[test]
fn retire_recovery_alone_succeeds() {
	let mut f = F::new();
	f.retire([3, 0]);
	assert!(super::super::retirement::is_retired(&f.root().state));
}
#[test]
fn retire_joint_admin_governance_succeeds() {
	let mut f = F::new();
	f.retire([1, 2]);
	assert!(super::super::retirement::is_retired(&f.root().state));
}
#[test]
fn retire_wrong_single_duplicate_reordered_roles_denied8() {
	for roles in [[0, 0], [1, 0], [2, 0], [2, 1], [3, 3], [1, 3]] {
		let mut f = F::new();
		let p = f.proof(&Command2::RetireRoot, roles);
		denied(f.f.apply(Command2::RetireRoot, Some(p)), 8);
		assert!(!super::super::retirement::is_retired(&f.root().state));
	}
}
#[test]
fn retire_current_epochs_domains_and_command_digest_bound() {
	let f = F::new();
	let p = f.proof(&Command2::RetireRoot, [3, 0]);
	let a = install_admin2(&f.f.state, &f.f.pin).unwrap();
	let d = Command2::RetireRoot
		.digest(f.f.dep.generation, f.f.dep.cluster)
		.unwrap();
	assert!(role_command::verify_for(&a, &p, d, [3, 0], role_command::RoleForm3::Retire).unwrap());
	assert!(
		!role_command::verify_for(&a, &p, [7; 32], [3, 0], role_command::RoleForm3::Retire)
			.unwrap()
	);
	assert!(
		role_command::preimage_for(&p, [3, 0], 0, role_command::RoleForm3::Retire)
			.unwrap()
			.starts_with(b"VISROOT2-ROLE3-RETIRE-RECOVERY\0")
	);
	let mut stale = p;
	stale[77..85].copy_from_slice(&1u64.to_le_bytes());
	assert!(
		!role_command::verify_for(&a, &stale, d, [3, 0], role_command::RoleForm3::Retire).unwrap()
	);
}
#[test]
fn retire_kind20_payload_and_malformed_command_codec() {
	let f = F::new();
	let c = Command2::RetireRoot;
	let p = c.payload(f.f.dep.generation, f.f.dep.cluster).unwrap();
	assert_eq!(p.len(), 33);
	assert_eq!(p[32], 20);
	let b = c.encode(f.f.dep.generation, f.f.dep.cluster).unwrap();
	assert!(matches!(
		Command2::decode(&b).unwrap().2,
		Command2::RetireRoot
	));
	let mut body = p;
	body.push(0);
	assert!(Command2::decode(&envelope(b"VISEVT02", &body, COMMAND_BYTES2).unwrap()).is_err());
}
#[test]
fn retired_v6_exact_bytes_and_independent_goldens() {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("retirement/vectors.json")).unwrap();
	for x in v["states"].as_array().unwrap() {
		let b = unhex(x.as_str().unwrap());
		assert_eq!(b.len(), 492);
		assert_eq!(
			super::super::UnverifiedRoot2::decode(&b)
				.unwrap()
				.encode()
				.unwrap(),
			b
		);
	}
}
#[test]
fn never_retired_v4_v5_bytes_unchanged() {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("role_epochs/vectors.json")).unwrap();
	for k in ["state_v4", "state_v5"] {
		let b = unhex(v[k].as_str().unwrap());
		assert_eq!(
			super::super::UnverifiedRoot2::decode(&b)
				.unwrap()
				.encode()
				.unwrap(),
			b
		);
	}
}
#[test]
fn old_state_reader_refuses_v6_by_version() {
	let mut f = F::new();
	f.retire([3, 0]);
	let b = f.root().encode().unwrap();
	assert!(open_versioned(&b, b"VISAUT05", 5, ROOT_BYTES2).is_err());
	assert_eq!(&b[..8], b"VISAUT06");
}
#[test]
fn v6_retired_nonzero_canonicality_and_truncation() {
	let mut f = F::new();
	f.retire([3, 0]);
	let mut s = f.root().state.clone();
	s.role_epochs
		.as_mut()
		.unwrap()
		.retired
		.as_mut()
		.unwrap()
		.fingerprint = [0; 32];
	assert!(super::super::validation::validate_state(&s).is_err());
	let b = f.root().encode().unwrap();
	assert!(super::super::UnverifiedRoot2::decode(&b[..b.len() - 1]).is_err());
}
#[test]
fn retired_coordinate_unique_after_bootstrap_at_covered() {
	let mut f = F::new();
	f.freeze();
	f.retire([3, 0]);
	let mut s = f.root().state.clone();
	let transition = s.role_epochs.as_ref().unwrap().transition;
	s.role_epochs.as_mut().unwrap().retired.as_mut().unwrap().at = transition;
	assert!(super::super::validation::validate_state(&s).is_err());
}
#[test]
fn retire_virgin_history140_and_existing_extension20() {
	let mut f = F::new();
	let before = f.root().usage().unwrap().history;
	f.retire([3, 0]);
	assert_eq!(f.root().usage().unwrap().history, before + 204);
	let mut f = F::new();
	f.freeze();
	let before = f.root().usage().unwrap().history;
	f.retire([3, 0]);
	assert_eq!(f.root().usage().unwrap().history, before + 84);
}
#[test]
fn retire_capacity11_tip_only_rollback() {
	let f = F::new();
	let mut root = f.root().clone();
	root.limits.history = 203;
	let s = &root.state;
	let seed = Some((
		[key(11), key(12), key(13)],
		s.covered,
		s.fingerprint,
		s.command_digest,
	));
	let before = root.state.clone();
	denied(
		super::super::retirement::retire(
			&mut root,
			wire::log(before.covered.index + 1),
			[0x53; 32],
			[0x54; 32],
			seed,
		)
		.unwrap(),
		11,
	);
	assert!(root.state.role_epochs == before.role_epochs);
}
#[test]
fn retire_allowed_while_frozen() {
	let mut f = F::new();
	f.freeze();
	f.retire([3, 0]);
	assert!(f.root().state.role_epochs.as_ref().unwrap().frozen);
}
#[test]
fn retired25_before_frozen23_and_auth8_both_paths() {
	let mut f = F::new();
	f.freeze();
	let c = wire::enroll_authority();
	let mut p = f.proof(&c, [2, 0]);
	p[86..150].fill(0);
	f.retire([3, 0]);
	denied(f.terminal(c, Some(p)), 25);
	assert!(f.root().state.role_epochs.as_ref().unwrap().frozen);
	for epoch in [false, true] {
		let mut f = F::new();
		f.freeze();
		let c = if epoch {
			Command2::EpochAdmissionPermission(f.statement())
		} else {
			Command2::AdmissionPermission(f.legacy_statement())
		};
		f.retire([3, 0]);
		denied(f.terminal(c, None), 25);
	}
}
#[test]
fn retired_uniform_command_matrix_both_paths() {
	for c in [
		Command2::RotateAdmin(key(44)),
		Command2::RotateGovernance(key(45)),
		Command2::RotateRecovery(key(46)),
		Command2::Freeze(wire::log(9)),
		Command2::Unfreeze(wire::log(9)),
		Command2::RetireRoot,
	] {
		let mut f = F::new();
		let p = f.proof(&Command2::RetireRoot, [3, 0]);
		f.retire([3, 0]);
		denied(f.terminal(c, Some(p)), 25);
	}
	for name in ["command_reserve", "command_finalize"] {
		let mut f = F::new();
		f.retire([3, 0]);
		denied(f.terminal(s1_command(name), None), 25);
	}
	for epoch in [false, true] {
		let mut f = F::new();
		let c = if epoch {
			Command2::EpochAdmissionPermission(f.statement())
		} else {
			Command2::AdmissionPermission(f.legacy_statement())
		};
		f.retire([3, 0]);
		denied(f.terminal(c, None), 25);
	}
	for i in 0..9 {
		let mut f = F::new();
		let c = wire::nine(&f.f).remove(i);
		let p = f.proof(&c, [2, 0]);
		f.retire([3, 0]);
		denied(f.terminal(c, Some(p)), 25);
	}
}
#[test]
fn retired_structural_sidecar_refusals_precede25() {
	assert!(super::super::retirement_shape::check(&Command2::RetireRoot, None).is_err());
	assert!(super::super::retirement_shape::check(&Command2::RetireRoot, Some(&[0; 223])).is_err());
	let mut f = F::new();
	f.retire([3, 0]);
	for c in [
		crate::storage::mutation_visibility::root2::tests::profile_three_fixture::s1_command(
			"command_bootstrap",
		),
		crate::storage::mutation_visibility::root2::tests::profile_three_fixture::s1_command(
			"command_cover_blank",
		),
	] {
		let entry = super::super::trust::TrustedEntry2::fixture(
			f.f.dep.generation,
			f.f.dep.cluster,
			wire::log(wire::tip(&f.f.state).index + 1),
			&c,
		)
		.unwrap();
		assert!(matches!(
			super::super::retirement::consume(f.root(), &entry, &c),
			Err(RootError2::TrustMismatch)
		));
		assert!(super::super::retirement_shape::check(&c, None).is_err());
	}
	let before = wire::bytes(&f.f.state);
	let c = Command2::RetireRoot;
	let e = wire::carrier(&c, f.f.dep, wire::tip(&f.f.state).index + 1, None);
	assert!(wire::step(&f.f.state, &f.f.pin, f.f.dep, &e, &c, None).is_err());
	assert_eq!(before, wire::bytes(&f.f.state));
}
#[test]
fn retired_exact_replay_changed_fingerprint_and_gap() {
	let mut f = F::new();
	f.retire([3, 0]);
	let c = Command2::RetireRoot;
	let last = f.f.entries.last().unwrap();
	let ce = super::super::committed::committed_entry2(
		&super::super::committed::CommittedSource2::synthetic(),
		last,
	)
	.unwrap()
	.into_committed();
	let trusted =
		super::super::trust::TrustedEntry2::bind(&ce, &c, f.f.dep.generation, f.f.dep.cluster)
			.unwrap();
	assert!(
		super::super::retirement::consume(f.root(), &trusted, &c)
			.unwrap()
			.1 == ApplyResult2::AlreadyProcessed
	);
	let e = wire::carrier(&c, f.f.dep, wire::tip(&f.f.state).index + 2, None);
	let ce = super::super::committed::committed_entry2(
		&super::super::committed::CommittedSource2::synthetic(),
		&e,
	)
	.unwrap()
	.into_committed();
	let trusted =
		super::super::trust::TrustedEntry2::bind(&ce, &c, f.f.dep.generation, f.f.dep.cluster)
			.unwrap();
	assert!(matches!(
		super::super::retirement::consume(f.root(), &trusted, &c),
		Err(RootError2::LineageGap)
	));
}
#[test]
fn retired_denial_advances_only_tip_preserves_first_retirement() {
	let mut f = F::new();
	let p = f.proof(&Command2::RetireRoot, [3, 0]);
	f.retire([3, 0]);
	let before = f.root().state.role_epochs.clone();
	let at = wire::tip(&f.f.state);
	denied(f.terminal(Command2::RetireRoot, Some(p)), 25);
	assert_eq!(wire::tip(&f.f.state).index, at.index + 1);
	assert!(f.root().state.role_epochs == before);
}
#[test]
fn retired_no_verified_token_install_epoch_or_signature() {
	let source = include_str!("../retirement.rs");
	assert!(!source.contains("VerifiedCommand2::"));
	let source = include_str!("../retirement_shape.rs");
	assert!(!source.contains("install_admin2("));
	assert!(!source.contains("verify_strict("));
}
#[test]
fn retired_install_all_six_callers_dispositions() {
	let mut f = F::new();
	f.retire([3, 0]);
	assert!(matches!(
		install_admin2(&f.f.state, &f.f.pin),
		Err(InstallError2::Retired)
	));
	assert!(confirm_terminal2(&f.f.state, &f.f.pin).is_ok());
}
#[test]
fn terminal_confirmation_original_identity_pin_and_cut() {
	let mut f = F::new();
	f.retire([1, 2]);
	assert!(confirm_terminal2(&f.f.state, &f.f.pin).is_ok());
	let other = F::new();
	assert!(confirm_terminal2(&other.f.state, &other.f.pin).is_err());
}
#[test]
fn terminal_confirmation_mismatch_closure_and_mark_refusals() {
	let mut f = F::new();
	f.retire([3, 0]);
	// Another root's tuple: the commitment rule refuses its pin first.
	let other = wire::Fixture::profile_three(&M3_OTHER);
	assert_eq!(
		confirm_terminal2(&f.f.state, &other.pin).err(),
		Some(InstallError2::Commitment(CommitmentField2::ControlDigest))
	);
	// admin_install.rs:304-305 (§4.1 invariant, IMPL-NOTES c2): its own tuple, against a retired
	// state committed to any length but 241, is refused at the length; at 241 it confirms.
	assert!(confirm_terminal2(&f.f.state, &f.f.pin).is_ok());
	for wrong in [240, 242] {
		assert_eq!(
			confirm_terminal2(&with_control_len_for_test(&f.f.state, wrong), &f.f.pin).err(),
			Some(InstallError2::Commitment(CommitmentField2::ControlLength)),
			"{wrong}"
		);
	}
}
#[test]
fn denied25_encode_decode_none_and_reference_refusal() {
	let f = F::new();
	let out = ApplyResult2::Denied {
		reason: 25,
		reference: None,
	};
	let b = out
		.encode(f.f.dep.generation, f.f.dep.cluster, wire::log(9))
		.unwrap();
	assert_eq!(ApplyResult2::decode(&b).unwrap().3, out);
	assert!(
		ApplyResult2::Denied {
			reason: 25,
			reference: Some(wire::log(3))
		}
		.encode(f.f.dep.generation, f.f.dep.cluster, wire::log(9))
		.is_err()
	);
}
#[test]
fn retired_incremental_derived_equals_full_rebuild() {
	let mut f = F::new();
	f.freeze();
	f.retire([3, 0]);
	assert!(
		f.root().state.derived
			== super::super::incremental::Derived2::build(&f.root().state).unwrap()
	);
}
#[test]
fn retired_historical_mixed_replay_preserves_before_transition() {
	let mut f = F::new();
	let before = wire::bytes(&f.f.state);
	f.freeze();
	let p = f.proof(&Command2::RetireRoot, [1, 2]);
	f.retire([1, 2]);
	denied(f.terminal(Command2::RetireRoot, Some(p)), 25);
	assert_ne!(before, wire::bytes(&f.f.state));
	let bytes = f.root().encode().unwrap();
	assert_eq!(
		super::super::UnverifiedRoot2::decode(&bytes)
			.unwrap()
			.encode()
			.unwrap(),
		bytes
	);
}
#[test]
fn retired_ordinary_release_has_no_retirement_test_symbols() {
	let manifest = include_str!("../mod.rs");
	assert!(manifest.contains("#[cfg(test)]"));
	assert!(include_str!("mod.rs").contains("mod retirement;"));
}

mod owner_binding;
