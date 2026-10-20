//! Role epoch tests use actual carrier adaptation and the sole production verification producer.
use super::governance_verification::support as wire;
use super::profile_three_fixture::s1_command;
use crate::storage::mutation_visibility::root2::{
	admin_install::*, admin_statement::*, codec::*, command::*, origin::*, role_command, types::*,
	verification::*,
};
use ed25519_dalek::{Signer, SigningKey};
mod support;
use support::*;

#[test]
fn freeze_recovery_only_current_epoch_succeeds() {
	let mut f = F::new();
	f.rotate(2, 41);
	f.freeze();
	assert!(f.root().state.role_epochs.as_ref().unwrap().frozen);
	assert_eq!(f.transition(), wire::tip(&f.f.state));
	f.roundtrip();
}
#[test]
fn unfreeze_recovery_and_admin_succeeds() {
	let mut f = F::new();
	f.freeze();
	f.unfreeze([1, 3]);
	assert!(!f.root().state.role_epochs.as_ref().unwrap().frozen);
	assert_eq!(f.transition(), wire::tip(&f.f.state));
	f.roundtrip();
}
#[test]
fn unfreeze_recovery_and_governance_succeeds() {
	let mut f = F::new();
	f.freeze();
	f.unfreeze([2, 3]);
	assert!(!f.root().state.role_epochs.as_ref().unwrap().frozen);
	f.roundtrip();
}
#[test]
fn wrong_missing_duplicate_or_reordered_roles_denied8() {
	for roles in [[1, 0], [2, 0], [1, 2], [0, 0], [3, 3], [3, 1]] {
		let mut f = F::new();
		let c = Command2::Freeze(zero());
		let proof = f.proof_as(&c, roles);
		deny(f.apply(c, proof), 8);
		assert!(f.root().state.role_epochs.is_none());
	}
	for roles in [[1, 0], [2, 0], [3, 0], [1, 2], [3, 1], [3, 2], [3, 3]] {
		let mut f = F::new();
		f.freeze();
		let c = Command2::Unfreeze(f.transition());
		let proof = f.proof_as(&c, roles);
		deny(f.apply(c, proof), 8);
		assert!(f.root().state.role_epochs.as_ref().unwrap().frozen);
	}
}
#[test]
fn freeze_and_unfreeze_domains_are_distinct_and_exhaustive() {
	let f = F::new();
	let c = Command2::Freeze(zero());
	let proof = f.proof(&c);
	let domains = [
		(
			role_command::RoleForm3::Freeze,
			[3, 0],
			0,
			b"VISROOT2-ROLE3-FREEZE-RECOVERY\0".as_slice(),
		),
		(
			role_command::RoleForm3::Unfreeze,
			[1, 3],
			0,
			b"VISROOT2-ROLE3-UNFREEZE-ADMIN\0".as_slice(),
		),
		(
			role_command::RoleForm3::Unfreeze,
			[1, 3],
			1,
			b"VISROOT2-ROLE3-UNFREEZE-RECOVERY-WITH-ADMIN\0".as_slice(),
		),
		(
			role_command::RoleForm3::Unfreeze,
			[2, 3],
			0,
			b"VISROOT2-ROLE3-UNFREEZE-GOVERNANCE\0".as_slice(),
		),
		(
			role_command::RoleForm3::Unfreeze,
			[2, 3],
			1,
			b"VISROOT2-ROLE3-UNFREEZE-RECOVERY-WITH-GOVERNANCE\0".as_slice(),
		),
	];
	for (form, roles, slot, domain) in domains {
		let mut input = proof.clone();
		input[76] = roles[0];
		input[150] = roles[1];
		// Canonical presence/absence for this tuple; signatures are irrelevant to preimage construction.
		for (i, at) in [76, 150].into_iter().enumerate() {
			input[at..at + 74].fill(0);
			input[at] = roles[i];
			if roles[i] != 0 {
				input[at + 9] = 1;
			}
		}
		let bytes = role_command::preimage_for(&input, roles, slot, form).unwrap();
		assert!(bytes.starts_with(domain));
	}
	for roles in [[1, 0], [2, 0], [1, 2], [3, 0], [1, 3], [2, 3]] {
		for slot in 0..2 {
			let valid = roles == [3, 0] && slot == 0;
			let result =
				role_command::preimage_for(&proof, roles, slot, role_command::RoleForm3::Freeze);
			if !valid {
				assert!(result.is_err());
			}
		}
	}
}
#[test]
fn command_kinds18_19_binding_log_roundtrip_and_malformed_refusal() {
	let f = F::new();
	for log in [zero(), wire::log(7)] {
		for (kind, c) in [(18, Command2::Freeze(log)), (19, Command2::Unfreeze(log))] {
			let bytes = c.encode(f.f.dep.generation, f.f.dep.cluster).unwrap();
			let (generation, cluster, decoded) = Command2::decode(&bytes).unwrap();
			assert_eq!((generation, cluster), (f.f.dep.generation, f.f.dep.cluster));
			assert!(decoded == c);
			let payload = c.payload(generation, cluster).unwrap();
			assert_eq!(payload[32], kind);
			assert_eq!(payload.len(), 53);
			assert!(Command2::decode(&bytes[..bytes.len() - 1]).is_err());
		}
	}
}
#[test]
fn repeated_or_stale_freeze_unfreeze_denied24() {
	let mut f = F::new();
	f.freeze();
	let c = Command2::Freeze(f.transition());
	let proof = f.proof(&c);
	deny(f.apply(c, proof), 24);
	let c = Command2::Unfreeze(zero());
	let proof = f.proof(&c);
	deny(f.apply(c, proof), 24);
	f.unfreeze([1, 3]);
	let c = Command2::Unfreeze(f.transition());
	let proof = f.proof(&c);
	deny(f.apply(c, proof), 24);
	let c = Command2::Freeze(zero());
	let proof = f.proof(&c);
	deny(f.apply(c, proof), 24);
}
#[test]
fn freeze_zero_then_last_unfreeze_binding_is_single_use() {
	let mut f = F::new();
	f.freeze();
	f.unfreeze([1, 3]);
	let previous = f.transition();
	f.freeze();
	let c = Command2::Freeze(previous);
	let proof = f.proof(&c);
	deny(f.apply(c, proof), 24);
}
#[test]
fn unfreeze_exact_freeze_binding_is_single_use() {
	let mut f = F::new();
	f.freeze();
	let previous = f.transition();
	f.unfreeze([2, 3]);
	f.freeze();
	let c = Command2::Unfreeze(previous);
	let proof = f.proof(&c);
	deny(f.apply(c, proof), 24);
}
#[test]
fn n0_first_freeze_creates_v5_and_unfreeze_keeps_v5_for_life() {
	let mut f = F::new();
	assert_eq!(&f.root().encode().unwrap()[..8], b"VISAUT04");
	f.freeze();
	let bytes = f.root().encode().unwrap();
	assert_eq!(&bytes[..8], b"VISAUT05");
	assert_eq!(f.root().usage().unwrap().history, 120);
	f.unfreeze([1, 3]);
	assert_eq!(&f.root().encode().unwrap()[..8], b"VISAUT05");
	assert_eq!(f.root().usage().unwrap().history, 120);
	assert!(
		f.root()
			.state
			.role_epochs
			.as_ref()
			.unwrap()
			.successors
			.iter()
			.all(Vec::is_empty)
	);
	f.roundtrip();
}

#[test]
fn old_role_preimages_and_rotation_vectors_unchanged() {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("role_epochs/vectors.json")).unwrap();
	let unhex = |s: &str| {
		(0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
			.collect::<Vec<_>>()
	};
	let proof = unhex(v["proof"].as_str().unwrap());
	for slot in 0..2 {
		let old = role_command::preimage(&proof, [1, 2], slot).unwrap();
		assert_eq!(old, unhex(v["proof_preimages"][slot].as_str().unwrap()));
		assert_eq!(
			old,
			role_command::preimage_for(&proof, [1, 2], slot, role_command::RoleForm3::Legacy)
				.unwrap()
		);
	}
}
#[test]
fn full_command_profile_frozen_state_matrix() {
	let rows: serde_json::Value =
		serde_json::from_str(include_str!("role_freeze/matrix.json")).unwrap();
	assert_eq!(rows.as_array().unwrap().len(), 120);
	let mut seen = std::collections::BTreeSet::new();
	for row in rows.as_array().unwrap() {
		let kind = row["kind"].as_u64().unwrap() as u8;
		let profile = row["profile"].as_u64().unwrap();
		let frozen = row["frozen"].as_bool().unwrap();
		assert!(seen.insert((kind, profile, frozen)));
		let mut f = F::new();
		if frozen {
			f.freeze();
		}
		let mut commands = wire::nine(&f.f);
		for name in [
			"command_bootstrap",
			"command_cover_blank",
			"command_reserve",
			"command_finalize",
		] {
			commands.push(s1_command(name));
		}
		commands.extend([
			Command2::AdmissionPermission(f.legacy_statement()),
			Command2::EpochAdmissionPermission(f.statement()),
			F::command(0, 40),
			F::command(1, 41),
			F::command(2, 42),
			Command2::Freeze(zero()),
			Command2::Unfreeze(f.transition()),
		]);
		assert_eq!(commands.len(), 20);
		let c = commands
			.into_iter()
			.find(|c| c.payload(f.f.dep.generation, f.f.dep.cluster).unwrap()[32] == kind)
			.unwrap();
		if frozen && profile < 3 {
			let mut state = f.root().state.clone();
			state.control_len = if profile == 1 { 137 } else { 189 };
			assert!(super::super::validation::validate_state(&state).is_err());
		} else {
			let expected = frozen && [2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 17].contains(&kind);
			assert_eq!(
				super::super::role_freeze::blocked(&f.root().state, &c),
				expected
			);
		}
	}
	for profile in ["a", "b"] {
		for c in [Command2::Freeze(zero()), Command2::Unfreeze(zero())] {
			let mut f = wire::Fixture::new(profile);
			let proof = F::new().proof(&c);
			deny(f.apply(c, Some(proof)), 8);
		}
	}
}

#[test]
fn frozen_canonical_unauthorized_control_denied23_before8() {
	for index in 0..9 {
		let mut f = F::new();
		f.freeze();
		let c = wire::nine(&f.f).remove(index);
		let mut proof = f.proof(&c);
		proof[86..150].fill(0);
		proof[160..224].fill(0);
		role_command::parse(&proof).unwrap();
		deny(f.f.apply(c, Some(proof)), 23);
	}
	for epoch in [false, true] {
		let mut f = F::new();
		f.freeze();
		let c = if epoch {
			Command2::EpochAdmissionPermission(f.statement())
		} else {
			Command2::AdmissionPermission(f.legacy_statement())
		};
		deny(f.f.apply(c, None), 23);
	}
}
#[test]
fn malformed_sidecar_precedes_frozen_denial() {
	let mut f = F::new();
	f.freeze();
	let c = wire::enroll_authority();
	let malformed = vec![0; 223];
	let before = wire::bytes(&f.f.state);
	let index = wire::tip(&f.f.state).index + 1;
	let entry = wire::carrier(&c, f.f.dep, index, Some(&malformed));
	let adapted = super::super::committed::committed_entry2(
		&super::super::committed::CommittedSource2::synthetic(),
		&entry,
	)
	.unwrap();
	assert!(matches!(
		adapted.committed().entry_fields().2,
		EntryClass2::MalformedRoot2
	));
	assert_eq!(wire::bytes(&f.f.state), before);
}
#[test]
fn rotate_recovery_and_recovery_rotations_remain_available() {
	let mut f = F::new();
	f.freeze();
	for (role, seed) in [(0, 40), (1, 41), (2, 42)] {
		f.rotate(role, seed);
	}
	assert!(f.root().state.role_epochs.as_ref().unwrap().frozen);
	f.unfreeze([2, 3]);
	f.roundtrip();
}
#[test]
fn cover_reserve_finalize_and_nonroot_entries_unchanged() {
	let mut f = F::new();
	f.freeze();
	for name in ["command_cover_blank", "command_reserve", "command_finalize"] {
		let c =
			crate::storage::mutation_visibility::root2::tests::profile_three_fixture::s1_command(
				name,
			);
		assert!(!super::super::role_freeze::blocked(&f.root().state, &c));
	}
	let c = Command2::Cover {
		kind: 1,
		external: [9; 32],
	};
	assert!(!super::super::role_freeze::blocked(&f.root().state, &c));
	let old = f.root().state.role_epochs.clone();
	let index = wire::tip(&f.f.state).index + 1;
	let entry = wire::entry(index);
	let adapted = super::super::committed::committed_entry2(
		&super::super::committed::CommittedSource2::synthetic(),
		&entry,
	)
	.unwrap();
	let (next, result) = advance_cover2(&f.f.state, &adapted.into_committed()).unwrap();
	assert!(result == ApplyResult2::Covered);
	assert_eq!(wire::tip(&next), wire::log(index));
	assert!(root_and_origin2(&next).unwrap().0.state.role_epochs == old);
}
#[test]
fn existing_permission_lease_holder_authority_survives_freeze() {
	let mut f = F::new();
	f.enroll();
	let permission = Command2::EpochAdmissionPermission(f.statement());
	assert!(matches!(
		f.f.apply(permission, None),
		ApplyResult2::Permission { outcome: 1, .. }
	));
	let c = Command2::EnrollSelector(wire::selector(&f.f));
	let proof = f.proof(&c);
	assert!(matches!(f.apply(c, proof), ApplyResult2::Registered(_)));
	let c = Command2::EnrollLeaseIssuer {
		node: 7,
		key: key(wire::ISSUER_SEED),
	};
	let proof = f.proof(&c);
	assert!(matches!(f.apply(c, proof), ApplyResult2::Registered(_)));
	let c = wire::grant(&f.f, wire::ISSUER_SEED);
	let proof = f.proof(&c);
	assert!(matches!(f.apply(c, proof), ApplyResult2::Granted(_)));
	let permissions = f.root().state.permissions.clone();
	let authorities = f.root().state.authorities.clone();
	let grants = f.root().state.lineages.clone();
	let issuers = f.root().state.issuers.clone();
	f.freeze();
	assert!(f.root().state.permissions == permissions);
	assert!(f.root().state.authorities == authorities);
	assert!(f.root().state.lineages == grants);
	assert!(f.root().state.issuers == issuers);
	f.unfreeze([1, 3]);
	f.roundtrip();
}
#[test]
fn exact_entry_replay_and_changed_fingerprint_preserved() {
	let mut f = F::new();
	let c = Command2::Freeze(zero());
	let proof = f.proof(&c);
	f.apply(c.clone(), proof.clone());
	let entry = f.f.entries.last().unwrap();
	let (next, result) =
		wire::step(&f.f.state, &f.f.pin, f.f.dep, entry, &c, Some(&proof)).unwrap();
	assert!(result == ApplyResult2::AlreadyProcessed);
	assert_eq!(wire::bytes(&next), wire::bytes(&f.f.state));
	let committed = super::super::committed::committed_entry2(
		&super::super::committed::CommittedSource2::synthetic(),
		entry,
	)
	.unwrap()
	.into_committed();
	let mut trusted = super::super::trust::TrustedEntry2::bind(
		&committed,
		&c,
		f.f.dep.generation,
		f.f.dep.cluster,
	)
	.unwrap();
	trusted.change_fingerprint();
	assert!(
		advance_verified2(&f.f.state, &f.f.pin, &committed, &trusted, &c, Some(&proof)).is_err()
	);
}
#[test]
fn frozen_denial_advances_only_tip_without_effect_or_history() {
	let mut f = F::new();
	f.freeze();
	let old = f.root().state.clone();
	let old_tip = wire::tip(&f.f.state);
	let c = wire::enroll_authority();
	let mut proof = f.proof(&c);
	proof[86..150].fill(0);
	proof[160..224].fill(0);
	role_command::parse(&proof).unwrap();
	let result = f.f.apply(c, Some(proof));
	deny(result.clone(), 23);
	let roundtrip = |f: &F, result: &ApplyResult2| {
		let bytes = result
			.encode(f.f.dep.generation, f.f.dep.cluster, wire::tip(&f.f.state))
			.unwrap();
		assert_eq!(ApplyResult2::decode(&bytes).unwrap().3, *result);
	};
	roundtrip(&f, &result);
	assert_eq!(wire::tip(&f.f.state).index, old_tip.index + 1);
	assert!(f.root().state.role_epochs == old.role_epochs);
	assert!(f.root().state.authorities == old.authorities);
	assert!(f.root().state.lineages == old.lineages);
	assert!(f.root().state.derived.usage == old.derived.usage);
	let c = Command2::Freeze(zero());
	let proof = f.proof(&c);
	let result = f.apply(c, proof);
	deny(result.clone(), 24);
	roundtrip(&f, &result);
}
#[test]
fn never_frozen_n0_v4_and_b1_v5_bytes_unchanged() {
	let f = F::new();
	assert_eq!(&f.root().encode().unwrap()[..8], b"VISAUT04");
	let v: serde_json::Value =
		serde_json::from_str(include_str!("role_epochs/vectors.json")).unwrap();
	for key in ["state_v4", "state_v5"] {
		let s = v[key].as_str().unwrap();
		let b = (0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
			.collect::<Vec<_>>();
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
fn v5_empty_zero_extension_is_noncanonical_and_bad_flags_refused() {
	let mut f = F::new();
	f.freeze();
	let bytes = f.root().encode().unwrap();
	let flag = 16 + 240 + 96 + 3;
	for value in [2, 255] {
		let mut b = bytes.clone();
		b[flag] = value;
		assert!(super::super::UnverifiedRoot2::decode(&reseal(b)).is_err());
	}
	let mut b = bytes;
	b[flag..flag + 21].fill(0);
	assert!(super::super::UnverifiedRoot2::decode(&reseal(b)).is_err());
}
#[test]
fn freeze_coordinate_validation_and_derived_non_grants_recompute() {
	let mut f = F::new();
	f.freeze();
	let first = f.transition();
	assert!(f.root().state.derived.non_grants.contains_key(&first.index));
	f.unfreeze([1, 3]);
	let second = f.transition();
	assert!(!f.root().state.derived.non_grants.contains_key(&first.index));
	assert!(
		f.root()
			.state
			.derived
			.non_grants
			.contains_key(&second.index)
	);
	let mut s = f.root().state.clone();
	s.role_epochs.as_mut().unwrap().transition = s.bootstrap;
	assert!(super::super::validation::validate_state(&s).is_err());
	f.roundtrip();
	f.freeze();
	assert_eq!(
		f.root().state.derived,
		super::super::incremental::Derived2::build(&f.root().state).unwrap()
	);
	f.unfreeze([2, 3]);
	assert_eq!(
		f.root().state.derived,
		super::super::incremental::Derived2::build(&f.root().state).unwrap()
	);
	f.freeze();
	assert_eq!(
		f.root().state.derived,
		super::super::incremental::Derived2::build(&f.root().state).unwrap()
	);
}
#[test]
fn freeze_capacity_failure_rolls_back_field_and_extension() {
	for budget in [0, 119, 120] {
		let mut f = F::new();
		f.f.state = lower_limits_for_test(
			&f.f.state,
			Limits2 {
				history: budget,
				..Limits2::default()
			},
		);
		let c = Command2::Freeze(zero());
		let proof = f.proof(&c);
		let result = f.apply(c, proof);
		if budget == 120 {
			assert!(matches!(result, ApplyResult2::Registered(_)));
			assert_eq!(f.root().usage().unwrap().history, 120);
		} else {
			deny(result, 11);
			assert!(f.root().state.role_epochs.is_none());
		}
		assert_eq!(
			f.root().state.derived,
			super::super::incremental::Derived2::build(&f.root().state).unwrap()
		);
	}
}

#[test]
fn independent_python_vectors_and_mixed_history_replay() {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("role_freeze/vectors.json")).unwrap();
	let unhex = |s: &str| {
		(0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
			.collect::<Vec<_>>()
	};
	for state in v["states"].as_array().unwrap() {
		let bytes = unhex(state.as_str().unwrap());
		assert_eq!(
			super::super::UnverifiedRoot2::decode(&bytes)
				.unwrap()
				.encode()
				.unwrap(),
			bytes
		);
	}
	for vector in v["proofs"].as_array().unwrap() {
		let form = if vector["form"] == "freeze" {
			role_command::RoleForm3::Freeze
		} else {
			role_command::RoleForm3::Unfreeze
		};
		let roles = [
			vector["roles"][0].as_u64().unwrap() as u8,
			vector["roles"][1].as_u64().unwrap() as u8,
		];
		let proof = unhex(vector["proof"].as_str().unwrap());
		assert_eq!(proof.len(), 224);
		assert_eq!(
			role_command::preimage_for(
				&proof,
				roles,
				vector["slot"].as_u64().unwrap() as usize,
				form
			)
			.unwrap(),
			unhex(vector["preimage"].as_str().unwrap())
		);
	}
	let mut f = F::new();
	let mut state = F::new().f.state;
	assert_eq!(wire::bytes(&state), wire::bytes(&f.f.state));
	let start = f.f.entries.len();
	f.freeze();
	f.rotate(0, 40);
	f.unfreeze([1, 3]);
	f.rotate(2, 41);
	f.freeze();
	f.unfreeze([2, 3]);
	for entry in &f.f.entries[start..] {
		let adapted = super::super::committed::committed_entry2(
			&super::super::committed::CommittedSource2::synthetic(),
			entry,
		)
		.unwrap();
		let (_, _, carrier) = adapted.into_parts();
		let (_, _, command, proof) = carrier.unwrap().into_parts();
		state = wire::step(&state, &f.f.pin, f.f.dep, entry, &command, proof)
			.unwrap()
			.0;
	}
	assert_eq!(wire::bytes(&state), wire::bytes(&f.f.state));
	f.roundtrip();
}
