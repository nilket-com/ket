//! S3 verifier tests over actual carrier bytes. Current-reducer differentials are explicitly
//! fixture-authorized, not the frozen independent S1c oracle; issuer expectations are explicit.
#[path = "governance_verification_support.rs"]
pub(in crate::storage::mutation_visibility::root2) mod support;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::committed::{CommittedSource2, committed_entry2};
use crate::storage::mutation_visibility::root2::governance_command::*;
use crate::storage::mutation_visibility::root2::governance_verification::verify_governance2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::trust::{TrustedEntry2, VerifiedCommand2};
use crate::storage::mutation_visibility::root2::types::*;
use crate::storage::mutation_visibility::root2::verification::*;
use support::*;

/// Direct producer over real-carrier identities -> S3 -> current-reducer chain, compared with the EXISTING
/// fixture-authorized current reducer at the same identity/fingerprint, not an independent oracle.
pub(in crate::storage::mutation_visibility::root2) fn positive_chain() -> Fixture {
	let mut f = Fixture::new("b");
	let selector = selector(&f);
	assert!(
		differential(&mut f, Command2::EnrollSelector(selector))
			== ApplyResult2::Registered(log(3))
	);
	assert!(differential(&mut f, enroll_authority()) == ApplyResult2::Registered(log(4)));
	assert!(
		differential(
			&mut f,
			Command2::EnrollLeaseIssuer {
				node: 7,
				key: key(ISSUER_SEED)
			}
		) == ApplyResult2::Registered(log(5))
	);
	let root = root_and_origin2(&f.state).unwrap().0;
	let issuer = root.state.issuers.get(&7u32.to_be_bytes()).unwrap();
	assert_eq!(issuer.node, 7);
	assert_eq!(issuer.key, key(ISSUER_SEED));
	assert_eq!(issuer.enrolled, log(5));
	assert_eq!(issuer.revoked, None);
	assert_eq!(issuer.encode().unwrap().len(), 77);
	assert_eq!(root.usage().unwrap().registry, 68 + 121 + 81);
	assert_eq!(&root.encode().unwrap()[..8], b"VISAUT04");
	let command = grant(&f, ISSUER_SEED);
	assert!(matches!(
		differential(&mut f, command),
		ApplyResult2::Granted(_)
	));
	let original = root_and_origin2(&f.state)
		.unwrap()
		.0
		.state
		.lineages
		.values()
		.next()
		.unwrap()
		.latest
		.clone();
	assert_eq!(original.log, log(6));
	assert_eq!(original.lease.issuer, 7);
	assert_eq!(
		original.lease.signature,
		signed_lease(&f, ISSUER_SEED).signature
	);
	let claim = prepared_claim(&f);
	let permission = permission(&f, &claim);
	assert!(matches!(
		f.apply(permission, None),
		ApplyResult2::Permission { outcome: 1, .. }
	));
	assert!(matches!(
		f.apply(Command2::Reserve(claim.clone()), None),
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert!(
		differential(
			&mut f,
			Command2::Release {
				lineage: original.lineage,
				latest: original.reference().unwrap()
			}
		) == ApplyResult2::Registered(log(9))
	);
	assert!(matches!(
		differential(
			&mut f,
			Command2::Resolve {
				identity: claim.manifest.identity.clone(),
				digest: claim.digest().unwrap(),
				reservation: log(8),
				cause: 2,
				anchor: log(9)
			}
		),
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	let parent = root_and_origin2(&f.state)
		.unwrap()
		.0
		.lookup(&claim.manifest.identity)
		.unwrap()
		.unwrap();
	assert!(
		matches!(parent.terminal,Some(Terminal2::Refused{first,cause:2,anchor}) if first==log(10)&&anchor==log(9))
	);
	assert!(
		differential(&mut f, Command2::RevokeAuthority(authority()))
			== ApplyResult2::Registered(log(11))
	);
	let epoch = f.any.admission().initial_epoch;
	assert!(
		differential(
			&mut f,
			Command2::Cut {
				expected: epoch,
				next: epoch + 1,
				configuration: [0x61; 32]
			}
		) == ApplyResult2::Registered(log(12))
	);
	assert!(
		differential(&mut f, Command2::RevokeLeaseIssuer { node: 7 })
			== ApplyResult2::Registered(log(13))
	);
	let root = root_and_origin2(&f.state).unwrap().0;
	assert_eq!(
		root.state.issuers.get(&7u32.to_be_bytes()).unwrap().revoked,
		Some(log(13))
	);
	assert_eq!(
		root.state
			.lineages
			.values()
			.next()
			.unwrap()
			.closure
			.unwrap()
			.log,
		log(9)
	);
	assert_eq!(root.state.issuers.len(), 1);
	assert_eq!(
		root.usage().unwrap().registry,
		68 + 121
			+ 81 + crate::storage::mutation_visibility::root2::admission_registry::PERMISSION_CHARGE2
	);
	assert_eq!(root.state.parents.len(), 1);
	// apply_terminal retains the parent: 766 fixed-width claim bytes plus 111.
	assert_eq!(root.usage().unwrap().held, 766 + 111);
	f
}
#[test]
#[cfg(target_endian = "little")]
fn s3_nine_real_carriers_match_current_reducer_and_explicit_issuer_effects() {
	let f = positive_chain();
	assert_eq!(tip(&f.state), log(13));
}

#[test]
#[cfg(target_endian = "little")]
fn s3_presence_length_and_structural_refusals_are_atomic() {
	let f = Fixture::new("b");
	let before = bytes(&f.state);
	for command in nine(&f) {
		let e = carrier(&command, f.dep, 3, None);
		assert_eq!(
			step(&f.state, &f.pin, f.dep, &e, &command, None).err(),
			Some(VerifyError2::MissingSidecar)
		);
		let envelope = f.envelope(&f.state, &command, 0x92);
		// The adapter refuses wrong raw size; direct producer can receive these slices.
		let valid_entry = carrier(&command, f.dep, 3, Some(&envelope));
		for side in [&envelope[..295], &[0u8; 297][..]] {
			assert_eq!(
				step(&f.state, &f.pin, f.dep, &valid_entry, &command, Some(side)).err(),
				Some(VerifyError2::SidecarLength)
			);
		}
	}
	let command = Command2::EnrollSelector(selector(&f));
	let valid = f.envelope(&f.state, &command, 0x92);
	let cases = [
		(0..1, GovernanceCommandError2::Magic),
		(8..10, GovernanceCommandError2::Version),
		(10..12, GovernanceCommandError2::Reserved),
		(
			12..28,
			GovernanceCommandError2::ZeroField(GovernanceField2::Generation),
		),
		(
			28..44,
			GovernanceCommandError2::ZeroField(GovernanceField2::Cluster),
		),
		(
			160..168,
			GovernanceCommandError2::ZeroField(GovernanceField2::Epoch),
		),
		(
			44..52,
			GovernanceCommandError2::ZeroField(GovernanceField2::InstalledAt),
		),
		(
			52..56,
			GovernanceCommandError2::ZeroField(GovernanceField2::InstalledAt),
		),
		(
			56..64,
			GovernanceCommandError2::ZeroField(GovernanceField2::InstalledAt),
		),
	];
	for (range, error) in cases {
		let mut side = valid.clone();
		side[range.clone()].fill(0);
		if error == GovernanceCommandError2::Reserved {
			side[10] = 1;
		}
		if error == GovernanceCommandError2::Version {
			side[8] = 2;
		}
		let e = carrier(&command, f.dep, 3, Some(&side));
		assert_eq!(
			step(&f.state, &f.pin, f.dep, &e, &command, Some(&side)).err(),
			Some(VerifyError2::Governance(error))
		);
	}
	assert_eq!(
		parse_governance_command2(&valid[..295]).err(),
		Some(GovernanceCommandError2::Length)
	); // shadowed by SidecarLength in producer
	assert_eq!(bytes(&f.state), before);
}

#[test]
#[cfg(target_endian = "little")]
fn s3_profile_context_digest_signature_and_scope_denials_are_consumed() {
	let f = Fixture::new("a");
	for command in nine(&f) {
		let side = f.envelope(&f.state, &command, 0x92);
		denied(&f, &command, &side);
	}
	let f = Fixture::new("b");
	let command = Command2::EnrollSelector(selector(&f));
	let ctx = admin_context2(&install_admin2(&f.state, &f.pin).unwrap());
	let digest = command.digest(f.dep.generation, f.dep.cluster).unwrap();
	for i in 0..8 {
		let mut c = ctx;
		match i {
			0 => c.generation[0] ^= 1,
			1 => c.cluster[0] ^= 1,
			2 => c.installed_at.index += 1,
			3 => c.bootstrap_fingerprint[0] ^= 1,
			4 => c.bootstrap_command[0] ^= 1,
			5 => c.genesis[0] ^= 1,
			6 => c.initial_epoch += 1,
			_ => c.manifest_pin[0] ^= 1,
		}
		denied(&f, &command, &sign_context(c, digest, 0x92));
	}
	let mut wrong = digest;
	wrong[0] ^= 1;
	denied(&f, &command, &sign_context(ctx, wrong, 0x92));
	for seed in [0x42, 0x93] {
		denied(&f, &command, &f.envelope(&f.state, &command, seed));
	}
	let mut side = f.envelope(&f.state, &command, 0x92);
	side[240] ^= 1;
	denied(&f, &command, &side);
	let mut side = f.envelope(&f.state, &command, 0x92);
	side[264..296].copy_from_slice(&[0xff; 32]);
	denied(&f, &command, &side);
	for i in 0..2 {
		let mut b = selector(&f);
		if i == 0 {
			b.tenant += 1
		} else {
			b.generation[0] ^= 1
		};
		let c = Command2::EnrollSelector(b);
		denied(&f, &c, &f.envelope(&f.state, &c, 0x92));
	}
	for i in 0..3 {
		let mut id = identity(&f);
		match i {
			0 => id.tenant += 1,
			1 => id.generation[0] ^= 1,
			_ => {
				id.scope = MutationScope::Table;
				id.table = [0x51; 16];
				id.schema_generation = 1;
			}
		}
		let c = Command2::Resolve {
			identity: id,
			digest: [0x31; 32],
			reservation: log(3),
			cause: 1,
			anchor: log(2),
		};
		denied(&f, &c, &f.envelope(&f.state, &c, 0x92));
	}
	let f = Fixture::table_namespace();
	for i in 0..2 {
		let mut id = identity(&f);
		if i == 0 {
			id.table[0] ^= 1;
		} else {
			id.schema_generation += 1;
		}
		let c = Command2::Resolve {
			identity: id,
			digest: [0x31; 32],
			reservation: log(3),
			cause: 1,
			anchor: log(2),
		};
		denied(&f, &c, &f.envelope(&f.state, &c, 0x92));
	}
}

#[test]
#[cfg(target_endian = "little")]
fn s3_issuer_failures_after_valid_resigned_envelopes_preserve_effects() {
	let mut f = Fixture::new("b");
	let command = grant(&f, ISSUER_SEED);
	denied(&f, &command, &f.envelope(&f.state, &command, 0x92));
	f.governed(Command2::EnrollLeaseIssuer {
		node: 7,
		key: key(ISSUER_SEED),
	});
	for seed in [0x34, ISSUER_SEED] {
		let mut command = grant(&f, seed);
		if seed == ISSUER_SEED {
			let Command2::Grant { lease, .. } = &mut command else {
				unreachable!()
			};
			lease.expiry.physical_ms += 1;
		}
		let side = f.envelope(&f.state, &command, 0x92);
		denied(&f, &command, &side);
	}
	let mut command = grant(&f, ISSUER_SEED);
	let Command2::Grant { lease, .. } = &mut command else {
		unreachable!()
	};
	lease.signature[32..].fill(0xff);
	let side = f.envelope(&f.state, &command, 0x92);
	denied(&f, &command, &side);
	assert!(matches!(
		f.governed(grant(&f, ISSUER_SEED)),
		ApplyResult2::Granted(_)
	));
	let original = root_and_origin2(&f.state)
		.unwrap()
		.0
		.state
		.lineages
		.values()
		.next()
		.unwrap()
		.latest
		.clone();
	f.governed(Command2::Release {
		lineage: original.lineage,
		latest: original.reference().unwrap(),
	});
	let closed = root_and_origin2(&f.state)
		.unwrap()
		.0
		.state
		.lineages
		.values()
		.next()
		.unwrap()
		.clone();
	f.governed(Command2::RevokeLeaseIssuer { node: 7 });
	let command = grant(&f, ISSUER_SEED);
	denied(&f, &command, &f.envelope(&f.state, &command, 0x92));
	assert_eq!(
		root_and_origin2(&f.state)
			.unwrap()
			.0
			.state
			.lineages
			.values()
			.next()
			.unwrap()
			.encode()
			.unwrap(),
		closed.encode().unwrap()
	);
}

#[test]
fn s3_reusable_envelope_registers_first_and_exact_coordinate_replays() {
	let mut f = Fixture::new("b");
	let command = Command2::EnrollSelector(selector(&f));
	let side = f.envelope(&f.state, &command, 0x92);
	assert!(f.apply(command.clone(), Some(side.clone())) == ApplyResult2::Registered(log(3)));
	assert!(f.apply(command.clone(), Some(side.clone())) == ApplyResult2::Registered(log(3)));
	let e = f.entries.last().unwrap();
	let before = bytes(&f.state);
	let (same, out) = step(&f.state, &f.pin, f.dep, e, &command, Some(&side)).unwrap();
	assert!(out == ApplyResult2::AlreadyProcessed);
	assert_eq!(bytes(&same), before);
	let gap = carrier(&command, f.dep, 6, Some(&side));
	assert_eq!(
		step(&f.state, &f.pin, f.dep, &gap, &command, Some(&side)).err(),
		Some(VerifyError2::Consumer(RootError2::LineageGap))
	);
	assert_eq!(bytes(&f.state), before);
	assert_eq!(
		root_and_origin2(&f.state).unwrap().0.state.selectors.len(),
		1
	);
}

#[test]
fn s3_install_commitment_and_record_refusals_follow_structure() {
	let f = Fixture::new("b");
	let command = Command2::EnrollSelector(selector(&f));
	let side = f.envelope(&f.state, &command, 0x92);
	let digest = command.digest(f.dep.generation, f.dep.cluster).unwrap();
	let wrong_length = with_control_len_for_test(&f.state, 137);
	let mut bad = side.clone();
	bad[0] ^= 1;
	assert_eq!(
		verify_governance2(&wrong_length, &f.pin, &command, Some(&bad), digest),
		Err(VerifyError2::Governance(GovernanceCommandError2::Magic))
	);
	assert_eq!(
		verify_governance2(&wrong_length, &f.pin, &command, Some(&side), digest),
		Err(VerifyError2::Install(InstallError2::Commitment(
			CommitmentField2::ControlLength
		)))
	);
	let other = Fixture::new("a");
	assert_eq!(
		verify_governance2(&f.state, &other.pin, &command, Some(&side), digest),
		Err(VerifyError2::Install(InstallError2::Commitment(
			CommitmentField2::ControlDigest
		)))
	);
	for i in 0..4 {
		let mut record = f.record;
		let field = match i {
			0 => {
				record.identity.bootstrap.index += 1;
				InstallField2::Bootstrap
			}
			1 => {
				record.identity.bootstrap_fingerprint[0] ^= 1;
				InstallField2::BootstrapFingerprint
			}
			2 => {
				record.identity.bootstrap_command[0] ^= 1;
				InstallField2::BootstrapCommand
			}
			_ => {
				record.identity.genesis_fingerprint[0] ^= 1;
				InstallField2::Genesis
			}
		};
		let pin = tuple_for(&f.manifest, record);
		assert_eq!(
			verify_governance2(&f.state, &pin, &command, Some(&side), digest),
			Err(VerifyError2::Install(InstallError2::Record(field)))
		);
	}
	assert_eq!(
		verify_governance2(&empty_authority2(), &f.pin, &command, None, digest),
		Err(VerifyError2::StateKind)
	); // prerequisite unavailable shadowed by state
	assert_eq!(tip(&f.state), log(2));
}

#[test]
fn s3_empty_issuer_old_kinds_keep_frozen_oracle_control() {
	let mut f = Fixture::new("b");
	for command in [
		Command2::EnrollSelector(selector(&f)),
		enroll_authority(),
		Command2::RevokeAuthority(authority()),
	] {
		let side = f.envelope(&f.state, &command, 0x92);
		let e = carrier(&command, f.dep, tip(&f.state).index + 1, Some(&side));
		let ce = committed_entry2(&CommittedSource2::synthetic(), &e)
			.unwrap()
			.into_committed();
		let trusted = TrustedEntry2::bind(&ce, &command, f.dep.generation, f.dep.cluster).unwrap();
		let token =
			VerifiedCommand2::fixture(f.dep.generation, f.dep.cluster, &command, &[], 0).unwrap();
		let root = root_and_origin2(&f.state).unwrap().0;
		assert_eq!(root.state.issuers.len(), 0);
		let (expected, out) = crate::storage::mutation_visibility::root2::tests::s1c::checked_step(
			root, &trusted, &token, &command,
		)
		.unwrap();
		let actual = f.apply(command, Some(side));
		assert!(actual == out);
		assert_eq!(
			root_and_origin2(&f.state).unwrap().0.encode().unwrap(),
			expected.encode().unwrap()
		);
	}
}
