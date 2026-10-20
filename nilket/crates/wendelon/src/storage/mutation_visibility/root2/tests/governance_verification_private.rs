//! Exact first-cause evidence nested under the private S3 helper. No public reason API.
use super::*;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin_install::{
	CommitmentField2, InstallError2, admin_context2, install_admin2,
};
use crate::storage::mutation_visibility::root2::governance_command::GovernanceField2;
use crate::storage::mutation_visibility::root2::origin::{
	empty_authority2, with_control_len_for_test,
};
use crate::storage::mutation_visibility::root2::tests::governance_verification::support::*;

fn failure(
	f: &Fixture,
	command: &Command2,
	sidecar: Option<&[u8]>,
) -> Result<Option<Denial2>, VerifyError2> {
	governance_failure2(
		&f.state,
		&f.pin,
		command,
		sidecar,
		command.digest(f.dep.generation, f.dep.cluster).unwrap(),
	)
}
#[test]
#[cfg(target_endian = "little")]
fn s3_private_envelope_scope_causes_and_signature_precedence() {
	let f = Fixture::new("a");
	let c = Command2::EnrollSelector(selector(&f));
	let mut b = f.envelope(&f.state, &c, 0x93);
	b[200] ^= 1;
	assert_eq!(
		failure(&f, &c, Some(&b)),
		Ok(Some(Denial2::Envelope(
			GovernanceCommandError2::NoGovernance
		)))
	);
	let f = Fixture::primary();
	let c = Command2::EnrollSelector(selector(&f));
	let ctx = admin_context2(&install_admin2(&f.state, &f.pin).unwrap());
	let digest = c.digest(f.dep.generation, f.dep.cluster).unwrap();
	let mut wrong = ctx;
	wrong.genesis[0] ^= 1;
	let mut b = sign_context(wrong, [0x99; 32], 0x93);
	assert_eq!(
		failure(&f, &c, Some(&b)),
		Ok(Some(Denial2::Envelope(GovernanceCommandError2::Context)))
	);
	b = sign_context(ctx, [0x99; 32], 0x93);
	assert_eq!(
		failure(&f, &c, Some(&b)),
		Ok(Some(Denial2::Envelope(GovernanceCommandError2::Digest)))
	);
	b = sign_context(ctx, digest, NON_GOVERNANCE_SEEDS[0]);
	assert_eq!(
		failure(&f, &c, Some(&b)),
		Ok(Some(Denial2::Envelope(GovernanceCommandError2::Signature)))
	);
	let mut binding = selector(&f);
	binding.tenant += 1;
	let c = Command2::EnrollSelector(binding);
	let b = f.envelope(&f.state, &c, 0x93);
	assert_eq!(
		failure(&f, &c, Some(&b)),
		Ok(Some(Denial2::Envelope(GovernanceCommandError2::Signature)))
	);
	let b = f.envelope(&f.state, &c, GOVERNANCE_SEED);
	assert_eq!(failure(&f, &c, Some(&b)), Ok(Some(Denial2::Scope)));
	// VALID Table namespace makes isolated table and schema mismatches codec-reachable.
	let f = Fixture::table_namespace();
	for i in 0..5 {
		let mut id = identity(&f);
		match i {
			0 => id.generation[0] ^= 1,
			1 => id.tenant += 1,
			2 => {
				id.scope = MutationScope::Raw;
				id.table = [0; 16];
				id.schema_generation = 0;
			}
			3 => id.table[0] ^= 1,
			_ => id.schema_generation += 1,
		};
		let c = Command2::Resolve {
			identity: id,
			digest: [0x71; 32],
			reservation: log(3),
			cause: 1,
			anchor: log(2),
		};
		let b = f.envelope(&f.state, &c, GOVERNANCE_SEED);
		assert_eq!(failure(&f, &c, Some(&b)), Ok(Some(Denial2::Scope)));
	}
	// Action failure is unreachable with the fixed 0x1ff governance mask; existing permits tests
	// pin that primitive. Scope and issuer cannot coexist on one kind (Grant is root-wide).
}
#[test]
#[cfg(target_endian = "little")]
fn s3_private_presence_parse_install_and_authorization_adjacent_precedence() {
	let f = Fixture::primary();
	let c = Command2::EnrollSelector(selector(&f));
	let digest = c.digest(f.dep.generation, f.dep.cluster).unwrap();
	let mut b = f.envelope(&f.state, &c, 0x93);
	assert_eq!(failure(&f, &c, None), Err(VerifyError2::MissingSidecar));
	b[0] = 0;
	assert_eq!(
		failure(&f, &c, Some(&b[..295])),
		Err(VerifyError2::SidecarLength)
	);
	let wrong = with_control_len_for_test(&f.state, 242);
	assert_eq!(
		governance_failure2(&wrong, &f.pin, &c, Some(&b), digest),
		Err(VerifyError2::Governance(GovernanceCommandError2::Magic))
	);
	b = f.envelope(&f.state, &c, 0x93);
	assert_eq!(
		governance_failure2(&wrong, &f.pin, &c, Some(&b), digest),
		Err(VerifyError2::Install(InstallError2::Commitment(
			CommitmentField2::ControlLength
		)))
	);
	// Structural zero-field order before all context/digest/signature/install differences.
	let mut b = f.envelope(&f.state, &c, 0x93);
	b[12..28].fill(0);
	b[28..44].fill(0);
	b[200] ^= 1;
	assert_eq!(
		governance_failure2(&wrong, &f.pin, &c, Some(&b), digest),
		Err(VerifyError2::Governance(
			GovernanceCommandError2::ZeroField(GovernanceField2::Generation)
		))
	);
	assert_eq!(
		governance_failure2(&empty_authority2(), &f.pin, &c, None, digest),
		Err(VerifyError2::StateKind)
	);
}
#[test]
#[cfg(target_endian = "little")]
fn s3_private_issuer_causes_require_the_changed_digest_envelope() {
	let mut f = Fixture::primary();
	let c = grant(&f, 0x34);
	let side = f.envelope(&f.state, &c, GOVERNANCE_SEED);
	assert_eq!(
		failure(&f, &c, Some(&side)),
		Ok(Some(Denial2::IssuerMissing))
	);
	let bad_envelope = f.envelope(&f.state, &c, 0x93);
	assert_eq!(
		failure(&f, &c, Some(&bad_envelope)),
		Ok(Some(Denial2::Envelope(GovernanceCommandError2::Signature)))
	);
	f.governed(Command2::EnrollLeaseIssuer {
		node: 7,
		key: key(ISSUER_SEED),
	});
	// This canonical wrong signer is a Signature error, not malformed enrolled-key error.
	let c = grant(&f, 0x34);
	let side = f.envelope(&f.state, &c, GOVERNANCE_SEED);
	assert_eq!(
		failure(&f, &c, Some(&side)),
		Ok(Some(Denial2::IssuerSignature))
	);
	let mut c = grant(&f, ISSUER_SEED);
	let Command2::Grant { lease, .. } = &mut c else {
		unreachable!()
	};
	lease.expiry.physical_ms += 1;
	let side = f.envelope(&f.state, &c, GOVERNANCE_SEED);
	assert_eq!(
		failure(&f, &c, Some(&side)),
		Ok(Some(Denial2::IssuerSignature))
	);
	let mut c = grant(&f, ISSUER_SEED);
	let Command2::Grant { lease, .. } = &mut c else {
		unreachable!()
	};
	lease.signature[32..].fill(0xff);
	let side = f.envelope(&f.state, &c, GOVERNANCE_SEED);
	assert_eq!(
		failure(&f, &c, Some(&side)),
		Ok(Some(Denial2::IssuerSignature))
	);
	f.governed(Command2::RevokeLeaseIssuer { node: 7 });
	let side = f.envelope(&f.state, &c, GOVERNANCE_SEED);
	assert_eq!(
		failure(&f, &c, Some(&side)),
		Ok(Some(Denial2::IssuerRevoked))
	);
	// Missing/revoked issuer precedes payload endian and key/signature checks. LE execution
	// cannot demonstrate the Big-endian IssuerPayload branch; the guard pins its source order.
}
#[test]
#[cfg(target_endian = "little")]
fn s3_private_invalid_issuer_key_is_primitive_only_unreachable_from_valid_registry() {
	let f = Fixture::primary();
	let lease = signed_lease(&f, ISSUER_SEED);
	assert_eq!(
		verify_issuer_signature2(&[2; 32], &lease),
		Err(LeasePayloadError2::Key)
	);
	// IssuerKey is retained defensively, but enrolled records reject this malformed key:
	// no authenticated S3 producer-path reachability is claimed for Denial2::IssuerKey.
}
