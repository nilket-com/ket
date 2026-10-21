//! Independent governance fixtures on `M3_PRIMARY` (the `p3_` half of governance_vectors.json);
//! no production authority construction.
use super::profile_three_fixture::{M3_PRIMARY, PRIMARY_SEEDS, chain_of};
use super::*;
use crate::storage::mutation_visibility::root2::admin_command::{
	EnvelopeError2, admin_command_preimage2, check_admin_command2, encode_admin_command2,
};
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::governance_command::*;
use ed25519_dalek::Signer;
const SEED: [u8; 32] = [PRIMARY_SEEDS[1]; 32];
const ADMIN_SEED: [u8; 32] = [PRIMARY_SEEDS[0]; 32];
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("governance_vectors.json")).unwrap()
}
fn vv(field: &str) -> Vec<u8> {
	unhex(vectors()[field].as_str().unwrap())
}
fn primary() -> super::profile_three_fixture::Chain {
	chain_of(&M3_PRIMARY, 241)
}
fn d32(v: Vec<u8>) -> Digest2 {
	v.try_into().unwrap()
}
fn admin() -> InstalledAdmin2 {
	let c = primary();
	install_admin2(&c.rooted, &c.tuple).unwrap()
}
fn context() -> AdminContext2 {
	admin_context2(&admin())
}
fn valid() -> Vec<u8> {
	vv("p3_valid")
}
fn d1() -> Digest2 {
	d32(vv("digest_1"))
}
fn check(bytes: &[u8], digest: Digest2) -> Result<(), GovernanceCommandError2> {
	check_governance_command2(&admin().view(), GovernanceAction2::Grant, bytes, digest).map(|_| ())
}
#[test]
fn fixture_matches_independent_reference() {
	let chain = primary();
	assert_eq!(M3_PRIMARY.to_vec(), vv("p3_manifest"));
	let (g, c, _, fp, d) = chain.entry.fields();
	assert_eq!(chain.cmd.payload(g, c).unwrap(), vv("p3_bootstrap_payload"));
	assert_eq!(fp.to_vec(), vv("p3_bootstrap_fingerprint"));
	assert_eq!(d.to_vec(), vv("p3_bootstrap_command"));
	assert_eq!(
		admin_governance2(&admin()).unwrap().key().to_vec(),
		vv("p3_governance_public")
	);
	assert_eq!(context().manifest_pin.to_vec(), vv("p3_pin"));
}
/// Re-sign `body` (the first 232 bytes) with `seed` under `domain`.
fn signed_with(body: &[u8], seed: [u8; 32], domain: &[u8]) -> Vec<u8> {
	let key = ed25519_dalek::SigningKey::from_bytes(&seed);
	let mut preimage = domain.to_vec();
	preimage.extend_from_slice(&body[..232]);
	let mut out = body[..232].to_vec();
	out.extend_from_slice(&key.sign(&preimage).to_bytes());
	out
}

#[test]
fn valid_envelope_checks_and_round_trips() {
	let checked =
		check_governance_command2(&admin().view(), GovernanceAction2::Grant, &valid(), d1())
			.unwrap();
	assert!(checked.context() == context());
	assert_eq!(checked.action(), GovernanceAction2::Grant);
	assert_eq!(checked.command_digest(), d1());
	let signature: [u8; 64] = valid()[232..].try_into().unwrap();
	assert_eq!(
		encode_governance_command2(&context(), d1(), &signature)
			.unwrap()
			.to_vec(),
		valid()
	);
	let fixed: [u8; 296] = valid().try_into().unwrap();
	assert_eq!(
		governance_command_preimage2(&fixed).unwrap(),
		vv("p3_preimage")
	);
}

#[test]
fn structural_errors_each_and_in_order() {
	use GovernanceCommandError2 as E;
	use GovernanceField2 as F;
	let v = valid();
	let with = |edits: &[(usize, u8)]| {
		let mut b = v.clone();
		for &(i, x) in edits {
			b[i] = x;
		}
		b
	};
	// Length: short, long, trailing byte.
	assert_eq!(check(&v[..295], d1()), Err(E::Length));
	let mut long = v.clone();
	long.push(0);
	assert_eq!(check(&long, d1()), Err(E::Length));
	assert_eq!(check(&[], d1()), Err(E::Length));
	// Magic, version, each reserved byte.
	assert_eq!(check(&with(&[(0, b'X')]), d1()), Err(E::Magic));
	assert_eq!(check(&with(&[(8, 2)]), d1()), Err(E::Version));
	assert_eq!(check(&with(&[(10, 1)]), d1()), Err(E::Reserved));
	assert_eq!(check(&with(&[(11, 1)]), d1()), Err(E::Reserved));
	// Zero fields, each alone.
	let zero = |range: std::ops::Range<usize>| {
		let mut b = v.clone();
		b[range].fill(0);
		b
	};
	assert_eq!(check(&zero(12..28), d1()), Err(E::ZeroField(F::Generation)));
	assert_eq!(check(&zero(28..44), d1()), Err(E::ZeroField(F::Cluster)));
	assert_eq!(check(&zero(160..168), d1()), Err(E::ZeroField(F::Epoch)));
	for part in [44..52, 52..56, 56..64] {
		assert_eq!(check(&zero(part), d1()), Err(E::ZeroField(F::InstalledAt)));
	}
	// Precedence pairs.
	assert_eq!(check(&with(&[(0, b'X'), (8, 2)]), d1()), Err(E::Magic));
	assert_eq!(check(&with(&[(8, 2), (10, 1)]), d1()), Err(E::Version));
	let mut b = zero(12..28);
	b[10] = 1;
	assert_eq!(check(&b, d1()), Err(E::Reserved));
	let mut b = zero(12..28);
	b[28..44].fill(0);
	assert_eq!(check(&b, d1()), Err(E::ZeroField(F::Generation)));
	let mut b = zero(160..168);
	b[56..64].fill(0);
	assert_eq!(check(&b, d1()), Err(E::ZeroField(F::Epoch)));
	// A zero field precedes a context mismatch, which precedes a digest mismatch, which precedes
	// a bad signature.
	let mut b = zero(56..64);
	b[64] ^= 1;
	assert_eq!(check(&b, d1()), Err(E::ZeroField(F::InstalledAt)));
	let ctx_changed = unhex(
		vectors()["p3_context_variants"]["genesis"]
			.as_str()
			.unwrap(),
	);
	assert_eq!(check(&ctx_changed, d32(vv("digest_2"))), Err(E::Context));
	let mut bad_sig = v.clone();
	bad_sig[240] ^= 1;
	assert_eq!(check(&bad_sig, d32(vv("digest_2"))), Err(E::Digest));
	// The preimage also requires structure.
	let fixed: [u8; 296] = with(&[(0, b'X')]).try_into().unwrap();
	assert_eq!(governance_command_preimage2(&fixed).err(), Some(E::Magic));
}

/// Every context field, the full installed_at Log2 term, node and index separately included, in
/// a coherently re-signed envelope, refuses Context (not Signature).
#[test]
fn every_context_field_is_bound() {
	let variants = vectors()["p3_context_variants"].clone();
	let fields = [
		"generation",
		"cluster",
		"term",
		"node",
		"index",
		"fingerprint",
		"command",
		"genesis",
		"epoch",
		"pin",
	];
	assert_eq!(variants.as_object().unwrap().len(), fields.len());
	for f in fields {
		let b = unhex(variants[f].as_str().unwrap());
		assert_eq!(
			check(&b, d1()),
			Err(GovernanceCommandError2::Context),
			"{f}"
		);
	}
}

#[test]
fn digest_must_equal_the_supplied_one() {
	assert_eq!(
		check(&valid(), d32(vv("digest_2"))),
		Err(GovernanceCommandError2::Digest)
	);
}

#[test]
fn signature_failures_reach_the_signature_check() {
	use GovernanceCommandError2 as E;
	let v = valid();
	// A changed signed byte inside the embedded digest, with that digest supplied.
	let mut b = v.clone();
	b[205] ^= 1;
	let tampered: Digest2 = b[200..232].try_into().unwrap();
	assert_eq!(check(&b, tampered), Err(E::Signature));
	// A changed signature byte.
	let mut b = v.clone();
	b[232] ^= 1;
	assert_eq!(check(&b, d1()), Err(E::Signature));
	// The same bytes signed by another key.
	assert_eq!(
		check(
			&signed_with(&v, [0x5b; 32], b"VISROOT2-GOVERNANCE-COMMAND\0"),
			d1()
		),
		Err(E::Signature)
	);
	// The same bytes signed by the administrator under the statement domain.
	let cross = vv("p3_statement_domain_signature");
	assert_eq!(cross[..232], v[..232]);
	assert_eq!(check(&cross, d1()), Err(E::Signature));
	// A non-canonical S (S + L): rejected by the strict verification.
	let noncanonical = vv("p3_noncanonical_s");
	assert_eq!(noncanonical[..264], v[..264]);
	assert_eq!(check(&noncanonical, d1()), Err(E::Signature));
	// Sanity: the in-test re-signing reproduces the reference envelope byte for byte.
	assert_eq!(signed_with(&v, SEED, b"VISROOT2-GOVERNANCE-COMMAND\0"), v);
}

#[test]
fn encode_refuses_invalid_context() {
	use GovernanceCommandError2 as E;
	use GovernanceField2 as F;
	let sig = [7u8; 64];
	let mut c = context();
	c.generation = [0; 16];
	assert_eq!(
		encode_governance_command2(&c, d1(), &sig).err(),
		Some(E::ZeroField(F::Generation))
	);
	let mut c = context();
	c.installed_at.index = 0;
	assert_eq!(
		encode_governance_command2(&c, d1(), &sig).err(),
		Some(E::ZeroField(F::InstalledAt))
	);
	let mut c = context();
	c.initial_epoch = 0;
	assert_eq!(
		encode_governance_command2(&c, d1(), &sig).err(),
		Some(E::ZeroField(F::Epoch))
	);
}
#[test]
fn purpose_separation_reaches_signature_and_preserves_admin_checker() {
	let body = valid();
	let mut adc = encode_admin_command2(&context(), d1(), &[0; 64]).unwrap();
	let preimage = admin_command_preimage2(&adc).unwrap();
	adc[232..].copy_from_slice(
		&ed25519_dalek::SigningKey::from_bytes(&ADMIN_SEED)
			.sign(&preimage)
			.to_bytes(),
	);
	assert!(check_admin_command2(&admin(), &adc, d1()).is_ok());
	adc[..8].copy_from_slice(b"VISADG01");
	assert_eq!(check(&adc, d1()), Err(GovernanceCommandError2::Signature));
	assert_eq!(
		check(&signed_with(&body, SEED, b"VISROOT2-ADMIN-COMMAND\0"), d1()),
		Err(GovernanceCommandError2::Signature)
	);
	assert_eq!(
		check(
			&signed_with(&body, ADMIN_SEED, b"VISROOT2-GOVERNANCE-COMMAND\0"),
			d1()
		),
		Err(GovernanceCommandError2::Signature)
	);
	assert_eq!(
		check_admin_command2(&admin(), &body, d1()).err(),
		Some(EnvelopeError2::Magic)
	);
}
#[test]
fn action_table_matches_current_command_tags_exhaustively() {
	use GovernanceAction2 as A;
	fn action(c: &Command2) -> Option<A> {
		match c {
			Command2::EnrollSelector(_) => Some(A::EnrollSelector),
			Command2::EnrollAuthority { .. } => Some(A::EnrollAuthority),
			Command2::RevokeAuthority(_) => Some(A::RevokeAuthority),
			Command2::EnrollLeaseIssuer { .. } => Some(A::EnrollLeaseIssuer),
			Command2::RevokeLeaseIssuer { .. } => Some(A::RevokeLeaseIssuer),
			Command2::Grant { .. } => Some(A::Grant),
			Command2::Release { .. } => Some(A::Release),
			Command2::Cut { .. } => Some(A::Cut),
			Command2::Resolve { .. } => Some(A::Resolve),
			Command2::Bootstrap { .. }
			| Command2::Cover { .. }
			| Command2::Reserve(_)
			| Command2::Finalize { .. }
			| Command2::AdmissionPermission(_)
			| Command2::RotateAdmin(_)
			| Command2::RotateGovernance(_)
			| Command2::RotateRecovery(_)
			| Command2::EpochAdmissionPermission(_)
			| Command2::Freeze(_)
			| Command2::Unfreeze(_)
			| Command2::RetireRoot => None,
		}
	}
	let v: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	let mut seen = std::collections::BTreeSet::new();
	for f in v["fixtures"].as_array().unwrap() {
		if !f["name"].as_str().unwrap().starts_with("command_") {
			continue;
		}
		let (g, c, cmd) = Command2::decode(&unhex(f["hex"].as_str().unwrap())).unwrap();
		if let Some(a) = action(&cmd) {
			assert_eq!(cmd.payload(g, c).unwrap()[32], a.command_tag());
			seen.insert(a.command_tag());
		}
	}
	assert_eq!(
		seen.into_iter().collect::<Vec<_>>(),
		[2, 3, 4, 5, 6, 7, 10, 12, 13]
	);
	assert_eq!(
		A::ALL.map(|a| a.command_tag()),
		[2, 3, 4, 5, 6, 7, 10, 12, 13]
	);
	assert_eq!(A::ALL.map(|a| a.bit()), [1, 2, 4, 8, 16, 32, 64, 128, 256]);
	assert_eq!(A::ALL.iter().fold(0, |mask, a| mask | a.bit()), 0x1ff);
}
