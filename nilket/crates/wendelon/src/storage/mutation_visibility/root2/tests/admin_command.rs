//! The pure `VISADC01` administrator command envelope check. The fixture administrator is
//! manifest_prime with a fixed TEST seed key; every fixture byte (manifest, pin, Bootstrap digest
//! and stand-in fingerprint, context) and every envelope is compared against the independent
//! Python reference `admin_command_reference.py`. FIXTURE: the root is built with
//! `VerifiedCommand2::fixture`; nothing here is a production authorization path.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, admin_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_command::*;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use ed25519_dalek::Signer;

const SEED: [u8; 32] = [0x5a; 32];

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn sv(field: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	unhex(v[field].as_str().unwrap())
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("admin_command_vectors.json")).unwrap()
}
fn vv(field: &str) -> Vec<u8> {
	unhex(vectors()[field].as_str().unwrap())
}
fn d32(v: Vec<u8>) -> Digest2 {
	v.try_into().unwrap()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
fn manifest() -> AdminManifest2 {
	let mut m = AdminManifest2::decode(&sv("manifest_prime")).unwrap();
	m.admin_key = ed25519_dalek::SigningKey::from_bytes(&SEED)
		.verifying_key()
		.to_bytes();
	m
}
fn manifest_bytes() -> [u8; 137] {
	manifest().encode().unwrap()
}
fn bootstrap() -> Command2 {
	Command2::Bootstrap {
		epoch: manifest().initial_epoch,
		control: admin_manifest_digest2(&manifest_bytes()),
		length: 137,
	}
}
/// FIXTURE: genesis, one scanned entry, then the Bootstrap at (1,1,2) with a fixture token.
fn rooted() -> MutationAuthorityState2 {
	let m = manifest();
	let first = CommittedEntry2::fixture(at(0, 0, 0), d32(sv("genesis")), EntryClass2::Membership);
	let mut s = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	let one = CommittedEntry2::fixture(at(1, 1, 1), [1; 32], EntryClass2::NonRoot2);
	s = advance_pre_root2(&s, &one).unwrap().0;
	let cmd = bootstrap();
	let e = TrustedEntry2::fixture(m.generation, m.cluster, l(2), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let v = VerifiedCommand2::fixture(m.generation, m.cluster, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d));
	bootstrap2(&s, &ce, &e, &v, &cmd).unwrap().0
}
fn tuple() -> PinnedBoundTuple2 {
	let bytes = manifest_bytes();
	let m = manifest();
	let pin = admin_manifest_digest2(&bytes);
	let m0 = manifest();
	let e = TrustedEntry2::fixture(m0.generation, m0.cluster, l(2), &bootstrap()).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let mut record = BootstrapRecord2::decode(&sv("record")).unwrap();
	record.identity.pin = pin;
	record.identity.bootstrap_fingerprint = fp;
	record.identity.bootstrap_command = d;
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: m.generation,
				cluster: m.cluster,
				pin,
				admin_key: m.admin_key,
			},
			manifest: AnyAdminManifest2::One(m),
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	let inventory_pin = crate::storage::mutation_visibility::root2::codec::hash(b"", &snapshot);
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.unwrap()
}
fn admin() -> InstalledAdmin2 {
	install_admin2(&rooted(), &tuple()).unwrap()
}
fn context() -> AdminContext2 {
	admin_context2(&admin())
}
fn valid() -> Vec<u8> {
	vv("valid")
}
fn d1() -> Digest2 {
	d32(vv("digest_1"))
}
fn check(bytes: &[u8], digest: Digest2) -> Result<(), EnvelopeError2> {
	check_admin_command2(&admin(), bytes, digest).map(|_| ())
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
fn fixture_matches_independent_reference() {
	let v = vectors();
	assert_eq!(manifest_bytes().to_vec(), vv("manifest"));
	assert_eq!(manifest().admin_key.to_vec(), vv("admin_public"));
	assert_eq!(
		admin_manifest_digest2(&manifest_bytes()).to_vec(),
		vv("pin")
	);
	let m = manifest();
	assert_eq!(
		bootstrap().payload(m.generation, m.cluster).unwrap(),
		vv("bootstrap_payload")
	);
	let e = TrustedEntry2::fixture(m.generation, m.cluster, l(2), &bootstrap()).unwrap();
	let (_, _, _, fp, d) = e.fields();
	assert_eq!(d.to_vec(), vv("bootstrap_command"));
	assert_eq!(fp.to_vec(), vv("bootstrap_fingerprint"));
	let c = context();
	let x = &v["context"];
	let h = |k: &str| unhex(x[k].as_str().unwrap());
	assert_eq!(c.generation.to_vec(), h("generation"));
	assert_eq!(c.cluster.to_vec(), h("cluster"));
	assert_eq!(
		(
			c.installed_at.term,
			c.installed_at.node as u64,
			c.installed_at.index
		),
		(
			x["term"].as_u64().unwrap(),
			x["node"].as_u64().unwrap(),
			x["index"].as_u64().unwrap()
		)
	);
	assert_eq!(c.bootstrap_fingerprint.to_vec(), h("fingerprint"));
	assert_eq!(c.bootstrap_command.to_vec(), h("command"));
	assert_eq!(c.genesis.to_vec(), h("genesis"));
	assert_eq!(c.initial_epoch, x["epoch"].as_u64().unwrap());
	assert_eq!(c.manifest_pin.to_vec(), h("pin"));
}

#[test]
fn valid_envelope_checks_and_round_trips() {
	let checked = check_admin_command2(&admin(), &valid(), d1()).unwrap();
	assert!(checked.context() == context());
	assert_eq!(checked.command_digest(), d1());
	let signature: [u8; 64] = valid()[232..].try_into().unwrap();
	assert_eq!(
		encode_admin_command2(&context(), d1(), &signature)
			.unwrap()
			.to_vec(),
		valid()
	);
	let fixed: [u8; 296] = valid().try_into().unwrap();
	assert_eq!(admin_command_preimage2(&fixed).unwrap(), vv("preimage"));
}

#[test]
fn structural_errors_each_and_in_order() {
	use EnvelopeError2 as E;
	use EnvelopeField2 as F;
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
	let ctx_changed = unhex(vectors()["context_variants"]["genesis"].as_str().unwrap());
	assert_eq!(check(&ctx_changed, d32(vv("digest_2"))), Err(E::Context));
	let mut bad_sig = v.clone();
	bad_sig[240] ^= 1;
	assert_eq!(check(&bad_sig, d32(vv("digest_2"))), Err(E::Digest));
	// The preimage also requires structure.
	let fixed: [u8; 296] = with(&[(0, b'X')]).try_into().unwrap();
	assert_eq!(admin_command_preimage2(&fixed).err(), Some(E::Magic));
}

/// Every context field, the full installed_at Log2 term, node and index separately included, in
/// a coherently re-signed envelope, refuses Context (not Signature).
#[test]
fn every_context_field_is_bound() {
	let variants = vectors()["context_variants"].clone();
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
		assert_eq!(check(&b, d1()), Err(EnvelopeError2::Context), "{f}");
	}
}

#[test]
fn digest_must_equal_the_supplied_one() {
	assert_eq!(
		check(&valid(), d32(vv("digest_2"))),
		Err(EnvelopeError2::Digest)
	);
}

#[test]
fn signature_failures_reach_the_signature_check() {
	use EnvelopeError2 as E;
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
			&signed_with(&v, [0x5b; 32], b"VISROOT2-ADMIN-COMMAND\0"),
			d1()
		),
		Err(E::Signature)
	);
	// The same bytes signed by the administrator under the statement domain.
	let cross = vv("statement_domain_signature");
	assert_eq!(cross[..232], v[..232]);
	assert_eq!(check(&cross, d1()), Err(E::Signature));
	// A non-canonical S (S + L): rejected by the strict verification.
	let noncanonical = vv("noncanonical_s");
	assert_eq!(noncanonical[..264], v[..264]);
	assert_eq!(check(&noncanonical, d1()), Err(E::Signature));
	// Sanity: the in-test re-signing reproduces the reference envelope byte for byte.
	assert_eq!(signed_with(&v, SEED, b"VISROOT2-ADMIN-COMMAND\0"), v);
}

#[test]
fn encode_refuses_invalid_context() {
	use EnvelopeError2 as E;
	use EnvelopeField2 as F;
	let sig = [7u8; 64];
	let mut c = context();
	c.generation = [0; 16];
	assert_eq!(
		encode_admin_command2(&c, d1(), &sig).err(),
		Some(E::ZeroField(F::Generation))
	);
	let mut c = context();
	c.installed_at.index = 0;
	assert_eq!(
		encode_admin_command2(&c, d1(), &sig).err(),
		Some(E::ZeroField(F::InstalledAt))
	);
	let mut c = context();
	c.initial_epoch = 0;
	assert_eq!(
		encode_admin_command2(&c, d1(), &sig).err(),
		Some(E::ZeroField(F::Epoch))
	);
}
