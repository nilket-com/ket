//! M1 dormancy at the commitment boundary: a root whose Bootstrap committed `length = 189` (built
//! here with a FIXTURE token; no production producer exists) is refused by every unchanged
//! consumer: install, observe, the issuer gate and C's Bootstrap verification.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, admin_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use crate::storage::mutation_visibility::root2::verification::*;

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
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
fn manifest() -> AdminManifest2 {
	AdminManifest2::decode(&sv("manifest_prime")).unwrap()
}
fn pin() -> Digest2 {
	admin_manifest_digest2(&manifest().encode().unwrap())
}
fn tuple() -> PinnedBoundTuple2 {
	let bytes = manifest().encode().unwrap();
	let m = manifest();
	let record = BootstrapRecord2::decode(&sv("record")).unwrap();
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: m.generation,
				cluster: m.cluster,
				pin: pin(),
				admin_key: m.admin_key,
			},
			manifest: m,
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
		manifest_pin: Some(&pin()),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.unwrap()
}
fn pre_root() -> MutationAuthorityState2 {
	let first = CommittedEntry2::fixture(
		at(0, 0, 0),
		sv("genesis").try_into().unwrap(),
		EntryClass2::Membership,
	);
	let s = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	let one = CommittedEntry2::fixture(at(1, 1, 1), [1; 32], EntryClass2::NonRoot2);
	advance_pre_root2(&s, &one).unwrap().0
}
fn bootstrap(length: u64) -> Command2 {
	Command2::Bootstrap {
		epoch: manifest().initial_epoch,
		control: pin(),
		length,
	}
}
/// FIXTURE: root the pre-root state with a Bootstrap of `length` and an always-authorized token.
fn rooted(length: u64) -> MutationAuthorityState2 {
	let m = manifest();
	let cmd = bootstrap(length);
	let e = TrustedEntry2::fixture(m.generation, m.cluster, l(2), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let v = VerifiedCommand2::fixture(m.generation, m.cluster, &cmd, &[], 0).unwrap();
	let ce = CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d));
	bootstrap2(&pre_root(), &ce, &e, &v, &cmd).unwrap().0
}

#[test]
fn install_and_observe_refuse_a_189_byte_commitment() {
	let s = rooted(189);
	assert_eq!(
		install_admin2(&s, &tuple()).err(),
		Some(InstallError2::Commitment(CommitmentField2::ControlLength))
	);
	assert_eq!(
		observe_bootstrap2(&s).err(),
		Some(ObserveError2::ControlLength)
	);
}

#[test]
fn the_issuer_gate_refuses_a_189_byte_commitment() {
	// The administrator installed from the coherent 137-byte root, presented against the
	// 189-byte root of the same generation and cluster: the gate's expected-context check.
	let admin = install_admin2(&rooted(137), &tuple()).unwrap();
	let s = rooted(189);
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	let grant: [u8; 337] = unhex(v["grant"]["bytes"].as_str().unwrap())
		.try_into()
		.unwrap();
	let cmd = Command2::AdmissionPermission(grant);
	let m = manifest();
	let e = TrustedEntry2::fixture(m.generation, m.cluster, l(3), &cmd).unwrap();
	let token = VerifiedCommand2::fixture(m.generation, m.cluster, &cmd, &[], 0).unwrap();
	assert_eq!(
		Root2::reduce_admission(&s, &admin, &e, &token, &cmd).err(),
		Some(RootError2::TrustMismatch)
	);
}

#[test]
fn c_bootstrap_verification_refuses_a_189_byte_length() {
	let m = manifest();
	let cmd = bootstrap(189);
	let e = TrustedEntry2::fixture(m.generation, m.cluster, l(2), &cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let ce = CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d));
	assert_eq!(
		advance_verified2(&pre_root(), &tuple(), &ce, &e, &cmd).err(),
		Some(VerifyError2::Consumer(RootError2::TrustMismatch))
	);
}
