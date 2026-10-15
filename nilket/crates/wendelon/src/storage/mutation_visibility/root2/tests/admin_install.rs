use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AdminManifest2;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
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

const G: [u8; 16] = [0xa1; 16];
const C: [u8; 16] = [0xc2; 16];

fn sha(b: &[u8]) -> Digest2 {
	crate::storage::mutation_visibility::root2::codec::hash(b"", b)
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn v() -> serde_json::Value {
	serde_json::from_str(include_str!("install_vectors.json")).unwrap()
}
fn digest(s: &serde_json::Value) -> Digest2 {
	unhex(s.as_str().unwrap()).try_into().unwrap()
}
fn m1() -> (Vec<u8>, Digest2) {
	let a: serde_json::Value =
		serde_json::from_str(include_str!("../provisioning/tests/anchor_vectors.json")).unwrap();
	let m = unhex(a["manifests"]["m1"]["bytes"].as_str().unwrap());
	(m, digest(&a["manifests"]["m1"]["pin"]))
}
fn canonical_genesis() -> Digest2 {
	digest(&v()["genesis"])
}
fn vector_record() -> BootstrapRecord2 {
	BootstrapRecord2::decode(&unhex(v()["record"].as_str().unwrap())).unwrap()
}
fn at(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}
struct Boot {
	g: [u8; 16],
	c: [u8; 16],
	epoch: u64,
	control: Digest2,
	length: u64,
	genesis: Digest2,
	flip_fingerprint: bool,
}
impl Boot {
	fn standard() -> Self {
		Self {
			g: G,
			c: C,
			epoch: 7,
			control: m1().1,
			length: 137,
			genesis: canonical_genesis(),
			flip_fingerprint: false,
		}
	}
	fn pre_root(&self) -> MutationAuthorityState2 {
		let genesis = CommittedEntry2::fixture(at(0, 0, 0), self.genesis, EntryClass2::Membership);
		let s = advance_pre_root2(&empty_authority2(), &genesis).unwrap().0;
		let scanned = CommittedEntry2::fixture(at(1, 1, 1), [1; 32], EntryClass2::NonRoot2);
		advance_pre_root2(&s, &scanned).unwrap().0
	}
	// Genesis, one scanned entry, then the Bootstrap at (1, 1, 2), through the existing fixtures.
	fn rooted(&self) -> MutationAuthorityState2 {
		let cmd = Command2::Bootstrap {
			epoch: self.epoch,
			control: self.control,
			length: self.length,
		};
		let mut e = TrustedEntry2::fixture(self.g, self.c, l(2), &cmd).unwrap();
		if self.flip_fingerprint {
			e.change_fingerprint();
		}
		let (_, _, _, fp, d) = e.fields();
		let verified = VerifiedCommand2::fixture(self.g, self.c, &cmd, &[], 0).unwrap();
		let ce = CommittedEntry2::fixture(at(1, 1, 2), fp, EntryClass2::Bootstrap(d));
		bootstrap2(&self.pre_root(), &ce, &e, &verified, &cmd)
			.unwrap()
			.0
	}
}
// A coherent Part A tuple for m1 whose Bound record is `record`; Part A must accept it.
fn tuple(record: BootstrapRecord2) -> (PinnedBoundTuple2, Vec<u8>) {
	let (m, pin) = m1();
	let manifest = AdminManifest2::decode(&m).unwrap();
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: manifest.generation,
				cluster: manifest.cluster,
				pin,
				admin_key: manifest.admin_key,
			},
			manifest: AnyAdminManifest2::One(manifest),
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	let inventory_pin = sha(&snapshot);
	let t = validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&m),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.expect("Part A accepts the coherent tuple");
	(t, snapshot)
}

#[test]
fn installs_from_the_original_bootstrap_and_matches_the_vectors() {
	let x = v();
	let (t, snapshot) = tuple(vector_record());
	// The generator built the same snapshot bytes independently.
	assert_eq!(snapshot, unhex(x["snapshot"].as_str().unwrap()));
	assert_eq!(sha(&snapshot), digest(&x["inventory_pin"]));
	let state = Boot::standard().rooted();
	let (_, origin) = root_and_origin2(&state).unwrap();
	assert_eq!(origin.genesis, digest(&x["genesis"]));
	assert_eq!(origin.bootstrap_command, digest(&x["bootstrap_command"]));
	assert_eq!(
		origin.bootstrap_fingerprint,
		digest(&x["bootstrap_fingerprint_stand_in"])
	);
	let a = install_admin2(&state, &t).unwrap();
	let ctx = admin_context2(&a);
	let want = &x["context"];
	assert_eq!(
		ctx.generation.to_vec(),
		unhex(want["generation"].as_str().unwrap())
	);
	assert_eq!(
		ctx.cluster.to_vec(),
		unhex(want["cluster"].as_str().unwrap())
	);
	assert_eq!(ctx.installed_at, l(2));
	assert_eq!(admin_installed_at2(&a), l(2));
	assert_eq!(
		ctx.bootstrap_fingerprint,
		digest(&want["bootstrap_fingerprint"])
	);
	assert_eq!(ctx.bootstrap_command, digest(&want["bootstrap_command"]));
	assert_eq!(ctx.genesis, digest(&want["genesis"]));
	assert_eq!(ctx.initial_epoch, want["initial_epoch"].as_u64().unwrap());
	assert_eq!(ctx.manifest_pin, digest(&want["manifest_pin"]));
	assert_eq!(
		admin_id2(&a).to_vec(),
		unhex(x["admin_id"].as_str().unwrap())
	);
	assert_eq!(
		admin_key2(&a).to_vec(),
		unhex(x["admin_key"].as_str().unwrap())
	);
	assert!(admin_namespace2(&a) == AdminManifest2::decode(&m1().0).unwrap().namespace);
	assert!(install_admin2(&state, &t).unwrap() == a);
	assert_eq!(format!("{a:?}"), "InstalledAdmin2 { redacted }");
	assert_eq!(format!("{ctx:?}"), "AdminContext2 { redacted }");
	assert_eq!(format!("{origin:?}"), "OriginFacts2 { redacted }");
}
#[test]
fn later_entries_move_the_tip_but_not_the_installed_bootstrap() {
	let (t, _) = tuple(vector_record());
	let mut state = Boot::standard().rooted();
	let before = install_admin2(&state, &t).unwrap();
	let tip = |s: &MutationAuthorityState2| {
		let (root, _) = root_and_origin2(s).unwrap();
		(
			root.state.covered,
			root.state.fingerprint,
			root.state.command_digest,
		)
	};
	let original_tip = tip(&state);
	for (i, authorized) in [(3u64, true), (4, false)] {
		let cmd = Command2::Cover {
			kind: 0,
			external: [i as u8; 32],
		};
		let e = TrustedEntry2::fixture(G, C, l(i), &cmd).unwrap();
		let mut verified = VerifiedCommand2::fixture(G, C, &cmd, &[], 0).unwrap();
		if !authorized {
			verified = verified.unauthorized();
		}
		let (next, result) = advance_root2(&state, &e, &verified, &cmd).unwrap();
		assert_eq!(matches!(result, ApplyResult2::Covered), authorized);
		state = next;
		assert!(tip(&state) != original_tip, "the tip advanced at {i}");
		let a = install_admin2(&state, &t).unwrap();
		assert!(a == before, "installed view unchanged after entry {i}");
		assert_eq!(admin_installed_at2(&a), l(2));
	}
}
#[test]
fn each_refusal_is_reached_with_every_earlier_check_equal() {
	let (t, _) = tuple(vector_record());
	let refuse = |boot: Boot| install_admin2(&boot.rooted(), &t).err();
	assert_eq!(
		install_admin2(&empty_authority2(), &t).err(),
		Some(InstallError2::PrerequisiteUnavailable)
	);
	assert_eq!(
		install_admin2(&Boot::standard().pre_root(), &t).err(),
		Some(InstallError2::PrerequisiteUnavailable)
	);
	assert_eq!(
		refuse(Boot {
			control: [0x9f; 32],
			..Boot::standard()
		}),
		Some(InstallError2::Commitment(CommitmentField2::ControlDigest))
	);
	assert_eq!(
		refuse(Boot {
			length: 189,
			..Boot::standard()
		}),
		Some(InstallError2::Commitment(CommitmentField2::ControlLength))
	);
	assert_eq!(
		refuse(Boot {
			g: [0xa3; 16],
			..Boot::standard()
		}),
		Some(InstallError2::Record(InstallField2::Generation))
	);
	assert_eq!(
		refuse(Boot {
			c: [0xc3; 16],
			..Boot::standard()
		}),
		Some(InstallError2::Record(InstallField2::Cluster))
	);
	assert_eq!(
		refuse(Boot {
			flip_fingerprint: true,
			..Boot::standard()
		}),
		Some(InstallError2::Record(InstallField2::BootstrapFingerprint))
	);
	assert_eq!(
		refuse(Boot {
			genesis: [0x77; 32],
			..Boot::standard()
		}),
		Some(InstallError2::Record(InstallField2::Genesis))
	);
	// Record-side mismatches through coherent re-pinned tuples against the standard root.
	let state = Boot::standard().rooted();
	let mut other_log = vector_record();
	other_log.identity.bootstrap = l(3);
	assert_eq!(
		install_admin2(&state, &tuple(other_log).0).err(),
		Some(InstallError2::Record(InstallField2::Bootstrap))
	);
	let mut other_command = vector_record();
	other_command.identity.bootstrap_command[0] ^= 1;
	assert_eq!(
		install_admin2(&state, &tuple(other_command).0).err(),
		Some(InstallError2::Record(InstallField2::BootstrapCommand))
	);
	// Epoch: a root whose Bootstrap epoch differs, with the record's Bootstrap identity aligned to
	// that root so every earlier check is equal; Part A keeps record and manifest epochs bound.
	let epoch_root = Boot {
		epoch: 8,
		..Boot::standard()
	}
	.rooted();
	let (_, origin) = root_and_origin2(&epoch_root).unwrap();
	let mut aligned = vector_record();
	aligned.identity.bootstrap_fingerprint = origin.bootstrap_fingerprint;
	aligned.identity.bootstrap_command = origin.bootstrap_command;
	assert_eq!(
		install_admin2(&epoch_root, &tuple(aligned).0).err(),
		Some(InstallError2::Record(InstallField2::InitialEpoch))
	);
	// Record(Pin) is unreachable: Part A binds the record pin to the manifest pin, and the
	// commitment rule binds that pin to the root's control digest.
}
