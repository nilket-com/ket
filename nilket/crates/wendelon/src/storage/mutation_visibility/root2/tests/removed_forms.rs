//! The removed-forms corpus (`removed_forms.json`): the exact bytes of every admin form the
//! profile-3-only slice removes, each with today's outcome. The plan pins the file's digest; this
//! test proves every entry still behaves as recorded, so a later sub-slice can flip each one to its
//! typed refusal against an accepted twin.
use super::*;
use crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifestThree3, AnyAdminManifest2, ManifestProfile2,
};
use crate::storage::mutation_visibility::root2::compromise::compromise_audit2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::RootState2;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::{
	Classification2, decode_evidence2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_log::OwnerDir2;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::{
	ReplicaInputs2, validate_replica_tuple2,
};
use crate::storage::mutation_visibility::root2::provisioning::snapshot::decode_snapshot2;
use serde_json::Value;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

const CORPUS: &[u8] = include_bytes!("removed_forms.json");
/// SHA-256 of `removed_forms.json`, as recorded in the plan.
const CORPUS_SHA256: &str = "3244bfa6faa088ec63f8433c3faff0b1a8b31747d61fbd5f8dc3bab0884b9f89";

static NEXT: AtomicUsize = AtomicUsize::new(0);
// A fresh 0700 directory under the test temp root; removed on drop.
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("removed-forms-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
	fn log(&self, bytes: &[u8]) {
		let p = self.0.join("inventory.log");
		fs::write(&p, bytes).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

fn hex(b: &[u8]) -> String {
	b.iter().map(|x| format!("{x:02x}")).collect()
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn entries() -> Vec<Value> {
	let v: Value = serde_json::from_slice(CORPUS).unwrap();
	v["entries"].as_array().unwrap().clone()
}
fn bytes(e: &Value) -> Vec<u8> {
	unhex(e["hex"].as_str().unwrap())
}
fn of<'a>(all: &'a [Value], id: &str) -> Vec<&'a Value> {
	all.iter().filter(|e| e["id"] == id).collect()
}
fn one(all: &[Value], id: &str) -> Vec<u8> {
	let found = of(all, id);
	assert_eq!(found.len(), 1, "{id}");
	bytes(found[0])
}

#[test]
fn corpus_is_pinned_complete_and_self_consistent() {
	assert_eq!(hex(&hash(b"", CORPUS)), CORPUS_SHA256);
	let all = entries();
	let counts: Vec<(String, usize)> = (1..=15)
		.map(|i| format!("N{i}"))
		.map(|id| (id.clone(), of(&all, &id).len()))
		.collect();
	let expected = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 10, 3, 1];
	for ((id, n), want) in counts.iter().zip(expected) {
		assert_eq!(*n, want, "{id}");
	}
	assert_eq!(all.len(), expected.iter().sum::<usize>());
	for e in &all {
		assert_eq!(hex(&hash(b"", &bytes(e))), e["sha256"].as_str().unwrap());
		assert!(e["source"]["file"].is_string() && e["today"].is_string());
		assert!(matches!(e["flips_in"].as_str(), Some("c1" | "c2")));
	}
}

#[test]
fn removed_manifests_decode_today() {
	let all = entries();
	let n1 = one(&all, "N1");
	assert_eq!(n1.len(), 137);
	assert!(matches!(
		AnyAdminManifest2::decode(&n1),
		Ok(AnyAdminManifest2::One(_))
	));
	let n2 = one(&all, "N2");
	assert_eq!(n2.len(), 189);
	assert!(matches!(
		AnyAdminManifest2::decode(&n2),
		Ok(AnyAdminManifest2::Two(_))
	));
	let n3 = one(&all, "N3");
	assert_eq!(n3.len(), 241);
	assert_eq!(
		AdminManifestThree3::decode(&n3).err(),
		Some(AdminManifestError2::ProfileLength)
	);
}

#[test]
fn removed_tuple_and_snapshots_are_accepted_today() {
	let all = entries();
	let n4 = of(&all, "N4")[0];
	assert_eq!(n4["manifest"], "N1");
	let manifest = one(&all, "N1");
	let manifest_pin = hash(b"VISROOT2-ADMIN-MANIFEST\0", &manifest);
	let snapshot = bytes(n4);
	let inventory_pin = hash(b"", &snapshot);
	assert!(
		validate_replica_tuple2(&ReplicaInputs2 {
			manifest: Some(&manifest),
			manifest_pin: Some(&manifest_pin),
			snapshot: Some(&snapshot),
			inventory_pin: Some(&inventory_pin),
		})
		.is_ok()
	);
	for (id, version, count) in [
		("N5", 2u16, 3usize),
		("N6", 1, 1),
		("N7", 3, 1),
		("N8", 3, 3),
	] {
		let b = one(&all, id);
		assert_eq!(u16::from_le_bytes([b[8], b[9]]), version, "{id}");
		assert_eq!(decode_snapshot2(&b).unwrap().entries.len(), count, "{id}");
	}
	let n7 = decode_snapshot2(&one(&all, "N7")).unwrap();
	assert_eq!(n7.entries[0].state, RootState2::Retired);
	assert!(n7.entries[0].terminal.is_none() && n7.entries[0].record.is_none());
	let n8 = decode_snapshot2(&one(&all, "N8")).unwrap();
	assert_eq!(
		n8.entries.iter().map(|e| e.profile).collect::<Vec<_>>(),
		[
			ManifestProfile2::One,
			ManifestProfile2::Two,
			ManifestProfile2::Three
		]
	);
}

#[test]
fn removed_owner_forms_are_accepted_today() {
	let all = entries();
	// N9: VISOEV02 RESERVE events with n = 137 and n = 189 fold in a v2 directory.
	let n9 = one(&all, "N9");
	let n = |e: &[u8]| u16::from_le_bytes([e[132], e[133]]);
	assert_eq!([n(&n9[..512]), n(&n9[1024..])], [137, 189]);
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	s.log(&n9);
	assert_eq!(
		OwnerDir2::open(&s.0)
			.unwrap()
			.inspect()
			.unwrap()
			.classification,
		Classification2::Clean { last_seq: 3 }
	);
	// N10: a plain RETIRE (n = 0) of the profile-3 root folds and exports it Retired.
	let n10 = one(&all, "N10");
	assert_eq!(n(&n10[6 * 512..]), 0);
	let (s, out) = (Scratch::new(), Scratch::new());
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	s.log(&n10);
	let mut o = OwnerDir2::open(&s.0).unwrap();
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::Clean { last_seq: 7 }
	);
	o.export(&out.0, "retired").unwrap();
	let exported = decode_snapshot2(&fs::read(out.0.join("retired")).unwrap()).unwrap();
	assert_eq!(
		exported.entries.iter().map(|e| e.state).collect::<Vec<_>>(),
		[RootState2::Bound, RootState2::Bound, RootState2::Retired]
	);
	assert!(exported.entries[2].terminal.is_none());
	drop(o);
	// N11: a VISIEV02 log in an unmarked directory opens as v1.
	let s = Scratch::new();
	s.log(&one(&all, "N11"));
	let status = OwnerDir2::open(&s.0).unwrap().inspect().unwrap();
	assert!(!status.marker_present);
	assert_eq!(
		status.classification,
		Classification2::Clean { last_seq: 6 }
	);
	// N12: VISREP03 evidence decodes under its `.rep` name.
	for e in of(&all, "N12") {
		let b = bytes(e);
		assert_eq!((b.len(), &b[..8]), (256, &b"VISREP03"[..]));
		assert!(decode_evidence2(e["name"].as_str().unwrap(), &b).is_ok());
	}
}

#[test]
fn removed_roots_and_bootstraps_are_accepted_today() {
	let all = entries();
	for e in of(&all, "N13") {
		let b = bytes(e);
		assert!(UnverifiedRoot2::decode(&b).is_ok(), "{}", e["part"]);
		assert_eq!(
			u64::from_le_bytes(b[192..200].try_into().unwrap()),
			e["control_len"].as_u64().unwrap()
		);
	}
	for e in of(&all, "N14") {
		let (g, c, cmd) = Command2::decode(&bytes(e)).unwrap();
		let length = e["length"].as_u64().unwrap();
		assert!(matches!(cmd, Command2::Bootstrap { length: n, .. } if n == length));
		let entry = TrustedEntry2::fixture(g, c, l(1), &cmd).unwrap();
		let verified = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
		let (root, result) = Root2::reduce(None, &entry, &verified, &cmd).unwrap();
		assert_eq!(result, ApplyResult2::Registered(l(1)));
		assert_eq!(root.state.control_len, length);
	}
	// N15: the compromise audit refuses N1 as a trust input before reading the data directory.
	let missing = std::env::temp_dir().join("removed-forms-no-data-dir");
	let runtime = tokio::runtime::Builder::new_current_thread()
		.build()
		.unwrap();
	let (stdout, export, diagnostic, code) = runtime.block_on(compromise_audit2(
		&missing,
		&one(&all, "N15"),
		&[(1, 0)],
		None,
		false,
		None,
	));
	assert_eq!((stdout.as_str(), export, code), ("", None, 2));
	assert_eq!(
		diagnostic.as_deref(),
		Some("trusted manifest refused: the trusted manifest is not a profile-3 manifest")
	);
}
