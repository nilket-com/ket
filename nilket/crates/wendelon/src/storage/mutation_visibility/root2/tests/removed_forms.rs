//! The removed-forms corpus (`removed_forms.json`): the exact bytes of every admin form the
//! profile-3-only slice removes, each with its recorded outcome. The plan pins the file's digest.
//! Every entry is refused at its exact typed error and step, each against an accepted twin: N4-N12
//! since c1, N1-N3 and N13-N15 since c2.
use super::*;
use crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2;
use crate::storage::mutation_visibility::root2::admin::manifest::AdminManifestThree3;
use crate::storage::mutation_visibility::root2::compromise::compromise_audit2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	CorruptReason2, LogError2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_core::{
	Classification2, EvidenceError2, OwnerRequest2, op_id2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_log::{
	OwnerDir2, OwnerIoError2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::{
	ReplicaAnchorError2, ReplicaInput2, ReplicaInputs2, tuple_manifest_len2,
	validate_replica_tuple2,
};
use crate::storage::mutation_visibility::root2::provisioning::snapshot::{
	SnapshotError2, decode_snapshot2,
};
use crate::storage::mutation_visibility::root2::tests::profile_three_fixture::{
	M3_PRIMARY, chain_of, pre_root,
};
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use crate::storage::mutation_visibility::root2::verification::{VerifyError2, advance_verified2};
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
fn removed_manifests_are_refused_against_twins() {
	let all = entries();
	// N1 (137 bytes) and N2 (189 bytes) are refused at the length, before any header access.
	for id in ["N1", "N2"] {
		assert_eq!(
			AdminManifestThree3::decode(&one(&all, id)).err(),
			Some(AdminManifestError2::Length),
			"{id}"
		);
	}
	assert!(AdminManifestThree3::decode(&M3_PRIMARY).is_ok());
	// N3 (241 bytes, profile word 2) and its profile-1 relabel are refused at the profile; the
	// profile-3 relabel is the accepted twin.
	let n3 = one(&all, "N3");
	assert_eq!(&n3[10..12], &2u16.to_le_bytes());
	let with_word = |w: u16| {
		let mut b = n3.clone();
		b[10..12].copy_from_slice(&w.to_le_bytes());
		b
	};
	for word in [1u16, 2] {
		assert_eq!(
			AdminManifestThree3::decode(&with_word(word)).err(),
			Some(AdminManifestError2::UnsupportedProfile),
			"{word}"
		);
	}
	assert!(AdminManifestThree3::decode(&with_word(3)).is_ok());
}

// ---- c1: N4-N12 refused, each at its typed error and step, against an accepted twin ----

fn provisioning_vectors(name: &str) -> Value {
	serde_json::from_str(match name {
		"anchor" => include_str!("../provisioning/tests/anchor_vectors.json"),
		"three" => include_str!("../provisioning/tests/profile_three_vectors.json"),
		"terminal" => include_str!("../provisioning/tests/profile_three/terminal_vectors.json"),
		"owner" => include_str!("../provisioning/tests/owner_vectors.json"),
		_ => unreachable!(),
	})
	.unwrap()
}
fn reseal(mut b: Vec<u8>) -> Vec<u8> {
	let n = b.len() - 32;
	let sum = hash(b"", &b[..n]);
	b[n..].copy_from_slice(&sum);
	b
}
fn tuple(manifest: &[u8], snapshot: &[u8]) -> Result<(), ReplicaAnchorError2> {
	let manifest_pin = hash(b"VISROOT2-ADMIN-MANIFEST\0", manifest);
	let inventory_pin = hash(b"", snapshot);
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(manifest),
		manifest_pin: Some(&manifest_pin),
		snapshot: Some(snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.map(|_| ())
}
fn classification(log: &[u8]) -> Classification2 {
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	s.log(log);
	OwnerDir2::open(&s.0)
		.unwrap()
		.inspect()
		.unwrap()
		.classification
}

#[test]
fn removed_tuple_and_snapshots_are_refused_against_twins() {
	let all = entries();
	// N4: the 137-byte manifest is refused at its length, before any pin or parse.
	let n4 = of(&all, "N4")[0];
	assert_eq!(n4["manifest"], "N1");
	assert_eq!(
		tuple(&one(&all, "N1"), &bytes(n4)),
		Err(ReplicaAnchorError2::Length(ReplicaInput2::Manifest))
	);
	let chain = chain_of(&M3_PRIMARY, 241);
	assert_eq!(tuple_manifest_len2(&chain.tuple), 241);
	// N5 (version 2) and N6 (version 1 with an entry) are refused at the version.
	for id in ["N5", "N6"] {
		assert_eq!(
			decode_snapshot2(&one(&all, id)).err(),
			Some(SnapshotError2::Version),
			"{id}"
		);
	}
	let anchor = provisioning_vectors("anchor");
	let v3 = unhex(anchor["p3_snapshots"]["bound"]["bytes"].as_str().unwrap());
	assert_eq!(u16::from_le_bytes([v3[8], v3[9]]), 3);
	assert_eq!(decode_snapshot2(&v3).unwrap().entries.len(), 1);
	// N7: a Retired entry with no terminal record passes Version, Trailer and ProfileTag and is
	// refused at the S4 binding; the twin is the v4 terminal snapshot.
	let n7 = one(&all, "N7");
	assert_eq!(
		(u16::from_le_bytes([n7[8], n7[9]]), n7[22 + 97], n7[22 + 96]),
		(3, 3, 4)
	);
	assert_eq!(decode_snapshot2(&n7).err(), Some(SnapshotError2::Binding));
	let terminal = provisioning_vectors("terminal");
	let v4 = unhex(terminal["snapshot"].as_str().unwrap());
	let t = decode_snapshot2(&v4).unwrap();
	assert_eq!(u16::from_le_bytes([v4[8], v4[9]]), 4);
	assert!(t.entries[0].state == RootState2::Retired && t.entries[0].terminal.is_some());
	// N8: tags 1 and 2 are refused at the tag; the tag-3 entry alone is accepted.
	let n8 = one(&all, "N8");
	assert_eq!(&n8[22 + 97..22 + 98], &[1]);
	assert_eq!(
		decode_snapshot2(&n8).err(),
		Some(SnapshotError2::ProfileTag)
	);
	let mut tag_two = n8.clone();
	tag_two[22 + 97] = 3;
	assert_eq!(tag_two[22 + 320 + 97], 2);
	assert_eq!(
		decode_snapshot2(&reseal(tag_two)).err(),
		Some(SnapshotError2::ProfileTag)
	);
	let entry = &n8[22 + 2 * 320..22 + 3 * 320];
	assert_eq!(entry[97], 3);
	let mut alone = n8[..22].to_vec();
	alone[10..14].copy_from_slice(&1u32.to_le_bytes());
	alone.extend_from_slice(entry);
	alone.extend_from_slice(&[0; 32]);
	assert_eq!(decode_snapshot2(&reseal(alone)).unwrap().entries.len(), 1);
}

#[test]
fn removed_owner_forms_are_refused_against_twins() {
	let all = entries();
	let corrupt = |seq, reason| Classification2::Log(LogError2::Corrupt { seq, reason });
	// N9: VISOEV02 RESERVE events with n = 137 and n = 189 are refused at the payload.
	let n9 = one(&all, "N9");
	let n = |e: &[u8]| u16::from_le_bytes([e[132], e[133]]);
	assert_eq!([n(&n9[..512]), n(&n9[1024..])], [137, 189]);
	assert_eq!(classification(&n9), corrupt(1, CorruptReason2::Payload));
	let mut second = n9[1024..].to_vec();
	second[12..20].copy_from_slice(&1u64.to_le_bytes());
	assert_eq!(
		classification(&reseal_event(second)),
		corrupt(1, CorruptReason2::Payload)
	);
	// The twin: an n = 241 Reserve of M3_PRIMARY appends and folds.
	let s = Scratch::new();
	let mut o = OwnerDir2::create_v2(&s.0).unwrap();
	o.apply(
		&op_id2([9; 16]).unwrap(),
		&OwnerRequest2::Reserve {
			manifest: AdminManifestThree3::decode(&M3_PRIMARY).unwrap(),
		},
	)
	.unwrap();
	let log = fs::read(s.0.join("inventory.log")).unwrap();
	assert_eq!(n(&log), 241);
	assert_eq!(
		o.inspect().unwrap().classification,
		Classification2::Clean { last_seq: 1 }
	);
	drop(o);
	// N10: a plain RETIRE (n = 0) is refused at the payload, at its own sequence.
	let n10 = one(&all, "N10");
	let retire = &n10[6 * 512..];
	assert_eq!(n(retire), 0);
	// In context, after the profile-3 root's own prefix (the corpus log starts with N9's form).
	let three = provisioning_vectors("three");
	let p3: Vec<u8> = three["p3_events"]
		.as_array()
		.unwrap()
		.iter()
		.flat_map(|e| unhex(e.as_str().unwrap()))
		.collect();
	let mut plain = retire.to_vec();
	plain[12..20].copy_from_slice(&3u64.to_le_bytes());
	plain[36..132].copy_from_slice(&p3[512 + 36..512 + 132]);
	assert_eq!(
		classification(&[p3.clone(), reseal_event(plain)].concat()),
		corrupt(3, CorruptReason2::Payload)
	);
	// The twin: the n = 156 terminal Retire of the profile-3 root folds it Retired.
	let terminal = provisioning_vectors("terminal");
	let event = unhex(terminal["event"].as_str().unwrap());
	assert_eq!(n(&event), 156);
	assert_eq!(
		classification(&[p3, event].concat()),
		Classification2::Clean { last_seq: 3 }
	);
	// N11: an unmarked directory is Unformatted, with this VISIEV02 log, with none, and with a
	// VISOEV02 log; the twin is the same empty directory after create_v2.
	let n11 = one(&all, "N11");
	assert_eq!(&n11[..8], b"VISIEV02");
	let mut visoev02 = vec![0u8; 512];
	visoev02[..8].copy_from_slice(b"VISOEV02");
	for log in [Some(n11), None, Some(visoev02)] {
		let s = Scratch::new();
		if let Some(log) = &log {
			s.log(log);
		}
		assert_eq!(
			OwnerDir2::open(&s.0).err(),
			Some(OwnerIoError2::Unformatted)
		);
		if log.is_none() {
			drop(OwnerDir2::create_v2(&s.0).unwrap());
			assert!(OwnerDir2::open(&s.0).is_ok());
		}
	}
	// N12: VISREP03 evidence passes the name and the length bound and is refused at the magic.
	for e in of(&all, "N12") {
		let b = bytes(e);
		assert_eq!((b.len(), &b[..8]), (256, &b"VISREP03"[..]));
		let s = Scratch::new();
		drop(OwnerDir2::create_v2(&s.0).unwrap());
		let repairs = s.0.join("repairs");
		fs::create_dir(&repairs).unwrap();
		fs::set_permissions(&repairs, fs::Permissions::from_mode(0o700)).unwrap();
		fs::write(repairs.join(e["name"].as_str().unwrap()), &b).unwrap();
		assert_eq!(
			OwnerDir2::open(&s.0)
				.unwrap()
				.inspect()
				.unwrap()
				.classification,
			Classification2::EvidenceInvalid {
				index: 0,
				error: EvidenceError2::Magic
			},
			"{}",
			e["part"]
		);
	}
	// The twin: VISREP04 evidence (the p3_ case) is accepted against its own torn log.
	let owner = provisioning_vectors("owner");
	let case = &owner["p3_cases"]["base"];
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	s.log(&unhex(case["log"].as_str().unwrap()));
	let repairs = s.0.join("repairs");
	fs::create_dir(&repairs).unwrap();
	fs::set_permissions(&repairs, fs::Permissions::from_mode(0o700)).unwrap();
	fs::write(
		repairs.join(case["name"].as_str().unwrap()),
		unhex(case["evidence"].as_str().unwrap()),
	)
	.unwrap();
	assert!(matches!(
		OwnerDir2::open(&s.0)
			.unwrap()
			.inspect()
			.unwrap()
			.classification,
		Classification2::Pending { .. }
	));
}
fn reseal_event(mut b: Vec<u8>) -> Vec<u8> {
	let sum = hash(b"", &b[..480]);
	b[480..].copy_from_slice(&sum);
	b
}

#[test]
fn removed_roots_and_bootstraps_are_refused_against_twins() {
	let all = entries();
	// N13: a root committed to length 0 or 137 passes the checksum and is refused at the class
	// (Derived2::build); so is the same root relabelled to the other removed length. The twins are
	// the same-named S1 goldens and the profile-3 post-grant root.
	let gate: Value = serde_json::from_str(include_str!("gate_vectors.json")).unwrap();
	for e in of(&all, "N13") {
		let b = bytes(e);
		let part = e["part"].as_str().unwrap();
		let length = e["control_len"].as_u64().unwrap();
		assert_eq!(u64::from_le_bytes(b[192..200].try_into().unwrap()), length);
		assert_eq!(
			UnverifiedRoot2::decode(&b).err(),
			Some(RootError2::ControlClass),
			"{part}"
		);
		let other = if length == 0 { 189u64 } else { 0 };
		let mut relabelled = b.clone();
		relabelled[192..200].copy_from_slice(&other.to_le_bytes());
		let end = relabelled.len() - 32;
		let sum = hash(b"", &relabelled[..end]);
		relabelled[end..].copy_from_slice(&sum);
		assert_eq!(
			UnverifiedRoot2::decode(&relabelled).err(),
			Some(RootError2::ControlClass),
			"{part} relabelled"
		);
		let twin = if part == "post_grant_root" {
			unhex(gate["p3_post_grant_root"]["hex"].as_str().unwrap())
		} else {
			fixture(part)
		};
		assert_eq!(u64::from_le_bytes(twin[192..200].try_into().unwrap()), 241);
		assert!(UnverifiedRoot2::decode(&twin).is_ok(), "{part} twin");
	}
	// N14: a Bootstrap of length 0, 137 or 189 is refused at the class with the test token, and
	// against an M3_PRIMARY tuple in production; length 241 is the accepted twin.
	let chain = chain_of(&M3_PRIMARY, 241);
	let Command2::Bootstrap { epoch, control, .. } = chain.cmd.clone() else {
		unreachable!()
	};
	let (g, c, _, _, _) = chain.entry.fields();
	for e in of(&all, "N14") {
		let (eg, ec, cmd) = Command2::decode(&bytes(e)).unwrap();
		let length = e["length"].as_u64().unwrap();
		assert!(matches!(cmd, Command2::Bootstrap { length: n, .. } if n == length));
		let entry = TrustedEntry2::fixture(eg, ec, l(1), &cmd).unwrap();
		let verified = VerifiedCommand2::fixture(eg, ec, &cmd, &[], 0).unwrap();
		assert_eq!(
			Root2::reduce(None, &entry, &verified, &cmd).err(),
			Some(RootError2::ControlClass),
			"{length}"
		);
		let wrong = Command2::Bootstrap {
			epoch,
			control,
			length,
		};
		let (e2, committed) = bootstrap_entry(g, c, &wrong);
		assert_eq!(
			advance_verified2(&pre_root(), &chain.tuple, &committed, &e2, &wrong, None).err(),
			Some(VerifyError2::Consumer(RootError2::TrustMismatch)),
			"{length}"
		);
	}
	let (e2, committed) = bootstrap_entry(g, c, &chain.cmd);
	assert!(
		advance_verified2(&pre_root(), &chain.tuple, &committed, &e2, &chain.cmd, None).is_ok()
	);
	// N15: the compromise audit refuses N1 as a trust input before reading the data directory, at
	// the decode; the twin M3_PRIMARY passes that step (and stops at the missing directory).
	let missing = std::env::temp_dir().join("removed-forms-no-data-dir");
	let runtime = tokio::runtime::Builder::new_current_thread()
		.build()
		.unwrap();
	let audit = |manifest: &[u8]| {
		runtime.block_on(compromise_audit2(
			&missing,
			manifest,
			&[(1, 0)],
			None,
			false,
			None,
		))
	};
	let (stdout, export, diagnostic, code) = audit(&one(&all, "N15"));
	assert_eq!((stdout.as_str(), export, code), ("", None, 2));
	assert_eq!(
		diagnostic.as_deref(),
		Some(
			"trusted manifest refused: the trusted manifest does not decode as an administrator manifest"
		)
	);
	let (_, _, diagnostic, _) = audit(&M3_PRIMARY);
	assert!(
		!diagnostic
			.as_deref()
			.unwrap_or("")
			.starts_with("trusted manifest refused"),
		"{diagnostic:?}"
	);
}
/// The committed and trusted entries of a Bootstrap at (1, 1, 2), through the fixtures.
fn bootstrap_entry(g: [u8; 16], c: [u8; 16], cmd: &Command2) -> (TrustedEntry2, CommittedEntry2) {
	use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
	use crate::storage::mutation_visibility::root2::origin::EntryClass2;
	let e = TrustedEntry2::fixture(g, c, l(2), cmd).unwrap();
	let (_, _, _, fp, d) = e.fields();
	let committed = CommittedEntry2::fixture(
		EntryPosition2 {
			term: 1,
			node: 1,
			index: 2,
		},
		fp,
		EntryClass2::Bootstrap(d),
	);
	(e, committed)
}
