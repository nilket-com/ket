use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::{
	IoStep2, OwnerDir2, OwnerIoError2, seam,
};
use crate::storage::mutation_visibility::root2::provisioning::{inventory, owner_core, snapshot};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, path::PathBuf};
#[path = "profile_three/old_v2_snapshot.rs"]
pub(super) mod old_v2_snapshot;
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let p = std::env::temp_dir().join(format!(
			"owner-v2-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::SeqCst)
		));
		fs::create_dir(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
	fn at(&self, n: &str) -> PathBuf {
		self.0.join(n)
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		fs::remove_dir_all(&self.0).unwrap();
	}
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("profile_three_vectors.json")).unwrap()
}
fn b(v: &serde_json::Value) -> Vec<u8> {
	unhex(v.as_str().unwrap())
}
fn log() -> Vec<u8> {
	vectors()["events"]
		.as_array()
		.unwrap()
		.iter()
		.flat_map(b)
		.collect()
}
fn op(n: u8) -> owner_core::OpId2 {
	owner_core::op_id2([n; 16]).unwrap()
}
#[test]
fn independent_v2_events_fold_and_snapshot_versions_by_content() {
	let v = vectors();
	let mut prefix = Vec::new();
	for (i, bytes) in v["events"].as_array().unwrap().iter().enumerate() {
		let bytes = b(bytes);
		let e = inventory::v2::decode_event2(&bytes).unwrap();
		assert_eq!(inventory::v2::encode_event2(&e).unwrap().as_slice(), bytes);
		prefix.extend(bytes);
		let state = inventory::v2::fold_events2(&prefix).unwrap();
		if i % 2 == 1 {
			let name = ["one", "two", "three"][i / 2];
			assert_eq!(
				snapshot::encode_snapshot2(&state).unwrap(),
				b(&v["snapshots"][name])
			);
		}
	}
	prefix.extend(b(&v["retire"]));
	assert_eq!(
		snapshot::encode_snapshot2(&inventory::v2::fold_events2(&prefix).unwrap()).unwrap(),
		b(&v["snapshots"]["retired"])
	);
}
#[test]
fn old_v2_reader_accepts_v1_v2_and_refuses_v3_version() {
	let bytes = include_bytes!("profile_three/snapshot_8beb.txt");
	let digest = hash(b"", bytes)
		.iter()
		.map(|x| format!("{x:02x}"))
		.collect::<String>();
	assert_eq!(
		digest,
		"75248f2016dacde8accafe87fe1692c69d6c0c7b362ef85f6a6a0066f313c7b4"
	);
	let v = vectors();
	for name in ["one", "two"] {
		let bytes = b(&v["snapshots"][name]);
		assert_eq!(old_v2_snapshot::roundtrip(&bytes).unwrap(), bytes);
	}
	assert_eq!(
		old_v2_snapshot::roundtrip(&b(&v["snapshots"]["three"])).err(),
		Some(old_v2_snapshot::SnapshotError2::Version)
	);
}
#[test]
fn snapshot_three_canonicality_tags_and_reserved_are_strict() {
	let v = vectors();
	let mut no_three = b(&v["snapshots"]["two"]);
	no_three[8..10].copy_from_slice(&3u16.to_le_bytes());
	let no_three = reseal_snapshot(no_three);
	assert_eq!(
		snapshot::decode_snapshot2(&no_three).err(),
		Some(snapshot::SnapshotError2::NotCanonical)
	);
	let three = b(&v["snapshots"]["three"]);
	for (version, tag) in [(2u16, 3u8), (3, 4)] {
		let mut x = three.clone();
		x[8..10].copy_from_slice(&version.to_le_bytes());
		x[22 + 2 * 320 + 97] = tag;
		assert_eq!(
			snapshot::decode_snapshot2(&reseal_snapshot(x)).err(),
			Some(snapshot::SnapshotError2::ProfileTag)
		);
	}
	let mut x = three;
	x[22 + 98] = 1;
	assert_eq!(
		snapshot::decode_snapshot2(&reseal_snapshot(x)).err(),
		Some(snapshot::SnapshotError2::Reserved)
	);
}
#[test]
fn v1_profile_three_refuses_without_writes_and_v2_accepts_all_profiles() {
	let v = vectors();
	let s = Scratch::new();
	let mut old = OwnerDir2::open(&s.0).unwrap();
	let three = AnyAdminManifest2::decode(&b(&v["manifests"][2])).unwrap();
	assert_eq!(
		old.apply(
			&op(9),
			&owner_core::OwnerRequest2::Reserve { manifest: three }
		)
		.err(),
		Some(OwnerIoError2::Decision(
			owner_core::OwnerError2::ProfileUnsupported
		))
	);
	assert!(!s.at("inventory.log").exists());
	drop(old);
	let s = Scratch::new();
	let mut new = OwnerDir2::create_v2(&s.0).unwrap();
	for (i, m) in v["manifests"].as_array().unwrap().iter().enumerate() {
		new.apply(
			&op(i as u8 + 1),
			&owner_core::OwnerRequest2::Reserve {
				manifest: AnyAdminManifest2::decode(&b(m)).unwrap(),
			},
		)
		.unwrap();
	}
	assert_eq!(fs::read(s.at("inventory.log")).unwrap().len(), 3 * 512);
	drop(new);
	assert_eq!(
		OwnerDir2::open(&s.0)
			.unwrap()
			.inspect()
			.unwrap()
			.classification,
		owner_core::Classification2::Clean { last_seq: 3 }
	);
}
#[test]
fn owner_v2_initialization_crash_matrix_and_strict_marker() {
	use IoStep2::*;
	for step in [
		FormatTempCreate,
		FormatWrite,
		FormatSync,
		FormatPublish,
		FormatDirSync,
		FormatLogCreate,
		FormatLogSync,
		FormatFinalSync,
	] {
		let s = Scratch::new();
		{
			let _p = seam::plan(&[], Some((step, 1)));
			let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
				let _ = OwnerDir2::create_v2(&s.0);
			}));
			assert!(stopped.is_err(), "{step:?}");
		}
		let got = OwnerDir2::open(&s.0);
		if matches!(step, FormatLogCreate | FormatLogSync | FormatFinalSync) {
			assert!(got.is_ok(), "{step:?}");
		} else {
			assert_eq!(got.err(), Some(OwnerIoError2::Incomplete), "{step:?}");
		}
	}
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	let path = s.at("OWNER_FORMAT");
	let original = fs::read(&path).unwrap();
	assert_eq!(original.len(), 48);
	assert_eq!(original, b(&vectors()["marker"]));
	for mode in [0o400, 0o644] {
		fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
		assert_eq!(
			OwnerDir2::open(&s.0).err(),
			Some(OwnerIoError2::MarkerCorrupt)
		);
	}
	fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
	let mut wrong = original;
	wrong[12] = 1;
	fs::write(&path, wrong).unwrap();
	assert_eq!(
		OwnerDir2::open(&s.0).err(),
		Some(OwnerIoError2::MarkerCorrupt)
	);
}
#[test]
fn owner_v2_creation_never_upgrades_or_replaces_entries() {
	for name in [
		"inventory.log",
		"OWNER_FORMAT",
		"OWNER_FORMAT.tmp",
		"SYNC_FAILED",
		"foreign",
	] {
		let s = Scratch::new();
		fs::write(s.at(name), b"retained").unwrap();
		assert_eq!(
			OwnerDir2::create_v2(&s.0).err(),
			Some(OwnerIoError2::CreationNotEmpty)
		);
		assert_eq!(fs::read(s.at(name)).unwrap(), b"retained");
	}
	let s = Scratch::new();
	fs::create_dir(s.at("repairs")).unwrap();
	assert_eq!(
		OwnerDir2::create_v2(&s.0).err(),
		Some(OwnerIoError2::CreationNotEmpty)
	);
}
#[test]
fn both_cross_framing_repair_paths_refuse_before_any_write() {
	let v = vectors();
	for two in [false, true] {
		let s = Scratch::new();
		let mut o = if two {
			OwnerDir2::create_v2(&s.0).unwrap()
		} else {
			OwnerDir2::open(&s.0).unwrap()
		};
		let bytes = if two {
			event_bytes("reserve")
		} else {
			b(&v["events"][0])
		};
		fs::write(s.at("inventory.log"), &bytes).unwrap();
		assert_eq!(
			o.repair_fresh(&hash(b"", &bytes)).err(),
			Some(OwnerIoError2::ForeignFormat)
		);
		assert_eq!(
			o.repair_resume(&[0; 32]).err(),
			Some(OwnerIoError2::ForeignFormat)
		);
		assert_eq!(fs::read(s.at("inventory.log")).unwrap(), bytes);
		drop(o);
		assert_eq!(
			OwnerDir2::open(&s.0).err(),
			Some(OwnerIoError2::ForeignFormat)
		);
	}
}
#[test]
fn independent_v2_repair_context_reopen_and_cross_evidence_refusal() {
	let v = vectors();
	let prefix = log();
	let tail = b(&v["repair"]["tail"]);
	let torn = [prefix.clone(), tail.clone()].concat();
	let (name, evidence) = owner_core::v2::encode_evidence2(&torn).unwrap();
	assert_eq!(name, v["repair"]["name"].as_str().unwrap());
	assert_eq!(evidence, b(&v["repair"]["bytes"]));
	assert_eq!(evidence.len(), 124 + 5 + 32);
	assert_eq!(
		owner_core::decode_evidence2(&name, &evidence).err(),
		Some(owner_core::EvidenceError2::Magic)
	);
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	fs::write(s.at("inventory.log"), &torn).unwrap();
	let mut o = OwnerDir2::open(&s.0).unwrap();
	o.repair_fresh(&hash(b"", &tail)).unwrap();
	drop(o);
	let mut reopened = OwnerDir2::open(&s.0).unwrap();
	assert_eq!(reopened.repair_check().unwrap(),crate::storage::mutation_visibility::root2::provisioning::owner_log::RepairOutcome2::NothingToRepair);
	let repaired = fs::read(s.at("inventory.log")).unwrap();
	assert_eq!(&repaired[..prefix.len()], prefix);
	assert_eq!(repaired.len(), 7 * 512);

	let mut bad_format = evidence.clone();
	bad_format[10..12].copy_from_slice(&1u16.to_le_bytes());
	assert_eq!(
		owner_core::v2::decode_evidence2(&name, &bad_format).err(),
		Some(owner_core::EvidenceError2::Format)
	);
	// The reverse evidence direction also refuses, before owner mutation.
	let old_torn = [event_bytes("reserve"), tail].concat();
	let (old_name, old_evidence) = owner_core::encode_evidence2(&old_torn).unwrap();
	assert_eq!(
		owner_core::v2::decode_evidence2(&old_name, &old_evidence).err(),
		Some(owner_core::EvidenceError2::Magic)
	);

	for two in [false, true] {
		let s = Scratch::new();
		let mut o = if two {
			OwnerDir2::create_v2(&s.0).unwrap()
		} else {
			OwnerDir2::open(&s.0).unwrap()
		};
		let prefix = if two { log() } else { event_bytes("reserve") };
		let torn = [prefix, b(&v["repair"]["tail"])].concat();
		fs::write(s.at("inventory.log"), &torn).unwrap();
		fs::create_dir(s.at("repairs")).unwrap();
		fs::set_permissions(s.at("repairs"), fs::Permissions::from_mode(0o700)).unwrap();
		let (n, e) = if two {
			(&old_name, &old_evidence)
		} else {
			(&name, &evidence)
		};
		fs::write(s.at("repairs").join(n), e).unwrap();
		let _plan = seam::plan(&[], None);
		for error in [
			o.repair_fresh(&hash(b"", &b(&v["repair"]["tail"]))).err(),
			o.repair_resume(&[0; 32]).err(),
		] {
			assert!(matches!(
				error,
				Some(OwnerIoError2::Classified(
					owner_core::Classification2::EvidenceInvalid {
						error: owner_core::EvidenceError2::Magic,
						..
					}
				))
			));
		}
		assert!(
			!seam::seen()
				.iter()
				.any(|s| matches!(s, IoStep2::Truncate | IoStep2::LogWrite))
		);
		assert_eq!(fs::read(s.at("inventory.log")).unwrap(), torn);
	}
}
#[test]
fn v2_framing_payload_zero_fill_and_repair_bounds_are_strict() {
	let mut bytes = b(&vectors()["events"][0]);
	let seal = |mut x: Vec<u8>| {
		let sum = hash(b"", &x[..480]);
		x[480..].copy_from_slice(&sum);
		x
	};
	bytes[132..134].copy_from_slice(&240u16.to_le_bytes());
	assert!(matches!(
		inventory::v2::decode_event2(&seal(bytes)),
		Err(inventory::LogError2::Corrupt {
			reason: inventory::CorruptReason2::Payload,
			..
		})
	));
	for offset in [11, 134, 144 + 137] {
		let mut x = b(&vectors()["events"][0]);
		x[offset] = 1;
		let reason = if offset == 144 + 137 {
			inventory::CorruptReason2::Payload
		} else {
			inventory::CorruptReason2::Reserved
		};
		assert!(
			matches!(inventory::v2::decode_event2(&seal(x)),Err(inventory::LogError2::Corrupt{reason:r,..})if r==reason)
		);
	}
	let e = inventory::Event2 {
		seq: 7,
		op_id: [7; 16],
		identity: None,
		body: inventory::EventBody2::Repair {
			tail_sha256: [1; 32],
			original_len: 6 * 512 + 1024,
			boundary: 6 * 512,
			tail_len: 1024,
		},
	};
	assert!(matches!(
		inventory::v2::encode_event2(&e),
		Err(inventory::LogError2::Corrupt {
			reason: inventory::CorruptReason2::RepairBounds,
			..
		})
	));
}
#[test]
fn v2_max_tail_evidence_and_log_read_bounds_are_exact() {
	let prefix = log();
	let torn = [prefix, vec![0; 1023]].concat();
	let (name, bytes) = owner_core::v2::encode_evidence2(&torn).unwrap();
	assert_eq!(bytes.len(), 1179);
	assert_eq!(
		owner_core::v2::decode_evidence2(&name, &bytes)
			.unwrap()
			.tail
			.len(),
		1023
	);
	let too_big = vec![0; 33554944];
	assert!(matches!(
		inventory::v2::fold_events2(&too_big),
		Err(inventory::LogError2::TooLong)
	));
	let mut evidence = bytes;
	evidence.push(0);
	assert_eq!(
		owner_core::v2::decode_evidence2(&name, &evidence).err(),
		Some(owner_core::EvidenceError2::Length)
	);
}
#[test]
fn deferred_nonregular_probe_never_allows_a_later_foreign_log_write() {
	let s = Scratch::new();
	fs::create_dir(s.at("inventory.log")).unwrap();
	let mut o = OwnerDir2::open(&s.0).unwrap();
	assert!(matches!(
		o.inspect(),
		Err(OwnerIoError2::NotRegular {
			step: IoStep2::LogOpen
		})
	));
	fs::remove_dir(s.at("inventory.log")).unwrap();
	let bytes = b(&vectors()["events"][0]);
	fs::write(s.at("inventory.log"), &bytes).unwrap();
	let req = owner_core::OwnerRequest2::Reserve {
		manifest: AnyAdminManifest2::decode(&b(&vectors()["manifests"][0])).unwrap(),
	};
	assert_eq!(
		o.apply(&op(8), &req).err(),
		Some(OwnerIoError2::ForeignFormat)
	);
	assert_eq!(
		o.repair_fresh(&[0; 32]).err(),
		Some(OwnerIoError2::ForeignFormat)
	);
	assert_eq!(
		o.repair_resume(&[0; 32]).err(),
		Some(OwnerIoError2::ForeignFormat)
	);
	assert_eq!(o.repair_check().err(), Some(OwnerIoError2::ForeignFormat));
	assert_eq!(
		o.clear_sync_failure(&[0; 32]).err(),
		Some(OwnerIoError2::ForeignFormat)
	);
	assert_eq!(
		o.export(&s.0, "snapshot").err(),
		Some(OwnerIoError2::ForeignFormat)
	);
	assert_eq!(
		o.export_tuple(&s.0, "manifest", "snapshot", [0; 16], [0; 16])
			.err(),
		Some(OwnerIoError2::ForeignFormat)
	);
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), bytes);
	assert!(!s.at("snapshot").exists());
	assert!(!s.at("manifest").exists());
}

mod terminal_binding;
