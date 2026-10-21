use super::*;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::{
	IoStep2, OwnerDir2, OwnerIoError2, seam,
};
use crate::storage::mutation_visibility::root2::provisioning::{inventory, owner_core, snapshot};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, path::PathBuf};
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
fn op(n: u8) -> owner_core::OpId2 {
	owner_core::op_id2([n; 16]).unwrap()
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
	// A marked directory holding a VISIEV02 log refuses both repair paths, then refuses to open.
	let s = Scratch::new();
	let mut o = OwnerDir2::create_v2(&s.0).unwrap();
	let bytes = visiev02_bytes();
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
	// An unmarked directory (here holding a VISOEV02 log) is never opened, so no repair path runs.
	let s = Scratch::new();
	let bytes = b(&vectors()["p3_events"][0]);
	fs::write(s.at("inventory.log"), &bytes).unwrap();
	assert_eq!(
		OwnerDir2::open(&s.0).err(),
		Some(OwnerIoError2::Unformatted)
	);
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), bytes);
}
/// The v2 open reads the log header eagerly: a non-regular log refuses the open itself, so no
/// handle with a deferred probe exists.
#[test]
fn a_nonregular_v2_log_is_refused_at_open() {
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	fs::remove_file(s.at("inventory.log")).unwrap();
	fs::create_dir(s.at("inventory.log")).unwrap();
	assert!(matches!(
		OwnerDir2::open(&s.0).err(),
		Some(OwnerIoError2::NotRegular {
			step: IoStep2::LogOpen
		})
	));
}
mod p3_keys;
mod terminal_binding;
/// The c1 transitional refusals of profile-1/2 inputs (deleted in c2): the snapshot encoder refuses
/// a One/Two entry with `ProfileTag`, the VISOEV02 encoder refuses a One/Two Reserve with
/// `Payload`, and the owner core therefore refuses such a Reserve as `Codec` before any state.
/// The profile-3 manifest is the accepted twin in each row.
#[test]
fn profile_one_and_two_inputs_are_refused_by_the_encoders() {
	use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
	let v = vectors();
	for (i, profile_three) in [(0, false), (1, false), (2, true)] {
		let manifest = AnyAdminManifest2::decode(&b(&v["manifests"][i])).unwrap();
		let a = manifest.admission();
		let identity = inventory::RootIdentity2 {
			generation: a.generation,
			cluster: a.cluster,
			pin: [0x33; 32],
			admin_key: a.admin_key,
		};
		let state = inventory::LogState2 {
			roots: vec![inventory::RootEntry2 {
				identity,
				manifest: manifest.clone(),
				state: inventory::RootState2::Prepared,
				last_seq: 1,
				record: None,
				terminal: None,
			}],
			last_seq: 1,
		};
		let snapshot = snapshot::encode_snapshot2(&state);
		let event = inventory::v2::encode_event2(&inventory::Event2 {
			seq: 1,
			op_id: [1; 16],
			identity: Some(identity),
			body: inventory::EventBody2::Reserve {
				manifest: manifest.clone(),
			},
		});
		let decision = owner_core::v2::decide2(
			&[],
			&op(1),
			&owner_core::OwnerRequest2::Reserve { manifest },
		);
		if profile_three {
			assert!(snapshot.is_ok());
			assert!(event.is_ok());
			assert!(matches!(decision, owner_core::Decision2::Append(_)));
		} else {
			assert_eq!(
				snapshot.err(),
				Some(snapshot::SnapshotError2::ProfileTag),
				"{i}"
			);
			assert_eq!(
				event.err(),
				Some(inventory::LogError2::Corrupt {
					seq: 1,
					reason: inventory::CorruptReason2::Payload
				}),
				"{i}"
			);
			assert_eq!(
				decision,
				owner_core::Decision2::Refuse(owner_core::OwnerError2::Codec),
				"{i}"
			);
		}
	}
}
