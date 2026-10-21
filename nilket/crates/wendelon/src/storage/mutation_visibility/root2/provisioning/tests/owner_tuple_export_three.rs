//! Profile-3 ports of the `owner_tuple_export.rs` rows on a marked (format 2) owner directory: the
//! same refusals, residue and next-call outcomes for `export_tuple` over VISOEV02 logs of profile-3
//! roots (the independent `p3_` vectors), with the exact 241-byte manifest artifact.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestProfile2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::inventory::v2::{
	decode_event2, encode_event2, fold_events2,
};
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::seam;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::decode_snapshot2;
use IoStep2::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

// A fresh 0700 directory unique to this process and call; removed on drop.
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("p-s1-export-three-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
	fn at(&self, rel: &str) -> PathBuf {
		self.0.join(rel)
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
fn listing(dir: &Path) -> Vec<String> {
	let mut v: Vec<_> = fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().file_name().into_string().unwrap())
		.collect();
	v.sort();
	v
}
/// A marked owner directory holding `log`, opened.
fn owner_with(log: &[u8]) -> (Scratch, OwnerDir2) {
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	fs::write(s.at("inventory.log"), log).unwrap();
	let o = OwnerDir2::open(&s.0).unwrap();
	(s, o)
}
fn key_of(id: &RootIdentity2) -> ([u8; 16], [u8; 16]) {
	(id.generation, id.cluster)
}
const E: usize =
	crate::storage::mutation_visibility::root2::provisioning::inventory::v2::EVENT_BYTES2;
/// The golden profile-3 log through the Conflict: m1 Bound, m2 Conflicted.
fn mixed() -> Vec<u8> {
	p3_log_bytes()[..4 * E].to_vec()
}
/// The whole golden log: m1 Retired (terminal), m2 Conflicted, then a Repair.
fn retired() -> Vec<u8> {
	p3_log_bytes()
}
fn identity_of(name: &str) -> RootIdentity2 {
	p3_event(name).identity.unwrap()
}
/// The golden profile-3 log through Approve: m1 Bound.
fn golden_bound() -> Vec<u8> {
	p3_log_bytes()[..2 * E].to_vec()
}
/// The exact canonical manifest bytes of the first root.
fn m1_bytes() -> Vec<u8> {
	let EventBody2::Reserve { manifest } = p3_event("reserve").body else {
		unreachable!()
	};
	manifest.encode().unwrap().as_slice().to_vec()
}
fn export(
	o: &mut OwnerDir2,
	out: &Scratch,
	key: ([u8; 16], [u8; 16]),
) -> Result<TupleExport2, OwnerIoError2> {
	o.export_tuple(&out.0, "root2.manifest", "root2.inventory", key.0, key.1)
}

#[test]
fn export_tuple_publishes_both_artifacts() {
	let b = identity_of("reserve");
	let (_s, mut o) = owner_with(&mixed());
	let out = Scratch::new();
	let t = export(&mut o, &out, key_of(&b)).unwrap();
	assert_eq!(listing(&out.0), ["root2.inventory", "root2.manifest"]);
	let manifest = fs::read(out.at("root2.manifest")).unwrap();
	let snapshot = fs::read(out.at("root2.inventory")).unwrap();
	assert_eq!(
		manifest,
		m1_bytes(),
		"the exact canonical 241-byte manifest"
	);
	assert_eq!(manifest.len(), 241);
	assert_eq!((t.profile, t.export_seq), (ManifestProfile2::Three, 4));
	assert_eq!(decode_snapshot2(&snapshot).unwrap().export_seq, 4);
	// The snapshot is byte-identical to `export` of the same state.
	let again = Scratch::new();
	let pin = o.export(&again.0, "inv").unwrap();
	assert_eq!(fs::read(again.at("inv")).unwrap(), snapshot);
	assert_eq!(pin, t.inventory_pin);
	// Through Approve only: the independent bound snapshot.
	let (_s, mut o) = owner_with(&golden_bound());
	let out = Scratch::new();
	let t = export(&mut o, &out, key_of(&b)).unwrap();
	assert_eq!(fs::read(out.at("root2.manifest")).unwrap(), m1_bytes());
	// The pin is the whole-manifest digest of the exact exported bytes (= the event header pin).
	let bytes = fs::read(out.at("root2.manifest")).unwrap();
	let any = AnyAdminManifest2::decode(&bytes).unwrap();
	let encoded = any.encode().unwrap();
	assert_eq!(encoded.as_slice(), &bytes[..]);
	assert_eq!(t.manifest_pin, any_manifest_digest2(&encoded));
	assert_eq!(t.manifest_pin, b.pin);
	assert_eq!(t.inventory_pin, hash(b"", &p3_snapshot_bytes("bound")));
	assert_eq!(
		fs::read(out.at("root2.inventory")).unwrap(),
		p3_snapshot_bytes("bound")
	);
}

/// The manifest pin is the domain-separated whole-manifest digest (= the event header pin); the
/// inventory pin is the empty-domain snapshot hash (= `export`'s). Their inequality here is
/// evidence for these fixtures, not a collision-impossibility claim.
#[test]
fn export_tuple_pins_are_the_two_distinct_rules() {
	let b = identity_of("reserve");
	let (_s, mut o) = owner_with(&mixed());
	let out = Scratch::new();
	let t = export(&mut o, &out, key_of(&b)).unwrap();
	let manifest = fs::read(out.at("root2.manifest")).unwrap();
	let snapshot = fs::read(out.at("root2.inventory")).unwrap();
	let any = AnyAdminManifest2::decode(&manifest).unwrap();
	let encoded = any.encode().unwrap();
	assert_eq!(encoded.as_slice(), &manifest[..], "the exact encoded bytes");
	assert_eq!(t.manifest_pin, any_manifest_digest2(&encoded));
	assert_eq!(t.manifest_pin, b.pin, "the Reserve event header's pin");
	assert_eq!(
		t.manifest_pin.to_vec(),
		unhex(
			serde_json::from_str::<serde_json::Value>(include_str!("anchor_vectors.json")).unwrap()
				["p3_manifests"]["m1"]["pin"]
				.as_str()
				.unwrap()
		),
		"the independent reference pin"
	);
	assert_eq!(t.inventory_pin, hash(b"", &snapshot));
	assert_ne!(t.manifest_pin, t.inventory_pin);
	assert_ne!(
		t.manifest_pin,
		hash(b"", &manifest),
		"not the snapshot rule"
	);
	assert_eq!(format!("{t:?}"), "TupleExport2 { redacted }");
}

/// Every refusal before any artifact, in the documented order: blocked, names, output directory,
/// the clean log view, root selection.
#[test]
fn export_tuple_refusals_in_order() {
	let (b, c) = (identity_of("reserve"), identity_of("reserve_second"));
	let a = b;
	let out = Scratch::new();
	let (s, mut o) = owner_with(&mixed());
	let names = |o: &mut OwnerDir2, m: &str, n: &str| {
		o.export_tuple(&out.0, m, n, b.generation, b.cluster).err()
	};
	for (m, n) in [
		("same", "same"),
		("", "inv"),
		(".hidden", "inv"),
		("x.tmp", "inv"),
		("man", "a/b"),
		("man", &"x".repeat(65)),
	] {
		assert_eq!(
			names(&mut o, m, n),
			Some(OwnerIoError2::BadExportName),
			"{m} {n}"
		);
	}
	// Output directory: canonicalization I/O, then containment.
	assert!(matches!(
		o.export_tuple(&s.at("absent"), "m", "i", b.generation, b.cluster),
		Err(OwnerIoError2::Io {
			step: ExportCanonical,
			..
		})
	));
	fs::create_dir(s.at("inside")).unwrap();
	assert_eq!(
		o.export_tuple(&s.at("inside"), "m", "i", b.generation, b.cluster)
			.err(),
		Some(OwnerIoError2::BadExportDir)
	);
	// Root selection on the clean folded state.
	assert_eq!(
		export(&mut o, &out, ([0x99; 16], [0x98; 16])).err(),
		Some(OwnerIoError2::ExportNoSuchRoot)
	);
	assert_eq!(
		export(&mut o, &out, (a.generation, [0x98; 16])).err(),
		Some(OwnerIoError2::ExportNoSuchRoot),
		"the exact (generation, cluster) key"
	);
	let (_r, mut retired_owner) = owner_with(&retired());
	assert_eq!(
		export(&mut retired_owner, &out, key_of(&a)).err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Retired))
	);
	assert_eq!(
		export(&mut o, &out, key_of(&c)).err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Conflicted))
	);
	let (_p, mut prepared) = owner_with(&p3_event_bytes("reserve"));
	assert_eq!(
		export(&mut prepared, &out, key_of(&a)).err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Prepared))
	);
	assert!(listing(&out.0).is_empty(), "no artifact on any refusal");
	// A non-Clean log is refused before root selection (a torn tail: Unclaimed).
	let (_t, mut torn) = owner_with(&[mixed(), vec![7; 100]].concat());
	assert!(matches!(
		export(&mut torn, &out, ([0x99; 16], [0x98; 16])),
		Err(OwnerIoError2::Classified(_))
	));
	// Multi-fault precedence: names before the directory; the directory before root selection.
	assert_eq!(
		o.export_tuple(&s.at("inside"), "same", "same", [0x99; 16], [0x98; 16])
			.err(),
		Some(OwnerIoError2::BadExportName)
	);
	assert_eq!(
		o.export_tuple(&s.at("inside"), "m", "i", [0x99; 16], [0x98; 16])
			.err(),
		Some(OwnerIoError2::BadExportDir)
	);
	// A pending sync-failure marker refuses first (blocked).
	fs::write(s.at("SYNC_FAILED"), b"x").unwrap();
	assert_eq!(
		names(&mut o, "same", "same"),
		Some(OwnerIoError2::SyncFailurePending)
	);
	assert!(listing(&out.0).is_empty());
}

/// Each publication stage's fault: the exact error, the filesystem residue, and what the next call
/// (same names, fresh handle) does. Only the sync steps poison and attempt a marker.
#[test]
fn export_tuple_faults_residue_and_next_call() {
	let b = key_of(&identity_of("reserve"));
	let eio = Some(libc::EIO);
	let io = |step| OwnerIoError2::Io { step, errno: eio };
	struct Case {
		step: IoStep2,
		residue: &'static [&'static str],
		poisons: bool,
		next: Option<OwnerIoError2>,
	}
	let cases = [
		// Manifest artifact.
		Case {
			step: ManifestTmpCreate,
			residue: &[],
			poisons: false,
			next: None,
		},
		Case {
			step: ManifestWrite,
			residue: &["m.tmp"],
			poisons: false,
			next: Some(OwnerIoError2::ExportTmpExists),
		},
		Case {
			step: ManifestSync,
			residue: &["m.tmp"],
			poisons: true,
			next: Some(OwnerIoError2::SyncFailurePending),
		},
		Case {
			step: ManifestLink,
			residue: &["m.tmp"],
			poisons: false,
			next: Some(OwnerIoError2::ExportTmpExists),
		},
		Case {
			step: ManifestTmpUnlink,
			residue: &["m", "m.tmp"],
			poisons: false,
			next: Some(OwnerIoError2::ExportTmpExists),
		},
		Case {
			step: ManifestDirSync,
			residue: &["m"],
			poisons: true,
			next: Some(OwnerIoError2::SyncFailurePending),
		},
		// Snapshot artifact: the manifest is already published.
		Case {
			step: ExportTmpCreate,
			residue: &["m"],
			poisons: false,
			next: Some(OwnerIoError2::ExportExists),
		},
		Case {
			step: ExportWrite,
			residue: &["i.tmp", "m"],
			poisons: false,
			next: Some(OwnerIoError2::ExportExists),
		},
		Case {
			step: ExportSync,
			residue: &["i.tmp", "m"],
			poisons: true,
			next: Some(OwnerIoError2::SyncFailurePending),
		},
		Case {
			step: ExportLink,
			residue: &["i.tmp", "m"],
			poisons: false,
			next: Some(OwnerIoError2::ExportExists),
		},
		// Both finals visible, the call fails, no pins from this call.
		Case {
			step: ExportTmpUnlink,
			residue: &["i", "i.tmp", "m"],
			poisons: false,
			next: Some(OwnerIoError2::ExportExists),
		},
		Case {
			step: ExportDirSync,
			residue: &["i", "m"],
			poisons: true,
			next: Some(OwnerIoError2::SyncFailurePending),
		},
	];
	for c in cases {
		let (s, mut o) = owner_with(&mixed());
		let out = Scratch::new();
		let got = {
			let _plan = seam::plan(&[(c.step, 1)], None);
			o.export_tuple(&out.0, "m", "i", b.0, b.1)
		};
		let err = got.expect_err("faulted export must fail");
		if c.poisons {
			assert!(
				matches!(err, OwnerIoError2::SyncFailed { step, .. } if step == c.step),
				"{:?}: {err:?}",
				c.step
			);
			assert!(
				s.at("SYNC_FAILED").exists(),
				"{:?}: marker attempted",
				c.step
			);
			assert_eq!(
				o.export_tuple(&out.0, "m2", "i2", b.0, b.1).err(),
				Some(OwnerIoError2::Poisoned { step: c.step }),
				"{:?}: this handle is latched",
				c.step
			);
		} else {
			assert_eq!(err, io(c.step), "{:?}", c.step);
			assert!(!s.at("SYNC_FAILED").exists(), "{:?}: no marker", c.step);
		}
		assert_eq!(listing(&out.0), c.residue, "{:?}: residue", c.step);
		drop(o);
		let mut fresh = OwnerDir2::open(&s.0).unwrap();
		match c.next {
			None => assert!(fresh.export_tuple(&out.0, "m", "i", b.0, b.1).is_ok()),
			Some(e) => assert_eq!(
				fresh.export_tuple(&out.0, "m", "i", b.0, b.1).err(),
				Some(e),
				"{:?}: next call",
				c.step
			),
		}
		// No call ever removes a final or adopts another invocation's tmp.
		for f in c.residue.iter().filter(|f| !f.ends_with(".tmp")) {
			assert!(out.at(f).exists(), "{:?}: final {f} kept", c.step);
		}
	}
}

/// A crash right after the manifest's directory sync leaves a durable manifest and no snapshot;
/// nothing is completed or cleaned on the next open, and the same names are refused.
#[test]
fn a_crash_between_artifacts_leaves_only_the_manifest() {
	let b = key_of(&identity_of("reserve"));
	let (s, o) = owner_with(&mixed());
	drop(o);
	let out = Scratch::new();
	let r = {
		let _plan = seam::plan(&[], Some((ManifestDirSync, 1)));
		catch_unwind(AssertUnwindSafe(|| {
			let mut o = OwnerDir2::open(&s.0).unwrap();
			let _ = o.export_tuple(&out.0, "m", "i", b.0, b.1);
		}))
	};
	assert!(r.is_err(), "the crash stop was reached");
	assert_eq!(listing(&out.0), ["m"]);
	assert_eq!(fs::read(out.at("m")).unwrap(), m1_bytes());
	let mut o = OwnerDir2::open(&s.0).unwrap();
	assert_eq!(
		o.export_tuple(&out.0, "m", "i", b.0, b.1).err(),
		Some(OwnerIoError2::ExportExists)
	);
	assert_eq!(
		listing(&out.0),
		["m"],
		"the refusal removed only its own tmp"
	);
	// New names complete normally; the stray manifest is the operator's to discard by name.
	assert!(o.export_tuple(&out.0, "m2", "i2", b.0, b.1).is_ok());
	assert_eq!(listing(&out.0), ["i2", "m", "m2"]);
}

/// Existing finals and stale tmps: never replaced, never adopted.
#[test]
fn existing_finals_and_tmps_are_refused_without_replacement() {
	let b = key_of(&identity_of("reserve"));
	for (pre, want, left) in [
		(&["m"][..], OwnerIoError2::ExportExists, &["m"][..]),
		(
			&["m.tmp"][..],
			OwnerIoError2::ExportTmpExists,
			&["m.tmp"][..],
		),
		(&["i"][..], OwnerIoError2::ExportExists, &["i", "m"][..]),
		(
			&["i.tmp"][..],
			OwnerIoError2::ExportTmpExists,
			&["i.tmp", "m"][..],
		),
	] {
		let (_s, mut o) = owner_with(&mixed());
		let out = Scratch::new();
		for f in pre {
			fs::write(out.at(f), b"operator").unwrap();
		}
		assert_eq!(
			o.export_tuple(&out.0, "m", "i", b.0, b.1).err(),
			Some(want),
			"{pre:?}"
		);
		assert_eq!(listing(&out.0), left, "{pre:?}");
		for f in pre {
			assert_eq!(
				fs::read(out.at(f)).unwrap(),
				b"operator",
				"{pre:?}: untouched"
			);
		}
	}
}

/// The fold invariant: one root per (generation, cluster). A duplicate would be a broken invariant
/// (`Decision(Inconsistent)`); it is unreachable through the fold, which refuses shared components.
#[test]
fn the_duplicate_root_invariant_is_the_fold_refusal() {
	let b = decode_event2(&p3_event_bytes("reserve")).unwrap();
	let mut dup = b.clone();
	dup.seq = 7;
	dup.op_id = [0x55; 16];
	let log = [retired(), encode_event2(&dup).unwrap().to_vec()].concat();
	assert!(matches!(
		fold_events2(&log),
		Err(LogError2::Corrupt {
			seq: 7,
			reason: CorruptReason2::IdentityTaken
		})
	));
}
