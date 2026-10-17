//! M2 through the NEW owner: apply and export over a log mixing version-2 Reserve events with
//! version-1 Approve/Conflict/Retire/Repair events, and the live refusal path an unreadable event
//! version takes (the path the old owner's source trace follows for version 2): refused before
//! any log, repair or export byte is written.
use super::profile_two_inventory::{p2_bytes, p2_events, p2_manifest};
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin::record::BootstrapObservation2;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::seam;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::decode_snapshot2;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

// A fresh 0700 directory unique to this process and call; removed on drop.
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("p2-owner-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
	fn at(&self, rel: &str) -> PathBuf {
		self.0.join(rel)
	}
	fn open(&self) -> OwnerDir2 {
		OwnerDir2::open(&self.0).unwrap()
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
// Every file under a directory: relative path, length and SHA-256.
fn manifest_of_dir(root: &Path) -> Vec<(String, usize, [u8; 32])> {
	let mut out = Vec::new();
	let mut stack = vec![root.to_path_buf()];
	while let Some(dir) = stack.pop() {
		for e in fs::read_dir(dir).unwrap() {
			let p = e.unwrap().path();
			if p.is_dir() {
				out.push((
					p.strip_prefix(root).unwrap().display().to_string(),
					0,
					[0; 32],
				));
				stack.push(p);
			} else {
				let b = fs::read(&p).unwrap();
				out.push((
					p.strip_prefix(root).unwrap().display().to_string(),
					b.len(),
					hash(b"", &b),
				));
			}
		}
	}
	out.sort();
	out
}
fn op(n: u8) -> OpId2 {
	let mut b = [n; 16];
	b[15] = 0x0e;
	op_id2(b).unwrap()
}
fn any(name: &str) -> AnyAdminManifest2 {
	AnyAdminManifest2::decode(&p2_manifest(name).0).unwrap()
}
fn approve(event: &[u8]) -> OwnerRequest2 {
	let e = decode_event2(event).unwrap();
	let id = e.identity.unwrap();
	let observation = match e.body {
		EventBody2::Approve { record } => BootstrapObservation2 {
			identity: record.identity,
		},
		EventBody2::Conflict { observation } => observation,
		_ => unreachable!(),
	};
	OwnerRequest2::Approve {
		identity: id,
		observation,
		confirmed: id,
	}
}
fn outcome(op: EventOp2, seq: u64, current: RootState2) -> OpOutcome2 {
	OpOutcome2 {
		original: Some(EventRef2 { op, seq }),
		current,
	}
}

#[test]
fn owner_apply_and_export_over_mixed_log() {
	let events = p2_events();
	let s = Scratch::new();
	let out = Scratch::new();
	let mut o = s.open();
	let a = decode_event2(&events[0]).unwrap().identity.unwrap();
	let steps: [(OwnerRequest2, OpOutcome2); 7] = [
		(
			OwnerRequest2::Reserve { manifest: any("a") },
			outcome(EventOp2::Reserve, 1, RootState2::Prepared),
		),
		(
			OwnerRequest2::Reserve { manifest: any("b") },
			outcome(EventOp2::Reserve, 2, RootState2::Prepared),
		),
		(
			OwnerRequest2::Reserve { manifest: any("c") },
			outcome(EventOp2::Reserve, 3, RootState2::Prepared),
		),
		(
			approve(&events[3]),
			outcome(EventOp2::Approve, 4, RootState2::Bound),
		),
		(
			approve(&events[4]),
			outcome(EventOp2::Approve, 5, RootState2::Bound),
		),
		(
			approve(&events[5]),
			outcome(EventOp2::Conflict, 6, RootState2::Conflicted),
		),
		(
			OwnerRequest2::Retire {
				terminal: None,
				identity: a,
			},
			outcome(EventOp2::Retire, 7, RootState2::Retired),
		),
	];
	for (i, (req, want)) in steps.iter().enumerate() {
		assert_eq!(o.apply(&op(i as u8 + 1), req), Ok(*want), "step {i}");
		// The owner wrote exactly the independently generated events.
		assert_eq!(
			fs::read(s.at("inventory.log")).unwrap(),
			events[..=i].concat()
		);
	}
	// A retry of the version-2 Reserve is recognised by its exact manifest bytes; nothing is written.
	assert_eq!(
		o.apply(&op(2), &steps[1].0),
		Ok(outcome(EventOp2::Reserve, 2, RootState2::Bound))
	);
	assert_eq!(
		fs::read(s.at("inventory.log")).unwrap(),
		events[..7].concat()
	);
	let pin = o.export(&out.0, "inv-7").unwrap();
	let bytes = fs::read(out.at("inv-7")).unwrap();
	assert_eq!(pin, hash(b"", &bytes));
	assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), 2);
	assert_eq!(
		bytes,
		crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2(
			&fold_events2(&events[..7].concat()).unwrap()
		)
		.unwrap()
	);
	drop(o);

	// The whole generated mixed log, Repair included: the export is the generated snapshot, and
	// the owner keeps appending version-1 events after version-2 ones.
	let s = Scratch::new();
	fs::write(s.at("inventory.log"), events.concat()).unwrap();
	let mut o = s.open();
	let pin = o.export(&out.0, "inv-8").unwrap();
	let bytes = fs::read(out.at("inv-8")).unwrap();
	assert_eq!(bytes, p2_bytes("mixed_snapshot"));
	assert_eq!(pin, hash(b"", &bytes));
	let b = decode_event2(&events[1]).unwrap().identity.unwrap();
	assert_eq!(
		o.apply(
			&op(9),
			&OwnerRequest2::Retire {
				terminal: None,
				identity: b
			}
		),
		Ok(outcome(EventOp2::Retire, 9, RootState2::Retired))
	);
	let log = fs::read(s.at("inventory.log")).unwrap();
	assert_eq!(
		(&log[..8 * 384], log.len()),
		(&events.concat()[..], 9 * 384)
	);
	let s9 = decode_snapshot2(&{
		o.export(&out.0, "inv-9").unwrap();
		fs::read(out.at("inv-9")).unwrap()
	})
	.unwrap();
	assert_eq!(s9.export_seq, 9);
}

/// The live owner over a log whose third event carries a version it cannot read (3): apply,
/// export and repair check refuse at the classification, before any log append, truncation,
/// evidence or export file; only the open-time lock file exists, and no byte changes.
#[test]
fn owner_refuses_an_unreadable_event_version_before_any_write() {
	let events = p2_events();
	let mut third = events[2].clone();
	third[8..10].copy_from_slice(&3u16.to_le_bytes());
	let sum = hash(b"", &third[..352]);
	third[352..].copy_from_slice(&sum);
	let log = [events[0].clone(), events[1].clone(), third].concat();
	let s = Scratch::new();
	let out = Scratch::new();
	fs::write(s.at("inventory.log"), &log).unwrap();
	let mut o = s.open();
	// Open creates only the lock file (metadata, not log, history or export bytes).
	let before = manifest_of_dir(&s.0);
	assert_eq!(
		before.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
		["LOCK", "inventory.log"]
	);
	let refused = Err(OwnerIoError2::Classified(Classification2::Log(
		LogError2::Corrupt {
			seq: 3,
			reason: CorruptReason2::Version,
		},
	)));
	let _plan = seam::plan(&[], None);
	assert_eq!(
		o.apply(&op(0x31), &OwnerRequest2::Reserve { manifest: any("d") })
			.map(|_| ()),
		refused
	);
	assert_eq!(o.export(&out.0, "inv").map(|_| ()), refused);
	assert_eq!(o.repair_check().map(|_| ()), refused);
	let seen = seam::seen();
	for step in [
		IoStep2::LogCreate,
		IoStep2::LogWrite,
		IoStep2::LogSync,
		IoStep2::Truncate,
		IoStep2::RepairsCreate,
		IoStep2::EvidenceTmpCreate,
		IoStep2::ExportTmpCreate,
		IoStep2::ExportWrite,
		IoStep2::MarkerCreate,
	] {
		assert!(!seen.contains(&step), "{step:?} in {seen:?}");
	}
	assert_eq!(manifest_of_dir(&s.0), before);
	assert!(manifest_of_dir(&out.0).is_empty());
	assert_eq!(fs::read(s.at("inventory.log")).unwrap(), log);
}
