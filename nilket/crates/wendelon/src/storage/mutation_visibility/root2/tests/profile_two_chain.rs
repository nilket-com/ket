//! M3 propagation chain (r1 §5 as ordered by M3 r2 §1): a profile-2 manifest travels owner
//! request -> version-2 event -> owner export (snapshot v2) -> replica read, refused while
//! Prepared; then the committed Bootstrap FIXTURE, owner approval from its observation, re-export,
//! the Bound tuple; then the checked paths: C's Bootstrap verification from the pre-root, install,
//! the issuer gate and the recovery monitor. Placing the manifest file beside the export is fixture
//! setup (the owner manifest-export gap stays open). A propagation test only: `Ready` is that
//! monitor's verdict, not L readiness.
use super::profile_two_binding::{
	ADMISSION_SEED_B, GOVERNANCE_SEED_B, enrolled, exact_pin, grant, manifest_bytes, pre_root,
	reduce_at, rooted,
};
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestProfile2,
};
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::anchored::AnchorDir2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_core::{
	EventOp2, OwnerRequest2, op_id2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_log::OwnerDir2;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_read::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::decode_snapshot2;
use crate::storage::mutation_visibility::root2::recovery::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use crate::storage::mutation_visibility::root2::verification::*;
use std::ffi::CString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

// A fresh 0700 directory unique to this process and call; removed on drop.
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("p2-chain-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
fn read(
	out: &Scratch,
	manifest_pin: &Digest2,
	inventory_pin: &Digest2,
) -> Result<PinnedBoundTuple2, ReplicaReadError2> {
	let anchor = AnchorDir2::open(&out.0).unwrap();
	let (m, s) = (
		CString::new("p2.adm").unwrap(),
		CString::new("inv").unwrap(),
	);
	read_replica_tuple2(&ReplicaFiles2 {
		anchor: &anchor,
		manifest: &m,
		manifest_pin: Some(manifest_pin),
		snapshot: &s,
		inventory_pin: Some(inventory_pin),
	})
}
fn export(owner: &mut OwnerDir2, out: &Scratch, name: &str) -> Digest2 {
	let _ = fs::remove_file(out.0.join("inv"));
	owner.export(&out.0, name).unwrap()
}

#[test]
fn profile_two_chain_survives_owner_to_install() {
	let bytes = manifest_bytes("b");
	let any = AnyAdminManifest2::decode(&bytes).unwrap();
	let AnyAdminManifest2::Two(two) = &any else {
		unreachable!()
	};
	let pin = exact_pin(&bytes);
	let a = any.admission();
	let identity = RootIdentity2 {
		generation: a.generation,
		cluster: a.cluster,
		pin,
		admin_key: a.admin_key,
	};
	let (owner_dir, out) = (Scratch::new(), Scratch::new());
	// Fixture setup: the manifest file beside the export, under the anchor.
	fs::write(out.0.join("p2.adm"), &bytes).unwrap();
	let mut owner = OwnerDir2::open(&owner_dir.0).unwrap();

	// 1. Reserve: a version-2 event; the export is snapshot v2, tagged Two, Prepared.
	let reserved = owner
		.apply(
			&op_id2([1; 16]).unwrap(),
			&OwnerRequest2::Reserve {
				manifest: any.clone(),
			},
		)
		.unwrap();
	assert_eq!(reserved.original.map(|e| e.op), Some(EventOp2::Reserve));
	let log = fs::read(owner_dir.0.join("inventory.log")).unwrap();
	assert_eq!(u16::from_le_bytes([log[8], log[9]]), 2, "version-2 event");
	let inventory_pin = export(&mut owner, &out, "inv");
	let snapshot = fs::read(out.0.join("inv")).unwrap();
	assert_eq!(u16::from_le_bytes([snapshot[8], snapshot[9]]), 2);
	let s = decode_snapshot2(&snapshot).unwrap();
	assert_eq!(
		(s.entries[0].profile, s.entries[0].state),
		(ManifestProfile2::Two, RootState2::Prepared)
	);
	assert_eq!(
		read(&out, &pin, &inventory_pin).err(),
		Some(ReplicaReadError2::Tuple(ReplicaAnchorError2::NotBound(
			RootState2::Prepared
		)))
	);

	// 2. FIXTURE construction: the committed Bootstrap root; then the owner approves its
	// observation, and the re-export is Bound.
	let (cmd, entry, committed, fixture_root) = rooted(&bytes, 189);
	let approved = owner
		.apply(
			&op_id2([2; 16]).unwrap(),
			&OwnerRequest2::Approve {
				identity,
				observation: observe_bootstrap2(&fixture_root).unwrap(),
				confirmed: identity,
			},
		)
		.unwrap();
	assert_eq!(
		(approved.original.map(|e| e.op), approved.current),
		(Some(EventOp2::Approve), RootState2::Bound)
	);
	let inventory_pin = export(&mut owner, &out, "inv");
	let tuple = read(&out, &pin, &inventory_pin).unwrap();
	assert!(*tuple_manifest2(&tuple) == any);
	assert_eq!(tuple_manifest_len2(&tuple), 189);

	// 3. Checked paths. C: the actual Bootstrap verification from the pre-root, over the same
	// entry and command, reaches exactly the fixture-built root.
	let (verified_root, _) =
		advance_verified2(&pre_root(), &tuple, &committed, &entry, &cmd, None).unwrap();
	assert_eq!(
		encode_authority2(&verified_root).unwrap(),
		encode_authority2(&fixture_root).unwrap()
	);
	// Install: every profile-2 field survives.
	let admin = install_admin2(&verified_root, &tuple).unwrap();
	assert_eq!(admin_profile2(&admin), ManifestProfile2::Two);
	assert_eq!(admin_manifest_len2(&admin), 189);
	let role = admin_governance2(&admin).unwrap();
	assert!(role == two.governance());
	assert_eq!(
		(role.id(), role.key(), role.actions()),
		(
			bytes[137..153].try_into().unwrap(),
			bytes[153..185].try_into().unwrap(),
			u32::from_le_bytes(bytes[185..189].try_into().unwrap())
		)
	);
	assert_eq!(
		(admin_id2(&admin), admin_key2(&admin)),
		(a.admin_id, a.admin_key)
	);
	assert!(admin_namespace2(&admin) == a.namespace);
	assert_eq!(admin_context2(&admin).manifest_pin, pin);
	// The issuer gate: the Admission key applies; the governance key is a consumed denial.
	let state = enrolled(&verified_root, &any);
	assert!(matches!(
		reduce_at(&state, &admin, &grant(&admin, ADMISSION_SEED_B), 4),
		Ok(ApplyResult2::Permission { .. })
	));
	assert!(matches!(
		reduce_at(&state, &admin, &grant(&admin, GOVERNANCE_SEED_B), 4),
		Ok(ApplyResult2::Denied { reason: 8, .. })
	));
	// Recovery: replaying the same committed history reaches Ready at the root's tip (that
	// monitor's verdict only), and a re-install from the replayed state equals the first.
	let first = CommittedEntry2::fixture(
		EntryPosition2 {
			term: 0,
			node: 0,
			index: 0,
		},
		root_and_origin2(&verified_root).unwrap().1.genesis,
		EntryClass2::Membership,
	);
	let replayed_genesis = advance_pre_root2(&empty_authority2(), &first).unwrap().0;
	let replayed_pre_root = pre_root();
	let (replayed_root, _) =
		advance_verified2(&replayed_pre_root, &tuple, &committed, &entry, &cmd, None).unwrap();
	let (root, _) = root_and_origin2(&replayed_root).unwrap();
	let mut m = replay_monitor2(
		Deployment2 {
			generation: a.generation,
			cluster: a.cluster,
		},
		Some(Baseline2::Provisioned),
		ReplayTarget2 {
			cut: root.state.covered,
			fingerprint: root.state.fingerprint,
			command: crate::storage::mutation_visibility::root2::recovery::TargetCommand2::Expected(
				root.state.command_digest,
			),
		},
	)
	.unwrap();
	let mut last = None;
	for s in [&replayed_genesis, &replayed_pre_root, &replayed_root] {
		last = Some(observe2(&mut m, s).unwrap());
	}
	assert!(last == Some(Progress2::Ready(mark_of2(&replayed_root).unwrap())));
	assert!(install_admin2(&replayed_root, &tuple).unwrap() == admin);
}
