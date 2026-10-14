//! P-S2 (contract r1 §E test 1): owner-produced tuples feed the replica read, C's Bootstrap check
//! and install. A real `OwnerDir2` reserves, approves from the FIXTURE-built committed root's
//! observation (as in g1c/M3) and runs `export_tuple`; the replica reads exactly the files it
//! wrote, beneath an `AnchorDir2` over its output directory, with the pins it returned. No manifest
//! is placed by hand on this leg. A local filesystem chain with the output directory as the
//! anchor: it does not prove a replica deployment or an operator copy. The intended-root binding
//! is C's and install's (against the committed root), distinct from the tuple's own-key entry
//! selection, which the wrong-context case shows passing before C and install refuse.
use super::profile_two_binding::{exact_pin, expected_context, manifest_bytes, pre_root, rooted};
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestProfile2,
};
use crate::storage::mutation_visibility::root2::admin_install::*;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::anchored::AnchorDir2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_core::{
	EventOp2, OwnerRequest2, op_id2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_log::{
	OwnerDir2, TupleExport2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_read::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use crate::storage::mutation_visibility::root2::verification::*;
use std::ffi::CString;
use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

/// A fresh 0700 directory under the temp dir, created exclusively under a random name: an existing
/// path is never adopted or removed (a collision retries with a new name). Drop removes only the
/// directory this value created.
pub(super) struct Scratch(pub PathBuf);
impl Scratch {
	pub(super) fn new() -> Self {
		for _ in 0..8 {
			let p = std::env::temp_dir().join(format!("p-s2-delivery-{}", uuid::Uuid::new_v4()));
			match fs::DirBuilder::new().mode(0o700).create(&p) {
				Ok(()) => return Self(p),
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
				Err(e) => panic!("{}: {e}", p.display()),
			}
		}
		panic!("no fresh scratch directory in 8 attempts")
	}
	pub(super) fn at(&self, rel: &str) -> PathBuf {
		self.0.join(rel)
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}

pub(super) const MANIFEST: &str = "root2.manifest";
pub(super) const INVENTORY: &str = "root2.inventory";

/// One owner whose manifest `name` was reserved and approved from the FIXTURE root committed with
/// Bootstrap `length`, then exported with `export_tuple` into `out`.
pub(super) struct Delivered {
	pub bytes: Vec<u8>,
	pub any: AnyAdminManifest2,
	pub identity: RootIdentity2,
	pub cmd: Command2,
	pub entry: TrustedEntry2,
	pub committed: CommittedEntry2,
	pub root: MutationAuthorityState2,
	pub _owner_dir: Scratch,
	pub owner: OwnerDir2,
	pub out: Scratch,
	pub export: TupleExport2,
}
pub(super) fn identity_of(bytes: &[u8]) -> RootIdentity2 {
	let any = AnyAdminManifest2::decode(bytes).unwrap();
	let a = any.admission();
	RootIdentity2 {
		generation: a.generation,
		cluster: a.cluster,
		pin: exact_pin(bytes),
		admin_key: a.admin_key,
	}
}
/// A real owner with `bytes` reserved (Prepared); nothing exported.
pub(super) fn reserved(bytes: &[u8]) -> (Scratch, OwnerDir2) {
	let dir = Scratch::new();
	let mut owner = OwnerDir2::open(&dir.0).unwrap();
	let reserved = owner
		.apply(
			&op_id2([1; 16]).unwrap(),
			&OwnerRequest2::Reserve {
				manifest: AnyAdminManifest2::decode(bytes).unwrap(),
			},
		)
		.unwrap();
	assert_eq!(
		(reserved.original.map(|e| e.op), reserved.current),
		(Some(EventOp2::Reserve), RootState2::Prepared)
	);
	(dir, owner)
}
pub(super) fn delivered(name: &str, length: u64) -> Delivered {
	let bytes = manifest_bytes(name);
	let any = AnyAdminManifest2::decode(&bytes).unwrap();
	let identity = identity_of(&bytes);
	let (_owner_dir, mut owner) = reserved(&bytes);
	let (cmd, entry, committed, root) = rooted(&bytes, length);
	let approved = owner
		.apply(
			&op_id2([2; 16]).unwrap(),
			&OwnerRequest2::Approve {
				identity,
				observation: observe_bootstrap2(&root).unwrap(),
				confirmed: identity,
			},
		)
		.unwrap();
	assert_eq!(
		(approved.original.map(|e| e.op), approved.current),
		(Some(EventOp2::Approve), RootState2::Bound)
	);
	let out = Scratch::new();
	let export = owner
		.export_tuple(
			&out.0,
			MANIFEST,
			INVENTORY,
			identity.generation,
			identity.cluster,
		)
		.unwrap();
	Delivered {
		bytes,
		any,
		identity,
		cmd,
		entry,
		committed,
		root,
		_owner_dir,
		owner,
		out,
		export,
	}
}
/// The replica read beneath an anchor over `dir`, with these names and (optional) pins.
pub(super) fn read_at(
	dir: &Path,
	manifest: &str,
	manifest_pin: Option<&Digest2>,
	snapshot: &str,
	inventory_pin: Option<&Digest2>,
) -> Result<PinnedBoundTuple2, ReplicaReadError2> {
	let anchor = AnchorDir2::open(dir).unwrap();
	let (m, s) = (
		CString::new(manifest).unwrap(),
		CString::new(snapshot).unwrap(),
	);
	read_replica_tuple2(&ReplicaFiles2 {
		anchor: &anchor,
		manifest: &m,
		manifest_pin,
		snapshot: &s,
		inventory_pin,
	})
}
/// The delivery exactly as exported: the owner's files and the owner's returned pins.
pub(super) fn read_delivered(d: &Delivered) -> Result<PinnedBoundTuple2, ReplicaReadError2> {
	read_at(
		&d.out.0,
		MANIFEST,
		Some(&d.export.manifest_pin),
		INVENTORY,
		Some(&d.export.inventory_pin),
	)
}

#[test]
fn owner_export_tuple_feeds_install_for_both_profiles() {
	for (name, length, profile) in [
		("a", 137, ManifestProfile2::One),
		("b", 189, ManifestProfile2::Two),
	] {
		let d = delivered(name, length);
		// The files are the owner's: the exact canonical manifest and nothing placed by the test.
		assert_eq!(fs::read(d.out.at(MANIFEST)).unwrap(), d.bytes, "{name}");
		assert_eq!(d.export.manifest_pin, d.identity.pin, "{name}");
		assert_eq!(d.export.profile, profile, "{name}");
		let tuple = read_delivered(&d).unwrap();
		assert!(*tuple_manifest2(&tuple) == d.any, "{name}");
		assert_eq!(tuple_manifest_len2(&tuple), length, "{name}");
		assert_eq!(
			(tuple_manifest_pin2(&tuple), tuple_inventory_pin2(&tuple)),
			(d.export.manifest_pin, d.export.inventory_pin),
			"{name}"
		);
		assert_eq!(tuple_export_seq2(&tuple), d.export.export_seq, "{name}");
		// The owner-approved record is the observation of the committed root (Approve is seq 2).
		let record = tuple_record2(&tuple);
		assert!(record.identity == observe_bootstrap2(&d.root).unwrap().identity);
		assert_eq!(
			(record.inventory_seq, tuple_entry_last_seq2(&tuple)),
			(2, 2),
			"{name}"
		);
		// C: the actual Bootstrap verification from the pre-root reaches exactly the fixture root.
		let (verified, _) =
			advance_verified2(&pre_root(), &tuple, &d.committed, &d.entry, &d.cmd, None).unwrap();
		assert_eq!(
			encode_authority2(&verified).unwrap(),
			encode_authority2(&d.root).unwrap(),
			"{name}"
		);
		// Install: identity, record, manifest binding and profile.
		let admin = install_admin2(&verified, &tuple).unwrap();
		let a = d.any.admission();
		assert_eq!(
			(admin_id2(&admin), admin_key2(&admin)),
			(a.admin_id, a.admin_key),
			"{name}"
		);
		assert!(admin_namespace2(&admin) == a.namespace, "{name}");
		assert!(
			admin_context2(&admin) == expected_context(&verified),
			"{name}"
		);
		assert_eq!(admin_context2(&admin).manifest_pin, d.export.manifest_pin);
		assert_eq!(
			admin_context2(&admin).bootstrap_command,
			record.identity.bootstrap_command,
			"{name}"
		);
		assert_eq!(
			(admin_profile2(&admin), admin_manifest_len2(&admin)),
			(profile, length),
			"{name}"
		);
		match &d.any {
			AnyAdminManifest2::One(_) => assert!(admin_governance2(&admin).is_none()),
			AnyAdminManifest2::Two(two) => {
				assert!(admin_governance2(&admin).unwrap() == two.governance())
			}
		}
	}
}

/// Coherent owner tuples that pass the replica read and refuse only at C or install: a
/// wrong-length Bootstrap (the M3 length refusals, now from owner bytes) and a wrong context
/// (another root's tuple, valid under its own pins, against this committed root).
#[test]
fn owner_tuples_reach_the_c_and_install_refusals() {
	for (name, right, wrong) in [("a", 137, 189), ("b", 189, 137)] {
		// The owner approved the observation of a root committed with the other length.
		let d = delivered(name, wrong);
		let tuple = read_delivered(&d).expect("the tuple prerequisites pass");
		assert_eq!(tuple_manifest_len2(&tuple), right, "{name}");
		assert_eq!(
			advance_verified2(&pre_root(), &tuple, &d.committed, &d.entry, &d.cmd, None).err(),
			Some(VerifyError2::Consumer(RootError2::TrustMismatch)),
			"{name}"
		);
		let (root, _) = root_and_origin2(&d.root).unwrap();
		assert_eq!(root.state.control_digest, d.export.manifest_pin, "{name}");
		assert_eq!(
			install_admin2(&d.root, &tuple).err(),
			Some(InstallError2::Commitment(CommitmentField2::ControlLength)),
			"{name}"
		);
	}
	// Wrong context: each owner's tuple is valid by its own key, and refused against the other
	// owner's committed root. C's Bootstrap authorization refuses: the tuple record names its own
	// root, not the other entry's generation, cluster and control pin.
	let (a, b) = (delivered("a", 137), delivered("b", 189));
	let (ta, tb) = (read_delivered(&a).unwrap(), read_delivered(&b).unwrap());
	for (tuple, other) in [(&tb, &a), (&ta, &b)] {
		let (g, cl, _, _, _) = other.entry.fields();
		let r = tuple_record2(tuple).identity;
		assert_ne!((r.generation, r.cluster), (g, cl));
		let Command2::Bootstrap { control, .. } = &other.cmd else {
			unreachable!()
		};
		assert_ne!(*control, tuple_manifest_pin2(tuple));
		assert_eq!(
			advance_verified2(
				&pre_root(),
				tuple,
				&other.committed,
				&other.entry,
				&other.cmd,
				None
			)
			.err(),
			Some(VerifyError2::Consumer(RootError2::TrustMismatch))
		);
		assert_eq!(
			install_admin2(&other.root, tuple).err(),
			Some(InstallError2::Commitment(CommitmentField2::ControlDigest))
		);
	}
}
